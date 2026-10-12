"""Append-only SQLite ledger for reproducible daily equity pools."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)

from .allocator import EquityPoolAllocator
from .models import (
    CanonicalClassification,
    DirectionLabel,
    EquityPoolInput,
    EquityPoolSnapshot,
    EquityScore,
    PoolDecision,
    PoolDisposition,
    PositionMode,
)


GENESIS_HASH = "0" * 64
SCHEMA_VERSION = 2
REQUIRED_TRIGGERS = (
    "equity_pool_snapshots_no_update",
    "equity_pool_snapshots_no_delete",
    "equity_pool_rows_no_update",
    "equity_pool_rows_no_delete",
)


class EquityPoolStoreError(RuntimeError):
    pass


class EquityPoolStoreConflict(EquityPoolStoreError):
    pass


class EquityPoolStoreCorruption(EquityPoolStoreError):
    pass


@dataclass(frozen=True, slots=True)
class StoredEquityPoolSnapshot:
    sequence: int
    snapshot: EquityPoolSnapshot
    normalized_inputs: tuple[EquityPoolInput, ...]
    normalized_inputs_hash: str
    snapshot_hash: str
    previous_chain_hash: str
    chain_hash: str
    recorded_at: datetime


class EquityPoolStore:
    """WAL/FULL/FK append-only store with deterministic offline replay."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path,
            isolation_level=None,
            check_same_thread=False,
            timeout=10,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA busy_timeout=10000")
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._migrate()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "EquityPoolStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        value = int(self._connection.execute("PRAGMA synchronous").fetchone()[0])
        return {0: "off", 1: "normal", 2: "full", 3: "extra"}[value]

    @property
    def schema_version(self) -> int:
        self._ensure()
        return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def append(
        self,
        *,
        slot: datetime,
        inputs: Sequence[EquityPoolInput],
        allocator: EquityPoolAllocator,
        position_mode: PositionMode = PositionMode.CLEAR,
        recorded_at: datetime | None = None,
        commit_guard: Callable[[], bool] | None = None,
    ) -> StoredEquityPoolSnapshot:
        self._ensure()
        frozen_slot = utc_datetime(slot, field="slot")
        recorded = utc_datetime(
            recorded_at or datetime.now(timezone.utc),
            field="recorded_at",
        )
        if not isinstance(allocator, EquityPoolAllocator):
            raise TypeError("allocator must be EquityPoolAllocator")
        raw_inputs = tuple(inputs)
        if any(not isinstance(item, EquityPoolInput) for item in raw_inputs):
            raise TypeError("inputs must contain EquityPoolInput values")
        canonical_inputs = tuple(
            sorted(
                raw_inputs,
                key=lambda item: (
                    item.discovery_rank,
                    item.symbol,
                    item.canonical_hash,
                ),
            )
        )
        normalized_bodies = tuple(item.canonical_body() for item in canonical_inputs)
        normalized_json = canonical_json(normalized_bodies)
        normalized_hash = canonical_hash(normalized_bodies)
        snapshot = allocator.allocate(
            canonical_inputs,
            slot=frozen_slot,
            position_mode=position_mode,
        )
        body_json = canonical_json(snapshot.as_dict())
        snapshot_hash = canonical_hash(
            {
                "normalized_inputs_hash": normalized_hash,
                "snapshot": snapshot.as_dict(),
            }
        )
        slot_text = datetime_text(frozen_slot)
        recorded_text = datetime_text(recorded)

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                existing_rows = self._connection.execute(
                    "SELECT * FROM equity_pool_snapshots ORDER BY sequence"
                ).fetchall()
                self._verify_rows(existing_rows)
                existing = self._connection.execute(
                    "SELECT * FROM equity_pool_snapshots WHERE slot = ?",
                    (slot_text,),
                ).fetchone()
                if existing is not None:
                    if str(existing["snapshot_hash"]) != snapshot_hash:
                        raise EquityPoolStoreConflict(
                            "equity pool slot already contains a different snapshot"
                        )
                    if commit_guard is not None and not commit_guard():
                        raise TimeoutError("equity pool commit cancelled")
                    self._connection.execute("COMMIT")
                    return self.read(snapshot.pool_id)

                previous = self._connection.execute(
                    "SELECT chain_hash FROM equity_pool_snapshots ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                previous_hash = (
                    str(previous["chain_hash"]) if previous is not None else GENESIS_HASH
                )
                chain_hash = canonical_hash(
                    {
                        "previous_chain_hash": previous_hash,
                        "slot": frozen_slot,
                        "normalized_inputs_hash": normalized_hash,
                        "snapshot_hash": snapshot_hash,
                    }
                )
                cursor = self._connection.execute(
                    """
                    INSERT INTO equity_pool_snapshots (
                        pool_id, slot, body_json, normalized_inputs_json,
                        normalized_inputs_hash, snapshot_hash,
                        previous_chain_hash, chain_hash, recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.pool_id,
                        slot_text,
                        body_json,
                        normalized_json,
                        normalized_hash,
                        snapshot_hash,
                        previous_hash,
                        chain_hash,
                        recorded_text,
                    ),
                )
                sequence = int(cursor.lastrowid)
                decisions = snapshot.selected + snapshot.excluded
                for ordinal, decision in enumerate(decisions, start=1):
                    row_body = decision.as_dict()
                    self._connection.execute(
                        """
                        INSERT INTO equity_pool_rows (
                            snapshot_sequence, ordinal, symbol, disposition,
                            row_json, row_hash
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            sequence,
                            ordinal,
                            decision.symbol,
                            decision.disposition.value,
                            canonical_json(row_body),
                            canonical_hash(row_body),
                        ),
                    )
                if commit_guard is not None and not commit_guard():
                    raise TimeoutError("equity pool commit cancelled")
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
        return self.read(snapshot.pool_id)

    def latest(self) -> StoredEquityPoolSnapshot | None:
        self._ensure()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM equity_pool_snapshots ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            return self._verified_through_row(row) if row is not None else None

    def read(self, pool_id: str) -> StoredEquityPoolSnapshot:
        self._ensure()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM equity_pool_snapshots WHERE pool_id = ?",
                (pool_id,),
            ).fetchone()
            if row is None:
                raise KeyError(pool_id)
            return self._verified_through_row(row)

    def replay(
        self,
        pool_id: str,
        *,
        allocator: EquityPoolAllocator,
    ) -> EquityPoolSnapshot:
        stored = self.read(pool_id)
        replayed = allocator.allocate(
            stored.normalized_inputs,
            slot=stored.snapshot.slot,
            position_mode=stored.snapshot.position_mode,
        )
        if canonical_json(replayed.as_dict()) != canonical_json(stored.snapshot.as_dict()):
            raise EquityPoolStoreCorruption("persisted equity pool does not replay")
        expected_hash = canonical_hash(
            {
                "normalized_inputs_hash": stored.normalized_inputs_hash,
                "snapshot": replayed.as_dict(),
            }
        )
        if expected_hash != stored.snapshot_hash:
            raise EquityPoolStoreCorruption("replayed equity pool hash mismatch")
        return replayed

    def assert_integrity(self) -> None:
        self._ensure()
        with self._lock:
            snapshots = self._connection.execute(
                "SELECT * FROM equity_pool_snapshots ORDER BY sequence"
            ).fetchall()
            self._verify_rows(snapshots)

    def _verified_through_row(self, target: sqlite3.Row) -> StoredEquityPoolSnapshot:
        sequence = _strict_int(target["sequence"], "sequence", minimum=1)
        rows = self._connection.execute(
            "SELECT * FROM equity_pool_snapshots WHERE sequence <= ? ORDER BY sequence",
            (sequence,),
        ).fetchall()
        verified = self._verify_rows(rows)
        if not verified or verified[-1].sequence != sequence:
            raise EquityPoolStoreCorruption("equity pool predecessor chain is incomplete")
        return verified[-1]

    def _verify_rows(
        self,
        rows: Sequence[sqlite3.Row],
    ) -> tuple[StoredEquityPoolSnapshot, ...]:
        expected_previous = GENESIS_HASH
        expected_sequence = 1
        verified: list[StoredEquityPoolSnapshot] = []
        for row in rows:
            try:
                stored = self._stored_from_row(row)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise EquityPoolStoreCorruption(
                    "equity pool snapshot cannot be decoded"
                ) from exc
            if stored.sequence != expected_sequence:
                raise EquityPoolStoreCorruption("equity pool sequence is not contiguous")
            if stored.previous_chain_hash != expected_previous:
                raise EquityPoolStoreCorruption("equity pool chain predecessor mismatch")
            expected_chain = canonical_hash(
                {
                    "previous_chain_hash": expected_previous,
                    "slot": stored.snapshot.slot,
                    "normalized_inputs_hash": stored.normalized_inputs_hash,
                    "snapshot_hash": stored.snapshot_hash,
                }
            )
            if expected_chain != stored.chain_hash:
                raise EquityPoolStoreCorruption("equity pool chain hash mismatch")
            self._assert_rows(stored)
            verified.append(stored)
            expected_previous = stored.chain_hash
            expected_sequence += 1
        return tuple(verified)

    def _assert_rows(self, stored: StoredEquityPoolSnapshot) -> None:
        rows = self._connection.execute(
            "SELECT * FROM equity_pool_rows WHERE snapshot_sequence = ? ORDER BY ordinal",
            (stored.sequence,),
        ).fetchall()
        decisions = stored.snapshot.selected + stored.snapshot.excluded
        if len(rows) != len(decisions):
            raise EquityPoolStoreCorruption("equity pool row count mismatch")
        for row, decision in zip(rows, decisions):
            try:
                decoded = _decode_canonical(str(row["row_json"]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise EquityPoolStoreCorruption("equity pool row cannot be decoded") from exc
            if not isinstance(decoded, Mapping):
                raise EquityPoolStoreCorruption("equity pool row is not a mapping")
            if canonical_hash(decoded) != str(row["row_hash"]):
                raise EquityPoolStoreCorruption("equity pool row hash mismatch")
            if canonical_json(decoded) != canonical_json(decision.as_dict()):
                raise EquityPoolStoreCorruption("equity pool row/body mismatch")
            if str(row["symbol"]) != decision.symbol:
                raise EquityPoolStoreCorruption("equity pool row symbol mismatch")
            if str(row["disposition"]) != decision.disposition.value:
                raise EquityPoolStoreCorruption("equity pool row disposition mismatch")

    def _stored_from_row(self, row: sqlite3.Row) -> StoredEquityPoolSnapshot:
        try:
            body = _decode_canonical(str(row["body_json"]))
            normalized = _decode_canonical(str(row["normalized_inputs_json"]))
            if not isinstance(body, Mapping) or not isinstance(normalized, list):
                raise TypeError("stored values have invalid shapes")
            snapshot = _snapshot_from_body(body)
            inputs = tuple(
                EquityPoolInput.from_canonical_body(item)
                for item in normalized
                if isinstance(item, Mapping)
            )
            if len(inputs) != len(normalized):
                raise TypeError("normalized inputs contain a non-mapping")
            stored = StoredEquityPoolSnapshot(
                sequence=_strict_int(row["sequence"], "sequence", minimum=1),
                snapshot=snapshot,
                normalized_inputs=inputs,
                normalized_inputs_hash=str(row["normalized_inputs_hash"]),
                snapshot_hash=str(row["snapshot_hash"]),
                previous_chain_hash=str(row["previous_chain_hash"]),
                chain_hash=str(row["chain_hash"]),
                recorded_at=datetime.fromisoformat(str(row["recorded_at"])),
            )
            if str(row["pool_id"]) != snapshot.pool_id:
                raise EquityPoolStoreCorruption("equity pool identifier was tampered")
            if str(row["slot"]) != datetime_text(snapshot.slot):
                raise EquityPoolStoreCorruption("equity pool slot was tampered")
            if canonical_hash(
                tuple(item.canonical_body() for item in inputs)
            ) != stored.normalized_inputs_hash:
                raise EquityPoolStoreCorruption("normalized equity inputs were tampered")
            expected_snapshot_hash = canonical_hash(
                {
                    "normalized_inputs_hash": stored.normalized_inputs_hash,
                    "snapshot": snapshot.as_dict(),
                }
            )
            if expected_snapshot_hash != stored.snapshot_hash:
                raise EquityPoolStoreCorruption("equity pool snapshot was tampered")
            expected_chain_hash = canonical_hash(
                {
                    "previous_chain_hash": stored.previous_chain_hash,
                    "slot": snapshot.slot,
                    "normalized_inputs_hash": stored.normalized_inputs_hash,
                    "snapshot_hash": stored.snapshot_hash,
                }
            )
            if expected_chain_hash != stored.chain_hash:
                raise EquityPoolStoreCorruption("equity pool chain hash mismatch")
            return stored
        except EquityPoolStoreCorruption:
            raise
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise EquityPoolStoreCorruption("equity pool snapshot cannot be decoded") from exc

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version not in {0, 1, SCHEMA_VERSION}:
            raise EquityPoolStoreError(f"unsupported equity pool schema version {version}")
        if version == SCHEMA_VERSION:
            self._assert_required_triggers()
            return
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            if version == 1:
                existing = self._connection.execute(
                    "SELECT * FROM equity_pool_snapshots ORDER BY sequence"
                ).fetchall()
                self._verify_rows(existing)
            if version == 0:
                self._connection.execute(
                """
                CREATE TABLE equity_pool_snapshots (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    pool_id TEXT NOT NULL UNIQUE,
                    slot TEXT NOT NULL UNIQUE,
                    body_json TEXT NOT NULL,
                    normalized_inputs_json TEXT NOT NULL,
                    normalized_inputs_hash TEXT NOT NULL,
                    snapshot_hash TEXT NOT NULL,
                    previous_chain_hash TEXT NOT NULL,
                    chain_hash TEXT NOT NULL UNIQUE,
                    recorded_at TEXT NOT NULL
                )
                """
            )
                self._connection.execute(
                """
                CREATE TABLE equity_pool_rows (
                    snapshot_sequence INTEGER NOT NULL,
                    ordinal INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    row_json TEXT NOT NULL,
                    row_hash TEXT NOT NULL,
                    PRIMARY KEY (snapshot_sequence, ordinal),
                    FOREIGN KEY (snapshot_sequence)
                        REFERENCES equity_pool_snapshots(sequence)
                        ON DELETE RESTRICT
                )
                """
            )
                self._connection.execute(
                """
                CREATE INDEX equity_pool_rows_symbol_idx
                    ON equity_pool_rows(symbol, snapshot_sequence)
                """
            )
            self._create_required_triggers()
            self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self._connection.execute("COMMIT")
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        self._assert_required_triggers()

    def _ensure(self) -> None:
        if self._closed:
            raise EquityPoolStoreError("equity pool store is closed")
        self._assert_required_triggers()

    def _assert_required_triggers(self) -> None:
        rows = self._connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
        definitions = {str(row[0]): str(row[1] or "") for row in rows}
        names = set(definitions)
        missing = tuple(name for name in REQUIRED_TRIGGERS if name not in names)
        if missing:
            raise EquityPoolStoreCorruption(
                f"equity pool immutable triggers missing: {', '.join(missing)}"
            )
        for table, prefix in (
            ("equity_pool_snapshots", "equity_pool_snapshots"),
            ("equity_pool_rows", "equity_pool_rows"),
        ):
            for operation in ("UPDATE", "DELETE"):
                name = f"{prefix}_no_{operation.lower()}"
                expected = _normalize_trigger_sql(_trigger_sql(name, table, operation))
                actual = _normalize_trigger_sql(definitions[name])
                if actual != expected:
                    raise EquityPoolStoreCorruption(
                        f"equity pool immutable trigger definition invalid: {name}"
                    )

    def _create_required_triggers(self) -> None:
        for table, prefix in (
            ("equity_pool_snapshots", "equity_pool_snapshots"),
            ("equity_pool_rows", "equity_pool_rows"),
        ):
            for operation in ("UPDATE", "DELETE"):
                name = f"{prefix}_no_{operation.lower()}"
                self._connection.execute(_trigger_sql(name, table, operation, if_not_exists=True))


def _trigger_sql(
    name: str,
    table: str,
    operation: str,
    *,
    if_not_exists: bool = False,
) -> str:
    qualifier = " IF NOT EXISTS" if if_not_exists else ""
    return (
        f"CREATE TRIGGER{qualifier} {name} BEFORE {operation} ON {table} "
        f"BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END"
    )


def _normalize_trigger_sql(value: str) -> str:
    return " ".join(value.replace("IF NOT EXISTS", "").split()).upper()


def _snapshot_from_body(value: Mapping[str, object]) -> EquityPoolSnapshot:
    selected = value["selected"]
    excluded = value["excluded"]
    counts = value["concentration_counts"]
    if not isinstance(selected, list) or not isinstance(excluded, list):
        raise TypeError("snapshot decisions must be lists")
    if not isinstance(counts, Mapping):
        raise TypeError("concentration_counts must be a mapping")
    return EquityPoolSnapshot(
        pool_id=str(value["pool_id"]),
        slot=datetime.fromisoformat(str(value["slot"])),
        generated_at=datetime.fromisoformat(str(value["generated_at"])),
        policy_version=str(value["policy_version"]),
        policy_hash=str(value["policy_hash"]),
        taxonomy_version=str(value["taxonomy_version"]),
        taxonomy_hash=str(value["taxonomy_hash"]),
        position_mode=PositionMode(str(value["position_mode"])),
        discovery_count=_strict_int(value["discovery_count"], "discovery_count", minimum=0),
        considered_count=_strict_int(value["considered_count"], "considered_count", minimum=0),
        selected=tuple(_decision_from_body(item) for item in selected),
        excluded=tuple(_decision_from_body(item) for item in excluded),
        concentration_counts={
            str(key): _strict_int(item, "concentration_count", minimum=0)
            for key, item in counts.items()
        },
        research_only=value["research_only"],  # type: ignore[arg-type]
        entry_authority=value["entry_authority"],  # type: ignore[arg-type]
        approval_eligible=value["approval_eligible"],  # type: ignore[arg-type]
        decision_authority=value["decision_authority"],  # type: ignore[arg-type]
        instruction_creation_allowed=value["instruction_creation_allowed"],  # type: ignore[arg-type]
        order_allowed=value["order_allowed"],  # type: ignore[arg-type]
    )


def _decode_canonical(value: str) -> object:
    return thaw_json(freeze_json(json.loads(value)))


def _decision_from_body(value: object) -> PoolDecision:
    if not isinstance(value, Mapping):
        raise TypeError("decision must be a mapping")
    score = value["score"]
    classification = value["classification"]
    reasons = value["reasons"]
    if not isinstance(score, Mapping) or not isinstance(classification, Mapping):
        raise TypeError("decision score and classification must be mappings")
    if not isinstance(reasons, list):
        raise TypeError("decision reasons must be a list")
    return PoolDecision(
        symbol=str(value["symbol"]),
        disposition=PoolDisposition(str(value["disposition"])),
        score=EquityScore(
            symbol=str(score["symbol"]),
            direction_score=score["direction_score"],  # type: ignore[arg-type]
            coverage_confidence=score["coverage_confidence"],  # type: ignore[arg-type]
            positive_evidence_mass=score["positive_evidence_mass"],  # type: ignore[arg-type]
            negative_evidence_mass=score["negative_evidence_mass"],  # type: ignore[arg-type]
            conflict_penalty=score["conflict_penalty"],  # type: ignore[arg-type]
            uncertainty=score["uncertainty"],  # type: ignore[arg-type]
            liquidity_score=score.get("liquidity_score"),  # type: ignore[arg-type]
            opportunity_score=score.get("opportunity_score"),  # type: ignore[arg-type]
            direction_label=DirectionLabel(str(score["direction_label"])),
        ),
        classification=CanonicalClassification.from_dict(classification),
        reasons=tuple(str(item) for item in reasons),
        canonical_input_hash=str(value["canonical_input_hash"]),
        selected_rank=(
            _strict_int(value["selected_rank"], "selected_rank", minimum=1)
            if value.get("selected_rank") is not None
            else None
        ),
        decision_authority=value["decision_authority"],  # type: ignore[arg-type]
        instruction_creation_allowed=value["instruction_creation_allowed"],  # type: ignore[arg-type]
        order_allowed=value["order_allowed"],  # type: ignore[arg-type]
        entry_eligible=value.get("entry_eligible", False),  # type: ignore[arg-type]
    )


def _strict_int(value: object, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field} must be at least {minimum}")
    return value


__all__ = [
    "EquityPoolStore",
    "EquityPoolStoreConflict",
    "EquityPoolStoreCorruption",
    "EquityPoolStoreError",
    "StoredEquityPoolSnapshot",
    "REQUIRED_TRIGGERS",
]
