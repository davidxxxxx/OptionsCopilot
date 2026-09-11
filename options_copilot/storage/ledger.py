"""Append-only, point-in-time decision ledger for Options Copilot.

The ledger deliberately stores rejected ideas and controls alongside actual
trades.  This makes later learning reproducible and prevents survivor bias.
SQLite WAL plus synchronous=FULL provides durable local operation, while SQL
triggers and a hash chain make mutation detectable and fail closed.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading

from .canonical import (
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


SCHEMA_VERSION = 1
GENESIS_HASH = "0" * 64
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")


class DecisionKind(str, Enum):
    CANDIDATE = "candidate"
    REJECTED = "rejected"
    REJECTED_CANDIDATE = "rejected"  # descriptive compatibility alias
    RANDOM_CONTROL = "random_control"
    ACTUAL_TRADE = "actual_trade"
    OUTCOME_LABEL = "outcome_label"
    MODEL_REGISTERED = "model_registered"
    SHADOW_RESULT = "shadow_result"
    PROMOTION_REPORT = "promotion_report"
    PROMOTION_APPROVAL = "promotion_approval"
    MODEL_PROMOTED = "model_promoted"
    MODEL_ROLLED_BACK = "model_rolled_back"


class DecisionLedgerError(RuntimeError):
    pass


class DecisionIdentityConflict(DecisionLedgerError):
    """A decision ID was reused with different immutable content."""


class DecisionHashCollision(DecisionLedgerError):
    """The same digest was observed for distinct immutable content."""


class DecisionLedgerCorruption(DecisionLedgerError):
    """Stored rows no longer satisfy the ledger hash/chain contract."""


@dataclass(frozen=True, slots=True)
class PointInTime:
    """Four timestamps needed to prove what was knowable at decision time."""

    first_seen: datetime
    published: datetime
    ingested: datetime
    asof: datetime

    def __post_init__(self) -> None:
        for field in ("first_seen", "published", "ingested", "asof"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        if self.published > self.first_seen:
            raise ValueError("published cannot be after first_seen")
        if self.first_seen > self.ingested:
            raise ValueError("first_seen cannot be after ingested")
        if self.ingested > self.asof:
            raise ValueError("ingested cannot be after asof")

    def as_dict(self) -> dict[str, str]:
        return {
            "first_seen": datetime_text(self.first_seen),
            "published": datetime_text(self.published),
            "ingested": datetime_text(self.ingested),
            "asof": datetime_text(self.asof),
        }


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    decision_id: str
    kind: DecisionKind | str
    scenario_id: str
    source: str
    model_version: str
    timing: PointInTime
    payload: Mapping[str, object]
    related_decision_id: str | None = None

    def __post_init__(self) -> None:
        _validate_identifier("decision_id", self.decision_id)
        _validate_identifier("scenario_id", self.scenario_id)
        _validate_identifier("source", self.source)
        _validate_identifier("model_version", self.model_version)
        if self.related_decision_id is not None:
            _validate_identifier("related_decision_id", self.related_decision_id)
        object.__setattr__(self, "kind", _coerce_kind(self.kind))
        if not isinstance(self.timing, PointInTime):
            raise TypeError("timing must be PointInTime")
        if not isinstance(self.payload, Mapping):
            raise TypeError("payload must be a mapping")
        frozen = freeze_json(self.payload)
        assert isinstance(frozen, Mapping)
        object.__setattr__(self, "payload", frozen)

    @property
    def first_seen(self) -> datetime:
        return self.timing.first_seen

    @property
    def published(self) -> datetime:
        return self.timing.published

    @property
    def ingested(self) -> datetime:
        return self.timing.ingested

    @property
    def asof(self) -> datetime:
        return self.timing.asof

    def immutable_document(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "kind": self.kind.value,
            "scenario_id": self.scenario_id,
            "source": self.source,
            "model_version": self.model_version,
            "related_decision_id": self.related_decision_id,
            **self.timing.as_dict(),
            "payload": thaw_json(self.payload),
        }


@dataclass(frozen=True, slots=True)
class StoredDecision:
    sequence: int
    record: DecisionRecord
    content_hash: str
    previous_hash: str
    chain_hash: str
    recorded_at: datetime

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            **self.record.immutable_document(),
            "content_hash": self.content_hash,
            "previous_hash": self.previous_hash,
            "chain_hash": self.chain_hash,
            "recorded_at": datetime_text(self.recorded_at),
        }


@dataclass(frozen=True, slots=True)
class AppendResult:
    decision: StoredDecision
    inserted: bool

    def __iter__(self):
        yield self.decision
        yield self.inserted


class DecisionLedger:
    """Thread-safe append/query/export interface over an immutable SQLite DB."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        content_hasher: Callable[[bytes], str] | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._content_hasher = content_hasher or _sha256
        self._lock = threading.RLock()
        self._closed = False
        self._verified_integrity_token: tuple[int, int, int] | None = None
        self._connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            synchronous = int(
                self._connection.execute("PRAGMA synchronous").fetchone()[0]
            )
            self._synchronous = {0: "off", 1: "normal", 2: "full", 3: "extra"}.get(
                synchronous, str(synchronous)
            )
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._migrate()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "DecisionLedger":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        return self._synchronous

    @property
    def schema_version(self) -> int:
        self._ensure_open()
        return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def append(self, record: DecisionRecord) -> AppendResult:
        if not isinstance(record, DecisionRecord):
            raise TypeError("record must be DecisionRecord")
        immutable_json = canonical_json(record.immutable_document())
        content_hash = self._content_digest(immutable_json)
        recorded_at = utc_datetime(self._clock(), field="clock result")
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM decision_records WHERE decision_id = ?",
                (record.decision_id,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["immutable_json"]) == immutable_json
                    and str(existing["content_hash"]) == content_hash
                ):
                    return AppendResult(_row_to_stored(existing), False)
                if str(existing["content_hash"]) == content_hash:
                    raise DecisionHashCollision(
                        f"digest collision for decision_id {record.decision_id}"
                    )
                raise DecisionIdentityConflict(
                    f"decision_id {record.decision_id} has different immutable content"
                )

            same_hash = self._connection.execute(
                "SELECT decision_id, immutable_json FROM decision_records WHERE content_hash = ?",
                (content_hash,),
            ).fetchone()
            if same_hash is not None:
                if str(same_hash["immutable_json"]) != immutable_json:
                    raise DecisionHashCollision(
                        "content hash collision between "
                        f"{same_hash['decision_id']} and {record.decision_id}"
                    )
                raise DecisionIdentityConflict(
                    "identical immutable document unexpectedly used a different decision ID"
                )

            tail = self._connection.execute(
                "SELECT sequence, chain_hash FROM decision_records ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            previous_hash = GENESIS_HASH if tail is None else str(tail["chain_hash"])
            chain_hash = _chain_digest(sequence, previous_hash, content_hash)
            self._connection.execute(
                """
                INSERT INTO decision_records(
                    sequence, decision_id, kind, scenario_id, source, model_version,
                    related_decision_id, first_seen, published, ingested, asof,
                    payload_json, immutable_json, content_hash, previous_hash,
                    chain_hash, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sequence,
                    record.decision_id,
                    record.kind.value,
                    record.scenario_id,
                    record.source,
                    record.model_version,
                    record.related_decision_id,
                    datetime_text(record.first_seen),
                    datetime_text(record.published),
                    datetime_text(record.ingested),
                    datetime_text(record.asof),
                    canonical_json(record.payload),
                    immutable_json,
                    content_hash,
                    previous_hash,
                    chain_hash,
                    datetime_text(recorded_at),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM decision_records WHERE sequence = ?", (sequence,)
            ).fetchone()
            if row is None:
                raise DecisionLedgerCorruption("inserted decision is missing")
            return AppendResult(_row_to_stored(row), True)

    append_record = append

    def get(self, decision_id: str) -> StoredDecision | None:
        _validate_identifier("decision_id", decision_id)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM decision_records WHERE decision_id = ?", (decision_id,)
            ).fetchone()
        return None if row is None else _row_to_stored(row)

    def query(
        self,
        *,
        kinds: Sequence[DecisionKind | str] | None = None,
        scenario_id: str | None = None,
        related_decision_id: str | None = None,
        asof_from: datetime | None = None,
        asof_to: datetime | None = None,
        after_sequence: int = 0,
        limit: int = 500,
    ) -> tuple[StoredDecision, ...]:
        if not isinstance(after_sequence, int) or isinstance(after_sequence, bool):
            raise TypeError("after_sequence must be an integer")
        if after_sequence < 0:
            raise ValueError("after_sequence cannot be negative")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        clauses = ["sequence > ?"]
        params: list[object] = [after_sequence]
        if kinds:
            normalized = tuple(_coerce_kind(kind).value for kind in kinds)
            placeholders = ",".join("?" for _ in normalized)
            clauses.append(f"kind IN ({placeholders})")
            params.extend(normalized)
        if scenario_id is not None:
            _validate_identifier("scenario_id", scenario_id)
            clauses.append("scenario_id = ?")
            params.append(scenario_id)
        if related_decision_id is not None:
            _validate_identifier("related_decision_id", related_decision_id)
            clauses.append("related_decision_id = ?")
            params.append(related_decision_id)
        if asof_from is not None:
            clauses.append("asof >= ?")
            params.append(datetime_text(utc_datetime(asof_from, field="asof_from")))
        if asof_to is not None:
            clauses.append("asof <= ?")
            params.append(datetime_text(utc_datetime(asof_to, field="asof_to")))
        if asof_from is not None and asof_to is not None and asof_from > asof_to:
            raise ValueError("asof_from cannot be after asof_to")
        params.append(limit)
        sql = (
            "SELECT * FROM decision_records WHERE "
            + " AND ".join(clauses)
            + " ORDER BY sequence LIMIT ?"
        )
        self._ensure_open()
        with self._lock:
            rows = self._connection.execute(sql, params).fetchall()
        return tuple(_row_to_stored(row) for row in rows)

    def count(self, *, kinds: Sequence[DecisionKind | str] | None = None) -> int:
        if not kinds:
            sql = "SELECT COUNT(*) FROM decision_records"
            params: tuple[object, ...] = ()
        else:
            normalized = tuple(_coerce_kind(kind).value for kind in kinds)
            sql = "SELECT COUNT(*) FROM decision_records WHERE kind IN (" + ",".join(
                "?" for _ in normalized
            ) + ")"
            params = normalized
        self._ensure_open()
        with self._lock:
            return int(self._connection.execute(sql, params).fetchone()[0])

    def export_jsonl(
        self,
        destination: str | Path,
        *,
        kinds: Sequence[DecisionKind | str] | None = None,
        scenario_id: str | None = None,
    ) -> int:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        after = 0
        exported = 0
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            while rows := self.query(
                kinds=kinds,
                scenario_id=scenario_id,
                after_sequence=after,
                limit=1000,
            ):
                for row in rows:
                    handle.write(canonical_json(row.as_dict()) + "\n")
                exported += len(rows)
                after = rows[-1].sequence
        return exported

    export = export_jsonl

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self._ensure_open()
        with self._lock:
            token = self._integrity_token()
            if token == self._verified_integrity_token:
                return
            self._assert_integrity_uncached()
            self._verified_integrity_token = self._integrity_token()

    def _integrity_token(self) -> tuple[int, int, int]:
        return (
            int(self._connection.total_changes),
            int(self._connection.execute("PRAGMA data_version").fetchone()[0]),
            int(self._connection.execute("PRAGMA schema_version").fetchone()[0]),
        )

    def _assert_integrity_uncached(self) -> None:
        self._ensure_open()
        rows = self._connection.execute(
            "SELECT * FROM decision_records ORDER BY sequence"
        ).fetchall()
        expected_previous = GENESIS_HASH
        expected_sequence = 1
        for row in rows:
            stored = _row_to_stored(row)
            if stored.sequence != expected_sequence:
                raise DecisionLedgerCorruption("decision sequence contains a gap")
            immutable_json = canonical_json(stored.record.immutable_document())
            if immutable_json != str(row["immutable_json"]):
                raise DecisionLedgerCorruption(
                    f"immutable document mismatch at sequence {stored.sequence}"
                )
            expected_content = self._content_digest(immutable_json)
            if expected_content != stored.content_hash:
                raise DecisionLedgerCorruption(
                    f"content hash mismatch at sequence {stored.sequence}"
                )
            if stored.previous_hash != expected_previous:
                raise DecisionLedgerCorruption(
                    f"previous hash mismatch at sequence {stored.sequence}"
                )
            expected_chain = _chain_digest(
                stored.sequence, stored.previous_hash, stored.content_hash
            )
            if stored.chain_hash != expected_chain:
                raise DecisionLedgerCorruption(
                    f"chain hash mismatch at sequence {stored.sequence}"
                )
            expected_previous = stored.chain_hash
            expected_sequence += 1

    def _content_digest(self, immutable_json: str) -> str:
        digest = self._content_hasher(immutable_json.encode("utf-8"))
        if not isinstance(digest, str) or _HASH_RE.fullmatch(digest) is None:
            raise ValueError("content_hasher must return a lowercase SHA-256-shaped digest")
        return digest

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"decision ledger schema {version} is newer than supported")
        # sqlite3.executescript commits any pending Python-managed transaction.
        # Put BEGIN/COMMIT inside the script so schema creation is one atomic unit.
        with self._lock:
            self._connection.executescript(
                f"""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS decision_records (
                    sequence INTEGER PRIMARY KEY,
                    decision_id TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL,
                    scenario_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    model_version TEXT NOT NULL,
                    related_decision_id TEXT,
                    first_seen TEXT NOT NULL,
                    published TEXT NOT NULL,
                    ingested TEXT NOT NULL,
                    asof TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    immutable_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    previous_hash TEXT NOT NULL,
                    chain_hash TEXT NOT NULL UNIQUE,
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS decision_records_kind_asof_idx
                    ON decision_records(kind, asof, sequence);
                CREATE INDEX IF NOT EXISTS decision_records_scenario_idx
                    ON decision_records(scenario_id, sequence);
                CREATE INDEX IF NOT EXISTS decision_records_related_idx
                    ON decision_records(related_decision_id, sequence);
                CREATE TRIGGER IF NOT EXISTS decision_records_no_update
                BEFORE UPDATE ON decision_records
                BEGIN
                    SELECT RAISE(ABORT, 'immutable decision ledger: update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS decision_records_no_delete
                BEFORE DELETE ON decision_records
                BEGIN
                    SELECT RAISE(ABORT, 'immutable decision ledger: delete forbidden');
                END;
                PRAGMA user_version={SCHEMA_VERSION};
                COMMIT;
                """
            )

    class _Transaction:
        def __init__(self, ledger: "DecisionLedger") -> None:
            self.ledger = ledger

        def __enter__(self) -> None:
            self.ledger._ensure_open()
            self.ledger._lock.acquire()
            try:
                self.ledger._connection.execute("BEGIN IMMEDIATE")
            except BaseException:
                self.ledger._lock.release()
                raise

        def __exit__(self, exc_type: object, *_: object) -> None:
            try:
                self.ledger._connection.execute("ROLLBACK" if exc_type else "COMMIT")
            finally:
                self.ledger._lock.release()

    def _transaction(self) -> "DecisionLedger._Transaction":
        return self._Transaction(self)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("decision ledger is closed")


def _coerce_kind(value: DecisionKind | str) -> DecisionKind:
    if isinstance(value, DecisionKind):
        return value
    if not isinstance(value, str):
        raise TypeError("kind must be DecisionKind or string")
    normalized = value.strip().lower()
    aliases = {
        "rejected_candidate": DecisionKind.REJECTED,
        "actual": DecisionKind.ACTUAL_TRADE,
        "outcome": DecisionKind.OUTCOME_LABEL,
    }
    if normalized in aliases:
        return aliases[normalized]
    try:
        return DecisionKind(normalized)
    except ValueError as exc:
        raise ValueError(f"unsupported decision kind: {value}") from exc


def _validate_identifier(field: str, value: str) -> None:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ValueError(
            f"{field} must be 1-160 characters using letters, digits, . _ : / or -"
        )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _chain_digest(sequence: int, previous_hash: str, content_hash: str) -> str:
    document = {
        "sequence": sequence,
        "previous_hash": previous_hash,
        "content_hash": content_hash,
    }
    return hashlib.sha256(canonical_json(document).encode("utf-8")).hexdigest()


def _row_to_stored(row: sqlite3.Row) -> StoredDecision:
    timing = PointInTime(
        first_seen=datetime.fromisoformat(str(row["first_seen"])),
        published=datetime.fromisoformat(str(row["published"])),
        ingested=datetime.fromisoformat(str(row["ingested"])),
        asof=datetime.fromisoformat(str(row["asof"])),
    )
    payload = json.loads(str(row["payload_json"]))
    return StoredDecision(
        sequence=int(row["sequence"]),
        record=DecisionRecord(
            decision_id=str(row["decision_id"]),
            kind=str(row["kind"]),
            scenario_id=str(row["scenario_id"]),
            source=str(row["source"]),
            model_version=str(row["model_version"]),
            related_decision_id=(
                None
                if row["related_decision_id"] is None
                else str(row["related_decision_id"])
            ),
            timing=timing,
            payload=payload,
        ),
        content_hash=str(row["content_hash"]),
        previous_hash=str(row["previous_hash"]),
        chain_hash=str(row["chain_hash"]),
        recorded_at=datetime.fromisoformat(str(row["recorded_at"])),
    )


__all__ = [
    "AppendResult",
    "DecisionHashCollision",
    "DecisionIdentityConflict",
    "DecisionKind",
    "DecisionLedger",
    "DecisionLedgerCorruption",
    "DecisionLedgerError",
    "DecisionRecord",
    "GENESIS_HASH",
    "PointInTime",
    "StoredDecision",
]
