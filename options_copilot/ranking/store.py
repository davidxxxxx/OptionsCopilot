"""Append-only, current-authority-aware ranking evidence."""
from __future__ import annotations

import inspect
import json
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


_GuardResult = TypeVar("_GuardResult")
GENESIS_HASH = "0" * 64
SCHEMA_VERSION = 3
_SNAPSHOT_INPUT_FIELDS = (
    "input_hash",
    "evidence_hash",
    "broker_snapshot_hash",
)
_SNAPSHOT_IDENTITY_FIELDS = (
    "input_hash",
    "evidence_hash",
    "broker_snapshot_hash",
    "current_policy_version",
    "current_policy_hash",
    "policy_authority_marker_hash",
    "cost_version",
    "cost_hash",
    "risk_contract_hash",
    "risk_authority_version",
    "risk_authority_marker_hash",
)
_ACTIVE_TABLES = (
    "ranking_decisions",
    "ranking_rows",
    "ranking_bases",
    "ranking_snapshots",
    "ranking_migration_anchors",
)


class RankingStoreError(RuntimeError):
    pass


class RankingStoreCorruption(RankingStoreError):
    pass


class RankingStoreConflict(RankingStoreError):
    pass


@dataclass(frozen=True, slots=True)
class StoredRankingSnapshot:
    ranking_snapshot_id: str
    scan_run_id: str
    snapshot_hash: str
    previous_snapshot_hash: str
    valid_until: datetime
    expected_hashes: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class StoredRankingDecision:
    sequence: int
    scan_run_id: str
    record_type: str
    record: Mapping[str, object]
    record_hash: str
    previous_decision_hash: str
    decision_hash: str
    recorded_at: datetime

    def as_dict(self) -> dict[str, object]:
        return {
            field.name: getattr(self, field.name)
            for field in fields(self)
        }


@dataclass(frozen=True, slots=True)
class _PreparedDecision:
    scan_run_id: str
    record_type: str
    record: Mapping[str, object]
    record_json: str
    record_hash: str
    recorded_at: str


@dataclass(frozen=True, slots=True)
class FrozenRankOneAuthorization:
    ranking_snapshot_id: str
    scan_run_id: str
    candidate_id: str
    proposal_hash: str
    candidate_hash: str
    rank: int
    ranking_basis_hash: str
    row_hash: str
    snapshot_hash: str
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    cost_version: str
    cost_hash: str
    risk_contract_hash: str
    risk_authority_version: str
    risk_authority_marker_hash: str
    candidate_body: Mapping[str, object]
    proposal_body: Mapping[str, object]
    expected_hashes: Mapping[str, object]

    def __post_init__(self) -> None:
        for name in ("candidate_body", "proposal_body", "expected_hashes"):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, Mapping):
                raise TypeError(f"{name} must be a mapping")
            object.__setattr__(self, name, frozen)

    def as_dict(self) -> dict[str, object]:
        return {
            field.name: (
                thaw_json(value) if isinstance(value, Mapping) else value
            )
            for field in fields(self)
            for value in (getattr(self, field.name),)
        }


class RankingStore:
    """SQLite/WAL immutable ranking ledger and rank-one authorization gate."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._verified_integrity_token: tuple[int, int, int] | None = None
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

    def __enter__(self) -> "RankingStore":
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

    def append_snapshot(
        self,
        *,
        scan_run_id: str,
        input_hash: str,
        evidence_hash: str,
        broker_snapshot_hash: str,
        candidates: Sequence[Mapping[str, Any] | object],
        valid_until: datetime,
        policy_authority_marker_hash: str,
        cost_version: str,
        cost_hash: str,
        risk_contract_hash: str,
        risk_authority_version: str,
        risk_authority_marker_hash: str,
        policy_version: str | None = None,
        policy_hash: str | None = None,
        current_policy_version: str | None = None,
        current_policy_hash: str | None = None,
        governance_evidence: Sequence[Mapping[str, Any] | object] = (),
        ranking_snapshot_id: str | None = None,
        policy_resolver: object | None = None,
        risk_authority_resolver: object | None = None,
        resolved_policy: object | None = None,
        risk_authority: object | None = None,
        decision_records: Sequence[Mapping[str, Any] | object] = (),
        now: datetime | None = None,
    ) -> StoredRankingSnapshot:
        """Append a snapshot after current-authority proof under the write lock.

        A resolver-free compatibility append is allowed only when it contains
        governance evidence and creates no ranked row.  It can never become an
        authorization source.
        """

        self._ensure()
        created = utc_datetime(now or datetime.now(timezone.utc), field="now")
        expires = utc_datetime(valid_until, field="valid_until")
        if expires <= created:
            raise ValueError("valid_until must be in the future")
        version = _same_identity(
            "current_policy_version", current_policy_version, policy_version
        )
        resolved_policy_hash = _same_hash(
            "current_policy_hash", current_policy_hash, policy_hash
        )
        identities: dict[str, str] = {
            "input_hash": _hash(input_hash),
            "evidence_hash": _hash(evidence_hash),
            "broker_snapshot_hash": _hash(broker_snapshot_hash),
            "current_policy_version": _identity(version),
            "current_policy_hash": resolved_policy_hash,
            "policy_authority_marker_hash": _hash(policy_authority_marker_hash),
            "cost_version": _identity(cost_version),
            "cost_hash": _hash(cost_hash),
            "risk_contract_hash": _hash(risk_contract_hash),
            "risk_authority_version": _identity(risk_authority_version),
            "risk_authority_marker_hash": _hash(risk_authority_marker_hash),
        }
        scan_id = _identity(scan_run_id)
        snapshot_id = _identity(ranking_snapshot_id or uuid4().hex)
        ranked_raw = tuple(candidates)
        governance_raw = tuple(governance_evidence)
        if not ranked_raw and not governance_raw:
            raise ValueError("snapshot must contain ranked or governance evidence")
        if len(ranked_raw) > 10:
            raise ValueError("snapshots permit at most ten ranked candidates")
        immutable_inputs = _snapshot_immutable_inputs(
            ranked_raw + governance_raw, identities
        )
        ranked_bases = tuple(
            _basis_record(
                item,
                identities,
                immutable_inputs,
                authorizable_required=True,
            )
            for item in ranked_raw
        )
        governance_bases = tuple(
            _basis_record(
                item,
                identities,
                immutable_inputs,
                authorizable_required=False,
            )
            for item in governance_raw
        )
        all_bases = ranked_bases + governance_bases
        candidate_ids = [str(item["candidate_id"]) for item in all_bases]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("ranking candidates must be unique")
        rows = tuple(
            _row_record(base, item, rank, identities)
            for rank, (base, item) in enumerate(
                zip(ranked_bases, ranked_raw), start=1
            )
        )
        immutable_json = canonical_json(immutable_inputs)
        immutable_hash = canonical_hash(immutable_inputs)
        created_text = datetime_text(created)
        prepared_decisions = _prepare_decision_records(
            decision_records,
            default_scan_run_id=scan_id,
            recorded_at=created_text,
        )
        decision_kinds = tuple(item.record_type for item in prepared_decisions)
        if decision_kinds and not (
            all(kind == "SCENARIO" for kind in decision_kinds)
            or (
                decision_kinds[-1] == "GATE_BUNDLE_TRADE"
                and all(kind == "SCENARIO" for kind in decision_kinds[:-1])
            )
        ):
            raise ValueError(
                "snapshot decision_records must contain scenarios followed by one Gate bundle"
            )
        gate_hash = immutable_inputs.get("gate_bundle_hash")
        gate_records = tuple(
            item
            for item in prepared_decisions
            if item.record_type == "GATE_BUNDLE_TRADE"
        )
        if gate_hash is not None:
            if _hash(gate_hash) != gate_hash or len(gate_records) != 1:
                raise ValueError("RANKING_GATE_BUNDLE_MISMATCH")
            _assert_gate_bundle_record(
                gate_records[0],
                expected_outcome="TRADE",
                expected_hash=str(gate_hash),
            )
        if prepared_decisions and not rows:
            raise ValueError("governance-only snapshots cannot finalize scenario records")
        decision_bindings = tuple(
            _decision_binding(item) for item in prepared_decisions
        )
        decision_records_json = canonical_json(decision_bindings)
        decision_records_hash = canonical_hash(decision_bindings)
        valid_text = datetime_text(expires)

        with self._transaction():
            self.assert_integrity()
            if rows:
                _assert_append_authority(
                    identities,
                    ranked_bases=ranked_bases,
                    policy_resolver=policy_resolver,
                    risk_authority_resolver=risk_authority_resolver,
                    resolved_policy=resolved_policy,
                    risk_authority=risk_authority,
                )
            existing = self._connection.execute(
                "SELECT * FROM ranking_snapshots WHERE scan_run_id=?", (scan_id,)
            ).fetchone()
            if existing is not None:
                if not self._same_snapshot(
                    existing,
                    identities,
                    immutable_json,
                    immutable_hash,
                    decision_records_json,
                    decision_records_hash,
                    valid_text,
                    all_bases,
                    rows,
                ):
                    raise RankingStoreConflict(
                        "scan_run_id already has different immutable ranking bindings"
                    )
                stored = self._stored_snapshot(existing)
                if rows:
                    terminal = _ranking_terminal_decision(
                        scan_run_id=scan_id,
                        ranking_snapshot_id=stored.ranking_snapshot_id,
                        snapshot_hash=stored.snapshot_hash,
                        recorded_at=str(existing["created_at"]),
                    )
                    self._append_prepared_batch_in_transaction(
                        prepared_decisions + (terminal,)
                    )
                return stored

            tail = self._connection.execute(
                "SELECT snapshot_hash FROM ranking_snapshots "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            previous = self._chain_origin() if tail is None else str(tail[0])
            payload = _snapshot_payload(
                ranking_snapshot_id=snapshot_id,
                scan_run_id=scan_id,
                identities=identities,
                immutable_inputs=immutable_inputs,
                immutable_inputs_json=immutable_json,
                immutable_inputs_hash=immutable_hash,
                decision_records=decision_bindings,
                decision_records_json=decision_records_json,
                decision_records_hash=decision_records_hash,
                created_at=created_text,
                valid_until=valid_text,
                previous_snapshot_hash=previous,
                bases=all_bases,
                rows=rows,
            )
            snapshot_hash = canonical_hash(payload)
            self._connection.execute(
                """
                INSERT INTO ranking_snapshots(
                    ranking_snapshot_id, scan_run_id, input_hash, evidence_hash,
                    broker_snapshot_hash, immutable_inputs_json,
                    immutable_inputs_hash, decision_records_json,
                    decision_records_hash, current_policy_version,
                    current_policy_hash, policy_authority_marker_hash,
                    cost_version, cost_hash, risk_contract_hash,
                    risk_authority_version, risk_authority_marker_hash,
                    created_at, valid_until, previous_snapshot_hash, snapshot_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    snapshot_id,
                    scan_id,
                    identities["input_hash"],
                    identities["evidence_hash"],
                    identities["broker_snapshot_hash"],
                    immutable_json,
                    immutable_hash,
                    decision_records_json,
                    decision_records_hash,
                    identities["current_policy_version"],
                    identities["current_policy_hash"],
                    identities["policy_authority_marker_hash"],
                    identities["cost_version"],
                    identities["cost_hash"],
                    identities["risk_contract_hash"],
                    identities["risk_authority_version"],
                    identities["risk_authority_marker_hash"],
                    created_text,
                    valid_text,
                    previous,
                    snapshot_hash,
                ),
            )
            self._connection.executemany(
                """
                INSERT INTO ranking_bases(
                    ranking_snapshot_id, candidate_id, proposal_hash,
                    candidate_hash, candidate_body_json, candidate_body_hash,
                    proposal_body_json, proposal_body_hash,
                    ranking_basis_hash, evidence_inputs_json,
                    evidence_inputs_hash, authority_status, authorizable,
                    basis_json, basis_content_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        snapshot_id,
                        base["candidate_id"],
                        base["proposal_hash"],
                        base["candidate_hash"],
                        base["candidate_body_json"],
                        base["candidate_body_hash"],
                        base["proposal_body_json"],
                        base["proposal_body_hash"],
                        base["ranking_basis_hash"],
                        base["evidence_inputs_json"],
                        base["evidence_inputs_hash"],
                        base["authority_status"],
                        1 if base["authorizable"] else 0,
                        base["basis_json"],
                        base["basis_content_hash"],
                    )
                    for base in all_bases
                ],
            )
            self._connection.executemany(
                """
                INSERT INTO ranking_rows(
                    ranking_snapshot_id, rank, candidate_id, proposal_hash,
                    candidate_hash, candidate_body_json, candidate_body_hash,
                    proposal_body_json, proposal_body_hash,
                    ranking_basis_hash, authority_status, authorizable,
                    risk_authority_marker_hash, score_json, score_hash, row_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        snapshot_id,
                        row["rank"],
                        row["candidate_id"],
                        row["proposal_hash"],
                        row["candidate_hash"],
                        row["candidate_body_json"],
                        row["candidate_body_hash"],
                        row["proposal_body_json"],
                        row["proposal_body_hash"],
                        row["ranking_basis_hash"],
                        row["authority_status"],
                        1,
                        row["risk_authority_marker_hash"],
                        row["score_json"],
                        row["score_hash"],
                        row["row_hash"],
                    )
                    for row in rows
                ],
            )
            if rows:
                terminal = _ranking_terminal_decision(
                    scan_run_id=scan_id,
                    ranking_snapshot_id=snapshot_id,
                    snapshot_hash=snapshot_hash,
                    recorded_at=created_text,
                )
                self._append_prepared_batch_in_transaction(
                    prepared_decisions + (terminal,)
                )
        expected = (
            _expected_hashes(identities, rows[0], snapshot_hash, immutable_hash)
            if rows
            else _snapshot_expected_hashes(
                identities, snapshot_hash, immutable_hash
            )
        )
        return StoredRankingSnapshot(
            snapshot_id,
            scan_id,
            snapshot_hash,
            previous,
            expires,
            expected,
        )

    append = append_snapshot

    def append_decision(
        self,
        scan_run_id: str,
        record_type: str | None = None,
        record: Mapping[str, Any] | object | None = None,
        *,
        decision_type: str | None = None,
        payload: Mapping[str, Any] | object | None = None,
        decision: Mapping[str, Any] | object | None = None,
        now: datetime | None = None,
    ) -> StoredRankingDecision:
        """Compatibility single-record append.

        A TRADE terminal is never accepted here; only append_snapshot may
        create that authority-bearing terminal in the snapshot transaction.
        """

        kind = _same_identity("record_type", record_type, decision_type).upper()
        if kind in {"TRADE", "RANKING_FINALIZED"}:
            raise ValueError("TRADE terminals may be created only by append_snapshot")
        supplied = [item for item in (record, payload, decision) if item is not None]
        if len(supplied) != 1:
            raise ValueError("exactly one decision record is required")
        prepared = _prepare_decision(
            scan_run_id=scan_run_id,
            record_type=kind,
            record=supplied[0],
            recorded_at=datetime_text(
                utc_datetime(now or datetime.now(timezone.utc), field="now")
            ),
        )
        with self._transaction():
            self.assert_integrity()
            return self._append_prepared_batch_in_transaction((prepared,))[0]

    def append_decisions(
        self,
        *,
        scan_run_id: str,
        records: Sequence[Mapping[str, Any] | object],
        now: datetime | None = None,
    ) -> tuple[StoredRankingDecision, ...]:
        """Atomically append zero or more scenarios and terminal NO_TRADE."""

        recorded_at = datetime_text(
            utc_datetime(now or datetime.now(timezone.utc), field="now")
        )
        prepared = _prepare_decision_records(
            records,
            default_scan_run_id=_identity(scan_run_id),
            recorded_at=recorded_at,
        )
        if not prepared:
            raise ValueError("decision batch cannot be empty")
        prefix = prepared[:-1]
        prefix_kinds = tuple(item.record_type for item in prefix)
        valid_prefix = all(kind == "SCENARIO" for kind in prefix_kinds) or (
            bool(prefix_kinds)
            and prefix_kinds[-1] == "GATE_BUNDLE_NO_TRADE"
            and all(kind == "SCENARIO" for kind in prefix_kinds[:-1])
        )
        if prepared[-1].record_type != "NO_TRADE" or not valid_prefix:
            raise ValueError(
                "decision batch must contain scenarios, optional Gate bundle, and terminal NO_TRADE"
            )
        gate_records = tuple(
            item for item in prefix if item.record_type == "GATE_BUNDLE_NO_TRADE"
        )
        if gate_records:
            expected_hash = prepared[-1].record.get("gate_bundle_hash")
            if _hash(expected_hash) != expected_hash or len(gate_records) != 1:
                raise ValueError("RANKING_GATE_BUNDLE_MISMATCH")
            _assert_gate_bundle_record(
                gate_records[0],
                expected_outcome="NO_TRADE",
                expected_hash=str(expected_hash),
            )
        terminal_status = prepared[-1].record.get("status")
        if terminal_status is not None and terminal_status != "NO_TRADE":
            raise ValueError("NO_TRADE terminal status mismatch")
        with self._transaction():
            self.assert_integrity()
            return self._append_prepared_batch_in_transaction(prepared)

    def _append_prepared_batch_in_transaction(
        self, prepared: Sequence[_PreparedDecision]
    ) -> tuple[StoredRankingDecision, ...]:
        if not prepared:
            return ()
        hashes = [item.record_hash for item in prepared]
        if len(hashes) != len(set(hashes)):
            raise RankingStoreConflict("decision batch contains duplicate records")
        existing_rows = [
            self._connection.execute(
                "SELECT * FROM ranking_decisions WHERE record_hash=?",
                (item.record_hash,),
            ).fetchone()
            for item in prepared
        ]
        if all(row is not None for row in existing_rows):
            rows = [row for row in existing_rows if row is not None]
            sequences = [int(row["sequence"]) for row in rows]
            if sequences != list(range(sequences[0], sequences[0] + len(rows))):
                raise RankingStoreConflict("decision batch is not contiguous")
            stored = tuple(_stored_decision(row) for row in rows)
            if any(
                item.scan_run_id != expected.scan_run_id
                or item.record_type != expected.record_type
                or item.record_json != canonical_json(expected.record)
                for item, expected in zip(prepared, stored)
            ):
                raise RankingStoreConflict("decision hash collision")
            return stored
        if any(row is not None for row in existing_rows):
            raise RankingStoreConflict("decision batch was only partially persisted")

        stored: list[StoredRankingDecision] = []
        for item in prepared:
            tail = self._connection.execute(
                "SELECT sequence, decision_hash FROM ranking_decisions "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            previous = (
                self._chain_origin()
                if tail is None
                else str(tail["decision_hash"])
            )
            decision_payload = _decision_payload(
                sequence=sequence,
                scan_run_id=item.scan_run_id,
                record_type=item.record_type,
                record=item.record,
                record_json=item.record_json,
                record_hash=item.record_hash,
                previous_decision_hash=previous,
                recorded_at=item.recorded_at,
            )
            decision_hash = canonical_hash(decision_payload)
            self._connection.execute(
                """
                INSERT INTO ranking_decisions(
                    sequence, scan_run_id, record_type, record_json,
                    record_hash, previous_decision_hash, decision_hash,
                    recorded_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    item.scan_run_id,
                    item.record_type,
                    item.record_json,
                    item.record_hash,
                    previous,
                    decision_hash,
                    item.recorded_at,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM ranking_decisions WHERE sequence=?", (sequence,)
            ).fetchone()
            if row is None:
                raise RankingStoreCorruption("inserted decision is missing")
            stored.append(_stored_decision(row))
        return tuple(stored)

    def read_decisions(
        self, scan_run_id: str | None = None
    ) -> tuple[StoredRankingDecision, ...]:
        self._ensure()
        if scan_run_id is None:
            rows = self._connection.execute(
                "SELECT * FROM ranking_decisions ORDER BY sequence"
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM ranking_decisions WHERE scan_run_id=? "
                "ORDER BY sequence",
                (_identity(scan_run_id),),
            ).fetchall()
        return tuple(_stored_decision(row) for row in rows)

    def latest_decision(self) -> StoredRankingDecision | None:
        """Return the verified decision-chain head for safe read models."""

        self._ensure()
        with self._lock:
            self.assert_integrity()
            row = self._connection.execute(
                "SELECT * FROM ranking_decisions ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            return None if row is None else _stored_decision(row)

    def current_terminal_for_snapshot(self, ranking_snapshot_id: str) -> bool:
        """Prove that the decision head is this snapshot's TRADE terminal."""

        self._ensure()
        snapshot_id = _identity(ranking_snapshot_id)
        with self._lock:
            self.assert_integrity()
            snapshot = self._connection.execute(
                "SELECT * FROM ranking_snapshots WHERE ranking_snapshot_id=?",
                (snapshot_id,),
            ).fetchone()
            return bool(
                snapshot is not None
                and self._current_terminal_matches_snapshot(snapshot)
            )

    def get_by_scan_run(self, scan_run_id: str) -> StoredRankingSnapshot | None:
        self._ensure()
        row = self._connection.execute(
            "SELECT * FROM ranking_snapshots WHERE scan_run_id=?", (scan_run_id,)
        ).fetchone()
        return None if row is None else self._stored_snapshot(row)

    def latest(self) -> StoredRankingSnapshot | None:
        self._ensure()
        row = self._connection.execute(
            "SELECT * FROM ranking_snapshots ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return None if row is None else self._stored_snapshot(row)

    def read_snapshot(self, ranking_snapshot_id: str) -> dict[str, object]:
        self._ensure()
        # Every public read is authority-bearing input for the GUI/runtime.
        # Verify the complete append-only chain before projecting any row so a
        # trigger-bypassing database edit cannot become a self-consistent but
        # forged candidate/evidence view.
        self.assert_integrity()
        snapshot = self._connection.execute(
            "SELECT * FROM ranking_snapshots WHERE ranking_snapshot_id=?",
            (ranking_snapshot_id,),
        ).fetchone()
        if snapshot is None:
            raise KeyError(ranking_snapshot_id)
        rows = self._connection.execute(
            "SELECT * FROM ranking_rows WHERE ranking_snapshot_id=? ORDER BY rank",
            (ranking_snapshot_id,),
        ).fetchall()
        bases = self._connection.execute(
            "SELECT * FROM ranking_bases WHERE ranking_snapshot_id=? ORDER BY rowid",
            (ranking_snapshot_id,),
        ).fetchall()
        ranked_ids = {str(row["candidate_id"]) for row in rows}
        immutable_inputs = json.loads(str(snapshot["immutable_inputs_json"]))
        raw_funnel_trace = immutable_inputs.get("funnel_trace")
        if (
            isinstance(raw_funnel_trace, dict)
            and raw_funnel_trace.get("ranked_count") != len(rows)
        ):
            raise RankingStoreCorruption(
                "immutable funnel trace ranked count does not match ranking rows"
            )
        funnel_trace = raw_funnel_trace if isinstance(raw_funnel_trace, dict) else None
        terminal_projection: dict[str, object] = {}
        if rows:
            expected_terminal = _ranking_terminal_decision(
                scan_run_id=str(snapshot["scan_run_id"]),
                ranking_snapshot_id=str(snapshot["ranking_snapshot_id"]),
                snapshot_hash=str(snapshot["snapshot_hash"]),
                recorded_at=str(snapshot["created_at"]),
            )
            terminal = self._connection.execute(
                "SELECT * FROM ranking_decisions WHERE record_hash=?",
                (expected_terminal.record_hash,),
            ).fetchone()
            if terminal is None:
                raise RankingStoreCorruption(
                    "snapshot TRADE terminal decision is unavailable"
                )
            terminal_projection = {
                "decision_hash": str(terminal["decision_hash"]),
                "record_hash": str(terminal["record_hash"]),
                "terminal_recorded_at": str(terminal["recorded_at"]),
            }
            gate_bundle_hash = immutable_inputs.get("gate_bundle_hash")
            if (
                isinstance(gate_bundle_hash, str)
                and _hash(gate_bundle_hash) == gate_bundle_hash
            ):
                terminal_projection["gate_bundle_hash"] = gate_bundle_hash
        return {
            "ranking_snapshot_id": str(snapshot["ranking_snapshot_id"]),
            "scan_run_id": str(snapshot["scan_run_id"]),
            "snapshot_hash": str(snapshot["snapshot_hash"]),
            "previous_snapshot_hash": str(snapshot["previous_snapshot_hash"]),
            "valid_until": str(snapshot["valid_until"]),
            "immutable_inputs": immutable_inputs,
            "funnel_trace": funnel_trace,
            "immutable_inputs_hash": str(snapshot["immutable_inputs_hash"]),
            "decision_records": json.loads(str(snapshot["decision_records_json"])),
            "decision_records_hash": str(snapshot["decision_records_hash"]),
            **terminal_projection,
            **{
                name: str(snapshot[name])
                for name in _SNAPSHOT_IDENTITY_FIELDS
            },
            "candidates": [_public_row(row) for row in rows],
            "governance_evidence": [
                _public_basis(base)
                for base in bases
                if str(base["candidate_id"]) not in ranked_ids
            ],
        }

    def outcome_targets(
        self,
        *,
        after_sequence: int = 0,
    ) -> tuple[Mapping[str, object], ...]:
        """Return every immutable historical ranked-candidate outcome target."""

        if (
            not isinstance(after_sequence, int)
            or isinstance(after_sequence, bool)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        self._ensure()
        with self._lock:
            self.assert_integrity()
            rows = self._connection.execute(
                "SELECT "
                "s.sequence AS snapshot_sequence, s.ranking_snapshot_id, "
                "s.scan_run_id, s.snapshot_hash, s.input_hash, s.evidence_hash, "
                "s.broker_snapshot_hash, s.current_policy_version, "
                "s.current_policy_hash, s.policy_authority_marker_hash, "
                "s.cost_version, s.cost_hash, s.created_at, "
                "r.rank, r.candidate_id, r.candidate_hash, "
                "r.candidate_body_json, r.ranking_basis_hash, r.row_hash "
                "FROM ranking_snapshots AS s "
                "JOIN ranking_rows AS r USING(ranking_snapshot_id) "
                "WHERE (s.sequence * 10 + r.rank) > ? "
                "ORDER BY s.sequence, r.rank",
                (after_sequence,),
            ).fetchall()
            targets = tuple(_ranking_outcome_target(row) for row in rows)
        return targets

    def open_outcome_target_cursor(
        self,
    ) -> tuple[tuple[Mapping[str, object], ...], int, str]:
        """Verify once, then return historical targets and the snapshot tail."""

        self.assert_integrity()
        with self._lock:
            rows = self._outcome_target_rows_locked(after_snapshot_sequence=0)
            tail = self._connection.execute(
                "SELECT sequence, snapshot_hash FROM ranking_snapshots "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            origin = self._assert_migration_anchors() if tail is None else None
        return (
            tuple(_ranking_outcome_target(row) for row in rows),
            0 if tail is None else int(tail["sequence"]),
            str(origin) if tail is None else str(tail["snapshot_hash"]),
        )

    def outcome_target_cursor_page(
        self,
        *,
        after_snapshot_sequence: int,
        previous_snapshot_hash: str,
        limit: int = 500,
    ) -> tuple[tuple[Mapping[str, object], ...], int, str]:
        """Verify new snapshots incrementally from a previously verified tail."""

        if (
            not isinstance(after_snapshot_sequence, int)
            or isinstance(after_snapshot_sequence, bool)
            or after_snapshot_sequence < 0
        ):
            raise ValueError(
                "after_snapshot_sequence must be a non-negative integer"
            )
        previous_snapshot_hash = _hash(previous_snapshot_hash)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        self._ensure()
        with self._lock:
            self._assert_outcome_cursor_guard_locked()
            if after_snapshot_sequence:
                anchor = self._connection.execute(
                    "SELECT snapshot_hash FROM ranking_snapshots WHERE sequence=?",
                    (after_snapshot_sequence,),
                ).fetchone()
                if (
                    anchor is None
                    or str(anchor["snapshot_hash"]) != previous_snapshot_hash
                ):
                    raise RankingStoreCorruption("ranking cursor anchor mismatch")
            snapshots = self._connection.execute(
                "SELECT * FROM ranking_snapshots WHERE sequence > ? "
                "ORDER BY sequence LIMIT ?",
                (after_snapshot_sequence, limit),
            ).fetchall()
            expected_sequence = after_snapshot_sequence + 1
            expected_previous = previous_snapshot_hash
            for snapshot in snapshots:
                if int(snapshot["sequence"]) != expected_sequence:
                    raise RankingStoreCorruption(
                        "ranking cursor snapshot sequence gap"
                    )
                self._verify_outcome_cursor_snapshot_locked(
                    snapshot,
                    previous_snapshot_hash=expected_previous,
                )
                expected_sequence += 1
                expected_previous = str(snapshot["snapshot_hash"])
            rows = self._outcome_target_rows_locked(
                after_snapshot_sequence=after_snapshot_sequence,
                at_or_before_snapshot_sequence=(
                    None if not snapshots else int(snapshots[-1]["sequence"])
                ),
            )
        return (
            tuple(_ranking_outcome_target(row) for row in rows),
            expected_sequence - 1,
            expected_previous,
        )

    def _outcome_target_rows_locked(
        self,
        *,
        after_snapshot_sequence: int,
        at_or_before_snapshot_sequence: int | None = None,
    ) -> tuple[sqlite3.Row, ...]:
        clauses = ["s.sequence > ?"]
        parameters: list[object] = [after_snapshot_sequence]
        if at_or_before_snapshot_sequence is not None:
            clauses.append("s.sequence <= ?")
            parameters.append(at_or_before_snapshot_sequence)
        return tuple(
            self._connection.execute(
                "SELECT "
                "s.sequence AS snapshot_sequence, s.ranking_snapshot_id, "
                "s.scan_run_id, s.snapshot_hash, s.input_hash, s.evidence_hash, "
                "s.broker_snapshot_hash, s.current_policy_version, "
                "s.current_policy_hash, s.policy_authority_marker_hash, "
                "s.cost_version, s.cost_hash, s.created_at, "
                "r.rank, r.candidate_id, r.candidate_hash, "
                "r.candidate_body_json, r.ranking_basis_hash, r.row_hash "
                "FROM ranking_snapshots AS s "
                "JOIN ranking_rows AS r USING(ranking_snapshot_id) WHERE "
                + " AND ".join(clauses)
                + " ORDER BY s.sequence, r.rank",
                parameters,
            ).fetchall()
        )

    def _assert_outcome_cursor_guard_locked(self) -> None:
        if int(self._connection.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
            raise RankingStoreCorruption("ranking cursor schema guard failed")
        expected = {
            f"{table}_no_{operation}"
            for table in _ACTIVE_TABLES
            for operation in ("update", "delete")
        }
        rows = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        present = {str(row["name"]) for row in rows}
        if not expected.issubset(present):
            raise RankingStoreCorruption(
                "ranking cursor append-only trigger guard failed"
            )

    def _verify_outcome_cursor_snapshot_locked(
        self,
        snapshot: sqlite3.Row,
        *,
        previous_snapshot_hash: str,
    ) -> None:
        snapshot_id = str(snapshot["ranking_snapshot_id"])
        identities = _snapshot_identities(snapshot)
        immutable_inputs = _canonical_json_column(snapshot, "immutable_inputs_json")
        if not isinstance(immutable_inputs, dict):
            raise RankingStoreCorruption("ranking cursor immutable inputs invalid")
        if canonical_hash(immutable_inputs) != str(snapshot["immutable_inputs_hash"]):
            raise RankingStoreCorruption("ranking cursor immutable input hash mismatch")
        if any(
            immutable_inputs.get(name) != identities[name]
            for name in _SNAPSHOT_INPUT_FIELDS
        ):
            raise RankingStoreCorruption("ranking cursor immutable input mismatch")
        decision_bindings = _canonical_json_column(snapshot, "decision_records_json")
        if not isinstance(decision_bindings, list) or any(
            not isinstance(item, dict) for item in decision_bindings
        ):
            raise RankingStoreCorruption("ranking cursor decision bindings invalid")
        if canonical_hash(decision_bindings) != str(snapshot["decision_records_hash"]):
            raise RankingStoreCorruption("ranking cursor decision binding mismatch")
        bases_raw = self._connection.execute(
            "SELECT * FROM ranking_bases WHERE ranking_snapshot_id=? ORDER BY rowid",
            (snapshot_id,),
        ).fetchall()
        rows_raw = self._connection.execute(
            "SELECT * FROM ranking_rows WHERE ranking_snapshot_id=? ORDER BY rank",
            (snapshot_id,),
        ).fetchall()
        if (
            len(rows_raw) > 10
            or [int(row["rank"]) for row in rows_raw]
            != list(range(1, len(rows_raw) + 1))
            or not bases_raw
        ):
            raise RankingStoreCorruption("invalid ranking cursor rows")
        bases = tuple(
            _verified_basis_from_sql(row, identities, immutable_inputs)
            for row in bases_raw
        )
        bases_by_id = {str(base["candidate_id"]): base for base in bases}
        if len(bases_by_id) != len(bases):
            raise RankingStoreCorruption("duplicate ranking cursor candidate")
        verified_rows: list[dict[str, object]] = []
        ranked_ids: set[str] = set()
        for raw in rows_raw:
            candidate_id = str(raw["candidate_id"])
            base = bases_by_id.get(candidate_id)
            if base is None:
                raise RankingStoreCorruption("ranking cursor row has no basis")
            row = _row_from_sql(raw)
            if row != _row_record(
                base,
                {"score_components": row["score_components"]},
                int(row["rank"]),
                identities,
            ):
                raise RankingStoreCorruption("ranking cursor row hash mismatch")
            verified_rows.append(row)
            ranked_ids.add(candidate_id)
        for base in bases:
            is_ranked = str(base["candidate_id"]) in ranked_ids
            if bool(base["authorizable"]) is not is_ranked:
                raise RankingStoreCorruption(
                    "ranking cursor authorizability mismatch"
                )
        if str(snapshot["previous_snapshot_hash"]) != previous_snapshot_hash:
            raise RankingStoreCorruption("broken ranking cursor chain")
        payload = _snapshot_payload(
            ranking_snapshot_id=snapshot_id,
            scan_run_id=str(snapshot["scan_run_id"]),
            identities=identities,
            immutable_inputs=immutable_inputs,
            immutable_inputs_json=str(snapshot["immutable_inputs_json"]),
            immutable_inputs_hash=str(snapshot["immutable_inputs_hash"]),
            decision_records=decision_bindings,
            decision_records_json=str(snapshot["decision_records_json"]),
            decision_records_hash=str(snapshot["decision_records_hash"]),
            created_at=str(snapshot["created_at"]),
            valid_until=str(snapshot["valid_until"]),
            previous_snapshot_hash=previous_snapshot_hash,
            bases=bases,
            rows=tuple(verified_rows),
        )
        if canonical_hash(payload) != str(snapshot["snapshot_hash"]):
            raise RankingStoreCorruption("ranking cursor snapshot hash mismatch")
        self._assert_snapshot_decision_batch(
            snapshot,
            decision_bindings,
            has_ranked_rows=bool(verified_rows),
        )

    def authorize_frozen_rank_one(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        expected_hashes: Mapping[str, Any] | object | None = None,
        *,
        policy_resolver: object | None = None,
        risk_authority_resolver: object | None = None,
        now: datetime | None = None,
    ) -> FrozenRankOneAuthorization | None:
        """Reread and authorize only the current, fully expected rank-one row."""

        self._ensure()
        if (
            not isinstance(expected_hashes, Mapping)
            or policy_resolver is None
            or risk_authority_resolver is None
            or not _has_current_api(policy_resolver)
            or not _has_current_api(risk_authority_resolver)
        ):
            return None
        expected = dict(expected_hashes)
        checked_at = utc_datetime(now or datetime.now(timezone.utc), field="now")
        try:
            with self._transaction():
                return self._frozen_rank_one_locked(
                    ranking_snapshot_id,
                    candidate_id,
                    checked_at=checked_at,
                    policy_resolver=policy_resolver,
                    risk_authority_resolver=risk_authority_resolver,
                    execution_cost_contract=None,
                    expected_hashes=expected,
                    require_current_cost=False,
                )
        except Exception:
            return None

    def guard_frozen_rank_one(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        *,
        policy_resolver: object,
        risk_authority_resolver: object,
        execution_cost_contract: object,
        callback: Callable[[FrozenRankOneAuthorization], "_GuardResult"],
        now: datetime | None = None,
    ) -> "_GuardResult | None":
        """Linearize a restricted side effect against the current rank-one head.

        The callback receives only the canonical, server-derived authorization
        and runs while this store still owns ``BEGIN IMMEDIATE``.  Validation
        failures return ``None`` without invoking it.  Callback failures are
        deliberately not swallowed so the callback's own transaction can roll
        back and callers cannot mistake a partial write for a rejection.
        """

        self._ensure()
        if not callable(callback):
            raise TypeError("callback must be callable")
        if (
            policy_resolver is None
            or risk_authority_resolver is None
            or execution_cost_contract is None
            or not _has_guard_current_api(policy_resolver)
            or not _has_guard_current_api(risk_authority_resolver)
            or not _has_guard_current_api(execution_cost_contract)
        ):
            return None
        checked_at = utc_datetime(now or datetime.now(timezone.utc), field="now")
        callback_started = False

        def guarded_callback(
            authorization: FrozenRankOneAuthorization,
        ) -> "_GuardResult":
            nonlocal callback_started
            callback_started = True
            return callback(authorization)

        with self._transaction():
            try:
                result = self._frozen_rank_one_locked(
                    ranking_snapshot_id,
                    candidate_id,
                    checked_at=checked_at,
                    policy_resolver=policy_resolver,
                    risk_authority_resolver=risk_authority_resolver,
                    execution_cost_contract=execution_cost_contract,
                    expected_hashes=None,
                    require_current_cost=True,
                    guard_callback=guarded_callback,
                )
            except Exception:
                if callback_started:
                    raise
                return None
            return result

    def _frozen_rank_one_locked(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        *,
        checked_at: datetime,
        policy_resolver: object,
        risk_authority_resolver: object,
        execution_cost_contract: object | None,
        expected_hashes: Mapping[str, object] | None,
        require_current_cost: bool,
        guard_callback: Callable[[FrozenRankOneAuthorization], object] | None = None,
    ) -> object | None:
        self.assert_integrity()
        snapshot = self._connection.execute(
            "SELECT * FROM ranking_snapshots WHERE ranking_snapshot_id=?",
            (ranking_snapshot_id,),
        ).fetchone()
        head = self._connection.execute(
            "SELECT ranking_snapshot_id FROM ranking_snapshots "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        if (
            snapshot is None
            or head is None
            or str(head[0]) != ranking_snapshot_id
            or datetime.fromisoformat(str(snapshot["valid_until"])) <= checked_at
        ):
            return None
        row = self._connection.execute(
            "SELECT * FROM ranking_rows "
            "WHERE ranking_snapshot_id=? AND candidate_id=?",
            (ranking_snapshot_id, candidate_id),
        ).fetchone()
        if (
            row is None
            or int(row["rank"]) != 1
            or int(row["authorizable"]) != 1
            or str(row["authority_status"]) == "A_GRADE_PENDING"
            or not self._current_terminal_matches_snapshot(snapshot)
        ):
            return None

        stored_expected = _expected_from_rows(snapshot, row)
        if expected_hashes is not None and any(
            key not in expected_hashes or expected_hashes[key] != value
            for key, value in stored_expected.items()
        ):
            return None
        policy = _resolve_once(policy_resolver, now=checked_at)
        cost: object | None = None
        if require_current_cost:
            if execution_cost_contract is None:
                return None
            cost = _resolve_cost_once(
                execution_cost_contract,
                now=checked_at,
                current_policy=policy,
                resolved_policy=policy,
            )
        risk = _resolve_once(
            risk_authority_resolver,
            now=checked_at,
            current_policy=policy,
            resolved_policy=policy,
            proposal_hash=str(row["proposal_hash"]),
            candidate_hash=str(row["candidate_hash"]),
            execution_cost_version=str(snapshot["cost_version"]),
            execution_cost_hash=str(snapshot["cost_hash"]),
            ranking_basis_hash=str(row["ranking_basis_hash"]),
        )
        if (
            not _policy_matches(snapshot, policy)
            or not _risk_binding_matches(snapshot, row, risk)
            or not _resolver_is_current(policy_resolver, policy)
            or not _resolver_is_current(risk_authority_resolver, risk)
            or (
                require_current_cost
                and (
                    not _cost_matches(snapshot, cost)
                    or not _resolver_is_current(execution_cost_contract, cost)
                )
            )
        ):
            return None
        candidate_body = json.loads(str(row["candidate_body_json"]))
        proposal_body = json.loads(str(row["proposal_body_json"]))
        if not isinstance(candidate_body, dict) or not isinstance(proposal_body, dict):
            raise RankingStoreCorruption("rank-one bodies are not canonical objects")
        authorization = FrozenRankOneAuthorization(
            ranking_snapshot_id=str(snapshot["ranking_snapshot_id"]),
            scan_run_id=str(snapshot["scan_run_id"]),
            candidate_id=str(row["candidate_id"]),
            proposal_hash=str(row["proposal_hash"]),
            candidate_hash=str(row["candidate_hash"]),
            rank=1,
            ranking_basis_hash=str(row["ranking_basis_hash"]),
            row_hash=str(row["row_hash"]),
            snapshot_hash=str(snapshot["snapshot_hash"]),
            current_policy_version=str(snapshot["current_policy_version"]),
            current_policy_hash=str(snapshot["current_policy_hash"]),
            policy_authority_marker_hash=str(
                snapshot["policy_authority_marker_hash"]
            ),
            cost_version=str(snapshot["cost_version"]),
            cost_hash=str(snapshot["cost_hash"]),
            risk_contract_hash=str(snapshot["risk_contract_hash"]),
            risk_authority_version=str(snapshot["risk_authority_version"]),
            risk_authority_marker_hash=str(
                snapshot["risk_authority_marker_hash"]
            ),
            candidate_body=candidate_body,
            proposal_body=proposal_body,
            expected_hashes=stored_expected,
        )
        if guard_callback is None:
            return authorization

        def invoke_callback() -> object:
            return guard_callback(authorization)

        def guard_cost() -> object | None:
            if not require_current_cost or cost is None:
                return invoke_callback()
            return _guard_current_resolution(
                execution_cost_contract,
                cost,
                invoke_callback,
            )

        def guard_risk() -> object | None:
            return _guard_current_resolution(
                risk_authority_resolver,
                risk,
                guard_cost,
            )

        return _guard_current_resolution(
            policy_resolver,
            policy,
            guard_risk,
        )

    def record_counts(self) -> dict[str, int]:
        self._ensure()
        return {
            table: int(
                self._connection.execute(
                    f"SELECT COUNT(*) FROM {_quote_identifier(table)}"
                ).fetchone()[0]
            )
            for table in (
                "ranking_snapshots",
                "ranking_bases",
                "ranking_rows",
                "ranking_decisions",
            )
        }

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self._ensure()
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
        self._ensure()
        origin = self._assert_migration_anchors()
        snapshots = self._connection.execute(
            "SELECT * FROM ranking_snapshots ORDER BY sequence"
        ).fetchall()
        previous = origin
        for expected_sequence, snapshot in enumerate(snapshots, start=1):
            if int(snapshot["sequence"]) != expected_sequence:
                raise RankingStoreCorruption("ranking snapshot sequence gap")
            snapshot_id = str(snapshot["ranking_snapshot_id"])
            identities = _snapshot_identities(snapshot)
            immutable_inputs = _canonical_json_column(
                snapshot, "immutable_inputs_json"
            )
            if not isinstance(immutable_inputs, dict):
                raise RankingStoreCorruption("snapshot immutable inputs are invalid")
            if canonical_hash(immutable_inputs) != str(
                snapshot["immutable_inputs_hash"]
            ):
                raise RankingStoreCorruption("snapshot immutable input hash mismatch")
            if any(
                immutable_inputs.get(name) != identities[name]
                for name in _SNAPSHOT_INPUT_FIELDS
            ):
                raise RankingStoreCorruption("snapshot immutable input mismatch")
            decision_bindings = _canonical_json_column(
                snapshot, "decision_records_json"
            )
            if not isinstance(decision_bindings, list) or any(
                not isinstance(item, dict) for item in decision_bindings
            ):
                raise RankingStoreCorruption("snapshot decision bindings are invalid")
            if canonical_hash(decision_bindings) != str(
                snapshot["decision_records_hash"]
            ):
                raise RankingStoreCorruption("snapshot decision binding hash mismatch")
            bases_raw = self._connection.execute(
                "SELECT * FROM ranking_bases WHERE ranking_snapshot_id=? "
                "ORDER BY rowid",
                (snapshot_id,),
            ).fetchall()
            rows_raw = self._connection.execute(
                "SELECT * FROM ranking_rows WHERE ranking_snapshot_id=? "
                "ORDER BY rank",
                (snapshot_id,),
            ).fetchall()
            if (
                len(rows_raw) > 10
                or [int(row["rank"]) for row in rows_raw]
                != list(range(1, len(rows_raw) + 1))
                or not bases_raw
            ):
                raise RankingStoreCorruption("invalid ranking rows")

            bases: list[dict[str, object]] = []
            bases_by_id: dict[str, dict[str, object]] = {}
            for base_row in bases_raw:
                base = _verified_basis_from_sql(
                    base_row, identities, immutable_inputs
                )
                candidate_key = str(base["candidate_id"])
                if candidate_key in bases_by_id:
                    raise RankingStoreCorruption("duplicate ranking candidate")
                bases.append(base)
                bases_by_id[candidate_key] = base

            rows: list[dict[str, object]] = []
            ranked_ids: set[str] = set()
            for row_raw in rows_raw:
                candidate_key = str(row_raw["candidate_id"])
                base = bases_by_id.get(candidate_key)
                if base is None:
                    raise RankingStoreCorruption("ranking row has no basis")
                row = _row_from_sql(row_raw)
                expected_row = _row_record(
                    base,
                    {"score_components": row["score_components"]},
                    int(row["rank"]),
                    identities,
                )
                if row != expected_row:
                    raise RankingStoreCorruption("ranking row hash mismatch")
                ranked_ids.add(candidate_key)
                rows.append(row)
            for base in bases:
                is_ranked = str(base["candidate_id"]) in ranked_ids
                if bool(base["authorizable"]) is not is_ranked:
                    raise RankingStoreCorruption(
                        "ranking authorizability and row membership disagree"
                    )
                if not is_ranked and base["authority_status"] != "A_GRADE_PENDING":
                    raise RankingStoreCorruption(
                        "non-authorizable evidence has invalid status"
                    )
            if str(snapshot["previous_snapshot_hash"]) != previous:
                raise RankingStoreCorruption("broken ranking chain")
            payload = _snapshot_payload(
                ranking_snapshot_id=snapshot_id,
                scan_run_id=str(snapshot["scan_run_id"]),
                identities=identities,
                immutable_inputs=immutable_inputs,
                immutable_inputs_json=str(snapshot["immutable_inputs_json"]),
                immutable_inputs_hash=str(snapshot["immutable_inputs_hash"]),
                decision_records=decision_bindings,
                decision_records_json=str(snapshot["decision_records_json"]),
                decision_records_hash=str(snapshot["decision_records_hash"]),
                created_at=str(snapshot["created_at"]),
                valid_until=str(snapshot["valid_until"]),
                previous_snapshot_hash=previous,
                bases=tuple(bases),
                rows=tuple(rows),
            )
            if canonical_hash(payload) != str(snapshot["snapshot_hash"]):
                raise RankingStoreCorruption("ranking snapshot hash mismatch")
            self._assert_snapshot_decision_batch(
                snapshot, decision_bindings, has_ranked_rows=bool(rows)
            )
            previous = str(snapshot["snapshot_hash"])

        decisions = self._connection.execute(
            "SELECT * FROM ranking_decisions ORDER BY sequence"
        ).fetchall()
        previous_decision = origin
        for expected_sequence, decision in enumerate(decisions, start=1):
            if int(decision["sequence"]) != expected_sequence:
                raise RankingStoreCorruption("ranking decision sequence gap")
            record = _canonical_json_column(decision, "record_json")
            identity_payload = {
                "schema": "options_copilot.ranking_decision_record.v2",
                "scan_run_id": str(decision["scan_run_id"]),
                "record_type": str(decision["record_type"]),
                "record": record,
            }
            if canonical_hash(identity_payload) != str(decision["record_hash"]):
                raise RankingStoreCorruption("ranking decision record mismatch")
            if str(decision["previous_decision_hash"]) != previous_decision:
                raise RankingStoreCorruption("broken ranking decision chain")
            payload = _decision_payload(
                sequence=expected_sequence,
                scan_run_id=str(decision["scan_run_id"]),
                record_type=str(decision["record_type"]),
                record=record,
                record_json=str(decision["record_json"]),
                record_hash=str(decision["record_hash"]),
                previous_decision_hash=previous_decision,
                recorded_at=str(decision["recorded_at"]),
            )
            if canonical_hash(payload) != str(decision["decision_hash"]):
                raise RankingStoreCorruption("ranking decision hash mismatch")
            previous_decision = str(decision["decision_hash"])

        if self._connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RankingStoreCorruption("SQLite integrity check failed")

    def _same_snapshot(
        self,
        existing: sqlite3.Row,
        identities: Mapping[str, str],
        immutable_json: str,
        immutable_hash: str,
        decision_records_json: str,
        decision_records_hash: str,
        valid_until: str,
        bases: Sequence[Mapping[str, object]],
        rows: Sequence[Mapping[str, object]],
    ) -> bool:
        if (
            any(str(existing[key]) != value for key, value in identities.items())
            or str(existing["immutable_inputs_json"]) != immutable_json
            or str(existing["immutable_inputs_hash"]) != immutable_hash
            or str(existing["decision_records_json"]) != decision_records_json
            or str(existing["decision_records_hash"]) != decision_records_hash
            or str(existing["valid_until"]) != valid_until
        ):
            return False
        stored_bases = self._connection.execute(
            "SELECT * FROM ranking_bases WHERE ranking_snapshot_id=? ORDER BY rowid",
            (existing["ranking_snapshot_id"],),
        ).fetchall()
        stored_rows = self._connection.execute(
            "SELECT * FROM ranking_rows WHERE ranking_snapshot_id=? ORDER BY rank",
            (existing["ranking_snapshot_id"],),
        ).fetchall()
        return (
            tuple(_basis_from_sql(item) for item in stored_bases) == tuple(bases)
            and tuple(_row_from_sql(item) for item in stored_rows) == tuple(rows)
        )

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError("ranking schema newer than supported")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                if version == SCHEMA_VERSION and self._schema_is_current():
                    self._install_triggers_in_transaction()
                    self._connection.execute("COMMIT")
                    return

                if version == 2 and self._schema_columns_are_current():
                    self._drop_ranking_triggers_in_transaction()
                    self._migrate_v2_rank_limit_in_transaction()
                    self._install_triggers_in_transaction()
                    self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                    self._connection.execute("COMMIT")
                    return

                self._drop_ranking_triggers_in_transaction()
                source_label = (
                    "v1"
                    if version == 1
                    else "v3_unbound"
                    if version == SCHEMA_VERSION
                    else "v0_unknown"
                )
                self._preserve_active_tables(source_label)
                self._create_schema_in_transaction()
                legacy_tables = self._legacy_table_names()
                if legacy_tables:
                    self._append_migration_anchor_in_transaction(
                        source_version=version,
                        source_label=source_label,
                        legacy_tables=legacy_tables,
                    )
                self._install_triggers_in_transaction()
                self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self._connection.execute("COMMIT")
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def _schema_is_current(self) -> bool:
        return (
            self._schema_columns_are_current()
            and self._ranking_rows_support_top10()
        )

    def _schema_columns_are_current(self) -> bool:
        required = {
            "ranking_snapshots": {
                "immutable_inputs_json",
                "immutable_inputs_hash",
                "decision_records_json",
                "decision_records_hash",
                "snapshot_hash",
            },
            "ranking_bases": {
                "candidate_body_json",
                "candidate_body_hash",
                "proposal_body_json",
                "proposal_body_hash",
                "basis_json",
            },
            "ranking_rows": {
                "candidate_body_json",
                "candidate_body_hash",
                "proposal_body_json",
                "proposal_body_hash",
                "row_hash",
            },
            "ranking_decisions": {
                "record_hash",
                "previous_decision_hash",
                "decision_hash",
            },
            "ranking_migration_anchors": {"anchor_hash", "legacy_json"},
        }
        return all(
            self._table_exists(table)
            and columns <= self._table_columns(table)
            for table, columns in required.items()
        )

    def _ranking_rows_support_top10(self) -> bool:
        row = self._connection.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type='table' AND name='ranking_rows'"
        ).fetchone()
        if row is None or not isinstance(row[0], str):
            return False
        normalized = " ".join(str(row[0]).upper().split())
        return "CHECK(RANK BETWEEN 1 AND 10)" in normalized

    def _migrate_v2_rank_limit_in_transaction(self) -> None:
        """Widen only the rank check while preserving every signed row byte."""

        self._connection.execute(
            "ALTER TABLE ranking_rows RENAME TO ranking_rows_v2_top3"
        )
        self._connection.execute(
            """
            CREATE TABLE ranking_rows(
                ranking_snapshot_id TEXT NOT NULL,
                rank INTEGER NOT NULL CHECK(rank BETWEEN 1 AND 10),
                candidate_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                candidate_body_json TEXT NOT NULL,
                candidate_body_hash TEXT NOT NULL,
                proposal_body_json TEXT NOT NULL,
                proposal_body_hash TEXT NOT NULL,
                ranking_basis_hash TEXT NOT NULL,
                authority_status TEXT NOT NULL,
                authorizable INTEGER NOT NULL CHECK(authorizable=1),
                risk_authority_marker_hash TEXT NOT NULL,
                score_json TEXT NOT NULL,
                score_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                PRIMARY KEY(ranking_snapshot_id,rank),
                UNIQUE(ranking_snapshot_id,candidate_id),
                FOREIGN KEY(ranking_snapshot_id,candidate_id)
                    REFERENCES ranking_bases(ranking_snapshot_id,candidate_id)
            )
            """
        )
        columns = (
            "ranking_snapshot_id,rank,candidate_id,proposal_hash,candidate_hash,"
            "candidate_body_json,candidate_body_hash,proposal_body_json,"
            "proposal_body_hash,ranking_basis_hash,authority_status,authorizable,"
            "risk_authority_marker_hash,score_json,score_hash,row_hash"
        )
        self._connection.execute(
            f"INSERT INTO ranking_rows({columns}) "
            f"SELECT {columns} FROM ranking_rows_v2_top3 ORDER BY rank"
        )
        self._connection.execute("DROP TABLE ranking_rows_v2_top3")

    def _create_schema_in_transaction(self) -> None:
        statements = (
            """
            CREATE TABLE ranking_migration_anchors(
                sequence INTEGER PRIMARY KEY,
                source_version INTEGER NOT NULL,
                source_label TEXT NOT NULL,
                legacy_tables_json TEXT NOT NULL,
                legacy_json TEXT NOT NULL,
                legacy_hash TEXT NOT NULL,
                previous_anchor_hash TEXT NOT NULL,
                anchor_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE ranking_snapshots(
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                ranking_snapshot_id TEXT NOT NULL UNIQUE,
                scan_run_id TEXT NOT NULL UNIQUE,
                input_hash TEXT NOT NULL,
                evidence_hash TEXT NOT NULL,
                broker_snapshot_hash TEXT NOT NULL,
                immutable_inputs_json TEXT NOT NULL,
                immutable_inputs_hash TEXT NOT NULL,
                decision_records_json TEXT NOT NULL,
                decision_records_hash TEXT NOT NULL,
                current_policy_version TEXT NOT NULL,
                current_policy_hash TEXT NOT NULL,
                policy_authority_marker_hash TEXT NOT NULL,
                cost_version TEXT NOT NULL,
                cost_hash TEXT NOT NULL,
                risk_contract_hash TEXT NOT NULL,
                risk_authority_version TEXT NOT NULL,
                risk_authority_marker_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                valid_until TEXT NOT NULL,
                previous_snapshot_hash TEXT NOT NULL,
                snapshot_hash TEXT NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE ranking_bases(
                ranking_snapshot_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                candidate_body_json TEXT NOT NULL,
                candidate_body_hash TEXT NOT NULL,
                proposal_body_json TEXT NOT NULL,
                proposal_body_hash TEXT NOT NULL,
                ranking_basis_hash TEXT NOT NULL,
                evidence_inputs_json TEXT NOT NULL,
                evidence_inputs_hash TEXT NOT NULL,
                authority_status TEXT NOT NULL,
                authorizable INTEGER NOT NULL CHECK(authorizable IN (0,1)),
                basis_json TEXT NOT NULL,
                basis_content_hash TEXT NOT NULL,
                PRIMARY KEY(ranking_snapshot_id,candidate_id),
                FOREIGN KEY(ranking_snapshot_id)
                    REFERENCES ranking_snapshots(ranking_snapshot_id)
            )
            """,
            """
            CREATE TABLE ranking_rows(
                ranking_snapshot_id TEXT NOT NULL,
                rank INTEGER NOT NULL CHECK(rank BETWEEN 1 AND 10),
                candidate_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                candidate_body_json TEXT NOT NULL,
                candidate_body_hash TEXT NOT NULL,
                proposal_body_json TEXT NOT NULL,
                proposal_body_hash TEXT NOT NULL,
                ranking_basis_hash TEXT NOT NULL,
                authority_status TEXT NOT NULL,
                authorizable INTEGER NOT NULL CHECK(authorizable=1),
                risk_authority_marker_hash TEXT NOT NULL,
                score_json TEXT NOT NULL,
                score_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                PRIMARY KEY(ranking_snapshot_id,rank),
                UNIQUE(ranking_snapshot_id,candidate_id),
                FOREIGN KEY(ranking_snapshot_id,candidate_id)
                    REFERENCES ranking_bases(ranking_snapshot_id,candidate_id)
            )
            """,
            """
            CREATE TABLE ranking_decisions(
                sequence INTEGER PRIMARY KEY,
                scan_run_id TEXT NOT NULL,
                record_type TEXT NOT NULL,
                record_json TEXT NOT NULL,
                record_hash TEXT NOT NULL UNIQUE,
                previous_decision_hash TEXT NOT NULL,
                decision_hash TEXT NOT NULL UNIQUE,
                recorded_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX ranking_decisions_scan_idx "
            "ON ranking_decisions(scan_run_id,sequence)",
        )
        for statement in statements:
            self._connection.execute(statement)

    def _install_triggers_in_transaction(self) -> None:
        for table in _ACTIVE_TABLES:
            if not self._table_exists(table):
                continue
            for operation in ("update", "delete"):
                trigger = f"{table}_no_{operation}"
                self._connection.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {_quote_identifier(trigger)} "
                    f"BEFORE {operation.upper()} ON {_quote_identifier(table)} "
                    "BEGIN SELECT RAISE(ABORT,'immutable ranking ledger'); END"
                )
        for index, table in enumerate(self._legacy_table_names(), start=1):
            for operation in ("update", "delete", "insert"):
                trigger = f"ranking_legacy_{index}_no_{operation}"
                self._connection.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {_quote_identifier(trigger)} "
                    f"BEFORE {operation.upper()} ON {_quote_identifier(table)} "
                    "BEGIN SELECT RAISE(ABORT,'immutable legacy ranking ledger'); END"
                )

    def _drop_ranking_triggers_in_transaction(self) -> None:
        rows = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name LIKE 'ranking_%'"
        ).fetchall()
        for row in rows:
            self._connection.execute(
                f"DROP TRIGGER {_quote_identifier(str(row[0]))}"
            )

    def _preserve_active_tables(self, label: str) -> None:
        for table in _ACTIVE_TABLES:
            if not self._table_exists(table):
                continue
            target = f"{table}_legacy_{label}"
            suffix = 1
            while self._table_exists(target):
                suffix += 1
                target = f"{table}_legacy_{label}_{suffix}"
            self._connection.execute(
                f"ALTER TABLE {_quote_identifier(table)} "
                f"RENAME TO {_quote_identifier(target)}"
            )

    def _append_migration_anchor_in_transaction(
        self,
        *,
        source_version: int,
        source_label: str,
        legacy_tables: Sequence[str],
    ) -> None:
        bundle = self._legacy_bundle(legacy_tables)
        legacy_json = canonical_json(bundle)
        legacy_hash = canonical_hash(bundle)
        tail = self._connection.execute(
            "SELECT sequence, anchor_hash FROM ranking_migration_anchors "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        sequence = 1 if tail is None else int(tail["sequence"]) + 1
        previous = GENESIS_HASH if tail is None else str(tail["anchor_hash"])
        created_at = datetime_text(datetime.now(timezone.utc))
        names_json = canonical_json(tuple(sorted(legacy_tables)))
        anchor_payload = _migration_anchor_payload(
            sequence=sequence,
            source_version=source_version,
            source_label=source_label,
            legacy_tables_json=names_json,
            legacy_json=legacy_json,
            legacy_hash=legacy_hash,
            previous_anchor_hash=previous,
            created_at=created_at,
        )
        anchor_hash = canonical_hash(anchor_payload)
        self._connection.execute(
            """
            INSERT INTO ranking_migration_anchors(
                sequence, source_version, source_label, legacy_tables_json,
                legacy_json, legacy_hash, previous_anchor_hash, anchor_hash,
                created_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                sequence,
                source_version,
                source_label,
                names_json,
                legacy_json,
                legacy_hash,
                previous,
                anchor_hash,
                created_at,
            ),
        )

    def _assert_migration_anchors(self) -> str:
        anchors = self._connection.execute(
            "SELECT * FROM ranking_migration_anchors ORDER BY sequence"
        ).fetchall()
        previous = GENESIS_HASH
        for expected_sequence, anchor in enumerate(anchors, start=1):
            if int(anchor["sequence"]) != expected_sequence:
                raise RankingStoreCorruption("migration anchor sequence gap")
            names = _canonical_json_column(anchor, "legacy_tables_json")
            if not isinstance(names, list) or not all(
                isinstance(name, str) for name in names
            ):
                raise RankingStoreCorruption("migration anchor table list invalid")
            actual_bundle = self._legacy_bundle(tuple(names))
            actual_json = canonical_json(actual_bundle)
            if actual_json != str(anchor["legacy_json"]):
                raise RankingStoreCorruption("legacy ranking history mismatch")
            if canonical_hash(actual_bundle) != str(anchor["legacy_hash"]):
                raise RankingStoreCorruption("legacy ranking history hash mismatch")
            if str(anchor["previous_anchor_hash"]) != previous:
                raise RankingStoreCorruption("migration anchor chain broken")
            payload = _migration_anchor_payload(
                sequence=expected_sequence,
                source_version=int(anchor["source_version"]),
                source_label=str(anchor["source_label"]),
                legacy_tables_json=str(anchor["legacy_tables_json"]),
                legacy_json=str(anchor["legacy_json"]),
                legacy_hash=str(anchor["legacy_hash"]),
                previous_anchor_hash=previous,
                created_at=str(anchor["created_at"]),
            )
            if canonical_hash(payload) != str(anchor["anchor_hash"]):
                raise RankingStoreCorruption("migration anchor hash mismatch")
            previous = str(anchor["anchor_hash"])
        return previous

    def _assert_snapshot_decision_batch(
        self,
        snapshot: sqlite3.Row,
        decision_bindings: Sequence[Mapping[str, object]],
        *,
        has_ranked_rows: bool,
    ) -> None:
        if not has_ranked_rows:
            if decision_bindings:
                raise RankingStoreCorruption(
                    "governance snapshot carries finalized scenario decisions"
                )
            return
        prepared: list[_PreparedDecision] = []
        try:
            for binding in decision_bindings:
                item = _prepare_decision(
                    scan_run_id=str(binding["scan_run_id"]),
                    record_type=str(binding["record_type"]),
                    record=binding["record"],
                    recorded_at=str(snapshot["created_at"]),
                )
                if item.record_type not in {
                    "SCENARIO",
                    "GATE_BUNDLE_TRADE",
                } or _decision_binding(item) != dict(binding):
                    raise RankingStoreCorruption(
                        "snapshot scenario decision binding mismatch"
                    )
                prepared.append(item)
            kinds = tuple(item.record_type for item in prepared)
            if kinds and not (
                all(kind == "SCENARIO" for kind in kinds)
                or (
                    kinds[-1] == "GATE_BUNDLE_TRADE"
                    and all(kind == "SCENARIO" for kind in kinds[:-1])
                )
            ):
                raise RankingStoreCorruption(
                    "snapshot Gate bundle decision order mismatch"
                )
            gate_records = tuple(
                item for item in prepared if item.record_type == "GATE_BUNDLE_TRADE"
            )
            if gate_records:
                immutable_inputs = json.loads(str(snapshot["immutable_inputs_json"]))
                expected_gate_hash = _hash(immutable_inputs.get("gate_bundle_hash"))
                _assert_gate_bundle_record(
                    gate_records[0],
                    expected_outcome="TRADE",
                    expected_hash=expected_gate_hash,
                )
            terminal = _ranking_terminal_decision(
                scan_run_id=str(snapshot["scan_run_id"]),
                ranking_snapshot_id=str(snapshot["ranking_snapshot_id"]),
                snapshot_hash=str(snapshot["snapshot_hash"]),
                recorded_at=str(snapshot["created_at"]),
            )
            expected = tuple(prepared) + (terminal,)
            rows = [
                self._connection.execute(
                    "SELECT * FROM ranking_decisions WHERE record_hash=?",
                    (item.record_hash,),
                ).fetchone()
                for item in expected
            ]
            if any(row is None for row in rows):
                raise RankingStoreCorruption(
                    "snapshot decision batch is incomplete"
                )
            present = [row for row in rows if row is not None]
            sequences = [int(row["sequence"]) for row in present]
            if sequences != list(
                range(sequences[0], sequences[0] + len(sequences))
            ):
                raise RankingStoreCorruption(
                    "snapshot decision batch is not contiguous"
                )
            for item, row in zip(expected, present):
                if (
                    str(row["scan_run_id"]) != item.scan_run_id
                    or str(row["record_type"]) != item.record_type
                    or str(row["record_json"]) != item.record_json
                ):
                    raise RankingStoreCorruption(
                        "snapshot decision row mismatch"
                    )
        except RankingStoreCorruption:
            raise
        except Exception as exc:
            raise RankingStoreCorruption(
                "invalid snapshot decision binding"
            ) from exc

    def _current_terminal_matches_snapshot(self, snapshot: sqlite3.Row) -> bool:
        terminal = self._connection.execute(
            "SELECT * FROM ranking_decisions ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        if terminal is None:
            return False
        expected = _ranking_terminal_decision(
            scan_run_id=str(snapshot["scan_run_id"]),
            ranking_snapshot_id=str(snapshot["ranking_snapshot_id"]),
            snapshot_hash=str(snapshot["snapshot_hash"]),
            recorded_at=str(snapshot["created_at"]),
        )
        return (
            str(terminal["record_type"]) == "TRADE"
            and str(terminal["record_hash"]) == expected.record_hash
            and str(terminal["record_json"]) == expected.record_json
        )

    def _legacy_bundle(self, table_names: Sequence[str]) -> dict[str, object]:
        tables: list[dict[str, object]] = []
        for name in sorted(table_names):
            if not self._table_exists(name):
                raise RankingStoreCorruption(f"legacy table is missing: {name}")
            schema_row = self._connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            ).fetchone()
            columns = [
                str(row[1])
                for row in self._connection.execute(
                    f"PRAGMA table_info({_quote_identifier(name)})"
                ).fetchall()
            ]
            raw_rows = self._connection.execute(
                f"SELECT * FROM {_quote_identifier(name)}"
            ).fetchall()
            documents = [
                {
                    column: _sqlite_json_value(row[column])
                    for column in columns
                }
                for row in raw_rows
            ]
            documents.sort(key=canonical_json)
            tables.append(
                {
                    "name": name,
                    "schema_sql": None if schema_row is None else schema_row[0],
                    "columns": columns,
                    "rows": documents,
                }
            )
        return {
            "schema": "options_copilot.ranking_migration_bundle.v1",
            "tables": tables,
        }

    def _legacy_table_names(self) -> tuple[str, ...]:
        rows = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'ranking_%_legacy_%' ORDER BY name"
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def _table_exists(self, table: str) -> bool:
        return self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None

    def _table_columns(self, table: str) -> set[str]:
        return {
            str(row[1])
            for row in self._connection.execute(
                f"PRAGMA table_info({_quote_identifier(table)})"
            ).fetchall()
        }

    def _chain_origin(self) -> str:
        row = self._connection.execute(
            "SELECT anchor_hash FROM ranking_migration_anchors "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return GENESIS_HASH if row is None else str(row[0])

    class _Transaction:
        def __init__(self, outer: "RankingStore") -> None:
            self.outer = outer

        def __enter__(self) -> None:
            self.outer._ensure()
            self.outer._lock.acquire()
            try:
                self.outer._connection.execute("BEGIN IMMEDIATE")
            except BaseException:
                self.outer._lock.release()
                raise

        def __exit__(self, exc_type: object, *_: object) -> None:
            try:
                self.outer._connection.execute("ROLLBACK" if exc_type else "COMMIT")
            finally:
                self.outer._lock.release()

    def _transaction(self) -> "RankingStore._Transaction":
        return self._Transaction(self)

    def _ensure(self) -> None:
        if self._closed:
            raise RuntimeError("ranking store is closed")

    def _stored_snapshot(self, row: sqlite3.Row) -> StoredRankingSnapshot:
        rank_one = self._connection.execute(
            "SELECT * FROM ranking_rows WHERE ranking_snapshot_id=? AND rank=1",
            (row["ranking_snapshot_id"],),
        ).fetchone()
        identities = _snapshot_identities(row)
        immutable_hash = str(row["immutable_inputs_hash"])
        expected = (
            _expected_hashes(
                identities,
                _row_from_sql(rank_one),
                str(row["snapshot_hash"]),
                immutable_hash,
            )
            if rank_one is not None
            else _snapshot_expected_hashes(
                identities, str(row["snapshot_hash"]), immutable_hash
            )
        )
        return StoredRankingSnapshot(
            ranking_snapshot_id=str(row["ranking_snapshot_id"]),
            scan_run_id=str(row["scan_run_id"]),
            snapshot_hash=str(row["snapshot_hash"]),
            previous_snapshot_hash=str(row["previous_snapshot_hash"]),
            valid_until=datetime.fromisoformat(str(row["valid_until"])),
            expected_hashes=expected,
        )


def _snapshot_immutable_inputs(
    raw_items: Sequence[Mapping[str, Any] | object],
    identities: Mapping[str, str],
) -> dict[str, Any]:
    normalized: dict[str, Any] | None = None
    rendered: str | None = None
    for raw in raw_items:
        value = _document(raw).get("evidence_inputs")
        if not isinstance(value, Mapping) or not value:
            raise ValueError("every ranking row requires immutable evidence_inputs")
        current_json = canonical_json(value)
        current = json.loads(current_json)
        if not isinstance(current, dict):
            raise ValueError("evidence_inputs must be a canonical mapping")
        if any(current.get(name) != identities[name] for name in _SNAPSHOT_INPUT_FIELDS):
            raise ValueError("candidate evidence_inputs disagree with snapshot inputs")
        if rendered is None:
            rendered, normalized = current_json, current
        elif current_json != rendered:
            raise ValueError("ranking rows must share identical immutable evidence_inputs")
    assert normalized is not None
    return normalized


def _ranking_outcome_target(row: sqlite3.Row) -> Mapping[str, object]:
    candidate_body = thaw_json(json.loads(str(row["candidate_body_json"])))
    if not isinstance(candidate_body, Mapping):
        raise RankingStoreCorruption("ranked candidate body is not a mapping")
    symbol = _ranking_outcome_symbol(candidate_body)
    ranking_snapshot_id = str(row["ranking_snapshot_id"])
    ranking_snapshot_hash = str(row["snapshot_hash"])
    candidate_id = str(row["candidate_id"])
    candidate_hash = str(row["candidate_hash"])
    ranking_basis_hash = str(row["ranking_basis_hash"])
    rank = int(row["rank"])
    row_hash = str(row["row_hash"])
    subject_id = f"{ranking_snapshot_id}:{candidate_id}"
    subject_hash = canonical_hash(
        {
            "schema": "options_copilot.candidate_outcome_subject.v1",
            "ranking_snapshot_id": ranking_snapshot_id,
            "ranking_snapshot_hash": ranking_snapshot_hash,
            "candidate_id": candidate_id,
            "candidate_hash": candidate_hash,
            "ranking_basis_hash": ranking_basis_hash,
            "rank": rank,
            "row_hash": row_hash,
        }
    )
    decision_at = datetime.fromisoformat(str(row["created_at"]))
    decision_hash = canonical_hash(
        {
            "schema": "options_copilot.ranking_outcome_decision.v1",
            "scan_run_id": str(row["scan_run_id"]),
            "ranking_snapshot_id": ranking_snapshot_id,
            "ranking_snapshot_hash": ranking_snapshot_hash,
            "candidate_id": candidate_id,
            "candidate_hash": candidate_hash,
            "ranking_basis_hash": ranking_basis_hash,
            "rank": rank,
            "row_hash": row_hash,
        }
    )
    exit_plan = candidate_body.get("exit_plan")
    exit_policy_hash = (
        canonical_hash(exit_plan) if isinstance(exit_plan, Mapping) else None
    )
    thesis_hash_raw = candidate_body.get("equity_thesis_hash")
    thesis_hash = (
        str(thesis_hash_raw)
        if isinstance(thesis_hash_raw, str)
        and len(thesis_hash_raw) == 64
        and all(character in "0123456789abcdef" for character in thesis_hash_raw)
        else None
    )
    legs = candidate_body.get("legs")
    quote_identity_hash = (
        canonical_hash({
            "schema": "options_copilot.candidate_quote_identity.v1",
            "broker_snapshot_hash": str(row["broker_snapshot_hash"]),
            "secdef_hash": candidate_body.get("secdef_hash"),
            "legs": legs,
        })
        if isinstance(legs, list) and legs
        else None
    )
    position_management_hash = canonical_hash({
        "schema": "options_copilot.position_management_binding.v1",
        "candidate_hash": candidate_hash,
        "exit_policy_hash": exit_policy_hash,
        "status": "BOUND" if exit_policy_hash is not None else "UNAVAILABLE",
    })
    counterfactual_spec_hash = canonical_hash({
        "schema": "options_copilot.outcome_counterfactual_spec.v1",
        "candidate_hash": candidate_hash,
        "horizons": ("30M", "SESSION_CLOSE", "1D", "3D", "5D"),
        "paths": ("FOLLOW_EXIT_POLICY", "HOLD_TO_HORIZON"),
        "decision_authority": "SUPPORTING_ONLY",
    })
    result_authority = _ranking_result_authority(
        candidate_body,
        candidate_hash=candidate_hash,
        cost_contract_hash=str(row["cost_hash"]),
    )
    target = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "CANDIDATE",
        "subject_id": subject_id,
        "subject_hash": subject_hash,
        "symbol": symbol,
        "occurred_at": decision_at,
        "thesis_hash": thesis_hash,
        "source_sequence": int(row["snapshot_sequence"]) * 10 + rank,
        "binding_context": {
            "source_sequence": int(row["snapshot_sequence"]) * 10 + rank,
            "scan_run_id": str(row["scan_run_id"]),
            "ranking_snapshot_id": ranking_snapshot_id,
            "ranking_snapshot_hash": ranking_snapshot_hash,
            "candidate_id": candidate_id,
            "candidate_hash": candidate_hash,
            "ranking_basis_hash": ranking_basis_hash,
            "rank": rank,
            "event_ids": _ranking_outcome_event_ids(candidate_body),
            "thesis_hash": thesis_hash,
            "quote_identity_hash": quote_identity_hash,
            "position_management_hash": position_management_hash,
            "counterfactual_spec_hash": counterfactual_spec_hash,
        },
        "capture_plan": _ranking_capture_plan(candidate_body),
        "outcome_template": {
            "decision_id": f"ranking:{ranking_snapshot_id}:{candidate_id}",
            "decision_hash": decision_hash,
            "candidate_id": candidate_id,
            "candidate_hash": candidate_hash,
            "outcome_subject_id": subject_id,
            "outcome_subject_hash": subject_hash,
            "ranking_snapshot_id": ranking_snapshot_id,
            "ranking_snapshot_hash": ranking_snapshot_hash,
            "ranking_basis_hash": ranking_basis_hash,
            "decision_at": decision_at,
            "input_hash": str(row["input_hash"]),
            "evidence_hash": str(row["evidence_hash"]),
            "broker_snapshot_hash": str(row["broker_snapshot_hash"]),
            "current_policy_version": str(row["current_policy_version"]),
            "current_policy_hash": str(row["current_policy_hash"]),
            "policy_authority_marker_hash": str(
                row["policy_authority_marker_hash"]
            ),
            "cost_version": str(row["cost_version"]),
            "cost_hash": str(row["cost_hash"]),
            "exit_policy_hash": exit_policy_hash,
            "thesis_hash": thesis_hash,
            "quote_identity_hash": quote_identity_hash,
            "position_management_hash": position_management_hash,
            "counterfactual_spec_hash": counterfactual_spec_hash,
            "quote_quality": {
                "status": "MISSING",
                "bid_ask_complete": False,
                "source": "OUTCOME_EVIDENCE_UNAVAILABLE",
            },
            "cluster_evidence": {
                "status": "KNOWN",
                "ticker": symbol,
                "issuer_id": f"issuer-{symbol}",
                "provider": "RANKING_LEDGER",
                "event_id": str(row["scan_run_id"]),
                "slot_at": decision_at,
            },
        },
    }
    if result_authority is not None:
        target["result_authority"] = result_authority
    frozen = freeze_json(target)
    assert isinstance(frozen, Mapping)
    return frozen


def _ranking_result_authority(
    candidate_body: Mapping[str, object],
    *,
    candidate_hash: str,
    cost_contract_hash: str,
) -> Mapping[str, object] | None:
    legs_raw = candidate_body.get("legs")
    if not isinstance(legs_raw, Sequence) or isinstance(
        legs_raw, (str, bytes, bytearray, memoryview)
    ) or not legs_raw:
        return None
    legs: list[Mapping[str, object]] = []
    for raw in legs_raw:
        if not isinstance(raw, Mapping):
            return None
        contract_id = raw.get("contract_id_ex", raw.get("con_id"))
        side = str(raw.get("side") or "").strip().upper()
        if side == "LONG":
            side = "BUY"
        elif side == "SHORT":
            side = "SELL"
        quantity = raw.get("quantity", raw.get("ratio"))
        if (
            contract_id in (None, "")
            or side not in {"BUY", "SELL"}
            or not isinstance(quantity, int)
            or isinstance(quantity, bool)
            or quantity <= 0
        ):
            return None
        legs.append(
            {
                "contract_id": str(contract_id),
                "side": side,
                "quantity": quantity,
            }
        )
    try:
        entry_value = Decimal(str(candidate_body.get("debit_usd"))) - Decimal(
            str(candidate_body.get("credit_usd"))
        )
        costs = Decimal(
            str(candidate_body.get("estimated_commissions_usd"))
        ) + Decimal(str(candidate_body.get("estimated_slippage_usd")))
        maximum_loss = Decimal(str(candidate_body.get("max_loss_usd")))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not all(value.is_finite() for value in (entry_value, costs, maximum_loss)):
        return None
    if costs < 0 or maximum_loss < 0:
        return None
    costs_document = {
        "schema": "options_copilot.outcome_cost_evidence.v1",
        "candidate_hash": candidate_hash,
        "cost_contract_hash": cost_contract_hash,
        "costs_usd": costs,
    }
    maximum_loss_document = {
        "schema": "options_copilot.outcome_max_loss_evidence.v1",
        "candidate_hash": candidate_hash,
        "max_loss_usd": maximum_loss,
    }
    body = {
        "schema": "options_copilot.outcome_result_authority.v1",
        "candidate_hash": candidate_hash,
        "cost_contract_hash": cost_contract_hash,
        "legs": tuple(legs),
        "entry_value_usd": entry_value,
        "costs_usd": costs,
        "costs_hash": canonical_hash(costs_document),
        "max_loss_usd": maximum_loss,
        "max_loss_evidence_hash": canonical_hash(maximum_loss_document),
    }
    frozen = freeze_json({**body, "authority_hash": canonical_hash(body)})
    assert isinstance(frozen, Mapping)
    return frozen


def _ranking_outcome_event_ids(
    candidate_body: Mapping[str, object],
) -> tuple[str, ...]:
    values: set[str] = set()
    for name in ("event_id", "catalyst_event_id"):
        value = candidate_body.get(name)
        if isinstance(value, str) and value.strip():
            values.add(value.strip())
    raw = candidate_body.get("event_ids")
    if isinstance(raw, Sequence) and not isinstance(
        raw,
        (str, bytes, bytearray, memoryview),
    ):
        values.update(str(item).strip() for item in raw if str(item).strip())
    return tuple(sorted(values))


def _ranking_capture_plan(
    candidate_body: Mapping[str, object],
) -> Mapping[str, object]:
    value = candidate_body.get("outcome_capture_plan")
    if (
        isinstance(value, Mapping)
        and value.get("schema") == "options_copilot.outcome_capture_plan.v1"
        and value.get("status") in {"READY", "BLOCKED"}
    ):
        frozen = freeze_json(value)
        assert isinstance(frozen, Mapping)
        return frozen
    frozen = freeze_json(
        {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "BLOCKED",
            "reason_codes": ("OUTCOME_CAPTURE_BASELINE_UNAVAILABLE",),
        }
    )
    assert isinstance(frozen, Mapping)
    return frozen


def _ranking_outcome_symbol(candidate_body: Mapping[str, object]) -> str:
    raw = candidate_body.get("symbol", candidate_body.get("underlying"))
    if not isinstance(raw, str) or not raw.strip():
        raise RankingStoreCorruption("ranked candidate outcome symbol is missing")
    return raw.strip().upper()


def _basis_record(
    raw: Mapping[str, Any] | object,
    identities: Mapping[str, str],
    immutable_inputs: Mapping[str, object],
    *,
    authorizable_required: bool,
) -> dict[str, object]:
    doc = _document(raw)
    candidate_id = _identity(doc.get("candidate_id"))
    candidate_body = doc.get("candidate_body")
    proposal_body = doc.get("proposal_body")
    if not isinstance(candidate_body, Mapping):
        raise ValueError("ranked candidates require canonical candidate_body")
    if proposal_body is not None and not isinstance(proposal_body, Mapping):
        raise ValueError("proposal_body must be a canonical mapping")
    binding = build_ranking_basis(
        candidate_body=candidate_body,
        proposal_body=proposal_body,
        candidate_hash=_hash(doc.get("candidate_hash")),
        proposal_hash=_hash(doc.get("proposal_hash")),
        current_policy_version=identities["current_policy_version"],
        current_policy_hash=identities["current_policy_hash"],
        policy_authority_marker_hash=identities[
            "policy_authority_marker_hash"
        ],
        cost_version=identities["cost_version"],
        cost_hash=identities["cost_hash"],
        risk_contract_hash=identities["risk_contract_hash"],
        evidence_inputs=immutable_inputs,
    )
    body_candidate_id = binding.candidate_body.get("candidate_id")
    if body_candidate_id != candidate_id:
        raise ValueError("candidate_id disagrees with canonical candidate_body")
    authority_status = _identity(doc.get("authority_status", "NORMAL"))
    authorizable = doc.get("authorizable", True) is True
    if authority_status not in {"NORMAL", "A_GRADE", "A_GRADE_PENDING"}:
        raise ValueError("invalid ranking authority status")
    if authorizable_required and (
        not authorizable or authority_status == "A_GRADE_PENDING"
    ):
        raise ValueError("only authorizable candidates may receive a rank")
    if not authorizable_required and (
        authorizable or authority_status != "A_GRADE_PENDING"
    ):
        raise ValueError("governance evidence must be A_GRADE_PENDING")
    supplied_basis = doc.get("ranking_basis_hash")
    if (
        supplied_basis is not None
        and _hash(supplied_basis) != binding.ranking_basis_hash
    ):
        raise ValueError("ranking basis does not match canonical immutable bindings")
    return {
        "candidate_id": candidate_id,
        "proposal_hash": binding.proposal_hash,
        "candidate_hash": binding.candidate_hash,
        "candidate_body": dict(binding.candidate_body),
        "candidate_body_json": binding.candidate_body_json,
        "candidate_body_hash": binding.candidate_hash,
        "proposal_body": dict(binding.proposal_body),
        "proposal_body_json": binding.proposal_body_json,
        "proposal_body_hash": binding.proposal_hash,
        "ranking_basis_hash": binding.ranking_basis_hash,
        "evidence_inputs": dict(binding.evidence_inputs),
        "evidence_inputs_json": binding.evidence_inputs_json,
        "evidence_inputs_hash": binding.evidence_inputs_hash,
        "authority_status": authority_status,
        "authorizable": authorizable,
        "basis_json": binding.basis_json,
        "basis_content_hash": canonical_hash(binding.basis_json),
    }


def _row_record(
    base: Mapping[str, object],
    raw: Mapping[str, Any] | object,
    rank: int,
    identities: Mapping[str, str],
) -> dict[str, object]:
    doc = _document(raw)
    score_json = canonical_json(doc.get("score_components", {}))
    score_components = json.loads(score_json)
    payload = {
        "schema": "options_copilot.ranking_row.v2",
        "rank": rank,
        "candidate_id": base["candidate_id"],
        "proposal_hash": base["proposal_hash"],
        "candidate_hash": base["candidate_hash"],
        "candidate_body": base["candidate_body"],
        "candidate_body_json": base["candidate_body_json"],
        "candidate_body_hash": base["candidate_body_hash"],
        "proposal_body": base["proposal_body"],
        "proposal_body_json": base["proposal_body_json"],
        "proposal_body_hash": base["proposal_body_hash"],
        "ranking_basis_hash": base["ranking_basis_hash"],
        "authority_status": base["authority_status"],
        "authorizable": True,
        "risk_authority_marker_hash": identities[
            "risk_authority_marker_hash"
        ],
        "score_components": score_components,
        "score_json": score_json,
    }
    return {
        **payload,
        "score_hash": canonical_hash(score_components),
        "row_hash": canonical_hash(payload),
    }


def _snapshot_payload(
    *,
    ranking_snapshot_id: str,
    scan_run_id: str,
    identities: Mapping[str, str],
    immutable_inputs: Mapping[str, object],
    immutable_inputs_json: str,
    immutable_inputs_hash: str,
    decision_records: Sequence[Mapping[str, object]],
    decision_records_json: str,
    decision_records_hash: str,
    created_at: str,
    valid_until: str,
    previous_snapshot_hash: str,
    bases: Sequence[Mapping[str, object]],
    rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return {
        "schema": "options_copilot.ranking_snapshot.v2",
        "ranking_snapshot_id": ranking_snapshot_id,
        "scan_run_id": scan_run_id,
        **dict(identities),
        "immutable_inputs": dict(immutable_inputs),
        "immutable_inputs_json": immutable_inputs_json,
        "immutable_inputs_hash": immutable_inputs_hash,
        "decision_records": list(decision_records),
        "decision_records_json": decision_records_json,
        "decision_records_hash": decision_records_hash,
        "created_at": created_at,
        "valid_until": valid_until,
        "previous_snapshot_hash": previous_snapshot_hash,
        "bases": list(bases),
        "rows": list(rows),
    }


def _decision_payload(
    *,
    sequence: int,
    scan_run_id: str,
    record_type: str,
    record: object,
    record_json: str,
    record_hash: str,
    previous_decision_hash: str,
    recorded_at: str,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.ranking_decision_chain.v2",
        "sequence": sequence,
        "scan_run_id": scan_run_id,
        "record_type": record_type,
        "record": record,
        "record_json": record_json,
        "record_hash": record_hash,
        "previous_decision_hash": previous_decision_hash,
        "recorded_at": recorded_at,
    }


def _prepare_decision_records(
    records: Sequence[Mapping[str, Any] | object],
    *,
    default_scan_run_id: str,
    recorded_at: str,
) -> tuple[_PreparedDecision, ...]:
    prepared: list[_PreparedDecision] = []
    for raw in records:
        document = _document(raw)
        if not document:
            raise ValueError("decision batch rows must be non-empty mappings")
        record_type = document.get(
            "record_type", document.get("decision_type", document.get("type"))
        )
        scan_run_id = document.get("scan_run_id", default_scan_run_id)
        supplied = [
            document[name]
            for name in ("record", "decision", "payload")
            if name in document and document[name] is not None
        ]
        if len(supplied) != 1:
            raise ValueError(
                "each decision batch row requires exactly one record payload"
            )
        prepared.append(
            _prepare_decision(
                scan_run_id=scan_run_id,
                record_type=record_type,
                record=supplied[0],
                recorded_at=recorded_at,
            )
        )
    return tuple(prepared)


def _assert_gate_bundle_record(
    item: _PreparedDecision,
    *,
    expected_outcome: str,
    expected_hash: str,
) -> None:
    record = item.record
    expected_type = f"GATE_BUNDLE_{expected_outcome}"
    bundle = record.get("gate_bundle")
    if (
        record.get("schema") != "options_copilot.gate_append_payload.v1"
        or record.get("record_type") != expected_type
        or record.get("scan_run_id") != item.scan_run_id
        or record.get("gate_bundle_hash") != expected_hash
        or not isinstance(bundle, Mapping)
        or bundle.get("scan_run_id") != item.scan_run_id
        or bundle.get("outcome") != expected_outcome
        or bundle.get("gate_bundle_hash") != expected_hash
    ):
        raise ValueError("RANKING_GATE_BUNDLE_MISMATCH")
    hash_payload = {
        key: value
        for key, value in bundle.items()
        if key not in {"gate_bundle_hash", "hard_failure_candidate_keys"}
    }
    if canonical_hash(hash_payload) != expected_hash:
        raise ValueError("RANKING_GATE_BUNDLE_MISMATCH")


def _prepare_decision(
    *,
    scan_run_id: object,
    record_type: object,
    record: Mapping[str, Any] | object,
    recorded_at: str,
) -> _PreparedDecision:
    scan_id = _identity(scan_run_id)
    kind = _identity(record_type).upper()
    document = _document(record)
    if not document:
        raise ValueError("decision record must be a non-empty mapping")
    record_json = canonical_json(document)
    normalized = json.loads(record_json)
    if not isinstance(normalized, dict) or not normalized:
        raise ValueError("decision record must be a canonical object")
    bound_scan = normalized.get("scan_run_id")
    if bound_scan is not None and bound_scan != scan_id:
        raise ValueError("decision record scan_run_id mismatch")
    identity_payload = {
        "schema": "options_copilot.ranking_decision_record.v2",
        "scan_run_id": scan_id,
        "record_type": kind,
        "record": normalized,
    }
    return _PreparedDecision(
        scan_run_id=scan_id,
        record_type=kind,
        record=normalized,
        record_json=record_json,
        record_hash=canonical_hash(identity_payload),
        recorded_at=recorded_at,
    )


def _decision_binding(item: _PreparedDecision) -> dict[str, object]:
    return {
        "scan_run_id": item.scan_run_id,
        "record_type": item.record_type,
        "record": dict(item.record),
        "record_json": item.record_json,
        "record_hash": item.record_hash,
    }


def _ranking_terminal_decision(
    *,
    scan_run_id: str,
    ranking_snapshot_id: str,
    snapshot_hash: str,
    recorded_at: str,
) -> _PreparedDecision:
    return _prepare_decision(
        scan_run_id=scan_run_id,
        record_type="TRADE",
        record={
            "schema": "options_copilot.ranking_finalized.v1",
            "status": "TRADE",
            "event": "RANKING_FINALIZED",
            "scan_run_id": scan_run_id,
            "ranking_snapshot_id": ranking_snapshot_id,
            "ranking_snapshot_hash": snapshot_hash,
        },
        recorded_at=recorded_at,
    )


def _expected_hashes(
    identities: Mapping[str, str],
    row: Mapping[str, object],
    snapshot_hash: str,
    immutable_inputs_hash: str,
) -> Mapping[str, object]:
    return {
        **dict(identities),
        "policy_version": identities["current_policy_version"],
        "policy_hash": identities["current_policy_hash"],
        "immutable_inputs_hash": immutable_inputs_hash,
        "candidate_id": row["candidate_id"],
        "proposal_hash": row["proposal_hash"],
        "candidate_hash": row["candidate_hash"],
        "candidate_body_hash": row["candidate_body_hash"],
        "proposal_body_hash": row["proposal_body_hash"],
        "ranking_basis_hash": row["ranking_basis_hash"],
        "row_hash": row["row_hash"],
        "ranking_snapshot_hash": snapshot_hash,
    }


def _snapshot_expected_hashes(
    identities: Mapping[str, str],
    snapshot_hash: str,
    immutable_inputs_hash: str,
) -> Mapping[str, object]:
    return {
        **dict(identities),
        "policy_version": identities["current_policy_version"],
        "policy_hash": identities["current_policy_hash"],
        "immutable_inputs_hash": immutable_inputs_hash,
        "ranking_snapshot_hash": snapshot_hash,
    }


def _expected_from_rows(
    snapshot: sqlite3.Row, row: sqlite3.Row
) -> Mapping[str, object]:
    return _expected_hashes(
        _snapshot_identities(snapshot),
        _row_from_sql(row),
        str(snapshot["snapshot_hash"]),
        str(snapshot["immutable_inputs_hash"]),
    )


def _snapshot_identities(row: sqlite3.Row) -> dict[str, str]:
    return {key: str(row[key]) for key in _SNAPSHOT_IDENTITY_FIELDS}


def _basis_from_sql(row: sqlite3.Row) -> dict[str, object]:
    return {
        "candidate_id": str(row["candidate_id"]),
        "proposal_hash": str(row["proposal_hash"]),
        "candidate_hash": str(row["candidate_hash"]),
        "candidate_body": json.loads(str(row["candidate_body_json"])),
        "candidate_body_json": str(row["candidate_body_json"]),
        "candidate_body_hash": str(row["candidate_body_hash"]),
        "proposal_body": json.loads(str(row["proposal_body_json"])),
        "proposal_body_json": str(row["proposal_body_json"]),
        "proposal_body_hash": str(row["proposal_body_hash"]),
        "ranking_basis_hash": str(row["ranking_basis_hash"]),
        "evidence_inputs": json.loads(str(row["evidence_inputs_json"])),
        "evidence_inputs_json": str(row["evidence_inputs_json"]),
        "evidence_inputs_hash": str(row["evidence_inputs_hash"]),
        "authority_status": str(row["authority_status"]),
        "authorizable": bool(row["authorizable"]),
        "basis_json": str(row["basis_json"]),
        "basis_content_hash": str(row["basis_content_hash"]),
    }


def _verified_basis_from_sql(
    row: sqlite3.Row,
    identities: Mapping[str, str],
    immutable_inputs: Mapping[str, object],
) -> dict[str, object]:
    try:
        base = _basis_from_sql(row)
        if (
            canonical_json(base["candidate_body"])
            != base["candidate_body_json"]
            or canonical_json(base["proposal_body"])
            != base["proposal_body_json"]
            or canonical_json(base["evidence_inputs"])
            != base["evidence_inputs_json"]
            or base["evidence_inputs"] != dict(immutable_inputs)
        ):
            raise RankingStoreCorruption("ranking canonical JSON mismatch")
        binding = build_ranking_basis(
            candidate_body=base["candidate_body"],
            proposal_body=base["proposal_body"],
            candidate_hash=str(base["candidate_hash"]),
            proposal_hash=str(base["proposal_hash"]),
            current_policy_version=identities["current_policy_version"],
            current_policy_hash=identities["current_policy_hash"],
            policy_authority_marker_hash=identities[
                "policy_authority_marker_hash"
            ],
            cost_version=identities["cost_version"],
            cost_hash=identities["cost_hash"],
            risk_contract_hash=identities["risk_contract_hash"],
            evidence_inputs=immutable_inputs,
        )
        if (
            binding.candidate_hash != base["candidate_body_hash"]
            or binding.proposal_hash != base["proposal_body_hash"]
            or binding.evidence_inputs_hash != base["evidence_inputs_hash"]
            or binding.ranking_basis_hash != base["ranking_basis_hash"]
            or binding.basis_json != base["basis_json"]
            or canonical_hash(binding.basis_json) != base["basis_content_hash"]
            or binding.candidate_body.get("candidate_id")
            != base["candidate_id"]
        ):
            raise RankingStoreCorruption("ranking basis binding mismatch")
        return base
    except RankingStoreCorruption:
        raise
    except Exception as exc:
        raise RankingStoreCorruption("invalid ranking basis") from exc


def _row_from_sql(row: sqlite3.Row) -> dict[str, object]:
    candidate_body = json.loads(str(row["candidate_body_json"]))
    proposal_body = json.loads(str(row["proposal_body_json"]))
    score_components = json.loads(str(row["score_json"]))
    return {
        "schema": "options_copilot.ranking_row.v2",
        "rank": int(row["rank"]),
        "candidate_id": str(row["candidate_id"]),
        "proposal_hash": str(row["proposal_hash"]),
        "candidate_hash": str(row["candidate_hash"]),
        "candidate_body": candidate_body,
        "candidate_body_json": str(row["candidate_body_json"]),
        "candidate_body_hash": str(row["candidate_body_hash"]),
        "proposal_body": proposal_body,
        "proposal_body_json": str(row["proposal_body_json"]),
        "proposal_body_hash": str(row["proposal_body_hash"]),
        "ranking_basis_hash": str(row["ranking_basis_hash"]),
        "authority_status": str(row["authority_status"]),
        "authorizable": bool(row["authorizable"]),
        "risk_authority_marker_hash": str(row["risk_authority_marker_hash"]),
        "score_components": score_components,
        "score_json": str(row["score_json"]),
        "score_hash": str(row["score_hash"]),
        "row_hash": str(row["row_hash"]),
    }


def _public_row(row: sqlite3.Row) -> dict[str, object]:
    return _row_from_sql(row)


def _public_basis(row: sqlite3.Row) -> dict[str, object]:
    base = _basis_from_sql(row)
    return {
        "rank": None,
        **base,
    }


def _stored_decision(row: sqlite3.Row) -> StoredRankingDecision:
    record = json.loads(str(row["record_json"]))
    if not isinstance(record, dict):
        raise RankingStoreCorruption("stored decision is not an object")
    return StoredRankingDecision(
        sequence=int(row["sequence"]),
        scan_run_id=str(row["scan_run_id"]),
        record_type=str(row["record_type"]),
        record=record,
        record_hash=str(row["record_hash"]),
        previous_decision_hash=str(row["previous_decision_hash"]),
        decision_hash=str(row["decision_hash"]),
        recorded_at=datetime.fromisoformat(str(row["recorded_at"])),
    )


def _assert_append_authority(
    identities: Mapping[str, str],
    *,
    ranked_bases: Sequence[Mapping[str, object]],
    policy_resolver: object | None,
    risk_authority_resolver: object | None,
    resolved_policy: object | None,
    risk_authority: object | None,
) -> None:
    if (
        policy_resolver is None
        or risk_authority_resolver is None
        or resolved_policy is None
        or risk_authority is None
        or not _has_current_api(policy_resolver)
        or not _has_current_api(risk_authority_resolver)
    ):
        raise RankingStoreConflict(
            "authorizable snapshots require explicit current authority proofs"
        )
    if (
        not _policy_matches_identities(identities, resolved_policy)
        or not _risk_matches_identities(identities, risk_authority)
        or not _resolver_is_current(policy_resolver, resolved_policy)
        or not _resolver_is_current(risk_authority_resolver, risk_authority)
    ):
        raise RankingStoreConflict("authority resolution is stale or mismatched")
    risk = _document(risk_authority)
    tier = getattr(risk.get("tier"), "value", risk.get("tier"))
    if tier == "A_GRADE" and risk.get("a_grade_approved") is True:
        if (
            not ranked_bases
            or ranked_bases[0].get("authority_status") != "A_GRADE"
            or not _risk_binding_matches_identities(
                identities, ranked_bases[0], risk_authority
            )
            or any(
                base.get("authority_status") == "A_GRADE"
                for base in ranked_bases[1:]
            )
        ):
            raise RankingStoreConflict(
                "A-grade authority must bind the exact rank-one row"
            )
    elif tier == "NORMAL" and risk.get("a_grade_approved") is False:
        if any(base.get("authority_status") == "A_GRADE" for base in ranked_bases):
            raise RankingStoreConflict("NORMAL authority cannot authorize A-grade rows")
    else:
        raise RankingStoreConflict("risk authority tier is invalid")


def _resolve_once(resolver: object, **kwargs: object) -> object:
    target = getattr(resolver, "resolve", None)
    if not callable(target):
        raise TypeError("authority resolver has no resolve method")
    signature = inspect.signature(target)
    accepted = (
        kwargs
        if any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        else {key: value for key, value in kwargs.items() if key in signature.parameters}
    )
    return target(**accepted)


def _resolve_cost_once(resolver: object, **kwargs: object) -> object:
    target = getattr(resolver, "resolve", None)
    if not callable(target):
        target = getattr(resolver, "verify", None)
    if not callable(target):
        raise TypeError("execution cost contract has no resolve or verify method")
    signature = inspect.signature(target)
    accepted = (
        kwargs
        if any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        else {key: value for key, value in kwargs.items() if key in signature.parameters}
    )
    return target(**accepted)


def _has_current_api(resolver: object) -> bool:
    return callable(getattr(resolver, "is_current", None)) or callable(
        getattr(resolver, "assert_current", None)
    )


def _has_guard_current_api(resolver: object) -> bool:
    return callable(getattr(resolver, "guard_current", None))


def _resolver_is_current(resolver: object | None, resolution: object) -> bool:
    if resolver is None:
        return False
    check = getattr(resolver, "is_current", None)
    if callable(check):
        try:
            return bool(check(resolution))
        except Exception:
            return False
    assertion = getattr(resolver, "assert_current", None)
    if callable(assertion):
        try:
            result = assertion(resolution)
        except Exception:
            return False
        return result is not False
    return False


def _guard_current_resolution(
    resolver: object | None,
    resolution: object,
    callback: Callable[[], object],
) -> object | None:
    if resolver is None or not callable(callback):
        return None
    guard = getattr(resolver, "guard_current", None)
    if not callable(guard):
        return None
    try:
        signature = inspect.signature(guard)
    except (TypeError, ValueError):
        return None
    kwargs: dict[str, object] = {"callback": callback}
    if not any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    ):
        kwargs = {
            key: value
            for key, value in kwargs.items()
            if key in signature.parameters
        }
    return guard(resolution, **kwargs)


def _policy_matches(snapshot: sqlite3.Row, policy: object) -> bool:
    return _policy_matches_identities(_snapshot_identities(snapshot), policy)


def _policy_matches_identities(
    identities: Mapping[str, str], policy: object
) -> bool:
    doc = _document(policy)
    return (
        doc.get("current_policy_version")
        == identities["current_policy_version"]
        and doc.get("current_policy_hash")
        == identities["current_policy_hash"]
        and doc.get("policy_authority_marker_hash")
        == identities["policy_authority_marker_hash"]
    )


def _risk_matches(snapshot: sqlite3.Row, risk: object) -> bool:
    return _risk_matches_identities(_snapshot_identities(snapshot), risk)


def _risk_binding_matches(
    snapshot: sqlite3.Row,
    row: sqlite3.Row,
    risk: object,
) -> bool:
    return _risk_binding_matches_identities(
        _snapshot_identities(snapshot), row, risk
    )


def _risk_binding_matches_identities(
    identities: Mapping[str, str],
    row: Mapping[str, object] | sqlite3.Row,
    risk: object,
) -> bool:
    if not _risk_matches_identities(identities, risk):
        return False
    doc = _document(risk)
    tier = getattr(doc.get("tier"), "value", doc.get("tier"))
    authority_status = str(row["authority_status"])
    if tier == "NORMAL" and doc.get("a_grade_approved") is False:
        return authority_status == "NORMAL"
    if tier != "A_GRADE" or doc.get("a_grade_approved") is not True:
        return False
    return bool(
        authority_status == "A_GRADE"
        and doc.get("proposal_hash") == str(row["proposal_hash"])
        and doc.get("candidate_hash") == str(row["candidate_hash"])
        and doc.get("current_policy_version")
        == identities["current_policy_version"]
        and doc.get("current_policy_hash") == identities["current_policy_hash"]
        and doc.get("policy_authority_marker_hash")
        == identities["policy_authority_marker_hash"]
        and doc.get("execution_cost_version") == identities["cost_version"]
        and doc.get("execution_cost_hash") == identities["cost_hash"]
        and doc.get("ranking_basis_hash") == str(row["ranking_basis_hash"])
        and doc.get("risk_contract_hash") == identities["risk_contract_hash"]
    )


def _risk_matches_identities(
    identities: Mapping[str, str], risk: object
) -> bool:
    doc = _document(risk)
    return (
        doc.get("version") == identities["risk_authority_version"]
        and doc.get("risk_contract_hash") == identities["risk_contract_hash"]
        and doc.get("risk_authority_marker_hash", doc.get("marker_hash"))
        == identities["risk_authority_marker_hash"]
    )


def _cost_matches(snapshot: sqlite3.Row, cost: object | None) -> bool:
    doc = _document(cost)
    version = doc.get(
        "cost_version",
        doc.get("execution_cost_contract_version", doc.get("version")),
    )
    content_hash = doc.get(
        "cost_hash",
        doc.get("execution_cost_contract_hash", doc.get("contract_hash")),
    )
    return (
        version == str(snapshot["cost_version"])
        and content_hash == str(snapshot["cost_hash"])
    )


def _migration_anchor_payload(
    *,
    sequence: int,
    source_version: int,
    source_label: str,
    legacy_tables_json: str,
    legacy_json: str,
    legacy_hash: str,
    previous_anchor_hash: str,
    created_at: str,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.ranking_migration_anchor.v1",
        "sequence": sequence,
        "source_version": source_version,
        "source_label": source_label,
        "legacy_tables_json": legacy_tables_json,
        "legacy_json": legacy_json,
        "legacy_hash": legacy_hash,
        "previous_anchor_hash": previous_anchor_hash,
        "created_at": created_at,
    }


def _canonical_json_column(row: sqlite3.Row, column: str) -> object:
    try:
        value = json.loads(str(row[column]))
    except (TypeError, ValueError) as exc:
        raise RankingStoreCorruption(f"invalid canonical JSON in {column}") from exc
    if canonical_json(value) != str(row[column]):
        raise RankingStoreCorruption(f"non-canonical JSON in {column}")
    return value


def _sqlite_json_value(value: object) -> object:
    if isinstance(value, bytes):
        return {"$sqlite_blob_hex": value.hex()}
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _document(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    return vars(value) if hasattr(value, "__dict__") else {}


def _same_identity(name: str, primary: object, alias: object) -> str:
    values = [value for value in (primary, alias) if value is not None]
    if not values:
        raise ValueError(f"{name} is required")
    normalized = [_identity(value) for value in values]
    if len(set(normalized)) != 1:
        raise ValueError(f"{name} aliases disagree")
    return normalized[0]


def _same_hash(name: str, primary: object, alias: object) -> str:
    values = [value for value in (primary, alias) if value is not None]
    if not values:
        raise ValueError(f"{name} is required")
    normalized = [_hash(value) for value in values]
    if len(set(normalized)) != 1:
        raise ValueError(f"{name} aliases disagree")
    return normalized[0]


def _hash(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("hash must be lowercase SHA-256")
    return value


def _identity(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("identity values cannot be blank")
    return value.strip()


__all__ = [
    "FrozenRankOneAuthorization",
    "RankingStore",
    "RankingStoreConflict",
    "RankingStoreCorruption",
    "RankingStoreError",
    "StoredRankingDecision",
    "StoredRankingSnapshot",
]
