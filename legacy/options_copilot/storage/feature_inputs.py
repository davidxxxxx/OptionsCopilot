"""Append-only, point-in-time feature inputs; no producer or decision authority."""
from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import threading
import time

from options_copilot.analytics.feature_contracts import FeatureContractError, FeatureHistoryBatch
from options_copilot.storage.canonical import canonical_hash, canonical_json, datetime_text, utc_datetime


_VERSION = 1
_GENESIS = "0" * 64
_COLUMNS = ("sequence", "identity", "con_id", "request_hash", "basis_hash", "batch_hash",
            "batch_json", "first_seen_at", "previous_hash", "row_hash")
_TABLE_SQL = """CREATE TABLE feature_history (
    sequence INTEGER PRIMARY KEY, identity TEXT NOT NULL UNIQUE,
    con_id INTEGER NOT NULL, request_hash TEXT NOT NULL, basis_hash TEXT NOT NULL,
    batch_hash TEXT NOT NULL, batch_json TEXT NOT NULL, first_seen_at TEXT NOT NULL,
    previous_hash TEXT NOT NULL, row_hash TEXT NOT NULL UNIQUE
)"""
_TRIGGER_SQL = {
    f"feature_history_no_{action.lower()}":
        f"CREATE TRIGGER feature_history_no_{action.lower()} BEFORE {action} ON feature_history "
        "BEGIN SELECT RAISE(ABORT,'feature history is immutable'); END"
    for action in ("UPDATE", "DELETE")
}
_AUDIT_BUDGET_SECONDS = 2.0
_MAX_BATCH_DOCUMENT_BYTES = 1024 * 1024
_INDEX_SQL = "CREATE INDEX feature_history_lookup ON feature_history(con_id,basis_hash,request_hash,first_seen_at)"


def _digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _identity(batch: FeatureHistoryBatch) -> str:
    return canonical_hash({"request": batch.request.contract_hash, "basis": batch.basis.contract_hash,
                           "source_revision": batch.source_revision_hash, "request_fingerprint": batch.request_fingerprint})


@dataclass(frozen=True, slots=True)
class _DatabaseState:
    data_version: int
    schema_version: int
    total_changes: int
    schema_hash: str


@dataclass(frozen=True, slots=True)
class _VerifiedHead:
    sequence: int
    row_hash: str
    state: _DatabaseState | None


@dataclass(slots=True)
class _VerificationTransaction:
    checkpoint: _VerifiedHead | None


class FeatureInputStore:
    """Dedicated bounded-context SQLite cache using the existing ledger pattern.

    First-seen time belongs to this store's clock, not imported market dates.
    Request and basis documents are immutable observations, never approvals.
    The runtime owns one instance; SQLite transactions also serialize writers
    from separate instances without relying on that ownership assumption.
    """

    def __init__(self, path: str | Path, *, clock: Callable[[], datetime] | None = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        # Never load this trust anchor from the database being inspected.
        # Opening an instance establishes it only after a complete audit.
        self._verified_checkpoint: _VerifiedHead | None = None
        self._connection = sqlite3.connect(self.path, timeout=5, isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            # Recheck schema only after SQLite owns the initialization lease;
            # a competing first open may have created it while we waited.
            with self._transaction(write=True):
                version = self._connection.execute("PRAGMA user_version").fetchone()[0]
                if version not in (0, _VERSION):
                    raise FeatureContractError("FEATURE_HISTORY_SCHEMA_UNSUPPORTED")
                if version == 0:
                    tables = self._connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    if tables:
                        raise FeatureContractError("LEGACY_BASIS_UNRESOLVED")
                    self._connection.execute(_TABLE_SQL)
                    self._connection.execute(_INDEX_SQL)
                    for sql in _TRIGGER_SQL.values():
                        self._connection.execute(sql)
                    self._connection.execute("PRAGMA user_version=1")
            self._connection.execute("PRAGMA journal_mode=WAL")
            self.verify_integrity()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> FeatureInputStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    @contextmanager
    def _transaction(self, *, write: bool = False) -> Iterator[_VerificationTransaction]:
        with self._lock:
            if self._closed:
                raise FeatureContractError("FEATURE_HISTORY_STORE_CLOSED")
            self._connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            transaction = _VerificationTransaction(self._verified_checkpoint)
            try:
                yield transaction
                checkpoint = transaction.checkpoint
                if (checkpoint is not None and checkpoint.state is not None
                        and self._data_version() != checkpoint.state.data_version):
                    checkpoint = replace(checkpoint, state=None)
                self._connection.execute("COMMIT")
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
            # A read transaction can hold an older WAL snapshot while another
            # connection commits. Retain its verified anchor, never certify the
            # newer version using rows from that older snapshot.
            if checkpoint is not None and checkpoint.state is not None:
                try:
                    if self._data_version() != checkpoint.state.data_version:
                        checkpoint = replace(checkpoint, state=None)
                except sqlite3.DatabaseError:
                    checkpoint = replace(checkpoint, state=None)
            self._verified_checkpoint = checkpoint

    def _data_version(self) -> int:
        return int(self._connection.execute("PRAGMA data_version").fetchone()[0])

    @staticmethod
    def _check_budget(deadline: float) -> None:
        if time.monotonic() > deadline:
            raise FeatureContractError("FEATURE_HISTORY_VERIFICATION_BUDGET_EXCEEDED")

    def _database_state(self) -> tuple[_DatabaseState, bool]:
        """Validate small schema metadata and bind the transaction's version.

        Capture the version before pinning the read snapshot. If an external
        commit crosses that boundary, never reuse the old checkpoint or stamp
        the newly observed version onto an unverified snapshot.
        """
        try:
            data_version = self._data_version()
            if self._connection.execute("PRAGMA user_version").fetchone()[0] != _VERSION:
                raise ValueError("schema version changed")
            table = self._connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='feature_history'"
            ).fetchone()
            # Column names alone do not prove the PK/UNIQUE/NOT NULL contract.
            # Whitespace normalization accepts the original v1 DDL unchanged.
            if (table is None or not isinstance(table[0], str)
                    or " ".join(table[0].split()) != " ".join(_TABLE_SQL.split())):
                raise ValueError("table constraints changed")
            columns = tuple(row[1] for row in self._connection.execute("PRAGMA table_info(feature_history)"))
            triggers = dict(self._connection.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'"))
            index = self._connection.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='feature_history_lookup'").fetchone()
            if (columns != _COLUMNS or index is None or index[0] != _INDEX_SQL
                    or any(triggers.get(name) != sql for name, sql in _TRIGGER_SQL.items())):
                raise ValueError("schema or immutable triggers missing")
            schema = tuple(tuple(row) for row in self._connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            ))
            state = _DatabaseState(
                data_version=data_version,
                schema_version=int(self._connection.execute("PRAGMA schema_version").fetchone()[0]),
                total_changes=self._connection.total_changes,
                schema_hash=canonical_hash(schema),
            )
            return state, data_version == self._data_version()
        except (TypeError, ValueError, ArithmeticError, sqlite3.DatabaseError) as exc:
            raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID") from exc

    def _decode_row(self, row: sqlite3.Row) -> FeatureHistoryBatch:
        """Independently verify every returned document and its SQL projection."""
        try:
            if not isinstance(row["batch_json"], str) or len(row["batch_json"].encode("utf-8")) > _MAX_BATCH_DOCUMENT_BYTES:
                raise ValueError("oversize batch")
            batch = FeatureHistoryBatch.from_document(json.loads(row["batch_json"]))
            observed = utc_datetime(datetime.fromisoformat(row["first_seen_at"]))
            payload = {key: row[key] for key in _COLUMNS if key != "row_hash"}
            if (type(row["sequence"]) is not int or row["sequence"] <= 0
                    or not _digest(row["previous_hash"])
                    or (row["sequence"] == 1 and row["previous_hash"] != _GENESIS)
                    or canonical_hash(payload) != row["row_hash"] or _identity(batch) != row["identity"]
                    or batch.request.con_id != row["con_id"] or batch.request.contract_hash != row["request_hash"]
                    or batch.basis.contract_hash != row["basis_hash"] or batch.batch_hash != row["batch_hash"]
                    or batch.available_at > observed or datetime_text(observed) != row["first_seen_at"]):
                raise ValueError("row identity or chronology mismatch")
            return batch
        except (TypeError, ValueError, ArithmeticError, sqlite3.DatabaseError) as exc:
            raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID") from exc

    def _verify(self, state: _DatabaseState, *, anchor: _VerifiedHead | None, deadline: float) -> _VerifiedHead:
        previous = _GENESIS
        sequence = 0
        anchor_found = anchor is None or anchor.sequence == 0
        try:
            for row in self._connection.execute("SELECT * FROM feature_history ORDER BY sequence"):
                self._check_budget(deadline)
                sequence += 1
                self._decode_row(row)
                if row["sequence"] != sequence or row["previous_hash"] != previous:
                    raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
                if anchor is not None and sequence == anchor.sequence:
                    if row["row_hash"] != anchor.row_hash:
                        raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
                    anchor_found = True
                previous = row["row_hash"]
            if not anchor_found:
                raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
            self._check_budget(deadline)
            return _VerifiedHead(sequence, previous, state)
        except sqlite3.DatabaseError as exc:
            raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID") from exc

    def _ensure_verified(self, transaction: _VerificationTransaction, *, deadline: float) -> _VerifiedHead:
        self._check_budget(deadline)
        state, stable = self._database_state()
        checkpoint = transaction.checkpoint
        if checkpoint is not None and stable and checkpoint.state == state:
            head = self._connection.execute(
                "SELECT sequence,row_hash FROM feature_history ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            actual = (0, _GENESIS) if head is None else (head["sequence"], head["row_hash"])
            if actual != (checkpoint.sequence, checkpoint.row_hash):
                raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
        else:
            checkpoint = self._verify(state, anchor=checkpoint, deadline=deadline)
            if not stable:
                checkpoint = replace(checkpoint, state=None)
        self._check_budget(deadline)
        transaction.checkpoint = checkpoint
        return checkpoint

    def verify_integrity(self) -> None:
        """Audit SQLite indexes as well as the evidence chain on open/on demand."""
        with self._transaction() as transaction:
            deadline = time.monotonic() + _AUDIT_BUDGET_SECONDS
            self._connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            try:
                result = self._connection.execute("PRAGMA integrity_check").fetchall()
                if tuple(row[0] for row in result) != ("ok",):
                    raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
            except sqlite3.DatabaseError as exc:
                reason = ("FEATURE_HISTORY_VERIFICATION_BUDGET_EXCEEDED" if time.monotonic() > deadline
                          else "FEATURE_HISTORY_STORE_INVALID")
                raise FeatureContractError(reason) from exc
            finally:
                self._connection.set_progress_handler(None, 0)
            state, stable = self._database_state()
            checkpoint = self._verify(state, anchor=transaction.checkpoint, deadline=deadline)
            transaction.checkpoint = checkpoint if stable else replace(checkpoint, state=None)

    def append(self, batch: FeatureHistoryBatch) -> dict[str, object]:
        if not isinstance(batch, FeatureHistoryBatch):
            raise TypeError("batch must be a FeatureHistoryBatch")
        now = utc_datetime(self._clock(), field="feature store clock")
        if batch.available_at > now:
            raise FeatureContractError("FEATURE_BATCH_FROM_FUTURE")
        # Revalidate at the serialization boundary before taking a write lease.
        batch = FeatureHistoryBatch.from_document(batch.as_dict())
        document = canonical_json(batch.as_dict())
        if len(document.encode("utf-8")) > _MAX_BATCH_DOCUMENT_BYTES:
            raise FeatureContractError("FEATURE_BATCH_SIZE_LIMIT")
        identity = _identity(batch)
        with self._transaction(write=True) as transaction:
            deadline = time.monotonic() + _AUDIT_BUDGET_SECONDS
            head = self._ensure_verified(transaction, deadline=deadline)
            existing = self._connection.execute("SELECT * FROM feature_history WHERE identity=?", (identity,)).fetchone()
            if existing is not None:
                self._decode_row(existing)
                self._check_budget(deadline)
                if existing["identity"] != identity:
                    raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
                if existing["batch_hash"] != batch.batch_hash:
                    raise FeatureContractError("FEATURE_HISTORY_REVISION_CONFLICT")
                return {"inserted": False, "sequence": existing["sequence"], "row_hash": existing["row_hash"],
                        "first_seen_at": existing["first_seen_at"], "batch_hash": batch.batch_hash}
            payload = {"sequence": head.sequence + 1,
                       "identity": identity, "con_id": batch.request.con_id,
                       "request_hash": batch.request.contract_hash, "basis_hash": batch.basis.contract_hash,
                       "batch_hash": batch.batch_hash, "batch_json": document,
                       "first_seen_at": datetime_text(now), "previous_hash": head.row_hash}
            row_hash = canonical_hash(payload)
            changes_before = self._connection.total_changes
            self._connection.execute(
                "INSERT INTO feature_history VALUES(?,?,?,?,?,?,?,?,?,?)",
                tuple(payload[key] for key in _COLUMNS if key != "row_hash") + (row_hash,),
            )
            inserted_head = self._connection.execute(
                "SELECT * FROM feature_history ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            # An additional INSERT trigger could substitute a row while copying
            # its claimed hash. Compare all persisted fields with the document
            # validated above before granting the in-process checkpoint.
            if (inserted_head is None
                    or inserted_head["row_hash"] != row_hash
                    or any(inserted_head[key] != value for key, value in payload.items())):
                raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
            if head.state is not None and self._connection.total_changes == changes_before + 1:
                transaction.checkpoint = _VerifiedHead(
                    payload["sequence"], row_hash,
                    replace(head.state, total_changes=self._connection.total_changes),
                )
            else:
                state, stable = self._database_state()
                checked = self._verify(state, anchor=head, deadline=deadline)
                transaction.checkpoint = checked if stable else replace(checked, state=None)
            self._check_budget(deadline)
            return {"inserted": True, "sequence": payload["sequence"], "row_hash": row_hash,
                    "first_seen_at": payload["first_seen_at"], "batch_hash": batch.batch_hash}

    def read(
        self, *, con_id: int, request_hashes: Sequence[str], basis_hash: str, cutoff: datetime,
    ) -> tuple[FeatureHistoryBatch, ...]:
        """Return immutable fragments, never merge across contracts or fetch data.

        The caller resolves the full calendar and chooses required sessions.
        Historical revisions are returned in append order, not overwritten.
        """
        if (type(con_id) is not int or con_id <= 0 or not _digest(basis_hash)
                or not isinstance(request_hashes, (tuple, list)) or not 1 <= len(request_hashes) <= 600
                or any(not _digest(value) for value in request_hashes)
                or len(set(request_hashes)) != len(request_hashes)):
            raise FeatureContractError("FEATURE_HISTORY_QUERY_INVALID")
        instant = utc_datetime(cutoff, field="feature history cutoff")
        placeholders = ",".join("?" for _ in request_hashes)
        with self._transaction() as transaction:
            deadline = time.monotonic() + _AUDIT_BUDGET_SECONDS
            head = self._ensure_verified(transaction, deadline=deadline)
            rows = self._connection.execute(
                "SELECT * FROM feature_history WHERE con_id=? AND basis_hash=? AND first_seen_at<=? "
                f"AND request_hash IN ({placeholders}) ORDER BY sequence LIMIT 5001",
                (con_id, basis_hash, datetime_text(instant), *request_hashes),
            ).fetchall()
            if len(rows) > 5000:
                raise FeatureContractError("FEATURE_HISTORY_QUERY_LIMIT")
            batches = []
            previous_sequence = 0
            for row in rows:
                self._check_budget(deadline)
                batch = self._decode_row(row)
                if (not previous_sequence < row["sequence"] <= head.sequence
                        or row["con_id"] != con_id or row["basis_hash"] != basis_hash
                        or row["request_hash"] not in request_hashes
                        or utc_datetime(datetime.fromisoformat(row["first_seen_at"])) > instant
                        or batch.available_at > instant):
                    raise FeatureContractError("FEATURE_HISTORY_STORE_INVALID")
                previous_sequence = row["sequence"]
                batches.append(batch)
            self._check_budget(deadline)
            return tuple(batches)

    def count(self) -> int:
        with self._transaction() as transaction:
            deadline = time.monotonic() + _AUDIT_BUDGET_SECONDS
            return self._ensure_verified(transaction, deadline=deadline).sequence


__all__ = ["FeatureInputStore"]
