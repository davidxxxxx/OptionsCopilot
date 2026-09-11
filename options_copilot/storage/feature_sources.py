"""Append-only normalized source projections, never lossless raw data or authority."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import threading
import time

from options_copilot.feature_source_diagnostic import SOURCE_KINDS, validate_feature_source_observation
from options_copilot.storage.canonical import canonical_hash, canonical_json, datetime_text, utc_datetime


_VERSION = 1
_GENESIS = "0" * 64
_AUDIT_SECONDS = 2.0
_MAX_DOCUMENT_BYTES = 1_100_000
_COLUMNS = (
    "sequence", "observation_id", "operation_id", "symbol", "con_id", "kind",
    "source_hash", "request_hash", "basis_hash", "observation_json", "request_json", "basis_json",
    "available_at", "first_seen_at", "previous_hash", "row_hash",
)
_REFERENCE_COLUMNS = (
    "observation_id", "sequence", "row_hash", "source_hash", "request_hash", "basis_hash", "first_seen_at",
)
_TABLE_SQL = """CREATE TABLE feature_source_observations (
    sequence INTEGER PRIMARY KEY, observation_id TEXT NOT NULL UNIQUE, operation_id TEXT NOT NULL,
    symbol TEXT NOT NULL, con_id INTEGER NOT NULL, kind TEXT NOT NULL,
    source_hash TEXT NOT NULL, request_hash TEXT NOT NULL, basis_hash TEXT NOT NULL,
    observation_json TEXT NOT NULL, request_json TEXT NOT NULL, basis_json TEXT NOT NULL,
    available_at TEXT NOT NULL, first_seen_at TEXT NOT NULL, previous_hash TEXT NOT NULL,
    row_hash TEXT NOT NULL UNIQUE
)"""
_INDEX_SQL = "CREATE INDEX feature_sources_lookup ON feature_source_observations(symbol,con_id,kind,first_seen_at,sequence)"
_TRIGGER_SQL = {
    f"feature_sources_no_{action.lower()}":
        f"CREATE TRIGGER feature_sources_no_{action.lower()} BEFORE {action} ON feature_source_observations "
        "BEGIN SELECT RAISE(ABORT,'feature sources are immutable'); END"
    for action in ("UPDATE", "DELETE")
}


class FeatureSourceStoreError(ValueError):
    """Stable, secret-free rejection of a source observation or its ledger."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _require(condition: bool, reason: str = "FEATURE_SOURCE_STORE_INVALID") -> None:
    if not condition:
        raise FeatureSourceStoreError(reason)


def _symbol(value: object) -> str:
    _require(isinstance(value, str) and re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,31}", value) is not None,
             "FEATURE_SOURCE_SYMBOL_INVALID")
    return value


def _operation_id(value: object) -> str:
    _require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:\-]{0,127}", value) is not None
             and value.upper() != "READ_FEATURE_SOURCE_DIAGNOSTIC", "FEATURE_SOURCE_OPERATION_ID_INVALID")
    return value


def _time(value: datetime) -> datetime:
    try:
        return utc_datetime(value)
    except (TypeError, ValueError):
        raise FeatureSourceStoreError("FEATURE_SOURCE_TIMESTAMP_INVALID") from None


def _source(raw: object, cutoff: datetime) -> dict[str, object]:
    try:
        _require(isinstance(raw, Mapping), "FEATURE_SOURCE_OBSERVATION_INVALID")
        symbol = _symbol(raw.get("symbol"))
        observed = validate_feature_source_observation(
            raw, kind=raw.get("kind"), symbol=symbol, cutoff=cutoff, allow_failure=False,
        )
        _require(observed["contract"] is not None, "FEATURE_SOURCE_CONTRACT_IDENTITY_REQUIRED")
        return observed
    except FeatureSourceStoreError:
        raise
    except (TypeError, ValueError, ArithmeticError, KeyError):
        raise FeatureSourceStoreError("FEATURE_SOURCE_OBSERVATION_INVALID") from None


def _documents(source: Mapping[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    request = {
        "schema": "options_copilot.feature_source_wire_request.v1",
        "wire_schema": source["schema"],
        **{name: source[name] for name in (
            "source", "kind", "symbol", "contract", "request_parameters", "cutoff_at",
            "requested_at", "request_sent", "broker_request_id",
        )},
    }
    basis = {
        "schema": "options_copilot.feature_source_unresolved_basis.v1",
        "source": source["source"], "kind": source["kind"], "symbol": source["symbol"],
        "con_id": source["contract"]["con_id"],
        "basis_status": "PROVIDER_NATIVE_UNRESOLVED",
        "methodology_authority": "UNRESOLVED", "calendar_authority": "UNRESOLVED",
        "comparability_authority": "UNRESOLVED", "decision_authority": "OBSERVATION_ONLY",
        "production_eligible": False, "model_input_complete": False, "point_in_time_verified": False,
    }
    return request, basis


def _projection(source: Mapping[str, object], operation_id: str) -> dict[str, object]:
    request, basis = _documents(source)
    return {
        "observation_id": canonical_hash({
            "operation_id": operation_id, "kind": source["kind"], "source_hash": source["content_hash"],
        }),
        "operation_id": operation_id, "symbol": source["symbol"], "con_id": source["contract"]["con_id"],
        "kind": source["kind"], "source_hash": source["content_hash"],
        "request_hash": canonical_hash(request), "basis_hash": canonical_hash(basis),
        "observation_json": canonical_json(source), "request_json": canonical_json(request),
        "basis_json": canonical_json(basis),
        "available_at": datetime_text(_time(datetime.fromisoformat(source["available_at"]))),
    }


def _reference(row: Mapping[str, object]) -> dict[str, object]:
    return {key: row[key] for key in _REFERENCE_COLUMNS}


class FeatureSourceObservationStore:
    """Local, low-volume ledger for validated normalized diagnostic responses.

    No producer, broker, calendar resolution, feature promotion or confirmation
    authority is introduced. Every operation audits the complete ledger; there
    is no persisted/self-certified checkpoint and no arbitrary lifetime row cap.
    """

    def __init__(self, path: str | Path, *, clock: Callable[[], datetime] | None = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(self.path, timeout=5, isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            with self._transaction(write=True):
                version = self._connection.execute("PRAGMA user_version").fetchone()[0]
                _require(version in (0, _VERSION), "FEATURE_SOURCE_STORE_SCHEMA_UNSUPPORTED")
                if version == 0:
                    tables = self._connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    _require(not tables, "FEATURE_SOURCE_STORE_LEGACY_UNSUPPORTED")
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

    def __enter__(self) -> FeatureSourceObservationStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    @staticmethod
    def _check_budget(deadline: float) -> None:
        _require(time.monotonic() <= deadline, "FEATURE_SOURCE_STORE_VERIFICATION_BUDGET_EXCEEDED")

    @contextmanager
    def _transaction(self, *, write: bool = False) -> Iterator[float]:
        with self._lock:
            _require(not self._closed, "FEATURE_SOURCE_STORE_CLOSED")
            deadline = time.monotonic() + _AUDIT_SECONDS
            try:
                self._connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                self._connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                yield deadline
                self._check_budget(deadline)
                self._connection.execute("COMMIT")
            except BaseException as exc:
                self._connection.set_progress_handler(None, 0)
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                if isinstance(exc, sqlite3.DatabaseError):
                    reason = ("FEATURE_SOURCE_STORE_VERIFICATION_BUDGET_EXCEEDED"
                              if time.monotonic() > deadline else "FEATURE_SOURCE_STORE_INVALID")
                    raise FeatureSourceStoreError(reason) from None
                raise
            finally:
                self._connection.set_progress_handler(None, 0)

    def _verify_schema(self) -> None:
        _require(self._connection.execute("PRAGMA user_version").fetchone()[0] == _VERSION)
        columns = tuple(row[1] for row in self._connection.execute("PRAGMA table_info(feature_source_observations)"))
        _require(columns == _COLUMNS)
        definitions = dict(self._connection.execute("SELECT name,sql FROM sqlite_master"))
        _require(definitions.get("feature_source_observations") == _TABLE_SQL)
        _require(definitions.get("feature_sources_lookup") == _INDEX_SQL)
        triggers = dict(self._connection.execute(
            "SELECT name,sql FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name='feature_source_observations'"
        ))
        # REPLACE inside an unexpected trigger can bypass DELETE immutability.
        _require(triggers == _TRIGGER_SQL)

    def _decode_row(self, row: Mapping[str, object]) -> dict[str, object]:
        try:
            _require(type(row["sequence"]) is int and row["sequence"] > 0)
            _require(isinstance(row["observation_json"], str)
                     and len(row["observation_json"].encode("utf-8")) <= _MAX_DOCUMENT_BYTES)
            raw = json.loads(row["observation_json"])
            source = _source(raw, _time(datetime.fromisoformat(raw["cutoff_at"])))
            operation_id = _operation_id(row["operation_id"])
            expected = _projection(source, operation_id)
            _require(all(row[key] == value for key, value in expected.items()))
            seen = _time(datetime.fromisoformat(row["first_seen_at"]))
            _require(datetime_text(seen) == row["first_seen_at"]
                     and expected["available_at"] <= row["first_seen_at"])
            _require(isinstance(row["previous_hash"], str)
                     and re.fullmatch(r"[a-f0-9]{64}", row["previous_hash"]) is not None)
            payload = {key: row[key] for key in _COLUMNS if key != "row_hash"}
            _require(canonical_hash(payload) == row["row_hash"])
            request, basis = _documents(source)
            return {"reference": _reference(row), "source": source, "operation_id": operation_id,
                    "request_document": request, "basis_document": basis}
        except (TypeError, ValueError, ArithmeticError, KeyError):
            raise FeatureSourceStoreError("FEATURE_SOURCE_STORE_INVALID") from None

    def _verify(self, deadline: float) -> tuple[int, str, str | None, dict[str, int]]:
        self._check_budget(deadline)
        self._verify_schema()
        sequence, previous, last_seen = 0, _GENESIS, None
        counts = dict.fromkeys(SOURCE_KINDS, 0)
        for row in self._connection.execute("SELECT * FROM feature_source_observations ORDER BY sequence"):
            self._check_budget(deadline)
            self._decode_row(row)
            sequence += 1
            _require(row["sequence"] == sequence and row["previous_hash"] == previous)
            _require(last_seen is None or row["first_seen_at"] >= last_seen)
            previous, last_seen = row["row_hash"], row["first_seen_at"]
            counts[row["kind"]] += 1
        self._check_budget(deadline)
        return sequence, previous, last_seen, counts

    def verify_integrity(self) -> None:
        with self._transaction() as deadline:
            _require(tuple(row[0] for row in self._connection.execute("PRAGMA integrity_check")) == ("ok",))
            self._verify(deadline)

    def append(self, raw: Mapping[str, object], *, operation_id: str, cutoff: datetime) -> dict[str, object]:
        operation_id = _operation_id(operation_id)
        source = _source(raw, _time(cutoff))
        projection = _projection(source, operation_id)
        now = _time(self._clock())
        _require(projection["available_at"] <= datetime_text(now), "FEATURE_SOURCE_FROM_FUTURE")
        with self._transaction(write=True) as deadline:
            sequence, previous, last_seen, _counts = self._verify(deadline)
            existing = self._connection.execute(
                "SELECT * FROM feature_source_observations WHERE observation_id=?", (projection["observation_id"],),
            ).fetchone()
            if existing is not None:
                self._decode_row(existing)
                _require(all(existing[key] == value for key, value in projection.items()))
                return {**_reference(existing), "inserted": False}
            first_seen = datetime_text(now)
            _require(last_seen is None or first_seen >= last_seen, "FEATURE_SOURCE_STORE_CLOCK_REGRESSED")
            payload = {"sequence": sequence + 1, **projection, "first_seen_at": first_seen, "previous_hash": previous}
            row_hash = canonical_hash(payload)
            changes_before = self._connection.total_changes
            self._connection.execute(
                "INSERT INTO feature_source_observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(payload[key] for key in _COLUMNS if key != "row_hash") + (row_hash,),
            )
            _require(self._connection.total_changes == changes_before + 1)
            inserted = self._connection.execute(
                "SELECT * FROM feature_source_observations ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            _require(inserted is not None and inserted["row_hash"] == row_hash
                     and all(inserted[key] == value for key, value in payload.items()))
            return {**_reference(inserted), "inserted": True}

    def read(self, *, symbol: str, con_id: int, cutoff: datetime) -> tuple[dict[str, object], ...]:
        symbol = _symbol(symbol)
        _require(type(con_id) is int and con_id > 0, "FEATURE_SOURCE_QUERY_INVALID")
        instant = _time(cutoff)
        cutoff_text = datetime_text(instant)
        with self._transaction() as deadline:
            self._verify(deadline)
            result = []
            for kind in SOURCE_KINDS:
                row = self._connection.execute(
                    "SELECT * FROM feature_source_observations WHERE symbol=? AND con_id=? AND kind=? "
                    "AND first_seen_at<=? AND available_at<=? ORDER BY sequence DESC LIMIT 1",
                    (symbol, con_id, kind, cutoff_text, cutoff_text),
                ).fetchone()
                if row is None:
                    continue
                envelope = self._decode_row(row)
                _require(row["symbol"] == symbol and row["con_id"] == con_id and row["kind"] == kind
                         and row["first_seen_at"] <= cutoff_text and row["available_at"] <= cutoff_text)
                result.append(envelope)
            return tuple(result)

    def status(self) -> dict[str, object]:
        with self._transaction() as deadline:
            count, _head, last_seen, counts = self._verify(deadline)
            return {
                "schema": "options_copilot.feature_source_store_status.v1", "status": "VERIFIED",
                "observation_count": count, "counts_by_kind": counts, "last_first_seen_at": last_seen,
                "decision_authority": "OBSERVATION_ONLY", "production_eligible": False, "model_input_complete": False,
            }


__all__ = ["FeatureSourceObservationStore", "FeatureSourceStoreError"]
