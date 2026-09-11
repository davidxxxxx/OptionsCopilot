"""Durable at-most-once native-history intents and immutable source fragments."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import threading
import time

from options_copilot.history_source_contracts import validate_native_history_request, validate_native_history_result
from options_copilot.storage.canonical import canonical_hash, canonical_json, datetime_text, utc_datetime


_GENESIS = "0" * 64
_AUDIT_SECONDS = 2.0
_MAX_DOCUMENT_BYTES = 1_200_000
_COLUMNS = (
    "sequence", "event_id", "event_kind", "manifest_id", "claim_id", "con_id",
    "request_hash", "basis_hash", "payload_json", "available_at", "first_seen_at", "previous_hash", "row_hash",
)
_TABLE_SQL = """CREATE TABLE history_source_events (
    sequence INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, event_kind TEXT NOT NULL,
    manifest_id TEXT NOT NULL, claim_id TEXT, con_id INTEGER, request_hash TEXT, basis_hash TEXT,
    payload_json TEXT NOT NULL, available_at TEXT, first_seen_at TEXT NOT NULL,
    previous_hash TEXT NOT NULL, row_hash TEXT NOT NULL UNIQUE
)"""
_INDEX_SQL = {
    "history_sources_manifest": "CREATE UNIQUE INDEX history_sources_manifest ON history_source_events(manifest_id) WHERE event_kind='MANIFEST'",
    "history_sources_intent": "CREATE UNIQUE INDEX history_sources_intent ON history_source_events(claim_id) WHERE event_kind='SEND_INTENT'",
    "history_sources_terminal": "CREATE UNIQUE INDEX history_sources_terminal ON history_source_events(claim_id) WHERE event_kind IN ('COMPLETED','FAILED')",
    "history_sources_lookup": "CREATE INDEX history_sources_lookup ON history_source_events(con_id,basis_hash,request_hash,first_seen_at)",
}
_TRIGGER_SQL = {
    f"history_sources_no_{action.lower()}":
        f"CREATE TRIGGER history_sources_no_{action.lower()} BEFORE {action} ON history_source_events "
        "BEGIN SELECT RAISE(ABORT,'history sources are immutable'); END"
    for action in ("UPDATE", "DELETE")
}
_PARENT_FIELDS = {"parent_run_id", "owner", "operation", "session_date", "scheduled_for", "deadline_at", "calendar_hash"}


class HistorySourceStoreError(ValueError):
    """Stable, secret-free rejection of a request, guard or durable ledger."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class HistorySourceSendPermit:
    claim_id: str
    manifest_id: str
    request_hash: str
    basis_hash: str
    _instance: object = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class ClaimResult:
    status: str
    permit: HistorySourceSendPermit | None
    reference: dict[str, object]


@dataclass(frozen=True, slots=True)
class _DatabaseState:
    data_version: int
    schema_version: int
    total_changes: int
    schema_hash: str


@dataclass(frozen=True, slots=True)
class _View:
    manifests: dict[str, dict[str, object]] = field(default_factory=dict)
    parents: dict[str, str] = field(default_factory=dict)
    intents: dict[str, dict[str, object]] = field(default_factory=dict)
    terminals: dict[str, dict[str, object]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Head:
    sequence: int
    row_hash: str
    last_seen: str | None
    state: _DatabaseState | None
    view: _View


@dataclass(slots=True)
class _Transaction:
    deadline: float
    head: _Head


def _require(condition: bool, reason: str = "HISTORY_SOURCE_STORE_INVALID") -> None:
    if not condition:
        raise HistorySourceStoreError(reason)


def _digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _time(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        return utc_datetime(parsed)
    except (TypeError, ValueError):
        raise HistorySourceStoreError("HISTORY_SOURCE_TIMESTAMP_INVALID") from None


def _guard(guard: Callable[[], bool]) -> None:
    try:
        allowed = callable(guard) and guard() is True
    except Exception:
        allowed = False
    _require(allowed, "HISTORY_SOURCE_PARENT_GUARD_REJECTED")


def _request(raw: object) -> dict[str, object]:
    try:
        return validate_native_history_request(raw)
    except Exception:
        raise HistorySourceStoreError("HISTORY_SOURCE_REQUEST_INVALID") from None


def _fragment(raw: object, request: Mapping[str, object]) -> dict[str, object]:
    try:
        return validate_native_history_result(raw, prepared_request=request)
    except Exception:
        raise HistorySourceStoreError("HISTORY_SOURCE_FRAGMENT_INVALID") from None


def _manifest(parent: object, requests: object) -> dict[str, object]:
    _require(isinstance(parent, Mapping) and set(parent) == _PARENT_FIELDS, "HISTORY_SOURCE_PARENT_INVALID")
    checked_parent = deepcopy(dict(parent))
    run_id = parent["parent_run_id"]
    _require(isinstance(run_id, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:\-]{0,127}", run_id) is not None,
             "HISTORY_SOURCE_PARENT_INVALID")
    _require(parent["operation"] == "NEXT_SESSION_PREPARATION"
             and parent["owner"] == "daily-operation.next_session_preparation"
             and _digest(parent["calendar_hash"]), "HISTORY_SOURCE_PARENT_INVALID")
    try:
        session = date.fromisoformat(parent["session_date"])
        _require(session.isoformat() == parent["session_date"], "HISTORY_SOURCE_PARENT_INVALID")
    except (TypeError, ValueError):
        raise HistorySourceStoreError("HISTORY_SOURCE_PARENT_INVALID") from None
    scheduled, deadline = _time(parent["scheduled_for"]), _time(parent["deadline_at"])
    _require(scheduled < deadline, "HISTORY_SOURCE_PARENT_INVALID")
    _require(isinstance(requests, (tuple, list)) and 1 <= len(requests) <= 6, "HISTORY_SOURCE_MANIFEST_BOUND_EXCEEDED")
    checked = tuple(_request(raw) for raw in requests)
    symbols = {row["symbol"] for row in checked}
    _require(len(symbols) <= 3 and len({(row["symbol"], row["kind"]) for row in checked}) == len(checked),
             "HISTORY_SOURCE_MANIFEST_BOUND_EXCEEDED")
    _require(len({(row["request_hash"], row["basis_hash"]) for row in checked}) == len(checked),
             "HISTORY_SOURCE_REQUEST_DUPLICATE")
    for symbol in symbols:
        _require(len({row["con_id"] for row in checked if row["symbol"] == symbol}) == 1,
                 "HISTORY_SOURCE_IDENTITY_CONFLICT")
    for con_id in {row["con_id"] for row in checked}:
        _require(len({row["symbol"] for row in checked if row["con_id"] == con_id}) == 1,
                 "HISTORY_SOURCE_IDENTITY_CONFLICT")
    for row in checked:
        cutoff = row["cutoff"]
        _require(_time(cutoff["scheduled_for"]) == scheduled
                 and cutoff["completed_session"] == parent["session_date"]
                 and cutoff["calendar_hash"] == parent["calendar_hash"], "HISTORY_SOURCE_PARENT_REQUEST_MISMATCH")
    return {"schema": "options_copilot.history_source_manifest.v1", "parent_document": checked_parent,
            "requests": sorted(checked, key=lambda row: (row["symbol"], row["kind"], row["request_hash"]))}


def _claim_id(manifest: Mapping[str, object], request: Mapping[str, object]) -> str:
    parent = manifest["parent_document"]
    return canonical_hash({"parent_run_id": parent["parent_run_id"], "session_date": parent["session_date"],
                           "con_id": request["con_id"], "request_hash": request["request_hash"], "basis_hash": request["basis_hash"]})


def _reference(row: Mapping[str, object]) -> dict[str, object]:
    return {key: row[key] for key in ("event_id", "sequence", "row_hash", "first_seen_at")}


class HistorySourceStore:
    """One append-only event transaction binds a response fragment and terminal.

    Guards are injected by the trusted runtime; parent documents/self hashes do
    not grant authority. Only the original claimant receives a memory-only send
    permit. Reopening cannot reissue it, even when a crash preceded wire send.
    """

    def __init__(self, path: str | Path, *, clock: Callable[[], datetime] | None = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        self._instance = object()
        self._permits: dict[str, HistorySourceSendPermit] = {}
        self._checkpoint: _Head | None = None
        self._connection = sqlite3.connect(self.path, timeout=2, isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("BEGIN IMMEDIATE")
            version = self._connection.execute("PRAGMA user_version").fetchone()[0]
            _require(version in (0, 1), "HISTORY_SOURCE_SCHEMA_UNSUPPORTED")
            if version == 0:
                _require(not self._connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),
                         "HISTORY_SOURCE_LEGACY_UNSUPPORTED")
                self._connection.execute(_TABLE_SQL)
                for sql in (*_INDEX_SQL.values(), *_TRIGGER_SQL.values()):
                    self._connection.execute(sql)
                self._connection.execute("PRAGMA user_version=1")
            self._connection.execute("COMMIT")
            self._connection.execute("PRAGMA journal_mode=WAL")
            self.verify_integrity()
        except BaseException:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> HistorySourceStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    @staticmethod
    def _budget(deadline: float) -> None:
        _require(time.monotonic() <= deadline, "HISTORY_SOURCE_VERIFICATION_BUDGET_EXCEEDED")

    def _state(self) -> tuple[_DatabaseState, bool]:
        data_version = self._connection.execute("PRAGMA data_version").fetchone()[0]
        _require(self._connection.execute("PRAGMA user_version").fetchone()[0] == 1)
        # TEMP objects can shadow the persistent table or inject trigger writes
        # without changing main.schema_version. This store owns no TEMP schema.
        _require(not self._connection.execute("SELECT name FROM sqlite_temp_master").fetchall())
        definitions = tuple(tuple(row) for row in self._connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        ))
        tables = {name: sql for kind, name, _table, sql in definitions if kind == "table"}
        _require(tables == {"history_source_events": _TABLE_SQL})
        triggers = {name: sql for kind, name, _table, sql in definitions if kind == "trigger"}
        _require(triggers == _TRIGGER_SQL)
        indexes = {name: sql for kind, name, _table, sql in definitions if kind == "index" and sql is not None}
        _require(indexes == _INDEX_SQL)
        state = _DatabaseState(data_version, self._connection.execute("PRAGMA schema_version").fetchone()[0],
                               self._connection.total_changes, canonical_hash(definitions))
        return state, data_version == self._connection.execute("PRAGMA data_version").fetchone()[0]

    @contextmanager
    def _transaction(self, *, guard: Callable[[], bool] | None = None) -> Iterator[_Transaction]:
        if guard is not None:
            _guard(guard)
        with self._lock:
            _require(not self._closed, "HISTORY_SOURCE_STORE_CLOSED")
            deadline = time.monotonic() + _AUDIT_SECONDS
            try:
                self._connection.execute("BEGIN IMMEDIATE" if guard is not None else "BEGIN")
                self._connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                state, stable = self._state()
                checkpoint = self._checkpoint
                if not stable or checkpoint is None or checkpoint.state != state:
                    checkpoint = self._verify(state, anchor=checkpoint, deadline=deadline)
                    if not stable:
                        checkpoint = replace(checkpoint, state=None)
                transaction = _Transaction(deadline, checkpoint)
                yield transaction
                self._budget(deadline)
                if guard is not None:
                    _guard(guard)
                candidate = transaction.head
                if candidate.state is not None and self._connection.execute("PRAGMA data_version").fetchone()[0] != candidate.state.data_version:
                    candidate = replace(candidate, state=None)
                self._connection.execute("COMMIT")
            except BaseException as exc:
                self._connection.set_progress_handler(None, 0)
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                if isinstance(exc, sqlite3.DatabaseError):
                    reason = "HISTORY_SOURCE_VERIFICATION_BUDGET_EXCEEDED" if time.monotonic() > deadline else "HISTORY_SOURCE_STORE_INVALID"
                    raise HistorySourceStoreError(reason) from None
                raise
            finally:
                self._connection.set_progress_handler(None, 0)
            if candidate.state is not None:
                try:
                    if self._connection.execute("PRAGMA data_version").fetchone()[0] != candidate.state.data_version:
                        candidate = replace(candidate, state=None)
                except sqlite3.DatabaseError:
                    candidate = replace(candidate, state=None)
            self._checkpoint = candidate

    def _verify(self, state: _DatabaseState, *, anchor: _Head | None, deadline: float) -> _Head:
        sequence, previous, seen, view = 0, _GENESIS, None, _View()
        for raw in self._connection.execute("SELECT * FROM history_source_events ORDER BY sequence"):
            self._budget(deadline)
            row = dict(raw)
            sequence += 1
            _require(row["sequence"] == sequence and row["previous_hash"] == previous)
            _require(seen is None or row["first_seen_at"] >= seen)
            if anchor is not None and sequence == anchor.sequence:
                _require(row["row_hash"] == anchor.row_hash)
            view = self._apply_event(row, view)
            previous, seen = row["row_hash"], row["first_seen_at"]
        _require(anchor is None or sequence >= anchor.sequence)
        self._budget(deadline)
        return _Head(sequence, previous, seen, state, view)

    @staticmethod
    def _find_request(manifest: Mapping[str, object], request_hash: str, basis_hash: str) -> dict[str, object]:
        found = [row for row in manifest["requests"] if row["request_hash"] == request_hash and row["basis_hash"] == basis_hash]
        _require(len(found) == 1, "HISTORY_SOURCE_REQUEST_NOT_IN_MANIFEST")
        return found[0]

    def _apply_event(self, row: dict[str, object], view: _View) -> _View:
        try:
            _require(isinstance(row["payload_json"], str) and len(row["payload_json"].encode("utf-8")) <= _MAX_DOCUMENT_BYTES)
            payload = json.loads(row["payload_json"])
            _require(canonical_json(payload) == row["payload_json"])
            _require(datetime_text(_time(row["first_seen_at"])) == row["first_seen_at"])
            _require(canonical_hash({key: row[key] for key in _COLUMNS if key != "row_hash"}) == row["row_hash"])
            kind, manifest_id, claim_id = row["event_kind"], row["manifest_id"], row["claim_id"]
            _require(_digest(manifest_id) and _digest(row["event_id"]))
            _require(canonical_hash({"event_kind": kind, "manifest_id": manifest_id, "claim_id": claim_id, "payload": payload}) == row["event_id"])
            if kind == "MANIFEST":
                _require(set(payload) == {"schema", "parent_document", "requests"})
                checked = _manifest(payload["parent_document"], payload["requests"])
                _require(checked == payload and canonical_hash(payload) == manifest_id and claim_id is None)
                _require(all(row[key] is None for key in ("con_id", "request_hash", "basis_hash", "available_at")))
                parent_id = payload["parent_document"]["parent_run_id"]
                _require(manifest_id not in view.manifests and parent_id not in view.parents)
                return replace(view, manifests={**view.manifests, manifest_id: row}, parents={**view.parents, parent_id: manifest_id})
            _require(manifest_id in view.manifests and _digest(claim_id))
            manifest = json.loads(view.manifests[manifest_id]["payload_json"])
            request = self._find_request(manifest, row["request_hash"], row["basis_hash"])
            _require(row["con_id"] == request["con_id"] and claim_id == _claim_id(manifest, request))
            _require(payload["claim_id"] == claim_id and payload["manifest_id"] == manifest_id)
            if kind == "SEND_INTENT":
                _require(set(payload) == {"schema", "claim_id", "manifest_id", "request_hash", "basis_hash"})
                _require(payload["schema"] == "options_copilot.history_source_send_intent.v1"
                         and payload["request_hash"] == row["request_hash"] and payload["basis_hash"] == row["basis_hash"])
                _require(claim_id not in view.intents and row["available_at"] is None)
                return replace(view, intents={**view.intents, claim_id: row})
            _require(kind in {"COMPLETED", "FAILED"} and claim_id in view.intents and claim_id not in view.terminals)
            _require(payload["schema"] == "options_copilot.history_source_terminal.v1" and payload["status"] == kind)
            if kind == "COMPLETED":
                _require(set(payload) == {"schema", "claim_id", "manifest_id", "status", "fragment"})
                fragment = _fragment(payload["fragment"], request)
                _require(fragment == payload["fragment"] and fragment["claim_id"] == claim_id)
                _require(row["available_at"] == datetime_text(_time(fragment["available_at"]))
                         and row["available_at"] <= row["first_seen_at"])
            else:
                _require(set(payload) == {"schema", "claim_id", "manifest_id", "status", "reason_code"})
                _require(isinstance(payload["reason_code"], str) and re.fullmatch(r"[A-Z0-9_:]{1,160}", payload["reason_code"]) is not None
                         and row["available_at"] is None)
            return replace(view, terminals={**view.terminals, claim_id: row})
        except HistorySourceStoreError:
            raise
        except (TypeError, ValueError, KeyError, ArithmeticError):
            raise HistorySourceStoreError("HISTORY_SOURCE_STORE_INVALID") from None

    def _append(self, transaction: _Transaction, *, kind: str, manifest_id: str, payload: dict[str, object],
                claim_id: str | None = None, request: Mapping[str, object] | None = None) -> dict[str, object]:
        head = transaction.head
        now = datetime_text(_time(self._clock()))
        _require(head.last_seen is None or now >= head.last_seen, "HISTORY_SOURCE_CLOCK_REGRESSED")
        available = datetime_text(_time(payload["fragment"]["available_at"])) if kind == "COMPLETED" else None
        _require(available is None or available <= now, "HISTORY_SOURCE_FRAGMENT_FROM_FUTURE")
        event_id = canonical_hash({"event_kind": kind, "manifest_id": manifest_id, "claim_id": claim_id, "payload": payload})
        row = dict(sequence=head.sequence + 1, event_id=event_id, event_kind=kind, manifest_id=manifest_id,
                   claim_id=claim_id, con_id=None if request is None else request["con_id"],
                   request_hash=None if request is None else request["request_hash"],
                   basis_hash=None if request is None else request["basis_hash"], payload_json=canonical_json(payload),
                   available_at=available, first_seen_at=now, previous_hash=head.row_hash)
        row["row_hash"] = canonical_hash(row)
        view = self._apply_event(row, head.view)
        self._budget(transaction.deadline)
        before = self._connection.total_changes
        self._connection.execute("INSERT INTO history_source_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", tuple(row[key] for key in _COLUMNS))
        _require(self._connection.total_changes == before + 1)
        actual = self._connection.execute("SELECT * FROM history_source_events WHERE sequence=?", (row["sequence"],)).fetchone()
        _require(actual is not None and dict(actual) == row)
        state = None if head.state is None else replace(head.state, total_changes=self._connection.total_changes)
        transaction.head = _Head(row["sequence"], row["row_hash"], now, state, view)
        return row

    def verify_integrity(self) -> None:
        with self._transaction() as transaction:
            _require(tuple(row[0] for row in self._connection.execute("PRAGMA integrity_check")) == ("ok",))
            state, stable = self._state()
            head = self._verify(state, anchor=self._checkpoint, deadline=transaction.deadline)
            transaction.head = head if stable else replace(head, state=None)

    def freeze_manifest(self, parent_document: Mapping[str, object], requests: Sequence[Mapping[str, object]],
                        *, guard: Callable[[], bool]) -> dict[str, object]:
        payload = _manifest(parent_document, requests)
        manifest_id = canonical_hash(payload)
        parent_id = payload["parent_document"]["parent_run_id"]
        with self._transaction(guard=guard) as transaction:
            existing_id = transaction.head.view.parents.get(parent_id)
            _require(existing_id is None or existing_id == manifest_id, "HISTORY_SOURCE_MANIFEST_CONFLICT")
            existing = transaction.head.view.manifests.get(manifest_id)
            row = existing or self._append(transaction, kind="MANIFEST", manifest_id=manifest_id, payload=payload)
            result = {"manifest_id": manifest_id, "parent_run_id": parent_id, "request_count": len(payload["requests"]),
                      "reference": _reference(row), "inserted": existing is None}
        return result

    def claim_for_send(self, manifest_id: str, request_hash: str, basis_hash: str, *, guard: Callable[[], bool]) -> ClaimResult:
        _require(all(_digest(value) for value in (manifest_id, request_hash, basis_hash)), "HISTORY_SOURCE_QUERY_INVALID")
        permit: HistorySourceSendPermit | None = None
        with self._transaction(guard=guard) as transaction:
            view = transaction.head.view
            _require(manifest_id in view.manifests, "HISTORY_SOURCE_MANIFEST_UNKNOWN")
            manifest = json.loads(view.manifests[manifest_id]["payload_json"])
            request = self._find_request(manifest, request_hash, basis_hash)
            claim_id = _claim_id(manifest, request)
            if claim_id in view.intents:
                terminal = view.terminals.get(claim_id)
                status = terminal["event_kind"] if terminal is not None else "ALREADY_CLAIMED" if claim_id in self._permits else "UNCERTAIN"
                result = ClaimResult(status, None, _reference(terminal or view.intents[claim_id]))
            else:
                payload = {"schema": "options_copilot.history_source_send_intent.v1", "manifest_id": manifest_id,
                           "claim_id": claim_id, "request_hash": request_hash, "basis_hash": basis_hash}
                row = self._append(transaction, kind="SEND_INTENT", manifest_id=manifest_id, claim_id=claim_id, request=request, payload=payload)
                permit = HistorySourceSendPermit(claim_id, manifest_id, request_hash, basis_hash, self._instance)
                result = ClaimResult("CLAIMED", permit, _reference(row))
        if permit is not None:
            self._permits[permit.claim_id] = permit
        return result

    def _permit_request(self, permit: HistorySourceSendPermit, view: _View) -> dict[str, object]:
        _require(isinstance(permit, HistorySourceSendPermit) and permit._instance is self._instance
                 and self._permits.get(permit.claim_id) is permit, "HISTORY_SOURCE_SEND_PERMIT_INVALID")
        _require(permit.manifest_id in view.manifests and permit.claim_id in view.intents)
        manifest = json.loads(view.manifests[permit.manifest_id]["payload_json"])
        request = self._find_request(manifest, permit.request_hash, permit.basis_hash)
        _require(_claim_id(manifest, request) == permit.claim_id)
        return request

    def complete(self, permit: HistorySourceSendPermit, fragment: Mapping[str, object], *, guard: Callable[[], bool]) -> dict[str, object]:
        return self._terminal(permit, fragment=fragment, reason_code=None, guard=guard)

    def record_failure(self, permit: HistorySourceSendPermit, reason_code: str, *, guard: Callable[[], bool]) -> dict[str, object]:
        _require(isinstance(reason_code, str) and re.fullmatch(r"[A-Z0-9_:]{1,160}", reason_code) is not None,
                 "HISTORY_SOURCE_FAILURE_REASON_INVALID")
        return self._terminal(permit, fragment=None, reason_code=reason_code, guard=guard)

    def _terminal(self, permit: HistorySourceSendPermit, *, fragment: Mapping[str, object] | None,
                  reason_code: str | None, guard: Callable[[], bool]) -> dict[str, object]:
        with self._transaction(guard=guard) as transaction:
            request = self._permit_request(permit, transaction.head.view)
            kind = "COMPLETED" if fragment is not None else "FAILED"
            payload = {"schema": "options_copilot.history_source_terminal.v1", "manifest_id": permit.manifest_id,
                       "claim_id": permit.claim_id, "status": kind}
            if fragment is not None:
                payload["fragment"] = _fragment(fragment, request)
                _require(payload["fragment"]["claim_id"] == permit.claim_id, "HISTORY_SOURCE_CLAIM_MISMATCH")
            else:
                payload["reason_code"] = reason_code
            existing = transaction.head.view.terminals.get(permit.claim_id)
            _require(existing is None or existing["payload_json"] == canonical_json(payload), "HISTORY_SOURCE_TERMINAL_CONFLICT")
            row = existing or self._append(transaction, kind=kind, manifest_id=permit.manifest_id, claim_id=permit.claim_id,
                                            request=request, payload=payload)
            result = {"claim_id": permit.claim_id, "manifest_id": permit.manifest_id, "reference": _reference(row), "inserted": existing is None}
            if reason_code is not None:
                result["reason_code"] = reason_code
        return result

    def find_fragments(self, *, symbol: str, con_id: int, cutoff: datetime) -> tuple[dict[str, object], ...]:
        return self._read(symbol=symbol, con_id=con_id, request_hashes=None, basis_hash=None, cutoff=cutoff)

    def find_fragments_page(
        self,
        *,
        symbol: str,
        con_id: int,
        cutoff: datetime,
        before_sequence: int | None = None,
        limit: int = 128,
    ) -> dict[str, object]:
        """Return an explicit newest-first page without deleting old evidence.

        The exclusive immutable sequence cursor prevents concurrent newer
        appends from shifting later pages. Callers retain the same cutoff and
        must expose has_more rather than treating a bounded page as all data.
        """

        _require(type(limit) is int and 1 <= limit <= 128
                 and (before_sequence is None or type(before_sequence) is int
                      and 1 <= before_sequence <= 2**63 - 1), "HISTORY_SOURCE_QUERY_INVALID")
        _require(type(con_id) is int and con_id > 0 and isinstance(symbol, str)
                 and re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,31}", symbol) is not None,
                 "HISTORY_SOURCE_QUERY_INVALID")
        instant = datetime_text(_time(cutoff))
        with self._transaction() as transaction:
            fragments = []
            has_more = False
            for row in reversed(transaction.head.view.terminals.values()):
                self._budget(transaction.deadline)
                if (row["event_kind"] != "COMPLETED" or row["con_id"] != con_id
                        or row["first_seen_at"] > instant or row["available_at"] > instant
                        or before_sequence is not None and row["sequence"] >= before_sequence):
                    continue
                manifest = json.loads(transaction.head.view.manifests[row["manifest_id"]]["payload_json"])
                request = self._find_request(manifest, row["request_hash"], row["basis_hash"])
                if request["symbol"] != symbol:
                    continue
                if len(fragments) == limit:
                    has_more = True
                    break
                fragments.append(self._envelope(row, manifest, request))
            return {
                "fragments": tuple(fragments),
                "has_more": has_more,
                "next_before_sequence": fragments[-1]["reference"]["sequence"] if has_more else None,
            }

    def read_fragments(self, *, con_id: int, request_hashes: Sequence[str], basis_hash: str,
                       cutoff: datetime) -> tuple[dict[str, object], ...]:
        _require(isinstance(request_hashes, (tuple, list)) and 1 <= len(request_hashes) <= 128
                 and len(set(request_hashes)) == len(request_hashes) and all(_digest(value) for value in request_hashes)
                 and _digest(basis_hash), "HISTORY_SOURCE_QUERY_INVALID")
        return self._read(symbol=None, con_id=con_id, request_hashes=request_hashes, basis_hash=basis_hash, cutoff=cutoff)

    def _read(self, *, symbol: str | None, con_id: int, request_hashes: Sequence[str] | None,
              basis_hash: str | None, cutoff: datetime) -> tuple[dict[str, object], ...]:
        _require(type(con_id) is int and con_id > 0 and (symbol is None or isinstance(symbol, str)
                 and re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,31}", symbol) is not None), "HISTORY_SOURCE_QUERY_INVALID")
        instant = datetime_text(_time(cutoff))
        with self._transaction() as transaction:
            result = []
            for row in transaction.head.view.terminals.values():
                self._budget(transaction.deadline)
                if (row["event_kind"] != "COMPLETED" or row["con_id"] != con_id
                        or row["first_seen_at"] > instant or row["available_at"] > instant
                        or basis_hash is not None and row["basis_hash"] != basis_hash
                        or request_hashes is not None and row["request_hash"] not in request_hashes):
                    continue
                manifest = json.loads(transaction.head.view.manifests[row["manifest_id"]]["payload_json"])
                request = self._find_request(manifest, row["request_hash"], row["basis_hash"])
                if symbol is not None and request["symbol"] != symbol:
                    continue
                result.append(self._envelope(row, manifest, request))
                _require(len(result) <= 128, "HISTORY_SOURCE_QUERY_LIMIT")
            return tuple(deepcopy(sorted(result, key=lambda item: item["reference"]["sequence"])))

    @staticmethod
    def _envelope(row: Mapping[str, object], manifest: Mapping[str, object],
                  request: Mapping[str, object]) -> dict[str, object]:
        request = _request(request)
        payload = json.loads(row["payload_json"])
        fragment = _fragment(payload["fragment"], request)
        _require(fragment["claim_id"] == row["claim_id"]
                 and canonical_hash({key: row[key] for key in _COLUMNS if key != "row_hash"}) == row["row_hash"])
        return deepcopy({"claim_id": row["claim_id"], "manifest_id": row["manifest_id"],
                         "parent_document": manifest["parent_document"], "prepared_request": request,
                         "fragment": fragment, "reference": _reference(row)})

    def status(self) -> dict[str, object]:
        with self._transaction() as transaction:
            view = transaction.head.view
            completed = sum(row["event_kind"] == "COMPLETED" for row in view.terminals.values())
            return {"schema": "options_copilot.history_source_store_status.v1", "status": "VERIFIED",
                    "manifest_count": len(view.manifests), "intent_count": len(view.intents), "completed_count": completed,
                    "failed_count": len(view.terminals) - completed,
                    "uncertain_count": sum(claim_id not in view.terminals for claim_id in view.intents),
                    "decision_authority": "OBSERVATION_ONLY", "production_eligible": False, "model_input_complete": False}


__all__ = ["HistorySourceStore", "HistorySourceStoreError", "HistorySourceSendPermit", "ClaimResult"]
