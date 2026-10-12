"""Durable, bounded scheduling for read-only research scans."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
from pathlib import Path
import json
import sqlite3
import threading
import uuid

from options_copilot.market.session_calendar import CalendarStatus, US_OPTIONS_TIMEZONE, UsOptionsCalendarSnapshot
from options_copilot.storage.canonical import canonical_hash, canonical_json


DAILY_MANIFEST_GENESIS_HASH = "0" * 64
DAILY_OPERATION_PIPELINES = {
    "RESEARCH_REFRESH": "daily-research-refresh-v1",
    "OUTCOME_PROCESSING": "daily-outcome-processing-v1",
    "AFTER_HOURS_DISCOVERY": "daily-after-hours-discovery-v1",
    "AFTER_HOURS_REPRICE": "daily-after-hours-reprice-v1",
    "AFTER_HOURS_REPRICE_RETRY": "daily-after-hours-reprice-retry-v1",
    "NEXT_SESSION_PREPARATION": "daily-next-session-preparation-v1",
}
DAILY_OPERATION_BY_PIPELINE = {
    pipeline: operation for operation, pipeline in DAILY_OPERATION_PIPELINES.items()
}


# These are deliberately research times, not an execution cadence.  They match
# the human-approved pre-market freeze, open re-pricing, and intraday research
# windows.  Every slot remains read-only and independently calendar-gated.
ORDINARY_SCAN_SLOT_TIMES = (
    time(10, 0),
    time(11, 30),
    time(13, 30),
    time(15, 30),
)
TOP10_PRODUCER_SLOT_TIMES = (
    time(9, 20),
    time(9, 35),
)
RECOVERY_MAX_AGE = timedelta(hours=2)
DAILY_OPERATION_TIMES = (
    ("RESEARCH_REFRESH", time(8, 30)),
    ("TOP10_FREEZE", time(9, 20)),
    ("TOP10_REPRICE", time(9, 35)),
    *(("ORDINARY_SCAN", moment) for moment in ORDINARY_SCAN_SLOT_TIMES),
    ("OUTCOME_PROCESSING", time(16, 15)),
    ("AFTER_HOURS_DISCOVERY", time(16, 20)),
    ("AFTER_HOURS_REPRICE", time(16, 30)),
    ("NEXT_SESSION_PREPARATION", time(16, 40)),
)


@dataclass(frozen=True, slots=True)
class ScanSlot:
    trading_date: date
    slot_at: datetime
    kind: str = "READ_ONLY_RESEARCH"

    def __post_init__(self) -> None:
        if self.slot_at.tzinfo is None or self.slot_at.utcoffset() is None:
            raise ValueError("slot_at must be timezone-aware")
        object.__setattr__(self, "slot_at", self.slot_at.astimezone(US_OPTIONS_TIMEZONE))
        if self.slot_at.date() != self.trading_date:
            raise ValueError("slot time must fall on trading_date")


@dataclass(frozen=True, slots=True)
class DailyOperationSlot:
    trading_date: date
    slot_at: datetime
    operation: str

    def __post_init__(self) -> None:
        if self.slot_at.tzinfo is None or self.slot_at.utcoffset() is None:
            raise ValueError("slot_at must be timezone-aware")
        object.__setattr__(self, "slot_at", self.slot_at.astimezone(US_OPTIONS_TIMEZONE))
        if self.slot_at.date() != self.trading_date:
            raise ValueError("operation slot must fall on trading_date")


@dataclass(frozen=True, slots=True)
class ScanRun:
    scan_run_id: str
    trading_date: date
    slot_at: datetime
    pipeline_version: str
    status: str
    owner: str | None
    lease_expires_at: datetime | None
    duplicate_reason: str | None = None
    result_hash: str | None = None
    failure_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ScanAcquireResult:
    acquired: bool
    run: ScanRun
    duplicate_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ProducerRunResult:
    scan_run_id: str
    trading_date: date
    slot_at: datetime
    pipeline_version: str
    producer_status: str
    reason_codes: tuple[str, ...]
    missing_symbols: tuple[str, ...]
    written_count: int
    producer_slot: str | None
    producer_run_id: str | None
    result_hash: str
    evidence_hash: str
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class DailyOperationResult:
    scan_run_id: str
    operation: str
    status: str
    payload: dict[str, object]
    result_hash: str
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class DailyOperationManifest:
    sequence: int
    manifest_id: str
    trading_date: date
    payload: dict[str, object]
    previous_hash: str
    manifest_hash: str
    recorded_at: datetime


def slots_for_session(snapshot: UsOptionsCalendarSnapshot, trading_date: date) -> tuple[ScanSlot, ...]:
    """Return ordinary recovery slots for a broker-published open day."""
    if snapshot.status is not CalendarStatus.READY:
        return ()
    session = snapshot.session_for(trading_date)
    if session is None:
        return ()
    return tuple(
        ScanSlot(trading_date, datetime.combine(trading_date, moment, US_OPTIONS_TIMEZONE))
        for moment in ORDINARY_SCAN_SLOT_TIMES
        if datetime.combine(trading_date, moment, US_OPTIONS_TIMEZONE) < session.close_et
    )


def daily_operation_slots_for_session(
    snapshot: UsOptionsCalendarSnapshot,
    trading_date: date,
) -> tuple[DailyOperationSlot, ...]:
    """Return the complete fixed ET operation rhythm for one broker-open day."""

    if snapshot.status is not CalendarStatus.READY:
        return ()
    session = snapshot.session_for(trading_date)
    if session is None:
        return ()
    operations: list[DailyOperationSlot] = []
    for operation, moment in DAILY_OPERATION_TIMES:
        scheduled = (
            session.close_et + timedelta(minutes=20)
            if operation == "AFTER_HOURS_DISCOVERY"
            else session.close_et + timedelta(minutes=30)
            if operation == "AFTER_HOURS_REPRICE"
            else session.close_et + timedelta(minutes=40)
            if operation == "NEXT_SESSION_PREPARATION"
            else datetime.combine(trading_date, moment, US_OPTIONS_TIMEZONE)
        )
        if operation == "ORDINARY_SCAN" and scheduled >= session.close_et:
            continue
        operations.append(DailyOperationSlot(trading_date, scheduled, operation))
    return tuple(operations)


def exact_top10_slot(
    snapshot: UsOptionsCalendarSnapshot,
    *,
    now: datetime,
) -> ScanSlot | None:
    """Return only the exact 09:20/09:35 ET producer slot.

    Unlike the research scheduler, the independent Top-10 ledger never catches
    up a missed run.  A later heartbeat or restart must therefore return no
    slot rather than repricing against a fabricated wall-clock default.
    """

    instant = _utc(now).astimezone(US_OPTIONS_TIMEZONE)
    wall = instant.timetz().replace(
        second=0,
        microsecond=0,
        tzinfo=None,
    )
    if wall not in TOP10_PRODUCER_SLOT_TIMES:
        return None
    if snapshot.status is not CalendarStatus.READY:
        return None
    trading_date = instant.date()
    if snapshot.session_for(trading_date) is None:
        return None
    return ScanSlot(
        trading_date,
        instant.replace(second=0, microsecond=0),
        kind=("TOP10_FREEZE" if wall == time(9, 20) else "TOP10_REPRICE"),
    )


class ScanRunStore:
    """SQLite/WAL scan ledger with one transactionally leased run per slot."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._verified_manifest_tail: dict[str, tuple[int, str]] = {}
        self._connection = sqlite3.connect(
            self.path, timeout=10, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout=10000")
        self._journal_mode = str(self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS scan_runs (
              scan_run_id TEXT PRIMARY KEY,
              trading_date TEXT NOT NULL,
              slot_at TEXT NOT NULL,
              pipeline_version TEXT NOT NULL,
              status TEXT NOT NULL,
              owner TEXT,
              lease_expires_at TEXT,
              result_hash TEXT,
              failure_reason TEXT,
              duplicate_reason TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              UNIQUE(trading_date, slot_at, pipeline_version)
            );
            CREATE INDEX IF NOT EXISTS scan_runs_recovery
              ON scan_runs(status, trading_date, slot_at);
            CREATE TABLE IF NOT EXISTS scan_operational_timings (
              scan_run_id TEXT PRIMARY KEY REFERENCES scan_runs(scan_run_id),
              timing_json TEXT NOT NULL,
              timing_hash TEXT NOT NULL,
              recorded_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS scan_operational_timings_no_update
              BEFORE UPDATE ON scan_operational_timings
              BEGIN SELECT RAISE(ABORT, 'scan operational timings are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS scan_operational_timings_no_delete
              BEFORE DELETE ON scan_operational_timings
              BEGIN SELECT RAISE(ABORT, 'scan operational timings are append-only'); END;
            CREATE TABLE IF NOT EXISTS top10_producer_results (
              scan_run_id TEXT PRIMARY KEY REFERENCES scan_runs(scan_run_id),
              trading_date TEXT NOT NULL,
              slot_at TEXT NOT NULL,
              pipeline_version TEXT NOT NULL,
              producer_status TEXT NOT NULL,
              reason_codes_json TEXT NOT NULL,
              missing_symbols_json TEXT NOT NULL,
              written_count INTEGER NOT NULL,
              producer_slot TEXT,
              producer_run_id TEXT,
              result_hash TEXT NOT NULL,
              evidence_hash TEXT NOT NULL,
              recorded_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS top10_producer_results_latest
              ON top10_producer_results(
                pipeline_version, trading_date, slot_at DESC
              );
            CREATE TRIGGER IF NOT EXISTS top10_producer_results_no_update
              BEFORE UPDATE ON top10_producer_results
              BEGIN SELECT RAISE(ABORT, 'top10 producer results are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS top10_producer_results_no_delete
              BEFORE DELETE ON top10_producer_results
              BEGIN SELECT RAISE(ABORT, 'top10 producer results are append-only'); END;
            CREATE TABLE IF NOT EXISTS daily_operation_results (
              scan_run_id TEXT PRIMARY KEY REFERENCES scan_runs(scan_run_id),
              operation TEXT NOT NULL,
              status TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              result_hash TEXT NOT NULL,
              recorded_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS daily_operation_results_latest
              ON daily_operation_results(operation, recorded_at DESC);
            CREATE TRIGGER IF NOT EXISTS daily_operation_results_no_update
              BEFORE UPDATE ON daily_operation_results
              BEGIN SELECT RAISE(ABORT, 'daily operation results are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS daily_operation_results_no_delete
              BEFORE DELETE ON daily_operation_results
              BEGIN SELECT RAISE(ABORT, 'daily operation results are append-only'); END;
            CREATE TABLE IF NOT EXISTS daily_operation_manifests (
              sequence INTEGER PRIMARY KEY AUTOINCREMENT,
              manifest_id TEXT NOT NULL UNIQUE,
              trading_date TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              previous_hash TEXT NOT NULL,
              manifest_hash TEXT NOT NULL,
              recorded_at TEXT NOT NULL,
              UNIQUE(trading_date, manifest_hash)
            );
            CREATE INDEX IF NOT EXISTS daily_operation_manifests_latest
              ON daily_operation_manifests(trading_date, sequence DESC);
            CREATE TRIGGER IF NOT EXISTS daily_operation_manifests_no_update
              BEFORE UPDATE ON daily_operation_manifests
              BEGIN SELECT RAISE(ABORT, 'daily operation manifests are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS daily_operation_manifests_no_delete
              BEFORE DELETE ON daily_operation_manifests
              BEGIN SELECT RAISE(ABORT, 'daily operation manifests are append-only'); END;
            """
        )

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "ScanRunStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def acquire(
        self, slot: ScanSlot, *, pipeline_version: str, owner: str | None = None,
        now: datetime | None = None, lease_seconds: int = 90,
    ) -> ScanAcquireResult:
        """Atomically claim a slot, or return its durable fixed duplicate reason."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        version = _text(pipeline_version, "pipeline_version")
        holder = owner or uuid.uuid4().hex
        instant = _utc(now or datetime.now(timezone.utc))
        expiry = instant + timedelta(seconds=lease_seconds)
        slot_text = _timestamp(slot.slot_at)
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM scan_runs WHERE trading_date=? AND slot_at=? AND pipeline_version=?",
                (slot.trading_date.isoformat(), slot_text, version),
            ).fetchone()
            if row is None:
                run_id = f"scan.{uuid.uuid4().hex}"
                self._connection.execute(
                    "INSERT INTO scan_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (run_id, slot.trading_date.isoformat(), slot_text, version, "LEASED", holder,
                     _timestamp(expiry), None, None, None, _timestamp(instant), _timestamp(instant)),
                )
                return ScanAcquireResult(True, self.get(run_id), None)
            prior = _run(row)
            if prior.status == "COMPLETED":
                return ScanAcquireResult(False, prior, "SLOT_ALREADY_COMPLETED")
            if prior.status == "MISSED_NOT_REPLAYED":
                return ScanAcquireResult(False, prior, "SLOT_MISSED_NOT_REPLAYED")
            if prior.status == "FAILED":
                return ScanAcquireResult(False, prior, "SLOT_ALREADY_FAILED")
            if prior.lease_expires_at is not None and prior.lease_expires_at > instant:
                return ScanAcquireResult(False, prior, "SLOT_LEASE_HELD")
            self._expire_leased_row(row, now=instant)
            return ScanAcquireResult(
                False,
                self.get(prior.scan_run_id),
                "SLOT_LEASE_EXPIRED",
            )

    def heartbeat(self, scan_run_id: str, *, owner: str, now: datetime, lease_seconds: int = 90) -> bool:
        expiry = _utc(now) + timedelta(seconds=lease_seconds)
        # Lease renewal must not wait behind unrelated users of the primary
        # store connection or its process-local lock.  A scan may spend longer
        # than one lease period in bounded broker reads while other scheduler
        # projections inspect the same store.  Use one short WAL writer so the
        # liveness proof remains independent of those read paths.
        connection = sqlite3.connect(
            self.path,
            timeout=10,
            isolation_level=None,
        )
        try:
            connection.execute("PRAGMA busy_timeout=10000")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "UPDATE scan_runs SET lease_expires_at=?, updated_at=? WHERE scan_run_id=? AND status='LEASED' AND owner=?",
                (_timestamp(expiry), _timestamp(now), scan_run_id, owner),
            )
            connection.execute("COMMIT")
            return cursor.rowcount == 1
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    def complete(
        self,
        scan_run_id: str,
        *,
        owner: str,
        result_hash: str,
        now: datetime,
        operational_timing: Mapping[str, object] | None = None,
    ) -> ScanRun:
        normalized_timing: Mapping[str, object] | None = None
        if operational_timing is not None:
            try:
                normalized_timing = _normalise_operational_timing(
                    operational_timing,
                    scan_run_id=scan_run_id,
                )
            except (TypeError, ValueError):
                # Performance telemetry is observation-only.  A malformed
                # timing document is dropped instead of changing the scan's
                # authoritative terminal state or result hash.
                normalized_timing = None
        if normalized_timing is None:
            return self._finish(
                scan_run_id,
                owner=owner,
                status="COMPLETED",
                result_hash=_text(result_hash, "result_hash"),
                now=now,
            )
        return self._complete_with_operational_timing(
            scan_run_id,
            owner=owner,
            result_hash=_text(result_hash, "result_hash"),
            now=now,
            operational_timing=normalized_timing,
        )

    def operational_timing(
        self,
        scan_run_id: str,
    ) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM scan_operational_timings WHERE scan_run_id=?",
                (_text(scan_run_id, "scan_run_id"),),
            ).fetchone()
            if row is None:
                return None
            payload = json.loads(row["timing_json"])
            normalized = _normalise_operational_timing(
                payload,
                scan_run_id=scan_run_id,
            )
            if canonical_json(normalized) != row["timing_json"]:
                raise ValueError("stored scan operational timing is not canonical")
            expected = canonical_hash(
                {
                    "schema": "options_copilot.scan_operational_timing_record.v1",
                    "scan_run_id": scan_run_id,
                    "timing": normalized,
                    "recorded_at": row["recorded_at"],
                }
            )
            if expected != row["timing_hash"]:
                raise ValueError("scan operational timing integrity is invalid")
            return {
                **normalized,
                "timing_hash": row["timing_hash"],
                "recorded_at": row["recorded_at"],
            }

    def complete_with_producer_result(
        self,
        scan_run_id: str,
        *,
        owner: str,
        result_hash: str,
        producer_status: str,
        reason_codes: tuple[str, ...],
        missing_symbols: tuple[str, ...],
        written_count: int,
        producer_slot: str | None,
        producer_run_id: str | None,
        evidence_hash: str,
        now: datetime,
    ) -> ScanRun:
        """Atomically finish a slot and append its safe producer projection."""

        if isinstance(written_count, bool) or not isinstance(written_count, int):
            raise TypeError("written_count must be an integer")
        if written_count < 0 or written_count > 10:
            raise ValueError("written_count must be between zero and ten")
        digest = _text(result_hash, "result_hash")
        projection_hash = _text(evidence_hash, "evidence_hash")
        status = _text(producer_status, "producer_status").upper()
        reasons_json = json.dumps(reason_codes, separators=(",", ":"))
        missing_json = json.dumps(missing_symbols, separators=(",", ":"))
        recorded_at = _utc(now)
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM scan_runs WHERE scan_run_id=?",
                (scan_run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(scan_run_id)
            cursor = self._connection.execute(
                "UPDATE scan_runs SET status='COMPLETED', result_hash=?, failure_reason=NULL, lease_expires_at=NULL, updated_at=? "
                "WHERE scan_run_id=? AND status='LEASED' AND owner=? AND lease_expires_at>?",
                (digest, _timestamp(recorded_at), scan_run_id, owner, _timestamp(recorded_at)),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("scan run is not leased by this owner")
            self._connection.execute(
                "INSERT INTO top10_producer_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    scan_run_id,
                    row["trading_date"],
                    row["slot_at"],
                    row["pipeline_version"],
                    status,
                    reasons_json,
                    missing_json,
                    written_count,
                    _optional_text(producer_slot),
                    _optional_text(producer_run_id),
                    digest,
                    projection_hash,
                    _timestamp(recorded_at),
                ),
            )
            return self.get(scan_run_id)

    def fail(self, scan_run_id: str, *, owner: str, reason: str, now: datetime) -> ScanRun:
        return self._finish(scan_run_id, owner=owner, status="FAILED", failure_reason=_text(reason, "reason"), now=now)

    def complete_with_daily_result(
        self,
        scan_run_id: str,
        *,
        owner: str,
        operation: str,
        payload: dict[str, object],
        now: datetime,
    ) -> ScanRun:
        """Atomically terminalize one leased daily operation and persist its payload."""

        return self._finish_with_daily_result(
            scan_run_id,
            owner=owner,
            operation=operation,
            status="COMPLETED",
            payload=payload,
            now=now,
        )

    def fail_with_daily_result(
        self,
        scan_run_id: str,
        *,
        owner: str,
        operation: str,
        reason: str,
        payload: dict[str, object],
        now: datetime,
    ) -> ScanRun:
        """Atomically fail one leased daily operation and persist safe evidence."""

        return self._finish_with_daily_result(
            scan_run_id,
            owner=owner,
            operation=operation,
            status="FAILED",
            payload=payload,
            failure_reason=_text(reason, "reason"),
            now=now,
        )

    def expire_leases(self, *, now: datetime) -> tuple[ScanRun, ...]:
        """Terminalize expired attempts so they are never silently re-leased."""

        instant = _utc(now)
        with self._transaction():
            rows = self._connection.execute(
                "SELECT * FROM scan_runs "
                "WHERE status='LEASED' AND lease_expires_at IS NOT NULL "
                "AND lease_expires_at<=? ORDER BY slot_at, scan_run_id",
                (_timestamp(instant),),
            ).fetchall()
            identifiers = tuple(str(row["scan_run_id"]) for row in rows)
            for row in rows:
                self._expire_leased_row(row, now=instant)
            return tuple(self.get(identifier) for identifier in identifiers)

    def _expire_leased_row(self, row: sqlite3.Row, *, now: datetime) -> None:
        operation = DAILY_OPERATION_BY_PIPELINE.get(str(row["pipeline_version"]))
        digest: str | None = None
        payload: dict[str, object] | None = None
        if operation is not None:
            payload = {
                "schema": "options_copilot.daily_operation_result.v1",
                "operation": operation,
                "status": "FAILED",
                "observed_at": _timestamp(now),
                "reason_codes": ["LEASE_EXPIRED"],
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }
            digest = canonical_hash(
                {
                    "schema": "options_copilot.daily_operation_result_record.v1",
                    "scan_run_id": str(row["scan_run_id"]),
                    "operation": operation,
                    "status": "FAILED",
                    "payload": payload,
                    "recorded_at": _timestamp(now),
                }
            )
        self._connection.execute(
            "UPDATE scan_runs SET status='FAILED', owner=NULL, "
            "lease_expires_at=NULL, result_hash=?, failure_reason='LEASE_EXPIRED', "
            "updated_at=? WHERE scan_run_id=? AND status='LEASED'",
            (digest, _timestamp(now), str(row["scan_run_id"])),
        )
        if payload is not None and digest is not None:
            self._connection.execute(
                "INSERT INTO daily_operation_results VALUES (?,?,?,?,?,?)",
                (
                    str(row["scan_run_id"]),
                    operation,
                    "FAILED",
                    canonical_json(payload),
                    digest,
                    _timestamp(now),
                ),
            )

    def daily_result(self, scan_run_id: str) -> DailyOperationResult | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT daily_operation_results.*, scan_runs.result_hash AS run_result_hash, "
                "scan_runs.status AS run_status FROM daily_operation_results "
                "JOIN scan_runs USING(scan_run_id) WHERE scan_run_id=?",
                (scan_run_id,),
            ).fetchone()
            return None if row is None else _daily_operation_result(row)

    def latest_daily_result(self, operation: str) -> DailyOperationResult | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT daily_operation_results.*, scan_runs.result_hash AS run_result_hash, "
                "scan_runs.status AS run_status FROM daily_operation_results "
                "JOIN scan_runs USING(scan_run_id) WHERE operation=? "
                "ORDER BY recorded_at DESC LIMIT 1",
                (_text(operation, "operation").upper(),),
            ).fetchone()
            return None if row is None else _daily_operation_result(row)

    def record_daily_manifest(
        self,
        *,
        trading_date: date,
        payload: dict[str, object],
        now: datetime,
    ) -> DailyOperationManifest:
        payload_json = canonical_json(payload)
        instant = _utc(now)
        with self._transaction():
            previous = self._connection.execute(
                "SELECT * FROM daily_operation_manifests "
                "WHERE trading_date=? ORDER BY sequence DESC LIMIT 1",
                (trading_date.isoformat(),),
            ).fetchone()
            if previous is not None and previous["payload_json"] == payload_json:
                return _daily_operation_manifest(previous)
            previous_hash = (
                DAILY_MANIFEST_GENESIS_HASH
                if previous is None
                else str(previous["manifest_hash"])
            )
            next_row = self._connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS value "
                "FROM daily_operation_manifests"
            ).fetchone()
            assert next_row is not None
            sequence = int(next_row["value"])
            manifest_id = f"daily-manifest.{uuid.uuid4().hex}"
            recorded_at = _timestamp(instant)
            digest = canonical_hash(
                {
                    "schema": "options_copilot.daily_operation_manifest_record.v1",
                    "sequence": sequence,
                    "manifest_id": manifest_id,
                    "trading_date": trading_date.isoformat(),
                    "payload": payload,
                    "previous_hash": previous_hash,
                    "recorded_at": recorded_at,
                }
            )
            existing = self._connection.execute(
                "SELECT * FROM daily_operation_manifests "
                "WHERE trading_date=? AND manifest_hash=?",
                (trading_date.isoformat(), digest),
            ).fetchone()
            if existing is None:
                self._connection.execute(
                    "INSERT INTO daily_operation_manifests VALUES (?,?,?,?,?,?,?)",
                    (
                        sequence,
                        manifest_id,
                        trading_date.isoformat(),
                        payload_json,
                        previous_hash,
                        digest,
                        recorded_at,
                    ),
                )
                existing = self._connection.execute(
                    "SELECT * FROM daily_operation_manifests "
                    "WHERE trading_date=? AND manifest_hash=?",
                    (trading_date.isoformat(), digest),
                ).fetchone()
            assert existing is not None
            return _daily_operation_manifest(existing)

    def latest_daily_manifest(
        self,
        *,
        trading_date: date,
    ) -> DailyOperationManifest | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM daily_operation_manifests WHERE trading_date=? "
                "ORDER BY sequence DESC LIMIT 1",
                (trading_date.isoformat(),),
            ).fetchone()
            if row is None:
                return None
            self.assert_daily_integrity(trading_date=trading_date)
            return _daily_operation_manifest(row)

    def latest_completed_daily_manifest(self) -> DailyOperationManifest | None:
        with self._lock:
            rows = self._connection.execute(
                "SELECT manifests.* FROM daily_operation_manifests AS manifests "
                "JOIN (SELECT trading_date, MAX(sequence) AS sequence "
                "FROM daily_operation_manifests GROUP BY trading_date) AS latest "
                "ON manifests.sequence=latest.sequence ORDER BY manifests.sequence DESC"
            ).fetchall()
            terminal = {"COMPLETED", "FAILED", "MISSED_NOT_REPLAYED"}
            for row in rows:
                manifest = _daily_operation_manifest(row)
                runs = manifest.payload.get("runs")
                if (
                    isinstance(runs, list)
                    and runs
                    and all(
                        isinstance(item, dict)
                        and str(item.get("status", "")).upper() in terminal
                        for item in runs
                    )
                ):
                    self.assert_daily_integrity(trading_date=manifest.trading_date)
                    return manifest
            return None

    def assert_daily_integrity(self, *, trading_date: date | None = None) -> None:
        with self._lock:
            result_rows = self._connection.execute(
                "SELECT daily_operation_results.*, scan_runs.result_hash AS run_result_hash, "
                "scan_runs.status AS run_status FROM daily_operation_results "
                "JOIN scan_runs USING(scan_run_id) ORDER BY recorded_at, scan_run_id"
            ).fetchall()
            for row in result_rows:
                _daily_operation_result(row)
            trading_dates = (
                (trading_date.isoformat(),)
                if trading_date is not None
                else tuple(
                    str(row["trading_date"])
                    for row in self._connection.execute(
                        "SELECT DISTINCT trading_date FROM daily_operation_manifests "
                        "ORDER BY trading_date"
                    ).fetchall()
                )
            )
            for trading_date_text in trading_dates:
                verified_sequence, expected_previous = (
                    self._verified_manifest_tail.get(
                        trading_date_text,
                        (0, DAILY_MANIFEST_GENESIS_HASH),
                    )
                )
                rows = self._connection.execute(
                    "SELECT * FROM daily_operation_manifests "
                    "WHERE trading_date=? AND sequence>? ORDER BY sequence",
                    (trading_date_text, verified_sequence),
                ).fetchall()
                for row in rows:
                    if str(row["previous_hash"]) != expected_previous:
                        raise ValueError("daily operation manifest chain is invalid")
                    manifest = _daily_operation_manifest(row)
                    verified_sequence = manifest.sequence
                    expected_previous = manifest.manifest_hash
                if rows:
                    self._verified_manifest_tail[trading_date_text] = (
                        verified_sequence,
                        expected_previous,
                    )

    def get(self, scan_run_id: str) -> ScanRun:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM scan_runs WHERE scan_run_id=?",
                (scan_run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(scan_run_id)
            return _run(row)

    def producer_result(self, scan_run_id: str) -> ProducerRunResult | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM top10_producer_results WHERE scan_run_id=?",
                (scan_run_id,),
            ).fetchone()
            return None if row is None else _producer_result(row)

    def latest_producer_result(
        self,
        *,
        trading_date: date,
        pipeline_version: str,
    ) -> ProducerRunResult | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM top10_producer_results WHERE trading_date=? AND pipeline_version=? ORDER BY slot_at DESC, recorded_at DESC LIMIT 1",
                (trading_date.isoformat(), _text(pipeline_version, "pipeline_version")),
            ).fetchone()
            return None if row is None else _producer_result(row)

    def runs_for_slot(self, slot: ScanSlot, *, pipeline_version: str) -> tuple[ScanRun, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM scan_runs WHERE trading_date=? AND slot_at=? AND pipeline_version=?",
                (slot.trading_date.isoformat(), _timestamp(slot.slot_at), _text(pipeline_version, "pipeline_version")),
            ).fetchall()
            return tuple(_run(row) for row in rows)

    def recover(
        self,
        slots: Iterable[ScanSlot],
        *,
        pipeline_version: str,
        owner: str,
        now: datetime,
        lease_seconds: int = 90,
    ) -> ScanAcquireResult | None:
        """Persist old missed slots and lease at most one current recoverable slot."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        instant = _utc(now)
        eligible: list[ScanSlot] = []
        for slot in sorted(slots, key=lambda value: value.slot_at):
            if slot.slot_at > instant:
                continue
            if instant - slot.slot_at > RECOVERY_MAX_AGE:
                self._mark_missed(slot, pipeline_version=pipeline_version, now=instant)
            else:
                eligible.append(slot)
        if not eligible:
            return None
        return self.acquire(
            eligible[-1],
            pipeline_version=pipeline_version,
            owner=owner,
            now=instant,
            lease_seconds=lease_seconds,
        )

    def _mark_missed(self, slot: ScanSlot, *, pipeline_version: str, now: datetime) -> None:
        version = _text(pipeline_version, "pipeline_version")
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM scan_runs WHERE trading_date=? AND slot_at=? AND pipeline_version=?",
                (slot.trading_date.isoformat(), _timestamp(slot.slot_at), version),
            ).fetchone()
            if row is None:
                run_id = f"scan.{uuid.uuid4().hex}"
                self._connection.execute("INSERT INTO scan_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
                    run_id, slot.trading_date.isoformat(), _timestamp(slot.slot_at), version,
                    "MISSED_NOT_REPLAYED", None, None, None, None, "MISSED_NOT_REPLAYED",
                    _timestamp(now), _timestamp(now)))

    def _finish(self, scan_run_id: str, *, owner: str, status: str, now: datetime, result_hash: str | None = None, failure_reason: str | None = None) -> ScanRun:
        instant = _utc(now)
        with self._transaction():
            cursor = self._connection.execute(
                "UPDATE scan_runs SET status=?, result_hash=?, failure_reason=?, lease_expires_at=NULL, updated_at=? "
                "WHERE scan_run_id=? AND status='LEASED' AND owner=? "
                "AND lease_expires_at>?",
                (status, result_hash, failure_reason, _timestamp(instant), scan_run_id, owner, _timestamp(instant)),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("scan run is not leased by this owner")
            return self.get(scan_run_id)

    def _complete_with_operational_timing(
        self,
        scan_run_id: str,
        *,
        owner: str,
        result_hash: str,
        now: datetime,
        operational_timing: Mapping[str, object],
    ) -> ScanRun:
        instant = _utc(now)
        normalized = dict(operational_timing)
        body = canonical_json(normalized)
        timing_hash = canonical_hash(
            {
                "schema": "options_copilot.scan_operational_timing_record.v1",
                "scan_run_id": scan_run_id,
                "timing": normalized,
                "recorded_at": _timestamp(instant),
            }
        )
        with self._transaction():
            cursor = self._connection.execute(
                "UPDATE scan_runs SET status='COMPLETED', result_hash=?, "
                "failure_reason=NULL, lease_expires_at=NULL, updated_at=? "
                "WHERE scan_run_id=? AND status='LEASED' AND owner=? "
                "AND lease_expires_at>?",
                (
                    result_hash,
                    _timestamp(instant),
                    scan_run_id,
                    owner,
                    _timestamp(instant),
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("scan run is not leased by this owner")
            self._connection.execute(
                "INSERT INTO scan_operational_timings VALUES (?,?,?,?)",
                (
                    scan_run_id,
                    body,
                    timing_hash,
                    _timestamp(instant),
                ),
            )
            return self.get(scan_run_id)

    def _finish_with_daily_result(
        self,
        scan_run_id: str,
        *,
        owner: str,
        operation: str,
        status: str,
        payload: dict[str, object],
        now: datetime,
        failure_reason: str | None = None,
    ) -> ScanRun:
        instant = _utc(now)
        normalized_operation = _text(operation, "operation").upper()
        body = canonical_json(payload)
        digest = canonical_hash(
            {
                "schema": "options_copilot.daily_operation_result_record.v1",
                "scan_run_id": scan_run_id,
                "operation": normalized_operation,
                "status": status,
                "payload": payload,
                "recorded_at": _timestamp(instant),
            }
        )
        with self._transaction():
            cursor = self._connection.execute(
                "UPDATE scan_runs SET status=?, result_hash=?, failure_reason=?, "
                "lease_expires_at=NULL, updated_at=? WHERE scan_run_id=? "
                "AND status='LEASED' AND owner=? AND lease_expires_at>?",
                (
                    status,
                    digest,
                    failure_reason,
                    _timestamp(instant),
                    scan_run_id,
                    owner,
                    _timestamp(instant),
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("scan run is not actively leased by this owner")
            self._connection.execute(
                "INSERT INTO daily_operation_results VALUES (?,?,?,?,?,?)",
                (
                    scan_run_id,
                    normalized_operation,
                    status,
                    body,
                    digest,
                    _timestamp(instant),
                ),
            )
            return self.get(scan_run_id)

    def _transaction(self):
        return _ImmediateTransaction(self._connection, self._lock)


class _ImmediateTransaction:
    def __init__(self, connection: sqlite3.Connection, lock: threading.RLock) -> None:
        self.connection, self.lock = connection, lock
    def __enter__(self) -> "_ImmediateTransaction":
        self.lock.acquire()
        try:
            self.connection.execute("BEGIN IMMEDIATE")
        except BaseException:
            self.lock.release()
            raise
        return self
    def __exit__(self, exc_type: object, *_: object) -> None:
        try:
            self.connection.execute("ROLLBACK" if exc_type else "COMMIT")
        finally:
            self.lock.release()


def _run(row: sqlite3.Row) -> ScanRun:
    return ScanRun(row["scan_run_id"], date.fromisoformat(row["trading_date"]), _parse_timestamp(row["slot_at"]), row["pipeline_version"], row["status"], row["owner"], _parse_timestamp(row["lease_expires_at"]) if row["lease_expires_at"] else None, row["duplicate_reason"], row["result_hash"], row["failure_reason"])
def _producer_result(row: sqlite3.Row) -> ProducerRunResult:
    reasons = json.loads(row["reason_codes_json"])
    missing = json.loads(row["missing_symbols_json"])
    if not isinstance(reasons, list) or not isinstance(missing, list):
        raise ValueError("stored producer result arrays are invalid")
    return ProducerRunResult(
        scan_run_id=row["scan_run_id"],
        trading_date=date.fromisoformat(row["trading_date"]),
        slot_at=_parse_timestamp(row["slot_at"]),
        pipeline_version=row["pipeline_version"],
        producer_status=row["producer_status"],
        reason_codes=tuple(str(item) for item in reasons),
        missing_symbols=tuple(str(item) for item in missing),
        written_count=int(row["written_count"]),
        producer_slot=row["producer_slot"],
        producer_run_id=row["producer_run_id"],
        result_hash=row["result_hash"],
        evidence_hash=row["evidence_hash"],
        recorded_at=_parse_timestamp(row["recorded_at"]),
    )
def _daily_operation_result(row: sqlite3.Row) -> DailyOperationResult:
    payload = json.loads(row["payload_json"])
    if not isinstance(payload, dict):
        raise ValueError("stored daily operation result must be an object")
    if canonical_json(payload) != row["payload_json"]:
        raise ValueError("stored daily operation result is not canonical")
    expected = canonical_hash(
        {
            "schema": "options_copilot.daily_operation_result_record.v1",
            "scan_run_id": row["scan_run_id"],
            "operation": row["operation"],
            "status": row["status"],
            "payload": payload,
            "recorded_at": row["recorded_at"],
        }
    )
    if (
        expected != row["result_hash"]
        or row["run_result_hash"] != row["result_hash"]
        or row["run_status"] != row["status"]
    ):
        raise ValueError("daily operation result integrity is invalid")
    return DailyOperationResult(
        scan_run_id=row["scan_run_id"],
        operation=row["operation"],
        status=row["status"],
        payload=dict(payload),
        result_hash=row["result_hash"],
        recorded_at=_parse_timestamp(row["recorded_at"]),
    )
def _daily_operation_manifest(row: sqlite3.Row) -> DailyOperationManifest:
    payload = json.loads(row["payload_json"])
    if not isinstance(payload, dict):
        raise ValueError("stored daily operation manifest must be an object")
    if canonical_json(payload) != row["payload_json"]:
        raise ValueError("stored daily operation manifest is not canonical")
    expected = canonical_hash(
        {
            "schema": "options_copilot.daily_operation_manifest_record.v1",
            "sequence": row["sequence"],
            "manifest_id": row["manifest_id"],
            "trading_date": row["trading_date"],
            "payload": payload,
            "previous_hash": row["previous_hash"],
            "recorded_at": row["recorded_at"],
        }
    )
    if expected != row["manifest_hash"]:
        raise ValueError("daily operation manifest integrity is invalid")
    return DailyOperationManifest(
        sequence=int(row["sequence"]),
        manifest_id=row["manifest_id"],
        trading_date=date.fromisoformat(row["trading_date"]),
        payload=dict(payload),
        previous_hash=row["previous_hash"],
        manifest_hash=row["manifest_hash"],
        recorded_at=_parse_timestamp(row["recorded_at"]),
    )
def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None: raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)
def _timestamp(value: datetime) -> str: return _utc(value).isoformat(timespec="microseconds")
def _parse_timestamp(value: str) -> datetime: return datetime.fromisoformat(value).astimezone(timezone.utc)
def _text(value: object, name: str) -> str:
    result = str(value).strip()
    if not result: raise ValueError(f"{name} cannot be blank")
    return result
def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _normalise_operational_timing(
    value: object,
    *,
    scan_run_id: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError("scan operational timing must be an object")
    if value.get("schema") != "options_copilot.scan_operational_timing.v1":
        raise ValueError("scan operational timing schema is invalid")
    if value.get("scan_run_id") != scan_run_id:
        raise ValueError("scan operational timing run binding is invalid")
    total = value.get("total_duration_ms")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise ValueError("scan operational total duration is invalid")
    raw_stages = value.get("stages")
    if (
        isinstance(raw_stages, (str, bytes, bytearray))
        or not isinstance(raw_stages, Iterable)
    ):
        raise TypeError("scan operational stages must be iterable")
    stages: list[dict[str, object]] = []
    for raw in raw_stages:
        if not isinstance(raw, Mapping):
            raise TypeError("scan operational stage must be an object")
        stage = str(raw.get("stage", "")).strip().upper()
        duration = raw.get("duration_ms")
        if (
            not stage
            or len(stage) > 64
            or any(not (character.isalnum() or character == "_") for character in stage)
            or isinstance(duration, bool)
            or not isinstance(duration, int)
            or duration < 0
        ):
            raise ValueError("scan operational stage is invalid")
        stages.append({"stage": stage, "duration_ms": duration})
    if len(stages) > 32 or sum(int(row["duration_ms"]) for row in stages) > total:
        raise ValueError("scan operational stage durations are invalid")
    if value.get("decision_authority") != "OBSERVATION_ONLY":
        raise ValueError("scan operational timing authority is invalid")
    if value.get("affects_decision") is not False:
        raise ValueError("scan operational timing cannot affect decisions")
    return {
        "schema": "options_copilot.scan_operational_timing.v1",
        "scan_run_id": scan_run_id,
        "total_duration_ms": total,
        "stages": tuple(stages),
        "decision_authority": "OBSERVATION_ONLY",
        "affects_decision": False,
    }
