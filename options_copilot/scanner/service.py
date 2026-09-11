"""One-port heartbeat service for authority-free decision research."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
import inspect
import json
import threading
from time import monotonic
from typing import Callable, Protocol
import uuid

from options_copilot.after_hours_indicative import (
    after_hours_candidate_identity_manifest,
    after_hours_campaign_lineage_hash,
    after_hours_campaign_progress_status,
)
from options_copilot.external_bundle_commit import (
    ExternalBundleCommitGuard,
    ExternalBundleError,
    ExternalBundleSnapshot,
    external_slot_for_instant,
)
from options_copilot.market.session_calendar import (
    CalendarStatus,
    DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS,
    US_OPTIONS_TIMEZONE,
    UsOptionsCalendarSnapshot,
)
from options_copilot.storage.canonical import canonical_hash
from .operation_context import ScheduledOperationContext
from .scheduler import (
    DAILY_OPERATION_PIPELINES,
    DailyOperationSlot,
    RECOVERY_MAX_AGE,
    ScanAcquireResult,
    ScanRunStore,
    ScanSlot,
    daily_operation_slots_for_session,
    exact_top10_slot,
    slots_for_session,
)


TOP10_PRODUCER_UNAVAILABLE = "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
TOP10_PRODUCER_FAILED = "TOP10_PRESELECTION_PRODUCER_FAILED"
DURABLE_PRODUCER_RESULT_STATUS_UNAVAILABLE = (
    "DURABLE_PRODUCER_RESULT_STATUS_UNAVAILABLE"
)
DAILY_CALLBACK_TIMEOUT_SECONDS = 60
DAILY_CALLBACK_LEASE_SECONDS = 120
DAILY_CALLBACK_START_GRACE_SECONDS = 0.01
SCANNER_CLOSE_TIMEOUT_SECONDS = 5.0
ORDINARY_SCAN_LEASE_SECONDS = 90
ORDINARY_SCAN_LEASE_HEARTBEAT_SECONDS = 30.0
AFTER_HOURS_REPRICE_RETRY_LIMIT = 6
CALENDAR_REFRESH_BACKOFF_SECONDS = (60.0, 120.0, 300.0)


def _after_hours_campaign_hash(payload: Mapping[str, object]) -> str:
    return after_hours_campaign_lineage_hash(payload)


def _valid_formal_descriptor(
    formal: Mapping[str, object],
    after_hours: Mapping[str, object],
    *,
    verified_subset: bool = False,
) -> bool:
    if after_hours_campaign_progress_status(after_hours) is None:
        return False
    descriptor = formal.get("descriptor")
    if (
        formal.get("schema") != "options_copilot.after_hours_formal_pools.v2"
        or not isinstance(descriptor, Mapping)
        or descriptor.get("schema")
        != "options_copilot.after_hours_formal_pool_descriptor.v2"
        or formal.get("descriptor_hash") != canonical_hash(descriptor)
        or formal.get("campaign_hash") != _after_hours_campaign_hash(after_hours)
        or descriptor.get("campaign_hash") != formal.get("campaign_hash")
    ):
        return False
    if not all(
        descriptor.get(field) == formal.get(field)
        for field in (
            "materialization_revision_hash",
            "campaign_observed_at",
            "materialized_at",
            "equity_pool_hash",
            "equity_pool_reference_hash",
            "equity_research_count",
            "equity_selected_count",
            "option_pool_hash",
            "option_pool_scan_run_id",
            "option_structure_count",
        )
    ):
        return False
    equity_research_count = formal.get("equity_research_count")
    equity_selected_count = formal.get("equity_selected_count")
    option_structure_count = formal.get("option_structure_count")
    if any(
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        for value in (
            equity_research_count,
            equity_selected_count,
            option_structure_count,
        )
    ):
        return False
    assert isinstance(equity_research_count, int)
    assert isinstance(equity_selected_count, int)
    assert isinstance(option_structure_count, int)
    maximum_option_structures = (
        equity_research_count if verified_subset else equity_selected_count
    )
    if (
        equity_selected_count > equity_research_count
        or option_structure_count > maximum_option_structures
        or option_structure_count > 10
    ):
        return False
    identity_manifest = _after_hours_candidate_identity_manifest(after_hours)
    if identity_manifest is None or option_structure_count > len(identity_manifest):
        return False
    identity_hash = descriptor.get("option_candidate_identity_hash")
    if option_structure_count == 0:
        return identity_hash == canonical_hash(())
    if len(identity_manifest) == option_structure_count:
        return identity_hash == canonical_hash(identity_manifest)
    # A provider-bound payload has already been checked against the current
    # equity and option stores by the production cross-store verifier.  The
    # raw after-hours campaign intentionally retains unselected structures, so
    # its complete identity manifest cannot reproduce a thesis-bound subset.
    # Keep the weaker durable-only path strict while accepting only a hash-
    # shaped subset descriptor from the verified provider contract.
    return bool(
        verified_subset
        and isinstance(identity_hash, str)
        and len(identity_hash) == 64
        and all(character in "0123456789abcdef" for character in identity_hash)
    )


def _after_hours_candidate_identity_manifest(
    after_hours: Mapping[str, object],
) -> tuple[str, ...] | None:
    return after_hours_candidate_identity_manifest(after_hours)


@dataclass(slots=True)
class _DailyCallbackJob:
    scan_run_id: str
    owner: str
    operation: str
    started_at: datetime
    deadline_at: datetime
    event: threading.Event
    cancel_event: threading.Event
    operation_token: str
    operation_context: ScheduledOperationContext | None = None
    retry_attempt: int | None = None
    payload: object | None = None
    error: BaseException | None = None
    completed_at: datetime | None = None
    terminalization_error: str | None = None


def _invoke_cooperative_callback(
    callback: Callable[..., object],
    args: tuple[object, ...],
    job: _DailyCallbackJob,
) -> object:
    """Pass cancellation authority only through an explicitly supported seam."""

    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args)
    parameters = signature.parameters
    accepts_kwargs = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    kwargs: dict[str, object] = {}
    if accepts_kwargs or "cancel_event" in parameters:
        kwargs["cancel_event"] = job.cancel_event
    if accepts_kwargs or "deadline_at" in parameters:
        kwargs["deadline_at"] = job.deadline_at
    if accepts_kwargs or "operation_token" in parameters:
        kwargs["operation_token"] = job.operation_token
    if job.operation_context is not None and (
        accepts_kwargs or "operation_context" in parameters
    ):
        kwargs["operation_context"] = job.operation_context
    return callback(*args, **kwargs)


class DecisionPipelinePort(Protocol):
    """P5 implementation seam.  Implementations may only return read models."""
    def run_slot(self, scan_run_id: str, slot_at: datetime) -> Mapping[str, object]:
        raise NotImplementedError


class CalendarSnapshotProvider(Protocol):
    """Read-only broker-calendar seam used by the background lifecycle."""

    def snapshot(self, *, now: datetime) -> UsOptionsCalendarSnapshot:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class TickResult:
    scan_run_id: str | None
    duplicate_reason: str | None
    result_hash: str | None
    status: str
    producer_status: str | None = None
    producer_reason_codes: tuple[str, ...] = ()
    producer_missing_symbols: tuple[str, ...] = ()
    producer_written_count: int | None = None
    producer_slot: str | None = None
    producer_run_id: str | None = None
    producer_evidence_hash: str | None = None


class Top10ProducerPort(Protocol):
    """Two-slot, supporting-only producer seam with no approval authority."""

    def tick(self, *, scheduled_for: datetime) -> object:
        raise NotImplementedError


class Top10SchedulerService:
    """Dispatch an independent Top-10 producer under its own durable lease."""

    def __init__(
        self,
        store: ScanRunStore | None,
        producer: Top10ProducerPort | None,
        *,
        pipeline_version: str,
        owner: str | None = None,
        unavailable_reason: str = TOP10_PRODUCER_UNAVAILABLE,
    ) -> None:
        self.store = store
        self.producer = producer
        self.pipeline_version = str(pipeline_version).strip()
        if not self.pipeline_version:
            raise ValueError("pipeline_version cannot be blank")
        self.owner = owner or f"top10-producer.{uuid.uuid4().hex}"
        self.unavailable_reason = str(unavailable_reason).strip()
        if not self.unavailable_reason:
            raise ValueError("unavailable_reason cannot be blank")

    @property
    def available(self) -> bool:
        return self.store is not None and callable(getattr(self.producer, "tick", None))

    def tick(
        self,
        calendar: UsOptionsCalendarSnapshot,
        *,
        now: datetime | None = None,
        producer: Top10ProducerPort | None = None,
    ) -> TickResult:
        instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        selected_producer = self.producer if producer is None else producer
        if self.store is None or not callable(getattr(selected_producer, "tick", None)):
            return TickResult(None, self.unavailable_reason, None, "NO_TRADE")
        calendar_age = (instant - calendar.observed_at).total_seconds()
        if (
            calendar_age < 0
            or calendar_age > float(DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS)
        ):
            return TickResult(None, "RECOVERY_EVIDENCE_STALE", None, "NO_TRADE")
        slot = exact_top10_slot(calendar, now=instant)
        if slot is None:
            return self.latest_result(
                trading_date=instant.astimezone(US_OPTIONS_TIMEZONE).date(),
                duplicate_reason="NO_TOP10_SLOT_DUE",
            )
        assert self.store is not None
        acquired = self.store.acquire(
            slot,
            pipeline_version=self.pipeline_version,
            owner=self.owner,
            now=instant,
        )
        if not acquired.acquired:
            durable = self.store.producer_result(acquired.run.scan_run_id)
            if durable is not None:
                return _tick_from_durable(
                    durable,
                    duplicate_reason=acquired.duplicate_reason,
                    status=acquired.run.status,
                )
            return TickResult(
                acquired.run.scan_run_id,
                acquired.duplicate_reason,
                acquired.run.result_hash,
                acquired.run.status,
                "NO_TRADE",
                (DURABLE_PRODUCER_RESULT_STATUS_UNAVAILABLE,),
            )
        assert selected_producer is not None
        return self._run(acquired, instant, selected_producer)

    def _run(
        self,
        acquired: ScanAcquireResult,
        instant: datetime,
        producer: Top10ProducerPort,
    ) -> TickResult:
        assert self.store is not None
        try:
            raw_result = producer.tick(scheduled_for=acquired.run.slot_at)
            payload = _producer_payload(raw_result)
            producer_status = str(payload.get("status", "")).strip().upper()
            if not producer_status:
                raise TypeError("producer status is required")
            if any(
                payload.get(name) is True
                for name in (
                    "approval_eligible",
                    "instruction_creation_allowed",
                    "order_allowed",
                )
            ):
                raise ValueError("Top-10 producer cannot grant authority")
            decision_authority = payload.get("decision_authority")
            if decision_authority not in (None, "SUPPORTING_ONLY"):
                raise ValueError("Top-10 producer must remain supporting-only")
            reasons = _reason_codes(payload.get("reason_codes", ()))
            missing_symbols = _missing_symbols(payload.get("missing_symbols", ()))
            reasons, missing_symbols = self._bind_premarket_failure_context(
                acquired,
                reasons=reasons,
                missing_symbols=missing_symbols,
            )
            written_count = _written_count(payload.get("written_count", 0))
            producer_slot = _optional_result_text(payload.get("slot"))
            producer_run_id = _optional_result_text(payload.get("run_id"))
            digest = sha256(
                json.dumps(
                    payload,
                    sort_keys=True,
                    default=str,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            safe_projection = {
                "schema": "options_copilot.top10_producer_result.v1",
                "scan_run_id": acquired.run.scan_run_id,
                "trading_date": acquired.run.trading_date.isoformat(),
                "slot_at": acquired.run.slot_at.isoformat(),
                "pipeline_version": acquired.run.pipeline_version,
                "producer_status": producer_status,
                "reason_codes": list(reasons),
                "missing_symbols": list(missing_symbols),
                "written_count": written_count,
                "producer_slot": producer_slot,
                "producer_run_id": producer_run_id,
                "result_hash": digest,
            }
            evidence_hash = sha256(
                json.dumps(
                    safe_projection,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            finished = self.store.complete_with_producer_result(
                acquired.run.scan_run_id,
                owner=self.owner,
                result_hash=digest,
                producer_status=producer_status,
                reason_codes=reasons,
                missing_symbols=missing_symbols,
                written_count=written_count,
                producer_slot=producer_slot,
                producer_run_id=producer_run_id,
                evidence_hash=evidence_hash,
                now=instant,
            )
            return TickResult(
                finished.scan_run_id,
                None,
                digest,
                finished.status,
                producer_status,
                reasons,
                missing_symbols,
                written_count,
                producer_slot,
                producer_run_id,
                evidence_hash,
            )
        except Exception:
            finished = self.store.fail(
                acquired.run.scan_run_id,
                owner=self.owner,
                reason=TOP10_PRODUCER_FAILED,
                now=instant,
            )
            return TickResult(
                finished.scan_run_id,
                TOP10_PRODUCER_FAILED,
                None,
                finished.status,
            )

    def _bind_premarket_failure_context(
        self,
        acquired: ScanAcquireResult,
        *,
        reasons: tuple[str, ...],
        missing_symbols: tuple[str, ...],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Bind an open parent miss to the same-day durable freeze result."""

        if "TODAY_0920_PARENT_MISSING" not in reasons or self.store is None:
            return reasons, missing_symbols
        scheduled_et = acquired.run.slot_at.astimezone(US_OPTIONS_TIMEZONE)
        if scheduled_et.timetz().replace(tzinfo=None) != time(9, 35):
            return reasons, missing_symbols
        premarket_slot = ScanSlot(
            acquired.run.trading_date,
            scheduled_et.replace(hour=9, minute=20, second=0, microsecond=0),
            kind="TOP10_FREEZE",
        )
        try:
            runs = self.store.runs_for_slot(
                premarket_slot,
                pipeline_version=self.pipeline_version,
            )
            if not runs:
                return (
                    tuple(
                        dict.fromkeys(
                            (*reasons, "PREMARKET_PARENT_RUN_UNAVAILABLE")
                        )
                    ),
                    missing_symbols,
                )
            durable = self.store.producer_result(runs[0].scan_run_id)
        except (RuntimeError, TypeError, ValueError):
            durable = None
        if durable is None:
            return (
                tuple(
                    dict.fromkeys(
                        (*reasons, "PREMARKET_PARENT_RESULT_UNAVAILABLE")
                    )
                ),
                missing_symbols,
            )
        context = (
            "PREMARKET_PARENT_LEDGER_BINDING_MISMATCH"
            if durable.producer_status == "PREMARKET_FROZEN"
            and durable.written_count > 0
            else "PREMARKET_PARENT_NOT_PRODUCED"
        )
        return (
            tuple(dict.fromkeys((*reasons, context, *durable.reason_codes))),
            tuple(
                dict.fromkeys((*missing_symbols, *durable.missing_symbols))
            ),
        )

    def latest_result(
        self,
        *,
        trading_date,
        duplicate_reason: str,
    ) -> TickResult:
        if self.store is None:
            return TickResult(None, duplicate_reason, None, "NO_TRADE")
        durable = self.store.latest_producer_result(
            trading_date=trading_date,
            pipeline_version=self.pipeline_version,
        )
        if durable is None:
            return TickResult(None, duplicate_reason, None, "NO_TRADE")
        return _tick_from_durable(
            durable,
            duplicate_reason=duplicate_reason,
            status="COMPLETED",
        )


class Top10OnlySchedulerLoop:
    """Connector-free heartbeat for the external, supporting-only Top-10 path."""

    HEARTBEAT_SECONDS = 1

    def __init__(
        self,
        service: Top10SchedulerService,
        calendar_provider: CalendarSnapshotProvider,
        *,
        bundle_guard: ExternalBundleCommitGuard,
        bundle_runtime_factory: Callable[
            [ExternalBundleSnapshot, datetime],
            tuple[CalendarSnapshotProvider, Top10ProducerPort],
        ],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(service, Top10SchedulerService):
            raise TypeError("service must be a Top10SchedulerService")
        if not callable(getattr(calendar_provider, "snapshot", None)):
            raise TypeError("calendar_provider must expose snapshot(now=...)")
        if not isinstance(bundle_guard, ExternalBundleCommitGuard):
            raise TypeError("bundle_guard must be an ExternalBundleCommitGuard")
        if not callable(bundle_runtime_factory):
            raise TypeError("bundle_runtime_factory must be callable")
        self.service = service
        self.calendar_provider = calendar_provider
        self.bundle_guard = bundle_guard
        self.bundle_runtime_factory = bundle_runtime_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._tick_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._last_tick_at: datetime | None = None
        self._last_result: TickResult | None = None
        self._status = "STOPPED"

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._status = "STARTING"
            self._thread = threading.Thread(
                target=self._run,
                name="options-copilot-external-top10",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> bool:
        self._stop.set()
        with self._lock:
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
        with self._lock:
            if thread is not None and thread.is_alive():
                self._status = "CLOSE_TIMEOUT"
                return False
        with self._lock:
            self._status = "STOPPED"
            self._thread = None
            return True

    stop = close

    def tick_once(self) -> TickResult:
        with self._tick_lock:
            instant = self._clock()
            if (
                not isinstance(instant, datetime)
                or instant.tzinfo is None
                or instant.utcoffset() is None
            ):
                return self._record_failure(
                    datetime.now(timezone.utc),
                    "SCANNER_CLOCK_INVALID",
                )
            checked_at = instant.astimezone(timezone.utc)
            try:
                scheduled_for = external_slot_for_instant(checked_at)
            except ExternalBundleError:
                return self._record_failure(
                    checked_at,
                    "SCANNER_CLOCK_INVALID",
                )
            if scheduled_for is None:
                return self._record_idle(checked_at)
            try:
                with self.bundle_guard.consume(
                    scheduled_for=scheduled_for,
                    now=checked_at,
                    lock_timeout_seconds=0,
                ) as snapshot:
                    final_instant = self._clock()
                    if (
                        not isinstance(final_instant, datetime)
                        or final_instant.tzinfo is None
                        or final_instant.utcoffset() is None
                    ):
                        return self._record_failure(
                            checked_at,
                            "SCANNER_CLOCK_INVALID",
                        )
                    final_checked_at = final_instant.astimezone(timezone.utc)
                    if (
                        final_checked_at < checked_at
                        or final_checked_at > snapshot.manifest.feed_expires_at
                    ):
                        return self._record_failure(
                            final_checked_at,
                            "EXTERNAL_BUNDLE_NOT_READY",
                        )
                    checked_at = final_checked_at
                    try:
                        provider, producer = self.bundle_runtime_factory(
                            snapshot,
                            checked_at,
                        )
                        if not callable(getattr(provider, "snapshot", None)):
                            raise TypeError("bundle provider is unavailable")
                        if not callable(getattr(producer, "tick", None)):
                            raise TypeError("bundle producer is unavailable")
                        calendar = provider.snapshot(now=checked_at)
                    except Exception:
                        return self._record_failure(
                            checked_at,
                            "EXTERNAL_BUNDLE_NOT_READY",
                        )
                    if not isinstance(calendar, UsOptionsCalendarSnapshot):
                        return self._record_failure(
                            checked_at,
                            "CALENDAR_PROVIDER_INVALID",
                        )
            except ExternalBundleError:
                return self._record_failure(
                    checked_at,
                    "EXTERNAL_BUNDLE_NOT_READY",
                )
            except Exception:
                return self._record_failure(
                    checked_at,
                    "EXTERNAL_BUNDLE_NOT_READY",
                )
            try:
                result = self.service.tick(
                    calendar,
                    now=checked_at,
                    producer=producer,
                )
            except Exception:
                return self._record_failure(
                    checked_at,
                    "CALENDAR_PROVIDER_UNAVAILABLE",
                )
            with self._lock:
                self._last_tick_at = checked_at
                self._last_result = result
                self._status = (
                    "DEGRADED"
                    if result.status == "FAILED"
                    or result.producer_status
                    in {"NO_TRADE", "POSITION_MANAGEMENT_ONLY"}
                    else "READY"
                )
            return result

    def _record_idle(self, instant: datetime) -> TickResult:
        result = self.service.latest_result(
            trading_date=instant.astimezone(US_OPTIONS_TIMEZONE).date(),
            duplicate_reason="NO_TOP10_SLOT_DUE",
        )
        with self._lock:
            self._last_tick_at = instant.astimezone(timezone.utc)
            self._last_result = result
            self._status = "READY"
        return result

    def health(self) -> dict[str, object]:
        with self._lock:
            result = self._last_result
            return {
                "status": self._status,
                "heartbeat_seconds": self.HEARTBEAT_SECONDS,
                "last_tick_at": (
                    None
                    if self._last_tick_at is None
                    else self._last_tick_at.isoformat()
                ),
                "last_tick_status": None if result is None else result.status,
                "last_scan_run_id": (
                    None if result is None else result.scan_run_id
                ),
                "last_producer_status": (
                    None if result is None else result.producer_status
                ),
                "last_reason": (
                    None if result is None else result.duplicate_reason
                ),
                "last_reason_codes": (
                    () if result is None else result.producer_reason_codes
                ),
                "last_missing_symbols": (
                    () if result is None else result.producer_missing_symbols
                ),
                "last_written_count": (
                    None if result is None else result.producer_written_count
                ),
                "last_producer_slot": (
                    None if result is None else result.producer_slot
                ),
                "last_producer_run_id": (
                    None if result is None else result.producer_run_id
                ),
                "last_producer_evidence_hash": (
                    None if result is None else result.producer_evidence_hash
                ),
                "acquisition_mode": "EXTERNAL",
                "connected": False,
                "review_only": True,
                "approval_allowed": False,
                "direct_order_submission": False,
            }

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick_once()
            except Exception:
                self._record_failure(
                    datetime.now(timezone.utc),
                    "SCANNER_HEARTBEAT_FAILED",
                )
            if self._stop.wait(self.HEARTBEAT_SECONDS):
                break

    def _record_failure(self, instant: datetime, reason: str) -> TickResult:
        result = TickResult(None, reason, None, "NO_TRADE")
        with self._lock:
            self._last_tick_at = instant.astimezone(timezone.utc)
            self._last_result = result
            self._status = "DEGRADED"
        return result


class ScanSchedulerService:
    HEARTBEAT_SECONDS = 30
    def __init__(
        self,
        store: ScanRunStore,
        pipeline: DecisionPipelinePort,
        *,
        pipeline_version: str,
        owner: str | None = None,
        lease_seconds: int = ORDINARY_SCAN_LEASE_SECONDS,
        lease_heartbeat_seconds: float = ORDINARY_SCAN_LEASE_HEARTBEAT_SECONDS,
        lease_clock: Callable[[], datetime] | None = None,
    ) -> None:
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int):
            raise TypeError("lease_seconds must be an integer")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if isinstance(lease_heartbeat_seconds, bool) or not isinstance(
            lease_heartbeat_seconds,
            (int, float),
        ):
            raise TypeError("lease_heartbeat_seconds must be numeric")
        if not 0 < float(lease_heartbeat_seconds) < lease_seconds:
            raise ValueError("lease heartbeat must be positive and shorter than lease")
        if lease_clock is not None and not callable(lease_clock):
            raise TypeError("lease_clock must be callable or None")
        self.store, self.pipeline = store, pipeline
        self.pipeline_version = str(pipeline_version).strip()
        if not self.pipeline_version: raise ValueError("pipeline_version cannot be blank")
        self.owner = owner or f"scanner.{uuid.uuid4().hex}"
        self.lease_seconds = lease_seconds
        self.lease_heartbeat_seconds = float(lease_heartbeat_seconds)
        self._lease_clock = lease_clock

    def tick(self, calendar: UsOptionsCalendarSnapshot, *, now: datetime | None = None) -> TickResult:
        instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        calendar_age = (instant - calendar.observed_at).total_seconds()
        if (
            calendar_age < 0
            or calendar_age > float(DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS)
        ):
            return TickResult(
                None,
                "RECOVERY_EVIDENCE_STALE",
                None,
                "NO_TRADE",
            )
        trading_date = instant.astimezone(US_OPTIONS_TIMEZONE).date()
        slots = slots_for_session(calendar, trading_date)
        if not slots:
            # Closed days remain explicitly read-only; no pipeline and no replay.
            reason = (
                "CALENDAR_NOT_READY_PENDING_REEVALUATION"
                if calendar.status.value != "READY"
                else "MARKET_CLOSED_PENDING_NEXT_OPEN"
            )
            return TickResult(None, reason, None, "NO_TRADE")
        acquired = self.store.recover(
            slots,
            pipeline_version=self.pipeline_version,
            owner=self.owner,
            now=instant,
            lease_seconds=self.lease_seconds,
        )
        if acquired is None:
            return TickResult(None, "NO_SLOT_DUE", None, "NO_TRADE")
        return self._run(acquired, instant)

    def run_now(
        self,
        calendar: UsOptionsCalendarSnapshot,
        *,
        now: datetime | None = None,
    ) -> TickResult:
        """Run one operator-requested, read-only scan during the live session.

        Manual scans use their actual point-in-time timestamp and the same
        durable lease, pipeline, pacing authority, and broker-read boundaries
        as scheduled scans.  They never replay or consume a future fixed slot.
        """

        instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        calendar_age = (instant - calendar.observed_at).total_seconds()
        if (
            calendar_age < 0
            or calendar_age > float(DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS)
        ):
            return TickResult(
                None,
                "RECOVERY_EVIDENCE_STALE",
                None,
                "NO_TRADE",
            )
        trading_date = instant.astimezone(US_OPTIONS_TIMEZONE).date()
        # A weekend is closed independently of provider or broker state.  Keep
        # weekday holidays and shortened sessions authority-bound to the
        # supplied calendar instead of guessing from a local weekday check.
        if trading_date.weekday() >= 5:
            return TickResult(
                None,
                "MARKET_SESSION_NOT_OPEN",
                None,
                "NO_TRADE",
            )
        if calendar.status is not CalendarStatus.READY:
            reason = (
                "CALENDAR_PACING_DENIED"
                if any("PACING" in item for item in calendar.reason_codes)
                else "CALENDAR_NOT_READY_PENDING_REEVALUATION"
            )
            return TickResult(None, reason, None, "NO_TRADE")
        session = calendar.session_for(trading_date)
        if session is None or not session.contains(instant):
            return TickResult(
                None,
                "MARKET_SESSION_NOT_OPEN",
                None,
                "NO_TRADE",
            )
        slot = ScanSlot(
            trading_date,
            instant.astimezone(US_OPTIONS_TIMEZONE),
            kind="MANUAL_READ_ONLY_SCAN",
        )
        acquired = self.store.acquire(
            slot,
            pipeline_version=self.pipeline_version,
            owner=self.owner,
            now=instant,
            lease_seconds=self.lease_seconds,
        )
        return self._run(acquired, instant)

    def _run(self, acquired: ScanAcquireResult, instant: datetime) -> TickResult:
        if not acquired.acquired:
            return TickResult(acquired.run.scan_run_id, acquired.duplicate_reason, acquired.run.result_hash, acquired.run.status)
        lease_stop = threading.Event()
        lease_lost = threading.Event()
        started_monotonic = monotonic()

        def keep_lease_alive() -> None:
            while not lease_stop.wait(self.lease_heartbeat_seconds):
                try:
                    heartbeat_at = self._lease_instant(
                        base=instant,
                        started_monotonic=started_monotonic,
                    )
                    renewed = self.store.heartbeat(
                        acquired.run.scan_run_id,
                        owner=self.owner,
                        now=heartbeat_at,
                        lease_seconds=self.lease_seconds,
                    )
                except (RuntimeError, TypeError, ValueError):
                    renewed = False
                if not renewed:
                    lease_lost.set()
                    return

        lease_thread = threading.Thread(
            target=keep_lease_alive,
            name=f"options-copilot-scan-lease-{acquired.run.scan_run_id}",
            daemon=True,
        )
        lease_thread.start()
        try:
            result = self.pipeline.run_slot(acquired.run.scan_run_id, acquired.run.slot_at)
            raw_timing = getattr(self.pipeline, "last_operational_timing", None)
            operational_timing = (
                dict(raw_timing)
                if isinstance(raw_timing, Mapping)
                and raw_timing.get("scan_run_id") == acquired.run.scan_run_id
                else None
            )
            finished_at = self._lease_instant(
                base=instant,
                started_monotonic=started_monotonic,
            )
            if lease_lost.is_set():
                return self._lease_failure_result(
                    acquired.run.scan_run_id,
                    instant=finished_at,
                    reason="LEASE_HEARTBEAT_LOST",
                )
            digest = sha256(json.dumps(dict(result), sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()
            finished = self.store.complete(
                acquired.run.scan_run_id,
                owner=self.owner,
                result_hash=digest,
                now=finished_at,
                operational_timing=operational_timing,
            )
            return TickResult(finished.scan_run_id, None, digest, finished.status)
        except Exception:
            try:
                failed_at = self._lease_instant(
                    base=instant,
                    started_monotonic=started_monotonic,
                )
            except (TypeError, ValueError):
                failed_at = instant
            return self._lease_failure_result(
                acquired.run.scan_run_id,
                instant=failed_at,
                reason="PIPELINE_FAILED",
            )
        finally:
            lease_stop.set()
            lease_thread.join(timeout=1)

    def _lease_instant(
        self,
        *,
        base: datetime,
        started_monotonic: float,
    ) -> datetime:
        if self._lease_clock is None:
            elapsed = max(0.0, monotonic() - started_monotonic)
            return base + timedelta(seconds=elapsed)
        value = self._lease_clock()
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError("lease clock must be timezone-aware")
        return value.astimezone(timezone.utc)

    def _lease_failure_result(
        self,
        scan_run_id: str,
        *,
        instant: datetime,
        reason: str,
    ) -> TickResult:
        try:
            finished = self.store.fail(
                scan_run_id,
                owner=self.owner,
                reason=reason,
                now=instant,
            )
        except (RuntimeError, ValueError):
            try:
                finished = self.store.get(scan_run_id)
            except (KeyError, RuntimeError, ValueError):
                return TickResult(scan_run_id, reason, None, "FAILED")
            durable_reason = finished.failure_reason or reason
            return TickResult(
                finished.scan_run_id,
                durable_reason,
                finished.result_hash,
                finished.status,
            )
        return TickResult(finished.scan_run_id, reason, None, finished.status)


class ScanSchedulerLoop:
    """Thirty-second, stoppable lifecycle around the one-shot scheduler.

    The loop owns no broker, approval, bridge, creator, or order operation.  A
    calendar failure is observable ``NO_TRADE`` and never calls the pipeline.
    """

    HEARTBEAT_SECONDS = 30

    def __init__(
        self,
        service: ScanSchedulerService,
        calendar_provider: CalendarSnapshotProvider,
        *,
        clock: Callable[[], datetime] | None = None,
        top10_service: Top10SchedulerService | None = None,
    ) -> None:
        if not callable(getattr(calendar_provider, "snapshot", None)):
            raise TypeError("calendar_provider must expose snapshot(now=...)")
        self.service = service
        self.calendar_provider = calendar_provider
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.top10_service = top10_service
        self._stop = threading.Event()
        self._closing = threading.Event()
        self._lock = threading.RLock()
        self._tick_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._last_tick_at: datetime | None = None
        self._last_result: TickResult | None = None
        self._last_top10_result: TickResult | None = None
        self._last_calendar: UsOptionsCalendarSnapshot | None = None
        self._calendar_last_attempt_at: datetime | None = None
        self._calendar_next_refresh_at: datetime | None = None
        self._calendar_backoff_index = 0
        self._position_research_callback: Callable[[], object] | None = None
        self._after_hours_reprice_callback: Callable[[], object] | None = None
        self._verified_after_hours_provider: Callable[[], object] | None = None
        self._after_hours_next_retry_at: datetime | None = None
        self._after_hours_retry_attempts = 0
        self._daily_callbacks: dict[str, Callable[..., object] | None] = {
            "RESEARCH_REFRESH": None,
            "OUTCOME_PROCESSING": None,
            "AFTER_HOURS_DISCOVERY": None,
            "AFTER_HOURS_REPRICE": None,
            "NEXT_SESSION_PREPARATION": None,
        }
        self._daily_jobs: dict[str, _DailyCallbackJob] = {}
        # Terminalizing a durable job is not proof its callback has exited.
        # Keep lifetime ownership independent of the active-job dictionary.
        self._callback_workers: dict[threading.Thread, _DailyCallbackJob] = {}
        self._position_research_job: _DailyCallbackJob | None = None
        self._last_daily_results: dict[str, Mapping[str, object]] = {}
        self._last_daily_manifest_state_hash: str | None = None
        self._last_daily_operations_summary: dict[str, object] | None = None
        self._status = "STOPPED"

    def bind_daily_callbacks(
        self,
        *,
        research_refresh: Callable[
            [UsOptionsCalendarSnapshot, datetime, datetime], object
        ]
        | None = None,
        outcome_process: Callable[[], object] | None = None,
        position_research: Callable[[], object] | None = None,
        after_hours_discovery: Callable[[], object] | None = None,
        after_hours_reprice: Callable[[], object] | None = None,
        verified_after_hours: Callable[[], object] | None = None,
        next_session_preparation: Callable[
            [UsOptionsCalendarSnapshot, datetime, datetime], object
        ]
        | None = None,
    ) -> None:
        for name, callback in (
            ("research_refresh", research_refresh),
            ("outcome_process", outcome_process),
            ("position_research", position_research),
            ("after_hours_discovery", after_hours_discovery),
            ("after_hours_reprice", after_hours_reprice),
            ("verified_after_hours", verified_after_hours),
            ("next_session_preparation", next_session_preparation),
        ):
            if callback is not None and not callable(callback):
                raise TypeError(f"{name} must be callable or None")
        with self._lock:
            self._daily_callbacks["RESEARCH_REFRESH"] = research_refresh
            self._daily_callbacks["OUTCOME_PROCESSING"] = outcome_process
            self._position_research_callback = position_research
            self._daily_callbacks["AFTER_HOURS_DISCOVERY"] = after_hours_discovery
            self._daily_callbacks["AFTER_HOURS_REPRICE"] = after_hours_reprice
            self._verified_after_hours_provider = verified_after_hours
            self._daily_callbacks["NEXT_SESSION_PREPARATION"] = (
                next_session_preparation
            )
            self._after_hours_reprice_callback = after_hours_reprice
            if after_hours_reprice is not None:
                self._restore_after_hours_retry_state()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._closing.is_set() and self._status != "STOPPED":
                return
            self._closing.clear()
            self._stop.clear()
            self._status = "STARTING"
            self._thread = threading.Thread(
                target=self._run,
                name="options-copilot-scanner",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> bool:
        drain_deadline = monotonic() + SCANNER_CLOSE_TIMEOUT_SECONDS
        with self._lock:
            if (
                self._closing.is_set()
                and self._status == "STOPPED"
                and self._thread is None
                and not self._callback_workers
            ):
                return True
            self._closing.set()
            self._stop.set()
            self._status = "CLOSING"
            thread = self._thread
            for job in self._callback_workers.values():
                job.cancel_event.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, drain_deadline - monotonic()))
        with self._lock:
            if thread is not None and thread.is_alive():
                self._status = "CLOSE_TIMEOUT"
                return False
        # A manual tick has no lifecycle thread handle. Fence its admission,
        # then acquire its serialization lock before releasing dependencies.
        if not self._tick_lock.acquire(
            timeout=max(0.0, drain_deadline - monotonic()),
        ):
            with self._lock:
                self._status = "CLOSE_TICK_TIMEOUT"
            return False
        try:
            try:
                closed_at = self._clock()
            except Exception:
                closed_at = None
            if (
                not isinstance(closed_at, datetime)
                or closed_at.tzinfo is None
                or closed_at.utcoffset() is None
            ):
                closed_at = datetime.now(timezone.utc)
            else:
                closed_at = closed_at.astimezone(timezone.utc)
            # A stopped heartbeat cannot reap an expired lease for us. Use the
            # same no-replay expiry transition before reconciling retired jobs.
            self.service.store.expire_leases(now=closed_at)
            self._reap_daily_jobs(closed_at)
            terminalized = self._abandon_daily_jobs(closed_at)
            with self._lock:
                position_job = self._position_research_job
                if position_job is not None:
                    position_job.cancel_event.set()
                    self._last_daily_results["POSITION_RESEARCH"] = {
                        "status": "FAILED",
                        "observed_at": closed_at.isoformat(),
                        "reason_codes": ("POSITION_RESEARCH_ABANDONED_ON_CLOSE",),
                        "decision_authority": "SUPPORTING_ONLY",
                        "action_pool_count": 0,
                    }
                    self._position_research_job = None
            drained = self._drain_callback_workers(drain_deadline)
            with self._lock:
                if not terminalized:
                    self._status = "CLOSE_DAILY_JOB_FAILURE"
                    return False
                if not drained:
                    self._status = "CLOSE_CALLBACK_TIMEOUT"
                    return False
                self._status = "STOPPED"
                self._thread = None
                return True
        except Exception:
            # Storage failure must not drop lifetime ownership or let the
            # runtime release dependencies. Cancellation/drain is still useful.
            self._drain_callback_workers(drain_deadline)
            with self._lock:
                self._status = "CLOSE_DAILY_JOB_FAILURE"
            return False
        finally:
            self._tick_lock.release()

    stop = close

    def tick_once(self) -> TickResult:
        # Manual probes and the background heartbeat share this serialization
        # boundary, so the independent and immutable Top-10 paths cannot
        # overlap quote batches.
        with self._tick_lock:
            if self._closing.is_set():
                return TickResult(None, "SCANNER_CLOSING", None, "FAILED")
            return self._tick_once_locked()

    def run_now(self) -> TickResult:
        """Serialize one manual ordinary scan against all background ticks."""

        with self._tick_lock:
            if self._closing.is_set():
                return TickResult(None, "SCANNER_CLOSING", None, "FAILED")
            instant = self._clock()
            if (
                not isinstance(instant, datetime)
                or instant.tzinfo is None
                or instant.utcoffset() is None
            ):
                return self._record_failure(
                    datetime.now(timezone.utc), "SCANNER_CLOCK_INVALID"
                )
            checked_at = instant.astimezone(timezone.utc)
            try:
                calendar = self.calendar_provider.snapshot(now=checked_at)
            except Exception:
                self._schedule_calendar_refresh_retry(checked_at)
                return self._record_failure(
                    checked_at, "CALENDAR_PROVIDER_UNAVAILABLE"
                )
            if not isinstance(calendar, UsOptionsCalendarSnapshot):
                self._schedule_calendar_refresh_retry(checked_at)
                return self._record_failure(
                    checked_at, "CALENDAR_PROVIDER_INVALID"
                )
            if calendar.normalized_at > checked_at:
                checked_at = calendar.normalized_at
            if self._closing.is_set():
                return TickResult(None, "SCANNER_CLOSING", None, "FAILED")
            if calendar.status is CalendarStatus.READY:
                self._reset_calendar_refresh_backoff(calendar, checked_at)
            else:
                self._schedule_calendar_refresh_retry(
                    checked_at,
                    calendar=calendar,
                )
            result = self.service.run_now(calendar, now=checked_at)
            with self._lock:
                self._last_tick_at = checked_at
                self._last_result = result
                self._last_calendar = calendar
                reason = str(result.duplicate_reason or "").strip().upper()
                self._status = (
                    "DEGRADED"
                    if result.status == "FAILED" or "PACING" in reason
                    else "READY"
                )
            return result

    def _tick_once_locked(self) -> TickResult:
        instant = self._clock()
        if (
            not isinstance(instant, datetime)
            or instant.tzinfo is None
            or instant.utcoffset() is None
        ):
            return self._record_failure(
                datetime.now(timezone.utc), "SCANNER_CLOCK_INVALID"
            )
        checked_at = instant.astimezone(timezone.utc)
        with self._lock:
            next_calendar_refresh = self._calendar_next_refresh_at
        if (
            next_calendar_refresh is not None
            and checked_at < next_calendar_refresh
        ):
            return self._record_failure(checked_at, "CALENDAR_REFRESH_BACKOFF")
        with self._lock:
            self._calendar_last_attempt_at = checked_at
        try:
            calendar = self.calendar_provider.snapshot(now=checked_at)
        except Exception:
            self._schedule_calendar_refresh_retry(checked_at)
            return self._record_failure(
                checked_at, "CALENDAR_PROVIDER_UNAVAILABLE"
            )
        if not isinstance(calendar, UsOptionsCalendarSnapshot):
            self._schedule_calendar_refresh_retry(checked_at)
            return self._record_failure(checked_at, "CALENDAR_PROVIDER_INVALID")
        if calendar.normalized_at > checked_at:
            checked_at = calendar.normalized_at
        if self._closing.is_set():
            return TickResult(None, "SCANNER_CLOSING", None, "FAILED")
        if calendar.status is not CalendarStatus.READY:
            self._schedule_calendar_refresh_retry(checked_at, calendar=calendar)
            # Preserve the first exact-slot fail-closed audit record.  Only
            # repeated heartbeats are suppressed by the retry window.
            self.service.store.expire_leases(now=checked_at)
            self._reap_daily_jobs(checked_at)
            self._reap_position_research_job(checked_at)
            self._run_daily_callback(calendar, checked_at)
            reason = next(
                (
                    str(item).strip().upper()
                    for item in calendar.reason_codes
                    if str(item).strip()
                ),
                "CALENDAR_NOT_READY_PENDING_REEVALUATION",
            )
            return self._record_failure(checked_at, reason)
        self._reset_calendar_refresh_backoff(calendar, checked_at)
        self.service.store.expire_leases(now=checked_at)
        self._reap_daily_jobs(checked_at)
        self._reap_position_research_job(checked_at)
        self._run_daily_callback(calendar, checked_at)
        self._run_after_hours_retry(checked_at)
        top10_result = (
            None
            if self.top10_service is None
            else self.top10_service.tick(calendar, now=checked_at)
        )
        self._run_position_research(top10_result, checked_at=checked_at)
        result = self.service.tick(calendar, now=checked_at)
        durable_daily_degraded = self._current_daily_run_degraded(
            calendar,
            checked_at=checked_at,
        )
        with self._lock:
            self._last_tick_at = checked_at
            self._last_result = result
            self._last_top10_result = top10_result
            self._last_calendar = calendar
            daily_terminalization_degraded = any(
                job.terminalization_error is not None
                for job in self._daily_jobs.values()
            )
            self._status = (
                "DEGRADED"
                if result.status == "FAILED"
                or daily_terminalization_degraded
                or durable_daily_degraded
                else "READY"
            )
        self._record_daily_manifest(checked_at)
        return result

    def _schedule_calendar_refresh_retry(
        self,
        checked_at: datetime,
        *,
        calendar: UsOptionsCalendarSnapshot | None = None,
    ) -> None:
        with self._lock:
            index = min(
                self._calendar_backoff_index,
                len(CALENDAR_REFRESH_BACKOFF_SECONDS) - 1,
            )
            delay = CALENDAR_REFRESH_BACKOFF_SECONDS[index]
            self._calendar_backoff_index = min(
                self._calendar_backoff_index + 1,
                len(CALENDAR_REFRESH_BACKOFF_SECONDS) - 1,
            )
            self._calendar_last_attempt_at = checked_at
            self._calendar_next_refresh_at = checked_at + timedelta(seconds=delay)
            if calendar is not None:
                self._last_calendar = calendar

    def _reset_calendar_refresh_backoff(
        self,
        calendar: UsOptionsCalendarSnapshot,
        checked_at: datetime,
    ) -> None:
        with self._lock:
            self._last_calendar = calendar
            self._calendar_last_attempt_at = checked_at
            self._calendar_next_refresh_at = None
            self._calendar_backoff_index = 0

    def _current_daily_run_degraded(
        self,
        calendar: UsOptionsCalendarSnapshot,
        *,
        checked_at: datetime,
    ) -> bool:
        trading_date = checked_at.astimezone(US_OPTIONS_TIMEZONE).date()
        for operation in daily_operation_slots_for_session(calendar, trading_date):
            pipeline_version = DAILY_OPERATION_PIPELINES.get(operation.operation)
            if pipeline_version is None:
                continue
            runs = self.service.store.runs_for_slot(
                ScanSlot(
                    operation.trading_date,
                    operation.slot_at,
                    kind=operation.operation,
                ),
                pipeline_version=pipeline_version,
            )
            if runs and runs[0].status == "FAILED":
                return True
        return False

    def summary(self) -> dict[str, object]:
        """Return cached scheduler state after one bounded durable hydration."""

        with self._lock:
            if (
                self._last_daily_operations_summary is None
                and self._last_calendar is not None
                and self._last_tick_at is not None
            ):
                self._daily_operations_health()
            result = self._last_result
            top10 = self._last_top10_result
            daily_operations = (
                None
                if self._last_daily_operations_summary is None
                else deepcopy(self._last_daily_operations_summary)
            )
            if daily_operations is not None:
                # Failure/backoff ticks do not rebuild durable daily history.
                # Their current in-memory calendar state must still supersede
                # the earlier summary without adding ledger reads to polling.
                daily_operations["calendar_refresh"] = self._calendar_refresh_health()
            return {
                "status": self._status,
                "heartbeat_seconds": self.HEARTBEAT_SECONDS,
                "closing": self._closing.is_set(),
                "callback_workers_alive": sum(
                    worker.is_alive() for worker in self._callback_workers
                ),
                "last_tick_at": (
                    None
                    if self._last_tick_at is None
                    else self._last_tick_at.isoformat()
                ),
                "last_tick_status": None if result is None else result.status,
                "last_scan_run_id": None if result is None else result.scan_run_id,
                "last_reason": None if result is None else result.duplicate_reason,
                "top10_producer": {
                    "status": (
                        "UNAVAILABLE"
                        if self.top10_service is None
                        or not self.top10_service.available
                        else "DEGRADED"
                        if top10 is not None
                        and (
                            top10.status == "FAILED"
                            or top10.producer_status
                            in {"NO_TRADE", "POSITION_MANAGEMENT_ONLY"}
                        )
                        else "READY"
                    ),
                    "last_tick_status": None if top10 is None else top10.status,
                    "last_producer_status": (
                        None if top10 is None else top10.producer_status
                    ),
                    "last_reason": (
                        TOP10_PRODUCER_UNAVAILABLE
                        if self.top10_service is None
                        else self.top10_service.unavailable_reason
                        if not self.top10_service.available
                        else None if top10 is None else top10.duplicate_reason
                    ),
                    "last_reason_codes": (
                        () if top10 is None else top10.producer_reason_codes
                    ),
                    "last_missing_symbols": (
                        () if top10 is None else top10.producer_missing_symbols
                    ),
                    "last_written_count": (
                        None if top10 is None else top10.producer_written_count
                    ),
                    "review_only": True,
                    "approval_allowed": False,
                    "direct_order_submission": False,
                },
                **(
                    {"daily_operations": daily_operations}
                    if daily_operations is not None
                    else {}
                ),
                "review_only": True,
                "direct_order_submission": False,
            }

    def health(self) -> dict[str, object]:
        with self._lock:
            result = self._last_result
            top10 = self._last_top10_result
            daily_operations = self._daily_operations_health()
            return {
                "status": self._status,
                "heartbeat_seconds": self.HEARTBEAT_SECONDS,
                "closing": self._closing.is_set(),
                "callback_workers_alive": sum(
                    worker.is_alive() for worker in self._callback_workers
                ),
                "last_tick_at": (
                    None
                    if self._last_tick_at is None
                    else self._last_tick_at.isoformat()
                ),
                "last_tick_status": None if result is None else result.status,
                "last_scan_run_id": None if result is None else result.scan_run_id,
                "last_reason": None if result is None else result.duplicate_reason,
                "top10_producer": {
                    "status": (
                        "UNAVAILABLE"
                        if self.top10_service is None
                        or not self.top10_service.available
                        else (
                            "DEGRADED"
                            if top10 is not None
                            and (
                                top10.status == "FAILED"
                                or top10.producer_status
                                in {"NO_TRADE", "POSITION_MANAGEMENT_ONLY"}
                            )
                            else "READY"
                        )
                    ),
                    "last_tick_status": None if top10 is None else top10.status,
                    "last_producer_status": (
                        None if top10 is None else top10.producer_status
                    ),
                    "last_reason": (
                        TOP10_PRODUCER_UNAVAILABLE
                        if self.top10_service is None
                        else (
                            self.top10_service.unavailable_reason
                            if not self.top10_service.available
                            else (None if top10 is None else top10.duplicate_reason)
                        )
                    ),
                    "last_reason_codes": (
                        () if top10 is None else top10.producer_reason_codes
                    ),
                    "last_missing_symbols": (
                        () if top10 is None else top10.producer_missing_symbols
                    ),
                    "last_written_count": (
                        None if top10 is None else top10.producer_written_count
                    ),
                    "last_producer_slot": (
                        None if top10 is None else top10.producer_slot
                    ),
                    "last_producer_run_id": (
                        None if top10 is None else top10.producer_run_id
                    ),
                    "last_producer_evidence_hash": (
                        None if top10 is None else top10.producer_evidence_hash
                    ),
                    "review_only": True,
                    "approval_allowed": False,
                    "direct_order_submission": False,
                },
                "daily_operations": daily_operations,
                "review_only": True,
                "direct_order_submission": False,
            }

    def _calendar_refresh_health(self) -> dict[str, object]:
        calendar = self._last_calendar
        calendar_last_attempt_at = self._calendar_last_attempt_at
        calendar_next_refresh_at = self._calendar_next_refresh_at
        reason_codes = () if calendar is None else calendar.reason_codes
        # Calendar refresh completes before the potentially long scan. Its
        # independent attempt time must not be compared to the prior scan's
        # tick timestamp, which is only replaced after that scan returns.
        if calendar is None or calendar_last_attempt_at is None:
            calendar_status = "NOT_RUN"
        else:
            age = (calendar_last_attempt_at - calendar.observed_at).total_seconds()
            calendar_status = (
                "COMPLETED"
                if calendar.status is CalendarStatus.READY
                and 0 <= age <= float(DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS)
                else "DEGRADED"
            )
            if calendar_status == "DEGRADED" and not reason_codes:
                reason_codes = (
                    "CALENDAR_OBSERVED_IN_FUTURE"
                    if age < 0
                    else "CALENDAR_STALE"
                    if age > float(DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS)
                    else "CALENDAR_NOT_READY_PENDING_REEVALUATION",
                )
        return {
            "schedule": (
                "FAILURE_BACKOFF"
                if calendar_next_refresh_at is not None
                else "EVERY_HEARTBEAT"
            ),
            "status": calendar_status,
            "reason_codes": reason_codes,
            "last_run_at": (
                None
                if calendar_last_attempt_at is None
                else calendar_last_attempt_at.isoformat()
            ),
            "next_retry_at": (
                None
                if calendar_next_refresh_at is None
                else calendar_next_refresh_at.isoformat()
            ),
        }

    def _daily_operations_health(self) -> dict[str, object]:
        calendar = self._last_calendar
        checked_at = self._last_tick_at
        result: dict[str, object] = {
            "timezone": "America/New_York",
            "calendar_refresh": self._calendar_refresh_health(),
            "outcome_processing": dict(
                self._latest_daily_payload(
                    "OUTCOME_PROCESSING",
                    {
                        "status": (
                            "NOT_RUN"
                            if self._daily_callbacks["OUTCOME_PROCESSING"] is not None
                            else "UNAVAILABLE"
                        ),
                        "checked_at": None,
                        "due_count": 0,
                        "records_appended": 0,
                        "records_superseded": 0,
                        "records_skipped": 0,
                        "records_blocked": 0,
                        "records_rejected": 0,
                        "reason_codes": (),
                        "candidate_ledger_head_hash": None,
                        "shadow_ledger_head_hash": None,
                        "manifest_hash": None,
                        "processing_hash": None,
                    },
                )
            ),
            "position_research": dict(
                self._last_daily_results.get(
                    "POSITION_RESEARCH",
                    {
                        "status": (
                            "NOT_RUN"
                            if self._position_research_callback is not None
                            else "UNAVAILABLE"
                        ),
                        "decision_authority": "SUPPORTING_ONLY",
                        "action_pool_count": 0,
                    },
                )
            ),
            "after_hours_reprice": dict(
                self._latest_daily_payload(
                    "AFTER_HOURS_REPRICE",
                    {
                        "status": (
                            "NOT_RUN"
                            if self._after_hours_reprice_callback is not None
                            else "UNAVAILABLE"
                        ),
                        "priced_count": 0,
                        "requested_count": 0,
                        "next_retry_at": (
                            None
                            if self._after_hours_next_retry_at is None
                            else self._after_hours_next_retry_at.isoformat()
                        ),
                        "decision": "NO_TRADE",
                        "decision_authority": "SUPPORTING_ONLY",
                    },
                )
            ),
            "next_session_preparation": dict(
                self._latest_next_session_payload(
                    {
                        "schema": "options_copilot.next_session_preparation.v1",
                        "status": (
                            "NOT_RUN"
                            if self._daily_callbacks["NEXT_SESSION_PREPARATION"]
                            is not None
                            else "UNAVAILABLE"
                        ),
                        "prepared_at": None,
                        "next_trading_date": None,
                        "equity_research_count": 0,
                        "equity_selected_count": 0,
                        "option_research_structure_count": 0,
                        "option_structure_count": 0,
                        "executable_count": 0,
                        "reason_codes": (),
                        "decision_authority": "SUPPORTING_ONLY",
                    },
                )
            ),
            "day_manifest": None,
            "today": None,
            "last_completed_day": None,
            "runs": (),
        }
        after_hours_health = result["after_hours_reprice"]
        assert isinstance(after_hours_health, dict)
        after_hours_health["next_retry_at"] = (
            None
            if self._after_hours_next_retry_at is None
            else self._after_hours_next_retry_at.isoformat()
        )
        after_hours_health["retry_attempt"] = self._after_hours_retry_attempts
        after_hours_health["retry_limit"] = AFTER_HOURS_REPRICE_RETRY_LIMIT
        after_hours_health["retry_pending"] = (
            self._after_hours_next_retry_at is not None
        )
        if calendar is None or checked_at is None:
            self._publish_daily_operations_summary(result)
            return result
        trading_date = checked_at.astimezone(US_OPTIONS_TIMEZONE).date()
        completed_manifest = self.service.store.latest_completed_daily_manifest()
        durable_today = (
            completed_manifest
            if completed_manifest is not None
            and completed_manifest.trading_date == trading_date
            else None
        )
        session = calendar.session_for(trading_date)
        market_status = (
            "TRADING_SESSION"
            if session is not None or durable_today is not None
            else "CLOSED"
            if trading_date.weekday() >= 5 or trading_date in calendar.closed_dates
            else "UNVERIFIED"
        )
        next_trading_date = next(
            (
                item.trading_date
                for item in calendar.sessions
                if item.trading_date > trading_date
            ),
            None,
        )
        if next_trading_date is None:
            prepared_date = result["next_session_preparation"].get(
                "next_trading_date"
            )
            if isinstance(prepared_date, str):
                try:
                    parsed_prepared_date = date.fromisoformat(prepared_date)
                except ValueError:
                    parsed_prepared_date = None
                if (
                    parsed_prepared_date is not None
                    and parsed_prepared_date > trading_date
                ):
                    next_trading_date = parsed_prepared_date
        runs: list[dict[str, object]] = []
        operations = daily_operation_slots_for_session(calendar, trading_date)
        if not operations:
            research = _research_operation_for_minute(checked_at)
            operations = () if research is None else (research,)
        if not operations and durable_today is not None:
            durable_runs = durable_today.payload.get("runs")
            if isinstance(durable_runs, Sequence) and not isinstance(
                durable_runs,
                (str, bytes, bytearray, memoryview),
            ):
                runs.extend(
                    dict(item) for item in durable_runs if isinstance(item, Mapping)
                )
        for operation in operations:
            scheduled_utc = operation.slot_at.astimezone(timezone.utc)
            status = "PENDING" if checked_at < scheduled_utc else "DUE"
            recovery_policy = "EXACT_ONLY_NO_REPLAY"
            scan_run_id: str | None = None
            handler_status: str | None = None
            terminalization_error: str | None = None
            reason_codes: tuple[str, ...] = ()
            recorded_at: str | None = None
            producer_status: str | None = None
            producer_written_count: int | None = None
            producer_missing_symbols: tuple[str, ...] = ()
            producer_evidence_hash: str | None = None
            if operation.operation == "ORDINARY_SCAN":
                handler_status = "READY"
                recovery_policy = "LATEST_ONLY_WITHIN_TWO_HOURS_FRESH_EVIDENCE"
                scan_slot = ScanSlot(operation.trading_date, operation.slot_at)
                stored = self.service.store.runs_for_slot(
                    scan_slot,
                    pipeline_version=self.service.pipeline_version,
                )
                if stored:
                    status = stored[0].status
                    scan_run_id = stored[0].scan_run_id
                    durable_result = self.service.store.daily_result(scan_run_id)
                    if durable_result is not None:
                        self._last_daily_results[operation.operation] = (
                            durable_result.payload
                        )
                elif checked_at >= scheduled_utc:
                    status = (
                        "MISSED_NOT_REPLAYED"
                        if checked_at - scheduled_utc > RECOVERY_MAX_AGE
                        else "RECOVERABLE"
                    )
            elif operation.operation in DAILY_OPERATION_PIPELINES:
                callback = self._daily_callbacks[operation.operation]
                handler_status = "READY" if callback is not None else "UNAVAILABLE"
                stored = self.service.store.runs_for_slot(
                    ScanSlot(
                        operation.trading_date,
                        operation.slot_at,
                        kind=operation.operation,
                    ),
                    pipeline_version=DAILY_OPERATION_PIPELINES[operation.operation],
                )
                if stored:
                    status = stored[0].status
                    scan_run_id = stored[0].scan_run_id
                    durable_result = self.service.store.daily_result(scan_run_id)
                    if durable_result is not None:
                        reason_codes = tuple(
                            str(reason).strip().upper()
                            for reason in durable_result.payload.get(
                                "reason_codes", ()
                            )
                            if str(reason).strip()
                        )
                        recorded_at = durable_result.recorded_at.isoformat()
                elif checked_at >= scheduled_utc + timedelta(minutes=1):
                    status = "MISSED_NOT_REPLAYED"
            elif operation.operation in {"TOP10_FREEZE", "TOP10_REPRICE"}:
                top10_service = self.top10_service
                handler_status = (
                    "READY"
                    if top10_service is not None and top10_service.available
                    else "UNAVAILABLE"
                )
                top10_store = (
                    None if top10_service is None else top10_service.store
                )
                if top10_store is not None:
                    stored = top10_store.runs_for_slot(
                        ScanSlot(
                            operation.trading_date,
                            operation.slot_at,
                            kind=operation.operation,
                        ),
                        pipeline_version=top10_service.pipeline_version,
                    )
                    if stored:
                        status = stored[0].status
                        scan_run_id = stored[0].scan_run_id
                        durable_producer = top10_store.producer_result(
                            scan_run_id
                        )
                        if durable_producer is not None:
                            producer_status = durable_producer.producer_status
                            producer_written_count = durable_producer.written_count
                            producer_missing_symbols = (
                                durable_producer.missing_symbols
                            )
                            producer_evidence_hash = (
                                durable_producer.evidence_hash
                            )
                            reason_codes = durable_producer.reason_codes
                            recorded_at = (
                                durable_producer.recorded_at.isoformat()
                            )
                    elif checked_at >= scheduled_utc + timedelta(minutes=1):
                        status = "MISSED_NOT_REPLAYED"
                elif checked_at >= scheduled_utc + timedelta(minutes=1):
                    status = "MISSED_NOT_REPLAYED"
            if scan_run_id is not None:
                with self._lock:
                    job = self._daily_jobs.get(scan_run_id)
                terminalization_error = (
                    None if job is None else job.terminalization_error
                )
            run_payload = {
                "operation": operation.operation,
                "scheduled_at": operation.slot_at.isoformat(),
                "status": status,
                "recovery_policy": recovery_policy,
                "scan_run_id": scan_run_id,
                "handler_status": handler_status,
            }
            if terminalization_error is not None:
                run_payload["terminalization_error"] = terminalization_error
            if reason_codes:
                run_payload["reason_codes"] = reason_codes
            if recorded_at is not None:
                run_payload["recorded_at"] = recorded_at
            if producer_status is not None:
                run_payload["producer_status"] = producer_status
                run_payload["producer_written_count"] = producer_written_count
                run_payload["producer_missing_symbols"] = (
                    producer_missing_symbols
                )
                run_payload["producer_evidence_hash"] = producer_evidence_hash
            runs.append(run_payload)
        result["runs"] = tuple(runs)
        result["today"] = {
            "trading_date": trading_date.isoformat(),
            "market_status": market_status,
            "next_trading_date": (
                None
                if next_trading_date is None
                else next_trading_date.isoformat()
            ),
            "runs": tuple(runs),
        }
        manifest = (
            durable_today
            if session is None
            else self.service.store.latest_daily_manifest(
                trading_date=trading_date
            )
        )
        if manifest is not None:
            result["day_manifest"] = {
                "manifest_id": manifest.manifest_id,
                "trading_date": manifest.trading_date.isoformat(),
                "manifest_hash": manifest.manifest_hash,
                "recorded_at": manifest.recorded_at.isoformat(),
            }
        if completed_manifest is not None:
            result["last_completed_day"] = {
                "trading_date": completed_manifest.trading_date.isoformat(),
                "manifest_id": completed_manifest.manifest_id,
                "manifest_hash": completed_manifest.manifest_hash,
                "recorded_at": completed_manifest.recorded_at.isoformat(),
                "runs": completed_manifest.payload.get("runs", ()),
            }
        self._publish_daily_operations_summary(result)
        return result

    def _publish_daily_operations_summary(
        self,
        value: Mapping[str, object],
    ) -> None:
        with self._lock:
            self._last_daily_operations_summary = deepcopy(dict(value))

    def _latest_daily_payload(
        self,
        operation: str,
        default: Mapping[str, object],
    ) -> Mapping[str, object]:
        cached = self._last_daily_results.get(operation)
        if cached is not None:
            return cached
        durable = self.service.store.latest_daily_result(operation)
        if operation == "AFTER_HOURS_REPRICE":
            retry = self.service.store.latest_daily_result(
                "AFTER_HOURS_REPRICE_RETRY"
            )
            if retry is not None and (
                durable is None or retry.recorded_at > durable.recorded_at
            ):
                durable = retry
        return default if durable is None else durable.payload

    def _latest_next_session_payload(
        self,
        default: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Reconcile the handoff with a later durable after-hours completion."""

        prepared = dict(
            self._latest_daily_payload("NEXT_SESSION_PREPARATION", default)
        )
        durable_after_hours = self._latest_daily_payload("AFTER_HOURS_REPRICE", {})
        candidates: list[tuple[Mapping[str, object], bool]] = []
        provider = self._verified_after_hours_provider
        provider_failure_reason: str | None = None
        if provider is not None:
            try:
                provided = provider()
            except Exception:
                provided = None
                provider_failure_reason = "AFTER_HOURS_VERIFIER_FAILED"
            if isinstance(provided, Mapping):
                provided_formal = provided.get("formal_research_pools")
                if (
                    isinstance(provided_formal, Mapping)
                    and provided_formal.get("status") == "READY"
                    and _valid_formal_descriptor(
                        provided_formal,
                        provided,
                        verified_subset=True,
                    )
                ):
                    candidates.append((provided, True))
                elif isinstance(provided_formal, Mapping):
                    provider_failure_reason = (
                        "AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID"
                    )
                else:
                    provider_failure_reason = (
                        "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE"
                    )
            elif provider_failure_reason is None:
                provider_failure_reason = "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE"
        # Once the production cross-store verifier is bound, a cache miss,
        # corruption, or lineage mismatch must not fall back to the weaker
        # scanner-local descriptor check.  The durable-only path remains for
        # legacy compositions that do not expose the verifier.
        if provider is None and isinstance(durable_after_hours, Mapping):
            candidates.append((durable_after_hours, False))

        after_hours: Mapping[str, object] | None = None
        formal: Mapping[str, object] | None = None
        verified_subset = False
        for candidate, candidate_verified_subset in candidates:
            candidate_formal = candidate.get("formal_research_pools")
            if (
                isinstance(candidate_formal, Mapping)
                and candidate_formal.get("status") == "READY"
                and _valid_formal_descriptor(
                    candidate_formal,
                    candidate,
                    verified_subset=candidate_verified_subset,
                )
            ):
                after_hours = candidate
                formal = candidate_formal
                verified_subset = candidate_verified_subset
                break
        if after_hours is None or formal is None:
            if provider is not None:
                prepared["equity_research_count"] = 0
                prepared["equity_selected_count"] = 0
                prepared["option_research_structure_count"] = 0
                prepared["option_structure_count"] = 0
                prepared["premarket_parent_eligible_structure_count"] = 0
                prepared["equity_pool_hash"] = None
                prepared["option_pool_hash"] = None
                prepared["after_hours_campaign_hash"] = None
                prepared["after_hours_campaign"] = None
                prepared["after_hours_formal_research_pools"] = None
            else:
                prepared.setdefault("equity_research_count", 0)
                prepared.setdefault("equity_selected_count", 0)
                prepared.setdefault(
                    "option_research_structure_count",
                    prepared.get("option_structure_count", 0),
                )
                prepared.setdefault("option_structure_count", 0)
                prepared.setdefault("premarket_parent_eligible_structure_count", 0)
            prepared["executable_count"] = 0
            prepared["decision"] = "NO_TRADE"
            prepared["decision_authority"] = "SUPPORTING_ONLY"
            prepared["approval_eligible"] = False
            prepared["instruction_creation_allowed"] = False
            prepared["order_allowed"] = False
            if provider is not None:
                prepared["status"] = "DEGRADED"
                prepared["reason_codes"] = tuple(
                    dict.fromkeys(
                        (
                            *tuple(prepared.get("reason_codes", ())),
                            provider_failure_reason
                            or "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE",
                        )
                    )
                )
                prepared["reconciled_from_verified_after_hours"] = False
                prepared["reconciled_from_durable_after_hours"] = False
            return prepared
        if not _valid_formal_descriptor(
            formal,
            after_hours,
            verified_subset=verified_subset,
        ):
            prepared["status"] = "DEGRADED"
            prepared["reason_codes"] = tuple(
                dict.fromkeys(
                    (
                        *tuple(prepared.get("reason_codes", ())),
                        "AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID",
                    )
                )
            )
            prepared["reconciled_from_durable_after_hours"] = False
            return prepared
        equity_research_count = formal.get("equity_research_count")
        equity_selected_count = formal.get("equity_selected_count")
        option_research_count = formal.get("option_structure_count")
        campaign = after_hours.get("campaign")
        if (
            any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                for value in (
                    equity_research_count,
                    equity_selected_count,
                    option_research_count,
                )
            )
        ):
            prepared["equity_research_count"] = 0
            prepared["equity_selected_count"] = 0
            prepared["option_research_structure_count"] = 0
            prepared["option_structure_count"] = 0
            prepared["premarket_parent_eligible_structure_count"] = 0
            prepared["executable_count"] = 0
            prepared["equity_pool_hash"] = None
            prepared["option_pool_hash"] = None
            prepared["after_hours_campaign_hash"] = None
            prepared["after_hours_campaign"] = None
            prepared["after_hours_formal_research_pools"] = None
            prepared["status"] = "DEGRADED"
            prepared["reason_codes"] = tuple(
                dict.fromkeys(
                    (
                        *tuple(prepared.get("reason_codes", ())),
                        "AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID",
                    )
                )
            )
            prepared["reconciled_from_verified_after_hours"] = False
            prepared["reconciled_from_durable_after_hours"] = False
            prepared["decision"] = "NO_TRADE"
            prepared["decision_authority"] = "SUPPORTING_ONLY"
            prepared["approval_eligible"] = False
            prepared["instruction_creation_allowed"] = False
            prepared["order_allowed"] = False
            return prepared
        prepared.update(
            {
                "source_preparation_checked_at": prepared.get(
                    "source_preparation_checked_at",
                    prepared.get("checked_at", prepared.get("prepared_at")),
                ),
                "checked_at": formal.get("materialized_at")
                or after_hours.get("observed_at")
                or prepared.get("checked_at")
                or prepared.get("prepared_at"),
                "equity_research_count": equity_research_count,
                "equity_selected_count": equity_selected_count,
                "option_research_structure_count": option_research_count,
                "option_structure_count": option_research_count,
                "premarket_parent_eligible_structure_count": (
                    prepared.get("premarket_parent_eligible_structure_count")
                    if isinstance(
                        prepared.get("premarket_parent_eligible_structure_count"),
                        int,
                    )
                    and not isinstance(
                        prepared.get("premarket_parent_eligible_structure_count"),
                        bool,
                    )
                    and 0
                    <= prepared.get("premarket_parent_eligible_structure_count", 0)
                    <= 10
                    else 0
                ),
                "executable_count": 0,
                "equity_pool_hash": formal.get("equity_pool_hash"),
                "option_pool_hash": formal.get("option_pool_hash"),
                "after_hours_campaign_hash": formal.get("campaign_hash"),
                "after_hours_campaign": (
                    dict(campaign) if isinstance(campaign, Mapping) else None
                ),
                "after_hours_formal_research_pools": dict(formal),
                "reconciled_from_durable_after_hours": provider is None,
                "reconciled_from_verified_after_hours": provider is not None,
                "decision": "NO_TRADE",
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }
        )
        campaign_progress = after_hours_campaign_progress_status(after_hours)
        recovered_provider_reasons = {
            "AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID",
            "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE",
            "AFTER_HOURS_VERIFIER_FAILED",
        }
        reasons = [
            str(item)
            for item in prepared.get("reason_codes", ())
            if not (
                provider is not None
                and item in recovered_provider_reasons
            )
            if not (
                item == "EQUITY_POOL_EMPTY_OR_UNAVAILABLE"
                and equity_research_count > 0
            )
            and not (
                item == "OPTION_POOL_EMPTY_OR_UNAVAILABLE"
                and option_research_count > 0
            )
            if not (
                item == "AFTER_HOURS_INDICATIVE_PARTIAL"
                and campaign_progress == "COMPLETE"
            )
        ]
        parent_eligible_count = prepared[
            "premarket_parent_eligible_structure_count"
        ]
        if parent_eligible_count == 0:
            reasons.append("PREMARKET_PARENT_ELIGIBLE_STRUCTURE_UNAVAILABLE")
        else:
            reasons = [
                item
                for item in reasons
                if item != "PREMARKET_PARENT_ELIGIBLE_STRUCTURE_UNAVAILABLE"
            ]
        if campaign_progress == "PARTIAL":
            reasons.append("AFTER_HOURS_INDICATIVE_PARTIAL")
        prepared["reason_codes"] = tuple(dict.fromkeys(reasons))
        prepared["status"] = (
            "READY"
            if prepared.get("next_trading_date") is not None and not reasons
            else "DEGRADED"
        )
        return prepared

    def _complete_callback_job(self, job: _DailyCallbackJob) -> None:
        try:
            completed_at = self._clock()
        except Exception:
            completed_at = None
        job.completed_at = (
            completed_at.astimezone(timezone.utc)
            if isinstance(completed_at, datetime)
            and completed_at.tzinfo is not None
            and completed_at.utcoffset() is not None
            # An invalid clock cannot certify an in-budget completion.
            else job.deadline_at + timedelta(microseconds=1)
        )
        job.event.set()

    def _start_callback_worker(
        self,
        job: _DailyCallbackJob,
        invoke: Callable[[], None],
        *,
        name: str,
    ) -> None:
        with self._lock:
            self._callback_workers = {
                worker: running_job
                for worker, running_job in self._callback_workers.items()
                if worker.is_alive()
            }
            if self._closing.is_set():
                job.cancel_event.set()
                job.error = RuntimeError("SCANNER_CLOSING")
                self._complete_callback_job(job)
                return
            worker = threading.Thread(target=invoke, name=name, daemon=True)
            # Register and start under the same lock as the close fence. A
            # retired job remains here until the actual thread is no longer live.
            self._callback_workers[worker] = job
            try:
                worker.start()
            except Exception as exc:
                self._callback_workers.pop(worker, None)
                job.error = exc
                self._complete_callback_job(job)

    def _acquire_callback_slot(
        self,
        slot: ScanSlot,
        *,
        pipeline_version: str,
        owner: str,
        now: datetime,
    ) -> ScanAcquireResult | None:
        # Lease creation and close admission share one short critical section;
        # no new durable callback can be acquired after the close fence is set.
        with self._lock:
            if self._closing.is_set():
                return None
            return self.service.store.acquire(
                slot,
                pipeline_version=pipeline_version,
                owner=owner,
                now=now,
                lease_seconds=DAILY_CALLBACK_LEASE_SECONDS,
            )

    def _drain_callback_workers(self, deadline: float) -> bool:
        with self._lock:
            workers = tuple(self._callback_workers.items())
            for _worker, job in workers:
                job.cancel_event.set()
        for worker, _job in workers:
            if worker is not threading.current_thread():
                worker.join(timeout=max(0.0, deadline - monotonic()))
        with self._lock:
            self._callback_workers = {
                worker: job
                for worker, job in self._callback_workers.items()
                if worker.is_alive()
            }
            return not self._callback_workers

    def _run_position_research(
        self,
        top10_result: TickResult | None,
        *,
        checked_at: datetime,
    ) -> None:
        """Refresh research after the entry producer correctly yields to management."""

        with self._lock:
            callback = self._position_research_callback
            active_job = self._position_research_job
        if (
            self._closing.is_set()
            or callback is None
            or active_job is not None
            or top10_result is None
            or top10_result.duplicate_reason is not None
            or top10_result.producer_status != "POSITION_MANAGEMENT_ONLY"
            or top10_result.producer_slot != "OPEN_REPRICE_0935"
        ):
            return
        job = _DailyCallbackJob(
            scan_run_id=f"position-research.{uuid.uuid4().hex}",
            owner="position-research",
            operation="POSITION_RESEARCH",
            started_at=checked_at,
            deadline_at=checked_at
            + timedelta(seconds=DAILY_CALLBACK_TIMEOUT_SECONDS),
            event=threading.Event(),
            cancel_event=threading.Event(),
            operation_token=f"position-research-token.{uuid.uuid4().hex}",
        )
        with self._lock:
            self._position_research_job = job

        def invoke() -> None:
            try:
                payload = _invoke_cooperative_callback(callback, (), job)
                if not job.cancel_event.is_set():
                    job.payload = payload
            except BaseException as exc:
                if not job.cancel_event.is_set():
                    job.error = exc
            finally:
                self._complete_callback_job(job)

        self._start_callback_worker(
            job,
            invoke,
            name="options-copilot-position-research",
        )
        job.event.wait(DAILY_CALLBACK_START_GRACE_SECONDS)
        self._reap_position_research_job(checked_at)

    def _reap_position_research_job(self, checked_at: datetime) -> None:
        with self._lock:
            job = self._position_research_job
        if job is None:
            return
        completed_in_time = (
            job.event.is_set()
            and job.completed_at is not None
            and job.completed_at <= job.deadline_at
        )
        timed_out = checked_at >= job.deadline_at and not completed_in_time
        if not completed_in_time and not timed_out:
            return
        if timed_out:
            job.cancel_event.set()
        if completed_in_time and job.error is None:
            try:
                payload = _daily_result_payload(job.payload)
                if payload is None:
                    raise TypeError("position research callback returned no read model")
            except (TypeError, ValueError):
                payload = None
        else:
            payload = None
        if payload is None:
            payload = {
                "status": "FAILED",
                "observed_at": checked_at.isoformat(),
                "reason_codes": (
                    "POSITION_RESEARCH_TIMEOUT"
                    if timed_out
                    else "POSITION_RESEARCH_REFRESH_FAILED",
                ),
            }
        payload["decision_authority"] = "SUPPORTING_ONLY"
        payload["action_pool_count"] = 0
        with self._lock:
            if self._position_research_job is job:
                self._last_daily_results["POSITION_RESEARCH"] = payload
                self._position_research_job = None

    def _run_daily_callback(
        self,
        calendar: UsOptionsCalendarSnapshot,
        checked_at: datetime,
    ) -> None:
        if self._closing.is_set():
            return
        research_fallback = _research_operation_for_minute(checked_at)
        calendar_age = (checked_at - calendar.observed_at).total_seconds()
        if (
            calendar_age < 0
            or calendar_age > float(DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS)
        ) and research_fallback is None:
            return
        trading_date = checked_at.astimezone(US_OPTIONS_TIMEZONE).date()
        operation = next(
            (
                item
                for item in daily_operation_slots_for_session(calendar, trading_date)
                if item.operation in DAILY_OPERATION_PIPELINES
                and item.slot_at.astimezone(timezone.utc)
                <= checked_at
                < item.slot_at.astimezone(timezone.utc) + timedelta(minutes=1)
            ),
            None,
        )
        if operation is None:
            operation = research_fallback
        if operation is None:
            return
        with self._lock:
            callback = self._daily_callbacks[operation.operation]
        if callback is None:
            return
        pipeline_version = DAILY_OPERATION_PIPELINES[operation.operation]
        slot = ScanSlot(
            operation.trading_date,
            operation.slot_at,
            kind=operation.operation,
        )
        monotonic_started = monotonic()
        acquired = self._acquire_callback_slot(
            slot,
            pipeline_version=pipeline_version,
            owner=f"daily-operation.{operation.operation.lower()}",
            now=checked_at,
        )
        if acquired is None or not acquired.acquired:
            return
        owner = acquired.run.owner
        assert owner is not None
        job = _DailyCallbackJob(
            scan_run_id=acquired.run.scan_run_id,
            owner=owner,
            operation=operation.operation,
            started_at=checked_at,
            deadline_at=checked_at
            + timedelta(seconds=DAILY_CALLBACK_TIMEOUT_SECONDS),
            event=threading.Event(),
            cancel_event=threading.Event(),
            operation_token=acquired.run.scan_run_id,
        )
        if operation.operation == "NEXT_SESSION_PREPARATION":
            job.operation_context = ScheduledOperationContext(
                scan_run_id=acquired.run.scan_run_id,
                owner=owner,
                operation=operation.operation,
                pipeline_version=pipeline_version,
                trading_date=operation.trading_date,
                slot_at=operation.slot_at.astimezone(timezone.utc),
                deadline_at=job.deadline_at,
                cancel_event=job.cancel_event,
                _store=self.service.store,
                _clock=self._clock,
                _closing_event=self._closing,
                _monotonic_deadline=(
                    monotonic_started + DAILY_CALLBACK_TIMEOUT_SECONDS
                ),
            )
        with self._lock:
            self._daily_jobs[job.scan_run_id] = job

        def invoke() -> None:
            try:
                args: tuple[object, ...] = (
                    (calendar, operation.slot_at, checked_at)
                    if operation.operation in {
                        "RESEARCH_REFRESH",
                        "NEXT_SESSION_PREPARATION",
                    }
                    else ()
                )
                payload = _invoke_cooperative_callback(callback, args, job)
                if not job.cancel_event.is_set():
                    job.payload = payload
            except BaseException as exc:
                if not job.cancel_event.is_set():
                    job.error = exc
            finally:
                self._complete_callback_job(job)

        self._start_callback_worker(
            job,
            invoke,
            name=f"options-copilot-{operation.operation.lower()}",
        )
        job.event.wait(DAILY_CALLBACK_START_GRACE_SECONDS)
        self._reap_daily_jobs(checked_at)

    def _reap_daily_jobs(self, checked_at: datetime) -> None:
        with self._lock:
            jobs = tuple(self._daily_jobs.values())
        for job in jobs:
            completed_in_time = (
                job.event.is_set()
                and job.completed_at is not None
                and job.completed_at <= job.deadline_at
            )
            timed_out = checked_at >= job.deadline_at and not completed_in_time
            if not completed_in_time and not timed_out:
                continue
            if timed_out:
                job.cancel_event.set()
            if completed_in_time and job.error is None:
                try:
                    payload = _normalise_daily_callback_payload(
                        job.payload,
                        operation=job.operation,
                        checked_at=checked_at,
                    )
                except (TypeError, ValueError):
                    payload = _daily_failure_payload(
                        job.operation,
                        checked_at,
                        reason=f"{job.operation}_INVALID_RESULT",
                    )
                    status = "FAILED"
                else:
                    status = "COMPLETED"
            else:
                reason = (
                    f"{job.operation}_TIMEOUT"
                    if timed_out
                    else f"{job.operation}_FAILED"
                )
                payload = _daily_failure_payload(
                    job.operation,
                    checked_at,
                    reason=reason,
                )
                status = "FAILED"
            if job.operation in {
                "AFTER_HOURS_REPRICE",
                "AFTER_HOURS_REPRICE_RETRY",
            }:
                with self._lock:
                    self._schedule_after_hours_retry(
                        payload,
                        checked_at=checked_at,
                        retry_attempt=job.retry_attempt,
                    )
            try:
                if status == "COMPLETED":
                    self.service.store.complete_with_daily_result(
                        job.scan_run_id,
                        owner=job.owner,
                        operation=job.operation,
                        payload=payload,
                        now=checked_at,
                    )
                else:
                    self.service.store.fail_with_daily_result(
                        job.scan_run_id,
                        owner=job.owner,
                        operation=job.operation,
                        reason=str(payload["reason_codes"][0]),
                        payload=payload,
                        now=checked_at,
                    )
            except (RuntimeError, ValueError) as exc:
                job.terminalization_error = type(exc).__name__
                if self._reconcile_terminal_daily_job(job):
                    continue
                with self._lock:
                    self._status = "DEGRADED"
                continue
            with self._lock:
                self._last_daily_results[job.operation] = payload
                if job.operation == "AFTER_HOURS_REPRICE_RETRY":
                    self._last_daily_results["AFTER_HOURS_REPRICE"] = payload
                self._daily_jobs.pop(job.scan_run_id, None)

    def _reconcile_terminal_daily_job(self, job: _DailyCallbackJob) -> bool:
        """Bound a failed terminalization against the authoritative store state."""

        try:
            run = self.service.store.get(job.scan_run_id)
        except (KeyError, RuntimeError, ValueError):
            return False
        if run.status == "LEASED":
            return False
        durable = self.service.store.daily_result(job.scan_run_id)
        payload = (
            durable.payload
            if durable is not None
            else _daily_failure_payload(
                job.operation,
                job.completed_at or job.deadline_at,
                reason=(run.failure_reason or f"{job.operation}_TERMINALIZATION_LOST"),
            )
        )
        payload["terminalization_error"] = job.terminalization_error
        payload["scan_run_status"] = run.status
        with self._lock:
            self._last_daily_results[job.operation] = payload
            if job.operation == "AFTER_HOURS_REPRICE_RETRY":
                self._last_daily_results["AFTER_HOURS_REPRICE"] = payload
            self._daily_jobs.pop(job.scan_run_id, None)
            self._status = "DEGRADED"
        return True

    def _abandon_daily_jobs(self, closed_at: datetime) -> bool:
        """Fail unfinished callback leases before the owning store is closed."""

        with self._lock:
            jobs = tuple(self._daily_jobs.values())
        succeeded = True
        for job in jobs:
            job.cancel_event.set()
            reason = f"{job.operation}_ABANDONED_ON_CLOSE"
            payload = _daily_failure_payload(
                job.operation,
                closed_at,
                reason=reason,
            )
            try:
                self.service.store.fail_with_daily_result(
                    job.scan_run_id,
                    owner=job.owner,
                    operation=job.operation,
                    reason=reason,
                    payload=payload,
                    now=closed_at,
                )
            except (RuntimeError, ValueError) as exc:
                job.terminalization_error = type(exc).__name__
                succeeded = False
                continue
            with self._lock:
                self._last_daily_results[job.operation] = payload
                self._daily_jobs.pop(job.scan_run_id, None)
        return succeeded

    def _record_daily_manifest(self, checked_at: datetime) -> None:
        calendar = self._last_calendar
        if calendar is None:
            return
        trading_date = checked_at.astimezone(US_OPTIONS_TIMEZONE).date()
        if calendar.session_for(trading_date) is None:
            with self._lock:
                summary_missing = self._last_daily_operations_summary is None
            if summary_missing:
                self._daily_operations_health()
            return
        health = self._daily_operations_health()
        runs = health.get("runs")
        if not isinstance(runs, tuple):
            return
        payload = {
            "schema": "options_copilot.daily_operation_manifest.v1",
            "trading_date": trading_date.isoformat(),
            "observed_at": checked_at.isoformat(),
            "runs": runs,
            "next_session_preparation": health.get(
                "next_session_preparation"
            ),
            "review_only": True,
            "direct_order_submission": False,
        }
        state_payload = dict(payload)
        state_payload.pop("observed_at", None)
        state_hash = canonical_hash(
            {
                "schema": "options_copilot.daily_operation_manifest_state.v1",
                "payload": state_payload,
            }
        )
        if state_hash == self._last_daily_manifest_state_hash:
            return
        latest = self.service.store.latest_daily_manifest(
            trading_date=trading_date
        )
        if latest is not None:
            latest_state_payload = dict(latest.payload)
            latest_state_payload.pop("observed_at", None)
            latest_state_hash = canonical_hash(
                {
                    "schema": "options_copilot.daily_operation_manifest_state.v1",
                    "payload": latest_state_payload,
                }
            )
            if latest_state_hash == state_hash:
                self._last_daily_manifest_state_hash = state_hash
                return
        manifest = self.service.store.record_daily_manifest(
            trading_date=trading_date,
            payload=payload,
            now=checked_at,
        )
        health["day_manifest"] = {
            "manifest_id": manifest.manifest_id,
            "trading_date": manifest.trading_date.isoformat(),
            "manifest_hash": manifest.manifest_hash,
            "recorded_at": manifest.recorded_at.isoformat(),
        }
        self._publish_daily_operations_summary(health)
        self._last_daily_manifest_state_hash = state_hash

    def _schedule_after_hours_retry(
        self,
        payload: dict[str, object],
        *,
        checked_at: datetime,
        retry_attempt: int | None,
    ) -> None:
        priced = int(payload.get("priced_count", 0) or 0)
        requested = int(payload.get("requested_count", 0) or 0)
        reasons = tuple(str(item).upper() for item in payload.get("reason_codes", ()))
        campaign = payload.get("campaign")
        remaining = (
            int(campaign.get("remaining_underlyings", 0) or 0)
            if isinstance(campaign, Mapping)
            else 0
        )
        basis_remaining = (
            int(campaign.get("remaining_basis_underlyings", 0) or 0)
            if isinstance(campaign, Mapping)
            else 0
        )
        needs_retry = (
            remaining > 0
            or basis_remaining > 0
            or (requested > 0 and priced < requested)
        )
        attempts = 0 if retry_attempt is None else retry_attempt
        self._after_hours_retry_attempts = attempts
        payload["retry_attempt"] = attempts
        payload["retry_limit"] = AFTER_HOURS_REPRICE_RETRY_LIMIT
        if not needs_retry:
            self._after_hours_next_retry_at = None
            payload["next_retry_at"] = None
            return
        if attempts >= AFTER_HOURS_REPRICE_RETRY_LIMIT:
            self._after_hours_next_retry_at = None
            payload["next_retry_at"] = None
            payload["retry_exhausted"] = True
            payload["reason_codes"] = list(
                dict.fromkeys(
                    (
                        *reasons,
                        "AFTER_HOURS_REPRICE_RETRY_BUDGET_EXHAUSTED",
                    )
                )
            )
            return
        wait = timedelta(minutes=10) if any("PACING" in item for item in reasons) else timedelta(minutes=1)
        self._after_hours_next_retry_at = checked_at + wait
        payload["next_retry_at"] = self._after_hours_next_retry_at.isoformat()
        payload["retry_exhausted"] = False

    def _restore_after_hours_retry_state(self) -> None:
        self._after_hours_next_retry_at = None
        self._after_hours_retry_attempts = 0
        latest = self.service.store.latest_daily_result("AFTER_HOURS_REPRICE")
        retry = self.service.store.latest_daily_result("AFTER_HOURS_REPRICE_RETRY")
        if retry is not None and (
            latest is None or retry.recorded_at > latest.recorded_at
        ):
            latest = retry
        if latest is None:
            return
        raw_attempts = latest.payload.get("retry_attempt")
        if (
            isinstance(raw_attempts, bool)
            or not isinstance(raw_attempts, int)
            or raw_attempts < 0
            or raw_attempts >= AFTER_HOURS_REPRICE_RETRY_LIMIT
        ):
            return
        raw_due = latest.payload.get("next_retry_at")
        if not isinstance(raw_due, str):
            return
        try:
            due = datetime.fromisoformat(raw_due.replace("Z", "+00:00"))
        except ValueError:
            return
        if due.tzinfo is None or due.utcoffset() is None:
            return
        self._after_hours_retry_attempts = raw_attempts
        self._after_hours_next_retry_at = due.astimezone(timezone.utc)

    def _run_after_hours_retry(self, checked_at: datetime) -> None:
        if self._closing.is_set():
            return
        instant_et = checked_at.astimezone(US_OPTIONS_TIMEZONE)
        with self._lock:
            callback = self._after_hours_reprice_callback
            due = self._after_hours_next_retry_at
            retry_attempt = self._after_hours_retry_attempts + 1
        if (
            callback is None
            or due is None
            or checked_at < due
        ):
            return
        if retry_attempt > AFTER_HOURS_REPRICE_RETRY_LIMIT:
            with self._lock:
                self._after_hours_next_retry_at = None
            return
        if (
            instant_et.weekday() >= 5
            or instant_et.date() != due.astimezone(US_OPTIONS_TIMEZONE).date()
            or instant_et.time() > time(20, 0)
        ):
            self._record_missed_after_hours_retry(due, checked_at=checked_at)
            return
        with self._lock:
            if any(
                job.operation == "AFTER_HOURS_REPRICE_RETRY"
                for job in self._daily_jobs.values()
            ):
                return
        slot_at = checked_at.astimezone(US_OPTIONS_TIMEZONE).replace(
            second=0,
            microsecond=0,
        )
        acquired = self._acquire_callback_slot(
            ScanSlot(slot_at.date(), slot_at, kind="AFTER_HOURS_REPRICE_RETRY"),
            pipeline_version=DAILY_OPERATION_PIPELINES[
                "AFTER_HOURS_REPRICE_RETRY"
            ],
            owner="daily-operation.after_hours_reprice_retry",
            now=checked_at,
        )
        if acquired is None or not acquired.acquired:
            return
        owner = acquired.run.owner
        assert owner is not None
        job = _DailyCallbackJob(
            scan_run_id=acquired.run.scan_run_id,
            owner=owner,
            operation="AFTER_HOURS_REPRICE_RETRY",
            started_at=checked_at,
            deadline_at=checked_at
            + timedelta(seconds=DAILY_CALLBACK_TIMEOUT_SECONDS),
            event=threading.Event(),
            cancel_event=threading.Event(),
            operation_token=acquired.run.scan_run_id,
            retry_attempt=retry_attempt,
        )
        with self._lock:
            self._daily_jobs[job.scan_run_id] = job

        def invoke() -> None:
            try:
                payload = _invoke_cooperative_callback(callback, (), job)
                if not job.cancel_event.is_set():
                    job.payload = payload
            except BaseException as exc:
                if not job.cancel_event.is_set():
                    job.error = exc
            finally:
                self._complete_callback_job(job)

        self._start_callback_worker(
            job,
            invoke,
            name="options-copilot-after-hours-reprice-retry",
        )
        job.event.wait(DAILY_CALLBACK_START_GRACE_SECONDS)
        self._reap_daily_jobs(checked_at)

    def _record_missed_after_hours_retry(
        self,
        due: datetime,
        *,
        checked_at: datetime,
    ) -> None:
        slot_at = due.astimezone(US_OPTIONS_TIMEZONE).replace(second=0, microsecond=0)
        acquired = self.service.store.acquire(
            ScanSlot(slot_at.date(), slot_at, kind="AFTER_HOURS_REPRICE_RETRY"),
            pipeline_version=DAILY_OPERATION_PIPELINES["AFTER_HOURS_REPRICE_RETRY"],
            owner="daily-operation.after_hours_reprice_retry.missed",
            now=checked_at,
            lease_seconds=DAILY_CALLBACK_LEASE_SECONDS,
        )
        if acquired.acquired:
            owner = acquired.run.owner
            assert owner is not None
            payload = _daily_failure_payload(
                "AFTER_HOURS_REPRICE_RETRY",
                checked_at,
                reason="AFTER_HOURS_REPRICE_RETRY_MISSED_ON_RESTART",
            )
            self.service.store.fail_with_daily_result(
                acquired.run.scan_run_id,
                owner=owner,
                operation="AFTER_HOURS_REPRICE_RETRY",
                reason="AFTER_HOURS_REPRICE_RETRY_MISSED_ON_RESTART",
                payload=payload,
                now=checked_at,
            )
            with self._lock:
                self._last_daily_results["AFTER_HOURS_REPRICE"] = payload
        self._after_hours_next_retry_at = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick_once()
            except Exception:
                self._record_failure(
                    datetime.now(timezone.utc),
                    "SCANNER_HEARTBEAT_FAILED",
                )
            if self._stop.wait(self.HEARTBEAT_SECONDS):
                break

    def _record_failure(self, instant: datetime, reason: str) -> TickResult:
        result = TickResult(None, reason, None, "NO_TRADE")
        with self._lock:
            self._last_tick_at = instant.astimezone(timezone.utc)
            self._last_result = result
            self._status = "DEGRADED"
        return result


def _research_operation_for_minute(
    checked_at: datetime,
) -> DailyOperationSlot | None:
    instant = checked_at.astimezone(US_OPTIONS_TIMEZONE)
    if instant.weekday() >= 5 or (instant.hour, instant.minute) != (8, 30):
        return None
    scheduled = instant.replace(second=0, microsecond=0)
    return DailyOperationSlot(
        trading_date=scheduled.date(),
        slot_at=scheduled,
        operation="RESEARCH_REFRESH",
    )


def _producer_payload(value: object) -> dict[str, object]:
    serializer = getattr(value, "as_dict", None)
    if callable(serializer):
        value = serializer()
    if not isinstance(value, Mapping):
        raise TypeError("producer result must be a mapping or expose as_dict()")
    return dict(value)


def _daily_result_payload(value: object) -> dict[str, object] | None:
    serializer = getattr(value, "as_dict", None)
    if callable(serializer):
        value = serializer()
    return dict(value) if isinstance(value, Mapping) else None


def _normalise_daily_callback_payload(
    value: object,
    *,
    operation: str,
    checked_at: datetime,
) -> dict[str, object]:
    payload = _daily_result_payload(value)
    if payload is None:
        raise TypeError("daily operation callback must return an object")
    status = str(payload.get("status") or "").strip().upper()
    if not status:
        raise ValueError("daily operation callback status is required")
    if any(
        payload.get(name) is True
        for name in (
            "approval_eligible",
            "instruction_creation_allowed",
            "order_allowed",
        )
    ):
        raise ValueError("daily operation callback cannot grant authority")
    authority = payload.get("decision_authority")
    if authority not in (None, "SUPPORTING_ONLY"):
        raise ValueError("daily operation callback must remain supporting-only")
    return {
        **payload,
        "schema": "options_copilot.daily_operation_result.v1",
        "operation": operation,
        "status": status,
        "checked_at": str(payload.get("checked_at") or checked_at.isoformat()),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _daily_failure_payload(
    operation: str,
    checked_at: datetime,
    *,
    reason: str,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "options_copilot.daily_operation_result.v1",
        "operation": operation,
        "status": "FAILED",
        "checked_at": checked_at.isoformat(),
        "reason_codes": (reason,),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    if operation == "OUTCOME_PROCESSING":
        payload.update(
            {
                "due_count": 0,
                "records_appended": 0,
                "records_superseded": 0,
                "records_skipped": 0,
                "records_blocked": 0,
                "records_rejected": 1,
                "candidate_ledger_head_hash": None,
                "shadow_ledger_head_hash": None,
                "manifest_hash": None,
                "processing_hash": None,
            }
        )
    return payload


def _reason_codes(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        raise TypeError("producer reason_codes must be an array")
    return tuple(
        dict.fromkeys(
            text
            for item in value
            if (text := str(item).strip().upper())
        )
    )


def _missing_symbols(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        raise TypeError("producer missing_symbols must be an array")
    return tuple(
        dict.fromkeys(
            text
            for item in value
            if (text := str(item).strip().upper())
        )
    )


def _written_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("producer written_count must be an integer")
    if value < 0 or value > 10:
        raise ValueError("producer written_count must be between zero and ten")
    return value


def _optional_result_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _tick_from_durable(
    durable,
    *,
    duplicate_reason: str | None,
    status: str,
) -> TickResult:
    return TickResult(
        durable.scan_run_id,
        duplicate_reason,
        durable.result_hash,
        status,
        durable.producer_status,
        durable.reason_codes,
        durable.missing_symbols,
        durable.written_count,
        durable.producer_slot,
        durable.producer_run_id,
        durable.evidence_hash,
    )
