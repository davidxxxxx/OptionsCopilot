"""Adversarial parent-lease, shared-budget, and retired-worker lifecycle tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import threading

import pytest

from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.scanner.operation_context import ScheduledOperationContext
from options_copilot.scanner.scheduler import (
    DAILY_OPERATION_PIPELINES,
    ScanRunStore,
    ScanSlot,
)
from options_copilot.scanner.service import (
    ScanSchedulerLoop,
    ScanSchedulerService,
    TickResult,
)


NOW = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)


def _calendar(now, hours="20260804:0930-1600"):
    return UsOptionsSessionCalendar().normalize(
        liquid_hours=hours,
        trading_hours=hours,
        timezone_id="America/New_York",
        observed_at=now,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=now,
    )


class _CalendarProvider:
    def snapshot(self, *, now):
        return _calendar(now)


class _Pipeline:
    def __init__(self):
        self.calls = []

    def run_slot(self, scan_run_id, slot_at):
        self.calls.append(scan_run_id)
        return {"decision": "NO_TRADE", "reason_codes": ("TEST_ONLY",)}


def _loop(store, current, calendar_provider=None):
    return ScanSchedulerLoop(
        ScanSchedulerService(store, _Pipeline(), pipeline_version="test-v1"),
        calendar_provider or _CalendarProvider(),
        clock=lambda: current[0],
    )


def _context(store, current, monotonic_now, *, lease_seconds=120):
    operation = "NEXT_SESSION_PREPARATION"
    slot_at = current[0].replace(second=0)
    acquired = store.acquire(
        ScanSlot(slot_at.date(), slot_at, kind=operation),
        pipeline_version=DAILY_OPERATION_PIPELINES[operation],
        owner="daily-operation.next_session_preparation",
        now=current[0],
        lease_seconds=lease_seconds,
    )
    assert acquired.acquired
    return ScheduledOperationContext(
        scan_run_id=acquired.run.scan_run_id,
        owner=acquired.run.owner,
        operation=operation,
        pipeline_version=DAILY_OPERATION_PIPELINES[operation],
        trading_date=slot_at.date(),
        slot_at=slot_at,
        deadline_at=current[0] + timedelta(seconds=60),
        cancel_event=threading.Event(),
        _store=store,
        _clock=lambda: current[0],
        _closing_event=threading.Event(),
        _monotonic_deadline=monotonic_now[0] + 60,
        _monotonic_clock=lambda: monotonic_now[0],
    )


def test_context_requires_real_exact_leased_parent_and_is_immutable(tmp_path):
    current, monotonic_now = [NOW], [100.0]
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        context = _context(store, current, monotonic_now)
        assert context.is_active()
        context.require_active()
        assert context.remaining_seconds() == 60
        with pytest.raises(FrozenInstanceError):
            context.owner = "someone-else"
        alterations = (
            {"scan_run_id": "DIRECT_RUNTIME_CALL"},
            {"owner": "someone-else"},
            {"operation": "RESEARCH_REFRESH"},
            {"pipeline_version": "daily-research-refresh-v1"},
            {"trading_date": NOW.date() + timedelta(days=1)},
            {"slot_at": context.slot_at - timedelta(minutes=1)},
            {"deadline_at": NOW},
            {"deadline_at": NOW.replace(tzinfo=None)},
        )
        for alteration in alterations:
            invalid = replace(context, **alteration)
            assert invalid.is_active() is False
            with pytest.raises(RuntimeError, match="SCHEDULED_OPERATION_NOT_ACTIVE"):
                invalid.require_active()
        store.complete(
            context.scan_run_id,
            owner=context.owner,
            result_hash="completed",
            now=NOW,
        )
        assert context.is_active() is False
    assert context.is_active() is False


@pytest.mark.parametrize(
    ("column", "value"),
    (
        ("owner", "other-owner"),
        ("pipeline_version", "other-pipeline"),
        ("trading_date", "2026-08-05"),
        ("slot_at", "2026-08-04T20:39:00+00:00"),
        ("status", "FAILED"),
        ("lease_expires_at", "2026-08-04T20:40:30+00:00"),
    ),
)
def test_context_rechecks_changed_durable_identity(tmp_path, column, value):
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        context = _context(store, [NOW], [100.0])
        assert context.is_active()
        store._connection.execute(
            f"UPDATE scan_runs SET {column}=? WHERE scan_run_id=?",
            (value, context.scan_run_id),
        )
        assert context.is_active() is False


def test_shared_budget_never_resets_on_wall_clock_rollback(tmp_path):
    current, monotonic_now = [NOW], [100.0]
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        context = _context(store, current, monotonic_now)
        monotonic_now[0] += 25
        current[0] -= timedelta(seconds=10)
        assert context.remaining_seconds() == 35
        monotonic_now[0] += 35
        assert context.remaining_seconds() == 0
        assert context.is_active() is False


def test_context_rechecks_expiry_after_store_read_delay(tmp_path, monkeypatch):
    current, monotonic_now = [NOW], [100.0]
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        context = _context(store, current, monotonic_now, lease_seconds=1)
        original_get = store.get

        def slow_get(run_id):
            result = original_get(run_id)
            current[0] += timedelta(seconds=2)
            return result

        monkeypatch.setattr(store, "get", slow_get)
        assert context.is_active() is False


@pytest.mark.parametrize("boundary", ("cancel", "close", "invalid_clock", "deadline"))
def test_context_fails_closed_at_local_boundaries(tmp_path, boundary):
    current, monotonic_now = [NOW], [100.0]
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        context = _context(store, current, monotonic_now)
        if boundary == "cancel":
            context.cancel_event.set()
        elif boundary == "close":
            context._closing_event.set()
        elif boundary == "invalid_clock":
            current[0] = NOW.replace(tzinfo=None)
        else:
            current[0] += timedelta(seconds=60)
        assert context.remaining_seconds() == 0
        assert context.is_active() is False


def test_scheduler_passes_context_only_from_actual_preparation_acquisition(tmp_path):
    current = [NOW]
    captured = []
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, current)

        def prepare(calendar, scheduled_for, checked_at, *, operation_context):
            context = operation_context
            assert isinstance(context, ScheduledOperationContext)
            assert context.is_active()
            assert context.slot_at == scheduled_for
            assert context.deadline_at == checked_at + timedelta(seconds=60)
            assert store.get(context.scan_run_id).status == "LEASED"
            captured.append(context)
            return {"status": "DEGRADED", "reason_codes": ("TEST_SOURCE_ONLY",)}

        loop.bind_daily_callbacks(next_session_preparation=prepare)
        try:
            loop.tick_once()
            loop.tick_once()
            assert len(captured) == 1
            for worker in tuple(loop._callback_workers):
                worker.join(timeout=1)
            loop._reap_daily_jobs(NOW)
            assert captured[0].is_active() is False
            durable = store.latest_daily_result("NEXT_SESSION_PREPARATION")
            assert durable is not None
            assert durable.payload["approval_eligible"] is False
            assert durable.payload["instruction_creation_allowed"] is False
            assert durable.payload["order_allowed"] is False
        finally:
            assert loop.close()


@pytest.mark.parametrize("legacy", (True, False))
def test_nonpreparation_callbacks_keep_legacy_signature_and_no_context(tmp_path, legacy):
    current = [NOW.replace(minute=15)]
    calls = []

    def no_arguments():
        calls.append({})
        return {"status": "COMPLETED"}

    def accepting_kwargs(**kwargs):
        calls.append(kwargs)
        return {"status": "COMPLETED"}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, current)
        loop.bind_daily_callbacks(
            outcome_process=no_arguments if legacy else accepting_kwargs,
        )
        try:
            loop.tick_once()
            assert len(calls) == 1
            assert "operation_context" not in calls[0]
        finally:
            assert loop.close()


@pytest.mark.parametrize("lane", ("daily", "position", "retry"))
def test_close_retains_timed_out_workers_after_active_job_removed(
    tmp_path, monkeypatch, lane,
):
    monkeypatch.setattr(
        "options_copilot.scanner.service.SCANNER_CLOSE_TIMEOUT_SECONDS", 0.03,
    )
    current = [NOW.replace(minute=15)]
    entered, release = threading.Event(), threading.Event()

    def blocked_callback():
        entered.set()
        release.wait(timeout=5)
        return {"status": "COMPLETED"}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, current)
        if lane == "daily":
            loop.bind_daily_callbacks(outcome_process=blocked_callback)
            loop.tick_once()
        elif lane == "position":
            loop.bind_daily_callbacks(position_research=blocked_callback)
            loop._run_position_research(
                TickResult(
                    "test-top10", None, None, "COMPLETED",
                    producer_status="POSITION_MANAGEMENT_ONLY",
                    producer_slot="OPEN_REPRICE_0935",
                ),
                checked_at=current[0],
            )
        else:
            loop.bind_daily_callbacks(after_hours_reprice=blocked_callback)
            loop._after_hours_next_retry_at = current[0]
            loop._run_after_hours_retry(current[0])
        try:
            assert entered.wait(timeout=1)
            current[0] += timedelta(seconds=61)
            loop._reap_daily_jobs(current[0])
            loop._reap_position_research_job(current[0])
            assert loop._daily_jobs == {}
            assert loop._position_research_job is None
            assert loop.close() is False
            assert loop.health()["status"] == "CLOSE_CALLBACK_TIMEOUT"
            assert loop.health()["callback_workers_alive"] == 1
            assert loop._callback_workers
            assert all(job.cancel_event.is_set() for job in loop._callback_workers.values())
        finally:
            release.set()
            for worker in tuple(loop._callback_workers):
                worker.join(timeout=1)
            assert loop.close() is True
        assert loop._callback_workers == {}
        assert loop.health()["status"] == "STOPPED"


def test_close_drains_actual_thread_even_after_completion_event_is_set(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        "options_copilot.scanner.service.SCANNER_CLOSE_TIMEOUT_SECONDS", 0.03,
    )
    entered, release = threading.Event(), threading.Event()
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, [NOW])
        original_complete = loop._complete_callback_job

        def still_exiting(job):
            original_complete(job)
            entered.set()
            release.wait(timeout=5)

        monkeypatch.setattr(loop, "_complete_callback_job", still_exiting)
        loop.bind_daily_callbacks(
            next_session_preparation=lambda *args: {"status": "DEGRADED"},
        )
        loop.tick_once()
        try:
            assert entered.wait(timeout=1)
            assert loop._daily_jobs == {}
            assert all(job.event.is_set() for job in loop._callback_workers.values())
            assert loop.close() is False
            assert loop.health()["status"] == "CLOSE_CALLBACK_TIMEOUT"
        finally:
            release.set()
            for worker in tuple(loop._callback_workers):
                worker.join(timeout=1)
            assert loop.close()


def test_close_reconciles_expired_callback_lease_without_replay(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "options_copilot.scanner.service.SCANNER_CLOSE_TIMEOUT_SECONDS", 0.03,
    )
    current = [NOW]
    entered, release = threading.Event(), threading.Event()
    calls = []

    def blocked_callback(*args):
        calls.append(args)
        entered.set()
        release.wait(timeout=5)
        return {"status": "DEGRADED"}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, current)
        loop.bind_daily_callbacks(next_session_preparation=blocked_callback)
        loop.tick_once()
        try:
            assert entered.wait(timeout=1)
            current[0] += timedelta(seconds=121)
            assert loop.close() is False
            assert loop.health()["status"] == "CLOSE_CALLBACK_TIMEOUT"
            assert loop._daily_jobs == {}
            durable = store.latest_daily_result("NEXT_SESSION_PREPARATION")
            assert durable is not None
            assert durable.payload["reason_codes"] == ["LEASE_EXPIRED"]
        finally:
            release.set()
            for worker in tuple(loop._callback_workers):
                worker.join(timeout=1)
            assert loop.close()
        assert len(calls) == 1


def test_successful_close_is_idempotent_after_store_shutdown(tmp_path):
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, [NOW])
        assert loop.close()
    assert loop.close()


def test_close_storage_failure_is_retryable_and_keeps_dependencies_owned(
    tmp_path, monkeypatch,
):
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, [NOW])
        original_expire = store.expire_leases

        def unavailable_store(*, now):
            raise RuntimeError("TEST_STORAGE_UNAVAILABLE")

        monkeypatch.setattr(store, "expire_leases", unavailable_store)
        assert loop.close() is False
        assert loop.summary()["status"] == "CLOSE_DAILY_JOB_FAILURE"
        monkeypatch.setattr(store, "expire_leases", original_expire)
        assert loop.close() is True


@pytest.mark.parametrize("boundary", ("timeout", "close", "parent_terminal"))
def test_scheduler_context_rejects_late_child_send_and_commit(
    tmp_path, monkeypatch, boundary,
):
    monkeypatch.setattr(
        "options_copilot.scanner.service.SCANNER_CLOSE_TIMEOUT_SECONDS", 0.03,
    )
    current = [NOW]
    entered, release = threading.Event(), threading.Event()
    effects, captured = [], []

    def prepare(*args, operation_context):
        captured.append(operation_context)
        entered.set()
        release.wait(timeout=5)
        # Deliberately ignore the cooperative cancel event. The trusted lease
        # guard must still fence each producer side effect after parent loss.
        if operation_context.is_active():
            effects.append("broker_send")
        if operation_context.is_active():
            effects.append("source_commit")
        return {"status": "DEGRADED"}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, current)
        loop.bind_daily_callbacks(next_session_preparation=prepare)
        loop.tick_once()
        try:
            assert entered.wait(timeout=1)
            assert captured[0].is_active()
            if boundary == "timeout":
                current[0] += timedelta(seconds=61)
                loop._reap_daily_jobs(current[0])
            elif boundary == "close":
                assert loop.close() is False
            else:
                store.fail(
                    captured[0].scan_run_id,
                    owner=captured[0].owner,
                    reason="TEST_PARENT_REVOKED",
                    now=NOW,
                )
            assert captured[0].is_active() is False
        finally:
            release.set()
            for worker in tuple(loop._callback_workers):
                worker.join(timeout=1)
            assert loop.close()
        assert effects == []


@pytest.mark.parametrize("entrypoint", ("tick_once", "run_now"))
def test_close_fences_manual_tick_and_refuses_new_admission(
    tmp_path, monkeypatch, entrypoint,
):
    monkeypatch.setattr(
        "options_copilot.scanner.service.SCANNER_CLOSE_TIMEOUT_SECONDS", 0.03,
    )
    entered, release = threading.Event(), threading.Event()
    calendar_calls, callback_calls = [], []

    class BlockingCalendar:
        def snapshot(self, *, now):
            calendar_calls.append(now)
            entered.set()
            release.wait(timeout=5)
            return _calendar(now)

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, [NOW], BlockingCalendar())
        loop.bind_daily_callbacks(
            next_session_preparation=lambda *args: callback_calls.append(args),
        )
        worker = threading.Thread(target=getattr(loop, entrypoint), daemon=True)
        worker.start()
        try:
            assert entered.wait(timeout=1)
            assert loop.close() is False
            assert loop.health()["status"] == "CLOSE_TICK_TIMEOUT"
        finally:
            release.set()
            worker.join(timeout=1)
            assert loop.close() is True
        assert callback_calls == []
        assert loop.service.pipeline.calls == []
        assert getattr(loop, entrypoint)().duplicate_reason == "SCANNER_CLOSING"
        assert len(calendar_calls) == 1


@pytest.mark.parametrize(
    ("now", "hours", "expected_calls"),
    (
        (NOW + timedelta(minutes=1), "20260804:0930-1600", 0),
        (NOW, "20260804:CLOSED", 0),
        (NOW.replace(hour=17), "20260804:0930-1300", 1),
        (NOW, "20260804:0930-1300", 0),
        (
            datetime(2026, 11, 3, 21, 40, 30, tzinfo=timezone.utc),
            "20261103:0930-1600", 1,
        ),
    ),
)
def test_preparation_context_obeys_natural_minute_early_close_and_dst(
    tmp_path, now, hours, expected_calls,
):
    calls = []
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = _loop(store, [now])

        def prepare(*args, operation_context):
            calls.append(operation_context.is_active())
            return {"status": "DEGRADED"}

        loop.bind_daily_callbacks(next_session_preparation=prepare)
        try:
            loop._run_daily_callback(_calendar(now, hours), now)
            loop._run_daily_callback(_calendar(now, hours), now)
            assert calls == [True] * expected_calls
        finally:
            assert loop.close()
