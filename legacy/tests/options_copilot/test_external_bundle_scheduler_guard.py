from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

import options_copilot.external_bundle_commit as commit_module
from options_copilot.external_bundle_commit import (
    ExternalBundleCommitGuard,
    ExternalBundlePaths,
)
from options_copilot.external_input_cli import publish_external_input_bundle
from options_copilot.market.external_session_calendar import (
    ExternalSessionCalendarProvider,
)
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.scanner.service import (
    TickResult,
    Top10OnlySchedulerLoop,
    Top10SchedulerService,
)
from options_copilot.scanner.scheduler import ScanRunStore, ScanSlot
from tests.options_copilot.test_external_input_cli import (
    PUBLISHED_AT,
    SCHEDULED_FOR,
    _bundle,
    _destinations,
)


class _CountingCalendar:
    def __init__(self) -> None:
        self.calls = 0

    def snapshot(self, *, now):
        self.calls += 1
        return UsOptionsSessionCalendar().normalize(
            liquid_hours="20260806:0930-1600",
            trading_hours="20260806:0930-1600",
            timezone_id="America/New_York",
            observed_at=now,
            source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
            now=now,
        )


class _CountingService(Top10SchedulerService):
    def __init__(self) -> None:
        super().__init__(
            None,
            None,
            pipeline_version="test-external-bundle-guard",
        )
        self.calls = 0

    def tick(self, calendar, *, now=None, producer=None):
        self.calls += 1
        return TickResult(
            "scan-run",
            None,
            "a" * 64,
            "COMPLETED",
            "PREMARKET_FROZEN",
            (),
        )


class _CountingProducer:
    def tick(self, *, scheduled_for):
        return {"status": "PREMARKET_FROZEN"}


def _factory(calendar: _CountingCalendar):
    producer = _CountingProducer()
    return lambda _snapshot, _now: (calendar, producer)


def _guard(destinations: dict[str, Path]) -> ExternalBundleCommitGuard:
    return ExternalBundleCommitGuard(
        ExternalBundlePaths(
            readonly_feed=destinations["readonly_feed_path"],
            top10=destinations["top10_path"],
            session_calendar=destinations["session_calendar_path"],
        )
    )


def test_uncommitted_bundle_does_not_call_calendar_or_consume_service_slot(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    service = _CountingService()
    calendar = _CountingCalendar()
    loop = Top10OnlySchedulerLoop(
        service,
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=_factory(calendar),
        clock=lambda: PUBLISHED_AT,
    )

    result = loop.tick_once()

    assert result.scan_run_id is None
    assert result.duplicate_reason == "EXTERNAL_BUNDLE_NOT_READY"
    assert service.calls == 0
    assert calendar.calls == 0
    assert loop.HEARTBEAT_SECONDS == 1


def test_external_top10_loop_close_preserves_its_independent_contract(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    calendar = _CountingCalendar()
    loop = Top10OnlySchedulerLoop(
        _CountingService(),
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=_factory(calendar),
        clock=lambda: PUBLISHED_AT,
    )

    loop.start()

    assert loop.close() is True
    assert loop.health()["status"] == "STOPPED"


def test_fresh_committed_bundle_is_consumed_inside_guard(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    service = _CountingService()
    calendar = _CountingCalendar()
    loop = Top10OnlySchedulerLoop(
        service,
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=_factory(calendar),
        clock=lambda: PUBLISHED_AT,
    )

    result = loop.tick_once()

    assert result.producer_status == "PREMARKET_FROZEN"
    assert service.calls == 1
    assert calendar.calls == 1


def test_stale_manifest_does_not_call_calendar_or_service(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    service = _CountingService()
    calendar = _CountingCalendar()
    stale_now = PUBLISHED_AT + timedelta(seconds=5)
    loop = Top10OnlySchedulerLoop(
        service,
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=_factory(calendar),
        clock=lambda: stale_now,
    )

    result = loop.tick_once()

    assert result.duplicate_reason == "EXTERNAL_BUNDLE_NOT_READY"
    assert service.calls == 0
    assert calendar.calls == 0
    assert stale_now > SCHEDULED_FOR.astimezone(stale_now.tzinfo)


def test_bundle_expiring_during_preflight_does_not_acquire_service_slot(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    service = _CountingService()
    calendar = _CountingCalendar()
    instants = iter(
        (
            PUBLISHED_AT,
            PUBLISHED_AT + timedelta(seconds=5),
        )
    )
    loop = Top10OnlySchedulerLoop(
        service,
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=_factory(calendar),
        clock=lambda: next(instants),
    )

    result = loop.tick_once()

    assert result.duplicate_reason == "EXTERNAL_BUNDLE_NOT_READY"
    assert service.calls == 0
    assert calendar.calls == 0


def test_bundle_runtime_factory_is_required_and_cannot_return_none(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    service = _CountingService()
    calendar = _CountingCalendar()

    with pytest.raises(TypeError, match="bundle_runtime_factory must be callable"):
        Top10OnlySchedulerLoop(
            service,
            calendar,
            bundle_guard=_guard(destinations),
            bundle_runtime_factory=None,  # type: ignore[arg-type]
            clock=lambda: PUBLISHED_AT,
        )

    publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    loop = Top10OnlySchedulerLoop(
        service,
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=lambda _snapshot, _now: (calendar, None),  # type: ignore[return-value]
        clock=lambda: PUBLISHED_AT,
    )

    result = loop.tick_once()

    assert result.duplicate_reason == "EXTERNAL_BUNDLE_NOT_READY"
    assert service.calls == 0
    assert calendar.calls == 0


def test_final_time_revalidates_calendar_before_service_slot_acquire(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    bundle = _bundle()
    bundle["session_calendar"]["observed_at"] = PUBLISHED_AT - timedelta(
        seconds=5
    )
    publish_external_input_bundle(
        bundle,
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    scan_store = ScanRunStore(tmp_path / "scan-runs.sqlite3")
    pipeline_version = "final-time-calendar-revalidation"
    service = Top10SchedulerService(
        scan_store,
        _CountingProducer(),
        pipeline_version=pipeline_version,
    )
    calendar = _CountingCalendar()
    final_time = PUBLISHED_AT + timedelta(seconds=4)
    instants = iter((PUBLISHED_AT, final_time))

    def runtime_factory(snapshot, checked_at):
        provider = ExternalSessionCalendarProvider(
            snapshot.paths.session_calendar,
            clock=lambda: checked_at,
        )
        return provider, _CountingProducer()

    loop = Top10OnlySchedulerLoop(
        service,
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=runtime_factory,
        clock=lambda: next(instants),
    )

    try:
        result = loop.tick_once()
        unconsumed = scan_store.acquire(
            ScanSlot(SCHEDULED_FOR.date(), SCHEDULED_FOR),
            pipeline_version=pipeline_version,
            owner="post-check",
            now=final_time,
        )

        assert result.duplicate_reason == "EXTERNAL_BUNDLE_NOT_READY"
        assert unconsumed.acquired is True
        assert calendar.calls == 0
    finally:
        scan_store.close()


def test_snapshot_cleanup_failure_happens_before_service_slot_acquire(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destinations = _destinations(tmp_path)
    publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    service = _CountingService()
    calendar = _CountingCalendar()
    real_temporary_directory = commit_module.tempfile.TemporaryDirectory

    class FailingCleanup:
        def __init__(self, *args, **kwargs) -> None:
            self._inner = real_temporary_directory(*args, **kwargs)

        def __enter__(self):
            return self._inner.__enter__()

        def __exit__(self, *exc_info) -> None:
            self._inner.__exit__(*exc_info)
            raise OSError("simulated snapshot cleanup failure")

    monkeypatch.setattr(
        commit_module.tempfile,
        "TemporaryDirectory",
        FailingCleanup,
    )
    loop = Top10OnlySchedulerLoop(
        service,
        calendar,
        bundle_guard=_guard(destinations),
        bundle_runtime_factory=_factory(calendar),
        clock=lambda: PUBLISHED_AT,
    )

    result = loop.tick_once()

    assert result.duplicate_reason == "EXTERNAL_BUNDLE_NOT_READY"
    assert service.calls == 0
    assert calendar.calls == 1
