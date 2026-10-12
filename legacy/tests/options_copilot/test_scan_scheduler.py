from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading
import time

import pytest

from options_copilot.after_hours_indicative import after_hours_campaign_lineage_hash
from options_copilot.market.session_calendar import (
    CalendarStatus,
    US_OPTIONS_TIMEZONE,
    UsOptionsSessionCalendar,
)
from options_copilot.option_pool.models import option_candidate_identity
from options_copilot.scanner.cli import main as scanner_main
from options_copilot.scanner.scheduler import (
    DAILY_OPERATION_PIPELINES,
    ScanRunStore,
    ScanSlot,
    daily_operation_slots_for_session,
    slots_for_session,
)
from options_copilot.scanner.service import (
    AFTER_HOURS_REPRICE_RETRY_LIMIT,
    DURABLE_PRODUCER_RESULT_STATUS_UNAVAILABLE,
    DecisionPipelinePort,
    ScanSchedulerLoop,
    ScanSchedulerService,
    Top10SchedulerService,
    _after_hours_candidate_identity_manifest,
    _after_hours_campaign_hash,
    _valid_formal_descriptor,
)
from options_copilot.storage.canonical import canonical_hash
from options_copilot.runtime import (
    _after_hours_campaign_hash as _runtime_after_hours_campaign_hash,
)


# The fixture callback blocks for two seconds. One second still proves the
# scheduler does not wait synchronously while tolerating loaded Windows CI I/O.
NONBLOCKING_CALLBACK_ASSERTION_SECONDS = 1.0


def _calendar(now: datetime, hours: str = "20260804:0930-1600"):
    return UsOptionsSessionCalendar().normalize(
        liquid_hours=hours, trading_hours=hours, timezone_id="America/New_York",
        observed_at=now, source="IBKR_REQ_CONTRACT_DETAILS_READONLY", now=now,
    )


def test_lease_is_durable_and_duplicate_has_fixed_reason(tmp_path):
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    slots = slots_for_session(_calendar(now), now.date())
    with ScanRunStore(tmp_path / "runs.sqlite") as first, ScanRunStore(tmp_path / "runs.sqlite") as second:
        claim = first.acquire(slots[0], pipeline_version="p4", owner="a", now=now)
        duplicate = second.acquire(slots[0], pipeline_version="p4", owner="b", now=now)
        assert claim.acquired and not duplicate.acquired
        assert duplicate.duplicate_reason == "SLOT_LEASE_HELD"
        first.complete(claim.run.scan_run_id, owner="a", result_hash="h", now=now)
        assert second.acquire(slots[0], pipeline_version="p4", owner="b", now=now).duplicate_reason == "SLOT_ALREADY_COMPLETED"


def _operational_timing(scan_run_id: str) -> dict[str, object]:
    return {
        "schema": "options_copilot.scan_operational_timing.v1",
        "scan_run_id": scan_run_id,
        "total_duration_ms": 13,
        "stages": (
            {"stage": "INITIALIZATION", "duration_ms": 2},
            {"stage": "BROKER_EVIDENCE", "duration_ms": 11},
        ),
        "decision_authority": "OBSERVATION_ONLY",
        "affects_decision": False,
    }


def test_operational_timing_is_atomic_integrity_checked_and_append_only(tmp_path):
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    path = tmp_path / "timed-runs.sqlite"
    slot = slots_for_session(_calendar(now), now.date())[0]

    with ScanRunStore(path) as store:
        claim = store.acquire(slot, pipeline_version="timed-v1", owner="a", now=now)
        run_id = claim.run.scan_run_id
        completed = store.complete(
            run_id,
            owner="a",
            result_hash="result-hash",
            now=now,
            operational_timing=_operational_timing(run_id),
        )
        timing = store.operational_timing(run_id)

        assert completed.status == "COMPLETED"
        assert completed.result_hash == "result-hash"
        assert timing is not None
        assert timing["total_duration_ms"] == 13
        assert timing["timing_hash"] == canonical_hash(
            {
                "schema": "options_copilot.scan_operational_timing_record.v1",
                "scan_run_id": run_id,
                "timing": _operational_timing(run_id),
                "recorded_at": timing["recorded_at"],
            }
        )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(
                "UPDATE scan_operational_timings SET timing_json='{}' "
                "WHERE scan_run_id=?",
                (run_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(
                "DELETE FROM scan_operational_timings WHERE scan_run_id=?",
                (run_id,),
            )

    with ScanRunStore(path) as reopened:
        assert reopened.operational_timing(run_id) == timing


def test_invalid_operational_timing_is_dropped_without_changing_scan_result(tmp_path):
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    slot = slots_for_session(_calendar(now), now.date())[0]

    with ScanRunStore(tmp_path / "invalid-timing.sqlite") as store:
        claim = store.acquire(slot, pipeline_version="timed-v1", owner="a", now=now)
        completed = store.complete(
            claim.run.scan_run_id,
            owner="a",
            result_hash="authoritative-result",
            now=now,
            operational_timing={
                **_operational_timing("wrong-run"),
                "affects_decision": True,
            },
        )

        assert completed.status == "COMPLETED"
        assert completed.result_hash == "authoritative-result"
        assert store.operational_timing(claim.run.scan_run_id) is None


def test_operational_timing_reader_rejects_tampered_integrity_hash(tmp_path):
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    path = tmp_path / "tampered-timing.sqlite"
    slot = slots_for_session(_calendar(now), now.date())[0]

    with ScanRunStore(path) as store:
        claim = store.acquire(slot, pipeline_version="timed-v1", owner="a", now=now)
        run_id = claim.run.scan_run_id
        store.complete(
            run_id,
            owner="a",
            result_hash="authoritative-result",
            now=now,
            operational_timing=_operational_timing(run_id),
        )

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER scan_operational_timings_no_update")
        connection.execute(
            "UPDATE scan_operational_timings SET timing_hash=? WHERE scan_run_id=?",
            ("0" * 64, run_id),
        )

    with ScanRunStore(path) as reopened:
        with pytest.raises(ValueError, match="integrity"):
            reopened.operational_timing(run_id)


def test_existing_scan_database_adds_optional_operational_timing_table(tmp_path):
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    path = tmp_path / "legacy-runs.sqlite"
    slot = slots_for_session(_calendar(now), now.date())[0]

    with ScanRunStore(path) as initial:
        claim = initial.acquire(slot, pipeline_version="legacy-v1", owner="a", now=now)
        initial.complete(
            claim.run.scan_run_id,
            owner="a",
            result_hash="legacy-result",
            now=now,
        )
        initial._connection.execute("DROP TRIGGER scan_operational_timings_no_update")
        initial._connection.execute("DROP TRIGGER scan_operational_timings_no_delete")
        initial._connection.execute("DROP TABLE scan_operational_timings")

    with ScanRunStore(path) as upgraded:
        assert upgraded.get(claim.run.scan_run_id).result_hash == "legacy-result"
        assert upgraded.operational_timing(claim.run.scan_run_id) is None
        table = upgraded._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='scan_operational_timings'"
        ).fetchone()
        assert table is not None


def test_recovery_marks_old_slots_without_a_catchup_storm(tmp_path):
    now = datetime(2026, 8, 4, 18, 30, tzinfo=timezone.utc)  # 14:30 ET
    calendar = _calendar(now)
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        claim = store.recover(slots_for_session(calendar, now.date()), pipeline_version="p4", owner="one", now=now)
        assert claim is not None and claim.acquired
        assert store.runs_for_slot(slots_for_session(calendar, now.date())[0], pipeline_version="p4")[0].status == "MISSED_NOT_REPLAYED"


def test_ordinary_scan_slots_exclude_exact_only_freeze_and_reprice():
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    slots = slots_for_session(_calendar(now), now.date())

    assert [slot.slot_at.strftime("%H:%M") for slot in slots] == [
        "10:00",
        "11:30",
        "13:30",
        "15:30",
    ]


def test_daily_operation_schedule_exposes_every_fixed_et_run():
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)

    operations = daily_operation_slots_for_session(_calendar(now), now.date())

    assert [(item.operation, item.slot_at.strftime("%H:%M")) for item in operations] == [
        ("RESEARCH_REFRESH", "08:30"),
        ("TOP10_FREEZE", "09:20"),
        ("TOP10_REPRICE", "09:35"),
        ("ORDINARY_SCAN", "10:00"),
        ("ORDINARY_SCAN", "11:30"),
        ("ORDINARY_SCAN", "13:30"),
        ("ORDINARY_SCAN", "15:30"),
        ("OUTCOME_PROCESSING", "16:15"),
        ("AFTER_HOURS_DISCOVERY", "16:20"),
        ("AFTER_HOURS_REPRICE", "16:30"),
        ("NEXT_SESSION_PREPARATION", "16:40"),
    ]


class _Pipeline(DecisionPipelinePort):
    def __init__(self): self.calls = []
    def run_slot(self, scan_run_id, slot_at):
        self.calls.append((scan_run_id, slot_at)); return {"decision": "NO_TRADE"}


class _TimedPipeline(_Pipeline):
    def __init__(self):
        super().__init__()
        self.last_operational_timing: Mapping[str, object] = {}

    def run_slot(self, scan_run_id, slot_at):
        result = super().run_slot(scan_run_id, slot_at)
        self.last_operational_timing = _operational_timing(scan_run_id)
        return result


class _Top10Producer:
    def __init__(self):
        self.calls = 0
        self.scheduled_for = []

    def tick(self, *, scheduled_for):
        self.calls += 1
        self.scheduled_for.append(scheduled_for)
        return {
            "status": "PREMARKET_FROZEN",
            "reason_codes": (),
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }


def test_service_runs_one_pipeline_per_due_slot(tmp_path):
    now = datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)  # 10:05 ET: 10:00 slot
    pipeline = _Pipeline()
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        service = ScanSchedulerService(store, pipeline, pipeline_version="p4", owner="test")
        first = service.tick(_calendar(now), now=now)
        second = service.tick(_calendar(now), now=now)
    assert first.status == "COMPLETED"
    assert len(pipeline.calls) == 1
    assert second.duplicate_reason == "SLOT_ALREADY_COMPLETED"


def test_service_persists_timing_without_changing_result_hash(tmp_path):
    now = datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)
    pipeline = _TimedPipeline()
    expected_hash = canonical_hash({"decision": "NO_TRADE"})

    with ScanRunStore(tmp_path / "service-timing.sqlite") as store:
        service = ScanSchedulerService(
            store,
            pipeline,
            pipeline_version="timed-v1",
            owner="test",
        )
        result = service.tick(_calendar(now), now=now)
        assert result.scan_run_id is not None
        stored = store.get(result.scan_run_id)
        timing = store.operational_timing(result.scan_run_id)

    assert stored.result_hash == result.result_hash
    assert result.result_hash == expected_hash
    assert timing is not None
    assert timing["total_duration_ms"] == 13
    assert timing["decision_authority"] == "OBSERVATION_ONLY"
    assert timing["affects_decision"] is False


def test_service_runs_operator_requested_scan_at_actual_market_time(tmp_path):
    now = datetime(2026, 8, 4, 15, 10, 29, tzinfo=timezone.utc)  # 11:10 ET
    pipeline = _Pipeline()
    calendar = _calendar(now)

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        service = ScanSchedulerService(
            store,
            pipeline,
            pipeline_version="p4",
            owner="manual-test",
        )
        result = service.run_now(calendar, now=now)
        manual_slot = ScanSlot(
            now.astimezone(US_OPTIONS_TIMEZONE).date(),
            now.astimezone(US_OPTIONS_TIMEZONE),
            kind="MANUAL_READ_ONLY_SCAN",
        )
        stored = store.runs_for_slot(manual_slot, pipeline_version="p4")

    assert result.status == "COMPLETED"
    assert result.scan_run_id is not None
    assert len(pipeline.calls) == 1
    assert pipeline.calls[0][1] == now.astimezone(US_OPTIONS_TIMEZONE)
    assert len(stored) == 1
    assert stored[0].scan_run_id == result.scan_run_id


def test_service_refuses_operator_requested_scan_outside_market_session(tmp_path):
    now = datetime(2026, 8, 4, 21, 0, tzinfo=timezone.utc)  # 17:00 ET
    pipeline = _Pipeline()

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        result = ScanSchedulerService(
            store,
            pipeline,
            pipeline_version="p4",
        ).run_now(_calendar(now), now=now)

    assert result.status == "NO_TRADE"
    assert result.duplicate_reason == "MARKET_SESSION_NOT_OPEN"
    assert pipeline.calls == []


def test_service_reports_weekend_closed_before_degraded_calendar(tmp_path):
    now = datetime(2026, 8, 29, 16, 0, tzinfo=timezone.utc)  # Saturday 12:00 ET
    pipeline = _Pipeline()
    calendar = replace(
        _calendar(now),
        status=CalendarStatus.DEGRADED,
        reason_codes=("CALENDAR_SESSION_UNAVAILABLE",),
        sessions=(),
    )

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        result = ScanSchedulerService(
            store,
            pipeline,
            pipeline_version="p4",
        ).run_now(calendar, now=now)

    assert result.status == "NO_TRADE"
    assert result.duplicate_reason == "MARKET_SESSION_NOT_OPEN"
    assert result.scan_run_id is None
    assert pipeline.calls == []


def test_service_reports_calendar_pacing_denial_instead_of_market_closed(tmp_path):
    now = datetime(2026, 8, 4, 15, 10, 29, tzinfo=timezone.utc)  # 11:10 ET
    pipeline = _Pipeline()
    calendar = replace(
        _calendar(now),
        status=CalendarStatus.DEGRADED,
        reason_codes=("CALENDAR_PACING_DENIED",),
        sessions=(),
    )

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        result = ScanSchedulerService(
            store,
            pipeline,
            pipeline_version="p4",
        ).run_now(calendar, now=now)

    assert result.status == "NO_TRADE"
    assert result.duplicate_reason == "CALENDAR_PACING_DENIED"
    assert pipeline.calls == []


def test_manual_loop_health_degrades_when_calendar_pacing_is_denied(tmp_path):
    now = datetime(2026, 8, 4, 15, 10, 29, tzinfo=timezone.utc)  # 11:10 ET
    calendar = replace(
        _calendar(now),
        status=CalendarStatus.DEGRADED,
        reason_codes=("CALENDAR_PACING_DENIED",),
        sessions=(),
    )

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(calendar),
            clock=lambda: now,
        )
        result = loop.run_now()
        health = loop.health()

    assert result.duplicate_reason == "CALENDAR_PACING_DENIED"
    assert health["status"] == "DEGRADED"


def test_recovery_refuses_a_calendar_snapshot_stale_at_acquisition_time(tmp_path):
    observed = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    recovered_at = observed.replace(minute=5)
    pipeline = _Pipeline()

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        result = ScanSchedulerService(
            store,
            pipeline,
            pipeline_version="p4",
        ).tick(_calendar(observed), now=recovered_at)

    assert result.status == "NO_TRADE"
    assert result.duplicate_reason == "RECOVERY_EVIDENCE_STALE"
    assert pipeline.calls == []


def test_exact_top10_slot_refuses_stale_calendar_evidence(tmp_path):
    now = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)
    producer = _Top10Producer()

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        result = Top10SchedulerService(
            store,
            producer,
            pipeline_version="top10-v1",
        ).tick(_calendar(now - timedelta(seconds=6)), now=now)

    assert result.status == "NO_TRADE"
    assert result.duplicate_reason == "RECOVERY_EVIDENCE_STALE"
    assert producer.calls == 0


def test_top10_service_routes_only_exact_slots_and_uses_independent_lease(tmp_path):
    morning = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)  # 09:20 ET
    intraday = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)  # 10:00 ET
    producer = _Top10Producer()
    db = tmp_path / "runs.sqlite"

    with ScanRunStore(db) as store:
        service = Top10SchedulerService(
            store,
            producer,
            pipeline_version="top10-v1",
            owner="top10-first",
        )
        first = service.tick(_calendar(morning), now=morning)
        duplicate = service.tick(_calendar(morning), now=morning)
        other_slot = service.tick(_calendar(intraday), now=intraday)

    with ScanRunStore(db) as restarted_store:
        restarted = Top10SchedulerService(
            restarted_store,
            producer,
            pipeline_version="top10-v1",
            owner="top10-restarted",
        ).tick(_calendar(morning), now=morning)

    assert first.status == "COMPLETED"
    assert first.producer_status == "PREMARKET_FROZEN"
    assert producer.calls == 1
    assert duplicate.duplicate_reason == "SLOT_ALREADY_COMPLETED"
    assert duplicate.producer_status == "PREMARKET_FROZEN"
    assert duplicate.producer_reason_codes == ()
    assert duplicate.producer_evidence_hash
    assert restarted.duplicate_reason == "SLOT_ALREADY_COMPLETED"
    assert restarted.producer_status == "PREMARKET_FROZEN"
    assert restarted.producer_reason_codes == ()
    assert restarted.producer_evidence_hash == duplicate.producer_evidence_hash
    assert other_slot.duplicate_reason == "NO_TOP10_SLOT_DUE"
    assert other_slot.producer_status == "PREMARKET_FROZEN"
    assert other_slot.producer_evidence_hash == duplicate.producer_evidence_hash


def test_top10_scheduler_health_preserves_structured_missing_symbols(tmp_path):
    now = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)

    class Producer:
        def tick(self, *, scheduled_for):
            assert scheduled_for == now
            return {
                "status": "NO_TRADE",
                "reason_codes": (
                    "TOP10_ELIGIBLE_COUNT_SHORTFALL",
                    "UNDERLYING_QUOTE_BATCH_INCOMPLETE",
                ),
                "missing_symbols": ("AAPL", "MSFT"),
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        service = Top10SchedulerService(
            store,
            Producer(),
            pipeline_version="top10-v1",
        )
        result = service.tick(_calendar(now), now=now)
        health = ScanSchedulerLoop(
            ScanSchedulerService(
                store,
                _Pipeline(),
                pipeline_version="ordinary-v1",
            ),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
            top10_service=service,
        )
        health._last_top10_result = result
        top10_health = health.health()["top10_producer"]

    assert result.producer_missing_symbols == ("AAPL", "MSFT")
    assert top10_health["last_missing_symbols"] == ("AAPL", "MSFT")


def test_open_parent_miss_binds_same_day_durable_freeze_failure(tmp_path):
    morning = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)  # 09:20 ET
    opened = datetime(2026, 8, 4, 13, 35, tzinfo=timezone.utc)  # 09:35 ET

    class Producer:
        def tick(self, *, scheduled_for):
            if scheduled_for == morning:
                return {
                    "status": "NO_TRADE",
                    "reason_codes": (
                        "STRUCTURE_SOURCE_INCOMPLETE",
                        "EQUITY_THESIS_EVIDENCE_UNAVAILABLE",
                    ),
                    "missing_symbols": ("GLD", "TLT"),
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                }
            assert scheduled_for == opened
            return {
                "status": "NO_TRADE",
                "reason_codes": ("TODAY_0920_PARENT_MISSING",),
                "missing_symbols": (),
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }

    database = tmp_path / "runs.sqlite"
    with ScanRunStore(database) as store:
        service = Top10SchedulerService(
            store,
            Producer(),
            pipeline_version="top10-v1",
        )
        frozen = service.tick(_calendar(morning), now=morning)
        repriced = service.tick(_calendar(opened), now=opened)

    with ScanRunStore(database) as restarted_store:
        duplicate = Top10SchedulerService(
            restarted_store,
            Producer(),
            pipeline_version="top10-v1",
        ).tick(_calendar(opened), now=opened)

    assert frozen.producer_reason_codes == (
        "STRUCTURE_SOURCE_INCOMPLETE",
        "EQUITY_THESIS_EVIDENCE_UNAVAILABLE",
    )
    assert repriced.producer_reason_codes == (
        "TODAY_0920_PARENT_MISSING",
        "PREMARKET_PARENT_NOT_PRODUCED",
        "STRUCTURE_SOURCE_INCOMPLETE",
        "EQUITY_THESIS_EVIDENCE_UNAVAILABLE",
    )
    assert repriced.producer_missing_symbols == ("GLD", "TLT")
    assert duplicate.duplicate_reason == "SLOT_ALREADY_COMPLETED"
    assert duplicate.producer_reason_codes == repriced.producer_reason_codes
    assert duplicate.producer_missing_symbols == repriced.producer_missing_symbols


def test_top10_service_routes_open_minute_but_not_later_minute(tmp_path):
    exact = datetime(2026, 8, 4, 13, 35, tzinfo=timezone.utc)  # 09:35 ET
    heartbeat = exact.replace(second=30)
    late = exact.replace(minute=36)
    producer = _Top10Producer()

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        service = Top10SchedulerService(
            store,
            producer,
            pipeline_version="top10-v1",
        )
        heartbeat_result = service.tick(_calendar(heartbeat), now=heartbeat)
        duplicate = service.tick(_calendar(exact), now=exact)
        late_result = service.tick(_calendar(late), now=late)

    assert heartbeat_result.status == "COMPLETED"
    assert duplicate.duplicate_reason == "SLOT_ALREADY_COMPLETED"
    assert late_result.duplicate_reason == "NO_TOP10_SLOT_DUE"
    assert producer.calls == 1


def test_open_position_refreshes_research_once_without_entering_action_pool(
    tmp_path,
) -> None:
    now = datetime(2026, 8, 4, 13, 35, 30, tzinfo=timezone.utc)
    refreshes: list[datetime] = []

    class PositionProducer:
        def tick(self, *, scheduled_for):
            return {
                "status": "POSITION_MANAGEMENT_ONLY",
                "reason_codes": ("DERIVATIVE_POSITION_PRESENT",),
                "slot": "OPEN_REPRICE_0935",
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(
                store,
                _Pipeline(),
                pipeline_version="ordinary-v1",
            ),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
            top10_service=Top10SchedulerService(
                store,
                PositionProducer(),
                pipeline_version="top10-v1",
            ),
        )
        loop.bind_daily_callbacks(
            position_research=lambda: refreshes.append(now) or {
                "status": "DEGRADED",
                "phase": "INTRADAY_RECOVERY",
                "available_count": 7,
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }
        )

        loop.tick_once()
        loop.tick_once()
        research = loop.health()["daily_operations"]["position_research"]

    assert refreshes == [now]
    assert research["phase"] == "INTRADAY_RECOVERY"
    assert research["available_count"] == 7
    assert research["decision_authority"] == "SUPPORTING_ONLY"
    assert research["action_pool_count"] == 0


def test_blocked_position_research_does_not_starve_heartbeat(tmp_path):
    current = [datetime(2026, 8, 4, 13, 35, 30, tzinfo=timezone.utc)]
    entered = threading.Event()
    release = threading.Event()

    class PositionProducer:
        def tick(self, *, scheduled_for):
            return {
                "status": "POSITION_MANAGEMENT_ONLY",
                "reason_codes": ("DERIVATIVE_POSITION_PRESENT",),
                "slot": "OPEN_REPRICE_0935",
            }

    def blocked_research():
        entered.set()
        release.wait(timeout=2)
        return {"status": "COMPLETED"}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
            top10_service=Top10SchedulerService(
                store,
                PositionProducer(),
                pipeline_version="top10-v1",
            ),
        )
        loop.bind_daily_callbacks(position_research=blocked_research)
        started = time.monotonic()
        loop.tick_once()
        assert time.monotonic() - started < NONBLOCKING_CALLBACK_ASSERTION_SECONDS
        assert entered.wait(timeout=0.5)

        current[0] += timedelta(seconds=61)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()
        research = loop.health()["daily_operations"]["position_research"]

        assert loop.health()["last_tick_at"] == current[0].isoformat()
        assert research["status"] == "FAILED"
        assert research["reason_codes"] == ("POSITION_RESEARCH_TIMEOUT",)
        release.set()


def test_top10_service_passes_canonical_slot_without_overwriting_observation_clock(
    tmp_path,
):
    heartbeat = datetime(2026, 8, 4, 13, 35, 30, tzinfo=timezone.utc)
    observation_clock = lambda: heartbeat

    class Producer:
        observed = []

        def tick(self, *, scheduled_for):
            self.observed.append((scheduled_for, observation_clock()))
            return {
                "status": "OPEN_REPRICED",
                "reason_codes": (),
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }

    producer = Producer()
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        result = Top10SchedulerService(
            store,
            producer,
            pipeline_version="top10-v1",
        ).tick(_calendar(heartbeat), now=heartbeat)

    assert result.status == "COMPLETED"
    assert producer.observed == [
        (heartbeat.replace(second=0), heartbeat),
    ]


def test_top10_producer_result_can_never_grant_approval_or_order_authority(
    tmp_path,
):
    now = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)

    class InvalidProducer:
        calls = 0

        def tick(self, *, scheduled_for):
            self.calls += 1
            return {
                "status": "PREMARKET_FROZEN",
                "reason_codes": (),
                "approval_eligible": True,
                "instruction_creation_allowed": True,
                "order_allowed": True,
            }

    producer = InvalidProducer()
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        service = Top10SchedulerService(
            store,
            producer,
            pipeline_version="top10-v1",
        )
        result = service.tick(_calendar(now), now=now)
        duplicate = service.tick(_calendar(now), now=now)

    assert result.status == "FAILED"
    assert result.duplicate_reason == "TOP10_PRESELECTION_PRODUCER_FAILED"
    assert result.producer_status is None
    assert duplicate.duplicate_reason == "SLOT_ALREADY_FAILED"
    assert duplicate.producer_status == "NO_TRADE"
    assert duplicate.producer_reason_codes == (
        DURABLE_PRODUCER_RESULT_STATUS_UNAVAILABLE,
    )
    assert producer.calls == 1


class _CalendarProvider:
    def __init__(self, calendar):
        self.calendar = calendar
        self.calls = []

    def snapshot(self, *, now):
        self.calls.append(now)
        return self.calendar


def test_background_loop_tick_is_calendar_gated_and_exposes_health(tmp_path):
    now = datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)
    pipeline = _Pipeline()
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        service = ScanSchedulerService(
            store, pipeline, pipeline_version="p4", owner="loop-test"
        )
        calendar_provider = _CalendarProvider(_calendar(now))
        loop = ScanSchedulerLoop(
            service,
            calendar_provider,
            clock=lambda: now,
        )

        result = loop.tick_once()
        health = loop.health()

    assert result.status == "COMPLETED"
    assert calendar_provider.calls == [now]
    assert len(pipeline.calls) == 1
    assert health["status"] == "READY"
    assert health["last_tick_status"] == "COMPLETED"
    daily = health["daily_operations"]
    assert daily["timezone"] == "America/New_York"
    assert daily["calendar_refresh"]["schedule"] == "EVERY_HEARTBEAT"
    assert daily["calendar_refresh"]["status"] == "COMPLETED"
    assert [item["operation"] for item in daily["runs"]] == [
        "RESEARCH_REFRESH",
        "TOP10_FREEZE",
        "TOP10_REPRICE",
        "ORDINARY_SCAN",
        "ORDINARY_SCAN",
        "ORDINARY_SCAN",
        "ORDINARY_SCAN",
        "OUTCOME_PROCESSING",
        "AFTER_HOURS_DISCOVERY",
        "AFTER_HOURS_REPRICE",
        "NEXT_SESSION_PREPARATION",
    ]
    assert [item["status"] for item in daily["runs"][:4]] == [
        "MISSED_NOT_REPLAYED",
        "MISSED_NOT_REPLAYED",
        "MISSED_NOT_REPLAYED",
        "COMPLETED",
    ]
    assert health["review_only"] is True
    assert health["direct_order_submission"] is False


def test_background_loop_backs_off_repeated_degraded_calendar_refreshes(tmp_path):
    current = [datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)]
    degraded = replace(
        _calendar(current[0]),
        status=CalendarStatus.DEGRADED,
        reason_codes=("CALENDAR_CURRENT_DATE_UNCOVERED",),
        sessions=(),
    )
    provider = _CalendarProvider(degraded)
    pipeline = _Pipeline()

    with ScanRunStore(tmp_path / "calendar-backoff.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, pipeline, pipeline_version="p4"),
            provider,
            clock=lambda: current[0],
        )

        first = loop.tick_once()
        current[0] += timedelta(seconds=30)
        backed_off = loop.tick_once()
        health = loop.health()["daily_operations"]["calendar_refresh"]
        current[0] += timedelta(seconds=31)
        retried = loop.tick_once()

    assert first.duplicate_reason == "CALENDAR_CURRENT_DATE_UNCOVERED"
    assert backed_off.duplicate_reason == "CALENDAR_REFRESH_BACKOFF"
    assert retried.duplicate_reason == "CALENDAR_CURRENT_DATE_UNCOVERED"
    assert provider.calls == [
        datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc),
        datetime(2026, 8, 4, 14, 6, 1, tzinfo=timezone.utc),
    ]
    assert pipeline.calls == []
    assert health["schedule"] == "FAILURE_BACKOFF"
    assert health["next_retry_at"] == "2026-08-04T14:06:00+00:00"


def test_cached_summary_refreshes_calendar_retry_without_reauditing_ledgers(tmp_path):
    current = [datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)]
    degraded = replace(
        _calendar(current[0]),
        status=CalendarStatus.DEGRADED,
        reason_codes=("CALENDAR_BROKER_REQUEST_TIMEOUT",),
        sessions=(),
    )
    provider = _CalendarProvider(degraded)
    with ScanRunStore(tmp_path / "summary-calendar-retry.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            provider,
            clock=lambda: current[0],
        )
        loop.tick_once()
        first = loop.summary()["daily_operations"]["calendar_refresh"]

        def refuse_deep_audit():
            raise AssertionError("summary must not repeat durable ledger reads")

        loop._daily_operations_health = refuse_deep_audit
        current[0] += timedelta(seconds=61)
        loop.tick_once()
        retried = loop.summary()["daily_operations"]["calendar_refresh"]

        assert first["last_run_at"] == "2026-08-04T14:05:00+00:00"
        assert retried["last_run_at"] == "2026-08-04T14:06:01+00:00"
        assert retried["next_retry_at"] == "2026-08-04T14:08:01+00:00"
        assert retried["reason_codes"] == ("CALENDAR_BROKER_REQUEST_TIMEOUT",)

        ready = _calendar(current[0])
        loop._reset_calendar_refresh_backoff(ready, current[0])
        recovered = loop.summary()["daily_operations"]["calendar_refresh"]
        assert recovered["status"] == "COMPLETED"
        assert recovered["schedule"] == "EVERY_HEARTBEAT"
        assert recovered["next_retry_at"] is None
        assert recovered["reason_codes"] == ()


@pytest.mark.parametrize("operation", ["run_now", "tick_once"])
def test_calendar_refresh_summary_pairs_new_snapshot_during_unfinished_scan(
    tmp_path, monkeypatch, operation,
):
    current = [datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)]
    provider = _CalendarProvider(_calendar(current[0]))
    pipeline = _Pipeline()
    with ScanRunStore(tmp_path / "calendar-inflight.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, pipeline, pipeline_version="p4"),
            provider,
            clock=lambda: current[0],
        )
        assert loop.tick_once().status == "COMPLETED"
        previous = loop.summary()
        current[0] = current[0].replace(hour=15, minute=30)
        provider.calendar = _calendar(current[0])
        during_scan = []
        run_slot = pipeline.run_slot

        def capture_before_scan_completes(scan_run_id, slot_at):
            during_scan.append(loop.summary())
            return run_slot(scan_run_id, slot_at)

        monkeypatch.setattr(pipeline, "run_slot", capture_before_scan_completes)
        assert getattr(loop, operation)().status == "COMPLETED"
        assert len(during_scan) == 1
        # The scan has not returned, so its tick timestamp still belongs to
        # the previous cycle; calendar diagnostics must use the new attempt.
        assert during_scan[0]["last_tick_at"] == previous["last_tick_at"]
        calendar = during_scan[0]["daily_operations"]["calendar_refresh"]
        assert calendar["last_run_at"] == current[0].isoformat()
        assert calendar["status"] == "COMPLETED"
        assert calendar["reason_codes"] == ()
        assert loop.summary()["daily_operations"]["calendar_refresh"] == calendar
        assert len(pipeline.calls) == 2


@pytest.mark.parametrize(
    ("observation_offset_seconds", "reason"),
    [(-6, "CALENDAR_STALE"), (1, "CALENDAR_OBSERVED_IN_FUTURE")],
)
def test_calendar_refresh_summary_explains_invalid_observation_time(
    tmp_path, observation_offset_seconds, reason,
):
    now = datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)
    calendar = replace(
        _calendar(now),
        observed_at=now + timedelta(seconds=observation_offset_seconds),
    )
    pipeline = _Pipeline()
    with ScanRunStore(tmp_path / "calendar-time-invalid.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, pipeline, pipeline_version="p4"),
            _CalendarProvider(calendar),
            clock=lambda: now,
        )
        result = loop.run_now()
        diagnostic = loop.summary()["daily_operations"]["calendar_refresh"]

        assert result.status == "NO_TRADE"
        assert result.duplicate_reason == "RECOVERY_EVIDENCE_STALE"
        assert pipeline.calls == []
        assert diagnostic["status"] == "DEGRADED"
        assert diagnostic["reason_codes"] == (reason,)
        assert diagnostic["last_run_at"] == now.isoformat()


def test_weekend_health_projects_closed_next_session_without_manifest(tmp_path):
    now = datetime(2026, 8, 22, 14, 0, tzinfo=timezone.utc)
    calendar = _calendar(
        now,
        hours="20260822:CLOSED;20260824:0930-1600",
    )

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(calendar),
            clock=lambda: now,
        )

        result = loop.tick_once()
        daily = loop.health()["daily_operations"]

        assert result.duplicate_reason == "MARKET_CLOSED_PENDING_NEXT_OPEN"
        assert result.status == "NO_TRADE"
        assert daily["today"] == {
            "trading_date": "2026-08-22",
            "market_status": "CLOSED",
            "next_trading_date": "2026-08-24",
            "runs": (),
        }
        assert daily["runs"] == ()
        assert daily["day_manifest"] is None
        assert store.latest_daily_manifest(trading_date=now.date()) is None


def test_unchanged_heartbeat_and_restart_do_not_append_duplicate_daily_manifests(
    tmp_path,
):
    path = tmp_path / "runs.sqlite"
    current = [datetime(2026, 8, 4, 11, 0, tzinfo=timezone.utc)]
    calendar = _calendar(current[0])

    with ScanRunStore(path) as store:
        first = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(calendar),
            clock=lambda: current[0],
        )
        first.tick_once()
        current[0] += timedelta(seconds=30)
        first.tick_once()

        restarted = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(calendar),
            clock=lambda: current[0],
        )
        current[0] += timedelta(seconds=30)
        restarted.tick_once()

    connection = sqlite3.connect(path)
    try:
        count = connection.execute(
            "SELECT COUNT(*) FROM daily_operation_manifests"
        ).fetchone()[0]
    finally:
        connection.close()

    assert count == 1


def test_material_daily_state_change_appends_a_new_manifest(tmp_path):
    path = tmp_path / "runs.sqlite"
    current = [datetime(2026, 8, 4, 13, 0, tzinfo=timezone.utc)]
    calendar = _calendar(current[0])

    with ScanRunStore(path) as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(calendar),
            clock=lambda: current[0],
        )
        loop.tick_once()
        current[0] = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
        loop.tick_once()

    connection = sqlite3.connect(path)
    try:
        manifests = connection.execute(
            "SELECT payload_json FROM daily_operation_manifests ORDER BY sequence"
        ).fetchall()
    finally:
        connection.close()

    assert len(manifests) == 2
    first_runs = json.loads(manifests[0][0])["runs"]
    second_runs = json.loads(manifests[1][0])["runs"]
    assert first_runs != second_runs


def test_0830_research_refresh_is_durable_and_never_runs_twice(tmp_path):
    now = datetime(2026, 8, 4, 12, 30, 30, tzinfo=timezone.utc)
    refreshes: list[tuple[object, datetime, datetime]] = []
    calendar = _calendar(now)

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(calendar),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(
            research_refresh=lambda snapshot, scheduled_for, checked_at: (
                refreshes.append((snapshot, scheduled_for, checked_at))
                or {"status": "READY"}
            ),
        )

        loop.tick_once()
        loop.tick_once()
        research = loop.health()["daily_operations"]["runs"][0]

    assert refreshes == [
        (
            calendar,
            datetime(2026, 8, 4, 8, 30, tzinfo=calendar.sessions[0].open_et.tzinfo),
            now,
        )
    ]
    assert research["operation"] == "RESEARCH_REFRESH"
    assert research["status"] == "COMPLETED"
    assert research["handler_status"] == "READY"
    assert research["scan_run_id"]


def test_1615_outcome_processing_is_ready_durable_and_observable(tmp_path):
    now = datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)
    calls: list[datetime] = []
    payload = {
        "status": "DEGRADED",
        "checked_at": now.isoformat(),
        "due_count": 4,
        "records_appended": 3,
        "records_superseded": 1,
        "records_skipped": 2,
        "records_blocked": 5,
        "records_rejected": 1,
        "reason_codes": ("OUTCOME_OBSERVATION_CONFLICTED",),
        "candidate_ledger_head_hash": "a" * 64,
        "shadow_ledger_head_hash": "b" * 64,
        "manifest_hash": "c" * 64,
        "processing_hash": "d" * 64,
        "shadow_evaluation_refresh": {
            "status": "DEGRADED",
            "reason": "SHADOW_EVALUATION_REFRESH_FAILED",
            "decision_authority": "SUPPORTING_ONLY",
            "persisted": False,
        },
    }

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(
            outcome_process=lambda: calls.append(now) or payload,
        )

        loop.tick_once()
        loop.tick_once()
        health = loop.health()["daily_operations"]
        outcome_run = next(
            item
            for item in health["runs"]
            if item["operation"] == "OUTCOME_PROCESSING"
        )

    assert calls == [now]
    assert outcome_run["status"] == "COMPLETED"
    assert outcome_run["handler_status"] == "READY"
    assert outcome_run["scan_run_id"]
    assert health["outcome_processing"] == {
        **payload,
        "schema": "options_copilot.daily_operation_result.v1",
        "operation": "OUTCOME_PROCESSING",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def test_discovery_failure_reason_is_visible_after_restart(tmp_path):
    path = tmp_path / "runs.sqlite"
    now = datetime(2026, 8, 4, 20, 20, 30, tzinfo=timezone.utc)
    with ScanRunStore(path) as store:
        first = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
        )
        first.bind_daily_callbacks(
            after_hours_discovery=lambda: (_ for _ in ()).throw(
                RuntimeError("provider failed")
            )
        )
        first.tick_once()

    restarted_at = now + timedelta(minutes=1)
    with ScanRunStore(path) as reopened:
        restarted = ScanSchedulerLoop(
            ScanSchedulerService(reopened, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(restarted_at)),
            clock=lambda: restarted_at,
        )
        restarted.bind_daily_callbacks(
            after_hours_discovery=lambda: pytest.fail("must not replay")
        )
        restarted.tick_once()
        discovery = next(
            item
            for item in restarted.health()["daily_operations"]["runs"]
            if item["operation"] == "AFTER_HOURS_DISCOVERY"
        )

        assert discovery["status"] == "FAILED"
        assert discovery["reason_codes"] == (
            "AFTER_HOURS_DISCOVERY_FAILED",
        )
        assert discovery["recorded_at"]


def test_blocked_outcome_does_not_starve_heartbeat_or_after_hours(tmp_path):
    current = [datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)]
    entered = threading.Event()
    release = threading.Event()
    discovery_calls: list[datetime] = []
    reprice_calls: list[datetime] = []

    def blocked_outcome():
        entered.set()
        release.wait(timeout=2)
        return {"status": "COMPLETED"}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
        )
        loop.bind_daily_callbacks(
            outcome_process=blocked_outcome,
            after_hours_discovery=lambda: discovery_calls.append(current[0]) or {
                "status": "COMPLETED"
            },
            after_hours_reprice=lambda: reprice_calls.append(current[0]) or {
                "status": "COMPLETED",
                "priced_count": 0,
                "requested_count": 0,
            },
        )
        started = time.monotonic()
        loop.tick_once()
        assert time.monotonic() - started < NONBLOCKING_CALLBACK_ASSERTION_SECONDS
        assert entered.wait(timeout=0.5)

        current[0] = datetime(2026, 8, 4, 20, 16, 31, tzinfo=timezone.utc)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()
        current[0] = datetime(2026, 8, 4, 20, 20, 30, tzinfo=timezone.utc)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()
        current[0] = datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()

        assert discovery_calls
        assert reprice_calls
        assert loop.health()["last_tick_at"] == current[0].isoformat()
        outcome = next(
            item
            for item in loop.health()["daily_operations"]["runs"]
            if item["operation"] == "OUTCOME_PROCESSING"
        )
        assert outcome["status"] == "FAILED"
        release.set()


def test_timed_out_daily_worker_receives_cancel_and_cannot_publish_late_payload(
    tmp_path,
) -> None:
    current = [datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)]
    entered = threading.Event()
    exited = threading.Event()
    late_writes: list[str] = []

    def cooperative_outcome(*, cancel_event, deadline_at):
        assert deadline_at == current[0] + timedelta(seconds=60)
        entered.set()
        cancel_event.wait(timeout=2)
        if not cancel_event.is_set():
            late_writes.append("outcome")
        exited.set()
        return {"status": "COMPLETED", "candidate_outcomes": 1}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
        )
        loop.bind_daily_callbacks(outcome_process=cooperative_outcome)
        loop.tick_once()
        assert entered.wait(timeout=1)

        current[0] += timedelta(seconds=61)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()
        assert exited.wait(timeout=1)
        durable = store.latest_daily_result("OUTCOME_PROCESSING")

    assert late_writes == []
    assert durable is not None
    assert durable.status == "FAILED"
    assert durable.payload["reason_codes"] == ["OUTCOME_PROCESSING_TIMEOUT"]


def test_expired_lease_is_terminal_and_old_owner_cannot_complete(tmp_path):
    now = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    slot = slots_for_session(_calendar(now), now.date())[0]
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        first = store.acquire(
            slot,
            pipeline_version="p4",
            owner="old-owner",
            now=now,
            lease_seconds=1,
        )
        expired = store.acquire(
            slot,
            pipeline_version="p4",
            owner="new-owner",
            now=now + timedelta(seconds=2),
        )
        assert expired.acquired is False
        assert expired.duplicate_reason == "SLOT_LEASE_EXPIRED"
        assert expired.run.status == "FAILED"
        assert expired.run.failure_reason == "LEASE_EXPIRED"
        with pytest.raises(RuntimeError):
            store.complete(
                first.run.scan_run_id,
                owner="old-owner",
                result_hash="late",
                now=now + timedelta(seconds=2),
            )


def test_expired_daily_lease_persists_failure_result_across_restart(tmp_path):
    path = tmp_path / "runs.sqlite"
    now = datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)
    slot = ScanSlot(now.date(), now, "OUTCOME_PROCESSING")
    with ScanRunStore(path) as store:
        claim = store.acquire(
            slot,
            pipeline_version=DAILY_OPERATION_PIPELINES["OUTCOME_PROCESSING"],
            owner="crashed-owner",
            now=now,
            lease_seconds=1,
        )
        assert claim.acquired

    with ScanRunStore(path) as restarted:
        expired = restarted.expire_leases(now=now + timedelta(seconds=2))
        durable = restarted.latest_daily_result("OUTCOME_PROCESSING")

        assert [run.status for run in expired] == ["FAILED"]
        assert durable is not None
        assert durable.status == "FAILED"
        assert durable.payload["reason_codes"] == ["LEASE_EXPIRED"]
        assert durable.result_hash == expired[0].result_hash


def test_expired_daily_lease_collision_rolls_back_instead_of_hiding_corruption(
    tmp_path,
):
    now = datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)
    slot = ScanSlot(now.date(), now, "OUTCOME_PROCESSING")
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        claim = store.acquire(
            slot,
            pipeline_version=DAILY_OPERATION_PIPELINES["OUTCOME_PROCESSING"],
            owner="crashed-owner",
            now=now,
            lease_seconds=1,
        )
        store._connection.execute(
            "INSERT INTO daily_operation_results VALUES (?,?,?,?,?,?)",
            (
                claim.run.scan_run_id,
                "OUTCOME_PROCESSING",
                "FAILED",
                "{}",
                "conflicting-hash",
                now.isoformat(),
            ),
        )

        with pytest.raises(sqlite3.IntegrityError):
            store.expire_leases(now=now + timedelta(seconds=2))

        assert store.get(claim.run.scan_run_id).status == "LEASED"


def test_daily_terminalization_retries_once_and_clears_error(tmp_path, monkeypatch):
    current = [datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)]
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
        )
        loop.bind_daily_callbacks(outcome_process=lambda: {"status": "COMPLETED"})
        original = store.complete_with_daily_result
        calls = [0]

        def fail_once(*args, **kwargs):
            calls[0] += 1
            if calls[0] == 1:
                raise RuntimeError("transient terminalization failure")
            return original(*args, **kwargs)

        monkeypatch.setattr(store, "complete_with_daily_result", fail_once)
        loop.tick_once()
        first = next(
            item
            for item in loop.health()["daily_operations"]["runs"]
            if item["operation"] == "OUTCOME_PROCESSING"
        )
        assert first["status"] == "LEASED"
        assert first["terminalization_error"] == "RuntimeError"
        assert loop.health()["status"] == "DEGRADED"

        current[0] += timedelta(seconds=1)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()
        second = next(
            item
            for item in loop.health()["daily_operations"]["runs"]
            if item["operation"] == "OUTCOME_PROCESSING"
        )
        assert second["status"] == "COMPLETED"
        assert "terminalization_error" not in second
        assert loop._daily_jobs == {}


def test_daily_terminalization_reconciles_expired_durable_failure(
    tmp_path,
    monkeypatch,
):
    current = [datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)]
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
        )
        loop.bind_daily_callbacks(outcome_process=lambda: {"status": "COMPLETED"})
        monkeypatch.setattr(
            store,
            "complete_with_daily_result",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("failed")),
        )
        loop.tick_once()
        first_health = loop.health()
        assert first_health["status"] == "DEGRADED"
        assert len(loop._daily_jobs) == 1

        current[0] += timedelta(seconds=121)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()
        durable = store.latest_daily_result("OUTCOME_PROCESSING")

        assert durable is not None
        assert durable.payload["reason_codes"] == ["LEASE_EXPIRED"]
        assert loop._daily_jobs == {}
        reconciled_health = loop.health()
        assert reconciled_health["status"] == "DEGRADED"
        assert reconciled_health["daily_operations"]["outcome_processing"][
            "terminalization_error"
        ] == "RuntimeError"


def test_daily_result_and_manifest_survive_restart_and_detect_timestamp_tamper(
    tmp_path,
):
    path = tmp_path / "runs.sqlite"
    now = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    with ScanRunStore(path) as store:
        slot = ScanSlot(now.date(), now.replace(second=0), "NEXT_SESSION_PREPARATION")
        claim = store.acquire(
            slot,
            pipeline_version="daily-next-session-preparation-v1",
            owner="next-session",
            now=now,
        )
        store.complete_with_daily_result(
            claim.run.scan_run_id,
            owner="next-session",
            operation="NEXT_SESSION_PREPARATION",
            payload={
                "schema": "options_copilot.daily_operation_result.v1",
                "operation": "NEXT_SESSION_PREPARATION",
                "status": "READY",
                "next_trading_date": "2026-08-05",
            },
            now=now,
        )
        first = store.record_daily_manifest(
            trading_date=now.date(),
            payload={"runs": [{"status": "COMPLETED"}], "revision": 1},
            now=now,
        )
        second = store.record_daily_manifest(
            trading_date=now.date(),
            payload={"runs": [{"status": "COMPLETED"}], "revision": 2},
            now=now,
        )
        assert (first.sequence, second.sequence) == (1, 2)
        assert second.previous_hash == first.manifest_hash

    with ScanRunStore(path) as reopened:
        assert reopened.latest_daily_result("NEXT_SESSION_PREPARATION").payload[
            "next_trading_date"
        ] == "2026-08-05"
        assert reopened.latest_daily_manifest(trading_date=now.date()).sequence == 2

    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER daily_operation_results_no_update")
    connection.execute(
        "UPDATE daily_operation_results SET recorded_at=?",
        ((now + timedelta(days=1)).isoformat(),),
    )
    connection.commit()
    connection.close()
    with ScanRunStore(path) as tampered:
        with pytest.raises(ValueError, match="integrity"):
            tampered.latest_daily_result("NEXT_SESSION_PREPARATION")


def test_daily_manifest_timestamp_tamper_breaks_hash_and_chain(tmp_path):
    path = tmp_path / "runs.sqlite"
    now = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    with ScanRunStore(path) as store:
        store.record_daily_manifest(
            trading_date=now.date(),
            payload={"runs": [{"status": "COMPLETED"}], "revision": 1},
            now=now,
        )
        store.record_daily_manifest(
            trading_date=now.date(),
            payload={"runs": [{"status": "COMPLETED"}], "revision": 2},
            now=now,
        )

    connection = sqlite3.connect(path)
    connection.execute("DROP TRIGGER daily_operation_manifests_no_update")
    connection.execute(
        "UPDATE daily_operation_manifests SET recorded_at=? WHERE sequence=1",
        ((now + timedelta(hours=1)).isoformat(),),
    )
    connection.commit()
    connection.close()
    with ScanRunStore(path) as tampered:
        with pytest.raises(ValueError, match="integrity"):
            tampered.assert_daily_integrity(trading_date=now.date())


def test_incremental_manifest_verification_rejects_a_new_invalid_tail(tmp_path):
    path = tmp_path / "runs.sqlite"
    now = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    with ScanRunStore(path) as store:
        store.record_daily_manifest(
            trading_date=now.date(),
            payload={"runs": [{"status": "COMPLETED"}], "revision": 1},
            now=now,
        )
        store.assert_daily_integrity(trading_date=now.date())

        connection = sqlite3.connect(path)
        try:
            connection.execute(
                "INSERT INTO daily_operation_manifests VALUES (?,?,?,?,?,?,?)",
                (
                    2,
                    "daily-manifest.invalid-tail",
                    now.date().isoformat(),
                    '{"revision":2,"runs":[{"status":"COMPLETED"}]}',
                    "f" * 64,
                    "e" * 64,
                    (now + timedelta(seconds=30)).isoformat(),
                ),
            )
            connection.commit()
        finally:
            connection.close()

        with pytest.raises(ValueError, match="chain is invalid"):
            store.assert_daily_integrity(trading_date=now.date())


def test_1630_after_hours_reprice_is_durable_and_schedules_gap_retry(tmp_path):
    now = datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)
    calls: list[datetime] = []

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(
            after_hours_reprice=lambda: calls.append(now) or {
                "status": "DEGRADED",
                "priced_count": 4,
                "requested_count": 10,
                "reason_codes": ["AFTER_HOURS_REPRICE_CONTINUES_NEXT_WINDOW"],
                "decision": "NO_TRADE",
                "decision_authority": "SUPPORTING_ONLY",
            },
        )

        loop.tick_once()
        loop.tick_once()
        health = loop.health()["daily_operations"]
        run = next(
            item for item in health["runs"]
            if item["operation"] == "AFTER_HOURS_REPRICE"
        )

    assert calls == [now]
    assert run["status"] == "COMPLETED"
    assert health["after_hours_reprice"]["priced_count"] == 4
    assert health["after_hours_reprice"]["next_retry_at"] is not None
    assert health["after_hours_reprice"]["decision"] == "NO_TRADE"


def test_after_hours_complete_prices_still_retry_missing_formal_basis(tmp_path):
    now = datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(
            after_hours_reprice=lambda: {
                "status": "DEGRADED",
                "priced_count": 10,
                "requested_count": 10,
                "campaign": {
                    "completed_underlyings": 10,
                    "target_underlyings": 10,
                    "remaining_underlyings": 0,
                    "basis_bound_underlyings": 8,
                    "remaining_basis_underlyings": 2,
                },
                "reason_codes": [
                    "AFTER_HOURS_UNDERLYING_BASIS_MIGRATION_PENDING"
                ],
                "decision": "NO_TRADE",
            }
        )

        loop.tick_once()
        result = loop.health()["daily_operations"]["after_hours_reprice"]

    assert result["priced_count"] == 10
    assert result["next_retry_at"] is not None


def test_after_hours_gap_retry_is_async_and_durable(tmp_path):
    current = [datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)]
    calls = [0]

    def reprice():
        calls[0] += 1
        return {
            "status": "DEGRADED" if calls[0] == 1 else "COMPLETED",
            "priced_count": 4 if calls[0] == 1 else 10,
            "requested_count": 10,
            "reason_codes": (
                ("AFTER_HOURS_REPRICE_CONTINUES_NEXT_WINDOW",)
                if calls[0] == 1
                else ()
            ),
            "decision": "NO_TRADE",
        }

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
        )
        loop.bind_daily_callbacks(after_hours_reprice=reprice)
        loop.tick_once()
        current[0] = datetime(2026, 8, 4, 20, 31, 30, tzinfo=timezone.utc)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()

        retry = store.latest_daily_result("AFTER_HOURS_REPRICE_RETRY")
        assert calls[0] == 2
        assert retry is not None
        assert retry.status == "COMPLETED"
        assert retry.payload["priced_count"] == 10
        assert loop.health()["daily_operations"]["after_hours_reprice"][
            "priced_count"
        ] == 10


def test_after_hours_gap_retry_due_is_restored_after_restart(tmp_path):
    path = tmp_path / "runs.sqlite"
    current = datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)
    with ScanRunStore(path) as store:
        first = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current)),
            clock=lambda: current,
        )
        first.bind_daily_callbacks(
            after_hours_reprice=lambda: {
                "status": "DEGRADED",
                "priced_count": 1,
                "requested_count": 2,
                "reason_codes": ["AFTER_HOURS_REPRICE_CONTINUES_NEXT_WINDOW"],
            }
        )
        first.tick_once()
        due = store.latest_daily_result("AFTER_HOURS_REPRICE")
        assert due is not None
        assert due.payload["next_retry_at"]

    restarted_at = current + timedelta(minutes=1)
    calls = [0]
    with ScanRunStore(path) as reopened:
        restarted = ScanSchedulerLoop(
            ScanSchedulerService(reopened, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(restarted_at)),
            clock=lambda: restarted_at,
        )
        restarted.bind_daily_callbacks(
            after_hours_reprice=lambda: calls.__setitem__(0, calls[0] + 1)
            or {
                "status": "COMPLETED",
                "priced_count": 2,
                "requested_count": 2,
            }
        )
        restarted.tick_once()
        retry = reopened.latest_daily_result("AFTER_HOURS_REPRICE_RETRY")

        assert calls == [1]
        assert retry is not None
        assert retry.status == "COMPLETED"


def test_legacy_after_hours_retry_without_budget_is_not_restored(tmp_path):
    path = tmp_path / "runs.sqlite"
    initial = datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)
    with ScanRunStore(path) as store:
        slot = ScanSlot(
            initial.astimezone(US_OPTIONS_TIMEZONE).date(),
            initial.astimezone(US_OPTIONS_TIMEZONE).replace(second=0),
            kind="AFTER_HOURS_REPRICE",
        )
        acquired = store.acquire(
            slot,
            pipeline_version=DAILY_OPERATION_PIPELINES["AFTER_HOURS_REPRICE"],
            owner="legacy-after-hours",
            now=initial,
        )
        store.complete_with_daily_result(
            acquired.run.scan_run_id,
            owner="legacy-after-hours",
            operation="AFTER_HOURS_REPRICE",
            payload={
                "status": "DEGRADED",
                "priced_count": 1,
                "requested_count": 10,
                "next_retry_at": (initial + timedelta(minutes=1)).isoformat(),
                "reason_codes": ["AFTER_HOURS_REPRICE_CONTINUES_NEXT_WINDOW"],
            },
            now=initial,
        )

    restarted_at = initial + timedelta(minutes=1)
    with ScanRunStore(path) as reopened:
        restarted = ScanSchedulerLoop(
            ScanSchedulerService(reopened, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(restarted_at)),
            clock=lambda: restarted_at,
        )
        restarted.bind_daily_callbacks(
            after_hours_reprice=lambda: pytest.fail(
                "legacy retry state without a durable budget must not run"
            )
        )
        restarted.tick_once()
        health = restarted.health()["daily_operations"]["after_hours_reprice"]
        assert health["next_retry_at"] is None
        assert health["retry_attempt"] == 0
        assert health["retry_limit"] == AFTER_HOURS_REPRICE_RETRY_LIMIT
        assert health["retry_pending"] is False


def test_after_hours_gap_retry_budget_is_bounded_and_durable(tmp_path):
    current = [datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)]
    calls = [0]

    def incomplete_reprice():
        calls[0] += 1
        return {
            "status": "DEGRADED",
            "priced_count": 0,
            "requested_count": 10,
            "reason_codes": ["AFTER_HOURS_REPRICE_CONTINUES_NEXT_WINDOW"],
            "decision": "NO_TRADE",
        }

    path = tmp_path / "runs.sqlite"
    with ScanRunStore(path) as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
        )
        loop.bind_daily_callbacks(after_hours_reprice=incomplete_reprice)
        loop.tick_once()
        for _ in range(AFTER_HOURS_REPRICE_RETRY_LIMIT):
            current[0] += timedelta(minutes=1)
            loop.calendar_provider.calendar = _calendar(current[0])
            loop.tick_once()

        exhausted = store.latest_daily_result("AFTER_HOURS_REPRICE_RETRY")
        assert exhausted is not None
        assert exhausted.payload["retry_attempt"] == AFTER_HOURS_REPRICE_RETRY_LIMIT
        assert exhausted.payload["retry_limit"] == AFTER_HOURS_REPRICE_RETRY_LIMIT
        assert exhausted.payload["retry_exhausted"] is True
        assert exhausted.payload["next_retry_at"] is None
        assert (
            "AFTER_HOURS_REPRICE_RETRY_BUDGET_EXHAUSTED"
            in exhausted.payload["reason_codes"]
        )
        current[0] += timedelta(minutes=1)
        loop.calendar_provider.calendar = _calendar(current[0])
        loop.tick_once()
        assert calls[0] == AFTER_HOURS_REPRICE_RETRY_LIMIT + 1

    with ScanRunStore(path) as reopened:
        restarted = ScanSchedulerLoop(
            ScanSchedulerService(reopened, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(current[0])),
            clock=lambda: current[0],
        )
        restarted.bind_daily_callbacks(
            after_hours_reprice=lambda: pytest.fail(
                "an exhausted campaign must not restart"
            )
        )
        current[0] += timedelta(minutes=1)
        restarted.calendar_provider.calendar = _calendar(current[0])
        restarted.tick_once()


def test_long_ordinary_scan_renews_its_durable_lease(tmp_path):
    initial = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    lease_clock = [initial]
    started = threading.Event()
    release = threading.Event()

    class BlockingPipeline:
        def run_slot(self, scan_run_id, slot_at):
            started.set()
            assert release.wait(2)
            return {"scan_run_id": scan_run_id, "slot_at": slot_at.isoformat()}

    path = tmp_path / "runs.sqlite"
    with ScanRunStore(path) as store, ScanRunStore(path) as observer:
        service = ScanSchedulerService(
            store,
            BlockingPipeline(),
            pipeline_version="p4",
            owner="lease-test",
            lease_seconds=1,
            lease_heartbeat_seconds=0.01,
            lease_clock=lambda: lease_clock[0],
        )
        result: list[object] = []
        worker = threading.Thread(
            target=lambda: result.append(
                service.run_now(_calendar(initial), now=initial)
            )
        )
        worker.start()
        assert started.wait(1)
        lease_clock[0] = initial + timedelta(milliseconds=750)
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            runs = observer.runs_for_slot(
                ScanSlot(initial.date(), initial, kind="MANUAL_READ_ONLY_SCAN"),
                pipeline_version="p4",
            )
            if runs and runs[0].lease_expires_at is not None and (
                runs[0].lease_expires_at > initial + timedelta(seconds=1)
            ):
                break
            time.sleep(0.01)
        else:
            pytest.fail("scan lease was not renewed")

        assert observer.expire_leases(
            now=initial + timedelta(milliseconds=1100)
        ) == ()
        release.set()
        worker.join(2)
        assert not worker.is_alive()
        assert result[0].status == "COMPLETED"


def test_scan_lease_heartbeat_does_not_depend_on_primary_store_lock(tmp_path):
    initial = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    lease_clock = [initial]
    started = threading.Event()
    release = threading.Event()

    class BlockingPipeline:
        def run_slot(self, scan_run_id, slot_at):
            started.set()
            assert release.wait(2)
            return {"scan_run_id": scan_run_id, "slot_at": slot_at.isoformat()}

    path = tmp_path / "runs.sqlite"
    with ScanRunStore(path) as store, ScanRunStore(path) as observer:
        service = ScanSchedulerService(
            store,
            BlockingPipeline(),
            pipeline_version="p4",
            owner="independent-heartbeat-test",
            lease_seconds=1,
            lease_heartbeat_seconds=0.01,
            lease_clock=lambda: lease_clock[0],
        )
        result: list[object] = []
        worker = threading.Thread(
            target=lambda: result.append(
                service.run_now(_calendar(initial), now=initial)
            )
        )
        worker.start()
        assert started.wait(1)
        lease_clock[0] = initial + timedelta(milliseconds=750)

        # Simulate another projection holding only the primary store lock.
        # The WAL heartbeat must still renew through its independent writer.
        with store._lock:
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                runs = observer.runs_for_slot(
                    ScanSlot(
                        initial.date(),
                        initial,
                        kind="MANUAL_READ_ONLY_SCAN",
                    ),
                    pipeline_version="p4",
                )
                if runs and runs[0].lease_expires_at is not None and (
                    runs[0].lease_expires_at
                    > initial + timedelta(seconds=1)
                ):
                    break
                time.sleep(0.01)
            else:
                pytest.fail("independent scan lease heartbeat did not renew")

            assert observer.expire_leases(
                now=initial + timedelta(milliseconds=1100)
            ) == ()

        release.set()
        worker.join(2)
        assert not worker.is_alive()
        assert result[0].status == "COMPLETED"


def test_scan_completion_uses_the_actual_lease_clock(tmp_path):
    initial = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)
    completed = initial + timedelta(seconds=2)
    lease_clock = [initial]

    class Pipeline:
        def run_slot(self, scan_run_id, slot_at):
            lease_clock[0] = completed
            return {"scan_run_id": scan_run_id, "slot_at": slot_at.isoformat()}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        result = ScanSchedulerService(
            store,
            Pipeline(),
            pipeline_version="p4",
            lease_clock=lambda: lease_clock[0],
        ).run_now(_calendar(initial), now=initial)
        stored_updated_at = store._connection.execute(
            "SELECT updated_at FROM scan_runs WHERE scan_run_id=?",
            (result.scan_run_id,),
        ).fetchone()[0]

    assert result.status == "COMPLETED"
    assert datetime.fromisoformat(stored_updated_at) == completed


def test_after_hours_gap_retry_missed_on_restart_is_durable(tmp_path):
    path = tmp_path / "runs.sqlite"
    initial = datetime(2026, 8, 4, 20, 30, 30, tzinfo=timezone.utc)
    with ScanRunStore(path) as store:
        first = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(initial)),
            clock=lambda: initial,
        )
        first.bind_daily_callbacks(
            after_hours_reprice=lambda: {
                "status": "DEGRADED",
                "priced_count": 1,
                "requested_count": 2,
                "reason_codes": ["AFTER_HOURS_REPRICE_CONTINUES_NEXT_WINDOW"],
            }
        )
        first.tick_once()

    restarted_at = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)
    with ScanRunStore(path) as reopened:
        restarted = ScanSchedulerLoop(
            ScanSchedulerService(reopened, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(restarted_at, "20260805:0930-1600")),
            clock=lambda: restarted_at,
        )
        restarted.bind_daily_callbacks(
            after_hours_reprice=lambda: pytest.fail("missed retry must not replay")
        )
        restarted.tick_once()
        missed = reopened.latest_daily_result("AFTER_HOURS_REPRICE_RETRY")

        assert missed is not None
        assert missed.status == "FAILED"
        assert missed.payload["reason_codes"] == [
            "AFTER_HOURS_REPRICE_RETRY_MISSED_ON_RESTART"
        ]


def test_1640_next_session_preparation_is_durable_across_restart(tmp_path):
    path = tmp_path / "runs.sqlite"
    now = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    payload = {
        "status": "READY",
        "next_trading_date": "2026-08-05",
        "equity_selected_count": 12,
        "option_structure_count": 8,
        "research_watchlist_count": 6,
        "reason_codes": (),
    }
    with ScanRunStore(path) as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(
            next_session_preparation=lambda _calendar, _slot, _checked: payload,
        )
        loop.tick_once()
        first = loop.health()["daily_operations"]
        first_summary = loop.summary()["daily_operations"]
        assert first["next_session_preparation"]["next_trading_date"] == "2026-08-05"
        assert first["day_manifest"]["manifest_hash"]
        assert first_summary["next_session_preparation"]["next_trading_date"] == (
            "2026-08-05"
        )
        assert first_summary["day_manifest"]["manifest_hash"] == first[
            "day_manifest"
        ]["manifest_hash"]

    later = now + timedelta(minutes=1)
    with ScanRunStore(path) as reopened:
        restarted = ScanSchedulerLoop(
            ScanSchedulerService(reopened, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(later, "20260805:0930-1600")),
            clock=lambda: later,
        )
        restarted.bind_daily_callbacks(
            next_session_preparation=lambda *_args: pytest.fail("must not replay"),
        )
        restarted.tick_once()
        restored = restarted.health()["daily_operations"]
        restarted._last_daily_operations_summary = None
        restored_summary = restarted.summary()["daily_operations"]
        assert restored["next_session_preparation"]["next_trading_date"] == "2026-08-05"
        assert restored["day_manifest"]["manifest_hash"]
        assert restored_summary["next_session_preparation"][
            "next_trading_date"
        ] == "2026-08-05"
        assert restored_summary["day_manifest"]["manifest_hash"] == restored[
            "day_manifest"
        ]["manifest_hash"]


def test_next_session_handoff_reconciles_later_durable_after_hours_formal_pools(
    tmp_path,
) -> None:
    path = tmp_path / "runs.sqlite"
    retry_at = datetime(2026, 8, 4, 20, 39, 30, tzinfo=timezone.utc)
    campaign = {
        "completed_underlyings": 1,
        "target_underlyings": 1,
        "remaining_underlyings": 0,
    }
    candidates = (
        {
            "research_id": "after-hours.xlf",
            "underlying": "XLF",
            "source_scan": "MOST_ACTIVE",
            "legs": (
                {
                    "side": "BUY",
                    "contract_id": 101,
                    "contract_id_ex": "101@SMART",
                    "expiration": "2026-09-04",
                    "strike": "50",
                    "right": "C",
                    "exchange": "SMART",
                    "multiplier": 100,
                },
                {
                    "side": "SELL",
                    "contract_id": 102,
                    "contract_id_ex": "102@SMART",
                    "expiration": "2026-09-04",
                    "strike": "51",
                    "right": "C",
                    "exchange": "SMART",
                    "multiplier": 100,
                },
            ),
        },
    )
    campaign_payload = {"campaign": campaign, "candidates": candidates}
    campaign_hash = _after_hours_campaign_hash(campaign_payload)
    identity_manifest = _after_hours_candidate_identity_manifest(campaign_payload)
    assert identity_manifest is not None
    descriptor = {
        "schema": "options_copilot.after_hours_formal_pool_descriptor.v2",
        "campaign_hash": campaign_hash,
        "equity_pool_hash": "b" * 64,
        "equity_pool_reference_hash": "d" * 64,
        "equity_research_count": 10,
        "equity_selected_count": 6,
        "option_pool_hash": "c" * 64,
        "option_pool_scan_run_id": "after-hours-formal.test",
        "option_structure_count": 1,
        "option_candidate_identity_hash": canonical_hash(identity_manifest),
    }
    formal = {
        "schema": "options_copilot.after_hours_formal_pools.v2",
        "status": "READY",
        "campaign_hash": campaign_hash,
        "equity_pool_hash": "b" * 64,
        "equity_pool_reference_hash": "d" * 64,
        "equity_research_count": 10,
        "option_pool_hash": "c" * 64,
        "option_pool_scan_run_id": "after-hours-formal.test",
        "equity_selected_count": 6,
        "option_structure_count": 1,
        "descriptor": descriptor,
        "descriptor_hash": canonical_hash(descriptor),
    }
    after_hours_payload = {
        "campaign": campaign,
        "candidates": candidates,
        "formal_research_pools": formal,
    }
    assert _valid_formal_descriptor(formal, after_hours_payload)
    assert not _valid_formal_descriptor(
        {**formal, "option_structure_count": 2},
        after_hours_payload,
    )
    tampered_candidate = dict(candidates[0])
    tampered_legs = [dict(item) for item in candidates[0]["legs"]]
    tampered_legs[0]["contract_id"] = 999
    tampered_candidate["legs"] = tuple(tampered_legs)
    assert not _valid_formal_descriptor(
        formal,
        {**after_hours_payload, "candidates": (tampered_candidate,)},
    )
    with ScanRunStore(path) as store:
        claim = store.acquire(
            ScanSlot(
                retry_at.astimezone(US_OPTIONS_TIMEZONE).date(),
                retry_at.astimezone(US_OPTIONS_TIMEZONE).replace(second=0),
                "AFTER_HOURS_REPRICE_RETRY",
            ),
            pipeline_version=DAILY_OPERATION_PIPELINES[
                "AFTER_HOURS_REPRICE_RETRY"
            ],
            owner="test.after-hours-retry",
            now=retry_at,
        )
        store.complete_with_daily_result(
            claim.run.scan_run_id,
            owner="test.after-hours-retry",
            operation="AFTER_HOURS_REPRICE_RETRY",
            payload={
                "status": "COMPLETED",
                "campaign": campaign,
                "candidates": candidates,
                "formal_research_pools": formal,
            },
            now=retry_at,
        )

    prepared_at = retry_at + timedelta(minutes=1)
    with ScanRunStore(path) as reopened:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(reopened, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(prepared_at)),
            clock=lambda: prepared_at,
        )
        loop.bind_daily_callbacks(
            next_session_preparation=lambda *_args: {
                "status": "DEGRADED",
                "next_trading_date": "2026-08-05",
                "equity_selected_count": 0,
                "option_structure_count": 0,
                "premarket_parent_eligible_structure_count": 1,
                "reason_codes": (
                    "EQUITY_POOL_EMPTY_OR_UNAVAILABLE",
                    "OPTION_POOL_EMPTY_OR_UNAVAILABLE",
                ),
            }
        )
        loop.tick_once()
        reconciled = loop.health()["daily_operations"][
            "next_session_preparation"
        ]

    assert reconciled["status"] == "READY"
    assert reconciled["equity_research_count"] == 10
    assert reconciled["equity_selected_count"] == 6
    assert reconciled["option_research_structure_count"] == 1
    assert reconciled["option_structure_count"] == 1
    assert reconciled["premarket_parent_eligible_structure_count"] == 1
    assert reconciled["executable_count"] == 0
    assert reconciled["equity_pool_hash"] == "b" * 64
    assert reconciled["option_pool_hash"] == "c" * 64
    assert reconciled["reason_codes"] == ()
    assert reconciled["reconciled_from_durable_after_hours"] is True


def _formal_after_hours_payload(
    *,
    equity_research_count: int,
    equity_selected_count: int,
    option_research_count: int,
    raw_candidate_count: int | None = None,
    campaign_target_count: int | None = None,
    reason_codes: tuple[str, ...] = (),
) -> dict[str, object]:
    raw_count = (
        option_research_count
        if raw_candidate_count is None
        else raw_candidate_count
    )
    candidates = tuple(
        {
            "research_id": f"after-hours.test-{index}",
            "underlying": f"T{index}",
            "strategy_type": "BULL_CALL_VERTICAL",
            "source_scan": "TEST",
            "legs": (
                {
                    "side": "BUY",
                    "contract_id": 10_000 + (index * 2),
                    "contract_id_ex": f"{10_000 + (index * 2)}@SMART",
                    "expiration": "2026-09-04",
                    "strike": "100",
                    "right": "C",
                    "exchange": "SMART",
                    "multiplier": 100,
                    "ratio": 1,
                },
                {
                    "side": "SELL",
                    "contract_id": 10_001 + (index * 2),
                    "contract_id_ex": f"{10_001 + (index * 2)}@SMART",
                    "expiration": "2026-09-04",
                    "strike": "101",
                    "right": "C",
                    "exchange": "SMART",
                    "multiplier": 100,
                    "ratio": 1,
                },
            ),
        }
        for index in range(raw_count)
    )
    target_count = (
        equity_research_count
        if campaign_target_count is None
        else campaign_target_count
    )
    campaign = {
        "completed_underlyings": equity_research_count,
        "target_underlyings": target_count,
        "remaining_underlyings": target_count - equity_research_count,
    }
    payload: dict[str, object] = {
        "campaign": campaign,
        "candidates": candidates,
        "reason_codes": reason_codes,
    }
    campaign_hash = _after_hours_campaign_hash(payload)
    identity_manifest = tuple(
        option_candidate_identity(
            {
                "symbol": candidate["underlying"],
                "structure": "DEBIT_VERTICAL",
                "legs": tuple(
                    {
                        "con_id": leg["contract_id"],
                        "contract_id_ex": leg["contract_id_ex"],
                        "expiration": leg["expiration"],
                        "strike": leg["strike"],
                        "right": "CALL" if leg["right"] == "C" else "PUT",
                        "side": leg["side"],
                        "ratio": 1,
                        "multiplier": leg["multiplier"],
                        "exchange": leg["exchange"],
                    }
                    for leg in candidate["legs"]
                ),
            }
        )
        for candidate in candidates[:option_research_count]
    )
    campaign_observed_at = "2026-08-04T20:30:00+00:00"
    materialized_at = "2026-08-04T20:30:00.000001+00:00"
    materialization_revision_hash = "e" * 64
    descriptor = {
        "schema": "options_copilot.after_hours_formal_pool_descriptor.v2",
        "campaign_hash": campaign_hash,
        "materialization_revision_hash": materialization_revision_hash,
        "campaign_observed_at": campaign_observed_at,
        "materialized_at": materialized_at,
        "equity_pool_hash": "b" * 64,
        "equity_pool_reference_hash": "d" * 64,
        "equity_research_count": equity_research_count,
        "equity_selected_count": equity_selected_count,
        "option_pool_hash": "c" * 64,
        "option_pool_scan_run_id": "after-hours-formal.test",
        "option_structure_count": option_research_count,
        "option_candidate_identity_hash": canonical_hash(identity_manifest),
    }
    formal = {
        "schema": "options_copilot.after_hours_formal_pools.v2",
        "status": "READY",
        "campaign_hash": campaign_hash,
        "materialization_revision_hash": materialization_revision_hash,
        "campaign_observed_at": campaign_observed_at,
        "materialized_at": materialized_at,
        "equity_pool_hash": "b" * 64,
        "equity_pool_reference_hash": "d" * 64,
        "equity_research_count": equity_research_count,
        "equity_selected_count": equity_selected_count,
        "option_pool_hash": "c" * 64,
        "option_pool_scan_run_id": "after-hours-formal.test",
        "option_structure_count": option_research_count,
        "descriptor": descriptor,
        "descriptor_hash": canonical_hash(descriptor),
    }
    payload["formal_research_pools"] = formal
    return payload


def test_scanner_campaign_hash_matches_runtime_v2_lineage() -> None:
    payload = _formal_after_hours_payload(
        equity_research_count=2,
        equity_selected_count=2,
        option_research_count=2,
    )
    candidates = payload["candidates"]
    assert isinstance(candidates, tuple)
    expected = canonical_hash(
        {
            "schema": "options_copilot.after_hours_formal_pool_lineage.v2",
            "campaign": payload["campaign"],
            "candidates": tuple(
                {
                    "research_id": row["research_id"],
                    "underlying": row["underlying"],
                    "strategy_type": row["strategy_type"],
                    "source_scan": row["source_scan"],
                    "underlying_quote_basis_hash": row.get(
                        "underlying_quote_basis_hash"
                    ),
                    "legs": tuple(
                        (
                            leg["contract_id"],
                            leg["expiration"],
                            leg["strike"],
                            leg["right"],
                            leg["side"],
                            leg["ratio"],
                        )
                        for leg in row["legs"]
                    ),
                }
                for row in candidates
            ),
        }
    )

    assert after_hours_campaign_lineage_hash(payload) == expected
    assert _runtime_after_hours_campaign_hash(payload) == expected
    assert _after_hours_campaign_hash(payload) == expected
    formal = payload["formal_research_pools"]
    assert isinstance(formal, Mapping)
    assert _valid_formal_descriptor(formal, payload)


@pytest.mark.parametrize(
    ("strategy_type", "structure", "ratios", "keep_leg_count"),
    (
        ("BULL_PUT_CREDIT_VERTICAL", "CREDIT_VERTICAL", (2, 1), 2),
        ("LONG_CALL", "LONG_OPTION", (3, 1), 1),
    ),
)
def test_scanner_descriptor_preserves_runtime_strategy_and_leg_ratios(
    strategy_type,
    structure,
    ratios,
    keep_leg_count,
) -> None:
    payload = _formal_after_hours_payload(
        equity_research_count=1,
        equity_selected_count=1,
        option_research_count=1,
    )
    candidate = dict(payload["candidates"][0])
    candidate["strategy_type"] = strategy_type
    legs = [dict(item) for item in candidate["legs"][:keep_leg_count]]
    for leg, ratio in zip(legs, ratios, strict=False):
        leg["ratio"] = ratio
    candidate["legs"] = tuple(legs)
    payload["candidates"] = (candidate,)

    formal = dict(payload["formal_research_pools"])
    descriptor = dict(formal["descriptor"])
    campaign_hash = after_hours_campaign_lineage_hash(payload)
    identity_manifest = (
        option_candidate_identity(
            {
                "symbol": candidate["underlying"],
                "structure": structure,
                "legs": tuple(
                    {
                        "con_id": leg["contract_id"],
                        "contract_id_ex": leg["contract_id_ex"],
                        "expiration": leg["expiration"],
                        "strike": leg["strike"],
                        "right": "CALL" if leg["right"] in {"C", "CALL"} else "PUT",
                        "side": leg["side"],
                        "ratio": leg["ratio"],
                        "multiplier": leg["multiplier"],
                        "exchange": leg["exchange"],
                    }
                    for leg in legs
                ),
            }
        ),
    )
    descriptor["campaign_hash"] = campaign_hash
    descriptor["option_candidate_identity_hash"] = canonical_hash(identity_manifest)
    formal["campaign_hash"] = campaign_hash
    formal["descriptor"] = descriptor
    formal["descriptor_hash"] = canonical_hash(descriptor)
    payload["formal_research_pools"] = formal

    assert _after_hours_candidate_identity_manifest(payload) == identity_manifest
    assert _valid_formal_descriptor(formal, payload)


def test_next_session_health_reconciles_only_verified_late_research_counts(
    tmp_path,
) -> None:
    provided: list[Mapping[str, object]] = [{}]
    prepared = {
        "schema": "options_copilot.next_session_preparation.v1",
        "status": "DEGRADED",
        "next_trading_date": "2026-08-05",
        "equity_research_count": 0,
        "equity_selected_count": 0,
        "option_research_structure_count": 0,
        "option_structure_count": 0,
        "premarket_parent_eligible_structure_count": 10,
        "executable_count": 0,
        "reason_codes": (
            "EQUITY_POOL_EMPTY_OR_UNAVAILABLE",
            "OPTION_POOL_EMPTY_OR_UNAVAILABLE",
        ),
    }
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(datetime(2026, 8, 4, 20, 40, tzinfo=timezone.utc))),
        )
        loop.bind_daily_callbacks(
            verified_after_hours=lambda: provided[0],
        )
        loop._last_daily_results["NEXT_SESSION_PREPARATION"] = prepared

        verified_payload = _formal_after_hours_payload(
            equity_research_count=10,
            equity_selected_count=10,
            option_research_count=10,
        )
        provided[0] = verified_payload
        reconciled = loop.health()["daily_operations"]["next_session_preparation"]

        assert reconciled["status"] == "READY"
        assert reconciled["equity_research_count"] == 10
        assert reconciled["equity_selected_count"] == 10
        assert reconciled["option_research_structure_count"] == 10
        assert reconciled["executable_count"] == 0
        assert reconciled["checked_at"] == verified_payload[
            "formal_research_pools"
        ]["materialized_at"]
        assert reconciled["source_preparation_checked_at"] is None
        assert reconciled["reason_codes"] == ()
        assert reconciled["decision"] == "NO_TRADE"
        assert reconciled["decision_authority"] == "SUPPORTING_ONLY"

        provided[0] = {}
        loop._last_daily_results["AFTER_HOURS_REPRICE"] = verified_payload
        unverified = loop.health()["daily_operations"]["next_session_preparation"]

        assert unverified["equity_research_count"] == 0
        assert unverified["equity_selected_count"] == 0
        assert unverified["option_research_structure_count"] == 0
        assert unverified["executable_count"] == 0
        assert unverified["status"] == "DEGRADED"
        assert unverified["reason_codes"] == (
            *prepared["reason_codes"],
            "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE",
        )
        assert unverified["reconciled_from_verified_after_hours"] is False

        forged = _formal_after_hours_payload(
            equity_research_count=10,
            equity_selected_count=10,
            option_research_count=10,
        )
        forged_formal = dict(forged["formal_research_pools"])
        forged_formal["equity_research_count"] = 99
        forged["formal_research_pools"] = forged_formal
        provided[0] = forged
        rejected = loop.health()["daily_operations"]["next_session_preparation"]

        assert rejected["equity_research_count"] == 0
        assert rejected["option_research_structure_count"] == 0
        assert rejected["executable_count"] == 0
        assert "AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID" in rejected["reason_codes"]

        provided[0] = _formal_after_hours_payload(
            equity_research_count=10,
            equity_selected_count=0,
            option_research_count=0,
        )
        partial = loop.health()["daily_operations"]["next_session_preparation"]

        assert partial["equity_research_count"] == 10
        assert partial["equity_selected_count"] == 0
        assert partial["option_research_structure_count"] == 0
        assert partial["executable_count"] == 0
        assert partial["reason_codes"] == ("OPTION_POOL_EMPTY_OR_UNAVAILABLE",)


def test_next_session_health_degrades_empty_parent_subset_with_unbound_watches(
    tmp_path,
) -> None:
    prepared = {
        "schema": "options_copilot.next_session_preparation.v1",
        "status": "DEGRADED",
        "next_trading_date": "2026-08-05",
        "equity_research_count": 0,
        "equity_selected_count": 0,
        "option_research_structure_count": 0,
        "option_structure_count": 0,
        "premarket_parent_eligible_structure_count": 0,
        "executable_count": 0,
        "reason_codes": ("AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID",),
    }
    verified_payload = _formal_after_hours_payload(
        equity_research_count=8,
        equity_selected_count=0,
        option_research_count=0,
        raw_candidate_count=8,
    )
    formal = verified_payload["formal_research_pools"]
    assert isinstance(formal, Mapping)
    assert _valid_formal_descriptor(formal, verified_payload)

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(
                _calendar(datetime(2026, 8, 4, 20, 40, tzinfo=timezone.utc))
            ),
        )
        loop.bind_daily_callbacks(verified_after_hours=lambda: verified_payload)
        loop._last_daily_results["NEXT_SESSION_PREPARATION"] = prepared

        reconciled = loop.health()["daily_operations"]["next_session_preparation"]

    assert reconciled["status"] == "DEGRADED"
    assert reconciled["equity_research_count"] == 8
    assert reconciled["equity_selected_count"] == 0
    assert reconciled["option_research_structure_count"] == 0
    assert reconciled["option_structure_count"] == 0
    assert reconciled["premarket_parent_eligible_structure_count"] == 0
    assert reconciled["reason_codes"] == (
        "PREMARKET_PARENT_ELIGIBLE_STRUCTURE_UNAVAILABLE",
    )
    assert reconciled["reconciled_from_verified_after_hours"] is True
    assert reconciled["decision"] == "NO_TRADE"


def test_next_session_health_keeps_unselected_structures_as_degraded_research_only(
    tmp_path,
) -> None:
    prepared = {
        "schema": "options_copilot.next_session_preparation.v1",
        "status": "DEGRADED",
        "next_trading_date": "2026-08-28",
        "equity_research_count": 0,
        "equity_selected_count": 0,
        "option_research_structure_count": 0,
        "option_structure_count": 0,
        "premarket_parent_eligible_structure_count": 0,
        "executable_count": 0,
        "reason_codes": ("AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID",),
    }
    verified_payload = _formal_after_hours_payload(
        equity_research_count=8,
        equity_selected_count=0,
        option_research_count=8,
    )
    formal = verified_payload["formal_research_pools"]
    assert isinstance(formal, Mapping)
    assert not _valid_formal_descriptor(formal, verified_payload)
    assert _valid_formal_descriptor(
        formal,
        verified_payload,
        verified_subset=True,
    )

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(
                _calendar(datetime(2026, 8, 27, 20, 40, tzinfo=timezone.utc))
            ),
        )
        loop.bind_daily_callbacks(verified_after_hours=lambda: verified_payload)
        loop._last_daily_results["NEXT_SESSION_PREPARATION"] = prepared

        reconciled = loop.health()["daily_operations"]["next_session_preparation"]

    assert reconciled["status"] == "DEGRADED"
    assert reconciled["equity_research_count"] == 8
    assert reconciled["equity_selected_count"] == 0
    assert reconciled["option_research_structure_count"] == 8
    assert reconciled["option_structure_count"] == 8
    assert reconciled["premarket_parent_eligible_structure_count"] == 0
    assert reconciled["reason_codes"] == (
        "PREMARKET_PARENT_ELIGIBLE_STRUCTURE_UNAVAILABLE",
    )
    assert reconciled["reconciled_from_verified_after_hours"] is True
    assert reconciled["decision"] == "NO_TRADE"
    assert reconciled["decision_authority"] == "SUPPORTING_ONLY"


def test_next_session_health_recovers_verified_partial_research_handoff(
    tmp_path,
) -> None:
    prepared = {
        "schema": "options_copilot.next_session_preparation.v1",
        "status": "DEGRADED",
        "next_trading_date": "2026-08-05",
        "equity_research_count": 0,
        "equity_selected_count": 0,
        "option_research_structure_count": 0,
        "option_structure_count": 0,
        "premarket_parent_eligible_structure_count": 8,
        "executable_count": 0,
        "reason_codes": (
            "EQUITY_POOL_EMPTY_OR_UNAVAILABLE",
            "OPTION_POOL_EMPTY_OR_UNAVAILABLE",
            "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE",
        ),
    }
    verified_partial = _formal_after_hours_payload(
        equity_research_count=8,
        equity_selected_count=8,
        option_research_count=8,
        campaign_target_count=10,
        reason_codes=("AFTER_HOURS_INDICATIVE_PARTIAL",),
    )
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(
                _calendar(datetime(2026, 8, 4, 20, 40, tzinfo=timezone.utc))
            ),
        )
        loop.bind_daily_callbacks(verified_after_hours=lambda: verified_partial)
        loop._last_daily_results["NEXT_SESSION_PREPARATION"] = prepared

        reconciled = loop.health()["daily_operations"]["next_session_preparation"]

    assert reconciled["status"] == "DEGRADED"
    assert reconciled["equity_research_count"] == 8
    assert reconciled["equity_selected_count"] == 8
    assert reconciled["option_research_structure_count"] == 8
    assert reconciled["executable_count"] == 0
    assert reconciled["reason_codes"] == ("AFTER_HOURS_INDICATIVE_PARTIAL",)
    assert reconciled["reconciled_from_verified_after_hours"] is True
    assert reconciled["decision"] == "NO_TRADE"
    assert reconciled["decision_authority"] == "SUPPORTING_ONLY"
    assert reconciled["approval_eligible"] is False
    assert reconciled["instruction_creation_allowed"] is False
    assert reconciled["order_allowed"] is False


@pytest.mark.parametrize(
    ("provider_result", "expected_reason"),
    (
        (None, "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE"),
        ({}, "AFTER_HOURS_VERIFIED_RESULT_UNAVAILABLE"),
        (RuntimeError("cross-store verifier failed"), "AFTER_HOURS_VERIFIER_FAILED"),
    ),
)
def test_bound_after_hours_verifier_failure_clears_stale_ready_pools(
    tmp_path,
    provider_result,
    expected_reason,
) -> None:
    stale = {
        "schema": "options_copilot.next_session_preparation.v1",
        "status": "READY",
        "next_trading_date": "2026-08-05",
        "equity_research_count": 10,
        "equity_selected_count": 0,
        "option_research_structure_count": 10,
        "option_structure_count": 10,
        "executable_count": 0,
        "equity_pool_hash": "a" * 64,
        "option_pool_hash": "b" * 64,
        "after_hours_campaign_hash": "c" * 64,
        "after_hours_campaign": {"status": "READY"},
        "after_hours_formal_research_pools": {"status": "READY"},
        "reason_codes": (),
    }
    durable = _formal_after_hours_payload(
        equity_research_count=10,
        equity_selected_count=10,
        option_research_count=10,
    )

    def verified_provider():
        if isinstance(provider_result, Exception):
            raise provider_result
        return provider_result

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(
                _calendar(datetime(2026, 8, 4, 20, 40, tzinfo=timezone.utc))
            ),
        )
        loop.bind_daily_callbacks(verified_after_hours=verified_provider)
        loop._last_daily_results["NEXT_SESSION_PREPARATION"] = stale
        loop._last_daily_results["AFTER_HOURS_REPRICE"] = durable

        result = loop.health()["daily_operations"]["next_session_preparation"]

    assert result["status"] == "DEGRADED"
    assert result["equity_research_count"] == 0
    assert result["equity_selected_count"] == 0
    assert result["option_research_structure_count"] == 0
    assert result["option_structure_count"] == 0
    assert result["executable_count"] == 0
    assert result["equity_pool_hash"] is None
    assert result["option_pool_hash"] is None
    assert result["after_hours_campaign_hash"] is None
    assert result["after_hours_campaign"] is None
    assert result["after_hours_formal_research_pools"] is None
    assert result["reason_codes"] == (expected_reason,)
    assert result["reconciled_from_verified_after_hours"] is False
    assert result["reconciled_from_durable_after_hours"] is False
    assert result["decision"] == "NO_TRADE"
    assert result["approval_eligible"] is False
    assert result["instruction_creation_allowed"] is False
    assert result["order_allowed"] is False


@pytest.mark.parametrize(
    ("equity_research_count", "equity_selected_count", "option_research_count"),
    (
        (True, 0, 1),
        (10, -1, 1),
        (10, 11, 1),
        (10, 0, -1),
    ),
)
def test_bound_after_hours_verifier_rejects_invalid_verified_counts(
    tmp_path,
    equity_research_count,
    equity_selected_count,
    option_research_count,
) -> None:
    stale = {
        "schema": "options_copilot.next_session_preparation.v1",
        "status": "READY",
        "next_trading_date": "2026-08-05",
        "equity_research_count": 10,
        "equity_selected_count": 10,
        "option_research_structure_count": 10,
        "option_structure_count": 10,
        "executable_count": 0,
        "equity_pool_hash": "a" * 64,
        "option_pool_hash": "b" * 64,
        "after_hours_campaign_hash": "c" * 64,
        "after_hours_campaign": {"status": "READY"},
        "after_hours_formal_research_pools": {"status": "READY"},
        "reason_codes": (),
    }
    provided = _formal_after_hours_payload(
        equity_research_count=equity_research_count,
        equity_selected_count=equity_selected_count,
        option_research_count=option_research_count,
    )

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(
                _calendar(datetime(2026, 8, 4, 20, 40, tzinfo=timezone.utc))
            ),
        )
        loop.bind_daily_callbacks(verified_after_hours=lambda: provided)
        loop._last_daily_results["NEXT_SESSION_PREPARATION"] = stale

        result = loop.health()["daily_operations"]["next_session_preparation"]

    assert result["status"] == "DEGRADED"
    assert result["equity_research_count"] == 0
    assert result["equity_selected_count"] == 0
    assert result["option_research_structure_count"] == 0
    assert result["option_structure_count"] == 0
    assert result["executable_count"] == 0
    assert result["equity_pool_hash"] is None
    assert result["option_pool_hash"] is None
    assert result["after_hours_campaign_hash"] is None
    assert result["after_hours_campaign"] is None
    assert result["after_hours_formal_research_pools"] is None
    assert result["reason_codes"] == ("AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID",)
    assert result["reconciled_from_verified_after_hours"] is False
    assert result["reconciled_from_durable_after_hours"] is False
    assert result["decision"] == "NO_TRADE"
    assert result["approval_eligible"] is False
    assert result["instruction_creation_allowed"] is False
    assert result["order_allowed"] is False


def test_0830_research_refresh_refuses_stale_calendar_evidence(tmp_path):
    now = datetime(2026, 8, 4, 12, 30, 30, tzinfo=timezone.utc)
    refreshes: list[datetime] = []

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(now - timedelta(seconds=6))),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(
            research_refresh=lambda _calendar, _scheduled_for, _checked_at: refreshes.append(now),
        )

        result = loop.tick_once()
        research = loop.health()["daily_operations"]["runs"][0]

    assert result.duplicate_reason == "RECOVERY_EVIDENCE_STALE"
    assert refreshes == [now]
    assert research["status"] == "FAILED"


def test_0830_research_refresh_records_unavailable_calendar_without_replay(tmp_path):
    now = datetime(2026, 8, 3, 12, 30, 30, tzinfo=timezone.utc)
    unavailable = UsOptionsSessionCalendar().normalize(
        liquid_hours="",
        trading_hours="",
        timezone_id="",
        observed_at=now,
        source="",
        now=now,
    )
    calls: list[tuple[object, datetime, datetime]] = []

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(unavailable),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(
            research_refresh=lambda snapshot, scheduled_for, checked_at: (
                calls.append((snapshot, scheduled_for, checked_at))
                or {"status": "NOT_RUN"}
            ),
        )

        loop.tick_once()
        loop.tick_once()
        research = loop.health()["daily_operations"]["runs"][0]

    assert calls == [
        (
            unavailable,
            datetime(2026, 8, 3, 8, 30, tzinfo=US_OPTIONS_TIMEZONE),
            now,
        )
    ]
    assert research["operation"] == "RESEARCH_REFRESH"
    assert research["status"] == "COMPLETED"


def test_daily_health_binds_each_top10_slot_to_its_own_durable_run(tmp_path):
    freeze_at = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)
    reprice_at = datetime(2026, 8, 4, 13, 35, tzinfo=timezone.utc)
    checked_at = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)

    class Producer:
        def tick(self, *, scheduled_for):
            if scheduled_for == freeze_at:
                return {
                    "status": "PREMARKET_FROZEN",
                    "reason_codes": (),
                    "written_count": 10,
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                }
            return {
                "status": "NO_TRADE",
                "reason_codes": ("QUOTE_STALE_OR_FUTURE",),
                "written_count": 0,
                "missing_symbols": ("SPY",),
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        top10_service = Top10SchedulerService(
            store,
            Producer(),
            pipeline_version="top10-v1",
        )
        freeze = top10_service.tick(_calendar(freeze_at), now=freeze_at)
        reprice = top10_service.tick(_calendar(reprice_at), now=reprice_at)
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(checked_at)),
            clock=lambda: checked_at,
            top10_service=top10_service,
        )

        loop.tick_once()
        top10_runs = loop.health()["daily_operations"]["runs"][1:3]

    assert [item["status"] for item in top10_runs] == [
        "COMPLETED",
        "COMPLETED",
    ]
    assert [item["scan_run_id"] for item in top10_runs] == [
        freeze.scan_run_id,
        reprice.scan_run_id,
    ]
    assert freeze.scan_run_id != reprice.scan_run_id
    assert [item["producer_status"] for item in top10_runs] == [
        "PREMARKET_FROZEN",
        "NO_TRADE",
    ]
    assert [item["producer_written_count"] for item in top10_runs] == [10, 0]
    assert top10_runs[1]["reason_codes"] == ("QUOTE_STALE_OR_FUTURE",)
    assert top10_runs[1]["producer_missing_symbols"] == ("SPY",)
    assert len(top10_runs[1]["producer_evidence_hash"]) == 64


def test_background_loop_close_does_not_report_stopped_while_tick_is_blocked(
    tmp_path,
    monkeypatch,
):
    now = datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)
    entered = threading.Event()
    release = threading.Event()

    class BlockingCalendar:
        def snapshot(self, *, now):
            entered.set()
            release.wait(timeout=2)
            return _calendar(now)

    original_join = threading.Thread.join
    worker = None
    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            BlockingCalendar(),
            clock=lambda: now,
        )
        loop.start()
        assert entered.wait(timeout=1)
        worker = loop._thread
        assert worker is not None

        def timed_out_join(thread, timeout=None):
            original_join(thread, timeout=0.01)

        monkeypatch.setattr(threading.Thread, "join", timed_out_join)
        try:
            loop.close()
            alive_after_close = worker.is_alive()
            health_after_close = loop.health()
            retained_worker = loop._thread is worker
        finally:
            release.set()
            original_join(worker, timeout=1)
            monkeypatch.setattr(threading.Thread, "join", original_join)
            loop.close()

    assert alive_after_close is True
    assert health_after_close["status"] != "STOPPED"
    assert retained_worker is True


def test_close_terminalizes_running_daily_job_before_store_shutdown(tmp_path):
    now = datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)
    entered = threading.Event()
    release = threading.Event()

    def blocked_outcome():
        entered.set()
        release.wait(timeout=2)
        return {"status": "COMPLETED"}

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, _Pipeline(), pipeline_version="p4"),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
        )
        loop.bind_daily_callbacks(outcome_process=blocked_outcome)
        loop.tick_once()
        assert entered.wait(timeout=1)

        assert loop.close() is True
        run = next(
            item
            for item in loop.health()["daily_operations"]["runs"]
            if item["operation"] == "OUTCOME_PROCESSING"
        )
        durable = store.latest_daily_result("OUTCOME_PROCESSING")

        assert run["status"] == "FAILED"
        assert durable is not None
        assert durable.status == "FAILED"
        assert durable.payload["reason_codes"] == [
            "OUTCOME_PROCESSING_ABANDONED_ON_CLOSE"
        ]
        assert loop._daily_jobs == {}
        release.set()


def test_top10_duplicate_health_fails_closed_in_process_and_after_restart(tmp_path):
    now = datetime(2026, 8, 4, 13, 35, 30, tzinfo=timezone.utc)
    database = tmp_path / "runs.sqlite"

    class NoTradeProducer:
        calls = 0

        def tick(self, *, scheduled_for):
            self.calls += 1
            return {
                "status": "NO_TRADE",
                "reason_codes": ("QUOTE_STALE_OR_FUTURE",),
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }

    producer = NoTradeProducer()
    with ScanRunStore(database) as store:
        pipeline = _Pipeline()
        loop = ScanSchedulerLoop(
            ScanSchedulerService(store, pipeline, pipeline_version="p4"),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
            top10_service=Top10SchedulerService(
                store,
                producer,
                pipeline_version="top10-v1",
            ),
        )
        loop.tick_once()
        first_health = loop.health()["top10_producer"]
        loop.tick_once()
        duplicate_health = loop.health()["top10_producer"]

    with ScanRunStore(database) as restarted_store:
        restarted_loop = ScanSchedulerLoop(
            ScanSchedulerService(
                restarted_store,
                _Pipeline(),
                pipeline_version="p4",
            ),
            _CalendarProvider(_calendar(now)),
            clock=lambda: now,
            top10_service=Top10SchedulerService(
                restarted_store,
                producer,
                pipeline_version="top10-v1",
            ),
        )
        restarted_loop.tick_once()
        restarted_health = restarted_loop.health()["top10_producer"]

    assert producer.calls == 1
    assert first_health["status"] == "DEGRADED"
    assert first_health["last_producer_status"] == "NO_TRADE"
    assert first_health["last_reason_codes"] == ("QUOTE_STALE_OR_FUTURE",)
    for health in (duplicate_health, restarted_health):
        assert health["status"] == "DEGRADED"
        assert health["last_producer_status"] == "NO_TRADE"
        assert health["last_reason"] == "SLOT_ALREADY_COMPLETED"
        assert health["last_reason_codes"] == ("QUOTE_STALE_OR_FUTURE",)


def test_background_loop_calendar_failure_is_no_trade_and_skips_pipeline(tmp_path):
    now = datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)
    pipeline = _Pipeline()

    class _BrokenCalendar:
        def snapshot(self, *, now):
            raise RuntimeError("fixture calendar unavailable")

    with ScanRunStore(tmp_path / "runs.sqlite") as store:
        service = ScanSchedulerService(store, pipeline, pipeline_version="p4")
        loop = ScanSchedulerLoop(service, _BrokenCalendar(), clock=lambda: now)

        result = loop.tick_once()
        health = loop.health()

    assert result.status == "NO_TRADE"
    assert result.duplicate_reason == "CALENDAR_PROVIDER_UNAVAILABLE"
    assert pipeline.calls == []
    assert health["status"] == "DEGRADED"


def test_cli_tick_and_inspect_emit_durable_review_only_result(
    tmp_path,
    capsys,
):
    now = datetime(2026, 8, 4, 14, 5, tzinfo=timezone.utc)
    calendar_path = tmp_path / "calendar.json"
    calendar_path.write_text(
        json.dumps(
            {
                "liquid_hours": "20260804:0930-1600",
                "trading_hours": "20260804:0930-1600",
                "timezone_id": "America/New_York",
                "observed_at": now.isoformat(),
                "source": "IBKR_REQ_CONTRACT_DETAILS_READONLY",
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "runs.sqlite"

    exit_code = scanner_main(
        [
            "--db",
            str(db_path),
            "--pipeline-version",
            "p4-cli",
            "tick",
            "--calendar",
            str(calendar_path),
            "--now",
            now.isoformat(),
            "--owner",
            "cli-test",
        ]
    )
    tick = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert tick["status"] == "COMPLETED"
    assert tick["scan_run_id"].startswith("scan.")
    assert tick["result_hash"]

    inspect_code = scanner_main(
        [
            "--db",
            str(db_path),
            "--pipeline-version",
            "p4-cli",
            "inspect-slot",
            "--trading-date",
            "2026-08-04",
            "--slot-at",
            "2026-08-04T10:00:00-04:00",
        ]
    )
    inspected = json.loads(capsys.readouterr().out)

    assert inspect_code == 0
    assert inspected["status"] == "FOUND"
    assert inspected["runs"][0]["scan_run_id"] == tick["scan_run_id"]
    assert inspected["review_only"] is True
    assert inspected["direct_order_submission"] is False
