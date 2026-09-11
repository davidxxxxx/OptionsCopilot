from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading
import time as time_module

from options_copilot.market.session_calendar import (
    US_OPTIONS_TIMEZONE,
    UsOptionsSessionCalendar,
)
from options_copilot.news_runtime import NewsCoordinator


CUTOFF = datetime(2026, 8, 10, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)


def _calendar(observed_at: datetime):
    hours = ";".join(
        (
            "20260810:0930-1600",
            "20260811:0930-1600",
            "20260812:0930-1600",
            "20260813:0930-1600",
            "20260814:0930-1600",
        )
    )
    return UsOptionsSessionCalendar().normalize(
        liquid_hours=hours,
        trading_hours=hours,
        timezone_id="America/New_York",
        observed_at=observed_at,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=observed_at,
    )


def test_news_coordinator_exact_tick_persists_zero_to_ten_provisional_brief(tmp_path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(seconds=10),
    )
    try:
        runtime.refresh_research(
            _calendar(CUTOFF + timedelta(seconds=10)),
            CUTOFF,
            CUTOFF + timedelta(seconds=10),
        )
        first = runtime.weekly_brief_payload()
        runtime.refresh_research(
            _calendar(CUTOFF + timedelta(seconds=10)),
            CUTOFF,
            CUTOFF + timedelta(seconds=10),
        )
        retry = runtime.weekly_brief_payload()

        assert first["status"] == "PROVISIONAL"
        assert first["decision"] == "OBSERVATION_ONLY"
        assert first["execution_allowed"] is False
        assert first["review_allowed"] is False
        assert first["watch_count"] == 0
        assert first["persistence"]["append_only"] is True
        assert runtime.weekly_brief_store.count == 1
        assert retry["content_hash"] == first["content_hash"]
    finally:
        runtime.close()


def test_observation_only_scheduler_runs_without_broker_or_provider_worker(tmp_path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(seconds=10),
    )
    try:
        runtime.start()
        deadline = time_module.monotonic() + 1.0
        while (
            runtime.weekly_brief_store.count == 0
            and time_module.monotonic() < deadline
        ):
            time_module.sleep(0.01)

        result = runtime.weekly_brief_payload()

        assert runtime._thread is None
        assert runtime._weekly_thread is not None
        assert runtime._weekly_thread.is_alive()
        assert runtime.weekly_brief_store.count == 1
        assert result["status"] == "PROVISIONAL"
        assert result["decision"] == "OBSERVATION_ONLY"
        assert result["decision_authority"] == "SUPPORTING_ONLY"
        assert result["execution_allowed"] is False
        assert result["review_allowed"] is False
        assert result["combination_generation_allowed"] is False
    finally:
        runtime.close()


def test_observation_only_tick_never_refreshes_configured_provider(tmp_path) -> None:
    class ForbiddenProvider:
        health = "READY"
        health_reason = None

        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            raise AssertionError("weekly observation must not call providers")

    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(ForbiddenProvider(),),
        clock=lambda: CUTOFF + timedelta(seconds=10),
    )
    try:
        result = runtime.refresh_weekly_observation(
            checked_at=CUTOFF + timedelta(seconds=10),
        )

        assert result["weekly_brief_attempted"] is True
        assert result["status"] == "PROVISIONAL"
        assert runtime.weekly_brief_store.count == 1
    finally:
        runtime.close()


def test_scanner_weekly_callback_never_refreshes_configured_provider(tmp_path) -> None:
    calls = 0

    class CountingProvider:
        health = "READY"
        health_reason = None

        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            nonlocal calls
            calls += 1
            return ()

    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(CountingProvider(),),
        clock=lambda: CUTOFF + timedelta(seconds=10),
    )
    try:
        result = runtime.refresh_research(
            _calendar(CUTOFF + timedelta(seconds=10)),
            CUTOFF,
            CUTOFF + timedelta(seconds=10),
        )

        assert calls == 0
        assert result["status"] == "PROVISIONAL"
        assert result["provider_refresh"] == {
            "status": "NOT_RUN",
            "reason": "INDEPENDENT_PROVIDER_POLLER_OWNS_REFRESH",
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
        }
    finally:
        runtime.close()


def test_observation_only_tick_never_replays_missed_exact_minute(tmp_path) -> None:
    runtime = NewsCoordinator(tmp_path / "evidence.sqlite3", clock=lambda: CUTOFF)
    try:
        before = runtime.refresh_weekly_observation(
            checked_at=CUTOFF - timedelta(seconds=1),
        )
        missed = runtime.refresh_weekly_observation(
            checked_at=CUTOFF + timedelta(minutes=1),
        )

        assert before["weekly_brief_attempted"] is False
        assert before["reason_codes"] == ["WEEKLY_BRIEF_SLOT_NOT_DUE"]
        assert missed["weekly_brief_attempted"] is False
        assert missed["reason_codes"] == ["WEEKLY_BRIEF_SLOT_MISSED"]
        assert runtime.weekly_brief_store.count == 0
    finally:
        runtime.close()


def test_observation_and_broker_ticks_share_one_append_only_weekly_record(tmp_path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(seconds=10),
    )
    try:
        offline = runtime.refresh_weekly_observation(
            checked_at=CUTOFF + timedelta(seconds=10),
        )
        broker = runtime.refresh_research(
            _calendar(CUTOFF + timedelta(seconds=20)),
            CUTOFF,
            CUTOFF + timedelta(seconds=20),
        )

        assert offline["weekly_brief_attempted"] is True
        assert broker["weekly_brief_attempted"] is True
        assert runtime.weekly_brief_store.count == 1
        assert broker["weekly_brief"]["content_hash"] == offline["weekly_brief"][
            "content_hash"
        ]
    finally:
        runtime.close()


def test_stale_broker_first_cannot_poison_source_neutral_weekly_record(tmp_path) -> None:
    now = CUTOFF + timedelta(seconds=10)
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now,
    )
    try:
        stale_calendar = UsOptionsSessionCalendar().normalize(
            liquid_hours=";".join(
                f"2026081{offset}:0930-1600" for offset in range(5)
            ),
            trading_hours=";".join(
                f"2026081{offset}:0930-1600" for offset in range(5)
            ),
            timezone_id="America/New_York",
            observed_at=CUTOFF - timedelta(seconds=10),
            source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
            now=now,
        )
        broker_first = runtime.refresh_research(stale_calendar, CUTOFF, now)
        offline_second = runtime.refresh_weekly_observation(checked_at=now)

        assert broker_first["status"] == "PROVISIONAL"
        assert broker_first["weekly_brief_attempted"] is True
        assert offline_second["status"] == "PROVISIONAL"
        assert runtime.weekly_brief_store.count == 1
        assert broker_first["weekly_brief"]["content_hash"] == offline_second[
            "weekly_brief"
        ]["content_hash"]
    finally:
        runtime.close()


def test_simultaneous_scheduler_triggers_converge_on_same_weekly_record(tmp_path) -> None:
    now = CUTOFF + timedelta(seconds=10)
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now,
    )
    barrier = threading.Barrier(3)
    results: list[dict[str, object]] = []
    result_lock = threading.Lock()

    def record(callback) -> None:
        barrier.wait(timeout=1)
        result = callback()
        with result_lock:
            results.append(result)

    offline = threading.Thread(
        target=record,
        args=(lambda: runtime.refresh_weekly_observation(checked_at=now),),
        daemon=True,
    )
    broker = threading.Thread(
        target=record,
        args=(lambda: runtime.refresh_research(_calendar(now), CUTOFF, now),),
        daemon=True,
    )
    try:
        offline.start()
        broker.start()
        barrier.wait(timeout=1)
        offline.join(timeout=2)
        broker.join(timeout=2)

        assert offline.is_alive() is False
        assert broker.is_alive() is False
        assert len(results) == 2
        assert runtime.weekly_brief_store.count == 1
        content_hashes = {
            str(
                result.get("content_hash")
                or dict(result["weekly_brief"])["content_hash"]
            )
            for result in results
        }
        assert len(content_hashes) == 1
    finally:
        runtime.close()


def test_observation_only_scheduler_fails_closed_outside_reviewed_year(tmp_path) -> None:
    checked_at = datetime(2027, 1, 4, 8, 30, 10, tzinfo=US_OPTIONS_TIMEZONE)
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: checked_at,
    )
    try:
        result = runtime.refresh_weekly_observation(checked_at=checked_at)

        assert result["status"] == "NOT_RUN"
        assert result["weekly_brief_attempted"] is False
        assert result["reason_codes"] == ["RESEARCH_SESSION_YEAR_UNREVIEWED"]
        assert result["decision_authority"] == "SUPPORTING_ONLY"
        assert result["approval_eligible"] is False
        assert result["instruction_creation_allowed"] is False
        assert result["order_creation_allowed"] is False
        assert runtime.weekly_brief_payload()["reason_codes"] == [
            "RESEARCH_SESSION_YEAR_UNREVIEWED"
        ]
        assert runtime.weekly_brief_store.count == 0
    finally:
        runtime.close()


def test_unreviewed_year_hides_persisted_prior_year_brief(tmp_path) -> None:
    now = [CUTOFF + timedelta(seconds=10)]
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now[0],
    )
    try:
        produced = runtime.refresh_weekly_observation(checked_at=now[0])
        assert produced["status"] == "PROVISIONAL"

        now[0] = datetime(2027, 1, 4, 8, 30, 10, tzinfo=US_OPTIONS_TIMEZONE)
        visible = runtime.weekly_brief_payload()

        assert visible["status"] == "NOT_RUN"
        assert visible["reason_codes"] == ["RESEARCH_SESSION_YEAR_UNREVIEWED"]
        assert visible["weekly_brief"] is None
        assert runtime.weekly_brief_store.count == 1
    finally:
        runtime.close()


def test_exact_minute_hides_prior_week_until_current_record_exists(tmp_path) -> None:
    prior_cutoff = CUTOFF - timedelta(days=7)
    now = [prior_cutoff + timedelta(seconds=10)]
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now[0],
    )
    try:
        prior = runtime.refresh_weekly_observation(checked_at=now[0])
        assert prior["status"] == "PROVISIONAL"

        now[0] = CUTOFF + timedelta(seconds=1)
        pending = runtime.weekly_brief_payload()

        assert pending["status"] == "NOT_RUN"
        assert pending["reason_codes"] == ["WEEKLY_BRIEF_SLOT_PENDING"]
        assert pending["weekly_brief"] is None
        assert runtime.weekly_brief_store.count == 1
    finally:
        runtime.close()


def test_weekly_worker_failure_is_sanitized_and_visible(tmp_path, monkeypatch) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(seconds=10),
    )

    def fail_weekly_write(**_kwargs):
        raise RuntimeError("private storage detail")

    monkeypatch.setattr(
        runtime.weekly_brief_runtime,
        "evaluate_research_read_models",
        fail_weekly_write,
    )
    try:
        runtime.start()
        deadline = time_module.monotonic() + 1.0
        result = runtime.weekly_brief_payload()
        while (
            result["reason_codes"] != ["WEEKLY_BRIEF_SCHEDULER_FAILED"]
            and time_module.monotonic() < deadline
        ):
            time_module.sleep(0.01)
            result = runtime.weekly_brief_payload()

        assert result["status"] == "NOT_RUN"
        assert result["reason_codes"] == ["WEEKLY_BRIEF_SCHEDULER_FAILED"]
        assert "private storage detail" not in str(result)
        assert runtime.weekly_brief_store.count == 0
    finally:
        runtime.close()


def test_recovered_pre_slot_worker_failure_is_cleared(tmp_path) -> None:
    now = [CUTOFF - timedelta(minutes=1)]
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now[0],
    )
    try:
        runtime._record_weekly_scheduler_failure()
        assert runtime.weekly_brief_payload()["reason_codes"] == [
            "WEEKLY_BRIEF_SCHEDULER_FAILED"
        ]

        recovered = runtime.refresh_weekly_observation(checked_at=now[0])
        visible = runtime.weekly_brief_payload()

        assert recovered["reason_codes"] == ["WEEKLY_BRIEF_SLOT_NOT_DUE"]
        assert visible["reason_codes"] == ["WEEKLY_BRIEF_SLOT_NOT_DUE"]
        assert runtime._weekly_scheduler_failure is None
    finally:
        runtime.close()


def test_exact_slot_worker_failure_survives_post_slot_poll(tmp_path) -> None:
    now = [CUTOFF + timedelta(seconds=10)]
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now[0],
    )
    try:
        runtime._record_weekly_scheduler_failure()
        now[0] = CUTOFF + timedelta(minutes=1)

        missed = runtime.refresh_weekly_observation(checked_at=now[0])
        visible = runtime.weekly_brief_payload()

        assert missed["reason_codes"] == ["WEEKLY_BRIEF_SLOT_MISSED"]
        assert visible["reason_codes"] == ["WEEKLY_BRIEF_SCHEDULER_FAILED"]
        assert runtime._weekly_scheduler_failure is not None
        assert runtime._weekly_scheduler_failure["terminal"] is True
    finally:
        runtime.close()


def test_terminal_failure_remains_monotonic_through_later_failure_and_recovery(
    tmp_path,
) -> None:
    now = [CUTOFF + timedelta(seconds=10)]
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now[0],
    )
    try:
        runtime._record_weekly_scheduler_failure(checked_at=now[0])
        now[0] = CUTOFF + timedelta(minutes=1)
        runtime._record_weekly_scheduler_failure(checked_at=now[0])

        assert runtime._weekly_scheduler_failure is not None
        assert runtime._weekly_scheduler_failure["terminal"] is True
        assert runtime._weekly_scheduler_failure["failure_count"] == 2

        runtime.refresh_weekly_observation(checked_at=now[0])
        visible = runtime.weekly_brief_payload()

        assert visible["reason_codes"] == ["WEEKLY_BRIEF_SCHEDULER_FAILED"]
        assert runtime._weekly_scheduler_failure is not None
        assert runtime._weekly_scheduler_failure["terminal"] is True
    finally:
        runtime.close()


def test_weekly_store_read_failure_returns_sanitized_projection(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(seconds=10),
    )

    def fail_store_read():
        raise RuntimeError("private sqlite path")

    monkeypatch.setattr(runtime.weekly_brief_runtime, "read_model", fail_store_read)
    try:
        unavailable = runtime.weekly_brief_payload()
        runtime._record_weekly_scheduler_failure()
        scheduler_failed = runtime.weekly_brief_payload()

        assert unavailable["reason_codes"] == ["WEEKLY_BRIEF_STORE_UNAVAILABLE"]
        assert scheduler_failed["reason_codes"] == [
            "WEEKLY_BRIEF_SCHEDULER_FAILED"
        ]
        assert "private sqlite path" not in str(unavailable)
        assert "private sqlite path" not in str(scheduler_failed)
    finally:
        runtime.close()


def test_news_coordinator_missed_tick_never_backfills(tmp_path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(minutes=1),
    )
    try:
        runtime.refresh_research(
            _calendar(CUTOFF + timedelta(minutes=1)),
            CUTOFF,
            CUTOFF + timedelta(minutes=1),
        )
        result = runtime.weekly_brief_payload()

        assert result["status"] == "NOT_RUN"
        assert result["reason_codes"] == ["WEEKLY_BRIEF_SLOT_MISSED"]
        assert runtime.weekly_brief_store.count == 0
    finally:
        runtime.close()


def test_news_coordinator_daily_refresh_does_not_replace_first_session_brief(tmp_path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(days=1, seconds=10),
    )
    try:
        runtime.refresh_research(
            _calendar(CUTOFF + timedelta(seconds=10)),
            CUTOFF,
            CUTOFF + timedelta(seconds=10),
        )
        first = runtime.weekly_brief_payload()
        tuesday = CUTOFF + timedelta(days=1)
        result = runtime.refresh_research(
            _calendar(tuesday + timedelta(seconds=10)),
            tuesday,
            tuesday + timedelta(seconds=10),
        )

        assert result["scheduled_for"] == tuesday.astimezone(timezone.utc).isoformat()
        assert result["weekly_brief_scheduled_for"] == CUTOFF.isoformat()
        assert result["weekly_brief_attempted"] is False
        assert runtime.weekly_brief_payload()["content_hash"] == first["content_hash"]
        assert runtime.weekly_brief_store.count == 1
    finally:
        runtime.close()


def test_news_coordinator_restart_after_missed_slot_projects_no_replay(tmp_path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: CUTOFF + timedelta(minutes=1),
    )
    try:
        result = runtime.weekly_brief_payload()

        assert result["status"] == "NOT_RUN"
        assert result["reason_codes"] == ["WEEKLY_BRIEF_SLOT_MISSED"]
        assert result["execution_allowed"] is False
        assert runtime.weekly_brief_store.count == 0
    finally:
        runtime.close()


def test_stale_broker_calendar_does_not_change_current_weekly_brief(tmp_path) -> None:
    now = [CUTOFF + timedelta(seconds=10)]
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: now[0],
    )
    try:
        hours = ";".join(
            f"2026081{offset}:0930-1600" for offset in range(5)
        )
        stale_calendar = UsOptionsSessionCalendar().normalize(
            liquid_hours=hours,
            trading_hours=hours,
            timezone_id="America/New_York",
            observed_at=CUTOFF - timedelta(seconds=10),
            source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
            now=now[0],
        )
        runtime.refresh_research(
            stale_calendar,
            CUTOFF,
            now[0],
        )
        now[0] = CUTOFF + timedelta(minutes=1)
        result = runtime.weekly_brief_payload()

        assert result["status"] == "PROVISIONAL"
        assert result["persistence"]["sequence"] == 1
        assert runtime.weekly_brief_store.count == 1
    finally:
        runtime.close()
