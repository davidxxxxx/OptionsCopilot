from __future__ import annotations

from datetime import date, datetime, timedelta

from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.news.weekly_brief import (
    SourceHealthStatus,
    WeeklyBriefSourceHealth,
)
from options_copilot.news.weekly_brief_runtime import WeeklyBriefRuntime
from options_copilot.news.weekly_brief_store import WeeklyBriefStore
from options_copilot.storage.canonical import canonical_hash


CUTOFF = datetime(2026, 9, 8, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)
SESSIONS = (date(2026, 9, 8), date(2026, 9, 9))
CALENDAR_HASH = canonical_hash({"calendar": "holiday-week"})


def _health() -> WeeklyBriefSourceHealth:
    return WeeklyBriefSourceHealth.build(
        source="OFFICIAL_EVENTS",
        status=SourceHealthStatus.READY,
        mandatory=True,
        observed_at=CUTOFF - timedelta(seconds=1),
        source_hash=canonical_hash({"source": "official-events"}),
    )


def test_runtime_persists_due_slot_and_exact_retry_is_idempotent(tmp_path) -> None:
    clock = lambda: CUTOFF + timedelta(seconds=10)
    with WeeklyBriefStore(tmp_path / "weekly.sqlite3", clock=clock) as store:
        runtime = WeeklyBriefRuntime(store)
        first = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=clock(),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(_health(),),
            watch_items=(),
        )
        retry = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=clock(),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(_health(),),
            watch_items=(),
        )

        assert first["status"] == "PROVISIONAL"
        assert first["execution_allowed"] is False
        assert first["persistence"]["inserted"] is True
        assert retry["persistence"]["inserted"] is False
        assert store.count == 1
        assert runtime.read_model()["content_hash"] == first["content_hash"]


def test_due_slot_retry_keeps_first_append_when_source_bundle_changes(tmp_path) -> None:
    clock = lambda: CUTOFF + timedelta(seconds=10)
    with WeeklyBriefStore(tmp_path / "weekly.sqlite3", clock=clock) as store:
        runtime = WeeklyBriefRuntime(store)
        first = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=clock(),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(_health(),),
            watch_items=(),
        )
        changed_health = WeeklyBriefSourceHealth.build(
            source="OFFICIAL_EVENTS",
            status=SourceHealthStatus.READY,
            mandatory=True,
            observed_at=CUTOFF,
            source_hash=canonical_hash({"source": "changed-official-events"}),
        )

        retry = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=clock(),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(changed_health,),
            watch_items=(),
        )

        assert retry["content_hash"] == first["content_hash"]
        assert retry["persistence"]["inserted"] is False
        assert store.count == 1


def test_runtime_missed_slot_is_not_run_and_does_not_append(tmp_path) -> None:
    clock = lambda: CUTOFF + timedelta(minutes=1)
    with WeeklyBriefStore(tmp_path / "weekly.sqlite3", clock=clock) as store:
        runtime = WeeklyBriefRuntime(store)
        result = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=clock(),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(_health(),),
            watch_items=(),
        )

        assert result["status"] == "NOT_RUN"
        assert result["reason_codes"] == ["WEEKLY_BRIEF_SLOT_MISSED"]
        assert result["execution_allowed"] is False
        assert store.count == 1
        assert runtime.read_model() == result

        restarted = WeeklyBriefRuntime(store).read_model()
        assert restarted == {**result, "persistence": {**result["persistence"], "inserted": False}}


def test_runtime_persists_mandatory_source_unavailable_once(tmp_path) -> None:
    evaluated_at = CUTOFF + timedelta(seconds=10)
    with WeeklyBriefStore(tmp_path / "weekly.sqlite3", clock=lambda: evaluated_at) as store:
        runtime = WeeklyBriefRuntime(store)
        first = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=evaluated_at,
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(
                WeeklyBriefSourceHealth.build(
                    source="IBKR_SESSION_CALENDAR",
                    status=SourceHealthStatus.UNAVAILABLE,
                    mandatory=True,
                    observed_at=evaluated_at,
                    reason_codes=("CALENDAR_STALE",),
                ),
            ),
            watch_items=(),
        )
        retry = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=evaluated_at + timedelta(seconds=1),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(
                WeeklyBriefSourceHealth.build(
                    source="IBKR_SESSION_CALENDAR",
                    status=SourceHealthStatus.UNAVAILABLE,
                    mandatory=True,
                    observed_at=evaluated_at,
                    reason_codes=("CALENDAR_STALE",),
                ),
            ),
            watch_items=(),
        )

        assert first["status"] == "NOT_RUN"
        assert first["reason_codes"] == ["WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"]
        assert first["persistence"]["inserted"] is True
        assert retry["persistence"]["inserted"] is False
        assert store.count == 1


def test_same_slot_not_run_then_provisional_keeps_first_immutable_outcome(
    tmp_path,
) -> None:
    evaluated_at = CUTOFF + timedelta(seconds=10)
    with WeeklyBriefStore(
        tmp_path / "weekly.sqlite3",
        clock=lambda: evaluated_at,
    ) as store:
        runtime = WeeklyBriefRuntime(store)
        first = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=evaluated_at,
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(
                WeeklyBriefSourceHealth.build(
                    source="IBKR_SESSION_CALENDAR",
                    status=SourceHealthStatus.UNAVAILABLE,
                    mandatory=True,
                    observed_at=evaluated_at,
                    reason_codes=("CALENDAR_STALE",),
                ),
            ),
            watch_items=(),
        )
        retry = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=evaluated_at + timedelta(seconds=1),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(_health(),),
            watch_items=(),
        )

        assert first["status"] == "NOT_RUN"
        assert retry["status"] == "NOT_RUN"
        assert retry["content_hash"] == first["content_hash"]
        assert retry["persistence"]["inserted"] is False
        assert store.count == 1


def test_same_slot_provisional_then_not_run_keeps_first_immutable_outcome(
    tmp_path,
) -> None:
    evaluated_at = CUTOFF + timedelta(seconds=10)
    with WeeklyBriefStore(
        tmp_path / "weekly.sqlite3",
        clock=lambda: evaluated_at,
    ) as store:
        runtime = WeeklyBriefRuntime(store)
        first = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=evaluated_at,
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(_health(),),
            watch_items=(),
        )
        retry = runtime.evaluate_and_append(
            scheduled_for=CUTOFF,
            evaluated_at=evaluated_at + timedelta(seconds=1),
            official_session_dates=SESSIONS,
            calendar_hash=CALENDAR_HASH,
            evidence_items=(),
            source_health=(
                WeeklyBriefSourceHealth.build(
                    source="IBKR_SESSION_CALENDAR",
                    status=SourceHealthStatus.UNAVAILABLE,
                    mandatory=True,
                    observed_at=evaluated_at,
                    reason_codes=("CALENDAR_STALE",),
                ),
            ),
            watch_items=(),
        )

        assert first["status"] == "PROVISIONAL"
        assert retry["status"] == "PROVISIONAL"
        assert retry["content_hash"] == first["content_hash"]
        assert retry["persistence"]["inserted"] is False
        assert store.count == 1


def test_empty_runtime_is_explicitly_not_available(tmp_path) -> None:
    with WeeklyBriefStore(tmp_path / "weekly.sqlite3") as store:
        result = WeeklyBriefRuntime(store).read_model()

    assert result["status"] == "NOT_RUN"
    assert result["reason_codes"] == ["WEEKLY_BRIEF_NOT_AVAILABLE"]
    assert result["persistence"]["append_only"] is True
