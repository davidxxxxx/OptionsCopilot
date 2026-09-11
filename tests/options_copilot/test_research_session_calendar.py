from __future__ import annotations

from datetime import date, datetime, timezone

from options_copilot.news.research_session_calendar import (
    BoundedResearchSessionCalendar,
)


def test_normal_week_starts_on_monday_and_is_research_only() -> None:
    snapshot = BoundedResearchSessionCalendar().snapshot(
        now=datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    )

    assert snapshot.ready is True
    assert snapshot.verify_hash() is True
    assert snapshot.week_start == date(2026, 8, 10)
    assert snapshot.sessions == tuple(date(2026, 8, day) for day in range(10, 15))


def test_labor_day_week_uses_tuesday_as_first_session() -> None:
    snapshot = BoundedResearchSessionCalendar().snapshot(
        now=datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
    )

    assert snapshot.ready is True
    assert snapshot.verify_hash() is True
    assert snapshot.sessions[0] == date(2026, 9, 8)
    assert date(2026, 9, 7) not in snapshot.sessions


def test_unreviewed_year_fails_closed() -> None:
    snapshot = BoundedResearchSessionCalendar().snapshot(
        now=datetime(2027, 1, 4, 12, 0, tzinfo=timezone.utc)
    )

    assert snapshot.ready is False
    assert snapshot.verify_hash() is True
    assert snapshot.sessions == ()
    assert snapshot.reason_codes == ("RESEARCH_SESSION_YEAR_UNREVIEWED",)
