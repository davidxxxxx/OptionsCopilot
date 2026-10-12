"""Bounded research-only US options session dates for weekly scheduling.

This calendar can schedule observation-only weekly research while IBKR is
offline.  It never authorizes scans, quotes, ranking, review, or orders.  The
table is intentionally year-bounded so an unreviewed future year fails closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.storage.canonical import canonical_hash, utc_datetime


RESEARCH_SESSION_CALENDAR_SCHEMA = "options_copilot.research_session_calendar.v1"
RESEARCH_SESSION_CALENDAR_SOURCE = "STATIC_RESEARCH_CALENDAR_2026"
RESEARCH_SESSION_CALENDAR_EFFECTIVE_AT = datetime(
    2026,
    1,
    1,
    tzinfo=timezone.utc,
)
_CLOSED_DATES_2026 = frozenset(
    {
        date(2026, 1, 1),
        date(2026, 1, 19),
        date(2026, 2, 16),
        date(2026, 4, 3),
        date(2026, 5, 25),
        date(2026, 6, 19),
        date(2026, 7, 3),
        date(2026, 9, 7),
        date(2026, 11, 26),
        date(2026, 12, 25),
    }
)


@dataclass(frozen=True, slots=True)
class ResearchSessionCalendarSnapshot:
    status: str
    reason_codes: tuple[str, ...]
    week_start: date
    sessions: tuple[date, ...]
    source: str
    effective_at: datetime
    calendar_hash: str

    @property
    def ready(self) -> bool:
        return self.status == "READY" and bool(self.sessions)

    def verify_hash(self) -> bool:
        """Verify the bundled calendar without consulting a live provider."""

        payload = {
            "schema": RESEARCH_SESSION_CALENDAR_SCHEMA,
            "source": self.source,
            "effective_at": self.effective_at,
            "week_start": self.week_start,
            "closed_dates": tuple(sorted(_CLOSED_DATES_2026)),
            "sessions": self.sessions,
            "status": self.status,
            "reason_codes": self.reason_codes,
            "decision_authority": "SUPPORTING_ONLY",
        }
        return canonical_hash(payload) == self.calendar_hash


class BoundedResearchSessionCalendar:
    """Return only reviewed 2026 weekday/closure dates for research timing."""

    def snapshot(self, *, now: datetime) -> ResearchSessionCalendarSnapshot:
        checked = utc_datetime(now, field="now").astimezone(US_OPTIONS_TIMEZONE)
        week_start = checked.date() - timedelta(days=checked.date().weekday())
        covered = all(
            (week_start + timedelta(days=offset)).year == 2026
            for offset in range(7)
        )
        sessions = (
            tuple(
                day
                for offset in range(7)
                if (day := week_start + timedelta(days=offset)).weekday() < 5
                and day not in _CLOSED_DATES_2026
            )
            if covered
            else ()
        )
        status = "READY" if covered and sessions else "UNAVAILABLE"
        reasons = () if status == "READY" else ("RESEARCH_SESSION_YEAR_UNREVIEWED",)
        payload = {
            "schema": RESEARCH_SESSION_CALENDAR_SCHEMA,
            "source": RESEARCH_SESSION_CALENDAR_SOURCE,
            "effective_at": RESEARCH_SESSION_CALENDAR_EFFECTIVE_AT,
            "week_start": week_start,
            "closed_dates": tuple(sorted(_CLOSED_DATES_2026)),
            "sessions": sessions,
            "status": status,
            "reason_codes": reasons,
            "decision_authority": "SUPPORTING_ONLY",
        }
        return ResearchSessionCalendarSnapshot(
            status=status,
            reason_codes=reasons,
            week_start=week_start,
            sessions=sessions,
            source=RESEARCH_SESSION_CALENDAR_SOURCE,
            effective_at=RESEARCH_SESSION_CALENDAR_EFFECTIVE_AT,
            calendar_hash=canonical_hash(payload),
        )


__all__ = [
    "BoundedResearchSessionCalendar",
    "RESEARCH_SESSION_CALENDAR_EFFECTIVE_AT",
    "RESEARCH_SESSION_CALENDAR_SCHEMA",
    "RESEARCH_SESSION_CALENDAR_SOURCE",
    "ResearchSessionCalendarSnapshot",
]
