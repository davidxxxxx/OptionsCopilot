"""Timezone-aware event calendar windows for earnings and macro research."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

from .models import CalendarEvent


class CalendarWindow(str, Enum):
    THIS_WEEK = "THIS_WEEK"
    NEXT_WEEK = "NEXT_WEEK"
    FUTURE_TWO_WEEKS = "FUTURE_TWO_WEEKS"


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return value


class EventCalendar:
    def __init__(self, events: Iterable[CalendarEvent] = ()) -> None:
        indexed: dict[str, CalendarEvent] = {}
        for event in events:
            if event.event_id in indexed:
                raise ValueError(f"duplicate calendar event_id: {event.event_id}")
            indexed[event.event_id] = event
        self._events = tuple(sorted(indexed.values(), key=lambda item: item.scheduled_at))

    def events(self, window: CalendarWindow, now: datetime) -> tuple[CalendarEvent, ...]:
        checked = _aware(now)
        try:
            selected = CalendarWindow(window)
        except ValueError as exc:
            raise ValueError(f"invalid calendar window: {window!r}") from exc
        week_start = checked.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=checked.weekday())
        if selected is CalendarWindow.THIS_WEEK:
            start, end = week_start, week_start + timedelta(days=7)
        elif selected is CalendarWindow.NEXT_WEEK:
            start, end = week_start + timedelta(days=7), week_start + timedelta(days=14)
        else:
            start, end = checked, checked + timedelta(days=14)
        return tuple(event for event in self._events if start <= event.scheduled_at < end)

    def as_payload(self, window: CalendarWindow, now: datetime) -> dict[str, object]:
        return {
            "window": CalendarWindow(window).value,
            "generated_at": _aware(now).astimezone(timezone.utc).isoformat(),
            "events": [event.as_dict() for event in self.events(window, now)],
        }
