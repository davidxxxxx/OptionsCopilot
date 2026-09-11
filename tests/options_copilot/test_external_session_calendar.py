from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from options_copilot.market import (
    EXTERNAL_SESSION_CALENDAR_SOURCE,
    CalendarStatus,
    ExternalSessionCalendarError,
    ExternalSessionCalendarProvider,
    ExternalSessionCalendarPublisher,
)


NOW = datetime(2026, 8, 6, 13, 20, 10, tzinfo=timezone.utc)


def _payload(*, observed_at: datetime = NOW) -> dict[str, object]:
    return {
        "calendar_id": "ibkr-options-session-20260806",
        "observed_at": observed_at,
        "liquid_hours": "20260806:0930-1600",
        "trading_hours": "20260806:0930-1600",
        "timezone_id": "America/New_York",
        "source": EXTERNAL_SESSION_CALENDAR_SOURCE,
    }


def test_external_session_calendar_round_trip_is_ready_and_hash_bound(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session-calendar.json"
    published = ExternalSessionCalendarPublisher(
        path,
        clock=lambda: NOW,
    ).publish(_payload())
    loaded = ExternalSessionCalendarProvider(
        path,
        clock=lambda: NOW,
    ).snapshot(now=NOW)

    assert published.status is loaded.status is CalendarStatus.READY
    assert loaded.verify_hash()
    assert loaded.source == EXTERNAL_SESSION_CALENDAR_SOURCE
    assert loaded.session_for(NOW.astimezone().date()) is not None


def test_external_session_calendar_rejects_tamper_and_stale_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session-calendar.json"
    ExternalSessionCalendarPublisher(path, clock=lambda: NOW).publish(_payload())
    document = json.loads(path.read_text(encoding="utf-8"))
    document["liquid_hours"] = "20260806:CLOSED"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ExternalSessionCalendarError, match="hash"):
        ExternalSessionCalendarProvider(
            path,
            clock=lambda: NOW,
        ).snapshot(now=NOW)

    stale_path = tmp_path / "stale-calendar.json"
    stale_at = NOW - timedelta(minutes=6)
    ExternalSessionCalendarPublisher(
        stale_path,
        clock=lambda: stale_at,
    ).publish(_payload(observed_at=stale_at))
    with pytest.raises(ExternalSessionCalendarError, match="stale"):
        ExternalSessionCalendarProvider(
            stale_path,
            clock=lambda: NOW,
        ).snapshot(now=NOW)
