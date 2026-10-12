from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from copilot.config import ScheduleSettings
from copilot.news.models import NewsItem
from copilot.scheduler import RunManager, Scheduler, due_slot, is_trading_day, nyse_holidays, upcoming_slots
from copilot.store import Store

NY = ZoneInfo("America/New_York")
SCHEDULE = ScheduleSettings(run_times_et=("09:00", "09:45", "12:30", "15:00"), retry_minutes=15, max_attempts_per_slot=3)


def ny(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=NY)


def test_nyse_holidays_2026() -> None:
    holidays = nyse_holidays(2026)
    for expected in (date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
                     date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
                     date(2026, 11, 26), date(2026, 12, 25)):
        assert expected in holidays
    assert not is_trading_day(date(2026, 10, 10))  # Saturday
    assert is_trading_day(date(2026, 10, 9))


def test_due_slot_catches_up_to_current_window_only(store) -> None:
    day = date(2026, 10, 9)
    assert due_slot(ny(day, 8, 59), SCHEDULE, store) is None
    assert due_slot(ny(day, 9, 5), SCHEDULE, store) == "09:00"
    # Machine woke up at 13:10: the 12:30 window runs, earlier slots are not replayed.
    assert due_slot(ny(day, 13, 10), SCHEDULE, store) == "12:30"
    assert due_slot(ny(day, 16, 20), SCHEDULE, store) is None
    assert due_slot(ny(date(2026, 10, 10), 10, 0), SCHEDULE, store) is None


def _set_finished(store, run_id: int, moment: datetime) -> None:
    with store._tx() as db:
        db.execute("UPDATE runs SET finished_at=? WHERE id=?", (moment.astimezone(timezone.utc).isoformat(), run_id))


def test_partial_runs_retry_after_spacing_and_stop_after_success(store) -> None:
    day = date(2026, 10, 9)
    run_id = store.create_run(trading_date=day, slot="09:45", trigger="schedule")
    assert due_slot(ny(day, 9, 50), SCHEDULE, store) is None  # still running
    store.finish_run(run_id, status="partial", stages={})
    _set_finished(store, run_id, ny(day, 9, 52))
    assert due_slot(ny(day, 10, 0), SCHEDULE, store) is None  # inside retry spacing
    assert due_slot(ny(day, 10, 8), SCHEDULE, store) == "09:45"
    ok_id = store.create_run(trading_date=day, slot="09:45", trigger="schedule")
    store.finish_run(ok_id, status="ok", stages={})
    assert due_slot(ny(day, 10, 30), SCHEDULE, store) is None


def test_attempt_limit(store) -> None:
    day = date(2026, 10, 9)
    for _ in range(3):
        run_id = store.create_run(trading_date=day, slot="12:30", trigger="schedule")
        store.finish_run(run_id, status="failed", stages={}, error="x")
    assert due_slot(ny(day, 14, 0), SCHEDULE, store) is None


def test_upcoming_slots_skip_weekend() -> None:
    friday_evening = ny(date(2026, 10, 9), 17, 0)
    slots = upcoming_slots(friday_evening, SCHEDULE, count=2)
    assert slots[0] == ny(date(2026, 10, 12), 9, 0)


def test_run_manager_serialises_runs() -> None:
    calls = []
    manager = RunManager(lambda trigger, slot: calls.append((trigger, slot)) or 7)
    assert manager.run_blocking("manual", "manual") == 7
    assert not manager.busy
    assert calls == [("manual", "manual")]


def test_scheduler_tick_runs_due_slot_then_polls_news(store) -> None:
    day = date(2026, 10, 9)
    runs = []
    polled = []

    def fake_run(trigger, slot):
        run_id = store.create_run(trading_date=day, slot=slot, trigger=trigger)
        store.finish_run(run_id, status="ok", stages={})
        runs.append(slot)
        return run_id

    clock_value = [ny(day, 9, 50)]
    scheduler = Scheduler(SCHEDULE, store, RunManager(fake_run), poll_news=lambda: polled.append(1),
                          clock=lambda: clock_value[0])
    assert scheduler.tick() == "09:45"
    assert scheduler.tick() is None  # slot done; news poll not yet due (just polled after run)
    clock_value[0] = ny(day, 10, 10)
    scheduler.tick()
    assert runs == ["09:45"] and polled == [1]
    assert scheduler.status()["enabled"] is True


def test_interrupted_runs_are_closed(store) -> None:
    store.create_run(trading_date=date(2026, 10, 9), slot="09:00", trigger="schedule")
    assert store.fail_interrupted_runs() == 1
    assert store.list_runs()[0]["error"] == "interrupted"


def test_regular_session_detection() -> None:
    from copilot.market.calendar import is_regular_session

    assert is_regular_session(ny(date(2026, 10, 9), 10, 0))
    assert not is_regular_session(ny(date(2026, 10, 9), 9, 0))
    assert not is_regular_session(ny(date(2026, 10, 9), 16, 30))
    assert not is_regular_session(ny(date(2026, 11, 26), 11, 0))  # Thanksgiving


def test_schema_v2_upgrade_preserves_rows(tmp_path: Path) -> None:
    path = tmp_path / "upgrade.sqlite3"
    old = Store(path)
    old.add_news([NewsItem(source="jin10", source_id="old", published_at=datetime.now(timezone.utc), title="旧新闻")])
    old.close()
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=2")
    upgraded = Store(path)
    try:
        assert upgraded.recent_news(datetime(2000, 1, 1, tzinfo=timezone.utc))[0].title == "旧新闻"
        with sqlite3.connect(path) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 3
    finally:
        upgraded.close()


def test_tracked_entry_is_immutable_and_open_key_is_unique(store) -> None:
    entry = {"idea": {"symbol": "SPY"}}
    tracked_id = store.add_tracked(
        idea_key="key", symbol="SPY", structure="vertical", origin="news",
        params_version="v1", entry_run_id=1, entered_at="2026-10-09T14:00:00+00:00", entry=entry,
    )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with store._tx() as db:
            db.execute("UPDATE tracked_ideas SET entry=? WHERE id=?", ('{}', tracked_id))
    with pytest.raises(sqlite3.IntegrityError):
        store.add_tracked(
            idea_key="key", symbol="SPY", structure="vertical", origin="news",
            params_version="v1", entry_run_id=2, entered_at="2026-10-09T15:00:00+00:00", entry=entry,
        )
    store.close_tracked(tracked_id, {"code": "take_profit"})
    assert store.add_tracked(
        idea_key="key", symbol="SPY", structure="vertical", origin="news",
        params_version="v1", entry_run_id=3, entered_at="2026-10-09T16:00:00+00:00", entry=entry,
    ) > tracked_id
