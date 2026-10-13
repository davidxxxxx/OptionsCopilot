from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from copilot.analysis.tagging import Impact, NewsTag
from copilot.config import EventSettings, ScheduleSettings
from copilot.news.macro_calendar import MacroEvent
from copilot.news.models import NewsItem
from copilot.scheduler import (
    RunManager,
    Scheduler,
    due_slot,
    event_active,
    fast_poll_window,
    is_trading_day,
    nyse_holidays,
    upcoming_slots,
)
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
            assert db.execute("PRAGMA user_version").fetchone()[0] == 5
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


class _EventRuns:
    def __init__(self) -> None:
        self.current = None
        self.blocking = []
        self.background = []

    @property
    def busy(self) -> bool:
        return False

    def run_blocking(self, trigger, slot, reason=None):
        self.blocking.append((trigger, slot, reason))
        return 1

    def start_background(self, trigger, slot, reason=None):
        self.background.append((trigger, slot, reason))
        return True


def _macro(day: date, hour: int = 8, minute: int = 30) -> MacroEvent:
    return MacroEvent(
        source="tradingview",
        source_id=f"cpi-{day}",
        event_time=ny(day, hour, minute),
        country="US",
        title="Core Inflation Rate MoM",
        category="cpi",
        importance=3,
        actual="0.4",
        forecast="0.2",
    )


def _save_macro(store, event: MacroEvent) -> None:
    start = datetime.combine(event.event_time.astimezone(NY).date(), datetime.min.time(), NY)
    store.replace_macro_events("tradingview", start, start + timedelta(days=1), [event])


def test_fast_poll_window_boundaries_are_inclusive() -> None:
    event = _macro(date(2026, 10, 9))
    settings = EventSettings(window_before_minutes=2, window_after_minutes=15)
    assert fast_poll_window(event.event_time - timedelta(minutes=2), [event], settings)
    assert fast_poll_window(event.event_time + timedelta(minutes=15), [event], settings)
    assert not fast_poll_window(event.event_time - timedelta(minutes=2, seconds=1), [event], settings)
    assert not fast_poll_window(event.event_time + timedelta(minutes=15, seconds=1), [event], settings)


def test_scheduler_uses_fast_jin10_callback_inside_release_window(store) -> None:
    event = _macro(date(2026, 10, 9))
    _save_macro(store, event)
    calls = []
    scheduler = Scheduler(
        ScheduleSettings(enabled=False),
        store,
        _EventRuns(),
        poll_news=lambda: calls.append("normal"),
        poll_fast=lambda: calls.append("jin10"),
        events=EventSettings(),
        clock=lambda: event.event_time,
    )

    scheduler.tick()

    assert calls == ["jin10"]


def test_release_trigger_fires_once_after_delay(store) -> None:
    event = _macro(date(2026, 10, 9))
    _save_macro(store, event)
    runs = _EventRuns()
    clock = [event.event_time + timedelta(minutes=2)]
    scheduler = Scheduler(
        ScheduleSettings(enabled=False),
        store,
        runs,
        poll_fast=lambda: None,
        events=EventSettings(),
        clock=lambda: clock[0],
    )

    assert scheduler.tick() == "event"
    clock[0] += timedelta(seconds=30)
    assert scheduler.tick() is None
    assert len(runs.background) == 1
    assert runs.background[0][2]["kind"] == "release"


def test_breaking_news_trigger_requires_importance_and_universe_impact(store) -> None:
    item = NewsItem(
        source="jin10",
        source_id="breaking",
        published_at=ny(date(2026, 10, 9), 10, 0),
        title="重大利好",
    )
    store.add_news([item])
    saved = store.recent_news(datetime(2020, 1, 1, tzinfo=timezone.utc))[0]
    store.save_news_tags([
        NewsTag(
            news_id=saved.id,
            category="macro_data",
            importance=3,
            summary="重大利好",
            impacts=(Impact("SPY", 1, 3, "days", "up"),),
        )
    ], model="fake")
    store.set_kv("event_news_highwater", 0)
    runs = _EventRuns()
    scheduler = Scheduler(
        ScheduleSettings(enabled=False),
        store,
        runs,
        poll_news=lambda: None,
        events=EventSettings(),
        universe=("SPY",),
        clock=lambda: ny(date(2026, 10, 9), 10, 1),
    )

    assert scheduler.tick() == "event"
    assert runs.background[0][2]["kind"] == "news"
    assert runs.background[0][2]["news_ids"] == [saved.id]


def test_cooldown_coalesces_pending_reasons(store) -> None:
    day = date(2026, 10, 9)
    previous = store.create_run(trading_date=day, slot="event", trigger="event")
    store.finish_run(previous, status="ok", stages={})
    _set_finished(store, previous, ny(day, 10, 0))
    store.set_kv("event_triggers_pending", [
        {"kind": "news", "title": "A", "category": "macro_data", "news_ids": [1], "at": ny(day, 10, 5).isoformat()},
        {"kind": "news", "title": "B", "category": "macro_data", "news_ids": [2], "at": ny(day, 10, 6).isoformat()},
    ])
    runs = _EventRuns()
    clock = [ny(day, 10, 10)]
    scheduler = Scheduler(
        ScheduleSettings(enabled=False), store, runs, poll_news=lambda: None,
        events=EventSettings(cooldown_minutes=20), clock=lambda: clock[0],
        news_poll_minutes=1,
    )
    assert scheduler.tick() is None
    assert runs.background == []
    clock[0] = ny(day, 10, 21)
    assert scheduler.tick() == "event"
    assert runs.background[0][2]["kind"] == "multiple"
    assert runs.background[0][2]["news_ids"] == [1, 2]


def test_daily_cap_blocks_event_run(store) -> None:
    day = date(2026, 10, 9)
    for index in range(2):
        run_id = store.create_run(trading_date=day, slot="event", trigger="event")
        store.finish_run(run_id, status="ok", stages={})
        _set_finished(store, run_id, ny(day, 8, index))
    store.set_kv("event_triggers_pending", [
        {"kind": "news", "title": "A", "category": "macro_data", "news_ids": [1], "at": ny(day, 10).isoformat()}
    ])
    runs = _EventRuns()
    scheduler = Scheduler(
        ScheduleSettings(enabled=False), store, runs, poll_news=lambda: None,
        events=EventSettings(max_event_runs_per_day=2, cooldown_minutes=0),
        clock=lambda: ny(day, 10, 0),
    )
    assert scheduler.tick() is None
    assert runs.background == []


def test_event_triggers_inactive_outside_hours_and_on_weekends() -> None:
    settings = EventSettings(active_start_et="07:00", active_end_et="20:00")
    assert not event_active(ny(date(2026, 10, 9), 6, 59), settings)
    assert event_active(ny(date(2026, 10, 9), 7, 0), settings)
    assert event_active(ny(date(2026, 10, 9), 20, 0), settings)
    assert not event_active(ny(date(2026, 10, 9), 20, 1), settings)
    assert not event_active(ny(date(2026, 10, 10), 10, 0), settings)


def test_scheduled_slot_keeps_priority_over_pending_event(store) -> None:
    day = date(2026, 10, 9)
    store.set_kv("event_triggers_pending", [
        {"kind": "news", "title": "A", "category": "macro_data", "news_ids": [1], "at": ny(day, 9).isoformat()}
    ])
    runs = _EventRuns()
    scheduler = Scheduler(
        SCHEDULE,
        store,
        runs,
        poll_news=lambda: None,
        events=EventSettings(),
        clock=lambda: ny(day, 9, 5),
    )

    assert scheduler.tick() == "09:00"
    assert runs.blocking == [("schedule", "09:00", None)]
    assert runs.background == []
