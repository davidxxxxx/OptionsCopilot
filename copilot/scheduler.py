"""In-process scheduler: run the pipeline at New York slot times, catch up after restarts.

A slot is due from its start time until the next slot begins (the last one until
16:15 New York). Missed earlier slots are not replayed — the current window's run
supersedes them. A partial or failed run is retried every ``retry_minutes`` until
it succeeds or ``max_attempts_per_slot`` is reached.
"""
from __future__ import annotations

import logging
import inspect
import threading
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from copilot.config import EventSettings, ScheduleSettings
from copilot.market.calendar import is_trading_day, nyse_holidays
from copilot.news.macro_calendar import CATEGORY_NAMES, MAJOR_CATEGORIES, MacroEvent
from copilot.store import Store

log = logging.getLogger(__name__)
NEW_YORK = ZoneInfo("America/New_York")
SESSION_END = time(16, 15)


def _slot_windows(day: date, schedule: ScheduleSettings) -> list[tuple[str, datetime, datetime]]:
    times = sorted(time.fromisoformat(value) for value in schedule.run_times_et)
    windows = []
    for index, start in enumerate(times):
        end = times[index + 1] if index + 1 < len(times) else SESSION_END
        windows.append((
            start.strftime("%H:%M"),
            datetime.combine(day, start, NEW_YORK),
            datetime.combine(day, end, NEW_YORK),
        ))
    return windows


def due_slot(now: datetime, schedule: ScheduleSettings, store: Store) -> str | None:
    local = now.astimezone(NEW_YORK)
    if not schedule.enabled or not is_trading_day(local.date()):
        return None
    for slot, start, end in _slot_windows(local.date(), schedule):
        if not start <= local < end:
            continue
        runs = store.slot_runs(local.date(), slot)
        if any(run["status"] == "ok" for run in runs):
            return None
        if any(run["status"] == "running" for run in runs):
            return None
        if len(runs) >= schedule.max_attempts_per_slot:
            return None
        if runs:
            finished = runs[-1]["finished_at"]
            if finished and local - datetime.fromisoformat(finished) < timedelta(
                minutes=schedule.retry_minutes
            ):
                return None
        return slot
    return None


def upcoming_slots(now: datetime, schedule: ScheduleSettings, count: int = 4) -> list[datetime]:
    local = now.astimezone(NEW_YORK)
    found: list[datetime] = []
    day = local.date()
    for _ in range(14):
        if is_trading_day(day):
            for _slot, start, _end in _slot_windows(day, schedule):
                if start > local:
                    found.append(start)
                    if len(found) >= count:
                        return found
        day += timedelta(days=1)
    return found


class RunManager:
    """Serialises pipeline runs: one at a time, from the scheduler or the GUI."""

    def __init__(self, run: Callable[..., int]) -> None:
        self._run = run
        try:
            inspect.signature(run).bind("manual", "manual", None)
            self._accepts_reason = True
        except TypeError:
            self._accepts_reason = False
        self._lock = threading.Lock()
        self.current: dict[str, Any] | None = None

    @property
    def busy(self) -> bool:
        return self._lock.locked()

    def run_blocking(
        self, trigger: str, slot: str, reason: dict[str, Any] | None = None
    ) -> int | None:
        if not self._lock.acquire(blocking=False):
            return None
        return self._run_acquired(trigger, slot, reason)

    def _run_acquired(
        self, trigger: str, slot: str, reason: dict[str, Any] | None
    ) -> int | None:
        try:
            self.current = {
                "trigger": trigger,
                "slot": slot,
                "reason": reason,
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            if self._accepts_reason:
                return self._run(trigger, slot, reason)
            return self._run(trigger, slot)
        finally:
            self.current = None
            self._lock.release()

    def start_background(
        self, trigger: str, slot: str, reason: dict[str, Any] | None = None
    ) -> bool:
        if not self._lock.acquire(blocking=False):
            return False
        threading.Thread(
            target=self._run_acquired,
            args=(trigger, slot, reason),
            name="copilot-run",
            daemon=True,
        ).start()
        return True


class Scheduler:
    def __init__(
        self,
        schedule: ScheduleSettings,
        store: Store,
        runs: RunManager,
        *,
        poll_news: Callable[[], None] | None = None,
        poll_fast: Callable[[], None] | None = None,
        events: EventSettings | None = None,
        universe: Sequence[str] = (),
        refresh_event_study: Callable[[], None] | None = None,
        news_poll_minutes: int = 15,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        tick_seconds: float = 30.0,
    ) -> None:
        self.schedule = schedule
        self.store = store
        self.runs = runs
        self.poll_news = poll_news
        self.poll_fast = poll_fast
        self.events = events or EventSettings(enabled=False)
        self.universe = set(universe)
        self.refresh_event_study = refresh_event_study
        self.news_poll_interval = timedelta(minutes=news_poll_minutes)
        self.clock = clock
        self.tick_seconds = tick_seconds
        self.last_news_poll: datetime | None = None
        self.last_fast_poll: datetime | None = None
        self.last_tick: datetime | None = None
        self.last_study_refresh_day: date | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, name="copilot-scheduler", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("scheduler tick failed")
            self._stop.wait(self.tick_seconds)

    def tick(self) -> str | None:
        now = self.clock()
        self.last_tick = now
        if self.runs.busy:
            return None
        slot = due_slot(now, self.schedule, self.store)
        if slot is not None:
            log.info("scheduled run for slot %s", slot)
            self.runs.run_blocking("schedule", slot)
            self.last_news_poll = self.clock()
            return slot
        local = now.astimezone(NEW_YORK)
        if (
            self.refresh_event_study is not None
            and local.time() >= time(18, 0)
            and self.last_study_refresh_day != local.date()
        ):
            self.last_study_refresh_day = local.date()
            self.refresh_event_study()
        fast = self.events.enabled and fast_poll_window(
            now, self._today_macro_events(now), self.events
        )
        polled = False
        if fast and self.poll_fast and (
            self.last_fast_poll is None
            or (now - self.last_fast_poll).total_seconds() >= self.events.fast_poll_seconds
        ):
            self.last_fast_poll = now
            polled = True
            try:
                self.poll_fast()
            except Exception:
                log.exception("fast news poll failed")
        elif self.poll_news and (
            self.last_news_poll is None or now - self.last_news_poll >= self.news_poll_interval
        ):
            self.last_news_poll = now
            polled = True
            try:
                self.poll_news()
            except Exception:
                log.exception("news poll failed")
        if polled and self.events.enabled:
            reason = self._pending_reason(now)
            if reason is not None and self._event_run_allowed(now):
                if self.runs.start_background("event", "event", reason):
                    self.store.set_kv("event_triggers_pending", [])
                    return "event"
        return None

    def _today_macro_events(self, now: datetime) -> list[MacroEvent]:
        local = now.astimezone(NEW_YORK)
        start = datetime.combine(local.date(), time.min, NEW_YORK).astimezone(timezone.utc)
        end = start + timedelta(days=1)
        return self.store.macro_events_between(start, end, min_importance=self.events.trigger_importance)

    def _pending_reason(self, now: datetime) -> dict[str, Any] | None:
        if not event_active(now, self.events):
            return None
        pending = list(self.store.get_kv("event_triggers_pending") or [])
        done = set(self.store.get_kv("event_triggers_done") or [])
        for event in self._today_macro_events(now):
            if event.category not in MAJOR_CATEGORIES:
                continue
            local = event.event_time.astimezone(NEW_YORK)
            key = f"{local.date().isoformat()}:{event.category}"
            if key in done or now < event.event_time + timedelta(minutes=self.events.release_delay_minutes):
                continue
            pending.append({
                "kind": "release",
                "title": f"美国 {CATEGORY_NAMES[event.category]} 公布",
                "category": event.category,
                "news_ids": [],
                "at": now.isoformat(),
            })
            done.add(key)
        highwater = self.store.get_kv("event_news_highwater")
        tagged = self.store.tagged_news_after_id(int(highwater or 0))
        if highwater is None:
            if tagged:
                self.store.set_kv("event_news_highwater", max(item.id or 0 for item, _tag in tagged))
            tagged = []
        elif tagged:
            self.store.set_kv("event_news_highwater", max(item.id or 0 for item, _tag in tagged))
        for item, tag in tagged:
            impacts = [impact for impact in tag.impacts if impact.symbol in self.universe]
            if tag.importance < self.events.trigger_importance or not impacts:
                continue
            pending.append({
                "kind": "news",
                "title": tag.summary or item.title,
                "category": tag.category,
                "news_ids": [item.id],
                "at": now.isoformat(),
            })
        pending = _unique_reasons(pending)
        self.store.set_kv("event_triggers_done", sorted(done))
        self.store.set_kv("event_triggers_pending", pending)
        return _merge_reasons(pending, now) if pending else None

    def _event_run_allowed(self, now: datetime) -> bool:
        local = now.astimezone(NEW_YORK)
        runs = self.store.slot_runs(local.date(), "event")
        if len(runs) >= self.events.max_event_runs_per_day:
            return False
        finished = [run for run in runs if run.get("finished_at")]
        if not finished:
            return True
        latest = datetime.fromisoformat(finished[-1]["finished_at"])
        return now - latest.astimezone(now.tzinfo or timezone.utc) >= timedelta(
            minutes=self.events.cooldown_minutes
        )

    def status(self) -> dict[str, Any]:
        now = self.clock()
        return {
            "enabled": self.schedule.enabled,
            "run_times_et": list(self.schedule.run_times_et),
            "next_runs": [moment.isoformat() for moment in upcoming_slots(now, self.schedule)],
            "trading_day": is_trading_day(now.astimezone(NEW_YORK).date()),
            "last_tick": self.last_tick.isoformat() if self.last_tick else None,
            "last_news_poll": self.last_news_poll.isoformat() if self.last_news_poll else None,
            "last_fast_poll": self.last_fast_poll.isoformat() if self.last_fast_poll else None,
            "fast_poll": self.events.enabled and fast_poll_window(
                now, self._today_macro_events(now), self.events
            ),
            "running": self.runs.current,
        }


def fast_poll_window(now: datetime, events: Sequence[MacroEvent], settings: EventSettings) -> bool:
    return any(
        event.category in MAJOR_CATEGORIES
        and event.importance >= settings.trigger_importance
        and event.event_time - timedelta(minutes=settings.window_before_minutes)
        <= now
        <= event.event_time + timedelta(minutes=settings.window_after_minutes)
        for event in events
    )


def event_active(now: datetime, settings: EventSettings) -> bool:
    local = now.astimezone(NEW_YORK)
    return bool(
        settings.enabled
        and is_trading_day(local.date())
        and time.fromisoformat(settings.active_start_et)
        <= local.time()
        <= time.fromisoformat(settings.active_end_et)
    )


def _unique_reasons(reasons: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for reason in reasons:
        key = (
            reason.get("kind"), reason.get("title"), reason.get("category"),
            tuple(reason.get("news_ids") or ()),
        )
        unique[key] = reason
    return list(unique.values())


def _merge_reasons(reasons: Sequence[dict[str, Any]], now: datetime) -> dict[str, Any]:
    if len(reasons) == 1:
        return dict(reasons[0])
    return {
        "kind": "multiple",
        "title": "；".join(str(reason.get("title") or "事件") for reason in reasons),
        "category": "multiple",
        "news_ids": sorted({
            int(news_id) for reason in reasons for news_id in reason.get("news_ids") or () if news_id
        }),
        "at": now.isoformat(),
        "reasons": list(reasons),
    }


__all__ = [
    "RunManager",
    "Scheduler",
    "due_slot",
    "event_active",
    "fast_poll_window",
    "is_trading_day",
    "nyse_holidays",
    "upcoming_slots",
]
