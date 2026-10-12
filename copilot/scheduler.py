"""In-process scheduler: run the pipeline at New York slot times, catch up after restarts.

A slot is due from its start time until the next slot begins (the last one until
16:15 New York). Missed earlier slots are not replayed — the current window's run
supersedes them. A partial or failed run is retried every ``retry_minutes`` until
it succeeds or ``max_attempts_per_slot`` is reached.
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from copilot.config import ScheduleSettings
from copilot.market.calendar import is_trading_day, nyse_holidays
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

    def __init__(self, run: Callable[[str, str], int]) -> None:
        self._run = run
        self._lock = threading.Lock()
        self.current: dict[str, Any] | None = None

    @property
    def busy(self) -> bool:
        return self._lock.locked()

    def run_blocking(self, trigger: str, slot: str) -> int | None:
        if not self._lock.acquire(blocking=False):
            return None
        try:
            self.current = {
                "trigger": trigger,
                "slot": slot,
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            return self._run(trigger, slot)
        finally:
            self.current = None
            self._lock.release()

    def start_background(self, trigger: str, slot: str) -> bool:
        if self.busy:
            return False
        threading.Thread(
            target=self.run_blocking, args=(trigger, slot), name="copilot-run", daemon=True
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
        news_poll_minutes: int = 15,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        tick_seconds: float = 30.0,
    ) -> None:
        self.schedule = schedule
        self.store = store
        self.runs = runs
        self.poll_news = poll_news
        self.news_poll_interval = timedelta(minutes=news_poll_minutes)
        self.clock = clock
        self.tick_seconds = tick_seconds
        self.last_news_poll: datetime | None = None
        self.last_tick: datetime | None = None
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
        if self.poll_news and (
            self.last_news_poll is None or now - self.last_news_poll >= self.news_poll_interval
        ):
            self.last_news_poll = now
            try:
                self.poll_news()
            except Exception:
                log.exception("news poll failed")
        return None

    def status(self) -> dict[str, Any]:
        now = self.clock()
        return {
            "enabled": self.schedule.enabled,
            "run_times_et": list(self.schedule.run_times_et),
            "next_runs": [moment.isoformat() for moment in upcoming_slots(now, self.schedule)],
            "trading_day": is_trading_day(now.astimezone(NEW_YORK).date()),
            "last_tick": self.last_tick.isoformat() if self.last_tick else None,
            "last_news_poll": self.last_news_poll.isoformat() if self.last_news_poll else None,
            "running": self.runs.current,
        }


__all__ = [
    "RunManager",
    "Scheduler",
    "due_slot",
    "is_trading_day",
    "nyse_holidays",
    "upcoming_slots",
]
