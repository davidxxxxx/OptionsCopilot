"""Background orchestration for storage-only intraday data collection."""
from __future__ import annotations

import logging
import threading
import time as time_module
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from copilot.config import Settings
from copilot.market.calendar import is_trading_day
from copilot.market.ibkr import IbkrUnavailable
from copilot.market.intraday import (
    NasdaqIntraday,
    fetch_jin10_24h,
    ibkr_intraday_history,
)
from copilot.news.jin10 import Jin10Error
from copilot.scheduler import RunManager
from copilot.store import Store

log = logging.getLogger(__name__)
NEW_YORK = ZoneInfo("America/New_York")
UTC = timezone.utc
SOURCES = ("nasdaq", "jin10", "ibkr")


@dataclass
class IntradayArchive:
    settings: Settings
    store: Store
    runs: RunManager
    nasdaq: Any = None
    jin10: Any = None
    market_factory: Callable[[], Any] | None = None
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    sleep: Callable[[float], None] = time_module.sleep
    thread_start: Callable[[str, Callable[[], None]], None] | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _active: str | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.nasdaq is None:
            self.nasdaq = NasdaqIntraday()

    def tick(self, now: datetime | None = None, *, startup: bool = False) -> str | None:
        """Decide one due job and launch it without blocking the scheduler tick."""

        now = now or self.clock()
        job = next_intraday_job(
            now, self.settings, self.store, self.runs, startup=startup,
        )
        if job is None or not self._lock.acquire(blocking=False):
            return None
        self._active = job
        target = lambda: self._run(job, now)
        if self.thread_start is not None:
            self.thread_start(f"copilot-intraday-{job}", target)
        else:
            threading.Thread(
                target=target,
                name=f"copilot-intraday-{job}",
                daemon=True,
            ).start()
        return job

    def status(self) -> dict[str, Any]:
        stored = {row["source"]: row for row in self.store.intraday_status()}
        jobs = self.store.get_kv("intraday_job_status") or {}
        sources = []
        for source in SOURCES:
            summary = stored.get(source, {
                "source": source,
                "symbols": 0,
                "days": 0,
                "last_day": None,
                "rows": 0,
                "last_fetch": None,
            })
            job = jobs.get(source) or {}
            sources.append({
                **summary,
                "last_error": job.get("error"),
                "last_job": job.get("finished_at"),
                "job_status": job.get("status"),
            })
        return {"enabled": self.settings.intraday.enabled, "active": self._active, "sources": sources}

    def _run(self, job: str, now: datetime) -> None:
        try:
            started = self.clock().astimezone(UTC).isoformat(timespec="seconds")
            result: dict[str, Any]
            try:
                if job == "nasdaq":
                    result = archive_nasdaq_day(
                        self.store, self.settings, self.nasdaq, now=now,
                    )
                elif job == "jin10":
                    result = archive_jin10_day(
                        self.store, self.settings, self.jin10, now=now,
                    )
                else:
                    result = backfill_ibkr_intraday(
                        self.store,
                        self.settings,
                        self.runs,
                        self.market_factory,
                        now=now,
                        sleep=self.sleep,
                    )
            except Exception as exc:
                log.exception("intraday %s archive failed", job)
                result = {"status": "failed", "rows": 0, "error": _safe_error(exc)}
            result = {
                **result,
                "started_at": started,
                "finished_at": self.clock().astimezone(UTC).isoformat(timespec="seconds"),
            }
            jobs = dict(self.store.get_kv("intraday_job_status") or {})
            jobs[job] = result
            self.store.set_kv("intraday_job_status", jobs)
        finally:
            self._active = None
            self._lock.release()


def next_intraday_job(
    now: datetime,
    settings: Settings,
    store: Store,
    runs: RunManager,
    *,
    startup: bool = False,
) -> str | None:
    if not settings.intraday.enabled:
        return None
    local = now.astimezone(NEW_YORK)
    if (
        is_trading_day(local.date())
        and local.time() >= time.fromisoformat(settings.intraday.nasdaq_after_et)
        and store.get_kv("intraday_nasdaq_last_day") != local.date().isoformat()
    ):
        return "nasdaq"
    last_jin10 = store.get_kv("intraday_jin10_last_run")
    if startup or last_jin10 is None or (
        now.astimezone(UTC) - datetime.fromisoformat(str(last_jin10)).astimezone(UTC)
        >= timedelta(hours=settings.intraday.jin10_interval_hours)
    ):
        return "jin10"
    if (
        settings.intraday.ibkr_backfill
        and local.time() >= time(20, 30)
        and store.get_kv("intraday_ibkr_last_day") != local.date().isoformat()
        and not runs.busy
    ):
        return "ibkr"
    return None


def select_nasdaq_symbols(store: Store, settings: Settings, day: date) -> tuple[str, ...]:
    base = settings.intraday.nasdaq_symbols or settings.events.study_symbols
    start = datetime.combine(day, time.min, NEW_YORK).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time.min, NEW_YORK).astimezone(UTC)
    counts: Counter[str] = Counter()
    universe = set(settings.universe)
    for item, tag in store.tagged_news(start, min_importance=2):
        if item.published_at >= end:
            continue
        counts.update({impact.symbol for impact in tag.impacts if impact.symbol in universe})
    news = [
        symbol for symbol, _count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        [:settings.intraday.max_news_symbols]
    ]
    tracked = [row["symbol"] for row in store.open_tracked()]
    selected: dict[str, None] = {}
    for symbol in (*base, *news, *tracked):
        selected.setdefault(symbol.upper(), None)
    return tuple(selected)


def archive_nasdaq_day(
    store: Store,
    settings: Settings,
    client: Any,
    *,
    now: datetime,
) -> dict[str, Any]:
    local_day = now.astimezone(NEW_YORK).date()
    failures: list[str] = []
    consecutive = 0
    rows = 0
    attempted = 0
    for symbol in select_nasdaq_symbols(store, settings, local_day):
        attempted += 1
        try:
            bars = client.fetch(symbol)
            if not bars:
                raise ValueError("EMPTY")
            rows += store.merge_intraday("nasdaq", symbol, bars)
            consecutive = 0
        except Exception as exc:
            consecutive += 1
            failures.append(f"{symbol}:{_safe_error(exc)}")
            if consecutive >= 3:
                break
    store.set_kv("intraday_nasdaq_last_day", local_day.isoformat())
    return {
        "status": "partial" if failures else "ok",
        "symbols": attempted,
        "rows": rows,
        "error": "；".join(failures) or None,
    }


def archive_jin10_day(
    store: Store,
    settings: Settings,
    client: Any,
    *,
    now: datetime,
) -> dict[str, Any]:
    unsupported = set(store.get_kv("intraday_jin10_unsupported") or [])
    failures: list[str] = []
    rows = 0
    attempted = 0
    if client is not None:
        for code in settings.intraday.jin10_codes:
            if code in unsupported:
                continue
            attempted += 1
            try:
                bars = fetch_jin10_24h(client, code, now=now)
                rows += store.merge_intraday("jin10", code, bars)
            except Jin10Error as exc:
                if str(exc) == "HTTP_400":
                    unsupported.add(code)
                    log.warning("Jin10 intraday code unsupported: %s", code)
                else:
                    failures.append(f"{code}:{exc}")
            except Exception as exc:
                failures.append(f"{code}:{_safe_error(exc)}")
    store.set_kv("intraday_jin10_unsupported", sorted(unsupported))
    store.set_kv("intraday_jin10_last_run", now.astimezone(UTC).isoformat(timespec="seconds"))
    return {
        "status": "skipped" if client is None else "partial" if failures else "ok",
        "symbols": attempted,
        "rows": rows,
        "error": "；".join(failures) or None,
    }


def backfill_ibkr_intraday(
    store: Store,
    settings: Settings,
    runs: RunManager,
    market_factory: Callable[[], Any] | None,
    *,
    now: datetime,
    sleep: Callable[[float], None] = time_module.sleep,
) -> dict[str, Any]:
    local_day = now.astimezone(NEW_YORK).date()
    if runs.busy:
        return {"status": "skipped", "requests": 0, "rows": 0, "error": None}
    if market_factory is None:
        store.set_kv("intraday_ibkr_last_day", local_day.isoformat())
        return {"status": "skipped", "requests": 0, "rows": 0, "error": None}
    market = market_factory()
    try:
        try:
            market.connect()
        except IbkrUnavailable:
            store.set_kv("intraday_ibkr_last_day", local_day.isoformat())
            return {"status": "skipped", "requests": 0, "rows": 0, "error": None}
        requests = 0
        rows = 0
        consecutive_failures = 0
        interrupted = False
        for symbol in settings.intraday.nasdaq_symbols or settings.events.study_symbols:
            completed = int(store.get_kv(f"intraday_ibkr_months:{symbol}") or 0)
            while completed < settings.intraday.ibkr_backfill_months:
                if runs.busy:
                    interrupted = True
                    break
                if requests >= settings.intraday.ibkr_requests_per_day:
                    break
                end = _subtract_months(now.astimezone(UTC), completed)
                requests += 1
                try:
                    bars = ibkr_intraday_history(
                        market,
                        symbol,
                        end,
                        duration="1 M",
                        bar_size=settings.intraday.ibkr_bar_size,
                    )
                    rows += store.merge_intraday("ibkr", symbol, bars)
                    completed += 1
                    store.set_kv(f"intraday_ibkr_months:{symbol}", completed)
                    consecutive_failures = 0
                except (TimeoutError, IbkrUnavailable):
                    consecutive_failures += 1
                    if consecutive_failures >= 2:
                        break
                if requests < settings.intraday.ibkr_requests_per_day and not runs.busy:
                    sleep(settings.intraday.ibkr_request_spacing_seconds)
            if interrupted or requests >= settings.intraday.ibkr_requests_per_day or consecutive_failures >= 2:
                break
        if not interrupted:
            store.set_kv("intraday_ibkr_last_day", local_day.isoformat())
        return {
            "status": "partial" if interrupted or consecutive_failures else "ok",
            "requests": requests,
            "rows": rows,
            "error": "IBKR_TIMEOUT" if consecutive_failures else None,
        }
    finally:
        market.close()


def _subtract_months(value: datetime, months: int) -> datetime:
    year = value.year
    month = value.month - months
    while month <= 0:
        year -= 1
        month += 12
    return value.replace(year=year, month=month, day=min(value.day, 28))


def _safe_error(exc: Exception) -> str:
    text = str(exc).strip()
    return (text or type(exc).__name__)[:160]


__all__ = [
    "IntradayArchive",
    "archive_jin10_day",
    "archive_nasdaq_day",
    "backfill_ibkr_intraday",
    "next_intraday_job",
    "select_nasdaq_symbols",
]
