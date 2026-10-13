"""Composition root: build settings, store, pipeline dependencies, runner and scheduler."""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from dataclasses import field
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

from copilot.analysis.event_study import STUDY_INDICATORS, compute_event_stats
from copilot.analysis.deepseek import DeepSeekClient
from copilot.config import Settings, load_settings
from copilot.market.cboe import CboeClient
from copilot.market.ibkr import IbkrMarket
from copilot.market.nasdaq_history import daily_bars, daily_closes, weekly_cache_date
from copilot.news.collect import NewsSources, build_sources, collect_news
from copilot.pipeline import PipelineDeps, run_pipeline, tag_pending_news
from copilot.scheduler import RunManager, Scheduler
from copilot.secrets import Secrets, load_secrets
from copilot.store import Store

log = logging.getLogger(__name__)
NEW_YORK = ZoneInfo("America/New_York")
EVENT_HISTORY_START = date(2021, 1, 1)


@dataclass
class Runtime:
    settings: Settings
    secrets: Secrets
    store: Store
    deps: PipelineDeps
    runs: RunManager
    scheduler: Scheduler | None
    _study_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _study_thread: threading.Thread | None = field(default=None, init=False, repr=False)

    def poll_news(self) -> None:
        if self.deps.news_sources is None:
            return
        now = datetime.now(timezone.utc)
        collect_news(self.settings, self.store, self.deps.news_sources, now=now, include_calendar=False)
        # Classify as news arrives so the feed shows tags and runs have little backlog.
        if self.settings.llm.enabled and self.deps.llm is not None:
            tag_pending_news(
                self.store, self.deps.llm, self.settings,
                since=now - timedelta(days=self.settings.views.lookback_days),
            )

    def poll_fast_news(self) -> None:
        if self.deps.news_sources is None:
            return
        now = datetime.now(timezone.utc)
        collect_news(
            self.settings,
            self.store,
            self.deps.news_sources,
            now=now,
            include_calendar=False,
            include_finnhub=False,
        )
        if self.settings.llm.enabled and self.deps.llm is not None:
            tag_pending_news(
                self.store,
                self.deps.llm,
                self.settings,
                since=now - timedelta(days=self.settings.views.lookback_days),
            )

    def refresh_event_study(self) -> bool:
        if not self.settings.events.enabled or self._study_lock.locked():
            return False
        self._study_thread = threading.Thread(
            target=self._refresh_event_study_now,
            name="copilot-event-study",
            daemon=True,
        )
        self._study_thread.start()
        return True

    def _refresh_event_study_now(self) -> None:
        if not self._study_lock.acquire(blocking=False):
            return
        try:
            today = datetime.now(NEW_YORK).date()
            source = self.deps.news_sources.tradingview if self.deps.news_sources else None
            if source is not None:
                watermark = self.store.get_kv("event_study_macro_watermark")
                cursor = (
                    max(
                        EVENT_HISTORY_START,
                        min(
                            date.fromisoformat(str(watermark)) + timedelta(days=1),
                            today - timedelta(days=14),
                        ),
                    )
                    if watermark
                    else EVENT_HISTORY_START
                )
                while cursor <= today:
                    window_end = min(cursor + timedelta(days=89), today)
                    rows = source.fetch(cursor, window_end)
                    start_at = datetime.combine(cursor, time.min, NEW_YORK).astimezone(timezone.utc)
                    end_at = datetime.combine(
                        window_end + timedelta(days=1), time.min, NEW_YORK
                    ).astimezone(timezone.utc)
                    self.store.replace_macro_events("tradingview", start_at, end_at, rows)
                    self.store.set_kv("event_study_macro_watermark", window_end.isoformat())
                    cursor = window_end + timedelta(days=1)
            start_at = datetime.combine(EVENT_HISTORY_START, time.min, NEW_YORK).astimezone(timezone.utc)
            end_at = datetime.combine(today + timedelta(days=1), time.min, NEW_YORK).astimezone(timezone.utc)
            events = self.store.macro_events_between(start_at, end_at)
            cache_day = weekly_cache_date(today)
            stats = []
            for symbol in self.settings.events.study_symbols:
                cached = self.store.get_daily_cache(symbol, "NASDAQ_OHLC", cache_day)
                if cached is None:
                    bars = daily_bars(symbol, today=today, start=EVENT_HISTORY_START)
                    self.store.put_daily_cache(
                        symbol,
                        "NASDAQ_OHLC",
                        cache_day,
                        [[day.isoformat(), open_, high, low, close] for day, open_, high, low, close in bars],
                    )
                else:
                    bars = [
                        (date.fromisoformat(row[0]), *[float(value) for value in row[1:]])
                        for row in cached
                    ]
                for category in STUDY_INDICATORS:
                    payload = compute_event_stats(
                        category,
                        symbol,
                        events,
                        bars,
                        shrink_k=self.settings.events.shrink_k,
                    )
                    if payload is not None:
                        stats.append(payload)
            self.store.replace_event_stats(stats)
        except Exception:
            log.exception("event-study refresh failed")
        finally:
            self._study_lock.release()

    def close(self) -> None:
        if self.scheduler is not None:
            self.scheduler.stop()
        self.store.close()


def make_deps(
    settings: Settings,
    secrets: Secrets,
    store: Store,
    *,
    use_ibkr: bool = True,
    use_llm: bool = True,
    use_news: bool = True,
) -> PipelineDeps:
    key = secrets.get("deepseek_api_key")
    model = (
        DeepSeekClient(key, model=settings.llm.model, timeout=settings.llm.timeout_seconds)
        if use_llm and settings.llm.enabled and key
        else None
    )
    sources: NewsSources | None = build_sources(secrets, settings) if use_news else None
    return PipelineDeps(
        settings=settings,
        store=store,
        news_sources=sources,
        llm=model,
        market_factory=(lambda: IbkrMarket(settings.ibkr)) if use_ibkr else None,
        option_source=(
            CboeClient(timeout=settings.options.cboe_timeout_seconds)
            if settings.options.source == "cboe" else None
        ),
        index_history=lambda symbol, today: daily_closes(symbol, today=today, timeout=10.0),
    )


def build_runtime(
    settings: Settings | None = None,
    *,
    with_scheduler: bool = True,
    use_ibkr: bool = True,
    use_llm: bool = True,
) -> Runtime:
    settings = settings or load_settings()
    configure_logging(settings)
    secrets = load_secrets(settings.secrets_file)
    store = Store(settings.database_path)
    interrupted = store.fail_interrupted_runs()
    if interrupted:
        log.warning("closed %s interrupted run(s)", interrupted)
    if use_ibkr:
        # Import ib_insync once on the main thread so its import-time event-loop lookup
        # never happens first on a run thread (scheduler or the GUI "run now" button).
        import ib_insync  # noqa: F401
    deps = make_deps(settings, secrets, store, use_ibkr=use_ibkr, use_llm=use_llm)
    runs = RunManager(
        lambda trigger, slot, reason=None: run_pipeline(
            deps, trigger=trigger, slot=slot, reason=reason
        )
    )
    runtime = Runtime(settings, secrets, store, deps, runs, None)
    if with_scheduler:
        runtime.scheduler = Scheduler(
            settings.schedule,
            store,
            runs,
            poll_news=runtime.poll_news,
            poll_fast=runtime.poll_fast_news,
            events=settings.events,
            universe=settings.universe,
            refresh_event_study=runtime.refresh_event_study,
        )
    if settings.events.enabled and not store.event_stats():
        runtime.refresh_event_study()
    return runtime


def configure_logging(settings: Settings) -> None:
    root = logging.getLogger()
    if any(getattr(handler, "_copilot", False) for handler in root.handlers):
        return
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        settings.log_dir / "copilot.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    handler._copilot = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(logging.INFO)


__all__ = ["Runtime", "build_runtime", "configure_logging", "make_deps"]
