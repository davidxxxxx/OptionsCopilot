"""Composition root: build settings, store, pipeline dependencies, runner and scheduler."""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any
from zoneinfo import ZoneInfo

from copilot.analysis.event_study import (
    EVENT_STATS_VERSION,
    STUDY_INDICATORS,
    compute_event_stats,
    reaction_observations,
    walk_forward_evaluation,
)
from copilot.analysis.deepseek import DeepSeekClient
from copilot.analysis.news_calibration import compute_calibration
from copilot.config import Settings, load_settings
from copilot.intraday_archive import IntradayArchive
from copilot.market.cboe import CboeClient
from copilot.market.ibkr import IbkrMarket
from copilot.market.nasdaq_history import (
    daily_bars,
    daily_closes,
    merge_daily_bars,
    weekly_cache_date,
)
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
    intraday: IntradayArchive | None = None
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
        if (
            not self.settings.events.enabled
            and not self.settings.news_calibration.enabled
        ) or self._study_lock.locked():
            return False
        self._study_thread = threading.Thread(
            target=self._refresh_event_study_now,
            name="copilot-event-study",
            daemon=True,
        )
        self._study_thread.start()
        return True

    def tick_intraday(self, now: datetime, startup: bool = False) -> None:
        if self.intraday is not None:
            self.intraday.tick(now, startup=startup)

    def _refresh_event_study_now(self) -> None:
        if not self._study_lock.acquire(blocking=False):
            return
        try:
            today = datetime.now(NEW_YORK).date()
            if self.settings.events.enabled:
                try:
                    self._refresh_event_stats(today)
                except Exception:
                    log.exception("event-study refresh failed")
            if self.settings.news_calibration.enabled:
                try:
                    calibration, _built = compute_calibration(
                        self.store,
                        self.settings.news_calibration,
                        today=today,
                        index_history=self.deps.index_history,
                    )
                    self.store.set_kv("news_calibration", calibration)
                except Exception:
                    log.exception("news-calibration refresh failed")
        finally:
            self._study_lock.release()

    def _refresh_event_stats(self, today: date) -> None:
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
        bars_by_symbol: dict[str, list[tuple[date, float, float, float, float]]] = {}
        for symbol in self.settings.events.study_symbols:
            cached = self.store.get_daily_cache(symbol, "NASDAQ_OHLC", cache_day)
            if cached is None:
                bars = daily_bars(symbol, today=today, start=EVENT_HISTORY_START)
            else:
                bars = [
                    (date.fromisoformat(row[0]), *[float(value) for value in row[1:]])
                    for row in cached
                ]
            recent = daily_bars(symbol, today=today, start=today - timedelta(days=15))
            bars = merge_daily_bars(bars, recent)
            bars_by_symbol[symbol] = bars
            self.store.put_daily_cache(
                symbol,
                "NASDAQ_OHLC",
                cache_day,
                [[day.isoformat(), open_, high, low, close] for day, open_, high, low, close in bars],
            )
        sync_event_reactions(self.store, events, bars_by_symbol, self.settings)
        for symbol, bars in bars_by_symbol.items():
            for category in STUDY_INDICATORS:
                payload = compute_event_stats(
                    category,
                    symbol,
                    events,
                    bars,
                    shrink_k=self.settings.events.shrink_k,
                    min_half_t=self.settings.events.min_half_t,
                    recency_half_life=self.settings.events.recency_half_life,
                    min_events=self.settings.events.min_events,
                    min_t=self.settings.events.min_t,
                    min_wf_views=self.settings.events.min_wf_views,
                    min_wf_hit_rate=self.settings.events.min_wf_hit_rate,
                )
                if payload is not None:
                    stats.append(payload)
        self.store.replace_event_stats(stats)

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
    runtime.intraday = IntradayArchive(
        settings,
        store,
        runs,
        jin10=deps.news_sources.jin10 if deps.news_sources else None,
        market_factory=(
            lambda: IbkrMarket(replace(
                settings.ibkr,
                client_id=settings.ibkr.client_id + settings.intraday.ibkr_client_id_offset,
            ))
            if use_ibkr else None
        ),
    )
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
            archive_intraday=runtime.tick_intraday,
        )
    if (
        settings.events.enabled and event_study_refresh_needed(store)
    ) or (
        settings.news_calibration.enabled and store.get_kv("news_calibration") is None
    ):
        runtime.refresh_event_study()
    return runtime


def event_study_refresh_needed(store: Store) -> bool:
    return not store.event_stats_are_current(EVENT_STATS_VERSION)


def backfill_event_reactions(
    store: Store,
    events: list,
    bars_by_symbol: dict[str, list[tuple[date, float, float, float, float]]],
    settings: Settings,
) -> None:
    """Insert missing releases with honest expanding-window historical predictions."""

    existing = {
        (row["event_date"], row["category"], row["symbol"])
        for row in store.event_reactions(limit=100_000)
    }
    for symbol, bars in bars_by_symbol.items():
        for category in STUDY_INDICATORS:
            evaluation = walk_forward_evaluation(
                category,
                events,
                bars,
                shrink_k=settings.events.shrink_k,
                min_events=settings.events.min_events,
                min_t=settings.events.min_t,
                min_half_t=settings.events.min_half_t,
                recency_half_life=settings.events.recency_half_life,
            )
            for row in evaluation["predictions"]:
                event = row["event"]
                observation = row["observation"]
                key = (row["event_date"].isoformat(), category, symbol)
                if key in existing:
                    continue
                store.upsert_event_reaction({
                    "event_date": row["event_date"].isoformat(),
                    "category": category,
                    "symbol": symbol,
                    "event_time": event.event_time.isoformat(),
                    "title": event.title,
                    "actual": row["actual"],
                    "forecast": row["forecast"],
                    "surprise": row["surprise"],
                    "z": row["z"],
                    "prediction": row["prediction"],
                    "reaction": observation["reaction"] if observation else None,
                    "vol": _event_vol(store, symbol, row["event_date"]),
                    "status": observation["status"] if observation else "predicted",
                })
                existing.add(key)


def finalize_event_reactions(
    store: Store,
    events: list,
    bars_by_symbol: dict[str, list[tuple[date, float, float, float, float]]],
) -> None:
    """Advance predicted rows to partial or complete when Nasdaq bars exist."""

    pending = store.event_reactions(limit=100_000, incomplete_only=True)
    pending_keys = {
        (row["event_date"], row["category"], row["symbol"]): row for row in pending
    }
    for symbol, bars in bars_by_symbol.items():
        for category in STUDY_INDICATORS:
            for observation in reaction_observations(category, events, bars):
                key = (observation["date"].isoformat(), category, symbol)
                existing = pending_keys.get(key)
                if existing is None:
                    continue
                store.upsert_event_reaction({
                    **existing,
                    "reaction": observation["reaction"],
                    "vol": _event_vol(store, symbol, observation["date"]),
                    "status": observation["status"],
                })


def sync_event_reactions(
    store: Store,
    events: list,
    bars_by_symbol: dict[str, list[tuple[date, float, float, float, float]]],
    settings: Settings,
) -> None:
    """Finalize known rows, then backfill releases missed by live event runs."""

    finalize_event_reactions(store, events, bars_by_symbol)
    backfill_event_reactions(store, events, bars_by_symbol, settings)


def _event_vol(store: Store, symbol: str, event_date: date) -> dict[str, Any] | None:
    before = store.latest_market_state_before(symbol, event_date)
    event_day = store.market_state_on(symbol, event_date)
    before_iv = before.get("iv_30d") if before else None
    event_iv = event_day.get("iv_30d") if event_day else None
    if before_iv is None and event_iv is None:
        return None
    return {
        "before_date": before.get("as_of") if before else None,
        "before_iv_30d": before_iv,
        "event_date": event_day.get("as_of") if event_day else None,
        "event_iv_30d": event_iv,
        "change": (
            float(event_iv) - float(before_iv)
            if event_iv is not None and before_iv is not None else None
        ),
    }


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


__all__ = [
    "Runtime", "backfill_event_reactions", "build_runtime", "configure_logging",
    "event_study_refresh_needed", "finalize_event_reactions", "make_deps",
    "sync_event_reactions",
]
