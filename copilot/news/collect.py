"""Fetch every configured news/calendar source independently and store the results."""
from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from copilot.config import Settings
from copilot.news.macro_calendar import (
    ForexFactoryCalendar,
    TradingViewCalendar,
    fomc_events,
)
from copilot.secrets import Secrets
from copilot.store import Store
from .finnhub import FinnhubClient
from .jin10 import Jin10Client
from .models import EarningsEvent, SourceResult
from .nasdaq import earnings_for_date

log = logging.getLogger(__name__)


@dataclass
class NewsSources:
    jin10: Jin10Client | None = None
    finnhub: FinnhubClient | None = None
    nasdaq_earnings: Callable[[date], list[EarningsEvent]] | None = earnings_for_date
    tradingview: TradingViewCalendar | None = None
    forexfactory: ForexFactoryCalendar | None = None


def build_sources(secrets: Secrets, settings: Settings) -> NewsSources:
    token = secrets.get("jin10_mcp_token")
    key = secrets.get("finnhub_api_key")
    return NewsSources(
        jin10=Jin10Client(token) if token else None,
        finnhub=FinnhubClient(key) if key else None,
        tradingview=(
            TradingViewCalendar() if settings.calendar.enabled and settings.calendar.tradingview else None
        ),
        forexfactory=(
            ForexFactoryCalendar() if settings.calendar.enabled and settings.calendar.forexfactory else None
        ),
    )


def collect_news(
    settings: Settings,
    store: Store,
    sources: NewsSources,
    *,
    now: datetime,
    company_symbols: Sequence[str] = (),
    include_calendar: bool = True,
    include_finnhub: bool = True,
    include_company_news: bool = True,
    force_calendar: bool = False,
    calendar_start: date | None = None,
    calendar_end: date | None = None,
) -> list[SourceResult]:
    results: list[SourceResult] = []
    lookback_start = now - timedelta(hours=settings.news.lookback_hours)

    if sources.jin10 is None:
        results.append(SourceResult("jin10", "skipped", detail="未配置 token"))
    else:
        latest = store.latest_news_time("jin10")
        stop_before = max(latest, lookback_start) if latest else lookback_start
        results.append(_guard("jin10", lambda: store.add_news(
            item for item in sources.jin10.fetch_flash(900, stop_before=stop_before)
            if item.published_at >= lookback_start
        )))

    if include_finnhub and sources.finnhub is None:
        results.append(SourceResult("finnhub", "skipped", detail="未配置 key"))
    elif include_finnhub:
        finnhub = sources.finnhub
        results.append(_guard("finnhub_market", lambda: store.add_news(
            item for item in finnhub.market_news() if item.published_at >= lookback_start
        )))
        if include_company_news and company_symbols:
            limit = settings.news.finnhub_items_per_symbol
            start = (now - timedelta(days=2)).date()

            def company() -> int:
                added = 0
                failures = 0
                consecutive = 0
                for symbol in company_symbols[: settings.news.finnhub_company_news_symbols]:
                    try:
                        added += store.add_news(
                            finnhub.company_news(symbol, start, now.date(), limit=limit)
                        )
                        consecutive = 0
                    except Exception as exc:  # one symbol must not stop the rest
                        failures += 1
                        consecutive += 1
                        log.warning("finnhub company news %s failed: %s", symbol, exc)
                        if consecutive >= 3:
                            # Finnhub is struggling (ETF requests time out); don't stall the run.
                            log.warning("finnhub company news: 3 failures in a row, stopping")
                            break
                if failures and not added:
                    raise RuntimeError(f"{failures} 个标的请求失败")
                return added

            results.append(_guard("finnhub_company", company))

    if include_calendar:
        results.append(_guard("earnings", lambda: _collect_earnings(settings, store, sources, now)))
        if settings.calendar.enabled:
            results.append(_guard_result(
                "macro_calendar", lambda: _collect_macro_calendar(
                    settings, store, sources, now, force=force_calendar,
                    start=calendar_start, end=calendar_end,
                )
            ))
    return results


def _collect_earnings(settings: Settings, store: Store, sources: NewsSources, now: datetime) -> int:
    start = now.date()
    end = start + timedelta(days=settings.news.earnings_days_ahead)
    if sources.finnhub is not None:
        try:
            return store.upsert_earnings(sources.finnhub.earnings_calendar(start, end))
        except Exception as exc:
            log.warning("finnhub earnings calendar failed, falling back to Nasdaq: %s", exc)
    if sources.nasdaq_earnings is None:
        raise RuntimeError("没有可用的财报日历来源")
    events: list[EarningsEvent] = []
    day = start
    while day <= end:
        if day.weekday() < 5:
            events.extend(sources.nasdaq_earnings(day))
        day += timedelta(days=1)
    return store.upsert_earnings(events)


def _collect_macro_calendar(
    settings: Settings,
    store: Store,
    sources: NewsSources,
    now: datetime,
    *,
    force: bool = False,
    start: date | None = None,
    end: date | None = None,
) -> SourceResult:
    refreshed = store.get_kv("macro_calendar_refreshed_at")
    if refreshed and not force:
        try:
            last = datetime.fromisoformat(str(refreshed))
            if now.astimezone(timezone.utc) - last.astimezone(timezone.utc) < timedelta(hours=settings.calendar.refresh_hours):
                return SourceResult("macro_calendar", "ok", detail="未到刷新时间")
        except ValueError:
            pass
    ny = ZoneInfo("America/New_York")
    today = now.astimezone(ny).date()
    start = start or today
    end = end or today + timedelta(days=settings.calendar.days_ahead)
    count = 0
    status = "failed"
    detail = ""
    if sources.tradingview is not None:
        try:
            events = sources.tradingview.fetch(start, end)
            count += store.replace_macro_events(
                "tradingview", _day_start(start, ny), _day_start(end + timedelta(days=1), ny), events
            )
            status = "ok"
        except Exception as exc:
            detail = f"TradingView 失败: {exc}"[:200]
            log.warning("TradingView calendar failed: %s", exc)
    if status != "ok" and sources.forexfactory is not None:
        try:
            events = sources.forexfactory.fetch_this_week()
            week_start = today - timedelta(days=today.weekday())
            count += store.replace_macro_events(
                "forexfactory", _day_start(week_start, ny), _day_start(week_start + timedelta(days=7), ny), events
            )
            status = "ok"
            detail = "TradingView 失败，已用 ForexFactory 本周数据"
        except Exception as exc:
            detail = f"{detail}; ForexFactory 失败: {exc}".strip("; ")[:200]
            log.warning("ForexFactory calendar failed: %s", exc)
    configured = settings.calendar.fomc_decision_dates
    if configured:
        fed_start = min(configured)
        fed_end = max(configured) + timedelta(days=1)
        fed = fomc_events(configured)
        count += store.replace_macro_events("fed", _day_start(fed_start, ny), _day_start(fed_end, ny), fed)
    if status == "ok":
        store.set_kv("macro_calendar_refreshed_at", now.astimezone(timezone.utc).isoformat())
    return SourceResult("macro_calendar", status, count=count, detail=detail)


def _day_start(day: date, zone: ZoneInfo) -> datetime:
    return datetime.combine(day, time.min, zone).astimezone(timezone.utc)


def _guard(name: str, action: Callable[[], int]) -> SourceResult:
    try:
        return SourceResult(name, "ok", count=int(action()))
    except Exception as exc:
        log.warning("news source %s failed: %s", name, exc)
        return SourceResult(name, "failed", detail=str(exc)[:200])


def _guard_result(name: str, action: Callable[[], SourceResult]) -> SourceResult:
    try:
        return action()
    except Exception as exc:
        log.warning("news source %s failed: %s", name, exc)
        return SourceResult(name, "failed", detail=str(exc)[:200])


__all__ = ["NewsSources", "build_sources", "collect_news"]
