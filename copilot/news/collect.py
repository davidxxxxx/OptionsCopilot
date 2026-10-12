"""Fetch every configured news/calendar source independently and store the results."""
from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from copilot.config import Settings
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


def build_sources(secrets: Secrets) -> NewsSources:
    token = secrets.get("jin10_mcp_token")
    key = secrets.get("finnhub_api_key")
    return NewsSources(
        jin10=Jin10Client(token) if token else None,
        finnhub=FinnhubClient(key) if key else None,
    )


def collect_news(
    settings: Settings,
    store: Store,
    sources: NewsSources,
    *,
    now: datetime,
    company_symbols: Sequence[str] = (),
    include_calendar: bool = True,
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

    if sources.finnhub is None:
        results.append(SourceResult("finnhub", "skipped", detail="未配置 key"))
    else:
        finnhub = sources.finnhub
        results.append(_guard("finnhub_market", lambda: store.add_news(
            item for item in finnhub.market_news() if item.published_at >= lookback_start
        )))
        if company_symbols:
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


def _guard(name: str, action: Callable[[], int]) -> SourceResult:
    try:
        return SourceResult(name, "ok", count=int(action()))
    except Exception as exc:
        log.warning("news source %s failed: %s", name, exc)
        return SourceResult(name, "failed", detail=str(exc)[:200])


__all__ = ["NewsSources", "build_sources", "collect_news"]
