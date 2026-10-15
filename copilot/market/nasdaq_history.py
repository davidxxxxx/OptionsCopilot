"""Daily closes from Nasdaq's public quote history (no key)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from copilot.http import request_json

URL = "https://api.nasdaq.com/api/quote/{symbol}/historical"
HOSTS = ("api.nasdaq.com",)
# Nasdaq rejects requests without a browser-like agent.
_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/2.0"}
ETF_HINTS = frozenset(
    "SPY QQQ IWM DIA TLT IEF SHY GLD SLV USO UNG XLE XLF XLK XLV XLI XLY XLP XLU XLB "
    "XLRE XLC SMH SOXX ARKK EEM EFA FXI KWEB HYG LQD UUP XBI KRE IBIT".split()
)


def daily_closes(
    symbol: str, *, today: date, assetclass: str | None = None, days: int = 380, timeout: float = 25.0
) -> list[tuple[date, float]]:
    """About a year of daily closes, oldest first."""

    classes = (assetclass,) if assetclass else (("etf", "stocks") if symbol.upper() in ETF_HINTS else ("stocks", "etf"))
    for name in classes:
        payload = request_json(
            "GET", URL.format(symbol=symbol), allowed_hosts=HOSTS, headers=_HEADERS, timeout=timeout,
            params={
                "assetclass": name,
                "fromdate": (today - timedelta(days=days)).isoformat(),
                "todate": today.isoformat(),
                "limit": 400,
            },
        )
        closes = parse_closes(payload)
        if closes:
            return closes
    return []


def daily_bars(
    symbol: str,
    *,
    today: date,
    start: date = date(2021, 1, 1),
    assetclass: str | None = None,
    timeout: float = 25.0,
) -> list[tuple[date, float, float, float, float]]:
    """Daily OHLC rows oldest first, with ETF/stocks asset-class fallback."""

    classes = (
        (assetclass,)
        if assetclass
        else (("etf", "stocks") if symbol.upper() in ETF_HINTS else ("stocks", "etf"))
    )
    for name in classes:
        payload = request_json(
            "GET", URL.format(symbol=symbol), allowed_hosts=HOSTS, headers=_HEADERS, timeout=timeout,
            params={
                "assetclass": name,
                "fromdate": start.isoformat(),
                "todate": today.isoformat(),
                "limit": 2000,
            },
        )
        bars = parse_bars(payload)
        if bars:
            return bars
    return []


def parse_closes(payload: Any) -> list[tuple[date, float]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    table = data.get("tradesTable") if isinstance(data, dict) else None
    rows = table.get("rows") if isinstance(table, dict) else None
    closes: dict[date, float] = {}
    for row in rows or ():
        if not isinstance(row, dict):
            continue
        try:
            day = datetime.strptime(str(row.get("date")), "%m/%d/%Y").date()
            close = float(str(row.get("close") or "").replace("$", "").replace(",", ""))
        except ValueError:
            continue
        if close > 0:
            closes[day] = close
    return sorted(closes.items())


def parse_bars(payload: Any) -> list[tuple[date, float, float, float, float]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    table = data.get("tradesTable") if isinstance(data, dict) else None
    rows = table.get("rows") if isinstance(table, dict) else None
    bars: dict[date, tuple[date, float, float, float, float]] = {}
    for row in rows or ():
        if not isinstance(row, dict):
            continue
        try:
            day = datetime.strptime(str(row.get("date")), "%m/%d/%Y").date()
            values = tuple(_price(row.get(name)) for name in ("open", "high", "low", "close"))
        except ValueError:
            continue
        if all(value > 0 for value in values):
            bars[day] = (day, *values)
    return sorted(bars.values())


def weekly_cache_date(today: date) -> date:
    return today - timedelta(days=today.weekday())


def merge_daily_bars(
    cached: list[tuple[date, float, float, float, float]],
    recent: list[tuple[date, float, float, float, float]],
) -> list[tuple[date, float, float, float, float]]:
    """Merge a short fresh window into long cached history by trading date."""

    merged = {row[0]: row for row in cached}
    merged.update({row[0]: row for row in recent})
    return [merged[day] for day in sorted(merged)]


def _price(value: Any) -> float:
    return float(str(value or "").replace("$", "").replace(",", ""))


__all__ = [
    "ETF_HINTS",
    "daily_bars",
    "daily_closes",
    "parse_bars",
    "parse_closes",
    "merge_daily_bars",
    "weekly_cache_date",
]
