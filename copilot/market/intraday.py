"""Minute-bar parsers and thin read-only clients for the intraday archive."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from copilot.http import request_json
from copilot.market.nasdaq_history import ETF_HINTS

IntradayBar = tuple[int, float, float, float, float, float | None]

NASDAQ_CHART_URL = "https://api.nasdaq.com/api/quote/{symbol}/chart"
NASDAQ_HOSTS = ("api.nasdaq.com",)
NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/2.0"
}
NEW_YORK = ZoneInfo("America/New_York")
UTC = timezone.utc


class KlineClient(Protocol):
    def kline(self, code: str, start: int, count: int) -> Any: ...


def parse_nasdaq_chart(payload: Any, *, symbol: str) -> list[IntradayBar]:
    """Convert Nasdaq's UTC-shaped New York wall-clock timestamps to real UTC."""

    del symbol
    data = payload.get("data") if isinstance(payload, dict) else None
    chart = data.get("chart") if isinstance(data, dict) else None
    volumes = data.get("volumeChart") if isinstance(data, dict) else None
    if not isinstance(chart, list):
        return []
    volume_by_x: dict[int, float] = {}
    for row in volumes if isinstance(volumes, list) else ():
        if not isinstance(row, dict):
            continue
        try:
            raw_x = int(row["x"])
        except (KeyError, TypeError, ValueError):
            continue
        volume = _number(row.get("y"))
        if volume is None and isinstance(row.get("z"), dict):
            volume = _number(row["z"].get("value"))
        if volume is not None:
            volume_by_x[raw_x] = volume

    bars: dict[int, IntradayBar] = {}
    for row in chart:
        if not isinstance(row, dict):
            continue
        try:
            raw_x = int(row["x"])
        except (KeyError, TypeError, ValueError):
            continue
        price = _number(row.get("y"))
        if price is None and isinstance(row.get("z"), dict):
            price = _number(row["z"].get("value"))
        if price is None:
            continue
        wall = datetime.fromtimestamp(raw_x / 1000, UTC).replace(tzinfo=None)
        label = row.get("z", {}).get("dateTime") if isinstance(row.get("z"), dict) else None
        if label and not _wall_label_agrees(wall, str(label)):
            continue
        timestamp = int(wall.replace(tzinfo=NEW_YORK).astimezone(UTC).timestamp())
        volume = volume_by_x.get(raw_x)
        if volume is not None and abs(volume - price) <= 1e-9:
            volume = None
        bars[timestamp] = (
            timestamp, price, price, price, price, volume,
        )
    return [bars[key] for key in sorted(bars)]


class NasdaqIntraday:
    def __init__(self, *, timeout: float = 25.0) -> None:
        self.timeout = timeout

    def fetch(self, symbol: str) -> list[IntradayBar]:
        symbol = symbol.upper()
        classes = ("etf", "stocks") if symbol in ETF_HINTS else ("stocks", "etf")
        for assetclass in classes:
            payload = request_json(
                "GET",
                NASDAQ_CHART_URL.format(symbol=symbol),
                allowed_hosts=NASDAQ_HOSTS,
                headers=NASDAQ_HEADERS,
                timeout=self.timeout,
                params={"assetclass": assetclass},
            )
            bars = parse_nasdaq_chart(payload, symbol=symbol)
            if bars:
                return bars
        return []


def parse_jin10_klines(payload: Any) -> list[IntradayBar]:
    data = payload.get("data") if isinstance(payload, dict) else None
    rows = data.get("klines") if isinstance(data, dict) else None
    bars: dict[int, IntradayBar] = {}
    for row in rows if isinstance(rows, list) else ():
        if not isinstance(row, dict):
            continue
        try:
            timestamp = int(float(row["time"]))
        except (KeyError, TypeError, ValueError):
            continue
        values = tuple(_number(row.get(key)) for key in ("open", "high", "low", "close"))
        if any(value is None for value in values):
            continue
        volume = _number(row.get("volume"))
        open_, high, low, close = values
        bars[timestamp] = (
            timestamp, float(open_), float(high), float(low), float(close), volume,
        )
    return [bars[key] for key in sorted(bars)]


def fetch_jin10_24h(
    client: KlineClient,
    code: str,
    *,
    now: datetime,
    count: int = 100,
) -> list[IntradayBar]:
    """Page the rolling 24-hour minute window in fixed ``count``-minute steps."""

    if not 1 <= count <= 100:
        raise ValueError("Jin10 kline count must be 1-100")
    end = int(now.astimezone(UTC).timestamp())
    start = end - 24 * 60 * 60
    bars: dict[int, IntradayBar] = {}
    for window_start in range(start, end, count * 60):
        for bar in parse_jin10_klines(client.kline(code, window_start, count)):
            if start <= bar[0] <= end:
                bars[bar[0]] = bar
    return [bars[key] for key in sorted(bars)]


def ibkr_intraday_history(
    market: Any,
    symbol: str,
    end: datetime,
    duration: str = "1 M",
    bar_size: str = "5 mins",
) -> list[IntradayBar]:
    """Read one RTH-only historical window through the read-only broker adapter."""

    return market.intraday_history(
        symbol, end=end, duration=duration, bar_size=bar_size
    )


def _wall_label_agrees(wall: datetime, label: str) -> bool:
    cleaned = label.strip().upper().removesuffix(" ET").strip()
    for pattern in ("%I:%M %p", "%I:%M:%S %p"):
        try:
            parsed = datetime.strptime(cleaned, pattern)
        except ValueError:
            continue
        return (parsed.hour, parsed.minute) == (wall.hour, wall.minute)
    return False


def _number(value: Any) -> float | None:
    try:
        number = float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


__all__ = [
    "IntradayBar",
    "NasdaqIntraday",
    "fetch_jin10_24h",
    "ibkr_intraday_history",
    "parse_jin10_klines",
    "parse_nasdaq_chart",
]
