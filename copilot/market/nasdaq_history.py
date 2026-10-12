"""Daily closes from Nasdaq's public quote history (no key); used for the index ladder."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from copilot.http import request_json

URL = "https://api.nasdaq.com/api/quote/{symbol}/historical"
HOSTS = ("api.nasdaq.com",)
# Nasdaq rejects requests without a browser-like agent.
_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/2.0"}


def daily_closes(
    symbol: str, *, today: date, assetclass: str = "etf", days: int = 380, timeout: float = 25.0
) -> list[tuple[date, float]]:
    """About a year of daily closes, oldest first."""

    payload = request_json(
        "GET", URL.format(symbol=symbol), allowed_hosts=HOSTS, headers=_HEADERS, timeout=timeout,
        params={
            "assetclass": assetclass,
            "fromdate": (today - timedelta(days=days)).isoformat(),
            "todate": today.isoformat(),
            "limit": 400,
        },
    )
    return parse_closes(payload)


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


__all__ = ["daily_closes", "parse_closes"]
