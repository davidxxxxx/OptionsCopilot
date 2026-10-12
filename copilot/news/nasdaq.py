"""Nasdaq public earnings calendar (no key); used when Finnhub is unavailable."""
from __future__ import annotations

from datetime import date
from typing import Any

from copilot.http import request_json
from .models import EarningsEvent

URL = "https://api.nasdaq.com/api/calendar/earnings"
HOSTS = ("api.nasdaq.com",)
_HOURS = {"time-pre-market": "bmo", "time-after-hours": "amc", "time-not-supplied": ""}
# Nasdaq rejects requests without a browser-like agent.
_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/2.0"}


def earnings_for_date(day: date, *, timeout: float = 10.0) -> list[EarningsEvent]:
    payload = request_json(
        "GET", URL, allowed_hosts=HOSTS, params={"date": day.isoformat()},
        headers=_HEADERS, timeout=timeout,
    )
    data = payload.get("data") if isinstance(payload, dict) else None
    rows: Any = data.get("rows") if isinstance(data, dict) else None
    events: list[EarningsEvent] = []
    for row in rows or ():
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        events.append(EarningsEvent(
            symbol=symbol,
            report_date=day,
            hour=_HOURS.get(str(row.get("time") or "").lower(), ""),
            eps_estimate=_number(row.get("epsForecast")),
            source="nasdaq",
        ))
    return events


def _number(value: Any) -> float | None:
    text = str(value or "").replace("$", "").replace(",", "").strip()
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        return float(text)
    except ValueError:
        return None


__all__ = ["earnings_for_date"]
