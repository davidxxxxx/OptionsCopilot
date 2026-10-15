"""Public dividend dates, deterministic projections and weekly caching."""
from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any

from copilot.http import request_json


HOSTS = ("api.nasdaq.com",)
URL = "https://api.nasdaq.com/api/quote/{symbol}/dividends"
_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/2.0"}
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_ETF_SYMBOLS = frozenset({
    "DIA", "EEM", "GLD", "HYG", "IWM", "QQQ", "SLV", "SPY", "TLT", "XLE", "XLF",
})


@dataclass(frozen=True)
class DividendEvent:
    ex_date: date
    amount: float
    estimated: bool = False
    source: str = "nasdaq"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ex_date": self.ex_date.isoformat(),
            "amount": round(self.amount, 6),
            "estimated": self.estimated,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "DividendEvent":
        return cls(
            ex_date=date.fromisoformat(str(payload["ex_date"])),
            amount=float(payload.get("amount") or 0.0),
            estimated=bool(payload.get("estimated")),
            source=str(payload.get("source") or "nasdaq"),
        )


def nasdaq_dividends(
    symbol: str,
    *,
    asset_class: str | None = None,
    timeout: float = 10.0,
) -> list[DividendEvent]:
    normalized = symbol.strip().upper()
    if not _SYMBOL.fullmatch(normalized):
        raise ValueError("invalid dividend symbol")
    payload = request_json(
        "GET",
        URL.format(symbol=normalized),
        allowed_hosts=HOSTS,
        params={"assetclass": asset_class or ("etf" if normalized in _ETF_SYMBOLS else "stocks")},
        headers=_HEADERS,
        timeout=timeout,
    )
    return parse_nasdaq_dividends(payload)


def parse_nasdaq_dividends(payload: Any) -> list[DividendEvent]:
    data = payload.get("data") if isinstance(payload, dict) else None
    dividends = data.get("dividends") if isinstance(data, dict) else None
    rows = dividends.get("rows") if isinstance(dividends, dict) else None
    events: list[DividendEvent] = []
    for row in rows or ():
        if not isinstance(row, dict):
            continue
        ex_date = _nasdaq_date(row.get("exOrEffDate"))
        amount = _number(row.get("amount"))
        if ex_date is None or amount is None or amount <= 0:
            continue
        events.append(DividendEvent(ex_date=ex_date, amount=amount))
    return sorted(events, key=lambda event: event.ex_date)


def projected_dividend(
    symbol: str,
    history: Sequence[DividendEvent],
    *,
    today: date,
    spot: float | None = None,
    spy_quarterly_yield: float = 0.003,
) -> DividendEvent | None:
    exact_upcoming = [event for event in history if event.ex_date >= today]
    if exact_upcoming:
        return min(exact_upcoming, key=lambda event: event.ex_date)
    ordered = sorted(history, key=lambda event: event.ex_date)
    if len(ordered) >= 2:
        intervals = [
            (current.ex_date - previous.ex_date).days
            for previous, current in zip(ordered, ordered[1:])
            if current.ex_date > previous.ex_date
        ]
        if intervals:
            interval = max(1, round(median(intervals)))
            projected = ordered[-1].ex_date
            while projected < today:
                projected += timedelta(days=interval)
            return DividendEvent(
                ex_date=projected,
                amount=ordered[-1].amount,
                estimated=True,
                source="history_projection",
            )
    if symbol.upper() == "SPY":
        if spot is None or spot <= 0:
            return None
        return DividendEvent(
            ex_date=_next_spy_ex_date(today),
            amount=spot * spy_quarterly_yield,
            estimated=True,
            source="spy_quarterly_rule",
        )
    return None


def weekly_dividends(
    store: Any,
    symbol: str,
    *,
    today: date,
    fetcher: Callable[[str], Sequence[DividendEvent]] = nasdaq_dividends,
    spot: float | None = None,
    spy_quarterly_yield: float = 0.003,
) -> list[DividendEvent]:
    normalized = symbol.strip().upper()
    week_start = today - timedelta(days=today.weekday())
    cache_key = f"dividends:{normalized}:{week_start.isoformat()}"
    cached = store.get_kv(cache_key)
    if isinstance(cached, list):
        cached_events = [DividendEvent.from_dict(item) for item in cached]
        if normalized == "SPY" and not cached_events and spot is not None and spot > 0:
            projected = projected_dividend(
                normalized,
                (),
                today=today,
                spot=spot,
                spy_quarterly_yield=spy_quarterly_yield,
            )
            cached_events = [projected] if projected is not None else []
            store.set_kv(cache_key, [event.to_dict() for event in cached_events])
            return cached_events
        if normalized != "SPY" or cached_events:
            if normalized == "SPY" and spot is not None and spot > 0:
                cached_events = [
                    replace(event, amount=spot * spy_quarterly_yield)
                    if event.source == "spy_quarterly_rule" else event
                    for event in cached_events
                ]
                store.set_kv(cache_key, [event.to_dict() for event in cached_events])
            return cached_events

    history_key = f"dividend_history:{normalized}"
    stored_history = store.get_kv(history_key)
    history = [
        DividendEvent.from_dict(item)
        for item in stored_history or ()
        if isinstance(item, dict)
    ]
    try:
        fetched = list(fetcher(normalized))
    except Exception:
        fetched = []
    if fetched:
        merged = {
            (event.ex_date, event.amount): replace(event, estimated=False, source="nasdaq")
            for event in [*history, *fetched]
        }
        history = sorted(merged.values(), key=lambda event: event.ex_date)
        store.set_kv(history_key, [event.to_dict() for event in history])

    upcoming = [event for event in fetched if event.ex_date >= today]
    if upcoming:
        result = sorted(upcoming, key=lambda event: event.ex_date)
    else:
        projected = projected_dividend(
            normalized,
            history,
            today=today,
            spot=spot,
            spy_quarterly_yield=spy_quarterly_yield,
        )
        result = [projected] if projected is not None else []
    store.set_kv(cache_key, [event.to_dict() for event in result])
    return result


def _next_spy_ex_date(today: date) -> date:
    for year in range(today.year, today.year + 3):
        for month in (3, 6, 9, 12):
            first = date(year, month, 1)
            first_friday = first + timedelta(days=(4 - first.weekday()) % 7)
            third_friday = first_friday + timedelta(days=14)
            if third_friday >= today:
                return third_friday
    raise AssertionError("quarterly SPY date not found")


def _nasdaq_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for pattern in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None


def _number(value: Any) -> float | None:
    text = str(value or "").replace("$", "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


__all__ = [
    "DividendEvent",
    "nasdaq_dividends",
    "parse_nasdaq_dividends",
    "projected_dividend",
    "weekly_dividends",
]
