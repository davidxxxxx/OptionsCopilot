"""Finnhub company news, general market news and the earnings calendar."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from copilot.http import request_json
from .models import EarningsEvent, NewsItem

BASE_URL = "https://finnhub.io/api/v1"
HOSTS = ("finnhub.io",)


class FinnhubClient:
    def __init__(self, api_key: str, *, timeout: float = 8.0) -> None:
        if not api_key:
            raise ValueError("Finnhub API key missing")
        self._headers = {"X-Finnhub-Token": api_key}
        self._timeout = timeout

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        return request_json(
            "GET",
            f"{BASE_URL}{path}",
            allowed_hosts=HOSTS,
            params=params,
            headers=self._headers,
            timeout=self._timeout,
        )

    def company_news(self, symbol: str, start: date, end: date, *, limit: int = 5) -> list[NewsItem]:
        rows = self._get("/company-news", {
            "symbol": symbol, "from": start.isoformat(), "to": end.isoformat(),
        })
        items = [_news(row, default_symbol=symbol) for row in rows if isinstance(row, dict)] \
            if isinstance(rows, list) else []
        items = [item for item in items if item is not None]
        items.sort(key=lambda item: item.published_at, reverse=True)
        return items[:limit]

    def market_news(self, category: str = "general") -> list[NewsItem]:
        rows = self._get("/news", {"category": category})
        if not isinstance(rows, list):
            return []
        return [item for row in rows if isinstance(row, dict) and (item := _news(row))]

    def earnings_calendar(self, start: date, end: date) -> list[EarningsEvent]:
        payload = self._get("/calendar/earnings", {"from": start.isoformat(), "to": end.isoformat()})
        rows = payload.get("earningsCalendar") if isinstance(payload, dict) else None
        events: list[EarningsEvent] = []
        for row in rows or ():
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            try:
                report_date = date.fromisoformat(str(row.get("date")))
            except ValueError:
                continue
            if not symbol:
                continue
            estimate = row.get("epsEstimate")
            events.append(EarningsEvent(
                symbol=symbol,
                report_date=report_date,
                hour=str(row.get("hour") or ""),
                eps_estimate=float(estimate) if isinstance(estimate, (int, float)) else None,
                source="finnhub",
            ))
        return events


def _news(row: dict[str, Any], *, default_symbol: str | None = None) -> NewsItem | None:
    headline = str(row.get("headline") or "").strip()
    stamp = row.get("datetime")
    if not headline or not isinstance(stamp, (int, float)) or stamp <= 0:
        return None
    related = [
        part.strip().upper()
        for part in str(row.get("related") or "").split(",")
        if part.strip()
    ]
    if default_symbol and default_symbol not in related:
        related.insert(0, default_symbol)
    return NewsItem(
        source="finnhub",
        source_id=str(row.get("id") or f"{stamp}:{headline[:60]}"),
        published_at=datetime.fromtimestamp(stamp, tz=timezone.utc),
        title=headline[:300],
        body=str(row.get("summary") or "")[:1200],
        url=str(row.get("url") or "")[:500],
        symbols=tuple(related[:8]),
    )


__all__ = ["FinnhubClient"]
