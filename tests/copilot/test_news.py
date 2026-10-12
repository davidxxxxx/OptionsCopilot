from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from copilot.news.collect import NewsSources, collect_news
from copilot.news.jin10 import Jin10Client, Jin10Error, flash_to_news, parse_jin10_time
from copilot.news.models import EarningsEvent, NewsItem


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, headers: dict[str, str]) -> None:
        super().__init__(body)
        self.headers = headers


class FakeMcp:
    """Simulates the Jin10 MCP server; records methods, serves two flash pages."""

    def __init__(self, *, sse: bool = False) -> None:
        self.calls: list[str] = []
        self.sse = sse

    def __call__(self, request: Any, timeout: float) -> FakeResponse:
        if request.get_method() == "DELETE":
            self.calls.append("DELETE")
            return FakeResponse(b"", {})
        assert request.headers["Authorization"] == "Bearer secret-token"
        payload = json.loads(request.data)
        method = payload["method"]
        self.calls.append(method)
        if "id" not in payload:
            return FakeResponse(b"", {"Content-Type": "application/json"})
        if method == "initialize":
            result: dict[str, Any] = {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}}
        else:
            cursor = payload["params"]["arguments"].get("cursor")
            items = (
                [{"content": "第二页快讯", "time": "2026-10-09T01:00:00+08:00", "url": ""}]
                if cursor else
                [{"content": "美联储理事暗示降息", "time": "2026-10-09T04:25:02+08:00", "url": "https://flash.jin10.com/x"}]
            )
            data = {"items": items, "has_more": not cursor, "next_cursor": None if cursor else "c2"}
            result = {"content": [{"type": "text", "text": json.dumps({"status": 200, "data": data}, ensure_ascii=False)}]}
        body = json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": result}, ensure_ascii=False).encode()
        headers = {"Mcp-Session-Id": "sess-1"}
        if self.sse:
            headers["Content-Type"] = "text/event-stream"
            return FakeResponse(b"event: message\ndata: " + body + b"\n\n", headers)
        headers["Content-Type"] = "application/json"
        return FakeResponse(body, headers)


@pytest.mark.parametrize("sse", [False, True])
def test_jin10_client_pages_and_cleans_up_session(sse: bool) -> None:
    server = FakeMcp(sse=sse)
    items = Jin10Client("secret-token", opener=server).fetch_flash(10)
    assert [item.title for item in items] == ["美联储理事暗示降息", "第二页快讯"]
    assert server.calls == ["initialize", "notifications/initialized", "tools/call", "tools/call", "DELETE"]
    assert items[0].published_at == datetime(2026, 10, 8, 20, 25, 2, tzinfo=timezone.utc)


def test_jin10_stops_paging_at_known_items() -> None:
    server = FakeMcp()
    stop = datetime(2026, 10, 8, 21, 0, tzinfo=timezone.utc)
    items = Jin10Client("secret-token", opener=server).fetch_flash(10, stop_before=stop)
    assert len(items) == 1
    assert server.calls.count("tools/call") == 1


def test_jin10_auth_failure_does_not_leak_token() -> None:
    from urllib.error import HTTPError

    def opener(request: Any, timeout: float) -> Any:
        raise HTTPError(request.full_url, 401, "Unauthorized", {}, None)

    with pytest.raises(Jin10Error) as caught:
        Jin10Client("secret-token", opener=opener).fetch_flash(5)
    assert "secret-token" not in str(caught.value)
    assert str(caught.value) == "AUTH_FAILED"


def test_jin10_time_without_offset_is_beijing() -> None:
    assert parse_jin10_time("2026-10-09 08:00:00") == datetime(2026, 10, 9, 0, 0, tzinfo=timezone.utc)
    assert flash_to_news({"content": "", "time": "2026-10-09"}) is None


class FailingFinnhub:
    def market_news(self) -> list[NewsItem]:
        raise RuntimeError("HTTP_ERROR (HTTP 429)")

    def company_news(self, *args: Any, **kwargs: Any) -> list[NewsItem]:
        return []

    def earnings_calendar(self, start: Any, end: Any) -> list[EarningsEvent]:
        raise RuntimeError("down")


def test_collect_isolates_failing_sources(settings, store) -> None:
    now = datetime(2026, 10, 9, 0, 0, tzinfo=timezone.utc)
    nasdaq_days = []

    def nasdaq(day):
        nasdaq_days.append(day)
        return [EarningsEvent("JPM", day, "bmo")] if not nasdaq_days[:-1] else []

    sources = NewsSources(jin10=Jin10Client("secret-token", opener=FakeMcp()), finnhub=FailingFinnhub(),
                          nasdaq_earnings=nasdaq)
    results = {r.source: r for r in collect_news(settings, store, sources, now=now, company_symbols=["SPY"])}
    assert results["jin10"].status == "ok" and results["jin10"].count == 2
    assert results["finnhub_market"].status == "failed"
    assert results["earnings"].status == "ok"
    assert nasdaq_days  # fell back to Nasdaq after Finnhub's calendar failed
    assert len(store.recent_news(now - timedelta(days=1))) == 2
    # Running again adds nothing new (deduplicated by source id).
    again = {r.source: r for r in collect_news(settings, store, sources, now=now, include_calendar=False)}
    assert again["jin10"].count == 0


def test_company_news_stops_after_three_consecutive_failures(settings, store) -> None:
    calls = []

    class SlowFinnhub:
        def market_news(self):
            return []

        def company_news(self, symbol, *args, **kwargs):
            calls.append(symbol)
            raise RuntimeError("TIMEOUT")

        def earnings_calendar(self, start, end):
            return []

    now = datetime(2026, 10, 9, 0, 0, tzinfo=timezone.utc)
    sources = NewsSources(jin10=None, finnhub=SlowFinnhub(), nasdaq_earnings=None)
    results = {r.source: r for r in collect_news(
        settings, store, sources, now=now, company_symbols=["SPY", "QQQ", "NVDA", "JPM", "AAPL"],
        include_calendar=False,
    )}
    assert calls == ["SPY", "QQQ", "NVDA"]
    assert results["finnhub_company"].status == "failed"
