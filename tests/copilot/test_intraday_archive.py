"""Offline coverage for the storage-only intraday archive."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from copilot.config import load_settings
from copilot.intraday_archive import (
    backfill_ibkr_intraday,
    next_intraday_job,
    select_nasdaq_symbols,
)
from copilot.market.ibkr import IbkrUnavailable
from copilot.market.intraday import (
    fetch_jin10_24h,
    parse_jin10_klines,
    parse_nasdaq_chart,
)
from copilot.news.jin10 import Jin10Client, Jin10Error
from copilot.params import params_version
from copilot.runtime import Runtime
from copilot.scheduler import RunManager, Scheduler
from copilot.secrets import Secrets
from copilot.web.app import create_app

UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")


def _fake_nasdaq_x(day: date, hour: int, minute: int = 0) -> int:
    wall = datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)
    return int(wall.timestamp() * 1000)


def test_parse_nasdaq_chart_converts_edt_wall_clock_and_aligns_volume() -> None:
    raw_x = _fake_nasdaq_x(date(2026, 7, 15), 4)
    payload = {
        "data": {
            "chart": [{"x": raw_x, "y": "$101.25", "z": {"dateTime": "04:00 AM ET"}}],
            "volumeChart": [{"x": raw_x, "y": "1,234"}],
        }
    }

    bars = parse_nasdaq_chart(payload, symbol="SPY")

    assert bars == [(
        int(datetime(2026, 7, 15, 8, tzinfo=UTC).timestamp()),
        101.25,
        101.25,
        101.25,
        101.25,
        1234.0,
    )]


def test_parse_nasdaq_chart_ignores_volume_chart_when_it_copies_price() -> None:
    raw_x = 1791864000000
    copied = {
        "x": raw_x,
        "y": 777.77,
        "z": {"dateTime": "4:00 AM ET", "value": "777.77"},
    }
    payload = {"data": {"chart": [copied], "volumeChart": [dict(copied)]}}

    bars = parse_nasdaq_chart(payload, symbol="SPY")

    assert bars == [(
        int(datetime(2026, 10, 13, 8, tzinfo=UTC).timestamp()),
        777.77,
        777.77,
        777.77,
        777.77,
        None,
    )]


def test_parse_nasdaq_chart_uses_distinct_volume_chart_value() -> None:
    raw_x = 1791864000000
    payload = {"data": {
        "chart": [{
            "x": raw_x,
            "y": 777.77,
            "z": {"dateTime": "4:00 AM ET", "value": "777.77"},
        }],
        "volumeChart": [{
            "x": raw_x,
            "y": 12345,
            "z": {"dateTime": "4:00 AM ET", "value": "12345"},
        }],
    }}

    bars = parse_nasdaq_chart(payload, symbol="SPY")

    assert bars[0][5] == 12345.0


def test_parse_nasdaq_chart_converts_est_wall_clock() -> None:
    raw_x = _fake_nasdaq_x(date(2026, 1, 15), 4)
    payload = {"data": {"chart": [{
        "x": raw_x,
        "y": 99,
        "z": {"dateTime": "04:00 AM ET"},
    }]}}

    bars = parse_nasdaq_chart(payload, symbol="QQQ")

    assert bars[0][0] == int(datetime(2026, 1, 15, 9, tzinfo=UTC).timestamp())


def test_parse_nasdaq_chart_rejects_mismatched_or_odd_payloads() -> None:
    raw_x = _fake_nasdaq_x(date(2026, 7, 15), 4)
    assert parse_nasdaq_chart({}, symbol="SPY") == []
    assert parse_nasdaq_chart({"data": {"chart": "odd"}}, symbol="SPY") == []
    assert parse_nasdaq_chart({"data": {"chart": [
        {"x": raw_x, "y": 100, "z": {"dateTime": "04:01 AM ET"}},
        {"x": "bad", "y": 100},
        {"x": raw_x + 60_000, "y": "not-a-number"},
    ]}}, symbol="SPY") == []


def test_parse_jin10_klines_sorts_string_values() -> None:
    payload = {"data": {"klines": [
        {"time": "20", "open": "2", "high": "3", "low": "1", "close": "2.5", "volume": "8"},
        {"time": "10", "open": "1", "high": "2", "low": ".5", "close": "1.5", "volume": "7"},
    ]}}

    assert parse_jin10_klines(payload) == [
        (10, 1.0, 2.0, 0.5, 1.5, 7.0),
        (20, 2.0, 3.0, 1.0, 2.5, 8.0),
    ]


def test_jin10_kline_400_uses_fixed_error_without_token() -> None:
    class FakeJin10(Jin10Client):
        def _call(self, method, params, session_id):
            if method == "initialize":
                return {}, "session"
            return {"isError": True, "content": [{"type": "text", "text": "HTTP 400 bad code"}]}, session_id

        def _notify(self, method, session_id):
            return None

        def _delete(self, session_id):
            return None

    client = FakeJin10("top-secret-token")

    with pytest.raises(Jin10Error) as raised:
        client.kline("BAD", 0, 100)

    assert str(raised.value) == "HTTP_400"
    assert "top-secret-token" not in str(raised.value)


def test_fetch_jin10_24h_pages_expected_windows() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int, int]] = []

        def kline(self, code: str, start: int, count: int):
            self.calls.append((code, start, count))
            return {"data": {"klines": [{
                "time": str(start),
                "open": "1",
                "high": "1",
                "low": "1",
                "close": "1",
                "volume": "1",
            }]}}

    client = FakeClient()
    now = datetime(2026, 10, 13, 12, tzinfo=UTC)
    start = int(now.timestamp()) - 24 * 60 * 60

    bars = fetch_jin10_24h(client, "SPX", now=now)

    assert len(client.calls) == 15
    assert client.calls[0] == ("SPX", start, 100)
    assert client.calls[-1] == ("SPX", start + 14 * 100 * 60, 100)
    assert len(bars) == 15


def test_merge_intraday_splits_new_york_days_and_newer_wins(store) -> None:
    before_midnight = int(datetime(2026, 10, 10, 3, 59, tzinfo=UTC).timestamp())
    midnight = int(datetime(2026, 10, 10, 4, 0, tzinfo=UTC).timestamp())
    store.merge_intraday("nasdaq", "spy", [
        (before_midnight, 1, 2, 0.5, 1.5, 10),
        (midnight, 2, 3, 1.5, 2.5, 20),
    ])
    store.merge_intraday("nasdaq", "SPY", [
        (before_midnight, 9, 10, 8, 9.5, 99),
    ])

    assert store.intraday_day("nasdaq", "SPY", "2026-10-09") == [
        [before_midnight, 9.0, 10.0, 8.0, 9.5, 99.0]
    ]
    assert store.intraday_day("nasdaq", "SPY", "2026-10-10") == [
        [midnight, 2.0, 3.0, 1.5, 2.5, 20.0]
    ]


def test_intraday_status_counts_symbols_days_and_rows(store) -> None:
    first = int(datetime(2026, 10, 9, 14, tzinfo=UTC).timestamp())
    second = int(datetime(2026, 10, 10, 14, tzinfo=UTC).timestamp())
    store.merge_intraday("nasdaq", "SPY", [(first, 1, 1, 1, 1, 1)])
    store.merge_intraday("nasdaq", "QQQ", [(first, 2, 2, 2, 2, 2)])
    store.merge_intraday("nasdaq", "SPY", [(second, 3, 3, 3, 3, 3)])

    status = store.intraday_status()

    assert status[0]["source"] == "nasdaq"
    assert status[0]["symbols"] == 2
    assert status[0]["days"] == 2
    assert status[0]["last_day"] == "2026-10-10"
    assert status[0]["rows"] == 3


def test_select_nasdaq_symbols_combines_study_news_and_tracked(settings) -> None:
    impact = SimpleNamespace(symbol="NVDA")
    tagged = [
        (SimpleNamespace(published_at=datetime(2026, 10, 13, 14, tzinfo=UTC)), SimpleNamespace(impacts=(impact,))),
        (SimpleNamespace(published_at=datetime(2026, 10, 13, 15, tzinfo=UTC)), SimpleNamespace(impacts=(impact,))),
        (SimpleNamespace(published_at=datetime(2026, 10, 13, 16, tzinfo=UTC)), SimpleNamespace(impacts=(SimpleNamespace(symbol="JPM"),))),
    ]
    fake_store = SimpleNamespace(
        tagged_news=lambda since, min_importance: tagged,
        open_tracked=lambda: [{"symbol": "GLD"}],
    )
    configured = replace(
        settings,
        intraday=replace(settings.intraday, nasdaq_symbols=("SPY",), max_news_symbols=2),
    )

    selected = select_nasdaq_symbols(fake_store, configured, date(2026, 10, 13))

    assert selected == ("SPY", "NVDA", "JPM", "GLD")


class MutableRuns:
    def __init__(self, busy: bool = False) -> None:
        self.busy = busy


def test_intraday_schedule_nasdaq_once_after_close_and_not_holidays(store, settings) -> None:
    runs = MutableRuns()
    before = datetime(2026, 10, 13, 20, 4, tzinfo=NEW_YORK)
    after = datetime(2026, 10, 13, 20, 5, tzinfo=NEW_YORK)

    assert next_intraday_job(before, settings, store, runs) == "jin10"
    assert next_intraday_job(after, settings, store, runs) == "nasdaq"
    store.set_kv("intraday_nasdaq_last_day", "2026-10-13")
    store.set_kv("intraday_jin10_last_run", after.astimezone(UTC).isoformat())
    assert next_intraday_job(after, settings, store, runs) is None
    assert next_intraday_job(
        datetime(2026, 11, 26, 20, 5, tzinfo=NEW_YORK), settings, store, runs
    ) != "nasdaq"
    assert next_intraday_job(
        datetime(2026, 10, 10, 20, 5, tzinfo=NEW_YORK), settings, store, runs
    ) != "nasdaq"


def test_intraday_schedule_startup_catchup_and_jin10_interval(store, settings) -> None:
    runs = MutableRuns()
    after_close = datetime(2026, 10, 13, 21, tzinfo=NEW_YORK)
    assert next_intraday_job(after_close, settings, store, runs, startup=True) == "nasdaq"
    store.set_kv("intraday_nasdaq_last_day", "2026-10-13")
    assert next_intraday_job(after_close, settings, store, runs, startup=True) == "jin10"
    store.set_kv("intraday_jin10_last_run", after_close.astimezone(UTC).isoformat())
    store.set_kv("intraday_ibkr_last_day", "2026-10-13")
    assert next_intraday_job(
        after_close.replace(hour=23), settings, store, runs
    ) is None
    assert next_intraday_job(
        datetime(2026, 10, 14, 3, 1, tzinfo=NEW_YORK), settings, store, runs
    ) == "jin10"


def test_intraday_schedule_skips_ibkr_while_run_busy(store, settings) -> None:
    now = datetime(2026, 10, 13, 20, 31, tzinfo=NEW_YORK)
    store.set_kv("intraday_nasdaq_last_day", "2026-10-13")
    store.set_kv("intraday_jin10_last_run", now.astimezone(UTC).isoformat())

    assert next_intraday_job(now, settings, store, MutableRuns(busy=True)) is None


class FakeMarket:
    def __init__(self, outcomes=()) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, datetime, str, str]] = []
        self.closed = False

    def connect(self) -> None:
        return None

    def intraday_history(self, symbol, *, end, duration, bar_size):
        self.calls.append((symbol, end, duration, bar_size))
        outcome = self.outcomes.pop(0) if self.outcomes else []
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self) -> None:
        self.closed = True


def test_ibkr_backfill_skips_gateway_down(store, settings) -> None:
    class DownMarket(FakeMarket):
        def connect(self) -> None:
            raise IbkrUnavailable("down")

    market = DownMarket()
    result = backfill_ibkr_intraday(
        store, settings, MutableRuns(), lambda: market,
        now=datetime(2026, 10, 13, 21, tzinfo=NEW_YORK), sleep=lambda seconds: None,
    )

    assert result == {"status": "skipped", "requests": 0, "rows": 0, "error": None}
    assert market.closed


def test_ibkr_backfill_respects_request_cap_pacing_and_resumes_watermarks(store, settings) -> None:
    bar = (int(datetime(2026, 10, 13, 14, tzinfo=UTC).timestamp()), 1, 1, 1, 1, 1)
    market = FakeMarket(outcomes=[[bar], [bar]])
    sleeps: list[float] = []
    configured = replace(settings, intraday=replace(
        settings.intraday,
        nasdaq_symbols=("SPY",),
        ibkr_backfill_months=3,
        ibkr_requests_per_day=2,
        ibkr_request_spacing_seconds=0.25,
    ))

    result = backfill_ibkr_intraday(
        store, configured, MutableRuns(), lambda: market,
        now=datetime(2026, 10, 13, 21, tzinfo=NEW_YORK), sleep=sleeps.append,
    )

    assert result["requests"] == 2
    assert len(market.calls) == 2
    assert sleeps == [0.25]
    assert store.get_kv("intraday_ibkr_months:SPY") == 2

    resumed = FakeMarket(outcomes=[[bar]])
    result = backfill_ibkr_intraday(
        store, configured, MutableRuns(), lambda: resumed,
        now=datetime(2026, 10, 14, 21, tzinfo=NEW_YORK), sleep=lambda seconds: None,
    )
    assert result["requests"] == 1
    assert len(resumed.calls) == 1
    assert store.get_kv("intraday_ibkr_months:SPY") == 3


def test_ibkr_backfill_stops_after_two_timeouts(store, settings) -> None:
    market = FakeMarket(outcomes=[TimeoutError(), TimeoutError(), []])
    configured = replace(settings, intraday=replace(
        settings.intraday,
        nasdaq_symbols=("SPY",),
        ibkr_backfill_months=3,
        ibkr_request_spacing_seconds=0,
    ))

    result = backfill_ibkr_intraday(
        store, configured, MutableRuns(), lambda: market,
        now=datetime(2026, 10, 13, 21, tzinfo=NEW_YORK), sleep=lambda seconds: None,
    )

    assert result == {"status": "partial", "requests": 2, "rows": 0, "error": "IBKR_TIMEOUT"}
    assert len(market.calls) == 2
    assert store.get_kv("intraday_ibkr_months:SPY") is None


def test_ibkr_backfill_stops_between_requests_when_run_starts(store, settings) -> None:
    runs = MutableRuns()
    market = FakeMarket(outcomes=[[]])
    configured = replace(settings, intraday=replace(
        settings.intraday,
        nasdaq_symbols=("SPY",),
        ibkr_backfill_months=3,
        ibkr_request_spacing_seconds=0,
    ))

    result = backfill_ibkr_intraday(
        store, configured, runs, lambda: market,
        now=datetime(2026, 10, 13, 21, tzinfo=NEW_YORK),
        sleep=lambda seconds: setattr(runs, "busy", True),
    )

    assert result["status"] == "partial"
    assert result["requests"] == 1
    assert len(market.calls) == 1
    assert store.get_kv("intraday_ibkr_last_day") is None


def test_scheduler_checks_intraday_before_busy_return(store, settings) -> None:
    calls: list[tuple[datetime, bool]] = []
    runs = MutableRuns(busy=True)
    now = datetime(2026, 10, 13, 20, 31, tzinfo=NEW_YORK)
    scheduler = Scheduler(
        settings.schedule,
        store,
        runs,
        archive_intraday=lambda moment, startup: calls.append((moment, startup)),
        clock=lambda: now,
    )

    assert scheduler.tick() is None
    assert calls == [(now, False)]


def test_intraday_api_shapes_and_validation(store, settings) -> None:
    timestamp = int(datetime(2026, 10, 13, 14, tzinfo=UTC).timestamp())
    store.merge_intraday("nasdaq", "SPY", [(timestamp, 1, 2, 0.5, 1.5, 10)])
    runtime = Runtime(
        settings,
        Secrets({}),
        store,
        SimpleNamespace(),
        RunManager(lambda trigger, slot: 0),
        None,
    )
    client = TestClient(create_app(runtime))

    status = client.get("/api/intraday/status")
    day = client.get("/api/intraday/nasdaq/spy?day=2026-10-13")

    assert status.status_code == 200
    assert status.json()["sources"][0]["source"] == "nasdaq"
    assert day.json() == {
        "source": "nasdaq",
        "symbol": "SPY",
        "day": "2026-10-13",
        "bars": [[timestamp, 1.0, 2.0, 0.5, 1.5, 10.0]],
    }
    assert client.get("/api/intraday/bad/SPY?day=2026-10-13").status_code == 422
    assert client.get("/api/intraday/nasdaq/SPY-USD?day=2026-10-13").status_code == 422
    assert client.get("/api/intraday/nasdaq/SPY?day=bad").status_code == 422


def test_intraday_settings_do_not_change_params_version(settings) -> None:
    changed = replace(settings, intraday=replace(
        settings.intraday,
        enabled=not settings.intraday.enabled,
        max_news_symbols=settings.intraday.max_news_symbols + 1,
        ibkr_backfill_months=settings.intraday.ibkr_backfill_months + 1,
    ))

    assert params_version(changed) == params_version(settings)


def test_intraday_settings_load_and_validate(tmp_path) -> None:
    loaded = load_settings(
        local_path=tmp_path / "none.toml",
        overrides={"intraday": {
            "nasdaq_symbols": ["spy", "qqq"],
            "jin10_codes": ["spx", "xauusd"],
            "nasdaq_after_et": "19:55",
        }},
    )
    assert loaded.intraday.nasdaq_symbols == ("SPY", "QQQ")
    assert loaded.intraday.jin10_codes == ("SPX", "XAUUSD")
    with pytest.raises(ValueError, match="intraday"):
        load_settings(
            local_path=tmp_path / "none.toml",
            overrides={"intraday": {"ibkr_requests_per_day": 0}},
        )
