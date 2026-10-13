from __future__ import annotations

from datetime import date, timedelta

from copilot.market import nasdaq_history
from copilot.pipeline import _History

from .conftest import TODAY


def payload(*rows: tuple[str, str]) -> dict:
    return {"data": {"tradesTable": {"rows": [
        {"date": day, "close": close} for day, close in rows
    ]}}}


def bar_payload(*rows: tuple[str, str, str, str, str]) -> dict:
    return {"data": {"tradesTable": {"rows": [
        {"date": day, "open": open_, "high": high, "low": low, "close": close}
        for day, open_, high, low, close in rows
    ]}}}


def test_daily_closes_falls_back_from_etf_to_stocks(monkeypatch) -> None:
    calls = []

    def request_json(*args, **kwargs):
        calls.append(kwargs["params"]["assetclass"])
        return payload() if calls[-1] == "etf" else payload(("10/10/2026", "$123.45"))

    monkeypatch.setattr(nasdaq_history, "request_json", request_json)
    assert nasdaq_history.daily_closes("SPY", today=date(2026, 10, 13)) == [
        (date(2026, 10, 10), 123.45)
    ]
    assert calls == ["etf", "stocks"]


def test_daily_closes_falls_back_from_stocks_to_etf(monkeypatch) -> None:
    calls = []

    def request_json(*args, **kwargs):
        calls.append(kwargs["params"]["assetclass"])
        return payload() if calls[-1] == "stocks" else payload(("10/10/2026", "99"))

    monkeypatch.setattr(nasdaq_history, "request_json", request_json)
    assert nasdaq_history.daily_closes("AAPL", today=date(2026, 10, 13)) == [
        (date(2026, 10, 10), 99.0)
    ]
    assert calls == ["stocks", "etf"]


def test_daily_closes_explicit_assetclass_does_not_fallback(monkeypatch) -> None:
    calls = []

    def request_json(*args, **kwargs):
        calls.append(kwargs["params"]["assetclass"])
        return payload()

    monkeypatch.setattr(nasdaq_history, "request_json", request_json)
    assert nasdaq_history.daily_closes("SPY", today=date(2026, 10, 13), assetclass="etf") == []
    assert calls == ["etf"]


def test_parse_bars_reads_ohlc_and_sorts_oldest_first() -> None:
    assert nasdaq_history.parse_bars(bar_payload(
        ("10/10/2026", "$101", "$103", "$99", "$102"),
        ("10/09/2026", "100", "102", "98", "101"),
        ("bad", "1", "2", "0", "1"),
    )) == [
        (date(2026, 10, 9), 100.0, 102.0, 98.0, 101.0),
        (date(2026, 10, 10), 101.0, 103.0, 99.0, 102.0),
    ]


def test_daily_bars_falls_back_and_uses_history_range(monkeypatch) -> None:
    calls = []

    def request_json(*args, **kwargs):
        calls.append(kwargs["params"])
        return bar_payload() if len(calls) == 1 else bar_payload(
            ("10/09/2026", "$100", "$102", "$98", "$101")
        )

    monkeypatch.setattr(nasdaq_history, "request_json", request_json)
    rows = nasdaq_history.daily_bars(
        "SPY", today=date(2026, 10, 13), start=date(2021, 1, 1)
    )
    assert rows == [(date(2026, 10, 9), 100.0, 102.0, 98.0, 101.0)]
    assert [call["assetclass"] for call in calls] == ["etf", "stocks"]
    assert all(call["fromdate"] == "2021-01-01" and call["limit"] == 2000 for call in calls)


def test_weekly_cache_date_is_monday() -> None:
    assert nasdaq_history.weekly_cache_date(date(2026, 10, 12)) == date(2026, 10, 12)
    assert nasdaq_history.weekly_cache_date(date(2026, 10, 18)) == date(2026, 10, 12)


def test_two_nasdaq_failures_stop_further_calls_for_the_run(store) -> None:
    calls = []

    def history(symbol: str, today: date):
        calls.append(symbol)
        return []

    source = _History(None, store, TODAY, 1.0, history)
    assert source.nasdaq_closes("SPY") == []
    assert source.nasdaq_closes("QQQ") == []
    assert source.nasdaq_closes("IWM") == []
    assert calls == ["SPY", "QQQ"]
    assert source.nasdaq_disabled


def test_ladder_and_history_reuse_one_daily_cache_fetch(store) -> None:
    calls = []

    def history(symbol: str, today: date):
        calls.append(symbol)
        return [(today - timedelta(days=index), 100.0 + index) for index in range(40, 0, -1)]

    source = _History(None, store, TODAY, 1.0, history)
    first = source.nasdaq_closes("SPY")
    second = source.nasdaq_closes("SPY")
    assert first == second
    assert calls == ["SPY"]
    assert source.closes_source in {"nasdaq", "cache"}
