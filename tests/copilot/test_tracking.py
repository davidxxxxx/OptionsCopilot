from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from copilot.market.models import OptionQuote
from copilot.strategies.tracking import (
    entry_record,
    exit_decision,
    leg_exit_price,
    mark_idea,
    settle_unmarked,
    tracking_stats,
)

TODAY = date(2026, 10, 9)
NOW = datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)
EXPIRY = TODAY + timedelta(days=21)


def quote(strike: float, right: str, bid: float | None, ask: float | None) -> OptionQuote:
    return OptionQuote(symbol="XYZ", expiry=EXPIRY, strike=strike, right=right, bid=bid, ask=ask)


def idea(*, net_debit: float = 2.0, max_profit: float | None = 800.0, dte: int = 21) -> dict:
    return {
        "symbol": "XYZ",
        "structure": "bull_call_spread",
        "structure_name": "牛市看涨价差",
        "expiry": EXPIRY.isoformat(),
        "dte": dte,
        "multiplier": 100,
        "legs": [
            {"action": "买入", "quantity": 1, "signed_quantity": 1, "strike": 100.0,
             "right": "C", "expiry": EXPIRY.isoformat(), "fill": 3.1},
            {"action": "卖出", "quantity": 1, "signed_quantity": -1, "strike": 110.0,
             "right": "C", "expiry": EXPIRY.isoformat(), "fill": 1.1},
        ],
        "net_debit": net_debit,
        "net_premium": net_debit * 100,
        "fees": 2.8,
        "max_loss": 202.8,
        "max_profit": max_profit,
        "return_on_risk": 0.08,
        "view_pop": 0.6,
        "market_pop": 0.5,
        "view": {"conviction": 4},
    }


def entry(**overrides) -> dict:
    item = idea(**overrides)
    return entry_record(
        item, run_id=1, slot="manual", trigger="test", pricing="live",
        params_version="abc", market_state=None,
    )


def test_leg_exit_price_long_short_and_missing_sides() -> None:
    assert leg_exit_price(quote(100, "C", 2.0, 2.4), 1, 0.5) == pytest.approx(2.1)
    assert leg_exit_price(quote(100, "C", 2.0, 2.4), -1, 0.5) == pytest.approx(2.3)
    assert leg_exit_price(quote(100, "C", None, 2.0), 1, 0.5) == pytest.approx(0.5)
    assert leg_exit_price(quote(100, "C", 1.5, None), 1, 0.5) == pytest.approx(1.5)
    assert leg_exit_price(quote(100, "C", 1.5, None), -1, 0.5) is None


def test_identical_quotes_lose_entry_and_exit_slippage_plus_fees() -> None:
    long_quote = quote(100, "C", 3.0, 3.4)
    short_quote = quote(110, "C", 1.0, 1.4)
    fill_ratio = 0.5
    long_mid = 3.2
    short_mid = 1.2
    net_debit = (long_mid + 0.1) - (short_mid - 0.1)
    item = entry(net_debit=net_debit)
    mark = mark_idea(
        item,
        {(EXPIRY, 100.0, "C"): long_quote, (EXPIRY, 110.0, "C"): short_quote},
        spot=105.0, today=TODAY, fill_ratio=fill_ratio, marked_at=NOW, quote_time=NOW,
    )
    assert mark is not None
    entry_slippage = net_debit - (long_mid - short_mid)
    exit_value = 3.1 - 1.3
    exit_slippage = (long_mid - short_mid) - exit_value
    expected = -(entry_slippage + exit_slippage) * 100 - item["idea"]["fees"]
    assert mark["pnl"] == pytest.approx(expected)


def test_exit_rules_and_unmarked_settlement(settings) -> None:
    rules = settings.rules
    assert exit_decision(entry(), {"pnl": 600.0, "dte": 15}, today=TODAY, rules=rules)[0] == "take_profit"
    assert exit_decision(entry(), {"pnl": -101.4, "dte": 15}, today=TODAY, rules=rules)[0] == "stop_loss"
    credit = entry(net_debit=-2.0, max_profit=200.0)
    assert exit_decision(credit, {"pnl": 100.0, "dte": 15}, today=TODAY, rules=rules)[0] == "take_profit"
    assert exit_decision(credit, {"pnl": -400.0, "dte": 15}, today=TODAY, rules=rules)[0] == "stop_loss"
    assert exit_decision(entry(), {"pnl": 0.0, "dte": 7}, today=TODAY, rules=rules)[0] == "time_exit"
    near = entry(dte=7)
    assert exit_decision(near, {"pnl": 0.0, "dte": 1}, today=TODAY, rules=rules) is None
    assert exit_decision(near, {"pnl": 0.0, "dte": 0}, today=TODAY, rules=rules)[0] == "expiry"
    past = TODAY + timedelta(days=22)
    settled = settle_unmarked(entry(), {"pnl": 12.0, "pnl_mid": 14.0, "spot": 108.0,
                                              "value": 2.2, "return_on_risk": 0.05}, today=past)
    assert settled["code"] == "expired_unmarked" and settled["approximate"] is True
    assert settled["pnl"] == 12.0
    unsettled = settle_unmarked(entry(), None, today=past)
    assert unsettled["pnl"] is None and unsettled["approximate"] is True


def test_tracking_stats_and_edge_buckets() -> None:
    rows = [
        {"status": "closed", "origin": "news", "structure": "vertical", "params_version": "a",
         "entry": {"idea": {"max_loss": 100.0, "return_on_risk": 0.01, "view_pop": 0.6,
                              "market_pop": 0.5, "view": {"conviction": 3},
                              "macro_events": [{"date": "2026-10-14", "name": "CPI"}]}},
         "exit": {"pnl": 20.0, "return_on_risk": 0.2, "code": "take_profit"}},
        {"status": "closed", "origin": "ladder", "structure": "vertical", "params_version": "b",
         "entry": {"idea": {"max_loss": 200.0, "return_on_risk": 0.07, "view_pop": 0.55,
                              "market_pop": 0.5, "view": None}},
         "exit": {"pnl": -40.0, "return_on_risk": -0.2, "code": "stop_loss"}},
        {"status": "closed", "origin": "news", "structure": "long", "params_version": "a",
         "entry": {"idea": {"max_loss": 50.0, "return_on_risk": 0.12}},
         "exit": {"pnl": None, "code": "expired_unmarked"}},
        {"status": "open", "origin": "news", "structure": "vertical", "params_version": "a",
         "entry": {"idea": {"max_loss": 100.0, "return_on_risk": 0.03}}, "mark": {"pnl": 5.0}},
    ]
    stats = tracking_stats(rows)
    assert stats["closed"] == 3 and stats["open"] == 1 and stats["unsettled"] == 1
    assert stats["wins"] == 1 and stats["win_rate"] == pytest.approx(0.5)
    assert stats["pnl_total"] == -20.0 and stats["risk_total"] == 300.0
    assert stats["open_unrealized"] == 5.0
    assert stats["exit_reasons"] == {"take_profit": 1, "stop_loss": 1, "expired_unmarked": 1}
    assert set(stats["by_edge_bucket"]) == {"<2%", "5–10%"}
    assert set(stats["by_macro_event"]) == {"跨越重大事件", "不跨越"}
