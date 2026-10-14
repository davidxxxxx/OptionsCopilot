from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import pytest

from copilot.market.state import market_state, with_own_rank
from copilot.market.models import OptionQuote

from .conftest import TODAY, make_quotes


def test_flat_iv_chain_has_flat_term_skew_and_butterfly() -> None:
    quotes = make_quotes("SPY", 600.0, 0.20, [TODAY + timedelta(days=30), TODAY + timedelta(days=90)])
    state = market_state(quotes, spot=600.0, today=TODAY)
    assert state is not None
    assert state["iv_30d"] == pytest.approx(0.20)
    assert state["iv_90d"] == pytest.approx(0.20)
    assert state["term_slope"] == pytest.approx(0.0)
    assert state["skew_25d"] == pytest.approx(0.0)
    assert state["butterfly_25d"] == pytest.approx(0.0)


def test_put_skew_and_positive_call_gex() -> None:
    base = make_quotes("SPY", 600.0, 0.20, [TODAY + timedelta(days=30)])
    skewed = [replace(q, iv=0.25 if q.right == "P" else q.iv) for q in base]
    state = market_state(skewed, spot=600.0, today=TODAY)
    assert state["skew_25d"] > 0
    calls = [replace(q, gamma=0.01, open_interest=100 if q.right == "C" else 0) for q in base]
    assert market_state(calls, spot=600.0, today=TODAY)["gex"] > 0


def test_near_dated_quote_without_iv_contributes_to_gex_and_call_oi() -> None:
    base = make_quotes("SPY", 600.0, 0.20, [TODAY + timedelta(days=30)])
    baseline = market_state(base, spot=600.0, today=TODAY)
    near_call = replace(
        next(quote for quote in base if quote.right == "C"),
        expiry=TODAY + timedelta(days=1),
        iv=None,
        gamma=0.02,
        open_interest=10_000,
        volume=2_000,
    )
    expanded = market_state([*base, near_call], spot=600.0, today=TODAY)
    assert expanded["gex"] > baseline["gex"]
    assert expanded["oi_put_call"] < baseline["oi_put_call"]
    assert expanded["volume_put_call"] < baseline["volume_put_call"]


def test_missing_iv_returns_none_and_own_rank_needs_twenty_days() -> None:
    quotes = [replace(q, iv=None) for q in make_quotes("SPY", 600.0, 0.20, [TODAY + timedelta(days=30)])]
    assert market_state(quotes, spot=600.0, today=TODAY) is None
    short = [{"iv_30d": 0.10 + index / 1000} for index in range(19)]
    assert with_own_rank(short[-1], short)["own_iv_rank"] is None
    history = [{"iv_30d": 0.10 + index / 100} for index in range(20)]
    ranked = with_own_rank(history[-1], history)
    assert ranked["own_iv_rank"] == pytest.approx(1.0)
    assert ranked["own_iv_percentile"] == pytest.approx(1.0)
    assert ranked["history_days"] == 20


def test_flow_levels_have_known_walls_max_oi_and_max_pain() -> None:
    expiry = date(2026, 11, 20)
    rows = [
        (90.0, "C", 10, 0.01), (90.0, "P", 40, 0.02),
        (100.0, "C", 100, 0.03), (100.0, "P", 100, 0.03),
        (110.0, "C", 50, 0.04), (110.0, "P", 10, 0.01),
    ]
    quotes = [
        OptionQuote(
            symbol="SPY", expiry=expiry, strike=strike, right=right,
            iv=0.20, delta=0.25 if right == "C" else -0.25,
            gamma=gamma, open_interest=oi,
        )
        for strike, right, oi, gamma in rows
    ]
    state = market_state(quotes, spot=100.0, today=date(2026, 10, 14))
    assert state is not None
    assert state["call_wall"]["strike"] == 100.0
    assert state["put_wall"]["strike"] == 100.0
    assert state["max_oi_strike"]["strike"] == 100.0
    assert state["max_pain"]["strike"] == 100.0
    assert state["monthly_expiry"] == expiry.isoformat()


def test_monthly_expiry_accepts_thursday_when_third_friday_is_absent() -> None:
    today = date(2026, 4, 1)
    weekly = date(2026, 4, 10)
    holiday_thursday = date(2026, 4, 16)
    quotes = make_quotes("SPY", 600.0, 0.20, [weekly, holiday_thursday])
    state = market_state(quotes, spot=600.0, today=today)
    assert state is not None
    assert state["expiry"] == weekly.isoformat()
    assert state["monthly_expiry"] == holiday_thursday.isoformat()
    assert state["days_to_monthly_opex"] == 15


def test_gex_sign_uses_one_percent_gross_dead_band() -> None:
    expiry = TODAY + timedelta(days=30)
    quotes = [
        OptionQuote(
            symbol="SPY", expiry=expiry, strike=600.0, right="C",
            iv=0.20, delta=0.25, gamma=0.01, open_interest=100,
        ),
        OptionQuote(
            symbol="SPY", expiry=expiry, strike=600.0, right="P",
            iv=0.20, delta=-0.25, gamma=0.01, open_interest=99,
        ),
    ]
    state = market_state(quotes, spot=600.0, today=TODAY)
    assert state is not None
    assert state["gex"] > 0
    assert state["gex_sign"] == 0
