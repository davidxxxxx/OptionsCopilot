from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from copilot.market.state import market_state, with_own_rank

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
