from __future__ import annotations

import math
from datetime import date

import numpy as np
import pytest

from copilot.market.models import OptionQuote
from copilot.strategies.payoff import Leg, analyze_payoff
from copilot.strategies.pricing import bs_price, implied_vol, terminal_distribution

EXP = date(2026, 11, 20)


def leg(right: str, strike: float, quantity: int, price: float) -> Leg:
    return Leg(OptionQuote(symbol="X", expiry=EXP, strike=strike, right=right), quantity, price)


def test_bull_call_spread_profile() -> None:
    profile = analyze_payoff(
        [leg("C", 100, 1, 4.0), leg("C", 110, -1, 1.5)], multiplier=100, commission_per_contract=0
    )
    assert profile is not None
    assert profile.net_debit == pytest.approx(2.5)
    assert profile.max_loss == pytest.approx(250)
    assert profile.max_profit == pytest.approx(750)
    assert profile.breakevens == (102.5,)


def test_iron_condor_profile_and_fees() -> None:
    legs = [leg("P", 90, -1, 1.2), leg("P", 85, 1, 0.5), leg("C", 110, -1, 1.1), leg("C", 115, 1, 0.4)]
    profile = analyze_payoff(legs, multiplier=100, commission_per_contract=0.7)
    assert profile is not None
    credit = 1.4
    fees = 0.7 * 4 * 2
    assert profile.net_debit == pytest.approx(-credit)
    assert profile.max_profit == pytest.approx(credit * 100 - fees)
    assert profile.max_loss == pytest.approx((5 - credit) * 100 + fees)
    assert len(profile.breakevens) == 2
    assert profile.breakevens[0] < 90 < 110 < profile.breakevens[1]


def test_straddle_has_unlimited_profit_and_two_breakevens() -> None:
    profile = analyze_payoff([leg("C", 100, 1, 3.0), leg("P", 100, 1, 3.0)], multiplier=100, commission_per_contract=0)
    assert profile is not None
    assert profile.max_profit is None
    assert profile.max_loss == pytest.approx(600)
    assert profile.breakevens == (94.0, 106.0)


def test_naked_short_call_is_rejected() -> None:
    assert analyze_payoff([leg("C", 100, -1, 3.0)], multiplier=100, commission_per_contract=0) is None


def test_butterfly_ratio_leg() -> None:
    profile = analyze_payoff(
        [leg("C", 95, 1, 7.0), leg("C", 100, -2, 4.0), leg("C", 105, 1, 2.0)],
        multiplier=100, commission_per_contract=0,
    )
    assert profile is not None
    assert profile.max_loss == pytest.approx(100)
    assert profile.max_profit == pytest.approx(400)


def test_distribution_preserves_forward_and_prices_options_fairly() -> None:
    grid = terminal_distribution(100.0, 0.25, 30 / 365, tail_weight=0.0)
    assert grid.mean == pytest.approx(100.0, rel=1e-4)
    call_payoff = np.maximum(grid.prices - 100.0, 0.0)
    expected = bs_price(100.0, 100.0, 30 / 365, 0.25, "C") * math.exp(0.04 * 30 / 365)
    # Undiscounted BS price with forward = spot*e^{rT}: compare against forward-based value.
    forward_grid = terminal_distribution(100.0 * math.exp(0.04 * 30 / 365), 0.25, 30 / 365, tail_weight=0.0)
    assert forward_grid.expectation(np.maximum(forward_grid.prices - 100.0, 0.0)) == pytest.approx(expected, rel=2e-3)
    assert grid.expectation(call_payoff) > 0


def test_fat_tail_mixture_keeps_mean() -> None:
    grid = terminal_distribution(250.0, 0.3, 0.1, tail_weight=0.15, tail_multiple=1.6, core_multiple=0.88)
    assert grid.mean == pytest.approx(250.0, rel=1e-4)
    assert grid.weights.sum() == pytest.approx(1.0)


def test_implied_vol_round_trip() -> None:
    price = bs_price(100, 105, 45 / 365, 0.32, "C")
    assert implied_vol(price, 100, 105, 45 / 365, "C") == pytest.approx(0.32, abs=1e-4)
    assert implied_vol(0.0, 100, 105, 45 / 365, "C") is None


def test_mixture_matches_atm_price_but_fattens_wings() -> None:
    t = 30 / 365
    flat = terminal_distribution(100.0, 0.25, t, tail_weight=0.0)
    fat = terminal_distribution(100.0, 0.25, t, tail_weight=0.15, tail_multiple=1.6, core_multiple=0.88)
    atm = lambda grid: grid.expectation(np.maximum(grid.prices - 100.0, 0.0))
    assert atm(fat) == pytest.approx(atm(flat), rel=2e-3)
    wing = lambda grid: grid.expectation(np.maximum(80.0 - grid.prices, 0.0))
    assert wing(fat) > wing(flat) * 1.2
