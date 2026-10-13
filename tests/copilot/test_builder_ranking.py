from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from copilot.analysis.models import View
from copilot.config import StrategySettings
from copilot.market.models import AccountSnapshot, Position
from dataclasses import replace

from copilot.market.models import OptionChain
from copilot.strategies.builder import (
    build_candidates,
    select_expiries,
    structures_for,
)
from copilot.strategies.ranking import rank_candidates

from .conftest import TODAY, make_chain

SETTINGS = StrategySettings()


def view(direction: str, *, conviction: int = 4, move: float = 0.06, vol: str = "neutral", symbol: str = "SPY") -> View:
    return View(symbol=symbol, direction=direction, conviction=conviction, expected_move_pct=move,
                horizon_days=14, volatility=vol)


def test_structures_follow_the_view() -> None:
    assert structures_for(view("bullish")) == ["bull_call_spread", "bull_put_spread", "long_call", "call_diagonal"]
    assert structures_for(view("bearish", conviction=2)) == ["bear_put_spread", "bear_call_spread"]
    assert structures_for(view("neutral", vol="expand")) == ["long_straddle", "long_strangle"]
    assert {"iron_condor", "call_calendar"} <= set(structures_for(view("neutral")))


def test_select_expiries_respects_horizon_and_catalyst() -> None:
    available = [TODAY + timedelta(days=d) for d in (2, 7, 14, 21, 28, 70)]
    chosen = select_expiries(available, TODAY, view("bullish"), SETTINGS)
    assert chosen == [TODAY + timedelta(days=21), TODAY + timedelta(days=28)]
    event_view = View(symbol="JPM", direction="neutral", conviction=2, expected_move_pct=0.04,
                      horizon_days=3, catalyst_date=TODAY + timedelta(days=15))
    assert select_expiries(available, TODAY, event_view, SETTINGS)[0] == TODAY + timedelta(days=21)


def test_bullish_view_produces_positive_ev_bullish_structures() -> None:
    chain = make_chain(spot=600, iv=0.18)
    candidates = build_candidates(view("bullish", conviction=5, move=0.05), chain, SETTINGS, today=TODAY)
    assert candidates
    assert {c.structure for c in candidates} <= {"bull_call_spread", "bull_put_spread", "long_call"}
    best = max(candidates, key=lambda c: c.return_on_risk)
    assert best.edge > 0
    # Under the market distribution the same trade is worth less than under the view.
    assert best.market_ev < best.view_ev
    for candidate in candidates:
        assert candidate.profile.max_loss > 0


def test_bearish_view_produces_bearish_structures() -> None:
    chain = make_chain(spot=600, iv=0.18)
    candidates = build_candidates(view("bearish", conviction=5, move=0.05), chain, SETTINGS, today=TODAY)
    assert candidates
    assert {c.structure for c in candidates} <= {"bear_put_spread", "bear_call_spread", "long_put"}
    assert max(c.edge for c in candidates) > 0


def test_neutral_view_without_edge_is_not_positive_after_costs() -> None:
    chain = make_chain(spot=600, iv=0.18)
    candidates = build_candidates(view("neutral", conviction=1, move=0.0), chain, SETTINGS, today=TODAY)
    assert candidates
    assert {c.structure for c in candidates} <= {"iron_condor", "iron_butterfly", "call_butterfly"}
    # No view means no edge: only slippage and fees remain.
    assert all(c.edge < 0 for c in candidates)


def test_rich_implied_vol_versus_realised_favours_premium_selling() -> None:
    chain = make_chain(spot=600, iv=0.30, hv20=0.15)
    candidates = build_candidates(view("neutral", conviction=1, move=0.0), chain, SETTINGS, today=TODAY)
    assert max(c.edge for c in candidates) > 0


def test_illiquid_quotes_are_ignored() -> None:
    chain = make_chain(spot=600, iv=0.18, open_interest=5)
    assert build_candidates(view("bullish"), chain, SETTINGS, today=TODAY) == []


def test_ranking_caps_per_symbol_and_sizes_positions() -> None:
    views = {"SPY": view("bullish", conviction=5, move=0.05), "QQQ": view("bearish", conviction=5, move=0.05, symbol="QQQ")}
    candidates = (
        build_candidates(views["SPY"], make_chain("SPY", 600, 0.18), SETTINGS, today=TODAY)
        + build_candidates(views["QQQ"], make_chain("QQQ", 500, 0.2), SETTINGS, today=TODAY)
    )
    account = AccountSnapshot(
        observed_at=datetime.now(timezone.utc), net_liquidation=20_000.0,
        positions=(Position(symbol="SPY", sec_type="STK", quantity=5, avg_cost=590.0),),
    )
    ideas = rank_candidates(candidates, views=views, settings=SETTINGS, account=account)
    assert ideas
    assert [idea["rank"] for idea in ideas] == list(range(1, len(ideas) + 1))
    for symbol in ("SPY", "QQQ"):
        assert sum(1 for idea in ideas if idea["symbol"] == symbol) <= SETTINGS.max_ideas_per_symbol
    scores = [idea["return_on_risk"] for idea in ideas]
    assert scores == sorted(scores, reverse=True)
    first = ideas[0]
    assert first["edge"] > 0
    assert first["suggested_quantity"] == int(2000 // first["max_loss"])
    assert any(idea["already_holding"] for idea in ideas if idea["symbol"] == "SPY")
    assert first["legs"] and first["legs"][0]["action"] in ("买入", "卖出")
    assert first["view"]["symbol"] == first["symbol"]


def test_closed_market_uses_flagged_reference_prices() -> None:
    chain = make_chain(spot=600, iv=0.18)
    closed = OptionChain(
        underlying=chain.underlying,
        quotes=tuple(replace(q, bid=None, ask=None, model_price=(q.bid + q.ask) / 2) for q in chain.quotes),
    )
    strong = view("bullish", conviction=5, move=0.05)
    assert build_candidates(strong, closed, SETTINGS, today=TODAY) == []
    found = build_candidates(strong, closed, SETTINGS, today=TODAY, indicative_prices=True)
    assert found and all(leg.quote.indicative for c in found for leg in c.legs)
    ideas = rank_candidates(found, views={"SPY": strong}, settings=SETTINGS)
    assert ideas and ideas[0]["indicative"] is True


def test_model_drift_is_capped_in_sigma_units() -> None:
    import math

    wild = View(symbol="SPY", direction="bullish", conviction=5, expected_move_pct=0.5, horizon_days=14)
    found = build_candidates(wild, make_chain(spot=600, iv=0.18), SETTINGS, today=TODAY)
    cap = SETTINGS.max_drift_sigma * 0.18 * math.sqrt(14 / 365)
    assert found
    assert all(abs(c.drift) <= cap + 1e-9 for c in found)
    assert max(c.drift for c in found) == pytest.approx(cap, rel=0.05)


def test_volatility_expansion_needs_a_dated_catalyst() -> None:
    chain = make_chain(spot=600, iv=0.18)
    vague = View(symbol="SPY", direction="neutral", conviction=3, expected_move_pct=0.0,
                 horizon_days=14, volatility="expand")
    assert {c.structure for c in build_candidates(vague, chain, SETTINGS, today=TODAY)} <= {
        "iron_condor", "iron_butterfly", "call_butterfly"}
    dated = View(symbol="SPY", direction="neutral", conviction=3, expected_move_pct=0.0,
                 horizon_days=14, volatility="expand", catalyst_date=TODAY + timedelta(days=5))
    assert {c.structure for c in build_candidates(dated, chain, SETTINGS, today=TODAY)} <= {
        "long_straddle", "long_strangle"}


def test_small_account_only_sees_ideas_within_the_loss_cap() -> None:
    strong = view("bullish", conviction=5, move=0.05)
    candidates = build_candidates(strong, make_chain(spot=600, iv=0.18), SETTINGS, today=TODAY)
    account = AccountSnapshot(observed_at=datetime.now(timezone.utc), net_liquidation=2_000.0)
    ideas = rank_candidates(candidates, views={"SPY": strong}, settings=SETTINGS, account=account)
    assert ideas
    assert all(idea["max_loss"] <= 400 for idea in ideas)
    assert all(idea["suggested_quantity"] >= 1 for idea in ideas)


def test_calendars_and_diagonals_are_priced_with_defined_risk() -> None:
    expiries = (TODAY + timedelta(days=21), TODAY + timedelta(days=49), TODAY + timedelta(days=77))
    chain = make_chain(spot=600, iv=0.20, expiries=expiries)
    bullish = view("bullish", conviction=5, move=0.05)
    diagonals = [c for c in build_candidates(bullish, chain, SETTINGS, today=TODAY, expiries=expiries[:1])
                 if c.structure == "call_diagonal"]
    assert diagonals
    for c in diagonals:
        front, back = sorted(c.legs, key=lambda leg: leg.quote.expiry)
        assert front.quantity == -1 and back.quantity == 1 and front.strike > back.strike
        # PMCC: the long back call covers the short front call, so risk is about the debit.
        assert c.profile.max_loss == pytest.approx(c.profile.net_debit * 100 + c.profile.fees, rel=0.05)
        assert c.profile.max_profit is not None and c.profile.max_profit > 0
    neutral = view("neutral", conviction=1, move=0.0)
    calendars = [c for c in build_candidates(neutral, chain, SETTINGS, today=TODAY, expiries=expiries[:1])
                 if c.structure == "call_calendar"]
    assert calendars
    cal = calendars[0]
    assert len({leg.quote.expiry for leg in cal.legs}) == 2 and len(cal.profile.breakevens) == 2
    assert cal.edge < 0  # no view: only costs remain, same as single-expiry structures


def test_earnings_gate_blocks_short_premium_spanning_earnings() -> None:
    chain = make_chain(spot=600, iv=0.25, hv20=0.12)
    neutral = view("neutral", conviction=1, move=0.0)
    before = {c.structure for c in build_candidates(neutral, chain, SETTINGS, today=TODAY)}
    assert "iron_condor" in before
    earnings = [TODAY + timedelta(days=10)]
    after = {c.structure for c in build_candidates(neutral, chain, SETTINGS, today=TODAY, event_dates=earnings)}
    assert not after & {"iron_condor", "iron_butterfly", "call_calendar"}
    assert "call_butterfly" in after  # long-premium structures stay available
    relaxed = replace(SETTINGS, no_short_premium_through_earnings=False)
    assert "iron_condor" in {c.structure for c in build_candidates(
        neutral, chain, relaxed, today=TODAY, event_dates=earnings)}


def test_macro_catalyst_keeps_expand_view_in_long_vol_structures() -> None:
    chain = make_chain(spot=600, iv=0.18)
    expanding = view("neutral", conviction=3, move=0.0, vol="expand")
    without = {candidate.structure for candidate in build_candidates(
        expanding, chain, SETTINGS, today=TODAY,
    )}
    with_macro = {candidate.structure for candidate in build_candidates(
        expanding, chain, SETTINGS, today=TODAY,
        catalyst_dates=[TODAY + timedelta(days=10)],
    )}
    assert not without & {"long_straddle", "long_strangle"}
    assert with_macro and with_macro <= {"long_straddle", "long_strangle"}


def test_macro_catalyst_does_not_trigger_earnings_short_premium_gate() -> None:
    chain = make_chain(spot=600, iv=0.25, hv20=0.12)
    neutral = view("neutral", conviction=1, move=0.0)
    macro = [TODAY + timedelta(days=10)]
    structures = {candidate.structure for candidate in build_candidates(
        neutral, chain, SETTINGS, today=TODAY, catalyst_dates=macro,
    )}
    earnings = {candidate.structure for candidate in build_candidates(
        neutral, chain, SETTINGS, today=TODAY, event_dates=macro,
    )}
    assert "iron_condor" in structures
    assert "iron_condor" not in earnings


def test_view_can_pin_structures_and_expiry_window() -> None:
    expiries = (TODAY + timedelta(days=21), TODAY + timedelta(days=120))
    chain = make_chain(spot=600, iv=0.2, expiries=expiries)
    ladder = View(symbol="SPY", direction="bullish", conviction=5, expected_move_pct=0.0, horizon_days=120,
                  drift_sigma=0.3, structures=("bull_call_spread", "long_call"), dte_min=90, dte_max=180,
                  source="ladder")
    found = build_candidates(ladder, chain, SETTINGS, today=TODAY)
    assert found and {c.structure for c in found} <= {"bull_call_spread", "long_call"}
    assert {c.expiry for c in found} == {expiries[1]} and all(c.origin == "ladder" for c in found)
    assert select_expiries(list(expiries), TODAY, ladder, SETTINGS) == [expiries[1]]


def test_rebuild_supports_calendars_and_respects_the_earnings_gate() -> None:
    from copilot.strategies.builder import rebuild_candidate

    expiries = (TODAY + timedelta(days=21), TODAY + timedelta(days=49))
    chain = make_chain(spot=600, iv=0.2, expiries=expiries)
    legs = [(expiries[0], 600.0, "C", -1), (expiries[1], 600.0, "C", 1)]
    neutral = view("neutral", conviction=1, move=0.0)
    rebuilt = rebuild_candidate(neutral, chain, SETTINGS, today=TODAY, structure="call_calendar", legs=legs)
    assert rebuilt is not None and rebuilt.structure == "call_calendar"
    assert rebuild_candidate(neutral, chain, SETTINGS, today=TODAY, structure="call_calendar", legs=legs,
                             event_dates=[TODAY + timedelta(days=7)]) is None
