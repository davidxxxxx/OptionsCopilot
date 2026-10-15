from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

import pytest

from copilot.analysis.models import View
from copilot.config import StrategySettings
from copilot.market.models import AccountSnapshot, OptionQuote, Position
from copilot.strategies.builder import build_candidates
from copilot.strategies.decision_portfolio import (
    allocate_joint_budget,
    annotate_idea_beta,
    annotate_relationships,
    beta_to_spy,
    combined_exposure,
    fixed_vertical_benchmark,
    match_open_position,
    stress_test,
    structure_alternatives,
)
from copilot.strategies.portfolio import PositionGroup
from copilot.strategies.pricing import bs_price
from copilot.strategies.ranking import to_idea
from copilot.strategies.tracking import entry_record, mark_idea, tracking_stats

from .conftest import TODAY, make_chain


def _view(direction: str = "bullish", symbol: str = "SPY") -> View:
    return View(
        symbol=symbol,
        direction=direction,
        conviction=5,
        expected_move_pct=0.05,
        horizon_days=14,
    )


def _idea(
    symbol: str,
    spy_delta: float,
    *,
    origin: str = "news",
    max_loss: float = 200.0,
) -> dict:
    return {
        "symbol": symbol,
        "structure": "bear_put_spread" if spy_delta < 0 else "bull_call_spread",
        "structure_name": "测试价差",
        "origin": origin,
        "spot": 100.0,
        "max_loss": max_loss,
        "suggested_quantity": 1,
        "fees": 2.0,
        "slippage": 3.0,
        "greeks": {
            "delta_shares": spy_delta / 100.0,
            "delta_dollars": spy_delta,
            "spy_equivalent_delta": spy_delta,
            "vega": 5.0,
            "theta": -1.0,
        },
    }


def _quote(
    strike: float,
    right: str,
    bid: float,
    ask: float,
    *,
    delta: float,
    iv: float = 0.25,
    expiry: date | None = None,
) -> OptionQuote:
    return OptionQuote(
        symbol="XYZ",
        expiry=expiry or TODAY + timedelta(days=30),
        strike=strike,
        right=right,
        bid=bid,
        ask=ask,
        delta=delta,
        iv=iv,
        gamma=0.01,
        vega=0.08,
        theta=-0.02,
    )


def test_beta_spy_equivalent_delta_and_missing_fallback() -> None:
    spy = [(TODAY, 100.0)]
    asset = [(TODAY, 50.0)]
    for index in range(1, 70):
        spy_return = 0.002 + 0.004 * math.sin(index / 3)
        spy.append((TODAY + timedelta(days=index), spy[-1][1] * math.exp(spy_return)))
        asset.append((TODAY + timedelta(days=index), asset[-1][1] * math.exp(2 * spy_return)))
    assert beta_to_spy(asset, spy) == pytest.approx(2.0)
    idea = _idea("XYZ", 100.0)
    annotate_idea_beta(idea, 1.5)
    assert idea["greeks"]["spy_equivalent_delta"] == 150.0
    annotate_idea_beta(idea, None)
    assert idea["beta_to_spy"] == 1.0 and idea["beta_fallback"] is True


def test_relationship_labels_cover_conflicts_alternatives_and_positions() -> None:
    spy_news = _idea("SPY", -100.0)
    spy_vrp = _idea("SPY", 60.0, origin="vrp")
    iwm_news = _idea("IWM", -80.0)
    iwm_ladder = _idea("IWM", 40.0, origin="ladder")
    qqq_one = _idea("QQQ", 30.0)
    qqq_two = _idea("QQQ", 20.0)
    hedge = _idea("XLE", -20.0)
    add = _idea("EEM", 20.0)
    positions = [
        PositionGroup("XLE", [], 100, 0.0, 0.0, 0.0, net_delta=1.0, spot=100.0),
        PositionGroup("EEM", [], 100, 0.0, 0.0, 0.0, net_delta=1.0, spot=100.0),
    ]
    ideas = [spy_news, spy_vrp, iwm_news, iwm_ladder, qqq_one, qqq_two, hedge, add]
    conflicts = annotate_relationships(ideas, positions)
    assert len(conflicts) >= 2
    assert any(item["type"] == "冲突" for item in spy_news["relationships"])
    assert any(item["type"] == "冲突" for item in iwm_news["relationships"])
    assert any(item["type"] == "替代方案" for item in qqq_one["relationships"])
    assert any(item["type"] == "对冲现有持仓" for item in hedge["relationships"])
    assert any(item["type"] == "加大现有暴露" for item in add["relationships"])


def test_joint_budget_allocates_main_event_vrp_in_order() -> None:
    account = AccountSnapshot(datetime.now(timezone.utc), net_liquidation=10_000.0)
    settings = StrategySettings(risk_per_trade_pct=0.10)
    main = [_idea("SPY", 10.0, max_loss=600.0)]
    event = [_idea("QQQ", 10.0, origin="macro", max_loss=600.0)]
    vrp = [_idea("IWM", 10.0, origin="vrp", max_loss=600.0)]
    result = allocate_joint_budget(
        [main, event, vrp],
        account=account,
        settings=settings,
        remaining=1_500.0,
        origin_verified={},
    )
    assert [main[0]["suggested_quantity"], event[0]["suggested_quantity"], vrp[0]["suggested_quantity"]] == [1, 1, 0]
    assert vrp[0]["quantity_reason"] == "组合预算已被前面的推荐用完"
    assert result["remaining"] == 300.0


def test_same_symbol_direction_alternatives_only_size_highest_ranked_idea() -> None:
    account = AccountSnapshot(datetime.now(timezone.utc), net_liquidation=2_000.0)
    settings = StrategySettings(risk_per_trade_pct=0.20)
    long_put = _idea("TLT", -20.0, max_loss=400.0)
    long_put.update({"rank": 1, "structure": "long_put", "structure_name": "买入 Put"})
    bear_spread = _idea("TLT", -15.0, max_loss=400.0)
    bear_spread.update({"rank": 2, "structure": "bear_put_spread", "structure_name": "熊市看跌价差"})

    annotate_relationships([long_put, bear_spread], [])
    result = allocate_joint_budget(
        [[long_put, bear_spread]],
        account=account,
        settings=settings,
        remaining=800.0,
        origin_verified={},
    )

    assert any(item["type"] == "替代方案" for item in long_put["relationships"])
    assert long_put["suggested_quantity"] == 1
    assert bear_spread["suggested_quantity"] == 0
    assert bear_spread["quantity_reason"] == "与 #1 互为替代方案，只需选一个"
    assert result == {"starting_budget": 800.0, "allocated": 400.0, "remaining": 400.0}


def test_unverified_budget_defaults_equal_and_can_shrink() -> None:
    account = AccountSnapshot(datetime.now(timezone.utc), net_liquidation=10_000.0)
    normal = StrategySettings(risk_per_trade_pct=0.10)
    assert normal.unverified_risk_per_trade_pct == normal.risk_per_trade_pct
    idea = _idea("SPY", 10.0, max_loss=200.0)
    idea["news_unverified"] = True
    allocate_joint_budget([[idea]], account=account, settings=normal, remaining=None, origin_verified={})
    assert idea["suggested_quantity"] == 5
    reduced = _idea("SPY", 10.0, max_loss=200.0)
    reduced["news_unverified"] = True
    allocate_joint_budget(
        [[reduced]],
        account=account,
        settings=StrategySettings(risk_per_trade_pct=0.10, unverified_risk_per_trade_pct=0.03),
        remaining=None,
        origin_verified={},
    )
    assert reduced["suggested_quantity"] == 1


def test_unverified_budget_does_not_use_one_contract_exception() -> None:
    account = AccountSnapshot(datetime.now(timezone.utc), net_liquidation=2_000.0)
    settings = StrategySettings(
        risk_per_trade_pct=0.01,
        unverified_risk_per_trade_pct=0.01,
    )
    unverified = _idea("SPY", 10.0, max_loss=40.0)
    unverified["news_unverified"] = True
    verified = _idea("QQQ", 10.0, max_loss=40.0)

    allocate_joint_budget(
        [[unverified, verified]],
        account=account,
        settings=settings,
        remaining=100.0,
        origin_verified={},
    )

    assert unverified["suggested_quantity"] == 0
    assert unverified["quantity_reason"] == "超出未验证推荐的单笔预算"
    assert verified["suggested_quantity"] == 1
    assert "quantity_reason" not in verified


def test_stress_test_matches_black_scholes_repricing() -> None:
    expiry = TODAY + timedelta(days=30)
    idea = {
        "symbol": "XYZ", "spot": 100.0, "beta_to_spy": 1.0,
        "suggested_quantity": 1, "dte": 30, "multiplier": 100,
        "fees": 0.0, "atm_iv": 0.25,
        "legs": [{
            "strike": 100.0, "right": "C", "signed_quantity": 1,
            "fill": 4.0, "iv": 0.25, "expiry": expiry.isoformat(),
        }],
    }
    row = next(item for item in stress_test([idea], [], today=TODAY) if item["scenario"] == "市场 +3%")
    expected = (bs_price(103.0, 100.0, 29 / 365, 0.25, "C") - 4.0) * 100
    assert row["ideas_pnl"] == pytest.approx(expected, abs=0.01)


def test_structure_alternatives_normalize_to_listed_max_loss() -> None:
    settings = StrategySettings(min_expected_return_pct=-0.5)
    candidates = build_candidates(_view(), make_chain(), settings, today=TODAY)
    listed_candidate = candidates[0]
    listed = to_idea(
        listed_candidate, 1, view=_view(), settings=settings, account=None, portfolio=None
    )
    rows = structure_alternatives(listed, candidates, settings)
    assert rows
    selected = next(row for row in rows if row["structure"] == listed_candidate.structure)
    assert selected["normalization"] == pytest.approx(1.0)
    assert selected["explanation"]
    assert {"expected_return", "pop", "breakeven_sigma", "vega", "theta", "admissible"} <= set(selected)


def test_attribution_benchmarks_and_direction_hit() -> None:
    expiry = TODAY + timedelta(days=30)
    long_quote = _quote(100, "C", 4.8, 5.2, delta=0.55, expiry=expiry)
    short_quote = _quote(110, "C", 1.8, 2.2, delta=0.30, expiry=expiry)
    item = {
        "symbol": "XYZ", "structure": "bull_call_spread", "structure_name": "牛市看涨价差",
        "spot": 100.0, "dte": 30, "multiplier": 100, "net_debit": 3.0,
        "fees": 2.0, "slippage": 4.0, "max_loss": 302.0, "atm_iv": 0.25,
        "greeks": {"delta_shares": 25.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0},
        "view": {"direction": "bullish"},
        "legs": [
            {"expiry": expiry.isoformat(), "strike": 100.0, "right": "C", "signed_quantity": 1, "fill": 5.0},
            {"expiry": expiry.isoformat(), "strike": 110.0, "right": "C", "signed_quantity": -1, "fill": 2.0},
        ],
    }
    item["benchmarks"] = {
        "entry_spot": 100.0,
        "delta_shares": 25.0,
        "vertical": fixed_vertical_benchmark(
            [long_quote, short_quote], symbol="XYZ", spot=100.0, today=TODAY,
            direction="bullish", max_loss=302.0, fill_ratio=0.0,
        ),
    }
    entry = entry_record(item, run_id=1, slot="manual", trigger="test", pricing="live", params_version="x", market_state=None)
    quotes = {(expiry, 100.0, "C"): long_quote, (expiry, 110.0, "C"): short_quote}
    mark = mark_idea(
        entry, quotes, spot=102.0, today=TODAY, fill_ratio=0.0,
        marked_at=datetime(2026, 10, 9, 15, tzinfo=timezone.utc), quote_time=None,
    )
    assert mark is not None
    assert sum(mark["attribution"].values()) == pytest.approx(mark["pnl"], abs=0.01)
    assert mark["benchmarks"]["underlying"] == 50.0
    assert mark["direction_hit"] is True


def test_origin_verdict_ci_approximate_exclusion_and_actual_split() -> None:
    rows = []
    for index in range(30):
        rows.append({
            "status": "closed", "origin": "vrp", "structure": "vertical",
            "params_version": "x", "entered_at": f"2026-09-{index % 15 + 1:02d}T12:00:00+00:00",
            "entry": {"idea": {"max_loss": 100.0, "taken": index < 5}},
            "exit": {
                "pnl": 10.0, "return_on_risk": 0.1, "approximate": False,
                "direction_hit": True,
                "attribution": {"delta": 8.0, "residual": 2.0},
                "benchmarks": {"underlying": 5.0, "vertical": 6.0, "cash": 0.0},
            },
        })
    rows.append({
        "status": "closed", "origin": "vrp", "structure": "vertical",
        "params_version": "x", "entered_at": "2026-09-30T12:00:00+00:00",
        "entry": {"idea": {"max_loss": 100.0}},
        "exit": {"pnl": -999.0, "return_on_risk": -9.99, "approximate": True},
    })
    stats = tracking_stats(rows)
    quality = stats["origin_quality"]["vrp"]
    assert quality["count"] == 30 and quality["verdict"] == "有效"
    assert stats["approximate_exits"] == 1
    assert stats["actual_vs_paper"]["actual"]["count"] == 5
    assert stats["actual_vs_paper"]["paper"]["count"] == 25


def test_origin_drawdown_sorts_settled_rows_chronologically() -> None:
    chronological = [
        ("2026-10-01T12:00:00+00:00", 100.0),
        ("2026-10-02T12:00:00+00:00", -80.0),
        ("2026-10-03T12:00:00+00:00", -80.0),
        ("2026-10-04T12:00:00+00:00", 40.0),
    ]
    rows = [
        {
            "status": "closed",
            "origin": "news",
            "entered_at": entered_at,
            "entry": {"idea": {"max_loss": 200.0}},
            "exit": {"pnl": pnl, "return_on_risk": pnl / 200.0},
        }
        for entered_at, pnl in (
            chronological[0],
            chronological[1],
            chronological[3],
            chronological[2],
        )
    ]

    quality = tracking_stats(rows)["origin_quality"]["news"]

    assert quality["max_drawdown"] == 160.0


def test_ibkr_position_matching_requires_every_leg_and_sign() -> None:
    expiry = TODAY + timedelta(days=30)
    idea = {
        "symbol": "XYZ",
        "legs": [
            {"expiry": expiry.isoformat(), "strike": 100.0, "right": "C", "signed_quantity": 1},
            {"expiry": expiry.isoformat(), "strike": 110.0, "right": "C", "signed_quantity": -1},
        ],
    }
    positions = [
        Position("XYZ", "OPT", 1, 500.0, 100, expiry, 100.0, "C"),
        Position("XYZ", "OPT", -1, 200.0, 100, expiry, 110.0, "C"),
    ]
    assert match_open_position(idea, positions) == {"taken": True, "average_cost": 700.0}
    assert match_open_position(idea, positions[:1])["taken"] is False


def test_combined_exposure_uses_suggested_quantities() -> None:
    first = _idea("SPY", 100.0)
    first["suggested_quantity"] = 2
    result = combined_exposure([first])
    assert result["max_loss"] == 400.0
    assert result["spy_equivalent_delta"] == 200.0
    assert result["entry_costs"] == 10.0
