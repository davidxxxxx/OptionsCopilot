from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from copilot.config import RulesSettings
from copilot.market.models import AccountSnapshot, OptionQuote, Position
from copilot.news.dividends import (
    DividendEvent,
    parse_nasdaq_dividends,
    projected_dividend,
    weekly_dividends,
)
from copilot.strategies.builder import Candidate
from copilot.strategies.decision_portfolio import (
    CALENDAR_MODEL_NOTE,
    IDEA_SCENARIO_NAMES,
    calendar_sensitivity,
    candidate_greeks,
    idea_card_summary,
    idea_scenarios,
)
from copilot.strategies.payoff import Leg, PayoffProfile
from copilot.strategies.portfolio import (
    ROLL_REASSESSMENT,
    exit_alerts,
    group_positions,
    idea_assignment_warnings,
    mark_to_market,
    profit_exit,
)
from copilot.strategies.pricing import bs_price

from .conftest import TODAY


def _quote(
    expiry: date,
    strike: float,
    right: str,
    *,
    bid: float,
    ask: float,
    iv: float = 0.25,
    delta: float | None = None,
    gamma: float | None = None,
    vega: float | None = None,
    theta: float | None = None,
) -> OptionQuote:
    return OptionQuote(
        symbol="XYZ",
        expiry=expiry,
        strike=strike,
        right=right,
        bid=bid,
        ask=ask,
        iv=iv,
        delta=delta,
        gamma=gamma,
        vega=vega,
        theta=theta,
    )


def _idea(*, structure: str = "long_call", legs: list[dict] | None = None) -> dict:
    expiry = TODAY + timedelta(days=30)
    return {
        "symbol": "XYZ",
        "structure": structure,
        "structure_name": structure,
        "spot": 100.0,
        "atm_iv": 0.25,
        "multiplier": 100,
        "max_loss": 500.0,
        "fees": 0.0,
        "origin": "news",
        "limit_net_price": 4.5,
        "view": {"direction": "bullish", "horizon_days": 8, "catalyst": "事件"},
        "legs": legs or [{
            "expiry": expiry.isoformat(),
            "strike": 100.0,
            "right": "C",
            "signed_quantity": 1,
            "fill": 5.0,
            "iv": 0.25,
            "bid": 4.9,
            "ask": 5.1,
        }],
    }


def test_candidate_greeks_aggregate_net_contract_exposure() -> None:
    expiry = TODAY + timedelta(days=30)
    long_quote = _quote(
        expiry, 100.0, "C", bid=4.9, ask=5.1,
        delta=0.60, gamma=0.02, vega=0.10, theta=-0.05,
    )
    short_quote = _quote(
        expiry, 110.0, "C", bid=1.9, ask=2.1,
        delta=0.30, gamma=0.01, vega=0.05, theta=-0.02,
    )
    candidate = Candidate(
        symbol="XYZ",
        structure="bull_call_spread",
        expiry=expiry,
        dte=30,
        legs=(Leg(long_quote, 1, 5.0), Leg(short_quote, -1, 2.0)),
        multiplier=100,
        profile=PayoffProfile(3.0, 300.0, 700.0, (103.0,), 0.0),
        view_ev=10.0,
        view_pop=0.5,
        market_ev=0.0,
        market_pop=0.4,
        edge=10.0,
        slippage=0.0,
        spread_cost=0.0,
        atm_iv=0.25,
        view_sigma=0.25,
        spot=100.0,
    )

    greeks = candidate_greeks(candidate)

    assert greeks == pytest.approx({
        "delta_shares": 30.0,
        "gamma": 1.0,
        "vega": 5.0,
        "theta": -3.0,
        "delta_dollars": 3_000.0,
    })


def test_each_idea_scenario_reprices_hand_built_leg_with_black_scholes() -> None:
    idea = _idea()
    rows = idea_scenarios(idea, today=TODAY)
    assert [row["scenario"] for row in rows] == list(IDEA_SCENARIO_NAMES)
    horizon_sigma = 0.25 * math.sqrt(8 / 365)
    cases = (
        (5, 0.5, -0.03),
        (15, 1.0, 0.0),
        (1, 0.0, -0.05),
        (1, 2.0, 0.0),
        (1, -2.0, 0.0),
        (8, 0.0, 0.0),
    )
    for row, (days_forward, sigma_units, iv_shift) in zip(rows, cases):
        scenario_spot = 100.0 * math.exp(sigma_units * horizon_sigma)
        value = bs_price(
            scenario_spot,
            100.0,
            max(30 - days_forward, 0) / 365,
            0.25 + iv_shift,
            "C",
        )
        expected_pnl = (value - 5.0) * 100
        assert row["valuation"] == pytest.approx(value * 100, abs=0.01)
        assert row["pnl"] == pytest.approx(expected_pnl, abs=0.01)
        assert row["max_loss_pct"] == pytest.approx(expected_pnl / 500, abs=0.0001)


def test_calendar_sensitivity_changes_only_far_month_iv() -> None:
    front = TODAY + timedelta(days=20)
    back = TODAY + timedelta(days=60)
    idea = _idea(
        structure="call_calendar",
        legs=[
            {"expiry": front.isoformat(), "strike": 100.0, "right": "C", "signed_quantity": -1, "fill": 3.0, "iv": 0.25},
            {"expiry": back.isoformat(), "strike": 100.0, "right": "C", "signed_quantity": 1, "fill": 6.0, "iv": 0.25},
        ],
    )

    result = calendar_sensitivity(idea, today=TODAY)

    assert result is not None and result["note"] == CALENDAR_MODEL_NOTE
    expected = []
    for shift in (-0.03, 0.03):
        front_value = bs_price(100.0, 100.0, 20 / 365, 0.25, "C")
        back_value = bs_price(100.0, 100.0, 60 / 365, 0.25 + shift, "C")
        expected.append((back_value - front_value) * 100)
    assert [row["valuation"] for row in result["rows"]] == pytest.approx(expected, abs=0.01)


def test_nasdaq_dividend_parse_and_history_projection() -> None:
    payload = {"data": {"dividends": {"rows": [
        {"exOrEffDate": "03/20/2026", "amount": "$0.27"},
        {"exOrEffDate": "bad", "amount": "$0.20"},
    ]}}}
    parsed = parse_nasdaq_dividends(payload)
    assert parsed == [DividendEvent(date(2026, 3, 20), 0.27)]

    history = [
        DividendEvent(date(2025, 3, 21), 0.25),
        DividendEvent(date(2025, 6, 20), 0.26),
        DividendEvent(date(2025, 9, 19), 0.27),
    ]
    projected = projected_dividend("XYZ", history, today=date(2025, 10, 1))
    assert projected == DividendEvent(
        date(2025, 12, 19), 0.27, estimated=True, source="history_projection"
    )


def test_dividend_settings_default_spy_quarterly_yield(settings) -> None:
    assert settings.dividends.spy_quarterly_yield == pytest.approx(0.003)


def test_spy_projection_uses_next_quarter_third_friday_without_history() -> None:
    projected = projected_dividend(
        "SPY",
        (),
        today=date(2026, 1, 5),
        spot=600.0,
        spy_quarterly_yield=0.003,
    )
    assert projected is not None
    assert projected.ex_date == date(2026, 3, 20)
    assert projected.amount == pytest.approx(1.8)
    assert projected.estimated is True
    assert projected.source == "spy_quarterly_rule"


def test_spy_weekly_projection_uses_spot_and_configured_yield(store) -> None:
    calls: list[str] = []

    def fetcher(symbol: str) -> list[DividendEvent]:
        calls.append(symbol)
        return []

    first = weekly_dividends(
        store,
        "SPY",
        today=date(2026, 10, 14),
        fetcher=fetcher,
        spot=600.0,
        spy_quarterly_yield=0.003,
    )
    second = weekly_dividends(
        store,
        "SPY",
        today=date(2026, 10, 15),
        fetcher=fetcher,
        spot=610.0,
        spy_quarterly_yield=0.003,
    )
    assert first[0].amount == pytest.approx(1.8)
    assert second[0].amount == pytest.approx(1.83)
    assert first[0].source == second[0].source == "spy_quarterly_rule"
    assert calls == ["SPY"]


def test_dividend_weekly_cache_fetches_once_per_symbol_week(store) -> None:
    calls: list[str] = []

    def fetcher(symbol: str) -> list[DividendEvent]:
        calls.append(symbol)
        return [DividendEvent(date(2026, 10, 30), 0.50)]

    first = weekly_dividends(store, "XYZ", today=date(2026, 10, 14), fetcher=fetcher)
    second = weekly_dividends(store, "XYZ", today=date(2026, 10, 15), fetcher=fetcher)
    third = weekly_dividends(store, "XYZ", today=date(2026, 10, 19), fetcher=fetcher)
    assert first == second == third
    assert calls == ["XYZ", "XYZ"]


def test_short_call_dividend_assignment_warning_and_non_trigger() -> None:
    idea = _idea(legs=[{
        "expiry": (TODAY + timedelta(days=20)).isoformat(),
        "strike": 100.0,
        "right": "C",
        "signed_quantity": -1,
        "fill": 0.55,
        "bid": 0.50,
        "ask": 0.60,
        "iv": 0.25,
    }])
    idea["spot"] = 100.5
    dividend = DividendEvent(TODAY + timedelta(days=5), 0.60)
    warnings = idea_assignment_warnings(idea, [dividend], today=TODAY)
    assert {warning["code"] for warning in warnings} == {"early_assignment_call"}

    idea["legs"][0]["signed_quantity"] = 1
    assert idea_assignment_warnings(idea, [dividend], today=TODAY) == []


def test_short_put_early_assignment_and_physical_pin_risk() -> None:
    idea = _idea(legs=[{
        "expiry": (TODAY + timedelta(days=2)).isoformat(),
        "strike": 100.0,
        "right": "P",
        "signed_quantity": -1,
        "fill": 10.05,
        "bid": 10.0,
        "ask": 10.1,
        "iv": 0.25,
    }])
    idea["spot"] = 90.0
    warnings = idea_assignment_warnings(idea, (), today=TODAY)
    assert {warning["code"] for warning in warnings} == {
        "dividend_unknown", "early_assignment_put",
    }

    idea["spot"] = 100.0
    warnings = idea_assignment_warnings(idea, (), today=TODAY)
    assert {warning["code"] for warning in warnings} == {"dividend_unknown", "pin_risk"}
    assert next(warning for warning in warnings if warning["code"] == "pin_risk")["text"] == "到期交割/钉住风险，建议到期前处理"


def test_unknown_dividend_warns_short_idea_and_position_but_not_long_only() -> None:
    short_idea = _idea(legs=[{
        "expiry": (TODAY + timedelta(days=20)).isoformat(),
        "strike": 105.0,
        "right": "C",
        "signed_quantity": -1,
        "fill": 1.0,
        "iv": 0.25,
    }])
    short_idea["symbol"] = "XLE"
    warnings = idea_assignment_warnings(short_idea, (), today=TODAY)
    assert warnings[0] == {
        "level": "warn",
        "code": "dividend_unknown",
        "text": "除息日未知，请自行核对是否可能提前指派",
    }

    short_idea["legs"][0]["signed_quantity"] = 1
    assert idea_assignment_warnings(short_idea, (), today=TODAY) == []

    expiry = TODAY + timedelta(days=20)
    position = Position("XLE", "OPT", -1, 100.0, 100, expiry, 105.0, "C")
    group = group_positions([position])[0]
    alerts = exit_alerts(group, today=TODAY, rules=RulesSettings(), dividends=())
    assert "dividend_unknown" in {alert["code"] for alert in alerts}


def test_cash_settled_index_note_and_open_position_dividend_warning() -> None:
    spx = _idea(legs=[{
        "expiry": (TODAY + timedelta(days=2)).isoformat(),
        "strike": 100.0,
        "right": "C",
        "signed_quantity": -1,
        "fill": 1.0,
        "bid": 0.9,
        "ask": 1.1,
        "iv": 0.25,
    }])
    spx["symbol"] = "SPX"
    assert {warning["code"] for warning in idea_assignment_warnings(spx, (), today=TODAY)} == {"cash_settlement"}

    expiry = TODAY + timedelta(days=20)
    position = Position("XYZ", "OPT", -1, 55.0, 100, expiry, 100.0, "C")
    group = group_positions([position])[0]
    quote = _quote(expiry, 100.0, "C", bid=0.50, ask=0.60)
    mark_to_market(group, {(expiry, 100.0, "C"): quote}, spot=100.5)
    alerts = exit_alerts(
        group,
        today=TODAY,
        rules=RulesSettings(),
        dividends=[DividendEvent(TODAY + timedelta(days=5), 0.60)],
    )
    assert "early_assignment_call" in {alert["code"] for alert in alerts}


def test_close_and_stop_alerts_include_fresh_trade_roll_language() -> None:
    rules = RulesSettings()
    take_profit = profit_exit("debit", 100.0, 80.0, 100.0, rules)
    stop_loss = profit_exit("debit", 100.0, -60.0, 100.0, rules)
    assert take_profit is not None and ROLL_REASSESSMENT in take_profit[1]
    assert stop_loss is not None and ROLL_REASSESSMENT in stop_loss[1]


def test_card_summary_has_seven_lines_and_missing_fields_use_dash() -> None:
    idea = _idea()
    idea.update({
        "greeks": {"delta_dollars": 2_000.0, "delta_shares": 20.0, "gamma": 1.0, "vega": 4.0, "theta": -2.0},
        "priced_in": {"status": "ok", "priced_in": 0.4},
        "calibration_verified": True,
    })
    account = AccountSnapshot(datetime.now(timezone.utc), net_liquidation=10_000.0)
    rows = idea_card_summary(
        idea,
        origin_quality={"news": {"verdict": "有效"}},
        account=account,
        rules=RulesSettings(),
    )
    assert [row["label"] for row in rows] == [
        "为什么现在做", "赚什么钱", "价格以内值得做", "什么情况说明判断失效",
        "最多亏多少", "何时退出", "证据有多充分",
    ]
    assert all(row["text"] != "—" for row in rows)
    assert "已反映 40%" in rows[0]["text"]
    assert "净值 5.0%" in rows[4]["text"]

    missing = idea_card_summary(
        {}, origin_quality={}, account=None, rules=RulesSettings()
    )
    assert len(missing) == 7
    assert all(row["text"] == "—" for row in missing)


def test_phase3_gui_contains_scenarios_summary_and_calendar_disclaimer() -> None:
    text = Path("copilot/web/static/app.js").read_text(encoding="utf-8")
    assert "idea.card_summary" in text
    assert "idea.dividends" in text
    assert "（推算）" in text
    assert "关键情景重估" in text
    assert "远月 IV ±3 点时的估值" in text
