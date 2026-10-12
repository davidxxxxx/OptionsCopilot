from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from copilot.analysis.ladder import ladder_signal, ladder_view
from copilot.analysis.models import View
from copilot.config import RulesSettings
from copilot.market.models import OptionQuote, Position
from copilot.market.nasdaq_history import parse_closes
from copilot.pipeline import PipelineDeps, run_pipeline
from copilot.news.models import NewsItem
from copilot.strategies.portfolio import exit_alerts, group_positions, mark_to_market, portfolio_risk

from .conftest import TODAY, FakeCboe, FakeLlm, FakeMarket

RULES = RulesSettings()
EXP = date(2026, 10, 16)


def opt(strike: float, right: str, quantity: float, avg_cost: float, expiry: date = EXP, symbol: str = "SPY") -> Position:
    return Position(symbol=symbol, sec_type="OPT", quantity=quantity, avg_cost=avg_cost, multiplier=100,
                    expiry=expiry, strike=strike, right=right)


# The user's real position on 2026-10-09: a long iron condor on SPY.
LONG_CONDOR = (opt(765, "P", -1, 141.9576), opt(795, "C", -1, 99.9584), opt(775, "P", 1, 341.0395), opt(785, "C", 1, 375.0395))


def quotes_for(mids: dict[tuple[float, str], tuple[float, float]]) -> dict:
    return {
        (EXP, strike, right): OptionQuote(symbol="SPY", expiry=EXP, strike=strike, right=right,
                                         bid=mid - 0.05, ask=mid + 0.05, delta=delta)
        for (strike, right), (mid, delta) in mids.items()
    }


def test_long_iron_condor_risk_matches_the_debit() -> None:
    (group,) = group_positions(LONG_CONDOR)
    assert group.kind == "debit"
    assert group.entry_cost == pytest.approx(474.16, abs=0.01)
    assert group.max_loss == pytest.approx(474.16, abs=0.01)
    assert group.max_profit == pytest.approx(525.84, abs=0.01)


def test_exit_alerts_for_the_long_condor_near_expiry_and_oversized() -> None:
    (group,) = group_positions(LONG_CONDOR)
    mark_to_market(group, quotes_for({(765, "P"): (0.5, -0.1), (775, "P"): (1.5, -0.3),
                                      (785, "C"): (1.6, 0.3), (795, "C"): (0.4, 0.1)}))
    assert group.pnl == pytest.approx((1.5 + 1.6 - 0.5 - 0.4) * 100 - 474.16, abs=0.01)  # -54% of the debit
    assert group.direction == 0  # balanced wings
    alerts = exit_alerts(group, today=EXP - timedelta(days=7), rules=RULES, net_liquidation=2117.31)
    codes = {a["code"] for a in alerts}
    assert {"expiry", "oversized", "stop_loss"} <= codes  # lost >50% of the debit, 7 days left, 22% of NLV


def test_credit_spread_take_profit_earnings_and_view_conflict() -> None:
    # IBKR reports a short leg's avgCost as positive premium received; the sign comes from quantity.
    spread = (opt(500, "P", -1, 250.0, symbol="QQQ"), opt(490, "P", 1, 100.0, symbol="QQQ"))
    (group,) = group_positions(spread)
    assert group.kind == "credit" and group.max_profit == pytest.approx(150.0)
    quotes = {(EXP, 500.0, "P"): OptionQuote(symbol="QQQ", expiry=EXP, strike=500, right="P", bid=0.6, ask=0.7, delta=-0.2),
              (EXP, 490.0, "P"): OptionQuote(symbol="QQQ", expiry=EXP, strike=490, right="P", bid=0.1, ask=0.2, delta=-0.05)}
    mark_to_market(group, quotes)
    bearish = View(symbol="QQQ", direction="bearish", conviction=4, expected_move_pct=0.0, horizon_days=10)
    alerts = exit_alerts(group, today=EXP - timedelta(days=12), rules=RULES,
                         earnings=[EXP - timedelta(days=3)], view=bearish)
    codes = {a["code"] for a in alerts}
    assert {"take_profit", "earnings", "view_conflict", "expiry"} <= codes


def test_naked_short_call_is_undefined_risk() -> None:
    (group,) = group_positions((opt(800, "C", -1, 300.0),))
    assert group.max_loss is None
    assert "undefined_risk" in {a["code"] for a in exit_alerts(group, today=TODAY, rules=RULES)}


def test_calendar_risk_is_the_debit_when_covered() -> None:
    calendar = (opt(600, "C", -1, 400.0), opt(600, "C", 1, 900.0, expiry=EXP + timedelta(days=28)))
    (group,) = group_positions(calendar)
    assert group.max_loss == pytest.approx(500.0)
    uncovered = (opt(600, "C", -2, 400.0), opt(600, "C", 1, 900.0, expiry=EXP + timedelta(days=28)))
    assert group_positions(uncovered)[0].max_loss is None


def test_portfolio_risk_remaining_budget_and_direction() -> None:
    groups = group_positions(LONG_CONDOR)
    groups[0].net_delta = 45.0  # pretend the condor leans long
    risk = portfolio_risk(groups, 2117.31, RULES)
    assert risk.cap == pytest.approx(423.46, abs=0.01)
    assert risk.remaining == pytest.approx(423.46 - 474.16, abs=0.01)
    assert risk.same_direction("QQQ", 1) and not risk.same_direction("QQQ", -1)
    assert not risk.same_direction("NVDA", 1)


def test_ladder_tiers_and_views() -> None:
    days = [TODAY - timedelta(days=300 - i) for i in range(300)]
    closes = [(day, 100.0) for day in days[:-1]] + [(days[-1], 88.0)]
    signal = ladder_signal("SPY", closes, RULES)
    assert signal.tier == 2 and signal.drawdown == pytest.approx(0.12)
    view = ladder_view(signal, RULES)
    assert view.source == "ladder" and view.structures == ("bull_call_spread",)
    assert (view.dte_min, view.dte_max) == (30, 60) and view.drift_sigma == pytest.approx(0.20)
    calm = ladder_signal("SPY", [(day, 100.0) for day in days], RULES)
    assert calm.tier == 0 and ladder_view(calm, RULES) is None
    assert ladder_signal("SPY", closes[:50], RULES) is None  # not enough history
    deep = ladder_view(ladder_signal("SPY", closes[:-1] + [(days[-1], 80.0)], RULES), RULES)
    assert (deep.dte_min, deep.dte_max) == (90, 180) and "long_call" in deep.structures


def test_parse_nasdaq_closes() -> None:
    payload = {"data": {"tradesTable": {"rows": [
        {"date": "10/08/2026", "close": "$773.93"}, {"date": "10/07/2026", "close": "$777.22"},
        {"date": "bad", "close": "$1"}, {"date": "10/06/2026", "close": "N/A"},
    ]}}}
    assert parse_closes(payload) == [(date(2026, 10, 7), 777.22), (date(2026, 10, 8), 773.93)]


NOW = datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)
SPOTS = {"SPY": 600.0, "QQQ": 500.0, "NVDA": 180.0, "JPM": 300.0}


def test_pipeline_adds_ladder_ideas_positions_and_portfolio_flags(settings, store) -> None:
    store.add_news([NewsItem(source="jin10", source_id="a", published_at=NOW, title="英伟达发布新芯片")])
    rules = {"英伟达": {"category": "company", "importance": 3, "summary": "英伟达发布新芯片",
                        "impacts": [{"symbol": "NVDA", "direction": 1, "magnitude": 3, "horizon": "weeks"}]}}
    held = (opt(590, "P", -1, 600.0, expiry=TODAY + timedelta(days=14)),
            opt(580, "P", 1, 300.0, expiry=TODAY + timedelta(days=14)))
    market = FakeMarket(SPOTS, positions=held, net_liquidation=5_000.0)

    def history(symbol, today):
        days = [today - timedelta(days=300 - i) for i in range(300)]
        return [(day, 100.0) for day in days[:-1]] + [(days[-1], 93.0)]  # 7% off the high: tier 1

    deps = PipelineDeps(
        settings=settings, store=store, news_sources=None, llm=FakeLlm(rules),
        market_factory=lambda: market, clock=lambda: NOW, option_source=FakeCboe(SPOTS),
        index_history=history,
    )
    run = store.get_run(run_pipeline(deps, trigger="test", slot="manual"))
    ladder = {s["symbol"]: s for s in run["stages"]["rules"]["ladder"]}
    assert ladder["SPY"]["tier"] == 1
    book = run["snapshots"]["positions"]["book"]
    assert book["risk"]["open_risk"] == pytest.approx(700.0)  # 10-wide put spread for a 3.00 credit
    assert book["groups"][0]["kind"] == "credit"
    for idea in run["ideas"]:
        assert idea["portfolio_ok"] == (idea["max_loss"] <= 1_000 - 700.0 + 1e-6)
    ladder_ideas = [i for i in run["ideas"] if i["origin"] == "ladder"]
    for idea in ladder_ideas:
        assert idea["structure"] == "bull_put_spread" and idea["view"]["source"] == "ladder"
    # Cached closes: a second run does not fetch again.
    assert store.get_daily_cache("SPY", "NASDAQ_CLOSE", date(2026, 10, 9))
