from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from options_copilot.domain import (
    CandidateRiskTier,
    OptionContract,
    OptionLeg,
    OptionLegQuote,
    OptionRight,
    PositionSide,
    StrategyCandidate,
    TerminalScenario,
)
from options_copilot.ranking import CandidateRanker, RankingAction
from options_copilot.risk import (
    PayoffStatus,
    RiskEngine,
    RiskRejection,
    analyze_expiration_payoff,
)


D = Decimal
NOW = datetime(2026, 8, 3, 14, 30, tzinfo=timezone.utc)
EXPIRY = date(2026, 9, 18)


def _leg_quote(
    strike: str,
    right: OptionRight,
    side: PositionSide,
    quantity: int,
    *,
    bid: str | None,
    ask: str | None,
    suffix: str = "",
    expiration: date = EXPIRY,
    underlying: str = "SPY",
) -> OptionLegQuote:
    contract = OptionContract(
        contract_id=f"{underlying}-{expiration}-{right.value}-{strike}{suffix}",
        underlying=underlying,
        expiration=expiration,
        strike=D(strike),
        right=right,
    )
    return OptionLegQuote(
        leg=OptionLeg(contract=contract, side=side, quantity=quantity),
        bid=None if bid is None else D(bid),
        ask=None if ask is None else D(ask),
        last=D("3.50"),
        implied_volatility=D("0.22"),
        volume=1200,
        open_interest=5400,
        observed_at=NOW,
    )


def _call_spread(
    candidate_id: str,
    *,
    long_bid: str = "4.90",
    long_ask: str = "5",
    short_bid: str = "2",
    commissions: str = "0",
    slippage: str = "0",
    tier: CandidateRiskTier = CandidateRiskTier.NORMAL,
    scenarios: tuple[TerminalScenario, ...] = (),
) -> StrategyCandidate:
    return StrategyCandidate(
        candidate_id=candidate_id,
        leg_quotes=(
            _leg_quote(
                "100",
                OptionRight.CALL,
                PositionSide.LONG,
                1,
                bid=long_bid,
                ask=long_ask,
                suffix=f"-{candidate_id}",
            ),
            _leg_quote(
                "110",
                OptionRight.CALL,
                PositionSide.SHORT,
                1,
                bid=short_bid,
                ask="2.10",
                suffix=f"-{candidate_id}",
            ),
        ),
        terminal_scenarios=scenarios,
        estimated_commissions=D(commissions),
        estimated_slippage=D(slippage),
        risk_tier=tier,
    )


def test_domain_models_are_deeply_immutable_and_decimal_only() -> None:
    candidate = _call_spread("immutable")

    with pytest.raises(FrozenInstanceError):
        candidate.candidate_id = "changed"  # type: ignore[misc]
    assert isinstance(candidate.leg_quotes, tuple)
    assert isinstance(candidate.terminal_scenarios, tuple)

    with pytest.raises(TypeError, match="strike must be a Decimal"):
        OptionContract(
            contract_id="float-strike",
            underlying="SPY",
            expiration=EXPIRY,
            strike=100.0,  # type: ignore[arg-type]
            right=OptionRight.CALL,
        )


def test_debit_call_spread_has_exact_piecewise_expiration_geometry() -> None:
    candidate = _call_spread("vertical", commissions="2")

    payoff = analyze_expiration_payoff(candidate)

    assert payoff.status is PayoffStatus.CALCULATED
    assert payoff.net_opening_cashflow == D("-302")
    assert payoff.max_loss == D("302")
    assert payoff.max_profit == D("698")
    assert payoff.unbounded_profit is False
    assert payoff.breakevens == (D("103.02"),)
    assert payoff.pnl_at(D("95")) == D("-302")
    assert payoff.pnl_at(D("105")) == D("198")
    assert payoff.pnl_at(D("120")) == D("698")


def test_long_call_keeps_finite_known_loss_and_reports_unbounded_profit() -> None:
    candidate = StrategyCandidate(
        candidate_id="long-call",
        leg_quotes=(
            _leg_quote(
                "100",
                OptionRight.CALL,
                PositionSide.LONG,
                1,
                bid="4.90",
                ask="5",
            ),
        ),
        estimated_commissions=D("1"),
    )

    payoff = analyze_expiration_payoff(candidate)

    assert payoff.status is PayoffStatus.CALCULATED
    assert payoff.max_loss == D("501")
    assert payoff.max_profit is None
    assert payoff.unbounded_profit is True
    assert payoff.breakevens == (D("105.01"),)


def test_ratio_butterfly_is_calculated_from_all_legs_not_strategy_templates() -> None:
    candidate = StrategyCandidate(
        candidate_id="butterfly",
        leg_quotes=(
            _leg_quote("90", OptionRight.CALL, PositionSide.LONG, 1, bid="11.9", ask="12"),
            _leg_quote("100", OptionRight.CALL, PositionSide.SHORT, 2, bid="5", ask="5.1"),
            _leg_quote("110", OptionRight.CALL, PositionSide.LONG, 1, bid="0.9", ask="1"),
        ),
    )

    payoff = analyze_expiration_payoff(candidate)

    assert payoff.status is PayoffStatus.CALCULATED
    assert payoff.max_loss == D("300")
    assert payoff.max_profit == D("700")
    assert payoff.breakevens == (D("93"), D("107"))
    assert tuple(segment.lower_bound for segment in payoff.segments) == (
        D("0"),
        D("90"),
        D("100"),
        D("110"),
    )


def test_defined_risk_short_put_spread_is_accepted_and_calculated_exactly() -> None:
    candidate = StrategyCandidate(
        candidate_id="put-credit-spread",
        leg_quotes=(
            _leg_quote("100", OptionRight.PUT, PositionSide.SHORT, 1, bid="3", ask="3.1"),
            _leg_quote("90", OptionRight.PUT, PositionSide.LONG, 1, bid="0.9", ask="1"),
        ),
    )

    payoff = analyze_expiration_payoff(candidate)

    assert payoff.status is PayoffStatus.CALCULATED
    assert payoff.net_opening_cashflow == D("200")
    assert payoff.max_loss == D("800")
    assert payoff.max_profit == D("200")
    assert payoff.breakevens == (D("98"),)
    assert payoff.pnl_at(D("0")) == D("-800")
    assert payoff.pnl_at(D("105")) == D("200")


@pytest.mark.parametrize(
    ("right", "expected_rejection"),
    [
        (OptionRight.CALL, RiskRejection.NAKED_SHORT_CALL),
        (OptionRight.PUT, RiskRejection.NAKED_SHORT_PUT),
    ],
)
def test_every_naked_short_is_permanently_rejected_even_if_loss_is_mathematically_finite(
    right: OptionRight,
    expected_rejection: RiskRejection,
) -> None:
    candidate = StrategyCandidate(
        candidate_id=f"naked-{right.value.lower()}",
        leg_quotes=(
            _leg_quote("100", right, PositionSide.SHORT, 1, bid="2", ask="2.1"),
        ),
    )

    payoff = analyze_expiration_payoff(candidate)

    assert payoff.status is PayoffStatus.REJECTED
    assert expected_rejection in payoff.rejections
    assert payoff.max_loss is None


def test_unavailable_quote_and_mixed_expirations_are_uncalculable_and_rejected() -> None:
    missing_quote = StrategyCandidate(
        candidate_id="missing-ask",
        leg_quotes=(
            _leg_quote("100", OptionRight.CALL, PositionSide.LONG, 1, bid="4", ask=None),
        ),
    )
    mixed_expirations = StrategyCandidate(
        candidate_id="calendar",
        leg_quotes=(
            _leg_quote("100", OptionRight.CALL, PositionSide.LONG, 1, bid="4", ask="5"),
            _leg_quote(
                "110",
                OptionRight.CALL,
                PositionSide.LONG,
                1,
                bid="2",
                ask="3",
                expiration=date(2026, 10, 16),
            ),
        ),
    )

    missing = analyze_expiration_payoff(missing_quote)
    calendar = analyze_expiration_payoff(mixed_expirations)

    assert missing.status is PayoffStatus.REJECTED
    assert RiskRejection.MISSING_EXECUTABLE_QUOTE in missing.rejections
    assert calendar.status is PayoffStatus.REJECTED
    assert RiskRejection.MIXED_EXPIRATION in calendar.rejections


def test_zero_payoff_region_is_represented_without_losing_infinite_breakevens() -> None:
    candidate = StrategyCandidate(
        candidate_id="zero-everywhere",
        leg_quotes=(
            _leg_quote("100", OptionRight.CALL, PositionSide.LONG, 1, bid="1", ask="1", suffix="-same"),
            _leg_quote("100", OptionRight.CALL, PositionSide.SHORT, 1, bid="1", ask="1", suffix="-same"),
        ),
    )

    payoff = analyze_expiration_payoff(candidate)

    assert payoff.max_loss == D("0")
    assert payoff.max_profit == D("0")
    assert len(payoff.break_even_regions) == 1
    assert payoff.break_even_regions[0].lower_bound == D("0")
    assert payoff.break_even_regions[0].upper_bound is None


def test_risk_engine_enforces_normal_validated_hard_and_single_combo_limits() -> None:
    engine = RiskEngine()
    normal = _call_spread("normal")  # exact max loss: $300
    validated = _call_spread(
        "validated",
        tier=CandidateRiskTier.VALIDATED_A_GRADE,
    )

    normal_assessment = engine.assess(normal, account_equity=D("2500"))
    validated_assessment = engine.assess(validated, account_equity=D("2500"))
    hard_assessment = engine.assess(validated, account_equity=D("1500"))
    occupied_assessment = engine.assess(
        validated,
        account_equity=D("10000"),
        open_combinations=1,
    )

    assert normal_assessment.approved is False
    assert normal_assessment.risk_fraction == D("0.12")
    assert RiskRejection.NORMAL_RISK_LIMIT_EXCEEDED in normal_assessment.rejections
    assert validated_assessment.approved is True
    assert validated_assessment.allowed_risk_fraction == D("0.15")
    assert hard_assessment.approved is False
    assert hard_assessment.risk_fraction == D("0.2")
    assert RiskRejection.HARD_RISK_LIMIT_REACHED in hard_assessment.rejections
    assert occupied_assessment.approved is False
    assert RiskRejection.MAX_OPEN_COMBINATIONS in occupied_assessment.rejections


def test_risk_cap_boundaries_allow_exactly_ten_and_fifteen_percent() -> None:
    engine = RiskEngine()
    normal = _call_spread("normal-boundary")
    validated = _call_spread(
        "validated-boundary",
        tier=CandidateRiskTier.VALIDATED_A_GRADE,
    )

    assert engine.assess(normal, account_equity=D("3000")).approved is True
    assert engine.assess(validated, account_equity=D("2000")).approved is True
    above_validated = engine.assess(validated, account_equity=D("1999"))
    assert above_validated.approved is False
    assert RiskRejection.VALIDATED_RISK_LIMIT_EXCEEDED in above_validated.rejections


def test_ranker_uses_probability_weighted_ev_after_all_estimated_costs() -> None:
    scenarios = (
        TerminalScenario(terminal_underlying_price=D("90"), probability=D("0.4")),
        TerminalScenario(terminal_underlying_price=D("110"), probability=D("0.6")),
    )
    higher_gross_but_costly = _call_spread(
        "costly",
        long_bid="3.90",
        long_ask="4",
        commissions="100",
        slippage="50",
        scenarios=scenarios,
    )
    lower_gross_but_better_net = _call_spread(
        "better-net",
        long_ask="5",
        scenarios=scenarios,
    )

    decision = CandidateRanker().rank(
        (higher_gross_but_costly, lower_gross_but_better_net),
        account_equity=D("10000"),
    )

    assert decision.action is RankingAction.TRADE
    assert decision.selected_candidate is lower_gross_but_better_net
    assert decision.selected_candidates == (lower_gross_but_better_net,)
    by_id = {item.candidate.candidate_id: item for item in decision.evaluations}
    assert by_id["costly"].expected_value_before_costs == D("400.0")
    assert by_id["costly"].expected_value_after_costs == D("250.0")
    assert by_id["better-net"].expected_value_after_costs == D("300.0")


def test_ranker_returns_no_trade_when_ev_or_risk_leaves_no_qualified_candidate() -> None:
    losing_distribution = (
        TerminalScenario(terminal_underlying_price=D("90"), probability=D("0.9")),
        TerminalScenario(terminal_underlying_price=D("110"), probability=D("0.1")),
    )
    negative_ev = _call_spread("negative-ev", scenarios=losing_distribution)
    over_normal_limit = _call_spread(
        "over-risk",
        scenarios=(
            TerminalScenario(terminal_underlying_price=D("110"), probability=D("1")),
        ),
    )

    decision = CandidateRanker().rank(
        (negative_ev, over_normal_limit),
        account_equity=D("2500"),
    )

    assert decision.action is RankingAction.NO_TRADE
    assert decision.selected_candidate is None
    assert decision.selected_candidates == ()
    assert len(decision.evaluations) == 2
