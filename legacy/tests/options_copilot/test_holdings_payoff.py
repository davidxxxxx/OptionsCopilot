"""Holding opportunity-cost payoff uses no entry, reopening or quote fiction."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timezone
from decimal import Decimal, Inexact, localcontext
from fractions import Fraction
import inspect

import pytest

from options_copilot.positions.holdings_payoff import (
    HoldingPayoffLeg,
    HoldingsPayoffError,
    analyze_holdings_payoff,
)
from options_copilot.storage.canonical import canonical_hash


D = Decimal
EXPIRATION = date(2026, 9, 18)


def _leg(contract_id=1, *, strike="650", right="P", quantity=-1, **kwargs):
    return HoldingPayoffLeg(contract_id, "QQQ", EXPIRATION, D(strike), right, quantity, **kwargs)


def _legs():
    return (_leg(), _leg(2, strike="680", quantity=1), _leg(3, strike="760", right="C", quantity=1))


def _analyze(legs=None, *, baseline=D("480"), reserve=D("48.75")):
    return analyze_holdings_payoff(_legs() if legs is None else legs,
                                   gross_liquidation_cashflow_usd=baseline, estimated_future_exit_cost_usd=reserve)


def test_exact_observed_qqq_geometry_and_single_exit_cost_reference():
    # Synthetic natural close quote fixture, never a live quote or fill claim.
    baseline = (D("4") + D("2") - D("1.20")) * 100
    normal_slippage = sum((max(D("0.02"), spread / 2) * 100 for spread in (D("0.20"), D("0.30"), D("0.10"))), D("0"))
    commission, stress_slippage = D("1.25") * 3, normal_slippage * D("1.50")
    reserve = commission + stress_slippage
    result = _analyze(baseline=baseline, reserve=reserve)
    assert (baseline, normal_slippage, commission, stress_slippage, reserve) == tuple(map(D, ("480", "30", "3.75", "45", "48.75")))
    assert baseline - reserve == D("431.25")
    assert result.min_intrinsic_usd == 0 and result.max_intrinsic_usd is None
    assert result.gross_holding_max_loss_usd == D("480")
    assert result.max_loss_usd == result.cost_adjusted_holding_max_loss_usd == D("528.75")
    assert result.max_profit_usd is None and result.unbounded_profit is True
    assert result.breakevens == (D("674.7125"), D("765.2875"))
    assert result.breakevens_exact is True
    assert result.valuation_basis == "GROSS_COMPONENT_NATURAL_LIQUIDATION"
    assert result.model == "SAME_EXPIRY_INTRINSIC_STANDARD_100"
    assert [(row.lower_bound, row.upper_bound, row.slope, row.intrinsic_intercept) for row in result.segments] == [
        (D("0"), D("650"), D("0"), D("3000")),
        (D("650"), D("680"), D("-100"), D("68000")),
        (D("680"), D("760"), D("0"), D("0")),
        (D("760"), None, D("100"), D("-76000")),
    ]
    for price, intrinsic in (("0", "3000"), ("650", "3000"), ("665", "1500"), ("680", "0"), ("760", "0"), ("770", "1000")):
        assert result.intrinsic_at(D(price)) == D(intrinsic)
        assert result.pnl_at(D(price)) == D(intrinsic) - baseline - reserve
        assert result.pnl_at(D(price), include_future_exit_cost=False) == D(intrinsic) - baseline
    document = result.as_dict()
    assert document["future_exit_cost_included_in_max_loss"] is True
    assert document["historical_entry_cost_included"] is document["reopening_premium_included"] is False
    assert document["atomic_fill_verified"] is document["broker_margin_verified"] is False
    assert document["assignment_settlement_risk_verified"] is document["strategy_grouping_verified"] is False
    assert document["instruction_creation_allowed"] is document["order_allowed"] is False
    assert result.verify_hash() is True
    assert canonical_hash(result.hash_payload()) == document["payoff_hash"]


def test_reopening_premium_or_historical_cost_cannot_enter_the_signature():
    assert tuple(inspect.signature(analyze_holdings_payoff).parameters) == (
        "legs", "gross_liquidation_cashflow_usd", "estimated_future_exit_cost_usd",
    )
    result = _analyze()
    reopening = (D("4.30") + D("2.10") - D("1")) * 100
    assert reopening == D("540")
    assert reopening + result.estimated_future_exit_cost_usd - result.max_loss_usd == D("60")


def test_signed_negative_liquidation_cashflow_is_not_mistaken_for_a_positive_asset():
    result = _analyze((_leg(quantity=1), _leg(2, strike="680", quantity=-1)), baseline=D("-500"), reserve=D("10"))
    assert result.min_intrinsic_usd == D("-3000")
    assert result.max_intrinsic_usd == 0
    assert result.gross_holding_max_loss_usd == D("2500")
    assert result.max_loss_usd == D("2510")
    assert result.max_profit_usd == D("490")
    assert result.unbounded_profit is False
    assert result.pnl_at(D("0")) == D("-2510")
    assert result.pnl_at(D("1000")) == D("490")


def test_maximum_loss_is_clipped_at_zero_but_negative_maximum_profit_is_retained():
    legs = (_leg(quantity=1), _leg(2, strike="680", quantity=-1))
    no_loss = _analyze(legs, baseline=D("-4000"), reserve=D("10"))
    assert no_loss.max_loss_usd == 0 and no_loss.max_profit_usd == D("3990")
    all_loss = _analyze(legs, baseline=D("4000"), reserve=D("10"))
    assert all_loss.max_profit_usd == D("-4010") and all_loss.breakevens == ()


@pytest.mark.parametrize("field,value", [
    ("contract_id", True), ("contract_id", 0), ("contract_id", -1), ("contract_id", 2**63),
    ("symbol", "qqq"), ("symbol", ""), ("symbol", "QQQ\n"),
    ("expiration", "2026-09-18"), ("expiration", datetime(2026, 9, 18)),
    ("expiration", datetime(2026, 9, 18, tzinfo=timezone.utc)),
    ("strike", 650), ("strike", 650.0), ("strike", True), ("strike", D("NaN")),
    ("strike", D("Infinity")), ("strike", D("0")), ("strike", D("-1")),
    ("strike", D("1e129")), ("strike", D("1e-129")), ("strike", D("1" * 129)),
    ("right", "CALL"), ("right", "p"), ("right", True),
    ("signed_quantity", 0), ("signed_quantity", True), ("signed_quantity", D("1")),
    ("signed_quantity", 1.0), ("signed_quantity", 2**31),
    ("multiplier", 10), ("multiplier", 99), ("multiplier", D("100")),
    ("multiplier", True), ("currency", "EUR"),
])
def test_hostile_leg_contracts_are_rejected(field, value):
    with pytest.raises(HoldingsPayoffError):
        replace(_leg(), **{field: value})


@pytest.mark.parametrize("baseline,reserve", [
    (1, D("1")), (True, D("1")), (1.0, D("1")), (D("NaN"), D("1")),
    (D("Infinity"), D("1")), (D("1e129"), D("1")), (D("1"), D("-0.01")),
    (D("1"), 0), (D("1"), True), (D("1"), D("NaN")), (D("1"), D("Infinity")),
])
def test_invalid_liquidation_or_exit_cost_inputs_are_rejected(baseline, reserve):
    with pytest.raises(HoldingsPayoffError):
        _analyze(baseline=baseline, reserve=reserve)


@pytest.mark.parametrize("case", ["empty", "nine", "duplicate", "symbol", "expiration", "naked_call", "naked_put", "uncovered_ratio", "not_leg", "generator"])
def test_coverage_and_full_set_shape_are_bounded(case):
    cases = {
        "empty": (), "nine": tuple(_leg(index + 1, quantity=1) for index in range(9)),
        "duplicate": (_leg(quantity=1), _leg(quantity=1)),
        "symbol": (_leg(quantity=1), replace(_leg(2, quantity=1), symbol="SPY")),
        "expiration": (_leg(quantity=1), replace(_leg(2, quantity=1), expiration=date(2026, 9, 25))),
        "naked_call": (_leg(right="C", quantity=-1),),
        "naked_put": (_leg(quantity=-1),),
        "uncovered_ratio": (_leg(quantity=-2), _leg(2, quantity=1)),
        "not_leg": ({"contract_id": 1},), "generator": (row for row in _legs()),
    }
    with pytest.raises(HoldingsPayoffError):
        _analyze(cases[case])


def test_eight_distinct_legs_and_unlimited_long_profit_are_supported():
    result = _analyze(tuple(_leg(index + 1, right="C", quantity=1) for index in range(8)))
    assert len(result.legs) == 8
    assert result.max_loss_usd == D("528.75") and result.unbounded_profit is True


def test_geometry_hash_is_basis_independent_but_payoff_hash_binds_basis_and_cost():
    original, changed_basis, changed_cost = _analyze(), _analyze(baseline=D("480.0001")), _analyze(reserve=D("48.7501"))
    assert original.geometry_hash == changed_basis.geometry_hash == changed_cost.geometry_hash
    assert len({row.payoff_hash for row in (original, changed_basis, changed_cost)}) == 3
    assert _analyze(tuple(reversed(_legs()))).payoff_hash == original.payoff_hash
    changed_leg = _analyze((_leg(), _leg(2, strike="681", quantity=1), _leg(3, strike="760", right="C", quantity=1)))
    assert changed_leg.geometry_hash != original.geometry_hash
    assert _analyze(baseline=D("480.000"), reserve=D("48.75000")).payoff_hash == original.payoff_hash


def test_high_precision_math_and_hashes_are_independent_of_decimal_context():
    baseline = D("20.00000000000000000000000000000000001")
    reserve = D("0.00000000000000000000000000000000001")
    expected = D("20.00000000000000000000000000000000002")
    ordinary = _analyze(baseline=baseline, reserve=reserve)
    with localcontext() as context:
        context.prec = 6
        context.traps[Inexact] = True
        low_precision = _analyze(baseline=baseline, reserve=reserve)
        assert low_precision.max_loss_usd == expected
        assert low_precision.pnl_at(D("700")) == -Fraction(expected)
        assert low_precision.as_dict()["max_loss_usd"] == str(expected)
        assert low_precision.verify_hash() is True
        assert low_precision.payoff_hash == ordinary.payoff_hash
    assert ordinary.max_loss_usd == expected


def test_fractional_cent_cashflows_are_not_rounded_in_the_favorable_direction():
    result = _analyze(baseline=D("480.00001"), reserve=D("48.75"))
    assert result.max_loss_usd == D("528.75001")
    assert result.pnl_at(D("700")) == D("-528.75001")
    assert result.gross_liquidation_cashflow_usd - result.estimated_future_exit_cost_usd == D("431.25001")


def test_recurring_breakeven_display_is_explicitly_approximate_with_exact_rational_evidence():
    result = _analyze((_leg(right="C", quantity=3),), baseline=D("1"), reserve=D("0"))
    assert result.breakevens_exact is False
    region = result.break_even_regions[0]
    assert region.is_point and region.lower_bound == Fraction(195001, 300)
    assert result.as_dict()["break_even_regions"][0]["lower_bound"] == {"numerator": "195001", "denominator": "300"}
    assert result.verify_hash() is True


def test_zero_pnl_plateau_is_an_exact_region_not_two_misleading_point_breakevens():
    result = _analyze(baseline=D("0"), reserve=D("0"))
    assert result.breakevens == ()
    assert len(result.break_even_regions) == 1
    assert result.break_even_regions[0].lower_bound == 680
    assert result.break_even_regions[0].upper_bound == 760
    flat_tail = _analyze((_leg(quantity=1), _leg(2, strike="680", quantity=-1)), baseline=D("0"), reserve=D("0"))
    assert flat_tail.break_even_regions[-1].upper_bound is None


def test_frozen_result_detached_serialization_and_hash_tampering():
    result = _analyze()
    with pytest.raises(FrozenInstanceError):
        result.max_loss_usd = D("0")
    document = result.as_dict()
    document["legs"].clear()
    document["segments"][0]["intercept"] = "0"
    assert len(result.as_dict()["legs"]) == 3 and result.verify_hash() is True
    object.__setattr__(result, "max_loss_usd", D("0"))
    assert result.verify_hash() is False


@pytest.mark.parametrize("price", [D("-1"), D("NaN"), 700, 700.0, True])
def test_price_queries_refuse_invalid_or_non_decimal_underlyings(price):
    with pytest.raises(HoldingsPayoffError):
        _analyze().pnl_at(price)
    with pytest.raises(HoldingsPayoffError):
        _analyze().intrinsic_at(price)
