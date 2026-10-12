"""Signed economics reject missing proof and preserve exact boundary rules."""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from options_copilot.analytics.economic_gates import signed_economics_reasons
from options_copilot.analytics.scenarios import InitialPolicyResolver


THRESHOLDS = InitialPolicyResolver().resolve(
    now=datetime(2026, 9, 7, tzinfo=timezone.utc),
).payload["hard_no_trade_thresholds"]["cost_and_expectancy"]
VALUES = {
    "max_loss": Decimal("100"), "max_profit": Decimal("120"),
    "after_cost_expected_value": Decimal("6"), "execution_cost_usd": Decimal("24"),
    "stress_after_cost_expected_value": Decimal("0"),
}


@pytest.mark.parametrize("mutation,reason", [
    ({}, None),
    ({"after_cost_expected_value": Decimal("5")}, "SIGNED_MINIMUM_AFTER_COST_EV_NOT_MET"),
    ({"max_profit": Decimal("119.99")}, "SIGNED_MINIMUM_REWARD_RISK_NOT_MET"),
    ({"execution_cost_usd": Decimal("24.01")}, "SIGNED_MAXIMUM_COST_RATIO_EXCEEDED"),
    ({"stress_after_cost_expected_value": Decimal("-.01")}, "SIGNED_STRESS_AFTER_COST_EV_NEGATIVE"),
    ({"execution_cost_usd": Decimal("NaN")}, "SIGNED_COST_RATIO_EVIDENCE_UNAVAILABLE"),
])
def test_signed_boundaries(mutation, reason):
    reasons = signed_economics_reasons({**VALUES, **mutation}, THRESHOLDS)
    if reason is None:
        assert reasons == ()
    else:
        assert reason in reasons


def test_unbounded_long_call_requires_explicit_payoff_and_geometry_proof():
    values = {**VALUES, "max_profit": None, "max_profit_type": "UNBOUNDED",
              "structure": "LONG_OPTION", "legs": ({"side": "LONG", "right": "CALL"},)}
    assert signed_economics_reasons(values, THRESHOLDS) == ()
    for mutation in (
        {"max_profit_type": None}, {"structure": "DEBIT_VERTICAL"},
        {"legs": ({"side": "SHORT", "right": "CALL"},)},
        {"legs": ({"side": "LONG", "right": "PUT"},)},
    ):
        assert "SIGNED_REWARD_RISK_EVIDENCE_UNAVAILABLE" in signed_economics_reasons(
            {**values, **mutation}, THRESHOLDS,
        )
    assert "SIGNED_STRESS_AFTER_COST_EV_NEGATIVE" in signed_economics_reasons(
        {**values, "stress_after_cost_expected_value": Decimal("-1")}, THRESHOLDS,
    )
