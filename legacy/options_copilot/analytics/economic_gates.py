"""Candidate-local enforcement of the frozen signed economic thresholds."""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal


def signed_economics_reasons(
    values: Mapping[str, object], thresholds: Mapping[str, object],
) -> tuple[str, ...]:
    """Reject unavailable or nonconforming economic proof; never estimate it."""
    expected = {
        "minimum_after_cost_expected_value_usd": "max(5.00,0.05*maximum_loss)",
        "minimum_max_profit_to_maximum_loss": "1.20",
        "maximum_total_round_trip_cost_to_max_profit": "0.20",
        "stress_after_cost_ev": "must be greater than or equal to 0.00",
    }
    if any(thresholds.get(key) != value for key, value in expected.items()):
        return ("SIGNED_ECONOMIC_THRESHOLDS_UNSUPPORTED",)
    loss = _decimal(values.get("max_loss"))
    profit = _decimal(values.get("max_profit"))
    ev = _decimal(values.get("after_cost_expected_value"))
    cost = _decimal(values.get("execution_cost_usd"))
    stress_ev = _decimal(values.get("stress_after_cost_expected_value"))
    legs = values.get("legs")
    unbounded = (
        values.get("max_profit_type") == "UNBOUNDED"
        and values.get("max_profit") is None
        and values.get("structure") == "LONG_OPTION"
        and isinstance(legs, (tuple, list)) and len(legs) == 1
        and isinstance(legs[0], Mapping)
        and legs[0].get("side") in {"BUY", "LONG"}
        and legs[0].get("right") in {"CALL", "C"}
    )
    reasons: list[str] = []
    if loss is None or loss <= 0 or ev is None:
        reasons.append("SIGNED_MINIMUM_EV_EVIDENCE_UNAVAILABLE")
    elif ev <= max(Decimal("5.00"), Decimal("0.05") * loss):
        reasons.append("SIGNED_MINIMUM_AFTER_COST_EV_NOT_MET")
    if loss is None or loss <= 0 or (not unbounded and (profit is None or profit <= 0)):
        reasons.append("SIGNED_REWARD_RISK_EVIDENCE_UNAVAILABLE")
    elif not unbounded and profit < Decimal("1.20") * loss:
        reasons.append("SIGNED_MINIMUM_REWARD_RISK_NOT_MET")
    if (not unbounded and (profit is None or profit <= 0)) or cost is None or cost < 0:
        reasons.append("SIGNED_COST_RATIO_EVIDENCE_UNAVAILABLE")
    elif not unbounded and cost > Decimal("0.20") * profit:
        reasons.append("SIGNED_MAXIMUM_COST_RATIO_EXCEEDED")
    if stress_ev is None:
        reasons.append("SIGNED_STRESS_EV_EVIDENCE_UNAVAILABLE")
    elif stress_ev < 0:
        reasons.append("SIGNED_STRESS_AFTER_COST_EV_NEGATIVE")
    return tuple(reasons)


def _decimal(value: object) -> Decimal | None:
    return value if isinstance(value, Decimal) and value.is_finite() else None


__all__ = ["signed_economics_reasons"]
