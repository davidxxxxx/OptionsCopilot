"""Bounded EMA20 calculation semantics without source or policy authority."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext

from options_copilot.storage.canonical import canonical_hash, utc_datetime


EMA20_CONFIRMATION_OBSERVED_AT = datetime(
    2026,
    9,
    9,
    7,
    51,
    50,
    tzinfo=timezone.utc,
)
_EMA20_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN)
_EMA20_PERIOD = 20
_EMA20_WINDOW = 60


def calculate_ema20(closes: Sequence[Decimal]) -> Decimal:
    """Calculate one SMA-seeded EMA20 from exactly 60 bounded closes."""

    if not isinstance(closes, (tuple, list)):
        raise TypeError("EMA20 closes must be a tuple or list")
    if len(closes) != _EMA20_WINDOW:
        raise ValueError("EMA20 requires exactly 60 completed-session closes")
    values = tuple(closes)
    for value in values:
        if not isinstance(value, Decimal):
            raise TypeError("EMA20 closes must be Decimal values")
        decimal_tuple = value.as_tuple()
        if (
            not value.is_finite()
            or value <= 0
            or len(decimal_tuple.digits) > 64
            or not -64 <= decimal_tuple.exponent <= 64
        ):
            raise ValueError("EMA20 closes must be positive finite bounded Decimals")

    with localcontext(_EMA20_CONTEXT):
        ema = sum(values[:_EMA20_PERIOD], Decimal(0)) / Decimal(_EMA20_PERIOD)
        alpha = Decimal(2) / Decimal(21)
        one_minus_alpha = Decimal(1) - alpha
        for close in values[_EMA20_PERIOD:]:
            ema = alpha * close + one_minus_alpha * ema
        return +ema


def ema20_convention(cutoff: datetime) -> dict[str, object] | None:
    """Return the confirmed calculation-only convention at or after its boundary."""

    checked_at = utc_datetime(cutoff, field="EMA20 convention cutoff")
    if checked_at < EMA20_CONFIRMATION_OBSERVED_AT:
        return None
    body: dict[str, object] = {
        "schema": "options_copilot.ema20_convention.v1",
        "confirmation_observed_at": EMA20_CONFIRMATION_OBSERVED_AT.isoformat(),
        "convention_id": "EMA20_FIRST20_SMA_THEN40_EMA_UPDATES",
        "period": _EMA20_PERIOD,
        "window": _EMA20_WINDOW,
        "seed": {
            "method": "SIMPLE_MOVING_AVERAGE",
            "observations": _EMA20_PERIOD,
        },
        "updates": _EMA20_WINDOW - _EMA20_PERIOD,
        "alpha": {"numerator": 2, "denominator": 21},
        "decimal_precision": _EMA20_CONTEXT.prec,
        "decimal_rounding": "ROUND_HALF_EVEN",
        "price_basis": "IBKR_ADJUSTED_LAST_COMPLETED_SESSION_CLOSE",
        "scope": "CALCULATION_SEMANTICS_ONLY",
        "human_signature_verified": False,
        "production_eligible": False,
        "production_policy_status": "UNVERIFIED",
    }
    return {**body, "content_hash": canonical_hash(body)}


__all__ = [
    "EMA20_CONFIRMATION_OBSERVED_AT",
    "calculate_ema20",
    "ema20_convention",
]
