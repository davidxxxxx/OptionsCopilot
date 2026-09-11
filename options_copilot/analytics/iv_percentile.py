"""Bounded IV percentile calculation semantics without source authority."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext

from options_copilot.storage.canonical import canonical_hash, utc_datetime


IV_PERCENTILE_CONFIRMATION_OBSERVED_AT = datetime(
    2026,
    9,
    10,
    1,
    57,
    44,
    tzinfo=timezone.utc,
)
_IV_PERCENTILE_CONTEXT = Context(prec=28, rounding=ROUND_HALF_EVEN)
_IV_PERCENTILE_WINDOW = 252


def calculate_iv_percentile(
    current_underlying_iv: Decimal,
    prior_completed_underlying_ivs: Sequence[Decimal],
) -> Decimal:
    """Calculate an inclusive empirical CDF from exactly 252 prior values."""

    if not isinstance(prior_completed_underlying_ivs, (tuple, list)):
        raise TypeError("IV percentile history must be a tuple or list")
    if len(prior_completed_underlying_ivs) != _IV_PERCENTILE_WINDOW:
        raise ValueError("IV percentile requires exactly 252 prior completed-session values")
    _validate_iv(current_underlying_iv)
    values = tuple(prior_completed_underlying_ivs)
    for value in values:
        _validate_iv(value)
    with localcontext(_IV_PERCENTILE_CONTEXT):
        inclusive_count = sum(value <= current_underlying_iv for value in values)
        return Decimal(inclusive_count) / Decimal(_IV_PERCENTILE_WINDOW)


def iv_percentile_convention(cutoff: datetime) -> dict[str, object] | None:
    """Return confirmed calculation semantics at or after the observed boundary."""

    checked_at = utc_datetime(cutoff, field="IV percentile convention cutoff")
    if checked_at < IV_PERCENTILE_CONFIRMATION_OBSERVED_AT:
        return None
    body: dict[str, object] = {
        "schema": "options_copilot.iv_percentile_convention.v1",
        "confirmation_observed_at": IV_PERCENTILE_CONFIRMATION_OBSERVED_AT.isoformat(),
        "convention_id": "IBKR_NATIVE_UNDERLYING_IV_EMPIRICAL_CDF_252",
        "current_measure": "IBKR_NATIVE_CURRENT_UNDERLYING_IV",
        "history_measure": "IBKR_NATIVE_PRIOR_COMPLETED_SESSION_UNDERLYING_IV",
        "completed_session_window": _IV_PERCENTILE_WINDOW,
        "cdf_comparison": "LESS_THAN_OR_EQUAL",
        "ties_included": True,
        "cdf_value_formula": "COUNT_PRIOR_IV_LE_CURRENT_IV_DIVIDED_BY_252",
        "feature_normalization": "2*CDF-1",
        "atm_option_iv_role": "SEPARATE_SURFACE_INPUT_NOT_PERCENTILE_CURRENT",
        "incomparable_behavior": "NO_CALCULATION",
        "decimal_precision": _IV_PERCENTILE_CONTEXT.prec,
        "decimal_rounding": "ROUND_HALF_EVEN",
        "scope": "CALCULATION_SEMANTICS_ONLY",
        "human_signature_verified": False,
        "production_eligible": False,
        "production_policy_status": "UNVERIFIED",
    }
    return {**body, "content_hash": canonical_hash(body)}


def _validate_iv(value: object) -> None:
    if not isinstance(value, Decimal):
        raise TypeError("IV percentile values must be Decimals")
    decimal_tuple = value.as_tuple()
    if (
        not value.is_finite()
        or value <= 0
        or len(decimal_tuple.digits) > 64
        or not -64 <= decimal_tuple.exponent <= 64
    ):
        raise ValueError("IV percentile values must be positive finite bounded Decimals")


__all__ = [
    "IV_PERCENTILE_CONFIRMATION_OBSERVED_AT",
    "calculate_iv_percentile",
    "iv_percentile_convention",
]
