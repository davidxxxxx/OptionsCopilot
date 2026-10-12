"""Calculation-only IV percentile semantics remain bounded and non-authoritative."""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal, localcontext

import pytest

from options_copilot.analytics.iv_percentile import (
    IV_PERCENTILE_CONFIRMATION_OBSERVED_AT,
    calculate_iv_percentile,
    iv_percentile_convention,
)
from options_copilot.storage.canonical import canonical_hash


def test_iv_percentile_convention_begins_at_confirmation_boundary() -> None:
    assert iv_percentile_convention(
        IV_PERCENTILE_CONFIRMATION_OBSERVED_AT - timedelta(microseconds=1)
    ) is None
    assert iv_percentile_convention(IV_PERCENTILE_CONFIRMATION_OBSERVED_AT) is not None
    assert iv_percentile_convention(
        IV_PERCENTILE_CONFIRMATION_OBSERVED_AT + timedelta(microseconds=1)
    ) is not None
    with pytest.raises(ValueError, match="timezone-aware"):
        iv_percentile_convention(
            IV_PERCENTILE_CONFIRMATION_OBSERVED_AT.replace(tzinfo=None)
        )


def test_iv_percentile_convention_is_fresh_hash_bound_and_never_authority() -> None:
    first = iv_percentile_convention(IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)
    second = iv_percentile_convention(IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)

    assert first == second
    assert first is not second
    assert first == {
        "schema": "options_copilot.iv_percentile_convention.v1",
        "confirmation_observed_at": "2026-09-10T01:57:44+00:00",
        "convention_id": "IBKR_NATIVE_UNDERLYING_IV_EMPIRICAL_CDF_252",
        "current_measure": "IBKR_NATIVE_CURRENT_UNDERLYING_IV",
        "history_measure": "IBKR_NATIVE_PRIOR_COMPLETED_SESSION_UNDERLYING_IV",
        "completed_session_window": 252,
        "cdf_comparison": "LESS_THAN_OR_EQUAL",
        "ties_included": True,
        "cdf_value_formula": "COUNT_PRIOR_IV_LE_CURRENT_IV_DIVIDED_BY_252",
        "feature_normalization": "2*CDF-1",
        "atm_option_iv_role": "SEPARATE_SURFACE_INPUT_NOT_PERCENTILE_CURRENT",
        "incomparable_behavior": "NO_CALCULATION",
        "decimal_precision": 28,
        "decimal_rounding": "ROUND_HALF_EVEN",
        "scope": "CALCULATION_SEMANTICS_ONLY",
        "human_signature_verified": False,
        "production_eligible": False,
        "production_policy_status": "UNVERIFIED",
        "content_hash": first["content_hash"],
    }
    assert first["content_hash"] == canonical_hash(
        {key: value for key, value in first.items() if key != "content_hash"}
    )


def test_iv_percentile_uses_exact_252_prior_values_and_includes_ties() -> None:
    history = tuple(Decimal(index + 1) / Decimal(1000) for index in range(252))

    assert calculate_iv_percentile(Decimal("0.126"), history) == Decimal(126) / Decimal(252)
    assert calculate_iv_percentile(Decimal("0.0005"), history) == Decimal(0)
    assert calculate_iv_percentile(Decimal("0.999"), history) == Decimal(1)
    with localcontext() as context:
        context.prec = 7
        assert calculate_iv_percentile(Decimal("0.126"), history) == Decimal(126) / Decimal(252)


@pytest.mark.parametrize(
    "current,history,error",
    (
        (Decimal("0.2"), tuple(Decimal("0.2") for _ in range(251)), "exactly 252"),
        (Decimal("0.2"), tuple(Decimal("0.2") for _ in range(253)), "exactly 252"),
        (Decimal("NaN"), tuple(Decimal("0.2") for _ in range(252)), "positive finite"),
        (Decimal("0"), tuple(Decimal("0.2") for _ in range(252)), "positive finite"),
        (Decimal("0.2"), (*tuple(Decimal("0.2") for _ in range(251)), Decimal("Infinity")), "positive finite"),
    ),
)
def test_iv_percentile_rejects_invalid_or_incomplete_values(
    current: Decimal,
    history: tuple[Decimal, ...],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        calculate_iv_percentile(current, history)


def test_iv_percentile_accepts_sequences_but_rejects_non_decimal_values() -> None:
    history = tuple(Decimal("0.2") for _ in range(252))
    assert calculate_iv_percentile(Decimal("0.2"), list(history)) == Decimal(1)
    with pytest.raises(TypeError, match="Decimal"):
        calculate_iv_percentile(0.2, history)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="Decimal"):
        calculate_iv_percentile(Decimal("0.2"), (*history[:-1], 0.2))  # type: ignore[arg-type]
