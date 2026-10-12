"""Confirmed EMA20 calculation semantics remain bounded and non-authoritative."""
from __future__ import annotations

from datetime import timedelta
from decimal import Context, Decimal, ROUND_DOWN, ROUND_HALF_EVEN, localcontext

import pytest

from options_copilot.analytics.ema20 import (
    EMA20_CONFIRMATION_OBSERVED_AT,
    calculate_ema20,
    ema20_convention,
)
from options_copilot.storage.canonical import canonical_hash


def _trend() -> tuple[Decimal, ...]:
    return tuple(Decimal("100") + Decimal(index) / Decimal("7") for index in range(60))


def _independent_ema20(closes: tuple[Decimal, ...]) -> Decimal:
    context = Context(prec=28, rounding=ROUND_HALF_EVEN)
    with localcontext(context):
        seed = sum(closes[:20], Decimal(0)) / Decimal(20)
        alpha = Decimal(2) / Decimal(21)
        one_minus_alpha = Decimal(1) - alpha
        for close in closes[20:]:
            seed = alpha * close + one_minus_alpha * seed
        return +seed


def test_calculate_ema20_matches_constant_and_independent_trend() -> None:
    constant = (Decimal("123.45"),) * 60
    trend = _trend()

    assert calculate_ema20(constant) == Decimal("123.45")
    assert calculate_ema20(trend) == _independent_ema20(trend)


def test_calculate_ema20_is_independent_of_caller_decimal_context() -> None:
    closes = _trend()
    expected = calculate_ema20(closes)

    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN
        actual = calculate_ema20(closes)

    assert actual == expected


@pytest.mark.parametrize("count", [59, 61])
def test_calculate_ema20_requires_exactly_sixty_closes(count: int) -> None:
    with pytest.raises(ValueError, match="exactly 60"):
        calculate_ema20((Decimal("100"),) * count)


@pytest.mark.parametrize(
    "bad",
    [
        100.0,
        True,
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("0"),
        Decimal("-1"),
        Decimal("1e65"),
        Decimal("9" * 65),
    ],
)
def test_calculate_ema20_rejects_non_decimal_nonpositive_or_unbounded_values(
    bad: object,
) -> None:
    closes: list[object] = [Decimal("100")] * 60
    closes[37] = bad

    with pytest.raises((TypeError, ValueError)):
        calculate_ema20(closes)  # type: ignore[arg-type]


def test_calculate_ema20_result_does_not_depend_on_later_input_mutation() -> None:
    closes = list(_trend())
    result = calculate_ema20(closes)
    closes[0] = Decimal("999")

    assert result == _independent_ema20(_trend())


@pytest.mark.parametrize("closes", [None, Decimal("100"), "100"])
def test_calculate_ema20_rejects_non_sequences(closes: object) -> None:
    with pytest.raises(TypeError, match="tuple or list"):
        calculate_ema20(closes)  # type: ignore[arg-type]


def test_calculate_ema20_rejects_generator_without_consuming_it() -> None:
    consumed = False

    def values():
        nonlocal consumed
        consumed = True
        yield from (Decimal("100"),) * 60

    with pytest.raises(TypeError, match="tuple or list"):
        calculate_ema20(values())  # type: ignore[arg-type]
    assert consumed is False


def test_ema20_convention_begins_at_confirmation_boundary() -> None:
    assert ema20_convention(
        EMA20_CONFIRMATION_OBSERVED_AT - timedelta(microseconds=1)
    ) is None
    assert ema20_convention(EMA20_CONFIRMATION_OBSERVED_AT) is not None
    assert ema20_convention(
        EMA20_CONFIRMATION_OBSERVED_AT + timedelta(days=1)
    ) is not None
    with pytest.raises(ValueError, match="timezone-aware"):
        ema20_convention(EMA20_CONFIRMATION_OBSERVED_AT.replace(tzinfo=None))


def test_ema20_convention_is_fresh_hash_bound_and_never_authority() -> None:
    first = ema20_convention(EMA20_CONFIRMATION_OBSERVED_AT)
    second = ema20_convention(EMA20_CONFIRMATION_OBSERVED_AT)
    assert first is not None and second is not None
    assert first == second and first is not second
    content_hash = first.pop("content_hash")
    assert content_hash == canonical_hash(first)
    assert first == {
        "schema": "options_copilot.ema20_convention.v1",
        "confirmation_observed_at": "2026-09-09T07:51:50+00:00",
        "convention_id": "EMA20_FIRST20_SMA_THEN40_EMA_UPDATES",
        "period": 20,
        "window": 60,
        "seed": {"method": "SIMPLE_MOVING_AVERAGE", "observations": 20},
        "updates": 40,
        "alpha": {"numerator": 2, "denominator": 21},
        "decimal_precision": 28,
        "decimal_rounding": "ROUND_HALF_EVEN",
        "price_basis": "IBKR_ADJUSTED_LAST_COMPLETED_SESSION_CLOSE",
        "scope": "CALCULATION_SEMANTICS_ONLY",
        "human_signature_verified": False,
        "production_eligible": False,
        "production_policy_status": "UNVERIFIED",
    }
    first["human_signature_verified"] = True
    first["production_eligible"] = True
    first["production_policy_status"] = "VERIFIED"
    assert second["human_signature_verified"] is False
    assert second["production_eligible"] is False
    assert second["production_policy_status"] == "UNVERIFIED"
    assert second["content_hash"] != canonical_hash(first)
