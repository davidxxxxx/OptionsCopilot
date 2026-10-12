"""Bounded benchmark-mapping convention tests; no taxonomy or authority."""
from __future__ import annotations

from datetime import timedelta

import pytest

from options_copilot.analytics.benchmark import (
    BENCHMARK_CONFIRMATION_OBSERVED_AT,
    benchmark_convention,
)
from options_copilot.storage.canonical import canonical_hash


def test_benchmark_convention_activates_at_exact_observed_boundary() -> None:
    assert benchmark_convention(
        "QQQ",
        BENCHMARK_CONFIRMATION_OBSERVED_AT - timedelta(microseconds=1),
    ) is None

    convention = benchmark_convention(
        "QQQ",
        BENCHMARK_CONFIRMATION_OBSERVED_AT,
    )

    assert convention is not None
    assert convention == {
        "schema": "options_copilot.benchmark_convention.v1",
        "confirmation_observed_at": "2026-09-10T12:27:58+00:00",
        "symbol": "QQQ",
        "benchmark_symbol": "SPY",
        "scope": "CALCULATION_SEMANTICS_ONLY",
        "human_signature_verified": False,
        "production_eligible": False,
        "production_policy_status": "UNVERIFIED",
        "content_hash": convention["content_hash"],
    }
    body = dict(convention)
    del body["content_hash"]
    assert convention["content_hash"] == canonical_hash(body)


def test_benchmark_convention_is_exact_symbol_only_and_returns_fresh_metadata() -> None:
    cutoff = BENCHMARK_CONFIRMATION_OBSERVED_AT + timedelta(seconds=1)

    assert benchmark_convention("qqq", cutoff) is None
    assert benchmark_convention("SPY", cutoff) is None
    assert benchmark_convention("XOM", cutoff) is None

    first = benchmark_convention("QQQ", cutoff)
    second = benchmark_convention("QQQ", cutoff)
    assert first == second
    assert first is not second
    assert first is not None
    first["benchmark_symbol"] = "QQQ"
    assert second["benchmark_symbol"] == "SPY"
    assert second["content_hash"] == canonical_hash(
        {key: value for key, value in second.items() if key != "content_hash"}
    )


def test_benchmark_convention_rejects_naive_cutoff() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        benchmark_convention(
            "QQQ",
            BENCHMARK_CONFIRMATION_OBSERVED_AT.replace(tzinfo=None),
        )
