"""Exact QQQ benchmark semantics without taxonomy or production authority."""
from __future__ import annotations

from datetime import datetime, timezone

from options_copilot.storage.canonical import canonical_hash, utc_datetime


BENCHMARK_CONFIRMATION_OBSERVED_AT = datetime(
    2026,
    9,
    10,
    12,
    27,
    58,
    tzinfo=timezone.utc,
)


def benchmark_convention(symbol: str, cutoff: datetime) -> dict[str, object] | None:
    """Return the calculation-only QQQ-to-SPY mapping after its boundary."""

    checked_at = utc_datetime(cutoff, field="benchmark convention cutoff")
    if symbol != "QQQ" or checked_at < BENCHMARK_CONFIRMATION_OBSERVED_AT:
        return None
    body: dict[str, object] = {
        "schema": "options_copilot.benchmark_convention.v1",
        "confirmation_observed_at": BENCHMARK_CONFIRMATION_OBSERVED_AT.isoformat(),
        "symbol": "QQQ",
        "benchmark_symbol": "SPY",
        "scope": "CALCULATION_SEMANTICS_ONLY",
        "human_signature_verified": False,
        "production_eligible": False,
        "production_policy_status": "UNVERIFIED",
    }
    return {**body, "content_hash": canonical_hash(body)}


__all__ = [
    "BENCHMARK_CONFIRMATION_OBSERVED_AT",
    "benchmark_convention",
]
