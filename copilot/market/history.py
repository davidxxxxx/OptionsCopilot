"""Daily-bar statistics: realised volatility, returns and implied-volatility rank."""
from __future__ import annotations

import math
from collections.abc import Sequence


def realized_vol(closes: Sequence[float], window: int = 20) -> float | None:
    """Annualised close-to-close volatility over the last ``window`` returns."""

    prices = [value for value in closes if value and value > 0]
    if len(prices) < window + 1:
        return None
    tail = prices[-(window + 1):]
    returns = [math.log(b / a) for a, b in zip(tail, tail[1:])]
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    return math.sqrt(variance) * math.sqrt(252)


def trailing_return(closes: Sequence[float], days: int) -> float | None:
    prices = [value for value in closes if value and value > 0]
    if len(prices) < days + 1:
        return None
    return prices[-1] / prices[-(days + 1)] - 1


def iv_rank_and_percentile(series: Sequence[float]) -> tuple[float | None, float | None, float | None]:
    """Return (current IV, IV rank 0-1, IV percentile 0-1) from a daily IV history."""

    values = [value for value in series if value and value > 0 and math.isfinite(value)]
    if len(values) < 20:
        return (values[-1] if values else None), None, None
    current = values[-1]
    low, high = min(values), max(values)
    rank = (current - low) / (high - low) if high > low else None
    percentile = sum(1 for value in values[:-1] if value < current) / (len(values) - 1)
    return current, rank, percentile


__all__ = ["iv_rank_and_percentile", "realized_vol", "trailing_return"]
