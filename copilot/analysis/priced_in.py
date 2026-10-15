"""Pure calculations for measuring how much news-ledger drift is already priced in."""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from .models import View


def price_at_or_before(bars: Sequence[Sequence[Any]], published_at: datetime) -> float | None:
    target = published_at.timestamp()
    eligible = [bar for bar in bars if len(bar) >= 5 and _number(bar[0]) is not None and float(bar[0]) <= target]
    if not eligible:
        return None
    price = _number(max(eligible, key=lambda bar: float(bar[0]))[4])
    return price if price is not None and price > 0 else None


def apply_priced_in(
    view: View,
    contributors: Sequence[Mapping[str, Any]],
    *,
    current_spot: float,
    current_iv: float | None,
    now: datetime,
    publication_prices: Mapping[int, tuple[float, str] | None],
    publication_ivs: Mapping[int, float | None] | None = None,
    raw_drift_sigma: float | None = None,
) -> tuple[View, dict[str, Any]]:
    """Shrink the calibrated pricing drift by the fraction already travelled.

    The ledger's pre-calibration drift is the denominator. The shrink is applied to the
    calibrated pricing copy, so calibration and priced-in adjustment remain multiplicative.
    """

    raw = raw_drift_sigma if raw_drift_sigma is not None else view.drift_sigma
    diagnostics: dict[str, Any] = {
        "status": "unknown",
        "moved_sigma": None,
        "priced_in": 0.0,
        "iv_change": None,
        "remaining_drift_sigma": view.drift_sigma,
        "sources": [],
    }
    if not raw or raw <= 0 or not current_iv or current_iv <= 0 or current_spot <= 0:
        return view, diagnostics

    horizon_volatility = current_iv * math.sqrt(view.horizon_days / 365)
    expected_move = raw * horizon_volatility
    if expected_move <= 0:
        return view, diagnostics

    weighted_return = 0.0
    total_weight = 0.0
    iv_then_values: list[tuple[float, float]] = []
    for contributor in contributors:
        news_id = int(contributor.get("news_id") or 0)
        located = publication_prices.get(news_id)
        if not located:
            continue
        published_at = _datetime(contributor.get("published_at"))
        publication_price, source = located
        if published_at is None or publication_price <= 0:
            continue
        moved_return = view.sign * math.log(current_spot / publication_price)
        moved_sigma = moved_return / horizon_volatility
        weight = abs(float(contributor.get("value") or contributor.get("vol") or 0.0))
        if weight <= 0:
            weight = 1.0
        weighted_return += moved_return * weight
        total_weight += weight
        diagnostics["sources"].append({
            "news_id": news_id,
            "source": source,
            "publication_price": round(publication_price, 4),
            "moved_sigma": round(moved_sigma, 4),
            "weight": round(weight, 4),
        })
        iv_then = (publication_ivs or {}).get(news_id)
        if iv_then is not None and iv_then > 0:
            iv_then_values.append((iv_then, weight))

    if total_weight <= 0:
        return view, diagnostics

    moved_return = weighted_return / total_weight
    moved_sigma = moved_return / horizon_volatility
    priced_in = min(max(moved_return / expected_move, 0.0), 1.0)
    remaining = (view.drift_sigma or 0.0) * (1 - priced_in)
    adjusted = replace(view, drift_sigma=remaining)
    diagnostics.update({
        "status": "ok",
        "moved_sigma": round(moved_sigma, 4),
        "priced_in": round(priced_in, 4),
        "remaining_drift_sigma": round(remaining, 4),
    })

    if iv_then_values:
        weight_sum = sum(weight for _iv, weight in iv_then_values)
        iv_then = sum(iv * weight for iv, weight in iv_then_values) / weight_sum
        iv_change = current_iv - iv_then
        diagnostics["iv_change"] = round(iv_change, 4)
        multiplier = view.volatility_multiplier or 1.15
        if view.volatility == "expand" and iv_change >= (multiplier - 1) * iv_then:
            adjusted = replace(adjusted, volatility="neutral", volatility_multiplier=None)
            diagnostics["volatility_neutralized"] = True
    return adjusted, diagnostics


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


__all__ = ["apply_priced_in", "price_at_or_before"]
