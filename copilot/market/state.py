"""Pure daily option-chain market-state features for display and later analysis.

GEX uses the approximate dealer-long-calls/short-puts convention requested by the
decision-ledger plan; it is descriptive only and does not affect recommendations.
"""
from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

from copilot.market.history import iv_rank_and_percentile
from copilot.market.models import OptionQuote


def market_state(
    quotes: Sequence[OptionQuote],
    *,
    spot: float,
    today: date,
    as_of: datetime | None = None,
    hv20: float | None = None,
    vendor_iv30: float | None = None,
) -> dict[str, Any] | None:
    usable = [
        quote for quote in quotes
        if quote.iv is not None and math.isfinite(quote.iv) and quote.iv > 0
        and (quote.expiry - today).days >= 2
    ]
    if not usable:
        return None
    by_expiry: dict[date, list[OptionQuote]] = {}
    for quote in usable:
        by_expiry.setdefault(quote.expiry, []).append(quote)
    atm = {
        expiry: value for expiry, expiry_quotes in by_expiry.items()
        if (value := _atm_iv(expiry_quotes, spot)) is not None
    }
    if not atm:
        return None
    nonexpired = [quote for quote in quotes if quote.expiry >= today]
    iv30, extrapolated30 = _target_iv(atm, today, 30)
    iv90, extrapolated90 = _target_iv(atm, today, 90)
    skew_expiry, skew_dte, skew, butterfly = _skew(by_expiry, atm, today)
    call_oi = sum(quote.open_interest or 0 for quote in nonexpired if quote.right == "C")
    put_oi = sum(quote.open_interest or 0 for quote in nonexpired if quote.right == "P")
    call_volume = sum(quote.volume or 0 for quote in nonexpired if quote.right == "C")
    put_volume = sum(quote.volume or 0 for quote in nonexpired if quote.right == "P")
    gex = sum(
        float(quote.gamma) * int(quote.open_interest) * int(quote.multiplier or 100)
        * spot * spot * 0.01 * (1 if quote.right == "C" else -1)
        for quote in nonexpired
        if quote.gamma is not None and math.isfinite(quote.gamma)
        and quote.open_interest is not None
    )
    return {
        "spot": round(spot, 2),
        "as_of": (as_of.isoformat() if as_of else today.isoformat()),
        "iv_30d": _round(iv30),
        "iv_90d": _round(iv90),
        "iv_30d_extrapolated": extrapolated30,
        "iv_90d_extrapolated": extrapolated90,
        "term_slope": _round(iv90 - iv30) if iv30 is not None and iv90 is not None else None,
        "skew_25d": _round(skew),
        "butterfly_25d": _round(butterfly),
        "skew_expiry": skew_expiry.isoformat() if skew_expiry else None,
        "skew_dte": skew_dte,
        "gex": round(gex, 2),
        "oi_put_call": round(put_oi / call_oi, 4) if call_oi else None,
        "volume_put_call": round(put_volume / call_volume, 4) if call_volume else None,
        "hv20": _round(hv20),
        "iv_minus_hv20": _round(iv30 - hv20) if iv30 is not None and hv20 is not None else None,
        "vendor_iv30": _round(vendor_iv30),
        "expiries_used": [expiry.isoformat() for expiry in sorted(atm)],
    }


def with_own_rank(latest: dict[str, Any], history: Sequence[dict[str, Any]]) -> dict[str, Any]:
    series = [row.get("iv_30d") for row in history[-252:]]
    _current, rank, percentile = iv_rank_and_percentile(series)
    return {
        **latest,
        "own_iv_rank": rank,
        "own_iv_percentile": percentile,
        "history_days": len([value for value in series if value is not None]),
    }


def _atm_iv(quotes: Sequence[OptionQuote], spot: float) -> float | None:
    by_strike: dict[float, dict[str, float]] = {}
    for quote in quotes:
        by_strike.setdefault(float(quote.strike), {})[quote.right] = float(quote.iv)
    points = sorted(
        (strike, (sides["C"] + sides["P"]) / 2)
        for strike, sides in by_strike.items() if "C" in sides and "P" in sides
    )
    if not points:
        return None
    below = [point for point in points if point[0] <= spot]
    above = [point for point in points if point[0] >= spot]
    if below and above:
        low, high = below[-1], above[0]
        if high[0] == low[0]:
            return low[1]
        weight = (spot - low[0]) / (high[0] - low[0])
        return low[1] + weight * (high[1] - low[1])
    return min(points, key=lambda point: abs(point[0] - spot))[1]


def _target_iv(atm: dict[date, float], today: date, target_days: int) -> tuple[float | None, bool]:
    points = sorted(((expiry - today).days, iv) for expiry, iv in atm.items())
    if not points:
        return None, False
    if target_days <= points[0][0]:
        return points[0][1], target_days < points[0][0]
    if target_days >= points[-1][0]:
        return points[-1][1], target_days > points[-1][0]
    low, high = next(
        (left, right) for left, right in zip(points, points[1:])
        if left[0] <= target_days <= right[0]
    )
    target_t = target_days / 365
    low_t, high_t = low[0] / 365, high[0] / 365
    low_w, high_w = low[1] ** 2 * low_t, high[1] ** 2 * high_t
    weight = (target_t - low_t) / (high_t - low_t)
    variance = (low_w + weight * (high_w - low_w)) / target_t
    return math.sqrt(max(variance, 0.0)), False


def _skew(
    by_expiry: dict[date, list[OptionQuote]], atm: dict[date, float], today: date
) -> tuple[date | None, int | None, float | None, float | None]:
    expiries = [expiry for expiry in atm if (expiry - today).days >= 7]
    if not expiries:
        return None, None, None, None
    expiry = min(expiries, key=lambda value: abs((value - today).days - 30))
    quotes = by_expiry[expiry]
    put = _delta_quote(quotes, "P", -0.25)
    call = _delta_quote(quotes, "C", 0.25)
    if put is None or call is None:
        return expiry, (expiry - today).days, None, None
    skew = float(put.iv) - float(call.iv)
    butterfly = (float(put.iv) + float(call.iv)) / 2 - atm[expiry]
    return expiry, (expiry - today).days, skew, butterfly


def _delta_quote(quotes: Sequence[OptionQuote], right: str, target: float) -> OptionQuote | None:
    eligible = [
        quote for quote in quotes
        if quote.right == right and quote.delta is not None
        and math.isfinite(quote.delta) and abs(quote.delta - target) <= 0.10
    ]
    return min(eligible, key=lambda quote: abs(float(quote.delta) - target)) if eligible else None


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 4)


__all__ = ["market_state", "with_own_rank"]
