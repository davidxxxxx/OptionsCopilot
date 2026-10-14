"""Pure daily option-chain market-state features for display and later analysis.

GEX uses the approximate dealer-long-calls/short-puts convention requested by the
decision-ledger plan; it is descriptive only and does not affect recommendations.
"""
from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import date, datetime, timedelta
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
    exposures = [
        float(quote.gamma) * int(quote.open_interest) * int(quote.multiplier or 100)
        * spot * spot * 0.01 * (1 if quote.right == "C" else -1)
        for quote in nonexpired
        if quote.gamma is not None and math.isfinite(quote.gamma)
        and quote.open_interest is not None
    ]
    gex = sum(exposures)
    gross_gex = sum(abs(value) for value in exposures)
    gex_sign = 0 if gross_gex and abs(gex) <= gross_gex * 0.01 else (1 if gex > 0 else -1 if gex < 0 else 0)
    expiries = sorted({quote.expiry for quote in nonexpired})
    nearest_expiry = expiries[0] if expiries else None
    monthly_expiry = _nearest_monthly_expiry(expiries, today)
    nearest_flow = _expiry_flow(nonexpired, nearest_expiry, spot, iv30, today)
    monthly_flow = _expiry_flow(nonexpired, monthly_expiry, spot, iv30, today)
    standard_opex = _third_friday(today.year, today.month)
    if today > standard_opex:
        next_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1)
        standard_opex = _third_friday(next_month.year, next_month.month)
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
        "gex_sign": gex_sign,
        **nearest_flow,
        **{f"monthly_{key}": value for key, value in monthly_flow.items()},
        "opex_week": standard_opex - timedelta(days=4) <= today <= standard_opex,
        "days_to_monthly_opex": (
            (monthly_expiry - today).days if monthly_expiry is not None else None
        ),
        "is_expiry_day": nearest_expiry == today,
        "oi_put_call": round(put_oi / call_oi, 4) if call_oi else None,
        "volume_put_call": round(put_volume / call_volume, 4) if call_volume else None,
        "hv20": _round(hv20),
        "iv_minus_hv20": _round(iv30 - hv20) if iv30 is not None and hv20 is not None else None,
        "vendor_iv30": _round(vendor_iv30),
        "expiries_used": [expiry.isoformat() for expiry in sorted(atm)],
    }


def _third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(4 - first.weekday()) % 7 + 14)


def _nearest_monthly_expiry(expiries: Sequence[date], today: date) -> date | None:
    available = set(expiries)
    months = sorted({(expiry.year, expiry.month) for expiry in expiries if expiry >= today})
    for year, month in months:
        friday = _third_friday(year, month)
        if friday in available:
            return friday
        thursday = friday - timedelta(days=1)
        if thursday in available:
            return thursday
    return None


def _expiry_flow(
    quotes: Sequence[OptionQuote],
    expiry: date | None,
    spot: float,
    iv30: float | None,
    today: date,
) -> dict[str, Any]:
    empty = {
        "expiry": None,
        "call_wall": None,
        "put_wall": None,
        "max_oi_strike": None,
        "max_pain": None,
        "nearest_listed_strike": None,
    }
    if expiry is None:
        return empty
    expiry_quotes = [quote for quote in quotes if quote.expiry == expiry]
    if not expiry_quotes:
        return empty
    strikes = sorted({float(quote.strike) for quote in expiry_quotes})
    nearby = [quote for quote in expiry_quotes if abs(float(quote.strike) / spot - 1) <= 0.15]

    def wall(right: str) -> dict[str, Any] | None:
        eligible = [
            quote for quote in nearby
            if quote.right == right and quote.open_interest is not None
            and quote.gamma is not None and math.isfinite(quote.gamma)
        ]
        if not eligible:
            return None
        quote = max(
            eligible,
            key=lambda item: int(item.open_interest or 0) * abs(float(item.gamma or 0.0))
            * int(item.multiplier or 100) * spot * spot,
        )
        return _strike_feature(float(quote.strike), expiry, spot, iv30, today)

    oi_by_strike = {
        strike: sum(int(quote.open_interest or 0) for quote in expiry_quotes if quote.strike == strike)
        for strike in strikes
    }
    max_oi = max(strikes, key=lambda strike: (oi_by_strike[strike], -abs(strike - spot)))
    payouts = {
        settlement: sum(
            int(quote.open_interest or 0) * int(quote.multiplier or 100)
            * (
                max(settlement - float(quote.strike), 0.0)
                if quote.right == "C"
                else max(float(quote.strike) - settlement, 0.0)
            )
            for quote in expiry_quotes
        )
        for settlement in strikes
    }
    max_pain = min(strikes, key=lambda strike: (payouts[strike], abs(strike - spot)))
    nearest = min(strikes, key=lambda strike: (abs(strike - spot), strike))
    return {
        "expiry": expiry.isoformat(),
        "call_wall": wall("C"),
        "put_wall": wall("P"),
        "max_oi_strike": _strike_feature(max_oi, expiry, spot, iv30, today),
        "max_pain": _strike_feature(max_pain, expiry, spot, iv30, today),
        "nearest_listed_strike": _strike_feature(nearest, expiry, spot, iv30, today),
    }


def _strike_feature(
    strike: float,
    expiry: date,
    spot: float,
    iv30: float | None,
    today: date,
) -> dict[str, Any]:
    distance_pct = strike / spot - 1
    dte = max((expiry - today).days, 0)
    sigma = iv30 * math.sqrt(dte / 365) if iv30 is not None and dte > 0 else None
    return {
        "strike": round(strike, 4),
        "expiry": expiry.isoformat(),
        "distance_pct": round(distance_pct, 6),
        "distance_sigma": round(distance_pct / sigma, 4) if sigma else None,
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
