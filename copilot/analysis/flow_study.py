"""Pure option-flow hypothesis studies over stored market and price observations."""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from statistics import mean, stdev
from typing import Any
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")
MIN_DAYS = 20


def compute_flow_stats(
    market_states: Mapping[str, Sequence[dict[str, Any]]],
    intraday: Mapping[tuple[str, date], Sequence[Sequence[Any]]],
    daily_bars: Mapping[str, Sequence[Sequence[Any]]],
    *,
    min_days: int = MIN_DAYS,
) -> dict[str, Any]:
    """Compute descriptive GEX, expiry-pinning and wall tests from stored inputs."""

    normalized_daily = {
        symbol: {_day(row[0]): tuple(float(value) for value in row[1:5]) for row in rows}
        for symbol, rows in daily_bars.items()
    }
    return {
        "gex": _gex_study(market_states, intraday, normalized_daily, min_days),
        "pinning": _pinning_study(market_states, intraday, normalized_daily, min_days),
        "walls": _wall_study(market_states, normalized_daily, min_days),
    }


def _gex_study(
    market_states: Mapping[str, Sequence[dict[str, Any]]],
    intraday: Mapping[tuple[str, date], Sequence[Sequence[Any]]],
    daily: Mapping[str, Mapping[date, tuple[float, float, float, float]]],
    min_days: int,
) -> dict[str, Any]:
    samples: dict[int, list[tuple[float, float, float]]] = {-1: [], 0: [], 1: []}
    for symbol in ("SPY", "QQQ", "IWM"):
        closes = {day: values[3] for day, values in daily.get(symbol, {}).items()}
        ordered_days = sorted(closes)
        previous = {ordered_days[index]: closes[ordered_days[index - 1]] for index in range(1, len(ordered_days))}
        for state in market_states.get(symbol, ()):
            day = _day(state.get("as_of"))
            bars = intraday.get((symbol, day), ())
            if day not in previous or not bars:
                continue
            session = _session_values(bars)
            if session is None:
                continue
            close_1530, close_1600, high, low = session
            first_return = math.log(close_1530 / previous[day])
            last_return = math.log(close_1600 / close_1530)
            iv30 = _positive(state.get("iv_30d"))
            daily_sigma = iv30 / math.sqrt(252) if iv30 else None
            range_sigma = (high - low) / previous[day] / daily_sigma if daily_sigma else math.nan
            sign = int(state.get("gex_sign") or 0)
            samples.setdefault(sign, []).append((first_return, last_return, range_sigma))
    regimes = {
        name: _momentum_stats(samples[sign])
        for sign, name in ((-1, "negative"), (0, "neutral"), (1, "positive"))
    }
    observed = regimes["negative"]["n"] + regimes["positive"]["n"]
    verdict = "样本不足" if observed < min_days else (
        "负伽马动量更强"
        if regimes["negative"]["agreement_rate"] > regimes["positive"]["agreement_rate"]
        else "未发现负伽马动量优势"
    )
    return {"regimes": regimes, "n": observed, "verdict": verdict}


def _momentum_stats(rows: Sequence[tuple[float, float, float]]) -> dict[str, Any]:
    products = [left * right for left, right, _range in rows]
    ranges = [value for _left, _right, value in rows if math.isfinite(value)]
    return {
        "n": len(rows),
        "agreement_rate": _rounded(mean(left * right > 0 for left, right, _range in rows)) if rows else None,
        "correlation": _rounded(_correlation([row[0] for row in rows], [row[1] for row in rows])),
        "mean": _rounded(mean(products)) if products else None,
        "t": _rounded(_t_stat(products)),
        "mean_range_sigma": _rounded(mean(ranges)) if ranges else None,
    }


def _pinning_study(
    market_states: Mapping[str, Sequence[dict[str, Any]]],
    intraday: Mapping[tuple[str, date], Sequence[Sequence[Any]]],
    daily: Mapping[str, Mapping[date, tuple[float, float, float, float]]],
    min_days: int,
) -> dict[str, Any]:
    groups: dict[bool, list[float]] = {True: [], False: []}
    for symbol, states in market_states.items():
        for state in states:
            day = _day(state.get("as_of"))
            close = _close_for_day(symbol, day, intraday, daily)
            iv30 = _positive(state.get("iv_30d"))
            if close is None or iv30 is None:
                continue
            strikes = [
                _feature_strike(state.get(name))
                for name in ("max_oi_strike", "max_pain", "nearest_listed_strike")
            ]
            strikes = [strike for strike in strikes if strike is not None]
            if not strikes:
                continue
            sigma = close * iv30 / math.sqrt(252)
            groups[bool(state.get("is_expiry_day"))].append(
                min(abs(close - strike) for strike in strikes) / sigma
            )
    expiry = _distance_stats(groups[True])
    ordinary = _distance_stats(groups[False])
    observed = expiry["n"] + ordinary["n"]
    verdict = "样本不足" if observed < min_days or not expiry["n"] else (
        "到期日更接近关键行权价"
        if expiry["mean_distance_sigma"] < ordinary["mean_distance_sigma"]
        else "未发现到期钉住"
    )
    return {"expiry": expiry, "non_expiry": ordinary, "n": observed, "verdict": verdict}


def _distance_stats(values: Sequence[float]) -> dict[str, Any]:
    return {
        "n": len(values),
        "mean_distance_sigma": _rounded(mean(values)) if values else None,
        "t": _rounded(_t_stat(values)),
    }


def _wall_study(
    market_states: Mapping[str, Sequence[dict[str, Any]]],
    daily: Mapping[str, Mapping[date, tuple[float, float, float, float]]],
    min_days: int,
) -> dict[str, Any]:
    observations: list[dict[str, float | bool]] = []
    for symbol in ("SPY", "QQQ"):
        bars = daily.get(symbol, {})
        ordered = sorted(bars)
        next_day = {ordered[index]: ordered[index + 1] for index in range(len(ordered) - 1)}
        for state in market_states.get(symbol, ()):
            day = _day(state.get("as_of"))
            following = next_day.get(day)
            spot = _positive(state.get("spot"))
            call = _feature_strike(state.get("call_wall"))
            put = _feature_strike(state.get("put_wall"))
            if following is None or spot is None or call is None or put is None:
                continue
            _open, high, low, _close = bars[following]
            observations.append({
                "call_hit": high <= call,
                "put_hit": low >= put,
                "call_distance": call / spot - 1,
                "put_distance": 1 - put / spot,
                "spot": spot,
                "high": high,
                "low": low,
            })
    call_baselines = _shifted_hits(observations, "call")
    put_baselines = _shifted_hits(observations, "put")
    n = len(observations)
    wall_rate = mean(
        (int(bool(row["call_hit"])) + int(bool(row["put_hit"]))) / 2 for row in observations
    ) if observations else None
    baseline_rate = mean([*call_baselines, *put_baselines]) if observations else None
    verdict = "样本不足" if n < min_days else (
        "墙位优于等距离基准" if wall_rate > baseline_rate else "墙位未优于等距离基准"
    )
    return {
        "n": n,
        "wall_hold_rate": _rounded(wall_rate),
        "baseline_hold_rate": _rounded(baseline_rate),
        "verdict": verdict,
    }


def _shifted_hits(rows: Sequence[dict[str, float | bool]], side: str) -> list[bool]:
    if not rows:
        return []
    distances = [float(row[f"{side}_distance"]) for row in rows]
    shifted = distances[1:] + distances[:1]
    if side == "call":
        return [float(row["high"]) <= float(row["spot"]) * (1 + distance) for row, distance in zip(rows, shifted)]
    return [float(row["low"]) >= float(row["spot"]) * (1 - distance) for row, distance in zip(rows, shifted)]


def _session_values(bars: Sequence[Sequence[Any]]) -> tuple[float, float, float, float] | None:
    rows = []
    for bar in bars:
        if len(bar) < 5:
            continue
        stamp = datetime.fromtimestamp(int(bar[0]), tz=NEW_YORK)
        if stamp.hour < 9 or (stamp.hour == 9 and stamp.minute < 30) or stamp.hour >= 16:
            continue
        rows.append((stamp, float(bar[2]), float(bar[3]), float(bar[4])))
    if not rows:
        return None
    before_1530 = [row for row in rows if (row[0].hour, row[0].minute) <= (15, 30)]
    if not before_1530:
        return None
    return before_1530[-1][3], rows[-1][3], max(row[1] for row in rows), min(row[2] for row in rows)


def _close_for_day(
    symbol: str,
    day: date,
    intraday: Mapping[tuple[str, date], Sequence[Sequence[Any]]],
    daily: Mapping[str, Mapping[date, tuple[float, float, float, float]]],
) -> float | None:
    session = _session_values(intraday.get((symbol, day), ()))
    if session is not None:
        return session[1]
    row = daily.get(symbol, {}).get(day)
    return row[3] if row else None


def _feature_strike(value: Any) -> float | None:
    return _positive(value.get("strike")) if isinstance(value, dict) else None


def _correlation(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) < 2 or len(right) != len(left):
        return None
    left_mean, right_mean = mean(left), mean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right))
    denominator = math.sqrt(
        sum((x - left_mean) ** 2 for x in left) * sum((y - right_mean) ** 2 for y in right)
    )
    return numerator / denominator if denominator else None


def _t_stat(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    deviation = stdev(values)
    return mean(values) / (deviation / math.sqrt(len(values))) if deviation else None


def _positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _day(value: Any) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 6)


__all__ = ["MIN_DAYS", "compute_flow_stats"]
