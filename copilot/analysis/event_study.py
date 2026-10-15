"""Deterministic macro surprise and ETF reaction study."""
from __future__ import annotations

import math
import re
import statistics
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from copilot.analysis.models import View
from copilot.config import EventSettings
from copilot.market.calendar import is_trading_day
from copilot.news.macro_calendar import CATEGORY_NAMES, MacroEvent

NEW_YORK = ZoneInfo("America/New_York")
EVENT_STATS_VERSION = 3
CATEGORY_CAVEATS = {
    "gdp": "GDP 初值通常与大型科技股财报周重叠，结果可能受财报干扰",
}
STUDY_INDICATORS: dict[str, tuple[str, ...]] = {
    "cpi": ("Core Inflation Rate MoM", "Inflation Rate MoM"),
    "ppi": ("PPI MoM",),
    "nfp": ("Non Farm Payrolls",),
    "retail_sales": ("Retail Sales MoM",),
    "pce": ("Core PCE Price Index MoM",),
    "gdp": ("GDP Growth Rate QoQ Adv",),
    "fomc_decision": ("Fed Interest Rate Decision",),
}


def parse_number(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("$", "")
    if not text or text in {"—", "-", "N/A"}:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if match is None:
        return None
    number = float(match.group())
    suffix = text[match.end():].strip().upper()
    if suffix.startswith("K"):
        number *= 1_000
    elif suffix.startswith("M"):
        number *= 1_000_000
    elif suffix.startswith("B"):
        number *= 1_000_000_000
    return number


def indicator_events(events: Sequence[MacroEvent]) -> list[MacroEvent]:
    """One preferred indicator row per category and New York release date."""

    selected: dict[tuple[str, date], tuple[int, MacroEvent]] = {}
    for event in events:
        titles = STUDY_INDICATORS.get(event.category)
        if not titles:
            continue
        try:
            priority = titles.index(event.title)
        except ValueError:
            continue
        key = (event.category, event.event_time.astimezone(NEW_YORK).date())
        current = selected.get(key)
        if current is None or priority < current[0]:
            selected[key] = (priority, event)
    return [item[1] for item in sorted(selected.values(), key=lambda item: item[1].event_time)]


def raw_surprise_rows(events: Sequence[MacroEvent]) -> list[dict[str, Any]]:
    """Selected releases with numeric actual-minus-forecast surprises."""

    rows: list[dict[str, Any]] = []
    for event in indicator_events(events):
        actual = parse_number(event.actual)
        forecast = parse_number(event.forecast)
        if actual is None or forecast is None:
            continue
        rows.append({
            "event": event,
            "actual": actual,
            "forecast": forecast,
            "surprise": actual - forecast,
        })
    return rows


def surprise_rows(events: Sequence[MacroEvent]) -> list[dict[str, Any]]:
    """Selected releases with full-sample standardized surprises."""

    rows = raw_surprise_rows(events)
    by_category: dict[str, list[float]] = {}
    for row in rows:
        by_category.setdefault(row["event"].category, []).append(row["surprise"])
    result = []
    for row in rows:
        values = by_category[row["event"].category]
        deviation = statistics.stdev(values) if len(values) >= 2 else 0.0
        if not math.isfinite(deviation) or deviation <= 0:
            continue
        result.append({**row, "z": row["surprise"] / deviation, "surprise_std": deviation})
    return result


def reaction_observations(
    category: str,
    events: Sequence[MacroEvent],
    bars: Sequence[tuple[date, float, float, float, float]],
) -> list[dict[str, Any]]:
    """Return per-release market reactions, including incomplete in-session rows."""

    indexed = {bar[0]: index for index, bar in enumerate(bars)}
    closes = [bar[4] for bar in bars]
    observations: list[dict[str, Any]] = []
    for row in raw_surprise_rows(events):
        event = row["event"]
        if event.category != category:
            continue
        day = event.event_time.astimezone(NEW_YORK).date()
        index = indexed.get(day)
        if index is None or index < 21:
            continue
        daily = [closes[position] / closes[position - 1] - 1 for position in range(index - 20, index)]
        trailing_vol = statistics.stdev(daily) if len(daily) >= 2 else 0.0
        if not math.isfinite(trailing_vol) or trailing_vol <= 0:
            continue
        _day, open_price, _high, _low, close_price = bars[index]
        previous_close = closes[index - 1]
        gap = open_price / previous_close - 1
        day_return = close_price / previous_close - 1
        next_day = closes[index + 1] / close_price - 1 if index + 1 < len(closes) else None
        in_session = event.event_time.astimezone(NEW_YORK).time() >= time(9, 30)
        after = next_day if in_session else close_price / open_price - 1
        after_label = "次日" if in_session else "开盘后"
        observations.append({
            **row,
            "date": day,
            "trailing_vol": trailing_vol,
            "after_label": after_label,
            "status": "complete" if after is not None else "partial",
            "gap_sigma": (day_return if in_session else gap) / trailing_vol,
            "after_sigma": after / trailing_vol if after is not None else None,
            "day_abs": abs(day_return),
            "reaction": {
                "gap": {"return": gap, "sigma": gap / trailing_vol},
                "after": (
                    {"return": after, "sigma": after / trailing_vol}
                    if after is not None else None
                ),
                "day": {"return": day_return, "sigma": day_return / trailing_vol},
                "next_day": (
                    {"return": next_day, "sigma": next_day / trailing_vol}
                    if next_day is not None else None
                ),
                "after_label": after_label,
                "trailing_vol": trailing_vol,
            },
        })
    return observations


def compute_event_stats(
    category: str,
    symbol: str,
    events: Sequence[MacroEvent],
    bars: Sequence[tuple[date, float, float, float, float]],
    *,
    shrink_k: float,
    min_half_t: float,
    recency_half_life: float = 0.0,
    min_events: int = 12,
    min_t: float = 3.0,
    min_wf_views: int = 8,
    min_wf_hit_rate: float = 0.55,
) -> dict[str, Any] | None:
    """Compute standardized reaction slopes and event-day absolute-move ratio."""

    surprises = [row for row in raw_surprise_rows(events) if row["event"].category == category]
    deviation = statistics.stdev([row["surprise"] for row in surprises]) if len(surprises) >= 2 else 0.0
    if not math.isfinite(deviation) or deviation <= 0:
        return None
    reactions = [
        {**row, "z": float(row["surprise"]) / deviation}
        for row in reaction_observations(category, events, bars)
        if row["status"] == "complete"
    ]
    if not reactions:
        return None
    closes = [bar[4] for bar in bars]
    reactions.sort(key=lambda row: row["date"])
    event_days = {row["date"] for row in reactions}
    normal_moves = [
        abs(closes[index] / closes[index - 1] - 1)
        for index in range(1, len(bars))
        if bars[index][0] not in event_days
    ]
    event_abs = statistics.fmean(row["day_abs"] for row in reactions)
    normal_abs = statistics.fmean(normal_moves) if normal_moves else 0.0
    ratio = event_abs / normal_abs if normal_abs > 0 else None
    payload: dict[str, Any] = {
        "stats_version": EVENT_STATS_VERSION,
        "category": category,
        "symbol": symbol,
        "n": len(reactions),
        "last_event_date": max(row["date"] for row in reactions).isoformat(),
        "event_abs_move": event_abs,
        "normal_abs_move": normal_abs,
        "abs_move_ratio": ratio,
        "surprise_std": deviation,
        "after_label": reactions[-1]["after_label"],
        "mean_trailing_vol": statistics.fmean(row["trailing_vol"] for row in reactions),
        "caveat": CATEGORY_CAVEATS.get(category),
        "recency_half_life": recency_half_life,
        "min_wf_views": min_wf_views,
        "min_wf_hit_rate": min_wf_hit_rate,
    }
    walk_forward = walk_forward_evaluation(
        category,
        events,
        bars,
        shrink_k=shrink_k,
        min_events=min_events,
        min_t=min_t,
        min_half_t=min_half_t,
        recency_half_life=recency_half_life,
    )
    payload.update(walk_forward["metrics"])
    if category == "fomc_decision":
        payload.update({
            "beta_gap": None, "se_gap": None, "t_gap": None, "beta_shrunk_gap": None,
            "beta_gap_unweighted": None, "se_gap_unweighted": None, "t_gap_unweighted": None,
            "beta_after": None, "se_after": None, "t_after": None, "beta_shrunk_after": None,
            "beta_after_unweighted": None, "se_after_unweighted": None,
            "t_after_unweighted": None,
            "beta_gap_first": None, "t_gap_first": None,
            "beta_gap_second": None, "t_gap_second": None, "stable_gap": False,
            "beta_after_first": None, "t_after_first": None,
            "beta_after_second": None, "t_after_second": None, "stable_after": False,
        })
        return payload
    for name in ("gap", "after"):
        outcome = "gap_sigma" if name == "gap" else "after_sigma"
        weights = _recency_weights(len(reactions), recency_half_life)
        beta, standard_error, t_stat = _origin_ols(
            [row["z"] for row in reactions],
            [row[outcome] for row in reactions],
            weights,
        )
        beta_unweighted, se_unweighted, t_unweighted = _origin_ols(
            [row["z"] for row in reactions], [row[outcome] for row in reactions]
        )
        payload[f"beta_{name}"] = beta
        payload[f"se_{name}"] = standard_error
        payload[f"t_{name}"] = t_stat
        payload[f"beta_{name}_unweighted"] = beta_unweighted
        payload[f"se_{name}_unweighted"] = se_unweighted
        payload[f"t_{name}_unweighted"] = t_unweighted
        payload[f"beta_shrunk_{name}"] = beta * len(reactions) / (len(reactions) + shrink_k)
        split = len(reactions) // 2
        first = reactions[:split]
        second = reactions[split:]
        beta_first, _se_first, t_first = _origin_ols(
            [row["z"] for row in first],
            [row[outcome] for row in first],
            _recency_weights(len(first), recency_half_life),
        )
        beta_second, _se_second, t_second = _origin_ols(
            [row["z"] for row in second],
            [row[outcome] for row in second],
            _recency_weights(len(second), recency_half_life),
        )
        payload[f"beta_{name}_first"] = beta_first
        payload[f"t_{name}_first"] = t_first
        payload[f"beta_{name}_second"] = beta_second
        payload[f"t_{name}_second"] = t_second
        payload[f"stable_{name}"] = bool(
            len(first) >= 4
            and len(second) >= 4
            and beta * beta_first > 0
            and beta * beta_second > 0
            and t_first is not None
            and t_second is not None
            and abs(t_first) >= min_half_t
            and abs(t_second) >= min_half_t
        )
    return payload


def walk_forward_evaluation(
    category: str,
    events: Sequence[MacroEvent],
    bars: Sequence[tuple[date, float, float, float, float]],
    *,
    shrink_k: float,
    min_events: int,
    min_t: float,
    min_half_t: float,
    recency_half_life: float,
) -> dict[str, Any]:
    """Expanding-window predictions where every release uses strictly earlier data."""

    raw = [row for row in raw_surprise_rows(events) if row["event"].category == category]
    observations = {
        row["date"]: row for row in reaction_observations(category, events, bars)
    }
    predictions: list[dict[str, Any]] = []
    evaluated: list[dict[str, Any]] = []
    for index, current in enumerate(raw):
        event = current["event"]
        event_date = event.event_time.astimezone(NEW_YORK).date()
        prior_raw = raw[:index]
        deviation = (
            statistics.stdev([row["surprise"] for row in prior_raw])
            if len(prior_raw) >= 2 else 0.0
        )
        z = (
            float(current["surprise"]) / deviation
            if math.isfinite(deviation) and deviation > 0 else None
        )
        training = []
        if z is not None:
            for prior in prior_raw:
                prior_date = prior["event"].event_time.astimezone(NEW_YORK).date()
                observation = observations.get(prior_date)
                if observation is None or observation["status"] != "complete":
                    continue
                training.append({
                    **observation,
                    "z": float(prior["surprise"]) / deviation,
                })
        prediction = None
        if category != "fomc_decision" and z is not None and len(training) >= min_events:
            regression = _regression_summary(
                training,
                "after_sigma",
                half_life=recency_half_life,
                min_half_t=min_half_t,
            )
            beta = regression["beta"]
            t_stat = regression["t"]
            beta_shrunk = beta * len(training) / (len(training) + shrink_k)
            expected = beta_shrunk * z
            generated = bool(
                t_stat is not None
                and abs(float(t_stat)) >= min_t
                and regression["stable"]
                and abs(expected) >= 0.05
            )
            observation = observations.get(event_date)
            trailing_vol = observation.get("trailing_vol") if observation else None
            prediction = {
                "source": "walk_forward",
                "expected_after_sigma": expected,
                "expected_after_pct": expected * trailing_vol if trailing_vol is not None else None,
                "beta": beta_shrunk,
                "t": t_stat,
                "stable": regression["stable"],
                "generated_view": generated,
                "stats_version": EVENT_STATS_VERSION,
            }
            realized = observation.get("after_sigma") if observation else None
            if realized is not None:
                evaluated.append({
                    "expected": expected,
                    "realized": float(realized),
                    "generated": generated,
                })
        predictions.append({
            "event": event,
            "event_date": event_date,
            "actual": current["actual"],
            "forecast": current["forecast"],
            "surprise": current["surprise"],
            "z": z,
            "prediction": prediction,
            "observation": observations.get(event_date),
        })
    eligible = [row for row in evaluated if abs(row["expected"]) >= 0.05]
    views = [row for row in evaluated if row["generated"]]
    correlation = _correlation(
        [row["expected"] for row in evaluated], [row["realized"] for row in evaluated]
    )
    correlation_t = _correlation_t(correlation, len(evaluated))
    return {
        "predictions": predictions,
        "metrics": {
            "wf_n": len(evaluated),
            "wf_views": len(views),
            "wf_hit_rate": _hit_rate(eligible),
            "wf_corr": correlation,
            "wf_corr_t": correlation_t,
            "wf_view_hit_rate": _hit_rate(views),
            "wf_view_mean": (
                statistics.fmean(
                    row["realized"] * (1 if row["expected"] > 0 else -1)
                    for row in views
                )
                if views else None
            ),
        },
    }


def build_macro_views(
    events: Sequence[MacroEvent],
    stats: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    settings: EventSettings,
) -> tuple[dict[tuple[str, str], View], dict[str, Any] | None]:
    """Build one latest qualifying macro view per symbol and its GUI study payload."""

    released = _active_surprises(events, now)
    if not released:
        return {}, None
    latest = max(released, key=lambda row: row["event"].event_time)
    event = latest["event"]
    rows = [dict(row) for row in stats if row.get("category") == event.category]
    views: dict[tuple[str, str], View] = {}
    table = []
    for stat in rows:
        beta = stat.get("beta_shrunk_after")
        t_stat = stat.get("t_after")
        expected = (
            float(beta) * float(latest["z"])
            if beta is not None and latest.get("z") is not None else None
        )
        qualifies = bool(
            expected is not None
            and stat.get("stats_version") == EVENT_STATS_VERSION
            and int(stat.get("n") or 0) >= settings.min_events
            and t_stat is not None
            and abs(float(t_stat)) >= settings.min_t
            and stat.get("stable_after") is True
            and abs(expected) >= 0.05
            and int(stat.get("wf_views") or 0) >= settings.min_wf_views
            and float(stat.get("wf_view_hit_rate") or 0.0) >= settings.min_wf_hit_rate
            and float(stat.get("wf_view_mean") or 0.0) > 0
        )
        if qualifies:
            conviction = 5 if abs(float(t_stat)) >= 4 else 4 if abs(float(t_stat)) >= 3 else 3
            symbol = str(stat["symbol"])
            in_session = event.event_time.astimezone(NEW_YORK).time() >= time(9, 30)
            views[("macro", symbol)] = View(
                symbol=symbol,
                direction="bullish" if expected > 0 else "bearish",
                conviction=conviction,
                expected_move_pct=0.0,
                horizon_days=2 if in_session else 1,
                catalyst=CATEGORY_NAMES[event.category],
                catalyst_date=event.event_time.astimezone(NEW_YORK).date(),
                rationale=(
                    f"{CATEGORY_NAMES[event.category]} {_surprise_text(float(latest['z']))}；"
                    f"历史 {stat['n']} 次：{stat.get('after_label') or '开盘后'} {symbol} "
                    f"平均 {float(beta):+.2f}σ/σ（t={float(t_stat):.1f}）"
                ),
                source="macro",
                drift_sigma=abs(expected) * math.sqrt(365 / 252),
            )
        table.append({
            **stat,
            "z": latest.get("z"),
            "expected_after_sigma": expected,
            "expected_after_pct": (
                expected * float(stat["mean_trailing_vol"])
                if expected is not None and stat.get("mean_trailing_vol") is not None
                else None
            ),
            "stable": stat.get("stable_after") is True,
            "wf_pass": bool(
                int(stat.get("wf_views") or 0) >= settings.min_wf_views
                and float(stat.get("wf_view_hit_rate") or 0.0) >= settings.min_wf_hit_rate
                and float(stat.get("wf_view_mean") or 0.0) > 0
            ),
            "generated": qualifies,
        })
    return views, {
        "event": event_payload(event, latest.get("z"), latest.get("surprise_std")),
        "rows": table,
        "thresholds": {
            "min_events": settings.min_events,
            "min_t": settings.min_t,
            "min_half_t": settings.min_half_t,
            "min_wf_views": settings.min_wf_views,
            "min_wf_hit_rate": settings.min_wf_hit_rate,
        },
    }


def event_payload(
    event: MacroEvent,
    z: float | None,
    surprise_std: float | None,
) -> dict[str, Any]:
    actual = parse_number(event.actual)
    forecast = parse_number(event.forecast)
    local = event.event_time.astimezone(NEW_YORK)
    return {
        "category": event.category,
        "name": CATEGORY_NAMES[event.category],
        "title": event.title,
        "time": event.event_time.isoformat(),
        "time_ny": local.strftime("%m/%d %H:%M ET"),
        "actual": actual,
        "forecast": forecast,
        "surprise": (actual - forecast) if actual is not None and forecast is not None else None,
        "z": z,
        "surprise_std": surprise_std,
        "surprise_text": _surprise_text(z) if z is not None else "历史样本不足，暂无法标准化",
        "caveat": CATEGORY_CAVEATS.get(event.category),
    }


def _origin_ols(
    xs: Sequence[float],
    ys: Sequence[float],
    weights: Sequence[float] | None = None,
) -> tuple[float, float | None, float | None]:
    actual_weights = list(weights) if weights is not None else [1.0] * len(xs)
    denominator = sum(weight * value * value for value, weight in zip(xs, actual_weights, strict=True))
    if denominator <= 0:
        return 0.0, None, None
    beta = sum(
        weight * x * y
        for x, y, weight in zip(xs, ys, actual_weights, strict=True)
    ) / denominator
    if len(xs) < 2:
        return beta, None, None
    residual = sum(
        weight * (y - beta * x) ** 2
        for x, y, weight in zip(xs, ys, actual_weights, strict=True)
    )
    standard_error = math.sqrt(residual / (len(xs) - 1) / denominator)
    t_stat = beta / standard_error if standard_error > 0 else math.inf if beta else 0.0
    return beta, standard_error, t_stat


def _recency_weights(count: int, half_life: float) -> list[float]:
    if half_life <= 0:
        return [1.0] * count
    return [0.5 ** ((count - 1 - index) / half_life) for index in range(count)]


def _regression_summary(
    rows: Sequence[Mapping[str, Any]],
    outcome: str,
    *,
    half_life: float,
    min_half_t: float,
) -> dict[str, Any]:
    xs = [float(row["z"]) for row in rows]
    ys = [float(row[outcome]) for row in rows]
    beta, standard_error, t_stat = _origin_ols(xs, ys, _recency_weights(len(rows), half_life))
    split = len(rows) // 2
    halves = (rows[:split], rows[split:])
    half_results = []
    for half in halves:
        half_results.append(_origin_ols(
            [float(row["z"]) for row in half],
            [float(row[outcome]) for row in half],
            _recency_weights(len(half), half_life),
        ))
    stable = bool(
        all(len(half) >= 4 for half in halves)
        and all(result[2] is not None and abs(float(result[2])) >= min_half_t for result in half_results)
        and all(beta * result[0] > 0 for result in half_results)
    )
    return {
        "beta": beta,
        "se": standard_error,
        "t": t_stat,
        "stable": stable,
    }


def _active_surprises(events: Sequence[MacroEvent], now: datetime) -> list[dict[str, Any]]:
    raw = raw_surprise_rows(events)
    active: list[dict[str, Any]] = []
    for index, row in enumerate(raw):
        event = row["event"]
        if not (event.event_time <= now <= _view_active_until(event)):
            continue
        prior = [
            item["surprise"] for item in raw[:index]
            if item["event"].category == event.category
        ]
        deviation = statistics.stdev(prior) if len(prior) >= 2 else 0.0
        active.append({
            **row,
            "z": float(row["surprise"]) / deviation
            if math.isfinite(deviation) and deviation > 0 else None,
            "surprise_std": deviation if math.isfinite(deviation) and deviation > 0 else None,
        })
    return active


def _hit_rate(rows: Sequence[Mapping[str, Any]]) -> float | None:
    if not rows:
        return None
    return sum(row["expected"] * row["realized"] > 0 for row in rows) / len(rows)


def _correlation(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) < 2 or len(ys) != len(xs):
        return None
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True))
    denominator = math.sqrt(
        sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys)
    )
    return numerator / denominator if denominator > 0 else None


def _correlation_t(correlation: float | None, count: int) -> float | None:
    if correlation is None or count < 3:
        return None
    denominator = max(1 - correlation * correlation, 0.0)
    if denominator == 0:
        return math.copysign(math.inf, correlation)
    return correlation * math.sqrt((count - 2) / denominator)


def _view_active_until(event: MacroEvent) -> datetime:
    local = event.event_time.astimezone(NEW_YORK)
    active_day = local.date()
    if local.time() >= time(9, 30):
        active_day = date.fromordinal(active_day.toordinal() + 1)
        while not is_trading_day(active_day):
            active_day = date.fromordinal(active_day.toordinal() + 1)
    return datetime.combine(active_day, time(16, 0), NEW_YORK)


def _surprise_text(z: float) -> str:
    return f"{'超' if z >= 0 else '低于'}预期 {z:+.1f}σ"


__all__ = [
    "CATEGORY_CAVEATS",
    "EVENT_STATS_VERSION",
    "STUDY_INDICATORS",
    "build_macro_views",
    "compute_event_stats",
    "event_payload",
    "indicator_events",
    "parse_number",
    "raw_surprise_rows",
    "reaction_observations",
    "surprise_rows",
    "walk_forward_evaluation",
]
