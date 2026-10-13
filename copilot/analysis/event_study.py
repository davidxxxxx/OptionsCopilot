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


def surprise_rows(events: Sequence[MacroEvent]) -> list[dict[str, Any]]:
    """Selected releases with raw and standardized actual-minus-forecast surprise."""

    rows = []
    for event in indicator_events(events):
        actual = parse_number(event.actual)
        forecast = parse_number(event.forecast)
        if actual is None or forecast is None:
            continue
        rows.append({"event": event, "surprise": actual - forecast})
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


def compute_event_stats(
    category: str,
    symbol: str,
    events: Sequence[MacroEvent],
    bars: Sequence[tuple[date, float, float, float, float]],
    *,
    shrink_k: float,
) -> dict[str, Any] | None:
    """Compute standardized reaction slopes and event-day absolute-move ratio."""

    surprises = [row for row in surprise_rows(events) if row["event"].category == category]
    indexed = {bar[0]: index for index, bar in enumerate(bars)}
    closes = [bar[4] for bar in bars]
    reactions = []
    event_days: set[date] = set()
    for row in surprises:
        event = row["event"]
        day = event.event_time.astimezone(NEW_YORK).date()
        index = indexed.get(day)
        if index is None or index < 21:
            continue
        daily = [closes[i] / closes[i - 1] - 1 for i in range(index - 20, index)]
        vol = statistics.stdev(daily) if len(daily) >= 2 else 0.0
        if not math.isfinite(vol) or vol <= 0:
            continue
        _day, open_price, _high, _low, close_price = bars[index]
        previous_close = closes[index - 1]
        in_session = event.event_time.astimezone(NEW_YORK).time() >= time(9, 30)
        if in_session:
            gap = close_price / previous_close - 1
            after = closes[index + 1] / close_price - 1 if index + 1 < len(closes) else None
            after_label = "次日"
        else:
            gap = open_price / previous_close - 1
            after = close_price / open_price - 1
            after_label = "开盘后"
        if after is None:
            continue
        event_days.add(day)
        reactions.append({
            "date": day,
            "z": float(row["z"]),
            "gap": gap / vol,
            "after": after / vol,
            "day_abs": abs(close_price / previous_close - 1),
            "surprise_std": float(row["surprise_std"]),
            "after_label": after_label,
            "vol": vol,
        })
    if not reactions:
        return None
    normal_moves = [
        abs(closes[index] / closes[index - 1] - 1)
        for index in range(1, len(bars))
        if bars[index][0] not in event_days
    ]
    event_abs = statistics.fmean(row["day_abs"] for row in reactions)
    normal_abs = statistics.fmean(normal_moves) if normal_moves else 0.0
    ratio = event_abs / normal_abs if normal_abs > 0 else None
    payload: dict[str, Any] = {
        "category": category,
        "symbol": symbol,
        "n": len(reactions),
        "last_event_date": max(row["date"] for row in reactions).isoformat(),
        "event_abs_move": event_abs,
        "normal_abs_move": normal_abs,
        "abs_move_ratio": ratio,
        "surprise_std": reactions[-1]["surprise_std"],
        "after_label": reactions[-1]["after_label"],
        "mean_trailing_vol": statistics.fmean(row["vol"] for row in reactions),
    }
    if category == "fomc_decision":
        payload.update({
            "beta_gap": None, "se_gap": None, "t_gap": None, "beta_shrunk_gap": None,
            "beta_after": None, "se_after": None, "t_after": None, "beta_shrunk_after": None,
        })
        return payload
    for name in ("gap", "after"):
        beta, standard_error, t_stat = _origin_ols(
            [row["z"] for row in reactions], [row[name] for row in reactions]
        )
        payload[f"beta_{name}"] = beta
        payload[f"se_{name}"] = standard_error
        payload[f"t_{name}"] = t_stat
        payload[f"beta_shrunk_{name}"] = beta * len(reactions) / (len(reactions) + shrink_k)
    return payload


def build_macro_views(
    events: Sequence[MacroEvent],
    stats: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    settings: EventSettings,
) -> tuple[dict[tuple[str, str], View], dict[str, Any] | None]:
    """Build one latest qualifying macro view per symbol and its GUI study payload."""

    released = [
        row for row in surprise_rows(events)
        if row["event"].event_time <= now <= _view_active_until(row["event"])
    ]
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
        expected = float(beta) * float(latest["z"]) if beta is not None else None
        qualifies = bool(
            expected is not None
            and int(stat.get("n") or 0) >= settings.min_events
            and t_stat is not None
            and abs(float(t_stat)) >= settings.min_t
            and abs(expected) >= 0.05
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
            "z": latest["z"],
            "expected_after_sigma": expected,
            "expected_after_pct": (
                expected * float(stat["mean_trailing_vol"])
                if expected is not None and stat.get("mean_trailing_vol") is not None
                else None
            ),
            "generated": qualifies,
        })
    return views, {
        "event": event_payload(event, float(latest["z"]), float(latest["surprise_std"])),
        "rows": table,
    }


def event_payload(event: MacroEvent, z: float, surprise_std: float) -> dict[str, Any]:
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
        "z": z,
        "surprise_std": surprise_std,
        "surprise_text": _surprise_text(z),
    }


def _origin_ols(xs: Sequence[float], ys: Sequence[float]) -> tuple[float, float | None, float | None]:
    denominator = sum(value * value for value in xs)
    if denominator <= 0:
        return 0.0, None, None
    beta = sum(x * y for x, y in zip(xs, ys, strict=True)) / denominator
    if len(xs) < 2:
        return beta, None, None
    residual = sum((y - beta * x) ** 2 for x, y in zip(xs, ys, strict=True))
    standard_error = math.sqrt(residual / (len(xs) - 1) / denominator)
    t_stat = beta / standard_error if standard_error > 0 else math.inf if beta else 0.0
    return beta, standard_error, t_stat


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
    "STUDY_INDICATORS",
    "build_macro_views",
    "compute_event_stats",
    "event_payload",
    "indicator_events",
    "parse_number",
    "surprise_rows",
]
