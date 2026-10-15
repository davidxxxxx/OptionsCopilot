"""View ledger: per-symbol views computed from stored news tags by fixed rules.

Each tagged impact is worth
    direction x magnitude x importance_weight x 0.5 ** (age / half_life(horizon)).
Repeated evidence has diminishing weight: within one (category, sign) group the
strongest item counts fully, the next 1/2, then 1/4, ... so a story re-posted ten
times adds at most about twice its strongest version, while independent kinds of
evidence (macro vs earnings, say) still add up.
A directional view opens at |score| >= open_threshold, reverses only when the score
crosses the opposite open threshold, and closes when |score| drops below
close_threshold. The same stored tags at the same time always give the same views,
and every change is recorded with the news that caused it.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from copilot.config import ViewsSettings
from copilot.news.models import EarningsEvent, NewsItem, is_upcoming, release_time
from .models import View
from .tagging import NewsTag

NEW_YORK = ZoneInfo("America/New_York")
HORIZON_DAYS = {"intraday": 2, "days": 7, "weeks": 21}
DEFAULT_HORIZON = 14
CONTRIBUTORS_KEPT = 8


@dataclass(frozen=True)
class Contribution:
    news_id: int
    value: float  # signed direction contribution
    vol: float  # signed volatility contribution
    summary: str
    published_at: str
    category: str = "other"


@dataclass
class Evidence:
    score: float = 0.0
    vol_score: float = 0.0
    horizon_weight: float = 0.0
    horizon_sum: float = 0.0
    contributions: list[Contribution] = field(default_factory=list)

    @property
    def horizon_days(self) -> int:
        if self.horizon_weight <= 0:
            return DEFAULT_HORIZON
        return max(1, round(self.horizon_sum / self.horizon_weight))

    def top(self, count: int = CONTRIBUTORS_KEPT) -> list[Contribution]:
        return sorted(self.contributions, key=lambda c: abs(c.value) + abs(c.vol), reverse=True)[:count]


@dataclass
class LedgerResult:
    views: dict[str, View]
    book: dict[str, dict[str, Any]]
    changes: list[dict[str, Any]]


def score_news(
    tagged: Sequence[tuple[NewsItem, NewsTag]], *, now: datetime, settings: ViewsSettings
) -> dict[str, Evidence]:
    half_lives = {
        "intraday": settings.half_life_intraday,
        "days": settings.half_life_days,
        "weeks": settings.half_life_weeks,
    }
    evidence: dict[str, Evidence] = {}
    for item, tag in tagged:
        weight = settings.importance_weights[max(0, min(3, tag.importance))]
        if weight <= 0:
            continue
        age_days = max(0.0, (now - item.published_at).total_seconds() / 86400)
        for impact in tag.impacts:
            decay = 0.5 ** (age_days / half_lives.get(impact.horizon, settings.half_life_days))
            strength = impact.magnitude * weight * decay
            vol_sign = {"up": 1, "down": -1}.get(impact.vol, 0)
            value, vol = impact.direction * strength, vol_sign * strength
            if abs(value) < 1e-4 and abs(vol) < 1e-4:
                continue
            entry = evidence.setdefault(impact.symbol, Evidence())
            entry.horizon_sum += HORIZON_DAYS.get(impact.horizon, 7) * strength
            entry.horizon_weight += strength
            entry.contributions.append(Contribution(
                news_id=tag.news_id,
                value=round(value, 3),
                vol=round(vol, 3),
                summary=tag.summary or item.title[:60],
                published_at=item.published_at.isoformat(),
                category=tag.category,
            ))
    for entry in evidence.values():
        entry.score = _diminishing(entry.contributions, "value")
        entry.vol_score = _diminishing(entry.contributions, "vol")
    return evidence


def _diminishing(contributions: Sequence[Contribution], field_name: str) -> float:
    groups: dict[tuple[str, bool], list[float]] = {}
    for contribution in contributions:
        value = getattr(contribution, field_name)
        if abs(value) >= 1e-4:
            groups.setdefault((contribution.category, value > 0), []).append(value)
    total = 0.0
    for values in groups.values():
        ordered = sorted(values, key=abs, reverse=True)
        total += sum(value * 0.5 ** index for index, value in enumerate(ordered))
    return total


def update_ledger(
    previous: Mapping[str, Mapping[str, Any]],
    evidence: Mapping[str, Evidence],
    *,
    earnings: Sequence[EarningsEvent],
    core: Sequence[str],
    now: datetime,
    settings: ViewsSettings,
    max_drift_sigma: float = 1.0,
) -> LedgerResult:
    """New ledger state from the previous one plus current evidence (pure function)."""

    today = now.astimezone(NEW_YORK).date()
    next_earnings: dict[str, EarningsEvent] = {}
    for event in sorted(earnings, key=release_time):
        if is_upcoming(event, now):
            next_earnings.setdefault(event.symbol, event)
    candidates = list(dict.fromkeys([*previous, *evidence]))
    book: dict[str, dict[str, Any]] = {}
    changes: list[dict[str, Any]] = []
    stamp = now.isoformat(timespec="seconds")
    for symbol in candidates:
        ev = evidence.get(symbol, Evidence())
        prev = previous.get(symbol)
        direction = _direction(prev.get("direction") if prev else "neutral", ev.score, settings)
        volatility = _volatility(prev.get("volatility") if prev else "neutral", ev.vol_score, settings)
        if direction == "neutral" and volatility == "neutral":
            if prev:
                changes.append(_change(symbol, "close", prev, None, ev, stamp))
            continue
        previous_conviction = int(prev.get("conviction") or 1) if prev and prev.get("direction") == direction else None
        entry = _entry(
            symbol, direction, volatility, ev, next_earnings.get(symbol), settings, today, previous_conviction,
            max_drift_sigma,
        )
        entry["opened_at"] = prev.get("opened_at", stamp) if prev and prev.get("direction") == direction else stamp
        entry["updated_at"] = stamp
        book[symbol] = entry
        action = _action(prev, entry)
        if action:
            changes.append(_change(symbol, action, prev, entry, ev, stamp))
    # Keep the strongest views if the book grows past its limit.
    if len(book) > settings.max_open_views:
        keep = sorted(book, key=lambda s: abs(book[s]["score"]) + abs(book[s]["vol_score"]), reverse=True)
        for symbol in keep[settings.max_open_views:]:
            changes.append(_change(symbol, "close", book.pop(symbol), None, evidence.get(symbol, Evidence()), stamp))
    views = {symbol: View.from_dict(entry) for symbol, entry in book.items()}
    for symbol in core:
        if symbol not in views:
            ev = evidence.get(symbol, Evidence())
            views[symbol] = View.from_dict(
                _entry(symbol, "neutral", "neutral", ev, next_earnings.get(symbol), settings, today)
            )
    return LedgerResult(views=views, book=book, changes=changes)


def _direction(previous: str, score: float, settings: ViewsSettings) -> str:
    # Schmitt trigger: open/reverse at the open threshold, close only below the close threshold.
    if score >= settings.open_threshold:
        return "bullish"
    if score <= -settings.open_threshold:
        return "bearish"
    if previous == "bullish" and score >= settings.close_threshold:
        return "bullish"
    if previous == "bearish" and score <= -settings.close_threshold:
        return "bearish"
    return "neutral"


def _volatility(previous: str, vol_score: float, settings: ViewsSettings) -> str:
    if vol_score >= settings.vol_open_threshold:
        return "expand"
    if vol_score <= -settings.vol_open_threshold:
        return "contract"
    if previous == "expand" and vol_score >= settings.vol_close_threshold:
        return "expand"
    if previous == "contract" and vol_score <= -settings.vol_close_threshold:
        return "contract"
    return "neutral"


def _entry(
    symbol: str,
    direction: str,
    volatility: str,
    ev: Evidence,
    earnings: EarningsEvent | None,
    settings: ViewsSettings,
    today: date,
    previous_conviction: int | None = None,
    max_drift_sigma: float = 1.0,
) -> dict[str, Any]:
    magnitude = abs(ev.score) if direction != "neutral" else 0.0
    conviction = 1 if direction == "neutral" else _conviction(magnitude, settings, previous_conviction)
    horizon = ev.horizon_days
    catalyst, catalyst_date = "", None
    if earnings and (earnings.report_date - today).days <= max(horizon, 7) + 14:
        hour = {"bmo": "盘前", "amc": "盘后", "dmh": "盘中"}.get(earnings.hour, "")
        catalyst, catalyst_date = f"财报 {earnings.report_date:%m/%d}{hour}", earnings.report_date
    top = ev.top()
    return {
        "symbol": symbol,
        "direction": direction,
        "conviction": conviction,
        "expected_move_pct": 0.0,
        "horizon_days": min(120, max(1, horizon)),
        "volatility": volatility,
        "catalyst": catalyst,
        "catalyst_date": catalyst_date.isoformat() if catalyst_date else None,
        "rationale": _rationale(ev, top),
        "news_ids": [c.news_id for c in top],
        "source": "ledger",
        "score": round(ev.score, 3),
        "vol_score": round(ev.vol_score, 3),
        # Stored already capped, so the ledger shows the move the pricing actually uses.
        "drift_sigma": round(min(max_drift_sigma, settings.sigma_per_point * magnitude), 4) if magnitude else None,
        "contributors": [
            {"news_id": c.news_id, "value": c.value, "vol": c.vol, "summary": c.summary,
             "published_at": c.published_at}
            for c in top
        ],
    }


def _conviction(magnitude: float, settings: ViewsSettings, previous: int | None) -> int:
    """1 + |score| / open_threshold, floored to 1-5, with a quarter-level dead band so a
    score sitting on a boundary (or decaying by a hair) does not flip the stars."""

    level = 1 + magnitude / settings.open_threshold
    plain = max(1, min(5, int(level)))
    if previous is not None and previous - 0.25 <= level < previous + 1.25:
        return previous
    return plain


def _rationale(ev: Evidence, top: Sequence[Contribution]) -> str:
    if not top:
        return "近期没有相关新闻证据"
    parts = []
    for index, c in enumerate(top[:3], start=1):
        signed = f"{c.value:+.1f}" if abs(c.value) >= 0.05 else f"波动{c.vol:+.1f}"
        parts.append(f"{'①②③'[index - 1]}{c.summary}（{signed}）")
    return f"证据分 {ev.score:+.1f}，波动分 {ev.vol_score:+.1f}：" + " ".join(parts)


def _action(prev: Mapping[str, Any] | None, entry: Mapping[str, Any]) -> str | None:
    if prev is None:
        return "open"
    if prev.get("direction") != entry["direction"]:
        if "neutral" in (prev.get("direction"), entry["direction"]):
            return "open" if prev.get("direction") == "neutral" else "close_direction"
        return "reverse"
    if entry["conviction"] > int(prev.get("conviction") or 1):
        return "strengthen"
    if entry["conviction"] < int(prev.get("conviction") or 1):
        return "weaken"
    if prev.get("volatility") != entry["volatility"]:
        return "volatility"
    return None


def _change(
    symbol: str,
    action: str,
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any] | None,
    ev: Evidence,
    stamp: str,
) -> dict[str, Any]:
    def brief(entry: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not entry:
            return None
        return {key: entry.get(key) for key in ("direction", "conviction", "volatility", "score", "vol_score")}

    return {
        "symbol": symbol,
        "action": action,
        "at": stamp,
        "before": brief(before),
        "after": brief(after),
        "score": round(ev.score, 3),
        "reasons": [
            {"news_id": c.news_id, "value": c.value, "vol": c.vol, "summary": c.summary}
            for c in ev.top(3)
        ],
    }


ACTION_NAMES = {
    "open": "建立",
    "reverse": "反转",
    "close_direction": "转为中性",
    "close": "结束",
    "strengthen": "加强",
    "weaken": "减弱",
    "volatility": "波动判断变化",
}


__all__ = [
    "ACTION_NAMES",
    "Contribution",
    "Evidence",
    "LedgerResult",
    "score_news",
    "update_ledger",
]
