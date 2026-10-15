"""Admission, price-limit and live-repricing rules for recommendation quality."""
from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from copilot.analysis.models import View
from copilot.config import OptionsSettings, StrategySettings
from copilot.market.calendar import add_trading_days, is_regular_session, next_session_open
from .builder import Candidate

NEW_YORK = ZoneInfo("America/New_York")


def conservative_expected_return(candidate: Candidate) -> float:
    return min(candidate.view_ev, candidate.edge)


def expected_return_pct(candidate: Candidate) -> float:
    loss = candidate.profile.max_loss
    return conservative_expected_return(candidate) / loss if loss > 0 else float("-inf")


def is_admissible(candidate: Candidate, settings: StrategySettings) -> bool:
    loss = candidate.profile.max_loss
    threshold = settings.min_expected_return_pct * loss
    return loss > 0 and candidate.edge >= threshold and candidate.view_ev >= threshold


def limit_net_price(candidate: Candidate, min_return_pct: float) -> float:
    """Net debit per share at the exact conservative-return admission boundary."""

    current = candidate.profile.net_debit
    expected = conservative_expected_return(candidate)
    loss = candidate.profile.max_loss
    denominator = candidate.multiplier * (1 + min_return_pct)
    return current + (expected - min_return_pct * loss) / denominator


def reprice_metrics(
    *,
    net_price_now: float,
    net_price: float,
    multiplier: int,
    view_ev_now: float,
    edge_now: float,
    max_loss_now: float,
) -> dict[str, float]:
    change = (net_price - net_price_now) * multiplier
    view_ev = view_ev_now - change
    edge = edge_now - change
    max_loss = max_loss_now + change
    expected = min(view_ev, edge)
    return {
        "view_ev": view_ev,
        "edge": edge,
        "max_loss": max_loss,
        "expected_return": expected,
        "expected_return_pct": expected / max_loss if max_loss > 0 else float("-inf"),
    }


def annotate_validity(
    idea: dict[str, Any], *, now: datetime, options: OptionsSettings
) -> None:
    quote_time = _datetime(idea.get("quote_time"))
    if is_regular_session(now) and quote_time is not None:
        valid_until = quote_time + timedelta(minutes=options.quote_validity_minutes)
        idea["opening_recheck"] = False
    else:
        valid_until = next_session_open(now) + timedelta(minutes=15)
        idea["opening_recheck"] = True
    idea["valid_until"] = valid_until.isoformat()


def watch_until(now: datetime, view: View | None) -> date:
    today = now.astimezone(NEW_YORK).date()
    five_sessions = add_trading_days(today, 5)
    if view is None:
        return five_sessions
    horizon_end = today + timedelta(days=view.horizon_days)
    return min(five_sessions, horizon_end)


def price_gap_pct(current: float, limit: float) -> float:
    return abs(current - limit) / max(abs(current), 0.01)


def price_trigger(limit: float, multiplier: int) -> str:
    amount = abs(limit * multiplier)
    if limit >= 0:
        return f"净支出降至 ${amount:.2f}/组或以下"
    return f"净收入升至 ${amount:.2f}/组或以上"


def invalidation_text(view: View | None, expiry: date) -> str:
    parts = ["观点结束或反转"]
    if view and view.catalyst:
        parts.append("催化剂已过")
    parts.append(f"结构于 {expiry.isoformat()} 到期")
    return "；".join(parts)


def _datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.astimezone()


__all__ = [
    "annotate_validity",
    "conservative_expected_return",
    "expected_return_pct",
    "invalidation_text",
    "is_admissible",
    "limit_net_price",
    "price_gap_pct",
    "price_trigger",
    "reprice_metrics",
    "watch_until",
]
