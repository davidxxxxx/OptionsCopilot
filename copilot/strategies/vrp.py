"""Pure gates for the separate systematic index variance-risk-premium rule."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

from copilot.config import VrpSettings
from copilot.strategies.builder import Candidate


def blackout_event(
    events: Sequence[dict[str, Any]], today: date, days: int
) -> dict[str, Any] | None:
    return next((
        event for event in events
        if event.get("category") in {"fomc_decision", "cpi"}
        and today <= date.fromisoformat(event["date"]) <= today + timedelta(days=days)
    ), None)


def entry_block_reason(
    *,
    iv30: float | None,
    hv20: float | None,
    blackout: dict[str, Any] | None,
    settings: VrpSettings,
) -> str | None:
    if blackout is not None:
        return "重大宏观事件静默期"
    if hv20 is None:
        return "HV20 未知"
    if iv30 is None or iv30 - hv20 < settings.min_iv_minus_hv20:
        return "IV−HV20 不足"
    return None


def candidate_block_reason(candidate: Candidate | None, remaining: float | None) -> str | None:
    if candidate is None:
        return "到期日、Delta 或风险预算不满足"
    if candidate.edge <= 0:
        return "期望优势不为正"
    if remaining is not None and candidate.profile.max_loss > remaining:
        return "组合风险余额不足"
    return None


__all__ = ["blackout_event", "candidate_block_reason", "entry_block_reason"]
