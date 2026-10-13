"""Deterministic versioning for settings that shape recommendations."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

from copilot.config import Settings

SCORING_REVISION = 3


def params_payload(settings: Settings) -> dict[str, Any]:
    return {
        "scoring_revision": SCORING_REVISION,
        "universe": {
            "core": list(settings.core_symbols),
            "watchlist": list(settings.watchlist),
        },
        "options": asdict(settings.options),
        "llm_model": settings.llm.model,
        "views": asdict(settings.views),
        "strategy": asdict(settings.strategy),
        "rules": asdict(settings.rules),
        "calendar": {"catalyst_symbols": list(settings.calendar.catalyst_symbols)},
        "events": {
            "study_symbols": list(settings.events.study_symbols),
            "min_events": settings.events.min_events,
            "min_t": settings.events.min_t,
            "min_half_t": settings.events.min_half_t,
            "shrink_k": settings.events.shrink_k,
            "recency_half_life": settings.events.recency_half_life,
            "min_wf_views": settings.events.min_wf_views,
            "min_wf_hit_rate": settings.events.min_wf_hit_rate,
        },
    }


def params_version(settings: Settings) -> str:
    payload = params_payload(settings)
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:10]


__all__ = ["SCORING_REVISION", "params_payload", "params_version"]
