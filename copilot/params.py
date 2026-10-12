"""Deterministic versioning for settings that shape recommendations."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

from copilot.config import Settings

SCORING_REVISION = 1


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
    }


def params_version(settings: Settings) -> str:
    payload = params_payload(settings)
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:10]


__all__ = ["SCORING_REVISION", "params_payload", "params_version"]
