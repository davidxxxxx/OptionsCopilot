"""Additive deterministic event-intelligence projections for display and research."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import re
from typing import Any

from options_copilot.storage.canonical import canonical_hash


EVENT_INTELLIGENCE_SCHEMA = "options_copilot.event_intelligence.v1"


class IntelligenceCategory(str, Enum):
    MACRO = "MACRO"
    SECTOR = "SECTOR"
    COMPANY = "COMPANY"
    EARNINGS = "EARNINGS"
    REGULATORY = "REGULATORY"
    GEOPOLITICAL = "GEOPOLITICAL"
    MARKET_STRUCTURE = "MARKET_STRUCTURE"
    UNKNOWN = "UNKNOWN"


_PRECEDENCE = (
    IntelligenceCategory.EARNINGS,
    IntelligenceCategory.REGULATORY,
    IntelligenceCategory.MACRO,
    IntelligenceCategory.GEOPOLITICAL,
    IntelligenceCategory.MARKET_STRUCTURE,
    IntelligenceCategory.SECTOR,
    IntelligenceCategory.COMPANY,
    IntelligenceCategory.UNKNOWN,
)
_LEGACY_CATEGORY_MAP = {
    "EARNINGS": IntelligenceCategory.EARNINGS,
    "GUIDANCE": IntelligenceCategory.EARNINGS,
    "REGULATORY": IntelligenceCategory.REGULATORY,
    "FOMC": IntelligenceCategory.MACRO,
    "MACRO": IntelligenceCategory.MACRO,
    "M_AND_A": IntelligenceCategory.COMPANY,
    "PRODUCT": IntelligenceCategory.COMPANY,
    "ANALYST": IntelligenceCategory.COMPANY,
}
_KEYWORDS = (
    (IntelligenceCategory.EARNINGS, ("earnings", "eps", "quarterly results")),
    (IntelligenceCategory.REGULATORY, ("regulator", "regulatory", " sec ", "doj")),
    (IntelligenceCategory.MACRO, ("fomc", "inflation", " cpi ", " gdp ", "payroll")),
    (IntelligenceCategory.GEOPOLITICAL, ("sanction", "war ", "ceasefire", "geopolit")),
    (IntelligenceCategory.MARKET_STRUCTURE, ("exchange halt", "market structure", "clearing", "settlement")),
    (IntelligenceCategory.SECTOR, ("sector", "industry-wide")),
    (IntelligenceCategory.COMPANY, ("merger", "acquisition", "product launch", "price target")),
)


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object, maximum: int = 160) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.strip().split())
    if not normalized:
        return None
    return normalized[:maximum]


def _digest(value: object) -> str | None:
    text = _text(value, 64)
    if text is None:
        return None
    lowered = text.lower()
    return lowered if re.fullmatch(r"[0-9a-f]{64}", lowered) else None


def _choice(value: object, allowed: set[str], reason: str) -> dict[str, object]:
    normalized = (_text(value, 40) or "").upper()
    if normalized in allowed:
        return {"value": normalized, "reason": "DETERMINISTIC_CLASSIFICATION"}
    return {"value": None, "reason": reason}


def _facets(row: Mapping[str, object]) -> tuple[IntelligenceCategory, ...]:
    classification = _mapping(row.get("classification"))
    structured = (
        _text(classification.get("category"), 48)
        or _text(row.get("category"), 48)
        or ""
    ).upper()
    facets: set[IntelligenceCategory] = set()
    raw_facets = classification.get("facets", row.get("facets"))
    if isinstance(raw_facets, Sequence) and not isinstance(
        raw_facets, (str, bytes, bytearray)
    ):
        for value in raw_facets:
            candidate = (_text(value, 48) or "").upper()
            mapped_facet = _LEGACY_CATEGORY_MAP.get(candidate)
            if mapped_facet is not None:
                facets.add(mapped_facet)
            elif candidate in IntelligenceCategory._value2member_map_:
                facets.add(IntelligenceCategory(candidate))
    mapped = _LEGACY_CATEGORY_MAP.get(structured)
    if mapped is not None:
        facets.add(mapped)
    elif (
        structured in IntelligenceCategory._value2member_map_
        and structured != IntelligenceCategory.UNKNOWN.value
    ):
        facets.add(IntelligenceCategory(structured))
    if facets:
        if len(facets) > 1:
            facets.discard(IntelligenceCategory.UNKNOWN)
    else:
        corpus = f" {_text(row.get('title'), 280) or ''} {_text(row.get('summary'), 1200) or ''} ".lower()
        for category, keywords in _KEYWORDS:
            if any(keyword in corpus for keyword in keywords):
                facets.add(category)
    if not facets:
        facets.add(IntelligenceCategory.UNKNOWN)
    return tuple(category for category in _PRECEDENCE if category in facets)


def _has_structured_category(row: Mapping[str, object]) -> bool:
    classification = _mapping(row.get("classification"))
    structured = (
        _text(classification.get("category"), 48)
        or _text(row.get("category"), 48)
        or ""
    ).upper()
    if structured in _LEGACY_CATEGORY_MAP:
        return True
    if (
        structured in IntelligenceCategory._value2member_map_
        and structured != IntelligenceCategory.UNKNOWN.value
    ):
        return True
    raw_facets = classification.get("facets", row.get("facets"))
    if not isinstance(raw_facets, Sequence) or isinstance(
        raw_facets, (str, bytes, bytearray)
    ):
        return False
    return any(
        (_text(value, 48) or "").upper() in _LEGACY_CATEGORY_MAP
        or (
            (_text(value, 48) or "").upper()
            in IntelligenceCategory._value2member_map_
            and (_text(value, 48) or "").upper()
            != IntelligenceCategory.UNKNOWN.value
        )
        for value in raw_facets
    )


def _timing(row: Mapping[str, object]) -> dict[str, object]:
    times = _mapping(row.get("times"))
    reaction = _mapping(row.get("reaction"))
    release = _mapping(reaction.get("release"))
    expected = (
        _text(row.get("scheduled_at"), 64)
        or _text(row.get("event_at"), 64)
        or _text(times.get("event_at"), 64)
    )
    actual = (
        _text(release.get("released_at"), 64)
        or _text(row.get("published_at"), 64)
        or _text(times.get("published_at"), 64)
    )
    precision = (_text(row.get("schedule_precision"), 24) or "UNKNOWN").upper()
    return {
        "expected": {
            "value": expected,
            "precision": precision if expected is not None else "UNAVAILABLE",
            "reason": "SCHEDULED_SOURCE_TIME" if expected is not None else "EXPECTED_TIME_UNAVAILABLE",
        },
        "actual": {
            "value": actual,
            "precision": "SOURCE_TIMESTAMP" if actual is not None else "UNAVAILABLE",
            "reason": "SOURCE_OBSERVED_TIME" if actual is not None else "ACTUAL_TIME_UNAVAILABLE",
        },
    }


def _affected_assets(row: Mapping[str, object]) -> dict[str, object]:
    symbols = row.get("symbols")
    values = []
    if isinstance(symbols, Sequence) and not isinstance(symbols, (str, bytes, bytearray)):
        for value in symbols:
            symbol = (_text(value, 15) or "").upper()
            if re.fullmatch(r"[A-Z][A-Z0-9.\-/]{0,14}", symbol) and symbol not in values:
                values.append(symbol)
    binding = _mapping(row.get("symbol_binding"))
    status = (_text(binding.get("status"), 48) or "UNAVAILABLE").upper()
    verified = status.startswith("VERIFIED") or status in {
        "PROVIDER_VERIFIED",
        "IBKR_VERIFIED",
    }
    if values and verified:
        return {"values": values, "binding": "VERIFIED", "reason": "EXISTING_VERIFIED_SYMBOL_BINDING"}
    return {
        "values": [],
        "binding": status,
        "reason": "SYMBOL_BINDING_UNVERIFIED" if values else "AFFECTED_ASSETS_UNAVAILABLE",
    }


def project_event_intelligence(row: Mapping[str, object]) -> dict[str, Any]:
    """Build a deterministic, authority-free projection without changing v1 storage."""

    facets = _facets(row)
    classification = _mapping(row.get("classification"))
    reaction = _mapping(row.get("reaction"))
    scores = _mapping(row.get("scores"))
    surprise = _mapping(reaction.get("surprise"))
    reaction_status = (_text(reaction.get("status"), 24) or "UNAVAILABLE").upper()
    event_hash = _digest(reaction.get("event_hash")) or _digest(row.get("record_hash"))
    reaction_hash = (
        _digest(reaction.get("record_hash"))
        or _digest(reaction.get("reaction_hash"))
        or _digest(reaction.get("head_hash"))
    )
    reaction_ready = (
        reaction_status == "READY"
        and event_hash is not None
        and reaction_hash is not None
    )
    reaction_reason = (
        "HASH_BOUND_REACTION"
        if reaction_ready
        else "REACTION_CONFLICTED"
        if reaction_status == "CONFLICTED"
        else "REACTION_CHAIN_HASH_UNAVAILABLE"
        if reaction_status == "READY" and reaction_hash is None
        else "REACTION_EVENT_HASH_UNAVAILABLE"
        if reaction_status == "READY"
        else "REACTION_UNAVAILABLE"
    )
    surprise_hash = _digest(surprise.get("content_hash"))
    release_hash = _digest(surprise.get("release_hash"))
    surprise_bound = (
        reaction_ready
        and event_hash is not None
        and surprise.get("delta") is not None
        and surprise_hash is not None
        and release_hash is not None
    )
    payload: dict[str, Any] = {
        "schema": EVENT_INTELLIGENCE_SCHEMA,
        "primary_category": facets[0].value,
        "category_reason": (
            "STRUCTURED_CATEGORY_PRECEDENCE"
            if _has_structured_category(row)
            else "DETERMINISTIC_TEXT_FALLBACK"
            if facets[0] is not IntelligenceCategory.UNKNOWN
            else "CATEGORY_UNAVAILABLE"
        ),
        "facets": [item.value for item in facets],
        "direction": _choice(
            classification.get("direction", row.get("direction")),
            {"BULLISH", "BEARISH", "MIXED", "NEUTRAL", "UNKNOWN"},
            "DIRECTION_UNAVAILABLE",
        ),
        "horizon": _choice(
            classification.get("horizon", row.get("horizon")),
            {"INTRADAY", "DAYS_1_3", "DAYS_4_10", "WEEKS_2_4"},
            "HORIZON_UNAVAILABLE",
        ),
        "confidence": {
            "value": row.get("confidence", classification.get("confidence")),
            "reason": "DETERMINISTIC_CLASSIFICATION" if row.get("confidence", classification.get("confidence")) is not None else "CONFIDENCE_UNAVAILABLE",
        },
        "impact": {
            "value": row.get("event_impact_score", scores.get("event_impact_score")),
            "reason": "DETERMINISTIC_EVENT_IMPACT" if row.get("event_impact_score", scores.get("event_impact_score")) is not None else "IMPACT_UNAVAILABLE",
        },
        "timing": _timing(row),
        "surprise": {
            "value": surprise.get("delta"),
            "reason": "HASH_BOUND_REACTION" if surprise_bound else "SURPRISE_UNAVAILABLE",
            "event_hash": event_hash,
            "reaction_hash": reaction_hash,
            "surprise_hash": surprise_hash,
            "release_hash": release_hash,
        },
        "reaction": {
            "status": reaction_status,
            "event_hash": event_hash,
            "reaction_hash": reaction_hash,
            "reason": reaction_reason,
        },
        "affected_assets": _affected_assets(row),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "risk_effect": "NONE",
        "eligibility_effect": "NONE",
        "action_effect": "NONE",
    }
    payload["intelligence_hash"] = canonical_hash(payload)
    return payload


__all__ = [
    "EVENT_INTELLIGENCE_SCHEMA",
    "IntelligenceCategory",
    "project_event_intelligence",
]
