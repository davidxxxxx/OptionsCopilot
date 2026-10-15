"""Fail-closed identity checks for durable news shadow predictions."""
from __future__ import annotations

from types import MappingProxyType
from typing import Mapping
from datetime import datetime

from options_copilot.storage.canonical import canonical_hash, utc_datetime
from .macro_proxy import require_current_market_proxy_binding


LEGACY_NEWS_SHADOW_PREDICTION_SCHEMA = (
    "options_copilot.news_shadow_prediction.v1"
)
NEWS_SHADOW_PREDICTION_SCHEMA = "options_copilot.news_shadow_prediction.v2"
NEWS_SHADOW_PREDICTION_ID_VERSION = "v2"
NEWS_SHADOW_PREDICTION_TARGETS: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "30M": ("PREDICTED_AT_PLUS_30_MINUTES", "30m"),
        "SESSION_CLOSE": ("NEXT_ELIGIBLE_SESSION_CLOSE", "session-close"),
        "1D": ("SESSION_CLOSE_PLUS_1_TRADING_DAY", "1d"),
        "3D": ("SESSION_CLOSE_PLUS_3_TRADING_DAYS", "3d"),
        "5D": ("SESSION_CLOSE_PLUS_5_TRADING_DAYS", "5d"),
    }
)
NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG: Mapping[str, tuple[str, str]] = (
    MappingProxyType(
        {
            slug: (horizon, target_rule)
            for horizon, (target_rule, slug) in NEWS_SHADOW_PREDICTION_TARGETS.items()
        }
    )
)


def projectable_shadow_prediction_identity(
    body: object,
    *,
    prediction_id: object,
    independence_key: object,
    predicted_at: object = None,
) -> tuple[str, str] | None:
    """Return ``(horizon, symbol)`` only for an outcome-safe prediction.

    Non-news prediction schemas retain the established generic outcome contract.
    The production news schema is stricter because it crosses the boundary from
    model research into durable outcome measurement.
    """

    if not isinstance(body, Mapping):
        return None
    exclusion = shadow_prediction_exclusion_reason(
        body,
        prediction_id=prediction_id,
        independence_key=independence_key,
        predicted_at=predicted_at,
    )
    if exclusion not in {None, "NOT_NEWS_SHADOW"}:
        return None
    horizon = str(body.get("horizon") or "").strip().upper()
    symbol = str(body.get("symbol") or "").strip().upper()
    target = NEWS_SHADOW_PREDICTION_TARGETS.get(horizon)
    if target is None or body.get("target_rule") != target[0] or not symbol:
        return None
    if exclusion == "NOT_NEWS_SHADOW":
        return horizon, symbol

    return horizon, symbol


def news_shadow_prediction_id(advisory_id: str, slug: str) -> str:
    """Return the append-only v2 identity without colliding with v1 rows."""

    checked_advisory_id = str(advisory_id).strip()
    checked_slug = str(slug).strip().lower()
    if not checked_advisory_id or checked_slug not in NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG:
        raise ValueError("news shadow prediction identity is invalid")
    return f"{checked_advisory_id}:{NEWS_SHADOW_PREDICTION_ID_VERSION}:{checked_slug}"


def shadow_prediction_exclusion_reason(
    body: object,
    *,
    prediction_id: object,
    independence_key: object,
    predicted_at: object = None,
) -> str | None:
    """Explain why one news prediction cannot enter outcome measurement."""

    if not isinstance(body, Mapping):
        return "PAYLOAD_INVALID"
    schema = body.get("schema")
    if schema == LEGACY_NEWS_SHADOW_PREDICTION_SCHEMA:
        return "LEGACY_V1_CONTRACT_EXCLUDED"
    if schema != NEWS_SHADOW_PREDICTION_SCHEMA:
        return "NOT_NEWS_SHADOW"
    horizon = str(body.get("horizon") or "").strip().upper()
    symbol = str(body.get("symbol") or "").strip().upper()
    target = NEWS_SHADOW_PREDICTION_TARGETS.get(horizon)
    if target is None:
        return "HORIZON_INVALID"
    if body.get("target_rule") != target[0]:
        return "TARGET_RULE_INVALID"
    if not symbol:
        return "SYMBOL_MISSING"

    checked_prediction_id = str(prediction_id or "").strip()
    checked_independence_key = str(independence_key or "").strip()
    advisory_id = str(body.get("advisory_id") or "").strip()
    event_id = str(body.get("event_id") or "").strip()
    model_hash = str(body.get("model_visible_snapshot_hash") or "").strip().lower()
    classification = body.get("classification")
    try:
        symbol_binding = require_current_market_proxy_binding(
            body.get("symbol_binding"),
            symbol=symbol,
        )
    except (TypeError, ValueError):
        return "SYMBOL_BINDING_INVALID"
    if not advisory_id or not event_id:
        return "EVENT_IDENTITY_MISSING"
    if checked_prediction_id != news_shadow_prediction_id(advisory_id, target[1]):
        return "PREDICTION_ID_INVALID"
    if not checked_independence_key:
        return "INDEPENDENCE_KEY_MISSING"
    if not isinstance(classification, Mapping):
        return "CLASSIFICATION_INVALID"
    if len(model_hash) != 64 or any(
        character not in "0123456789abcdef" for character in model_hash
    ):
        return "MODEL_SNAPSHOT_HASH_INVALID"
    prediction_baseline_hash = str(
        body.get("prediction_baseline_hash") or ""
    ).strip().lower()
    prediction_set_predicted_at = str(
        body.get("prediction_set_predicted_at") or ""
    ).strip()
    if len(prediction_baseline_hash) != 64 or any(
        character not in "0123456789abcdef"
        for character in prediction_baseline_hash
    ):
        return "PREDICTION_BASELINE_HASH_INVALID"
    if not prediction_set_predicted_at:
        return "PREDICTION_SET_TIMESTAMP_MISSING"
    try:
        set_time = utc_datetime(
            datetime.fromisoformat(
                prediction_set_predicted_at.replace("Z", "+00:00")
            ),
            field="prediction_set_predicted_at",
        )
        record_time = utc_datetime(predicted_at, field="predicted_at")
    except (TypeError, ValueError):
        return "PREDICTION_SET_TIMESTAMP_INVALID"
    if set_time != record_time:
        return "PREDICTION_SET_TIMESTAMP_MISMATCH"
    expected_baseline_hash = canonical_hash(
        {
            "schema": "options_copilot.news_prediction_baseline.v2",
            "advisory_id": advisory_id,
            "event_id": event_id,
            "symbol": symbol,
            "model_visible_snapshot_hash": model_hash,
            "prediction_set_predicted_at": set_time,
        }
    )
    if prediction_baseline_hash != expected_baseline_hash:
        return "PREDICTION_BASELINE_HASH_MISMATCH"
    if body.get("decision_authority") != "SUPPORTING_ONLY":
        return "AUTHORITY_INVALID"
    if body.get("approval_eligible") is not False:
        return "APPROVAL_FLAG_INVALID"
    if body.get("instruction_creation_allowed") is not False:
        return "INSTRUCTION_FLAG_INVALID"
    if body.get("order_allowed") is not False:
        return "ORDER_FLAG_INVALID"
    if symbol_binding is not None and symbol_binding.proxy_symbol != symbol:
        return "SYMBOL_BINDING_INVALID"
    return None


__all__ = [
    "LEGACY_NEWS_SHADOW_PREDICTION_SCHEMA",
    "NEWS_SHADOW_PREDICTION_ID_VERSION",
    "NEWS_SHADOW_PREDICTION_SCHEMA",
    "NEWS_SHADOW_PREDICTION_TARGETS",
    "NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG",
    "news_shadow_prediction_id",
    "projectable_shadow_prediction_identity",
    "shadow_prediction_exclusion_reason",
]
