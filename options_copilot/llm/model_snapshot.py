"""Allowlist-only, bounded state exposed to external advisory models."""
from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol


MAX_SNAPSHOT_BYTES = 32_768
MAX_SNAPSHOT_DEPTH = 12
MAX_COLLECTION_ITEMS = 256
MAX_STRING_LENGTH = 1_000
MAX_NEARBY_LEVELS = 8
MAX_SCENARIOS = 8
MAX_TIMEFRAME_SUMMARIES = 12


class ModelSnapshotPrivacyError(ValueError):
    """Raised when model-visible state violates the outbound privacy boundary."""


class _Compact(Protocol):
    def compact(self) -> object: ...


class _Sanitized(Protocol):
    def sanitized(self) -> object: ...


class _AllowlistedSummaries(Protocol):
    def allowlisted_summaries(self) -> object: ...


class JoinedState(Protocol):
    market_quality: _Compact
    position: _Sanitized
    features: _AllowlistedSummaries
    scenarios: _Compact
    horizon: _Compact
    change_conditions: Sequence[str]


class V2JoinedState(Protocol):
    market: object
    position: object
    broker_protection: object
    session: object
    features: object
    levels: object
    scenarios: object
    path: object
    changes: object


_V2_SECTION_NAMES = (
    "market",
    "position",
    "broker_protection",
    "session",
    "features",
    "levels",
    "scenarios",
    "path",
    "changes",
)
_LEVEL_KEYS = ("level_id", "kind", "relation", "distance_ticks")
_SCENARIO_KEYS = ("scenario_id", "direction", "risk_usd", "reward_risk")
_MARKET_KEYS = frozenset(
    {
        "asof_ts",
        "source_ts",
        "symbol",
        "bid",
        "ask",
        "last",
        "executable_exit_price",
        "spread_ticks",
        "source_age_ms",
        "receive_age_ms",
        "local_latency_ms",
        "fresh",
    }
)
_POSITION_KEYS = frozenset(
    {
        "direction",
        "quantity",
        "average_price",
        "mark_price",
        "unrealized_pnl_usd",
        "elapsed_hold_seconds",
        "position_version",
        "reconciled",
        "protected",
    }
)
_BROKER_PROTECTION_KEYS = frozenset(
    {
        "protected",
        "stop_level_id",
        "target_level_id",
        "stop_confirmed",
        "target_confirmed",
        "distance_to_stop_ticks",
        "distance_to_target_ticks",
    }
)
_SESSION_KEYS = frozenset(
    {
        "session_high",
        "session_low",
        "vwap",
        "opening_range_high",
        "opening_range_low",
        "recent_volatility_ticks",
        "volume_regime",
        "one_minute_summary",
        "five_minute_summary",
    }
)
_SUMMARY_KEYS = frozenset(
    {"state", "direction", "range_ticks", "body_ticks", "volume_regime", "closed"}
)
_FEATURE_KEYS = frozenset(
    {
        "price_action_state",
        "trend_state",
        "regime",
        "flow",
        "order_flow_direction",
        "imbalance_state",
        "absorption_state",
        "exhaustion_state",
        "confidence",
        "ready",
    }
)
_PATH_KEYS = frozenset({"mfe_r", "mae_r", "r_multiple", "giveback_bucket"})
_CHANGES_KEYS = frozenset(
    {"previous_advice_id", "trigger_reason", "material_facts", "changed_fields"}
)
_FORBIDDEN_COMPACT_KEYS = {
    "account",
    "accountid",
    "accountnumber",
    "apikey",
    "authorization",
    "basketid",
    "bars",
    "broker",
    "brokerid",
    "brokerorderid",
    "command",
    "commands",
    "commandschema",
    "credential",
    "credentials",
    "filepath",
    "fcmid",
    "functioncall",
    "functions",
    "gateway",
    "mbo",
    "mborows",
    "orderid",
    "password",
    "rawbars",
    "events",
    "eventhistory",
    "jsonl",
    "rawhistory",
    "rawmbo",
    "rawticks",
    "candles",
    "klines",
    "ohlcv",
    "secret",
    "secrets",
    "token",
    "toolchoice",
    "tools",
    "usertag",
}
_FORBIDDEN_KEY_PREFIXES = ("account", "broker", "fcm")
_FORBIDDEN_KEY_SUFFIXES = (
    "accountid",
    "apikey",
    "basketid",
    "credential",
    "fcmid",
    "filepath",
    "orderid",
    "password",
    "secret",
    "token",
)
_SECRET_OR_IDENTIFIER_PATTERNS = (
    re.compile(r"\b(?:ACCOUNT|ORDER|BASKET|FCM)[-_][A-Z0-9_-]{3,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b", re.IGNORECASE),
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/-]{4,}\b", re.IGNORECASE),
    re.compile(
        r"\b(?:api[_ -]?key|password|credential|secret|token)\s*[:=]\s*\S+",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:account|broker|order|basket|fcm)[_ -]?(?:id|number)?\s*[:=]\s*"
        r"[A-Za-z0-9_-]{4,}\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:account(?:[_ -]+(?:id|number))?|broker[_ -]+account)\s*"
        r"(?::|=)?\s*[A-Z]{0,3}\d{5,}\b",
        re.IGNORECASE,
    ),
)
_PATH_PATTERNS = (
    re.compile(r"(?<![A-Za-z0-9])[A-Za-z]:\\[^\s\"']+"),
    re.compile(r"(?<![A-Za-z0-9])/(?:home|users?|var|tmp|opt|etc)/[^\s\"']+"),
    re.compile(
        r"(?<![A-Za-z0-9])(?:[A-Za-z0-9_.-]+[\\/])+[A-Za-z0-9_.-]+"
        r"\.(?:jsonl?|csv|parquet|db|sqlite|pem|key)\b",
        re.IGNORECASE,
    ),
)


def build_model_snapshot(state: JoinedState | V2JoinedState) -> dict[str, object]:
    """Build either the v2 bounded context or the legacy compatibility projection."""
    if all(hasattr(state, name) for name in _V2_SECTION_NAMES):
        snapshot = _build_v2_snapshot(state)
        assert_model_snapshot_safe(snapshot)
        return snapshot

    snapshot: dict[str, object] = {
        "market_quality": state.market_quality.compact(),
        "position": state.position.sanitized(),
        "features": state.features.allowlisted_summaries(),
        "scenarios": state.scenarios.compact(),
        "horizon": state.horizon.compact(),
        "change_conditions": list(state.change_conditions),
    }
    assert_model_snapshot_safe(snapshot)
    return snapshot


def _build_v2_snapshot(state: object) -> dict[str, object]:
    levels = _bounded_records(
        _call_projection(getattr(state, "levels"), "compact", "levels"),
        keys=_LEVEL_KEYS,
        limit=MAX_NEARBY_LEVELS,
        sort_key=_level_sort_key,
        label="levels",
    )
    scenarios = _bounded_records(
        _call_projection(getattr(state, "scenarios"), "compact", "scenarios"),
        keys=_SCENARIO_KEYS,
        limit=MAX_SCENARIOS,
        sort_key=lambda record: (str(record["scenario_id"]),),
        label="scenarios",
    )
    snapshot = {
        "market": _project_mapping(
            getattr(state, "market"),
            method_name="compact",
            allowed_keys=_MARKET_KEYS,
            label="market",
        ),
        "position": _project_mapping(
            getattr(state, "position"),
            method_name="sanitized",
            allowed_keys=_POSITION_KEYS,
            label="position",
        ),
        "broker_protection": _project_mapping(
            getattr(state, "broker_protection"),
            method_name="compact",
            allowed_keys=_BROKER_PROTECTION_KEYS,
            label="broker_protection",
        ),
        "session": _bounded_session(
            _project_mapping(
                getattr(state, "session"),
                method_name="compact",
                allowed_keys=_SESSION_KEYS,
                label="session",
                allow_sequences=True,
            )
        ),
        "features": _project_mapping(
            getattr(state, "features"),
            method_name="allowlisted_summaries",
            allowed_keys=_FEATURE_KEYS,
            label="features",
        ),
        "levels": levels,
        "scenarios": scenarios,
        "path": _project_mapping(
            getattr(state, "path"),
            method_name="compact",
            allowed_keys=_PATH_KEYS,
            label="path",
        ),
        "changes": _bounded_changes(
            _project_mapping(
                getattr(state, "changes"),
                method_name="compact",
                allowed_keys=_CHANGES_KEYS,
                label="changes",
                allow_sequences=True,
            )
        ),
    }
    return snapshot


def _call_projection(value: object, method_name: str, label: str) -> object:
    method = getattr(value, method_name, None)
    if not callable(method):
        raise ModelSnapshotPrivacyError(
            f"model snapshot {label} requires {method_name} projection"
        )
    return method()


def _project_mapping(
    value: object,
    *,
    method_name: str,
    allowed_keys: frozenset[str],
    label: str,
    allow_sequences: bool = False,
) -> dict[str, object]:
    projected = _call_projection(value, method_name, label)
    if not isinstance(projected, Mapping):
        raise ModelSnapshotPrivacyError(f"model snapshot {label} must be a mapping")
    keys = {str(key) for key in projected}
    if keys.difference(allowed_keys):
        raise ModelSnapshotPrivacyError(
            f"model snapshot {label} field is not allowlisted"
        )
    result = {str(key): item for key, item in projected.items()}
    for item in result.values():
        if isinstance(item, Mapping) or (
            isinstance(item, Sequence)
            and not isinstance(item, (str, bytes, bytearray))
            and not allow_sequences
        ):
            raise ModelSnapshotPrivacyError(
                f"model snapshot {label} value violates section schema"
            )
    return result


def _bounded_records(
    value: object,
    *,
    keys: tuple[str, ...],
    limit: int,
    sort_key: Callable[[dict[str, object]], object],
    label: str,
) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ModelSnapshotPrivacyError(f"model snapshot {label} must be a sequence")
    records: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != set(keys):
            raise ModelSnapshotPrivacyError(
                f"model snapshot {label} record field is not allowlisted"
            )
        if any(
            isinstance(record_value, Mapping)
            or (
                isinstance(record_value, Sequence)
                and not isinstance(record_value, (str, bytes, bytearray))
            )
            for record_value in item.values()
        ):
            raise ModelSnapshotPrivacyError(
                f"model snapshot {label} record violates section schema"
            )
        records.append({key: item[key] for key in keys})
    records.sort(key=sort_key)
    return records[:limit]


def _level_sort_key(record: Mapping[str, object]) -> tuple[float, str]:
    distance = record["distance_ticks"]
    if not isinstance(distance, (int, float)) or isinstance(distance, bool):
        raise ModelSnapshotPrivacyError("model snapshot level distance must be numeric")
    return abs(float(distance)), str(record["level_id"])


def _bounded_session(value: object) -> object:
    if not isinstance(value, Mapping):
        return value
    bounded = dict(value)
    for key in ("one_minute_summary", "five_minute_summary"):
        summaries = bounded.get(key)
        if isinstance(summaries, Sequence) and not isinstance(
            summaries,
            (str, bytes, bytearray),
        ):
            projected: list[dict[str, object]] = []
            for summary in summaries[-MAX_TIMEFRAME_SUMMARIES:]:
                if (
                    not isinstance(summary, Mapping)
                    or set(summary).difference(_SUMMARY_KEYS)
                ):
                    raise ModelSnapshotPrivacyError(
                        "model snapshot session summary field is not allowlisted"
                    )
                if any(isinstance(item, (Mapping, list, tuple)) for item in summary.values()):
                    raise ModelSnapshotPrivacyError(
                        "model snapshot session summary violates section schema"
                    )
                projected.append(dict(summary))
            bounded[key] = projected
        elif summaries is not None:
            raise ModelSnapshotPrivacyError(
                "model snapshot session summary must be a sequence"
            )
    for key, item in bounded.items():
        if key not in {"one_minute_summary", "five_minute_summary"} and isinstance(
            item, (Mapping, list, tuple)
        ):
            raise ModelSnapshotPrivacyError(
                "model snapshot session value violates section schema"
            )
    return bounded


def _bounded_changes(value: Mapping[str, object]) -> dict[str, object]:
    bounded = dict(value)
    for key in ("material_facts", "changed_fields"):
        items = bounded.get(key)
        if items is None:
            continue
        if not isinstance(items, Sequence) or isinstance(
            items,
            (str, bytes, bytearray),
        ):
            raise ModelSnapshotPrivacyError(
                "model snapshot changes value violates section schema"
            )
        if len(items) > MAX_COLLECTION_ITEMS or not all(
            isinstance(item, str) for item in items
        ):
            raise ModelSnapshotPrivacyError(
                "model snapshot changes value violates section schema"
            )
        bounded[key] = list(items)
    for key in ("previous_advice_id", "trigger_reason"):
        item = bounded.get(key)
        if isinstance(item, (Mapping, list, tuple)):
            raise ModelSnapshotPrivacyError(
                "model snapshot changes value violates section schema"
            )
    return bounded


def assert_model_snapshot_safe(value: object) -> None:
    """Recursively reject forbidden model data without exposing it in errors."""
    _assert_safe(value, path="$", depth=0)
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ModelSnapshotPrivacyError("model snapshot is not strict JSON") from exc
    if len(encoded) > MAX_SNAPSHOT_BYTES:
        raise ModelSnapshotPrivacyError("model snapshot exceeds byte limit")


def _assert_safe(value: object, *, path: str, depth: int) -> None:
    if depth > MAX_SNAPSHOT_DEPTH:
        raise ModelSnapshotPrivacyError(f"model snapshot exceeds depth limit at {path}")
    if isinstance(value, Mapping):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ModelSnapshotPrivacyError(f"model snapshot mapping is too large at {path}")
        _assert_not_tool_schema(value, path)
        for raw_key, item in value.items():
            key = str(raw_key)
            compact = re.sub(r"[^a-z0-9]", "", key.lower())
            required_v2_section = path == "$" and compact == "brokerprotection"
            if (
                not required_v2_section
                and (
                    compact in _FORBIDDEN_COMPACT_KEYS
                    or compact.startswith(_FORBIDDEN_KEY_PREFIXES)
                    or compact.endswith(_FORBIDDEN_KEY_SUFFIXES)
                )
            ):
                raise ModelSnapshotPrivacyError(f"forbidden model key at {path}")
            _assert_safe(item, path=f"{path}.{key}", depth=depth + 1)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise ModelSnapshotPrivacyError(f"model snapshot sequence is too large at {path}")
        for index, item in enumerate(value):
            _assert_safe(item, path=f"{path}[{index}]", depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            raise ModelSnapshotPrivacyError(f"model snapshot string is too long at {path}")
        if any(pattern.search(value) for pattern in _SECRET_OR_IDENTIFIER_PATTERNS):
            raise ModelSnapshotPrivacyError(f"forbidden model value at {path}")
        if any(pattern.search(value) for pattern in _PATH_PATTERNS):
            raise ModelSnapshotPrivacyError(f"filesystem path forbidden at {path}")
        return
    if value is None or isinstance(value, (bool, int, float)):
        return
    raise ModelSnapshotPrivacyError(f"unsupported model value type at {path}")


def _assert_not_tool_schema(value: Mapping[object, object], path: str) -> None:
    normalized = {str(key).strip().lower() for key in value}
    if "function" in normalized or "tool" in normalized:
        raise ModelSnapshotPrivacyError(f"tool schema forbidden at {path}")
    kind = value.get("type")
    if isinstance(kind, str) and kind.strip().lower() in {"function", "tool"}:
        raise ModelSnapshotPrivacyError(f"tool schema forbidden at {path}")
    if {"name", "parameters"}.issubset(normalized):
        raise ModelSnapshotPrivacyError(f"command schema forbidden at {path}")
