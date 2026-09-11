"""Deterministic, JSON-safe hashing helpers used by Options Copilot stores."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
import math
from types import MappingProxyType
_DECIMAL_TAG = "$decimal"
_DATE_TAG = "$date"


def canonical_json(value: object) -> str:
    """Return a stable JSON representation or fail closed on unsafe values.

    Only JSON data plus timezone-aware datetimes, enums, and tuples are
    accepted.  In particular, NaN and infinity are rejected because they do
    not have portable JSON semantics and would make hashes ambiguous.
    """

    normalized = _normalize(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def utc_datetime(value: datetime, *, field: str = "timestamp") -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def datetime_text(value: datetime) -> str:
    return utc_datetime(value).isoformat(timespec="microseconds")


def freeze_json(value: object) -> object:
    """Return a recursively immutable copy of already JSON-safe data."""

    normalized = _normalize(value)
    return _freeze_normalized(normalized)


def thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        if set(value) == {_DECIMAL_TAG}:
            return Decimal(str(value[_DECIMAL_TAG]))
        if set(value) == {_DATE_TAG}:
            return date.fromisoformat(str(value[_DATE_TAG]))
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def _normalize(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON cannot contain non-finite numbers")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical JSON cannot contain non-finite decimals")
        normalized = value.normalize()
        rendered = "0" if not normalized else format(normalized, "f")
        return {_DECIMAL_TAG: rendered}
    if isinstance(value, datetime):
        return datetime_text(value)
    if isinstance(value, date):
        return {_DATE_TAG: value.isoformat()}
    if isinstance(value, Enum):
        return _normalize(value.value)
    if isinstance(value, Mapping):
        if set(value) == {_DECIMAL_TAG}:
            decimal = Decimal(str(value[_DECIMAL_TAG]))
            if not decimal.is_finite():
                raise ValueError("canonical JSON cannot contain non-finite decimals")
            normalized = decimal.normalize()
            return {_DECIMAL_TAG: "0" if not normalized else format(normalized, "f")}
        if set(value) == {_DATE_TAG}:
            parsed = date.fromisoformat(str(value[_DATE_TAG]))
            return {_DATE_TAG: parsed.isoformat()}
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            normalized[key] = _normalize(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [_normalize(item) for item in value]
    raise TypeError(f"unsupported canonical JSON type: {type(value).__name__}")


def _freeze_normalized(value: object) -> object:
    if isinstance(value, dict):
        if set(value) == {_DECIMAL_TAG}:
            return Decimal(str(value[_DECIMAL_TAG]))
        if set(value) == {_DATE_TAG}:
            return date.fromisoformat(str(value[_DATE_TAG]))
        return MappingProxyType(
            {str(key): _freeze_normalized(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_normalized(item) for item in value)
    return value


__all__ = [
    "canonical_hash",
    "canonical_json",
    "datetime_text",
    "freeze_json",
    "thaw_json",
    "utc_datetime",
]
