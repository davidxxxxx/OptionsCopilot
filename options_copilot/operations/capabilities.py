"""Immutable, fail-closed capability contracts for Options Copilot readiness.

The contracts in this module are data-only.  They deliberately expose no
broker client and no operation that can create, transmit, or cancel an order.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
import math
import re
from typing import Literal, TypeAlias

from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


class CapabilityStatus(str, Enum):
    """Fixed readiness states ordered around a single safe success state."""

    READY_FOR_REVIEW = "READY_FOR_REVIEW"
    MISSING = "MISSING"
    STALE = "STALE"
    DEGRADED = "DEGRADED"
    FORBIDDEN = "FORBIDDEN"


PacingSource: TypeAlias = Literal[
    "broker_disclosed",
    "observed",
    "conservative_default",
]

PACING_REQUEST_CLASSES = (
    "scanner",
    "secdef",
    "snapshot_quote",
    "streaming_quote",
    "historical",
)
PACING_LIMIT_FIELDS = (
    "max_concurrency",
    "request_window",
    "max_requests",
    "cooldown",
)
PACING_SOURCES = frozenset(
    {"broker_disclosed", "observed", "conservative_default"}
)
DEFAULT_PACING_MAX_AGE = timedelta(hours=24)

_PACING_FIELDS = frozenset(
    {
        "version",
        "observed_at",
        "source",
        "request_classes",
        "content_hash",
        "signer",
    }
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class CapabilityRecord:
    """One immutable probe result with deterministic reason codes."""

    name: str
    status: CapabilityStatus
    observed_at: datetime
    reason_codes: tuple[str, ...] = ()
    details: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        name = str(self.name).strip()
        if not name:
            raise ValueError("capability name cannot be blank")
        status = (
            self.status
            if isinstance(self.status, CapabilityStatus)
            else CapabilityStatus(str(self.status))
        )
        observed_at = utc_datetime(self.observed_at, field="observed_at")
        reason_codes = tuple(_reason_code(value) for value in self.reason_codes)
        raw_details: object = {} if self.details is None else self.details
        frozen_details = freeze_json(raw_details)
        if not isinstance(frozen_details, Mapping):
            raise TypeError("capability details must be a mapping")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "reason_codes", reason_codes)
        object.__setattr__(self, "details", frozen_details)

    @property
    def content_hash(self) -> str:
        return canonical_hash(self._hash_payload())

    def _hash_payload(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status.value,
            "observed_at": datetime_text(self.observed_at),
            "reason_codes": list(self.reason_codes),
            "details": thaw_json(self.details),
        }

    def as_dict(self) -> dict[str, object]:
        payload = self._hash_payload()
        payload["content_hash"] = self.content_hash
        return payload


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    """Canonical aggregate that can be READY only when every record is READY."""

    observed_at: datetime
    records: tuple[CapabilityRecord, ...]
    version: str = "options-copilot-readiness.v1"

    def __post_init__(self) -> None:
        observed_at = utc_datetime(self.observed_at, field="observed_at")
        version = str(self.version).strip()
        if not version:
            raise ValueError("readiness report version cannot be blank")
        records = tuple(self.records)
        if any(not isinstance(record, CapabilityRecord) for record in records):
            raise TypeError("readiness report records must be CapabilityRecord values")
        names = [record.name for record in records]
        if len(names) != len(set(names)):
            raise ValueError("readiness report capability names must be unique")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "records", records)
        object.__setattr__(self, "version", version)

    @property
    def status(self) -> CapabilityStatus:
        statuses = {record.status for record in self.records}
        if self.records and statuses == {CapabilityStatus.READY_FOR_REVIEW}:
            return CapabilityStatus.READY_FOR_REVIEW
        for status in (
            CapabilityStatus.FORBIDDEN,
            CapabilityStatus.MISSING,
            CapabilityStatus.STALE,
            CapabilityStatus.DEGRADED,
        ):
            if status in statuses:
                return status
        return CapabilityStatus.MISSING

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return _dedupe(
            code
            for record in self.records
            if record.status is not CapabilityStatus.READY_FOR_REVIEW
            for code in record.reason_codes
        )

    @property
    def content_hash(self) -> str:
        return canonical_hash(self._hash_payload())

    def _hash_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "observed_at": datetime_text(self.observed_at),
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "records": [record.as_dict() for record in self.records],
            "review_only": True,
            "direct_order_submission": False,
        }

    def as_dict(self) -> dict[str, object]:
        payload = self._hash_payload()
        payload["content_hash"] = self.content_hash
        return payload


@dataclass(frozen=True, slots=True)
class MarketDataPacingCapability:
    """Versioned budgets for the five distinct IBKR market-data request classes."""

    version: str
    observed_at: datetime
    source: PacingSource | str
    request_classes: Mapping[str, Mapping[str, int | float]]
    content_hash: str
    signer: str | None = None

    def __post_init__(self) -> None:
        version = str(self.version).strip()
        source = str(self.source).strip()
        observed_at = utc_datetime(self.observed_at, field="observed_at")
        signer = None if self.signer is None else str(self.signer).strip() or None
        frozen_classes = freeze_json(self.request_classes)
        if not isinstance(frozen_classes, Mapping):
            raise TypeError("request_classes must be a mapping")
        content_hash = str(self.content_hash).strip().lower()
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "request_classes", frozen_classes)
        object.__setattr__(self, "content_hash", content_hash)
        object.__setattr__(self, "signer", signer)

    @classmethod
    def create(
        cls,
        *,
        version: str,
        observed_at: datetime,
        source: PacingSource | str,
        request_classes: Mapping[str, Mapping[str, int | float]],
        signer: str | None,
    ) -> "MarketDataPacingCapability":
        normalized_at = utc_datetime(observed_at, field="observed_at")
        payload = {
            "version": str(version).strip(),
            "observed_at": datetime_text(normalized_at),
            "source": str(source).strip(),
            "request_classes": _detached_request_classes(request_classes),
            "signer": None if signer is None else str(signer).strip() or None,
        }
        return cls(
            version=str(payload["version"]),
            observed_at=normalized_at,
            source=str(payload["source"]),
            request_classes=payload["request_classes"],  # type: ignore[arg-type]
            content_hash=canonical_hash(payload),
            signer=payload["signer"],  # type: ignore[arg-type]
        )

    @classmethod
    def inspect(
        cls,
        payload: object,
        *,
        now: datetime,
        max_age: timedelta = DEFAULT_PACING_MAX_AGE,
    ) -> CapabilityRecord:
        inspected_at = utc_datetime(now, field="now")
        if not isinstance(payload, Mapping):
            return _pacing_record(
                status=CapabilityStatus.MISSING,
                observed_at=inspected_at,
                reason_codes=("PACING_CAPABILITY_MISSING",),
            )

        keys = {str(key) for key in payload}
        unknown_fields = sorted(keys - _PACING_FIELDS)
        if unknown_fields:
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=_safe_observed_at(payload.get("observed_at"), inspected_at),
                reason_codes=(
                    "PACING_CAPABILITY_UNKNOWN_FIELD",
                    "PACING_CAPABILITY_MISSING",
                ),
                details={"unknown_fields": unknown_fields},
            )

        missing_fields = sorted(_PACING_FIELDS - keys)
        if missing_fields:
            return _pacing_record(
                status=CapabilityStatus.MISSING,
                observed_at=_safe_observed_at(payload.get("observed_at"), inspected_at),
                reason_codes=(
                    "PACING_FIELD_MISSING",
                    "PACING_CAPABILITY_MISSING",
                ),
                details={"missing_fields": missing_fields},
            )

        structural = _inspect_request_classes(payload.get("request_classes"))
        if structural is not None:
            status, codes, details = structural
            return _pacing_record(
                status=status,
                observed_at=_safe_observed_at(payload.get("observed_at"), inspected_at),
                reason_codes=(*codes, "PACING_CAPABILITY_MISSING"),
                details=details,
            )

        version = payload.get("version")
        if not isinstance(version, str) or not version.strip():
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=_safe_observed_at(payload.get("observed_at"), inspected_at),
                reason_codes=(
                    "PACING_VERSION_INVALID",
                    "PACING_CAPABILITY_MISSING",
                ),
            )

        source = payload.get("source")
        if source not in PACING_SOURCES:
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=_safe_observed_at(payload.get("observed_at"), inspected_at),
                reason_codes=(
                    "PACING_SOURCE_FORBIDDEN",
                    "PACING_CAPABILITY_MISSING",
                ),
            )

        try:
            observed_at = _parse_datetime(payload.get("observed_at"), field="observed_at")
        except (TypeError, ValueError):
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=inspected_at,
                reason_codes=(
                    "PACING_OBSERVATION_INVALID",
                    "PACING_CAPABILITY_MISSING",
                ),
            )

        signer = payload.get("signer")
        if signer is not None and (not isinstance(signer, str) or not signer.strip()):
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=observed_at,
                reason_codes=(
                    "PACING_SIGNER_INVALID",
                    "PACING_CAPABILITY_MISSING",
                ),
            )
        if source == "conservative_default" and not (
            isinstance(signer, str) and signer.strip()
        ):
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=observed_at,
                reason_codes=(
                    "PACING_CONSERVATIVE_DEFAULT_UNSIGNED",
                    "PACING_CAPABILITY_MISSING",
                ),
                details={"source": source, "version": version},
            )

        if not isinstance(max_age, timedelta) or max_age <= timedelta(0):
            raise ValueError("max_age must be a positive timedelta")
        if observed_at > inspected_at:
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=observed_at,
                reason_codes=(
                    "PACING_OBSERVATION_FUTURE",
                    "PACING_CAPABILITY_MISSING",
                ),
            )
        if inspected_at - observed_at > max_age:
            return _pacing_record(
                status=CapabilityStatus.STALE,
                observed_at=observed_at,
                reason_codes=(
                    "PACING_OBSERVATION_STALE",
                    "PACING_CAPABILITY_MISSING",
                ),
                details={
                    "source": source,
                    "version": version,
                    "max_age_seconds": max_age.total_seconds(),
                },
            )

        supplied_hash = payload.get("content_hash")
        immutable_payload = {
            "version": version.strip(),
            "observed_at": datetime_text(observed_at),
            "source": source,
            "request_classes": _detached_request_classes(
                payload["request_classes"]  # type: ignore[arg-type]
            ),
            "signer": None if signer is None else signer.strip(),
        }
        expected_hash = canonical_hash(immutable_payload)
        if (
            not isinstance(supplied_hash, str)
            or not _HASH_RE.fullmatch(supplied_hash)
            or supplied_hash != expected_hash
        ):
            return _pacing_record(
                status=CapabilityStatus.FORBIDDEN,
                observed_at=observed_at,
                reason_codes=(
                    "PACING_CONTENT_HASH_MISMATCH",
                    "PACING_CAPABILITY_MISSING",
                ),
                details={"source": source, "version": version},
            )

        return _pacing_record(
            status=CapabilityStatus.READY_FOR_REVIEW,
            observed_at=observed_at,
            reason_codes=(),
            details={
                "version": version,
                "source": source,
                "request_classes": immutable_payload["request_classes"],
                "content_hash": expected_hash,
                "signer": immutable_payload["signer"],
            },
        )

    def validate(
        self,
        *,
        now: datetime,
        max_age: timedelta = DEFAULT_PACING_MAX_AGE,
    ) -> CapabilityRecord:
        return self.inspect(self.as_dict(), now=now, max_age=max_age)

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "observed_at": datetime_text(self.observed_at),
            "source": self.source,
            "request_classes": _detached_request_classes(self.request_classes),
            "content_hash": self.content_hash,
            "signer": self.signer,
        }


def _inspect_request_classes(
    value: object,
) -> tuple[CapabilityStatus, tuple[str, ...], dict[str, object]] | None:
    if not isinstance(value, Mapping):
        return (
            CapabilityStatus.MISSING,
            ("PACING_REQUEST_CLASSES_MISSING",),
            {},
        )
    names = {str(key) for key in value}
    missing = [name for name in PACING_REQUEST_CLASSES if name not in names]
    if missing:
        return (
            CapabilityStatus.MISSING,
            ("PACING_REQUEST_CLASS_MISSING",),
            {"missing_request_classes": missing},
        )
    unknown = sorted(names - set(PACING_REQUEST_CLASSES))
    if unknown:
        return (
            CapabilityStatus.FORBIDDEN,
            ("PACING_REQUEST_CLASS_UNKNOWN",),
            {"unknown_request_classes": unknown},
        )

    for class_name in PACING_REQUEST_CLASSES:
        limits = value.get(class_name)
        if not isinstance(limits, Mapping):
            return (
                CapabilityStatus.DEGRADED,
                ("PACING_REQUEST_CLASS_INVALID",),
                {"request_class": class_name},
            )
        fields = {str(key) for key in limits}
        missing_fields = [name for name in PACING_LIMIT_FIELDS if name not in fields]
        if missing_fields:
            return (
                CapabilityStatus.DEGRADED,
                ("PACING_LIMIT_FIELD_MISSING",),
                {
                    "request_class": class_name,
                    "missing_fields": missing_fields,
                },
            )
        unknown_fields = sorted(fields - set(PACING_LIMIT_FIELDS))
        if unknown_fields:
            return (
                CapabilityStatus.FORBIDDEN,
                ("PACING_REQUEST_CLASS_UNKNOWN_FIELD",),
                {
                    "request_class": class_name,
                    "unknown_fields": unknown_fields,
                },
            )
        for field in PACING_LIMIT_FIELDS:
            limit = limits.get(field)
            if not _is_number(limit):
                return (
                    CapabilityStatus.DEGRADED,
                    ("PACING_LIMIT_INVALID",),
                    {"request_class": class_name, "field": field},
                )
            if field in {"max_concurrency", "max_requests"} and not _is_integer(limit):
                return (
                    CapabilityStatus.DEGRADED,
                    ("PACING_LIMIT_INVALID",),
                    {"request_class": class_name, "field": field},
                )
            if limit <= 0:  # type: ignore[operator]
                return (
                    CapabilityStatus.DEGRADED,
                    ("PACING_LIMIT_NON_POSITIVE",),
                    {"request_class": class_name, "field": field},
                )
    return None


def _detached_request_classes(value: Mapping[str, object]) -> dict[str, dict[str, int | float]]:
    detached: dict[str, dict[str, int | float]] = {}
    for class_name in PACING_REQUEST_CLASSES:
        raw_limits = value.get(class_name)
        if not isinstance(raw_limits, Mapping):
            continue
        detached[class_name] = {
            field: raw_limits[field]  # type: ignore[dict-item]
            for field in PACING_LIMIT_FIELDS
            if field in raw_limits
        }
    for class_name, raw_limits in value.items():
        name = str(class_name)
        if name in detached or not isinstance(raw_limits, Mapping):
            continue
        detached[name] = {str(key): item for key, item in raw_limits.items()}  # type: ignore[dict-item]
    return detached


def _pacing_record(
    *,
    status: CapabilityStatus,
    observed_at: datetime,
    reason_codes: Sequence[str],
    details: Mapping[str, object] | None = None,
) -> CapabilityRecord:
    return CapabilityRecord(
        name="market_data_pacing",
        status=status,
        observed_at=observed_at,
        reason_codes=_dedupe(reason_codes),
        details={} if details is None else details,
    )


def _parse_datetime(value: object, *, field: str) -> datetime:
    if isinstance(value, datetime):
        return utc_datetime(value, field=field)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be a timezone-aware datetime")
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO 8601") from exc
    return utc_datetime(parsed, field=field)


def _safe_observed_at(value: object, fallback: datetime) -> datetime:
    try:
        return _parse_datetime(value, field="observed_at")
    except (TypeError, ValueError):
        return fallback


def _is_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return False
    if isinstance(value, Decimal):
        return value.is_finite()
    return math.isfinite(float(value))


def _is_integer(value: object) -> bool:
    if not _is_number(value):
        return False
    if isinstance(value, Decimal):
        return value == value.to_integral_value()
    return float(value).is_integer()  # type: ignore[arg-type]


def _reason_code(value: object) -> str:
    code = str(value).strip().upper()
    if not code or not re.fullmatch(r"[A-Z][A-Z0-9_]{1,127}", code):
        raise ValueError("invalid capability reason code")
    return code


def _dedupe(values: Sequence[str] | object) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in values:  # type: ignore[union-attr]
        code = _reason_code(raw)
        if code not in seen:
            seen.add(code)
            result.append(code)
    return tuple(result)


__all__ = [
    "CapabilityRecord",
    "CapabilityStatus",
    "DEFAULT_PACING_MAX_AGE",
    "MarketDataPacingCapability",
    "PACING_LIMIT_FIELDS",
    "PACING_REQUEST_CLASSES",
    "ReadinessReport",
]
