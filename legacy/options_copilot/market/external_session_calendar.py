"""Hash-bound broker session calendar published by an external IBKR connector."""
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

from options_copilot.storage.canonical import canonical_hash, canonical_json

from .session_calendar import (
    DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS,
    CalendarStatus,
    UsOptionsCalendarSnapshot,
    UsOptionsSessionCalendar,
)


EXTERNAL_SESSION_CALENDAR_SCHEMA = (
    "options_copilot.external_ibkr_session_calendar"
)
EXTERNAL_SESSION_CALENDAR_VERSION = 1
EXTERNAL_SESSION_CALENDAR_SOURCE = "IBKR_REQ_CONTRACT_DETAILS_READONLY"
MAXIMUM_DOCUMENT_BYTES = 2 * 1024 * 1024
_PAYLOAD_FIELDS = {
    "calendar_id",
    "observed_at",
    "liquid_hours",
    "trading_hours",
    "timezone_id",
    "source",
}
_DOCUMENT_FIELDS = {
    "schema",
    "version",
    "written_at",
    "content_hash",
    *_PAYLOAD_FIELDS,
}


class ExternalSessionCalendarError(ValueError):
    """External broker calendar is unavailable, stale, partial, or tampered."""


class ExternalSessionCalendarPublisher:
    def __init__(
        self,
        path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def publish(self, payload: Mapping[str, object]) -> UsOptionsCalendarSnapshot:
        if not isinstance(payload, Mapping) or set(payload) != _PAYLOAD_FIELDS:
            raise ExternalSessionCalendarError(
                "external session calendar fields are incomplete"
            )
        written_at = _timestamp(self._clock(), "publisher clock")
        normalized = json.loads(
            canonical_json(payload),
            object_pairs_hook=_unique_object,
        )
        signable: dict[str, object] = {
            "schema": EXTERNAL_SESSION_CALENDAR_SCHEMA,
            "version": EXTERNAL_SESSION_CALENDAR_VERSION,
            **normalized,
            "written_at": written_at.isoformat(timespec="microseconds"),
        }
        document = {**signable, "content_hash": canonical_hash(signable)}
        snapshot = _parse_document(document, now=written_at)
        rendered = (canonical_json(document) + "\n").encode("utf-8")
        if len(rendered) > MAXIMUM_DOCUMENT_BYTES:
            raise ExternalSessionCalendarError(
                "external session calendar document is too large"
            )
        _atomic_replace(self.path, rendered)
        return snapshot


class ExternalSessionCalendarProvider:
    """Read one current connector-published calendar without local fallback."""

    def __init__(
        self,
        path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def snapshot(self, *, now: datetime) -> UsOptionsCalendarSnapshot:
        checked_now = _timestamp(now, "now")
        clock_now = _timestamp(self._clock(), "provider clock")
        clock_skew = Decimal(str(abs((clock_now - checked_now).total_seconds())))
        if clock_skew > Decimal("1"):
            raise ExternalSessionCalendarError(
                "calendar request and provider clock disagree"
            )
        try:
            size = self.path.stat().st_size
            if size <= 0 or size > MAXIMUM_DOCUMENT_BYTES:
                raise ExternalSessionCalendarError(
                    "external session calendar size is invalid"
                )
            raw = self.path.read_bytes()
        except ExternalSessionCalendarError:
            raise
        except OSError as exc:
            raise ExternalSessionCalendarError(
                "external session calendar is unavailable"
            ) from exc
        try:
            document = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_unique_object,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExternalSessionCalendarError(
                "external session calendar JSON is invalid"
            ) from exc
        return _parse_document(document, now=checked_now)


def _parse_document(
    document: object,
    *,
    now: datetime,
) -> UsOptionsCalendarSnapshot:
    if not isinstance(document, Mapping) or set(document) != _DOCUMENT_FIELDS:
        raise ExternalSessionCalendarError(
            "external session calendar document is incomplete"
        )
    if (
        document["schema"] != EXTERNAL_SESSION_CALENDAR_SCHEMA
        or document["version"] != EXTERNAL_SESSION_CALENDAR_VERSION
    ):
        raise ExternalSessionCalendarError(
            "external session calendar schema is unsupported"
        )
    calendar_id = document["calendar_id"]
    if not isinstance(calendar_id, str) or not calendar_id.strip():
        raise ExternalSessionCalendarError("calendar_id is invalid")
    if document["source"] != EXTERNAL_SESSION_CALENDAR_SOURCE:
        raise ExternalSessionCalendarError("calendar source is not broker-published")
    signable = dict(document)
    content_hash = signable.pop("content_hash")
    if not isinstance(content_hash, str) or canonical_hash(signable) != content_hash:
        raise ExternalSessionCalendarError("external session calendar hash is invalid")

    checked_now = _timestamp(now, "now")
    observed_at = _timestamp(document["observed_at"], "observed_at")
    written_at = _timestamp(document["written_at"], "written_at")
    for name, timestamp in (
        ("observed_at", observed_at),
        ("written_at", written_at),
    ):
        age = Decimal(str((checked_now - timestamp).total_seconds()))
        if age < 0 or age > DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS:
            raise ExternalSessionCalendarError(f"{name} is stale or future")
    if written_at < observed_at:
        raise ExternalSessionCalendarError("calendar write precedes observation")

    snapshot = UsOptionsSessionCalendar().normalize(
        liquid_hours=_text(document["liquid_hours"], "liquid_hours"),
        trading_hours=_text(document["trading_hours"], "trading_hours"),
        timezone_id=_text(document["timezone_id"], "timezone_id"),
        observed_at=observed_at,
        source=EXTERNAL_SESSION_CALENDAR_SOURCE,
        now=checked_now,
    )
    if snapshot.status is not CalendarStatus.READY or snapshot.verify_hash() is not True:
        raise ExternalSessionCalendarError(
            "external session calendar cannot produce a READY broker snapshot"
        )
    return snapshot


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ExternalSessionCalendarError(f"{field} is invalid") from exc
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ExternalSessionCalendarError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExternalSessionCalendarError(f"{field} cannot be blank")
    return value.strip()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ExternalSessionCalendarError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _atomic_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


__all__ = [
    "EXTERNAL_SESSION_CALENDAR_SCHEMA",
    "EXTERNAL_SESSION_CALENDAR_SOURCE",
    "EXTERNAL_SESSION_CALENDAR_VERSION",
    "ExternalSessionCalendarError",
    "ExternalSessionCalendarProvider",
    "ExternalSessionCalendarPublisher",
]
