"""Read-only official event anchors and fail-closed two-week calendars.

Nothing in this module grants proposal, approval, bridge, or broker authority.
Remote access is constructor-injected, source URLs must be HTTPS, and a
partially parsed or unavailable official source degrades the complete snapshot
to ``NO_TRADE``.  Valid rows from healthy sources remain visible for research,
but every row is permanently ``SUPPORTING_ONLY``.

The Federal Reserve, BLS, BEA, SEC, and company investor-relations sites do not
share one stable machine format.  ``OfficialCalendarSource`` therefore binds a
declared official URL, category, timezone, and parser.  The parser may extract
records from JSON, iCalendar, or HTML, but it cannot choose the source identity
or authority.  Missing dates are rejected; date-only schedules stay date-only
instead of receiving a fabricated time.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
import re
from threading import RLock
from typing import Callable
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .events import JsonTransport, NewsEvent, ProviderUnavailable, _event_id, _symbol


_UTC = timezone.utc
_TWO_WEEKS = timedelta(days=14)
_DIGEST = re.compile(r"[0-9a-f]{64}")
_CATEGORIES = frozenset({"FOMC", "MACRO", "EARNINGS", "GUIDANCE"})
_PRECISIONS = frozenset({"EXACT", "DATE_ONLY"})
_EVENT_STATUSES = frozenset({"ACTIVE", "CONFLICTED"})
_READY = "READY"
_DEGRADED = "DEGRADED"
_SUPPORTING_ONLY = "SUPPORTING_ONLY"
_TRANSPORT_FAILURE_REASONS = frozenset(
    {
        "CONNECT_ERROR",
        "HTTP_403",
        "HTTP_429",
        "HTTP_5XX",
        "HTTP_ERROR",
        "REQUEST_TIMEOUT",
        "TLS_ERROR",
    }
)

OfficialCalendarParser = Callable[[object], Iterable[Mapping[str, object]]]
CompanyIrParser = Callable[[object], Iterable[Mapping[str, object]]]


class OfficialCalendarTransportError(RuntimeError):
    """A fixed, credential-free failure code from an official HTTPS source."""

    __slots__ = ("reason",)

    def __init__(self, reason: str) -> None:
        if reason not in _TRANSPORT_FAILURE_REASONS:
            raise ValueError("unsupported official calendar transport failure reason")
        self.reason = reason
        super().__init__(reason)


def _nonblank(value: object, field: str, *, maximum: int = 500) -> str:
    text = str(value or "").strip()
    if not text or len(text) > maximum:
        raise ValueError(f"{field} must be nonblank and at most {maximum} characters")
    return text


def _https(value: object, field: str) -> str:
    text = _nonblank(value, field, maximum=2000)
    if not text.lower().startswith("https://"):
        raise ValueError(f"{field} must use HTTPS")
    return text


def _digest(value: object, field: str) -> str:
    text = str(value or "").strip().lower()
    if _DIGEST.fullmatch(text) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return text


def _category(value: object) -> str:
    text = _nonblank(value, "category", maximum=40).upper()
    if text not in _CATEGORIES:
        raise ValueError(f"unsupported official calendar category: {text}")
    return text


def _timezone(value: str | None) -> ZoneInfo | None:
    if value is None:
        return None
    name = _nonblank(value, "timezone_name", maximum=80)
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown IANA timezone: {name}") from exc


def _symbols(values: object) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        raw: Sequence[object] = (values,)
    elif isinstance(values, Sequence) and not isinstance(values, (bytes, bytearray)):
        raw = values
    else:
        raise TypeError("symbols must be a string or sequence")
    normalized: list[str] = []
    for value in raw:
        symbol = _symbol(str(value))
        if symbol not in normalized:
            normalized.append(symbol)
    return tuple(normalized)


def _date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        raise TypeError("event_date must not be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError("event_date must be an ISO date")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError("event_date must be an ISO date") from exc


def _localize_strict(value: datetime, zone: ZoneInfo) -> datetime:
    """Attach an IANA zone only when the wall time is unambiguous and real."""

    first = value.replace(tzinfo=zone, fold=0)
    second = value.replace(tzinfo=zone, fold=1)
    if first.utcoffset() != second.utcoffset():
        raise ValueError("ambiguous local calendar time requires an explicit offset")
    round_trip = first.astimezone(_UTC).astimezone(zone).replace(tzinfo=None)
    if round_trip != value:
        raise ValueError("nonexistent local calendar time")
    return first


def _calendar_time(
    value: object,
    timezone_name: str | None = None,
) -> datetime | None:
    """Parse a timestamp without ever assigning an undeclared timezone."""

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("scheduled_at must be an ISO datetime") from exc
    else:
        raise TypeError("scheduled_at must be an ISO datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        zone = _timezone(timezone_name)
        if zone is None:
            raise ValueError("naive calendar time has no declared timezone")
        parsed = _localize_strict(parsed, zone)
    return utc_datetime(parsed, field="calendar timestamp")


def _timezone_label(value: datetime) -> str:
    offset = value.utcoffset()
    if offset is None:
        raise ValueError("calendar timestamp must be timezone-aware")
    if offset == timedelta(0):
        return "UTC"
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


def _source_key(value: str) -> str:
    rendered = re.sub(r"[^A-Z0-9]+", "_", value.upper()).strip("_")
    return rendered or "OFFICIAL_SOURCE"


@dataclass(frozen=True, slots=True)
class OfficialCalendarSource:
    """One explicitly declared official calendar endpoint and parser."""

    source: str
    source_url: str
    category: str
    parser: OfficialCalendarParser
    timezone_name: str | None
    symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _nonblank(self.source, "source", maximum=160))
        object.__setattr__(self, "source_url", _https(self.source_url, "source_url"))
        object.__setattr__(self, "category", _category(self.category))
        if not callable(self.parser):
            raise TypeError("official calendar parser must be callable")
        if self.timezone_name is not None:
            zone = _timezone(self.timezone_name)
            assert zone is not None
            object.__setattr__(self, "timezone_name", zone.key)
        object.__setattr__(self, "symbols", _symbols(self.symbols))

    @property
    def source_key(self) -> str:
        return _source_key(self.source)


@dataclass(frozen=True, slots=True)
class DeclaredCompanyIrSource:
    """One immutable issuer/entity-bound IR destination and parser."""

    issuer_entity_id: str
    symbol: str
    source: str
    source_url: str
    parser: CompanyIrParser
    transport: JsonTransport
    source_key: str | None = None
    source_hash: str | None = None

    def __post_init__(self) -> None:
        entity_id = _nonblank(
            self.issuer_entity_id,
            "issuer_entity_id",
            maximum=160,
        )
        symbol = _symbol(self.symbol)
        source = _nonblank(self.source, "source", maximum=160)
        source_url = _exact_company_ir_url(self.source_url)
        if not callable(self.parser):
            raise TypeError("company IR parser must be callable")
        if not callable(self.transport):
            raise TypeError("company IR transport must be callable")
        expected_key = _source_key(f"{entity_id}_{symbol}_{source}")
        if self.source_key is not None and self.source_key != expected_key:
            raise ValueError("company IR source_key does not match declared identity")
        payload = {
            "issuer_entity_id": entity_id,
            "symbol": symbol,
            "source": source,
            "source_url": source_url,
            "source_key": expected_key,
        }
        expected_hash = canonical_hash(payload)
        if self.source_hash is not None and _digest(
            self.source_hash,
            "source_hash",
        ) != expected_hash:
            raise ValueError("company IR source_hash does not match declared identity")
        object.__setattr__(self, "issuer_entity_id", entity_id)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "source_url", source_url)
        object.__setattr__(self, "source_key", expected_key)
        object.__setattr__(self, "source_hash", expected_hash)


@dataclass(frozen=True, slots=True)
class OfficialEventProvenance:
    """Point-in-time retrieval provenance for one normalized official row."""

    source: str
    source_url: str
    source_id: str
    source_payload_hash: str
    first_seen_at: datetime
    ingested_at: datetime
    observed_at: datetime
    published_at: datetime | None = None
    decision_authority: str = _SUPPORTING_ONLY
    provenance_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _nonblank(self.source, "source", maximum=160))
        object.__setattr__(self, "source_url", _https(self.source_url, "source_url"))
        object.__setattr__(self, "source_id", _nonblank(self.source_id, "source_id", maximum=240))
        object.__setattr__(
            self,
            "source_payload_hash",
            _digest(self.source_payload_hash, "source_payload_hash"),
        )
        if self.decision_authority != _SUPPORTING_ONLY:
            raise ValueError("official event provenance is SUPPORTING_ONLY")
        for field in ("first_seen_at", "ingested_at", "observed_at"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        if self.published_at is not None:
            object.__setattr__(
                self,
                "published_at",
                utc_datetime(self.published_at, field="published_at"),
            )
        if (
            (self.published_at is not None and self.published_at > self.first_seen_at)
            or self.first_seen_at > self.ingested_at
            or self.ingested_at > self.observed_at
        ):
            raise ValueError("official provenance timestamps must be ordered")
        payload = {
            "source": self.source,
            "source_url": self.source_url,
            "source_id": self.source_id,
            "source_payload_hash": self.source_payload_hash,
            "published_at": self.published_at,
            "first_seen_at": self.first_seen_at,
            "ingested_at": self.ingested_at,
            "observed_at": self.observed_at,
            "decision_authority": self.decision_authority,
        }
        expected = canonical_hash(payload)
        if self.provenance_hash is None:
            object.__setattr__(self, "provenance_hash", expected)
        elif _digest(self.provenance_hash, "provenance_hash") != expected:
            raise ValueError("official provenance hash does not match")

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "source_url": self.source_url,
            "source_id": self.source_id,
            "source_payload_hash": self.source_payload_hash,
            "published_at": None if self.published_at is None else self.published_at.isoformat(),
            "first_seen_at": self.first_seen_at.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "observed_at": self.observed_at.isoformat(),
            "decision_authority": self.decision_authority,
            "provenance_hash": self.provenance_hash,
        }


@dataclass(frozen=True, slots=True)
class OfficialCalendarEvent:
    """Normalized event that preserves exact-time versus date-only precision."""

    event_id: str
    source: str
    source_id: str
    title: str
    scheduled_at: datetime | None
    published_at: datetime | None
    first_seen_at: datetime
    ingested_at: datetime
    observed_at: datetime
    category: str = "MACRO"
    source_url: str = ""
    event_date: date | None = None
    timezone_name: str | None = None
    schedule_precision: str = "EXACT"
    symbols: tuple[str, ...] = ()
    url: str = ""
    provenance: tuple[OfficialEventProvenance, ...] = ()
    content_hash: str | None = None
    record_hash: str | None = None
    status: str = "ACTIVE"
    decision_authority: str = _SUPPORTING_ONLY

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _nonblank(self.event_id, "event_id", maximum=160))
        object.__setattr__(self, "source", _nonblank(self.source, "source", maximum=160))
        object.__setattr__(self, "source_id", _nonblank(self.source_id, "source_id", maximum=240))
        object.__setattr__(self, "title", _nonblank(self.title, "title", maximum=500))
        object.__setattr__(self, "category", _category(self.category))
        if self.source_url:
            object.__setattr__(self, "source_url", _https(self.source_url, "source_url"))
        if self.url:
            object.__setattr__(self, "url", _https(self.url, "url"))
        if self.decision_authority != _SUPPORTING_ONLY:
            raise ValueError("official calendar events are SUPPORTING_ONLY")
        if self.status not in _EVENT_STATUSES:
            raise ValueError("invalid official event status")
        precision = _nonblank(self.schedule_precision, "schedule_precision", maximum=20).upper()
        if precision not in _PRECISIONS:
            raise ValueError("schedule_precision must be EXACT or DATE_ONLY")
        object.__setattr__(self, "schedule_precision", precision)
        zone = _timezone(self.timezone_name)
        if zone is not None:
            object.__setattr__(self, "timezone_name", zone.key)
        scheduled = self.scheduled_at
        if scheduled is not None:
            scheduled = utc_datetime(scheduled, field="scheduled_at")
            object.__setattr__(self, "scheduled_at", scheduled)
        declared_date = _date(self.event_date)
        if precision == "EXACT":
            if scheduled is None:
                raise ValueError("EXACT calendar event requires scheduled_at")
            local = scheduled if zone is None else scheduled.astimezone(zone)
            derived = local.date()
            if declared_date is not None and declared_date != derived:
                raise ValueError("event_date conflicts with scheduled_at in declared timezone")
            declared_date = derived
            if self.timezone_name is None:
                object.__setattr__(self, "timezone_name", _timezone_label(local))
        else:
            if scheduled is not None:
                raise ValueError("DATE_ONLY event cannot carry a fabricated scheduled_at")
            if declared_date is None or zone is None:
                raise ValueError("DATE_ONLY event requires event_date and declared IANA timezone")
        object.__setattr__(self, "event_date", declared_date)
        for field in ("first_seen_at", "ingested_at", "observed_at"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        if self.published_at is not None:
            object.__setattr__(
                self,
                "published_at",
                utc_datetime(self.published_at, field="published_at"),
            )
        if (
            (self.published_at is not None and self.published_at > self.first_seen_at)
            or self.first_seen_at > self.ingested_at
            or self.ingested_at > self.observed_at
        ):
            raise ValueError("official calendar timestamps must be ordered")
        object.__setattr__(self, "symbols", _symbols(self.symbols))
        provenance = tuple(self.provenance)
        if not provenance or any(not isinstance(item, OfficialEventProvenance) for item in provenance):
            raise ValueError("official calendar event requires provenance")
        if any(item.source != self.source or item.source_id != self.source_id for item in provenance):
            raise ValueError("official calendar provenance identity mismatch")
        object.__setattr__(self, "provenance", provenance)
        semantic = {
            "source": self.source,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "title": self.title,
            "scheduled_at": self.scheduled_at,
            "event_date": self.event_date,
            "timezone_name": self.timezone_name,
            "schedule_precision": self.schedule_precision,
            "category": self.category,
            "symbols": self.symbols,
            "url": self.url,
        }
        expected_content = canonical_hash(semantic)
        if self.content_hash is None:
            object.__setattr__(self, "content_hash", expected_content)
        elif _digest(self.content_hash, "content_hash") != expected_content:
            raise ValueError("official event content hash does not match")
        record = {
            "event_id": self.event_id,
            "content_hash": self.content_hash,
            "published_at": self.published_at,
            "first_seen_at": self.first_seen_at,
            "ingested_at": self.ingested_at,
            "observed_at": self.observed_at,
            "provenance_hashes": tuple(item.provenance_hash for item in provenance),
            "status": self.status,
            "decision_authority": self.decision_authority,
        }
        expected_record = canonical_hash(record)
        if self.record_hash is None:
            object.__setattr__(self, "record_hash", expected_record)
        elif _digest(self.record_hash, "record_hash") != expected_record:
            raise ValueError("official event record hash does not match")

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.event_id,
            "event_id": self.event_id,
            "source": self.source,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "title": self.title,
            "category": self.category,
            "event_at": None if self.scheduled_at is None else self.scheduled_at.isoformat(),
            "scheduled_at": None if self.scheduled_at is None else self.scheduled_at.isoformat(),
            "event_date": self.event_date.isoformat() if self.event_date is not None else None,
            "timezone": self.timezone_name,
            "schedule_precision": self.schedule_precision,
            "symbols": list(self.symbols),
            "url": self.url,
            "published_at": None if self.published_at is None else self.published_at.isoformat(),
            "first_seen_at": self.first_seen_at.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "observed_at": self.observed_at.isoformat(),
            "provenance": [item.as_dict() for item in self.provenance],
            "content_hash": self.content_hash,
            "record_hash": self.record_hash,
            "status": self.status,
            "decision_authority": self.decision_authority,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
        }


@dataclass(frozen=True, slots=True)
class OfficialSourceHealth:
    source: str
    source_url: str
    status: str
    reason: str | None
    observed_at: datetime
    event_count: int
    content_hash: str | None = None
    source_id: str | None = None
    configured: bool = True
    readiness: str | None = None
    last_success_at: datetime | None = None
    as_of: datetime | None = None
    freshness_age_seconds: int | None = None
    provenance: tuple[str, ...] = ()
    pacing: str = "PACING_UNVERIFIED"
    decision_authority: str = _SUPPORTING_ONLY

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _nonblank(self.source, "source", maximum=160))
        object.__setattr__(self, "source_url", _https(self.source_url, "source_url"))
        if self.status not in {
            _READY,
            _DEGRADED,
            "UNCONFIGURED",
            "LIMITED",
            "FAILED",
            "STALE",
        }:
            raise ValueError("official source health status is invalid")
        if not isinstance(self.configured, bool):
            raise TypeError("configured must be boolean")
        if self.status == "UNCONFIGURED" and self.configured:
            raise ValueError("unconfigured official source cannot be configured")
        if self.status == _READY and self.reason is not None:
            raise ValueError("ready official source cannot have a failure reason")
        if self.status != _READY:
            object.__setattr__(self, "reason", _nonblank(self.reason, "reason", maximum=80))
        if isinstance(self.event_count, bool) or not isinstance(self.event_count, int) or self.event_count < 0:
            raise ValueError("event_count must be a non-negative integer")
        object.__setattr__(self, "observed_at", utc_datetime(self.observed_at, field="observed_at"))
        source_id = self.source_id or _source_key(self.source).lower()
        object.__setattr__(
            self,
            "source_id",
            _nonblank(source_id, "source_id", maximum=160),
        )
        readiness = self.readiness or self.status
        object.__setattr__(
            self,
            "readiness",
            _nonblank(readiness, "readiness", maximum=40).upper(),
        )
        last_success_at = self.last_success_at
        if last_success_at is None and self.status == _READY:
            last_success_at = self.observed_at
        if last_success_at is not None:
            last_success_at = utc_datetime(
                last_success_at,
                field="last_success_at",
            )
            if last_success_at > self.observed_at:
                raise ValueError("last_success_at cannot follow observed_at")
        object.__setattr__(self, "last_success_at", last_success_at)
        as_of = self.as_of or self.observed_at
        as_of = utc_datetime(as_of, field="as_of")
        if as_of > self.observed_at:
            raise ValueError("as_of cannot follow observed_at")
        object.__setattr__(self, "as_of", as_of)
        freshness = self.freshness_age_seconds
        if freshness is None and last_success_at is not None:
            freshness = max(
                0,
                int((self.observed_at - last_success_at).total_seconds()),
            )
        if (
            freshness is not None
            and (
                isinstance(freshness, bool)
                or not isinstance(freshness, int)
                or freshness < 0
            )
        ):
            raise ValueError("freshness_age_seconds must be a non-negative integer")
        object.__setattr__(self, "freshness_age_seconds", freshness)
        provenance = tuple(
            dict.fromkeys(
                _nonblank(item, "provenance", maximum=240)
                for item in self.provenance
            )
        )
        object.__setattr__(self, "provenance", provenance)
        object.__setattr__(
            self,
            "pacing",
            _nonblank(self.pacing, "pacing", maximum=40).upper(),
        )
        if self.decision_authority != _SUPPORTING_ONLY:
            raise ValueError("official source health is SUPPORTING_ONLY")
        payload = {
            "source_id": self.source_id,
            "source": self.source,
            "source_url": self.source_url,
            "configured": self.configured,
            "readiness": self.readiness,
            "status": self.status,
            "reason": self.reason,
            "observed_at": self.observed_at,
            "as_of": self.as_of,
            "last_success_at": self.last_success_at,
            "freshness_age_seconds": self.freshness_age_seconds,
            "provenance": self.provenance,
            "pacing": self.pacing,
            "event_count": self.event_count,
            "decision_authority": self.decision_authority,
        }
        expected = canonical_hash(payload)
        if self.content_hash is None:
            object.__setattr__(self, "content_hash", expected)
        elif _digest(self.content_hash, "content_hash") != expected:
            raise ValueError("official source health hash does not match")

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "source": self.source,
            "source_url": self.source_url,
            "configured": self.configured,
            "readiness": self.readiness,
            "status": self.status,
            "reason": self.reason,
            "observed_at": self.observed_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "last_success_at": (
                None
                if self.last_success_at is None
                else self.last_success_at.isoformat()
            ),
            "freshness_age_seconds": self.freshness_age_seconds,
            "provenance": self.provenance,
            "pacing": self.pacing,
            "event_count": self.event_count,
            "content_hash": self.content_hash,
            "decision_authority": self.decision_authority,
        }


@dataclass(frozen=True, slots=True)
class OfficialCalendarSnapshot:
    status: str
    decision: str
    window_start: datetime
    window_end: datetime
    observed_at: datetime
    events: tuple[OfficialCalendarEvent, ...]
    sources: tuple[OfficialSourceHealth, ...]
    reasons: tuple[str, ...]
    snapshot_hash: str | None = None
    decision_authority: str = _SUPPORTING_ONLY
    approval_eligible: bool = False
    instruction_creation_allowed: bool = False

    def __post_init__(self) -> None:
        if self.status not in {_READY, _DEGRADED}:
            raise ValueError("official calendar status must be READY or DEGRADED")
        expected_decision = "OBSERVATION_ONLY" if self.status == _READY else "NO_TRADE"
        if self.decision != expected_decision:
            raise ValueError("official calendar decision does not match health")
        if self.decision_authority != _SUPPORTING_ONLY:
            raise ValueError("official calendar is SUPPORTING_ONLY")
        if self.approval_eligible or self.instruction_creation_allowed:
            raise ValueError("official calendar cannot grant trading authority")
        for field in ("window_start", "window_end", "observed_at"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        if self.window_end - self.window_start != _TWO_WEEKS:
            raise ValueError("official calendar window must be exactly two weeks")
        events = tuple(self.events)
        sources = tuple(self.sources)
        if any(not isinstance(item, OfficialCalendarEvent) for item in events):
            raise TypeError("events must contain OfficialCalendarEvent values")
        if any(not isinstance(item, OfficialSourceHealth) for item in sources):
            raise TypeError("sources must contain OfficialSourceHealth values")
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "sources", sources)
        reasons = tuple(dict.fromkeys(_nonblank(item, "reason", maximum=160) for item in self.reasons))
        if self.status == _READY and reasons:
            raise ValueError("ready official calendar cannot have failure reasons")
        if self.status == _DEGRADED and not reasons:
            raise ValueError("degraded official calendar requires reasons")
        object.__setattr__(self, "reasons", reasons)
        payload = {
            "status": self.status,
            "decision": self.decision,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "observed_at": self.observed_at,
            "event_record_hashes": tuple(item.record_hash for item in events),
            "source_health_hashes": tuple(item.content_hash for item in sources),
            "reasons": reasons,
            "decision_authority": self.decision_authority,
            "approval_eligible": self.approval_eligible,
            "instruction_creation_allowed": self.instruction_creation_allowed,
        }
        expected = canonical_hash(payload)
        if self.snapshot_hash is None:
            object.__setattr__(self, "snapshot_hash", expected)
        elif _digest(self.snapshot_hash, "snapshot_hash") != expected:
            raise ValueError("official calendar snapshot hash does not match")

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "decision": self.decision,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "observed_at": self.observed_at.isoformat(),
            "asof": self.observed_at.isoformat(),
            "calendar": [item.as_dict() for item in self.events],
            "events": [item.as_dict() for item in self.events],
            "count": len(self.events),
            "sources": [item.as_dict() for item in self.sources],
            "reasons": list(self.reasons),
            "snapshot_hash": self.snapshot_hash,
            "decision_authority": self.decision_authority,
            "approval_eligible": self.approval_eligible,
            "instruction_creation_allowed": self.instruction_creation_allowed,
        }


class OfficialCalendarProvider:
    """Aggregate declared official feeds into one strict two-week snapshot."""

    def __init__(
        self,
        *,
        sources: Sequence[OfficialCalendarSource],
        transport: JsonTransport | None,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be numeric")
        if not 0 < float(timeout_seconds) <= 30:
            raise ValueError("timeout_seconds must be between 0 and 30")
        checked_sources = tuple(sources)
        if any(not isinstance(item, OfficialCalendarSource) for item in checked_sources):
            raise TypeError("sources must contain OfficialCalendarSource values")
        identities = [(item.source, item.source_url) for item in checked_sources]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate official calendar source")
        self._sources = checked_sources
        self._transport = transport
        self._now = now or (lambda: datetime.now(_UTC))
        self._timeout = float(timeout_seconds)
        self._first_seen: dict[tuple[str, str, str], datetime] = {}
        self._lock = RLock()
        self.health = _DEGRADED
        self.health_reason: str | None = "not_observed"

    def future_two_weeks(self, *, now: datetime | None = None) -> OfficialCalendarSnapshot:
        observed = utc_datetime(now or self._now(), field="now")
        window_end = observed + _TWO_WEEKS
        with self._lock:
            if not self._sources:
                self.health, self.health_reason = _DEGRADED, "official_sources_not_configured"
                return OfficialCalendarSnapshot(
                    status=_DEGRADED,
                    decision="NO_TRADE",
                    window_start=observed,
                    window_end=window_end,
                    observed_at=observed,
                    events=(),
                    sources=(),
                    reasons=("OFFICIAL_SOURCES_NOT_CONFIGURED",),
                )
            all_events: list[OfficialCalendarEvent] = []
            health: list[OfficialSourceHealth] = []
            for source in self._sources:
                events, source_health = self._fetch_source(
                    source,
                    observed=observed,
                    window_start=observed,
                    window_end=window_end,
                )
                all_events.extend(events)
                health.append(source_health)
            all_events.sort(key=_event_sort_key)
            reasons = tuple(
                f"{_source_key(item.source)}:{item.reason}"
                for item in health
                if item.status != _READY and item.reason is not None
            )
            status = _READY if not reasons else _DEGRADED
            decision = "OBSERVATION_ONLY" if status == _READY else "NO_TRADE"
            self.health = status
            self.health_reason = None if status == _READY else "official_source_degraded"
            return OfficialCalendarSnapshot(
                status=status,
                decision=decision,
                window_start=observed,
                window_end=window_end,
                observed_at=observed,
                events=tuple(all_events),
                sources=tuple(health),
                reasons=reasons,
            )

    def calendar_payload(self) -> dict[str, object]:
        """Provider-compatible read model for a future API composition seam."""

        return self.future_two_weeks().as_dict()

    def bounded_events(
        self,
        *,
        now: datetime,
        horizon_days: int,
    ) -> tuple[tuple[OfficialCalendarEvent, ...], tuple[OfficialSourceHealth, ...]]:
        """Read a bounded longer-horizon schedule without changing the public window."""

        if isinstance(horizon_days, bool) or not isinstance(horizon_days, int):
            raise TypeError("horizon_days must be an integer")
        if not 14 < horizon_days <= 120:
            raise ValueError("horizon_days must be between 15 and 120")
        observed = utc_datetime(now, field="now")
        window_end = observed + timedelta(days=horizon_days)
        with self._lock:
            events: list[OfficialCalendarEvent] = []
            health: list[OfficialSourceHealth] = []
            for source in self._sources:
                source_events, source_health = self._fetch_source(
                    source,
                    observed=observed,
                    window_start=observed,
                    window_end=window_end,
                )
                events.extend(source_events)
                health.append(source_health)
        return tuple(sorted(events, key=_event_sort_key)), tuple(health)

    def _fetch_source(
        self,
        source: OfficialCalendarSource,
        *,
        observed: datetime,
        window_start: datetime | None,
        window_end: datetime | None,
    ) -> tuple[tuple[OfficialCalendarEvent, ...], OfficialSourceHealth]:
        if self._transport is None:
            return (), OfficialSourceHealth(
                source.source,
                source.source_url,
                _DEGRADED,
                "TRANSPORT_NOT_CONFIGURED",
                observed,
                0,
            )
        try:
            payload = self._transport(
                source.source_url,
                headers={"Accept": "application/json, text/calendar, text/html;q=0.9"},
                timeout_seconds=self._timeout,
            )
        except OfficialCalendarTransportError as exc:
            return (), OfficialSourceHealth(
                source.source,
                source.source_url,
                _DEGRADED,
                exc.reason,
                observed,
                0,
            )
        except TimeoutError:
            return (), OfficialSourceHealth(
                source.source,
                source.source_url,
                _DEGRADED,
                "REQUEST_TIMEOUT",
                observed,
                0,
            )
        except Exception:
            return (), OfficialSourceHealth(
                source.source,
                source.source_url,
                _DEGRADED,
                "REQUEST_FAILED",
                observed,
                0,
            )
        try:
            parsed = source.parser(payload)
            if isinstance(parsed, (Mapping, str, bytes, bytearray)):
                raise TypeError("parser result must be an iterable of mappings")
            rows = tuple(parsed)
        except Exception:
            return (), OfficialSourceHealth(
                source.source,
                source.source_url,
                _DEGRADED,
                "PARSER_FAILED",
                observed,
                0,
            )
        events: list[OfficialCalendarEvent] = []
        malformed = False
        for row in rows:
            if not isinstance(row, Mapping):
                malformed = True
                continue
            try:
                event = self._normalize_event(source, row, observed=observed)
            except (TypeError, ValueError):
                malformed = True
                continue
            if _in_window(event, start=window_start, end=window_end):
                events.append(event)
        events, conflict = _mark_source_id_conflicts(events)
        malformed = malformed or conflict
        reason = "SOURCE_ID_CONFLICT" if conflict else "INCOMPLETE_RECORDS" if malformed else None
        status = _DEGRADED if reason is not None else _READY
        return tuple(sorted(events, key=_event_sort_key)), OfficialSourceHealth(
            source.source,
            source.source_url,
            status,
            reason,
            observed,
            len(events),
        )

    def _normalize_event(
        self,
        source: OfficialCalendarSource,
        row: Mapping[str, object],
        *,
        observed: datetime,
    ) -> OfficialCalendarEvent:
        source_id = _nonblank(row.get("id") or row.get("source_id") or row.get("uid"), "source_id", maximum=240)
        title = _nonblank(row.get("title") or row.get("summary"), "title", maximum=500)
        row_timezone = row.get("timezone") or row.get("timezone_name")
        timezone_name = source.timezone_name
        if row_timezone is not None:
            declared = _timezone(str(row_timezone))
            assert declared is not None
            if timezone_name is not None and declared.key != timezone_name:
                raise ValueError("row timezone conflicts with official source timezone")
            timezone_name = declared.key
        scheduled_raw = row.get("scheduled_at", row.get("event_at"))
        declared_date = _date(
            row.get("event_date", row.get("scheduled_date", row.get("report_date")))
        )
        scheduled = _calendar_time(scheduled_raw, timezone_name)
        if scheduled is None and declared_date is None:
            raise ValueError("official calendar row has no schedule")
        precision = "EXACT" if scheduled is not None else "DATE_ONLY"
        if precision == "DATE_ONLY" and timezone_name is None:
            raise ValueError("date-only event requires declared timezone")
        if scheduled is not None and timezone_name is None:
            timezone_name = _timezone_label(scheduled)
        published = _calendar_time(row.get("published_at"), timezone_name)
        if published is not None and published > observed:
            raise ValueError("official publication cannot be in the future")
        symbols = _symbols(row.get("symbols", row.get("symbol", source.symbols)))
        event_url = str(row.get("url") or "").strip()
        if event_url:
            event_url = _https(event_url, "url")
        semantic = {
            "source": source.source,
            "source_url": source.source_url,
            "source_id": source_id,
            "title": title,
            "scheduled_at": scheduled,
            "event_date": declared_date,
            "timezone_name": timezone_name,
            "schedule_precision": precision,
            "category": source.category,
            "symbols": symbols,
            "url": event_url,
            "published_at": published,
        }
        source_payload_hash = canonical_hash(semantic)
        first_seen_key = (source.source, source_id, source_payload_hash)
        first_seen = self._first_seen.setdefault(first_seen_key, observed)
        provenance = OfficialEventProvenance(
            source=source.source,
            source_url=source.source_url,
            source_id=source_id,
            source_payload_hash=source_payload_hash,
            published_at=published,
            first_seen_at=first_seen,
            ingested_at=observed,
            observed_at=observed,
        )
        return OfficialCalendarEvent(
            event_id=_event_id("official-calendar", source.source, source_id),
            source=source.source,
            source_id=source_id,
            title=title,
            scheduled_at=scheduled,
            event_date=declared_date,
            timezone_name=timezone_name,
            schedule_precision=precision,
            published_at=published,
            first_seen_at=first_seen,
            ingested_at=observed,
            observed_at=observed,
            category=source.category,
            source_url=source.source_url,
            symbols=symbols,
            url=event_url,
            provenance=(provenance,),
        )


class CompanyIrEventProvider:
    """Fetch only issuer IR endpoints present in an immutable declared registry."""

    decision_authority = _SUPPORTING_ONLY
    approval_eligible = False
    instruction_creation_allowed = False
    order_allowed = False

    def __init__(
        self,
        *,
        sources: Sequence[DeclaredCompanyIrSource] = (),
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        checked = tuple(sources)
        if any(not isinstance(item, DeclaredCompanyIrSource) for item in checked):
            raise TypeError("sources must contain DeclaredCompanyIrSource values")
        identities = {
            (item.issuer_entity_id, item.symbol, item.source_url)
            for item in checked
        }
        if len(identities) != len(checked):
            raise ValueError("duplicate declared company IR source")
        if isinstance(timeout_seconds, bool) or not isinstance(
            timeout_seconds,
            (int, float),
        ):
            raise TypeError("timeout_seconds must be numeric")
        timeout = float(timeout_seconds)
        if not 0 < timeout <= 30:
            raise ValueError("timeout_seconds must be between 0 and 30")
        self._sources = checked
        self._now = now or (lambda: datetime.now(_UTC))
        self._timeout = timeout
        self.health = "UNCONFIGURED" if not checked else _DEGRADED
        self.health_reason: str | None = (
            "UNCONFIGURED" if not checked else "NOT_OBSERVED"
        )
        self.last_observed_at: datetime | None = None
        self.last_success_at: datetime | None = None

    def news(
        self,
        symbols: Sequence[str],
        *,
        limit: int = 50,
    ) -> tuple[NewsEvent, ...]:
        requested = tuple(dict.fromkeys(_symbol(value) for value in symbols))
        if not requested:
            raise ValueError("symbols cannot be empty")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 1000
        ):
            raise ValueError("limit must be between 1 and 1000")
        selected = tuple(
            source for source in self._sources if source.symbol in requested
        )
        if not selected:
            self.health, self.health_reason = "UNCONFIGURED", "UNCONFIGURED"
            return ()
        observed = utc_datetime(self._now(), field="now")
        self.last_observed_at = observed
        events: list[NewsEvent] = []
        failure_reason: str | None = None
        for source in selected:
            try:
                payload = source.transport(
                    source.source_url,
                    headers={"Accept": "application/json"},
                    timeout_seconds=self._timeout,
                )
                parsed = source.parser(payload)
                if isinstance(parsed, (Mapping, str, bytes, bytearray)):
                    raise TypeError("company IR parser must return row mappings")
                rows = tuple(parsed)
            except TimeoutError:
                failure_reason = failure_reason or "REQUEST_TIMEOUT"
                continue
            except Exception:
                failure_reason = failure_reason or "REQUEST_FAILED"
                continue
            for row in rows:
                if not isinstance(row, Mapping):
                    failure_reason = failure_reason or "PARSER_FAILED"
                    continue
                try:
                    event = self._normalize(source, row, observed=observed)
                except (TypeError, ValueError):
                    failure_reason = failure_reason or "PARSER_FAILED"
                    continue
                events.append(event)
        if events:
            self.last_success_at = observed
        if failure_reason is None:
            self.health, self.health_reason = _READY, None
        else:
            self.health, self.health_reason = _DEGRADED, failure_reason
        return tuple(
            sorted(
                events,
                key=lambda event: (event.published_at, event.event_id),
                reverse=True,
            )[:limit]
        )

    def health_snapshot(self) -> dict[str, object]:
        configured = bool(self._sources)
        observed = self.last_observed_at
        last_success = self.last_success_at
        return {
            "source_id": "company_ir",
            "configured": configured,
            "readiness": self.health if configured else "UNCONFIGURED",
            "status": self.health if configured else "UNCONFIGURED",
            "observed_at": None if observed is None else observed.isoformat(),
            "as_of": None if observed is None else observed.isoformat(),
            "last_success_at": (
                None if last_success is None else last_success.isoformat()
            ),
            "freshness_age_seconds": (
                None
                if observed is None or last_success is None
                else max(0, int((observed - last_success).total_seconds()))
            ),
            "provenance": tuple(
                source.source_hash for source in self._sources
            ),
            "pacing": "PACING_UNVERIFIED",
            "reason": self.health_reason if configured else "UNCONFIGURED",
            "decision_authority": _SUPPORTING_ONLY,
        }

    @staticmethod
    def _normalize(
        source: DeclaredCompanyIrSource,
        row: Mapping[str, object],
        *,
        observed: datetime,
    ) -> NewsEvent:
        source_id = _nonblank(
            row.get("id") or row.get("source_id"),
            "source_id",
            maximum=240,
        )
        title = _nonblank(
            row.get("title") or row.get("headline"),
            "title",
            maximum=500,
        )
        published = _calendar_time(row.get("published_at"), "UTC")
        if published is None or published > observed:
            raise ValueError("company IR publication timestamp is invalid")
        row_url = str(row.get("url") or source.source_url).strip()
        if row_url != source.source_url:
            raise ValueError("company IR row URL differs from declared source")
        evidence_id = f"{source.source_key}:{source_id}"
        return NewsEvent(
            event_id=_event_id("company-ir", source.issuer_entity_id, source_id),
            symbol=source.symbol,
            entity_id=source.issuer_entity_id,
            source="Company IR",
            source_id=source_id,
            source_rank=1,
            source_tier=1,
            lineage_id=evidence_id,
            evidence_ids=(evidence_id,),
            headline=title,
            summary=str(row.get("summary") or "").strip()[:2000],
            url=source.source_url,
            published_at=published,
            first_seen_at=observed,
            ingested_at=observed,
            observed_at=observed,
            provenance=(source.source, source.source_hash),
        )


class OfficialEventProvider:
    """Create SEC/company-IR anchors and fetch one declared official feed.

    This compatibility adapter retains the existing filing/news API while
    sharing the strict calendar normalizer used by ``OfficialCalendarProvider``.
    """

    def __init__(
        self,
        *,
        transport: JsonTransport | None = None,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self._transport = transport
        self._now = now or (lambda: datetime.now(_UTC))
        self._timeout = timeout_seconds
        self._first_seen: dict[tuple[str, str, str], datetime] = {}
        self.health = _READY
        self.health_reason: str | None = None

    def filing(
        self,
        *,
        symbol: str,
        source_id: str,
        title: str,
        published_at: datetime,
        summary: str = "",
        url: str = "",
    ) -> NewsEvent:
        ticker = _symbol(symbol)
        observed = utc_datetime(self._now(), field="now")
        if url:
            _https(url, "url")
        return NewsEvent(
            event_id=_event_id("official", ticker, _nonblank(source_id, "source_id", maximum=240)),
            symbol=ticker,
            source="SEC",
            headline=_nonblank(title, "title", maximum=500),
            summary=summary.strip()[:2000],
            url=url.strip()[:2000],
            published_at=published_at,
            first_seen_at=observed,
            ingested_at=observed,
            observed_at=observed,
            source_rank=0,
            source_id=source_id,
            provenance=("SEC",),
        )

    def company_ir(
        self,
        *,
        symbol: str,
        source_id: str,
        title: str,
        published_at: datetime,
        summary: str = "",
        url: str = "",
        entity_id: str | None = None,
        lineage_id: str | None = None,
        evidence_ids: Sequence[str] = (),
    ) -> NewsEvent:
        event = self.filing(
            symbol=symbol,
            source_id=source_id,
            title=title,
            published_at=published_at,
            summary=summary,
            url=url,
        )
        return replace(
            event,
            source="Company IR",
            source_rank=1,
            source_tier=1,
            entity_id=entity_id or event.entity_id,
            lineage_id=lineage_id or event.lineage_id,
            evidence_ids=tuple(evidence_ids) or event.evidence_ids,
            provenance=("Company IR",),
        )

    def fetch_sec_company_facts(self, symbol: str, cik: str) -> object:
        """Fetch SEC companyfacts only through an injected read-only transport."""

        if self._transport is None:
            self.health, self.health_reason = "DISABLED", "transport_not_injected"
            raise ProviderUnavailable("official provider transport is not configured")
        _symbol(symbol)
        normalized_cik = "".join(character for character in cik if character.isdigit())
        if not normalized_cik:
            self.health, self.health_reason = "MISSING_FIELDS", "invalid_cik"
            raise ProviderUnavailable("official company facts require a numeric CIK")
        try:
            payload = self._transport(
                f"https://data.sec.gov/api/xbrl/companyfacts/CIK{normalized_cik.zfill(10)}.json",
                headers={
                    "Accept": "application/json",
                    "User-Agent": "OptionsCopilot/0.1 contact@example.invalid",
                },
                timeout_seconds=self._timeout,
            )
        except TimeoutError as exc:
            self.health, self.health_reason = "TIMEOUT", "request_timeout"
            raise ProviderUnavailable("official provider timed out") from exc
        except Exception as exc:
            self.health, self.health_reason = _DEGRADED, "request_failed"
            raise ProviderUnavailable("official provider unavailable") from exc
        if not isinstance(payload, dict):
            self.health, self.health_reason = "BAD_JSON", "response_not_object"
            raise ProviderUnavailable("official provider response is invalid")
        self.health, self.health_reason = _READY, None
        return payload

    def fetch_calendar(
        self,
        source_url: str,
        *,
        parser: OfficialCalendarParser,
        source: str,
        category: str = "MACRO",
        timezone_name: str | None = "UTC",
        symbols: Sequence[str] = (),
    ) -> tuple[OfficialCalendarEvent, ...]:
        """Fetch one official calendar without imposing a date window.

        Use ``OfficialCalendarProvider.future_two_weeks`` for the production
        two-week aggregate.  This method remains useful for source probes and
        backwards-compatible provider contract tests.
        """

        declared = OfficialCalendarSource(
            source=source,
            source_url=source_url,
            category=category,
            parser=parser,
            timezone_name=timezone_name,
            symbols=tuple(symbols),
        )
        provider = OfficialCalendarProvider(
            sources=(declared,),
            transport=self._transport,
            now=self._now,
            timeout_seconds=self._timeout,
        )
        provider._first_seen = self._first_seen
        observed = utc_datetime(self._now(), field="now")
        with provider._lock:
            events, health = provider._fetch_source(
                declared,
                observed=observed,
                window_start=None,
                window_end=None,
            )
        self._first_seen = provider._first_seen
        self.health = health.status
        self.health_reason = None if health.status == _READY else health.reason.lower()
        return events


def _in_window(
    event: OfficialCalendarEvent,
    *,
    start: datetime | None,
    end: datetime | None,
) -> bool:
    if start is None or end is None:
        return True
    if event.scheduled_at is not None:
        return start <= event.scheduled_at < end
    assert event.event_date is not None
    zone = _timezone(event.timezone_name)
    assert zone is not None
    start_date = start.astimezone(zone).date()
    end_date = start_date + timedelta(days=14)
    return start_date <= event.event_date < end_date


def _exact_company_ir_url(value: object) -> str:
    text = _https(value, "source_url")
    try:
        parsed = urlsplit(text)
        port = parsed.port
    except ValueError:
        raise ValueError("company IR source URL is invalid") from None
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.netloc != parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "company IR source must declare one exact HTTPS default-port host/path"
        )
    return text


def _event_sort_key(event: OfficialCalendarEvent) -> tuple[date, time, str, str]:
    assert event.event_date is not None
    event_time = (
        time.max
        if event.scheduled_at is None
        else event.scheduled_at.astimezone(_timezone(event.timezone_name) or _UTC).time()
    )
    return event.event_date, event_time, event.source, event.source_id


def _mark_source_id_conflicts(
    events: Sequence[OfficialCalendarEvent],
) -> tuple[list[OfficialCalendarEvent], bool]:
    grouped: dict[tuple[str, str], list[OfficialCalendarEvent]] = {}
    for event in events:
        grouped.setdefault((event.source, event.source_id), []).append(event)
    result: list[OfficialCalendarEvent] = []
    conflicted = False
    for group in grouped.values():
        by_hash: dict[str, OfficialCalendarEvent] = {}
        for event in group:
            assert event.content_hash is not None
            by_hash.setdefault(event.content_hash, event)
        if len(by_hash) == 1:
            result.append(next(iter(by_hash.values())))
            continue
        conflicted = True
        for event in by_hash.values():
            assert event.content_hash is not None
            result.append(
                replace(
                    event,
                    event_id=f"{event.event_id}_{event.content_hash[:8]}",
                    status="CONFLICTED",
                    record_hash=None,
                )
            )
    return result, conflicted


__all__ = [
    "CompanyIrEventProvider",
    "CompanyIrParser",
    "DeclaredCompanyIrSource",
    "OfficialCalendarEvent",
    "OfficialCalendarParser",
    "OfficialCalendarProvider",
    "OfficialCalendarSnapshot",
    "OfficialCalendarSource",
    "OfficialCalendarTransportError",
    "OfficialEventProvenance",
    "OfficialEventProvider",
    "OfficialSourceHealth",
]
