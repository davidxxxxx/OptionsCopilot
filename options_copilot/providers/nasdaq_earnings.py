"""Strict, cached Nasdaq earnings-calendar metadata.

The Nasdaq calendar is an expected-earnings calendar rather than a company
investor-relations confirmation source.  Every row is therefore marked as an
estimate and remains permanently ``SUPPORTING_ONLY``.  The provider retains
only event metadata; company descriptions and response bodies never enter the
returned model or cache.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import re
from threading import RLock
from urllib.error import HTTPError
from urllib.parse import parse_qsl, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .events import EarningsEvent, JsonTransport, _symbol


NASDAQ_EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"
NASDAQ_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/0.1"
)
MAXIMUM_NASDAQ_RESPONSE_BYTES = 2 * 1024 * 1024
MAXIMUM_WINDOW_DAYS = 14
DEFAULT_TIMEOUT_SECONDS = 8.0
DEFAULT_CACHE_TTL = timedelta(minutes=15)

_ALLOWED_HOST = "api.nasdaq.com"
_ALLOWED_PATH = "/api/calendar/earnings"
_ALLOWED_CONTENT_TYPES = frozenset({"application/json"})
_ALLOWED_SESSIONS = {
    "time-pre-market": ("PRE_MARKET", "bmo"),
    "time-after-hours": ("AFTER_MARKET", "amc"),
    "time-not-supplied": ("NOT_SUPPLIED", None),
}
_FAILURE_REASONS = frozenset(
    {
        "AS_OF_INVALID",
        "AS_OF_MISSING",
        "DATA_NOT_OBJECT",
        "DATE_MISMATCH",
        "DUPLICATE_SYMBOL_DATE",
        "HTTP_ERROR",
        "INVALID_RESPONSE",
        "REQUEST_FAILED",
        "REQUEST_TIMEOUT",
        "ROOT_NOT_OBJECT",
        "ROWS_NOT_ARRAY",
        "ROW_NOT_OBJECT",
        "UNEXPECTED_FAILURE",
        "UNSUPPORTED_SESSION",
    }
)
_CREDENTIAL_HEADER = re.compile(
    r"(?:authorization|proxy-authorization|cookie|token|secret|api[_-]?key)",
    re.I,
)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise HTTPError(
            req.full_url,
            code,
            "Nasdaq earnings redirects are disabled",
            headers,
            fp,
        )


class _NasdaqPayloadError(ValueError):
    """Schema failure carrying only a fixed, non-sensitive reason code."""

    def __init__(self, reason: str, message: str) -> None:
        if reason not in _FAILURE_REASONS:
            raise ValueError("unsupported Nasdaq earnings failure reason")
        super().__init__(message)
        self.reason = reason


class NasdaqHttpsTransport:
    """Bounded GET-only transport for one exact public Nasdaq endpoint."""

    def __init__(self, *, opener: object | None = None) -> None:
        # Honour only the proxy environment inherited by this isolated
        # Options Copilot process.  The exact host/path/query allowlist and
        # no-redirect handler still prevent this public metadata request from
        # escaping its declared network boundary.
        self._opener = opener or build_opener(ProxyHandler(), _NoRedirect())

    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> object:
        checked_url = _allowed_nasdaq_url(url)
        timeout = _timeout(timeout_seconds)
        if not isinstance(headers, Mapping):
            raise TypeError("headers must be a mapping")
        for raw_name in headers:
            name = str(raw_name).strip()
            if _CREDENTIAL_HEADER.search(name):
                raise ValueError("credential headers are forbidden")
            if name.lower() != "accept":
                raise ValueError("only the Accept request header is permitted")
        accept = str(headers.get("Accept") or "application/json").strip()
        if accept != "application/json":
            raise ValueError("Nasdaq earnings transport requires application/json")

        request = Request(
            checked_url,
            data=None,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "User-Agent": NASDAQ_USER_AGENT,
            },
            method="GET",
        )
        response = self._opener.open(request, timeout=timeout)  # type: ignore[attr-defined]
        with response:
            if int(getattr(response, "status", 200)) != 200:
                raise OSError("Nasdaq earnings response was not HTTP 200")
            final_url = str(response.geturl())
            if _allowed_nasdaq_url(final_url) != checked_url:
                raise ValueError("Nasdaq earnings redirect changed the exact URL")
            response_headers = response.headers
            content_encoding = str(
                response_headers.get("Content-Encoding") or "identity"
            ).strip().lower()
            if content_encoding not in {"", "identity"}:
                raise ValueError("compressed Nasdaq earnings responses are rejected")
            raw_length = response_headers.get("Content-Length")
            if raw_length is not None:
                try:
                    declared_length = int(raw_length)
                except (TypeError, ValueError) as exc:
                    raise ValueError("invalid Nasdaq earnings Content-Length") from exc
                if (
                    declared_length < 0
                    or declared_length > MAXIMUM_NASDAQ_RESPONSE_BYTES
                ):
                    raise ValueError("Nasdaq earnings response exceeds size limit")
            content_type = str(response_headers.get("Content-Type") or "").lower()
            media_type = content_type.split(";", 1)[0].strip()
            if media_type not in _ALLOWED_CONTENT_TYPES:
                raise ValueError("unexpected Nasdaq earnings content type")
            charset_match = re.search(
                r"charset\s*=\s*[\"']?([^;\s\"']+)", content_type
            )
            charset = (charset_match.group(1) if charset_match else "utf-8").lower()
            if charset not in {"utf-8", "utf8", "us-ascii", "ascii"}:
                raise ValueError("unsupported Nasdaq earnings charset")
            body = response.read(MAXIMUM_NASDAQ_RESPONSE_BYTES + 1)
            if not isinstance(body, bytes):
                raise TypeError("Nasdaq earnings response body must be bytes")
            if len(body) > MAXIMUM_NASDAQ_RESPONSE_BYTES:
                raise ValueError("Nasdaq earnings response exceeds size limit")
        try:
            return json.loads(
                body.decode(charset, errors="strict").lstrip("\ufeff"),
                object_pairs_hook=_object_without_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Nasdaq earnings JSON is invalid") from exc


@dataclass(frozen=True, slots=True)
class NasdaqEarningsEvent(EarningsEvent):
    """Runtime-compatible earnings event with Nasdaq-specific audit facts."""

    report_session: str = "NOT_SUPPLIED"
    is_estimated: bool = True
    source_url: str = NASDAQ_EARNINGS_URL

    def __post_init__(self) -> None:
        EarningsEvent.__post_init__(self)
        if self.report_session not in {
            "PRE_MARKET",
            "AFTER_MARKET",
            "NOT_SUPPLIED",
        }:
            raise ValueError("unsupported Nasdaq earnings report_session")
        if self.is_estimated is not True:
            raise ValueError("Nasdaq earnings calendar rows must remain estimated")
        checked_url = _allowed_nasdaq_url(self.source_url)
        query_day = date.fromisoformat(checked_url.rsplit("=", 1)[1])
        if query_day != self.report_date:
            raise ValueError("Nasdaq source URL date does not match report_date")
        object.__setattr__(self, "source_url", checked_url)
        object.__setattr__(
            self,
            "content_hash",
            canonical_hash(
                {
                    "symbol": self.symbol,
                    "report_date": self.report_date,
                    "hour": self.hour,
                    "report_session": self.report_session,
                    "is_estimated": self.is_estimated,
                    "eps_estimate": self.eps_estimate,
                    "revenue_estimate": self.revenue_estimate,
                    "source_url": self.source_url,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class NasdaqEarningsDayFailure:
    """Auditable failure for one requested day without remote error text."""

    report_date: date
    reason: str
    source_url: str

    def __post_init__(self) -> None:
        if not isinstance(self.report_date, date) or isinstance(
            self.report_date, datetime
        ):
            raise TypeError("report_date must be a date")
        if self.reason not in _FAILURE_REASONS:
            raise ValueError("unsupported Nasdaq earnings failure reason")
        checked_url = _allowed_nasdaq_url(self.source_url)
        query_day = date.fromisoformat(checked_url.rsplit("=", 1)[1])
        if query_day != self.report_date:
            raise ValueError("Nasdaq failure URL date does not match report_date")
        object.__setattr__(self, "source_url", checked_url)


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    fetched_at: datetime
    events: tuple[NasdaqEarningsEvent, ...]


class NasdaqEarningsProvider:
    """Fetch an inclusive window while auditing every failed requested day."""

    def __init__(
        self,
        *,
        transport: JsonTransport | None = None,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        cache_ttl: timedelta = DEFAULT_CACHE_TTL,
    ) -> None:
        if not isinstance(cache_ttl, timedelta):
            raise TypeError("cache_ttl must be a timedelta")
        if cache_ttl <= timedelta(0) or cache_ttl > timedelta(days=1):
            raise ValueError("cache_ttl must be positive and at most one day")
        self._transport = transport or NasdaqHttpsTransport()
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._timeout_seconds = _timeout(timeout_seconds)
        self._cache_ttl = cache_ttl
        self._cache: dict[date, _CacheEntry] = {}
        self._lock = RLock()
        self.health = "DEGRADED"
        self.health_reason: str | None = "NOT_OBSERVED"
        self.failures: tuple[NasdaqEarningsDayFailure, ...] = ()

    @property
    def failed_dates(self) -> tuple[date, ...]:
        return tuple(item.report_date for item in self.failures)

    def earnings_calendar(
        self,
        start: date,
        end: date,
    ) -> tuple[NasdaqEarningsEvent, ...]:
        days = _window(start, end)
        with self._lock:
            try:
                observed = utc_datetime(self._now(), field="now")
            except Exception:
                self.health = "DEGRADED"
                self.health_reason = "NASDAQ_EARNINGS_UNAVAILABLE"
                self.failures = ()
                return ()

            resolved: dict[date, tuple[NasdaqEarningsEvent, ...]] = {}
            missing: list[date] = []
            for day in days:
                cached = self._cache.get(day)
                age = None if cached is None else observed - cached.fetched_at
                if (
                    cached is not None
                    and age is not None
                    and timedelta(0) <= age < self._cache_ttl
                ):
                    resolved[day] = cached.events
                else:
                    missing.append(day)

            # Each date is an independent evidence observation.  Successful
            # dates remain usable supporting metadata, while failed dates are
            # omitted and explicitly reported.  No stale cache is substituted
            # for a failed refresh.
            failures: list[NasdaqEarningsDayFailure] = []
            for day in missing:
                source_url = _source_url(day)
                try:
                    payload = self._transport(
                        source_url,
                        headers={"Accept": "application/json"},
                        timeout_seconds=self._timeout_seconds,
                    )
                    day_events = _parse_day(
                        payload,
                        report_date=day,
                        observed_at=observed,
                        source_url=source_url,
                    )
                    day_events = self._preserve_first_seen(day, day_events)
                except Exception as exc:
                    failures.append(
                        NasdaqEarningsDayFailure(
                            report_date=day,
                            reason=_sanitized_failure_reason(exc),
                            source_url=source_url,
                        )
                    )
                    continue
                entry = _CacheEntry(observed, day_events)
                self._cache[day] = entry
                resolved[day] = entry.events

            events = tuple(
                sorted(
                    (
                        event
                        for day in days
                        for event in resolved.get(day, ())
                    ),
                    key=lambda item: (
                        item.report_date,
                        item.symbol,
                        item.report_session,
                    ),
                )
            )
            self.failures = tuple(failures)
            if failures:
                self.health = "DEGRADED"
                self.health_reason = (
                    "NASDAQ_EARNINGS_PARTIAL_WINDOW"
                    if resolved
                    else "NASDAQ_EARNINGS_UNAVAILABLE"
                )
            else:
                self.health = "READY"
                self.health_reason = None
            return events

    def _preserve_first_seen(
        self,
        day: date,
        events: tuple[NasdaqEarningsEvent, ...],
    ) -> tuple[NasdaqEarningsEvent, ...]:
        prior = self._cache.get(day)
        if prior is None:
            return events
        by_id = {item.event_id: item for item in prior.events}
        preserved: list[NasdaqEarningsEvent] = []
        for event in events:
            previous = by_id.get(event.event_id)
            if previous is None:
                preserved.append(event)
            else:
                preserved.append(
                    replace(
                        event,
                        first_seen_at=previous.first_seen_at,
                        published_at=previous.published_at,
                    )
                )
        return tuple(preserved)


def _parse_day(
    payload: object,
    *,
    report_date: date,
    observed_at: datetime,
    source_url: str,
) -> tuple[NasdaqEarningsEvent, ...]:
    if not isinstance(payload, Mapping):
        raise _NasdaqPayloadError(
            "ROOT_NOT_OBJECT", "Nasdaq earnings root must be an object"
        )
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise _NasdaqPayloadError(
            "DATA_NOT_OBJECT", "Nasdaq earnings data must be an object"
        )
    raw_as_of = data.get("asOf")
    if not isinstance(raw_as_of, str):
        raise _NasdaqPayloadError(
            "AS_OF_MISSING", "Nasdaq earnings asOf is missing"
        )
    try:
        as_of = datetime.strptime(raw_as_of.strip(), "%a, %b %d, %Y").date()
    except ValueError as exc:
        raise _NasdaqPayloadError(
            "AS_OF_INVALID", "Nasdaq earnings asOf is invalid"
        ) from exc
    if as_of != report_date:
        raise _NasdaqPayloadError(
            "DATE_MISMATCH",
            "Nasdaq earnings response date does not match request",
        )
    rows = data.get("rows")
    if rows is None and "rows" in data and _verified_empty_response(payload):
        return ()
    if not isinstance(rows, Sequence) or isinstance(
        rows, (str, bytes, bytearray, memoryview)
    ):
        raise _NasdaqPayloadError(
            "ROWS_NOT_ARRAY", "Nasdaq earnings rows must be an array"
        )

    results: list[NasdaqEarningsEvent] = []
    seen: set[str] = set()
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise _NasdaqPayloadError(
                "ROW_NOT_OBJECT", "Nasdaq earnings row must be an object"
            )
        symbol = _symbol(str(raw.get("symbol") or ""))
        raw_session = str(raw.get("time") or "").strip().lower()
        try:
            report_session, hour = _ALLOWED_SESSIONS[raw_session]
        except KeyError as exc:
            raise _NasdaqPayloadError(
                "UNSUPPORTED_SESSION",
                "Nasdaq earnings row has unsupported time",
            ) from exc
        event_id = f"nasdaq-earnings-{report_date.isoformat()}-{symbol.lower()}"
        if event_id in seen:
            raise _NasdaqPayloadError(
                "DUPLICATE_SYMBOL_DATE",
                "Nasdaq earnings response has duplicate symbol/date",
            )
        seen.add(event_id)
        results.append(
            NasdaqEarningsEvent(
                event_id=event_id,
                symbol=symbol,
                report_date=report_date,
                hour=hour,
                eps_estimate=_eps(raw.get("epsForecast")),
                revenue_estimate=None,
                source="Nasdaq Earnings Calendar",
                first_seen_at=observed_at,
                ingested_at=observed_at,
                observed_at=observed_at,
                source_id=event_id,
                provenance=(source_url,),
                status="ACTIVE",
                decision_authority="SUPPORTING_ONLY",
                published_at=observed_at,
                report_session=report_session,
                is_estimated=True,
                source_url=source_url,
            )
        )
    return tuple(sorted(results, key=lambda item: (item.symbol, item.report_session)))


def _verified_empty_response(payload: Mapping[str, object]) -> bool:
    """Accept only Nasdaq's complete, explicit success envelope for no rows."""

    if "message" not in payload or not _empty_text(payload.get("message")):
        return False
    status = payload.get("status")
    if not isinstance(status, Mapping):
        return False
    if not {"rCode", "developerMessage", "bCodeMessage"}.issubset(status):
        return False
    response_code = status.get("rCode")
    if type(response_code) is not int or response_code != 200:
        return False
    return _empty_text(status.get("developerMessage")) and _empty_business_codes(
        status.get("bCodeMessage")
    )


def _empty_text(value: object) -> bool:
    return value is None or value == ""


def _empty_business_codes(value: object) -> bool:
    return value is None or value == []


def _window(start: date, end: date) -> tuple[date, ...]:
    for name, value in (("start", start), ("end", end)):
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError(f"{name} must be a date")
    if end < start:
        raise ValueError("earnings window end cannot precede start")
    if (end - start).days > MAXIMUM_WINDOW_DAYS:
        raise ValueError("earnings window cannot exceed two weeks")
    return tuple(start + timedelta(days=offset) for offset in range((end - start).days + 1))


def _source_url(day: date) -> str:
    return f"{NASDAQ_EARNINGS_URL}?date={day.isoformat()}"


def _allowed_nasdaq_url(value: object) -> str:
    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
        port = parsed.port
        query = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except (TypeError, ValueError) as exc:
        raise ValueError("Nasdaq earnings URL is invalid") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != _ALLOWED_HOST
        or parsed.netloc != _ALLOWED_HOST
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.path != _ALLOWED_PATH
        or parsed.fragment
        or len(query) != 1
        or query[0][0] != "date"
    ):
        raise ValueError("Nasdaq earnings URL is outside the exact allowlist")
    try:
        report_date = date.fromisoformat(query[0][1])
    except ValueError as exc:
        raise ValueError("Nasdaq earnings URL date is invalid") from exc
    canonical = _source_url(report_date)
    if text != canonical:
        raise ValueError("Nasdaq earnings URL is not canonical")
    return canonical


def _eps(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError("Nasdaq EPS estimate has unsupported numeric type")
    text = str(value).strip()
    if text.lower() in {"", "--", "n/a", "na"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()
    text = text.replace("$", "").replace(",", "").strip()
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Nasdaq EPS estimate is invalid") from exc
    if not parsed.is_finite():
        raise ValueError("Nasdaq EPS estimate must be finite")
    return -parsed if negative else parsed


def _timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("timeout_seconds must be numeric")
    timeout = float(value)
    if not 0 < timeout <= 30:
        raise ValueError("timeout_seconds must be between 0 and 30")
    return timeout


def _sanitized_failure_reason(exc: Exception) -> str:
    """Map arbitrary failures to fixed codes without retaining exception text."""

    if isinstance(exc, _NasdaqPayloadError):
        return exc.reason
    if isinstance(exc, TimeoutError):
        return "REQUEST_TIMEOUT"
    if isinstance(exc, HTTPError):
        return "HTTP_ERROR"
    if isinstance(exc, OSError):
        return "REQUEST_FAILED"
    if isinstance(exc, (TypeError, ValueError, UnicodeError)):
        return "INVALID_RESPONSE"
    return "UNEXPECTED_FAILURE"


def _object_without_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


__all__ = [
    "MAXIMUM_NASDAQ_RESPONSE_BYTES",
    "NASDAQ_EARNINGS_URL",
    "NASDAQ_USER_AGENT",
    "NasdaqEarningsDayFailure",
    "NasdaqEarningsEvent",
    "NasdaqEarningsProvider",
    "NasdaqHttpsTransport",
]
