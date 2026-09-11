"""Production-safe, read-only sources for the US event calendar.

Only fixed public HTTPS documents are fetched.  Parsers return source facts;
they do not infer missing dates or fabricate announcement times.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
import hashlib
from http.client import HTTPException
from html.parser import HTMLParser
import json
import re
import socket
import ssl
from threading import RLock
from time import sleep
import urllib.request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .events import JsonTransport
from .official import (
    OfficialCalendarEvent,
    OfficialCalendarProvider,
    OfficialCalendarSnapshot,
    OfficialCalendarSource,
    OfficialCalendarTransportError,
    OfficialSourceHealth,
)


FEDERAL_RESERVE_FOMC_URL = (
    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
)
FEDERAL_RESERVE_RELEASE_CALENDAR_URL = (
    "https://www.federalreserve.gov/json/calendar.json"
)
BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"
BEA_RELEASE_DATES_URL = "https://apps.bea.gov/API/signup/release_dates.json"
BLS_CALENDAR_URL = "https://www.bls.gov/schedule/2026/home.htm"
BLS_CALENDAR_ICS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
MAXIMUM_OFFICIAL_RESPONSE_BYTES = 2 * 1024 * 1024
OFFICIAL_USER_AGENT = (
    "OptionsCopilot/0.1 read-only-calendar (+https://localhost.invalid/options-copilot)"
)
_ALLOWED_URLS = frozenset(
    {
        FEDERAL_RESERVE_FOMC_URL,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
        BEA_RELEASE_DATES_URL,
        BEA_SCHEDULE_URL,
        BLS_CALENDAR_URL,
        BLS_CALENDAR_ICS_URL,
    }
)
_ALLOWED_CONTENT_TYPES = frozenset(
    {
        "application/json",
        "text/html",
        "application/xhtml+xml",
        "text/calendar",
        "text/plain",
    }
)
_OFFICIAL_ACCEPT_BY_URL = {
    FEDERAL_RESERVE_FOMC_URL: "text/html",
    FEDERAL_RESERVE_RELEASE_CALENDAR_URL: "application/json",
    BEA_SCHEDULE_URL: "text/html",
    BEA_RELEASE_DATES_URL: "application/json",
    BLS_CALENDAR_URL: "text/html",
    BLS_CALENDAR_ICS_URL: "text/calendar, text/plain;q=0.9",
}
_BLS_FEDERAL_HOLIDAYS = frozenset(
    {
        "New Year's Day",
        "Birthday of Martin Luther King, Jr.",
        "Washington's Birthday",
        "Memorial Day",
        "Juneteenth National Independence Day",
        "Independence Day",
        "Labor Day",
        "Columbus Day",
        "Veterans Day",
        "Thanksgiving Day",
        "Christmas Day",
    }
)
_CREDENTIAL_HEADER = re.compile(
    r"(?:authorization|proxy-authorization|cookie|token|secret|api[_-]?key)", re.I
)
_RETRYABLE_TRANSPORT_REASONS = frozenset(
    {
        "CONNECT_ERROR",
        "HTTP_403",
        "HTTP_429",
        "HTTP_5XX",
        "REQUEST_TIMEOUT",
        "TLS_ERROR",
    }
)
_MONTHS = {
    name.lower(): number
    for number, name in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
}
_MONTH_NAMES = {number: name.title() for name, number in _MONTHS.items()}
_VOID_TAGS = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise HTTPError(req.full_url, code, "official calendar redirects are disabled", headers, fp)


class OfficialHttpsTransport:
    """Bounded GET transport for the declared public official URLs."""

    def __init__(
        self,
        *,
        opener: object | None = None,
        opener_factory: Callable[[], object] | None = None,
        system_proxy_opener_factory: Callable[[], object | None] | None = None,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        if opener is not None and opener_factory is not None:
            raise ValueError("opener and opener_factory are mutually exclusive")
        # Honour only the proxy environment inherited by this isolated
        # Options Copilot process.  The exact URL allowlist, no-redirect
        # handler, credential-header ban and bounded response checks below
        # remain the authority boundary.
        self._opener_factory = opener_factory or (
            lambda: build_opener(ProxyHandler(), _NoRedirect())
        )
        self._system_proxy_opener_factory = (
            system_proxy_opener_factory
            if system_proxy_opener_factory is not None
            else (
                None
                if opener is not None or opener_factory is not None
                else _windows_system_proxy_opener
            )
        )
        self._rebuild_bls_opener = opener is None
        self._opener = opener or self._opener_factory()
        self._sleeper = sleeper

    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> str:
        checked_url = _allowed_url(url)
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be numeric")
        timeout = float(timeout_seconds)
        if not 0 < timeout <= 30:
            raise ValueError("timeout_seconds must be between 0 and 30")
        if not isinstance(headers, Mapping):
            raise TypeError("headers must be a mapping")
        for name in headers:
            if _CREDENTIAL_HEADER.search(str(name)):
                raise ValueError("credential headers are forbidden")
            if str(name).strip().lower() != "accept":
                raise ValueError("only the Accept request header is permitted")
        # Each destination has one declared representation.  Sending the
        # aggregator's broad cross-source Accept value to BLS intermittently
        # trips its edge policy with HTTP 403 on long-lived Windows runtimes.
        # Narrowing by the already-validated exact URL is deterministic and
        # cannot expand either the host or content-type boundary.
        accept = _OFFICIAL_ACCEPT_BY_URL[checked_url]
        request = Request(
            checked_url,
            data=None,
            headers={
                "Accept": accept,
                "Accept-Encoding": "identity",
                "User-Agent": OFFICIAL_USER_AGENT,
            },
            method="GET",
        )
        failure_reason: str | None = None
        bls_url = checked_url in {BLS_CALENDAR_URL, BLS_CALENDAR_ICS_URL}
        attempt_limit = 3 if bls_url else 2
        # A Windows system-proxy retry is scoped to this one BLS request.  The
        # transport's base opener remains unchanged so a later Fed or BEA call
        # cannot inherit the BLS fallback route.
        attempt_opener = self._opener
        system_proxy_used = False
        for attempt in range(attempt_limit):
            response, failure_reason, retry_after = _open_official_response(
                attempt_opener,
                request,
                timeout=timeout,
            )
            if response is not None:
                try:
                    return _read_official_response(response, checked_url=checked_url)
                except (
                    HTTPError,
                    URLError,
                    TimeoutError,
                    socket.timeout,
                    ssl.SSLError,
                    HTTPException,
                    OSError,
                ) as exc:
                    failure_reason = _official_transport_failure_reason(exc)
            if (
                attempt + 1 < attempt_limit
                and failure_reason in _RETRYABLE_TRANSPORT_REASONS
            ):
                if bls_url and failure_reason in {"HTTP_403", "HTTP_429"}:
                    if self._rebuild_bls_opener:
                        system_proxy_opener = (
                            None
                            if self._system_proxy_opener_factory is None
                            or system_proxy_used
                            else self._system_proxy_opener_factory()
                        )
                        if system_proxy_opener is not None:
                            attempt_opener = system_proxy_opener
                            system_proxy_used = True
                        else:
                            attempt_opener = self._opener_factory()
                self._sleeper(
                    retry_after
                    if retry_after is not None
                    else min(2.0, 1.0 + attempt)
                )
                continue
            break
        assert failure_reason is not None
        raise OfficialCalendarTransportError(failure_reason)


def _windows_system_proxy_opener() -> object | None:
    """Build one credential-free Windows registry proxy opener if available."""

    reader = getattr(urllib_request, "getproxies_registry", None)
    if not callable(reader):
        return None
    try:
        raw = reader()
    except Exception:
        return None
    if not isinstance(raw, Mapping):
        return None
    proxies: dict[str, str] = {}
    for scheme in ("http", "https"):
        value = str(raw.get(scheme) or "").strip()
        if not value or len(value) > 2048:
            continue
        parsed = urlsplit(value)
        try:
            port = parsed.port
        except ValueError:
            return None
        if (
            parsed.scheme.lower() not in {"http", "https"}
            or not parsed.hostname
            or port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            return None
        proxies[scheme] = value
    if "https" not in proxies:
        return None
    return build_opener(ProxyHandler(proxies), _NoRedirect())


def _read_official_response(response: object, *, checked_url: str) -> str:
    """Validate and consume one HTTP 200 response without retaining its body."""

    with response:
        if _allowed_url(str(response.geturl())) != checked_url:
            raise ValueError("official calendar redirect left the exact allowlist")
        response_headers = response.headers
        content_encoding = str(
            response_headers.get("Content-Encoding") or "identity"
        ).lower()
        if content_encoding not in {"", "identity"}:
            raise ValueError("compressed official calendar responses are not accepted")
        raw_length = response_headers.get("Content-Length")
        if raw_length is not None:
            try:
                declared_length = int(raw_length)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid official calendar Content-Length") from exc
            if declared_length < 0 or declared_length > MAXIMUM_OFFICIAL_RESPONSE_BYTES:
                raise ValueError("official calendar response exceeds size limit")
        content_type = str(response_headers.get("Content-Type") or "").strip().lower()
        media_type = content_type.split(";", 1)[0].strip()
        if media_type not in _ALLOWED_CONTENT_TYPES:
            raise ValueError("unexpected official calendar content type")
        charset_match = re.search(
            r"charset\s*=\s*[\"']?([^;\s\"']+)",
            content_type,
        )
        charset = (charset_match.group(1) if charset_match else "utf-8").lower()
        if charset not in {"utf-8", "utf8", "us-ascii", "ascii"}:
            raise ValueError("unsupported official calendar charset")
        body = response.read(MAXIMUM_OFFICIAL_RESPONSE_BYTES + 1)
        if not isinstance(body, bytes):
            raise TypeError("official calendar response body must be bytes")
        if len(body) > MAXIMUM_OFFICIAL_RESPONSE_BYTES:
            raise ValueError("official calendar response exceeds size limit")
    return body.decode(charset, errors="strict").lstrip("\ufeff")


def _open_official_response(
    opener: object,
    request: Request,
    *,
    timeout: float,
) -> tuple[object | None, str | None, float | None]:
    """Open one GET attempt and retain only a finite redacted failure code."""

    try:
        response = opener.open(request, timeout=timeout)  # type: ignore[attr-defined]
    except Exception as exc:
        return (
            None,
            _official_transport_failure_reason(exc),
            _bounded_retry_after(getattr(exc, "headers", None)),
        )
    try:
        status = int(getattr(response, "status", 200))
    except (TypeError, ValueError):
        _close_response(response)
        return None, "HTTP_ERROR", None
    if status != 200:
        retry_after = _bounded_retry_after(getattr(response, "headers", None))
        _close_response(response)
        return None, _http_failure_reason(status), retry_after
    return response, None, None


def _bounded_retry_after(headers: object) -> float | None:
    """Honor only a small numeric Retry-After window; never sleep unbounded."""

    getter = getattr(headers, "get", None)
    if not callable(getter):
        return None
    try:
        value = float(str(getter("Retry-After") or "").strip())
    except (TypeError, ValueError):
        return None
    return min(2.0, max(1.0, value))


def _close_response(response: object) -> None:
    closer = getattr(response, "close", None)
    if callable(closer):
        try:
            closer()
        except Exception:
            pass


def _official_transport_failure_reason(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        return _http_failure_reason(exc.code)
    if isinstance(exc, URLError):
        reason = exc.reason
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return "REQUEST_TIMEOUT"
        if isinstance(reason, ssl.SSLError):
            return "TLS_ERROR"
        return "CONNECT_ERROR"
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "REQUEST_TIMEOUT"
    if isinstance(exc, ssl.SSLError):
        return "TLS_ERROR"
    if isinstance(exc, (HTTPException, OSError)):
        return "CONNECT_ERROR"
    return "CONNECT_ERROR"


def _http_failure_reason(status: object) -> str:
    try:
        code = int(status)
    except (TypeError, ValueError):
        return "HTTP_ERROR"
    if code == 403:
        return "HTTP_403"
    if code == 429:
        return "HTTP_429"
    if 500 <= code <= 599:
        return "HTTP_5XX"
    return "HTTP_ERROR"


def _allowed_url(value: object) -> str:
    text = str(value or "").strip()
    parsed = urlsplit(text)
    if parsed.scheme.lower() != "https":
        raise ValueError("official calendar transport requires HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("official calendar URL credentials are forbidden")
    if text not in _ALLOWED_URLS:
        raise ValueError("official calendar URL is not on the exact allowlist")
    return text


def _text_payload(payload: object, *, label: str) -> str:
    if isinstance(payload, bytes):
        text = payload.decode("utf-8", errors="strict").lstrip("\ufeff")
    elif isinstance(payload, str):
        text = payload
    else:
        raise TypeError(f"{label} payload must be text")
    if not text.strip() or len(text.encode("utf-8")) > MAXIMUM_OFFICIAL_RESPONSE_BYTES:
        raise ValueError(f"{label} payload is empty or exceeds size limit")
    return text


class _FomcHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.panel_depth: int | None = None
        self.heading_depth: int | None = None
        self.heading_parts: list[str] = []
        self.section_depth: int | None = None
        self.section_seen = False
        self.row_depth: int | None = None
        self.month_depth: int | None = None
        self.date_depth: int | None = None
        self.month_parts: list[str] = []
        self.date_parts: list[str] = []
        self.rows: list[tuple[str, str]] = []
        self.invalid_row = False

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        tag = tag.lower()
        attributes = {str(name).lower(): str(value or "") for name, value in attrs}
        if tag not in _VOID_TAGS:
            self.stack.append(tag)
        depth = len(self.stack)
        if attributes.get("id") == "2026":
            if self.section_seen:
                raise ValueError("duplicate 2026 FOMC section")
            self.section_seen = True
            self.section_depth = depth
        classes = set(attributes.get("class", "").split())
        if tag == "div" and {"panel", "panel-default"} <= classes:
            if self.panel_depth is not None:
                raise ValueError("nested FOMC calendar panel")
            self.panel_depth = depth
            self.heading_parts = []
        if self.panel_depth is not None and tag == "div" and "panel-heading" in classes:
            if self.heading_depth is not None:
                raise ValueError("nested FOMC calendar heading")
            self.heading_depth = depth
        if self.section_depth is None:
            return
        if "fomc-meeting" in classes:
            if self.row_depth is not None:
                raise ValueError("nested 2026 FOMC meeting row")
            self.row_depth = depth
            self.month_parts = []
            self.date_parts = []
        if self.row_depth is not None and "fomc-meeting__month" in classes:
            self.month_depth = depth
        if self.row_depth is not None and "fomc-meeting__date" in classes:
            self.date_depth = depth

    def handle_data(self, data: str) -> None:
        if self.heading_depth is not None:
            self.heading_parts.append(data)
        if self.month_depth is not None:
            self.month_parts.append(data)
        if self.date_depth is not None:
            self.date_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        depth = len(self.stack)
        if self.heading_depth == depth:
            heading = " ".join("".join(self.heading_parts).split())
            if heading == "2026 FOMC Meetings":
                if self.section_seen:
                    raise ValueError("duplicate 2026 FOMC section")
                if self.panel_depth is None:
                    raise ValueError("2026 FOMC heading is outside a calendar panel")
                self.section_seen = True
                self.section_depth = self.panel_depth
            self.heading_depth = None
        if self.month_depth == depth:
            self.month_depth = None
        if self.date_depth == depth:
            self.date_depth = None
        if self.row_depth == depth:
            month = " ".join("".join(self.month_parts).split())
            days = " ".join("".join(self.date_parts).split())
            if not month or not days:
                self.invalid_row = True
            else:
                self.rows.append((month, days))
            self.row_depth = None
        if self.section_depth == depth:
            self.section_depth = None
        if self.panel_depth == depth:
            self.panel_depth = None
            self.heading_depth = None
            self.heading_parts = []
        if tag in self.stack:
            while self.stack:
                popped = self.stack.pop()
                if popped == tag:
                    break


def parse_fomc_2026_html(payload: object) -> Iterable[Mapping[str, object]]:
    """Parse the Fed's 2026 meeting table as date-only decision-day facts."""

    parser = _FomcHtmlParser()
    parser.feed(_text_payload(payload, label="FOMC HTML"))
    parser.close()
    if not parser.section_seen or parser.section_depth is not None or not parser.rows:
        raise ValueError("2026 FOMC section is missing or incomplete")
    if parser.invalid_row:
        raise ValueError("2026 FOMC meeting row is incomplete")
    results: list[dict[str, object]] = []
    identifiers: set[str] = set()
    for raw_month, raw_days in parser.rows:
        month = _MONTHS.get(raw_month.strip().lower())
        match = re.fullmatch(
            r"\s*(\d{1,2})(?:\s*[-\u2013\u2014]\s*(\d{1,2}))?\s*[*\u2020\u2021]*\s*",
            raw_days,
        )
        if month is None or match is None:
            raise ValueError("2026 FOMC meeting row has an unsupported date")
        start_day = int(match.group(1))
        end_day = int(match.group(2) or match.group(1))
        if end_day < start_day:
            raise ValueError("2026 FOMC cross-month row cannot be inferred")
        date(2026, month, start_day)
        meeting_day = date(2026, month, end_day)
        identifier = f"fomc-2026-{month:02d}-{start_day:02d}-{end_day:02d}"
        if identifier in identifiers:
            raise ValueError("duplicate 2026 FOMC meeting row")
        identifiers.add(identifier)
        day_text = str(start_day) if start_day == end_day else f"{start_day}-{end_day}"
        results.append(
            {
                "id": identifier,
                "title": f"FOMC meeting ({_MONTH_NAMES[month]} {day_text}, 2026)",
                "event_date": meeting_day.isoformat(),
                "timezone": "America/New_York",
                "url": FEDERAL_RESERVE_FOMC_URL,
            }
        )
    return tuple(results)


def _federal_reserve_json_object_without_duplicates(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Federal Reserve calendar JSON has duplicate object keys")
        result[key] = value
    return result


def _reject_federal_reserve_json_constant(value: str) -> None:
    raise ValueError(
        f"Federal Reserve calendar JSON has invalid constant {value}"
    )


def _federal_reserve_clock(value: object) -> time:
    rendered = " ".join(str(value or "").strip().lower().split())
    match = re.fullmatch(r"(\d{1,2}):(\d{2})\s+([ap])\.m\.", rendered)
    if match is None:
        raise ValueError("Federal Reserve FOMC calendar row has an invalid time")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if not 1 <= hour <= 12 or not 0 <= minute <= 59:
        raise ValueError("Federal Reserve FOMC calendar row has an invalid time")
    if match.group(3) == "p" and hour != 12:
        hour += 12
    elif match.group(3) == "a" and hour == 12:
        hour = 0
    return time(hour, minute)


def parse_federal_reserve_release_calendar_json(
    payload: object,
) -> Iterable[Mapping[str, object]]:
    """Parse explicit FOMC-minutes release times from the Fed calendar JSON."""

    text = _text_payload(payload, label="Federal Reserve release calendar JSON")
    try:
        document = json.loads(
            text,
            object_pairs_hook=_federal_reserve_json_object_without_duplicates,
            parse_constant=_reject_federal_reserve_json_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Federal Reserve release calendar JSON is invalid") from exc
    if not isinstance(document, dict) or set(document) != {"events", "announcement"}:
        raise ValueError("Federal Reserve release calendar JSON root is invalid")
    events = document.get("events")
    announcements = document.get("announcement")
    if (
        not isinstance(events, list)
        or not events
        or len(events) > 10_000
        or not isinstance(announcements, list)
    ):
        raise ValueError("Federal Reserve release calendar JSON envelope is invalid")

    results: list[dict[str, object]] = []
    identifiers: set[str] = set()
    for row in events:
        if not isinstance(row, Mapping):
            raise ValueError("Federal Reserve release calendar row is invalid")
        if not row:
            continue
        event_type = " ".join(str(row.get("type") or "").strip().upper().split())
        if event_type != "FOMC":
            continue
        title = " ".join(str(row.get("title") or "").strip().split())
        if not title:
            raise ValueError("Federal Reserve FOMC calendar row has no title")
        if title.upper() != "FOMC MINUTES":
            continue

        month_match = re.fullmatch(r"(20\d{2})-(0[1-9]|1[0-2])", str(row.get("month") or "").strip())
        day_match = re.fullmatch(r"(0?[1-9]|[12]\d|3[01])", str(row.get("days") or "").strip())
        if month_match is None or day_match is None:
            raise ValueError("Federal Reserve FOMC calendar row has an invalid date")
        release_date = date(
            int(month_match.group(1)),
            int(month_match.group(2)),
            int(day_match.group(1)),
        )
        release_time = _federal_reserve_clock(row.get("time"))
        identifier = f"fomc-minutes-{release_date.isoformat()}"
        if identifier in identifiers:
            raise ValueError("duplicate FOMC minutes release identity")
        identifiers.add(identifier)
        results.append(
            {
                "id": identifier,
                "title": "FOMC Minutes",
                "scheduled_at": datetime.combine(release_date, release_time).isoformat(),
                "timezone": "America/New_York",
                "url": FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
            }
        )
    if not results:
        raise ValueError("Federal Reserve release calendar has no FOMC minutes rows")
    return tuple(sorted(results, key=lambda row: str(row["scheduled_at"])))


@dataclass
class _HtmlNode:
    tag: str
    attrs: dict[str, str]
    parent: "_HtmlNode | None" = None
    children: list["_HtmlNode"] = field(default_factory=list)
    text_parts: list[str] = field(default_factory=list)

    def text(self) -> str:
        parts = [*self.text_parts]
        for child in self.children:
            parts.append(child.text())
        return " ".join(" ".join(parts).split())


class _DomParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _HtmlNode("document", {})
        self.stack = [self.root]

    def handle_starttag(self, tag: str, attrs) -> None:  # noqa: ANN001
        node = _HtmlNode(
            tag.lower(),
            {str(name).lower(): str(value or "") for name, value in attrs},
            self.stack[-1],
        )
        self.stack[-1].children.append(node)
        if tag.lower() not in _VOID_TAGS:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs) -> None:  # noqa: ANN001
        self.handle_starttag(tag, attrs)
        if tag.lower() not in _VOID_TAGS:
            self.stack.pop()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                return

    def handle_data(self, data: str) -> None:
        self.stack[-1].text_parts.append(data)


def _walk(node: _HtmlNode) -> Iterable[_HtmlNode]:
    for child in node.children:
        yield child
        yield from _walk(child)


def _explicit_html_schedule(node: _HtmlNode) -> str | None:
    if node.tag == "time":
        value = node.attrs.get("datetime")
    elif node.attrs.get("property", "").strip().lower() == "dc:date":
        datatype = node.attrs.get("datatype", "").strip().lower()
        if datatype not in {"", "xsd:date", "xsd:datetime"}:
            return None
        value = node.attrs.get("content")
    else:
        return None
    text = str(value or "").strip()
    if not text or re.fullmatch(
        r"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?)?",
        text,
    ) is None:
        return None
    return text


def _class_nodes(node: _HtmlNode, class_name: str, *, tag: str | None = None) -> list[_HtmlNode]:
    return [
        child
        for child in _walk(node)
        if (tag is None or child.tag == tag)
        and class_name in set(child.attrs.get("class", "").split())
    ]


def parse_bls_schedule_html(payload: object) -> Iterable[Mapping[str, object]]:
    """Parse the fixed official BLS 2026 release tables fail-closed.

    BLS intermittently returns HTTP 403 for its iCalendar export while the
    official annual HTML schedule remains available.  The HTML source is not a
    weaker or inferred calendar: every accepted row must carry an explicit
    Eastern date, time, and description in the declared release table.
    """

    parser = _DomParser()
    parser.feed(_text_payload(payload, label="BLS HTML"))
    parser.close()
    tables = _class_nodes(parser.root, "release-list", tag="table")
    if not tables:
        raise ValueError("BLS release schedule tables were not found")

    results: list[dict[str, object]] = []
    identifiers: set[str] = set()
    for table in tables:
        bodies = [node for node in table.children if node.tag == "tbody"]
        if len(bodies) != 1:
            raise ValueError("BLS release schedule body is incomplete")
        rows = [node for node in bodies[0].children if node.tag == "tr"]
        if not rows:
            raise ValueError("BLS release schedule table has no rows")
        for row in rows:
            date_cells = _class_nodes(row, "date-cell", tag="td")
            time_cells = _class_nodes(row, "time-cell", tag="td")
            description_cells = _class_nodes(row, "desc-cell", tag="td")
            if (
                len(date_cells) != 1
                or len(time_cells) != 1
                or len(description_cells) != 1
            ):
                raise ValueError("BLS release schedule row is incomplete")
            date_text = " ".join(date_cells[0].text().split())
            time_text = " ".join(time_cells[0].text().upper().split())
            description = description_cells[0]
            title_nodes = [node for node in _walk(description) if node.tag == "strong"]
            if len(title_nodes) != 1:
                raise ValueError("BLS release schedule row title is incomplete")
            title_container = title_nodes[0].parent
            if title_container is None:
                raise ValueError("BLS release schedule row title is incomplete")
            title = " ".join(
                (
                    title_nodes[0].text(),
                    " ".join(title_container.text_parts),
                )
            ).strip()
            title = " ".join(title.split())
            if not time_text:
                if title in _BLS_FEDERAL_HOLIDAYS:
                    continue
                raise ValueError("BLS release schedule row has no release time")
            try:
                event_date = datetime.strptime(
                    date_text,
                    "%A, %B %d, %Y",
                ).date()
                event_time = datetime.strptime(time_text, "%I:%M %p").time()
            except ValueError as exc:
                raise ValueError(
                    "BLS release schedule row has an unsupported date or time"
                ) from exc
            if not title:
                raise ValueError("BLS release schedule row title is missing")
            scheduled = datetime.combine(event_date, event_time)
            digest = hashlib.sha256(
                f"{scheduled.isoformat()}\x1f{title}".encode("utf-8")
            ).hexdigest()
            identifier = f"bls-release-{digest[:24]}"
            if identifier in identifiers:
                raise ValueError("BLS release schedule row identity is duplicated")
            identifiers.add(identifier)
            results.append(
                {
                    "id": identifier,
                    "title": title,
                    "scheduled_at": scheduled.isoformat(),
                    "timezone": "America/New_York",
                    "url": BLS_CALENDAR_URL,
                }
            )
    if not results:
        raise ValueError("BLS release schedule has no dated rows")
    return tuple(results)


def _bea_event_url(row: _HtmlNode) -> tuple[str, str]:
    anchors = [item for item in _walk(row) if item.tag == "a" and item.text()]
    if len(anchors) > 1:
        raise ValueError("BEA release row has ambiguous links")
    if not anchors:
        return "", BEA_SCHEDULE_URL
    title = anchors[0].text()
    href = anchors[0].attrs.get("href", "").strip()
    event_url = urljoin(BEA_SCHEDULE_URL, href) if href else BEA_SCHEDULE_URL
    parsed_url = urlsplit(event_url)
    if parsed_url.scheme != "https" or parsed_url.hostname not in {"bea.gov", "www.bea.gov"}:
        raise ValueError("BEA release row URL is not an official HTTPS URL")
    return title, event_url


def _bea_identity(row: _HtmlNode, *, title: str, schedule: str, event_url: str) -> str:
    identifier = row.attrs.get("data-release-id") or row.attrs.get("id")
    if not identifier:
        digest = hashlib.sha256(f"{title}\x1f{schedule}\x1f{event_url}".encode()).hexdigest()
        identifier = f"bea-release-{digest[:24]}"
    return identifier.strip()


def _bea_visible_date(value: str, *, year: int) -> date:
    match = re.fullmatch(
        r"(January|February|March|April|May|June|July|August|September|October|November|December) "
        r"([1-9]|[12]\d|3[01])",
        " ".join(value.split()),
    )
    if match is None:
        raise ValueError("BEA release row has an unsupported visible date")
    return date(year, _MONTHS[match.group(1).lower()], int(match.group(2)))


def _parse_bea_current_table(root: _HtmlNode) -> tuple[dict[str, object], ...] | None:
    tables = [
        node
        for node in _walk(root)
        if node.tag == "table" and node.attrs.get("id") == "release-schedule-table"
    ]
    if not tables:
        return None
    if len(tables) != 1:
        raise ValueError("BEA release schedule table is ambiguous")
    table = tables[0]
    year_headers = [
        node
        for node in _walk(table)
        if node.tag == "th"
        and node.attrs.get("id") == "view-field-scheduled-release-date-1-table-column"
    ]
    title_headers = [
        node
        for node in _walk(table)
        if node.tag == "th"
        and node.attrs.get("id") == "view-field-scheduled-release-subject-table-column"
    ]
    if len(year_headers) != 1 or len(title_headers) != 1:
        raise ValueError("BEA release schedule headers are incomplete")
    year_match = re.fullmatch(r"Year (20\d{2})", year_headers[0].text())
    if year_match is None or title_headers[0].text() != "Release":
        raise ValueError("BEA release schedule headers are unsupported")
    year = int(year_match.group(1))
    bodies = [node for node in table.children if node.tag == "tbody"]
    if len(bodies) != 1:
        raise ValueError("BEA release schedule body is incomplete")
    rows = [node for node in bodies[0].children if node.tag == "tr"]
    if not rows:
        raise ValueError("BEA release schedule rows were not found")

    results: list[dict[str, object]] = []
    identifiers: set[str] = set()
    allowed_row_classes = {"scheduled-releases-type-press", "scheduled-releases-type-data"}
    for row in rows:
        if not set(row.attrs.get("class", "").split()) & allowed_row_classes:
            raise ValueError("BEA release row type is unsupported")
        date_cells = _class_nodes(row, "scheduled-date", tag="td")
        title_cells = _class_nodes(row, "release-title", tag="td")
        if len(date_cells) != 1 or len(title_cells) != 1:
            raise ValueError("BEA release row is incomplete")
        title = title_cells[0].text()
        if not title:
            raise ValueError("BEA release row title is missing")
        visible_schedule = date_cells[0].text()
        if visible_schedule == f"To Be Announced {year}":
            continue
        date_nodes = _class_nodes(date_cells[0], "release-date")
        time_nodes = _class_nodes(date_cells[0], "text-muted", tag="small")
        if len(date_nodes) != 1 or len(time_nodes) > 1:
            raise ValueError("BEA release row schedule is incomplete")
        event_date = _bea_visible_date(date_nodes[0].text(), year=year)
        event_url = BEA_SCHEDULE_URL
        schedule_identity = event_date.isoformat()
        result: dict[str, object] = {
            "title": title,
            "timezone": "America/New_York",
            "url": event_url,
        }
        if time_nodes:
            time_match = re.fullmatch(
                r"(1[0-2]|[1-9]):([0-5]\d) (AM|PM)",
                " ".join(time_nodes[0].text().upper().split()),
            )
            if time_match is None:
                raise ValueError("BEA release row has an unsupported visible time")
            parsed_time = datetime.strptime(
                f"{time_match.group(1)}:{time_match.group(2)} {time_match.group(3)}",
                "%I:%M %p",
            ).time()
            scheduled = datetime.combine(event_date, parsed_time)
            schedule_identity = scheduled.isoformat()
            result["scheduled_at"] = schedule_identity
        else:
            result["event_date"] = schedule_identity
        identifier = _bea_identity(
            row,
            title=title,
            schedule=schedule_identity,
            event_url=event_url,
        )
        if not identifier or len(identifier) > 240 or identifier in identifiers:
            raise ValueError("BEA release row has an invalid or duplicate identity")
        identifiers.add(identifier)
        result["id"] = identifier
        results.append(result)
    if not results:
        raise ValueError("BEA release schedule has no dated rows")
    return tuple(results)


def parse_bea_schedule_html(payload: object) -> Iterable[Mapping[str, object]]:
    """Parse explicitly scheduled BEA release rows; missing schedule is fatal."""

    parser = _DomParser()
    parser.feed(_text_payload(payload, label="BEA HTML"))
    parser.close()
    current_table = _parse_bea_current_table(parser.root)
    if current_table is not None:
        return current_table
    candidates: list[_HtmlNode] = []
    for node in _walk(parser.root):
        classes = set(node.attrs.get("class", "").split())
        if classes & {"release-row", "calendar-row", "views-row"}:
            candidates.append(node)
        elif node.tag == "tr" and any(
            _explicit_html_schedule(child) is not None for child in _walk(node)
        ):
            candidates.append(node)
    if not candidates:
        raise ValueError("BEA release schedule rows were not found")
    results: list[dict[str, object]] = []
    identifiers: set[str] = set()
    for row in candidates:
        schedule_nodes = [
            item for item in _walk(row) if _explicit_html_schedule(item) is not None
        ]
        if len(schedule_nodes) != 1:
            raise ValueError("BEA release row is incomplete")
        raw_schedule = _explicit_html_schedule(schedule_nodes[0])
        assert raw_schedule is not None
        title, event_url = _bea_event_url(row)
        if not title:
            raise ValueError("BEA release row title is missing")
        identifier = _bea_identity(
            row,
            title=title,
            schedule=raw_schedule,
            event_url=event_url,
        )
        if not identifier or len(identifier) > 240 or identifier in identifiers:
            raise ValueError("BEA release row has an invalid or duplicate identity")
        identifiers.add(identifier)
        result: dict[str, object] = {
            "id": identifier,
            "title": title,
            "timezone": "America/New_York",
            "url": event_url,
        }
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_schedule):
            date.fromisoformat(raw_schedule)
            result["event_date"] = raw_schedule
        else:
            try:
                scheduled = datetime.fromisoformat(raw_schedule.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("BEA release row has an invalid explicit schedule") from exc
            result["scheduled_at"] = scheduled.isoformat()
        results.append(result)
    return tuple(results)


def _json_object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("BEA release-date JSON has duplicate object keys")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class BeaReleaseDateAudit:
    duplicate_count: int = 0
    duplicate_record_hashes: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    audit_hash: str | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.duplicate_count, bool)
            or not isinstance(self.duplicate_count, int)
            or self.duplicate_count < 0
        ):
            raise ValueError("duplicate_count must be a non-negative integer")
        hashes = tuple(self.duplicate_record_hashes)
        if len(hashes) != self.duplicate_count or any(
            re.fullmatch(r"[0-9a-f]{64}", item) is None for item in hashes
        ):
            raise ValueError("duplicate_record_hashes do not match duplicate_count")
        warnings = tuple(dict.fromkeys(str(item).strip() for item in self.warnings))
        if any(not item or len(item) > 160 for item in warnings):
            raise ValueError("BEA duplicate warning is invalid")
        if self.duplicate_count and "IDENTICAL_DUPLICATES_FOLDED" not in warnings:
            raise ValueError("folded duplicates require an explicit warning")
        object.__setattr__(self, "duplicate_record_hashes", hashes)
        object.__setattr__(self, "warnings", warnings)
        payload = {
            "duplicate_count": self.duplicate_count,
            "duplicate_record_hashes": hashes,
            "warnings": warnings,
        }
        expected = canonical_hash(payload)
        if self.audit_hash is None:
            object.__setattr__(self, "audit_hash", expected)
        elif self.audit_hash != expected:
            raise ValueError("BEA release-date audit hash does not match")

    def as_dict(self) -> dict[str, object]:
        return {
            "duplicate_count": self.duplicate_count,
            "duplicate_record_hashes": list(self.duplicate_record_hashes),
            "warnings": list(self.warnings),
            "audit_hash": self.audit_hash,
        }


def _parse_bea_release_dates_json(
    payload: object,
) -> tuple[tuple[dict[str, object], ...], BeaReleaseDateAudit]:
    """Return unique business events plus a hash-bound duplicate audit."""

    text = _text_payload(payload, label="BEA release-date JSON")
    try:
        document = json.loads(text, object_pairs_hook=_json_object_without_duplicates)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("BEA release-date JSON is invalid") from exc
    if not isinstance(document, dict) or not document:
        raise ValueError("BEA release-date JSON root must be a non-empty object")
    last_updated = document.get("file_last_updated")
    if not isinstance(last_updated, str):
        raise ValueError("BEA release-date JSON is missing file_last_updated")
    try:
        datetime.fromisoformat(last_updated)
    except ValueError as exc:
        raise ValueError("BEA release-date JSON has invalid file_last_updated") from exc

    results: list[dict[str, object]] = []
    events_by_identity: dict[str, dict[str, object]] = {}
    duplicate_hashes: list[str] = []
    for title, record in document.items():
        if title == "file_last_updated":
            continue
        if not isinstance(title, str) or not title.strip() or len(title.strip()) > 500:
            raise ValueError("BEA release-date JSON has an invalid title")
        if not isinstance(record, dict) or set(record) != {"release_dates"}:
            raise ValueError("BEA release-date JSON has an invalid release record")
        release_dates = record["release_dates"]
        if not isinstance(release_dates, list) or not release_dates:
            raise ValueError("BEA release-date JSON has an invalid release_dates list")
        for raw_timestamp in release_dates:
            if not isinstance(raw_timestamp, str) or not raw_timestamp.strip():
                raise ValueError("BEA release-date JSON has an invalid timestamp")
            try:
                parsed = datetime.fromisoformat(raw_timestamp.strip().replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("BEA release-date JSON has an invalid timestamp") from exc
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError("BEA release-date JSON timestamp has no explicit timezone")
            if parsed.utcoffset().total_seconds() != 0:
                raise ValueError("BEA release-date JSON timestamp is not UTC")
            canonical = parsed.astimezone(timezone.utc).isoformat()
            digest = hashlib.sha256(f"{title.strip()}\x1f{canonical}".encode()).hexdigest()
            identifier = f"bea-release-{digest[:24]}"
            business_event: dict[str, object] = {
                "id": identifier,
                "title": title.strip(),
                "scheduled_at": canonical,
                "timezone": "America/New_York",
                "url": BEA_RELEASE_DATES_URL,
            }
            existing = events_by_identity.get(identifier)
            if existing is not None:
                if existing != business_event:
                    raise ValueError("BEA release-date JSON has a conflicting event identity")
                duplicate_hashes.append(canonical_hash(business_event))
                continue
            events_by_identity[identifier] = business_event
            results.append(business_event)
    if not results:
        raise ValueError("BEA release-date JSON has no release timestamps")
    warnings = ("IDENTICAL_DUPLICATES_FOLDED",) if duplicate_hashes else ()
    audit = BeaReleaseDateAudit(
        duplicate_count=len(duplicate_hashes),
        duplicate_record_hashes=tuple(duplicate_hashes),
        warnings=warnings,
    )
    return (
        tuple(sorted(results, key=lambda row: (str(row["scheduled_at"]), str(row["title"])))),
        audit,
    )


def parse_bea_release_dates_json(payload: object) -> Iterable[Mapping[str, object]]:
    """Strictly parse BEA JSON; unaudited callers cannot fold duplicates."""

    rows, audit = _parse_bea_release_dates_json(payload)
    if audit.duplicate_count:
        raise ValueError("BEA identical duplicates require an audited provider envelope")
    return rows


class _AuditedBeaReleaseDatesParser:
    def __init__(self) -> None:
        self.last_audit = BeaReleaseDateAudit()

    def reset(self) -> None:
        self.last_audit = BeaReleaseDateAudit()

    def __call__(self, payload: object) -> Iterable[Mapping[str, object]]:
        rows, audit = _parse_bea_release_dates_json(payload)
        self.last_audit = audit
        return rows


def parse_bls_calendar_ics(payload: object) -> Iterable[Mapping[str, object]]:
    """Strictly parse VEVENT facts from the declared BLS iCalendar feed."""

    text = _text_payload(payload, label="BLS calendar")
    unfolded: list[str] = []
    for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if line.startswith((" ", "\t")):
            if not unfolded:
                raise ValueError("BLS calendar has an invalid folded line")
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)
    events: list[list[str]] = []
    current: list[str] | None = None
    components: list[str] = []
    calendar_seen = False
    calendar_closed = False
    calendar_properties: dict[str, str] = {}
    for line in unfolded:
        if not line.strip():
            continue
        if line.startswith("BEGIN:"):
            component = line.removeprefix("BEGIN:").strip().upper()
            if not component:
                raise ValueError("BLS calendar component is malformed")
            if component == "VCALENDAR":
                if calendar_seen or calendar_closed or components:
                    raise ValueError("BLS calendar has nested or duplicate VCALENDAR")
                calendar_seen = True
                components.append(component)
            elif component == "VEVENT":
                if components != ["VCALENDAR"] or current is not None:
                    raise ValueError("BLS calendar VEVENT is outside VCALENDAR")
                components.append(component)
                current = []
            else:
                if not components or components[0] != "VCALENDAR" or current is not None:
                    raise ValueError("BLS calendar component is outside VCALENDAR")
                components.append(component)
            continue
        if line.startswith("END:"):
            component = line.removeprefix("END:").strip().upper()
            if not components or components[-1] != component:
                raise ValueError("BLS calendar component boundaries are unmatched")
            if component == "VEVENT":
                if current is None:
                    raise ValueError("BLS calendar has unmatched VEVENT")
                events.append(current)
                current = None
            components.pop()
            if component == "VCALENDAR":
                calendar_closed = True
            continue
        if current is not None:
            current.append(line)
            continue
        if components == ["VCALENDAR"]:
            if ":" not in line:
                raise ValueError("BLS calendar property is malformed")
            raw_name, value = line.split(":", 1)
            name = raw_name.split(";", 1)[0].upper()
            if name in {"VERSION", "PRODID"}:
                if raw_name.upper() != name or name in calendar_properties:
                    raise ValueError(f"BLS calendar has duplicate or invalid {name}")
                calendar_properties[name] = value.strip()
            continue
        if not components:
            raise ValueError("BLS calendar has content outside VCALENDAR")
    if components or current is not None or not calendar_seen or not calendar_closed:
        raise ValueError("BLS calendar VCALENDAR is missing or unbalanced")
    if calendar_properties.get("VERSION") != "2.0":
        raise ValueError("BLS calendar requires exactly VERSION:2.0")
    if not calendar_properties.get("PRODID"):
        raise ValueError("BLS calendar requires a nonblank PRODID")
    if not events:
        raise ValueError("BLS calendar VEVENT records were not found")
    results: list[dict[str, object]] = []
    identifiers: set[str] = set()
    for event in events:
        properties: dict[str, tuple[dict[str, str], str]] = {}
        for line in event:
            if ":" not in line:
                raise ValueError("BLS calendar property is malformed")
            raw_name, value = line.split(":", 1)
            pieces = raw_name.split(";")
            name = pieces[0].upper()
            params: dict[str, str] = {}
            for piece in pieces[1:]:
                if "=" not in piece:
                    raise ValueError("BLS calendar parameter is malformed")
                key, parameter_value = piece.split("=", 1)
                params[key.upper()] = parameter_value
            if name in {"UID", "SUMMARY", "DTSTART", "URL"}:
                if name in properties:
                    raise ValueError(f"BLS calendar has duplicate {name}")
                properties[name] = (params, value.strip())
        if not {"UID", "SUMMARY", "DTSTART"} <= set(properties):
            raise ValueError("BLS calendar VEVENT requires UID, SUMMARY, and DTSTART")
        identifier = properties["UID"][1]
        title = _ics_text(properties["SUMMARY"][1])
        if not identifier or not title or identifier in identifiers:
            raise ValueError("BLS calendar identity or title is invalid")
        identifiers.add(identifier)
        dt_params, raw_start = properties["DTSTART"]
        result: dict[str, object] = {"id": identifier, "title": title}
        if dt_params.get("VALUE", "").upper() == "DATE" or re.fullmatch(r"\d{8}", raw_start):
            parsed_date = datetime.strptime(raw_start, "%Y%m%d").date()
            result.update(event_date=parsed_date.isoformat(), timezone="America/New_York")
        else:
            is_utc = raw_start.endswith("Z")
            value = raw_start[:-1] if is_utc else raw_start
            try:
                parsed = datetime.strptime(value, "%Y%m%dT%H%M%S")
            except ValueError as exc:
                raise ValueError("BLS calendar DTSTART is invalid") from exc
            if is_utc:
                result["scheduled_at"] = parsed.isoformat() + "+00:00"
                result["timezone"] = "UTC"
            else:
                timezone_name = dt_params.get("TZID", "America/New_York")
                if timezone_name not in {"America/New_York", "US-Eastern"}:
                    raise ValueError("BLS calendar DTSTART timezone is unsupported")
                result["scheduled_at"] = parsed.isoformat()
                # BLS currently publishes the legacy IANA link
                # ``US-Eastern``.  Persist the canonical zone name so the
                # evidence model does not fork identities for the same clock.
                result["timezone"] = "America/New_York"
        if "URL" in properties:
            event_url = properties["URL"][1]
            parsed_url = urlsplit(event_url)
            if parsed_url.scheme != "https" or parsed_url.hostname not in {"bls.gov", "www.bls.gov"}:
                raise ValueError("BLS calendar URL is not an official HTTPS URL")
            result["url"] = event_url
        results.append(result)
    return tuple(results)


def _ics_text(value: str) -> str:
    return (
        value.replace("\\n", " ")
        .replace("\\N", " ")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
        .strip()
    )


@dataclass(frozen=True, slots=True)
class AuditedOfficialSourceHealth(OfficialSourceHealth):
    """Source health with a separately hash-bound machine-feed duplicate audit."""

    duplicate_audit: BeaReleaseDateAudit = field(default_factory=BeaReleaseDateAudit)

    def __post_init__(self) -> None:
        OfficialSourceHealth.__post_init__(self)
        if not isinstance(self.duplicate_audit, BeaReleaseDateAudit):
            raise TypeError("duplicate_audit must be a BeaReleaseDateAudit")

    def as_dict(self) -> dict[str, object]:
        return {
            **OfficialSourceHealth.as_dict(self),
            **self.duplicate_audit.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class OfficialReactionSchedule:
    """Longer-horizon, supporting-only schedule retained before public filtering."""

    status: str
    observed_at: datetime
    horizon_end: datetime
    events: tuple[OfficialCalendarEvent, ...]
    reasons: tuple[str, ...]
    schedule_hash: str = ""

    def __post_init__(self) -> None:
        observed = utc_datetime(self.observed_at, field="observed_at")
        horizon = utc_datetime(self.horizon_end, field="horizon_end")
        if self.status not in {"READY", "DEGRADED"} or horizon <= observed:
            raise ValueError("official reaction schedule is invalid")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "horizon_end", horizon)
        object.__setattr__(self, "events", tuple(self.events))
        object.__setattr__(self, "reasons", tuple(dict.fromkeys(self.reasons)))
        document = {
            "schema": "options_copilot.official_reaction_schedule.v1",
            "status": self.status,
            "observed_at": observed.isoformat(),
            "horizon_end": horizon.isoformat(),
            "event_record_hashes": [item.record_hash for item in self.events],
            "reasons": list(self.reasons),
            "decision_authority": "SUPPORTING_ONLY",
        }
        expected = canonical_hash(document)
        if self.schedule_hash and self.schedule_hash != expected:
            raise ValueError("official reaction schedule hash mismatch")
        object.__setattr__(self, "schedule_hash", expected)


def _event_sort_key(event: OfficialCalendarEvent) -> tuple[date, time, str, str]:
    if event.event_date is None:
        raise ValueError("official event has no event_date")
    event_time = (
        time.max
        if event.scheduled_at is None
        else event.scheduled_at.astimezone(
            ZoneInfo(event.timezone_name) if event.timezone_name else timezone.utc
        ).time()
    )
    return event.event_date, event_time, event.source, event.source_id


class ControlledOfficialCalendarProvider:
    """Select bounded official fallbacks without weakening source provenance."""

    def __init__(
        self,
        *,
        transport: JsonTransport,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._transport = transport
        self._timeout_seconds = timeout_seconds
        self._lock = RLock()
        declared = {item.source_url: item for item in build_official_calendar_sources()}
        fed_sources = (
            declared[FEDERAL_RESERVE_FOMC_URL],
            declared[FEDERAL_RESERVE_RELEASE_CALENDAR_URL],
        )
        bls_source = declared[BLS_CALENDAR_URL]
        machine_template = declared[BEA_RELEASE_DATES_URL]
        self._machine_parser = _AuditedBeaReleaseDatesParser()
        machine_source = OfficialCalendarSource(
            source=machine_template.source,
            source_url=machine_template.source_url,
            category=machine_template.category,
            parser=self._machine_parser,
            timezone_name=machine_template.timezone_name,
            symbols=machine_template.symbols,
        )
        fallback_source = OfficialCalendarSource(
            source="Bureau of Economic Analysis HTML Fallback",
            source_url=BEA_SCHEDULE_URL,
            category="MACRO",
            parser=parse_bea_schedule_html,
            timezone_name="America/New_York",
        )
        provider_kwargs = {
            "transport": transport,
            "now": self._now,
            "timeout_seconds": timeout_seconds,
        }
        self._core_provider = OfficialCalendarProvider(
            sources=fed_sources,
            **provider_kwargs,
        )
        self._bls_primary_provider = OfficialCalendarProvider(
            sources=(bls_source,),
            **provider_kwargs,
        )
        self._bls_fallback_provider = OfficialCalendarProvider(
            sources=(
                OfficialCalendarSource(
                    source="Bureau of Labor Statistics",
                    source_url=BLS_CALENDAR_ICS_URL,
                    category="MACRO",
                    parser=parse_bls_calendar_ics,
                    timezone_name="America/New_York",
                ),
            ),
            **provider_kwargs,
        )
        self._machine_provider = OfficialCalendarProvider(
            sources=(machine_source,),
            **provider_kwargs,
        )
        self._fallback_provider = OfficialCalendarProvider(
            sources=(fallback_source,),
            **provider_kwargs,
        )
        self.health = "DEGRADED"
        self.health_reason: str | None = "not_observed"

    def fork_reaction_schedule_provider(self) -> "ControlledOfficialCalendarProvider":
        """Return an independently locked provider for the slower schedule lane."""

        return ControlledOfficialCalendarProvider(
            transport=self._transport,
            now=self._now,
            timeout_seconds=self._timeout_seconds,
        )

    def future_two_weeks(self, *, now: datetime | None = None) -> OfficialCalendarSnapshot:
        observed = utc_datetime(now or self._now(), field="now")
        with self._lock:
            core = self._core_provider.future_two_weeks(now=observed)
            bls_primary = self._bls_primary_provider.future_two_weeks(now=observed)
            if bls_primary.status == "READY":
                bls = bls_primary
                bls_reasons = bls_primary.reasons
                bls_health = bls_primary.sources[0]
            else:
                bls_fallback = self._bls_fallback_provider.future_two_weeks(
                    now=observed
                )
                bls = bls_fallback
                bls_reasons = (
                    ()
                    if bls_fallback.status == "READY"
                    else tuple(
                        dict.fromkeys(
                            (*bls_primary.reasons, *bls_fallback.reasons)
                        )
                    )
                )
                fallback_health = bls_fallback.sources[0]
                if bls_fallback.status == "READY":
                    primary_health = bls_primary.sources[0]
                    bls_health = replace(
                        fallback_health,
                        content_hash=None,
                        provenance=(
                            f"FALLBACK_SELECTED:{BLS_CALENDAR_ICS_URL}",
                            (
                                f"PRIMARY_UNAVAILABLE:{BLS_CALENDAR_URL}:"
                                f"{primary_health.reason}"
                            ),
                        ),
                    )
                else:
                    bls_health = fallback_health
            self._machine_parser.reset()
            machine = self._machine_provider.future_two_weeks(now=observed)
            if len(machine.sources) != 1:
                raise RuntimeError("BEA machine provider returned invalid source health")
            raw_machine_health = machine.sources[0]
            machine_health = AuditedOfficialSourceHealth(
                source=raw_machine_health.source,
                source_url=raw_machine_health.source_url,
                status=raw_machine_health.status,
                reason=raw_machine_health.reason,
                observed_at=raw_machine_health.observed_at,
                event_count=raw_machine_health.event_count,
                content_hash=raw_machine_health.content_hash,
                duplicate_audit=self._machine_parser.last_audit,
            )

            fallback: OfficialCalendarSnapshot | None = None
            if machine.status == "READY":
                bea_events = machine.events
                bea_health: tuple[OfficialSourceHealth, ...] = (machine_health,)
                reasons = (*core.reasons, *machine.reasons, *bls_reasons)
            else:
                fallback = self._fallback_provider.future_two_weeks(now=observed)
                bea_events = fallback.events
                bea_health = (machine_health, *fallback.sources)
                reasons = (
                    *core.reasons,
                    *machine.reasons,
                    *fallback.reasons,
                    *bls_reasons,
                    "BEA_FALLBACK_ACTIVE",
                )

            sources = (
                *core.sources,
                *bea_health,
                bls_health,
            )
            unique_reasons = tuple(dict.fromkeys(reasons))
            status = "READY" if not unique_reasons else "DEGRADED"
            decision = "OBSERVATION_ONLY" if status == "READY" else "NO_TRADE"
            self.health = status
            self.health_reason = None if status == "READY" else "official_source_degraded"
            return OfficialCalendarSnapshot(
                status=status,
                decision=decision,
                window_start=observed,
                window_end=core.window_end,
                observed_at=observed,
                events=tuple(
                    sorted(
                        (*core.events, *bea_events, *bls.events),
                        key=_event_sort_key,
                    )
                ),
                sources=sources,
                reasons=unique_reasons,
            )

    def calendar_payload(self) -> dict[str, object]:
        return self.future_two_weeks().as_dict()

    def reaction_schedule(
        self,
        *,
        now: datetime | None = None,
        horizon_days: int = 120,
    ) -> OfficialReactionSchedule:
        """Fetch a separate bounded schedule for next-release reaction planning."""

        observed = utc_datetime(now or self._now(), field="now")
        with self._lock:
            core_events, core_health = self._core_provider.bounded_events(
                now=observed,
                horizon_days=horizon_days,
            )
            bls_events, bls_health = self._bls_primary_provider.bounded_events(
                now=observed,
                horizon_days=horizon_days,
            )
            machine_events, machine_health = self._machine_provider.bounded_events(
                now=observed,
                horizon_days=horizon_days,
            )
            # The BEA machine feed is authoritative for exact timestamps but
            # intentionally exposes generic release names.  The independent
            # official HTML schedule carries the quarter/month and GDP
            # estimate label required to bind a release document without
            # guessing.  Keep both provenance-distinct rows in the reaction
            # schedule; only a descriptor with explicit official precision is
            # capture eligible downstream.
            bea_detail_events, bea_detail_health = (
                self._fallback_provider.bounded_events(
                    now=observed,
                    horizon_days=horizon_days,
                )
            )
        health = (
            *core_health,
            *bls_health,
            *machine_health,
            *bea_detail_health,
        )
        reasons = tuple(
            f"{item.source}:{item.reason}"
            for item in health
            if item.status != "READY" and item.reason is not None
        )
        events = tuple(
            sorted(
                (
                    *core_events,
                    *bls_events,
                    *machine_events,
                    *bea_detail_events,
                ),
                key=_event_sort_key,
            )
        )
        return OfficialReactionSchedule(
            status="READY" if not reasons else "DEGRADED",
            observed_at=observed,
            horizon_end=observed + timedelta(days=horizon_days),
            events=events,
            reasons=reasons,
        )


def build_official_calendar_sources() -> tuple[OfficialCalendarSource, ...]:
    return (
        OfficialCalendarSource(
            source="Federal Reserve",
            source_url=FEDERAL_RESERVE_FOMC_URL,
            category="FOMC",
            parser=parse_fomc_2026_html,
            timezone_name="America/New_York",
        ),
        OfficialCalendarSource(
            source="Federal Reserve Release Calendar",
            source_url=FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
            category="FOMC",
            parser=parse_federal_reserve_release_calendar_json,
            timezone_name="America/New_York",
        ),
        OfficialCalendarSource(
            source="Bureau of Economic Analysis",
            source_url=BEA_RELEASE_DATES_URL,
            category="MACRO",
            parser=parse_bea_release_dates_json,
            timezone_name="America/New_York",
        ),
        OfficialCalendarSource(
            source="Bureau of Labor Statistics",
            source_url=BLS_CALENDAR_URL,
            category="MACRO",
            parser=parse_bls_schedule_html,
            timezone_name="America/New_York",
        ),
    )


def build_official_calendar_provider(
    *,
    transport: JsonTransport | None = None,
    now: Callable[[], datetime] | None = None,
    timeout_seconds: float = 8.0,
) -> ControlledOfficialCalendarProvider:
    return ControlledOfficialCalendarProvider(
        transport=transport if transport is not None else OfficialHttpsTransport(),
        now=now,
        timeout_seconds=timeout_seconds,
    )


__all__ = [
    "AuditedOfficialSourceHealth",
    "BEA_RELEASE_DATES_URL",
    "BEA_SCHEDULE_URL",
    "BLS_CALENDAR_ICS_URL",
    "BLS_CALENDAR_URL",
    "BeaReleaseDateAudit",
    "ControlledOfficialCalendarProvider",
    "FEDERAL_RESERVE_FOMC_URL",
    "FEDERAL_RESERVE_RELEASE_CALENDAR_URL",
    "MAXIMUM_OFFICIAL_RESPONSE_BYTES",
    "OFFICIAL_USER_AGENT",
    "OfficialHttpsTransport",
    "OfficialReactionSchedule",
    "build_official_calendar_provider",
    "build_official_calendar_sources",
    "parse_bea_schedule_html",
    "parse_bea_release_dates_json",
    "parse_bls_calendar_ics",
    "parse_bls_schedule_html",
    "parse_federal_reserve_release_calendar_json",
    "parse_fomc_2026_html",
]
