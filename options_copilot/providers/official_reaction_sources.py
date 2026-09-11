"""Bounded GET-only official release-document adapters for G040 event families."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import hashlib
import re
from time import sleep
from types import MappingProxyType
from urllib.parse import urlsplit
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from xml.etree import ElementTree

from options_copilot.news.reaction_specs import (
    EventFamily,
    EventRole,
    FAMILY_SPECS,
    SUPPORTING_ONLY,
    ScheduledReactionSpec,
)
from options_copilot.storage.canonical import canonical_hash, utc_datetime


class OfficialReactionSourceError(RuntimeError):
    """Credential-free stable failure code for an official reaction source."""


def _retry_after_seconds(headers: object) -> int | None:
    value = None
    if headers is not None:
        getter = getattr(headers, "get", None)
        if callable(getter):
            value = getter("Retry-After")
    text = str(value or "").strip()
    return int(text) if text in {"1", "2"} else None


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


@dataclass(frozen=True, slots=True)
class SourcePolicy:
    host: str
    path_patterns: tuple[re.Pattern[str], ...]
    media_types: tuple[str, ...]
    maximum_bytes: int = 2 * 1024 * 1024
    timeout_seconds: float = 8.0


@dataclass(frozen=True, slots=True)
class OfficialDocument:
    family: EventFamily
    source_role: EventRole
    url: str
    received_at: datetime
    media_type: str
    raw_bytes: bytes
    declared_release_at: datetime | None = None
    first_observed_release_at: datetime | None = None
    charset: str = "utf-8"

    def __post_init__(self) -> None:
        object.__setattr__(self, "received_at", utc_datetime(self.received_at, field="received_at"))
        for field_name in ("declared_release_at", "first_observed_release_at"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, utc_datetime(value, field=field_name))
        if not self.url.startswith("https://") or not self.raw_bytes:
            raise ValueError("official document must retain HTTPS URL and raw bytes")

    @property
    def raw_hash(self) -> str:
        return hashlib.sha256(self.raw_bytes).hexdigest()

    @property
    def content_hash(self) -> str:
        return canonical_hash({"schema": "options_copilot.official_reaction_document.v1", "family": self.family.value, "source_role": self.source_role.value, "url": self.url, "received_at": self.received_at.isoformat(), "media_type": self.media_type, "raw_hash": self.raw_hash, "declared_release_at": None if self.declared_release_at is None else self.declared_release_at.isoformat(), "first_observed_release_at": None if self.first_observed_release_at is None else self.first_observed_release_at.isoformat(), "decision_authority": SUPPORTING_ONLY})


@dataclass(frozen=True, slots=True)
class ParsedMeasure:
    measure_id: str
    value: Decimal | None
    unit: str
    basis: str
    label: str


@dataclass(frozen=True, slots=True)
class ParsedOfficialRelease:
    family: EventFamily
    reference_period: str
    estimate_label: str | None
    measures: tuple[ParsedMeasure, ...]
    revision_labels: tuple[str, ...]
    document_hash: str
    actual_parse_available: bool = True
    declared_release_at: datetime | None = None
    first_observed_release_at: datetime | None = None
    revision_narrative: str | None = None
    revision_of: str | None = None

    def __post_init__(self) -> None:
        if re.fullmatch(r"[0-9a-f]{64}", self.document_hash) is None:
            raise ValueError("document_hash must be SHA-256")
        if self.revision_of is not None and re.fullmatch(
            r"[0-9a-f]{64}", self.revision_of
        ) is None:
            raise ValueError("revision_of must be an explicit SHA-256 release hash")

    @property
    def decision_authority(self) -> str:
        return SUPPORTING_ONLY


POLICIES: Mapping[EventFamily, SourcePolicy] = MappingProxyType(
    {
        EventFamily.CPI: SourcePolicy("www.bls.gov", (re.compile(r"/news\.release/cpi\.nr0\.htm\Z", re.I),), ("text/html",)),
        EventFamily.PPI: SourcePolicy("www.bls.gov", (re.compile(r"/news\.release/ppi\.nr0\.htm\Z", re.I),), ("text/html",)),
        EventFamily.EMPLOYMENT_SITUATION: SourcePolicy("www.bls.gov", (re.compile(r"/news\.release/empsit\.(?:nr0|toc)\.htm\Z", re.I), re.compile(r"/schedule/news_release/empsit\.htm\Z", re.I)), ("text/html",)),
        EventFamily.PCE: SourcePolicy("www.bea.gov", (re.compile(r"/news/\d{4}/personal-income-and-outlays-[a-z]+-\d{4}\Z", re.I),), ("text/html",)),
        EventFamily.GDP: SourcePolicy("www.bea.gov", (re.compile(r"/news/\d{4}/gross-domestic-product-[a-z0-9-]+\Z", re.I),), ("text/html",)),
        EventFamily.FOMC_STATEMENT: SourcePolicy("www.federalreserve.gov", (re.compile(r"/newsevents/pressreleases/monetary\d{8}a\.htm\Z", re.I),), ("text/html",)),
        EventFamily.FOMC_MINUTES: SourcePolicy("www.federalreserve.gov", (re.compile(r"/newsevents/pressreleases/monetary\d{8}a\.htm\Z", re.I), re.compile(r"/monetarypolicy/fomcminutes\d{8}\.htm\Z", re.I)), ("text/html",)),
    }
)


class BoundedOfficialDocumentClient:
    def __init__(
        self,
        *,
        opener: object | None = None,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self._opener = opener or build_opener(ProxyHandler({}), _NoRedirect())
        self._sleeper = sleeper

    def fetch(self, family: EventFamily, url: str, *, received_at: datetime, declared_release_at: datetime) -> OfficialDocument:
        policy = POLICIES.get(family)
        parsed = urlsplit(url)
        if policy is None or parsed.scheme != "https" or parsed.hostname != policy.host or parsed.port not in {None, 443} or parsed.query or parsed.fragment or not any(pattern.fullmatch(parsed.path) for pattern in policy.path_patterns):
            raise OfficialReactionSourceError("OFFICIAL_URL_NOT_ALLOWLISTED")
        request = Request(url, headers={"Accept": ", ".join(policy.media_types), "User-Agent": "OptionsCopilot/1.0 official-release-reader"}, method="GET")
        raw = b""
        media_type = ""
        charset = "utf-8"
        for attempt in range(2):
            try:
                with self._opener.open(request, timeout=policy.timeout_seconds) as response:
                    if response.geturl() != url:
                        raise OfficialReactionSourceError("OFFICIAL_REDIRECT_REJECTED")
                    content_type = str(response.headers.get("Content-Type") or "")
                    parts = [item.strip() for item in content_type.split(";")]
                    media_type = parts[0].lower()
                    charset_values = [item.split("=", 1)[1].strip().lower() for item in parts[1:] if item.lower().startswith("charset=")]
                    if media_type not in policy.media_types:
                        raise OfficialReactionSourceError("OFFICIAL_MEDIA_TYPE_INVALID")
                    if len(charset_values) > 1 or (charset_values and charset_values[0] not in {"utf-8", "us-ascii"}):
                        raise OfficialReactionSourceError("OFFICIAL_CHARSET_INVALID")
                    charset = charset_values[0] if charset_values else "utf-8"
                    raw = response.read(policy.maximum_bytes + 1)
                break
            except OfficialReactionSourceError:
                raise
            except HTTPError as exc:
                if exc.code == 403:
                    raise OfficialReactionSourceError("HTTP_403") from None
                if exc.code == 429:
                    retry_after = _retry_after_seconds(exc.headers)
                    if attempt == 0 and retry_after is not None:
                        self._sleeper(retry_after)
                        continue
                    raise OfficialReactionSourceError(
                        "HTTP_429_RETRY_AFTER_INVALID"
                        if retry_after is None
                        else "HTTP_429"
                    ) from None
                if 500 <= exc.code <= 599:
                    if attempt == 0:
                        continue
                    raise OfficialReactionSourceError(f"HTTP_{exc.code}") from None
                raise OfficialReactionSourceError(f"HTTP_{exc.code}") from None
            except (URLError, TimeoutError, OSError):
                if attempt == 0:
                    continue
                raise OfficialReactionSourceError("OFFICIAL_CONNECTION_FAILED") from None
            except Exception:
                raise OfficialReactionSourceError("OFFICIAL_TRANSPORT_FAILED") from None
        if len(raw) > policy.maximum_bytes:
            raise OfficialReactionSourceError("OFFICIAL_RESPONSE_TOO_LARGE")
        return OfficialDocument(family, EventRole.OFFICIAL_RELEASE_DOCUMENT, url, received_at, media_type, raw, declared_release_at=declared_release_at, first_observed_release_at=received_at, charset=charset)


@dataclass(frozen=True, slots=True)
class DiscoveryRecord:
    family: EventFamily
    feed_url: str
    observed_at: datetime
    raw_hash: str | None
    discovered_url: str
    identity_hash: str | None = None

    @property
    def content_hash(self) -> str:
        return canonical_hash({"schema": "options_copilot.official_release_discovery.v2", "family": self.family.value, "feed_url": self.feed_url, "observed_at": utc_datetime(self.observed_at, field="observed_at").isoformat(), "feed_raw_hash": self.raw_hash, "fixed_path_identity_hash": self.identity_hash, "discovered_url": self.discovered_url, "decision_authority": SUPPORTING_ONLY})


@dataclass(frozen=True, slots=True)
class CapturedOfficialRelease:
    request: ScheduledReactionSpec
    discovery: DiscoveryRecord
    document: OfficialDocument
    parsed: ParsedOfficialRelease


class OfficialReleaseCaptureCoordinator:
    """Discover and capture one due official release document without polling loops."""

    def __init__(self, *, document_client: BoundedOfficialDocumentClient | None = None, opener: object | None = None, sleeper: Callable[[float], None] = sleep) -> None:
        self._documents = document_client or BoundedOfficialDocumentClient(opener=opener, sleeper=sleeper)
        self._opener = opener or build_opener(ProxyHandler({}), _NoRedirect())
        self._sleeper = sleeper

    def capture(self, request: ScheduledReactionSpec, *, now: datetime) -> CapturedOfficialRelease:
        checked = utc_datetime(now, field="now")
        if checked < request.scheduled_at:
            raise OfficialReactionSourceError("WAITING_DECLARED_RELEASE_TIME")
        if checked > request.capture_deadline:
            raise OfficialReactionSourceError("WAIT_NEXT_ELIGIBLE_RELEASE:LATE_CAPTURE_DEADLINE")
        family = request.parent.family
        feed_url: str
        feed_hash: str | None
        discovery_identity_hash: str | None = None
        if family in {EventFamily.CPI, EventFamily.PPI, EventFamily.EMPLOYMENT_SITUATION}:
            slug = {EventFamily.CPI: "cpi", EventFamily.PPI: "ppi", EventFamily.EMPLOYMENT_SITUATION: "empsit"}[family]
            url = f"https://www.bls.gov/news.release/{slug}.nr0.htm"
            feed_url = f"https://www.bls.gov/schedule/news_release/{slug}.htm"
            feed_hash = None
            discovery_identity_hash = canonical_hash(
                {
                    "schema": "options_copilot.fixed_path_release_binding.v1",
                    "family": family.value,
                    "feed_url": feed_url,
                    "document_url": url,
                    "reaction_identity_hash": request.official_event_hash,
                }
            )
        elif family in {EventFamily.PCE, EventFamily.GDP}:
            feed_url = "https://apps.bea.gov/rss/rss.xml"
            feed = self._fetch_feed(feed_url, host="apps.bea.gov", path="/rss/rss.xml")
            feed_hash = hashlib.sha256(feed).hexdigest()
            url = _discover_bea_release(feed, request)
        elif family in {EventFamily.FOMC_STATEMENT, EventFamily.FOMC_MINUTES}:
            feed_url = "https://www.federalreserve.gov/feeds/press_all.xml"
            feed = self._fetch_feed(feed_url, host="www.federalreserve.gov", path="/feeds/press_all.xml")
            feed_hash = hashlib.sha256(feed).hexdigest()
            matches = [
                url
                for candidate, _title, url in discover_fomc_documents(feed)
                if candidate is family and _fomc_url_matches_request(url, request)
            ]
            if len(matches) != 1:
                raise OfficialReactionSourceError("OFFICIAL_RELEASE_DOCUMENT_NOT_DISCOVERED")
            url = matches[0]
        else:
            raise OfficialReactionSourceError("OFFICIAL_FAMILY_NOT_CAPTURE_ELIGIBLE")
        document = self._documents.fetch(family, url, received_at=checked, declared_release_at=request.scheduled_at)
        parsed = parse_bound_release(document, request)
        if parsed.family is not family or parsed.reference_period.upper() != request.parent.reference_period:
            raise OfficialReactionSourceError("OFFICIAL_RELEASE_IDENTITY_MISMATCH")
        if family is EventFamily.GDP and parsed.estimate_label != request.parent.estimate_label:
            raise OfficialReactionSourceError("OFFICIAL_ESTIMATE_LABEL_MISMATCH")
        _verify_url_date(document.url, request)
        discovery = DiscoveryRecord(
            family,
            feed_url,
            checked,
            feed_hash,
            url,
            discovery_identity_hash,
        )
        return CapturedOfficialRelease(request, discovery, document, parsed)

    def _fetch_feed(self, url: str, *, host: str, path: str) -> bytes:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != host or parsed.path != path or parsed.query or parsed.fragment:
            raise OfficialReactionSourceError("OFFICIAL_DISCOVERY_URL_INVALID")
        request = Request(url, headers={"Accept": "application/rss+xml, application/xml, text/xml", "User-Agent": "OptionsCopilot/1.0 official-release-reader"}, method="GET")
        raw = b""
        for attempt in range(2):
            try:
                with self._opener.open(request, timeout=8.0) as response:
                    if response.geturl() != url:
                        raise OfficialReactionSourceError("OFFICIAL_REDIRECT_REJECTED")
                    content_type = str(response.headers.get("Content-Type") or "")
                    parts = [item.strip() for item in content_type.split(";")]
                    media = parts[0].lower()
                    charsets = [item.split("=", 1)[1].strip().lower() for item in parts[1:] if item.lower().startswith("charset=")]
                    if media not in {"application/rss+xml", "application/xml", "text/xml"}:
                        raise OfficialReactionSourceError("OFFICIAL_MEDIA_TYPE_INVALID")
                    if len(charsets) > 1 or (charsets and charsets[0] not in {"utf-8", "us-ascii"}):
                        raise OfficialReactionSourceError("OFFICIAL_CHARSET_INVALID")
                    raw = response.read(2 * 1024 * 1024 + 1)
                break
            except OfficialReactionSourceError:
                raise
            except HTTPError as exc:
                if exc.code == 403:
                    raise OfficialReactionSourceError("HTTP_403") from None
                if exc.code == 429:
                    retry_after = _retry_after_seconds(exc.headers)
                    if attempt == 0 and retry_after is not None:
                        self._sleeper(retry_after)
                        continue
                    raise OfficialReactionSourceError(
                        "HTTP_429_RETRY_AFTER_INVALID"
                        if retry_after is None
                        else "HTTP_429"
                    ) from None
                if 500 <= exc.code <= 599 and attempt == 0:
                    continue
                raise OfficialReactionSourceError(f"HTTP_{exc.code}") from None
            except (URLError, TimeoutError, OSError):
                if attempt == 0:
                    continue
                raise OfficialReactionSourceError("OFFICIAL_CONNECTION_FAILED") from None
            except Exception:
                raise OfficialReactionSourceError("OFFICIAL_DISCOVERY_UNAVAILABLE") from None
        if len(raw) > 2 * 1024 * 1024:
            raise OfficialReactionSourceError("OFFICIAL_RESPONSE_TOO_LARGE")
        return raw


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        value = " ".join(data.split())
        if value:
            self.parts.append(value)


def _text(document: OfficialDocument) -> str:
    parser = _TextExtractor()
    try:
        parser.feed(document.raw_bytes.decode(document.charset, errors="strict"))
    except (UnicodeDecodeError, ValueError):
        raise OfficialReactionSourceError("OFFICIAL_DOCUMENT_ENCODING_INVALID") from None
    return " ".join(parser.parts)


def _decimal(value: str) -> Decimal:
    try:
        return Decimal(value.replace(",", ""))
    except InvalidOperation:
        raise OfficialReactionSourceError("OFFICIAL_ACTUAL_PARSE_INVALID") from None


def parse_bls_employment(document: OfficialDocument, *, reference_period: str) -> ParsedOfficialRelease:
    text = _text(document)
    payroll_matches = list(
        re.finditer(
            r"(?:^|[.!?]\s+)Total nonfarm payroll employment "
            r"(?P<direction>increased|rose|gained|declined|decreased|fell|lost) "
            r"by (?P<jobs>\d{1,3}(?:,\d{3})+|\d+)\b",
            text,
            re.I,
        )
    )
    rate_matches = [
        *re.finditer(
            r"(?:^|[.!?]\s+)The unemployment rate "
            r"(?:was|held at|changed little at|edged (?:up|down) to) "
            r"(?P<rate>[\d.]+) percent\b",
            text,
            re.I,
        ),
        *re.finditer(
            r"(?:^|[.!?]\s+)The unemployment rate, at "
            r"(?P<rate>[\d.]+) percent, changed little in "
            r"[A-Za-z]+(?: \d{4})?(?:[.!?]|$)",
            text,
            re.I,
        ),
    ]
    if len(payroll_matches) != 1 or len(rate_matches) != 1:
        raise OfficialReactionSourceError("OFFICIAL_SCHEMA_DRIFT")
    payroll = payroll_matches[0]
    unemployment = rate_matches[0]
    jobs = _decimal(payroll.group("jobs"))
    if jobs % Decimal("1000") != 0:
        raise OfficialReactionSourceError("OFFICIAL_ACTUAL_PARSE_INVALID")
    payroll_value = jobs / Decimal("1000")
    if payroll.group("direction").lower() in {
        "declined",
        "decreased",
        "fell",
        "lost",
    }:
        payroll_value = -payroll_value
    revisions = tuple(dict.fromkeys(match.group(0) for match in re.finditer(r"(?:revised|revision)[^.]{0,240}", text, re.I)))[:10]
    return ParsedOfficialRelease(EventFamily.EMPLOYMENT_SITUATION, reference_period.upper(), None, (ParsedMeasure("total_nonfarm_payroll_change_thousands", payroll_value, "THOUSANDS", "SEASONALLY_ADJUSTED", "Total nonfarm payroll employment"), ParsedMeasure("unemployment_rate_pct", _decimal(unemployment.group("rate")), "PERCENT", "SEASONALLY_ADJUSTED", "Unemployment rate")), revisions, document.raw_hash, declared_release_at=document.declared_release_at, first_observed_release_at=document.first_observed_release_at, revision_narrative=" | ".join(revisions) or None)


def parse_bls_prices(document: OfficialDocument, *, family: EventFamily, reference_period: str) -> ParsedOfficialRelease:
    text = _text(document)
    if family is EventFamily.CPI:
        patterns = (
            ("headline_cpi_mom_pct", r"CPI-U\)?\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent\s+on a seasonally adjusted basis"),
            ("headline_cpi_yoy_pct", r"Over the last 12 months,\s+the all items index\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent"),
            ("core_cpi_mom_pct", r"index for all items less food and energy\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent\s+(?:in|over the month)"),
            ("core_cpi_yoy_pct", r"index for all items less food and energy\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent\s+over the last 12 months"),
        )
    elif family is EventFamily.PPI:
        patterns = (
            ("headline_ppi_mom_pct", r"final demand\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent\s+in"),
            ("headline_ppi_yoy_pct", r"final demand\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent\s+for the 12 months"),
            ("core_ppi_mom_pct", r"final demand less foods?, energy, and trade services\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent\s+in"),
            ("core_ppi_yoy_pct", r"final demand less foods?, energy, and trade services\s+(?:increased|rose|declined|decreased)\s+([\d.]+)\s+percent\s+for the 12 months"),
        )
    else:
        raise OfficialReactionSourceError("OFFICIAL_FAMILY_NOT_PARSEABLE")
    measures: list[ParsedMeasure] = []
    for measure_id, pattern in patterns:
        matches = list(re.finditer(pattern, text, re.I))
        if len(matches) != 1:
            raise OfficialReactionSourceError("OFFICIAL_MEASURE_SET_INCOMPLETE")
        match = matches[0]
        value = _decimal(match.group(1))
        if re.search(r"declined|decreased", match.group(0), re.I):
            value = -value
        spec = next(item for item in FAMILY_SPECS[family].measures if item.measure_id == measure_id)
        measures.append(ParsedMeasure(measure_id, value, spec.unit, spec.basis, spec.label))
    revisions = tuple(dict.fromkeys(match.group(0) for match in re.finditer(r"(?:revised|revision)[^.]{0,240}", text, re.I)))[:10]
    return ParsedOfficialRelease(family, reference_period.upper(), None, tuple(measures), revisions, document.raw_hash, declared_release_at=document.declared_release_at, first_observed_release_at=document.first_observed_release_at, revision_narrative=" | ".join(revisions) or None)


def parse_bea_pce(document: OfficialDocument, *, reference_period: str) -> ParsedOfficialRelease:
    text = _text(document)
    month_section = re.search(r"From the preceding month,(?P<body>.*?)(?=From the same month one year ago,)", text, re.I)
    year_section = re.search(r"From the same month one year ago,(?P<body>.*?)(?=(?:The increase in current-dollar|Personal income|$))", text, re.I)
    if month_section is None or year_section is None:
        raise OfficialReactionSourceError("OFFICIAL_SCHEMA_DRIFT")
    def exact(section: str, core: bool) -> Decimal:
        prefix = r"Excluding food and energy,\s+the PCE price index" if core else r"(?<!energy,\s)the PCE price index"
        matches = re.findall(prefix + r"\s+(?:increased|decreased)\s+([\d.]+)\s+percent", section, re.I)
        if len(matches) != 1:
            raise OfficialReactionSourceError("OFFICIAL_PCE_FIELD_AMBIGUOUS")
        match = re.search(prefix + r"\s+(increased|decreased)\s+([\d.]+)\s+percent", section, re.I)
        assert match is not None
        value = _decimal(match.group(2))
        return -value if match.group(1).lower() == "decreased" else value
    measures = (
        ParsedMeasure("pce_price_index_mom_pct", exact(month_section.group("body"), False), "PERCENT", "SEASONALLY_ADJUSTED", "PCE price index from preceding month"),
        ParsedMeasure("pce_price_index_yoy_pct", exact(year_section.group("body"), False), "PERCENT", "NOT_SEASONALLY_ADJUSTED_YEAR_OVER_YEAR", "PCE price index from year ago"),
        ParsedMeasure("core_pce_price_index_mom_pct", exact(month_section.group("body"), True), "PERCENT", "SEASONALLY_ADJUSTED", "Core PCE price index from preceding month"),
        ParsedMeasure("core_pce_price_index_yoy_pct", exact(year_section.group("body"), True), "PERCENT", "NOT_SEASONALLY_ADJUSTED_YEAR_OVER_YEAR", "Core PCE price index from year ago"),
    )
    revisions = tuple(dict.fromkeys(match.group(0) for match in re.finditer(r"(?:revised|revision)[^.]{0,240}", text, re.I)))[:10]
    return ParsedOfficialRelease(EventFamily.PCE, reference_period.upper(), None, measures, revisions, document.raw_hash, declared_release_at=document.declared_release_at, first_observed_release_at=document.first_observed_release_at, revision_narrative=" | ".join(revisions) or None)


def parse_bea_gdp(document: OfficialDocument, *, reference_period: str) -> ParsedOfficialRelease:
    text = _text(document)
    label_match = re.search(r"(Advance|Second|Third) Estimate", text, re.I)
    value_match = re.search(r"real gross domestic product \(GDP\) (?:increased|decreased) at an annual rate of ([\d.]+) percent", text, re.I)
    if label_match is None or value_match is None:
        raise OfficialReactionSourceError("OFFICIAL_SCHEMA_DRIFT")
    value = _decimal(value_match.group(1))
    if "decreased" in value_match.group(0).lower():
        value = -value
    return ParsedOfficialRelease(EventFamily.GDP, reference_period.upper(), label_match.group(1).upper(), (ParsedMeasure("real_gdp_annual_rate_pct", value, "PERCENT", "SEASONALLY_ADJUSTED_ANNUAL_RATE", "Real GDP percent change"),), (), document.raw_hash, declared_release_at=document.declared_release_at, first_observed_release_at=document.first_observed_release_at)


def parse_fomc_statement(document: OfficialDocument, *, meeting_end_date: str) -> ParsedOfficialRelease:
    text = _text(document)
    unchanged = re.search(r"maintain the target range for the federal funds rate at ([\d.]+) to ([\d.]+) percent", text, re.I)
    changed = re.search(r"(?:raise|lower) the target range for the federal funds rate to ([\d.]+) to ([\d.]+) percent", text, re.I)
    match = unchanged or changed
    if match is None:
        return ParsedOfficialRelease(EventFamily.FOMC_STATEMENT, meeting_end_date.upper(), None, (ParsedMeasure("policy_statement", None, "DOCUMENT", "OFFICIAL_STATEMENT", "Policy statement"),), (), document.raw_hash, actual_parse_available=False, declared_release_at=document.declared_release_at, first_observed_release_at=document.first_observed_release_at)
    label = "UNCHANGED" if unchanged is not None else ("RAISED" if "raise" in match.group(0).lower() else "LOWERED")
    return ParsedOfficialRelease(EventFamily.FOMC_STATEMENT, meeting_end_date.upper(), None, (ParsedMeasure("policy_statement", None, "DOCUMENT", "OFFICIAL_STATEMENT", f"{label}:{match.group(1)}-{match.group(2)}"),), (), document.raw_hash, declared_release_at=document.declared_release_at, first_observed_release_at=document.first_observed_release_at)


def parse_fomc_minutes(document: OfficialDocument, *, meeting_range: str) -> ParsedOfficialRelease:
    text = _text(document)
    if not re.search(r"minutes of the Federal Open Market Committee", text, re.I):
        raise OfficialReactionSourceError("OFFICIAL_SCHEMA_DRIFT")
    return ParsedOfficialRelease(EventFamily.FOMC_MINUTES, meeting_range.upper(), None, (ParsedMeasure("meeting_minutes", None, "DOCUMENT", "OFFICIAL_MINUTES", "Meeting minutes"),), (), document.raw_hash, actual_parse_available=False, declared_release_at=document.declared_release_at, first_observed_release_at=document.first_observed_release_at)


def parse_bound_release(document: OfficialDocument, request: ScheduledReactionSpec) -> ParsedOfficialRelease:
    family = request.parent.family
    if document.family is not family:
        raise OfficialReactionSourceError("OFFICIAL_PARSED_FAMILY_MISMATCH")
    _verify_document_context(_text(document), request)
    if family in {EventFamily.CPI, EventFamily.PPI}:
        parsed = parse_bls_prices(document, family=family, reference_period=request.parent.reference_period)
    elif family is EventFamily.EMPLOYMENT_SITUATION:
        parsed = parse_bls_employment(document, reference_period=request.parent.reference_period)
    elif family is EventFamily.PCE:
        parsed = parse_bea_pce(document, reference_period=request.parent.reference_period)
    elif family is EventFamily.GDP:
        parsed = parse_bea_gdp(document, reference_period=request.parent.reference_period)
    elif family is EventFamily.FOMC_STATEMENT:
        parsed = parse_fomc_statement(document, meeting_end_date=request.parent.reference_period)
    elif family is EventFamily.FOMC_MINUTES:
        parsed = parse_fomc_minutes(document, meeting_range=request.parent.reference_period)
    else:
        raise OfficialReactionSourceError("OFFICIAL_FAMILY_NOT_PARSEABLE")
    expected = {item.measure_id for item in FAMILY_SPECS[family].measures}
    actual = {item.measure_id for item in parsed.measures}
    if actual != expected or len(actual) != len(parsed.measures):
        raise OfficialReactionSourceError("OFFICIAL_MEASURE_SET_INCOMPLETE")
    return parsed


def _verify_url_date(url: str, request: ScheduledReactionSpec) -> None:
    family = request.parent.family
    path = urlsplit(url).path.lower()
    if family is EventFamily.FOMC_STATEMENT:
        match = re.search(r"monetary(\d{8})a\.htm\Z", path)
        expected = request.scheduled_at.strftime("%Y%m%d")
        if match is None or match.group(1) != expected:
            raise OfficialReactionSourceError("OFFICIAL_URL_DATE_MISMATCH")
    elif family is EventFamily.FOMC_MINUTES:
        match = re.search(r"fomcminutes(\d{8})\.htm\Z", path)
        meeting_end = request.parent.reference_period.split("/")[-1].replace("-", "")
        if match is None or match.group(1) != meeting_end:
            raise OfficialReactionSourceError("OFFICIAL_URL_DATE_MISMATCH")
    elif family in {EventFamily.PCE, EventFamily.GDP}:
        year = request.parent.reference_period[:4]
        if f"/news/{request.scheduled_at.year}/" not in path or year not in path:
            raise OfficialReactionSourceError("OFFICIAL_URL_DATE_MISMATCH")
        if family is EventFamily.PCE:
            month = datetime.strptime(request.parent.reference_period, "%Y-%m").strftime("%B").lower()
            if f"-{month}-{year}" not in path:
                raise OfficialReactionSourceError("OFFICIAL_URL_DATE_MISMATCH")
        else:
            quarter = request.parent.reference_period[-1]
            estimate = str(request.parent.estimate_label or "").lower()
            ordinal = {"1": "1st", "2": "2nd", "3": "3rd", "4": "4th"}[quarter]
            word = {"1": "first", "2": "second", "3": "third", "4": "fourth"}[quarter]
            if not any(
                token in path
                for token in (
                    f"-{ordinal}-quarter-",
                    f"-{word}-quarter-",
                    f"-q{quarter}-",
                )
            ) or estimate not in path:
                raise OfficialReactionSourceError("OFFICIAL_URL_DATE_MISMATCH")


def _verify_document_context(text: str, request: ScheduledReactionSpec) -> None:
    family = request.parent.family
    reference = request.parent.reference_period
    if family in {EventFamily.CPI, EventFamily.PPI, EventFamily.EMPLOYMENT_SITUATION, EventFamily.PCE}:
        month = datetime.strptime(reference, "%Y-%m").strftime("%B")
        if re.search(rf"\b{month}\s+{reference[:4]}\b", text, re.I) is None:
            raise OfficialReactionSourceError("OFFICIAL_REFERENCE_PERIOD_MISMATCH")
    elif family is EventFamily.GDP:
        quarter = reference[-1]
        ordinal = {"1": "First", "2": "Second", "3": "Third", "4": "Fourth"}[quarter]
        if re.search(rf"\b(?:{ordinal}|{quarter}(?:st|nd|rd|th))\s+Quarter\s+{reference[:4]}\b", text, re.I) is None:
            raise OfficialReactionSourceError("OFFICIAL_REFERENCE_PERIOD_MISMATCH")
    elif family is EventFamily.FOMC_MINUTES:
        start, end = reference.split("/", 1)
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")
        expected = rf"{start_date.strftime('%B')}\s+{start_date.day}\s*[-–]\s*{end_date.day},\s*{end_date.year}"
        if re.search(expected, text, re.I) is None:
            raise OfficialReactionSourceError("OFFICIAL_MEETING_RANGE_MISMATCH")


def discover_fomc_documents(raw_feed: bytes) -> tuple[tuple[EventFamily, str, str], ...]:
    """Discover only allowlisted statement/minutes links from the official RSS feed."""

    try:
        root = ElementTree.fromstring(raw_feed)
    except ElementTree.ParseError:
        raise OfficialReactionSourceError("OFFICIAL_DISCOVERY_SCHEMA_DRIFT") from None
    output: list[tuple[EventFamily, str, str]] = []
    for item in root.findall(".//item")[:100]:
        title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        family = EventFamily.FOMC_MINUTES if "minutes" in title.lower() else EventFamily.FOMC_STATEMENT if "statement" in title.lower() else None
        if family is None:
            continue
        policy = POLICIES[family]
        parsed = urlsplit(url)
        if parsed.scheme == "https" and parsed.hostname == policy.host and any(pattern.fullmatch(parsed.path) for pattern in policy.path_patterns):
            output.append((family, title, url))
    return tuple(output)


def _discover_bea_release(raw_feed: bytes, request: ScheduledReactionSpec) -> str:
    try:
        root = ElementTree.fromstring(raw_feed)
    except ElementTree.ParseError:
        raise OfficialReactionSourceError("OFFICIAL_DISCOVERY_SCHEMA_DRIFT") from None
    family = request.parent.family
    phrase = "personal income and outlays" if family is EventFamily.PCE else "gross domestic product"
    matches: list[str] = []
    for item in root.findall(".//item")[:100]:
        title = (item.findtext("title") or "").strip().lower()
        url = (item.findtext("link") or "").strip()
        if phrase not in title:
            continue
        policy = POLICIES[family]
        parsed = urlsplit(url)
        if parsed.scheme == "https" and parsed.hostname == policy.host and not parsed.query and not parsed.fragment and any(pattern.fullmatch(parsed.path) for pattern in policy.path_patterns):
            if _bea_url_matches_request(url, request):
                matches.append(url)
    if len(matches) != 1:
        raise OfficialReactionSourceError("OFFICIAL_RELEASE_DOCUMENT_NOT_DISCOVERED")
    return matches[0]


def _bea_url_matches_request(url: str, request: ScheduledReactionSpec) -> bool:
    path = urlsplit(url).path.lower()
    reference = request.parent.reference_period
    if request.parent.family is EventFamily.PCE:
        month = datetime.strptime(reference, "%Y-%m").strftime("%B").lower()
        return f"-{month}-{reference[:4]}" in path
    quarter = reference[-1]
    ordinal = {"1": "1st", "2": "2nd", "3": "3rd", "4": "4th"}[quarter]
    word = {"1": "first", "2": "second", "3": "third", "4": "fourth"}[quarter]
    estimate = str(request.parent.estimate_label or "").lower()
    return estimate in path and any(
        token in path
        for token in (
            f"-{ordinal}-quarter-",
            f"-{word}-quarter-",
            f"-q{quarter}-",
        )
    )


def _fomc_url_matches_request(url: str, request: ScheduledReactionSpec) -> bool:
    path = urlsplit(url).path.lower()
    if request.parent.family is EventFamily.FOMC_STATEMENT:
        return path.endswith(f"monetary{request.scheduled_at.strftime('%Y%m%d')}a.htm")
    meeting_end = request.parent.reference_period.split("/")[-1].replace("-", "")
    return path.endswith(f"fomcminutes{meeting_end}.htm")


__all__ = ["BoundedOfficialDocumentClient", "CapturedOfficialRelease", "DiscoveryRecord", "OfficialDocument", "OfficialReactionSourceError", "OfficialReleaseCaptureCoordinator", "ParsedMeasure", "ParsedOfficialRelease", "discover_fomc_documents", "parse_bea_gdp", "parse_bea_pce", "parse_bls_employment", "parse_bls_prices", "parse_bound_release", "parse_fomc_minutes", "parse_fomc_statement"]
