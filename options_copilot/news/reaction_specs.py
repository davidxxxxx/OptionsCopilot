"""Authoritative support, identity, and lifecycle contracts for event reactions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
import re
from types import MappingProxyType
from typing import Mapping, Sequence

from options_copilot.storage.canonical import canonical_hash, datetime_text, utc_datetime


SUPPORTING_ONLY = "SUPPORTING_ONLY"


class EventFamily(str, Enum):
    CPI = "CPI"
    PPI = "PPI"
    EMPLOYMENT_SITUATION = "EMPLOYMENT_SITUATION"
    PCE = "PCE"
    GDP = "GDP"
    RETAIL_SALES = "RETAIL_SALES"
    JOBLESS_CLAIMS = "JOBLESS_CLAIMS"
    ISM = "ISM"
    FOMC_STATEMENT = "FOMC_STATEMENT"
    FOMC_MINUTES = "FOMC_MINUTES"
    FOMC_PRESS_CONFERENCE = "FOMC_PRESS_CONFERENCE"
    EARNINGS_GUIDANCE = "EARNINGS_GUIDANCE"
    UNKNOWN = "UNKNOWN"


class EventRole(str, Enum):
    OFFICIAL_CALENDAR = "OFFICIAL_CALENDAR"
    OFFICIAL_RELEASE_DOCUMENT = "OFFICIAL_RELEASE_DOCUMENT"
    LATEST_REVISED_SERIES = "LATEST_REVISED_SERIES"
    SECONDARY_EXPECTATION = "SECONDARY_EXPECTATION"


class SupportState(str, Enum):
    SUPPORTED = "SUPPORTED"
    PREFLIGHT_ONLY = "PREFLIGHT_ONLY"
    UNSUPPORTED = "UNSUPPORTED"


class ActualKind(str, Enum):
    NUMERIC = "NUMERIC"
    DOCUMENT = "DOCUMENT"
    BOUNDED_SEMANTIC = "BOUNDED_SEMANTIC"


class DocumentStage(str, Enum):
    SCHEDULED = "SCHEDULED"
    WAITING_RELEASE_DOCUMENT = "WAITING_RELEASE_DOCUMENT"
    DOCUMENT_CAPTURED = "DOCUMENT_CAPTURED"
    ACTUAL_PARSE_UNAVAILABLE = "ACTUAL_PARSE_UNAVAILABLE"
    COMPLETE_SUPPORTING_ONLY = "COMPLETE_SUPPORTING_ONLY"


@dataclass(frozen=True, slots=True)
class MeasureSpec:
    measure_id: str
    label: str
    unit: str
    basis: str
    actual_kind: ActualKind = ActualKind.NUMERIC


@dataclass(frozen=True, slots=True)
class ReactionFamilySpec:
    family: EventFamily
    publisher: str
    support_state: SupportState
    measures: tuple[MeasureSpec, ...]
    surprise_supported: bool
    reason: str | None = None

    @property
    def supported(self) -> bool:
        return self.support_state is SupportState.SUPPORTED


_PERCENT = "PERCENT"
FAMILY_SPECS: Mapping[EventFamily, ReactionFamilySpec] = MappingProxyType(
    {
        EventFamily.CPI: ReactionFamilySpec(EventFamily.CPI, "BLS", SupportState.SUPPORTED, (MeasureSpec("headline_cpi_yoy_pct", "Headline CPI y/y", _PERCENT, "NOT_SEASONALLY_ADJUSTED"), MeasureSpec("headline_cpi_mom_pct", "Headline CPI m/m", _PERCENT, "SEASONALLY_ADJUSTED"), MeasureSpec("core_cpi_yoy_pct", "Core CPI y/y", _PERCENT, "NOT_SEASONALLY_ADJUSTED"), MeasureSpec("core_cpi_mom_pct", "Core CPI m/m", _PERCENT, "SEASONALLY_ADJUSTED")), True),
        EventFamily.PPI: ReactionFamilySpec(EventFamily.PPI, "BLS", SupportState.SUPPORTED, (MeasureSpec("headline_ppi_yoy_pct", "Headline PPI y/y", _PERCENT, "NOT_SEASONALLY_ADJUSTED"), MeasureSpec("headline_ppi_mom_pct", "Headline PPI m/m", _PERCENT, "SEASONALLY_ADJUSTED"), MeasureSpec("core_ppi_yoy_pct", "Core PPI y/y", _PERCENT, "NOT_SEASONALLY_ADJUSTED"), MeasureSpec("core_ppi_mom_pct", "Core PPI m/m", _PERCENT, "SEASONALLY_ADJUSTED")), True),
        EventFamily.EMPLOYMENT_SITUATION: ReactionFamilySpec(EventFamily.EMPLOYMENT_SITUATION, "BLS", SupportState.SUPPORTED, (MeasureSpec("total_nonfarm_payroll_change_thousands", "Total nonfarm payroll change", "THOUSANDS", "SEASONALLY_ADJUSTED"), MeasureSpec("unemployment_rate_pct", "Unemployment rate", _PERCENT, "SEASONALLY_ADJUSTED")), True),
        EventFamily.PCE: ReactionFamilySpec(EventFamily.PCE, "BEA", SupportState.SUPPORTED, (MeasureSpec("pce_price_index_mom_pct", "PCE price index m/m", _PERCENT, "SEASONALLY_ADJUSTED"), MeasureSpec("pce_price_index_yoy_pct", "PCE price index y/y", _PERCENT, "NOT_SEASONALLY_ADJUSTED_YEAR_OVER_YEAR"), MeasureSpec("core_pce_price_index_mom_pct", "Core PCE price index m/m", _PERCENT, "SEASONALLY_ADJUSTED"), MeasureSpec("core_pce_price_index_yoy_pct", "Core PCE price index y/y", _PERCENT, "NOT_SEASONALLY_ADJUSTED_YEAR_OVER_YEAR")), True),
        EventFamily.GDP: ReactionFamilySpec(EventFamily.GDP, "BEA", SupportState.SUPPORTED, (MeasureSpec("real_gdp_annual_rate_pct", "Real GDP percent change, annual rate", _PERCENT, "SEASONALLY_ADJUSTED_ANNUAL_RATE"),), True),
        EventFamily.FOMC_STATEMENT: ReactionFamilySpec(EventFamily.FOMC_STATEMENT, "FED", SupportState.SUPPORTED, (MeasureSpec("policy_statement", "Policy statement", "DOCUMENT", "OFFICIAL_STATEMENT", ActualKind.BOUNDED_SEMANTIC),), False, "NUMERIC_SURPRISE_UNAVAILABLE"),
        EventFamily.FOMC_MINUTES: ReactionFamilySpec(EventFamily.FOMC_MINUTES, "FED", SupportState.SUPPORTED, (MeasureSpec("meeting_minutes", "Meeting minutes", "DOCUMENT", "OFFICIAL_MINUTES", ActualKind.DOCUMENT),), False, "NUMERIC_ACTUAL_AND_SURPRISE_UNSUPPORTED"),
        EventFamily.RETAIL_SALES: ReactionFamilySpec(EventFamily.RETAIL_SALES, "CENSUS", SupportState.PREFLIGHT_ONLY, (), False, "EXACT_INITIAL_RELEASE_VARIABLE_TUPLE_UNVERIFIED"),
        EventFamily.JOBLESS_CLAIMS: ReactionFamilySpec(EventFamily.JOBLESS_CLAIMS, "DOL", SupportState.UNSUPPORTED, (), False, "NO_DOCUMENTED_STRUCTURED_ACTUAL_FEED"),
        EventFamily.ISM: ReactionFamilySpec(EventFamily.ISM, "ISM", SupportState.UNSUPPORTED, (), False, "NO_DOCUMENTED_PUBLIC_STRUCTURED_FEED"),
        EventFamily.FOMC_PRESS_CONFERENCE: ReactionFamilySpec(EventFamily.FOMC_PRESS_CONFERENCE, "FED", SupportState.UNSUPPORTED, (), False, "NO_STABLE_REAL_TIME_STRUCTURED_TRANSCRIPT"),
        EventFamily.EARNINGS_GUIDANCE: ReactionFamilySpec(EventFamily.EARNINGS_GUIDANCE, "SEC_OR_IR", SupportState.UNSUPPORTED, (), False, "NO_GENERIC_AUTHORITATIVE_ACTUAL_CONTRACT"),
        EventFamily.UNKNOWN: ReactionFamilySpec(EventFamily.UNKNOWN, "UNKNOWN", SupportState.UNSUPPORTED, (), False, "REACTION_EVENT_UNSUPPORTED"),
    }
)


@dataclass(frozen=True, slots=True)
class ParentEventIdentity:
    publisher: str
    family: EventFamily
    reference_period: str
    scheduled_date: date
    estimate_label: str | None = None

    def __post_init__(self) -> None:
        publisher = self.publisher.strip().upper()
        reference = self.reference_period.strip().upper()
        estimate = None if self.estimate_label is None else self.estimate_label.strip().upper()
        if not publisher or not reference:
            raise ValueError("parent identity fields cannot be blank")
        if self.family is EventFamily.GDP and not estimate:
            raise ValueError("GDP parent identity requires estimate_label")
        object.__setattr__(self, "publisher", publisher)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "estimate_label", estimate)

    @property
    def stable_id(self) -> str:
        parts = [self.publisher.upper(), self.family.value, self.reference_period]
        if self.estimate_label:
            parts.append(self.estimate_label.upper().replace(" ", "_"))
        parts.append(self.scheduled_date.isoformat())
        return ":".join(parts)

    @property
    def content_hash(self) -> str:
        return canonical_hash({"schema": "options_copilot.reaction_parent_identity.v1", "publisher": self.publisher.upper(), "family": self.family.value, "reference_period": self.reference_period, "estimate_label": self.estimate_label, "scheduled_date": self.scheduled_date.isoformat(), "decision_authority": SUPPORTING_ONLY})


@dataclass(frozen=True, slots=True)
class MeasureIdentity:
    parent: ParentEventIdentity
    measure_id: str

    @property
    def stable_id(self) -> str:
        return f"{self.parent.stable_id}:{self.measure_id}"

    @property
    def content_hash(self) -> str:
        return canonical_hash({"schema": "options_copilot.reaction_measure_identity.v1", "parent_hash": self.parent.content_hash, "measure_id": self.measure_id, "decision_authority": SUPPORTING_ONLY})


@dataclass(frozen=True, slots=True)
class ScheduledReactionSpec:
    parent: ParentEventIdentity
    scheduled_at: datetime
    official_event_hash: str
    capture_deadline: datetime
    next_eligible_release_at: datetime | None = None

    def __post_init__(self) -> None:
        scheduled = utc_datetime(self.scheduled_at, field="scheduled_at")
        deadline = utc_datetime(self.capture_deadline, field="capture_deadline")
        if deadline <= scheduled or deadline - scheduled > timedelta(minutes=15):
            raise ValueError("capture deadline must be within fifteen minutes after release")
        if len(self.official_event_hash) != 64:
            raise ValueError("official_event_hash must be SHA-256")
        object.__setattr__(self, "scheduled_at", scheduled)
        object.__setattr__(self, "capture_deadline", deadline)
        if self.next_eligible_release_at is not None:
            next_release = utc_datetime(self.next_eligible_release_at, field="next_eligible_release_at")
            if next_release <= scheduled:
                raise ValueError("next eligible release must be after scheduled release")
            object.__setattr__(self, "next_eligible_release_at", next_release)

    @property
    def next_release_text(self) -> str:
        return "NEXT_ELIGIBLE_RELEASE_UNKNOWN" if self.next_eligible_release_at is None else self.next_eligible_release_at.isoformat()


@dataclass(frozen=True, slots=True)
class CalendarReactionDescriptor:
    """Typed, hash-bound calendar input for one prospective reaction lifecycle."""

    event_id: str
    publisher: str
    family: EventFamily
    source_id: str
    source_url: str
    title: str
    scheduled_at: datetime | None
    schedule_precision: str
    reference_period: str | None
    estimate_label: str | None
    meeting_range: str | None
    calendar_event_hash: str
    calendar_record_hash: str
    calendar_feed_hash: str
    stable_event_key: str
    identity_hash: str
    version_provenance_hash: str
    wait_reason: str | None = None

    @property
    def capture_eligible(self) -> bool:
        return (
            FAMILY_SPECS[self.family].support_state is SupportState.SUPPORTED
            and self.scheduled_at is not None
            and self.schedule_precision == "EXACT"
            and self.reference_period is not None
            and self.wait_reason is None
        )

    @property
    def parent(self) -> ParentEventIdentity | None:
        if not self.capture_eligible or self.scheduled_at is None or self.reference_period is None:
            return None
        return ParentEventIdentity(
            self.publisher,
            self.family,
            self.reference_period,
            self.scheduled_at.date(),
            self.estimate_label,
        )


@dataclass(frozen=True, slots=True)
class SupportAssessment:
    family: EventFamily
    support_state: SupportState
    supported: bool
    capture_eligible: bool
    surprise_eligible: bool
    progressed: bool
    next_action: str
    reason: str | None
    event_count: int = 1
    measure_count: int = 0

    @property
    def decision_authority(self) -> str:
        return SUPPORTING_ONLY


def assess_support(
    family: EventFamily,
    *,
    scheduled_at: datetime,
    now: datetime,
    expectation_captured_at: datetime | None = None,
    release_document_captured: bool = False,
) -> SupportAssessment:
    spec = FAMILY_SPECS[family]
    checked_now = utc_datetime(now, field="now")
    scheduled = utc_datetime(scheduled_at, field="scheduled_at")
    prospective_expectation = expectation_captured_at is not None and utc_datetime(expectation_captured_at, field="expectation_captured_at") < scheduled
    capture_eligible = spec.supported and checked_now >= scheduled
    surprise_eligible = capture_eligible and spec.surprise_supported and prospective_expectation
    progressed = release_document_captured
    if not spec.supported:
        next_action = "PREFLIGHT_OFFICIAL_CONTRACT" if spec.support_state is SupportState.PREFLIGHT_ONLY else "NONE_UNSUPPORTED"
        reason = spec.reason
    elif checked_now < scheduled:
        next_action, reason = "WAIT_FOR_DECLARED_RELEASE_TIME", "WAITING_DECLARED_RELEASE_TIME"
    elif not release_document_captured:
        next_action, reason = "CAPTURE_OFFICIAL_RELEASE_DOCUMENT", "WAITING_OFFICIAL_RELEASE_DOCUMENT"
    elif spec.surprise_supported and not prospective_expectation:
        next_action, reason = "WAIT_NEXT_ELIGIBLE_RELEASE", "NO_PROSPECTIVE_COMPATIBLE_EXPECTATION"
    else:
        next_action, reason = "OBSERVE_PROSPECTIVE_FIVE_MINUTE_WINDOW", spec.reason
    return SupportAssessment(family, spec.support_state, spec.supported, capture_eligible, surprise_eligible, progressed, next_action, reason, measure_count=len(spec.measures))


def classify_event_family(source: object, title: object, category: object = None) -> EventFamily:
    publisher = str(source or "").upper()
    text = str(title or "").upper()
    category_text = str(category or "").upper()
    bls = publisher in {"BLS", "BUREAU OF LABOR STATISTICS"}
    bea = publisher in {
        "BEA",
        "BUREAU OF ECONOMIC ANALYSIS",
        "BUREAU OF ECONOMIC ANALYSIS HTML FALLBACK",
    }
    fed = publisher in {
        "FED",
        "FEDERAL RESERVE",
        "FEDERAL RESERVE BOARD",
        "FEDERAL RESERVE RELEASE CALENDAR",
    }
    census = publisher in {"CENSUS", "U.S. CENSUS BUREAU", "US CENSUS BUREAU"}
    dol = publisher in {"DOL", "U.S. DEPARTMENT OF LABOR", "DEPARTMENT OF LABOR"}
    ism = publisher == "ISM" or "INSTITUTE FOR SUPPLY MANAGEMENT" in publisher
    if "EARNINGS" in text or "GUIDANCE" in text or category_text in {"EARNINGS", "GUIDANCE"}:
        return EventFamily.EARNINGS_GUIDANCE
    if fed and re.search(r"\bPRESS\s+CONFERENCE\b", text):
        return EventFamily.FOMC_PRESS_CONFERENCE
    if fed and re.search(r"\b(?:FOMC\s+)?MINUTES\b", text):
        return EventFamily.FOMC_MINUTES
    if fed and (re.search(r"\bFOMC\s+(?:RATE\s+)?DECISION\b", text) or re.search(r"\bFOMC\s+STATEMENT\b", text)):
        return EventFamily.FOMC_STATEMENT
    if fed and category_text == "FOMC" and re.fullmatch(r"FOMC\s+MEETING\s*\([^)]+\)", text):
        return EventFamily.FOMC_STATEMENT
    if bls and (re.search(r"\bEMPLOYMENT\s+SITUATION\b", text) or re.search(r"\bNONFARM\s+PAYROLLS?\b", text) or re.search(r"\bUNEMPLOYMENT\s+RATE\b", text)):
        return EventFamily.EMPLOYMENT_SITUATION
    if bea and (re.search(r"\bPERSONAL\s+INCOME\s+AND\s+OUTLAYS\b", text) or re.search(r"\bPCE\s+PRICE\s+INDEX\b", text)):
        return EventFamily.PCE
    if bea and (
        re.search(r"\bGROSS\s+DOMESTIC\s+PRODUCT\b", text)
        or re.search(r"\bGDP\s+(?:ADVANCE|SECOND|THIRD)\s+ESTIMATE\b", text)
        or re.search(
            r"\bGDP\s*\(\s*(?:ADVANCE|SECOND|THIRD)\s+ESTIMATE\s*\)",
            text,
        )
    ):
        return EventFamily.GDP
    if census and re.search(r"\b(?:ADVANCE\s+)?RETAIL\s+SALES\b", text):
        return EventFamily.RETAIL_SALES
    if dol and (re.search(r"\bJOBLESS\s+CLAIMS\b", text) or re.search(r"\bUNEMPLOYMENT\s+INSURANCE\s+WEEKLY\s+CLAIMS\b", text)):
        return EventFamily.JOBLESS_CLAIMS
    if ism and (re.search(r"\bISM\b", text) or re.search(r"\bPURCHASING\s+MANAGERS\b", text)):
        return EventFamily.ISM
    if bls and (re.search(r"\bCONSUMER\s+PRICE\s+INDEX\b", text) or re.search(r"\bCPI\b", text)):
        return EventFamily.CPI
    if bls and (re.search(r"\bPRODUCER\s+PRICE\s+INDEX\b", text) or re.search(r"\bPPI\b", text)):
        return EventFamily.PPI
    return EventFamily.UNKNOWN


_MONTHS = {
    name: number
    for number, name in enumerate(
        (
            "JANUARY",
            "FEBRUARY",
            "MARCH",
            "APRIL",
            "MAY",
            "JUNE",
            "JULY",
            "AUGUST",
            "SEPTEMBER",
            "OCTOBER",
            "NOVEMBER",
            "DECEMBER",
        ),
        start=1,
    )
}


def parent_identity_from_calendar(
    *,
    source: object,
    title: object,
    category: object,
    scheduled_at: datetime,
) -> ParentEventIdentity | None:
    """Derive only identities explicitly present in an official calendar row."""

    scheduled = utc_datetime(scheduled_at, field="scheduled_at")
    family = classify_event_family(source, title, category)
    spec = FAMILY_SPECS[family]
    if not spec.supported:
        return None
    text = " ".join(str(title or "").upper().replace(",", " ").split())
    publisher = spec.publisher
    estimate_label: str | None = None
    reference_period: str | None = None
    if family in {
        EventFamily.CPI,
        EventFamily.PPI,
        EventFamily.EMPLOYMENT_SITUATION,
        EventFamily.PCE,
    }:
        matches = tuple(
            re.finditer(
                r"\b(" + "|".join(_MONTHS) + r")\s+(20\d{2})\b",
                text,
            )
        )
        if len(matches) == 1:
            reference_period = (
                f"{int(matches[0].group(2)):04d}-{_MONTHS[matches[0].group(1)]:02d}"
            )
    elif family is EventFamily.GDP:
        quarter_matches = tuple(
            re.finditer(
                r"\b(?:Q([1-4])|([1-4])(?:ST|ND|RD|TH)\s+QUARTER|"
                r"(FIRST|SECOND|THIRD|FOURTH)\s+QUARTER)\s+(20\d{2})\b",
                text,
            )
        )
        estimate_matches = tuple(re.finditer(r"\b(ADVANCE|SECOND|THIRD)\s+ESTIMATE\b", text))
        if len(quarter_matches) == 1 and len(estimate_matches) == 1:
            word_quarters = {"FIRST": "1", "SECOND": "2", "THIRD": "3", "FOURTH": "4"}
            quarter = (
                quarter_matches[0].group(1)
                or quarter_matches[0].group(2)
                or word_quarters[quarter_matches[0].group(3)]
            )
            reference_period = f"{quarter_matches[0].group(4)}-Q{quarter}"
            estimate_label = estimate_matches[0].group(1)
    elif family is EventFamily.FOMC_STATEMENT:
        meeting_range = fomc_meeting_range_from_title(text)
        reference_period = (
            scheduled.date().isoformat()
            if meeting_range is None
            else meeting_range.split("/", 1)[1]
        )
    elif family is EventFamily.FOMC_MINUTES:
        range_match = re.search(
            r"\b(20\d{2})-(\d{2})-(\d{2})\s*/\s*(20\d{2})-(\d{2})-(\d{2})\b",
            text,
        )
        if range_match is not None:
            reference_period = (
                f"{range_match.group(1)}-{range_match.group(2)}-{range_match.group(3)}"
                f"/{range_match.group(4)}-{range_match.group(5)}-{range_match.group(6)}"
            )
    if reference_period is None:
        return None
    return ParentEventIdentity(
        publisher,
        family,
        reference_period,
        scheduled.date(),
        estimate_label,
    )


def reaction_descriptor_from_calendar(
    event: object,
    *,
    meeting_ranges: Sequence[str] = (),
) -> CalendarReactionDescriptor:
    """Convert a real official-calendar row without inventing missing precision."""

    event_id = str(getattr(event, "event_id", "")).strip()
    source = str(getattr(event, "source", "")).strip()
    source_id = str(getattr(event, "source_id", "")).strip()
    source_url = str(getattr(event, "source_url", "")).strip()
    title = str(getattr(event, "title", "")).strip()
    precision = str(getattr(event, "schedule_precision", "")).strip().upper()
    scheduled = getattr(event, "scheduled_at", None)
    family = classify_event_family(source, title, getattr(event, "category", ""))
    publisher = FAMILY_SPECS[family].publisher
    event_hash = str(getattr(event, "content_hash", "")).strip().lower()
    record_hash = str(getattr(event, "record_hash", "")).strip().lower()
    provenance = tuple(getattr(event, "provenance", ()))
    feed_hashes = tuple(
        str(getattr(item, "source_payload_hash", "")).strip().lower()
        for item in provenance
        if re.fullmatch(
            r"[0-9a-f]{64}",
            str(getattr(item, "source_payload_hash", "")).strip().lower(),
        )
    )
    feed_hash = canonical_hash({"source_payload_hashes": feed_hashes})
    wait_reason: str | None = None
    parent: ParentEventIdentity | None = None
    meeting_range: str | None = None
    if precision != "EXACT" or scheduled is None:
        wait_reason = "WAIT_EXACT_RELEASE_TIME_UNAVAILABLE"
    elif FAMILY_SPECS[family].support_state is not SupportState.SUPPORTED:
        wait_reason = FAMILY_SPECS[family].reason or "REACTION_EVENT_UNSUPPORTED"
    else:
        parent = parent_identity_from_calendar(
            source=source,
            title=title,
            category=getattr(event, "category", ""),
            scheduled_at=scheduled,
        )
        if parent is None and family is EventFamily.FOMC_MINUTES:
            candidates = [
                value
                for value in meeting_ranges
                if 14
                <= (
                    scheduled.date()
                    - datetime.strptime(value.split("/", 1)[1], "%Y-%m-%d").date()
                ).days
                <= 35
            ]
            if len(candidates) == 1:
                meeting_range = candidates[0]
                parent = ParentEventIdentity(
                    "FED",
                    family,
                    meeting_range,
                    scheduled.date(),
                )
        if parent is None:
            wait_reason = "WAIT_OFFICIAL_REFERENCE_PERIOD_UNAVAILABLE"
    if parent is not None and family in {
        EventFamily.FOMC_STATEMENT,
        EventFamily.FOMC_MINUTES,
    }:
        meeting_range = parent.reference_period
    stable_document = {
        "schema": "options_copilot.reaction_stable_event_key.v1",
        "publisher": publisher,
        "family": family.value,
        "source_id": source_id,
        "scheduled_at": None if scheduled is None else datetime_text(scheduled),
        "schedule_precision": precision,
        "reference_period": None if parent is None else parent.reference_period,
        "estimate_label": None if parent is None else parent.estimate_label,
        "meeting_range": meeting_range,
        "decision_authority": SUPPORTING_ONLY,
    }
    stable_event_key = canonical_hash(stable_document)
    provenance_document = {
        "schema": "options_copilot.calendar_reaction_descriptor_provenance.v1",
        "stable_event_key": stable_event_key,
        "event_id": event_id,
        "source_url": source_url,
        "title": title,
        "calendar_event_hash": event_hash,
        "calendar_record_hash": record_hash,
        "calendar_feed_hash": feed_hash,
        "wait_reason": wait_reason,
        "decision_authority": SUPPORTING_ONLY,
    }
    return CalendarReactionDescriptor(
        event_id=event_id,
        publisher=publisher,
        family=family,
        source_id=source_id,
        source_url=source_url,
        title=title,
        scheduled_at=(
            None
            if scheduled is None
            else utc_datetime(scheduled, field="scheduled_at")
        ),
        schedule_precision=precision,
        reference_period=None if parent is None else parent.reference_period,
        estimate_label=None if parent is None else parent.estimate_label,
        meeting_range=meeting_range,
        calendar_event_hash=event_hash,
        calendar_record_hash=record_hash,
        calendar_feed_hash=feed_hash,
        stable_event_key=stable_event_key,
        identity_hash=stable_event_key,
        version_provenance_hash=canonical_hash(provenance_document),
        wait_reason=wait_reason,
    )


def fomc_meeting_range_from_title(title: object) -> str | None:
    text = " ".join(str(title or "").upper().replace("–", "-").split())
    match = re.search(
        r"\bFOMC\s+MEETING\s*\((" + "|".join(_MONTHS) + r")\s+(\d{1,2})-(\d{1,2}),\s*(20\d{2})\)",
        text,
    )
    if match is None:
        return None
    month = _MONTHS[match.group(1)]
    try:
        start = date(int(match.group(4)), month, int(match.group(2)))
        end = date(int(match.group(4)), month, int(match.group(3)))
    except ValueError:
        return None
    if end < start or end - start > timedelta(days=2):
        return None
    return f"{start.isoformat()}/{end.isoformat()}"


__all__ = ["ActualKind", "CalendarReactionDescriptor", "DocumentStage", "EventFamily", "EventRole", "FAMILY_SPECS", "MeasureIdentity", "MeasureSpec", "ParentEventIdentity", "ReactionFamilySpec", "ScheduledReactionSpec", "SupportAssessment", "SupportState", "assess_support", "classify_event_family", "fomc_meeting_range_from_title", "parent_identity_from_calendar", "reaction_descriptor_from_calendar"]
