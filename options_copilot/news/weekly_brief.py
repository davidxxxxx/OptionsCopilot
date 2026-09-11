"""Pure provisional weekly-brief lifecycle and append-ready contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any

from options_copilot.decision.gates import ProvisionalWatchGatePreview
from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    utc_datetime,
)


WEEKLY_BRIEF_SCHEMA = "options_copilot.weekly_brief.v1"
WEEKLY_BRIEF_SLOT_SCHEMA = "options_copilot.weekly_brief_slot.v1"
WEEKLY_BRIEF_APPEND_SCHEMA = "options_copilot.weekly_brief_append.v1"
WEEKLY_BRIEF_VERSION = 1
WEEKLY_BRIEF_SLOT_ET = time(8, 30)
WEEKLY_BRIEF_SLOT_WINDOW = timedelta(minutes=1)
MAXIMUM_WATCH_ITEMS = 10
_HEX = frozenset("0123456789abcdef")


class WeeklyBriefSlotStatus(str, Enum):
    DUE = "DUE"
    NOT_RUN = "NOT_RUN"


class SourceHealthStatus(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class WeeklyBriefSlotDecision:
    """Exact-only first-session 08:30 ET lifecycle decision."""

    schema: str
    version: int
    status: WeeklyBriefSlotStatus
    reason_codes: tuple[str, ...]
    week_start: date
    first_session_date: date | None
    scheduled_for: datetime
    evaluated_at: datetime
    calendar_hash: str
    decision_hash: str

    def __post_init__(self) -> None:
        if self.schema != WEEKLY_BRIEF_SLOT_SCHEMA or self.version != WEEKLY_BRIEF_VERSION:
            raise ValueError("unsupported weekly brief slot schema/version")
        object.__setattr__(self, "status", WeeklyBriefSlotStatus(self.status))
        object.__setattr__(self, "reason_codes", _codes(self.reason_codes))
        if not isinstance(self.week_start, date) or isinstance(self.week_start, datetime):
            raise TypeError("week_start must be a date")
        if self.week_start.weekday() != 0:
            raise ValueError("week_start must be an ET Monday")
        if self.first_session_date is not None and (
            not isinstance(self.first_session_date, date)
            or isinstance(self.first_session_date, datetime)
        ):
            raise TypeError("first_session_date must be a date or None")
        scheduled = utc_datetime(self.scheduled_for, field="scheduled_for").astimezone(
            US_OPTIONS_TIMEZONE,
        )
        evaluated = utc_datetime(self.evaluated_at, field="evaluated_at")
        object.__setattr__(self, "scheduled_for", scheduled)
        object.__setattr__(self, "evaluated_at", evaluated)
        object.__setattr__(self, "calendar_hash", _digest("calendar_hash", self.calendar_hash))
        if self.status is WeeklyBriefSlotStatus.DUE:
            if self.reason_codes:
                raise ValueError("DUE slot cannot carry reason_codes")
            if self.first_session_date != scheduled.date():
                raise ValueError("DUE slot must be the first official session date")
            if scheduled.timetz().replace(tzinfo=None) != WEEKLY_BRIEF_SLOT_ET:
                raise ValueError("DUE slot must be exactly 08:30 ET")
            if scheduled.second or scheduled.microsecond:
                raise ValueError("DUE slot cannot contain sub-minute time")
            cutoff_utc = scheduled.astimezone(evaluated.tzinfo)
            if not cutoff_utc <= evaluated < cutoff_utc + WEEKLY_BRIEF_SLOT_WINDOW:
                raise ValueError("DUE slot must be evaluated inside its exact minute")
        elif not self.reason_codes:
            raise ValueError("NOT_RUN slot requires stable reason_codes")
        _digest("decision_hash", self.decision_hash)
        if canonical_hash(self.hash_payload()) != self.decision_hash:
            raise ValueError("decision_hash does not match weekly slot decision")

    @property
    def produce_allowed(self) -> bool:
        return self.status is WeeklyBriefSlotStatus.DUE

    @property
    def cutoff_at(self) -> datetime:
        return self.scheduled_for

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "status": self.status.value,
            "reason_codes": self.reason_codes,
            "week_start": self.week_start.isoformat(),
            "first_session_date": (
                None if self.first_session_date is None else self.first_session_date.isoformat()
            ),
            "scheduled_for": self.scheduled_for.isoformat(),
            "evaluated_at": datetime_text(self.evaluated_at),
            "calendar_hash": self.calendar_hash,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.hash_payload(),
            "produce_allowed": self.produce_allowed,
            "decision_hash": self.decision_hash,
        }


def evaluate_weekly_brief_slot(
    *,
    scheduled_for: datetime,
    evaluated_at: datetime,
    official_session_dates: Sequence[date],
    calendar_hash: str,
) -> WeeklyBriefSlotDecision:
    """Evaluate one scheduled callback without replaying a missed 08:30 slot."""

    scheduled = utc_datetime(scheduled_for, field="scheduled_for").astimezone(
        US_OPTIONS_TIMEZONE,
    )
    evaluated = utc_datetime(evaluated_at, field="evaluated_at")
    normalized_calendar_hash = _digest("calendar_hash", calendar_hash)
    week_start = scheduled.date() - timedelta(days=scheduled.date().weekday())
    week_end = week_start + timedelta(days=7)
    sessions = tuple(
        sorted(
            {
                _session_date(value)
                for value in official_session_dates
                if week_start <= _session_date(value) < week_end
            },
        ),
    )
    first_session = sessions[0] if sessions else None
    scheduled_is_exact = (
        scheduled.timetz().replace(tzinfo=None) == WEEKLY_BRIEF_SLOT_ET
        and scheduled.second == 0
        and scheduled.microsecond == 0
    )
    cutoff_utc = scheduled.astimezone(evaluated.tzinfo)
    inside_exact_minute = cutoff_utc <= evaluated < cutoff_utc + WEEKLY_BRIEF_SLOT_WINDOW
    if first_session is None:
        status = WeeklyBriefSlotStatus.NOT_RUN
        reasons = ("WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE",)
    elif not scheduled_is_exact or scheduled.date() != first_session:
        status = WeeklyBriefSlotStatus.NOT_RUN
        reasons = ("WEEKLY_BRIEF_SLOT_MISSED",)
    elif evaluated < cutoff_utc:
        status = WeeklyBriefSlotStatus.NOT_RUN
        reasons = ("WEEKLY_BRIEF_SLOT_NOT_DUE",)
    elif not inside_exact_minute:
        status = WeeklyBriefSlotStatus.NOT_RUN
        reasons = ("WEEKLY_BRIEF_SLOT_MISSED",)
    else:
        status = WeeklyBriefSlotStatus.DUE
        reasons = ()
    values = {
        "schema": WEEKLY_BRIEF_SLOT_SCHEMA,
        "version": WEEKLY_BRIEF_VERSION,
        "status": status,
        "reason_codes": reasons,
        "week_start": week_start,
        "first_session_date": first_session,
        "scheduled_for": scheduled,
        "evaluated_at": evaluated,
        "calendar_hash": normalized_calendar_hash,
    }
    provisional = _provisional(WeeklyBriefSlotDecision, values)
    return WeeklyBriefSlotDecision(
        **values,
        decision_hash=canonical_hash(provisional.hash_payload()),
    )


@dataclass(frozen=True, slots=True)
class WeeklyBriefSourceHealth:
    source: str
    status: SourceHealthStatus
    mandatory: bool
    observed_at: datetime
    reason_codes: tuple[str, ...]
    source_hash: str | None
    health_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _identity("source", self.source).upper())
        object.__setattr__(self, "status", SourceHealthStatus(self.status))
        if not isinstance(self.mandatory, bool):
            raise TypeError("mandatory must be a bool")
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )
        object.__setattr__(self, "reason_codes", _codes(self.reason_codes))
        object.__setattr__(
            self,
            "source_hash",
            None if self.source_hash is None else _digest("source_hash", self.source_hash),
        )
        if self.status is not SourceHealthStatus.READY and not self.reason_codes:
            raise ValueError("non-READY source health requires reason_codes")
        if self.status is SourceHealthStatus.READY and self.source_hash is None:
            raise ValueError("READY source health requires source_hash")
        _digest("health_hash", self.health_hash)
        if canonical_hash(self.hash_payload()) != self.health_hash:
            raise ValueError("health_hash does not match source health")

    @classmethod
    def build(
        cls,
        *,
        source: str,
        status: SourceHealthStatus,
        mandatory: bool,
        observed_at: datetime,
        reason_codes: Sequence[str] = (),
        source_hash: str | None = None,
    ) -> WeeklyBriefSourceHealth:
        values = {
            "source": _identity("source", source).upper(),
            "status": SourceHealthStatus(status),
            "mandatory": mandatory,
            "observed_at": utc_datetime(observed_at, field="observed_at"),
            "reason_codes": _codes(reason_codes),
            "source_hash": (
                None if source_hash is None else _digest("source_hash", source_hash)
            ),
        }
        provisional = _provisional(cls, values)
        return cls(**values, health_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "status": self.status.value,
            "mandatory": self.mandatory,
            "observed_at": datetime_text(self.observed_at),
            "reason_codes": self.reason_codes,
            "source_hash": self.source_hash,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "health_hash": self.health_hash}


@dataclass(frozen=True, slots=True)
class WeeklyBriefEvidenceItem:
    """Point-in-time news/event evidence used in one weekly window."""

    item_id: str
    occurred_at: datetime
    observed_at: datetime
    source: str
    source_hash: str
    headline: str
    summary: str
    symbols: tuple[str, ...]
    affected_assets: tuple[str, ...]
    direction: str
    supporting_evidence_ids: tuple[str, ...]
    contradicting_evidence_ids: tuple[str, ...]
    deepseek_summary: str | None
    item_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "item_id", _identity("item_id", self.item_id))
        object.__setattr__(
            self,
            "occurred_at",
            utc_datetime(self.occurred_at, field="occurred_at"),
        )
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )
        object.__setattr__(self, "source", _identity("source", self.source))
        object.__setattr__(self, "source_hash", _digest("source_hash", self.source_hash))
        object.__setattr__(self, "headline", _identity("headline", self.headline))
        object.__setattr__(self, "summary", _identity("summary", self.summary))
        object.__setattr__(self, "symbols", _labels(self.symbols, upper=True))
        object.__setattr__(self, "affected_assets", _labels(self.affected_assets, upper=True))
        object.__setattr__(self, "direction", _identity("direction", self.direction).upper())
        object.__setattr__(
            self,
            "supporting_evidence_ids",
            _labels(self.supporting_evidence_ids),
        )
        object.__setattr__(
            self,
            "contradicting_evidence_ids",
            _labels(self.contradicting_evidence_ids),
        )
        if self.deepseek_summary is not None:
            object.__setattr__(
                self,
                "deepseek_summary",
                _identity("deepseek_summary", self.deepseek_summary),
            )
        _digest("item_hash", self.item_hash)
        if canonical_hash(self.hash_payload()) != self.item_hash:
            raise ValueError("item_hash does not match evidence item")

    @classmethod
    def build(
        cls,
        *,
        item_id: str,
        occurred_at: datetime,
        observed_at: datetime,
        source: str,
        source_hash: str,
        headline: str,
        summary: str,
        symbols: Sequence[str] = (),
        affected_assets: Sequence[str] = (),
        direction: str = "UNKNOWN",
        supporting_evidence_ids: Sequence[str] = (),
        contradicting_evidence_ids: Sequence[str] = (),
        deepseek_summary: str | None = None,
    ) -> WeeklyBriefEvidenceItem:
        values = {
            "item_id": _identity("item_id", item_id),
            "occurred_at": utc_datetime(occurred_at, field="occurred_at"),
            "observed_at": utc_datetime(observed_at, field="observed_at"),
            "source": _identity("source", source),
            "source_hash": _digest("source_hash", source_hash),
            "headline": _identity("headline", headline),
            "summary": _identity("summary", summary),
            "symbols": _labels(symbols, upper=True),
            "affected_assets": _labels(affected_assets, upper=True),
            "direction": _identity("direction", direction).upper(),
            "supporting_evidence_ids": _labels(supporting_evidence_ids),
            "contradicting_evidence_ids": _labels(contradicting_evidence_ids),
            "deepseek_summary": (
                None
                if deepseek_summary is None
                else _identity("deepseek_summary", deepseek_summary)
            ),
        }
        provisional = _provisional(cls, values)
        return cls(**values, item_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "item_id": self.item_id,
            "occurred_at": datetime_text(self.occurred_at),
            "observed_at": datetime_text(self.observed_at),
            "source": self.source,
            "source_hash": self.source_hash,
            "headline": self.headline,
            "summary": self.summary,
            "symbols": self.symbols,
            "affected_assets": self.affected_assets,
            "direction": self.direction,
            "supporting_evidence_ids": self.supporting_evidence_ids,
            "contradicting_evidence_ids": self.contradicting_evidence_ids,
            "deepseek_summary": self.deepseek_summary,
            "deepseek_authority": (
                None if self.deepseek_summary is None else "SUPPORTING_ONLY"
            ),
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "item_hash": self.item_hash}


@dataclass(frozen=True, slots=True)
class WeeklyBrief:
    """Hash-bound provisional research artifact with no trading authority."""

    schema: str
    version: int
    week_start: date
    first_session_date: date
    cutoff_at: datetime
    calendar_hash: str
    prior_week_start: datetime
    prior_week_end: datetime
    upcoming_start: datetime
    upcoming_end: datetime
    next_preview_start: datetime
    next_preview_end: datetime
    source_bundle_hash: str
    source_health: tuple[WeeklyBriefSourceHealth, ...]
    prior_week_items: tuple[WeeklyBriefEvidenceItem, ...]
    upcoming_items: tuple[WeeklyBriefEvidenceItem, ...]
    next_preview_items: tuple[WeeklyBriefEvidenceItem, ...]
    watch_items: tuple[ProvisionalWatchGatePreview, ...]
    idempotency_key: str
    content_hash: str

    def __post_init__(self) -> None:
        if self.schema != WEEKLY_BRIEF_SCHEMA or self.version != WEEKLY_BRIEF_VERSION:
            raise ValueError("unsupported weekly brief schema/version")
        if not isinstance(self.week_start, date) or isinstance(self.week_start, datetime):
            raise TypeError("week_start must be a date")
        if not isinstance(self.first_session_date, date) or isinstance(
            self.first_session_date,
            datetime,
        ):
            raise TypeError("first_session_date must be a date")
        if self.week_start.weekday() != 0:
            raise ValueError("week_start must be Monday")
        cutoff = utc_datetime(self.cutoff_at, field="cutoff_at").astimezone(
            US_OPTIONS_TIMEZONE,
        )
        object.__setattr__(self, "cutoff_at", cutoff)
        if not self.week_start <= self.first_session_date < self.week_start + timedelta(
            days=7,
        ):
            raise ValueError("first_session_date must belong to week_start")
        if cutoff.date() != self.first_session_date:
            raise ValueError("cutoff_at date must match first_session_date")
        if (
            cutoff.timetz().replace(tzinfo=None) != WEEKLY_BRIEF_SLOT_ET
            or cutoff.second
            or cutoff.microsecond
        ):
            raise ValueError("cutoff_at must be exactly 08:30 ET")
        object.__setattr__(self, "calendar_hash", _digest("calendar_hash", self.calendar_hash))
        for field in (
            "prior_week_start",
            "prior_week_end",
            "upcoming_start",
            "upcoming_end",
            "next_preview_start",
            "next_preview_end",
        ):
            object.__setattr__(
                self,
                field,
                utc_datetime(getattr(self, field), field=field).astimezone(
                    US_OPTIONS_TIMEZONE,
                ),
            )
        week_start_at = datetime.combine(
            self.week_start,
            time.min,
            tzinfo=US_OPTIONS_TIMEZONE,
        )
        upcoming_start = datetime.combine(
            self.first_session_date,
            time.min,
            tzinfo=US_OPTIONS_TIMEZONE,
        )
        expected_windows = (
            week_start_at - timedelta(days=7),
            week_start_at,
            upcoming_start,
            upcoming_start + timedelta(days=8),
            upcoming_start + timedelta(days=8),
            upcoming_start + timedelta(days=15),
        )
        actual_windows = (
            self.prior_week_start,
            self.prior_week_end,
            self.upcoming_start,
            self.upcoming_end,
            self.next_preview_start,
            self.next_preview_end,
        )
        if actual_windows != expected_windows:
            raise ValueError("weekly brief windows must match calendar identity")
        object.__setattr__(
            self,
            "source_bundle_hash",
            _digest("source_bundle_hash", self.source_bundle_hash),
        )
        health = tuple(sorted(self.source_health, key=lambda item: item.source))
        if len({item.source for item in health}) != len(health):
            raise ValueError("source health entries must be unique")
        if any(
            item.observed_at >= cutoff + WEEKLY_BRIEF_SLOT_WINDOW
            for item in health
        ):
            raise ValueError("source health must be observed inside the exact slot")
        if any(
            item.mandatory and item.status is not SourceHealthStatus.READY
            for item in health
        ):
            raise ValueError("WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE")
        if not any(
            item.mandatory and item.status is SourceHealthStatus.READY
            for item in health
        ):
            raise ValueError("WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE")
        object.__setattr__(self, "source_health", health)
        window_specs = (
            ("prior_week_items", self.prior_week_start, self.prior_week_end),
            ("upcoming_items", self.upcoming_start, self.upcoming_end),
            (
                "next_preview_items",
                self.next_preview_start,
                self.next_preview_end,
            ),
        )
        all_items = tuple(
            item
            for field, _, _ in window_specs
            for item in getattr(self, field)
        )
        if len({item.item_id for item in all_items}) != len(all_items):
            raise ValueError("weekly evidence items must not appear in multiple windows")
        for field, window_start, window_end in window_specs:
            items = tuple(sorted(getattr(self, field), key=lambda item: item.item_hash))
            if any(item.observed_at > cutoff for item in items):
                raise ValueError("weekly evidence cannot be observed after cutoff")
            if any(
                not window_start
                <= item.occurred_at.astimezone(US_OPTIONS_TIMEZONE)
                < window_end
                for item in items
            ):
                raise ValueError(f"{field} contains evidence outside its window")
            object.__setattr__(self, field, items)
        bound_items = (
            self.prior_week_items
            + self.upcoming_items
            + self.next_preview_items
        )
        expected_source_bundle_hash = weekly_brief_source_bundle_hash(
            calendar_hash=self.calendar_hash,
            evidence_items=bound_items,
            source_health=health,
        )
        if self.source_bundle_hash != expected_source_bundle_hash:
            raise ValueError("source_bundle_hash does not match weekly brief evidence")
        watches = tuple(sorted(self.watch_items, key=lambda item: item.watch_id))
        if len(watches) > MAXIMUM_WATCH_ITEMS:
            raise ValueError("weekly brief supports at most ten watch items")
        if len({item.watch_id for item in watches}) != len(watches):
            raise ValueError("weekly watch_ids must be unique")
        for watch in watches:
            if watch.cutoff_at != cutoff:
                raise ValueError("watch cutoff_at must match weekly brief cutoff")
            if watch.source_bundle_hash != self.source_bundle_hash:
                raise ValueError("watch source bundle must match weekly brief evidence")
        object.__setattr__(self, "watch_items", watches)
        expected_idempotency = weekly_brief_idempotency_key(
            week_start=self.week_start,
            cutoff_at=cutoff,
        )
        if _digest("idempotency_key", self.idempotency_key) != expected_idempotency:
            raise ValueError("idempotency_key does not match weekly brief identity")
        _digest("content_hash", self.content_hash)
        if canonical_hash(self.hash_payload()) != self.content_hash:
            raise ValueError("content_hash does not match weekly brief")

    @classmethod
    def build(
        cls,
        *,
        slot: WeeklyBriefSlotDecision,
        evidence_items: Sequence[WeeklyBriefEvidenceItem],
        source_health: Sequence[WeeklyBriefSourceHealth],
        watch_items: Sequence[ProvisionalWatchGatePreview],
    ) -> WeeklyBrief:
        if not slot.produce_allowed or slot.first_session_date is None:
            raise ValueError("weekly brief may be built only for a DUE slot")
        cutoff = slot.cutoff_at
        week_start_at = datetime.combine(
            slot.week_start,
            time.min,
            tzinfo=US_OPTIONS_TIMEZONE,
        )
        prior_start = week_start_at - timedelta(days=7)
        upcoming_start = datetime.combine(
            slot.first_session_date,
            time.min,
            tzinfo=US_OPTIONS_TIMEZONE,
        )
        upcoming_end = upcoming_start + timedelta(days=8)
        next_end = upcoming_start + timedelta(days=15)
        normalized_items = tuple(evidence_items)
        if len({item.item_id for item in normalized_items}) != len(normalized_items):
            raise ValueError("weekly evidence item_ids must be unique")
        if any(item.observed_at > cutoff for item in normalized_items):
            raise ValueError("weekly evidence must be point-in-time at cutoff")
        prior = tuple(
            item
            for item in normalized_items
            if prior_start <= item.occurred_at.astimezone(US_OPTIONS_TIMEZONE) < week_start_at
        )
        upcoming = tuple(
            item
            for item in normalized_items
            if upcoming_start <= item.occurred_at.astimezone(US_OPTIONS_TIMEZONE) < upcoming_end
        )
        next_preview = tuple(
            item
            for item in normalized_items
            if upcoming_end <= item.occurred_at.astimezone(US_OPTIONS_TIMEZONE) < next_end
        )
        if len(prior) + len(upcoming) + len(next_preview) != len(normalized_items):
            raise ValueError("weekly evidence falls outside declared weekly windows")
        health = tuple(sorted(source_health, key=lambda item: item.source))
        source_bundle_hash = weekly_brief_source_bundle_hash(
            calendar_hash=slot.calendar_hash,
            evidence_items=normalized_items,
            source_health=health,
        )
        idempotency_key = weekly_brief_idempotency_key(
            week_start=slot.week_start,
            cutoff_at=cutoff,
        )
        values = {
            "schema": WEEKLY_BRIEF_SCHEMA,
            "version": WEEKLY_BRIEF_VERSION,
            "week_start": slot.week_start,
            "first_session_date": slot.first_session_date,
            "cutoff_at": cutoff,
            "calendar_hash": slot.calendar_hash,
            "prior_week_start": prior_start,
            "prior_week_end": week_start_at,
            "upcoming_start": upcoming_start,
            "upcoming_end": upcoming_end,
            "next_preview_start": upcoming_end,
            "next_preview_end": next_end,
            "source_bundle_hash": source_bundle_hash,
            "source_health": health,
            "prior_week_items": tuple(sorted(prior, key=lambda item: item.item_hash)),
            "upcoming_items": tuple(sorted(upcoming, key=lambda item: item.item_hash)),
            "next_preview_items": tuple(
                sorted(next_preview, key=lambda item: item.item_hash)
            ),
            "watch_items": tuple(sorted(watch_items, key=lambda item: item.watch_id)),
            "idempotency_key": idempotency_key,
        }
        provisional = _provisional(cls, values)
        return cls(**values, content_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "status": "PROVISIONAL",
            "decision": "OBSERVATION_ONLY",
            "decision_authority": "SUPPORTING_ONLY",
            "execution_allowed": False,
            "review_allowed": False,
            "combination_generation_allowed": False,
            "week_start": self.week_start.isoformat(),
            "first_session_date": self.first_session_date.isoformat(),
            "cutoff_at": self.cutoff_at.isoformat(),
            "calendar_hash": self.calendar_hash,
            "windows": {
                "prior_week": {
                    "start": self.prior_week_start.isoformat(),
                    "end_exclusive": self.prior_week_end.isoformat(),
                },
                "upcoming": {
                    "start": self.upcoming_start.isoformat(),
                    "end_exclusive": self.upcoming_end.isoformat(),
                },
                "next_week_preview": {
                    "start": self.next_preview_start.isoformat(),
                    "end_exclusive": self.next_preview_end.isoformat(),
                },
            },
            "source_bundle_hash": self.source_bundle_hash,
            "source_health": tuple(item.as_dict() for item in self.source_health),
            "prior_week_items": tuple(item.as_dict() for item in self.prior_week_items),
            "upcoming_items": tuple(item.as_dict() for item in self.upcoming_items),
            "next_preview_items": tuple(item.as_dict() for item in self.next_preview_items),
            "watch_items": tuple(item.as_dict() for item in self.watch_items),
            "unavailable_fields": {
                "option_chain": {
                    "status": "UNAVAILABLE",
                    "reason_codes": ("OPTION_CHAIN_NOT_BOUND",),
                },
                "option_quotes": {
                    "status": "UNAVAILABLE",
                    "reason_codes": ("OPTION_QUOTES_NOT_BOUND",),
                },
                "strategy_nav": {
                    "status": "UNAVAILABLE",
                    "reason_codes": ("STRATEGY_NAV_NOT_BOUND",),
                },
                "positioning": {
                    "status": "UNAVAILABLE",
                    "reason_codes": ("POSITIONING_NOT_BOUND",),
                },
            },
            "target_count": MAXIMUM_WATCH_ITEMS,
            "watch_count": len(self.watch_items),
            "idempotency_key": self.idempotency_key,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "content_hash": self.content_hash}

    def append_payload(self) -> dict[str, object]:
        return {
            "schema": WEEKLY_BRIEF_APPEND_SCHEMA,
            "record_type": "WEEKLY_BRIEF_PROVISIONAL",
            "idempotency_key": self.idempotency_key,
            "content_hash": self.content_hash,
            "weekly_brief": self.as_dict(),
        }


def weekly_brief_idempotency_key(*, week_start: date, cutoff_at: datetime) -> str:
    """Identify one exact weekly producer slot independently of mutable sources."""

    return canonical_hash(
        {
            "week_start": week_start.isoformat(),
            "cutoff_at": datetime_text(utc_datetime(cutoff_at, field="cutoff_at")),
        },
    )


def weekly_brief_source_bundle_hash(
    *,
    calendar_hash: str,
    evidence_items: Sequence[WeeklyBriefEvidenceItem],
    source_health: Sequence[WeeklyBriefSourceHealth],
) -> str:
    """Hash source evidence independently of derived watch previews."""

    return canonical_hash(
        {
            "calendar_hash": _digest("calendar_hash", calendar_hash),
            "evidence_items": tuple(
                item.as_dict() for item in sorted(evidence_items, key=lambda value: value.item_hash)
            ),
            "source_health": tuple(
                item.as_dict() for item in sorted(source_health, key=lambda value: value.source)
            ),
        },
    )


def _session_date(value: object) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError("official_session_dates must contain date values")
    return value


def _identity(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be blank")
    return value.strip()


def _digest(name: str, value: object) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _HEX for char in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")
    return value


def _codes(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("reason_codes must be a sequence")
    return tuple(sorted({_identity("reason_code", value).upper() for value in values}))


def _labels(values: Sequence[str], *, upper: bool = False) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("labels must be a sequence")
    normalized = (_identity("label", value) for value in values)
    return tuple(sorted({value.upper() if upper else value for value in normalized}))


def _provisional(cls: type[Any], values: Mapping[str, object]) -> Any:
    instance = object.__new__(cls)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


__all__ = [
    "MAXIMUM_WATCH_ITEMS",
    "SourceHealthStatus",
    "WEEKLY_BRIEF_APPEND_SCHEMA",
    "WEEKLY_BRIEF_SCHEMA",
    "WEEKLY_BRIEF_SLOT_SCHEMA",
    "WEEKLY_BRIEF_VERSION",
    "WeeklyBrief",
    "WeeklyBriefEvidenceItem",
    "WeeklyBriefSlotDecision",
    "WeeklyBriefSlotStatus",
    "WeeklyBriefSourceHealth",
    "evaluate_weekly_brief_slot",
    "weekly_brief_idempotency_key",
    "weekly_brief_source_bundle_hash",
]
