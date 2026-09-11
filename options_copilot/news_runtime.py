"""Durable, read-only coordination for news and event-calendar research.

This module is intentionally outside the proposal and approval composition
roots.  It polls external fact providers, stores immutable point-in-time
evidence, and builds a display-only research projection.  It never imports a
broker write, approval, bridge, or instruction-creation primitive.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path
import re
import threading
import time as monotonic_time
from typing import Protocol
import urllib.parse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.market.session_calendar import UsOptionsCalendarSnapshot
from options_copilot.news import CalendarEvent, EventCategory, NewsAnalysisService, NewsInput
from options_copilot.news.analysis_store import (
    NewsAnalysisStore,
    analysis_contract,
    analysis_input_document,
    analysis_store_path,
)
from options_copilot.news.classifier import DeterministicNewsClassifier, NewsClassifier
from options_copilot.news.cadence import SourceCadenceStore, canonical_source_id
from options_copilot.news.intelligence import project_event_intelligence
from options_copilot.news.advisory_models import ADVISORY_SCHEMA_VERSION
from options_copilot.news.models import (
    AnalyzedNews,
    AnalysisStage,
    ClassifiedEvent,
    ConditionalOptionPreselection,
    MarketConfirmation,
    NewsAuthority,
    OptionTradabilityInput,
)
from options_copilot.news.macro_proxy import (
    bind_market_proxy,
    bind_research_proxy,
    require_current_research_proxy_binding,
)
from options_copilot.news.preselection import MAXIMUM_QUOTE_AGE, build_preselection_pools
from options_copilot.news.publication import PublicationDiagnostic, RefreshProgress
from options_copilot.news.reaction import (
    EventReactionLedger,
    ReactionStage,
    ScheduledEventIdentity,
)
from options_copilot.news.reaction_specs import (
    EventFamily,
    FAMILY_SPECS,
    classify_event_family,
    parent_identity_from_calendar,
)
from options_copilot.news.shadow_research import (
    ResearchAdvisoryInput,
    ResearchAdvisoryProjection,
    ShadowResearchAdvisory,
    research_advisory_input_hash,
)
from options_copilot.news.shadow_store import NewsShadowLearningWriter
from options_copilot.news.scoring import combined_opportunity_score, score_band
from options_copilot.news.weekly_brief_runtime import WeeklyBriefRuntime
from options_copilot.news.weekly_brief_store import WeeklyBriefStore
from options_copilot.news.research_session_calendar import (
    BoundedResearchSessionCalendar,
)
from options_copilot.providers import (
    EarningsEvent,
    NewsAggregator,
    NewsEvent,
    OfficialCalendarEvent,
    OfficialCalendarSnapshot,
    OfficialEventProvenance,
    OfficialSourceHealth,
)
from options_copilot.providers.entity_linking import (
    ENTITY_LINK_CATALOG_HASH,
    ENTITY_LINK_CATALOG_VERSION,
)
from options_copilot.providers.sec_identity import sec_filer_group_identity
from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore, StoredEvidence
from options_copilot.storage.canonical import canonical_hash, datetime_text, utc_datetime


_UTC = timezone.utc
_EASTERN = ZoneInfo("America/New_York")
_READY_STATES = frozenset({"UP", "READY", "HEALTHY"})
_UNCONFIGURED_STATES = frozenset({"DISABLED", "UNCONFIGURED"})
_MAXIMUM_IBKR_BINDING_AGE = timedelta(seconds=5)
_OFFICIAL_ANCHOR_SOURCES = frozenset({"SEC", "COMPANY IR"})
_OFFICIAL_CALENDAR_SUCCESS_TTL = timedelta(minutes=15)
_OFFICIAL_CALENDAR_FAILURE_RETRY = timedelta(minutes=5)
_DURABLE_OFFICIAL_CALENDAR_LIMIT = 5000
_DURABLE_OFFICIAL_CALENDAR_REASON = "DURABLE_OFFICIAL_CALENDAR_RESTORED_STALE"
_MAX_HISTORICAL_REACTION_EVENTS = 100
_REACTION_OBSERVATION_POLL_SECONDS = 5.0
_REACTION_QUOTE_SAMPLE_POLL_SECONDS = 1.0
_REACTION_QUOTE_SAMPLE_RETRY_SECONDS = 5.0
_REACTION_CAPTURE_POLL_SECONDS = 30.0
_REACTION_SCHEDULE_POLL_SECONDS = 15 * 60.0
# Acquisition providers can append substantially more than 500 records between
# decision cycles.  Search the store's maximum bounded recent window so a
# lower-frequency macro release cannot be starved by a high-frequency feed.
# The public projection remains separately capped below.
_NEWS_READ_MODEL_EVIDENCE_LIMIT = 5_000
_NEWS_READ_MODEL_OUTPUT_LIMIT = 500
_EQUITY_NEWS_READ_MODEL_OUTPUT_LIMIT = _NEWS_READ_MODEL_EVIDENCE_LIMIT
_EQUITY_NEWS_PER_SYMBOL_LIMIT = 50
_CALENDAR_READ_MODEL_EVIDENCE_LIMIT = 5000
_NEWS_READ_MODEL_MAX_AGE = timedelta(days=14)
_NEWS_ANALYSIS_BATCH_SIZE = 10
_NEWS_ANALYSIS_LOOKUP_BATCH_SIZE = 500
_NEWS_LOCAL_RESTORE_ANALYSIS_BATCH_SIZE = _NEWS_READ_MODEL_EVIDENCE_LIMIT
_NEWS_ANALYSIS_INTEGRITY_BATCH_SIZE = 5000
_NEWS_LOCAL_RESTORE_BATCH_LIMIT = 16
_WEEKLY_OBSERVATION_POLL_SECONDS = 30.0
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_LEGACY_HASHED_NEWS_EVENT_RE = re.compile(r"evt_[0-9a-f]{32}\Z")
_SOURCE_BATCH_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SOURCE_HEALTH_REASON_CODES = frozenset(
    {
        "CLOCK_INVALID",
        "CLOCK_REGRESSED",
        "INVALID_ATOM",
        "INVALID_RECORDS",
        "NASDAQ_EARNINGS_PARTIAL_WINDOW",
        "NASDAQ_EARNINGS_UNAVAILABLE",
        "NOT_FETCHED",
        "NOT_OBSERVED",
        "NO_USABLE_RECORDS",
        "NO_VERIFIED_RELATED_RECORDS",
        "PROVIDER_RELATED_ENTITY_PROOF_MISSING",
        "SYMBOL_BINDING_UNVERIFIED",
        "OFFICIAL_CALENDAR_EVENT_INCOMPLETE",
        "OFFICIAL_CALENDAR_EVENT_REJECTED",
        "OFFICIAL_CALENDAR_PROVIDER_DEGRADED",
        "OFFICIAL_CALENDAR_PROVIDER_UNAVAILABLE",
        "OFFICIAL_CALENDAR_SNAPSHOT_INVALID",
        "OFFICIAL_CALENDAR_SNAPSHOT_STALE_OR_MISALIGNED",
        "OFFICIAL_CALENDAR_SOURCE_HEALTH_MISSING",
        "OFFICIAL_SOURCES_NOT_CONFIGURED",
        "OFFICIAL_SOURCE_DEGRADED",
        "PARTIAL_PARSE",
        "PARTIAL_TOOL_FAILURE",
        "PROVIDER_DEGRADED",
        "RATE_LIMITED",
        "COOLDOWN_ACTIVE",
        "AUTHENTICATION_FAILED",
        "BAD_JSON",
        "CREDENTIAL_NOT_ACTIVATED",
        "REQUEST_FAILED",
        "REQUEST_TIMEOUT",
        "SYMBOL_BINDING_REJECTED",
        "VERIFIED_RELATED_RECORD_REJECTED",
        "TICKER_RESOLUTION_FAILED",
        "TRANSPORT_UNVERIFIED",
        "UNSAFE_XML",
        "NOT_CONFIGURED",
        "UNCONFIGURED",
        "PACING_UNVERIFIED",
        "SOURCE_STALE",
    }
)
_PHASE2_SOURCE_IDS = (
    "sec",
    "nasdaq",
    "company_ir",
    "finnhub",
    "alpha_vantage",
    "jin10",
)
_PHASE2_SOURCE_STATES = frozenset(
    {
        "READY",
        "STALE",
        "UNCONFIGURED",
        "NOT_CONFIGURED",
        "RATE_LIMITED",
        "LIMITED",
        "FAILED",
        "UNAVAILABLE",
        "DEGRADED",
        "DOWN",
        "TIMEOUT",
        "BAD_JSON",
        "UNKNOWN",
    }
)
_PHASE2_PACING_STATES = frozenset(
    {"VERIFIED", "PACING_UNVERIFIED", "RATE_LIMITED", "LIMITED"}
)


class NewsProvider(Protocol):
    def news(self, symbols: tuple[str, ...], *, limit: int = 50) -> Iterable[NewsEvent]: ...


class EarningsCalendarProvider(Protocol):
    def earnings_calendar(self, start: date, end: date) -> Iterable[EarningsEvent]: ...


class OfficialCalendarSnapshotProvider(Protocol):
    """Read-only seam for one provider-normalized, future two-week snapshot."""

    def future_two_weeks(self, *, now: datetime | None = None) -> OfficialCalendarSnapshot: ...


@dataclass(frozen=True, slots=True)
class IbkrNewsBinding:
    """One exact, point-in-time quote and market-confirmation binding.

    The quote snapshot ID must also be the confirmation evidence ID, which
    prevents a confirmation for one observation from being paired with another
    quote.  This remains research-only and grants no approval authority.
    """

    symbol: str
    quote_snapshot_id: str
    tradability: OptionTradabilityInput
    confirmation: MarketConfirmation

    def __post_init__(self) -> None:
        symbol = _symbols((self.symbol,))[0]
        object.__setattr__(self, "symbol", symbol)
        snapshot_id = str(self.quote_snapshot_id).strip()
        if not snapshot_id or len(snapshot_id) > 160:
            raise ValueError("IBKR quote_snapshot_id is invalid")
        object.__setattr__(self, "quote_snapshot_id", snapshot_id)
        if self.tradability.symbol != symbol:
            raise ValueError("IBKR tradability symbol does not match binding symbol")
        if self.tradability.observed_at != self.confirmation.observed_at:
            raise ValueError("IBKR quote and confirmation timestamps must match")
        if snapshot_id not in self.confirmation.evidence_ids:
            raise ValueError("IBKR confirmation is not bound to the quote snapshot")


class IbkrNewsBindingProvider(Protocol):
    def bindings(self, symbols: tuple[str, ...]) -> Iterable[IbkrNewsBinding]: ...


class OptionPreselectionProvider(Protocol):
    """Read-only seam for already-resolved option-chain research."""

    def preselections(self) -> Iterable[ConditionalOptionPreselection]: ...


class EventReactionProvider(Protocol):
    """Read-only seam for already-built official-event reaction ledgers."""

    def reactions(self, event_ids: tuple[str, ...]) -> Iterable[EventReactionLedger]: ...


# Short alias for callers that use the constructor argument's terminology.
ReactionProvider = EventReactionProvider


@dataclass(frozen=True, slots=True)
class _RefreshHealth:
    status: str
    latency_ms: float | None
    asof: datetime | None
    message: str

    def as_dict(self, *, name: str) -> dict[str, object]:
        return {
            "name": name,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "asof": None if self.asof is None else self.asof.isoformat(),
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class _OfficialCalendarCacheEntry:
    """One honest provider observation and its minimum refresh boundary.

    ``observed_at`` is the time of the real provider attempt, not the time a
    cached value is read.  Keeping that timestamp immutable prevents the
    60-120 second news loop from making an older official observation appear
    newly fetched.
    """

    snapshot: OfficialCalendarSnapshot | None
    reasons: tuple[str, ...]
    failed: bool
    observed_at: datetime
    refresh_not_before: datetime


@dataclass(frozen=True, slots=True)
class _OfficialCalendarRefresh:
    failed: bool
    observed_at: datetime | None
    cache_hit: bool


@dataclass(frozen=True, slots=True)
class _CachedNewsAnalysis:
    signature: str
    analysis: AnalyzedNews
    evidence: tuple[StoredEvidence, ...]
    conflicted: bool
    binding: IbkrNewsBinding | None


class _AnalysisLookupMiss(LookupError):
    """Internal control flow for a verified fingerprint cache miss."""


class NewsCoordinator:
    """Poll providers and expose a restart-safe, observation-only read model."""

    def __init__(
        self,
        evidence_path: str | Path,
        *,
        news_providers: Sequence[NewsProvider] = (),
        calendar_providers: Sequence[EarningsCalendarProvider] = (),
        official_calendar_provider: OfficialCalendarSnapshotProvider | None = None,
        reaction_schedule_provider: object | None = None,
        official_calendar_snapshot: OfficialCalendarSnapshot | None = None,
        ibkr_binding_provider: IbkrNewsBindingProvider | None = None,
        preselection_provider: OptionPreselectionProvider | None = None,
        reaction_provider: EventReactionProvider | None = None,
        classifier: NewsClassifier | None = None,
        shadow_advisory: ShadowResearchAdvisory | None = None,
        phase2_advisory: object | None = None,
        phase2_advisory_fallback_reason: str = "MODEL_DISABLED",
        shadow_writer: NewsShadowLearningWriter | None = None,
        core_symbols: Sequence[str] = (),
        clock: Callable[[], datetime] | None = None,
        poll_interval_seconds: int = 90,
        cadence_path: str | Path | None = None,
    ) -> None:
        if isinstance(poll_interval_seconds, bool) or not 60 <= poll_interval_seconds <= 120:
            raise ValueError("news polling interval must be between 60 and 120 seconds")
        self._clock = clock or (lambda: datetime.now(_UTC))
        if official_calendar_provider is not None and official_calendar_snapshot is not None:
            raise ValueError(
                "official_calendar_provider and official_calendar_snapshot are mutually exclusive"
            )
        if official_calendar_snapshot is not None and not isinstance(
            official_calendar_snapshot, OfficialCalendarSnapshot
        ):
            raise TypeError("official_calendar_snapshot must be an OfficialCalendarSnapshot")
        self._news_providers = tuple(news_providers)
        self._calendar_providers = tuple(calendar_providers)
        self._official_calendar_provider = official_calendar_provider
        if reaction_schedule_provider is not None:
            self._reaction_schedule_provider = reaction_schedule_provider
        else:
            fork_schedule = getattr(
                official_calendar_provider,
                "fork_reaction_schedule_provider",
                None,
            )
            self._reaction_schedule_provider = (
                fork_schedule() if callable(fork_schedule) else official_calendar_provider
            )
        self._injected_official_calendar_snapshot = official_calendar_snapshot
        self._official_calendar_snapshot: OfficialCalendarSnapshot | None = None
        self._official_calendar_reasons: tuple[str, ...] = ()
        self._official_calendar_cache: _OfficialCalendarCacheEntry | None = None
        self._official_calendar_last_valid_snapshot: OfficialCalendarSnapshot | None = None
        self._ibkr_binding_provider = ibkr_binding_provider
        self._ibkr_bindings: dict[str, IbkrNewsBinding] = {}
        self._preselection_provider = preselection_provider
        self._preselections: tuple[ConditionalOptionPreselection, ...] = ()
        self._preselection_lineage: dict[
            tuple[str, str], dict[str, object]
        ] = {}
        self._preselection_atomic_binding_available = False
        self._preselection_coverage = _preselection_coverage(preselection_provider)
        self._preselection_action_expires_at: datetime | None = None
        self._reaction_provider = reaction_provider
        self._shadow_advisory = shadow_advisory
        self._phase2_advisory = phase2_advisory
        self._phase2_advisory_fallback_reason = _phase2_fallback_reason(
            phase2_advisory_fallback_reason
        )
        self._shadow_writer = shadow_writer
        self._core_symbols = _symbols(core_symbols)
        self._poll_interval_seconds = poll_interval_seconds
        self._cadence_enabled = cadence_path is not None
        self._cadence = SourceCadenceStore(
            cadence_path
            if cadence_path is not None
            else Path(evidence_path).with_name("news_source_cadence.json")
        )
        news_cadence_lanes = [
            (_provider_source_name(provider, {}, source_kind="NEWS"), "NEWS")
            for provider in self._news_providers
        ]
        calendar_cadence_lanes = [
            (
                _provider_source_name(provider, {}, source_kind="CALENDAR"),
                "CALENDAR",
            )
            for provider in self._calendar_providers
        ]
        all_cadence_lanes = [*news_cadence_lanes, *calendar_cadence_lanes]
        active_cadence_lanes = [
            lane
            for provider, lane in zip(
                (*self._news_providers, *self._calendar_providers),
                all_cadence_lanes,
                strict=True,
            )
            if _provider_explicitly_configured(provider)
        ]
        if official_calendar_provider is not None or official_calendar_snapshot is not None:
            official_lane = ("OFFICIAL_CALENDAR", "OFFICIAL_CALENDAR")
            all_cadence_lanes.append(official_lane)
            active_cadence_lanes.append(official_lane)
        # Preserve an explicit SUPPRESSED row for declared-but-unconfigured
        # providers while reconciling only genuinely configured lanes active.
        self._cadence.reconcile(all_cadence_lanes, persist=False)
        self._cadence.reconcile(
            active_cadence_lanes,
            persist=self._cadence_enabled,
        )
        selected_classifier = (
            classifier if classifier is not None else DeterministicNewsClassifier()
        )
        self._analysis_contract = analysis_contract(selected_classifier)
        self._analysis = NewsAnalysisService(
            classifier=selected_classifier,
            now=self._clock,
        )
        self.evidence_store = EvidenceStore(evidence_path, clock=self._clock)
        try:
            self.analysis_store = NewsAnalysisStore(
                analysis_store_path(evidence_path),
                defer_integrity_check=True,
            )
        except BaseException:
            self.evidence_store.close()
            raise
        try:
            weekly_path = Path(evidence_path).with_name("weekly_briefs.sqlite3")
            self.weekly_brief_store = WeeklyBriefStore(weekly_path, clock=self._clock)
            self.weekly_brief_runtime = WeeklyBriefRuntime(self.weekly_brief_store)
        except BaseException:
            self.analysis_store.close()
            self.evidence_store.close()
            raise
        self._refresh_lock = threading.Lock()
        self._reaction_quote_sampler_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._reaction_thread: threading.Thread | None = None
        self._reaction_quote_sampler_thread: threading.Thread | None = None
        self._reaction_publication_thread: threading.Thread | None = None
        self._reaction_capture_thread: threading.Thread | None = None
        self._reaction_schedule_thread: threading.Thread | None = None
        self._reaction_publication_requested = threading.Event()
        self._reaction_publication_generation = 0
        self._reaction_published_generation = 0
        self._weekly_thread: threading.Thread | None = None
        self._weekly_schedule = BoundedResearchSessionCalendar()
        self._weekly_scheduler_failure: dict[str, object] | None = None
        self._closed = False
        self._closing = False
        initial_news = (
            _RefreshHealth("UNKNOWN", None, None, "news providers have not refreshed")
            if self._news_providers
            else _RefreshHealth("UNCONFIGURED", None, None, "no news providers are configured")
        )
        official_calendar_configured = (
            self._official_calendar_provider is not None
            or self._injected_official_calendar_snapshot is not None
        )
        initial_calendar = (
            _RefreshHealth("UNKNOWN", None, None, "calendar providers have not refreshed")
            if self._calendar_providers or official_calendar_configured
            else _RefreshHealth("UNCONFIGURED", None, None, "no calendar providers are configured")
        )
        self._news_health = initial_news
        self._calendar_health = initial_calendar
        self._source_health: tuple[dict[str, object], ...] = ()
        initial_as_of = _aware(self._clock())
        self._phase2_source_rows = _phase2_source_rows(
            (
                *((provider, "NEWS") for provider in self._news_providers),
                *((provider, "CALENDAR") for provider in self._calendar_providers),
            ),
            attempted_at=initial_as_of,
            cadence_rows=(
                self._cadence.projections(now=initial_as_of)
                if self._cadence_enabled
                else ()
            ),
        )
        self._news_payload: dict[str, object] = {}
        self._equity_news_payload: dict[str, object] = {}
        self._calendar_payload: dict[str, object] = {}
        self._calendar_envelope: dict[str, object] | None = None
        self._advisory_payload = _phase2_advisory_fallback_payload(
            as_of=initial_as_of,
            reason=self._phase2_advisory_fallback_reason,
        )
        self._source_evidence_payload = _phase2_source_evidence_payload(
            rows=self._phase2_source_rows,
            as_of=initial_as_of,
            conflicts=(),
        )
        self._analysis_cache: dict[str, _CachedNewsAnalysis] = {}
        self._analysis_ignored: dict[str, str] = {}
        self._analysis_pending: list[tuple[str, str]] = []
        self._analysis_model_pending: list[tuple[str, str]] = []
        self._analysis_failures: set[tuple[str, str]] = set()
        self._analysis_backfill_status = "PENDING"
        self._analysis_integrity: dict[str, object] = {
            "status": "PENDING",
            "batch_rows": 0,
            "verified_rows": 0,
            "remaining_rows": self.analysis_store.count,
            "complete": False,
        }
        self._publication_diagnostic = PublicationDiagnostic(
            observed_at=initial_as_of,
        ).capture_sources(
            source_health=(),
            source_runtime=self._cadence.projections(now=initial_as_of),
            observed_at=initial_as_of,
            default_freshness_seconds=self._poll_interval_seconds,
        )
        try:
            # Never project or classify unverified persisted evidence.  A
            # broken chain must fail construction before any read model exists.
            self.evidence_store.assert_integrity()
            self._restore_durable_official_calendar()
            # Ledger-backed preselections are local durable state, so restore
            # them during construction rather than waiting for a poll cycle.
            # Other provider shapes retain their historical refresh behavior.
            if callable(getattr(self._preselection_provider, "read_snapshot", None)):
                self._refresh_preselections()
            # Construction publishes a bounded, fail-closed projection but
            # never invokes the classifier.  Persisted analyses are restored
            # in deterministic batches by refresh_once(), which keeps an LLM
            # adapter out of the synchronous GUI startup path.
            self._rebuild_read_model(analysis_budget=0)
        except BaseException:
            self._closed = True
            self.analysis_store.close()
            self.evidence_store.close()
            raise

    def _restore_durable_official_calendar(self) -> None:
        """Restore typed official identities without manufacturing freshness.

        The append-only evidence ledger is the only restart-safe source for a
        schedule that was observed before a provider outage.  Rehydration
        preserves every original timestamp and is deliberately installed as a
        degraded last-valid snapshot.  A later live provider failure therefore
        remains ``NO_TRADE`` and the restored rows never become a current
        official snapshot or gain reaction/trading authority.
        """

        if self._official_calendar_provider is None:
            return
        groups: dict[str, list[StoredEvidence]] = defaultdict(list)
        for stored in self.evidence_store.iter_verified(
            kinds=("CALENDAR",),
            page_size=_DURABLE_OFFICIAL_CALENDAR_LIMIT,
        ):
            if stored.record.payload.get("calendar_origin") != "OFFICIAL":
                continue
            event_id = str(stored.record.payload.get("event_id") or "").strip()
            if event_id:
                groups[event_id].append(stored)

        restored: list[OfficialCalendarEvent] = []
        for rows in groups.values():
            versions = {
                (
                    str(row.record.payload.get("content_hash") or ""),
                    str(row.record.payload.get("record_hash") or ""),
                )
                for row in rows
            }
            if len(versions) != 1:
                # A changed schedule cannot be resolved from durable evidence
                # alone; only a new authoritative provider snapshot may do so.
                continue
            event = _restored_official_calendar_event(rows[-1])
            if event is not None:
                restored.append(event)
        if not restored:
            return

        restored.sort(
            key=lambda event: (
                event.scheduled_at or datetime.max.replace(tzinfo=_UTC),
                event.event_id,
            )
        )
        observed_at = max(event.observed_at for event in restored)
        sources: list[OfficialSourceHealth] = []
        for source, source_url in sorted(
            {(event.source, event.source_url) for event in restored}
        ):
            source_events = tuple(
                event
                for event in restored
                if event.source == source and event.source_url == source_url
            )
            source_observed_at = max(event.observed_at for event in source_events)
            sources.append(
                OfficialSourceHealth(
                    source=source,
                    source_url=source_url,
                    status="STALE",
                    reason=_DURABLE_OFFICIAL_CALENDAR_REASON,
                    observed_at=source_observed_at,
                    event_count=len(source_events),
                    last_success_at=source_observed_at,
                    as_of=source_observed_at,
                    freshness_age_seconds=0,
                    provenance=("HASH_VERIFIED_DURABLE_CALENDAR",),
                )
            )
        snapshot = OfficialCalendarSnapshot(
            status="DEGRADED",
            decision="NO_TRADE",
            window_start=observed_at,
            window_end=observed_at + timedelta(days=14),
            observed_at=observed_at,
            events=tuple(restored),
            sources=tuple(sources),
            reasons=(_DURABLE_OFFICIAL_CALENDAR_REASON,),
        )
        self._official_calendar_last_valid_snapshot = snapshot
        restore_identities = getattr(
            self._reaction_provider,
            "restore_official_identities",
            None,
        )
        if callable(restore_identities):
            try:
                restore_identities(snapshot.events)
            except Exception:
                # Calendar recovery remains useful for display even when the
                # optional reaction adapter rejects an identity batch.
                self._official_calendar_reasons = (
                    "REACTION_IDENTITY_RESTORE_UNAVAILABLE",
                )

    @contextmanager
    def _publication_cycle(self, initial_stage: str) -> Iterator[None]:
        """Track one owner-lane cycle without retaining failure details."""

        started_at = _aware(self._clock())
        started_monotonic = monotonic_time.perf_counter()
        with self._state_lock:
            self._publication_diagnostic = self._publication_diagnostic.with_progress(
                RefreshProgress(
                    status="RUNNING",
                    stage=initial_stage,
                    cycle_started_at=started_at,
                    stage_started_at=started_at,
                    stage_started_monotonic=started_monotonic,
                )
            )
        try:
            yield
        except BaseException:
            self._finish_publication_cycle(status="FAILED")
            raise
        else:
            self._finish_publication_cycle(status="COMPLETED")

    def _begin_publication_stage(self, stage: str) -> None:
        """Finalize the prior stage and publish a new active stage atomically."""

        observed_at = _aware(self._clock())
        monotonic_now = monotonic_time.perf_counter()
        with self._state_lock:
            progress = self._publication_diagnostic.progress
            durations = list(progress.stage_durations_ms)
            if progress.stage_started_monotonic is not None:
                durations.append(
                    (
                        progress.stage,
                        max(
                            0.0,
                            (monotonic_now - progress.stage_started_monotonic) * 1000,
                        ),
                    )
                )
            self._publication_diagnostic = self._publication_diagnostic.with_progress(
                RefreshProgress(
                    status="RUNNING",
                    stage=stage,
                    cycle_started_at=progress.cycle_started_at,
                    stage_started_at=observed_at,
                    stage_started_monotonic=monotonic_now,
                    stage_durations_ms=tuple(durations),
                )
            )

    def _finish_publication_cycle(self, *, status: str) -> None:
        completed_at = _aware(self._clock())
        monotonic_now = monotonic_time.perf_counter()
        with self._state_lock:
            progress = self._publication_diagnostic.progress
            durations = list(progress.stage_durations_ms)
            if progress.stage_started_monotonic is not None:
                durations.append(
                    (
                        progress.stage,
                        max(
                            0.0,
                            (monotonic_now - progress.stage_started_monotonic) * 1000,
                        ),
                    )
                )
            self._publication_diagnostic = self._publication_diagnostic.with_progress(
                RefreshProgress(
                    status=status,
                    stage=("IDLE" if status == "COMPLETED" else progress.stage),
                    cycle_started_at=progress.cycle_started_at,
                    cycle_completed_at=completed_at,
                    stage_durations_ms=tuple(durations),
                )
            )

    def _capture_source_diagnostic(
        self,
        source_health: Sequence[Mapping[str, object]],
    ) -> None:
        """Capture cadence only on the owning lane, never from a public reader."""

        captured_at = _aware(self._clock())
        runtime_rows = self._cadence.projections(now=captured_at)
        with self._state_lock:
            self._publication_diagnostic = (
                self._publication_diagnostic.capture_sources(
                    source_health=source_health,
                    source_runtime=runtime_rows,
                    observed_at=captured_at,
                    default_freshness_seconds=self._poll_interval_seconds,
                )
            )

    def refresh_once(self) -> dict[str, object]:
        """Run one bounded provider cycle; provider failures never escape."""

        self._ensure_open()
        with self._refresh_lock, self._publication_cycle("NEWS_PROVIDERS"):
            now = _aware(self._clock())
            news_groups: list[tuple[NewsEvent, ...]] = []
            source_health: list[dict[str, object]] = []
            phase2_forced_reasons: dict[int, str] = {}
            phase2_cycle_observations: dict[int, dict[str, object]] = {}
            phase2_cycle_health: dict[int, dict[str, object]] = {}
            news_failed = False
            calendar_failed = False
            news_provider_attempted = False

            news_started = monotonic_time.perf_counter()
            for provider in self._news_providers:
                if not _provider_explicitly_configured(provider):
                    provider_health = _provider_source_health(
                        provider,
                        source_kind="NEWS",
                        success_count=0,
                        failure_date_count=0,
                        attempted_at=now,
                        forced_reason=None,
                    )
                    source_health.append(provider_health)
                    phase2_cycle_health[id(provider)] = provider_health
                    self._capture_source_diagnostic(source_health)
                    continue
                cadence_source = canonical_source_id(
                    _provider_source_name(provider, {}, source_kind="NEWS")
                )
                cadence_due, cadence_reason = (
                    self._cadence.due(cadence_source, "NEWS", now=now)
                    if self._cadence_enabled
                    else (True, "CADENCE_DUE")
                )
                if not cadence_due:
                    provider_health = _cadence_skipped_health(
                        self._cadence,
                        cadence_source,
                        "NEWS",
                        now=now,
                        reason=cadence_reason,
                    )
                    source_health.append(provider_health)
                    phase2_cycle_health[id(provider)] = provider_health
                    news_failed = news_failed or cadence_reason == "CADENCE_STATE_CORRUPT"
                    self._capture_source_diagnostic(source_health)
                    continue
                news_provider_attempted = True
                if not _provider_transport_verified(provider):
                    # Jin10 remains disabled until a separately verified
                    # transport contract exists.  Possession of any token is
                    # deliberately insufficient and no provider call occurs.
                    news_failed = True
                    provider_health = _provider_source_health(
                        provider,
                        source_kind="NEWS",
                        success_count=0,
                        failure_date_count=0,
                        attempted_at=now,
                        forced_reason="TRANSPORT_UNVERIFIED",
                    )
                    source_health.append(provider_health)
                    phase2_cycle_health[id(provider)] = provider_health
                    phase2_forced_reasons[id(provider)] = "TRANSPORT_UNVERIFIED"
                    if self._cadence_enabled:
                        self._cadence.record(
                            cadence_source,
                            "NEWS",
                            now=now,
                            success=False,
                            failure_code="TRANSPORT_UNVERIFIED",
                        )
                    self._capture_source_diagnostic(source_health)
                    continue
                success_count = 0
                forced_reason: str | None = None
                checked: tuple[NewsEvent, ...] = ()
                try:
                    raw_rows = tuple(provider.news(self._core_symbols, limit=50))
                    checked = tuple(
                        item for item in raw_rows if isinstance(item, NewsEvent)
                    )
                    success_count = len(checked)
                    if len(checked) != len(raw_rows):
                        forced_reason = "INVALID_RECORDS"
                        news_failed = True
                    news_groups.append(checked)
                    news_failed = news_failed or not _provider_is_ready(provider)
                except Exception:
                    news_failed = True
                    forced_reason = "REQUEST_FAILED"
                provider_health = _provider_source_health(
                    provider,
                    source_kind="NEWS",
                    success_count=success_count,
                    failure_date_count=0,
                    attempted_at=now,
                    forced_reason=forced_reason,
                )
                source_health.append(provider_health)
                phase2_cycle_health[id(provider)] = provider_health
                phase2_cycle_observations[id(provider)] = (
                    _phase2_news_cycle_observation(provider_health, checked)
                )
                if forced_reason is not None:
                    phase2_forced_reasons[id(provider)] = forced_reason
                if self._cadence_enabled:
                    self._cadence.record(
                        cadence_source,
                        "NEWS",
                        now=now,
                        success=provider_health["status"] == "READY",
                        failure_code=provider_health.get("reason"),
                    )
                self._capture_source_diagnostic(source_health)
            news_latency = round((monotonic_time.perf_counter() - news_started) * 1000, 2)

            self._begin_publication_stage("NEWS_APPEND")
            merged = NewsAggregator.merge(*news_groups) if news_groups else ()
            for event in merged:
                try:
                    self._append_news(event)
                except Exception:
                    # Malformed provider records are observation failures.  No
                    # exception details are retained because they may contain
                    # credentials or transport headers.
                    news_failed = True

            self._begin_publication_stage("IBKR_BINDINGS")
            news_failed = self._refresh_ibkr_bindings(now) or news_failed
            self._begin_publication_stage("PRESELECTIONS")
            news_failed = self._refresh_preselections() or news_failed

            self._begin_publication_stage("CALENDAR_PROVIDERS")
            calendar_started = monotonic_time.perf_counter()
            declared_window_end = now + timedelta(days=14)
            provider_window_start = now.astimezone(_EASTERN).date()
            provider_window_end = declared_window_end.astimezone(_EASTERN).date()
            calendar_source_batches: list[dict[str, object]] = []
            calendar_generation_complete = True
            calendar_provider_attempted = False
            for provider in self._calendar_providers:
                if not _provider_explicitly_configured(provider):
                    provider_health = _provider_source_health(
                        provider,
                        source_kind="CALENDAR",
                        success_count=0,
                        failure_date_count=0,
                        attempted_at=now,
                        forced_reason=None,
                    )
                    source_health.append(provider_health)
                    phase2_cycle_health[id(provider)] = provider_health
                    self._capture_source_diagnostic(source_health)
                    continue
                cadence_source = canonical_source_id(
                    _provider_source_name(provider, {}, source_kind="CALENDAR")
                )
                cadence_due, cadence_reason = (
                    self._cadence.due(cadence_source, "CALENDAR", now=now)
                    if self._cadence_enabled
                    else (True, "CADENCE_DUE")
                )
                if not cadence_due:
                    provider_health = _cadence_skipped_health(
                        self._cadence,
                        cadence_source,
                        "CALENDAR",
                        now=now,
                        reason=cadence_reason,
                    )
                    source_health.append(provider_health)
                    phase2_cycle_health[id(provider)] = provider_health
                    calendar_generation_complete = False
                    calendar_failed = calendar_failed or cadence_reason == "CADENCE_STATE_CORRUPT"
                    self._capture_source_diagnostic(source_health)
                    continue
                calendar_provider_attempted = True
                success_count = 0
                failure_date_count = 0
                forced_reason: str | None = None
                batch_members: list[dict[str, object]] = []
                try:
                    for range_start, range_end in _calendar_provider_date_windows(
                        provider_window_start,
                        provider_window_end,
                    ):
                        rows = tuple(provider.earnings_calendar(range_start, range_end))
                        failure_date_count += _provider_failure_date_count(provider)
                        if not _provider_is_ready(provider):
                            calendar_failed = True
                            if forced_reason is None:
                                forced_reason = str(
                                    getattr(provider, "health_reason", None)
                                    or "PROVIDER_DEGRADED"
                                )
                        for event in rows:
                            if not isinstance(event, EarningsEvent):
                                calendar_failed = True
                                forced_reason = "INVALID_RECORDS"
                                continue
                            try:
                                stored = self._append_earnings(event)
                            except Exception:
                                calendar_failed = True
                                forced_reason = "INVALID_RECORDS"
                                continue
                            batch_members.append(
                                _calendar_envelope_member(stored)
                            )
                            success_count += 1
                except Exception:
                    calendar_failed = True
                    forced_reason = "REQUEST_FAILED"
                provider_health = _provider_source_health(
                    provider,
                    source_kind="CALENDAR",
                    success_count=success_count,
                    failure_date_count=failure_date_count,
                    attempted_at=now,
                    forced_reason=forced_reason,
                )
                source_health.append(provider_health)
                phase2_cycle_health[id(provider)] = provider_health
                calendar_source_batches.append(
                    _calendar_source_batch(
                        provider_health,
                        batch_members,
                        window_start=now,
                        window_end=declared_window_end,
                    )
                )
                if forced_reason is not None:
                    phase2_forced_reasons[id(provider)] = forced_reason
                if self._cadence_enabled:
                    self._cadence.record(
                        cadence_source,
                        "CALENDAR",
                        now=now,
                        success=provider_health["status"] == "READY",
                        failure_code=provider_health.get("reason"),
                    )
                self._capture_source_diagnostic(source_health)
            official_configured = (
                self._official_calendar_provider is not None
                or self._injected_official_calendar_snapshot is not None
            )
            startup_official_snapshot_required = (
                official_configured
                and self._official_calendar_snapshot is None
                and self._official_calendar_cache is None
            )
            if not self._cadence_enabled:
                official_due, official_cadence_reason = True, "CADENCE_DUE"
            elif startup_official_snapshot_required:
                # Cadence is durable across process restarts, but the current
                # official snapshot is deliberately not.  A new coordinator
                # must make one bounded provider observation before reaction
                # logic may treat any schedule as current.  Once an attempt
                # creates a cache entry, normal success/failure cadence resumes.
                official_due, official_cadence_reason = (
                    True,
                    "STARTUP_OFFICIAL_SNAPSHOT_REQUIRED",
                )
            elif official_configured:
                official_due, official_cadence_reason = self._cadence.due(
                    "OFFICIAL_CALENDAR",
                    "OFFICIAL_CALENDAR",
                    now=now,
                )
            else:
                official_due, official_cadence_reason = (
                    False,
                    "SOURCE_UNCONFIGURED",
                )
            if official_due:
                official_refresh = self._refresh_official_calendar(now)
                if self._cadence_enabled and not official_refresh.cache_hit:
                    self._cadence.record(
                        "OFFICIAL_CALENDAR",
                        "OFFICIAL_CALENDAR",
                        now=now,
                        success=not official_refresh.failed,
                        failure_code=(
                            _official_source_reason(self._official_calendar_reasons)
                            if official_refresh.failed
                            else None
                        ),
                    )
            else:
                official_cache = self._official_calendar_cache
                official_refresh = _OfficialCalendarRefresh(
                    failed=(
                        official_configured
                        and (
                            official_cadence_reason == "CADENCE_STATE_CORRUPT"
                        or official_cache is None
                        or official_cache.failed
                        )
                    ),
                    observed_at=(
                        None if official_cache is None else official_cache.observed_at
                    ),
                    cache_hit=True,
                )
            calendar_failed = official_refresh.failed or calendar_failed
            if (
                self._official_calendar_provider is not None
                or self._injected_official_calendar_snapshot is not None
            ):
                official_snapshot = self._official_calendar_snapshot
                official_reason = (
                    _official_source_reason(self._official_calendar_reasons)
                    if official_refresh.failed
                    else None
                )
                source_health.append(
                    {
                        "source": "OFFICIAL_CALENDAR",
                        "source_kind": "OFFICIAL_CALENDAR",
                        "status": "DEGRADED" if official_refresh.failed else "READY",
                        "reason": official_reason,
                        "success_count": (
                            0
                            if official_snapshot is None
                            else len(official_snapshot.events)
                        ),
                        "failure_date_count": 0,
                        "asof": (
                            official_refresh.observed_at or now
                        ).isoformat(),
                        "decision_authority": "SUPPORTING_ONLY",
                    }
                )
                self._capture_source_diagnostic(source_health)
            self._source_health = tuple(source_health)
            self._phase2_source_rows = _phase2_source_rows(
                (
                    *((provider, "NEWS") for provider in self._news_providers),
                    *((provider, "CALENDAR") for provider in self._calendar_providers),
                ),
                attempted_at=now,
                forced_reasons=phase2_forced_reasons,
                cycle_observations=phase2_cycle_observations,
                cycle_health=phase2_cycle_health,
                cadence_rows=(
                    self._cadence.projections(now=now)
                    if self._cadence_enabled
                    else ()
                ),
            )
            calendar_latency: float | None = round(
                (monotonic_time.perf_counter() - calendar_started) * 1000,
                2,
            )
            calendar_cached_only = (
                official_refresh.cache_hit and not self._calendar_providers
            )
            calendar_asof = (
                official_refresh.observed_at
                if calendar_cached_only and official_refresh.observed_at is not None
                else now
            )
            if calendar_cached_only:
                # A cache hit is not a new source observation and therefore
                # must not manufacture fresh latency or an ``asof`` timestamp.
                calendar_latency = None

            self._news_health = (
                _health_after_refresh(
                    configured=bool(self._news_providers),
                    failed=news_failed,
                    latency_ms=news_latency,
                    asof=now,
                    noun="providers",
                )
                if news_provider_attempted or not self._cadence_enabled
                else _coordinator_health_from_cadence(
                    self._cadence,
                    "NEWS",
                    now=now,
                    configured=bool(self._news_providers),
                    noun="providers",
                )
            )
            self._calendar_health = (
                _health_after_refresh(
                    configured=self._calendar_is_configured(),
                    failed=calendar_failed,
                    latency_ms=calendar_latency,
                    asof=calendar_asof,
                    noun="calendar providers",
                    cached=calendar_cached_only,
                )
                if calendar_provider_attempted
                or official_due
                or not self._cadence_enabled
                else _coordinator_health_from_cadence(
                    self._cadence,
                    "CALENDAR",
                    now=now,
                    configured=self._calendar_is_configured(),
                    noun="calendar providers",
                    include_official=True,
                )
            )
            # Provider-owned first-seen/ingested timestamps may be observed
            # after the cycle's initial clock sample.  Re-sample only for the
            # point-in-time read-model cutoff so those valid rows are visible
            # on the first refresh; evidence timestamps and future-time
            # validation remain unchanged.
            rebuild_asof = max(now, _aware(self._clock()))
            calendar_envelope = (
                _calendar_generation_envelope(
                    calendar_source_batches,
                    observed_at=rebuild_asof,
                    window_start=now,
                    window_end=declared_window_end,
                )
                if calendar_generation_complete
                else self._calendar_envelope
            )
            self._begin_publication_stage("READ_MODEL_REBUILD")
            self._rebuild_read_model(
                asof=rebuild_asof,
                calendar_envelope=calendar_envelope,
            )
            statuses = {self._news_health.status, self._calendar_health.status}
            configured = bool(self._news_providers) or self._calendar_is_configured()
            overall = (
                "DEGRADED"
                if self._analysis_backfill_status == "DEGRADED"
                else "PENDING"
                if self._analysis_backfill_status == "PENDING"
                else "UNCONFIGURED"
                if not configured
                else "DEGRADED"
                if "DEGRADED" in statuses
                else "READY"
            )
            return {
                "status": overall,
                "asof": rebuild_asof.isoformat(),
                "news_count": int(self._news_payload.get("count", 0)),
                "calendar_count": int(self._calendar_payload.get("count", 0)),
                "pre_market_preselection_count": int(
                    self._news_payload.get("pre_market_preselection_count", 0)
                ),
                "open_market_repriced_count": int(
                    self._news_payload.get("open_market_repriced_count", 0)
                ),
                "option_action_pool_count": int(
                    self._news_payload.get("option_action_pool_count", 0)
                ),
                "approval_eligible": False,
            }

    def refresh_reaction_once(self) -> None:
        """Run one hard five-second local observer tick with zero provider I/O."""

        self._ensure_open()
        self._refresh_reaction_provider(_aware(self._clock()))
        with self._state_lock:
            self._reaction_publication_generation += 1
        self._reaction_publication_requested.set()

    def refresh_reaction_publication_once(self) -> bool:
        """Publish cached reaction evidence without waiting for the main refresh."""

        self._ensure_open()
        with self._state_lock:
            generation = self._reaction_publication_generation
        publication_asof = _aware(self._clock())
        if not self._refresh_lock.acquire(blocking=False):
            return False
        try:
            self._rebuild_read_model(
                asof=publication_asof,
                analysis_budget=0,
            )
        finally:
            self._refresh_lock.release()
        with self._state_lock:
            self._reaction_published_generation = max(
                self._reaction_published_generation,
                generation,
            )
            caught_up = (
                self._reaction_published_generation
                >= self._reaction_publication_generation
            )
        if caught_up:
            self._reaction_publication_requested.clear()
            with self._state_lock:
                if (
                    self._reaction_published_generation
                    < self._reaction_publication_generation
                ):
                    self._reaction_publication_requested.set()
        return True

    def refresh_reaction_schedule_once(self) -> None:
        """Refresh the longer schedule on its own provider instance and cadence."""

        self._ensure_open()
        now = _aware(self._clock())
        provider = self._reaction_provider
        schedule_reader = getattr(
            self._reaction_schedule_provider,
            "reaction_schedule",
            None,
        )
        refresh_with_health = getattr(provider, "refresh_schedule_with_health", None)
        record_failure = getattr(provider, "record_worker_failure", None)
        if not callable(schedule_reader) or not callable(refresh_with_health):
            if callable(record_failure):
                record_failure(
                    "SCHEDULE",
                    now=now,
                    reason="REACTION_ATOMIC_SCHEDULE_REFRESH_UNAVAILABLE",
                )
            return
        try:
            schedule = schedule_reader(now=now)
            schedule_status = str(getattr(schedule, "status", "UNAVAILABLE"))
            schedule_reason = (
                None
                if not getattr(schedule, "reasons", ())
                else "REACTION_SCHEDULE_SOURCE_DEGRADED"
            )
            schedule_hash = getattr(schedule, "schedule_hash", None)
            refresh_with_health(
                schedule,
                now=now,
                status=schedule_status,
                reason=schedule_reason,
                schedule_hash=schedule_hash,
            )
        except Exception:
            if callable(record_failure):
                record_failure(
                    "SCHEDULE",
                    now=now,
                    reason="REACTION_SCHEDULE_REFRESH_UNAVAILABLE",
                )

    def refresh_reaction_capture_once(self) -> None:
        """Run bounded Jin10/document capture independently from publication."""

        self._ensure_open()
        provider = self._reaction_provider
        refresh_capture = getattr(provider, "refresh_capture", None)
        if not callable(refresh_capture):
            return
        now = _aware(self._clock())
        try:
            refresh_capture(now=now)
        except Exception:
            record_failure = getattr(provider, "record_worker_failure", None)
            if callable(record_failure):
                record_failure(
                    "CAPTURE",
                    now=now,
                    reason="REACTION_CAPTURE_WORKER_UNAVAILABLE",
                )

    def refresh_research(
        self,
        calendar: UsOptionsCalendarSnapshot,
        scheduled_for: datetime,
        checked_at: datetime,
    ) -> dict[str, object]:
        """Run the source-neutral weekly tick from cached provider inputs.

        ``ScanSchedulerLoop`` retains this callback for operational timing, but
        its broker calendar cannot write weekly evidence. Both scheduler
        triggers therefore converge on the same bounded offline calendar and
        cached point-in-time inputs. Provider acquisition remains exclusively
        owned by the independent provider poller.
        """

        self._ensure_open()
        if not isinstance(calendar, UsOptionsCalendarSnapshot):
            raise TypeError("calendar must be a UsOptionsCalendarSnapshot")
        scheduled = _aware(scheduled_for)
        evaluated = _aware(checked_at)
        weekly_tick = self.refresh_weekly_observation(checked_at=evaluated)
        weekly = dict(weekly_tick["weekly_brief"])
        return {
            "status": str(weekly.get("status") or "NOT_RUN"),
            "scheduled_for": scheduled.isoformat(),
            "weekly_brief_scheduled_for": weekly_tick.get("scheduled_for"),
            "checked_at": evaluated.isoformat(),
            "weekly_brief": weekly,
            "weekly_brief_attempted": bool(
                weekly_tick.get("weekly_brief_attempted")
            ),
            "provider_refresh": {
                "status": "NOT_RUN",
                "reason": "INDEPENDENT_PROVIDER_POLLER_OWNS_REFRESH",
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
            },
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }

    def refresh_weekly_observation(
        self,
        *,
        checked_at: datetime | None = None,
    ) -> dict[str, object]:
        """Evaluate the offline 08:30 ET slot from cached evidence only.

        This path has no broker, scanner, provider-refresh, ranking, approval,
        instruction, or order authority. It intentionally does nothing outside
        the exact minute so restarting the process cannot replay a missed slot.
        """

        self._ensure_open()
        evaluated = _aware(self._clock() if checked_at is None else checked_at)
        evaluated_et = evaluated.astimezone(_EASTERN)
        calendar = self._weekly_schedule.snapshot(now=evaluated_et)
        base = {
            "scheduled_for": None,
            "checked_at": evaluated.isoformat(),
            "weekly_brief_attempted": False,
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }
        calendar_hash_valid = calendar.verify_hash()
        if not calendar.ready or not calendar_hash_valid:
            reasons = set(calendar.reason_codes)
            if not calendar_hash_valid:
                reasons.add("CALENDAR_HASH_INVALID")
            reason_codes = tuple(
                sorted(reasons or {"WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"})
            )
            previous = self.weekly_brief_runtime.read_model()
            self._clear_recovered_weekly_failure(
                week_start=calendar.week_start,
                include_terminal=False,
            )
            return {
                **base,
                "status": "NOT_RUN",
                "reason_codes": list(reason_codes),
                "weekly_brief": _weekly_not_run_projection(
                    previous,
                    reason_codes,
                ),
            }
        scheduled = datetime.combine(
            calendar.sessions[0],
            time(8, 30),
            tzinfo=_EASTERN,
        )
        base["scheduled_for"] = scheduled.isoformat()
        if evaluated_et < scheduled:
            previous = self.weekly_brief_runtime.read_model()
            self._clear_recovered_weekly_failure(
                week_start=calendar.week_start,
                include_terminal=False,
            )
            return {
                **base,
                "status": "NOT_RUN",
                "reason_codes": ["WEEKLY_BRIEF_SLOT_NOT_DUE"],
                "weekly_brief": _weekly_not_run_projection(
                    previous,
                    ("WEEKLY_BRIEF_SLOT_NOT_DUE",),
                ),
            }
        if evaluated_et >= scheduled + timedelta(minutes=1):
            previous = self.weekly_brief_runtime.read_model()
            self._clear_recovered_weekly_failure(
                week_start=calendar.week_start,
                include_terminal=False,
            )
            return {
                **base,
                "status": "NOT_RUN",
                "reason_codes": ["WEEKLY_BRIEF_SLOT_MISSED"],
                "weekly_brief": _weekly_not_run_projection(
                    previous,
                    ("WEEKLY_BRIEF_SLOT_MISSED",),
                ),
            }
        with self._state_lock:
            news_payload = _copy_json(self._news_payload)
            calendar_payload = _copy_json(self._calendar_payload)
        weekly = self.weekly_brief_runtime.evaluate_research_read_models(
            calendar=calendar,
            scheduled_for=scheduled,
            evaluated_at=evaluated,
            news_payload=news_payload,
            calendar_payload=calendar_payload,
        )
        self._clear_recovered_weekly_failure(
            week_start=calendar.week_start,
            include_terminal=True,
        )
        return {
            **base,
            "status": str(weekly.get("status") or "NOT_RUN"),
            "reason_codes": list(weekly.get("reason_codes") or ()),
            "weekly_brief": weekly,
            "weekly_brief_attempted": True,
        }

    def news_payload(self) -> dict[str, object]:
        self._ensure_open()
        refresh_lock_acquired = self._refresh_lock.acquire(blocking=False)
        if refresh_lock_acquired:
            try:
                now = _aware(self._clock())
                expired = [
                    symbol
                    for symbol, binding in self._ibkr_bindings.items()
                    if not _binding_is_current(binding, now)
                ]
                if expired:
                    for symbol in expired:
                        self._ibkr_bindings.pop(symbol, None)
                    self._rebuild_read_model(asof=now, analysis_budget=0)
                elif (
                    self._preselection_action_expires_at is not None
                    and now > self._preselection_action_expires_at
                ):
                    self._rebuild_read_model(asof=now, analysis_budget=0)
            finally:
                self._refresh_lock.release()
        evaluated_at = _aware(self._clock())
        monotonic_now = monotonic_time.perf_counter()
        with self._state_lock:
            payload = _copy_json(self._news_payload)
            diagnostic = self._publication_diagnostic
        projection = diagnostic.project(
            evaluated_at=evaluated_at,
            monotonic_now=monotonic_now,
        )
        action_expires_at = _optional_timestamp(projection.pop("action_expires_at"))
        payload.update(projection)
        payload.setdefault("option_approval_eligible", False)
        if (
            not refresh_lock_acquired
            or diagnostic.progress.status == "RUNNING"
            or evaluated_at < diagnostic.observed_at
            or (
                action_expires_at is not None
                and evaluated_at > action_expires_at
            )
        ):
            _fail_closed_public_news_actions(payload, quote_stale=True)
        return payload

    def calendar_payload(self) -> dict[str, object]:
        self._ensure_open()
        evaluated_at = _aware(self._clock())
        monotonic_now = monotonic_time.perf_counter()
        with self._state_lock:
            payload = _copy_json(self._calendar_payload)
            diagnostic = self._publication_diagnostic
        projection = diagnostic.project(
            evaluated_at=evaluated_at,
            monotonic_now=monotonic_now,
        )
        projection.pop("action_expires_at", None)
        payload.update(projection)
        return payload

    def decision_event_payload(self) -> dict[str, object]:
        """Return one read-only news/calendar projection for deterministic event gates."""

        self._ensure_open()
        # Both projections are published under this same lock in
        # ``_rebuild_read_model``.  Copy them together so a refresh cannot pair
        # one generation's source health with another generation's calendar.
        with self._state_lock:
            news = _copy_json(self._news_payload)
            calendar = _copy_json(self._calendar_payload)
        raw_calendar_rows = calendar.get("calendar", [])
        deterministic_calendar_rows = [
            _deterministic_decision_calendar_row(row)
            for row in raw_calendar_rows
            if isinstance(row, Mapping)
        ] if isinstance(raw_calendar_rows, Sequence) else []
        generation_payload = {
            "news_asof": news.get("asof"),
            "source_health": news.get("source_health", []),
            "calendar_asof": calendar.get("asof"),
            "calendar_snapshot_hash": calendar.get("snapshot_hash"),
            "calendar_window_start": calendar.get("window_start"),
            "calendar_window_end": calendar.get("window_end"),
            "calendar_envelope": calendar.get("calendar_envelope"),
            "calendar": deterministic_calendar_rows,
        }
        raw_news_rows = news.get("news", [])
        deterministic_news_rows = [
            _deterministic_decision_news_row(row)
            for row in raw_news_rows
            if isinstance(row, Mapping)
        ] if isinstance(raw_news_rows, Sequence) else []
        return {
            "news": deterministic_news_rows,
            "news_asof": news.get("asof"),
            "source_health": news.get("source_health", []),
            "calendar": deterministic_calendar_rows,
            "calendar_asof": calendar.get("asof"),
            "calendar_snapshot_hash": calendar.get("snapshot_hash"),
            "calendar_window_start": calendar.get("window_start"),
            "calendar_window_end": calendar.get("window_end"),
            "calendar_envelope": calendar.get("calendar_envelope"),
            "event_generation_hash": canonical_hash(generation_payload),
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }

    def decision_equity_news_payload(self) -> dict[str, object]:
        """Return symbol-bound news without the public 500-row display cap."""

        self._ensure_open()
        with self._state_lock:
            payload = _copy_json(self._equity_news_payload)
        raw_rows = payload.get("news", [])
        rows = (
            [
                _deterministic_decision_news_row(row)
                for row in raw_rows
                if isinstance(row, Mapping)
            ]
            if isinstance(raw_rows, Sequence)
            and not isinstance(raw_rows, (str, bytes, bytearray))
            else []
        )
        return {
            "news": rows,
            "news_asof": payload.get("asof"),
            "source_health": payload.get("source_health", []),
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }

    def advisory_payload(self) -> dict[str, object]:
        """Return the cached Phase 2 advisory without model or provider work."""

        self._ensure_open()
        with self._state_lock:
            return _copy_json(self._advisory_payload)

    def source_evidence_payload(self) -> dict[str, object]:
        """Return the cached six-source snapshot without provider work."""

        self._ensure_open()
        with self._state_lock:
            return _copy_json(self._source_evidence_payload)

    def weekly_brief_payload(self) -> dict[str, object]:
        """Return the latest persisted brief or an explicit NOT_RUN snapshot."""

        self._ensure_open()
        now = _aware(self._clock()).astimezone(_EASTERN)
        schedule = self._weekly_schedule.snapshot(now=now)
        schedule_hash_valid = schedule.verify_hash()
        if not schedule.ready or not schedule_hash_valid:
            reasons = set(schedule.reason_codes)
            if not schedule_hash_valid:
                reasons.add("CALENDAR_HASH_INVALID")
            return _weekly_not_run_projection(
                {},
                tuple(
                    sorted(
                        reasons
                        or {"WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"}
                    )
                ),
            )
        try:
            payload = _copy_json(self.weekly_brief_runtime.read_model())
        except Exception:
            with self._state_lock:
                scheduler_failure = dict(self._weekly_scheduler_failure or {})
            reason = (
                "WEEKLY_BRIEF_SCHEDULER_FAILED"
                if scheduler_failure.get("week_start")
                == schedule.week_start.isoformat()
                else "WEEKLY_BRIEF_STORE_UNAVAILABLE"
            )
            return _weekly_not_run_projection({}, (reason,))
        first_session = schedule.sessions[0]
        scheduled_for = datetime.combine(
            first_session,
            time(8, 30),
            tzinfo=_EASTERN,
        )
        slot = payload.get("slot")
        slot = slot if isinstance(slot, Mapping) else {}
        current_record = (
            payload.get("week_start") == schedule.week_start.isoformat()
            or slot.get("week_start") == schedule.week_start.isoformat()
        )
        with self._state_lock:
            scheduler_failure = dict(self._weekly_scheduler_failure or {})
        if (
            not current_record
            and scheduler_failure.get("week_start")
            == schedule.week_start.isoformat()
        ):
            return _weekly_not_run_projection(
                payload,
                ("WEEKLY_BRIEF_SCHEDULER_FAILED",),
            )
        if not current_record and now < scheduled_for:
            return _weekly_not_run_projection(
                payload,
                ("WEEKLY_BRIEF_SLOT_NOT_DUE",),
            )
        if (
            not current_record
            and now < scheduled_for + timedelta(minutes=1)
        ):
            return _weekly_not_run_projection(
                payload,
                ("WEEKLY_BRIEF_SLOT_PENDING",),
            )
        if not current_record and now >= scheduled_for + timedelta(minutes=1):
            return _weekly_not_run_projection(
                payload,
                ("WEEKLY_BRIEF_SLOT_MISSED",),
            )
        return payload

    def health(self) -> dict[str, object]:
        """Return a small dependency projection with no provider exception text."""

        self._ensure_open()
        with self._state_lock:
            statuses = {self._news_health.status, self._calendar_health.status}
            configured = bool(self._news_providers) or self._calendar_is_configured()
            status = (
                "DEGRADED"
                if self._analysis_backfill_status == "DEGRADED"
                else "PENDING"
                if self._analysis_backfill_status == "PENDING"
                else "UNCONFIGURED"
                if not configured
                else "DEGRADED"
                if "DEGRADED" in statuses
                else "READY"
                if statuses <= (_READY_STATES | _UNCONFIGURED_STATES)
                else "UNKNOWN"
            )
            return {
                "status": status,
                "stale": status != "READY",
                "asof": self._latest_asof(),
                "message": (
                    "read-only news research is current"
                    if status == "READY"
                    else "read-only news research is degraded or unconfigured"
                ),
            }

    def start(self) -> None:
        """Start independent provider and observation-only weekly pollers."""

        self._ensure_open()
        with self._state_lock:
            if self._closing:
                return
            self._stop.clear()
            if self._weekly_thread is None or not self._weekly_thread.is_alive():
                self._weekly_thread = threading.Thread(
                    target=self._poll_weekly_observation,
                    name="options-copilot-weekly-observation",
                    daemon=True,
                )
                self._weekly_thread.start()
            if (
                (self._news_providers or self._calendar_is_configured())
                and (self._thread is None or not self._thread.is_alive())
            ):
                self._thread = threading.Thread(
                    target=self._poll,
                    name="options-copilot-news-poller",
                    daemon=True,
                )
                self._thread.start()
            if (
                self._reaction_provider is not None
                and self._calendar_is_configured()
                and (
                    self._reaction_thread is None
                    or not self._reaction_thread.is_alive()
                )
            ):
                self._reaction_thread = threading.Thread(
                    target=self._poll_reaction,
                    name="options-copilot-reaction-observer",
                    daemon=True,
                )
                self._reaction_thread.start()
            if (
                self._reaction_provider is not None
                and self._ibkr_binding_provider is not None
                and (
                    self._reaction_quote_sampler_thread is None
                    or not self._reaction_quote_sampler_thread.is_alive()
                )
            ):
                self._reaction_quote_sampler_thread = threading.Thread(
                    target=self._poll_reaction_quote_sampler,
                    name="options-copilot-reaction-quote-sampler",
                    daemon=True,
                )
                self._reaction_quote_sampler_thread.start()
            if (
                self._reaction_provider is not None
                and (
                    self._reaction_publication_thread is None
                    or not self._reaction_publication_thread.is_alive()
                )
            ):
                self._reaction_publication_thread = threading.Thread(
                    target=self._poll_reaction_publication,
                    name="options-copilot-reaction-publication",
                    daemon=True,
                )
                self._reaction_publication_thread.start()
            if (
                self._reaction_provider is not None
                and (
                    self._reaction_capture_thread is None
                    or not self._reaction_capture_thread.is_alive()
                )
            ):
                self._reaction_capture_thread = threading.Thread(
                    target=self._poll_reaction_capture,
                    name="options-copilot-reaction-capture",
                    daemon=True,
                )
                self._reaction_capture_thread.start()
            if (
                self._reaction_provider is not None
                and self._reaction_schedule_provider is not None
                and (
                    self._reaction_schedule_thread is None
                    or not self._reaction_schedule_thread.is_alive()
                )
            ):
                self._reaction_schedule_thread = threading.Thread(
                    target=self._poll_reaction_schedule,
                    name="options-copilot-reaction-schedule",
                    daemon=True,
                )
                self._reaction_schedule_thread.start()

    def close(self) -> bool:
        with self._state_lock:
            if self._closed:
                return True
            self._closing = True
            self._stop.set()
            self._reaction_publication_requested.set()
            thread = self._thread
            reaction_thread = self._reaction_thread
            reaction_quote_sampler_thread = self._reaction_quote_sampler_thread
            reaction_publication_thread = self._reaction_publication_thread
            reaction_capture_thread = self._reaction_capture_thread
            reaction_schedule_thread = self._reaction_schedule_thread
            weekly_thread = self._weekly_thread
        for worker in (
            thread,
            reaction_thread,
            reaction_quote_sampler_thread,
            reaction_publication_thread,
            reaction_capture_thread,
            reaction_schedule_thread,
            weekly_thread,
        ):
            if worker is not None and worker is not threading.current_thread():
                worker.join(timeout=5.0)
        with self._state_lock:
            if any(
                worker is not None and worker.is_alive()
                for worker in (
                    thread,
                    reaction_thread,
                    reaction_quote_sampler_thread,
                    reaction_publication_thread,
                    reaction_capture_thread,
                    reaction_schedule_thread,
                    weekly_thread,
                )
            ):
                return False
            self._thread = None
            self._reaction_thread = None
            self._reaction_quote_sampler_thread = None
            self._reaction_publication_thread = None
            self._reaction_capture_thread = None
            self._reaction_schedule_thread = None
            self._weekly_thread = None
            self._closed = True
            self._closing = False
        self.analysis_store.close()
        self.weekly_brief_store.close()
        self.evidence_store.close()
        return True

    def _poll(self) -> None:
        while not self._stop.is_set():
            cycle_started = monotonic_time.perf_counter()
            try:
                # Restore append-only local authority before any network-bound
                # provider can delay the first useful read model after restart.
                # This lane performs no provider, shadow-model, or Broker call.
                self._finish_local_analysis_restore()
            except Exception:
                # Integrity/backfill failures remain fail-closed without
                # suppressing the provider observation attempt below.
                pass
            try:
                self.refresh_once()
            except Exception:
                # The poller is an optional read-only dependency.  A storage
                # or lifecycle error must never destabilize the trading GUI.
                pass
            try:
                # Provider acquisition owns the cadence. A second local pass
                # absorbs rows appended by this cycle without waiting for the
                # next 60-120 second provider interval.
                self._finish_local_analysis_restore()
            except Exception:
                # Integrity/backfill failures remain fail-closed without
                # suppressing the next provider observation.
                pass
            wait_seconds = _remaining_poll_wait_seconds(
                cycle_started,
                self._poll_interval_seconds,
            )
            if self._stop.wait(wait_seconds):
                break

    def _poll_reaction(self) -> None:
        """Run the hard local observer lane independently from publication."""

        while not self._stop.is_set():
            try:
                self.refresh_reaction_once()
            except Exception:
                if self._stop.is_set() or self._closing:
                    break
                self._record_reaction_poller_failure(
                    "OBSERVER",
                    "REACTION_OBSERVER_POLLER_UNAVAILABLE",
                )
            if self._stop.wait(_REACTION_OBSERVATION_POLL_SECONDS):
                break

    def _poll_reaction_quote_sampler(self) -> None:
        """Refresh one atomic read-only cache outside the hard observer lane."""

        symbol_reader = getattr(
            self._reaction_provider,
            "pending_reaction_sample_symbols",
            None,
        )
        refresh = getattr(
            self._ibkr_binding_provider,
            "reaction_underlying_quotes",
            None,
        )
        observe = getattr(self._reaction_provider, "observe_local", None)
        if not callable(symbol_reader) or not callable(refresh):
            return
        next_attempt_at: datetime | None = None
        while not self._stop.is_set():
            try:
                if self._reaction_quote_sampler_lock.acquire(blocking=False):
                    try:
                        sampled_at = _aware(self._clock())
                        symbols = tuple(symbol_reader(now=sampled_at))
                        if not symbols:
                            next_attempt_at = None
                        elif (
                            next_attempt_at is None
                            or sampled_at >= next_attempt_at
                        ):
                            # A reaction batch contains six macro symbols.  A
                            # one-second failure loop can consume the complete
                            # signed 30-request rolling-minute snapshot budget
                            # during the five-second baseline window and starve
                            # the scheduler-owned 08:30 ET research refresh.
                            # Arm the retry fence before acquisition so every
                            # transport or pacing failure remains bounded too.
                            next_attempt_at = sampled_at + timedelta(
                                seconds=_REACTION_QUOTE_SAMPLE_RETRY_SECONDS
                            )
                            bindings = tuple(refresh(symbols))
                            if callable(observe):
                                post_refresh_at = _aware(self._clock())
                                confirmation_times = tuple(
                                    _aware(observed_at)
                                    for binding in bindings
                                    if (
                                        observed_at := getattr(
                                            binding,
                                            "observed_at",
                                            None,
                                        )
                                    )
                                    is not None
                                )
                                if not confirmation_times or max(
                                    confirmation_times
                                ) <= post_refresh_at:
                                    observe(now=post_refresh_at)
                    finally:
                        self._reaction_quote_sampler_lock.release()
            except Exception:
                if self._stop.is_set() or self._closing:
                    break
                self._record_reaction_poller_failure(
                    "OBSERVER",
                    "REACTION_QUOTE_SAMPLER_UNAVAILABLE",
                )
            if self._stop.wait(_REACTION_QUOTE_SAMPLE_POLL_SECONDS):
                break

    def _poll_reaction_publication(self) -> None:
        """Retry cached publication without ever blocking the observer lane."""

        while not self._stop.is_set():
            if not self._reaction_publication_requested.wait(timeout=0.25):
                continue
            if self._stop.is_set():
                break
            try:
                published = self.refresh_reaction_publication_once()
            except Exception:
                published = False
                if self._stop.is_set() or self._closing:
                    break
                self._record_reaction_poller_failure(
                    "OBSERVER",
                    "REACTION_PUBLICATION_WORKER_UNAVAILABLE",
                )
            if not published and self._stop.wait(0.05):
                break

    def _poll_reaction_capture(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh_reaction_capture_once()
            except Exception:
                if self._stop.is_set() or self._closing:
                    break
                self._record_reaction_poller_failure(
                    "CAPTURE",
                    "REACTION_CAPTURE_POLLER_UNAVAILABLE",
                )
            if self._stop.wait(_REACTION_CAPTURE_POLL_SECONDS):
                break

    def _poll_reaction_schedule(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh_reaction_schedule_once()
            except Exception:
                if self._stop.is_set() or self._closing:
                    break
                self._record_reaction_poller_failure(
                    "SCHEDULE",
                    "REACTION_SCHEDULE_POLLER_UNAVAILABLE",
                )
            if self._stop.wait(_REACTION_SCHEDULE_POLL_SECONDS):
                break

    def _record_reaction_poller_failure(self, lane: str, reason: str) -> None:
        provider = self._reaction_provider
        record_failure = getattr(provider, "record_worker_failure", None)
        if not callable(record_failure):
            return
        try:
            record_failure(lane, now=_aware(self._clock()), reason=reason)
        except Exception:
            # The provider owns a second in-memory health path. A completely
            # invalid adapter cannot safely expose exception text here.
            return

    def _finish_local_analysis_restore(self) -> None:
        with self._state_lock:
            if self._analysis_backfill_status != "PENDING":
                return
        with self._publication_cycle("LOCAL_ANALYSIS_RESTORE"):
            self._run_local_analysis_restore()

    def _run_local_analysis_restore(self) -> None:
        """Finish bounded restart verification and reanalysis locally.

        Provider polling is intentionally slow and network-bounded, while the
        deferred analysis-ledger verification and deterministic classifier
        backfill are entirely local. Advancing bounded local batches after the
        first successful cycle avoids showing a partial news surface for many
        provider intervals after a classifier contract upgrade. The projection
        remains fail-closed until the complete chain is verified.
        """

        for _batch in range(_NEWS_LOCAL_RESTORE_BATCH_LIMIT):
            if self._stop.is_set():
                return
            with self._state_lock:
                integrity_complete = (
                    self._analysis_integrity.get("complete") is True
                )
                backfill_pending = self._analysis_backfill_status == "PENDING"
                before = (
                    self._analysis_backfill_status,
                    self._analysis_integrity.get("complete"),
                    self._analysis_integrity.get("verified_rows"),
                    self._analysis_integrity.get("remaining_rows"),
                    len(self._analysis_pending),
                    len(self._analysis_model_pending),
                )
            if not backfill_pending:
                return
            if not integrity_complete:
                try:
                    progress = self.analysis_store.verify_integrity_batch(
                        _NEWS_ANALYSIS_INTEGRITY_BATCH_SIZE
                    )
                except Exception:
                    with self._state_lock:
                        self._analysis_integrity = {
                            "status": "DEGRADED",
                            "batch_rows": 0,
                            "verified_rows": 0,
                            "remaining_rows": self.analysis_store.count,
                            "complete": False,
                        }
                        self._analysis_backfill_status = "DEGRADED"
                    return
                with self._state_lock:
                    self._analysis_integrity = {
                        "status": "VERIFIED" if progress.complete else "PENDING",
                        "batch_rows": progress.batch_rows,
                        "verified_rows": progress.verified_rows,
                        "remaining_rows": progress.remaining_rows,
                        "complete": progress.complete,
                    }
                    after = (
                        self._analysis_backfill_status,
                        self._analysis_integrity.get("complete"),
                        self._analysis_integrity.get("verified_rows"),
                        self._analysis_integrity.get("remaining_rows"),
                        len(self._analysis_pending),
                        len(self._analysis_model_pending),
                    )
                if after == before:
                    return
                if not progress.complete:
                    continue

            # Integrity is now complete. Deterministic classification is local
            # and carries no provider, shadow-model, or Broker authority, so a
            # restart may restore the entire bounded evidence window in one
            # projection instead of remaining degraded for hundreds of 60-120
            # second provider intervals.
            with self._refresh_lock:
                self._rebuild_read_model(
                    asof=_aware(self._clock()),
                    analysis_budget=_NEWS_LOCAL_RESTORE_ANALYSIS_BATCH_SIZE,
                    allow_shadow_model_calls=False,
                )
            with self._state_lock:
                after = (
                    self._analysis_backfill_status,
                    self._analysis_integrity.get("complete"),
                    self._analysis_integrity.get("verified_rows"),
                    self._analysis_integrity.get("remaining_rows"),
                    len(self._analysis_pending),
                    len(self._analysis_model_pending),
                )
            if after == before:
                return
            # Persisted lookup work is intentionally split into 500-row
            # slices. Continue the bounded local loop so the complete 5,000-row
            # read-model window is restored before provider acquisition.

    def _poll_weekly_observation(self) -> None:
        while not self._stop.is_set():
            checked_at: datetime | None = None
            try:
                checked_at = _aware(self._clock())
                self.refresh_weekly_observation(checked_at=checked_at)
            except Exception:
                # The cached weekly lane is display-only. Storage or lifecycle
                # faults stay isolated from broker authority but remain visible
                # because an exact-slot failure cannot be replayed safely.
                self._record_weekly_scheduler_failure(checked_at=checked_at)
            if self._stop.wait(_WEEKLY_OBSERVATION_POLL_SECONDS):
                break

    def _record_weekly_scheduler_failure(
        self,
        *,
        checked_at: datetime | None = None,
    ) -> None:
        try:
            checked = _aware(
                self._clock() if checked_at is None else checked_at
            ).astimezone(_EASTERN)
            week_start = checked.date() - timedelta(days=checked.date().weekday())
            schedule = self._weekly_schedule.snapshot(now=checked)
            terminal = False
            if schedule.ready and schedule.sessions:
                scheduled = datetime.combine(
                    schedule.sessions[0],
                    time(8, 30),
                    tzinfo=_EASTERN,
                )
                terminal = scheduled <= checked < scheduled + timedelta(minutes=1)
            failure = {
                "week_start": week_start.isoformat(),
                "checked_at": checked.isoformat(),
                "terminal": terminal,
                "reason_codes": ("WEEKLY_BRIEF_SCHEDULER_FAILED",),
            }
        except Exception:
            failure = {
                "week_start": None,
                "checked_at": None,
                "terminal": False,
                "reason_codes": ("WEEKLY_BRIEF_SCHEDULER_FAILED",),
            }
        with self._state_lock:
            previous = self._weekly_scheduler_failure or {}
            same_week = previous.get("week_start") == failure.get("week_start")
            if same_week and previous.get("terminal") is True:
                failure["terminal"] = True
            previous_count = (
                int(previous.get("failure_count", 0))
                if same_week
                else 0
            )
            failure["failure_count"] = previous_count + 1
            self._weekly_scheduler_failure = failure

    def _clear_recovered_weekly_failure(
        self,
        *,
        week_start: date,
        include_terminal: bool,
    ) -> None:
        with self._state_lock:
            failure = self._weekly_scheduler_failure
            if failure is None:
                return
            if failure.get("week_start") != week_start.isoformat():
                return
            if failure.get("terminal") is True and not include_terminal:
                return
            self._weekly_scheduler_failure = None

    def _refresh_ibkr_bindings(self, now: datetime) -> bool:
        """Load one strict, fresh binding per symbol or leave the pool empty."""

        self._ibkr_bindings = {}
        provider = self._ibkr_binding_provider
        if provider is None or not self._core_symbols:
            return False
        try:
            rows = tuple(provider.bindings(self._core_symbols))
        except Exception:
            return True
        if not _provider_is_ready(provider):
            # A degraded batch cannot contribute selectively "good" rows to
            # the action pool.  Treat the whole provider observation as
            # unavailable so Top 3 fails closed to zero.
            return True
        failed = False
        checked: dict[str, IbkrNewsBinding] = {}
        duplicates: set[str] = set()
        for item in rows:
            if not isinstance(item, IbkrNewsBinding):
                failed = True
                continue
            if item.symbol not in self._core_symbols or not _binding_is_current(item, now):
                failed = True
                continue
            if item.symbol in checked:
                duplicates.add(item.symbol)
                failed = True
                continue
            checked[item.symbol] = item
        for symbol in duplicates:
            checked.pop(symbol, None)
        self._ibkr_bindings = checked
        return failed

    def _refresh_preselections(self) -> bool:
        """Replace the ephemeral research batch or fail closed to an empty batch."""

        self._preselections = ()
        self._preselection_lineage = {}
        self._preselection_atomic_binding_available = False
        provider = self._preselection_provider
        if provider is None:
            self._preselection_coverage = _preselection_coverage(None)
            return False
        snapshot_reader = getattr(provider, "read_snapshot", None)
        raw_lineage: Mapping[object, object] | None = None
        try:
            if callable(snapshot_reader):
                snapshot = snapshot_reader()
                rows = tuple(getattr(snapshot, "preselections"))
                raw_lineage = getattr(snapshot, "lineage")
                raw_coverage = getattr(snapshot, "coverage")
                self._preselection_coverage = _normalize_preselection_coverage(
                    raw_coverage
                )
            else:
                rows = tuple(provider.preselections())
                self._preselection_coverage = _preselection_coverage(provider)
        except Exception:
            self._preselection_coverage = _failed_preselection_coverage(
                "PRESELECTION_LEDGER_UNREADABLE"
                if callable(snapshot_reader)
                else "PRESELECTION_PROVIDER_FAILED"
            )
            return True
        if not _provider_is_ready(provider):
            self._preselection_coverage = _failed_preselection_coverage(
                "PRESELECTION_LEDGER_UNREADABLE"
                if callable(snapshot_reader)
                else "PRESELECTION_PROVIDER_NOT_READY"
            )
            return True
        checked: dict[tuple[str, str], ConditionalOptionPreselection] = {}
        duplicates: set[tuple[str, str]] = set()
        failed = False
        for item in rows:
            if not isinstance(item, ConditionalOptionPreselection):
                failed = True
                continue
            key = (item.preselection_id, item.phase.value)
            if key in checked:
                duplicates.add(key)
                failed = True
                continue
            checked[key] = item
        for key in duplicates:
            checked.pop(key, None)
        if callable(snapshot_reader):
            normalized_lineage = _validated_preselection_lineage(
                raw_lineage,
                expected_keys=set(checked),
            )
            if normalized_lineage is None or failed:
                self._preselection_coverage = _failed_preselection_coverage(
                    "PRESELECTION_LINEAGE_BINDING_INVALID"
                )
                return True
            if _preselection_source_lineage_is_missing(normalized_lineage):
                self._preselection_lineage = normalized_lineage
                self._preselections = tuple(checked.values())
                self._preselection_coverage = _source_lineage_missing_coverage(
                    self._preselection_coverage
                )
                return True
            if not _preselection_atomic_binding_is_valid(
                tuple(checked.values()),
                normalized_lineage,
                self._preselection_coverage,
            ):
                self._preselection_coverage = _failed_preselection_coverage(
                    "PRESELECTION_ATOMIC_BINDING_INVALID"
                )
                return True
            self._preselection_lineage = normalized_lineage
            self._preselection_atomic_binding_available = True
        else:
            self._preselection_coverage = _source_lineage_missing_coverage(
                self._preselection_coverage
            )
        self._preselections = tuple(checked.values())
        return failed

    def _calendar_is_configured(self) -> bool:
        return bool(self._calendar_providers) or (
            self._official_calendar_provider is not None
            or self._injected_official_calendar_snapshot is not None
        )

    def _refresh_official_calendar(self, now: datetime) -> _OfficialCalendarRefresh:
        """Consume one typed official snapshot without inferring missing facts.

        A degraded snapshot may still contain individually valid official rows;
        those remain visible as supporting research while the aggregate calendar
        decision fails closed to ``NO_TRADE``.  Untyped, stale, source-orphaned,
        or out-of-window rows are never projected.  A successful observation is
        reused for fifteen minutes; degraded or failed observations are retried
        no sooner than five minutes.  Cache reads preserve the original source
        and attempt timestamps.
        """

        self._official_calendar_snapshot = None
        self._official_calendar_reasons = ()
        provider = self._official_calendar_provider
        injected = self._injected_official_calendar_snapshot
        if provider is None and injected is None:
            return _OfficialCalendarRefresh(False, None, False)

        cached = self._official_calendar_cache
        if (
            cached is not None
            and cached.observed_at <= now < cached.refresh_not_before
        ):
            self._official_calendar_snapshot = cached.snapshot
            self._official_calendar_reasons = cached.reasons
            return _OfficialCalendarRefresh(
                cached.failed,
                cached.observed_at,
                True,
            )

        try:
            snapshot = injected if injected is not None else provider.future_two_weeks(now=now)  # type: ignore[union-attr]
        except Exception:
            return self._cache_official_failure(
                now,
                "OFFICIAL_CALENDAR_PROVIDER_UNAVAILABLE",
            )
        if not isinstance(snapshot, OfficialCalendarSnapshot):
            return self._cache_official_failure(
                now,
                "OFFICIAL_CALENDAR_SNAPSHOT_INVALID",
            )

        reasons = list(snapshot.reasons)
        fatal = False
        if (
            snapshot.window_start != now
            or snapshot.observed_at != now
            or snapshot.window_end != now + timedelta(days=14)
        ):
            reasons.append("OFFICIAL_CALENDAR_SNAPSHOT_STALE_OR_MISALIGNED")
            fatal = True
        source_keys = {(item.source, item.source_url) for item in snapshot.sources}
        if snapshot.status == "READY" and not source_keys:
            reasons.append("OFFICIAL_CALENDAR_SOURCE_HEALTH_MISSING")
            fatal = True
        if (
            provider is not None
            and snapshot.status == "READY"
            and not _provider_is_ready(provider)
        ):
            reasons.append("OFFICIAL_CALENDAR_PROVIDER_DEGRADED")

        if not fatal:
            for event in snapshot.events:
                if (
                    not _official_event_in_snapshot(event, snapshot)
                    or (event.source, event.source_url) not in source_keys
                ):
                    reasons.append("OFFICIAL_CALENDAR_EVENT_INCOMPLETE")
                    continue
                try:
                    self._append_official_calendar(event)
                except Exception:
                    reasons.append("OFFICIAL_CALENDAR_EVENT_REJECTED")

        checked_reasons = tuple(dict.fromkeys(reasons))
        failed = snapshot.status != "READY" or bool(checked_reasons)
        if not fatal:
            self._official_calendar_last_valid_snapshot = snapshot
            projected = snapshot
        else:
            projected = self._official_calendar_last_valid_snapshot
            if projected is not None:
                checked_reasons = tuple(
                    dict.fromkeys((*projected.reasons, *checked_reasons))
                )

        entry = _OfficialCalendarCacheEntry(
            snapshot=projected,
            reasons=checked_reasons,
            failed=failed,
            observed_at=now,
            refresh_not_before=now
            + (
                _OFFICIAL_CALENDAR_FAILURE_RETRY
                if failed
                else _OFFICIAL_CALENDAR_SUCCESS_TTL
            ),
        )
        self._official_calendar_cache = entry
        self._official_calendar_snapshot = entry.snapshot
        self._official_calendar_reasons = entry.reasons
        restore_identities = getattr(
            self._reaction_provider,
            "restore_official_identities",
            None,
        )
        if entry.snapshot is not None and callable(restore_identities):
            try:
                restore_identities(entry.snapshot.events)
            except Exception:
                # Scope restore is local and optional.  Leaving it unknown makes
                # the read model treat current official rows as eligible and
                # therefore fail closed until the reaction cadence can retry.
                pass
        return _OfficialCalendarRefresh(entry.failed, entry.observed_at, False)

    def _refresh_reaction_provider(self, now: datetime) -> None:
        """Run only local baseline/quote observation on the hard five-second lane."""

        provider = self._reaction_provider
        observe_local = getattr(provider, "observe_local", None)
        if provider is None or not callable(observe_local):
            return
        try:
            observe_local(now=now)
        except Exception:
            record_failure = getattr(provider, "record_worker_failure", None)
            if callable(record_failure):
                record_failure(
                    "OBSERVER",
                    now=now,
                    reason="REACTION_OBSERVER_WORKER_UNAVAILABLE",
                )

    def _cache_official_failure(
        self,
        now: datetime,
        reason: str,
    ) -> _OfficialCalendarRefresh:
        snapshot = self._official_calendar_last_valid_snapshot
        reasons = tuple(
            dict.fromkeys(
                (
                    *(snapshot.reasons if snapshot is not None else ()),
                    reason,
                )
            )
        )
        entry = _OfficialCalendarCacheEntry(
            snapshot=snapshot,
            reasons=reasons,
            failed=True,
            observed_at=now,
            refresh_not_before=now + _OFFICIAL_CALENDAR_FAILURE_RETRY,
        )
        self._official_calendar_cache = entry
        self._official_calendar_snapshot = entry.snapshot
        self._official_calendar_reasons = entry.reasons
        return _OfficialCalendarRefresh(True, now, False)

    def _append_news(self, event: NewsEvent) -> None:
        identity = self._news_persistence_identity(event)
        payload: dict[str, object] = {
            "event_id": event.event_id,
            "provider_story_id": event.provider_story_id,
            "symbol": event.symbol,
            "source": event.source,
            "headline": event.headline,
            "summary": event.summary,
            "url": event.url,
            "sentiment_score": event.sentiment_score,
            "source_rank": event.source_rank,
            "provenance": list(event.provenance),
            "provider_status": event.status,
            "source_content_hash": event.content_hash,
            "provider_adapter": event.provider_adapter,
            "symbol_binding_status": event.symbol_binding_status,
            "symbol_binding_proof": (
                None
                if event.symbol_binding_proof is None
                else event.symbol_binding_proof.as_dict()
            ),
        }
        record = EvidenceRecord(
            identity=identity,
            kind="NEWS",
            symbol=None if event.symbol is None else event.symbol.upper(),
            provider=_identifier("provider", event.source),
            source_id=_identifier("source", str(event.source_id or event.event_id)),
            published_at=event.published_at,
            first_seen_at=event.first_seen_at,
            ingested_at=event.ingested_at,
            observed_at=event.observed_at or event.ingested_at,
            payload=payload,
            status="CONFLICTED" if event.status == "CONFLICTED" else "ACTIVE",
        )
        if event.source == "SEC" and event.status == "CONFLICTED":
            # A cached source row can be implicated by a later conflicting
            # observation. Do not backdate this newly discovered conflict.
            discovered_at = max(record.observed_at, _aware(self._clock()))
            record = replace(
                record, first_seen_at=discovered_at,
                ingested_at=discovered_at, observed_at=discovered_at,
            )
        self._append_new_provider_version(record)

    def _news_persistence_identity(self, event: NewsEvent) -> str:
        legacy_identity = _news_identity(event)
        filer_identity = sec_filer_group_identity(
            source=event.source, source_id=str(event.source_id),
            event_id=event.event_id, lineage_id=event.lineage_id,
            evidence_ids=event.evidence_ids, url=event.url,
            provider_story_id=event.provider_story_id,
        )
        if filer_identity is None:
            return legacy_identity
        legacy_event_id = _identifier("sec-current", str(event.source_id))
        for stored in self.evidence_store.query(identities=(legacy_identity,), limit=5000):
            prior = stored.record
            prior_url = prior.payload.get("url")
            if (
                prior.provider != _identifier("provider", "SEC")
                or prior.source_id != _identifier("source", str(event.source_id))
                or prior.payload.get("event_id") != legacy_event_id
                or not isinstance(prior_url, str)
            ):
                continue
            # Only the exact old accession-only format may alias a new filer.
            # Validate its URL with the current accession/CIK-bound markers;
            # sharing a headline, ticker or accession alone is insufficient.
            prior_filer = sec_filer_group_identity(
                source=str(prior.payload.get("source")),
                source_id=str(event.source_id), event_id=filer_identity,
                lineage_id=filer_identity, evidence_ids=(filer_identity,),
                url=prior_url,
                provider_story_id=prior.payload.get("provider_story_id"),
            )
            if prior_filer == filer_identity:
                # Changed metadata stays in its original conflict partition;
                # exact semantic replay is still handled by the append seam.
                return legacy_identity
        return _identifier("news-sec-filer", f"{legacy_identity}\x1f{filer_identity}")

    def _append_earnings(self, event: EarningsEvent) -> StoredEvidence:
        identity = _calendar_identity(event.event_id)
        payload: dict[str, object] = {
            "event_id": event.event_id,
            "source_id": event.source_id,
            "symbol": event.symbol.upper(),
            "source": event.source,
            "report_date": event.report_date.isoformat(),
            "hour": event.hour,
            "eps_estimate": event.eps_estimate,
            "revenue_estimate": event.revenue_estimate,
            "report_session": getattr(event, "report_session", None),
            "is_estimated": bool(getattr(event, "is_estimated", False)),
            "source_url": str(getattr(event, "source_url", "") or ""),
            "provenance": list(event.provenance),
            "provider_status": event.status,
            "source_content_hash": event.content_hash,
            "published_at": event.published_at.isoformat(),
            "first_seen_at": event.first_seen_at.isoformat(),
            "ingested_at": event.ingested_at.isoformat(),
            "observed_at": event.observed_at.isoformat(),
            "decision_authority": "SUPPORTING_ONLY",
        }
        record = EvidenceRecord(
            identity=identity,
            kind="CALENDAR",
            symbol=event.symbol.upper(),
            provider=_identifier("provider", event.source),
            source_id=_identifier("source", str(event.source_id or event.event_id)),
            published_at=event.published_at or event.first_seen_at,
            first_seen_at=event.first_seen_at,
            ingested_at=event.ingested_at,
            observed_at=event.observed_at or event.ingested_at,
            payload=payload,
            status="CONFLICTED" if event.status == "CONFLICTED" else "ACTIVE",
        )
        return self._append_new_provider_version(record)

    def _append_official_calendar(self, event: OfficialCalendarEvent) -> None:
        payload = event.as_dict()
        payload.update(
            {
                "calendar_origin": "OFFICIAL",
                "provider_status": event.status,
                "source_content_hash": event.content_hash,
            }
        )
        record = EvidenceRecord(
            identity=_calendar_identity(event.event_id),
            kind="CALENDAR",
            symbol=event.symbols[0] if len(event.symbols) == 1 else None,
            provider=_identifier("provider", event.source),
            source_id=_identifier("source", event.source_id),
            published_at=event.published_at or event.first_seen_at,
            first_seen_at=event.first_seen_at,
            ingested_at=event.ingested_at,
            observed_at=event.observed_at,
            payload=payload,
            status="CONFLICTED" if event.status == "CONFLICTED" else "ACTIVE",
        )
        self._append_new_provider_version(record)

    def _append_new_provider_version(self, record: EvidenceRecord) -> StoredEvidence:
        existing = self.evidence_store.query(identities=(record.identity,), limit=5000)
        semantic_hash = record.payload.get("source_content_hash")
        for stored in existing:
            if (
                stored.record.provider == record.provider
                and stored.record.source_id == record.source_id
                and stored.record.payload.get("source_content_hash") == semantic_hash
                and (
                    record.payload.get("calendar_origin") != "OFFICIAL"
                    or stored.record.payload.get("record_hash")
                    == record.payload.get("record_hash")
                )
                and stored.record.payload.get("provider_adapter")
                == record.payload.get("provider_adapter")
                and stored.record.payload.get("symbol_binding_status")
                == record.payload.get("symbol_binding_status")
                and stored.record.payload.get("symbol_binding_proof")
                == record.payload.get("symbol_binding_proof")
                and (
                    record.payload.get("source") != "SEC"
                    or record.status != "CONFLICTED"
                    or stored.record.status == "CONFLICTED"
                )
            ):
                return stored
        return self.evidence_store.append(record).evidence

    def _rebuild_read_model(
        self,
        *,
        asof: datetime | None = None,
        analysis_budget: int = _NEWS_ANALYSIS_BATCH_SIZE,
        calendar_envelope: Mapping[str, object] | None = None,
        allow_shadow_model_calls: bool = True,
    ) -> None:
        now = _aware(asof or self._clock())
        active_calendar_envelope = (
            dict(calendar_envelope)
            if calendar_envelope is not None
            else self._calendar_envelope
        )
        maximum_analysis_budget = (
            _NEWS_ANALYSIS_BATCH_SIZE
            if allow_shadow_model_calls
            else _NEWS_LOCAL_RESTORE_ANALYSIS_BATCH_SIZE
        )
        if (
            isinstance(analysis_budget, bool)
            or not isinstance(analysis_budget, int)
            or not 0 <= analysis_budget <= maximum_analysis_budget
        ):
            raise ValueError("analysis budget is outside the bounded refresh contract")
        if not isinstance(allow_shadow_model_calls, bool):
            raise TypeError("allow_shadow_model_calls must be a bool")
        # NewsAnalysisStore verifies the complete append-only chain when it is
        # opened.  Thereafter resolve() verifies each loaded row (or the append
        # tail), so rescanning the permanent ledger on every 60-120 second
        # projection refresh adds latency without increasing authority safety.
        news_records = self.evidence_store.query(
            first_seen_at_or_before=now,
            kinds=("NEWS",),
            limit=_NEWS_READ_MODEL_EVIDENCE_LIMIT,
        )
        calendar_records = self.evidence_store.query(
            first_seen_at_or_before=now,
            kinds=("CALENDAR",),
            # Calendar projection has no classifier/LLM work. Preserve the
            # previous 5,000-row restart horizon so later provider versions do
            # not evict an earlier-sequence event that is still in two weeks.
            limit=_CALENDAR_READ_MODEL_EVIDENCE_LIMIT,
        )
        calendar_groups: dict[str, list[StoredEvidence]] = defaultdict(list)
        news_groups = _group_news_story_records(
            tuple(
                stored
                for stored in news_records
                if (
                    stored.record.kind == "NEWS"
                    and stored.record.first_seen_at >= now - _NEWS_READ_MODEL_MAX_AGE
                )
            )
        )
        for stored in calendar_records:
            if stored.record.kind == "CALENDAR":
                calendar_groups[stored.identity].append(stored)

        analyses, backfill = self._advance_analysis_backfill(
            news_groups,
            now=now,
            budget=analysis_budget,
            evidence_window_truncated=(
                len(news_records) >= _NEWS_READ_MODEL_EVIDENCE_LIMIT
            ),
        )
        research_pool = self._analysis.pre_market_research_pool(item[0] for item in analyses)
        # General market news remains visible in the bounded research pool, but
        # symbol-watch ranking must be computed independently so quarantined,
        # symbol-less legacy rows cannot consume all ten watch positions.
        watch_pool = self._analysis.pre_market_research_pool(
            item[0]
            for item in analyses
            if len(item[0].news.symbols) == 1
        )
        action_pool = self._analysis.open_market_action_pool(item[0] for item in analyses)
        shadow_by_event, shadow_status = self._shadow_advisory_overlays(
            tuple(item[0] for item in analyses),
            research_pool,
            now=now,
            allow_model_calls=analysis_budget > 0 and allow_shadow_model_calls,
        )
        shadow_suggested_ranks = _shadow_suggested_ranks(shadow_by_event)
        if backfill["status"] != "READY":
            # A partial or failed classification window may remain visible for
            # research, but cannot contribute even to the display action pool.
            action_pool = ()
        research_ranked = {item.analysis_id: item for item in research_pool}
        watch_ranked = {item.analysis_id: item for item in watch_pool}
        action_ranked = {item.analysis_id: item for item in action_pool}

        pre_market, open_repriced, option_action_pool = build_preselection_pools(
            self._preselections,
            now=now,
        )
        if not self._preselection_atomic_binding_available:
            option_action_pool = ()
        pre_market_rows = [
            item.as_dict(research_rank=index)
            for index, item in enumerate(pre_market, start=1)
        ]
        for row in pre_market_rows:
            lineage = self._preselection_lineage.get(
                (str(row.get("preselection_id")), str(row.get("phase")))
            )
            if lineage is not None:
                row["ledger_lineage"] = dict(lineage)
        option_action_ranks = {
            item.candidate.preselection_id: index
            for index, item in enumerate(option_action_pool, start=1)
        }
        open_repriced_rows = [
            item.as_dict(
                repriced_rank=index,
                action_rank=option_action_ranks.get(item.candidate.preselection_id),
            )
            for index, item in enumerate(open_repriced, start=1)
        ]
        for row in open_repriced_rows:
            lineage = self._preselection_lineage.get(
                (str(row.get("preselection_id")), str(row.get("phase")))
            )
            if lineage is not None:
                row["ledger_lineage"] = dict(lineage)
            if not self._preselection_atomic_binding_available:
                blockers = row.get("blockers")
                safe_blockers = (
                    list(blockers)
                    if isinstance(blockers, Sequence)
                    and not isinstance(blockers, (str, bytes, bytearray))
                    else []
                )
                source_blocker = str(
                    self._preselection_coverage.get("reason")
                    or "PRESELECTION_ATOMIC_BINDING_INVALID"
                )
                row["blockers"] = list(
                    dict.fromkeys([*safe_blockers, source_blocker])
                )
                row["action_rank"] = None
                row["action_pool_eligible"] = False
                row["research_only"] = True
        option_action_rows = [
            row for row in open_repriced_rows if row.get("action_rank") is not None
        ]
        action_quote_times = [
            leg.quote_asof
            for item in option_action_pool
            for leg in item.candidate.legs
            if leg.quote_asof is not None
        ]
        self._preselection_action_expires_at = (
            min(action_quote_times) + MAXIMUM_QUOTE_AGE
            if action_quote_times
            else None
        )
        related_by_symbol: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in (*pre_market_rows, *open_repriced_rows):
            symbol = str(row.get("underlying") or "").strip().upper()
            if symbol:
                related_by_symbol[symbol].append(row)

        rows: list[dict[str, object]] = []
        for analysis, evidence, conflicted, binding in analyses:
            research = research_ranked.get(analysis.analysis_id)
            watch = watch_ranked.get(analysis.analysis_id)
            action = action_ranked.get(analysis.analysis_id)
            display_analysis = action or research or analysis
            row = self._news_row(
                    display_analysis,
                    evidence,
                    conflicted,
                    research_rank=None if research is None else research.rank,
                    watch_rank=None if watch is None else watch.rank,
                    action_rank=None if action is None else action.rank,
                    binding=binding,
                    rebuild_at=now,
                    related_options=related_by_symbol.get(
                        display_analysis.news.symbols[0]
                        if len(display_analysis.news.symbols) == 1
                        else "",
                        (),
                    ),
                )
            shadow = shadow_by_event.get(display_analysis.news.event_id)
            row["deterministic_research_rank"] = row.get("research_rank")
            row["shadow_suggested_rank"] = None
            row["rank_displacement"] = None
            row["shadow_action_effect"] = "NONE"
            row["shadow_risk_effect"] = "NONE"
            row["shadow_eligibility_effect"] = "NONE"
            if shadow is not None:
                row["research_advisory"] = dict(shadow)
                row["shadow_research_priority_score"] = shadow.get(
                    "research_priority_score"
                )
                row["shadow_prediction_count"] = shadow.get(
                    "shadow_prediction_count", 0
                )
                suggested = shadow_suggested_ranks.get(
                    display_analysis.news.event_id
                )
                if suggested is not None:
                    row["shadow_suggested_rank"] = suggested
                    deterministic_rank = row.get("deterministic_research_rank")
                    if isinstance(deterministic_rank, int):
                        row["rank_displacement"] = suggested - deterministic_rank
            row["intelligence"] = project_event_intelligence(row)
            rows.append(row)
        rows.sort(
            key=lambda item: (
                int(item["research_rank"])
                if isinstance(item.get("research_rank"), int)
                else 10_000,
                -float(item["combined_opportunity_score"]),
                -float(item["event_impact_score"]),
                str(item["published_at"]),
                str(item["id"]),
            )
        )
        equity_news_rows = _equity_news_rows(rows)
        # Keep the API/GUI payload bounded even though the internal candidate
        # window is wider for provider-fair deterministic research.  Ranking
        # order is authority-neutral here: every row remains SUPPORTING_ONLY.
        rows = rows[:_NEWS_READ_MODEL_OUTPUT_LIMIT]

        official_snapshot = self._official_calendar_snapshot
        official_cache = self._official_calendar_cache
        current_official_event_versions = (
            _official_snapshot_event_versions(official_snapshot)
            if (
                official_snapshot is not None
                and official_cache is not None
                and official_snapshot.observed_at == official_cache.observed_at
            )
            else None
        )
        calendar_window_start = _calendar_envelope_timestamp(
            active_calendar_envelope,
            "window_start",
        ) or now
        calendar_window_end = _calendar_envelope_timestamp(
            active_calendar_envelope,
            "window_end",
        ) or (now + timedelta(days=14))
        calendar_rows = [
            row
            for group in calendar_groups.values()
            if (
                row := self._calendar_row(
                    group,
                    now=now,
                    window_start=calendar_window_start,
                    window_end=calendar_window_end,
                )
            )
            is not None
        ]
        calendar_rows, reaction_projection = self._merge_reaction_read_models(
            calendar_rows,
            now=now,
            window_start=calendar_window_start,
            window_end=calendar_window_end,
            current_official_event_versions=current_official_event_versions,
        )
        calendar_rows = [
            row
            for row in calendar_rows
            if _calendar_payload_in_declared_window(
                row,
                window_start=calendar_window_start,
                window_end=calendar_window_end,
            )
            or _has_completed_historical_reaction(row)
        ]
        calendar_rows = [
            _mark_calendar_generation(row, active_calendar_envelope)
            for row in calendar_rows
        ]
        for row in calendar_rows:
            row["intelligence"] = project_event_intelligence(row)
        calendar_rows.sort(key=_calendar_sort_key)
        calendar_windows = {
            "this_week": [
                str(item["id"])
                for item in calendar_rows
                if "THIS_WEEK" in item.get("windows", ())
            ],
            "next_week": [
                str(item["id"])
                for item in calendar_rows
                if "NEXT_WEEK" in item.get("windows", ())
            ],
            "future_two_weeks": [
                str(item["id"])
                for item in calendar_rows
                if "FUTURE_TWO_WEEKS" in item.get("windows", ())
            ],
        }
        calendar_reasons = list(self._official_calendar_reasons)
        if self._calendar_health.status == "DEGRADED" and not calendar_reasons:
            calendar_reasons.append("CALENDAR_PROVIDER_DEGRADED")
        elif self._calendar_health.status in {"UNKNOWN", "UNCONFIGURED"} and not calendar_reasons:
            calendar_reasons.append(
                "CALENDAR_NOT_REFRESHED"
                if self._calendar_health.status == "UNKNOWN"
                else "CALENDAR_PROVIDERS_UNCONFIGURED"
            )
        calendar_decision = (
            "OBSERVATION_ONLY"
            if self._calendar_health.status == "READY"
            else "NO_TRADE"
        )
        preselection_coverage = dict(self._preselection_coverage)
        action_expiry_candidates = [
            _aware(binding.tradability.observed_at) + _MAXIMUM_IBKR_BINDING_AGE
            for binding in self._ibkr_bindings.values()
            if binding.tradability.complete
        ]
        if self._preselection_action_expires_at is not None:
            action_expiry_candidates.append(self._preselection_action_expires_at)
        action_expires_at = (
            min(action_expiry_candidates) if action_expiry_candidates else None
        )
        # Prepare every fallible projection before swapping any frozen payload.
        # A bad clock, cadence store, or projection helper must leave the prior
        # body and publication metadata paired as one complete generation.
        with self._state_lock:
            base_diagnostic = self._publication_diagnostic
        source_runtime = self._cadence.projections(now=now)
        frozen_source_runtime = [dict(item) for item in source_runtime]
        frozen_source_health = [dict(item) for item in self._source_health]
        next_calendar_envelope = (
            None
            if active_calendar_envelope is None
            else dict(active_calendar_envelope)
        )
        news_payload = {
            "news": rows,
            "count": len(rows),
            "asof": now.isoformat(),
            "provider": self._news_health.as_dict(name="news-coordinator"),
            "source_health": frozen_source_health,
            "source_runtime": frozen_source_runtime,
            "analysis_backfill": backfill,
            "shadow_advisory": shadow_status,
            "research_pool_count": len(research_pool),
            "action_pool_count": len(action_pool),
            "top3_count": len(action_pool),
            "pre_market_preselections": pre_market_rows,
            "pre_market_preselection_count": len(pre_market_rows),
            "open_market_repriced": open_repriced_rows,
            "open_market_repriced_count": len(open_repriced_rows),
            "option_action_pool": option_action_rows,
            "option_action_pool_count": len(option_action_rows),
            "preselection_available_count": preselection_coverage.get(
                "available_count",
                0,
            ),
            "preselection_coverage_reason": preselection_coverage.get("reason"),
            "preselection_coverage": preselection_coverage,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
        }
        equity_news_payload = {
            "news": equity_news_rows,
            "count": len(equity_news_rows),
            "asof": now.isoformat(),
            "source_health": [dict(item) for item in frozen_source_health],
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }
        calendar_payload = {
            "calendar": calendar_rows,
            "count": len(calendar_rows),
            "asof": now.isoformat(),
            "provider": self._calendar_health.as_dict(name="calendar-coordinator"),
            "source_runtime": [dict(item) for item in frozen_source_runtime],
            "decision": calendar_decision,
            "decision_authority": "SUPPORTING_ONLY",
            "window_start": calendar_window_start.isoformat(),
            "window_end": calendar_window_end.isoformat(),
            "windows": calendar_windows,
            "window_counts": {
                name: len(identifiers)
                for name, identifiers in calendar_windows.items()
            },
            "sources": (
                [item.as_dict() for item in official_snapshot.sources]
                if official_snapshot is not None
                else []
            ),
            "reasons": calendar_reasons,
            "snapshot_hash": (
                official_snapshot.snapshot_hash
                if official_snapshot is not None
                else None
            ),
            "calendar_envelope": (
                None
                if next_calendar_envelope is None
                else _copy_json(next_calendar_envelope)
            ),
            "reaction_provider": reaction_projection,
            "reaction_decision": reaction_projection["decision"],
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }
        advisory_payload = _phase2_advisory_fallback_payload(
            as_of=now,
            reason=self._phase2_advisory_fallback_reason,
        )
        conflicts = _phase2_source_conflicts(rows)
        source_evidence_payload = _phase2_source_evidence_payload(
            rows=self._phase2_source_rows,
            as_of=now,
            conflicts=conflicts,
        )
        read_model_published_at = _aware(self._clock())
        published_diagnostic = base_diagnostic.publish_read_model(
            published_at=read_model_published_at,
            read_model_asof=now,
            action_expires_at=action_expires_at,
        )

        with self._state_lock:
            self._calendar_envelope = next_calendar_envelope
            self._news_payload = news_payload
            self._equity_news_payload = equity_news_payload
            self._calendar_payload = calendar_payload
            self._advisory_payload = advisory_payload
            self._source_evidence_payload = source_evidence_payload
            self._publication_diagnostic = published_diagnostic

    def _shadow_advisory_overlays(
        self,
        analyses: Sequence[AnalyzedNews],
        research_pool: Sequence[AnalyzedNews],
        *,
        now: datetime,
        allow_model_calls: bool,
    ) -> tuple[dict[str, Mapping[str, object]], dict[str, object]]:
        """Restore and optionally advance the supporting-only model lane."""

        rank_by_analysis = {
            item.analysis_id: item.rank
            for item in research_pool
            if item.rank is not None
        }
        inputs = tuple(
            _shadow_research_input(
                item,
                eligible=item.analysis_id in rank_by_analysis,
                pre_model_priority_rank=rank_by_analysis.get(item.analysis_id),
                allowed_symbols=self._core_symbols,
            )
            for item in analyses
        )
        pre_model_skipped_reasons = _shadow_pre_model_skip_reasons(inputs)
        pre_model_skipped_count = sum(pre_model_skipped_reasons.values())
        eligible_inputs = tuple(
            item
            for item in inputs
            if item.eligible is True and item.pre_model_priority_rank is not None
        )
        event_ids = tuple(item.news.event_id for item in eligible_inputs)
        expected_advisory_ids = {
            item.news.event_id: (
                f"news-advisory:{_shadow_advisory_input_hash(item)}"
            )
            for item in eligible_inputs
        }
        restored_states: Mapping[str, Mapping[str, object]] = {}
        writer = self._shadow_writer
        if writer is not None:
            try:
                restored_states = writer.advisory_projections(
                    event_ids,
                    expected_advisory_ids=expected_advisory_ids,
                )
            except Exception:
                return {}, _shadow_status("DEGRADED", "SHADOW_LEDGER_UNAVAILABLE")
        restored = {
            item.news.event_id: _normalize_restored_advisory(state)
            for item in eligible_inputs
            if (
                (state := restored_states.get(item.news.event_id)) is not None
                and _restored_advisory_matches_input(state, item)
            )
        }

        if not eligible_inputs:
            return {}, _shadow_status(
                "UNAVAILABLE",
                "SHADOW_ADVISORY_NO_ELIGIBLE_INPUTS",
                input_count=len(inputs),
                eligible_input_count=0,
                skipped_count=pre_model_skipped_count,
                skipped_reasons=pre_model_skipped_reasons,
            )

        advisory = self._shadow_advisory
        if advisory is None or not allow_model_calls:
            status = "READY" if restored else "UNAVAILABLE"
            reason = None if restored else "SHADOW_ADVISORY_NOT_ADVANCED"
            return restored, _shadow_status(
                status,
                reason,
                advisory_count=len(restored),
                input_count=len(inputs),
                eligible_input_count=len(eligible_inputs),
                skipped_count=pre_model_skipped_count,
                skipped_reasons=pre_model_skipped_reasons,
            )

        pending_inputs: list[ResearchAdvisoryInput] = []
        repair_failure_count = 0
        for item in eligible_inputs:
            state = restored_states.get(item.news.event_id)
            if _restored_advisory_matches_input(state, item):
                continue
            if writer is not None and _restored_advisory_has_exact_input(state, item):
                repaired = _rebuild_restored_advisory(state, item)
                if repaired is not None:
                    try:
                        writer.record(repaired, item.news, recorded_at=now)
                    except Exception:
                        repair_failure_count += 1
                        continue
                    else:
                        restored[item.news.event_id] = _advisory_projection(repaired)
                        continue
            pending_inputs.append(item)
        try:
            batch = advisory.process(pending_inputs)
        except Exception:
            return restored, _shadow_status(
                "DEGRADED",
                "SHADOW_ADVISORY_FAILED",
                advisory_count=len(restored),
                input_count=len(inputs),
                eligible_input_count=len(eligible_inputs),
                skipped_count=pre_model_skipped_count,
                skipped_reasons=pre_model_skipped_reasons,
            )

        by_event = {item.news.event_id: item.news for item in eligible_inputs}
        champion_by_event = {
            item.news.event_id: item.classification
            for item in analyses
            if item.news.event_id in by_event
        }
        persisted_failures = 0
        for projection in batch.advisories:
            if writer is not None:
                news = by_event.get(projection.event_id)
                if news is None:
                    persisted_failures += 1
                    continue
                try:
                    writer.record(
                        projection,
                        news,
                        recorded_at=now,
                        champion_classification=champion_by_event.get(
                            projection.event_id
                        ),
                    )
                except Exception:
                    persisted_failures += 1
                    continue
            restored[projection.event_id] = _advisory_projection(projection)

        failure_count = (
            len(batch.failures) + persisted_failures + repair_failure_count
        )
        failure_reasons: dict[str, int] = defaultdict(int)
        for failure in batch.failures:
            failure_reasons[failure.reason_code] += 1
        if persisted_failures:
            failure_reasons["SHADOW_ADVISORY_LEDGER_WRITE_FAILED"] += (
                persisted_failures
            )
        if repair_failure_count:
            failure_reasons["SHADOW_ADVISORY_LEDGER_REPAIR_FAILED"] += (
                repair_failure_count
            )
        inner_skipped_reasons = dict(batch.skipped_reasons)
        skipped_reasons = _merge_reason_counts(
            pre_model_skipped_reasons,
            inner_skipped_reasons,
        )
        skipped_count = pre_model_skipped_count + batch.skipped_count
        no_attempt_or_restore = batch.attempted_count == 0 and not restored
        status = (
            "DEGRADED"
            if failure_count
            else "UNAVAILABLE"
            if no_attempt_or_restore
            else "PENDING"
            if batch.deferred_count or batch.skipped_count
            else "READY"
        )
        reason = (
            "SHADOW_ADVISORY_LEDGER_REPAIR_FAILED"
            if repair_failure_count
            else "SHADOW_ADVISORY_FAILURE"
            if failure_count
            else "SHADOW_ADVISORY_NO_ATTEMPT"
            if no_attempt_or_restore
            else "SHADOW_ADVISORY_DEFERRED"
            if batch.deferred_count
            else "SHADOW_ADVISORY_SKIPPED"
            if batch.skipped_count
            else None
        )
        return restored, _shadow_status(
            status,
            reason,
            advisory_count=len(restored),
            attempted_count=batch.attempted_count,
            failure_count=failure_count,
            deferred_count=batch.deferred_count,
            failure_reasons=failure_reasons,
            input_count=len(inputs),
            eligible_input_count=len(eligible_inputs),
            skipped_count=skipped_count,
            skipped_reasons=skipped_reasons,
        )

    def _advance_analysis_backfill(
        self,
        news_groups: Mapping[str, Sequence[StoredEvidence]],
        *,
        now: datetime,
        budget: int,
        evidence_window_truncated: bool,
    ) -> tuple[
        list[tuple[AnalyzedNews, list[StoredEvidence], bool, IbkrNewsBinding | None]],
        dict[str, object],
    ]:
        """Advance one bounded, retryable slice of the recent news window."""

        groups = {
            identity: tuple(sorted(group, key=lambda item: item.sequence))
            for identity, group in news_groups.items()
        }
        signatures = {
            identity: self._analysis_group_signature(group, now=now)
            for identity, group in groups.items()
        }

        for identity, cached in tuple(self._analysis_cache.items()):
            signature = signatures.get(identity)
            if signature == cached.signature:
                continue
            group = groups.get(identity)
            if (
                group is not None
                and cached.binding is not None
                and self._current_analysis_binding(group, now=now) is None
                and _same_evidence(cached.evidence, group)
            ):
                # Quote expiry is evaluated on the GUI read path. Reuse only
                # the immutable classification/evidence while dropping every
                # quote-derived score and confirmation, so expiry is immediate
                # without invoking an LLM from news_payload().
                combined_score = combined_opportunity_score(
                    cached.analysis.event_impact_score,
                    Decimal("0"),
                )
                self._analysis_cache[identity] = _CachedNewsAnalysis(
                    signature=signature,
                    analysis=replace(
                        cached.analysis,
                        stage=AnalysisStage.PROVISIONAL,
                        option_tradability=score_band(Decimal("0")),
                        combined_opportunity=score_band(combined_score),
                        option_tradability_score=Decimal("0"),
                        combined_opportunity_score=combined_score,
                        tradability_data=None,
                        market_confirmation=None,
                        rank=None,
                        rank_one=False,
                    ),
                    evidence=cached.evidence,
                    conflicted=cached.conflicted,
                    binding=None,
                )
                continue
            self._analysis_cache.pop(identity, None)
        for identity, signature in tuple(self._analysis_ignored.items()):
            if signatures.get(identity) != signature:
                self._analysis_ignored.pop(identity, None)
        self._analysis_failures = {
            (identity, signature)
            for identity, signature in self._analysis_failures
            if signatures.get(identity) == signature
        }

        retained_pending: list[tuple[str, str]] = []
        retained_keys: set[tuple[str, str]] = set()
        for identity, signature in self._analysis_pending:
            key = (identity, signature)
            if (
                signatures.get(identity) == signature
                and identity not in self._analysis_cache
                and self._analysis_ignored.get(identity) != signature
                and key not in retained_keys
            ):
                retained_pending.append(key)
                retained_keys.add(key)

        retained_model: list[tuple[str, str]] = []
        retained_model_keys: set[tuple[str, str]] = set()
        for identity, signature in self._analysis_model_pending:
            key = (identity, signature)
            if (
                signatures.get(identity) == signature
                and identity not in self._analysis_cache
                and self._analysis_ignored.get(identity) != signature
                and key not in retained_model_keys
            ):
                retained_model.append(key)
                retained_model_keys.add(key)

        newest_first = sorted(
            groups,
            key=lambda identity: (
                -max(item.sequence for item in groups[identity]),
                identity,
            ),
        )
        newly_pending = [
            (identity, signatures[identity])
            for identity in newest_first
            if identity not in self._analysis_cache
            and self._analysis_ignored.get(identity) != signatures[identity]
            and (identity, signatures[identity]) not in retained_keys
            and (identity, signatures[identity]) not in retained_model_keys
        ]
        # Retained work always stays ahead of newly observed rows. This gives a
        # finite recent-window backlog forward progress even under a continuous
        # provider stream.
        pending = [*retained_pending, *newly_pending]
        self._analysis_pending = pending
        self._analysis_model_pending = retained_model

        if budget > 0 and not self.analysis_store.integrity_verified:
            try:
                progress = self.analysis_store.verify_integrity_batch(
                    _NEWS_ANALYSIS_INTEGRITY_BATCH_SIZE
                )
            except Exception:
                self._analysis_integrity = {
                    "status": "DEGRADED",
                    "batch_rows": 0,
                    "verified_rows": 0,
                    "remaining_rows": self.analysis_store.count,
                    "complete": False,
                }
            else:
                self._analysis_integrity = {
                    "status": "VERIFIED" if progress.complete else "PENDING",
                    "batch_rows": progress.batch_rows,
                    "verified_rows": progress.verified_rows,
                    "remaining_rows": progress.remaining_rows,
                    "complete": progress.complete,
                }
        if not self.analysis_store.integrity_verified:
            integrity_failed = self._analysis_integrity["status"] == "DEGRADED"
            status = "DEGRADED" if integrity_failed else "PENDING"
            reason = (
                "ANALYSIS_LEDGER_INTEGRITY_FAILED"
                if integrity_failed
                else "ANALYSIS_LEDGER_INTEGRITY_PENDING"
            )
            self._analysis_backfill_status = status
            return [], self._analysis_backfill_payload(
                status=status,
                reason=reason,
                evidence_window_truncated=evidence_window_truncated,
            )

        selected_lookup = pending[:_NEWS_ANALYSIS_LOOKUP_BATCH_SIZE]
        remaining_lookup = pending[_NEWS_ANALYSIS_LOOKUP_BATCH_SIZE:]
        model_pending = list(retained_model)
        model_keys = set(retained_model)
        processed = 0
        failed_this_cycle = 0
        persisted_hits = 0
        for identity, signature in selected_lookup:
            try:
                built = self._analyze_group(
                    groups[identity],
                    now=now,
                    lookup_only=True,
                )
            except _AnalysisLookupMiss:
                key = (identity, signature)
                if key not in model_keys:
                    model_pending.append(key)
                    model_keys.add(key)
                continue
            except Exception:
                self._analysis_cache.pop(identity, None)
                self._analysis_ignored.pop(identity, None)
                self._analysis_failures.add((identity, signature))
                remaining_lookup.append((identity, signature))
                failed_this_cycle += 1
                continue
            processed += 1
            persisted_hits += 1
            self._analysis_failures.discard((identity, signature))
            if built is None:
                self._analysis_ignored[identity] = signature
                continue
            analysis, evidence, conflicted, binding = built
            self._analysis_cache[identity] = _CachedNewsAnalysis(
                signature=signature,
                analysis=analysis,
                evidence=tuple(evidence),
                conflicted=conflicted,
                binding=binding,
            )

        selected_model = model_pending[:budget]
        remaining_model = model_pending[budget:]
        for identity, signature in selected_model:
            try:
                built = self._analyze_group(groups[identity], now=now)
            except Exception:
                self._analysis_cache.pop(identity, None)
                self._analysis_ignored.pop(identity, None)
                self._analysis_failures.add((identity, signature))
                remaining_model.append((identity, signature))
                failed_this_cycle += 1
                continue
            processed += 1
            self._analysis_failures.discard((identity, signature))
            if built is None:
                self._analysis_ignored[identity] = signature
                continue
            analysis, evidence, conflicted, binding = built
            self._analysis_cache[identity] = _CachedNewsAnalysis(
                signature=signature,
                analysis=analysis,
                evidence=tuple(evidence),
                conflicted=conflicted,
                binding=binding,
            )

        self._analysis_pending = remaining_lookup
        self._analysis_model_pending = remaining_model
        if self._analysis_failures:
            status = "DEGRADED"
            reason = "ANALYSIS_FAILED_RETRY_PENDING"
        elif self._analysis_pending or self._analysis_model_pending:
            status = "PENDING"
            reason = "BOUNDED_BACKFILL_PENDING"
        else:
            status = "READY"
            reason = None
        self._analysis_backfill_status = status

        cached_rows = sorted(
            self._analysis_cache.items(),
            key=lambda item: (
                -max(evidence.sequence for evidence in item[1].evidence),
                item[0],
            ),
        )
        analyses = [
            (
                cached.analysis,
                list(cached.evidence),
                cached.conflicted,
                cached.binding,
            )
            for _identity, cached in cached_rows
        ]
        return analyses, self._analysis_backfill_payload(
            status=status,
            reason=reason,
            evidence_window_truncated=evidence_window_truncated,
            processed_this_cycle=processed,
            failed_this_cycle=failed_this_cycle,
            persisted_hits_this_cycle=persisted_hits,
            model_misses_this_cycle=len(selected_model),
        )

    def _analysis_backfill_payload(
        self,
        *,
        status: str,
        reason: str | None,
        evidence_window_truncated: bool,
        processed_this_cycle: int = 0,
        failed_this_cycle: int = 0,
        persisted_hits_this_cycle: int = 0,
        model_misses_this_cycle: int = 0,
    ) -> dict[str, object]:
        return {
            "status": status,
            "reason": reason,
            "pending_count": len(self._analysis_pending)
            + len(self._analysis_model_pending),
            "failed_count": len(self._analysis_failures),
            "processed_this_cycle": processed_this_cycle,
            "failed_this_cycle": failed_this_cycle,
            "persisted_hits_this_cycle": persisted_hits_this_cycle,
            "model_misses_this_cycle": model_misses_this_cycle,
            "model_batch_limit": _NEWS_ANALYSIS_BATCH_SIZE,
            "local_restore_batch_limit": (
                _NEWS_LOCAL_RESTORE_ANALYSIS_BATCH_SIZE
            ),
            "lookup_batch_limit": _NEWS_ANALYSIS_LOOKUP_BATCH_SIZE,
            "evidence_window_limit": _NEWS_READ_MODEL_EVIDENCE_LIMIT,
            "recent_window_days": _NEWS_READ_MODEL_MAX_AGE.days,
            "window_truncated": evidence_window_truncated,
            "integrity": dict(self._analysis_integrity),
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
        }

    def _analysis_group_signature(
        self,
        group: Sequence[StoredEvidence],
        *,
        now: datetime,
    ) -> str:
        ordered = sorted(group, key=lambda item: (_source_rank(item), item.sequence))
        binding = self._current_analysis_binding(ordered, now=now)
        parts = [
            f"{item.sequence}:{item.content_hash}:{item.status}"
            for item in ordered
        ]
        parts.append("binding:none" if binding is None else f"binding:{binding!r}")
        return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

    def _current_analysis_binding(
        self,
        group: Sequence[StoredEvidence],
        *,
        now: datetime,
    ) -> IbkrNewsBinding | None:
        symbols, _, _ = _news_symbol_bindings(group)
        symbol = symbols[0] if len(symbols) == 1 else ""
        binding = self._ibkr_bindings.get(symbol)
        return (
            binding
            if binding is not None and _binding_is_current(binding, now)
            else None
        )

    def _analyze_group(
        self,
        group: Sequence[StoredEvidence],
        *,
        now: datetime,
        lookup_only: bool = False,
    ) -> tuple[
        AnalyzedNews,
        list[StoredEvidence],
        bool,
        IbkrNewsBinding | None,
    ] | None:
        if not group:
            return None
        ordered = sorted(group, key=lambda item: (_source_rank(item), item.sequence))
        primary = ordered[0]
        payload = primary.record.payload
        headline = str(payload.get("headline") or "").strip()
        source = str(payload.get("source") or primary.record.provider).strip()
        if not headline or not source:
            return None
        semantic_hashes = {_news_story_content_hash(item) for item in ordered}
        conflicted = (
            len(semantic_hashes) > 1
            or any(_news_record_conflicted(item) for item in ordered)
        )
        url = str(payload.get("url") or "").strip()
        evidence_ids = tuple(item.evidence_id for item in ordered)
        symbols, _, _ = _news_symbol_bindings(ordered)
        symbol = symbols[0] if len(symbols) == 1 else ""
        anchored = not conflicted and _is_official_anchor(primary)
        news = NewsInput(
            event_id=str(payload.get("event_id") or primary.identity),
            headline=headline,
            summary=str(payload.get("summary") or ""),
            source=source,
            source_url=url or f"urn:options-copilot:evidence:{primary.evidence_id}",
            published_at=primary.record.published_at,
            first_seen_at=min(item.record.first_seen_at for item in ordered),
            evidence_ids=evidence_ids,
            symbols=symbols,
            # ANCHORED means the factual source is official; the persisted and
            # public decision authority remains SUPPORTING_ONLY below.
            authority=(NewsAuthority.ANCHORED if anchored else NewsAuthority.SUPPORTING_ONLY),
            conflicting_evidence_ids=evidence_ids if conflicted else (),
            is_complete=bool(url.startswith(("https://", "http://"))) and not conflicted,
        )
        binding = (
            self._ibkr_bindings.get(symbol)
            if news.symbols == (symbol,) and symbol
            else None
        )
        if binding is not None and not _binding_is_current(binding, now):
            binding = None
        input_document = analysis_input_document(
            news=news,
            evidence_content_hashes=tuple(item.content_hash for item in ordered),
            analyzer_contract=self._analysis_contract,
            ibkr_binding=_analysis_binding_document(binding),
        )

        def build_analysis() -> AnalyzedNews:
            analysis = self._analysis.analyze(
                news,
                None if binding is None else binding.tradability,
            )
            if (
                binding is not None
                and anchored
                and binding.confirmation.direction
                is analysis.classification.direction
            ):
                try:
                    analysis = self._analysis.market_confirm(
                        analysis,
                        binding.confirmation,
                    )
                except ValueError:
                    # A quote/confirmation that predates the event cannot
                    # confirm it. Keep it in research and out of action pools.
                    pass
            return analysis

        if lookup_only:
            analysis = self.analysis_store.lookup(input_document)
            if analysis is None:
                raise _AnalysisLookupMiss
        else:
            analysis = self.analysis_store.resolve(input_document, build_analysis)
        return analysis, ordered, conflicted, binding

    def _news_row(
        self,
        analysis: AnalyzedNews,
        evidence: Sequence[StoredEvidence],
        conflicted: bool,
        *,
        research_rank: int | None,
        watch_rank: int | None,
        action_rank: int | None,
        binding: IbkrNewsBinding | None,
        rebuild_at: datetime,
        related_options: Sequence[Mapping[str, object]],
    ) -> dict[str, object]:
        primary = min(evidence, key=lambda item: (_source_rank(item), item.sequence))
        payload = primary.record.payload
        provenance = _provenance_sources(evidence)
        symbols, symbol_binding_status, provider_adapters = _news_symbol_bindings(
            evidence
        )
        provider_adapter = (
            provider_adapters[0] if len(provider_adapters) == 1 else None
        )
        event_ids = tuple(
            sorted(
                {
                    str(item.record.payload.get("event_id") or item.identity)
                    for item in evidence
                }
            )
        )
        raw_analysis_completed_at = getattr(analysis, "analyzed_at", None)
        analysis_completed_at = (
            _aware(raw_analysis_completed_at)
            if isinstance(raw_analysis_completed_at, datetime)
            else _aware(rebuild_at)
        )
        row = {
            "id": analysis.news.event_id,
            "title": analysis.news.headline,
            "headline": analysis.news.headline,
            "summary": analysis.news.summary,
            "source": analysis.news.source,
            "source_url": analysis.news.source_url,
            "story_identity": _news_story_group_identity(evidence),
            "provider_story_id": next(
                (
                    str(item.record.payload.get("provider_story_id"))
                    for item in evidence
                    if str(item.record.payload.get("provider_story_id") or "").strip()
                ),
                None,
            ),
            "deduplicated": len(event_ids) > 1,
            "merged_event_count": len(event_ids),
            "evidence_count": len(evidence),
            "symbols": list(symbols),
            "category": analysis.classification.category.value,
            "classifier": analysis.classification.classifier,
            "classification": {
                "category": analysis.classification.category.value,
                "symbols": list(analysis.classification.symbols),
                "direction": analysis.classification.direction.value,
                "horizon": analysis.classification.horizon.value,
                "confidence": float(analysis.classification.confidence),
                "classifier": analysis.classification.classifier,
                "counter_evidence": list(analysis.classification.counter_evidence),
                "evidence_ids": list(analysis.classification.evidence_ids),
                "decision_authority": "SUPPORTING_ONLY",
            },
            "direction": analysis.classification.direction.value,
            "horizon": analysis.classification.horizon.value,
            "confidence": float(analysis.classification.confidence),
            "status": "CONFLICTED" if conflicted else analysis.stage.value,
            "event_impact_score": float(analysis.event_impact_score),
            "option_tradability_score": float(analysis.option_tradability_score),
            "combined_opportunity_score": float(analysis.combined_opportunity_score),
            "event_impact": analysis.event_impact.value,
            "option_tradability": analysis.option_tradability.value,
            "combined_opportunity": analysis.combined_opportunity.value,
            "published_at": analysis.news.published_at.isoformat(),
            "first_seen_at": analysis.news.first_seen_at.isoformat(),
            "analysis_completed_at": analysis_completed_at.isoformat(),
            "published_to_first_seen_ms": _elapsed_ms(
                analysis.news.published_at,
                analysis.news.first_seen_at,
            ),
            "first_seen_to_analysis_ms": _elapsed_ms(
                analysis.news.first_seen_at,
                analysis_completed_at,
            ),
            "received_at": primary.record.ingested_at.isoformat(),
            "observed_at": max(item.record.observed_at for item in evidence).isoformat(),
            "rank": research_rank,
            "rank_one": research_rank == 1,
            "research_rank": research_rank,
            "watch_rank": watch_rank,
            "action_rank": action_rank,
            "action_rank_one": action_rank == 1,
            "research_pool": research_rank is not None,
            "action_pool": action_rank is not None,
            "action_pool_eligible": analysis.action_pool_eligible,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "decision_authority": "SUPPORTING_ONLY",
            "symbol_binding": {
                "status": symbol_binding_status,
                "provider_adapter": provider_adapter,
                "decision_authority": "SUPPORTING_ONLY",
            },
            "provenance": provenance,
            "ibkr_provenance": (
                None
                if binding is None
                else {
                    "source": "IBKR",
                    "symbol": binding.symbol,
                    "quote_snapshot_id": binding.quote_snapshot_id,
                    "observed_at": binding.tradability.observed_at.isoformat(),
                    "evidence_ids": list(binding.confirmation.evidence_ids),
                    "decision_authority": "SUPPORTING_ONLY",
                }
            ),
            "counter_evidence": list(analysis.classification.counter_evidence),
            "evidence": [
                {
                    "id": item.evidence_id,
                    "source": str(item.record.payload.get("source") or item.record.provider),
                    "provider": item.record.provider,
                    "source_id": item.record.source_id,
                    "title": str(item.record.payload.get("headline") or analysis.news.headline),
                    "url": str(item.record.payload.get("url") or ""),
                    "published_at": item.record.published_at.isoformat(),
                    "first_seen_at": item.record.first_seen_at.isoformat(),
                    "ingested_at": item.record.ingested_at.isoformat(),
                    "observed_at": item.record.observed_at.isoformat(),
                    "content_hash": item.content_hash,
                    "status": item.status,
                    "decision_authority": "SUPPORTING_ONLY",
                }
                for item in evidence
            ],
            "related_options": [_copy_json(dict(item)) for item in related_options],
            "source_content_hash": str(payload.get("source_content_hash") or ""),
        }
        research_proxy = bind_research_proxy(
            analysis.news,
            allowed_symbols=self._core_symbols,
        )
        if research_proxy is not None:
            row["research_proxy_binding"] = research_proxy.as_dict()
        return row

    def _calendar_row(
        self,
        group: Sequence[StoredEvidence],
        *,
        now: datetime,
        window_start: datetime,
        window_end: datetime,
    ) -> dict[str, object] | None:
        if not group:
            return None
        ordered = sorted(group, key=lambda item: item.sequence)
        primary = ordered[-1]
        payload = primary.record.payload
        if str(payload.get("calendar_origin") or "").upper() == "OFFICIAL":
            return self._official_calendar_row(ordered, now=now)

        symbol = str(payload.get("symbol") or primary.record.symbol or "").strip().upper()
        raw_date = str(payload.get("report_date") or "")
        try:
            report_date = date.fromisoformat(raw_date)
        except ValueError:
            return None
        scheduled_at = _earnings_time(report_date, payload.get("hour"))
        if not window_start <= scheduled_at < window_end:
            return None
        semantic_hashes = {
            str(item.record.payload.get("source_content_hash") or item.content_hash)
            for item in ordered
        }
        conflicted = len(semantic_hashes) > 1 or any(
            item.record.status == "CONFLICTED" for item in ordered
        )
        calendar_event = CalendarEvent(
            event_id=str(payload.get("event_id") or primary.identity),
            category=EventCategory.EARNINGS,
            scheduled_at=scheduled_at,
            title=f"{symbol} earnings" if symbol else "Company earnings",
            symbols=(symbol,) if symbol else (),
            source=str(payload.get("source") or primary.record.provider),
        )
        source = str(payload.get("source") or primary.record.provider)
        is_estimated = payload.get("is_estimated") is True
        source_url = str(payload.get("source_url") or "")
        windows = _calendar_window_memberships(
            event_at=calendar_event.scheduled_at,
            event_date=report_date,
            timezone_name="America/New_York",
            now=now,
        )
        return {
            "id": calendar_event.event_id,
            "event_id": calendar_event.event_id,
            "calendar_origin": "LEGACY",
            "source_id": str(payload.get("source_id") or calendar_event.event_id),
            "title": calendar_event.title,
            "category": calendar_event.category.value,
            "event_at": calendar_event.scheduled_at.isoformat(),
            "scheduled_at": calendar_event.scheduled_at.isoformat(),
            "event_date": report_date.isoformat(),
            "timezone": "America/New_York",
            "schedule_precision": "ESTIMATED" if is_estimated else "EXACT",
            "report_session": payload.get("report_session"),
            "is_estimated": is_estimated,
            "eps_estimate": payload.get("eps_estimate"),
            "revenue_estimate": payload.get("revenue_estimate"),
            "source_url": source_url,
            "symbols": list(calendar_event.symbols),
            "source": source,
            "status": "CONFLICTED" if conflicted else "PROVISIONAL",
            "importance": "HIGH",
            "country": "US",
            "published_at": primary.record.published_at.isoformat(),
            "first_seen_at": min(
                item.record.first_seen_at for item in ordered
            ).isoformat(),
            "ingested_at": max(
                item.record.ingested_at for item in ordered
            ).isoformat(),
            "observed_at": max(
                item.record.observed_at for item in ordered
            ).isoformat(),
            "windows": windows,
            "provenance": _legacy_calendar_provenance(ordered),
            "content_hash": str(payload.get("source_content_hash") or primary.content_hash),
            "record_hash": primary.content_hash,
            "evidence_identity": primary.identity,
            "evidence_row_hash": primary.row_hash,
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
            "evidence": [
                {
                    "id": item.evidence_id,
                    "source": str(item.record.payload.get("source") or item.record.provider),
                    "title": calendar_event.title,
                    "url": source_url,
                    "first_seen_at": item.record.first_seen_at.isoformat(),
                    "observed_at": item.record.observed_at.isoformat(),
                    "content_hash": item.content_hash,
                    "decision_authority": "SUPPORTING_ONLY",
                }
                for item in ordered
            ],
        }

    def _official_calendar_row(
        self,
        ordered: Sequence[StoredEvidence],
        *,
        now: datetime,
    ) -> dict[str, object] | None:
        primary = ordered[-1]
        payload = primary.record.payload
        event_at = _optional_timestamp(payload.get("scheduled_at", payload.get("event_at")))
        event_date = _optional_date(payload.get("event_date"))
        if event_at is None and event_date is None:
            return None
        if event_date is None and event_at is not None:
            timezone_name = str(payload.get("timezone") or "UTC")
            try:
                event_date = event_at.astimezone(ZoneInfo(timezone_name)).date()
            except Exception:
                return None
        assert event_date is not None
        source = str(payload.get("source") or "").strip()
        source_id = str(payload.get("source_id") or "").strip()
        title = str(payload.get("title") or "").strip()
        category = str(payload.get("category") or "").strip().upper()
        if not source or not source_id or not title or not category:
            return None
        semantic_hashes = {
            str(item.record.payload.get("source_content_hash") or item.content_hash)
            for item in ordered
        }
        conflicted = len(semantic_hashes) > 1 or any(
            item.record.status == "CONFLICTED"
            or str(item.record.payload.get("provider_status") or "").upper() == "CONFLICTED"
            for item in ordered
        )
        timezone_name = str(payload.get("timezone") or "UTC").strip()
        symbols = _stored_symbols(payload.get("symbols"))
        provenance = _structured_calendar_provenance(payload.get("provenance"))
        if not provenance:
            return None
        windows = _calendar_window_memberships(
            event_at=event_at,
            event_date=event_date,
            timezone_name=timezone_name,
            now=now,
        )
        event_id = str(payload.get("event_id") or payload.get("id") or primary.identity)
        row: dict[str, object] = {
            "id": event_id,
            "event_id": event_id,
            "calendar_origin": "OFFICIAL",
            "source_id": source_id,
            "title": title,
            "summary": "",
            "category": category,
            "event_at": None if event_at is None else event_at.isoformat(),
            "scheduled_at": None if event_at is None else event_at.isoformat(),
            "event_date": event_date.isoformat(),
            "timezone": timezone_name,
            "schedule_precision": str(payload.get("schedule_precision") or "").upper(),
            "symbols": symbols,
            "source": source,
            "source_url": str(payload.get("source_url") or ""),
            "url": str(payload.get("url") or ""),
            "status": "CONFLICTED" if conflicted else "PROVISIONAL",
            "importance": _calendar_importance(
                source=source,
                title=title,
                category=category,
            ),
            "country": "US",
            "published_at": _timestamp_text(payload.get("published_at")),
            "first_seen_at": min(
                item.record.first_seen_at for item in ordered
            ).isoformat(),
            "ingested_at": max(
                item.record.ingested_at for item in ordered
            ).isoformat(),
            "observed_at": max(
                item.record.observed_at for item in ordered
            ).isoformat(),
            "windows": windows,
            "provenance": provenance,
            "content_hash": str(payload.get("content_hash") or ""),
            "record_hash": str(payload.get("record_hash") or primary.content_hash),
            "evidence_identity": primary.identity,
            "evidence_row_hash": primary.row_hash,
            "evidence_hash": primary.content_hash,
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
            "evidence": [
                {
                    "id": item.evidence_id,
                    "source": str(item.record.payload.get("source") or item.record.provider),
                    "title": str(item.record.payload.get("title") or title),
                    "url": str(item.record.payload.get("url") or item.record.payload.get("source_url") or ""),
                    "published_at": item.record.published_at.isoformat(),
                    "first_seen_at": item.record.first_seen_at.isoformat(),
                    "ingested_at": item.record.ingested_at.isoformat(),
                    "observed_at": item.record.observed_at.isoformat(),
                    "content_hash": item.content_hash,
                    "status": item.status,
                    "decision_authority": "SUPPORTING_ONLY",
                }
                for item in ordered
            ],
        }
        # Display timestamps intentionally describe the whole evidence group,
        # but reaction identity must stay bound to one persisted schedule
        # version.  Using this version's original observation avoids changing
        # the event hash merely because a later refresh was observed.
        row["reaction_identity_provenance"] = {
            "published_at": _timestamp_text(payload.get("published_at")),
            "first_seen_at": primary.record.first_seen_at.isoformat(),
            "observed_at": primary.record.observed_at.isoformat(),
            "calendar_record_hash": primary.content_hash,
            "decision_authority": "SUPPORTING_ONLY",
        }
        row["reaction_identity_hash"] = _calendar_reaction_identity_hash(row)
        return row

    def _merge_reaction_read_models(
        self,
        rows: Sequence[dict[str, object]],
        *,
        now: datetime,
        window_start: datetime,
        window_end: datetime,
        current_official_event_versions: Mapping[
            str, tuple[str, str]
        ] | None,
    ) -> tuple[list[dict[str, object]], dict[str, object]]:
        """Attach verified reaction summaries without altering calendar facts.

        The provider batch for the current declared window is atomic.  Historical
        rows use a separate bounded lookup so corrupt or duplicate old ledgers
        cannot alter current-window provider health or conflict state.
        """

        projected = [dict(row) for row in rows]
        all_official = [
            row for row in projected if row.get("calendar_origin") == "OFFICIAL"
        ]
        for row in projected:
            if row.get("calendar_origin") != "OFFICIAL":
                row["reaction"] = _reaction_unsupported_projection(
                    row,
                )

        if current_official_event_versions is None:
            for row in all_official:
                row["reaction"] = _reaction_failure_projection(
                    row,
                    status="UNAVAILABLE",
                    reason="OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE",
                    event_hash=_calendar_reaction_identity_hash(row),
                )
            return projected, _reaction_provider_projection(
                status="UNAVAILABLE",
                decision="NO_TRADE",
                reason="OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE",
                ledger_count=0,
                matched_count=0,
            )

        official_candidates: list[dict[str, object]] = []
        historical: list[dict[str, object]] = []
        inactive: list[dict[str, object]] = []
        for row in all_official:
            if (
                _calendar_payload_matches_snapshot_version(
                    row,
                    current_official_event_versions,
                )
                and _calendar_payload_in_declared_window(
                    row,
                    window_start=window_start,
                    window_end=window_end,
                )
            ):
                official_candidates.append(row)
            elif _calendar_payload_before_window(row, window_start=window_start):
                historical.append(row)
            else:
                inactive.append(row)

        for row in inactive:
            row["reaction"] = _reaction_failure_projection(
                row,
                status="UNAVAILABLE",
                reason="REACTION_EVENT_NOT_IN_CURRENT_OFFICIAL_SNAPSHOT",
                event_hash=_calendar_reaction_identity_hash(row),
            )

        def finalize(
            provider_projection: dict[str, object],
        ) -> tuple[list[dict[str, object]], dict[str, object]]:
            provider_projection.update(
                {
                    "event_count": scope.get("event_count", 0),
                    "measure_count": scope.get("measure_count", 0),
                    "capture_spec_count": scope.get("capture_spec_count", 0),
                    "captured_release_vintage_count": scope.get("captured_release_vintage_count", 0),
                    "captured_measure_count": scope.get("captured_measure_count", 0),
                    "capture_eligible_count": scope.get("capture_eligible_count", 0),
                    "surprise_ready_count": scope.get("surprise_ready_count", 0),
                    "progressed_event_count": scope.get("progressed_event_count", 0),
                    "next_eligible_release_at": scope.get(
                        "next_eligible_release_at",
                        "NEXT_ELIGIBLE_RELEASE_UNKNOWN",
                    ),
                    "schedule_refresh_status": scope.get(
                        "schedule_refresh_status",
                        "UNKNOWN",
                    ),
                    "schedule_refresh_reason": scope.get(
                        "schedule_refresh_reason",
                    ),
                    "schedule_hash": scope.get("schedule_hash"),
                    "descriptor_wait_count": scope.get(
                        "descriptor_wait_count",
                        0,
                    ),
                    "family_counts": dict(scope.get("family_counts", {})),
                    "support_matrix": list(scope.get("support_matrix", ())),
                    "worker_health": dict(scope.get("worker_health", {})),
                    "lifecycle_supersessions": dict(
                        scope.get("lifecycle_supersessions", {})
                    ),
                    "next_action": (
                        "WAIT_FOR_DECLARED_RELEASE_TIME"
                        if declared_release_waiting
                        else "WAIT_NEXT_ELIGIBLE_RELEASE"
                        if provider_projection.get("reason")
                        in {
                            "NO_ELIGIBLE_REACTION_EVENTS",
                            "NO_PRE_RELEASE_EXPECTATION",
                        }
                        else "OBSERVE_CURRENT_ELIGIBLE_EVENTS"
                    ),
                }
            )
            _attach_historical_reaction_read_models(
                historical,
                provider=self._reaction_provider,
                now=now,
            )
            return projected, provider_projection

        scope = _reaction_scope_projection(self._reaction_provider)
        scoped_ids = (
            set(scope["eligible_event_ids"])
            if scope.get("scope_known") is True
            else None
        )
        scoped_official = [
            row
            for row in official_candidates
            if scoped_ids is None or str(row.get("event_id") or "") in scoped_ids
        ]
        supported_ids = set(scope.get("supported_event_ids", ()))
        coverage = _reaction_coverage_projection(
            self._reaction_provider,
            tuple(str(row.get("event_id") or "") for row in official_candidates),
        )
        declared_release_waiting = [
            row
            for row in scoped_official
            if _reaction_is_declared_release_wait(
                row,
                coverage.get(str(row.get("event_id") or "")),
                now=now,
            )
        ]
        official = [
            row for row in scoped_official if row not in declared_release_waiting
        ]
        supported_waiting = [
            *declared_release_waiting,
            *[
                row
                for row in official_candidates
                if row not in scoped_official
                and str(row.get("event_id") or "") in supported_ids
            ],
        ]
        unsupported = [
            row
            for row in official_candidates
            if row not in official and row not in supported_waiting
        ]
        for row in supported_waiting:
            event_id = str(row.get("event_id") or "")
            row["reaction"] = _reaction_supported_wait_projection(
                row,
                coverage.get(event_id),
            )
        for row in unsupported:
            row["reaction"] = _reaction_unsupported_projection(row)
        eligible_count = len(official)
        unsupported_count = len(unsupported)
        supported_event_ids = tuple(
            str(item) for item in scope.get("supported_event_ids", ())
        )
        eligible_event_ids = tuple(
            str(row.get("event_id") or "") for row in official
        )
        if (
            self._reaction_provider is not None
            and scope.get("scope_known") is not True
            and not supported_event_ids
        ):
            supported_event_ids = eligible_event_ids
        last_attempt = scope.get("last_attempt")

        if not official:
            return finalize(
                _reaction_provider_projection(
                    status="READY",
                    decision="OBSERVATION_ONLY",
                    reason="NO_ELIGIBLE_REACTION_EVENTS",
                    ledger_count=0,
                    matched_count=0,
                    eligible_count=0,
                    unsupported_count=unsupported_count,
                    supported_event_ids=supported_event_ids,
                    eligible_event_ids=(),
                    last_attempt=last_attempt,
                )
            )

        official_event_ids = tuple(
            str(row.get("event_id") or "") for row in official
        )
        active_roots: Mapping[str, str] = {}
        root_reader = getattr(self._reaction_provider, "reaction_roots", None)
        if callable(root_reader):
            try:
                supplied_roots = root_reader(official_event_ids)
            except Exception:
                supplied_roots = {}
            if isinstance(supplied_roots, Mapping):
                active_roots = supplied_roots
        expected_hashes = {
            event_id: (
                str(active_roots[event_id])
                if event_id in active_roots
                and re.fullmatch(r"[0-9a-f]{64}", str(active_roots[event_id]))
                is not None
                else _calendar_reaction_identity_hash(row)
            )
            for event_id, row in zip(official_event_ids, official)
        }
        stable_to_public = {
            stable_key: public_event_id
            for public_event_id, stable_key in expected_hashes.items()
            if stable_key is not None
        }
        provider_identity_hashes: dict[str, str] = {}
        for row in official:
            public_event_id = str(row.get("event_id") or "")
            stable_key = expected_hashes.get(public_event_id)
            for provider_event_id in (public_event_id, stable_key):
                if provider_event_id is None:
                    continue
                identity_hash = _calendar_provider_identity_hash(
                    row,
                    provider_event_id=provider_event_id,
                )
                if identity_hash is not None:
                    provider_identity_hashes[provider_event_id] = identity_hash
        provider = self._reaction_provider
        if provider is None:
            for row in official:
                row["reaction"] = _reaction_failure_projection(
                    row,
                    status="UNAVAILABLE",
                    reason="REACTION_PROVIDER_UNCONFIGURED",
                    event_hash=expected_hashes.get(str(row.get("event_id") or "")),
                )
            return finalize(
                _reaction_provider_projection(
                    status="UNAVAILABLE",
                    decision="NO_TRADE",
                    reason="REACTION_PROVIDER_UNCONFIGURED",
                    ledger_count=0,
                    matched_count=0,
                    eligible_count=eligible_count,
                    unsupported_count=unsupported_count,
                    supported_event_ids=supported_event_ids,
                    eligible_event_ids=eligible_event_ids,
                    last_attempt=last_attempt,
                )
            )

        try:
            # Only current-window IDs participate in the atomic health batch.
            supplied = provider.reactions(tuple(sorted(expected_hashes)))
            iterator = iter(supplied)
            ledgers: list[EventReactionLedger] = []
            for index, item in enumerate(iterator):
                if index >= 5000:
                    raise ValueError("reaction provider batch exceeds 5000 ledgers")
                if not isinstance(item, EventReactionLedger):
                    raise TypeError("reaction provider returned a non-ledger value")
                ledgers.append(item)
        except Exception:
            for row in official:
                row["reaction"] = _reaction_failure_projection(
                    row,
                    status="UNAVAILABLE",
                    reason="REACTION_PROVIDER_UNAVAILABLE",
                    event_hash=expected_hashes.get(str(row.get("event_id") or "")),
                )
            return finalize(
                _reaction_provider_projection(
                    status="UNAVAILABLE",
                    decision="NO_TRADE",
                    reason="REACTION_PROVIDER_UNAVAILABLE",
                    ledger_count=0,
                    matched_count=0,
                    eligible_count=eligible_count,
                    unsupported_count=unsupported_count,
                    supported_event_ids=supported_event_ids,
                    eligible_event_ids=eligible_event_ids,
                    last_attempt=last_attempt,
                )
            )

        by_event_id: dict[str, EventReactionLedger] = {}
        conflict_reason: str | None = None
        official_ids = set(expected_hashes) | set(stable_to_public)
        relevant_ledgers = [
            ledger for ledger in ledgers if ledger.identity.event_id in official_ids
        ]
        ignored_count = len(ledgers) - len(relevant_ledgers)
        try:
            for ledger in relevant_ledgers:
                ledger.verify_integrity()
                provider_event_id = ledger.identity.event_id
                event_id = stable_to_public.get(provider_event_id, provider_event_id)
                if event_id in by_event_id:
                    conflict_reason = "REACTION_LEDGER_DUPLICATE"
                    break
                if ledger.transitions[-1].recorded_at > now:
                    conflict_reason = "REACTION_LEDGER_FROM_FUTURE"
                    break
                by_event_id[event_id] = ledger
        except Exception:
            conflict_reason = "REACTION_LEDGER_INTEGRITY_FAILED"

        if conflict_reason is None:
            for _event_id, ledger in by_event_id.items():
                expected_hash = provider_identity_hashes.get(
                    ledger.identity.event_id
                )
                if (
                    expected_hash is not None
                    and ledger.identity.content_hash != expected_hash
                ):
                    conflict_reason = "REACTION_LEDGER_EVENT_HASH_MISMATCH"
                    break

        if conflict_reason is not None:
            for row in official:
                row["reaction"] = _reaction_failure_projection(
                    row,
                    status="CONFLICTED",
                    reason=conflict_reason,
                    event_hash=expected_hashes.get(str(row.get("event_id") or "")),
                )
            return finalize(
                _reaction_provider_projection(
                    status="CONFLICTED",
                    decision="NO_TRADE",
                    reason=conflict_reason,
                    ledger_count=len(relevant_ledgers),
                    matched_count=0,
                    ignored_count=ignored_count,
                    eligible_count=eligible_count,
                    unsupported_count=unsupported_count,
                    supported_event_ids=supported_event_ids,
                    eligible_event_ids=eligible_event_ids,
                    last_attempt=last_attempt,
                )
            )

        matched_count = 0
        reaction_decision = "OBSERVATION_ONLY"
        unavailable = False
        for row in official:
            event_id = str(row.get("event_id") or "")
            ledger = by_event_id.get(event_id)
            expected_hash = expected_hashes.get(event_id)
            if ledger is None or expected_hash is None:
                unavailable = True
                row["reaction"] = _reaction_failure_projection(
                    row,
                    status="UNAVAILABLE",
                    reason=(
                        "REACTION_EVENT_IDENTITY_UNAVAILABLE"
                        if expected_hash is None
                        else "REACTION_LEDGER_UNAVAILABLE"
                    ),
                    event_hash=expected_hash,
                )
                continue
            read_model = _reaction_read_model(
                ledger,
                event_id=event_id,
                event_hash=expected_hash,
            )
            row["reaction"] = read_model
            matched_count += 1
            if read_model["decision"] == "NO_TRADE":
                reaction_decision = "NO_TRADE"

        child_conflict = _attach_measure_reaction_read_models(
            official,
            provider=provider,
            expected_hashes=expected_hashes,
            stable_to_public=stable_to_public,
            now=now,
        )
        if child_conflict is not None:
            for row in official:
                row["reaction"] = _reaction_failure_projection(
                    row,
                    status="CONFLICTED",
                    reason=child_conflict,
                    event_hash=expected_hashes.get(
                        str(row.get("event_id") or "")
                    ),
                )
            return finalize(
                _reaction_provider_projection(
                    status="CONFLICTED",
                    decision="NO_TRADE",
                    reason=child_conflict,
                    ledger_count=len(relevant_ledgers),
                    matched_count=0,
                    ignored_count=ignored_count,
                    eligible_count=eligible_count,
                    unsupported_count=unsupported_count,
                    supported_event_ids=supported_event_ids,
                    eligible_event_ids=eligible_event_ids,
                    last_attempt=last_attempt,
                )
            )
        if unavailable:
            reaction_decision = "NO_TRADE"
        return finalize(
            _reaction_provider_projection(
                status="UNAVAILABLE" if unavailable else "READY",
                decision=reaction_decision,
                reason=(
                    "REACTION_LEDGER_COVERAGE_INCOMPLETE" if unavailable else None
                ),
                ledger_count=len(relevant_ledgers),
                matched_count=matched_count,
                ignored_count=ignored_count,
                eligible_count=eligible_count,
                unsupported_count=unsupported_count,
                supported_event_ids=supported_event_ids,
                eligible_event_ids=eligible_event_ids,
                last_attempt=last_attempt,
            )
        )

    def _latest_asof(self) -> str | None:
        values = [
            item.asof for item in (self._news_health, self._calendar_health) if item.asof is not None
        ]
        return None if not values else max(values).isoformat()

    def _ensure_open(self) -> None:
        if self._closed or self._closing:
            raise RuntimeError("news coordinator is closed")


def _calendar_reaction_identity_hash(row: Mapping[str, object]) -> str | None:
    """Recompute the stable reaction key without mutable observation provenance."""

    scheduled_at = _optional_timestamp(row.get("scheduled_at"))
    source = str(row.get("source") or "").strip()
    source_id = str(row.get("source_id") or "").strip()
    title = str(row.get("title") or "").strip()
    category = str(row.get("category") or "").strip()
    precision = str(row.get("schedule_precision") or "").strip().upper()
    if (
        not all((source, source_id, title, category, precision))
        or precision not in {"EXACT", "DATE_ONLY"}
    ):
        return None
    family = classify_event_family(source, title, category)
    parent = (
        None
        if scheduled_at is None
        else parent_identity_from_calendar(
            source=source,
            title=title,
            category=category,
            scheduled_at=scheduled_at,
        )
    )
    meeting_range = (
        None
        if parent is None
        or family not in {EventFamily.FOMC_STATEMENT, EventFamily.FOMC_MINUTES}
        else parent.reference_period
    )
    identity_hash = canonical_hash(
        {
            "schema": "options_copilot.reaction_stable_event_key.v1",
            "publisher": FAMILY_SPECS[family].publisher,
            "family": family.value,
            "source_id": source_id,
            "scheduled_at": None if scheduled_at is None else datetime_text(scheduled_at),
            "schedule_precision": precision,
            "reference_period": None if parent is None else parent.reference_period,
            "estimate_label": None if parent is None else parent.estimate_label,
            "meeting_range": meeting_range,
            "decision_authority": "SUPPORTING_ONLY",
        }
    )
    claimed = row.get("reaction_identity_hash")
    if claimed not in (None, "") and claimed != identity_hash:
        return None
    return identity_hash


def _calendar_provider_identity_hash(
    row: Mapping[str, object],
    *,
    provider_event_id: str,
) -> str | None:
    """Rebuild the provider ledger identity for public or stable aliases."""

    provenance = row.get("reaction_identity_provenance")
    provenance_map = provenance if isinstance(provenance, Mapping) else {}
    scheduled_at = _optional_timestamp(row.get("scheduled_at"))
    first_seen_at = _optional_timestamp(
        provenance_map.get("first_seen_at") or row.get("first_seen_at")
    )
    observed_at = _optional_timestamp(
        provenance_map.get("observed_at") or row.get("observed_at")
    )
    if scheduled_at is None or first_seen_at is None or observed_at is None:
        return None
    try:
        identity = ScheduledEventIdentity(
            event_id=provider_event_id,
            official_source=str(row.get("source") or ""),
            official_source_id=str(row.get("source_id") or ""),
            title=str(row.get("title") or ""),
            category=str(row.get("category") or ""),
            scheduled_at=scheduled_at,
            schedule_published_at=_optional_timestamp(row.get("published_at")),
            schedule_first_seen_at=first_seen_at,
            schedule_observed_at=observed_at,
            symbols=tuple(row.get("symbols", ())),
            reaction_root_hash=(
                provider_event_id
                if re.fullmatch(r"[0-9a-f]{64}", provider_event_id)
                else None
            ),
        )
    except Exception:
        return None
    return identity.content_hash


def _reaction_failure_projection(
    row: Mapping[str, object],
    *,
    status: str,
    reason: str,
    event_hash: str | None = None,
) -> dict[str, object]:
    if status not in {"UNAVAILABLE", "CONFLICTED"}:
        raise ValueError("reaction failure status must be UNAVAILABLE or CONFLICTED")
    return {
        "status": status,
        "current_stage": None,
        "analysis_available": False,
        "event_id": str(row.get("event_id") or ""),
        "event_hash": event_hash,
        "asof": None,
        "head_hash": None,
        "transition_count": 0,
        "expectation": None,
        "release": None,
        "surprise": None,
        "market_reaction": None,
        "option_reevaluation": None,
        "decision": "NO_TRADE",
        "reasons": [reason],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _reaction_unsupported_projection(
    row: Mapping[str, object],
) -> dict[str, object]:
    return {
        "status": "UNAVAILABLE",
        "current_stage": None,
        "analysis_available": False,
        "event_id": str(row.get("event_id") or row.get("id") or ""),
        "event_hash": _calendar_reaction_identity_hash(row),
        "asof": None,
        "head_hash": None,
        "transition_count": 0,
        "expectation": None,
        "release": None,
        "surprise": None,
        "market_reaction": None,
        "option_reevaluation": None,
        "decision": "OBSERVATION_ONLY",
        "reasons": ["REACTION_EVENT_UNSUPPORTED"],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _reaction_supported_wait_projection(
    row: Mapping[str, object],
    coverage: Mapping[str, object] | None,
) -> dict[str, object]:
    state = coverage if isinstance(coverage, Mapping) else {}
    next_action = str(
        state.get("next_action") or "WAIT_NEXT_ELIGIBLE_RELEASE"
    )[:96]
    reason = (
        "WAIT_FOR_DECLARED_RELEASE_TIME"
        if next_action.upper() == "WAIT_FOR_DECLARED_RELEASE_TIME"
        else str(state.get("reason") or "WAITING_NEXT_ELIGIBLE_RELEASE")[:160]
    )
    return {
        "status": "UNAVAILABLE",
        "current_stage": None,
        "analysis_available": False,
        "event_id": str(row.get("event_id") or row.get("id") or ""),
        "event_hash": _calendar_reaction_identity_hash(row),
        "asof": None,
        "head_hash": None,
        "transition_count": 0,
        "expectation": None,
        "release": None,
        "surprise": None,
        "market_reaction": (
            dict(state["document_market_reaction"])
            if isinstance(state.get("document_market_reaction"), Mapping)
            else None
        ),
        "option_reevaluation": None,
        "document_progression": {
            "status": str(state.get("document_stage") or "SCHEDULED")[:48],
            "capture_count": _reaction_nonnegative_integer(
                state.get("capture_count")
            )
            or 0,
            "next_action": next_action,
            "decision_authority": "SUPPORTING_ONLY",
        },
        "numeric_surprise": {
            "status": "UNAVAILABLE",
            "reason": "NUMERIC_SURPRISE_UNSUPPORTED",
            "decision_authority": "SUPPORTING_ONLY",
        },
        "coverage": {
            "family": str(state.get("family") or "UNKNOWN")[:64],
            "support_state": str(state.get("support_state") or "SUPPORTED")[:32],
            "supported": state.get("supported") is True,
            "capture_eligible": state.get("capture_eligible") is True,
            "surprise_eligible": state.get("surprise_eligible") is True,
            "progressed": state.get("progressed") is True,
            "next_action": next_action,
            "measure_count": _reaction_nonnegative_integer(state.get("measure_count")) or 0,
            "capture_count": _reaction_nonnegative_integer(state.get("capture_count")) or 0,
            "document_stage": str(state.get("document_stage") or "SCHEDULED")[:48],
            "next_eligible_release_at": str(
                state.get("next_eligible_release_at")
                or "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
            )[:64],
        },
        "decision": "OBSERVATION_ONLY",
        "reasons": [reason],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _reaction_is_declared_release_wait(
    row: Mapping[str, object],
    coverage: Mapping[str, object] | None,
    *,
    now: datetime,
) -> bool:
    """Recognize a supported future release before any ledger should exist."""

    if not isinstance(coverage, Mapping):
        return False
    scheduled_at = _optional_timestamp(row.get("scheduled_at"))
    coverage_scheduled_at = _optional_timestamp(coverage.get("scheduled_at"))
    next_release_at = _optional_timestamp(
        coverage.get("next_eligible_release_at")
    )
    event_id = str(row.get("event_id") or "")
    event_hash = _calendar_reaction_identity_hash(row)
    raw_measure_reactions = coverage.get("measure_reactions")
    measure_reactions_empty = (
        isinstance(raw_measure_reactions, Sequence)
        and not isinstance(
            raw_measure_reactions,
            (str, bytes, bytearray, memoryview),
        )
        and not raw_measure_reactions
    )
    return (
        scheduled_at is not None
        and scheduled_at > now
        and coverage_scheduled_at == scheduled_at
        and next_release_at == scheduled_at
        and bool(event_id)
        and coverage.get("event_id") == event_id
        and event_hash is not None
        and coverage.get("event_hash") == event_hash
        and str(coverage.get("support_state") or "").upper() == "SUPPORTED"
        and coverage.get("supported") is True
        and coverage.get("capture_eligible") is False
        and coverage.get("surprise_eligible") is False
        and coverage.get("progressed") is False
        and coverage.get("capture_spec_available") is True
        and coverage.get("capture_count") == 0
        and str(coverage.get("document_stage") or "").upper() == "SCHEDULED"
        and str(coverage.get("next_action") or "").upper()
        == "WAIT_FOR_DECLARED_RELEASE_TIME"
        and str(coverage.get("reason") or "").upper()
        in {
            "WAIT_FOR_DECLARED_RELEASE_TIME",
            "WAITING_DECLARED_RELEASE_TIME",
        }
        and coverage.get("document_market_reaction") is None
        and measure_reactions_empty
        and coverage.get("decision_authority") == "SUPPORTING_ONLY"
        and coverage.get("approval_eligible") is False
        and coverage.get("instruction_creation_allowed") is False
        and coverage.get("order_creation_allowed") is False
    )


def _reaction_coverage_projection(
    provider: object | None,
    event_ids: tuple[str, ...],
) -> Mapping[str, Mapping[str, object]]:
    reader = getattr(provider, "coverage", None)
    if not callable(reader) or not event_ids:
        return {}
    try:
        raw = reader(event_ids)
    except Exception:
        return {}
    if not isinstance(raw, Mapping):
        return {}
    return {
        event_id: value
        for event_id, value in raw.items()
        if isinstance(event_id, str)
        and event_id in event_ids
        and isinstance(value, Mapping)
    }


def _reaction_scope_projection(provider: object | None) -> dict[str, object]:
    reader = getattr(provider, "projection", None)
    if not callable(reader):
        return {
            "supported_event_ids": (),
            "eligible_event_ids": (),
            "last_attempt": None,
            "scope_known": False,
        }
    try:
        raw = reader()
    except Exception:
        return {
            "supported_event_ids": (),
            "eligible_event_ids": (),
            "last_attempt": None,
            "scope_known": False,
        }
    if not isinstance(raw, Mapping):
        return {
            "supported_event_ids": (),
            "eligible_event_ids": (),
            "last_attempt": None,
            "scope_known": False,
        }
    supported = _reaction_event_ids(raw.get("supported_event_ids"))
    eligible = _reaction_event_ids(raw.get("eligible_event_ids"))
    last_attempt = _timestamp_text(raw.get("last_attempt"))
    event_count = _reaction_nonnegative_integer(raw.get("event_count")) or 0
    measure_count = _reaction_nonnegative_integer(raw.get("measure_count")) or 0
    count_fields = {
        name: _reaction_nonnegative_integer(raw.get(name)) or 0
        for name in (
            "capture_spec_count",
            "captured_release_vintage_count",
            "captured_measure_count",
            "capture_eligible_count",
            "surprise_ready_count",
            "progressed_event_count",
        )
    }
    next_release = str(
        raw.get("next_eligible_release_at") or "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
    )[:64]
    if next_release != "NEXT_ELIGIBLE_RELEASE_UNKNOWN":
        try:
            next_release = utc_datetime(
                datetime.fromisoformat(next_release),
                field="next_eligible_release_at",
            ).isoformat()
        except Exception:
            next_release = "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
    family_counts = {
        str(key)[:64]: count
        for key, value in (
            raw.get("family_counts", {}).items()
            if isinstance(raw.get("family_counts"), Mapping)
            else ()
        )
        if (count := _reaction_nonnegative_integer(value)) is not None
    }
    support_matrix = tuple(
        {
            "family": str(item.get("family") or "")[:64],
            "support_state": str(item.get("support_state") or "")[:32],
            "measure_count": _reaction_nonnegative_integer(item.get("measure_count")) or 0,
            "surprise_supported": item.get("surprise_supported") is True,
            "reason": str(item.get("reason") or "")[:160] or None,
        }
        for item in tuple(raw.get("support_matrix", ()))[:32]
        if isinstance(item, Mapping)
        and str(item.get("support_state") or "")
        in {"SUPPORTED", "PREFLIGHT_ONLY", "UNSUPPORTED"}
    )
    schedule_refresh_status = str(
        raw.get("schedule_refresh_status") or "UNKNOWN"
    ).upper()
    if schedule_refresh_status not in {
        "READY",
        "DEGRADED",
        "UNAVAILABLE",
        "UNKNOWN",
    }:
        schedule_refresh_status = "UNKNOWN"
    schedule_refresh_reason = str(
        raw.get("schedule_refresh_reason") or ""
    )[:160] or None
    schedule_hash = raw.get("schedule_hash")
    if not isinstance(schedule_hash, str) or re.fullmatch(
        r"[0-9a-f]{64}", schedule_hash
    ) is None:
        schedule_hash = None
    descriptor_wait_count = (
        _reaction_nonnegative_integer(raw.get("descriptor_wait_count")) or 0
    )
    worker_health = {
        lane: {
            "status": "DEGRADED",
            "reason": str(item.get("reason") or "WORKER_FAILURE")[:160],
            "attempted_at": _timestamp_text(item.get("attempted_at")),
            "durable": item.get("durable") is True,
        }
        for lane, item in (
            raw.get("worker_health", {}).items()
            if isinstance(raw.get("worker_health"), Mapping)
            else ()
        )
        if lane in {"schedule", "capture", "observer"}
        and isinstance(item, Mapping)
    }
    lifecycle_supersessions = {
        current: superseded
        for current, superseded in (
            raw.get("lifecycle_supersessions", {}).items()
            if isinstance(raw.get("lifecycle_supersessions"), Mapping)
            else ()
        )
        if isinstance(current, str)
        and isinstance(superseded, str)
        and re.fullmatch(r"[0-9a-f]{64}", current) is not None
        and re.fullmatch(r"[0-9a-f]{64}", superseded) is not None
    }
    return {
        "supported_event_ids": supported,
        "eligible_event_ids": eligible,
        "last_attempt": last_attempt,
        "scope_known": raw.get("scope_known") is True,
        "event_count": event_count,
        "measure_count": measure_count,
        **count_fields,
        "next_eligible_release_at": next_release,
        "family_counts": family_counts,
        "support_matrix": support_matrix,
        "schedule_refresh_status": schedule_refresh_status,
        "schedule_refresh_reason": schedule_refresh_reason,
        "schedule_hash": schedule_hash,
        "descriptor_wait_count": descriptor_wait_count,
        "worker_health": worker_health,
        "lifecycle_supersessions": lifecycle_supersessions,
    }


def _reaction_nonnegative_integer(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _reaction_event_ids(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(
        dict.fromkeys(
            str(item).strip()
            for item in tuple(value)[:500]
            if isinstance(item, str) and str(item).strip()
        )
    )


def _reaction_provider_projection(
    *,
    status: str,
    decision: str,
    reason: str | None,
    ledger_count: int,
    matched_count: int,
    ignored_count: int = 0,
    eligible_count: int = 0,
    unsupported_count: int = 0,
    supported_event_ids: Sequence[str] = (),
    eligible_event_ids: Sequence[str] = (),
    last_attempt: object = None,
) -> dict[str, object]:
    return {
        "name": "event-reaction-provider",
        "status": status,
        "decision": decision,
        "reason": reason,
        "ledger_count": ledger_count,
        "matched_count": matched_count,
        "ignored_count": ignored_count,
        "supported_event_ids": list(supported_event_ids),
        "supported_count": len(tuple(supported_event_ids)),
        "eligible_event_ids": list(eligible_event_ids),
        "eligible_count": eligible_count,
        "unsupported_count": unsupported_count,
        "last_attempt": last_attempt,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _reaction_read_model(
    ledger: EventReactionLedger,
    *,
    event_id: str | None = None,
    event_hash: str | None = None,
    reaction_id: str | None = None,
) -> dict[str, object]:
    """Return a compact pointer-rich view; never expose authority-like results."""

    stage = ledger.current_stage
    terminal = stage in {
        ReactionStage.DEGRADED,
        ReactionStage.CONFLICTED,
        ReactionStage.NO_TRADE,
    }
    release = ledger.release_chain[-1] if ledger.release_chain else None
    surprise = ledger.surprise
    market = ledger.market_reaction
    option = ledger.option_reevaluation
    tail = ledger.transitions[-1]
    result = {
        "status": stage.value if terminal else "READY",
        "current_stage": stage.value,
        "analysis_available": stage
        in {
            ReactionStage.SURPRISE_ASSESSED,
            ReactionStage.MARKET_REACTION_OBSERVED,
            ReactionStage.OPTION_REEVALUATED,
        },
        "event_id": ledger.identity.event_id if event_id is None else event_id,
        "event_hash": ledger.identity.event_hash if event_hash is None else event_hash,
        "scheduled_at": ledger.identity.scheduled_at.isoformat(),
        "asof": tail.recorded_at.isoformat(),
        "head_hash": ledger.head_hash,
        "transition_count": len(ledger.transitions),
        "expectation": {
            "content_hash": ledger.expectation.content_hash,
            "metric": ledger.expectation.metric,
            "expected_value": str(ledger.expectation.expected_value),
            "unit": ledger.expectation.unit,
            "provider": ledger.expectation.provider,
            "source_id": ledger.expectation.source_id,
            "published_at": ledger.expectation.published_at.isoformat(),
            "first_seen_at": ledger.expectation.first_seen_at.isoformat(),
            "observed_at": ledger.expectation.observed_at.isoformat(),
            "vintage": ledger.expectation.vintage,
            "decision_authority": "SUPPORTING_ONLY",
        },
        "release": (
            None
            if release is None
            else {
                "content_hash": release.content_hash,
                "actual_value": (
                    None if release.actual_value is None else str(release.actual_value)
                ),
                "unit": release.unit,
                "official_source": release.official_source,
                "source_id": release.source_id,
                "released_at": release.released_at.isoformat(),
                "vintage_at": release.vintage_at.isoformat(),
                "captured_at": release.captured_at.isoformat(),
                "revision": release.revision,
                "supersedes_hash": release.supersedes_hash,
                "release_chain_hashes": [
                    item.content_hash for item in ledger.release_chain
                ],
                "decision_authority": "SUPPORTING_ONLY",
            }
        ),
        "surprise": (
            None
            if surprise is None
            else {
                "content_hash": surprise.content_hash,
                "expectation_hash": surprise.expectation_hash,
                "release_hash": surprise.release_hash,
                "delta": str(surprise.delta),
                "relative_delta": (
                    None
                    if surprise.relative_delta is None
                    else str(surprise.relative_delta)
                ),
                "assessed_at": surprise.assessed_at.isoformat(),
                "supporting_evidence_hashes": list(
                    surprise.supporting_evidence_hashes
                ),
                "decision_authority": "SUPPORTING_ONLY",
            }
        ),
        "market_reaction": (
            None
            if market is None
            else {
                "content_hash": market.content_hash,
                "release_hash": market.release_hash,
                "source": market.source,
                "window_start": market.window_start.isoformat(),
                "window_end": market.window_end.isoformat(),
                "evidence_asof": market.evidence_asof.isoformat(),
                "observed_at": market.observed_at.isoformat(),
                "decision_authority": "SUPPORTING_ONLY",
            }
        ),
        "option_reevaluation": (
            None
            if option is None
            else {
                "content_hash": option.content_hash,
                "market_reaction_hash": option.market_reaction_hash,
                "option_id": option.option_id,
                "candidate_hash": option.candidate_hash,
                "source": option.source,
                "evidence_asof": option.evidence_asof.isoformat(),
                "observed_at": option.observed_at.isoformat(),
                "input_evidence_hashes": list(option.input_evidence_hashes),
                "decision_authority": "SUPPORTING_ONLY",
            }
        ),
        "decision": ledger.decision,
        "reasons": [reason.value for reason in tail.reasons],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }
    if reaction_id is not None:
        result["reaction_id"] = reaction_id
        result["ledger_event_id"] = ledger.identity.event_id
        result["ledger_event_hash"] = ledger.identity.content_hash
    return result


def _attach_measure_reaction_read_models(
    rows: Sequence[dict[str, object]],
    *,
    provider: object,
    expected_hashes: Mapping[str, str | None],
    stable_to_public: Mapping[str, str],
    now: datetime,
) -> str | None:
    """Attach independently verified child ledgers for multi-measure releases."""

    reader = getattr(provider, "child_reactions", None)
    if not callable(reader):
        return None
    requested = tuple(
        sorted(str(row.get("event_id") or "") for row in rows)
    )
    try:
        supplied = reader(requested)
    except Exception:
        return "REACTION_PROVIDER_UNAVAILABLE"
    if not isinstance(supplied, Mapping):
        return "REACTION_API_READ_MODEL_INVALID"
    row_by_public_id = {
        str(row.get("event_id") or ""): row for row in rows
    }
    seen_reaction_ids: set[str] = set()
    for supplied_parent, raw_children in supplied.items():
        public_event_id = stable_to_public.get(
            str(supplied_parent),
            str(supplied_parent),
        )
        row = row_by_public_id.get(public_event_id)
        stable_key = expected_hashes.get(public_event_id)
        if row is None or stable_key is None:
            return "REACTION_API_BINDING_MISMATCH"
        if not isinstance(raw_children, Sequence) or isinstance(
            raw_children,
            (str, bytes, bytearray),
        ):
            return "REACTION_API_READ_MODEL_INVALID"
        children: list[dict[str, object]] = []
        for child in tuple(raw_children)[:128]:
            if not isinstance(child, EventReactionLedger):
                return "REACTION_API_READ_MODEL_INVALID"
            try:
                child.verify_integrity()
            except Exception:
                return "REACTION_LEDGER_INTEGRITY_FAILED"
            if child.transitions[-1].recorded_at > now:
                return "REACTION_LEDGER_FROM_FUTURE"
            reaction_id = child.identity.event_id
            if (
                reaction_id in seen_reaction_ids
                or not reaction_id.startswith(f"{stable_key}:")
            ):
                return (
                    "REACTION_LEDGER_DUPLICATE"
                    if reaction_id in seen_reaction_ids
                    else "REACTION_API_BINDING_MISMATCH"
                )
            seen_reaction_ids.add(reaction_id)
            children.append(
                _reaction_read_model(
                    child,
                    reaction_id=reaction_id,
                )
            )
        row["measure_reactions"] = children
    revision_reader = getattr(provider, "revision_views", None)
    if callable(revision_reader):
        try:
            revision_views = revision_reader(requested)
        except Exception:
            return "REACTION_PROVIDER_UNAVAILABLE"
        if not isinstance(revision_views, Mapping):
            return "REACTION_API_READ_MODEL_INVALID"
        for supplied_parent, raw_views in revision_views.items():
            public_event_id = stable_to_public.get(
                str(supplied_parent),
                str(supplied_parent),
            )
            row = row_by_public_id.get(public_event_id)
            if row is None or not isinstance(raw_views, Sequence):
                return "REACTION_API_BINDING_MISMATCH"
            row["revision_views"] = [
                dict(item)
                for item in tuple(raw_views)[:128]
                if isinstance(item, Mapping)
            ]
    return None


def _attach_historical_reaction_read_models(
    rows: Sequence[dict[str, object]],
    *,
    provider: EventReactionProvider | None,
    now: datetime,
) -> None:
    """Attach a bounded historical projection without changing current health."""

    for row in rows:
        row["reaction"] = _reaction_failure_projection(
            row,
            status="UNAVAILABLE",
            reason="REACTION_LEDGER_UNAVAILABLE",
            event_hash=_calendar_reaction_identity_hash(row),
        )
    if provider is None or not rows:
        return
    selected = sorted(rows, key=_calendar_sort_key, reverse=True)[
        :_MAX_HISTORICAL_REACTION_EVENTS
    ]
    expected_hashes: dict[str, str] = {}
    stable_to_public: dict[str, str] = {}
    provider_identity_hashes: dict[str, str] = {}
    row_by_event_id: dict[str, dict[str, object]] = {}
    for row in selected:
        event_id = str(row.get("event_id") or "")
        expected_hash = _calendar_reaction_identity_hash(row)
        if not event_id or expected_hash is None or event_id in expected_hashes:
            return
        expected_hashes[event_id] = expected_hash
        stable_to_public[expected_hash] = event_id
        for provider_event_id in (event_id, expected_hash):
            identity_hash = _calendar_provider_identity_hash(
                row,
                provider_event_id=provider_event_id,
            )
            if identity_hash is not None:
                provider_identity_hashes[provider_event_id] = identity_hash
        row_by_event_id[event_id] = row
    if not expected_hashes:
        return
    try:
        supplied = provider.reactions(tuple(sorted(expected_hashes)))
        relevant: list[EventReactionLedger] = []
        for index, item in enumerate(iter(supplied)):
            if index >= _MAX_HISTORICAL_REACTION_EVENTS:
                raise ValueError("historical reaction batch exceeds limit")
            if not isinstance(item, EventReactionLedger):
                raise TypeError("historical reaction provider returned non-ledger")
            if item.identity.event_id in set(expected_hashes) | set(stable_to_public):
                relevant.append(item)
        by_event_id: dict[str, EventReactionLedger] = {}
        for ledger in relevant:
            ledger.verify_integrity()
            provider_event_id = ledger.identity.event_id
            event_id = stable_to_public.get(provider_event_id, provider_event_id)
            if event_id in by_event_id:
                raise ValueError("duplicate historical reaction ledger")
            if ledger.transitions[-1].recorded_at > now:
                raise ValueError("future historical reaction ledger")
            if ledger.identity.content_hash != provider_identity_hashes.get(
                provider_event_id
            ):
                raise ValueError("misbound historical reaction ledger")
            by_event_id[event_id] = ledger
    except Exception:
        return
    for event_id, ledger in by_event_id.items():
        read_model = _reaction_read_model(
            ledger,
            event_id=event_id,
            event_hash=expected_hashes[event_id],
        )
        if read_model.get("analysis_available") is True:
            row_by_event_id[event_id]["reaction"] = read_model


def _official_event_in_snapshot(
    event: object,
    snapshot: OfficialCalendarSnapshot,
) -> bool:
    if not isinstance(event, OfficialCalendarEvent):
        return False
    if event.observed_at != snapshot.observed_at:
        return False
    if event.scheduled_at is not None:
        return snapshot.window_start <= event.scheduled_at < snapshot.window_end
    if event.event_date is None or not event.timezone_name:
        return False
    try:
        zone = ZoneInfo(event.timezone_name)
    except Exception:
        return False
    start_date = snapshot.window_start.astimezone(zone).date()
    return start_date <= event.event_date < start_date + timedelta(days=14)


def _restored_official_calendar_event(
    stored: StoredEvidence,
) -> OfficialCalendarEvent | None:
    """Rebuild one validated event from its immutable stored document."""

    payload = stored.record.payload
    raw_provenance = payload.get("provenance")
    if not isinstance(raw_provenance, Sequence) or isinstance(
        raw_provenance,
        (str, bytes, bytearray),
    ):
        return None
    provenance: list[OfficialEventProvenance] = []
    try:
        for raw in raw_provenance:
            if not isinstance(raw, Mapping):
                return None
            provenance.append(
                OfficialEventProvenance(
                    source=str(raw.get("source") or ""),
                    source_url=str(raw.get("source_url") or ""),
                    source_id=str(raw.get("source_id") or ""),
                    source_payload_hash=str(raw.get("source_payload_hash") or ""),
                    published_at=_optional_timestamp(raw.get("published_at")),
                    first_seen_at=_required_timestamp(raw.get("first_seen_at")),
                    ingested_at=_required_timestamp(raw.get("ingested_at")),
                    observed_at=_required_timestamp(raw.get("observed_at")),
                    provenance_hash=str(raw.get("provenance_hash") or "") or None,
                )
            )
        event_date = _optional_date(payload.get("event_date"))
        symbols = _stored_symbols(payload.get("symbols"))
        return OfficialCalendarEvent(
            event_id=str(payload.get("event_id") or payload.get("id") or ""),
            source=str(payload.get("source") or ""),
            source_id=str(payload.get("source_id") or ""),
            source_url=str(payload.get("source_url") or ""),
            title=str(payload.get("title") or ""),
            category=str(payload.get("category") or ""),
            scheduled_at=_optional_timestamp(
                payload.get("scheduled_at", payload.get("event_at"))
            ),
            event_date=event_date,
            timezone_name=str(payload.get("timezone") or "") or None,
            schedule_precision=str(payload.get("schedule_precision") or ""),
            symbols=tuple(symbols),
            url=str(payload.get("url") or ""),
            published_at=_optional_timestamp(payload.get("published_at")),
            first_seen_at=_required_timestamp(payload.get("first_seen_at")),
            ingested_at=_required_timestamp(payload.get("ingested_at")),
            observed_at=_required_timestamp(payload.get("observed_at")),
            provenance=tuple(provenance),
            content_hash=str(payload.get("content_hash") or "") or None,
            record_hash=str(payload.get("record_hash") or "") or None,
            status=str(payload.get("status") or "ACTIVE"),
        )
    except (TypeError, ValueError):
        return None


def _required_timestamp(value: object) -> datetime:
    parsed = _optional_timestamp(value)
    if parsed is None:
        raise ValueError("stored official timestamp is missing")
    return parsed


def _optional_timestamp(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return _aware(parsed)
    except (TypeError, ValueError):
        return None


def _optional_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    try:
        return value if isinstance(value, date) and not isinstance(value, datetime) else date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _timestamp_text(value: object) -> str | None:
    parsed = _optional_timestamp(value)
    return None if parsed is None else parsed.isoformat()


def _stored_symbols(value: object) -> list[str]:
    if isinstance(value, str):
        values: Sequence[str] = (value,)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        values = tuple(str(item) for item in value)
    else:
        return []
    try:
        return list(_symbols(values))
    except (TypeError, ValueError):
        return []


def _structured_calendar_provenance(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        source = str(item.get("source") or "").strip()
        source_id = str(item.get("source_id") or "").strip()
        source_url = str(item.get("source_url") or "").strip()
        source_payload_hash = str(item.get("source_payload_hash") or "").strip()
        provenance_hash = str(item.get("provenance_hash") or "").strip()
        if not all((source, source_id, source_url, source_payload_hash, provenance_hash)):
            continue
        rows.append(
            {
                "source": source,
                "source_url": source_url,
                "source_id": source_id,
                "source_payload_hash": source_payload_hash,
                "published_at": _timestamp_text(item.get("published_at")),
                "first_seen_at": _timestamp_text(item.get("first_seen_at")),
                "ingested_at": _timestamp_text(item.get("ingested_at")),
                "observed_at": _timestamp_text(item.get("observed_at")),
                "provenance_hash": provenance_hash,
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
    return rows


def _legacy_calendar_provenance(
    evidence: Sequence[StoredEvidence],
) -> list[dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for item in evidence:
        payload = item.record.payload
        raw = payload.get("provenance")
        names: list[str] = []
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            names.extend(str(name).strip() for name in raw if str(name).strip())
        source = str(payload.get("source") or item.record.provider).strip()
        if source:
            names.append(source)
        for name in names:
            rows.setdefault(
                name,
                {
                    "source": name,
                    "source_id": str(payload.get("source_id") or payload.get("event_id") or ""),
                    "published_at": item.record.published_at.isoformat(),
                    "first_seen_at": item.record.first_seen_at.isoformat(),
                    "ingested_at": item.record.ingested_at.isoformat(),
                    "observed_at": item.record.observed_at.isoformat(),
                    "content_hash": item.content_hash,
                    "decision_authority": "SUPPORTING_ONLY",
                },
            )
    return list(rows.values())


def _calendar_window_memberships(
    *,
    event_at: datetime | None,
    event_date: date,
    timezone_name: str,
    now: datetime,
) -> list[str]:
    checked_now = _aware(now)
    try:
        zone = ZoneInfo(timezone_name)
    except Exception:
        zone = _EASTERN
    market_now = checked_now.astimezone(_EASTERN)
    if event_at is not None:
        market_event_date = event_at.astimezone(_EASTERN).date()
    else:
        # Date-only official events retain their declared date and timezone;
        # no midnight or market-open timestamp is fabricated for filtering.
        market_event_date = event_date
        if zone != _EASTERN:
            market_event_date = event_date
    week_start = market_now.date() - timedelta(days=market_now.weekday())
    memberships: list[str] = []
    if week_start <= market_event_date < week_start + timedelta(days=7):
        memberships.append("THIS_WEEK")
    if week_start + timedelta(days=7) <= market_event_date < week_start + timedelta(days=14):
        memberships.append("NEXT_WEEK")
    if event_at is not None:
        if checked_now <= event_at < checked_now + timedelta(days=14):
            memberships.append("FUTURE_TWO_WEEKS")
    else:
        start_date = checked_now.astimezone(zone).date()
        if start_date <= event_date < start_date + timedelta(days=14):
            memberships.append("FUTURE_TWO_WEEKS")
    return memberships


def _calendar_provider_date_windows(
    start: date,
    end: date,
) -> tuple[tuple[date, date], ...]:
    """Split inclusive provider ranges without weakening the exact API window."""

    if end < start:
        raise ValueError("calendar provider window end cannot precede start")
    ranges: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        range_end = min(cursor + timedelta(days=14), end)
        ranges.append((cursor, range_end))
        cursor = range_end + timedelta(days=1)
    return tuple(ranges)


def _calendar_payload_in_declared_window(
    payload: Mapping[str, object],
    *,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    start = _aware(window_start)
    end = _aware(window_end)
    if end <= start:
        return False
    event_at = _optional_timestamp(
        payload.get("scheduled_at", payload.get("event_at"))
    )
    if event_at is not None:
        return start <= event_at < end
    event_date = _optional_date(payload.get("event_date"))
    if event_date is None:
        return False
    try:
        zone = ZoneInfo(str(payload.get("timezone") or "UTC").strip())
        day_start = datetime.combine(event_date, time.min, tzinfo=zone)
        day_end = datetime.combine(
            event_date + timedelta(days=1),
            time.min,
            tzinfo=zone,
        )
    except (OverflowError, ValueError, ZoneInfoNotFoundError):
        return False
    return day_end > start and day_start < end


def _official_snapshot_event_versions(
    snapshot: OfficialCalendarSnapshot,
) -> dict[str, tuple[str, str]] | None:
    """Return exact event versions from one successful official snapshot.

    The durable evidence store intentionally retains prior schedules.  Current
    reaction health must therefore be scoped by both event identity and the
    exact semantic/record hashes present in the active snapshot, not merely by
    whether an older row still falls inside today's two-week date window.
    """

    ready_sources = {
        (item.source, item.source_url)
        for item in snapshot.sources
        if item.status == "READY"
    }
    if snapshot.status != "READY" and not ready_sources:
        return None

    versions: dict[str, tuple[str, str]] = {}
    for event in snapshot.events:
        if (event.source, event.source_url) not in ready_sources:
            continue
        event_id = str(event.event_id)
        version = (str(event.content_hash), str(event.record_hash))
        if event_id in versions:
            # Duplicate identities make the complete current binding
            # ambiguous, even when the duplicated records happen to match.
            return None
        versions[event_id] = version
    return versions


def _calendar_payload_matches_snapshot_version(
    payload: Mapping[str, object],
    event_versions: Mapping[str, tuple[str, str]],
) -> bool:
    event_id = str(payload.get("event_id") or "").strip()
    if not event_id:
        return False
    return event_versions.get(event_id) == (
        str(payload.get("content_hash") or ""),
        str(payload.get("record_hash") or ""),
    )


def _calendar_payload_before_window(
    payload: Mapping[str, object],
    *,
    window_start: datetime,
) -> bool:
    start = _aware(window_start)
    event_at = _optional_timestamp(
        payload.get("scheduled_at", payload.get("event_at"))
    )
    if event_at is not None:
        return event_at < start
    event_date = _optional_date(payload.get("event_date"))
    if event_date is None:
        return False
    try:
        zone = ZoneInfo(str(payload.get("timezone") or "UTC").strip())
        day_end = datetime.combine(
            event_date + timedelta(days=1),
            time.min,
            tzinfo=zone,
        )
    except (OverflowError, ValueError, ZoneInfoNotFoundError):
        return False
    return day_end <= start


def _has_completed_historical_reaction(payload: Mapping[str, object]) -> bool:
    reaction = payload.get("reaction")
    return (
        isinstance(reaction, Mapping)
        and reaction.get("analysis_available") is True
        and reaction.get("decision_authority") == "SUPPORTING_ONLY"
        and reaction.get("approval_eligible") is False
        and reaction.get("instruction_creation_allowed") is False
        and reaction.get("order_creation_allowed") is False
    )


def _calendar_importance(*, source: str, title: str, category: str) -> str:
    """Rank named market-moving families without promoting every macro row."""

    normalized_category = category.strip().upper()
    family = classify_event_family(source, title, normalized_category)
    if normalized_category == "FOMC" or family in {
        EventFamily.FOMC_STATEMENT,
        EventFamily.FOMC_MINUTES,
        EventFamily.FOMC_PRESS_CONFERENCE,
    }:
        return "CRITICAL"
    if family in {
        EventFamily.CPI,
        EventFamily.PPI,
        EventFamily.EMPLOYMENT_SITUATION,
        EventFamily.PCE,
        EventFamily.GDP,
        EventFamily.RETAIL_SALES,
        EventFamily.JOBLESS_CLAIMS,
        EventFamily.ISM,
        EventFamily.EARNINGS_GUIDANCE,
    }:
        return "HIGH"
    return "MEDIUM"


def _calendar_sort_key(item: Mapping[str, object]) -> tuple[str, str, str]:
    event_date = str(item.get("event_date") or "9999-12-31")
    event_at = str(item.get("event_at") or "9999-12-31T23:59:59+00:00")
    return event_date, event_at, str(item.get("id") or "")


def _health_after_refresh(
    *,
    configured: bool,
    failed: bool,
    latency_ms: float | None,
    asof: datetime,
    noun: str,
    cached: bool = False,
) -> _RefreshHealth:
    if not configured:
        return _RefreshHealth("UNCONFIGURED", None, None, f"no {noun} are configured")
    if failed:
        return _RefreshHealth(
            "DEGRADED",
            latency_ms,
            asof,
            (
                "one or more providers remain degraded; retry is deferred"
                if cached
                else "one or more providers are degraded"
            ),
        )
    return _RefreshHealth(
        "READY",
        latency_ms,
        asof,
        "official calendar cache remains current" if cached else f"{noun} refreshed",
    )


def _coordinator_health_from_cadence(
    cadence: SourceCadenceStore,
    source_kind: str,
    *,
    now: datetime,
    configured: bool,
    noun: str,
    include_official: bool = False,
) -> _RefreshHealth:
    """Summarize deferred lanes from their last success, never dispatcher time."""

    if not configured:
        return _RefreshHealth("UNCONFIGURED", None, None, f"no {noun} are configured")
    allowed_kinds = {source_kind}
    if include_official:
        allowed_kinds.add("OFFICIAL_CALENDAR")
    rows = [
        row
        for row in cadence.projections(now=now)
        if row.get("source_kind") in allowed_kinds
        and row.get("configured") is True
    ]
    successes = [
        timestamp
        for row in rows
        if (timestamp := _optional_timestamp(row.get("last_success"))) is not None
    ]
    healthy = bool(rows) and all(
        row.get("freshness") == "CURRENT" and row.get("failure_code") is None
        for row in rows
    )
    return _RefreshHealth(
        "READY" if healthy else "DEGRADED",
        None,
        max(successes) if successes else None,
        "source cadence deferred; freshness is unchanged",
    )


def _provider_is_ready(provider: object) -> bool:
    status = str(getattr(provider, "health", "READY") or "READY").strip().upper()
    return status in _READY_STATES


def _remaining_poll_wait_seconds(
    cycle_started: float,
    interval_seconds: float,
    *,
    clock: Callable[[], float] = monotonic_time.perf_counter,
) -> float:
    """Keep provider-cycle starts on cadence instead of adding work latency."""

    elapsed = max(0.0, float(clock()) - float(cycle_started))
    return max(0.0, float(interval_seconds) - elapsed)


def _provider_explicitly_configured(provider: object) -> bool:
    """Suppress only providers that explicitly attest they are unconfigured."""

    reader = getattr(provider, "health_snapshot", None)
    if not callable(reader):
        return True
    try:
        snapshot = reader()
    except Exception:
        return True
    return not (
        isinstance(snapshot, Mapping)
        and snapshot.get("configured") is False
    )


def _provider_source_health(
    provider: object,
    *,
    source_kind: str,
    success_count: int,
    failure_date_count: int,
    attempted_at: datetime,
    forced_reason: str | None,
) -> dict[str, object]:
    """Project one provider observation without retaining arbitrary text."""

    snapshot: Mapping[object, object] = {}
    reader = getattr(provider, "health_snapshot", None)
    if callable(reader):
        try:
            raw_snapshot = reader()
        except Exception:
            raw_snapshot = None
        if isinstance(raw_snapshot, Mapping):
            snapshot = raw_snapshot
    source = _provider_source_name(provider, snapshot, source_kind=source_kind)
    configured = snapshot.get("configured")
    raw_status = snapshot.get("status", getattr(provider, "health", "DEGRADED"))
    normalized_status = str(raw_status or "").strip().upper()
    unconfigured_status = (
        "UNCONFIGURED" if source == "COMPANY_IR" else "NOT_CONFIGURED"
    )
    ready = (
        configured is not False
        and forced_reason is None
        and failure_date_count == 0
        and normalized_status in _READY_STATES
    )
    raw_reason = (
        forced_reason
        if forced_reason is not None
        else snapshot.get("reason", getattr(provider, "health_reason", None))
    )
    reason = (
        unconfigured_status
        if configured is False
        else None if ready else _source_reason(raw_reason)
    )
    down = normalized_status == "DOWN" or reason == "AUTHENTICATION_FAILED"
    status = (
        unconfigured_status
        if configured is False
        else "READY" if ready else "DOWN" if down else "DEGRADED"
    )
    observed = getattr(provider, "last_observed_at", None)
    asof = (
        observed
        if isinstance(observed, datetime)
        and observed.tzinfo is not None
        and observed.utcoffset() is not None
        else attempted_at
    )
    result: dict[str, object] = {
        "source": source,
        "source_kind": source_kind,
        "status": status,
        "reason": reason,
        "success_count": max(0, int(success_count)),
        "failure_date_count": max(0, int(failure_date_count)),
        "asof": asof.astimezone(_UTC).isoformat(),
        "decision_authority": "SUPPORTING_ONLY",
    }
    coverage_status = str(snapshot.get("coverage_status") or "").strip().upper()
    requested_symbol_count = snapshot.get("requested_symbol_count")
    queried_symbol_count = snapshot.get("queried_symbol_count")
    if (
        coverage_status in {"FULL", "BOUNDED"}
        and isinstance(requested_symbol_count, int)
        and not isinstance(requested_symbol_count, bool)
        and isinstance(queried_symbol_count, int)
        and not isinstance(queried_symbol_count, bool)
        and 0 <= queried_symbol_count <= requested_symbol_count
    ):
        coverage_reason = str(
            snapshot.get("coverage_reason") or ""
        ).strip().upper()
        result.update(
            {
                "coverage_status": coverage_status,
                "coverage_reason": (
                    coverage_reason
                    if coverage_status == "BOUNDED"
                    and coverage_reason
                    in {"PROVIDER_TICKER_LIMIT", "PROVIDER_SYMBOL_ROTATION"}
                    else None
                ),
                "requested_symbol_count": requested_symbol_count,
                "queried_symbol_count": queried_symbol_count,
            }
        )
    return result


def _cadence_skipped_health(
    cadence: SourceCadenceStore,
    source_id: str,
    source_kind: str,
    *,
    now: datetime,
    reason: str,
) -> dict[str, object]:
    """Describe a suppressed lane without turning a skip into fresh evidence."""

    projection = next(
        (
            row
            for row in cadence.projections(now=now)
            if row.get("source_id") == source_id
            and row.get("source_kind") == source_kind
        ),
        {},
    )
    last_success = projection.get("last_success")
    return {
        "source": source_id,
        "source_kind": source_kind,
        "status": (
            "DEGRADED"
            if reason == "CADENCE_STATE_CORRUPT"
            or projection.get("freshness") in {"STALE", "NEVER", "UNAVAILABLE"}
            else "READY"
        ),
        "reason": reason,
        "success_count": 0,
        "failure_date_count": 0,
        "asof": last_success,
        "decision_authority": "SUPPORTING_ONLY",
    }


def _calendar_envelope_member(stored: StoredEvidence) -> dict[str, object]:
    """Bind one provider-returned calendar version to its persisted ledger row."""

    payload = stored.record.payload
    return {
        "identity": stored.identity,
        "source": str(payload.get("source") or stored.record.provider).strip().upper(),
        "source_id": str(
            payload.get("source_id")
            or payload.get("event_id")
            or stored.record.source_id
        ),
        "source_content_hash": str(
            payload.get("source_content_hash") or stored.content_hash
        ),
        "record_hash": stored.content_hash,
        "row_hash": stored.row_hash,
        "observed_at": stored.record.observed_at.astimezone(_UTC).isoformat(),
    }


def _calendar_source_batch(
    health: Mapping[str, object],
    members: Sequence[Mapping[str, object]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, object]:
    source = str(health.get("source") or "").strip().upper()
    unique = {
        canonical_hash({**dict(member), "batch_source": source}): {
            **dict(member),
            "batch_source": source,
        }
        for member in members
    }
    payload: dict[str, object] = {
        "schema": "options_copilot.calendar_source_batch.v1",
        "source": source,
        "status": str(health.get("status") or "").strip().upper(),
        "reason": health.get("reason"),
        "success_count": health.get("success_count"),
        "failure_date_count": health.get("failure_date_count"),
        "observed_at": health.get("asof"),
        "window_start": _aware(window_start).isoformat(),
        "window_end": _aware(window_end).isoformat(),
        "members": tuple(unique[key] for key in sorted(unique)),
    }
    return {**payload, "batch_hash": canonical_hash(payload)}


def _calendar_generation_envelope(
    source_batches: Sequence[Mapping[str, object]],
    *,
    observed_at: datetime,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, object]:
    ordered_batches = tuple(
        dict(item)
        for item in sorted(
            source_batches,
            key=lambda item: (
                str(item.get("source") or ""),
                str(item.get("batch_hash") or ""),
            ),
        )
    )
    members = tuple(
        dict(member)
        for batch in ordered_batches
        for member in (
            batch.get("members")
            if isinstance(batch.get("members"), Sequence)
            and not isinstance(batch.get("members"), (str, bytes, bytearray))
            else ()
        )
        if isinstance(member, Mapping)
    )
    payload: dict[str, object] = {
        "schema": "options_copilot.calendar_generation_envelope.v1",
        "observed_at": _aware(observed_at).isoformat(),
        "window_start": _aware(window_start).isoformat(),
        "window_end": _aware(window_end).isoformat(),
        "source_batches": ordered_batches,
        "current_members": members,
        "current_member_count": len(members),
    }
    return {**payload, "envelope_hash": canonical_hash(payload)}


def _calendar_envelope_timestamp(
    envelope: Mapping[str, object] | None,
    field: str,
) -> datetime | None:
    if not isinstance(envelope, Mapping):
        return None
    return _optional_timestamp(envelope.get(field))


def _mark_calendar_generation(
    row: Mapping[str, object],
    envelope: Mapping[str, object] | None,
) -> dict[str, object]:
    projected = dict(row)
    projected.update(
        {
            "current_generation": False,
            "calendar_envelope_hash": None,
            "calendar_generation_member_hash": None,
            "calendar_generation_source": None,
        }
    )
    if not isinstance(envelope, Mapping):
        return projected
    envelope_hash = envelope.get("envelope_hash")
    payload = {key: value for key, value in envelope.items() if key != "envelope_hash"}
    if (
        envelope.get("schema") != "options_copilot.calendar_generation_envelope.v1"
        or not isinstance(envelope_hash, str)
        or _HASH_RE.fullmatch(envelope_hash) is None
        or canonical_hash(payload) != envelope_hash
    ):
        return projected
    candidate = {
        "identity": projected.get("evidence_identity"),
        "source": str(projected.get("source") or "").strip().upper(),
        "source_id": projected.get("source_id"),
        "source_content_hash": projected.get("content_hash"),
        "record_hash": projected.get("record_hash"),
        "row_hash": projected.get("evidence_row_hash"),
        "observed_at": projected.get("observed_at"),
    }
    members = envelope.get("current_members")
    if not isinstance(members, Sequence) or isinstance(
        members,
        (str, bytes, bytearray),
    ):
        return projected
    matched_member = next(
        (
            dict(member)
            for member in members
            if isinstance(member, Mapping)
            and canonical_hash(
                {
                    key: value
                    for key, value in member.items()
                    if key != "batch_source"
                }
            )
            == canonical_hash(candidate)
        ),
        None,
    )
    if matched_member is None:
        return projected
    member_hash = canonical_hash(matched_member)
    projected.update(
        {
            "current_generation": True,
            "calendar_envelope_hash": envelope_hash,
            "calendar_generation_member_hash": member_hash,
            "calendar_generation_source": matched_member.get("batch_source"),
        }
    )
    return projected


def _provider_source_name(
    provider: object,
    snapshot: Mapping[object, object],
    *,
    source_kind: str,
) -> str:
    class_name = provider.__class__.__name__.strip("_").upper()
    module = provider.__class__.__module__.lower()
    hint = canonical_source_id(
        str(snapshot.get("source_id") or snapshot.get("source") or "")
    )
    if hint in {
        "SEC",
        "JIN10",
        "NASDAQ",
        "FINNHUB",
        "ALPHA_VANTAGE",
        "COMPANY_IR",
    }:
        return hint
    if "sec_current" in module or class_name == "SECCURRENT8KPROVIDER":
        return "SEC"
    if "nasdaq_earnings" in module or "NASDAQ" in class_name:
        return "NASDAQ"
    if "jin10" in module or "JIN10" in class_name:
        return "JIN10"
    if "finnhub" in module or "FINNHUB" in class_name:
        return "FINNHUB"
    if "alpha" in module or "ALPHA" in class_name:
        return "ALPHA_VANTAGE"
    fallback = re.sub(r"[^A-Z0-9_]+", "_", class_name).strip("_")
    if fallback:
        return fallback[:64]
    return "NEWS_PROVIDER" if source_kind == "NEWS" else "CALENDAR_PROVIDER"


def _phase2_news_cycle_observation(
    health: Mapping[str, object],
    events: Sequence[NewsEvent],
) -> dict[str, object]:
    """Project only facts proved by this completed provider call.

    Some legacy providers, including Jin10, do not expose a separate health
    snapshot with success timestamps.  The coordinator still knows the exact
    call result and the already-validated events it accepted.  Use those facts
    as a fallback without mutating provider state or manufacturing freshness
    after a failed/degraded cycle.
    """

    if str(health.get("status") or "").strip().upper() != "READY":
        return {}
    observed_at = _phase2_timestamp(health.get("asof"))
    if observed_at is None:
        return {}
    provenance: list[str] = []
    for event in events[:200]:
        for value in event.provenance:
            candidate = re.sub(
                r"[^A-Za-z0-9._:/-]+",
                "_",
                str(value).strip(),
            ).strip("_")
            if candidate and candidate not in provenance:
                provenance.append(candidate)
            if len(provenance) >= 64:
                break
        if len(provenance) >= 64:
            break
    return {
        "observed_at": observed_at,
        "last_success_at": observed_at,
        "freshness_age_seconds": 0,
        "provenance": tuple(provenance),
    }


def _phase2_source_rows(
    provider_lanes: Sequence[tuple[object, str]],
    *,
    attempted_at: datetime,
    forced_reasons: Mapping[int, str] | None = None,
    cycle_observations: Mapping[int, Mapping[str, object]] | None = None,
    cycle_health: Mapping[int, Mapping[str, object]] | None = None,
    cadence_rows: Sequence[Mapping[str, object]] = (),
) -> tuple[dict[str, object], ...]:
    """Retain exactly six independent, allowlisted source observations."""

    at = _aware(attempted_at)
    rows = {
        source_id: _phase2_default_source_row(source_id, as_of=at)
        for source_id in _PHASE2_SOURCE_IDS
    }
    reasons = forced_reasons or {}
    observations = cycle_observations or {}
    health_by_provider = cycle_health or {}
    cadence_by_lane = {
        (
            canonical_source_id(str(row.get("source_id") or "")),
            str(row.get("source_kind") or "").strip().upper(),
        ): row
        for row in cadence_rows
        if str(row.get("source_id") or "").strip()
        and str(row.get("source_kind") or "").strip()
    }
    for provider, source_kind in provider_lanes:
        snapshot: Mapping[object, object] = {}
        reader = getattr(provider, "health_snapshot", None)
        if callable(reader):
            try:
                candidate = reader()
            except Exception:
                candidate = None
            if isinstance(candidate, Mapping):
                snapshot = candidate
        source_id = _phase2_source_id(provider, snapshot)
        if source_id is None:
            continue
        cadence_source = canonical_source_id(
            _provider_source_name(provider, snapshot, source_kind=source_kind)
        )
        candidate = _phase2_source_row(
            source_id,
            provider=provider,
            snapshot=snapshot,
            attempted_at=at,
            forced_reason=reasons.get(id(provider)),
            cycle_observation=observations.get(id(provider)),
            cycle_health=health_by_provider.get(id(provider)),
            cadence_projection=cadence_by_lane.get(
                (cadence_source, source_kind.strip().upper())
            ),
        )
        current = rows[source_id]
        rows[source_id] = (
            candidate
            if current.get("configured") is not True
            else _merge_phase2_source_rows(current, candidate)
        )
    return tuple(rows[source_id] for source_id in _PHASE2_SOURCE_IDS)


def _phase2_default_source_row(
    source_id: str,
    *,
    as_of: datetime,
) -> dict[str, object]:
    status = "UNCONFIGURED" if source_id == "company_ir" else "NOT_CONFIGURED"
    return {
        "source_id": source_id,
        "configured": False,
        "readiness": status,
        "status": status,
        "observed_at": None,
        "as_of": _aware(as_of).isoformat(),
        "last_success_at": None,
        "freshness_age_seconds": None,
        "provenance": (),
        "pacing": "PACING_UNVERIFIED",
        "reason": status,
        "decision_authority": "SUPPORTING_ONLY",
    }


def _phase2_source_row(
    source_id: str,
    *,
    provider: object,
    snapshot: Mapping[object, object],
    attempted_at: datetime,
    forced_reason: str | None,
    cycle_observation: Mapping[str, object] | None,
    cycle_health: Mapping[str, object] | None,
    cadence_projection: Mapping[str, object] | None,
) -> dict[str, object]:
    observed_cycle = cycle_observation or {}
    current_health = cycle_health or {}
    cadence = cadence_projection or {}
    snapshot_status = str(
        snapshot.get("status", getattr(provider, "health", "UNKNOWN")) or "UNKNOWN"
    ).strip().upper()
    snapshot_status = (
        snapshot_status
        if snapshot_status in _PHASE2_SOURCE_STATES
        else "DEGRADED"
    )
    cycle_status = str(current_health.get("status") or "").strip().upper()
    if cycle_status not in _PHASE2_SOURCE_STATES:
        cycle_status = ""
    status = (
        cycle_status
        if cycle_status and cycle_status != "READY" and snapshot_status == "READY"
        else snapshot_status
    )
    configured_value = snapshot.get("configured")
    configured = (
        configured_value
        if isinstance(configured_value, bool)
        else status not in {"UNCONFIGURED", "NOT_CONFIGURED"}
    )
    raw_readiness = str(snapshot.get("readiness") or status).strip().upper()
    readiness = (
        raw_readiness
        if raw_readiness in _PHASE2_SOURCE_STATES
        else "DEGRADED"
    )
    if cycle_status and cycle_status != "READY" and snapshot_status == "READY":
        readiness = (
            cycle_status
            if cycle_status in {"DOWN", "UNAVAILABLE", "FAILED", "TIMEOUT"}
            else "DEGRADED"
        )
    cadence_failure = str(cadence.get("failure_code") or "").strip().upper()
    cadence_skipped = str(current_health.get("reason") or "").strip().upper() in {
        "CADENCE_NOT_DUE",
        "SOURCE_UNCONFIGURED",
        "CADENCE_STATE_CORRUPT",
    }
    if cadence_failure and (not current_health or cadence_skipped):
        status = "DEGRADED"
        readiness = "DEGRADED"
    elif (
        str(cadence.get("freshness") or "").strip().upper() == "STALE"
        and not current_health
        and status == "READY"
    ):
        status = "STALE"
        readiness = "DEGRADED"
    reason = (
        cadence_failure
        if cadence_failure and (not current_health or cadence_skipped)
        else (
            current_health.get("reason")
            or forced_reason
            or snapshot.get("reason")
            or getattr(provider, "health_reason", None)
            or cadence_failure
        )
    )
    force_projection = forced_reason is not None and (
        forced_reason in {"TRANSPORT_UNVERIFIED", "REQUEST_FAILED", "INVALID_RECORDS"}
        or "status" not in snapshot
    )
    if force_projection:
        normalized_forced = _source_reason(forced_reason)
        status = (
            "UNAVAILABLE"
            if normalized_forced == "TRANSPORT_UNVERIFIED"
            else "FAILED"
            if normalized_forced in {"REQUEST_FAILED", "INVALID_RECORDS"}
            else "DEGRADED"
        )
        readiness = "DEGRADED"
        reason = normalized_forced
    if status == "STALE" and reason is None:
        reason = "SOURCE_STALE"
    normalized_reason = None if status == "READY" else _source_reason(reason)
    cadence_success = cadence.get("last_success")
    observed_at = _latest_phase2_timestamp(
        snapshot.get("observed_at", getattr(provider, "last_observed_at", None)),
        observed_cycle.get("observed_at"),
        cadence_success,
    )
    as_of = _latest_phase2_timestamp(
        snapshot.get("as_of", snapshot.get("asof")),
        current_health.get("asof"),
        cadence.get("last_attempt"),
        cadence_success,
        attempted_at,
    )
    last_success_at = _latest_phase2_timestamp(
        snapshot.get("last_success_at", getattr(provider, "last_success_at", None)),
        observed_cycle.get("last_success_at"),
        cadence_success,
    )
    freshness = snapshot.get(
        "freshness_age_seconds",
        snapshot.get("freshness_seconds"),
    )
    if (
        isinstance(freshness, bool)
        or not isinstance(freshness, int)
        or not 0 <= freshness <= 31_536_000
    ):
        cycle_freshness = observed_cycle.get("freshness_age_seconds")
        freshness = (
            cycle_freshness
            if isinstance(cycle_freshness, int)
            and not isinstance(cycle_freshness, bool)
            and 0 <= cycle_freshness <= 31_536_000
            else None
        )
    if freshness is None:
        freshness = _phase2_freshness_age(
            as_of=as_of,
            last_success_at=last_success_at,
        )
    raw_pacing = str(snapshot.get("pacing") or "PACING_UNVERIFIED").strip().upper()
    pacing = (
        raw_pacing
        if raw_pacing in _PHASE2_PACING_STATES
        else "PACING_UNVERIFIED"
    )
    provenance = (
        _phase2_provenance(snapshot.get("provenance"))
        or _phase2_provenance(observed_cycle.get("provenance"))
    )
    if not configured:
        status = "UNCONFIGURED" if source_id == "company_ir" else "NOT_CONFIGURED"
        readiness = status
        normalized_reason = status
        observed_at = None
        last_success_at = None
        freshness = None
        provenance = ()
        pacing = "PACING_UNVERIFIED"
    return {
        "source_id": source_id,
        "configured": bool(configured),
        "readiness": readiness,
        "status": status,
        "observed_at": observed_at,
        "as_of": as_of or observed_at or _aware(attempted_at).isoformat(),
        "last_success_at": last_success_at,
        "freshness_age_seconds": freshness,
        "provenance": provenance,
        "pacing": pacing,
        "reason": normalized_reason,
        "decision_authority": "SUPPORTING_ONLY",
    }


def _merge_phase2_source_rows(
    left: Mapping[str, object],
    right: Mapping[str, object],
) -> dict[str, object]:
    """Merge multiple lanes without letting one healthy lane hide another failure."""

    configured_rows = [row for row in (left, right) if row.get("configured") is True]
    if not configured_rows:
        configured_rows = [left, right]
    worst = max(
        configured_rows,
        key=lambda row: _phase2_source_severity(row.get("status")),
    )
    result = dict(worst)
    result["configured"] = any(row.get("configured") is True for row in (left, right))
    result["readiness"] = max(
        (row.get("readiness") for row in configured_rows),
        key=_phase2_source_severity,
    )
    result["observed_at"] = _latest_phase2_timestamp(
        *(row.get("observed_at") for row in configured_rows),
    )
    result["as_of"] = _latest_phase2_timestamp(
        *(row.get("as_of") for row in configured_rows),
    )
    result["last_success_at"] = _latest_phase2_timestamp(
        *(row.get("last_success_at") for row in configured_rows),
    )
    result["freshness_age_seconds"] = _phase2_freshness_age(
        as_of=result["as_of"],
        last_success_at=result["last_success_at"],
    )
    result["provenance"] = tuple(
        dict.fromkeys(
            (
                *(
                    value
                    for row in configured_rows
                    for value in _phase2_provenance(row.get("provenance"))
                ),
            )
        )
    )
    result["pacing"] = max(
        (row.get("pacing") for row in configured_rows),
        key=_phase2_pacing_severity,
    )
    if str(result.get("status") or "").strip().upper() == "READY":
        result["reason"] = None
    return result


def _phase2_source_severity(value: object) -> int:
    status = str(value or "UNKNOWN").strip().upper()
    return {
        "READY": 0,
        "UNKNOWN": 1,
        "STALE": 2,
        "LIMITED": 3,
        "RATE_LIMITED": 4,
        "DEGRADED": 5,
        "BAD_JSON": 6,
        "TIMEOUT": 6,
        "FAILED": 7,
        "UNAVAILABLE": 7,
        "DOWN": 8,
        "UNCONFIGURED": 9,
        "NOT_CONFIGURED": 9,
    }.get(status, 5)


def _phase2_pacing_severity(value: object) -> int:
    return {
        "VERIFIED": 0,
        "PACING_UNVERIFIED": 1,
        "LIMITED": 2,
        "RATE_LIMITED": 3,
    }.get(str(value or "PACING_UNVERIFIED").strip().upper(), 1)


def _latest_phase2_timestamp(*values: object) -> str | None:
    parsed: list[datetime] = []
    for value in values:
        normalized = _phase2_timestamp(value)
        if normalized is not None:
            parsed.append(datetime.fromisoformat(normalized))
    return max(parsed).isoformat() if parsed else None


def _phase2_freshness_age(
    *,
    as_of: object,
    last_success_at: object,
) -> int | None:
    as_of_text = _phase2_timestamp(as_of)
    success_text = _phase2_timestamp(last_success_at)
    if as_of_text is None or success_text is None:
        return None
    seconds = int(
        (
            datetime.fromisoformat(as_of_text)
            - datetime.fromisoformat(success_text)
        ).total_seconds()
    )
    return seconds if 0 <= seconds <= 31_536_000 else None


def _phase2_source_id(
    provider: object,
    snapshot: Mapping[object, object],
) -> str | None:
    candidate = str(snapshot.get("source_id") or "").strip().lower()
    if candidate in _PHASE2_SOURCE_IDS:
        return candidate
    source = _provider_source_name(provider, snapshot, source_kind="NEWS")
    normalized = source.strip().lower()
    if normalized in _PHASE2_SOURCE_IDS:
        return normalized
    if normalized in {"alpha-vantage", "alpha vantage"}:
        return "alpha_vantage"
    if "company" in normalized and "ir" in normalized:
        return "company_ir"
    return None


def _phase2_timestamp(value: object) -> str | None:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(_UTC).isoformat()


def _phase2_provenance(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return ()
    result: list[str] = []
    for item in value[:64]:
        candidate = str(item).strip()
        if (
            not candidate
            or len(candidate) > 240
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,239}", candidate)
            is None
        ):
            continue
        if candidate not in result:
            result.append(candidate)
    return tuple(result)


def _phase2_fallback_reason(value: object) -> str:
    normalized = str(value or "").strip().upper()
    allowed = {
        "MODEL_DISABLED",
        "MODEL_EVALUATION_PENDING",
        "MODEL_BUDGET_EXHAUSTED",
        "MODEL_CONTEXT_LIMIT",
        "MODEL_TRANSPORT_UNAVAILABLE",
        "MODEL_OUTPUT_INVALID",
        "MODEL_BINDING_INVALID",
    }
    return normalized if normalized in allowed else "MODEL_EVALUATION_PENDING"


def _phase2_advisory_fallback_payload(
    *,
    as_of: datetime,
    reason: str,
) -> dict[str, object]:
    unavailable_slice = {
        "status": "UNAVAILABLE",
        "direction": "UNCERTAIN",
        "summary": (
            "Model advisory is unavailable; supplied observations remain "
            "supporting-only and uncertain."
        ),
        "evidence_ids": [],
    }
    return {
        "schema_version": ADVISORY_SCHEMA_VERSION,
        "symbol": None,
        "consensus_state": "UNCERTAIN",
        "model_state": "FALLBACK",
        "fallback_reason": _phase2_fallback_reason(reason),
        "as_of": _aware(as_of).isoformat(),
        "observations": [],
        "provenance_ids": [],
        "event_news_facts": [],
        "fundamental_support": dict(unavailable_slice),
        "expected_price_impact": dict(unavailable_slice),
        "options_volatility_impact": dict(unavailable_slice),
        "counter_evidence": [],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _phase2_source_conflicts(
    news_rows: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    conflicts: list[dict[str, object]] = []
    for row in news_rows:
        if str(row.get("status") or "").strip().upper() != "CONFLICTED":
            continue
        evidence = row.get("evidence")
        if not isinstance(evidence, Sequence) or isinstance(
            evidence,
            (str, bytes, bytearray),
        ):
            continue
        source_ids: list[str] = []
        evidence_ids: list[str] = []
        for item in evidence[:64]:
            if not isinstance(item, Mapping):
                continue
            source_id = _phase2_source_id_from_text(item.get("source"))
            evidence_id = str(item.get("id") or "").strip()
            if source_id is not None and source_id not in source_ids:
                source_ids.append(source_id)
            if (
                evidence_id
                and len(evidence_id) <= 240
                and evidence_id not in evidence_ids
            ):
                evidence_ids.append(evidence_id)
        if len(source_ids) < 2 or not evidence_ids:
            continue
        identity = "\x1f".join((*sorted(source_ids), *sorted(evidence_ids)))
        conflicts.append(
            {
                "conflict_id": "source-conflict:" + hashlib.sha256(
                    identity.encode("utf-8")
                ).hexdigest(),
                "source_ids": sorted(source_ids),
                "evidence_ids": sorted(evidence_ids),
                "reason": "INDEPENDENT_SOURCE_CONFLICT",
            }
        )
    return tuple(conflicts[:64])


def _phase2_source_id_from_text(value: object) -> str | None:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in _PHASE2_SOURCE_IDS:
        return normalized
    if normalized in {"sec/xbrl", "sec_current"}:
        return "sec"
    if normalized in {"issuer_ir", "company_official"}:
        return "company_ir"
    return None


def _phase2_source_evidence_payload(
    *,
    rows: Sequence[Mapping[str, object]],
    as_of: datetime,
    conflicts: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    configured = [row for row in rows if row.get("configured") is True]
    degraded = any(row.get("status") != "READY" for row in configured)
    status = "UNAVAILABLE" if not configured else "DEGRADED" if degraded else "READY"
    return {
        "schema": "options_copilot.source_evidence.v1",
        "status": status,
        "decision": "OBSERVATION_ONLY" if status == "READY" else "NO_TRADE",
        "as_of": _aware(as_of).isoformat(),
        "sources": [dict(row) for row in rows],
        "conflicts": [dict(item) for item in conflicts],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _provider_failure_date_count(provider: object) -> int:
    failed_dates = getattr(provider, "failed_dates", ())
    if not isinstance(failed_dates, Sequence) or isinstance(
        failed_dates, (str, bytes, bytearray)
    ):
        return 0
    return min(len(failed_dates), 15)


def _source_reason(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return (
        normalized
        if normalized in _SOURCE_HEALTH_REASON_CODES
        else "PROVIDER_DEGRADED"
    )


def _official_source_reason(reasons: Sequence[str]) -> str:
    for reason in reasons:
        normalized = str(reason).strip().upper()
        if normalized in _SOURCE_HEALTH_REASON_CODES:
            return normalized
    return "OFFICIAL_CALENDAR_PROVIDER_DEGRADED"


def _provider_transport_verified(provider: object) -> bool:
    module = provider.__class__.__module__
    if module != "options_copilot.providers.jin10":
        return True
    return getattr(provider, "transport_verified", False) is True


def _preselection_coverage(provider: object | None) -> dict[str, object]:
    if provider is None:
        return {
            "requested_count": 10,
            "available_count": 0,
            "source": None,
            "status": "UNCONFIGURED",
            "reason": "PRESELECTION_PROVIDER_UNCONFIGURED",
            "decision_authority": "SUPPORTING_ONLY",
        }
    reader = getattr(provider, "coverage", None)
    if not callable(reader):
        return {
            "requested_count": 10,
            "available_count": 0,
            "source": None,
            "status": "UNKNOWN",
            "reason": "PRESELECTION_COVERAGE_UNAVAILABLE",
            "decision_authority": "SUPPORTING_ONLY",
        }
    try:
        raw = reader()
    except Exception:
        raw = None
    return _normalize_preselection_coverage(raw)


def _normalize_preselection_coverage(raw: object) -> dict[str, object]:
    if not isinstance(raw, Mapping):
        return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    requested = raw.get("requested_count")
    available = raw.get("available_count")
    open_count = raw.get("open_count", 0)
    if (
        isinstance(requested, bool)
        or not isinstance(requested, int)
        or requested != 10
        or isinstance(available, bool)
        or not isinstance(available, int)
        or available < 0
        or available > requested
        or isinstance(open_count, bool)
        or not isinstance(open_count, int)
        or open_count < 0
        or open_count > available
    ):
        return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    status = str(raw.get("status") or "UNKNOWN").upper()
    source = str(raw.get("source") or "UNKNOWN")
    latest_run_id = raw.get("latest_run_id")
    latest_head_hash = raw.get("latest_head_hash")
    latest_open_batch_id = raw.get("latest_open_batch_id")
    latest_open_batch_head_hash = raw.get("latest_open_batch_head_hash")
    freeze_slot = raw.get("freeze_slot")
    reprice_slot = raw.get("reprice_slot")
    if latest_run_id is not None and (
        not isinstance(latest_run_id, str) or not latest_run_id.strip()
    ):
        return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    if latest_head_hash is not None and (
        not isinstance(latest_head_hash, str)
        or _HASH_RE.fullmatch(latest_head_hash) is None
    ):
        return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    if latest_open_batch_id is not None and (
        not isinstance(latest_open_batch_id, str)
        or not latest_open_batch_id.strip()
    ):
        return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    if latest_open_batch_head_hash is not None and (
        not isinstance(latest_open_batch_head_hash, str)
        or _HASH_RE.fullmatch(latest_open_batch_head_hash) is None
    ):
        return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    for slot in (freeze_slot, reprice_slot):
        if slot is None:
            continue
        if not isinstance(slot, str):
            return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
        try:
            _aware(datetime.fromisoformat(slot))
        except (TypeError, ValueError):
            return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    authority_fields = {
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    if any(
        name in raw and raw.get(name) != expected
        for name, expected in authority_fields.items()
    ):
        return _failed_preselection_coverage("PRESELECTION_COVERAGE_INVALID")
    return {
        "requested_count": requested,
        "available_count": available,
        "source": source,
        "status": status,
        "reason": (
            None if raw.get("reason") is None else str(raw.get("reason"))
        ),
        "ledger_reason": (
            None
            if raw.get("ledger_reason") is None
            else str(raw.get("ledger_reason"))
        ),
        "latest_run_id": latest_run_id,
        "latest_head_hash": latest_head_hash,
        "freeze_slot": freeze_slot,
        "open_count": open_count,
        "latest_open_batch_id": latest_open_batch_id,
        "latest_open_batch_head_hash": latest_open_batch_head_hash,
        "reprice_slot": reprice_slot,
        "open_reprice_producer_status": str(
            raw.get("open_reprice_producer_status") or "UNKNOWN"
        ).upper(),
        "open_reprice_writer": (
            None
            if raw.get("open_reprice_writer") is None
            else str(raw.get("open_reprice_writer"))
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _failed_preselection_coverage(reason: str) -> dict[str, object]:
    return {
        "requested_count": 10,
        "available_count": 0,
        "source": "INDEPENDENT_TOP10_LEDGER",
        "status": "UNAVAILABLE",
        "reason": reason,
        "ledger_reason": None,
        "latest_run_id": None,
        "latest_head_hash": None,
        "freeze_slot": None,
        "open_count": 0,
        "latest_open_batch_id": None,
        "latest_open_batch_head_hash": None,
        "reprice_slot": None,
        "open_reprice_producer_status": "UNAVAILABLE",
        "open_reprice_writer": "PENDING_FULL_IBKR_8_FIELD_IDENTITY_FREEZE",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _validated_preselection_lineage(
    raw: object,
    *,
    expected_keys: set[tuple[str, str]],
) -> dict[tuple[str, str], dict[str, object]] | None:
    if not isinstance(raw, Mapping) or set(raw) != expected_keys:
        return None
    result: dict[tuple[str, str], dict[str, object]] = {}
    common = {
        "source",
        "source_batch_purpose",
        "source_batch_id",
        "source_batch_hash",
        "preselection_id",
        "phase",
        "run_id",
        "run_created_at",
        "head_hash",
        "row_id",
        "row_hash",
        "premarket_rank",
    }
    open_only = {
        "observation_id",
        "observed_at",
        "observation_hash",
        "batch_id",
        "batch_head_hash",
        "scheduled_for",
        "quote_batch_id",
    }
    premarket_only = {
        "production_parent_eligible",
        "production_parent_blocker",
    }
    for key, raw_value in raw.items():
        if (
            not isinstance(key, tuple)
            or len(key) != 2
            or not all(isinstance(item, str) for item in key)
            or not isinstance(raw_value, Mapping)
        ):
            return None
        value = dict(raw_value)
        if key[1] == "OPEN_REPRICED":
            expected_fields = common | open_only
        elif key[1] == "PRE_MARKET":
            expected_fields = common | premarket_only
        else:
            return None
        if set(value) != expected_fields:
            return None
        if (
            value.get("source") != "INDEPENDENT_TOP10_LEDGER"
            or value.get("preselection_id") != key[0]
            or value.get("phase") != key[1]
            or not isinstance(value.get("run_id"), str)
            or not str(value.get("run_id") or "").strip()
            or not isinstance(value.get("run_created_at"), str)
            or not isinstance(value.get("row_id"), str)
            or not str(value.get("row_id") or "").strip()
            or isinstance(value.get("premarket_rank"), bool)
            or not isinstance(value.get("premarket_rank"), int)
            or int(value.get("premarket_rank", 0)) < 1
            or _HASH_RE.fullmatch(str(value.get("head_hash") or "")) is None
            or _HASH_RE.fullmatch(str(value.get("row_hash") or "")) is None
        ):
            return None
        source_fields = (
            value.get("source_batch_purpose"),
            value.get("source_batch_id"),
            value.get("source_batch_hash"),
        )
        source_present = tuple(item is not None for item in source_fields)
        if any(source_present) != all(source_present):
            return None
        if all(source_present):
            expected_purpose = (
                OPEN_REPRICE_PURPOSE
                if key[1] == "OPEN_REPRICED"
                else PREMARKET_ACCOUNT_PURPOSE
            )
            if (
                value.get("source_batch_purpose") != expected_purpose
                or _SOURCE_BATCH_ID_RE.fullmatch(
                    str(value.get("source_batch_id") or "")
                )
                is None
                or _HASH_RE.fullmatch(str(value.get("source_batch_hash") or ""))
                is None
            ):
                return None
        try:
            _aware(datetime.fromisoformat(str(value["run_created_at"])))
        except (TypeError, ValueError):
            return None
        if key[1] == "OPEN_REPRICED":
            if (
                not isinstance(value.get("observation_id"), str)
                or not str(value.get("observation_id") or "").strip()
                or _HASH_RE.fullmatch(str(value.get("observation_hash") or ""))
                is None
                or not isinstance(value.get("batch_id"), str)
                or not str(value.get("batch_id") or "").strip()
                or _HASH_RE.fullmatch(str(value.get("batch_head_hash") or ""))
                is None
                or not isinstance(value.get("scheduled_for"), str)
                or not isinstance(value.get("quote_batch_id"), str)
                or not str(value.get("quote_batch_id") or "").strip()
            ):
                return None
            try:
                observed_at = _aware(
                    datetime.fromisoformat(str(value["observed_at"]))
                )
                scheduled_for = _aware(
                    datetime.fromisoformat(str(value["scheduled_for"]))
                )
            except (TypeError, ValueError):
                return None
            if observed_at < scheduled_for:
                return None
        else:
            production_parent_eligible = value.get("production_parent_eligible")
            production_parent_blocker = value.get("production_parent_blocker")
            if (
                not isinstance(production_parent_eligible, bool)
                or (
                    production_parent_eligible
                    and production_parent_blocker is not None
                )
                or (
                    not production_parent_eligible
                    and production_parent_blocker
                    != "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
                )
            ):
                return None
        result[key] = value

    run_heads = {
        (str(item["run_id"]), str(item["head_hash"])) for item in result.values()
    }
    if len(run_heads) > 1:
        return None
    premarket_rows = {
        key[0]: value
        for key, value in result.items()
        if key[1] == "PRE_MARKET"
    }
    if any(
        key[0] not in premarket_rows
        or any(
            value[field] != premarket_rows[key[0]][field]
            for field in (
                "run_id",
                "run_created_at",
                "head_hash",
                "row_id",
                "row_hash",
                "premarket_rank",
            )
        )
        for key, value in result.items()
        if key[1] == "OPEN_REPRICED"
    ):
        return None
    return result


def _preselection_source_lineage_is_missing(
    lineage: Mapping[tuple[str, str], Mapping[str, object]],
) -> bool:
    """Return true for displayable legacy rows without an external batch claim."""

    return any(
        value.get("source_batch_purpose") is None
        or value.get("source_batch_id") is None
        or value.get("source_batch_hash") is None
        for value in lineage.values()
    )


def _source_lineage_missing_coverage(
    coverage: Mapping[str, object],
) -> dict[str, object]:
    """Keep legacy research visible while making its action authority impossible."""

    result = dict(coverage)
    result.update(
        {
            "status": "UNAVAILABLE",
            "reason": "SOURCE_LINEAGE_MISSING_LEGACY",
            "ledger_reason": "SOURCE_LINEAGE_MISSING_LEGACY",
            "open_reprice_producer_status": "UNAVAILABLE",
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }
    )
    return result


def _preselection_atomic_binding_is_valid(
    candidates: tuple[ConditionalOptionPreselection, ...],
    lineage: Mapping[tuple[str, str], Mapping[str, object]],
    coverage: Mapping[str, object],
) -> bool:
    """Verify one complete ledger snapshot before any action-pool evaluation."""

    if (
        coverage.get("source") != "INDEPENDENT_TOP10_LEDGER"
        or coverage.get("decision_authority") != "SUPPORTING_ONLY"
        or coverage.get("approval_eligible") is not False
        or coverage.get("instruction_creation_allowed") is not False
        or coverage.get("order_allowed") is not False
        or any(
            item.decision_authority is not NewsAuthority.SUPPORTING_ONLY
            or item.approval_eligible is not False
            or item.instruction_creation_allowed is not False
            for item in candidates
        )
    ):
        return False

    pre_candidates = {
        item.preselection_id: item
        for item in candidates
        if item.phase.value == "PRE_MARKET"
    }
    open_candidates = {
        item.preselection_id: item
        for item in candidates
        if item.phase.value == "OPEN_REPRICED"
    }
    if len(pre_candidates) + len(open_candidates) != len(candidates):
        return False
    pre_lineage = {
        key[0]: value for key, value in lineage.items() if key[1] == "PRE_MARKET"
    }
    open_lineage = {
        key[0]: value
        for key, value in lineage.items()
        if key[1] == "OPEN_REPRICED"
    }
    if (
        set(pre_candidates) != set(pre_lineage)
        or set(open_candidates) != set(open_lineage)
        or coverage.get("available_count") != len(pre_candidates)
        or coverage.get("open_count") != len(open_candidates)
    ):
        return False

    if pre_lineage:
        run_ids = {value["run_id"] for value in pre_lineage.values()}
        run_heads = {value["head_hash"] for value in pre_lineage.values()}
        freeze_slots = {value["run_created_at"] for value in pre_lineage.values()}
        row_ids = {value["row_id"] for value in pre_lineage.values()}
        row_hashes = {value["row_hash"] for value in pre_lineage.values()}
        ranks = {value["premarket_rank"] for value in pre_lineage.values()}
        if (
            len(run_ids) != 1
            or len(run_heads) != 1
            or len(freeze_slots) != 1
            or len(row_ids) != len(pre_lineage)
            or len(row_hashes) != len(pre_lineage)
            or len(ranks) != len(pre_lineage)
            or coverage.get("latest_run_id") != next(iter(run_ids))
            or coverage.get("latest_head_hash") != next(iter(run_heads))
            or coverage.get("freeze_slot") != next(iter(freeze_slots))
        ):
            return False
        pre_source_bindings = {
            (
                value.get("source_batch_purpose"),
                value.get("source_batch_id"),
                value.get("source_batch_hash"),
            )
            for value in pre_lineage.values()
        }
        if (
            len(pre_source_bindings) != 1
            or next(iter(pre_source_bindings))[0] != PREMARKET_ACCOUNT_PURPOSE
            or any(item is None for item in next(iter(pre_source_bindings)))
        ):
            return False

    if not open_lineage:
        return (
            not open_candidates
            and coverage.get("latest_open_batch_id") is None
            and coverage.get("latest_open_batch_head_hash") is None
            and coverage.get("reprice_slot") is None
        )

    if set(open_lineage) != set(pre_lineage) or any(
        pre_lineage[identifier].get("production_parent_eligible") is not True
        or pre_lineage[identifier].get("production_parent_blocker") is not None
        or any(
            open_lineage[identifier][field] != pre_lineage[identifier][field]
            for field in (
                "run_id",
                "run_created_at",
                "head_hash",
                "row_id",
                "row_hash",
                "premarket_rank",
            )
        )
        for identifier in open_lineage
    ):
        return False

    batch_ids = {value["batch_id"] for value in open_lineage.values()}
    batch_heads = {value["batch_head_hash"] for value in open_lineage.values()}
    reprice_slots = {value["scheduled_for"] for value in open_lineage.values()}
    quote_batch_ids = {value["quote_batch_id"] for value in open_lineage.values()}
    open_source_bindings = {
        (
            value.get("source_batch_purpose"),
            value.get("source_batch_id"),
            value.get("source_batch_hash"),
        )
        for value in open_lineage.values()
    }
    observation_ids = {value["observation_id"] for value in open_lineage.values()}
    observation_hashes = {
        value["observation_hash"] for value in open_lineage.values()
    }
    if (
        len(batch_ids) != 1
        or len(batch_heads) != 1
        or len(reprice_slots) != 1
        or len(quote_batch_ids) != 1
        or len(open_source_bindings) != 1
        or len(observation_ids) != len(open_lineage)
        or len(observation_hashes) != len(open_lineage)
        or coverage.get("latest_open_batch_id") != next(iter(batch_ids))
        or coverage.get("latest_open_batch_head_hash") != next(iter(batch_heads))
        or coverage.get("reprice_slot") != next(iter(reprice_slots))
        or coverage.get("open_reprice_producer_status") != "AVAILABLE"
    ):
        return False

    expected_quote_batch_id = next(iter(quote_batch_ids))
    open_source_purpose, open_source_batch_id, open_source_batch_hash = next(
        iter(open_source_bindings)
    )
    if (
        open_source_purpose != OPEN_REPRICE_PURPOSE
        or open_source_batch_id != expected_quote_batch_id
        or open_source_batch_hash is None
    ):
        return False
    for candidate in open_candidates.values():
        if not candidate.legs or any(
            leg.quote_batch_id != expected_quote_batch_id for leg in candidate.legs
        ):
            return False
    return True


def _binding_is_current(binding: IbkrNewsBinding, now: datetime) -> bool:
    if not binding.tradability.complete:
        return False
    observed = _aware(binding.tradability.observed_at)
    confirmed = _aware(binding.confirmation.observed_at)
    checked = _aware(now)
    if observed != confirmed or checked < observed:
        return False
    return checked - observed <= _MAXIMUM_IBKR_BINDING_AGE


def _fail_closed_public_news_actions(
    payload: dict[str, object],
    *,
    quote_stale: bool,
) -> None:
    """Clear actionable fields on a public clone without rebuilding storage."""

    payload["action_pool_count"] = 0
    payload["top3_count"] = 0
    payload["option_action_pool"] = []
    payload["option_action_pool_count"] = 0
    payload["approval_eligible"] = False
    payload["option_approval_eligible"] = False
    payload["instruction_creation_allowed"] = False
    payload["order_creation_allowed"] = False
    rows = payload.get("news")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            row["action_pool"] = False
            row["action_pool_eligible"] = False
            row["action_rank"] = None
            row["rank_one"] = False
            if quote_stale:
                row["option_tradability_score"] = 0
                row["ibkr_provenance"] = None
    repriced = payload.get("open_market_repriced")
    if quote_stale and isinstance(repriced, list):
        for row in repriced:
            if not isinstance(row, dict):
                continue
            row["action_pool_eligible"] = False
            row["action_rank"] = None
            blockers = row.get("blockers")
            safe_blockers = (
                list(blockers)
                if isinstance(blockers, Sequence)
                and not isinstance(blockers, (str, bytes, bytearray))
                else []
            )
            row["blockers"] = list(dict.fromkeys([*safe_blockers, "QUOTE_STALE"]))


def _analysis_binding_document(
    binding: IbkrNewsBinding | None,
) -> dict[str, object] | None:
    """Freeze the exact research-only IBKR inputs used by one analysis."""

    if binding is None:
        return None
    tradability = binding.tradability
    confirmation = binding.confirmation
    return {
        "symbol": binding.symbol,
        "quote_snapshot_id": binding.quote_snapshot_id,
        "tradability": {
            "symbol": tradability.symbol,
            "source": tradability.source,
            "observed_at": tradability.observed_at.isoformat(),
            "bid": None if tradability.bid is None else str(tradability.bid),
            "ask": None if tradability.ask is None else str(tradability.ask),
            "volume": tradability.volume,
            "open_interest": tradability.open_interest,
        },
        "confirmation": {
            "source": confirmation.source,
            "observed_at": confirmation.observed_at.isoformat(),
            "direction": confirmation.direction.value,
            "evidence_ids": list(confirmation.evidence_ids),
        },
    }


def _is_official_anchor(item: StoredEvidence) -> bool:
    source = str(
        item.record.payload.get("source") or item.record.provider
    ).strip().upper()
    return source in _OFFICIAL_ANCHOR_SOURCES and _source_rank(item) <= 1


def _news_symbol_bindings(
    evidence: Sequence[StoredEvidence],
) -> tuple[tuple[str, ...], str, tuple[str, ...]]:
    """Project every symbol whose provider binding remains auditable.

    Historical provider rows can predate persistence of an auditable upstream
    symbol-binding decision. They remain immutable and visible as ordinary
    news, but their requested ticker cannot feed a watchlist, quote binding, or
    action pool until a later provider observation records verified binding.
    One provider story can legitimately be returned for several requested
    tickers; exact story folding retains all independently verified symbols.
    """

    if not evidence:
        return (), "UNBOUND", ()
    ordered = sorted(evidence, key=lambda item: (_source_rank(item), item.sequence))
    primary = ordered[0]
    primary_payload = primary.record.payload
    verified: dict[str, set[str]] = defaultdict(set)
    for item in ordered:
        payload = item.record.payload
        symbol = str(payload.get("symbol") or item.record.symbol or "").strip().upper()
        adapter = str(payload.get("provider_adapter") or "").strip().upper()
        if (
            symbol
            and adapter
            and str(payload.get("symbol_binding_status") or "").strip().upper()
            == "VERIFIED_PROVIDER_RELATED"
            and _verified_symbol_binding_proof(
                payload.get("symbol_binding_proof"),
                symbol=symbol,
                provider_adapter=adapter,
            )
        ):
            verified[symbol].add(adapter)
    if verified:
        return (
            tuple(sorted(verified)),
            "VERIFIED_PROVIDER_RELATED",
            tuple(sorted({adapter for values in verified.values() for adapter in values})),
        )

    symbol = str(
        primary_payload.get("symbol") or primary.record.symbol or ""
    ).strip().upper()
    if not symbol:
        adapter = str(primary_payload.get("provider_adapter") or "").strip().upper()
        return (), "UNBOUND", (adapter,) if adapter else ()

    adapter = str(primary_payload.get("provider_adapter") or "").strip().upper()
    if adapter:
        return (), "UNVERIFIED_PROVIDER_BINDING", (adapter,)

    event_id = str(primary_payload.get("event_id") or "").strip().lower()
    if (
        _source_rank(primary) >= 2
        and _LEGACY_HASHED_NEWS_EVENT_RE.fullmatch(event_id) is not None
    ):
        return (), "UNVERIFIED_LEGACY_PROVIDER", ()

    return (symbol,), "SOURCE_DECLARED", ()


def _news_symbol_binding(
    evidence: Sequence[StoredEvidence],
) -> tuple[str, str, str | None]:
    """Compatibility projection for callers that require exactly one symbol."""

    symbols, status, adapters = _news_symbol_bindings(evidence)
    return (
        symbols[0] if len(symbols) == 1 else "",
        status,
        adapters[0] if len(adapters) == 1 else None,
    )


def _provenance_sources(
    evidence: Sequence[StoredEvidence],
) -> list[dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    ordered = sorted(evidence, key=lambda item: (_source_rank(item), item.sequence))
    for item in ordered:
        raw = item.record.payload.get("provenance")
        sources: list[str] = []
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
            sources.extend(
                str(value).strip() for value in raw if str(value).strip()
            )
        source = str(
            item.record.payload.get("source") or item.record.provider
        ).strip()
        if source:
            sources.append(source)
        for name in sources:
            rows.setdefault(
                name,
                {
                    "source": name,
                    "provider": item.record.provider,
                    "source_rank": _source_rank(item),
                    "published_at": item.record.published_at.isoformat(),
                    "first_seen_at": item.record.first_seen_at.isoformat(),
                    "observed_at": item.record.observed_at.isoformat(),
                    "content_hash": item.content_hash,
                    "decision_authority": "SUPPORTING_ONLY",
                },
            )
    return list(rows.values())


def _advisory_projection(
    advisory: ResearchAdvisoryProjection,
) -> dict[str, object]:
    classification = advisory.classification
    projection = {
        "advisory_id": advisory.advisory_id,
        "symbol": advisory.symbol,
        "classifier": classification.classifier,
        "research_priority_score": str(advisory.research_priority_score),
        "classification": classification.as_dict(),
        "shadow_prediction_count": len(advisory.prediction_specs),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    if advisory.symbol_binding is not None:
        projection["symbol_binding"] = advisory.symbol_binding.as_dict()
    return projection


def _shadow_research_input(
    analysis: AnalyzedNews,
    *,
    eligible: bool,
    pre_model_priority_rank: int | None,
    allowed_symbols: Sequence[str],
) -> ResearchAdvisoryInput:
    """Keep issuer symbols intact, or create one shadow-only macro proxy copy."""

    news = analysis.news
    binding = None
    if not news.symbols:
        binding = bind_market_proxy(news, allowed_symbols=allowed_symbols)
        if binding is not None:
            news = replace(news, symbols=(binding.proxy_symbol,))
    return ResearchAdvisoryInput(
        news=news,
        eligible=eligible,
        pre_model_priority_rank=pre_model_priority_rank,
        symbol_binding=binding,
    )


def _restored_advisory_matches_input(
    restored: Mapping[str, object] | None,
    candidate: ResearchAdvisoryInput,
) -> bool:
    """Match one persisted advisory to the exact immutable model input."""

    return bool(
        _restored_advisory_has_exact_input(restored, candidate)
        and restored is not None
        and restored.get("prediction_set_complete") is True
    )


def _restored_advisory_has_exact_input(
    restored: Mapping[str, object] | None,
    candidate: ResearchAdvisoryInput,
) -> bool:
    if restored is None:
        return False
    advisory_id = restored.get("advisory_id")
    if not isinstance(advisory_id, str):
        return False
    return advisory_id == f"news-advisory:{_shadow_advisory_input_hash(candidate)}"


def _shadow_advisory_input_hash(candidate: ResearchAdvisoryInput) -> str:
    return research_advisory_input_hash(candidate)


def _shadow_pre_model_skip_reasons(
    inputs: Sequence[ResearchAdvisoryInput],
) -> dict[str, int]:
    reasons: dict[str, int] = defaultdict(int)
    for item in inputs:
        if item.eligible is not True:
            reasons["SHADOW_PRE_MODEL_NOT_RANKED"] += 1
        elif item.pre_model_priority_rank is None:
            reasons["SHADOW_PRE_MODEL_RANK_UNAVAILABLE"] += 1
    return dict(reasons)


def _merge_reason_counts(
    *groups: Mapping[str, int],
) -> dict[str, int]:
    merged: dict[str, int] = defaultdict(int)
    for group in groups:
        for reason, count in group.items():
            if (
                isinstance(reason, str)
                and reason
                and isinstance(count, int)
                and not isinstance(count, bool)
                and count > 0
            ):
                merged[reason] += count
    return dict(sorted(merged.items()))


def _verified_symbol_binding_proof(
    value: object,
    *,
    symbol: str,
    provider_adapter: str,
) -> bool:
    """Accept only the current versioned proof; legacy status markers fail closed."""

    if not isinstance(value, Mapping):
        return False
    adapter = provider_adapter.strip().upper()
    method = str(value.get("method") or "").strip().upper()
    provider_symbols = value.get("provider_symbols")
    corroborating_terms = value.get("corroborating_terms")
    if (
        value.get("schema_version") != 1
        or value.get("verified") is not True
        or str(value.get("provider_adapter") or "").strip().upper() != adapter
        or str(value.get("requested_symbol") or "").strip().upper() != symbol
        or not isinstance(provider_symbols, Sequence)
        or isinstance(provider_symbols, (str, bytes, bytearray))
        or symbol
        not in {str(item).strip().upper() for item in provider_symbols}
        or not isinstance(corroborating_terms, Sequence)
        or isinstance(corroborating_terms, (str, bytes, bytearray))
    ):
        return False
    terms = {str(item).strip().upper() for item in corroborating_terms}
    if method == "PROVIDER_RELATED_PLUS_ENTITY_LINK":
        required = {
            symbol,
            f"CATALOG_VERSION={ENTITY_LINK_CATALOG_VERSION}",
            f"CATALOG_HASH={ENTITY_LINK_CATALOG_HASH.upper()}",
        }
        methods = {
            "METHOD=CONTROLLED_ALIAS",
            "METHOD=EXPLICIT_CASHTAG",
            "METHOD=EXPLICIT_EXCHANGE_TICKER",
        }
        return (
            adapter == "FINNHUB"
            and len(terms) == 4
            and required.issubset(terms)
            and len(terms & methods) == 1
        )
    if method == "PROVIDER_TICKER_SENTIMENT_EXACT":
        return adapter == "ALPHA_VANTAGE" and "TICKER_SENTIMENT" in terms
    return False


def _rebuild_restored_advisory(
    restored: Mapping[str, object],
    candidate: ResearchAdvisoryInput,
) -> ResearchAdvisoryProjection | None:
    """Rebuild a partial exact-input projection without another model call."""

    classification = restored.get("classification")
    if not isinstance(classification, Mapping):
        return None
    try:
        classification_payload = dict(classification)
        classification_payload["confidence"] = Decimal(
            str(classification_payload.get("confidence"))
        )
        checked_classification = ClassifiedEvent(**classification_payload)
        return ShadowResearchAdvisory._projection(
            candidate,
            _shadow_advisory_input_hash(candidate),
            checked_classification,
        )
    except (TypeError, ValueError):
        return None


def _normalize_restored_advisory(
    restored: Mapping[str, object],
) -> dict[str, object]:
    """Preserve the public projection shape when replaying an older ledger row."""

    normalized = dict(restored)
    normalized.pop("prediction_set_complete", None)
    classification = restored.get("classification")
    if isinstance(classification, Mapping):
        classifier = classification.get("classifier")
        if isinstance(classifier, str):
            normalized.setdefault("classifier", classifier)
    return normalized


def _shadow_status(
    status: str,
    reason: str | None,
    *,
    advisory_count: int = 0,
    attempted_count: int = 0,
    failure_count: int = 0,
    deferred_count: int = 0,
    failure_reasons: Mapping[str, int] | None = None,
    input_count: int = 0,
    eligible_input_count: int = 0,
    skipped_count: int = 0,
    skipped_reasons: Mapping[str, int] | None = None,
) -> dict[str, object]:
    return {
        "status": status,
        "reason": reason,
        "advisory_count": advisory_count,
        "attempted_count": attempted_count,
        "failure_count": failure_count,
        "deferred_count": deferred_count,
        "failure_reasons": dict(sorted((failure_reasons or {}).items())),
        "input_count": input_count,
        "eligible_input_count": eligible_input_count,
        "skipped_count": skipped_count,
        "skipped_reasons": dict(sorted((skipped_reasons or {}).items())),
        "maximum_batch_size": 3,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _shadow_priority(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and Decimal("0") <= parsed <= Decimal("100") else None


def _shadow_suggested_ranks(
    overlays: Mapping[str, Mapping[str, object]],
) -> dict[str, int]:
    """Rank the shadow display overlay without touching deterministic pools."""

    scored = [
        (event_id, score)
        for event_id, overlay in overlays.items()
        if (score := _shadow_priority(overlay.get("research_priority_score")))
        is not None
    ]
    scored.sort(key=lambda item: (-item[1], item[0]))
    return {
        event_id: rank
        for rank, (event_id, _score) in enumerate(scored, start=1)
    }


def _equity_news_rows(
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Keep a bounded, provider-verified factor view independent of GUI ranking."""

    selected: list[dict[str, object]] = []
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        symbols = _equity_news_binding_symbols(row)
        if not symbols or all(
            counts[symbol] >= _EQUITY_NEWS_PER_SYMBOL_LIMIT
            for symbol in symbols
        ):
            continue
        selected.append(_copy_json(dict(row)))
        for symbol in symbols:
            if counts[symbol] < _EQUITY_NEWS_PER_SYMBOL_LIMIT:
                counts[symbol] += 1
        if len(selected) >= _EQUITY_NEWS_READ_MODEL_OUTPUT_LIMIT:
            break
    return selected


def _equity_news_binding_symbols(
    row: Mapping[str, object],
) -> tuple[str, ...]:
    raw_symbols = row.get("symbols")
    symbols = (
        tuple(
            dict.fromkeys(
                str(value).strip().upper()
                for value in raw_symbols
                if str(value).strip()
            )
        )
        if isinstance(raw_symbols, Sequence)
        and not isinstance(raw_symbols, (str, bytes, bytearray))
        else ()
    )
    binding = row.get("symbol_binding")
    status = (
        str(binding.get("status") or "").strip().upper()
        if isinstance(binding, Mapping)
        else ""
    )
    if symbols and (
        status.startswith("VERIFIED")
        or status in {"PROVIDER_VERIFIED", "IBKR_VERIFIED"}
    ):
        return symbols
    if symbols:
        return ()

    research_proxy = row.get("research_proxy_binding")
    proxy_symbol = (
        str(research_proxy.get("proxy_symbol") or "").strip().upper()
        if isinstance(research_proxy, Mapping)
        else ""
    )
    if not proxy_symbol:
        return ()
    try:
        binding = require_current_research_proxy_binding(
            research_proxy,
            symbol=proxy_symbol,
        )
    except (TypeError, ValueError):
        return ()
    return () if binding is None else (binding.proxy_symbol,)


def _deterministic_decision_news_row(
    row: Mapping[str, object],
) -> dict[str, object]:
    """Remove every shadow-only field from production decision-event input."""

    projected = {
        key: value
        for key, value in row.items()
        if key != "research_advisory"
        and not key.startswith("shadow_")
        and key not in {"rank_displacement", "research_priority_score"}
    }
    scores = projected.get("scores")
    if (
        "event_impact_score" not in projected
        and isinstance(scores, Mapping)
        and "event_impact_score" in scores
    ):
        projected["event_impact_score"] = scores.get("event_impact_score")
    impact_score = projected.get("event_impact_score")
    if isinstance(impact_score, float):
        # Both the internal coordinator row and the public read model serialize
        # this already-deterministic score as a JSON number.  Re-enter the
        # strict Decimal boundary through the same shortest decimal spelling
        # JSON publishes, never through Decimal.from_float() or a binary-float
        # calculation.
        projected["event_impact_score"] = str(impact_score)
    elif isinstance(impact_score, Decimal):
        projected["event_impact_score"] = format(impact_score, "f")
    times = projected.get("times")
    if isinstance(times, Mapping):
        # The public read model groups timestamps under ``times`` while the
        # deterministic equity-factor boundary consumes canonical top-level
        # point-in-time fields.  Normalize the same trusted values here so a
        # GUI-shaped refresh cannot silently erase news direction evidence.
        for field in ("observed_at", "published_at"):
            if projected.get(field) is None and times.get(field) is not None:
                projected[field] = times.get(field)
    return _copy_json(projected)


def _deterministic_decision_calendar_row(
    row: Mapping[str, object],
) -> dict[str, object]:
    """Project only the positive Gate-3 calendar authority schema."""

    return _copy_json(
        {
            key: value
            for key, value in row.items()
            if key in {
                "id",
                "event_id",
                "calendar_origin",
                "source_id",
                "title",
                "summary",
                "category",
                "event_at",
                "scheduled_at",
                "event_date",
                "timezone",
                "schedule_precision",
                "report_session",
                "is_estimated",
                "eps_estimate",
                "revenue_estimate",
                "symbols",
                "source",
                "source_url",
                "url",
                "status",
                "importance",
                "country",
                "published_at",
                "first_seen_at",
                "ingested_at",
                "observed_at",
                "windows",
                "provenance",
                "content_hash",
                "record_hash",
                "evidence_identity",
                "evidence_row_hash",
                "evidence_hash",
                "current_generation",
                "calendar_envelope_hash",
                "calendar_generation_member_hash",
                "calendar_generation_source",
                "decision_authority",
                "approval_eligible",
                "instruction_creation_allowed",
                "order_creation_allowed",
                "evidence",
            }
        }
    )


def _symbols(values: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or len(symbol) > 12 or not symbol.replace(".", "").isalnum():
            raise ValueError("invalid core news symbol")
        if symbol not in result:
            result.append(symbol)
    return tuple(result)


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("news coordinator clock must return a timezone-aware datetime")
    return value.astimezone(_UTC)


def _elapsed_ms(start: datetime, end: datetime) -> float | None:
    """Return a truthful non-negative interval without inventing timestamps."""

    elapsed = (_aware(end) - _aware(start)).total_seconds() * 1000
    return None if elapsed < 0 else round(elapsed, 3)


def _identifier(namespace: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()
    return f"{namespace}:{digest}"


def _news_identity(event: NewsEvent) -> str:
    symbol, headline, minute = event.identity_key
    return _identifier("news", f"{symbol}\x1f{headline}\x1f{minute}")


def _news_record_conflicted(item: StoredEvidence) -> bool:
    payload = item.record.payload
    return (
        item.record.status == "CONFLICTED"
        or str(payload.get("provider_status") or "").upper() == "CONFLICTED"
        or (
            item.record.kind == "NEWS"
            and payload.get("source") == "SEC"
            and item.status == "CONFLICTED"
        )
    )


def _news_story_group_key(item: StoredEvidence) -> str:
    """Return an exact, deterministic read-model identity for one story.

    Raw evidence identities remain append-only and symbol-specific. A stable
    provider story id is authoritative for read-model folding when present;
    otherwise only an exact normalized source/title/publication/URL tuple may
    fold records. No fuzzy title matching is permitted here.
    """

    if _news_record_conflicted(item):
        # Preserve the existing explicit-conflict envelope. These versions are
        # not considered one clean story and remain ineligible for action.
        return _identifier("news-conflict", item.identity)
    return _news_provider_story_group_key(item) or _news_exact_story_group_key(item)


def _news_provider_story_group_key(item: StoredEvidence) -> str | None:
    payload = item.record.payload
    adapter = " ".join(
        str(payload.get("provider_adapter") or "").upper().split()
    )
    provider_story_id = str(payload.get("provider_story_id") or "").strip()
    if not adapter or not provider_story_id:
        return None
    return _identifier(
        "news-story",
        canonical_hash(
            {
                "kind": "PROVIDER_STORY",
                "provider_adapter": adapter,
                "provider_story_id": provider_story_id,
            }
        ),
    )


def _news_exact_story_group_key(item: StoredEvidence) -> str:
    payload = item.record.payload
    return _identifier(
        "news-story",
        canonical_hash(
            {
                "kind": "EXACT_STORY",
                "source": " ".join(
                    str(payload.get("source") or item.record.provider).lower().split()
                ),
                "headline": " ".join(
                    str(payload.get("headline") or "").lower().split()
                ),
                "published_at": item.record.published_at.isoformat(),
                "canonical_url": _canonical_story_url(payload.get("url")),
            }
        ),
    )


def _group_news_story_records(
    evidence: Sequence[StoredEvidence],
) -> dict[str, list[StoredEvidence]]:
    """Fold legacy exact rows into one unambiguous stable provider story."""

    stable_by_exact: dict[str, set[str]] = defaultdict(set)
    for item in evidence:
        if _news_record_conflicted(item):
            continue
        stable = _news_provider_story_group_key(item)
        if stable is not None:
            stable_by_exact[_news_exact_story_group_key(item)].add(stable)

    groups: dict[str, list[StoredEvidence]] = defaultdict(list)
    for item in evidence:
        if _news_record_conflicted(item):
            key = _identifier("news-conflict", item.identity)
        else:
            stable = _news_provider_story_group_key(item)
            exact = _news_exact_story_group_key(item)
            candidates = stable_by_exact.get(exact, set())
            key = stable or (next(iter(candidates)) if len(candidates) == 1 else exact)
        groups[key].append(item)
    return groups


def _news_story_group_identity(evidence: Sequence[StoredEvidence]) -> str:
    if not evidence:
        return ""
    stable = {
        key
        for item in evidence
        if (key := _news_provider_story_group_key(item)) is not None
    }
    if len(stable) == 1:
        return next(iter(stable))
    return _news_story_group_key(
        min(evidence, key=lambda item: (_source_rank(item), item.sequence))
    )


def _news_story_content_hash(item: StoredEvidence) -> str:
    """Hash exact story content without the requested-symbol fan-out."""

    payload = item.record.payload
    return canonical_hash(
        {
            "source": " ".join(
                str(payload.get("source") or item.record.provider).lower().split()
            ),
            "headline": " ".join(
                str(payload.get("headline") or "").lower().split()
            ),
            "summary": " ".join(str(payload.get("summary") or "").split()),
            "canonical_url": _canonical_story_url(payload.get("url")),
            "published_at": item.record.published_at.isoformat(),
        }
    )


def _canonical_story_url(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = urllib.parse.urlsplit(text)
    except ValueError:
        return text
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return text
    query = urllib.parse.urlencode(
        sorted(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)),
        doseq=True,
    )
    return urllib.parse.urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            query,
            "",
        )
    )


def _calendar_identity(event_id: str) -> str:
    return _identifier("calendar", event_id)


def _source_rank(item: StoredEvidence) -> int:
    raw = item.record.payload.get("source_rank")
    if isinstance(raw, bool):
        return 999
    try:
        return int(raw)
    except (TypeError, ValueError, OverflowError):
        return 999


def _same_evidence(
    left: Sequence[StoredEvidence],
    right: Sequence[StoredEvidence],
) -> bool:
    """Compare immutable evidence identity without decoding provider payloads."""

    return tuple(
        (item.sequence, item.content_hash, item.status) for item in left
    ) == tuple(
        (item.sequence, item.content_hash, item.status) for item in right
    )


def _earnings_time(report_date: date, raw_hour: object) -> datetime:
    hour = str(raw_hour or "").strip().lower()
    local_time = time(8, 0) if hour in {"bmo", "before market open"} else time(16, 15) if hour in {"amc", "after market close"} else time(12, 0)
    return datetime.combine(report_date, local_time, tzinfo=_EASTERN).astimezone(_UTC)


def _weekly_not_run_projection(
    previous: Mapping[str, object],
    reason_codes: Sequence[str],
) -> dict[str, object]:
    """Hide stale weekly content behind a current fail-closed projection."""

    return {
        "read_model_schema": "options_copilot.weekly_brief_read_model.v1",
        "status": "NOT_RUN",
        "decision": "OBSERVATION_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "execution_allowed": False,
        "review_allowed": False,
        "combination_generation_allowed": False,
        "reason_codes": list(reason_codes),
        "weekly_brief": None,
        "persistence": dict(previous.get("persistence") or {}),
    }


def _copy_json(value: Mapping[str, object]) -> dict[str, object]:
    """Copy the small JSON-shaped projection without exposing mutable internals."""

    result: dict[str, object] = {}
    for key, item in value.items():
        if isinstance(item, dict):
            result[key] = _copy_json(item)
        elif isinstance(item, list):
            result[key] = [
                _copy_json(child) if isinstance(child, dict) else child for child in item
            ]
        else:
            result[key] = item
    return result


__all__ = [
    "EventReactionProvider",
    "IbkrNewsBinding",
    "IbkrNewsBindingProvider",
    "NewsCoordinator",
    "OfficialCalendarSnapshotProvider",
    "OptionPreselectionProvider",
    "ReactionProvider",
]
