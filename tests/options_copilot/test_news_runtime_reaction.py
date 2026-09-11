from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import threading
from types import SimpleNamespace
from typing import Iterable

import pytest

from options_copilot.news.reaction import (
    ConsensusExpectation,
    EventReactionLedger,
    MarketReactionEvidence,
    OfficialRelease,
    OptionReevaluationEvidence,
    ScheduledEventIdentity,
)
from options_copilot.news.reaction_runtime import (
    ProductionMacroReactionProvider,
    ProductionReactionObserver,
    ReactionEvidenceStore,
)
from options_copilot.news.reaction_specs import reaction_descriptor_from_calendar
from options_copilot.news_runtime import (
    NewsCoordinator,
    _calendar_reaction_identity_hash,
)
from options_copilot.providers.official import (
    OfficialCalendarEvent,
    OfficialCalendarSnapshot,
    OfficialEventProvenance,
    OfficialSourceHealth,
)
from options_copilot.providers.official_sources import (
    BEA_RELEASE_DATES_URL,
    BLS_CALENDAR_URL,
    FEDERAL_RESERVE_FOMC_URL,
    FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
    build_official_calendar_provider,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
SCHEDULED_AT = OBSERVED_AT + timedelta(minutes=1)
PUBLISHED_AT = OBSERVED_AT - timedelta(days=30)
SOURCE = "Bureau of Labor Statistics"
SOURCE_ID = "cpi-2026-07"
SOURCE_URL = "https://www.bls.gov/news.release/cpi.nr0.htm"
EVENT_ID = "bls-cpi-2026-07"


class _ReactionProvider:
    def __init__(self, values: Iterable[object] = (), *, error: Exception | None = None) -> None:
        self.values = tuple(values)
        self.error = error
        self.calls = 0
        self.requested_event_ids: list[tuple[str, ...]] = []

    def reactions(self, event_ids: tuple[str, ...]) -> Iterable[object]:
        self.calls += 1
        self.requested_event_ids.append(event_ids)
        if self.error is not None:
            raise self.error
        return self.values


class _RestoringReactionProvider(_ReactionProvider):
    def __init__(self) -> None:
        super().__init__()
        self.restored: tuple[OfficialCalendarEvent, ...] = ()

    def restore_official_identities(
        self,
        events: Iterable[OfficialCalendarEvent],
    ) -> None:
        self.restored = tuple(events)


class _RollingOfficialProvider:
    health = "READY"
    health_reason = None

    def __init__(self) -> None:
        self.calls: list[datetime] = []

    def future_two_weeks(self, *, now: datetime | None = None) -> OfficialCalendarSnapshot:
        assert now is not None
        self.calls.append(now)
        return _official_snapshot() if now == OBSERVED_AT else _empty_official_snapshot(now)


def _official_snapshot(
    *,
    published_at: datetime | None = PUBLISHED_AT,
) -> OfficialCalendarSnapshot:
    provenance = OfficialEventProvenance(
        source=SOURCE,
        source_url=SOURCE_URL,
        source_id=SOURCE_ID,
        source_payload_hash="a" * 64,
        published_at=published_at,
        first_seen_at=OBSERVED_AT,
        ingested_at=OBSERVED_AT,
        observed_at=OBSERVED_AT,
    )
    event = OfficialCalendarEvent(
        event_id=EVENT_ID,
        source=SOURCE,
        source_id=SOURCE_ID,
        source_url=SOURCE_URL,
        title="Consumer Price Index - July 2026",
        category="MACRO",
        scheduled_at=SCHEDULED_AT,
        published_at=published_at,
        first_seen_at=OBSERVED_AT,
        ingested_at=OBSERVED_AT,
        observed_at=OBSERVED_AT,
        timezone_name="UTC",
        schedule_precision="EXACT",
        symbols=("SPY",),
        provenance=(provenance,),
    )
    source = OfficialSourceHealth(
        source=SOURCE,
        source_url=SOURCE_URL,
        status="READY",
        reason=None,
        observed_at=OBSERVED_AT,
        event_count=1,
    )
    return OfficialCalendarSnapshot(
        status="READY",
        decision="OBSERVATION_ONLY",
        window_start=OBSERVED_AT,
        window_end=OBSERVED_AT + timedelta(days=14),
        observed_at=OBSERVED_AT,
        events=(event,),
        sources=(source,),
        reasons=(),
    )


def _empty_official_snapshot(now: datetime) -> OfficialCalendarSnapshot:
    source = OfficialSourceHealth(
        source=SOURCE,
        source_url=SOURCE_URL,
        status="READY",
        reason=None,
        observed_at=now,
        event_count=0,
    )
    return OfficialCalendarSnapshot(
        status="READY",
        decision="OBSERVATION_ONLY",
        window_start=now,
        window_end=now + timedelta(days=14),
        observed_at=now,
        events=(),
        sources=(source,),
        reasons=(),
    )


def test_restart_restores_hash_verified_official_identity_but_stays_no_trade(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "durable-official.sqlite3"
    initial = NewsCoordinator(
        evidence_path,
        official_calendar_snapshot=_official_snapshot(),
        clock=lambda: OBSERVED_AT,
    )
    try:
        initial.refresh_once()
        first = initial.calendar_payload()["calendar"][0]
        assert first["event_id"] == EVENT_ID
    finally:
        initial.close()

    class FailingOfficialProvider:
        health = "DEGRADED"
        health_reason = "HTTP_403"

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            raise RuntimeError("redacted BLS failure")

    reaction = _RestoringReactionProvider()
    restarted = NewsCoordinator(
        evidence_path,
        official_calendar_provider=FailingOfficialProvider(),
        reaction_provider=reaction,
        clock=lambda: OBSERVED_AT,
    )
    try:
        # Construction may restore durable identity, but does not perform a
        # provider observation or manufacture a current snapshot.
        assert [event.event_id for event in reaction.restored] == [EVENT_ID]
        assert reaction.restored[0].observed_at == OBSERVED_AT

        restarted.refresh_once()
        payload = restarted.calendar_payload()
        row = payload["calendar"][0]

        assert payload["decision"] == "NO_TRADE"
        assert payload["provider"]["status"] == "DEGRADED"
        assert payload["reasons"] == [
            "DURABLE_OFFICIAL_CALENDAR_RESTORED_STALE",
            "OFFICIAL_CALENDAR_PROVIDER_UNAVAILABLE",
        ]
        assert payload["sources"][0]["status"] == "STALE"
        assert payload["sources"][0]["reason"] == (
            "DURABLE_OFFICIAL_CALENDAR_RESTORED_STALE"
        )
        assert row["event_id"] == EVENT_ID
        assert row["observed_at"] == OBSERVED_AT.isoformat()
        assert row["reaction"]["status"] == "UNAVAILABLE"
        assert row["reaction"]["reasons"] == [
            "OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE"
        ]
        assert row["decision_authority"] == "SUPPORTING_ONLY"
        assert row["approval_eligible"] is False
        assert row["instruction_creation_allowed"] is False
        assert row["order_creation_allowed"] is False
    finally:
        restarted.close()


def _identity(
    *,
    title: str = "Consumer Price Index - July 2026",
    event_id: str = EVENT_ID,
    published_at: datetime | None = PUBLISHED_AT,
) -> ScheduledEventIdentity:
    return ScheduledEventIdentity(
        event_id=event_id,
        official_source=SOURCE,
        official_source_id=SOURCE_ID,
        title=title,
        category="MACRO",
        scheduled_at=SCHEDULED_AT,
        schedule_published_at=published_at,
        schedule_first_seen_at=OBSERVED_AT,
        schedule_observed_at=OBSERVED_AT,
        symbols=("SPY",),
    )


def _scheduled(*, identity: ScheduledEventIdentity | None = None) -> EventReactionLedger:
    event = identity or _identity()
    expectation = ConsensusExpectation(
        event_hash=event.content_hash,
        metric="headline_cpi_mom_pct",
        expected_value=Decimal("0.2"),
        unit="percent",
        provider="consensus-feed",
        source_id="consensus-cpi-2026-07-v3",
        published_at=OBSERVED_AT - timedelta(hours=2),
        first_seen_at=OBSERVED_AT - timedelta(hours=1),
        observed_at=OBSERVED_AT,
        vintage="2026-08-05T12:00:00Z",
    )
    return EventReactionLedger.schedule(
        event,
        expectation,
        recorded_at=OBSERVED_AT,
    )


def _completed() -> EventReactionLedger:
    awaiting = _scheduled().await_release(recorded_at=SCHEDULED_AT)
    release = OfficialRelease(
        event_hash=awaiting.identity.content_hash,
        metric=awaiting.expectation.metric,
        actual_value=Decimal("0.4"),
        unit=awaiting.expectation.unit,
        official_source=SOURCE,
        source_id="cpi-2026-07-initial",
        released_at=SCHEDULED_AT,
        vintage_at=SCHEDULED_AT,
        captured_at=SCHEDULED_AT + timedelta(seconds=5),
    )
    released = awaiting.capture_release(
        release,
        recorded_at=release.captured_at,
    )
    assessed = released.assess_surprise(
        recorded_at=SCHEDULED_AT + timedelta(seconds=6),
        supporting_evidence_hashes=("b" * 64,),
    )
    market = MarketReactionEvidence(
        event_hash=assessed.identity.content_hash,
        release_hash=release.content_hash,
        source="read-only market snapshot",
        window_start=SCHEDULED_AT,
        window_end=SCHEDULED_AT + timedelta(minutes=2),
        evidence_asof=SCHEDULED_AT + timedelta(minutes=2),
        observed_at=SCHEDULED_AT + timedelta(minutes=2, seconds=1),
        metrics={"underlying_return_pct": Decimal("-0.35")},
    )
    observed = assessed.observe_market(market, recorded_at=market.observed_at)
    option = OptionReevaluationEvidence(
        event_hash=observed.identity.content_hash,
        release_hash=release.content_hash,
        market_reaction_hash=market.content_hash,
        option_id="SPY-20260807-500-C",
        candidate_hash="c" * 64,
        source="local deterministic read-only re-evaluator",
        evidence_asof=SCHEDULED_AT + timedelta(minutes=2, seconds=10),
        observed_at=SCHEDULED_AT + timedelta(minutes=2, seconds=11),
        input_evidence_hashes=("d" * 64,),
        result={"research_disposition": "RECONSIDER"},
    )
    return observed.reevaluate_option(option, recorded_at=option.observed_at)


def _official_event_for(
    *,
    observed_at: datetime,
    scheduled_at: datetime,
    event_id: str,
    source_id: str,
    title: str,
    source_payload_hash: str,
    source: str = SOURCE,
    source_url: str = SOURCE_URL,
) -> OfficialCalendarEvent:
    published_at = observed_at - timedelta(days=1)
    provenance = OfficialEventProvenance(
        source=source,
        source_url=source_url,
        source_id=source_id,
        source_payload_hash=source_payload_hash,
        published_at=published_at,
        first_seen_at=observed_at,
        ingested_at=observed_at,
        observed_at=observed_at,
    )
    return OfficialCalendarEvent(
        event_id=event_id,
        source=source,
        source_id=source_id,
        source_url=source_url,
        title=title,
        category="MACRO",
        scheduled_at=scheduled_at,
        published_at=published_at,
        first_seen_at=observed_at,
        ingested_at=observed_at,
        observed_at=observed_at,
        timezone_name="UTC",
        schedule_precision="EXACT",
        symbols=("SPY",),
        provenance=(provenance,),
    )


def _snapshot_for(
    observed_at: datetime,
    events: Iterable[OfficialCalendarEvent],
) -> OfficialCalendarSnapshot:
    checked = tuple(events)
    return OfficialCalendarSnapshot(
        status="READY",
        decision="OBSERVATION_ONLY",
        window_start=observed_at,
        window_end=observed_at + timedelta(days=14),
        observed_at=observed_at,
        events=checked,
        sources=(
            OfficialSourceHealth(
                source=SOURCE,
                source_url=SOURCE_URL,
                status="READY",
                reason=None,
                observed_at=observed_at,
                event_count=len(checked),
            ),
        ),
        reasons=(),
    )


def test_production_reaction_root_matches_whole_second_calendar_projection(
    tmp_path: Path,
) -> None:
    event = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=SCHEDULED_AT,
        event_id="bls-employment-2026-08",
        source_id="bls-release-employment-2026-08",
        title="Employment Situation for August 2026",
        source_payload_hash="b" * 64,
    )
    snapshot = _snapshot_for(OBSERVED_AT, (event,))
    store = ReactionEvidenceStore(tmp_path / "whole-second-reaction.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: OBSERVED_AT,
    )
    runtime = NewsCoordinator(
        tmp_path / "whole-second-news.sqlite3",
        official_calendar_snapshot=snapshot,
        reaction_provider=provider,
        clock=lambda: OBSERVED_AT,
    )
    try:
        provider.restore_official_identities(snapshot.events)
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        row = next(
            item
            for item in payload["calendar"]
            if item["event_id"] == event.event_id
        )
        stable_root = reaction_descriptor_from_calendar(event).stable_event_key

        assert provider.reaction_roots((event.event_id,))[event.event_id] == stable_root
        assert row["reaction_identity_hash"] == stable_root
        assert provider.coverage((event.event_id,))[event.event_id][
            "next_eligible_release_at"
        ] == event.scheduled_at.isoformat()
        assert row["reaction"]["reasons"] == ["WAIT_FOR_DECLARED_RELEASE_TIME"]
        assert payload["reaction_provider"]["status"] == "READY"
        assert payload["reaction_provider"]["reason"] == "NO_ELIGIBLE_REACTION_EVENTS"
        assert payload["reaction_provider"]["next_eligible_release_at"] == (
            event.scheduled_at.isoformat()
        )
    finally:
        runtime.close()
        provider.close()


def _identity_for(event: OfficialCalendarEvent) -> ScheduledEventIdentity:
    assert event.scheduled_at is not None
    assert event.published_at is not None
    return ScheduledEventIdentity(
        event_id=event.event_id,
        official_source=event.source,
        official_source_id=event.source_id,
        title=event.title,
        category=event.category,
        scheduled_at=event.scheduled_at,
        schedule_published_at=event.published_at,
        schedule_first_seen_at=event.first_seen_at,
        schedule_observed_at=event.observed_at,
        symbols=event.symbols,
    )


def _scheduled_for(event: OfficialCalendarEvent) -> EventReactionLedger:
    identity = _identity_for(event)
    expectation = ConsensusExpectation(
        event_hash=identity.content_hash,
        metric=f"metric-{event.event_id}",
        expected_value=Decimal("1.0"),
        unit="percent",
        provider="consensus-feed",
        source_id=f"consensus-{event.event_id}",
        published_at=event.observed_at - timedelta(hours=2),
        first_seen_at=event.observed_at - timedelta(hours=1),
        observed_at=event.observed_at,
        vintage=event.observed_at.isoformat(),
    )
    return EventReactionLedger.schedule(
        identity,
        expectation,
        recorded_at=event.observed_at,
    )


def _completed_for(event: OfficialCalendarEvent) -> EventReactionLedger:
    assert event.scheduled_at is not None
    scheduled = _scheduled_for(event)
    awaiting = scheduled.await_release(recorded_at=event.scheduled_at)
    release = OfficialRelease(
        event_hash=awaiting.identity.content_hash,
        metric=awaiting.expectation.metric,
        actual_value=Decimal("1.5"),
        unit=awaiting.expectation.unit,
        official_source=SOURCE,
        source_id=f"release-{event.event_id}",
        released_at=event.scheduled_at,
        vintage_at=event.scheduled_at,
        captured_at=event.scheduled_at + timedelta(seconds=1),
    )
    released = awaiting.capture_release(release, recorded_at=release.captured_at)
    return released.assess_surprise(
        recorded_at=event.scheduled_at + timedelta(seconds=2),
        supporting_evidence_hashes=("b" * 64,),
    )


def _runtime(
    tmp_path: Path,
    *,
    reaction_provider: object | None,
    current: dict[str, datetime] | None = None,
) -> NewsCoordinator:
    clock = current or {"now": OBSERVED_AT}
    return NewsCoordinator(
        tmp_path / "reaction-runtime.sqlite3",
        official_calendar_snapshot=_official_snapshot(),
        reaction_provider=reaction_provider,  # type: ignore[arg-type]
        clock=lambda: clock["now"],
    )


class _ScopedReactionProvider:
    def __init__(
        self,
        eligible_event_ids: tuple[str, ...],
        ledgers: tuple[EventReactionLedger, ...] = (),
    ) -> None:
        self.eligible_event_ids = eligible_event_ids
        self.ledgers = ledgers
        self.refresh_calls: list[datetime] = []
        self.observe_calls: list[datetime] = []
        self.requested_event_ids: list[tuple[str, ...]] = []
        self.unsupported_count = 0

    def refresh(self, snapshot: OfficialCalendarSnapshot, *, now: datetime) -> None:
        self.refresh_calls.append(now)
        self.unsupported_count = len(snapshot.events) - len(self.eligible_event_ids)

    def observe_local(self, *, now: datetime) -> None:
        self.observe_calls.append(now)

    def projection(self) -> dict[str, object]:
        return {
            "supported_event_ids": list(self.eligible_event_ids),
            "supported_count": len(self.eligible_event_ids),
            "eligible_event_ids": list(self.eligible_event_ids),
            "eligible_count": len(self.eligible_event_ids),
            "unsupported_count": self.unsupported_count,
            "last_attempt": (
                None if not self.refresh_calls else self.refresh_calls[-1].isoformat()
            ),
            "scope_known": True,
            "reason": (
                "NO_ELIGIBLE_REACTION_EVENTS"
                if not self.eligible_event_ids
                else "READY"
            ),
        }

    def reactions(self, event_ids: tuple[str, ...]) -> Iterable[EventReactionLedger]:
        self.requested_event_ids.append(event_ids)
        return self.ledgers


def test_schedule_worker_requires_atomic_refresh_interface(tmp_path: Path) -> None:
    class ScheduleReader:
        def reaction_schedule(self, *, now):
            raise AssertionError("missing atomic provider must prevent source read")

    class LegacyProvider(_ReactionProvider):
        def __init__(self) -> None:
            super().__init__()
            self.legacy_calls = 0
            self.failures: list[tuple[str, str]] = []

        def update_schedule_refresh(self, **_kwargs):
            self.legacy_calls += 1

        def refresh_schedule(self, *_args, **_kwargs):
            self.legacy_calls += 1

        def record_worker_failure(self, lane, *, now, reason):
            self.failures.append((lane, reason))

    reactions = LegacyProvider()
    runtime = NewsCoordinator(
        tmp_path / "atomic-interface-required.sqlite3",
        official_calendar_snapshot=_empty_official_snapshot(OBSERVED_AT),
        reaction_schedule_provider=ScheduleReader(),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )
    try:
        runtime.refresh_reaction_schedule_once()
        assert reactions.legacy_calls == 0
        assert reactions.failures == [
            ("SCHEDULE", "REACTION_ATOMIC_SCHEDULE_REFRESH_UNAVAILABLE")
        ]
    finally:
        runtime.close()


def test_zero_eligible_official_snapshot_is_healthy_idle_not_missing_coverage(
    tmp_path: Path,
) -> None:
    unsupported = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=SCHEDULED_AT,
        event_id="fomc-unsupported",
        source_id="fomc-unsupported",
        title="FOMC rate decision",
        source_payload_hash="d" * 64,
    )
    reactions = _ScopedReactionProvider(())
    runtime = NewsCoordinator(
        tmp_path / "zero-eligible-runtime.sqlite3",
        official_calendar_snapshot=_snapshot_for(OBSERVED_AT, (unsupported,)),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        row = next(item for item in payload["calendar"] if item["event_id"] == unsupported.event_id)
        assert reactions.requested_event_ids == []
        assert row["reaction"]["decision"] == "OBSERVATION_ONLY"  # type: ignore[index]
        assert row["reaction"]["reasons"] == ["REACTION_EVENT_UNSUPPORTED"]  # type: ignore[index]
        assert payload["reaction_provider"]["status"] == "READY"  # type: ignore[index]
        assert payload["reaction_provider"]["decision"] == "OBSERVATION_ONLY"  # type: ignore[index]
        assert payload["reaction_provider"]["reason"] == "NO_ELIGIBLE_REACTION_EVENTS"  # type: ignore[index]
        assert payload["reaction_provider"]["eligible_count"] == 0  # type: ignore[index]
        assert payload["reaction_provider"]["unsupported_count"] == 1  # type: ignore[index]
    finally:
        runtime.close()


def test_mixed_scope_only_requires_supported_cpi_progression(tmp_path: Path) -> None:
    cpi = _official_snapshot().events[0]
    unsupported = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=SCHEDULED_AT + timedelta(hours=1),
        event_id="earnings-unsupported",
        source_id="earnings-unsupported",
        title="Legacy earnings release",
        source_payload_hash="e" * 64,
    )
    reactions = _ScopedReactionProvider((cpi.event_id,), (_scheduled(),))
    runtime = NewsCoordinator(
        tmp_path / "mixed-reaction-scope.sqlite3",
        official_calendar_snapshot=_snapshot_for(OBSERVED_AT, (cpi, unsupported)),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        rows = {row["event_id"]: row for row in payload["calendar"]}
        assert reactions.requested_event_ids == [(cpi.event_id,)]
        assert rows[cpi.event_id]["reaction"]["status"] == "READY"  # type: ignore[index]
        assert rows[unsupported.event_id]["reaction"]["reasons"] == [  # type: ignore[index]
            "REACTION_EVENT_UNSUPPORTED"
        ]
        assert payload["reaction_provider"]["status"] == "READY"  # type: ignore[index]
        assert payload["reaction_provider"]["eligible_count"] == 1  # type: ignore[index]
        assert payload["reaction_provider"]["matched_count"] == 1  # type: ignore[index]
        assert payload["reaction_provider"]["unsupported_count"] == 1  # type: ignore[index]
    finally:
        runtime.close()


def test_eligible_event_without_progression_remains_fail_closed(tmp_path: Path) -> None:
    cpi = _official_snapshot().events[0]
    reactions = _ScopedReactionProvider((cpi.event_id,), ())
    runtime = NewsCoordinator(
        tmp_path / "eligible-missing-progression.sqlite3",
        official_calendar_snapshot=_snapshot_for(OBSERVED_AT, (cpi,)),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        assert payload["reaction_provider"]["status"] == "UNAVAILABLE"  # type: ignore[index]
        assert payload["reaction_provider"]["decision"] == "NO_TRADE"  # type: ignore[index]
        assert payload["reaction_provider"]["reason"] == (  # type: ignore[index]
            "REACTION_LEDGER_COVERAGE_INCOMPLETE"
        )
        assert payload["reaction_provider"]["eligible_count"] == 1  # type: ignore[index]
        assert payload["reaction_provider"]["matched_count"] == 0  # type: ignore[index]
    finally:
        runtime.close()


def test_future_declared_release_waits_without_requiring_a_reaction_ledger(
    tmp_path: Path,
) -> None:
    class DeclaredReleaseProvider(_ScopedReactionProvider):
        def projection(self) -> dict[str, object]:
            return {
                **super().projection(),
                "schedule_refresh_status": "READY",
                "schedule_refresh_reason": "LATEST_GLOBAL_HEALTH",
                "schedule_hash": "d" * 64,
                "next_eligible_release_at": SCHEDULED_AT.isoformat(),
            }

        def coverage(self, event_ids: tuple[str, ...]) -> dict[str, object]:
            assert event_ids == (EVENT_ID,)
            event = _official_snapshot().events[0]
            event_hash = _calendar_reaction_identity_hash(event.as_dict())
            assert event_hash is not None
            return {
                EVENT_ID: {
                    "event_id": EVENT_ID,
                    "event_hash": event_hash,
                    "scheduled_at": SCHEDULED_AT.isoformat(),
                    "family": "CPI",
                    "support_state": "SUPPORTED",
                    "supported": True,
                    "capture_eligible": False,
                    "surprise_eligible": False,
                    "progressed": False,
                    "capture_spec_available": True,
                    "document_stage": "SCHEDULED",
                    "next_action": "WAIT_FOR_DECLARED_RELEASE_TIME",
                    "reason": "WAIT_FOR_DECLARED_RELEASE_TIME",
                    "measure_count": 2,
                    "capture_count": 0,
                    "document_market_reaction": None,
                    "measure_reactions": (),
                    "next_eligible_release_at": SCHEDULED_AT.isoformat(),
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_creation_allowed": False,
                }
            }

    reactions = DeclaredReleaseProvider((EVENT_ID,), ())
    runtime = NewsCoordinator(
        tmp_path / "future-declared-release.sqlite3",
        official_calendar_snapshot=_official_snapshot(),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        row = next(item for item in payload["calendar"] if item["event_id"] == EVENT_ID)
        reaction = row["reaction"]
        provider = payload["reaction_provider"]

        assert reactions.requested_event_ids == []
        assert reaction["current_stage"] is None  # type: ignore[index]
        assert reaction["decision"] == "OBSERVATION_ONLY"  # type: ignore[index]
        assert reaction["reasons"] == ["WAIT_FOR_DECLARED_RELEASE_TIME"]  # type: ignore[index]
        assert reaction["document_progression"] == {  # type: ignore[index]
            "status": "SCHEDULED",
            "capture_count": 0,
            "next_action": "WAIT_FOR_DECLARED_RELEASE_TIME",
            "decision_authority": "SUPPORTING_ONLY",
        }
        assert provider["status"] == "READY"  # type: ignore[index]
        assert provider["reason"] == "NO_ELIGIBLE_REACTION_EVENTS"  # type: ignore[index]
        assert provider["eligible_count"] == 0  # type: ignore[index]
        assert provider["matched_count"] == 0  # type: ignore[index]
        assert provider["next_action"] == "WAIT_FOR_DECLARED_RELEASE_TIME"  # type: ignore[index]
        assert provider["schedule_refresh_status"] == "READY"  # type: ignore[index]
        assert "REACTION_LEDGER_COVERAGE_INCOMPLETE" not in str(payload)
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    (
        ("event_id", "different-event"),
        ("event_hash", "f" * 64),
        ("scheduled_at", (SCHEDULED_AT + timedelta(seconds=1)).isoformat()),
        ("next_eligible_release_at", (SCHEDULED_AT + timedelta(seconds=1)).isoformat()),
        ("supported", False),
        ("capture_eligible", True),
        ("surprise_eligible", True),
        ("progressed", True),
        ("capture_spec_available", False),
        ("capture_count", 1),
        ("document_stage", "WAITING_RELEASE_DOCUMENT"),
        ("measure_reactions", ({"stage": "ACTUAL_CAPTURED"},)),
        ("decision_authority", "PRODUCTION"),
    ),
)
def test_malformed_declared_release_wait_remains_fail_closed(
    tmp_path: Path,
    field: str,
    invalid_value: object,
) -> None:
    class MalformedWaitProvider(_ScopedReactionProvider):
        def coverage(self, event_ids: tuple[str, ...]) -> dict[str, object]:
            assert event_ids == (EVENT_ID,)
            event = _official_snapshot().events[0]
            event_hash = _calendar_reaction_identity_hash(event.as_dict())
            assert event_hash is not None
            body: dict[str, object] = {
                "event_id": EVENT_ID,
                "event_hash": event_hash,
                "scheduled_at": SCHEDULED_AT.isoformat(),
                "family": "CPI",
                "support_state": "SUPPORTED",
                "supported": True,
                "capture_eligible": False,
                "surprise_eligible": False,
                "progressed": False,
                "capture_spec_available": True,
                "document_stage": "SCHEDULED",
                "next_action": "WAIT_FOR_DECLARED_RELEASE_TIME",
                "reason": "WAITING_DECLARED_RELEASE_TIME",
                "measure_count": 2,
                "capture_count": 0,
                "document_market_reaction": None,
                "measure_reactions": (),
                "next_eligible_release_at": SCHEDULED_AT.isoformat(),
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_creation_allowed": False,
            }
            body[field] = invalid_value
            return {EVENT_ID: body}

    reactions = MalformedWaitProvider((EVENT_ID,), ())
    runtime = NewsCoordinator(
        tmp_path / f"malformed-wait-{field}.sqlite3",
        official_calendar_snapshot=_official_snapshot(),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        assert reactions.requested_event_ids == [(EVENT_ID,)]
        assert payload["reaction_provider"]["status"] == "UNAVAILABLE"  # type: ignore[index]
        assert payload["reaction_provider"]["decision"] == "NO_TRADE"  # type: ignore[index]
        assert payload["reaction_provider"]["reason"] == (  # type: ignore[index]
            "REACTION_LEDGER_COVERAGE_INCOMPLETE"
        )
    finally:
        runtime.close()


def test_reaction_refresh_uses_dedicated_tick_outside_news_cadence(
    tmp_path: Path,
) -> None:
    clock = {"now": OBSERVED_AT}

    class OfficialProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls: list[datetime] = []

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls.append(now)
            return _snapshot_for(now, ())

    official = OfficialProvider()
    reactions = _ScopedReactionProvider(())
    runtime = NewsCoordinator(
        tmp_path / "reaction-cached-cadence.sqlite3",
        official_calendar_provider=official,
        reaction_provider=reactions,
        cadence_path=tmp_path / "reaction-cadence.json",
        clock=lambda: clock["now"],
    )
    try:
        runtime.refresh_once()
        runtime.refresh_reaction_once()
        clock["now"] = OBSERVED_AT + timedelta(seconds=5)
        runtime.refresh_once()
        runtime.refresh_reaction_once()
        assert official.calls == [OBSERVED_AT]
        assert reactions.observe_calls == [
            OBSERVED_AT,
            OBSERVED_AT + timedelta(seconds=5),
        ]
        assert reactions.refresh_calls == []
        assert all(
            row["source_id"] != "REACTION"
            for row in runtime.calendar_payload()["source_runtime"]
        )
    finally:
        runtime.close()


def test_five_second_observer_persists_endpoint_before_refresh_lock_and_does_no_provider_io(
    tmp_path: Path,
) -> None:
    current = {"now": SCHEDULED_AT - timedelta(seconds=5)}
    endpoint_seen = threading.Event()

    class QuoteAdapter:
        def cached_reaction_underlying_quotes(self, _symbols):
            if current["now"] >= SCHEDULED_AT + timedelta(minutes=5):
                endpoint_seen.set()
            return tuple(
                SimpleNamespace(
                    symbol=symbol,
                    observed_at=current["now"],
                    bid=Decimal("599"),
                    ask=Decimal("601"),
                )
                for symbol in ("SPY", "QQQ", "IWM", "TLT", "GLD", "UUP")
            )

        def preselections(self):
            return ()

    class ForbiddenJin10:
        calls = 0

        def fetch_calendar(self, _token):
            self.calls += 1
            raise AssertionError("five-second observer called Jin10")

    class ForbiddenSecrets:
        calls = 0

        def get(self, _name):
            self.calls += 1
            raise AssertionError("five-second observer read Jin10 credentials")

    class ForbiddenDocuments:
        calls = 0

        def capture(self, _request, *, now):
            self.calls += 1
            raise AssertionError(f"five-second observer captured a document at {now}")

    class ForbiddenSchedule:
        calls = 0

        def reaction_schedule(self, *, now):
            self.calls += 1
            raise AssertionError(f"five-second observer refreshed schedule at {now}")

    store = ReactionEvidenceStore(tmp_path / "five-second-observer.sqlite3")
    jin10 = ForbiddenJin10()
    secrets = ForbiddenSecrets()
    documents = ForbiddenDocuments()
    schedule = ForbiddenSchedule()
    observer = ProductionReactionObserver(
        QuoteAdapter(),
        SimpleNamespace(ready=True),
        store=store,
    )
    reactions = ProductionMacroReactionProvider(
        store,
        jin10_client=jin10,
        jin10_secret_store=secrets,
        official_actual_provider=object(),
        official_document_provider=documents,
        reaction_observer=observer,
        clock=lambda: current["now"],
    )
    reactions.restore_official_identities(_official_snapshot().events)
    runtime = NewsCoordinator(
        tmp_path / "five-second-news.sqlite3",
        official_calendar_snapshot=_official_snapshot(),
        reaction_schedule_provider=schedule,
        reaction_provider=reactions,
        clock=lambda: current["now"],
    )
    worker = threading.Thread(target=runtime.refresh_reaction_once, daemon=True)
    try:
        runtime.refresh_once()
        runtime.refresh_reaction_once()
        identity = next(iter(reactions._state().identities.values()))
        assert store.load_baseline(
            identity.event_hash,
            scheduled_at=SCHEDULED_AT,
        ) is not None

        current["now"] = SCHEDULED_AT + timedelta(minutes=5)
        with runtime._refresh_lock:
            worker.start()
            assert endpoint_seen.wait(timeout=2)
            worker.join(timeout=0.1)
            assert not worker.is_alive()
            endpoint = store.load_endpoint(
                identity.event_hash,
                scheduled_at=SCHEDULED_AT,
            )
            assert endpoint is not None
            assert endpoint.observed_at == current["now"]
            assert runtime.refresh_reaction_publication_once() is False
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert runtime.refresh_reaction_publication_once() is True
        assert schedule.calls == 0
        assert documents.calls == 0
        assert jin10.calls == 0
        assert secrets.calls == 0
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("poller_name", "refresh_name", "lane", "reason"),
    (
        (
            "_poll_reaction",
            "refresh_reaction_once",
            "observer",
            "REACTION_OBSERVER_POLLER_UNAVAILABLE",
        ),
        (
            "_poll_reaction_capture",
            "refresh_reaction_capture_once",
            "capture",
            "REACTION_CAPTURE_POLLER_UNAVAILABLE",
        ),
        (
            "_poll_reaction_schedule",
            "refresh_reaction_schedule_once",
            "schedule",
            "REACTION_SCHEDULE_POLLER_UNAVAILABLE",
        ),
    ),
)
def test_reaction_poller_failure_remains_visible_when_health_persistence_fails(
    tmp_path: Path,
    monkeypatch,
    poller_name: str,
    refresh_name: str,
    lane: str,
    reason: str,
) -> None:
    store = ReactionEvidenceStore(tmp_path / f"{lane}-poller-health.sqlite3")
    reactions = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: OBSERVED_AT,
    )
    runtime = NewsCoordinator(
        tmp_path / f"{lane}-poller-news.sqlite3",
        official_calendar_snapshot=_empty_official_snapshot(OBSERVED_AT),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )

    def fail_health_write(*_args, **_kwargs):
        raise OSError("simulated worker-health persistence failure")

    def fail_refresh() -> None:
        raise RuntimeError("simulated top-level poller failure")

    original_record_failure = reactions.record_worker_failure

    def record_then_stop(lane_name, *, now, reason):
        original_record_failure(lane_name, now=now, reason=reason)
        runtime._stop.set()

    monkeypatch.setattr(store, "record_worker_failure", fail_health_write)
    monkeypatch.setattr(reactions, "record_worker_failure", record_then_stop)
    monkeypatch.setattr(runtime, refresh_name, fail_refresh)
    runtime._stop.clear()
    try:
        getattr(runtime, poller_name)()
        health = reactions.projection()["worker_health"][lane]
        assert health["status"] == "DEGRADED"
        assert health["reason"] == reason
        assert health["durable"] is False
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("poller_name", "refresh_name", "lane"),
    (
        ("_poll_reaction", "refresh_reaction_once", "observer"),
        ("_poll_reaction_capture", "refresh_reaction_capture_once", "capture"),
        ("_poll_reaction_schedule", "refresh_reaction_schedule_once", "schedule"),
    ),
)
def test_reaction_poller_shutdown_exception_does_not_persist_degraded_health(
    tmp_path: Path,
    monkeypatch,
    poller_name: str,
    refresh_name: str,
    lane: str,
) -> None:
    reactions = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / f"{lane}-shutdown-health.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: OBSERVED_AT,
    )
    runtime = NewsCoordinator(
        tmp_path / f"{lane}-shutdown-news.sqlite3",
        official_calendar_snapshot=_empty_official_snapshot(OBSERVED_AT),
        reaction_provider=reactions,
        clock=lambda: OBSERVED_AT,
    )

    def fail_during_shutdown() -> None:
        runtime._stop.set()
        raise RuntimeError("in-flight shutdown")

    monkeypatch.setattr(runtime, refresh_name, fail_during_shutdown)
    runtime._stop.clear()
    try:
        getattr(runtime, poller_name)()
        assert lane not in reactions.projection()["worker_health"]
        assert reactions._store.worker_failures() == ()
    finally:
        runtime.close()


def test_blocked_reaction_network_work_does_not_delay_news_refresh(
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingReactionProvider(_ScopedReactionProvider):
        def refresh_capture(self, *, now):
            entered.set()
            assert release.wait(timeout=5)

    reactions = BlockingReactionProvider(())
    runtime = _runtime(tmp_path, reaction_provider=reactions)
    worker = threading.Thread(
        target=runtime.refresh_reaction_capture_once,
        daemon=True,
    )
    try:
        runtime.refresh_once()
        worker.start()
        assert entered.wait(timeout=2)

        result = runtime.refresh_once()

        assert result["asof"] == OBSERVED_AT.isoformat()
        assert worker.is_alive()
    finally:
        release.set()
        worker.join(timeout=5)
        runtime.close()
    assert not worker.is_alive()


def test_controlled_official_calendar_descriptor_flows_through_dedicated_tick(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: """
            <html><body><div id="2026" class="panel-collapse collapse in">
            <div class="row fomc-meeting">
            <div class="fomc-meeting__month"><strong>September</strong></div>
            <div class="fomc-meeting__date">15-16</div>
            </div></div></body></html>
        """,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: '{"events":[],"announcement":[]}',
        BEA_RELEASE_DATES_URL: """
            {"file_last_updated":"2026-08-20T12:00:00",
             "Gross Domestic Product":{"release_dates":["2026-09-30T12:30:00Z"]}}
        """,
        BLS_CALENDAR_URL: """
            <html><body><table class="release-list"><tbody>
            <tr class="release-list-odd-row">
            <td class="date-cell"><p>Friday, August 21, 2026</p></td>
            <td class="time-cell"><p>08:30 AM</p></td>
            <td class="desc-cell"><p><strong>Employment Situation</strong> for July 2026</p></td>
            </tr></tbody></table></body></html>
        """,
    }
    official = build_official_calendar_provider(
        transport=lambda url, **_kwargs: payloads[url],
        now=lambda: now,
    )
    store = ReactionEvidenceStore(tmp_path / "controlled-descriptor-reaction.sqlite3")
    reactions = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: now,
    )
    runtime = NewsCoordinator(
        tmp_path / "controlled-descriptor-news.sqlite3",
        official_calendar_provider=official,
        reaction_provider=reactions,
        clock=lambda: now,
    )
    try:
        runtime.refresh_once()
        runtime.refresh_reaction_schedule_once()
        runtime.refresh_reaction_once()

        descriptor = next(
            item
            for item in reactions._descriptors.values()
            if item.family.value == "EMPLOYMENT_SITUATION"
        )
        projection = reactions.projection()
        assert descriptor.reference_period == "2026-07"
        assert descriptor.schedule_precision == "EXACT"
        assert descriptor.calendar_feed_hash != descriptor.identity_hash
        assert descriptor.wait_reason is None
        assert descriptor.event_id in projection["eligible_event_ids"]
        assert projection["schedule_refresh_status"] == "DEGRADED"
        assert projection["schedule_refresh_reason"] == (
            "REACTION_SCHEDULE_SOURCE_DEGRADED"
        )
        assert len(str(projection["schedule_hash"])) == 64
    finally:
        runtime.close()


def test_restart_establishes_current_official_snapshot_before_reusing_cadence(
    tmp_path: Path,
) -> None:
    clock = {"now": OBSERVED_AT}
    official_event = _official_snapshot().events[0]
    cadence_path = tmp_path / "restart-reaction-cadence.json"
    evidence_path = tmp_path / "restart-reaction-evidence.sqlite3"
    reaction_path = tmp_path / "restart-reaction-ledger.sqlite3"

    class OfficialProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls: list[datetime] = []

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls.append(now)
            event = (
                official_event
                if now == OBSERVED_AT
                else _official_event_for(
                    observed_at=now,
                    scheduled_at=official_event.scheduled_at,
                    event_id=official_event.event_id,
                    source_id=official_event.source_id,
                    title=official_event.title,
                    source_payload_hash="a" * 64,
                )
            )
            return _snapshot_for(now, (event,))

    class Secrets:
        def get(self, _name: str) -> str:
            return "opaque-token"

    class Jin10Client:
        def __init__(self) -> None:
            self.calls = 0

        def fetch_calendar(self, _token: str) -> object:
            self.calls += 1
            return SimpleNamespace(
                payload={
                    "status": 200,
                    "data": [
                        {
                            "id": "jin10-cpi-2026-07",
                            "title": "美国7月未季调CPI年率",
                            "pub_time": "2026-08-05 20:01",
                            "consensus": "3.4",
                        }
                    ],
                }
            )

    first_official = OfficialProvider()
    first_client = Jin10Client()
    first_reactions = ProductionMacroReactionProvider(
        ReactionEvidenceStore(reaction_path),
        jin10_client=first_client,
        jin10_secret_store=Secrets(),
        official_actual_provider=object(),
        clock=lambda: clock["now"],
    )
    first = NewsCoordinator(
        evidence_path,
        official_calendar_provider=first_official,
        reaction_provider=first_reactions,
        cadence_path=cadence_path,
        clock=lambda: clock["now"],
    )
    try:
        first.refresh_once()
        first.refresh_reaction_schedule_once()
        first.refresh_reaction_capture_once()
        first.refresh_reaction_once()
        assert first_official.calls == [OBSERVED_AT]
        assert first_client.calls == 1
    finally:
        first.close()
        first_reactions.close()

    clock["now"] = OBSERVED_AT + timedelta(seconds=1)
    second_official = OfficialProvider()
    second_client = Jin10Client()
    second_reactions = ProductionMacroReactionProvider(
        ReactionEvidenceStore(reaction_path),
        jin10_client=second_client,
        jin10_secret_store=Secrets(),
        official_actual_provider=object(),
        clock=lambda: clock["now"],
    )
    second = NewsCoordinator(
        evidence_path,
        official_calendar_provider=second_official,
        reaction_provider=second_reactions,
        cadence_path=cadence_path,
        clock=lambda: clock["now"],
    )
    try:
        second.refresh_once()
        assert second_official.calls == [clock["now"]]
        assert second_client.calls == 0

        second.refresh_once()
        assert second_official.calls == [clock["now"]]

        clock["now"] = OBSERVED_AT + timedelta(seconds=5)
        second.refresh_reaction_once()

        assert second_official.calls == [OBSERVED_AT + timedelta(seconds=1)]
        assert second_client.calls == 0
        projection = second_reactions.projection()
        assert projection["eligible_event_ids"] == [official_event.event_id]
        assert projection["eligible_count"] == 1
        assert projection["last_attempt"] == OBSERVED_AT.isoformat()
        payload = second.calendar_payload()
        source_health = next(
            row
            for row in second.news_payload()["source_health"]
            if row["source"] == "OFFICIAL_CALENDAR"
        )
        assert source_health["status"] == "READY", source_health
        assert source_health["success_count"] == 1
        assert payload["reaction_provider"]["reason"] != (
            "OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE"
        )
        cadence = {
            (str(row["source_id"]), str(row["source_kind"])): row
            for row in payload["source_runtime"]  # type: ignore[union-attr]
        }
        assert cadence[("OFFICIAL_CALENDAR", "OFFICIAL_CALENDAR")][
            "attempt_count"
        ] == 2
        assert ("REACTION", "REACTION") not in cadence
        assert payload["reaction_provider"]["decision_authority"] == "SUPPORTING_ONLY"  # type: ignore[index]
    finally:
        second.close()
        second_reactions.close()


def test_restart_startup_official_failure_stays_fail_closed_and_honors_retry(
    tmp_path: Path,
) -> None:
    clock = {"now": OBSERVED_AT}
    official_event = _official_snapshot().events[0]
    cadence_path = tmp_path / "restart-failure-cadence.json"
    evidence_path = tmp_path / "restart-failure-evidence.sqlite3"

    class ReadyProvider:
        health = "READY"
        health_reason = None

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            return _snapshot_for(now, (official_event,))

    first = NewsCoordinator(
        evidence_path,
        official_calendar_provider=ReadyProvider(),
        cadence_path=cadence_path,
        clock=lambda: clock["now"],
    )
    try:
        first.refresh_once()
    finally:
        first.close()

    class FailingProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls += 1
            raise RuntimeError("redacted restart provider failure")

    clock["now"] = OBSERVED_AT + timedelta(seconds=1)
    provider = FailingProvider()
    reactions = _ReactionProvider((_scheduled_for(official_event),))
    second = NewsCoordinator(
        evidence_path,
        official_calendar_provider=provider,
        reaction_provider=reactions,
        cadence_path=cadence_path,
        clock=lambda: clock["now"],
    )
    try:
        second.refresh_once()
        assert provider.calls == 1
        payload = second.calendar_payload()
        assert payload["decision"] == "NO_TRADE"
        assert payload["reaction_provider"]["reason"] == (
            "OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE"
        )

        clock["now"] = OBSERVED_AT + timedelta(minutes=4, seconds=59)
        second.refresh_once()
        assert provider.calls == 1

        clock["now"] = OBSERVED_AT + timedelta(minutes=5, seconds=2)
        second.refresh_once()
        assert provider.calls == 2
    finally:
        second.close()


def _refresh_row(runtime: NewsCoordinator) -> tuple[dict[str, object], dict[str, object]]:
    runtime.refresh_once()
    payload = runtime.calendar_payload()
    assert payload["count"] == 1
    return payload, payload["calendar"][0]  # type: ignore[index,return-value]


def _assert_authority_never_true(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if any(word in lowered for word in ("approval", "instruction", "order")):
                assert item is not True, f"authority field unexpectedly true: {key}"
            _assert_authority_never_true(item)
    elif isinstance(value, list):
        for item in value:
            _assert_authority_never_true(item)


def test_missing_provider_keeps_official_calendar_fact_and_projects_unavailable(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, reaction_provider=None)
    try:
        payload, row = _refresh_row(runtime)

        assert row["event_id"] == EVENT_ID
        assert row["title"] == "Consumer Price Index - July 2026"
        assert row["provenance"]
        assert row["reaction"] == {
            "status": "UNAVAILABLE",
            "current_stage": None,
            "analysis_available": False,
            "event_id": EVENT_ID,
            "event_hash": row["reaction_identity_hash"],
            "asof": None,
            "head_hash": None,
            "transition_count": 0,
            "expectation": None,
            "release": None,
            "surprise": None,
            "market_reaction": None,
            "option_reevaluation": None,
            "decision": "NO_TRADE",
            "reasons": ["REACTION_PROVIDER_UNCONFIGURED"],
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }
        assert payload["reaction_provider"]["status"] == "UNAVAILABLE"  # type: ignore[index]
        assert payload["reaction_decision"] == "NO_TRADE"
        # Calendar health and reaction availability are separate read-only lanes.
        assert payload["decision"] == "OBSERVATION_ONLY"
    finally:
        runtime.close()


def test_missing_official_published_at_preserves_null_and_binds_reaction_identity(
    tmp_path: Path,
) -> None:
    identity = _identity(published_at=None)
    provider = _ReactionProvider((_scheduled(identity=identity),))
    runtime = NewsCoordinator(
        tmp_path / "missing-published-at.sqlite3",
        official_calendar_snapshot=_official_snapshot(published_at=None),
        reaction_provider=provider,
        clock=lambda: OBSERVED_AT,
    )
    try:
        payload, row = _refresh_row(runtime)

        assert row["published_at"] is None
        assert row["reaction_identity_provenance"]["published_at"] is None  # type: ignore[index]
        assert row["reaction_identity_hash"] != identity.content_hash
        assert row["reaction"]["status"] == "READY"  # type: ignore[index]
        assert row["reaction"]["event_hash"] == row["reaction_identity_hash"]  # type: ignore[index]
        assert row["reaction"]["expectation"]["expected_value"] == "0.2"  # type: ignore[index]
        assert payload["reaction_decision"] == "OBSERVATION_ONLY"
    finally:
        runtime.close()


def test_verified_matching_ledger_merges_compact_supporting_only_read_model(
    tmp_path: Path,
) -> None:
    current = {"now": OBSERVED_AT}
    provider = _ReactionProvider((_scheduled(),))
    runtime = _runtime(tmp_path, reaction_provider=provider, current=current)
    try:
        first_payload, first_row = _refresh_row(runtime)
        first = first_row["reaction"]
        assert first["status"] == "READY"  # type: ignore[index]
        assert first["current_stage"] == "SCHEDULED"  # type: ignore[index]
        assert first["analysis_available"] is False  # type: ignore[index]
        assert first["release"] is None  # type: ignore[index]
        assert first_payload["reaction_provider"]["matched_count"] == 1  # type: ignore[index]

        completed = _completed()
        provider.values = (completed,)
        current["now"] = OBSERVED_AT + timedelta(minutes=4)
        second_payload, second_row = _refresh_row(runtime)
        reaction = second_row["reaction"]

        assert reaction["current_stage"] == "OPTION_REEVALUATED"  # type: ignore[index]
        assert reaction["analysis_available"] is True  # type: ignore[index]
        assert reaction["event_hash"] == second_row["reaction_identity_hash"]  # type: ignore[index]
        assert reaction["head_hash"] == completed.head_hash  # type: ignore[index]
        assert reaction["expectation"]["expected_value"] == "0.2"  # type: ignore[index]
        assert reaction["release"]["actual_value"] == "0.4"  # type: ignore[index]
        assert reaction["release"]["revision"] == 0  # type: ignore[index]
        assert reaction["surprise"]["delta"] == "0.2"  # type: ignore[index]
        assert reaction["market_reaction"]["window_end"] == (  # type: ignore[index]
            SCHEDULED_AT + timedelta(minutes=2)
        ).isoformat()
        assert reaction["option_reevaluation"]["candidate_hash"] == "c" * 64  # type: ignore[index]
        assert "result" not in reaction["option_reevaluation"]  # type: ignore[operator]
        idle = second_payload["reaction_provider"]
        assert idle["status"] == "READY"  # type: ignore[index]
        assert idle["decision"] == "OBSERVATION_ONLY"  # type: ignore[index]
        assert idle["reason"] == "NO_ELIGIBLE_REACTION_EVENTS"  # type: ignore[index]
        assert idle["eligible_count"] == 0  # type: ignore[index]
        _assert_authority_never_true(second_payload)
        assert provider.calls == 2
        assert provider.requested_event_ids == [(EVENT_ID,), (EVENT_ID,)]
    finally:
        runtime.close()


def test_post_event_provider_refresh_keeps_original_reaction_identity_binding(
    tmp_path: Path,
) -> None:
    current = {"now": OBSERVED_AT}
    official = _RollingOfficialProvider()
    reactions = _ReactionProvider((_scheduled(),))
    runtime = NewsCoordinator(
        tmp_path / "post-event-refresh.sqlite3",
        official_calendar_provider=official,
        reaction_provider=reactions,
        clock=lambda: current["now"],
    )
    try:
        _, initial_row = _refresh_row(runtime)
        identity_hash = initial_row["reaction_identity_hash"]
        assert identity_hash != _identity().content_hash

        # Cross the official snapshot TTL and the release time.  The new
        # current-window snapshot is empty, but the durable original calendar
        # fact remains and must still bind the completed historical reaction.
        current["now"] = OBSERVED_AT + timedelta(minutes=16)
        reactions.values = (_completed(),)
        payload, refreshed_row = _refresh_row(runtime)

        assert official.calls == [OBSERVED_AT, current["now"]]
        assert payload["count"] == 1
        assert refreshed_row["event_id"] == EVENT_ID
        assert refreshed_row["reaction_identity_hash"] == identity_hash
        assert refreshed_row["reaction"]["event_hash"] == identity_hash  # type: ignore[index]
        assert refreshed_row["reaction"]["current_stage"] == "OPTION_REEVALUATED"  # type: ignore[index]
        assert refreshed_row["reaction"]["status"] == "READY"  # type: ignore[index]
    finally:
        runtime.close()


def test_same_semantic_official_event_binds_current_snapshot_record_version(
    tmp_path: Path,
) -> None:
    second_observation = OBSERVED_AT + timedelta(minutes=16)
    scheduled_at = second_observation + timedelta(hours=1)
    first_event = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=scheduled_at,
        event_id="same-semantic-current-event",
        source_id="same-semantic-current-source",
        title="Same semantic current release",
        source_payload_hash="7" * 64,
    )
    second_event = _official_event_for(
        observed_at=second_observation,
        scheduled_at=scheduled_at,
        event_id=first_event.event_id,
        source_id=first_event.source_id,
        title=first_event.title,
        source_payload_hash="7" * 64,
    )
    assert first_event.content_hash == second_event.content_hash
    assert first_event.record_hash != second_event.record_hash

    class RollingProvider:
        health = "READY"
        health_reason = None

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            return _snapshot_for(
                now,
                (first_event,) if now == OBSERVED_AT else (second_event,),
            )

    reactions = _ReactionProvider((_scheduled_for(first_event),))
    clock = {"now": OBSERVED_AT}
    runtime = NewsCoordinator(
        tmp_path / "same-semantic-record-version.sqlite3",
        official_calendar_provider=RollingProvider(),
        reaction_provider=reactions,
        clock=lambda: clock["now"],
    )
    try:
        runtime.refresh_once()
        clock["now"] = second_observation
        reactions.values = (_scheduled_for(second_event),)
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        row = next(
            item
            for item in payload["calendar"]
            if item["event_id"] == second_event.event_id
        )

        assert row["content_hash"] == second_event.content_hash
        assert row["record_hash"] == second_event.record_hash
        assert row["reaction_identity_hash"] != _identity_for(
            second_event
        ).content_hash
        assert row["reaction"]["event_hash"] == row["reaction_identity_hash"]
        assert row["reaction"]["status"] == "READY"
        assert row["reaction"]["reasons"] == []
        assert payload["reaction_provider"]["status"] == "READY"
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "provider",
    [
        _ReactionProvider(error=RuntimeError("redacted read failure")),
        _ReactionProvider(({"not": "a ledger"},)),
        _ReactionProvider(()),
    ],
    ids=("provider-error", "non-ledger", "empty-batch"),
)
def test_unavailable_provider_outputs_never_remove_calendar_fact(
    tmp_path: Path,
    provider: _ReactionProvider,
) -> None:
    runtime = _runtime(tmp_path, reaction_provider=provider)
    try:
        payload, row = _refresh_row(runtime)

        assert [item["event_id"] for item in payload["calendar"]] == [EVENT_ID]  # type: ignore[index]
        assert row["reaction"]["status"] == "UNAVAILABLE"  # type: ignore[index]
        assert row["reaction"]["decision"] == "NO_TRADE"  # type: ignore[index]
        assert row["reaction"]["analysis_available"] is False  # type: ignore[index]
        assert row["reaction"]["expectation"] is None  # type: ignore[index]
        assert row["reaction"]["release"] is None  # type: ignore[index]
        assert payload["reaction_decision"] == "NO_TRADE"
        _assert_authority_never_true(payload)
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("ledgers", "reason"),
    [
        (lambda: (_scheduled(), _scheduled()), "REACTION_LEDGER_DUPLICATE"),
        (
            lambda: (_scheduled(identity=_identity(title="Wrong event title")),),
            "REACTION_LEDGER_EVENT_HASH_MISMATCH",
        ),
    ],
    ids=("duplicate", "wrong-hash"),
)
def test_duplicate_or_misbound_batch_is_conflicted_no_trade(
    tmp_path: Path,
    ledgers: object,
    reason: str,
) -> None:
    provider = _ReactionProvider(ledgers())  # type: ignore[operator]
    runtime = _runtime(tmp_path, reaction_provider=provider)
    try:
        payload, row = _refresh_row(runtime)

        assert row["reaction"]["status"] == "CONFLICTED"  # type: ignore[index]
        assert row["reaction"]["decision"] == "NO_TRADE"  # type: ignore[index]
        assert row["reaction"]["reasons"] == [reason]  # type: ignore[index]
        assert payload["reaction_provider"]["status"] == "CONFLICTED"  # type: ignore[index]
        assert payload["reaction_decision"] == "NO_TRADE"
        assert row["title"] == "Consumer Price Index - July 2026"
    finally:
        runtime.close()


def test_historical_ledgers_outside_requested_window_are_ignored(tmp_path: Path) -> None:
    current = _scheduled()
    historical = _scheduled(identity=_identity(event_id="historical-cpi-event"))
    provider = _ReactionProvider((historical, current))
    runtime = _runtime(tmp_path, reaction_provider=provider)
    try:
        payload, row = _refresh_row(runtime)

        assert row["reaction"]["status"] == "READY"  # type: ignore[index]
        assert row["reaction"]["event_id"] == EVENT_ID  # type: ignore[index]
        assert payload["reaction_provider"]["status"] == "READY"  # type: ignore[index]
        assert payload["reaction_provider"]["ledger_count"] == 1  # type: ignore[index]
        assert payload["reaction_provider"]["ignored_count"] == 1  # type: ignore[index]
        assert provider.requested_event_ids == [(EVENT_ID,)]
    finally:
        runtime.close()


@pytest.mark.parametrize("historical_failure", ["duplicate", "tampered"])
def test_bad_historical_batch_does_not_contaminate_current_reaction_health(
    tmp_path: Path,
    historical_failure: str,
) -> None:
    current_now = OBSERVED_AT + timedelta(days=15)
    current_event_id = "bls-current-window-event"
    current_source_id = "bls-current-window-source"
    current_scheduled_at = current_now + timedelta(hours=1)
    current_identity = ScheduledEventIdentity(
        event_id=current_event_id,
        official_source=SOURCE,
        official_source_id=current_source_id,
        title="Current official release",
        category="MACRO",
        scheduled_at=current_scheduled_at,
        schedule_published_at=current_now - timedelta(days=1),
        schedule_first_seen_at=current_now,
        schedule_observed_at=current_now,
        symbols=("SPY",),
    )
    current_expectation = ConsensusExpectation(
        event_hash=current_identity.content_hash,
        metric="current_metric",
        expected_value=Decimal("1.0"),
        unit="percent",
        provider="consensus-feed",
        source_id="current-consensus",
        published_at=current_now - timedelta(hours=2),
        first_seen_at=current_now - timedelta(hours=1),
        observed_at=current_now,
        vintage=current_now.isoformat(),
    )
    current_ledger = EventReactionLedger.schedule(
        current_identity,
        current_expectation,
        recorded_at=current_now,
    )
    current_provenance = OfficialEventProvenance(
        source=SOURCE,
        source_url=SOURCE_URL,
        source_id=current_source_id,
        source_payload_hash="e" * 64,
        published_at=current_now - timedelta(days=1),
        first_seen_at=current_now,
        ingested_at=current_now,
        observed_at=current_now,
    )
    current_event = OfficialCalendarEvent(
        event_id=current_event_id,
        source=SOURCE,
        source_id=current_source_id,
        source_url=SOURCE_URL,
        title="Current official release",
        category="MACRO",
        scheduled_at=current_scheduled_at,
        published_at=current_now - timedelta(days=1),
        first_seen_at=current_now,
        ingested_at=current_now,
        observed_at=current_now,
        timezone_name="UTC",
        schedule_precision="EXACT",
        symbols=("SPY",),
        provenance=(current_provenance,),
    )
    current_snapshot = OfficialCalendarSnapshot(
        status="READY",
        decision="OBSERVATION_ONLY",
        window_start=current_now,
        window_end=current_now + timedelta(days=14),
        observed_at=current_now,
        events=(current_event,),
        sources=(
            OfficialSourceHealth(
                source=SOURCE,
                source_url=SOURCE_URL,
                status="READY",
                reason=None,
                observed_at=current_now,
                event_count=1,
            ),
        ),
        reasons=(),
    )

    class RollingProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime):
            self.calls += 1
            return _official_snapshot() if self.calls == 1 else current_snapshot

    class ScopedReactionProvider:
        def __init__(self) -> None:
            self.requested_event_ids: list[tuple[str, ...]] = []

        def reactions(self, event_ids: tuple[str, ...]):
            self.requested_event_ids.append(event_ids)
            if event_ids == (EVENT_ID,) and len(self.requested_event_ids) == 1:
                return (_completed(),)
            if event_ids == (current_event_id,):
                return (current_ledger,)
            if event_ids == (EVENT_ID,):
                historical = _completed()
                if historical_failure == "tampered":
                    object.__setattr__(
                        historical.transitions[-1],
                        "record_hash",
                        "f" * 64,
                    )
                    return (historical,)
                return (historical, historical)
            raise AssertionError(f"unexpected reaction scope: {event_ids!r}")

    current = {"now": OBSERVED_AT}
    reactions = ScopedReactionProvider()
    runtime = NewsCoordinator(
        tmp_path / f"historical-{historical_failure}.sqlite3",
        official_calendar_provider=RollingProvider(),
        reaction_provider=reactions,
        clock=lambda: current["now"],
    )
    try:
        runtime.refresh_once()
        current["now"] = current_now
        runtime.refresh_once()
        payload = runtime.calendar_payload()

        assert reactions.requested_event_ids == [
            (EVENT_ID,),
            (current_event_id,),
            (EVENT_ID,),
        ]
        assert payload["reaction_provider"]["status"] == "READY"  # type: ignore[index]
        assert payload["reaction_provider"]["ledger_count"] == 1  # type: ignore[index]
        assert payload["reaction_provider"]["matched_count"] == 1  # type: ignore[index]
        assert {row["event_id"] for row in payload["calendar"]} == {
            current_event_id
        }
        current_row = payload["calendar"][0]
        assert current_row["reaction"]["status"] == "READY"  # type: ignore[index]
        assert current_row["reaction"]["event_id"] == current_event_id  # type: ignore[index]
        _assert_authority_never_true(payload)
    finally:
        runtime.close()


def test_tampered_ledger_is_conflicted_and_not_partially_projected(tmp_path: Path) -> None:
    tampered = _scheduled()
    object.__setattr__(tampered.transitions[-1], "record_hash", "f" * 64)
    runtime = _runtime(
        tmp_path,
        reaction_provider=_ReactionProvider((tampered,)),
    )
    try:
        payload, row = _refresh_row(runtime)

        reaction = row["reaction"]
        assert reaction["status"] == "CONFLICTED"  # type: ignore[index]
        assert reaction["reasons"] == ["REACTION_LEDGER_INTEGRITY_FAILED"]  # type: ignore[index]
        assert reaction["expectation"] is None  # type: ignore[index]
        assert reaction["head_hash"] is None  # type: ignore[index]
        assert payload["count"] == 1
        _assert_authority_never_true(payload)
    finally:
        runtime.close()


def test_new_official_snapshot_excludes_deleted_future_event_from_current_batch(
    tmp_path: Path,
) -> None:
    second_observation = OBSERVED_AT + timedelta(minutes=16)
    removed = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=OBSERVED_AT + timedelta(days=1),
        event_id="removed-future-event",
        source_id="removed-future-source",
        title="Removed future official event",
        source_payload_hash="1" * 64,
    )
    current_event = _official_event_for(
        observed_at=second_observation,
        scheduled_at=second_observation + timedelta(days=2),
        event_id="current-future-event",
        source_id="current-future-source",
        title="Current future official event",
        source_payload_hash="2" * 64,
    )

    class RollingProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls += 1
            return (
                _snapshot_for(now, (removed,))
                if self.calls == 1
                else _snapshot_for(now, (current_event,))
            )

    class ScopedReactionProvider:
        def __init__(self) -> None:
            self.requested_event_ids: list[tuple[str, ...]] = []

        def reactions(self, event_ids: tuple[str, ...]) -> Iterable[EventReactionLedger]:
            self.requested_event_ids.append(event_ids)
            if event_ids == (removed.event_id,):
                return (_scheduled_for(removed),)
            if event_ids == (current_event.event_id,):
                return (_scheduled_for(current_event),)
            raise AssertionError(f"unexpected reaction scope: {event_ids!r}")

    clock = {"now": OBSERVED_AT}
    reactions = ScopedReactionProvider()
    runtime = NewsCoordinator(
        tmp_path / "removed-future-current-batch.sqlite3",
        official_calendar_provider=RollingProvider(),
        reaction_provider=reactions,
        clock=lambda: clock["now"],
    )
    try:
        runtime.refresh_once()
        clock["now"] = second_observation
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        rows = {row["event_id"]: row for row in payload["calendar"]}

        assert reactions.requested_event_ids == [
            (removed.event_id,),
            (current_event.event_id,),
        ]
        assert payload["reaction_provider"]["status"] == "READY"  # type: ignore[index]
        assert payload["reaction_provider"]["ledger_count"] == 1  # type: ignore[index]
        assert payload["reaction_provider"]["matched_count"] == 1  # type: ignore[index]
        assert rows[current_event.event_id]["reaction"]["status"] == "READY"  # type: ignore[index]
        removed_reaction = rows[removed.event_id]["reaction"]
        assert removed_reaction["status"] == "UNAVAILABLE"  # type: ignore[index]
        assert removed_reaction["decision"] == "NO_TRADE"  # type: ignore[index]
        assert removed_reaction["reasons"] == [  # type: ignore[index]
            "REACTION_EVENT_NOT_IN_CURRENT_OFFICIAL_SNAPSHOT"
        ]
        _assert_authority_never_true(payload)
    finally:
        runtime.close()


def test_degraded_official_snapshot_isolates_failed_source_from_ready_reactions(
    tmp_path: Path,
) -> None:
    second_observation = OBSERVED_AT + timedelta(minutes=16)
    federal_reserve = "Federal Reserve"
    federal_reserve_url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    first_bls = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=OBSERVED_AT + timedelta(days=1),
        event_id="bls-ready-event",
        source_id="bls-ready-source",
        title="BLS ready event",
        source_payload_hash="4" * 64,
    )
    first_fed = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=OBSERVED_AT + timedelta(days=2),
        event_id="fed-timeout-event",
        source_id="fed-timeout-source",
        title="Federal Reserve timeout event",
        source_payload_hash="5" * 64,
        source=federal_reserve,
        source_url=federal_reserve_url,
    )
    current_bls = _official_event_for(
        observed_at=second_observation,
        scheduled_at=OBSERVED_AT + timedelta(days=1),
        event_id=first_bls.event_id,
        source_id=first_bls.source_id,
        title=first_bls.title,
        source_payload_hash="4" * 64,
    )

    class PartiallyDegradedProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls += 1
            if self.calls == 1:
                return OfficialCalendarSnapshot(
                    status="READY",
                    decision="OBSERVATION_ONLY",
                    window_start=now,
                    window_end=now + timedelta(days=14),
                    observed_at=now,
                    events=(first_bls, first_fed),
                    sources=(
                        OfficialSourceHealth(
                            source=SOURCE,
                            source_url=SOURCE_URL,
                            status="READY",
                            reason=None,
                            observed_at=now,
                            event_count=1,
                        ),
                        OfficialSourceHealth(
                            source=federal_reserve,
                            source_url=federal_reserve_url,
                            status="READY",
                            reason=None,
                            observed_at=now,
                            event_count=1,
                        ),
                    ),
                    reasons=(),
                )
            return OfficialCalendarSnapshot(
                status="DEGRADED",
                decision="NO_TRADE",
                window_start=now,
                window_end=now + timedelta(days=14),
                observed_at=now,
                events=(current_bls,),
                sources=(
                    OfficialSourceHealth(
                        source=SOURCE,
                        source_url=SOURCE_URL,
                        status="READY",
                        reason=None,
                        observed_at=now,
                        event_count=1,
                    ),
                    OfficialSourceHealth(
                        source=federal_reserve,
                        source_url=federal_reserve_url,
                        status="DEGRADED",
                        reason="REQUEST_TIMEOUT",
                        observed_at=now,
                        event_count=0,
                    ),
                ),
                reasons=("FEDERAL_RESERVE:REQUEST_TIMEOUT",),
            )

    class SourceScopedReactions:
        def __init__(self) -> None:
            self.requested_event_ids: list[tuple[str, ...]] = []

        def reactions(self, event_ids: tuple[str, ...]) -> Iterable[EventReactionLedger]:
            self.requested_event_ids.append(event_ids)
            values = {
                first_bls.event_id: _scheduled_for(
                    current_bls if len(self.requested_event_ids) > 1 else first_bls
                ),
                first_fed.event_id: _scheduled_for(first_fed),
            }
            return tuple(values[event_id] for event_id in event_ids)

    clock = {"now": OBSERVED_AT}
    reactions = SourceScopedReactions()
    runtime = NewsCoordinator(
        tmp_path / "degraded-source-isolation.sqlite3",
        official_calendar_provider=PartiallyDegradedProvider(),
        reaction_provider=reactions,
        clock=lambda: clock["now"],
    )
    try:
        runtime.refresh_once()
        clock["now"] = second_observation
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        rows = {row["event_id"]: row for row in payload["calendar"]}

        assert reactions.requested_event_ids == [
            (first_bls.event_id, first_fed.event_id),
            (current_bls.event_id,),
        ]
        assert payload["decision"] == "NO_TRADE"
        assert payload["provider"]["status"] == "DEGRADED"
        assert payload["reaction_provider"]["status"] == "READY"
        assert rows[current_bls.event_id]["reaction"]["status"] == "READY"
        assert rows[first_fed.event_id]["reaction"]["status"] == "UNAVAILABLE"
        assert rows[first_fed.event_id]["reaction"]["reasons"] == [
            "REACTION_EVENT_NOT_IN_CURRENT_OFFICIAL_SNAPSHOT"
        ]
        _assert_authority_never_true(payload)
    finally:
        runtime.close()


def test_failed_official_refresh_keeps_future_last_valid_out_of_current_batch(
    tmp_path: Path,
) -> None:
    second_observation = OBSERVED_AT + timedelta(minutes=16)
    last_valid = _official_event_for(
        observed_at=OBSERVED_AT,
        scheduled_at=OBSERVED_AT + timedelta(days=1),
        event_id="last-valid-future-event",
        source_id="last-valid-future-source",
        title="Last-valid future official event",
        source_payload_hash="3" * 64,
    )

    class FailingProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls += 1
            if self.calls == 1:
                return _snapshot_for(now, (last_valid,))
            raise RuntimeError("secret-bearing official provider failure")

    reactions = _ReactionProvider((_scheduled_for(last_valid),))
    clock = {"now": OBSERVED_AT}
    runtime = NewsCoordinator(
        tmp_path / "failed-refresh-last-valid.sqlite3",
        official_calendar_provider=FailingProvider(),
        reaction_provider=reactions,
        clock=lambda: clock["now"],
    )
    try:
        runtime.refresh_once()
        assert reactions.requested_event_ids == [(last_valid.event_id,)]

        clock["now"] = second_observation
        runtime.refresh_once()
        payload = runtime.calendar_payload()
        row = payload["calendar"][0]

        assert row["event_id"] == last_valid.event_id
        assert reactions.requested_event_ids == [(last_valid.event_id,)]
        assert row["reaction"]["status"] == "UNAVAILABLE"  # type: ignore[index]
        assert row["reaction"]["decision"] == "NO_TRADE"  # type: ignore[index]
        assert row["reaction"]["reasons"] == [  # type: ignore[index]
            "OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE"
        ]
        assert payload["reaction_provider"]["status"] == "UNAVAILABLE"  # type: ignore[index]
        assert payload["reaction_provider"]["decision"] == "NO_TRADE"  # type: ignore[index]
        assert payload["reaction_provider"]["reason"] == (  # type: ignore[index]
            "OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE"
        )
        assert payload["decision"] == "NO_TRADE"
        _assert_authority_never_true(payload)
    finally:
        runtime.close()


def test_failed_official_refresh_retries_after_five_minutes(tmp_path: Path) -> None:
    class RecoveringProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("redacted transient provider failure")
            return _empty_official_snapshot(now)

    provider = RecoveringProvider()
    clock = {"now": OBSERVED_AT}
    runtime = NewsCoordinator(
        tmp_path / "official-retry-five-minutes.sqlite3",
        official_calendar_provider=provider,
        clock=lambda: clock["now"],
    )
    try:
        runtime.refresh_once()
        assert provider.calls == 1
        assert runtime.calendar_payload()["decision"] == "NO_TRADE"

        clock["now"] = OBSERVED_AT + timedelta(minutes=4, seconds=59)
        runtime.refresh_once()
        assert provider.calls == 1

        clock["now"] = OBSERVED_AT + timedelta(minutes=5, seconds=1)
        runtime.refresh_once()
        assert provider.calls == 2
        assert runtime.calendar_payload()["provider"]["status"] == "READY"
    finally:
        runtime.close()


def test_historical_reaction_scope_caps_request_and_retained_completed_rows_at_100(
    tmp_path: Path,
) -> None:
    second_observation = OBSERVED_AT + timedelta(minutes=16)
    historical_events = tuple(
        _official_event_for(
            observed_at=OBSERVED_AT,
            scheduled_at=OBSERVED_AT + timedelta(minutes=1, seconds=index),
            event_id=f"historical-event-{index:03d}",
            source_id=f"historical-source-{index:03d}",
            title=f"Historical event {index:03d}",
            source_payload_hash=f"{index:064x}",
        )
        for index in range(1, 102)
    )
    current_event = _official_event_for(
        observed_at=second_observation,
        scheduled_at=second_observation + timedelta(hours=1),
        event_id="bounded-history-current-event",
        source_id="bounded-history-current-source",
        title="Bounded history current event",
        source_payload_hash="4" * 64,
    )
    completed_by_id = {
        event.event_id: _completed_for(event) for event in historical_events
    }
    current_ledger = _scheduled_for(current_event)

    class RollingProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime) -> OfficialCalendarSnapshot:
            self.calls += 1
            return (
                _snapshot_for(now, historical_events)
                if self.calls == 1
                else _snapshot_for(now, (current_event,))
            )

    class BoundedHistoryProvider:
        def __init__(self) -> None:
            self.requested_event_ids: list[tuple[str, ...]] = []
            self.historical_calls = 0

        def reactions(self, event_ids: tuple[str, ...]) -> Iterable[EventReactionLedger]:
            self.requested_event_ids.append(event_ids)
            if event_ids == (current_event.event_id,):
                return (current_ledger,)
            if len(event_ids) == 100:
                self.historical_calls += 1
                if self.historical_calls == 1:
                    return tuple(completed_by_id.values())
                return tuple(completed_by_id[event_id] for event_id in event_ids)
            return ()

    clock = {"now": OBSERVED_AT}
    reactions = BoundedHistoryProvider()
    runtime = NewsCoordinator(
        tmp_path / "bounded-historical-reactions.sqlite3",
        official_calendar_provider=RollingProvider(),
        reaction_provider=reactions,
        clock=lambda: clock["now"],
    )
    try:
        runtime.refresh_once()
        clock["now"] = second_observation
        runtime.refresh_once()
        failed_history_payload = runtime.calendar_payload()

        historical_requests = [
            event_ids
            for event_ids in reactions.requested_event_ids
            if len(event_ids) == 100
        ]
        assert len(historical_requests) == 1
        assert len(historical_requests[0]) == 100
        assert failed_history_payload["reaction_provider"]["status"] == "READY"  # type: ignore[index]
        assert failed_history_payload["reaction_provider"]["matched_count"] == 1  # type: ignore[index]
        assert [
            row["event_id"] for row in failed_history_payload["calendar"]
        ] == [current_event.event_id]

        clock["now"] = second_observation + timedelta(minutes=1)
        runtime.refresh_once()
        bounded_payload = runtime.calendar_payload()
        completed_history = [
            row
            for row in bounded_payload["calendar"]
            if row["event_id"] != current_event.event_id
            and row["reaction"]["analysis_available"] is True
        ]

        assert reactions.historical_calls == 2
        assert len(reactions.requested_event_ids[-1]) == 100
        assert len(completed_history) == 100
        assert bounded_payload["count"] == 101
        assert bounded_payload["reaction_provider"]["status"] == "READY"  # type: ignore[index]
        assert bounded_payload["reaction_provider"]["matched_count"] == 1  # type: ignore[index]
        _assert_authority_never_true(bounded_payload)
    finally:
        runtime.close()
