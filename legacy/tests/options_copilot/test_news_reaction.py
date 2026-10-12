from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import inspect
import json
from pathlib import Path

import pytest

import options_copilot.news.reaction as reaction_module

from options_copilot.news.reaction import (
    ConsensusExpectation,
    DuplicateReactionError,
    EventReactionLedger,
    MarketReactionEvidence,
    OfficialRelease,
    OptionReevaluationEvidence,
    ReactionAuthorityError,
    ReactionBindingError,
    ReactionIntegrityError,
    ReactionReason,
    ReactionStage,
    ReactionTransitionError,
    ScheduledEventIdentity,
)
from options_copilot.providers.events import NewsAggregator, NewsEvent


UTC = timezone.utc
SCHEDULED_AT = datetime(2026, 8, 7, 14, 0, tzinfo=UTC)
PHASE2_CASES_PATH = (
    Path(__file__).parent / "fixtures" / "phase2_eval" / "candidate_v1" / "cases.json"
)
PHASE2_CASES = json.loads(PHASE2_CASES_PATH.read_text(encoding="utf-8"))


def _phase2_case(case_id: str) -> dict[str, object]:
    selected = [case for case in PHASE2_CASES if case.get("case_id") == case_id]
    assert len(selected) == 1
    return selected[0]


P2_01_CASE = _phase2_case("P2-01")
P2_08_CASE = _phase2_case("P2-08")


def _contract_red(detail: str) -> None:
    assert False, f"PHASE2_EXPECTED_RED:CONSENSUS_CONTRACT {detail}"


def _require_consensus_contract() -> tuple[type[object], object]:
    assessment_type = getattr(reaction_module, "ConsensusAssessment", None)
    assess = getattr(reaction_module, "assess_consensus", None)
    expectation_fields = set(inspect.signature(ConsensusExpectation).parameters)
    release_fields = set(inspect.signature(OfficialRelease).parameters)
    missing: list[str] = []
    if not isinstance(assessment_type, type):
        missing.append("ConsensusAssessment")
    if not callable(assess):
        missing.append("assess_consensus")
    for field_name in ("period", "basis"):
        if field_name not in expectation_fields:
            missing.append(f"ConsensusExpectation.{field_name}")
        if field_name not in release_fields:
            missing.append(f"OfficialRelease.{field_name}")
    if missing:
        _contract_red("missing point-in-time consensus API: " + ",".join(missing))
    assert isinstance(assessment_type, type) and callable(assess)
    return assessment_type, assess


def _phase2_expectation(
    identity: ScheduledEventIdentity,
    *,
    expected_value: Decimal = Decimal("1.10"),
    metric: str = "diluted_eps",
    unit: str = "USD_PER_SHARE",
    period: str = "2026-Q2",
    basis: str = "GAAP",
    observed_at: datetime = SCHEDULED_AT - timedelta(minutes=30),
) -> ConsensusExpectation:
    _require_consensus_contract()
    return ConsensusExpectation(
        event_hash=identity.content_hash,
        metric=metric,
        expected_value=expected_value,
        unit=unit,
        period=period,
        basis=basis,
        provider="point-in-time-consensus",
        source_id="consensus-synx-2026-q2",
        published_at=min(observed_at, SCHEDULED_AT - timedelta(hours=1)),
        first_seen_at=min(observed_at, SCHEDULED_AT - timedelta(minutes=45)),
        observed_at=observed_at,
        vintage="2026-08-07T13:30:00Z",
    )


def _phase2_release(
    identity: ScheduledEventIdentity,
    *,
    actual_value: Decimal = Decimal("1.25"),
    metric: str = "diluted_eps",
    unit: str = "USD_PER_SHARE",
    period: str = "2026-Q2",
    basis: str = "GAAP",
) -> OfficialRelease:
    _require_consensus_contract()
    return OfficialRelease(
        event_hash=identity.content_hash,
        metric=metric,
        actual_value=actual_value,
        unit=unit,
        period=period,
        basis=basis,
        official_source="SEC/XBRL",
        source_id="sec-synx-2026-q2",
        released_at=SCHEDULED_AT,
        vintage_at=SCHEDULED_AT,
        captured_at=SCHEDULED_AT + timedelta(seconds=10),
    )


def _assess_consensus(
    *,
    release: OfficialRelease,
    expectation: ConsensusExpectation | None,
) -> object:
    assessment_type, assess = _require_consensus_contract()
    result = assess(
        release=release,
        expectation=expectation,
        assessed_at=SCHEDULED_AT + timedelta(seconds=11),
    )
    assert isinstance(result, assessment_type)
    return result


def _assessment_document(value: object) -> dict[str, object]:
    method = getattr(value, "as_dict", None)
    assert callable(method)
    payload = method()
    assert isinstance(payload, dict)
    return payload


def _require_precedence_contract() -> None:
    event_fields = set(inspect.signature(NewsEvent).parameters)
    required = {"entity_id", "source_tier", "lineage_id", "evidence_ids"}
    missing = sorted(required - event_fields)
    if missing:
        _contract_red("missing source precedence fields: " + ",".join(missing))


def _source_event(
    *,
    source: str,
    source_id: str,
    source_tier: int,
    lineage_id: str,
    evidence_id: str,
    summary: str,
    url: str,
) -> NewsEvent:
    _require_precedence_contract()
    observed = SCHEDULED_AT - timedelta(minutes=5)
    return NewsEvent(
        event_id=f"evt-{source_id}",
        symbol="SYNX",
        entity_id="public-synthetic-issuer-synx",
        source=source,
        source_id=source_id,
        source_tier=source_tier,
        source_rank=source_tier,
        lineage_id=lineage_id,
        evidence_ids=(evidence_id,),
        headline="Synthetic quarterly earnings result",
        summary=summary,
        url=url,
        published_at=observed - timedelta(minutes=2),
        first_seen_at=observed - timedelta(minutes=1),
        ingested_at=observed,
        observed_at=observed,
    )


def _identity() -> ScheduledEventIdentity:
    return ScheduledEventIdentity(
        event_id="bls-cpi-2026-07",
        official_source="Bureau of Labor Statistics",
        official_source_id="cpi-2026-07",
        title="Consumer Price Index - July 2026",
        category="MACRO",
        scheduled_at=SCHEDULED_AT,
        schedule_published_at=SCHEDULED_AT - timedelta(days=30),
        schedule_first_seen_at=SCHEDULED_AT - timedelta(days=20),
        schedule_observed_at=SCHEDULED_AT - timedelta(hours=1),
        symbols=("SPY",),
    )


def test_schedule_identity_preserves_missing_publication_time_without_fabrication() -> None:
    identity = ScheduledEventIdentity(
        event_id="bls-cpi-2026-07-no-published-at",
        official_source="Bureau of Labor Statistics",
        official_source_id="cpi-2026-07",
        title="Consumer Price Index - July 2026",
        category="MACRO",
        scheduled_at=SCHEDULED_AT,
        schedule_published_at=None,
        schedule_first_seen_at=SCHEDULED_AT - timedelta(days=20),
        schedule_observed_at=SCHEDULED_AT - timedelta(hours=1),
        symbols=("SPY",),
    )

    assert identity.schedule_published_at is None
    assert identity.as_dict()["schedule_published_at"] is None
    assert identity.assert_integrity() is None


def test_schedule_identity_without_publication_time_still_rejects_time_travel() -> None:
    with pytest.raises(ReactionTransitionError, match="known in order"):
        ScheduledEventIdentity(
            event_id="bls-cpi-2026-07-future-observation",
            official_source="Bureau of Labor Statistics",
            official_source_id="cpi-2026-07",
            title="Consumer Price Index - July 2026",
            category="MACRO",
            scheduled_at=SCHEDULED_AT,
            schedule_published_at=None,
            schedule_first_seen_at=SCHEDULED_AT - timedelta(minutes=1),
            schedule_observed_at=SCHEDULED_AT + timedelta(seconds=1),
            symbols=("SPY",),
        )


def _expectation(identity: ScheduledEventIdentity) -> ConsensusExpectation:
    return ConsensusExpectation(
        event_hash=identity.content_hash,
        metric="headline_cpi_mom_pct",
        expected_value=Decimal("0.2"),
        unit="percent",
        provider="consensus-feed",
        source_id="consensus-cpi-2026-07-v3",
        published_at=SCHEDULED_AT - timedelta(hours=3),
        first_seen_at=SCHEDULED_AT - timedelta(hours=2),
        observed_at=SCHEDULED_AT - timedelta(hours=1, minutes=1),
        vintage="2026-08-07T12:59:00Z",
    )


def _scheduled(**kwargs: object) -> EventReactionLedger:
    identity = _identity()
    return EventReactionLedger.schedule(
        identity,
        _expectation(identity),
        recorded_at=SCHEDULED_AT - timedelta(hours=1),
        **kwargs,
    )


def _awaiting(**kwargs: object) -> EventReactionLedger:
    return _scheduled(**kwargs).await_release(recorded_at=SCHEDULED_AT)


def _release(
    ledger: EventReactionLedger,
    *,
    actual: Decimal | None = Decimal("0.4"),
    revision: int = 0,
    vintage_at: datetime | None = None,
    captured_at: datetime | None = None,
    supersedes_hash: str | None = None,
    event_hash: str | None = None,
) -> OfficialRelease:
    vintage = vintage_at or SCHEDULED_AT
    return OfficialRelease(
        event_hash=event_hash or ledger.identity.content_hash,
        metric=ledger.expectation.metric,
        actual_value=actual,
        unit=ledger.expectation.unit,
        official_source=ledger.identity.official_source,
        source_id=f"cpi-2026-07-r{revision}",
        released_at=SCHEDULED_AT,
        vintage_at=vintage,
        captured_at=captured_at or vintage + timedelta(seconds=10),
        revision=revision,
        supersedes_hash=supersedes_hash,
    )


def _released(**kwargs: object) -> EventReactionLedger:
    awaiting = _awaiting(**kwargs)
    release = _release(awaiting)
    return awaiting.capture_release(
        release,
        recorded_at=release.captured_at,
    )


def _assessed(**kwargs: object) -> EventReactionLedger:
    released = _released(**kwargs)
    return released.assess_surprise(
        recorded_at=SCHEDULED_AT + timedelta(seconds=11),
        supporting_evidence_hashes=("a" * 64, "b" * 64),
    )


def _market(ledger: EventReactionLedger) -> MarketReactionEvidence:
    release = ledger.release_chain[-1]
    return MarketReactionEvidence(
        event_hash=ledger.identity.content_hash,
        release_hash=release.content_hash,
        source="read-only consolidated market snapshot",
        window_start=SCHEDULED_AT,
        window_end=SCHEDULED_AT + timedelta(minutes=5),
        evidence_asof=SCHEDULED_AT + timedelta(minutes=5),
        observed_at=SCHEDULED_AT + timedelta(minutes=5, seconds=5),
        metrics={
            "underlying_return_pct": Decimal("-0.35"),
            "atm_iv_change_points": Decimal("-1.2"),
        },
    )


def _market_observed(**kwargs: object) -> EventReactionLedger:
    assessed = _assessed(**kwargs)
    market = _market(assessed)
    return assessed.observe_market(market, recorded_at=market.observed_at)


def _option(ledger: EventReactionLedger) -> OptionReevaluationEvidence:
    release = ledger.release_chain[-1]
    market = ledger.market_reaction
    assert market is not None
    return OptionReevaluationEvidence(
        event_hash=ledger.identity.content_hash,
        release_hash=release.content_hash,
        market_reaction_hash=market.content_hash,
        option_id="SPY-20260807-500-C",
        candidate_hash="c" * 64,
        source="local deterministic read-only re-evaluator",
        evidence_asof=SCHEDULED_AT + timedelta(minutes=5, seconds=10),
        observed_at=SCHEDULED_AT + timedelta(minutes=5, seconds=11),
        input_evidence_hashes=("d" * 64, "e" * 64),
        result={
            "quote_change_pct": Decimal("-8.2"),
            "research_disposition": "RECONSIDER",
        },
    )


def test_happy_path_is_immutable_hash_chained_and_supporting_only() -> None:
    scheduled = _scheduled()
    awaiting = scheduled.await_release(recorded_at=SCHEDULED_AT)
    release = _release(awaiting)
    released = awaiting.capture_release(release, recorded_at=release.captured_at)
    assessed = released.assess_surprise(
        recorded_at=SCHEDULED_AT + timedelta(seconds=11),
        supporting_evidence_hashes=("a" * 64,),
    )
    market = _market(assessed)
    observed = assessed.observe_market(market, recorded_at=market.observed_at)
    option = _option(observed)
    completed = observed.reevaluate_option(option, recorded_at=option.observed_at)

    assert scheduled.current_stage is ReactionStage.SCHEDULED
    assert [item.stage for item in completed.transitions] == [
        ReactionStage.SCHEDULED,
        ReactionStage.AWAITING_RELEASE,
        ReactionStage.RELEASE_CAPTURED,
        ReactionStage.SURPRISE_ASSESSED,
        ReactionStage.MARKET_REACTION_OBSERVED,
        ReactionStage.OPTION_REEVALUATED,
    ]
    assert all(
        record.prior_hash
        == ("0" * 64 if index == 0 else completed.transitions[index - 1].record_hash)
        for index, record in enumerate(completed.transitions)
    )
    assert completed.surprise is not None
    assert completed.surprise.delta == Decimal("0.2")
    assert completed.surprise.expectation_hash == completed.expectation.content_hash
    assert completed.surprise.release_hash == release.content_hash
    assert completed.verify_integrity() is True

    payload = completed.as_dict()
    assert payload["decision"] == "OBSERVATION_ONLY"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_creation_allowed"] is False
    assert payload["head_hash"] == completed.head_hash


def test_release_revision_chain_binds_vintages_and_latest_actual() -> None:
    awaiting = _awaiting()
    initial = _release(awaiting, actual=Decimal("0.3"))
    revised = _release(
        awaiting,
        actual=Decimal("0.4"),
        revision=1,
        vintage_at=SCHEDULED_AT + timedelta(minutes=30),
        captured_at=SCHEDULED_AT + timedelta(minutes=30, seconds=5),
        supersedes_hash=initial.content_hash,
    )

    released = awaiting.capture_release(
        (initial, revised),
        recorded_at=revised.captured_at,
    )
    assessed = released.assess_surprise(
        recorded_at=revised.captured_at,
    )

    assert released.current_stage is ReactionStage.RELEASE_CAPTURED
    assert released.transitions[-1].evidence_hashes == (
        initial.content_hash,
        revised.content_hash,
    )
    assert assessed.surprise is not None
    assert assessed.surprise.actual_value == Decimal("0.4")
    assert assessed.surprise.release_hash == revised.content_hash


def test_time_travel_and_duplicate_transition_are_rejected_without_mutation() -> None:
    scheduled = _scheduled()
    with pytest.raises(ReactionTransitionError, match="unreached release time"):
        scheduled.await_release(recorded_at=SCHEDULED_AT - timedelta(microseconds=1))
    assert scheduled.current_stage is ReactionStage.SCHEDULED

    awaiting = scheduled.await_release(recorded_at=SCHEDULED_AT)
    with pytest.raises(DuplicateReactionError, match="duplicate AWAITING_RELEASE"):
        awaiting.await_release(recorded_at=SCHEDULED_AT)
    assert len(awaiting.transitions) == 2


def test_duplicate_release_and_canonical_tamper_are_rejected() -> None:
    awaiting = _awaiting()
    release = _release(awaiting)
    with pytest.raises(DuplicateReactionError, match="duplicate release_hash"):
        awaiting.capture_release(
            (release, release),
            recorded_at=release.captured_at,
        )

    with pytest.raises(ReactionIntegrityError, match="content hash"):
        replace(
            release,
            actual_value=Decimal("99"),
            content_hash=release.content_hash,
        )


def test_missing_official_actual_is_terminal_degraded_no_trade() -> None:
    awaiting = _awaiting()
    missing = _release(awaiting, actual=None)
    degraded = awaiting.capture_release(missing, recorded_at=missing.captured_at)

    assert degraded.current_stage is ReactionStage.DEGRADED
    assert degraded.terminal is True
    assert degraded.decision == "NO_TRADE"
    assert degraded.transitions[-1].reasons == (
        ReactionReason.MISSING_OFFICIAL_ACTUAL,
    )
    with pytest.raises(ReactionTransitionError, match="terminal"):
        degraded.assess_surprise(recorded_at=missing.captured_at)


def test_conflicting_same_vintage_official_releases_fail_closed() -> None:
    awaiting = _awaiting()
    first = _release(awaiting, actual=Decimal("0.3"))
    second = _release(awaiting, actual=Decimal("0.5"))

    conflicted = awaiting.capture_release(
        (first, second),
        recorded_at=second.captured_at,
    )

    assert conflicted.current_stage is ReactionStage.CONFLICTED
    assert conflicted.decision == "NO_TRADE"
    assert conflicted.transitions[-1].reasons == (
        ReactionReason.OFFICIAL_RELEASE_CONFLICT,
    )
    assert set(conflicted.transitions[-1].evidence_hashes) == {
        first.content_hash,
        second.content_hash,
    }


def test_wrong_event_release_is_rejected_as_a_binding_error() -> None:
    awaiting = _awaiting()
    wrong = _release(awaiting, event_hash="f" * 64)
    with pytest.raises(ReactionBindingError, match="different event"):
        awaiting.capture_release(wrong, recorded_at=wrong.captured_at)


def test_stale_market_evidence_is_terminal_no_trade() -> None:
    assessed = _assessed(max_market_evidence_age=timedelta(minutes=1))
    market = _market(assessed)
    stale = assessed.observe_market(
        market,
        recorded_at=market.evidence_asof + timedelta(minutes=1, microseconds=1),
    )

    assert stale.current_stage is ReactionStage.NO_TRADE
    assert stale.transitions[-1].reasons == (
        ReactionReason.STALE_MARKET_EVIDENCE,
    )
    assert stale.market_reaction == market


def test_stale_option_evidence_is_terminal_no_trade() -> None:
    observed = _market_observed(max_option_evidence_age=timedelta(minutes=1))
    option = _option(observed)
    stale = observed.reevaluate_option(
        option,
        recorded_at=option.evidence_asof + timedelta(minutes=1, microseconds=1),
    )

    assert stale.current_stage is ReactionStage.NO_TRADE
    assert stale.transitions[-1].reasons == (
        ReactionReason.STALE_OPTION_EVIDENCE,
    )
    assert stale.option_reevaluation == option


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        ("market", {"approval_eligible": True}),
        ("market", {"decision_authority": "ELIGIBILITY"}),
        ("option", {"order_creation_allowed": True}),
    ],
)
def test_supporting_payloads_cannot_claim_approval_instruction_or_order_authority(
    kind: str,
    payload: dict[str, object],
) -> None:
    observed = _market_observed()
    with pytest.raises(ReactionAuthorityError):
        if kind == "market":
            MarketReactionEvidence(
                event_hash=observed.identity.content_hash,
                release_hash=observed.release_chain[-1].content_hash,
                source="LLM/news supporting narrative",
                window_start=SCHEDULED_AT,
                window_end=SCHEDULED_AT + timedelta(minutes=5),
                evidence_asof=SCHEDULED_AT + timedelta(minutes=5),
                observed_at=SCHEDULED_AT + timedelta(minutes=5, seconds=1),
                metrics=payload,
            )
        else:
            market = observed.market_reaction
            assert market is not None
            OptionReevaluationEvidence(
                event_hash=observed.identity.content_hash,
                release_hash=observed.release_chain[-1].content_hash,
                market_reaction_hash=market.content_hash,
                option_id="SPY-option",
                candidate_hash="c" * 64,
                source="LLM supporting explanation",
                evidence_asof=SCHEDULED_AT + timedelta(minutes=6),
                observed_at=SCHEDULED_AT + timedelta(minutes=6),
                input_evidence_hashes=("d" * 64,),
                result=payload,
            )


@pytest.mark.parametrize(
    ("actual_value", "expected_value", "expected_label"),
    [
        (Decimal("1.25"), Decimal("1.10"), "BEAT"),
        (Decimal("0.95"), Decimal("1.10"), "MISS"),
        (Decimal("1.10"), Decimal("1.10"), "IN_LINE"),
    ],
)
def test_consensus_comparable_pre_event_values_use_decimal_and_exact_labels(
    actual_value: Decimal,
    expected_value: Decimal,
    expected_label: str,
) -> None:
    identity = _identity()
    expectation = _phase2_expectation(identity, expected_value=expected_value)
    release = _phase2_release(identity, actual_value=actual_value)

    assessment = _assess_consensus(release=release, expectation=expectation)
    payload = _assessment_document(assessment)

    assert P2_01_CASE["case_id"] == "P2-01"
    assert P2_01_CASE["details"]["consensus_value"] == "1.10"
    assert assessment.label == expected_label
    assert assessment.reason is None
    assert assessment.comparable is True
    assert assessment.actual_value == actual_value
    assert assessment.consensus_value == expected_value
    assert isinstance(assessment.actual_value, Decimal)
    assert isinstance(assessment.consensus_value, Decimal)
    assert assessment.actual_evidence_hash == release.content_hash
    assert assessment.consensus_evidence_hash == expectation.content_hash
    assert assessment.consensus_observed_at == expectation.observed_at
    assert assessment.release_at == release.released_at
    assert assessment.consensus_observed_at.tzinfo is not None
    assert assessment.release_at.tzinfo is not None
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_creation_allowed"] is False


@pytest.mark.parametrize(
    ("observed_at", "vintage"),
    [
        (SCHEDULED_AT, "release-time-consensus"),
        (SCHEDULED_AT + timedelta(minutes=30), "post-release-revision"),
    ],
)
def test_consensus_observed_at_or_after_release_is_not_point_in_time(
    observed_at: datetime,
    vintage: str,
) -> None:
    identity = _identity()
    expectation = replace(
        _phase2_expectation(identity, observed_at=observed_at),
        vintage=vintage,
        content_hash="",
    )
    release = _phase2_release(identity)

    assessment = _assess_consensus(release=release, expectation=expectation)
    rendered = json.dumps(_assessment_document(assessment), sort_keys=True).upper()

    assert assessment.label == "UNCERTAIN"
    assert assessment.reason == "CONSENSUS_NOT_POINT_IN_TIME"
    assert assessment.comparable is False
    assert '"BEAT"' not in rendered
    assert '"MISS"' not in rendered
    assert "A_GRADE" not in rendered
    assert "A-GRADE" not in rendered


def test_consensus_missing_is_explicit_uncertain_read_model() -> None:
    identity = _identity()
    release = _phase2_release(identity)

    assessment = _assess_consensus(release=release, expectation=None)
    payload = _assessment_document(assessment)
    rendered = json.dumps(payload, sort_keys=True).upper()

    assert assessment.label == "UNCERTAIN"
    assert assessment.reason == "CONSENSUS_NOT_POINT_IN_TIME"
    assert assessment.comparable is False
    assert assessment.consensus_evidence_hash is None
    assert assessment.consensus_observed_at is None
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert '"BEAT"' not in rendered
    assert '"MISS"' not in rendered
    assert "A_GRADE" not in rendered


@pytest.mark.parametrize(
    ("mismatch", "expectation_value", "release_value"),
    [
        ("metric", "diluted_eps", "revenue"),
        ("unit", "USD_PER_SHARE", "USD_MILLIONS"),
        ("period", "2026-Q2", "2026-Q3"),
        ("basis", "GAAP", "ADJUSTED"),
    ],
)
def test_consensus_metric_unit_period_and_basis_mismatch_is_not_comparable(
    mismatch: str,
    expectation_value: str,
    release_value: str,
) -> None:
    identity = _identity()
    expectation_kwargs = {mismatch: expectation_value}
    release_kwargs = {mismatch: release_value}
    expectation = _phase2_expectation(identity, **expectation_kwargs)
    release = _phase2_release(identity, **release_kwargs)

    assessment = _assess_consensus(release=release, expectation=expectation)
    rendered = json.dumps(_assessment_document(assessment), sort_keys=True).upper()

    assert assessment.label == "UNCERTAIN"
    assert assessment.reason == "CONSENSUS_NOT_COMPARABLE"
    assert assessment.comparable is False
    assert '"BEAT"' not in rendered
    assert '"MISS"' not in rendered
    assert "A_GRADE" not in rendered


def test_consensus_assessment_is_frozen_and_canonical_supporting_only() -> None:
    identity = _identity()
    assessment = _assess_consensus(
        release=_phase2_release(identity),
        expectation=_phase2_expectation(identity),
    )
    payload = _assessment_document(assessment)

    with pytest.raises((FrozenInstanceError, AttributeError)):
        assessment.label = "MISS"
    assert payload["content_hash"] == assessment.content_hash
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_creation_allowed"] is False
    assert "risk_tier" not in payload
    assert "reviewability" not in payload


@pytest.mark.parametrize(
    ("official_source", "official_tier"),
    [("SEC/XBRL", 0), ("Company IR", 1)],
)
def test_precedence_official_fact_remains_primary_and_conflict_is_visible(
    official_source: str,
    official_tier: int,
) -> None:
    official = _source_event(
        source=official_source,
        source_id=f"official-{official_tier}",
        source_tier=official_tier,
        lineage_id=f"official-lineage-{official_tier}",
        evidence_id=f"official-evidence-{official_tier}",
        summary="Official reported value was 500 USD millions.",
        url="https://www.sec.gov/Archives/synthetic-report",
    )
    secondary = _source_event(
        source="Secondary Summary",
        source_id=f"secondary-{official_tier}",
        source_tier=2,
        lineage_id=f"secondary-lineage-{official_tier}",
        evidence_id=f"secondary-evidence-{official_tier}",
        summary="Secondary summary claimed 550 USD millions.",
        url="https://example.invalid/secondary-summary",
    )

    merged = NewsAggregator.merge((secondary, official))
    primary = min(merged, key=lambda event: event.source_tier)
    evidence_ids = {
        evidence_id for event in merged for evidence_id in event.evidence_ids
    }

    assert primary.source == official_source
    assert primary.summary == official.summary
    assert all(event.status == "CONFLICTED" for event in merged)
    assert evidence_ids == {
        f"official-evidence-{official_tier}",
        f"secondary-evidence-{official_tier}",
    }
    assert all(event.decision_authority == "SUPPORTING_ONLY" for event in merged)


def test_precedence_duplicate_syndication_collapses_to_one_lineage() -> None:
    first = _source_event(
        source="Syndication A",
        source_id="syndication-a",
        source_tier=2,
        lineage_id="public-synthetic-lineage-07",
        evidence_id="syndication-evidence-a",
        summary="Issuer release repeated verbatim.",
        url="https://example.invalid/syndicated-release",
    )
    second = _source_event(
        source="Syndication B",
        source_id="syndication-b",
        source_tier=2,
        lineage_id="public-synthetic-lineage-07",
        evidence_id="syndication-evidence-b",
        summary="Issuer release repeated verbatim.",
        url="https://example.invalid/syndicated-release",
    )

    merged = NewsAggregator.merge((first, second))

    assert len(merged) == 1
    assert merged[0].lineage_id == "public-synthetic-lineage-07"
    assert set(merged[0].evidence_ids) == {
        "syndication-evidence-a",
        "syndication-evidence-b",
    }
    assert merged[0].status == "ACTIVE"


def test_conflict_independent_secondary_sources_retain_every_evidence_id() -> None:
    evidence_id = P2_08_CASE["evidence"]["evidence_id"]
    first = _source_event(
        source="Independent Secondary A",
        source_id="independent-a",
        source_tier=2,
        lineage_id="independent-lineage-a",
        evidence_id=evidence_id,
        summary="Independent source A reported a positive revision.",
        url="https://example.invalid/independent-a",
    )
    second = _source_event(
        source="Independent Secondary B",
        source_id="independent-b",
        source_tier=2,
        lineage_id="independent-lineage-b",
        evidence_id="phase2-p2-08-conflicting-evidence",
        summary="Independent source B reported a negative revision.",
        url="https://example.invalid/independent-b",
    )

    merged = NewsAggregator.merge((first, second))
    evidence_ids = {
        item for event in merged for item in event.evidence_ids
    }

    assert P2_08_CASE["case_id"] == "P2-08"
    assert P2_08_CASE["expected"]["code"] == "CONFLICTED_UNCERTAIN"
    assert len(merged) == 2
    assert {event.status for event in merged} == {"CONFLICTED"}
    assert {event.lineage_id for event in merged} == {
        "independent-lineage-a",
        "independent-lineage-b",
    }
    assert evidence_ids == {
        evidence_id,
        "phase2-p2-08-conflicting-evidence",
    }
    assert all(event.decision_authority == "SUPPORTING_ONLY" for event in merged)
