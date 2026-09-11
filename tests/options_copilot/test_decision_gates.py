from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from options_copilot.decision import (
    CandidateGateResult,
    GateAuthority,
    GateBundle,
    GateId,
    GateLayerResult,
    GateRoutingContext,
    GateStatus,
    PipelineGateOutcome,
    ProvisionalWatchGatePreview,
    RankingGateContext,
    SourceComponentStatus,
    SupportingInput,
    SupportingStatus,
    WatchLayerPreview,
    candidate_gate_key,
    classify_source_availability,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)
LATER = NOW + timedelta(seconds=15)


def _hash(label: str) -> str:
    return canonical_hash({"label": label})


def _candidate_key(candidate_id: str) -> str:
    return candidate_gate_key(candidate_id, _hash(f"proposal-{candidate_id}"))


def _routing_gate(
    *,
    source: str = "regime",
    allowed: tuple[str, ...] = ("vertical_debit", "defined_risk_credit"),
    blocked: tuple[str, ...] = (),
) -> GateLayerResult:
    return GateLayerResult.build(
        gate_id=GateId.MARKET_CREDIT_REGIME,
        status=GateStatus.PASS,
        observed_at=NOW,
        input_payload={"regime": "NORMAL", "vix_state": "MID"},
        source_hashes=(_hash(source),),
        allowed_strategy_families=allowed,
        blocked_strategy_families=blocked,
    )


def _routing_context(gate: GateLayerResult | None = None) -> GateRoutingContext:
    return GateRoutingContext.build(
        scan_run_id="scan-20260810-001",
        cutoff_at=NOW,
        valid_until=LATER,
        market_credit_gate=gate or _routing_gate(),
    )


def _gate_1(proposal_hash: str, *, status: GateStatus = GateStatus.PASS) -> GateLayerResult:
    bindings = {
        "broker_snapshot_hash": _hash("broker"),
        "strategy_nav_authority_hash": _hash("nav-authority"),
        "strategy_nav_content_hash": _hash("nav-content"),
        "strategy_nav_contract_hash": _hash("nav-contract"),
        "strategy_nav_ledger_head_hash": _hash("nav-ledger"),
        "proposal_hash": proposal_hash,
    }
    return GateLayerResult.build(
        gate_id=GateId.AUTHORITY_DATA,
        status=status,
        observed_at=NOW,
        input_payload={"existing_broker_authority": bindings},
        reason_codes=() if status is GateStatus.PASS else ("BROKER_STALE",),
        source_hashes=tuple(bindings.values()),
        bindings=bindings,
    )


def _hard_layer(
    gate_id: GateId,
    *,
    status: GateStatus = GateStatus.PASS,
    supporting_inputs: tuple[SupportingInput, ...] = (),
    allowed: tuple[str, ...] = (),
    blocked: tuple[str, ...] | None = None,
) -> GateLayerResult:
    resolved_blocked = (
        (("DEFINED_RISK_CREDIT",) if status is GateStatus.BLOCK else ())
        if blocked is None
        else blocked
    )
    return GateLayerResult.build(
        gate_id=gate_id,
        status=status,
        observed_at=NOW,
        input_payload={"existing_authority_decision": gate_id.value, "status": status.value},
        reason_codes=() if status is GateStatus.PASS else ("EXISTING_AUTHORITY_REJECTED",),
        source_hashes=(_hash(gate_id.value),),
        allowed_strategy_families=allowed,
        blocked_strategy_families=resolved_blocked,
        supporting_inputs=supporting_inputs,
    )


def _ranking(
    *,
    rank: int | None,
    total: int,
    consistent: bool = True,
    ordered_candidate_keys: tuple[str, ...] | None = None,
) -> RankingGateContext:
    pre_gate_rank_order_payload = (
        None
        if rank is None
        else {
            "schema": "options_copilot.pre_gate_rank_order.v1",
            "ordered_candidate_keys": ordered_candidate_keys
            or tuple(_hash(f"rank-slot-{index}") for index in range(1, total + 1)),
            "score_inputs_hash": _hash("pre-gate-score-inputs"),
        }
    )
    return RankingGateContext.build(
        rank=rank,
        total_ranked_count=total,
        pre_gate_rank_order_payload=pre_gate_rank_order_payload,
        authority_consistent=consistent,
        reason_codes=("VIEW_ONLY",) if rank is not None and rank > 1 else (),
    )


def _gate_6(ranking: RankingGateContext, *, status: GateStatus | None = None) -> GateLayerResult:
    resolved = status or (GateStatus.PASS if ranking.rank is not None else GateStatus.UNAVAILABLE)
    return GateLayerResult.build(
        gate_id=GateId.RANKING_REVIEWABILITY,
        status=resolved,
        observed_at=NOW,
        input_payload=ranking.hash_payload(),
        reason_codes=ranking.reason_codes or (() if resolved is GateStatus.PASS else ("NOT_RANKED",)),
        source_hashes=()
        if ranking.pre_gate_rank_order_hash is None
        else (ranking.pre_gate_rank_order_hash,),
        bindings={
            "pre_gate_rank_order_hash": ranking.pre_gate_rank_order_hash,
            "rank_authority_context_hash": ranking.context_hash,
        },
    )


def _candidate(
    candidate_id: str,
    *,
    rank: int | None,
    total: int,
    failed_gate: GateId | None = None,
    gate_3_supporting: tuple[SupportingInput, ...] = (),
    routing_gate: GateLayerResult | None = None,
    strategy_family: str = "VERTICAL_DEBIT",
    ordered_candidate_keys: tuple[str, ...] | None = None,
    gate_3_allowed: tuple[str, ...] = (),
    gate_3_blocked: tuple[str, ...] | None = None,
    ranking_consistent: bool | None = None,
) -> CandidateGateResult:
    proposal_hash = _hash(f"proposal-{candidate_id}")
    ranking = _ranking(
        rank=rank,
        total=total,
        consistent=(rank is not None if ranking_consistent is None else ranking_consistent),
        ordered_candidate_keys=(
            ordered_candidate_keys
            if ordered_candidate_keys is not None
            else ((_candidate_key(candidate_id),) if rank is not None and total == 1 else None)
        ),
    )
    layers = (
        _gate_1(
            proposal_hash,
            status=GateStatus.UNAVAILABLE if failed_gate is GateId.AUTHORITY_DATA else GateStatus.PASS,
        ),
        routing_gate or _routing_gate(),
        _hard_layer(
            GateId.UNDERLYING_EVENT,
            status=GateStatus.BLOCK if failed_gate is GateId.UNDERLYING_EVENT else GateStatus.PASS,
            supporting_inputs=gate_3_supporting,
            allowed=gate_3_allowed,
            blocked=gate_3_blocked,
        ),
        _hard_layer(
            GateId.OPTION_EDGE_LIQUIDITY,
            status=GateStatus.BLOCK if failed_gate is GateId.OPTION_EDGE_LIQUIDITY else GateStatus.PASS,
        ),
        _hard_layer(
            GateId.STRUCTURE_ACCOUNT_RISK,
            status=GateStatus.BLOCK if failed_gate is GateId.STRUCTURE_ACCOUNT_RISK else GateStatus.PASS,
        ),
        _gate_6(ranking),
    )
    return CandidateGateResult.build(
        candidate_id=candidate_id,
        candidate_hash=_hash(f"candidate-{candidate_id}"),
        proposal_hash=proposal_hash,
        strategy_family=strategy_family,
        layers=reversed(layers),
        ranking=ranking,
    )


def test_candidate_key_is_exact_approved_canonical_identity() -> None:
    proposal_hash = _hash("proposal")

    assert candidate_gate_key("candidate-1", proposal_hash) == canonical_hash(
        {"candidate_id": "candidate-1", "proposal_hash": proposal_hash},
    )


def test_gate_layer_sorts_collections_hashes_and_is_frozen() -> None:
    proposal_hash = _hash("proposal")
    layer = GateLayerResult.build(
        gate_id=GateId.AUTHORITY_DATA,
        status=GateStatus.PASS,
        observed_at=NOW,
        input_payload={"z": 1, "a": 2},
        reason_codes=("z_reason", "a_reason", "z_reason"),
        source_hashes=(_hash("z"), _hash("a"), _hash("z")),
        allowed_strategy_families=("vertical", "calendar"),
        bindings={
            "proposal_hash": proposal_hash,
            "broker_snapshot_hash": _hash("broker"),
            "strategy_nav_authority_hash": _hash("nav-authority"),
            "strategy_nav_content_hash": _hash("nav-content"),
            "strategy_nav_contract_hash": _hash("nav-contract"),
            "strategy_nav_ledger_head_hash": _hash("nav-ledger"),
        },
    )

    assert layer.reason_codes == ("A_REASON", "Z_REASON")
    assert layer.source_hashes == tuple(sorted({_hash("a"), _hash("z")}))
    assert layer.allowed_strategy_families == ("CALENDAR", "VERTICAL")
    assert canonical_hash(layer.hash_payload()) == layer.gate_hash
    with pytest.raises(FrozenInstanceError):
        layer.status = GateStatus.BLOCK  # type: ignore[misc]


def test_two_phase_bundle_copies_route_and_represents_trade_plus_exclusion() -> None:
    route_gate = _routing_gate()
    route = _routing_context(route_gate)
    ranked = _candidate("ranked", rank=1, total=1, routing_gate=route_gate)
    excluded = _candidate(
        "excluded",
        rank=None,
        total=1,
        failed_gate=GateId.OPTION_EDGE_LIQUIDITY,
        routing_gate=route_gate,
    )

    bundle = GateBundle.build(
        scan_run_id=route.scan_run_id,
        cutoff_at=NOW,
        valid_until=LATER,
        pipeline_input_hash=_hash("pipeline-input"),
        routing_context=route,
        candidates=(excluded, ranked),
        outcome=PipelineGateOutcome.TRADE,
        run_reason_codes=(),
    )

    assert bundle.ranked_candidate_keys == (ranked.candidate_key,)
    assert bundle.hard_failure_candidate_keys == (excluded.candidate_key,)
    assert ranked.eligible is True
    assert ranked.reviewable is True
    assert excluded.eligible is False
    assert bundle.outcome is PipelineGateOutcome.TRADE
    assert bundle.append_payload()["record_type"] == "GATE_BUNDLE_TRADE"
    assert canonical_hash(bundle.hash_payload()) == bundle.gate_bundle_hash


def test_zero_candidate_no_trade_has_complete_append_only_payload() -> None:
    route = _routing_context()
    bundle = GateBundle.build(
        scan_run_id=route.scan_run_id,
        cutoff_at=NOW,
        valid_until=LATER,
        pipeline_input_hash=_hash("pipeline-input"),
        routing_context=route,
        candidates=(),
        outcome=PipelineGateOutcome.NO_TRADE,
        run_reason_codes=("NO_ELIGIBLE_CANDIDATES",),
    )

    document = bundle.append_payload()
    assert document["record_type"] == "GATE_BUNDLE_NO_TRADE"
    assert document["gate_bundle_hash"] == bundle.gate_bundle_hash
    assert document["gate_bundle"]["candidates"] == {}
    assert document["gate_bundle"]["outcome"] == "NO_TRADE"
    assert document["gate_bundle"]["run_reason_codes"] == ("NO_ELIGIBLE_CANDIDATES",)

    with pytest.raises(ValueError, match="gate_bundle_hash"):
        replace(bundle, run_reason_codes=("TAMPERED_RUN_REASON",))


def test_run_reason_contract_requires_no_trade_reason_and_forbids_trade_reason() -> None:
    route_gate = _routing_gate()
    route = _routing_context(route_gate)
    ranked = _candidate("ranked", rank=1, total=1, routing_gate=route_gate)

    with pytest.raises(ValueError, match="NO_TRADE requires stable run_reason_codes"):
        GateBundle.build(
            scan_run_id=route.scan_run_id,
            cutoff_at=NOW,
            valid_until=LATER,
            pipeline_input_hash=_hash("pipeline-input"),
            routing_context=route,
            candidates=(),
            outcome=PipelineGateOutcome.NO_TRADE,
            run_reason_codes=(),
        )
    with pytest.raises(ValueError, match="TRADE run_reason_codes must be empty"):
        GateBundle.build(
            scan_run_id=route.scan_run_id,
            cutoff_at=NOW,
            valid_until=LATER,
            pipeline_input_hash=_hash("pipeline-input"),
            routing_context=route,
            candidates=(ranked,),
            outcome=PipelineGateOutcome.TRADE,
            run_reason_codes=("SHOULD_NOT_EXIST",),
        )


def test_supporting_only_change_cannot_change_authority_projection() -> None:
    available = SupportingInput.build(
        source="DEEPSEEK",
        status=SupportingStatus.AVAILABLE,
        observed_at=NOW,
        source_hash=_hash("deepseek"),
        payload={"stance": "SUPPORTING"},
    )
    degraded = SupportingInput.degraded(
        source="DEEPSEEK",
        observed_at=NOW,
        reason_code="MODEL_UNAVAILABLE",
    )
    repeated_fallback = SupportingInput.degraded(
        source="DEEPSEEK",
        observed_at=NOW,
        reason_code="MODEL_UNAVAILABLE",
    )

    first = _candidate("same", rank=1, total=1, gate_3_supporting=(available,))
    second = _candidate("same", rank=1, total=1, gate_3_supporting=(degraded,))

    assert first.candidate_gate_hash != second.candidate_gate_hash
    assert degraded.supporting_hash == repeated_fallback.supporting_hash
    assert first.authority_projection() == second.authority_projection()
    assert first.eligible == second.eligible is True
    assert first.reviewable == second.reviewable is True
    assert first.ranking.context_hash == second.ranking.context_hash


def test_optional_absence_is_degraded_but_only_mandatory_absence_blocks_external() -> None:
    optional = classify_source_availability(
        source="DEEPSEEK",
        mandatory=False,
        available=False,
        reason_code="MODEL_UNAVAILABLE",
    )
    mandatory = classify_source_availability(
        source="OFFICIAL_EVENT_CALENDAR",
        mandatory=True,
        available=False,
        reason_code="CALENDAR_AUTHORITY_UNAVAILABLE",
    )

    assert optional.component_status is SourceComponentStatus.DEGRADED
    assert optional.gate_status is None
    assert optional.authority is GateAuthority.SUPPORTING_ONLY
    assert mandatory.component_status is SourceComponentStatus.BLOCKED_EXTERNAL
    assert mandatory.gate_status is GateStatus.UNAVAILABLE
    assert mandatory.authority is GateAuthority.HARD


def test_gate_6_rejects_circular_snapshot_or_bundle_bindings() -> None:
    ranking = _ranking(rank=1, total=1)

    with pytest.raises(ValueError, match="circular field"):
        GateLayerResult.build(
            gate_id=GateId.RANKING_REVIEWABILITY,
            status=GateStatus.PASS,
            observed_at=NOW,
            input_payload={"ranking_snapshot_hash": _hash("snapshot")},
            bindings={"pre_gate_rank_order_hash": ranking.pre_gate_rank_order_hash},
        )
    with pytest.raises(ValueError, match="circular field"):
        GateLayerResult.build(
            gate_id=GateId.RANKING_REVIEWABILITY,
            status=GateStatus.PASS,
            observed_at=NOW,
            input_payload=ranking.hash_payload(),
            bindings={"nested": {"gate_bundle_hash": _hash("bundle")}},
        )


@pytest.mark.parametrize(
    "derived_field",
    ("evidence_inputs", "ranking_basis_hash", "final_ranking_basis_hash"),
)
def test_pre_gate_rank_order_rejects_final_or_gate_derived_fields(
    derived_field: str,
) -> None:
    with pytest.raises(ValueError, match="circular field"):
        RankingGateContext.build(
            rank=1,
            total_ranked_count=1,
            pre_gate_rank_order_payload={
                "ordered_candidate_keys": (_hash("candidate"),),
                "nested": {derived_field: _hash("forbidden")},
            },
            authority_consistent=True,
        )


def test_pre_gate_rank_order_and_bundle_replay_are_deterministic() -> None:
    left_context = RankingGateContext.build(
        rank=1,
        total_ranked_count=1,
        pre_gate_rank_order_payload={
            "ordered_candidate_keys": (_hash("candidate"),),
            "score_inputs_hash": _hash("score-inputs"),
        },
        authority_consistent=True,
    )
    right_context = RankingGateContext.build(
        rank=1,
        total_ranked_count=1,
        pre_gate_rank_order_payload={
            "score_inputs_hash": _hash("score-inputs"),
            "ordered_candidate_keys": (_hash("candidate"),),
        },
        authority_consistent=True,
    )
    assert left_context.pre_gate_rank_order_hash == right_context.pre_gate_rank_order_hash
    assert left_context.context_hash == right_context.context_hash

    route_gate = _routing_gate()
    route = _routing_context(route_gate)
    ordered_keys = (_candidate_key("first"), _candidate_key("second"))
    first = _candidate(
        "first",
        rank=1,
        total=2,
        routing_gate=route_gate,
        ordered_candidate_keys=ordered_keys,
    )
    second = _candidate(
        "second",
        rank=2,
        total=2,
        routing_gate=route_gate,
        ordered_candidate_keys=ordered_keys,
    )
    arguments = {
        "scan_run_id": route.scan_run_id,
        "cutoff_at": NOW,
        "valid_until": LATER,
        "pipeline_input_hash": _hash("pipeline-input"),
        "routing_context": route,
        "outcome": PipelineGateOutcome.TRADE,
        "run_reason_codes": (),
    }
    left_bundle = GateBundle.build(candidates=(first, second), **arguments)
    replay_bundle = GateBundle.build(candidates=(second, first), **arguments)
    assert left_bundle.gate_bundle_hash == replay_bundle.gate_bundle_hash
    assert left_bundle.append_payload() == replay_bundle.append_payload()


def test_ranks_two_through_ten_are_view_only() -> None:
    ranking = _ranking(rank=2, total=2)
    candidate = _candidate("rank-two", rank=2, total=2)

    assert ranking.view_only is True
    assert ranking.reviewable is False
    assert candidate.eligible is True
    assert candidate.reviewable is False


def test_rank_one_inconsistent_authority_cannot_be_formal_survivor() -> None:
    ranking = _ranking(rank=1, total=1, consistent=False)
    assert ranking.view_only is True
    assert ranking.reviewable is False

    with pytest.raises(ValueError, match="requires consistent rank authority"):
        _candidate(
            "rank-one-inconsistent",
            rank=1,
            total=1,
            ranking_consistent=False,
        )


def test_direct_unranked_non_view_only_context_is_rejected_with_valid_hash() -> None:
    payload = {
        "rank": None,
        "total_ranked_count": 0,
        "pre_gate_rank_order_payload": None,
        "pre_gate_rank_order_hash": None,
        "authority_consistent": False,
        "view_only": False,
        "reviewable": False,
        "reason_codes": ("NOT_RANKED",),
    }

    with pytest.raises(ValueError, match="must be view-only"):
        RankingGateContext(
            **payload,
            context_hash=canonical_hash(payload),
        )


def test_bundle_rejects_candidate_rank_count_drift() -> None:
    route_gate = _routing_gate()
    route = _routing_context(route_gate)
    ranked = _candidate("ranked", rank=1, total=1, routing_gate=route_gate)
    excluded_with_wrong_total = _candidate(
        "excluded",
        rank=None,
        total=0,
        failed_gate=GateId.OPTION_EDGE_LIQUIDITY,
        routing_gate=route_gate,
    )

    with pytest.raises(ValueError, match="total_ranked_count"):
        GateBundle.build(
            scan_run_id=route.scan_run_id,
            cutoff_at=NOW,
            valid_until=LATER,
            pipeline_input_hash=_hash("pipeline-input"),
            routing_context=route,
            candidates=(ranked, excluded_with_wrong_total),
            outcome=PipelineGateOutcome.TRADE,
            run_reason_codes=(),
        )


@pytest.mark.parametrize(
    ("allowed", "blocked", "expected_message"),
    (
        ((), (), "must allow families"),
        (("CALENDAR",), ("VERTICAL_DEBIT",), "cannot PASS while blocking"),
        (("CALENDAR",), (), "PASS does not allow"),
    ),
)
def test_bundle_rejects_empty_blocked_or_undeclared_strategy_routes(
    allowed: tuple[str, ...],
    blocked: tuple[str, ...],
    expected_message: str,
) -> None:
    route_gate = _routing_gate(allowed=allowed, blocked=blocked)
    route = _routing_context(route_gate)
    if allowed:
        with pytest.raises(ValueError, match=expected_message):
            _candidate("route-check", rank=1, total=1, routing_gate=route_gate)
        return

    candidate = _candidate("route-check", rank=1, total=1, routing_gate=route_gate)
    with pytest.raises(ValueError, match=expected_message):
        GateBundle.build(
            scan_run_id=route.scan_run_id,
            cutoff_at=NOW,
            valid_until=LATER,
            pipeline_input_hash=_hash("pipeline-input"),
            routing_context=route,
            candidates=(candidate,),
            outcome=PipelineGateOutcome.TRADE,
            run_reason_codes=(),
        )


def test_candidate_rejects_gate_3_pass_that_blocks_its_strategy_family() -> None:
    with pytest.raises(
        ValueError,
        match="GATE_3_UNDERLYING_EVENT cannot PASS while blocking candidate strategy_family",
    ):
        _candidate(
            "earnings-overlap",
            rank=1,
            total=1,
            strategy_family="DEFINED_RISK_CREDIT",
            routing_gate=_routing_gate(allowed=("DEFINED_RISK_CREDIT",)),
            gate_3_blocked=("DEFINED_RISK_CREDIT",),
        )


def test_candidate_accepts_explicit_event_defined_family_route() -> None:
    family = "EVENT_DEFINED_FINITE_RISK"
    candidate = _candidate(
        "event-defined",
        rank=1,
        total=1,
        strategy_family=family,
        routing_gate=_routing_gate(allowed=(family,)),
        gate_3_allowed=(family,),
    )

    assert candidate.strategy_family == family
    assert candidate.layer(GateId.UNDERLYING_EVENT).status is GateStatus.PASS
    assert candidate.eligible is True


def test_provisional_watch_has_six_layers_and_no_candidate_or_rank_authority() -> None:
    unavailable = {
        GateId.AUTHORITY_DATA,
        GateId.OPTION_EDGE_LIQUIDITY,
        GateId.STRUCTURE_ACCOUNT_RISK,
        GateId.RANKING_REVIEWABILITY,
    }
    layers = tuple(
        WatchLayerPreview.build(
            gate_id=gate_id,
            status=GateStatus.UNAVAILABLE if gate_id in unavailable else GateStatus.PASS,
            observed_at=NOW,
            reason_codes=("PROVISIONAL_DATA_ONLY",),
            source_hashes=(_hash(gate_id.value),),
        )
        for gate_id in reversed(tuple(GateId))
    )

    preview = ProvisionalWatchGatePreview.build(
        symbol="spy",
        cutoff_at=NOW,
        source_bundle_hash=_hash("weekly-source-bundle"),
        layers=layers,
    )
    document = preview.as_dict()

    assert document["status"] == "PROVISIONAL"
    assert document["decision"] == "OBSERVATION_ONLY"
    assert document["decision_authority"] == "SUPPORTING_ONLY"
    assert len(document["layers"]) == 6
    for forbidden in (
        "candidate_id",
        "proposal_hash",
        "gate_bundle_hash",
        "eligible",
        "rank",
        "reviewable",
    ):
        assert forbidden not in document


def test_provisional_watch_rejects_gate_1_pass_and_missing_unavailable_reason() -> None:
    with pytest.raises(ValueError, match="stable reason code"):
        WatchLayerPreview.build(
            gate_id=GateId.AUTHORITY_DATA,
            status=GateStatus.UNAVAILABLE,
            observed_at=NOW,
        )

    layers = tuple(
        WatchLayerPreview.build(
            gate_id=gate_id,
            status=(
                GateStatus.UNAVAILABLE
                if gate_id
                in {
                    GateId.OPTION_EDGE_LIQUIDITY,
                    GateId.STRUCTURE_ACCOUNT_RISK,
                    GateId.RANKING_REVIEWABILITY,
                }
                else GateStatus.PASS
            ),
            observed_at=NOW,
            reason_codes=("PROVISIONAL_DATA_ONLY",),
        )
        for gate_id in GateId
    )
    with pytest.raises(ValueError, match="GATE_1_AUTHORITY_DATA must be unavailable"):
        ProvisionalWatchGatePreview.build(
            symbol="SPY",
            cutoff_at=NOW,
            source_bundle_hash=_hash("weekly-source-bundle"),
            layers=layers,
        )


def test_tampered_hash_and_relaxed_authority_fail_closed() -> None:
    gate = _hard_layer(GateId.UNDERLYING_EVENT)

    with pytest.raises(ValueError, match="gate_hash"):
        replace(gate, gate_hash=_hash("tampered"))
    with pytest.raises(ValueError, match="authority must be HARD"):
        replace(gate, authority=GateAuthority.SUPPORTING_ONLY)


def test_pass_gate_1_requires_all_existing_authority_bindings() -> None:
    with pytest.raises(ValueError, match="missing authority bindings"):
        GateLayerResult.build(
            gate_id=GateId.AUTHORITY_DATA,
            status=GateStatus.PASS,
            observed_at=NOW,
            input_payload={"broker": "existing-authority"},
            bindings={"proposal_hash": _hash("proposal")},
        )
