from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from options_copilot.decision.pipeline import (
    DecisionPipeline,
    _bind_candidates,
    _cost_adjusted,
    _option_pool_candidate_documents,
)
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
)
from options_copilot.learning.outcomes import (
    INDEPENDENCE_SPEC_SCHEMA,
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
    VerifiedIndependenceSpec,
    verify_independence_spec,
)
from options_copilot.ranking.store import RankingStore
from options_copilot.ranking.joint import (
    JointRankingEngine,
    JointRankingSnapshot,
    build_joint_ranking_input_document,
)
from options_copilot.replay.dataset import (
    ContractBinding,
    DatasetContractError,
    IndependenceSpecArtifactStore,
    PointInTimeDatasetBuilder,
    SourceBinding,
    build_rolling_windows,
)
from options_copilot.replay.engine import (
    HISTORICAL_ARTIFACT_SCHEMA,
    HistoricalDecisionPipeline,
    HistoricalPipelineArtifact,
    PointInTimeReplay,
    ReplayBindingError,
    ReplaySafetyError,
    TestHistoricalDecisionPipeline,
    replay_scan_run_id,
    _FrozenCandidate,
)
from options_copilot.storage.canonical import canonical_hash, thaw_json
from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore


HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64
POLICY_MARKER_HASH = "1" * 64
RISK_CONTRACT_HASH = "2" * 64
RISK_MARKER_HASH = "3" * 64
DECISION_AT = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
VERIFY_AT = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)


def _independence_fixture() -> VerifiedIndependenceSpec:
    effective_at = DECISION_AT - timedelta(days=1)
    rules = {
        "same_event_identity": True,
        "event_identity_namespace_fields": ("ticker", "issuer_id", "provider"),
        "same_ticker_event": True,
        "same_corporate_family_event": True,
        "adjacent_same_ticker_slots": True,
        "adjacent_slot_hours": 2,
        "overlapping_event_windows": True,
        "correlated_tickers_same_macro_event": True,
        "cross_provider_duplicates": True,
        "overlapping_holdings_thesis_structure": True,
        "representative_selection": "EARLIEST_DECISION_THEN_ID",
        "independent_weight": 1,
        "duplicate_weight": 0,
        "unknown_weight": 0,
        "unknown_exclusion_reason": "INDEPENDENCE_UNKNOWN",
        "correlated_ticker_groups": (),
    }
    body = {
        "schema": INDEPENDENCE_SPEC_SCHEMA,
        "version": "v1",
        "effective_at": effective_at.isoformat(timespec="microseconds"),
        "actor": "test:replay-fixture",
        "signed_at": effective_at.isoformat(timespec="microseconds"),
        "test_only": True,
        "initial_policy_version": INITIAL_POLICY_VERSION,
        "initial_policy_hash": INITIAL_POLICY_HASH,
        "execution_cost_version": EXECUTION_COST_VERSION,
        "execution_cost_hash": EXECUTION_COST_HASH,
        "rules": rules,
    }
    return verify_independence_spec(
        {**body, "spec_hash": canonical_hash(body)},
        allow_test_fixture=True,
        as_of=VERIFY_AT,
    )


def _independence_store() -> IndependenceSpecArtifactStore:
    return IndependenceSpecArtifactStore((_independence_fixture(),))


def _append_evidence(
    store: EvidenceStore,
    identity: str,
    source_id: str,
    *,
    first_seen_at: datetime,
    headline: str,
) -> None:
    store.append(
        EvidenceRecord(
            identity=identity,
            kind="NEWS",
            symbol="SPY",
            provider="fixture",
            source_id=source_id,
            published_at=first_seen_at - timedelta(seconds=1),
            first_seen_at=first_seen_at,
            ingested_at=first_seen_at,
            observed_at=first_seen_at,
            payload={"headline": headline},
        )
    )


def _universe() -> tuple[dict[str, object], ...]:
    snapshot_id = "universe-snapshot-1"
    snapshot_hash = canonical_hash({"snapshot_id": snapshot_id})
    return (
        {
            "symbol": "SPY",
            "included": True,
            "reason": None,
            "first_seen_at": DECISION_AT - timedelta(minutes=1),
            "as_of": DECISION_AT - timedelta(seconds=2),
            "source_id": "universe:SPY",
            "source_hash": HASH_C,
            "source_snapshot_id": snapshot_id,
            "source_snapshot_hash": snapshot_hash,
        },
        {
            "symbol": "QQQ",
            "included": False,
            "reason": "UNIVERSE_LIQUIDITY_REJECTED",
            "first_seen_at": DECISION_AT - timedelta(minutes=1),
            "as_of": DECISION_AT - timedelta(seconds=2),
            "source_id": "universe:QQQ",
            "source_hash": HASH_D,
            "source_snapshot_id": snapshot_id,
            "source_snapshot_hash": snapshot_hash,
        },
    )


def _execution(
    contract_id: str,
    *,
    bid: str | None,
    ask: str | None,
    age_seconds: float = 1.0,
    received_offset_seconds: float = -0.5,
) -> dict[str, object]:
    return {
        "contract_id": contract_id,
        "source_id": f"quote:{contract_id}",
        "source_hash": canonical_hash({"contract_id": contract_id}),
        "observed_at": DECISION_AT - timedelta(seconds=age_seconds),
        "received_at": DECISION_AT + timedelta(seconds=received_offset_seconds),
        "bid": bid,
        "ask": ask,
        "last": "99.00",
        "close": "98.00",
        "mid": "97.00",
        "secdef_hash": HASH_E,
        "quote_batch_id": "quotes-1",
    }


def _window(*, suffix: str = "base"):
    samples = []
    for index in range(6):
        observed = DECISION_AT - timedelta(days=5 - index)
        if suffix != "base" and index == 0:
            observed -= timedelta(hours=1)
        samples.append({"sample_id": f"base-s{index}", "decision_at": observed})
    return build_rolling_windows(
        tuple(samples),
        train_size=3,
        calibration_size=2,
        test_size=1,
        step_size=1,
    )[0]


def _store(tmp_path: Path) -> EvidenceStore:
    store = EvidenceStore(tmp_path / "evidence.sqlite3")
    _append_evidence(
        store,
        "news:visible",
        "source:visible",
        first_seen_at=DECISION_AT - timedelta(minutes=1),
        headline="visible",
    )
    return store


def _builder() -> PointInTimeDatasetBuilder:
    return PointInTimeDatasetBuilder(
        allow_test_fixtures=True,
        verification_as_of=VERIFY_AT,
    )


def _manifest_values(tmp_path: Path, **overrides: object) -> dict[str, object]:
    evidence_store = overrides.pop("evidence_store", None) or _store(tmp_path)
    values: dict[str, object] = {
        "decision_at": DECISION_AT,
        "sample_id": "base-s5",
        "scan_run_id": "scan-live-1",
        "ranking_snapshot_id": "ranking-live-1",
        "ranking_snapshot_hash": HASH_E,
        "universe_membership": _universe(),
        "evidence_store": evidence_store,
        "execution_records": (
            _execution("conid:1", bid="1.00", ask="1.20"),
            _execution("conid:2", bid=None, ask=None),
            _execution(
                "conid:stale",
                bid="2.00",
                ask="2.20",
                age_seconds=6,
            ),
        ),
        "initial_policy": ContractBinding(
            "INITIAL_POLICY", INITIAL_POLICY_VERSION, INITIAL_POLICY_HASH
        ),
        "execution_cost": ContractBinding(
            "EXECUTION_COST", EXECUTION_COST_VERSION, EXECUTION_COST_HASH
        ),
        "independence_store": _independence_store(),
        "independence_hash": _independence_fixture().spec_hash,
        "source_versions": {
            "IBKR": SourceBinding("IBKR", "snapshot-v1", HASH_C),
            "EVIDENCE": SourceBinding("EVIDENCE", "ledger-v2", HASH_D),
        },
        "window": _window(),
        "pipeline_version": "decision-pipeline-v1",
        "pipeline_hash": HASH_F,
        "input_hash": canonical_hash({"input": 1}),
        "evidence_hash": canonical_hash({"evidence": 1}),
        "broker_snapshot_hash": canonical_hash({"broker": 1}),
    }
    values.update(overrides)
    return values


def _manifest(tmp_path: Path, **overrides: object):
    owns_store = "evidence_store" not in overrides
    values = _manifest_values(tmp_path, **overrides)
    try:
        return _builder().build(**values)
    finally:
        if owns_store:
            values["evidence_store"].close()  # type: ignore[union-attr]


def _pipeline_result(manifest, *, status: str = "TRADE") -> dict[str, object]:
    scan_id = replay_scan_run_id(manifest)
    body: dict[str, object] = {
        "scan_run_id": scan_id,
        "status": status,
        "ranking_snapshot_id": "historical-ranking-1" if status == "TRADE" else None,
        "reasons": () if status == "TRADE" else ("PORTFOLIO_EMPTY",),
        "input_hash": manifest.input_hash,
        "evidence_hash": manifest.evidence_hash,
        "broker_snapshot_hash": manifest.broker_snapshot_hash,
        "policy_version": manifest.initial_policy_version,
        "policy_hash": manifest.initial_policy_hash,
        "current_policy_version": manifest.initial_policy_version,
        "current_policy_hash": manifest.initial_policy_hash,
        "policy_authority_marker_hash": POLICY_MARKER_HASH,
        "cost_version": manifest.execution_cost_version,
        "cost_hash": manifest.execution_cost_hash,
        "risk_contract_hash": RISK_CONTRACT_HASH,
        "risk_authority_version": "v1",
        "risk_authority_marker_hash": RISK_MARKER_HASH,
        "candidate_hashes": (HASH_C, HASH_D) if status == "TRADE" else (),
        "ranking_basis_hashes": (HASH_D, HASH_E) if status == "TRADE" else (),
        "ranking_snapshot_hash": HASH_F if status == "TRADE" else None,
        "gate_bundle_hash": canonical_hash(
            {"scan_run_id": scan_id, "status": status, "gate_bundle": "fixture"}
        ),
    }
    return {**body, "result_hash": canonical_hash(body)}


def _historical_pipeline(manifest, result: dict[str, object] | None = None):
    return TestHistoricalDecisionPipeline.from_immutable_test_result(
        result or _pipeline_result(manifest),
        decision_at=manifest.decision_at,
        pipeline_version=manifest.pipeline_version,
        pipeline_hash=manifest.pipeline_hash,
        independence_store=_independence_store(),
        independence_hash=manifest.independence_hash,
        verification_as_of=VERIFY_AT,
    )


def _funnel_trace(
    *,
    scan_run_id: str = "scan-live-1",
    ranked_count: int = 1,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": scan_run_id,
        "discovered_underlyings": 150,
        "deep_scan_requested": 30,
        "deep_scan_completed": 30,
        "ranked_limit": 10,
        "ranked_count": ranked_count,
        "filler_candidates": 0,
        "pacing_capability_hash": "a" * 64,
        "pacing_usage": {
            "scanner": {"used": 3, "limit": 10},
        },
    }


def _trade_artifact(
    *,
    funnel_trace: Mapping[str, object] | None = None,
) -> HistoricalPipelineArtifact:
    candidate_id = "historical-spy-vertical"
    terminal_scenarios = (
        {"terminal_underlying_price": "90", "probability": "0.50"},
        {"terminal_underlying_price": "110", "probability": "0.50"},
    )
    legs = (
        {
            "con_id": 101,
            "side": "LONG",
            "ratio": 1,
            "bid": "2.00",
            "ask": "2.10",
            "observed_at": DECISION_AT.isoformat(),
        },
        {
            "con_id": 102,
            "side": "SHORT",
            "ratio": 1,
            "bid": "1.00",
            "ask": "1.10",
            "observed_at": DECISION_AT.isoformat(),
        },
    )
    body = {
        "candidate_id": candidate_id,
        "symbol": "SPY",
        "structure": "DEBIT_VERTICAL",
        "legs": legs,
        "terminal_scenarios": terminal_scenarios,
        "debit_usd": "100",
        "credit_usd": "0",
        "all_in_cost_usd": "100",
        "max_loss_usd": "100",
        "max_profit_usd": "200",
        "breakevens": ("100",),
        "liquidity_score": "8",
        "exit_plan": {
            "thesis_invalidation": "trend reverses",
            "risk_stop": "50 percent loss",
            "profit_take": "50 percent gain",
            "time_stop": "before expiry",
            "maximum_holding_date": "2026-08-20",
            "bad_quote_action": "do not trade",
        },
        "dte": 20,
        "strategy_nav_usd": "1000",
        "strategy_nav_hash": HASH_C,
        "strategy_nav_content_hash": HASH_C,
        "strategy_nav_contract_hash": HASH_D,
        "strategy_nav_ledger_head_hash": HASH_B,
        "strategy_nav_observed_account_nlv": "1000",
        "strategy_nav_reconciliation_difference": "0",
        "strategy_nav_asof": DECISION_AT.isoformat(),
        "broker_snapshot_hash": HASH_E,
        "quote_batch_id": "quotes-1",
        "secdef_hash": HASH_F,
        "evidence_hashes": {"LIQUIDITY": HASH_D},
        "event_evidence_status": "AVAILABLE",
        "earnings_overlap": False,
        "event_defined": False,
        "event_evidence_hash": HASH_B,
        "execution_cost_contract_version": EXECUTION_COST_VERSION,
        "execution_cost_contract_hash": EXECUTION_COST_HASH,
        "policy_version": INITIAL_POLICY_VERSION,
        "policy_hash": INITIAL_POLICY_HASH,
        "dte_exception_hash": None,
    }
    candidate_hash = canonical_hash(body)
    proposal = {
        "schema": "options_copilot.proposal.v1",
        "review_only": True,
        "rank": 1,
        "eligible_to_send": True,
        "proposal_id": candidate_id,
        "candidate_id": candidate_id,
        "candidate_hash": candidate_hash,
        "symbol": "SPY",
        "underlying": "SPY",
        "structure": "DEBIT_VERTICAL",
        "dte": 20,
        "quote_snapshot_id": "quotes-1",
        "expected_value_usd": "10",
        "terminal_scenarios": terminal_scenarios,
        "broker_snapshot_hash": HASH_E,
        "secdef_hash": HASH_F,
        "strategy_nav": {
            "strategy_nav_usd": "1000",
            "authority_hash": HASH_C,
            "content_hash": HASH_C,
            "contract_hash": HASH_D,
            "ledger_head_hash": HASH_B,
            "observed_account_nlv": "1000",
            "reconciliation_difference": "0",
            "asof": DECISION_AT.isoformat(),
        },
        "policy": {
            "version": INITIAL_POLICY_VERSION,
            "hash": INITIAL_POLICY_HASH,
            "dte_exception_hash": None,
        },
        "execution_cost_contract": {
            "version": EXECUTION_COST_VERSION,
            "hash": EXECUTION_COST_HASH,
        },
        "evidence_hashes": {"LIQUIDITY": HASH_D},
        "pricing": {
            "reference_cost_usd": "100",
            "estimated_commissions_usd": "0",
            "estimated_slippage_usd": "0",
            "estimated_execution_costs_usd": "0",
            "all_in_executable_cost_usd": "100",
            "net_debit_usd": "100",
        },
        "risk": {
            "maximum_loss_usd": "100",
            "maximum_profit_usd": "200",
            "risk_fraction": "0.1",
            "defined_risk": True,
            "breakevens": ("100",),
        },
        "legs": legs,
        "exit_plan": body["exit_plan"],
    }
    inputs: dict[str, object] = {
        "input_hash": HASH_C,
        "universe": {},
        "positions": (),
        "strategy_nav_snapshot": {
            "valid": True,
            "authority_hash": HASH_C,
            "content_hash": HASH_C,
            "contract_hash": HASH_D,
            "ledger_head_hash": HASH_B,
            "observed_account_nlv": "1000",
            "reconciliation_difference": "0",
            "asof": DECISION_AT.isoformat(),
        },
    }
    universe: dict[str, object] = {}
    if funnel_trace is not None:
        pre_rank_trace = {**funnel_trace, "ranked_count": 0}
        inputs["funnel_trace"] = pre_rank_trace
        inputs["universe"] = {"funnel_trace": pre_rank_trace}
        universe["funnel_trace"] = pre_rank_trace
    artifact_values: dict[str, object] = {
        "inputs": inputs,
        "universe": universe,
        "broker_evidence": {
            "snapshot_hash": HASH_E,
            "evidence_hash": HASH_C,
        },
        "candidates": ({"candidate_body": body, "proposal_body": proposal},),
        "volatility": {"eligible": True, "evidence_hash": HASH_D},
        "scenarios": (
            {
                "action": "TRADE",
                "scenarios": (
                    {"terminal_price": "90", "probability": "0.50"},
                    {"terminal_price": "110", "probability": "0.50"},
                ),
                "cost_hash": EXECUTION_COST_HASH,
                "current_policy_version": INITIAL_POLICY_VERSION,
                "current_policy_hash": INITIAL_POLICY_HASH,
                "policy_authority_marker_hash": POLICY_MARKER_HASH,
                "risk_authority_version": "v1",
                "risk_authority_marker_hash": RISK_MARKER_HASH,
                "risk_contract_hash": RISK_CONTRACT_HASH,
            },
        ),
        "policy": {
            "current_policy_version": INITIAL_POLICY_VERSION,
            "current_policy_hash": INITIAL_POLICY_HASH,
            "policy_authority_marker_hash": POLICY_MARKER_HASH,
            "payload": {"hard_no_trade_thresholds": {"cost_and_expectancy": {
                "minimum_after_cost_expected_value_usd": "max(5.00,0.05*maximum_loss)",
                "minimum_max_profit_to_maximum_loss": "1.20",
                "maximum_total_round_trip_cost_to_max_profit": "0.20",
                "stress_after_cost_ev": "must be greater than or equal to 0.00",
            }}},
        },
        "risk_authority": {
            "version": "v1",
            "risk_authority_marker_hash": RISK_MARKER_HASH,
            "risk_contract_hash": RISK_CONTRACT_HASH,
            "tier": "NORMAL",
            "a_grade_approved": False,
        },
        "cost": {
            "version": EXECUTION_COST_VERSION,
            "hash": EXECUTION_COST_HASH,
            "candidates": (
                {
                    "candidate_id": candidate_id,
                    "cost_hash": EXECUTION_COST_HASH,
                    "cost_version": EXECUTION_COST_VERSION,
                    "after_cost_expected_value": Decimal("10"),
                    "execution_cost_usd": Decimal("0"),
                    "stress_after_cost_expected_value": Decimal("10"),
                    "calculation_hash": HASH_D,
                },
            ),
        },
        "gate": {"eligible": True, "risk_fraction": Decimal("0.10")},
    }
    if funnel_trace is not None:
        artifact_values["funnel_trace"] = dict(funnel_trace)
    return HistoricalPipelineArtifact.build(**artifact_values)  # type: ignore[arg-type]


def _joint_v2_artifact() -> HistoricalPipelineArtifact:
    legacy = _trade_artifact()
    document = legacy.identity_document()
    candidate_body = document["candidates"][0]["candidate_body"]
    proposal_body = document["candidates"][0]["proposal_body"]
    for index, leg in enumerate(candidate_body["legs"]):
        leg.update({
            "contract_id_ex": f"{leg['con_id']}@SMART",
            "expiration": "2026-08-21",
            "strike": "100" if index == 0 else "105",
            "right": "CALL",
            "multiplier": 100,
            "exchange": "SMART",
            "exchange_time": DECISION_AT.isoformat(),
            "quote_age_seconds": "0",
            "market_data_type": 1,
            "delta": "0.55" if index == 0 else "0.35",
            "gamma": "0.02",
            "theta": "-0.08",
            "vega": "0.11",
            "volume": 100,
            "open_interest": 500,
            "short_leg_risk_evidence": (
                {"status": "SUPPORTED", "evidence_hash": HASH_B}
                if index == 1
                else {"status": "NOT_APPLICABLE", "evidence_hash": HASH_B}
            ),
        })
    proposal_body["legs"] = candidate_body["legs"]
    proposal_body["expiration"] = "2026-08-21"
    candidate_body["liquidity_score"] = "0.8"
    proposal_body["candidate_hash"] = canonical_hash(candidate_body)
    proposal_body["expected_value_usd"] = "150"
    document["cost"]["candidates"][0].update({
        "commission_usd": Decimal("0"),
        "slippage_usd": Decimal("0"),
        "after_cost_expected_value": Decimal("150"),
    })
    document["candidates"] = ({
        "candidate_body": candidate_body,
        "proposal_body": proposal_body,
    },)
    equity_reference = {
        "schema": "options_copilot.equity_pool_reference.v1",
        "snapshot_id": "1" * 64,
        "snapshot_hash": "2" * 64,
        "input_manifest_hash": "3" * 64,
        "rows_hash": "4" * 64,
        "policy_hash": "5" * 64,
        "taxonomy_hash": "6" * 64,
        "scoring_hash": "7" * 64,
        "selected_symbols": ("SPY",),
        "discovery_count": 1,
        "selected_count": 1,
        "excluded_count": 0,
        "exclusion_stats": {},
    }
    thesis = {
        "schema": "options_copilot.equity_thesis_evidence.v1",
        "symbol": "SPY",
        "direction_label": "BULLISH",
        "direction_score": Decimal("40"),
        "uncertainty": Decimal("0.2"),
        "observed_at": DECISION_AT,
        "source_hashes": (HASH_B,),
        "canonical_input_hash": HASH_C,
        "selected_rank": 1,
    }
    theses = {
        "schema": "options_copilot.equity_theses.v1",
        "equity_pool_reference_hash": canonical_hash(equity_reference),
        "rows": (thesis,),
        "rows_hash": canonical_hash((thesis,)),
    }
    frozen_candidate = _FrozenCandidate.from_artifact(document["candidates"][0])
    bindings, binding_error = _bind_candidates((frozen_candidate,))
    assert binding_error is None
    scenarios = tuple(document["scenarios"])
    costs = document["cost"]
    adjusted = _cost_adjusted(
        (frozen_candidate,),
        scenarios,
        costs,
        EXECUTION_COST_HASH,
        EXECUTION_COST_VERSION,
    )
    assert adjusted is not None
    finalized = _option_pool_candidate_documents(
        bindings,
        scenarios,
        costs,
        adjusted,
        equity_theses=theses,
    )
    joint_snapshot = JointRankingEngine().rank(
        finalized,
        scan_run_id="scan-live-1",
        now=DECISION_AT,
        equity_theses=theses,
        broker_snapshot_hash=HASH_E,
        strategy_nav_hash=HASH_C,
        gate_reasons_by_candidate={"historical-spy-vertical": ()},
        aggregate_open_risk_usd=Decimal("0"),
        concentration_by_underlying={"SPY": Decimal("0")},
    )
    joint_input = build_joint_ranking_input_document(
        finalized,
        scan_run_id="scan-live-1",
        now=DECISION_AT,
        equity_theses=theses,
        broker_snapshot_hash=HASH_E,
        strategy_nav_hash=HASH_C,
        gate_reasons_by_candidate={"historical-spy-vertical": ()},
        aggregate_open_risk_usd=Decimal("0"),
        concentration_by_underlying={"SPY": Decimal("0")},
    )
    trace = {
        **_funnel_trace(),
        "equity_pool_reference": equity_reference,
        "equity_theses": theses,
        "finalized_option_candidates": tuple(
            {
                "candidate_id": item.payload["candidate_id"],
                "candidate_hash": item.candidate_hash,
                "payload": thaw_json(item.payload),
            }
            for item in finalized
        ),
        "joint_ranking_input": thaw_json(joint_input),
        "joint_ranking": joint_snapshot.as_dict(),
    }
    pre_rank_trace = {
        key: value
        for key, value in trace.items()
        if key not in {
            "finalized_option_candidates",
            "joint_ranking_input",
            "joint_ranking",
        }
    }
    pre_rank_trace["ranked_count"] = 0
    inputs = dict(document["inputs"])
    inputs["funnel_trace"] = pre_rank_trace
    inputs["universe"] = {"funnel_trace": pre_rank_trace}
    universe = dict(document["universe"])
    universe["funnel_trace"] = pre_rank_trace
    account_body = {
        "broker_snapshot_hash": HASH_E,
        "open_position_underlyings": (),
        "aggregate_open_risk_usd": Decimal("0"),
        "concentration_by_underlying": {"SPY": Decimal("0")},
    }
    broker_evidence = {
        **document["broker_evidence"],
        "joint_account_context": {
            **account_body,
            "context_hash": canonical_hash(account_body),
        },
    }
    return HistoricalPipelineArtifact.build(
        inputs=inputs,
        universe=universe,
        broker_evidence=broker_evidence,
        candidates=document["candidates"],
        volatility=document["volatility"],
        scenarios=document["scenarios"],
        policy=document["policy"],
        risk_authority=document["risk_authority"],
        cost=document["cost"],
        gate=document["gate"],
        funnel_trace=trace,
        schema=HISTORICAL_ARTIFACT_SCHEMA,
    )


def test_replay_v2_requires_hash_bound_joint_evidence_and_v1_is_explicit_legacy() -> None:
    legacy = _trade_artifact()
    assert legacy.requires_joint_ranking is False

    with pytest.raises(ReplayBindingError, match="HISTORICAL_JOINT_RANKING_REQUIRED"):
        document = legacy.identity_document()
        HistoricalPipelineArtifact.build(
            inputs=document["inputs"],
            universe=document["universe"],
            broker_evidence=document["broker_evidence"],
            candidates=document["candidates"],
            volatility=document["volatility"],
            scenarios=document["scenarios"],
            policy=document["policy"],
            risk_authority=document["risk_authority"],
            cost=document["cost"],
            gate=document["gate"],
            schema=HISTORICAL_ARTIFACT_SCHEMA,
        )


@pytest.mark.parametrize(
    ("artifact_factory", "pipeline_version"),
    (
        (_trade_artifact, "decision-pipeline-v2"),
        (_joint_v2_artifact, "decision-pipeline-v1"),
    ),
)
def test_replay_artifact_schema_is_bound_to_pipeline_version(
    artifact_factory,
    pipeline_version: str,
) -> None:
    with pytest.raises(ReplayBindingError, match="PIPELINE_ARTIFACT_VERSION_MISMATCH"):
        TestHistoricalDecisionPipeline.from_point_in_time_artifact(
            artifact_factory(),
            decision_at=DECISION_AT,
            pipeline_version=pipeline_version,
            independence_store=_independence_store(),
            independence_hash=_independence_fixture().spec_hash,
            verification_as_of=VERIFY_AT,
        )


def test_replay_v2_recomputes_joint_ranking_through_real_pipeline() -> None:
    artifact = _joint_v2_artifact()
    store = _independence_store()
    historical = TestHistoricalDecisionPipeline.from_point_in_time_artifact(
        artifact,
        decision_at=DECISION_AT,
        pipeline_version="decision-pipeline-v2",
        independence_store=store,
        independence_hash=_independence_fixture().spec_hash,
        verification_as_of=VERIFY_AT,
    )

    run = historical.run_slot("scan-live-1", DECISION_AT)

    assert run.result["status"] == "TRADE"
    assert run.result["funnel_trace"]["joint_ranking"] == (
        artifact.funnel_trace["joint_ranking"]
    )

    current = _joint_v2_artifact()
    assert current.requires_joint_ranking is True
    assert current.verify() is current

    tampered = current.identity_document()
    tampered["funnel_trace"]["joint_ranking"]["executable"][0]["score"] = "99"
    tampered_pre_rank = {
        key: value
        for key, value in tampered["funnel_trace"].items()
        if key not in {
            "finalized_option_candidates",
            "joint_ranking_input",
            "joint_ranking",
        }
    }
    tampered_pre_rank["ranked_count"] = 0
    tampered["inputs"]["funnel_trace"] = tampered_pre_rank
    tampered["inputs"]["universe"] = {"funnel_trace": tampered_pre_rank}
    tampered["universe"]["funnel_trace"] = tampered_pre_rank
    with pytest.raises(ReplayBindingError, match="HISTORICAL_JOINT_RANKING_INVALID"):
        HistoricalPipelineArtifact.build(
            inputs=tampered["inputs"],
            universe=tampered["universe"],
            broker_evidence=tampered["broker_evidence"],
            candidates=tampered["candidates"],
            volatility=tampered["volatility"],
            scenarios=tampered["scenarios"],
            policy=tampered["policy"],
            risk_authority=tampered["risk_authority"],
            cost=tampered["cost"],
            gate=tampered["gate"],
            funnel_trace=tampered["funnel_trace"],
            schema=HISTORICAL_ARTIFACT_SCHEMA,
        )


def test_point_in_time_replay_v2_rebinds_nested_joint_identity(
    tmp_path: Path,
) -> None:
    artifact = _joint_v2_artifact()
    manifest = _manifest(
        tmp_path,
        pipeline_version="decision-pipeline-v2",
        pipeline_hash=artifact.artifact_hash,
        input_hash=HASH_C,
        evidence_hash=canonical_hash({"broker": HASH_C, "volatility": HASH_D}),
        broker_snapshot_hash=HASH_E,
    )
    historical = TestHistoricalDecisionPipeline.from_point_in_time_artifact(
        artifact,
        decision_at=DECISION_AT,
        pipeline_version=manifest.pipeline_version,
        independence_store=_independence_store(),
        independence_hash=manifest.independence_hash,
        verification_as_of=VERIFY_AT,
    )
    replay_scan_id = replay_scan_run_id(manifest)

    envelope = historical.run_slot(replay_scan_id, DECISION_AT)
    rebound = JointRankingSnapshot.from_dict(
        envelope.result["funnel_trace"]["joint_ranking"]
    )
    captured = JointRankingSnapshot.from_dict(
        artifact.funnel_trace["joint_ranking"]
    )

    assert envelope.result["status"] == "TRADE"
    assert rebound.scan_run_id == replay_scan_id
    assert rebound.input_hash != captured.input_hash
    assert rebound.snapshot_hash != captured.snapshot_hash
    replay = PointInTimeReplay(historical).run(manifest)
    assert replay.status == "TRADE"
    assert replay.funnel_trace["joint_ranking"]["snapshot_hash"] == (
        rebound.snapshot_hash
    )


def test_replay_v2_rejects_recomputed_joint_input_hash_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = _joint_v2_artifact()
    original_rank = JointRankingEngine.rank

    def drifted_rank(self, *args, **kwargs):
        snapshot = original_rank(self, *args, **kwargs)
        document = snapshot.as_dict()
        document["input_hash"] = "f" * 64
        body = {key: value for key, value in document.items() if key != "snapshot_hash"}
        document["snapshot_hash"] = canonical_hash(body)
        return JointRankingSnapshot.from_dict(document)

    monkeypatch.setattr(JointRankingEngine, "rank", drifted_rank)
    historical = TestHistoricalDecisionPipeline.from_point_in_time_artifact(
        artifact,
        decision_at=DECISION_AT,
        pipeline_version="decision-pipeline-v2",
        independence_store=_independence_store(),
        independence_hash=_independence_fixture().spec_hash,
        verification_as_of=VERIFY_AT,
    )

    with pytest.raises(ReplayBindingError, match="PIPELINE_FUNNEL_TRACE_MISMATCH"):
        historical.run_slot("scan-live-1", DECISION_AT)


def test_dataset_fails_closed_without_unified_independence_authority(
    tmp_path: Path,
) -> None:
    with pytest.raises(DatasetContractError, match="INDEPENDENCE_ARTIFACT_STORE_REQUIRED"):
        _manifest(tmp_path, independence_store=None)

    with pytest.raises(DatasetContractError, match="test fixture"):
        store = _store(tmp_path / "fixture-not-allowed")
        try:
            PointInTimeDatasetBuilder(verification_as_of=VERIFY_AT).build(
                **_manifest_values(
                    tmp_path / "fixture-not-allowed",
                    evidence_store=store,
                    independence_store=_independence_store(),
                    independence_hash=_independence_fixture().spec_hash,
                )
            )
        finally:
            store.close()


def test_real_evidence_store_recomputes_conflict_at_cutoff_and_ignores_future_version(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    baseline = _manifest(tmp_path, evidence_store=store)
    _append_evidence(
        store,
        "news:visible",
        "source:future-correction",
        first_seen_at=DECISION_AT + timedelta(seconds=1),
        headline="future correction",
    )
    rebuilt = _manifest(tmp_path, evidence_store=store)

    assert baseline.dataset_hash == rebuilt.dataset_hash
    assert len(rebuilt.included_evidence) == 1
    assert rebuilt.included_evidence[0]["effective_status"] == "ACTIVE"
    store.close()


def test_quotes_never_backfill_and_stale_quotes_are_reasoned(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    reasons = {row["reason"] for row in manifest.exclusions}
    assert "MISSING_EXECUTION_DATA" in reasons
    assert "STALE_EXECUTION_DATA" in reasons
    unavailable = next(
        row for row in manifest.execution_availability if row["contract_id"] == "conid:2"
    )
    assert unavailable["bid"] is None
    assert unavailable["ask"] is None
    assert unavailable["observed_at"]
    assert unavailable["received_at"]
    assert unavailable["quote_batch_id"] == "quotes-1"
    assert unavailable["secdef_hash"] == HASH_E
    assert not {"last", "close", "mid"}.intersection(unavailable)

    stale_without_secdef = {
        **_execution(
            "conid:stale-no-secdef",
            bid="3.00",
            ask="3.20",
            age_seconds=6,
        ),
        "secdef_hash": None,
    }
    stale_manifest = _manifest(
        tmp_path / "stale-no-secdef",
        execution_records=(stale_without_secdef,),
    )
    assert stale_manifest.execution_availability[0]["reason"] == (
        "STALE_EXECUTION_DATA"
    )


def test_universe_and_quote_cutoffs_fail_closed(tmp_path: Path) -> None:
    future_universe = list(_universe())
    future_universe[0] = {
        **future_universe[0],
        "as_of": DECISION_AT + timedelta(microseconds=1),
    }
    with pytest.raises(DatasetContractError, match="UNIVERSE_AFTER_CUTOFF"):
        _manifest(tmp_path, universe_membership=tuple(future_universe))

    mixed_batch = (
        _execution("conid:1", bid="1.00", ask="1.20"),
        {**_execution("conid:2", bid="2.00", ask="2.20"), "quote_batch_id": "quotes-2"},
    )
    with pytest.raises(DatasetContractError, match="MIXED_QUOTE_BATCH"):
        _manifest(tmp_path, execution_records=mixed_batch)


@pytest.mark.parametrize(
    ("override", "value"),
    [
        ("source_versions", {"IBKR": SourceBinding("IBKR", "v2", HASH_E)}),
        ("window", _window(suffix="changed")),
        ("pipeline_hash", HASH_E),
        ("ranking_snapshot_hash", HASH_D),
    ],
)
def test_dataset_hash_binds_all_non_evidence_contracts(
    tmp_path: Path, override: str, value: object
) -> None:
    assert _manifest(tmp_path).dataset_hash != _manifest(
        tmp_path / override, **{override: value}
    ).dataset_hash


def test_rolling_windows_are_disjoint_chronological_and_current_sample_is_test(
    tmp_path: Path,
) -> None:
    window = _window()
    assert window.train_ids == ("base-s0", "base-s1", "base-s2")
    assert window.calibration_ids == ("base-s3", "base-s4")
    assert window.test_ids == ("base-s5",)
    assert set(window.train_ids).isdisjoint(window.calibration_ids)
    assert set(window.train_ids).isdisjoint(window.test_ids)
    assert set(window.calibration_ids).isdisjoint(window.test_ids)
    assert _manifest(tmp_path, window=window).sample_id in window.test_ids

    broken = replace(window, test_samples=())
    with pytest.raises(DatasetContractError, match="WINDOW_PARTITION_EMPTY"):
        _manifest(tmp_path / "broken", window=broken)


def test_replay_sealed_pipeline_is_deterministic_and_never_authorizable(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    historical = _historical_pipeline(manifest)
    replay = PointInTimeReplay(historical)
    first = replay.run(manifest)
    second = replay.run(manifest)

    assert first == second
    assert first.pipeline_result_hash == _pipeline_result(manifest)["result_hash"]
    assert first.gate_bundle_hash == _pipeline_result(manifest)["gate_bundle_hash"]
    assert first.pipeline_projection_hash == second.pipeline_projection_hash
    assert first.authority == "REPLAY"
    assert first.pipeline_mode == "TEST_ONLY"
    assert first.production_count_eligible is False
    assert all(row["authority"] == "REPLAY" for row in first.rankings)
    assert all(row["authorizable"] is False for row in first.rankings)
    envelope = historical.run_slot(replay_scan_run_id(manifest), DECISION_AT)
    assert all(value == 0 for value in envelope.forbidden_call_counts.values())

    class Resolver:
        def resolve(self, **_: object) -> object:
            return {}

        def is_current(self, _: object) -> bool:
            return True

    with RankingStore(tmp_path / "live-ranking.sqlite3") as live:
        assert live.authorize_frozen_rank_one(
            first.ranking_snapshot_id,
            str(first.rankings[0]["candidate_hash"]),
            {},
            policy_resolver=Resolver(),
            risk_authority_resolver=Resolver(),
            now=DECISION_AT,
        ) is None


def test_immutable_test_pipeline_can_never_become_production_count_eligible(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    assert "independence_test_only" not in manifest.__dataclass_fields__
    assert "production_count_eligible" not in manifest.__dataclass_fields__

    forged_shell = replace(
        manifest,
        independence_hash=HASH_D,
        dataset_hash="0" * 64,
    )
    forged_manifest = replace(
        forged_shell,
        dataset_hash=canonical_hash(forged_shell.identity_document()),
    )
    with pytest.raises(ReplayBindingError, match="DATASET_VERIFICATION_FAILED"):
        PointInTimeReplay(_historical_pipeline(manifest)).run(forged_manifest)

    artifact = _trade_artifact()
    with pytest.raises(ReplaySafetyError, match="test fixture"):
        HistoricalDecisionPipeline.from_point_in_time_artifact(
            artifact,
            decision_at=DECISION_AT,
            pipeline_version="decision-pipeline-v1",
            independence_store=_independence_store(),
            independence_hash=_independence_fixture().spec_hash,
            verification_as_of=VERIFY_AT,
        )


def test_arbitrary_run_slot_wrapper_and_external_dependencies_are_rejected(
    tmp_path: Path,
) -> None:
    calls = {"inputs": 0}

    class SideEffectWrapper:
        def run_slot(self, **_: object) -> object:
            raise AssertionError("must not be called")

    class SideEffectInputs:
        def run(self, **_: object) -> object:
            calls["inputs"] += 1
            raise AssertionError("must not be called")

    with pytest.raises(ReplaySafetyError, match="HISTORICAL_PIPELINE_REQUIRED"):
        PointInTimeReplay(SideEffectWrapper())

    pipeline = DecisionPipeline(
        inputs=SideEffectInputs(),
        universe_funnel=object(),
        broker_evidence=object(),
        strategy_registry=object(),
        strategy_generator=object(),
        volatility_engine=object(),
        scenario_engine=object(),
        cost_contract=object(),
        eligibility_gate=object(),
        portfolio_ranker=object(),
        ranking_store=object(),
        clock=lambda: datetime.now(timezone.utc),
    )
    assert not hasattr(HistoricalDecisionPipeline, "from_decision_pipeline")
    with pytest.raises(ReplaySafetyError, match="HISTORICAL_PIPELINE_REQUIRED"):
        PointInTimeReplay(pipeline)
    assert calls == {"inputs": 0}


@pytest.mark.parametrize("mutation", ("ALIAS", "RUN_ID", "RESULT_HASH", "NO_TRADE"))
def test_pipeline_binding_adversaries_fail_closed(tmp_path: Path, mutation: str) -> None:
    manifest = _manifest(tmp_path)
    result = _pipeline_result(manifest, status="NO_TRADE" if mutation == "NO_TRADE" else "TRADE")
    if mutation == "ALIAS":
        result["current_policy_hash"] = HASH_D
        body = {key: value for key, value in result.items() if key != "result_hash"}
        result["result_hash"] = canonical_hash(body)
    elif mutation == "RUN_ID":
        result["scan_run_id"] = "wrong-run"
        body = {key: value for key, value in result.items() if key != "result_hash"}
        result["result_hash"] = canonical_hash(body)
    elif mutation == "RESULT_HASH":
        result["result_hash"] = HASH_D
    else:
        result.pop("broker_snapshot_hash")
        body = {key: value for key, value in result.items() if key != "result_hash"}
        result["result_hash"] = canonical_hash(body)

    with pytest.raises(ReplayBindingError):
        PointInTimeReplay(_historical_pipeline(manifest, result)).run(manifest)


@pytest.mark.parametrize(
    "mutation",
    (
        "UNKNOWN",
        "LIVE",
        "DECISION_LIVE",
        "AUTHORIZABLE",
        "APPROVAL",
        "INSTRUCTION",
        "ORDER",
        "SEND",
        "SUBMIT",
    ),
)
def test_pipeline_result_schema_and_live_authority_injection_fail_closed(
    tmp_path: Path,
    mutation: str,
) -> None:
    manifest = _manifest(tmp_path)
    result = _pipeline_result(manifest)
    field = {
        "UNKNOWN": "surprise",
        "LIVE": "authority",
        "DECISION_LIVE": "decision_authority",
        "AUTHORIZABLE": "authorizable",
        "APPROVAL": "approval_eligible",
        "INSTRUCTION": "instruction_eligible",
        "ORDER": "order_eligible",
        "SEND": "eligible_to_send",
        "SUBMIT": "order_submission_allowed",
    }[mutation]
    result[field] = "LIVE" if mutation in {"LIVE", "DECISION_LIVE"} else True
    body = {key: value for key, value in result.items() if key != "result_hash"}
    result["result_hash"] = canonical_hash(body)

    error = ReplayBindingError if mutation == "UNKNOWN" else ReplaySafetyError
    with pytest.raises(error):
        _historical_pipeline(manifest, result)


def test_full_trade_uses_internal_frozen_graph_and_survives_source_mutation(
    tmp_path: Path,
) -> None:
    artifact = _trade_artifact()
    final_evidence_hash = canonical_hash({"broker": HASH_C, "volatility": HASH_D})
    manifest = _manifest(
        tmp_path,
        pipeline_hash=artifact.artifact_hash,
        input_hash=HASH_C,
        evidence_hash=final_evidence_hash,
        broker_snapshot_hash=HASH_E,
    )
    historical = TestHistoricalDecisionPipeline.from_point_in_time_artifact(
        artifact,
        decision_at=DECISION_AT,
        pipeline_version="decision-pipeline-v1",
        independence_store=_independence_store(),
        independence_hash=manifest.independence_hash,
        verification_as_of=VERIFY_AT,
    )
    envelope = historical.run_slot(replay_scan_run_id(manifest), DECISION_AT)
    assert envelope.result["status"] == "TRADE"
    assert envelope.result["funnel_trace"] == {}
    assert all(value == 0 for value in envelope.forbidden_call_counts.values())

    replay = PointInTimeReplay(historical).run(manifest)
    assert replay.status == "TRADE"
    assert replay.pipeline_mode == "TEST_ONLY"
    assert replay.production_count_eligible is False
    assert replay.rankings and replay.rankings[0]["authorizable"] is False

    raw_inputs = {"input_hash": HASH_C, "positions": ()}
    plain_artifact = artifact.identity_document()
    frozen_artifact = HistoricalPipelineArtifact.build(
        inputs=raw_inputs,
        universe=plain_artifact["universe"],  # type: ignore[arg-type]
        broker_evidence=plain_artifact["broker_evidence"],  # type: ignore[arg-type]
        candidates=plain_artifact["candidates"],  # type: ignore[arg-type]
        volatility=plain_artifact["volatility"],  # type: ignore[arg-type]
        scenarios=plain_artifact["scenarios"],  # type: ignore[arg-type]
        policy=plain_artifact["policy"],  # type: ignore[arg-type]
        risk_authority=plain_artifact["risk_authority"],  # type: ignore[arg-type]
        cost=plain_artifact["cost"],  # type: ignore[arg-type]
        gate=plain_artifact["gate"],  # type: ignore[arg-type]
    )
    raw_inputs["side_effect"] = object()
    assert "side_effect" not in frozen_artifact.inputs


def test_nonempty_funnel_trace_round_trips_through_artifact_and_replay_hash(
    tmp_path: Path,
) -> None:
    source_trace = _funnel_trace()
    artifact = _trade_artifact(funnel_trace=source_trace)
    final_evidence_hash = canonical_hash({"broker": HASH_C, "volatility": HASH_D})
    manifest = _manifest(
        tmp_path,
        pipeline_hash=artifact.artifact_hash,
        input_hash=HASH_C,
        evidence_hash=final_evidence_hash,
        broker_snapshot_hash=HASH_E,
    )
    historical = TestHistoricalDecisionPipeline.from_point_in_time_artifact(
        artifact,
        decision_at=DECISION_AT,
        pipeline_version="decision-pipeline-v1",
        independence_store=_independence_store(),
        independence_hash=manifest.independence_hash,
        verification_as_of=VERIFY_AT,
    )
    expected = {
        **source_trace,
        "scan_run_id": replay_scan_run_id(manifest),
    }

    envelope = historical.run_slot(replay_scan_run_id(manifest), DECISION_AT)
    replay = PointInTimeReplay(historical).run(manifest)

    assert artifact.funnel_trace == source_trace
    assert envelope.result["funnel_trace"] == expected
    assert replay.funnel_trace == expected
    assert replay.as_dict()["funnel_trace"] == expected
    assert replay.verify() is replay


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    (
        ("ranked_count", 2, "HISTORICAL_FUNNEL_RANKED_COUNT_MISMATCH"),
        ("pacing_capability_hash", "b" * 64, "HISTORICAL_FUNNEL_TRACE_MISMATCH"),
    ),
)
def test_rehashed_hostile_final_funnel_trace_is_rejected(
    field: str,
    value: object,
    reason: str,
) -> None:
    artifact = _trade_artifact(funnel_trace=_funnel_trace())
    document = artifact.identity_document()
    hostile_trace = {**document["funnel_trace"], field: value}  # type: ignore[misc]

    with pytest.raises(ReplayBindingError, match=reason):
        HistoricalPipelineArtifact.build(
            inputs=document["inputs"],  # type: ignore[arg-type]
            universe=document["universe"],  # type: ignore[arg-type]
            broker_evidence=document["broker_evidence"],  # type: ignore[arg-type]
            candidates=document["candidates"],  # type: ignore[arg-type]
            volatility=document["volatility"],  # type: ignore[arg-type]
            scenarios=document["scenarios"],  # type: ignore[arg-type]
            policy=document["policy"],  # type: ignore[arg-type]
            risk_authority=document["risk_authority"],  # type: ignore[arg-type]
            cost=document["cost"],  # type: ignore[arg-type]
            gate=document["gate"],  # type: ignore[arg-type]
            funnel_trace=hostile_trace,
        )


def test_post_build_funnel_trace_mutation_is_rejected_by_artifact_hash() -> None:
    artifact = _trade_artifact(funnel_trace=_funnel_trace())
    document = artifact.identity_document()
    hostile_trace = {
        **document["funnel_trace"],  # type: ignore[misc]
        "pacing_capability_hash": "b" * 64,
    }
    hostile_pre_rank = {**hostile_trace, "ranked_count": 0}
    hostile_inputs = {
        **document["inputs"],  # type: ignore[misc]
        "funnel_trace": hostile_pre_rank,
        "universe": {"funnel_trace": hostile_pre_rank},
    }
    hostile_universe = {
        **document["universe"],  # type: ignore[misc]
        "funnel_trace": hostile_pre_rank,
    }

    with pytest.raises(ReplayBindingError, match="HISTORICAL_ARTIFACT_HASH_MISMATCH"):
        HistoricalPipelineArtifact(
            inputs=hostile_inputs,
            universe=hostile_universe,
            broker_evidence=document["broker_evidence"],  # type: ignore[arg-type]
            candidates=tuple(document["candidates"]),  # type: ignore[arg-type]
            volatility=document["volatility"],  # type: ignore[arg-type]
            scenarios=tuple(document["scenarios"]),  # type: ignore[arg-type]
            policy=document["policy"],  # type: ignore[arg-type]
            risk_authority=document["risk_authority"],  # type: ignore[arg-type]
            cost=document["cost"],  # type: ignore[arg-type]
            gate=document["gate"],  # type: ignore[arg-type]
            artifact_hash=artifact.artifact_hash,
            funnel_trace=hostile_trace,
        )


def test_historical_artifact_rejects_active_mapping_without_invocation() -> None:
    calls = {"iter": 0, "getitem": 0, "len": 0}

    class ActiveMapping(Mapping[str, object]):
        def __getitem__(self, key: str) -> object:
            calls["getitem"] += 1
            raise AssertionError(key)

        def __iter__(self):
            calls["iter"] += 1
            raise AssertionError("must not iterate")

        def __len__(self) -> int:
            calls["len"] += 1
            raise AssertionError("must not measure")

    probe = ActiveMapping()
    for inputs in (probe, {"nested": probe}):
        with pytest.raises(TypeError, match="exact dict/list/tuple"):
            HistoricalPipelineArtifact.build(
                inputs=inputs,  # type: ignore[arg-type]
                universe={},
                broker_evidence={},
                candidates=(),
                volatility={},
                scenarios=(),
                policy={},
                risk_authority={},
                cost={},
                gate={},
            )
        assert calls == {"iter": 0, "getitem": 0, "len": 0}


def test_post_construction_dependency_replacement_and_composite_forgery_reject(
    tmp_path: Path,
) -> None:
    artifact = _trade_artifact()
    pipeline = TestHistoricalDecisionPipeline.from_point_in_time_artifact(
        artifact,
        decision_at=DECISION_AT,
        pipeline_version="decision-pipeline-v1",
        independence_store=_independence_store(),
        independence_hash=_independence_fixture().spec_hash,
        verification_as_of=VERIFY_AT,
    )
    calls = {"inputs": 0}

    class EvilPipeline:
        def run_slot(self, *_: object, **__: object) -> object:
            calls["inputs"] += 1
            raise AssertionError("must not be called")

    with pytest.raises(ReplaySafetyError, match="HISTORICAL_PIPELINE_IMMUTABLE"):
        pipeline._artifact = EvilPipeline()  # type: ignore[assignment]
    with pytest.raises(ReplaySafetyError, match="HISTORICAL_PIPELINE_IMMUTABLE"):
        pipeline.inputs = EvilPipeline()  # type: ignore[attr-defined]
    with pytest.raises(ReplaySafetyError, match="HISTORICAL_PIPELINE_IMMUTABLE"):
        pipeline.ranking_store = EvilPipeline()  # type: ignore[attr-defined]
    assert calls == {"inputs": 0}

    class Composite(TestHistoricalDecisionPipeline):
        pass

    forged = object.__new__(Composite)
    with pytest.raises(ReplaySafetyError, match="HISTORICAL_PIPELINE_REQUIRED"):
        PointInTimeReplay(forged)
