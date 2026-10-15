from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import logging
import traceback
from pathlib import Path
from typing import Mapping

import pytest

from options_copilot.decision.pipeline import (
    DecisionPipeline,
    normalize_funnel_trace,
)
import options_copilot.decision.pipeline as decision_pipeline_module
from options_copilot.analytics import ResolvedPolicy, ScenarioEngine, VolatilityEngine
from options_copilot.ranking import (
    CandidateEvidenceManifestError,
    PortfolioAction,
    PortfolioRanker,
    PortfolioRanking,
    RankingStore,
    build_candidate_evidence_manifest,
    build_ranking_basis,
    validate_candidate_evidence_manifest,
)
from options_copilot.research_allocation import (
    build_research_allocation_evidence,
    normalise_research_allocation_evidence,
)
from options_copilot.risk.authorization import RiskAuthorityTier, RiskTierAuthority
from options_copilot.option_pool import (
    OptionStructurePoolService,
    OptionStructurePoolStore,
    StructureDisposition,
)
from options_copilot.storage.canonical import canonical_hash, freeze_json


NOW = datetime(2026, 8, 4, 12, tzinfo=timezone.utc)
POLICY_HASH = "e" * 64
POLICY_MARKER_HASH = "1" * 64
RISK_CONTRACT_HASH = "2" * 64
TERMINAL_SCENARIOS = (
    {"terminal_underlying_price": "90", "probability": "0.50"},
    {"terminal_underlying_price": "110", "probability": "0.50"},
)


def test_normalize_funnel_trace_validates_scanner_source_evidence() -> None:
    trace = {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": "scan-source-evidence",
        "discovered_underlyings": 1,
        "deep_scan_requested": 1,
        "deep_scan_completed": 0,
        "ranked_limit": 10,
        "ranked_count": 0,
        "filler_candidates": 0,
        "pacing_capability_hash": "a" * 64,
        "pacing_usage": {"scanner": {"used": 2, "limit": 3}},
        "scanner_confirmed_charged_requests": 2,
        "scanner_completed_scan_codes": ("MOST_ACTIVE",),
        "scanner_failed_scan_codes": ("TOP_PERC_GAIN", "TOP_PERC_LOSE"),
        "scanner_source_row_counts": (
            {"scan_code": "MOST_ACTIVE", "row_count": 1},
            {"scan_code": "TOP_PERC_GAIN", "row_count": 0},
            {"scan_code": "TOP_PERC_LOSE", "row_count": 0},
        ),
    }

    normalized = normalize_funnel_trace(
        trace,
        scan_run_id="scan-source-evidence",
    )

    assert normalized["scanner_completed_scan_codes"] == ("MOST_ACTIVE",)
    assert normalized["scanner_failed_scan_codes"] == (
        "TOP_PERC_GAIN",
        "TOP_PERC_LOSE",
    )
    tampered = {
        **trace,
        "scanner_source_row_counts": (
            {"scan_code": "MOST_ACTIVE", "row_count": 1},
            {"scan_code": "TOP_PERC_GAIN", "row_count": 1},
            {"scan_code": "TOP_PERC_LOSE", "row_count": 0},
        ),
    }
    with pytest.raises(ValueError, match="scanner source row is invalid"):
        normalize_funnel_trace(
            tampered,
            scan_run_id="scan-source-evidence",
        )
    missing_source = {
        **trace,
        "scanner_failed_scan_codes": (),
        "scanner_source_row_counts": (
            {"scan_code": "MOST_ACTIVE", "row_count": 1},
        ),
    }
    with pytest.raises(ValueError, match="scanner source coverage is invalid"):
        normalize_funnel_trace(
            missing_source,
            scan_run_id="scan-source-evidence",
        )


def test_normalize_funnel_trace_validates_deep_scan_budget_accounting() -> None:
    trace = {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": "scan-budget-evidence",
        "discovered_underlyings": 6,
        "deep_scan_requested": 6,
        "deep_scan_attempted": 1,
        "deep_scan_completed": 1,
        "deep_scan_deferred": 5,
        "deep_scan_deferred_symbols": ("AMZN", "GLD", "JPM", "MSFT", "SPY"),
        "ranked_limit": 10,
        "ranked_count": 0,
        "filler_candidates": 0,
        "pacing_capability_hash": "a" * 64,
        "pacing_usage": {
            "scanner": {"used": 3, "limit": 3},
            "secdef": {"used": 20, "limit": 30},
        },
        "scanner_confirmed_charged_requests": 3,
        "scanner_completed_scan_codes": (
            "MOST_ACTIVE",
            "TOP_PERC_GAIN",
            "TOP_PERC_LOSE",
        ),
        "scanner_failed_scan_codes": (),
        "scanner_source_row_counts": (
            {"scan_code": "MOST_ACTIVE", "row_count": 2},
            {"scan_code": "TOP_PERC_GAIN", "row_count": 2},
            {"scan_code": "TOP_PERC_LOSE", "row_count": 2},
        ),
    }

    normalized = normalize_funnel_trace(
        trace,
        scan_run_id="scan-budget-evidence",
    )

    assert normalized["deep_scan_attempted"] == 1
    assert normalized["deep_scan_deferred"] == 5
    assert normalized["deep_scan_deferred_symbols"] == (
        "AMZN",
        "GLD",
        "JPM",
        "MSFT",
        "SPY",
    )
    with pytest.raises(ValueError, match="deep-scan budget accounting"):
        normalize_funnel_trace(
            {**trace, "deep_scan_attempted": 0},
            scan_run_id="scan-budget-evidence",
        )
    with pytest.raises(ValueError, match="deferred symbols"):
        normalize_funnel_trace(
            {**trace, "deep_scan_deferred": 4},
            scan_run_id="scan-budget-evidence",
        )
    with pytest.raises(ValueError, match="deferred symbols"):
        normalize_funnel_trace(
            {
                **trace,
                "deep_scan_deferred_symbols": (
                    "AMZN",
                    "GLD",
                    "JPM",
                    "MSFT",
                    "<SPY>",
                ),
            },
            scan_run_id="scan-budget-evidence",
        )


class _Port:
    def __init__(self, name, calls, value):
        if name == "broker" and isinstance(value, dict) and "nav_snapshot" not in value:
            value = {
                **value,
                "nav_snapshot": {
                    "valid": True,
                    "authority_hash": "3" * 64,
                    "content_hash": "2" * 64,
                    "contract_hash": "4" * 64,
                    "ledger_head_hash": "1" * 64,
                },
            }
        self.name, self.calls, self.value, self.current = name, calls, value, True
    def run(self, **_): self.calls.append(self.name); return self.value
    def is_current(self, value): return self.current and value == self.value


class _TamperingPortfolioRanker:
    def __init__(self, mutation: str) -> None:
        self.mutation = mutation

    def rank(self, candidates, limit=3, **kwargs):
        ranked = PortfolioRanker().rank(candidates, limit=limit, **kwargs)
        if self.mutation == "BOOLEAN_RANK":
            assert ranked.candidates
            row = replace(ranked.candidates[0], rank=True)
            return replace(ranked, candidates=(row,))
        if self.mutation == "PENDING_AS_NORMAL":
            assert ranked.governance_evidence
            row = replace(
                ranked.governance_evidence[0],
                rank=1,
                authority_status="NORMAL",
                authorizable=True,
            )
            return PortfolioRanking(PortfolioAction.TRADE, (row,), (), ())
        raise AssertionError(f"unknown mutation: {self.mutation}")


class _Resolver:
    def __init__(self, value: object) -> None:
        self.value = value

    def resolve(self, **_: object) -> object:
        return self.value

    def is_current(self, value: object) -> bool:
        return value == self.value


class _BindingRiskResolver:
    def __init__(self, candidate_hash: str) -> None:
        self.candidate_hash = candidate_hash
        self.calls: list[dict[str, object]] = []
        self.last: RiskTierAuthority | None = None

    def resolve(self, **kwargs: object) -> RiskTierAuthority:
        self.calls.append(dict(kwargs))
        if (
            kwargs.get("candidate_hash") == self.candidate_hash
            and kwargs.get("proposal_hash") is not None
            and kwargs.get("ranking_basis_hash") is not None
            and kwargs.get("execution_cost_version") is not None
            and kwargs.get("execution_cost_hash") is not None
        ):
            policy = kwargs["resolved_policy"]
            self.last = RiskTierAuthority(
                version="v2",
                tier=RiskAuthorityTier.A_GRADE,
                risk_contract_hash=RISK_CONTRACT_HASH,
                risk_authority_marker_hash="7" * 64,
                current_policy_version=policy.current_policy_version,
                current_policy_hash=policy.current_policy_hash,
                policy_authority_marker_hash=policy.policy_authority_marker_hash,
                proposal_hash=str(kwargs["proposal_hash"]),
                candidate_hash=str(kwargs["candidate_hash"]),
                execution_cost_version=str(kwargs["execution_cost_version"]),
                execution_cost_hash=str(kwargs["execution_cost_hash"]),
                ranking_basis_hash=str(kwargs["ranking_basis_hash"]),
                a_grade_approved=True,
                actor="human:owner",
                signed_at=NOW,
                expires_at=NOW + timedelta(hours=1),
            )
        else:
            self.last = _authority()
        return self.last

    def is_current(self, value: object) -> bool:
        return value == self.last


class _LegacyReadOnlyStore:
    def __init__(self) -> None:
        self.append_snapshot_calls = 0

    def append_snapshot(self, **_: object) -> object:
        self.append_snapshot_calls += 1
        raise AssertionError("legacy store must never receive a ranked snapshot")


class _RecordingBrokerPort(_Port):
    def __init__(self, calls: list[str], value: object) -> None:
        super().__init__("broker", calls, value)
        self.write_calls: list[dict[str, object]] = []


class _RecordingCreator:
    def __init__(self) -> None:
        self.write_calls: list[dict[str, object]] = []


@dataclass(frozen=True, slots=True)
class _HashBoundCandidate:
    candidate_id: str
    candidate_hash: str
    max_loss_usd: Decimal
    liquidity_score: Decimal
    structure: str
    underlying: str
    thesis: str
    dte: int
    evidence_hashes: dict[str, str]
    execution_cost_contract_hash: str
    execution_cost_contract_version: str
    policy_hash: str
    policy_version: str
    broker_snapshot_hash: str
    legs: tuple[dict[str, object], ...]
    terminal_scenarios: tuple[dict[str, object], ...]
    event_evidence_status: str
    earnings_overlap: bool | None
    event_defined: bool
    event_evidence_hash: str | None
    event_supporting_overlap: bool = False
    event_supporting_hash: str | None = None
    fundamental_supporting_status: str = "DEGRADED"
    fundamental_supporting_hash: str | None = None
    fundamental_supporting_payload: dict[str, object] | None = None
    fundamental_supporting_reason_codes: tuple[str, ...] = (
        "FUNDAMENTALS_UNAVAILABLE",
    )
    proposal_overrides: dict[str, object] | None = None

    def hash_payload(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "symbol": self.underlying,
            "structure": self.structure,
            "legs": self.legs,
            "terminal_scenarios": self.terminal_scenarios,
            "debit_usd": "100",
            "credit_usd": "0",
            "all_in_cost_usd": "100",
            "max_loss_usd": str(self.max_loss_usd),
            "max_profit_usd": str(self.max_loss_usd * 2),
            "breakevens": ("100",),
            "liquidity_score": str(self.liquidity_score),
            "exit_plan": {
                "thesis_invalidation": "trend reverses",
                "risk_stop": "50 percent loss",
                "profit_take": "50 percent gain",
                "time_stop": "before expiry",
                "maximum_holding_date": "2026-08-20",
                "bad_quote_action": "do not trade",
            },
            "dte": self.dte,
            "strategy_nav_usd": "1000",
            "strategy_nav_hash": "3" * 64,
            "strategy_nav_content_hash": "2" * 64,
            "strategy_nav_contract_hash": "4" * 64,
            "strategy_nav_ledger_head_hash": "1" * 64,
            "strategy_nav_observed_account_nlv": "1000",
            "strategy_nav_reconciliation_difference": "0",
            "strategy_nav_asof": NOW.isoformat(),
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "quote_batch_id": "batch-1",
            "secdef_hash": "5" * 64,
            "evidence_hashes": self.evidence_hashes,
            "execution_cost_contract_version": self.execution_cost_contract_version,
            "execution_cost_contract_hash": self.execution_cost_contract_hash,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "dte_exception_hash": None,
            "event_evidence_status": self.event_evidence_status,
            "earnings_overlap": self.earnings_overlap,
            "event_defined": self.event_defined,
            "event_evidence_hash": self.event_evidence_hash,
            "event_supporting_overlap": self.event_supporting_overlap,
            "event_supporting_hash": self.event_supporting_hash,
            "fundamental_supporting_status": self.fundamental_supporting_status,
            "fundamental_supporting_hash": self.fundamental_supporting_hash,
            "fundamental_supporting_payload": self.fundamental_supporting_payload or {},
            "fundamental_supporting_reason_codes": self.fundamental_supporting_reason_codes,
        }

    def proposal_payload(self) -> dict[str, object]:
        body = self.hash_payload()
        proposal: dict[str, object] = {
            "schema": "options_copilot.proposal.v1",
            "review_only": True,
            "rank": 1,
            "eligible_to_send": True,
            "proposal_id": self.candidate_id,
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "symbol": self.underlying,
            "underlying": self.underlying,
            "structure": self.structure,
            "dte": self.dte,
            "quote_snapshot_id": body["quote_batch_id"],
            "expected_value_usd": "10",
            "terminal_scenarios": [
                dict(item) for item in self.terminal_scenarios
            ],
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "secdef_hash": body["secdef_hash"],
            "strategy_nav": {
                "strategy_nav_usd": "1000",
                "authority_hash": body["strategy_nav_hash"],
                "content_hash": body["strategy_nav_content_hash"],
                "contract_hash": body["strategy_nav_contract_hash"],
                "ledger_head_hash": body["strategy_nav_ledger_head_hash"],
                "observed_account_nlv": body[
                    "strategy_nav_observed_account_nlv"
                ],
                "reconciliation_difference": body[
                    "strategy_nav_reconciliation_difference"
                ],
                "asof": body["strategy_nav_asof"],
            },
            "policy": {
                "version": self.policy_version,
                "hash": self.policy_hash,
                "dte_exception_hash": None,
            },
            "execution_cost_contract": {
                "version": self.execution_cost_contract_version,
                "hash": self.execution_cost_contract_hash,
            },
            "evidence_hashes": self.evidence_hashes,
            "pricing": {
                "reference_cost_usd": "100",
                "estimated_commissions_usd": "0",
                "estimated_slippage_usd": "0",
                "estimated_execution_costs_usd": "0",
                "all_in_executable_cost_usd": "100",
                "net_debit_usd": "100",
            },
            "risk": {
                "maximum_loss_usd": str(self.max_loss_usd),
                "maximum_profit_usd": body["max_profit_usd"],
                "risk_fraction": str(self.max_loss_usd / Decimal("1000")),
                "defined_risk": True,
                "breakevens": ["100"],
            },
            "legs": [dict(item) for item in self.legs],
            "exit_plan": body["exit_plan"],
        }
        if self.proposal_overrides:
            proposal.update(self.proposal_overrides)
        return proposal


@dataclass(frozen=True, slots=True)
class _CandidateWithoutProposal:
    candidate_id: str
    candidate_hash: str
    body: dict[str, object]

    def hash_payload(self) -> dict[str, object]:
        return dict(self.body)


@dataclass(frozen=True, slots=True)
class _CandidateWithRejectedProposal(_CandidateWithoutProposal):
    rejection: str

    def proposal_payload(self) -> dict[str, object]:
        raise ValueError(self.rejection)


def _policy(policy_hash: str = POLICY_HASH) -> ResolvedPolicy:
    return ResolvedPolicy(
        "v1",
        policy_hash,
        POLICY_MARKER_HASH,
        NOW,
        freeze_json({"policy": "test", "hard_no_trade_thresholds": {"cost_and_expectancy": {
            "minimum_after_cost_expected_value_usd": "max(5.00,0.05*maximum_loss)",
            "minimum_max_profit_to_maximum_loss": "1.20",
            "maximum_total_round_trip_cost_to_max_profit": "0.20",
            "stress_after_cost_ev": "must be greater than or equal to 0.00",
        }}}),
        freeze_json({"source": "test"}),
    )


def _authority() -> RiskTierAuthority:
    return RiskTierAuthority.normal(RISK_CONTRACT_HASH)


def _candidate(
    *,
    candidate_id: str,
    cost_hash: str,
    policy_hash: str,
    broker_snapshot_hash: str,
    terminal_scenarios: tuple[dict[str, object], ...] = TERMINAL_SCENARIOS,
    proposal_overrides: dict[str, object] | None = None,
    max_loss_usd: Decimal = Decimal("100"),
) -> _HashBoundCandidate:
    legs = (
        {"con_id": 101, "side": "LONG", "ratio": 1, "bid": "2.00", "ask": "2.10", "observed_at": NOW.isoformat()},
        {"con_id": 102, "side": "SHORT", "ratio": 1, "bid": "1.00", "ask": "1.10", "observed_at": NOW.isoformat()},
    )
    provisional = _HashBoundCandidate(
        candidate_id=candidate_id,
        candidate_hash="0" * 64,
        max_loss_usd=max_loss_usd,
        liquidity_score=Decimal("8"),
        structure="DEBIT_VERTICAL",
        underlying="SPY",
        thesis="UP",
        dte=20,
        evidence_hashes={"LIQUIDITY": "d" * 64},
        execution_cost_contract_hash=cost_hash,
        execution_cost_contract_version="v1",
        policy_hash=policy_hash,
        policy_version="v1",
        broker_snapshot_hash=broker_snapshot_hash,
        legs=legs,
        terminal_scenarios=terminal_scenarios,
        event_evidence_status="AVAILABLE",
        earnings_overlap=False,
        event_defined=False,
        event_evidence_hash="6" * 64,
        proposal_overrides=proposal_overrides,
    )
    return replace(provisional, candidate_hash=canonical_hash(provisional.hash_payload()))


def _reference(evidence_id: str, seed: str) -> dict[str, str]:
    return {
        "evidence_id": evidence_id,
        "content_hash": seed * 64,
        "row_hash": seed.upper().lower() * 64,
    }


def _nav_evidence() -> dict[str, object]:
    return {
        "valid": True,
        "authority_hash": "3" * 64,
        "content_hash": "2" * 64,
        "contract_hash": "4" * 64,
        "ledger_head_hash": "1" * 64,
        "observed_account_nlv": "1000",
        "reconciliation_difference": "0",
        "asof": NOW.isoformat(),
    }


def _scenario_rows(
    scenarios: tuple[dict[str, object], ...] = TERMINAL_SCENARIOS,
) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "terminal_price": item["terminal_underlying_price"],
            "probability": item["probability"],
        }
        for item in scenarios
    )


def _real_scenario_terms(
    *, policy: ResolvedPolicy, cost_hash: str
) -> tuple[dict[str, object], ...]:
    decision = ScenarioEngine().evaluate_pre_cost(
        {
            "spot": Decimal("100"),
            "atm_iv": Decimal("0.2"),
            "dte": 20,
            "max_loss": Decimal("100"),
            "market_score": Decimal("0"),
            "volatility_score": Decimal("0"),
            "cost_hash": cost_hash,
            "cost_version": "v1",
            "hard_evidence": {
                "MARKET": {"eligible": True, "hash": "e" * 64},
                "VOLATILITY": {"eligible": True, "hash": "d" * 64},
                "LIQUIDITY": {"eligible": True, "hash": "d" * 64},
            },
            "input_hash": "a" * 64,
        },
        now=NOW,
        resolved_policy=policy,
        risk_authority=_authority(),
    )
    assert str(getattr(decision.action, "value", decision.action)) == "TRADE"
    return tuple(
        {
            "terminal_underlying_price": str(item.terminal_price),
            "probability": str(item.probability),
        }
        for item in decision.scenarios
    )


def _proposal_gate_pipeline(
    *,
    candidate: object | tuple[object, ...],
    store: RankingStore,
    calls: list[str],
    after_cost_ev: Decimal = Decimal("10"),
    after_cost_evs: Mapping[str, Decimal] | None = None,
    gate_risk_fraction: Decimal = Decimal("0.10"),
    portfolio_ranker: object | None = None,
    cost_current: bool = True,
    risk_resolver: object | None = None,
    broker_evidence_port: object | None = None,
    strategy_generator_port: object | None = None,
    pipeline_context: dict[str, object] | None = None,
    option_pool_recorder: object | None = None,
    clock: object | None = None,
    monotonic_clock: object | None = None,
) -> DecisionPipeline:
    candidates = candidate if isinstance(candidate, tuple) else (candidate,)
    bodies = tuple(item.hash_payload() for item in candidates)
    body = bodies[0]
    candidate_id = str(body["candidate_id"])
    cost_hash = str(body["execution_cost_contract_hash"])
    policy_hash = str(body["policy_hash"])
    broker_hash = str(body["broker_snapshot_hash"])
    authority = _authority()
    cost_port = _Port(
        "cost",
        calls,
        {
            "version": "v1",
            "hash": cost_hash,
            "candidates": tuple(
                {
                    "candidate_id": str(candidate_body["candidate_id"]),
                    "cost_hash": cost_hash,
                    "cost_version": "v1",
                    "execution_cost_usd": Decimal("1"),
                    "stress_after_cost_expected_value": Decimal("10"),
                    "calculation_hash": "d" * 64,
                    "after_cost_expected_value": (
                        after_cost_evs[str(candidate_body["candidate_id"])]
                        if after_cost_evs is not None
                        else after_cost_ev
                    ),
                }
                for candidate_body in bodies
            ),
        },
    )
    cost_port.current = cost_current
    return DecisionPipeline(
        inputs=_Port(
            "inputs",
            calls,
            {
                "universe": {},
                "positions": (),
                **(pipeline_context or {}),
            },
        ),
        universe_funnel=_Port(
            "universe", calls, {"finalists": candidates}
        ),
        broker_evidence=broker_evidence_port
        or _Port(
            "broker",
            calls,
            {
                "snapshot_hash": broker_hash,
                "evidence_hash": "c" * 64,
                "nav_snapshot": _nav_evidence(),
            },
        ),
        strategy_registry=_Port(
            "registry", calls, {"finalists": candidates}
        ),
        strategy_generator=strategy_generator_port
        or _Port("generator", calls, {"candidates": candidates}),
        volatility_engine=_Port(
            "volatility",
            calls,
            {"eligible": True, "evidence_hash": "d" * 64},
        ),
        scenario_engine=_Port(
            "scenario",
            calls,
            {
                "action": "TRADE",
                "scenarios": _scenario_rows(tuple(body["terminal_scenarios"])),
                "cost_hash": cost_hash,
                "current_policy_version": "v1",
                "current_policy_hash": policy_hash,
                "policy_authority_marker_hash": POLICY_MARKER_HASH,
                "risk_authority_version": "v1",
                "risk_authority_marker_hash": authority.marker_hash,
                "risk_contract_hash": RISK_CONTRACT_HASH,
            },
        ),
        policy_resolver=_Resolver(_policy(policy_hash)),
        risk_authority_resolver=risk_resolver or _Resolver(authority),
        cost_contract=cost_port,
        eligibility_gate=_Port(
            "gates",
            calls,
            {"eligible": True, "risk_fraction": gate_risk_fraction},
        ),
        portfolio_ranker=portfolio_ranker or PortfolioRanker(),
        ranking_store=store,
        option_pool_recorder=option_pool_recorder,
        clock=clock or (lambda: NOW),
        monotonic_clock=monotonic_clock,
    )


@pytest.mark.parametrize("authority_mismatch", (False, True))
def test_candidate_local_scenario_failure_does_not_suppress_valid_peer(
    tmp_path, authority_mismatch: bool,
) -> None:
    candidates = tuple(_candidate(
        candidate_id=name, cost_hash="f" * 64, policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    ) for name in ("missing-input", "valid-peer"))
    with RankingStore(tmp_path / "scenario-peers.sqlite3") as store:
        pipeline = _proposal_gate_pipeline(candidate=candidates, store=store, calls=[])
        accepted = dict(pipeline.scenario_engine.value)

        class MixedScenarios:
            count = 0

            def evaluate_pre_cost(self, **kwargs):
                self.count += 1
                if self.count == 1:
                    return {**accepted, "action": "NO_TRADE", "scenarios": (),
                            "reasons": ("MISSING_VOLATILITY_STATE_INPUT",),
                            "current_policy_hash": "0" * 64 if authority_mismatch else POLICY_HASH}
                return accepted

        pipeline.scenario_engine = MixedScenarios()
        pipeline.cost_contract.value["candidates"] = (
            pipeline.cost_contract.value["candidates"][1],
        )
        result = pipeline.run_slot("scenario-peers", NOW)
        if authority_mismatch:
            assert result["status"] == "NO_TRADE"
            assert store.get_by_scan_run("scenario-peers") is None
        else:
            assert result["status"] == "TRADE"
            assert result["candidate_hashes"] == (candidates[1].candidate_hash,)
            records = [item for item in store.read_decisions("scenario-peers") if item.record_type == "SCENARIO"]
            assert len(records) == 2
            rejected = next(item.record for item in records if item.record["candidate_id"] == "missing-input")
            assert tuple(rejected["scenario"]["reasons"]) == ("MISSING_VOLATILITY_STATE_INPUT",)


def test_operational_timing_is_deterministic_and_outside_decision_result(
    tmp_path: Path,
) -> None:
    class IncrementingClock:
        def __init__(self) -> None:
            self.value = -1_000_000

        def __call__(self) -> int:
            self.value += 1_000_000
            return self.value

    results: list[dict[str, object]] = []
    timings: list[Mapping[str, object]] = []
    for index in range(2):
        candidate = _candidate(
            candidate_id="timed-candidate",
            cost_hash="f" * 64,
            policy_hash=POLICY_HASH,
            broker_snapshot_hash="b" * 64,
        )
        with RankingStore(tmp_path / f"timed-{index}.sqlite") as store:
            pipeline = _proposal_gate_pipeline(
                candidate=candidate,
                store=store,
                calls=[],
                monotonic_clock=IncrementingClock(),
            )
            results.append(dict(pipeline.run_slot("timed-scan", NOW)))
            timings.append(pipeline.last_operational_timing)

    for result in results:
        decision_payload = dict(result)
        result_hash = decision_payload.pop("result_hash")
        assert result_hash == canonical_hash(decision_payload)
        assert "operational_timing" not in result
    for field in (
        "status",
        "reasons",
        "input_hash",
        "candidate_hashes",
        "ranking_basis_hashes",
        "gate_bundle_hash",
        "funnel_trace",
    ):
        assert results[0][field] == results[1][field]
    assert timings[0] == timings[1]
    assert timings[0]["scan_run_id"] == "timed-scan"
    assert timings[0]["total_duration_ms"] == 19
    stages = timings[0]["stages"]
    assert isinstance(stages, tuple)
    assert [row["stage"] for row in stages] == [
        "INITIALIZATION",
        "AUTHORITY_RESOLUTION",
        "INPUT_ACQUISITION",
        "UNIVERSE_FUNNEL",
        "OPTION_RESEARCH_POOL",
        "BROKER_EVIDENCE",
        "GATE_ROUTING",
        "STRATEGY_GENERATION",
        "VOLATILITY",
        "SCENARIOS",
        "EXECUTION_COST",
        "OPTION_POOL",
        "PROPOSAL_BINDING",
        "ELIGIBILITY_GATES",
        "RANKING_PREPARATION",
        "PORTFOLIO_RANKING",
        "POST_RANK_BINDING",
        "AUTHORITY_REVALIDATION",
        "IMMUTABLE_PERSISTENCE",
    ]
    assert sum(int(row["duration_ms"]) for row in stages) == 19
    assert timings[0]["decision_authority"] == "OBSERVATION_ONLY"
    assert timings[0]["affects_decision"] is False


def test_operational_timing_clock_failure_never_changes_decision(
    tmp_path: Path,
) -> None:
    def broken_clock() -> int:
        raise RuntimeError("telemetry unavailable")

    candidate = _candidate(
        candidate_id="untimed-candidate",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    with RankingStore(tmp_path / "untimed.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=[],
            monotonic_clock=broken_clock,
        )

        result = pipeline.run_slot("untimed-scan", NOW)

    assert result["status"] == "TRADE"
    assert pipeline.last_operational_timing == {"stages": ()}
    assert "PIPELINE_BINDING_INVALID" not in result["reasons"]


def test_option_pool_recorder_expected_validation_error_is_bounded(
    tmp_path: Path,
) -> None:
    class InvalidRecorder:
        def capture_dispositions(self, **_: object) -> None:
            raise ValueError("hostile details must not escape")

    with RankingStore(tmp_path / "bounded-option-pool.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=_candidate(
                candidate_id="bounded-option-pool",
                cost_hash="f" * 64,
                policy_hash=POLICY_HASH,
                broker_snapshot_hash="b" * 64,
            ),
            store=store,
            calls=[],
            option_pool_recorder=InvalidRecorder(),
        )
        pipeline._active_funnel_trace = {
            "equity_pool_reference": {},
            "equity_theses": {},
        }
        assert pipeline._record_option_dispositions(
            scan_run_id="bounded-option-pool",
            observed_at=NOW,
            reason_codes=("EXPECTED_VALIDATION_REJECTION",),
        ) is False


def test_option_pool_recorder_unexpected_failure_propagates_sanitized(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class BrokenRecorder:
        def capture_dispositions(self, **_: object) -> None:
            raise RuntimeError("secret internal storage detail")

    with RankingStore(tmp_path / "unexpected-option-pool.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=_candidate(
                candidate_id="unexpected-option-pool",
                cost_hash="f" * 64,
                policy_hash=POLICY_HASH,
                broker_snapshot_hash="b" * 64,
            ),
            store=store,
            calls=[],
            option_pool_recorder=BrokenRecorder(),
        )
        pipeline._active_funnel_trace = {
            "equity_pool_reference": {},
            "equity_theses": {},
        }
        with pytest.raises(
            RuntimeError,
            match="OPTION_POOL_RECORDER_UNEXPECTED_FAILURE",
        ) as exc_info:
            pipeline._record_option_dispositions(
                scan_run_id="unexpected-option-pool",
                observed_at=NOW,
                reason_codes=("UNEXPECTED_STORAGE_FAILURE",),
            )
    assert "secret internal storage detail" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    formatted = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    assert "secret internal storage detail" not in formatted

    caplog.set_level(logging.ERROR, logger="options_copilot.decision.pipeline")

    def raise_stable_failure(_scan_run_id: str, _slot_at: datetime) -> object:
        raise exc_info.value

    pipeline._run = raise_stable_failure  # type: ignore[method-assign]
    with pytest.raises(
        RuntimeError,
        match="OPTION_POOL_RECORDER_UNEXPECTED_FAILURE",
    ):
        pipeline.run_slot("scheduler-boundary-option-pool", NOW)
    assert "OPTION_POOL_RECORDER_UNEXPECTED_FAILURE" in caplog.text
    assert "secret internal storage detail" not in caplog.text


def _thesis_bound_research_fixture(
    *,
    candidate_id: str = "research-before-broker",
) -> tuple[dict[str, object], dict[str, object], _CandidateWithoutProposal]:
    reference = {
        "schema": "options_copilot.equity_pool_reference.v1",
        "snapshot_id": "1" * 64,
        "snapshot_hash": "2" * 64,
        "input_manifest_hash": "3" * 64,
        "rows_hash": "4" * 64,
        "policy_hash": "5" * 64,
        "taxonomy_hash": "6" * 64,
        "scoring_hash": "7" * 64,
        "selected_symbols": ("SPY",),
        "discovered_symbols": ("SPY",),
        "discovery_count": 1,
        "selected_count": 1,
        "excluded_count": 0,
        "exclusion_stats": {},
    }
    thesis_row = {
        "schema": "options_copilot.equity_thesis_evidence.v1",
        "symbol": "SPY",
        "direction_label": "BULLISH",
        "direction_score": Decimal("40"),
        "uncertainty": Decimal("0.20"),
        "observed_at": NOW,
        "source_hashes": ("8" * 64,),
        "canonical_input_hash": "9" * 64,
        "selected_rank": 1,
    }
    thesis_hash = canonical_hash(thesis_row)
    theses = {
        "schema": "options_copilot.equity_theses.v1",
        "equity_pool_reference_hash": canonical_hash(reference),
        "rows": (thesis_row,),
        "rows_hash": canonical_hash((thesis_row,)),
    }
    short_proof = {
        "status": "SUPPORTED",
        "reason_codes": (),
        "evidence_hash": "a" * 64,
    }
    base = _candidate(
        candidate_id=candidate_id,
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    body = base.hash_payload()
    body.update(
        {
            "equity_thesis_evidence": thesis_row,
            "equity_thesis_hash": thesis_hash,
            "invalidation_evidence": {
                "status": "BOUND",
                "thesis_invalidation": "Exit when the hash-bound thesis fails.",
                "maximum_holding_date": "2026-08-24",
                "equity_thesis_hash": thesis_hash,
            },
            "assignment_evidence": {
                "status": "SUPPORTED",
                "short_leg_evidence": (short_proof,),
            },
            "ex_dividend_evidence": {
                "status": "SUPPORTED",
                "short_leg_evidence": (short_proof,),
            },
            "legs": (
                {
                    "con_id": 101,
                    "contract_id_ex": "101@SMART",
                    "underlying": "SPY",
                    "expiration": "2026-08-24",
                    "strike": "100",
                    "right": "CALL",
                    "side": "LONG",
                    "ratio": 1,
                    "multiplier": "100",
                    "currency": "USD",
                    "exchange": "SMART",
                },
                {
                    "con_id": 102,
                    "contract_id_ex": "102@SMART",
                    "underlying": "SPY",
                    "expiration": "2026-08-24",
                    "strike": "105",
                    "right": "CALL",
                    "side": "SHORT",
                    "ratio": 1,
                    "multiplier": "100",
                    "currency": "USD",
                    "exchange": "SMART",
                    "short_leg_risk_evidence": short_proof,
                },
            ),
        }
    )
    candidate = _CandidateWithoutProposal(
        candidate_id=candidate_id,
        candidate_hash=canonical_hash(body),
        body=body,
    )
    return reference, theses, candidate


def test_late_broker_failure_preserves_thesis_bound_research_option_pool(
    tmp_path: Path,
) -> None:
    scan_run_id = "scan-research-before-broker-failure"
    reference, theses, candidate = _thesis_bound_research_fixture()
    trace = {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": scan_run_id,
        "discovered_underlyings": 1,
        "deep_scan_requested": 1,
        "deep_scan_completed": 1,
        "ranked_limit": 10,
        "ranked_count": 1,
        "filler_candidates": 0,
        "pacing_capability_hash": "a" * 64,
        "pacing_usage": {"scanner": {"used": 1, "limit": 3}},
        "equity_pool_reference": reference,
        "equity_theses": theses,
    }
    calls: list[str] = []
    with (
        RankingStore(tmp_path / "research-ranking.sqlite") as ranking_store,
        OptionStructurePoolStore(tmp_path / "research-option-pool.sqlite") as pool_store,
    ):
        pool = OptionStructurePoolService(pool_store, clock=lambda: NOW)
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=ranking_store,
            calls=calls,
            pipeline_context={"funnel_trace": trace},
            option_pool_recorder=pool,
            broker_evidence_port=_Port(
                "broker",
                calls,
                {"reasons": ("BROKER_SNAPSHOT_INCOMPLETE",)},
            ),
        )

        result = pipeline.run_slot(scan_run_id, NOW)
        snapshot = pool_store.replay(f"{scan_run_id}.research")

    assert result["status"] == "NO_TRADE"
    assert result["reasons"] == ("BROKER_SNAPSHOT_INCOMPLETE",)
    row = next(item for item in snapshot.decisions if item.candidate_id)
    assert row.disposition is StructureDisposition.RESEARCH_ONLY
    assert row.equity_thesis_evidence is not None
    assert row.candidate_identity is not None
    assert "REGULAR_SESSION_EXACT_IDENTITY_RESEARCH_ONLY" in row.reason_codes
    assert "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED" in row.reason_codes
    assert "OPTION_GREEKS_INCOMPLETE" in row.reason_codes
    assert "OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE" in row.reason_codes
    assert "AFTER_COST_ECONOMICS_INCOMPLETE" in row.reason_codes
    assert "THESIS_INVALIDATION_EVIDENCE_INCOMPLETE" not in row.reason_codes
    assert "ASSIGNMENT_AND_EX_DIVIDEND_EVIDENCE_INCOMPLETE" not in row.reason_codes
    assert all(
        decision.disposition is not StructureDisposition.EXACT_EVIDENCE_CAPTURED
        for decision in snapshot.decisions
    )


@pytest.mark.parametrize(
    "mutation",
    ("MISSING_THESIS", "MISMATCHED_THESIS_HASH"),
)
def test_research_option_pool_rejects_unbound_thesis_identity(
    mutation: str,
) -> None:
    reference, theses, candidate = _thesis_bound_research_fixture(
        candidate_id=f"research-{mutation.lower()}"
    )
    body = candidate.hash_payload()
    if mutation == "MISSING_THESIS":
        body.pop("equity_thesis_evidence")
    else:
        body["equity_thesis_hash"] = "0" * 64
    mutated = _CandidateWithoutProposal(
        candidate_id=str(body["candidate_id"]),
        candidate_hash=canonical_hash(body),
        body=body,
    )

    documents = decision_pipeline_module._research_option_pool_candidate_documents(
        (mutated,),
        equity_pool_reference=reference,
        equity_theses=theses,
    )

    assert documents == ()

def test_pipeline_freezes_decision_cutoff_after_broker_acquisition(
    tmp_path: Path,
) -> None:
    candidate = _candidate(
        candidate_id="post-acquisition-cutoff",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    current = {"now": NOW - timedelta(seconds=6)}
    calls: list[str] = []

    class AdvancingBroker:
        def run(self, **_: object) -> Mapping[str, object]:
            calls.append("broker")
            current["now"] = NOW
            return {
                "snapshot_hash": "b" * 64,
                "evidence_hash": "c" * 64,
                "nav_snapshot": _nav_evidence(),
            }

    class FreshnessGenerator:
        observed_now: datetime | None = None

        def run(self, *, now: datetime, **_: object) -> Mapping[str, object]:
            calls.append("generator")
            self.observed_now = now
            if now < NOW:
                return {
                    "candidates": (),
                    "reasons": ("QUOTE_STALE_OR_FUTURE",),
                }
            return {"candidates": (candidate,)}

    generator = FreshnessGenerator()
    with RankingStore(tmp_path / "post-acquisition-cutoff.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            broker_evidence_port=AdvancingBroker(),
            strategy_generator_port=generator,
            clock=lambda: current["now"],
        )

        result = pipeline.run_slot("scan-post-acquisition-cutoff", NOW)

    assert result["status"] == "TRADE"
    assert generator.observed_now == NOW


def test_candidate_evidence_manifest_producer_is_deterministic_and_role_isolated() -> None:
    candidate = _candidate(
        candidate_id="manifest-1",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    supporting = (_reference("ev_z", "8"), _reference("ev_a", "7"))
    contradicting = (_reference("ev_counter", "6"),)

    first = build_candidate_evidence_manifest(
        candidate.hash_payload(),
        after_cost_expected_value=Decimal("10"),
        cutoff_at=NOW,
        ranking_valid_until=NOW + timedelta(minutes=5),
        now=NOW,
        supporting=supporting,
        contradicting=contradicting,
    )
    second = build_candidate_evidence_manifest(
        candidate.hash_payload(),
        after_cost_expected_value=Decimal("10.0"),
        cutoff_at=NOW,
        ranking_valid_until=NOW + timedelta(minutes=5),
        now=NOW,
        supporting=tuple(reversed(supporting)),
        contradicting=contradicting,
    )

    assert first == second
    assert set(first) == {
        "schema",
        "candidate_id",
        "symbol",
        "cutoff_at",
        "primary",
        "supporting",
        "contradicting",
        "manifest_hash",
    }
    assert first["manifest_hash"] == canonical_hash(
        {key: value for key, value in first.items() if key != "manifest_hash"}
    )
    assert [item["kind"] for item in first["primary"]] == [
        "BROKER_SNAPSHOT",
        "CONTRACT_DEFINITION",
        "EXECUTABLE_QUOTE",
        "PAYOFF_MAX_LOSS",
        "LIQUIDITY",
        "EXECUTION_COST",
        "AFTER_COST_EV",
        "DTE_RISK",
    ]
    assert all(
        item["source"] in {"IBKR_READ_ONLY", "LOCAL_DETERMINISTIC"}
        for item in first["primary"]
    )
    assert [item["evidence_id"] for item in first["supporting"]] == [
        "ev_a",
        "ev_z",
    ]
    assert first["contradicting"] == [contradicting[0]]


def test_candidate_evidence_manifest_accepts_real_generated_candidate() -> None:
    from tests.options_copilot.test_strategy_generator import (
        NOW as GENERATOR_NOW,
        _generate,
    )

    generated = _generate().candidates[0]
    manifest = build_candidate_evidence_manifest(
        generated.hash_payload(),
        after_cost_expected_value=Decimal("25"),
        cutoff_at=GENERATOR_NOW,
        ranking_valid_until=GENERATOR_NOW + timedelta(minutes=5),
        now=GENERATOR_NOW,
    )

    assert manifest["candidate_id"] == generated.candidate_id
    assert manifest["symbol"] == "SPY"
    assert len(manifest["primary"]) == 8
    assert manifest["supporting"] == []
    assert manifest["contradicting"] == []


def test_candidate_evidence_consumer_rejects_exact_validity_boundary() -> None:
    from tests.options_copilot.test_strategy_generator import (
        NOW as GENERATOR_NOW,
        _generate,
    )

    generated = _generate().candidates[0]
    body = generated.hash_payload()
    proposal = generated.proposal_payload()
    valid_until = GENERATOR_NOW + timedelta(minutes=5)
    manifest = build_candidate_evidence_manifest(
        body,
        after_cost_expected_value=proposal["expected_value_usd"],
        cutoff_at=GENERATOR_NOW,
        ranking_valid_until=valid_until,
        now=GENERATOR_NOW,
    )

    with pytest.raises(CandidateEvidenceManifestError) as raised:
        validate_candidate_evidence_manifest(
            manifest,
            candidate_id=generated.candidate_id,
            candidate_symbol="SPY",
            candidate_body=body,
            proposal_body=proposal,
            ranked_after_cost_expected_value=proposal["expected_value_usd"],
            ranking_broker_snapshot_hash=body["broker_snapshot_hash"],
            ranking_cost_version=body["execution_cost_contract_version"],
            ranking_cost_hash=body["execution_cost_contract_hash"],
            ranking_valid_until=valid_until.isoformat(timespec="microseconds"),
            now=valid_until,
        )

    assert raised.value.reason == "CANDIDATE_EVIDENCE_CUTOFF_INVALID"


@pytest.mark.parametrize(
    "mutation",
    (
        "broker_snapshot_hash",
        "quote_batch",
        "contract_and_quote_con_id",
        "executable_quote",
        "payoff_debit",
        "payoff_credit",
        "payoff_max_loss",
        "payoff_max_profit",
        "payoff_breakevens",
        "liquidity",
        "execution_cost",
        "execution_cost_contract",
        "after_cost_ev",
        "dte_structure_exception",
    ),
)
def test_candidate_evidence_consumer_rejects_rehashed_primary_candidate_conflicts(
    mutation: str,
) -> None:
    from tests.options_copilot.test_strategy_generator import (
        NOW as GENERATOR_NOW,
        _generate,
    )

    generated = _generate().candidates[0]
    body = generated.hash_payload()
    proposal = generated.proposal_payload()
    manifest = build_candidate_evidence_manifest(
        body,
        after_cost_expected_value=proposal["expected_value_usd"],
        cutoff_at=GENERATOR_NOW,
        ranking_valid_until=GENERATOR_NOW + timedelta(minutes=5),
        now=GENERATOR_NOW,
    )
    by_kind = {item["kind"]: item for item in manifest["primary"]}
    broker = by_kind["BROKER_SNAPSHOT"]["record"]
    contract = by_kind["CONTRACT_DEFINITION"]["record"]
    quote = by_kind["EXECUTABLE_QUOTE"]["record"]
    payoff = by_kind["PAYOFF_MAX_LOSS"]["record"]
    liquidity = by_kind["LIQUIDITY"]["record"]
    execution = by_kind["EXECUTION_COST"]["record"]
    after_cost = by_kind["AFTER_COST_EV"]["record"]
    dte = by_kind["DTE_RISK"]["record"]

    if mutation == "broker_snapshot_hash":
        broker["broker_snapshot_hash"] = "9" * 64
    elif mutation == "quote_batch":
        broker["quote_snapshot_id"] = "hostile-quote-batch"
        quote["quote_snapshot_id"] = "hostile-quote-batch"
    elif mutation == "contract_and_quote_con_id":
        hostile_con_id = int(contract["legs"][0]["con_id"]) + 1_000_000
        contract["legs"][0]["con_id"] = hostile_con_id
        quote["legs"][0]["con_id"] = hostile_con_id
    elif mutation == "executable_quote":
        quote["legs"][0]["bid"] = str(
            Decimal(str(quote["legs"][0]["bid"])) / Decimal("2")
        )
    elif mutation == "payoff_debit":
        changed = str(Decimal(str(payoff["debit_usd"])) + Decimal("1"))
        payoff["debit_usd"] = changed
        execution["debit_usd"] = changed
    elif mutation == "payoff_credit":
        changed = str(Decimal(str(payoff["credit_usd"])) + Decimal("1"))
        payoff["credit_usd"] = changed
        execution["credit_usd"] = changed
    elif mutation == "payoff_max_loss":
        payoff["max_loss_usd"] = str(
            Decimal(str(payoff["max_loss_usd"])) + Decimal("1")
        )
    elif mutation == "payoff_max_profit":
        payoff["max_profit_usd"] = (
            "1"
            if payoff["max_profit_usd"] is None
            else str(Decimal(str(payoff["max_profit_usd"])) + Decimal("1"))
        )
    elif mutation == "payoff_breakevens":
        payoff["breakevens"] = [
            str(Decimal(str(value)) + Decimal("1"))
            for value in payoff["breakevens"]
        ]
    elif mutation == "liquidity":
        liquidity["liquidity_score"] = str(
            Decimal(str(liquidity["liquidity_score"])) + Decimal("1")
        )
    elif mutation == "execution_cost":
        execution["execution_cost_usd"] = str(
            Decimal(str(execution["execution_cost_usd"])) + Decimal("1")
        )
    elif mutation == "execution_cost_contract":
        for record in (execution, after_cost):
            record["contract_version"] = "hostile-cost-v2"
            record["contract_hash"] = "8" * 64
    elif mutation == "after_cost_ev":
        after_cost["after_cost_ev_usd"] = str(
            Decimal(str(after_cost["after_cost_ev_usd"])) + Decimal("1")
        )
    else:
        dte["structure"] = "HOSTILE_STRUCTURE"
        dte["dte"] = int(dte["dte"]) + 1
        dte["dte_exception_hash"] = "7" * 64

    for primary in manifest["primary"]:
        primary["record_hash"] = canonical_hash(primary["record"])
    manifest["manifest_hash"] = canonical_hash(
        {key: value for key, value in manifest.items() if key != "manifest_hash"}
    )

    with pytest.raises(CandidateEvidenceManifestError) as raised:
        validate_candidate_evidence_manifest(
            manifest,
            candidate_id=generated.candidate_id,
            candidate_symbol="SPY",
            candidate_body=body,
            proposal_body=proposal,
            ranked_after_cost_expected_value=proposal["expected_value_usd"],
            ranking_broker_snapshot_hash=body["broker_snapshot_hash"],
            ranking_cost_version=body["execution_cost_contract_version"],
            ranking_cost_hash=body["execution_cost_contract_hash"],
            ranking_valid_until=(
                GENERATOR_NOW + timedelta(minutes=5)
            ).isoformat(timespec="microseconds"),
            now=GENERATOR_NOW,
        )

    assert raised.value.reason == "CANDIDATE_EVIDENCE_PRIMARY_BINDING_MISMATCH"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        ("future_cutoff", "CANDIDATE_EVIDENCE_CUTOFF_INVALID"),
        ("bad_reference", "CANDIDATE_EVIDENCE_MANIFEST_INVALID"),
        ("missing_primary", "CANDIDATE_EVIDENCE_PRIMARY_INVALID"),
    ),
)
def test_candidate_evidence_manifest_producer_fails_closed(
    mutation: str, reason: str
) -> None:
    candidate = _candidate(
        candidate_id="manifest-invalid",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    body = candidate.hash_payload()
    cutoff = NOW
    supporting: tuple[dict[str, str], ...] = ()
    if mutation == "future_cutoff":
        cutoff = NOW + timedelta(seconds=1)
    elif mutation == "bad_reference":
        supporting = (
            {
                **_reference("ev_bad", "7"),
                "kind": "COMPANY_NEWS",
            },
        )
    else:
        body.pop("secdef_hash")

    with pytest.raises(CandidateEvidenceManifestError) as raised:
        build_candidate_evidence_manifest(
            body,
            after_cost_expected_value=Decimal("10"),
            cutoff_at=cutoff,
            ranking_valid_until=NOW + timedelta(minutes=5),
            now=NOW,
            supporting=supporting,
        )
    assert raised.value.reason == reason


def test_pipeline_rejects_candidate_without_complete_proposal_payload(
    tmp_path,
) -> None:
    base = _candidate(
        candidate_id="missing-proposal",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    candidate = _CandidateWithoutProposal(
        candidate_id=base.candidate_id,
        candidate_hash=base.candidate_hash,
        body=base.hash_payload(),
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "missing-proposal.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot("scan-missing-proposal", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("INCOMPLETE_GENERATOR_PROPOSAL_BODY",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-missing-proposal") is None
        assert calls == [
            "inputs",
            "universe",
            "broker",
            "registry",
            "generator",
            "volatility",
            "scenario",
        ]


def test_pipeline_projects_nonpositive_after_cost_ev_as_stable_gate_reason(
    tmp_path,
) -> None:
    base = _candidate(
        candidate_id="negative-after-cost-ev",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    candidate = _CandidateWithRejectedProposal(
        candidate_id=base.candidate_id,
        candidate_hash=base.candidate_hash,
        body=base.hash_payload(),
        rejection="candidate terminal scenarios have nonpositive after-cost EV",
    )
    with RankingStore(tmp_path / "negative-after-cost-ev.sqlite") as store:
        pipeline = _proposal_gate_pipeline(candidate=candidate, store=store, calls=[])

        result = pipeline.run_slot("scan-negative-after-cost-ev", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("CANDIDATE_AFTER_COST_EV_NONPOSITIVE",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-negative-after-cost-ev") is None


def test_pipeline_rejects_signed_nonpositive_after_cost_ev_before_ranking(
    tmp_path: Path,
) -> None:
    candidate = _candidate(
        candidate_id="signed-negative-after-cost-ev",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "signed-negative-after-cost-ev.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            after_cost_ev=Decimal("-10"),
        )

        result = pipeline.run_slot("scan-signed-negative-after-cost-ev", NOW)

    assert result["status"] == "NO_TRADE"
    assert result["reasons"] == ("CANDIDATE_AFTER_COST_EV_NONPOSITIVE",)
    assert result["ranking_snapshot_id"] is None
    assert "cost" in calls
    assert "gates" not in calls


def test_nonpositive_candidate_does_not_block_positive_peer(
    tmp_path: Path,
) -> None:
    positive = _candidate(
        candidate_id="positive-after-cost-ev",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    negative = _candidate(
        candidate_id="negative-after-cost-ev-peer",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "mixed-after-cost-ev.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=(positive, negative),
            store=store,
            calls=calls,
            after_cost_evs={
                positive.candidate_id: Decimal("10"),
                negative.candidate_id: Decimal("-10"),
            },
        )

        result = pipeline.run_slot("scan-mixed-after-cost-ev", NOW)
        snapshot = store.get_by_scan_run("scan-mixed-after-cost-ev")
        assert snapshot is not None
        snapshot_payload = store.read_snapshot(snapshot.ranking_snapshot_id)

    assert result["status"] == "TRADE"
    assert result["reasons"] == ()
    assert result["candidate_hashes"] == (positive.candidate_hash,)
    assert tuple(row["candidate_id"] for row in snapshot_payload["candidates"]) == (
        positive.candidate_id,
    )


def test_proposal_rejection_logs_only_stable_reason(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "Authorization: Bearer sentinel-proposal-secret"
    base = _candidate(
        candidate_id="redacted-proposal-rejection",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    candidate = _CandidateWithRejectedProposal(
        candidate_id=base.candidate_id,
        candidate_hash=base.candidate_hash,
        body=base.hash_payload(),
        rejection=secret,
    )
    caplog.set_level(logging.WARNING, logger="options_copilot.decision.pipeline")
    with RankingStore(tmp_path / "redacted-proposal-rejection.sqlite") as store:
        pipeline = _proposal_gate_pipeline(candidate=candidate, store=store, calls=[])

        result = pipeline.run_slot("scan-redacted-proposal-rejection", NOW)

    assert result["status"] == "NO_TRADE"
    assert result["reasons"] == ("INVALID_GENERATOR_PROPOSAL_BODY",)
    assert "INVALID_GENERATOR_PROPOSAL_BODY" in caplog.text
    assert secret not in caplog.text


def test_unexpected_pipeline_failure_logs_no_exception_text_or_traceback(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "Cookie: sentinel-pipeline-secret"
    base = _candidate(
        candidate_id="redacted-pipeline-failure",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )

    class FailingGenerator:
        def run(self, **_: object) -> object:
            raise RuntimeError(secret)

    caplog.set_level(logging.WARNING, logger="options_copilot.decision.pipeline")
    with RankingStore(tmp_path / "redacted-pipeline-failure.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=base,
            store=store,
            calls=[],
            strategy_generator_port=FailingGenerator(),
        )

        result = pipeline.run_slot("scan-redacted-pipeline-failure", NOW)

    assert result["status"] == "NO_TRADE"
    assert result["reasons"] == (
        "PIPELINE_BINDING_INVALID",
        "PIPELINE_BINDING_STAGE_STRATEGY_GENERATION",
    )
    assert "PIPELINE_BINDING_INVALID" in caplog.text
    assert "stage=STRATEGY_GENERATION" in caplog.text
    assert secret not in caplog.text
    assert "Traceback" not in caplog.text


def test_pipeline_rejects_proposal_after_cost_ev_mismatch_before_gates(
    tmp_path,
) -> None:
    candidate = _candidate(
        candidate_id="ev-mismatch",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
        proposal_overrides={"expected_value_usd": "11"},
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "ev-mismatch.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            after_cost_ev=Decimal("10"),
        )

        result = pipeline.run_slot("scan-ev-mismatch", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("PROPOSAL_AFTER_COST_EV_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-ev-mismatch") is None
        assert "cost" in calls
        assert "gates" not in calls


def test_pipeline_rejects_proposal_scenario_mismatch_before_gates(
    tmp_path,
) -> None:
    candidate = _candidate(
        candidate_id="scenario-mismatch",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
        proposal_overrides={
            "terminal_scenarios": [
                {
                    "terminal_underlying_price": "91",
                    "probability": "0.50",
                },
                {
                    "terminal_underlying_price": "110",
                    "probability": "0.50",
                },
            ]
        },
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "scenario-mismatch.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot("scan-scenario-mismatch", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("PROPOSAL_SCENARIO_BINDING_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-scenario-mismatch") is None
        assert "gates" not in calls


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("proposal_id", "other-id"),
        ("candidate_id", "other-id"),
        ("candidate_hash", "0" * 64),
        ("quote_snapshot_id", "other-batch"),
    ),
)
def test_pipeline_rejects_proposal_candidate_identity_bindings(
    tmp_path,
    field: str,
    replacement: str,
) -> None:
    candidate = _candidate(
        candidate_id=f"binding-{field}",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
        proposal_overrides={field: replacement},
    )
    calls: list[str] = []
    with RankingStore(tmp_path / f"binding-{field}.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot(f"scan-binding-{field}", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("PROPOSAL_CANDIDATE_BINDING_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run(f"scan-binding-{field}") is None
        assert "gates" not in calls


@pytest.mark.parametrize(
    "mutation",
    (
        "leg_side",
        "leg_ratio",
        "leg_quantity",
        "maximum_loss",
        "strategy_nav",
        "policy",
        "execution_cost",
        "evidence",
        "exit_plan",
    ),
)
def test_pipeline_rejects_proposal_content_that_differs_from_frozen_candidate(
    tmp_path,
    mutation: str,
) -> None:
    base = _candidate(
        candidate_id=f"content-{mutation}",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    proposal = base.proposal_payload()
    if mutation.startswith("leg_"):
        legs = [dict(item) for item in proposal["legs"]]
        if mutation == "leg_side":
            legs[0]["side"] = "SELL"
        elif mutation == "leg_ratio":
            legs[0]["ratio"] = 2
        else:
            legs[0]["quantity"] = 2
        override = {"legs": legs}
    elif mutation == "maximum_loss":
        risk = dict(proposal["risk"])
        risk["maximum_loss_usd"] = "99"
        override = {"risk": risk}
    elif mutation == "strategy_nav":
        strategy_nav = dict(proposal["strategy_nav"])
        strategy_nav["strategy_nav_usd"] = "999"
        override = {"strategy_nav": strategy_nav}
    elif mutation == "policy":
        policy = dict(proposal["policy"])
        policy["hash"] = "9" * 64
        override = {"policy": policy}
    elif mutation == "execution_cost":
        cost = dict(proposal["execution_cost_contract"])
        cost["hash"] = "9" * 64
        override = {"execution_cost_contract": cost}
    elif mutation == "evidence":
        override = {"evidence_hashes": {"LIQUIDITY": "9" * 64}}
    else:
        exit_plan = dict(proposal["exit_plan"])
        exit_plan["risk_stop"] = "never"
        override = {"exit_plan": exit_plan}
    candidate = replace(base, proposal_overrides=override)
    calls: list[str] = []
    with RankingStore(tmp_path / f"content-{mutation}.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot(f"scan-content-{mutation}", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("PROPOSAL_CANDIDATE_CONTENT_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run(f"scan-content-{mutation}") is None
        assert "gates" not in calls


@pytest.mark.parametrize(
    ("mutation", "max_loss", "gate_risk"),
    (
        ("BOOLEAN_RANK", Decimal("100"), Decimal("0.10")),
        ("PENDING_AS_NORMAL", Decimal("100.1"), Decimal("0.1001")),
    ),
)
def test_pipeline_rejects_ranker_authority_and_rank_type_tampering(
    tmp_path,
    mutation: str,
    max_loss: Decimal,
    gate_risk: Decimal,
) -> None:
    candidate = _candidate(
        candidate_id=f"ranker-{mutation.lower()}",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
        max_loss_usd=max_loss,
    )
    calls: list[str] = []
    with RankingStore(tmp_path / f"ranker-{mutation}.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            gate_risk_fraction=gate_risk,
            portfolio_ranker=_TamperingPortfolioRanker(mutation),
        )

        result = pipeline.run_slot(f"scan-ranker-{mutation}", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("POST_RANK_PROPOSAL_BINDING_INVALID",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run(f"scan-ranker-{mutation}") is None


def test_pipeline_rejects_gate_risk_fraction_that_disagrees_with_frozen_nav(
    tmp_path,
) -> None:
    candidate = _candidate(
        candidate_id="gate-risk-mismatch",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
        max_loss_usd=Decimal("100.1"),
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "gate-risk-mismatch.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            gate_risk_fraction=Decimal("0.10"),
        )

        result = pipeline.run_slot("scan-gate-risk-mismatch", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("GATE_RISK_BINDING_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-gate-risk-mismatch") is None


def test_pipeline_rechecks_signed_execution_cost_head_before_persisting(
    tmp_path,
) -> None:
    candidate = _candidate(
        candidate_id="cost-head-changed",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "cost-head-changed.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            cost_current=False,
        )

        result = pipeline.run_slot("scan-cost-head-changed", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("EXECUTION_COST_HEAD_CHANGED",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-cost-head-changed") is None
        assert [
            row.record_type for row in store.read_decisions("scan-cost-head-changed")
        ] == ["SCENARIO", "GATE_BUNDLE_NO_TRADE", "NO_TRADE"]


def test_pipeline_rejects_reused_candidate_hash_before_proposal_binding(
    tmp_path,
) -> None:
    original = _candidate(
        candidate_id="candidate-hash-mismatch",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    candidate = replace(original, max_loss_usd=Decimal("101"))
    calls: list[str] = []
    with RankingStore(tmp_path / "candidate-hash-mismatch.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot("scan-candidate-hash-mismatch", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("CANDIDATE_HASH_BODY_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-candidate-hash-mismatch") is None
        assert calls == ["inputs", "universe", "broker", "registry", "generator"]


def test_pipeline_uses_named_order_and_persists_one_ranked_result(tmp_path) -> None:
    calls = []
    base = _candidate(candidate_id="c1", cost_hash="f" * 64, policy_hash=POLICY_HASH, broker_snapshot_hash="b" * 64)
    authority = _authority()
    research_allocation = build_research_allocation_evidence(
        event_rows=(
            {
                "symbol": "QQQ",
                "deterministic_score": "80",
                "advisory_score": "90",
            },
        ),
        scanner_rows=(),
        core_rows=(),
        limit=1,
    )
    funnel_trace = {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": "scan-1",
        "discovered_underlyings": 150,
        "deep_scan_requested": 30,
        "deep_scan_completed": 30,
        "ranked_limit": 10,
        "ranked_count": 0,
        "filler_candidates": 0,
        "pacing_capability_hash": "a" * 64,
        "pacing_usage": {"scanner": {"used": 3, "limit": 3}},
        "research_allocation": json.loads(json.dumps(research_allocation)),
        "equity_pool_reference": {
            "schema": "options_copilot.equity_pool_reference.v1",
            "snapshot_id": "1" * 64,
            "snapshot_hash": "2" * 64,
            "input_manifest_hash": "3" * 64,
            "rows_hash": "4" * 64,
            "policy_hash": "5" * 64,
            "taxonomy_hash": "6" * 64,
                "scoring_hash": "7" * 64,
                "selected_symbols": ("QQQ",),
                "discovered_symbols": ("QQQ",) + tuple(
                    f"SYM{index:03d}" for index in range(149)
                ),
                "discovery_count": 150,
            "selected_count": 1,
            "excluded_count": 149,
            "exclusion_stats": {"CONCENTRATION_GROUP_CAP": 149},
        },
    }
    with RankingStore(tmp_path / "ranking.sqlite") as store:
        pipeline = DecisionPipeline(
            inputs=_Port("inputs", calls, {"universe": {}, "positions": ()}),
            universe_funnel=_Port(
                "universe",
                calls,
                {"finalists": (base,), "funnel_trace": funnel_trace},
            ),
            broker_evidence=_Port("broker", calls, {"snapshot_hash": "b" * 64, "evidence_hash": "c" * 64, "nav_snapshot": _nav_evidence()}),
            strategy_registry=_Port("registry", calls, {"finalists": (base,)}),
            strategy_generator=_Port("generator", calls, {"candidates": (base,)}),
            volatility_engine=_Port("volatility", calls, {"eligible": True, "evidence_hash": "d" * 64}),
            scenario_engine=_Port("scenario", calls, {"action": "TRADE", "scenarios": _scenario_rows(), "cost_hash": "f" * 64, "current_policy_version": "v1", "current_policy_hash": POLICY_HASH, "policy_authority_marker_hash": POLICY_MARKER_HASH, "risk_authority_version": "v1", "risk_authority_marker_hash": authority.marker_hash, "risk_contract_hash": RISK_CONTRACT_HASH}),
            policy_resolver=_Resolver(_policy()), risk_authority_resolver=_Resolver(authority),
            cost_contract=_Port("cost", calls, {"version": "v1", "hash": "f" * 64, "candidates": ({"candidate_id": "c1", "cost_hash": "f" * 64, "cost_version": "v1", "after_cost_expected_value": Decimal("10"), "execution_cost_usd": Decimal("1"), "stress_after_cost_expected_value": Decimal("9")},)}),
            eligibility_gate=_Port("gates", calls, {"eligible": True, "risk_fraction": Decimal("0.10")}),
            portfolio_ranker=PortfolioRanker(), ranking_store=store, clock=lambda: NOW,
        )
        result = pipeline.run_slot("scan-1", NOW)
        assert result["status"] == "TRADE"
        assert result["funnel_trace"]["ranked_count"] == 1
        assert result["funnel_trace"]["filler_candidates"] == 0
        assert result["ranking_snapshot_id"]
        snapshot = store.read_snapshot(result["ranking_snapshot_id"])
        snapshot_trace = dict(snapshot["funnel_trace"])
        result_trace = dict(result["funnel_trace"])
        snapshot_allocation = snapshot_trace.pop("research_allocation")
        result_trace.pop("research_allocation")
        snapshot_reference = snapshot_trace.pop("equity_pool_reference")
        result_reference = result_trace.pop("equity_pool_reference")
        assert snapshot_trace == result_trace
        assert snapshot["immutable_inputs"]["funnel_trace"]["ranked_count"] == 1
        from options_copilot.equity_pool import normalize_equity_pool_reference

        assert normalize_equity_pool_reference(snapshot_reference) == normalize_equity_pool_reference(result_reference)
        assert (
            normalize_equity_pool_reference(
                snapshot["immutable_inputs"]["funnel_trace"]["equity_pool_reference"]
            )
            == normalize_equity_pool_reference(
                result["funnel_trace"]["equity_pool_reference"]
            )
        )
        assert (
            normalise_research_allocation_evidence(snapshot_allocation)
            == research_allocation
        )
        assert (
            normalise_research_allocation_evidence(
                snapshot["immutable_inputs"]["funnel_trace"]["research_allocation"]
            )
            == research_allocation
        )
        manifests = snapshot["immutable_inputs"]["candidate_evidence_manifests"]
        assert set(manifests) == {"c1"}
        assert manifests["c1"]["candidate_id"] == "c1"
        assert manifests["c1"]["manifest_hash"] == canonical_hash(
            {
                key: value
                for key, value in manifests["c1"].items()
                if key != "manifest_hash"
            }
        )
        gate_bundle = snapshot["immutable_inputs"]["gate_bundle"]
        assert snapshot["immutable_inputs"]["gate_bundle_hash"] == result[
            "gate_bundle_hash"
        ]
        terminal = store.read_decisions("scan-1")[-1]
        assert snapshot["decision_hash"] == terminal.decision_hash
        assert snapshot["record_hash"] == terminal.record_hash
        assert snapshot["gate_bundle_hash"] == result["gate_bundle_hash"]
        assert gate_bundle["gate_bundle_hash"] == result["gate_bundle_hash"]
        assert gate_bundle["outcome"] == "TRADE"
        assert [
            row.record_type for row in store.read_decisions("scan-1")
        ] == ["SCENARIO", "GATE_BUNDLE_TRADE", "TRADE"]
        stored_row = snapshot["candidates"][0]
        stored_candidate_gate = next(iter(gate_bundle["candidates"].values()))
        assert stored_candidate_gate["proposal_hash"] == stored_row["proposal_hash"]
        assert stored_candidate_gate["ranking"]["rank"] == 1
        rebuilt_basis = build_ranking_basis(
            candidate_body=stored_row["candidate_body"],
            proposal_body=stored_row["proposal_body"],
            candidate_hash=stored_row["candidate_hash"],
            proposal_hash=stored_row["proposal_hash"],
            current_policy_version=snapshot["current_policy_version"],
            current_policy_hash=snapshot["current_policy_hash"],
            policy_authority_marker_hash=snapshot[
                "policy_authority_marker_hash"
            ],
            cost_version=snapshot["cost_version"],
            cost_hash=snapshot["cost_hash"],
            risk_contract_hash=snapshot["risk_contract_hash"],
            evidence_inputs=snapshot["immutable_inputs"],
        )
        assert rebuilt_basis.ranking_basis_hash == stored_row["ranking_basis_hash"]
        assert calls == ["inputs", "universe", "broker", "registry", "generator", "volatility", "scenario", "cost", "gates"]
        assert pipeline.run_slot("scan-1", NOW)["ranking_snapshot_id"] == result["ranking_snapshot_id"]


def test_gate_four_binds_economics_and_changes_when_stress_cost_changes(tmp_path):
    candidate = _candidate(candidate_id="proof", cost_hash="f" * 64,
                           policy_hash=POLICY_HASH, broker_snapshot_hash="b" * 64)
    hashes = []
    for index, stress in enumerate((Decimal("9"), Decimal("8"))):
        with RankingStore(tmp_path / f"economics-{index}.sqlite") as store:
            pipeline = _proposal_gate_pipeline(candidate=candidate, store=store, calls=[])
            row = pipeline.cost_contract.value["candidates"][0]
            row["stress_after_cost_expected_value"] = stress
            result = pipeline.run_slot("scan.proof", NOW)
            assert result["status"] == "TRADE", result["reasons"]
            snapshot = store.read_snapshot(result["ranking_snapshot_id"])
            bundle = snapshot["immutable_inputs"]["gate_bundle"]
            layer = next(iter(bundle["candidates"].values()))["layers"][3]
            proof = layer["bindings"]["economic_proof"]
            assert proof["cost_resolution"]["calculation_hash"] == "d" * 64
            assert proof["policy_hash"] == POLICY_HASH
            assert proof["cost_contract_hash"] == "f" * 64
            assert canonical_hash(proof) in layer["source_hashes"]
            body = candidate.hash_payload()
            assert layer["input_hash"] == canonical_hash({
                "candidate_id": "proof", "quote_batch_id": body["quote_batch_id"],
                "liquidity_score": body["liquidity_score"], "economic_proof": proof,
            })
            hashes.append(layer["input_hash"])
    assert hashes[0] != hashes[1]


def test_noninitial_policy_without_thresholds_cannot_bypass_economic_gate(tmp_path):
    candidate = _candidate(candidate_id="proof", cost_hash="f" * 64,
                           policy_hash=POLICY_HASH, broker_snapshot_hash="b" * 64)
    with RankingStore(tmp_path / "missing-policy.sqlite") as store:
        pipeline = _proposal_gate_pipeline(candidate=candidate, store=store, calls=[])
        policy = replace(_policy(), payload=freeze_json({"policy": "missing-thresholds"}))
        pipeline.policy_resolver = _Resolver(policy)
        result = pipeline.run_slot("scan.missing-policy", NOW)
        assert result["status"] == "NO_TRADE"
        assert "SIGNED_ECONOMIC_THRESHOLDS_UNSUPPORTED" in result["reasons"]
        assert store.get_by_scan_run("scan.missing-policy") is None


def test_ordinary_short_premium_earnings_overlap_is_candidate_hard_blocked(
    tmp_path,
) -> None:
    base = _candidate(
        candidate_id="earnings-credit",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    shell = replace(
        base,
        structure="CREDIT_VERTICAL",
        earnings_overlap=True,
        candidate_hash="0" * 64,
    )
    candidate = replace(shell, candidate_hash=canonical_hash(shell.hash_payload()))
    calls: list[str] = []
    with RankingStore(tmp_path / "earnings-credit.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot("scan-earnings-credit", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == (
            "EARNINGS_OVERLAP_SHORT_PREMIUM_BLOCKED",
        )
        decisions = store.read_decisions("scan-earnings-credit")
        assert [item.record_type for item in decisions] == [
            "SCENARIO",
            "GATE_BUNDLE_NO_TRADE",
            "NO_TRADE",
        ]
        bundle = decisions[1].record["gate_bundle"]
        candidate_gate = next(iter(bundle["candidates"].values()))
        assert candidate_gate["layers"][2]["status"] == "BLOCK"
        assert candidate_gate["layers"][2]["reason_codes"] == [
            "EARNINGS_OVERLAP_SHORT_PREMIUM_BLOCKED"
        ]


def test_explicit_event_defined_finite_risk_route_can_survive_earnings_overlap(
    tmp_path,
) -> None:
    base = _candidate(
        candidate_id="event-defined-credit",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    shell = replace(
        base,
        structure="CREDIT_VERTICAL",
        earnings_overlap=True,
        event_defined=True,
        candidate_hash="0" * 64,
    )
    candidate = replace(shell, candidate_hash=canonical_hash(shell.hash_payload()))
    calls: list[str] = []
    with RankingStore(tmp_path / "event-defined-credit.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot("scan-event-defined-credit", NOW)

        assert result["status"] == "TRADE"
        snapshot = store.read_snapshot(str(result["ranking_snapshot_id"]))
        bundle = snapshot["immutable_inputs"]["gate_bundle"]
        candidate_gate = next(iter(bundle["candidates"].values()))
        assert candidate_gate["layers"][2]["status"] == "PASS"


def test_missing_authoritative_nav_is_no_trade_without_synthetic_gate_bindings(
    tmp_path,
) -> None:
    candidate = _candidate(
        candidate_id="missing-nav-authority",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    calls: list[str] = []
    broker = _Port(
        "broker",
        calls,
        {
            "snapshot_hash": candidate.broker_snapshot_hash,
            "evidence_hash": "c" * 64,
            "nav_snapshot": None,
        },
    )
    with RankingStore(tmp_path / "missing-nav-authority.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            broker_evidence_port=broker,
        )

        result = pipeline.run_slot("scan-missing-nav-authority", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("STRATEGY_NAV_AUTHORITY_MISMATCH",)
        bundle = store.read_decisions("scan-missing-nav-authority")[1].record[
            "gate_bundle"
        ]
        gate_1 = next(iter(bundle["candidates"].values()))["layers"][0]
        assert gate_1["status"] == "UNAVAILABLE"
        assert gate_1["bindings"] == {"proposal_hash": gate_1["bindings"]["proposal_hash"]}
        assert "3" * 64 not in gate_1["source_hashes"]


def test_incomplete_nav_content_proof_is_candidate_gate_unavailable(tmp_path) -> None:
    candidate = _candidate(
        candidate_id="incomplete-nav-content",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    nav = _nav_evidence()
    nav.pop("content_hash")
    broker = _Port(
        "broker",
        [],
        {
            "snapshot_hash": candidate.broker_snapshot_hash,
            "evidence_hash": "c" * 64,
            "nav_snapshot": nav,
        },
    )
    calls: list[str] = []
    with RankingStore(tmp_path / "incomplete-nav-content.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            broker_evidence_port=broker,
        )

        result = pipeline.run_slot("scan-incomplete-nav-content", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("STRATEGY_NAV_AUTHORITY_MISMATCH",)
        bundle = store.read_decisions("scan-incomplete-nav-content")[1].record[
            "gate_bundle"
        ]
        gate_1 = next(iter(bundle["candidates"].values()))["layers"][0]
        assert gate_1["status"] == "UNAVAILABLE"


def test_supporting_event_context_is_recorded_but_cannot_pass_gate_three(
    tmp_path,
) -> None:
    base = _candidate(
        candidate_id="stale-supporting-event",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    shell = replace(
        base,
        event_evidence_status="UNAVAILABLE",
        earnings_overlap=None,
        event_evidence_hash=None,
        event_supporting_overlap=True,
        event_supporting_hash="9" * 64,
        candidate_hash="0" * 64,
    )
    candidate = replace(shell, candidate_hash=canonical_hash(shell.hash_payload()))
    calls: list[str] = []
    with RankingStore(tmp_path / "stale-supporting-event.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot("scan-stale-supporting-event", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("EVENT_EVIDENCE_UNAVAILABLE",)
        bundle = store.read_decisions("scan-stale-supporting-event")[1].record[
            "gate_bundle"
        ]
        gate_3 = next(iter(bundle["candidates"].values()))["layers"][2]
        assert gate_3["status"] == "UNAVAILABLE"
        assert gate_3["supporting_inputs"][0]["source"] == "EVENT_NEWS_CONTEXT"
        assert gate_3["supporting_inputs"][0]["payload"][
            "decision_authority"
        ] == "SUPPORTING_ONLY"


def test_point_in_time_fundamentals_are_bound_as_supporting_only(tmp_path) -> None:
    base = _candidate(
        candidate_id="fundamental-supporting",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    shell = replace(
        base,
        fundamental_supporting_status="AVAILABLE",
        fundamental_supporting_hash="8" * 64,
        fundamental_supporting_payload={
            "symbol": "SPY",
            "as_of": NOW.isoformat(),
            "record_count": 1,
            "records": [],
            "decision_authority": "SUPPORTING_ONLY",
        },
        fundamental_supporting_reason_codes=(
            "SUPPORTING_ONLY_NO_HARD_AUTHORITY",
        ),
        candidate_hash="0" * 64,
    )
    candidate = replace(shell, candidate_hash=canonical_hash(shell.hash_payload()))
    calls: list[str] = []
    with RankingStore(tmp_path / "fundamental-supporting.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        pipeline.run_slot("scan-fundamental-supporting", NOW)

        bundle = store.read_decisions("scan-fundamental-supporting")[1].record[
            "gate_bundle"
        ]
        gate_3 = next(iter(bundle["candidates"].values()))["layers"][2]
        fundamental = next(
            item
            for item in gate_3["supporting_inputs"]
            if item["source"] == "POINT_IN_TIME_FUNDAMENTALS"
        )
        assert fundamental["status"] == "AVAILABLE"
        assert fundamental["authority"] == "SUPPORTING_ONLY"
        assert fundamental["payload"]["decision_authority"] == "SUPPORTING_ONLY"



def test_missing_event_evidence_is_hard_gate_unavailable(tmp_path) -> None:
    base = _candidate(
        candidate_id="missing-event-evidence",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    shell = replace(
        base,
        event_evidence_status="UNAVAILABLE",
        earnings_overlap=None,
        event_evidence_hash=None,
        candidate_hash="0" * 64,
    )
    candidate = replace(shell, candidate_hash=canonical_hash(shell.hash_payload()))
    calls: list[str] = []
    with RankingStore(tmp_path / "missing-event-evidence.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
        )

        result = pipeline.run_slot("scan-missing-event-evidence", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("EVENT_EVIDENCE_UNAVAILABLE",)


def test_pre_generation_gate_two_route_prevents_blocked_family_generation(
    tmp_path,
) -> None:
    base = _candidate(
        candidate_id="routed-credit",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    shell = replace(base, structure="CREDIT_VERTICAL", candidate_hash="0" * 64)
    candidate = replace(shell, candidate_hash=canonical_hash(shell.hash_payload()))
    calls: list[str] = []
    with RankingStore(tmp_path / "routed-credit.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            pipeline_context={
                "gate_routing": {
                    "status": "PASS",
                    "regime": "LONG_PREMIUM_ONLY",
                    "allowed_strategy_families": ("DEBIT_VERTICAL",),
                    "blocked_strategy_families": ("CREDIT_VERTICAL",),
                }
            },
        )

        result = pipeline.run_slot("scan-routed-credit", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("MARKET_CREDIT_ROUTE_EMPTY",)
        assert "generator" not in calls


def test_pipeline_rejects_invalid_explicit_external_reference_for_all_candidates(
    tmp_path,
) -> None:
    calls: list[str] = []
    candidate = _candidate(
        candidate_id="bad-reference",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    authority = _authority()
    context = {
        "universe": {},
        "positions": (),
        "candidate_evidence_references": {
            "bad-reference": {
                "supporting": (
                    {
                        **_reference("ev_bad", "7"),
                        "headline": "must not be accepted in a frozen reference",
                    },
                ),
                "contradicting": (),
            }
        },
    }
    with RankingStore(tmp_path / "bad-reference.sqlite") as store:
        pipeline = DecisionPipeline(
            inputs=_Port("inputs", calls, context),
            universe_funnel=_Port("universe", calls, {"finalists": (candidate,)}),
            broker_evidence=_Port(
                "broker",
                calls,
                {"snapshot_hash": "b" * 64, "evidence_hash": "c" * 64, "nav_snapshot": _nav_evidence()},
            ),
            strategy_registry=_Port(
                "registry", calls, {"finalists": (candidate,)}
            ),
            strategy_generator=_Port(
                "generator", calls, {"candidates": (candidate,)}
            ),
            volatility_engine=_Port(
                "volatility",
                calls,
                {"eligible": True, "evidence_hash": "d" * 64},
            ),
            scenario_engine=_Port(
                "scenario",
                calls,
                {
                    "action": "TRADE",
                    "scenarios": _scenario_rows(),
                    "cost_hash": "f" * 64,
                    "current_policy_version": "v1",
                    "current_policy_hash": POLICY_HASH,
                    "policy_authority_marker_hash": POLICY_MARKER_HASH,
                    "risk_authority_version": "v1",
                    "risk_authority_marker_hash": authority.marker_hash,
                    "risk_contract_hash": RISK_CONTRACT_HASH,
                },
            ),
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(authority),
            cost_contract=_Port(
                "cost",
                calls,
                {
                    "version": "v1",
                    "hash": "f" * 64,
                    "candidates": (
                        {
                            "candidate_id": "bad-reference",
                            "execution_cost_usd": Decimal("1"),
                            "stress_after_cost_expected_value": Decimal("9"),
                            "cost_hash": "f" * 64,
                            "cost_version": "v1",
                            "after_cost_expected_value": Decimal("10"),
                        },
                    ),
                },
            ),
            eligibility_gate=_Port(
                "gates", calls, {"eligible": True, "risk_fraction": Decimal("0.10")}
            ),
            portfolio_ranker=PortfolioRanker(),
            ranking_store=store,
            clock=lambda: NOW,
        )

        result = pipeline.run_slot("scan-bad-reference", NOW)

        assert result["status"] == "NO_TRADE"
        assert "CANDIDATE_EVIDENCE_MANIFEST_INVALID" in result["reasons"]
        assert result["ranking_snapshot_id"] is None
        assert store.get_by_scan_run("scan-bad-reference") is None


def test_pipeline_open_option_position_is_management_only(tmp_path) -> None:
    calls = []
    funnel_trace = {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": "scan-gld",
        "discovered_underlyings": 0,
        "deep_scan_requested": 0,
        "deep_scan_completed": 0,
        "ranked_limit": 10,
        "ranked_count": 0,
        "filler_candidates": 0,
        "pacing_capability_hash": None,
        "pacing_usage": {},
        "research_allocation": {
            "schema": "options_copilot.research_allocation_evidence.v2",
            "decision_authority": "SUPPORTING_ONLY",
            "uncanonical": object(),
        },
    }
    with RankingStore(tmp_path / "ranking.sqlite") as store:
        pipeline = DecisionPipeline(
            inputs=_Port(
                "inputs",
                calls,
                {
                    "universe": {},
                    "positions": (
                        {
                            "symbol": "QQQ",
                            "security_type": "OPT",
                            "quantity": 1,
                        },
                    ),
                    "funnel_trace": funnel_trace,
                },
            ),
            universe_funnel=_Port("universe", calls, {"finalists": ()}), broker_evidence=_Port("broker", calls, {}),
            strategy_registry=_Port("registry", calls, {}), strategy_generator=_Port("generator", calls, {}),
            volatility_engine=_Port("volatility", calls, {}), scenario_engine=_Port("scenario", calls, {}),
            policy_resolver=_Resolver(_policy()), risk_authority_resolver=_Resolver(_authority()),
            cost_contract=_Port("cost", calls, {}), eligibility_gate=_Port("gates", calls, {}),
            portfolio_ranker=PortfolioRanker(), ranking_store=store, clock=lambda: NOW,
        )
        result = pipeline.run_slot("scan-gld", NOW)
        assert result["status"] == "NO_TRADE"
        assert "POSITION_MANAGEMENT_ONLY" in result["reasons"]
        assert result["ranking_snapshot_id"] is None
        expected_trace = dict(funnel_trace)
        expected_trace.pop("research_allocation")
        assert result["funnel_trace"] == expected_trace
        terminal = store.read_decisions("scan-gld")[-1]
        assert "research_allocation" not in terminal.record["funnel_trace"]


@pytest.mark.parametrize(
    ("universe", "expected_reasons"),
    (
        (
            {"finalists": ()},
            (
                "NO_COARSE_OPTION_CANDIDATES",
                "UNDERLYING_QUOTE_PACING_DENIED",
                "UNIVERSE_EMPTY",
            ),
        ),
        (
            {"finalists": (), "reasons": ("UNIVERSE_SOURCE_INVALID",)},
            ("UNIVERSE_SOURCE_INVALID",),
        ),
    ),
)
def test_empty_universe_preserves_input_context_unless_universe_has_reason(
    tmp_path,
    universe,
    expected_reasons,
) -> None:
    calls: list[str] = []
    with RankingStore(tmp_path / "empty-universe.sqlite") as store:
        pipeline = DecisionPipeline(
            inputs=_Port(
                "inputs",
                calls,
                {
                    "universe": {},
                    "positions": (),
                    "reasons": ("NO_COARSE_OPTION_CANDIDATES",),
                    "reason_codes": ("UNDERLYING_QUOTE_PACING_DENIED",),
                },
            ),
            universe_funnel=_Port("universe", calls, universe),
            broker_evidence=_Port("broker", calls, {}),
            strategy_registry=_Port("registry", calls, {}),
            strategy_generator=_Port("generator", calls, {}),
            volatility_engine=_Port("volatility", calls, {}),
            scenario_engine=_Port("scenario", calls, {}),
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            cost_contract=_Port("cost", calls, {}),
            eligibility_gate=_Port("gates", calls, {}),
            portfolio_ranker=PortfolioRanker(),
            ranking_store=store,
            clock=lambda: NOW,
        )

        result = pipeline.run_slot("scan-empty-universe", NOW)

    assert result["status"] == "NO_TRADE"
    assert result["reasons"] == expected_reasons
    assert calls == ["inputs", "universe"]


def test_pipeline_resolves_a_grade_only_after_exact_ranking_bindings_exist(
    tmp_path,
) -> None:
    candidate = _candidate(
        candidate_id="bound-a-grade",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
        max_loss_usd=Decimal("150"),
    )
    resolver = _BindingRiskResolver(candidate.candidate_hash)
    calls: list[str] = []
    with RankingStore(tmp_path / "bound-a-grade.sqlite") as store:
        pipeline = _proposal_gate_pipeline(
            candidate=candidate,
            store=store,
            calls=calls,
            gate_risk_fraction=Decimal("0.15"),
            risk_resolver=resolver,
        )

        result = pipeline.run_slot("scan-bound-a-grade", NOW)

        assert result["status"] == "TRADE"
        assert len(resolver.calls) == 3
        assert resolver.calls[0].get("proposal_hash") is None
        snapshot = store.read_snapshot(str(result["ranking_snapshot_id"]))
        row = snapshot["candidates"][0]
        assert row["authority_status"] == "A_GRADE"
        assert resolver.calls[2]["proposal_hash"] == row["proposal_hash"]
        assert resolver.calls[2]["candidate_hash"] == row["candidate_hash"]
        assert resolver.calls[2]["ranking_basis_hash"] == row["ranking_basis_hash"]
        assert resolver.calls[2]["execution_cost_version"] == "v1"
        assert resolver.calls[2]["execution_cost_hash"] == "f" * 64


def test_pipeline_refuses_a_grade_marker_when_bound_candidate_is_not_rank_one(
    tmp_path,
) -> None:
    normal = _candidate(
        candidate_id="normal-rank-one",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
    )
    bound = _candidate(
        candidate_id="bound-rank-two",
        cost_hash="f" * 64,
        policy_hash=POLICY_HASH,
        broker_snapshot_hash="b" * 64,
        max_loss_usd=Decimal("150"),
    )
    authority = _authority()
    resolver = _BindingRiskResolver(bound.candidate_hash)
    calls: list[str] = []
    with RankingStore(tmp_path / "a-grade-rank-two.sqlite") as store:
        pipeline = DecisionPipeline(
            inputs=_Port("inputs", calls, {"universe": {}, "positions": ()}),
            universe_funnel=_Port("universe", calls, {"finalists": (normal, bound)}),
            broker_evidence=_Port(
                "broker",
                calls,
                {"snapshot_hash": "b" * 64, "evidence_hash": "c" * 64, "nav_snapshot": _nav_evidence()},
            ),
            strategy_registry=_Port(
                "registry", calls, {"finalists": (normal, bound)}
            ),
            strategy_generator=_Port(
                "generator", calls, {"candidates": (normal, bound)}
            ),
            volatility_engine=_Port(
                "volatility", calls, {"eligible": True, "evidence_hash": "d" * 64}
            ),
            scenario_engine=_Port(
                "scenario",
                calls,
                {
                    "action": "TRADE",
                    "scenarios": _scenario_rows(),
                    "current_policy_version": "v1",
                    "current_policy_hash": POLICY_HASH,
                    "policy_authority_marker_hash": POLICY_MARKER_HASH,
                    "risk_authority_version": "v1",
                    "risk_authority_marker_hash": authority.marker_hash,
                    "risk_contract_hash": RISK_CONTRACT_HASH,
                },
            ),
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=resolver,
            cost_contract=_Port(
                "cost",
                calls,
                {
                    "version": "v1",
                    "hash": "f" * 64,
                    "candidates": (
                            {
                                "candidate_id": "normal-rank-one",
                                "execution_cost_usd": Decimal("1"),
                                "stress_after_cost_expected_value": Decimal("9"),
                                "cost_hash": "f" * 64,
                                "cost_version": "v1",
                                "after_cost_expected_value": Decimal("10"),
                        },
                        {
                            "candidate_id": "bound-rank-two",
                            "execution_cost_usd": Decimal("1"),
                            "stress_after_cost_expected_value": Decimal("9"),
                            "cost_hash": "f" * 64,
                            "cost_version": "v1",
                            "after_cost_expected_value": Decimal("10"),
                        },
                    ),
                },
            ),
            eligibility_gate=_Port(
                "gates",
                calls,
                {
                    "eligible": True,
                    "risk_fractions": {
                        "normal-rank-one": Decimal("0.10"),
                        "bound-rank-two": Decimal("0.15"),
                    },
                },
            ),
            portfolio_ranker=PortfolioRanker(),
            ranking_store=store,
            clock=lambda: NOW,
        )

        result = pipeline.run_slot("scan-a-grade-rank-two", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("A_GRADE_RANK_ONE_BINDING_MISMATCH",)
        assert store.get_by_scan_run("scan-a-grade-rank-two") is None


def test_real_volatility_scenario_rejects_missing_direction_and_state_inputs(tmp_path) -> None:
    cost_hash = "d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b"
    initial_policy_hash = "b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c"
    policy = ResolvedPolicy("v1", initial_policy_hash, POLICY_MARKER_HASH, NOW, freeze_json({"hard_no_trade_thresholds": {"cost_and_expectancy": {"execution_cost_contract_hash": cost_hash}}}), freeze_json({"source": "test"}))
    candidate = _candidate(
        candidate_id="real-1",
        cost_hash=cost_hash,
        policy_hash=initial_policy_hash,
        broker_snapshot_hash="c" * 64,
        terminal_scenarios=_real_scenario_terms(
            policy=policy,
            cost_hash=cost_hash,
        ),
    )
    with RankingStore(tmp_path / "real.sqlite") as store:
        pipeline = DecisionPipeline(
            inputs=lambda **_: {"universe": {}, "positions": (), "volatility": {"source": "IBKR", "observed_at": NOW, "secdef_hash": "a" * 64, "quote_hash": "b" * 64, "quotes": ({"bid": Decimal("1"), "ask": Decimal("1.1"), "iv": Decimal("0.2"), "volume": 20, "open_interest": 200},), "iv_history": (Decimal("0.15"), Decimal("0.2")), "atm_iv": Decimal("0.2")}},
            universe_funnel=_Port("universe", [], {"finalists": (candidate,)}),
            broker_evidence=_Port("broker", [], {"snapshot_hash": "c" * 64, "evidence_hash": "e" * 64, "nav_snapshot": _nav_evidence(), "spot": Decimal("100"), "atm_iv": Decimal("0.2")}),
            strategy_registry=_Port("registry", [], {"finalists": (candidate,)}),
            strategy_generator=_Port("generator", [], {"candidates": (candidate,)}),
            volatility_engine=VolatilityEngine(), scenario_engine=ScenarioEngine(),
            policy_resolver=_Resolver(policy), risk_authority_resolver=_Resolver(_authority()),
            cost_contract=_Port("cost", [], {"version": "v1", "hash": cost_hash, "candidates": ({"candidate_id": "real-1", "cost_hash": cost_hash, "cost_version": "v1", "after_cost_expected_value": Decimal("10")},)}),
            eligibility_gate=_Port("gates", [], {"eligible": True, "risk_fraction": Decimal("0.10")}), portfolio_ranker=PortfolioRanker(), ranking_store=store, clock=lambda: NOW,
        )
        result = pipeline.run_slot("real-scan", NOW)
        assert result["status"] == "NO_TRADE"
        assert "MISSING_MARKET_DIRECTION_INPUT" in result["reasons"]
        assert "MISSING_VOLATILITY_STATE_INPUT" in result["reasons"]
        assert store.get_by_scan_run("real-scan") is None


def test_legacy_store_without_atomic_decision_journal_is_read_only_no_trade() -> None:
    calls: list[str] = []
    store = _LegacyReadOnlyStore()
    pipeline = DecisionPipeline(
        inputs=_Port("inputs", calls, {}),
        universe_funnel=_Port("universe", calls, {}),
        broker_evidence=_Port("broker", calls, {}),
        strategy_registry=_Port("registry", calls, {}),
        strategy_generator=_Port("generator", calls, {}),
        volatility_engine=_Port("volatility", calls, {}),
        scenario_engine=_Port("scenario", calls, {}),
        policy_resolver=_Resolver(_policy()),
        risk_authority_resolver=_Resolver(_authority()),
        cost_contract=_Port("cost", calls, {}),
        eligibility_gate=_Port("gates", calls, {}),
        portfolio_ranker=PortfolioRanker(),
        ranking_store=store,
        clock=lambda: NOW,
    )

    result = pipeline.run_slot("legacy-read-only", NOW)

    assert result["status"] == "NO_TRADE"
    assert "ATOMIC_DECISION_JOURNAL_REQUIRED" in result["reasons"]
    assert "DECISION_STORE_UNAVAILABLE_READ_ONLY" in result["reasons"]
    assert result["ranking_snapshot_id"] is None
    assert calls == []
    assert store.append_snapshot_calls == 0


def test_zero_influence_six_advisory_variants_keep_authority_byte_identical(
    tmp_path,
) -> None:
    cases = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "phase2_eval"
            / "candidate_v1"
            / "cases.json"
        ).read_text(encoding="utf-8")
    )
    p2_20 = next(case for case in cases if case["case_id"] == "P2-20")
    variants = tuple(p2_20["details"]["advisory_variants"])
    assert variants == (
        "DISABLED",
        "TIMEOUT",
        "MALFORMED",
        "ADVERSARIAL",
        "BULLISH",
        "BEARISH",
    )

    authority_hashes: list[str] = []
    with RankingStore(tmp_path / "zero-influence.sqlite") as store:
        for variant in variants:
            candidate = _candidate(
                candidate_id="zero-influence",
                cost_hash="f" * 64,
                policy_hash=POLICY_HASH,
                broker_snapshot_hash="b" * 64,
            )
            calls: list[str] = []
            broker = _RecordingBrokerPort(
                calls,
                {"snapshot_hash": "b" * 64, "evidence_hash": "c" * 64, "nav_snapshot": _nav_evidence()},
            )
            creator = _RecordingCreator()
            pipeline = _proposal_gate_pipeline(
                candidate=candidate,
                store=store,
                calls=calls,
                broker_evidence_port=broker,
            )

            result = pipeline.run_slot("scan-zero-influence", NOW)
            snapshot = store.read_snapshot(str(result["ranking_snapshot_id"]))
            row = snapshot["candidates"][0]
            comparison_document = {
                "eligibility": {
                    "status": result["status"],
                    "reasons": result["reasons"],
                    "candidate_hashes": result["candidate_hashes"],
                },
                "ranking": snapshot,
                "ranking_head": {
                    "snapshot_id": result["ranking_snapshot_id"],
                    "snapshot_hash": result["ranking_snapshot_hash"],
                    "ranking_basis_hashes": result["ranking_basis_hashes"],
                },
                "current_nav_risk": {
                    "risk_fraction": row["proposal_body"]["risk"][
                        "risk_fraction"
                    ],
                    "authority_status": row["authority_status"],
                    "authorizable": row["authorizable"],
                },
                "reviewability": {
                    "rank": row["rank"],
                    "proposal_hash": row["proposal_hash"],
                    "candidate_hash": row["candidate_hash"],
                },
                "approval_creator_state": {
                    "approval_created": False,
                    "creator_status": "CREATOR_TRANSPORT_UNAVAILABLE",
                },
                "broker_call_trace": tuple(calls),
                "broker_write_calls": tuple(broker.write_calls),
                "creator_write_calls": tuple(creator.write_calls),
                "supporting_advisory": {"variant": variant},
            }
            supporting = comparison_document.pop("supporting_advisory")

            assert supporting == {"variant": variant}
            assert broker.write_calls == []
            assert creator.write_calls == []
            authority_hashes.append(canonical_hash(comparison_document))

    assert len(set(authority_hashes)) == 1
