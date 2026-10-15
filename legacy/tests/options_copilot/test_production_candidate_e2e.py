from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from options_copilot.analytics import (
    InitialPolicyResolver,
    ResolvedPolicy,
    ScenarioEngine,
    VolatilityEngine,
)
from options_copilot.api import (
    OptionsCopilotServices,
    RankOneChallengeRequest,
    create_app,
)
from options_copilot.approval import ProposalApprovalStore
from options_copilot.decision import DecisionPipeline
from options_copilot.decision.pipeline import _joint_account_context
from options_copilot.domain import TerminalScenario
from options_copilot.execution_cost import SignedExecutionCostResolver
from options_copilot.gateway import (
    BatchedOptionQuote,
    BrokerSnapshotBuilder,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)
from options_copilot.gateway.broker_snapshot import StateComponentEvidence
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.governance.contracts import load_contract
from options_copilot.option_pool.models import normalize_equity_thesis_row
from options_copilot.ranking import PortfolioRanker, RankingStore, build_ranking_basis
from options_copilot.risk.authorization import RiskTierAuthority
from options_copilot.runtime import RuntimeServices
from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore
from options_copilot.storage.canonical import canonical_hash, freeze_json
from options_copilot.strategies import StrategyCandidateGenerator, StrategyTemplateRegistry


POLICY_HASH = "a" * 64
POLICY_MARKER_HASH = "b" * 64
COST_HASH = "c" * 64
RISK_CONTRACT_HASH = "d" * 64
NAV_CONTRACT_HASH = "e" * 64
NAV_LEDGER_HASH = "f" * 64
MARKET_EVIDENCE_HASH = "1" * 64
LIQUIDITY_EVIDENCE_HASH = "2" * 64


class _Resolver:
    def __init__(self, value: object) -> None:
        self.value = value

    def resolve(self, **_: object) -> object:
        return self.value

    def is_current(self, value: object) -> bool:
        return value == self.value

    def guard_current(self, value: object, *, callback):
        if not self.is_current(value):
            return None
        return callback()


class _CostResolver(_Resolver):
    def __init__(self, candidate_ids: Sequence[str]) -> None:
        super().__init__(
            {
                "version": "cost-v1",
                "hash": COST_HASH,
                "cost_version": "cost-v1",
                "cost_hash": COST_HASH,
                "candidates": tuple(
                    {
                        "candidate_id": candidate_id,
                        "cost_version": "cost-v1",
                        "cost_hash": COST_HASH,
                        "after_cost_expected_value": Decimal("120"),
                        "execution_cost_usd": Decimal("5"),
                        "stress_after_cost_expected_value": Decimal("100"),
                        "calculation_hash": "b" * 64,
                    }
                    for candidate_id in candidate_ids
                ),
            }
        )


class _Inputs:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self.payload = dict(payload)

    def run(self, **_: object) -> Mapping[str, object]:
        return dict(self.payload)


class _Universe:
    def __init__(self, finalists: Sequence[Mapping[str, object]]) -> None:
        self.finalists = tuple(finalists)

    def run(self, **_: object) -> Mapping[str, object]:
        return {"finalists": self.finalists}


class _BrokerEvidence:
    def __init__(self, payload: Mapping[str, object]) -> None:
        self.payload = dict(payload)

    def acquire(self, **_: object) -> Mapping[str, object]:
        return dict(self.payload)


class _ExactScenarioPort:
    def __init__(self, policy: ResolvedPolicy, authority: RiskTierAuthority) -> None:
        self.policy = policy
        self.authority = authority

    def evaluate_pre_cost(self, **_: object) -> Mapping[str, object]:
        scenarios = (
            {"terminal_price": "100", "probability": "0.5"},
            {"terminal_price": "105", "probability": "0.5"},
        )
        return {
            "action": "TRADE",
            "scenarios": scenarios,
            "current_policy_version": self.policy.current_policy_version,
            "current_policy_hash": self.policy.current_policy_hash,
            "policy_authority_marker_hash": (
                self.policy.policy_authority_marker_hash
            ),
            "risk_authority_version": self.authority.version,
            "risk_authority_marker_hash": self.authority.marker_hash,
            "risk_contract_hash": self.authority.risk_contract_hash,
            "result_hash": canonical_hash(scenarios),
        }


class _EligibilityGate:
    def evaluate(self, **_: object) -> Mapping[str, object]:
        return {
            "eligible": True,
            "risk_fraction": Decimal("0.065"),
            "open_combinations": 0,
        }


class _PassiveGate:
    def evaluate(self, **_: object) -> Mapping[str, object]:
        return {"eligible": True}


class _ScanRunStore:
    def get(self, scan_run_id: str) -> Mapping[str, object]:
        return {"scan_run_id": scan_run_id, "status": "COMPLETE"}

    def runs_for_slot(self, *_: object, **__: object) -> tuple[object, ...]:
        return ()


class _NavSource:
    def __init__(self, snapshot: StrategyNavSnapshot) -> None:
        self.value = snapshot

    def snapshot(self, **_: object) -> StrategyNavSnapshot:
        return self.value

    def guard_current(self, value: object, *, callback):
        if value != self.value:
            return None
        return callback()


class _PositionManager:
    def read_model(self) -> Mapping[str, object]:
        return {"available": True, "approval_enabled": False}


class _BridgeReadPort:
    def get(self, *_: object, **__: object) -> object | None:
        return None

    def list_pending(self, *_: object, **__: object) -> tuple[object, ...]:
        return ()

    def reconciliation_status(self) -> Mapping[str, object]:
        return {"status": "HANDOFF_ONLY"}


class _OptionsEvidence:
    def __init__(self, contracts: Sequence[OptionContractRef]) -> None:
        self.contracts = tuple(contracts)

    def acquire(self, **_: object) -> Mapping[str, object]:
        return {"status": "READY"}

    def resolve_contracts(self, **_: object) -> tuple[OptionContractRef, ...]:
        return self.contracts


class _BrokerSource:
    def __init__(
        self,
        *,
        now: datetime,
        secdefs: Sequence[OptionSecDefSnapshot],
    ) -> None:
        self.now = now
        self.secdefs = tuple(secdefs)

    def account_snapshot(self) -> Mapping[str, object]:
        return {"account_id": "DU-REDACTED", "net_liquidation": "999999"}

    def positions(self) -> tuple[object, ...]:
        return ()

    def working_orders(self) -> tuple[object, ...]:
        return ()

    def unsubmitted_instructions(self) -> tuple[object, ...]:
        return ()

    def option_contract_definitions(
        self, contracts: Sequence[OptionContractRef]
    ) -> tuple[OptionSecDefSnapshot, ...]:
        by_id = {item.contract_id: item for item in self.secdefs}
        return tuple(by_id[item.contract_id] for item in contracts)

    def option_quote_batch(
        self, contracts: Sequence[OptionContractRef]
    ) -> OptionQuoteBatch:
        requested_at = self.now - timedelta(seconds=2)
        observed_at = self.now - timedelta(seconds=1)
        prices = {
            101: (Decimal("2.00"), Decimal("2.10")),
            102: (Decimal("1.00"), Decimal("1.10")),
        }
        quotes = tuple(
            BatchedOptionQuote(
                contract_id=contract.contract_id,
                batch_id="batch-production-e2e",
                request_id=f"request-{contract.contract_id}",
                requested_at=requested_at,
                observed_at=observed_at,
                completed_at=self.now,
                source="IBKR",
                bid=prices[contract.contract_id][0],
                ask=prices[contract.contract_id][1],
                exchange_time=observed_at,
                volume=500,
                open_interest=5000,
                implied_volatility=Decimal("0.20"),
                delta=(
                    Decimal("0.55")
                    if contract.contract_id == 101
                    else Decimal("0.35")
                ),
                gamma=Decimal("0.02"),
                theta=Decimal("-0.04"),
                vega=Decimal("0.10"),
                market_data_type=1,
            )
            for contract in contracts
        )
        return OptionQuoteBatch(
            "batch-production-e2e",
            QuoteBatchStatus.COMPLETE,
            requested_at,
            self.now,
            "IBKR",
            quotes,
        )


@dataclass(slots=True)
class _Harness:
    now: datetime
    scan_run_id: str
    pipeline_result: Mapping[str, object]
    ranking_store: RankingStore
    evidence_store: EvidenceStore
    approval_store: ProposalApprovalStore
    runtime: RuntimeServices
    app: object
    cost_resolver: object

    def close(self) -> None:
        self.approval_store.close()
        self.evidence_store.close()
        self.ranking_store.close()


def _nav(now: datetime) -> StrategyNavSnapshot:
    fields = {
        "asof": now,
        "strategy_nav": Decimal("2000"),
        "strategy_deposits": Decimal("2000"),
        "strategy_withdrawals": Decimal("0"),
        "realized_pnl": Decimal("0"),
        "open_position_unrealized_pnl": Decimal("0"),
        "fees": Decimal("0"),
        "signed_corrections": Decimal("0"),
        "non_strategy_contribution": Decimal("0"),
        "fill_principal_contribution": Decimal("0"),
        "observed_account_nlv": Decimal("999999"),
        "reconciliation_difference": Decimal("997999"),
        "contract_version": "strategy-nav-v1",
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
        "valid": True,
        "no_trade_reasons": (),
    }
    return StrategyNavSnapshot(**fields, content_hash=canonical_hash(fields))


def _signed_contracts(now: datetime) -> tuple[dict[str, object], dict[str, object]]:
    signed = {
        "schema": "options_copilot.governance.signed_contract.v1",
        "actor": "human:test",
        "signed_at": (now - timedelta(days=1)).isoformat(),
        "effective_at": (now - timedelta(days=1)).isoformat(),
    }
    cost = {
        **signed,
        "version": "cost-v1",
        "contract_hash": COST_HASH,
        "payload": {
            "commission_and_fees": {},
            "quote_spread_and_slippage": {},
            "assignment_exercise_and_dividend": {
                "status": "SUPPORTED",
                "evidence_hash": "8" * 64,
            },
        },
    }
    policy_payload = {
        "hard_no_trade_thresholds": {
            "cost_and_expectancy": {
                "minimum_after_cost_expected_value_usd": "max(5.00,0.05*maximum_loss)",
                "minimum_max_profit_to_maximum_loss": "1.20",
                "maximum_total_round_trip_cost_to_max_profit": "0.20",
                "stress_after_cost_ev": "must be greater than or equal to 0.00",
                "execution_cost_contract_version": "cost-v1",
                "execution_cost_contract_hash": COST_HASH,
            }
        }
    }
    policy = {
        **signed,
        "version": "policy-v1",
        "contract_hash": POLICY_HASH,
        "payload": policy_payload,
    }
    return cost, policy


def _finalist(
    candidate_id: str,
    expiry,
    *,
    now: datetime,
    include_terminal_scenarios: bool = True,
) -> dict[str, object]:
    underlying_quote_basis = {
        "schema": "options_copilot.underlying_quote_basis.v1",
        "symbol": "SPY",
        "contract_id": 756733,
        "exchange": "NASDAQ",
        "source": "IBKR_REQ_TICKERS_READONLY",
        "observed_at": (now - timedelta(seconds=1)).isoformat(),
        "bid": "102.40",
        "ask": "102.60",
        "last": "102.50",
        "close": "101.75",
        "market_data_type": 1,
    }
    finalist = {
        "candidate_id": candidate_id,
        "structure": "DEBIT_VERTICAL",
        "legs": (
            {"con_id": 101, "side": "LONG", "ratio": 1},
            {"con_id": 102, "side": "SHORT", "ratio": 1},
        ),
        "exit_plan": {
            "thesis_invalidation": "trend reverses",
            "risk_stop": "close at 50 percent loss",
            "profit_take": "close at 50 percent gain",
            "time_stop": "close before expiry",
            "maximum_holding_date": (expiry - timedelta(days=1)).isoformat(),
            "bad_quote_action": "do not trade",
        },
        "thesis": f"deterministic thesis {candidate_id}",
        "event_evidence_status": "AVAILABLE",
        "earnings_overlap": False,
        "event_defined": False,
        "event_evidence_hash": "9" * 64,
        "underlying_quote_basis": underlying_quote_basis,
        "underlying_quote_basis_hash": canonical_hash(
            underlying_quote_basis
        ),
    }
    if include_terminal_scenarios:
        finalist["terminal_scenarios"] = (
            TerminalScenario(Decimal("100"), Decimal("0.5")),
            TerminalScenario(Decimal("105"), Decimal("0.5")),
        )
    return finalist


def _evidence_ref(value: object) -> dict[str, object]:
    return {
        "evidence_id": getattr(value, "evidence_id"),
        "content_hash": getattr(value, "content_hash"),
        "row_hash": getattr(value, "row_hash"),
    }


def _route(app: object, path: str, method: str = "GET"):
    matches = [
        route
        for route in getattr(app, "routes")
        if getattr(route, "path", None) == path
        and method in getattr(route, "methods", set())
    ]
    assert len(matches) == 1
    return matches[0].endpoint


def _api_services(runtime: RuntimeServices) -> OptionsCopilotServices:
    return OptionsCopilotServices(
        health_provider=lambda: {"dependencies": {}},
        bootstrap_provider=lambda: {"account": {}, "safety": {}},
        candidates_provider=lambda: {"candidates": ()},
        positions_provider=lambda: {"positions": ()},
        learning_provider=lambda: {"stage": "COLLECTING"},
        latest_scan_provider=runtime.latest_scan,
        latest_ranking_provider=runtime.latest_ranking,
        ranking_provider=runtime.ranking,
        candidate_evidence_provider=runtime.candidate_evidence,
        management_provider=runtime.management,
        rank_one_challenge_handler=runtime.create_rank_one_challenge,
        challenge_confirmation_handler=runtime.confirm_challenge,
    )


def _build_harness(
    tmp_path: Path,
    *,
    real_scenario_and_cost: bool = False,
    observed_scenario_scores: tuple[Decimal, Decimal] | None = None,
    include_contradicting_evidence: bool = True,
    approval_blockers: tuple[str, ...] = (),
) -> _Harness:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    expiry = now.date() + timedelta(days=17)
    scan_run_id = "scan-production-e2e"
    candidate_ids = ("candidate-a", "candidate-b")
    secdefs = (
        OptionSecDefSnapshot(
            101,
            "SPY CALL 100",
            "SPY",
            100,
            "SMART",
            expiry,
            Decimal("100"),
            "C",
            "OPT",
            "USD",
            True,
            False,
            "IBKR",
        ),
        OptionSecDefSnapshot(
            102,
            "SPY CALL 105",
            "SPY",
            100,
            "SMART",
            expiry,
            Decimal("105"),
            "C",
            "OPT",
            "USD",
            True,
            False,
            "IBKR",
        ),
    )
    contracts = tuple(
        OptionContractRef(
            contract_id=item.contract_id,
            contract_id_ex=f"{item.contract_id}@{item.exchange}",
            symbol="SPY",
            local_symbol=item.local_symbol,
            expiration=item.expiration,
            strike=item.strike,
            right=item.right,
            exchange=item.exchange,
            trading_class=item.trading_class,
            multiplier=item.multiplier,
            currency=item.currency,
        )
        for item in secdefs
    )
    broker_source = _BrokerSource(now=now, secdefs=secdefs)
    quote_batch = broker_source.option_quote_batch(contracts)
    broker_builder = BrokerSnapshotBuilder(broker_source, clock=lambda: now)
    broker_snapshot = broker_builder.build(contracts)
    assert broker_snapshot.complete

    nav = _nav(now)
    if real_scenario_and_cost:
        policy_resolver = InitialPolicyResolver()
        policy = policy_resolver.resolve(now=now)
        cost_resolver = SignedExecutionCostResolver(clock=lambda: now)
        cost_contract = load_contract(cost_resolver.contract_path).to_dict()
        policy_contract = load_contract(policy_resolver.path).to_dict()
    else:
        cost_contract, policy_contract = _signed_contracts(now)
        policy = ResolvedPolicy(
            "policy-v1",
            POLICY_HASH,
            POLICY_MARKER_HASH,
            now,
            freeze_json(policy_contract["payload"]),
            freeze_json({"source": "production-e2e"}),
        )
        policy_resolver = _Resolver(policy)
        cost_resolver = _CostResolver(candidate_ids)
    authority = RiskTierAuthority.normal(RISK_CONTRACT_HASH)
    risk_resolver = _Resolver(authority)

    evidence_store = EvidenceStore(tmp_path / "evidence.sqlite")
    supporting = evidence_store.append(
        EvidenceRecord(
            identity="spy-supporting-production-e2e",
            kind="COMPANY_NEWS",
            symbol="SPY",
            provider="SEC",
            source_id="sec-supporting",
            published_at=now - timedelta(minutes=10),
            first_seen_at=now - timedelta(minutes=9),
            ingested_at=now - timedelta(minutes=8),
            observed_at=now - timedelta(minutes=7),
            payload={"title": "Supporting filing", "url": "https://www.sec.gov/example"},
        )
    ).evidence
    contradicting = evidence_store.append(
        EvidenceRecord(
            identity="spy-contradicting-production-e2e",
            kind="COMPANY_NEWS",
            symbol="SPY",
            provider="COMPANY_IR",
            source_id="ir-contradicting",
            published_at=now - timedelta(minutes=6),
            first_seen_at=now - timedelta(minutes=5),
            ingested_at=now - timedelta(minutes=4),
            observed_at=now - timedelta(minutes=3),
            payload={"title": "Contradicting release"},
        )
    ).evidence
    references = {
        candidate_id: {
            "supporting": (_evidence_ref(supporting),),
            "contradicting": (
                (_evidence_ref(contradicting),)
                if include_contradicting_evidence
                else ()
            ),
        }
        for candidate_id in candidate_ids
    }

    finalists = tuple(
        _finalist(
            candidate_id,
            expiry,
            now=now,
            include_terminal_scenarios=not real_scenario_and_cost,
        )
        for candidate_id in candidate_ids
    )
    universe = _Universe(finalists)
    generation_evidence = {
        "snapshot_hash": broker_snapshot.snapshot_hash,
        "broker_snapshot_hash": broker_snapshot.snapshot_hash,
        "evidence_hash": MARKET_EVIDENCE_HASH,
        "market_evidence_hash": MARKET_EVIDENCE_HASH,
        "liquidity_evidence_hash": LIQUIDITY_EVIDENCE_HASH,
        "spot": Decimal("102.50"),
        "atm_iv": Decimal("0.20"),
        "secdefs": secdefs,
        "quote_batch": quote_batch,
        "nav_snapshot": nav,
        "broker_snapshot": broker_snapshot,
        "execution_cost_contract": cost_contract,
        "policy_contract": policy_contract,
        "evidence_hashes": {
            "MARKET": MARKET_EVIDENCE_HASH,
            "VOLATILITY": "3" * 64,
            "LIQUIDITY": LIQUIDITY_EVIDENCE_HASH,
        },
    }
    generator = StrategyCandidateGenerator()
    volatility = VolatilityEngine()
    if observed_scenario_scores is not None:
        # Explicit test-only observations isolate scenario/cost finalization.
        # Production acquisition must supply its own complete signed features.
        direction_score, state_score = observed_scenario_scores
        generation_evidence["market_score"] = direction_score

        class ObservedFeatureVolatility(VolatilityEngine):
            def evaluate(self, raw, *, now=None):
                evidence = super().evaluate(raw, now=now)
                document = {item.name: getattr(evidence, item.name) for item in fields(evidence)}
                document["volatility_score"] = state_score
                document["evidence_hash"] = canonical_hash({
                    "volatility_evidence_hash": evidence.evidence_hash,
                    "test_observed_state_score": state_score,
                })
                return document

        volatility = ObservedFeatureVolatility()
    broker_evidence = _BrokerEvidence(generation_evidence)
    scenario = (
        ScenarioEngine(policy_resolver)
        if real_scenario_and_cost
        else _ExactScenarioPort(policy, authority)
    )
    gate = _EligibilityGate()
    ranker = PortfolioRanker()
    ranking_store = RankingStore(tmp_path / "ranking.sqlite")
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
    equity_thesis_row = normalize_equity_thesis_row(
        {
            "schema": "options_copilot.equity_thesis_evidence.v1",
            "symbol": "SPY",
            "direction_label": "BULLISH",
            "direction_score": "40",
            "uncertainty": "0.20",
            "observed_at": now,
            "source_hashes": ("8" * 64,),
            "canonical_input_hash": "9" * 64,
            "selected_rank": 1,
        },
        expected_symbol="SPY",
    )
    assert equity_thesis_row is not None
    equity_thesis_rows = (equity_thesis_row,)
    inputs = _Inputs(
        {
            "universe": {},
            "positions": (),
            "funnel_trace": {
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
                "equity_pool_reference": equity_reference,
                "equity_theses": {
                    "schema": "options_copilot.equity_theses.v1",
                    "equity_pool_reference_hash": canonical_hash(equity_reference),
                    "rows": equity_thesis_rows,
                    "rows_hash": canonical_hash(equity_thesis_rows),
                },
            },
            "candidate_evidence_references": references,
            "volatility": {
                "source": "IBKR",
                "observed_at": now - timedelta(seconds=1),
                "secdef_hash": "4" * 64,
                "quote_hash": "5" * 64,
                "quotes": tuple(
                    {
                        "bid": quote.bid,
                        "ask": quote.ask,
                        "implied_volatility": quote.implied_volatility,
                        "volume": quote.volume,
                        "open_interest": quote.open_interest,
                    }
                    for quote in quote_batch.quotes
                ),
                "iv_history": (
                    Decimal("0.15"),
                    Decimal("0.20"),
                    Decimal("0.25"),
                ),
                "atm_iv": Decimal("0.20"),
                "near_atm_iv": Decimal("0.20"),
                "next_atm_iv": Decimal("0.19"),
                "put_25_delta_iv": Decimal("0.22"),
                "call_25_delta_iv": Decimal("0.18"),
                "calendar_days": Decimal("17"),
            },
        }
    )
    pipeline = DecisionPipeline(
        inputs=inputs,
        universe_funnel=universe,
        broker_evidence=broker_evidence,
        strategy_registry=StrategyTemplateRegistry(),
        strategy_generator=generator,
        volatility_engine=volatility,
        scenario_engine=scenario,
        policy_resolver=policy_resolver,
        risk_authority_resolver=risk_resolver,
        cost_contract=cost_resolver,
        eligibility_gate=gate,
        portfolio_ranker=ranker,
        ranking_store=ranking_store,
        joint_ranking_required=True,
        clock=lambda: now,
    )
    approval_store = ProposalApprovalStore(tmp_path / "approvals.sqlite")
    bridge = _BridgeReadPort()
    options_evidence = _OptionsEvidence(contracts)
    nav_source = _NavSource(nav)
    position_manager = _PositionManager()
    risk_gate = _PassiveGate()
    dte_gate = _PassiveGate()
    single_combination_gate = _PassiveGate()
    broker_evidence.broker_snapshot_builder = broker_builder
    broker_evidence.options_evidence_acquisition = options_evidence
    broker_evidence.evidence_store = evidence_store
    broker_evidence.policy_resolver = policy_resolver
    broker_evidence.strategy_nav_source = nav_source
    inputs.position_manager = position_manager
    gate.risk_gate = risk_gate
    gate.dte_gate = dte_gate
    gate.single_combination_gate = single_combination_gate
    runtime = RuntimeServices(
        broker_snapshot_builder=broker_builder,
        evidence_store=evidence_store,
        scan_run_store=_ScanRunStore(),
        pipeline_inputs=inputs,
        universe_funnel=universe,
        broker_evidence_acquisition=broker_evidence,
        options_evidence_acquisition=options_evidence,
        strategy_registry=pipeline.strategy_registry,
        strategy_candidate_generator=generator,
        volatility_engine=volatility,
        scenario_engine=scenario,
        policy_resolver=policy_resolver,
        risk_authority_resolver=risk_resolver,
        execution_cost_contract=cost_resolver,
        eligibility_gate=gate,
        risk_gate=risk_gate,
        dte_gate=dte_gate,
        single_combination_gate=single_combination_gate,
        portfolio_ranker=ranker,
        ranking_store=ranking_store,
        decision_pipeline=pipeline,
        strategy_nav_source=nav_source,
        position_manager=position_manager,
        approval_store=approval_store,
        bridge_status_reader=bridge,
        bridge_reconciliation_reader=bridge,
        approval_blockers=approval_blockers,
    )
    assert runtime.readiness()["status"] == (
        "DEGRADED" if approval_blockers else "READY"
    )
    result = runtime.run_slot(scan_run_id, now)
    app = create_app(_api_services(runtime))
    return _Harness(
        now,
        scan_run_id,
        result,
        ranking_store,
        evidence_store,
        approval_store,
        runtime,
        app,
        cost_resolver,
    )


@pytest.fixture
def production_harness(tmp_path: Path):
    harness = _build_harness(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


def _terms(rows: object, price_fields: Sequence[str]) -> tuple[tuple[Decimal, Decimal], ...]:
    assert isinstance(rows, Sequence) and not isinstance(rows, (str, bytes))
    result: list[tuple[Decimal, Decimal]] = []
    for raw in rows:
        assert isinstance(raw, Mapping)
        price = next(raw[field] for field in price_fields if field in raw)
        result.append((_stored_decimal(price), _stored_decimal(raw["probability"])))
    return tuple(result)


def _stored_decimal(value: object) -> Decimal:
    if isinstance(value, Mapping) and set(value) == {"$decimal"}:
        value = value["$decimal"]
    return Decimal(str(value))


def _snapshot_with_positions(snapshot, positions: tuple[Mapping[str, object], ...]):
    position_hash = canonical_hash(positions)
    state_evidence = dict(snapshot.state_evidence)
    state_evidence["positions"] = StateComponentEvidence(
        "positions",
        True,
        len(positions),
        position_hash,
        position_hash,
        True,
        positions,
    )
    interim = replace(
        snapshot,
        state_evidence=state_evidence,
        snapshot_hash="0" * 64,
    )
    return replace(interim, snapshot_hash=canonical_hash(interim.hash_payload()))


def test_joint_account_context_is_bound_to_same_atomic_snapshot(
    production_harness: _Harness,
) -> None:
    acquisition = production_harness.runtime.broker_evidence_acquisition
    evidence = dict(acquisition.payload)
    snapshot = evidence["broker_snapshot"]
    candidates = (SimpleNamespace(payload={"symbol": "SPY"}),)

    valid, reason = _joint_account_context(
        candidates=candidates,
        gate={"open_combinations": 0},
        evidence=evidence,
        broker_snapshot_hash=snapshot.snapshot_hash,
    )
    assert reason is None
    assert valid is not None
    assert valid.aggregate_open_risk_usd == 0

    missing, reason = _joint_account_context(
        candidates=candidates,
        gate={"open_combinations": 0},
        evidence={"nav_snapshot": evidence["nav_snapshot"]},
        broker_snapshot_hash=snapshot.snapshot_hash,
    )
    assert missing is None
    assert reason == "JOINT_ACCOUNT_SNAPSHOT_INVALID"

    mismatch, reason = _joint_account_context(
        candidates=candidates,
        gate={"open_combinations": 0},
        evidence=evidence,
        broker_snapshot_hash="0" * 64,
    )
    assert mismatch is None
    assert reason == "JOINT_ACCOUNT_SNAPSHOT_INVALID"

    stock_snapshot = _snapshot_with_positions(
        snapshot,
        ({
            "symbol": "SPY",
            "security_type": "STK",
            "quantity": "1",
            "market_value": "500",
        },),
    )
    stock_evidence = {**evidence, "broker_snapshot": stock_snapshot}
    stock_context, reason = _joint_account_context(
        candidates=candidates,
        gate={"open_combinations": 0},
        evidence=stock_evidence,
        broker_snapshot_hash=stock_snapshot.snapshot_hash,
    )
    assert reason is None
    assert stock_context is not None
    assert stock_context.concentration_by_underlying["SPY"] == Decimal("0.25")

    option_snapshot = _snapshot_with_positions(
        snapshot,
        ({
            "symbol": "QQQ",
            "security_type": "OPT",
            "quantity": "1",
            "market_value": "100",
        },),
    )
    option_evidence = {**evidence, "broker_snapshot": option_snapshot}
    open_context, reason = _joint_account_context(
        candidates=candidates,
        gate={"open_combinations": 1},
        evidence=option_evidence,
        broker_snapshot_hash=option_snapshot.snapshot_hash,
    )
    assert open_context is None
    assert reason == "JOINT_OPEN_POSITION_RISK_UNAVAILABLE"


def test_real_scenario_engine_finalizes_generator_skeleton_before_signed_cost(
    tmp_path: Path,
) -> None:
    harness = _build_harness(
        tmp_path, real_scenario_and_cost=True,
        observed_scenario_scores=(Decimal("0"), Decimal("0")),
    )
    try:
        assert harness.pipeline_result["status"] == "TRADE"
        stored = harness.ranking_store.read_snapshot(
            str(harness.pipeline_result["ranking_snapshot_id"])
        )
        scenario_records = {
            row.record["candidate_id"]: row.record["scenario"]
            for row in harness.ranking_store.read_decisions(harness.scan_run_id)
            if row.record_type == "SCENARIO"
        }

        # Both generated rows share SPY.  Joint ranking admits only one into
        # the executable ranking and preserves the other in the separately
        # hash-bound research watchlist.
        assert len(stored["candidates"]) == 1
        joint = stored["immutable_inputs"]["joint_ranking"]
        assert [row["candidate_id"] for row in joint["executable"]] == [
            "candidate-a"
        ]
        assert [row["candidate_id"] for row in joint["research_watchlist"]] == [
            "candidate-b"
        ]
        for row in stored["candidates"]:
            candidate_id = row["candidate_id"]
            candidate_terms = _terms(
                row["candidate_body"]["terminal_scenarios"],
                ("terminal_underlying_price",),
            )
            proposal_terms = _terms(
                row["proposal_body"]["terminal_scenarios"],
                ("terminal_underlying_price",),
            )
            scenario_terms = _terms(
                scenario_records[candidate_id]["scenarios"],
                ("terminal_price", "terminal_underlying_price"),
            )
            assert len(scenario_terms) == 5
            assert candidate_terms == proposal_terms == scenario_terms
            assert row["candidate_hash"] == canonical_hash(row["candidate_body"])
            assert Decimal(row["proposal_body"]["expected_value_usd"]) > 0
    finally:
        harness.close()


def test_real_scenario_without_observed_features_does_not_publish_ranking(tmp_path) -> None:
    harness = _build_harness(tmp_path, real_scenario_and_cost=True)
    try:
        assert harness.pipeline_result["status"] == "NO_TRADE"
        assert "MISSING_MARKET_DIRECTION_INPUT" in harness.pipeline_result["reasons"]
        assert "MISSING_VOLATILITY_STATE_INPUT" in harness.pipeline_result["reasons"]
        assert harness.ranking_store.get_by_scan_run(harness.scan_run_id) is None
    finally:
        harness.close()


def test_real_candidates_are_finalized_after_ranking_and_rehashed(
    production_harness: _Harness,
) -> None:
    harness = production_harness
    assert harness.pipeline_result["status"] == "TRADE"
    snapshot_id = str(harness.pipeline_result["ranking_snapshot_id"])
    stored = harness.ranking_store.read_snapshot(snapshot_id)
    rows = stored["candidates"]
    assert [row["candidate_id"] for row in rows] == ["candidate-a"]
    joint = stored["immutable_inputs"]["joint_ranking"]
    assert joint["snapshot_hash"] == stored["immutable_inputs"]["funnel_trace"][
        "joint_ranking"
    ]["snapshot_hash"]
    assert [row["candidate_id"] for row in joint["research_watchlist"]] == [
        "candidate-b"
    ]

    rank_one = rows[0]
    assert rank_one["proposal_body"]["schema"] == "options_copilot.proposal.v1"
    assert rank_one["proposal_body"]["rank"] == 1
    assert rank_one["proposal_body"]["eligible_to_send"] is True

    for row in rows:
        underlying_basis = row["candidate_body"]["underlying_quote_basis"]
        assert row["candidate_body"]["underlying_quote_basis_hash"] == (
            canonical_hash(underlying_basis)
        )
        basis = build_ranking_basis(
            candidate_body=row["candidate_body"],
            proposal_body=row["proposal_body"],
            candidate_hash=row["candidate_hash"],
            proposal_hash=row["proposal_hash"],
            current_policy_version=stored["current_policy_version"],
            current_policy_hash=stored["current_policy_hash"],
            policy_authority_marker_hash=stored["policy_authority_marker_hash"],
            cost_version=stored["cost_version"],
            cost_hash=stored["cost_hash"],
            risk_contract_hash=stored["risk_contract_hash"],
            evidence_inputs=stored["immutable_inputs"],
        )
        assert row["proposal_hash"] == canonical_hash(row["proposal_body"])
        assert row["proposal_hash"] == basis.proposal_hash
        assert row["ranking_basis_hash"] == basis.ranking_basis_hash


def test_underlying_quote_basis_mutation_invalidates_immutable_ranking(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path)
    try:
        assert harness.pipeline_result["status"] == "TRADE"
        snapshot_id = str(harness.pipeline_result["ranking_snapshot_id"])
        with sqlite3.connect(harness.ranking_store.path) as connection:
            row = connection.execute(
                "SELECT candidate_body_json FROM ranking_rows "
                "WHERE ranking_snapshot_id=? AND rank=1",
                (snapshot_id,),
            ).fetchone()
            assert row is not None
            body = json.loads(str(row[0]))
            body["underlying_quote_basis"]["close"] = "999"
            connection.execute("DROP TRIGGER ranking_rows_no_update")
            connection.execute(
                "UPDATE ranking_rows SET candidate_body_json=? "
                "WHERE ranking_snapshot_id=? AND rank=1",
                (json.dumps(body, sort_keys=True), snapshot_id),
            )

        result = harness.runtime.ranking(snapshot_id)
        assert result["decision"] == "NO_TRADE"
        assert "IMMUTABLE_RANKING_NOT_FOUND" in result["reasons"]
    finally:
        harness.close()


def test_real_scenario_cost_manifest_and_api_projection_are_identical(
    production_harness: _Harness,
) -> None:
    harness = production_harness
    assert harness.pipeline_result["status"] == "TRADE"
    snapshot_id = str(harness.pipeline_result["ranking_snapshot_id"])
    stored = harness.ranking_store.read_snapshot(snapshot_id)
    scenario_records = {
        row.record["candidate_id"]: row.record["scenario"]
        for row in harness.ranking_store.read_decisions(harness.scan_run_id)
        if row.record_type == "SCENARIO"
    }
    adjusted = {
        row["candidate_id"]: row["after_cost_expected_value"]
        for row in harness.cost_resolver.value["candidates"]
    }
    for row in stored["candidates"]:
        candidate_id = row["candidate_id"]
        proposal = row["proposal_body"]
        assert proposal["schema"] != "options_copilot.proposal_from_candidate.v1"
        assert _terms(
            proposal["terminal_scenarios"], ("terminal_underlying_price",)
        ) == _terms(
            scenario_records[candidate_id]["scenarios"],
            ("terminal_price", "terminal_underlying_price"),
        )
        assert Decimal(proposal["expected_value_usd"]) == adjusted[candidate_id]
        assert proposal["execution_cost_contract"] == {
            "version": stored["cost_version"],
            "hash": stored["cost_hash"],
        }

    latest = asyncio.run(_route(harness.app, "/api/rankings/latest")())
    assert latest["ranking_snapshot_id"] == snapshot_id
    assert [row["interaction"] for row in latest["candidates"]] == ["VIEW_ONLY"]
    latest_rows = [
        latest["candidates"][0],
        *latest["candidates"][0]["alternatives"],
    ]
    assert len(latest_rows) == 1
    for row in latest_rows:
        assert row["source_health"]["status"] == "DEGRADED"
        assert (
            row["source_health"]["reason"]
            == "CANDIDATE_EVIDENCE_CONTRADICTED"
        )
        assert row["account_capacity"]["status"] == "READY"

    evidence = asyncio.run(
        _route(
            harness.app,
            "/api/scans/{scan_run_id}/candidates/{candidate_id}/evidence",
        )(harness.scan_run_id, "candidate-a")
    )
    assert evidence["status"] == "READY"
    assert evidence["decision"] == "OBSERVATION_ONLY"
    assert len(evidence["primary"]) == 8
    assert [item["payload"]["title"] for item in evidence["supporting"]] == [
        "Supporting filing"
    ]
    assert [item["payload"]["title"] for item in evidence["contradicting"]] == [
        "Contradicting release"
    ]


def test_latest_ranking_derives_source_health_and_capacity_for_main_and_alternative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _build_harness(tmp_path, include_contradicting_evidence=False)
    try:
        latest = asyncio.run(_route(harness.app, "/api/rankings/latest")())
        assert len(latest["candidates"]) == 1
        rows = [latest["candidates"][0], *latest["candidates"][0]["alternatives"]]
        assert len(rows) == 1
        assert [row["interaction"] for row in rows] == ["CHALLENGE_ALLOWED"]
        for row in rows:
            assert row["source_health"] == {
                "status": "READY",
                "reason": "CANDIDATE_EVIDENCE_PRIMARY_COMPLETE",
                "primary_count": 8,
                "supporting_count": 1,
                "contradicting_count": 0,
                "decision_authority": "SUPPORTING_ONLY",
            }
            assert row["account_capacity"]["status"] == "READY"
            assert row["account_capacity"]["reason"] == "ACCOUNT_CAPACITY_CONFIRMED"
            assert row["account_capacity"]["decision_authority"] == "SUPPORTING_ONLY"

        base_services = _api_services(harness.runtime)
        service_values = {
            name: getattr(base_services, name)
            for name in OptionsCopilotServices.__dataclass_fields__
            if name != "latest_ranking_provider"
        }

        def project(payload: Mapping[str, object]) -> Mapping[str, object]:
            app = create_app(
                OptionsCopilotServices(
                    **service_values,
                    latest_ranking_provider=lambda: payload,
                )
            )
            return asyncio.run(_route(app, "/api/rankings/latest")())

        frozen = harness.ranking_store.read_snapshot(
            str(harness.pipeline_result["ranking_snapshot_id"])
        )
        hostile_authority = copy.deepcopy(frozen)
        hostile_authority["approval_enabled"] = True
        hostile_authority["decision"] = "EXECUTE"
        hostile_authority["review_only"] = False
        hostile_authority["direct_order_submission"] = True
        hostile_result = project(hostile_authority)
        assert hostile_result["decision"] == "NO_TRADE"
        assert hostile_result["approval_enabled"] is False
        assert hostile_result["review_only"] is True
        assert hostile_result["direct_order_submission"] is False
        hostile_rows = [
            hostile_result["candidates"][0],
            *hostile_result["candidates"][0]["alternatives"],
        ]
        assert all(row["interaction"] == "VIEW_ONLY" for row in hostile_rows)


        rehashed = copy.deepcopy(frozen)
        rehashed_manifest = rehashed["immutable_inputs"][
            "candidate_evidence_manifests"
        ]["candidate-a"]
        rehashed_manifest["symbol"] = "QQQ"
        for primary in rehashed_manifest["primary"]:
            record = primary["record"]
            if "symbol" in record:
                record["symbol"] = "QQQ"
            if "underlying" in record:
                record["underlying"] = "QQQ"
            primary["record_hash"] = canonical_hash(record)
        manifest_body = dict(rehashed_manifest)
        manifest_body.pop("manifest_hash")
        rehashed_manifest["manifest_hash"] = canonical_hash(manifest_body)
        rehashed_result = project(rehashed)
        assert (
            rehashed_result["candidates"][0]["source_health"]["reason"]
            == "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        )

        after_cutoff = copy.deepcopy(frozen)
        after_cutoff_manifest = after_cutoff["immutable_inputs"][
            "candidate_evidence_manifests"
        ]["candidate-a"]
        cutoff = datetime.fromisoformat(after_cutoff_manifest["cutoff_at"])
        after_cutoff_primary = after_cutoff_manifest["primary"][0]
        after_cutoff_primary["record"]["observed_at"] = (
            cutoff + timedelta(seconds=1)
        ).isoformat()
        after_cutoff_primary["record_hash"] = canonical_hash(
            after_cutoff_primary["record"]
        )
        after_cutoff_body = dict(after_cutoff_manifest)
        after_cutoff_body.pop("manifest_hash")
        after_cutoff_manifest["manifest_hash"] = canonical_hash(after_cutoff_body)
        after_cutoff_result = project(after_cutoff)
        assert (
            after_cutoff_result["candidates"][0]["source_health"]["reason"]
            == "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        )
        assert after_cutoff_result["candidates"][0]["interaction"] == "VIEW_ONLY"

        candidate_mismatch = copy.deepcopy(frozen)
        mismatch_manifest = candidate_mismatch["immutable_inputs"][
            "candidate_evidence_manifests"
        ]["candidate-a"]
        mismatch_primary = mismatch_manifest["primary"][0]
        mismatch_primary["record"]["candidate_id"] = "candidate-b"
        mismatch_primary["record_hash"] = canonical_hash(mismatch_primary["record"])
        mismatch_body = dict(mismatch_manifest)
        mismatch_body.pop("manifest_hash")
        mismatch_manifest["manifest_hash"] = canonical_hash(mismatch_body)
        mismatch_result = project(candidate_mismatch)
        assert (
            mismatch_result["candidates"][0]["source_health"]["reason"]
            == "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        )
        assert mismatch_result["candidates"][0]["interaction"] == "VIEW_ONLY"

        candidate_binding_mismatch = copy.deepcopy(frozen)
        binding_manifest = candidate_binding_mismatch["immutable_inputs"][
            "candidate_evidence_manifests"
        ]["candidate-a"]
        broker_primary = next(
            item
            for item in binding_manifest["primary"]
            if item["kind"] == "BROKER_SNAPSHOT"
        )
        broker_primary["record"]["broker_snapshot_hash"] = "9" * 64
        broker_primary["record_hash"] = canonical_hash(broker_primary["record"])
        binding_body = dict(binding_manifest)
        binding_body.pop("manifest_hash")
        binding_manifest["manifest_hash"] = canonical_hash(binding_body)

        binding_result = project(candidate_binding_mismatch)
        assert (
            binding_result["candidates"][0]["source_health"]["reason"]
            == "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        )
        assert binding_result["candidates"][0]["interaction"] == "VIEW_ONLY"

        with monkeypatch.context() as scoped:
            scoped.setattr(
                type(harness.ranking_store),
                "read_snapshot",
                lambda _store, _snapshot_id: candidate_binding_mismatch,
            )
            resolved = harness.runtime.candidate_evidence(
                harness.scan_run_id,
                "candidate-a",
            )
        assert resolved["decision"] == "NO_TRADE"
        assert (
            "CANDIDATE_EVIDENCE_PRIMARY_BINDING_MISMATCH"
            in resolved["reasons"]
        )

        duplicate_primary = copy.deepcopy(frozen)
        duplicate_manifest = duplicate_primary["immutable_inputs"][
            "candidate_evidence_manifests"
        ]["candidate-a"]
        duplicate_manifest["primary"][-1] = copy.deepcopy(
            duplicate_manifest["primary"][0]
        )
        duplicate_body = dict(duplicate_manifest)
        duplicate_body.pop("manifest_hash")
        duplicate_manifest["manifest_hash"] = canonical_hash(duplicate_body)
        duplicate_result = project(duplicate_primary)
        assert (
            duplicate_result["candidates"][0]["source_health"]["reason"]
            == "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        )
        assert duplicate_result["candidates"][0]["interaction"] == "VIEW_ONLY"

        incomplete = harness.ranking_store.read_snapshot(
            str(harness.pipeline_result["ranking_snapshot_id"])
        )
        for row in incomplete["candidates"]:
            row["source_health"] = {"status": "READY", "reason": "HOSTILE_READY"}
            row["account_capacity"] = {"status": "READY", "reason": "HOSTILE_READY"}
            row["candidate_body"].pop("strategy_nav_usd", None)
            strategy_nav = row["proposal_body"].get("strategy_nav")
            if isinstance(strategy_nav, dict):
                strategy_nav.pop("strategy_nav_usd", None)

        blocked = project(incomplete)
        blocked_rows = [
            blocked["candidates"][0],
            *blocked["candidates"][0]["alternatives"],
        ]
        for row in blocked_rows:
            assert row["source_health"]["reason"] != "HOSTILE_READY"
            assert row["account_capacity"]["status"] == "BLOCKED"
            assert row["account_capacity"]["reason"] == "STRATEGY_NAV_UNAVAILABLE"
            assert row["account_capacity"]["decision_authority"] == "SUPPORTING_ONLY"
            assert row["interaction"] == "VIEW_ONLY"
    finally:
        harness.close()


def test_creator_transport_blocker_preserves_review_only_ranked_recommendations(
    tmp_path: Path,
) -> None:
    harness = _build_harness(
        tmp_path,
        include_contradicting_evidence=False,
        approval_blockers=("CREATOR_TRANSPORT_UNAVAILABLE",),
    )
    try:
        readiness = harness.runtime.readiness()
        assert readiness["decision"] == "READY"
        assert readiness["approval_enabled"] is False
        assert readiness["approval_blockers"] == (
            "CREATOR_TRANSPORT_UNAVAILABLE",
        )

        runtime_ranking = harness.runtime.latest_ranking()
        assert runtime_ranking["decision"] == "CANDIDATES_AVAILABLE"
        assert runtime_ranking["recommendations_available"] is True
        assert runtime_ranking["approval_enabled"] is False
        assert runtime_ranking["approval_blockers"] == (
            "CREATOR_TRANSPORT_UNAVAILABLE",
        )
        assert len(runtime_ranking["candidates"]) == 1

        api_ranking = asyncio.run(_route(harness.app, "/api/rankings/latest")())
        assert api_ranking["decision"] == "CANDIDATES_AVAILABLE"
        assert api_ranking["recommendations_available"] is True
        assert api_ranking["approval_enabled"] is False
        assert len(api_ranking["candidates"]) == 1
        assert api_ranking["candidates"][0]["interaction"] == "VIEW_ONLY"
        assert api_ranking["candidates"][0]["recommendation_ready"] is True

        endpoint = _route(
            harness.app,
            "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
            "POST",
        )
        with pytest.raises(HTTPException) as blocked:
            asyncio.run(
                endpoint(
                    str(runtime_ranking["ranking_snapshot_id"]),
                    "candidate-a",
                    RankOneChallengeRequest(),
                )
            )
        assert blocked.value.status_code == 503
        assert "CREATOR_TRANSPORT_UNAVAILABLE" in str(blocked.value.detail)
    finally:
        harness.close()


def test_only_current_frozen_rank_one_can_create_a_review_challenge(
    tmp_path: Path,
) -> None:
    harness = _build_harness(tmp_path, include_contradicting_evidence=False)
    try:
        assert harness.pipeline_result["status"] == "TRADE"
        snapshot_id = str(harness.pipeline_result["ranking_snapshot_id"])
        endpoint = _route(
            harness.app,
            "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
            "POST",
        )

        with pytest.raises(HTTPException) as forbidden:
            asyncio.run(endpoint(snapshot_id, "candidate-b", RankOneChallengeRequest()))
        assert forbidden.value.status_code in {403, 404}

        challenge = asyncio.run(
            endpoint(snapshot_id, "candidate-a", RankOneChallengeRequest())
        )
        assert challenge["status"] == "PENDING_SECOND_CONFIRMATION"
        assert challenge["candidate_id"] == "candidate-a"
        assert challenge["review_only"] is True
        assert challenge["instruction_id"] is None
        assert challenge["order_submitted"] is False
        assert challenge["transmitted_to_broker"] is False
    finally:
        harness.close()


def test_direct_challenge_post_reuses_fail_closed_candidate_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _build_harness(tmp_path, include_contradicting_evidence=True)
    try:
        snapshot_id = str(harness.pipeline_result["ranking_snapshot_id"])
        endpoint = _route(
            harness.app,
            "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
            "POST",
        )
        with pytest.raises(HTTPException) as contradicted:
            asyncio.run(endpoint(snapshot_id, "candidate-a", RankOneChallengeRequest()))
        assert contradicted.value.status_code in {403, 409}

        ready = dict(harness.runtime.ranking(snapshot_id))
        for mode in ("HOLLOW", "AFTER_CUTOFF"):
            hostile = copy.deepcopy(ready)
            manifest = hostile["immutable_inputs"]["candidate_evidence_manifests"][
                "candidate-a"
            ]
            if mode == "HOLLOW":
                for primary in manifest["primary"]:
                    primary["source"] = "LOCAL_DETERMINISTIC"
                    primary["record"] = {"symbol": "SPY"}
                    primary["record_hash"] = canonical_hash(primary["record"])
            else:
                cutoff = datetime.fromisoformat(manifest["cutoff_at"])
                primary = manifest["primary"][0]
                primary["record"]["observed_at"] = (
                    cutoff + timedelta(seconds=1)
                ).isoformat()
                primary["record_hash"] = canonical_hash(primary["record"])
            manifest_body = dict(manifest)
            manifest_body.pop("manifest_hash")
            manifest["manifest_hash"] = canonical_hash(manifest_body)

            with monkeypatch.context() as scoped:
                scoped.setattr(
                    RuntimeServices,
                    "ranking",
                    lambda self, ranking_id, payload=hostile: payload,
                )
                latest = asyncio.run(_route(harness.app, "/api/rankings/latest")())
                assert latest["candidates"][0]["source_health"]["status"] == "DEGRADED"
                assert latest["candidates"][0]["interaction"] == "VIEW_ONLY"
                with pytest.raises(HTTPException) as blocked:
                    asyncio.run(
                        endpoint(
                            snapshot_id,
                            "candidate-a",
                            RankOneChallengeRequest(),
                        )
                    )
                assert blocked.value.status_code in {403, 409}

        with sqlite3.connect(tmp_path / "approvals.sqlite") as connection:
            created = connection.execute(
                "SELECT COUNT(*) FROM approval_challenges"
            ).fetchone()[0]
        assert created == 0
    finally:
        harness.close()


@pytest.mark.parametrize(
    ("target", "expected_reason"),
    (
        ("ranking", "RANKING_DECISION_LEDGER_UNAVAILABLE"),
        ("proposal", "IMMUTABLE_RANKING_NOT_FOUND"),
        ("evidence", "CANDIDATE_EVIDENCE_STORE_UNAVAILABLE"),
        ("manifest", "CANDIDATE_EVIDENCE_UNAVAILABLE"),
    ),
)
def test_ranking_proposal_evidence_and_manifest_tamper_fail_closed(
    tmp_path: Path,
    target: str,
    expected_reason: str,
) -> None:
    harness = _build_harness(tmp_path)
    try:
        assert harness.pipeline_result["status"] == "TRADE"
        snapshot_id = str(harness.pipeline_result["ranking_snapshot_id"])
        if target == "ranking":
            with sqlite3.connect(harness.ranking_store.path) as connection:
                connection.execute("DROP TRIGGER ranking_snapshots_no_update")
                connection.execute(
                    "UPDATE ranking_snapshots SET snapshot_hash=? WHERE ranking_snapshot_id=?",
                    ("0" * 64, snapshot_id),
                )
            result = harness.runtime.latest_ranking()
            assert result["decision"] == "NO_TRADE"
            assert expected_reason in result["reasons"]
        elif target == "proposal":
            with sqlite3.connect(harness.ranking_store.path) as connection:
                connection.execute("DROP TRIGGER ranking_rows_no_update")
                connection.execute(
                    "UPDATE ranking_rows SET proposal_body_json=? "
                    "WHERE ranking_snapshot_id=? AND rank=1",
                    ('{"schema":"tampered"}', snapshot_id),
                )
            result = harness.runtime.ranking(snapshot_id)
            assert result["decision"] == "NO_TRADE"
            assert expected_reason in result["reasons"]
        elif target == "evidence":
            with sqlite3.connect(harness.evidence_store.path) as connection:
                connection.execute("DROP TRIGGER evidence_records_no_update")
                connection.execute(
                    "UPDATE evidence_records SET row_hash=? WHERE sequence=1",
                    ("0" * 64,),
                )
            result = harness.runtime.candidate_evidence(
                harness.scan_run_id, "candidate-a"
            )
            assert result["decision"] == "NO_TRADE"
            assert result["primary"] == []
            assert result["supporting"] == []
            assert result["contradicting"] == []
            assert expected_reason in result["reasons"]
        else:
            with sqlite3.connect(harness.ranking_store.path) as connection:
                connection.execute("DROP TRIGGER ranking_snapshots_no_update")
                connection.execute(
                    "UPDATE ranking_snapshots SET immutable_inputs_hash=? "
                    "WHERE ranking_snapshot_id=?",
                    ("0" * 64, snapshot_id),
                )
            result = harness.runtime.candidate_evidence(
                harness.scan_run_id, "candidate-a"
            )
            assert result["decision"] == "NO_TRADE"
            assert result["primary"] == []
            assert result["supporting"] == []
            assert result["contradicting"] == []
            assert expected_reason in result["reasons"]
    finally:
        harness.close()
