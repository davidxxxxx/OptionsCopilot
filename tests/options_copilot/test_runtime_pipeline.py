from __future__ import annotations

import inspect
import json
import threading
import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import options_copilot.intraday_top10_recovery as intraday_recovery_module
import options_copilot.runtime as runtime_module

from options_copilot.analytics.scenarios import InitialPolicyResolver, ResolvedPolicy
from options_copilot.api import (
    OptionsCopilotServices,
    OptionsCopilotUnavailable,
    ProposalApprovalConflict,
    RankOneAuthorizationForbidden,
)
from options_copilot.approval import ProposalApprovalStore
from options_copilot.approval.store import ApprovalChallengeRejected
from options_copilot.bridge import CodexBridgeStore
from options_copilot.config import OptionsCopilotConfig
from options_copilot.decision.pipeline import DecisionPipeline
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    SignedExecutionCostResolver,
)
from options_copilot.gateway import (
    BatchedOptionQuote,
    BrokerSnapshotBuilder,
    IBKRReadOnlyGateway,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)
from options_copilot.governance.contracts import create_correction, load_contract
from options_copilot.learning.policy_authority import (
    CurrentPolicyResolver,
    PolicyAuthorityTampered,
)
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.production_runtime import (
    ProductionBrokerEvidenceAcquisition,
    creator_unavailable_reason,
)
from options_copilot.ranking.evidence_manifest import (
    build_candidate_evidence_manifest,
)
from options_copilot.ranking.joint import (
    JointDisposition,
    JointRankingRow,
    JointRankingSnapshot,
)
from options_copilot.ranking.portfolio import PortfolioRanker
from options_copilot.ranking.store import RankingStore
from options_copilot.research_allocation import build_research_allocation_evidence
from options_copilot.risk.authorization import RiskTierAuthority
from options_copilot.risk.resolver import (
    CurrentRiskAuthorityResolver,
    PolicyLedgerRiskAuthorityMarkerSource,
)
from options_copilot.runtime import (
    BASELINE_MODEL_VERSION,
    OptionsCopilotRuntime,
    RuntimeServices,
    _normalise_research_allocation,
    _shutdown_runtime_for_asgi,
    _unavailable_runtime_services,
    build_production_composition,
)
from options_copilot.storage.canonical import canonical_hash, freeze_json
from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore


NOW = datetime(2026, 8, 4, 12, tzinfo=timezone.utc)
POLICY_HASH = "1" * 64
POLICY_MARKER_HASH = "2" * 64
COST_HASH = "3" * 64
RISK_CONTRACT_HASH = "4" * 64


def test_asgi_shutdown_wrapper_surfaces_bounded_incomplete_close() -> None:
    closed = SimpleNamespace(close=lambda: True)
    assert _shutdown_runtime_for_asgi(closed) is None

    incomplete = SimpleNamespace(close=lambda: False)
    with pytest.raises(RuntimeError, match="^OPTIONS_COPILOT_SHUTDOWN_INCOMPLETE$"):
        _shutdown_runtime_for_asgi(incomplete)


class _Port:
    def __init__(self, value: object) -> None:
        self.value = value

    def run(self, **_: object) -> object:
        return self.value

    def is_current(self, value: object) -> bool:
        return value == self.value


class _Resolver:
    def __init__(self, value: object) -> None:
        self.value = value
        self.resolve_calls = 0
        self.current = True

    def resolve(self, **_: object) -> object:
        self.resolve_calls += 1
        return self.value

    def is_current(self, value: object) -> bool:
        return self.current and value == self.value

    def guard_current(self, value: object, *, callback):
        if not self.is_current(value):
            return None
        return callback()


class _ResolveOnly:
    def __init__(self, value: object) -> None:
        self.value = value

    def resolve(self, **_: object) -> object:
        return self.value


class _ProtocolPort:
    """Structural fake for the production ports named by RuntimeServices."""

    _METHODS = {
        "acquire",
        "append_decisions",
        "append_snapshot",
        "apply",
        "approve",
        "authorize_frozen_rank_one",
        "build",
        "complete",
        "confirm_challenge",
        "create_challenge",
        "create_rank_one_challenge",
        "current_terminal_for_snapshot",
        "evaluate",
        "evaluate_pre_cost",
        "fail",
        "generate",
        "get",
        "get_challenge",
        "get_by_scan_run",
        "guard_current",
        "is_current",
        "latest",
        "list_active",
        "list_pending",
        "query",
        "rank",
        "read_model",
        "read_snapshot",
        "reconciliation_status",
        "resolve",
        "resolve_contracts",
        "run",
        "run_slot",
        "runs_for_slot",
        "snapshot",
        "validate",
        "verify_integrity",
    }

    def __init__(self, value: object = None) -> None:
        self.value = value

    def __getattr__(self, name: str):
        if name not in self._METHODS:
            raise AttributeError(name)

        def invoke(*_: object, **__: object) -> object:
            return self.value

        return invoke

    def guard_current(self, value: object, *, callback):
        if value != self.value:
            return None
        return callback()


class _SlotPipeline(_ProtocolPort):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def run_slot(self, scan_run_id: str, slot_at: datetime) -> dict[str, object]:
        self.calls += 1
        return {
            "scan_run_id": scan_run_id,
            "slot_at": slot_at,
            "status": "NO_TRADE",
            "decision": "NO_TRADE",
        }


@dataclass(frozen=True, slots=True)
class _StoredSnapshot:
    ranking_snapshot_id: str
    scan_run_id: str
    expected_hashes: object


class _RankingPort(_ProtocolPort):
    def __init__(self, payload: dict[str, object]) -> None:
        super().__init__()
        self.payload = payload
        self.stored = _StoredSnapshot(
            str(payload["ranking_snapshot_id"]),
            str(payload["scan_run_id"]),
            {"candidate_hash": "c" * 64},
        )
        self.authorize_args: tuple[object, ...] | None = None
        self.terminal_current = True

    def latest(self) -> _StoredSnapshot:
        return self.stored

    def get_by_scan_run(self, scan_run_id: str) -> _StoredSnapshot | None:
        return self.stored if scan_run_id == self.stored.scan_run_id else None

    def read_snapshot(self, ranking_snapshot_id: str) -> dict[str, object]:
        if ranking_snapshot_id != self.stored.ranking_snapshot_id:
            raise KeyError(ranking_snapshot_id)
        return self.payload

    def current_terminal_for_snapshot(self, ranking_snapshot_id: str) -> bool:
        return (
            ranking_snapshot_id == self.stored.ranking_snapshot_id
            and self.terminal_current
        )

    def authorize_frozen_rank_one(self, *args: object, **_: object) -> object:
        self.authorize_args = args
        return {"authorized": True}

    def append_decisions(self, *_: object, **__: object) -> None:
        return None

    def append_snapshot(self, *_: object, **__: object) -> None:
        return None


class _ApprovalPort(_ProtocolPort):
    def __init__(self) -> None:
        super().__init__()
        self.create_kwargs: dict[str, object] | None = None
        self.confirm_kwargs: dict[str, object] | None = None
        self.challenge_record: dict[str, object] | None = None
        self.create_error: Exception | None = None
        self.get_error: Exception | None = None
        self.confirm_error: Exception | None = None

    def create_challenge(
        self, ranking_snapshot_id: str, candidate_id: str, **kwargs: object
    ) -> dict[str, object]:
        if self.create_error is not None:
            raise self.create_error
        self.create_kwargs = kwargs
        ranking_store = kwargs["ranking_store"]
        snapshot = ranking_store.read_snapshot(ranking_snapshot_id)
        frozen = snapshot["candidates"][0]
        self.challenge_record = {
            "challenge_id": "challenge-1",
            "ranking_snapshot_id": ranking_snapshot_id,
            "candidate_id": candidate_id,
            "proposal_hash": "a" * 64,
            "candidate_body": frozen["candidate_body"],
            "proposal_body": frozen["proposal_body"],
            "challenge_response": "x" * 32,
            "expires_at": (
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat(),
        }
        return self.challenge_record

    def get_challenge(self, challenge_id: str) -> dict[str, object] | None:
        if self.get_error is not None:
            raise self.get_error
        if self.challenge_record is None:
            return None
        return (
            self.challenge_record
            if challenge_id == self.challenge_record["challenge_id"]
            else None
        )

    def confirm_challenge(
        self, challenge_id: str, **kwargs: object
    ) -> dict[str, object]:
        if self.confirm_error is not None:
            raise self.confirm_error
        self.confirm_kwargs = {"challenge_id": challenge_id, **kwargs}
        return {
            "approval": {
                "approval_id": "approval-1",
                "proposal_hash": "a" * 64,
                "expires_at": (
                    datetime.now(timezone.utc) + timedelta(minutes=5)
                ).isoformat(),
            }
        }


def _contracts() -> tuple[OptionContractRef, ...]:
    expiration = date.today() + timedelta(days=20)
    return (
        OptionContractRef(
            101,
            "101@SMART",
            "SPY",
            "SPY  C00100000",
            expiration,
            Decimal("100"),
            "C",
            "SMART",
            "SPY",
            100,
        ),
        OptionContractRef(
            102,
            "102@SMART",
            "SPY",
            "SPY  C00105000",
            expiration,
            Decimal("105"),
            "C",
            "SMART",
            "SPY",
            100,
        ),
    )


class _OptionsEvidence(_ProtocolPort):
    def __init__(self, contracts: tuple[OptionContractRef, ...]) -> None:
        super().__init__()
        self.contracts = contracts

    def resolve_contracts(self, **_: object) -> tuple[OptionContractRef, ...]:
        return self.contracts


class _BrokerSource:
    def __init__(
        self,
        contracts: tuple[OptionContractRef, ...],
        *,
        working_orders: tuple[dict[str, object], ...] = (),
    ) -> None:
        self.contracts = contracts
        self.working = working_orders
        self.quote_calls = 0

    def account_snapshot(self) -> dict[str, object]:
        return {"account": "DU123", "net_liquidation": "999999"}

    def positions(self) -> tuple[object, ...]:
        return ()

    def working_orders(self) -> tuple[dict[str, object], ...]:
        return self.working

    def unsubmitted_instructions(self) -> tuple[object, ...]:
        return ()

    def option_contract_definitions(
        self, contracts: tuple[OptionContractRef, ...]
    ) -> tuple[OptionSecDefSnapshot, ...]:
        return tuple(
            OptionSecDefSnapshot(
                item.contract_id,
                item.local_symbol,
                item.trading_class,
                item.multiplier,
                item.exchange,
                item.expiration,
                item.strike,
                item.right,
                "OPT",
                item.currency,
                True,
                False,
                "test",
            )
            for item in contracts
        )

    def option_quote_batch(
        self, contracts: tuple[OptionContractRef, ...]
    ) -> OptionQuoteBatch:
        self.quote_calls += 1
        observed = datetime.now(timezone.utc)
        quotes = tuple(
            BatchedOptionQuote(
                item.contract_id,
                f"batch-{self.quote_calls}",
                f"request-{item.contract_id}",
                observed,
                observed,
                observed,
                "test",
                Decimal("1.00"),
                Decimal("1.10"),
                exchange_time=observed,
            )
            for item in contracts
        )
        return OptionQuoteBatch(
            f"batch-{self.quote_calls}",
            QuoteBatchStatus.COMPLETE,
            observed,
            observed,
            "test",
            quotes,
        )


def _nav_snapshot(*, valid: bool = True) -> StrategyNavSnapshot:
    fields = {
        "asof": NOW,
        "strategy_nav": Decimal("2000") if valid else None,
        "strategy_deposits": Decimal("0"),
        "strategy_withdrawals": Decimal("0"),
        "realized_pnl": Decimal("0"),
        "open_position_unrealized_pnl": Decimal("0"),
        "fees": Decimal("0"),
        "signed_corrections": Decimal("0"),
        "non_strategy_contribution": Decimal("0"),
        "fill_principal_contribution": Decimal("0"),
        "observed_account_nlv": Decimal("999999"),
        "reconciliation_difference": Decimal("997999"),
        "contract_version": "v1" if valid else None,
        "contract_hash": "a" * 64 if valid else None,
        "ledger_head_hash": "b" * 64 if valid else None,
        "valid": valid,
        "no_trade_reasons": () if valid else ("MISSING_LEDGER_HEAD",),
    }
    return StrategyNavSnapshot(**fields, content_hash=canonical_hash(fields))


def _replace_nav(
    nav: StrategyNavSnapshot, **changes: object
) -> StrategyNavSnapshot:
    provisional = replace(nav, content_hash="0" * 64, **changes)
    return replace(
        provisional,
        content_hash=canonical_hash(provisional.hash_payload()),
    )


def _runtime_graph(**overrides: object) -> dict[str, object]:
    port = _ProtocolPort({})
    graph: dict[str, object] = {
        "broker_snapshot_builder": port,
        "evidence_store": port,
        "scan_run_store": port,
        "pipeline_inputs": _ProtocolPort({}),
        "universe_funnel": port,
        "broker_evidence_acquisition": _ProtocolPort({}),
        "options_evidence_acquisition": port,
        "strategy_registry": port,
        "strategy_candidate_generator": port,
        "volatility_engine": port,
        "scenario_engine": port,
        "policy_resolver": _Resolver(_policy()),
        "risk_authority_resolver": _Resolver(_authority()),
        "execution_cost_contract": port,
        "eligibility_gate": _ProtocolPort({}),
        "risk_gate": port,
        "dte_gate": port,
        "single_combination_gate": port,
        "portfolio_ranker": port,
        "ranking_store": port,
        "decision_pipeline": port,
        "strategy_nav_source": _ProtocolPort(_nav_snapshot()),
        "position_manager": port,
        "approval_store": port,
        "bridge_status_reader": port,
        "bridge_reconciliation_reader": port,
    }
    graph.update(overrides)
    pipeline_inputs = graph["pipeline_inputs"]
    if isinstance(pipeline_inputs, _ProtocolPort):
        pipeline_inputs.position_manager = graph["position_manager"]
    broker_evidence = graph["broker_evidence_acquisition"]
    if isinstance(broker_evidence, _ProtocolPort):
        broker_evidence.broker_snapshot_builder = graph["broker_snapshot_builder"]
        broker_evidence.options_evidence_acquisition = graph[
            "options_evidence_acquisition"
        ]
        broker_evidence.evidence_store = graph["evidence_store"]
        broker_evidence.policy_resolver = graph["policy_resolver"]
        broker_evidence.strategy_nav_source = graph["strategy_nav_source"]
    eligibility = graph["eligibility_gate"]
    if isinstance(eligibility, _ProtocolPort):
        eligibility.risk_gate = graph["risk_gate"]
        eligibility.dte_gate = graph["dte_gate"]
        eligibility.single_combination_gate = graph["single_combination_gate"]
    pipeline = graph["decision_pipeline"]
    if isinstance(pipeline, _ProtocolPort):
        for attribute, service_name in (
            ("inputs", "pipeline_inputs"),
            ("universe_funnel", "universe_funnel"),
            ("broker_evidence", "broker_evidence_acquisition"),
            ("strategy_registry", "strategy_registry"),
            ("strategy_generator", "strategy_candidate_generator"),
            ("volatility_engine", "volatility_engine"),
            ("scenario_engine", "scenario_engine"),
            ("policy_resolver", "policy_resolver"),
            ("risk_authority_resolver", "risk_authority_resolver"),
            ("cost_contract", "execution_cost_contract"),
            ("eligibility_gate", "eligibility_gate"),
            ("portfolio_ranker", "portfolio_ranker"),
            ("ranking_store", "ranking_store"),
        ):
            setattr(pipeline, attribute, graph[service_name])
    return graph


def _ranking_payload(
    nav: StrategyNavSnapshot,
    *,
    cutoff_at: datetime | None = None,
) -> dict[str, object]:
    cutoff_at = cutoff_at or datetime.now(timezone.utc)
    valid_until = cutoff_at + timedelta(minutes=5)
    candidate_body = _candidate_evidence_body(cutoff_at=cutoff_at)
    candidate_body.update(
        {
            "strategy_nav_hash": nav.authority_hash,
            "strategy_nav_content_hash": nav.content_hash,
            "strategy_nav_contract_hash": nav.contract_hash,
            "strategy_nav_ledger_head_hash": nav.ledger_head_hash,
            "strategy_nav_usd": str(nav.strategy_nav),
            "strategy_nav_observed_account_nlv": str(
                nav.observed_account_nlv
            ),
            "strategy_nav_reconciliation_difference": str(
                nav.reconciliation_difference
            ),
            "strategy_nav_asof": nav.asof.isoformat(),
            "risk_fraction": "0.05",
            "policy_hash": "5" * 64,
            "evidence_hashes": {"LIQUIDITY": "f" * 64},
            "exit_plan": {"time_stop": "before expiry"},
        }
    )
    proposal_legs = [
        {
            **{
                key: leg[key]
                for key in (
                    "contract_id_ex",
                    "underlying",
                    "security_type",
                    "expiration",
                    "strike",
                    "right",
                    "multiplier",
                    "currency",
                    "exchange",
                    "local_symbol",
                    "trading_class",
                    "con_id",
                    "ratio",
                    "bid",
                    "ask",
                )
            },
            "side": "BUY" if leg["side"] == "LONG" else "SELL",
            "quote_snapshot_id": candidate_body["quote_batch_id"],
            "quote_time": leg["observed_at"],
        }
        for leg in candidate_body["legs"]
    ]
    proposal_body = {
        "candidate_id": "candidate-1",
        "proposal_id": "candidate-1",
        "symbol": "SPY",
        "underlying": "SPY",
        "structure": candidate_body["structure"],
        "dte": candidate_body["dte"],
        "quote_snapshot_id": candidate_body["quote_batch_id"],
        "expected_value_usd": "10",
        "broker_snapshot_hash": candidate_body["broker_snapshot_hash"],
        "secdef_hash": candidate_body["secdef_hash"],
        "execution_cost_contract": {"version": "v1", "hash": "7" * 64},
        "policy": {"dte_exception_hash": None},
        "pricing": {
            "reference_cost_usd": "100",
            "estimated_execution_costs_usd": "0",
            "all_in_executable_cost_usd": "100",
        },
        "risk": {
            "maximum_loss_usd": "100",
            "maximum_profit_usd": "100",
            "breakevens": ["100"],
            "risk_fraction": "0.05",
        },
        "legs": proposal_legs,
    }
    manifest = build_candidate_evidence_manifest(
        candidate_body,
        after_cost_expected_value=Decimal("10"),
        cutoff_at=cutoff_at,
        ranking_valid_until=valid_until,
        now=cutoff_at,
    )
    return {
        "ranking_snapshot_id": "ranking-1",
        "scan_run_id": "scan-1",
        "snapshot_hash": "1" * 64,
        "input_hash": "2" * 64,
        "evidence_hash": "3" * 64,
        "broker_snapshot_hash": "4" * 64,
        "current_policy_version": "v1",
        "current_policy_hash": "5" * 64,
        "policy_authority_marker_hash": "6" * 64,
        "cost_version": "v1",
        "cost_hash": "7" * 64,
        "risk_contract_hash": "8" * 64,
        "risk_authority_version": "v1",
        "risk_authority_marker_hash": "9" * 64,
        "valid_until": valid_until.isoformat(),
        "candidates": [
            {
                "candidate_id": "candidate-1",
                "proposal_hash": "a" * 64,
                "rank": 1,
                "authorizable": True,
                "authority_status": "NORMAL",
                "candidate_hash": "c" * 64,
                "ranking_basis_hash": "d" * 64,
                "row_hash": "e" * 64,
                "candidate_body": candidate_body,
                "proposal_body": proposal_body,
                "score_components": {
                    "after_cost_expected_value": "10",
                },
            }
        ],
        "governance_evidence": [],
        "immutable_inputs": {
            "input_hash": "2" * 64,
            "evidence_hash": "3" * 64,
            "broker_snapshot_hash": "4" * 64,
            "candidate_evidence_manifests": {
                "candidate-1": manifest,
            },
        },
    }


@dataclass(frozen=True, slots=True)
class _GeneratedCandidate:
    candidate_id: str
    legs: tuple[dict[str, object], ...]
    candidate_hash: str
    dte: int = 20
    max_loss_usd: Decimal = Decimal("100")
    liquidity_score: Decimal = Decimal("8")
    structure: str = "DEBIT_VERTICAL"
    evidence_hashes: object = None
    execution_cost_contract_hash: str = COST_HASH
    execution_cost_contract_version: str = "v1"
    evidence_inputs: object = None
    proposal_hash: str | None = None

    @staticmethod
    def _terminal_scenarios() -> tuple[dict[str, str], ...]:
        return (
            {"terminal_underlying_price": "90", "probability": "0.5"},
            {"terminal_underlying_price": "110", "probability": "0.5"},
        )

    def hash_payload(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "symbol": "SPY",
            "structure": self.structure,
            "legs": self.legs,
            "terminal_scenarios": self._terminal_scenarios(),
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
            "strategy_nav_hash": "a" * 64,
            "strategy_nav_content_hash": "d" * 64,
            "strategy_nav_contract_hash": "b" * 64,
            "strategy_nav_ledger_head_hash": "f" * 64,
            "strategy_nav_observed_account_nlv": "999999",
            "strategy_nav_reconciliation_difference": "997999",
            "strategy_nav_asof": NOW.isoformat(),
            "broker_snapshot_hash": "6" * 64,
            "quote_batch_id": "batch-1",
            "secdef_hash": "c" * 64,
            "evidence_hashes": self.evidence_hashes or {"LIQUIDITY": "8" * 64},
            "execution_cost_contract_version": self.execution_cost_contract_version,
            "execution_cost_contract_hash": self.execution_cost_contract_hash,
            "policy_version": "v1",
            "policy_hash": POLICY_HASH,
            "dte_exception_hash": None,
            "event_evidence_status": "AVAILABLE",
            "earnings_overlap": False,
            "event_defined": False,
            "event_evidence_hash": "e" * 64,
            "event_supporting_overlap": False,
            "event_supporting_hash": None,
        }

    def proposal_payload(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.proposal.v1",
            "review_only": True,
            "rank": 1,
            "eligible_to_send": True,
            "proposal_id": self.candidate_id,
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "symbol": "SPY",
            "underlying": "SPY",
            "structure": self.structure,
            "expiration": "2026-08-20",
            "dte": self.dte,
            "quote_snapshot_id": "batch-1",
            "expected_value_usd": "10",
            "expected_value_before_costs_usd": "10",
            "terminal_scenarios": self._terminal_scenarios(),
            "broker_snapshot_hash": "6" * 64,
            "secdef_hash": "c" * 64,
            "strategy_nav": {
                "strategy_nav_usd": "1000",
                "authority_hash": "a" * 64,
                "content_hash": "d" * 64,
                "contract_hash": "b" * 64,
                "ledger_head_hash": "f" * 64,
                "observed_account_nlv": "999999",
                "reconciliation_difference": "997999",
                "asof": NOW.isoformat(),
            },
            "policy": {
                "version": "v1",
                "hash": POLICY_HASH,
                "dte_exception_hash": None,
            },
            "execution_cost_contract": {"version": "v1", "hash": COST_HASH},
            "evidence_hashes": self.evidence_hashes or {"LIQUIDITY": "8" * 64},
            "legs": self.legs,
            "pricing": {
                "reference_cost_usd": "100",
                "estimated_commissions_usd": "0",
                "estimated_slippage_usd": "0",
                "estimated_execution_costs_usd": "0",
                "all_in_executable_cost_usd": "100",
                "net_debit_usd": "100",
            },
            "risk": {
                "defined_risk": True,
                "maximum_loss_usd": str(self.max_loss_usd),
                "maximum_profit_usd": str(self.max_loss_usd * 2),
                "risk_fraction": str(self.max_loss_usd / Decimal("1000")),
                "allowed_risk_fraction": "0.10",
                "breakevens": ["100"],
            },
            "exit_plan": self.hash_payload()["exit_plan"],
        }


class _Scenario:
    def __init__(self) -> None:
        self.resolutions: list[tuple[object, object]] = []

    def evaluate_pre_cost(
        self,
        raw: object,
        *,
        resolved_policy: object,
        risk_authority: object,
        **_: object,
    ) -> dict[str, object]:
        self.resolutions.append((resolved_policy, risk_authority))
        return {
            "action": "TRADE",
            "current_policy_version": "v1",
            "current_policy_hash": POLICY_HASH,
            "policy_authority_marker_hash": POLICY_MARKER_HASH,
            "risk_authority_version": "v1",
            "risk_authority_marker_hash": _authority().marker_hash,
            "risk_contract_hash": RISK_CONTRACT_HASH,
            "result_hash": canonical_hash(raw),
            "scenarios": (
                {"terminal_price": "90", "probability": "0.5"},
                {"terminal_price": "110", "probability": "0.5"},
            ),
        }


def _policy() -> ResolvedPolicy:
    return ResolvedPolicy(
        "v1",
        POLICY_HASH,
        POLICY_MARKER_HASH,
        NOW,
        freeze_json({"policy": "test", "hard_no_trade_thresholds": {"cost_and_expectancy": {
            "minimum_after_cost_expected_value_usd": "max(5.00,0.05*maximum_loss)",
            "minimum_max_profit_to_maximum_loss": "1.20",
            "maximum_total_round_trip_cost_to_max_profit": "0.20",
            "stress_after_cost_ev": "must be greater than or equal to 0.00",
        }}}),
        freeze_json({"source": "locked"}),
    )


def _authority() -> RiskTierAuthority:
    return RiskTierAuthority.normal(RISK_CONTRACT_HASH)


@dataclass(frozen=True, slots=True)
class _SignedCostCandidate:
    policy_version: str
    policy_hash: str
    strategy_nav_hash: str
    strategy_nav_content_hash: str
    strategy_nav_contract_hash: str
    strategy_nav_ledger_head_hash: str
    strategy_nav_observed_account_nlv: Decimal
    strategy_nav_reconciliation_difference: Decimal
    strategy_nav_asof: datetime
    candidate_hash: str

    candidate_id: str = "signed-cost-candidate"

    @staticmethod
    def _legs() -> tuple[dict[str, object], ...]:
        common = {
            "underlying": "SPY",
            "security_type": "OPT",
            "expiration": "2026-08-21",
            "right": "CALL",
            "ratio": 1,
            "multiplier": "100",
            "currency": "USD",
            "exchange": "SMART",
            "observed_at": NOW.isoformat(),
        }
        return (
            {
                **common,
                "con_id": 501,
                "contract_id_ex": "501@SMART",
                "strike": "100",
                "side": "LONG",
                "bid": "2.00",
                "ask": "2.10",
            },
            {
                **common,
                "con_id": 502,
                "contract_id_ex": "502@SMART",
                "strike": "110",
                "side": "SHORT",
                "bid": "1.00",
                "ask": "1.10",
            },
        )

    @staticmethod
    def _terminal_scenarios() -> tuple[dict[str, str], ...]:
        return (
            {"terminal_underlying_price": "100", "probability": "0.50"},
            {"terminal_underlying_price": "120", "probability": "0.50"},
        )

    @staticmethod
    def _exit_plan() -> dict[str, str]:
        return {
            "thesis_invalidation": "trend reverses",
            "risk_stop": "close at 50 percent loss",
            "profit_take": "close at 50 percent gain",
            "time_stop": "close before expiry",
            "maximum_holding_date": "2026-08-20",
            "bad_quote_action": "do not trade",
        }

    @staticmethod
    def _evidence_hashes() -> dict[str, str]:
        return {
            "MARKET": "a" * 64,
            "VOLATILITY": "b" * 64,
            "LIQUIDITY": "c" * 64,
        }

    def hash_payload(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "symbol": "SPY",
            "structure": "DEBIT_VERTICAL",
            "legs": self._legs(),
            "terminal_scenarios": self._terminal_scenarios(),
            "estimated_commissions_usd": "5.00",
            "estimated_slippage_usd": "15.00",
            "debit_usd": "210.00",
            "credit_usd": "100.00",
            "all_in_cost_usd": "130.00",
            "max_loss_usd": "130.00",
            "max_profit_usd": "870.00",
            "breakevens": ("101.30",),
            "liquidity_score": "0.80",
            "exit_plan": self._exit_plan(),
            "dte": 16,
            "strategy_nav_usd": "1300.00",
            "strategy_nav_hash": self.strategy_nav_hash,
            "strategy_nav_content_hash": self.strategy_nav_content_hash,
            "strategy_nav_contract_hash": self.strategy_nav_contract_hash,
            "strategy_nav_ledger_head_hash": self.strategy_nav_ledger_head_hash,
            "strategy_nav_observed_account_nlv": str(
                self.strategy_nav_observed_account_nlv
            ),
            "strategy_nav_reconciliation_difference": str(
                self.strategy_nav_reconciliation_difference
            ),
            "strategy_nav_asof": self.strategy_nav_asof.isoformat(),
            "broker_snapshot_hash": "6" * 64,
            "quote_batch_id": "signed-cost-batch",
            "secdef_hash": "d" * 64,
            "evidence_hashes": self._evidence_hashes(),
            "execution_cost_contract_version": "v1",
            "execution_cost_contract_hash": EXECUTION_COST_HASH,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "dte_exception_hash": None,
            "event_evidence_status": "AVAILABLE",
            "earnings_overlap": False,
            "event_defined": False,
            "event_evidence_hash": "e" * 64,
            "event_supporting_overlap": False,
            "event_supporting_hash": None,
        }

    def proposal_payload(self) -> dict[str, object]:
        proposal_legs = []
        for leg in self._legs():
            row = dict(leg)
            row["side"] = "BUY" if leg["side"] == "LONG" else "SELL"
            row["quantity"] = leg["ratio"]
            row["quote_time"] = row.pop("observed_at")
            row["quote_snapshot_id"] = "signed-cost-batch"
            proposal_legs.append(row)
        return {
            "schema": "options_copilot.proposal.v1",
            "review_only": True,
            "rank": 1,
            "eligible_to_send": True,
            "proposal_id": self.candidate_id,
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "symbol": "SPY",
            "underlying": "SPY",
            "structure": "DEBIT_VERTICAL",
            "expiration": "2026-08-21",
            "dte": 16,
            "quote_snapshot_id": "signed-cost-batch",
            "expected_value_usd": "370.00",
            "expected_value_before_costs_usd": "390.00",
            "terminal_scenarios": self._terminal_scenarios(),
            "broker_snapshot_hash": "6" * 64,
            "secdef_hash": "d" * 64,
            "strategy_nav": {
                "strategy_nav_usd": "1300.00",
                "authority_hash": self.strategy_nav_hash,
                "content_hash": self.strategy_nav_content_hash,
                "contract_hash": self.strategy_nav_contract_hash,
                "ledger_head_hash": self.strategy_nav_ledger_head_hash,
                "observed_account_nlv": str(
                    self.strategy_nav_observed_account_nlv
                ),
                "reconciliation_difference": str(
                    self.strategy_nav_reconciliation_difference
                ),
                "asof": self.strategy_nav_asof.isoformat().replace(
                    "+00:00",
                    "Z",
                ),
            },
            "policy": {
                "version": self.policy_version,
                "hash": self.policy_hash,
                "dte_exception_hash": None,
            },
            "execution_cost_contract": {
                "version": "v1",
                "hash": EXECUTION_COST_HASH,
            },
            "evidence_hashes": self._evidence_hashes(),
            "legs": proposal_legs,
            "pricing": {
                "reference_cost_usd": "110.00",
                "estimated_commissions_usd": "5.00",
                "estimated_slippage_usd": "15.00",
                "estimated_execution_costs_usd": "20.00",
                "all_in_executable_cost_usd": "130.00",
                "net_debit_usd": "110.00",
            },
            "risk": {
                "defined_risk": True,
                "maximum_loss_usd": "130.00",
                "maximum_profit_usd": "870.00",
                "risk_fraction": "0.10",
                "breakevens": ["101.30"],
            },
            "exit_plan": self._exit_plan(),
        }


class _SignedCostScenario:
    def __init__(self, policy: object, authority: object) -> None:
        self.policy = policy
        self.authority = authority

    def evaluate_pre_cost(
        self,
        raw: object,
        *,
        resolved_policy: object,
        risk_authority: object,
        **_: object,
    ) -> dict[str, object]:
        assert resolved_policy == self.policy
        assert risk_authority == self.authority
        return {
            "candidate_id": "signed-cost-candidate",
            "action": "TRADE",
            "current_policy_version": getattr(
                resolved_policy, "current_policy_version"
            ),
            "current_policy_hash": getattr(resolved_policy, "current_policy_hash"),
            "policy_authority_marker_hash": getattr(
                resolved_policy, "policy_authority_marker_hash"
            ),
            "risk_authority_version": getattr(risk_authority, "version"),
            "risk_authority_marker_hash": getattr(risk_authority, "marker_hash"),
            "risk_contract_hash": getattr(risk_authority, "risk_contract_hash"),
            "cost_version": "v1",
            "cost_hash": EXECUTION_COST_HASH,
            "result_hash": canonical_hash(raw),
            "scenarios": (
                {"terminal_price": "100", "probability": "0.50"},
                {"terminal_price": "120", "probability": "0.50"},
            ),
        }


def _signed_cost_candidate(
    policy: object, nav: StrategyNavSnapshot
) -> _SignedCostCandidate:
    provisional = _SignedCostCandidate(
        policy_version=getattr(policy, "current_policy_version"),
        policy_hash=getattr(policy, "current_policy_hash"),
        strategy_nav_hash=nav.authority_hash,
        strategy_nav_content_hash=nav.content_hash,
        strategy_nav_contract_hash=str(nav.contract_hash),
        strategy_nav_ledger_head_hash=str(nav.ledger_head_hash),
        strategy_nav_observed_account_nlv=Decimal(nav.observed_account_nlv),
        strategy_nav_reconciliation_difference=Decimal(
            nav.reconciliation_difference
        ),
        strategy_nav_asof=nav.asof,
        candidate_hash="0" * 64,
    )
    return replace(
        provisional,
        candidate_hash=canonical_hash(provisional.hash_payload()),
    )


def _candidate(**changes: object) -> _GeneratedCandidate:
    candidate_id = str(changes.pop("candidate_id", "candidate-1"))
    legs = changes.pop(
        "legs",
        (
            {"con_id": 101, "side": "LONG", "ratio": 1, "bid": "2.00", "ask": "2.10", "observed_at": NOW.isoformat()},
            {"con_id": 102, "side": "SHORT", "ratio": 1, "bid": "1.00", "ask": "1.10", "observed_at": NOW.isoformat()},
        ),
    )
    provisional = _GeneratedCandidate(
        candidate_id=candidate_id,
        legs=legs,
        candidate_hash="0" * 64,
        **changes,
    )
    return replace(provisional, candidate_hash=canonical_hash(provisional.hash_payload()))


def _pipeline(
    tmp_path,
    *,
    policy_resolver: object | None,
    risk_resolver: object | None,
    candidate: _GeneratedCandidate | None = None,
    candidates: tuple[_GeneratedCandidate, ...] | None = None,
    gate: Mapping[str, object] | None = None,
):
    if candidate is not None and candidates is not None:
        raise ValueError("candidate and candidates are mutually exclusive")
    generated_candidates = candidates or (candidate or _candidate(),)
    scenario = _Scenario()
    store = RankingStore(tmp_path / "ranking.sqlite")
    pipeline = DecisionPipeline(
        inputs=_Port({"universe": {}, "positions": ()}),
        universe_funnel=_Port({"finalists": generated_candidates}),
        broker_evidence=_Port(
            {
                "snapshot_hash": "6" * 64,
                "evidence_hash": "7" * 64,
                "spot": Decimal("100"),
                "atm_iv": Decimal("0.2"),
                "nav_snapshot": {
                    "valid": True,
                    "authority_hash": "a" * 64,
                    "content_hash": "d" * 64,
                    "contract_hash": "b" * 64,
                    "ledger_head_hash": "f" * 64,
                    "observed_account_nlv": "999999",
                    "reconciliation_difference": "997999",
                    "asof": NOW.isoformat(),
                },
            }
        ),
        strategy_registry=_Port({"finalists": generated_candidates}),
        strategy_generator=_Port({"candidates": generated_candidates}),
        volatility_engine=_Port({"eligible": True, "evidence_hash": "9" * 64}),
        scenario_engine=scenario,
        policy_resolver=policy_resolver,
        risk_authority_resolver=risk_resolver,
        cost_contract=_Port(
            {
                "version": "v1",
                "hash": COST_HASH,
                "candidates": tuple(
                    {
                        "candidate_id": item.candidate_id,
                        "cost_hash": COST_HASH,
                        "cost_version": "v1",
                        "after_cost_expected_value": Decimal("10"),
                        "execution_cost_usd": Decimal("0"),
                        "stress_after_cost_expected_value": Decimal("10"),
                        "calculation_hash": "d" * 64,
                    }
                    for item in generated_candidates
                ),
            }
        ),
        eligibility_gate=_Port(
            gate
            if gate is not None
            else {"eligible": True, "risk_fraction": Decimal("0.10")}
        ),
        portfolio_ranker=PortfolioRanker(),
        ranking_store=store,
        clock=lambda: NOW,
    )
    return pipeline, scenario, store


def _signed_cost_runtime(
    tmp_path: Path,
    *,
    cost_resolver: SignedExecutionCostResolver,
) -> tuple[RuntimeServices, RankingStore, object]:
    policy_resolver = InitialPolicyResolver()
    policy = policy_resolver.resolve(now=NOW)
    risk_resolver = CurrentRiskAuthorityResolver(
        "e" * 64,
        clock=lambda: NOW,
    )
    authority = risk_resolver.resolve(now=NOW, current_policy=policy)
    nav = _replace_nav(
        _nav_snapshot(),
        strategy_nav=Decimal("1300"),
        reconciliation_difference=Decimal("998699"),
    )
    candidate = _signed_cost_candidate(policy, nav)

    inputs = _ProtocolPort({"universe": {}, "positions": ()})
    universe = _ProtocolPort({"finalists": (candidate,)})
    broker_evidence = _ProtocolPort(
        {
            "snapshot_hash": "6" * 64,
            "evidence_hash": "7" * 64,
            "spot": Decimal("105"),
            "atm_iv": Decimal("0.20"),
            "nav_snapshot": nav,
        }
    )
    registry = _ProtocolPort({"finalists": (candidate,)})
    generator = _ProtocolPort({"candidates": (candidate,)})
    volatility = _ProtocolPort(
        {"eligible": True, "evidence_hash": "9" * 64}
    )
    scenario = _SignedCostScenario(policy, authority)
    eligibility = _ProtocolPort(
        {"eligible": True, "risk_fraction": Decimal("0.10")}
    )
    ranker = PortfolioRanker()
    store = RankingStore(tmp_path / "signed-cost-ranking.sqlite")
    pipeline = DecisionPipeline(
        inputs=inputs,
        universe_funnel=universe,
        broker_evidence=broker_evidence,
        strategy_registry=registry,
        strategy_generator=generator,
        volatility_engine=volatility,
        scenario_engine=scenario,
        policy_resolver=policy_resolver,
        risk_authority_resolver=risk_resolver,
        cost_contract=cost_resolver,
        eligibility_gate=eligibility,
        portfolio_ranker=ranker,
        ranking_store=store,
        clock=lambda: NOW,
    )

    broker_builder = _ProtocolPort({})
    evidence_store = _ProtocolPort({})
    options_evidence = _ProtocolPort(())
    nav_source = _ProtocolPort(nav)
    position_manager = _ProtocolPort({})
    risk_gate = _ProtocolPort({})
    dte_gate = _ProtocolPort({})
    single_combination_gate = _ProtocolPort({})
    approval_store = _ProtocolPort({})
    bridge = _ProtocolPort({})
    inputs.position_manager = position_manager
    broker_evidence.broker_snapshot_builder = broker_builder
    broker_evidence.options_evidence_acquisition = options_evidence
    broker_evidence.evidence_store = evidence_store
    broker_evidence.policy_resolver = policy_resolver
    broker_evidence.strategy_nav_source = nav_source
    eligibility.risk_gate = risk_gate
    eligibility.dte_gate = dte_gate
    eligibility.single_combination_gate = single_combination_gate

    services = RuntimeServices(
        broker_snapshot_builder=broker_builder,
        evidence_store=evidence_store,
        scan_run_store=_ProtocolPort({}),
        pipeline_inputs=inputs,
        universe_funnel=universe,
        broker_evidence_acquisition=broker_evidence,
        options_evidence_acquisition=options_evidence,
        strategy_registry=registry,
        strategy_candidate_generator=generator,
        volatility_engine=volatility,
        scenario_engine=scenario,
        policy_resolver=policy_resolver,
        risk_authority_resolver=risk_resolver,
        execution_cost_contract=cost_resolver,
        eligibility_gate=eligibility,
        risk_gate=risk_gate,
        dte_gate=dte_gate,
        single_combination_gate=single_combination_gate,
        portfolio_ranker=ranker,
        ranking_store=store,
        decision_pipeline=pipeline,
        strategy_nav_source=nav_source,
        position_manager=position_manager,
        approval_store=approval_store,
        bridge_status_reader=bridge,
        bridge_reconciliation_reader=bridge,
    )
    return services, store, policy


def test_pipeline_resolves_each_authority_once_and_persists_identical_bindings(tmp_path) -> None:
    policy_resolver, risk_resolver = _Resolver(_policy()), _Resolver(_authority())
    pipeline, scenario, store = _pipeline(
        tmp_path,
        policy_resolver=policy_resolver,
        risk_resolver=risk_resolver,
    )
    try:
        result = pipeline.run_slot("scan-1", NOW)
        assert result["status"] == "TRADE"
        assert policy_resolver.resolve_calls == risk_resolver.resolve_calls == 1
        assert scenario.resolutions == [(_policy(), _authority())]
        assert result["current_policy_hash"] == POLICY_HASH
        assert result["policy_authority_marker_hash"] == POLICY_MARKER_HASH
        assert result["risk_authority_marker_hash"] == _authority().marker_hash
        stored = store.read_snapshot(str(result["ranking_snapshot_id"]))
        assert stored["current_policy_hash"] == result["current_policy_hash"]
        assert stored["risk_authority_marker_hash"] == result["risk_authority_marker_hash"]
        assert stored["candidates"][0]["ranking_basis_hash"]
        assert [row.record_type for row in store.read_decisions("scan-1")] == [
            "SCENARIO",
            "GATE_BUNDLE_TRADE",
            "TRADE",
        ]
    finally:
        store.close()


def test_pipeline_binds_gate_risk_fraction_by_candidate_id(tmp_path) -> None:
    candidates = (
        _candidate(candidate_id="candidate-1", max_loss_usd=Decimal("100")),
        _candidate(candidate_id="candidate-2", max_loss_usd=Decimal("50")),
    )
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=_Resolver(_policy()),
        risk_resolver=_Resolver(_authority()),
        candidates=candidates,
        gate={
            "eligible": True,
            "risk_fractions": {
                "candidate-1": Decimal("0.10"),
                "candidate-2": Decimal("0.05"),
            },
        },
    )
    try:
        result = pipeline.run_slot("scan-risk-fractions", NOW)

        assert result["status"] == "TRADE"
        assert result["ranking_snapshot_id"] is not None
        assert store.record_counts()["ranking_rows"] > 0
    finally:
        store.close()


@pytest.mark.parametrize(
    "gate",
    (
        {"eligible": True},
        {
            "eligible": True,
            "risk_fractions": {"candidate-1": Decimal("0.10")},
        },
        {
            "eligible": True,
            "risk_fractions": {
                "candidate-1": Decimal("0.10"),
                "candidate-2": Decimal("0.05"),
                "unexpected": Decimal("0.01"),
            },
        },
        {"eligible": True, "risk_fractions": ()},
        {
            "eligible": True,
            "risk_fractions": {
                "candidate-1": Decimal("0.10"),
                "candidate-2": "NaN",
            },
        },
        {
            "eligible": True,
            "risk_fractions": {
                "candidate-1": Decimal("0.10"),
                "candidate-2": Decimal("-0.05"),
            },
        },
        {
            "eligible": True,
            "risk_fractions": {
                "candidate-1": Decimal("0.10"),
                "candidate-2": Decimal("0.06"),
            },
        },
        {"eligible": True, "risk_fraction": Decimal("0.10")},
    ),
    ids=(
        "missing-all",
        "missing-candidate",
        "extra-candidate",
        "not-a-mapping",
        "nonfinite",
        "negative",
        "mismatch",
        "legacy-scalar-cannot-cover-unlike-candidates",
    ),
)
def test_pipeline_rejects_unbound_per_candidate_gate_risk(
    tmp_path, gate: Mapping[str, object]
) -> None:
    candidates = (
        _candidate(candidate_id="candidate-1", max_loss_usd=Decimal("100")),
        _candidate(candidate_id="candidate-2", max_loss_usd=Decimal("50")),
    )
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=_Resolver(_policy()),
        risk_resolver=_Resolver(_authority()),
        candidates=candidates,
        gate=gate,
    )
    try:
        result = pipeline.run_slot("scan-invalid-risk-fractions", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("GATE_RISK_BINDING_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.record_counts()["ranking_rows"] == 0
    finally:
        store.close()


def test_concurrent_authority_head_change_leaves_no_approvable_row(tmp_path) -> None:
    policy_resolver, risk_resolver = _Resolver(_policy()), _Resolver(_authority())
    policy_resolver.current = False
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=policy_resolver,
        risk_resolver=risk_resolver,
    )
    try:
        result = pipeline.run_slot("scan-stale", NOW)
        assert result["status"] == "NO_TRADE"
        assert "AUTHORITY_HEAD_CHANGED" in result["reasons"]
        assert result["ranking_snapshot_id"] is None
        assert store.record_counts()["ranking_rows"] == 0
    finally:
        store.close()


@pytest.mark.parametrize(
    ("policy_resolver", "risk_resolver", "expected_reasons"),
    (
        (None, _Resolver(_authority()), {"POLICY_RESOLVER_REQUIRED"}),
        (_Resolver(_policy()), None, {"RISK_AUTHORITY_RESOLVER_REQUIRED"}),
        (None, None, {"POLICY_RESOLVER_REQUIRED", "RISK_AUTHORITY_RESOLVER_REQUIRED"}),
    ),
)
def test_missing_authority_resolver_is_no_trade_with_zero_ranking_rows(
    tmp_path,
    policy_resolver,
    risk_resolver,
    expected_reasons,
) -> None:
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=policy_resolver,
        risk_resolver=risk_resolver,
    )
    try:
        result = pipeline.run_slot("scan-missing", NOW)
        assert result["status"] == "NO_TRADE"
        assert expected_reasons <= set(result["reasons"])
        assert result["ranking_snapshot_id"] is None
        counts = store.record_counts()
        assert counts["ranking_rows"] == 0
        assert counts["ranking_decisions"] == 2
        assert [row.record_type for row in store.read_decisions("scan-missing")] == [
            "GATE_BUNDLE_NO_TRADE",
            "NO_TRADE"
        ]
    finally:
        store.close()


def test_resolve_only_dynamic_resolver_cannot_establish_current_head(tmp_path) -> None:
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=_ResolveOnly(_policy()),
        risk_resolver=_Resolver(_authority()),
    )
    try:
        result = pipeline.run_slot("scan-resolve-only", NOW)
        assert result["status"] == "NO_TRADE"
        assert "AUTHORITY_HEAD_CHANGED" in result["reasons"]
        assert store.record_counts()["ranking_rows"] == 0
    finally:
        store.close()


def test_reused_candidate_hash_after_leg_quote_tamper_is_rejected(tmp_path) -> None:
    original = _candidate()
    tampered_legs = [dict(item) for item in original.legs]
    tampered_legs[0]["ask"] = "9.99"
    tampered = replace(original, legs=tuple(tampered_legs))
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=_Resolver(_policy()),
        risk_resolver=_Resolver(_authority()),
        candidate=tampered,
    )
    try:
        result = pipeline.run_slot("scan-tampered", NOW)
        assert result["status"] == "NO_TRADE"
        assert "CANDIDATE_HASH_BODY_MISMATCH" in result["reasons"]
        assert store.record_counts()["ranking_rows"] == 0
    finally:
        store.close()


def test_candidate_evidence_cannot_override_pipeline_evidence(tmp_path) -> None:
    candidate = _candidate(evidence_inputs={"broker_snapshot_hash": "f" * 64})
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=_Resolver(_policy()),
        risk_resolver=_Resolver(_authority()),
        candidate=candidate,
    )
    try:
        result = pipeline.run_slot("scan-evidence-override", NOW)
        assert result["status"] == "NO_TRADE"
        assert "CANDIDATE_EVIDENCE_MISMATCH" in result["reasons"]
        assert store.record_counts()["ranking_rows"] == 0
        assert [row.record_type for row in store.read_decisions("scan-evidence-override")] == [
            "SCENARIO",
            "GATE_BUNDLE_NO_TRADE",
            "NO_TRADE",
        ]
    finally:
        store.close()


def test_supplied_proposal_hash_must_match_frozen_proposal_body(tmp_path) -> None:
    candidate = _candidate(proposal_hash="f" * 64)
    pipeline, _, store = _pipeline(
        tmp_path,
        policy_resolver=_Resolver(_policy()),
        risk_resolver=_Resolver(_authority()),
        candidate=candidate,
    )
    try:
        result = pipeline.run_slot("scan-proposal-hash-mismatch", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("PROPOSAL_HASH_BODY_MISMATCH",)
        assert result["ranking_snapshot_id"] is None
        assert store.record_counts()["ranking_rows"] == 0
        assert [
            row.record_type
            for row in store.read_decisions("scan-proposal-hash-mismatch")
        ] == ["GATE_BUNDLE_NO_TRADE", "NO_TRADE"]
    finally:
        store.close()


def test_default_fail_closed_runtime_uses_the_real_signed_cost_resolver() -> None:
    services = _unavailable_runtime_services(
        approval_store=_ProtocolPort({}),
        bridge_reader=_ProtocolPort({}),
    )

    assert isinstance(
        services.execution_cost_contract, SignedExecutionCostResolver
    )
    assert "execution_cost_contract" not in services.missing_dependencies
    assert "execution_cost_contract" not in services.invalid_dependencies
    assert services.approval_enabled is False
    assert services.readiness()["decision"] == "NO_TRADE"
    resolution = services.execution_cost_contract.resolve(now=NOW)
    assert resolution.cost_hash == EXECUTION_COST_HASH
    assert services.execution_cost_contract.is_current(resolution) is True


def test_missing_current_policy_contract_stops_before_market_data_and_generator(
    tmp_path: Path,
) -> None:
    policy = _policy()

    class RawPayloadPolicyResolver(_Resolver):
        def __init__(self) -> None:
            super().__init__(policy)
            self.contract_resolution = None

        def policy_contract_document(self, resolution: object) -> None:
            self.contract_resolution = resolution
            return None

    class ForbiddenPacing:
        @property
        def ready(self) -> bool:
            raise AssertionError("market data must not be touched")

    class CountingGenerator:
        def __init__(self) -> None:
            self.calls = 0

        def generate(self, **_: object) -> Mapping[str, object]:
            self.calls += 1
            return {"candidates": ()}

    policy_resolver = RawPayloadPolicyResolver()
    broker_evidence = ProductionBrokerEvidenceAcquisition(
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        ForbiddenPacing(),  # type: ignore[arg-type]
        object(),
        execution_cost_contract={},
        policy_resolver=policy_resolver,
    )
    generator = CountingGenerator()
    store = RankingStore(tmp_path / "policy-contract-unavailable.sqlite3")
    pipeline = DecisionPipeline(
        inputs=_Port({"universe": {}, "positions": ()}),
        universe_funnel=_Port({"finalists": (_candidate(),)}),
        broker_evidence=broker_evidence,
        strategy_registry=_Port({}),
        strategy_generator=generator,
        volatility_engine=_Port({}),
        scenario_engine=_Port({}),
        policy_resolver=policy_resolver,
        risk_authority_resolver=_Resolver(_authority()),
        cost_contract=_Port({}),
        eligibility_gate=_Port({}),
        portfolio_ranker=_Port({}),
        ranking_store=store,
        clock=lambda: NOW,
    )
    try:
        result = pipeline.run_slot("scan-raw-promoted-policy", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == (
            "CURRENT_POLICY_CONTRACT_UNAVAILABLE",
        )
        assert policy_resolver.contract_resolution is policy
        assert generator.calls == 0
        assert store.record_counts()["ranking_rows"] == 0
    finally:
        store.close()


def test_production_composition_uses_durable_fail_closed_p9_authorities(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    config.ensure_runtime_directories()
    approvals = ProposalApprovalStore(config.data_dir / "approvals.sqlite3")
    bridge = CodexBridgeStore(
        config.data_dir / "codex_bridge.sqlite3",
        approvals,
    )
    composition = build_production_composition(
        config,
        approval_store=approvals,
        bridge_reader=bridge,
        clock=lambda: NOW,
    )
    try:
        services = composition.services
        policy_resolver = services.policy_resolver
        assert isinstance(policy_resolver, CurrentPolicyResolver)
        assert policy_resolver.ledger.path == (
            config.data_dir / "governance" / "policy_authority.sqlite3"
        )
        assert policy_resolver.ledger.journal_mode == "wal"
        assert policy_resolver.ledger.synchronous == "full"
        assert policy_resolver.ledger.signature_verifier is None
        assert services.scenario_engine.resolver is policy_resolver
        assert services.decision_pipeline.policy_resolver is policy_resolver
        assert (
            services.broker_evidence_acquisition.policy_resolver
            is policy_resolver
        )
        assert not hasattr(
            services.broker_evidence_acquisition,
            "policy_contract",
        )

        policy = policy_resolver.resolve(now=NOW)
        risk_resolver = services.risk_authority_resolver
        assert isinstance(risk_resolver, CurrentRiskAuthorityResolver)
        assert isinstance(
            risk_resolver.marker_source,
            PolicyLedgerRiskAuthorityMarkerSource,
        )
        assert risk_resolver.marker_source.ledger is policy_resolver.ledger
        assert risk_resolver.signature_verifier is None
        assert risk_resolver.expected_evaluation_report_hash is None
        assert risk_resolver.expected_reference_dataset_hash is None
        assert risk_resolver.expected_independence_hash is None
        authority = risk_resolver.resolve(now=NOW, current_policy=policy)
        assert authority.tier.value == "NORMAL"
        assert authority.a_grade_approved is False
        assert authority.allows_risk_fraction(Decimal("0.10")) is True
        assert authority.allows_risk_fraction(Decimal("0.1001")) is False
        assert authority.allows_risk_fraction(Decimal("0.15")) is False
        assert authority.allows_risk_fraction(Decimal("0.20")) is False

        callback_calls = 0

        guarded_result = object()

        def commit_under_shared_authority_lease() -> object:
            nonlocal callback_calls
            callback_calls += 1
            return guarded_result

        assert risk_resolver.guard_current(
            authority,
            callback=commit_under_shared_authority_lease,
        ) is guarded_result
        assert callback_calls == 1
        assert services.approval_enabled is False
        assert services.approval_blockers == (
            "CREATOR_TRANSPORT_UNAVAILABLE",
        )
        assert services.readiness()["direct_order_submission"] is False
    finally:
        composition.lifecycle.close()
        bridge.close()
        approvals.close()


def test_learning_status_projects_verified_read_only_p9_governance_and_degrades(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = OptionsCopilotRuntime(
        OptionsCopilotConfig(
            data_dir=tmp_path / "runtime-data",
            log_dir=tmp_path / "runtime-logs",
        )
    )
    try:
        status = runtime.learning_status()
        governance = status["governance"]
        assert status["champion"] == BASELINE_MODEL_VERSION
        assert status["automatic_production_promotion"] is False
        assert status["a_grade_unlocked"] is False
        assert status["creator_transport_status"] == (
            "CREATOR_TRANSPORT_UNAVAILABLE"
        )
        assert governance["schema"] == "options_copilot.learning.governance.v1"
        assert governance["status"] == "BLOCKED"
        assert governance["reason"] == "NO_TRUSTED_HUMAN_SIGNER"

        current = governance["current_policy"]
        assert current["status"] == "VERIFIED"
        assert current["reason"] is None
        assert current["version"] == "v1"
        assert len(current["hash"]) == 64
        assert len(current["authority_marker_hash"]) == 64
        assert len(current["authority_head_hash"]) == 64
        assert len(current["immutable_initial_policy_hash"]) == 64

        evaluation = governance["evaluation"]
        assert evaluation == {
            "status": "COLLECTING",
            "reason": "ZERO_INDEPENDENT_SAMPLES",
            "report_hash": evaluation["report_hash"],
            "dataset_hash": evaluation["dataset_hash"],
            "independence_spec_hash": evaluation["independence_spec_hash"],
                "independent_count": 0,
                "stage": "COLLECTING",
                "comparison_complete": False,
                "champion_accuracy": None,
                "challenger_accuracy": None,
                "challenger_accuracy_delta": None,
                "champion_brier_score": None,
                "challenger_brier_score": None,
                "challenger_brier_improvement": None,
            }
        assert all(
            len(str(evaluation[field])) == 64
            for field in (
                "report_hash",
                "dataset_hash",
                "independence_spec_hash",
            )
        )
        for field in ("promotion", "rollback", "a_grade"):
            assert governance[field]["status"] == "BLOCKED"
            assert governance[field]["reason"] == "NO_TRUSTED_HUMAN_SIGNER"
        authority = governance["authority"]
        assert authority["human_signer_status"] == "NO_TRUSTED_HUMAN_SIGNER"
        assert authority["read_only"] is True
        assert set(authority.values()).issubset(
            {False, True, "NO_TRUSTED_HUMAN_SIGNER"}
        )
        assert all(
            value is False
            for key, value in authority.items()
            if key not in {"human_signer_status", "read_only"}
        )
        risk = governance["risk"]
        assert risk["tier"] == "NORMAL"
        assert risk["normal_max_fraction"] == "0.10"
        assert risk["a_grade_max_fraction"] == "0.15"
        assert risk["absolute_reject_fraction"] == "0.20"
        assert risk["authority_version"] == "v1"
        assert len(risk["authority_marker_hash"]) == 64
        rendered = json.dumps(status, sort_keys=True)
        for private_field in (
            "governance_signature",
            "signature_algorithm",
            "signer_key_id",
            "raw_authority",
        ):
            assert private_field not in rendered

        composition = runtime.production_composition
        assert composition is not None
        resolver = composition.services.policy_resolver
        assert isinstance(resolver, CurrentPolicyResolver)

        def tampered_policy(*, now: datetime) -> object:
            del now
            raise PolicyAuthorityTampered("raw ledger detail must stay private")

        monkeypatch.setattr(resolver, "resolve", tampered_policy)
        degraded = runtime.learning_status()["governance"]["current_policy"]
        assert degraded == {
            "status": "UNAVAILABLE",
            "reason": "CURRENT_POLICY_TAMPERED",
            "version": None,
            "hash": None,
            "authority_marker_hash": None,
            "authority_head_hash": None,
            "immutable_initial_policy_hash": None,
        }
    finally:
        runtime.close()


def test_complete_runtime_pipeline_uses_signed_cost_and_persists_recomputed_ev(
    tmp_path: Path,
) -> None:
    cost_resolver = SignedExecutionCostResolver(clock=lambda: NOW)
    services, store, policy = _signed_cost_runtime(
        tmp_path,
        cost_resolver=cost_resolver,
    )
    try:
        assert services.readiness()["status"] == "READY"
        assert services.execution_cost_contract is cost_resolver
        assert services.decision_pipeline.cost_contract is cost_resolver

        result = services.run_slot("scan-signed-cost", NOW)

        assert result["status"] == "TRADE"
        assert result["cost_version"] == "v1"
        assert result["cost_hash"] == EXECUTION_COST_HASH
        assert result["current_policy_hash"] == getattr(
            policy, "current_policy_hash"
        )
        stored = store.read_snapshot(str(result["ranking_snapshot_id"]))
        assert stored["cost_hash"] == EXECUTION_COST_HASH
        assert stored["current_policy_hash"] == getattr(
            policy, "current_policy_hash"
        )
        stored_candidate = stored["candidates"][0]
        assert Decimal(
            stored_candidate["score_components"][
                "after_cost_expected_value"
            ]["$decimal"]
        ) == Decimal("370")
        assert stored_candidate["proposal_body"]["expected_value_usd"] == "370.00"
        assert stored_candidate["proposal_body"][
            "expected_value_before_costs_usd"
        ] == "390.00"

        identity = cost_resolver.resolve(
            now=NOW,
            current_policy=policy,
            resolved_policy=policy,
        )
        assert identity.candidates == ()
        assert cost_resolver.is_current(identity) is True
    finally:
        store.close()


def test_signed_cost_head_change_before_persistence_is_no_trade(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "execution-cost-head.json"
    checked_in_path = SignedExecutionCostResolver().contract_path
    contract_path.write_text(
        checked_in_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    class _HeadReplacingResolver(SignedExecutionCostResolver):
        def __init__(self) -> None:
            super().__init__(contract_path, clock=lambda: NOW)
            self.last_resolution = None

        def resolve(self, **kwargs: object):
            resolution = super().resolve(**kwargs)
            self.last_resolution = resolution
            prior = load_contract(self.contract_path)
            correction = create_correction(
                prior,
                version="v2",
                effective_at=NOW - timedelta(minutes=2),
                provenance=prior.provenance,
                payload=prior.payload,
                actor=prior.actor,
                signed_at=NOW - timedelta(minutes=1),
            )
            self.contract_path.write_text(
                json.dumps(correction.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return resolution

    cost_resolver = _HeadReplacingResolver()
    services, store, _ = _signed_cost_runtime(
        tmp_path,
        cost_resolver=cost_resolver,
    )
    try:
        result = services.run_slot("scan-cost-head-change", NOW)

        assert result["status"] == "NO_TRADE"
        assert result["reasons"] == ("EXECUTION_COST_HEAD_CHANGED",)
        assert result["ranking_snapshot_id"] is None
        assert store.record_counts()["ranking_rows"] == 0
        assert cost_resolver.last_resolution is not None
        assert cost_resolver.is_current(cost_resolver.last_resolution) is False
    finally:
        store.close()


def test_static_authority_contract_keeps_advisory_out_of_production_ports(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )

    assert creator_unavailable_reason() == "CREATOR_TRANSPORT_UNAVAILABLE"
    assert config.ibkr_readonly is True
    assert config.live_instruction_enabled is False
    assert config.normal_risk_fraction == 0.10
    assert config.a_grade_risk_fraction == 0.15
    assert config.hard_risk_fraction == 0.20
    assert "readonly=True" in inspect.getsource(
        IBKRReadOnlyGateway._connect_on_owner
    )

    forbidden_order_writes = {
        "place_order",
        "submit_order",
        "transmit_order",
        "modify_order",
        "cancel_order",
    }
    for authority_boundary in (
        IBKRReadOnlyGateway,
        DecisionPipeline,
        RuntimeServices,
    ):
        callables = {
            name
            for name, member in inspect.getmembers(
                authority_boundary,
                predicate=callable,
            )
        }
        assert forbidden_order_writes.isdisjoint(callables)

    pipeline_ports = set(inspect.signature(DecisionPipeline).parameters)
    assert not any(
        forbidden in port
        for port in pipeline_ports
        for forbidden in ("advisory", "model", "provider")
    )
    assert not any(
        "advisory" in port
        for port in inspect.signature(RuntimeServices).parameters
    )


def test_zero_influence_runtime_exposes_only_supporting_projection_ports() -> None:
    api_ports = set(inspect.signature(OptionsCopilotServices).parameters)
    required_projection_ports = {
        "advisory_provider",
        "source_evidence_provider",
    }
    missing_projection_ports = required_projection_ports - api_ports
    assert not missing_projection_ports, (
        "PHASE2_EXPECTED_RED:ZERO_INFLUENCE "
        "OptionsCopilotServices is missing supporting-only projection ports: "
        f"{sorted(missing_projection_ports)}"
    )

    service_source = "".join(
        inspect.getsource(OptionsCopilotRuntime.services).split()
    )
    assert "advisory_provider=self.news.advisory_payload" in service_source
    assert (
        "source_evidence_provider=self.news.source_evidence_payload"
        in service_source
    )
    assert not any(
        "advisory" in port
        for port in inspect.signature(DecisionPipeline).parameters
    )
    assert not any(
        "advisory" in port
        for port in inspect.signature(RuntimeServices).parameters
    )


def test_runtime_services_names_complete_graph_and_missing_dependency_disables_approval() -> None:
    expected = {
        "broker_snapshot_builder",
        "evidence_store",
        "scan_run_store",
        "pipeline_inputs",
        "universe_funnel",
        "broker_evidence_acquisition",
        "options_evidence_acquisition",
        "strategy_registry",
        "strategy_candidate_generator",
        "volatility_engine",
        "scenario_engine",
        "policy_resolver",
        "risk_authority_resolver",
        "execution_cost_contract",
        "eligibility_gate",
        "risk_gate",
        "dte_gate",
        "single_combination_gate",
        "portfolio_ranker",
        "ranking_store",
        "decision_pipeline",
        "strategy_nav_source",
        "position_manager",
        "approval_store",
        "bridge_status_reader",
        "bridge_reconciliation_reader",
    }
    assert expected <= set(inspect.signature(RuntimeServices).parameters)

    shell_services = RuntimeServices(**{name: object() for name in expected})
    assert shell_services.approval_enabled is False
    assert set(shell_services.readiness()["invalid_dependencies"]) == expected

    services = RuntimeServices(**_runtime_graph())
    assert services.approval_enabled is True
    assert services.readiness()["status"] == "READY"

    missing = _runtime_graph()
    missing["risk_authority_resolver"] = None
    degraded = RuntimeServices(**missing)
    assert degraded.approval_enabled is False
    assert degraded.readiness()["decision"] == "NO_TRADE"
    assert "risk_authority_resolver" in degraded.readiness()["missing_dependencies"]


@pytest.mark.parametrize(
    "dependency",
    [
        "broker_snapshot_builder",
        "execution_cost_contract",
        "policy_resolver",
        "risk_authority_resolver",
        "strategy_nav_source",
        "position_manager",
        "approval_store",
    ],
)
def test_runtime_services_reject_invalid_authority_capabilities(
    dependency: str,
) -> None:
    graph = _runtime_graph()
    graph[dependency] = (
        _ResolveOnly(_policy())
        if dependency == "policy_resolver"
        else _ResolveOnly(_authority())
        if dependency == "risk_authority_resolver"
        else object()
    )
    services = RuntimeServices(**graph)

    assert services.approval_enabled is False
    assert services.readiness()["decision"] == "NO_TRADE"
    assert dependency in services.readiness()["invalid_dependencies"]


def test_runtime_services_require_valid_strategy_nav_before_pipeline_execution() -> None:
    pipeline = _SlotPipeline()
    invalid = RuntimeServices(
        **_runtime_graph(
            decision_pipeline=pipeline,
            strategy_nav_source=_ProtocolPort(_nav_snapshot(valid=False)),
        )
    )

    rejected = invalid.run_slot("scan-invalid-nav", NOW)
    assert rejected["decision"] == "NO_TRADE"
    assert "STRATEGY_NAV_INVALID" in rejected["reasons"]
    assert pipeline.calls == 0

    ready = RuntimeServices(
        **_runtime_graph(
            decision_pipeline=pipeline,
            strategy_nav_source=_ProtocolPort(_nav_snapshot()),
        )
    )
    result = ready.run_slot("scan-valid-nav", NOW)
    assert result["scan_run_id"] == "scan-valid-nav"
    assert pipeline.calls == 1


def test_runtime_services_projects_pipeline_operational_timing() -> None:
    class TimedSlotPipeline(_SlotPipeline):
        def __init__(self) -> None:
            super().__init__()
            self._timing: dict[str, object] = {}

        def run_slot(
            self,
            scan_run_id: str,
            slot_at: datetime,
        ) -> dict[str, object]:
            result = super().run_slot(scan_run_id, slot_at)
            self._timing = {
                "schema": "options_copilot.scan_operational_timing.v1",
                "scan_run_id": scan_run_id,
                "total_duration_ms": 7,
                "stages": (
                    {"stage": "INITIALIZATION", "duration_ms": 2},
                    {"stage": "INPUT_ACQUISITION", "duration_ms": 5},
                ),
                "decision_authority": "OBSERVATION_ONLY",
                "affects_decision": False,
            }
            return result

        @property
        def last_operational_timing(self) -> Mapping[str, object]:
            return self._timing

    pipeline = TimedSlotPipeline()
    services = RuntimeServices(
        **_runtime_graph(decision_pipeline=pipeline)
    )

    result = services.run_slot("scan-timed-runtime", NOW)

    assert result["scan_run_id"] == "scan-timed-runtime"
    assert services.last_operational_timing == {
        "schema": "options_copilot.scan_operational_timing.v1",
        "scan_run_id": "scan-timed-runtime",
        "total_duration_ms": 7,
        "stages": (
            {"stage": "INITIALIZATION", "duration_ms": 2},
            {"stage": "INPUT_ACQUISITION", "duration_ms": 5},
        ),
        "decision_authority": "OBSERVATION_ONLY",
        "affects_decision": False,
    }


def test_runtime_ranking_and_authorization_bind_current_strategy_nav() -> None:
    nav = _nav_snapshot()
    ranking_store = _RankingPort(_ranking_payload(nav))
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=ranking_store,
            strategy_nav_source=_ProtocolPort(nav),
        )
    )

    ranking = services.latest_ranking()
    assert ranking["decision"] == "CANDIDATES_AVAILABLE"
    assert ranking["approval_enabled"] is True
    assert services.authorize_rank_one("ranking-1", "candidate-1") == {
        "authorized": True
    }
    assert ranking_store.authorize_args == (
        "ranking-1",
        "candidate-1",
        {"candidate_hash": "c" * 64},
    )

    ranking_store.terminal_current = False
    terminal_no_trade = services.latest_ranking()
    assert terminal_no_trade["decision"] == "NO_TRADE"
    assert terminal_no_trade["approval_enabled"] is False
    assert "RANKING_TERMINAL_NOT_CURRENT" in terminal_no_trade["reasons"]
    assert services.authorize_rank_one("ranking-1", "candidate-1") is None
    ranking_store.terminal_current = True

    changed_nav = replace(
        nav,
        strategy_nav=Decimal("2001"),
        content_hash="0" * 64,
    )
    stale = RuntimeServices(
        **_runtime_graph(
            ranking_store=ranking_store,
            strategy_nav_source=_ProtocolPort(changed_nav),
        )
    )
    assert stale.latest_ranking()["decision"] == "NO_TRADE"
    assert stale.authorize_rank_one("ranking-1", "candidate-1") is None


def _research_allocation_payload() -> dict[str, object]:
    return build_research_allocation_evidence(
        event_rows=(
            {"symbol": "NVDA", "deterministic_score": "40", "advisory_score": "91"},
            {"symbol": "TSLA", "deterministic_score": "45", "advisory_score": "70"},
            {"symbol": "AAPL", "deterministic_score": "80", "advisory_score": None},
            {"symbol": "MSFT", "deterministic_score": "75", "advisory_score": None},
            {"symbol": "GOOG", "deterministic_score": "30", "advisory_score": None},
        ),
        scanner_rows=(),
        core_rows=(),
        limit=2,
    )


def _large_research_allocation_payload() -> dict[str, object]:
    event_symbols = tuple(f"S{index:02d}" for index in range(11))
    return build_research_allocation_evidence(
        event_rows=tuple(
            {
                "symbol": symbol,
                "deterministic_score": str(40 + index),
                "advisory_score": str(90 - index),
            }
            for index, symbol in enumerate(event_symbols)
        ),
        scanner_rows=(),
        core_rows=(),
        limit=11,
    )


def _research_joint_snapshot() -> dict[str, object]:
    row = JointRankingRow.build(
        candidate_id="qqq-research-1",
        underlying="QQQ",
        candidate_hash="e" * 64,
        disposition=JointDisposition.RESEARCH_ONLY,
        rank=None,
        score=Decimal("61.25"),
        score_components={"liquidity_quality": Decimal("0.8")},
        reason_codes=("AFTER_COST_EV_NOT_POSITIVE",),
    )
    body = {
        "schema": "options_copilot.joint_ranking.v1",
        "scan_run_id": "scan-no-trade",
        "generated_at": NOW.isoformat(timespec="microseconds"),
        "broker_snapshot_hash": "6" * 64,
        "strategy_nav_hash": "7" * 64,
        "executable": (),
        "research_watchlist": (row.as_dict(),),
        "input_hash": "8" * 64,
        "review_only": True,
        "direct_order_submission": False,
    }
    return JointRankingSnapshot(
        scan_run_id="scan-no-trade",
        generated_at=NOW,
        broker_snapshot_hash="6" * 64,
        strategy_nav_hash="7" * 64,
        executable=(),
        research_watchlist=(row,),
        input_hash="8" * 64,
        snapshot_hash=canonical_hash(body),
    ).as_dict()


def test_runtime_projects_latest_immutable_no_trade_diagnostics_without_snapshot() -> None:
    research_allocation = _research_allocation_payload()
    joint_ranking = _research_joint_snapshot()

    class NoTradeRankingPort(_ProtocolPort):
        def latest(self):
            return None

        def latest_decision(self):
            return {
                "record_type": "NO_TRADE",
                "record": {
                    "scan_run_id": "scan-no-trade",
                    "status": "NO_TRADE",
                    "reasons": ["UNIVERSE_EMPTY"],
                    "gate_bundle_hash": "a" * 64,
                    "funnel_trace": {
                        "discovered_underlyings": 135,
                        "deep_scan_requested": 30,
                        "deep_scan_completed": 0,
                        "ranked_count": 0,
                        "ranked_limit": 10,
                        "filler_candidates": 0,
                        "underlying_quote_reason_codes": [
                            "UNDERLYING_QUOTE_BATCH_FAILED",
                            "UNDERLYING_QUOTE_BATCH_INCOMPLETE",
                        ],
                        "underlying_quote_missing_symbols": ["SOAR", "BCARU"],
                        "research_allocation": research_allocation,
                        "joint_ranking": joint_ranking,
                    },
                },
                "decision_hash": "b" * 64,
                "record_hash": "c" * 64,
                "recorded_at": NOW,
            }

    class ScanPort(_ProtocolPort):
        def get(self, scan_run_id: str):
            assert scan_run_id == "scan-no-trade"
            return {
                "scan_run_id": scan_run_id,
                "status": "COMPLETED",
                "result_hash": "d" * 64,
            }

    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=NoTradeRankingPort(),
            scan_run_store=ScanPort(),
        )
    )

    ranking = services.latest_ranking()
    assert ranking["decision"] == "NO_TRADE"
    assert ranking["scan_run_id"] == "scan-no-trade"
    assert ranking["missing_symbols"] == ("SOAR", "BCARU")
    assert ranking["reasons"] == (
        "UNIVERSE_EMPTY",
        "UNDERLYING_QUOTE_BATCH_FAILED",
        "UNDERLYING_QUOTE_BATCH_INCOMPLETE",
    )
    assert ranking["funnel_trace"]["discovered_underlyings"] == 135
    allocation = ranking["funnel_trace"]["research_allocation"]
    assert allocation["decision_authority"] == "SUPPORTING_ONLY"
    assert allocation["selected_symbols"] == ("NVDA", "AAPL")
    assert allocation["approval_eligible"] is False
    assert allocation["instruction_creation_allowed"] is False
    assert allocation["order_allowed"] is False
    assert allocation == _normalise_research_allocation(research_allocation)
    assert ranking["approval_enabled"] is False
    assert ranking["joint_ranking_hash"] == joint_ranking["snapshot_hash"]
    assert ranking["research_watchlist"][0]["candidate_id"] == "qqq-research-1"
    assert ranking["funnel_trace"]["joint_ranking"] == joint_ranking

    scan = services.latest_scan()
    assert scan["status"] == "COMPLETED"
    assert scan["decision"] == "NO_TRADE"
    assert scan["missing_symbols"] == ("SOAR", "BCARU")
    assert scan["funnel_trace"]["research_allocation"] == allocation
    assert scan["approval_enabled"] is False


def test_runtime_no_trade_rejects_tampered_joint_research_watchlist() -> None:
    joint_ranking = _research_joint_snapshot()
    joint_ranking["research_watchlist"][0]["score"] = Decimal("99")

    class NoTradeRankingPort(_ProtocolPort):
        def latest(self):
            return None

        def latest_decision(self):
            return {
                "record_type": "NO_TRADE",
                "record": {
                    "scan_run_id": "scan-no-trade",
                    "status": "NO_TRADE",
                    "reasons": ["PORTFOLIO_EMPTY"],
                    "funnel_trace": {"joint_ranking": joint_ranking},
                },
                "recorded_at": NOW,
            }

    ranking = RuntimeServices(
        **_runtime_graph(ranking_store=NoTradeRankingPort())
    ).latest_ranking()

    assert ranking["decision"] == "NO_TRADE"
    assert ranking["approval_enabled"] is False
    assert ranking["research_watchlist"] == ()
    assert ranking["joint_ranking_hash"] is None
    assert "joint_ranking" not in ranking["funnel_trace"]


@pytest.mark.parametrize(
    "variant",
    (
        "unknown_top_level_authority_key",
        "unknown_nested_authority_key",
        "false_displacement_count",
        "false_order_change_count",
        "shadow_selected_score_mismatch",
        "producer_impossible_source",
        "unscored_promoted_symbol",
        "numeric_symbol_identity",
        "noncanonical_symbol_identity",
        "producer_impossible_count_bound",
    ),
)
def test_research_allocation_v3_rejects_malformed_immutable_evidence(
    variant: str,
) -> None:
    payload = _research_allocation_payload()
    score_evidence = payload["score_evidence"]
    assert isinstance(score_evidence, tuple)
    first_score = score_evidence[0]
    assert isinstance(first_score, dict)

    if variant == "unknown_top_level_authority_key":
        payload["order_submission_requested"] = True
    elif variant == "unknown_nested_authority_key":
        first_score["order_allowed"] = True
    elif variant == "false_displacement_count":
        payload["advisory_selection_displacement_count"] = 0
    elif variant == "false_order_change_count":
        payload["advisory_order_changed_count"] = 1
    elif variant == "shadow_selected_score_mismatch":
        first_score["selected_research_priority_score"] = "12"
    elif variant == "producer_impossible_source":
        shadow_score = next(
            row
            for row in score_evidence
            if row["selected_research_priority_source"]
            == "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
        )
        shadow_score["selected_research_priority_source"] = "NEWS_SUPPORTING_ONLY"
        shadow_score["selected_research_priority_score"] = shadow_score[
            "deterministic_score"
        ]
    elif variant == "unscored_promoted_symbol":
        payload["advisory_promoted_symbols"] = ("NVDA", "AAPL")
    elif variant == "numeric_symbol_identity":
        first_score["symbol"] = 123
    elif variant == "noncanonical_symbol_identity":
        first_score["symbol"] = " nvda "
    elif variant == "producer_impossible_count_bound":
        payload["total_event_symbol_count"] = 51
    else:  # pragma: no cover - parametrization owns the variants.
        raise AssertionError(f"unexpected test variant: {variant}")

    assert _normalise_research_allocation(payload) is None


def test_research_allocation_v3_round_trips_more_than_ten_score_rows() -> None:
    payload = _large_research_allocation_payload()

    assert _normalise_research_allocation(payload) == payload
    json_payload = json.loads(json.dumps(payload))
    assert _normalise_research_allocation(json_payload) == payload


def test_legacy_research_allocation_v1_is_omitted_fail_closed() -> None:
    payload = _research_allocation_payload()
    payload["schema"] = "options_copilot.research_allocation_evidence.v1"

    assert _normalise_research_allocation(payload) is None


@pytest.mark.parametrize(
    "corruption",
    (
        "order_authority",
        "producer_impossible_source",
        "unscored_promoted_above_ten",
        "numeric_symbol_identity",
    ),
)
def test_runtime_drops_semantically_corrupt_research_allocation(
    corruption: str,
) -> None:
    payload = (
        _large_research_allocation_payload()
        if corruption == "unscored_promoted_above_ten"
        else _research_allocation_payload()
    )
    scores = payload["score_evidence"]
    assert isinstance(scores, tuple)
    first_score = scores[0]
    assert isinstance(first_score, dict)
    if corruption == "order_authority":
        payload["order_allowed"] = True
    elif corruption == "producer_impossible_source":
        shadow_score = next(
            row
            for row in scores
            if row["selected_research_priority_source"]
            == "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
        )
        shadow_score["selected_research_priority_source"] = "NEWS_SUPPORTING_ONLY"
        shadow_score["selected_research_priority_score"] = shadow_score[
            "deterministic_score"
        ]
    elif corruption == "unscored_promoted_above_ten":
        payload["advisory_promoted_symbols"] = ("CORE",)
    elif corruption == "numeric_symbol_identity":
        first_score["symbol"] = 123
    else:  # pragma: no cover - parametrization owns the variants.
        raise AssertionError(f"unexpected corruption: {corruption}")

    class UnsafeNoTradeRankingPort(_ProtocolPort):
        def latest(self):
            return None

        def latest_decision(self):
            return {
                "record_type": "NO_TRADE",
                "record": {
                    "scan_run_id": "scan-unsafe-allocation",
                    "status": "NO_TRADE",
                    "reasons": ["UNIVERSE_EMPTY"],
                    "funnel_trace": {
                        "research_allocation": payload,
                    },
                },
                "decision_hash": "b" * 64,
                "record_hash": "c" * 64,
                "recorded_at": NOW,
            }

    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=UnsafeNoTradeRankingPort(),
            scan_run_store=_ProtocolPort({}),
        )
    )

    assert "research_allocation" not in services.latest_ranking()["funnel_trace"]
    assert "research_allocation" not in services.latest_scan()["funnel_trace"]


@pytest.mark.parametrize(
    "schema",
    (
        "options_copilot.research_allocation_evidence.v1",
        "options_copilot.research_allocation_evidence.v2",
    ),
)
def test_runtime_omits_legacy_allocation_from_ranking_and_scan_read_models(
    schema: str,
) -> None:
    nav = _nav_snapshot()
    legacy = {"schema": schema, "decision_authority": "SUPPORTING_ONLY"}
    ranking_payload = _ranking_payload(nav)
    ranking_payload["funnel_trace"] = {"research_allocation": legacy}
    ranking_payload["immutable_inputs"] = {
        "funnel_trace": {"research_allocation": legacy},
    }
    ranking_store = _RankingPort(ranking_payload)

    class ScanPort(_ProtocolPort):
        def get(self, scan_run_id: str) -> dict[str, object]:
            assert scan_run_id == ranking_payload["scan_run_id"]
            return {
                "scan_run_id": scan_run_id,
                "funnel_trace": {"research_allocation": legacy},
            }

    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=ranking_store,
            scan_run_store=ScanPort(),
            strategy_nav_source=_ProtocolPort(nav),
        )
    )

    ranking = services.latest_ranking()
    scan = services.latest_scan()

    assert "research_allocation" not in ranking["funnel_trace"]
    assert "research_allocation" not in ranking["immutable_inputs"][
        "funnel_trace"
    ]
    assert "research_allocation" not in scan["funnel_trace"]


def test_runtime_projects_newer_no_trade_instead_of_older_ranking_snapshot() -> None:
    nav = _nav_snapshot()

    class NewerNoTradeRankingPort(_RankingPort):
        def latest_decision(self):
            return {
                "record_type": "NO_TRADE",
                "record": {
                    "scan_run_id": "scan-new-no-trade",
                    "status": "NO_TRADE",
                    "reasons": ["UNIVERSE_EMPTY"],
                    "gate_bundle_hash": "a" * 64,
                    "funnel_trace": {
                        "discovered_underlyings": 135,
                        "deep_scan_requested": 30,
                        "deep_scan_completed": 28,
                        "ranked_count": 0,
                        "ranked_limit": 10,
                        "filler_candidates": 0,
                        "underlying_quote_reason_codes": [
                            "UNDERLYING_QUOTE_BATCH_INCOMPLETE",
                        ],
                        "underlying_quote_missing_symbols": ["SOAR"],
                    },
                },
                "decision_hash": "b" * 64,
                "record_hash": "c" * 64,
                "recorded_at": NOW,
            }

    class ScanPort(_ProtocolPort):
        def get(self, scan_run_id: str):
            assert scan_run_id == "scan-new-no-trade"
            return {
                "scan_run_id": scan_run_id,
                "status": "COMPLETED",
                "result_hash": "d" * 64,
            }

    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=NewerNoTradeRankingPort(_ranking_payload(nav)),
            scan_run_store=ScanPort(),
            strategy_nav_source=_ProtocolPort(nav),
        )
    )

    ranking = services.latest_ranking()
    assert ranking["decision"] == "NO_TRADE"
    assert ranking["scan_run_id"] == "scan-new-no-trade"
    assert ranking["ranking_snapshot_id"] is None
    assert ranking["candidates"] == ()
    assert ranking["missing_symbols"] == ("SOAR",)
    assert ranking["reasons"] == (
        "UNIVERSE_EMPTY",
        "UNDERLYING_QUOTE_BATCH_INCOMPLETE",
    )

    scan = services.latest_scan()
    assert scan["status"] == "COMPLETED"
    assert scan["decision"] == "NO_TRADE"
    assert scan["scan_run_id"] == "scan-new-no-trade"
    assert scan["ranking_snapshot_id"] is None
    assert scan["candidates"] == ()
    assert scan["missing_symbols"] == ("SOAR",)
    assert scan["approval_enabled"] is False


def test_runtime_scan_timing_failure_does_not_suppress_immutable_scan() -> None:
    nav = _nav_snapshot()

    class NewerNoTradeRankingPort(_RankingPort):
        def latest_decision(self):
            return {
                "record_type": "NO_TRADE",
                "record": {
                    "scan_run_id": "scan-timing-failure",
                    "status": "NO_TRADE",
                    "reasons": ["UNIVERSE_EMPTY"],
                    "gate_bundle_hash": "a" * 64,
                    "funnel_trace": {},
                },
                "decision_hash": "b" * 64,
                "record_hash": "c" * 64,
                "recorded_at": NOW,
            }

    class ScanPort(_ProtocolPort):
        def get(self, scan_run_id: str):
            return {
                "scan_run_id": scan_run_id,
                "status": "COMPLETED",
                "result_hash": "d" * 64,
            }

        def operational_timing(self, scan_run_id: str):
            raise OSError(f"timing store unavailable for {scan_run_id}")

    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=NewerNoTradeRankingPort(_ranking_payload(nav)),
            scan_run_store=ScanPort(),
            strategy_nav_source=_ProtocolPort(nav),
        )
    )

    scan = services.latest_scan()

    assert scan["scan_run_id"] == "scan-timing-failure"
    assert scan["status"] == "COMPLETED"
    assert scan["decision"] == "NO_TRADE"
    assert "operational_timing" not in scan


def test_runtime_fails_closed_when_latest_decision_ledger_read_fails() -> None:
    nav = _nav_snapshot()

    class BrokenDecisionRankingPort(_RankingPort):
        def latest_decision(self):
            raise RuntimeError("simulated immutable decision ledger failure")

    class OldScanPort(_ProtocolPort):
        def get(self, scan_run_id: str):
            return {
                "scan_run_id": scan_run_id,
                "status": "COMPLETED",
                "decision": "READY",
            }

    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=BrokenDecisionRankingPort(_ranking_payload(nav)),
            scan_run_store=OldScanPort(),
            strategy_nav_source=_ProtocolPort(nav),
        )
    )

    ranking = services.latest_ranking()
    assert ranking["decision"] == "NO_TRADE"
    assert ranking["approval_enabled"] is False
    assert ranking["candidates"] == ()
    assert ranking["reasons"] == ("RANKING_DECISION_LEDGER_UNAVAILABLE",)

    scan = services.latest_scan()
    assert scan["decision"] == "NO_TRADE"
    assert scan["approval_enabled"] is False
    assert scan["reasons"] == ("RANKING_DECISION_LEDGER_UNAVAILABLE",)


def test_immediate_scan_records_symbol_gate_and_immutable_hashes_in_campaign() -> None:
    class Inputs:
        @contextmanager
        def manual_core_only(self):
            yield {
                "target_symbol": "META",
                "attempt_number": 2,
                "attempt_kind": "REEVALUATION",
                "core_index": 8,
                "core_count": 21,
                "cycle": 1,
                "next_symbol": "GOOGL",
            }

    class Loop:
        def run_now(self):
            return type(
                "Tick",
                (),
                {
                    "scan_run_id": "scan.meta",
                    "status": "COMPLETED",
                    "duplicate_reason": None,
                },
            )()

    class Lifecycle:
        scanner_loop = Loop()

    class Composition:
        lifecycle = Lifecycle()
        pipeline_inputs = Inputs()

    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime.production_composition = Composition()
    runtime._immediate_scan_lock = threading.Lock()
    runtime._closed = False
    runtime._closing = False
    runtime._immediate_campaign_lock = threading.RLock()
    runtime._immediate_campaign_key = None
    runtime._immediate_campaign_id = None
    runtime._immediate_campaign_started_at = None
    runtime._immediate_campaign_updated_at = None
    runtime._immediate_campaign_attempts = []
    runtime._immediate_campaign_target_count = 0
    runtime._immediate_campaign_next_symbol = None
    runtime.latest_ranking = lambda: {  # type: ignore[method-assign]
        "status": "DEGRADED",
        "decision": "NO_TRADE",
        "approval_enabled": False,
        "reasons": ("QUOTE_LIQUIDITY_SPREAD_REJECTED",),
        "candidates": (),
        "scan_run_id": "scan.meta",
        "ranking_snapshot_id": None,
        "decision_hash": "1" * 64,
        "record_hash": "2" * 64,
        "gate_bundle_hash": "3" * 64,
        "recorded_at": "2026-08-11T17:11:51+00:00",
    }

    response = runtime.immediate_scan()
    campaign = response["campaign"]
    assert isinstance(campaign, Mapping)
    assert campaign["status"] == "RUNNING_NO_TRADE"
    assert campaign["completed_symbol_count"] == 1
    assert campaign["target_symbol_count"] == 21
    assert campaign["next_symbol"] == "GOOGL"
    assert campaign["decision_authority"] == "OBSERVATION_ONLY"
    assert campaign["approval_enabled"] is False
    attempt = campaign["attempts"][0]
    assert attempt["target_symbol"] == "META"
    assert attempt["attempt_kind"] == "REEVALUATION"
    assert attempt["stopped_at_gate"] == "GATE_4_OPTION_EDGE_LIQUIDITY"
    assert attempt["decision_hash"] == "1" * 64
    assert attempt["record_hash"] == "2" * 64
    assert attempt["gate_bundle_hash"] == "3" * 64


def test_immediate_bounded_market_scan_returns_only_the_current_ranking() -> None:
    class Loop:
        calls = 0

        def run_now(self):
            self.calls += 1
            return type(
                "Tick",
                (),
                {
                    "scan_run_id": "scan.bounded",
                    "status": "COMPLETED",
                    "duplicate_reason": None,
                },
            )()

    loop = Loop()

    class Lifecycle:
        scanner_loop = loop

    class Composition:
        lifecycle = Lifecycle()

    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime.production_composition = Composition()
    runtime._immediate_scan_lock = threading.Lock()
    runtime._closed = False
    runtime._closing = False
    ranking_calls = 0

    def latest_ranking():
        nonlocal ranking_calls
        ranking_calls += 1
        return {
            "status": "READY",
            "decision": "CANDIDATES_AVAILABLE",
            "scan_run_id": "scan.bounded",
            "ranking_snapshot_id": "ranking.bounded",
            "candidates": ({"candidate_id": "candidate.bounded"},),
        }

    runtime.latest_ranking = latest_ranking  # type: ignore[method-assign]

    response = runtime.immediate_scan("BOUNDED_MARKET")

    assert loop.calls == 1
    assert ranking_calls == 1
    assert response["scan_run_id"] == "scan.bounded"
    assert response["scan_scope"] == "BOUNDED_MARKET"
    assert response["decision"] == "CANDIDATES_AVAILABLE"
    assert response["candidates"] == ({"candidate_id": "candidate.bounded"},)
    assert response["approval_enabled"] is False
    assert response["review_only"] is True
    assert response["direct_order_submission"] is False


def test_immediate_bounded_market_scan_does_not_read_stale_ranking_when_closed() -> None:
    class Loop:
        calls = 0

        def run_now(self):
            self.calls += 1
            return type(
                "Tick",
                (),
                {
                    "scan_run_id": None,
                    "status": "NO_TRADE",
                    "duplicate_reason": "MARKET_SESSION_NOT_OPEN",
                },
            )()

    loop = Loop()

    class Lifecycle:
        scanner_loop = loop

    class Composition:
        lifecycle = Lifecycle()

    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime.production_composition = Composition()
    runtime._immediate_scan_lock = threading.Lock()
    runtime._closed = False
    runtime._closing = False
    ranking_calls = 0

    def latest_ranking():
        nonlocal ranking_calls
        ranking_calls += 1
        raise AssertionError("closed scans must not read an old ranking")

    runtime.latest_ranking = latest_ranking  # type: ignore[method-assign]

    response = runtime.immediate_scan("BOUNDED_MARKET")

    assert loop.calls == 1
    assert ranking_calls == 0
    assert response["decision"] == "NO_TRADE"
    assert response["reasons"] == ("MARKET_SESSION_NOT_OPEN",)
    assert response["scan_run_id"] is None
    assert response["scan_scope"] == "BOUNDED_MARKET"
    assert response["review_only"] is True
    assert response["direct_order_submission"] is False


def test_immediate_scan_campaign_pauses_without_completing_pacing_denial() -> None:
    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime._immediate_campaign_lock = threading.RLock()
    runtime._immediate_campaign_key = None
    runtime._immediate_campaign_id = None
    runtime._immediate_campaign_started_at = None
    runtime._immediate_campaign_updated_at = None
    runtime._immediate_campaign_attempts = []
    runtime._immediate_campaign_target_count = 0
    runtime._immediate_campaign_next_symbol = None

    runtime._record_immediate_scan_attempt(
        scope={
            "target_symbol": "SPY",
            "attempt_number": 2,
            "attempt_kind": "REEVALUATION",
            "core_index": 0,
            "core_count": 2,
            "cycle": 1,
            "next_symbol": "QQQ",
        },
        response={
            "scan_run_id": "scan.pacing",
            "scan_status": "COMPLETED",
            "decision": "NO_TRADE",
            "reasons": ("OPTIONABILITY_PACING_DENIED",),
            "candidates": (),
            "decision_hash": "1" * 64,
            "record_hash": "2" * 64,
            "gate_bundle_hash": "3" * 64,
        },
        started_at=NOW,
    )

    campaign = runtime.immediate_scan_campaign()

    assert campaign["status"] == "PAUSED_PACING"
    assert campaign["completed_symbol_count"] == 0
    assert campaign["next_symbol"] == "QQQ"


def test_immediate_scan_retries_scope_when_calendar_pacing_prevents_a_run() -> None:
    class Inputs:
        def __init__(self) -> None:
            self.retried: list[dict[str, object]] = []

        @contextmanager
        def manual_core_only(self):
            yield {
                "target_symbol": "IWM",
                "attempt_number": 2,
                "attempt_kind": "REEVALUATION",
                "core_index": 2,
                "core_count": 21,
                "cycle": 1,
                "next_symbol": "DIA",
            }

        def retry_manual_core_attempt(self, scope):
            self.retried.append(dict(scope))
            return True

    inputs = Inputs()

    class Loop:
        def run_now(self):
            return type(
                "Tick",
                (),
                {
                    "scan_run_id": None,
                    "status": "NO_TRADE",
                    "duplicate_reason": "CALENDAR_PACING_DENIED",
                },
            )()

    class Lifecycle:
        scanner_loop = Loop()

    class Composition:
        lifecycle = Lifecycle()
        pipeline_inputs = inputs

    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime.production_composition = Composition()
    runtime._immediate_scan_lock = threading.Lock()
    runtime._closed = False
    runtime._closing = False
    runtime._immediate_campaign_lock = threading.RLock()
    runtime._immediate_campaign_key = None
    runtime._immediate_campaign_id = None
    runtime._immediate_campaign_started_at = None
    runtime._immediate_campaign_updated_at = None
    runtime._immediate_campaign_attempts = []
    runtime._immediate_campaign_target_count = 0
    runtime._immediate_campaign_next_symbol = None

    response = runtime.immediate_scan()
    campaign = response["campaign"]

    assert inputs.retried[0]["target_symbol"] == "IWM"
    assert campaign["status"] == "PAUSED_PACING"
    assert campaign["completed_symbol_count"] == 0
    assert campaign["next_symbol"] == "IWM"


@pytest.mark.parametrize("failure_point", ("runner", "ranking", "campaign"))
def test_immediate_scan_retries_scope_when_runtime_step_raises(
    failure_point: str,
) -> None:
    class Inputs:
        def __init__(self) -> None:
            self.retry_calls = 0

        @contextmanager
        def manual_core_only(self):
            yield {
                "target_symbol": "SPY",
                "attempt_number": 1,
                "attempt_kind": "WARMUP",
                "core_index": 0,
                "core_count": 1,
                "cycle": 1,
                "next_symbol": "SPY",
            }

        def retry_manual_core_attempt(self, _scope):
            self.retry_calls += 1
            return True

    inputs = Inputs()

    class Loop:
        def run_now(self):
            if failure_point == "runner":
                raise RuntimeError("runner failed")
            return type(
                "Tick",
                (),
                {
                    "scan_run_id": "scan.spy",
                    "status": "COMPLETED",
                    "duplicate_reason": None,
                },
            )()

    class Lifecycle:
        scanner_loop = Loop()

    class Composition:
        lifecycle = Lifecycle()
        pipeline_inputs = inputs

    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime.production_composition = Composition()
    runtime._immediate_scan_lock = threading.Lock()
    runtime._closed = False
    runtime._closing = False
    runtime._immediate_campaign_lock = threading.RLock()
    runtime._immediate_campaign_key = None
    runtime._immediate_campaign_id = None
    runtime._immediate_campaign_started_at = None
    runtime._immediate_campaign_updated_at = None
    runtime._immediate_campaign_attempts = []
    runtime._immediate_campaign_target_count = 0
    runtime._immediate_campaign_next_symbol = None

    def ranking():
        if failure_point == "ranking":
            raise RuntimeError("ranking failed")
        return {
            "status": "DEGRADED",
            "decision": "NO_TRADE",
            "reasons": ("NO_ELIGIBLE_COMBINATIONS",),
            "candidates": (),
            "scan_run_id": "scan.spy",
            "decision_hash": "1" * 64,
            "record_hash": "2" * 64,
            "gate_bundle_hash": "3" * 64,
        }

    runtime.latest_ranking = ranking  # type: ignore[method-assign]
    original_record = runtime._record_immediate_scan_attempt
    if failure_point == "campaign":
        runtime._record_immediate_scan_attempt = (  # type: ignore[method-assign]
            lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("campaign failed"))
        )

    with pytest.raises(RuntimeError):
        runtime.immediate_scan()

    assert inputs.retry_calls == 1
    runtime._record_immediate_scan_attempt = original_record  # type: ignore[method-assign]


def test_immediate_scan_campaign_resets_when_scope_cycle_advances() -> None:
    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime._immediate_campaign_lock = threading.RLock()
    runtime._immediate_campaign_key = None
    runtime._immediate_campaign_id = None
    runtime._immediate_campaign_started_at = None
    runtime._immediate_campaign_updated_at = None
    runtime._immediate_campaign_attempts = []
    runtime._immediate_campaign_target_count = 0
    runtime._immediate_campaign_next_symbol = None
    response = {
        "scan_run_id": "scan.cycle-one",
        "scan_status": "COMPLETED",
        "decision": "NO_TRADE",
        "reasons": ("QUOTE_LIQUIDITY_SPREAD_REJECTED",),
        "candidates": (),
        "decision_hash": "1" * 64,
        "record_hash": "2" * 64,
        "gate_bundle_hash": "3" * 64,
    }
    runtime._record_immediate_scan_attempt(
        scope={
            "target_symbol": "QQQ",
            "attempt_number": 2,
            "attempt_kind": "REEVALUATION",
            "core_count": 2,
            "cycle": 1,
            "next_symbol": "SPY",
        },
        response=response,
        started_at=NOW,
    )
    first_campaign_id = runtime.immediate_scan_campaign()["campaign_id"]
    runtime._record_immediate_scan_attempt(
        scope={
            "target_symbol": "SPY",
            "attempt_number": 1,
            "attempt_kind": "WARMUP",
            "core_count": 2,
            "cycle": 2,
            "next_symbol": "SPY",
        },
        response={**response, "scan_run_id": "scan.cycle-two"},
        started_at=NOW + timedelta(minutes=1),
    )

    campaign = runtime.immediate_scan_campaign()

    assert campaign["campaign_id"] != first_campaign_id
    assert campaign["attempt_count"] == 1
    assert campaign["completed_symbol_count"] == 0
    assert campaign["attempts"][0]["scan_run_id"] == "scan.cycle-two"


def test_immediate_scans_serialize_scope_run_and_campaign_recording() -> None:
    order: list[str] = []
    scope_lock = threading.Lock()
    warmup_entered = threading.Event()
    release_warmup = threading.Event()
    reevaluation_completed = threading.Event()

    class Inputs:
        def __init__(self) -> None:
            self.cursor = 0
            self.local = threading.local()

        @contextmanager
        def manual_core_only(self):
            with scope_lock:
                attempt_index = self.cursor
                self.cursor += 1
            kind = "WARMUP" if attempt_index == 0 else "REEVALUATION"
            self.local.kind = kind
            yield {
                "target_symbol": "SPY",
                "attempt_number": attempt_index + 1,
                "attempt_kind": kind,
                "core_index": 0,
                "core_count": 1,
                "cycle": 1,
                "next_symbol": "SPY",
            }

    inputs = Inputs()

    class Loop:
        def run_now(self):
            kind = inputs.local.kind
            if kind == "WARMUP":
                warmup_entered.set()
                assert release_warmup.wait(timeout=2)
            order.append(kind)
            if kind == "REEVALUATION":
                reevaluation_completed.set()
            return type(
                "Tick",
                (),
                {
                    "scan_run_id": f"scan.{kind.lower()}",
                    "status": "COMPLETED",
                    "duplicate_reason": None,
                },
            )()

    class Lifecycle:
        scanner_loop = Loop()

    class Composition:
        lifecycle = Lifecycle()
        pipeline_inputs = inputs

    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime.production_composition = Composition()
    runtime._immediate_scan_lock = threading.Lock()
    runtime._closed = False
    runtime._closing = False
    runtime._immediate_campaign_lock = threading.RLock()
    runtime._immediate_campaign_key = None
    runtime._immediate_campaign_id = None
    runtime._immediate_campaign_started_at = None
    runtime._immediate_campaign_updated_at = None
    runtime._immediate_campaign_attempts = []
    runtime._immediate_campaign_target_count = 0
    runtime._immediate_campaign_next_symbol = None
    runtime.latest_ranking = lambda: {  # type: ignore[method-assign]
        "status": "DEGRADED",
        "decision": "NO_TRADE",
        "approval_enabled": False,
        "reasons": ("NO_ELIGIBLE_COMBINATIONS",),
        "candidates": (),
        "scan_run_id": f"scan.{inputs.local.kind.lower()}",
        "decision_hash": "1" * 64,
        "record_hash": "2" * 64,
        "gate_bundle_hash": "3" * 64,
    }

    first = threading.Thread(target=runtime.immediate_scan)
    second = threading.Thread(target=runtime.immediate_scan)
    first.start()
    assert warmup_entered.wait(timeout=2)
    second.start()
    time.sleep(0.05)
    assert not reevaluation_completed.is_set()
    release_warmup.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert order == ["WARMUP", "REEVALUATION"]
    assert [
        item["attempt_kind"]
        for item in runtime.immediate_scan_campaign()["attempts"]
    ] == ["WARMUP", "REEVALUATION"]


def test_position_management_refresh_persists_supporting_only_research(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 8, 4, 13, 35, 30, tzinfo=timezone.utc)
    old = {
        "status": "DEGRADED",
        "trading_date": "2026-08-03",
        "phase": "INTRADAY_RECOVERY",
        "action_pool_count": 0,
    }
    fresh = {
        "status": "DEGRADED",
        "trading_date": "2026-08-04",
        "phase": "INTRADAY_RECOVERY",
        "available_count": 1,
        "decision_authority": "SUPPORTING_ONLY",
        "action_pool_count": 0,
    }
    reads = iter((old, fresh))
    imported: list[Mapping[str, object]] = []
    source_arguments: list[dict[str, object]] = []

    class Source:
        def __init__(self, gateway, pacing, **kwargs):
            assert gateway is runtime.production_composition.gateway
            assert pacing is runtime.production_composition.pipeline_inputs.pacing
            source_arguments.append(kwargs)

        def resolve_top10(self, *, scheduled_for):
            assert scheduled_for == now
            return ("exact-structure",)

    monkeypatch.setattr(runtime_module, "read_research_top10", lambda: next(reads))
    monkeypatch.setattr(
        runtime_module,
        "import_research_top10",
        lambda payload: imported.append(payload) or {"status": "IMPORTED"},
    )
    monkeypatch.setattr(runtime_module, "DirectTop10StructureSource", Source)
    monkeypatch.setattr(
        intraday_recovery_module,
        "build_intraday_recovery_envelope",
        lambda structures, **kwargs: {
            "structures": tuple(structures),
            "observed_at": kwargs["observed_at"],
            "strategy_nav_usd": kwargs["strategy_nav_usd"],
            "decision_authority": "SUPPORTING_ONLY",
            "action_pool_eligible": False,
        },
    )

    runtime = OptionsCopilotRuntime.__new__(OptionsCopilotRuntime)
    runtime.production_composition = SimpleNamespace(
        gateway=SimpleNamespace(_now=lambda: now),
        pipeline_inputs=SimpleNamespace(pacing=object()),
    )
    runtime.config = SimpleNamespace(news_core_symbols=("AAPL", "MSFT"))
    runtime.runtime_services = SimpleNamespace(
        _strategy_nav=lambda *args, **kwargs: (
            SimpleNamespace(strategy_nav=Decimal("2000")),
            (),
        )
    )
    runtime._current_control_projection = lambda: {  # type: ignore[method-assign]
        "observed_at": now.isoformat(),
        "account": {"net_liquidation": "2100"},
        "positions": (
            {
                "security_type": "OPT",
                "quantity": "1",
            },
        ),
    }

    result = runtime.position_research_top10()

    assert result == fresh
    assert len(imported) == 1
    assert imported[0]["decision_authority"] == "SUPPORTING_ONLY"
    assert imported[0]["action_pool_eligible"] is False
    assert imported[0]["strategy_nav_usd"] == Decimal("2000")
    assert source_arguments[0]["include_scanner"] is True
    assert source_arguments[0]["maximum_structures"] == 10


def test_runtime_projects_persisted_challenge_and_confirmation_ports() -> None:
    nav = _nav_snapshot()
    ranking_store = _RankingPort(_ranking_payload(nav))
    approval_store = _ApprovalPort()
    contracts = _contracts()
    broker_source = _BrokerSource(contracts)
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=ranking_store,
            approval_store=approval_store,
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=BrokerSnapshotBuilder(broker_source),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )

    challenge = services.create_rank_one_challenge("ranking-1", "candidate-1")
    assert challenge is not None
    assert challenge["status"] == "PENDING_SECOND_CONFIRMATION"
    assert challenge["review_only"] is True
    assert challenge["order_submitted"] is False
    assert approval_store.create_kwargs is not None
    assert approval_store.create_kwargs["ranking_store"] is ranking_store
    assert (
        approval_store.create_kwargs["execution_cost_contract"]
        is services.execution_cost_contract
    )
    create_broker_proof = approval_store.create_kwargs["broker_proof"]
    assert create_broker_proof == {
        **create_broker_proof,
        "schema": "options_copilot.approval.broker_proof.v1",
        "ranking_snapshot_id": "ranking-1",
        "candidate_id": "candidate-1",
        "proposal_hash": "a" * 64,
        "quote_batch_id": "batch-1",
        "contract_ids": [101, 102],
        "account_nlv_usd": "999999",
        "open_option_position_count": 0,
        "working_order_count": 0,
        "unsubmitted_instruction_count": 0,
        "status": "COMPLETE",
    }
    assert set(create_broker_proof) == {
        "schema",
        "ranking_snapshot_id",
        "candidate_id",
        "proposal_hash",
        "snapshot_hash",
        "built_at",
        "quote_batch_id",
        "oldest_quote_observed_at",
        "state_hashes",
        "contract_definitions_hash",
        "quotes_hash",
        "contract_ids",
        "account_nlv_usd",
        "open_option_position_count",
        "working_order_count",
        "unsubmitted_instruction_count",
        "status",
    }
    assert set(create_broker_proof["state_hashes"]) == {
        "account",
        "positions",
        "working_orders",
        "unsubmitted_instructions",
    }
    assert all(
        len(str(value)) == 64
        for value in (
            create_broker_proof["snapshot_hash"],
            create_broker_proof["contract_definitions_hash"],
            create_broker_proof["quotes_hash"],
            *create_broker_proof["state_hashes"].values(),
        )
    )
    create_nav_proof = approval_store.create_kwargs["strategy_nav_proof"]
    assert create_nav_proof == {
        "schema": "options_copilot.approval.strategy_nav_proof.v3",
        "content_hash": nav.content_hash,
        "authority_hash": nav.authority_hash,
        "contract_hash": nav.contract_hash,
        "ledger_head_hash": nav.ledger_head_hash,
        "strategy_nav_usd": "2000",
        "observed_account_nlv": "999999",
        "reconciliation_difference": "997999",
        "asof": nav.asof.isoformat(),
        "snapshot_payload": nav.hash_payload(),
    }

    confirmation = services.confirm_challenge(
        "challenge-1",
        {
            "challenge_response": "x" * 32,
            "risk_acknowledged": True,
            "second_confirmation": True,
            "confirmation_token": "CREATE_IBKR_REVIEW_ONLY",
        },
    )
    assert confirmation is not None
    assert confirmation["status"] == "PENDING_CODEX_BRIDGE"
    assert confirmation["status_url"] == "/api/approvals/approval-1"
    assert confirmation["order_submitted"] is False
    assert approval_store.confirm_kwargs is not None
    assert approval_store.confirm_kwargs["ranking_store"] is ranking_store
    confirm_broker_proof = approval_store.confirm_kwargs["broker_proof"]
    assert confirm_broker_proof["quote_batch_id"] == "batch-2"
    assert confirm_broker_proof["snapshot_hash"] != create_broker_proof["snapshot_hash"]
    assert confirm_broker_proof["quotes_hash"] != create_broker_proof["quotes_hash"]
    assert approval_store.confirm_kwargs["strategy_nav_proof"] == create_nav_proof
    assert broker_source.quote_calls == 2


def test_runtime_challenge_rejects_current_broker_state_or_leg_mismatch() -> None:
    nav = _nav_snapshot()
    contracts = _contracts()
    approval_store = _ApprovalPort()

    working_source = _BrokerSource(
        contracts, working_orders=({"order_id": "existing"},)
    )
    working = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(_ranking_payload(nav)),
            approval_store=approval_store,
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=BrokerSnapshotBuilder(working_source),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )
    with pytest.raises(ProposalApprovalConflict, match="working order"):
        working.create_rank_one_challenge("ranking-1", "candidate-1")

    mismatched_payload = _ranking_payload(nav)
    mismatched_payload["candidates"][0]["proposal_body"]["underlying"] = "QQQ"
    mismatch = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(mismatched_payload),
            approval_store=_ApprovalPort(),
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=BrokerSnapshotBuilder(_BrokerSource(contracts)),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )
    with pytest.raises(
        RankOneAuthorizationForbidden,
        match="INVALID_IMMUTABLE_CANDIDATE_UNDERLYING",
    ):
        mismatch.create_rank_one_challenge("ranking-1", "candidate-1")

    unavailable = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(_ranking_payload(nav)),
            approval_store=_ApprovalPort(),
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=_ProtocolPort(None),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )
    with pytest.raises(OptionsCopilotUnavailable, match="broker snapshot"):
        unavailable.create_rank_one_challenge("ranking-1", "candidate-1")


@pytest.mark.parametrize(
    ("mutation", "expected", "message"),
    (
        (
            "candidate_symbol",
            RankOneAuthorizationForbidden,
            "INVALID_IMMUTABLE_CANDIDATE_UNDERLYING",
        ),
        ("candidate_local_symbol", ProposalApprovalConflict, "local_symbol"),
        ("candidate_trading_class", ProposalApprovalConflict, "trading_class"),
        (
            "proposal_con_id",
            RankOneAuthorizationForbidden,
            "CANDIDATE_EVIDENCE_MANIFEST_INVALID",
        ),
        (
            "conflicting_proposal_symbol",
            RankOneAuthorizationForbidden,
            "INVALID_IMMUTABLE_CANDIDATE_UNDERLYING",
        ),
    ),
)
def test_runtime_rejects_optional_frozen_contract_identity_conflicts(
    mutation: str, expected: type[Exception], message: str
) -> None:
    nav = _nav_snapshot()
    payload = _ranking_payload(nav)
    row = payload["candidates"][0]
    if mutation == "candidate_symbol":
        row["candidate_body"]["symbol"] = "QQQ"
    elif mutation == "candidate_local_symbol":
        row["candidate_body"]["legs"][0]["local_symbol"] = "MISMATCH"
    elif mutation == "candidate_trading_class":
        row["candidate_body"]["legs"][0]["trading_class"] = "QQQ"
    elif mutation == "proposal_con_id":
        row["proposal_body"]["legs"][0]["con_id"] = 999
    else:
        row["proposal_body"]["symbol"] = "QQQ"
    contracts = _contracts()
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(payload),
            approval_store=_ApprovalPort(),
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=BrokerSnapshotBuilder(_BrokerSource(contracts)),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )

    with pytest.raises(expected, match=message):
        services.create_rank_one_challenge("ranking-1", "candidate-1")


@pytest.mark.parametrize(
    ("observed_nlv", "message"),
    (
        (None, "Strategy NAV authority changed"),
        (Decimal("999998"), "account NLV"),
    ),
)
def test_runtime_rejects_missing_or_mismatched_strategy_nav_broker_nlv(
    observed_nlv: Decimal | None,
    message: str,
) -> None:
    nav = _replace_nav(_nav_snapshot(), observed_account_nlv=observed_nlv)
    contracts = _contracts()
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(_ranking_payload(nav)),
            approval_store=_ApprovalPort(),
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=BrokerSnapshotBuilder(_BrokerSource(contracts)),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )

    with pytest.raises(ProposalApprovalConflict, match=message):
        services.create_rank_one_challenge("ranking-1", "candidate-1")


def test_runtime_confirmation_rejects_changed_nav_before_broker_reread() -> None:
    nav = _nav_snapshot()
    nav_source = _ProtocolPort(nav)
    contracts = _contracts()
    broker_source = _BrokerSource(contracts)
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(_ranking_payload(nav)),
            approval_store=_ApprovalPort(),
            strategy_nav_source=nav_source,
            broker_snapshot_builder=BrokerSnapshotBuilder(broker_source),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )
    services.create_rank_one_challenge("ranking-1", "candidate-1")
    nav_source.value = _replace_nav(nav, strategy_nav=Decimal("2001"))

    with pytest.raises(ProposalApprovalConflict, match="authority changed"):
        services.confirm_challenge(
            "challenge-1",
            {
                "challenge_response": "x" * 32,
                "risk_acknowledged": True,
                "second_confirmation": True,
                "confirmation_token": "CREATE_IBKR_REVIEW_ONLY",
            },
        )
    assert broker_source.quote_calls == 1


def test_runtime_rejects_future_strategy_nav_asof_without_freshness_cutoff() -> None:
    future = _replace_nav(
        _nav_snapshot(),
        asof=datetime.now(timezone.utc) + timedelta(minutes=1),
    )
    contracts = _contracts()
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(_ranking_payload(future)),
            approval_store=_ApprovalPort(),
            strategy_nav_source=_ProtocolPort(future),
            broker_snapshot_builder=BrokerSnapshotBuilder(_BrokerSource(contracts)),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )

    with pytest.raises(ProposalApprovalConflict, match="Strategy NAV"):
        services.create_rank_one_challenge("ranking-1", "candidate-1")


@pytest.mark.parametrize(
    ("error", "expected"),
    (
        (ApprovalChallengeRejected("stale authority"), ProposalApprovalConflict),
        (RuntimeError("sqlite corruption"), OptionsCopilotUnavailable),
    ),
)
def test_runtime_classifies_challenge_store_failures(
    error: Exception, expected: type[Exception]
) -> None:
    nav = _nav_snapshot()
    contracts = _contracts()
    approval_store = _ApprovalPort()
    approval_store.create_error = error
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(_ranking_payload(nav)),
            approval_store=approval_store,
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=BrokerSnapshotBuilder(_BrokerSource(contracts)),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )

    with pytest.raises(expected):
        services.create_rank_one_challenge("ranking-1", "candidate-1")


@pytest.mark.parametrize(
    ("error", "expected"),
    (
        (ApprovalChallengeRejected("expired"), ProposalApprovalConflict),
        (RuntimeError("sqlite corruption"), OptionsCopilotUnavailable),
    ),
)
def test_runtime_classifies_confirmation_store_failures(
    error: Exception, expected: type[Exception]
) -> None:
    nav = _nav_snapshot()
    contracts = _contracts()
    approval_store = _ApprovalPort()
    broker_source = _BrokerSource(contracts)
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(_ranking_payload(nav)),
            approval_store=approval_store,
            strategy_nav_source=_ProtocolPort(nav),
            broker_snapshot_builder=BrokerSnapshotBuilder(broker_source),
            options_evidence_acquisition=_OptionsEvidence(contracts),
        )
    )
    services.create_rank_one_challenge("ranking-1", "candidate-1")
    approval_store.confirm_error = error

    with pytest.raises(expected):
        services.confirm_challenge(
            "challenge-1",
            {
                "challenge_response": "x" * 32,
                "risk_acknowledged": True,
                "second_confirmation": True,
                "confirmation_token": "CREATE_IBKR_REVIEW_ONLY",
            },
        )
    assert broker_source.quote_calls == 2


def _candidate_evidence_manifest(
    *,
    cutoff_at: datetime,
    supporting: tuple[object, ...] = (),
    contradicting: tuple[object, ...] = (),
    candidate_id: str = "candidate-1",
    symbol: str = "SPY",
) -> dict[str, object]:
    candidate_body = _candidate_evidence_body(
        cutoff_at=cutoff_at,
        candidate_id=candidate_id,
        symbol=symbol,
    )
    return build_candidate_evidence_manifest(
        candidate_body,
        after_cost_expected_value=Decimal("10"),
        cutoff_at=cutoff_at,
        ranking_valid_until=cutoff_at + timedelta(minutes=10),
        now=cutoff_at,
        supporting=tuple(
            item for item in supporting if isinstance(item, Mapping)
        ),
        contradicting=tuple(
            item for item in contradicting if isinstance(item, Mapping)
        ),
    )


def _candidate_evidence_body(
    *,
    cutoff_at: datetime,
    candidate_id: str = "candidate-1",
    symbol: str = "SPY",
) -> dict[str, object]:
    contracts = _contracts()
    legs = []
    for index, item in enumerate(contracts):
        legs.append(
            {
                "contract_id_ex": item.contract_id_ex,
                "underlying": item.symbol,
                "security_type": "OPT",
                "expiration": item.expiration.isoformat(),
                "strike": format(item.strike, "f"),
                "right": "CALL" if item.right == "C" else "PUT",
                "multiplier": format(Decimal(item.multiplier), "f"),
                "currency": item.currency,
                "exchange": item.exchange,
                "local_symbol": item.local_symbol,
                "trading_class": item.trading_class,
                "con_id": item.contract_id,
                "side": "LONG" if index == 0 else "SHORT",
                "ratio": 1,
                "bid": "2.00" if index == 0 else "1.00",
                "ask": "2.10" if index == 0 else "1.10",
                "observed_at": cutoff_at.isoformat(),
            }
        )
    return {
        "candidate_id": candidate_id,
        "symbol": symbol,
        "structure": "DEBIT_VERTICAL",
        "legs": tuple(legs),
        "debit_usd": "100",
        "credit_usd": "0",
        "all_in_cost_usd": "100",
        "max_loss_usd": "100",
        "max_profit_usd": "100",
        "breakevens": ("100",),
        "liquidity_score": "8",
        "dte": 20,
        "broker_snapshot_hash": "4" * 64,
        "quote_batch_id": "batch-1",
        "secdef_hash": "5" * 64,
        "execution_cost_contract_version": "v1",
        "execution_cost_contract_hash": "7" * 64,
        "dte_exception_hash": None,
    }


def _evidence_reference(stored: object) -> dict[str, object]:
    return {
        "evidence_id": getattr(stored, "evidence_id"),
        "content_hash": getattr(stored, "content_hash"),
        "row_hash": getattr(stored, "row_hash"),
    }


def _ranking_with_manifest(
    nav: StrategyNavSnapshot, manifest: Mapping[str, object] | None
) -> dict[str, object]:
    cutoff_at = None
    if isinstance(manifest, Mapping) and isinstance(manifest.get("cutoff_at"), str):
        cutoff_at = datetime.fromisoformat(str(manifest["cutoff_at"]))
    payload = _ranking_payload(nav, cutoff_at=cutoff_at)
    manifests = {} if manifest is None else {"candidate-1": manifest}
    payload["immutable_inputs"] = {
        "input_hash": payload["input_hash"],
        "evidence_hash": payload["evidence_hash"],
        "broker_snapshot_hash": payload["broker_snapshot_hash"],
        "candidate_evidence_manifests": manifests,
    }
    return payload


def test_candidate_evidence_reads_only_frozen_manifest_and_verified_evidence_store(
    tmp_path,
) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=1)
    with EvidenceStore(tmp_path / "evidence.sqlite") as evidence_store:
        supporting = evidence_store.append(
            EvidenceRecord(
                identity="news-spy-supporting",
                kind="COMPANY_NEWS",
                symbol="SPY",
                provider="SEC",
                source_id="sec-1",
                published_at=cutoff - timedelta(minutes=5),
                first_seen_at=cutoff - timedelta(minutes=4),
                ingested_at=cutoff - timedelta(minutes=3),
                observed_at=cutoff - timedelta(minutes=2),
                payload={
                    "title": "Frozen supporting filing",
                    "summary": "Material event observed before ranking",
                    "url": "https://www.sec.gov/example?tracking=remove",
                },
            )
        ).evidence
        contradicting = evidence_store.append(
            EvidenceRecord(
                identity="news-spy-contradicting",
                kind="COMPANY_NEWS",
                symbol="SPY",
                provider="COMPANY_IR",
                source_id="ir-1",
                published_at=cutoff - timedelta(minutes=3),
                first_seen_at=cutoff - timedelta(minutes=2),
                ingested_at=cutoff - timedelta(minutes=1),
                observed_at=cutoff,
                payload={"title": "Frozen counter-evidence"},
            )
        ).evidence
        manifest = _candidate_evidence_manifest(
            cutoff_at=cutoff,
            supporting=(_evidence_reference(supporting),),
            contradicting=(_evidence_reference(contradicting),),
        )
        nav = _nav_snapshot()
        services = RuntimeServices(
            **_runtime_graph(
                ranking_store=_RankingPort(_ranking_with_manifest(nav, manifest)),
                evidence_store=evidence_store,
                strategy_nav_source=_ProtocolPort(nav),
            )
        )

        result = services.candidate_evidence("scan-1", "candidate-1")

        assert result["status"] == "READY"
        assert result["decision"] == "OBSERVATION_ONLY"
        assert result["approval_enabled"] is False
        assert result["manifest_hash"] == manifest["manifest_hash"]
        assert result["primary"][0]["record"] == {
            "symbol": "SPY",
            "broker_snapshot_hash": "4" * 64,
            "quote_snapshot_id": "batch-1",
            "complete": True,
        }
        assert result["supporting"][0]["evidence_id"] == supporting.evidence_id
        assert result["supporting"][0]["payload"]["title"] == (
            "Frozen supporting filing"
        )
        assert result["contradicting"][0]["evidence_id"] == (
            contradicting.evidence_id
        )


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        ("missing", "CANDIDATE_EVIDENCE_MANIFEST_MISSING"),
        ("manifest_hash", "CANDIDATE_EVIDENCE_MANIFEST_INVALID"),
        ("future_cutoff", "CANDIDATE_EVIDENCE_CUTOFF_INVALID"),
        ("candidate_symbol", "CANDIDATE_EVIDENCE_SYMBOL_MISMATCH"),
        ("reference_hash", "CANDIDATE_EVIDENCE_RECORD_HASH_MISMATCH"),
        ("future_evidence", "CANDIDATE_EVIDENCE_AFTER_CUTOFF"),
        ("evidence_symbol", "CANDIDATE_EVIDENCE_SYMBOL_MISMATCH"),
    ),
)
def test_candidate_evidence_manifest_failures_return_empty_observation_only_read_model(
    tmp_path, mutation: str, reason: str
) -> None:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=2)
    evidence_symbol = "SPX" if mutation == "evidence_symbol" else "SPY"
    first_seen = (
        cutoff + timedelta(seconds=1)
        if mutation == "future_evidence"
        else cutoff - timedelta(minutes=1)
    )
    with EvidenceStore(tmp_path / f"evidence-{mutation}.sqlite") as evidence_store:
        stored = evidence_store.append(
            EvidenceRecord(
                identity=f"evidence-{mutation}",
                kind="COMPANY_NEWS",
                symbol=evidence_symbol,
                provider="SEC",
                source_id=f"source-{mutation}",
                published_at=cutoff - timedelta(minutes=2),
                first_seen_at=first_seen,
                ingested_at=max(first_seen, cutoff - timedelta(seconds=30)),
                observed_at=max(first_seen, cutoff),
                payload={"title": "Frozen evidence"},
            )
        ).evidence
        reference = _evidence_reference(stored)
        if mutation == "reference_hash":
            reference["row_hash"] = "f" * 64
        manifest = _candidate_evidence_manifest(
            cutoff_at=(now + timedelta(minutes=1) if mutation == "future_cutoff" else cutoff),
            supporting=(reference,),
            symbol="SPX" if mutation == "candidate_symbol" else "SPY",
        )
        if mutation == "manifest_hash":
            manifest["manifest_hash"] = "f" * 64
        nav = _nav_snapshot()
        services = RuntimeServices(
            **_runtime_graph(
                ranking_store=_RankingPort(
                    _ranking_with_manifest(nav, None if mutation == "missing" else manifest)
                ),
                evidence_store=evidence_store,
                strategy_nav_source=_ProtocolPort(nav),
            )
        )

        result = services.candidate_evidence("scan-1", "candidate-1")

        assert result["status"] == "DEGRADED"
        assert result["decision"] == "NO_TRADE"
        assert result["decision_authority"] == "OBSERVATION_ONLY"
        assert result["approval_enabled"] is False
        assert result["primary"] == []
        assert result["supporting"] == []
        assert result["contradicting"] == []
        assert reason in result["reasons"]


def test_candidate_evidence_store_unavailable_fails_closed_without_latest_query() -> None:
    class BrokenEvidenceStore(_ProtocolPort):
        def verify_integrity(self) -> bool:
            raise RuntimeError("evidence ledger offline")

        def query(self, **_: object) -> object:
            raise AssertionError("candidate evidence must never query latest records")

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=1)
    nav = _nav_snapshot()
    services = RuntimeServices(
        **_runtime_graph(
            ranking_store=_RankingPort(
                _ranking_with_manifest(
                    nav, _candidate_evidence_manifest(cutoff_at=cutoff)
                )
            ),
            evidence_store=BrokenEvidenceStore(),
            strategy_nav_source=_ProtocolPort(nav),
        )
    )

    result = services.candidate_evidence("scan-1", "candidate-1")

    assert result["decision"] == "NO_TRADE"
    assert "CANDIDATE_EVIDENCE_STORE_UNAVAILABLE" in result["reasons"]
    assert result["primary"] == result["supporting"] == result["contradicting"] == []
