"""The named, read-only scheduler-slot to immutable-decision pipeline.

This module owns no approval, bridge, creator, instruction, or broker-write
dependency.  Its durable side effects are atomically journaled decisions and,
only after current authority is rechecked, an immutable ranking snapshot.
"""
from __future__ import annotations

import inspect
import logging
import threading
from time import perf_counter_ns
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from options_copilot.analytics.economic_gates import signed_economics_reasons

from options_copilot.decision.gates import (
    CandidateGateResult,
    GateBundle,
    GateId,
    GateLayerResult,
    GateRoutingContext,
    GateStatus,
    PipelineGateOutcome,
    RankingGateContext,
    SupportingInput,
    SupportingStatus,
    candidate_gate_key,
)
from options_copilot.gateway.broker_snapshot import AtomicBrokerSnapshot
from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.ranking.evidence_manifest import (
    CandidateEvidenceManifestError,
    build_candidate_evidence_manifest,
)
from options_copilot.ranking.portfolio import PortfolioAction
from options_copilot.ranking.joint import (
    JointRankingEngine,
    JointRankingSnapshot,
    build_joint_ranking_input_document,
)
from options_copilot.ranking.store import RankingStoreConflict
from options_copilot.research_allocation import (
    normalise_research_allocation_evidence,
)
from options_copilot.equity_pool.reference import normalize_equity_pool_reference
from options_copilot.option_pool.models import (
    normalize_equity_thesis_row,
    normalize_equity_theses,
)
from options_copilot.option_pool.service import FinalizedOptionPoolCandidate
from options_copilot.storage.canonical import canonical_hash, freeze_json, thaw_json, utc_datetime


_LOGGER = logging.getLogger(__name__)


class _OptionPoolRecorderUnexpectedError(RuntimeError):
    """Sanitized boundary for unexpected option-pool persistence failures."""


@dataclass(frozen=True, slots=True)
class _JointAccountContext:
    open_position_underlyings: tuple[str, ...]
    aggregate_open_risk_usd: Decimal
    concentration_by_underlying: Mapping[str, Decimal]


_REQUIRED_CANDIDATE_BODY_FIELDS = frozenset(
    {
        "candidate_id",
        "symbol",
        "structure",
        "legs",
        "terminal_scenarios",
        "debit_usd",
        "credit_usd",
        "all_in_cost_usd",
        "max_loss_usd",
        "max_profit_usd",
        "breakevens",
        "liquidity_score",
        "exit_plan",
        "dte",
        "strategy_nav_usd",
        "strategy_nav_hash",
        "strategy_nav_content_hash",
        "strategy_nav_contract_hash",
        "strategy_nav_ledger_head_hash",
        "strategy_nav_observed_account_nlv",
        "strategy_nav_reconciliation_difference",
        "strategy_nav_asof",
        "broker_snapshot_hash",
        "quote_batch_id",
        "secdef_hash",
        "evidence_hashes",
        "execution_cost_contract_version",
        "execution_cost_contract_hash",
        "policy_version",
        "policy_hash",
        "dte_exception_hash",
    }
)


@dataclass(frozen=True, slots=True)
class _CandidateBinding:
    candidate_body: Mapping[str, Any]
    candidate_hash: str
    proposal_body: Mapping[str, Any] | None
    supplied_proposal_hash: str | None
    supplied_evidence_inputs: object | None


@dataclass(frozen=True, slots=True)
class _PreRankGateState:
    binding: _CandidateBinding
    strategy_family: str
    authority_status: GateStatus
    authority_reasons: tuple[str, ...]
    event_status: GateStatus
    event_reasons: tuple[str, ...]
    liquidity_status: GateStatus
    liquidity_reasons: tuple[str, ...]
    risk_status: GateStatus
    risk_reasons: tuple[str, ...]
    economic_proof: Mapping[str, object] | None = None

    @property
    def eligible(self) -> bool:
        return all(
            status is GateStatus.PASS
            for status in (
                self.authority_status,
                self.event_status,
                self.liquidity_status,
                self.risk_status,
            )
        )


@dataclass(frozen=True, slots=True)
class PipelineResult:
    scan_run_id: str
    status: str
    ranking_snapshot_id: str | None
    reasons: tuple[str, ...]
    input_hash: str
    evidence_hash: str | None
    broker_snapshot_hash: str | None
    policy_version: str | None
    policy_hash: str | None
    current_policy_version: str | None
    current_policy_hash: str | None
    policy_authority_marker_hash: str | None
    cost_version: str | None
    cost_hash: str | None
    risk_contract_hash: str | None
    risk_authority_version: str | None
    risk_authority_marker_hash: str | None
    candidate_hashes: tuple[str, ...]
    ranking_basis_hashes: tuple[str, ...]
    ranking_snapshot_hash: str | None
    gate_bundle_hash: str
    funnel_trace: Mapping[str, object]
    result_hash: str

    def as_dict(self) -> dict[str, object]:
        return {
            "scan_run_id": self.scan_run_id,
            "status": self.status,
            "ranking_snapshot_id": self.ranking_snapshot_id,
            "reasons": self.reasons,
            "input_hash": self.input_hash,
            "evidence_hash": self.evidence_hash,
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "current_policy_version": self.current_policy_version,
            "current_policy_hash": self.current_policy_hash,
            "policy_authority_marker_hash": self.policy_authority_marker_hash,
            "cost_version": self.cost_version,
            "cost_hash": self.cost_hash,
            "risk_contract_hash": self.risk_contract_hash,
            "risk_authority_version": self.risk_authority_version,
            "risk_authority_marker_hash": self.risk_authority_marker_hash,
            "candidate_hashes": self.candidate_hashes,
            "ranking_basis_hashes": self.ranking_basis_hashes,
            "ranking_snapshot_hash": self.ranking_snapshot_hash,
            "gate_bundle_hash": self.gate_bundle_hash,
            "funnel_trace": self.funnel_trace,
            "result_hash": self.result_hash,
        }


class DecisionPipeline:
    """Constructor-injected P6 pipeline with a fixed, auditable call order.

    Ports may be ordinary callables or objects exposing the conventional
    ``run``/``acquire``/``generate`` method.  Keyword filtering allows the
    concrete P4 components and narrow test fakes to share the same seam.
    """

    def __init__(
        self,
        *,
        inputs: object,
        universe_funnel: object,
        broker_evidence: object,
        strategy_registry: object,
        strategy_generator: object,
        volatility_engine: object,
        scenario_engine: object,
        policy_resolver: object | None = None,
        risk_authority_resolver: object | None = None,
        cost_contract: object,
        eligibility_gate: object,
        portfolio_ranker: object,
        ranking_store: object,
        option_pool_recorder: object | None = None,
        joint_ranker: object | None = None,
        joint_ranking_required: bool = False,
        joint_account_context_resolver: object | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], int] | None = None,
    ) -> None:
        # All dependencies are deliberately explicit: no module discovery,
        # globals, implicit gateway creation, or writer authority is allowed.
        self.inputs = inputs
        self.universe_funnel = universe_funnel
        self.broker_evidence = broker_evidence
        self.strategy_registry = strategy_registry
        self.strategy_generator = strategy_generator
        self.volatility_engine = volatility_engine
        self.scenario_engine = scenario_engine
        self.policy_resolver = policy_resolver
        self.risk_authority_resolver = risk_authority_resolver
        self.cost_contract = cost_contract
        self.eligibility_gate = eligibility_gate
        self.portfolio_ranker = portfolio_ranker
        self.ranking_store = ranking_store
        self.option_pool_recorder = option_pool_recorder
        self.joint_ranker = joint_ranker or JointRankingEngine()
        if not isinstance(joint_ranking_required, bool):
            raise TypeError("joint_ranking_required must be a bool")
        self.joint_ranking_required = joint_ranking_required
        self.joint_account_context_resolver = joint_account_context_resolver
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic_clock = monotonic_clock or perf_counter_ns
        self._lock = threading.RLock()
        self._active_decision_records: tuple[Mapping[str, object], ...] = ()
        self._active_funnel_trace: Mapping[str, object] = {}
        self._active_gate_routing: GateRoutingContext | None = None
        self._active_gate_bundle: GateBundle | None = None
        self._active_stage = "IDLE"
        self._timing_scan_run_id: str | None = None
        self._timing_started_ns: int | None = None
        self._timing_stage_started_ns: int | None = None
        self._timing_stages: list[dict[str, object]] = []
        self._last_operational_timing: Mapping[str, object] = {}

    @property
    def last_operational_timing(self) -> Mapping[str, object]:
        """Return detached, non-authoritative timing for the last completed run."""

        return {
            **dict(self._last_operational_timing),
            "stages": tuple(
                dict(item)
                for item in self._last_operational_timing.get("stages", ())
                if isinstance(item, Mapping)
            ),
        }

    def _timing_point(self) -> int | None:
        """Read the observation clock without letting telemetry affect a decision."""

        try:
            value = self._monotonic_clock()
        except Exception:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return value

    def _begin_operational_timing(self, scan_run_id: str) -> None:
        point = self._timing_point()
        if point is None:
            self._timing_scan_run_id = None
            self._timing_started_ns = None
            self._timing_stage_started_ns = None
            self._timing_stages = []
            self._last_operational_timing = {}
            return
        self._timing_scan_run_id = scan_run_id
        self._timing_started_ns = point
        self._timing_stage_started_ns = point
        self._timing_stages = []
        self._last_operational_timing = {}

    def _set_stage(self, stage: str) -> None:
        point = self._timing_point()
        if point is None:
            self._disable_operational_timing()
        elif self._timing_started_ns is not None:
            if self._active_stage not in {"IDLE", "INITIALIZATION"}:
                self._append_stage_timing(self._active_stage, point)
            elif self._active_stage == "INITIALIZATION" and self._timing_stages == []:
                self._append_stage_timing(self._active_stage, point)
        self._active_stage = stage
        if self._timing_started_ns is not None:
            self._timing_stage_started_ns = point

    def _disable_operational_timing(self) -> None:
        self._timing_scan_run_id = None
        self._timing_started_ns = None
        self._timing_stage_started_ns = None
        self._timing_stages = []
        self._last_operational_timing = {}

    def _append_stage_timing(self, stage: str, point: int) -> None:
        started = self._timing_stage_started_ns
        if started is None or point < started:
            return
        self._timing_stages.append(
            {
                "stage": stage,
                "duration_ms": (point - started) // 1_000_000,
            }
        )

    def _finish_operational_timing(self) -> None:
        point = self._timing_point()
        if point is None:
            self._disable_operational_timing()
            return
        if self._active_stage not in {"IDLE"}:
            self._append_stage_timing(self._active_stage, point)
        started = self._timing_started_ns
        scan_run_id = self._timing_scan_run_id
        if started is None or scan_run_id is None or point < started:
            self._last_operational_timing = {}
        else:
            self._last_operational_timing = {
                "schema": "options_copilot.scan_operational_timing.v1",
                "scan_run_id": scan_run_id,
                "total_duration_ms": (point - started) // 1_000_000,
                "stages": tuple(dict(item) for item in self._timing_stages),
                "decision_authority": "OBSERVATION_ONLY",
                "affects_decision": False,
            }
        self._timing_scan_run_id = None
        self._timing_started_ns = None
        self._timing_stage_started_ns = None
        self._timing_stages = []

    def _option_pool_context(self) -> tuple[Mapping[str, object], Mapping[str, object]] | None:
        reference = self._active_funnel_trace.get("equity_pool_reference")
        theses = self._active_funnel_trace.get("equity_theses")
        if not isinstance(reference, Mapping):
            return None
        return reference, theses if isinstance(theses, Mapping) else {}

    def _record_option_dispositions(
        self,
        *,
        scan_run_id: str,
        observed_at: datetime,
        reason_codes: Sequence[object],
    ) -> bool:
        if self.option_pool_recorder is None:
            return True
        context = self._option_pool_context()
        if context is None:
            return True
        reference, theses = context
        try:
            _invoke(
                self.option_pool_recorder,
                ("capture_dispositions",),
                scan_run_id=scan_run_id,
                observed_at=observed_at,
                equity_pool_reference=reference,
                equity_theses=theses,
                reason_codes=tuple(reason_codes),
            )
        except (TypeError, ValueError):
            return False
        except Exception:
            unexpected_failure = True
        else:
            unexpected_failure = False
        if unexpected_failure:
            raise _OptionPoolRecorderUnexpectedError(
                "OPTION_POOL_RECORDER_UNEXPECTED_FAILURE"
            ) from None
        return True

    def _record_research_option_pool(
        self,
        *,
        scan_run_id: str,
        observed_at: datetime,
        finalists: Sequence[object],
    ) -> bool:
        """Persist thesis-bound exact identities before dynamic evidence Gates."""

        if self.option_pool_recorder is None:
            return True
        context = self._option_pool_context()
        if context is None:
            return True
        reference, theses = context
        try:
            candidates = _research_option_pool_candidate_documents(
                finalists,
                equity_pool_reference=reference,
                equity_theses=theses,
            )
            if finalists and not candidates:
                return False
            if not candidates:
                return True
            _invoke(
                self.option_pool_recorder,
                ("capture_research_candidates",),
                scan_run_id=f"{scan_run_id}.research",
                candidates=candidates,
                observed_at=observed_at,
                equity_pool_reference=reference,
                equity_theses=theses,
                reason_codes=(
                    "REGULAR_SESSION_RESEARCH_POOL_CAPTURED",
                    "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED",
                ),
                research_reason_codes=(
                    "REGULAR_SESSION_EXACT_IDENTITY_RESEARCH_ONLY",
                    "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED",
                ),
            )
        except (TypeError, ValueError):
            return False
        except Exception:
            raise _OptionPoolRecorderUnexpectedError(
                "OPTION_POOL_RECORDER_UNEXPECTED_FAILURE"
            ) from None
        return True

    def _record_final_option_pool(
        self,
        *,
        scan_run_id: str,
        observed_at: datetime,
        candidates: tuple[FinalizedOptionPoolCandidate, ...],
        reason_codes: Sequence[object],
    ) -> bool:
        if self.option_pool_recorder is None:
            return True
        context = self._option_pool_context()
        if context is None:
            return True
        reference, theses = context
        try:
            _invoke(
                self.option_pool_recorder,
                ("capture_generation", "capture"),
                scan_run_id=scan_run_id,
                candidates=candidates,
                observed_at=observed_at,
                equity_pool_reference=reference,
                equity_theses=theses,
                reason_codes=tuple(reason_codes),
            )
        except (TypeError, ValueError):
            return False
        except Exception:
            unexpected_failure = True
        else:
            unexpected_failure = False
        if unexpected_failure:
            raise _OptionPoolRecorderUnexpectedError(
                "OPTION_POOL_RECORDER_UNEXPECTED_FAILURE"
            ) from None
        return True

    def run_slot(self, scan_run_id: str, slot_at: datetime) -> Mapping[str, object]:
        """Run exactly one authority-free decision attempt for an acquired slot."""

        if not isinstance(scan_run_id, str) or not scan_run_id.strip():
            raise ValueError("scan_run_id must be nonblank")
        slot_at = utc_datetime(slot_at, field="slot_at")
        with self._lock:
            self._begin_operational_timing(scan_run_id.strip())
            try:
                self._active_decision_records = ()
                self._active_funnel_trace = {}
                self._active_gate_routing = None
                self._active_gate_bundle = None
                self._active_stage = "INITIALIZATION"
                try:
                    result = self._run(scan_run_id.strip(), slot_at)
                except RankingStoreConflict:
                    fallback = canonical_hash({"scan_run_id": scan_run_id, "slot_at": slot_at})
                    result = self._no_trade(scan_run_id.strip(), fallback, ("RANKING_BINDING_CONFLICT",))
                except _OptionPoolRecorderUnexpectedError:
                    _LOGGER.error(
                        "Decision-pipeline option-pool persistence failed for scan %s: "
                        "OPTION_POOL_RECORDER_UNEXPECTED_FAILURE",
                        scan_run_id.strip(),
                    )
                    raise
                except Exception:
                    # A malformed/tampered dependency document is an analytical
                    # rejection, never an exception that can bypass the decision
                    # plane's fail-closed contract.  Dependency exception text
                    # and tracebacks are deliberately excluded from diagnostics.
                    _LOGGER.warning(
                        "Decision-pipeline binding rejected for scan %s: "
                        "PIPELINE_BINDING_INVALID stage=%s",
                        scan_run_id.strip(),
                        self._active_stage,
                    )
                    fallback = canonical_hash({"scan_run_id": scan_run_id, "slot_at": slot_at})
                    result = self._no_trade(
                        scan_run_id.strip(),
                        fallback,
                        (
                            "PIPELINE_BINDING_INVALID",
                            f"PIPELINE_BINDING_STAGE_{self._active_stage}",
                        ),
                    )
                return result.as_dict()
            finally:
                self._finish_operational_timing()
                self._active_decision_records = ()
                self._active_funnel_trace = {}
                self._active_gate_routing = None
                self._active_gate_bundle = None
                self._active_stage = "IDLE"

    def _run(self, scan_run_id: str, slot_at: datetime) -> PipelineResult:
        now = utc_datetime(self._clock(), field="clock result")
        missing_authorities = tuple(
            reason
            for resolver, reason in (
                (self.policy_resolver, "POLICY_RESOLVER_REQUIRED"),
                (self.risk_authority_resolver, "RISK_AUTHORITY_RESOLVER_REQUIRED"),
            )
            if resolver is None
        )
        if missing_authorities:
            return self._no_trade(
                scan_run_id,
                canonical_hash({"scan_run_id": scan_run_id, "slot_at": slot_at}),
                missing_authorities,
            )
        if (
            not callable(getattr(self.ranking_store, "append_decisions", None))
            or not _port_accepts_keyword(
                self.ranking_store, ("append_snapshot", "append"), "decision_records"
            )
        ):
            return self._no_trade(
                scan_run_id,
                canonical_hash({"scan_run_id": scan_run_id, "slot_at": slot_at}),
                ("ATOMIC_DECISION_JOURNAL_REQUIRED",),
            )

        self._set_stage("AUTHORITY_RESOLUTION")
        resolved_policy = _resolve_once(self.policy_resolver, now=now)
        risk_authority = _resolve_once(
            self.risk_authority_resolver,
            now=now,
            current_policy=resolved_policy,
            resolved_policy=resolved_policy,
        )
        policy_version, policy_hash, policy_marker_hash = _policy_identity(
            resolved_policy
        )
        risk_version, risk_marker_hash, risk_contract_hash = _risk_identity(
            risk_authority
        )
        if not _is_unbound_normal_authority(risk_authority):
            return self._no_trade(
                scan_run_id,
                canonical_hash({"scan_run_id": scan_run_id, "slot_at": slot_at}),
                ("UNBOUND_A_GRADE_AUTHORITY",),
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        self._set_stage("INPUT_ACQUISITION")
        context = _document(_invoke(self.inputs, ("run", "load", "acquire"), scan_run_id=scan_run_id, slot_at=slot_at))
        self._active_funnel_trace = normalize_funnel_trace(
            context.get("funnel_trace"),
            scan_run_id=scan_run_id,
        )
        if "funnel_trace" in context:
            context = {
                **context,
                "funnel_trace": self._active_funnel_trace,
            }
        input_hash = _hash_or_value(context.get("input_hash")) or canonical_hash(
            {"scan_run_id": scan_run_id, "slot_at": slot_at, "inputs": context}
        )
        positions = tuple(context.get("positions", ()))
        if _option_position_open(positions):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("POSITION_MANAGEMENT_ONLY",),
            )

        # Fixed order: universe -> durable research identities -> broker evidence
        # -> registry/generator -> volatility/scenarios -> signed costs -> gates
        # -> rank -> final store.
        self._set_stage("UNIVERSE_FUNNEL")
        universe = _document(_invoke(
            self.universe_funnel, ("run",), scan_run_id=scan_run_id, slot_at=slot_at,
            **_document(context.get("universe", {})),
        ))
        universe_funnel_trace = normalize_funnel_trace(
            universe.get("funnel_trace"),
            scan_run_id=scan_run_id,
        )
        if (
            self._active_funnel_trace
            and universe_funnel_trace
            and self._active_funnel_trace != universe_funnel_trace
        ):
            raise ValueError("input and universe funnel traces do not match")
        if universe_funnel_trace:
            self._active_funnel_trace = universe_funnel_trace
        finalists = tuple(universe.get("finalists", ()))
        if not finalists:
            universe_reasons = _reasons(universe, "UNIVERSE_EMPTY")
            if universe_reasons == ("UNIVERSE_EMPTY",):
                universe_reasons = tuple(
                    sorted(
                        set(
                            universe_reasons
                            + _context_reason_codes(context)
                        )
                    )
                )
            return self._no_trade(scan_run_id, input_hash, universe_reasons)

        self._set_stage("OPTION_RESEARCH_POOL")
        if not self._record_research_option_pool(
            scan_run_id=scan_run_id,
            observed_at=now,
            finalists=finalists,
        ):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("OPTION_STRUCTURE_RESEARCH_POOL_UNAVAILABLE",),
            )

        self._set_stage("BROKER_EVIDENCE")
        evidence = _document(_invoke(
            self.broker_evidence, ("acquire", "run", "build"), scan_run_id=scan_run_id,
            slot_at=slot_at, universe=universe, context=context,
            current_policy=resolved_policy, resolved_policy=resolved_policy,
        ))
        broker_hash = _hash_or_value(evidence.get("broker_snapshot_hash", evidence.get("snapshot_hash")))
        evidence_hash = _hash_or_value(evidence.get("evidence_hash"))
        if broker_hash is None or evidence_hash is None:
            return self._no_trade(
                scan_run_id,
                input_hash,
                _reasons(evidence, "MISSING_BROKER_OR_EVIDENCE_BINDING"),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        # Acquisition may take several seconds and can observe quotes after the
        # run-start clock was captured.  Freeze the decision cutoff only after
        # the atomic broker evidence exists so fresh quotes are never treated
        # as future-dated, while every downstream gate shares one instant.
        now = utc_datetime(self._clock(), field="post-acquisition clock result")
        ranking_valid_until = now + timedelta(minutes=5)
        if not _resolver_is_current(self.policy_resolver, resolved_policy):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("AUTHORITY_HEAD_CHANGED",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        self._set_stage("GATE_ROUTING")
        routing = _build_gate_routing_context(
            scan_run_id=scan_run_id,
            cutoff_at=now,
            valid_until=ranking_valid_until,
            context=context,
            evidence=evidence,
        )
        self._active_gate_routing = routing
        if (
            routing.market_credit_gate.status is not GateStatus.PASS
            or not routing.market_credit_gate.allowed_strategy_families
        ):
            return self._no_trade(
                scan_run_id,
                input_hash,
                routing.market_credit_gate.reason_codes
                or ("MARKET_CREDIT_ROUTE_UNAVAILABLE",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        self._set_stage("STRATEGY_GENERATION")
        registered = _registry_finalists(
            self.strategy_registry, finalists=finalists, evidence=evidence, context=context,
        )
        generator_input = _route_finalists(
            tuple(registered.get("finalists", finalists)),
            routing,
        )
        if not generator_input:
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("MARKET_CREDIT_ROUTE_EMPTY",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        generated = _document(_invoke(
            self.strategy_generator, ("generate", "run"), finalists=generator_input,
            evidence=evidence, context=context, now=now, positions=positions,
            gate_routing_context=routing,
            allowed_strategy_families=routing.market_credit_gate.allowed_strategy_families,
            **_document(context.get("generation", {})), **evidence,
        ))
        candidates = _route_generated_candidates(
            tuple(generated.get("candidates", ())),
            routing,
        )
        if not candidates:
            self._record_option_dispositions(
                scan_run_id=scan_run_id,
                observed_at=now,
                reason_codes=_reasons(generated, "GENERATOR_EMPTY"),
            )
            return self._no_trade(scan_run_id, input_hash, _reasons(generated, "GENERATOR_EMPTY"), evidence_hash=evidence_hash, broker_hash=broker_hash)
        candidate_bindings, candidate_error = _bind_candidates(
            candidates,
            require_proposal=False,
        )
        if candidate_error is not None:
            return self._no_trade(
                scan_run_id,
                input_hash,
                (candidate_error,),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        frozen_candidates = tuple(item.candidate_body for item in candidate_bindings)

        self._set_stage("VOLATILITY")
        volatility = _document(_invoke(
            self.volatility_engine, ("evaluate", "run", "build"),
            raw=context.get("volatility", evidence), now=now, evidence=evidence, candidates=frozen_candidates,
        ))
        if not bool(volatility.get("eligible", False)):
            return self._no_trade(scan_run_id, input_hash, _reasons(volatility, "VOLATILITY_INELIGIBLE"), evidence_hash=evidence_hash, broker_hash=broker_hash)
        candidates, candidate_bindings = _filter_candidates_by_volatility(
            candidates,
            candidate_bindings,
            volatility,
        )
        if not candidates:
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("VOLATILITY_INELIGIBLE",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
            )
        frozen_candidates = tuple(
            item.candidate_body for item in candidate_bindings
        )
        volatility_hash = _hash_or_value(volatility.get("evidence_hash"))
        if volatility_hash is None:
            return self._no_trade(scan_run_id, input_hash, ("MISSING_VOLATILITY_BINDING",), evidence_hash=evidence_hash, broker_hash=broker_hash)
        evidence_hash = canonical_hash({"broker": evidence_hash, "volatility": volatility_hash})

        self._set_stage("SCENARIOS")
        scenarios = tuple(_document(_invoke(
            self.scenario_engine, ("evaluate_pre_cost", "run", "evaluate", "assess"),
            raw=_scenario_input(evidence, volatility, binding.candidate_body), now=now,
            resolved_policy=resolved_policy, risk_authority=risk_authority,
        )) for binding in candidate_bindings)
        scenario_disagrees = any(
            not _scenario_authority_matches(
                item,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
                risk_contract_hash=risk_contract_hash,
                strict=True,
            )
            for item in scenarios
        )
        scenario_survivors = tuple(
            index for index, item in enumerate(scenarios)
            if _value_text(item.get("action")) == "TRADE"
        )
        if (
            not scenario_survivors
            or policy_hash is None
            or policy_version is None
            or scenario_disagrees
        ):
            scenario_reasons = tuple(sorted({
                "SCENARIO_OR_POLICY_INELIGIBLE",
                *(
                    reason for item in scenarios
                    if _value_text(item.get("action")) != "TRADE"
                    for reason in _context_reason_codes(item)
                ),
            }))
            self._record_option_dispositions(
                scan_run_id=scan_run_id,
                observed_at=now,
                reason_codes=scenario_reasons,
            )
            return self._no_trade(
                scan_run_id,
                input_hash,
                scenario_reasons,
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_hash=policy_hash,
                policy_version=policy_version,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        if len(scenario_survivors) != len(scenarios):
            rejected = tuple(
                index for index in range(len(scenarios))
                if index not in scenario_survivors
            )
            self._active_decision_records = _scenario_decision_records(
                scan_run_id,
                tuple(candidate_bindings[index] for index in rejected),
                tuple(scenarios[index] for index in rejected),
            )
            if self._active_funnel_trace:
                self._active_funnel_trace = freeze_json({
                    **dict(self._active_funnel_trace),
                    "scenario_rejections": tuple({
                        "candidate_id": candidate_bindings[index].candidate_body["candidate_id"],
                        "reasons": _context_reason_codes(scenarios[index]),
                    } for index in rejected),
                })
            candidates = tuple(candidates[index] for index in scenario_survivors)
            candidate_bindings = tuple(candidate_bindings[index] for index in scenario_survivors)
            scenarios = tuple(scenarios[index] for index in scenario_survivors)

        finalized_candidates, candidate_bindings, finalization_error = (
            _finalize_scenario_candidates(
                candidates,
                candidate_bindings,
                scenarios,
            )
        )
        if finalization_error is not None:
            self._record_option_dispositions(
                scan_run_id=scan_run_id,
                observed_at=now,
                reason_codes=(finalization_error,),
            )
            return self._no_trade(
                scan_run_id,
                input_hash,
                (finalization_error,),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        frozen_candidates = tuple(
            item.candidate_body for item in candidate_bindings
        )
        self._active_decision_records += _scenario_decision_records(
            scan_run_id,
            candidate_bindings,
            scenarios,
        )

        self._set_stage("EXECUTION_COST")
        cost_resolution = _invoke(
            self.cost_contract, ("resolve", "apply", "run"), scan_run_id=scan_run_id,
            candidates=frozen_candidates, scenarios=scenarios, context=context, now=now,
            current_policy=resolved_policy, resolved_policy=resolved_policy,
        )
        costs = _document(cost_resolution)
        cost_hash = _hash_or_value(costs.get("cost_hash", costs.get("hash", costs.get("contract_hash"))))
        cost_version = _string_or_none(costs.get("cost_version", costs.get("version")))
        if cost_hash is None or cost_version is None:
            self._record_option_dispositions(
                scan_run_id=scan_run_id,
                observed_at=now,
                reason_codes=("MISSING_EXECUTION_COST_BINDING",),
            )
            return self._no_trade(scan_run_id, input_hash, ("MISSING_EXECUTION_COST_BINDING",), evidence_hash=evidence_hash, broker_hash=broker_hash, policy_hash=policy_hash, policy_version=policy_version)
        adjusted = _cost_adjusted(frozen_candidates, scenarios, costs, cost_hash, cost_version)
        if adjusted is None:
            self._record_option_dispositions(
                scan_run_id=scan_run_id,
                observed_at=now,
                reason_codes=("COST_OR_AFTER_COST_EV_INVALID",),
            )
            return self._no_trade(scan_run_id, input_hash, ("COST_OR_AFTER_COST_EV_INVALID",), evidence_hash=evidence_hash, broker_hash=broker_hash, policy_hash=policy_hash, policy_version=policy_version, cost_hash=cost_hash, cost_version=cost_version)

        self._set_stage("OPTION_POOL")
        option_pool_context = self._option_pool_context()
        option_pool_has_theses = bool(
            option_pool_context is not None
            and isinstance(option_pool_context[1].get("rows"), (list, tuple))
            and option_pool_context[1].get("rows")
        )
        if option_pool_context is None or (
            self.option_pool_recorder is None and not option_pool_has_theses
        ):
            finalized_option_pool_candidates: tuple[FinalizedOptionPoolCandidate, ...] = ()
        else:
            _, option_pool_theses = option_pool_context
            try:
                finalized_option_pool_candidates = _option_pool_candidate_documents(
                    candidate_bindings,
                    scenarios,
                    costs,
                    adjusted,
                    equity_theses=option_pool_theses,
                )
            except (TypeError, ValueError):
                return self._no_trade(
                    scan_run_id,
                    input_hash,
                    ("OPTION_STRUCTURE_POOL_UNAVAILABLE",),
                    evidence_hash=evidence_hash,
                    broker_hash=broker_hash,
                    policy_hash=policy_hash,
                    policy_version=policy_version,
                    cost_hash=cost_hash,
                    cost_version=cost_version,
                )

        nonpositive_after_cost_ev = any(
            value <= Decimal("0") for value in adjusted.values()
        )
        option_pool_reason_codes = tuple(generated.get("reason_codes", ()))
        if nonpositive_after_cost_ev:
            option_pool_reason_codes = tuple(dict.fromkeys((
                *option_pool_reason_codes,
                "CANDIDATE_AFTER_COST_EV_NONPOSITIVE",
            )))

        if not self._record_final_option_pool(
            scan_run_id=scan_run_id,
            observed_at=now,
            candidates=finalized_option_pool_candidates,
            reason_codes=option_pool_reason_codes,
        ):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("OPTION_STRUCTURE_POOL_UNAVAILABLE",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_hash=policy_hash,
                policy_version=policy_version,
                cost_hash=cost_hash,
                cost_version=cost_version,
            )

        positive_indexes = tuple(
            index
            for index, binding in enumerate(candidate_bindings)
            if adjusted[str(binding.candidate_body["candidate_id"])] > Decimal("0")
        )
        if not positive_indexes:
            if self._active_funnel_trace and finalized_option_pool_candidates:
                self._active_funnel_trace = freeze_json(
                    {
                        **dict(self._active_funnel_trace),
                        "finalized_option_candidates": tuple(
                            {
                                "candidate_id": str(candidate.payload["candidate_id"]),
                                "candidate_hash": candidate.candidate_hash,
                                "payload": thaw_json(candidate.payload),
                            }
                            for candidate in finalized_option_pool_candidates
                        ),
                    }
                )
                assert isinstance(self._active_funnel_trace, Mapping)
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("CANDIDATE_AFTER_COST_EV_NONPOSITIVE",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        if nonpositive_after_cost_ev:
            finalized_candidates = tuple(
                finalized_candidates[index] for index in positive_indexes
            )
            candidate_bindings = tuple(
                candidate_bindings[index] for index in positive_indexes
            )
            scenarios = tuple(scenarios[index] for index in positive_indexes)
            adjusted = {
                str(binding.candidate_body["candidate_id"]): adjusted[
                    str(binding.candidate_body["candidate_id"])
                ]
                for binding in candidate_bindings
            }
            frozen_candidates = tuple(
                binding.candidate_body for binding in candidate_bindings
            )
            positive_ids = set(adjusted)
            finalized_option_pool_candidates = tuple(
                candidate
                for candidate in finalized_option_pool_candidates
                if str(candidate.payload["candidate_id"]) in positive_ids
            )

        if self._active_funnel_trace and finalized_option_pool_candidates:
            self._active_funnel_trace = freeze_json(
                {
                    **dict(self._active_funnel_trace),
                    "finalized_option_candidates": tuple(
                        {
                            "candidate_id": str(candidate.payload["candidate_id"]),
                            "candidate_hash": candidate.candidate_hash,
                            "payload": thaw_json(candidate.payload),
                        }
                        for candidate in finalized_option_pool_candidates
                    ),
                }
            )
            assert isinstance(self._active_funnel_trace, Mapping)

        self._set_stage("PROPOSAL_BINDING")
        if any(binding.proposal_body is None for binding in candidate_bindings):
            candidate_bindings, proposal_construction_error = _bind_candidates(
                finalized_candidates,
            )
            if proposal_construction_error is not None:
                return self._no_trade(
                    scan_run_id,
                    input_hash,
                    (proposal_construction_error,),
                    evidence_hash=evidence_hash,
                    broker_hash=broker_hash,
                    policy_version=policy_version,
                    policy_hash=policy_hash,
                    policy_marker_hash=policy_marker_hash,
                    cost_version=cost_version,
                    cost_hash=cost_hash,
                    risk_contract_hash=risk_contract_hash,
                    risk_version=risk_version,
                    risk_marker_hash=risk_marker_hash,
                )
        proposal_error = _proposal_binding_error(
            candidate_bindings,
            scenarios,
            adjusted,
        )
        if proposal_error is not None:
            return self._no_trade(
                scan_run_id,
                input_hash,
                (proposal_error,),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        self._set_stage("ELIGIBILITY_GATES")
        gate = _document(_invoke(
            self.eligibility_gate, ("evaluate", "assess", "run"), candidates=frozen_candidates,
            scenarios=scenarios, costs=costs, evidence=evidence, universe=universe, now=now,
        ))
        gate_states = _pre_rank_gate_states(
            candidate_bindings,
            gate=gate,
            context=context,
            evidence=evidence,
            routing=routing,
            broker_snapshot_hash=broker_hash,
        )
        gate_states = _bind_economic_gate_states(
            gate_states, resolved_policy=resolved_policy, costs=costs,
            adjusted=adjusted, policy_hash=policy_hash, policy_marker_hash=policy_marker_hash,
            cost_hash=cost_hash,
        )
        if self.joint_ranking_required and (
            option_pool_context is None or not finalized_option_pool_candidates
        ):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("JOINT_RANKING_EVIDENCE_REQUIRED",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        joint_snapshot: JointRankingSnapshot | None = None
        if finalized_option_pool_candidates and option_pool_context is not None:
            gate_reasons = {
                str(state.binding.candidate_body["candidate_id"]): tuple(
                    dict.fromkeys(
                        state.authority_reasons
                        + state.event_reasons
                        + state.liquidity_reasons
                        + state.risk_reasons
                    )
                )
                for state in gate_states
            }
            if self.joint_account_context_resolver is None:
                account_context, account_reason = _joint_account_context(
                    candidates=finalized_option_pool_candidates,
                    gate=gate,
                    evidence=evidence,
                    broker_snapshot_hash=broker_hash,
                )
            else:
                account_context, account_reason = _resolved_joint_account_context(
                    self.joint_account_context_resolver,
                    candidates=finalized_option_pool_candidates,
                    gate=gate,
                    evidence=evidence,
                    broker_snapshot_hash=broker_hash,
                )
            if account_context is None:
                return self._no_trade(
                    scan_run_id,
                    input_hash,
                    (account_reason or "JOINT_ACCOUNT_CONTEXT_UNAVAILABLE",),
                    evidence_hash=evidence_hash,
                    broker_hash=broker_hash,
                    policy_version=policy_version,
                    policy_hash=policy_hash,
                    policy_marker_hash=policy_marker_hash,
                    cost_version=cost_version,
                    cost_hash=cost_hash,
                    risk_contract_hash=risk_contract_hash,
                    risk_version=risk_version,
                    risk_marker_hash=risk_marker_hash,
                )
            strategy_nav_hash = (
                _strategy_nav_authority_hash(context=context, evidence=evidence) or ""
            )
            joint_kwargs = {
                "candidates": finalized_option_pool_candidates,
                "scan_run_id": scan_run_id,
                "now": now,
                "equity_theses": option_pool_context[1],
                "broker_snapshot_hash": broker_hash,
                "strategy_nav_hash": strategy_nav_hash,
                "gate_reasons_by_candidate": gate_reasons,
                "open_position_underlyings": account_context.open_position_underlyings,
                "aggregate_open_risk_usd": account_context.aggregate_open_risk_usd,
                "concentration_by_underlying": account_context.concentration_by_underlying,
                "limit": 10,
            }
            try:
                joint_raw = _invoke(
                    self.joint_ranker,
                    ("rank",),
                    **joint_kwargs,
                )
                joint_input = build_joint_ranking_input_document(**joint_kwargs)
            except (TypeError, ValueError):
                joint_raw = None
                joint_input = None
            if isinstance(joint_raw, JointRankingSnapshot):
                finalized_hashes = {
                    str(candidate.payload["candidate_id"]): candidate.candidate_hash
                    for candidate in finalized_option_pool_candidates
                }
                joint_rows = joint_raw.executable + joint_raw.research_watchlist
                if (
                    joint_raw.scan_run_id != scan_run_id
                    or joint_raw.broker_snapshot_hash != broker_hash
                    or joint_raw.strategy_nav_hash != strategy_nav_hash
                    or set(finalized_hashes) != {row.candidate_id for row in joint_rows}
                    or any(
                        finalized_hashes.get(row.candidate_id) != row.candidate_hash
                        for row in joint_rows
                    )
                ):
                    joint_raw = None
            if isinstance(joint_raw, JointRankingSnapshot) and isinstance(
                joint_input, Mapping
            ):
                joint_snapshot = joint_raw
                self._active_funnel_trace = freeze_json(
                    {
                        **dict(self._active_funnel_trace),
                        "joint_ranking_input": joint_input,
                        "joint_ranking": joint_snapshot.as_dict(),
                    }
                )
                assert isinstance(self._active_funnel_trace, Mapping)
            else:
                return self._no_trade(
                    scan_run_id,
                    input_hash,
                    ("JOINT_RANKING_UNAVAILABLE",),
                    evidence_hash=evidence_hash,
                    broker_hash=broker_hash,
                    policy_version=policy_version,
                    policy_hash=policy_hash,
                    policy_marker_hash=policy_marker_hash,
                    cost_version=cost_version,
                    cost_hash=cost_hash,
                    risk_contract_hash=risk_contract_hash,
                    risk_version=risk_version,
                    risk_marker_hash=risk_marker_hash,
                )
        executable_ids = (
            {row.candidate_id for row in joint_snapshot.executable}
            if joint_snapshot is not None
            else set()
        )
        candidate_bindings = tuple(
            state.binding
            for state in gate_states
            if state.eligible
            and (
                joint_snapshot is None
                or str(state.binding.candidate_body["candidate_id"]) in executable_ids
            )
        )
        if not candidate_bindings:
            reasons = tuple(
                sorted(
                    {
                        reason
                        for state in gate_states
                        for reason in (
                            state.authority_reasons
                            + state.event_reasons
                            + state.liquidity_reasons
                            + state.risk_reasons
                        )
                    }
                )
            )
            if not reasons and joint_snapshot is not None:
                reasons = tuple(
                    sorted(
                        {
                            reason
                            for row in joint_snapshot.research_watchlist
                            for reason in row.reason_codes
                        }
                    )
                )
            reasons = reasons or _reasons(gate, "HARD_ELIGIBILITY_REJECTED")
            self._activate_gate_bundle(
                _build_gate_bundle(
                    scan_run_id=scan_run_id,
                    cutoff_at=now,
                    valid_until=ranking_valid_until,
                    pipeline_input_hash=input_hash,
                    routing=routing,
                    states=gate_states,
                    ranked=(),
                    outcome=PipelineGateOutcome.NO_TRADE,
                    run_reason_codes=reasons,
                    context=context,
                    evidence=evidence,
                )
            )
            return self._no_trade(
                scan_run_id,
                input_hash,
                reasons,
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_hash=policy_hash,
                policy_version=policy_version,
                cost_hash=cost_hash,
                cost_version=cost_version,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        self._set_stage("RANKING_PREPARATION")
        manifest_references = context.get("candidate_evidence_references", {})
        if joint_snapshot is not None and isinstance(manifest_references, Mapping):
            all_finalized_ids = {
                str(candidate.payload["candidate_id"])
                for candidate in finalized_option_pool_candidates
            }
            if any(str(candidate_id) not in all_finalized_ids for candidate_id in manifest_references):
                manifest_references = None
            else:
                manifest_references = {
                    candidate_id: value
                    for candidate_id, value in manifest_references.items()
                    if str(candidate_id) in executable_ids
                }
        candidate_manifests, manifest_error = _candidate_evidence_manifests(
            candidate_bindings,
            adjusted,
            manifest_references,
            cutoff_at=now,
            ranking_valid_until=ranking_valid_until,
        )
        if manifest_error is not None:
            return self._no_trade(
                scan_run_id,
                input_hash,
                (manifest_error,),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        evidence_inputs = {
            "input_hash": input_hash,
            "evidence_hash": evidence_hash,
            "broker_snapshot_hash": broker_hash,
            "volatility_evidence_hash": volatility_hash,
            "candidate_evidence_manifests": candidate_manifests,
        }
        if joint_snapshot is not None:
            evidence_inputs["joint_ranking"] = joint_snapshot.as_dict()
        if self._active_funnel_trace:
            evidence_inputs["funnel_trace"] = self._active_funnel_trace
        ranked_inputs, candidate_error = _ranking_inputs(
            candidate_bindings,
            adjusted,
            _gate_for_candidates(gate, candidate_bindings),
            policy_version=policy_version,
            policy_hash=policy_hash,
            policy_marker_hash=policy_marker_hash,
            cost_version=cost_version,
            cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
            evidence_inputs=evidence_inputs,
            joint_ranking=joint_snapshot,
        )
        if candidate_error is not None:
            return self._no_trade(
                scan_run_id,
                input_hash,
                (candidate_error,),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        risk_authority, risk_binding_error = _resolve_bound_risk_authority(
            self.risk_authority_resolver,
            initial_authority=risk_authority,
            candidates=ranked_inputs,
            now=now,
            resolved_policy=resolved_policy,
            policy_version=policy_version,
            policy_hash=policy_hash,
            policy_marker_hash=policy_marker_hash,
            cost_version=cost_version,
            cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
        )
        if risk_binding_error is not None:
            return self._no_trade(
                scan_run_id,
                input_hash,
                (risk_binding_error,),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        risk_version, risk_marker_hash, resolved_risk_contract_hash = _risk_identity(
            risk_authority
        )
        if resolved_risk_contract_hash != risk_contract_hash:
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("RISK_AUTHORITY_BINDING_MISMATCH",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        self._set_stage("PORTFOLIO_RANKING")
        ranking = _invoke(
            self.portfolio_ranker,
            ("rank",),
            candidates=ranked_inputs,
            limit=10,
            current_policy=resolved_policy,
            risk_authority=risk_authority,
            cost_version=cost_version,
            cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
            evidence_inputs=evidence_inputs,
        )
        ranking_doc = _document(ranking)
        ranked = tuple(ranking_doc.get("candidates", ()))
        if self._active_funnel_trace:
            self._active_funnel_trace = freeze_json(
                {
                    **dict(self._active_funnel_trace),
                    "ranked_count": len(ranked),
                }
            )
            assert isinstance(self._active_funnel_trace, Mapping)
            evidence_inputs["funnel_trace"] = self._active_funnel_trace
        governance_evidence = tuple(ranking_doc.get("governance_evidence", ()))
        action = _value_text(ranking_doc.get("action"))
        if action != PortfolioAction.TRADE.value or not ranked:
            reasons = _reasons(ranking_doc, "PORTFOLIO_EMPTY")
            self._activate_gate_bundle(
                _build_gate_bundle(
                    scan_run_id=scan_run_id,
                    cutoff_at=now,
                    valid_until=ranking_valid_until,
                    pipeline_input_hash=input_hash,
                    routing=routing,
                    states=gate_states,
                    ranked=(),
                    outcome=PipelineGateOutcome.NO_TRADE,
                    run_reason_codes=reasons,
                    context=context,
                    evidence=evidence,
                )
            )
            return self._no_trade(
                scan_run_id,
                input_hash,
                reasons,
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_hash=policy_hash,
                policy_version=policy_version,
                cost_hash=cost_hash,
                cost_version=cost_version,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        if _is_a_grade_authority(risk_authority) and not _a_grade_binding_matches(
            risk_authority,
            proposal_hash=_hash_or_value(_document(ranked[0]).get("proposal_hash")),
            candidate_hash=_hash_or_value(_document(ranked[0]).get("candidate_hash")),
            ranking_basis_hash=_hash_or_value(
                _document(ranked[0]).get("ranking_basis_hash")
            ),
            policy_version=policy_version,
            policy_hash=policy_hash,
            policy_marker_hash=policy_marker_hash,
            cost_version=cost_version,
            cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
        ):
            reasons = ("A_GRADE_RANK_ONE_BINDING_MISMATCH",)
            self._activate_gate_bundle(
                _build_gate_bundle(
                    scan_run_id=scan_run_id,
                    cutoff_at=now,
                    valid_until=ranking_valid_until,
                    pipeline_input_hash=input_hash,
                    routing=routing,
                    states=gate_states,
                    ranked=(),
                    outcome=PipelineGateOutcome.NO_TRADE,
                    run_reason_codes=reasons,
                    context=context,
                    evidence=evidence,
                )
            )
            return self._no_trade(
                scan_run_id,
                input_hash,
                reasons,
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )

        self._set_stage("POST_RANK_BINDING")
        gate_bundle = _build_gate_bundle(
            scan_run_id=scan_run_id,
            cutoff_at=now,
            valid_until=ranking_valid_until,
            pipeline_input_hash=input_hash,
            routing=routing,
            states=gate_states,
            ranked=ranked,
            outcome=PipelineGateOutcome.TRADE,
            run_reason_codes=(),
            context=context,
            evidence=evidence,
        )
        self._activate_gate_bundle(gate_bundle)
        evidence_inputs = {
            **evidence_inputs,
            "gate_bundle_hash": gate_bundle.gate_bundle_hash,
            "gate_bundle": gate_bundle.as_dict(),
        }

        try:
            rows = _finalized_store_rows(
                ranked,
                candidate_bindings,
                ranked_inputs,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                evidence_inputs=evidence_inputs,
                risk_authority=risk_authority,
                ranked=True,
            )
            governance_rows = _finalized_store_rows(
                governance_evidence,
                candidate_bindings,
                ranked_inputs,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                evidence_inputs=evidence_inputs,
                risk_authority=risk_authority,
                ranked=False,
            )
        except (TypeError, ValueError):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("POST_RANK_PROPOSAL_BINDING_INVALID",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        self._set_stage("AUTHORITY_REVALIDATION")
        if _is_a_grade_authority(risk_authority):
            risk_authority, final_risk_error = _resolve_final_rank_authority(
                self.risk_authority_resolver,
                current_authority=risk_authority,
                rank_one=rows[0],
                now=now,
                resolved_policy=resolved_policy,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
            )
            if final_risk_error is not None:
                return self._no_trade(
                    scan_run_id,
                    input_hash,
                    (final_risk_error,),
                    evidence_hash=evidence_hash,
                    broker_hash=broker_hash,
                    policy_version=policy_version,
                    policy_hash=policy_hash,
                    policy_marker_hash=policy_marker_hash,
                    cost_version=cost_version,
                    cost_hash=cost_hash,
                    risk_contract_hash=risk_contract_hash,
                    risk_version=risk_version,
                    risk_marker_hash=risk_marker_hash,
                )
            risk_version, risk_marker_hash, resolved_risk_contract_hash = (
                _risk_identity(risk_authority)
            )
            if resolved_risk_contract_hash != risk_contract_hash:
                return self._no_trade(
                    scan_run_id,
                    input_hash,
                    ("RANKING_GATE_BUNDLE_MISMATCH",),
                    evidence_hash=evidence_hash,
                    broker_hash=broker_hash,
                    policy_version=policy_version,
                    policy_hash=policy_hash,
                    policy_marker_hash=policy_marker_hash,
                    cost_version=cost_version,
                    cost_hash=cost_hash,
                    risk_contract_hash=risk_contract_hash,
                    risk_version=risk_version,
                    risk_marker_hash=risk_marker_hash,
                )
        if (
            not _resolver_is_current(self.policy_resolver, resolved_policy)
            or not _resolver_is_current(self.risk_authority_resolver, risk_authority)
        ):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("AUTHORITY_HEAD_CHANGED",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        if not _resolver_is_current(self.cost_contract, cost_resolution):
            return self._no_trade(
                scan_run_id,
                input_hash,
                ("EXECUTION_COST_HEAD_CHANGED",),
                evidence_hash=evidence_hash,
                broker_hash=broker_hash,
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                risk_version=risk_version,
                risk_marker_hash=risk_marker_hash,
            )
        self._set_stage("IMMUTABLE_PERSISTENCE")
        snapshot = _invoke(
            self.ranking_store, ("append_snapshot", "append"), scan_run_id=scan_run_id,
            input_hash=input_hash, evidence_hash=evidence_hash,
            broker_snapshot_hash=broker_hash, policy_version=policy_version, policy_hash=policy_hash,
            policy_authority_marker_hash=policy_marker_hash,
            cost_version=cost_version, cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
            risk_authority_version=risk_version,
            risk_authority_marker_hash=risk_marker_hash,
            candidates=rows, governance_evidence=governance_rows,
            policy_resolver=self.policy_resolver,
            risk_authority_resolver=self.risk_authority_resolver,
            resolved_policy=resolved_policy,
            risk_authority=risk_authority,
            decision_records=self._decision_records_with_gate_bundle(),
            valid_until=ranking_valid_until, now=now,
        )
        snap = _document(snapshot)
        snapshot_id = _string_or_none(snap.get("ranking_snapshot_id"))
        snapshot_hash = _hash_or_value(snap.get("snapshot_hash"))
        if snapshot_id is None or snapshot_hash is None:
            return self._no_trade(scan_run_id, input_hash, ("RANKING_STORE_BINDING_INVALID",), evidence_hash=evidence_hash, broker_hash=broker_hash, policy_hash=policy_hash, policy_version=policy_version, cost_hash=cost_hash, cost_version=cost_version)
        candidate_hashes = tuple(str(row["candidate_hash"]) for row in rows)
        basis_hashes = tuple(str(row["ranking_basis_hash"]) for row in rows)
        return self._result(
            scan_run_id,
            "TRADE",
            snapshot_id,
            (),
            input_hash,
            evidence_hash,
            broker_hash,
            policy_version,
            policy_hash,
            policy_marker_hash,
            cost_version,
            cost_hash,
            risk_contract_hash,
            risk_version,
            risk_marker_hash,
            candidate_hashes,
            basis_hashes,
            snapshot_hash,
        )

    def _no_trade(
        self,
        scan_run_id: str,
        input_hash: str,
        reasons: tuple[str, ...],
        *,
        evidence_hash: str | None = None,
        broker_hash: str | None = None,
        policy_version: str | None = None,
        policy_hash: str | None = None,
        policy_marker_hash: str | None = None,
        cost_version: str | None = None,
        cost_hash: str | None = None,
        risk_contract_hash: str | None = None,
        risk_version: str | None = None,
        risk_marker_hash: str | None = None,
    ) -> PipelineResult:
        normalized_reasons = tuple(sorted(set(reasons)))
        self._ensure_no_trade_gate_bundle(
            scan_run_id=scan_run_id,
            input_hash=input_hash,
            reasons=normalized_reasons,
        )
        append_decisions = getattr(self.ranking_store, "append_decisions", None)
        if not callable(append_decisions):
            # Legacy P5 stores remain usable for read-only research, but an
            # unjournaled result can never become a ranking or approval row.
            normalized_reasons = tuple(
                sorted(set(normalized_reasons + ("DECISION_STORE_UNAVAILABLE_READ_ONLY",)))
            )
            return self._result(
                scan_run_id,
                "NO_TRADE",
                None,
                normalized_reasons,
                input_hash,
                evidence_hash,
                broker_hash,
                policy_version,
                policy_hash,
                policy_marker_hash,
                cost_version,
                cost_hash,
                risk_contract_hash,
                risk_version,
                risk_marker_hash,
                (),
                (),
                None,
            )

        result = self._result(
            scan_run_id,
            "NO_TRADE",
            None,
            normalized_reasons,
            input_hash,
            evidence_hash,
            broker_hash,
            policy_version,
            policy_hash,
            policy_marker_hash,
            cost_version,
            cost_hash,
            risk_contract_hash,
            risk_version,
            risk_marker_hash,
            (),
            (),
            None,
        )
        records = self._decision_records_with_gate_bundle() + (
            {"record_type": "NO_TRADE", "record": result.as_dict()},
        )
        try:
            _invoke(
                self.ranking_store,
                ("append_decisions",),
                scan_run_id=scan_run_id,
                records=records,
                now=utc_datetime(self._clock(), field="clock result"),
            )
        except Exception:
            return self._result(
                scan_run_id,
                "NO_TRADE",
                None,
                tuple(sorted(set(normalized_reasons + ("DECISION_PERSISTENCE_FAILED_READ_ONLY",)))),
                input_hash,
                evidence_hash,
                broker_hash,
                policy_version,
                policy_hash,
                policy_marker_hash,
                cost_version,
                cost_hash,
                risk_contract_hash,
                risk_version,
                risk_marker_hash,
                (),
                (),
                None,
            )
        return result

    def _activate_gate_bundle(self, bundle: GateBundle) -> None:
        if bundle.scan_run_id == "":
            raise ValueError("Gate bundle scan_run_id must be nonblank")
        self._active_gate_bundle = bundle

    def _decision_records_with_gate_bundle(
        self,
    ) -> tuple[Mapping[str, object], ...]:
        bundle = self._active_gate_bundle
        if bundle is None:
            return self._active_decision_records
        return self._active_decision_records + (
            {
                "record_type": f"GATE_BUNDLE_{bundle.outcome.value}",
                "record": bundle.append_payload(),
            },
        )

    def _ensure_no_trade_gate_bundle(
        self,
        *,
        scan_run_id: str,
        input_hash: str,
        reasons: tuple[str, ...],
    ) -> None:
        current = self._active_gate_bundle
        if current is not None and current.outcome is PipelineGateOutcome.NO_TRADE:
            return
        cutoff = utc_datetime(self._clock(), field="clock result")
        valid_until = cutoff + timedelta(minutes=5)
        routing = self._active_gate_routing
        if routing is None or routing.scan_run_id != scan_run_id:
            route_gate = GateLayerResult.build(
                gate_id=GateId.MARKET_CREDIT_REGIME,
                status=GateStatus.UNAVAILABLE,
                observed_at=cutoff,
                input_payload={
                    "scan_run_id": scan_run_id,
                    "pipeline_input_hash": input_hash,
                    "reason_codes": reasons,
                },
                reason_codes=reasons or ("PIPELINE_NO_TRADE",),
            )
            routing = GateRoutingContext.build(
                scan_run_id=scan_run_id,
                cutoff_at=cutoff,
                valid_until=valid_until,
                market_credit_gate=route_gate,
            )
            self._active_gate_routing = routing
        else:
            cutoff = routing.cutoff_at
            valid_until = routing.valid_until
        self._active_gate_bundle = GateBundle.build(
            scan_run_id=scan_run_id,
            cutoff_at=cutoff,
            valid_until=valid_until,
            pipeline_input_hash=input_hash,
            routing_context=routing,
            candidates=(),
            outcome=PipelineGateOutcome.NO_TRADE,
            run_reason_codes=reasons or ("PIPELINE_NO_TRADE",),
        )

    def _result(self, scan_run_id: str, status: str, ranking_snapshot_id: str | None, reasons: tuple[str, ...], input_hash: str, evidence_hash: str | None, broker_snapshot_hash: str | None, policy_version: str | None, policy_hash: str | None, policy_authority_marker_hash: str | None, cost_version: str | None, cost_hash: str | None, risk_contract_hash: str | None, risk_authority_version: str | None, risk_authority_marker_hash: str | None, candidate_hashes: tuple[str, ...], ranking_basis_hashes: tuple[str, ...], snapshot_hash: str | None) -> PipelineResult:
        gate_bundle_hash = (
            self._active_gate_bundle.gate_bundle_hash
            if self._active_gate_bundle is not None
            else canonical_hash({"scan_run_id": scan_run_id, "status": status})
        )
        payload = {"scan_run_id": scan_run_id, "status": status, "ranking_snapshot_id": ranking_snapshot_id, "reasons": reasons, "input_hash": input_hash, "evidence_hash": evidence_hash, "broker_snapshot_hash": broker_snapshot_hash, "policy_version": policy_version, "policy_hash": policy_hash, "current_policy_version": policy_version, "current_policy_hash": policy_hash, "policy_authority_marker_hash": policy_authority_marker_hash, "cost_version": cost_version, "cost_hash": cost_hash, "risk_contract_hash": risk_contract_hash, "risk_authority_version": risk_authority_version, "risk_authority_marker_hash": risk_authority_marker_hash, "candidate_hashes": candidate_hashes, "ranking_basis_hashes": ranking_basis_hashes, "ranking_snapshot_hash": snapshot_hash, "gate_bundle_hash": gate_bundle_hash, "funnel_trace": self._active_funnel_trace}
        return PipelineResult(**payload, result_hash=canonical_hash(payload))


_DEFINED_RISK_FAMILIES = (
    "BUTTERFLY",
    "CALENDAR",
    "CREDIT_VERTICAL",
    "DEBIT_VERTICAL",
    "DIAGONAL",
    "EVENT_DEFINED_FINITE_RISK",
    "IRON_CONDOR",
    "LONG_OPTION",
)
_ORDINARY_SHORT_PREMIUM_FAMILIES = frozenset(
    {"CREDIT_VERTICAL", "IRON_CONDOR"}
)


def _build_gate_routing_context(
    *,
    scan_run_id: str,
    cutoff_at: datetime,
    valid_until: datetime,
    context: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> GateRoutingContext:
    raw = _document(
        context.get(
            "gate_routing",
            context.get(
                "market_credit_regime",
                evidence.get("market_credit_regime", {}),
            ),
        )
    )
    raw_status = str(raw.get("status", "PASS")).strip().upper()
    if raw_status not in {item.value for item in GateStatus}:
        raw_status = GateStatus.UNAVAILABLE.value
    status = GateStatus(raw_status)
    raw_allowed = raw.get("allowed_strategy_families")
    if raw_allowed is None:
        allowed = _DEFINED_RISK_FAMILIES if status is GateStatus.PASS else ()
    else:
        allowed = _strategy_families(raw_allowed)
    blocked = _strategy_families(raw.get("blocked_strategy_families", ()))
    allowed = tuple(item for item in allowed if item not in set(blocked))
    reasons = _stable_codes(
        raw.get(
            "reason_codes",
            () if status is GateStatus.PASS else ("MARKET_CREDIT_ROUTE_UNAVAILABLE",),
        )
    )
    source_hashes = tuple(
        sorted(
            {
                digest
                for value in (
                    raw.get("regime_hash"),
                    raw.get("vix_hash"),
                    raw.get("credit_hash"),
                    evidence.get("evidence_hash"),
                )
                for digest in (_hash_or_value(value),)
                if digest is not None
            }
        )
    )
    gate = GateLayerResult.build(
        gate_id=GateId.MARKET_CREDIT_REGIME,
        status=status,
        observed_at=cutoff_at,
        input_payload={
            "regime": raw.get("regime", "EXISTING_DEFINED_RISK_ROUTE"),
            "vix_state": raw.get("vix_state"),
            "credit_state": raw.get("credit_state"),
            "source_hashes": source_hashes,
        },
        reason_codes=reasons,
        source_hashes=source_hashes,
        allowed_strategy_families=allowed,
        blocked_strategy_families=blocked,
    )
    return GateRoutingContext.build(
        scan_run_id=scan_run_id,
        cutoff_at=cutoff_at,
        valid_until=valid_until,
        market_credit_gate=gate,
    )


def _route_finalists(
    finalists: tuple[object, ...], routing: GateRoutingContext
) -> tuple[object, ...]:
    allowed = set(routing.market_credit_gate.allowed_strategy_families)
    routed: list[object] = []
    for finalist in finalists:
        family = _strategy_family(_document(finalist))
        if family is None or family in allowed:
            routed.append(finalist)
    return tuple(routed)


def _route_generated_candidates(
    candidates: tuple[object, ...], routing: GateRoutingContext
) -> tuple[object, ...]:
    allowed = set(routing.market_credit_gate.allowed_strategy_families)
    return tuple(
        candidate
        for candidate in candidates
        if _candidate_strategy_family(candidate) in allowed
    )


def _bind_economic_gate_states(
    states: tuple[_PreRankGateState, ...], *, resolved_policy: object,
    costs: Mapping[str, Any], adjusted: Mapping[str, Decimal],
    policy_hash: str, policy_marker_hash: str, cost_hash: str,
) -> tuple[_PreRankGateState, ...]:
    """Bind every input that can change economic eligibility into Gate 4."""
    policy_payload = _document(_document(resolved_policy).get("payload"))
    thresholds = _document(_document(policy_payload.get(
        "hard_no_trade_thresholds"
    )).get("cost_and_expectancy"))
    cost_rows = {
        str(row.get("candidate_id")): row
        for item in costs.get("candidates", ())
        if (row := _document(item))
    }
    checked: list[_PreRankGateState] = []
    for state in states:
        body = state.binding.candidate_body
        candidate_id = str(body["candidate_id"])
        cost = cost_rows.get(candidate_id, {})
        values = {
            "max_loss": _pipeline_decimal(body.get("max_loss_usd")),
            "max_profit": _pipeline_decimal(body.get("max_profit_usd")),
            "max_profit_type": body.get("max_profit_type"),
            "structure": body.get("structure"),
            "legs": body.get("legs"),
            "after_cost_expected_value": adjusted.get(candidate_id),
            "execution_cost_usd": _pipeline_decimal(cost.get("execution_cost_usd")),
            "stress_after_cost_expected_value": _pipeline_decimal(cost.get("stress_after_cost_expected_value")),
        }
        reasons = signed_economics_reasons(values, thresholds)
        proof = freeze_json({
            "schema": "options_copilot.signed_economic_gate_proof.v1",
            "candidate_hash": state.binding.candidate_hash,
            "values": values,
            "thresholds": thresholds,
            "policy_hash": policy_hash,
            "policy_authority_marker_hash": policy_marker_hash,
            "cost_contract_hash": cost_hash,
            "cost_resolution": cost,
            "reason_codes": reasons,
        })
        checked.append(replace(
            state, economic_proof=proof,
            liquidity_status=GateStatus.BLOCK if reasons else state.liquidity_status,
            liquidity_reasons=tuple(dict.fromkeys((*state.liquidity_reasons, *reasons))),
        ))
    return tuple(checked)


def _pre_rank_gate_states(
    candidates: tuple[_CandidateBinding, ...],
    *,
    gate: Mapping[str, Any],
    context: Mapping[str, Any],
    evidence: Mapping[str, Any],
    routing: GateRoutingContext,
    broker_snapshot_hash: str,
) -> tuple[_PreRankGateState, ...]:
    gate_rows = context.get("candidate_gate_inputs", {})
    if not isinstance(gate_rows, Mapping):
        gate_rows = {}
    risk_fractions, risk_error = _bound_gate_risk_fractions(candidates, gate)
    gate_reasons = _reasons(gate, "HARD_ELIGIBILITY_REJECTED")
    gate_eligible = bool(gate.get("eligible", False))
    open_combinations = gate.get("open_combinations", 0)
    states: list[_PreRankGateState] = []
    for binding in candidates:
        body = binding.candidate_body
        candidate_id = str(body["candidate_id"])
        family = _strategy_family(body)
        if family is None or family not in set(
            routing.market_credit_gate.allowed_strategy_families
        ):
            raise ValueError("candidate strategy family escaped Gate 2 routing")
        row = _document(gate_rows.get(candidate_id, {}))

        authority_status = GateStatus.PASS
        authority_reasons: tuple[str, ...] = ()
        if body.get("broker_snapshot_hash") != broker_snapshot_hash:
            authority_status = GateStatus.UNAVAILABLE
            authority_reasons = ("BROKER_SNAPSHOT_BINDING_MISMATCH",)
        elif not _nav_authority_matches(body, context=context, evidence=evidence):
            authority_status = GateStatus.UNAVAILABLE
            authority_reasons = ("STRATEGY_NAV_AUTHORITY_MISMATCH",)

        event_evidence_status = str(
            body.get("event_evidence_status", "UNAVAILABLE")
        ).strip().upper()
        event_evidence_hash = _hash_or_value(body.get("event_evidence_hash"))
        overlap_value = body.get("earnings_overlap")
        event_evidence_available = (
            event_evidence_status == "AVAILABLE"
            and isinstance(overlap_value, bool)
            and event_evidence_hash is not None
        )
        overlap = overlap_value is True
        event_defined = _truthy_gate_flag(body.get("event_defined", False)) or (
            family == "EVENT_DEFINED_FINITE_RISK"
        )
        if not event_evidence_available:
            event_status = GateStatus.UNAVAILABLE
            event_reasons = ("EVENT_EVIDENCE_UNAVAILABLE",)
        elif overlap and family in _ORDINARY_SHORT_PREMIUM_FAMILIES and not event_defined:
            event_status = GateStatus.BLOCK
            event_reasons = ("EARNINGS_OVERLAP_SHORT_PREMIUM_BLOCKED",)
        else:
            event_status = GateStatus.PASS
            event_reasons = ()

        liquidity_status = GateStatus.PASS
        liquidity_reasons: tuple[str, ...] = ()
        risk_status = GateStatus.PASS
        risk_reasons: tuple[str, ...] = ()
        candidate_gate_status = str(row.get("status", "PASS")).strip().upper()
        candidate_reasons = _stable_codes(row.get("reason_codes", ()))
        if candidate_gate_status in {GateStatus.BLOCK.value, GateStatus.UNAVAILABLE.value}:
            target = (
                "LIQUIDITY"
                if any(
                    token in reason
                    for reason in candidate_reasons
                    for token in ("QUOTE", "LIQUID", "SPREAD", "MARKET")
                )
                else "RISK"
            )
            if target == "LIQUIDITY":
                liquidity_status = GateStatus(candidate_gate_status)
                liquidity_reasons = candidate_reasons or ("OPTION_EDGE_UNAVAILABLE",)
            else:
                risk_status = GateStatus(candidate_gate_status)
                risk_reasons = candidate_reasons or ("STRUCTURE_RISK_UNAVAILABLE",)

        risk_fraction = risk_fractions.get(candidate_id)
        if risk_error is not None or risk_fraction is None:
            risk_status = GateStatus.UNAVAILABLE
            risk_reasons = (risk_error or "CANDIDATE_RISK_BINDING_INVALID",)
        elif risk_fraction > Decimal("0.15"):
            risk_status = GateStatus.BLOCK
            risk_reasons = ("RISK_ABOVE_A_GRADE_CEILING",)
        elif (
            isinstance(open_combinations, bool)
            or not isinstance(open_combinations, int)
            or open_combinations >= 1
        ):
            risk_status = GateStatus.BLOCK
            risk_reasons = ("MAX_OPEN_COMBINATIONS",)
        elif not gate_eligible and risk_status is GateStatus.PASS:
            if any(
                token in reason
                for reason in gate_reasons
                for token in ("QUOTE", "LIQUID", "SPREAD", "MARKET")
            ):
                liquidity_status = GateStatus.BLOCK
                liquidity_reasons = gate_reasons
            else:
                risk_status = GateStatus.BLOCK
                risk_reasons = gate_reasons

        states.append(
            _PreRankGateState(
                binding=binding,
                strategy_family=family,
                authority_status=authority_status,
                authority_reasons=authority_reasons,
                event_status=event_status,
                event_reasons=event_reasons,
                liquidity_status=liquidity_status,
                liquidity_reasons=liquidity_reasons,
                risk_status=risk_status,
                risk_reasons=risk_reasons,
            )
        )
    return tuple(states)


def _build_gate_bundle(
    *,
    scan_run_id: str,
    cutoff_at: datetime,
    valid_until: datetime,
    pipeline_input_hash: str,
    routing: GateRoutingContext,
    states: Sequence[_PreRankGateState],
    ranked: Sequence[object],
    outcome: PipelineGateOutcome,
    run_reason_codes: Sequence[str],
    context: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> GateBundle:
    ranked_docs = tuple(_document(item) for item in ranked)
    rank_by_id = {
        str(row.get("candidate_id")): index
        for index, row in enumerate(ranked_docs, start=1)
    }
    total = len(ranked_docs)
    proposal_hashes = {
        str(state.binding.candidate_body["candidate_id"]): _final_proposal_hash(
            state.binding,
            rank=rank_by_id.get(str(state.binding.candidate_body["candidate_id"])),
        )
        for state in states
    }
    ordered_keys = tuple(
        candidate_gate_key(
            str(row["candidate_id"]),
            proposal_hashes[str(row["candidate_id"])],
        )
        for row in ranked_docs
    )
    pre_rank_payload = {
        "schema": "options_copilot.pre_gate_rank_order.v1",
        "ordered_candidate_keys": ordered_keys,
        "score_inputs_hash": canonical_hash(
            tuple(
                {
                    "candidate_id": row.get("candidate_id"),
                    "candidate_hash": row.get("candidate_hash"),
                    "score_components": _canonical_value(row.get("score_components", {})),
                }
                for row in ranked_docs
            )
        ),
    }
    candidates: list[CandidateGateResult] = []
    for state in states:
        body = state.binding.candidate_body
        candidate_id = str(body["candidate_id"])
        proposal_hash = proposal_hashes[candidate_id]
        rank = rank_by_id.get(candidate_id)
        ranking_context = RankingGateContext.build(
            rank=rank,
            total_ranked_count=total,
            pre_gate_rank_order_payload=pre_rank_payload if rank is not None else None,
            authority_consistent=rank is not None,
            reason_codes=()
            if rank == 1
            else ("VIEW_ONLY",)
            if rank is not None
            else ("NOT_RANKED",),
        )
        nav_bindings = _nav_gate_bindings(
            body,
            proposal_hash=proposal_hash,
            context=context,
            evidence=evidence,
        )
        evidence_hashes = _document(body.get("evidence_hashes", {}))
        event_sources = tuple(
            digest
            for digest in (
                _hash_or_value(body.get("event_evidence_hash")),
                _hash_or_value(evidence_hashes.get("EVENT")),
                _hash_or_value(evidence_hashes.get("NEWS")),
            )
            if digest is not None
        )
        event_supporting = _event_supporting_inputs(
            body,
            observed_at=cutoff_at,
        )
        liquidity_sources = tuple(
            digest
            for digest in (
                _hash_or_value(evidence_hashes.get("LIQUIDITY")),
                _hash_or_value(body.get("secdef_hash")),
                canonical_hash(state.economic_proof) if state.economic_proof is not None else None,
            )
            if digest is not None
        )
        layers = (
            GateLayerResult.build(
                gate_id=GateId.AUTHORITY_DATA,
                status=state.authority_status,
                observed_at=cutoff_at,
                input_payload={"existing_authority_bindings": nav_bindings},
                reason_codes=state.authority_reasons,
                source_hashes=tuple(
                    value
                    for value in nav_bindings.values()
                    if _hash_or_value(value) is not None
                ),
                bindings=nav_bindings,
            ),
            routing.market_credit_gate,
            GateLayerResult.build(
                gate_id=GateId.UNDERLYING_EVENT,
                status=state.event_status,
                observed_at=cutoff_at,
                input_payload={
                    "candidate_id": candidate_id,
                    "strategy_family": state.strategy_family,
                    "event_evidence_status": body.get("event_evidence_status"),
                    "earnings_overlap": body.get("earnings_overlap"),
                    "event_defined": body.get("event_defined"),
                    "event_reason_codes": state.event_reasons,
                },
                reason_codes=state.event_reasons,
                source_hashes=event_sources,
                allowed_strategy_families=(state.strategy_family,)
                if state.event_status is GateStatus.PASS
                else (),
                blocked_strategy_families=(state.strategy_family,)
                if state.event_status is not GateStatus.PASS
                else (),
                supporting_inputs=event_supporting,
            ),
            GateLayerResult.build(
                gate_id=GateId.OPTION_EDGE_LIQUIDITY,
                status=state.liquidity_status,
                observed_at=cutoff_at,
                input_payload={
                    "candidate_id": candidate_id,
                    "quote_batch_id": body.get("quote_batch_id"),
                    "liquidity_score": body.get("liquidity_score"),
                    "economic_proof": state.economic_proof,
                },
                reason_codes=state.liquidity_reasons,
                source_hashes=liquidity_sources,
                bindings={"economic_proof": state.economic_proof},
                blocked_strategy_families=(state.strategy_family,)
                if state.liquidity_status is not GateStatus.PASS
                else (),
            ),
            GateLayerResult.build(
                gate_id=GateId.STRUCTURE_ACCOUNT_RISK,
                status=state.risk_status,
                observed_at=cutoff_at,
                input_payload={
                    "candidate_id": candidate_id,
                    "max_loss_usd": body.get("max_loss_usd"),
                    "strategy_nav_usd": body.get("strategy_nav_usd"),
                },
                reason_codes=state.risk_reasons,
                source_hashes=tuple(
                    value
                    for value in (
                        nav_bindings.get("strategy_nav_authority_hash"),
                    )
                    if isinstance(value, str)
                ),
                blocked_strategy_families=(state.strategy_family,)
                if state.risk_status is not GateStatus.PASS
                else (),
            ),
            GateLayerResult.build(
                gate_id=GateId.RANKING_REVIEWABILITY,
                status=GateStatus.PASS if rank is not None else GateStatus.UNAVAILABLE,
                observed_at=cutoff_at,
                input_payload=ranking_context.hash_payload(),
                reason_codes=()
                if rank is not None
                else ("NOT_RANKED",),
                source_hashes=()
                if ranking_context.pre_gate_rank_order_hash is None
                else (ranking_context.pre_gate_rank_order_hash,),
                bindings={
                    "pre_gate_rank_order_hash": ranking_context.pre_gate_rank_order_hash,
                    "rank_authority_context_hash": ranking_context.context_hash,
                },
            ),
        )
        candidates.append(
            CandidateGateResult.build(
                candidate_id=candidate_id,
                candidate_hash=state.binding.candidate_hash,
                proposal_hash=proposal_hash,
                strategy_family=state.strategy_family,
                layers=layers,
                ranking=ranking_context,
            )
        )
    return GateBundle.build(
        scan_run_id=scan_run_id,
        cutoff_at=cutoff_at,
        valid_until=valid_until,
        pipeline_input_hash=pipeline_input_hash,
        routing_context=routing,
        candidates=candidates,
        outcome=outcome,
        run_reason_codes=run_reason_codes,
    )


def _final_proposal_hash(binding: _CandidateBinding, *, rank: int | None) -> str:
    if binding.proposal_body is None:
        raise ValueError("candidate proposal body is unavailable")
    proposal = dict(binding.proposal_body)
    proposal["rank"] = rank
    proposal["eligible_to_send"] = rank == 1
    return canonical_hash(freeze_json(proposal))


def _gate_for_candidates(
    gate: Mapping[str, Any], candidates: Sequence[_CandidateBinding]
) -> Mapping[str, Any]:
    values = dict(gate)
    raw = values.get("risk_fractions")
    if isinstance(raw, Mapping):
        ids = {str(item.candidate_body["candidate_id"]) for item in candidates}
        values["risk_fractions"] = {
            str(key): value for key, value in raw.items() if str(key) in ids
        }
    return values


def _event_supporting_inputs(
    body: Mapping[str, Any],
    *,
    observed_at: datetime,
) -> tuple[SupportingInput, ...]:
    source_hash = _hash_or_value(body.get("event_supporting_hash"))
    inputs: list[SupportingInput] = []
    overlap = body.get("event_supporting_overlap")
    if source_hash is not None and isinstance(overlap, bool):
        inputs.append(
            SupportingInput.build(
                source="EVENT_NEWS_CONTEXT",
                status=SupportingStatus.AVAILABLE,
                observed_at=observed_at,
                reason_codes=("SUPPORTING_ONLY_NO_HARD_AUTHORITY",),
                source_hash=source_hash,
                payload={
                    "candidate_id": body.get("candidate_id"),
                    "earnings_overlap": overlap,
                    "decision_authority": "SUPPORTING_ONLY",
                },
            )
        )
    fundamental_hash = _hash_or_value(body.get("fundamental_supporting_hash"))
    fundamental_payload = body.get("fundamental_supporting_payload")
    fundamental_status = str(
        body.get("fundamental_supporting_status") or "DEGRADED"
    ).strip().upper()
    raw_reasons = body.get("fundamental_supporting_reason_codes")
    if (
        fundamental_hash is not None
        and isinstance(fundamental_payload, Mapping)
        and fundamental_payload.get("decision_authority") == "SUPPORTING_ONLY"
        and fundamental_status == "AVAILABLE"
    ):
        reasons = (
            tuple(str(item).strip().upper() for item in raw_reasons)
            if isinstance(raw_reasons, Sequence)
            and not isinstance(raw_reasons, (str, bytes, bytearray))
            else ("SUPPORTING_ONLY_NO_HARD_AUTHORITY",)
        )
        inputs.append(
            SupportingInput.build(
                source="POINT_IN_TIME_FUNDAMENTALS",
                status=SupportingStatus.AVAILABLE,
                observed_at=observed_at,
                reason_codes=reasons,
                source_hash=fundamental_hash,
                payload=fundamental_payload,
            )
        )
    return tuple(inputs)


def _nav_authority_matches(
    body: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> bool:
    nav_source = evidence.get(
        "nav_snapshot", context.get("strategy_nav_snapshot", {})
    )
    nav = _document(nav_source)
    if not nav:
        return False
    candidate_nav_hash = _hash_or_value(body.get("strategy_nav_hash"))
    candidate_content_hash = _hash_or_value(
        body.get("strategy_nav_content_hash")
    )
    candidate_ledger_head_hash = _hash_or_value(
        body.get("strategy_nav_ledger_head_hash")
    )
    contract_hash = _hash_or_value(nav.get("contract_hash"))
    authority_hash = _hash_or_value(
        nav.get(
            "authority_hash",
            nav.get(
                "strategy_nav_authority_hash",
                getattr(nav_source, "authority_hash", None),
            ),
        )
    )
    content_hash = _hash_or_value(nav.get("content_hash"))
    ledger_head_hash = _hash_or_value(nav.get("ledger_head_hash"))
    observed_nlv = _pipeline_decimal(nav.get("observed_account_nlv"))
    candidate_observed_nlv = _pipeline_decimal(
        body.get("strategy_nav_observed_account_nlv")
    )
    reconciliation = _pipeline_decimal(nav.get("reconciliation_difference"))
    candidate_reconciliation = _pipeline_decimal(
        body.get("strategy_nav_reconciliation_difference")
    )
    nav_asof_text = _timestamp_text(nav.get("asof"))
    return bool(
        nav.get("valid") is True
        and authority_hash == candidate_nav_hash
        and content_hash == candidate_content_hash
        and contract_hash == body.get("strategy_nav_contract_hash")
        and ledger_head_hash == candidate_ledger_head_hash
        and observed_nlv is not None
        and observed_nlv > 0
        and observed_nlv == candidate_observed_nlv
        and reconciliation is not None
        and reconciliation == candidate_reconciliation
        and nav_asof_text == _timestamp_text(body.get("strategy_nav_asof"))
    )


def _strategy_nav_authority_hash(
    *,
    context: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> str | None:
    nav_source = evidence.get(
        "nav_snapshot", context.get("strategy_nav_snapshot", {})
    )
    nav = _document(nav_source)
    return _hash_or_value(
        nav.get(
            "authority_hash",
            nav.get(
                "strategy_nav_authority_hash",
                getattr(nav_source, "authority_hash", None),
            ),
        )
    )


def _joint_account_context(
    *,
    candidates: Sequence[FinalizedOptionPoolCandidate],
    gate: Mapping[str, Any],
    evidence: Mapping[str, Any],
    broker_snapshot_hash: str,
) -> tuple[_JointAccountContext | None, str | None]:
    snapshot = evidence.get("broker_snapshot")
    if (
        not isinstance(snapshot, AtomicBrokerSnapshot)
        or not snapshot.complete
        or not snapshot.verify_hash()
        or snapshot.snapshot_hash != broker_snapshot_hash
    ):
        return None, "JOINT_ACCOUNT_SNAPSHOT_INVALID"
    positions = snapshot.state_evidence.get("positions")
    if (
        positions is None
        or not positions.known
        or not positions.stable
        or positions.pre_hash != positions.post_hash
        or positions.count is None
        or not isinstance(positions.state, Sequence)
        or isinstance(positions.state, (str, bytes, bytearray, memoryview))
        or len(positions.state) != positions.count
    ):
        return None, "JOINT_POSITION_AUTHORITY_UNAVAILABLE"
    nav_source = evidence.get("nav_snapshot")
    nav = _document(nav_source)
    strategy_nav = _pipeline_decimal(nav.get("strategy_nav"))
    if (
        strategy_nav is None
        or strategy_nav <= 0
        or _strategy_nav_authority_hash(context={}, evidence=evidence) is None
    ):
        return None, "JOINT_NAV_AUTHORITY_UNAVAILABLE"
    candidate_symbols = {
        str(candidate.payload.get("symbol", "")).strip().upper()
        for candidate in candidates
    }
    if "" in candidate_symbols:
        return None, "JOINT_CANDIDATE_SYMBOL_INVALID"
    concentration_values = {
        symbol: Decimal("0") for symbol in candidate_symbols
    }
    option_underlyings: set[str] = set()
    option_position_count = 0
    for raw_position in positions.state:
        row = _document(raw_position)
        quantity = _pipeline_decimal(
            row.get("quantity", row.get("position"))
        )
        if quantity is None:
            return None, "JOINT_POSITION_AUTHORITY_INVALID"
        if quantity == 0:
            continue
        security_type = str(
            row.get("security_type", row.get("secType", ""))
        ).strip().upper()
        symbol = str(
            row.get("symbol", row.get("underlying", ""))
        ).strip().upper()
        if security_type in {"OPT", "OPTION", "BAG", "COMBO"}:
            option_position_count = 1
            if symbol:
                option_underlyings.add(symbol)
            continue
        if security_type not in {"STK", "ETF"} or not symbol:
            return None, "JOINT_CONCENTRATION_AUTHORITY_UNAVAILABLE"
        market_value = _pipeline_decimal(row.get("market_value"))
        if market_value is None:
            return None, "JOINT_CONCENTRATION_AUTHORITY_UNAVAILABLE"
        if symbol in concentration_values:
            concentration_values[symbol] += abs(market_value) / strategy_nav
    gate_open_combinations = gate.get("open_combinations")
    if (
        isinstance(gate_open_combinations, bool)
        or not isinstance(gate_open_combinations, int)
        or gate_open_combinations != option_position_count
    ):
        return None, "JOINT_OPEN_COMBINATION_BINDING_MISMATCH"
    if option_position_count:
        return None, "JOINT_OPEN_POSITION_RISK_UNAVAILABLE"
    if any(value < 0 or value > 1 for value in concentration_values.values()):
        return None, "JOINT_CONCENTRATION_AUTHORITY_INVALID"
    return (
        _JointAccountContext(
            open_position_underlyings=tuple(sorted(option_underlyings)),
            aggregate_open_risk_usd=Decimal("0"),
            concentration_by_underlying=freeze_json(concentration_values),
        ),
        None,
    )


def _resolved_joint_account_context(
    resolver: object,
    *,
    candidates: Sequence[FinalizedOptionPoolCandidate],
    gate: Mapping[str, Any],
    evidence: Mapping[str, Any],
    broker_snapshot_hash: str,
) -> tuple[_JointAccountContext | None, str | None]:
    try:
        raw = _document(_invoke(
            resolver,
            ("resolve",),
            candidates=candidates,
            gate=gate,
            evidence=evidence,
            broker_snapshot_hash=broker_snapshot_hash,
        ))
    except (TypeError, ValueError):
        return None, "JOINT_ACCOUNT_CONTEXT_UNAVAILABLE"
    if raw.get("broker_snapshot_hash") != broker_snapshot_hash:
        return None, "JOINT_ACCOUNT_SNAPSHOT_INVALID"
    open_underlyings = raw.get("open_position_underlyings")
    concentrations = raw.get("concentration_by_underlying")
    aggregate = _pipeline_decimal(raw.get("aggregate_open_risk_usd"))
    expected_symbols = {
        str(candidate.payload.get("symbol", "")).strip().upper()
        for candidate in candidates
    }
    if (
        not isinstance(open_underlyings, Sequence)
        or isinstance(open_underlyings, (str, bytes, bytearray, memoryview))
        or not isinstance(concentrations, Mapping)
        or aggregate is None
        or aggregate < 0
        or {str(key).strip().upper() for key in concentrations} != expected_symbols
    ):
        return None, "JOINT_ACCOUNT_CONTEXT_UNAVAILABLE"
    normalized_concentrations: dict[str, Decimal] = {}
    for key, value in concentrations.items():
        symbol = str(key).strip().upper()
        concentration = _pipeline_decimal(value)
        if symbol in normalized_concentrations or concentration is None or not 0 <= concentration <= 1:
            return None, "JOINT_CONCENTRATION_AUTHORITY_INVALID"
        normalized_concentrations[symbol] = concentration
    return _JointAccountContext(
        open_position_underlyings=tuple(
            sorted({str(value).strip().upper() for value in open_underlyings if str(value).strip()})
        ),
        aggregate_open_risk_usd=aggregate,
        concentration_by_underlying=freeze_json(normalized_concentrations),
    ), None


def _nav_gate_bindings(
    body: Mapping[str, Any],
    *,
    proposal_hash: str,
    context: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> dict[str, str]:
    nav_source = evidence.get(
        "nav_snapshot", context.get("strategy_nav_snapshot", {})
    )
    nav = _document(nav_source)
    bindings = {"proposal_hash": proposal_hash}
    candidate_nav_hash = _hash_or_value(body.get("strategy_nav_hash"))
    candidate_content_hash = _hash_or_value(
        body.get("strategy_nav_content_hash")
    )
    candidate_ledger_head_hash = _hash_or_value(
        body.get("strategy_nav_ledger_head_hash")
    )
    contract_hash = _hash_or_value(nav.get("contract_hash"))
    authority_hash = _hash_or_value(
        nav.get(
            "authority_hash",
            nav.get(
                "strategy_nav_authority_hash",
                getattr(nav_source, "authority_hash", None),
            ),
        )
    )
    content_hash = _hash_or_value(nav.get("content_hash"))
    ledger_head_hash = _hash_or_value(nav.get("ledger_head_hash"))
    observed_nlv = _pipeline_decimal(nav.get("observed_account_nlv"))
    candidate_observed_nlv = _pipeline_decimal(
        body.get("strategy_nav_observed_account_nlv")
    )
    reconciliation = _pipeline_decimal(nav.get("reconciliation_difference"))
    candidate_reconciliation = _pipeline_decimal(
        body.get("strategy_nav_reconciliation_difference")
    )
    nav_asof_text = _timestamp_text(nav.get("asof"))
    broker_snapshot_hash = _hash_or_value(body.get("broker_snapshot_hash"))
    if (
        nav.get("valid") is True
        and candidate_nav_hash is not None
        and authority_hash == candidate_nav_hash
        and content_hash == candidate_content_hash
        and contract_hash == _hash_or_value(body.get("strategy_nav_contract_hash"))
        and ledger_head_hash == candidate_ledger_head_hash
        and observed_nlv is not None
        and observed_nlv > 0
        and observed_nlv == candidate_observed_nlv
        and reconciliation is not None
        and reconciliation == candidate_reconciliation
        and nav_asof_text == _timestamp_text(body.get("strategy_nav_asof"))
        and broker_snapshot_hash is not None
    ):
        bindings.update(
            {
                "broker_snapshot_hash": broker_snapshot_hash,
                "strategy_nav_authority_hash": authority_hash,
                "strategy_nav_content_hash": content_hash,
                "strategy_nav_contract_hash": contract_hash,
                "strategy_nav_ledger_head_hash": ledger_head_hash,
                "strategy_nav_observed_account_nlv": str(observed_nlv),
                "strategy_nav_reconciliation_difference": str(reconciliation),
                "strategy_nav_asof": nav_asof_text,
            }
        )
    return bindings


def _strategy_family(document: Mapping[str, Any]) -> str | None:
    raw = document.get("strategy_family", document.get("structure"))
    value = getattr(raw, "value", raw)
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().upper()


def _candidate_strategy_family(candidate: object) -> str | None:
    family = _strategy_family(_document(candidate))
    if family is not None:
        return family
    payload = getattr(candidate, "hash_payload", None)
    if not callable(payload):
        return None
    try:
        body = payload()
    except (TypeError, ValueError):
        return None
    return _strategy_family(body) if isinstance(body, Mapping) else None


def _strategy_families(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raise TypeError("strategy families must be a sequence")
    return tuple(
        sorted(
            {
                normalized
                for item in value
                for normalized in (_strategy_family({"structure": item}),)
                if normalized is not None
            }
        )
    )


def _stable_codes(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        values = tuple(value)
    else:
        values = ()
    return tuple(
        sorted(
            {
                str(item).strip().upper()
                for item in values
                if str(item).strip()
            }
        )
    )


def _truthy_gate_flag(value: object) -> bool:
    return value is True


def _resolve_final_rank_authority(
    resolver: object,
    *,
    current_authority: object,
    rank_one: Mapping[str, object],
    now: datetime,
    resolved_policy: object,
    policy_version: str,
    policy_hash: str,
    policy_marker_hash: str,
    cost_version: str,
    cost_hash: str,
    risk_contract_hash: str,
) -> tuple[object, str | None]:
    try:
        final = _resolve_once(
            resolver,
            now=now,
            current_policy=resolved_policy,
            resolved_policy=resolved_policy,
            proposal_hash=rank_one.get("proposal_hash"),
            candidate_hash=rank_one.get("candidate_hash"),
            execution_cost_version=cost_version,
            execution_cost_hash=cost_hash,
            ranking_basis_hash=rank_one.get("ranking_basis_hash"),
        )
    except (TypeError, ValueError):
        return current_authority, "RANKING_GATE_BUNDLE_MISMATCH"
    if not _a_grade_binding_matches(
        final,
        proposal_hash=_hash_or_value(rank_one.get("proposal_hash")),
        candidate_hash=_hash_or_value(rank_one.get("candidate_hash")),
        ranking_basis_hash=_hash_or_value(rank_one.get("ranking_basis_hash")),
        policy_version=policy_version,
        policy_hash=policy_hash,
        policy_marker_hash=policy_marker_hash,
        cost_version=cost_version,
        cost_hash=cost_hash,
        risk_contract_hash=risk_contract_hash,
    ):
        return current_authority, "RANKING_GATE_BUNDLE_MISMATCH"
    return final, None


def _invoke(port: object, names: tuple[str, ...], **kwargs: object) -> object:
    target = port if callable(port) else next((getattr(port, name) for name in names if callable(getattr(port, name, None))), None)
    if target is None: raise TypeError(f"port lacks one of {names!r}")
    signature = inspect.signature(target)
    accepted = kwargs if any(item.kind is inspect.Parameter.VAR_KEYWORD for item in signature.parameters.values()) else {key: value for key, value in kwargs.items() if key in signature.parameters}
    return target(**accepted)


def normalize_funnel_trace(
    value: object,
    *,
    scan_run_id: str,
) -> Mapping[str, object]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("funnel_trace must be a mapping")
    if not value:
        return {}
    normalized = dict(value)
    allocation = normalise_research_allocation_evidence(
        value.get("research_allocation")
    )
    if allocation is None:
        normalized.pop("research_allocation", None)
    else:
        normalized["research_allocation"] = allocation
    equity_pool_reference = normalize_equity_pool_reference(
        value.get("equity_pool_reference")
    )
    if equity_pool_reference is None:
        normalized.pop("equity_pool_reference", None)
        normalized.pop("equity_theses", None)
    else:
        normalized["equity_pool_reference"] = equity_pool_reference
        equity_theses = normalize_equity_theses(
            value.get("equity_theses"),
            equity_pool_reference=equity_pool_reference,
        )
        if equity_theses:
            normalized["equity_theses"] = {
                "schema": "options_copilot.equity_theses.v1",
                "equity_pool_reference_hash": canonical_hash(equity_pool_reference),
                "rows": tuple(equity_theses[symbol] for symbol in sorted(equity_theses)),
                "rows_hash": canonical_hash(
                    tuple(equity_theses[symbol] for symbol in sorted(equity_theses))
                ),
            }
        else:
            normalized.pop("equity_theses", None)
    frozen = freeze_json(normalized)
    if not isinstance(frozen, Mapping):
        raise TypeError("funnel_trace must be a canonical mapping")
    if (
        frozen.get("schema") != "options_copilot.discovery_funnel_trace.v1"
        or frozen.get("scan_run_id") != scan_run_id
        or frozen.get("filler_candidates") != 0
    ):
        raise ValueError("funnel_trace identity or no-filler invariant is invalid")
    limits = {
        "discovered_underlyings": 150,
        "deep_scan_requested": 30,
        "deep_scan_completed": 30,
        "ranked_limit": 10,
        "ranked_count": 10,
    }
    for name, maximum in limits.items():
        raw = frozen.get(name)
        if isinstance(raw, bool) or not isinstance(raw, int) or not 0 <= raw <= maximum:
            raise ValueError(f"funnel_trace {name} is invalid")
    if frozen.get("ranked_limit") != 10:
        raise ValueError("funnel_trace ranked_limit must remain ten")
    discovered = frozen["discovered_underlyings"]
    deep_requested = frozen["deep_scan_requested"]
    deep_completed = frozen["deep_scan_completed"]
    ranked_count = frozen["ranked_count"]
    deep_attempted = frozen.get("deep_scan_attempted")
    deep_deferred = frozen.get("deep_scan_deferred")
    deferred_symbols_raw = frozen.get("deep_scan_deferred_symbols", ())
    has_budget_observability = (
        deep_attempted is not None
        or deep_deferred is not None
        or bool(deferred_symbols_raw)
    )
    if has_budget_observability:
        if (
            isinstance(deep_attempted, bool)
            or not isinstance(deep_attempted, int)
            or not 0 <= deep_attempted <= deep_requested
            or isinstance(deep_deferred, bool)
            or not isinstance(deep_deferred, int)
            or not 0 <= deep_deferred <= deep_requested
            or deep_attempted + deep_deferred > deep_requested
            or deep_completed > deep_attempted
        ):
            raise ValueError("funnel_trace deep-scan budget accounting is invalid")
        if not isinstance(deferred_symbols_raw, Sequence) or isinstance(
            deferred_symbols_raw,
            (str, bytes, bytearray, memoryview),
        ):
            raise ValueError("funnel_trace deferred symbols are invalid")
        deferred_symbols = tuple(deferred_symbols_raw)
        if (
            len(deferred_symbols) != deep_deferred
            or any(
                not isinstance(symbol, str)
                or symbol != symbol.strip().upper()
                or not 1 <= len(symbol) <= 24
                or any(
                    not (character.isascii() and character.isalnum())
                    and character not in {".", "-"}
                    for character in symbol
                )
                for symbol in deferred_symbols
            )
        ):
            raise ValueError("funnel_trace deferred symbols are invalid")
        if len(set(deferred_symbols)) != len(deferred_symbols):
            raise ValueError("funnel_trace deferred symbols are invalid")
    elif deep_completed > deep_requested:
        raise ValueError("funnel_trace completed count exceeds requested count")
    if ranked_count > deep_completed or ranked_count > frozen["ranked_limit"]:
        raise ValueError("funnel_trace ranked count exceeds its protected bounds")
    pacing_hash = frozen.get("pacing_capability_hash")
    if pacing_hash is not None and (
        not isinstance(pacing_hash, str)
        or len(pacing_hash) != 64
        or any(character not in "0123456789abcdef" for character in pacing_hash)
    ):
        raise ValueError("funnel_trace pacing capability hash is invalid")
    pacing_usage = frozen.get("pacing_usage")
    if not isinstance(pacing_usage, Mapping):
        raise ValueError("funnel_trace pacing usage is invalid")
    for request_class, raw_usage in pacing_usage.items():
        if not isinstance(request_class, str) or not request_class.strip():
            raise ValueError("funnel_trace pacing request class is invalid")
        if not isinstance(raw_usage, Mapping) or set(raw_usage) != {"used", "limit"}:
            raise ValueError("funnel_trace pacing usage row is invalid")
        used = raw_usage.get("used")
        limit = raw_usage.get("limit")
        if (
            isinstance(used, bool)
            or not isinstance(used, int)
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or used < 0
            or limit < 0
            or used > limit
        ):
            raise ValueError("funnel_trace pacing usage bounds are invalid")
    scanner_source_fields = (
        "scanner_completed_scan_codes",
        "scanner_failed_scan_codes",
        "scanner_source_row_counts",
    )
    scanner_confirmed_charged_requests = frozen.get(
        "scanner_confirmed_charged_requests"
    )
    if scanner_confirmed_charged_requests is not None and (
        isinstance(scanner_confirmed_charged_requests, bool)
        or not isinstance(scanner_confirmed_charged_requests, int)
        or not 0 <= scanner_confirmed_charged_requests <= 3
    ):
        raise ValueError(
            "funnel_trace scanner confirmed charged requests is invalid"
        )
    if any(name in frozen for name in scanner_source_fields):
        if any(name not in frozen for name in scanner_source_fields):
            raise ValueError("funnel_trace scanner source evidence is incomplete")
        if scanner_confirmed_charged_requests is None:
            raise ValueError("funnel_trace scanner request evidence is incomplete")
        allowed_scan_codes = (
            "MOST_ACTIVE",
            "TOP_PERC_GAIN",
            "TOP_PERC_LOSE",
        )
        raw_completed = frozen["scanner_completed_scan_codes"]
        raw_failed = frozen["scanner_failed_scan_codes"]
        raw_counts = frozen["scanner_source_row_counts"]
        for value_name, raw_value in (
            ("completed", raw_completed),
            ("failed", raw_failed),
            ("row counts", raw_counts),
        ):
            if not isinstance(raw_value, Sequence) or isinstance(
                raw_value,
                (str, bytes, bytearray),
            ):
                raise ValueError(
                    f"funnel_trace scanner source {value_name} is invalid"
                )
        completed = tuple(raw_completed)
        failed = tuple(raw_failed)
        if (
            any(not isinstance(item, str) for item in (*completed, *failed))
            or len(set(completed)) != len(completed)
            or len(set(failed)) != len(failed)
            or set(completed) & set(failed)
            or any(item not in allowed_scan_codes for item in (*completed, *failed))
            or completed
            != tuple(item for item in allowed_scan_codes if item in completed)
            or failed != tuple(item for item in allowed_scan_codes if item in failed)
        ):
            raise ValueError("funnel_trace scanner source status is invalid")
        row_counts: dict[str, int] = {}
        for raw_row in raw_counts:
            if not isinstance(raw_row, Mapping) or set(raw_row) != {
                "scan_code",
                "row_count",
            }:
                raise ValueError("funnel_trace scanner source row is invalid")
            scan_code = raw_row.get("scan_code")
            row_count = raw_row.get("row_count")
            if (
                not isinstance(scan_code, str)
                or scan_code not in allowed_scan_codes
                or scan_code in row_counts
                or isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or not 0 <= row_count <= 50
                or (scan_code in failed and row_count != 0)
            ):
                raise ValueError("funnel_trace scanner source row is invalid")
            row_counts[scan_code] = row_count
        if (
            set(row_counts) != set(completed) | set(failed)
            or set(row_counts) != set(allowed_scan_codes)
        ):
            raise ValueError("funnel_trace scanner source coverage is invalid")
        if discovered > sum(row_counts.values()):
            raise ValueError("funnel_trace scanner source counts are inconsistent")
        if (
            len(completed) > scanner_confirmed_charged_requests
            or discovered > scanner_confirmed_charged_requests * 50
        ):
            raise ValueError("funnel_trace scanner request counts are inconsistent")
    if discovered > 0 or scanner_confirmed_charged_requests is not None:
        scanner_usage = pacing_usage.get("scanner")
        if pacing_hash is None or not isinstance(scanner_usage, Mapping):
            raise ValueError("funnel_trace pacing identity is required")
        scanner_used = scanner_usage.get("used")
        if (
            isinstance(scanner_used, bool)
            or not isinstance(scanner_used, int)
            or (
                scanner_confirmed_charged_requests is not None
                and scanner_used < scanner_confirmed_charged_requests
            )
            or (
                scanner_confirmed_charged_requests is None
                and (scanner_used < 1 or discovered > scanner_used * 50)
            )
        ):
            raise ValueError("funnel_trace scanner request identity is invalid")
    return frozen


def _port_accepts_keyword(
    port: object, names: tuple[str, ...], keyword: str
) -> bool:
    target = port if callable(port) else next(
        (
            getattr(port, name)
            for name in names
            if callable(getattr(port, name, None))
        ),
        None,
    )
    if target is None:
        return False
    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):
        return False
    return keyword in signature.parameters or any(
        item.kind is inspect.Parameter.VAR_KEYWORD
        for item in signature.parameters.values()
    )

def _registry_finalists(registry: object, *, finalists: tuple[object, ...], evidence: Mapping[str, Any], context: Mapping[str, Any]) -> Mapping[str, Any]:
    if callable(registry) or any(callable(getattr(registry, name, None)) for name in ("run", "prepare", "validate_finalists")):
        return _document(_invoke(registry, ("run", "prepare", "validate_finalists"), finalists=finalists, evidence=evidence, context=context))
    # The concrete closed P4 registry is embedded in StrategyCandidateGenerator.
    # Reading its registered templates verifies the registry is present without
    # attempting to validate unresolved pre-broker template legs prematurely.
    templates = getattr(registry, "registered_templates", None)
    if templates is None:
        raise TypeError("strategy_registry does not expose an entry-safe registry port")
    tuple(templates)
    return {"finalists": finalists}

def _document(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping): return value
    if is_dataclass(value): return {field.name: getattr(value, field.name) for field in fields(value)}
    return vars(value) if hasattr(value, "__dict__") else {}

def _canonical_value(value: object) -> object:
    if is_dataclass(value):
        return {
            field.name: _canonical_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(_canonical_value(item) for item in value)
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    return value

def _hash_or_value(value: object) -> str | None:
    return value if isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value) else None


def _timestamp_text(value: object) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(timezone.utc).isoformat()
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat()

def _string_or_none(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None

def _value_text(value: object) -> str:
    return str(getattr(value, "value", value))

def _reasons(document: Mapping[str, Any], fallback: str) -> tuple[str, ...]:
    values = document.get("reasons", document.get("reason_codes", (fallback,)))
    if isinstance(values, str): return (values,)
    try:
        result = tuple(str(value) for value in values)
    except TypeError:
        result = ()
    return tuple(sorted(set(result))) or (fallback,)

def _context_reason_codes(document: Mapping[str, Any]) -> tuple[str, ...]:
    values: list[object] = []
    for name in ("reasons", "reason_codes"):
        raw = document.get(name, ())
        if isinstance(raw, str):
            values.append(raw)
        elif isinstance(raw, Sequence) and not isinstance(
            raw, (str, bytes, bytearray, memoryview)
        ):
            values.extend(raw)
    return tuple(
        sorted(
            {
                text
                for value in values
                if (text := str(value).strip().upper())
            }
        )
    )

def _option_position_open(positions: tuple[object, ...]) -> bool:
    for item in positions:
        value = _document(item)
        security_type = str(
            value.get("security_type", value.get("secType", ""))
        ).upper()
        if security_type not in {"OPT", "OPTION", "BAG", "COMBO"}:
            continue
        quantity = value.get("quantity", value.get("position", 0))
        try:
            if Decimal(str(quantity)) != Decimal("0"):
                return True
        except Exception:
            return True
    return False

def _scenario_input(evidence: Mapping[str, Any], volatility: Mapping[str, Any], candidate: object) -> dict[str, object]:
    """Build a scenario request exclusively from preceding trusted stages."""
    raw = _document(candidate)
    underlying = str(
        raw.get("underlying") or raw.get("symbol") or ""
    ).strip().upper()
    volatility_rows = _document(
        _document(volatility.get("features", {})).get("by_underlying", {})
    )
    candidate_volatility = _document(volatility_rows.get(underlying, volatility))
    spot_by_underlying = _document(evidence.get("spot_by_underlying", {}))
    atm_iv_by_underlying = _document(evidence.get("atm_iv_by_underlying", {}))
    candidate_evidence = _document(raw.get("evidence_hashes", {}))
    market_hash = _hash_or_value(evidence.get("market_evidence_hash")) or _hash_or_value(evidence.get("evidence_hash"))
    liquidity_hash = _hash_or_value(candidate_evidence.get("LIQUIDITY")) or _hash_or_value(evidence.get("liquidity_evidence_hash"))
    volatility_hash = _hash_or_value(candidate_volatility.get("evidence_hash"))
    return {
        "spot": spot_by_underlying.get(
            underlying,
            evidence.get("spot", evidence.get("underlying_spot")),
        ),
        "atm_iv": atm_iv_by_underlying.get(
            underlying,
            evidence.get("atm_iv", evidence.get("implied_volatility")),
        ),
        "dte": raw.get("dte"),
        # Canonical candidate documents intentionally serialize Decimal values
        # as strings.  Restore only the numeric fields consumed by the trusted
        # scenario engine; passing the canonical string through would make a
        # valid, hash-bound maximum loss look unknown.
        "max_loss": _pipeline_decimal(raw.get("max_loss_usd", raw.get("max_loss"))),
        "market_score": evidence.get("market_score"),
        "volatility_score": candidate_volatility.get("volatility_score"),
        "cost_hash": raw.get("execution_cost_contract_hash"),
        "cost_version": raw.get("execution_cost_contract_version"),
        "hard_evidence": {
            "MARKET": {"eligible": market_hash is not None, "hash": market_hash},
            "VOLATILITY": {"eligible": bool(volatility.get("eligible", False)), "hash": volatility_hash},
            "LIQUIDITY": {"eligible": liquidity_hash is not None, "hash": liquidity_hash},
        },
        "input_hash": canonical_hash({"broker": evidence.get("snapshot_hash", evidence.get("broker_snapshot_hash")), "volatility": volatility_hash, "candidate": canonical_hash(raw)}),
    }


def _filter_candidates_by_volatility(
    candidates: tuple[object, ...],
    bindings: tuple[_CandidateBinding, ...],
    volatility: Mapping[str, Any],
) -> tuple[tuple[object, ...], tuple[_CandidateBinding, ...]]:
    rows = _document(
        _document(volatility.get("features", {})).get("by_underlying", {})
    )
    if not rows:
        return candidates, bindings
    accepted_candidates: list[object] = []
    accepted_bindings: list[_CandidateBinding] = []
    for candidate, binding in zip(candidates, bindings):
        raw = _document(binding.candidate_body)
        underlying = str(
            raw.get("underlying") or raw.get("symbol") or ""
        ).strip().upper()
        row = _document(rows.get(underlying, {}))
        if bool(row.get("eligible", False)):
            accepted_candidates.append(candidate)
            accepted_bindings.append(binding)
    return tuple(accepted_candidates), tuple(accepted_bindings)

def _cost_adjusted(candidates: tuple[object, ...], scenarios: tuple[Mapping[str, Any], ...], costs: Mapping[str, Any], cost_hash: str, cost_version: str) -> Mapping[str, Decimal] | None:
    """Require a signed per-candidate after-cost EV; no shared EV is allowed."""
    supplied = costs.get("candidates", costs.get("candidate_costs", ()))
    if isinstance(supplied, Mapping): by_id = {str(key): _document(value) for key, value in supplied.items()}
    else:
        try: by_id = {str(_document(value).get("candidate_id")): _document(value) for value in supplied}
        except TypeError: return None
    adjusted: dict[str, Decimal] = {}
    for candidate, scenario in zip(candidates, scenarios):
        raw = _document(candidate); candidate_id = _string_or_none(raw.get("candidate_id"))
        row = by_id.get(candidate_id or "")
        if candidate_id is None or row is None: return None
        bound_hash = _hash_or_value(row.get("cost_hash", row.get("execution_cost_contract_hash", cost_hash)))
        bound_version = _string_or_none(row.get("cost_version", row.get("execution_cost_contract_version", cost_version)))
        candidate_cost_hash = _hash_or_value(raw.get("execution_cost_contract_hash", cost_hash))
        candidate_cost_version = _string_or_none(raw.get("execution_cost_contract_version", cost_version))
        scenario_cost_hash = _hash_or_value(scenario.get("cost_hash", candidate_cost_hash))
        if bound_hash != cost_hash or bound_version != cost_version or candidate_cost_hash != cost_hash or candidate_cost_version != cost_version or scenario_cost_hash != cost_hash:
            return None
        ev = row.get("after_cost_expected_value", row.get("net_after_cost_ev"))
        if not isinstance(ev, Decimal) or not ev.is_finite(): return None
        adjusted[candidate_id] = ev
    return adjusted


def _research_option_pool_candidate_documents(
    finalists: Sequence[object],
    *,
    equity_pool_reference: Mapping[str, object],
    equity_theses: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    """Freeze only thesis-bound exact identities as non-executable research."""

    thesis_by_symbol = normalize_equity_theses(
        equity_theses,
        equity_pool_reference=equity_pool_reference,
    )
    documents: list[Mapping[str, object]] = []
    for finalist in finalists:
        payload_reader = getattr(finalist, "hash_payload", None)
        raw_payload = (
            payload_reader()
            if callable(payload_reader)
            else _document(finalist)
        )
        frozen_payload = freeze_json(raw_payload)
        payload = thaw_json(frozen_payload)
        if not isinstance(payload, dict):
            raise TypeError("research option candidate payload is invalid")
        symbol = str(payload.get("symbol", "")).strip().upper()
        thesis = thesis_by_symbol.get(symbol)
        if thesis is None:
            continue
        expected_thesis_hash = canonical_hash(thesis)
        embedded_thesis = normalize_equity_thesis_row(
            payload.get("equity_thesis_evidence"),
            expected_symbol=symbol,
        )
        invalidation = payload.get("invalidation_evidence")
        if (
            embedded_thesis is None
            or canonical_hash(embedded_thesis) != expected_thesis_hash
            or payload.get("equity_thesis_hash") != expected_thesis_hash
            or not isinstance(invalidation, Mapping)
            or invalidation.get("status") != "BOUND"
            or invalidation.get("equity_thesis_hash") != expected_thesis_hash
            or not str(invalidation.get("thesis_invalidation") or "").strip()
            or not str(invalidation.get("maximum_holding_date") or "").strip()
        ):
            continue
        if not _has_exact_research_contract_identity(payload):
            continue
        payload["equity_thesis_evidence"] = embedded_thesis
        candidate_hash = canonical_hash(payload)
        frozen_document = freeze_json(
            {
                "candidate_hash": candidate_hash,
                "payload": payload,
            }
        )
        assert isinstance(frozen_document, Mapping)
        documents.append(frozen_document)
    return tuple(documents)


def _has_exact_research_contract_identity(
    payload: Mapping[str, object],
) -> bool:
    symbol = str(payload.get("symbol", "")).strip().upper()
    structure = str(payload.get("structure", "")).strip().upper()
    legs = payload.get("legs")
    if (
        not symbol
        or not structure
        or not isinstance(legs, Sequence)
        or isinstance(legs, (str, bytes, bytearray, memoryview))
        or not legs
    ):
        return False
    seen_contracts: set[int] = set()
    for leg in legs:
        if not isinstance(leg, Mapping):
            return False
        con_id = leg.get("con_id")
        ratio = leg.get("ratio")
        if (
            isinstance(con_id, bool)
            or not isinstance(con_id, int)
            or con_id <= 0
            or con_id in seen_contracts
            or isinstance(ratio, bool)
            or not isinstance(ratio, int)
            or ratio <= 0
        ):
            return False
        try:
            expiration = datetime.fromisoformat(str(leg.get("expiration")))
            strike = Decimal(str(leg.get("strike")))
            multiplier = Decimal(str(leg.get("multiplier")))
        except (ArithmeticError, TypeError, ValueError):
            return False
        if (
            expiration.date().isoformat() != str(leg.get("expiration"))
            or not strike.is_finite()
            or strike <= 0
            or multiplier != Decimal("100")
            or str(leg.get("right", "")).strip().upper()
            not in {"C", "P", "CALL", "PUT"}
            or str(leg.get("side", "")).strip().upper()
            not in {"BUY", "SELL", "LONG", "SHORT"}
            or not str(leg.get("contract_id_ex") or "").strip()
            or not str(leg.get("exchange") or "").strip()
        ):
            return False
        seen_contracts.add(con_id)
    return True


def _option_pool_candidate_documents(
    candidates: tuple[_CandidateBinding, ...],
    scenarios: tuple[Mapping[str, Any], ...],
    costs: Mapping[str, Any],
    after_cost_evs: Mapping[str, Decimal],
    *,
    equity_theses: Mapping[str, object],
) -> tuple[FinalizedOptionPoolCandidate, ...]:
    """Bind final scenarios and signed costs into research-pool documents."""

    if len(candidates) != len(scenarios):
        raise ValueError("option pool candidate scenarios do not align")
    raw_cost_rows = costs.get("candidates", costs.get("candidate_costs", ()))
    if isinstance(raw_cost_rows, Mapping):
        cost_by_id = {
            str(candidate_id): _document(row)
            for candidate_id, row in raw_cost_rows.items()
        }
    else:
        cost_by_id = {
            str(_document(row).get("candidate_id", "")): _document(row)
            for row in raw_cost_rows
        }
    thesis_by_symbol = {
        str(row.get("symbol", "")).strip().upper(): row
        for row in equity_theses.get("rows", ())
        if isinstance(row, Mapping)
    }
    documents: list[FinalizedOptionPoolCandidate] = []
    for binding, scenario in zip(candidates, scenarios, strict=True):
        payload = thaw_json(binding.candidate_body)
        if not isinstance(payload, dict):
            raise TypeError("option pool candidate body is invalid")
        candidate_id = str(payload["candidate_id"])
        symbol = str(payload["symbol"]).strip().upper()
        cost_row = cost_by_id.get(candidate_id, {})
        after_cost_ev = after_cost_evs.get(candidate_id)
        thesis = thesis_by_symbol.get(symbol)
        if after_cost_ev is None or thesis is None:
            raise ValueError("option pool final evidence is incomplete")
        commission = _pipeline_decimal(
            cost_row.get("commission_usd", payload.get("estimated_commissions_usd"))
        )
        slippage = _pipeline_decimal(
            cost_row.get("slippage_usd", payload.get("estimated_slippage_usd"))
        )
        if commission is None or commission < 0 or slippage is None or slippage < 0:
            raise ValueError("option pool final costs are invalid")
        payload["estimated_commissions_usd"] = commission
        payload["estimated_slippage_usd"] = slippage
        payload["after_cost_ev_usd"] = after_cost_ev
        payload["source_candidate_hash"] = binding.candidate_hash
        payload["equity_thesis_hash"] = canonical_hash(
            {key: value for key, value in thesis.items() if key != "thesis_hash"}
        )
        scenario_pnl = _option_scenario_pnl(
            payload,
            scenario,
            commission=commission,
            slippage=slippage,
        )
        expected = sum(
            _pipeline_decimal(row["probability"]) * _pipeline_decimal(row["pnl_usd"])
            for row in scenario_pnl
        )
        if expected != after_cost_ev:
            raise ValueError("option pool scenario pnl does not match after-cost EV")
        payload["scenario_pnl"] = scenario_pnl
        payload["final_costs"] = {
            "cost_version": costs.get("cost_version", costs.get("version")),
            "cost_hash": costs.get("cost_hash", costs.get("hash", costs.get("contract_hash"))),
            "commission_usd": commission,
            "slippage_usd": slippage,
            "execution_cost_usd": commission + slippage,
            "calculation_hash": cost_row.get("calculation_hash"),
        }
        exit_plan = _document(payload.get("exit_plan"))
        payload["invalidation_evidence"] = {
            "status": "BOUND",
            "thesis_invalidation": exit_plan.get("thesis_invalidation"),
            "maximum_holding_date": exit_plan.get("maximum_holding_date"),
            "equity_thesis_hash": payload["equity_thesis_hash"],
        }
        legs = payload.get("legs")
        if not isinstance(legs, list):
            raise ValueError("option pool legs are invalid")
        short_evidence: list[Mapping[str, object]] = []
        for leg in legs:
            if not isinstance(leg, dict):
                raise ValueError("option pool leg is invalid")
            bid = _pipeline_decimal(leg.get("bid"))
            ask = _pipeline_decimal(leg.get("ask"))
            if bid is None or ask is None or ask < bid:
                raise ValueError("option pool executable quote is invalid")
            side = str(leg.get("side", "")).strip().upper()
            if side == "LONG":
                leg["side"] = "BUY"
            elif side == "SHORT":
                leg["side"] = "SELL"
            leg["liquidity"] = {
                "status": "MEASURED",
                "bid_ask_spread": ask - bid,
                "volume": leg.get("volume"),
                "open_interest": leg.get("open_interest"),
            }
            if str(leg.get("side", "")).upper() in {"SHORT", "SELL"}:
                proof = leg.get("short_leg_risk_evidence")
                if isinstance(proof, Mapping) and proof.get("status") == "SUPPORTED":
                    short_evidence.append(proof)
        short_count = sum(
            str(leg.get("side", "")).upper() in {"SHORT", "SELL"}
            for leg in legs
        )
        if short_count == 0:
            payload["assignment_evidence"] = {"status": "NOT_APPLICABLE"}
            payload["ex_dividend_evidence"] = {"status": "NOT_APPLICABLE"}
        elif len(short_evidence) == short_count:
            payload["assignment_evidence"] = {
                "status": "SUPPORTED",
                "short_leg_evidence": tuple(short_evidence),
            }
            payload["ex_dividend_evidence"] = {
                "status": "SUPPORTED",
                "short_leg_evidence": tuple(short_evidence),
            }
        if payload.get("max_profit_usd") is None:
            payload["max_profit_type"] = "UNBOUNDED"
        frozen_payload = freeze_json(payload)
        assert isinstance(frozen_payload, Mapping)
        documents.append(FinalizedOptionPoolCandidate(
            payload=frozen_payload,
            candidate_hash=canonical_hash(frozen_payload),
            source_candidate_hash=binding.candidate_hash,
            broker_snapshot_hash=str(payload.get("broker_snapshot_hash", "")),
            quote_batch_id=str(payload.get("quote_batch_id", "")),
            strategy_nav_hash=str(payload.get("strategy_nav_hash", "")),
            secdef_hash=str(payload.get("secdef_hash", "")),
            signed_cost_hash=str(payload["final_costs"].get("cost_hash", "")),
            scenario_hash=canonical_hash(payload["scenario_pnl"]),
        ))
    return tuple(documents)


def _option_scenario_pnl(
    candidate: Mapping[str, object],
    scenario: Mapping[str, Any],
    *,
    commission: Decimal,
    slippage: Decimal,
) -> tuple[Mapping[str, object], ...]:
    debit = _pipeline_decimal(candidate.get("debit_usd"))
    credit = _pipeline_decimal(candidate.get("credit_usd"))
    legs = candidate.get("legs")
    rows = scenario.get("scenarios")
    if (
        debit is None
        or credit is None
        or not isinstance(legs, Sequence)
        or isinstance(legs, (str, bytes, bytearray, memoryview))
        or not isinstance(rows, Sequence)
        or isinstance(rows, (str, bytes, bytearray, memoryview))
    ):
        raise ValueError("option scenario payoff inputs are invalid")
    results: list[Mapping[str, object]] = []
    for raw_scenario in rows:
        row = _document(raw_scenario)
        terminal = _pipeline_decimal(
            row.get("terminal_price", row.get("terminal_underlying_price"))
        )
        probability = _pipeline_decimal(row.get("probability"))
        if terminal is None or terminal < 0 or probability is None or probability <= 0:
            raise ValueError("option scenario term is invalid")
        pnl = credit - debit - commission - slippage
        for raw_leg in legs:
            leg = _document(raw_leg)
            strike = _pipeline_decimal(leg.get("strike"))
            multiplier = _pipeline_decimal(leg.get("multiplier"))
            ratio = leg.get("ratio")
            if (
                strike is None
                or multiplier is None
                or isinstance(ratio, bool)
                or not isinstance(ratio, int)
                or ratio <= 0
            ):
                raise ValueError("option scenario leg is invalid")
            right = str(leg.get("right", "")).upper()
            side = str(leg.get("side", "")).upper()
            intrinsic = (
                max(Decimal("0"), terminal - strike)
                if right in {"C", "CALL"}
                else max(Decimal("0"), strike - terminal)
                if right in {"P", "PUT"}
                else None
            )
            if intrinsic is None or side not in {"LONG", "BUY", "SHORT", "SELL"}:
                raise ValueError("option scenario leg direction is invalid")
            sign = Decimal("1") if side in {"LONG", "BUY"} else Decimal("-1")
            pnl += sign * Decimal(ratio) * multiplier * intrinsic
        results.append({
            "name": row.get("name"),
            "terminal_underlying_price": terminal,
            "probability": probability,
            "pnl_usd": pnl,
        })
    return tuple(results)

def _bind_candidates(
    candidates: tuple[object, ...],
    *,
    require_proposal: bool = True,
) -> tuple[tuple[_CandidateBinding, ...], str | None]:
    bindings: list[_CandidateBinding] = []
    for candidate in candidates:
        payload = getattr(candidate, "hash_payload", None)
        if not callable(payload):
            return (), "UNVERIFIED_GENERATOR_CANDIDATE_BODY"
        try:
            candidate_body = freeze_json(payload())
        except (TypeError, ValueError):
            return (), "INVALID_GENERATOR_CANDIDATE_BODY"
        if not isinstance(candidate_body, Mapping) or not _REQUIRED_CANDIDATE_BODY_FIELDS.issubset(candidate_body):
            return (), "INCOMPLETE_GENERATOR_CANDIDATE_BODY"
        raw = _document(candidate)
        supplied_evidence_inputs = raw.get("evidence_inputs")
        if supplied_evidence_inputs is not None:
            try:
                # Freeze transport-only evidence before invoking any additional
                # candidate callback.  A proposal_payload implementation must
                # not be able to erase or rewrite an attempted evidence override.
                supplied_evidence_inputs = freeze_json(supplied_evidence_inputs)
            except (TypeError, ValueError):
                return (), "CANDIDATE_EVIDENCE_MISMATCH"
        candidate_id = _string_or_none(raw.get("candidate_id"))
        if candidate_id is None or _string_or_none(candidate_body.get("candidate_id")) != candidate_id:
            return (), "CANDIDATE_ID_BODY_MISMATCH"
        supplied_candidate_hash = _hash_or_value(raw.get("candidate_hash"))
        computed_candidate_hash = canonical_hash(candidate_body)
        if supplied_candidate_hash is None or supplied_candidate_hash != computed_candidate_hash:
            return (), "CANDIDATE_HASH_BODY_MISMATCH"
        supplied_proposal = raw.get("proposal_hash")
        if supplied_proposal is not None and _hash_or_value(supplied_proposal) is None:
            return (), "INVALID_PROPOSAL_HASH"
        supplied_proposal_hash = _hash_or_value(supplied_proposal)
        if not require_proposal:
            bindings.append(
                _CandidateBinding(
                    candidate_body,
                    computed_candidate_hash,
                    None,
                    supplied_proposal_hash,
                    supplied_evidence_inputs,
                )
            )
            continue
        proposal_payload = getattr(candidate, "proposal_payload", None)
        if not callable(proposal_payload):
            return (), "INCOMPLETE_GENERATOR_PROPOSAL_BODY"
        try:
            normalized_proposal = freeze_json(proposal_payload())
        except (TypeError, ValueError) as exc:
            rejection_reason = _proposal_rejection_reason(exc)
            _LOGGER.warning(
                "Generated review-only proposal rejected for candidate %s: %s",
                candidate_id,
                rejection_reason,
            )
            return (), rejection_reason
        if not isinstance(normalized_proposal, Mapping) or not normalized_proposal:
            return (), "INVALID_GENERATOR_PROPOSAL_BODY"
        proposal_body: Mapping[str, Any] = normalized_proposal
        computed_proposal_hash = canonical_hash(proposal_body)
        if (
            supplied_proposal_hash is not None
            and supplied_proposal_hash != computed_proposal_hash
        ):
            return (), "PROPOSAL_HASH_BODY_MISMATCH"
        bindings.append(
            _CandidateBinding(
                candidate_body,
                computed_candidate_hash,
                proposal_body,
                supplied_proposal_hash,
                supplied_evidence_inputs,
            )
        )
    return tuple(bindings), None


_PROPOSAL_REJECTION_REASONS = {
    "candidate legs are incomplete": "CANDIDATE_LEGS_INCOMPLETE",
    "candidate expirations cannot pass the current proposal validator": (
        "CANDIDATE_EXPIRATION_INVALID"
    ),
    "candidate broker contract identities are invalid": (
        "CANDIDATE_CONTRACT_IDENTITY_INVALID"
    ),
    "candidate contains a nonstandard option contract": (
        "CANDIDATE_CONTRACT_STANDARD_INVALID"
    ),
    "candidate executable quote is invalid": "CANDIDATE_EXECUTABLE_QUOTE_INVALID",
    "candidate debit does not match executable legs": (
        "CANDIDATE_DEBIT_BINDING_INVALID"
    ),
    "candidate credit does not match executable legs": (
        "CANDIDATE_CREDIT_BINDING_INVALID"
    ),
    "candidate all-in cost does not match executable legs": (
        "CANDIDATE_ALL_IN_COST_BINDING_INVALID"
    ),
    "candidate maximum loss is not exactly computable": (
        "CANDIDATE_MAXIMUM_LOSS_UNAVAILABLE"
    ),
    "candidate maximum loss does not match exact payoff": (
        "CANDIDATE_MAXIMUM_LOSS_BINDING_INVALID"
    ),
    "candidate maximum profit does not match exact payoff": (
        "CANDIDATE_MAXIMUM_PROFIT_BINDING_INVALID"
    ),
    "candidate breakevens do not match exact payoff": (
        "CANDIDATE_BREAKEVEN_BINDING_INVALID"
    ),
    "candidate terminal scenarios are incomplete": (
        "CANDIDATE_TERMINAL_SCENARIOS_INCOMPLETE"
    ),
    "candidate terminal scenarios have nonpositive after-cost EV": (
        "CANDIDATE_AFTER_COST_EV_NONPOSITIVE"
    ),
    "candidate liquidity score is outside zero to one": (
        "CANDIDATE_LIQUIDITY_SCORE_INVALID"
    ),
    "candidate risk reaches the absolute 20 percent reject line": (
        "CANDIDATE_HARD_RISK_CAP_REJECTED"
    ),
    "candidate evidence hashes are unavailable": "CANDIDATE_EVIDENCE_UNAVAILABLE",
    "candidate evidence hashes are incomplete": "CANDIDATE_EVIDENCE_INCOMPLETE",
}


def _proposal_rejection_reason(error: Exception) -> str:
    return _PROPOSAL_REJECTION_REASONS.get(
        str(error).strip(),
        "INVALID_GENERATOR_PROPOSAL_BODY",
    )


def _finalize_scenario_candidates(
    candidates: tuple[object, ...],
    skeleton_bindings: tuple[_CandidateBinding, ...],
    scenarios: tuple[Mapping[str, Any], ...],
) -> tuple[
    tuple[object, ...],
    tuple[_CandidateBinding, ...],
    str | None,
]:
    """Bind trusted scenario output without permitting unrelated drift."""

    if len(candidates) != len(skeleton_bindings) or len(candidates) != len(
        scenarios
    ):
        return (), (), "CANDIDATE_SCENARIO_FINALIZATION_INVALID"

    finalized: list[object] = []
    for candidate, binding, scenario in zip(
        candidates,
        skeleton_bindings,
        scenarios,
        strict=True,
    ):
        scenario_terms = _scenario_terms(
            scenario.get("scenarios"),
            price_fields=("terminal_price", "terminal_underlying_price"),
        )
        if scenario_terms is None:
            return (), (), "CANDIDATE_SCENARIO_FINALIZATION_INVALID"
        candidate_terms = _scenario_terms(
            binding.candidate_body.get("terminal_scenarios"),
            price_fields=("terminal_underlying_price",),
        )
        final_candidate = candidate
        if candidate_terms != scenario_terms:
            finalizer = getattr(candidate, "finalize_scenarios", None)
            if callable(finalizer):
                try:
                    final_candidate = finalizer(scenario.get("scenarios", ()))
                except (ArithmeticError, TypeError, ValueError):
                    return (), (), "CANDIDATE_SCENARIO_FINALIZATION_INVALID"
        finalized.append(final_candidate)

    finalized_candidates = tuple(finalized)
    # Preserve the finalized candidate body even when the later proposal gate
    # rejects it.  This lets the read-only option pool retain rejected
    # economics without granting proposal, ranking, or approval authority.
    finalized_bindings, binding_error = _bind_candidates(
        finalized_candidates,
        require_proposal=False,
    )
    if binding_error is not None:
        return (), (), binding_error

    for skeleton, completed, scenario in zip(
        skeleton_bindings,
        finalized_bindings,
        scenarios,
        strict=True,
    ):
        skeleton_body = dict(skeleton.candidate_body)
        completed_body = dict(completed.candidate_body)
        skeleton_body.pop("terminal_scenarios", None)
        completed_body.pop("terminal_scenarios", None)
        try:
            unchanged = freeze_json(skeleton_body) == freeze_json(completed_body)
            evidence_unchanged = (
                freeze_json(skeleton.supplied_evidence_inputs)
                == freeze_json(completed.supplied_evidence_inputs)
            )
        except (TypeError, ValueError):
            return (), (), "CANDIDATE_SCENARIO_FINALIZATION_DRIFT"
        completed_terms = _scenario_terms(
            completed.candidate_body.get("terminal_scenarios"),
            price_fields=("terminal_underlying_price",),
        )
        scenario_terms = _scenario_terms(
            scenario.get("scenarios"),
            price_fields=("terminal_price", "terminal_underlying_price"),
        )
        if not unchanged or not evidence_unchanged:
            return (), (), "CANDIDATE_SCENARIO_FINALIZATION_DRIFT"
        if completed_terms != scenario_terms:
            return (), (), "PROPOSAL_SCENARIO_BINDING_MISMATCH"

    proposal_bindings, proposal_error = _bind_candidates(finalized_candidates)
    if proposal_error is None:
        return finalized_candidates, proposal_bindings, None
    if proposal_error == "CANDIDATE_AFTER_COST_EV_NONPOSITIVE":
        return finalized_candidates, finalized_bindings, None
    return (), (), proposal_error


def _proposal_binding_error(
    candidates: tuple[_CandidateBinding, ...],
    scenarios: tuple[Mapping[str, Any], ...],
    after_cost_evs: Mapping[str, Decimal],
) -> str | None:
    """Require the frozen proposal to describe the exact ranked economics.

    The generator runs before the scenario and signed-cost ports.  A proposal
    is therefore never eligible merely because it is structurally complete:
    its candidate identity, deterministic scenario distribution, and declared
    cost-after EV must match the values independently recomputed later in this
    same pipeline run.
    """

    if len(candidates) != len(scenarios):
        return "PROPOSAL_SCENARIO_BINDING_MISMATCH"
    for binding, scenario in zip(candidates, scenarios, strict=True):
        proposal = binding.proposal_body
        if proposal is None:
            return "INCOMPLETE_GENERATOR_PROPOSAL_BODY"
        candidate_id = str(binding.candidate_body["candidate_id"])
        proposal_rank = proposal.get("rank")
        if (
            proposal.get("schema") != "options_copilot.proposal.v1"
            or proposal.get("review_only") is not True
            or proposal.get("eligible_to_send") is not True
            or isinstance(proposal_rank, bool)
            or proposal_rank != 1
            or proposal.get("proposal_id") != candidate_id
            or proposal.get("candidate_id") != candidate_id
            or proposal.get("candidate_hash") != binding.candidate_hash
            or proposal.get("quote_snapshot_id")
            != binding.candidate_body.get("quote_batch_id")
        ):
            return "PROPOSAL_CANDIDATE_BINDING_MISMATCH"
        if not _proposal_matches_candidate(binding.candidate_body, proposal):
            return "PROPOSAL_CANDIDATE_CONTENT_MISMATCH"

        adjusted = after_cost_evs.get(candidate_id)
        declared_ev = _pipeline_decimal(proposal.get("expected_value_usd"))
        if adjusted is None or declared_ev is None or declared_ev != adjusted:
            return "PROPOSAL_AFTER_COST_EV_MISMATCH"

        proposal_terms = _scenario_terms(
            proposal.get("terminal_scenarios"),
            price_fields=("terminal_underlying_price",),
        )
        candidate_terms = _scenario_terms(
            binding.candidate_body.get("terminal_scenarios"),
            price_fields=("terminal_underlying_price",),
        )
        scenario_terms = _scenario_terms(
            scenario.get("scenarios"),
            price_fields=("terminal_price", "terminal_underlying_price"),
        )
        if (
            proposal_terms is None
            or candidate_terms is None
            or scenario_terms is None
            or proposal_terms != candidate_terms
            or proposal_terms != scenario_terms
        ):
            return "PROPOSAL_SCENARIO_BINDING_MISMATCH"
    return None


def _proposal_matches_candidate(
    candidate: Mapping[str, Any], proposal: Mapping[str, Any]
) -> bool:
    """Bind every authority-bearing proposal field to the frozen candidate."""

    try:
        symbol = str(candidate["symbol"])
        if (
            proposal.get("symbol") != symbol
            or proposal.get("underlying") != symbol
            or proposal.get("structure") != candidate.get("structure")
            or proposal.get("dte") != candidate.get("dte")
            or proposal.get("broker_snapshot_hash")
            != candidate.get("broker_snapshot_hash")
            or proposal.get("secdef_hash") != candidate.get("secdef_hash")
        ):
            return False

        nav = _document(proposal.get("strategy_nav"))
        if (
            _pipeline_decimal(nav.get("strategy_nav_usd"))
            != _pipeline_decimal(candidate.get("strategy_nav_usd"))
            or nav.get("authority_hash") != candidate.get("strategy_nav_hash")
            or nav.get("content_hash")
            != candidate.get("strategy_nav_content_hash")
            or nav.get("contract_hash")
            != candidate.get("strategy_nav_contract_hash")
            or nav.get("ledger_head_hash")
            != candidate.get("strategy_nav_ledger_head_hash")
            or _pipeline_decimal(nav.get("observed_account_nlv"))
            != _pipeline_decimal(
                candidate.get("strategy_nav_observed_account_nlv")
            )
            or _pipeline_decimal(nav.get("reconciliation_difference"))
            != _pipeline_decimal(
                candidate.get("strategy_nav_reconciliation_difference")
            )
            or _timestamp_text(nav.get("asof"))
            != _timestamp_text(candidate.get("strategy_nav_asof"))
        ):
            return False
        policy = _document(proposal.get("policy"))
        if policy != {
            "version": candidate.get("policy_version"),
            "hash": candidate.get("policy_hash"),
            "dte_exception_hash": candidate.get("dte_exception_hash"),
        }:
            return False
        cost = _document(proposal.get("execution_cost_contract"))
        if cost != {
            "version": candidate.get("execution_cost_contract_version"),
            "hash": candidate.get("execution_cost_contract_hash"),
        }:
            return False
        if freeze_json(proposal.get("evidence_hashes")) != freeze_json(
            candidate.get("evidence_hashes")
        ):
            return False
        if freeze_json(proposal.get("exit_plan")) != freeze_json(
            candidate.get("exit_plan")
        ):
            return False

        maximum_loss = _pipeline_decimal(candidate.get("max_loss_usd"))
        strategy_nav = _pipeline_decimal(candidate.get("strategy_nav_usd"))
        risk = _document(proposal.get("risk"))
        if (
            maximum_loss is None
            or strategy_nav is None
            or strategy_nav <= 0
            or risk.get("defined_risk") is not True
            or _pipeline_decimal(risk.get("maximum_loss_usd")) != maximum_loss
            or _pipeline_decimal(risk.get("risk_fraction"))
            != maximum_loss / strategy_nav
            or not _optional_decimal_equal(
                risk.get("maximum_profit_usd"), candidate.get("max_profit_usd")
            )
            or _decimal_sequence(risk.get("breakevens"))
            != _decimal_sequence(candidate.get("breakevens"))
        ):
            return False

        debit = _pipeline_decimal(candidate.get("debit_usd"))
        credit = _pipeline_decimal(candidate.get("credit_usd"))
        all_in = _pipeline_decimal(candidate.get("all_in_cost_usd"))
        pricing = _document(proposal.get("pricing"))
        if debit is None or credit is None or all_in is None:
            return False
        reference = debit - credit
        if (
            _pipeline_decimal(pricing.get("reference_cost_usd")) != reference
            or _pipeline_decimal(pricing.get("all_in_executable_cost_usd"))
            != all_in
        ):
            return False
        if reference >= 0:
            if _pipeline_decimal(pricing.get("net_debit_usd")) != reference:
                return False
        elif _pipeline_decimal(pricing.get("net_credit_usd")) != -reference:
            return False
        commissions = _pipeline_decimal(
            candidate.get("estimated_commissions_usd")
        )
        slippage = _pipeline_decimal(candidate.get("estimated_slippage_usd"))
        if commissions is not None or slippage is not None:
            if commissions is None or slippage is None:
                return False
            if (
                _pipeline_decimal(pricing.get("estimated_commissions_usd"))
                != commissions
                or _pipeline_decimal(pricing.get("estimated_slippage_usd"))
                != slippage
                or _pipeline_decimal(pricing.get("estimated_execution_costs_usd"))
                != commissions + slippage
            ):
                return False

        candidate_legs = candidate.get("legs")
        proposal_legs = proposal.get("legs")
        if not _proposal_legs_match(
            candidate_legs,
            proposal_legs,
            quote_batch_id=str(candidate.get("quote_batch_id", "")),
            symbol=symbol,
        ):
            return False
        expirations = {
            str(_document(item).get("expiration"))
            for item in candidate_legs
            if _document(item).get("expiration") is not None
        }
        if expirations and (
            len(expirations) != 1 or proposal.get("expiration") not in expirations
        ):
            return False
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return False
    return True


def _proposal_legs_match(
    candidate_legs: object,
    proposal_legs: object,
    *,
    quote_batch_id: str,
    symbol: str,
) -> bool:
    if (
        not isinstance(candidate_legs, Sequence)
        or isinstance(candidate_legs, (str, bytes, bytearray, memoryview))
        or not isinstance(proposal_legs, Sequence)
        or isinstance(proposal_legs, (str, bytes, bytearray, memoryview))
        or len(candidate_legs) == 0
        or len(candidate_legs) != len(proposal_legs)
        or not quote_batch_id
    ):
        return False
    decimal_fields = ("strike", "multiplier", "bid", "ask", "last", "implied_volatility")
    text_fields = (
        "con_id",
        "contract_id_ex",
        "underlying",
        "security_type",
        "expiration",
        "right",
        "currency",
        "exchange",
        "local_symbol",
        "trading_class",
    )
    for raw_candidate, raw_proposal in zip(
        candidate_legs, proposal_legs, strict=True
    ):
        candidate = _document(raw_candidate)
        proposal = _document(raw_proposal)
        if not candidate or not proposal:
            return False
        candidate_side = str(candidate.get("side", "")).upper()
        proposal_side = str(proposal.get("side", "")).upper()
        expected_side = {"LONG": "BUY", "SHORT": "SELL"}.get(candidate_side)
        if expected_side is None or proposal_side not in {expected_side, candidate_side}:
            return False
        ratio = candidate.get("ratio")
        quantity = proposal.get("quantity", proposal.get("ratio"))
        if (
            isinstance(ratio, bool)
            or not isinstance(ratio, int)
            or ratio <= 0
            or isinstance(quantity, bool)
            or not isinstance(quantity, int)
            or quantity != ratio
            or (
                "ratio" in proposal
                and (
                    isinstance(proposal.get("ratio"), bool)
                    or proposal.get("ratio") != ratio
                )
            )
        ):
            return False
        for field in text_fields:
            if field in candidate and proposal.get(field) != candidate.get(field):
                return False
        if candidate.get("underlying", symbol) != symbol:
            return False
        for field in decimal_fields:
            if field in candidate and not _optional_decimal_equal(
                proposal.get(field), candidate.get(field)
            ):
                return False
        for field in ("volume", "open_interest"):
            if field in candidate and proposal.get(field) != candidate.get(field):
                return False
        observed_at = candidate.get("observed_at")
        if not _timestamp_equal(
            proposal.get("quote_time", proposal.get("observed_at")), observed_at
        ):
            return False
        if proposal.get("quote_snapshot_id", quote_batch_id) != quote_batch_id:
            return False
    return True


def _optional_decimal_equal(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return _pipeline_decimal(left) == _pipeline_decimal(right)


def _timestamp_equal(left: object, right: object) -> bool:
    if not isinstance(left, str) or not isinstance(right, str):
        return False
    try:
        left_value = datetime.fromisoformat(left.replace("Z", "+00:00"))
        right_value = datetime.fromisoformat(right.replace("Z", "+00:00"))
    except ValueError:
        return False
    if (
        left_value.tzinfo is None
        or left_value.utcoffset() is None
        or right_value.tzinfo is None
        or right_value.utcoffset() is None
    ):
        return False
    return left_value.astimezone(timezone.utc) == right_value.astimezone(timezone.utc)


def _decimal_sequence(value: object) -> tuple[Decimal, ...] | None:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return None
    parsed = tuple(_pipeline_decimal(item) for item in value)
    if any(item is None for item in parsed):
        return None
    return tuple(item for item in parsed if item is not None)


def _scenario_terms(
    value: object,
    *,
    price_fields: tuple[str, ...],
) -> tuple[tuple[Decimal, Decimal], ...] | None:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ) or not value:
        return None
    combined: dict[Decimal, Decimal] = {}
    for item in value:
        row = _document(item)
        raw_price = next(
            (row.get(field) for field in price_fields if field in row),
            None,
        )
        price = _pipeline_decimal(raw_price)
        probability = _pipeline_decimal(row.get("probability"))
        if price is None or price < Decimal("0") or probability is None or probability <= Decimal("0"):
            return None
        combined[price] = combined.get(price, Decimal("0")) + probability
    if sum(combined.values(), Decimal("0")) != Decimal("1"):
        return None
    return tuple(sorted(combined.items(), key=lambda item: item[0]))


def _pipeline_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _scenario_decision_records(
    scan_run_id: str,
    candidates: tuple[_CandidateBinding, ...],
    scenarios: tuple[Mapping[str, Any], ...],
) -> tuple[Mapping[str, object], ...]:
    if len(candidates) != len(scenarios):
        raise ValueError("scenario results do not align with canonical candidates")
    records: list[Mapping[str, object]] = []
    for candidate, scenario in zip(candidates, scenarios):
        record = freeze_json(
            {
                "schema": "options_copilot.scenario_decision_record.v1",
                "scan_run_id": scan_run_id,
                "candidate_id": candidate.candidate_body["candidate_id"],
                "candidate_hash": candidate.candidate_hash,
                "scenario": _canonical_value(scenario),
            }
        )
        assert isinstance(record, Mapping)
        records.append({"record_type": "SCENARIO", "record": record})
    return tuple(records)


def _candidate_evidence_manifests(
    candidates: tuple[_CandidateBinding, ...],
    after_cost_evs: Mapping[str, Decimal],
    references: object,
    *,
    cutoff_at: datetime,
    ranking_valid_until: datetime,
) -> tuple[Mapping[str, object], str | None]:
    """Freeze all candidate manifests or reject the entire ranking attempt."""

    if not isinstance(references, Mapping):
        return {}, "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
    candidate_ids = [str(item.candidate_body["candidate_id"]) for item in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        return {}, "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
    if any(
        not isinstance(candidate_id, str)
        or not candidate_id
        or candidate_id not in set(candidate_ids)
        for candidate_id in references
    ):
        return {}, "CANDIDATE_EVIDENCE_MANIFEST_INVALID"

    manifests: dict[str, object] = {}
    by_id = {str(item.candidate_body["candidate_id"]): item for item in candidates}
    for candidate_id in sorted(by_id):
        if candidate_id not in references:
            reference_doc: Mapping[str, object] = {
                "supporting": (),
                "contradicting": (),
            }
        else:
            supplied = references[candidate_id]
            if isinstance(supplied, Mapping) and set(supplied) == {
                "supporting",
                "contradicting",
            }:
                reference_doc = supplied
            else:
                return {}, "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        ev = after_cost_evs.get(candidate_id)
        if ev is None:
            return {}, "CANDIDATE_EVIDENCE_PRIMARY_INVALID"
        try:
            manifest = build_candidate_evidence_manifest(
                by_id[candidate_id].candidate_body,
                after_cost_expected_value=ev,
                cutoff_at=cutoff_at,
                ranking_valid_until=ranking_valid_until,
                now=cutoff_at,
                supporting=reference_doc["supporting"],
                contradicting=reference_doc["contradicting"],
            )
            frozen = freeze_json(manifest)
        except CandidateEvidenceManifestError as exc:
            return {}, exc.reason
        except (TypeError, ValueError):
            return {}, "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        if not isinstance(frozen, Mapping):
            return {}, "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        manifests[candidate_id] = frozen
    frozen_manifests = freeze_json(manifests)
    if not isinstance(frozen_manifests, Mapping):
        return {}, "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
    return frozen_manifests, None


def _ranking_inputs(
    candidates: tuple[_CandidateBinding, ...],
    after_cost_evs: Mapping[str, Decimal],
    gate: Mapping[str, Any],
    *,
    policy_version: str,
    policy_hash: str,
    policy_marker_hash: str,
    cost_version: str,
    cost_hash: str,
    risk_contract_hash: str,
    evidence_inputs: Mapping[str, object],
    joint_ranking: JointRankingSnapshot | None = None,
) -> tuple[tuple[dict[str, object], ...], str | None]:
    gate_risk_fractions, risk_error = _bound_gate_risk_fractions(candidates, gate)
    if risk_error is not None:
        return (), risk_error
    rows: list[dict[str, object]] = []
    trusted_evidence = freeze_json(evidence_inputs)
    assert isinstance(trusted_evidence, Mapping)
    joint_rows = (
        {row.candidate_id: row for row in joint_ranking.executable}
        if joint_ranking is not None
        else {}
    )
    for binding in candidates:
        body = binding.candidate_body
        raw = dict(body)
        candidate_id = str(body["candidate_id"])
        supplied_evidence = raw.get("evidence_inputs")
        try:
            for candidate_evidence in (
                supplied_evidence,
                binding.supplied_evidence_inputs,
            ):
                if (
                    candidate_evidence is not None
                    and freeze_json(candidate_evidence) != trusted_evidence
                ):
                    return (), "CANDIDATE_EVIDENCE_MISMATCH"
        except (TypeError, ValueError):
            return (), "CANDIDATE_EVIDENCE_MISMATCH"
        if body.get("policy_version") != policy_version or body.get("policy_hash") != policy_hash:
            return (), "CANDIDATE_POLICY_BINDING_MISMATCH"
        if body.get("execution_cost_contract_version") != cost_version or body.get("execution_cost_contract_hash") != cost_hash:
            return (), "CANDIDATE_COST_BINDING_MISMATCH"
        if body.get("broker_snapshot_hash") != trusted_evidence.get("broker_snapshot_hash"):
            return (), "CANDIDATE_BROKER_BINDING_MISMATCH"
        try:
            basis = build_ranking_basis(
                candidate_body=body,
                proposal_body=binding.proposal_body,
                candidate_hash=binding.candidate_hash,
                proposal_hash=binding.supplied_proposal_hash,
                current_policy_version=policy_version,
                current_policy_hash=policy_hash,
                policy_authority_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
                evidence_inputs=trusted_evidence,
            )
        except (TypeError, ValueError):
            return (), "CANDIDATE_OR_PROPOSAL_BINDING_INVALID"
        raw["candidate_id"] = candidate_id
        raw["underlying"] = body["symbol"]
        raw["structure"] = body["structure"]
        raw["dte"] = body["dte"]
        raw["evidence_hashes"] = body["evidence_hashes"]
        raw["eligible"] = bool(gate.get("eligible", False))
        raw["after_cost_expected_value"] = after_cost_evs.get(candidate_id)
        raw["max_loss"] = body["max_loss_usd"]
        raw["liquidity_score"] = body["liquidity_score"]
        # Risk tier authority is always derived locally from the frozen loss and
        # Strategy NAV.  Neither the gate nor the ranker may relabel the tier.
        raw["risk_fraction"] = gate_risk_fractions[candidate_id]
        raw["proposal_hash"] = basis.proposal_hash
        raw["proposal_body"] = basis.proposal_body
        raw["candidate_hash"] = basis.candidate_hash
        raw["candidate_body"] = basis.candidate_body
        raw["ranking_basis_hash"] = basis.ranking_basis_hash
        raw["evidence_inputs"] = basis.evidence_inputs
        raw["open_combinations"] = gate.get("open_combinations", 0)
        raw["supporting_bonus"] = Decimal("0")
        if joint_ranking is not None:
            joint_row = joint_rows.get(candidate_id)
            if (
                joint_row is None
                or joint_row.candidate_hash
                != next(
                    (
                        item.candidate_hash
                        for item in joint_ranking.executable
                        if item.candidate_id == candidate_id
                    ),
                    None,
                )
            ):
                return (), "JOINT_RANKING_BINDING_MISMATCH"
            raw["joint_score"] = joint_row.score
            raw["joint_score_components"] = joint_row.score_components
            raw["joint_row_hash"] = joint_row.row_hash
            raw["joint_snapshot_hash"] = joint_ranking.snapshot_hash
            raw["joint_candidate_hash"] = joint_row.candidate_hash
        raw.pop("structure_priority", None)
        raw["policy_hash"], raw["cost_hash"] = policy_hash, cost_hash
        rows.append(raw)
    return tuple(rows), None


def _bound_gate_risk_fractions(
    candidates: tuple[_CandidateBinding, ...],
    gate: Mapping[str, Any],
) -> tuple[dict[str, Decimal], str | None]:
    """Bind every gate risk result to its exact frozen candidate.

    Production gates may evaluate more than one candidate at a time and expose
    a ``risk_fractions`` mapping keyed by candidate id.  The legacy scalar is
    retained for single-value callers, but it is still checked independently
    against every candidate and therefore cannot silently stand in for unlike
    candidate risks.
    """

    candidate_ids = tuple(str(item.candidate_body["candidate_id"]) for item in candidates)
    raw_by_candidate = gate.get("risk_fractions")
    if raw_by_candidate is not None:
        if not isinstance(raw_by_candidate, Mapping):
            return {}, "GATE_RISK_BINDING_MISMATCH"
        if set(raw_by_candidate) != set(candidate_ids):
            return {}, "GATE_RISK_BINDING_MISMATCH"
        selected = {candidate_id: raw_by_candidate[candidate_id] for candidate_id in candidate_ids}
    else:
        if "risk_fraction" not in gate:
            return {}, "GATE_RISK_BINDING_MISMATCH"
        selected = {candidate_id: gate["risk_fraction"] for candidate_id in candidate_ids}

    bound: dict[str, Decimal] = {}
    for binding in candidates:
        body = binding.candidate_body
        candidate_id = str(body["candidate_id"])
        maximum_loss = _pipeline_decimal(body.get("max_loss_usd"))
        strategy_nav = _pipeline_decimal(body.get("strategy_nav_usd"))
        if (
            maximum_loss is None
            or maximum_loss <= 0
            or strategy_nav is None
            or strategy_nav <= 0
        ):
            return {}, "CANDIDATE_RISK_BINDING_INVALID"
        supplied = _pipeline_decimal(selected[candidate_id])
        expected = maximum_loss / strategy_nav
        if supplied is None or supplied < 0 or supplied != expected:
            return {}, "GATE_RISK_BINDING_MISMATCH"
        bound[candidate_id] = expected
    return bound, None

def _finalized_store_rows(
    items: Sequence[object],
    candidates: tuple[_CandidateBinding, ...],
    source_candidates: Sequence[Mapping[str, object]],
    *,
    policy_version: str,
    policy_hash: str,
    policy_marker_hash: str,
    cost_version: str,
    cost_hash: str,
    risk_contract_hash: str,
    evidence_inputs: Mapping[str, object],
    risk_authority: object,
    ranked: bool,
) -> tuple[dict[str, object], ...]:
    """Rebuild proposal authority only after the final rank is known.

    Generator proposals are complete rank-one-shaped review drafts so the
    independent proposal validator can recompute their economics before
    ranking.  They are not allowed to carry that provisional rank into the
    immutable ledger.  This boundary reuses the generator-bound body, writes
    the actual final rank/eligibility, and recomputes both the proposal and
    ranking-basis hashes.  Ranker-returned bodies and hashes are treated as
    untrusted transport data.
    """

    by_id = {
        str(candidate.candidate_body["candidate_id"]): candidate
        for candidate in candidates
    }
    source_by_id = {
        str(candidate.get("candidate_id")): candidate
        for candidate in source_candidates
    }
    if set(source_by_id) != set(by_id):
        raise ValueError("ranker source candidates are incomplete or duplicated")
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, item in enumerate(items, start=1):
        raw = _document(item)
        candidate_id = _string_or_none(raw.get("candidate_id"))
        expected_rank: int | None = index if ranked else None
        returned_rank = raw.get("rank")
        if (
            candidate_id is None
            or candidate_id in seen
            or candidate_id not in by_id
            or isinstance(returned_rank, bool)
            or returned_rank != expected_rank
        ):
            raise ValueError("ranker returned an invalid candidate identity or rank")
        seen.add(candidate_id)
        binding = by_id[candidate_id]
        if (
            _hash_or_value(raw.get("candidate_hash")) != binding.candidate_hash
            or freeze_json(raw.get("candidate_body")) != binding.candidate_body
        ):
            raise ValueError("ranker changed the canonical candidate binding")

        source = source_by_id[candidate_id]
        authority_status, authorizable = _expected_authority_state(
            source.get("risk_fraction"),
            risk_authority,
            proposal_hash=_hash_or_value(source.get("proposal_hash")),
            candidate_hash=_hash_or_value(source.get("candidate_hash")),
            ranking_basis_hash=_hash_or_value(source.get("ranking_basis_hash")),
            policy_version=policy_version,
            policy_hash=policy_hash,
            policy_marker_hash=policy_marker_hash,
            cost_version=cost_version,
            cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
        )
        if (
            raw.get("authority_status") != authority_status
            or raw.get("authorizable") is not authorizable
        ):
            raise ValueError("ranker changed the current risk authority state")
        if ranked:
            if not authorizable or authority_status == "A_GRADE_PENDING":
                raise ValueError("ranker returned a non-authorizable ranked candidate")
        elif authorizable or authority_status != "A_GRADE_PENDING":
            raise ValueError("governance evidence has invalid authority state")

        proposal_body = dict(binding.proposal_body)
        proposal_body["rank"] = expected_rank
        proposal_body["eligible_to_send"] = bool(ranked and index == 1 and authorizable)
        finalized = freeze_json(proposal_body)
        if not isinstance(finalized, Mapping):
            raise ValueError("finalized proposal is not canonical")
        basis = build_ranking_basis(
            candidate_body=binding.candidate_body,
            proposal_body=finalized,
            candidate_hash=binding.candidate_hash,
            current_policy_version=policy_version,
            current_policy_hash=policy_hash,
            policy_authority_marker_hash=policy_marker_hash,
            cost_version=cost_version,
            cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
            evidence_inputs=evidence_inputs,
        )
        rows.append(
            {
                "candidate_id": candidate_id,
                "proposal_hash": basis.proposal_hash,
                "proposal_body": basis.proposal_body,
                "candidate_hash": basis.candidate_hash,
                "candidate_body": basis.candidate_body,
                "ranking_basis_hash": basis.ranking_basis_hash,
                "evidence_inputs": basis.evidence_inputs,
                "authority_status": authority_status,
                "authorizable": authorizable,
                "score_components": raw.get(
                    "score_components", {"score": raw.get("score")}
                ),
            }
        )
    return tuple(rows)


def _expected_authority_state(
    risk_fraction_value: object,
    risk_authority: object,
    *,
    proposal_hash: str | None,
    candidate_hash: str | None,
    ranking_basis_hash: str | None,
    policy_version: str,
    policy_hash: str,
    policy_marker_hash: str,
    cost_version: str,
    cost_hash: str,
    risk_contract_hash: str,
) -> tuple[str, bool]:
    risk_fraction = _pipeline_decimal(risk_fraction_value)
    if risk_fraction is None or risk_fraction < Decimal("0"):
        raise ValueError("candidate risk fraction is unavailable")
    if risk_fraction <= Decimal("0.10"):
        return "NORMAL", True
    if risk_fraction > Decimal("0.15"):
        raise ValueError("candidate risk exceeds the A-grade ceiling")
    approved = _a_grade_binding_matches(
        risk_authority,
        proposal_hash=proposal_hash,
        candidate_hash=candidate_hash,
        ranking_basis_hash=ranking_basis_hash,
        policy_version=policy_version,
        policy_hash=policy_hash,
        policy_marker_hash=policy_marker_hash,
        cost_version=cost_version,
        cost_hash=cost_hash,
        risk_contract_hash=risk_contract_hash,
    )
    return ("A_GRADE", True) if approved else ("A_GRADE_PENDING", False)


def _resolve_bound_risk_authority(
    resolver: object,
    *,
    initial_authority: object,
    candidates: Sequence[Mapping[str, object]],
    now: datetime,
    resolved_policy: object,
    policy_version: str,
    policy_hash: str,
    policy_marker_hash: str,
    cost_version: str,
    cost_hash: str,
    risk_contract_hash: str,
) -> tuple[object, str | None]:
    elevated = sorted(
        (
            row
            for row in candidates
            if (
                (risk_fraction := _pipeline_decimal(row.get("risk_fraction")))
                is not None
                and Decimal("0.10") < risk_fraction <= Decimal("0.15")
            )
        ),
        key=lambda row: str(row.get("candidate_id", "")),
    )
    if not elevated:
        return initial_authority, None

    current = initial_authority
    for row in elevated:
        try:
            current = _resolve_once(
                resolver,
                now=now,
                current_policy=resolved_policy,
                resolved_policy=resolved_policy,
                proposal_hash=row.get("proposal_hash"),
                candidate_hash=row.get("candidate_hash"),
                execution_cost_version=cost_version,
                execution_cost_hash=cost_hash,
                ranking_basis_hash=row.get("ranking_basis_hash"),
            )
        except (TypeError, ValueError):
            return initial_authority, "RISK_AUTHORITY_BINDING_INVALID"
        if _is_a_grade_authority(current):
            if not _a_grade_binding_matches(
                current,
                proposal_hash=_hash_or_value(row.get("proposal_hash")),
                candidate_hash=_hash_or_value(row.get("candidate_hash")),
                ranking_basis_hash=_hash_or_value(row.get("ranking_basis_hash")),
                policy_version=policy_version,
                policy_hash=policy_hash,
                policy_marker_hash=policy_marker_hash,
                cost_version=cost_version,
                cost_hash=cost_hash,
                risk_contract_hash=risk_contract_hash,
            ):
                return initial_authority, "RISK_AUTHORITY_BINDING_MISMATCH"
            return current, None
        if not _is_unbound_normal_authority(current):
            return initial_authority, "RISK_AUTHORITY_BINDING_MISMATCH"
        _, _, current_contract_hash = _risk_identity(current)
        if current_contract_hash != risk_contract_hash:
            return initial_authority, "RISK_AUTHORITY_BINDING_MISMATCH"
    return current, None


def _is_unbound_normal_authority(authority: object) -> bool:
    doc = _document(authority)
    return bool(
        _value_text(doc.get("tier")) == "NORMAL"
        and doc.get("a_grade_approved") is False
    )


def _is_a_grade_authority(authority: object) -> bool:
    doc = _document(authority)
    return bool(
        _value_text(doc.get("tier")) == "A_GRADE"
        and doc.get("a_grade_approved") is True
        and _hash_or_value(
            doc.get("risk_authority_marker_hash", doc.get("marker_hash"))
        )
        is not None
    )


def _a_grade_binding_matches(
    authority: object,
    *,
    proposal_hash: str | None,
    candidate_hash: str | None,
    ranking_basis_hash: str | None,
    policy_version: str,
    policy_hash: str,
    policy_marker_hash: str,
    cost_version: str,
    cost_hash: str,
    risk_contract_hash: str,
) -> bool:
    if (
        proposal_hash is None
        or candidate_hash is None
        or ranking_basis_hash is None
        or not _is_a_grade_authority(authority)
    ):
        return False
    doc = _document(authority)
    return bool(
        _string_or_none(doc.get("current_policy_version")) == policy_version
        and _hash_or_value(doc.get("current_policy_hash")) == policy_hash
        and _hash_or_value(doc.get("policy_authority_marker_hash"))
        == policy_marker_hash
        and _hash_or_value(doc.get("risk_contract_hash")) == risk_contract_hash
        and _hash_or_value(doc.get("proposal_hash")) == proposal_hash
        and _hash_or_value(doc.get("candidate_hash")) == candidate_hash
        and _string_or_none(doc.get("execution_cost_version")) == cost_version
        and _hash_or_value(doc.get("execution_cost_hash")) == cost_hash
        and _hash_or_value(doc.get("ranking_basis_hash")) == ranking_basis_hash
    )


def _resolve_once(resolver: object | None, **kwargs: object) -> object:
    if resolver is None:
        raise TypeError("authority resolver is required")
    target = getattr(resolver, "resolve", None)
    if not callable(target):
        raise TypeError("authority resolver has no resolve method")
    signature = inspect.signature(target)
    accepted = (
        kwargs
        if any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        else {key: value for key, value in kwargs.items() if key in signature.parameters}
    )
    return target(**accepted)


def _resolver_is_current(resolver: object | None, resolution: object | None) -> bool:
    if resolver is None or resolution is None:
        return False
    check = getattr(resolver, "is_current", None)
    if callable(check):
        return bool(check(resolution))
    assertion = getattr(resolver, "assert_current", None)
    if callable(assertion):
        try:
            result = assertion(resolution)
        except Exception:
            return False
        return result is not False
    return False


def _policy_identity(policy: object) -> tuple[str, str, str]:
    doc = _document(policy)
    version = _string_or_none(doc.get("current_policy_version"))
    policy_hash = _hash_or_value(doc.get("current_policy_hash"))
    marker_hash = _hash_or_value(doc.get("policy_authority_marker_hash"))
    if version is None or policy_hash is None or marker_hash is None:
        raise ValueError("policy resolver returned incomplete authority")
    return version, policy_hash, marker_hash


def _risk_identity(authority: object) -> tuple[str, str, str]:
    doc = _document(authority)
    version = _string_or_none(doc.get("version"))
    marker_hash = _hash_or_value(
        doc.get("risk_authority_marker_hash", doc.get("marker_hash"))
    )
    contract_hash = _hash_or_value(doc.get("risk_contract_hash"))
    if version is None or marker_hash is None or contract_hash is None:
        raise ValueError("risk authority resolver returned incomplete authority")
    return version, marker_hash, contract_hash


def _scenario_authority_matches(
    scenario: Mapping[str, Any],
    *,
    policy_version: str | None,
    policy_hash: str | None,
    policy_marker_hash: str | None,
    risk_version: str | None,
    risk_marker_hash: str | None,
    risk_contract_hash: str | None,
    strict: bool,
) -> bool:
    if (
        _string_or_none(
            scenario.get("current_policy_version", scenario.get("policy_version"))
        )
        != policy_version
        or _hash_or_value(
            scenario.get("current_policy_hash", scenario.get("policy_hash"))
        )
        != policy_hash
    ):
        return False
    if not strict:
        return True
    return (
        _hash_or_value(scenario.get("policy_authority_marker_hash"))
        == policy_marker_hash
        and _string_or_none(scenario.get("risk_authority_version")) == risk_version
        and _hash_or_value(scenario.get("risk_authority_marker_hash"))
        == risk_marker_hash
        and _hash_or_value(scenario.get("risk_contract_hash")) == risk_contract_hash
    )


__all__ = ["DecisionPipeline", "PipelineResult"]
