"""Composition root for the standalone Options Copilot GUI."""
from __future__ import annotations

import hashlib
import inspect
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from typing import ClassVar, Mapping

from options_copilot.api import (
    APPROVAL_CONFIRMATION_TOKEN,
    ApprovalStatusNotFound,
    OptionsCopilotUnavailable,
    OptionsCopilotServices,
    ProposalApprovalConflict,
    RankOneAuthorizationForbidden,
    create_app,
)
from options_copilot.api.app import LearningRecordNotFound
from options_copilot.analytics.benchmark import benchmark_convention
from options_copilot.analytics.ema20 import ema20_convention
from options_copilot.analytics.iv_percentile import iv_percentile_convention
from options_copilot.feature_source_diagnostic import validate_feature_source_observation
from options_copilot.feature_source_resolution import FeatureSourceResolver
from options_copilot.history_source_runtime import ScheduledHistoryProducer
from options_copilot.scanner.operation_context import ScheduledOperationContext
from options_copilot.storage.history_sources import HistorySourceStore
from options_copilot.storage.feature_sources import FeatureSourceObservationStore, FeatureSourceStoreError
from options_copilot.after_hours_indicative import (
    AfterHoursIndicativeStore,
    after_hours_candidate_cache_identity,
    after_hours_candidate_identity_manifest,
    after_hours_campaign_lineage_hash,
    after_hours_campaign_progress_status,
    after_hours_leg_ratio,
    after_hours_option_identity_payload,
    build_after_hours_indicative_read_model,
    unavailable_after_hours_indicative_read_model,
)
from options_copilot.approval import ActiveApprovalExists, ProposalApprovalStore
from options_copilot.approval.proofs import (
    ApprovalProofError,
    BROKER_PROOF_SCHEMA,
    STRATEGY_NAV_PROOF_SCHEMA,
    require_candidate_strategy_nav_binding,
)
from options_copilot.approval.store import ApprovalChallengeRejected
from options_copilot.bridge import BridgeStatus, CodexBridgeStore
from options_copilot.bridge.instruction_reader import InstructionStateReader
from options_copilot.config import OptionsCopilotConfig
from options_copilot.analytics import ScenarioEngine, VolatilityEngine
from options_copilot.decision import DecisionPipeline
from options_copilot.execution_cost import SignedExecutionCostResolver
from options_copilot.equity_pool import (
    EquityPoolService,
    EquityPoolStore,
    EquityPoolStoreConflict,
    FactorKind,
    UnderlyingEvidenceCache,
    captured_records_from_quotes,
)
from options_copilot.fundamentals import (
    FinnhubValuationProvider,
    FundamentalsService,
    FundamentalsStore,
    SecCompanyFactsProvider,
    SecManagementGuidanceProvider,
)
from options_copilot.option_pool import (
    OptionStructurePoolService,
    OptionStructurePoolStore,
    StructureDisposition,
)
from options_copilot.option_pool.models import normalize_equity_thesis_row
from options_copilot.external_bundle_commit import (
    ExternalBundleCommitGuard,
    ExternalBundlePaths,
    ExternalBundleSnapshot,
)
from options_copilot.gateway import (
    AtomicBrokerSnapshot,
    atomic_account_nlv,
    BrokerSnapshotBuilder,
    BrokerSnapshotStatus,
    ExternalReadonlyBatch,
    ExternalReadonlyFeedReader,
    IBKRReadOnlyGateway,
    MarketDataPacingError,
    MAX_LEG_SKEW_SECONDS,
    MAX_QUOTE_AGE_SECONDS,
    OptionContractRef,
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
    QuoteBatchStatus,
)
from options_copilot.governance.contracts import (
    ContractKind,
    ContractValidationError,
    load_contract,
    verify_contract,
)
from options_copilot.learning import LearningGovernance
from options_copilot.learning.evaluation_runtime import ShadowEvaluationStore
from options_copilot.learning.outcome_processor import (
    EvidenceStoreOutcomeObservationProvider,
    ExactHorizonOutcomeCapture,
    ImmutableOutcomeProcessor,
    OUTCOME_HORIZONS,
    OUTCOME_TARGET_RULES,
    OutcomeCaptureCoordinator,
    OutcomeCaptureLoop,
    RankingOutcomeTargetCursor,
    ShadowPredictionTargetCursor,
)
from options_copilot.learning.outcomes import OutcomeRecorder
from options_copilot.learning.progress import OutcomeProgressStore
from options_copilot.learning.policy_authority import (
    CurrentPolicyResolver,
    PolicyAuthorityError,
    PolicyAuthorityTampered,
)
from options_copilot.learning_shadow import (
    DISCOVERY_SAMPLE_THRESHOLD,
    GovernanceState,
    OutcomeRecord,
    ShadowLearningLedger,
    UnknownRecordError,
    VerifiedReplaySnapshot,
    replay_record_to_dict,
    shadow_record_to_dict,
    similarity_match_to_dict,
)
from options_copilot.market import (
    ExternalSessionCalendarProvider,
    IBKRSessionCalendarProvider,
    US_OPTIONS_TIMEZONE,
    UsOptionsCalendarSnapshot,
)
from options_copilot.market.session_calendar import BrokerTradingSessionGate
from options_copilot.news.composition import (
    build_optional_shadow_news_classifier,
    build_optional_phase2_advisory,
)
from options_copilot.news.external_batch_producer import (
    ExternalBatchBoundTop10Producer,
)
from options_copilot.news.external_top10_source import ExternalTop10StructureSource
from options_copilot.news.external_tick_runner import (
    validate_external_readonly_batch,
)
from options_copilot.news.open_reprice_economics import OpenRepriceEconomicsResolver
from options_copilot.news.preselection_producer import Top10PreselectionProducer
from options_copilot.news.research_top10 import (
    INTRADAY_RECOVERY,
    import_research_top10,
    read_research_top10,
)
from options_copilot.news.preselection_store import (
    LedgerBackedPreselectionProvider,
    NewsPreselectionStore,
)
from options_copilot.news.reaction_runtime import (
    BlsPublicDataActualProvider,
    ProductionMacroReactionProvider,
    ProductionReactionObserver,
    ReactionEvidenceStore,
)
from options_copilot.providers.official_reaction_sources import (
    OfficialReleaseCaptureCoordinator,
)
from options_copilot.news.shadow_research import ShadowResearchAdvisory
from options_copilot.news.shadow_prediction import (
    projectable_shadow_prediction_identity,
    shadow_prediction_exclusion_reason,
)
from options_copilot.news.shadow_store import (
    CHALLENGER_VERSION as NEWS_SHADOW_CHALLENGER_VERSION,
    NewsShadowLearningWriter,
)
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.performance.nav_ledger import StrategyNavLedger, StrategyNavSnapshot
from options_copilot.positions import PositionManager, ProductionManagementCoordinator
from options_copilot.production_runtime import (
    DirectTop10StructureSource,
    DurableOptionPoolTop10StructureSource,
    GuardedRequestBudget,
    IBKRNewsResearchAdapter,
    PacingAuthorityGuard,
    ProductionBrokerEvidenceAcquisition,
    ProductionDteGate,
    ProductionEligibilityGate,
    ProductionLifecycle,
    ProductionOptionsEvidenceAcquisition,
    ProductionOutcomeMarketAdapter,
    ProductionPipelineInputs,
    ProductionRiskGate,
    ProductionSingleCombinationGate,
    ProductionTop10SnapshotProvider,
    SerializedBrokerSnapshotProvider,
    creator_unavailable_reason,
    equity_theses_from_pool_result,
)
from options_copilot.providers import (
    AlphaVantageNewsProvider,
    CompanyIrEventProvider,
    FinnhubEventProvider,
    Jin10EventProvider,
    Jin10McpHttpClient,
    NasdaqEarningsProvider,
    SecCurrent8KProvider,
    build_official_calendar_provider,
)
from options_copilot.ranking import PortfolioRanker, RankingStore
from options_copilot.ranking.joint import JointRankingSnapshot
from options_copilot.ranking.evidence_manifest import (
    CandidateEvidenceManifestError,
    resolve_candidate_evidence_manifest,
)
from options_copilot.ranking.readiness import (
    CandidateReadinessError,
    evaluate_candidate_readiness,
)
from options_copilot.research_allocation import (
    normalise_research_allocation_evidence,
    normalise_research_allocation_read_model as _normalise_allocation_read_model,
)
from options_copilot.security.local_api_keys import (
    LocalApiKeyFileError,
    LocalApiKeyStore,
    LocalJin10EnvelopeReader,
    local_api_key_path,
    provider_configuration_status,
)
from options_copilot.security.dpapi import DPAPISecretStore
from options_copilot.security.jin10_credentials import (
    jin10_rotation_evidence_dir,
    resolve_jin10_credential,
)
from options_copilot.scanner import (
    ScanRunStore,
    ScanSchedulerLoop,
    ScanSchedulerService,
    Top10OnlySchedulerLoop,
    UniverseFunnel,
)
from options_copilot.scanner.coverage import OrdinaryScanCoverage
from options_copilot.scanner.service import (
    TOP10_PRODUCER_UNAVAILABLE,
    Top10SchedulerService,
)
from options_copilot.scanner.pacing import PACING_CAPABILITY_MISSING
from options_copilot.operations.pacing_authority import (
    PACING_EXPECTED_ACTOR,
    load_pacing_authority_verifier,
)
from options_copilot.state import ManagedSnapshotStore, RuntimeSnapshot
from options_copilot.strategies import (
    StrategyCandidateGenerator,
    StrategyKind,
    StrategyTemplateRegistry,
)
from options_copilot.storage import DecisionLedger
from options_copilot.storage.canonical import (
    canonical_hash,
    freeze_json,
    utc_datetime,
)
from options_copilot.storage.evidence import EvidenceStore
from options_copilot.risk import (
    CurrentRiskAuthorityResolver,
    PolicyLedgerRiskAuthorityMarkerSource,
)


BASELINE_MODEL_VERSION = "baseline-v1"
BASELINE_ARTIFACT_HASH = hashlib.sha256(
    b"options-copilot-baseline-v1-human-gated"
).hexdigest()
_MISSING_RUNTIME_DEPENDENCY = object()
_DEFAULT_TOP10_DEPENDENCY = object()
_UNWIRED_PRODUCTION_FEATURE_REASONS = (
    "FEATURE_HISTORY_PRODUCER_UNWIRED",
    "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
    "CANDIDATE_FEATURE_BINDING_UNWIRED",
)
_KNOWN_PRODUCTION_FEATURE_REASONS = (
    *_UNWIRED_PRODUCTION_FEATURE_REASONS,
    "FEATURE_HISTORY_SCHEDULED_OBSERVATIONS_NOT_MODEL_AUTHORITY",
)
_FEATURE_SOURCE_DIAGNOSTIC_METHODS = (
    ("PRICE_HISTORY", "feature_price_history"),
    ("IV_HISTORY", "feature_iv_history"),
    ("CURRENT_IV", "feature_current_iv"),
)
_FEATURE_SOURCE_DIAGNOSTIC_COOLDOWN_SECONDS = 15 * 60
PRODUCTION_PIPELINE_VERSION = "options-copilot-production-v1"
TOP10_PRODUCER_PIPELINE_VERSION = "options-copilot-independent-top10-v1"
LEARNING_GOVERNANCE_SCHEMA = "options_copilot.learning.governance.v1"
NO_TRUSTED_HUMAN_SIGNER = "NO_TRUSTED_HUMAN_SIGNER"
OUTCOME_CALLBACK_TERMINALIZATION_RESERVE_SECONDS = 5
STRATEGY_NAV_CONTRACT_PATH = (
    Path(__file__).resolve().parent
    / "governance"
    / "strategy_nav_contract.v1.json"
)


@dataclass(frozen=True, slots=True)
class RuntimeServices:
    """Complete server-side dependency graph for immutable decisions.

    Every field is an explicit constructor argument.  ``None`` is representable
    only so a composition failure can be reported as deterministic
    ``DEGRADED/NO_TRADE`` state; it never enables approval or falls back to
    external candidate JSON.
    """

    broker_snapshot_builder: object | None
    evidence_store: object | None
    scan_run_store: object | None
    pipeline_inputs: object | None
    universe_funnel: object | None
    broker_evidence_acquisition: object | None
    options_evidence_acquisition: object | None
    strategy_registry: object | None
    strategy_candidate_generator: object | None
    volatility_engine: object | None
    scenario_engine: object | None
    policy_resolver: object | None
    risk_authority_resolver: object | None
    execution_cost_contract: object | None
    eligibility_gate: object | None
    risk_gate: object | None
    dte_gate: object | None
    single_combination_gate: object | None
    portfolio_ranker: object | None
    ranking_store: object | None
    decision_pipeline: object | None
    strategy_nav_source: object | None
    position_manager: object | None
    approval_store: object | None
    bridge_status_reader: object | None
    bridge_reconciliation_reader: object | None
    readiness_guard: object | None = None
    approval_blockers: tuple[str, ...] = ()
    equity_evidence_cache: object | None = None
    feature_data_chain_reasons: tuple[str, ...] = ()

    _CAPABILITIES: ClassVar[
        dict[str, tuple[tuple[str, ...], ...]]
    ] = {
        "broker_snapshot_builder": (("build",),),
        "evidence_store": (("query",), ("get",), ("verify_integrity",)),
        "scan_run_store": (("get",), ("runs_for_slot",)),
        "pipeline_inputs": (("run", "load", "acquire"),),
        "universe_funnel": (("run",),),
        "broker_evidence_acquisition": (("acquire", "run", "build"),),
        "options_evidence_acquisition": (
            ("acquire", "run", "build"),
            ("resolve_contracts",),
        ),
        "strategy_registry": (("get",), ("validate",)),
        "strategy_candidate_generator": (("generate",),),
        "volatility_engine": (("evaluate",),),
        "scenario_engine": (("evaluate_pre_cost",),),
        "policy_resolver": (
            ("resolve",),
            ("is_current", "assert_current"),
            ("guard_current",),
        ),
        "risk_authority_resolver": (
            ("resolve",),
            ("is_current", "assert_current"),
            ("guard_current",),
        ),
        "execution_cost_contract": (
            ("resolve", "apply", "run"),
            ("is_current", "assert_current"),
            ("guard_current",),
        ),
        "eligibility_gate": (("evaluate", "assess", "run"),),
        "risk_gate": (("evaluate", "assess", "run"),),
        "dte_gate": (("evaluate", "assess", "run", "resolve"),),
        "single_combination_gate": (("evaluate", "assess", "run"),),
        "portfolio_ranker": (("rank",),),
        "ranking_store": (
            ("latest",),
            ("get_by_scan_run",),
            ("read_snapshot",),
            ("authorize_frozen_rank_one",),
            ("append_decisions",),
            ("append_snapshot",),
            ("current_terminal_for_snapshot",),
        ),
        "decision_pipeline": (("run_slot",),),
        "strategy_nav_source": (("snapshot",), ("guard_current",)),
        "position_manager": (("read_model", "latest", "management"),),
        "approval_store": (
            ("create_challenge",),
            ("confirm_challenge",),
            ("get",),
            ("get_challenge",),
        ),
        "bridge_status_reader": (("get",), ("list_pending",)),
        "bridge_reconciliation_reader": (
            ("reconciliation_status", "read_reconciliation", "get"),
        ),
    }

    @property
    def missing_dependencies(self) -> tuple[str, ...]:
        return tuple(
            name for name in self._CAPABILITIES if getattr(self, name) is None
        )

    @property
    def invalid_dependencies(self) -> tuple[str, ...]:
        return tuple(
            name
            for name in self._CAPABILITIES
            if getattr(self, name) is not None
            and not _supports_capabilities(
                getattr(self, name), self._CAPABILITIES[name]
            )
        )

    @property
    def readiness_guard_reasons(self) -> tuple[str, ...]:
        guard = self.readiness_guard
        if guard is None:
            return ()
        reader = getattr(guard, "reasons", None)
        if not callable(reader):
            return ("RUNTIME_READINESS_GUARD_INVALID",)
        try:
            reasons = reader()
        except Exception:
            return ("RUNTIME_READINESS_GUARD_UNAVAILABLE",)
        if isinstance(reasons, str) or not isinstance(reasons, Sequence):
            return ("RUNTIME_READINESS_GUARD_INVALID",)
        checked = tuple(str(item).strip() for item in reasons if str(item).strip())
        return tuple(dict.fromkeys(checked))

    @property
    def normalized_approval_blockers(self) -> tuple[str, ...]:
        raw = self.approval_blockers
        if raw is None:  # compatibility with explicit all-None composition probes
            return ()
        if isinstance(raw, str) or not isinstance(raw, Sequence):
            return ("RUNTIME_APPROVAL_BLOCKERS_INVALID",)
        return tuple(dict.fromkeys(str(item).strip() for item in raw if str(item).strip()))

    @property
    def decision_readiness_reasons(self) -> tuple[str, ...]:
        """Return only blockers that invalidate research and ranking evidence.

        Creator transport is a downstream approval concern.  It must never
        erase a current immutable ranking whose broker, quote, NAV, cost, risk,
        and eligibility evidence is otherwise complete.
        """

        return tuple(
            [f"MISSING_DEPENDENCY:{name}" for name in self.missing_dependencies]
            + [
                f"INVALID_DEPENDENCY_PORT:{name}"
                for name in self.invalid_dependencies
            ]
            + [
                f"RESOLVER_OR_PIPELINE_DISAGREEMENT:{name}"
                for name in self.wiring_disagreements
            ]
            + list(self.readiness_guard_reasons)
        )

    @property
    def wiring_disagreements(self) -> tuple[str, ...]:
        pipeline = self.decision_pipeline
        if pipeline is None:
            return ()
        expected = {
            "inputs": self.pipeline_inputs,
            "universe_funnel": self.universe_funnel,
            "broker_evidence": self.broker_evidence_acquisition,
            "strategy_registry": self.strategy_registry,
            "strategy_generator": self.strategy_candidate_generator,
            "volatility_engine": self.volatility_engine,
            "scenario_engine": self.scenario_engine,
            "policy_resolver": self.policy_resolver,
            "risk_authority_resolver": self.risk_authority_resolver,
            "cost_contract": self.execution_cost_contract,
            "eligibility_gate": self.eligibility_gate,
            "portfolio_ranker": self.portfolio_ranker,
            "ranking_store": self.ranking_store,
        }
        direct = tuple(
            name
            for name, dependency in expected.items()
            if getattr(pipeline, name, _MISSING_RUNTIME_DEPENDENCY) is not dependency
        )
        nested_expected = (
            (
                "broker_evidence_acquisition.broker_snapshot_builder",
                self.broker_evidence_acquisition,
                "broker_snapshot_builder",
                self.broker_snapshot_builder,
            ),
            (
                "broker_evidence_acquisition.options_evidence_acquisition",
                self.broker_evidence_acquisition,
                "options_evidence_acquisition",
                self.options_evidence_acquisition,
            ),
            (
                "broker_evidence_acquisition.evidence_store",
                self.broker_evidence_acquisition,
                "evidence_store",
                self.evidence_store,
            ),
            (
                "broker_evidence_acquisition.policy_resolver",
                self.broker_evidence_acquisition,
                "policy_resolver",
                self.policy_resolver,
            ),
            (
                "broker_evidence_acquisition.strategy_nav_source",
                self.broker_evidence_acquisition,
                "strategy_nav_source",
                self.strategy_nav_source,
            ),
            (
                "pipeline_inputs.position_manager",
                self.pipeline_inputs,
                "position_manager",
                self.position_manager,
            ),
            (
                "eligibility_gate.risk_gate",
                self.eligibility_gate,
                "risk_gate",
                self.risk_gate,
            ),
            (
                "eligibility_gate.dte_gate",
                self.eligibility_gate,
                "dte_gate",
                self.dte_gate,
            ),
            (
                "eligibility_gate.single_combination_gate",
                self.eligibility_gate,
                "single_combination_gate",
                self.single_combination_gate,
            ),
        )
        nested = tuple(
            name
            for name, owner, attribute, dependency in nested_expected
            if owner is not None
            and getattr(owner, attribute, _MISSING_RUNTIME_DEPENDENCY)
            is not dependency
        )
        return direct + nested

    @property
    def decision_enabled(self) -> bool:
        return not self.decision_readiness_reasons

    @property
    def approval_enabled(self) -> bool:
        return (
            self.decision_enabled
            and not self.normalized_approval_blockers
        )

    @property
    def last_operational_timing(self) -> Mapping[str, object]:
        """Expose detached observation-only timing from the injected pipeline."""

        pipeline = self.decision_pipeline
        if pipeline is None:
            return {}
        try:
            raw = getattr(pipeline, "last_operational_timing", None)
        except Exception:
            return {}
        if not isinstance(raw, Mapping):
            return {}
        stages = raw.get("stages", ())
        if isinstance(stages, (str, bytes, bytearray)) or not isinstance(
            stages,
            Sequence,
        ):
            return {}
        return {
            **dict(raw),
            "stages": tuple(
                dict(item)
                for item in stages
                if isinstance(item, Mapping)
            ),
        }

    def feature_data_chain_readiness(self) -> dict[str, object]:
        """Describe composed feature gaps without granting or removing authority."""

        raw = self.feature_data_chain_reasons
        if raw is None or raw == ():
            reasons: tuple[str, ...] = ()
        elif (
            isinstance(raw, (str, bytes, bytearray))
            or not isinstance(raw, Sequence)
            or any(item not in _KNOWN_PRODUCTION_FEATURE_REASONS for item in raw)
        ):
            reasons = ("FEATURE_DATA_CHAIN_DIAGNOSTIC_INVALID",)
        else:
            reasons = tuple(dict.fromkeys(raw))
        return {
            "scope": "PRODUCTION_SCENARIO_INPUTS",
            "status": "INCOMPLETE" if reasons else "NOT_ASSESSED",
            "model_input_complete": False if reasons else None,
            "reason_codes": reasons,
            "decision_authority": "OBSERVATION_ONLY",
            "affects_decision": False,
        }

    def readiness(self) -> dict[str, object]:
        missing = self.missing_dependencies
        invalid = self.invalid_dependencies
        disagreements = self.wiring_disagreements
        guard_reasons = self.readiness_guard_reasons
        decision_ready = not missing and not invalid and not disagreements and not guard_reasons
        approval_blockers = self.normalized_approval_blockers
        feature_data_chain = self.feature_data_chain_readiness()
        feature_reasons = feature_data_chain["reason_codes"]
        ready = decision_ready and not approval_blockers and not feature_reasons
        decision_reasons = self.decision_readiness_reasons
        reasons = tuple((*decision_reasons, *approval_blockers, *feature_reasons))
        payload: dict[str, object] = {
            "status": "READY" if ready else "DEGRADED",
            "readiness_scope": "DEPENDENCY_WIRING_ONLY",
            "decision": "READY" if decision_ready else "NO_TRADE",
            "research_enabled": decision_ready,
            "approval_enabled": self.approval_enabled,
            "review_only": True,
            "direct_order_submission": False,
            "missing_dependencies": missing,
            "invalid_dependencies": invalid,
            "wiring_disagreements": disagreements,
            "readiness_guard_reasons": guard_reasons,
            "approval_blockers": approval_blockers,
            "decision_reasons": decision_reasons,
            "feature_data_chain": feature_data_chain,
            "reasons": reasons,
        }
        return {**payload, "content_hash": canonical_hash(payload)}

    def run_slot(self, scan_run_id: str, slot_at: datetime) -> Mapping[str, object]:
        """Execute only the injected DecisionPipeline; payload candidates are absent."""

        if not self.decision_enabled or self.decision_pipeline is None:
            return {
                **self.readiness(),
                "scan_run_id": scan_run_id,
                "ranking_snapshot_id": None,
            }
        nav, nav_reasons = self._strategy_nav(slot_at)
        if nav is None:
            return {
                **self.readiness(),
                "status": "DEGRADED",
                "decision": "NO_TRADE",
                "approval_enabled": False,
                "reasons": nav_reasons,
                "scan_run_id": scan_run_id,
                "ranking_snapshot_id": None,
            }
        runner = getattr(self.decision_pipeline, "run_slot", None)
        if not callable(runner):
            return {
                **self.readiness(),
                "status": "DEGRADED",
                "decision": "NO_TRADE",
                "approval_enabled": False,
                "reasons": ("DECISION_PIPELINE_PORT_INVALID",),
                "scan_run_id": scan_run_id,
                "ranking_snapshot_id": None,
            }
        try:
            result = runner(scan_run_id, slot_at)
        except Exception:
            return {
                **self.readiness(),
                "status": "DEGRADED",
                "decision": "NO_TRADE",
                "approval_enabled": False,
                "reasons": ("DECISION_PIPELINE_FAILED",),
                "scan_run_id": scan_run_id,
                "ranking_snapshot_id": None,
            }
        if not isinstance(result, Mapping):
            return {
                **self.readiness(),
                "status": "DEGRADED",
                "decision": "NO_TRADE",
                "approval_enabled": False,
                "reasons": ("DECISION_PIPELINE_RESULT_INVALID",),
                "scan_run_id": scan_run_id,
                "ranking_snapshot_id": None,
            }
        detached = dict(result)
        detached.setdefault("strategy_nav_hash", nav.authority_hash)
        detached.setdefault("strategy_nav_contract_hash", nav.contract_hash)
        detached.setdefault("review_only", True)
        detached.setdefault("direct_order_submission", False)
        return detached

    def latest_ranking(self) -> Mapping[str, object]:
        """Return a detached immutable ledger read model, never supplied JSON."""

        base = self._ranking_unavailable()
        if "ranking_store" in self.missing_dependencies:
            return base
        if "ranking_store" in self.invalid_dependencies:
            return {**base, "reasons": ("RANKING_READ_MODEL_PORT_INVALID",)}
        assert self.ranking_store is not None
        terminal = _latest_terminal_no_trade(self.ranking_store)
        if terminal is not None:
            return {**base, **terminal}
        try:
            snapshot = self.ranking_store.latest()
        except Exception:
            return {**base, "reasons": ("RANKING_STORE_UNAVAILABLE",)}
        if snapshot is None:
            return {**base, "reasons": ("NO_IMMUTABLE_RANKING",)}
        snapshot_id = getattr(snapshot, "ranking_snapshot_id", None)
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            return {**base, "reasons": ("RANKING_SNAPSHOT_ID_INVALID",)}
        return self.ranking(snapshot_id)

    def ranking(self, ranking_snapshot_id: str) -> Mapping[str, object]:
        base = self._ranking_unavailable()
        if (
            not isinstance(ranking_snapshot_id, str)
            or not ranking_snapshot_id.strip()
            or self.ranking_store is None
            or "ranking_store" in self.invalid_dependencies
        ):
            return {**base, "reasons": ("RANKING_READ_MODEL_PORT_INVALID",)}
        try:
            payload = self.ranking_store.read_snapshot(ranking_snapshot_id)
        except Exception:
            return {**base, "reasons": ("IMMUTABLE_RANKING_NOT_FOUND",)}
        if not isinstance(payload, Mapping):
            return {**base, "reasons": ("RANKING_READ_MODEL_INVALID",)}
        result = _normalise_allocation_read_model(payload)
        result.update(_joint_ranking_projection(result))
        checked_at = datetime.now(timezone.utc)
        nav, nav_reasons = self._strategy_nav(checked_at)
        binding_reasons = _ranking_binding_reasons(result, nav, now=checked_at)
        try:
            latest = self.ranking_store.latest()
        except Exception:
            latest = None
        if (
            latest is None
            or getattr(latest, "ranking_snapshot_id", None)
            != ranking_snapshot_id
        ):
            binding_reasons = (*binding_reasons, "RANKING_SNAPSHOT_NOT_CURRENT")
        try:
            terminal_current = self.ranking_store.current_terminal_for_snapshot(
                ranking_snapshot_id
            )
        except Exception:
            terminal_current = None
        if terminal_current is not True:
            binding_reasons = (
                *binding_reasons,
                "RANKING_TERMINAL_NOT_CURRENT",
            )
        decision_reasons = self.decision_readiness_reasons
        reasons = tuple(
            dict.fromkeys((*decision_reasons, *nav_reasons, *binding_reasons))
        )
        candidates = result.get("candidates")
        has_candidates = isinstance(candidates, (list, tuple)) and bool(candidates)
        recommendations_available = has_candidates and not reasons
        approval_blockers = self.normalized_approval_blockers
        approval_enabled = recommendations_available and self.approval_enabled
        result["decision"] = (
            "CANDIDATES_AVAILABLE" if recommendations_available else "NO_TRADE"
        )
        result["recommendations_available"] = recommendations_available
        result["approval_enabled"] = approval_enabled
        result["approval_blockers"] = approval_blockers
        result["approval_reasons"] = tuple(
            dict.fromkeys((*reasons, *approval_blockers))
        )
        result["reasons"] = reasons
        result["review_only"] = True
        result["direct_order_submission"] = False
        return result

    def latest_scan(self) -> Mapping[str, object]:
        if (
            self.scan_run_store is None
            or self.ranking_store is None
            or "scan_run_store" in self.invalid_dependencies
            or "ranking_store" in self.invalid_dependencies
        ):
            return _no_trade_read_model("SCAN_RUN_STORE_UNAVAILABLE")
        terminal = _latest_terminal_no_trade(self.ranking_store)
        if terminal is not None:
            scan_run_id = terminal.get("scan_run_id")
            if not isinstance(scan_run_id, str) or not scan_run_id:
                return terminal
            try:
                payload = _read_model(self.scan_run_store.get(scan_run_id))
            except Exception:
                return {
                    **terminal,
                    "reasons": tuple(
                        dict.fromkeys(
                            (
                                *terminal["reasons"],
                                "SCAN_RUN_STORE_UNAVAILABLE",
                            )
                        )
                    ),
                }
            timing = self._scan_operational_timing(scan_run_id)
            return {
                **payload,
                **terminal,
                "status": payload.get("status", terminal["status"]),
                "decision": "NO_TRADE",
                "approval_enabled": False,
                "review_only": True,
                "direct_order_submission": False,
                **(
                    {"operational_timing": timing}
                    if timing is not None
                    else {}
                ),
            }
        try:
            ranking = self.ranking_store.latest()
            scan_run_id = None if ranking is None else getattr(ranking, "scan_run_id", None)
            if not isinstance(scan_run_id, str):
                return _no_trade_read_model("NO_IMMUTABLE_SCAN")
            ranking_snapshot_id = getattr(ranking, "ranking_snapshot_id", None)
            if not isinstance(ranking_snapshot_id, str):
                return _no_trade_read_model("RANKING_SNAPSHOT_ID_INVALID")
            try:
                terminal_current = self.ranking_store.current_terminal_for_snapshot(
                    ranking_snapshot_id
                )
            except Exception:
                return _no_trade_read_model(
                    "RANKING_DECISION_LEDGER_UNAVAILABLE"
                )
            if terminal_current is not True:
                return _no_trade_read_model("RANKING_TERMINAL_NOT_CURRENT")
            scan = self.scan_run_store.get(scan_run_id)
            payload = _normalise_allocation_read_model(_read_model(scan))
        except Exception:
            return _no_trade_read_model("SCAN_RUN_STORE_UNAVAILABLE")
        timing = self._scan_operational_timing(scan_run_id)
        return {
            **payload,
            "decision": "READY" if self.decision_enabled else "NO_TRADE",
            "approval_enabled": self.approval_enabled,
            "review_only": True,
            "direct_order_submission": False,
            **(
                {"operational_timing": timing}
                if timing is not None
                else {}
            ),
        }

    def _scan_operational_timing(
        self,
        scan_run_id: str,
    ) -> Mapping[str, object] | None:
        if self.scan_run_store is None:
            return None
        reader = getattr(self.scan_run_store, "operational_timing", None)
        if not callable(reader):
            return None
        try:
            value = reader(scan_run_id)
        except Exception:
            # Performance telemetry is deliberately non-authoritative.  Store
            # or projection failures must not suppress the immutable scan.
            return None
        return dict(value) if isinstance(value, Mapping) else None

    def candidate_evidence(
        self, scan_run_id: str, candidate_id: str
    ) -> Mapping[str, object]:
        if (
            self.ranking_store is None
            or "ranking_store" in self.invalid_dependencies
            or not isinstance(scan_run_id, str)
            or not isinstance(candidate_id, str)
        ):
            return _candidate_evidence_no_trade(
                "CANDIDATE_EVIDENCE_UNAVAILABLE",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        try:
            stored = self.ranking_store.get_by_scan_run(scan_run_id)
            if stored is None:
                return _candidate_evidence_no_trade(
                    "CANDIDATE_NOT_FOUND",
                    scan_run_id=scan_run_id,
                    candidate_id=candidate_id,
                )
            ranking_snapshot_id = getattr(stored, "ranking_snapshot_id", None)
            if not isinstance(ranking_snapshot_id, str):
                return _candidate_evidence_no_trade(
                    "CANDIDATE_EVIDENCE_RANKING_MISMATCH",
                    scan_run_id=scan_run_id,
                    candidate_id=candidate_id,
                )
            payload = self.ranking_store.read_snapshot(ranking_snapshot_id)
        except Exception:
            return _candidate_evidence_no_trade(
                "CANDIDATE_EVIDENCE_UNAVAILABLE",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        if (
            not isinstance(payload, Mapping)
            or payload.get("scan_run_id") != scan_run_id
            or payload.get("ranking_snapshot_id") != ranking_snapshot_id
        ):
            return _candidate_evidence_no_trade(
                "CANDIDATE_EVIDENCE_RANKING_MISMATCH",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        ranked_rows = payload.get("candidates", ())
        governance_rows = payload.get("governance_evidence", ())
        if any(
            not isinstance(rows, Sequence)
            or isinstance(rows, (str, bytes, bytearray))
            for rows in (ranked_rows, governance_rows)
        ):
            return _candidate_evidence_no_trade(
                "CANDIDATE_EVIDENCE_RANKING_MISMATCH",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        rows = tuple(ranked_rows) + tuple(governance_rows)
        matches = [
            row
            for row in rows
            if isinstance(row, Mapping) and row.get("candidate_id") == candidate_id
        ]
        if len(matches) != 1:
            return _candidate_evidence_no_trade(
                "CANDIDATE_NOT_FOUND",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        row = dict(matches[0])
        body = row.get("candidate_body")
        proposal = row.get("proposal_body")
        if (
            not isinstance(body, Mapping)
            or not isinstance(proposal, Mapping)
            or body.get("candidate_id") != candidate_id
            or not isinstance(body.get("symbol"), str)
            or not str(body.get("symbol")).strip()
        ):
            return _candidate_evidence_no_trade(
                "CANDIDATE_EVIDENCE_CANDIDATE_MISMATCH",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        candidate_symbol = str(body["symbol"])
        immutable_inputs = payload.get("immutable_inputs")
        manifests = (
            immutable_inputs.get("candidate_evidence_manifests")
            if isinstance(immutable_inputs, Mapping)
            else None
        )
        manifest = manifests.get(candidate_id) if isinstance(manifests, Mapping) else None
        if manifest is None:
            return _candidate_evidence_no_trade(
                "CANDIDATE_EVIDENCE_MANIFEST_MISSING",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        try:
            score_components = row.get("score_components")
            projection = resolve_candidate_evidence_manifest(
                manifest,
                candidate_id=candidate_id,
                candidate_symbol=candidate_symbol,
                candidate_body=body,
                proposal_body=proposal,
                ranked_after_cost_expected_value=(
                    score_components.get("after_cost_expected_value")
                    if isinstance(score_components, Mapping)
                    else None
                ),
                ranking_broker_snapshot_hash=payload.get(
                    "broker_snapshot_hash"
                ),
                ranking_cost_version=payload.get("cost_version"),
                ranking_cost_hash=payload.get("cost_hash"),
                ranking_valid_until=payload.get("valid_until"),
                evidence_store=self.evidence_store,
                now=datetime.now(timezone.utc),
            )
        except CandidateEvidenceManifestError as exc:
            return _candidate_evidence_no_trade(
                exc.reason,
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        except Exception:
            return _candidate_evidence_no_trade(
                "CANDIDATE_EVIDENCE_UNAVAILABLE",
                scan_run_id=scan_run_id,
                candidate_id=candidate_id,
            )
        return {
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "decision_authority": "OBSERVATION_ONLY",
            "scan_run_id": scan_run_id,
            "candidate_id": candidate_id,
            "ranking_snapshot_id": ranking_snapshot_id,
            "ranking_snapshot_hash": payload.get("snapshot_hash"),
            "candidate_hash": row.get("candidate_hash"),
            "ranking_basis_hash": row.get("ranking_basis_hash"),
            "input_hash": payload.get("input_hash"),
            "evidence_hash": payload.get("evidence_hash"),
            "broker_snapshot_hash": payload.get("broker_snapshot_hash"),
            "current_policy_version": payload.get("current_policy_version"),
            "current_policy_hash": payload.get("current_policy_hash"),
            "policy_authority_marker_hash": payload.get(
                "policy_authority_marker_hash"
            ),
            "cost_version": payload.get("cost_version"),
            "cost_hash": payload.get("cost_hash"),
            "risk_contract_hash": payload.get("risk_contract_hash"),
            "risk_authority_version": payload.get("risk_authority_version"),
            "risk_authority_marker_hash": payload.get(
                "risk_authority_marker_hash"
            ),
            "strategy_nav_hash": body.get("strategy_nav_hash"),
            "strategy_nav_contract_hash": body.get(
                "strategy_nav_contract_hash"
            ),
            "schema": projection.schema,
            "symbol": projection.symbol,
            "cutoff_at": projection.cutoff_at,
            "manifest_hash": projection.manifest_hash,
            "primary": list(projection.primary),
            "supporting": list(projection.supporting),
            "contradicting": list(projection.contradicting),
            "reason": "FROZEN_POINT_IN_TIME_EVIDENCE",
            "reasons": ("FROZEN_POINT_IN_TIME_EVIDENCE",),
            "review_only": True,
            "direct_order_submission": False,
            "approval_enabled": False,
        }

    def management(self) -> Mapping[str, object]:
        manager = self.position_manager
        if manager is None or "position_manager" in self.invalid_dependencies:
            return {
                **_no_trade_read_model("POSITION_MANAGER_UNAVAILABLE"),
                "status": "UNAVAILABLE",
                "available": False,
            }
        try:
            payload = _invoke_port(manager, ("read_model", "latest", "management"))
        except Exception:
            return {
                **_no_trade_read_model("POSITION_MANAGER_UNAVAILABLE"),
                "status": "UNAVAILABLE",
                "available": False,
            }
        if not isinstance(payload, Mapping):
            return {
                **_no_trade_read_model("POSITION_MANAGER_READ_MODEL_INVALID"),
                "status": "UNAVAILABLE",
                "available": False,
            }
        result = dict(payload)
        result.setdefault("available", True)
        result.setdefault("mode", "POSITION_MANAGEMENT")
        result["approval_enabled"] = self.approval_enabled and bool(
            result.get("approval_enabled")
        )
        result["review_only"] = True
        result["direct_order_submission"] = False
        return result

    def positioning(self) -> Mapping[str, object]:
        """Return supporting-only chain analytics without trade authority."""

        source = self.broker_evidence_acquisition
        reader = getattr(source, "positioning", None)
        if source is None or not callable(reader):
            return _positioning_no_trade("POSITIONING_SOURCE_UNAVAILABLE")
        try:
            payload = reader()
        except Exception:
            return _positioning_no_trade("POSITIONING_SOURCE_UNAVAILABLE")
        if not isinstance(payload, Mapping):
            return _positioning_no_trade("POSITIONING_READ_MODEL_INVALID")
        result = dict(payload)
        result["decision_authority"] = "SUPPORTING_ONLY"
        result["supporting_only"] = True
        result["affects_eligibility"] = False
        result["approval_allowed"] = False
        result["instruction_allowed"] = False
        result["order_allowed"] = False
        return result

    def authorize_rank_one(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        *,
        now: datetime | None = None,
    ) -> object | None:
        """Identifier-only server reread; there is intentionally no rank input."""

        if not self.approval_enabled or self.ranking_store is None:
            return None
        authorize = getattr(self.ranking_store, "authorize_frozen_rank_one", None)
        if not callable(authorize):
            return None
        checked_at = now or datetime.now(timezone.utc)
        nav, nav_reasons = self._strategy_nav(checked_at)
        if nav is None or nav_reasons:
            return None
        try:
            latest = self.ranking_store.latest()
            if (
                latest is None
                or getattr(latest, "ranking_snapshot_id", None)
                != ranking_snapshot_id
                or self.ranking_store.current_terminal_for_snapshot(
                    ranking_snapshot_id
                )
                is not True
            ):
                return None
            expected_hashes = getattr(latest, "expected_hashes", None)
            payload = self.ranking_store.read_snapshot(ranking_snapshot_id)
            if not isinstance(expected_hashes, Mapping) or not isinstance(
                payload, Mapping
            ):
                return None
            if _ranking_binding_reasons(payload, nav, now=checked_at):
                return None
            rows = [
                row
                for row in payload.get("candidates", ())
                if isinstance(row, Mapping)
                and row.get("candidate_id") == candidate_id
            ]
            if (
                len(rows) != 1
                or rows[0].get("rank") != 1
                or rows[0].get("authorizable") is not True
                or rows[0].get("authority_status") == "A_GRADE_PENDING"
            ):
                return None
            return authorize(
                ranking_snapshot_id,
                candidate_id,
                expected_hashes,
                policy_resolver=self.policy_resolver,
                risk_authority_resolver=self.risk_authority_resolver,
                now=checked_at,
            )
        except Exception:
            return None

    def _require_rank_one_readiness(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        *,
        now: datetime | None = None,
    ) -> tuple[Mapping[str, object], Mapping[str, object]]:
        ranking = self.ranking(ranking_snapshot_id)
        matches = [
            row
            for row in ranking.get("candidates", ())
            if isinstance(row, Mapping)
            and row.get("candidate_id") == candidate_id
        ]
        if len(matches) != 1:
            raise RankOneAuthorizationForbidden(
                "VIEW_ONLY: candidate is not present in the frozen ranking"
            )
        try:
            readiness = evaluate_candidate_readiness(
                ranking,
                matches[0],
                expected_candidate_id=candidate_id,
                now=now,
            )
        except CandidateReadinessError as exc:
            raise RankOneAuthorizationForbidden(
                f"VIEW_ONLY: {exc.reason}"
            ) from exc
        if not readiness.challenge_allowed:
            if ranking.get("approval_enabled") is not True:
                raise ProposalApprovalConflict(
                    "ranking terminal, policy, risk, cost, or Strategy NAV authority changed"
                )
            reason = str(readiness.source_health.get("reason") or "")
            if readiness.source_health.get("status") == "READY":
                reason = str(readiness.account_capacity.get("reason") or "")
            raise RankOneAuthorizationForbidden(
                f"VIEW_ONLY: {reason or 'rank-one candidate is not challenge ready'}"
            )
        return ranking, matches[0]

    def create_rank_one_challenge(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        *,
        now: datetime | None = None,
    ) -> Mapping[str, object]:
        if not self.approval_enabled or self.approval_store is None:
            reasons = self.normalized_approval_blockers
            if self.approval_store is None:
                reasons = (*reasons, "APPROVAL_STORE_UNAVAILABLE")
            if not reasons:
                reasons = self.decision_readiness_reasons or (
                    "RANK_ONE_CHALLENGE_UNAVAILABLE",
                )
            raise OptionsCopilotUnavailable(
                "NO_TRADE: " + ", ".join(dict.fromkeys(reasons))
            )
        _, rank_one = self._require_rank_one_readiness(
            ranking_snapshot_id,
            candidate_id,
            now=now,
        )
        candidate_body = rank_one.get("candidate_body")
        proposal_body = rank_one.get("proposal_body")
        proposal_hash = rank_one.get("proposal_hash")
        if not isinstance(candidate_body, Mapping) or not isinstance(
            proposal_body, Mapping
        ):
            raise ProposalApprovalConflict(
                "frozen rank-one candidate or proposal body is unavailable"
            )
        if not _digest(proposal_hash):
            raise ProposalApprovalConflict(
                "frozen rank-one proposal hash is unavailable or invalid"
            )
        snapshot = self._revalidate_current_broker(
            ranking_snapshot_id=ranking_snapshot_id,
            candidate_id=candidate_id,
            candidate_body=candidate_body,
            proposal_body=proposal_body,
            now=now,
        )
        nav, nav_reasons = self._strategy_nav(
            snapshot.built_at,
            observed_account_nlv=_atomic_account_nlv(snapshot),
        )
        if nav is None or nav_reasons:
            raise ProposalApprovalConflict(
                "current Strategy NAV authority is unavailable or invalid"
            )
        _require_current_nav_binding(candidate_body, nav)
        _require_nav_matches_broker_account(nav, snapshot)
        broker_proof = _atomic_broker_proof(
            snapshot,
            ranking_snapshot_id=ranking_snapshot_id,
            candidate_id=candidate_id,
            proposal_hash=proposal_hash,
        )
        strategy_nav_proof = _strategy_nav_proof(nav)
        store_now = now or datetime.now(timezone.utc)
        try:
            result = _invoke_port(
                self.approval_store,
                ("create_challenge",),
                ranking_snapshot_id,
                candidate_id,
                ranking_store=self.ranking_store,
                policy_resolver=self.policy_resolver,
                risk_authority_resolver=self.risk_authority_resolver,
                execution_cost_contract=self.execution_cost_contract,
                broker_proof=broker_proof,
                strategy_nav_proof=strategy_nav_proof,
                strategy_nav_source=self.strategy_nav_source,
                strategy_nav_snapshot=nav,
                now=store_now,
            )
        except (
            ProposalApprovalConflict,
            RankOneAuthorizationForbidden,
            OptionsCopilotUnavailable,
        ):
            raise
        except (ApprovalChallengeRejected, ActiveApprovalExists) as exc:
            raise ProposalApprovalConflict(str(exc) or "approval challenge rejected") from exc
        except Exception as exc:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval challenge store is unavailable or corrupt"
            ) from exc
        if result is None:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval challenge store returned no result"
            )
        try:
            payload = _read_model(result)
        except TypeError as exc:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval challenge store returned an invalid result"
            ) from exc
        payload.update(
            {
                "status": "PENDING_SECOND_CONFIRMATION",
                "review_only": True,
                "instruction_id": None,
                "ibkr_deep_link": None,
                "order_submitted": False,
                "transmitted_to_broker": False,
            }
        )
        return payload

    def confirm_challenge(
        self,
        challenge_id: str,
        confirmation: Mapping[str, object],
        *,
        now: datetime | None = None,
    ) -> Mapping[str, object] | None:
        if not self.approval_enabled or self.approval_store is None:
            return None
        if (
            confirmation.get("risk_acknowledged") is not True
            or confirmation.get("second_confirmation") is not True
            or confirmation.get("confirmation_token")
            != APPROVAL_CONFIRMATION_TOKEN
        ):
            raise ProposalApprovalConflict(
                "explicit review-only confirmation is required"
            )
        try:
            challenge = _invoke_port(
                self.approval_store,
                ("get_challenge",),
                challenge_id,
            )
        except Exception as exc:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval challenge store is unavailable or corrupt"
            ) from exc
        if challenge is None:
            raise ProposalApprovalConflict(
                "approval challenge is invalid, expired, or unavailable"
            )
        try:
            challenge_payload = _read_model(challenge)
        except TypeError as exc:
            raise ProposalApprovalConflict(
                "approval challenge authority binding is invalid"
            ) from exc
        ranking_snapshot_id = getattr(
            challenge, "ranking_snapshot_id", challenge_payload.get("ranking_snapshot_id")
        )
        candidate_id = getattr(
            challenge, "candidate_id", challenge_payload.get("candidate_id")
        )
        candidate_body = getattr(
            challenge, "candidate_body", challenge_payload.get("candidate_body")
        )
        proposal_body = getattr(
            challenge, "proposal_body", challenge_payload.get("proposal_body")
        )
        proposal_hash = getattr(
            challenge, "proposal_hash", challenge_payload.get("proposal_hash")
        )
        if (
            not isinstance(ranking_snapshot_id, str)
            or not ranking_snapshot_id.strip()
            or not isinstance(candidate_id, str)
            or not candidate_id.strip()
            or not isinstance(candidate_body, Mapping)
            or not isinstance(proposal_body, Mapping)
            or not _digest(proposal_hash)
        ):
            raise ProposalApprovalConflict(
                "approval challenge authority binding is invalid"
            )
        _, current_rank_one = self._require_rank_one_readiness(
            ranking_snapshot_id,
            candidate_id,
            now=now,
        )
        if (
            current_rank_one.get("candidate_body") != candidate_body
            or current_rank_one.get("proposal_body") != proposal_body
            or current_rank_one.get("proposal_hash") != proposal_hash
        ):
            raise ProposalApprovalConflict(
                "approval challenge no longer matches the current rank-one authority"
            )
        snapshot = self._revalidate_current_broker(
            ranking_snapshot_id=ranking_snapshot_id,
            candidate_id=candidate_id,
            candidate_body=candidate_body,
            proposal_body=proposal_body,
            now=now,
        )
        nav, reasons = self._strategy_nav(
            snapshot.built_at,
            observed_account_nlv=_atomic_account_nlv(snapshot),
        )
        if nav is None or reasons:
            raise ProposalApprovalConflict(
                "current Strategy NAV authority is unavailable or invalid"
            )
        _require_current_nav_binding(candidate_body, nav)
        _require_nav_matches_broker_account(nav, snapshot)
        broker_proof = _atomic_broker_proof(
            snapshot,
            ranking_snapshot_id=ranking_snapshot_id,
            candidate_id=candidate_id,
            proposal_hash=proposal_hash,
        )
        strategy_nav_proof = _strategy_nav_proof(nav)
        store_now = now or datetime.now(timezone.utc)
        try:
            result = _invoke_port(
                self.approval_store,
                ("confirm_challenge",),
                challenge_id,
                challenge_response=confirmation.get("challenge_response"),
                risk_acknowledged=confirmation.get("risk_acknowledged"),
                second_confirmation=confirmation.get("second_confirmation"),
                confirmation_token=confirmation.get("confirmation_token"),
                ranking_store=self.ranking_store,
                policy_resolver=self.policy_resolver,
                risk_authority_resolver=self.risk_authority_resolver,
                execution_cost_contract=self.execution_cost_contract,
                broker_proof=broker_proof,
                strategy_nav_proof=strategy_nav_proof,
                strategy_nav_source=self.strategy_nav_source,
                strategy_nav_snapshot=nav,
                now=store_now,
            )
        except (
            ProposalApprovalConflict,
            RankOneAuthorizationForbidden,
            OptionsCopilotUnavailable,
        ):
            raise
        except (ApprovalChallengeRejected, ActiveApprovalExists) as exc:
            raise ProposalApprovalConflict(str(exc) or "approval confirmation rejected") from exc
        except Exception as exc:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval confirmation store is unavailable or corrupt"
            ) from exc
        if result is None:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval confirmation store returned no result"
            )
        approval = getattr(result, "approval", None)
        if approval is None and isinstance(result, Mapping):
            approval = result.get("approval", result)
        try:
            payload = _read_model(approval)
        except TypeError as exc:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval confirmation store returned an invalid result"
            ) from exc
        approval_id = payload.get("approval_id")
        expires_at = payload.get("expires_at")
        if isinstance(expires_at, datetime):
            expires_at = expires_at.isoformat()
        if not isinstance(approval_id, str) or not isinstance(expires_at, str):
            raise OptionsCopilotUnavailable(
                "NO_TRADE: approval confirmation result is incomplete"
            )
        return {
            "approval_id": approval_id,
            "proposal_hash": payload.get("proposal_hash"),
            "status": "PENDING_CODEX_BRIDGE",
            "expires_at": expires_at,
            "status_url": f"/api/approvals/{approval_id}",
            "instruction_id": None,
            "ibkr_deep_link": None,
            "review_only": True,
            "order_submitted": False,
            "transmitted_to_broker": False,
        }

    def _revalidate_current_broker(
        self,
        *,
        ranking_snapshot_id: str,
        candidate_id: str,
        candidate_body: Mapping[str, object],
        proposal_body: Mapping[str, object],
        now: datetime | None,
    ) -> AtomicBrokerSnapshot:
        """Requery immutable broker truth without crossing the bridge boundary."""

        resolver = self.options_evidence_acquisition
        builder = self.broker_snapshot_builder
        resolve_contracts = getattr(resolver, "resolve_contracts", None)
        build = getattr(builder, "build", None)
        if not callable(resolve_contracts) or not callable(build):
            raise OptionsCopilotUnavailable(
                "NO_TRADE: current broker snapshot revalidation is unavailable"
            )
        try:
            resolved = _invoke_port(
                resolver,
                ("resolve_contracts",),
                proposal=proposal_body,
                candidate=candidate_body,
                proposal_body=proposal_body,
                candidate_body=candidate_body,
                ranking_snapshot_id=ranking_snapshot_id,
                candidate_id=candidate_id,
            )
            contracts = tuple(resolved)
        except Exception as exc:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: current broker contract resolution is unavailable"
            ) from exc
        if not contracts or not all(
            isinstance(contract, OptionContractRef) for contract in contracts
        ):
            raise OptionsCopilotUnavailable(
                "NO_TRADE: broker contract resolution returned invalid contracts"
            )
        _require_frozen_contract_bindings(
            candidate_id=candidate_id,
            candidate_body=candidate_body,
            proposal_body=proposal_body,
            contracts=contracts,
        )
        try:
            snapshot = build(contracts)
        except Exception as exc:
            raise OptionsCopilotUnavailable(
                "NO_TRADE: current broker snapshot is unavailable"
            ) from exc
        if not isinstance(snapshot, AtomicBrokerSnapshot):
            raise OptionsCopilotUnavailable(
                "NO_TRADE: broker snapshot builder returned an invalid result"
            )
        _require_current_atomic_broker_snapshot(
            snapshot,
            contracts=contracts,
            now=now or datetime.now(timezone.utc),
        )
        return snapshot

    def _strategy_nav(
        self,
        at: datetime,
        *,
        observed_account_nlv: Decimal | None = None,
    ) -> tuple[StrategyNavSnapshot | None, tuple[str, ...]]:
        source = self.strategy_nav_source
        if source is None or "strategy_nav_source" in self.invalid_dependencies:
            return None, ("STRATEGY_NAV_UNAVAILABLE",)
        try:
            value = _invoke_port(
                source,
                ("snapshot",),
                asof=at,
                observed_account_nlv=observed_account_nlv,
            )
        except Exception:
            return None, ("STRATEGY_NAV_UNAVAILABLE",)
        if not isinstance(value, StrategyNavSnapshot):
            return None, ("STRATEGY_NAV_INVALID",)
        reasons: list[str] = []
        if not value.valid or value.strategy_nav is None:
            reasons.append("STRATEGY_NAV_INVALID")
        else:
            try:
                _positive_decimal(value.strategy_nav, "strategy_nav.strategy_nav")
            except ValueError:
                reasons.append("STRATEGY_NAV_INVALID")
        if (
            not _digest(value.content_hash)
            or canonical_hash(value.hash_payload()) != value.content_hash
            or not _digest(value.authority_hash)
            or canonical_hash(value.authority_payload()) != value.authority_hash
            or not _digest(value.contract_hash)
            or not _digest(value.ledger_head_hash)
        ):
            reasons.append("STRATEGY_NAV_BINDING_INVALID")
        at_is_aware = (
            isinstance(at, datetime)
            and at.tzinfo is not None
            and at.utcoffset() is not None
        )
        asof_is_aware = (
            isinstance(value.asof, datetime)
            and value.asof.tzinfo is not None
            and value.asof.utcoffset() is not None
        )
        if not at_is_aware or not asof_is_aware or value.asof > at:
            reasons.append("STRATEGY_NAV_ASOF_INVALID")
        return (value if not reasons else None), tuple(dict.fromkeys(reasons))

    @staticmethod
    def _ranking_unavailable() -> dict[str, object]:
        return {
            "decision": "NO_TRADE",
            "recommendations_available": False,
            "approval_enabled": False,
            "reasons": ("RANKING_STORE_UNAVAILABLE",),
            "candidates": (),
            "review_only": True,
            "direct_order_submission": False,
        }


def _require_frozen_contract_bindings(
    *,
    candidate_id: str,
    candidate_body: Mapping[str, object],
    proposal_body: Mapping[str, object],
    contracts: tuple[OptionContractRef, ...],
) -> None:
    """Bind both frozen leg representations to the same resolved contracts."""

    if candidate_body.get("candidate_id") != candidate_id:
        raise ProposalApprovalConflict("frozen candidate identity changed")
    proposal_identities = [
        proposal_body[key]
        for key in ("candidate_id", "proposal_id")
        if key in proposal_body
    ]
    if not proposal_identities or any(
        not isinstance(value, str) or value.strip() != candidate_id
        for value in proposal_identities
    ):
        raise ProposalApprovalConflict("frozen proposal identity changed")
    by_id: dict[int, OptionContractRef] = {}
    by_external_id: dict[str, OptionContractRef] = {}
    for contract in contracts:
        external_id = contract.contract_id_ex.strip()
        symbol = contract.symbol.strip().upper()
        if (
            isinstance(contract.contract_id, bool)
            or contract.contract_id <= 0
            or not external_id
            or not symbol
            or contract.contract_id in by_id
            or external_id in by_external_id
        ):
            raise ProposalApprovalConflict(
                "resolved broker contract identities are invalid or duplicated"
            )
        by_id[contract.contract_id] = contract
        by_external_id[external_id] = contract

    candidate_legs = _non_string_sequence(candidate_body.get("legs"))
    proposal_legs = _non_string_sequence(proposal_body.get("legs"))
    if not candidate_legs or not proposal_legs or len(candidate_legs) != len(contracts):
        raise ProposalApprovalConflict(
            "frozen candidate legs do not exactly match resolved broker contracts"
        )
    if len(proposal_legs) != len(contracts):
        raise ProposalApprovalConflict(
            "frozen proposal legs do not exactly match resolved broker contracts"
        )

    candidate_contracts: list[OptionContractRef] = []
    candidate_ids: set[int] = set()
    for index, leg in enumerate(candidate_legs):
        if not isinstance(leg, Mapping):
            raise ProposalApprovalConflict(
                f"frozen candidate leg {index} is invalid"
            )
        contract_id = leg.get("con_id")
        if (
            isinstance(contract_id, bool)
            or not isinstance(contract_id, int)
            or contract_id in candidate_ids
            or contract_id not in by_id
        ):
            raise ProposalApprovalConflict(
                "frozen candidate leg identities do not match resolved broker contracts"
            )
        candidate_ids.add(contract_id)
        candidate_contracts.append(by_id[contract_id])
    if candidate_ids != set(by_id):
        raise ProposalApprovalConflict(
            "frozen candidate leg identities do not exactly match resolved broker contracts"
        )

    resolved_symbols = {contract.symbol.strip().upper() for contract in contracts}
    if len(resolved_symbols) != 1:
        raise ProposalApprovalConflict(
            "resolved broker contracts do not share one underlying symbol"
        )
    resolved_symbol = next(iter(resolved_symbols))
    _require_root_symbol_binding(
        candidate_body,
        resolved_symbol=resolved_symbol,
        label="frozen candidate",
        required=False,
    )
    _require_root_symbol_binding(
        proposal_body,
        resolved_symbol=resolved_symbol,
        label="frozen proposal",
        required=True,
    )

    for index, (leg, contract) in enumerate(
        zip(candidate_legs, candidate_contracts, strict=True)
    ):
        assert isinstance(leg, Mapping)
        _require_optional_leg_identity(
            leg,
            contract,
            label=f"frozen candidate leg {index}",
            include_con_id=False,
        )

    proposal_ids: set[str] = set()
    for index, (leg, candidate_contract) in enumerate(
        zip(proposal_legs, candidate_contracts, strict=True)
    ):
        if not isinstance(leg, Mapping):
            raise ProposalApprovalConflict(f"frozen proposal leg {index} is invalid")
        external_id = leg.get("contract_id_ex")
        if (
            not isinstance(external_id, str)
            or external_id in proposal_ids
            or external_id not in by_external_id
            or by_external_id[external_id] is not candidate_contract
        ):
            raise ProposalApprovalConflict(
                "frozen proposal leg identities do not match resolved broker contracts"
            )
        proposal_ids.add(external_id)
        _require_optional_leg_identity(
            leg,
            candidate_contract,
            label=f"frozen proposal leg {index}",
            include_con_id=True,
        )
        _require_proposal_leg_matches_contract(leg, candidate_contract, index=index)
    if proposal_ids != set(by_external_id):
        raise ProposalApprovalConflict(
            "frozen proposal leg identities do not exactly match resolved broker contracts"
        )


def _require_root_symbol_binding(
    body: Mapping[str, object],
    *,
    resolved_symbol: str,
    label: str,
    required: bool,
) -> None:
    values = [body[key] for key in ("symbol", "underlying") if key in body]
    if required and not values:
        raise ProposalApprovalConflict(f"{label} symbol is missing")
    if not values:
        return
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ProposalApprovalConflict(f"{label} symbol is invalid")
    normalized = {str(value).strip().upper() for value in values}
    if len(normalized) != 1 or next(iter(normalized)) != resolved_symbol:
        raise ProposalApprovalConflict(
            f"{label} symbol does not match resolved broker contracts"
        )


def _require_optional_leg_identity(
    leg: Mapping[str, object],
    contract: OptionContractRef,
    *,
    label: str,
    include_con_id: bool,
) -> None:
    symbol_values = [leg[key] for key in ("symbol", "underlying") if key in leg]
    if symbol_values:
        if any(
            not isinstance(value, str) or not value.strip()
            for value in symbol_values
        ) or {
            str(value).strip().upper() for value in symbol_values
        } != {contract.symbol.strip().upper()}:
            raise ProposalApprovalConflict(
                f"{label} symbol does not match resolved broker contract"
            )
    for field, expected in (
        ("local_symbol", contract.local_symbol),
        ("trading_class", contract.trading_class),
    ):
        if field not in leg:
            continue
        value = leg[field]
        if not isinstance(value, str) or value.strip() != expected.strip():
            raise ProposalApprovalConflict(
                f"{label} {field} does not match resolved broker contract"
            )
    if include_con_id and "con_id" in leg:
        value = leg["con_id"]
        if isinstance(value, bool) or not isinstance(value, int) or value != contract.contract_id:
            raise ProposalApprovalConflict(
                f"{label} con_id does not match resolved broker contract"
            )


def _require_proposal_leg_matches_contract(
    leg: Mapping[str, object],
    contract: OptionContractRef,
    *,
    index: int,
) -> None:
    expected_text = {
        "underlying": contract.symbol.strip().upper(),
        "security_type": "OPT",
        "expiration": contract.expiration.isoformat(),
        "right": "CALL" if contract.right == "C" else "PUT",
        "currency": contract.currency.strip().upper(),
        "exchange": contract.exchange.strip().upper(),
    }
    for field, expected in expected_text.items():
        value = leg.get(field)
        if not isinstance(value, str) or value.strip().upper() != expected:
            label = "symbol" if field == "underlying" else "identity"
            raise ProposalApprovalConflict(
                f"frozen proposal leg {index} {label} does not match resolved broker contract"
            )
    try:
        strike = _positive_decimal(leg.get("strike"), f"proposal legs[{index}].strike")
        multiplier = _positive_decimal(
            leg.get("multiplier"), f"proposal legs[{index}].multiplier"
        )
    except ValueError as exc:
        raise ProposalApprovalConflict(
            f"frozen proposal leg {index} identity is invalid"
        ) from exc
    if strike != contract.strike or multiplier != Decimal(contract.multiplier):
        raise ProposalApprovalConflict(
            f"frozen proposal leg {index} identity does not match resolved broker contract"
        )


def _require_current_nav_binding(
    candidate_body: Mapping[str, object], nav: StrategyNavSnapshot
) -> None:
    if canonical_hash(nav.hash_payload()) != nav.content_hash:
        raise ProposalApprovalConflict(
            "current Strategy NAV content hash is invalid"
        )
    try:
        require_candidate_strategy_nav_binding(
            candidate_body,
            _strategy_nav_binding(nav),
        )
    except ApprovalProofError as exc:
        raise ProposalApprovalConflict(
            "current Strategy NAV does not match the frozen candidate binding"
        ) from exc


def _require_nav_matches_broker_account(
    nav: StrategyNavSnapshot, snapshot: AtomicBrokerSnapshot
) -> None:
    broker_nlv = _atomic_account_nlv(snapshot)
    observed_nlv = nav.observed_account_nlv
    if (
        observed_nlv is None
        or not observed_nlv.is_finite()
        or observed_nlv <= 0
        or observed_nlv != broker_nlv
    ):
        raise ProposalApprovalConflict(
            "Strategy NAV observed account NLV does not match current broker account NLV"
        )


def _strategy_nav_reconciles_control_observation(
    nav: StrategyNavSnapshot,
    *,
    asof: datetime,
    observed_account_nlv: Decimal,
) -> bool:
    """Verify the display-only control NAV against one exact account observation."""

    strategy_nav = nav.strategy_nav
    reconciliation = nav.reconciliation_difference
    if (
        not nav.valid
        or strategy_nav is None
        or not strategy_nav.is_finite()
        or strategy_nav <= 0
        or nav.asof != asof
        or nav.observed_account_nlv != observed_account_nlv
        or reconciliation is None
        or not reconciliation.is_finite()
    ):
        return False
    expected = (observed_account_nlv - strategy_nav).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_EVEN,
    )
    return reconciliation == expected


def _strategy_nav_proof(nav: StrategyNavSnapshot) -> dict[str, object]:
    if (
        nav.strategy_nav is None
        or nav.observed_account_nlv is None
        or nav.reconciliation_difference is None
        or not _digest(nav.content_hash)
        or not _digest(nav.authority_hash)
        or not _digest(nav.contract_hash)
        or not _digest(nav.ledger_head_hash)
    ):
        raise ProposalApprovalConflict(
            "current Strategy NAV proof is incomplete or invalid"
        )
    return {
        "schema": STRATEGY_NAV_PROOF_SCHEMA,
        "content_hash": nav.content_hash,
        "authority_hash": nav.authority_hash,
        "contract_hash": nav.contract_hash,
        "ledger_head_hash": nav.ledger_head_hash,
        "strategy_nav_usd": format(nav.strategy_nav, "f"),
        "observed_account_nlv": format(nav.observed_account_nlv, "f"),
        "reconciliation_difference": format(
            nav.reconciliation_difference,
            "f",
        ),
        "asof": nav.asof.isoformat(),
        "snapshot_payload": nav.hash_payload(),
    }


def _strategy_nav_binding(nav: StrategyNavSnapshot) -> dict[str, object]:
    return {
        "content_hash": nav.content_hash,
        "authority_hash": nav.authority_hash,
        "contract_hash": nav.contract_hash,
        "ledger_head_hash": nav.ledger_head_hash,
        "strategy_nav_usd": nav.strategy_nav,
        "observed_account_nlv": nav.observed_account_nlv,
        "reconciliation_difference": nav.reconciliation_difference,
        "asof": nav.asof,
    }


def _atomic_broker_proof(
    snapshot: AtomicBrokerSnapshot,
    *,
    ranking_snapshot_id: str,
    candidate_id: str,
    proposal_hash: str,
) -> dict[str, object]:
    if not _digest(proposal_hash):
        raise ProposalApprovalConflict("frozen proposal hash is invalid")
    state_hashes = {
        component: snapshot.state_evidence[component].post_hash
        for component in (
            "account",
            "positions",
            "working_orders",
            "unsubmitted_instructions",
        )
    }
    if any(not _digest(value) for value in state_hashes.values()):
        raise ProposalApprovalConflict(
            "current broker state proof hashes are incomplete"
        )
    oldest_quote_observed_at = min(
        quote.observed_at for quote in snapshot.quotes
    )
    contract_ids = sorted(item.contract_id for item in snapshot.secdef_evidence)
    return {
        "schema": BROKER_PROOF_SCHEMA,
        "ranking_snapshot_id": ranking_snapshot_id,
        "candidate_id": candidate_id,
        "proposal_hash": proposal_hash,
        "snapshot_hash": snapshot.snapshot_hash,
        "built_at": snapshot.built_at.isoformat(),
        "quote_batch_id": snapshot.quote_batch_id,
        "oldest_quote_observed_at": oldest_quote_observed_at.isoformat(),
        "state_hashes": state_hashes,
        "contract_definitions_hash": _atomic_contract_definitions_hash(snapshot),
        "quotes_hash": _atomic_quotes_hash(snapshot),
        "contract_ids": contract_ids,
        "account_nlv_usd": format(_atomic_account_nlv(snapshot), "f"),
        "open_option_position_count": 0,
        "working_order_count": 0,
        "unsubmitted_instruction_count": 0,
        "status": BrokerSnapshotStatus.COMPLETE.value,
    }


def _atomic_contract_definitions_hash(snapshot: AtomicBrokerSnapshot) -> str:
    definitions = [
        {
            "contract_id": item.contract_id,
            "identity": item.post_identity,
            "identity_hash": item.post_hash,
            "source": item.post_source,
            "standard_contract": True,
            "adjusted": False,
        }
        for item in sorted(
            snapshot.secdef_evidence,
            key=lambda evidence: evidence.contract_id,
        )
    ]
    if not definitions or any(
        item["identity"] is None
        or not _digest(item["identity_hash"])
        or not isinstance(item["source"], str)
        or not item["source"].strip()
        for item in definitions
    ):
        raise ProposalApprovalConflict(
            "current broker contract definition proof is incomplete"
        )
    return canonical_hash(
        {
            "schema": "options_copilot.atomic_contract_definitions.v1",
            "definitions": definitions,
        }
    )


def _atomic_quotes_hash(snapshot: AtomicBrokerSnapshot) -> str:
    quotes = snapshot.hash_payload().get("quotes")
    if not isinstance(quotes, list) or not quotes:
        raise ProposalApprovalConflict("current broker quote proof is incomplete")
    return canonical_hash(
        {
            "schema": "options_copilot.atomic_quotes.v1",
            "quote_batch_id": snapshot.quote_batch_id,
            "quotes": quotes,
        }
    )


def _atomic_account_nlv(snapshot: AtomicBrokerSnapshot) -> Decimal:
    try:
        return atomic_account_nlv(snapshot)
    except (TypeError, ValueError) as exc:
        raise ProposalApprovalConflict(
            "current broker account proof has invalid net liquidation"
        ) from exc


def _require_current_atomic_broker_snapshot(
    snapshot: AtomicBrokerSnapshot,
    *,
    contracts: tuple[OptionContractRef, ...],
    now: datetime,
) -> None:
    if not _digest(snapshot.snapshot_hash) or not snapshot.verify_hash():
        raise ProposalApprovalConflict("current broker snapshot immutable hash mismatch")
    if snapshot.status is not BrokerSnapshotStatus.COMPLETE or not snapshot.complete:
        details = ",".join(snapshot.reason_codes) or snapshot.status.value
        raise ProposalApprovalConflict(f"current broker snapshot conflict: {details}")
    age = Decimal(str((now - snapshot.built_at).total_seconds()))
    if age < 0 or age > MAX_QUOTE_AGE_SECONDS:
        raise ProposalApprovalConflict("current broker snapshot is stale or future-dated")
    if (
        snapshot.quote_batch_status is not QuoteBatchStatus.COMPLETE
        or not isinstance(snapshot.quote_batch_id, str)
        or not snapshot.quote_batch_id.strip()
        or not isinstance(snapshot.quote_batch_source, str)
        or not snapshot.quote_batch_source.strip()
        or snapshot.oldest_quote_age_seconds is None
        or snapshot.maximum_leg_skew_seconds is None
        or snapshot.oldest_quote_age_seconds + age > MAX_QUOTE_AGE_SECONDS
        or snapshot.maximum_leg_skew_seconds > MAX_LEG_SKEW_SECONDS
    ):
        raise ProposalApprovalConflict(
            "current broker snapshot quote batch is stale or incoherent"
        )
    contract_ids = {contract.contract_id for contract in contracts}
    quote_ids = {quote.contract_id for quote in snapshot.quotes}
    if (
        len(snapshot.quotes) != len(contracts)
        or quote_ids != contract_ids
        or any(
            quote.bid is None
            or quote.ask is None
            or quote.batch_id != snapshot.quote_batch_id
            for quote in snapshot.quotes
        )
    ):
        raise ProposalApprovalConflict(
            "current broker snapshot executable quotes do not match frozen legs"
        )
    if (
        len(snapshot.secdef_evidence) != len(contracts)
        or {item.contract_id for item in snapshot.secdef_evidence} != contract_ids
        or any(
            not item.stable
            or not item.standard_contract
            or item.adjusted
            or item.pre_identity is None
            or item.post_identity is None
            or not _digest(item.pre_hash)
            or item.pre_hash != item.post_hash
            for item in snapshot.secdef_evidence
        )
    ):
        raise ProposalApprovalConflict(
            "current broker snapshot contract definitions are incomplete or unstable"
        )

    states: dict[str, object] = {}
    for component in (
        "account",
        "positions",
        "working_orders",
        "unsubmitted_instructions",
    ):
        evidence = snapshot.state_evidence.get(component)
        if (
            evidence is None
            or not evidence.known
            or not evidence.stable
            or evidence.state is None
            or not _digest(evidence.pre_hash)
            or not _digest(evidence.post_hash)
            or (
                component != "account"
                and evidence.pre_hash != evidence.post_hash
            )
        ):
            raise ProposalApprovalConflict(
                f"current broker snapshot {component} state is unavailable or unstable"
            )
        states[component] = evidence.state
    account = states["account"]
    if not isinstance(account, Mapping) or not account:
        raise ProposalApprovalConflict("current broker snapshot account state is invalid")
    _atomic_account_nlv(snapshot)
    positions = _non_string_sequence(states["positions"])
    working_orders = _non_string_sequence(states["working_orders"])
    instructions = _non_string_sequence(states["unsubmitted_instructions"])
    if positions is None or _has_open_or_ambiguous_option_position(positions):
        raise ProposalApprovalConflict(
            "current broker snapshot contains open or ambiguous option positions"
        )
    if working_orders is None or working_orders:
        raise ProposalApprovalConflict(
            "current broker snapshot working orders must be empty"
        )
    if instructions is None or instructions:
        raise ProposalApprovalConflict(
            "current broker snapshot unsubmitted instructions must be empty"
        )


def _non_string_sequence(value: object) -> Sequence[object] | None:
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return value
    return None


def _has_open_or_ambiguous_option_position(positions: Sequence[object]) -> bool:
    for item in positions:
        if not isinstance(item, Mapping):
            return True
        raw_quantities = [
            item[key]
            for key in ("position", "quantity", "contracts", "size")
            if key in item
        ]
        if not raw_quantities:
            return True
        try:
            quantities = tuple(Decimal(str(value)) for value in raw_quantities)
        except (InvalidOperation, TypeError, ValueError):
            return True
        if (
            any(isinstance(value, bool) for value in raw_quantities)
            or any(not value.is_finite() for value in quantities)
            or any(value != quantities[0] for value in quantities[1:])
        ):
            return True
        if quantities[0] == 0:
            continue
        security_types = {
            str(item[key]).strip().upper()
            for key in ("asset_class", "security_type", "sec_type")
            if key in item
        }
        if not security_types or len(security_types) != 1:
            return True
        if next(iter(security_types)) in {"OPT", "OPTION", "BAG", "COMBO"}:
            return True
    return False


def _supports_capabilities(
    port: object, groups: tuple[tuple[str, ...], ...]
) -> bool:
    """Return true when every required capability group has one callable."""

    return all(
        any(callable(getattr(port, name, None)) for name in alternatives)
        for alternatives in groups
    )


def _invoke_port(
    port: object,
    names: tuple[str, ...],
    *args: object,
    **kwargs: object,
) -> object:
    target = port if callable(port) else next(
        (
            getattr(port, name)
            for name in names
            if callable(getattr(port, name, None))
        ),
        None,
    )
    if target is None:
        raise TypeError(f"runtime port lacks one of {names!r}")
    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):
        accepted = kwargs
    else:
        accepted = (
            kwargs
            if any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )
            else {
                key: value
                for key, value in kwargs.items()
                if key in signature.parameters
            }
        )
    return target(*args, **accepted)


def _read_model(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    converter = getattr(value, "as_dict", None)
    if callable(converter):
        converted = converter()
        if isinstance(converted, Mapping):
            return dict(converted)
    if is_dataclass(value):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    raise TypeError("runtime read model must be a mapping or frozen record")


def _joint_ranking_projection(value: Mapping[str, object]) -> dict[str, object]:
    """Expose only the separate research watchlist from immutable joint evidence."""

    immutable = value.get("immutable_inputs")
    if not isinstance(immutable, Mapping):
        return {"research_watchlist": (), "joint_ranking_hash": None}
    trace = immutable.get("funnel_trace")
    if not isinstance(trace, Mapping):
        return {"research_watchlist": (), "joint_ranking_hash": None}
    joint = trace.get("joint_ranking")
    if not isinstance(joint, Mapping):
        return {"research_watchlist": (), "joint_ranking_hash": None}
    try:
        verified = JointRankingSnapshot.from_dict(joint)
    except (TypeError, ValueError):
        return {"research_watchlist": (), "joint_ranking_hash": None}
    return {
        "research_watchlist": tuple(
            row.as_dict() for row in verified.research_watchlist
        ),
        "joint_ranking_hash": verified.snapshot_hash,
    }


def _learning_evaluation_stage(status: object, independent_count: object) -> str:
    """Expose comparison availability without claiming threshold discovery."""

    try:
        count = int(independent_count)
    except (TypeError, ValueError):
        count = 0
    if str(status or "").upper() != "AVAILABLE":
        return "COLLECTING"
    return (
        "DISCOVERY"
        if count >= DISCOVERY_SAMPLE_THRESHOLD
        else "COMPARISON_AVAILABLE"
    )


def _no_trade_read_model(reason: str) -> dict[str, object]:
    return {
        "status": "DEGRADED",
        "decision": "NO_TRADE",
        "approval_enabled": False,
        "reasons": (reason,),
        "review_only": True,
        "direct_order_submission": False,
    }


def _latest_terminal_no_trade(store: object) -> dict[str, object] | None:
    """Project the verified NO_TRADE chain head without inventing a ranking.

    A legitimate scan can terminate before a ranking snapshot exists.  The
    immutable decision chain is still the operator-facing authority for why no
    candidates were produced, so expose only its bounded diagnostic allowlist.
    """

    reader = getattr(store, "latest_decision", None)
    if not callable(reader):
        return None
    try:
        raw_latest = reader()
        if raw_latest is None:
            return None
        latest = _read_model(raw_latest)
    except Exception:
        return _no_trade_read_model("RANKING_DECISION_LEDGER_UNAVAILABLE")
    if str(latest.get("record_type") or "").strip().upper() != "NO_TRADE":
        return None
    record = latest.get("record")
    if not isinstance(record, Mapping):
        return None
    if str(record.get("status") or "").strip().upper() != "NO_TRADE":
        return None
    scan_run_id = str(record.get("scan_run_id") or "").strip()
    if not scan_run_id:
        return None

    trace_raw = record.get("funnel_trace")
    trace_source = trace_raw if isinstance(trace_raw, Mapping) else {}
    trace: dict[str, object] = {}
    for key in (
        "discovered_underlyings",
        "deep_scan_requested",
        "deep_scan_attempted",
        "deep_scan_completed",
        "deep_scan_deferred",
        "ranked_count",
        "ranked_limit",
        "filler_candidates",
    ):
        value = trace_source.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            trace[key] = value

    reason_codes = _bounded_string_values(record.get("reasons"), maximum=32)
    acquisition_reasons = _bounded_string_values(
        trace_source.get("underlying_quote_reason_codes"),
        maximum=32,
    )
    missing_symbols = _bounded_string_values(
        trace_source.get("underlying_quote_missing_symbols"),
        maximum=100,
    )
    if acquisition_reasons:
        trace["underlying_quote_reason_codes"] = acquisition_reasons
    if missing_symbols:
        trace["underlying_quote_missing_symbols"] = missing_symbols
    for key in (
        "deep_scan_deferred_symbols",
        "optionability_excluded_symbols",
        "underlying_quote_excluded_symbols",
    ):
        values = _bounded_string_values(trace_source.get(key), maximum=100)
        if values:
            trace[key] = values
    for key in (
        "optionability_exclusion_reasons",
        "underlying_quote_exclusion_reasons",
    ):
        raw_rows = trace_source.get(key)
        if not isinstance(raw_rows, Sequence) or isinstance(
            raw_rows,
            (str, bytes, bytearray, memoryview),
        ):
            continue
        rows: list[dict[str, str]] = []
        for raw_row in raw_rows[:100]:
            if not isinstance(raw_row, Mapping):
                continue
            symbol = str(raw_row.get("symbol") or "").strip().upper()
            reason_code = str(raw_row.get("reason_code") or "").strip().upper()
            if not symbol or not reason_code:
                continue
            rows.append({"symbol": symbol, "reason_code": reason_code})
        if rows:
            trace[key] = tuple(rows)
    research_allocation = _normalise_research_allocation(
        trace_source.get("research_allocation")
    )
    if research_allocation is not None:
        trace["research_allocation"] = research_allocation
    joint_projection = _joint_ranking_projection(
        {"immutable_inputs": {"funnel_trace": trace_source}}
    )
    if joint_projection["joint_ranking_hash"] is not None:
        trace["joint_ranking"] = trace_source["joint_ranking"]
    combined_reasons = tuple(dict.fromkeys((*reason_codes, *acquisition_reasons)))
    if not combined_reasons:
        combined_reasons = ("NO_TRADE",)

    result: dict[str, object] = {
        "status": "DEGRADED",
        "decision": "NO_TRADE",
        "approval_enabled": False,
        "reasons": combined_reasons,
        "scan_run_id": scan_run_id,
        "ranking_snapshot_id": None,
        "candidates": (),
        "funnel_trace": trace,
        "missing_symbols": missing_symbols,
        "review_only": True,
        "direct_order_submission": False,
        **joint_projection,
    }
    for key in ("decision_hash", "record_hash"):
        value = str(latest.get(key) or "").strip().lower()
        if len(value) == 64 and all(character in "0123456789abcdef" for character in value):
            result[key] = value
    gate_bundle_hash = str(record.get("gate_bundle_hash") or "").strip().lower()
    if len(gate_bundle_hash) == 64 and all(
        character in "0123456789abcdef" for character in gate_bundle_hash
    ):
        result["gate_bundle_hash"] = gate_bundle_hash
    recorded_at = latest.get("recorded_at")
    if isinstance(recorded_at, datetime):
        result["recorded_at"] = recorded_at.isoformat()
    return result


def _bounded_string_values(value: object, *, maximum: int) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        return ()
    checked: list[str] = []
    for item in value[:maximum]:
        text = str(item or "").strip().upper()
        if text and len(text) <= 64 and text not in checked:
            checked.append(text)
    return tuple(checked)


def _normalise_research_allocation(value: object) -> dict[str, object] | None:
    """Validate and replay complete v3 SUPPORTING_ONLY scheduling evidence."""

    return normalise_research_allocation_evidence(value)

def _nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _bounded_symbol(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if value != value.strip() or value != value.upper():
        return None
    if not value or len(value) > 16 or value[0] not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        return None
    allowed = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-")
    if any(character not in allowed for character in value):
        return None
    return value


def _bounded_identifier(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or len(text) > 160:
        return None
    if any(
        not (character.isalnum() or character in {".", "-", "_", ":"})
        for character in text
    ):
        return None
    return text


def _bounded_hash(value: object) -> str | None:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        return None
    return text


def _bounded_timestamp(value: object) -> str | None:
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, str) and value.strip():
        try:
            timestamp = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        return None
    return timestamp.astimezone(timezone.utc).isoformat()


def _scan_gate_from_reasons(reason_codes: Sequence[str]) -> str:
    """Classify a diagnostic stop without changing canonical Gate authority."""

    reasons = tuple(str(item).strip().upper() for item in reason_codes)
    if not reasons:
        return "ALL_HARD_GATES_PASSED"
    if any("RANKING" in item or "LEDGER" in item for item in reasons):
        return "GATE_6_RANKING_REVIEWABILITY"
    if any(
        token in item
        for item in reasons
        for token in (
            "RISK",
            "MAXIMUM_LOSS",
            "MAX_LOSS",
            "STRUCTURE",
            "PAYOFF",
            "BREAKEVEN",
            "DTE",
        )
    ):
        return "GATE_5_STRUCTURE_ACCOUNT_RISK"
    if any(
        token in item
        for item in reasons
        for token in (
            "LIQUIDITY",
            "SPREAD",
            "VOLUME",
            "OPEN_INTEREST",
            "AFTER_COST_EV",
            "VOLATILITY",
        )
    ):
        return "GATE_4_OPTION_EDGE_LIQUIDITY"
    if any("EVENT" in item or "EARNINGS" in item for item in reasons):
        return "GATE_3_UNDERLYING_EVENT"
    if any("MARKET_CREDIT" in item or "REGIME" in item for item in reasons):
        return "GATE_2_MARKET_CREDIT_REGIME"
    return "GATE_1_AUTHORITY_DATA"


def _consumed_immediate_scan_attempt(response: Mapping[str, object]) -> bool:
    """Return true only when one manual scope produced complete durable evidence."""

    reasons = _bounded_string_values(response.get("reasons"), maximum=32)
    if any("PACING" in reason for reason in reasons):
        return False
    return bool(
        str(response.get("scan_status") or "").strip().upper() == "COMPLETED"
        and _bounded_identifier(response.get("scan_run_id")) is not None
        and all(
            _bounded_hash(response.get(key)) is not None
            for key in ("decision_hash", "record_hash", "gate_bundle_hash")
        )
    )


def _completed_campaign_attempt(item: Mapping[str, object]) -> bool:
    """Count only hash-bound reevaluations that were not stopped by pacing."""

    if (
        item.get("attempt_kind") != "REEVALUATION"
        or item.get("scan_status") != "COMPLETED"
    ):
        return False
    reasons = _bounded_string_values(item.get("reason_codes"), maximum=32)
    if any("PACING" in reason for reason in reasons):
        return False
    return all(
        isinstance(item.get(key), str) and bool(item.get(key))
        for key in ("decision_hash", "record_hash", "gate_bundle_hash")
    )


def _positioning_no_trade(reason: str) -> dict[str, object]:
    return {
        "schema_version": "options_copilot.positioning_feed.v1",
        "status": "UNAVAILABLE",
        "reasons": (reason,),
        "positioning": (),
        "count": 0,
        "decision_authority": "SUPPORTING_ONLY",
        "supporting_only": True,
        "affects_eligibility": False,
        "approval_allowed": False,
        "instruction_allowed": False,
        "order_allowed": False,
    }


def _candidate_evidence_no_trade(
    reason: str,
    *,
    scan_run_id: object = None,
    candidate_id: object = None,
) -> dict[str, object]:
    payload = {
        **_no_trade_read_model(reason),
        "decision_authority": "OBSERVATION_ONLY",
        "reason": reason,
        "primary": [],
        "supporting": [],
        "contradicting": [],
    }
    if isinstance(scan_run_id, str):
        payload["scan_run_id"] = scan_run_id
    if isinstance(candidate_id, str):
        payload["candidate_id"] = candidate_id
    return payload


def _digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _ranking_binding_reasons(
    payload: Mapping[str, object],
    nav: StrategyNavSnapshot | None,
    *,
    now: datetime,
) -> tuple[str, ...]:
    reasons: list[str] = []
    for field in (
        "snapshot_hash",
        "input_hash",
        "evidence_hash",
        "broker_snapshot_hash",
        "current_policy_hash",
        "policy_authority_marker_hash",
        "cost_hash",
        "risk_contract_hash",
        "risk_authority_marker_hash",
    ):
        if not _digest(payload.get(field)):
            reasons.append(f"RANKING_BINDING_INVALID:{field}")
    for field in (
        "ranking_snapshot_id",
        "scan_run_id",
        "current_policy_version",
        "cost_version",
        "risk_authority_version",
    ):
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            reasons.append(f"RANKING_BINDING_INVALID:{field}")
    valid_until = payload.get("valid_until")
    try:
        expiry = (
            valid_until
            if isinstance(valid_until, datetime)
            else datetime.fromisoformat(str(valid_until))
        )
        if expiry.tzinfo is None or expiry.utcoffset() is None:
            raise ValueError("ranking expiry must be timezone-aware")
        if expiry <= now:
            reasons.append("RANKING_SNAPSHOT_EXPIRED")
    except (TypeError, ValueError):
        reasons.append("RANKING_BINDING_INVALID:valid_until")

    candidates = payload.get("candidates", ())
    if not isinstance(candidates, (list, tuple)):
        return tuple((*reasons, "RANKING_CANDIDATES_INVALID"))
    for row in candidates:
        if not isinstance(row, Mapping):
            reasons.append("RANKING_CANDIDATE_INVALID")
            continue
        if any(
            not _digest(row.get(field))
            for field in ("candidate_hash", "ranking_basis_hash", "row_hash")
        ):
            reasons.append("RANKING_CANDIDATE_BINDING_INVALID")
        body = row.get("candidate_body")
        if not isinstance(body, Mapping):
            reasons.append("RANKING_CANDIDATE_BODY_INVALID")
            continue
        if nav is None:
            reasons.append("STRATEGY_NAV_UNAVAILABLE")
        else:
            try:
                if canonical_hash(nav.hash_payload()) != nav.content_hash:
                    raise ApprovalProofError(
                        "current Strategy NAV content hash is invalid"
                    )
                require_candidate_strategy_nav_binding(
                    body,
                    _strategy_nav_binding(nav),
                )
            except ApprovalProofError:
                reasons.append("STRATEGY_NAV_BINDING_STALE")
        if body.get("broker_snapshot_hash") != payload.get(
            "broker_snapshot_hash"
        ):
            reasons.append("BROKER_SNAPSHOT_BINDING_MISMATCH")
        if body.get("execution_cost_contract_hash") != payload.get("cost_hash"):
            reasons.append("COST_BINDING_MISMATCH")
        if body.get("policy_hash") != payload.get("current_policy_hash"):
            reasons.append("POLICY_BINDING_MISMATCH")
        evidence = body.get("evidence_hashes")
        if (
            not isinstance(evidence, Mapping)
            or not evidence
            or any(not _digest(value) for value in evidence.values())
        ):
            reasons.append("CANDIDATE_EVIDENCE_BINDING_INVALID")
        if not isinstance(body.get("exit_plan"), Mapping):
            reasons.append("EXIT_PLAN_BINDING_INVALID")
    return tuple(dict.fromkeys(reasons))


@dataclass(slots=True)
class ProductionComposition:
    """Concrete, owned production graph around one shared read-only gateway."""

    services: RuntimeServices
    lifecycle: ProductionLifecycle
    gateway: IBKRReadOnlyGateway
    calendar_provider: IBKRSessionCalendarProvider
    news_adapter: IBKRNewsResearchAdapter
    pipeline_inputs: ProductionPipelineInputs
    management_coordinator: ProductionManagementCoordinator
    pacing_guard: PacingAuthorityGuard
    outcome_market_adapter: ProductionOutcomeMarketAdapter
    top10_preselection_producer: Top10PreselectionProducer | None
    top10_scheduler_service: Top10SchedulerService
    top10_producer_blocker: str | None
    execution_cost_contract: Mapping[str, object]


@dataclass(slots=True)
class ExternalTop10Composition:
    """Connector-free external Top-10 graph; never owns an IBKR client."""

    producer: ExternalBatchBoundTop10Producer
    scheduler_service: Top10SchedulerService
    scheduler_loop: Top10OnlySchedulerLoop
    calendar_provider: ExternalSessionCalendarProvider
    bundle_guard: ExternalBundleCommitGuard
    scan_store: ScanRunStore
    _closed: bool = False

    def start(self) -> None:
        if not self._closed:
            self.scheduler_loop.start()

    def close(self) -> bool:
        if self._closed:
            return True
        if self.scheduler_loop.close() is False:
            return False
        self.scan_store.close()
        self._closed = True
        return True

    def health(self) -> Mapping[str, object]:
        state = self.scheduler_loop.health()
        return {
            **state,
            # The external Top-10 path is observation/research only.  A healthy
            # scheduler never becomes trading authority.
            "decision": "NO_TRADE",
            "connector_count": 0,
            "creator_transport": "UNAVAILABLE",
        }


@dataclass(frozen=True, slots=True)
class _FrozenExternalCalendarProvider:
    value: UsOptionsCalendarSnapshot

    def snapshot(self, *, now: datetime) -> UsOptionsCalendarSnapshot:
        return self.value


@dataclass(frozen=True, slots=True)
class _FrozenExternalFeedReader:
    value: ExternalReadonlyBatch

    def read(self) -> ExternalReadonlyBatch:
        return self.value


class _LifecycleControlAccountStateReader:
    """Read one fresh supervisor-owned control snapshot without broker I/O."""

    _STALE_AFTER_SECONDS = 15.0

    def __init__(self, *, clock: Callable[[], datetime]) -> None:
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._clock = clock
        self._lock = threading.RLock()
        self._snapshot_reader: Callable[[], Mapping[str, object]] | None = None
        self._local = threading.local()

    def bind(self, reader: Callable[[], Mapping[str, object]]) -> None:
        if not callable(reader):
            raise TypeError("control snapshot reader must be callable")
        with self._lock:
            if self._snapshot_reader is not None:
                raise RuntimeError("control snapshot reader is already bound")
            self._snapshot_reader = reader

    def positions(self) -> object:
        snapshot = self._current_snapshot()
        self._local.snapshot = snapshot
        if snapshot is None:
            return None
        positions = snapshot.get("positions")
        if not isinstance(positions, Sequence) or isinstance(
            positions,
            (str, bytes, bytearray, memoryview),
        ):
            return None
        detached: list[dict[str, object]] = []
        for item in positions:
            if not isinstance(item, Mapping):
                return None
            detached.append(dict(item))
        return tuple(detached)

    def working_orders(self) -> object:
        snapshot = self._latched_snapshot()
        if snapshot is None:
            return None
        return self._count_only_sequence(snapshot.get("working_order_count"))

    def unsubmitted_instructions(self) -> object:
        snapshot = self._latched_snapshot()
        try:
            if snapshot is None:
                return None
            return self._count_only_sequence(
                snapshot.get("unsubmitted_instruction_count")
            )
        finally:
            self._local.snapshot = None

    def _latched_snapshot(self) -> Mapping[str, object] | None:
        snapshot = getattr(self._local, "snapshot", None)
        return snapshot if isinstance(snapshot, Mapping) else self._current_snapshot()

    def _current_snapshot(self) -> Mapping[str, object] | None:
        with self._lock:
            reader = self._snapshot_reader
        if reader is None:
            return None
        try:
            raw = reader()
            now = self._clock()
        except Exception:
            return None
        if (
            not isinstance(raw, Mapping)
            or str(raw.get("status") or "").strip().upper() != "CURRENT"
            or raw.get("stale") is not False
            or raw.get("reason") is not None
            or not isinstance(now, datetime)
            or now.tzinfo is None
            or now.utcoffset() is None
        ):
            return None
        try:
            observed_at = datetime.fromisoformat(
                str(raw.get("observed_at") or "").replace("Z", "+00:00")
            )
        except ValueError:
            return None
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            return None
        age = (
            now.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)
        ).total_seconds()
        if age < 0 or age > self._STALE_AFTER_SECONDS:
            return None
        return dict(raw)

    @staticmethod
    def _count_only_sequence(value: object) -> tuple[None, ...] | None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return (None,) * value


@dataclass(frozen=True, slots=True)
class _FrozenExternalStructureSource:
    scheduled_for: datetime
    structures: tuple[object, ...]

    def resolve_top10(self, *, scheduled_for: datetime) -> tuple[object, ...]:
        if (
            not isinstance(scheduled_for, datetime)
            or scheduled_for.tzinfo is None
            or scheduled_for.utcoffset() is None
            or scheduled_for.astimezone(timezone.utc) != self.scheduled_for
        ):
            raise ValueError("frozen Top-10 scheduled_for mismatch")
        return self.structures


def build_external_top10_composition(
    config: OptionsCopilotConfig,
    *,
    news_preselection_store: object,
    clock: object | None = None,
) -> ExternalTop10Composition:
    """Build the external observation graph without constructing IBKR gateway."""

    if config.broker_acquisition_mode != "EXTERNAL":
        raise ValueError("external Top-10 composition requires EXTERNAL mode")
    if (
        config.external_readonly_feed_path is None
        or config.external_top10_path is None
        or config.external_session_calendar_path is None
    ):
        raise ValueError("external broker-published session gate is unavailable")
    if not _top10_store_ready(news_preselection_store):
        raise TypeError("independent Top-10 ledger is unavailable")
    now = clock if callable(clock) else (lambda: datetime.now(timezone.utc))
    bundle_guard = ExternalBundleCommitGuard(
        ExternalBundlePaths(
            readonly_feed=config.external_readonly_feed_path,
            top10=config.external_top10_path,
            session_calendar=config.external_session_calendar_path,
        )
    )
    calendar_provider = ExternalSessionCalendarProvider(
        config.external_session_calendar_path,
        clock=now,
    )
    session_gate = BrokerTradingSessionGate(calendar_provider, clock=now)
    feed_reader = ExternalReadonlyFeedReader(
        config.external_readonly_feed_path,
        clock=now,
    )
    structure_source = ExternalTop10StructureSource(
        config.external_top10_path,
        clock=now,
    )
    open_economics_resolver = OpenRepriceEconomicsResolver(
        SignedExecutionCostResolver(clock=now)
    )
    producer = ExternalBatchBoundTop10Producer(
        session_gate=session_gate,
        feed_reader=feed_reader,
        structure_source=structure_source,
        store=news_preselection_store,  # type: ignore[arg-type]
        clock=now,
        open_economics_resolver=open_economics_resolver,
    )

    def bundle_runtime_factory(
        snapshot: ExternalBundleSnapshot,
        checked_at: datetime,
    ) -> tuple[_FrozenExternalCalendarProvider, ExternalBatchBoundTop10Producer]:
        fixed_clock = lambda: checked_at
        manifest = snapshot.manifest
        calendar = ExternalSessionCalendarProvider(
            snapshot.paths.session_calendar,
            clock=fixed_clock,
        ).snapshot(now=checked_at)
        batch = ExternalReadonlyFeedReader(
            snapshot.paths.readonly_feed,
            clock=fixed_clock,
        ).read()
        expected_purpose = manifest.purpose
        if expected_purpose not in {
            PREMARKET_ACCOUNT_PURPOSE,
            OPEN_REPRICE_PURPOSE,
        }:
            raise ValueError("frozen external bundle purpose is unsupported")
        _, batch_reason = validate_external_readonly_batch(
            batch,
            expected_purpose=expected_purpose,
            scheduled_for=manifest.scheduled_for,
        )
        if batch_reason is not None:
            raise ValueError(f"frozen external batch is invalid: {batch_reason}")
        structures: tuple[object, ...] = ()
        if expected_purpose == PREMARKET_ACCOUNT_PURPOSE:
            structures = tuple(
                ExternalTop10StructureSource(
                    snapshot.paths.top10,
                    clock=fixed_clock,
                ).resolve_top10(scheduled_for=manifest.scheduled_for)
            )
        frozen_calendar = _FrozenExternalCalendarProvider(calendar)
        frozen_feed = _FrozenExternalFeedReader(batch)
        frozen_source = _FrozenExternalStructureSource(
            manifest.scheduled_for,
            structures,
        )
        frozen_producer = ExternalBatchBoundTop10Producer(
            session_gate=BrokerTradingSessionGate(
                frozen_calendar,
                clock=fixed_clock,
            ),
            feed_reader=frozen_feed,
            structure_source=frozen_source,  # type: ignore[arg-type]
            store=news_preselection_store,  # type: ignore[arg-type]
            clock=fixed_clock,
            open_economics_resolver=open_economics_resolver,
        )
        return frozen_calendar, frozen_producer

    scan_store = ScanRunStore(config.data_dir / "scan_runs.sqlite3")
    try:
        scheduler_service = Top10SchedulerService(
            scan_store,
            producer,
            pipeline_version=TOP10_PRODUCER_PIPELINE_VERSION,
            unavailable_reason=TOP10_PRODUCER_UNAVAILABLE,
        )
        scheduler_loop = Top10OnlySchedulerLoop(
            scheduler_service,
            calendar_provider,
            bundle_guard=bundle_guard,
            bundle_runtime_factory=bundle_runtime_factory,
            clock=now,
        )
        return ExternalTop10Composition(
            producer=producer,
            scheduler_service=scheduler_service,
            scheduler_loop=scheduler_loop,
            calendar_provider=calendar_provider,
            bundle_guard=bundle_guard,
            scan_store=scan_store,
        )
    except BaseException:
        scan_store.close()
        raise


def _top10_dependencies_ready(
    *,
    store: object | None,
    structure_source: object | None,
    session_gate: object | None,
    instruction_reader: object | None,
) -> bool:
    """Recognize only the explicit independent production seams.

    In particular, ``RankingStore`` is never a Top-10 source or ledger
    fallback.  Missing dependencies keep the scheduler installed only as an
    observable unavailable handler and never construct the producer.
    """

    if not _top10_store_ready(store):
        return False
    return (
        callable(getattr(structure_source, "resolve_top10", None))
        and callable(getattr(session_gate, "is_trading_session", None))
        and callable(instruction_reader)
    )


def _top10_store_ready(store: object | None) -> bool:
    if store is None or isinstance(store, RankingStore):
        return False
    return all(
        callable(getattr(store, name, None))
        for name in (
            "append_premarket_run",
            "latest_premarket",
            "append_open_batch",
        )
    )


def _top10_strategy_nav(
    ledger: StrategyNavLedger,
    snapshot: object,
) -> Decimal:
    """Resolve signed Strategy NAV against the same atomic broker snapshot.

    Account NLV is supplied only as reconciliation evidence.  It must never
    replace the signed ledger NAV when the ledger is absent, corrupt, stale,
    or otherwise invalid.
    """

    if not isinstance(snapshot, AtomicBrokerSnapshot):
        raise TypeError("atomic broker snapshot is unavailable")
    observed_account_nlv = _atomic_account_nlv(snapshot)
    nav = ledger.snapshot(
        asof=snapshot.built_at,
        observed_account_nlv=observed_account_nlv,
    )
    if not isinstance(nav, StrategyNavSnapshot) or not nav.valid:
        raise ValueError("signed Strategy NAV is unavailable")
    strategy_nav = nav.strategy_nav
    if (
        not isinstance(strategy_nav, Decimal)
        or not strategy_nav.is_finite()
        or strategy_nav <= 0
    ):
        raise ValueError("signed Strategy NAV must be finite and positive")
    return strategy_nav


def build_production_composition(
    config: OptionsCopilotConfig,
    *,
    approval_store: ProposalApprovalStore,
    bridge_reader: CodexBridgeStore,
    clock: object | None = None,
    ib_factory: object | None = None,
    news_preselection_store: object | None = None,
    top10_research_pool_store: object | None = None,
    top10_structure_source: object = _DEFAULT_TOP10_DEPENDENCY,
    top10_session_gate: object = _DEFAULT_TOP10_DEPENDENCY,
    top10_instruction_reader: object = _DEFAULT_TOP10_DEPENDENCY,
    feature_source_resolver: object | None = None,
    history_preparation_requested: bool = False,
) -> ProductionComposition:
    """Build every read-only runtime dependency without opening a broker session.

    ``start()`` on the returned lifecycle is the only connector boundary.  A
    missing/stale/tampered pacing approval keeps that boundary closed.
    """

    if config.broker_acquisition_mode != "DIRECT":
        raise ValueError(
            "direct production composition is forbidden in EXTERNAL acquisition mode"
        )

    now = clock if callable(clock) else (lambda: datetime.now(timezone.utc))
    checked_at = now()
    if not isinstance(checked_at, datetime):
        raise TypeError("production composition clock must return datetime")
    checked_at = checked_at.astimezone(timezone.utc)
    pacing_guard = PacingAuthorityGuard(
        config.pacing_authority_dir,
        expected_actor=PACING_EXPECTED_ACTOR,
        clock=now,
        signature_verifier=load_pacing_authority_verifier(
            config.pacing_authority_keyring_path
        ),
    )
    pacing = GuardedRequestBudget(pacing_guard, now=checked_at)
    gateway_ref: dict[str, IBKRReadOnlyGateway] = {}
    creator_reader = (
        None
        if top10_instruction_reader is _DEFAULT_TOP10_DEPENDENCY
        or top10_instruction_reader is None
        else top10_instruction_reader
    )
    if creator_reader is not None and not callable(creator_reader):
        raise TypeError("top10_instruction_reader must be callable or None")
    instruction_state_reader = InstructionStateReader(
        bridge_reader,
        broker_working_order_reader=lambda: gateway_ref["gateway"].working_orders(),
        creator_state_reader=creator_reader,  # type: ignore[arg-type]
        creator_transport_enabled=(
            top10_instruction_reader is not _DEFAULT_TOP10_DEPENDENCY
        ),
    )
    gateway = IBKRReadOnlyGateway(
        config,
        ib_factory=ib_factory if callable(ib_factory) else None,
        historical_request_lease_factory=lambda: pacing.lease("historical"),
        historical_request_max_concurrency=pacing.approved_max_concurrency(
            "historical"
        ),
        market_data_request_lease_factory=pacing.lease,
        # The aggregate reader is real even when creator authority is absent;
        # in that state it returns UNKNOWN rather than a fabricated empty list.
        instruction_reader=instruction_state_reader,
        now=now,
    )
    gateway_ref["gateway"] = gateway
    calendar_provider = IBKRSessionCalendarProvider(gateway)
    direct_top10_structure_source = DirectTop10StructureSource(
        gateway,
        pacing,
        clock=now,
        core_symbols=_after_hours_core_symbols(config.news_core_symbols),
        # The 09:20 freeze owns static identity only.  A previous-close stock
        # basis may select strikes for fallback research, while 09:35 still
        # requires one fresh executable AtomicBrokerSnapshot for every leg.
        indicative_underlyings=True,
        # One real vertical consumes two SECDEF requests for optionability,
        # one for the underlying identity, and two for exact option legs.
        # Five structures therefore fit the approved 30-request rolling
        # minute with five requests of headroom; the prior fourteen-structure
        # fallback could never complete under the installed policy.
        maximum_optionability_attempts=5,
        maximum_optionable=5,
        maximum_structures=5,
    )
    top10_account_state_reader = _LifecycleControlAccountStateReader(clock=now)
    effective_top10_structure_source = (
        (
            DurableOptionPoolTop10StructureSource(
                top10_research_pool_store,
                fallback=direct_top10_structure_source,
                # The 09:35 AtomicBrokerSnapshot reads every contract
                # definition before and after its quote batch.  Fourteen
                # unique legs consume 28 of the fixed 30 SECDEF requests and
                # preserve two requests of headroom without changing policy.
                maximum_snapshot_contracts=14,
            )
            if top10_research_pool_store is not None
            else direct_top10_structure_source
        )
        if top10_structure_source is _DEFAULT_TOP10_DEPENDENCY
        else top10_structure_source
    )
    effective_top10_session_gate = (
        BrokerTradingSessionGate(calendar_provider, clock=now)
        if top10_session_gate is _DEFAULT_TOP10_DEPENDENCY
        else top10_session_gate
    )
    effective_top10_instruction_reader = (
        instruction_state_reader
        if top10_instruction_reader is _DEFAULT_TOP10_DEPENDENCY
        else top10_instruction_reader
    )
    owned: list[object] = []
    try:
        nav_ledger = StrategyNavLedger(
            config.data_dir / "strategy_nav.sqlite3",
            contract=STRATEGY_NAV_CONTRACT_PATH,
            clock=now,
        )
        owned.append(nav_ledger)
        execution_cost = SignedExecutionCostResolver(clock=now)
        evidence_store = EvidenceStore(config.news_evidence_path, clock=now)
        owned.append(evidence_store)
        scan_store = ScanRunStore(config.data_dir / "scan_runs.sqlite3")
        owned.append(scan_store)
        ranking_store = RankingStore(config.data_dir / "rankings.sqlite3")
        owned.append(ranking_store)
        equity_evidence_cache = UnderlyingEvidenceCache(
            config.data_dir / "underlying_equity_evidence.sqlite3"
        )
        owned.append(equity_evidence_cache)

        raw_broker_builder = BrokerSnapshotBuilder(gateway, clock=now)
        broker_batch_lock = threading.RLock()
        broker_builder = SerializedBrokerSnapshotProvider(
            raw_broker_builder,
            batch_lock=broker_batch_lock,
        )
        top10_snapshot_provider = ProductionTop10SnapshotProvider(
            broker_builder,
            pacing,
            batch_lock=broker_batch_lock,
        )
        top10_dependencies_ready = _top10_dependencies_ready(
            store=news_preselection_store,
            structure_source=effective_top10_structure_source,
            session_gate=effective_top10_session_gate,
            instruction_reader=effective_top10_instruction_reader,
        )
        top10_producer = (
            Top10PreselectionProducer(
                account_state_reader=top10_account_state_reader,
                structure_source=effective_top10_structure_source,  # type: ignore[arg-type]
                snapshot_provider=top10_snapshot_provider,
                store=news_preselection_store,  # type: ignore[arg-type]
                clock=now,
                session_gate=effective_top10_session_gate,  # type: ignore[arg-type]
                # The 09:20 stage freezes account state and exact contract
                # identities.  US options have not opened yet, so executable
                # quotes belong exclusively to the 09:35 atomic reprice.
                premarket_account_only=True,
                open_economics_resolver=OpenRepriceEconomicsResolver(
                    execution_cost
                ),
                strategy_nav_reader=lambda snapshot: _top10_strategy_nav(
                    nav_ledger,
                    snapshot,
                ),
            )
            if top10_dependencies_ready
            else None
        )
        top10_blocker = (
            None if top10_producer is not None else TOP10_PRODUCER_UNAVAILABLE
        )
        top10_scheduler_service = Top10SchedulerService(
            scan_store,
            top10_producer,
            pipeline_version=TOP10_PRODUCER_PIPELINE_VERSION,
            unavailable_reason=TOP10_PRODUCER_UNAVAILABLE,
        )
        position_manager = PositionManager()
        cost_contract = load_contract(execution_cost.contract_path).to_dict()
        coverage_store = EvidenceStore(config.data_dir / "ordinary_coverage.sqlite3", clock=now)
        owned.append(coverage_store)
        pipeline_inputs = ProductionPipelineInputs(
            gateway,
            pacing,
            position_manager,
            core_symbols=config.news_core_symbols,
            execution_cost_contract=cost_contract,
            ordinary_coverage=OrdinaryScanCoverage(coverage_store),
            clock=now,
        )
        universe_funnel = UniverseFunnel(pacing)  # type: ignore[arg-type]
        options_evidence = ProductionOptionsEvidenceAcquisition()
        policy_resolver = CurrentPolicyResolver(
            config.data_dir / "governance" / "policy_authority.sqlite3",
            # The frozen initial NORMAL policy is immutable and carries no
            # signed expiry.  Applying the resolver's optional 30-day fixture
            # TTL here would manufacture an unsigned production authority
            # transition.  Hash/source/current-head checks remain mandatory,
            # while promotion and A-grade still require genuine signatures.
            maximum_initial_age=None,
        )
        owned.append(policy_resolver)
        def management_market_data_gate() -> tuple[str, ...]:
            # The BrokerSnapshotBuilder leases every actual SECDEF and quote
            # read.  This preflight checks authority without double-charging.
            return () if pacing.ready else (PACING_CAPABILITY_MISSING,)

        def current_management_exit_contract(
            snapshot: AtomicBrokerSnapshot,
        ) -> Mapping[str, object] | None:
            """Bind conservative review rules to the exact open combination.

            This document carries no instruction or order authority.  Thesis
            invalidation stays explicitly unevaluated until a signed
            machine-readable thesis predicate exists; quote and time rules are
            evaluated by the generic manager from the same atomic snapshot.
            """

            positions = snapshot.state_evidence.get("positions")
            raw_state = None if positions is None else positions.state
            expirations: set[object] = set()
            if isinstance(raw_state, tuple):
                for row in raw_state:
                    if not isinstance(row, Mapping):
                        return None
                    quantity = row.get("quantity", row.get("position"))
                    if quantity in {0, Decimal("0"), "0"}:
                        continue
                    expirations.add(row.get("expiration", row.get("expiry")))
            if len(expirations) != 1:
                return None
            expiration = next(iter(expirations))
            if isinstance(expiration, str):
                try:
                    expiration = datetime.fromisoformat(expiration).date()
                except ValueError:
                    return None
            if not hasattr(expiration, "isoformat"):
                return None
            expiration_date = expiration
            cursor = expiration_date
            remaining = 2
            while remaining > 0:
                cursor -= timedelta(days=1)
                if cursor.weekday() < 5:
                    remaining -= 1
            maximum_holding_date = max(snapshot.built_at.date(), cursor)
            return {
                "thesis_invalidation": (
                    "NOT_EVALUATED until a signed point-in-time thesis "
                    "predicate is available; human review remains required"
                ),
                "risk_stop": (
                    "review the whole-combination executable close cashflow "
                    "at 60% of the exact entry debit when entry evidence exists"
                ),
                "profit_take": (
                    "review the whole-combination executable close cashflow "
                    "at 60% of exact maximum profit when entry evidence exists"
                ),
                "time_stop": "review exit no later than two weekdays before expiry",
                "maximum_holding_date": maximum_holding_date.isoformat(),
                "bad_quote_action": (
                    "NO_TRADE and OBSERVATION_ONLY until a fresh coherent "
                    "whole-combination quote batch exists"
                ),
                # These hashes bind the deterministic local date rules.  They
                # do not claim an external dividend/calendar authority, so
                # REDUCE_RISK stays suppressed; CLOSE_ALL remains available.
                "expiration_calendar_hash": None,
                "ex_dividend_calendar_hash": None,
                "early_exercise_risk": "UNKNOWN",
                "short_leg_exit_deadline": maximum_holding_date.isoformat(),
            }

        management_coordinator = ProductionManagementCoordinator(
            gateway,
            exit_contract_provider=current_management_exit_contract,
            cost_contract_provider=lambda _snapshot: cost_contract,
            position_manager=position_manager,
            snapshot_builder=broker_builder,
            market_data_gate=management_market_data_gate,
            clock=now,
        )
        pipeline_inputs.bind_management_refresher(
            management_coordinator.refresh
        )
        broker_evidence = ProductionBrokerEvidenceAcquisition(
            broker_builder,
            options_evidence,
            evidence_store,
            pacing,
            nav_ledger,
            execution_cost_contract=cost_contract,
            policy_resolver=policy_resolver,
            feature_source_resolver=feature_source_resolver,
            clock=now,
        )
        strategy_registry = StrategyTemplateRegistry()
        strategy_generator = StrategyCandidateGenerator(strategy_registry)
        volatility_engine = VolatilityEngine()
        scenario_engine = ScenarioEngine(policy_resolver)
        if not isinstance(nav_ledger.contract_hash, str):
            raise RuntimeError("Strategy NAV contract authority is unavailable")
        risk_resolver = CurrentRiskAuthorityResolver(
            nav_ledger.contract_hash,
            marker_source=PolicyLedgerRiskAuthorityMarkerSource(
                policy_resolver.ledger,
            ),
            clock=now,
        )
        risk_gate = ProductionRiskGate()
        dte_gate = ProductionDteGate()
        single_gate = ProductionSingleCombinationGate()
        eligibility_gate = ProductionEligibilityGate(
            risk_gate,
            dte_gate,
            single_gate,
        )
        portfolio_ranker = PortfolioRanker()
        pipeline = DecisionPipeline(
            inputs=pipeline_inputs,
            universe_funnel=universe_funnel,
            broker_evidence=broker_evidence,
            strategy_registry=strategy_registry,
            strategy_generator=strategy_generator,
            volatility_engine=volatility_engine,
            scenario_engine=scenario_engine,
            policy_resolver=policy_resolver,
            risk_authority_resolver=risk_resolver,
            cost_contract=execution_cost,
            eligibility_gate=eligibility_gate,
            portfolio_ranker=portfolio_ranker,
            ranking_store=ranking_store,
            joint_ranking_required=True,
            clock=now,
        )
        services = RuntimeServices(
            broker_snapshot_builder=broker_builder,
            evidence_store=evidence_store,
            scan_run_store=scan_store,
            pipeline_inputs=pipeline_inputs,
            universe_funnel=universe_funnel,
            broker_evidence_acquisition=broker_evidence,
            options_evidence_acquisition=options_evidence,
            strategy_registry=strategy_registry,
            strategy_candidate_generator=strategy_generator,
            volatility_engine=volatility_engine,
            scenario_engine=scenario_engine,
            policy_resolver=policy_resolver,
            risk_authority_resolver=risk_resolver,
            execution_cost_contract=execution_cost,
            eligibility_gate=eligibility_gate,
            risk_gate=risk_gate,
            dte_gate=dte_gate,
            single_combination_gate=single_gate,
            portfolio_ranker=portfolio_ranker,
            ranking_store=ranking_store,
            decision_pipeline=pipeline,
            strategy_nav_source=nav_ledger,
            position_manager=position_manager,
            approval_store=approval_store,
            bridge_status_reader=bridge_reader,
            bridge_reconciliation_reader=bridge_reader,
            readiness_guard=pacing_guard,
            approval_blockers=(creator_unavailable_reason(),),
            equity_evidence_cache=equity_evidence_cache,
            # This graph has executable analytical ports but no production
            # history/features binding. Report the gap without preventing the
            # read-only scan from acquiring evidence; its existing model gates
            # continue to reject missing D/V inputs.
            feature_data_chain_reasons=tuple(
                "FEATURE_HISTORY_SCHEDULED_OBSERVATIONS_NOT_MODEL_AUTHORITY"
                if reason == "FEATURE_HISTORY_PRODUCER_UNWIRED" and history_preparation_requested
                else reason
                for reason in _UNWIRED_PRODUCTION_FEATURE_REASONS
            ),
        )
        scanner_service = ScanSchedulerService(
            scan_store,
            services,
            pipeline_version=PRODUCTION_PIPELINE_VERSION,
        )
        scanner_loop = ScanSchedulerLoop(
            scanner_service,
            calendar_provider,
            clock=now,
            top10_service=top10_scheduler_service,
        )
        news_adapter = IBKRNewsResearchAdapter(
            gateway,
            ranking_store,
            pacing,
            clock=now,
            batch_lock=broker_batch_lock,
        )
        outcome_market_adapter = ProductionOutcomeMarketAdapter(
            gateway,
            broker_builder,
            pacing,
            batch_lock=broker_batch_lock,
        )
        lifecycle = ProductionLifecycle(
            gateway=gateway,
            scanner_loop=scanner_loop,
            stores=tuple(owned),
            pacing_guard=pacing_guard,
            management_refresher=management_coordinator.refresh,
        )
        top10_account_state_reader.bind(lifecycle.control_snapshot)
        return ProductionComposition(
            services=services,
            lifecycle=lifecycle,
            gateway=gateway,
            calendar_provider=calendar_provider,
            news_adapter=news_adapter,
            pipeline_inputs=pipeline_inputs,
            management_coordinator=management_coordinator,
            pacing_guard=pacing_guard,
            outcome_market_adapter=outcome_market_adapter,
            top10_preselection_producer=top10_producer,
            top10_scheduler_service=top10_scheduler_service,
            top10_producer_blocker=top10_blocker,
            execution_cost_contract=cost_contract,
        )
    except BaseException:
        gateway.disconnect()
        for resource in reversed(owned):
            close = getattr(resource, "close", None)
            if callable(close):
                close()
        raise


_PROVIDER_SECRET_NAMES = {
    "jin10_mcp_token": "JIN10_MCP_TOKEN",
    "finnhub_api_key": "FINNHUB_API_KEY",
    "alpha_vantage_api_key": "ALPHA_VANTAGE_API_KEY",
    "deepseek_api_key": "DEEPSEEK_API_KEY",
}


def _after_hours_identity(payload: Mapping[str, object]) -> tuple[str, ...]:
    rows = payload.get("candidates")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return ()
    identities: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return ()
        try:
            identity = after_hours_candidate_cache_identity(row)
        except (TypeError, ValueError):
            return ()
        identities.append(identity)
    return tuple(identities)


def _after_hours_formal_candidate_identity_manifest(
    payload: Mapping[str, object],
    *,
    research_symbols: Sequence[object],
) -> tuple[str, ...] | None:
    """Return exact identities bound to the current equity discovery set."""

    raw_manifest = after_hours_candidate_identity_manifest(payload)
    rows = payload.get("candidates")
    if raw_manifest is None or not isinstance(rows, Sequence) or isinstance(
        rows,
        (str, bytes, bytearray),
    ):
        return None
    bounded_rows = tuple(rows[:10])
    if len(bounded_rows) != len(raw_manifest):
        return None
    research = {
        str(symbol).strip().upper()
        for symbol in research_symbols
        if str(symbol).strip()
    }
    identities: list[str] = []
    for row, identity in zip(bounded_rows, raw_manifest, strict=True):
        if not isinstance(row, Mapping):
            return None
        if str(row.get("underlying", "")).strip().upper() in research:
            identities.append(identity)
    return tuple(identities)


def _should_replace_after_hours_best(
    latest: Mapping[str, object],
    best: Mapping[str, object] | None,
    *,
    now: datetime,
) -> bool:
    """Retain completeness only for the same exact canonical identities."""

    if best is None:
        return True
    raw_reasons = latest.get("reason_codes")
    latest_reasons = {
        str(item).strip().upper()
        for item in (
            raw_reasons
            if isinstance(raw_reasons, Sequence)
            and not isinstance(raw_reasons, (str, bytes, bytearray))
            else ()
        )
    }
    if latest_reasons & {
        "RESEARCH_LEG_RATIO_INVALID",
        "RESEARCH_CONTRACT_IDENTITY_INVALID",
        "RESEARCH_CONTRACT_IDENTITY_DUPLICATE",
    }:
        return True
    latest_priced = int(latest.get("priced_count", 0) or 0)
    best_priced = int(best.get("priced_count", 0) or 0)
    latest_identity = _after_hours_identity(latest)
    best_identity = _after_hours_identity(best)
    return (
        latest_priced >= best_priced
        or (bool(latest_identity) and latest_identity != best_identity)
        or not _after_hours_cache_fresh(best, now=now)
    )


def _after_hours_cache_fresh(
    payload: Mapping[str, object],
    *,
    now: datetime,
) -> bool:
    raw = payload.get("observed_at")
    if not isinstance(raw, str) or not raw.strip():
        return False
    try:
        observed_at = datetime.fromisoformat(raw.strip())
    except ValueError:
        return False
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        return False
    age = now.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)
    return timedelta(0) <= age <= timedelta(hours=18)


def _project_after_hours_passive_freshness(
    payload: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Re-evaluate cached closing marks without making a broker request."""

    result = dict(payload)
    raw_observed_at = payload.get("observed_at")
    observed_at: datetime | None = None
    if isinstance(raw_observed_at, str) and raw_observed_at.strip():
        try:
            parsed = datetime.fromisoformat(raw_observed_at.strip())
        except ValueError:
            parsed = None
        if (
            parsed is not None
            and parsed.tzinfo is not None
            and parsed.utcoffset() is not None
        ):
            observed_at = parsed
    current = now.astimezone(timezone.utc)
    age = (
        None
        if observed_at is None
        else current - observed_at.astimezone(timezone.utc)
    )
    if age is not None and timedelta(0) <= age <= timedelta(minutes=15):
        return result

    raw_reasons = payload.get("reason_codes")
    reasons = (
        tuple(str(item) for item in raw_reasons)
        if isinstance(raw_reasons, Sequence)
        and not isinstance(raw_reasons, (str, bytes, bytearray))
        else ()
    )
    raw_candidates = payload.get("candidates")
    candidates: list[object] = []
    if isinstance(raw_candidates, Sequence) and not isinstance(
        raw_candidates,
        (str, bytes, bytearray),
    ):
        for raw_candidate in raw_candidates:
            if not isinstance(raw_candidate, Mapping):
                candidates.append(raw_candidate)
                continue
            candidate = dict(raw_candidate)
            pricing_available = (
                str(candidate.get("pricing_status", "")).strip().upper()
                == "AVAILABLE"
            )
            mark_evidence_available = (
                str(candidate.get("mark_evidence_status", "")).strip().upper()
                == "AVAILABLE"
            )
            if pricing_available or mark_evidence_available:
                if pricing_available:
                    candidate["pricing_status"] = "STALE"
                candidate["freshness_status"] = "STALE"
                candidate["mark_evidence_status"] = "STALE"
                raw_blockers = candidate.get("blockers")
                blockers = (
                    tuple(str(item) for item in raw_blockers)
                    if isinstance(raw_blockers, Sequence)
                    and not isinstance(raw_blockers, (str, bytes, bytearray))
                    else ()
                )
                candidate["blockers"] = list(
                    dict.fromkeys((*blockers, "AFTER_HOURS_CACHED_MARKS_STALE"))
                )
            candidate.update(
                {
                    "trade_status": "NO_TRADE",
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                }
            )
            candidates.append(candidate)
    result.update(
        {
            "status": "DEGRADED",
            "freshness_status": "STALE",
            "decision": "NO_TRADE",
            "reason_codes": list(
                dict.fromkeys((*reasons, "AFTER_HOURS_CACHED_MARKS_STALE"))
            ),
            "candidates": candidates,
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
            "review_only": True,
            "direct_order_submission": False,
        }
    )
    return result


def _after_hours_research_row_from_cached(
    row: Mapping[str, object],
) -> dict[str, object] | None:
    """Recover exact research identity from an older display-only cache."""

    symbol = str(row.get("underlying", "")).strip().upper()
    raw_legs = row.get("legs")
    if not symbol or not isinstance(raw_legs, Sequence) or isinstance(
        raw_legs,
        (str, bytes, bytearray),
    ):
        return None
    legs: list[dict[str, object]] = []
    for raw in raw_legs:
        if not isinstance(raw, Mapping):
            return None
        try:
            contract_id = int(raw.get("contract_id"))
            ratio = after_hours_leg_ratio(raw)
        except (TypeError, ValueError):
            return None
        if contract_id <= 0:
            return None
        exchange = str(raw.get("exchange") or "SMART").strip().upper()
        trading_class = str(raw.get("trading_class") or symbol).strip().upper()
        legs.append(
            {
                "side": str(raw.get("side") or "").strip().upper(),
                "contract_id": contract_id,
                "contract_id_ex": str(
                    raw.get("contract_id_ex") or f"{contract_id}@{exchange}"
                ),
                "local_symbol": raw.get("local_symbol"),
                "expiration": raw.get("expiration"),
                "strike": raw.get("strike"),
                "right": raw.get("right"),
                "exchange": exchange,
                "trading_class": trading_class,
                "multiplier": int(raw.get("multiplier") or 100),
                "ratio": ratio,
            }
        )
    if len(legs) != 2:
        return None
    return {
        key: value
        for key, value in row.items()
        if key
        not in {
            "pricing_status",
            "indicative_entry_debit_usd",
            "indicative_maximum_loss_usd",
            "strategy_nav_fraction",
            "indicative_price_basis",
            "blockers",
            "trade_status",
            "decision_authority",
            "approval_eligible",
            "instruction_creation_allowed",
            "order_allowed",
        }
    } | {"legs": legs}


_AFTER_HOURS_DIVERSIFIED_CORE = (
    "SPY",
    "XLF",
    "XLE",
    "XLV",
    "XLI",
    "XLP",
    "XLU",
    "GLD",
    "TLT",
    "IWM",
    "QQQ",
    "SMH",
)

_AFTER_HOURS_CORE_SECTORS = {
    "SPY": "BROAD_MARKET",
    "XLF": "FINANCIALS",
    "XLE": "ENERGY",
    "XLV": "HEALTH_CARE",
    "XLI": "INDUSTRIALS",
    "XLP": "CONSUMER_STAPLES",
    "XLU": "UTILITIES",
    "GLD": "PRECIOUS_METALS",
    "TLT": "US_TREASURIES",
    "IWM": "US_SMALL_CAP",
    "QQQ": "LARGE_CAP_GROWTH",
    "SMH": "SEMICONDUCTORS",
}


class _AfterHoursFormalMaterializationConflict(RuntimeError):
    """A deterministic research-pool slot already has different lineage."""


def _after_hours_formal_conflict_payload(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Remove unproved formal lineage and expose the append-only conflict."""

    degraded = dict(payload)
    degraded.pop("formal_research_pools", None)
    raw_reasons = payload.get("reason_codes")
    reasons = (
        tuple(raw_reasons)
        if isinstance(raw_reasons, Sequence)
        and not isinstance(raw_reasons, (str, bytes, bytearray))
        else ()
    )
    degraded.update(
        {
            "status": "DEGRADED",
            "reason_codes": list(
                dict.fromkeys(
                    (*reasons, "AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT")
                )
            ),
            "decision": "NO_TRADE",
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }
    )
    return degraded


def _after_hours_with_verified_formal(
    payload: Mapping[str, object],
    formal: Mapping[str, object],
) -> dict[str, object]:
    """Attach verified lineage and clear only the superseded conflict state."""

    raw_reasons = payload.get("reason_codes")
    reasons = (
        tuple(raw_reasons)
        if isinstance(raw_reasons, Sequence)
        and not isinstance(raw_reasons, (str, bytes, bytearray))
        else ()
    )
    return {
        **payload,
        "formal_research_pools": dict(formal),
        "reason_codes": list(
            dict.fromkeys(
                reason
                for reason in reasons
                if reason != "AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT"
            )
        ),
    }


def _after_hours_core_symbols(configured: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*_AFTER_HOURS_DIVERSIFIED_CORE, *configured)))


@dataclass(frozen=True, slots=True)
class _AfterHoursUnderlyingQuote:
    symbol: str
    contract_id: int
    exchange: str
    source: str
    observed_at: datetime
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    close: Decimal
    market_data_type: int

    def authority_body(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.indicative_underlying_quote_basis.v1",
            "symbol": self.symbol,
            "contract_id": self.contract_id,
            "exchange": self.exchange,
            "source": self.source,
            "observed_at": self.observed_at,
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "close": self.close,
            "market_data_type": self.market_data_type,
            "decision_authority": "SUPPORTING_ONLY",
        }

    def cache_projection(self) -> dict[str, object]:
        return {
            **self.authority_body(),
            "observed_at": self.observed_at.isoformat(),
            "bid": None if self.bid is None else format(self.bid, "f"),
            "ask": None if self.ask is None else format(self.ask, "f"),
            "last": None if self.last is None else format(self.last, "f"),
            "close": format(self.close, "f"),
        }


def _after_hours_underlying_quote(
    value: object,
    digest: object,
) -> _AfterHoursUnderlyingQuote | None:
    if not isinstance(value, Mapping) or not isinstance(digest, str):
        return None
    try:
        observed_raw = value.get("observed_at")
        observed = (
            observed_raw
            if isinstance(observed_raw, datetime)
            else datetime.fromisoformat(str(observed_raw).replace("Z", "+00:00"))
        )
        if observed.tzinfo is None or observed.utcoffset() is None:
            return None
        contract_id = value.get("contract_id")
        market_data_type = value.get("market_data_type")
        if (
            isinstance(contract_id, bool)
            or not isinstance(contract_id, int)
            or contract_id <= 0
            or isinstance(market_data_type, bool)
            or not isinstance(market_data_type, int)
        ):
            return None

        def decimal(name: str, *, required: bool = False) -> Decimal | None:
            raw = value.get(name)
            if raw is None and not required:
                return None
            parsed = raw if isinstance(raw, Decimal) else Decimal(str(raw))
            if not parsed.is_finite() or parsed <= 0:
                raise ValueError(name)
            return parsed

        quote = _AfterHoursUnderlyingQuote(
            symbol=str(value.get("symbol", "")).strip().upper(),
            contract_id=contract_id,
            exchange=str(value.get("exchange", "")).strip().upper(),
            source=str(value.get("source", "")).strip().upper(),
            observed_at=observed.astimezone(timezone.utc),
            bid=decimal("bid"),
            ask=decimal("ask"),
            last=decimal("last"),
            close=decimal("close", required=True),
            market_data_type=market_data_type,
        )
    except (ArithmeticError, TypeError, ValueError):
        return None
    if (
        not quote.symbol
        or not quote.exchange
        or not quote.source
        or quote.close is None
        or (quote.bid is None) != (quote.ask is None)
        or (
            quote.bid is not None
            and quote.ask is not None
            and quote.ask < quote.bid
        )
        or canonical_hash(quote.authority_body()) != digest
    ):
        return None
    return quote


def _after_hours_campaign_hash(payload: Mapping[str, object]) -> str:
    return after_hours_campaign_lineage_hash(payload)


def _after_hours_materialization_revision_hash(
    payload: Mapping[str, object],
    *,
    campaign_observed_at: datetime,
) -> str:
    return canonical_hash(
        {
            "schema": "options_copilot.after_hours_materialization_revision.v5",
            "campaign_hash": _after_hours_campaign_hash(payload),
            "campaign_observed_at": campaign_observed_at,
        }
    )


def _after_hours_materialization_slot(
    campaign_observed_at: datetime,
    revision_hash: str,
) -> datetime:
    """Return the immutable ledger slot owned by one formal revision."""

    offset = 1 + (int(revision_hash[:12], 16) % 999_999)
    return campaign_observed_at + timedelta(microseconds=offset)


def _after_hours_formal_pools_projection(
    *,
    campaign_hash: str,
    revision_hash: str,
    campaign_observed_at: datetime,
    materialization_slot: datetime,
    equity_pool_id: object,
    equity_pool_hash: object,
    equity_pool_reference: Mapping[str, object],
    equity_research_count: int,
    equity_selected_count: int,
    option_pool_scan_run_id: str,
    option_pool_hash: str,
    option_candidate_identities: Sequence[str],
    underlying_basis_bound_count: int,
    underlying_basis_missing_count: int,
    migration_reasons: Sequence[str],
) -> dict[str, object]:
    """Build one authority-free descriptor from already verified stores."""

    descriptor = {
        "schema": "options_copilot.after_hours_formal_pool_descriptor.v2",
        "campaign_hash": campaign_hash,
        "materialization_revision_hash": revision_hash,
        "campaign_observed_at": campaign_observed_at.isoformat(),
        "materialized_at": materialization_slot.isoformat(),
        "equity_pool_hash": equity_pool_hash,
        "equity_pool_reference_hash": canonical_hash(equity_pool_reference),
        "equity_research_count": equity_research_count,
        "equity_selected_count": equity_selected_count,
        "option_pool_hash": option_pool_hash,
        "option_pool_scan_run_id": option_pool_scan_run_id,
        "option_structure_count": len(option_candidate_identities),
        "option_candidate_identity_hash": canonical_hash(
            tuple(option_candidate_identities)
        ),
    }
    return {
        "schema": "options_copilot.after_hours_formal_pools.v2",
        "status": "READY",
        "migration_status": (
            "COMPLETE" if underlying_basis_missing_count == 0 else "PENDING"
        ),
        "migration_reason_codes": tuple(migration_reasons),
        "campaign_hash": campaign_hash,
        "materialization_revision_hash": revision_hash,
        "campaign_observed_at": campaign_observed_at.isoformat(),
        "materialized_at": materialization_slot.isoformat(),
        "equity_pool_id": equity_pool_id,
        "equity_pool_hash": equity_pool_hash,
        "equity_pool_reference_hash": canonical_hash(equity_pool_reference),
        "equity_research_count": equity_research_count,
        "underlying_basis_bound_count": underlying_basis_bound_count,
        "underlying_basis_missing_count": underlying_basis_missing_count,
        "equity_selected_count": equity_selected_count,
        "option_pool_scan_run_id": option_pool_scan_run_id,
        "option_pool_hash": option_pool_hash,
        "option_structure_count": len(option_candidate_identities),
        "descriptor": descriptor,
        "descriptor_hash": canonical_hash(descriptor),
        "decision": "NO_TRADE",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _after_hours_campaign_completed(payload: Mapping[str, object]) -> bool:
    return after_hours_campaign_progress_status(payload) == "COMPLETE"


def _after_hours_formal_descriptor_valid(
    formal: object,
    *,
    payload: Mapping[str, object],
    equity: Mapping[str, object],
    option: Mapping[str, object],
) -> bool:
    if after_hours_campaign_progress_status(payload) is None:
        return False
    if not isinstance(formal, Mapping):
        return False
    if formal.get("schema") != "options_copilot.after_hours_formal_pools.v2":
        return False
    descriptor = formal.get("descriptor")
    if (
        not isinstance(descriptor, Mapping)
        or descriptor.get("schema")
        != "options_copilot.after_hours_formal_pool_descriptor.v2"
        or formal.get("descriptor_hash") != canonical_hash(descriptor)
        or formal.get("campaign_hash") != _after_hours_campaign_hash(payload)
        or descriptor.get("campaign_hash") != formal.get("campaign_hash")
    ):
        return False
    fields = (
        "materialization_revision_hash",
        "campaign_observed_at",
        "materialized_at",
        "equity_pool_hash",
        "equity_pool_reference_hash",
        "equity_research_count",
        "equity_selected_count",
        "option_pool_hash",
        "option_pool_scan_run_id",
        "option_structure_count",
    )
    if any(descriptor.get(field) != formal.get(field) for field in fields):
        return False
    try:
        campaign_observed_at = datetime.fromisoformat(
            str(formal.get("campaign_observed_at", "")).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        materialized_at = datetime.fromisoformat(
            str(formal.get("materialized_at", "")).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return False
    if (
        formal.get("materialization_revision_hash")
        != _after_hours_materialization_revision_hash(
            payload,
            campaign_observed_at=campaign_observed_at,
        )
        or materialized_at
        != _after_hours_materialization_slot(
            campaign_observed_at,
            str(formal.get("materialization_revision_hash", "")),
        )
    ):
        return False
    option_decisions = option.get("decisions")
    if not isinstance(option_decisions, Sequence) or isinstance(
        option_decisions,
        (str, bytes, bytearray),
    ):
        return False
    candidate_identities = tuple(
        decision.get("candidate_identity")
        for decision in option_decisions
        if isinstance(decision, Mapping)
    )
    equity_reference = equity.get("equity_pool_reference")
    research_symbols = (
        equity_reference.get("discovered_symbols", ())
        if isinstance(equity_reference, Mapping)
        else ()
    )
    payload_identities = _after_hours_formal_candidate_identity_manifest(
        payload,
        research_symbols=(
            research_symbols
            if isinstance(research_symbols, Sequence)
            and not isinstance(research_symbols, (str, bytes, bytearray))
            else ()
        ),
    )
    return bool(
        equity.get("snapshot_hash") == formal.get("equity_pool_hash")
        and equity.get("discovery_count") == formal.get("equity_research_count")
        and equity.get("selected_count") == formal.get("equity_selected_count")
        and option.get("snapshot_hash") == formal.get("option_pool_hash")
        and option.get("scan_run_id") == formal.get("option_pool_scan_run_id")
        and len(option_decisions) == formal.get("option_structure_count")
        and canonical_hash(candidate_identities)
        == descriptor.get("option_candidate_identity_hash")
        and payload_identities is not None
        and payload_identities == candidate_identities
        and all(identity is not None for identity in candidate_identities)
    )


def _after_hours_missing_basis_symbols(
    rows: object,
) -> tuple[str, ...]:
    if not isinstance(rows, Sequence) or isinstance(
        rows, (str, bytes, bytearray)
    ):
        return ()
    missing: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("underlying", "")).strip().upper()
        if not symbol:
            continue
        if _after_hours_underlying_quote(
            row.get("underlying_quote_basis"),
            row.get("underlying_quote_basis_hash"),
        ) is None:
            missing.append(symbol)
    return tuple(dict.fromkeys(missing))


def _merge_after_hours_underlying_basis(
    retained: Mapping[str, object],
    latest: Mapping[str, object],
) -> dict[str, object]:
    """Merge only verified underlying basis into the more complete price cache."""

    latest_rows = latest.get("candidates")
    by_symbol = {
        str(row.get("underlying", "")).strip().upper(): row
        for row in (
            latest_rows
            if isinstance(latest_rows, Sequence)
            and not isinstance(latest_rows, (str, bytes, bytearray))
            else ()
        )
        if isinstance(row, Mapping)
        and _after_hours_underlying_quote(
            row.get("underlying_quote_basis"),
            row.get("underlying_quote_basis_hash"),
        )
        is not None
    }
    retained_rows = retained.get("candidates")
    merged_rows: list[dict[str, object]] = []
    for row in (
        retained_rows
        if isinstance(retained_rows, Sequence)
        and not isinstance(retained_rows, (str, bytes, bytearray))
        else ()
    ):
        if not isinstance(row, Mapping):
            continue
        merged = dict(row)
        source = by_symbol.get(str(row.get("underlying", "")).strip().upper())
        if source is not None:
            merged["underlying_quote_basis"] = source["underlying_quote_basis"]
            merged["underlying_quote_basis_hash"] = source[
                "underlying_quote_basis_hash"
            ]
        merged_rows.append(merged)
    return {
        **retained,
        "candidates": merged_rows,
        "campaign": latest.get("campaign", retained.get("campaign")),
        "pacing_usage": latest.get("pacing_usage", retained.get("pacing_usage")),
        "discovery_mode": latest.get(
            "discovery_mode", retained.get("discovery_mode")
        ),
        "discovery_reason_codes": latest.get(
            "discovery_reason_codes", retained.get("discovery_reason_codes", ())
        ),
    }


def _after_hours_pacing_usage(composition: object) -> dict[str, object]:
    pipeline_inputs = getattr(composition, "pipeline_inputs", None)
    pacing = getattr(pipeline_inputs, "pacing", None)
    capability_hash = str(getattr(pacing, "capability_hash", "") or "").lower()
    return {
        "schema": "options_copilot.after_hours_pacing_usage.v1",
        "status": "APPROVED" if len(capability_hash) == 64 else "UNAVAILABLE",
        "capability_hash": (
            capability_hash
            if len(capability_hash) == 64
            and all(character in "0123456789abcdef" for character in capability_hash)
            else None
        ),
        "authority": "READ_ONLY_MARKET_DATA",
    }


def _operation_cancelled(
    cancel_event: threading.Event | None,
    deadline_at: datetime | None,
) -> bool:
    if cancel_event is not None and cancel_event.is_set():
        return True
    if deadline_at is None:
        return False
    if deadline_at.tzinfo is None or deadline_at.utcoffset() is None:
        return True
    return datetime.now(timezone.utc) >= deadline_at.astimezone(timezone.utc)


def _operation_commit_guard(
    cancel_event: threading.Event | None,
    deadline_at: datetime | None,
    operation_token: str | None,
) -> Callable[[], bool]:
    token = str(operation_token or "DIRECT_RUNTIME_CALL").strip()

    def allowed() -> bool:
        return bool(token) and not _operation_cancelled(cancel_event, deadline_at)

    return allowed


def _after_hours_option_pool_candidate(
    row: Mapping[str, object],
    *,
    campaign_hash: str,
    observed_at: datetime,
    equity_thesis: Mapping[str, object] | None = None,
    short_leg_risk_evidence: Mapping[str, object] | None = None,
) -> Mapping[str, object] | None:
    candidate_id = str(row.get("research_id", "")).strip()
    identity_payload = after_hours_option_identity_payload(row)
    if not candidate_id or identity_payload is None:
        return None
    symbol = str(identity_payload["symbol"])
    structure = str(identity_payload["structure"])
    raw_identity_legs = identity_payload["legs"]
    assert isinstance(raw_identity_legs, tuple)
    legs = [dict(leg) for leg in raw_identity_legs]
    expirations = [date.fromisoformat(str(leg["expiration"])) for leg in legs]
    source_scan = str(row.get("source_scan", "")).strip().upper()
    payload: dict[str, object] = {
        "candidate_id": candidate_id,
        "symbol": symbol,
        "structure": structure,
        "legs": tuple(legs),
        "dte": max(
            0,
            (
                min(expirations)
                - observed_at.astimezone(timezone.utc).date()
            ).days,
        ),
        "source_scan": source_scan or "MISSING_PROVENANCE",
        "campaign_hash": campaign_hash,
    }
    thesis = None
    try:
        thesis = normalize_equity_thesis_row(
            equity_thesis,
            expected_symbol=symbol,
        )
    except (ArithmeticError, TypeError, ValueError):
        thesis = None
    if thesis is not None:
        thesis_hash = canonical_hash(thesis)
        maximum_holding_date = min(
            min(expirations),
            observed_at.astimezone(timezone.utc).date() + timedelta(days=5),
        )
        payload["equity_thesis_evidence"] = thesis
        payload["equity_thesis_hash"] = thesis_hash
        payload["invalidation_evidence"] = {
            "status": "BOUND",
            "thesis_invalidation": (
                str(row.get("invalidation_condition", "")).strip()
                or "Discard when the hash-bound equity thesis is no longer valid."
            ),
            "maximum_holding_date": maximum_holding_date.isoformat(),
            "equity_thesis_hash": thesis_hash,
        }

    short_legs = tuple(
        leg
        for leg in legs
        if str(leg.get("side", "")).strip().upper() in {"SELL", "SHORT"}
    )
    if not short_legs:
        payload["assignment_evidence"] = {"status": "NOT_APPLICABLE"}
        payload["ex_dividend_evidence"] = {"status": "NOT_APPLICABLE"}
    elif (
        isinstance(short_leg_risk_evidence, Mapping)
        and short_leg_risk_evidence.get("status") == "SUPPORTED"
        and _digest(short_leg_risk_evidence.get("evidence_hash"))
    ):
        proof = dict(short_leg_risk_evidence)
        for leg in short_legs:
            leg["short_leg_risk_evidence"] = proof
        payload["assignment_evidence"] = {
            "status": "SUPPORTED",
            "short_leg_evidence": tuple(proof for _leg in short_legs),
        }
        payload["ex_dividend_evidence"] = {
            "status": "SUPPORTED",
            "short_leg_evidence": tuple(proof for _leg in short_legs),
        }
    basis = _after_hours_underlying_quote(
        row.get("underlying_quote_basis"),
        row.get("underlying_quote_basis_hash"),
    )
    if basis is not None and basis.symbol == symbol:
        payload["underlying_quote_basis"] = basis.cache_projection()
        payload["underlying_quote_basis_hash"] = canonical_hash(
            basis.authority_body()
        )
    return {
        "candidate_hash": canonical_hash(payload),
        "payload": payload,
    }


def _execution_cost_short_leg_risk_evidence(
    value: object,
    *,
    observed_at: datetime,
) -> Mapping[str, object] | None:
    """Project the existing signed cost policy into one bounded short-leg proof."""

    if not isinstance(value, Mapping):
        return None
    try:
        verified = verify_contract(
            value,
            expected_kind=ContractKind.EXECUTION_COST,
            as_of=observed_at,
        )
    except (ContractValidationError, TypeError, ValueError):
        return None
    document = verified.to_dict()
    payload = document.get("payload")
    if not isinstance(payload, Mapping):
        return None
    assignment = payload.get("assignment_exercise_and_dividend")
    if not isinstance(assignment, Mapping):
        return None
    status = str(assignment.get("status", "")).strip().upper()
    evidence_hash = assignment.get("evidence_hash")
    legacy_v1_complete = (
        verified.version == "v1"
        and all(
            isinstance(assignment.get(key), Mapping)
            and bool(assignment.get(key))
            for key in ("assignment", "exercise", "early_exercise", "ex_dividend")
        )
        and isinstance(assignment.get("short_leg_exit_deadline"), str)
        and bool(str(assignment.get("short_leg_exit_deadline", "")).strip())
    )
    if status == "" and legacy_v1_complete:
        status = "SUPPORTED"
        evidence_hash = canonical_hash(assignment)
    if status != "SUPPORTED" or not _digest(evidence_hash):
        return None
    return {
        "status": "SUPPORTED",
        "reason_codes": (),
        "evidence_hash": canonical_hash(
            {
                "execution_cost_contract_hash": verified.contract_hash,
                "assignment_exercise_and_dividend": assignment,
            }
        ),
        "source_contract_hash": verified.contract_hash,
        "source_evidence_hash": evidence_hash,
    }


def _after_hours_research_from_resolution(
    resolution: object,
    *,
    metadata: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    structures = tuple(getattr(resolution, "structures", ()))
    metadata_by_symbol = {
        str(item.get("symbol", "")).strip().upper(): dict(item)
        for item in metadata
        if str(item.get("symbol", "")).strip()
    }
    rows: list[dict[str, object]] = []
    for rank, resolved in enumerate(structures[:10], start=1):
        candidate = getattr(resolved, "candidate", None)
        if candidate is None or not callable(getattr(candidate, "as_dict", None)):
            continue
        raw = candidate.as_dict()
        raw_legs = raw.get("legs")
        if not isinstance(raw_legs, Sequence) or isinstance(
            raw_legs, (str, bytes, bytearray)
        ):
            continue
        legs: list[dict[str, object]] = []
        for item in raw_legs:
            if not isinstance(item, Mapping):
                continue
            raw_right = str(item.get("right", "")).strip().upper()
            right = "C" if raw_right in {"C", "CALL"} else "P" if raw_right in {
                "P",
                "PUT",
            } else raw_right
            legs.append(
                {
                    "side": item.get("side"),
                    "contract_id": item.get("con_id"),
                    "contract_id_ex": (
                        f"{item.get('con_id')}@{item.get('exchange')}"
                    ),
                    "local_symbol": item.get("local_symbol"),
                    "expiration": item.get("expiry"),
                    "strike": item.get("strike"),
                    "right": right,
                    "exchange": item.get("exchange"),
                    "trading_class": item.get("trading_class"),
                    "multiplier": item.get("multiplier"),
                    "ratio": item.get("ratio", 1),
                }
            )
        if not legs:
            continue
        strategy = str(raw.get("strategy_type", "")).strip().upper()
        if not strategy:
            right = str(legs[0].get("right", "")).strip().upper()
            strategy = (
                "LONG_CALL"
                if len(legs) == 1 and right == "C"
                else "LONG_PUT"
                if len(legs) == 1 and right == "P"
                else "BULL_CALL_VERTICAL"
                if len(legs) == 2 and right == "C"
                else "BEAR_PUT_VERTICAL"
                if len(legs) == 2 and right == "P"
                else "UNKNOWN"
            )
        symbol = str(raw.get("underlying", "")).strip().upper()
        source = metadata_by_symbol.get(symbol, {})
        basis = _after_hours_underlying_quote(
            source.get("underlying_quote_basis"),
            source.get("underlying_quote_basis_hash"),
        )
        sector = next(
            (
                str(source.get(name, "")).strip()
                for name in ("industry", "category", "subcategory")
                if str(source.get(name, "")).strip()
            ),
            _AFTER_HOURS_CORE_SECTORS.get(symbol, "UNCLASSIFIED"),
        )
        row = {
                "research_id": raw.get("preselection_id"),
                "rank": rank,
                "underlying": symbol,
                "strategy_type": strategy,
                "quantity": 1,
                "execution_cost_cap_usd": "20.00",
                "sector": sector,
                "source_scan": source.get("source_scan") or "MISSING_PROVENANCE",
                "research_summary": (
                    "Broad IBKR closing scan; exact contract identities and "
                    "last available marks only. Sector, direction, cost, and "
                    "risk are observable; volatility-surface and event Gates "
                    "remain mandatory at the next executable reprice."
                ),
                "entry_condition": (
                    "Wait for the next regular-session AtomicBrokerSnapshot, "
                    "five-second executable leg quotes, complete volatility "
                    "and liquidity evidence, cost-after-EV, NAV, and all Gates."
                ),
                "invalidation_condition": (
                    "Discard if direction, sector thesis, event evidence, "
                    "volatility regime, liquidity, or contract identity changes."
                ),
                "profit_target_condition": (
                    "No profit target is active before an executable reprice."
                ),
                "stop_loss_condition": "No order exists; human control is exclusive.",
                "legs": legs,
            }
        if basis is not None:
            row["underlying_quote_basis"] = basis.cache_projection()
            row["underlying_quote_basis_hash"] = canonical_hash(
                basis.authority_body()
            )
        rows.append(row)
    return {
        "candidates": rows,
        "reason_codes": list(getattr(resolution, "reason_codes", ())),
        "missing_symbols": list(getattr(resolution, "missing_symbols", ())),
        "discovery_mode": "IBKR_BOUNDED_MARKET",
    }


def _after_hours_sector_coverage(value: object) -> dict[str, object]:
    rows = value if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ) else ()
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        sector = str(row.get("sector", "UNCLASSIFIED") or "UNCLASSIFIED").strip()
        counts[sector] = counts.get(sector, 0) + 1
    return {
        "distinct_count": len(counts),
        "counts": counts,
        "concentration_warning": bool(counts) and max(counts.values()) > 3,
    }


def _after_hours_strategy_coverage(value: object) -> dict[str, object]:
    rows = value if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ) else ()
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        strategy = str(row.get("strategy_type", "UNKNOWN") or "UNKNOWN").strip()
        counts[strategy] = counts.get(strategy, 0) + 1
    return {
        "distinct_count": len(counts),
        "counts": counts,
        "single_structure_warning": len(counts) == 1 and bool(counts),
    }


def _provider_value_fingerprints(
    store: LocalApiKeyStore,
) -> dict[str, str | None]:
    """Compare in-memory credential versions without exposing derivatives."""

    result: dict[str, str | None] = {}
    for field, secret_name in _PROVIDER_SECRET_NAMES.items():
        try:
            value = store.get(secret_name)
        except Exception:
            result[field] = "ERROR"
            continue
        result[field] = (
            None
            if value is None
            else hashlib.sha256(
                b"options-copilot-runtime-provider-version\x00"
                + value.encode("utf-8")
            ).hexdigest()
        )
    return result


@dataclass(frozen=True, slots=True)
class ReactionRuntimeOverrides:
    """Constructor-only bounded seams for deterministic reaction acceptance tests."""

    provider_factory: Callable[[ReactionEvidenceStore], ProductionMacroReactionProvider]
    public_calendar_provider: object
    schedule_calendar_provider: object
    clock: Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class _LearningProjectionCache:
    """Small read model bound to one fully verified shadow-ledger head."""

    verified_head_sequence: int
    verified_head_hash: str
    selected_challenger: str | None
    governance_state: GovernanceState
    challengers: tuple[str, ...]
    record_counts: Mapping[str, int]
    outcome_horizons: Mapping[str, object]
    shadow_exclusions: Mapping[str, object]


def _feature_source_diagnostic_failure(
    kind: str,
    symbol: str,
    reason: str,
    *,
    status: str = "UNAVAILABLE",
    request_sent: bool | None = None,
) -> dict[str, object]:
    """Keep failed delivery unknown rather than inventing empty broker data."""

    body = {
        "schema": "options_copilot.feature_source_diagnostic.v1",
        "kind": kind,
        "symbol": symbol,
        "source": "IBKR",
        "status": status,
        "reason_codes": (reason,),
        "request_sent": request_sent,
        "available_at": None,
        "basis_status": "PROVIDER_NATIVE_UNRESOLVED",
        "model_input_complete": False,
        "production_eligible": False,
        "point_in_time_verified": False,
        "decision_authority": "OBSERVATION_ONLY",
        **(
            {"value": None, "received_at": None, "source_event_timestamp": None}
            if kind == "CURRENT_IV"
            else {"bars": None, "received_bar_count": None, "prior_completed_bar_count": None}
        ),
    }
    return {**body, "content_hash": canonical_hash(body)}




class OptionsCopilotRuntime:
    def __init__(
        self,
        config: OptionsCopilotConfig,
        runtime_services: RuntimeServices | None = None,
        *,
        reaction_overrides: ReactionRuntimeOverrides | None = None,
    ) -> None:
        config.validate()
        config.ensure_runtime_directories()
        self.config = config
        self._close_lock = threading.Lock()
        self._closed = False
        self._closing = False
        self._shutdown_health: dict[str, object] = {
            "status": "IDLE",
            "reason": None,
        }
        self._immediate_scan_lock = threading.Lock()
        self._feature_source_diagnostic_lock = threading.Lock()
        self._feature_source_diagnostic_last_attempt: float | None = None
        self._feature_source_diagnostic_cooldown_until: datetime | None = None
        self._feature_source_persistence_lock = threading.RLock()
        self._feature_source_persistence: dict[str, object] = {
            "status": "NOT_RUN", "operation_id": None, "references": {},
            "reason_codes": ("FEATURE_SOURCE_INGESTION_NOT_RUN",),
        }
        self._immediate_campaign_lock = threading.RLock()
        self._immediate_campaign_key: tuple[str, int] | None = None
        self._immediate_campaign_id: str | None = None
        self._immediate_campaign_started_at: datetime | None = None
        self._immediate_campaign_updated_at: datetime | None = None
        self._immediate_campaign_attempts: list[dict[str, object]] = []
        self._immediate_campaign_target_count = 0
        self._immediate_campaign_next_symbol: str | None = None
        self._after_hours_indicative_lock = threading.RLock()
        self._after_hours_store = AfterHoursIndicativeStore(
            config.data_dir / "after_hours_indicative.json"
        )
        try:
            restored_after_hours = self._after_hours_store.read()
        except RuntimeError:
            restored_after_hours = None
        self._after_hours_indicative_best: dict[str, object] | None = (
            restored_after_hours
            if restored_after_hours is not None
            and (
                _after_hours_cache_fresh(
                    restored_after_hours,
                    now=datetime.now(timezone.utc),
                )
                or _after_hours_campaign_completed(restored_after_hours)
            )
            else None
        )
        self._after_hours_research_rows: dict[str, dict[str, object]] = {}
        if self._after_hours_indicative_best is not None:
            for cached_row in self._after_hours_indicative_best.get("candidates", ()):
                if not isinstance(cached_row, Mapping):
                    continue
                research_row = _after_hours_research_row_from_cached(cached_row)
                if research_row is None:
                    continue
                symbol = str(research_row["underlying"]).strip().upper()
                self._after_hours_research_rows[symbol] = research_row
        self._after_hours_discovery_reasons: tuple[str, ...] = ()
        # This store is independent from RankingStore and is opened before any
        # production/broker composition.  Corruption therefore aborts startup
        # without crossing a broker boundary.
        self.news_preselection_store = NewsPreselectionStore(
            config.data_dir / "news_preselection.sqlite3"
        )
        self.equity_pool_store = EquityPoolStore(config.data_dir / "equity_pool.sqlite3")
        self.option_pool_store = OptionStructurePoolStore(
            config.data_dir / "option_structure_pool.sqlite3"
        )
        self.feature_source_store = FeatureSourceObservationStore(
            config.data_dir / "feature_source_observations.sqlite3"
        )
        self.history_source_store = HistorySourceStore(config.data_dir / "scheduled_history_sources.sqlite3")
        self.feature_source_resolver = FeatureSourceResolver(
            self.feature_source_store, history_store=self.history_source_store,
        )
        self.scheduled_history_producer: ScheduledHistoryProducer | None = None
        self.preselection_provider = LedgerBackedPreselectionProvider(
            self.news_preselection_store,
            require_external_source_lineage=(
                config.broker_acquisition_mode == "EXTERNAL"
            ),
        )
        self.snapshot_store = ManagedSnapshotStore(config.data_dir / "runtime_snapshot.json")
        self.ledger = DecisionLedger(config.data_dir / "learning.sqlite3")
        self.learning = LearningGovernance(self.ledger)
        self.shadow_learning = ShadowLearningLedger(
            config.data_dir / "shadow_learning.sqlite3"
        )
        self.shadow_evaluation_store = ShadowEvaluationStore(
            config.data_dir / "shadow_evaluation.sqlite3"
        )
        self.shadow_evaluation_store.refresh(self.shadow_learning)
        self._learning_projection_lock = threading.RLock()
        self._learning_projection_cache: _LearningProjectionCache | None = None
        self.outcome_recorder = OutcomeRecorder(
            config.data_dir / "candidate_outcomes.sqlite3"
        )
        self.outcome_progress_store = OutcomeProgressStore(
            config.data_dir / "outcome_progress.sqlite3"
        )
        self.outcome_processor: ImmutableOutcomeProcessor | None = None
        self.outcome_capture_loop: OutcomeCaptureLoop | None = None
        self.approvals = ProposalApprovalStore(config.data_dir / "approvals.sqlite3")
        self.bridge = CodexBridgeStore(
            config.data_dir / "codex_bridge.sqlite3",
            self.approvals,
        )
        self.production_composition: ProductionComposition | None = None
        self.external_top10_composition: ExternalTop10Composition | None = None
        self._production_composition_reason: str | None = None
        if runtime_services is not None:
            self.runtime_services = runtime_services
        elif config.broker_acquisition_mode == "EXTERNAL":
            self.runtime_services = _unavailable_runtime_services(
                approval_store=self.approvals,
                bridge_reader=self.bridge,
            )
            try:
                self.external_top10_composition = (
                    build_external_top10_composition(
                        config,
                        news_preselection_store=self.news_preselection_store,
                    )
                )
            except Exception:
                # External files alone do not prove an options session.  A
                # missing/tampered broker calendar never falls back to a local
                # weekday rule or a direct IBKR connector.
                self._production_composition_reason = TOP10_PRODUCER_UNAVAILABLE
        else:
            try:
                self.production_composition = build_production_composition(
                    config,
                    approval_store=self.approvals,
                    bridge_reader=self.bridge,
                    # DIRECT mode composes its read-only structure discovery,
                    # fresh broker calendar gate, and aggregate instruction
                    # reader by default.  RankingStore is still never used as
                    # the independent Top-10 ledger or structure source.
                    news_preselection_store=self.news_preselection_store,
                    top10_research_pool_store=self.option_pool_store,
                    feature_source_resolver=self.feature_source_resolver,
                    history_preparation_requested=True,
                )
            except Exception:
                self._production_composition_reason = (
                    "PRODUCTION_COMPOSITION_UNAVAILABLE"
                )
                self.runtime_services = _unavailable_runtime_services(
                    approval_store=self.approvals,
                    bridge_reader=self.bridge,
                )
            else:
                self.runtime_services = self.production_composition.services
        secret_store = LocalApiKeyStore(local_api_key_path(config.data_dir))
        fundamentals_providers: list[object] = [
            SecCompanyFactsProvider(),
            SecManagementGuidanceProvider(),
        ]
        try:
            if "FINNHUB_API_KEY" in set(secret_store.names()):
                fundamentals_providers.append(FinnhubValuationProvider(secret_store))
        except LocalApiKeyFileError:
            pass
        self.fundamentals = FundamentalsService(
            FundamentalsStore(config.data_dir / "fundamentals.sqlite3"),
            providers=fundamentals_providers,
            symbols=config.news_core_symbols,
        )
        jin10_binding_store = DPAPISecretStore(config.secrets_path)
        # Preserve the composition hook's single-argument contract used by
        # isolated runtimes.  The status stores below point at the same fixed
        # local paths; any concurrent edit is detected by the restart-required
        # fingerprint comparison rather than treated as runtime-loaded.
        news_providers, calendar_providers = (
            ((), ())
            if reaction_overrides is not None
            else _configured_event_providers(config)
        )
        self.provider_key_store = secret_store
        self.jin10_binding_store = jin10_binding_store
        phase2_advisory = None
        phase2_advisory_fallback_reason = (
            "MODEL_EVALUATION_PENDING"
            if config.news_llm_enabled
            else "MODEL_DISABLED"
        )
        try:
            phase2_advisory_composition = build_optional_phase2_advisory(
                enabled=config.news_llm_enabled,
                secrets=secret_store,
                cost_state_path=config.data_dir / "phase2_advisory_cost.json",
                readiness_evidence=None,
            )
        except Exception:
            # Optional advisory composition cannot replace or downgrade the
            # already-built deterministic production dependency graph.
            pass
        else:
            phase2_advisory = phase2_advisory_composition.adapter
            phase2_advisory_fallback_reason = (
                phase2_advisory_composition.fallback_reason
            )
        classifier = build_optional_shadow_news_classifier(
            enabled=config.news_llm_enabled,
            secrets=secret_store,
            cost_state_path=config.data_dir / "news_llm_cost.json",
        )
        # Keep the deterministic analyzer as the durable primary projection.
        # The optional model is retained for the bounded shadow-advisory lane;
        # injecting it as the primary classifier would invalidate every
        # persisted analysis fingerprint and replay the whole 500-row history.
        self.news_advisory_classifier = classifier
        shadow_advisory = None
        shadow_writer = None
        if classifier is not None:
            advisory_enabled_at = datetime.now(timezone.utc)
            try:
                shadow_writer = NewsShadowLearningWriter(
                    self.shadow_learning,
                    enabled_at=advisory_enabled_at,
                )
                shadow_advisory = ShadowResearchAdvisory(
                    classifier=classifier,
                    enabled_at=shadow_writer.enabled_at,
                    maximum_batch_size=3,
                )
            except Exception:
                # A model or shadow-ledger composition failure must leave the
                # deterministic primary news path intact and model-free.
                shadow_advisory = None
                shadow_writer = None
        initial_status = dict(provider_configuration_status(secret_store))
        initial_jin10_activation = (
            resolve_jin10_credential(
                LocalJin10EnvelopeReader(secret_store, jin10_binding_store),
                jin10_rotation_evidence_dir(config.data_dir),
            ).status
            == "ACTIVATED"
        )
        production = self.production_composition
        reaction_jin10 = resolve_jin10_credential(
            LocalJin10EnvelopeReader(secret_store, jin10_binding_store),
            jin10_rotation_evidence_dir(config.data_dir),
        )
        reaction_store = ReactionEvidenceStore(
            config.data_dir / "macro_reactions.sqlite3"
        )
        self.macro_reactions = (
            reaction_overrides.provider_factory(reaction_store)
            if reaction_overrides is not None
            else ProductionMacroReactionProvider(
                reaction_store,
                jin10_client=Jin10McpHttpClient(),
                jin10_secret_store=reaction_jin10.secret_store,
                official_actual_provider=BlsPublicDataActualProvider(),
                official_document_provider=OfficialReleaseCaptureCoordinator(),
                reaction_observer=(
                    None
                    if production is None
                    else ProductionReactionObserver(
                        production.news_adapter,
                        production.pacing_guard,
                        store=reaction_store,
                    )
                ),
            )
        )
        self._provider_startup_fingerprints = _provider_value_fingerprints(
            secret_store
        )
        self._provider_startup_state = {
            "jin10_mcp_token": {
                "status": initial_status["jin10_mcp_token"],
                "activated": initial_jin10_activation,
                "composed": initial_jin10_activation,
            },
            "finnhub_api_key": {
                "status": initial_status["finnhub_api_key"],
                "activated": True,
                "composed": any(
                    isinstance(item, FinnhubEventProvider)
                    for item in (*news_providers, *calendar_providers)
                ),
            },
            "alpha_vantage_api_key": {
                "status": initial_status["alpha_vantage_api_key"],
                "activated": True,
                "composed": any(
                    isinstance(item, AlphaVantageNewsProvider)
                    for item in news_providers
                ),
            },
            "deepseek_api_key": {
                "status": initial_status["deepseek_api_key"],
                "activated": True,
                "composed": classifier is not None,
            },
        }
        production = self.production_composition
        if production is not None:
            evidence_store = production.services.evidence_store
            ranking_store = production.services.ranking_store
            if isinstance(ranking_store, RankingStore) and evidence_store is not None:
                self.outcome_processor = ImmutableOutcomeProcessor(
                    shadow_ledger=self.shadow_learning,
                    candidate_recorder=self.outcome_recorder,
                    candidate_targets=ranking_store.outcome_targets,
                    observation_provider=EvidenceStoreOutcomeObservationProvider(
                        evidence_store
                    ),
                    progress_store=self.outcome_progress_store,
                    maximum_work_items=100,
                    maximum_pending_items=1000,
                )
                outcome_capture = ExactHorizonOutcomeCapture(
                    evidence_store=evidence_store,
                    market_adapter=production.outcome_market_adapter,
                )
                self.outcome_capture_loop = OutcomeCaptureLoop(
                    OutcomeCaptureCoordinator(
                        capture=outcome_capture,
                        candidate_targets=RankingOutcomeTargetCursor(
                            ranking_store
                        ),
                        prediction_targets=ShadowPredictionTargetCursor(
                            self.shadow_learning
                        ),
                        calendar_provider=production.calendar_provider,
                    )
                )
        public_official_calendar = (
            reaction_overrides.public_calendar_provider
            if reaction_overrides is not None
            else build_official_calendar_provider()
        )
        reaction_schedule_calendar = (
            reaction_overrides.schedule_calendar_provider
            if reaction_overrides is not None
            else build_official_calendar_provider()
        )
        self.news = NewsCoordinator(
            config.news_evidence_path,
            cadence_path=config.news_cadence_path,
            news_providers=news_providers,
            calendar_providers=calendar_providers,
            official_calendar_provider=public_official_calendar,
            reaction_schedule_provider=reaction_schedule_calendar,
            ibkr_binding_provider=(None if production is None else production.news_adapter),
            preselection_provider=self.preselection_provider,
            reaction_provider=self.macro_reactions,
            classifier=None,
            shadow_advisory=shadow_advisory,
            phase2_advisory=phase2_advisory,
            phase2_advisory_fallback_reason=phase2_advisory_fallback_reason,
            shadow_writer=shadow_writer,
            core_symbols=config.news_core_symbols,
            poll_interval_seconds=config.news_refresh_seconds,
            clock=(None if reaction_overrides is None else reaction_overrides.clock),
        )
        equity_cache = getattr(self.runtime_services, "equity_evidence_cache", None)
        self.equity_pool = EquityPoolService(
            self.equity_pool_store,
            news_reader=self.news.decision_equity_news_payload,
            fundamentals_reader=lambda symbol, as_of: self.fundamentals.supporting_evidence(symbol, as_of=as_of),
            factor_readers=(
                {}
                if equity_cache is None
                else {
                    kind: (
                        lambda symbol, as_of, factor_kind=kind: equity_cache.read_factor(
                            symbol, factor_kind, as_of
                        )
                    )
                    for kind in (
                        FactorKind.REGIME,
                        FactorKind.TREND_VOLATILITY,
                        FactorKind.POSITIONING,
                    )
                }
            ),
            liquidity_reader=(
                None
                if equity_cache is None
                else lambda symbol, as_of: equity_cache.read_liquidity(symbol, as_of)
            ),
            evidence_batch_reader=(
                None if equity_cache is None else equity_cache.snapshot_for
            ),
        )
        self.option_pool = OptionStructurePoolService(self.option_pool_store)
        if self._after_hours_indicative_best is not None:
            self._restore_after_hours_formal_pools()
        if production is not None:
            self.scheduled_history_producer = ScheduledHistoryProducer(
                production.gateway, self.history_source_store,
                closing=lambda: self._closing or self._closed,
            )
            production.pipeline_inputs.bind_equity_pool_builder(self.equity_pool.build)
            if equity_cache is not None:
                production.pipeline_inputs.bind_equity_evidence_capture(
                    lambda rows, captured_at: tuple(
                        equity_cache.append(
                            record
                        )
                        for record in captured_records_from_quotes(
                            tuple(rows), captured_at=captured_at
                        )
                    )
                )
            production.pipeline_inputs.bind_event_pool_reader(
                self.news.decision_event_payload
            )
            production.pipeline_inputs.bind_fundamentals_reader(
                lambda symbol, as_of: self.fundamentals.supporting_evidence(
                    symbol,
                    as_of=as_of,
                )
            )
            production.services.decision_pipeline.option_pool_recorder = (
                self.option_pool
            )
            bind_daily_callbacks = getattr(
                production.lifecycle.scanner_loop,
                "bind_daily_callbacks",
                None,
            )
            if callable(bind_daily_callbacks):
                bind_daily_callbacks(
                    research_refresh=self.news.refresh_research,
                    outcome_process=(
                        None
                        if self.outcome_processor is None
                        else self._process_outcomes
                    ),
                    position_research=self.position_research_top10,
                    after_hours_discovery=self.after_hours_indicative,
                    after_hours_reprice=self.after_hours_indicative,
                    verified_after_hours=self.verified_after_hours_payload,
                    next_session_preparation=self.next_session_preparation,
                )
        self._lock = threading.RLock()
        self._snapshot = self.snapshot_store.read()
        self._register_baseline_if_needed()

    def close(self) -> bool:
        with self._close_lock:
            if self._closed:
                return True
            # Fence new source writes first, then drain an already accepted
            # HTTP diagnostic before closing its store or broker owner.
            with self._feature_source_persistence_lock:
                self._closing = True
            if not self._feature_source_diagnostic_lock.acquire(timeout=1.0):
                self._shutdown_health = {
                    "status": "DEGRADED", "reason": "FEATURE_SOURCE_DIAGNOSTIC_SHUTDOWN_TIMEOUT",
                }
                return False
            self._feature_source_diagnostic_lock.release()
            # Manual scans run in HTTP workers, not the background scanner
            # thread joined by ProductionLifecycle. Drain that lane as well.
            if not self._immediate_scan_lock.acquire(timeout=1.0):
                self._shutdown_health = {
                    "status": "DEGRADED", "reason": "MANUAL_SCAN_SHUTDOWN_TIMEOUT",
                }
                return False
            self._immediate_scan_lock.release()
            # Daily callbacks can use news, fundamentals and source stores.
            # Stop their admission and drain even timed-out callbacks before
            # closing any of those dependencies, not merely their run leases.
            if self.production_composition is not None:
                scanner_close = getattr(
                    getattr(self.production_composition.lifecycle, "scanner_loop", None), "close", None,
                )
                if callable(scanner_close) and scanner_close() is False:
                    self._shutdown_health = {
                        "status": "DEGRADED", "reason": "DAILY_CALLBACK_SHUTDOWN_TIMEOUT",
                    }
                    return False
            if self.news.close() is False:
                self._shutdown_health = {
                    "status": "DEGRADED",
                    "reason": "NEWS_WORKER_SHUTDOWN_TIMEOUT",
                }
                return False
            if self.fundamentals.close() is False:
                self._shutdown_health = {
                    "status": "DEGRADED",
                    "reason": "FUNDAMENTALS_WORKER_SHUTDOWN_TIMEOUT",
                }
                return False
            self.macro_reactions.close()
            if (
                self.outcome_capture_loop is not None
                and self.outcome_capture_loop.close() is False
            ):
                self._shutdown_health = {
                    "status": "DEGRADED",
                    "reason": "OUTCOME_WORKER_SHUTDOWN_TIMEOUT",
                }
                return False
            if (
                self.production_composition is not None
                and self.production_composition.lifecycle.close() is False
            ):
                self._shutdown_health = {
                    "status": "DEGRADED",
                    "reason": "PRODUCTION_WORKER_SHUTDOWN_TIMEOUT",
                }
                return False
            if self.external_top10_composition is not None:
                if self.external_top10_composition.close() is False:
                    self._shutdown_health = {
                        "status": "DEGRADED",
                        "reason": "EXTERNAL_TOP10_WORKER_SHUTDOWN_TIMEOUT",
                    }
                    return False
            self.news_preselection_store.close()
            self.equity_pool_store.close()
            self.option_pool_store.close()
            self.feature_source_store.close()
            self.history_source_store.close()
            self.bridge.close()
            self.approvals.close()
            self.outcome_progress_store.close()
            self.outcome_recorder.close()
            self.shadow_evaluation_store.close()
            self.shadow_learning.close()
            self.learning.close()
            self.ledger.close()
            self._closed = True
            self._shutdown_health = {"status": "CLOSED", "reason": None}
            return True

    def start(self) -> None:
        """Start optional read-only background dependencies."""

        self._learning_projection(self.shadow_learning.verified_replay_snapshot())
        if self.outcome_capture_loop is not None:
            try:
                self.outcome_capture_loop.coordinator.capture.durable_counts()
            except Exception:
                # The API keeps reporting the exact fail-closed store blocker;
                # optional learning evidence must not take down the GUI.
                pass
        if self.production_composition is not None:
            self.production_composition.lifecycle.start()
            if self.outcome_capture_loop is not None:
                self.outcome_capture_loop.start()
        if self.external_top10_composition is not None:
            self.external_top10_composition.start()
        self.news.start()
        self.fundamentals.start()

    def services(self) -> OptionsCopilotServices:
        return OptionsCopilotServices(
            health_provider=self.health,
            health_summary_provider=self.health_summary,
            bootstrap_provider=self.bootstrap,
            candidates_provider=self.candidates,
            positions_provider=self.positions,
            learning_provider=self.learning_status,
            learning_records_provider=self.learning_records,
            learning_record_provider=self.learning_record,
            learning_replay_provider=self.learning_replay,
            learning_similarity_provider=self.learning_similar,
            news_provider=self.news.news_payload,
            calendar_provider=self.news.calendar_payload,
            advisory_provider=self.news.advisory_payload,
            fundamentals_provider=self.fundamentals.payload,
            source_evidence_provider=self.news.source_evidence_payload,
            positioning_provider=self.positioning,
            equity_pool_provider=self.equity_pool.latest_payload,
            option_pool_provider=self.option_pool.latest_payload,
            approval_handler=None,
            approval_status_provider=self.approval_status,
            readiness_provider=self.runtime_services.readiness,
            latest_scan_provider=self.latest_scan,
            latest_ranking_provider=self.latest_ranking,
            ranking_provider=self.ranking,
            candidate_evidence_provider=self.candidate_evidence,
            management_provider=self.management,
            rank_one_challenge_handler=self.rank_one_challenge,
            challenge_confirmation_handler=self.confirm_challenge,
            provider_configuration_provider=self.provider_configuration,
            weekly_brief_provider=self.news.weekly_brief_payload,
            after_hours_indicative_provider=self.after_hours_indicative,
            after_hours_latest_provider=self.after_hours_latest,
            immediate_scan_handler=self.immediate_scan,
            immediate_scan_campaign_provider=self.immediate_scan_campaign,
            option_market_data_diagnostic_handler=(
                self.option_market_data_diagnostic
            ),
            feature_sources_diagnostic_handler=self.feature_sources_diagnostic,
            feature_source_cache_provider=self.feature_source_cache,
        )

    def feature_source_cache(self) -> Mapping[str, object]:
        """Inspect persisted observations and actual consumption, with no requests."""

        try:
            cache = self.feature_source_store.status()
        except FeatureSourceStoreError as exc:
            reason = str(exc) if str(exc) in {
                "FEATURE_SOURCE_STORE_VERIFICATION_BUDGET_EXCEEDED", "FEATURE_SOURCE_STORE_CLOSED",
                "FEATURE_SOURCE_STORE_INVALID",
            } else "FEATURE_SOURCE_CACHE_UNAVAILABLE_OR_INVALID"
            cache = {"status": "UNAVAILABLE", "reason_codes": (reason,)}
        except Exception:
            cache = {"status": "UNAVAILABLE", "reason_codes": ("FEATURE_SOURCE_CACHE_UNAVAILABLE_OR_INVALID",)}
        acquisition = getattr(self.runtime_services, "broker_evidence_acquisition", None)
        reader = getattr(acquisition, "feature_source_bindings", None)
        try:
            consumption = reader() if callable(reader) else {
                "status": "WIRED_NOT_RUN", "reason_codes": ("FEATURE_SOURCE_RESOLVER_UNWIRED",),
            }
        except Exception:
            consumption = {"status": "INCOMPLETE", "reason_codes": ("FEATURE_SOURCE_BINDING_UNAVAILABLE",)}
        with self._feature_source_persistence_lock:
            ingestion = dict(self._feature_source_persistence)
        producer = getattr(self, "scheduled_history_producer", None)
        producer_status = (
            producer.status() if producer is not None else {
                "status": "UNWIRED", "reason_codes": ("FEATURE_HISTORY_PRODUCER_UNWIRED",),
            }
        )
        convention_cutoff = datetime.now(timezone.utc)
        body = {
            "schema": "options_copilot.feature_source_cache.v1", "status": "INCOMPLETE",
            "acquisition_mode": (
                "CONFIRMED_DIAGNOSTIC_AND_LEASED_DAILY_HISTORY"
                if producer is not None else "CONFIRMED_DIAGNOSTIC_ONLY"
            ),
            "cache": cache, "latest_ingestion": ingestion, "latest_consumption": consumption,
            "scheduled_producer": producer_status,
            "ema20_convention": ema20_convention(convention_cutoff),
            "iv_percentile_convention": iv_percentile_convention(convention_cutoff),
            "qqq_benchmark_convention": benchmark_convention("QQQ", convention_cutoff),
            "model_input_complete": False, "production_eligible": False,
            "decision_authority": "OBSERVATION_ONLY",
        }
        return {**body, "content_hash": canonical_hash(body)}

    def _persist_feature_source(
        self, source: Mapping[str, object], *, operation_id: str, cutoff: datetime,
    ) -> tuple[dict[str, object] | None, str | None]:
        """Save a validated wire projection; failures never alter source facts."""

        store = getattr(self, "feature_source_store", None)
        lock = getattr(self, "_feature_source_persistence_lock", None)
        if store is None or lock is None:
            return None, "FEATURE_SOURCE_STORE_UNWIRED"
        if not isinstance(source.get("contract"), Mapping):
            return None, "FEATURE_SOURCE_IDENTITY_NOT_OBSERVED"
        with lock:
            if self._closing or self._closed:
                return None, "FEATURE_SOURCE_RUNTIME_CLOSING"
            try:
                return store.append(source, operation_id=operation_id, cutoff=cutoff), None
            except FeatureSourceStoreError as exc:
                reason = str(exc) if str(exc) in {
                    "FEATURE_SOURCE_STORE_VERIFICATION_BUDGET_EXCEEDED", "FEATURE_SOURCE_STORE_CLOSED",
                    "FEATURE_SOURCE_STORE_INVALID", "FEATURE_SOURCE_STORE_CLOCK_REGRESSED",
                } else "FEATURE_SOURCE_PERSISTENCE_FAILED"
                return None, reason
            except Exception:
                return None, "FEATURE_SOURCE_PERSISTENCE_FAILED"

    def feature_sources_diagnostic(
        self,
        request: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Observe three bounded raw sources through the existing broker owner."""

        if (
            set(request) != {"confirmation_token", "scope", "symbol"}
            or request.get("confirmation_token") != "READ_FEATURE_SOURCE_DIAGNOSTIC"
            or request.get("scope") != "SINGLE_UNDERLYING"
        ):
            raise ValueError("FEATURE_SOURCE_DIAGNOSTIC_REQUEST_INVALID")
        symbol = request.get("symbol")
        if (
            not isinstance(symbol, str)
            or symbol != symbol.strip().upper()
            or symbol not in self.config.news_core_symbols
        ):
            raise ValueError("FEATURE_SOURCE_SYMBOL_NOT_ALLOWED")

        requested_at = datetime.now(timezone.utc)

        def result(
            status: str,
            reasons: tuple[str, ...],
            sources: Mapping[str, object],
        ) -> dict[str, object]:
            cooldown_until = self._feature_source_diagnostic_cooldown_until
            body = {
                "schema": "options_copilot.feature_sources_diagnostic.v1",
                "status": status,
                "scope": "SINGLE_UNDERLYING",
                "symbol": symbol,
                "requested_at": requested_at.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "cooldown_scope": "RUNTIME_INSTANCE",
                "cooldown_until": None if cooldown_until is None else cooldown_until.isoformat(),
                "sources": dict(sources),
                "reason_codes": reasons,
                "basis_status": "PROVIDER_NATIVE_UNRESOLVED",
                "model_input_complete": False,
                "production_eligible": False,
                "point_in_time_verified": False,
                "decision_authority": "OBSERVATION_ONLY",
                "review_only": True,
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "direct_order_submission": False,
                "broker_write_authority": False,
            }
            return {**body, "content_hash": canonical_hash(body)}

        def not_requested(reason: str) -> dict[str, object]:
            return {
                kind: _feature_source_diagnostic_failure(
                    kind, symbol, reason, status="NOT_REQUESTED", request_sent=False,
                )
                for kind, _method in _FEATURE_SOURCE_DIAGNOSTIC_METHODS
            }

        if not self._feature_source_diagnostic_lock.acquire(blocking=False):
            reason = "FEATURE_SOURCE_DIAGNOSTIC_RUNNING"
            return result("WAIT", (reason,), not_requested(reason))
        try:
            attempted_at = time.monotonic()
            previous = self._feature_source_diagnostic_last_attempt
            if (
                previous is not None
                and attempted_at - previous < _FEATURE_SOURCE_DIAGNOSTIC_COOLDOWN_SECONDS
            ):
                reason = "FEATURE_SOURCE_DIAGNOSTIC_COOLDOWN"
                return result("WAIT", (reason,), not_requested(reason))
            # Every accepted attempt consumes the cooldown, including partial
            # delivery and failure. There is no retry or alternate connection.
            self._feature_source_diagnostic_last_attempt = attempted_at
            self._feature_source_diagnostic_cooldown_until = (
                requested_at + timedelta(seconds=_FEATURE_SOURCE_DIAGNOSTIC_COOLDOWN_SECONDS)
            )
            composition = self.production_composition
            gateway = None if composition is None else composition.gateway
            if gateway is None or not gateway.connected or self._closed or getattr(self, "_closing", False):
                reason = "IBKR_READONLY_GATEWAY_NOT_CONNECTED"
                return result("UNAVAILABLE", (reason,), not_requested(reason))

            sources: dict[str, object] = {}
            statuses: list[str] = []
            reasons: list[str] = []
            operation_id = "feature-diagnostic." + canonical_hash({"symbol": symbol, "requested_at": requested_at})
            references: dict[str, object] = {}
            persistence_reasons: list[str] = []
            for kind, method_name in _FEATURE_SOURCE_DIAGNOSTIC_METHODS:
                reader = getattr(gateway, method_name, None)
                if self._closed or getattr(self, "_closing", False) or not gateway.connected:
                    source = _feature_source_diagnostic_failure(
                        kind, symbol, "IBKR_READONLY_GATEWAY_NOT_CONNECTED",
                        status="NOT_REQUESTED", request_sent=False,
                    )
                elif not callable(reader):
                    source = _feature_source_diagnostic_failure(
                        kind, symbol, "FEATURE_SOURCE_METHOD_UNAVAILABLE",
                        status="NOT_REQUESTED", request_sent=False,
                    )
                else:
                    try:
                        # Each call independently enters and leaves the shared
                        # gateway owner queue. The control loop can run between
                        # sources; this lock belongs only to the diagnostic.
                        raw = reader(symbol, end_at=requested_at)
                    except MarketDataPacingError:
                        source = _feature_source_diagnostic_failure(
                            kind, symbol, "FEATURE_SOURCE_PACING_DENIED", request_sent=False,
                        )
                    except TimeoutError:
                        source = _feature_source_diagnostic_failure(
                            kind, symbol, "FEATURE_SOURCE_TIMEOUT",
                        )
                    except Exception:
                        source = _feature_source_diagnostic_failure(
                            kind, symbol, "FEATURE_SOURCE_UNAVAILABLE",
                        )
                    else:
                        try:
                            source = validate_feature_source_observation(
                                raw, kind=kind, symbol=symbol, cutoff=requested_at,
                            )
                        except Exception:
                            source = _feature_source_diagnostic_failure(
                                kind, symbol, "FEATURE_SOURCE_RESPONSE_INVALID",
                            )
                sources[kind] = source
                reference, persistence_reason = self._persist_feature_source(
                    source, operation_id=operation_id, cutoff=requested_at,
                )
                if reference is not None:
                    references[kind] = reference
                if persistence_reason is not None:
                    persistence_reasons.append(persistence_reason)
                statuses.append(str(source["status"]))
                reasons.extend(source["reason_codes"])
            status = (
                "OBSERVED" if all(item == "DELIVERED" for item in statuses)
                else "PARTIAL" if any(item in {"DELIVERED", "PARTIAL"} for item in statuses)
                else "UNAVAILABLE"
            )
            persistence_lock = getattr(self, "_feature_source_persistence_lock", None)
            if persistence_lock is not None:
                with persistence_lock:
                    self._feature_source_persistence = {
                        "status": "PERSISTED" if len(references) == 3 else "PARTIAL" if references else "NOT_PERSISTED",
                        "operation_id": operation_id, "references": references,
                        "reason_codes": tuple(dict.fromkeys(persistence_reasons)),
                    }
            return result(status, tuple(dict.fromkeys(reasons)), sources)
        finally:
            self._feature_source_diagnostic_lock.release()

    def option_market_data_diagnostic(
        self,
        request: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Run one exact-contract read on the runtime-owned paced gateway."""

        composition = self.production_composition
        if composition is None or not composition.gateway.connected:
            raise RuntimeError("IBKR_READONLY_GATEWAY_NOT_CONNECTED")
        contract_id = int(request["contract_id"])
        contract = OptionContractRef(
            contract_id=contract_id,
            contract_id_ex=f"{contract_id}@{request['exchange']}",
            symbol=str(request["symbol"]),
            local_symbol=str(request["local_symbol"]),
            expiration=date.fromisoformat(str(request["expiration"])),
            strike=Decimal(str(request["strike"])),
            right=str(request["right"]),  # type: ignore[arg-type]
            exchange=str(request["exchange"]),
            trading_class=str(request["trading_class"]),
            multiplier=int(request["multiplier"]),
            currency=str(request["currency"]),
        )
        observed_at = datetime.now(timezone.utc)
        try:
            batch = composition.gateway.option_quote_batch((contract,))
        except MarketDataPacingError as exc:
            return {
                "schema": "options_copilot.option_market_data_diagnostic.v1",
                "status": "WAIT",
                "observed_at": observed_at.isoformat(),
                "contract_id": contract_id,
                "reason_codes": (exc.reason_code,),
                "request_sent": False,
                "decision_authority": "OBSERVATION_ONLY",
                "review_only": True,
                "direct_order_submission": False,
                "broker_write_authority": False,
            }

        quote = batch.quotes[0] if batch.quotes else None
        diagnostics = tuple(
            {
                "broker_request_id": item.broker_request_id,
                "broker_timed_bbo_request_id": (
                    item.broker_timed_bbo_request_id
                ),
                "contract_id": item.contract_id,
                "transport": item.transport,
                "generic_ticks": item.generic_ticks,
                "received_fields": item.received_fields,
                "missing_fields": item.missing_fields,
                "error_codes": item.error_codes,
                "deadline_expired": item.deadline_expired,
                "timeout_reason": item.timeout_reason,
            }
            for item in batch.request_diagnostics
        )
        return {
            "schema": "options_copilot.option_market_data_diagnostic.v1",
            "status": batch.status.value,
            "observed_at": (
                batch.observed_at or batch.completed_at
            ).isoformat(),
            "contract": {
                "contract_id": contract.contract_id,
                "symbol": contract.symbol,
                "local_symbol": contract.local_symbol,
                "expiration": contract.expiration.isoformat(),
                "strike": str(contract.strike),
                "right": contract.right,
                "exchange": contract.exchange,
                "trading_class": contract.trading_class,
                "multiplier": contract.multiplier,
                "currency": contract.currency,
            },
            "batch_id": batch.batch_id,
            "source": batch.source,
            "blockers": batch.blockers,
            "request_diagnostics": diagnostics,
            "quote": None
            if quote is None
            else {
                "bid": None if quote.bid is None else str(quote.bid),
                "ask": None if quote.ask is None else str(quote.ask),
                "exchange_time": None
                if quote.exchange_time is None
                else quote.exchange_time.isoformat(),
                "market_data_type": quote.market_data_type,
                "implied_volatility": None
                if quote.implied_volatility is None
                else str(quote.implied_volatility),
                "delta": None if quote.delta is None else str(quote.delta),
                "gamma": None if quote.gamma is None else str(quote.gamma),
                "theta": None if quote.theta is None else str(quote.theta),
                "vega": None if quote.vega is None else str(quote.vega),
                "volume": quote.volume,
                "open_interest": quote.open_interest,
            },
            "request_sent": bool(batch.request_diagnostics),
            "decision_authority": "OBSERVATION_ONLY",
            "review_only": True,
            "direct_order_submission": False,
            "broker_write_authority": False,
        }

    def position_research_top10(
        self,
        *,
        cancel_event: threading.Event | None = None,
        deadline_at: datetime | None = None,
    ) -> Mapping[str, object]:
        """Persist today's exact-identity watchlist while entry stays blocked."""

        if _operation_cancelled(cancel_event, deadline_at):
            return {
                "status": "NO_TRADE",
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "reason_codes": ("POSITION_RESEARCH_CANCELLED",),
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }

        composition = self.production_composition
        clock = (
            None
            if composition is None
            else getattr(composition.gateway, "_now", None)
        )
        observed_at = (
            clock().astimezone(timezone.utc)
            if callable(clock)
            else datetime.now(timezone.utc)
        )
        observed_et = observed_at.astimezone(US_OPTIONS_TIMEZONE)
        if (
            observed_et.weekday() >= 5
            or (observed_et.hour, observed_et.minute) < (9, 30)
            or (observed_et.hour, observed_et.minute) >= (16, 0)
        ):
            return {
                "status": "NO_TRADE",
                "observed_at": observed_at.isoformat(),
                "reason_codes": ("POSITION_RESEARCH_OUTSIDE_REGULAR_SESSION",),
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }
        current = read_research_top10()
        if (
            current.get("trading_date") == observed_et.date().isoformat()
            and current.get("phase") == INTRADAY_RECOVERY
        ):
            return current
        control = self._current_control_projection()
        positions = None if control is None else control.get("positions")
        if (
            composition is None
            or not isinstance(positions, Sequence)
            or isinstance(positions, (str, bytes, bytearray, memoryview))
            or not _has_open_or_ambiguous_option_position(positions)
        ):
            return {
                "status": "NO_TRADE",
                "observed_at": observed_at.isoformat(),
                "reason_codes": ("POSITION_RESEARCH_NOT_REQUIRED",),
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }
        account = control.get("account") if control is not None else None
        try:
            control_observed_at = datetime.fromisoformat(
                str(control.get("observed_at"))
            )
            observed_nlv = _positive_decimal(
                account.get("net_liquidation")
                if isinstance(account, Mapping)
                else None,
                "control.account.net_liquidation",
            )
            nav, _reasons = self.runtime_services._strategy_nav(
                control_observed_at,
                observed_account_nlv=observed_nlv,
            )
            if nav is None or nav.strategy_nav is None:
                raise ValueError("strategy NAV is unavailable")
            strategy_nav = nav.strategy_nav
        except (TypeError, ValueError):
            return {
                "status": "NO_TRADE",
                "observed_at": observed_at.isoformat(),
                "reason_codes": ("STRATEGY_NAV_RECONCILIATION_UNAVAILABLE",),
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }

        discovery = DirectTop10StructureSource(
            composition.gateway,
            composition.pipeline_inputs.pacing,
            clock=lambda: datetime.now(timezone.utc),
            core_symbols=_after_hours_core_symbols(self.config.news_core_symbols),
            include_scanner=True,
            maximum_optionability_attempts=10,
            maximum_optionable=10,
            maximum_structures=10,
            indicative_underlyings=True,
        )
        resolved = discovery.resolve_top10(scheduled_for=observed_at)
        if _operation_cancelled(cancel_event, deadline_at):
            return {
                "status": "NO_TRADE",
                "observed_at": observed_at.isoformat(),
                "reason_codes": ("POSITION_RESEARCH_CANCELLED",),
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }
        if not resolved:
            return {
                "status": "NO_TRADE",
                "observed_at": observed_at.isoformat(),
                "reason_codes": tuple(
                    dict.fromkeys(
                        (
                            *tuple(getattr(resolved, "reason_codes", ())),
                            "POSITION_RESEARCH_STRUCTURE_SOURCE_EMPTY",
                        )
                    )
                ),
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }
        # Imported lazily to avoid making the standalone recovery CLI part of
        # runtime module initialization.
        from options_copilot.intraday_top10_recovery import (
            build_intraday_recovery_envelope,
        )

        envelope = build_intraday_recovery_envelope(
            resolved,
            observed_at=observed_at,
            strategy_nav_usd=strategy_nav,
        )
        if _operation_cancelled(cancel_event, deadline_at):
            return {
                "status": "NO_TRADE",
                "observed_at": observed_at.isoformat(),
                "reason_codes": ("POSITION_RESEARCH_CANCELLED",),
                "decision_authority": "SUPPORTING_ONLY",
                "action_pool_count": 0,
            }
        import_research_top10(envelope)
        return read_research_top10()

    def after_hours_indicative(
        self,
        *,
        cancel_event: threading.Event | None = None,
        deadline_at: datetime | None = None,
        operation_token: str | None = None,
    ) -> Mapping[str, object]:
        """Run broad discovery and read closing marks without action authority."""

        if _operation_cancelled(cancel_event, deadline_at):
            return unavailable_after_hours_indicative_read_model(
                "AFTER_HOURS_OPERATION_CANCELLED"
            )

        composition = self.production_composition
        if composition is None:
            return unavailable_after_hours_indicative_read_model(
                "IBKR_READONLY_GATEWAY_UNAVAILABLE"
            )
        now_et = datetime.now(timezone.utc).astimezone(US_OPTIONS_TIMEZONE)
        if (
            now_et.weekday() < 5
            and (now_et.hour, now_et.minute) >= (9, 30)
            and (now_et.hour, now_et.minute) < (16, 0)
        ):
            return unavailable_after_hours_indicative_read_model(
                "REGULAR_SESSION_REQUIRES_EXECUTABLE_PIPELINE"
            )
        control = self._current_control_projection()
        strategy_nav: Decimal | None = None
        if control is not None:
            account = control.get("account")
            try:
                observed_at = datetime.fromisoformat(str(control.get("observed_at")))
                observed_nlv = _positive_decimal(
                    account.get("net_liquidation") if isinstance(account, Mapping) else None,
                    "control.account.net_liquidation",
                )
                nav, _reasons = self.runtime_services._strategy_nav(
                    observed_at,
                    observed_account_nlv=observed_nlv,
                )
            except (TypeError, ValueError):
                nav = None
            if nav is not None:
                strategy_nav = nav.strategy_nav
        with self._after_hours_indicative_lock:
            current_utc = datetime.now(timezone.utc)
            if (
                self._after_hours_indicative_best is not None
                and not _after_hours_cache_fresh(
                    self._after_hours_indicative_best,
                    now=current_utc,
                )
            ):
                self._after_hours_indicative_best = None
                self._after_hours_research_rows.clear()
                self._after_hours_discovery_reasons = ()
            missing_basis_symbols = _after_hours_missing_basis_symbols(
                tuple(self._after_hours_research_rows.values())
            )
            basis_recovery = bool(missing_basis_symbols)
            if len(self._after_hours_research_rows) < 10 or basis_recovery:
                excluded = (
                    ()
                    if basis_recovery
                    else tuple(self._after_hours_research_rows)
                )
                discovery = DirectTop10StructureSource(
                    composition.gateway,
                    composition.pipeline_inputs.pacing,
                    clock=lambda: datetime.now(timezone.utc),
                    core_symbols=(
                        missing_basis_symbols
                        if basis_recovery
                        else _after_hours_core_symbols(
                            self.config.news_core_symbols
                        )
                    ),
                    include_scanner=not basis_recovery,
                    excluded_symbols=excluded,
                    maximum_optionability_attempts=4,
                    maximum_optionable=2,
                    maximum_structures=2,
                    indicative_underlyings=True,
                )
                resolved = discovery.resolve_top10(
                    scheduled_for=datetime.now(timezone.utc)
                )
                if _operation_cancelled(cancel_event, deadline_at):
                    return unavailable_after_hours_indicative_read_model(
                        "AFTER_HOURS_OPERATION_CANCELLED"
                    )
                research = _after_hours_research_from_resolution(
                    resolved,
                    metadata=discovery.discovery_metadata,
                )
                self._after_hours_discovery_reasons = tuple(
                    dict.fromkeys(
                        (
                            *self._after_hours_discovery_reasons,
                            *tuple(resolved.reason_codes),
                        )
                    )
                )[-16:]
                for row in research["candidates"]:
                    if isinstance(row, Mapping):
                        symbol = str(row.get("underlying", "")).strip().upper()
                        if symbol:
                            self._after_hours_research_rows[symbol] = dict(row)
                self.fundamentals.observe_symbols(
                    tuple(self._after_hours_research_rows)
                )
            else:
                research = {"candidates": []}
            if self._after_hours_research_rows:
                merged_candidates = [
                    {
                        **row,
                        "rank": rank,
                    }
                    for rank, row in enumerate(
                        list(self._after_hours_research_rows.values())[:10],
                        start=1,
                    )
                ]
                research = {
                    "candidates": merged_candidates,
                    "reason_codes": list(self._after_hours_discovery_reasons),
                    "discovery_mode": (
                        "IBKR_BOUNDED_BASIS_RECOVERY"
                        if basis_recovery
                        else "IBKR_BOUNDED_MARKET_PROGRESSIVE"
                    ),
                }
            else:
                fallback = read_research_top10()
                research = {
                    **fallback,
                    "reason_codes": list(
                        dict.fromkeys(
                            (
                                *tuple(resolved.reason_codes),
                                "BROAD_AFTER_HOURS_DISCOVERY_UNAVAILABLE",
                            )
                        )
                    ),
                    "discovery_mode": "LEGACY_RESEARCH_FALLBACK",
                }
            latest = build_after_hours_indicative_read_model(
                research,
                quote_provider=composition.gateway,
                strategy_nav_usd=strategy_nav,
                normal_risk_fraction=Decimal(
                    str(self.config.normal_risk_fraction)
                ),
                # Eight legs per pass stays inside the approved streaming
                # window while subsequent scheduler heartbeats fill only the
                # remaining exact identities.
                maximum_quote_attempts=4,
                previous_read_model=self._after_hours_indicative_best,
            )
            if _operation_cancelled(cancel_event, deadline_at):
                return unavailable_after_hours_indicative_read_model(
                    "AFTER_HOURS_OPERATION_CANCELLED"
                )
            research_by_symbol = {
                str(row.get("underlying", "")).strip().upper(): row
                for row in research.get("candidates", ())
                if isinstance(row, Mapping)
            }
            projected_candidates: list[dict[str, object]] = []
            for candidate in latest.get("candidates", ()):
                if not isinstance(candidate, Mapping):
                    continue
                projected = dict(candidate)
                research_row = research_by_symbol.get(
                    str(candidate.get("underlying", "")).strip().upper()
                )
                if isinstance(research_row, Mapping):
                    basis = research_row.get("underlying_quote_basis")
                    basis_hash = research_row.get("underlying_quote_basis_hash")
                    if _after_hours_underlying_quote(basis, basis_hash) is not None:
                        projected["underlying_quote_basis"] = basis
                        projected["underlying_quote_basis_hash"] = basis_hash
                projected_candidates.append(projected)
            basis_missing_after_read = _after_hours_missing_basis_symbols(
                tuple(self._after_hours_research_rows.values())
            )
            latest = {
                **latest,
                "candidates": projected_candidates,
                "discovery_mode": research.get(
                    "discovery_mode", "IBKR_BOUNDED_MARKET"
                ),
                "discovery_reason_codes": list(
                    research.get("reason_codes", ())
                ),
                "sector_coverage": _after_hours_sector_coverage(
                    latest.get("candidates")
                ),
                "strategy_coverage": _after_hours_strategy_coverage(
                    latest.get("candidates")
                ),
                "selection_factors": {
                    "broad_ibkr_scanner": True,
                    "sector_diversification": True,
                    "direction_from_close": True,
                    "closing_option_marks": True,
                    "volatility_regime": "REQUIRES_COMPLETE_OPTION_IV_SURFACE",
                    "term_structure": "REQUIRES_MULTIPLE_EXPIRATIONS",
                    "skew": "REQUIRES_DELTA_BOUND_CALL_PUT_SURFACE",
                    "event_and_news": "SUPPORTING_ONLY",
                    "cost_after_ev": "REQUIRES_FRESH_EXECUTABLE_REPRICE",
                },
                "campaign": {
                    "completed_underlyings": len(self._after_hours_research_rows),
                    "target_underlyings": 10,
                    "remaining_underlyings": max(
                        0, 10 - len(self._after_hours_research_rows)
                    ),
                    "basis_bound_underlyings": max(
                        0,
                        len(self._after_hours_research_rows)
                        - len(basis_missing_after_read),
                    ),
                    "remaining_basis_underlyings": len(
                        basis_missing_after_read
                    ),
                    "continue_after_pacing_window": (
                        len(self._after_hours_research_rows) < 10
                        or bool(basis_missing_after_read)
                    ),
                },
                "pacing_usage": _after_hours_pacing_usage(composition),
            }
            best = self._after_hours_indicative_best
            if _should_replace_after_hours_best(
                latest,
                best,
                now=datetime.now(timezone.utc),
            ):
                selected = dict(latest)
                try:
                    formal = self._materialize_after_hours_research_pools(
                        selected,
                        cancel_event=cancel_event,
                        deadline_at=deadline_at,
                        operation_token=operation_token,
                        now=datetime.now(timezone.utc),
                    )
                except _AfterHoursFormalMaterializationConflict:
                    formal = None
                    selected = _after_hours_formal_conflict_payload(selected)
                if formal is not None:
                    selected = _after_hours_with_verified_formal(selected, formal)
                guard = _operation_commit_guard(
                    cancel_event,
                    deadline_at,
                    operation_token,
                )
                try:
                    self._after_hours_store.write(selected, commit_guard=guard)
                except TimeoutError:
                    return unavailable_after_hours_indicative_read_model(
                        "AFTER_HOURS_OPERATION_CANCELLED"
                    )
                if not guard():
                    return unavailable_after_hours_indicative_read_model(
                        "AFTER_HOURS_OPERATION_CANCELLED"
                    )
                self._after_hours_indicative_best = selected
                return selected
            retained = _merge_after_hours_underlying_basis(best, latest)
            try:
                formal = self._materialize_after_hours_research_pools(
                    retained,
                    cancel_event=cancel_event,
                    deadline_at=deadline_at,
                    operation_token=operation_token,
                    now=datetime.now(timezone.utc),
                )
            except _AfterHoursFormalMaterializationConflict:
                formal = None
                retained = _after_hours_formal_conflict_payload(retained)
            if formal is not None:
                retained = _after_hours_with_verified_formal(retained, formal)
            retained = {
                **retained,
                "status": "DEGRADED",
                "reason_codes": list(
                    dict.fromkeys(
                        (
                            *tuple(retained.get("reason_codes", ())),
                            "MORE_COMPLETE_RUNTIME_BATCH_RETAINED",
                        )
                    )
                ),
                "retained_after_less_complete_read_at": latest.get(
                    "observed_at"
                ),
            }
            guard = _operation_commit_guard(
                cancel_event,
                deadline_at,
                operation_token,
            )
            try:
                self._after_hours_store.write(retained, commit_guard=guard)
            except TimeoutError:
                return unavailable_after_hours_indicative_read_model(
                    "AFTER_HOURS_OPERATION_CANCELLED"
                )
            if not guard():
                return unavailable_after_hours_indicative_read_model(
                    "AFTER_HOURS_OPERATION_CANCELLED"
                )
            self._after_hours_indicative_best = retained
            return retained

    def _restore_after_hours_formal_pools(self) -> None:
        """Replay a fresh completed cache into formal research-only stores."""

        with self._after_hours_indicative_lock:
            cached = self._after_hours_indicative_best
            if cached is None:
                return
            try:
                formal = self._materialize_after_hours_research_pools(
                    cached,
                    allow_stale_migration=True,
                )
            except _AfterHoursFormalMaterializationConflict:
                degraded = _after_hours_formal_conflict_payload(cached)
                self._after_hours_store.write(degraded)
                self._after_hours_indicative_best = degraded
                return
            if formal is None:
                return
            restored = _after_hours_with_verified_formal(cached, formal)
            self._after_hours_store.write(restored)
            self._after_hours_indicative_best = restored

    def _materialize_after_hours_research_pools(
        self,
        payload: Mapping[str, object],
        *,
        cancel_event: threading.Event | None = None,
        deadline_at: datetime | None = None,
        operation_token: str | None = None,
        now: datetime | None = None,
        allow_stale_migration: bool = False,
    ) -> dict[str, object] | None:
        """Persist only hash-bound campaign evidence as formal research pools."""

        if _operation_cancelled(cancel_event, deadline_at):
            return None
        materialized_now = now or datetime.now(timezone.utc)
        cache_fresh = _after_hours_cache_fresh(payload, now=materialized_now)
        if not cache_fresh and not allow_stale_migration:
            return None
        campaign_progress = after_hours_campaign_progress_status(payload)
        if campaign_progress is None:
            return None
        raw_candidates = payload.get("candidates")
        if not isinstance(raw_candidates, Sequence) or isinstance(
            raw_candidates, (str, bytes, bytearray)
        ):
            return None
        rows: list[dict[str, object]] = []
        quotes: list[_AfterHoursUnderlyingQuote] = []
        for index, candidate in enumerate(raw_candidates[:10]):
            if not isinstance(candidate, Mapping):
                continue
            quote = _after_hours_underlying_quote(
                candidate.get("underlying_quote_basis"),
                candidate.get("underlying_quote_basis_hash"),
            )
            symbol = str(candidate.get("underlying", "")).strip().upper()
            if not symbol:
                continue
            if quote is not None and quote.symbol != symbol:
                quote = None
            source_scan = str(candidate.get("source_scan", "")).strip().upper()
            if source_scan not in {
                "MOST_ACTIVE",
                "TOP_PERC_GAIN",
                "TOP_PERC_LOSE",
                "CORE_UNIVERSE",
                "LEGACY_AFTER_HOURS_CACHE",
            }:
                source_scan = "MISSING_PROVENANCE"
            rows.append(
                {
                    "symbol": symbol,
                    "rank": index,
                    "source_scan": source_scan,
                    "contract_id": None if quote is None else quote.contract_id,
                    "exchange": None if quote is None else quote.exchange,
                    "industry": candidate.get("sector"),
                    "category": candidate.get("sector"),
                    "security_type": "ETF" if symbol in _AFTER_HOURS_CORE_SECTORS else None,
                }
            )
            if quote is not None:
                quotes.append(quote)
        if not rows:
            return None
        candidate_identity_manifest = after_hours_candidate_identity_manifest(payload)
        if not candidate_identity_manifest:
            return None
        if quotes and cache_fresh:
            campaign_observed_at = max(quote.observed_at for quote in quotes)
        else:
            try:
                campaign_observed_at = datetime.fromisoformat(
                    str(payload.get("observed_at", "")).replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except (TypeError, ValueError):
                return None
        campaign_hash = _after_hours_campaign_hash(payload)
        revision_hash = _after_hours_materialization_revision_hash(
            payload,
            campaign_observed_at=campaign_observed_at,
        )
        prior_formal = payload.get("formal_research_pools")
        materialization_slot = _after_hours_materialization_slot(
            campaign_observed_at,
            revision_hash,
        )
        if (
            isinstance(prior_formal, Mapping)
            and prior_formal.get("materialization_revision_hash") == revision_hash
        ):
            try:
                prior_slot = datetime.fromisoformat(
                    str(prior_formal.get("materialized_at", "")).replace(
                        "Z", "+00:00"
                    )
                ).astimezone(timezone.utc)
            except (TypeError, ValueError):
                return None
            if prior_slot != materialization_slot:
                return None
        basis_missing_count = len(rows) - len(quotes)
        migration_reasons = tuple(
            reason
            for reason, applies in (
                (
                    "AFTER_HOURS_UNDERLYING_BASIS_MIGRATION_PENDING",
                    basis_missing_count > 0,
                ),
                (
                    "AFTER_HOURS_STALE_CACHE_FORMALIZED_AS_EXCLUSIONS",
                    not cache_fresh,
                ),
            )
            if applies
        )
        existing_formal = self._recover_after_hours_formal_pools(
            campaign_hash=campaign_hash,
            revision_hash=revision_hash,
            campaign_observed_at=campaign_observed_at,
            materialization_slot=materialization_slot,
            rows=tuple(rows),
            candidate_identity_manifest=candidate_identity_manifest,
            underlying_basis_bound_count=len(quotes),
            underlying_basis_missing_count=basis_missing_count,
            migration_reasons=migration_reasons,
        )
        if existing_formal is not None:
            return existing_formal
        cache = getattr(self.runtime_services, "equity_evidence_cache", None)
        captured_quotes = tuple(
            quote
            for quote in quotes
            if cache_fresh
            if quote.bid is not None
            and quote.ask is not None
            and timedelta(0)
            <= campaign_observed_at - quote.observed_at
            <= timedelta(minutes=15)
        )
        if cache is not None and captured_quotes:
            for record in captured_records_from_quotes(
                captured_quotes,
                captured_at=campaign_observed_at,
            ):
                if _operation_cancelled(cancel_event, deadline_at):
                    return None
                cache.append(
                    record,
                    commit_guard=_operation_commit_guard(
                        cancel_event,
                        deadline_at,
                        operation_token,
                    ),
                )
        if _operation_cancelled(cancel_event, deadline_at):
            return None
        pacing_usage = payload.get("pacing_usage")
        try:
            build = self.equity_pool.build(
                scanner_rows=tuple(rows),
                slot=materialization_slot,
                pacing_usage=(
                    dict(pacing_usage) if isinstance(pacing_usage, Mapping) else {}
                ),
                commit_guard=_operation_commit_guard(
                    cancel_event,
                    deadline_at,
                    operation_token,
                ),
            )
        except EquityPoolStoreConflict as exc:
            raise _AfterHoursFormalMaterializationConflict(
                "after-hours formal equity-pool slot conflicts with existing lineage"
            ) from exc
        build_payload = build.as_dict()
        if _operation_cancelled(cancel_event, deadline_at):
            return None
        reference = build_payload["equity_pool_reference"]
        assert isinstance(reference, Mapping)
        equity_theses = equity_theses_from_pool_result(
            build,
            equity_pool_reference=reference,
        )
        thesis_rows = equity_theses.get("rows")
        thesis_by_symbol = {
            str(row.get("symbol", "")).strip().upper(): row
            for row in (
                thesis_rows
                if isinstance(thesis_rows, Sequence)
                and not isinstance(thesis_rows, (str, bytes, bytearray))
                else ()
            )
            if isinstance(row, Mapping) and str(row.get("symbol", "")).strip()
        }
        short_leg_risk_evidence = _execution_cost_short_leg_risk_evidence(
            (
                None
                if self.production_composition is None
                else getattr(
                    self.production_composition,
                    "execution_cost_contract",
                    None,
                )
            ),
            observed_at=materialization_slot,
        )
        all_option_candidates = tuple(
            candidate
            for row in raw_candidates[:10]
            if isinstance(row, Mapping)
            for candidate in (
                _after_hours_option_pool_candidate(
                    row,
                    campaign_hash=campaign_hash,
                    observed_at=campaign_observed_at,
                    equity_thesis=thesis_by_symbol.get(
                        str(row.get("underlying", "")).strip().upper()
                    ),
                    short_leg_risk_evidence=short_leg_risk_evidence,
                ),
            )
            if candidate is not None
        )
        thesis_symbols = set(thesis_by_symbol)
        option_candidates = all_option_candidates
        research_only_without_thesis = any(
            isinstance(candidate.get("payload"), Mapping)
            and str(candidate["payload"].get("symbol", "")).strip().upper()
            not in thesis_symbols
            for candidate in option_candidates
        )
        try:
            option_snapshot = self.option_pool.capture_research_candidates(
                scan_run_id=f"after-hours-formal.{revision_hash}",
                candidates=option_candidates,
                observed_at=materialization_slot,
                equity_pool_reference=reference,
                equity_theses=equity_theses,
                reason_codes=(
                    "AFTER_HOURS_RESEARCH_ONLY",
                    "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED",
                    *(
                        ("EQUITY_EXCLUDED_STRUCTURES_RESEARCH_ONLY",)
                        if research_only_without_thesis
                        else ()
                    ),
                    *migration_reasons,
                ),
                commit_guard=_operation_commit_guard(
                    cancel_event,
                    deadline_at,
                    operation_token,
                ),
            )
        except ValueError as exc:
            raise _AfterHoursFormalMaterializationConflict(
                "after-hours formal option-pool slot conflicts with existing lineage"
            ) from exc
        selected = build_payload.get("selected_symbols")
        selected_count = (
            len(selected)
            if isinstance(selected, Sequence)
            and not isinstance(selected, (str, bytes, bytearray))
            else 0
        )
        return _after_hours_formal_pools_projection(
            campaign_hash=campaign_hash,
            revision_hash=revision_hash,
            campaign_observed_at=campaign_observed_at,
            materialization_slot=materialization_slot,
            equity_pool_id=build_payload.get("pool_id"),
            equity_pool_hash=build_payload.get("snapshot_hash"),
            equity_pool_reference=reference,
            equity_research_count=int(build_payload.get("discovery_count", 0)),
            equity_selected_count=selected_count,
            option_pool_scan_run_id=option_snapshot.scan_run_id,
            option_pool_hash=option_snapshot.snapshot_hash,
            option_candidate_identities=tuple(
                decision.candidate_identity
                for decision in option_snapshot.decisions
                if decision.candidate_identity is not None
            ),
            underlying_basis_bound_count=len(quotes),
            underlying_basis_missing_count=basis_missing_count,
            migration_reasons=migration_reasons,
        )

    def _recover_after_hours_formal_pools(
        self,
        *,
        campaign_hash: str,
        revision_hash: str,
        campaign_observed_at: datetime,
        materialization_slot: datetime,
        rows: Sequence[Mapping[str, object]],
        candidate_identity_manifest: tuple[str, ...],
        underlying_basis_bound_count: int,
        underlying_basis_missing_count: int,
        migration_reasons: Sequence[str],
    ) -> dict[str, object] | None:
        """Reuse one exact append-only materialization after a cache rewrite."""

        try:
            # Both ``latest`` implementations verify their complete durable
            # predecessor chains.  Running explicit full-ledger audits first
            # repeated the same canonical decoding without adding authority.
            equity = self.equity_pool.store.latest()
            option = self.option_pool.store.latest()
        except Exception:
            return None
        if equity is None or option is None:
            return None
        expected_scan_run_id = f"after-hours-formal.{revision_hash}"
        option_identities = tuple(
            decision.candidate_identity
            for decision in option.decisions
            if decision.candidate_identity is not None
        )
        expected_option_identities = candidate_identity_manifest
        if (
            equity.snapshot.slot != materialization_slot
            or option.observed_at != materialization_slot
            or option.scan_run_id != expected_scan_run_id
            or option_identities != expected_option_identities
            or len(option_identities) != len(option.decisions)
            or equity.snapshot.discovery_count != len(rows)
        ):
            return None
        references = tuple(
            decision.equity_pool_reference
            for decision in option.decisions
            if isinstance(decision.equity_pool_reference, Mapping)
        )
        if len(references) != len(option.decisions):
            return None
        equity_payload = self.equity_pool.latest_payload()
        reference = equity_payload.get("equity_pool_reference")
        if not isinstance(reference, Mapping):
            return None
        reference_hash = canonical_hash(reference)
        if any(canonical_hash(item) != reference_hash for item in references):
            return None
        discovered_symbols = tuple(
            str(row.get("symbol", "")).strip().upper() for row in rows
        )
        reference_symbols = reference.get("discovered_symbols")
        if (
            not isinstance(reference_symbols, Sequence)
            or isinstance(reference_symbols, (str, bytes, bytearray))
            or any(
                not isinstance(symbol, str) or not symbol.strip()
                for symbol in reference_symbols
            )
            or any(not symbol for symbol in discovered_symbols)
        ):
            return None
        # Equity discovery uses source-priority order, unlike the raw cache.
        # Match unique membership without rewriting its canonical reference.
        discovered_members = set(discovered_symbols)
        reference_members = set(reference_symbols)
        if (
            reference.get("snapshot_id") != equity.snapshot.pool_id
            or reference.get("snapshot_hash") != equity.snapshot_hash
            or reference.get("input_manifest_hash")
            != equity.normalized_inputs_hash
            or reference.get("discovery_count") != equity.snapshot.discovery_count
            or reference.get("selected_count") != len(equity.snapshot.selected)
            or len(discovered_members) != len(discovered_symbols)
            or len(reference_members) != len(reference_symbols)
            or len(reference_symbols) != len(discovered_symbols)
            or reference_members != discovered_members
            or option_identities != expected_option_identities
            or any(
                decision.equity_thesis_evidence is None
                and (
                    decision.disposition is not StructureDisposition.RESEARCH_ONLY
                    or "EQUITY_THESIS_EVIDENCE_UNAVAILABLE"
                    not in decision.reason_codes
                )
                for decision in option.decisions
            )
            or any(
                not isinstance(decision.exact_economics, Mapping)
                or decision.exact_economics.get("campaign_hash") != campaign_hash
                for decision in option.decisions
            )
        ):
            return None
        return _after_hours_formal_pools_projection(
            campaign_hash=campaign_hash,
            revision_hash=revision_hash,
            campaign_observed_at=campaign_observed_at,
            materialization_slot=materialization_slot,
            equity_pool_id=equity.snapshot.pool_id,
            equity_pool_hash=equity.snapshot_hash,
            equity_pool_reference=reference,
            equity_research_count=equity.snapshot.discovery_count,
            equity_selected_count=len(equity.snapshot.selected),
            option_pool_scan_run_id=option.scan_run_id,
            option_pool_hash=option.snapshot_hash,
            option_candidate_identities=option_identities,
            underlying_basis_bound_count=underlying_basis_bound_count,
            underlying_basis_missing_count=underlying_basis_missing_count,
            migration_reasons=migration_reasons,
        )

    def next_session_preparation(
        self,
        calendar: UsOptionsCalendarSnapshot,
        scheduled_for: datetime,
        checked_at: datetime,
        *,
        cancel_event: threading.Event | None = None,
        deadline_at: datetime | None = None,
        operation_token: str | None = None,
        operation_context: ScheduledOperationContext | None = None,
    ) -> Mapping[str, object]:
        """Freeze a read-only handoff for the next eligible research session."""

        if _operation_cancelled(cancel_event, deadline_at):
            return {
                "status": "FAILED",
                "reason_codes": ("NEXT_SESSION_PREPARATION_CANCELLED",),
                "decision": "NO_TRADE",
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }

        current_date = scheduled_for.astimezone(US_OPTIONS_TIMEZONE).date()
        future_sessions = sorted(
            session.open_et.date()
            for session in calendar.sessions
            if session.open_et.date() > current_date
        )
        next_trading_date = future_sessions[0] if future_sessions else None
        reasons: list[str] = []
        if next_trading_date is None:
            reasons.append("NEXT_SESSION_CALENDAR_UNAVAILABLE")
        materialization_conflict = False
        with self._after_hours_indicative_lock:
            cached_after_hours = self._after_hours_indicative_best
            if cached_after_hours is not None:
                try:
                    formal = self._materialize_after_hours_research_pools(
                        cached_after_hours,
                        cancel_event=cancel_event,
                        deadline_at=deadline_at,
                        operation_token=operation_token,
                        now=checked_at,
                    )
                except _AfterHoursFormalMaterializationConflict:
                    formal = None
                    materialization_conflict = True
                    cached_after_hours = _after_hours_formal_conflict_payload(
                        cached_after_hours
                    )
                    reasons.append("AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT")
                if formal is not None:
                    cached_after_hours = _after_hours_with_verified_formal(
                        cached_after_hours,
                        formal,
                    )
                    guard = _operation_commit_guard(
                        cancel_event,
                        deadline_at,
                        operation_token,
                    )
                    try:
                        self._after_hours_store.write(
                            cached_after_hours,
                            commit_guard=guard,
                        )
                    except TimeoutError:
                        return {
                            "status": "FAILED",
                            "reason_codes": ("NEXT_SESSION_PREPARATION_CANCELLED",),
                            "decision": "NO_TRADE",
                            "decision_authority": "SUPPORTING_ONLY",
                            "approval_eligible": False,
                            "instruction_creation_allowed": False,
                            "order_allowed": False,
                        }
                    if not guard():
                        return {
                            "status": "FAILED",
                            "reason_codes": ("NEXT_SESSION_PREPARATION_CANCELLED",),
                            "decision": "NO_TRADE",
                            "decision_authority": "SUPPORTING_ONLY",
                            "approval_eligible": False,
                            "instruction_creation_allowed": False,
                            "order_allowed": False,
                        }
                    self._after_hours_indicative_best = cached_after_hours
        equity = (
            {}
            if materialization_conflict
            else self.equity_pool.latest_payload()
        )
        option = (
            {}
            if materialization_conflict
            else self.option_pool.latest_payload()
        )
        ranking = (
            {}
            if materialization_conflict
            else self.runtime_services.latest_ranking()
        )
        selected = equity.get("selected_symbols")
        option_decisions = option.get("decisions")
        research_watchlist = ranking.get("research_watchlist")
        equity_research_count = equity.get("discovery_count")
        if (
            isinstance(equity_research_count, bool)
            or not isinstance(equity_research_count, int)
            or equity_research_count < 0
        ):
            equity_research_count = 0
        selected_count = len(selected) if isinstance(selected, Sequence) else 0
        option_count = (
            len(option_decisions)
            if isinstance(option_decisions, Sequence)
            and not isinstance(option_decisions, (str, bytes, bytearray))
            else 0
        )
        watchlist_count = (
            len(research_watchlist)
            if isinstance(research_watchlist, Sequence)
            and not isinstance(research_watchlist, (str, bytes, bytearray))
            else 0
        )
        premarket_parent_eligible_structure_count = 0
        if next_trading_date is not None and not materialization_conflict:
            next_premarket_slot = datetime(
                next_trading_date.year,
                next_trading_date.month,
                next_trading_date.day,
                9,
                20,
                tzinfo=US_OPTIONS_TIMEZONE,
            )

            class _NoPremarketFallback:
                @staticmethod
                def resolve_top10(*, scheduled_for: datetime) -> tuple[object, ...]:
                    del scheduled_for
                    return ()

            try:
                parent_resolution = DurableOptionPoolTop10StructureSource(
                    self.option_pool_store,
                    fallback=_NoPremarketFallback(),
                    maximum_snapshot_contracts=14,
                ).resolve_top10(scheduled_for=next_premarket_slot)
                premarket_parent_eligible_structure_count = min(
                    10,
                    len(tuple(parent_resolution.structures)),
                )
            except Exception:
                # Preparation is a read model.  Any store or validation failure
                # must remain a zero-parent, research-only degraded result.
                premarket_parent_eligible_structure_count = 0
        if equity_research_count == 0 and not materialization_conflict:
            reasons.append("EQUITY_POOL_EMPTY_OR_UNAVAILABLE")
        if option_count == 0 and not materialization_conflict:
            reasons.append("OPTION_POOL_EMPTY_OR_UNAVAILABLE")
        if (
            premarket_parent_eligible_structure_count == 0
            and not materialization_conflict
        ):
            reasons.append("PREMARKET_PARENT_ELIGIBLE_STRUCTURE_UNAVAILABLE")
        after_hours_formal = (
            cached_after_hours.get("formal_research_pools")
            if not materialization_conflict
            and isinstance(cached_after_hours, Mapping)
            else None
        )
        if (
            isinstance(cached_after_hours, Mapping)
            and after_hours_formal is not None
            and not _after_hours_formal_descriptor_valid(
                after_hours_formal,
                payload=cached_after_hours,
                equity=equity,
                option=option,
            )
        ):
            after_hours_formal = None
            reasons.append("AFTER_HOURS_FORMAL_DESCRIPTOR_INVALID")
        campaign = (
            cached_after_hours.get("campaign")
            if isinstance(cached_after_hours, Mapping)
            else None
        )
        if (
            isinstance(cached_after_hours, Mapping)
            and after_hours_campaign_progress_status(cached_after_hours)
            == "PARTIAL"
        ):
            reasons.append("AFTER_HOURS_INDICATIVE_PARTIAL")
        preparation = {
            "schema": "options_copilot.next_session_preparation.v1",
            "status": (
                "READY"
                if next_trading_date is not None and not reasons
                else "DEGRADED"
            ),
            "prepared_at": checked_at.astimezone(timezone.utc).isoformat(),
            "scheduled_for": scheduled_for.isoformat(),
            "next_trading_date": (
                None if next_trading_date is None else next_trading_date.isoformat()
            ),
            "equity_research_count": equity_research_count,
            "equity_selected_count": selected_count,
            "option_research_structure_count": option_count,
            "option_structure_count": option_count,
            "premarket_parent_eligible_structure_count": (
                premarket_parent_eligible_structure_count
            ),
            "executable_count": 0,
            "research_watchlist_count": watchlist_count,
            "equity_pool_hash": equity.get("snapshot_hash"),
            "option_pool_hash": option.get("snapshot_hash"),
            "after_hours_campaign_hash": (
                after_hours_formal.get("campaign_hash")
                if isinstance(after_hours_formal, Mapping)
                else None
            ),
            "after_hours_campaign": (
                dict(campaign) if isinstance(campaign, Mapping) else None
            ),
            "after_hours_formal_research_pools": (
                dict(after_hours_formal)
                if isinstance(after_hours_formal, Mapping)
                else None
            ),
            "ranking_snapshot_id": ranking.get("ranking_snapshot_id"),
            "reason_codes": tuple(dict.fromkeys(reasons)),
            "decision": "NO_TRADE",
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }
        producer = self.scheduled_history_producer
        if producer is not None:
            # The original preparation outcome remains unchanged. A source
            # child's success cannot mask empty/conflicting research pools.
            symbols: list[str] = []
            if isinstance(selected, Sequence) and not isinstance(selected, (str, bytes)):
                symbols.extend(value for value in selected if isinstance(value, str))
            if isinstance(option_decisions, Sequence) and not isinstance(option_decisions, (str, bytes)):
                for decision in option_decisions:
                    if isinstance(decision, Mapping):
                        symbol = decision.get("symbol", decision.get("underlying"))
                        if isinstance(symbol, str):
                            symbols.append(symbol)
            symbols.extend(self.config.news_core_symbols)
            preparation["historical_sources"] = producer.run(
                calendar, scheduled_for, operation_context=operation_context,
                symbols=tuple(dict.fromkeys(symbols)),
            )
        return preparation

    def verified_after_hours_payload(self) -> Mapping[str, object]:
        """Read and verify the passive cache before scanner reconciliation."""

        try:
            persisted = self._after_hours_store.read()
        except RuntimeError:
            return {}
        if persisted is None:
            return {}
        if after_hours_campaign_progress_status(persisted) is None:
            return {}
        formal = persisted.get("formal_research_pools")
        equity = self.equity_pool.latest_payload()
        option = self.option_pool.latest_payload()
        if not _after_hours_formal_descriptor_valid(
            formal,
            payload=persisted,
            equity=equity,
            option=option,
        ):
            return {}
        return persisted

    def after_hours_latest(self) -> Mapping[str, object]:
        """Return the durable closing read model without any broker request."""

        with self._after_hours_indicative_lock:
            if self._after_hours_indicative_best is None:
                return unavailable_after_hours_indicative_read_model(
                    "AFTER_HOURS_CAMPAIGN_NOT_RUN"
                )
            return _project_after_hours_passive_freshness(
                self._after_hours_indicative_best,
                now=datetime.now(timezone.utc),
            )

    def immediate_scan(
        self,
        scope: str = "CORE_CAMPAIGN",
    ) -> Mapping[str, object]:
        """Run one user-requested scan on the owned read-only broker runtime."""

        checked_scope = str(scope).strip().upper()
        if checked_scope not in {"CORE_CAMPAIGN", "BOUNDED_MARKET"}:
            return {
                **_no_trade_read_model("IMMEDIATE_SCAN_SCOPE_INVALID"),
                "scan_run_id": None,
            }
        # Keep scope allocation, the broker read, and campaign recording in
        # one order.  The scanner loop serializes broker ticks, but without
        # this outer boundary two callers could allocate WARMUP/REEVALUATION
        # scopes and then acquire the scanner lock in the opposite order.
        with self._immediate_scan_lock:
            if self._closed or self._closing:
                return {
                    **_no_trade_read_model("RUNTIME_CLOSING"),
                    "scan_run_id": None,
                }
            if checked_scope == "BOUNDED_MARKET":
                return self._immediate_bounded_market_scan_unlocked()
            return self._immediate_scan_unlocked()

    def _immediate_bounded_market_scan_unlocked(self) -> Mapping[str, object]:
        """Run one full pacing-bounded market funnel without broker writes."""

        composition = self.production_composition
        if composition is None:
            return {
                **_no_trade_read_model("IMMEDIATE_SCAN_UNAVAILABLE"),
                "scan_run_id": None,
            }
        runner = getattr(composition.lifecycle.scanner_loop, "run_now", None)
        if not callable(runner):
            return {
                **_no_trade_read_model("IMMEDIATE_SCAN_UNAVAILABLE"),
                "scan_run_id": None,
            }
        result = runner()
        scan_run_id = getattr(result, "scan_run_id", None)
        status = str(getattr(result, "status", "NO_TRADE") or "NO_TRADE").upper()
        reason = str(getattr(result, "duplicate_reason", "") or "").strip().upper()
        if status != "COMPLETED" or not isinstance(scan_run_id, str):
            return {
                **_no_trade_read_model(reason or "IMMEDIATE_SCAN_FAILED"),
                "scan_run_id": scan_run_id,
                "scan_status": status,
                "manual_read_only_scan": True,
                "scan_scope": "BOUNDED_MARKET",
            }
        ranking = dict(self.latest_ranking())
        if ranking.get("scan_run_id") != scan_run_id:
            return {
                **_no_trade_read_model("IMMEDIATE_SCAN_RANKING_NOT_CURRENT"),
                "scan_run_id": scan_run_id,
                "scan_status": status,
                "manual_read_only_scan": True,
                "scan_scope": "BOUNDED_MARKET",
            }
        return {
            **ranking,
            "scan_status": status,
            "manual_read_only_scan": True,
            "scan_scope": "BOUNDED_MARKET",
            "decision_authority": "OBSERVATION_ONLY",
            "approval_enabled": False,
            "review_only": True,
            "direct_order_submission": False,
        }

    def _immediate_scan_unlocked(self) -> Mapping[str, object]:
        """Run one immediate scan while the caller owns the sequence lock."""

        composition = self.production_composition
        if composition is None:
            return {
                **_no_trade_read_model("IMMEDIATE_SCAN_UNAVAILABLE"),
                "scan_run_id": None,
            }
        runner = getattr(composition.lifecycle.scanner_loop, "run_now", None)
        if not callable(runner):
            return {
                **_no_trade_read_model("IMMEDIATE_SCAN_UNAVAILABLE"),
                "scan_run_id": None,
            }
        manual_scope = getattr(composition.pipeline_inputs, "manual_core_only", None)
        if not callable(manual_scope):
            return {
                **_no_trade_read_model("IMMEDIATE_SCAN_SCOPE_UNAVAILABLE"),
                "scan_run_id": None,
            }
        started_at = datetime.now(timezone.utc)
        scope: dict[str, object] = {}
        retry_attempted = False

        def retry_scope_once(*, suppress_error: bool) -> bool:
            nonlocal retry_attempted
            if retry_attempted or not scope:
                return False
            retry_attempted = True
            retry_scope = getattr(
                composition.pipeline_inputs,
                "retry_manual_core_attempt",
                None,
            )
            if not callable(retry_scope):
                return False
            try:
                return bool(retry_scope(scope))
            except Exception:
                if suppress_error:
                    return False
                raise

        try:
            with manual_scope() as raw_scope:
                scope = dict(raw_scope) if isinstance(raw_scope, Mapping) else {}
                result = runner()
            scan_run_id = getattr(result, "scan_run_id", None)
            status = str(
                getattr(result, "status", "NO_TRADE") or "NO_TRADE"
            ).upper()
            reason = str(
                getattr(result, "duplicate_reason", "") or ""
            ).strip().upper()
            if status != "COMPLETED" or not isinstance(scan_run_id, str):
                response = {
                    **_no_trade_read_model(reason or "IMMEDIATE_SCAN_FAILED"),
                    "scan_run_id": scan_run_id,
                    "scan_status": status,
                    "manual_read_only_scan": True,
                }
            else:
                ranking = dict(self.latest_ranking())
                if ranking.get("scan_run_id") != scan_run_id:
                    response = {
                        **_no_trade_read_model("IMMEDIATE_SCAN_RANKING_NOT_CURRENT"),
                        "scan_run_id": scan_run_id,
                        "scan_status": status,
                        "manual_read_only_scan": True,
                    }
                else:
                    response = {
                        **ranking,
                        "scan_status": status,
                        "manual_read_only_scan": True,
                        "decision_authority": "OBSERVATION_ONLY",
                        "approval_enabled": False,
                        "review_only": True,
                        "direct_order_submission": False,
                    }
            if (
                not _consumed_immediate_scan_attempt(response)
                and retry_scope_once(suppress_error=False)
            ):
                scope["next_symbol"] = scope.get("target_symbol")
            self._record_immediate_scan_attempt(
                scope=scope,
                response=response,
                started_at=started_at,
            )
        except Exception:
            # Scope allocation advances the process-local cursor.  Any failure
            # before the durable scan and campaign evidence are both complete
            # must retain the same WARMUP/REEVALUATION attempt.
            retry_scope_once(suppress_error=True)
            raise
        return {
            **response,
            "campaign": self.immediate_scan_campaign(),
        }

    def immediate_scan_campaign(self) -> Mapping[str, object]:
        """Return a detached diagnostic index over this process's manual scans."""

        with self._immediate_campaign_lock:
            attempts = tuple(dict(item) for item in self._immediate_campaign_attempts)
            campaign_key = self._immediate_campaign_key
            campaign_id = self._immediate_campaign_id
            started_at = self._immediate_campaign_started_at
            updated_at = self._immediate_campaign_updated_at
            target_count = self._immediate_campaign_target_count
            next_symbol = self._immediate_campaign_next_symbol
        if campaign_id is None:
            return {
                "schema_version": "options_copilot.immediate_scan_campaign.v1",
                "status": "NOT_RUN",
                "decision": "NO_TRADE",
                "reason_codes": ("IMMEDIATE_SCAN_CAMPAIGN_NOT_RUN",),
                "campaign_id": None,
                "attempt_count": 0,
                "completed_symbol_count": 0,
                "target_symbol_count": 0,
                "next_symbol": None,
                "attempts": (),
                "decision_authority": "OBSERVATION_ONLY",
                "approval_enabled": False,
                "review_only": True,
                "direct_order_submission": False,
            }
        completed_symbols = {
            str(item.get("target_symbol") or "").strip().upper()
            for item in attempts
            if _completed_campaign_attempt(item)
            and str(item.get("target_symbol") or "").strip()
        }
        latest = attempts[-1] if attempts else {}
        latest_reasons = _bounded_string_values(
            latest.get("reason_codes"),
            maximum=32,
        )
        candidate_count = sum(
            int(item.get("candidate_count") or 0)
            for item in attempts
            if isinstance(item.get("candidate_count"), int)
            and not isinstance(item.get("candidate_count"), bool)
        )
        latest_candidate_count = (
            int(latest.get("candidate_count") or 0)
            if isinstance(latest.get("candidate_count"), int)
            and not isinstance(latest.get("candidate_count"), bool)
            else 0
        )
        if any("PACING" in reason for reason in latest_reasons):
            status = "PAUSED_PACING"
        elif latest_candidate_count > 0:
            status = "CANDIDATES_OBSERVED_REQUIRES_CURRENT_RANKING"
        elif target_count > 0 and len(completed_symbols) >= target_count:
            status = "NO_TRADE_COMPLETE"
        else:
            status = "RUNNING_NO_TRADE"
        return {
            "schema_version": "options_copilot.immediate_scan_campaign.v1",
            "status": status,
            "decision": "NO_TRADE",
            "reason_codes": latest_reasons,
            "campaign_id": campaign_id,
            "trading_date": campaign_key[0]
            if campaign_key is not None
            else None,
            "started_at": started_at.isoformat() if started_at is not None else None,
            "updated_at": updated_at.isoformat() if updated_at is not None else None,
            "attempt_count": len(attempts),
            "completed_symbol_count": len(completed_symbols),
            "target_symbol_count": target_count,
            "candidate_observation_count": candidate_count,
            "next_symbol": next_symbol,
            "attempts": attempts,
            "decision_authority": "OBSERVATION_ONLY",
            "approval_enabled": False,
            "review_only": True,
            "direct_order_submission": False,
        }

    def _record_immediate_scan_attempt(
        self,
        *,
        scope: Mapping[str, object],
        response: Mapping[str, object],
        started_at: datetime,
    ) -> None:
        trading_date = started_at.astimezone(US_OPTIONS_TIMEZONE).date().isoformat()
        cycle = _nonnegative_int(scope.get("cycle")) or 1
        campaign_key = (trading_date, cycle)
        reason_codes = _bounded_string_values(response.get("reasons"), maximum=32)
        raw_candidates = response.get("candidates")
        candidate_count = (
            min(10, len(raw_candidates))
            if isinstance(raw_candidates, Sequence)
            and not isinstance(raw_candidates, (str, bytes, bytearray))
            else 0
        )
        target_symbol = _bounded_symbol(scope.get("target_symbol"))
        attempt_kind = str(scope.get("attempt_kind") or "UNKNOWN").strip().upper()
        if attempt_kind not in {"WARMUP", "REEVALUATION"}:
            attempt_kind = "UNKNOWN"
        recorded_at = _bounded_timestamp(response.get("recorded_at"))
        finished_at = datetime.now(timezone.utc)
        row: dict[str, object] = {
            "target_symbol": target_symbol,
            "attempt_number": _nonnegative_int(scope.get("attempt_number")),
            "attempt_kind": attempt_kind,
            "scan_run_id": _bounded_identifier(response.get("scan_run_id")),
            "scan_status": str(response.get("scan_status") or "UNKNOWN").strip().upper(),
            "decision": str(response.get("decision") or "NO_TRADE").strip().upper(),
            "reason_codes": reason_codes,
            "stopped_at_gate": _scan_gate_from_reasons(reason_codes),
            "candidate_count": candidate_count,
            "ranking_snapshot_id": _bounded_identifier(
                response.get("ranking_snapshot_id")
            ),
            "decision_hash": _bounded_hash(response.get("decision_hash")),
            "record_hash": _bounded_hash(response.get("record_hash")),
            "gate_bundle_hash": _bounded_hash(response.get("gate_bundle_hash")),
            "started_at": started_at.isoformat(),
            "recorded_at": recorded_at,
            "finished_at": finished_at.isoformat(),
            "review_only": True,
            "direct_order_submission": False,
        }
        target_count = _nonnegative_int(scope.get("core_count")) or 0
        next_symbol = _bounded_symbol(scope.get("next_symbol"))
        with self._immediate_campaign_lock:
            if self._immediate_campaign_key != campaign_key:
                first_scan = row.get("scan_run_id") or started_at.isoformat()
                digest = hashlib.sha256(
                    f"{trading_date}|{cycle}|{first_scan}".encode("utf-8")
                ).hexdigest()[:24]
                self._immediate_campaign_key = campaign_key
                self._immediate_campaign_id = f"manual-scan-campaign.{digest}"
                self._immediate_campaign_started_at = started_at
                self._immediate_campaign_attempts = []
            self._immediate_campaign_updated_at = finished_at
            self._immediate_campaign_target_count = target_count
            self._immediate_campaign_next_symbol = next_symbol
            self._immediate_campaign_attempts.append(row)
            del self._immediate_campaign_attempts[:-100]

    def provider_configuration(self) -> Mapping[str, object]:
        """Return configuration plus effective runtime load state, never values."""

        current_status = dict(
            provider_configuration_status(self.provider_key_store)
        )
        current_fingerprints = _provider_value_fingerprints(
            self.provider_key_store
        )
        current_jin10_activation = (
            resolve_jin10_credential(
                LocalJin10EnvelopeReader(
                    self.provider_key_store,
                    self.jin10_binding_store,
                ),
                jin10_rotation_evidence_dir(self.config.data_dir),
            ).status
            == "ACTIVATED"
        )
        result: dict[str, object] = {}
        for key, status in current_status.items():
            startup = self._provider_startup_state[key]
            activated = (
                current_jin10_activation
                if key == "jin10_mcp_token"
                else True
            )
            restart_required = bool(
                status != startup["status"]
                or current_fingerprints[key]
                != self._provider_startup_fingerprints[key]
                or (
                    key == "jin10_mcp_token"
                    and activated != startup["activated"]
                )
            )
            composed = bool(startup["composed"])
            result[key] = {
                "status": status,
                "activated": activated,
                "composed": composed,
                "runtime_loaded": bool(
                    status == "CONFIGURED"
                    and composed
                    and activated
                    and not restart_required
                ),
                "restart_required": restart_required,
            }
        return result

    def _connector_health(self, now: datetime) -> Mapping[str, object]:
        control = self._current_control_projection()
        snapshot = None
        connector = None
        if control is not None:
            status = str(control.get("status", "UNAVAILABLE")).upper()
            connector = {
                "status": status,
                "stale": status == "STALE",
                "age_ms": control.get("age_ms"),
                "asof": control.get("observed_at"),
                "message": control.get("reason") or "IBKR_READONLY_CONTROL",
            }
        else:
            try:
                snapshot = self._current_snapshot()
            except RuntimeError:
                snapshot = None
                connector = {
                    "status": "DOWN",
                    "stale": True,
                    "message": "The sanitized IBKR snapshot is unreadable.",
                }
        if snapshot is None:
            if connector is None:
                connector = {
                    "status": "DISCONNECTED",
                    "stale": True,
                    "message": "No managed connector or local IBKR snapshot has been ingested.",
                }
        elif connector is None:
            age_seconds = (now - snapshot.observed_at).total_seconds()
            stale = (
                age_seconds < 0
                or age_seconds > self.config.control_snapshot_stale_seconds
            )
            connector = {
                "status": "STALE" if stale else "UP",
                "stale": stale,
                "age_ms": int(abs(age_seconds) * 1000),
                "asof": snapshot.observed_at.isoformat(),
                "message": (
                    "Snapshot timestamp is in the future."
                    if age_seconds < 0
                    else snapshot.source
                ),
            }
        return connector

    def health_summary(self) -> Mapping[str, object]:
        """Return live control/runtime state without deep durable-ledger audits."""

        connector = self._connector_health(datetime.now(timezone.utc))
        production = (
            self.production_composition.lifecycle.summary()
            if self.production_composition is not None
            else self.external_top10_composition.health()
            if self.external_top10_composition is not None
            else {
                "status": "DEGRADED",
                "decision": "NO_TRADE",
                "reasons": (self._production_composition_reason,),
                "review_only": True,
                "direct_order_submission": False,
            }
        )
        return {
            "dependencies": {
                "ibkr_snapshot": connector,
                "decision_runtime": self.runtime_services.readiness(),
                "shutdown": dict(self._shutdown_health),
                "production_scanner": production,
            }
        }

    def health(self) -> Mapping[str, object]:
        connector = self._connector_health(datetime.now(timezone.utc))
        pending = self.bridge.list_pending(limit=500)
        bridge_status = (
            "ATTENTION"
            if any(item.status is BridgeStatus.AUTHORIZED for item in pending)
            else "PENDING"
            if pending
            else "HANDOFF_ONLY"
        )
        return {
            "dependencies": {
                "ibkr_snapshot": connector,
                "learning_ledger": {
                    "status": "UP" if self.ledger.verify_integrity() else "DOWN",
                    "stale": False,
                },
                "instruction_bridge": {
                    "status": bridge_status,
                    "paper": False,
                    "message": (
                        "Local claim is authorized; never retry an uncertain external call."
                        if bridge_status == "ATTENTION"
                        else "Local handoff only; the external Codex connector creates review instructions."
                    ),
                },
                "news_research": self.news.health(),
                "top10_preselection_ledger": self.preselection_provider.ledger_status(),
                "decision_runtime": self.runtime_services.readiness(),
                "shutdown": dict(self._shutdown_health),
                "production_scanner": (
                    self.production_composition.lifecycle.health()
                    if self.production_composition is not None
                    else self.external_top10_composition.health()
                    if self.external_top10_composition is not None
                    else {
                        "status": "DEGRADED",
                        "decision": "NO_TRADE",
                        "reasons": (self._production_composition_reason,),
                        "review_only": True,
                        "direct_order_submission": False,
                    }
                ),
            }
        }

    def bootstrap(self) -> Mapping[str, object]:
        control = self._current_control_projection()
        if control is not None:
            return self._control_bootstrap(control)
        snapshot, projection_reason = self._current_projection_snapshot()
        if snapshot is None:
            return {
                "asof": (
                    None
                    if self._snapshot is None
                    else self._snapshot.observed_at.isoformat()
                ),
                "source": None if self._snapshot is None else self._snapshot.source,
                "account": {"status": "UNAVAILABLE"},
                "campaign": {"target_nlv_usd": 10000, "progress_fraction": 0},
                "warnings": [
                    projection_reason or "SNAPSHOT_UNAVAILABLE",
                    "IBKR account and position values are hidden until a fresh, "
                    "complete control snapshot is available.",
                ],
                "safety": self._safety(),
            }
        account = dict(snapshot.account)
        campaign = dict(snapshot.campaign)
        warnings = list(snapshot.warnings)
        if projection_reason is not None:
            projection_status = (
                "STALE" if projection_reason == "SNAPSHOT_STALE" else "PARTIAL"
            )
            account["status"] = projection_status
            account["decision_authority"] = "LAST_KNOWN_ONLY"
            if projection_reason == "SNAPSHOT_STALE":
                # The snapshot proves what the broker reported at ``asof``; it
                # cannot prove the current session, reconciliation, or market-
                # data state.  Preserve historical NLV and timestamps while
                # preventing the operator GUI from presenting those old flags
                # as current runtime truth.
                account["connected"] = None
                account["reconciled"] = None
                account["market_data_status"] = None
            campaign["status"] = projection_status
            campaign["decision_authority"] = "LAST_KNOWN_ONLY"
            warnings = [
                projection_reason,
                "Displayed values are last-known observations only and cannot "
                "authorize scans, risk, approval, creator transport, or orders.",
                *warnings,
            ]
        return {
            "asof": snapshot.observed_at.isoformat(),
            "source": snapshot.source,
            "account": account,
            "campaign": campaign,
            "warnings": warnings,
            "safety": self._safety(),
        }

    def candidates(self) -> Mapping[str, object]:
        """Compatibility view of connector JSON with zero decision authority."""

        snapshot, projection_reason = self._current_projection_snapshot()
        rows = [] if snapshot is None else [dict(item) for item in snapshot.candidates]
        reason = projection_reason or (
            "Legacy runtime_snapshot.json candidates are observation-only; only "
            "immutable RankingStore rows can be authorized."
        )
        for item in rows[:10]:
            item.pop("approval_challenge", None)
            item.pop("approval_expires_at", None)
            item["eligible_to_send"] = False
            item["approval_enabled"] = False
            item["decision_authority"] = "OBSERVATION_ONLY"
            item["approval_blocked_reason"] = reason
            if projection_reason is not None:
                item["freshness_status"] = (
                    "STALE"
                    if projection_reason == "SNAPSHOT_STALE"
                    else "PARTIAL"
                )
                item["decision_authority"] = "LAST_KNOWN_ONLY"
        return {
            "decision": "NO_TRADE",
            "candidates": rows[:10],
            "approval_enabled": False,
            "decision_authority": "OBSERVATION_ONLY",
            "reason": reason,
            "review_only": True,
            "direct_order_submission": False,
        }

    def latest_scan(self) -> Mapping[str, object]:
        return self.runtime_services.latest_scan()

    def latest_ranking(self) -> Mapping[str, object]:
        return self.runtime_services.latest_ranking()

    def ranking(self, ranking_snapshot_id: str) -> Mapping[str, object]:
        return self.runtime_services.ranking(ranking_snapshot_id)

    def candidate_evidence(
        self, scan_run_id: str, candidate_id: str
    ) -> Mapping[str, object]:
        return self.runtime_services.candidate_evidence(scan_run_id, candidate_id)

    def management(self) -> Mapping[str, object]:
        return self.runtime_services.management()

    def positioning(self) -> Mapping[str, object]:
        return self.runtime_services.positioning()

    def rank_one_challenge(
        self, ranking_snapshot_id: str, candidate_id: str
    ) -> Mapping[str, object]:
        if not self.runtime_services.approval_enabled:
            reasons = ", ".join(self.runtime_services.readiness()["reasons"])
            raise OptionsCopilotUnavailable(f"NO_TRADE: {reasons}")
        ranking = self.runtime_services.ranking(ranking_snapshot_id)
        matches = [
            row
            for row in ranking.get("candidates", ())
            if isinstance(row, Mapping) and row.get("candidate_id") == candidate_id
        ]
        if len(matches) == 1 and (
            matches[0].get("rank") != 1
            or matches[0].get("authorizable") is not True
            or matches[0].get("authority_status") == "A_GRADE_PENDING"
        ):
            raise RankOneAuthorizationForbidden(
                "VIEW_ONLY: only the current frozen authorizable rank 1 can create a challenge"
            )
        if ranking.get("approval_enabled") is not True:
            raise ProposalApprovalConflict(
                "ranking, terminal, policy, risk, cost, broker, or Strategy NAV authority changed"
            )
        if len(matches) != 1:
            raise RankOneAuthorizationForbidden(
                "VIEW_ONLY: candidate is not present in the frozen ranking"
            )
        result = self.runtime_services.create_rank_one_challenge(
            ranking_snapshot_id, candidate_id
        )
        if result is None:
            raise ProposalApprovalConflict(
                "ranking, policy, risk, cost, broker, or Strategy NAV authority changed"
            )
        return result

    def confirm_challenge(
        self, challenge_id: str, confirmation: Mapping[str, object]
    ) -> Mapping[str, object]:
        if not self.runtime_services.approval_enabled:
            reasons = ", ".join(self.runtime_services.readiness()["reasons"])
            raise OptionsCopilotUnavailable(f"NO_TRADE: {reasons}")
        result = self.runtime_services.confirm_challenge(
            challenge_id, confirmation
        )
        if result is None:
            raise ProposalApprovalConflict(
                "approval challenge is invalid, expired, or no longer current"
            )
        return result

    def positions(self) -> Mapping[str, object]:
        control = self._current_control_projection()
        if control is not None:
            status = str(control.get("status", "UNAVAILABLE")).upper()
            rows = control.get("positions", ())
            positions = (
                [dict(item) for item in rows if isinstance(item, Mapping)]
                if isinstance(rows, Sequence)
                and not isinstance(rows, (str, bytes, bytearray, memoryview))
                else []
            )
            stale = status == "STALE"
            return {
                "positions": positions,
                "status": status,
                "position_state_known": not stale,
                "decision_authority": (
                    "LAST_KNOWN_ONLY" if status in {"STALE", "PARTIAL"}
                    else "OBSERVATION_ONLY"
                ),
                "asof": control.get("observed_at"),
                "reason": control.get("reason"),
            }
        snapshot, projection_reason = self._current_projection_snapshot()
        if snapshot is None:
            cached = self._snapshot
            return {
                "positions": [],
                "status": "UNAVAILABLE",
                "position_state_known": False,
                "asof": None if cached is None else cached.observed_at.isoformat(),
                "reason": projection_reason or "SNAPSHOT_UNAVAILABLE",
            }
        if projection_reason is not None:
            stale = projection_reason == "SNAPSHOT_STALE"
            return {
                "positions": [dict(item) for item in snapshot.positions],
                "status": "STALE" if stale else "PARTIAL",
                "position_state_known": not stale,
                "decision_authority": "LAST_KNOWN_ONLY",
                "asof": snapshot.observed_at.isoformat(),
                "reason": projection_reason,
            }
        return {
            "positions": [dict(item) for item in snapshot.positions],
            "status": "CURRENT",
            "position_state_known": True,
            "decision_authority": "OBSERVATION_ONLY",
            "asof": snapshot.observed_at.isoformat(),
            "reason": None,
        }

    def learning_status(self) -> Mapping[str, object]:
        champion = self.learning.current_champion()
        decisions = self.ledger.count()
        verified_snapshot = self.shadow_learning.verified_replay_snapshot()
        projection = self._learning_projection(verified_snapshot)
        state = projection.governance_state
        challengers = projection.challengers
        counts = dict(projection.record_counts)
        public_counts = {
            "THESIS": int(counts.get("theses", 0)),
            "EVIDENCE": int(counts.get("evidence", 0)),
            "PREDICTION": int(counts.get("predictions", 0)),
            "OUTCOME": int(counts.get("outcomes", 0)),
        }
        governance = self._learning_governance_read_model(state)
        outcome_horizons = projection.outcome_horizons
        shadow_exclusions = projection.shadow_exclusions
        return {
            "champion": champion,
            "challenger": state.challenger_version,
            "stage": state.stage.value,
            "decision_records": decisions,
            "automatic_production_promotion": False,
            "a_grade_unlocked": False,
            "minimum_discovery_scenarios": DISCOVERY_SAMPLE_THRESHOLD,
            "message": (
                "30 independent scenarios permit discovery only; production and "
                "15% A-grade risk require a report and explicit human approval."
            ),
            "governance": governance,
            "creator_transport_status": creator_unavailable_reason(),
            "outcome_capture": self._outcome_capture_status(),
            "outcome_processing": self._outcome_processing_status(),
            "outcome_horizons": outcome_horizons,
            "shadow_learning": {
                "status": "VERIFIED",
                "mode": state.mode,
                "stage": state.stage.value,
                "grade": state.grade,
                "independent_samples": state.independent_samples,
                "minimum_discovery_scenarios": state.discovery_threshold,
                "discovery_ready": state.discovery_ready,
                "selected_challenger": state.challenger_version,
                "challengers": list(challengers),
                "record_counts": public_counts,
                "record_count": sum(public_counts.values()),
                "prediction_contract": {
                    "challenger": NEWS_SHADOW_CHALLENGER_VERSION,
                    "legacy_excluded_count": shadow_exclusions["total"],
                    "exclusion_reasons": shadow_exclusions["reasons"],
                    "decision_authority": "SUPPORTING_ONLY",
                },
                "ledger": {
                    "schema_version": self.shadow_learning.schema_version,
                    "journal_mode": self.shadow_learning.journal_mode,
                    "integrity_verified": outcome_horizons.get("status") == "READY",
                },
                "authority": {
                    "can_auto_promote": False,
                    "can_change_production_weights": False,
                    "can_change_production_rules": False,
                    "a_grade_15_percent_unlocked": False,
                    "approval_authority": False,
                    "bridge_authority": False,
                    "order_authority": False,
                    "external_human_approval_required": True,
                },
                "queries": {
                    "records": "/api/learning/records",
                    "record": "/api/learning/records/{record_id}",
                    "replay": (
                        "/api/learning/predictions/{prediction_id}/replay"
                    ),
                    "similar": (
                        "/api/learning/predictions/{prediction_id}/similar"
                    ),
                },
            },
        }

    def _learning_projection(
        self,
        snapshot: VerifiedReplaySnapshot,
    ) -> _LearningProjectionCache:
        """Build each head-bound learning summary once, then reuse it in O(1)."""

        with self._learning_projection_lock:
            cached = self._learning_projection_cache
            if (
                cached is not None
                and cached.verified_head_sequence
                == snapshot.verified_head_sequence
                and cached.verified_head_hash == snapshot.verified_head_hash
            ):
                return cached
            state = self.shadow_learning.governance_state(snapshot=snapshot)
            challengers = self.shadow_learning.challenger_versions(snapshot=snapshot)
            counts = dict(self.shadow_learning.record_counts(snapshot=snapshot))
            outcome_horizons = self._outcome_horizon_summary(
                state.challenger_version,
                snapshot=snapshot,
            )
            shadow_exclusions = self._news_shadow_exclusion_summary(
                snapshot=snapshot,
            )
            projection = _LearningProjectionCache(
                verified_head_sequence=snapshot.verified_head_sequence,
                verified_head_hash=snapshot.verified_head_hash,
                selected_challenger=state.challenger_version,
                governance_state=state,
                challengers=challengers,
                record_counts=counts,
                outcome_horizons=outcome_horizons,
                shadow_exclusions=shadow_exclusions,
            )
            self._learning_projection_cache = projection
            return projection

    def _news_shadow_exclusion_summary(
        self,
        *,
        snapshot: VerifiedReplaySnapshot | None = None,
    ) -> Mapping[str, object]:
        """Report every incompatible news prediction instead of hiding it."""

        reasons: dict[str, int] = {}
        verified = snapshot or self.shadow_learning.verified_replay_snapshot()
        for record in verified.records:
            prediction = getattr(record, "prediction", None)
            if not isinstance(prediction, Mapping):
                continue
            schema = str(prediction.get("schema") or "")
            if not schema.startswith("options_copilot.news_shadow_prediction."):
                continue
            reason = shadow_prediction_exclusion_reason(
                prediction,
                prediction_id=getattr(record, "prediction_id", None),
                independence_key=getattr(record, "independence_key", None),
                predicted_at=getattr(record, "predicted_at", None),
            )
            if reason is not None:
                reasons[reason] = reasons.get(reason, 0) + 1
        return {
            "total": sum(reasons.values()),
            "reasons": dict(sorted(reasons.items())),
        }

    def _outcome_capture_status(self) -> Mapping[str, object]:
        """Expose the live observation producer without leaking broker state."""

        loop = self.outcome_capture_loop
        if loop is None:
            return {
                "status": "UNAVAILABLE",
                "checked_at": None,
                "specs_seen": 0,
                "specs_due": 0,
                "observations_appended": 0,
                "records_blocked": 0,
                "records_skipped": 0,
                "reason_codes": ("OUTCOME_CAPTURE_UNAVAILABLE",),
                "durable_status_counts": {},
                "durable_blocker_counts": {},
                "direction_outcomes_enabled": False,
                "option_economics_requires_bound_candidate": True,
                "decision_authority": "SUPPORTING_ONLY",
            }
        durable = _durable_outcome_capture_counts(loop.coordinator.capture)
        result = loop.last_result
        if result is None:
            return {
                "status": "NOT_RUN",
                "checked_at": None,
                "specs_seen": 0,
                "specs_due": 0,
                "observations_appended": 0,
                "records_blocked": 0,
                "records_skipped": 0,
                "reason_codes": (),
                **durable,
                "direction_outcomes_enabled": True,
                "option_economics_requires_bound_candidate": True,
                "decision_authority": "SUPPORTING_ONLY",
            }
        return {
            "status": result.status,
            "checked_at": result.checked_at.isoformat(),
            "specs_seen": result.specs_seen,
            "specs_due": result.specs_due,
            "observations_appended": result.observations_appended,
            "records_blocked": result.records_blocked,
            "records_skipped": result.records_skipped,
            "reason_codes": result.reason_codes,
            **durable,
            "direction_outcomes_enabled": True,
            "option_economics_requires_bound_candidate": True,
            "decision_authority": "SUPPORTING_ONLY",
        }

    def _outcome_horizon_summary(
        self,
        selected_challenger: str | None,
        *,
        snapshot: VerifiedReplaySnapshot | None = None,
    ) -> Mapping[str, object]:
        """Aggregate a verified, ledger-complete five-horizon display summary."""

        horizon_names = ("30M", "SESSION_CLOSE", "1D", "3D", "5D")
        counts = {
            horizon: {
                "observed_count": 0,
                "blocked_count": 0,
                "uncertain_count": 0,
                "observed_at": None,
            }
            for horizon in horizon_names
        }
        verified = snapshot or self.shadow_learning.verified_replay_snapshot()
        for record in verified.records:
            if (
                not isinstance(record, OutcomeRecord)
                or record.challenger_version != selected_challenger
            ):
                continue
            outcome = record.outcome
            if outcome.get("schema") != "options_copilot.outcome_observation.v2":
                continue
            horizon = str(outcome.get("horizon") or "").upper()
            if horizon not in counts:
                continue
            raw_status = str(outcome.get("status") or "OBSERVED").upper()
            bucket = counts[horizon]
            if raw_status == "BLOCKED":
                bucket["blocked_count"] += 1
            elif raw_status in {"UNCERTAIN", "UNKNOWN"}:
                bucket["uncertain_count"] += 1
            elif raw_status == "OBSERVED":
                bucket["observed_count"] += 1
            else:
                bucket["uncertain_count"] += 1
            observed_at = record.observed_at.isoformat()
            if bucket["observed_at"] is None or observed_at > str(
                bucket["observed_at"]
            ):
                bucket["observed_at"] = observed_at

        horizons: dict[str, Mapping[str, object]] = {}
        for horizon, bucket in counts.items():
            observed_count = int(bucket["observed_count"])
            blocked_count = int(bucket["blocked_count"])
            uncertain_count = int(bucket["uncertain_count"])
            count = observed_count + blocked_count + uncertain_count
            status = (
                "UNCERTAIN"
                if uncertain_count
                else "BLOCKED"
                if blocked_count
                else "OBSERVED"
                if observed_count
                else "NOT_OBSERVED"
            )
            horizons[horizon] = {
                "status": status,
                "count": count,
                "observed_count": observed_count,
                "blocked_count": blocked_count,
                "uncertain_count": uncertain_count,
                "reason": (
                    "OUTCOME_NOT_YET_OBSERVABLE"
                    if status == "NOT_OBSERVED"
                    else f"OUTCOME_HORIZON_{status}"
                ),
                "observed_at": bucket["observed_at"],
                "decision_authority": "SUPPORTING_ONLY",
            }
        return {
            "status": "READY",
            "decision_authority": "SUPPORTING_ONLY",
            "selected_challenger": selected_challenger,
            "complete_through_sequence": verified.verified_head_sequence,
            "verified_head_sequence": verified.verified_head_sequence,
            "verified_head_hash": verified.verified_head_hash,
            "horizons": horizons,
        }

    def _process_outcomes(self, **kwargs: object) -> object:
        processor = self.outcome_processor
        if processor is None:
            raise RuntimeError("outcome processor is unavailable")
        cancel_event = kwargs.get("cancel_event")
        deadline_at = kwargs.get("deadline_at")
        checked_deadline = (
            utc_datetime(deadline_at, field="deadline_at")
            if isinstance(deadline_at, datetime)
            else None
        )
        work_deadline = (
            None
            if checked_deadline is None
            else checked_deadline
            - timedelta(seconds=OUTCOME_CALLBACK_TERMINALIZATION_RESERVE_SECONDS)
        )
        processor_kwargs = dict(kwargs)
        if work_deadline is not None:
            # Primary persistence and supporting-only evaluation must both yield
            # enough time for the scheduler to terminalize the callback before
            # its hard deadline. Passing the hard deadline through would allow
            # a successful primary write to be followed by a failed scan run.
            processor_kwargs["deadline_at"] = work_deadline
        result = processor.process(**processor_kwargs)  # type: ignore[arg-type]
        checked_now = datetime.now(timezone.utc)
        externally_stopped = (
            isinstance(cancel_event, threading.Event) and cancel_event.is_set()
        )
        if checked_deadline is not None:
            externally_stopped = externally_stopped or checked_now >= checked_deadline
        evaluation_refresh: dict[str, object]
        if result.stopped or externally_stopped:
            evaluation_refresh = {
                "status": "SKIPPED",
                "reason": (
                    result.stop_reason
                    if result.stopped
                    else "OUTCOME_PROCESSING_CANCELLED"
                ),
                "decision_authority": "SUPPORTING_ONLY",
                "persisted": False,
            }
        elif result.records_appended == 0 and result.records_superseded == 0:
            # A bounded processor pass that only advances progress or records
            # wait reasons cannot change the shadow evaluation dataset.  Avoid
            # rescanning the verified ledger inside the scheduler deadline.
            evaluation_refresh = {
                "status": "SKIPPED",
                "reason": "SHADOW_EVALUATION_UNCHANGED",
                "decision_authority": "SUPPORTING_ONLY",
                "persisted": False,
            }
        elif work_deadline is not None and checked_now >= work_deadline:
            # Supporting-only evaluation yields a small terminalization reserve
            # so the primary durable processor result remains publishable by
            # the scheduler before its hard callback deadline.
            evaluation_refresh = {
                "status": "DEFERRED",
                "reason": "SHADOW_EVALUATION_DEFERRED_DEADLINE",
                "decision_authority": "SUPPORTING_ONLY",
                "persisted": False,
            }
        else:
            try:
                evaluation = self.shadow_evaluation_store.refresh(
                    self.shadow_learning,
                    generated_at=result.checked_at,
                    cancel_event=(
                        cancel_event
                        if isinstance(cancel_event, threading.Event)
                        else None
                    ),
                    deadline_at=work_deadline,
                )
            except Exception:
                # Outcome progress is the primary durable result.  The shadow
                # evaluation is supporting-only and must not replace a valid
                # processor result with a generic scheduler failure.
                evaluation_refresh = {
                    "status": "DEGRADED",
                    "reason": "SHADOW_EVALUATION_REFRESH_FAILED",
                    "decision_authority": "SUPPORTING_ONLY",
                    "persisted": False,
                }
            else:
                evaluation_status = str(
                    evaluation.get("status") or "UNKNOWN"
                ).upper()
                evaluation_reason = evaluation.get("reason")
                evaluation_refresh = {
                    "status": (
                        "DEFERRED"
                        if isinstance(evaluation_reason, str)
                        and evaluation_reason.startswith(
                            "SHADOW_EVALUATION_DEFERRED_"
                        )
                        else "CANCELLED"
                        if evaluation_status == "CANCELLED"
                        else "COMPLETED"
                    ),
                    "reason": evaluation_reason,
                    "evaluation_status": evaluation_status,
                    "decision_authority": "SUPPORTING_ONLY",
                    "persisted": evaluation.get("persisted", True),
                }
        return {
            **result.as_dict(),
            "shadow_evaluation_refresh": evaluation_refresh,
        }

    def _outcome_processing_status(self) -> Mapping[str, object]:
        processor = self.outcome_processor
        if processor is None:
            return {
                "status": "UNAVAILABLE",
                "checked_at": None,
                "due_count": 0,
                "records_appended": 0,
                "records_superseded": 0,
                "records_skipped": 0,
                "records_blocked": 0,
                "records_rejected": 0,
                "reason_codes": ("OUTCOME_PROCESSOR_UNAVAILABLE",),
                "candidate_ledger_head_hash": None,
                "shadow_ledger_head_hash": None,
                "manifest_hash": None,
                "processing_hash": None,
            }
        result = processor.last_result
        if result is None:
            return {
                "status": "NOT_RUN",
                "checked_at": None,
                "due_count": 0,
                "records_appended": 0,
                "records_superseded": 0,
                "records_skipped": 0,
                "records_blocked": 0,
                "records_rejected": 0,
                "reason_codes": (),
                "candidate_ledger_head_hash": None,
                "shadow_ledger_head_hash": None,
                "manifest_hash": None,
                "processing_hash": None,
            }
        return result.as_dict()

    def _prediction_outcome_targets(
        self,
        *,
        after_sequence: int = 0,
    ) -> tuple[tuple[Mapping[str, object], str], ...]:
        """Project durable shadow predictions into fail-closed capture intents."""

        checked_at = datetime.now(timezone.utc)
        output: list[tuple[Mapping[str, object], str]] = []
        cursor = after_sequence
        while True:
            rows = self.shadow_learning.query_replays(
                as_of=checked_at,
                limit=5000,
                after_sequence=cursor,
            )
            if not rows:
                break
            for replay in rows:
                prediction = replay.prediction
                body = prediction.prediction
                identity = projectable_shadow_prediction_identity(
                    body,
                    prediction_id=prediction.prediction_id,
                    independence_key=prediction.independence_key,
                    predicted_at=prediction.predicted_at,
                )
                if identity is None:
                    continue
                horizon, symbol = identity
                target = freeze_json(
                    {
                        "schema": "options_copilot.outcome_target.v1",
                        "subject_kind": "PREDICTION",
                        "subject_id": prediction.prediction_id,
                        "subject_hash": prediction.content_hash,
                        "symbol": symbol,
                        "occurred_at": prediction.predicted_at,
                        "baseline_at": max(
                            prediction.predicted_at,
                            getattr(
                                prediction,
                                "appended_at",
                                prediction.predicted_at,
                            ),
                        ),
                        "thesis_hash": prediction.thesis_hash,
                        "source_sequence": prediction.sequence,
                        "prediction_hash": prediction.content_hash,
                        "independence_key": prediction.independence_key,
                        "predicted_direction": (
                            str(body.get("classification", {}).get("direction", "UNKNOWN")).upper()
                            if isinstance(body.get("classification"), Mapping)
                            else "UNKNOWN"
                        ),
                        "prediction_set_hash": str(
                            body.get("prediction_baseline_hash", "")
                        ),
                        "binding_context": {
                            "source_sequence": prediction.sequence,
                            "event_id": body.get("event_id"),
                            "event_ids": (
                                (str(body.get("event_id")),)
                                if body.get("event_id")
                                else ()
                            ),
                            "independence_key": prediction.independence_key,
                        },
                        "prediction_baseline_request": {
                            "schema": "options_copilot.prediction_baseline_request.v1",
                            "benchmark_symbol": "SPY",
                        },
                        "capture_plan": {
                            "schema": "options_copilot.outcome_capture_plan.v1",
                            "status": "DIRECTION_ONLY",
                            "reason_codes": (),
                        },
                    }
                )
                assert isinstance(target, Mapping)
                output.append((target, horizon))
            next_sequence = rows[-1].prediction.sequence
            if next_sequence <= cursor:
                break
            cursor = next_sequence
            if len(rows) < 5000:
                break
        return tuple(output)

    def _learning_governance_read_model(self, state: object) -> Mapping[str, object]:
        """Expose only verified P9 identities and immutable safety state.

        This projection has no signing, append, promotion, rollback, approval,
        bridge, or order capability.  Raw authority documents and signatures
        never leave their durable ledger.
        """

        current_policy: dict[str, object] = {
            "status": "UNAVAILABLE",
            "reason": "PRODUCTION_COMPOSITION_UNAVAILABLE",
            "version": None,
            "hash": None,
            "authority_marker_hash": None,
            "authority_head_hash": None,
            "immutable_initial_policy_hash": None,
        }
        risk: dict[str, object] = {
            "status": "UNAVAILABLE",
            "tier": "NORMAL",
            "normal_max_fraction": "0.10",
            "a_grade_max_fraction": "0.15",
            "absolute_reject_fraction": "0.20",
            "authority_version": None,
            "authority_marker_hash": None,
        }
        composition = self.production_composition
        resolver = (
            None
            if composition is None
            else composition.services.policy_resolver
        )
        risk_resolver = (
            None
            if composition is None
            else composition.services.risk_authority_resolver
        )
        if isinstance(resolver, CurrentPolicyResolver):
            checked_at = datetime.now(timezone.utc)
            try:
                resolved = resolver.resolve(now=checked_at)

                def capture_verified_heads() -> tuple[dict[str, object], dict[str, object]]:
                    ledger_state = resolver.ledger.current_state()
                    initial = load_contract(resolver.ledger.initial_policy_path)
                    policy_view: dict[str, object] = {
                        "status": "VERIFIED",
                        "reason": None,
                        "version": resolved.current_policy_version,
                        "hash": resolved.current_policy_hash,
                        "authority_marker_hash": (
                            resolved.policy_authority_marker_hash
                        ),
                        "authority_head_hash": ledger_state.authority_head_hash,
                        "immutable_initial_policy_hash": initial.contract_hash,
                    }
                    risk_view = dict(risk)
                    if isinstance(risk_resolver, CurrentRiskAuthorityResolver):
                        try:
                            authority = risk_resolver.resolve(
                                now=checked_at,
                                current_policy=resolved,
                            )
                        except Exception:
                            pass
                        else:
                            if (
                                authority.tier.value == "NORMAL"
                                and authority.a_grade_approved is False
                            ):
                                risk_view.update(
                                    {
                                        "status": "AVAILABLE",
                                        "authority_version": authority.version,
                                        "authority_marker_hash": (
                                            authority.risk_authority_marker_hash
                                        ),
                                    }
                                )
                    return policy_view, risk_view

                captured = resolver.guard_current(
                    resolved,
                    callback=capture_verified_heads,
                )
                if captured is None:
                    current_policy["reason"] = "CURRENT_POLICY_HEAD_CHANGED"
                else:
                    current_policy, risk = captured
            except PolicyAuthorityTampered:
                current_policy["reason"] = "CURRENT_POLICY_TAMPERED"
            except PolicyAuthorityError as exc:
                reason = str(exc).lower()
                current_policy["reason"] = (
                    "CURRENT_POLICY_FUTURE"
                    if "future" in reason
                    else "CURRENT_POLICY_STALE"
                    if "freshness" in reason or "stale" in reason
                    else "CURRENT_POLICY_UNAVAILABLE"
                )
            except Exception:
                current_policy["reason"] = "CURRENT_POLICY_UNAVAILABLE"

        stage = getattr(getattr(state, "stage", None), "value", None)
        independent_count = getattr(state, "independent_samples", None)
        persisted_evaluation = self.shadow_evaluation_store.latest()
        evaluation = (
            {
                "status": "COLLECTING",
                "reason": "ZERO_INDEPENDENT_SAMPLES",
                "report_hash": None,
                "dataset_hash": None,
                "independence_spec_hash": None,
                "independent_count": independent_count,
                "stage": stage or "COLLECTING",
            }
            if persisted_evaluation is None
            else {
                "status": persisted_evaluation.get("status", "COLLECTING"),
                "reason": persisted_evaluation.get("reason"),
                "report_hash": persisted_evaluation.get("report_hash"),
                "dataset_hash": persisted_evaluation.get("dataset_hash"),
                "independence_spec_hash": persisted_evaluation.get(
                    "independence_spec_hash"
                ),
                "independent_count": persisted_evaluation.get(
                    "independent_count", independent_count
                ),
                "stage": _learning_evaluation_stage(
                    persisted_evaluation.get("status"),
                    persisted_evaluation.get("independent_count", independent_count),
                ),
                "comparison_complete": persisted_evaluation.get(
                    "comparison_complete", False
                ),
                "champion_accuracy": persisted_evaluation.get(
                    "champion_accuracy"
                ),
                "challenger_accuracy": persisted_evaluation.get(
                    "challenger_accuracy"
                ),
                "challenger_accuracy_delta": persisted_evaluation.get(
                    "challenger_accuracy_delta"
                ),
                "champion_brier_score": persisted_evaluation.get(
                    "champion_brier_score"
                ),
                "challenger_brier_score": persisted_evaluation.get(
                    "challenger_brier_score"
                ),
                "challenger_brier_improvement": persisted_evaluation.get(
                    "challenger_brier_improvement"
                ),
            }
        )
        blocked = {
            "status": "BLOCKED",
            "reason": NO_TRUSTED_HUMAN_SIGNER,
        }
        return {
            "schema": LEARNING_GOVERNANCE_SCHEMA,
            "status": "BLOCKED",
            "reason": NO_TRUSTED_HUMAN_SIGNER,
            "current_policy": current_policy,
            "evaluation": evaluation,
            "promotion": {**blocked, "authority_hash": None},
            "rollback": {
                **blocked,
                "authority_hash": None,
                "target_policy_hash": None,
            },
            "a_grade": {
                **blocked,
                "marker_hash": None,
                "max_risk_fraction": "0.15",
            },
            "authority": {
                "human_signer_status": NO_TRUSTED_HUMAN_SIGNER,
                "read_only": True,
                "can_sign": False,
                "can_auto_promote": False,
                "can_change_production_weights": False,
                "can_change_production_rules": False,
                "a_grade_15_percent_unlocked": False,
                "approval_authority": False,
                "bridge_authority": False,
                "order_authority": False,
            },
            "risk": risk,
        }

    def learning_records(
        self,
        record_type: str | None,
        challenger_version: str | None,
        limit: int,
    ) -> Mapping[str, object]:
        records = self.shadow_learning.query_records(
            record_type=record_type,
            challenger_version=challenger_version,
            limit=limit,
        )
        return {
            "records": [shadow_record_to_dict(record) for record in records],
            "count": len(records),
            "filters": {
                "record_type": record_type,
                "challenger_version": challenger_version,
                "limit": limit,
            },
        }

    def learning_record(self, record_id: str) -> Mapping[str, object]:
        try:
            record = self.shadow_learning.get_record(record_id)
        except UnknownRecordError as exc:
            raise LearningRecordNotFound(str(exc)) from exc
        return {"record": shadow_record_to_dict(record)}

    def learning_replay(self, prediction_id: str) -> Mapping[str, object]:
        try:
            replay = self.shadow_learning.replay(prediction_id)
        except UnknownRecordError as exc:
            raise LearningRecordNotFound(str(exc)) from exc
        return {"replay": replay_record_to_dict(replay)}

    def learning_similar(
        self,
        prediction_id: str,
        limit: int,
    ) -> Mapping[str, object]:
        try:
            matches = self.shadow_learning.query_similar(
                prediction_id=prediction_id,
                limit=limit,
            )
        except UnknownRecordError as exc:
            raise LearningRecordNotFound(str(exc)) from exc
        return {
            "prediction_id": prediction_id,
            "matches": [similarity_match_to_dict(match) for match in matches],
            "count": len(matches),
        }

    def approve_proposal(
        self,
        proposal_id: str,
        approval_request: Mapping[str, object],
    ) -> Mapping[str, object]:
        del proposal_id, approval_request
        raise ProposalApprovalConflict(
            "legacy runtime_snapshot.json candidates are observation-only; "
            "use the immutable rank-one challenge endpoint"
        )

    def approval_status(self, approval_id: str) -> Mapping[str, object]:
        approval = self.approvals.get(approval_id)
        bridge = self.bridge.get(approval_id)
        if approval is None and bridge is None:
            raise ApprovalStatusNotFound(approval_id)
        now = datetime.now(timezone.utc)
        expires_at = None if approval is None else approval.expires_at.isoformat()
        status = "PENDING_CODEX_BRIDGE"
        instruction_id = None
        deep_link = None
        failure_reason = None
        if approval is not None and now >= approval.expires_at:
            status = "EXPIRED"
            failure_reason = "The five-minute GUI approval expired."
        if bridge is not None:
            if bridge.status is BridgeStatus.COMPLETED:
                status = "FAILED"
                failure_reason = "creator review destination contract is unavailable"
            elif bridge.unknown_outcome:
                status = "UNKNOWN_OUTCOME"
                failure_reason = (
                    "The external connector boundary was crossed but no durable "
                    "result is confirmed; automatic retry is forbidden."
                )
            elif bridge.status is BridgeStatus.FAILED:
                status = "FAILED"
                failure_reason = bridge.failure_reason
            elif (
                bridge.status is BridgeStatus.AUTHORIZED
                and bridge.external_call_reserved
            ):
                status = "UNKNOWN_OUTCOME"
                failure_reason = (
                    "The external connector boundary was crossed but no durable "
                    "result is confirmed; automatic retry is forbidden."
                )
            elif status != "EXPIRED":
                status = bridge.status.value
        return {
            "approval_id": approval_id,
            "status": status,
            "expires_at": expires_at,
            "instruction_id": instruction_id,
            "ibkr_deep_link": deep_link,
            "failure_reason": failure_reason,
            "order_submitted": False,
            "transmitted_to_broker": False,
        }

    def ingest(self, snapshot: RuntimeSnapshot) -> None:
        with self._lock:
            self.snapshot_store.write(snapshot)
            self._snapshot = snapshot

    def _current_snapshot(self) -> RuntimeSnapshot | None:
        """Reload the atomic connector snapshot on every read-model request.

        The managed connector writes this file outside the GUI process.  A
        cached-only read would leave the operator looking at old positions and
        account reconciliation until the server restarted, so the disk
        snapshot remains the source of truth for this observation-only view.
        """

        with self._lock:
            current = self.snapshot_store.read()
            self._snapshot = current
            return current

    def _current_control_projection(self) -> Mapping[str, object] | None:
        composition = self.production_composition
        if composition is None:
            return None
        reader = getattr(composition.lifecycle, "control_snapshot", None)
        if not callable(reader):
            return None
        try:
            value = reader()
        except Exception:
            return None
        if not isinstance(value, Mapping):
            return None
        account = value.get("account")
        observed_at = value.get("observed_at")
        if not isinstance(account, Mapping) or not account or not isinstance(
            observed_at,
            str,
        ):
            return None
        return value

    def _control_bootstrap(
        self,
        control: Mapping[str, object],
    ) -> Mapping[str, object]:
        status = str(control.get("status", "UNAVAILABLE")).upper()
        account_source = control.get("account")
        account = dict(account_source) if isinstance(account_source, Mapping) else {}
        account["status"] = status
        # The direct IBKR account row does not itself prove Strategy NAV
        # reconciliation.  Publish a positive value only after the signed NAV
        # snapshot binds the same control NLV, observation time, difference,
        # and hashes below.  This remains display-only and does not replace the
        # stricter AtomicBrokerSnapshot decision Gate.
        account["reconciled"] = None
        account["decision_authority"] = (
            "LAST_KNOWN_ONLY"
            if status in {"STALE", "PARTIAL"}
            else "OBSERVATION_ONLY"
        )
        campaign: dict[str, object] = {
            "target_nlv_usd": 10000,
            "progress_fraction": 0,
            "status": status,
            "decision_authority": account["decision_authority"],
        }
        warnings: list[str] = []
        observed_at_raw = control.get("observed_at")
        try:
            observed_at = datetime.fromisoformat(str(observed_at_raw))
            observed_nlv = _positive_decimal(
                account.get("net_liquidation"),
                "control.account.net_liquidation",
            )
            nav, nav_reasons = self.runtime_services._strategy_nav(
                observed_at,
                observed_account_nlv=observed_nlv,
            )
        except (TypeError, ValueError):
            nav = None
            nav_reasons = ("STRATEGY_NAV_RECONCILIATION_UNAVAILABLE",)
        if (
            nav is not None
            and nav.strategy_nav is not None
            and _strategy_nav_reconciles_control_observation(
                nav,
                asof=observed_at,
                observed_account_nlv=observed_nlv,
            )
        ):
            if status == "CURRENT":
                account["reconciled"] = True
            account.update(
                {
                    "observed_at": observed_at.isoformat(),
                    "strategy_nav_asof": nav.asof.isoformat(),
                    "strategy_nav_usd": nav.strategy_nav,
                    "reconciliation_difference_usd": (
                        nav.reconciliation_difference
                    ),
                    "strategy_nav_content_hash": nav.content_hash,
                    "strategy_nav_authority_hash": nav.authority_hash,
                    "strategy_nav_contract_hash": nav.contract_hash,
                    "strategy_nav_ledger_head_hash": nav.ledger_head_hash,
                }
            )
            campaign.update(
                {
                    "strategy_nav_usd": nav.strategy_nav,
                    "account_nlv_usd": nav.observed_account_nlv,
                    "strategy_nav_asof": nav.asof.isoformat(),
                    "account_observed_at": observed_at.isoformat(),
                    "reconciliation_difference_usd": (
                        nav.reconciliation_difference
                    ),
                    "strategy_nav_content_hash": nav.content_hash,
                    "strategy_nav_authority_hash": nav.authority_hash,
                    "strategy_nav_contract_hash": nav.contract_hash,
                    "strategy_nav_ledger_head_hash": nav.ledger_head_hash,
                    "progress_fraction": min(
                        Decimal("1"),
                        nav.strategy_nav / Decimal("10000"),
                    ),
                }
            )
        else:
            if nav is not None:
                nav_reasons = (
                    *nav_reasons,
                    "STRATEGY_NAV_RECONCILIATION_INVALID",
                )
            warnings.extend(nav_reasons)
        reason = control.get("reason")
        if isinstance(reason, str) and reason:
            warnings.append(reason)
        if status in {"STALE", "PARTIAL"}:
            warnings.append(
                "Displayed IBKR control values are observation-only and cannot "
                "authorize scans, risk, approval, creator transport, or orders."
            )
        return {
            "asof": observed_at_raw,
            "source": "IBKR_READONLY_CONTROL",
            "account": account,
            "campaign": campaign,
            "warnings": list(dict.fromkeys(warnings)),
            "safety": self._safety(),
        }

    def _current_projection_snapshot(
        self,
    ) -> tuple[RuntimeSnapshot | None, str | None]:
        """Return only state safe to describe as current in the operator GUI.

        ``runtime_snapshot.json`` is an observation cache, not broker authority.
        Old or incomplete rows remain on disk for auditability but must never be
        rendered as the current account, position, or candidate state.
        """

        snapshot = self._current_snapshot()
        if snapshot is None:
            return None, "SNAPSHOT_UNAVAILABLE"
        age_seconds = (datetime.now(timezone.utc) - snapshot.observed_at).total_seconds()
        if age_seconds < 0:
            return None, "SNAPSHOT_FROM_FUTURE"
        if snapshot.working_order_count is None:
            return None, "WORKING_ORDER_STATE_UNKNOWN"
        if (
            not snapshot.broker_snapshot_complete
            and snapshot.unsubmitted_instruction_count is not None
        ):
            return None, "SNAPSHOT_INCOMPLETE"
        control_stale_seconds = float(
            getattr(self.config, "control_snapshot_stale_seconds", 15.0)
        )
        if age_seconds > control_stale_seconds:
            return snapshot, "SNAPSHOT_STALE"
        if snapshot.unsubmitted_instruction_count is None:
            return snapshot, "SAVED_INSTRUCTION_STATE_UNKNOWN"
        if not snapshot.broker_snapshot_complete:
            return None, "SNAPSHOT_INCOMPLETE"
        return snapshot, None

    def _register_baseline_if_needed(self) -> None:
        if self.learning.current_champion() is None:
            now = datetime.now(timezone.utc)
            self.learning.register_champion(
                BASELINE_MODEL_VERSION,
                BASELINE_ARTIFACT_HASH,
                registered_at=now,
                metadata={
                    "risk_governance": "human_gated",
                    "a_grade_unlocked": False,
                    "live_learning": False,
                },
            )

    def _safety(self) -> dict[str, object]:
        return {
            "review_only": True,
            "direct_order_submission": False,
            "approval_enabled": self.runtime_services.approval_enabled,
            "decision_runtime_status": self.runtime_services.readiness()["status"],
            "risk_basis": "STRATEGY_NAV_SNAPSHOT_ONLY",
            "account_nlv_authority": "OBSERVATION_AND_RECONCILIATION_ONLY",
            "naked_options_prohibited": True,
            "unknown_loss_prohibited": True,
            "normal_risk_cap": self.config.normal_risk_fraction,
            "a_grade_risk_cap": self.config.a_grade_risk_fraction,
            "hard_risk_cap": self.config.hard_risk_fraction,
            "a_grade_unlocked": False,
            "max_open_combinations": self.config.max_open_combinations,
        }

    @staticmethod
    def _open_combo_count(positions: tuple[Mapping[str, object], ...]) -> int:
        for item in positions:
            raw_quantities = [
                item[key]
                for key in ("position", "quantity", "contracts", "size")
                if key in item
            ]
            if not raw_quantities:
                return 1
            quantities: list[Decimal] = []
            for raw_quantity in raw_quantities:
                if isinstance(raw_quantity, bool):
                    return 1
                try:
                    quantity = Decimal(str(raw_quantity))
                except (InvalidOperation, TypeError, ValueError):
                    return 1
                if not quantity.is_finite():
                    return 1
                quantities.append(quantity)
            if any(value != quantities[0] for value in quantities[1:]):
                return 1
            if quantities[0] == 0:
                continue

            raw_security_types = [
                item[key]
                for key in ("asset_class", "security_type", "sec_type")
                if key in item
            ]
            if not raw_security_types:
                return 1
            security_types = {
                str(value).strip().upper() for value in raw_security_types
            }
            if len(security_types) != 1:
                return 1
            if next(iter(security_types)) in {"OPT", "OPTION", "BAG", "COMBO"}:
                return 1
        return 0


def _durable_outcome_capture_counts(
    capture: ExactHorizonOutcomeCapture,
) -> Mapping[str, object]:
    """Summarize latest immutable capture-spec states for truthful operator UI."""

    try:
        counts = capture.durable_counts()
    except Exception:
        return {
            "durable_status_counts": {},
            "durable_blocker_counts": {"OUTCOME_CAPTURE_STORE_CORRUPT": 1},
        }
    return {
        "durable_status_counts": dict(counts["durable_status_counts"]),
        "durable_blocker_counts": dict(counts["durable_blocker_counts"]),
    }


def _positive_decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite positive number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite positive number") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field} must be a finite positive number")
    return result


def _unavailable_runtime_services(
    *, approval_store: object, bridge_reader: object
) -> RuntimeServices:
    """Compose an explicit fail-closed graph until every production port exists.

    The signed execution-cost authority is locally complete and safe to compose
    even while broker/evidence/scanner ports remain absent.  Installing this
    resolver cannot make the graph READY by itself; it only prevents production
    from silently substituting a test stub once the remaining ports arrive.
    """

    # Keep the close/reduce-only position manager available even while the
    # entry/scanner graph is intentionally incomplete.  Its default read model
    # is a review-only NO_TRADE projection and it gains no broker authority by
    # being composed here.  Future broker snapshot/generator wiring must reuse
    # this exact instance so the API can never drift from the proof engine.
    position_manager = PositionManager()
    return RuntimeServices(
        broker_snapshot_builder=None,
        evidence_store=None,
        scan_run_store=None,
        pipeline_inputs=None,
        universe_funnel=None,
        broker_evidence_acquisition=None,
        options_evidence_acquisition=None,
        strategy_registry=None,
        strategy_candidate_generator=None,
        volatility_engine=None,
        scenario_engine=None,
        policy_resolver=None,
        risk_authority_resolver=None,
        execution_cost_contract=SignedExecutionCostResolver(),
        eligibility_gate=None,
        risk_gate=None,
        dte_gate=None,
        single_combination_gate=None,
        portfolio_ranker=None,
        ranking_store=None,
        decision_pipeline=None,
        strategy_nav_source=None,
        position_manager=position_manager,
        approval_store=approval_store,
        bridge_status_reader=bridge_reader,
        bridge_reconciliation_reader=bridge_reader,
    )


def _configured_event_providers(
    config: OptionsCopilotConfig,
    *,
    secrets_store: LocalApiKeyStore | None = None,
    jin10_binding_store: DPAPISecretStore | None = None,
) -> tuple[tuple[object, ...], tuple[object, ...]]:
    """Compose public metadata sources plus optional credentialed providers.

    Jin10 is always represented in source health.  Its transport can receive a
    credential only when the current local-file generation, zero-material
    activation, and latest attested rotation agree exactly.  It then uses the
    exact official Streamable HTTP MCP endpoint.  Every Jin10 row remains
    supplemental and ``SUPPORTING_ONLY``.
    """

    news: list[object] = [
        SecCurrent8KProvider(),
        CompanyIrEventProvider(),
    ]
    calendars: list[object] = [NasdaqEarningsProvider()]
    secrets_store = secrets_store or LocalApiKeyStore(
        local_api_key_path(config.data_dir)
    )
    jin10_binding_store = jin10_binding_store or DPAPISecretStore(
        config.secrets_path
    )
    try:
        names = set(secrets_store.names())
    except LocalApiKeyFileError:
        names = set()
    if "FINNHUB_API_KEY" in names:
        # Keep the high-frequency company-news lane bounded and give the
        # lower-frequency earnings calendar an independent transport limiter.
        # Sharing one adapter made a slow multi-symbol news fan-out delay both
        # Jin10 and the calendar while also coupling their health state.
        news.append(
            FinnhubEventProvider(
                secrets_store,
                max_news_symbols_per_cycle=1,
            )
        )
        calendars.append(FinnhubEventProvider(secrets_store))
    if "ALPHA_VANTAGE_API_KEY" in names:
        news.append(AlphaVantageNewsProvider(secrets_store))
    jin10_resolution = resolve_jin10_credential(
        LocalJin10EnvelopeReader(secrets_store, jin10_binding_store),
        jin10_rotation_evidence_dir(config.data_dir),
    )
    news.append(
        Jin10EventProvider(
            jin10_resolution.secret_store,
            mcp_client=Jin10McpHttpClient(),
        )
    )
    return tuple(news), tuple(calendars)


def _shutdown_runtime_for_asgi(runtime: OptionsCopilotRuntime) -> None:
    """Make a bounded shutdown failure visible to the ASGI lifespan owner."""

    if runtime.close() is False:
        raise RuntimeError("OPTIONS_COPILOT_SHUTDOWN_INCOMPLETE")


def build_app(config: OptionsCopilotConfig | None = None):
    runtime = OptionsCopilotRuntime(config or OptionsCopilotConfig.from_env())
    app = create_app(runtime.services())
    app.state.runtime = runtime
    app.router.add_event_handler("startup", runtime.start)
    app.router.add_event_handler(
        "shutdown",
        lambda: _shutdown_runtime_for_asgi(runtime),
    )
    return app


def create_runtime_app():
    """Uvicorn factory; avoids opening runtime databases on module import."""

    return build_app()
