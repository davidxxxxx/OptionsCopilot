"""Dependency-injected FastAPI surface for the Options Copilot.

This module deliberately owns no broker client.  The approval endpoint can only
ask an injected handler to create a review instruction; it never exposes an
order-placement primitive.
"""
from __future__ import annotations

import inspect
import math
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any, Literal, Protocol, TypeAlias
from urllib.parse import quote, urlsplit, urlunsplit

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from .holdings_projection import HOLDINGS_CLOSE_SCHEMA, project_holdings_close_preview

from options_copilot.operations.readiness import (
    SecretLikeFieldError,
    assert_no_secret_like,
    default_readiness_report,
)
from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
)
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
)
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomics,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)
from options_copilot.news.models import UNDERLYING_QUOTE_BASIS_SCHEMA
from options_copilot.news.intelligence import project_event_intelligence
from options_copilot.news.macro_proxy import require_current_research_proxy_binding
from options_copilot.news.research_top10 import (
    read_research_top10,
    safe_research_top10_read_model,
    unavailable_research_top10_read_model,
)
from options_copilot.ranking.readiness import (
    CandidateReadinessError,
    evaluate_candidate_readiness,
)
from options_copilot.research_allocation import (
    normalise_research_allocation_read_model as _normalise_allocation_read_model,
)
from options_copilot.storage.canonical import canonical_hash
from options_copilot.feature_source_diagnostic import validate_feature_sources_response


APPROVAL_CONFIRMATION_TOKEN = "CREATE_IBKR_REVIEW_ONLY"
_CANDIDATE_EVIDENCE_SCHEMA = "options_copilot.candidate_evidence_manifest.v1"
_PRIMARY_EVIDENCE_KINDS = frozenset(
    {
        "BROKER_SNAPSHOT",
        "CONTRACT_DEFINITION",
        "EXECUTABLE_QUOTE",
        "PAYOFF_MAX_LOSS",
        "LIQUIDITY",
        "EXECUTION_COST",
        "AFTER_COST_EV",
        "DTE_RISK",
    }
)
_PRIMARY_EVIDENCE_SOURCES = frozenset(
    {"IBKR_READ_ONLY", "LOCAL_DETERMINISTIC"}
)
_LEARNING_GOVERNANCE_SCHEMA = "options_copilot.learning.governance.v1"
_NO_TRUSTED_HUMAN_SIGNER = "NO_TRUSTED_HUMAN_SIGNER"
_PRIVATE_LEARNING_AUTHORITY_FIELDS = frozenset(
    {
        "promotion_authority",
        "rollback_authority",
        "a_grade_authority",
        "authority_document",
        "raw_authority",
        "signer_key_id",
        "signature_algorithm",
        "governance_signature",
        "public_key",
        "private_key",
        "creator_transport",
        "creator_capability",
        "creator_adapter",
    }
)
_PRIVATE_LEARNING_FIELD_PARTS = (
    "password",
    "secret",
    "token",
    "apikey",
    "authorizationheader",
    "credential",
    "sessioncookie",
    "signature",
    "signerkey",
    "publickey",
    "privatekey",
    "rawauthority",
    "authoritydocument",
    "rawjson",
)
_DROP_LEARNING_VALUE = object()
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
        "REQUIRED_FIELDS_ABSENT",
        "TOOL_FAILURE",
        "REQUEST_FAILED",
        "REQUEST_TIMEOUT",
        "SYMBOL_BINDING_REJECTED",
        "SOURCE_STATUS_STALE",
        "SOURCE_STATUS_CLOCK_REGRESSED",
        "VERIFIED_RELATED_RECORD_REJECTED",
        "TICKER_RESOLUTION_FAILED",
        "TRANSPORT_UNVERIFIED",
        "UNCONFIGURED",
        "NOT_CONFIGURED",
        "UNSAFE_XML",
    }
)
_ANALYSIS_BACKFILL_STATES = frozenset({"READY", "PENDING", "DEGRADED", "UNAVAILABLE"})
_NEWS_PUBLICATION_STAGES = frozenset({
    "IDLE", "LOCAL_ANALYSIS_RESTORE", "NEWS_PROVIDERS", "NEWS_APPEND",
    "IBKR_BINDINGS", "PRESELECTIONS", "CALENDAR_PROVIDERS", "READ_MODEL_REBUILD",
})
_ANALYSIS_INTEGRITY_STATES = frozenset({"VERIFIED", "PENDING", "DEGRADED"})
_SHADOW_ADVISORY_STATES = frozenset({"READY", "PENDING", "DEGRADED", "UNAVAILABLE"})
_SHADOW_ADVISORY_REASONS = frozenset(
    {
        "SHADOW_ADVISORY_DEFERRED",
        "SHADOW_ADVISORY_FAILED",
        "SHADOW_ADVISORY_FAILURE",
        "SHADOW_ADVISORY_LEDGER_REPAIR_FAILED",
        "SHADOW_ADVISORY_NOT_ADVANCED",
        "SHADOW_ADVISORY_NO_ATTEMPT",
        "SHADOW_ADVISORY_NO_ELIGIBLE_INPUTS",
        "SHADOW_ADVISORY_SKIPPED",
        "SHADOW_ADVISORY_UNAVAILABLE",
        "SHADOW_LEDGER_UNAVAILABLE",
    }
)
_SHADOW_SKIP_REASONS = frozenset(
    {
        "SHADOW_INPUT_BEFORE_ENABLEMENT",
        "SHADOW_INPUT_CONFLICTED",
        "SHADOW_INPUT_DUPLICATE",
        "SHADOW_INPUT_INCOMPLETE",
        "SHADOW_INPUT_NOT_ELIGIBLE",
        "SHADOW_INPUT_PRIORITY_RANK_MISSING",
        "SHADOW_INPUT_SYMBOL_COUNT_INVALID",
    }
)
_SHADOW_FAILURE_REASONS = frozenset(
    {
        "CLASSIFIER_INVENTED_EVIDENCE",
        "CLASSIFIER_INVENTED_SYMBOL",
        "CLASSIFIER_NOT_STRUCTURED_LLM",
        "CLASSIFIER_RESULT_TYPE_INVALID",
        "CLASSIFIER_TRANSIENT_FAILURE",
        "CLASSIFIER_TYPE_ERROR",
        "CLASSIFIER_VALUE_ERROR",
        "DEEPSEEK_BAD_JSON",
        "DEEPSEEK_CONNECT_ERROR",
        "DEEPSEEK_CONTENT_TYPE_INVALID",
        "DEEPSEEK_COST_ACCOUNTING_FAILED",
        "DEEPSEEK_DAILY_SPEND_CAP",
        "DEEPSEEK_EMPTY_RESPONSE",
        "DEEPSEEK_ENCODING_INVALID",
        "DEEPSEEK_ERROR",
        "DEEPSEEK_FLASH_DAILY_CALL_CAP",
        "DEEPSEEK_HTTP_ERROR",
        "DEEPSEEK_INCOMPLETE_FINISH",
        "DEEPSEEK_PRO_DAILY_CALL_CAP",
        "DEEPSEEK_RATE_LIMITED",
        "DEEPSEEK_REDIRECT_FORBIDDEN",
        "DEEPSEEK_REMOTE_UNAVAILABLE",
        "DEEPSEEK_REQUEST_FAILED",
        "DEEPSEEK_REQUEST_TIMEOUT",
        "DEEPSEEK_RESPONSE_SCHEMA_INVALID",
        "DEEPSEEK_RESPONSE_TOO_LARGE",
        "DEEPSEEK_TLS_ERROR",
        "DEEPSEEK_TRANSPORT_ERROR",
        "DEEPSEEK_UNSAFE_FINISH",
        "SHADOW_ADVISORY_LEDGER_REPAIR_FAILED",
        "SHADOW_ADVISORY_LEDGER_WRITE_FAILED",
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
_PHASE2_FALLBACK_REASONS = frozenset(
    {
        "MODEL_DISABLED",
        "MODEL_EVALUATION_PENDING",
        "MODEL_BUDGET_EXHAUSTED",
        "MODEL_CONTEXT_LIMIT",
        "MODEL_TRANSPORT_UNAVAILABLE",
        "MODEL_OUTPUT_INVALID",
        "MODEL_BINDING_INVALID",
    }
)
_PHASE2_SLICE_STATES = frozenset(
    {"SUPPORTS", "CONTRADICTS", "NEUTRAL", "UNCERTAIN", "UNAVAILABLE"}
)
_PHASE2_DIRECTIONS = frozenset(
    {"BULLISH", "BEARISH", "NEUTRAL", "MIXED", "UNCERTAIN"}
)

JsonMapping: TypeAlias = Mapping[str, Any]
ProviderResult: TypeAlias = JsonMapping | Awaitable[JsonMapping]
CollectionResult: TypeAlias = (
    JsonMapping
    | Sequence[JsonMapping]
    | Awaitable[JsonMapping | Sequence[JsonMapping]]
)


class SnapshotProvider(Protocol):
    """Small callable contract implemented by runtime snapshot adapters."""

    def __call__(self) -> ProviderResult: ...


class CollectionProvider(Protocol):
    """Provider that may return either an envelope or a bare collection."""

    def __call__(self) -> CollectionResult: ...


class MappingHandler(Protocol):
    """Runs one server-bounded operation from a validated mapping."""

    def __call__(self, request: Mapping[str, object]) -> ProviderResult: ...


class ApprovalHandler(Protocol):
    """Creates an IBKR review instruction without submitting an order."""

    def __call__(
        self,
        proposal_id: str,
        approval: Mapping[str, object],
    ) -> ProviderResult: ...


class IdentifierPairHandler(Protocol):
    """Server-authoritative action addressed only by two immutable IDs."""

    def __call__(self, first_id: str, second_id: str) -> ProviderResult: ...


class IdentifierHandler(Protocol):
    """Read/action port addressed by one immutable server identifier."""

    def __call__(self, identifier: str) -> ProviderResult: ...


class ConfirmationHandler(Protocol):
    """Consumes a persisted challenge; no proposal or authority copy is accepted."""

    def __call__(
        self,
        challenge_id: str,
        confirmation: Mapping[str, object],
    ) -> ProviderResult: ...


class ApprovalStatusProvider(Protocol):
    """Returns the read-only state of one local review handoff."""

    def __call__(self, approval_id: str) -> ProviderResult: ...


class ReadinessProvider(Protocol):
    """Returns one canonical, observation-only readiness report."""

    def __call__(self) -> ProviderResult: ...


class LearningRecordsProvider(Protocol):
    """Queries immutable shadow-learning records without mutation authority."""

    def __call__(
        self,
        record_type: str | None,
        challenger_version: str | None,
        limit: int,
    ) -> ProviderResult: ...


class LearningSimilarityProvider(Protocol):
    """Returns read-only similarity replays for one immutable prediction."""

    def __call__(self, prediction_id: str, limit: int) -> ProviderResult: ...


class ProposalApprovalConflict(RuntimeError):
    """Expected fail-closed proposal rejection at the runtime boundary."""


class RankOneAuthorizationForbidden(RuntimeError):
    """The requested candidate is read-only and cannot start confirmation."""


class OptionsCopilotUnavailable(RuntimeError):
    """A required current-authority dependency is unavailable."""


class ApprovalStatusNotFound(RuntimeError):
    """The requested approval does not exist in the local handoff stores."""


class LearningRecordNotFound(RuntimeError):
    """The requested immutable shadow-learning record does not exist."""


class ApprovalRequest(BaseModel):
    """Three independent user signals required before preparing a review."""

    model_config = ConfigDict(extra="forbid", strict=True)

    risk_acknowledged: Literal[True]
    second_confirmation: Literal[True]
    confirmation_token: Literal["CREATE_IBKR_REVIEW_ONLY"]
    quote_snapshot_id: str = Field(min_length=1, max_length=128)
    approval_challenge: str = Field(min_length=24, max_length=256)


class RankOneChallengeRequest(BaseModel):
    """Intentionally empty: ranking and candidate identities live in the path."""

    model_config = ConfigDict(extra="forbid", strict=True)


class ApprovalConfirmationRequest(BaseModel):
    """Second human confirmation for one persisted, server-derived challenge."""

    model_config = ConfigDict(extra="forbid", strict=True)

    challenge_response: str = Field(min_length=24, max_length=512)
    risk_acknowledged: Literal[True]
    second_confirmation: Literal[True]
    confirmation_token: Literal["CREATE_IBKR_REVIEW_ONLY"]


class ImmediateReadOnlyScanRequest(BaseModel):
    """Explicit operator intent for one pacing-bounded broker-read scan."""

    model_config = ConfigDict(extra="forbid", strict=True)

    confirmation_token: Literal["RUN_READ_ONLY_SCAN_NOW"]
    scope: Literal["CORE_CAMPAIGN", "BOUNDED_MARKET"] = "CORE_CAMPAIGN"


class ReadOnlyOptionMarketDataDiagnosticRequest(BaseModel):
    """Explicit operator intent for one exact-contract read-only quote probe."""

    model_config = ConfigDict(extra="forbid", strict=True)

    confirmation_token: Literal["READ_OPTION_MARKET_DATA_DIAGNOSTIC"]
    scope: Literal["SINGLE_EXACT_CONTRACT"]
    contract_id: int = Field(gt=0)
    symbol: str = Field(min_length=1, max_length=16, pattern=r"^[A-Z0-9.]+$")
    expiration: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    strike: str = Field(min_length=1, max_length=32, pattern=r"^\d+(?:\.\d+)?$")
    right: Literal["C", "P"]
    exchange: Literal["SMART"]
    currency: Literal["USD"]
    trading_class: str = Field(
        min_length=1,
        max_length=32,
        pattern=r"^[A-Z0-9.]+$",
    )
    multiplier: Literal[100]
    local_symbol: str = Field(min_length=1, max_length=64)


class ReadOnlyFeatureSourcesDiagnosticRequest(BaseModel):
    """Explicit operator intent to observe bounded sources for one core symbol."""

    model_config = ConfigDict(extra="forbid", strict=True)

    confirmation_token: Literal["READ_FEATURE_SOURCE_DIAGNOSTIC"]
    scope: Literal["SINGLE_UNDERLYING"]
    symbol: str = Field(min_length=1, max_length=12, pattern=r"^[A-Z0-9.]+$")


class AfterHoursIndicativeRequest(BaseModel):
    """Explicit operator intent for one non-executable option-mark read."""

    model_config = ConfigDict(extra="forbid", strict=True)

    confirmation_token: Literal["READ_AFTER_HOURS_OPTION_MARKS"]


@dataclass(frozen=True, slots=True)
class OptionsCopilotServices:
    """Runtime dependencies supplied by the composition root."""

    health_provider: SnapshotProvider
    bootstrap_provider: SnapshotProvider
    candidates_provider: CollectionProvider
    positions_provider: CollectionProvider
    learning_provider: SnapshotProvider
    health_summary_provider: SnapshotProvider | None = None
    approval_handler: ApprovalHandler | None = None
    approval_status_provider: ApprovalStatusProvider | None = None
    readiness_provider: ReadinessProvider | None = None
    # These providers are deliberately optional.  The standalone application
    # remains useful (and safe) when no news/calendar integration is composed.
    news_provider: CollectionProvider | None = None
    calendar_provider: CollectionProvider | None = None
    advisory_provider: SnapshotProvider | None = None
    fundamentals_provider: SnapshotProvider | None = None
    source_evidence_provider: SnapshotProvider | None = None
    positioning_provider: SnapshotProvider | None = None
    latest_scan_provider: SnapshotProvider | None = None
    latest_ranking_provider: SnapshotProvider | None = None
    ranking_provider: IdentifierHandler | None = None
    candidate_evidence_provider: IdentifierPairHandler | None = None
    management_provider: SnapshotProvider | None = None
    rank_one_challenge_handler: IdentifierPairHandler | None = None
    challenge_confirmation_handler: ConfirmationHandler | None = None
    learning_records_provider: LearningRecordsProvider | None = None
    learning_record_provider: IdentifierHandler | None = None
    learning_replay_provider: IdentifierHandler | None = None
    learning_similarity_provider: LearningSimilarityProvider | None = None
    research_top10_provider: SnapshotProvider | None = None
    equity_pool_provider: SnapshotProvider | None = None
    option_pool_provider: SnapshotProvider | None = None
    after_hours_indicative_provider: SnapshotProvider | None = None
    after_hours_latest_provider: SnapshotProvider | None = None
    provider_configuration_provider: SnapshotProvider | None = None
    weekly_brief_provider: SnapshotProvider | None = None
    immediate_scan_handler: SnapshotProvider | None = None
    immediate_scan_campaign_provider: SnapshotProvider | None = None
    option_market_data_diagnostic_handler: MappingHandler | None = None
    feature_sources_diagnostic_handler: MappingHandler | None = None
    feature_source_cache_provider: SnapshotProvider | None = None


def create_app(services: OptionsCopilotServices) -> FastAPI:
    """Build the standalone, human-gated Options Copilot application."""

    app = FastAPI(
        title="QuantumTrading Options Copilot",
        version="0.1.0",
        description="Human-gated US options research and IBKR review console.",
    )
    app.state.services = services

    frontend = Path(__file__).resolve().parents[1] / "frontend"
    app.mount("/assets", StaticFiles(directory=frontend), name="options-assets")

    @app.get("/", include_in_schema=False)
    async def frontend_index() -> FileResponse:
        return FileResponse(frontend / "index.html")

    @app.get("/health")
    async def health() -> dict[str, object]:
        raw = _require_mapping(
            await _invoke_collection_provider(services.health_provider)
        )
        return _normalise_health(raw)

    @app.get("/api/health/summary")
    async def health_summary() -> dict[str, object]:
        provider = services.health_summary_provider or services.health_provider
        raw = _require_mapping(await _invoke_collection_provider(provider))
        return _normalise_health(raw)

    @app.get("/api/bootstrap")
    async def bootstrap() -> dict[str, object]:
        raw = _require_mapping(await _invoke(services.bootstrap_provider))
        return _normalise_bootstrap(raw)

    @app.get("/api/candidates")
    async def candidates() -> dict[str, object]:
        raw = await _invoke(services.candidates_provider)
        return _normalise_collection(raw, key="candidates", maximum=10)

    @app.get("/api/positions")
    async def positions() -> dict[str, object]:
        raw = await _invoke(services.positions_provider)
        return _normalise_collection(raw, key="positions")

    @app.get("/api/learning")
    async def learning() -> dict[str, object]:
        return _normalise_learning_summary(
            _require_mapping(await _invoke(services.learning_provider))
        )

    @app.get("/api/research-top10")
    async def research_top10() -> dict[str, object]:
        try:
            raw = (
                read_research_top10()
                if services.research_top10_provider is None
                else await _invoke(services.research_top10_provider)
            )
            return safe_research_top10_read_model(raw)
        except Exception:
            return unavailable_research_top10_read_model(
                "RESEARCH_TOP10_UNAVAILABLE"
            )

    @app.get("/api/equity-pool/latest")
    async def equity_pool_latest() -> dict[str, object]:
        if services.equity_pool_provider is None:
            return _safe_equity_pool_read_model({
                "status": "UNAVAILABLE", "decision": "NO_TRADE",
                "reason_codes": ("EQUITY_POOL_UNAVAILABLE",), "selected": (),
                "decision_authority": "SUPPORTING_ONLY",
                "instruction_creation_allowed": False, "order_allowed": False,
            })
        try:
            raw = _require_mapping(await _invoke(services.equity_pool_provider))
        except Exception:
            return _safe_equity_pool_read_model({
                "status": "UNAVAILABLE", "decision": "NO_TRADE",
                "reason_codes": ("EQUITY_POOL_PROVIDER_FAILED",),
                "decision_authority": "SUPPORTING_ONLY",
                "instruction_creation_allowed": False, "order_allowed": False,
            })
        if (raw.get("decision_authority") != "SUPPORTING_ONLY"
                or raw.get("instruction_creation_allowed") is not False
                or raw.get("order_allowed") is not False):
            raise HTTPException(status_code=503, detail="equity pool authority invalid")
        return _safe_equity_pool_read_model(raw)

    @app.get("/api/option-pool/latest")
    async def option_pool_latest() -> dict[str, object]:
        if services.option_pool_provider is None:
            return _safe_option_pool_read_model({
                "status": "UNAVAILABLE", "decision": "NO_TRADE",
                "reason_codes": ("OPTION_STRUCTURE_POOL_UNAVAILABLE",),
                "decisions": (), "decision_authority": "SUPPORTING_ONLY",
                "entry_authority": False, "approval_eligible": False,
                "instruction_creation_allowed": False, "order_allowed": False,
            })
        try:
            raw = _require_mapping(await _invoke(services.option_pool_provider))
        except Exception:
            return _safe_option_pool_read_model({
                "status": "UNAVAILABLE", "decision": "NO_TRADE",
                "reason_codes": ("OPTION_STRUCTURE_POOL_PROVIDER_FAILED",),
                "decisions": (), "decision_authority": "SUPPORTING_ONLY",
                "entry_authority": False, "approval_eligible": False,
                "instruction_creation_allowed": False, "order_allowed": False,
            })
        if (raw.get("decision_authority") != "SUPPORTING_ONLY"
                or raw.get("entry_authority") is not False
                or raw.get("approval_eligible") is not False
                or raw.get("instruction_creation_allowed") is not False
                or raw.get("order_allowed") is not False):
            raise HTTPException(status_code=503, detail="option pool authority invalid")
        return _safe_option_pool_read_model(raw)

    @app.post("/api/research-top10/indicative")
    async def after_hours_indicative(
        request: AfterHoursIndicativeRequest,
    ) -> dict[str, object]:
        if services.after_hours_indicative_provider is None:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: after-hours indicative quotes unavailable",
            )
        if request.confirmation_token != "READ_AFTER_HOURS_OPTION_MARKS":
            raise HTTPException(status_code=422, detail="invalid confirmation token")
        try:
            raw = _require_mapping(
                await run_in_threadpool(services.after_hours_indicative_provider)
            )
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: after-hours indicative quote read failed",
            ) from exc
        try:
            return _normalise_after_hours_indicative(raw)
        except SecretLikeFieldError as exc:
            raise HTTPException(
                status_code=502,
                detail="unsafe after-hours indicative read model",
            ) from exc

    @app.get("/api/research-top10/indicative")
    async def latest_after_hours_indicative() -> dict[str, object]:
        """Return only the latest passive cache without requesting broker data."""

        if services.after_hours_latest_provider is None:
            return {
                "status": "UNAVAILABLE",
                "decision": "NO_TRADE",
                "reason_codes": ["AFTER_HOURS_INDICATIVE_UNAVAILABLE"],
                "candidates": [],
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
                "review_only": True,
                "direct_order_submission": False,
            }
        try:
            raw = _require_mapping(
                await run_in_threadpool(services.after_hours_latest_provider)
            )
        except Exception:
            raw = {
                "status": "UNAVAILABLE",
                "reason_codes": ["AFTER_HOURS_INDICATIVE_READ_FAILED"],
                "candidates": [],
            }
        try:
            return _normalise_after_hours_indicative(raw)
        except SecretLikeFieldError:
            return _normalise_after_hours_indicative(
                {
                    "status": "UNAVAILABLE",
                    "reason_codes": ["AFTER_HOURS_INDICATIVE_UNSAFE_PAYLOAD"],
                    "candidates": [],
                }
            )

    @app.get("/api/weekly-brief")
    async def weekly_brief() -> dict[str, object]:
        """Return the provisional weekly research brief with no action authority."""

        raw = await _read_optional_phase2_snapshot(
            services.weekly_brief_provider,
        )
        return _normalise_weekly_brief(raw)

    @app.get("/api/learning/records")
    async def learning_records(
        record_type: str | None = None,
        challenger_version: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        normalized_type = _normalise_learning_record_type(record_type)
        normalized_challenger = _normalise_optional_learning_text(
            challenger_version, field="challenger_version"
        )
        normalized_limit = _normalise_learning_limit(limit, maximum=500)
        if services.learning_records_provider is None:
            payload: Mapping[str, object] = {
                "records": [],
                "count": 0,
                "filters": {
                    "record_type": normalized_type,
                    "challenger_version": normalized_challenger,
                },
            }
        else:
            try:
                payload = _require_mapping(
                    await _invoke(
                        services.learning_records_provider,
                        normalized_type,
                        normalized_challenger,
                        normalized_limit,
                    )
                )
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        return _normalise_learning_read_model(payload)

    @app.get("/api/learning/records/{record_id}")
    async def learning_record(record_id: str) -> dict[str, object]:
        identifier = _normalise_learning_identifier(record_id, field="record_id")
        if services.learning_record_provider is None:
            raise HTTPException(status_code=404, detail="learning record not found")
        try:
            payload = _require_mapping(
                await _invoke(services.learning_record_provider, identifier)
            )
        except LearningRecordNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _normalise_learning_read_model(payload)

    @app.get("/api/learning/predictions/{prediction_id}/replay")
    async def learning_replay(prediction_id: str) -> dict[str, object]:
        identifier = _normalise_learning_identifier(
            prediction_id, field="prediction_id"
        )
        if services.learning_replay_provider is None:
            raise HTTPException(status_code=404, detail="learning replay not found")
        try:
            payload = _require_mapping(
                await _invoke(services.learning_replay_provider, identifier)
            )
        except LearningRecordNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _normalise_learning_read_model(payload)

    @app.get("/api/learning/predictions/{prediction_id}/similar")
    async def learning_similar(
        prediction_id: str,
        limit: int = 10,
    ) -> dict[str, object]:
        identifier = _normalise_learning_identifier(
            prediction_id, field="prediction_id"
        )
        normalized_limit = _normalise_learning_limit(limit, maximum=100)
        if services.learning_similarity_provider is None:
            raise HTTPException(status_code=404, detail="learning prediction not found")
        try:
            payload = _require_mapping(
                await _invoke(
                    services.learning_similarity_provider,
                    identifier,
                    normalized_limit,
                )
            )
        except LearningRecordNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _normalise_learning_read_model(payload)

    @app.get("/api/news")
    async def news() -> dict[str, object]:
        """Return a provider-agnostic, display-only news projection."""

        return await _read_only_feed(services.news_provider, key="news")

    @app.get("/api/calendar")
    async def calendar() -> dict[str, object]:
        """Return a provider-agnostic, display-only event-calendar projection."""

        return await _read_only_feed(services.calendar_provider, key="calendar")

    @app.get("/api/advisory")
    async def advisory() -> dict[str, object]:
        """Return one cached supporting-only Phase 2 advisory snapshot."""

        raw = await _read_optional_phase2_snapshot(services.advisory_provider)
        return _normalise_phase2_advisory(raw)

    @app.get("/api/fundamentals")
    async def fundamentals() -> dict[str, object]:
        """Return immutable point-in-time company facts as supporting evidence."""

        raw = await _read_safe_mapping(
            services.fundamentals_provider,
            unavailable="FUNDAMENTALS_UNAVAILABLE",
        )
        return _normalise_fundamentals(raw)

    @app.get("/api/source-evidence")
    async def source_evidence() -> dict[str, object]:
        """Return six independent cached secondary-source evidence rows."""

        raw = await _read_optional_phase2_snapshot(
            services.source_evidence_provider
        )
        return _normalise_source_evidence(raw)

    @app.get("/api/positioning")
    async def positioning() -> dict[str, object]:
        """Expose Max Pain/walls/PCR/GEX as supporting-only evidence."""

        raw = await _read_safe_mapping(
            services.positioning_provider,
            unavailable="NO_TRADE: positioning analytics unavailable",
        )
        return _normalise_positioning(raw)

    @app.get("/api/readiness")
    async def readiness() -> dict[str, object]:
        try:
            if services.readiness_provider is None:
                payload = default_readiness_report().as_dict()
            else:
                payload = dict(
                    _require_mapping(await _invoke(services.readiness_provider))
                )
            assert_no_secret_like(payload)
            if payload.get("review_only") is not True:
                raise ValueError("readiness report must be review-only")
            if payload.get("direct_order_submission") is not False:
                raise ValueError("readiness report cannot grant order submission")
            supplied_hash = payload.get("content_hash")
            if not isinstance(supplied_hash, str) or not re.fullmatch(
                r"[0-9a-f]{64}", supplied_hash
            ):
                raise ValueError("readiness report has no valid content hash")
            hash_payload = dict(payload)
            hash_payload.pop("content_hash")
            if canonical_hash(hash_payload) != supplied_hash:
                raise ValueError("readiness report content hash does not match")
            return payload
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=502,
                detail="invalid readiness provider payload",
            ) from exc

    @app.get("/api/configuration/providers")
    async def provider_configuration() -> dict[str, object]:
        """Expose only local provider presence; credential material never crosses the API."""

        if services.provider_configuration_provider is None:
            return _normalise_provider_configuration({})
        try:
            raw = _require_mapping(
                await _invoke(services.provider_configuration_provider)
            )
        except Exception:
            return _normalise_provider_configuration({}, failed=True)
        return _normalise_provider_configuration(raw)

    @app.get("/api/scans/latest")
    async def latest_scan() -> dict[str, object]:
        payload = _normalise_allocation_read_model(
            await _read_safe_mapping(
                services.latest_scan_provider,
                unavailable="NO_TRADE: immutable scan ledger unavailable",
            )
        )
        timing = _normalise_scan_operational_timing(
            payload.get("operational_timing")
        )
        scan_run_id = str(payload.get("scan_run_id", "")).strip()
        if timing is not None and timing.get("scan_run_id") != scan_run_id:
            timing = None
        if timing is None:
            payload.pop("operational_timing", None)
        else:
            payload["operational_timing"] = timing
        return payload

    @app.post("/api/scans/run-now")
    async def run_scan_now(
        request: ImmediateReadOnlyScanRequest,
    ) -> dict[str, object]:
        if services.immediate_scan_handler is None:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: immediate read-only scan unavailable",
            )
        if request.confirmation_token != "RUN_READ_ONLY_SCAN_NOW":
            raise HTTPException(status_code=422, detail="invalid confirmation token")
        try:
            if request.scope == "BOUNDED_MARKET":
                raw = await run_in_threadpool(
                    services.immediate_scan_handler,
                    request.scope,
                )
            else:
                raw = await run_in_threadpool(services.immediate_scan_handler)
            return _require_mapping(raw)
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: immediate read-only scan failed",
            ) from exc

    @app.post("/api/diagnostics/option-market-data")
    async def option_market_data_diagnostic(
        request: ReadOnlyOptionMarketDataDiagnosticRequest,
    ) -> dict[str, object]:
        """Run one paced option subscription without exposing broker writes."""

        handler = services.option_market_data_diagnostic_handler
        if handler is None:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: option market-data diagnostic unavailable",
            )
        try:
            raw = await run_in_threadpool(
                handler,
                request.model_dump(mode="python"),
            )
            return _require_mapping(raw)
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: option market-data diagnostic failed",
            ) from exc

    @app.post("/api/diagnostics/feature-sources")
    async def feature_sources_diagnostic(
        request: ReadOnlyFeatureSourcesDiagnosticRequest,
    ) -> dict[str, object]:
        """Return raw source observations without computing model features."""

        handler = services.feature_sources_diagnostic_handler
        if handler is None:
            raise HTTPException(status_code=503, detail="FEATURE_SOURCE_DIAGNOSTIC_UNAVAILABLE")
        try:
            raw = await run_in_threadpool(handler, request.model_dump(mode="python"))
        except ValueError as exc:
            if str(exc) in {
                "FEATURE_SOURCE_DIAGNOSTIC_REQUEST_INVALID",
                "FEATURE_SOURCE_SYMBOL_NOT_ALLOWED",
            }:
                raise HTTPException(status_code=422, detail=str(exc)) from None
            raise HTTPException(status_code=503, detail="FEATURE_SOURCE_DIAGNOSTIC_FAILED") from None
        except Exception:
            raise HTTPException(status_code=503, detail="FEATURE_SOURCE_DIAGNOSTIC_FAILED") from None
        try:
            return validate_feature_sources_response(raw, symbol=request.symbol)
        except (ArithmeticError, KeyError, TypeError, ValueError):
            raise HTTPException(status_code=502, detail="FEATURE_SOURCE_DIAGNOSTIC_RESPONSE_INVALID") from None

    @app.get("/api/diagnostics/feature-source-cache")
    async def feature_source_cache() -> dict[str, object]:
        """Inspect cached source lineage and consumption without acquiring data."""

        return await _read_safe_mapping(
            services.feature_source_cache_provider,
            unavailable="FEATURE_SOURCE_CACHE_UNAVAILABLE",
        )

    @app.get("/api/scans/campaign")
    async def immediate_scan_campaign() -> dict[str, object]:
        return await _read_safe_mapping(
            services.immediate_scan_campaign_provider,
            unavailable="NO_TRADE: immediate scan campaign unavailable",
        )

    @app.get("/api/rankings/latest")
    async def latest_ranking() -> dict[str, object]:
        raw = await _read_safe_mapping(
            services.latest_ranking_provider,
            unavailable="NO_TRADE: immutable ranking ledger unavailable",
        )
        return _normalise_ranking(raw)

    @app.get("/api/rankings/{ranking_snapshot_id}")
    async def ranking(ranking_snapshot_id: str) -> dict[str, object]:
        _validate_identifier(ranking_snapshot_id, "ranking snapshot id")
        if services.ranking_provider is None:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: immutable ranking ledger unavailable",
            )
        raw = _require_mapping(
            await _invoke(services.ranking_provider, ranking_snapshot_id)
        )
        return _normalise_ranking(_safe_mapping(raw))

    @app.get(
        "/api/scans/{scan_run_id}/candidates/{candidate_id}/evidence"
    )
    async def candidate_evidence(
        scan_run_id: str,
        candidate_id: str,
    ) -> dict[str, object]:
        _validate_identifier(scan_run_id, "scan run id")
        _validate_identifier(candidate_id, "candidate id")
        if services.candidate_evidence_provider is None:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: candidate evidence ledger unavailable",
            )
        raw = _require_mapping(
            await _invoke(
                services.candidate_evidence_provider,
                scan_run_id,
                candidate_id,
            )
        )
        return _normalise_candidate_evidence(raw)

    @app.get("/api/management/current")
    async def management() -> dict[str, object]:
        payload = _normalise_management(await _read_safe_mapping(
            services.management_provider,
            unavailable="NO_TRADE: position management unavailable",
        ))
        if payload.get("available") is False or str(
            payload.get("status") or ""
        ).upper() == "UNAVAILABLE":
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: position management unavailable",
            )
        payload.setdefault("mode", "POSITION_MANAGEMENT")
        payload.setdefault("review_only", True)
        payload.setdefault("direct_order_submission", False)
        return payload

    @app.post(
        "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
        status_code=201,
    )
    async def create_rank_one_challenge(
        ranking_snapshot_id: str,
        candidate_id: str,
        request: RankOneChallengeRequest,
    ) -> dict[str, object]:
        del request
        _validate_identifier(ranking_snapshot_id, "ranking snapshot id")
        _validate_identifier(candidate_id, "candidate id")
        if services.rank_one_challenge_handler is None:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: rank-one challenge service unavailable",
            )
        try:
            raw = _require_mapping(
                await _invoke(
                    services.rank_one_challenge_handler,
                    ranking_snapshot_id,
                    candidate_id,
                )
            )
        except RankOneAuthorizationForbidden as exc:
            raise HTTPException(
                status_code=403,
                detail=(str(exc).strip() or "VIEW_ONLY")[:240],
            ) from exc

        except ProposalApprovalConflict as exc:
            raise HTTPException(
                status_code=409,
                detail=(str(exc).strip() or "ranking authority changed")[:240],
            ) from exc
        except OptionsCopilotUnavailable as exc:
            raise HTTPException(
                status_code=503,
                detail=(str(exc).strip() or "NO_TRADE")[:240],
            ) from exc
        return _normalise_challenge(raw, ranking_snapshot_id, candidate_id)

    @app.post(
        "/api/approval-challenges/{challenge_id}/confirm",
        status_code=202,
    )
    async def confirm_approval_challenge(
        challenge_id: str,
        request: ApprovalConfirmationRequest,
    ) -> dict[str, object]:
        _validate_identifier(challenge_id, "challenge id")
        if services.challenge_confirmation_handler is None:
            raise HTTPException(
                status_code=503,
                detail="NO_TRADE: approval confirmation service unavailable",
            )
        try:
            raw = _require_mapping(
                await _invoke(
                    services.challenge_confirmation_handler,
                    challenge_id,
                    request.model_dump(mode="json"),
                )
            )
        except RankOneAuthorizationForbidden as exc:
            raise HTTPException(
                status_code=403,
                detail=(str(exc).strip() or "VIEW_ONLY")[:240],
            ) from exc
        except ProposalApprovalConflict as exc:
            raise HTTPException(
                status_code=409,
                detail=(str(exc).strip() or "approval authority changed")[:240],
            ) from exc
        except OptionsCopilotUnavailable as exc:
            raise HTTPException(
                status_code=503,
                detail=(str(exc).strip() or "NO_TRADE")[:240],
            ) from exc
        return _normalise_pending_handoff(raw)

    @app.get("/api/approvals/{approval_id}")
    async def approval_status(approval_id: str) -> dict[str, object]:
        if services.approval_status_provider is None:
            raise HTTPException(
                status_code=503,
                detail="review handoff status service unavailable",
            )
        if not approval_id.strip() or len(approval_id) > 160:
            raise HTTPException(status_code=422, detail="invalid approval id")
        try:
            raw = _require_mapping(
                await _invoke(services.approval_status_provider, approval_id)
            )
        except ApprovalStatusNotFound as exc:
            raise HTTPException(status_code=404, detail="approval not found") from exc
        return _normalise_approval_status(approval_id, raw)

    return app


async def _read_safe_mapping(
    provider: SnapshotProvider | None,
    *,
    unavailable: str,
) -> dict[str, object]:
    if provider is None:
        raise HTTPException(status_code=503, detail=unavailable)
    raw = _require_mapping(await _invoke(provider))
    return _safe_mapping(raw)


async def _read_optional_phase2_snapshot(
    provider: SnapshotProvider | None,
) -> Mapping[str, object] | None:
    """Read one cached callback and converge every failure to unavailable."""

    if provider is None:
        return None
    try:
        raw = await _invoke(provider)
    except Exception:
        return None
    return raw if isinstance(raw, Mapping) else None


def _safe_mapping(raw: Mapping[str, Any]) -> dict[str, object]:
    payload: dict[str, object] = dict(raw)
    try:
        assert_no_secret_like(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail="unsafe immutable read model",
        ) from exc
    return payload


_MANAGEMENT_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema",
        "status",
        "decision",
        "available",
        "mode",
        "reason",
        "reason_codes",
        "suppressed_reason_codes",
        "generated_at",
        "broker_snapshot_hash",
        "result_hash",
    }
)

_POSITIONING_TOP_LEVEL_FIELDS = frozenset(
    {"schema_version", "status", "reasons", "count"}
)
_POSITIONING_ROW_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "reasons",
        "underlying",
        "expiration",
        "generated_at",
        "data_asof",
        "data_age_seconds",
        "newest_data_asof",
        "newest_data_age_seconds",
        "maximum_data_age_seconds",
        "observed_contract_count",
        "unique_contract_count",
        "expected_contract_count",
        "option_chain_coverage_rate",
        "stale_contract_count",
        "future_contract_count",
        "delayed_contract_count",
        "missing_open_interest_count",
        "missing_open_interest_rate",
        "missing_greeks_count",
        "missing_greeks_rate",
        "missing_gamma_count",
        "missing_gamma_rate",
        "gex_usable_contract_count",
        "gex_usable_contract_rate",
        "max_pain",
        "call_wall",
        "put_wall",
        "call_open_interest",
        "put_open_interest",
        "put_call_open_interest_ratio",
        "estimated_net_gex_usd_per_one_percent",
        "limitations",
        "source",
        "scan_run_id",
        "broker_snapshot_hash",
        "chain_scope",
        "coverage_limitation",
    }
)
_MANAGEMENT_CANDIDATE_FIELDS = frozenset(
    {
        "schema",
        "candidate_id",
        "management_kind",
        "symbol",
        "structure",
        "review_state",
        "thesis_invalidation_state",
        "risk_stop_state",
        "profit_take_state",
        "time_stop_state",
        "entry_net_cost_usd",
        "entry_net_credit_usd",
        "entry_max_loss_usd",
        "entry_max_profit_usd",
        "stop_review_cashflow_usd",
        "profit_review_cashflow_usd",
        "expiration",
        "combo_quantity_before",
        "combo_quantity_after",
        "unit_ratio",
        "executable_close_cashflow_usd",
        "estimated_commission_usd",
        "normal_slippage_usd",
        "stress_slippage_usd",
        "estimated_execution_cost_usd",
        "all_in_close_cashflow_usd",
        "transition_proof_hash",
        "exit_contract_hash",
        "assignment_assumption_hash",
        "broker_snapshot_hash",
        "quote_batch_id",
        "quote_batch_hash",
        "oldest_quote_age_seconds",
        "maximum_leg_skew_seconds",
        "secdef_hashes",
        "positions_state_hash",
        "execution_cost_contract_version",
        "execution_cost_contract_hash",
        "candidate_hash",
    }
)
_MANAGEMENT_LEG_FIELDS = frozenset(
    {
        "contract_id",
        "local_symbol",
        "expiration",
        "strike",
        "right",
        "current_signed_quantity",
        "signed_quantity_delta",
        "action",
        "action_quantity",
        "multiplier",
        "executable_price",
        "bid",
        "ask",
        "quote_observed_at",
        "secdef_identity_hash",
    }
)
_MANAGEMENT_POSITION_FIELDS = frozenset(
    {
        "contract_id",
        "symbol",
        "local_symbol",
        "security_type",
        "currency",
        "exchange",
        "signed_quantity",
        "asof",
    }
)
_MANAGEMENT_RISK_FIELDS = frozenset(
    {"max_loss_usd", "exposure_usd", "capital_usage_usd"}
)
_MANAGEMENT_PAYOFF_FIELDS = frozenset(
    {
        "max_loss_usd",
        "max_profit_usd",
        "unbounded_profit",
        "net_opening_cashflow_usd",
        "estimated_future_exit_cost_usd",
        "breakevens",
        "geometry_hash",
        "payoff_hash",
    }
)
_MANAGEMENT_EXIT_FIELDS = frozenset(
    {
        "thesis_invalidation",
        "risk_stop",
        "profit_take",
        "time_stop",
        "maximum_holding_date",
        "bad_quote_action",
    }
)


def _normalise_positioning(raw: Mapping[str, Any]) -> dict[str, object]:
    payload = _safe_mapping(raw)
    result = {
        key: payload[key]
        for key in _POSITIONING_TOP_LEVEL_FIELDS
        if key in payload
    }
    rows = payload.get("positioning", ())
    if isinstance(rows, Sequence) and not isinstance(
        rows,
        (str, bytes, bytearray, memoryview),
    ):
        result["positioning"] = [
            {
                key: item[key]
                for key in _POSITIONING_ROW_FIELDS
                if key in item
            }
            for item in rows[:20]
            if isinstance(item, Mapping)
        ]
    else:
        result["positioning"] = []
    result["count"] = len(result["positioning"])
    result["decision_authority"] = "SUPPORTING_ONLY"
    result["supporting_only"] = True
    result["affects_eligibility"] = False
    result["approval_allowed"] = False
    result["instruction_allowed"] = False
    result["order_allowed"] = False
    return result


def _normalise_management(
    raw: Mapping[str, Any], *, now: datetime | None = None,
) -> dict[str, object]:
    """Project a management preview without exposing broker or creator authority.

    Position management is intentionally a separate read model.  The API keeps
    only proof/cost/exit fields needed for human review and overwrites every
    action-related flag, even if a provider attempts to smuggle one in.
    """

    payload = _safe_mapping(raw)
    result = _management_pick(payload, _MANAGEMENT_TOP_LEVEL_FIELDS)
    raw_candidates = payload.get("candidates", ())
    candidates: list[dict[str, object]] = []
    if isinstance(raw_candidates, Sequence) and not isinstance(
        raw_candidates, (str, bytes, bytearray)
    ):
        for raw_candidate in raw_candidates[:10]:
            if not isinstance(raw_candidate, Mapping):
                continue
            if raw_candidate.get("schema") == HOLDINGS_CLOSE_SCHEMA:
                candidates.append(project_holdings_close_preview(raw_candidate, now=now))
                continue
            candidate = _management_pick(
                raw_candidate, _MANAGEMENT_CANDIDATE_FIELDS
            )
            candidate["execution_legs"] = _management_rows(
                raw_candidate.get("execution_legs"), _MANAGEMENT_LEG_FIELDS
            )
            for field in ("before_payoff", "after_payoff"):
                candidate[field] = _management_nested(
                    raw_candidate.get(field), _MANAGEMENT_PAYOFF_FIELDS
                )
            for field in ("before_risk", "after_risk"):
                candidate[field] = _management_nested(
                    raw_candidate.get(field), _MANAGEMENT_RISK_FIELDS
                )
            candidate["exit_plan"] = _management_nested(
                raw_candidate.get("exit_plan"), _MANAGEMENT_EXIT_FIELDS
            )
            proof = raw_candidate.get("transition_proof")
            candidate["transition_proof"] = _normalise_management_proof(proof)
            candidate["review_only"] = True
            candidate["dry_run_only"] = True
            candidate["approval_enabled"] = False
            candidate["direct_order_submission"] = False
            candidates.append(candidate)

    result["candidates"] = candidates
    result["count"] = len(candidates)
    if any(candidate.get("schema") == HOLDINGS_CLOSE_SCHEMA for candidate in candidates):
        result["status"] = "NO_TRADE"
        result["decision"] = "NO_TRADE"
        result["reason"] = "HOLDINGS_CLOSE_PREVIEW_UNVERIFIED"
    result["available"] = payload.get("available") is not False
    result["mode"] = "POSITION_MANAGEMENT"
    result["review_only"] = True
    result["approval_enabled"] = False
    result["direct_order_submission"] = False
    result["order_submitted"] = False
    result["transmitted_to_broker"] = False
    result["external_attempt_count"] = 0
    result["instruction_count"] = 0
    result["action_text"] = "Create IBKR review instruction"
    result["action_enabled"] = False
    result["creator_transport_status"] = "CREATOR_TRANSPORT_UNAVAILABLE"
    return result


def _normalise_management_proof(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    allowed = frozenset(
        {
            "management_kind",
            "broker_snapshot_hash",
            "positions_state_hash",
            "quote_batch_id",
            "quote_batch_hash",
            "exit_contract_hash",
            "execution_cost_contract_version",
            "execution_cost_contract_hash",
            "verified_at",
            "proof_hash",
        }
    )
    proof = _management_pick(value, allowed)
    proof["before_positions"] = _management_rows(
        value.get("before_positions"), _MANAGEMENT_POSITION_FIELDS
    )
    proof["after_positions"] = _management_rows(
        value.get("after_positions"), _MANAGEMENT_POSITION_FIELDS
    )
    proof["deltas"] = _management_rows(
        value.get("deltas"),
        frozenset({"contract_id", "signed_quantity_delta"}),
    )
    proof["before_risk"] = _management_nested(
        value.get("before_risk"), _MANAGEMENT_RISK_FIELDS
    )
    proof["after_risk"] = _management_nested(
        value.get("after_risk"), _MANAGEMENT_RISK_FIELDS
    )
    proof["capital_usage"] = _management_nested(
        value.get("capital_usage"),
        frozenset(
            {
                "before_capital_usage_usd",
                "after_capital_usage_usd",
                "capital_change_usd",
                "before_gross_contracts",
                "after_gross_contracts",
                "before_net_short_contracts",
                "after_net_short_contracts",
                "non_increasing",
            }
        ),
    )
    proof["secdef_bindings"] = _management_rows(
        value.get("secdef_bindings"),
        frozenset(
            {
                "contract_id",
                "symbol",
                "local_symbol",
                "trading_class",
                "multiplier",
                "exchange",
                "expiration",
                "strike",
                "right",
                "identity_hash",
            }
        ),
    )
    return proof


def _management_pick(
    value: Mapping[str, Any], allowed: frozenset[str]
) -> dict[str, object]:
    return {key: value[key] for key in allowed if key in value}


def _management_nested(
    value: object, allowed: frozenset[str]
) -> dict[str, object]:
    return _management_pick(value, allowed) if isinstance(value, Mapping) else {}


def _management_rows(
    value: object, allowed: frozenset[str]
) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        return []
    return [
        _management_pick(item, allowed)
        for item in value[:20]
        if isinstance(item, Mapping)
    ]


def _validate_identifier(value: str, label: str) -> str:
    identifier = value.strip() if isinstance(value, str) else ""
    if (
        not identifier
        or len(identifier) > 160
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", identifier)
    ):
        raise HTTPException(status_code=422, detail=f"invalid {label}")
    return identifier


_WEEKLY_BRIEF_HASH_FIELDS = frozenset(
    {
        "schema",
        "version",
        "status",
        "decision",
        "decision_authority",
        "execution_allowed",
        "review_allowed",
        "combination_generation_allowed",
        "week_start",
        "first_session_date",
        "cutoff_at",
        "calendar_hash",
        "windows",
        "source_bundle_hash",
        "source_health",
        "prior_week_items",
        "upcoming_items",
        "next_preview_items",
        "watch_items",
        "unavailable_fields",
        "target_count",
        "watch_count",
        "idempotency_key",
    }
)


def _normalise_weekly_brief(raw: object) -> dict[str, object]:
    if not isinstance(raw, Mapping):
        return _weekly_brief_not_run(("WEEKLY_BRIEF_NOT_AVAILABLE",))
    status = str(raw.get("status") or "").strip().upper()
    if status != "PROVISIONAL":
        reasons = tuple(_normalise_text_list(raw.get("reason_codes"), maximum=8))
        return _weekly_brief_not_run(reasons or ("WEEKLY_BRIEF_NOT_AVAILABLE",))
    expected_keys = _WEEKLY_BRIEF_HASH_FIELDS | {
        "content_hash",
        "read_model_schema",
        "persistence",
    }
    content_hash = _normalise_digest(raw.get("content_hash"))
    source_bundle_hash = _normalise_digest(raw.get("source_bundle_hash"))
    calendar_hash = _normalise_digest(raw.get("calendar_hash"))
    idempotency_key = _normalise_digest(raw.get("idempotency_key"))
    hash_payload = {key: raw.get(key) for key in _WEEKLY_BRIEF_HASH_FIELDS}
    valid = (
        set(raw) <= expected_keys
        and raw.get("schema") == "options_copilot.weekly_brief.v1"
        and raw.get("version") == 1
        and raw.get("decision") == "OBSERVATION_ONLY"
        and raw.get("decision_authority") == "SUPPORTING_ONLY"
        and raw.get("execution_allowed") is False
        and raw.get("review_allowed") is False
        and raw.get("combination_generation_allowed") is False
        and raw.get("target_count") == 10
        and isinstance(raw.get("watch_count"), int)
        and not isinstance(raw.get("watch_count"), bool)
        and 0 <= int(raw.get("watch_count", -1)) <= 10
        and content_hash is not None
        and source_bundle_hash is not None
        and calendar_hash is not None
        and idempotency_key is not None
        and canonical_hash(hash_payload) == content_hash
    )
    watches = _normalise_weekly_watches(raw.get("watch_items"))
    if not valid or len(watches) != raw.get("watch_count"):
        return _weekly_brief_not_run(("WEEKLY_BRIEF_API_PROJECTION_INVALID",))
    source_health = _normalise_weekly_source_health(raw.get("source_health"))
    mandatory_sources = [
        item for item in source_health if item.get("mandatory") is True
    ]
    mandatory_names = [item.get("source") for item in mandatory_sources]
    if (
        not mandatory_sources
        or any(item.get("status") != "READY" for item in mandatory_sources)
        or any(not isinstance(name, str) or not name for name in mandatory_names)
        or len(set(mandatory_names)) != len(mandatory_names)
    ):
        return _weekly_brief_not_run(("WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE",))
    windows = raw.get("windows") if isinstance(raw.get("windows"), Mapping) else {}
    persistence = raw.get("persistence")
    persistence_mapping = persistence if isinstance(persistence, Mapping) else {}
    sequence = persistence_mapping.get("sequence")
    safe_sequence = (
        sequence
        if isinstance(sequence, int) and not isinstance(sequence, bool) and sequence >= 1
        else None
    )
    result = {
        "read_model_schema": "options_copilot.weekly_brief_read_model.v1",
        "schema": "options_copilot.weekly_brief.v1",
        "version": 1,
        "status": "PROVISIONAL",
        "decision": "OBSERVATION_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "execution_allowed": False,
        "review_allowed": False,
        "combination_generation_allowed": False,
        "week_start": _clean_text(raw.get("week_start"), 16),
        "first_session_date": _clean_text(raw.get("first_session_date"), 16),
        "cutoff_at": _normalise_timestamp(raw.get("cutoff_at")),
        "calendar_hash": calendar_hash,
        "windows": {
            name: {
                "start": _normalise_timestamp(value.get("start")),
                "end_exclusive": _normalise_timestamp(value.get("end_exclusive")),
            }
            for name in ("prior_week", "upcoming", "next_week_preview")
            if isinstance((value := windows.get(name)), Mapping)
        },
        "source_bundle_hash": source_bundle_hash,
        "source_health": source_health[:16],
        "prior_week_items": _normalise_weekly_items(raw.get("prior_week_items")),
        "upcoming_items": _normalise_weekly_items(raw.get("upcoming_items")),
        "next_preview_items": _normalise_weekly_items(raw.get("next_preview_items")),
        "watch_items": watches,
        "unavailable_fields": {
            "option_chain": {"status": "UNAVAILABLE", "reason_codes": ["OPTION_CHAIN_NOT_BOUND"]},
            "option_quotes": {"status": "UNAVAILABLE", "reason_codes": ["OPTION_QUOTES_NOT_BOUND"]},
            "strategy_nav": {"status": "UNAVAILABLE", "reason_codes": ["STRATEGY_NAV_NOT_BOUND"]},
            "positioning": {"status": "UNAVAILABLE", "reason_codes": ["POSITIONING_NOT_BOUND"]},
        },
        "target_count": 10,
        "watch_count": len(watches),
        "idempotency_key": idempotency_key,
        "content_hash": content_hash,
        "persistence": {
            "sequence": safe_sequence,
            "row_hash": _normalise_digest(persistence_mapping.get("row_hash")),
            "append_only": persistence_mapping.get("append_only") is True,
        },
    }
    assert_no_secret_like(result)
    return result


def _weekly_brief_not_run(reasons: Sequence[str]) -> dict[str, object]:
    return {
        "read_model_schema": "options_copilot.weekly_brief_read_model.v1",
        "status": "NOT_RUN",
        "decision": "OBSERVATION_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "execution_allowed": False,
        "review_allowed": False,
        "combination_generation_allowed": False,
        "reason_codes": list(dict.fromkeys(reasons))[:8],
        "weekly_brief": None,
        "watch_items": [],
        "watch_count": 0,
        "target_count": 10,
    }


def _normalise_weekly_source_health(value: object) -> list[dict[str, object]]:
    rows = _mapping_sequence(value)
    if rows is None:
        return []
    result: list[dict[str, object]] = []
    for raw in rows:
        status = str(raw.get("status") or "UNAVAILABLE").upper()
        if status not in {"READY", "DEGRADED", "UNAVAILABLE"}:
            status = "UNAVAILABLE"
        result.append(
            {
                "source": _clean_text(raw.get("source"), 80),
                "status": status,
                "mandatory": raw.get("mandatory") is True,
                "observed_at": _normalise_timestamp(raw.get("observed_at")),
                "reason_codes": _normalise_text_list(raw.get("reason_codes"), maximum=8),
                "source_hash": _normalise_digest(raw.get("source_hash")),
                "health_hash": _normalise_digest(raw.get("health_hash")),
            }
        )
    return result


def _normalise_weekly_items(value: object) -> list[dict[str, object]]:
    rows = _mapping_sequence(value)
    if rows is None:
        return []
    result: list[dict[str, object]] = []
    for raw in rows[:200]:
        result.append(
            {
                "item_id": _clean_text(raw.get("item_id"), 160),
                "occurred_at": _normalise_timestamp(raw.get("occurred_at")),
                "observed_at": _normalise_timestamp(raw.get("observed_at")),
                "source": _clean_text(raw.get("source"), 120),
                "headline": _clean_text(raw.get("headline"), 400),
                "summary": _clean_text(raw.get("summary"), 1600),
                "symbols": _normalise_symbols(raw.get("symbols"))[:16],
                "affected_assets": _normalise_text_list(raw.get("affected_assets"), maximum=16),
                "direction": _clean_text(raw.get("direction"), 32),
                "supporting_evidence_ids": _normalise_text_list(raw.get("supporting_evidence_ids"), maximum=16),
                "contradicting_evidence_ids": _normalise_text_list(raw.get("contradicting_evidence_ids"), maximum=16),
                "deepseek_summary": _clean_text(raw.get("deepseek_summary"), 1600),
                "deepseek_authority": (
                    "SUPPORTING_ONLY" if raw.get("deepseek_summary") is not None else None
                ),
                "source_hash": _normalise_digest(raw.get("source_hash")),
                "item_hash": _normalise_digest(raw.get("item_hash")),
            }
        )
    return result


def _normalise_weekly_watches(value: object) -> list[dict[str, object]]:
    rows = _mapping_sequence(value)
    if rows is None or len(rows) > 10:
        return []
    result: list[dict[str, object]] = []
    for raw in rows:
        layers = _mapping_sequence(raw.get("layers"))
        if layers is None or len(layers) != 6:
            return []
        safe_layers = []
        for layer in layers:
            safe_layers.append(
                {
                    "gate_id": _clean_text(layer.get("gate_id"), 80),
                    "status": _clean_text(layer.get("status"), 32),
                    "reason_codes": _normalise_text_list(layer.get("reason_codes"), maximum=8),
                    "observed_at": _normalise_timestamp(layer.get("observed_at")),
                    "authority": "SUPPORTING_ONLY",
                    "preview_layer_hash": _normalise_digest(layer.get("preview_layer_hash")),
                }
            )
        result.append(
            {
                "watch_id": _normalise_digest(raw.get("watch_id")),
                "symbol": (_normalise_symbols(raw.get("symbol")) or [None])[0],
                "cutoff_at": _normalise_timestamp(raw.get("cutoff_at")),
                "status": "PROVISIONAL",
                "decision": "OBSERVATION_ONLY",
                "decision_authority": "SUPPORTING_ONLY",
                "layers": safe_layers,
                "preview_hash": _normalise_digest(raw.get("preview_hash")),
            }
        )
    return result


def _normalise_scan_operational_timing(value: object) -> dict[str, object] | None:
    """Project bounded, observation-only timings without decision authority."""

    if not isinstance(value, Mapping):
        return None
    if (
        value.get("schema") != "options_copilot.scan_operational_timing.v1"
        or value.get("decision_authority") != "OBSERVATION_ONLY"
        or value.get("affects_decision") is not False
    ):
        return None
    scan_run_id = str(value.get("scan_run_id", "")).strip()
    total = value.get("total_duration_ms")
    raw_stages = value.get("stages")
    if (
        not scan_run_id
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total < 0
        or isinstance(raw_stages, (str, bytes, bytearray))
        or not isinstance(raw_stages, Sequence)
        or len(raw_stages) > 32
    ):
        return None
    stages: list[dict[str, object]] = []
    for raw in raw_stages:
        if not isinstance(raw, Mapping):
            return None
        stage = str(raw.get("stage", "")).strip().upper()
        duration = raw.get("duration_ms")
        if (
            not stage
            or len(stage) > 64
            or any(not (character.isalnum() or character == "_") for character in stage)
            or isinstance(duration, bool)
            or not isinstance(duration, int)
            or duration < 0
        ):
            return None
        stages.append({"stage": stage, "duration_ms": duration})
    if sum(int(row["duration_ms"]) for row in stages) > total:
        return None
    result: dict[str, object] = {
        "schema": "options_copilot.scan_operational_timing.v1",
        "scan_run_id": scan_run_id,
        "total_duration_ms": total,
        "stages": stages,
        "decision_authority": "OBSERVATION_ONLY",
        "affects_decision": False,
    }
    timing_hash = _normalise_digest(value.get("timing_hash"))
    recorded_at = _normalise_offset_timestamp(value.get("recorded_at"))
    if timing_hash is None or recorded_at is None:
        return None
    result["timing_hash"] = timing_hash
    result["recorded_at"] = recorded_at
    return result


def _normalise_ranking(raw: Mapping[str, Any]) -> dict[str, object]:
    payload = _normalise_allocation_read_model(_safe_mapping(raw))
    raw_candidates = payload.get("candidates", ())
    if not isinstance(raw_candidates, Sequence) or isinstance(
        raw_candidates, (str, bytes, bytearray)
    ):
        raise HTTPException(status_code=502, detail="invalid ranking candidates")
    candidates: list[dict[str, object]] = []
    seen_ranks: set[int] = set()
    raw_decision = str(payload.get("decision") or "").strip().upper()
    recommendations_claimed = raw_decision in {"CANDIDATES_AVAILABLE", "TRADE"}
    if (
        "recommendations_available" in payload
        and payload.get("recommendations_available") is not True
    ):
        recommendations_claimed = False
    approval_enabled = (
        payload.get("approval_enabled") is True
        and raw_decision in {"CANDIDATES_AVAILABLE", "TRADE"}
    )
    payload["approval_enabled"] = approval_enabled
    payload["decision"] = (
        "CANDIDATES_AVAILABLE" if approval_enabled else "NO_TRADE"
    )
    for raw_candidate in raw_candidates:
        if not isinstance(raw_candidate, Mapping):
            raise HTTPException(status_code=502, detail="invalid ranking candidate")
        candidate = _safe_mapping(raw_candidate)
        rank = candidate.get("rank")
        if (
            not isinstance(rank, int)
            or isinstance(rank, bool)
            or not 1 <= rank <= 10
            or rank in seen_ranks
        ):
            raise HTTPException(status_code=502, detail="invalid immutable rank")
        seen_ranks.add(rank)
        try:
            readiness = evaluate_candidate_readiness(payload, candidate)
        except CandidateReadinessError as exc:
            raise HTTPException(
                status_code=502,
                detail=exc.reason.lower().replace("_", " "),
            ) from exc
        candidate["underlying"] = readiness.underlying
        candidate["symbol"] = readiness.underlying
        candidate["source_health"] = dict(readiness.source_health)
        candidate["account_capacity"] = dict(readiness.account_capacity)
        candidate["interaction"] = readiness.interaction
        candidate["recommendation_ready"] = bool(
            candidate.get("authorizable") is True
            and str(candidate.get("authority_status") or "").upper()
            != "A_GRADE_PENDING"
            and readiness.source_health.get("status") == "READY"
            and readiness.account_capacity.get("status") == "READY"
        )
        candidates.append(candidate)
    candidates.sort(key=lambda item: int(item["rank"]))
    if len(candidates) > 10:
        raise HTTPException(status_code=502, detail="ranking exceeds Top 10")
    main_candidates: list[dict[str, object]] = []
    preferred_by_underlying: dict[str, dict[str, object]] = {}
    for candidate in candidates:
        group_key = str(candidate["underlying"])
        preferred = preferred_by_underlying.get(group_key)
        candidate.pop("alternatives", None)
        if preferred is None:
            candidate["preferred_for_underlying"] = True
            candidate["alternatives"] = []
            preferred_by_underlying[group_key] = candidate
            main_candidates.append(candidate)
            continue
        candidate["preferred_for_underlying"] = False
        candidate["interaction"] = "VIEW_ONLY"
        alternatives = preferred["alternatives"]
        assert isinstance(alternatives, list)
        alternatives.append(candidate)
    recommendations_available = bool(
        recommendations_claimed
        and candidates
        and all(candidate.get("recommendation_ready") is True for candidate in candidates)
    )
    challengeable_rank_one = any(
        candidate.get("rank") == 1
        and candidate.get("interaction") == "CHALLENGE_ALLOWED"
        for candidate in candidates
    )
    approval_enabled = (
        approval_enabled
        and recommendations_available
        and challengeable_rank_one
    )
    payload["candidates"] = main_candidates
    payload["count"] = len(main_candidates)
    payload["total_ranked_count"] = len(candidates)
    payload["recommendations_available"] = recommendations_available
    payload["approval_enabled"] = approval_enabled
    payload["decision"] = (
        "CANDIDATES_AVAILABLE" if recommendations_available else "NO_TRADE"
    )
    payload["review_only"] = True
    payload["direct_order_submission"] = False
    research_watchlist, integrity_reason = _normalise_joint_research_watchlist(payload)
    payload["research_watchlist"] = research_watchlist
    payload["research_watchlist_integrity_reason"] = integrity_reason
    return payload


def _normalise_joint_research_watchlist(
    payload: Mapping[str, object],
) -> tuple[list[dict[str, object]], str | None]:
    immutable = payload.get("immutable_inputs")
    joint = immutable.get("joint_ranking") if isinstance(immutable, Mapping) else None
    if not isinstance(joint, Mapping) and isinstance(immutable, Mapping):
        trace = immutable.get("funnel_trace")
        joint = trace.get("joint_ranking") if isinstance(trace, Mapping) else None
    if not isinstance(joint, Mapping):
        joint = payload.get("joint_ranking")
    if not isinstance(joint, Mapping):
        trace = payload.get("funnel_trace")
        joint = trace.get("joint_ranking") if isinstance(trace, Mapping) else None
    if not isinstance(joint, Mapping):
        return [], "JOINT_RANKING_EVIDENCE_UNAVAILABLE"
    snapshot_hash = _normalise_digest(joint.get("snapshot_hash"))
    body = {key: value for key, value in joint.items() if key != "snapshot_hash"}
    raw_rows = joint.get("research_watchlist")
    executable = joint.get("executable")
    if not isinstance(raw_rows, Sequence) or isinstance(
        raw_rows, (str, bytes, bytearray)
    ) or not isinstance(executable, Sequence) or isinstance(
        executable, (str, bytes, bytearray)
    ) or joint.get("schema") != "options_copilot.joint_ranking.v1" or (
        snapshot_hash is None or canonical_hash(body) != snapshot_hash
    ):
        return [], "JOINT_RANKING_INTEGRITY_INVALID"
    for collection in (executable, raw_rows):
        for raw in collection:
            if not isinstance(raw, Mapping):
                return [], "JOINT_RANKING_ROW_INTEGRITY_INVALID"
            row_hash = _normalise_digest(raw.get("row_hash"))
            row_body = {key: value for key, value in raw.items() if key != "row_hash"}
            if row_hash is None or canonical_hash(row_body) != row_hash:
                return [], "JOINT_RANKING_ROW_INTEGRITY_INVALID"
    result: list[dict[str, object]] = []
    for raw in raw_rows[:150]:
        if not isinstance(raw, Mapping):
            continue
        candidate_id = _clean_text(raw.get("candidate_id"), maximum=128)
        symbols = _normalise_symbols(raw.get("underlying"))
        candidate_hash = _normalise_digest(raw.get("candidate_hash"))
        row_hash = _normalise_digest(raw.get("row_hash"))
        reason_codes = _normalise_text_list(raw.get("reason_codes"), maximum=32)
        score = _normalise_nonnegative_number(raw.get("score"))
        if (
            not candidate_id
            or len(symbols) != 1
            or candidate_hash is None
            or row_hash is None
            or not reason_codes
        ):
            continue
        result.append(
            {
                "candidate_id": candidate_id,
                "underlying": symbols[0],
                "candidate_hash": candidate_hash,
                "disposition": "RESEARCH_ONLY",
                "rank": None,
                "score": score,
                "reason_codes": reason_codes,
                "row_hash": row_hash,
                "review_only": True,
                "direct_order_submission": False,
            }
        )
    return result, None


def _normalise_candidate_evidence(
    raw: Mapping[str, Any],
) -> dict[str, object]:
    """Build a strict, observation-only display projection.

    The runtime may retain full immutable records for audit and replay.  The
    browser receives only explicitly named display fields; provider payloads,
    transport metadata, credentials, and order-shaped fields are never copied.
    """

    raw_status = str(raw.get("status") or "").upper()
    raw_decision = str(raw.get("decision") or "").upper()
    ready = raw_status == "READY" and raw_decision == "OBSERVATION_ONLY"
    schema = (
        _CANDIDATE_EVIDENCE_SCHEMA
        if raw.get("schema") == _CANDIDATE_EVIDENCE_SCHEMA
        else None
    )
    manifest_hash = _normalise_digest(raw.get("manifest_hash"))
    scan_run_id = _normalise_evidence_identifier(raw.get("scan_run_id"))
    candidate_id = _normalise_evidence_identifier(raw.get("candidate_id"))
    ranking_snapshot_id = _normalise_evidence_identifier(
        raw.get("ranking_snapshot_id")
    )
    raw_symbol = raw.get("symbol")
    symbols = _normalise_symbols(raw_symbol) if isinstance(raw_symbol, str) else []
    symbol = symbols[0] if len(symbols) == 1 else None
    cutoff_at = _normalise_evidence_cutoff(raw.get("cutoff_at"))

    primary_raw = _mapping_sequence(raw.get("primary", ()))
    supporting_raw = _mapping_sequence(raw.get("supporting", ()))
    contradicting_raw = _mapping_sequence(raw.get("contradicting", ()))
    primary = (
        []
        if primary_raw is None
        else [
            item
            for source in primary_raw[:32]
            if (item := _normalise_primary_evidence(source)) is not None
        ]
    )
    supporting = (
        []
        if supporting_raw is None
        else [
            item
            for source in supporting_raw[:64]
            if (item := _normalise_external_evidence(source)) is not None
        ]
    )
    contradicting = (
        []
        if contradicting_raw is None
        else [
            item
            for source in contradicting_raw[:64]
            if (item := _normalise_external_evidence(source)) is not None
        ]
    )
    projection_invalid = (
        primary_raw is None
        or supporting_raw is None
        or contradicting_raw is None
        or (primary_raw is not None and len(primary) != len(primary_raw))
        or (supporting_raw is not None and len(supporting) != len(supporting_raw))
        or (
            contradicting_raw is not None
            and len(contradicting) != len(contradicting_raw)
        )
        or (
            ready
            and (
                not primary
                or schema is None
                or manifest_hash is None
                or scan_run_id is None
                or candidate_id is None
                or ranking_snapshot_id is None
                or symbol is None
                or cutoff_at is None
            )
        )
    )

    reasons = _normalise_text_list(raw.get("reasons", ()), maximum=16)
    reason = _clean_text(raw.get("reason"), 160)
    if projection_invalid:
        ready = False
        reason = "CANDIDATE_EVIDENCE_API_PROJECTION_INVALID"
        if reason not in reasons:
            reasons.append(reason)
    if not ready:
        primary = []
        supporting = []
        contradicting = []

    payload: dict[str, object] = {
        "status": "READY" if ready else "DEGRADED",
        "decision": "OBSERVATION_ONLY" if ready else "NO_TRADE",
        "decision_authority": "OBSERVATION_ONLY",
        "scan_run_id": scan_run_id,
        "candidate_id": candidate_id,
        "ranking_snapshot_id": ranking_snapshot_id,
        "schema": schema,
        "symbol": symbol,
        "cutoff_at": cutoff_at,
        "manifest_hash": manifest_hash,
        "primary": primary,
        "supporting": supporting,
        "contradicting": contradicting,
        "reason": reason,
        "reasons": reasons,
        "review_only": True,
        "direct_order_submission": False,
        "approval_enabled": False,
    }
    try:
        assert_no_secret_like(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail="unsafe candidate evidence provider payload",
        ) from exc
    return payload


def _normalise_primary_evidence(
    raw: Mapping[str, Any],
) -> dict[str, object] | None:
    kind = _clean_text(raw.get("kind"), 40)
    source = _clean_text(raw.get("source"), 40)
    record_hash = _normalise_digest(raw.get("record_hash"))
    record = raw.get("record")
    if (
        kind not in _PRIMARY_EVIDENCE_KINDS
        or source not in _PRIMARY_EVIDENCE_SOURCES
        or record_hash is None
        or not isinstance(record, Mapping)
    ):
        return None
    return {
        "kind": kind,
        "source": source,
        "record": _normalise_primary_record(record),
        "record_hash": record_hash,
    }


def _normalise_primary_record(raw: Mapping[str, Any]) -> dict[str, object]:
    text_or_number_fields = (
        "symbol",
        "underlying",
        "snapshot_id",
        "snapshot_hash",
        "broker_snapshot_id",
        "broker_snapshot_hash",
        "quote_snapshot_id",
        "quote_snapshot_hash",
        "contract_id",
        "contract_id_ex",
        "con_id",
        "local_symbol",
        "trading_class",
        "security_type",
        "sec_type",
        "expiration",
        "strike",
        "right",
        "multiplier",
        "currency",
        "exchange",
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "mid",
        "mark",
        "last",
        "spread",
        "spread_pct",
        "debit_usd",
        "credit_usd",
        "payoff",
        "max_loss_usd",
        "max_profit_usd",
        "liquidity",
        "liquidity_score",
        "volume",
        "open_interest",
        "execution_cost_usd",
        "commission_usd",
        "slippage_usd",
        "after_cost_ev_usd",
        "dte",
        "risk_status",
        "status",
    )
    timestamp_fields = (
        "observed_at",
        "asof",
        "captured_at",
        "quote_at",
        "computed_at",
        "valid_until",
    )
    projected: dict[str, object] = {}
    for field in text_or_number_fields:
        value = _normalise_display_scalar(raw.get(field), maximum=160)
        if value is not None:
            projected[field] = value
    for field in timestamp_fields:
        value = _normalise_timestamp(raw.get(field))
        if value is not None:
            projected[field] = value
    for field in ("complete", "executable", "eligible"):
        if isinstance(raw.get(field), bool):
            projected[field] = raw[field]
    reasons = _normalise_text_list(raw.get("reasons"), maximum=16)
    if reasons:
        projected["reasons"] = reasons
    breakevens = _normalise_scalar_list(raw.get("breakevens"), maximum=8)
    if breakevens:
        projected["breakevens"] = breakevens
    legs = _mapping_sequence(raw.get("legs", ()))
    if legs:
        projected["legs"] = [
            _normalise_primary_leg(item) for item in legs[:8]
        ]
    payoff = raw.get("payoff")
    if isinstance(payoff, Mapping):
        projected_payoff = {
            field: value
            for field in (
                "debit_usd",
                "credit_usd",
                "max_loss_usd",
                "max_profit_usd",
                "after_cost_ev_usd",
            )
            if (
                value := _normalise_display_scalar(
                    payoff.get(field), maximum=160
                )
            )
            is not None
        }
        payoff_breakevens = _normalise_scalar_list(
            payoff.get("breakevens"), maximum=8
        )
        if payoff_breakevens:
            projected_payoff["breakevens"] = payoff_breakevens
        if projected_payoff:
            projected["payoff"] = projected_payoff
    return projected


def _normalise_primary_leg(raw: Mapping[str, Any]) -> dict[str, object]:
    allowed = (
        "symbol",
        "underlying",
        "contract_id",
        "contract_id_ex",
        "con_id",
        "local_symbol",
        "trading_class",
        "expiration",
        "strike",
        "right",
        "side",
        "ratio",
        "multiplier",
        "currency",
        "exchange",
        "bid",
        "ask",
        "bid_size",
        "ask_size",
    )
    return {
        field: value
        for field in allowed
        if (value := _normalise_display_scalar(raw.get(field), maximum=128))
        is not None
    }


def _normalise_external_evidence(
    raw: Mapping[str, Any],
) -> dict[str, object] | None:
    evidence_id = _clean_text(raw.get("evidence_id"), 160)
    content_hash = _normalise_digest(raw.get("content_hash"))
    row_hash = _normalise_digest(raw.get("row_hash"))
    if evidence_id is None or content_hash is None or row_hash is None:
        return None
    source_payload = raw.get("payload")
    source_payload = source_payload if isinstance(source_payload, Mapping) else {}
    public_url = _normalise_public_url(
        source_payload.get(
            "public_url",
            source_payload.get("url", raw.get("public_url", raw.get("url"))),
        )
    )
    payload: dict[str, object] = {}
    for output, candidates, maximum in (
        ("title", ("title", "headline"), 280),
        ("headline", ("headline", "title"), 280),
        ("summary", ("summary", "description"), 1_200),
        ("category", ("category",), 80),
        ("event_type", ("event_type", "type"), 80),
    ):
        value = next(
            (
                _clean_text(source_payload.get(key, raw.get(key)), maximum)
                for key in candidates
                if source_payload.get(key, raw.get(key)) is not None
            ),
            None,
        )
        if value is not None:
            payload[output] = value
    if public_url is not None:
        payload["public_url"] = public_url
    sentiment = _normalise_sentiment(
        source_payload.get("sentiment", raw.get("sentiment"))
    )
    if sentiment is not None:
        payload["sentiment"] = sentiment

    return {
        "evidence_id": evidence_id,
        "content_hash": content_hash,
        "row_hash": row_hash,
        "identity": _clean_text(raw.get("identity"), 160),
        "kind": _clean_text(raw.get("kind"), 80),
        "symbol": _clean_text(raw.get("symbol"), 32),
        "source": _clean_text(raw.get("source", raw.get("provider")), 120),
        "source_id": _clean_text(raw.get("source_id"), 160),
        "published_at": _normalise_timestamp(raw.get("published_at")),
        "first_seen_at": _normalise_timestamp(raw.get("first_seen_at")),
        "ingested_at": _normalise_timestamp(raw.get("ingested_at")),
        "observed_at": _normalise_timestamp(raw.get("observed_at")),
        "effective_status": _clean_text(
            raw.get("effective_status", raw.get("status")), 40
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "payload": payload,
    }


def _mapping_sequence(value: object) -> list[Mapping[str, Any]] | None:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return None
    rows = [item for item in value if isinstance(item, Mapping)]
    return rows if len(rows) == len(value) else None


def _normalise_digest(value: object) -> str | None:
    if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
        return value
    return None


def _normalise_evidence_identifier(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    identifier = value.strip()
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", identifier):
        return identifier
    return None


def _normalise_evidence_cutoff(value: object) -> str | None:
    text = _normalise_timestamp(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(
            text[:-1] + "+00:00" if text.endswith("Z") else text
        )
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return None
    normalized = parsed.astimezone(timezone.utc)
    if normalized > datetime.now(timezone.utc):
        return None
    return normalized.isoformat()


def _normalise_display_scalar(
    value: object, *, maximum: int
) -> str | int | float | bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return _clean_text(value, maximum)


def _normalise_scalar_list(value: object, *, maximum: int) -> list[object]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return []
    result: list[object] = []
    for item in value:
        cleaned = _normalise_display_scalar(item, maximum=160)
        if cleaned is not None:
            result.append(cleaned)
        if len(result) == maximum:
            break
    return result


def _normalise_sentiment(value: object) -> object | None:
    scalar = _normalise_display_scalar(value, maximum=80)
    if scalar is not None:
        return scalar
    if not isinstance(value, Mapping):
        return None
    projected = {
        "label": _clean_text(value.get("label"), 40),
        "score": _normalise_display_scalar(value.get("score"), maximum=40),
    }
    return projected if any(item is not None for item in projected.values()) else None


def _normalise_challenge(
    raw: Mapping[str, Any],
    ranking_snapshot_id: str,
    candidate_id: str,
) -> dict[str, object]:
    if (
        raw.get("review_only") is not True
        or raw.get("order_submitted") is not False
        or raw.get("transmitted_to_broker") is not False
        or raw.get("status") != "PENDING_SECOND_CONFIRMATION"
        or raw.get("ibkr_deep_link") is not None
        or raw.get("instruction_id") is not None
        or any(
            key in raw
            for key in (
                "approval_id",
                "broker_order_id",
                "order_id",
                "handoff_database",
            )
        )
    ):
        raise HTTPException(
            status_code=502,
            detail="challenge handler violated review-only contract",
        )
    challenge_id = raw.get("challenge_id")
    response = raw.get("challenge_response")
    expires_at = raw.get("expires_at")
    if (
        not isinstance(challenge_id, str)
        or not challenge_id.strip()
        or not isinstance(response, str)
        or len(response) < 24
        or not isinstance(expires_at, str)
        or not expires_at.strip()
    ):
        raise HTTPException(
            status_code=502,
            detail="challenge handler returned an invalid challenge",
        )
    payload = {
        "ranking_snapshot_id": ranking_snapshot_id,
        "candidate_id": candidate_id,
        "challenge_id": challenge_id,
        "challenge_response": response,
        "status": "PENDING_SECOND_CONFIRMATION",
        "expires_at": expires_at,
        "approval_id": None,
        "instruction_id": None,
        "ibkr_deep_link": None,
        "review_only": True,
        "order_submitted": False,
        "transmitted_to_broker": False,
    }
    return _safe_mapping(payload)


def _normalise_pending_handoff(raw: Mapping[str, Any]) -> dict[str, object]:
    if (
        raw.get("order_submitted") is not False
        or raw.get("transmitted_to_broker") is not False
        or raw.get("review_only") is not True
        or raw.get("status") != "PENDING_CODEX_BRIDGE"
        or raw.get("ibkr_deep_link") is not None
        or raw.get("instruction_id") is not None
        or any(
            key in raw
            for key in ("broker_order_id", "order_id", "handoff_database")
        )
    ):
        raise HTTPException(
            status_code=502,
            detail="confirmation handler violated pending review-only contract",
        )
    approval_id = raw.get("approval_id")
    expires_at = raw.get("expires_at")
    status_url = raw.get("status_url")
    expected_url = (
        f"/api/approvals/{quote(approval_id, safe='')}"
        if isinstance(approval_id, str)
        else None
    )
    if (
        not isinstance(approval_id, str)
        or not approval_id.strip()
        or not isinstance(expires_at, str)
        or not expires_at.strip()
        or status_url != expected_url
    ):
        raise HTTPException(
            status_code=502,
            detail="confirmation handler returned an invalid handoff identity",
        )
    return _safe_mapping(
        {
            "approval_id": approval_id,
            "proposal_hash": raw.get("proposal_hash"),
            "status": "PENDING_CODEX_BRIDGE",
            "expires_at": expires_at,
            "status_url": status_url,
            "instruction_id": None,
            "ibkr_deep_link": None,
            "review_only": True,
            "order_submitted": False,
            "transmitted_to_broker": False,
        }
    )


async def _invoke(
    provider: Callable[..., object],
    *args: object,
) -> object:
    result = provider(*args)
    if inspect.isawaitable(result):
        return await result
    return result


async def _invoke_collection_provider(provider: CollectionProvider) -> object:
    """Keep slow synchronous read-model providers off the ASGI event loop."""

    result = await run_in_threadpool(provider)
    if inspect.isawaitable(result):
        return await result
    return result


def _require_mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("provider must return a mapping")
    return value


def _normalise_bootstrap(raw: Mapping[str, Any]) -> dict[str, object]:
    """Whitelist the browser bootstrap and keep account identity server-masked."""

    campaign_source = raw.get("campaign")
    campaign_raw = campaign_source if isinstance(campaign_source, Mapping) else {}
    strategy_nav_raw = campaign_raw.get(
        "strategy_nav_usd",
        campaign_raw.get("strategy_nav"),
    )
    strategy_nav = _normalise_nonnegative_number(strategy_nav_raw)
    account_nlv_raw = campaign_raw.get("account_nlv_usd")
    account_nlv = _normalise_nonnegative_number(account_nlv_raw)
    reconciliation_difference_raw = campaign_raw.get(
        "reconciliation_difference_usd"
    )
    reconciliation_difference = _normalise_bounded_number(
        reconciliation_difference_raw
    )
    strategy_nav_asof = _normalise_timestamp(
        campaign_raw.get("strategy_nav_asof")
    )
    campaign_account_observed_at = _normalise_timestamp(
        campaign_raw.get("account_observed_at")
    )
    strategy_nav_content_hash = _normalise_digest(
        campaign_raw.get("strategy_nav_content_hash")
    )
    strategy_nav_authority_hash = _normalise_digest(
        campaign_raw.get("strategy_nav_authority_hash")
    )
    strategy_nav_contract_hash = _normalise_digest(
        campaign_raw.get("strategy_nav_contract_hash")
    )
    strategy_nav_ledger_head_hash = _normalise_digest(
        campaign_raw.get("strategy_nav_ledger_head_hash")
    )
    campaign: dict[str, object] = {
        "strategy_nav_usd": strategy_nav if strategy_nav and strategy_nav > 0 else None,
        "account_nlv_usd": account_nlv,
        "reconciliation_difference_usd": reconciliation_difference,
        "strategy_nav_asof": strategy_nav_asof,
        "account_observed_at": campaign_account_observed_at,
        "strategy_nav_content_hash": strategy_nav_content_hash,
        "strategy_nav_authority_hash": strategy_nav_authority_hash,
        "strategy_nav_contract_hash": strategy_nav_contract_hash,
        "strategy_nav_ledger_head_hash": strategy_nav_ledger_head_hash,
        "starting_nlv_usd": _normalise_nonnegative_number(
            campaign_raw.get("starting_nlv_usd", campaign_raw.get("start_nlv_usd"))
        ),
        "target_nlv_usd": _normalise_nonnegative_number(
            campaign_raw.get("target_nlv_usd")
        )
        or 10_000.0,
        "progress_fraction": _normalise_fraction(
            campaign_raw.get("progress_fraction")
        ),
        "next_milestone_usd": _normalise_nonnegative_number(
            campaign_raw.get("next_milestone_usd")
        ),
    }

    account_source = next(
        (
            value
            for value in (raw.get("account"), raw.get("ibkr"), raw.get("broker"))
            if isinstance(value, Mapping)
        ),
        {},
    )
    assert isinstance(account_source, Mapping)
    account_identifier = account_source.get(
        "account_masked",
        account_source.get("account_id", account_source.get("account")),
    )
    market_data = account_source.get(
        "market_data_status",
        account_source.get("quote_status", account_source.get("market_data")),
    )
    account_status = _clean_text(
        account_source.get("status", account_source.get("state")),
        24,
    )
    observed_account_nlv_raw = account_source.get(
        "net_liquidation_usd",
        account_source.get("net_liquidation", account_source.get("nlv_usd")),
    )
    observed_account_nlv = _normalise_nonnegative_number(
        observed_account_nlv_raw
    )
    account_observed_at = _normalise_timestamp(
        account_source.get(
            "observed_at",
            account_source.get(
                "asof",
                campaign_account_observed_at or raw.get("asof"),
            ),
        )
    )
    raw_reconciled = account_source.get(
        "reconciled",
        account_source.get("account_reconciled"),
    )
    reconciliation_verified = _bootstrap_reconciliation_verified(
        status=account_status,
        raw_reconciled=raw_reconciled,
        strategy_nav=strategy_nav_raw,
        strategy_nav_asof=strategy_nav_asof,
        observed_account_nlv=observed_account_nlv_raw,
        campaign_account_nlv=account_nlv_raw,
        account_observed_at=account_observed_at,
        reconciliation_difference=reconciliation_difference_raw,
        content_hash=strategy_nav_content_hash,
        authority_hash=strategy_nav_authority_hash,
        contract_hash=strategy_nav_contract_hash,
        ledger_head_hash=strategy_nav_ledger_head_hash,
    )
    reconciled = (
        True
        if reconciliation_verified
        else False
        if raw_reconciled is False
        else None
    )
    reconciliation_status = (
        "VERIFIED"
        if reconciliation_verified
        else "INVALID"
        if raw_reconciled is True
        else "UNAVAILABLE"
    )
    account = {
        "account_masked": _mask_account_identifier(account_identifier),
        "net_liquidation_usd": observed_account_nlv,
        "status": account_status,
        "connected": account_source.get("connected")
        if isinstance(account_source.get("connected"), bool)
        else None,
        "reconciled": reconciled,
        "reconciliation_status": reconciliation_status,
        "strategy_nav_usd": campaign["strategy_nav_usd"],
        "strategy_nav_asof": strategy_nav_asof,
        "reconciliation_difference_usd": reconciliation_difference,
        "strategy_nav_content_hash": strategy_nav_content_hash,
        "strategy_nav_authority_hash": strategy_nav_authority_hash,
        "strategy_nav_contract_hash": strategy_nav_contract_hash,
        "strategy_nav_ledger_head_hash": strategy_nav_ledger_head_hash,
        "market_data_status": market_data
        if isinstance(market_data, bool)
        else _clean_text(market_data, 32),
        "observed_at": account_observed_at,
        "decision_authority": _clean_text(
            account_source.get("decision_authority"),
            48,
        ),
    }
    warnings = _normalise_text_list(raw.get("warnings"), maximum=8)
    if raw_reconciled is True and not reconciliation_verified:
        warnings = list(
            dict.fromkeys(
                (*warnings, "BOOTSTRAP_RECONCILIATION_PROOF_INVALID")
            )
        )[:8]
    payload: dict[str, object] = {
        "asof": _normalise_timestamp(raw.get("asof")),
        "source": _clean_text(raw.get("source"), 80),
        "account": account,
        "campaign": campaign,
        "warnings": warnings,
        "safety": {
            "review_only": True,
            "direct_order_submission": False,
            "approval_confirmation_required": True,
            "max_candidates": 10,
        },
    }
    try:
        assert_no_secret_like(payload)
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="unsafe bootstrap provider payload") from exc
    return payload


def _normalise_fraction(value: object) -> float | None:
    number = _normalise_nonnegative_number(value)
    return number if number is not None and number <= 1 else None


def _normalise_bounded_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if not isinstance(value, (int, float, Decimal)):
        return None
    number = float(value)
    return number if math.isfinite(number) and abs(number) < 1_000_000_000 else None


def _bootstrap_reconciliation_verified(
    *,
    status: str | None,
    raw_reconciled: object,
    strategy_nav: object,
    strategy_nav_asof: str | None,
    observed_account_nlv: object,
    campaign_account_nlv: object,
    account_observed_at: str | None,
    reconciliation_difference: object,
    content_hash: str | None,
    authority_hash: str | None,
    contract_hash: str | None,
    ledger_head_hash: str | None,
) -> bool:
    """Recheck the public proof instead of trusting a provider boolean."""

    if (
        status != "CURRENT"
        or raw_reconciled is not True
        or None in (
            content_hash,
            authority_hash,
            contract_hash,
            ledger_head_hash,
        )
    ):
        return False
    try:
        nav = Decimal(str(strategy_nav))
        observed_nlv = Decimal(str(observed_account_nlv))
        campaign_nlv = Decimal(str(campaign_account_nlv))
        difference = Decimal(str(reconciliation_difference))
    except (InvalidOperation, TypeError, ValueError):
        return False
    if (
        not all(
            value.is_finite()
            for value in (nav, observed_nlv, campaign_nlv, difference)
        )
        or nav <= 0
        or observed_nlv <= 0
        or campaign_nlv != observed_nlv
    ):
        return False
    nav_at = _aware_timestamp(strategy_nav_asof)
    account_at = _aware_timestamp(account_observed_at)
    if nav_at is None or account_at is None or nav_at != account_at:
        return False
    expected = (observed_nlv - nav).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_EVEN,
    )
    return difference == expected


def _aware_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _mask_account_identifier(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    identifier = "".join(value.split())[:64]
    if not identifier:
        return None
    if len(identifier) <= 4:
        return "••••"
    return f"{identifier[:2]}••••{identifier[-2:]}"


def _normalise_collection(
    raw: object,
    *,
    key: str,
    maximum: int | None = None,
) -> dict[str, object]:
    if isinstance(raw, Mapping):
        payload: dict[str, object] = dict(raw)
        raw_items = raw.get(key, ())
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        payload = {}
        raw_items = raw
    else:
        raise TypeError(f"{key} provider must return a mapping or sequence")

    if not isinstance(raw_items, Sequence) or isinstance(
        raw_items, (str, bytes, bytearray)
    ):
        raise TypeError(f"{key} must be a sequence")
    items = [dict(item) for item in raw_items if isinstance(item, Mapping)]
    total_count = len(items)
    if maximum is not None:
        items = items[:maximum]
    payload[key] = items
    payload["count"] = len(items)
    payload["total_count"] = total_count
    payload["truncated"] = total_count > len(items)

    if key == "candidates":
        decision = str(payload.get("decision") or "").upper()
        if not items:
            decision = "NO_TRADE"
        elif not decision:
            decision = "CANDIDATES_AVAILABLE"
        payload["decision"] = decision
    return payload


def _safe_equity_pool_read_model(raw: Mapping[str, object]) -> dict[str, object]:
    """Project the durable research pool through an explicit public allowlist."""

    return {
        "schema": "options_copilot.equity_pool_read_model.v1",
        "status": _clean_text(raw.get("status"), 40),
        "decision": _clean_text(raw.get("decision"), 40),
        "reason_codes": _normalise_text_list(raw.get("reason_codes"), maximum=32),
        "pool_id": _normalise_digest(raw.get("pool_id")),
        "slot": _normalise_timestamp(raw.get("slot")),
        "snapshot_hash": _normalise_digest(raw.get("snapshot_hash")),
        "chain_hash": _normalise_digest(raw.get("chain_hash")),
        "normalized_inputs_hash": _normalise_digest(
            raw.get("normalized_inputs_hash")
        ),
        "policy_version": _clean_text(raw.get("policy_version"), 80),
        "policy_hash": _normalise_digest(raw.get("policy_hash")),
        "taxonomy_version": _clean_text(raw.get("taxonomy_version"), 80),
        "taxonomy_hash": _normalise_digest(raw.get("taxonomy_hash")),
        "position_mode": _clean_text(raw.get("position_mode"), 40),
        "discovery_count": _normalise_nonnegative_integer(
            raw.get("discovery_count")
        ),
        "considered_count": _normalise_nonnegative_integer(
            raw.get("considered_count")
        ),
        "selected_count": _normalise_nonnegative_integer(raw.get("selected_count")),
        "excluded_count": _normalise_nonnegative_integer(raw.get("excluded_count")),
        "selected_symbols": _normalise_equity_pool_symbols(
            raw.get("selected_symbols"),
            maximum=30,
        ),
        "selected": _normalise_equity_pool_decisions(
            raw.get("selected"),
            maximum=30,
        ),
        "excluded": _normalise_equity_pool_decisions(
            raw.get("excluded"),
            maximum=150,
        ),
        "concentration_counts": _normalise_equity_pool_counts(
            raw.get("concentration_counts")
        ),
        "scanner_sources": _normalise_text_list(
            raw.get("scanner_sources"),
            maximum=3,
        ),
        "scanner_input_hashes": _normalise_digest_list(
            raw.get("scanner_input_hashes"),
            maximum=150,
        ),
        "pacing_usage_hash": _normalise_digest(raw.get("pacing_usage_hash")),
        "research_only": True,
        "entry_authority": False,
        "approval_eligible": False,
        "decision_authority": "SUPPORTING_ONLY",
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "review_only": True,
        "direct_order_submission": False,
    }


def _safe_option_pool_read_model(raw: Mapping[str, object]) -> dict[str, object]:
    """Expose option research evidence without forwarding provider-owned fields."""

    return {
        "schema": "options_copilot.option_structure_pool_read_model.v1",
        "status": _clean_text(raw.get("status"), 40),
        "decision": _clean_text(raw.get("decision"), 40),
        "reason_codes": _normalise_text_list(raw.get("reason_codes"), maximum=32),
        "scan_run_id": _clean_text(raw.get("scan_run_id"), 160),
        "observed_at": _normalise_timestamp(raw.get("observed_at")),
        "snapshot_hash": _normalise_digest(raw.get("snapshot_hash")),
        "exact_count": _normalise_nonnegative_integer(raw.get("exact_count")),
        "research_only_count": _normalise_nonnegative_integer(
            raw.get("research_only_count")
        ),
        "excluded_count": _normalise_nonnegative_integer(raw.get("excluded_count")),
        "generation_reason_codes": _normalise_text_list(
            raw.get("generation_reason_codes"),
            maximum=32,
        ),
        "decisions": _normalise_option_pool_decisions(
            raw.get("decisions"),
            maximum=300,
        ),
        "research_only": True,
        "decision_authority": "SUPPORTING_ONLY",
        "entry_authority": False,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "review_only": True,
        "direct_order_submission": False,
    }


def _normalise_option_pool_decisions(
    value: object,
    *,
    maximum: int,
) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    decisions: list[dict[str, object]] = []
    for item in value[:maximum]:
        if not isinstance(item, Mapping):
            continue
        thesis = (
            item.get("equity_thesis_evidence")
            if isinstance(item.get("equity_thesis_evidence"), Mapping)
            else {}
        )
        economics = (
            item.get("exact_economics")
            if isinstance(item.get("exact_economics"), Mapping)
            else None
        )
        decisions.append({
            "underlying": _clean_text(item.get("underlying"), 16),
            "thesis_class": _clean_text(item.get("thesis_class"), 48),
            "thesis_observed_at": _normalise_timestamp(
                item.get("thesis_observed_at")
            ),
            "direction_label": _clean_text(thesis.get("direction_label"), 32),
            "direction_score": _normalise_decimal_text(
                thesis.get("direction_score")
            ),
            "uncertainty": _normalise_decimal_text(thesis.get("uncertainty")),
            "equity_thesis_hash": _normalise_digest(
                item.get("equity_thesis_hash")
            ),
            "structure": _clean_text(item.get("structure"), 48),
            "disposition": _clean_text(item.get("disposition"), 48),
            "reason_codes": _normalise_text_list(
                item.get("reason_codes"),
                maximum=32,
            ),
            "candidate_id": _clean_text(item.get("candidate_id"), 160),
            "candidate_hash": _normalise_digest(item.get("candidate_hash")),
            "candidate_identity": _normalise_digest(
                item.get("candidate_identity")
            ),
            "quote_age_seconds": _normalise_decimal_text(
                item.get("quote_age_seconds")
            ),
            "freshness_degraded": item.get("freshness_degraded") is True,
            "economics": _normalise_option_pool_economics(economics),
            "entry_eligible": False,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        })
    return decisions


def _normalise_option_pool_economics(
    value: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if value is None:
        return None
    final_costs = (
        value.get("final_costs")
        if isinstance(value.get("final_costs"), Mapping)
        else {}
    )
    return {
        "dte": _normalise_nonnegative_integer(value.get("dte")),
        "max_loss_usd": _normalise_decimal_text(value.get("max_loss_usd")),
        "max_profit_usd": _normalise_decimal_text(value.get("max_profit_usd")),
        "max_profit_type": _clean_text(value.get("max_profit_type"), 32),
        "after_cost_ev_usd": _normalise_decimal_text(
            value.get("after_cost_ev_usd")
        ),
        "all_in_cost_usd": _normalise_decimal_text(value.get("all_in_cost_usd")),
        "breakevens": _normalise_decimal_list(value.get("breakevens"), maximum=8),
        "scenario_pnl": _normalise_option_pool_scenarios(
            value.get("scenario_pnl")
        ),
        "final_costs": {
            "cost_version": _clean_text(final_costs.get("cost_version"), 80),
            "cost_hash": _normalise_digest(final_costs.get("cost_hash")),
            "commission_usd": _normalise_decimal_text(
                final_costs.get("commission_usd")
            ),
            "slippage_usd": _normalise_decimal_text(
                final_costs.get("slippage_usd")
            ),
            "execution_cost_usd": _normalise_decimal_text(
                final_costs.get("execution_cost_usd")
            ),
        },
        "legs": _normalise_option_pool_legs(value.get("legs")),
    }


def _normalise_decimal_list(value: object, *, maximum: int) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    return [
        parsed
        for item in value[:maximum]
        if (parsed := _normalise_decimal_text(item)) is not None
    ]


def _normalise_option_pool_scenarios(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    return [
        {
            "name": _clean_text(item.get("name"), 48),
            "terminal_underlying_price": _normalise_decimal_text(
                item.get("terminal_underlying_price", item.get("underlying_price"))
            ),
            "underlying_price": _normalise_decimal_text(
                item.get("terminal_underlying_price", item.get("underlying_price"))
            ),
            "probability": _normalise_decimal_text(item.get("probability")),
            "pnl_usd": _normalise_decimal_text(item.get("pnl_usd")),
        }
        for item in value[:16]
        if isinstance(item, Mapping)
    ]


def _normalise_option_pool_legs(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    legs: list[dict[str, object]] = []
    for item in value[:8]:
        if not isinstance(item, Mapping):
            continue
        liquidity = (
            item.get("liquidity")
            if isinstance(item.get("liquidity"), Mapping)
            else {}
        )
        legs.append({
            "con_id": _normalise_positive_integer(item.get("con_id")),
            "contract_id_ex": _clean_text(item.get("contract_id_ex"), 120),
            "expiration": _normalise_option_expiry(item.get("expiration")),
            "strike": _normalise_decimal_text(item.get("strike")),
            "right": _clean_text(item.get("right"), 8),
            "side": _clean_text(item.get("side"), 8),
            "ratio": _normalise_positive_integer(item.get("ratio")),
            "multiplier": (
                _normalise_decimal_text(item.get("multiplier"))
                if (multiplier := _decimal_value(item.get("multiplier"))) is not None
                and multiplier > 0 else None
            ),
            "exchange": _clean_text(item.get("exchange"), 32),
            "bid": _normalise_decimal_text(item.get("bid")),
            "ask": _normalise_decimal_text(item.get("ask")),
            "implied_volatility": _normalise_decimal_text(item.get("implied_volatility")),
            "exchange_time": _normalise_aware_timestamp(item.get("exchange_time")),
            "observed_at": _normalise_aware_timestamp(item.get("observed_at")),
            "completed_at": _normalise_aware_timestamp(item.get("completed_at")),
            "market_data_type": (
                item.get("market_data_type")
                if _normalise_positive_integer(item.get("market_data_type")) in (1, 2, 3, 4)
                else None
            ),
            "delta": _normalise_decimal_text(item.get("delta")),
            "gamma": _normalise_decimal_text(item.get("gamma")),
            "theta": _normalise_decimal_text(item.get("theta")),
            "vega": _normalise_decimal_text(item.get("vega")),
            "volume": _normalise_nonnegative_integer_or_none(item.get("volume")),
            "open_interest": _normalise_nonnegative_integer_or_none(
                item.get("open_interest")
            ),
            "quote_age_seconds": _normalise_decimal_text(
                item.get("quote_age_seconds")
            ),
            "liquidity_status": _clean_text(liquidity.get("status"), 32),
            "spread_bps": _normalise_decimal_text(liquidity.get("spread_bps")),
            "bid_ask_spread": _normalise_decimal_text(liquidity.get("bid_ask_spread")),
        })
    return legs


def _normalise_equity_pool_symbols(
    value: object,
    *,
    maximum: int,
) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    symbols: list[str] = []
    for item in value[:maximum]:
        symbol = _clean_text(item, 16)
        if symbol is not None:
            symbols.append(symbol.upper())
    return symbols


def _normalise_equity_pool_decisions(
    value: object,
    *,
    maximum: int,
) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    decisions: list[dict[str, object]] = []
    for item in value[:maximum]:
        if not isinstance(item, Mapping):
            continue
        score = item.get("score") if isinstance(item.get("score"), Mapping) else {}
        classification = (
            item.get("classification")
            if isinstance(item.get("classification"), Mapping)
            else {}
        )
        decisions.append(
            {
                "symbol": _clean_text(item.get("symbol"), 16),
                "disposition": _clean_text(item.get("disposition"), 40),
                "score": {
                    key: _normalise_decimal_text(score.get(key))
                    for key in (
                        "direction_score",
                        "coverage_confidence",
                        "positive_evidence_mass",
                        "negative_evidence_mass",
                        "conflict_penalty",
                        "uncertainty",
                        "liquidity_score",
                        "opportunity_score",
                    )
                }
                | {
                    "symbol": _clean_text(score.get("symbol"), 16),
                    "direction_label": _clean_text(
                        score.get("direction_label"),
                        40,
                    ),
                },
                "classification": {
                    "symbol": _clean_text(classification.get("symbol"), 16),
                    "category": _clean_text(classification.get("category"), 80),
                    "source": _clean_text(classification.get("source"), 80),
                    "mega_cap_tech": classification.get("mega_cap_tech") is True,
                    "concentration_group": _clean_text(
                        classification.get("concentration_group"),
                        120,
                    ),
                    "taxonomy_version": _clean_text(
                        classification.get("taxonomy_version"),
                        80,
                    ),
                    "taxonomy_hash": _normalise_digest(
                        classification.get("taxonomy_hash")
                    ),
                },
                "reasons": _normalise_text_list(item.get("reasons"), maximum=16),
                "canonical_input_hash": _normalise_digest(
                    item.get("canonical_input_hash")
                ),
                "selected_rank": _normalise_positive_integer(
                    item.get("selected_rank")
                ),
                "decision_authority": "SUPPORTING_ONLY",
                "instruction_creation_allowed": False,
                "order_allowed": False,
                "entry_eligible": False,
            }
        )
    return decisions


def _normalise_equity_pool_counts(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    counts: dict[str, int] = {}
    for key, raw_count in tuple(value.items())[:64]:
        name = _clean_text(key, 120)
        count = _normalise_nonnegative_integer(raw_count)
        if name is not None and count is not None:
            counts[name] = count
    return counts


async def _read_only_feed(
    provider: CollectionProvider | None,
    *,
    key: Literal["news", "calendar"],
) -> dict[str, object]:
    """Map untrusted provider data into the intentionally small public schema.

    The provider may return an envelope or a bare sequence.  We never pass its
    mapping through: unknown fields (including credentials and any execution
    vocabulary) are discarded before serialization.
    """

    if provider is None:
        empty: dict[str, object] = {
            key: [],
            "count": 0,
            "asof": None,
            "provider": _normalise_provider_health({}, configured=False),
            "source_runtime": [],
        }
        if key == "news":
            empty.update(_read_only_pool_counts(()))
            empty.update(_normalise_preselection_pools({}))
            empty["source_health"] = []
            empty["analysis_backfill"] = _normalise_analysis_backfill(None)
            empty["shadow_advisory"] = _normalise_shadow_advisory_status(None)
        return empty
    raw = await _invoke_collection_provider(provider)
    if isinstance(raw, Mapping):
        raw_items = raw.get(
            key,
            raw.get("items", raw.get("events", raw.get("data", ()))),
        )
        raw_provider = raw.get("provider", raw.get("health", {}))
        asof = _normalise_timestamp(raw.get("asof", raw.get("observed_at")))
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        raw_items = raw
        raw_provider = {}
        asof = None
    else:
        raise HTTPException(status_code=502, detail=f"invalid {key} provider payload")
    if not isinstance(raw_items, Sequence) or isinstance(
        raw_items, (str, bytes, bytearray)
    ):
        raise HTTPException(status_code=502, detail=f"invalid {key} collection")

    items = [
        _normalise_feed_item(
            item,
            key=key,
            index=index,
            envelope_asof=asof,
        )
        for index, item in enumerate(raw_items)
        if isinstance(item, Mapping)
    ]
    if key == "news":
        _enforce_news_pool_consistency(items)
        analysis_backfill = _normalise_analysis_backfill(
            raw.get("analysis_backfill") if isinstance(raw, Mapping) else None
        )
        if analysis_backfill["status"] != "READY":
            for item in items:
                item["action_pool"] = False
                item["action_pool_eligible"] = False
                item["action_rank"] = None
                item["rank_one"] = False
    payload: dict[str, object] = {
        key: items,
        "count": len(items),
        "asof": asof,
        "provider": _normalise_provider_health(raw_provider, configured=True),
        "source_runtime": _normalise_source_runtime(
            raw.get("source_runtime") if isinstance(raw, Mapping) else None
        ),
    }
    payload.update(_normalise_news_publication(raw))
    if key == "news":
        payload.update(_read_only_pool_counts(items))
        payload["analysis_backfill"] = analysis_backfill
        payload["shadow_advisory"] = _normalise_shadow_advisory_status(
            raw.get("shadow_advisory") if isinstance(raw, Mapping) else None
        )
        payload["source_health"] = _normalise_news_source_health(
            raw.get("source_health") if isinstance(raw, Mapping) else None
        )
        payload.update(
            _normalise_preselection_pools(raw if isinstance(raw, Mapping) else {})
        )
    elif key == "calendar":
        payload.update(
            _normalise_calendar_envelope(raw if isinstance(raw, Mapping) else {})
        )
        reaction_rows = [
            item.get("reaction")
            for item in items
            if isinstance(item.get("reaction"), Mapping)
        ]
        provider_projection = payload.get("reaction_provider")
        healthy_idle = (
            isinstance(provider_projection, Mapping)
            and provider_projection.get("status") == "READY"
            and provider_projection.get("decision") == "OBSERVATION_ONLY"
            and provider_projection.get("reason") == "NO_ELIGIBLE_REACTION_EVENTS"
            and provider_projection.get("eligible_count") == 0
        )
        failed_reactions = [
            item
            for item in reaction_rows
            if item.get("decision") != "OBSERVATION_ONLY"
            and not (
                healthy_idle
                and item.get("reasons")
                == ["REACTION_EVENT_NOT_IN_CURRENT_OFFICIAL_SNAPSHOT"]
            )
        ]
        if (
            (not items and not healthy_idle)
            or len(reaction_rows) != len(items)
            or failed_reactions
        ):
            payload["reaction_decision"] = "NO_TRADE"
            if (
                isinstance(provider_projection, Mapping)
                and provider_projection.get("status") == "READY"
            ):
                conflicted = any(
                    item.get("status") == "CONFLICTED" for item in failed_reactions
                )
                first_reasons = (
                    failed_reactions[0].get("reasons")
                    if failed_reactions
                    else ()
                )
                first_reason = (
                    first_reasons[0]
                    if isinstance(first_reasons, Sequence)
                    and not isinstance(first_reasons, (str, bytes, bytearray))
                    and first_reasons
                    else "REACTION_API_READ_MODEL_INVALID"
                )
                payload["reaction_provider"] = _reaction_provider_failure(
                    str(first_reason),
                    status="CONFLICTED" if conflicted else "UNAVAILABLE",
                    ledger_count=_normalise_nonnegative_integer(
                        provider_projection.get("ledger_count")
                    ),
                    matched_count=0,
                    ignored_count=_normalise_nonnegative_integer(
                        provider_projection.get("ignored_count")
                    ),
                    supported_event_ids=_normalise_reaction_event_ids(
                        provider_projection.get("supported_event_ids")
                    ),
                    eligible_event_ids=_normalise_reaction_event_ids(
                        provider_projection.get("eligible_event_ids")
                    ),
                    supported_count=_normalise_nonnegative_integer(
                        provider_projection.get("supported_count")
                    ),
                    eligible_count=_normalise_nonnegative_integer(
                        provider_projection.get("eligible_count")
                    ),
                    unsupported_count=_normalise_nonnegative_integer(
                        provider_projection.get("unsupported_count")
                    ),
                    last_attempt=_normalise_reaction_timestamp(
                        provider_projection.get("last_attempt")
                    ),
                )
    try:
        assert_no_secret_like(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=502, detail=f"unsafe {key} provider payload"
        ) from exc
    return payload


def _normalise_provider_configuration(
    raw: Mapping[str, object],
    *,
    failed: bool = False,
) -> dict[str, object]:
    """Project fixed provider states without exposing file paths or credential values."""

    specs = (
        ("JIN10", "jin10_mcp_token", True),
        ("FINNHUB", "finnhub_api_key", False),
        ("ALPHA_VANTAGE", "alpha_vantage_api_key", False),
        ("DEEPSEEK", "deepseek_api_key", False),
    )
    allowed_keys = {key for _, key, _ in specs}
    invalid_shape = set(raw) - allowed_keys
    providers: list[dict[str, object]] = []
    for provider, key, activation_required in specs:
        supplied = raw.get(key)
        invalid_provider_shape = False
        if isinstance(supplied, Mapping):
            invalid_provider_shape = bool(
                set(supplied)
                - {
                    "status",
                    "activated",
                    "composed",
                    "runtime_loaded",
                    "restart_required",
                }
            )
            invalid_provider_shape = invalid_provider_shape or any(
                field in supplied and not isinstance(supplied.get(field), bool)
                for field in (
                    "activated",
                    "composed",
                    "runtime_loaded",
                    "restart_required",
                )
            )
            status = str(supplied.get("status") or "DISABLED").strip().upper()
            activated = supplied.get("activated") is True
            composed = supplied.get("composed") is True
            restart_required = supplied.get("restart_required") is True
            runtime_loaded = supplied.get("runtime_loaded") is True
        else:
            status = str(supplied or "DISABLED").strip().upper()
            activated = not activation_required
            # A legacy string proves only local presence.  It carries no
            # composition/runtime evidence and must never be projected as
            # loaded merely because the file contains a value.
            composed = False
            restart_required = status == "CONFIGURED"
            runtime_loaded = False
        if (
            failed
            or invalid_shape
            or invalid_provider_shape
            or status not in {"CONFIGURED", "DISABLED", "ERROR"}
        ):
            status = "ERROR"
            activated = False
            composed = False
            restart_required = False
            runtime_loaded = False
        if status != "CONFIGURED":
            runtime_loaded = False
        if restart_required or not composed or (activation_required and not activated):
            runtime_loaded = False
        providers.append(
            {
                "provider": provider,
                "status": status,
                "configured": status == "CONFIGURED",
                "activation_required": activation_required,
                "activated": activated,
                "composed": composed,
                "runtime_loaded": runtime_loaded,
                "restart_required": restart_required,
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
    payload = {
        "schema": "options_copilot.provider_configuration.v1",
        "source": "LOCAL_API_KEYS_FILE",
        "providers": providers,
        "values_exposed": False,
        "read_only": True,
        "decision_authority": "OBSERVATION_ONLY",
    }
    assert_no_secret_like(payload)
    return payload


def _normalise_provider_health(raw: object, *, configured: bool) -> dict[str, object]:
    source = raw if isinstance(raw, Mapping) else {"name": raw}
    status = _clean_text(source.get("status", source.get("state")), maximum=24)
    status = (status or ("UNKNOWN" if configured else "UNCONFIGURED")).upper()
    if status not in {
        "UP", "READY", "HEALTHY", "DEGRADED", "DOWN", "ERROR", "STALE",
        "UNKNOWN", "UNCONFIGURED",
    }:
        status = "UNKNOWN"
    return {
        "name": _clean_text(
            source.get("name", source.get("provider_name", source.get("provider"))),
            maximum=80,
        )
        or ("configured" if configured else "unconfigured"),
        "status": status,
        "latency_ms": _normalise_nonnegative_number(
            source.get("latency_ms", source.get("age_ms"))
        ),
        "asof": _normalise_timestamp(source.get("asof", source.get("observed_at"))),
        "message": _clean_text(source.get("message"), maximum=240),
    }


def _normalise_phase2_advisory(value: object) -> dict[str, object]:
    """Rebuild the advisory from a narrow public allowlist."""

    if not isinstance(value, Mapping) or value.get("schema_version") != (
        "options_copilot.phase2_advisory.v1"
    ):
        return _unavailable_phase2_advisory()
    as_of = _normalise_aware_timestamp(value.get("as_of"))
    model_state = _phase2_choice(value.get("model_state"), {"MODEL", "FALLBACK"})
    if as_of is None or model_state is None:
        return _unavailable_phase2_advisory(as_of=as_of)
    fallback_reason: str | None = None
    if model_state == "FALLBACK":
        fallback_reason = _phase2_choice(
            value.get("fallback_reason"),
            _PHASE2_FALLBACK_REASONS,
        )
        if fallback_reason is None:
            return _unavailable_phase2_advisory(as_of=as_of)
    symbol = _phase2_symbol(value.get("symbol"))
    if model_state == "MODEL" and symbol is None:
        return _unavailable_phase2_advisory(as_of=as_of)

    result: dict[str, object] = {
        "schema_version": "options_copilot.phase2_advisory.v1",
        "symbol": symbol,
        "consensus_state": _phase2_choice(
            value.get("consensus_state"),
            {"BEAT", "MISS", "IN_LINE", "UNCERTAIN", "NOT_APPLICABLE"},
        )
        or "UNCERTAIN",
        "model_state": model_state,
        "fallback_reason": fallback_reason,
        "as_of": as_of,
        "observations": _normalise_phase2_observations(value.get("observations")),
        "provenance_ids": _normalise_phase2_identifiers(
            value.get("provenance_ids"),
            maximum=64,
        ),
        "provenance_hashes": _normalise_phase2_digests(
            value.get("provenance_hashes"),
            maximum=64,
        ),
        "event_news_facts": _normalise_phase2_facts(
            value.get("event_news_facts")
        ),
        "fundamental_support": _normalise_phase2_slice(
            value.get("fundamental_support")
        ),
        "expected_price_impact": _normalise_phase2_slice(
            value.get("expected_price_impact")
        ),
        "options_volatility_impact": _normalise_phase2_slice(
            value.get("options_volatility_impact")
        ),
        "counter_evidence": _normalise_phase2_text_list(
            value.get("counter_evidence"),
            maximum=16,
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    try:
        assert_no_secret_like(result)
    except SecretLikeFieldError:
        return _unavailable_phase2_advisory(as_of=as_of)
    return result


def _unavailable_phase2_advisory(
    *,
    as_of: str | None = None,
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


def _normalise_after_hours_indicative(value: object) -> dict[str, object]:
    """Project one after-hours read model through fixed public allowlists."""

    raw = value if isinstance(value, Mapping) else {}
    raw_candidates = raw.get("candidates")
    candidates = [
        _normalise_after_hours_candidate(item)
        for item in (
            raw_candidates
            if isinstance(raw_candidates, Sequence)
            and not isinstance(raw_candidates, (str, bytes, bytearray))
            else ()
        )
        if isinstance(item, Mapping)
    ][:10]
    schema = _normalise_after_hours_safe_text(raw.get("schema"), 96)
    result = {
        "schema": (
            schema
            if schema == "options_copilot.after_hours_indicative.v1"
            else None
        ),
        "status": _normalise_after_hours_choice(
            raw.get("status"),
            {"AVAILABLE", "DEGRADED", "UNAVAILABLE"},
            default="UNAVAILABLE",
        ),
        "freshness_status": _normalise_after_hours_choice(
            raw.get("freshness_status"),
            {"CURRENT", "STALE", "UNAVAILABLE"},
            default="UNAVAILABLE",
        ),
        "decision": "NO_TRADE",
        "mode": "AFTER_HOURS_INDICATIVE",
        "observed_at": _normalise_aware_timestamp(raw.get("observed_at")),
        "quote_batch_id": _normalise_after_hours_safe_text(
            raw.get("quote_batch_id"), 160
        ),
        "quote_batch_status": _normalise_after_hours_choice(
            raw.get("quote_batch_status"),
            {"COMPLETE", "PARTIAL", "UNAVAILABLE"},
            default="UNAVAILABLE",
        ),
        "quote_source": _normalise_after_hours_safe_text(
            raw.get("quote_source"), 160
        ),
        "quote_batch_count": _normalise_nonnegative_integer(
            raw.get("quote_batch_count")
        ),
        "attempted_candidate_count": _normalise_nonnegative_integer(
            raw.get("attempted_candidate_count")
        ),
        "reused_priced_count": _normalise_nonnegative_integer(
            raw.get("reused_priced_count")
        ),
        "reused_mark_evidence_count": _normalise_nonnegative_integer(
            raw.get("reused_mark_evidence_count")
        ),
        "stale_reused_count": _normalise_nonnegative_integer(
            raw.get("stale_reused_count")
        ),
        "requested_count": _normalise_nonnegative_integer(
            raw.get("requested_count")
        ),
        "priced_count": _normalise_nonnegative_integer(raw.get("priced_count")),
        "mark_evidence_count": _normalise_nonnegative_integer(
            raw.get("mark_evidence_count")
        ),
        "reason_codes": _normalise_reason_codes(raw.get("reason_codes")),
        "strategy_nav_usd": _normalise_decimal_text(
            raw.get("strategy_nav_usd")
        ),
        "normal_risk_fraction": _normalise_decimal_text(
            raw.get("normal_risk_fraction")
        ),
        "discovery_mode": _normalise_reason_code(raw.get("discovery_mode")),
        "discovery_reason_codes": _normalise_reason_codes(
            raw.get("discovery_reason_codes")
        ),
        "sector_coverage": _normalise_after_hours_coverage(
            raw.get("sector_coverage"),
            warning_field="concentration_warning",
        ),
        "strategy_coverage": _normalise_after_hours_coverage(
            raw.get("strategy_coverage"),
            warning_field="single_structure_warning",
        ),
        "selection_factors": _normalise_after_hours_selection_factors(
            raw.get("selection_factors")
        ),
        "campaign": _normalise_after_hours_campaign(raw.get("campaign")),
        "candidates": candidates,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "review_only": True,
        "direct_order_submission": False,
    }
    assert_no_secret_like(result)
    return result


def _normalise_after_hours_candidate(
    raw: Mapping[str, object],
) -> dict[str, object]:
    raw_legs = raw.get("legs")
    legs = [
        _normalise_after_hours_leg(item)
        for item in (
            raw_legs
            if isinstance(raw_legs, Sequence)
            and not isinstance(raw_legs, (str, bytes, bytearray))
            else ()
        )
        if isinstance(item, Mapping)
    ][:8]
    result: dict[str, object] = {
        "research_id": _normalise_after_hours_safe_text(
            raw.get("research_id"), 160
        ),
        "rank": _normalise_positive_integer(raw.get("rank")),
        "underlying": (_normalise_symbols(raw.get("underlying")) or [None])[0],
        "sector": _normalise_after_hours_safe_text(raw.get("sector"), 80),
        "source_scan": _normalise_reason_code(raw.get("source_scan")),
        "strategy_type": _normalise_reason_code(raw.get("strategy_type")),
        "direction": _normalise_after_hours_choice(
            raw.get("direction"),
            {"BULLISH", "BEARISH", "NEUTRAL_OR_UNSPECIFIED"},
        ),
        "research_summary": _normalise_after_hours_safe_text(
            raw.get("research_summary"), 1_200
        ),
        "entry_condition": _normalise_after_hours_safe_text(
            raw.get("entry_condition"), 600
        ),
        "invalidation_condition": _normalise_after_hours_safe_text(
            raw.get("invalidation_condition"), 600
        ),
        "profit_target_condition": _normalise_after_hours_safe_text(
            raw.get("profit_target_condition"), 600
        ),
        "stop_loss_condition": _normalise_after_hours_safe_text(
            raw.get("stop_loss_condition"), 600
        ),
        "expiration": _normalise_option_expiry(raw.get("expiration")),
        "quantity": _normalise_positive_integer(raw.get("quantity")),
        "dte": _normalise_nonnegative_integer_or_none(raw.get("dte")),
        "assumed_multiplier": _normalise_positive_integer(
            raw.get("assumed_multiplier")
        ),
        "pricing_status": _normalise_after_hours_choice(
            raw.get("pricing_status"),
            {"AVAILABLE", "STALE", "UNAVAILABLE"},
            default="UNAVAILABLE",
        ),
        "mark_evidence_status": _normalise_after_hours_choice(
            raw.get("mark_evidence_status"),
            {"AVAILABLE", "STALE", "UNAVAILABLE"},
            default="UNAVAILABLE",
        ),
        "freshness_status": _normalise_after_hours_choice(
            raw.get("freshness_status"),
            {"CURRENT", "STALE", "UNAVAILABLE"},
            default="UNAVAILABLE",
        ),
        "quote_status": _normalise_reason_code(raw.get("quote_status")),
        "greeks_status": _normalise_reason_code(raw.get("greeks_status")),
        "liquidity_status": _normalise_reason_code(
            raw.get("liquidity_status")
        ),
        "indicative_price_basis": _normalise_after_hours_choice(
            raw.get("indicative_price_basis"),
            {
                "FROZEN_BBO",
                "LAST",
                "PREVIOUS_CLOSE",
                "PREVIOUS_SESSION_LAST_TRADE",
            },
        ),
        "blockers": _normalise_reason_codes(raw.get("blockers")),
        "legs": legs,
        "trade_status": "NO_TRADE",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    for field in (
        "indicative_entry_debit_usd",
        "execution_cost_cap_usd",
        "indicative_maximum_loss_usd",
        "indicative_maximum_profit_usd",
        "indicative_cost_after_ev_usd",
        "maximum_loss_usd",
        "maximum_profit_usd",
        "cost_after_ev_usd",
        "expected_value_before_costs_usd",
        "risk_adjusted_ev",
        "strategy_nav_usd",
        "strategy_nav_fraction",
        "breakeven_price",
    ):
        result[field] = _normalise_decimal_text(raw.get(field))
    return result


def _normalise_after_hours_leg(raw: Mapping[str, object]) -> dict[str, object]:
    market_data_type = _normalise_nonnegative_integer_or_none(
        raw.get("market_data_type")
    )
    if market_data_type not in {1, 2, 3, 4}:
        market_data_type = None
    result: dict[str, object] = {
        "underlying": (_normalise_symbols(raw.get("underlying")) or [None])[0],
        "side": _normalise_after_hours_choice(
            raw.get("side"), {"BUY", "SELL"}
        ),
        "contract_id": _normalise_positive_integer(raw.get("contract_id")),
        "contract_id_ex": _normalise_display_scalar(
            raw.get("contract_id_ex"), maximum=128
        ),
        "con_id": _normalise_positive_integer(raw.get("con_id")),
        "local_symbol": _clean_contract_identity_text(
            raw.get("local_symbol"), 160
        ),
        "trading_class": _clean_contract_identity_text(
            raw.get("trading_class"), 80
        ),
        "strike": _normalise_decimal_text(raw.get("strike")),
        "right": _normalise_after_hours_choice(
            raw.get("right"), {"C", "P", "CALL", "PUT"}
        ),
        "expiration": _normalise_option_expiry(raw.get("expiration")),
        "exchange": _clean_contract_identity_text(raw.get("exchange"), 40),
        "currency": _clean_contract_identity_text(raw.get("currency"), 16),
        "multiplier": _normalise_positive_integer(raw.get("multiplier")),
        "ratio": _normalise_positive_integer(raw.get("ratio")),
        "quantity": _normalise_positive_integer(raw.get("quantity")),
        "price_basis": _normalise_after_hours_choice(
            raw.get("price_basis"),
            {
                "FROZEN_BBO",
                "LAST",
                "PREVIOUS_CLOSE",
                "PREVIOUS_SESSION_LAST_TRADE",
            },
        ),
        "market_data_type": market_data_type,
        "quote_asof": _normalise_aware_timestamp(raw.get("quote_asof")),
        "quote_batch_id": _normalise_after_hours_safe_text(
            raw.get("quote_batch_id"), 160
        ),
        "quote_status": _normalise_reason_code(raw.get("quote_status")),
        "greeks_status": _normalise_reason_code(raw.get("greeks_status")),
        "liquidity_status": _normalise_reason_code(
            raw.get("liquidity_status")
        ),
        "volume": _normalise_nonnegative_integer_or_none(raw.get("volume")),
        "open_interest": _normalise_nonnegative_integer_or_none(
            raw.get("open_interest")
        ),
        "dte": _normalise_nonnegative_integer_or_none(raw.get("dte")),
    }
    for field in (
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "last",
        "close",
        "indicative_mark",
        "mark",
        "mid",
        "spread",
        "spread_pct",
        "implied_volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
    ):
        result[field] = _normalise_decimal_text(raw.get(field))
    return result


def _normalise_after_hours_coverage(
    value: object,
    *,
    warning_field: str,
) -> dict[str, object]:
    raw = value if isinstance(value, Mapping) else {}
    raw_counts = raw.get("counts")
    counts: dict[str, int] = {}
    if isinstance(raw_counts, Mapping):
        for raw_label, raw_count in list(raw_counts.items())[:32]:
            label = _normalise_after_hours_map_label(raw_label, 80)
            count = _normalise_nonnegative_integer_or_none(raw_count)
            if label is not None and count is not None:
                counts[label] = count
    distinct_count = (
        len(counts)
        if counts
        else _normalise_nonnegative_integer(raw.get("distinct_count"))
    )
    return {
        "distinct_count": distinct_count,
        "counts": counts,
        warning_field: raw.get(warning_field) is True,
    }


def _normalise_after_hours_selection_factors(value: object) -> dict[str, object]:
    raw = value if isinstance(value, Mapping) else {}
    result: dict[str, object] = {}
    for field in (
        "broad_ibkr_scanner",
        "sector_diversification",
        "direction_from_close",
        "closing_option_marks",
        "volatility_regime",
        "term_structure",
        "skew",
        "event_and_news",
        "cost_after_ev",
    ):
        current = raw.get(field)
        if isinstance(current, bool):
            result[field] = current
        else:
            text = _normalise_after_hours_safe_text(current, 160)
            if text is not None:
                result[field] = text
    return result


def _normalise_after_hours_campaign(value: object) -> dict[str, object]:
    raw = value if isinstance(value, Mapping) else {}
    return {
        "completed_underlyings": _normalise_nonnegative_integer(
            raw.get("completed_underlyings")
        ),
        "target_underlyings": _normalise_nonnegative_integer(
            raw.get("target_underlyings")
        ),
        "remaining_underlyings": _normalise_nonnegative_integer(
            raw.get("remaining_underlyings")
        ),
        "continue_after_pacing_window": raw.get(
            "continue_after_pacing_window"
        )
        is True,
    }


def _normalise_after_hours_choice(
    value: object,
    allowed: set[str],
    *,
    default: str | None = None,
) -> str | None:
    text = (_clean_text(value, 96) or "").upper()
    return text if text in allowed else default


def _normalise_after_hours_safe_text(value: object, maximum: int) -> str | None:
    text = _clean_text(value, maximum)
    if text is None:
        return None
    try:
        assert_no_secret_like(text)
    except SecretLikeFieldError:
        return None
    return text


def _normalise_after_hours_map_label(value: object, maximum: int) -> str | None:
    text = _normalise_after_hours_safe_text(value, maximum)
    if text is None or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9 &./_+-]{0,79}", text
    ):
        return None
    try:
        assert_no_secret_like({text: 0})
    except SecretLikeFieldError:
        return None
    return text


def _normalise_fundamentals(value: object) -> dict[str, object]:
    """Project a narrow, deterministic, non-authoritative fundamentals model."""

    if not isinstance(value, Mapping) or value.get("schema") != (
        "options_copilot.fundamentals_read_model.v1"
    ):
        return _unavailable_fundamentals()
    status = str(value.get("status") or "").strip().upper()
    if status not in {"READY", "DEGRADED", "UNAVAILABLE"}:
        return _unavailable_fundamentals()
    as_of = _normalise_aware_timestamp(value.get("as_of"))
    raw_rows = value.get("rows")
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes, bytearray)):
        raw_rows = ()
    rows: list[dict[str, object]] = []
    allowed_metrics = {
        "EPS_DILUTED",
        "REVENUE",
        "OPERATING_CASH_FLOW",
        "DEBT_CURRENT",
        "DEBT_NONCURRENT",
        "GUIDANCE_EPS",
        "GUIDANCE_REVENUE",
        "GUIDANCE_EPS_LOW",
        "GUIDANCE_EPS_HIGH",
        "GUIDANCE_REVENUE_LOW",
        "GUIDANCE_REVENUE_HIGH",
        "PE_TTM",
        "PB_ANNUAL",
        "PS_TTM",
    }
    allowed_categories = {"EPS", "REVENUE", "GUIDANCE", "CASH_FLOW", "DEBT", "VALUATION"}
    for item in raw_rows[:500]:
        if not isinstance(item, Mapping):
            continue
        symbol = _phase2_symbol(item.get("symbol"))
        metric = str(item.get("metric") or "").strip().upper()
        category = str(item.get("category") or "").strip().upper()
        value_text = _phase2_safe_text(item.get("value"), 96)
        period_end = _normalise_date(item.get("period_end"))
        observed_at = _normalise_aware_timestamp(item.get("observed_at"))
        content_hash = _normalise_digest(item.get("content_hash"))
        row_hash = _normalise_digest(item.get("row_hash"))
        if (
            symbol is None
            or metric not in allowed_metrics
            or category not in allowed_categories
            or value_text is None
            or period_end is None
            or observed_at is None
            or content_hash is None
            or row_hash is None
        ):
            continue
        revision_number = item.get("revision_number")
        rows.append(
            {
                "symbol": symbol,
                "metric": metric,
                "category": category,
                "value": value_text,
                "unit": _phase2_safe_text(item.get("unit"), 40),
                "basis": _phase2_safe_text(item.get("basis"), 40),
                "period_end": period_end,
                "fiscal_period": _phase2_safe_text(item.get("fiscal_period"), 24),
                "source": _phase2_safe_text(item.get("source"), 48),
                "source_id": _phase2_identifier(item.get("source_id"), maximum=240),
                "source_url": _normalise_public_https_url(item.get("source_url")),
                "source_filed_date": _normalise_date(item.get("source_filed_date")),
                "observed_at": observed_at,
                "taxonomy": _phase2_safe_text(item.get("taxonomy"), 160),
                "tag": _phase2_safe_text(item.get("tag"), 160),
                "form": _phase2_safe_text(item.get("form"), 24),
                "revision_number": (
                    revision_number
                    if isinstance(revision_number, int)
                    and not isinstance(revision_number, bool)
                    and revision_number > 0
                    else 1
                ),
                "supersedes_hash": _normalise_digest(item.get("supersedes_hash")),
                "content_hash": content_hash,
                "row_hash": row_hash,
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
    categories: dict[str, object] = {}
    raw_categories = value.get("categories")
    for category in sorted(allowed_categories):
        raw = raw_categories.get(category) if isinstance(raw_categories, Mapping) else None
        state = str(raw.get("status") or "UNAVAILABLE").strip().upper() if isinstance(raw, Mapping) else "UNAVAILABLE"
        count = raw.get("record_count") if isinstance(raw, Mapping) else 0
        categories[category] = {
            "status": state if state in {"AVAILABLE", "UNAVAILABLE"} else "UNAVAILABLE",
            "record_count": count if isinstance(count, int) and not isinstance(count, bool) and count >= 0 else 0,
            "reason_code": _phase2_label(raw.get("reason_code"), 64) if isinstance(raw, Mapping) else "FUNDAMENTAL_CATEGORY_UNAVAILABLE",
        }
    revisions: list[dict[str, object]] = []
    raw_revisions = value.get("revisions")
    if isinstance(raw_revisions, Sequence) and not isinstance(
        raw_revisions,
        (str, bytes, bytearray),
    ):
        for item in raw_revisions[:100]:
            if not isinstance(item, Mapping):
                continue
            symbol = _phase2_symbol(item.get("symbol"))
            metric = str(item.get("metric") or "").strip().upper()
            period_end = _normalise_date(item.get("period_end"))
            previous = _phase2_safe_text(item.get("previous_value"), 96)
            current = _phase2_safe_text(item.get("current_value"), 96)
            delta = _phase2_safe_text(item.get("delta"), 96)
            observed_at = _normalise_aware_timestamp(item.get("observed_at"))
            revision_number = item.get("revision_number")
            if (
                symbol is None
                or metric not in allowed_metrics
                or period_end is None
                or previous is None
                or current is None
                or delta is None
                or observed_at is None
                or not isinstance(revision_number, int)
                or isinstance(revision_number, bool)
                or revision_number < 2
            ):
                continue
            revisions.append(
                {
                    "symbol": symbol,
                    "metric": metric,
                    "period_end": period_end,
                    "revision_number": revision_number,
                    "previous_value": previous,
                    "current_value": current,
                    "delta": delta,
                    "supersedes_hash": _normalise_digest(item.get("supersedes_hash")),
                    "observed_at": observed_at,
                    "decision_authority": "SUPPORTING_ONLY",
                }
            )
    provider_health: dict[str, object] = {}
    raw_health = value.get("provider_health")
    if isinstance(raw_health, Mapping):
        for name in (
            "SecCompanyFactsProvider",
            "SecManagementGuidanceProvider",
            "FinnhubValuationProvider",
        ):
            item = raw_health.get(name)
            if not isinstance(item, Mapping):
                continue
            provider_status = str(item.get("status") or "DEGRADED").strip().upper()
            count = item.get("observation_count")
            provider_health[name] = {
                "status": provider_status if provider_status in {"READY", "DEGRADED"} else "DEGRADED",
                "reason_code": _phase2_label(item.get("reason_code"), 64),
                "as_of": _normalise_aware_timestamp(item.get("as_of")),
                "observation_count": count if isinstance(count, int) and not isinstance(count, bool) and count >= 0 else 0,
            }
    result = {
        "schema": "options_copilot.fundamentals_read_model.v1",
        "status": status,
        "reason_codes": _normalise_reason_codes(value.get("reason_codes")),
        "as_of": as_of,
        "rows": rows,
        "row_count": len(rows),
        "categories": categories,
        "revisions": revisions,
        "revision_count": len(revisions),
        "provider_health": provider_health,
        "content_hash": _normalise_digest(value.get("content_hash")),
        "point_in_time_semantics": "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF",
        "correction_policy": "APPEND_ONLY_SUPERSEDES_HASH",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    try:
        assert_no_secret_like(result)
    except SecretLikeFieldError:
        return _unavailable_fundamentals()
    return result


def _unavailable_fundamentals() -> dict[str, object]:
    return {
        "schema": "options_copilot.fundamentals_read_model.v1",
        "status": "UNAVAILABLE",
        "reason_codes": ["FUNDAMENTALS_UNAVAILABLE"],
        "as_of": None,
        "rows": [],
        "row_count": 0,
        "categories": {
            category: {"status": "UNAVAILABLE", "record_count": 0, "reason_code": "FUNDAMENTAL_CATEGORY_UNAVAILABLE"}
            for category in ("CASH_FLOW", "DEBT", "EPS", "GUIDANCE", "REVENUE", "VALUATION")
        },
        "revisions": [],
        "revision_count": 0,
        "provider_health": {},
        "content_hash": None,
        "point_in_time_semantics": "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF",
        "correction_policy": "APPEND_ONLY_SUPERSEDES_HASH",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _normalise_public_https_url(value: object) -> str | None:
    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"data.sec.gov", "www.sec.gov", "finnhub.io"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.fragment
    ):
        return None
    return text


def _normalise_date(value: object) -> str | None:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _normalise_reason_codes(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    result: list[str] = []
    for item in value[:32]:
        reason = _normalise_reason_code(item)
        if reason is not None and reason not in result:
            result.append(reason)
    return result
    return {
        "schema_version": "options_copilot.phase2_advisory.v1",
        "symbol": None,
        "consensus_state": "UNCERTAIN",
        "model_state": "FALLBACK",
        "fallback_reason": "MODEL_TRANSPORT_UNAVAILABLE",
        "as_of": as_of,
        "observations": [],
        "provenance_ids": [],
        "provenance_hashes": [],
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


def _normalise_phase2_observations(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    rows: list[dict[str, object]] = []
    for item in value[:64]:
        if not isinstance(item, Mapping):
            continue
        evidence_id = _phase2_identifier(item.get("evidence_id"), maximum=128)
        evidence_sha256 = _normalise_digest(item.get("evidence_sha256"))
        if evidence_id is None or evidence_sha256 is None:
            continue
        rows.append(
            {
                "evidence_id": evidence_id,
                "evidence_sha256": evidence_sha256,
                "source_tier": _phase2_label(item.get("source_tier"), 32)
                or "UNKNOWN",
                "published_at": _normalise_aware_timestamp(
                    item.get("published_at")
                ),
                "first_seen_at": _normalise_aware_timestamp(
                    item.get("first_seen_at")
                ),
                "observed_at": _normalise_aware_timestamp(
                    item.get("observed_at")
                ),
                "value": _phase2_safe_text(item.get("value"), 96),
                "unit": _phase2_label(item.get("unit"), 48),
                "period": _phase2_label(item.get("period"), 48),
                "basis": _phase2_label(item.get("basis"), 48),
            }
        )
    return rows


def _normalise_phase2_facts(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    rows: list[dict[str, object]] = []
    for item in value[:16]:
        if not isinstance(item, Mapping):
            continue
        statement = _phase2_safe_text(item.get("statement"), 320)
        evidence_ids = _normalise_phase2_identifiers(
            item.get("evidence_ids"),
            maximum=16,
        )
        if statement is None or not evidence_ids:
            continue
        rows.append(
            {
                "statement": statement,
                "status": _phase2_choice(
                    item.get("status"),
                    {"OBSERVED", "UNCERTAIN", "CONFLICTED"},
                )
                or "UNCERTAIN",
                "evidence_ids": evidence_ids,
            }
        )
    return rows


def _normalise_phase2_slice(value: object) -> dict[str, object]:
    unavailable = {
        "status": "UNAVAILABLE",
        "direction": "UNCERTAIN",
        "summary": "No bounded supporting evidence is available.",
        "evidence_ids": [],
    }
    if not isinstance(value, Mapping):
        return unavailable
    summary = _phase2_safe_text(value.get("summary"), 320)
    if summary is None:
        return unavailable
    return {
        "status": _phase2_choice(value.get("status"), _PHASE2_SLICE_STATES)
        or "UNCERTAIN",
        "direction": _phase2_choice(value.get("direction"), _PHASE2_DIRECTIONS)
        or "UNCERTAIN",
        "summary": summary,
        "evidence_ids": _normalise_phase2_identifiers(
            value.get("evidence_ids"),
            maximum=16,
        ),
    }


def _normalise_source_evidence(value: object) -> dict[str, object]:
    """Rebuild exactly six source rows without cross-source health fallback."""

    raw = value if isinstance(value, Mapping) else {}
    raw_sources = raw.get("sources")
    source_by_id: dict[str, Mapping[str, object]] = {}
    duplicate_ids: set[str] = set()
    if isinstance(raw_sources, Sequence) and not isinstance(
        raw_sources,
        (str, bytes, bytearray),
    ):
        for item in raw_sources[:64]:
            if not isinstance(item, Mapping):
                continue
            source_id = _phase2_source_id(item.get("source_id"))
            if source_id is None:
                continue
            if source_id in source_by_id:
                duplicate_ids.add(source_id)
                continue
            source_by_id[source_id] = item

    sources: list[dict[str, object]] = []
    for source_id in _PHASE2_SOURCE_IDS:
        item = source_by_id.get(source_id)
        if item is None or source_id in duplicate_ids:
            sources.append(_unavailable_phase2_source(source_id))
        else:
            sources.append(_normalise_phase2_source_row(source_id, item))

    conflicts = _normalise_phase2_conflicts(raw.get("conflicts"))
    configured = [item for item in sources if item["configured"] is True]
    status = (
        "UNAVAILABLE"
        if not configured
        else "READY"
        if not conflicts and all(item["status"] == "READY" for item in configured)
        else "DEGRADED"
    )
    result = {
        "schema": "options_copilot.source_evidence.v1",
        "status": status,
        "decision": "OBSERVATION_ONLY" if status == "READY" else "NO_TRADE",
        "as_of": _normalise_aware_timestamp(raw.get("as_of")),
        "sources": sources,
        "conflicts": conflicts,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    try:
        assert_no_secret_like(result)
    except SecretLikeFieldError:
        return _unavailable_phase2_source_evidence()
    return result


def _normalise_phase2_source_row(
    source_id: str,
    value: Mapping[str, object],
) -> dict[str, object]:
    configured = value.get("configured") is True
    status = _phase2_choice(value.get("status"), _PHASE2_SOURCE_STATES)
    readiness = _phase2_choice(value.get("readiness"), _PHASE2_SOURCE_STATES)
    if status is None:
        status = "DEGRADED" if configured else "UNAVAILABLE"
    if not configured and status not in {
        "UNCONFIGURED",
        "NOT_CONFIGURED",
        "UNAVAILABLE",
    }:
        status = "UNAVAILABLE"
    if readiness is None:
        readiness = status
    if not configured and readiness == "READY":
        readiness = "UNAVAILABLE"
    reason = None
    if status != "READY":
        reason = _phase2_choice(value.get("reason"), _SOURCE_HEALTH_REASON_CODES)
        if reason is None:
            reason = (
                status
                if status in {"UNCONFIGURED", "NOT_CONFIGURED"}
                else "PROVIDER_DEGRADED"
            )
    freshness = value.get("freshness_age_seconds")
    if (
        isinstance(freshness, bool)
        or not isinstance(freshness, int)
        or not 0 <= freshness <= 31_536_000
    ):
        freshness = None
    return {
        "source_id": source_id,
        "configured": configured,
        "readiness": readiness,
        "status": status,
        "observed_at": _normalise_aware_timestamp(value.get("observed_at")),
        "as_of": _normalise_aware_timestamp(value.get("as_of")),
        "last_success_at": _normalise_aware_timestamp(
            value.get("last_success_at")
        ),
        "freshness_age_seconds": freshness,
        "provenance": _normalise_phase2_identifiers(
            value.get("provenance"),
            maximum=64,
        ),
        "pacing": _phase2_choice(value.get("pacing"), _PHASE2_PACING_STATES)
        or "PACING_UNVERIFIED",
        "reason": reason,
        "decision_authority": "SUPPORTING_ONLY",
    }


def _unavailable_phase2_source(source_id: str) -> dict[str, object]:
    return {
        "source_id": source_id,
        "configured": False,
        "readiness": "UNAVAILABLE",
        "status": "UNAVAILABLE",
        "observed_at": None,
        "as_of": None,
        "last_success_at": None,
        "freshness_age_seconds": None,
        "provenance": [],
        "pacing": "PACING_UNVERIFIED",
        "reason": "PROVIDER_DEGRADED",
        "decision_authority": "SUPPORTING_ONLY",
    }


def _unavailable_phase2_source_evidence() -> dict[str, object]:
    return {
        "schema": "options_copilot.source_evidence.v1",
        "status": "UNAVAILABLE",
        "decision": "NO_TRADE",
        "as_of": None,
        "sources": [
            _unavailable_phase2_source(source_id)
            for source_id in _PHASE2_SOURCE_IDS
        ],
        "conflicts": [],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _normalise_phase2_conflicts(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    conflicts: list[dict[str, object]] = []
    for item in value[:64]:
        if not isinstance(item, Mapping):
            continue
        conflict_id = _phase2_identifier(item.get("conflict_id"), maximum=160)
        source_ids = _normalise_phase2_source_ids(item.get("source_ids"))
        evidence_ids = _normalise_phase2_identifiers(
            item.get("evidence_ids"),
            maximum=64,
        )
        if conflict_id is None or len(source_ids) < 2 or not evidence_ids:
            continue
        if item.get("reason") != "INDEPENDENT_SOURCE_CONFLICT":
            continue
        conflicts.append(
            {
                "conflict_id": conflict_id,
                "source_ids": source_ids,
                "evidence_ids": evidence_ids,
                "reason": "INDEPENDENT_SOURCE_CONFLICT",
            }
        )
    return conflicts


def _normalise_phase2_source_ids(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    result: list[str] = []
    for item in value[:16]:
        source_id = _phase2_source_id(item)
        if source_id is not None and source_id not in result:
            result.append(source_id)
    return result


def _normalise_phase2_identifiers(
    value: object,
    *,
    maximum: int,
) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    result: list[str] = []
    for item in value[:maximum]:
        identifier = _phase2_identifier(item, maximum=240)
        if identifier is not None and identifier not in result:
            result.append(identifier)
    return result


def _normalise_phase2_digests(value: object, *, maximum: int) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    result: list[str] = []
    for item in value[:maximum]:
        digest = _normalise_digest(item)
        if digest is not None and digest not in result:
            result.append(digest)
    return result


def _normalise_phase2_text_list(value: object, *, maximum: int) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    result: list[str] = []
    for item in value[:maximum]:
        text = _phase2_safe_text(item, 320)
        if text is not None and text not in result:
            result.append(text)
    return result


def _phase2_identifier(value: object, *, maximum: int) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if len(candidate) > maximum:
        return None
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/\-]{0,239}", candidate) is None:
        return None
    return candidate


def _phase2_source_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().lower()
    return candidate if candidate in _PHASE2_SOURCE_IDS else None


def _phase2_symbol(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().upper()
    return (
        candidate
        if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", candidate) is not None
        else None
    )


def _phase2_choice(value: object, allowed: frozenset[str] | set[str]) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().upper()
    return candidate if candidate in allowed else None


def _phase2_label(value: object, maximum: int) -> str | None:
    text = _phase2_safe_text(value, maximum)
    if text is None or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/+%()\-]*", text) is None:
        return None
    return text.upper()


def _phase2_safe_text(value: object, maximum: int) -> str | None:
    text = _clean_text(value, maximum)
    if text is None:
        return None
    if (
        "://" in text
        or re.search(r"\b[A-Za-z]:[\\/]", text) is not None
        or re.search(r"\bauthorization\s*:", text, re.IGNORECASE) is not None
    ):
        return None
    try:
        assert_no_secret_like(text)
    except SecretLikeFieldError:
        return None
    return text


def _normalise_news_source_health(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        raw_source = (_clean_text(item.get("source"), 64) or "UNKNOWN").upper()
        source = re.sub(r"[^A-Z0-9_]+", "_", raw_source).strip("_") or "UNKNOWN"
        source_kind = (
            _clean_text(item.get("source_kind"), 32) or "UNKNOWN"
        ).upper()
        if source_kind not in {"NEWS", "CALENDAR", "OFFICIAL_CALENDAR"}:
            source_kind = "UNKNOWN"
        raw_status = (_clean_text(item.get("status"), 24) or "DEGRADED").upper()
        success_count = _normalise_nonnegative_integer(item.get("success_count"))
        failure_date_count = _normalise_nonnegative_integer(
            item.get("failure_date_count")
        )
        status = (
            "READY"
            if raw_status in {"UP", "READY", "HEALTHY"}
            and failure_date_count == 0
            else "DOWN"
            if raw_status == "DOWN"
            else raw_status
            if raw_status in {"UNCONFIGURED", "NOT_CONFIGURED"}
            else "DEGRADED"
        )
        reason: str | None = None
        if status in {"UNCONFIGURED", "NOT_CONFIGURED"}:
            reason = status
        elif status != "READY":
            candidate = (_clean_text(item.get("reason"), 96) or "").upper()
            reason = (
                candidate
                if candidate in _SOURCE_HEALTH_REASON_CODES
                else "PROVIDER_DEGRADED"
            )
        row: dict[str, object] = {
            "source": source[:64],
            "source_kind": source_kind,
            "status": status,
            "reason": reason,
            "success_count": success_count,
            "failure_date_count": failure_date_count,
            "asof": _normalise_timestamp(item.get("asof")),
            "decision_authority": "SUPPORTING_ONLY",
        }
        coverage_status = (
            _clean_text(item.get("coverage_status"), 24) or ""
        ).upper()
        requested_raw = item.get("requested_symbol_count")
        queried_raw = item.get("queried_symbol_count")
        requested_symbol_count = _normalise_nonnegative_integer(requested_raw)
        queried_symbol_count = _normalise_nonnegative_integer(queried_raw)
        coverage_reason = (
            _clean_text(item.get("coverage_reason"), 64) or ""
        ).upper()
        if (
            coverage_status in {"FULL", "BOUNDED"}
            and isinstance(requested_raw, int)
            and not isinstance(requested_raw, bool)
            and isinstance(queried_raw, int)
            and not isinstance(queried_raw, bool)
            and queried_symbol_count <= requested_symbol_count
        ):
            row.update(
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
        rows.append(row)
        if len(rows) == 16:
            break
    return rows


def _normalise_news_publication(value: object) -> dict[str, object]:
    """Keep live acquisition clocks separate from the frozen decision cutoff."""

    if (
        not isinstance(value, Mapping)
        or value.get("source_status_scope") != "LIVE_ACQUISITION_DIAGNOSTIC"
    ):
        return {}
    raw_progress = value.get("refresh_progress")
    progress = raw_progress if isinstance(raw_progress, Mapping) else {}
    raw_durations = progress.get("stage_durations_ms")
    durations = raw_durations if isinstance(raw_durations, Mapping) else {}
    status = progress.get("status")
    stage = progress.get("stage")
    return {
        "source_status_scope": "LIVE_ACQUISITION_DIAGNOSTIC",
        "source_status_observed_at": _normalise_timestamp(value.get("source_status_observed_at")),
        "source_status_evaluated_at": _normalise_timestamp(value.get("source_status_evaluated_at")),
        "read_model_published_at": _normalise_timestamp(value.get("read_model_published_at")),
        "refresh_progress": {
            "status": status if isinstance(status, str) and status in {
                "IDLE", "RUNNING", "COMPLETED", "FAILED",
            } else "UNAVAILABLE",
            "stage": stage if isinstance(stage, str) and stage in _NEWS_PUBLICATION_STAGES else "IDLE",
            "cycle_started_at": _normalise_timestamp(progress.get("cycle_started_at")),
            "cycle_completed_at": _normalise_timestamp(progress.get("cycle_completed_at")),
            "stage_started_at": _normalise_timestamp(progress.get("stage_started_at")),
            "read_model_asof": _normalise_timestamp(progress.get("read_model_asof")),
            "elapsed_ms": _normalise_nonnegative_number(progress.get("elapsed_ms")),
            "stage_elapsed_ms": _normalise_nonnegative_number(progress.get("stage_elapsed_ms")),
            "stage_durations_ms": {
                name: duration
                for name in sorted(_NEWS_PUBLICATION_STAGES - {"IDLE"})
                if (duration := _normalise_nonnegative_number(durations.get(name))) is not None
            },
        },
    }


def _normalise_source_runtime(value: object) -> list[dict[str, object]]:
    """Expose bounded cadence state without paths, provider payloads, or authority."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    rows: list[dict[str, object]] = []
    for item in value[:16]:
        if not isinstance(item, Mapping):
            continue
        source_id = (_clean_text(item.get("source_id"), 64) or "UNKNOWN").upper()
        source_kind = (_clean_text(item.get("source_kind"), 32) or "UNKNOWN").upper()
        cadence_status = (_clean_text(item.get("cadence_status"), 24) or "SUPPRESSED").upper()
        freshness = (_clean_text(item.get("freshness"), 24) or "UNAVAILABLE").upper()
        failure_code = (_clean_text(item.get("failure_code"), 96) or "").upper() or None
        rows.append({
            "schema": "options_copilot.source_cadence.v1",
            "source_id": re.sub(r"[^A-Z0-9_]+", "_", source_id).strip("_") or "UNKNOWN",
            "source_kind": source_kind if source_kind in {"NEWS", "CALENDAR", "OFFICIAL_CALENDAR", "REACTION", "ALL"} else "UNKNOWN",
            "configured": item.get("configured") is True,
            "authority": "SUPPORTING_ONLY",
            "cadence_status": cadence_status if cadence_status in {"DUE", "WAITING", "SUPPRESSED"} else "SUPPRESSED",
            "interval_seconds": _normalise_nonnegative_integer(item.get("interval_seconds")),
            "last_attempt": _normalise_timestamp(item.get("last_attempt")),
            "last_success": _normalise_timestamp(item.get("last_success")),
            "next_due": _normalise_timestamp(item.get("next_due")),
            "freshness": freshness if freshness in {"CURRENT", "STALE", "NEVER", "UNAVAILABLE"} else "UNAVAILABLE",
            "failure_code": failure_code,
            "attempt_count": _normalise_nonnegative_integer(item.get("attempt_count")),
            "success_count": _normalise_nonnegative_integer(item.get("success_count")),
            "skip_count": _normalise_nonnegative_integer(item.get("skip_count")),
        })
    return rows


def _normalise_feed_item(
    raw: Mapping[str, Any],
    *,
    key: Literal["news", "calendar"],
    index: int,
    envelope_asof: str | None = None,
) -> dict[str, object]:
    """Create one JSON-safe, authority-free record from a source mapping."""

    source_event = raw.get("news") if isinstance(raw.get("news"), Mapping) else {}
    identifier = _clean_text(
        raw.get("id", raw.get("analysis_id", raw.get("event_id", raw.get("uuid", source_event.get("event_id"))))),
        128,
    )
    title = _clean_display_text(
        raw.get("title", raw.get("headline", raw.get("name", source_event.get("headline")))), maximum=280
    )
    times = {
        "event_at": _normalise_timestamp(raw.get("event_at", raw.get("scheduled_at"))),
        "published_at": _normalise_timestamp(raw.get("published_at", raw.get("published", source_event.get("published_at")))),
        "first_seen_at": _normalise_timestamp(raw.get("first_seen_at", source_event.get("first_seen_at"))),
        "received_at": _normalise_timestamp(raw.get("received_at", raw.get("ingested_at", source_event.get("first_seen_at")))),
        "observed_at": _normalise_timestamp(raw.get("observed_at", raw.get("asof"))),
        "analysis_completed_at": _normalise_timestamp(raw.get("analysis_completed_at")),
    }
    raw_scores = raw.get("scores") if isinstance(raw.get("scores"), Mapping) else {}
    item: dict[str, object] = {
        "id": identifier or f"{key}-{index + 1}",
        "title": title or "Untitled observation",
        "summary": _clean_display_text(
            raw.get("summary", raw.get("description", source_event.get("summary"))),
            1_200,
        ),
        "source": _clean_text(raw.get("source", raw.get("provider", source_event.get("source"))), 120),
        "symbols": _normalise_symbols(raw.get("symbols", raw.get("tickers", raw.get("symbol", source_event.get("symbols"))))),
        "status": _normalise_observation_status(
            raw.get("status", raw.get("stage", raw.get("state")))
        ),
        "category": _normalise_category(
            raw.get(
                "category",
                raw.get("classification", {}).get("category")
                if isinstance(raw.get("classification"), Mapping)
                else None,
            )
        ),
        "scores": {
            "event_impact_score": _normalise_score(raw.get("event_impact_score", raw_scores.get("event_impact_score"))),
            "option_tradability_score": _normalise_score(
                raw.get("option_tradability_score", raw_scores.get("option_tradability_score"))
            ),
            "combined_opportunity_score": _normalise_score(
                raw.get("combined_opportunity_score", raw_scores.get("combined_opportunity_score"))
            ),
        },
        "direction": _clean_text(raw.get("direction"), 24),
        "horizon": _clean_text(raw.get("horizon"), 32),
        "confidence": _normalise_score(raw.get("confidence")),
        "rank": _normalise_rank(raw.get("rank"), maximum=10),
        "research_rank": _normalise_rank(raw.get("research_rank", raw.get("rank")), maximum=10),
        "deterministic_research_rank": _normalise_rank(
            raw.get("research_rank", raw.get("rank")), maximum=10
        ),
        "shadow_suggested_rank": _normalise_rank(
            raw.get("shadow_suggested_rank"), maximum=10
        ),
        "watch_rank": _normalise_rank(raw.get("watch_rank"), maximum=10),
        "action_rank": _normalise_rank(raw.get("action_rank"), maximum=3),
        "rank_one": raw.get("rank_one") is True,
        "research_pool": raw.get("research_pool") is True,
        "action_pool": raw.get("action_pool") is True,
        "action_pool_eligible": raw.get("action_pool_eligible") is True,
        "decision_authority": "SUPPORTING_ONLY",
        "times": times,
        "latency": {
            "published_to_first_seen_ms": _normalise_nonnegative_number(
                raw.get("published_to_first_seen_ms")
            ),
            "first_seen_to_analysis_ms": _normalise_nonnegative_number(
                raw.get("first_seen_to_analysis_ms")
            ),
        },
        "evidence": _normalise_evidence(raw.get("evidence", raw.get("sources"))),
        "provenance": _normalise_provenance(raw.get("provenance")),
        "counter_evidence": _normalise_text_list(raw.get("counter_evidence"), maximum=8),
        "ibkr_provenance": _normalise_ibkr_provenance(raw.get("ibkr_provenance")),
        "related_options": _normalise_related_options(
            raw.get("related_options", raw.get("options_research"))
        ),
    }
    if key == "news":
        if "source_url" in raw:
            item["source_url"] = _normalise_public_url(raw.get("source_url"))
        if "story_identity" in raw:
            item["story_identity"] = _clean_text(raw.get("story_identity"), 128)
        if "provider_story_id" in raw:
            item["provider_story_id"] = _clean_text(
                raw.get("provider_story_id"),
                180,
            )
        if "deduplicated" in raw:
            item["deduplicated"] = raw.get("deduplicated") is True
        if "merged_event_count" in raw:
            item["merged_event_count"] = _normalise_nonnegative_integer(
                raw.get("merged_event_count")
            )
        if "evidence_count" in raw:
            item["evidence_count"] = _normalise_nonnegative_integer(
                raw.get("evidence_count")
            )
        deterministic_rank = item["deterministic_research_rank"]
        shadow_rank = item["shadow_suggested_rank"]
        item["rank_displacement"] = (
            shadow_rank - deterministic_rank
            if isinstance(shadow_rank, int) and isinstance(deterministic_rank, int)
            else None
        )
        item["shadow_action_effect"] = "NONE"
        item["shadow_risk_effect"] = "NONE"
        item["shadow_eligibility_effect"] = "NONE"
        item["classifier"] = _clean_text(raw.get("classifier"), 80)
        research_advisory = _normalise_research_advisory(
            raw.get("research_advisory")
        )
        if research_advisory is not None:
            item["research_advisory"] = research_advisory
        item["symbol_binding"] = _normalise_symbol_binding(
            raw.get("symbol_binding"),
            symbols=item["symbols"],
        )
        research_proxy = _normalise_research_proxy_binding(
            raw.get("research_proxy_binding"),
            symbols=item["symbols"],
        )
        if research_proxy is not None:
            item["research_proxy_binding"] = research_proxy
        binding_status = str(item["symbol_binding"]["status"])
        if binding_status == "UNBOUND" or _is_unverified_symbol_binding_status(
            binding_status
        ):
            item["symbols"] = []
            item["ibkr_provenance"] = None
            item["watch_rank"] = None
            item["action_rank"] = None
            item["action_pool"] = False
            item["action_pool_eligible"] = False
            item["related_options"] = []
    if key == "calendar":
        for news_only in (
            "scores",
            "direction",
            "horizon",
            "confidence",
            "rank",
            "research_rank",
            "watch_rank",
            "action_rank",
            "rank_one",
            "research_pool",
            "action_pool",
            "action_pool_eligible",
            "latency",
            "counter_evidence",
            "ibkr_provenance",
            "related_options",
        ):
            item.pop(news_only, None)
        item["importance"] = _clean_text(raw.get("importance"), 24)
        item["country"] = _clean_text(raw.get("country", raw.get("region")), 48)
        item["event_date"] = _clean_text(raw.get("event_date"), 16)
        item["event_timezone"] = _clean_text(
            raw.get("event_timezone", raw.get("timezone")), 80
        )
        item["schedule_precision"] = _clean_text(
            raw.get("schedule_precision", raw.get("time_precision")), 32
        )
        item["windows"] = _normalise_calendar_windows(raw.get("windows"))
        item["content_hash"] = _normalise_digest(raw.get("content_hash"))
        item["record_hash"] = _normalise_digest(raw.get("record_hash"))
        calendar_origin = (_clean_text(raw.get("calendar_origin"), 16) or "UNKNOWN").upper()
        if calendar_origin not in {"OFFICIAL", "LEGACY"}:
            calendar_origin = "UNKNOWN"
        reaction_identity_hash = _normalise_digest(raw.get("reaction_identity_hash"))
        item["calendar_origin"] = calendar_origin
        item["reaction_identity_hash"] = reaction_identity_hash
        item["reaction"] = _normalise_calendar_reaction(
            raw.get("reaction"),
            event_id=str(item["id"]),
            event_hash=reaction_identity_hash,
            official=calendar_origin == "OFFICIAL",
            scheduled_at=times["event_at"],
            observed_at=envelope_asof,
        )
        item["measure_reactions"] = _normalise_measure_reactions(
            raw.get("measure_reactions"),
            official=calendar_origin == "OFFICIAL",
        )
        item["revision_views"] = _normalise_revision_views(
            raw.get("revision_views"),
            official=calendar_origin == "OFFICIAL",
        )
        item["approval_eligible"] = False
        item["instruction_creation_allowed"] = False
        item["order_creation_allowed"] = False
    intelligence_input = dict(item)
    raw_intelligence = raw.get("intelligence")
    if isinstance(raw_intelligence, Mapping):
        allowed_categories = {
            "MACRO", "SECTOR", "COMPANY", "EARNINGS", "REGULATORY",
            "GEOPOLITICAL", "MARKET_STRUCTURE", "UNKNOWN",
        }
        primary = (_clean_text(raw_intelligence.get("primary_category"), 48) or "").upper()
        if primary in allowed_categories:
            intelligence_input["category"] = primary
        raw_facets = raw_intelligence.get("facets")
        if isinstance(raw_facets, Sequence) and not isinstance(
            raw_facets, (str, bytes, bytearray)
        ):
            intelligence_input["facets"] = [
                facet
                for value in raw_facets[:8]
                if (facet := (_clean_text(value, 48) or "").upper()) in allowed_categories
            ]
    item["intelligence"] = project_event_intelligence(intelligence_input)
    return item


def _normalise_research_advisory(value: object) -> dict[str, object] | None:
    """Expose only the narrow, authority-free DeepSeek shadow projection."""

    if not isinstance(value, Mapping):
        return None
    classifier = (_clean_text(value.get("classifier"), 80) or "").upper()
    raw_classification = value.get("classification")
    classification = (
        raw_classification if isinstance(raw_classification, Mapping) else {}
    )
    classification_classifier = (
        _clean_text(classification.get("classifier"), 80) or ""
    ).upper()
    if (
        classifier != "STRUCTURED_LLM"
        or classification_classifier != "STRUCTURED_LLM"
    ):
        return None

    direction = (
        _clean_text(classification.get("direction"), 24) or "UNKNOWN"
    ).upper()
    if direction not in {"BULLISH", "BEARISH", "MIXED", "NEUTRAL", "UNKNOWN"}:
        direction = "UNKNOWN"
    horizon = (
        _clean_text(classification.get("horizon"), 32) or "UNKNOWN"
    ).upper()
    if horizon not in {"INTRADAY", "DAYS_1_3", "DAYS_4_10", "WEEKS_2_4"}:
        horizon = "UNKNOWN"

    return {
        "classifier": "STRUCTURED_LLM",
        "research_priority_score": _normalise_score(
            value.get("research_priority_score")
        ),
        "classification": {
            "category": _normalise_category(classification.get("category")),
            "symbols": _normalise_symbols(classification.get("symbols")),
            "direction": direction,
            "horizon": horizon,
            "confidence": _normalise_score(classification.get("confidence")),
            "counter_evidence": _normalise_phase2_text_list(
                classification.get("counter_evidence"),
                maximum=8,
            ),
            "classifier": "STRUCTURED_LLM",
        },
        "shadow_prediction_count": _normalise_nonnegative_integer(
            value.get("shadow_prediction_count")
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _clean_text(value: object, maximum: int) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned[:maximum] if cleaned else None


_DISPLAY_MOJIBAKE_MARKERS = (
    "\u00e2\u0080",
    "\u00e2\u20ac",
    "\u00c2\u00a0",
)


def _clean_display_text(value: object, maximum: int) -> str | None:
    """Repair proven UTF-8 mojibake only in the non-authoritative read model."""

    if not isinstance(value, str):
        return None
    repaired = _repair_display_mojibake(value)
    return _clean_text(repaired, maximum)


def _repair_display_mojibake(value: str) -> str:
    marker_count = sum(value.count(marker) for marker in _DISPLAY_MOJIBAKE_MARKERS)
    if marker_count == 0:
        return value
    best = value
    best_marker_count = marker_count
    for encoding in ("latin-1", "cp1252"):
        try:
            candidate = value.encode(encoding, errors="strict").decode(
                "utf-8",
                errors="strict",
            )
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if "\ufffd" in candidate:
            continue
        candidate_marker_count = sum(
            candidate.count(marker) for marker in _DISPLAY_MOJIBAKE_MARKERS
        )
        if candidate_marker_count < best_marker_count:
            best = candidate
            best_marker_count = candidate_marker_count
    return best


def _clean_contract_identity_text(value: object, maximum: int) -> str | None:
    """Preserve broker-significant internal spaces without truncating identity."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned if cleaned and len(cleaned) <= maximum else None


def _normalise_timestamp(value: object) -> str | None:
    return _clean_text(value, 64)


def _normalise_nonnegative_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if not isinstance(value, (int, float, Decimal)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return (
        number
        if math.isfinite(number) and number >= 0 and number < 1_000_000_000
        else None
    )


def _enforce_news_pool_consistency(items: Sequence[dict[str, object]]) -> None:
    """Fail closed on duplicate ranks or incomplete action-pool bindings."""

    research_ranks: set[int] = set()
    watch_ranks: set[int] = set()
    action_ranks: set[int] = set()
    for item in items:
        research_rank = item.get("research_rank")
        research_pool = (
            item.get("research_pool") is True
            and isinstance(research_rank, int)
            and research_rank not in research_ranks
            and len(research_ranks) < 10
        )
        item["research_pool"] = research_pool
        if research_pool:
            research_ranks.add(research_rank)

        watch_rank = item.get("watch_rank")
        symbols = item.get("symbols")
        binding = item.get("symbol_binding")
        binding_status = (
            str(binding.get("status") or "").upper()
            if isinstance(binding, Mapping)
            else ""
        )
        watch_rank_valid = (
            isinstance(watch_rank, int)
            and isinstance(symbols, Sequence)
            and not isinstance(symbols, (str, bytes, bytearray))
            and len(symbols) == 1
            and binding_status != "UNBOUND"
            and not _is_unverified_symbol_binding_status(binding_status)
            and watch_rank not in watch_ranks
            and len(watch_ranks) < 10
        )
        item["watch_rank"] = watch_rank if watch_rank_valid else None
        if watch_rank_valid:
            watch_ranks.add(watch_rank)

        action_rank = item.get("action_rank")
        action_pool = (
            item.get("action_pool") is True
            and item.get("action_pool_eligible") is True
            and isinstance(item.get("ibkr_provenance"), Mapping)
            and isinstance(action_rank, int)
            and action_rank not in action_ranks
            and len(action_ranks) < 3
        )
        item["action_pool"] = action_pool
        item["action_pool_eligible"] = action_pool
        item["rank_one"] = action_pool and action_rank == 1
        if action_pool:
            action_ranks.add(action_rank)


def _read_only_pool_counts(items: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Derive bounded pool counts from normalized rows, never provider claims."""

    return {
        "research_pool_count": min(
            10, sum(item.get("research_pool") is True for item in items)
        ),
        "action_pool_count": min(
            3, sum(item.get("action_pool") is True for item in items)
        ),
        "approval_eligible": False,
    }


def _normalise_analysis_backfill(value: object) -> dict[str, object]:
    """Expose bounded recovery progress without trusting provider authority."""

    raw = value if isinstance(value, Mapping) else {}
    status = str(raw.get("status") or "UNAVAILABLE").strip().upper()
    if status not in _ANALYSIS_BACKFILL_STATES:
        status = "DEGRADED"
    reason = _clean_text(raw.get("reason"), 96)
    if reason is not None:
        reason = reason.upper().replace("-", "_").replace(" ", "_")
        if re.fullmatch(r"[A-Z0-9_]{1,96}", reason) is None:
            reason = "ANALYSIS_BACKFILL_INVALID"
    elif status != "READY":
        reason = "ANALYSIS_BACKFILL_UNAVAILABLE"

    raw_integrity = raw.get("integrity")
    integrity = raw_integrity if isinstance(raw_integrity, Mapping) else {}
    integrity_status = str(
        integrity.get("status") or ("VERIFIED" if status == "READY" else "PENDING")
    ).strip().upper()
    if integrity_status not in _ANALYSIS_INTEGRITY_STATES:
        integrity_status = "DEGRADED"
    complete = integrity.get("complete") is True and integrity_status == "VERIFIED"
    if integrity_status == "DEGRADED":
        status = "DEGRADED"
        reason = reason or "ANALYSIS_INTEGRITY_INVALID"
    elif not complete and status == "READY":
        status = "DEGRADED"
        reason = "ANALYSIS_INTEGRITY_NOT_VERIFIED"

    return {
        "status": status,
        "reason": reason,
        "pending_count": _normalise_nonnegative_integer(raw.get("pending_count")),
        "failed_count": _normalise_nonnegative_integer(raw.get("failed_count")),
        "persisted_hits_this_cycle": _normalise_nonnegative_integer(
            raw.get("persisted_hits_this_cycle")
        ),
        "model_misses_this_cycle": _normalise_nonnegative_integer(
            raw.get("model_misses_this_cycle")
        ),
        "model_batch_limit": _normalise_nonnegative_integer(
            raw.get("model_batch_limit", raw.get("batch_limit"))
        ),
        "local_restore_batch_limit": _normalise_nonnegative_integer(
            raw.get("local_restore_batch_limit")
        ),
        "lookup_batch_limit": _normalise_nonnegative_integer(
            raw.get("lookup_batch_limit")
        ),
        "integrity": {
            "status": integrity_status,
            "batch_rows": _normalise_nonnegative_integer(integrity.get("batch_rows")),
            "verified_rows": _normalise_nonnegative_integer(
                integrity.get("verified_rows")
            ),
            "remaining_rows": _normalise_nonnegative_integer(
                integrity.get("remaining_rows")
            ),
            "complete": complete,
        },
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
    }


def _normalise_shadow_advisory_status(value: object) -> dict[str, object]:
    """Expose only bounded, authority-free shadow execution evidence."""

    raw = value if isinstance(value, Mapping) else {}
    status = str(raw.get("status") or "UNAVAILABLE").strip().upper()
    if status not in _SHADOW_ADVISORY_STATES:
        status = "DEGRADED"
        reason = "SHADOW_ADVISORY_STATUS_INVALID"
    else:
        reason = (_clean_text(raw.get("reason"), 96) or "").upper()
        if reason and reason not in _SHADOW_ADVISORY_REASONS:
            status = "DEGRADED"
            reason = "SHADOW_ADVISORY_STATUS_INVALID"
        elif not reason and status != "READY":
            reason = "SHADOW_ADVISORY_UNAVAILABLE"
        elif not reason:
            reason = None

    maximum_batch_size = _normalise_nonnegative_integer(
        raw.get("maximum_batch_size")
    )
    if maximum_batch_size > 3:
        maximum_batch_size = 0
        status = "DEGRADED"
        reason = "SHADOW_ADVISORY_STATUS_INVALID"

    raw_failure_reasons = raw.get("failure_reasons")
    failure_reasons: dict[str, int] = {}
    if isinstance(raw_failure_reasons, Mapping):
        for raw_reason, raw_count in raw_failure_reasons.items():
            fixed_reason = str(raw_reason).strip().upper()
            count = _normalise_nonnegative_integer(raw_count)
            if fixed_reason in _SHADOW_FAILURE_REASONS and count:
                failure_reasons[fixed_reason] = count

    raw_skipped_reasons = raw.get("skipped_reasons")
    skipped_reasons: dict[str, int] = {}
    if isinstance(raw_skipped_reasons, Mapping):
        for raw_reason, raw_count in raw_skipped_reasons.items():
            fixed_reason = str(raw_reason).strip().upper()
            count = _normalise_nonnegative_integer(raw_count)
            if fixed_reason in _SHADOW_SKIP_REASONS and count:
                skipped_reasons[fixed_reason] = count

    return {
        "status": status,
        "reason": reason,
        "advisory_count": _normalise_nonnegative_integer(
            raw.get("advisory_count")
        ),
        "attempted_count": _normalise_nonnegative_integer(
            raw.get("attempted_count")
        ),
        "failure_count": _normalise_nonnegative_integer(raw.get("failure_count")),
        "deferred_count": _normalise_nonnegative_integer(
            raw.get("deferred_count")
        ),
        "failure_reasons": dict(sorted(failure_reasons.items())),
        "input_count": _normalise_nonnegative_integer(raw.get("input_count")),
        "eligible_input_count": _normalise_nonnegative_integer(
            raw.get("eligible_input_count")
        ),
        "skipped_count": _normalise_nonnegative_integer(
            raw.get("skipped_count")
        ),
        "skipped_reasons": dict(sorted(skipped_reasons.items())),
        "maximum_batch_size": maximum_batch_size,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _normalise_nonnegative_integer(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _normalise_bounded_nonnegative_integer(
    value: object,
    *,
    maximum: int,
) -> int | None:
    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= maximum
    ):
        return value
    return None


def _normalise_rank(value: object, *, maximum: int) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= maximum:
        return value
    return None


def _normalise_score(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    if not math.isfinite(number) or number < 0 or number > 100:
        return None
    return round(number * 100 if number <= 1 else number, 2)


def _normalise_symbols(value: object) -> list[str]:
    raw_values = value if isinstance(value, Sequence) and not isinstance(value, str) else [value]
    symbols: list[str] = []
    for raw_symbol in raw_values:
        if not isinstance(raw_symbol, str):
            continue
        symbol = raw_symbol.strip().upper()
        if re.fullmatch(r"[A-Z][A-Z0-9.\-/]{0,14}", symbol) and symbol not in symbols:
            symbols.append(symbol)
    return symbols[:12]


def _normalise_observation_status(value: object) -> str:
    status = (_clean_text(value, 32) or "PROVISIONAL").upper()
    return status if status in {
        "PROVISIONAL", "CONFIRMED", "MARKET_CONFIRMED", "CONFLICTED", "NO_TRADE"
    } else "PROVISIONAL"


def _normalise_category(value: object) -> str:
    """Expose a display-only event classification without provider passthrough."""
    category = (_clean_text(value, 48) or "UNCATEGORIZED").upper()
    return category if re.fullmatch(r"[A-Z][A-Z0-9_ -]{0,47}", category) else "UNCATEGORIZED"


def _normalise_calendar_envelope(raw: Mapping[str, Any]) -> dict[str, object]:
    decision = (_clean_text(raw.get("decision"), 32) or "NO_TRADE").upper()
    if decision not in {"OBSERVATION_ONLY", "NO_TRADE"}:
        decision = "NO_TRADE"
    reaction_provider = _normalise_reaction_provider(raw.get("reaction_provider"))
    reaction_decision = (
        _clean_text(raw.get("reaction_decision"), 32) or "NO_TRADE"
    ).upper()
    if (
        reaction_decision not in {"OBSERVATION_ONLY", "NO_TRADE"}
        or reaction_provider["status"] != "READY"
        or reaction_provider["decision"] != "OBSERVATION_ONLY"
    ):
        reaction_decision = "NO_TRADE"
    return {
        "window_start": _normalise_timestamp(raw.get("window_start")),
        "window_end": _normalise_timestamp(raw.get("window_end")),
        "decision": decision,
        "decision_authority": "SUPPORTING_ONLY",
        "reasons": _normalise_text_list(raw.get("reasons"), maximum=32),
        "sources": _normalise_calendar_sources(raw.get("sources")),
        "snapshot_hash": _normalise_digest(raw.get("snapshot_hash")),
        "reaction_provider": reaction_provider,
        "reaction_decision": reaction_decision,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


_REACTION_REASON_CODES = frozenset(
    {
        "MISSING_OFFICIAL_ACTUAL",
        "OFFICIAL_SOURCE_MISMATCH",
        "OFFICIAL_RELEASE_CONFLICT",
        "RELEASE_EXPECTATION_MISMATCH",
        "MISSING_MARKET_EVIDENCE",
        "STALE_MARKET_EVIDENCE",
        "MISSING_OPTION_EVIDENCE",
        "STALE_OPTION_EVIDENCE",
        "REACTION_REQUIRES_OFFICIAL_CALENDAR",
        "OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE",
        "OFFICIAL_CALENDAR_EVENT_UNAVAILABLE",
        "REACTION_PROVIDER_UNCONFIGURED",
        "REACTION_PROVIDER_UNAVAILABLE",
        "REACTION_LEDGER_DUPLICATE",
        "REACTION_LEDGER_FROM_FUTURE",
        "REACTION_LEDGER_INTEGRITY_FAILED",
        "REACTION_LEDGER_EVENT_HASH_MISMATCH",
        "REACTION_EVENT_IDENTITY_UNAVAILABLE",
        "REACTION_EVENT_NOT_IN_CURRENT_OFFICIAL_SNAPSHOT",
        "REACTION_LEDGER_UNAVAILABLE",
        "REACTION_LEDGER_COVERAGE_INCOMPLETE",
        "REACTION_EVENT_UNSUPPORTED",
        "NO_ELIGIBLE_REACTION_EVENTS",
        "WAIT_FOR_DECLARED_RELEASE_TIME",
        "WAITING_NEXT_ELIGIBLE_RELEASE",
        "REACTION_API_BINDING_MISMATCH",
        "REACTION_API_READ_MODEL_INVALID",
    }
)
_REACTION_NORMAL_STAGES = (
    "SCHEDULED",
    "AWAITING_RELEASE",
    "RELEASE_CAPTURED",
    "SURPRISE_ASSESSED",
    "MARKET_REACTION_OBSERVED",
    "OPTION_REEVALUATED",
)
_REACTION_FAILURE_STATUSES = frozenset(
    {"UNAVAILABLE", "CONFLICTED", "DEGRADED", "NO_TRADE"}
)


def _normalise_reaction_provider(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return _reaction_provider_failure("REACTION_PROVIDER_UNAVAILABLE")
    status = (_clean_text(value.get("status"), 24) or "UNAVAILABLE").upper()
    if status not in {"READY", "UNAVAILABLE", "CONFLICTED"}:
        return _reaction_provider_failure("REACTION_PROVIDER_UNAVAILABLE")
    decision = (_clean_text(value.get("decision"), 32) or "NO_TRADE").upper()
    ledger_count = _normalise_nonnegative_integer(value.get("ledger_count"))
    matched_count = _normalise_nonnegative_integer(value.get("matched_count"))
    ignored_count = _normalise_nonnegative_integer(value.get("ignored_count"))
    supported_event_ids = _normalise_reaction_event_ids(
        value.get("supported_event_ids")
    )
    eligible_event_ids = _normalise_reaction_event_ids(
        value.get("eligible_event_ids")
    )
    supported_count = _normalise_nonnegative_integer(value.get("supported_count"))
    eligible_count = _normalise_nonnegative_integer(value.get("eligible_count"))
    unsupported_count = _normalise_nonnegative_integer(value.get("unsupported_count"))
    last_attempt = _normalise_reaction_timestamp(value.get("last_attempt"))
    reason = _normalise_reaction_reason(value.get("reason"))
    event_count = _normalise_nonnegative_integer(value.get("event_count"))
    if "event_count" not in value:
        event_count = supported_count + unsupported_count
    measure_count = _normalise_nonnegative_integer(value.get("measure_count"))
    reaction_counts = {
        name: _normalise_nonnegative_integer(value.get(name))
        for name in (
            "capture_spec_count",
            "captured_release_vintage_count",
            "captured_measure_count",
            "capture_eligible_count",
            "surprise_ready_count",
            "progressed_event_count",
        )
    }
    next_eligible_release_at = _clean_text(value.get("next_eligible_release_at"), 64)
    if next_eligible_release_at != "NEXT_ELIGIBLE_RELEASE_UNKNOWN":
        next_eligible_release_at = _normalise_reaction_timestamp(
            next_eligible_release_at
        ) or "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
    schedule_refresh_status = _clean_text(
        value.get("schedule_refresh_status"),
        32,
    )
    if schedule_refresh_status not in {"READY", "DEGRADED", "UNAVAILABLE", "UNKNOWN"}:
        schedule_refresh_status = "UNKNOWN"
    schedule_refresh_reason = _normalise_reaction_label(
        value.get("schedule_refresh_reason"),
        160,
    )
    schedule_hash = _normalise_digest(value.get("schedule_hash"))
    descriptor_wait_count = _normalise_nonnegative_integer(
        value.get("descriptor_wait_count")
    )
    next_action = _normalise_reaction_label(value.get("next_action"), 96)
    if next_action is None:
        next_action = (
            "WAIT_NEXT_ELIGIBLE_RELEASE"
            if reason == "NO_ELIGIBLE_REACTION_EVENTS"
            else "OBSERVE_CURRENT_ELIGIBLE_EVENTS"
        )
    support_matrix = _normalise_reaction_support_matrix(value.get("support_matrix"))
    worker_health = _normalise_reaction_worker_health(value.get("worker_health"))
    lifecycle_supersessions = _normalise_reaction_supersessions(
        value.get("lifecycle_supersessions")
    )
    healthy_idle = (
        status == "READY"
        and decision == "OBSERVATION_ONLY"
        and reason == "NO_ELIGIBLE_REACTION_EVENTS"
        and eligible_count == 0
        and matched_count == 0
        and ledger_count == 0
    )
    if (
        status != "READY"
        or decision not in {"OBSERVATION_ONLY", "NO_TRADE"}
        or matched_count > ledger_count
        or matched_count > eligible_count
        or len(supported_event_ids) != supported_count
        or len(eligible_event_ids) != eligible_count
        or not set(eligible_event_ids).issubset(supported_event_ids)
        or (matched_count == 0 and not healthy_idle)
        or (eligible_count > 0 and matched_count != eligible_count)
    ):
        return _reaction_provider_failure(
            reason or (
                "REACTION_LEDGER_COVERAGE_INCOMPLETE"
                if status == "READY"
                else "REACTION_PROVIDER_UNAVAILABLE"
            ),
            status=status if status in {"UNAVAILABLE", "CONFLICTED"} else "UNAVAILABLE",
            ledger_count=ledger_count,
            matched_count=min(matched_count, ledger_count),
            ignored_count=ignored_count,
            supported_event_ids=supported_event_ids,
            eligible_event_ids=eligible_event_ids,
            supported_count=supported_count,
            eligible_count=eligible_count,
            unsupported_count=unsupported_count,
            last_attempt=last_attempt,
            worker_health=worker_health,
            lifecycle_supersessions=lifecycle_supersessions,
            schedule_refresh_status=schedule_refresh_status,
            schedule_refresh_reason=schedule_refresh_reason,
            schedule_hash=schedule_hash,
        )
    return {
        "name": "event-reaction-provider",
        "status": "READY",
        "decision": decision,
        "reason": reason if healthy_idle else None,
        "ledger_count": ledger_count,
        "matched_count": matched_count,
        "ignored_count": ignored_count,
        "supported_event_ids": supported_event_ids,
        "supported_count": supported_count,
        "eligible_event_ids": eligible_event_ids,
        "eligible_count": eligible_count,
        "unsupported_count": unsupported_count,
        "last_attempt": last_attempt,
        "event_count": event_count,
        "measure_count": measure_count,
        **reaction_counts,
        "next_eligible_release_at": next_eligible_release_at,
        "schedule_refresh_status": schedule_refresh_status,
        "schedule_refresh_reason": schedule_refresh_reason,
        "schedule_hash": schedule_hash,
        "descriptor_wait_count": descriptor_wait_count,
        "next_action": next_action,
        "support_matrix": support_matrix,
        "worker_health": worker_health,
        "lifecycle_supersessions": lifecycle_supersessions,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _reaction_provider_failure(
    reason: str,
    *,
    status: str = "UNAVAILABLE",
    ledger_count: int = 0,
    matched_count: int = 0,
    ignored_count: int = 0,
    supported_event_ids: Sequence[str] = (),
    eligible_event_ids: Sequence[str] = (),
    supported_count: int = 0,
    eligible_count: int = 0,
    unsupported_count: int = 0,
    last_attempt: str | None = None,
    worker_health: Mapping[str, Mapping[str, object]] | None = None,
    lifecycle_supersessions: Mapping[str, str] | None = None,
    schedule_refresh_status: str = "UNAVAILABLE",
    schedule_refresh_reason: str | None = "REACTION_SCHEDULE_REFRESH_UNAVAILABLE",
    schedule_hash: str | None = None,
) -> dict[str, object]:
    return {
        "name": "event-reaction-provider",
        "status": status,
        "decision": "NO_TRADE",
        "reason": reason,
        "ledger_count": ledger_count,
        "matched_count": matched_count,
        "ignored_count": ignored_count,
        "supported_event_ids": list(supported_event_ids),
        "supported_count": supported_count,
        "eligible_event_ids": list(eligible_event_ids),
        "eligible_count": eligible_count,
        "unsupported_count": unsupported_count,
        "last_attempt": last_attempt,
        "event_count": 0,
        "measure_count": 0,
        "capture_spec_count": 0,
        "captured_release_vintage_count": 0,
        "captured_measure_count": 0,
        "capture_eligible_count": 0,
        "surprise_ready_count": 0,
        "progressed_event_count": 0,
        "next_eligible_release_at": "NEXT_ELIGIBLE_RELEASE_UNKNOWN",
        "schedule_refresh_status": schedule_refresh_status,
        "schedule_refresh_reason": schedule_refresh_reason,
        "schedule_hash": schedule_hash,
        "descriptor_wait_count": 0,
        "next_action": "WAIT_NEXT_ELIGIBLE_RELEASE",
        "support_matrix": [],
        "worker_health": {
            str(lane): dict(health)
            for lane, health in (worker_health or {}).items()
        },
        "lifecycle_supersessions": dict(lifecycle_supersessions or {}),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _normalise_reaction_support_matrix(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    output: list[dict[str, object]] = []
    for raw in tuple(value)[:32]:
        if not isinstance(raw, Mapping):
            continue
        state = (_clean_text(raw.get("support_state"), 32) or "").upper()
        if state not in {"SUPPORTED", "PREFLIGHT_ONLY", "UNSUPPORTED"}:
            continue
        output.append(
            {
                "family": _clean_text(raw.get("family"), 64) or "UNKNOWN",
                "support_state": state,
                "measure_count": _normalise_nonnegative_integer(raw.get("measure_count")),
                "surprise_supported": raw.get("surprise_supported") is True,
                "reason": _normalise_reaction_reason(raw.get("reason")),
            }
        )
    return output


def _normalise_reaction_worker_health(value: object) -> dict[str, dict[str, object]]:
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, dict[str, object]] = {}
    for lane in ("schedule", "capture", "observer"):
        raw = value.get(lane)
        if not isinstance(raw, Mapping):
            continue
        reason = _normalise_reaction_label(raw.get("reason"), 160)
        attempted_at = _normalise_reaction_timestamp(raw.get("attempted_at"))
        if reason is None or attempted_at is None:
            continue
        output[lane] = {
            "status": "DEGRADED",
            "reason": reason,
            "attempted_at": attempted_at,
            "durable": raw.get("durable") is True,
        }
    return output


def _normalise_reaction_supersessions(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    output: dict[str, str] = {}
    for current, superseded in tuple(value.items())[:500]:
        current_hash = _normalise_digest(current)
        superseded_hash = _normalise_digest(superseded)
        if current_hash is not None and superseded_hash is not None:
            output[current_hash] = superseded_hash
    return output


def _normalise_calendar_reaction(
    value: object,
    *,
    event_id: str,
    event_hash: str | None,
    official: bool,
    scheduled_at: object = None,
    observed_at: object = None,
) -> dict[str, object]:
    if not official:
        return _calendar_reaction_unsupported(
            event_id=event_id,
            event_hash=event_hash,
        )
    if event_hash is None:
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=None,
            reason="REACTION_EVENT_IDENTITY_UNAVAILABLE",
        )
    if not isinstance(value, Mapping):
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason="REACTION_PROVIDER_UNAVAILABLE",
        )

    status = (_clean_text(value.get("status"), 24) or "UNAVAILABLE").upper()
    supplied_event_id = _normalise_reaction_identifier(value.get("event_id"), 128)
    supplied_event_hash = _normalise_digest(value.get("event_hash"))
    if supplied_event_id != event_id or supplied_event_hash != event_hash:
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason="REACTION_API_BINDING_MISMATCH",
            status="CONFLICTED",
        )

    supplied_reasons = _normalise_reaction_reasons(value.get("reasons"))
    if supplied_reasons == ["REACTION_EVENT_UNSUPPORTED"]:
        return _calendar_reaction_unsupported(
            event_id=event_id,
            event_hash=event_hash,
        )

    if status in _REACTION_FAILURE_STATUSES:
        coverage = _normalise_reaction_coverage(value.get("coverage"))
        document_progression = _normalise_document_progression(
            value.get("document_progression")
        )
        if _is_valid_declared_release_wait(
            value,
            coverage=coverage,
            document_progression=document_progression,
            scheduled_at=scheduled_at,
            observed_at=observed_at,
        ):
            result = _calendar_reaction_failure(
                event_id=event_id,
                event_hash=event_hash,
                reason="WAIT_FOR_DECLARED_RELEASE_TIME",
                status=status,
                reasons=["WAIT_FOR_DECLARED_RELEASE_TIME"],
                coverage=coverage,
            )
            result["decision"] = "OBSERVATION_ONLY"
            result["document_progression"] = document_progression
            result["numeric_surprise"] = {
                "status": "UNAVAILABLE",
                "reason": "NUMERIC_SURPRISE_UNSUPPORTED",
                "decision_authority": "SUPPORTING_ONLY",
            }
            return result
        reasons = supplied_reasons
        result = _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason=reasons[0] if reasons else "REACTION_API_READ_MODEL_INVALID",
            status=status,
            reasons=reasons,
            coverage=coverage,
        )
        if document_progression is not None:
            result["document_progression"] = document_progression
            result["numeric_surprise"] = {
                "status": "UNAVAILABLE",
                "reason": "NUMERIC_SURPRISE_UNSUPPORTED",
                "decision_authority": "SUPPORTING_ONLY",
            }
            document_market = _normalise_reaction_market(
                value.get("market_reaction")
            )
            if document_market is not None:
                result["market_reaction"] = document_market
                result["analysis_available"] = True
        return result
    if status != "READY":
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason="REACTION_API_READ_MODEL_INVALID",
        )

    stage = (_clean_text(value.get("current_stage"), 40) or "").upper()
    decision = (_clean_text(value.get("decision"), 32) or "NO_TRADE").upper()
    asof = _normalise_reaction_timestamp(value.get("asof"))
    head_hash = _normalise_digest(value.get("head_hash"))
    transition_count = _normalise_positive_integer(value.get("transition_count"))
    expectation = _normalise_reaction_expectation(value.get("expectation"))
    if (
        stage not in _REACTION_NORMAL_STAGES
        or decision != "OBSERVATION_ONLY"
        or asof is None
        or head_hash is None
        or transition_count is None
        or transition_count != _REACTION_NORMAL_STAGES.index(stage) + 1
        or expectation is None
        or _normalise_reaction_reasons(value.get("reasons"))
    ):
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason="REACTION_API_READ_MODEL_INVALID",
        )

    release = _normalise_reaction_release(value.get("release"))
    surprise = _normalise_reaction_surprise(value.get("surprise"))
    market = _normalise_reaction_market(value.get("market_reaction"))
    option = _normalise_reaction_option(value.get("option_reevaluation"))
    stage_index = _REACTION_NORMAL_STAGES.index(stage)
    evidence = (release, surprise, market, option)
    required = (stage_index >= 2, stage_index >= 3, stage_index >= 4, stage_index >= 5)
    if any((item is None) == needed for item, needed in zip(evidence, required)):
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason="REACTION_API_READ_MODEL_INVALID",
        )
    if not _reaction_bindings_match(expectation, release, surprise, market, option):
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason="REACTION_API_BINDING_MISMATCH",
            status="CONFLICTED",
        )
    evidence_times = [
        expectation["observed_at"],
        None if release is None else release["captured_at"],
        None if surprise is None else surprise["assessed_at"],
        None if market is None else market["observed_at"],
        None if option is None else option["observed_at"],
    ]
    if any(item is not None and str(item) > asof for item in evidence_times):
        return _calendar_reaction_failure(
            event_id=event_id,
            event_hash=event_hash,
            reason="REACTION_API_READ_MODEL_INVALID",
        )
    return {
        "status": "READY",
        "current_stage": stage,
        "analysis_available": stage_index >= 3,
        "event_id": event_id,
        "event_hash": event_hash,
        "asof": asof,
        "head_hash": head_hash,
        "transition_count": transition_count,
        "expectation": expectation,
        "release": release,
        "surprise": surprise,
        "market_reaction": market,
        "option_reevaluation": option,
        "decision": "OBSERVATION_ONLY",
        "reasons": [],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _normalise_measure_reactions(
    value: object,
    *,
    official: bool,
) -> list[dict[str, object]]:
    """Sanitize every child ledger independently; never trust the parent batch."""

    if not official or not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    output: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in tuple(value)[:128]:
        if not isinstance(raw, Mapping):
            continue
        reaction_id = _normalise_reaction_identifier(raw.get("reaction_id"), 256)
        event_id = _normalise_reaction_identifier(raw.get("event_id"), 256)
        event_hash = _normalise_digest(raw.get("event_hash"))
        if (
            reaction_id is None
            or event_id != reaction_id
            or event_hash is None
            or reaction_id in seen
        ):
            continue
        normalized = _normalise_calendar_reaction(
            raw,
            event_id=event_id,
            event_hash=event_hash,
            official=True,
        )
        if normalized["status"] == "CONFLICTED":
            continue
        normalized["reaction_id"] = reaction_id
        output.append(normalized)
        seen.add(reaction_id)
    return output


def _normalise_revision_views(
    value: object,
    *,
    official: bool,
) -> list[dict[str, object]]:
    if not official or not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return []
    output: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in tuple(value)[:128]:
        if not isinstance(raw, Mapping):
            continue
        reaction_id = _normalise_reaction_identifier(raw.get("reaction_id"), 256)
        initial = _normalise_reaction_release(raw.get("initial_release"))
        revised = (
            None
            if raw.get("revised_release") is None
            else _normalise_reaction_release(raw.get("revised_release"))
        )
        history_raw = raw.get("revision_history")
        history = []
        if isinstance(history_raw, Sequence) and not isinstance(
            history_raw,
            (str, bytes, bytearray),
        ):
            history = [
                item
                for value_item in tuple(history_raw)[:32]
                if (item := _normalise_reaction_release(value_item)) is not None
            ]
        if (
            reaction_id is None
            or reaction_id in seen
            or initial is None
            or (revised is not None and not history)
            or (history and revised != history[-1])
        ):
            continue
        output.append(
            {
                "reaction_id": reaction_id,
                "initial_release": initial,
                "revised_release": revised,
                "revision_history": history,
                "initial_reaction_immutable": True,
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
        seen.add(reaction_id)
    return output


def _calendar_reaction_failure(
    *,
    event_id: str,
    event_hash: str | None,
    reason: str,
    status: str = "UNAVAILABLE",
    reasons: Sequence[str] = (),
    coverage: Mapping[str, object] | None = None,
) -> dict[str, object]:
    safe_status = status if status in _REACTION_FAILURE_STATUSES else "UNAVAILABLE"
    safe_reasons = list(reasons) or [reason]
    result: dict[str, object] = {
        "status": safe_status,
        "current_stage": None,
        "analysis_available": False,
        "event_id": event_id,
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
        "reasons": safe_reasons,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }
    if coverage is not None:
        result["coverage"] = dict(coverage)
    return result


def _normalise_reaction_coverage(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    state = (_clean_text(value.get("support_state"), 32) or "").upper()
    if state not in {"SUPPORTED", "PREFLIGHT_ONLY", "UNSUPPORTED"}:
        return None
    return {
        "family": _clean_text(value.get("family"), 64) or "UNKNOWN",
        "support_state": state,
        "supported": value.get("supported") is True,
        "capture_eligible": value.get("capture_eligible") is True,
        "surprise_eligible": value.get("surprise_eligible") is True,
        "progressed": value.get("progressed") is True,
        "next_action": _normalise_reaction_label(value.get("next_action"), 96)
        or "WAIT_NEXT_ELIGIBLE_RELEASE",
        "measure_count": _normalise_nonnegative_integer(value.get("measure_count")),
        "capture_count": _normalise_nonnegative_integer(value.get("capture_count")),
        "document_stage": _normalise_reaction_label(value.get("document_stage"), 48)
        or "SCHEDULED",
        "next_eligible_release_at": (
            _clean_text(value.get("next_eligible_release_at"), 64)
            if _clean_text(value.get("next_eligible_release_at"), 64)
            == "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
            else _normalise_reaction_timestamp(value.get("next_eligible_release_at"))
            or "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
        ),
    }


def _is_valid_declared_release_wait(
    value: Mapping[str, object],
    *,
    coverage: Mapping[str, object] | None,
    document_progression: Mapping[str, object] | None,
    scheduled_at: object,
    observed_at: object,
) -> bool:
    """Accept only a supported future release whose ledger is not due yet."""

    scheduled = _normalise_reaction_timestamp(scheduled_at)
    observed = _normalise_reaction_timestamp(observed_at)
    if scheduled is None or observed is None or scheduled <= observed:
        return False
    raw_reasons = value.get("reasons")
    if raw_reasons is None:
        raw_reason_values: tuple[object, ...] = ()
    elif isinstance(raw_reasons, Sequence) and not isinstance(
        raw_reasons,
        (str, bytes, bytearray),
    ):
        raw_reason_values = tuple(raw_reasons)
    else:
        return False
    wait_reasons = {
        "WAIT_FOR_DECLARED_RELEASE_TIME",
        "WAITING_DECLARED_RELEASE_TIME",
        "WAITING_NEXT_ELIGIBLE_RELEASE",
    }
    if any(
        (_clean_text(reason, 96) or "").upper() not in wait_reasons
        for reason in raw_reason_values
    ):
        return False
    if coverage is None or document_progression is None:
        return False
    return (
        (_clean_text(value.get("status"), 24) or "").upper() == "UNAVAILABLE"
        and (_clean_text(value.get("decision"), 32) or "").upper()
        == "OBSERVATION_ONLY"
        and value.get("analysis_available") in {None, False}
        and value.get("current_stage") is None
        and value.get("transition_count") in {None, 0}
        and all(
            value.get(field) is None
            for field in (
                "asof",
                "head_hash",
                "expectation",
                "release",
                "surprise",
                "market_reaction",
                "option_reevaluation",
            )
        )
        and coverage.get("support_state") == "SUPPORTED"
        and coverage.get("supported") is True
        and coverage.get("capture_eligible") is False
        and coverage.get("surprise_eligible") is False
        and coverage.get("progressed") is False
        and coverage.get("next_action") == "WAIT_FOR_DECLARED_RELEASE_TIME"
        and coverage.get("document_stage") == "SCHEDULED"
        and coverage.get("capture_count") == 0
        and document_progression.get("status") == "SCHEDULED"
        and document_progression.get("capture_count") == 0
        and document_progression.get("next_action")
        == "WAIT_FOR_DECLARED_RELEASE_TIME"
    )


def _normalise_document_progression(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    status = _normalise_reaction_label(value.get("status"), 48)
    next_action = _normalise_reaction_label(value.get("next_action"), 96)
    if status not in {
        "SCHEDULED",
        "WAITING_RELEASE_DOCUMENT",
        "DOCUMENT_CAPTURED",
    } or next_action is None:
        return None
    return {
        "status": status,
        "capture_count": _normalise_nonnegative_integer(value.get("capture_count")),
        "next_action": next_action,
        "decision_authority": "SUPPORTING_ONLY",
    }


def _calendar_reaction_unsupported(
    *,
    event_id: str,
    event_hash: str | None,
) -> dict[str, object]:
    result = _calendar_reaction_failure(
        event_id=event_id,
        event_hash=event_hash,
        reason="REACTION_EVENT_UNSUPPORTED",
    )
    result["decision"] = "OBSERVATION_ONLY"
    return result


def _normalise_reaction_expectation(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    result: dict[str, object] = {
        "content_hash": _normalise_digest(value.get("content_hash")),
        "metric": _normalise_reaction_label(value.get("metric"), 120),
        "expected_value": _normalise_decimal_text(value.get("expected_value")),
        "unit": _normalise_reaction_label(value.get("unit"), 48),
        "observed_at": _normalise_reaction_timestamp(value.get("observed_at")),
        "vintage": _normalise_reaction_label(value.get("vintage"), 96),
        "decision_authority": "SUPPORTING_ONLY",
    }
    return result if all(result[name] is not None for name in result if name != "decision_authority") else None


def _normalise_reaction_release(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    hashes = _normalise_digest_list(value.get("release_chain_hashes"), maximum=32)
    result: dict[str, object] = {
        "content_hash": _normalise_digest(value.get("content_hash")),
        "actual_value": _normalise_decimal_text(value.get("actual_value")),
        "unit": _normalise_reaction_label(value.get("unit"), 48),
        "released_at": _normalise_reaction_timestamp(value.get("released_at")),
        "vintage_at": _normalise_reaction_timestamp(value.get("vintage_at")),
        "captured_at": _normalise_reaction_timestamp(value.get("captured_at")),
        "revision": _normalise_nonnegative_integer_or_none(value.get("revision")),
        "supersedes_hash": (
            None
            if value.get("supersedes_hash") is None
            else _normalise_digest(value.get("supersedes_hash"))
        ),
        "release_chain_hashes": hashes,
        "decision_authority": "SUPPORTING_ONLY",
    }
    required = (
        "content_hash",
        "actual_value",
        "unit",
        "released_at",
        "vintage_at",
        "captured_at",
        "revision",
    )
    if any(result[name] is None for name in required) or not hashes:
        return None
    if hashes[-1] != result["content_hash"]:
        return None
    revision = result["revision"]
    supersedes_hash = result["supersedes_hash"]
    if (revision == 0 and supersedes_hash is not None) or (
        isinstance(revision, int) and revision > 0 and supersedes_hash is None
    ):
        return None
    if not (
        str(result["released_at"])
        <= str(result["vintage_at"])
        <= str(result["captured_at"])
    ):
        return None
    return result


def _normalise_reaction_surprise(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    result: dict[str, object] = {
        "content_hash": _normalise_digest(value.get("content_hash")),
        "expectation_hash": _normalise_digest(value.get("expectation_hash")),
        "release_hash": _normalise_digest(value.get("release_hash")),
        "delta": _normalise_decimal_text(value.get("delta")),
        "relative_delta": (
            None
            if value.get("relative_delta") is None
            else _normalise_decimal_text(value.get("relative_delta"))
        ),
        "assessed_at": _normalise_reaction_timestamp(value.get("assessed_at")),
        "supporting_evidence_hashes": _normalise_digest_list(
            value.get("supporting_evidence_hashes"), maximum=32
        ),
        "decision_authority": "SUPPORTING_ONLY",
    }
    required = ("content_hash", "expectation_hash", "release_hash", "delta", "assessed_at")
    return result if all(result[name] is not None for name in required) else None


def _normalise_reaction_market(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    result: dict[str, object] = {
        "content_hash": _normalise_digest(value.get("content_hash")),
        "release_hash": _normalise_digest(value.get("release_hash")),
        "window_start": _normalise_reaction_timestamp(value.get("window_start")),
        "window_end": _normalise_reaction_timestamp(value.get("window_end")),
        "evidence_asof": _normalise_reaction_timestamp(value.get("evidence_asof")),
        "observed_at": _normalise_reaction_timestamp(value.get("observed_at")),
        "decision_authority": "SUPPORTING_ONLY",
    }
    if any(result[name] is None for name in result if name != "decision_authority"):
        return None
    if not (
        str(result["window_start"])
        < str(result["window_end"])
        <= str(result["evidence_asof"])
        <= str(result["observed_at"])
    ):
        return None
    return result


def _normalise_reaction_option(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    hashes = _normalise_digest_list(value.get("input_evidence_hashes"), maximum=64)
    result: dict[str, object] = {
        "content_hash": _normalise_digest(value.get("content_hash")),
        "market_reaction_hash": _normalise_digest(value.get("market_reaction_hash")),
        "option_id": _normalise_reaction_identifier(value.get("option_id"), 160),
        "candidate_hash": _normalise_digest(value.get("candidate_hash")),
        "evidence_asof": _normalise_reaction_timestamp(value.get("evidence_asof")),
        "observed_at": _normalise_reaction_timestamp(value.get("observed_at")),
        "input_evidence_hashes": hashes,
        "decision_authority": "SUPPORTING_ONLY",
    }
    required = (
        "content_hash",
        "market_reaction_hash",
        "option_id",
        "candidate_hash",
        "evidence_asof",
        "observed_at",
    )
    if any(result[name] is None for name in required) or not hashes:
        return None
    if str(result["evidence_asof"]) > str(result["observed_at"]):
        return None
    return result


def _reaction_bindings_match(
    expectation: Mapping[str, object],
    release: Mapping[str, object] | None,
    surprise: Mapping[str, object] | None,
    market: Mapping[str, object] | None,
    option: Mapping[str, object] | None,
) -> bool:
    if surprise is not None and release is not None:
        expected_value = _decimal_value(expectation.get("expected_value"))
        actual_value = _decimal_value(release.get("actual_value"))
        delta = _decimal_value(surprise.get("delta"))
        if (
            expected_value is None
            or actual_value is None
            or delta is None
            or actual_value - expected_value != delta
        ):
            return False
    if surprise is not None and (
        release is None
        or surprise.get("expectation_hash") != expectation.get("content_hash")
        or surprise.get("release_hash") != release.get("content_hash")
    ):
        return False
    if market is not None and (
        release is None or market.get("release_hash") != release.get("content_hash")
    ):
        return False
    if option is not None and (
        market is None
        or option.get("market_reaction_hash") != market.get("content_hash")
    ):
        return False
    return True


def _normalise_reaction_timestamp(value: object) -> str | None:
    text = _clean_text(value, 64)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat()


def _normalise_reaction_label(value: object, maximum: int) -> str | None:
    text = _clean_text(value, maximum)
    if text is None or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._:/+%()\-]{0,159}", text) is None:
        return None
    return text


def _normalise_reaction_identifier(value: object, maximum: int) -> str | None:
    text = _clean_text(value, maximum)
    if text is None or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/\-]{0,159}", text) is None:
        return None
    return text


def _normalise_reaction_event_ids(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    result: list[str] = []
    for raw in tuple(value)[:500]:
        event_id = _normalise_reaction_identifier(raw, 128)
        if event_id is not None and event_id not in result:
            result.append(event_id)
    return result


def _normalise_digest_list(value: object, *, maximum: int) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    result: list[str] = []
    for item in value:
        digest = _normalise_digest(item)
        if digest is None or digest in result:
            return []
        result.append(digest)
        if len(result) == maximum:
            break
    return result


def _normalise_reaction_reason(value: object) -> str | None:
    reason = (_clean_text(value, 96) or "").upper()
    return reason if reason in _REACTION_REASON_CODES else None


def _normalise_reaction_reasons(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    result: list[str] = []
    for item in value:
        reason = _normalise_reaction_reason(item)
        if reason is not None and reason not in result:
            result.append(reason)
        if len(result) == 8:
            break
    return result


def _normalise_calendar_sources(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        status = (_clean_text(item.get("status"), 24) or "DEGRADED").upper()
        if status not in {"READY", "DEGRADED"}:
            status = "DEGRADED"
        row: dict[str, object] = {
            "source": _clean_text(item.get("source"), 120),
            "status": status,
            "reason": _clean_text(item.get("reason"), 160),
            "observed_at": _normalise_timestamp(item.get("observed_at")),
            "content_hash": _normalise_digest(item.get("content_hash")),
            "decision_authority": "SUPPORTING_ONLY",
        }
        # Audited upstream duplicate handling is useful operator evidence, but
        # only a deliberately small allowlist crosses the public API.  Raw
        # payloads, headers, and record bodies remain private to the provider.
        source_url = _normalise_public_url(item.get("source_url"))
        if source_url is not None:
            row["source_url"] = source_url
        if "event_count" in item:
            row["event_count"] = _normalise_nonnegative_integer(item.get("event_count"))
        if "duplicate_count" in item:
            row["duplicate_count"] = _normalise_nonnegative_integer(
                item.get("duplicate_count")
            )
        if "warnings" in item:
            row["warnings"] = _normalise_text_list(item.get("warnings"), maximum=8)
        audit_hash = _normalise_digest(item.get("audit_hash"))
        if audit_hash is not None:
            row["audit_hash"] = audit_hash
        rows.append(row)
        if len(rows) == 16:
            break
    return rows


def _normalise_calendar_windows(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    allowed = {"THIS_WEEK", "NEXT_WEEK", "FUTURE_TWO_WEEKS"}
    result: list[str] = []
    for item in value:
        window = (_clean_text(item, 32) or "").upper()
        if window in allowed and window not in result:
            result.append(window)
    return result


def _normalise_learning_summary(
    raw: Mapping[str, object],
) -> dict[str, object]:
    """Build an explicit public learning projection with no private passthrough."""

    payload: dict[str, object] = {
        "champion": _normalise_learning_model_descriptor(raw.get("champion")),
        "challenger": _normalise_learning_model_descriptor(raw.get("challenger")),
        "stage": _normalise_learning_stage(raw.get("stage")),
        "decision_records": _normalise_nonnegative_integer(raw.get("decision_records")),
        "minimum_discovery_scenarios": _normalise_nonnegative_integer(
            raw.get("minimum_discovery_scenarios")
        ),
        "message": _clean_text(raw.get("message"), 500),
        "gate_status": "SHADOW_ONLY",
        "shadow_learning": _normalise_learning_shadow_summary(
            raw.get("shadow_learning")
        ),
    }
    models = _normalise_learning_models(raw.get("models"))
    if models:
        payload["models"] = models
    payload["governance"] = _normalise_learning_governance(
        raw.get("governance")
    )
    payload["outcome_processing"] = _normalise_outcome_processing(
        raw.get("outcome_processing")
    )
    payload["outcome_capture"] = _normalise_outcome_capture(
        raw.get("outcome_capture")
    )
    payload["outcome_horizons"] = _normalise_outcome_horizon_summary(
        raw.get("outcome_horizons")
    )
    # No server-owned creator transport adapter is composed in this release.
    # A provider claim cannot turn a display field into transport capability.
    payload["creator_transport_status"] = "CREATOR_TRANSPORT_UNAVAILABLE"
    payload.update(
        {
            "read_only": True,
            "decision_authority": "OBSERVATION_ONLY",
            "automatic_production_promotion": False,
            "can_auto_promote": False,
            "production_weights_mutable": False,
            "production_rules_mutable": False,
            "a_grade_unlocked": False,
            "approval_authority": False,
            "bridge_authority": False,
            "order_authority": False,
        }
    )
    assert_no_secret_like(payload)
    return payload


def _normalise_learning_read_model(
    raw: Mapping[str, object],
) -> dict[str, object]:
    """Sanitize immutable record/query views and force observation-only authority."""

    sanitised = _sanitise_learning_provider_value(raw)
    payload = dict(sanitised) if isinstance(sanitised, Mapping) else {}
    payload.update(
        {
            "read_only": True,
            "decision_authority": "OBSERVATION_ONLY",
            "automatic_production_promotion": False,
            "can_auto_promote": False,
            "production_weights_mutable": False,
            "production_rules_mutable": False,
            "a_grade_unlocked": False,
            "approval_authority": False,
            "bridge_authority": False,
            "order_authority": False,
        }
    )
    assert_no_secret_like(payload)
    return payload


def _normalise_learning_model_descriptor(value: object) -> object:
    if isinstance(value, str):
        return _clean_text(value, 160)
    if not isinstance(value, Mapping):
        return None
    result: dict[str, object] = {}
    for field, maximum in (
        ("name", 160),
        ("version", 160),
        ("status", 64),
        ("stage", 64),
        ("grade", 64),
        ("description", 500),
    ):
        text = _clean_text(value.get(field), maximum)
        if text is not None:
            result[field] = text
    for field in ("selected", "active", "shadow_only"):
        if isinstance(value.get(field), bool):
            result[field] = value[field]
    for field in ("independent_samples", "sample_count"):
        if field in value:
            result[field] = _normalise_nonnegative_integer(value.get(field))
    return result


def _normalise_learning_models(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    return {
        key: _normalise_learning_model_descriptor(value.get(key))
        for key in ("champion", "challenger")
        if key in value
    }


def _normalise_learning_stage(value: object) -> str:
    stage = (_clean_text(value, 64) or "COLLECTING").upper()
    return (
        stage
        if stage in {"COLLECTING", "COMPARISON_AVAILABLE", "DISCOVERY"}
        else "COLLECTING"
    )


def _normalise_learning_shadow_summary(value: object) -> dict[str, object]:
    raw = _learning_mapping(value)
    counts_raw = _learning_mapping(raw.get("record_counts"))
    record_counts = {
        kind: _normalise_nonnegative_integer(counts_raw.get(kind))
        for kind in ("THESIS", "EVIDENCE", "PREDICTION", "OUTCOME")
    }
    challengers = _normalise_text_list(raw.get("challengers"), maximum=32)
    ledger_raw = _learning_mapping(raw.get("ledger"))
    schema_version = ledger_raw.get("schema_version")
    journal_mode = _clean_text(ledger_raw.get("journal_mode"), 32)
    ledger = {
        "schema_version": (
            schema_version
            if isinstance(schema_version, int) and not isinstance(schema_version, bool)
            else None
        ),
        "journal_mode": journal_mode,
        "integrity_verified": ledger_raw.get("integrity_verified") is True,
    }
    contract_raw = _learning_mapping(raw.get("prediction_contract"))
    exclusion_raw = _learning_mapping(contract_raw.get("exclusion_reasons"))
    prediction_contract = {
        "challenger": _clean_text(contract_raw.get("challenger"), 160),
        "legacy_excluded_count": _normalise_nonnegative_integer(
            contract_raw.get("legacy_excluded_count")
        ),
        "exclusion_reasons": {
            str(key)[:120]: _normalise_nonnegative_integer(value)
            for key, value in exclusion_raw.items()
            if isinstance(key, str)
        },
        "decision_authority": "SUPPORTING_ONLY",
    }
    return {
        "status": (_clean_text(raw.get("status"), 64) or "UNAVAILABLE").upper(),
        "mode": "SHADOW_ONLY",
        "stage": _normalise_learning_stage(raw.get("stage")),
        "grade": _clean_text(raw.get("grade"), 64),
        "independent_samples": _normalise_nonnegative_integer(
            raw.get("independent_samples")
        ),
        "minimum_discovery_scenarios": _normalise_nonnegative_integer(
            raw.get("minimum_discovery_scenarios")
        ),
        "discovery_ready": raw.get("discovery_ready") is True,
        "selected_challenger": _clean_text(raw.get("selected_challenger"), 160),
        "challengers": challengers,
        "record_counts": record_counts,
        "record_count": _normalise_nonnegative_integer(raw.get("record_count")),
        "ledger": ledger,
        "prediction_contract": prediction_contract,
        "authority": {
            "can_auto_promote": False,
            "can_change_production_weights": False,
            "can_change_production_rules": False,
            "a_grade_15_percent_unlocked": False,
            "approval_authority": False,
            "bridge_authority": False,
            "order_authority": False,
            "external_human_approval_required": True,
            "promotion_requires_external_human_approval": True,
            "a_grade_requires_external_human_approval": True,
        },
        "queries": {
            "records": "/api/learning/records",
            "record": "/api/learning/records/{record_id}",
            "replay": "/api/learning/predictions/{prediction_id}/replay",
            "similar": "/api/learning/predictions/{prediction_id}/similar",
        },
    }


def _normalise_learning_governance(value: object) -> dict[str, object]:
    """Project a stable, read-only P9 governance view from verified runtime data.

    This is intentionally an allowlist projection.  In particular, signatures,
    public-key material, verifier objects, and raw authority documents never
    cross the HTTP boundary.  The API does not verify or create authorities;
    the current runtime has no trusted production verifier, so it never
    renders a positive promotion, rollback, or A-grade production state.
    """

    raw = dict(value) if isinstance(value, Mapping) else {}
    root_test_only = _learning_governance_is_test_only(raw)

    # The current composition has no server-owned production governance
    # verifier.  Provider strings, booleans, hashes, and callbacks are display
    # inputs only and can never manufacture this capability.  The future seam
    # is a separately typed, server-side verifier dependency on the service
    # composition -- never another field in this provider mapping.
    human_signer_status = _NO_TRUSTED_HUMAN_SIGNER

    current_policy = _normalise_learning_current_policy(
        raw.get("current_policy"), root_test_only=root_test_only
    )
    evaluation = _normalise_learning_evaluation(
        raw.get("evaluation"), root_test_only=root_test_only
    )
    promotion = _normalise_learning_promotion(
        raw.get("promotion"),
        root_test_only=root_test_only,
    )
    rollback = _normalise_learning_rollback(
        raw.get("rollback"),
        root_test_only=root_test_only,
    )
    a_grade = _normalise_learning_a_grade(
        raw.get("a_grade"),
        root_test_only=root_test_only,
    )
    risk = _normalise_learning_risk(
        raw.get("risk"), root_test_only=root_test_only
    )

    if root_test_only:
        status, reason = "BLOCKED", "TEST_ONLY_AUTHORITY"
    else:
        status, reason = "BLOCKED", _NO_TRUSTED_HUMAN_SIGNER

    return {
        "schema": _LEARNING_GOVERNANCE_SCHEMA,
        "status": status,
        "reason": reason,
        "current_policy": current_policy,
        "evaluation": evaluation,
        "promotion": promotion,
        "rollback": rollback,
        "a_grade": a_grade,
        "authority": {
            "human_signer_status": human_signer_status,
            "read_only": True,
            "can_sign": False,
            "can_auto_promote": False,
            "approval_authority": False,
            "bridge_authority": False,
            "order_authority": False,
        },
        "risk": risk,
    }


def _normalise_learning_current_policy(
    value: object,
    *,
    root_test_only: bool,
) -> dict[str, object]:
    raw = _learning_mapping(value)
    version = _clean_text(
        raw.get("version", raw.get("current_policy_version")), 160
    )
    policy_hash = _normalise_digest(
        raw.get("hash", raw.get("current_policy_hash"))
    )
    marker_hash = _normalise_digest(
        raw.get("authority_marker_hash", raw.get("policy_authority_marker_hash"))
    )
    head_hash = _normalise_digest(raw.get("authority_head_hash"))
    initial_hash = _normalise_digest(
        raw.get(
            "immutable_initial_policy_hash",
            raw.get("initial_policy_hash", raw.get("initial_policy_source_hash")),
        )
    )
    test_only = root_test_only or _learning_governance_is_test_only(raw)
    complete = all((version, policy_hash, marker_hash, head_hash, initial_hash))
    claimed_status = (_clean_text(raw.get("status"), 32) or "").upper()
    if test_only:
        status, reason = "TEST_ONLY", "TEST_ONLY_AUTHORITY"
    elif complete and claimed_status in {"VERIFIED", "AVAILABLE", "CURRENT"}:
        status, reason = "VERIFIED", None
    else:
        status, reason = "BLOCKED", "CURRENT_POLICY_BINDING_INCOMPLETE"
    return {
        "status": status,
        "reason": reason,
        "version": version,
        "hash": policy_hash,
        "authority_marker_hash": marker_hash,
        "authority_head_hash": head_hash,
        "immutable_initial_policy_hash": initial_hash,
    }


def _normalise_learning_evaluation(
    value: object,
    *,
    root_test_only: bool,
) -> dict[str, object]:
    raw = _learning_mapping(value)
    report_hash = _normalise_digest(
        raw.get("report_hash", raw.get("evaluation_report_hash"))
    )
    dataset_hash = _normalise_digest(
        raw.get("dataset_hash", raw.get("reference_dataset_hash"))
    )
    independence_hash = _normalise_digest(
        raw.get("independence_spec_hash", raw.get("independence_hash"))
    )
    independent_count = _normalise_nonnegative_integer_or_none(
        raw.get("independent_count", raw.get("independent_samples"))
    )
    stage = (_clean_text(raw.get("stage"), 32) or "").upper()
    if stage not in {"COLLECTING", "COMPARISON_AVAILABLE", "DISCOVERY"}:
        stage = None
    claimed_status = (_clean_text(raw.get("status"), 32) or "").upper()
    complete = (
        report_hash is not None
        and dataset_hash is not None
        and independence_hash is not None
        and independent_count is not None
        and stage is not None
    )
    test_only = root_test_only or _learning_governance_is_test_only(raw)
    if test_only:
        status, reason = "TEST_ONLY", "TEST_ONLY_EVALUATION"
    elif complete and claimed_status in {"AVAILABLE", "VERIFIED"}:
        status, reason = "AVAILABLE", None
    elif complete and claimed_status == "COLLECTING":
        status = "COLLECTING"
        raw_reason = (_clean_text(raw.get("reason"), 96) or "").upper()
        reason = (
            raw_reason
            if raw_reason in {
                "ZERO_INDEPENDENT_SAMPLES",
                "CHAMPION_BASELINE_UNAVAILABLE",
            }
            else "EVALUATION_COLLECTING"
        )
    else:
        status, reason = "UNAVAILABLE", "EVALUATION_BINDING_INCOMPLETE"
    return {
        "status": status,
        "reason": reason,
        "report_hash": report_hash,
        "dataset_hash": dataset_hash,
        "independence_spec_hash": independence_hash,
        "independent_count": independent_count,
        "stage": stage,
        "comparison_complete": raw.get("comparison_complete") is True,
        "champion_accuracy": _normalise_learning_fraction(
            raw.get("champion_accuracy")
        ),
        "challenger_accuracy": _normalise_learning_fraction(
            raw.get("challenger_accuracy")
        ),
        "challenger_accuracy_delta": _normalise_decimal_text(
            raw.get("challenger_accuracy_delta")
        ),
        "champion_brier_score": _normalise_learning_fraction(
            raw.get("champion_brier_score")
        ),
        "challenger_brier_score": _normalise_learning_fraction(
            raw.get("challenger_brier_score")
        ),
        "challenger_brier_improvement": _normalise_decimal_text(
            raw.get("challenger_brier_improvement")
        ),
    }


def _normalise_learning_promotion(
    value: object,
    *,
    root_test_only: bool,
) -> dict[str, object]:
    raw = _learning_mapping(value)
    authority_hash = _normalise_digest(
        raw.get("authority_hash", raw.get("promotion_authority_hash"))
    )
    test_only = root_test_only or _learning_governance_is_test_only(raw)
    if test_only:
        status, reason = "BLOCKED", "TEST_ONLY_AUTHORITY"
    else:
        status, reason = "BLOCKED", _NO_TRUSTED_HUMAN_SIGNER
    return {
        "status": status,
        "reason": reason,
        "authority_hash": authority_hash,
    }


def _normalise_learning_rollback(
    value: object,
    *,
    root_test_only: bool,
) -> dict[str, object]:
    raw = _learning_mapping(value)
    authority_hash = _normalise_digest(
        raw.get("authority_hash", raw.get("rollback_authority_hash"))
    )
    target_policy_hash = _normalise_digest(raw.get("target_policy_hash"))
    test_only = root_test_only or _learning_governance_is_test_only(raw)
    if test_only:
        status, reason = "BLOCKED", "TEST_ONLY_AUTHORITY"
    else:
        status, reason = "BLOCKED", _NO_TRUSTED_HUMAN_SIGNER
    return {
        "status": status,
        "reason": reason,
        "authority_hash": authority_hash,
        "target_policy_hash": target_policy_hash,
    }


def _normalise_learning_a_grade(
    value: object,
    *,
    root_test_only: bool,
) -> dict[str, object]:
    raw = _learning_mapping(value)
    marker_hash = _normalise_digest(
        raw.get("marker_hash", raw.get("a_grade_marker_hash"))
    )
    proposal_id = _clean_text(raw.get("proposal_id"), 256)
    proposal_hash = _normalise_digest(raw.get("proposal_hash"))
    candidate_hash = _normalise_digest(raw.get("candidate_hash"))
    ranking_basis_hash = _normalise_digest(raw.get("ranking_basis_hash"))
    current_policy_version = _clean_text(raw.get("current_policy_version"), 160)
    current_policy_hash = _normalise_digest(raw.get("current_policy_hash"))
    policy_marker_hash = _normalise_digest(raw.get("policy_authority_marker_hash"))
    execution_cost_version = _clean_text(raw.get("execution_cost_version"), 160)
    execution_cost_hash = _normalise_digest(raw.get("execution_cost_hash"))
    evaluation_report_hash = _normalise_digest(raw.get("evaluation_report_hash"))
    dataset_hash = _normalise_digest(
        raw.get("dataset_hash", raw.get("reference_dataset_hash"))
    )
    independence_hash = _normalise_digest(
        raw.get("independence_spec_hash", raw.get("independence_hash"))
    )
    risk_contract_hash = _normalise_digest(raw.get("risk_contract_hash"))
    max_risk_fraction = _normalise_learning_fraction(raw.get("max_risk_fraction"))
    test_only = root_test_only or _learning_governance_is_test_only(raw)
    if test_only:
        status, reason = "BLOCKED", "TEST_ONLY_AUTHORITY"
    else:
        status, reason = "BLOCKED", _NO_TRUSTED_HUMAN_SIGNER
    return {
        "status": status,
        "reason": reason,
        "marker_hash": marker_hash,
        "proposal_id": proposal_id,
        "proposal_hash": proposal_hash,
        "candidate_hash": candidate_hash,
        "ranking_basis_hash": ranking_basis_hash,
        "current_policy_version": current_policy_version,
        "current_policy_hash": current_policy_hash,
        "policy_authority_marker_hash": policy_marker_hash,
        "execution_cost_version": execution_cost_version,
        "execution_cost_hash": execution_cost_hash,
        "evaluation_report_hash": evaluation_report_hash,
        "dataset_hash": dataset_hash,
        "independence_spec_hash": independence_hash,
        "risk_contract_hash": risk_contract_hash,
        "max_risk_fraction": max_risk_fraction,
    }


def _normalise_learning_risk(
    value: object,
    *,
    root_test_only: bool,
) -> dict[str, object]:
    raw = _learning_mapping(value)
    test_only = root_test_only or _learning_governance_is_test_only(raw)
    return {
        # These are safety invariants, not provider-configurable display values.
        "normal_max_fraction": "0.10",
        "a_grade_max_fraction": "0.15",
        "absolute_reject_fraction": "0.20",
        "authority_version": (
            None if test_only else _clean_text(raw.get("authority_version"), 160)
        ),
        "authority_marker_hash": (
            None if test_only else _normalise_digest(raw.get("authority_marker_hash"))
        ),
    }


def _normalise_learning_fraction(value: object) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not result.is_finite() or result < 0 or result > 1:
        return None
    return format(result.normalize(), "f")


def _learning_mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sanitise_learning_provider_value(value: object) -> object:
    """Recursively drop private authority and credential-like provider data."""

    if isinstance(value, Mapping):
        result: dict[object, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                continue
            name = str(key)
            normalized = re.sub(r"[^a-z0-9]", "", name.lower())
            if (
                name in _PRIVATE_LEARNING_AUTHORITY_FIELDS
                or any(part in normalized for part in _PRIVATE_LEARNING_FIELD_PARTS)
            ):
                continue
            safe = _sanitise_learning_provider_value(item)
            if safe is not _DROP_LEARNING_VALUE:
                result[key] = safe
        return result
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        result = []
        for item in value:
            safe = _sanitise_learning_provider_value(item)
            if safe is not _DROP_LEARNING_VALUE:
                result.append(safe)
        return result
    try:
        assert_no_secret_like(value)
    except SecretLikeFieldError:
        return _DROP_LEARNING_VALUE
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else _DROP_LEARNING_VALUE
    # Dataclasses, verifier objects, ORM rows, byte buffers, and arbitrary
    # provider instances are not JSON primitives.  FastAPI's encoder may walk
    # their attributes, so retaining them would bypass the key allowlist above.
    return _DROP_LEARNING_VALUE


def _learning_governance_is_test_only(value: Mapping[str, object]) -> bool:
    if value.get("test_only") is True:
        return True
    for field in ("scope", "authority_scope", "mode", "source"):
        text = (_clean_text(value.get(field), 64) or "").upper()
        if text in {"TEST", "TEST_ONLY", "DIAGNOSTIC", "FIXTURE"}:
            return True
    return False


def _normalise_learning_record_type(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise HTTPException(status_code=422, detail="record_type must be a string")
    normalized = value.strip().upper()
    if normalized not in {"THESIS", "EVIDENCE", "PREDICTION", "OUTCOME"}:
        raise HTTPException(status_code=422, detail="unsupported learning record_type")
    return normalized


def _normalise_optional_learning_text(
    value: str | None,
    *,
    field: str,
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise HTTPException(status_code=422, detail=f"{field} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > 256:
        raise HTTPException(
            status_code=422,
            detail=f"{field} must contain 1 to 256 characters",
        )
    return normalized


def _normalise_learning_identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}", value
    ) is None:
        raise HTTPException(status_code=422, detail=f"invalid {field}")
    return value


def _normalise_learning_limit(value: int, *, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
        raise HTTPException(
            status_code=422,
            detail=f"limit must be between 1 and {maximum}",
        )
    return value


def _normalise_evidence(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    evidence: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        url = _normalise_public_url(item.get("url", item.get("link")))
        evidence.append(
            {
                "source": _clean_text(item.get("source", item.get("publisher")), 120),
                "title": _clean_text(item.get("title", item.get("headline")), 280),
                "url": url,
                "observed_at": _normalise_timestamp(item.get("observed_at", item.get("asof"))),
            }
        )
        if len(evidence) == 8:
            break
    return evidence


def _normalise_provenance(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "source": _clean_text(item.get("source", item.get("provider")), 120),
                "source_id": _clean_text(item.get("source_id"), 160),
                "source_url": _normalise_public_url(
                    item.get("source_url", item.get("url"))
                ),
                "source_rank": _normalise_nonnegative_integer(item.get("source_rank")),
                "published_at": _normalise_timestamp(item.get("published_at")),
                "first_seen_at": _normalise_timestamp(item.get("first_seen_at")),
                "observed_at": _normalise_timestamp(item.get("observed_at")),
                "content_hash": _normalise_digest(item.get("content_hash")),
                "provenance_hash": _normalise_digest(item.get("provenance_hash")),
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
        if len(rows) == 8:
            break
    return rows


def _normalise_text_list(value: object, *, maximum: int) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    rows: list[str] = []
    for item in value:
        text = _clean_text(item, 480)
        if text:
            rows.append(text)
        if len(rows) == maximum:
            break
    return rows


def _normalise_symbol_binding(
    value: object,
    *,
    symbols: Sequence[str],
) -> dict[str, object]:
    allowed = {
        "SOURCE_DECLARED",
        "VERIFIED_PROVIDER_RELATED",
        "PROVIDER_RELATED_UNVERIFIED",
        "UNBOUND",
        "UNVERIFIED_PROVIDER_BINDING",
        "UNVERIFIED_LEGACY_PROVIDER",
    }
    raw = value if isinstance(value, Mapping) else {}
    status = (_clean_text(raw.get("status"), 48) or "SOURCE_DECLARED").upper()
    if status not in allowed:
        status = "UNVERIFIED_PROVIDER_BINDING"
    adapter = (_clean_text(raw.get("provider_adapter"), 64) or "").upper()
    if adapter and re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", adapter) is None:
        adapter = ""
    if status == "VERIFIED_PROVIDER_RELATED" and (not symbols or not adapter):
        status = "UNVERIFIED_PROVIDER_BINDING"
    return {
        "status": status,
        "provider_adapter": adapter or None,
        "decision_authority": "SUPPORTING_ONLY",
    }


def _normalise_research_proxy_binding(
    value: object,
    *,
    symbols: Sequence[str],
) -> dict[str, object] | None:
    """Expose only a current deterministic macro proxy for unbound news."""

    if symbols or not isinstance(value, Mapping):
        return None
    proxy_symbol = (_clean_text(value.get("proxy_symbol"), 15) or "").upper()
    if re.fullmatch(r"[A-Z][A-Z0-9.\-/]{0,14}", proxy_symbol) is None:
        return None
    try:
        binding = require_current_research_proxy_binding(
            value,
            symbol=proxy_symbol,
        )
    except (TypeError, ValueError):
        return None
    return binding.as_dict() if binding is not None else None


_UNVERIFIED_SYMBOL_BINDING_STATUSES = frozenset(
    {
        "PROVIDER_RELATED_UNVERIFIED",
        "UNVERIFIED_PROVIDER_BINDING",
        "UNVERIFIED_LEGACY_PROVIDER",
    }
)


def _is_unverified_symbol_binding_status(value: object) -> bool:
    status = str(value or "").strip().upper()
    return (
        status in _UNVERIFIED_SYMBOL_BINDING_STATUSES
        or status.startswith("UNVERIFIED")
    )


def _normalise_ibkr_provenance(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    return {
        "source": "IBKR",
        "symbol": (_normalise_symbols(value.get("symbol")) or [None])[0],
        "quote_snapshot_id": _clean_text(value.get("quote_snapshot_id"), 128),
        "observed_at": _normalise_timestamp(value.get("observed_at")),
        "decision_authority": "SUPPORTING_ONLY",
    }


def _normalise_public_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parts = urlsplit(value.strip())
    if (
        parts.scheme not in {"http", "https"}
        or not parts.netloc
        or parts.hostname is None
        or parts.username is not None
        or parts.password is not None
    ):
        return None
    # Userinfo, query strings, and fragments are unnecessary for a public
    # citation and can carry credentials or tracking identifiers.
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


_PRESELECTION_LEDGER_SOURCE = "INDEPENDENT_TOP10_LEDGER"
_SOURCE_BATCH_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


def _normalise_preselection_pools(raw: Mapping[str, Any]) -> dict[str, object]:
    pre_market, pre_market_valid = _normalise_preselection_sequence(
        raw.get("pre_market_preselections"),
        phase="PRE_MARKET",
        maximum=10,
    )
    repriced, repriced_valid = _normalise_preselection_sequence(
        raw.get("open_market_repriced"),
        phase="OPEN_REPRICED",
        maximum=10,
    )
    coverage = _normalise_preselection_coverage(
        raw.get("preselection_coverage"),
        pre_market=pre_market,
        repriced=repriced,
    )
    if not pre_market_valid or not repriced_valid or coverage is None:
        pre_market = []
        repriced = []
        coverage = _empty_preselection_coverage(
            reason="PRESELECTION_LINEAGE_PROJECTION_INVALID",
            ledger_reason="API_FAIL_CLOSED",
        )
        atomic_batch_available = False
        atomic_batch_blocker = "PRESELECTION_LINEAGE_PROJECTION_INVALID"
    else:
        atomic_batch_available, atomic_batch_blocker = (
            _preselection_atomic_batch_validation(
                pre_market,
                repriced,
                coverage=coverage,
            )
        )
        coverage = _finalise_preselection_coverage(
            coverage,
            atomic_batch_available=atomic_batch_available,
            atomic_batch_blocker=atomic_batch_blocker,
            has_pre_market=bool(pre_market),
            has_open_market=bool(repriced),
        )
        if not atomic_batch_available:
            for item in repriced:
                blockers = item.get("blockers")
                safe_blockers = list(blockers) if isinstance(blockers, list) else []
                item["blockers"] = list(
                    dict.fromkeys([*safe_blockers, atomic_batch_blocker])
                )
                item["action_pool_eligible"] = False
                item["research_only"] = True
                item["action_rank"] = None

    action_pool = (
        [item for item in repriced if item.get("action_pool_eligible") is True][
            :3
        ]
        if atomic_batch_available
        else []
    )
    return {
        "pre_market_preselections": pre_market,
        "pre_market_preselection_count": len(pre_market),
        "open_market_repriced": repriced,
        "open_market_repriced_count": len(repriced),
        "option_action_pool": action_pool,
        "option_action_pool_count": len(action_pool),
        "preselection_coverage": coverage,
        "option_approval_eligible": False,
    }


def _normalise_preselection_sequence(
    value: object,
    *,
    phase: Literal["PRE_MARKET", "OPEN_REPRICED"],
    maximum: int,
) -> tuple[list[dict[str, object]], bool]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return [], value is None
    if len(value) > maximum:
        return [], False
    rows: list[dict[str, object]] = []
    identifiers: set[str] = set()
    ranks: set[int] = set()
    action_ranks: set[int] = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            return [], False
        item = _normalise_option_preselection(raw, index=index)
        if item is None:
            return [], False
        if item["phase"] != phase:
            return [], False
        identifier = str(item["preselection_id"])
        rank_name = "research_rank" if phase == "PRE_MARKET" else "repriced_rank"
        rank = item.get(rank_name)
        if (
            identifier in identifiers
            or not isinstance(rank, int)
            or rank in ranks
        ):
            return [], False
        action_rank = item.get("action_rank")
        if phase == "PRE_MARKET" and action_rank is not None:
            return [], False
        if isinstance(action_rank, int):
            if action_rank in action_ranks:
                return [], False
            action_ranks.add(action_rank)
        identifiers.add(identifier)
        ranks.add(rank)
        rows.append(item)
    if ranks != set(range(1, len(rows) + 1)):
        return [], False
    if action_ranks != set(range(1, len(action_ranks) + 1)):
        return [], False
    if any(
        item.get("action_rank") is not None
        and item.get("action_pool_eligible") is not True
        for item in rows
    ):
        return [], False
    return rows, True


def _normalise_option_preselection(
    raw: Mapping[str, Any],
    *,
    index: int,
) -> dict[str, object] | None:
    preselection_id = _normalise_ledger_identifier(raw.get("preselection_id"))
    if preselection_id is None:
        return None
    phase = (_clean_text(raw.get("phase"), 24) or "").upper()
    if phase not in {"PRE_MARKET", "OPEN_REPRICED"}:
        phase = "UNKNOWN"
    legs, legs_valid = _normalise_preselection_legs(raw.get("legs"))
    if not legs_valid:
        return None
    evidence_ids = _normalise_text_list(raw.get("evidence_ids"), maximum=16)
    evidence_hashes = [
        digest
        for value in _normalise_text_list(raw.get("evidence_hashes"), maximum=16)
        if (digest := _normalise_digest(value)) is not None
    ]
    strategy_hash = _normalise_digest(raw.get("strategy_hash"))
    underlying_quote_basis = _normalise_underlying_quote_basis(
        raw.get("underlying_quote_basis")
    )
    underlying_quote_basis_hash = _normalise_digest(
        raw.get("underlying_quote_basis_hash")
    )
    if (
        underlying_quote_basis is None
        or underlying_quote_basis_hash is None
        or canonical_hash(underlying_quote_basis) != underlying_quote_basis_hash
    ):
        underlying_quote_basis = None
        underlying_quote_basis_hash = None
    blockers = _normalise_text_list(raw.get("blockers"), maximum=64)
    maximum_quote_age = _normalise_nonnegative_number(
        raw.get("maximum_quote_age_seconds")
    )
    ledger_lineage = _normalise_preselection_lineage(
        raw.get("ledger_lineage"),
        preselection_id=preselection_id,
        phase=phase,
    )
    if ledger_lineage is None:
        return None
    item: dict[str, object] = {
        "preselection_id": preselection_id,
        "underlying": (_normalise_symbols(raw.get("underlying")) or [None])[0],
        "strategy_type": _clean_text(raw.get("strategy_type"), 80),
        "phase": phase,
        "legs": legs,
        "risk_defined": raw.get("risk_defined") is True,
        "maximum_loss_usd": _normalise_decimal_text(raw.get("maximum_loss_usd")),
        "estimated_cost_usd": _normalise_decimal_text(raw.get("estimated_cost_usd")),
        "cost_after_ev_usd": _normalise_decimal_text(raw.get("cost_after_ev_usd")),
        "terminal_scenarios": _normalise_terminal_scenarios(
            raw.get("terminal_scenarios")
        ),
        "scenario_asof": _normalise_aware_timestamp(raw.get("scenario_asof")),
        "scenario_hash": _normalise_digest(raw.get("scenario_hash")),
        "execution_cost_contract_version": _clean_text(
            raw.get("execution_cost_contract_version"), 80
        ),
        "execution_cost_contract_hash": _normalise_digest(
            raw.get("execution_cost_contract_hash")
        ),
        "risk_policy_version": _clean_text(raw.get("risk_policy_version"), 80),
        "risk_policy_hash": _normalise_digest(raw.get("risk_policy_hash")),
        "broker_snapshot_hash": _normalise_digest(
            raw.get("broker_snapshot_hash")
        ),
        "strategy_nav_usd": _normalise_decimal_text(raw.get("strategy_nav_usd")),
        "strategy_nav_post_hash": _normalise_digest(
            raw.get("strategy_nav_post_hash")
        ),
        "economics_quote_batch_id": _clean_text(
            raw.get("economics_quote_batch_id"), 160
        ),
        "economics_quote_asof": _normalise_aware_timestamp(
            raw.get("economics_quote_asof")
        ),
        "payoff_hash": _normalise_digest(raw.get("payoff_hash")),
        "economics_calculation_hash": _normalise_digest(
            raw.get("economics_calculation_hash")
        ),
        "debit_usd": _normalise_decimal_text(raw.get("debit_usd")),
        "credit_usd": _normalise_decimal_text(raw.get("credit_usd")),
        "net_entry_cost_usd": _normalise_decimal_text(
            raw.get("net_entry_cost_usd")
        ),
        "estimated_commission_usd": _normalise_decimal_text(
            raw.get("estimated_commission_usd")
        ),
        "estimated_entry_slippage_usd": _normalise_decimal_text(
            raw.get("estimated_entry_slippage_usd")
        ),
        "estimated_exit_slippage_usd": _normalise_decimal_text(
            raw.get("estimated_exit_slippage_usd")
        ),
        "estimated_slippage_usd": _normalise_decimal_text(
            raw.get("estimated_slippage_usd")
        ),
        "expected_value_before_costs_usd": _normalise_decimal_text(
            raw.get("expected_value_before_costs_usd")
        ),
        "risk_fraction": _normalise_decimal_text(raw.get("risk_fraction")),
        "risk_adjusted_ev": _normalise_decimal_text(raw.get("risk_adjusted_ev")),
        "entry_condition": _clean_text(raw.get("entry_condition"), 800),
        "invalidation_condition": _clean_text(
            raw.get("invalidation_condition"), 800
        ),
        "profit_target_condition": _clean_text(
            raw.get("profit_target_condition"), 800
        ),
        "stop_loss_condition": _clean_text(raw.get("stop_loss_condition"), 800),
        "evidence_ids": evidence_ids,
        "evidence_hashes": evidence_hashes,
        "strategy_hash": strategy_hash,
        "research_summary": _clean_text(raw.get("research_summary"), 1_200),
        "underlying_quote_basis": underlying_quote_basis,
        "underlying_quote_basis_hash": underlying_quote_basis_hash,
        "quote_batch_id": _clean_text(raw.get("quote_batch_id"), 160),
        "oldest_quote_asof": _normalise_aware_timestamp(
            raw.get("oldest_quote_asof")
        ),
        "maximum_quote_age_seconds": maximum_quote_age,
        "blockers": blockers,
        "research_rank": _normalise_rank(raw.get("research_rank"), maximum=10),
        "repriced_rank": _normalise_rank(raw.get("repriced_rank"), maximum=10),
        "action_rank": _normalise_rank(raw.get("action_rank"), maximum=3),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }
    item["ledger_lineage"] = ledger_lineage
    if not _preselection_structure_hash_matches(item):
        return None
    complete = _api_preselection_action_complete(item)
    action_eligible = (
        raw.get("action_pool_eligible") is True
        and not blockers
        and complete
        and item.get("action_rank") is not None
    )
    if raw.get("action_pool_eligible") is True and not action_eligible:
        item["blockers"] = list(dict.fromkeys([*blockers, "API_FAIL_CLOSED_INCOMPLETE"]))
        item["action_rank"] = None
    item["action_pool_eligible"] = action_eligible
    item["research_only"] = not action_eligible
    # Evidence and structure hashes remain internal ledger material. Quote-batch
    # identifiers are intentionally projected so the GUI can independently
    # verify that every displayed executable leg belongs to one atomic snapshot.
    item.pop("strategy_hash", None)
    item.pop("evidence_hashes", None)
    return item


def _normalise_underlying_quote_basis(
    value: object,
) -> dict[str, object] | None:
    fields = {
        "symbol",
        "contract_id",
        "exchange",
        "source",
        "observed_at",
        "bid",
        "ask",
        "last",
        "close",
        "market_data_type",
        "schema",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        return None
    symbol = (_normalise_symbols(value.get("symbol")) or [None])[0]
    contract_id = value.get("contract_id")
    exchange = (_clean_text(value.get("exchange"), 40) or "").upper()
    source = (_clean_text(value.get("source"), 80) or "").upper()
    observed_at = _normalise_aware_timestamp(value.get("observed_at"))
    market_data_type = value.get("market_data_type")
    if (
        symbol is None
        or isinstance(contract_id, bool)
        or not isinstance(contract_id, int)
        or contract_id <= 0
        or not exchange
        or source != "IBKR_REQ_TICKERS_READONLY"
        or observed_at is None
        or isinstance(market_data_type, bool)
        or market_data_type != 1
        or value.get("schema") != UNDERLYING_QUOTE_BASIS_SCHEMA
    ):
        return None
    prices: dict[str, str | None] = {}
    for name in ("bid", "ask", "last", "close"):
        text = _normalise_decimal_text(value.get(name))
        parsed = None if text is None else _decimal_value(text)
        if parsed is not None and (not parsed.is_finite() or parsed <= 0):
            return None
        prices[name] = text
    if prices["close"] is None:
        return None
    bid = _decimal_value(prices["bid"])
    ask = _decimal_value(prices["ask"])
    if bid is not None and ask is not None and ask < bid:
        return None
    if bid is None and ask is None and prices["last"] is None:
        return None
    return {
        "symbol": symbol,
        "contract_id": contract_id,
        "exchange": exchange,
        "source": source,
        "observed_at": observed_at,
        "bid": prices["bid"],
        "ask": prices["ask"],
        "last": prices["last"],
        "close": prices["close"],
        "market_data_type": market_data_type,
        "schema": UNDERLYING_QUOTE_BASIS_SCHEMA,
    }


def _normalise_terminal_scenarios(value: object) -> list[dict[str, str]]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes, bytearray))
        or not value
        or len(value) > 64
    ):
        return []
    rows: list[dict[str, str]] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            return []
        price = _normalise_decimal_text(raw.get("terminal_underlying_price"))
        probability = _normalise_decimal_text(raw.get("probability"))
        if price is None or probability is None:
            return []
        rows.append(
            {
                "terminal_underlying_price": price,
                "probability": probability,
            }
        )
    return rows


def _normalise_preselection_legs(
    value: object,
) -> tuple[list[dict[str, object]], bool]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return [], False
    if not value or len(value) > 8:
        return [], False
    rows: list[dict[str, object]] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            return [], False
        right = (_clean_text(raw.get("right"), 8) or "").upper()
        side = (_clean_text(raw.get("side"), 8) or "").upper()
        exchange = (
            _clean_contract_identity_text(raw.get("exchange"), 40) or ""
        ).upper()
        expiry = _normalise_option_expiry(raw.get("expiry"))
        strike = _normalise_decimal_text(raw.get("strike"))
        leg = {
            "underlying": (_normalise_symbols(raw.get("underlying")) or [None])[0],
            "con_id": _normalise_positive_integer(raw.get("con_id")),
            "local_symbol": _clean_contract_identity_text(
                raw.get("local_symbol"), 160
            ),
            "trading_class": _clean_contract_identity_text(
                raw.get("trading_class"), 80
            ),
            "multiplier": _normalise_positive_integer(raw.get("multiplier")),
            "exchange": exchange or None,
            "expiry": expiry,
            "strike": strike,
            "right": right if right in {"CALL", "PUT"} else None,
            "side": side if side in {"BUY", "SELL"} else None,
            "ratio": _normalise_positive_integer(raw.get("ratio")),
            "quantity": _normalise_positive_integer(raw.get("quantity")),
            "bid": _normalise_decimal_text(raw.get("bid")),
            "ask": _normalise_decimal_text(raw.get("ask")),
            "quote_asof": _normalise_aware_timestamp(raw.get("quote_asof")),
            "quote_batch_id": _clean_text(raw.get("quote_batch_id"), 160),
            "implied_volatility": _normalise_decimal_text(
                raw.get("implied_volatility")
            ),
            "delta": _normalise_decimal_text(raw.get("delta")),
            "gamma": _normalise_decimal_text(raw.get("gamma")),
            "theta": _normalise_decimal_text(raw.get("theta")),
            "vega": _normalise_decimal_text(raw.get("vega")),
            "volume": _normalise_nonnegative_integer_or_none(raw.get("volume")),
            "open_interest": _normalise_nonnegative_integer_or_none(
                raw.get("open_interest")
            ),
            "dte": _normalise_nonnegative_integer_or_none(raw.get("dte")),
        }
        if (
            leg["underlying"] is None
            or leg["con_id"] is None
            or leg["expiry"] is None
            or _decimal_value(leg["strike"]) is None
            or _decimal_value(leg["strike"]) <= 0
            or leg["right"] is None
            or leg["side"] is None
            or leg["ratio"] is None
            or leg["quantity"] is None
        ):
            return [], False
        rows.append(leg)
    return rows, True


def _normalise_option_expiry(value: object) -> str | None:
    text = _clean_text(value, 16)
    if text is None:
        return None
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    return parsed.isoformat()


def _normalise_aware_timestamp(value: object) -> str | None:
    parsed = _parse_aware_timestamp(value)
    return None if parsed is None else parsed.astimezone(timezone.utc).isoformat()


def _normalise_offset_timestamp(value: object) -> str | None:
    text = _clean_text(value, 64)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.isoformat()


def _parse_aware_timestamp(value: object) -> datetime | None:
    text = _clean_text(value, 64)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _preselection_structure_hash_matches(item: Mapping[str, object]) -> bool:
    expected = _normalise_digest(item.get("strategy_hash"))
    underlying = item.get("underlying")
    strategy_type = item.get("strategy_type")
    legs = item.get("legs")
    if (
        expected is None
        or not isinstance(underlying, str)
        or not isinstance(strategy_type, str)
        or not isinstance(legs, Sequence)
        or isinstance(legs, (str, bytes, bytearray))
        or not legs
    ):
        return False
    v2_identity_fields = (
        "local_symbol",
        "trading_class",
        "multiplier",
        "exchange",
    )
    schema: str | None = None
    structures: list[dict[str, object]] = []
    try:
        for raw in legs:
            if not isinstance(raw, Mapping):
                return False
            identity_present = tuple(
                raw.get(name) is not None for name in v2_identity_fields
            )
            leg_schema = (
                "options_copilot.conditional_option_strategy.v2"
                if all(identity_present)
                else "options_copilot.conditional_option_strategy.v1"
                if not any(identity_present)
                else None
            )
            if leg_schema is None or (schema is not None and leg_schema != schema):
                return False
            schema = leg_schema
            structure = {
                "con_id": int(raw["con_id"]),
                "expiry": date.fromisoformat(str(raw["expiry"])),
                "strike": Decimal(str(raw["strike"])),
                "right": str(raw["right"]),
                "side": str(raw["side"]),
                "ratio": int(raw["ratio"]),
                "quantity": int(raw["quantity"]),
            }
            if schema == "options_copilot.conditional_option_strategy.v1":
                structure = {
                    "underlying": str(raw["underlying"]),
                    **structure,
                }
            else:
                structure = {
                    "con_id": structure["con_id"],
                    "local_symbol": str(raw["local_symbol"]),
                    "trading_class": str(raw["trading_class"]),
                    "multiplier": int(raw["multiplier"]),
                    "exchange": str(raw["exchange"]),
                    **{name: value for name, value in structure.items() if name != "con_id"},
                }
            structures.append(structure)
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return False
    if schema is None:
        return False
    actual = canonical_hash(
        {
            "schema": schema,
            "underlying": underlying.strip().upper(),
            "strategy_type": strategy_type.strip().upper(),
            "legs": structures,
        }
    )
    return actual == expected


def _normalise_preselection_coverage(
    value: object,
    *,
    pre_market: Sequence[Mapping[str, object]],
    repriced: Sequence[Mapping[str, object]],
) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        if pre_market or repriced:
            return None
        return _empty_preselection_coverage(
            reason="OPEN_REPRICE_PRODUCER_UNAVAILABLE",
            ledger_reason="PRESELECTION_PROVIDER_UNCONFIGURED",
        )

    source = _clean_text(value.get("source"), 64)
    if source != _PRESELECTION_LEDGER_SOURCE:
        if pre_market or repriced:
            return None
        source = _PRESELECTION_LEDGER_SOURCE
    requested_count = value.get("requested_count")
    declared_available_count = value.get("available_count")
    declared_open_count = value.get("open_count")
    if (
        requested_count != 10
        or isinstance(requested_count, bool)
        or isinstance(declared_available_count, bool)
        or not isinstance(declared_available_count, int)
        or not 0 <= declared_available_count <= 10
        or isinstance(declared_open_count, bool)
        or not isinstance(declared_open_count, int)
        or not 0 <= declared_open_count <= declared_available_count
    ):
        return None

    latest_run_id = _normalise_ledger_identifier(value.get("latest_run_id"))
    latest_head_hash = _normalise_digest(value.get("latest_head_hash"))
    if (latest_run_id is None) is not (latest_head_hash is None):
        return None
    if pre_market and (latest_run_id is None or latest_head_hash is None):
        return None

    freeze_slot = _normalise_aware_timestamp(value.get("freeze_slot"))
    latest_open_batch_id = _normalise_ledger_identifier(
        value.get("latest_open_batch_id")
    )
    latest_open_batch_head_hash = _normalise_digest(
        value.get("latest_open_batch_head_hash")
    )
    if (latest_open_batch_id is None) is not (
        latest_open_batch_head_hash is None
    ):
        return None
    reprice_slot = _normalise_aware_timestamp(value.get("reprice_slot"))

    raw_status = (_clean_text(value.get("status"), 24) or "UNAVAILABLE").upper()
    if raw_status not in {"AVAILABLE", "PARTIAL", "UNAVAILABLE"}:
        raw_status = "UNAVAILABLE"
    status = raw_status if pre_market else "UNAVAILABLE"
    reason = _normalise_reason_code(value.get("reason"))
    ledger_reason = _normalise_reason_code(value.get("ledger_reason"))
    producer_status = (
        _clean_text(value.get("open_reprice_producer_status"), 24) or "UNKNOWN"
    ).upper()
    if producer_status not in {"AVAILABLE", "NOT_STARTED", "UNAVAILABLE"}:
        producer_status = "UNKNOWN"
    open_status = (
        "UNAVAILABLE"
        if not pre_market
        else "NOT_STARTED"
        if not repriced
        else "AVAILABLE"
        if len(repriced) == len(pre_market)
        else "PARTIAL"
    )
    return {
        "requested_count": 10,
        "available_count": len(pre_market),
        "open_count": len(repriced),
        "source": source,
        "status": status,
        "reason": reason,
        "ledger_reason": ledger_reason,
        "latest_run_id": latest_run_id,
        "latest_head_hash": latest_head_hash,
        "freeze_slot": freeze_slot,
        "latest_open_batch_id": latest_open_batch_id,
        "latest_open_batch_head_hash": latest_open_batch_head_hash,
        "reprice_slot": reprice_slot,
        "open_reprice_producer_status": producer_status,
        "open_observation_status": open_status,
        "atomic_batch_available": False,
        "atomic_batch_blocker": None,
        "_declared_available_count": declared_available_count,
        "_declared_open_count": declared_open_count,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _empty_preselection_coverage(
    *,
    reason: str,
    ledger_reason: str | None,
) -> dict[str, object]:
    return {
        "requested_count": 10,
        "available_count": 0,
        "open_count": 0,
        "source": _PRESELECTION_LEDGER_SOURCE,
        "status": "UNAVAILABLE",
        "reason": reason,
        "ledger_reason": ledger_reason,
        "latest_run_id": None,
        "latest_head_hash": None,
        "freeze_slot": None,
        "latest_open_batch_id": None,
        "latest_open_batch_head_hash": None,
        "reprice_slot": None,
        "open_reprice_producer_status": "UNAVAILABLE",
        "open_observation_status": "UNAVAILABLE",
        "atomic_batch_available": False,
        "atomic_batch_blocker": reason,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _normalise_preselection_lineage(
    value: object,
    *,
    preselection_id: str,
    phase: str,
) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    common_fields = {
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
    expected_fields = (
        common_fields
        | {
            "observation_id",
            "observed_at",
            "observation_hash",
            "batch_id",
            "batch_head_hash",
            "scheduled_for",
            "quote_batch_id",
        }
        if phase == "OPEN_REPRICED"
        else common_fields
        | {"production_parent_eligible", "production_parent_blocker"}
        if phase == "PRE_MARKET"
        else set()
    )
    if not expected_fields or set(value) != expected_fields:
        return None
    run_id = _normalise_ledger_identifier(value.get("run_id"))
    row_id = _normalise_ledger_identifier(value.get("row_id"))
    run_created_at = _normalise_aware_timestamp(value.get("run_created_at"))
    head_hash = _normalise_digest(value.get("head_hash"))
    row_hash = _normalise_digest(value.get("row_hash"))
    premarket_rank = _normalise_rank(value.get("premarket_rank"), maximum=10)
    if (
        value.get("source") != _PRESELECTION_LEDGER_SOURCE
        or value.get("preselection_id") != preselection_id
        or value.get("phase") != phase
        or run_id is None
        or row_id is None
        or run_created_at is None
        or head_hash is None
        or row_hash is None
        or premarket_rank is None
    ):
        return None
    source_values = (
        value.get("source_batch_purpose"),
        value.get("source_batch_id"),
        value.get("source_batch_hash"),
    )
    source_present = tuple(item is not None for item in source_values)
    if any(source_present) != all(source_present):
        return None
    source_batch_purpose: str | None = None
    source_batch_id: str | None = None
    source_batch_hash: str | None = None
    if all(source_present):
        source_batch_purpose = _clean_text(value.get("source_batch_purpose"), 32)
        source_batch_id = _normalise_source_batch_id(value.get("source_batch_id"))
        source_batch_hash = _normalise_digest(value.get("source_batch_hash"))
        expected_purpose = (
            OPEN_REPRICE_PURPOSE
            if phase == "OPEN_REPRICED"
            else PREMARKET_ACCOUNT_PURPOSE
        )
        if (
            source_batch_purpose != expected_purpose
            or source_batch_id is None
            or source_batch_hash is None
        ):
            return None
    result: dict[str, object] = {
        "source": _PRESELECTION_LEDGER_SOURCE,
        "source_batch_purpose": source_batch_purpose,
        "source_batch_id": source_batch_id,
        "source_batch_hash": source_batch_hash,
        "run_id": run_id,
        "run_created_at": run_created_at,
        "head_hash": head_hash,
        "row_id": row_id,
        "row_hash": row_hash,
        "premarket_rank": premarket_rank,
    }
    if phase == "OPEN_REPRICED":
        observation_id = _normalise_ledger_identifier(value.get("observation_id"))
        observed_at = _normalise_aware_timestamp(value.get("observed_at"))
        observation_hash = _normalise_digest(value.get("observation_hash"))
        batch_values_present = tuple(
            value.get(name) is not None
            for name in (
                "batch_id",
                "batch_head_hash",
                "scheduled_for",
                "quote_batch_id",
            )
        )
        if (
            observation_id is None
            or observed_at is None
            or observation_hash is None
            or any(batch_values_present) != all(batch_values_present)
        ):
            return None
        result.update(
            {
                "observation_id": observation_id,
                "observed_at": observed_at,
                "observation_hash": observation_hash,
            }
        )
        if all(batch_values_present):
            batch_id = _normalise_ledger_identifier(value.get("batch_id"))
            batch_head_hash = _normalise_digest(value.get("batch_head_hash"))
            scheduled_for = _normalise_aware_timestamp(value.get("scheduled_for"))
            quote_batch_id = _normalise_ledger_identifier(
                value.get("quote_batch_id")
            )
            if (
                batch_id is None
                or batch_head_hash is None
                or scheduled_for is None
                or quote_batch_id is None
            ):
                return None
            result.update(
                {
                    "batch_id": batch_id,
                    "batch_head_hash": batch_head_hash,
                    "scheduled_for": scheduled_for,
                    "quote_batch_id": quote_batch_id,
                }
            )
    elif phase != "PRE_MARKET":
        return None
    else:
        eligibility_values_present = tuple(
            value.get(name) is not None
            for name in (
                "production_parent_eligible",
                "production_parent_blocker",
            )
        )
        production_parent_eligible = value.get("production_parent_eligible")
        production_parent_blocker = _normalise_reason_code(
            value.get("production_parent_blocker")
        )
        if any(eligibility_values_present):
            if (
                not isinstance(production_parent_eligible, bool)
                or (
                    production_parent_eligible
                    and value.get("production_parent_blocker") is not None
                )
                or (
                    not production_parent_eligible
                    and production_parent_blocker
                    != "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
                )
            ):
                return None
            result.update(
                {
                    "production_parent_eligible": production_parent_eligible,
                    "production_parent_blocker": production_parent_blocker,
                }
            )
    return result


def _preselection_atomic_batch_validation(
    pre_market: Sequence[Mapping[str, object]],
    repriced: Sequence[Mapping[str, object]],
    *,
    coverage: Mapping[str, object],
) -> tuple[bool, str]:
    if not pre_market:
        return False, "TOP10_PREMARKET_COVERAGE_UNAVAILABLE"
    if coverage.get("open_reprice_producer_status") != "AVAILABLE":
        return False, str(
            coverage.get("reason") or "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
        )
    if (
        coverage.get("_declared_available_count") != len(pre_market)
        or coverage.get("_declared_open_count") != len(repriced)
    ):
        return False, "PRESELECTION_COVERAGE_COUNT_MISMATCH"
    if len(repriced) != len(pre_market):
        return False, "OPEN_REPRICE_ATOMIC_SET_INCOMPLETE"
    run_id = coverage.get("latest_run_id")
    head_hash = coverage.get("latest_head_hash")
    freeze_slot = _parse_aware_timestamp(coverage.get("freeze_slot"))
    batch_id = coverage.get("latest_open_batch_id")
    batch_head_hash = coverage.get("latest_open_batch_head_hash")
    reprice_slot = _parse_aware_timestamp(coverage.get("reprice_slot"))
    if (
        not isinstance(run_id, str)
        or not isinstance(head_hash, str)
        or freeze_slot is None
        or not isinstance(batch_id, str)
        or not isinstance(batch_head_hash, str)
        or reprice_slot is None
    ):
        return False, "OPEN_REPRICE_ATOMIC_BATCH_INCOMPLETE"
    parents: dict[str, Mapping[str, object]] = {}
    row_ids: set[str] = set()
    row_hashes: set[str] = set()
    run_created_at: datetime | None = None
    premarket_source_binding: tuple[object, object, object] | None = None
    for item in pre_market:
        identifier = item.get("preselection_id")
        lineage = item.get("ledger_lineage")
        created_at = (
            _parse_aware_timestamp(lineage.get("run_created_at"))
            if isinstance(lineage, Mapping)
            else None
        )
        source_binding = (
            lineage.get("source_batch_purpose"),
            lineage.get("source_batch_id"),
            lineage.get("source_batch_hash"),
        ) if isinstance(lineage, Mapping) else (None, None, None)
        if any(value is None for value in source_binding):
            return False, "SOURCE_LINEAGE_MISSING_LEGACY"
        if (
            source_binding[0] != PREMARKET_ACCOUNT_PURPOSE
            or (
                premarket_source_binding is not None
                and source_binding != premarket_source_binding
            )
        ):
            return False, "PRESELECTION_LINEAGE_BINDING_INVALID"
        premarket_source_binding = source_binding
        if (
            not isinstance(identifier, str)
            or not isinstance(lineage, Mapping)
            or lineage.get("run_id") != run_id
            or lineage.get("head_hash") != head_hash
            or lineage.get("premarket_rank") != item.get("research_rank")
            or lineage.get("production_parent_eligible") is not True
            or lineage.get("production_parent_blocker") is not None
            or not isinstance(lineage.get("row_id"), str)
            or lineage["row_id"] in row_ids
            or not isinstance(lineage.get("row_hash"), str)
            or lineage["row_hash"] in row_hashes
            or created_at is None
            or created_at != freeze_slot
            or (run_created_at is not None and created_at != run_created_at)
            or any(
                quote_time > created_at
                for quote_time in _preselection_quote_times(item)
            )
        ):
            return False, (
                "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
                if isinstance(lineage, Mapping)
                and lineage.get("production_parent_eligible") is not True
                else "PRESELECTION_LINEAGE_BINDING_INVALID"
            )
        run_created_at = created_at
        parents[identifier] = lineage
        row_ids.add(str(lineage["row_id"]))
        row_hashes.add(str(lineage["row_hash"]))

    preselection_ids = set(parents)
    open_ids = {
        str(item.get("preselection_id"))
        for item in repriced
        if isinstance(item.get("preselection_id"), str)
    }
    if open_ids != preselection_ids or len(open_ids) != len(repriced):
        return False, "OPEN_REPRICE_ATOMIC_SET_INCOMPLETE"

    observation_ids: set[str] = set()
    observation_hashes: set[str] = set()
    quote_batch_id: str | None = None
    scheduled_for: datetime | None = None
    open_source_binding: tuple[object, object, object] | None = None
    for item in repriced:
        identifier = item.get("preselection_id")
        parent = parents.get(str(identifier))
        lineage = item.get("ledger_lineage")
        observed_at = (
            _parse_aware_timestamp(lineage.get("observed_at"))
            if isinstance(lineage, Mapping)
            else None
        )
        source_binding = (
            lineage.get("source_batch_purpose"),
            lineage.get("source_batch_id"),
            lineage.get("source_batch_hash"),
        ) if isinstance(lineage, Mapping) else (None, None, None)
        if any(value is None for value in source_binding):
            return False, "SOURCE_LINEAGE_MISSING_LEGACY"
        if (
            source_binding[0] != OPEN_REPRICE_PURPOSE
            or source_binding[1] != lineage.get("quote_batch_id")
            or (
                open_source_binding is not None
                and source_binding != open_source_binding
            )
        ):
            return False, "PRESELECTION_LINEAGE_BINDING_INVALID"
        open_source_binding = source_binding
        if (
            parent is None
            or not isinstance(lineage, Mapping)
            or lineage.get("run_id") != run_id
            or lineage.get("head_hash") != head_hash
            or lineage.get("row_id") != parent.get("row_id")
            or lineage.get("row_hash") != parent.get("row_hash")
            or lineage.get("premarket_rank") != parent.get("premarket_rank")
            or not isinstance(lineage.get("observation_id"), str)
            or lineage["observation_id"] in observation_ids
            or not isinstance(lineage.get("observed_at"), str)
            or not isinstance(lineage.get("observation_hash"), str)
            or lineage["observation_hash"] in observation_hashes
            or lineage.get("batch_id") != batch_id
            or lineage.get("batch_head_hash") != batch_head_hash
            or not isinstance(lineage.get("quote_batch_id"), str)
            or lineage.get("quote_batch_id") != item.get("quote_batch_id")
            or run_created_at is None
            or observed_at is None
            or observed_at < run_created_at
            or any(
                quote_time > observed_at
                for quote_time in _preselection_quote_times(item)
            )
        ):
            return False, "PRESELECTION_LINEAGE_BINDING_INVALID"
        row_scheduled_for = _parse_aware_timestamp(lineage.get("scheduled_for"))
        row_quote_batch_id = str(lineage["quote_batch_id"])
        if (
            row_scheduled_for is None
            or row_scheduled_for != reprice_slot
            or observed_at < row_scheduled_for
            or (
                scheduled_for is not None
                and row_scheduled_for != scheduled_for
            )
            or (
                quote_batch_id is not None
                and row_quote_batch_id != quote_batch_id
            )
            or not _preselection_quote_batch_matches(item, row_quote_batch_id)
        ):
            return False, "OPEN_REPRICE_ATOMIC_BATCH_INCOMPLETE"
        scheduled_for = row_scheduled_for
        quote_batch_id = row_quote_batch_id
        observation_ids.add(str(lineage["observation_id"]))
        observation_hashes.add(str(lineage["observation_hash"]))
    return True, "AVAILABLE"


def _preselection_quote_batch_matches(
    item: Mapping[str, object], quote_batch_id: str
) -> bool:
    legs = item.get("legs")
    return (
        isinstance(legs, Sequence)
        and not isinstance(legs, (str, bytes, bytearray))
        and bool(legs)
        and all(
            isinstance(leg, Mapping)
            and leg.get("quote_batch_id") == quote_batch_id
            for leg in legs
        )
    )


def _finalise_preselection_coverage(
    coverage: Mapping[str, object],
    *,
    atomic_batch_available: bool,
    atomic_batch_blocker: str,
    has_pre_market: bool,
    has_open_market: bool,
) -> dict[str, object]:
    result = {
        key: value for key, value in coverage.items() if not key.startswith("_")
    }
    result["atomic_batch_available"] = atomic_batch_available
    result["atomic_batch_blocker"] = (
        None if atomic_batch_available else atomic_batch_blocker
    )
    if atomic_batch_available:
        result["open_reprice_producer_status"] = "AVAILABLE"
        result["open_observation_status"] = "AVAILABLE"
    else:
        result["status"] = "PARTIAL" if has_pre_market else "UNAVAILABLE"
        result["reason"] = atomic_batch_blocker
        result["open_reprice_producer_status"] = (
            "NOT_STARTED"
            if has_pre_market and not has_open_market
            else "UNAVAILABLE"
        )
        result["open_observation_status"] = (
            "UNAVAILABLE"
            if not has_pre_market
            else "NOT_STARTED"
            if not has_open_market
            else "PARTIAL"
        )
    result.update(
        {
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }
    )
    return result


def _preselection_quote_times(item: Mapping[str, object]) -> tuple[datetime, ...]:
    legs = item.get("legs")
    if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes, bytearray)):
        return ()
    return tuple(
        parsed
        for leg in legs
        if isinstance(leg, Mapping)
        and (parsed := _parse_aware_timestamp(leg.get("quote_asof"))) is not None
    )


def _normalise_ledger_identifier(value: object) -> str | None:
    text = _clean_text(value, 256)
    if text is None:
        return None
    return text if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}", text) else None


def _normalise_source_batch_id(value: object) -> str | None:
    text = _clean_text(value, 128)
    return text if text is not None and _SOURCE_BATCH_ID_RE.fullmatch(text) else None


def _normalise_reason_code(value: object) -> str | None:
    text = (_clean_text(value, 96) or "").upper()
    return text if re.fullmatch(r"[A-Z0-9_]{1,96}", text) else None


def _api_preselection_action_complete(item: Mapping[str, object]) -> bool:
    if (
        item.get("phase") != "OPEN_REPRICED"
        or item.get("risk_defined") is not True
        or not item.get("underlying")
        or not item.get("strategy_type")
        or not item.get("strategy_hash")
        or not item.get("evidence_ids")
        or not item.get("evidence_hashes")
        or not isinstance(item.get("ledger_lineage"), Mapping)
        or len(item["evidence_ids"]) != len(item["evidence_hashes"])
        or not item.get("research_summary")
        or any(
            not item.get(name)
            for name in (
                "entry_condition",
                "invalidation_condition",
                "profit_target_condition",
                "stop_loss_condition",
            )
        )
    ):
        return False
    evidence_ids = item.get("evidence_ids")
    evidence_hashes = item.get("evidence_hashes")
    direct_discovery = any(
        str(value).startswith("IBKR_DIRECT_DISCOVERY:")
        for value in evidence_ids
    )
    if direct_discovery:
        basis = item.get("underlying_quote_basis")
        basis_hash = item.get("underlying_quote_basis_hash")
        if (
            not isinstance(basis, Mapping)
            or not isinstance(basis_hash, str)
            or basis_hash not in evidence_hashes
            or canonical_hash(basis) != basis_hash
            or basis.get("symbol") != item.get("underlying")
        ):
            return False
    maximum_loss = _decimal_value(item.get("maximum_loss_usd"))
    estimated_cost = _decimal_value(item.get("estimated_cost_usd"))
    cost_after_ev = _decimal_value(item.get("cost_after_ev_usd"))
    quote_age = item.get("maximum_quote_age_seconds")
    if (
        maximum_loss is None
        or maximum_loss <= 0
        or estimated_cost is None
        or estimated_cost < 0
        or cost_after_ev is None
        or cost_after_ev <= 0
        or not isinstance(quote_age, (int, float))
        or isinstance(quote_age, bool)
        or not 0 <= float(quote_age) <= 5
        or not _api_preselection_economics_valid(item)
    ):
        return False
    legs = item.get("legs")
    if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes, bytearray)) or not legs:
        return False
    required = (
        "underlying",
        "con_id",
        "local_symbol",
        "trading_class",
        "multiplier",
        "exchange",
        "expiry",
        "strike",
        "right",
        "side",
        "ratio",
        "quantity",
        "bid",
        "ask",
        "quote_asof",
        "quote_batch_id",
        "implied_volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "volume",
        "open_interest",
        "dte",
    )
    if any(
        not isinstance(leg, Mapping)
        or any(leg.get(name) is None for name in required)
        or leg.get("underlying") != item.get("underlying")
        or not isinstance(leg.get("dte"), int)
        or int(leg["dte"]) < 7
        for leg in legs
    ):
        return False
    batches = {str(leg["quote_batch_id"]) for leg in legs if isinstance(leg, Mapping)}
    return (
        len(batches) == 1
        and item.get("quote_batch_id") in batches
        and _api_preselection_quotes_valid(item, legs)
        and not _normalised_legs_have_naked_short(legs)
    )


def _api_preselection_economics_valid(item: Mapping[str, object]) -> bool:
    """Independently re-hash the complete 09:35 economics evidence.

    API callers cannot promote a legacy row by supplying three positive money
    fields.  Every scenario, policy, NAV, quote-batch, cost and payoff input
    must reproduce the immutable producer hash before the row can appear in
    the display-only action pool.
    """

    try:
        candidate_id = str(item["preselection_id"])
        strategy_hash = str(item["strategy_hash"])
        snapshot_hash = str(item["broker_snapshot_hash"])
        quote_batch_id = str(item["economics_quote_batch_id"])
        quote_asof = _parse_aware_timestamp(item.get("economics_quote_asof"))
        scenario_asof = _parse_aware_timestamp(item.get("scenario_asof"))
        if quote_asof is None or scenario_asof is None:
            return False
        scenario_rows = item.get("terminal_scenarios")
        if not isinstance(scenario_rows, Sequence) or isinstance(
            scenario_rows, (str, bytes, bytearray)
        ):
            return False
        scenarios = tuple(
            TrustedTerminalScenario(
                _required_api_decimal(row, "terminal_underlying_price"),
                _required_api_decimal(row, "probability"),
            )
            for row in scenario_rows
            if isinstance(row, Mapping)
        )
        if len(scenarios) != len(scenario_rows) or not scenarios:
            return False
        scenario_set = TrustedTerminalScenarioSet.create(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            scenario_asof=scenario_asof,
            scenarios=scenarios,
            current_policy_version=str(item["risk_policy_version"]),
            current_policy_hash=str(item["risk_policy_hash"]),
        )
        if (
            scenario_set.scenario_hash != item.get("scenario_hash")
            or scenario_asof > quote_asof
            or item.get("execution_cost_contract_version")
            != EXECUTION_COST_VERSION
            or item.get("execution_cost_contract_hash") != EXECUTION_COST_HASH
            or item.get("risk_policy_version") != INITIAL_POLICY_VERSION
            or item.get("risk_policy_hash") != INITIAL_POLICY_HASH
            or quote_batch_id != item.get("quote_batch_id")
        ):
            return False

        nav = _required_api_decimal(item, "strategy_nav_usd")
        debit = _required_api_decimal(item, "debit_usd")
        credit = _required_api_decimal(item, "credit_usd")
        commission = _required_api_decimal(item, "estimated_commission_usd")
        entry_slippage = _required_api_decimal(
            item, "estimated_entry_slippage_usd"
        )
        exit_slippage = _required_api_decimal(
            item, "estimated_exit_slippage_usd"
        )
        total_slippage = _required_api_decimal(item, "estimated_slippage_usd")
        all_in_cost = _required_api_decimal(item, "net_entry_cost_usd")
        maximum_loss = _required_api_decimal(item, "maximum_loss_usd")
        before_cost_ev = _required_api_decimal(
            item, "expected_value_before_costs_usd"
        )
        after_cost_ev = _required_api_decimal(item, "cost_after_ev_usd")
        risk_fraction = _required_api_decimal(item, "risk_fraction")
        if (
            nav <= 0
            or debit < 0
            or credit < 0
            or commission < 0
            or entry_slippage < 0
            or exit_slippage < 0
            or all_in_cost < 0
            or maximum_loss <= 0
            or after_cost_ev <= 0
            or total_slippage != entry_slippage + exit_slippage
            or all_in_cost != debit - credit + commission + total_slippage
            or _required_api_decimal(item, "estimated_cost_usd") != all_in_cost
            or before_cost_ev != after_cost_ev + commission + total_slippage
            or risk_fraction != maximum_loss / nav
            or risk_fraction > Decimal("0.10")
        ):
            return False
        nav_hash = strategy_nav_post_hash(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            snapshot_hash=snapshot_hash,
            strategy_nav_usd=nav,
        )
        if nav_hash != item.get("strategy_nav_post_hash"):
            return False
        economics = OpenRepriceEconomics(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            broker_snapshot_hash=snapshot_hash,
            quote_batch_id=quote_batch_id,
            quote_asof=quote_asof,
            scenario_hash=str(item["scenario_hash"]),
            scenario_asof=scenario_asof,
            cost_contract_version=str(item["execution_cost_contract_version"]),
            cost_contract_hash=str(item["execution_cost_contract_hash"]),
            policy_version=str(item["risk_policy_version"]),
            policy_hash=str(item["risk_policy_hash"]),
            strategy_nav_usd=nav,
            strategy_nav_post_hash=nav_hash,
            debit_usd=debit,
            credit_usd=credit,
            commission_usd=commission,
            entry_slippage_usd=entry_slippage,
            exit_slippage_usd=exit_slippage,
            total_slippage_usd=total_slippage,
            all_in_cost_usd=all_in_cost,
            maximum_loss_usd=maximum_loss,
            before_cost_expected_value_usd=before_cost_ev,
            after_cost_expected_value_usd=after_cost_ev,
            payoff_hash=str(item["payoff_hash"]),
            risk_fraction=risk_fraction,
            economics_hash=str(item["economics_calculation_hash"]),
        )
        return economics.verify_hash()
    except (ArithmeticError, KeyError, TypeError, ValueError):
        return False


def _required_api_decimal(value: Mapping[str, object], name: str) -> Decimal:
    parsed = _decimal_value(value.get(name))
    if parsed is None or not parsed.is_finite():
        raise ValueError(f"{name} is not a finite decimal")
    return parsed


def _api_preselection_quotes_valid(
    item: Mapping[str, object],
    legs: Sequence[object],
) -> bool:
    quote_times: list[datetime] = []
    for raw in legs:
        if not isinstance(raw, Mapping):
            return False
        strike = _decimal_value(raw.get("strike"))
        bid = _decimal_value(raw.get("bid"))
        ask = _decimal_value(raw.get("ask"))
        implied_volatility = _decimal_value(raw.get("implied_volatility"))
        quote_time = _parse_aware_timestamp(raw.get("quote_asof"))
        if (
            strike is None
            or strike <= 0
            or bid is None
            or bid < 0
            or ask is None
            or ask < 0
            or bid > ask
            or implied_volatility is None
            or implied_volatility < 0
            or quote_time is None
            or any(
                _decimal_value(raw.get(name)) is None
                for name in ("delta", "gamma", "theta", "vega")
            )
        ):
            return False
        quote_times.append(quote_time)
    oldest = _parse_aware_timestamp(item.get("oldest_quote_asof"))
    if oldest is None or not quote_times or oldest != min(quote_times):
        return False
    lineage = item.get("ledger_lineage")
    if not isinstance(lineage, Mapping):
        return False
    observed_at = _parse_aware_timestamp(lineage.get("observed_at"))
    if observed_at is None or any(value > observed_at for value in quote_times):
        return False
    return True


def _normalised_legs_have_naked_short(legs: Sequence[object]) -> bool:
    buckets: dict[tuple[str, str], dict[str, int]] = {}
    for value in legs:
        if not isinstance(value, Mapping):
            return True
        expiry = value.get("expiry")
        right = value.get("right")
        side = value.get("side")
        ratio = value.get("ratio")
        quantity = value.get("quantity")
        if (
            not isinstance(expiry, str)
            or right not in {"CALL", "PUT"}
            or side not in {"BUY", "SELL"}
            or not isinstance(ratio, int)
            or not isinstance(quantity, int)
        ):
            return True
        bucket = buckets.setdefault((expiry, str(right)), {"BUY": 0, "SELL": 0})
        bucket[str(side)] += ratio * quantity
    return any(bucket["SELL"] > bucket["BUY"] for bucket in buckets.values())


def _normalise_decimal_text(value: object) -> str | None:
    parsed = _decimal_value(value)
    return None if parsed is None else str(value).strip()


def _decimal_value(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _normalise_positive_integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _normalise_nonnegative_integer_or_none(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _normalise_digest(value: object) -> str | None:
    text = _clean_text(value, 64)
    if text is None:
        return None
    lowered = text.lower()
    return lowered if re.fullmatch(r"[0-9a-f]{64}", lowered) else None


def _normalise_related_options(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    research: list[dict[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            continue
        if "preselection_id" in item or "legs" in item:
            normalised = _normalise_option_preselection(item, index=index)
            if normalised is not None:
                research.append(normalised)
        else:
            research.append(
                {
                    "symbol": _clean_text(item.get("symbol", item.get("underlying")), 16),
                    "summary": _clean_text(item.get("summary", item.get("thesis")), 480),
                    "asof": _normalise_timestamp(item.get("asof", item.get("observed_at"))),
                }
            )
        if len(research) == 10:
            break
    return research


def _normalise_top10_preselection_ledger_health(
    value: Mapping[str, Any],
) -> dict[str, object]:
    projection: dict[str, object] = {}
    text_fields = {
        "health": 32,
        "reason": 128,
        "ledger_reason": 128,
        "source": 64,
        "open_reprice_producer_status": 32,
        "open_reprice_writer": 64,
    }
    for field, maximum in text_fields.items():
        if field in value:
            projection[field] = _clean_text(value.get(field), maximum)

    readable = value.get("readable")
    if isinstance(readable, bool):
        projection["readable"] = readable

    for field in ("requested_count", "available_count", "open_count"):
        count = _normalise_nonnegative_integer_or_none(value.get(field))
        if count is not None and count <= 10:
            projection[field] = count

    for field in ("latest_run_id", "latest_open_batch_id"):
        if field in value:
            projection[field] = _normalise_ledger_identifier(value.get(field))
    for field in ("latest_head_hash", "latest_open_batch_head_hash"):
        if field in value:
            projection[field] = _normalise_digest(value.get(field))
    for field in ("freeze_slot", "reprice_slot"):
        if field in value:
            projection[field] = _normalise_timestamp(value.get(field))

    if "decision_authority" in value:
        projection["decision_authority"] = "SUPPORTING_ONLY"
    for field in (
        "approval_eligible",
        "instruction_creation_allowed",
        "order_allowed",
    ):
        if field in value:
            projection[field] = False
    return projection


def _normalise_health(raw: Mapping[str, Any]) -> dict[str, object]:
    dependencies_raw = raw.get("dependencies")
    if isinstance(dependencies_raw, Mapping):
        source = dependencies_raw
    elif raw and all(isinstance(value, Mapping) for value in raw.values()):
        source = raw
    else:
        source = {"options_copilot": raw}

    allowed = {
        "status",
        "state",
        "connected",
        "paper",
        "stale",
        "age_ms",
        "asof",
        "message",
    }
    dependencies: dict[str, dict[str, object]] = {}
    statuses: list[str] = []
    for name, value in source.items():
        if not isinstance(value, Mapping):
            continue
        dependency = {str(key): item for key, item in value.items() if key in allowed}
        if str(name) == "top10_preselection_ledger":
            dependency.update(_normalise_top10_preselection_ledger_health(value))
        if str(name) == "production_scanner":
            control = _normalise_control_snapshot_health(
                value.get("control_snapshot")
            )
            if control is not None:
                dependency["control_snapshot"] = control
            upstream = value.get("broker_upstream")
            if isinstance(upstream, Mapping):
                upstream_status = upstream.get("status")
                authority_state = upstream.get("authority_state")
                reasons = upstream.get("reason_codes", ())
                dependency["broker_upstream"] = {
                    "status": upstream_status if upstream_status in {"UP", "DOWN", "DEGRADED"} else "DEGRADED",
                    "authority_state": authority_state if authority_state in {
                        "READY", "LOST", "RECOVERY_PENDING", "RECONNECT_REQUIRED", "DISCONNECTED",
                    } else "UNAVAILABLE",
                    "verified_at": _normalise_aware_timestamp(upstream.get("verified_at")),
                    "last_success_at": _normalise_aware_timestamp(upstream.get("last_success_at")),
                    "generation": _normalise_bounded_nonnegative_integer(upstream.get("generation"), maximum=2**53 - 1),
                    "reason_codes": [
                        reason for reason in (reasons[:16] if isinstance(reasons, (tuple, list)) else ())
                        if isinstance(reason, str) and re.fullmatch(r"[A-Z0-9_]{1,128}", reason)
                    ],
                    "decision_authority": "OBSERVATION_ONLY",
                    "review_only": True,
                }
            scheduler = value.get("scheduler")
            if isinstance(scheduler, Mapping):
                daily_operations = _normalise_daily_operations(
                    scheduler.get("daily_operations")
                )
                if daily_operations is not None:
                    dependency["daily_operations"] = daily_operations
                top10_raw = scheduler.get("top10_producer")
                if isinstance(top10_raw, Mapping):
                    reason_codes = top10_raw.get("last_reason_codes")
                    missing_symbols = top10_raw.get("last_missing_symbols")
                    dependency["top10_producer"] = {
                        "status": _clean_text(top10_raw.get("status"), 32)
                        or "UNKNOWN",
                        "last_tick_status": _clean_text(
                            top10_raw.get("last_tick_status"),
                            32,
                        ),
                        "last_producer_status": _clean_text(
                            top10_raw.get("last_producer_status"),
                            32,
                        ),
                        "last_reason": _clean_text(
                            top10_raw.get("last_reason"),
                            128,
                        ),
                        "last_reason_codes": [
                            cleaned
                            for item in (
                                reason_codes
                                if isinstance(reason_codes, Sequence)
                                and not isinstance(
                                    reason_codes,
                                    (str, bytes, bytearray, memoryview),
                                )
                                else ()
                            )[:32]
                            if (cleaned := _clean_text(item, 128)) is not None
                        ],
                        "last_missing_symbols": [
                            cleaned
                            for item in (
                                missing_symbols
                                if isinstance(missing_symbols, Sequence)
                                and not isinstance(
                                    missing_symbols,
                                    (str, bytes, bytearray, memoryview),
                                )
                                else ()
                            )[:32]
                            if (cleaned := _clean_text(item, 32)) is not None
                        ],
                        "last_written_count": _normalise_bounded_nonnegative_integer(
                            top10_raw.get("last_written_count"), maximum=10
                        ),
                        "last_producer_slot": _clean_text(
                            top10_raw.get("last_producer_slot"), 64
                        ),
                        "last_producer_run_id": _normalise_ledger_identifier(
                            top10_raw.get("last_producer_run_id")
                        ),
                        "last_producer_evidence_hash": _normalise_digest(
                            top10_raw.get("last_producer_evidence_hash")
                        ),
                        "review_only": True,
                        "direct_order_submission": False,
                    }
        status = str(dependency.get("status") or dependency.get("state") or "UNKNOWN")
        dependency["status"] = status.upper()
        dependencies[str(name)] = dependency
        statuses.append(status.upper())

    down_states = {"DOWN", "ERROR", "DISCONNECTED"}
    ready_states = {"UP", "READY", "CONNECTED", "HEALTHY"}
    if any(status in down_states for status in statuses):
        overall = "DOWN"
    elif statuses and all(status in ready_states for status in statuses):
        overall = "UP"
    else:
        overall = "DEGRADED"
    return {"status": overall, "dependencies": dependencies}


def _normalise_control_snapshot_health(value: object) -> dict[str, object] | None:
    """Expose cached control counts without requesting fresh broker state."""

    if not isinstance(value, Mapping):
        return None
    status = str(value.get("status") or "UNAVAILABLE").strip().upper()
    if status not in {"CURRENT", "PARTIAL", "STALE", "UNAVAILABLE"}:
        status = "UNAVAILABLE"
    stale = value.get("stale")
    return {
        "status": status,
        "stale": stale if isinstance(stale, bool) else status != "CURRENT",
        "observed_at": _normalise_aware_timestamp(value.get("observed_at")),
        "age_ms": _normalise_bounded_nonnegative_integer(
            value.get("age_ms"),
            maximum=86_400_000,
        ),
        "positions_count": _normalise_bounded_nonnegative_integer(
            value.get("positions_count"),
            maximum=100_000,
        ),
        "working_order_count": _normalise_bounded_nonnegative_integer(
            value.get("working_order_count"),
            maximum=100_000,
        ),
        "unsubmitted_instruction_count": _normalise_bounded_nonnegative_integer(
            value.get("unsubmitted_instruction_count"),
            maximum=100_000,
        ),
        "decision_authority": "OBSERVATION_ONLY",
        "review_only": True,
        "direct_order_submission": False,
    }


def _normalise_daily_operations(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    calendar_raw = value.get("calendar_refresh")
    calendar = calendar_raw if isinstance(calendar_raw, Mapping) else {}
    calendar_status = str(calendar.get("status") or "NOT_RUN").upper()
    if calendar_status not in {"NOT_RUN", "COMPLETED", "DEGRADED"}:
        calendar_status = "DEGRADED"
    projected_runs = _normalise_daily_runs(value.get("runs"))
    next_session = _normalise_supporting_daily_result(
        value.get("next_session_preparation"),
        operation="NEXT_SESSION_PREPARATION",
    )
    position_research = _normalise_supporting_daily_result(
        value.get("position_research"),
        operation="POSITION_RESEARCH",
    )
    after_hours_reprice = _normalise_supporting_daily_result(
        value.get("after_hours_reprice"),
        operation="AFTER_HOURS_REPRICE",
    )
    manifest_raw = value.get("day_manifest")
    manifest = manifest_raw if isinstance(manifest_raw, Mapping) else {}
    today_raw = value.get("today")
    today = today_raw if isinstance(today_raw, Mapping) else {}
    market_status = str(today.get("market_status") or "UNVERIFIED").upper()
    if market_status not in {"TRADING_SESSION", "CLOSED", "UNVERIFIED"}:
        market_status = "UNVERIFIED"
    last_raw = value.get("last_completed_day")
    last_completed = last_raw if isinstance(last_raw, Mapping) else {}
    return {
        "timezone": (
            "America/New_York"
            if value.get("timezone") == "America/New_York"
            else "UNKNOWN"
        ),
        "calendar_refresh": {
            "schedule": (
                str(calendar.get("schedule"))
                if calendar.get("schedule")
                in {"EVERY_HEARTBEAT", "FAILURE_BACKOFF"}
                else "UNKNOWN"
            ),
            "status": calendar_status,
            "last_run_at": _normalise_aware_timestamp(calendar.get("last_run_at")),
            **(
                {
                    "next_retry_at": _normalise_aware_timestamp(
                        calendar.get("next_retry_at")
                    )
                }
                if calendar.get("next_retry_at") is not None
                else {}
            ),
            "reason_codes": _normalise_reason_codes(calendar.get("reason_codes")),
        },
        "outcome_processing": _normalise_outcome_processing(
            value.get("outcome_processing")
        ),
        "position_research": position_research,
        "after_hours_reprice": after_hours_reprice,
        "next_session_preparation": next_session,
        "day_manifest": {
            "manifest_id": _clean_text(manifest.get("manifest_id"), 96),
            "trading_date": _clean_text(manifest.get("trading_date"), 10),
            "manifest_hash": _normalise_digest(manifest.get("manifest_hash")),
            "recorded_at": _normalise_aware_timestamp(manifest.get("recorded_at")),
        },
        "today": {
            "trading_date": _normalise_date(today.get("trading_date")),
            "market_status": market_status,
            "next_trading_date": _normalise_date(today.get("next_trading_date")),
            "runs": projected_runs,
        },
        "last_completed_day": {
            "trading_date": _clean_text(last_completed.get("trading_date"), 10),
            "manifest_id": _clean_text(last_completed.get("manifest_id"), 96),
            "manifest_hash": _normalise_digest(last_completed.get("manifest_hash")),
            "recorded_at": _normalise_aware_timestamp(
                last_completed.get("recorded_at")
            ),
            "runs": _normalise_daily_runs(last_completed.get("runs")),
        },
        "runs": projected_runs,
    }


def _normalise_daily_runs(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return []
    allowed_operations = {
        "RESEARCH_REFRESH",
        "TOP10_FREEZE",
        "TOP10_REPRICE",
        "ORDINARY_SCAN",
        "OUTCOME_PROCESSING",
        "AFTER_HOURS_DISCOVERY",
        "AFTER_HOURS_REPRICE",
        "NEXT_SESSION_PREPARATION",
    }
    allowed_statuses = {
        "PENDING",
        "DUE",
        "RECOVERABLE",
        "LEASED",
        "COMPLETED",
        "FAILED",
        "MISSED_NOT_REPLAYED",
        "NO_TRADE",
    }
    result: list[dict[str, object]] = []
    for raw in value[:32]:
        if not isinstance(raw, Mapping):
            continue
        operation = str(raw.get("operation") or "").upper()
        if operation not in allowed_operations:
            continue
        status = str(raw.get("status") or "NO_TRADE").upper()
        if status not in allowed_statuses:
            status = "NO_TRADE"
        policy = str(raw.get("recovery_policy") or "").upper()
        if policy not in {
            "EXACT_ONLY_NO_REPLAY",
            "LATEST_ONLY_WITHIN_TWO_HOURS_FRESH_EVIDENCE",
        }:
            policy = "EXACT_ONLY_NO_REPLAY"
        handler = raw.get("handler_status")
        handler_status = None if handler is None else str(handler).upper()
        if handler_status not in {None, "READY", "UNAVAILABLE"}:
            handler_status = "UNAVAILABLE"
        terminalization_error = (
            _clean_text(raw.get("terminalization_error"), 64) or ""
        )
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", terminalization_error):
            terminalization_error = None
        projected = {
            "operation": operation,
            "scheduled_at": _normalise_offset_timestamp(raw.get("scheduled_at")),
            "status": status,
            "recovery_policy": policy,
            "scan_run_id": _clean_text(raw.get("scan_run_id"), 96),
            "handler_status": handler_status,
        }
        producer_status = str(raw.get("producer_status") or "").upper()
        if producer_status in {
            "PREMARKET_FROZEN",
            "OPEN_REPRICED",
            "POSITION_MANAGEMENT_ONLY",
            "NO_TRADE",
        }:
            projected["producer_status"] = producer_status
            projected["producer_written_count"] = _normalise_bounded_nonnegative_integer(
                raw.get("producer_written_count"),
                maximum=10,
            )
            projected["producer_missing_symbols"] = [
                cleaned
                for item in (
                    raw.get("producer_missing_symbols")
                    if isinstance(raw.get("producer_missing_symbols"), Sequence)
                    and not isinstance(
                        raw.get("producer_missing_symbols"),
                        (str, bytes, bytearray, memoryview),
                    )
                    else ()
                )[:32]
                if (cleaned := _clean_text(item, 32)) is not None
            ]
            projected["producer_evidence_hash"] = _normalise_digest(
                raw.get("producer_evidence_hash")
            )
        if terminalization_error is not None:
            projected["terminalization_error"] = terminalization_error
        reason_codes = _normalise_reason_codes(raw.get("reason_codes"))
        if reason_codes:
            projected["reason_codes"] = reason_codes
        recorded_at = _normalise_aware_timestamp(raw.get("recorded_at"))
        if recorded_at is not None:
            projected["recorded_at"] = recorded_at
        result.append(projected)
    return result


def _normalise_supporting_daily_result(
    value: object,
    *,
    operation: str,
) -> dict[str, object]:
    raw = value if isinstance(value, Mapping) else {}
    status = str(raw.get("status") or "UNAVAILABLE").upper()
    if status not in {
        "UNAVAILABLE",
        "NOT_RUN",
        "READY",
        "COMPLETED",
        "DEGRADED",
        "FAILED",
    }:
        status = "DEGRADED"
    return {
        "operation": operation,
        "status": status,
        "checked_at": _normalise_aware_timestamp(
            raw.get("checked_at", raw.get("observed_at", raw.get("prepared_at")))
        ),
        "source_preparation_checked_at": _normalise_aware_timestamp(
            raw.get("source_preparation_checked_at")
        ),
        "reconciled_from_verified_after_hours": (
            raw.get("reconciled_from_verified_after_hours") is True
        ),
        "reconciled_from_durable_after_hours": (
            raw.get("reconciled_from_durable_after_hours") is True
        ),
        "next_trading_date": _clean_text(raw.get("next_trading_date"), 10),
        "priced_count": _normalise_nonnegative_integer(raw.get("priced_count")),
        "requested_count": _normalise_nonnegative_integer(raw.get("requested_count")),
        "equity_research_count": _normalise_nonnegative_integer(
            raw.get("equity_research_count")
        ),
        "equity_selected_count": _normalise_nonnegative_integer(
            raw.get("equity_selected_count")
        ),
        "option_research_structure_count": _normalise_nonnegative_integer(
            raw.get(
                "option_research_structure_count",
                raw.get("option_structure_count"),
            )
        ),
        "option_structure_count": _normalise_nonnegative_integer(
            raw.get("option_structure_count")
        ),
        "premarket_parent_eligible_structure_count": _normalise_nonnegative_integer(
            raw.get("premarket_parent_eligible_structure_count")
        ),
        "executable_count": 0,
        "research_watchlist_count": _normalise_nonnegative_integer(
            raw.get("research_watchlist_count")
        ),
        "next_retry_at": _normalise_aware_timestamp(raw.get("next_retry_at")),
        "retry_attempt": _normalise_nonnegative_integer(raw.get("retry_attempt")),
        "retry_limit": _normalise_nonnegative_integer(raw.get("retry_limit")),
        "retry_pending": raw.get("retry_pending") is True,
        "retry_exhausted": raw.get("retry_exhausted") is True,
        "reason_codes": _normalise_reason_codes(raw.get("reason_codes")),
        "decision": "NO_TRADE",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "review_only": True,
        "direct_order_submission": False,
    }


def _normalise_outcome_processing(value: object) -> dict[str, object]:
    raw = value if isinstance(value, Mapping) else {}
    status = str(raw.get("status") or "UNAVAILABLE").upper()
    if status not in {
        "UNAVAILABLE",
        "NOT_RUN",
        "WAITING_FOR_OBSERVATIONS",
        "COMPLETED",
        "DEGRADED",
        "FAILED",
    }:
        status = "DEGRADED"
    appended = _normalise_nonnegative_integer(raw.get("records_appended"))
    superseded = _normalise_nonnegative_integer(raw.get("records_superseded"))
    reasons: list[str] = []
    raw_reasons = raw.get("reason_codes")
    if isinstance(raw_reasons, Sequence) and not isinstance(
        raw_reasons,
        (str, bytes, bytearray, memoryview),
    ):
        for value in raw_reasons[:16]:
            reason = (_clean_text(value, 96) or "").upper()
            if re.fullmatch(r"[A-Z0-9_]+", reason) and reason not in reasons:
                reasons.append(reason)
    return {
        "status": status,
        "checked_at": _normalise_aware_timestamp(raw.get("checked_at")),
        "due_count": _normalise_nonnegative_integer(raw.get("due_count")),
        "recorded_count": appended + superseded,
        "skipped_count": _normalise_nonnegative_integer(
            raw.get("records_skipped")
        ),
        "blocked_count": _normalise_nonnegative_integer(
            raw.get("records_blocked")
        ),
        "error_count": _normalise_nonnegative_integer(
            raw.get("records_rejected")
        ),
        "reason_codes": reasons,
        "candidate_ledger_head_hash": _normalise_digest(
            raw.get("candidate_ledger_head_hash")
        ),
        "shadow_ledger_head_hash": _normalise_digest(
            raw.get("shadow_ledger_head_hash")
        ),
        "manifest_hash": _normalise_digest(raw.get("manifest_hash")),
        "processing_hash": _normalise_digest(raw.get("processing_hash")),
        "prediction_cursor": _normalise_nonnegative_integer(
            raw.get("prediction_cursor")
        ),
        "candidate_cursor": _normalise_nonnegative_integer(
            raw.get("candidate_cursor")
        ),
        "remaining_count": _normalise_nonnegative_integer(
            raw.get("remaining_count")
        ),
        "progress_sequence": _normalise_nonnegative_integer(
            raw.get("progress_sequence")
        ),
        "progress_hash": _normalise_digest(raw.get("progress_hash")),
        "bounded": raw.get("bounded") is True,
        "decision_authority": "SUPPORTING_ONLY",
        "affects_production_weights": False,
        "affects_eligibility": False,
        "affects_risk": False,
        "affects_ranking": False,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _normalise_outcome_capture(value: object) -> dict[str, object]:
    """Allowlist the live read-only outcome observation producer status."""

    raw = value if isinstance(value, Mapping) else {}
    status = str(raw.get("status") or "UNAVAILABLE").upper()
    if status not in {
        "UNAVAILABLE",
        "NOT_RUN",
        "WAITING",
        "COMPLETED",
        "DEGRADED",
        "BLOCKED",
    }:
        status = "DEGRADED"
    reasons: list[str] = []
    raw_reasons = raw.get("reason_codes")
    if isinstance(raw_reasons, Sequence) and not isinstance(
        raw_reasons,
        (str, bytes, bytearray, memoryview),
    ):
        for value in raw_reasons[:16]:
            reason = (_clean_text(value, 96) or "").upper()
            if re.fullmatch(r"[A-Z0-9_]+", reason) and reason not in reasons:
                reasons.append(reason)
    def counts(name: str) -> dict[str, int]:
        source = raw.get(name)
        if not isinstance(source, Mapping):
            return {}
        output: dict[str, int] = {}
        for key, count in source.items():
            label = (_clean_text(key, 96) or "").upper()
            if (
                re.fullmatch(r"[A-Z0-9_]+", label)
                and isinstance(count, int)
                and not isinstance(count, bool)
                and count >= 0
            ):
                output[label] = count
        return dict(sorted(output.items()))

    return {
        "status": status,
        "checked_at": _normalise_aware_timestamp(raw.get("checked_at")),
        "specs_seen": _normalise_nonnegative_integer(raw.get("specs_seen")),
        "specs_due": _normalise_nonnegative_integer(raw.get("specs_due")),
        "observations_appended": _normalise_nonnegative_integer(
            raw.get("observations_appended")
        ),
        "records_blocked": _normalise_nonnegative_integer(
            raw.get("records_blocked")
        ),
        "records_skipped": _normalise_nonnegative_integer(
            raw.get("records_skipped")
        ),
        "reason_codes": reasons,
        "durable_status_counts": counts("durable_status_counts"),
        "durable_blocker_counts": counts("durable_blocker_counts"),
        "direction_outcomes_enabled": raw.get("direction_outcomes_enabled") is True,
        "option_economics_requires_bound_candidate": (
            raw.get("option_economics_requires_bound_candidate") is True
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "affects_production_weights": False,
        "affects_eligibility": False,
        "affects_risk": False,
        "affects_ranking": False,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _normalise_outcome_horizon_summary(value: object) -> dict[str, object]:
    """Allowlist one ledger-complete, selected-challenger GUI summary."""

    unavailable = {
        "status": "UNAVAILABLE",
        "reason": "OUTCOME_HORIZON_SUMMARY_UNAVAILABLE",
        "decision_authority": "SUPPORTING_ONLY",
        "selected_challenger": None,
        "complete_through_sequence": None,
        "verified_head_sequence": None,
        "verified_head_hash": None,
        "horizons": {},
    }
    if not isinstance(value, Mapping) or str(value.get("status") or "").upper() != "READY":
        return unavailable
    selected_challenger = _clean_text(value.get("selected_challenger"), 160)
    complete_through = value.get("complete_through_sequence")
    verified_head_sequence = value.get("verified_head_sequence")
    verified_head_hash = _normalise_digest(value.get("verified_head_hash"))
    if (
        not selected_challenger
        or not isinstance(complete_through, int)
        or isinstance(complete_through, bool)
        or complete_through < 0
        or not isinstance(verified_head_sequence, int)
        or isinstance(verified_head_sequence, bool)
        or verified_head_sequence != complete_through
        or verified_head_hash is None
    ):
        return unavailable
    raw_horizons = value.get("horizons")
    if not isinstance(raw_horizons, Mapping):
        return unavailable
    horizons: dict[str, object] = {}
    for horizon in ("30M", "SESSION_CLOSE", "1D", "3D", "5D"):
        raw = raw_horizons.get(horizon)
        if not isinstance(raw, Mapping):
            return unavailable
        raw_counts = tuple(
            raw.get(field)
            for field in (
                "count",
                "observed_count",
                "blocked_count",
                "uncertain_count",
            )
        )
        if any(
            not isinstance(item, int) or isinstance(item, bool) or item < 0
            for item in raw_counts
        ):
            return unavailable
        count, observed_count, blocked_count, uncertain_count = raw_counts
        if observed_count + blocked_count + uncertain_count != count:
            return unavailable
        status = str(raw.get("status") or "").upper()
        if status not in {"OBSERVED", "BLOCKED", "UNCERTAIN", "NOT_OBSERVED"}:
            return unavailable
        reason = (_clean_text(raw.get("reason"), 96) or "").upper()
        if not re.fullmatch(r"[A-Z0-9_]+", reason):
            return unavailable
        observed_at = (
            None
            if raw.get("observed_at") is None
            else _normalise_aware_timestamp(raw.get("observed_at"))
        )
        if raw.get("observed_at") is not None and observed_at is None:
            return unavailable
        horizons[horizon] = {
            "status": status,
            "count": count,
            "observed_count": observed_count,
            "blocked_count": blocked_count,
            "uncertain_count": uncertain_count,
            "reason": reason,
            "observed_at": observed_at,
            "decision_authority": "SUPPORTING_ONLY",
        }
    return {
        "status": "READY",
        "decision_authority": "SUPPORTING_ONLY",
        "selected_challenger": selected_challenger,
        "complete_through_sequence": complete_through,
        "verified_head_sequence": verified_head_sequence,
        "verified_head_hash": verified_head_hash,
        "horizons": horizons,
    }


def _normalise_approval_status(
    approval_id: str,
    raw: Mapping[str, Any],
) -> dict[str, object]:
    allowed_states = {
        "PENDING_CODEX_BRIDGE",
        "CLAIMED",
        "AUTHORIZED",
        "UNKNOWN_OUTCOME",
        "READY_FOR_IBKR_REVIEW",
        "FAILED",
        "EXPIRED",
    }
    status = str(raw.get("status") or "").upper()
    if status not in allowed_states:
        raise HTTPException(status_code=502, detail="invalid review handoff state")
    if (
        raw.get("order_submitted") is not False
        or raw.get("transmitted_to_broker") is not False
    ):
        raise HTTPException(status_code=502, detail="unsafe review handoff state")

    deep_link = raw.get("ibkr_deep_link")
    instruction_id = raw.get("instruction_id")
    if status == "READY_FOR_IBKR_REVIEW":
        # Installed capability facts do not contain an authoritative destination
        # contract.  No provider URL can become a review destination by inference.
        raise HTTPException(
            status_code=502,
            detail="creator review destination contract is unavailable",
        )
    elif deep_link is not None or instruction_id is not None:
        raise HTTPException(
            status_code=502,
            detail="non-ready handoff exposed an instruction",
        )

    return {
        "approval_id": approval_id,
        "status": status,
        "expires_at": raw.get("expires_at"),
        "instruction_id": instruction_id,
        "ibkr_deep_link": deep_link,
        "failure_reason": raw.get("failure_reason"),
        "review_only": True,
        "order_submitted": False,
        "transmitted_to_broker": False,
    }
