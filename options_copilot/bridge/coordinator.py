"""Fail-closed local coordinator for the Codex review-instruction bridge.

The coordinator consumes a caller-supplied *snapshot*.  It never opens a
Managed Connector, imports a broker adapter, or accepts authentication
material.  A separately injected review-only creator may be invoked only
after the durable bridge state is ``AUTHORIZED`` and after an append-only
single-attempt reservation has committed.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import re
from typing import Protocol, runtime_checkable

from options_copilot.gateway import (
    AtomicBrokerSnapshot,
    BrokerSnapshotBuilder,
    BrokerSnapshotStatus,
    OptionContractRef,
)
from options_copilot.market import UsOptionsCalendarSnapshot
from options_copilot.proposals import ValidatedProposal, validate_proposal
from options_copilot.risk import (
    DteEntryExceptionAuthority,
    OptionTimePolicy,
    RiskEngine,
    TimePolicyDecision,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)

from .store import (
    AtomicBrokerGateInput,
    BridgeError,
    BridgeRecord,
    BridgeStateError,
    BridgeStatus,
    CodexBridgeStore,
)


HARD_BROKER_SNAPSHOT_AGE_SECONDS = Decimal("5")


class BridgeBrokerSnapshotRejected(BridgeError):
    """Current account, inventory, order, or quote state failed closed."""


class BridgeUnknownOutcomeError(BridgeError):
    """An external attempt happened but its safe durable result is uncertain."""

    def __init__(self, approval_id: str, reason_code: str) -> None:
        self.approval_id = approval_id
        self.reason_code = reason_code
        super().__init__(
            "external result is uncertain; approval is terminal and retry is forbidden"
        )


class CoordinatorStatus(str, Enum):
    CLAIMED = "CLAIMED"
    AUTHORIZED = "AUTHORIZED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"


@runtime_checkable
class ReviewInstructionCreator(Protocol):
    """Minimal injected capability; deliberately contains no auth surface."""

    def create_review_instruction(
        self,
        *,
        idempotency_key: str,
        proposal: Mapping[str, object],
        instruction_intent: Mapping[str, object],
        review_only: bool,
    ) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class BrokerGateResult:
    validated_proposal: ValidatedProposal
    net_liquidation_usd: Decimal
    quote_snapshot_id: str
    snapshot_observed_at: datetime
    oldest_quote_at: datetime
    contract_definitions_hash: str


@dataclass(frozen=True, slots=True)
class BridgeDecisionContext:
    """Immutable production risk, cost, and policy authority for one requery."""

    risk_engine: RiskEngine
    execution_cost_contract_version: str
    execution_cost_contract_hash: str
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    option_time_policy: OptionTimePolicy
    market_calendar: UsOptionsCalendarSnapshot
    dte_exception_authority: DteEntryExceptionAuthority | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.risk_engine, RiskEngine):
            raise TypeError("risk_engine must be a RiskEngine")
        if not self.risk_engine.production_bound:
            raise ValueError("bridge decision context requires production Strategy NAV")
        nav = self.risk_engine.strategy_nav
        authority = self.risk_engine.risk_tier_authority
        if (
            nav is None
            or not nav.valid
            or nav.strategy_nav is None
            or nav.contract_hash is None
            or nav.ledger_head_hash is None
        ):
            raise ValueError("bridge decision context requires valid Strategy NAV bindings")
        if authority is None:
            raise ValueError("bridge decision context requires risk authority")
        if authority.risk_contract_hash != nav.contract_hash:
            raise ValueError("risk authority is not bound to the Strategy NAV contract")
        if not isinstance(self.option_time_policy, OptionTimePolicy):
            raise TypeError("option_time_policy must be an OptionTimePolicy")
        if not isinstance(self.market_calendar, UsOptionsCalendarSnapshot):
            raise TypeError("market_calendar must be a UsOptionsCalendarSnapshot")
        if not self.market_calendar.verify_hash():
            raise ValueError("market_calendar immutable hash is invalid")
        if self.dte_exception_authority is not None and not isinstance(
            self.dte_exception_authority,
            DteEntryExceptionAuthority,
        ):
            raise TypeError(
                "dte_exception_authority must be a DteEntryExceptionAuthority"
            )
        for name in (
            "execution_cost_contract_version",
            "current_policy_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a nonblank string")
            object.__setattr__(self, name, value.strip())
        for name in (
            "execution_cost_contract_hash",
            "current_policy_hash",
            "policy_authority_marker_hash",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError(f"{name} must be lowercase SHA-256 hex")

    def bindings(
        self,
        *,
        entry_time_decision: TimePolicyDecision,
    ) -> dict[str, object]:
        nav = self.risk_engine.strategy_nav
        authority = self.risk_engine.risk_tier_authority
        assert nav is not None
        assert nav.strategy_nav is not None
        assert nav.contract_hash is not None
        assert nav.ledger_head_hash is not None
        assert authority is not None
        if not isinstance(entry_time_decision, TimePolicyDecision):
            raise TypeError("entry_time_decision must be a TimePolicyDecision")
        if (
            entry_time_decision.mode != "ENTRY"
            or not entry_time_decision.allowed
            or not entry_time_decision.verify_hash()
            or entry_time_decision.policy_version != self.option_time_policy.version
            or entry_time_decision.policy_hash != self.option_time_policy.policy_hash
            or entry_time_decision.calendar_hash != self.market_calendar.calendar_hash
            or entry_time_decision.calendar_source_hash
            != self.market_calendar.source_hash
        ):
            raise ValueError("entry_time_decision is not bound to the decision context")
        return {
            "schema": "options_copilot.bridge.decision_bindings.v2",
            "strategy_nav_usd": nav.strategy_nav,
            "strategy_nav_snapshot_hash": nav.authority_hash,
            "strategy_nav_contract_hash": nav.contract_hash,
            "strategy_nav_ledger_head_hash": nav.ledger_head_hash,
            "risk_authority_version": authority.version,
            "risk_authority_marker_hash": authority.risk_authority_marker_hash,
            "execution_cost_contract_version": self.execution_cost_contract_version,
            "execution_cost_contract_hash": self.execution_cost_contract_hash,
            "current_policy_version": self.current_policy_version,
            "current_policy_hash": self.current_policy_hash,
            "policy_authority_marker_hash": self.policy_authority_marker_hash,
            "entry_time_decision": entry_time_decision.as_dict(),
        }


class LocalCodexBridgeCoordinator:
    """Coordinate validation, authorization, one external attempt, and consume."""

    def __init__(
        self,
        store: CodexBridgeStore,
        *,
        clock: Callable[[], datetime] | None = None,
        proposal_validator: Callable[..., ValidatedProposal] = validate_proposal,
        a_grade_unlocked: bool = False,
        broker_snapshot_builder: BrokerSnapshotBuilder | None = None,
        contract_resolver: Callable[
            [Mapping[str, object]], Sequence[OptionContractRef]
        ]
        | None = None,
        decision_context_resolver: Callable[[], BridgeDecisionContext] | None = None,
    ) -> None:
        if not isinstance(store, CodexBridgeStore):
            raise TypeError("store must be a CodexBridgeStore")
        if not callable(proposal_validator):
            raise TypeError("proposal_validator must be callable")
        if not isinstance(a_grade_unlocked, bool):
            raise TypeError("a_grade_unlocked must be a boolean")
        atomic_components = (
            broker_snapshot_builder,
            contract_resolver,
            decision_context_resolver,
        )
        if any(value is not None for value in atomic_components) and not all(
            value is not None for value in atomic_components
        ):
            raise ValueError(
                "broker snapshot builder, contract resolver, and decision context "
                "resolver must be configured together"
            )
        if broker_snapshot_builder is not None and not isinstance(
            broker_snapshot_builder, BrokerSnapshotBuilder
        ):
            raise TypeError("broker_snapshot_builder must be a BrokerSnapshotBuilder")
        if contract_resolver is not None and not callable(contract_resolver):
            raise TypeError("contract_resolver must be callable")
        if decision_context_resolver is not None and not callable(
            decision_context_resolver
        ):
            raise TypeError("decision_context_resolver must be callable")
        self.store = store
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._proposal_validator = proposal_validator
        self._a_grade_unlocked = a_grade_unlocked
        self._broker_snapshot_builder = broker_snapshot_builder
        self._contract_resolver = contract_resolver
        self._decision_context_resolver = decision_context_resolver

    def claim(self, approval_id: str) -> str:
        return self.store.claim(approval_id)

    def authorize(
        self,
        approval_id: str,
        token: str,
        broker_snapshot: Mapping[str, object],
        instruction_intent: Mapping[str, object],
    ) -> BridgeRecord:
        """Revalidate current account/risk/quotes before durable authorization."""

        if self._atomic_requery_available:
            snapshot = _detached_mapping(broker_snapshot)
            _reject_auth_material(snapshot)
            if snapshot.get("broker_snapshot_complete") is not True:
                raise BridgeBrokerSnapshotRejected(
                    "broker_snapshot_complete must be exactly true"
                )
            atomic_snapshot, atomic_gate = self._build_atomic_gate(
                _proposal_from_snapshot(snapshot)
            )
            quote_snapshot_id = atomic_snapshot.quote_batch_id
            if quote_snapshot_id is None:
                raise BridgeBrokerSnapshotRejected(
                    "QUOTE_INCOHERENT: atomic quote batch ID is missing"
                )
            return self.store._authorize_after_broker_gate(
                approval_id,
                token,
                atomic_gate.repriced_proposal,
                atomic_gate.quotes_observed_at,
                instruction_intent,
                snapshot_observed_at=atomic_snapshot.built_at,
                quote_snapshot_id=quote_snapshot_id,
                net_liquidation_usd=atomic_gate.net_liquidation_usd,
                contract_definitions_hash=atomic_gate.contract_definitions_hash,
                atomic_broker_gate=atomic_gate,
            )

        gate = self.validate_broker_snapshot(broker_snapshot)
        canonical = gate.validated_proposal.canonical_proposal
        return self.store._authorize_after_broker_gate(
            approval_id,
            token,
            canonical,
            gate.oldest_quote_at,
            instruction_intent,
            snapshot_observed_at=gate.snapshot_observed_at,
            quote_snapshot_id=gate.quote_snapshot_id,
            net_liquidation_usd=gate.net_liquidation_usd,
            contract_definitions_hash=gate.contract_definitions_hash,
        )

    def validate_broker_snapshot(
        self, broker_snapshot: Mapping[str, object]
    ) -> BrokerGateResult:
        if not isinstance(broker_snapshot, Mapping):
            raise BridgeBrokerSnapshotRejected("broker_snapshot must be an object")
        snapshot = _detached_mapping(broker_snapshot)
        _reject_auth_material(snapshot)
        if snapshot.get("broker_snapshot_complete") is not True:
            raise BridgeBrokerSnapshotRejected(
                "broker_snapshot_complete must be exactly true"
            )
        now = utc_datetime(self._clock(), field="clock result")
        observed_at = _snapshot_time(snapshot)
        _require_current("broker_snapshot.observed_at", now, observed_at)
        nlv = _net_liquidation(snapshot)
        open_combinations = _open_option_combinations(snapshot)
        if open_combinations:
            raise BridgeBrokerSnapshotRejected(
                "current broker snapshot contains open option positions"
            )
        _require_empty_boundary(
            snapshot,
            collection_keys=("working_orders",),
            count_keys=("working_order_count", "broker_working_order_count"),
            label="working orders",
        )
        _require_empty_boundary(
            snapshot,
            collection_keys=(
                "unsubmitted_instructions",
                "pending_review_instructions",
                "review_instructions",
            ),
            count_keys=(
                "unsubmitted_instruction_count",
                "pending_review_instruction_count",
            ),
            label="unsubmitted instructions",
        )
        snapshot_id = _nonblank(
            snapshot.get("quote_snapshot_id"),
            "broker_snapshot.quote_snapshot_id",
        )
        proposal = _proposal_from_snapshot(snapshot)
        contract_definitions_hash = _require_authoritative_contract_definitions(
            snapshot, proposal
        )
        validator_kwargs = {
            "account_equity": nlv,
            "open_combinations": open_combinations,
            "now": now,
            "quote_fresh_seconds": HARD_BROKER_SNAPSHOT_AGE_SECONDS,
            "expected_quote_snapshot_id": snapshot_id,
            "a_grade_unlocked": self._a_grade_unlocked,
        }
        try:
            # The canonical validator is non-replaceable at this security
            # boundary.  An injected validator may add checks or observe calls,
            # but cannot remove the production risk/payoff validation.
            validated = validate_proposal(proposal, **validator_kwargs)
            if self._proposal_validator is not validate_proposal:
                supplemental = self._proposal_validator(
                    proposal, **validator_kwargs
                )
                if not isinstance(supplemental, ValidatedProposal):
                    raise TypeError(
                        "supplemental proposal validator returned an invalid result"
                    )
        except (TypeError, ValueError) as exc:
            raise BridgeBrokerSnapshotRejected(
                f"current broker proposal rejected: {exc}"
            ) from exc
        quote_times = tuple(
            utc_datetime(item.observed_at, field="option quote observed_at")
            for item in validated.candidate.leg_quotes
        )
        if not quote_times:
            raise BridgeBrokerSnapshotRejected("validated proposal has no option quotes")
        oldest = min(quote_times)
        _require_current("oldest option quote", now, oldest)
        return BrokerGateResult(
            validated_proposal=validated,
            net_liquidation_usd=nlv,
            quote_snapshot_id=snapshot_id,
            snapshot_observed_at=observed_at,
            oldest_quote_at=oldest,
            contract_definitions_hash=contract_definitions_hash,
        )

    def reserve_external_call(self, approval_id: str, token: str) -> BridgeRecord:
        """Reserve the only permitted external call and return its frozen payload."""

        if not self._atomic_requery_available:
            current = self.store.get(approval_id)
            if current is not None and current.external_call_reserved:
                # It is safe to ask the store to surface the durable one-shot
                # rejection: it checks the attempt row before any legacy gate
                # could be used, so no unconfigured requery path can execute.
                return self.store.reserve_external_call(approval_id, token)
            raise BridgeStateError(
                "atomic reserve-time broker requery is unavailable; external call "
                "reservation is forbidden"
            )
        return self.store.reserve_external_call(
            approval_id,
            token,
            lock_time_requery=lambda proposal: self._build_atomic_gate(proposal)[1],
        )

    @property
    def _atomic_requery_available(self) -> bool:
        return bool(
            self._broker_snapshot_builder is not None
            and self._contract_resolver is not None
            and self._decision_context_resolver is not None
        )

    def _build_atomic_gate(
        self,
        proposal: Mapping[str, object],
    ) -> tuple[AtomicBrokerSnapshot, AtomicBrokerGateInput]:
        builder = self._broker_snapshot_builder
        contract_resolver = self._contract_resolver
        context_resolver = self._decision_context_resolver
        if builder is None or contract_resolver is None or context_resolver is None:
            raise BridgeStateError("atomic broker requery is not configured")
        detached_proposal = _detached_mapping(proposal)
        try:
            contracts = tuple(contract_resolver(detached_proposal))
        except Exception as exc:
            raise BridgeBrokerSnapshotRejected(
                "atomic option contract resolution failed"
            ) from exc
        if not contracts or not all(
            isinstance(contract, OptionContractRef) for contract in contracts
        ):
            raise BridgeBrokerSnapshotRejected(
                "atomic option contract resolution returned invalid contracts"
            )
        _require_contract_refs_match_proposal(detached_proposal, contracts)

        atomic_snapshot = builder.build(contracts)
        if not isinstance(atomic_snapshot, AtomicBrokerSnapshot):
            raise BridgeBrokerSnapshotRejected(
                "atomic broker snapshot builder returned an invalid result"
            )
        if not atomic_snapshot.verify_hash():
            raise BridgeBrokerSnapshotRejected(
                "atomic broker snapshot immutable hash mismatch"
            )
        if atomic_snapshot.status is not BrokerSnapshotStatus.COMPLETE:
            reasons = ",".join(atomic_snapshot.reason_codes) or "UNSPECIFIED"
            raise BridgeBrokerSnapshotRejected(
                f"{atomic_snapshot.status.value}: {reasons}"
            )

        current_state = _current_state_from_atomic_snapshot(atomic_snapshot)
        net_liquidation = _net_liquidation(current_state)
        open_combinations = _open_option_combinations(current_state)
        if open_combinations:
            raise BridgeBrokerSnapshotRejected(
                "current broker snapshot contains open option positions"
            )
        _require_empty_boundary(
            current_state,
            collection_keys=("working_orders",),
            count_keys=(),
            label="working orders",
        )
        _require_empty_boundary(
            current_state,
            collection_keys=("unsubmitted_instructions",),
            count_keys=(),
            label="unsubmitted instructions",
        )

        try:
            context = context_resolver()
        except Exception as exc:
            raise BridgeBrokerSnapshotRejected(
                "current Strategy NAV decision context is unavailable"
            ) from exc
        if not isinstance(context, BridgeDecisionContext):
            raise BridgeBrokerSnapshotRejected(
                "decision context resolver returned an invalid result"
            )
        nav = context.risk_engine.strategy_nav
        assert nav is not None
        if nav.observed_account_nlv is None:
            raise BridgeBrokerSnapshotRejected(
                "Strategy NAV snapshot lacks observed broker NLV reconciliation"
            )
        if nav.observed_account_nlv != net_liquidation:
            raise BridgeBrokerSnapshotRejected(
                "Strategy NAV observed account NLV does not match atomic broker state"
            )

        repriced = _reprice_from_atomic_snapshot(
            detached_proposal,
            contracts=contracts,
            snapshot=atomic_snapshot,
        )
        quote_snapshot_id = atomic_snapshot.quote_batch_id
        if quote_snapshot_id is None:
            raise BridgeBrokerSnapshotRejected(
                "QUOTE_INCOHERENT: atomic quote batch ID is missing"
            )
        now = utc_datetime(self._clock(), field="clock result")
        validator_kwargs = {
            "account_equity": net_liquidation,
            "open_combinations": open_combinations,
            "now": now,
            "quote_fresh_seconds": HARD_BROKER_SNAPSHOT_AGE_SECONDS,
            "expected_quote_snapshot_id": quote_snapshot_id,
            "a_grade_unlocked": self._a_grade_unlocked,
            "risk_engine": context.risk_engine,
            "authoritative_repricing": True,
            "time_policy": context.option_time_policy,
            "dte_exception_authority": context.dte_exception_authority,
        }
        try:
            validated = validate_proposal(repriced, **validator_kwargs)
            if self._proposal_validator is not validate_proposal:
                supplemental = self._proposal_validator(
                    repriced,
                    **validator_kwargs,
                )
                if not isinstance(supplemental, ValidatedProposal):
                    raise TypeError(
                        "supplemental proposal validator returned an invalid result"
                    )
        except (TypeError, ValueError) as exc:
            raise BridgeBrokerSnapshotRejected(
                f"current atomic broker proposal rejected: {exc}"
            ) from exc

        entry_time_decision = context.option_time_policy.evaluate_entry(
            proposal_id=validated.candidate.candidate_id,
            expiration=contracts[0].expiration,
            now=now,
            calendar=context.market_calendar,
            exception_authority=context.dte_exception_authority,
        )
        if not entry_time_decision.allowed:
            reasons = ",".join(entry_time_decision.reason_codes) or "UNSPECIFIED"
            raise BridgeBrokerSnapshotRejected(
                f"entry time policy rejected: {reasons}"
            )

        quote_times = tuple(
            utc_datetime(quote.observed_at, field="atomic option quote observed_at")
            for quote in atomic_snapshot.quotes
        )
        if not quote_times:
            raise BridgeBrokerSnapshotRejected(
                "QUOTE_INCOHERENT: atomic snapshot contains no option quotes"
            )
        oldest_quote = min(quote_times)
        _require_current("oldest atomic option quote", now, oldest_quote)
        contract_definitions_hash = _atomic_contract_definitions_hash(
            atomic_snapshot
        )
        atomic_gate = AtomicBrokerGateInput(
            snapshot_hash=atomic_snapshot.snapshot_hash,
            snapshot_payload=atomic_snapshot.hash_payload(),
            decision_bindings=context.bindings(
                entry_time_decision=entry_time_decision,
            ),
            repriced_proposal=validated.canonical_proposal,
            quotes_observed_at=oldest_quote,
            net_liquidation_usd=net_liquidation,
            contract_definitions_hash=contract_definitions_hash,
        )
        return atomic_snapshot, atomic_gate

    def complete(
        self,
        approval_id: str,
        token: str,
        execution_result: Mapping[str, object],
    ) -> BridgeRecord:
        """Persist a result only after the one-shot external boundary was crossed."""

        record = self.store.get(approval_id)
        if record is None:
            raise BridgeStateError("bridge request is not claimed")
        if record.status is not BridgeStatus.AUTHORIZED:
            raise BridgeStateError(
                f"expected AUTHORIZED, found terminal/current {record.status.value}"
            )
        if not record.external_call_reserved:
            raise BridgeStateError(
                "external call must be durably reserved before completion"
            )
        if record.proposal is None or record.instruction_intent is None:
            raise BridgeStateError("authorized bridge payload is missing")
        return self.store.complete(
            approval_id,
            token,
            record.proposal,
            record.instruction_intent,
            execution_result,
        )

    def execute_authorized(
        self,
        approval_id: str,
        token: str,
        creator: ReviewInstructionCreator,
    ) -> BridgeRecord:
        """Invoke one injected review creator only after destination preflight.

        Any exception, malformed result, or persistence ambiguity after the
        reservation is classified as ``UNKNOWN_OUTCOME`` and can never retry.
        The exception text is intentionally not persisted because it might
        contain third-party details.
        """

        method = getattr(creator, "create_review_instruction", None)
        if not callable(method):
            raise TypeError("creator must provide create_review_instruction")
        current = self.store.get(approval_id)
        if current is None:
            raise BridgeStateError("bridge request is not claimed")
        if current.status is not BridgeStatus.AUTHORIZED:
            raise BridgeStateError(
                f"expected AUTHORIZED, found terminal/current {current.status.value}"
            )
        self.store.require_creator_destination_contract()
        reserved = self.reserve_external_call(approval_id, token)
        if reserved.status is not BridgeStatus.AUTHORIZED:
            raise BridgeStateError("external call requires AUTHORIZED state")
        if reserved.proposal is None or reserved.instruction_intent is None:
            raise BridgeStateError("authorized bridge payload is missing")
        try:
            result = method(
                idempotency_key=approval_id,
                proposal=reserved.proposal,
                instruction_intent=reserved.instruction_intent,
                review_only=True,
            )
            if not isinstance(result, Mapping):
                raise TypeError("review creator result must be a mapping")
            return self.complete(approval_id, token, result)
        except Exception as exc:
            try:
                current = self.store.get(approval_id)
            except BridgeError:
                current = None
            if current is not None and current.status is BridgeStatus.AUTHORIZED:
                try:
                    self.store.fail_unknown_outcome(
                        approval_id,
                        token,
                        "external_result_uncertain",
                    )
                except BridgeError:
                    # The one-shot reservation remains durable even if the
                    # terminal status write itself cannot be confirmed.
                    pass
            raise BridgeUnknownOutcomeError(
                approval_id, "external_result_uncertain"
            ) from exc

    def fail(
        self,
        approval_id: str,
        token: str,
        reason: str,
        *,
        unknown_outcome: bool = False,
    ) -> BridgeRecord:
        if unknown_outcome:
            return self.store.fail_unknown_outcome(approval_id, token, reason)
        return self.store.fail(approval_id, token, reason)

    def expire_stranded_claim(self, approval_id: str) -> BridgeRecord:
        return self.store.expire_stranded_claim(approval_id)

    def get(self, approval_id: str) -> BridgeRecord | None:
        return self.store.get(approval_id)

    def status(self, approval_id: str) -> dict[str, object] | None:
        record = self.store.get(approval_id)
        if record is None:
            return None
        if record.unknown_outcome or (
            record.status is BridgeStatus.AUTHORIZED
            and record.external_call_reserved
        ):
            effective = CoordinatorStatus.UNKNOWN_OUTCOME
        else:
            effective = CoordinatorStatus(record.status.value)
        return {
            "approval_id": record.approval_id,
            "status": effective.value,
            "bridge_status": record.status.value,
            "external_call_reserved": record.external_call_reserved,
            "automatic_retry_allowed": False,
            "record": record.as_dict(),
        }


def _detached_mapping(value: Mapping[str, object]) -> dict[str, object]:
    detached = thaw_json(freeze_json(value))
    if not isinstance(detached, dict):
        raise BridgeBrokerSnapshotRejected("broker_snapshot must be an object")
    return detached


def _reject_auth_material(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            compact = "".join(character for character in str(key).casefold() if character.isalnum())
            if any(
                marker in compact
                for marker in (
                    "password",
                    "secret",
                    "credential",
                    "apikey",
                    "authtoken",
                    "bearertoken",
                    "connectorauth",
                    "accesstoken",
                    "refreshtoken",
                    "clientsecret",
                    "authorizationheader",
                    "sessiontoken",
                    "oauth",
                    "cookie",
                )
            ):
                raise BridgeBrokerSnapshotRejected(
                    "broker_snapshot cannot contain authentication material"
                )
            _reject_auth_material(item)
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for item in value:
            _reject_auth_material(item)


def _snapshot_time(snapshot: Mapping[str, object]) -> datetime:
    value = snapshot.get("observed_at")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            parsed = datetime.fromisoformat(
                text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
            )
        except ValueError as exc:
            raise BridgeBrokerSnapshotRejected(
                "broker_snapshot.observed_at must be an ISO datetime"
            ) from exc
    else:
        raise BridgeBrokerSnapshotRejected(
            "broker_snapshot.observed_at is required"
        )
    try:
        return utc_datetime(parsed, field="broker_snapshot.observed_at")
    except (TypeError, ValueError) as exc:
        raise BridgeBrokerSnapshotRejected(str(exc)) from exc


def _require_current(field: str, now: datetime, observed_at: datetime) -> None:
    age = Decimal(str((now - observed_at).total_seconds()))
    if age < 0:
        raise BridgeBrokerSnapshotRejected(f"{field} cannot be in the future")
    if age > HARD_BROKER_SNAPSHOT_AGE_SECONDS:
        raise BridgeBrokerSnapshotRejected(f"{field} is older than 5 seconds")


def _net_liquidation(snapshot: Mapping[str, object]) -> Decimal:
    candidates: list[tuple[str, object]] = []
    for key in ("net_liquidation_usd", "net_liquidation", "nlv_usd", "nlv"):
        if key in snapshot:
            candidates.append((f"broker_snapshot.{key}", snapshot[key]))
    account = snapshot.get("account")
    if account is not None:
        if not isinstance(account, Mapping):
            raise BridgeBrokerSnapshotRejected("broker_snapshot.account must be an object")
        for key in ("net_liquidation_usd", "net_liquidation", "nlv_usd", "nlv"):
            if key in account:
                candidates.append((f"broker_snapshot.account.{key}", account[key]))
    if not candidates:
        raise BridgeBrokerSnapshotRejected("current net liquidation value is required")
    parsed = tuple(_positive_decimal(value, field) for field, value in candidates)
    if any(value != parsed[0] for value in parsed[1:]):
        raise BridgeBrokerSnapshotRejected(
            "conflicting current net liquidation values"
        )
    return parsed[0]


def _open_option_combinations(snapshot: Mapping[str, object]) -> int:
    positions = snapshot.get("positions")
    if not isinstance(positions, Sequence) or isinstance(
        positions, (str, bytes, bytearray, memoryview)
    ):
        raise BridgeBrokerSnapshotRejected(
            "broker_snapshot.positions must be an array"
        )
    has_open_option = False
    for index, item in enumerate(positions):
        if not isinstance(item, Mapping):
            raise BridgeBrokerSnapshotRejected(f"positions[{index}] must be an object")
        quantity_values = [
            item[key]
            for key in ("position", "quantity", "contracts", "size")
            if key in item
        ]
        if not quantity_values:
            raise BridgeBrokerSnapshotRejected(
                f"positions[{index}] requires current quantity"
            )
        quantities = tuple(
            _decimal(value, f"positions[{index}] quantity")
            for value in quantity_values
        )
        if any(value != quantities[0] for value in quantities[1:]):
            raise BridgeBrokerSnapshotRejected(
                f"positions[{index}] has conflicting quantities"
            )
        if quantities[0] == 0:
            continue
        security_values = [
            item[key]
            for key in ("asset_class", "security_type", "sec_type")
            if key in item
        ]
        if not security_values:
            # Unknown nonzero inventory is not assumed safe.
            has_open_option = True
            continue
        securities = {_nonblank(value, f"positions[{index}] security").upper() for value in security_values}
        if len(securities) != 1:
            raise BridgeBrokerSnapshotRejected(
                f"positions[{index}] has conflicting security types"
            )
        if next(iter(securities)) in {"OPT", "OPTION", "BAG", "COMBO"}:
            has_open_option = True
    return 1 if has_open_option else 0


def _require_empty_boundary(
    snapshot: Mapping[str, object],
    *,
    collection_keys: tuple[str, ...],
    count_keys: tuple[str, ...],
    label: str,
) -> None:
    observed = False
    counts: list[int] = []
    for key in collection_keys:
        if key not in snapshot:
            continue
        observed = True
        value = snapshot[key]
        if not isinstance(value, Sequence) or isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            raise BridgeBrokerSnapshotRejected(f"{key} must be an array")
        counts.append(len(value))
    for key in count_keys:
        if key not in snapshot:
            continue
        observed = True
        value = snapshot[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise BridgeBrokerSnapshotRejected(f"{key} must be a nonnegative integer")
        counts.append(value)
    if not observed:
        raise BridgeBrokerSnapshotRejected(f"current {label} state is required")
    if any(count != counts[0] for count in counts[1:]):
        raise BridgeBrokerSnapshotRejected(f"conflicting {label} counts")
    if counts and counts[0] != 0:
        raise BridgeBrokerSnapshotRejected(f"current {label} must be empty")


def _proposal_from_snapshot(snapshot: Mapping[str, object]) -> Mapping[str, object]:
    values = [
        snapshot[key]
        for key in ("proposal", "repriced_proposal", "candidate")
        if key in snapshot
    ]
    if len(values) != 1 or not isinstance(values[0], Mapping):
        raise BridgeBrokerSnapshotRejected(
            "broker_snapshot requires exactly one proposal object"
        )
    return values[0]


_AUTHORITATIVE_CONTRACT_FIELDS = {
    "contract_id_ex",
    "underlying",
    "security_type",
    "expiration",
    "strike",
    "right",
    "multiplier",
    "currency",
    "standard_contract",
}


def _require_authoritative_contract_definitions(
    snapshot: Mapping[str, object],
    proposal: Mapping[str, object],
) -> str:
    """Bind every proposed leg to a complete Connector-derived OPT secdef."""

    if snapshot.get("contract_definitions_complete") is not True:
        raise BridgeBrokerSnapshotRejected(
            "contract_definitions_complete must be exactly true"
        )
    source = snapshot.get("contract_definitions_source")
    if source != "managed_connector":
        raise BridgeBrokerSnapshotRejected(
            "contract_definitions_source must be managed_connector"
        )
    definitions = snapshot.get("contract_definitions")
    if not isinstance(definitions, Sequence) or isinstance(
        definitions, (str, bytes, bytearray, memoryview)
    ):
        raise BridgeBrokerSnapshotRejected(
            "contract_definitions must be an array"
        )
    legs = proposal.get("legs")
    if not isinstance(legs, Sequence) or isinstance(
        legs, (str, bytes, bytearray, memoryview)
    ) or not legs:
        raise BridgeBrokerSnapshotRejected("proposal legs must be a nonempty array")

    authoritative: dict[str, dict[str, object]] = {}
    for index, item in enumerate(definitions):
        if not isinstance(item, Mapping):
            raise BridgeBrokerSnapshotRejected(
                f"contract_definitions[{index}] must be an object"
            )
        unknown = sorted(set(item).difference(_AUTHORITATIVE_CONTRACT_FIELDS))
        missing = sorted(_AUTHORITATIVE_CONTRACT_FIELDS.difference(item))
        if missing or unknown:
            details: list[str] = []
            if missing:
                details.append("missing " + ", ".join(missing))
            if unknown:
                details.append("unsupported " + ", ".join(unknown))
            raise BridgeBrokerSnapshotRejected(
                f"contract_definitions[{index}] has " + "; ".join(details)
            )
        if item.get("standard_contract") is not True:
            raise BridgeBrokerSnapshotRejected(
                f"contract_definitions[{index}] is not a standard contract"
            )
        binding = _contract_binding(
            item, f"contract_definitions[{index}]"
        )
        contract_id = str(binding["contract_id_ex"])
        if contract_id in authoritative:
            raise BridgeBrokerSnapshotRejected(
                "contract_definitions contains duplicate contract_id_ex"
            )
        binding["standard_contract"] = True
        authoritative[contract_id] = binding

    proposed: dict[str, dict[str, object]] = {}
    for index, item in enumerate(legs):
        if not isinstance(item, Mapping):
            raise BridgeBrokerSnapshotRejected(f"proposal legs[{index}] must be an object")
        binding = _contract_binding(item, f"proposal legs[{index}]")
        contract_id = str(binding["contract_id_ex"])
        if contract_id in proposed:
            raise BridgeBrokerSnapshotRejected(
                "proposal legs contain duplicate contract_id_ex"
            )
        proposed[contract_id] = binding

    if set(authoritative) != set(proposed):
        raise BridgeBrokerSnapshotRejected(
            "authoritative contract IDs do not exactly match proposal legs"
        )
    for contract_id, proposal_binding in proposed.items():
        authoritative_binding = dict(authoritative[contract_id])
        authoritative_binding.pop("standard_contract", None)
        if authoritative_binding != proposal_binding:
            raise BridgeBrokerSnapshotRejected(
                f"authoritative contract metadata mismatch for {contract_id}"
            )

    return canonical_hash(
        {
            "source": "managed_connector",
            "complete": True,
            "definitions": [
                authoritative[key] for key in sorted(authoritative)
            ],
        }
    )


def _contract_binding(
    value: Mapping[str, object],
    label: str,
) -> dict[str, object]:
    contract_id = _nonblank(value.get("contract_id_ex"), f"{label}.contract_id_ex")
    underlying = _nonblank(value.get("underlying"), f"{label}.underlying").upper()
    security_type = _nonblank(
        value.get("security_type"), f"{label}.security_type"
    ).upper()
    if security_type != "OPT":
        raise BridgeBrokerSnapshotRejected(f"{label}.security_type must be OPT")
    expiration_text = _nonblank(
        value.get("expiration"), f"{label}.expiration"
    )
    try:
        expiration = date.fromisoformat(expiration_text).isoformat()
    except ValueError as exc:
        raise BridgeBrokerSnapshotRejected(
            f"{label}.expiration must be an ISO date"
        ) from exc
    strike = _positive_decimal(value.get("strike"), f"{label}.strike")
    right = _nonblank(value.get("right"), f"{label}.right").upper()
    if right not in {"CALL", "PUT"}:
        raise BridgeBrokerSnapshotRejected(f"{label}.right must be CALL or PUT")
    multiplier = _positive_decimal(
        value.get("multiplier"), f"{label}.multiplier"
    )
    if multiplier != Decimal("100"):
        raise BridgeBrokerSnapshotRejected(
            f"{label}.multiplier must be 100 for a standard contract"
        )
    currency = _nonblank(value.get("currency"), f"{label}.currency").upper()
    if currency != "USD":
        raise BridgeBrokerSnapshotRejected(f"{label}.currency must be USD")
    return {
        "contract_id_ex": contract_id,
        "underlying": underlying,
        "security_type": "OPT",
        "expiration": expiration,
        "strike": format(strike, "f"),
        "right": right,
        "multiplier": "100",
        "currency": "USD",
    }


def _require_contract_refs_match_proposal(
    proposal: Mapping[str, object],
    contracts: Sequence[OptionContractRef],
) -> None:
    legs = proposal.get("legs")
    if not isinstance(legs, Sequence) or isinstance(
        legs, (str, bytes, bytearray, memoryview)
    ) or not legs:
        raise BridgeBrokerSnapshotRejected("proposal legs must be a nonempty array")
    by_external_id: dict[str, OptionContractRef] = {}
    for index, contract in enumerate(contracts):
        external_id = _nonblank(
            contract.contract_id_ex,
            f"resolved contracts[{index}].contract_id_ex",
        )
        if external_id in by_external_id:
            raise BridgeBrokerSnapshotRejected(
                "resolved contracts contain duplicate contract_id_ex"
            )
        by_external_id[external_id] = contract
    proposed_ids: set[str] = set()
    for index, item in enumerate(legs):
        if not isinstance(item, Mapping):
            raise BridgeBrokerSnapshotRejected(f"proposal legs[{index}] must be an object")
        proposal_binding = _contract_binding(item, f"proposal legs[{index}]")
        external_id = str(proposal_binding["contract_id_ex"])
        if external_id in proposed_ids:
            raise BridgeBrokerSnapshotRejected(
                "proposal legs contain duplicate contract_id_ex"
            )
        proposed_ids.add(external_id)
        contract = by_external_id.get(external_id)
        if contract is None:
            raise BridgeBrokerSnapshotRejected(
                f"resolved contract is missing for proposal leg {external_id}"
            )
        expected = {
            "contract_id_ex": external_id,
            "underlying": contract.symbol.strip().upper(),
            "security_type": "OPT",
            "expiration": contract.expiration.isoformat(),
            "strike": format(contract.strike, "f"),
            "right": "CALL" if contract.right == "C" else "PUT",
            "multiplier": format(Decimal(contract.multiplier), "f"),
            "currency": contract.currency.strip().upper(),
        }
        if proposal_binding != expected:
            raise BridgeBrokerSnapshotRejected(
                f"resolved contract metadata mismatch for {external_id}"
            )
        proposal_exchange = _nonblank(
            item.get("exchange"),
            f"proposal legs[{index}].exchange",
        ).upper()
        if proposal_exchange != contract.exchange.strip().upper():
            raise BridgeBrokerSnapshotRejected(
                f"resolved contract exchange mismatch for {external_id}"
            )
    if proposed_ids != set(by_external_id):
        raise BridgeBrokerSnapshotRejected(
            "resolved contract IDs do not exactly match proposal legs"
        )


def _current_state_from_atomic_snapshot(
    snapshot: AtomicBrokerSnapshot,
) -> dict[str, object]:
    result: dict[str, object] = {}
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
        ):
            raise BridgeBrokerSnapshotRejected(
                f"STATE_INCOMPLETE: atomic {component} state is unavailable"
            )
        result[component] = evidence.state
    return result


def _reprice_from_atomic_snapshot(
    proposal: Mapping[str, object],
    *,
    contracts: Sequence[OptionContractRef],
    snapshot: AtomicBrokerSnapshot,
) -> dict[str, object]:
    detached = _detached_mapping(proposal)
    legs = detached.get("legs")
    if not isinstance(legs, list) or not legs:
        raise BridgeBrokerSnapshotRejected("proposal legs must be a nonempty array")
    contracts_by_external_id = {
        contract.contract_id_ex: contract for contract in contracts
    }
    quotes_by_contract_id = {quote.contract_id: quote for quote in snapshot.quotes}
    if len(quotes_by_contract_id) != len(snapshot.quotes):
        raise BridgeBrokerSnapshotRejected(
            "QUOTE_INCOHERENT: duplicate quote contract identity"
        )
    batch_id = snapshot.quote_batch_id
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise BridgeBrokerSnapshotRejected(
            "QUOTE_INCOHERENT: quote batch ID is missing"
        )
    optional_quote_fields = (
        "last",
        "implied_volatility",
        "iv",
        "volume",
        "open_interest",
        "oi",
    )
    repriced_legs: list[dict[str, object]] = []
    for index, item in enumerate(legs):
        if not isinstance(item, dict):
            raise BridgeBrokerSnapshotRejected(f"proposal legs[{index}] must be an object")
        external_id = _nonblank(
            item.get("contract_id_ex"),
            f"proposal legs[{index}].contract_id_ex",
        )
        contract = contracts_by_external_id.get(external_id)
        if contract is None:
            raise BridgeBrokerSnapshotRejected(
                f"resolved contract is missing for proposal leg {external_id}"
            )
        quote = quotes_by_contract_id.get(contract.contract_id)
        if quote is None or quote.bid is None or quote.ask is None:
            raise BridgeBrokerSnapshotRejected(
                f"QUOTE_INCOHERENT: executable quote is missing for {external_id}"
            )
        row = dict(item)
        for field in optional_quote_fields:
            row.pop(field, None)
        row.update(
            {
                "bid": format(quote.bid, "f"),
                "ask": format(quote.ask, "f"),
                "quote_time": datetime_text(quote.observed_at),
                "quote_snapshot_id": batch_id,
            }
        )
        if quote.last is not None:
            row["last"] = format(quote.last, "f")
        if quote.implied_volatility is not None:
            row["implied_volatility"] = format(quote.implied_volatility, "f")
        if quote.volume is not None:
            row["volume"] = quote.volume
        if quote.open_interest is not None:
            row["open_interest"] = quote.open_interest
        repriced_legs.append(row)
    if len(quotes_by_contract_id) != len(repriced_legs):
        raise BridgeBrokerSnapshotRejected(
            "QUOTE_INCOHERENT: quote contract IDs do not exactly match proposal legs"
        )
    detached["quote_snapshot_id"] = batch_id
    detached["legs"] = repriced_legs
    return detached


def _atomic_contract_definitions_hash(snapshot: AtomicBrokerSnapshot) -> str:
    definitions: list[dict[str, object]] = []
    for item in sorted(
        snapshot.secdef_evidence,
        key=lambda evidence: evidence.contract_id,
    ):
        if (
            not item.stable
            or not item.standard_contract
            or item.adjusted
            or item.post_identity is None
            or item.post_hash is None
            or not item.post_source
        ):
            raise BridgeBrokerSnapshotRejected(
                "MUTATED_DURING_BUILD: authoritative secdef proof is incomplete"
            )
        definitions.append(
            {
                "contract_id": item.contract_id,
                "identity": item.post_identity,
                "identity_hash": item.post_hash,
                "source": item.post_source,
                "standard_contract": True,
                "adjusted": False,
            }
        )
    if not definitions:
        raise BridgeBrokerSnapshotRejected(
            "MUTATED_DURING_BUILD: no authoritative secdef proof"
        )
    return canonical_hash(
        {
            "schema": "options_copilot.atomic_contract_definitions.v1",
            "definitions": definitions,
        }
    )


def _positive_decimal(value: object, field: str) -> Decimal:
    result = _decimal(value, field)
    if result <= 0:
        raise BridgeBrokerSnapshotRejected(f"{field} must be positive")
    return result


def _decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool):
        raise BridgeBrokerSnapshotRejected(f"{field} must be numeric")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BridgeBrokerSnapshotRejected(f"{field} must be finite numeric") from exc
    if not result.is_finite():
        raise BridgeBrokerSnapshotRejected(f"{field} must be finite numeric")
    return result


def _nonblank(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BridgeBrokerSnapshotRejected(f"{field} must be a nonblank string")
    return value.strip()


__all__ = [
    "BrokerGateResult",
    "BridgeBrokerSnapshotRejected",
    "BridgeDecisionContext",
    "BridgeUnknownOutcomeError",
    "CoordinatorStatus",
    "HARD_BROKER_SNAPSHOT_AGE_SECONDS",
    "LocalCodexBridgeCoordinator",
    "ReviewInstructionCreator",
]
