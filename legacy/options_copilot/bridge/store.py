"""Durable local state machine for a review-only Codex handoff.

This module intentionally imports no IBKR gateway, managed Connector, or
credential code.  The only upstream authority it accepts is an already-open
``ProposalApprovalStore``.  The external workflow receives an opaque bearer
token once; only its SHA-256 digest is ever persisted.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
import sqlite3
import threading

from options_copilot.approval import ApprovalAuthorityBinding, ProposalApprovalStore
from options_copilot.market import US_OPTIONS_TIMEZONE
from options_copilot.risk.time_policy import (
    NORMAL_MAXIMUM_ENTRY_DTE,
    PERMANENT_MINIMUM_ENTRY_DTE,
    TimePolicyDecision,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


MAX_QUOTE_AGE_SECONDS = Decimal("5")
CURRENT_AUTHORITY_PROOF_MAX_AGE_SECONDS = Decimal("5")
CURRENT_AUTHORITY_PROOF_SCHEMA = (
    "options_copilot.bridge.current_authority_proof.v1"
)
SCHEMA_VERSION = 5
UNKNOWN_OUTCOME_REASON_PREFIX = "UNKNOWN_OUTCOME:"
TRUSTED_APPROVAL_ISSUER = "options_copilot_gui"
_FAILURE_REASON_CODES = frozenset(
    {
        "operator_cancelled",
        "external_review_failed",
        "claim_abandoned_expired",
    }
)
_UNKNOWN_OUTCOME_CODES = frozenset(
    {"external_result_uncertain", "connector_result_uncertain"}
)
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
_CURRENT_AUTHORITY_PROOF_FIELDS = frozenset(
    {
        "schema",
        "status",
        "checked_at",
        "ranking_snapshot_id",
        "candidate_id",
        "proposal_hash",
        "snapshot_hash",
        "current_policy_version",
        "current_policy_hash",
        "policy_authority_marker_hash",
        "cost_version",
        "cost_hash",
        "risk_contract_hash",
        "risk_authority_version",
        "risk_authority_marker_hash",
        "strategy_nav_content_hash",
        "strategy_nav_contract_hash",
        "strategy_nav_ledger_head_hash",
    }
)
_SAFE_COLUMNS = """
    sequence, approval_id, status, claimed_at, authorized_at,
    quotes_observed_at, proposal_hash, proposal_json, intent_hash,
    intent_json, limit_price, completed_at, execution_hash,
    execution_json, instruction_id, deep_link, failed_at, failure_reason
    , (SELECT attempt.started_at
       FROM codex_bridge_external_attempts AS attempt
       WHERE attempt.approval_id = codex_bridge_requests.approval_id)
      AS external_call_started_at
    , (SELECT gate.snapshot_observed_at
       FROM codex_bridge_broker_gates AS gate
       WHERE gate.approval_id = codex_bridge_requests.approval_id)
      AS snapshot_observed_at
    , (SELECT gate.verified_at
       FROM codex_bridge_broker_gates AS gate
       WHERE gate.approval_id = codex_bridge_requests.approval_id)
      AS broker_gate_verified_at
    , (SELECT atomic.snapshot_hash
       FROM codex_bridge_atomic_authorization_gates AS atomic
       WHERE atomic.approval_id = codex_bridge_requests.approval_id)
      AS authorized_broker_snapshot_hash
    , (SELECT reserve.snapshot_hash
       FROM codex_bridge_reserve_gates AS reserve
       WHERE reserve.approval_id = codex_bridge_requests.approval_id)
      AS reserve_broker_snapshot_hash
    , (SELECT reserve.verified_at
       FROM codex_bridge_reserve_gates AS reserve
       WHERE reserve.approval_id = codex_bridge_requests.approval_id)
      AS reserve_gate_verified_at
    , (SELECT reserve.decision_bindings_hash
       FROM codex_bridge_reserve_gates AS reserve
       WHERE reserve.approval_id = codex_bridge_requests.approval_id)
      AS reserve_decision_bindings_hash
    , (SELECT authority.checked_at
       FROM codex_bridge_claim_authority_proofs AS authority
       WHERE authority.approval_id = codex_bridge_requests.approval_id)
      AS current_authority_checked_at
    , (SELECT authority.authority_binding_hash
       FROM codex_bridge_claim_authority_proofs AS authority
       WHERE authority.approval_id = codex_bridge_requests.approval_id)
      AS current_authority_binding_hash
    , (SELECT authority.proof_json
       FROM codex_bridge_claim_authority_proofs AS authority
       WHERE authority.approval_id = codex_bridge_requests.approval_id)
      AS current_authority_proof_json
    , (SELECT authority.proof_hash
       FROM codex_bridge_claim_authority_proofs AS authority
       WHERE authority.approval_id = codex_bridge_requests.approval_id)
      AS current_authority_proof_hash
"""


class BridgeError(RuntimeError):
    """Base class for fail-closed bridge errors."""


class BridgeAlreadyClaimed(BridgeError):
    """The approval already belongs to a bridge claimant."""


class BridgeActiveHandoffExists(BridgeAlreadyClaimed):
    """Another approval already owns the single active handoff slot."""


class BridgeTokenError(BridgeError):
    """The supplied claim token is invalid."""


class BridgeStateError(BridgeError):
    """The requested operation is not legal from the current state."""


class BridgeExternalCallAlreadyAttempted(BridgeStateError):
    """The approval has already crossed the external-call boundary."""


class BridgeValidationError(BridgeError):
    """A handoff payload violates the review-only contract."""


class BridgeApprovalRejected(BridgeValidationError):
    """The underlying GUI approval did not validate or consume."""

    def __init__(self, reasons: Sequence[str]) -> None:
        self.reasons = tuple(str(reason) for reason in reasons)
        rendered = ", ".join(self.reasons) or "approval rejected"
        super().__init__(rendered)


class BridgeStatus(str, Enum):
    CLAIMED = "CLAIMED"
    AUTHORIZED = "AUTHORIZED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class CurrentAuthorityProof:
    """Structured claim-time proof that frozen approval authority is current."""

    schema: str
    status: str
    checked_at: datetime
    ranking_snapshot_id: str
    candidate_id: str
    proposal_hash: str
    snapshot_hash: str
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    cost_version: str
    cost_hash: str
    risk_contract_hash: str
    risk_authority_version: str
    risk_authority_marker_hash: str
    strategy_nav_content_hash: str
    strategy_nav_contract_hash: str
    strategy_nav_ledger_head_hash: str

    def __post_init__(self) -> None:
        if self.schema != CURRENT_AUTHORITY_PROOF_SCHEMA:
            raise BridgeValidationError("current_authority_proof_schema_invalid")
        if self.status != "CURRENT":
            raise BridgeValidationError("current_authority_status_not_current")
        object.__setattr__(
            self,
            "checked_at",
            _authority_timestamp("current_authority.checked_at", self.checked_at),
        )
        for name in (
            "ranking_snapshot_id",
            "candidate_id",
            "current_policy_version",
            "cost_version",
            "risk_authority_version",
        ):
            object.__setattr__(
                self,
                name,
                _authority_identifier(f"current_authority.{name}", getattr(self, name)),
            )
        for name in (
            "proposal_hash",
            "snapshot_hash",
            "current_policy_hash",
            "policy_authority_marker_hash",
            "cost_hash",
            "risk_contract_hash",
            "risk_authority_marker_hash",
            "strategy_nav_content_hash",
            "strategy_nav_contract_hash",
            "strategy_nav_ledger_head_hash",
        ):
            object.__setattr__(
                self,
                name,
                _digest(f"current_authority.{name}", getattr(self, name)),
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "status": self.status,
            "checked_at": datetime_text(self.checked_at),
            "ranking_snapshot_id": self.ranking_snapshot_id,
            "candidate_id": self.candidate_id,
            "proposal_hash": self.proposal_hash,
            "snapshot_hash": self.snapshot_hash,
            "current_policy_version": self.current_policy_version,
            "current_policy_hash": self.current_policy_hash,
            "policy_authority_marker_hash": self.policy_authority_marker_hash,
            "cost_version": self.cost_version,
            "cost_hash": self.cost_hash,
            "risk_contract_hash": self.risk_contract_hash,
            "risk_authority_version": self.risk_authority_version,
            "risk_authority_marker_hash": self.risk_authority_marker_hash,
            "strategy_nav_content_hash": self.strategy_nav_content_hash,
            "strategy_nav_contract_hash": self.strategy_nav_contract_hash,
            "strategy_nav_ledger_head_hash": self.strategy_nav_ledger_head_hash,
        }


@dataclass(frozen=True, slots=True)
class BridgeRecord:
    """Public bridge view.  It intentionally has no token or token digest."""

    sequence: int
    approval_id: str
    status: BridgeStatus
    claimed_at: datetime
    authorized_at: datetime | None = None
    quotes_observed_at: datetime | None = None
    proposal_hash: str | None = None
    proposal: Mapping[str, object] | None = None
    intent_hash: str | None = None
    instruction_intent: Mapping[str, object] | None = None
    limit_price: Decimal | None = None
    completed_at: datetime | None = None
    execution_hash: str | None = None
    execution_result: Mapping[str, object] | None = None
    instruction_id: str | None = None
    deep_link: str | None = None
    failed_at: datetime | None = None
    failure_reason: str | None = None
    external_call_started_at: datetime | None = None
    snapshot_observed_at: datetime | None = None
    broker_gate_verified_at: datetime | None = None
    authorized_broker_snapshot_hash: str | None = None
    reserve_broker_snapshot_hash: str | None = None
    reserve_gate_verified_at: datetime | None = None
    reserve_decision_bindings_hash: str | None = None
    current_authority_checked_at: datetime | None = None
    current_authority_binding_hash: str | None = None
    current_authority_proof_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "claimed_at", utc_datetime(self.claimed_at))
        for name in (
            "authorized_at",
            "quotes_observed_at",
            "completed_at",
            "failed_at",
            "external_call_started_at",
            "snapshot_observed_at",
            "broker_gate_verified_at",
            "reserve_gate_verified_at",
            "current_authority_checked_at",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, utc_datetime(value, field=name))
        for name in ("proposal", "instruction_intent", "execution_result"):
            value = getattr(self, name)
            if value is not None:
                frozen = freeze_json(value)
                if not isinstance(frozen, Mapping):
                    raise TypeError(f"{name} must be a mapping")
                object.__setattr__(self, name, frozen)

    @property
    def intent(self) -> Mapping[str, object] | None:
        """Concise alias used by callers displaying the frozen instruction."""

        return self.instruction_intent

    @property
    def state(self) -> BridgeStatus:
        return self.status

    @property
    def unknown_outcome(self) -> bool:
        return bool(
            self.status is BridgeStatus.FAILED
            and self.failure_reason is not None
            and self.failure_reason.startswith(UNKNOWN_OUTCOME_REASON_PREFIX)
        )

    @property
    def external_call_reserved(self) -> bool:
        return self.external_call_started_at is not None

    @property
    def broker_gate_verified(self) -> bool:
        return self.broker_gate_verified_at is not None

    @property
    def current_authority_verified(self) -> bool:
        return self.current_authority_proof_hash is not None

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "approval_id": self.approval_id,
            "status": self.status.value,
            "claimed_at": datetime_text(self.claimed_at),
            "authorized_at": _optional_datetime_text(self.authorized_at),
            "quotes_observed_at": _optional_datetime_text(self.quotes_observed_at),
            "proposal_hash": self.proposal_hash,
            "proposal": None if self.proposal is None else thaw_json(self.proposal),
            "intent_hash": self.intent_hash,
            "instruction_intent": (
                None
                if self.instruction_intent is None
                else thaw_json(self.instruction_intent)
            ),
            "limit_price": (
                None if self.limit_price is None else format(self.limit_price, "f")
            ),
            "completed_at": _optional_datetime_text(self.completed_at),
            "execution_hash": self.execution_hash,
            "execution_result": None,
            "instruction_id": None,
            "deep_link": None,
            "failed_at": _optional_datetime_text(self.failed_at),
            "failure_reason": self.failure_reason,
            "external_call_started_at": _optional_datetime_text(
                self.external_call_started_at
            ),
            "snapshot_observed_at": _optional_datetime_text(
                self.snapshot_observed_at
            ),
            "broker_gate_verified_at": _optional_datetime_text(
                self.broker_gate_verified_at
            ),
            "broker_gate_verified": self.broker_gate_verified,
            "authorized_broker_snapshot_hash": self.authorized_broker_snapshot_hash,
            "reserve_broker_snapshot_hash": self.reserve_broker_snapshot_hash,
            "reserve_gate_verified_at": _optional_datetime_text(
                self.reserve_gate_verified_at
            ),
            "reserve_decision_bindings_hash": self.reserve_decision_bindings_hash,
            "current_authority_checked_at": _optional_datetime_text(
                self.current_authority_checked_at
            ),
            "current_authority_binding_hash": self.current_authority_binding_hash,
            "current_authority_proof_hash": self.current_authority_proof_hash,
            "current_authority_verified": self.current_authority_verified,
            "unknown_outcome": self.unknown_outcome,
            "automatic_retry_allowed": False,
        }


@dataclass(frozen=True, slots=True)
class AtomicBrokerGateInput:
    """Coordinator-built atomic broker proof passed into a locked store gate."""

    snapshot_hash: str
    snapshot_payload: Mapping[str, object]
    decision_bindings: Mapping[str, object]
    repriced_proposal: Mapping[str, object]
    quotes_observed_at: datetime
    net_liquidation_usd: Decimal
    contract_definitions_hash: str

    def __post_init__(self) -> None:
        snapshot_hash = _digest("snapshot_hash", self.snapshot_hash)
        definitions_hash = _digest(
            "contract_definitions_hash", self.contract_definitions_hash
        )
        snapshot = _canonical_mapping("snapshot_payload", self.snapshot_payload)
        if canonical_hash(snapshot) != snapshot_hash:
            raise BridgeValidationError("atomic broker snapshot hash mismatch")
        if snapshot.get("status") != "COMPLETE":
            raise BridgeValidationError("atomic broker snapshot is not COMPLETE")
        bindings = _normalize_decision_bindings(self.decision_bindings)
        proposal = _canonical_mapping("repriced_proposal", self.repriced_proposal)
        observed_at = utc_datetime(
            self.quotes_observed_at, field="quotes_observed_at"
        )
        nlv = _decimal("net_liquidation_usd", self.net_liquidation_usd)
        if nlv <= 0:
            raise BridgeValidationError("net_liquidation_usd must be positive")
        if _atomic_account_nlv(snapshot) != nlv:
            raise BridgeValidationError(
                "net_liquidation_usd does not match atomic account state"
            )
        object.__setattr__(self, "snapshot_hash", snapshot_hash)
        object.__setattr__(self, "snapshot_payload", freeze_json(snapshot))
        object.__setattr__(self, "decision_bindings", freeze_json(bindings))
        object.__setattr__(self, "repriced_proposal", freeze_json(proposal))
        object.__setattr__(self, "quotes_observed_at", observed_at)
        object.__setattr__(self, "net_liquidation_usd", nlv)
        object.__setattr__(self, "contract_definitions_hash", definitions_hash)

    @property
    def decision_bindings_hash(self) -> str:
        return canonical_hash(self.decision_bindings)

    @property
    def proposal_hash(self) -> str:
        return canonical_hash(self.repriced_proposal)


class CodexBridgeStore:
    """Independent SQLite state machine for one external review handoff.

    ``claim`` is the only method that returns a secret.  ``authorize`` binds a
    fresh executable proposal and a LIMIT/DAY intent.  ``complete`` accepts
    only a safe review result and consumes the GUI approval exactly once.
    """

    def __init__(
        self,
        path: str | Path,
        approval_store: ProposalApprovalStore,
        *,
        clock: Callable[[], datetime] | None = None,
        current_authority_validator: (
            Callable[
                [ApprovalAuthorityBinding, datetime],
                CurrentAuthorityProof | Mapping[str, object],
            ]
            | None
        ) = None,
    ) -> None:
        if not all(
            callable(getattr(approval_store, method, None))
            for method in ("get", "get_authority_binding", "validate", "consume")
        ):
            raise TypeError(
                "approval_store must provide get, get_authority_binding, validate, and consume"
            )
        if current_authority_validator is not None and not callable(
            current_authority_validator
        ):
            raise TypeError("current_authority_validator must be callable or None")
        self.path = Path(path)
        approval_path = getattr(approval_store, "path", None)
        if approval_path is not None and self.path.resolve() == Path(approval_path).resolve():
            raise ValueError("bridge database must be independent from approval database")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._approval_store = approval_store
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._current_authority_validator = current_authority_validator
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA busy_timeout=10000")
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            synchronous = int(
                self._connection.execute("PRAGMA synchronous").fetchone()[0]
            )
            self._synchronous = {
                0: "off",
                1: "normal",
                2: "full",
                3: "extra",
            }.get(synchronous, str(synchronous))
            self._migrate()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "CodexBridgeStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        return self._synchronous

    @property
    def schema_version(self) -> int:
        self._ensure_open()
        with self._lock:
            return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def claim(self, approval_id: str) -> str:
        """Claim an unconsumed approval and return its one-time bearer token."""

        _identifier("approval_id", approval_id)
        try:
            with self._transaction():
                if self._connection.execute(
                    "SELECT 1 FROM codex_bridge_requests WHERE approval_id=?",
                    (approval_id,),
                ).fetchone() is not None:
                    raise BridgeAlreadyClaimed(
                        "approval is already claimed and cannot be reclaimed"
                    )
                if self._connection.execute(
                    "SELECT 1 FROM codex_bridge_requests "
                    "WHERE status IN ('CLAIMED', 'AUTHORIZED') LIMIT 1"
                ).fetchone() is not None:
                    raise BridgeActiveHandoffExists(
                        "another approval already owns the active bridge handoff"
                    )

                now = self._trusted_now()
                approval = self._approval_store.get(approval_id)
                if approval is None:
                    raise BridgeApprovalRejected(("approval_not_found",))
                if approval.approved_by != TRUSTED_APPROVAL_ISSUER:
                    raise BridgeApprovalRejected(
                        ("approval_not_from_options_copilot_gui",)
                    )
                validation = self._approval_store.validate(
                    approval_id,
                    approval.proposal,
                    checked_at=now,
                    require_authority_binding=True,
                )
                if not validation.valid:
                    raise BridgeApprovalRejected(validation.reasons)
                binding = self._approval_store.get_authority_binding(approval_id)
                if binding is None:
                    raise BridgeApprovalRejected(("approval_authority_unbound",))
                authority_proof = self._validate_current_authority(
                    binding,
                    checked_at=now,
                )

                token = secrets.token_urlsafe(32)
                token_hash = _token_digest(token)
                self._connection.execute(
                    """
                    INSERT INTO codex_bridge_requests(
                        approval_id, status, token_hash, claimed_at
                    ) VALUES (?, 'CLAIMED', ?, ?)
                    """,
                    (approval_id, token_hash, datetime_text(now)),
                )
                self._connection.execute(
                    """
                    INSERT INTO codex_bridge_claim_authority_proofs(
                        approval_id, checked_at, authority_binding_hash,
                        proof_json, proof_hash
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    _claim_authority_audit_values(
                        approval_id,
                        binding,
                        authority_proof,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            existing = self.get(approval_id)
            if existing is not None:
                raise BridgeAlreadyClaimed(
                    "approval is already claimed and cannot be reclaimed"
                ) from exc
            active = self.list_pending(limit=1)
            if active:
                raise BridgeActiveHandoffExists(
                    "another approval already owns the active bridge handoff"
                ) from exc
            raise BridgeError("could not create a unique bridge claim") from exc
        return token

    def _validate_current_authority(
        self,
        binding: ApprovalAuthorityBinding,
        *,
        checked_at: datetime,
    ) -> CurrentAuthorityProof:
        validator = self._current_authority_validator
        if validator is None:
            raise BridgeApprovalRejected(
                ("current_authority_validator_unavailable",)
            )
        try:
            supplied = validator(binding, checked_at)
        except Exception as exc:
            raise BridgeApprovalRejected(
                ("current_authority_validator_failed",)
            ) from exc
        try:
            return _validate_current_authority_proof(
                supplied,
                binding=binding,
                checked_at=checked_at,
            )
        except BridgeValidationError as exc:
            raise BridgeApprovalRejected((str(exc),)) from exc

    def authorize(
        self,
        approval_id: str,
        token: str,
        repriced_proposal: Mapping[str, object],
        quotes_observed_at: datetime,
        instruction_intent: Mapping[str, object],
    ) -> BridgeRecord:
        """Reject direct authorization that has no current broker-state gate.

        Authorization is a coordinator operation because a proposal/quote pair
        alone cannot prove current NLV, positions, working orders, or pending
        instructions.  Keeping this legacy-shaped method fail-closed prevents a
        caller from manufacturing an executable ``AUTHORIZED`` row through the
        lower-level persistence object.
        """

        del approval_id, token, repriced_proposal, quotes_observed_at
        del instruction_intent
        raise BridgeStateError(
            "direct store authorization is forbidden; use "
            "LocalCodexBridgeCoordinator.authorize with a complete broker snapshot"
        )

    def _authorize_after_broker_gate(
        self,
        approval_id: str,
        token: str,
        repriced_proposal: Mapping[str, object],
        quotes_observed_at: datetime,
        instruction_intent: Mapping[str, object],
        *,
        snapshot_observed_at: datetime,
        quote_snapshot_id: str,
        net_liquidation_usd: Decimal,
        contract_definitions_hash: str,
        atomic_broker_gate: AtomicBrokerGateInput | None = None,
    ) -> BridgeRecord:
        """Persist authorization and coordinator broker-gate proof atomically.

        This is deliberately private.  The coordinator calls it only after its
        complete snapshot and ``validate_proposal`` gates succeed.  Reservation
        independently requires and verifies the append-only proof written here.
        """

        return self._persist_authorization(
            approval_id,
            token,
            repriced_proposal,
            quotes_observed_at,
            instruction_intent,
            broker_gate=(
                snapshot_observed_at,
                quote_snapshot_id,
                net_liquidation_usd,
                contract_definitions_hash,
            ),
            atomic_broker_gate=atomic_broker_gate,
        )

    def _authorize_persistence_only_for_test(
        self,
        approval_id: str,
        token: str,
        repriced_proposal: Mapping[str, object],
        quotes_observed_at: datetime,
        instruction_intent: Mapping[str, object],
    ) -> BridgeRecord:
        """Test hook that persists payload validation but no executable gate.

        Rows produced here intentionally cannot cross ``reserve_external_call``.
        It exists only so storage-contract tests can exercise validation without
        pretending that a broker snapshot was checked.
        """

        return self._persist_authorization(
            approval_id,
            token,
            repriced_proposal,
            quotes_observed_at,
            instruction_intent,
            broker_gate=None,
            atomic_broker_gate=None,
        )

    def _persist_authorization(
        self,
        approval_id: str,
        token: str,
        repriced_proposal: Mapping[str, object],
        quotes_observed_at: datetime,
        instruction_intent: Mapping[str, object],
        *,
        broker_gate: tuple[datetime, str, Decimal, str] | None,
        atomic_broker_gate: AtomicBrokerGateInput | None,
    ) -> BridgeRecord:
        """Validate and persist one authorization, optionally with gate proof."""

        _identifier("approval_id", approval_id)
        with self._transaction():
            # Sample time only after BEGIN IMMEDIATE owns the writer lock.  A
            # busy-lock wait must not preserve a pre-wait freshness decision.
            now = self._trusted_now()
            row = self._internal_row(approval_id)
            self._require_token(row, token)
            self._require_state(row, BridgeStatus.CLAIMED)
            if broker_gate is not None:
                self._require_claim_authority_audit_locked(approval_id)

            observed_at = utc_datetime(quotes_observed_at, field="quotes_observed_at")
            _require_fresh_quote(now, observed_at)
            proposal = _canonical_mapping("repriced_proposal", repriced_proposal)
            executable_price = _executable_net_price(proposal)
            current_cost_usd = _executable_cost_usd(proposal)
            intent, limit_price = _normalize_instruction_intent(instruction_intent)
            _bind_intent_to_proposal(intent, proposal)
            _require_limit_matches(intent, limit_price, executable_price)
            proposal_hash = canonical_hash(proposal)
            intent_hash = canonical_hash(intent)

            gate_values: tuple[datetime, str, Decimal, str] | None = None
            if broker_gate is not None:
                snapshot_at = utc_datetime(
                    broker_gate[0], field="snapshot_observed_at"
                )
                _require_fresh_observation(
                    now, snapshot_at, label="broker snapshot"
                )
                snapshot_id = _gate_identifier(
                    "quote_snapshot_id", broker_gate[1]
                )
                gate_nlv = _decimal(
                    "net_liquidation_usd", broker_gate[2]
                )
                if gate_nlv <= 0:
                    raise BridgeValidationError(
                        "net_liquidation_usd gate value must be positive"
                    )
                definitions_hash = str(broker_gate[3])
                if re.fullmatch(r"[0-9a-f]{64}", definitions_hash) is None:
                    raise BridgeValidationError(
                        "contract_definitions_hash gate value must be SHA-256 hex"
                    )
                gate_values = (
                    snapshot_at,
                    snapshot_id,
                    gate_nlv,
                    definitions_hash,
                )

            validation = self._approval_store.validate(
                approval_id,
                proposal,
                current_cost_usd=current_cost_usd,
                require_authority_binding=True,
            )
            if not validation.valid:
                raise BridgeApprovalRejected(validation.reasons)
            # The approval store is authoritative for immutable legs, risk,
            # material fields, and its configured (at most $5) adverse bound.
            if validation.approval is None:
                raise BridgeApprovalRejected(("approval_not_found",))
            if validation.adverse_change_usd is None:
                raise BridgeApprovalRejected(("adverse_change_unavailable",))
            if (
                validation.adverse_change_usd
                > validation.approval.adverse_tolerance_usd
            ):
                raise BridgeApprovalRejected(("adverse_tolerance_exceeded",))

            cursor = self._connection.execute(
                """
                UPDATE codex_bridge_requests
                SET status='AUTHORIZED', authorized_at=?, quotes_observed_at=?,
                    proposal_hash=?, proposal_json=?, intent_hash=?,
                    intent_json=?, limit_price=?
                WHERE approval_id=? AND status='CLAIMED'
                """,
                (
                    datetime_text(now),
                    datetime_text(observed_at),
                    proposal_hash,
                    canonical_json(proposal),
                    intent_hash,
                    canonical_json(intent),
                    format(limit_price, "f"),
                    approval_id,
                ),
            )
            if cursor.rowcount != 1:
                raise BridgeStateError("claim was not authorizable")
            if gate_values is not None:
                snapshot_at, snapshot_id, gate_nlv, definitions_hash = gate_values
                gate_payload = _broker_gate_payload(
                    approval_id=approval_id,
                    verified_at=now,
                    snapshot_observed_at=snapshot_at,
                    quotes_observed_at=observed_at,
                    proposal_hash=proposal_hash,
                    quote_snapshot_id=snapshot_id,
                    net_liquidation_usd=gate_nlv,
                    contract_definitions_hash=definitions_hash,
                )
                self._connection.execute(
                    """
                    INSERT INTO codex_bridge_broker_gates(
                        approval_id, verified_at, snapshot_observed_at,
                        quotes_observed_at, proposal_hash, quote_snapshot_id,
                        net_liquidation_usd, contract_definitions_hash,
                        broker_snapshot_complete,
                        open_option_combinations, working_order_count,
                        unsubmitted_instruction_count, proof_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 0, 0, ?)
                    """,
                    (
                        approval_id,
                        datetime_text(now),
                        datetime_text(snapshot_at),
                        datetime_text(observed_at),
                        proposal_hash,
                        snapshot_id,
                        format(gate_nlv, "f"),
                        definitions_hash,
                        canonical_hash(gate_payload),
                    ),
                )
            if atomic_broker_gate is not None:
                if gate_values is None:
                    raise BridgeValidationError(
                        "atomic broker gate requires the coordinator broker gate"
                    )
                if not isinstance(atomic_broker_gate, AtomicBrokerGateInput):
                    raise TypeError(
                        "atomic_broker_gate must be an AtomicBrokerGateInput"
                    )
                if not hmac.compare_digest(
                    atomic_broker_gate.proposal_hash, proposal_hash
                ):
                    raise BridgeValidationError(
                        "atomic broker gate proposal binding is invalid"
                    )
                if atomic_broker_gate.contract_definitions_hash != definitions_hash:
                    raise BridgeValidationError(
                        "atomic broker gate contract-definition binding is invalid"
                    )
                if atomic_broker_gate.net_liquidation_usd != gate_nlv:
                    raise BridgeValidationError(
                        "atomic broker gate account binding is invalid"
                    )
                if atomic_broker_gate.quotes_observed_at != observed_at:
                    raise BridgeValidationError(
                        "atomic broker gate quote-time binding is invalid"
                    )
                atomic_payload = _atomic_authorization_payload(
                    approval_id=approval_id,
                    verified_at=now,
                    proposal_hash=proposal_hash,
                    gate=atomic_broker_gate,
                )
                self._connection.execute(
                    """
                    INSERT INTO codex_bridge_atomic_authorization_gates(
                        approval_id, verified_at, proposal_hash,
                        snapshot_hash, snapshot_json,
                        decision_bindings_hash, decision_bindings_json,
                        contract_definitions_hash, proof_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        approval_id,
                        datetime_text(now),
                        proposal_hash,
                        atomic_broker_gate.snapshot_hash,
                        canonical_json(atomic_broker_gate.snapshot_payload),
                        atomic_broker_gate.decision_bindings_hash,
                        canonical_json(atomic_broker_gate.decision_bindings),
                        atomic_broker_gate.contract_definitions_hash,
                        canonical_hash(atomic_payload),
                    ),
                )
            return self._public_row_locked(approval_id)

    def complete(
        self,
        approval_id: str,
        token: str,
        repriced_proposal: Mapping[str, object],
        instruction_intent: Mapping[str, object],
        execution_result: Mapping[str, object],
    ) -> BridgeRecord:
        """Complete an authorized handoff without reapplying quote-age <=5s.

        The external review tool may take longer than five seconds.  Completion
        is therefore bound to the authorized hashes and exact Decimal limit,
        while the approval store still applies its own TTL and one-time consume.
        """

        _identifier("approval_id", approval_id)
        now = self._trusted_now()

        with self._transaction():
            row = self._internal_row(approval_id)
            self._require_token(row, token)
            self._require_state(row, BridgeStatus.AUTHORIZED)
            attempted = self._connection.execute(
                "SELECT 1 FROM codex_bridge_external_attempts WHERE approval_id=?",
                (approval_id,),
            ).fetchone()
            if attempted is None:
                raise BridgeStateError(
                    "external call must be durably reserved before completion"
                )

            proposal = _canonical_mapping("repriced_proposal", repriced_proposal)
            executable_price = _executable_net_price(proposal)
            current_cost_usd = _executable_cost_usd(proposal)
            intent, limit_price = _normalize_instruction_intent(instruction_intent)
            _bind_intent_to_proposal(intent, proposal)
            _require_limit_matches(intent, limit_price, executable_price)
            result = _canonical_mapping("execution_result", execution_result)
            proposal_hash = canonical_hash(proposal)
            intent_hash = canonical_hash(intent)
            execution_hash = canonical_hash(result)
            if not hmac.compare_digest(str(row["proposal_hash"]), proposal_hash):
                raise BridgeValidationError("authorized proposal hash changed")
            if not hmac.compare_digest(str(row["intent_hash"]), intent_hash):
                raise BridgeValidationError("authorized instruction intent changed")
            authorized_limit = Decimal(str(row["limit_price"]))
            if limit_price != authorized_limit:
                raise BridgeValidationError("authorized limit_price changed")
            instruction_id, deep_link = _validate_execution_result(result)

            validation, consumption = self._approval_store.consume(
                approval_id,
                proposal,
                execution=result,
                current_cost_usd=current_cost_usd,
                require_authority_binding=True,
            )
            if not validation.valid or consumption is None:
                raise BridgeApprovalRejected(validation.reasons)
            if not hmac.compare_digest(consumption.execution_hash, execution_hash):
                raise BridgeError("approval consumption hash mismatch")

            cursor = self._connection.execute(
                """
                UPDATE codex_bridge_requests
                SET status='COMPLETED', completed_at=?, execution_hash=?,
                    execution_json=?, instruction_id=?, deep_link=?
                WHERE approval_id=? AND status='AUTHORIZED'
                """,
                (
                    datetime_text(now),
                    execution_hash,
                    canonical_json(result),
                    instruction_id,
                    deep_link,
                    approval_id,
                ),
            )
            if cursor.rowcount != 1:
                raise BridgeStateError("authorization was not completable")
            return self._public_row_locked(approval_id)

    def require_creator_destination_contract(self) -> None:
        """Fail closed until an authoritative Creator destination is installed."""

        self._ensure_open()
        raise BridgeValidationError(
            "creator review destination contract is unavailable"
        )

    def reserve_external_call(
        self,
        approval_id: str,
        token: str,
        *,
        lock_time_requery: Callable[
            [Mapping[str, object]], AtomicBrokerGateInput
        ]
        | None = None,
    ) -> BridgeRecord:
        """Persist the one and only external-call attempt before any side effect.

        The primary state must already be ``AUTHORIZED``.  The append-only
        reservation is deliberately separate from the state column so a crash
        or unknown connector outcome can never make the approval eligible for
        another call.
        """

        _identifier("approval_id", approval_id)
        with self._transaction():
            # Freshness is evaluated only after the write lock is held.  A
            # lock wait may itself make the gate evidence stale.
            precheck_at = self._trusted_now()
            row = self._internal_row(approval_id)
            self._require_token(row, token)
            self._require_state(row, BridgeStatus.AUTHORIZED)
            attempted = self._connection.execute(
                "SELECT 1 FROM codex_bridge_external_attempts WHERE approval_id=?",
                (approval_id,),
            ).fetchone()
            if attempted is not None:
                raise BridgeExternalCallAlreadyAttempted(
                    "external call was already attempted; retry is forbidden"
                )
            self._require_claim_authority_audit_locked(approval_id)
            gate = self._connection.execute(
                """
                SELECT * FROM codex_bridge_broker_gates
                WHERE approval_id=?
                """,
                (approval_id,),
            ).fetchone()
            if gate is None:
                raise BridgeStateError(
                    "coordinator broker-gate proof is required before external call"
                )
            _verify_broker_gate(row, gate, now=precheck_at)
            quote_at = datetime.fromisoformat(str(row["quotes_observed_at"]))
            _require_fresh_quote(
                precheck_at,
                utc_datetime(quote_at, field="quotes_observed_at"),
            )
            proposal_value = thaw_json(
                freeze_json(json.loads(str(row["proposal_json"])))
            )
            if not isinstance(proposal_value, Mapping):
                raise BridgeError("stored authorized proposal is not an object")
            reserve_proof_hash: str | None = None
            validation_proposal = proposal_value
            reserve_gate: AtomicBrokerGateInput | None = None
            if lock_time_requery is not None:
                atomic_gate = self._connection.execute(
                    """
                    SELECT * FROM codex_bridge_atomic_authorization_gates
                    WHERE approval_id=?
                    """,
                    (approval_id,),
                ).fetchone()
                if atomic_gate is None:
                    raise BridgeStateError(
                        "atomic authorization proof is required before reserve-time requery"
                    )
                _verify_atomic_authorization_gate(
                    row,
                    atomic_gate,
                    now=precheck_at,
                )
                reserve_gate = lock_time_requery(proposal_value)
                if not isinstance(reserve_gate, AtomicBrokerGateInput):
                    raise TypeError(
                        "lock_time_requery must return AtomicBrokerGateInput"
                    )
                reserve_verified_at = self._trusted_now()
                reserve_payload = _verify_reserve_atomic_gate(
                    approval_id=approval_id,
                    request_row=row,
                    authorization_gate_row=atomic_gate,
                    reserve_gate=reserve_gate,
                    verified_at=reserve_verified_at,
                )
                reserve_proof_hash = canonical_hash(reserve_payload)
                validation_proposal = _canonical_mapping(
                    "reserve repriced_proposal",
                    reserve_gate.repriced_proposal,
                )
                self._connection.execute(
                    """
                    INSERT INTO codex_bridge_reserve_gates(
                        approval_id, verified_at, authorized_snapshot_hash,
                        snapshot_hash, snapshot_json,
                        decision_bindings_hash, decision_bindings_json,
                        proposal_hash, proposal_json, quotes_observed_at,
                        contract_definitions_hash,
                        strategy_nav_contract_hash,
                        strategy_nav_ledger_head_hash,
                        execution_cost_contract_hash,
                        current_policy_hash, proof_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        approval_id,
                        datetime_text(reserve_verified_at),
                        str(atomic_gate["snapshot_hash"]),
                        reserve_gate.snapshot_hash,
                        canonical_json(reserve_gate.snapshot_payload),
                        reserve_gate.decision_bindings_hash,
                        canonical_json(reserve_gate.decision_bindings),
                        reserve_gate.proposal_hash,
                        canonical_json(reserve_gate.repriced_proposal),
                        datetime_text(reserve_gate.quotes_observed_at),
                        reserve_gate.contract_definitions_hash,
                        str(
                            reserve_gate.decision_bindings[
                                "strategy_nav_contract_hash"
                            ]
                        ),
                        str(
                            reserve_gate.decision_bindings[
                                "strategy_nav_ledger_head_hash"
                            ]
                        ),
                        str(
                            reserve_gate.decision_bindings[
                                "execution_cost_contract_hash"
                            ]
                        ),
                        str(reserve_gate.decision_bindings["current_policy_hash"]),
                        reserve_proof_hash,
                    ),
                )
            validation = self._approval_store.validate(
                approval_id,
                validation_proposal,
                current_cost_usd=_executable_cost_usd(validation_proposal),
                require_authority_binding=True,
            )
            if not validation.valid:
                raise BridgeApprovalRejected(validation.reasons)
            attempt_started_at = self._trusted_now()
            if reserve_gate is not None:
                _require_fresh_observation(
                    attempt_started_at,
                    _snapshot_built_at(reserve_gate.snapshot_payload),
                    label="reserve broker snapshot",
                )
                _require_fresh_quote(
                    attempt_started_at,
                    reserve_gate.quotes_observed_at,
                )
                _require_current_entry_time_decision(
                    attempt_started_at,
                    reserve_gate.decision_bindings,
                )
            else:
                _require_fresh_quote(
                    attempt_started_at,
                    utc_datetime(quote_at, field="quotes_observed_at"),
                )
            payload_hash = canonical_hash(
                {
                    "approval_id": approval_id,
                    "proposal_hash": str(row["proposal_hash"]),
                    "intent_hash": str(row["intent_hash"]),
                    "limit_price": Decimal(str(row["limit_price"])),
                    "reserve_proof_hash": reserve_proof_hash,
                }
            )
            try:
                self._connection.execute(
                    """
                    INSERT INTO codex_bridge_external_attempts(
                        approval_id, started_at, payload_hash
                    ) VALUES (?, ?, ?)
                    """,
                    (approval_id, datetime_text(attempt_started_at), payload_hash),
                )
            except sqlite3.IntegrityError as exc:
                raise BridgeExternalCallAlreadyAttempted(
                    "external call was already attempted; retry is forbidden"
                ) from exc
            return self._public_row_locked(approval_id)

    def has_external_call_attempt(self, approval_id: str) -> bool:
        _identifier("approval_id", approval_id)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 FROM codex_bridge_external_attempts WHERE approval_id=?",
                (approval_id,),
            ).fetchone()
        return row is not None

    def fail_unknown_outcome(
        self,
        approval_id: str,
        token: str,
        reason_code: str = "external_result_uncertain",
    ) -> BridgeRecord:
        """Make an uncertain external result terminal without exposing details."""

        if reason_code not in _UNKNOWN_OUTCOME_CODES:
            raise ValueError("reason_code is not an allowed unknown-outcome code")
        return self.fail(
            approval_id,
            token,
            UNKNOWN_OUTCOME_REASON_PREFIX + reason_code,
        )

    def fail(self, approval_id: str, token: str, reason: str) -> BridgeRecord:
        """Enter the terminal FAILED state; a failed claim is never retried."""

        _identifier("approval_id", approval_id)
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a nonblank reason code")
        reason = reason.strip()
        with self._transaction():
            now = self._trusted_now()
            row = self._internal_row(approval_id)
            self._require_token(row, token)
            status = BridgeStatus(str(row["status"]))
            if status not in {BridgeStatus.CLAIMED, BridgeStatus.AUTHORIZED}:
                raise BridgeStateError(f"cannot fail a {status.value} bridge request")
            attempted = self._connection.execute(
                "SELECT 1 FROM codex_bridge_external_attempts WHERE approval_id=?",
                (approval_id,),
            ).fetchone()
            if attempted is not None and not reason.startswith(
                UNKNOWN_OUTCOME_REASON_PREFIX
            ):
                # Once the external boundary was crossed, an ordinary failure
                # would falsely imply it is safe to try again.
                reason = (
                    UNKNOWN_OUTCOME_REASON_PREFIX
                    + "external_result_uncertain"
                )
            elif reason.startswith(UNKNOWN_OUTCOME_REASON_PREFIX):
                code = reason.removeprefix(UNKNOWN_OUTCOME_REASON_PREFIX)
                if code not in _UNKNOWN_OUTCOME_CODES:
                    raise ValueError(
                        "reason is not an allowed unknown-outcome code"
                    )
            elif reason not in _FAILURE_REASON_CODES:
                raise ValueError("reason is not an allowed failure reason code")
            cursor = self._connection.execute(
                """
                UPDATE codex_bridge_requests
                SET status='FAILED', failed_at=?, failure_reason=?
                WHERE approval_id=? AND status IN ('CLAIMED', 'AUTHORIZED')
                """,
                (datetime_text(now), reason, approval_id),
            )
            if cursor.rowcount != 1:
                raise BridgeStateError("bridge request was not failable")
            return self._public_row_locked(approval_id)

    def expire_stranded_claim(self, approval_id: str) -> BridgeRecord:
        """Auditably terminate a token-lost claim after its GUI approval expires.

        This recovery path deliberately accepts no token, but it is limited to
        a never-authorized ``CLAIMED`` row with no external attempt and an
        approval store that currently proves ``approval_expired``.  It cannot
        affect an authorization or any possibly-issued external instruction.
        """

        _identifier("approval_id", approval_id)
        with self._transaction():
            now = self._trusted_now()
            row = self._internal_row(approval_id)
            self._require_state(row, BridgeStatus.CLAIMED)
            attempted = self._connection.execute(
                "SELECT 1 FROM codex_bridge_external_attempts WHERE approval_id=?",
                (approval_id,),
            ).fetchone()
            if attempted is not None:
                raise BridgeStateError("a claim with an external attempt cannot expire")
            approval = self._approval_store.get(approval_id)
            if approval is None:
                raise BridgeStateError("approval expiry cannot be proven")
            validation = self._approval_store.validate(
                approval_id,
                approval.proposal,
                require_authority_binding=True,
            )
            if "approval_expired" not in validation.reasons:
                raise BridgeStateError("claim is not backed by an expired approval")
            cursor = self._connection.execute(
                """
                UPDATE codex_bridge_requests
                SET status='FAILED', failed_at=?, failure_reason=?
                WHERE approval_id=? AND status='CLAIMED'
                """,
                (
                    datetime_text(now),
                    "claim_abandoned_expired",
                    approval_id,
                ),
            )
            if cursor.rowcount != 1:
                raise BridgeStateError("stranded claim was not expirable")
            return self._public_row_locked(approval_id)

    def get(self, approval_id: str) -> BridgeRecord | None:
        """Return a token-free record without mutating bridge state."""

        _identifier("approval_id", approval_id)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                f"SELECT {_SAFE_COLUMNS} FROM codex_bridge_requests "
                "WHERE approval_id=?",
                (approval_id,),
            ).fetchone()
        return None if row is None else _row_to_record(row)

    def status(self, approval_id: str) -> BridgeStatus | None:
        record = self.get(approval_id)
        return None if record is None else record.status

    def list_pending(self, *, limit: int = 500) -> tuple[BridgeRecord, ...]:
        """List CLAIMED/AUTHORIZED records only; terminal rows are excluded."""

        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        self._ensure_open()
        with self._lock:
            rows = self._connection.execute(
                f"SELECT {_SAFE_COLUMNS} FROM codex_bridge_requests "
                "WHERE status IN ('CLAIMED', 'AUTHORIZED') "
                "ORDER BY sequence LIMIT ?",
                (limit,),
            ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def _public_row_locked(self, approval_id: str) -> BridgeRecord:
        row = self._connection.execute(
            f"SELECT {_SAFE_COLUMNS} FROM codex_bridge_requests WHERE approval_id=?",
            (approval_id,),
        ).fetchone()
        if row is None:
            raise BridgeError("bridge record disappeared")
        return _row_to_record(row)

    def _internal_row(self, approval_id: str) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM codex_bridge_requests WHERE approval_id=?",
            (approval_id,),
        ).fetchone()
        if row is None:
            raise BridgeStateError("bridge request is not claimed")
        return row

    def _require_claim_authority_audit_locked(
        self,
        approval_id: str,
    ) -> CurrentAuthorityProof:
        row = self._connection.execute(
            """
            SELECT requests.approval_id,
                   authority.checked_at AS current_authority_checked_at,
                   authority.authority_binding_hash
                       AS current_authority_binding_hash,
                   authority.proof_json AS current_authority_proof_json,
                   authority.proof_hash AS current_authority_proof_hash
            FROM codex_bridge_requests AS requests
            LEFT JOIN codex_bridge_claim_authority_proofs AS authority
              ON authority.approval_id = requests.approval_id
            WHERE requests.approval_id=?
            """,
            (approval_id,),
        ).fetchone()
        if row is None:
            raise BridgeStateError("bridge request is not claimed")
        proof, _, _ = _claim_authority_audit_from_row(row)
        if proof is None:
            raise BridgeStateError("claim_authority_proof_missing")
        return proof

    @staticmethod
    def _require_token(row: sqlite3.Row, token: str) -> None:
        supplied_hash = _token_digest(token)
        if not hmac.compare_digest(str(row["token_hash"]), supplied_hash):
            raise BridgeTokenError("invalid bridge claim token")

    @staticmethod
    def _require_state(row: sqlite3.Row, expected: BridgeStatus) -> None:
        actual = BridgeStatus(str(row["status"]))
        if actual is not expected:
            raise BridgeStateError(
                f"expected {expected.value}, found terminal/current {actual.value}"
            )

    def _trusted_now(self) -> datetime:
        return utc_datetime(self._clock(), field="clock result")

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"bridge schema {version} is newer than supported")
        with self._lock:
            self._connection.executescript(
                f"""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS codex_bridge_requests (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    approval_id TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL CHECK (
                        status IN ('CLAIMED', 'AUTHORIZED', 'COMPLETED', 'FAILED')
                    ),
                    token_hash TEXT NOT NULL UNIQUE CHECK (length(token_hash) = 64),
                    claimed_at TEXT NOT NULL,
                    authorized_at TEXT,
                    quotes_observed_at TEXT,
                    proposal_hash TEXT,
                    proposal_json TEXT,
                    intent_hash TEXT,
                    intent_json TEXT,
                    limit_price TEXT,
                    completed_at TEXT,
                    execution_hash TEXT,
                    execution_json TEXT,
                    instruction_id TEXT,
                    deep_link TEXT,
                    failed_at TEXT,
                    failure_reason TEXT,
                    CHECK (
                        (status = 'CLAIMED'
                            AND authorized_at IS NULL
                            AND completed_at IS NULL AND failed_at IS NULL)
                        OR
                        (status = 'AUTHORIZED'
                            AND authorized_at IS NOT NULL
                            AND quotes_observed_at IS NOT NULL
                            AND proposal_hash IS NOT NULL
                            AND proposal_json IS NOT NULL
                            AND intent_hash IS NOT NULL
                            AND intent_json IS NOT NULL
                            AND limit_price IS NOT NULL
                            AND completed_at IS NULL AND failed_at IS NULL)
                        OR
                        (status = 'COMPLETED'
                            AND authorized_at IS NOT NULL
                            AND completed_at IS NOT NULL
                            AND execution_hash IS NOT NULL
                            AND execution_json IS NOT NULL
                            AND instruction_id IS NOT NULL
                            AND deep_link IS NOT NULL
                            AND failed_at IS NULL)
                        OR
                        (status = 'FAILED'
                            AND completed_at IS NULL
                            AND execution_hash IS NULL
                            AND execution_json IS NULL
                            AND failed_at IS NOT NULL
                            AND failure_reason IS NOT NULL)
                    )
                );
                CREATE INDEX IF NOT EXISTS codex_bridge_status_idx
                    ON codex_bridge_requests(status, sequence);
                CREATE UNIQUE INDEX IF NOT EXISTS codex_bridge_one_active_handoff
                    ON codex_bridge_requests((1))
                    WHERE status IN ('CLAIMED', 'AUTHORIZED');
                CREATE TABLE IF NOT EXISTS codex_bridge_claim_authority_proofs (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    approval_id TEXT NOT NULL UNIQUE,
                    checked_at TEXT NOT NULL,
                    authority_binding_hash TEXT NOT NULL CHECK (
                        length(authority_binding_hash) = 64
                    ),
                    proof_json TEXT NOT NULL,
                    proof_hash TEXT NOT NULL CHECK (length(proof_hash) = 64),
                    FOREIGN KEY(approval_id)
                        REFERENCES codex_bridge_requests(approval_id)
                );
                CREATE TABLE IF NOT EXISTS codex_bridge_external_attempts (
                    approval_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    payload_hash TEXT NOT NULL CHECK (length(payload_hash) = 64),
                    FOREIGN KEY(approval_id)
                        REFERENCES codex_bridge_requests(approval_id)
                );
                CREATE TABLE IF NOT EXISTS codex_bridge_broker_gates (
                    approval_id TEXT PRIMARY KEY,
                    verified_at TEXT NOT NULL,
                    snapshot_observed_at TEXT NOT NULL,
                    quotes_observed_at TEXT NOT NULL,
                    proposal_hash TEXT NOT NULL CHECK (length(proposal_hash) = 64),
                    quote_snapshot_id TEXT NOT NULL,
                    net_liquidation_usd TEXT NOT NULL,
                    contract_definitions_hash TEXT NOT NULL CHECK (
                        length(contract_definitions_hash) = 64
                    ),
                    broker_snapshot_complete INTEGER NOT NULL CHECK (
                        broker_snapshot_complete = 1
                    ),
                    open_option_combinations INTEGER NOT NULL CHECK (
                        open_option_combinations = 0
                    ),
                    working_order_count INTEGER NOT NULL CHECK (
                        working_order_count = 0
                    ),
                    unsubmitted_instruction_count INTEGER NOT NULL CHECK (
                        unsubmitted_instruction_count = 0
                    ),
                    proof_hash TEXT NOT NULL CHECK (length(proof_hash) = 64),
                    FOREIGN KEY(approval_id)
                        REFERENCES codex_bridge_requests(approval_id)
                );
                CREATE TABLE IF NOT EXISTS codex_bridge_atomic_authorization_gates (
                    approval_id TEXT PRIMARY KEY,
                    verified_at TEXT NOT NULL,
                    proposal_hash TEXT NOT NULL CHECK (length(proposal_hash) = 64),
                    snapshot_hash TEXT NOT NULL CHECK (length(snapshot_hash) = 64),
                    snapshot_json TEXT NOT NULL,
                    decision_bindings_hash TEXT NOT NULL CHECK (
                        length(decision_bindings_hash) = 64
                    ),
                    decision_bindings_json TEXT NOT NULL,
                    contract_definitions_hash TEXT NOT NULL CHECK (
                        length(contract_definitions_hash) = 64
                    ),
                    proof_hash TEXT NOT NULL CHECK (length(proof_hash) = 64),
                    FOREIGN KEY(approval_id)
                        REFERENCES codex_bridge_requests(approval_id)
                );
                CREATE TABLE IF NOT EXISTS codex_bridge_reserve_gates (
                    approval_id TEXT PRIMARY KEY,
                    verified_at TEXT NOT NULL,
                    authorized_snapshot_hash TEXT NOT NULL CHECK (
                        length(authorized_snapshot_hash) = 64
                    ),
                    snapshot_hash TEXT NOT NULL CHECK (length(snapshot_hash) = 64),
                    snapshot_json TEXT NOT NULL,
                    decision_bindings_hash TEXT NOT NULL CHECK (
                        length(decision_bindings_hash) = 64
                    ),
                    decision_bindings_json TEXT NOT NULL,
                    proposal_hash TEXT NOT NULL CHECK (length(proposal_hash) = 64),
                    proposal_json TEXT NOT NULL,
                    quotes_observed_at TEXT NOT NULL,
                    contract_definitions_hash TEXT NOT NULL CHECK (
                        length(contract_definitions_hash) = 64
                    ),
                    strategy_nav_contract_hash TEXT NOT NULL CHECK (
                        length(strategy_nav_contract_hash) = 64
                    ),
                    strategy_nav_ledger_head_hash TEXT NOT NULL CHECK (
                        length(strategy_nav_ledger_head_hash) = 64
                    ),
                    execution_cost_contract_hash TEXT NOT NULL CHECK (
                        length(execution_cost_contract_hash) = 64
                    ),
                    current_policy_hash TEXT NOT NULL CHECK (
                        length(current_policy_hash) = 64
                    ),
                    proof_hash TEXT NOT NULL CHECK (length(proof_hash) = 64),
                    FOREIGN KEY(approval_id)
                        REFERENCES codex_bridge_requests(approval_id)
                );
                CREATE TRIGGER IF NOT EXISTS codex_bridge_strict_transition
                BEFORE UPDATE ON codex_bridge_requests
                WHEN NOT (
                    (OLD.status = 'CLAIMED'
                        AND NEW.status IN ('AUTHORIZED', 'FAILED'))
                    OR
                    (OLD.status = 'AUTHORIZED'
                        AND NEW.status IN ('COMPLETED', 'FAILED'))
                )
                BEGIN
                    SELECT RAISE(ABORT, 'invalid bridge state transition');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_identity_immutable
                BEFORE UPDATE ON codex_bridge_requests
                WHEN NEW.sequence IS NOT OLD.sequence
                    OR NEW.approval_id IS NOT OLD.approval_id
                    OR NEW.token_hash IS NOT OLD.token_hash
                    OR NEW.claimed_at IS NOT OLD.claimed_at
                BEGIN
                    SELECT RAISE(ABORT, 'immutable bridge identity');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_authorization_immutable
                BEFORE UPDATE ON codex_bridge_requests
                WHEN OLD.status = 'AUTHORIZED' AND (
                    NEW.authorized_at IS NOT OLD.authorized_at
                    OR NEW.quotes_observed_at IS NOT OLD.quotes_observed_at
                    OR NEW.proposal_hash IS NOT OLD.proposal_hash
                    OR NEW.proposal_json IS NOT OLD.proposal_json
                    OR NEW.intent_hash IS NOT OLD.intent_hash
                    OR NEW.intent_json IS NOT OLD.intent_json
                    OR NEW.limit_price IS NOT OLD.limit_price
                )
                BEGIN
                    SELECT RAISE(ABORT, 'immutable bridge authorization');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_no_delete
                BEFORE DELETE ON codex_bridge_requests
                BEGIN
                    SELECT RAISE(ABORT, 'bridge request delete forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_attempt_no_update
                BEFORE UPDATE ON codex_bridge_external_attempts
                BEGIN
                    SELECT RAISE(ABORT, 'external attempt update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_attempt_no_delete
                BEFORE DELETE ON codex_bridge_external_attempts
                BEGIN
                    SELECT RAISE(ABORT, 'external attempt delete forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_claim_authority_no_update
                BEFORE UPDATE ON codex_bridge_claim_authority_proofs
                BEGIN
                    SELECT RAISE(ABORT, 'claim authority proof update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_claim_authority_no_delete
                BEFORE DELETE ON codex_bridge_claim_authority_proofs
                BEGIN
                    SELECT RAISE(ABORT, 'claim authority proof delete forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_gate_no_update
                BEFORE UPDATE ON codex_bridge_broker_gates
                BEGIN
                    SELECT RAISE(ABORT, 'broker gate update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_gate_no_delete
                BEFORE DELETE ON codex_bridge_broker_gates
                BEGIN
                    SELECT RAISE(ABORT, 'broker gate delete forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_atomic_gate_no_update
                BEFORE UPDATE ON codex_bridge_atomic_authorization_gates
                BEGIN
                    SELECT RAISE(ABORT, 'atomic broker gate update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_atomic_gate_no_delete
                BEFORE DELETE ON codex_bridge_atomic_authorization_gates
                BEGIN
                    SELECT RAISE(ABORT, 'atomic broker gate delete forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_reserve_gate_no_update
                BEFORE UPDATE ON codex_bridge_reserve_gates
                BEGIN
                    SELECT RAISE(ABORT, 'reserve broker gate update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS codex_bridge_reserve_gate_no_delete
                BEFORE DELETE ON codex_bridge_reserve_gates
                BEGIN
                    SELECT RAISE(ABORT, 'reserve broker gate delete forbidden');
                END;
                PRAGMA user_version={SCHEMA_VERSION};
                COMMIT;
                """
            )
            self._connection.execute("PRAGMA foreign_keys=ON")

    class _Transaction:
        def __init__(self, store: "CodexBridgeStore") -> None:
            self.store = store

        def __enter__(self) -> None:
            self.store._ensure_open()
            self.store._lock.acquire()
            try:
                self.store._connection.execute("BEGIN IMMEDIATE")
            except BaseException:
                self.store._lock.release()
                raise

        def __exit__(self, exc_type: object, *_: object) -> None:
            try:
                self.store._connection.execute("ROLLBACK" if exc_type else "COMMIT")
            finally:
                self.store._lock.release()

    def _transaction(self) -> "CodexBridgeStore._Transaction":
        return self._Transaction(self)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Codex bridge store is closed")


def _canonical_mapping(field: str, value: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be a mapping")
    # Round-tripping the canonical form gives us a detached, stable snapshot.
    # ``freeze_json`` also turns arrays into tuples, which lets ``thaw_json``
    # restore Decimal tags nested inside arrays (plain ``json.loads`` arrays
    # intentionally are not traversed by that shared helper).
    decoded = thaw_json(freeze_json(value))
    if not isinstance(decoded, dict):
        raise TypeError(f"{field} must be a mapping")
    return decoded


def _authority_identifier(field: str, value: object) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise BridgeValidationError(f"{field}_invalid")
    return value


def _authority_timestamp(field: str, value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value and value == value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BridgeValidationError(f"{field}_invalid") from exc
    else:
        raise BridgeValidationError(f"{field}_invalid")
    try:
        return utc_datetime(parsed, field=field)
    except (TypeError, ValueError) as exc:
        raise BridgeValidationError(f"{field}_invalid") from exc


def _current_authority_proof_from_value(
    value: CurrentAuthorityProof | Mapping[str, object] | object,
) -> CurrentAuthorityProof:
    if isinstance(value, CurrentAuthorityProof):
        return value
    if isinstance(value, bool) or not isinstance(value, Mapping):
        raise BridgeValidationError("current_authority_proof_invalid")
    if any(not isinstance(key, str) for key in value) or set(value) != set(
        _CURRENT_AUTHORITY_PROOF_FIELDS
    ):
        raise BridgeValidationError("current_authority_proof_fields_invalid")
    source = dict(value)
    return CurrentAuthorityProof(
        schema=source["schema"],
        status=source["status"],
        checked_at=source["checked_at"],
        ranking_snapshot_id=source["ranking_snapshot_id"],
        candidate_id=source["candidate_id"],
        proposal_hash=source["proposal_hash"],
        snapshot_hash=source["snapshot_hash"],
        current_policy_version=source["current_policy_version"],
        current_policy_hash=source["current_policy_hash"],
        policy_authority_marker_hash=source["policy_authority_marker_hash"],
        cost_version=source["cost_version"],
        cost_hash=source["cost_hash"],
        risk_contract_hash=source["risk_contract_hash"],
        risk_authority_version=source["risk_authority_version"],
        risk_authority_marker_hash=source["risk_authority_marker_hash"],
        strategy_nav_content_hash=source["strategy_nav_content_hash"],
        strategy_nav_contract_hash=source["strategy_nav_contract_hash"],
        strategy_nav_ledger_head_hash=source["strategy_nav_ledger_head_hash"],
    )


def _validate_current_authority_proof(
    value: CurrentAuthorityProof | Mapping[str, object] | object,
    *,
    binding: ApprovalAuthorityBinding,
    checked_at: datetime,
) -> CurrentAuthorityProof:
    proof = _current_authority_proof_from_value(value)
    at = utc_datetime(checked_at, field="current authority checked_at")
    age_seconds = Decimal(str((at - proof.checked_at).total_seconds()))
    if age_seconds < 0:
        raise BridgeValidationError("current_authority_checked_at_future")
    if age_seconds > CURRENT_AUTHORITY_PROOF_MAX_AGE_SECONDS:
        raise BridgeValidationError("current_authority_proof_stale")

    expected: dict[str, str] = {
        "ranking_snapshot_id": binding.ranking_snapshot_id,
        "candidate_id": binding.candidate_id,
        "proposal_hash": binding.proposal_hash,
        "snapshot_hash": binding.snapshot_hash,
        "current_policy_version": binding.current_policy_version,
        "current_policy_hash": binding.current_policy_hash,
        "policy_authority_marker_hash": binding.policy_authority_marker_hash,
        "cost_version": binding.cost_version,
        "cost_hash": binding.cost_hash,
        "risk_contract_hash": binding.risk_contract_hash,
        "risk_authority_version": binding.risk_authority_version,
        "risk_authority_marker_hash": binding.risk_authority_marker_hash,
        "strategy_nav_content_hash": _digest(
            "binding.strategy_nav_proof.content_hash",
            binding.strategy_nav_proof.get("content_hash"),
        ),
        "strategy_nav_contract_hash": _digest(
            "binding.strategy_nav_proof.contract_hash",
            binding.strategy_nav_proof.get("contract_hash"),
        ),
        "strategy_nav_ledger_head_hash": _digest(
            "binding.strategy_nav_proof.ledger_head_hash",
            binding.strategy_nav_proof.get("ledger_head_hash"),
        ),
    }
    for field, expected_value in expected.items():
        if getattr(proof, field) != expected_value:
            raise BridgeValidationError(f"{field}_mismatch")
    return proof


def _claim_authority_audit_values(
    approval_id: str,
    binding: ApprovalAuthorityBinding,
    proof: CurrentAuthorityProof,
) -> tuple[str, str, str, str, str]:
    binding_hash = _digest("authority_binding_hash", binding.binding_hash)
    proof_payload = proof.as_dict()
    proof_hash = canonical_hash(
        {
            "approval_id": approval_id,
            "authority_binding_hash": binding_hash,
            "proof": proof_payload,
        }
    )
    return (
        approval_id,
        datetime_text(proof.checked_at),
        binding_hash,
        canonical_json(proof_payload),
        proof_hash,
    )


def _claim_authority_audit_from_row(
    row: sqlite3.Row,
) -> tuple[CurrentAuthorityProof | None, str | None, str | None]:
    fields = (
        "current_authority_checked_at",
        "current_authority_binding_hash",
        "current_authority_proof_json",
        "current_authority_proof_hash",
    )
    values = tuple(row[field] for field in fields)
    if all(value is None for value in values):
        return None, None, None
    if any(value is None for value in values):
        raise BridgeStateError("stored claim authority proof is incomplete")
    try:
        decoded = json.loads(str(row["current_authority_proof_json"]))
        proof = _current_authority_proof_from_value(decoded)
        binding_hash = _digest(
            "stored authority_binding_hash",
            str(row["current_authority_binding_hash"]),
        )
        proof_hash = _digest(
            "stored current_authority_proof_hash",
            str(row["current_authority_proof_hash"]),
        )
    except (BridgeValidationError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise BridgeStateError("stored claim authority proof is invalid") from exc
    if datetime_text(proof.checked_at) != str(row["current_authority_checked_at"]):
        raise BridgeStateError("stored claim authority proof checked_at mismatch")
    expected_hash = canonical_hash(
        {
            "approval_id": str(row["approval_id"]),
            "authority_binding_hash": binding_hash,
            "proof": proof.as_dict(),
        }
    )
    if not hmac.compare_digest(proof_hash, expected_hash):
        raise BridgeStateError("stored claim authority proof hash is invalid")
    return proof, binding_hash, proof_hash


def _normalize_instruction_intent(
    value: Mapping[str, object],
) -> tuple[dict[str, object], Decimal]:
    intent = _canonical_mapping("instruction_intent", value)
    _assert_no_forbidden_intent_fields(intent)
    required = {
        "combo_legs",
        "action",
        "quantity",
        "order_type",
        "limit_price",
        "time_in_force",
    }
    missing = sorted(required.difference(intent))
    if missing:
        raise BridgeValidationError(
            "instruction_intent missing fields: " + ", ".join(missing)
        )
    unknown = sorted(set(intent).difference(required))
    if unknown:
        raise BridgeValidationError(
            "instruction_intent contains unsupported fields: " + ", ".join(unknown)
        )
    action = _side(intent["action"], field="instruction action")
    if action not in {"BUY", "SELL"}:
        raise BridgeValidationError("instruction action must be BUY or SELL")
    intent["action"] = action
    order_type = _upper_text(intent["order_type"], "order_type")
    if order_type != "LIMIT":
        raise BridgeValidationError("order_type must be LIMIT; MARKET is forbidden")
    intent["order_type"] = order_type
    time_in_force = _upper_text(intent["time_in_force"], "time_in_force")
    if time_in_force != "DAY":
        raise BridgeValidationError("time_in_force must be DAY")
    intent["time_in_force"] = time_in_force
    intent["quantity"] = _positive_integral_decimal("quantity", intent["quantity"])
    if intent["quantity"] != Decimal("1"):
        raise BridgeValidationError(
            "instruction quantity must be exactly 1 approved combination"
        )
    limit_price = _decimal("limit_price", intent["limit_price"])
    intent["limit_price"] = limit_price

    legs = intent["combo_legs"]
    if not isinstance(legs, list) or not legs:
        raise BridgeValidationError("combo_legs must be a nonempty array")
    normalized_legs: list[dict[str, object]] = []
    for index, value in enumerate(legs):
        if not isinstance(value, dict):
            raise BridgeValidationError(f"combo_legs[{index}] must be an object")
        leg = dict(value)
        side_keys = [key for key in ("action", "side", "position_side") if key in leg]
        if len(side_keys) != 1:
            raise BridgeValidationError(
                f"combo_legs[{index}] requires exactly one BUY/SELL side field"
            )
        side_key = side_keys[0]
        leg[side_key] = _side(leg[side_key], field=f"combo_legs[{index}].{side_key}")
        ratio_keys = [key for key in ("ratio", "quantity") if key in leg]
        if len(ratio_keys) != 1:
            raise BridgeValidationError(
                f"combo_legs[{index}] requires exactly one positive integral ratio field"
            )
        ratio_key = ratio_keys[0]
        leg[ratio_key] = _positive_integral_decimal(
            f"combo_legs[{index}].{ratio_key}", leg[ratio_key]
        )
        identity_keys = [
            key
            for key in (
                "contract_id_ex",
                "contract_id",
                "conid",
                "broker_contract_id",
                "contract",
            )
            if key in leg
        ]
        if len(identity_keys) != 1:
            raise BridgeValidationError(
                f"combo_legs[{index}] requires exactly one contract identifier"
            )
        identity_key = identity_keys[0]
        if isinstance(leg[identity_key], Mapping):
            raise BridgeValidationError(
                f"combo_legs[{index}].{identity_key} must be a scalar identifier"
            )
        allowed_leg_keys = {side_key, ratio_key, identity_key}
        unsupported = sorted(set(leg).difference(allowed_leg_keys))
        if unsupported:
            raise BridgeValidationError(
                f"combo_legs[{index}] contains unsupported fields: "
                + ", ".join(unsupported)
            )
        _contract_identity(leg, field=f"combo_legs[{index}]")
        normalized_legs.append(leg)
    intent["combo_legs"] = normalized_legs
    return intent, limit_price


def _assert_no_forbidden_intent_fields(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            folded = str(key).casefold()
            if "transmit" in folded or "submit" in folded:
                raise BridgeValidationError(
                    "instruction_intent cannot contain transmit/submit fields"
                )
            compact = re.sub(r"[^a-z0-9]", "", folded)
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
                raise BridgeValidationError(
                    "instruction_intent cannot contain authentication material"
                )
            _assert_no_forbidden_intent_fields(item)
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for item in value:
            _assert_no_forbidden_intent_fields(item)
    elif isinstance(value, str) and value.strip().upper() == "MARKET":
        raise BridgeValidationError("MARKET instructions are forbidden")


def _bind_intent_to_proposal(
    intent: Mapping[str, object], proposal: Mapping[str, object]
) -> None:
    combo_legs = intent.get("combo_legs")
    proposal_legs = proposal.get("legs")
    if not isinstance(combo_legs, list) or not isinstance(proposal_legs, list):
        raise BridgeValidationError("intent and proposal require option legs")
    if len(combo_legs) != len(proposal_legs):
        raise BridgeValidationError("instruction combo legs differ from proposal legs")
    for index, (intent_leg, proposal_item) in enumerate(
        zip(combo_legs, proposal_legs, strict=True)
    ):
        if not isinstance(intent_leg, Mapping) or not isinstance(proposal_item, Mapping):
            raise BridgeValidationError(f"invalid leg object at index {index}")
        proposal_leg = (
            proposal_item["leg"]
            if isinstance(proposal_item.get("leg"), Mapping)
            else proposal_item
        )
        assert isinstance(proposal_leg, Mapping)
        left = _contract_identity(intent_leg, field=f"combo_legs[{index}]")
        right = _contract_identity(proposal_leg, field=f"proposal legs[{index}]")
        if canonical_hash(left) != canonical_hash(right):
            raise BridgeValidationError(
                f"instruction contract differs at combo leg {index}"
            )
        intent_side = _leg_side(intent_leg, field=f"combo_legs[{index}]")
        proposal_side = _leg_side(proposal_leg, field=f"proposal legs[{index}]")
        if intent_side != proposal_side:
            raise BridgeValidationError(f"instruction side differs at combo leg {index}")
        intent_ratio = _leg_ratio(intent_leg, field=f"combo_legs[{index}]")
        proposal_ratio = _leg_ratio(proposal_leg, field=f"proposal legs[{index}]")
        if intent_ratio != proposal_ratio:
            raise BridgeValidationError(f"instruction ratio differs at combo leg {index}")


def _executable_net_price(proposal: Mapping[str, object]) -> Decimal:
    legs = proposal.get("legs")
    if not isinstance(legs, list) or not legs:
        raise BridgeValidationError("repriced_proposal requires option legs")
    total = Decimal("0")
    for index, item in enumerate(legs):
        if not isinstance(item, Mapping):
            raise BridgeValidationError(f"proposal legs[{index}] must be an object")
        leg = item["leg"] if isinstance(item.get("leg"), Mapping) else item
        quote = item["quote"] if isinstance(item.get("quote"), Mapping) else item
        assert isinstance(leg, Mapping) and isinstance(quote, Mapping)
        side = _leg_side(leg, field=f"proposal legs[{index}]")
        ratio = _leg_ratio(leg, field=f"proposal legs[{index}]")
        quote_key = "ask" if side == "BUY" else "bid"
        if quote.get(quote_key) is None:
            raise BridgeValidationError(
                f"proposal legs[{index}] requires executable {quote_key} quote"
            )
        price = _decimal(f"proposal legs[{index}].{quote_key}", quote[quote_key])
        if price < 0:
            raise BridgeValidationError("executable option quotes cannot be negative")
        total += price * ratio * (Decimal("1") if side == "BUY" else Decimal("-1"))
    return total


def _executable_cost_usd(proposal: Mapping[str, object]) -> Decimal:
    """Independently derive executable USD using every ratio and multiplier."""

    legs = proposal.get("legs")
    if not isinstance(legs, list) or not legs:
        raise BridgeValidationError("repriced_proposal requires option legs")
    total = Decimal("0")
    for index, item in enumerate(legs):
        if not isinstance(item, Mapping):
            raise BridgeValidationError(f"proposal legs[{index}] must be an object")
        leg = item["leg"] if isinstance(item.get("leg"), Mapping) else item
        quote = item["quote"] if isinstance(item.get("quote"), Mapping) else item
        assert isinstance(leg, Mapping) and isinstance(quote, Mapping)
        side = _leg_side(leg, field=f"proposal legs[{index}]")
        ratio = _leg_ratio(leg, field=f"proposal legs[{index}]")
        contract = leg.get("contract")
        contract_spec = contract if isinstance(contract, Mapping) else {}
        multiplier_value = leg.get(
            "multiplier", contract_spec.get("multiplier", Decimal("100"))
        )
        multiplier = _decimal(f"proposal legs[{index}].multiplier", multiplier_value)
        if multiplier <= 0:
            raise BridgeValidationError("option multiplier must be positive")
        quote_key = "ask" if side == "BUY" else "bid"
        if quote.get(quote_key) is None:
            raise BridgeValidationError(
                f"proposal legs[{index}] requires executable {quote_key} quote"
            )
        price = _decimal(f"proposal legs[{index}].{quote_key}", quote[quote_key])
        sign = Decimal("1") if side == "BUY" else Decimal("-1")
        total += sign * price * ratio * multiplier
    return total


def _require_limit_matches(
    intent: Mapping[str, object],
    limit_price: Decimal,
    executable_price: Decimal,
) -> None:
    # Combo leg sides are already the approved final directions.  A top-level
    # SELL would invert the entire BAG at many brokers, so BUY is the only safe
    # wrapper action for both debit and signed-credit combinations.
    if intent.get("action") != "BUY":
        raise BridgeValidationError(
            "instruction action must be BUY to preserve approved combo leg directions"
        )
    if limit_price != executable_price:
        raise BridgeValidationError(
            "limit_price must exactly equal the signed executable combo price"
        )


def _validate_execution_result(
    result: Mapping[str, object],
) -> tuple[str, str]:
    _assert_safe_execution_metadata(result)
    raise BridgeValidationError(
        "creator review destination contract is unavailable"
    )


def _assert_safe_execution_metadata(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            folded = str(key).casefold()
            compact = re.sub(r"[^a-z0-9]", "", folded)
            if compact in {
                "brokerorderid",
                "orderid",
                "permid",
                "tradeid",
                "fillid",
                "executionid",
            }:
                raise BridgeValidationError(
                    "execution_result cannot contain broker order or fill identifiers"
                )
            if (
                ("transmit" in folded or "submit" in folded)
                and key not in {"order_submitted", "transmitted_to_broker"}
            ):
                raise BridgeValidationError(
                    "execution_result contains an unsafe submit/transmit field"
                )
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
                raise BridgeValidationError(
                    "execution_result cannot contain authentication material"
                )
            _assert_safe_execution_metadata(item)
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for item in value:
            _assert_safe_execution_metadata(item)


def _contract_identity(leg: Mapping[str, object], *, field: str) -> object:
    for key in (
        "contract_id_ex",
        "contract_id",
        "conid",
        "broker_contract_id",
        "contract",
    ):
        if key in leg:
            value = leg[key]
            if value is None or value == "":
                break
            if isinstance(value, Mapping):
                nested = _contract_identity(value, field=f"{field}.{key}")
                return {key: nested}
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                return {key: value}
            raise BridgeValidationError(f"{field}.{key} is not a contract identifier")
    raise BridgeValidationError(f"{field} requires a contract identifier")


def _leg_side(leg: Mapping[str, object], *, field: str) -> str:
    for key in ("action", "side", "position_side"):
        if key in leg:
            return _side(leg[key], field=f"{field}.{key}")
    raise BridgeValidationError(f"{field} requires BUY/SELL side")


def _side(value: object, *, field: str) -> str:
    side = _upper_text(value, field)
    aliases = {"BOT": "BUY", "LONG": "BUY", "SLD": "SELL", "SHORT": "SELL"}
    side = aliases.get(side, side)
    if side not in {"BUY", "SELL"}:
        raise BridgeValidationError(f"{field} must be BUY or SELL")
    return side


def _leg_ratio(leg: Mapping[str, object], *, field: str) -> Decimal:
    for key in ("ratio", "quantity"):
        if key in leg:
            return _positive_integral_decimal(f"{field}.{key}", leg[key])
    return Decimal("1")


def _positive_integral_decimal(field: str, value: object) -> Decimal:
    result = _decimal(field, value)
    if result <= 0 or result != result.to_integral_value():
        raise BridgeValidationError(f"{field} must be a positive integer")
    return result


def _decimal(field: str, value: object) -> Decimal:
    if isinstance(value, bool):
        raise BridgeValidationError(f"{field} must be Decimal-compatible")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise BridgeValidationError(f"{field} must be finite Decimal-compatible") from exc
    if not result.is_finite():
        raise BridgeValidationError(f"{field} must be finite Decimal-compatible")
    return result


def _upper_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BridgeValidationError(f"{field} must be a nonblank string")
    return value.strip().upper()


def _require_fresh_observation(
    now: datetime,
    observed_at: datetime,
    *,
    label: str,
) -> None:
    age = Decimal(str((now - observed_at).total_seconds()))
    if age < 0:
        raise BridgeValidationError(f"future {label} is forbidden")
    if age > MAX_QUOTE_AGE_SECONDS:
        raise BridgeValidationError(
            f"{label} must be between 0 and 5 seconds old"
        )


def _require_fresh_quote(now: datetime, observed_at: datetime) -> None:
    _require_fresh_observation(now, observed_at, label="quotes")


def _gate_identifier(field: str, value: object) -> str:
    if not isinstance(value, str):
        raise BridgeValidationError(f"{field} gate value must be a string")
    normalized = value.strip()
    try:
        _identifier(field, normalized)
    except ValueError as exc:
        raise BridgeValidationError(str(exc)) from exc
    return normalized


def _broker_gate_payload(
    *,
    approval_id: str,
    verified_at: datetime,
    snapshot_observed_at: datetime,
    quotes_observed_at: datetime,
    proposal_hash: str,
    quote_snapshot_id: str,
    net_liquidation_usd: Decimal,
    contract_definitions_hash: str,
) -> dict[str, object]:
    return {
        "gate_version": 1,
        "approval_id": approval_id,
        "verified_at": datetime_text(verified_at),
        "snapshot_observed_at": datetime_text(snapshot_observed_at),
        "quotes_observed_at": datetime_text(quotes_observed_at),
        "proposal_hash": proposal_hash,
        "quote_snapshot_id": quote_snapshot_id,
        "net_liquidation_usd": format(net_liquidation_usd, "f"),
        "contract_definitions_hash": contract_definitions_hash,
        "broker_snapshot_complete": True,
        "open_option_combinations": 0,
        "working_order_count": 0,
        "unsubmitted_instruction_count": 0,
    }


def _verify_broker_gate(
    request_row: sqlite3.Row,
    gate_row: sqlite3.Row,
    *,
    now: datetime,
) -> None:
    approval_id = str(request_row["approval_id"])
    if str(gate_row["approval_id"]) != approval_id:
        raise BridgeStateError("broker-gate approval binding is invalid")
    if any(
        int(gate_row[name]) != expected
        for name, expected in (
            ("broker_snapshot_complete", 1),
            ("open_option_combinations", 0),
            ("working_order_count", 0),
            ("unsubmitted_instruction_count", 0),
        )
    ):
        raise BridgeStateError("broker-gate boundary evidence is invalid")

    try:
        verified_at = utc_datetime(
            datetime.fromisoformat(str(gate_row["verified_at"])),
            field="broker_gate_verified_at",
        )
        snapshot_at = utc_datetime(
            datetime.fromisoformat(str(gate_row["snapshot_observed_at"])),
            field="snapshot_observed_at",
        )
        quotes_at = utc_datetime(
            datetime.fromisoformat(str(gate_row["quotes_observed_at"])),
            field="quotes_observed_at",
        )
    except (TypeError, ValueError) as exc:
        raise BridgeStateError("broker-gate timestamps are invalid") from exc
    _require_fresh_observation(now, verified_at, label="broker gate")
    _require_fresh_observation(now, snapshot_at, label="broker snapshot")
    _require_fresh_quote(now, quotes_at)
    if snapshot_at > verified_at or quotes_at > verified_at:
        raise BridgeStateError("broker-gate observations postdate verification")
    if str(request_row["authorized_at"]) != datetime_text(verified_at):
        raise BridgeStateError("broker-gate verification is not bound to authorization")
    if str(request_row["quotes_observed_at"]) != datetime_text(quotes_at):
        raise BridgeStateError("broker-gate quote timestamp binding is invalid")

    proposal_hash = str(request_row["proposal_hash"])
    if not hmac.compare_digest(str(gate_row["proposal_hash"]), proposal_hash):
        raise BridgeStateError("broker-gate proposal binding is invalid")
    snapshot_id = _gate_identifier(
        "quote_snapshot_id", str(gate_row["quote_snapshot_id"])
    )
    try:
        proposal = json.loads(str(request_row["proposal_json"]))
    except (TypeError, ValueError) as exc:
        raise BridgeStateError("authorized proposal JSON is invalid") from exc
    if not isinstance(proposal, Mapping) or proposal.get("quote_snapshot_id") != snapshot_id:
        raise BridgeStateError("broker-gate quote snapshot binding is invalid")
    nlv = _decimal("net_liquidation_usd", gate_row["net_liquidation_usd"])
    if nlv <= 0:
        raise BridgeStateError("broker-gate NLV evidence is invalid")
    definitions_hash = str(gate_row["contract_definitions_hash"])
    if re.fullmatch(r"[0-9a-f]{64}", definitions_hash) is None:
        raise BridgeStateError("broker-gate contract-definition evidence is invalid")
    payload = _broker_gate_payload(
        approval_id=approval_id,
        verified_at=verified_at,
        snapshot_observed_at=snapshot_at,
        quotes_observed_at=quotes_at,
        proposal_hash=proposal_hash,
        quote_snapshot_id=snapshot_id,
        net_liquidation_usd=nlv,
        contract_definitions_hash=definitions_hash,
    )
    if not hmac.compare_digest(
        str(gate_row["proof_hash"]), canonical_hash(payload)
    ):
        raise BridgeStateError("broker-gate proof hash is invalid")


_DECISION_BINDING_HASH_FIELDS = (
    "strategy_nav_snapshot_hash",
    "strategy_nav_contract_hash",
    "strategy_nav_ledger_head_hash",
    "risk_authority_marker_hash",
    "execution_cost_contract_hash",
    "current_policy_hash",
    "policy_authority_marker_hash",
)
_DECISION_BINDING_VERSION_FIELDS = (
    "risk_authority_version",
    "execution_cost_contract_version",
    "current_policy_version",
)
_ENTRY_TIME_DECISION_FIELDS = frozenset(
    {
        "mode",
        "allowed",
        "reason_codes",
        "evaluated_at_utc",
        "et_trading_date",
        "expiration",
        "dte",
        "policy_version",
        "policy_hash",
        "calendar_hash",
        "calendar_source_hash",
        "exception_hash",
        "transition_proof_hash",
        "session_open_utc",
        "session_close_utc",
        "decision_hash",
    }
)


def _digest(field: str, value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise BridgeValidationError(f"{field} must be SHA-256 hex")
    return value


def _optional_digest(field: str, value: object) -> str | None:
    return None if value is None else _digest(field, value)


def _decision_date(field: str, value: object) -> date:
    if isinstance(value, datetime):
        raise BridgeValidationError(f"{field} must be an ISO date")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise BridgeValidationError(f"{field} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise BridgeValidationError(f"{field} must be an ISO date") from exc


def _decision_timestamp(field: str, value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text_value = value.strip()
        try:
            parsed = datetime.fromisoformat(
                text_value[:-1] + "+00:00"
                if text_value.endswith(("Z", "z"))
                else text_value
            )
        except ValueError as exc:
            raise BridgeValidationError(
                f"{field} must be an ISO datetime"
            ) from exc
    else:
        raise BridgeValidationError(f"{field} must be an ISO datetime")
    try:
        return utc_datetime(parsed, field=field)
    except (TypeError, ValueError) as exc:
        raise BridgeValidationError(str(exc)) from exc


def _normalize_entry_time_decision(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise BridgeValidationError(
            "decision_bindings.entry_time_decision must be an object"
        )
    payload = _canonical_mapping("entry_time_decision", value)
    if set(payload) != _ENTRY_TIME_DECISION_FIELDS:
        raise BridgeValidationError(
            "decision_bindings.entry_time_decision fields are invalid"
        )
    if payload.get("mode") != "ENTRY" or payload.get("allowed") is not True:
        raise BridgeValidationError("entry time decision must allow ENTRY")
    reason_codes = payload.get("reason_codes")
    if not isinstance(reason_codes, list) or reason_codes:
        raise BridgeValidationError(
            "allowed entry_time_decision.reason_codes must be empty"
        )
    dte = payload.get("dte")
    if isinstance(dte, bool) or not isinstance(dte, int):
        raise BridgeValidationError("entry_time_decision.dte must be an integer")
    decision = TimePolicyDecision(
        mode=str(payload.get("mode")),
        allowed=payload.get("allowed") is True,
        reason_codes=tuple(reason_codes),
        evaluated_at_utc=_decision_timestamp(
            "entry_time_decision.evaluated_at_utc",
            payload.get("evaluated_at_utc"),
        ),
        et_trading_date=_decision_date(
            "entry_time_decision.et_trading_date",
            payload.get("et_trading_date"),
        ),
        expiration=_decision_date(
            "entry_time_decision.expiration",
            payload.get("expiration"),
        ),
        dte=dte,
        policy_version=_gate_identifier(
            "entry_time_decision.policy_version",
            payload.get("policy_version"),
        ),
        policy_hash=_digest(
            "entry_time_decision.policy_hash",
            payload.get("policy_hash"),
        ),
        calendar_hash=_digest(
            "entry_time_decision.calendar_hash",
            payload.get("calendar_hash"),
        ),
        calendar_source_hash=_digest(
            "entry_time_decision.calendar_source_hash",
            payload.get("calendar_source_hash"),
        ),
        exception_hash=_optional_digest(
            "entry_time_decision.exception_hash",
            payload.get("exception_hash"),
        ),
        transition_proof_hash=_optional_digest(
            "entry_time_decision.transition_proof_hash",
            payload.get("transition_proof_hash"),
        ),
        session_open_utc=_decision_timestamp(
            "entry_time_decision.session_open_utc",
            payload.get("session_open_utc"),
        ),
        session_close_utc=_decision_timestamp(
            "entry_time_decision.session_close_utc",
            payload.get("session_close_utc"),
        ),
        decision_hash=_digest(
            "entry_time_decision.decision_hash",
            payload.get("decision_hash"),
        ),
    )
    if not decision.verify_hash():
        raise BridgeValidationError("entry time decision hash mismatch")
    if not PERMANENT_MINIMUM_ENTRY_DTE <= decision.dte <= NORMAL_MAXIMUM_ENTRY_DTE:
        raise BridgeValidationError("entry time decision DTE is outside 7-35")
    if (decision.expiration - decision.et_trading_date).days != decision.dte:
        raise BridgeValidationError("entry time decision DTE binding is invalid")
    if decision.dte < 14 and decision.exception_hash is None:
        raise BridgeValidationError("7-13 DTE entry requires an exception hash")
    if decision.dte >= 14 and decision.exception_hash is not None:
        raise BridgeValidationError("normal-band entry cannot carry an exception hash")
    if decision.transition_proof_hash is not None:
        raise BridgeValidationError("entry time decision cannot use a management proof")
    assert decision.session_open_utc is not None
    assert decision.session_close_utc is not None
    if not (
        decision.session_open_utc
        <= decision.evaluated_at_utc
        < decision.session_close_utc
    ):
        raise BridgeValidationError("entry time decision is outside its session")
    open_et = decision.session_open_utc.astimezone(US_OPTIONS_TIMEZONE)
    close_et = decision.session_close_utc.astimezone(US_OPTIONS_TIMEZONE)
    evaluated_et = decision.evaluated_at_utc.astimezone(US_OPTIONS_TIMEZONE)
    if (
        open_et.date() != decision.et_trading_date
        or close_et.date() != decision.et_trading_date
        or evaluated_et.date() != decision.et_trading_date
        or open_et.timetz().replace(tzinfo=None) != time(9, 30)
        or close_et.timetz().replace(tzinfo=None) not in {time(13, 0), time(16, 0)}
    ):
        raise BridgeValidationError("entry time decision session binding is invalid")
    return decision.as_dict()


def _normalize_decision_bindings(
    value: Mapping[str, object],
) -> dict[str, object]:
    result = _canonical_mapping("decision_bindings", value)
    if result.get("schema") != "options_copilot.bridge.decision_bindings.v2":
        raise BridgeValidationError("decision_bindings.schema is invalid")
    nav = _decimal("decision_bindings.strategy_nav_usd", result.get("strategy_nav_usd"))
    if nav <= 0:
        raise BridgeValidationError("decision_bindings.strategy_nav_usd must be positive")
    result["strategy_nav_usd"] = nav
    for field in _DECISION_BINDING_HASH_FIELDS:
        result[field] = _digest(f"decision_bindings.{field}", result.get(field))
    for field in _DECISION_BINDING_VERSION_FIELDS:
        result[field] = _gate_identifier(
            f"decision_bindings.{field}", result.get(field)
        )
    result["entry_time_decision"] = _normalize_entry_time_decision(
        result.get("entry_time_decision")
    )
    return result


def _require_current_entry_time_decision(
    now: datetime,
    bindings: Mapping[str, object],
) -> None:
    normalized = _normalize_decision_bindings(bindings)
    entry_time_decision = normalized["entry_time_decision"]
    assert isinstance(entry_time_decision, Mapping)
    evaluated_at = _decision_timestamp(
        "entry_time_decision.evaluated_at_utc",
        entry_time_decision["evaluated_at_utc"],
    )
    _require_fresh_observation(
        now,
        evaluated_at,
        label="entry time decision",
    )
    session_open = _decision_timestamp(
        "entry_time_decision.session_open_utc",
        entry_time_decision["session_open_utc"],
    )
    session_close = _decision_timestamp(
        "entry_time_decision.session_close_utc",
        entry_time_decision["session_close_utc"],
    )
    if not session_open <= now < session_close:
        raise BridgeValidationError(
            "external attempt is outside the bound US options session"
        )


def _atomic_authorization_payload(
    *,
    approval_id: str,
    verified_at: datetime,
    proposal_hash: str,
    gate: AtomicBrokerGateInput,
) -> dict[str, object]:
    return {
        "gate_version": 2,
        "approval_id": approval_id,
        "verified_at": datetime_text(verified_at),
        "proposal_hash": proposal_hash,
        "snapshot_hash": gate.snapshot_hash,
        "decision_bindings_hash": gate.decision_bindings_hash,
        "contract_definitions_hash": gate.contract_definitions_hash,
    }


def _stored_document(row: sqlite3.Row, field: str) -> Mapping[str, object]:
    try:
        decoded = thaw_json(freeze_json(json.loads(str(row[field]))))
    except (TypeError, ValueError) as exc:
        raise BridgeStateError(f"stored {field} is invalid") from exc
    if not isinstance(decoded, Mapping):
        raise BridgeStateError(f"stored {field} is not an object")
    return decoded


def _verify_atomic_authorization_gate(
    request_row: sqlite3.Row,
    atomic_row: sqlite3.Row,
    *,
    now: datetime,
) -> None:
    approval_id = str(request_row["approval_id"])
    if str(atomic_row["approval_id"]) != approval_id:
        raise BridgeStateError("atomic broker-gate approval binding is invalid")
    verified_at = utc_datetime(
        datetime.fromisoformat(str(atomic_row["verified_at"])),
        field="atomic_broker_gate.verified_at",
    )
    _require_fresh_observation(now, verified_at, label="atomic broker gate")
    if str(request_row["authorized_at"]) != datetime_text(verified_at):
        raise BridgeStateError("atomic broker gate is not bound to authorization")
    proposal_hash = _digest("proposal_hash", str(atomic_row["proposal_hash"]))
    if not hmac.compare_digest(proposal_hash, str(request_row["proposal_hash"])):
        raise BridgeStateError("atomic broker-gate proposal binding is invalid")
    snapshot_hash = _digest("snapshot_hash", str(atomic_row["snapshot_hash"]))
    snapshot = _stored_document(atomic_row, "snapshot_json")
    if canonical_hash(snapshot) != snapshot_hash:
        raise BridgeStateError("stored atomic broker snapshot hash mismatch")
    bindings_hash = _digest(
        "decision_bindings_hash", str(atomic_row["decision_bindings_hash"])
    )
    bindings = _normalize_decision_bindings(
        _stored_document(atomic_row, "decision_bindings_json")
    )
    if canonical_hash(bindings) != bindings_hash:
        raise BridgeStateError("stored decision bindings hash mismatch")
    definitions_hash = _digest(
        "contract_definitions_hash",
        str(atomic_row["contract_definitions_hash"]),
    )
    payload = {
        "gate_version": 2,
        "approval_id": approval_id,
        "verified_at": datetime_text(verified_at),
        "proposal_hash": proposal_hash,
        "snapshot_hash": snapshot_hash,
        "decision_bindings_hash": bindings_hash,
        "contract_definitions_hash": definitions_hash,
    }
    if not hmac.compare_digest(
        str(atomic_row["proof_hash"]), canonical_hash(payload)
    ):
        raise BridgeStateError("atomic broker-gate proof hash is invalid")


def _atomic_identity_projection(snapshot: Mapping[str, object]) -> dict[str, object]:
    if snapshot.get("status") != "COMPLETE":
        raise BridgeValidationError("atomic broker snapshot is not COMPLETE")
    state = snapshot.get("state_evidence")
    if not isinstance(state, Mapping):
        raise BridgeValidationError("atomic state evidence is missing")
    state_hashes: dict[str, str] = {}
    for name in (
        "account",
        "positions",
        "working_orders",
        "unsubmitted_instructions",
    ):
        evidence = state.get(name)
        if not isinstance(evidence, Mapping):
            raise BridgeValidationError(f"atomic {name} evidence is missing")
        if evidence.get("known") is not True or evidence.get("stable") is not True:
            raise BridgeValidationError(f"atomic {name} evidence is not stable")
        state_hashes[name] = _digest(
            f"atomic {name} post_hash", evidence.get("post_hash")
        )
    secdefs = snapshot.get("secdef_evidence")
    if not isinstance(secdefs, Sequence) or isinstance(
        secdefs, (str, bytes, bytearray, memoryview)
    ):
        raise BridgeValidationError("atomic secdef evidence is missing")
    identities: list[dict[str, object]] = []
    for index, item in enumerate(secdefs):
        if not isinstance(item, Mapping):
            raise BridgeValidationError(f"atomic secdef[{index}] is invalid")
        if item.get("stable") is not True or item.get("standard_contract") is not True:
            raise BridgeValidationError(f"atomic secdef[{index}] is not stable standard")
        identities.append(
            {
                "contract_id": item.get("contract_id"),
                "post_hash": _digest(
                    f"atomic secdef[{index}].post_hash", item.get("post_hash")
                ),
                "post_identity": item.get("post_identity"),
            }
        )
    quotes = snapshot.get("quotes")
    if not isinstance(quotes, Sequence) or isinstance(
        quotes, (str, bytes, bytearray, memoryview)
    ):
        raise BridgeValidationError("atomic quote evidence is missing")
    quote_contract_ids = sorted(
        int(item["contract_id"])
        for item in quotes
        if isinstance(item, Mapping) and "contract_id" in item
    )
    if len(quote_contract_ids) != len(quotes):
        raise BridgeValidationError("atomic quote contract identity is invalid")
    return {
        "state_hashes": state_hashes,
        "secdefs": sorted(identities, key=lambda item: int(item["contract_id"])),
        "quote_contract_ids": quote_contract_ids,
    }


def _atomic_account_nlv(snapshot: Mapping[str, object]) -> Decimal:
    state = snapshot.get("state_evidence")
    if not isinstance(state, Mapping):
        raise BridgeValidationError("atomic state evidence is missing")
    account = state.get("account")
    if not isinstance(account, Mapping):
        raise BridgeValidationError("atomic account evidence is missing")
    if account.get("known") is not True or account.get("stable") is not True:
        raise BridgeValidationError("atomic account evidence is not stable")
    account_state = account.get("state")
    if not isinstance(account_state, Mapping):
        raise BridgeValidationError("atomic account state is missing")
    values = [
        account_state[key]
        for key in ("net_liquidation_usd", "net_liquidation", "nlv_usd", "nlv")
        if key in account_state
    ]
    if not values:
        raise BridgeValidationError("atomic account NLV is missing")
    parsed = tuple(_decimal("atomic account NLV", value) for value in values)
    if any(value <= 0 for value in parsed):
        raise BridgeValidationError("atomic account NLV must be positive")
    if any(value != parsed[0] for value in parsed[1:]):
        raise BridgeValidationError("atomic account NLV values conflict")
    return parsed[0]


def _decision_binding_projection(bindings: Mapping[str, object]) -> dict[str, object]:
    normalized = _normalize_decision_bindings(bindings)
    entry_time_decision = normalized["entry_time_decision"]
    assert isinstance(entry_time_decision, Mapping)
    return {
        "decision_context": {
            key: value
            for key, value in normalized.items()
            if key not in {"strategy_nav_snapshot_hash", "entry_time_decision"}
        },
        "entry_time_invariants": {
            key: entry_time_decision[key]
            for key in (
                "mode",
                "allowed",
                "reason_codes",
                "et_trading_date",
                "expiration",
                "dte",
                "policy_version",
                "policy_hash",
                "exception_hash",
                "transition_proof_hash",
                "session_open_utc",
                "session_close_utc",
            )
        },
    }


def _snapshot_built_at(snapshot: Mapping[str, object]) -> datetime:
    value = snapshot.get("built_at")
    if not isinstance(value, str):
        raise BridgeValidationError("atomic snapshot built_at is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise BridgeValidationError("atomic snapshot built_at is invalid") from exc
    return utc_datetime(parsed, field="atomic snapshot built_at")


def _verify_reserve_atomic_gate(
    *,
    approval_id: str,
    request_row: sqlite3.Row,
    authorization_gate_row: sqlite3.Row,
    reserve_gate: AtomicBrokerGateInput,
    verified_at: datetime,
) -> dict[str, object]:
    _require_fresh_observation(
        verified_at,
        _snapshot_built_at(reserve_gate.snapshot_payload),
        label="reserve broker snapshot",
    )
    _require_fresh_quote(verified_at, reserve_gate.quotes_observed_at)
    authorized_snapshot = _stored_document(authorization_gate_row, "snapshot_json")
    if _atomic_identity_projection(authorized_snapshot) != _atomic_identity_projection(
        reserve_gate.snapshot_payload
    ):
        raise BridgeValidationError(
            "reserve-time position/order/instruction/secdef identity changed"
        )
    authorized_bindings = _normalize_decision_bindings(
        _stored_document(authorization_gate_row, "decision_bindings_json")
    )
    authorized_projection = _decision_binding_projection(authorized_bindings)
    reserve_projection = _decision_binding_projection(
        reserve_gate.decision_bindings
    )
    if (
        authorized_projection["decision_context"]
        != reserve_projection["decision_context"]
    ):
        raise BridgeValidationError(
            "reserve-time Strategy NAV or decision binding changed"
        )
    if (
        authorized_projection["entry_time_invariants"]
        != reserve_projection["entry_time_invariants"]
    ):
        raise BridgeValidationError(
            "reserve-time entry time-policy decision binding changed"
        )
    authorized_definitions_hash = _digest(
        "authorized contract_definitions_hash",
        str(authorization_gate_row["contract_definitions_hash"]),
    )
    if reserve_gate.contract_definitions_hash != authorized_definitions_hash:
        raise BridgeValidationError(
            "reserve-time contract-definition hash changed"
        )
    proposal_snapshot_id = reserve_gate.repriced_proposal.get("quote_snapshot_id")
    if proposal_snapshot_id != reserve_gate.snapshot_payload.get("quote_batch_id"):
        raise BridgeValidationError(
            "reserve-time proposal is not bound to the atomic quote batch"
        )
    return {
        "gate_version": 1,
        "approval_id": approval_id,
        "verified_at": datetime_text(verified_at),
        "authorized_proposal_hash": str(request_row["proposal_hash"]),
        "authorized_snapshot_hash": str(authorization_gate_row["snapshot_hash"]),
        "snapshot_hash": reserve_gate.snapshot_hash,
        "decision_bindings_hash": reserve_gate.decision_bindings_hash,
        "proposal_hash": reserve_gate.proposal_hash,
        "quotes_observed_at": datetime_text(reserve_gate.quotes_observed_at),
        "contract_definitions_hash": reserve_gate.contract_definitions_hash,
    }


def _token_digest(token: str) -> str:
    if not isinstance(token, str) or not 16 <= len(token) <= 512:
        # Hashing a fixed placeholder keeps the authentication path uniform
        # without ever reflecting the supplied token in an error.
        token = "invalid-token-placeholder"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _identifier(field: str, value: str) -> None:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a valid bridge identifier")


def _optional_datetime_text(value: datetime | None) -> str | None:
    return None if value is None else datetime_text(value)


def _row_to_record(row: sqlite3.Row) -> BridgeRecord:
    def timestamp(name: str) -> datetime | None:
        value = row[name]
        return None if value is None else datetime.fromisoformat(str(value))

    def document(name: str) -> Mapping[str, object] | None:
        value = row[name]
        if value is None:
            return None
        decoded = thaw_json(freeze_json(json.loads(str(value))))
        if not isinstance(decoded, Mapping):
            raise BridgeError(f"stored {name} is not an object")
        return decoded

    limit = row["limit_price"]
    authority_proof, authority_binding_hash, authority_proof_hash = (
        _claim_authority_audit_from_row(row)
    )
    return BridgeRecord(
        sequence=int(row["sequence"]),
        approval_id=str(row["approval_id"]),
        status=BridgeStatus(str(row["status"])),
        claimed_at=datetime.fromisoformat(str(row["claimed_at"])),
        authorized_at=timestamp("authorized_at"),
        quotes_observed_at=timestamp("quotes_observed_at"),
        proposal_hash=None if row["proposal_hash"] is None else str(row["proposal_hash"]),
        proposal=document("proposal_json"),
        intent_hash=None if row["intent_hash"] is None else str(row["intent_hash"]),
        instruction_intent=document("intent_json"),
        limit_price=None if limit is None else Decimal(str(limit)),
        completed_at=timestamp("completed_at"),
        execution_hash=(
            None if row["execution_hash"] is None else str(row["execution_hash"])
        ),
        # Historical completion payloads remain stored for ledger integrity, but
        # no public bridge projection may reveal an uncontracted destination.
        execution_result=None,
        instruction_id=None,
        deep_link=None,
        failed_at=timestamp("failed_at"),
        failure_reason=(
            None if row["failure_reason"] is None else str(row["failure_reason"])
        ),
        external_call_started_at=timestamp("external_call_started_at"),
        snapshot_observed_at=timestamp("snapshot_observed_at"),
        broker_gate_verified_at=timestamp("broker_gate_verified_at"),
        authorized_broker_snapshot_hash=(
            None
            if row["authorized_broker_snapshot_hash"] is None
            else str(row["authorized_broker_snapshot_hash"])
        ),
        reserve_broker_snapshot_hash=(
            None
            if row["reserve_broker_snapshot_hash"] is None
            else str(row["reserve_broker_snapshot_hash"])
        ),
        reserve_gate_verified_at=timestamp("reserve_gate_verified_at"),
        reserve_decision_bindings_hash=(
            None
            if row["reserve_decision_bindings_hash"] is None
            else str(row["reserve_decision_bindings_hash"])
        ),
        current_authority_checked_at=(
            None if authority_proof is None else authority_proof.checked_at
        ),
        current_authority_binding_hash=authority_binding_hash,
        current_authority_proof_hash=authority_proof_hash,
    )


# Public aliases keep the composition layer descriptive without duplicating
# implementations or creating any implicit connector behavior.
LocalCodexBridge = CodexBridgeStore
CodexBridgeStateMachine = CodexBridgeStore
CodexExternalBridge = CodexBridgeStore
ExternalBridgeStore = CodexBridgeStore


__all__ = [
    "CURRENT_AUTHORITY_PROOF_MAX_AGE_SECONDS",
    "CURRENT_AUTHORITY_PROOF_SCHEMA",
    "MAX_QUOTE_AGE_SECONDS",
    "AtomicBrokerGateInput",
    "BridgeAlreadyClaimed",
    "BridgeActiveHandoffExists",
    "BridgeApprovalRejected",
    "BridgeError",
    "BridgeExternalCallAlreadyAttempted",
    "BridgeRecord",
    "BridgeStateError",
    "BridgeStatus",
    "BridgeTokenError",
    "BridgeValidationError",
    "CodexBridgeStateMachine",
    "CodexBridgeStore",
    "CodexExternalBridge",
    "CurrentAuthorityProof",
    "ExternalBridgeStore",
    "LocalCodexBridge",
    "UNKNOWN_OUTCOME_REASON_PREFIX",
    "TRUSTED_APPROVAL_ISSUER",
]
