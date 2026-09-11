"""Local, review-only handoff boundary for an external Codex workflow.

The bridge deliberately has no broker or Connector dependency.  It turns a
short-lived :class:`~options_copilot.approval.ProposalApprovalStore` approval
into a durable, token-bound review instruction and records only a safe result.
"""

from .store import (
    CURRENT_AUTHORITY_PROOF_MAX_AGE_SECONDS,
    CURRENT_AUTHORITY_PROOF_SCHEMA,
    MAX_QUOTE_AGE_SECONDS,
    AtomicBrokerGateInput,
    BridgeAlreadyClaimed,
    BridgeActiveHandoffExists,
    BridgeApprovalRejected,
    BridgeError,
    BridgeExternalCallAlreadyAttempted,
    BridgeRecord,
    BridgeStateError,
    BridgeStatus,
    BridgeTokenError,
    BridgeValidationError,
    CodexBridgeStateMachine,
    CodexBridgeStore,
    CodexExternalBridge,
    CurrentAuthorityProof,
    ExternalBridgeStore,
    LocalCodexBridge,
    UNKNOWN_OUTCOME_REASON_PREFIX,
    TRUSTED_APPROVAL_ISSUER,
)
from .coordinator import (
    BrokerGateResult,
    BridgeBrokerSnapshotRejected,
    BridgeDecisionContext,
    BridgeUnknownOutcomeError,
    CoordinatorStatus,
    HARD_BROKER_SNAPSHOT_AGE_SECONDS,
    LocalCodexBridgeCoordinator,
    ReviewInstructionCreator,
)

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
    "BrokerGateResult",
    "BridgeBrokerSnapshotRejected",
    "BridgeDecisionContext",
    "BridgeUnknownOutcomeError",
    "CoordinatorStatus",
    "HARD_BROKER_SNAPSHOT_AGE_SECONDS",
    "LocalCodexBridgeCoordinator",
    "ReviewInstructionCreator",
]
