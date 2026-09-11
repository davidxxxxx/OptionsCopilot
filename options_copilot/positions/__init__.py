"""Close/reduce-only position-management proof surface."""

from .generator import (
    ManagementCandidate,
    ManagementCandidateGenerator,
    ManagementExecutionLeg,
    ManagementGenerationResult,
    ManagementGenerationStatus,
    ManagementPayoffMetrics,
    preview_json,
)
from .manager import PositionManager, TransitionRejected, prove_transition
from .models import (
    AuthoritativePosition,
    CapitalUsageProof,
    PositionDelta,
    PositionManagementKind,
    PositionRiskMetrics,
    PositionTransitionProof,
    SecDefBinding,
)
from .runtime_adapter import (
    ExecutionCostContractProvider,
    ExitContractProvider,
    MarketDataGate,
    ProductionManagementCoordinator,
    SnapshotBuilder,
)

__all__ = [
    "AuthoritativePosition",
    "CapitalUsageProof",
    "ExecutionCostContractProvider",
    "ExitContractProvider",
    "ManagementCandidate",
    "ManagementCandidateGenerator",
    "ManagementExecutionLeg",
    "ManagementGenerationResult",
    "ManagementGenerationStatus",
    "ManagementPayoffMetrics",
    "MarketDataGate",
    "PositionDelta",
    "PositionManagementKind",
    "PositionManager",
    "PositionRiskMetrics",
    "PositionTransitionProof",
    "ProductionManagementCoordinator",
    "SecDefBinding",
    "SnapshotBuilder",
    "TransitionRejected",
    "preview_json",
    "prove_transition",
]
