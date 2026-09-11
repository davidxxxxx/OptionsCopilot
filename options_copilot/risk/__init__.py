"""Finite-loss option payoff and locked account-risk evaluation."""

from .authorization import RiskAuthorityTier, RiskTierAuthority

from .payoff import (
    BreakEvenRegion,
    ExpirationPayoff,
    PayoffSegment,
    PayoffStatus,
    PayoffUnavailableError,
    RiskRejection,
    analyze_expiration_payoff,
)
from .policy import RiskAssessment, RiskEngine, RiskPolicy
from .resolver import (
    CurrentRiskAuthorityResolver,
    PolicyLedgerRiskAuthorityMarkerSource,
    RiskAuthorityCurrentnessError,
    RiskAuthorityMarkerSource,
)
from .time_policy import (
    DTE_EXCEPTION_SCHEMA,
    DteEntryExceptionAuthority,
    NORMAL_MAXIMUM_ENTRY_DTE,
    NORMAL_MINIMUM_ENTRY_DTE,
    OptionTimePolicy,
    P2ManagementTransitionProof,
    PERMANENT_MINIMUM_ENTRY_DTE,
    TIME_POLICY_VERSION,
    TimePolicyDecision,
)

__all__ = [
    "BreakEvenRegion",
    "CurrentRiskAuthorityResolver",
    "DTE_EXCEPTION_SCHEMA",
    "DteEntryExceptionAuthority",
    "ExpirationPayoff",
    "PayoffSegment",
    "PayoffStatus",
    "PayoffUnavailableError",
    "NORMAL_MAXIMUM_ENTRY_DTE",
    "NORMAL_MINIMUM_ENTRY_DTE",
    "OptionTimePolicy",
    "PolicyLedgerRiskAuthorityMarkerSource",
    "P2ManagementTransitionProof",
    "PERMANENT_MINIMUM_ENTRY_DTE",
    "RiskAssessment",
    "RiskAuthorityCurrentnessError",
    "RiskAuthorityMarkerSource",
    "RiskAuthorityTier",
    "RiskEngine",
    "RiskPolicy",
    "RiskRejection",
    "RiskTierAuthority",
    "TIME_POLICY_VERSION",
    "TimePolicyDecision",
    "analyze_expiration_payoff",
]
