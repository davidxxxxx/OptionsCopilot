"""Immutable risk-tier authority resolution.

NORMAL is always the safe default.  An A-grade ceiling is available only when
one append-only, human-attested P9 marker validates against every current
governance binding supplied by the caller.  Proposal fields, environment
variables, model output, ranking scores, and campaign progress are not inputs.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import re

from options_copilot.learning.markers import (
    A_GRADE_AUTHORITY_SCHEMA,
    AuthoritySignatureVerifier,
    AuthorityValidationError,
    verify_a_grade_authority,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    utc_datetime,
)


NORMAL_AUTHORITY_VERSION = "v1"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_VERSION_RE = re.compile(r"v[1-9][0-9]*(?:\.[0-9]+)*\Z")
_UNBOUND_POLICY_HASH = canonical_hash(
    {"schema": "options_copilot.risk.unbound_policy.v1"}
)
_UNBOUND_POLICY_MARKER_HASH = canonical_hash(
    {
        "schema": "options_copilot.risk.unbound_policy_authority.v1",
        "policy_hash": _UNBOUND_POLICY_HASH,
    }
)


class RiskAuthorityTier(str, Enum):
    NORMAL = "NORMAL"
    A_GRADE = "A_GRADE"


@dataclass(frozen=True, slots=True)
class RiskTierAuthority:
    """One resolved, immutable risk authority state."""

    version: str
    tier: RiskAuthorityTier
    risk_contract_hash: str
    risk_authority_marker_hash: str
    a_grade_approved: bool
    actor: str | None
    signed_at: datetime | None
    expires_at: datetime | None
    rejection_reasons: tuple[str, ...] = ()
    current_policy_version: str = "v1"
    current_policy_hash: str = _UNBOUND_POLICY_HASH
    policy_authority_marker_hash: str = _UNBOUND_POLICY_MARKER_HASH
    proposal_hash: str | None = None
    candidate_hash: str | None = None
    execution_cost_version: str | None = None
    execution_cost_hash: str | None = None
    ranking_basis_hash: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or _VERSION_RE.fullmatch(self.version) is None:
            raise ValueError("risk authority version is invalid")
        if not isinstance(self.tier, RiskAuthorityTier):
            raise TypeError("tier must be RiskAuthorityTier")
        _digest("risk_contract_hash", self.risk_contract_hash)
        _digest("risk_authority_marker_hash", self.risk_authority_marker_hash)
        if not isinstance(self.current_policy_version, str) or _VERSION_RE.fullmatch(self.current_policy_version) is None:
            raise ValueError("current policy version is invalid")
        _digest("current_policy_hash", self.current_policy_hash)
        _digest("policy_authority_marker_hash", self.policy_authority_marker_hash)
        for field in ("proposal_hash", "candidate_hash", "execution_cost_hash", "ranking_basis_hash"):
            value = getattr(self, field)
            if value is not None:
                _digest(field, value)
        if self.execution_cost_version is not None and _VERSION_RE.fullmatch(self.execution_cost_version) is None:
            raise ValueError("execution cost version is invalid")
        if not isinstance(self.a_grade_approved, bool):
            raise TypeError("a_grade_approved must be a boolean")
        if self.a_grade_approved is not (self.tier is RiskAuthorityTier.A_GRADE):
            raise ValueError("A-grade approval and authority tier disagree")
        if self.signed_at is not None:
            object.__setattr__(
                self,
                "signed_at",
                utc_datetime(self.signed_at, field="signed_at"),
            )
        if self.expires_at is not None:
            object.__setattr__(
                self,
                "expires_at",
                utc_datetime(self.expires_at, field="expires_at"),
            )
        object.__setattr__(
            self,
            "rejection_reasons",
            tuple(sorted(set(self.rejection_reasons))),
        )

    @property
    def marker_hash(self) -> str:
        return self.risk_authority_marker_hash

    @property
    def risk_authority_version(self) -> str:
        return self.version

    def allows_risk_fraction(self, value: Decimal) -> bool:
        """Apply the immutable Decimal 10/15/20 authorization boundary."""

        if not isinstance(value, Decimal):
            raise TypeError("risk fraction must be a Decimal")
        if not value.is_finite() or value < Decimal("0"):
            return False
        if value >= Decimal("0.20"):
            return False
        if value <= Decimal("0.10"):
            return True
        return self.a_grade_approved and value <= Decimal("0.15")

    @classmethod
    def normal(
        cls,
        risk_contract_hash: str,
        *,
        current_policy_version: str = "v1",
        current_policy_hash: str | None = None,
        policy_authority_marker_hash: str | None = None,
        rejection_reasons: tuple[str, ...] = (),
    ) -> "RiskTierAuthority":
        contract_hash = _digest("risk_contract_hash", risk_contract_hash)
        policy_hash = current_policy_hash or canonical_hash(
            {"schema": "options_copilot.risk.unbound_policy.v1", "risk_contract_hash": contract_hash}
        )
        policy_marker_hash = policy_authority_marker_hash or canonical_hash(
            {"schema": "options_copilot.risk.unbound_policy_authority.v1", "policy_hash": policy_hash}
        )
        marker_hash = canonical_hash(
            {
                "schema": "options_copilot.risk.normal_authority.v1",
                "version": NORMAL_AUTHORITY_VERSION,
                "tier": RiskAuthorityTier.NORMAL.value,
                "risk_contract_hash": contract_hash,
                "current_policy_version": current_policy_version,
                "current_policy_hash": policy_hash,
                "policy_authority_marker_hash": policy_marker_hash,
                "maximum_risk_fraction": "0.10",
            }
        )
        return cls(
            version=NORMAL_AUTHORITY_VERSION,
            tier=RiskAuthorityTier.NORMAL,
            risk_contract_hash=contract_hash,
            risk_authority_marker_hash=marker_hash,
            current_policy_version=current_policy_version,
            current_policy_hash=policy_hash,
            policy_authority_marker_hash=policy_marker_hash,
            proposal_hash=None,
            candidate_hash=None,
            execution_cost_version=None,
            execution_cost_hash=None,
            ranking_basis_hash=None,
            a_grade_approved=False,
            actor=None,
            signed_at=None,
            expires_at=None,
            rejection_reasons=rejection_reasons,
        )

    @classmethod
    def resolve(
        cls,
        *,
        risk_contract_hash: str,
        marker: Mapping[str, object] | None = None,
        asof: datetime | None = None,
        expected_evaluation_report_hash: str | None = None,
        expected_policy_hash: str | None = None,
        expected_reference_dataset_hash: str | None = None,
        expected_independence_hash: str | None = None,
        expected_proposal_hash: str | None = None,
        expected_candidate_hash: str | None = None,
        expected_current_policy_version: str | None = None,
        expected_policy_authority_marker_hash: str | None = None,
        expected_execution_cost_version: str | None = None,
        expected_execution_cost_hash: str | None = None,
        expected_ranking_basis_hash: str | None = None,
        signature_verifier: AuthoritySignatureVerifier | None = None,
        allow_test_authority: bool = False,
    ) -> "RiskTierAuthority":
        """Resolve A-grade or return a canonical NORMAL authority.

        A malformed or mismatched marker never raises the ceiling.  It resolves
        to NORMAL with stable reason codes so callers can journal the denial.
        """

        contract_hash = _digest("risk_contract_hash", risk_contract_hash)
        normal_version = expected_current_policy_version or "v1"
        normal_policy_hash = expected_policy_hash
        normal_policy_marker_hash = expected_policy_authority_marker_hash
        if marker is None:
            return cls.normal(
                contract_hash,
                current_policy_version=normal_version,
                current_policy_hash=normal_policy_hash,
                policy_authority_marker_hash=normal_policy_marker_hash,
            )
        if not isinstance(marker, Mapping):
            return cls.normal(
                contract_hash,
                current_policy_version=normal_version,
                current_policy_hash=normal_policy_hash,
                policy_authority_marker_hash=normal_policy_marker_hash,
                rejection_reasons=("A_GRADE_MARKER_NOT_AN_OBJECT",),
            )
        checked_at = utc_datetime(
            asof or datetime.now(timezone.utc),
            field="asof",
        )
        if marker.get("schema") == A_GRADE_AUTHORITY_SCHEMA:
            reasons = _validate_p9_marker(
                marker,
                checked_at=checked_at,
                risk_contract_hash=contract_hash,
                expected_evaluation_report_hash=expected_evaluation_report_hash,
                expected_policy_hash=expected_policy_hash,
                expected_reference_dataset_hash=expected_reference_dataset_hash,
                expected_independence_hash=expected_independence_hash,
                expected_proposal_hash=expected_proposal_hash,
                expected_candidate_hash=expected_candidate_hash,
                expected_current_policy_version=expected_current_policy_version,
                expected_policy_authority_marker_hash=expected_policy_authority_marker_hash,
                expected_execution_cost_version=expected_execution_cost_version,
                expected_execution_cost_hash=expected_execution_cost_hash,
                expected_ranking_basis_hash=expected_ranking_basis_hash,
                signature_verifier=signature_verifier,
                allow_test_authority=allow_test_authority,
            )
        else:
            reasons = ["A_GRADE_MARKER_LEGACY_OR_UNKNOWN_SCHEMA"]
        if reasons:
            return cls.normal(
                contract_hash,
                current_policy_version=normal_version,
                current_policy_hash=normal_policy_hash,
                policy_authority_marker_hash=normal_policy_marker_hash,
                rejection_reasons=tuple(reasons),
            )

        signed_at = _timestamp("signed_at", marker["signed_at"])
        expires_at = _timestamp("expires_at", marker["expires_at"])
        return cls(
            version=str(marker["version"]),
            tier=RiskAuthorityTier.A_GRADE,
            risk_contract_hash=contract_hash,
            risk_authority_marker_hash=str(marker["content_hash"]),
            current_policy_version=str(marker["current_policy_version"]),
            current_policy_hash=str(marker["current_policy_hash"]),
            policy_authority_marker_hash=str(marker["policy_authority_marker_hash"]),
            proposal_hash=str(marker["proposal_hash"]),
            candidate_hash=str(marker["candidate_hash"]),
            execution_cost_version=str(marker["execution_cost_version"]),
            execution_cost_hash=str(marker["execution_cost_hash"]),
            ranking_basis_hash=str(marker["ranking_basis_hash"]),
            a_grade_approved=True,
            actor=str(marker["actor"]),
            signed_at=signed_at,
            expires_at=expires_at,
        )


def _validate_p9_marker(
    marker: Mapping[str, object],
    *,
    checked_at: datetime,
    risk_contract_hash: str,
    expected_evaluation_report_hash: str | None,
    expected_policy_hash: str | None,
    expected_reference_dataset_hash: str | None,
    expected_independence_hash: str | None,
    expected_proposal_hash: str | None,
    expected_candidate_hash: str | None,
    expected_current_policy_version: str | None,
    expected_policy_authority_marker_hash: str | None,
    expected_execution_cost_version: str | None,
    expected_execution_cost_hash: str | None,
    expected_ranking_basis_hash: str | None,
    signature_verifier: AuthoritySignatureVerifier | None,
    allow_test_authority: bool,
) -> list[str]:
    try:
        authority = verify_a_grade_authority(
            marker,
            signature_verifier=signature_verifier,
            allow_test_authority=allow_test_authority,
        )
    except AuthorityValidationError as exc:
        return ("A_GRADE_MARKER_INVALID:" + str(exc).upper().replace(" ", "_"),)
    reasons: list[str] = []
    if authority.revoked:
        reasons.append("A_GRADE_MARKER_REVOKED")
    if authority.rolled_back:
        reasons.append("A_GRADE_MARKER_ROLLED_BACK")
    if authority.signed_at > checked_at:
        reasons.append("A_GRADE_MARKER_NOT_YET_SIGNED")
    if authority.expires_at <= checked_at:
        reasons.append("A_GRADE_MARKER_EXPIRED")
    bindings: tuple[tuple[str, object, object], ...] = (
        ("PROPOSAL_HASH", authority.proposal_hash, expected_proposal_hash),
        ("CANDIDATE_HASH", authority.candidate_hash, expected_candidate_hash),
        ("CURRENT_POLICY_VERSION", authority.current_policy_version, expected_current_policy_version),
        ("CURRENT_POLICY_HASH", authority.current_policy_hash, expected_policy_hash),
        ("POLICY_AUTHORITY_MARKER_HASH", authority.policy_authority_marker_hash, expected_policy_authority_marker_hash),
        ("EXECUTION_COST_VERSION", authority.execution_cost_version, expected_execution_cost_version),
        ("EXECUTION_COST_HASH", authority.execution_cost_hash, expected_execution_cost_hash),
        ("RANKING_BASIS_HASH", authority.ranking_basis_hash, expected_ranking_basis_hash),
        ("EVALUATION_REPORT_HASH", authority.evaluation_report_hash, expected_evaluation_report_hash),
        ("REFERENCE_DATASET_HASH", authority.reference_dataset_hash, expected_reference_dataset_hash),
        ("INDEPENDENCE_HASH", authority.independence_hash, expected_independence_hash),
        ("RISK_CONTRACT_HASH", authority.risk_contract_hash, risk_contract_hash),
    )
    for name, actual, expected in bindings:
        if expected is None or actual != expected:
            reasons.append(f"A_GRADE_MARKER_{name}_MISMATCH")
    return reasons


def _timestamp(field: str, value: object) -> datetime:
    if isinstance(value, datetime):
        return utc_datetime(value, field=field)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be a timezone-aware datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 datetime") from exc
    return utc_datetime(parsed, field=field)


def _digest(field: str, value: object) -> str:
    if not _is_digest(value):
        raise ValueError(f"{field} must be a lowercase 64-character SHA-256 hash")
    assert isinstance(value, str)
    return value


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and _HASH_RE.fullmatch(value) is not None


__all__ = [
    "NORMAL_AUTHORITY_VERSION",
    "RiskAuthorityTier",
    "RiskTierAuthority",
]
