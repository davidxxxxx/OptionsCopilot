"""Current, fail-closed risk-tier authority resolution.

The resolver has exactly one optional marker source supplied by composition.
It never discovers authority through environment variables, proposals,
candidates, model output, or campaign state.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import re
import threading
from typing import Protocol

from options_copilot.storage.canonical import utc_datetime
from options_copilot.learning.markers import (
    AuthoritySignatureVerifier,
    validate_authority_verifier,
)
from options_copilot.learning.policy_authority import PolicyAuthorityLedger

from .authorization import RiskTierAuthority


_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_SOURCE_UNAVAILABLE = "A_GRADE_MARKER_SOURCE_UNAVAILABLE"


class RiskAuthorityMarkerSource(Protocol):
    """Read and lease the current immutable P9 marker head."""

    def read(self) -> object: ...

    def guard_read(self, callback: Callable[[], object]) -> object | None: ...


class RiskAuthorityCurrentnessError(RuntimeError):
    """The previously resolved risk authority cannot be proven current."""


class _MarkerReadError(RuntimeError):
    """Internal marker-source failure retained as a currentness cause."""


class PolicyLedgerRiskAuthorityMarkerSource:
    """Production marker source sharing the policy ledger's write lease."""

    def __init__(self, ledger: PolicyAuthorityLedger) -> None:
        if not isinstance(ledger, PolicyAuthorityLedger):
            raise TypeError("ledger must be a PolicyAuthorityLedger")
        self.ledger = ledger

    def read(self) -> Mapping[str, object] | None:
        """Read the sole verified current A-grade marker, if one exists."""

        return self.ledger.current_a_grade_marker()

    def guard_read(self, callback: Callable[[], object]) -> object | None:
        """Hold the shared durable authority head while committing a result."""

        return self.ledger.guard_read(callback)


@dataclass(frozen=True, slots=True)
class _ResolutionContext:
    authority: RiskTierAuthority
    bindings: Mapping[str, str | None]


class CurrentRiskAuthorityResolver:
    """Resolve NORMAL or a fully bound, human-signed P9 A-grade authority.

    Optional P9 evaluation bindings must be supplied explicitly by the
    composition root.  Omitting any of them is safe: even a structurally valid
    marker then resolves to canonical NORMAL through ``RiskTierAuthority``.
    """

    def __init__(
        self,
        risk_contract_hash: str,
        marker_source: RiskAuthorityMarkerSource | Callable[[], object] | None = None,
        *,
        expected_evaluation_report_hash: str | None = None,
        expected_reference_dataset_hash: str | None = None,
        expected_independence_hash: str | None = None,
        signature_verifier: AuthoritySignatureVerifier | None = None,
        allow_test_authority: bool = False,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        # Reuse the domain authority's validation and retain its normalized,
        # lowercase-only contract identity.
        self.risk_contract_hash = RiskTierAuthority.normal(
            risk_contract_hash
        ).risk_contract_hash
        self.marker_source = _validate_source(
            marker_source,
            allow_test_authority=allow_test_authority,
        )
        self.expected_evaluation_report_hash = _optional_digest(
            "expected_evaluation_report_hash",
            expected_evaluation_report_hash,
        )
        self.expected_reference_dataset_hash = _optional_digest(
            "expected_reference_dataset_hash",
            expected_reference_dataset_hash,
        )
        self.expected_independence_hash = _optional_digest(
            "expected_independence_hash",
            expected_independence_hash,
        )
        validate_authority_verifier(
            signature_verifier,
            allow_test_authority=allow_test_authority,
        )
        self.signature_verifier = signature_verifier
        self.allow_test_authority = allow_test_authority
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._contexts: OrderedDict[int, _ResolutionContext] = OrderedDict()

    def resolve(
        self,
        *,
        now: datetime,
        current_policy: object | None = None,
        resolved_policy: object | None = None,
        proposal_hash: str | None = None,
        candidate_hash: str | None = None,
        execution_cost_version: str | None = None,
        execution_cost_hash: str | None = None,
        ranking_basis_hash: str | None = None,
    ) -> RiskTierAuthority:
        """Read one marker head and resolve it against explicit authorities."""

        checked_at = utc_datetime(now, field="now")
        policy = _policy_identity(current_policy, resolved_policy)
        bindings = {
            "proposal_hash": _optional_digest("proposal_hash", proposal_hash),
            "candidate_hash": _optional_digest("candidate_hash", candidate_hash),
            "current_policy_version": policy[0],
            "current_policy_hash": policy[1],
            "policy_authority_marker_hash": policy[2],
            "execution_cost_version": _optional_version(
                "execution_cost_version",
                execution_cost_version,
            ),
            "execution_cost_hash": _optional_digest(
                "execution_cost_hash",
                execution_cost_hash,
            ),
            "ranking_basis_hash": _optional_digest(
                "ranking_basis_hash",
                ranking_basis_hash,
            ),
        }
        try:
            marker = self._read_marker()
        except _MarkerReadError:
            authority = RiskTierAuthority.normal(
                self.risk_contract_hash,
                rejection_reasons=(_SOURCE_UNAVAILABLE,),
            )
        else:
            authority = self._resolve_marker(
                marker,
                checked_at=checked_at,
                bindings=bindings,
            )
        with self._lock:
            key = id(authority)
            self._contexts[key] = _ResolutionContext(
                authority=authority,
                bindings=dict(bindings),
            )
            self._contexts.move_to_end(key)
            while len(self._contexts) > 256:
                self._contexts.popitem(last=False)
        return authority

    def is_current(self, resolution: RiskTierAuthority) -> bool:
        """Re-read the marker head; source errors and drift are never current."""

        try:
            return self._current_authority(resolution) == resolution
        except (RiskAuthorityCurrentnessError, _MarkerReadError, TypeError, ValueError):
            return False

    def assert_current(
        self,
        resolution: RiskTierAuthority,
    ) -> RiskTierAuthority:
        """Return ``resolution`` only when its complete immutable head matches."""

        try:
            current = self._current_authority(resolution)
        except _MarkerReadError as exc:
            raise RiskAuthorityCurrentnessError(
                "unable to read current risk authority marker"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise RiskAuthorityCurrentnessError(
                "current risk authority marker is invalid"
            ) from exc
        if current != resolution:
            raise RiskAuthorityCurrentnessError("risk authority head changed")
        return resolution

    def guard_current(
        self,
        resolution: RiskTierAuthority,
        *,
        callback: Callable[[], object],
    ) -> object | None:
        """Run a restricted callback under the marker source's read lease.

        A resolver-local lock cannot exclude another process or database
        connection from changing the authority head.  Production therefore
        requires ``guard_read`` to own that exclusion for the complete
        re-read, comparison, and callback.  Read-only fixtures may use the
        local fallback only behind the explicit TEST_ONLY constructor gate.
        """

        if not callable(callback):
            raise TypeError("callback must be callable")

        def guarded() -> object | None:
            try:
                current = self._current_authority(resolution)
            except (
                RiskAuthorityCurrentnessError,
                _MarkerReadError,
                TypeError,
                ValueError,
            ):
                return None
            if current != resolution:
                return None
            return callback()

        source = self.marker_source
        source_guard = getattr(source, "guard_read", None)
        if callable(source_guard):
            return source_guard(guarded)
        if self.allow_test_authority and source is not None:
            with self._lock:
                return guarded()
        return None

    def _current_authority(
        self,
        resolution: RiskTierAuthority,
    ) -> RiskTierAuthority:
        if not isinstance(resolution, RiskTierAuthority):
            raise TypeError("resolution must be a RiskTierAuthority")
        with self._lock:
            context = self._contexts.get(id(resolution))
        if context is None or context.authority is not resolution:
            raise RiskAuthorityCurrentnessError(
                "risk authority was not resolved by this resolver"
            )
        checked_at = utc_datetime(self._clock(), field="clock result")
        marker = self._read_marker()
        return self._resolve_marker(
            marker,
            checked_at=checked_at,
            bindings=context.bindings,
        )

    def _resolve_marker(
        self,
        marker: object,
        *,
        checked_at: datetime,
        bindings: Mapping[str, str | None],
    ) -> RiskTierAuthority:
        # RiskTierAuthority.resolve is the sole A-grade authority validator.
        return RiskTierAuthority.resolve(
            risk_contract_hash=self.risk_contract_hash,
            marker=marker,  # type: ignore[arg-type]
            asof=checked_at,
            expected_evaluation_report_hash=(
                self.expected_evaluation_report_hash
            ),
            expected_proposal_hash=bindings["proposal_hash"],
            expected_candidate_hash=bindings["candidate_hash"],
            expected_current_policy_version=bindings["current_policy_version"],
            expected_policy_hash=bindings["current_policy_hash"],
            expected_policy_authority_marker_hash=(
                bindings["policy_authority_marker_hash"]
            ),
            expected_execution_cost_version=bindings["execution_cost_version"],
            expected_execution_cost_hash=bindings["execution_cost_hash"],
            expected_ranking_basis_hash=bindings["ranking_basis_hash"],
            expected_reference_dataset_hash=(
                self.expected_reference_dataset_hash
            ),
            expected_independence_hash=self.expected_independence_hash,
            signature_verifier=self.signature_verifier,
            allow_test_authority=self.allow_test_authority,
        )

    def _read_marker(self) -> object:
        source = self.marker_source
        if source is None:
            return None
        try:
            reader = getattr(source, "read", None)
            if callable(reader):
                return reader()
            assert callable(source)
            return source()
        except Exception as exc:
            raise _MarkerReadError("risk authority marker source failed") from exc


def _validate_source(
    source: RiskAuthorityMarkerSource | Callable[[], object] | None,
    *,
    allow_test_authority: bool,
) -> RiskAuthorityMarkerSource | Callable[[], object] | None:
    if source is None:
        return source
    if isinstance(source, PolicyLedgerRiskAuthorityMarkerSource):
        return source
    reader = getattr(source, "read", None)
    guard = getattr(source, "guard_read", None)
    if (
        allow_test_authority
        and getattr(source, "test_only", False) is True
        and (callable(source) or callable(reader))
        and (callable(guard) or callable(source) or callable(reader))
    ):
        return source
    raise TypeError(
        "trusted production risk marker adapter is unavailable; TEST_ONLY "
        "sources require allow_test_authority=True and test_only=True"
    )


def _optional_digest(field: str, value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase 64-character SHA-256 hash")
    return value


def _policy_identity(*policies: object | None) -> tuple[str | None, str | None, str | None]:
    found: list[tuple[str, str, str]] = []
    for policy in policies:
        if policy is None:
            continue
        if isinstance(policy, Mapping):
            version = policy.get("current_policy_version", policy.get("policy_version"))
            value = policy.get(
                "current_policy_hash",
                policy.get("policy_hash", policy.get("contract_hash")),
            )
            marker = policy.get("policy_authority_marker_hash")
        else:
            version = getattr(
                policy,
                "current_policy_version",
                getattr(policy, "policy_version", None),
            )
            value = getattr(
                policy,
                "current_policy_hash",
                getattr(policy, "policy_hash", getattr(policy, "contract_hash", None)),
            )
            marker = getattr(policy, "policy_authority_marker_hash", None)
        if (
            not isinstance(version, str)
            or re.fullmatch(r"v[1-9][0-9]*(?:\.[0-9]+)*", version) is None
            or not isinstance(value, str)
            or _HASH_RE.fullmatch(value) is None
            or not isinstance(marker, str)
            or _HASH_RE.fullmatch(marker) is None
        ):
            return None, None, None
        found.append((version, value, marker))
    if not found or len(set(found)) != 1:
        return None, None, None
    return found[0]


def _optional_version(field: str, value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or re.fullmatch(r"v[1-9][0-9]*(?:\.[0-9]+)*", value) is None:
        raise ValueError(f"{field} must be a v1-style version")
    return value


__all__ = [
    "CurrentRiskAuthorityResolver",
    "PolicyLedgerRiskAuthorityMarkerSource",
    "RiskAuthorityCurrentnessError",
    "RiskAuthorityMarkerSource",
]
