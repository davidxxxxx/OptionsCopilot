from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import threading

import pytest

from options_copilot.risk import RiskAuthorityTier
from options_copilot.risk.resolver import (
    CurrentRiskAuthorityResolver,
    RiskAuthorityCurrentnessError,
)
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.canonical import canonical_json


NOW = datetime(2026, 8, 5, 14, 30, tzinfo=timezone.utc)
RISK_CONTRACT_HASH = "1" * 64
POLICY_HASH = "2" * 64
REPORT_HASH = "3" * 64
DATASET_HASH = "4" * 64
INDEPENDENCE_HASH = "5" * 64
PROPOSAL_HASH = "6" * 64
CANDIDATE_HASH = "7" * 64
POLICY_MARKER_HASH = "8" * 64
COST_HASH = "9" * 64
RANKING_HASH = "a" * 64
FIXTURE_SECRET = b"non-production-risk-resolver-fixture"


class _FixtureSignatureVerifier:
    trust_domain = "TEST_ONLY"

    def verify(self, *, signer_key_id: str, signature_algorithm: str, message: bytes, signature: str) -> bool:
        return (
            signer_key_id == "test-only:risk-resolver"
            and signature_algorithm == "TEST_ONLY_SHA256"
            and signature == hashlib.sha256(FIXTURE_SECRET + message).hexdigest()
        )


FIXTURE_VERIFIER = _FixtureSignatureVerifier()


@dataclass(frozen=True)
class _Policy:
    current_policy_hash: str
    current_policy_version: str = "v2"
    policy_authority_marker_hash: str = POLICY_MARKER_HASH


class _MarkerSource:
    test_only = True

    def __init__(self, value: object = None) -> None:
        self.value = value
        self.error: Exception | None = None
        self.calls = 0

    def read(self) -> object:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.value


class _SqliteMarkerSource:
    test_only = True

    def __init__(self, path: Path) -> None:
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(
            path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS marker_head "
            "(singleton INTEGER PRIMARY KEY, document TEXT)"
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO marker_head(singleton, document) VALUES(1, NULL)"
        )

    def close(self) -> None:
        self.connection.close()

    def read(self) -> object:
        with self._lock:
            value = self.connection.execute(
                "SELECT document FROM marker_head WHERE singleton=1"
            ).fetchone()[0]
        return None if value is None else json.loads(str(value))

    def guard_read(self, callback):
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                before = self.connection.execute(
                    "SELECT document FROM marker_head WHERE singleton=1"
                ).fetchone()[0]
                result = callback()
                after = self.connection.execute(
                    "SELECT document FROM marker_head WHERE singleton=1"
                ).fetchone()[0]
                if after != before:
                    self.connection.rollback()
                    return None
                self.connection.commit()
                return result
            except BaseException:
                self.connection.rollback()
                raise

    def replace(self, value: object) -> None:
        document = None if value is None else json.dumps(value, sort_keys=True)
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                self.connection.execute(
                    "UPDATE marker_head SET document=? WHERE singleton=1",
                    (document,),
                )
                self.connection.commit()
            except BaseException:
                self.connection.rollback()
                raise


def _resolver(source: _MarkerSource | None = None) -> CurrentRiskAuthorityResolver:
    return CurrentRiskAuthorityResolver(
        RISK_CONTRACT_HASH,
        marker_source=source,
        expected_evaluation_report_hash=REPORT_HASH,
        expected_reference_dataset_hash=DATASET_HASH,
        expected_independence_hash=INDEPENDENCE_HASH,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
        clock=lambda: NOW,
    )


def _signed_marker(**changes: object) -> dict[str, object]:
    marker: dict[str, object] = {
        "schema": "options_copilot.learning.a_grade_authority.v1",
        "phase": "P9",
        "decision": "APPROVE_A_GRADE",
        "version": "v1",
        "sequence": 1,
        "append_only": True,
        "proposal_hash": PROPOSAL_HASH,
        "candidate_hash": CANDIDATE_HASH,
        "current_policy_version": "v2",
        "current_policy_hash": POLICY_HASH,
        "policy_authority_marker_hash": POLICY_MARKER_HASH,
        "execution_cost_version": "v1",
        "execution_cost_hash": COST_HASH,
        "ranking_basis_hash": RANKING_HASH,
        "evaluation_report_hash": REPORT_HASH,
        "reference_dataset_hash": DATASET_HASH,
        "independence_hash": INDEPENDENCE_HASH,
        "risk_contract_hash": RISK_CONTRACT_HASH,
        "actor": "human:xujie",
        "signed_at": (NOW - timedelta(minutes=1)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "previous_authority_hash": None,
        "revoked": False,
        "rolled_back": False,
        "signer_key_id": "test-only:risk-resolver",
        "signature_algorithm": "TEST_ONLY_SHA256",
    }
    marker.update(changes)
    marker["governance_signature"] = hashlib.sha256(
        FIXTURE_SECRET + canonical_json(marker).encode("utf-8")
    ).hexdigest()
    marker["content_hash"] = canonical_hash(marker)
    return marker


def test_missing_marker_is_canonical_normal_and_stably_current() -> None:
    source = _MarkerSource(None)
    resolver = _resolver(source)

    first = resolver.resolve(now=NOW, current_policy=_Policy(POLICY_HASH))
    second = resolver.resolve(now=NOW, resolved_policy=_Policy(POLICY_HASH))

    assert first == second
    assert first.tier is RiskAuthorityTier.NORMAL
    assert first.a_grade_approved is False
    assert first.rejection_reasons == ()
    assert resolver.is_current(second) is True
    assert resolver.assert_current(second) is second
    assert source.calls == 4


def test_risk_guard_blocks_second_marker_connection_and_rechecks_head(
    tmp_path: Path,
) -> None:
    path = tmp_path / "risk-marker.sqlite3"
    left = _SqliteMarkerSource(path)
    right = _SqliteMarkerSource(path)
    entered, release = threading.Event(), threading.Event()
    writer_started, writer_finished = threading.Event(), threading.Event()
    try:
        resolver = _resolver(left)  # type: ignore[arg-type]
        resolved = resolver.resolve(
            now=NOW,
            current_policy=_Policy(POLICY_HASH),
        )

        def approved_callback() -> str:
            entered.set()
            assert release.wait(timeout=5)
            return "approved"

        def guarded() -> object | None:
            return resolver.guard_current(
                resolved,
                callback=approved_callback,
            )

        def replace_from_second_connection() -> None:
            writer_started.set()
            right.replace(
                {"phase": "P9", "decision": "APPROVE_A_GRADE"},
            )
            writer_finished.set()

        with ThreadPoolExecutor(max_workers=2) as workers:
            guard_future = workers.submit(guarded)
            assert entered.wait(timeout=5)
            writer = workers.submit(replace_from_second_connection)
            assert writer_started.wait(timeout=5)
            assert not writer_finished.wait(timeout=0.1)
            release.set()
            assert guard_future.result(timeout=5) == "approved"
            writer.result(timeout=5)
            assert writer_finished.is_set()

        callbacks: list[str] = []
        assert resolver.guard_current(
            resolved,
            callback=lambda: callbacks.append("called"),
        ) is None
        assert callbacks == []
    finally:
        left.close()
        right.close()


def test_risk_guard_releases_source_lease_when_callback_raises(
    tmp_path: Path,
) -> None:
    path = tmp_path / "risk-marker-callback-error.sqlite3"
    left = _SqliteMarkerSource(path)
    right = _SqliteMarkerSource(path)
    right.connection.execute("PRAGMA busy_timeout=250")
    try:
        resolver = _resolver(left)  # type: ignore[arg-type]
        resolved = resolver.resolve(
            now=NOW,
            current_policy=_Policy(POLICY_HASH),
        )

        def fail_callback() -> object:
            raise RuntimeError("restricted commit failed")

        with pytest.raises(RuntimeError, match="restricted commit failed"):
            resolver.guard_current(resolved, callback=fail_callback)

        replacement = {"phase": "P9", "decision": "REVOKE_A_GRADE"}
        right.replace(replacement)
        assert right.read() == replacement
    finally:
        left.close()
        right.close()


def test_test_only_read_only_source_can_use_local_guard_fallback() -> None:
    source = _MarkerSource(None)
    resolver = _resolver(source)
    resolved = resolver.resolve(now=NOW, current_policy=_Policy(POLICY_HASH))
    callbacks: list[str] = []

    result = resolver.guard_current(
        resolved,
        callback=lambda: callbacks.append("called") or "approved",
    )

    assert result == "approved"
    assert callbacks == ["called"]


def test_production_without_guard_source_never_runs_restricted_callback() -> None:
    resolver = CurrentRiskAuthorityResolver(
        RISK_CONTRACT_HASH,
        clock=lambda: NOW,
    )
    resolved = resolver.resolve(now=NOW, current_policy=_Policy(POLICY_HASH))
    callbacks: list[str] = []

    assert resolver.guard_current(
        resolved,
        callback=lambda: callbacks.append("called"),
    ) is None
    assert callbacks == []


def test_invalid_marker_can_only_resolve_to_normal() -> None:
    source = _MarkerSource({"phase": "P9", "decision": "APPROVE_A_GRADE"})
    resolver = _resolver(source)

    authority = resolver.resolve(
        now=NOW,
        current_policy=_Policy(POLICY_HASH),
        proposal_hash=PROPOSAL_HASH,
        candidate_hash=CANDIDATE_HASH,
        execution_cost_version="v1",
        execution_cost_hash=COST_HASH,
        ranking_basis_hash=RANKING_HASH,
    )

    assert authority.tier is RiskAuthorityTier.NORMAL
    assert authority.a_grade_approved is False
    assert authority.rejection_reasons
    assert resolver.is_current(authority) is True


def test_source_read_failure_is_fail_closed_and_never_current() -> None:
    source = _MarkerSource(None)
    resolver = _resolver(source)
    source.error = OSError("marker ledger unavailable")

    authority = resolver.resolve(
        now=NOW,
        current_policy=_Policy(POLICY_HASH),
        proposal_hash=PROPOSAL_HASH,
        candidate_hash=CANDIDATE_HASH,
        execution_cost_version="v1",
        execution_cost_hash=COST_HASH,
        ranking_basis_hash=RANKING_HASH,
    )

    assert authority.tier is RiskAuthorityTier.NORMAL
    assert authority.a_grade_approved is False
    assert authority.rejection_reasons == ("A_GRADE_MARKER_SOURCE_UNAVAILABLE",)
    assert resolver.is_current(authority) is False
    with pytest.raises(
        RiskAuthorityCurrentnessError,
        match="unable to read current risk authority marker",
    ):
        resolver.assert_current(authority)


def test_full_normal_authority_comparison_detects_marker_head_change() -> None:
    source = _MarkerSource(None)
    resolver = _resolver(source)
    authority = resolver.resolve(now=NOW, current_policy=_Policy(POLICY_HASH))

    source.value = {}

    assert resolver.is_current(authority) is False
    with pytest.raises(RiskAuthorityCurrentnessError, match="head changed"):
        resolver.assert_current(authority)


def test_a_grade_requires_explicit_p9_bindings_and_current_policy() -> None:
    marker = _signed_marker()
    source = _MarkerSource(marker)
    resolver = _resolver(source)

    authority = resolver.resolve(
        now=NOW,
        current_policy=_Policy(POLICY_HASH),
        proposal_hash=PROPOSAL_HASH,
        candidate_hash=CANDIDATE_HASH,
        execution_cost_version="v1",
        execution_cost_hash=COST_HASH,
        ranking_basis_hash=RANKING_HASH,
    )

    assert authority.tier is RiskAuthorityTier.A_GRADE
    assert authority.a_grade_approved is True
    assert authority.risk_authority_marker_hash == marker["content_hash"]
    assert resolver.is_current(authority) is True

    unbound = CurrentRiskAuthorityResolver(
        RISK_CONTRACT_HASH,
        marker_source=source,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
        clock=lambda: NOW,
    ).resolve(now=NOW, current_policy=_Policy(POLICY_HASH))
    assert unbound.tier is RiskAuthorityTier.NORMAL
    assert unbound.a_grade_approved is False


def test_currentness_context_is_per_resolution_not_global_last_call() -> None:
    source = _MarkerSource(_signed_marker())
    resolver = _resolver(source)
    bound = resolver.resolve(
        now=NOW,
        current_policy=_Policy(POLICY_HASH),
        proposal_hash=PROPOSAL_HASH,
        candidate_hash=CANDIDATE_HASH,
        execution_cost_version="v1",
        execution_cost_hash=COST_HASH,
        ranking_basis_hash=RANKING_HASH,
    )
    later_unbound = resolver.resolve(
        now=NOW,
        current_policy=_Policy(POLICY_HASH),
    )

    assert bound.tier is RiskAuthorityTier.A_GRADE
    assert later_unbound.tier is RiskAuthorityTier.NORMAL
    assert resolver.is_current(bound) is True
    assert resolver.is_current(later_unbound) is True


def test_legacy_self_hashed_marker_is_always_normal() -> None:
    marker = {
        "schema": "options_copilot.risk.a_grade_authority.v1",
        "phase": "P9",
        "decision": "APPROVE_A_GRADE",
        "version": "v1",
        "governance_signature": "b" * 64,
        "content_hash": "c" * 64,
    }
    authority = _resolver(_MarkerSource(marker)).resolve(
        now=NOW,
        current_policy=_Policy(POLICY_HASH),
        proposal_hash=PROPOSAL_HASH,
        candidate_hash=CANDIDATE_HASH,
        execution_cost_version="v1",
        execution_cost_hash=COST_HASH,
        ranking_basis_hash=RANKING_HASH,
    )
    assert authority.tier is RiskAuthorityTier.NORMAL
    assert "LEGACY_OR_UNKNOWN_SCHEMA" in authority.rejection_reasons[0]


def test_constructor_rejects_non_lowercase_contract_hash() -> None:
    with pytest.raises(ValueError, match="lowercase 64-character SHA-256"):
        CurrentRiskAuthorityResolver("A" * 64)


def test_production_rejects_plain_callable_marker_source() -> None:
    with pytest.raises(TypeError, match="trusted production risk marker adapter"):
        CurrentRiskAuthorityResolver(
            RISK_CONTRACT_HASH,
            marker_source=lambda: None,
        )


def test_production_rejects_read_only_marker_source() -> None:
    with pytest.raises(TypeError, match="trusted production risk marker adapter"):
        CurrentRiskAuthorityResolver(
            RISK_CONTRACT_HASH,
            marker_source=_MarkerSource(None),
        )


def test_production_rejects_duck_typed_guarded_marker_source() -> None:
    class CallerFacade:
        def read(self):
            return None

        def guard_read(self, callback):
            return callback()

    with pytest.raises(TypeError, match="trusted production risk marker adapter"):
        CurrentRiskAuthorityResolver(
            RISK_CONTRACT_HASH,
            marker_source=CallerFacade(),
        )


def test_test_fixture_verifier_cannot_be_injected_into_production_resolver() -> None:
    with pytest.raises(ValueError, match="approved production human verifier"):
        CurrentRiskAuthorityResolver(
            RISK_CONTRACT_HASH,
            signature_verifier=FIXTURE_VERIFIER,
        )


def test_environment_flags_cannot_unlock_a_grade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPTIONS_COPILOT_A_GRADE_UNLOCKED", "true")
    monkeypatch.setenv("OPTIONS_COPILOT_RISK_TIER", "A_GRADE")

    authority = _resolver().resolve(
        now=NOW,
        current_policy=_Policy(POLICY_HASH),
    )

    assert authority.tier is RiskAuthorityTier.NORMAL
    assert authority.a_grade_approved is False
