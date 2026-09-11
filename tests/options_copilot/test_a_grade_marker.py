from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib

import pytest

from options_copilot.learning.markers import AGradeAuthority, verify_a_grade_authority
from options_copilot.risk.authorization import RiskAuthorityTier, RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash, canonical_json


NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
FIXTURE_SECRET = b"non-production-p9-fixture-key"


class FixtureSignatureVerifier:
    trust_domain = "TEST_ONLY"

    def verify(self, *, signer_key_id: str, signature_algorithm: str, message: bytes, signature: str) -> bool:
        if signer_key_id != "test-only:p9-fixture" or signature_algorithm != "TEST_ONLY_SHA256":
            return False
        return signature == hashlib.sha256(FIXTURE_SECRET + message).hexdigest()


FIXTURE_VERIFIER = FixtureSignatureVerifier()
H = {name: character * 64 for name, character in {
    "proposal": "1", "candidate": "2", "policy": "3", "policy_marker": "4",
    "cost": "5", "ranking": "6", "evaluation": "7", "dataset": "8",
    "independence": "9", "risk": "a",
}.items()}


def _fixture(**overrides: object) -> dict[str, object]:
    document: dict[str, object] = {
        "schema": "options_copilot.learning.a_grade_authority.v1",
        "phase": "P9",
        "decision": "APPROVE_A_GRADE",
        "version": "v1",
        "sequence": 1,
        "append_only": True,
        "proposal_hash": H["proposal"],
        "candidate_hash": H["candidate"],
        "current_policy_version": "v2",
        "current_policy_hash": H["policy"],
        "policy_authority_marker_hash": H["policy_marker"],
        "execution_cost_version": "v1",
        "execution_cost_hash": H["cost"],
        "ranking_basis_hash": H["ranking"],
        "evaluation_report_hash": H["evaluation"],
        "reference_dataset_hash": H["dataset"],
        "independence_hash": H["independence"],
        "risk_contract_hash": H["risk"],
        "actor": "human:test-fixture",
        "signed_at": (NOW - timedelta(minutes=1)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "previous_authority_hash": None,
        "revoked": False,
        "rolled_back": False,
        "signer_key_id": "test-only:p9-fixture",
        "signature_algorithm": "TEST_ONLY_SHA256",
    }
    document.update(overrides)
    document["governance_signature"] = hashlib.sha256(
        FIXTURE_SECRET + canonical_json(document).encode("utf-8")
    ).hexdigest()
    document["content_hash"] = canonical_hash(document)
    return document


def _resolve(marker: dict[str, object] | None) -> RiskTierAuthority:
    return RiskTierAuthority.resolve(
        risk_contract_hash=H["risk"], marker=marker, asof=NOW,
        expected_proposal_hash=H["proposal"], expected_candidate_hash=H["candidate"],
        expected_current_policy_version="v2", expected_policy_hash=H["policy"],
        expected_policy_authority_marker_hash=H["policy_marker"],
        expected_execution_cost_version="v1", expected_execution_cost_hash=H["cost"],
        expected_ranking_basis_hash=H["ranking"],
        expected_evaluation_report_hash=H["evaluation"],
        expected_reference_dataset_hash=H["dataset"],
        expected_independence_hash=H["independence"],
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )


def test_exact_current_proposal_marker_unlocks_only_10_01_through_15_00() -> None:
    marker = _fixture()
    parsed = verify_a_grade_authority(
        marker,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )
    assert isinstance(parsed, AGradeAuthority)
    authority = _resolve(marker)
    assert authority.tier is RiskAuthorityTier.A_GRADE
    assert authority.risk_authority_marker_hash == marker["content_hash"]
    assert authority.current_policy_version == "v2"

    expected = {
        Decimal("0.10"): True,
        Decimal("0.1001"): True,
        Decimal("0.15"): True,
        Decimal("0.1501"): False,
        Decimal("0.20"): False,
        Decimal("0.2001"): False,
    }
    assert {value: authority.allows_risk_fraction(value) for value in expected} == expected


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("proposal_hash", "b" * 64),
        ("current_policy_hash", "b" * 64),
        ("policy_authority_marker_hash", "b" * 64),
        ("execution_cost_hash", "b" * 64),
        ("ranking_basis_hash", "b" * 64),
        ("revoked", True),
        ("rolled_back", True),
    ],
)
def test_wrong_binding_or_inactive_marker_fails_closed(field: str, value: object) -> None:
    authority = _resolve(_fixture(**{field: value}))
    assert authority.tier is RiskAuthorityTier.NORMAL
    assert authority.a_grade_approved is False
    assert authority.allows_risk_fraction(Decimal("0.1001")) is False
    assert authority.rejection_reasons


def test_tampered_marker_and_expired_marker_fail_closed() -> None:
    tampered = _fixture()
    tampered["candidate_hash"] = "c" * 64
    expired = _fixture(expires_at=(NOW - timedelta(seconds=1)).isoformat())

    assert _resolve(tampered).tier is RiskAuthorityTier.NORMAL
    assert _resolve(expired).tier is RiskAuthorityTier.NORMAL


def test_normal_authority_has_non_null_versioned_marker_and_exact_ceiling() -> None:
    authority = _resolve(None)
    assert authority.tier is RiskAuthorityTier.NORMAL
    assert authority.risk_authority_version == "v1"
    assert len(authority.risk_authority_marker_hash) == 64
    assert authority.allows_risk_fraction(Decimal("0.10")) is True
    assert authority.allows_risk_fraction(Decimal("0.1001")) is False


def test_marker_has_no_authority_without_independently_injected_verifier() -> None:
    with pytest.raises(ValueError, match="NO_TRUSTED_HUMAN_SIGNER"):
        verify_a_grade_authority(_fixture())
    assert RiskTierAuthority.resolve(
        risk_contract_hash=H["risk"],
        marker=_fixture(),
        asof=NOW,
        expected_proposal_hash=H["proposal"],
        expected_candidate_hash=H["candidate"],
        expected_current_policy_version="v2",
        expected_policy_hash=H["policy"],
        expected_policy_authority_marker_hash=H["policy_marker"],
        expected_execution_cost_version="v1",
        expected_execution_cost_hash=H["cost"],
        expected_ranking_basis_hash=H["ranking"],
        expected_evaluation_report_hash=H["evaluation"],
        expected_reference_dataset_hash=H["dataset"],
        expected_independence_hash=H["independence"],
    ).tier is RiskAuthorityTier.NORMAL


def test_test_only_verifier_is_rejected_by_the_default_production_path() -> None:
    with pytest.raises(ValueError, match="approved production human verifier"):
        verify_a_grade_authority(
            _fixture(),
            signature_verifier=FIXTURE_VERIFIER,
        )
    authority = RiskTierAuthority.resolve(
        risk_contract_hash=H["risk"],
        marker=_fixture(),
        asof=NOW,
        expected_proposal_hash=H["proposal"],
        expected_candidate_hash=H["candidate"],
        expected_current_policy_version="v2",
        expected_policy_hash=H["policy"],
        expected_policy_authority_marker_hash=H["policy_marker"],
        expected_execution_cost_version="v1",
        expected_execution_cost_hash=H["cost"],
        expected_ranking_basis_hash=H["ranking"],
        expected_evaluation_report_hash=H["evaluation"],
        expected_reference_dataset_hash=H["dataset"],
        expected_independence_hash=H["independence"],
        signature_verifier=FIXTURE_VERIFIER,
    )
    assert authority.tier is RiskAuthorityTier.NORMAL
    assert "UNAPPROVED" in authority.rejection_reasons[0]
