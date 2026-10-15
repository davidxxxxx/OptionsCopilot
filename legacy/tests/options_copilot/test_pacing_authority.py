from __future__ import annotations

from datetime import datetime, timedelta, timezone
import base64
import hashlib
import json
from pathlib import Path

import pytest

from options_copilot.operations.capabilities import CapabilityStatus
from options_copilot.operations.pacing_authority import (
    PACING_KEYRING_SCHEMA,
    PACING_POLICY_AUTHORITY_KIND,
    PACING_POLICY_AUTHORITY_MODE,
    PACING_POLICY_REVOCATION_KIND,
    PACING_PRODUCTION_HUMAN,
    PACING_TEST_ONLY,
    load_approved_pacing_capability,
    load_pacing_authority_verifier,
    pacing_policy_content_hash,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


NOW = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
ACTOR = "human:xujie"
FIXTURE_SECRET = b"options-copilot-pacing-test-only"


class FixtureSignatureVerifier:
    trust_domain = PACING_TEST_ONLY
    keyring_hash = "f" * 64

    def verify(
        self,
        *,
        signer_key_id: str,
        signature_algorithm: str,
        message: bytes,
        signature: str,
    ) -> bool:
        return (
            signer_key_id == "test-only:pacing-fixture"
            and signature_algorithm == "TEST_ONLY_SHA256"
            and signature
            == hashlib.sha256(FIXTURE_SECRET + message).hexdigest()
        )


FIXTURE_VERIFIER = FixtureSignatureVerifier()


def _request_classes() -> dict[str, dict[str, int]]:
    return {
        name: {
            "max_concurrency": 1,
            "request_window": 60,
            "max_requests": 4,
            "cooldown": 2,
        }
        for name in (
            "scanner",
            "secdef",
            "snapshot_quote",
            "streaming_quote",
            "historical",
        )
    }


def _write_signed_pair(
    directory: Path,
    *,
    actor: str = ACTOR,
    decision: str = "APPROVED",
    observed_at: datetime = NOW - timedelta(minutes=5),
    long_lived: bool = False,
) -> tuple[dict[str, object], dict[str, object]]:
    directory.mkdir(parents=True, exist_ok=True)
    capability_body: dict[str, object] = {
        "version": "market-data-pacing.v1",
        "observed_at": observed_at.isoformat(timespec="microseconds"),
        "source": "conservative_default",
        "request_classes": _request_classes(),
        "signer": actor,
    }
    capability = {
        **capability_body,
        "content_hash": canonical_hash(capability_body),
    }
    approval_body: dict[str, object] = {
        "schema_version": 2 if long_lived else 1,
        "kind": (
            PACING_POLICY_AUTHORITY_KIND
            if long_lived
            else "market_data_pacing_approval"
        ),
        "actor": actor,
        "signer": actor,
        "decision": decision,
        "signed_at": (observed_at + timedelta(minutes=1)).isoformat(
            timespec="microseconds"
        ),
        "observed_at": capability["observed_at"],
        "version": capability["version"],
        "source": capability["source"],
        "capability": capability,
        "capability_content_hash": capability["content_hash"],
        "review_only": True,
        "direct_order_submission": False,
        "signer_key_id": "test-only:pacing-fixture",
        "signature_algorithm": "TEST_ONLY_SHA256",
    }
    if long_lived:
        approval_body["authority_mode"] = PACING_POLICY_AUTHORITY_MODE
        approval_body["policy_content_hash"] = pacing_policy_content_hash(
            capability,
            actor=actor,
        )
    signature = hashlib.sha256(
        FIXTURE_SECRET + canonical_json(approval_body).encode("utf-8")
    ).hexdigest()
    signed_approval = {**approval_body, "approval_signature": signature}
    approval = {
        **signed_approval,
        "content_hash": canonical_hash(signed_approval),
    }
    (directory / "capability.json").write_text(
        json.dumps(capability), encoding="utf-8"
    )
    (directory / "approval.json").write_text(
        json.dumps(approval), encoding="utf-8"
    )
    return capability, approval


def _write_revocation(
    directory: Path,
    approval: dict[str, object],
    *,
    reason: str = "operator requested",
) -> dict[str, object]:
    body = {
        "schema_version": 1,
        "kind": PACING_POLICY_REVOCATION_KIND,
        "actor": ACTOR,
        "signer": ACTOR,
        "decision": "REVOKED",
        "revoked_at": (NOW - timedelta(minutes=1)).isoformat(timespec="microseconds"),
        "reason": reason,
        "policy_content_hash": approval["policy_content_hash"],
        "approval_content_hash": approval["content_hash"],
        "review_only": True,
        "direct_order_submission": False,
        "signer_key_id": "test-only:pacing-fixture",
        "signature_algorithm": "TEST_ONLY_SHA256",
    }
    signature = hashlib.sha256(
        FIXTURE_SECRET + canonical_json(body).encode("utf-8")
    ).hexdigest()
    signed = {**body, "revocation_signature": signature}
    document = {**signed, "content_hash": canonical_hash(signed)}
    (directory / "revocation.json").write_text(
        json.dumps(document),
        encoding="utf-8",
    )
    return document


def test_loader_requires_matching_human_approval_and_returns_capability(
    tmp_path: Path,
) -> None:
    capability, approval = _write_signed_pair(tmp_path)

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is True
    assert result.record.status is CapabilityStatus.READY_FOR_REVIEW
    assert result.record.reason_codes == ()
    assert result.capability is not None
    assert result.capability.content_hash == capability["content_hash"]
    assert result.approval_hash == approval["content_hash"]
    assert result.actor == ACTOR


@pytest.mark.parametrize("missing_name", ["capability.json", "approval.json"])
def test_loader_fails_closed_when_either_artifact_is_missing(
    tmp_path: Path,
    missing_name: str,
) -> None:
    _write_signed_pair(tmp_path)
    (tmp_path / missing_name).unlink()

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is False
    assert result.capability is None
    assert result.record.status is CapabilityStatus.MISSING
    assert "PACING_CAPABILITY_MISSING" in result.record.reason_codes


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        (
            lambda capability, approval: approval.__setitem__(
                "decision", "REJECTED"
            ),
            "PACING_APPROVAL_HASH_MISMATCH",
        ),
        (
            lambda capability, approval: approval.__setitem__(
                "content_hash", "0" * 64
            ),
            "PACING_APPROVAL_HASH_MISMATCH",
        ),
        (
            lambda capability, approval: approval.__setitem__(
                "capability_content_hash", "0" * 64
            ),
            "PACING_APPROVAL_HASH_MISMATCH",
        ),
        (
            lambda capability, approval: capability["request_classes"][
                "scanner"
            ].__setitem__("max_requests", 999),
            "PACING_CONTENT_HASH_MISMATCH",
        ),
    ],
)
def test_loader_rejects_tampered_pair(
    tmp_path: Path,
    mutation,
    expected_reason: str,
) -> None:
    capability, approval = _write_signed_pair(tmp_path)
    mutation(capability, approval)
    (tmp_path / "capability.json").write_text(
        json.dumps(capability), encoding="utf-8"
    )
    (tmp_path / "approval.json").write_text(
        json.dumps(approval), encoding="utf-8"
    )

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is False
    assert result.capability is None
    assert expected_reason in result.record.reason_codes
    assert "PACING_CAPABILITY_MISSING" in result.record.reason_codes


def test_loader_rejects_self_consistent_but_unexpected_actor(tmp_path: Path) -> None:
    _write_signed_pair(tmp_path, actor="human:someone-else")

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is False
    assert result.capability is None
    assert "PACING_APPROVAL_ACTOR_MISMATCH" in result.record.reason_codes
    assert "PACING_CAPABILITY_MISSING" in result.record.reason_codes


def test_loader_rejects_stale_capability_even_when_approval_matches(
    tmp_path: Path,
) -> None:
    _write_signed_pair(tmp_path, observed_at=NOW - timedelta(hours=25))

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        max_age=timedelta(hours=24),
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is False
    assert result.capability is None
    assert result.record.status is CapabilityStatus.STALE
    assert "PACING_OBSERVATION_STALE" in result.record.reason_codes
    assert "PACING_CAPABILITY_MISSING" in result.record.reason_codes


def test_long_lived_policy_remains_valid_after_one_year(tmp_path: Path) -> None:
    capability, approval = _write_signed_pair(
        tmp_path,
        observed_at=NOW - timedelta(days=366),
        long_lived=True,
    )

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is True
    assert result.capability is not None
    assert result.capability.content_hash == capability["content_hash"]
    assert result.policy_authority is not None
    assert result.policy_authority.policy_content_hash == approval["policy_content_hash"]
    assert result.record.details["authority_mode"] == PACING_POLICY_AUTHORITY_MODE
    assert result.record.details["valid_until_revoked"] is True


def test_authenticated_revocation_immediately_blocks_long_lived_policy(
    tmp_path: Path,
) -> None:
    _, approval = _write_signed_pair(tmp_path, long_lived=True)
    _write_revocation(tmp_path, approval)

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is False
    assert result.capability is None
    assert "PACING_POLICY_REVOKED" in result.record.reason_codes
    assert "PACING_CAPABILITY_MISSING" in result.record.reason_codes


def test_malformed_or_tampered_revocation_fails_closed(tmp_path: Path) -> None:
    _, approval = _write_signed_pair(tmp_path, long_lived=True)
    revocation = _write_revocation(tmp_path, approval)
    revocation["policy_content_hash"] = "0" * 64
    (tmp_path / "revocation.json").write_text(
        json.dumps(revocation),
        encoding="utf-8",
    )

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is False
    assert result.capability is None
    assert "PACING_POLICY_REVOCATION_INVALID" in result.record.reason_codes


def test_loader_rejects_validly_rehashed_non_approved_decision(tmp_path: Path) -> None:
    _, approval = _write_signed_pair(tmp_path)
    approval["decision"] = "REJECTED"
    approval["approval_signature"] = hashlib.sha256(
        FIXTURE_SECRET
        + canonical_json(
            {
                key: value
                for key, value in approval.items()
                if key not in {"approval_signature", "content_hash"}
            }
        ).encode("utf-8")
    ).hexdigest()
    approval["content_hash"] = canonical_hash(
        {key: value for key, value in approval.items() if key != "content_hash"}
    )
    (tmp_path / "approval.json").write_text(
        json.dumps(approval), encoding="utf-8"
    )

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )

    assert result.ready is False
    assert result.capability is None
    assert "PACING_APPROVAL_DECISION_INVALID" in result.record.reason_codes
    assert "PACING_CAPABILITY_MISSING" in result.record.reason_codes


def test_loader_rejects_self_hashed_approval_without_trusted_verifier(
    tmp_path: Path,
) -> None:
    _write_signed_pair(tmp_path)

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
    )

    assert result.ready is False
    assert result.capability is None
    assert "PACING_APPROVAL_SIGNATURE_VERIFIER_MISSING" in result.record.reason_codes


def test_loader_rejects_caller_object_claiming_production_trust(
    tmp_path: Path,
) -> None:
    _write_signed_pair(tmp_path)

    class ForgedVerifier(FixtureSignatureVerifier):
        trust_domain = PACING_PRODUCTION_HUMAN

    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=ForgedVerifier(),
    )

    assert result.ready is False
    assert result.capability is None
    assert "PACING_APPROVAL_SIGNATURE_VERIFIER_UNTRUSTED" in result.record.reason_codes


def test_separate_ed25519_keyring_authenticates_exact_approval(
    tmp_path: Path,
) -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    capability, approval = _write_signed_pair(tmp_path)
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    keyring_body = {
        "schema": PACING_KEYRING_SCHEMA,
        "trust_domain": PACING_PRODUCTION_HUMAN,
        "signer_key_id": "human-key:xujie-pacing",
        "signature_algorithm": "ED25519",
        "public_key_base64": base64.b64encode(public_key).decode("ascii"),
    }
    keyring = {**keyring_body, "content_hash": canonical_hash(keyring_body)}
    keyring_path = tmp_path / "pacing-keyring.json"
    keyring_path.write_text(json.dumps(keyring), encoding="utf-8")

    approval.pop("content_hash")
    approval["signer_key_id"] = "human-key:xujie-pacing"
    approval["signature_algorithm"] = "ED25519"
    approval.pop("approval_signature")
    approval["approval_signature"] = base64.b64encode(
        private_key.sign(canonical_json(approval).encode("utf-8"))
    ).decode("ascii")
    approval["content_hash"] = canonical_hash(approval)
    (tmp_path / "approval.json").write_text(json.dumps(approval), encoding="utf-8")

    verifier = load_pacing_authority_verifier(keyring_path)
    result = load_approved_pacing_capability(
        tmp_path,
        now=NOW,
        expected_actor=ACTOR,
        signature_verifier=verifier,
    )

    assert result.ready is True
    assert result.capability is not None
    assert result.capability.content_hash == capability["content_hash"]
    assert result.signer_key_id == "human-key:xujie-pacing"
    assert result.record.details["approval_signature_authenticated"] is True
