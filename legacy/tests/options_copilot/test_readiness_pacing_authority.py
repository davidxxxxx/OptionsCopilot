"""Parity regressions for readiness and runtime pacing authority loading."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import pytest

from options_copilot.operations.capabilities import MarketDataPacingCapability
from options_copilot.operations.pacing_authority import (
    PACING_EXPECTED_ACTOR,
    PACING_KEYRING_SCHEMA,
    PACING_POLICY_AUTHORITY_KIND,
    PACING_POLICY_AUTHORITY_MODE,
    PACING_POLICY_REVOCATION_KIND,
    PACING_PRODUCTION_HUMAN,
    pacing_policy_content_hash,
)
from options_copilot.operations.readiness import (
    build_readiness_report,
    load_default_probe_inputs,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


NOW = datetime(2026, 8, 29, 4, 0, tzinfo=timezone.utc)
SIGNER_KEY_ID = "human-key:xujie-pacing"


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


def _install_authority(
    data_dir: Path,
    *,
    run_name: str = "authority",
) -> tuple[Path, Ed25519PrivateKey, dict[str, object]]:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    keyring_body = {
        "schema": PACING_KEYRING_SCHEMA,
        "trust_domain": PACING_PRODUCTION_HUMAN,
        "signer_key_id": SIGNER_KEY_ID,
        "signature_algorithm": "ED25519",
        "public_key_base64": base64.b64encode(public_key).decode("ascii"),
    }
    keyring_path = data_dir / "governance" / "pacing_authority_keyring.json"
    keyring_path.parent.mkdir(parents=True, exist_ok=True)
    keyring_path.write_text(
        json.dumps({**keyring_body, "content_hash": canonical_hash(keyring_body)}),
        encoding="utf-8",
    )

    authority_dir = (
        data_dir
        / "evidence"
        / "checkpoints"
        / "P0"
        / "market-data-pacing"
        / run_name
    )
    authority_dir.mkdir(parents=True, exist_ok=True)
    observed_at = NOW - timedelta(days=16)
    capability = MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=observed_at,
        source="conservative_default",
        request_classes=_request_classes(),
        signer=PACING_EXPECTED_ACTOR,
    ).as_dict()
    approval_body = {
        "schema_version": 2,
        "kind": PACING_POLICY_AUTHORITY_KIND,
        "authority_mode": PACING_POLICY_AUTHORITY_MODE,
        "actor": PACING_EXPECTED_ACTOR,
        "signer": PACING_EXPECTED_ACTOR,
        "decision": "APPROVED",
        "signed_at": (observed_at + timedelta(minutes=1)).isoformat(
            timespec="microseconds"
        ),
        "observed_at": capability["observed_at"],
        "version": capability["version"],
        "source": capability["source"],
        "capability": capability,
        "capability_content_hash": capability["content_hash"],
        "policy_content_hash": pacing_policy_content_hash(
            capability,
            actor=PACING_EXPECTED_ACTOR,
        ),
        "review_only": True,
        "direct_order_submission": False,
        "signer_key_id": SIGNER_KEY_ID,
        "signature_algorithm": "ED25519",
    }
    signature = base64.b64encode(
        private_key.sign(canonical_json(approval_body).encode("utf-8"))
    ).decode("ascii")
    signed = {**approval_body, "approval_signature": signature}
    approval = {**signed, "content_hash": canonical_hash(signed)}
    (authority_dir / "capability.json").write_text(
        json.dumps(capability),
        encoding="utf-8",
    )
    (authority_dir / "approval.json").write_text(
        json.dumps(approval),
        encoding="utf-8",
    )
    return authority_dir, private_key, approval


def _configure_data_dir(
    monkeypatch: pytest.MonkeyPatch,
    data_dir: Path,
) -> None:
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(data_dir))
    monkeypatch.delenv("OPTIONS_COPILOT_PACING_AUTHORITY_DIR", raising=False)
    monkeypatch.delenv("OPTIONS_COPILOT_PACING_CAPABILITY_PATH", raising=False)


def _pacing_report():
    inputs = load_default_probe_inputs(now=NOW)
    return build_readiness_report(
        inputs,
        probes=("market_data_pacing",),
        now=NOW,
    )


def test_unique_installed_signed_authority_is_auto_discovered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _install_authority(data_dir)
    _configure_data_dir(monkeypatch, data_dir)

    report = _pacing_report()

    assert report.status.value == "READY_FOR_REVIEW"
    assert report.reason_codes == ()
    details = report.records[0].details
    assert details["approval_actor"] == PACING_EXPECTED_ACTOR
    assert details["approval_signature_authenticated"] is True
    assert details["valid_until_revoked"] is True
    assert details["review_only"] is True
    assert details["direct_order_submission"] is False


def test_missing_keyring_keeps_signed_authority_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _install_authority(data_dir)
    (data_dir / "governance" / "pacing_authority_keyring.json").unlink()
    _configure_data_dir(monkeypatch, data_dir)

    report = _pacing_report()

    assert report.status.value != "READY_FOR_REVIEW"
    assert "PACING_APPROVAL_SIGNATURE_VERIFIER_MISSING" in report.reason_codes
    assert "PACING_CAPABILITY_MISSING" in report.reason_codes


def test_signed_revocation_keeps_authority_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    authority_dir, private_key, approval = _install_authority(data_dir)
    revocation_body = {
        "schema_version": 1,
        "kind": PACING_POLICY_REVOCATION_KIND,
        "actor": PACING_EXPECTED_ACTOR,
        "signer": PACING_EXPECTED_ACTOR,
        "decision": "REVOKED",
        "revoked_at": (NOW - timedelta(minutes=1)).isoformat(
            timespec="microseconds"
        ),
        "reason": "operator requested",
        "policy_content_hash": approval["policy_content_hash"],
        "approval_content_hash": approval["content_hash"],
        "review_only": True,
        "direct_order_submission": False,
        "signer_key_id": SIGNER_KEY_ID,
        "signature_algorithm": "ED25519",
    }
    signature = base64.b64encode(
        private_key.sign(canonical_json(revocation_body).encode("utf-8"))
    ).decode("ascii")
    signed = {**revocation_body, "revocation_signature": signature}
    (authority_dir / "revocation.json").write_text(
        json.dumps({**signed, "content_hash": canonical_hash(signed)}),
        encoding="utf-8",
    )
    _configure_data_dir(monkeypatch, data_dir)

    report = _pacing_report()

    assert report.status.value != "READY_FOR_REVIEW"
    assert "PACING_POLICY_REVOKED" in report.reason_codes
    assert "PACING_CAPABILITY_MISSING" in report.reason_codes


def test_ambiguous_installed_authorities_are_not_guessed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _install_authority(data_dir, run_name="first")
    _install_authority(data_dir, run_name="second")
    _configure_data_dir(monkeypatch, data_dir)

    report = _pacing_report()

    assert report.status.value != "READY_FOR_REVIEW"
    assert report.reason_codes == ("PACING_CAPABILITY_MISSING",)


def test_explicit_legacy_capability_observation_remains_compatible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    capability = MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=NOW - timedelta(minutes=1),
        source="broker_disclosed",
        request_classes=_request_classes(),
        signer=None,
    ).as_dict()
    capability_path = tmp_path / "legacy-capability.json"
    capability_path.write_text(json.dumps(capability), encoding="utf-8")
    _configure_data_dir(monkeypatch, data_dir)
    monkeypatch.setenv(
        "OPTIONS_COPILOT_PACING_CAPABILITY_PATH",
        str(capability_path),
    )

    report = _pacing_report()

    assert report.status.value == "READY_FOR_REVIEW"
    assert report.reason_codes == ()
    assert "approval_signature_authenticated" not in report.records[0].details
