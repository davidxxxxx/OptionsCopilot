"""Interactive pacing signing ceremony tests."""
from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import re

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

import options_copilot.operations.pacing_policy_revocation_cli as revocation_cli
from options_copilot.operations.pacing_authority import (
    PACING_KEYRING_SCHEMA,
    PACING_POLICY_AUTHORITY_KIND,
    PACING_POLICY_AUTHORITY_MODE,
    PACING_PRODUCTION_HUMAN,
    load_approved_pacing_capability,
    load_pacing_authority_verifier,
)
from options_copilot.operations.pacing_authority_cli import main
from options_copilot.operations.pacing_policy_revocation_cli import (
    main as revoke_main,
)
from options_copilot.security.dpapi import DPAPISecretStore
from options_copilot.storage.canonical import canonical_hash


class FakeTty(io.StringIO):
    def isatty(self) -> bool:
        return True


class ExactChallengeReply(FakeTty):
    def __init__(self, output: io.StringIO) -> None:
        super().__init__()
        self._output = output

    def readline(self, *args, **kwargs) -> str:
        match = re.search(r"CHALLENGE_HASH ([0-9a-f]{64})", self._output.getvalue())
        assert match is not None
        return f"SIGN PACING {match.group(1)}\n"


class ExactRevocationReply(FakeTty):
    def __init__(self, output: io.StringIO) -> None:
        super().__init__()
        self._output = output

    def readline(self, *args, **kwargs) -> str:
        match = re.search(r"CHALLENGE_HASH ([0-9a-f]{64})", self._output.getvalue())
        assert match is not None
        return f"REVOKE PACING {match.group(1)}\n"


class FinalRevocationReportFailure(FakeTty):
    def write(self, value: str) -> int:
        if value.startswith("REVOKED "):
            raise OSError("terminal closed after durable revocation")
        return super().write(value)


def _proposal(path: Path, *, observed_at: datetime | None = None) -> None:
    body = {
        "version": "market-data-pacing.v1",
        "observed_at": (observed_at or datetime.now(timezone.utc)).isoformat(),
        "source": "conservative_default",
        "request_classes": {
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
        },
        "signer": "human:xujie",
    }
    path.write_text(
        json.dumps({**body, "content_hash": canonical_hash(body)}),
        encoding="utf-8",
    )


def _private_key(path: Path) -> Ed25519PrivateKey:
    private_key = Ed25519PrivateKey.generate()
    private_pem = (
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    DPAPISecretStore(path).set(
        "PACING_ED25519_PRIVATE_KEY",
        private_pem.decode("ascii"),
    )
    return private_key


def _keyring(path: Path, private_key: Ed25519PrivateKey) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    body = {
        "schema": PACING_KEYRING_SCHEMA,
        "trust_domain": PACING_PRODUCTION_HUMAN,
        "signer_key_id": "human-key:xujie-pacing",
        "signature_algorithm": "ED25519",
        "public_key_base64": base64.b64encode(public_key).decode("ascii"),
    }
    path.write_text(
        json.dumps({**body, "content_hash": canonical_hash(body)}),
        encoding="utf-8",
    )


def _install_paths(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    data_dir = tmp_path / "data"
    run_directory = (
        data_dir
        / "evidence"
        / "checkpoints"
        / "P0"
        / "market-data-pacing"
        / "run"
    )
    run_directory.mkdir(parents=True)
    return (
        data_dir,
        run_directory / "capability.json",
        run_directory / "approval.json",
        data_dir / "governance" / "pacing_authority_keyring.json",
    )


def test_noninteractive_signing_is_rejected_without_outputs(tmp_path: Path) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)

    exit_code = main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=io.StringIO(),
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        data_dir=data_dir,
    )

    assert exit_code == 4
    assert not approval.exists()
    assert keyring.exists()


def test_wrong_interactive_challenge_is_rejected_without_outputs(
    tmp_path: Path,
) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)

    exit_code = main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=FakeTty("NO\n"),
        stdout=FakeTty(),
        stderr=io.StringIO(),
        data_dir=data_dir,
    )

    assert exit_code == 4
    assert not approval.exists()
    assert keyring.exists()


def test_exact_interactive_challenge_creates_production_loadable_pair(
    tmp_path: Path,
) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    run_directory = proposal.parent
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)
    output = FakeTty()

    exit_code = main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=ExactChallengeReply(output),
        stdout=output,
        stderr=io.StringIO(),
        data_dir=data_dir,
    )

    assert exit_code == 0
    verifier = load_pacing_authority_verifier(keyring)
    result = load_approved_pacing_capability(
        run_directory,
        now=datetime.now(timezone.utc),
        expected_actor="human:xujie",
        signature_verifier=verifier,
    )
    assert result.ready is True
    assert result.signer_key_id == "human-key:xujie-pacing"
    assert result.record.details["approval_signature_authenticated"] is True
    assert result.record.details["review_only"] is True
    assert result.record.details["direct_order_submission"] is False
    approval_document = json.loads(approval.read_text(encoding="utf-8"))
    assert approval_document["schema_version"] == 2
    assert approval_document["kind"] == PACING_POLICY_AUTHORITY_KIND
    assert approval_document["authority_mode"] == PACING_POLICY_AUTHORITY_MODE
    assert result.policy_authority is not None
    assert result.record.details["valid_until_revoked"] is True
    future = load_approved_pacing_capability(
        run_directory,
        now=datetime.now(timezone.utc) + timedelta(days=365),
        expected_actor="human:xujie",
        signature_verifier=verifier,
    )
    assert future.ready is True
    assert "PRODUCTION_LOADER_READY_FOR_REVIEW" in output.getvalue()
    assert "PACING_POLICY_AUTHORITY LONG_LIVED_UNTIL_REVOKED" in output.getvalue()


def test_capability_must_be_installed_beside_approval(tmp_path: Path) -> None:
    data_dir, _installed_proposal, approval, keyring = _install_paths(tmp_path)
    proposal = tmp_path / "capability.proposed.json"
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)
    error = io.StringIO()

    exit_code = main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=FakeTty(),
        stdout=FakeTty(),
        stderr=error,
        data_dir=data_dir,
    )

    assert exit_code == 4
    assert "capability.json beside approval output" in error.getvalue()
    assert not approval.exists()


def test_stale_proposal_is_rejected_before_signature_outputs(tmp_path: Path) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    private_key = tmp_path / "private.pem"
    _proposal(
        proposal,
        observed_at=datetime.now(timezone.utc) - timedelta(hours=25),
    )
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)
    output = FakeTty()
    error = io.StringIO()

    exit_code = main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=ExactChallengeReply(output),
        stdout=output,
        stderr=error,
        data_dir=data_dir,
    )

    assert exit_code == 4
    assert "PACING_OBSERVATION_STALE" in error.getvalue()
    assert "CHALLENGE_HASH" not in output.getvalue()
    assert not approval.exists()
    assert keyring.exists()


def test_private_key_must_match_preinstalled_keyring(tmp_path: Path) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    _private_key(private_key)
    _keyring(keyring, Ed25519PrivateKey.generate())
    output = FakeTty()
    error = io.StringIO()

    exit_code = main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=ExactChallengeReply(output),
        stdout=output,
        stderr=error,
        data_dir=data_dir,
    )

    assert exit_code == 4
    assert "does not match the installed production keyring" in error.getvalue()
    assert not approval.exists()


def test_interactive_revocation_blocks_the_installed_policy(tmp_path: Path) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)
    signing_output = FakeTty()
    assert main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=ExactChallengeReply(signing_output),
        stdout=signing_output,
        stderr=io.StringIO(),
        data_dir=data_dir,
    ) == 0

    revocation_output = FakeTty()
    exit_code = revoke_main(
        [
            "--authority-dir",
            str(proposal.parent),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--reason",
            "operator stop",
        ],
        stdin=ExactRevocationReply(revocation_output),
        stdout=revocation_output,
        stderr=io.StringIO(),
        data_dir=data_dir,
    )

    assert exit_code == 0
    assert (proposal.parent / "revocation.json").is_file()
    revoked = load_approved_pacing_capability(
        proposal.parent,
        now=datetime.now(timezone.utc),
        expected_actor="human:xujie",
        signature_verifier=load_pacing_authority_verifier(keyring),
    )
    assert revoked.ready is False
    assert "PACING_POLICY_REVOKED" in revoked.record.reason_codes
    assert "PACING_POLICY_REVOKED" in revocation_output.getvalue()


def test_revocation_remains_durable_when_final_terminal_reporting_fails(
    tmp_path: Path,
) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)
    signing_output = FakeTty()
    assert main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=ExactChallengeReply(signing_output),
        stdout=signing_output,
        stderr=io.StringIO(),
        data_dir=data_dir,
    ) == 0

    revocation_output = FinalRevocationReportFailure()
    error = io.StringIO()
    assert revoke_main(
        [
            "--authority-dir",
            str(proposal.parent),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--reason",
            "operator stop",
        ],
        stdin=ExactRevocationReply(revocation_output),
        stdout=revocation_output,
        stderr=error,
        data_dir=data_dir,
    ) == 4

    assert (proposal.parent / "revocation.json").is_file()
    revoked = load_approved_pacing_capability(
        proposal.parent,
        now=datetime.now(timezone.utc),
        expected_actor="human:xujie",
        signature_verifier=load_pacing_authority_verifier(keyring),
    )
    assert revoked.ready is False
    assert "PACING_POLICY_REVOKED" in revoked.record.reason_codes
    assert "PACING_REVOCATION_COMMITTED_REPORTING_FAILED" in error.getvalue()


def test_revocation_remains_durable_when_post_write_loader_verification_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir, proposal, approval, keyring = _install_paths(tmp_path)
    private_key = tmp_path / "private.pem"
    _proposal(proposal)
    signing_key = _private_key(private_key)
    _keyring(keyring, signing_key)
    signing_output = FakeTty()
    assert main(
        [
            "--capability",
            str(proposal),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--approval-output",
            str(approval),
        ],
        stdin=ExactChallengeReply(signing_output),
        stdout=signing_output,
        stderr=io.StringIO(),
        data_dir=data_dir,
    ) == 0

    real_loader = revocation_cli.load_approved_pacing_capability
    loader_calls = 0

    def fail_only_after_write(*args, **kwargs):
        nonlocal loader_calls
        loader_calls += 1
        if loader_calls == 2:
            raise ValueError("injected post-write loader failure")
        return real_loader(*args, **kwargs)

    monkeypatch.setattr(
        revocation_cli,
        "load_approved_pacing_capability",
        fail_only_after_write,
    )
    revocation_output = FakeTty()
    error = io.StringIO()
    assert revoke_main(
        [
            "--authority-dir",
            str(proposal.parent),
            "--private-key",
            str(private_key),
            "--signer-key-id",
            "human-key:xujie-pacing",
            "--reason",
            "operator stop",
        ],
        stdin=ExactRevocationReply(revocation_output),
        stdout=revocation_output,
        stderr=error,
        data_dir=data_dir,
    ) == 4

    revocation_path = proposal.parent / "revocation.json"
    assert revocation_path.is_file()
    revoked = load_approved_pacing_capability(
        proposal.parent,
        now=datetime.now(timezone.utc),
        expected_actor="human:xujie",
        signature_verifier=load_pacing_authority_verifier(keyring),
    )
    assert revoked.ready is False
    assert "PACING_POLICY_REVOKED" in revoked.record.reason_codes
    assert "PACING_REVOCATION_COMMITTED_REPORTING_FAILED" in error.getvalue()
