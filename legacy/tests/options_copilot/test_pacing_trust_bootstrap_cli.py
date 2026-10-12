"""Human-only pacing trust bootstrap tests."""
from __future__ import annotations

import base64
import io
from pathlib import Path
import re

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from options_copilot.operations.pacing_authority import (
    load_pacing_authority_verifier,
)
from options_copilot.operations.pacing_trust_bootstrap_cli import main
from options_copilot.security.dpapi import DPAPISecretStore


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
        return f"INSTALL PACING TRUST {match.group(1)}\n"


def _args(private_key: Path) -> list[str]:
    return [
        "--private-key-output",
        str(private_key),
        "--signer-key-id",
        "human-key:xujie-pacing",
    ]


def test_noninteractive_bootstrap_creates_nothing(tmp_path: Path) -> None:
    project = tmp_path / "project"
    external = tmp_path / "external"
    project.mkdir()
    external.mkdir()
    private_key = external / "pacing-private.pem"

    exit_code = main(
        _args(private_key),
        stdin=io.StringIO(),
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        project_root=project,
    )

    assert exit_code == 4
    assert not private_key.exists()
    assert not (project / "data").exists()


def test_private_key_inside_project_is_rejected(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    private_key = project / "pacing-private.pem"
    error = io.StringIO()

    exit_code = main(
        _args(private_key),
        stdin=FakeTty(),
        stdout=FakeTty(),
        stderr=error,
        project_root=project,
    )

    assert exit_code == 4
    assert "outside the project" in error.getvalue()
    assert not private_key.exists()


def test_wrong_challenge_creates_nothing(tmp_path: Path) -> None:
    project = tmp_path / "project"
    external = tmp_path / "external"
    project.mkdir()
    external.mkdir()
    private_key = external / "pacing-private.pem"

    exit_code = main(
        _args(private_key),
        stdin=FakeTty("NO\n"),
        stdout=FakeTty(),
        stderr=io.StringIO(),
        project_root=project,
    )

    assert exit_code == 4
    assert not private_key.exists()
    assert not (project / "data").exists()


def test_exact_challenge_installs_matching_external_key_and_public_keyring(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    external = tmp_path / "external"
    project.mkdir()
    external.mkdir()
    private_key_path = external / "pacing-private.pem"
    output = FakeTty()

    exit_code = main(
        _args(private_key_path),
        stdin=ExactChallengeReply(output),
        stdout=output,
        stderr=io.StringIO(),
        project_root=project,
    )

    assert exit_code == 0
    keyring_path = (
        project
        / "data"
        / "options_copilot"
        / "governance"
        / "pacing_authority_keyring.json"
    )
    verifier = load_pacing_authority_verifier(keyring_path)
    assert verifier is not None
    protected_pem = DPAPISecretStore(private_key_path).get(
        "PACING_ED25519_PRIVATE_KEY"
    )
    assert protected_pem is not None
    assert b"PRIVATE KEY" not in private_key_path.read_bytes()
    private_key = serialization.load_pem_private_key(
        protected_pem.encode("ascii"),
        password=None,
    )
    assert isinstance(private_key, Ed25519PrivateKey)
    message = b"pacing trust bootstrap proof"
    signature = base64.b64encode(private_key.sign(message)).decode("ascii")
    assert verifier.verify(
        signer_key_id="human-key:xujie-pacing",
        signature_algorithm="ED25519",
        message=message,
        signature=signature,
    )


def test_existing_keyring_is_never_replaced(tmp_path: Path) -> None:
    project = tmp_path / "project"
    external = tmp_path / "external"
    keyring = (
        project
        / "data"
        / "options_copilot"
        / "governance"
        / "pacing_authority_keyring.json"
    )
    keyring.parent.mkdir(parents=True)
    keyring.write_text("existing", encoding="utf-8")
    external.mkdir()
    private_key = external / "pacing-private.pem"
    error = io.StringIO()

    exit_code = main(
        _args(private_key),
        stdin=FakeTty(),
        stdout=FakeTty(),
        stderr=error,
        project_root=project,
    )

    assert exit_code == 4
    assert "already exists" in error.getvalue()
    assert keyring.read_text(encoding="utf-8") == "existing"
    assert not private_key.exists()
