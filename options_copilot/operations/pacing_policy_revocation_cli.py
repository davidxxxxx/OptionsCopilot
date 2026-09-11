"""Interactive, human-only revocation for a long-lived pacing policy."""
from __future__ import annotations

import argparse
import base64
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import TextIO

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from options_copilot.config import OptionsCopilotConfig
from options_copilot.security.dpapi import DPAPISecretStore, SecretStoreError
from options_copilot.storage.canonical import canonical_hash, canonical_json

from .pacing_authority import (
    PACING_POLICY_REVOCATION_KIND,
    load_approved_pacing_capability,
    load_pacing_authority_verifier,
)


EXIT_OK = 0
EXIT_INVALID = 4
_PRIVATE_KEY_BEGIN = "-----BEGIN PRIVATE KEY-----"
_PRIVATE_KEY_SECRET_NAME = "PACING_ED25519_PRIVATE_KEY"
_CONFIRM_PREFIX = "REVOKE PACING "


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.operations.pacing_policy_revocation_cli",
        description="Revoke one installed long-lived read-only pacing policy",
    )
    parser.add_argument("--authority-dir", required=True, type=Path)
    parser.add_argument("--private-key", required=True, type=Path)
    parser.add_argument("--signer-key-id", required=True)
    parser.add_argument("--actor", default="human:xujie")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--revocation-output", type=Path)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    data_dir: Path | None = None,
) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    input_stream = stdin or sys.stdin
    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    created_path: Path | None = None
    revocation_committed = False
    try:
        if not _is_tty(input_stream) or not _is_tty(output_stream):
            raise ValueError("pacing revocation requires a real interactive terminal")
        actor = str(args.actor or "").strip()
        signer_key_id = str(args.signer_key_id or "").strip()
        reason = str(args.reason or "").strip()
        if not actor.startswith("human:") or len(actor) <= len("human:"):
            raise ValueError("actor must use the human:<identity> form")
        if not signer_key_id or signer_key_id.lower().startswith("test-only:"):
            raise ValueError("production signer key id is invalid")
        if not reason or len(reason) > 500:
            raise ValueError("revocation reason must contain 1 to 500 characters")

        runtime_data_dir = (
            data_dir.resolve()
            if data_dir is not None
            else OptionsCopilotConfig.from_env().data_dir.resolve()
        )
        keyring_path = (
            runtime_data_dir / "governance" / "pacing_authority_keyring.json"
        ).resolve()
        pacing_root = (
            runtime_data_dir
            / "evidence"
            / "checkpoints"
            / "P0"
            / "market-data-pacing"
        ).resolve()
        authority_dir = args.authority_dir.resolve()
        if not _is_within(authority_dir, pacing_root):
            raise ValueError("authority directory must be inside the runtime pacing root")
        revocation_output = (
            args.revocation_output.resolve()
            if args.revocation_output is not None
            else (authority_dir / "revocation.json").resolve()
        )
        if revocation_output != (authority_dir / "revocation.json").resolve():
            raise ValueError("revocation output must be authority-dir/revocation.json")
        if revocation_output.exists():
            raise FileExistsError("revocation output already exists")

        verifier = load_pacing_authority_verifier(keyring_path)
        if verifier is None:
            raise ValueError("an independently installed production keyring is required")
        current = load_approved_pacing_capability(
            authority_dir,
            now=datetime.now(timezone.utc),
            expected_actor=actor,
            signature_verifier=verifier,
        )
        policy = current.policy_authority
        if not current.ready or policy is None:
            reasons = ",".join(current.record.reason_codes) or current.record.status.value
            raise ValueError(f"installed long-lived policy is not active: {reasons}")
        if policy.signer_key_id != signer_key_id:
            raise ValueError("signer key id does not match the installed policy")

        private_key = _load_private_key(args.private_key)
        revoked_at = datetime.now(timezone.utc)
        body = {
            "schema_version": 1,
            "kind": PACING_POLICY_REVOCATION_KIND,
            "actor": actor,
            "signer": actor,
            "decision": "REVOKED",
            "revoked_at": revoked_at.isoformat(timespec="microseconds"),
            "reason": reason,
            "policy_content_hash": policy.policy_content_hash,
            "approval_content_hash": policy.approval_hash,
            "review_only": True,
            "direct_order_submission": False,
            "signer_key_id": signer_key_id,
            "signature_algorithm": "ED25519",
        }
        challenge_hash = canonical_hash(body)
        output_stream.write("PACING POLICY REVOCATION REVIEW\n")
        output_stream.write(canonical_json(body) + "\n")
        output_stream.write(f"CHALLENGE_HASH {challenge_hash}\n")
        output_stream.write(
            f"TYPE {_CONFIRM_PREFIX}{challenge_hash} TO REVOKE (anything else aborts): "
        )
        output_stream.flush()
        if input_stream.readline().strip() != f"{_CONFIRM_PREFIX}{challenge_hash}":
            raise ValueError("human confirmation did not match the exact challenge")

        signature = base64.b64encode(
            private_key.sign(canonical_json(body).encode("utf-8"))
        ).decode("ascii")
        if not verifier.verify(
            signer_key_id=signer_key_id,
            signature_algorithm="ED25519",
            message=canonical_json(body).encode("utf-8"),
            signature=signature,
        ):
            raise ValueError("private key does not match the installed production keyring")
        signed = {**body, "revocation_signature": signature}
        document = {**signed, "content_hash": canonical_hash(signed)}
        _write_exclusive(revocation_output, document)
        created_path = revocation_output
        # Exclusive creation is the durable authority commit point.  Once the
        # signed revocation exists, no verification or reporting failure may
        # delete it and accidentally resurrect the pacing policy.
        revocation_committed = True

        revoked = load_approved_pacing_capability(
            authority_dir,
            now=datetime.now(timezone.utc),
            expected_actor=actor,
            signature_verifier=verifier,
        )
        if (
            revoked.ready
            or "PACING_POLICY_REVOKED" not in revoked.record.reason_codes
        ):
            raise ValueError("production loader did not enforce the revocation")
        output_stream.write(f"REVOKED {document['content_hash']}\n")
        output_stream.write("PACING_POLICY_REVOKED\n")
        return EXIT_OK
    except (
        FileExistsError,
        FileNotFoundError,
        OSError,
        SecretStoreError,
        TypeError,
        ValueError,
    ) as exc:
        if created_path is not None and not revocation_committed:
            created_path.unlink(missing_ok=True)
        prefix = (
            "PACING_REVOCATION_COMMITTED_REPORTING_FAILED"
            if revocation_committed
            else "PACING_REVOCATION_NOT_CREATED"
        )
        error_stream.write(f"{prefix}: {exc}\n")
        return EXIT_INVALID


def _load_private_key(path: Path) -> Ed25519PrivateKey:
    value = DPAPISecretStore(path).get(_PRIVATE_KEY_SECRET_NAME)
    if value is None:
        raise ValueError("DPAPI pacing private key is missing")
    data = value.encode("utf-8")
    if _PRIVATE_KEY_BEGIN.encode("ascii") not in data:
        raise ValueError("private key must be an unencrypted PKCS8 PEM Ed25519 key")
    try:
        key = serialization.load_pem_private_key(data, password=None)
    except (TypeError, ValueError) as exc:
        raise ValueError("private key could not be loaded") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("private key must be Ed25519")
    return key


def _write_exclusive(path: Path, value: Mapping[str, object]) -> None:
    rendered = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    ) + "\n"
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(rendered)
        stream.flush()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _is_tty(stream: TextIO) -> bool:
    return bool(getattr(stream, "isatty", lambda: False)())


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
