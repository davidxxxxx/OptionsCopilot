"""Interactive, human-only pacing approval ceremony with no broker authority."""
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
from options_copilot.operations.capabilities import (
    CapabilityStatus,
    MarketDataPacingCapability,
)
from options_copilot.security.dpapi import DPAPISecretStore, SecretStoreError
from options_copilot.storage.canonical import canonical_hash, canonical_json, utc_datetime

from .pacing_authority import (
    PACING_POLICY_AUTHORITY_KIND,
    PACING_POLICY_AUTHORITY_MODE,
    load_approved_pacing_capability,
    load_pacing_authority_verifier,
    pacing_policy_content_hash,
)


EXIT_OK = 0
EXIT_INVALID = 4
_PRIVATE_KEY_BEGIN = "-----BEGIN PRIVATE KEY-----"
_CONFIRM_PREFIX = "SIGN PACING "
_PRIVATE_KEY_SECRET_NAME = "PACING_ED25519_PRIVATE_KEY"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.operations.pacing_authority_cli",
        description="Sign one fresh read-only pacing proposal in an interactive terminal",
    )
    parser.add_argument("--capability", required=True, type=Path)
    parser.add_argument("--private-key", required=True, type=Path)
    parser.add_argument("--signer-key-id", required=True)
    parser.add_argument("--actor", default="human:xujie")
    parser.add_argument("--approval-output", required=True, type=Path)
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
    try:
        if not _is_tty(input_stream) or not _is_tty(output_stream):
            raise ValueError("pacing signing requires a real interactive terminal")
        capability = _read_object(args.capability)
        actor = str(args.actor or "").strip()
        signer_key_id = str(args.signer_key_id or "").strip()
        if not actor.startswith("human:") or len(actor) <= len("human:"):
            raise ValueError("actor must use the human:<identity> form")
        if not signer_key_id or signer_key_id.lower().startswith("test-only:"):
            raise ValueError("production signer key id is invalid")
        if args.approval_output.exists():
            raise FileExistsError("approval output must not exist")
        install_capability = args.approval_output.parent / "capability.json"
        if args.capability.resolve() != install_capability.resolve():
            raise ValueError(
                "capability must be the capability.json beside approval output"
            )
        if not args.approval_output.parent.is_dir():
            raise ValueError("approval output parent directory must already exist")

        runtime_data_dir = (
            data_dir.resolve()
            if data_dir is not None
            else OptionsCopilotConfig.from_env().data_dir.resolve()
        )
        keyring_path = (
            runtime_data_dir
            / "governance"
            / "pacing_authority_keyring.json"
        ).resolve()
        pacing_root = (
            runtime_data_dir
            / "evidence"
            / "checkpoints"
            / "P0"
            / "market-data-pacing"
        ).resolve()
        if not _is_within(args.approval_output.resolve(), pacing_root):
            raise ValueError("approval output must be inside the runtime pacing root")

        private_key = _load_private_key(args.private_key)
        verifier = load_pacing_authority_verifier(keyring_path)
        if verifier is None:
            raise ValueError("an independently installed production keyring is required")
        signed_at = datetime.now(timezone.utc)
        _validate_capability(capability, now=signed_at, expected_actor=actor)
        approval_body = {
            "schema_version": 2,
            "kind": PACING_POLICY_AUTHORITY_KIND,
            "authority_mode": PACING_POLICY_AUTHORITY_MODE,
            "actor": actor,
            "signer": actor,
            "decision": "APPROVED",
            "signed_at": signed_at.isoformat(timespec="microseconds"),
            "observed_at": capability["observed_at"],
            "version": capability["version"],
            "source": capability["source"],
            "capability": capability,
            "capability_content_hash": capability["content_hash"],
            "policy_content_hash": pacing_policy_content_hash(
                capability,
                actor=actor,
            ),
            "review_only": True,
            "direct_order_submission": False,
            "signer_key_id": signer_key_id,
            "signature_algorithm": "ED25519",
        }
        challenge_hash = canonical_hash(approval_body)
        output_stream.write("PACING HUMAN SIGNATURE REVIEW\n")
        output_stream.write(canonical_json(approval_body) + "\n")
        output_stream.write(f"CHALLENGE_HASH {challenge_hash}\n")
        output_stream.write(
            f"TYPE {_CONFIRM_PREFIX}{challenge_hash} TO SIGN (anything else aborts): "
        )
        output_stream.flush()
        if input_stream.readline().strip() != f"{_CONFIRM_PREFIX}{challenge_hash}":
            raise ValueError("human confirmation did not match the exact challenge")

        signature = base64.b64encode(
            private_key.sign(canonical_json(approval_body).encode("utf-8"))
        ).decode("ascii")
        if not verifier.verify(
            signer_key_id=signer_key_id,
            signature_algorithm="ED25519",
            message=canonical_json(approval_body).encode("utf-8"),
            signature=signature,
        ):
            raise ValueError("private key does not match the installed production keyring")
        signed_approval = {**approval_body, "approval_signature": signature}
        approval = {
            **signed_approval,
            "content_hash": canonical_hash(signed_approval),
        }
        _write_exclusive(args.approval_output, approval)
        loaded = load_approved_pacing_capability(
            args.approval_output.parent,
            now=datetime.now(timezone.utc),
            expected_actor=actor,
            signature_verifier=verifier,
        )
        if not loaded.ready:
            args.approval_output.unlink(missing_ok=True)
            reasons = ",".join(loaded.record.reason_codes) or loaded.record.status.value
            raise ValueError(f"production loader rejected installed approval: {reasons}")
        selected = _unique_approved_directory(pacing_root)
        if selected != args.approval_output.parent.resolve():
            args.approval_output.unlink(missing_ok=True)
            raise ValueError("runtime pacing discovery did not select this approval")
        if data_dir is None:
            runtime_config = OptionsCopilotConfig.from_env()
            if (
                runtime_config.pacing_authority_dir != selected
                or runtime_config.pacing_authority_keyring_path != keyring_path
            ):
                args.approval_output.unlink(missing_ok=True)
                raise ValueError("runtime configuration rejected installed pacing authority")
        output_stream.write(f"SIGNED {approval['content_hash']}\n")
        output_stream.write(
            "PACING_POLICY_AUTHORITY LONG_LIVED_UNTIL_REVOKED\n"
        )
        output_stream.write("PRODUCTION_LOADER_READY_FOR_REVIEW\n")
        return EXIT_OK
    except (
        FileExistsError,
        FileNotFoundError,
        OSError,
        SecretStoreError,
        TypeError,
        ValueError,
    ) as exc:
        error_stream.write(f"PACING_SIGNATURE_NOT_CREATED: {exc}\n")
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


def _unique_approved_directory(root: Path) -> Path | None:
    try:
        candidates = tuple(
            item.resolve()
            for item in root.iterdir()
            if item.is_dir()
            and item.name != "archive"
            and (item / "capability.json").is_file()
            and (item / "approval.json").is_file()
        )
    except OSError:
        return None
    return candidates[0] if len(candidates) == 1 else None


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _validate_capability(
    value: Mapping[str, object],
    *,
    now: datetime,
    expected_actor: str,
) -> None:
    required = {
        "version",
        "observed_at",
        "source",
        "request_classes",
        "signer",
        "content_hash",
    }
    if set(value) != required:
        raise ValueError("capability fields are invalid")
    signable = dict(value)
    supplied_hash = signable.pop("content_hash")
    if not isinstance(supplied_hash, str) or canonical_hash(signable) != supplied_hash:
        raise ValueError("capability content hash mismatch")
    if value.get("signer") != expected_actor:
        raise ValueError("capability signer does not match the approving human")
    inspected = MarketDataPacingCapability.inspect(value, now=now)
    if inspected.status is not CapabilityStatus.READY_FOR_REVIEW:
        reasons = ",".join(inspected.reason_codes) or inspected.status.value
        raise ValueError(f"capability is not current and installable: {reasons}")


def _read_object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("JSON document must be an object")
    return {str(key): item for key, item in value.items()}


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("observed_at must be an ISO timestamp")
    return utc_datetime(
        datetime.fromisoformat(value.replace("Z", "+00:00")),
        field="observed_at",
    )


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


def _is_tty(stream: TextIO) -> bool:
    return bool(getattr(stream, "isatty", lambda: False)())


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
