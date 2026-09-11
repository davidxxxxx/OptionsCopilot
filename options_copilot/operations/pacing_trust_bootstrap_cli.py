"""Human-only bootstrap for the independent pacing approval trust root."""
from __future__ import annotations

import argparse
import base64
from collections.abc import Sequence
import json
import os
from pathlib import Path
import re
import sys
from typing import TextIO

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from options_copilot.config import OptionsCopilotConfig
from options_copilot.security.dpapi import DPAPISecretStore, SecretStoreError
from options_copilot.storage.canonical import canonical_hash

from .pacing_authority import PACING_KEYRING_SCHEMA, PACING_PRODUCTION_HUMAN


EXIT_OK = 0
EXIT_INVALID = 4
_CONFIRM_PREFIX = "INSTALL PACING TRUST "
_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
_PRIVATE_KEY_SECRET_NAME = "PACING_ED25519_PRIVATE_KEY"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.operations.pacing_trust_bootstrap_cli",
        description=(
            "Create one external Ed25519 private key and independently install "
            "its public pacing keyring"
        ),
    )
    parser.add_argument("--private-key-output", required=True, type=Path)
    parser.add_argument("--signer-key-id", required=True)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    project_root: Path | None = None,
) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    input_stream = stdin or sys.stdin
    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    try:
        if not _is_tty(input_stream) or not _is_tty(output_stream):
            raise ValueError("pacing trust bootstrap requires a real interactive terminal")

        root = (project_root or Path(__file__).resolve().parents[2]).resolve()
        private_path = args.private_key_output.expanduser()
        if not private_path.is_absolute():
            raise ValueError("private key output must be an absolute path")
        private_path = private_path.resolve()
        if _is_within(private_path, root):
            raise ValueError("private key output must be outside the project")
        if not private_path.parent.is_dir():
            raise ValueError("private key parent directory must already exist")

        runtime_data_dir = (
            root / "data" / "options_copilot"
            if project_root is not None
            else OptionsCopilotConfig.from_env().data_dir.resolve()
        )
        keyring_path = (
            runtime_data_dir
            / "governance"
            / "pacing_authority_keyring.json"
        )
        if private_path.exists():
            raise FileExistsError("private key output must not exist")
        if keyring_path.exists():
            raise FileExistsError("production keyring already exists")

        signer_key_id = str(args.signer_key_id or "").strip()
        if (
            _KEY_ID_RE.fullmatch(signer_key_id) is None
            or signer_key_id.lower().startswith("test-only:")
        ):
            raise ValueError("production signer key id is invalid")

        private_key = Ed25519PrivateKey.generate()
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        keyring_body = {
            "schema": PACING_KEYRING_SCHEMA,
            "trust_domain": PACING_PRODUCTION_HUMAN,
            "signer_key_id": signer_key_id,
            "signature_algorithm": "ED25519",
            "public_key_base64": base64.b64encode(public_key).decode("ascii"),
        }
        keyring = {**keyring_body, "content_hash": canonical_hash(keyring_body)}
        challenge = canonical_hash(
            {
                "schema": "options_copilot.pacing_trust_bootstrap.v1",
                "signer_key_id": signer_key_id,
                "private_key_output": str(private_path),
                "keyring_output": str(keyring_path),
                "keyring_content_hash": keyring["content_hash"],
                "review_only": True,
                "direct_order_submission": False,
            }
        )

        output_stream.write("PACING TRUST BOOTSTRAP REVIEW\n")
        output_stream.write(f"PRIVATE_KEY_OUTPUT {private_path}\n")
        output_stream.write(f"PUBLIC_KEYRING_OUTPUT {keyring_path}\n")
        output_stream.write(f"KEYRING_CONTENT_HASH {keyring['content_hash']}\n")
        output_stream.write(f"CHALLENGE_HASH {challenge}\n")
        output_stream.write(
            f"TYPE {_CONFIRM_PREFIX}{challenge} TO INSTALL (anything else aborts): "
        )
        output_stream.flush()
        if input_stream.readline().strip() != f"{_CONFIRM_PREFIX}{challenge}":
            raise ValueError("human confirmation did not match the exact challenge")

        keyring_path.parent.mkdir(parents=True, exist_ok=True)
        private_created = False
        try:
            _write_dpapi_private_key(private_path, private_pem)
            private_created = True
            _write_exclusive(
                keyring_path,
                (json.dumps(keyring, sort_keys=True, indent=2) + "\n").encode("utf-8"),
                mode=0o644,
            )
        except Exception:
            if private_created:
                private_path.unlink(missing_ok=True)
            raise

        output_stream.write(f"INSTALLED {keyring['content_hash']}\n")
        output_stream.write(
            "PRIVATE KEY IS WINDOWS-DPAPI PROTECTED; KEEP ITS ENVELOPE OUTSIDE "
            "THE PROJECT AND DO NOT SHARE IT.\n"
        )
        return EXIT_OK
    except (
        FileExistsError,
        FileNotFoundError,
        OSError,
        SecretStoreError,
        TypeError,
        ValueError,
    ) as exc:
        error_stream.write(f"PACING_TRUST_NOT_CREATED: {exc}\n")
        return EXIT_INVALID


def _write_exclusive(path: Path, data: bytes, *, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        path.unlink(missing_ok=True)
        raise


def _write_dpapi_private_key(path: Path, private_pem: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.urandom(8).hex()}.tmp")
    try:
        DPAPISecretStore(temporary).set(
            _PRIVATE_KEY_SECRET_NAME,
            private_pem.decode("ascii"),
        )
        _write_exclusive(path, temporary.read_bytes(), mode=0o600)
    finally:
        temporary.unlink(missing_ok=True)


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
