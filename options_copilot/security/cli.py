"""Interactive secret administration; values are never command-line arguments."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import getpass
import json
from pathlib import Path

from options_copilot.config import OptionsCopilotConfig
from options_copilot.security.dpapi import DPAPISecretStore
from options_copilot.security.jin10_credentials import (
    Jin10CredentialError,
    activate_jin10_credential,
    encode_jin10_credential,
    resolve_jin10_credential,
)
from options_copilot.security.local_api_keys import (
    LocalApiKeyFileError,
    LocalApiKeyStore,
    LocalJin10BindingError,
    LocalJin10EnvelopeReader,
    local_api_key_path,
)
from options_copilot.security.revocation import (
    JIN10_SECRET_NAME,
    RevocationAttestation,
    RevocationError,
    latest_rotation_manifest,
    load_revocation_attestation,
    reserve_rotation,
    write_revocation_attestation,
)
from options_copilot.storage.canonical import datetime_text


ALLOWED_SECRET_NAMES = {
    "FINNHUB_API_KEY",
    "ALPHA_VANTAGE_API_KEY",
    "JIN10_MCP_TOKEN",
    "DEEPSEEK_API_KEY",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Options Copilot credentials and rotation evidence"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    set_parser = subparsers.add_parser("set", help="securely prompt for and encrypt a secret")
    set_parser.add_argument("name", choices=sorted(ALLOWED_SECRET_NAMES))
    set_parser.add_argument(
        "--revocation-attestation",
        type=Path,
        help="exact verified revocation artifact required only for JIN10_MCP_TOKEN",
    )
    delete_parser = subparsers.add_parser("delete", help="delete one encrypted secret")
    delete_parser.add_argument("name", choices=sorted(ALLOWED_SECRET_NAMES))
    subparsers.add_parser("list", help="list configured key names without values")

    attest_parser = subparsers.add_parser(
        "attest-revocation",
        help="record an explicit human assertion that the old Jin10 token was revoked",
    )
    attest_parser.add_argument("--name", required=True, choices=[JIN10_SECRET_NAME])
    attest_parser.add_argument(
        "--old-token-revoked",
        required=True,
        action="store_true",
        help="required explicit assertion; the old credential is never read",
    )
    attest_parser.add_argument("--actor", required=True)
    attest_parser.add_argument("--evidence-dir", required=True, type=Path)
    attest_parser.add_argument("--json", action="store_true")

    verify_parser = subparsers.add_parser(
        "verify-revocation", help="verify one exact revocation attestation"
    )
    verify_parser.add_argument("--path", required=True, type=Path)
    verify_parser.add_argument("--json", action="store_true")

    activate_local_parser = subparsers.add_parser(
        "activate-local-jin10",
        help="activate the Jin10 token already stored in api_keys.local.json",
    )
    activate_local_parser.add_argument(
        "--revocation-attestation",
        required=True,
        type=Path,
        help="exact verified artifact proving the previously exposed token was revoked",
    )

    status_parser = subparsers.add_parser(
        "secret-status", help="show configuration and attested rotation status without values"
    )
    status_parser.add_argument("--name", required=True, choices=sorted(ALLOWED_SECRET_NAMES))
    status_parser.add_argument("--evidence-dir", required=True, type=Path)
    status_parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "attest-revocation":
            return _attest_revocation(args)
        if args.command == "verify-revocation":
            return _verify_revocation(args)

        config = OptionsCopilotConfig.from_env()
        config.ensure_runtime_directories()
        if args.command == "activate-local-jin10":
            return _activate_local_jin10(
                args,
                LocalApiKeyStore(local_api_key_path(config.data_dir)),
                DPAPISecretStore(config.secrets_path),
            )
        store = DPAPISecretStore(config.secrets_path)
        if args.command == "list":
            for name in store.names():
                print(name)
            return 0
        if args.command == "delete":
            removed = store.delete(args.name)
            print(f"{args.name}: {'deleted' if removed else 'not configured'}")
            return 0
        if args.command == "secret-status":
            return _secret_status(args, store)
        if args.command == "set":
            return _set_secret(args, store)
        raise SystemExit("unsupported security command")
    except (
        Jin10CredentialError,
        LocalApiKeyFileError,
        LocalJin10BindingError,
        RevocationError,
    ) as exc:
        raise SystemExit(str(exc)) from exc


def _attest_revocation(args: argparse.Namespace) -> int:
    if args.old_token_revoked is not True:
        raise SystemExit("--old-token-revoked is required")
    attestation = RevocationAttestation.create(name=args.name, actor=args.actor)
    path = write_revocation_attestation(attestation, args.evidence_dir)
    payload = {"ok": True, "path": str(path), **attestation.to_dict()}
    if args.json:
        _print_json(payload)
    else:
        print(f"revocation attestation created: {path}")
        print(f"canonical_hash: {attestation.canonical_hash}")
    return 0


def _verify_revocation(args: argparse.Namespace) -> int:
    path = args.path.resolve()
    attestation = load_revocation_attestation(path)
    payload = {"ok": True, "path": str(path), **attestation.to_dict()}
    if args.json:
        _print_json(payload)
    else:
        print(f"valid revocation attestation: {path}")
        print(f"canonical_hash: {attestation.canonical_hash}")
    return 0


def _set_secret(args: argparse.Namespace, store: DPAPISecretStore) -> int:
    attestation_path: Path | None = args.revocation_attestation
    if args.name == JIN10_SECRET_NAME:
        if attestation_path is None:
            raise SystemExit(
                "JIN10_MCP_TOKEN refuses ordinary set; provide --revocation-attestation"
            )
        exact_path = attestation_path.resolve()
        attestation = load_revocation_attestation(
            exact_path, expected_name=JIN10_SECRET_NAME
        )
        with reserve_rotation(attestation, exact_path.parent) as reservation:
            value, confirmation = _hidden_double_input(args.name)
            # Close the prompt-time TOCTOU window: the exact file must still
            # verify and bind the identical canonical hash immediately before DPAPI.
            current = load_revocation_attestation(
                exact_path, expected_name=JIN10_SECRET_NAME
            )
            if current.canonical_hash != attestation.canonical_hash:
                raise SystemExit("revocation attestation changed during secret entry")
            _require_matching_values(value, confirmation)
            encoded, generation = encode_jin10_credential(value)
            store.set(args.name, encoded)
            activation_path = activate_jin10_credential(
                exact_path.parent,
                attestation_hash=attestation.canonical_hash,
                credential_generation=generation,
            )
            manifest_path = reservation.commit()
        print(f"{args.name}: encrypted with Windows DPAPI")
        print(f"rotation manifest: {manifest_path}")
        print(f"activation manifest: {activation_path}")
        print(f"attestation_hash: {attestation.canonical_hash}")
        return 0

    if attestation_path is not None:
        raise SystemExit(
            "--revocation-attestation is accepted only for JIN10_MCP_TOKEN"
        )
    value, confirmation = _hidden_double_input(args.name)
    _require_matching_values(value, confirmation)
    store.set(args.name, value)
    print(f"{args.name}: encrypted with Windows DPAPI")
    return 0


def _activate_local_jin10(
    args: argparse.Namespace,
    store: LocalApiKeyStore,
    binding_store: DPAPISecretStore,
) -> int:
    """Bind one replacement token from the local JSON to one revocation proof."""

    exact_path = args.revocation_attestation.resolve()
    attestation = load_revocation_attestation(
        exact_path,
        expected_name=JIN10_SECRET_NAME,
    )
    reader = LocalJin10EnvelopeReader(store, binding_store)
    with reserve_rotation(attestation, exact_path.parent) as reservation:
        current = load_revocation_attestation(
            exact_path,
            expected_name=JIN10_SECRET_NAME,
        )
        if current.canonical_hash != attestation.canonical_hash:
            raise SystemExit("revocation attestation changed during activation")
        generation = reader.create_opaque_binding()
        if reader.credential_generation() != generation:
            raise SystemExit("local Jin10 credential changed during activation")
        activation_path = activate_jin10_credential(
            exact_path.parent,
            attestation_hash=attestation.canonical_hash,
            credential_generation=generation,
        )
        manifest_path = reservation.commit()
    print("JIN10_MCP_TOKEN: activated from local API key file")
    print(f"rotation manifest: {manifest_path}")
    print(f"activation manifest: {activation_path}")
    print(f"attestation_hash: {attestation.canonical_hash}")
    return 0


def _secret_status(args: argparse.Namespace, store: DPAPISecretStore) -> int:
    rotation = (
        latest_rotation_manifest(args.evidence_dir, expected_name=args.name)
        if args.name == JIN10_SECRET_NAME
        else None
    )
    payload: dict[str, object] = {
        "name": args.name,
        "configured": store.contains(args.name),
        "backend": "WINDOWS_DPAPI",
        "observed_at": datetime_text(datetime.now(timezone.utc)),
        "rotation_attested": rotation is not None,
        "attestation_hash": rotation.attestation_hash if rotation else None,
        "rotated_at": datetime_text(rotation.rotated_at) if rotation else None,
        "activation_status": (
            resolve_jin10_credential(store, args.evidence_dir).status
            if args.name == JIN10_SECRET_NAME
            else "NOT_REQUIRED"
        ),
    }
    if args.json:
        _print_json(payload)
    else:
        print(f"{args.name}: {'configured' if payload['configured'] else 'not configured'}")
        print(f"backend: {payload['backend']}")
        print(f"rotation_attested: {str(payload['rotation_attested']).lower()}")
        print(f"activation_status: {payload['activation_status']}")
        if rotation is not None:
            print(f"attestation_hash: {rotation.attestation_hash}")
            print(f"rotated_at: {payload['rotated_at']}")
    return 0


def _hidden_double_input(name: str) -> tuple[str, str]:
    return (
        getpass.getpass(f"Enter {name} (input hidden): "),
        getpass.getpass("Enter it again: "),
    )


def _require_matching_values(value: str, confirmation: str) -> None:
    if value != confirmation:
        raise SystemExit("Secret values did not match")
    if not value:
        raise SystemExit("Secret value must not be empty")


def _print_json(value: dict[str, object]) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
