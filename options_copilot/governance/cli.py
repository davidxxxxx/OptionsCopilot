"""Secret-free CLI for signing and verifying governance contract artifacts."""
from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import sys
from typing import TextIO

from .contracts import (
    ContractKind,
    ContractValidationError,
    ContractWriteError,
    SignedContract,
    create_correction,
    load_contract,
    sign_contract,
    verify_contract,
    verify_correction,
    write_contract,
)


EXIT_OK = 0
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_INVALID = 4
EXIT_EXISTS = 5
EXIT_IO = 6


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.governance.cli",
        description="Sign and verify immutable Options Copilot governance contracts",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    verify = commands.add_parser(
        "verify-contract",
        help="verify the canonical hash and optional consumer bindings",
    )
    verify.add_argument("--path", required=True, type=Path)
    verify.add_argument("--expect-kind")
    verify.add_argument("--expect-version")
    verify.add_argument("--expect-hash")
    verify.add_argument("--expect-signer")
    verify.add_argument("--expect-effective-at")
    verify.add_argument("--as-of")
    verify.add_argument(
        "--supersedes-path",
        type=Path,
        help="also prove this correction directly supersedes the given contract",
    )
    verify.add_argument("--json", action="store_true")

    generic = commands.add_parser(
        "sign-contract",
        help="sign a draft for one explicitly selected contract kind",
    )
    generic.add_argument("--kind", required=True)
    _add_sign_arguments(generic)

    for command, kind, help_text in (
        (
            "sign-strategy-nav",
            ContractKind.STRATEGY_NAV,
            "sign a Strategy NAV cash-flow contract draft",
        ),
        (
            "sign-execution-cost",
            ContractKind.EXECUTION_COST,
            "sign an Execution Cost contract draft",
        ),
        (
            "sign-initial-policy",
            ContractKind.INITIAL_CHAMPION_SCENARIO_POLICY,
            "sign an Initial Champion and Scenario Policy draft",
        ),
    ):
        signer = commands.add_parser(command, help=help_text)
        signer.set_defaults(kind=kind.value)
        _add_sign_arguments(signer)

    correction = commands.add_parser(
        "correct-contract",
        help="create a new version that hash-links to an existing contract",
    )
    correction.add_argument("--previous", required=True, type=Path)
    correction.add_argument("--draft", required=True, type=Path)
    correction.add_argument("--output", required=True, type=Path)
    correction.add_argument("--actor", required=True)
    correction.add_argument("--signed-at", required=True)
    correction.add_argument("--json", action="store_true")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    json_output = bool(getattr(args, "json", False))

    try:
        if args.command == "verify-contract":
            contract = _verify(args)
            response = {"ok": True, "contract": _summary(contract, path=args.path)}
        else:
            contract = _sign(args)
            response = {
                "ok": True,
                "contract": _summary(contract, path=args.output),
            }
        _emit(response, output_stream, json_output=json_output)
        return EXIT_OK
    except FileNotFoundError as exc:
        path = exc.filename or str(exc)
        return _failure(
            code=EXIT_NOT_FOUND,
            error_code="CONTRACT_NOT_FOUND",
            message=f"contract input does not exist: {path}",
            stdout=output_stream,
            stderr=error_stream,
            json_output=json_output,
        )
    except ContractWriteError as exc:
        message = str(exc)
        code = EXIT_EXISTS if "already exists" in message or "overwrite" in message else EXIT_IO
        error_code = "CONTRACT_ALREADY_EXISTS" if code == EXIT_EXISTS else "CONTRACT_IO_ERROR"
        return _failure(
            code=code,
            error_code=error_code,
            message=message,
            stdout=output_stream,
            stderr=error_stream,
            json_output=json_output,
        )
    except (ContractValidationError, TypeError, ValueError) as exc:
        return _failure(
            code=EXIT_INVALID,
            error_code="INVALID_CONTRACT",
            message=str(exc),
            stdout=output_stream,
            stderr=error_stream,
            json_output=json_output,
        )
    except OSError as exc:
        return _failure(
            code=EXIT_IO,
            error_code="CONTRACT_IO_ERROR",
            message=str(exc),
            stdout=output_stream,
            stderr=error_stream,
            json_output=json_output,
        )


def _add_sign_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--draft",
        required=True,
        type=Path,
        help="JSON containing only version, effective_at, provenance, and payload",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--signed-at", required=True)
    parser.add_argument(
        "--previous",
        type=Path,
        help="create a correction linked to this prior artifact",
    )
    parser.add_argument("--json", action="store_true")


def _verify(args: argparse.Namespace) -> SignedContract:
    contract = load_contract(
        args.path,
        expected_kind=args.expect_kind,
        expected_version=args.expect_version,
        expected_hash=args.expect_hash,
        expected_signer=args.expect_signer,
        expected_effective_at=args.expect_effective_at,
        as_of=args.as_of,
    )
    if args.supersedes_path is not None:
        prior = load_contract(args.supersedes_path)
        contract = verify_correction(prior, contract)
    return contract


def _sign(args: argparse.Namespace) -> SignedContract:
    draft = _read_draft(args.draft)
    required = {"version", "effective_at", "provenance", "payload"}
    missing = sorted(required.difference(draft))
    if missing:
        raise ContractValidationError(
            f"draft is missing required field {missing[0]}"
        )
    unknown = sorted(set(draft).difference(required))
    if unknown:
        raise ContractValidationError(
            f"draft contains prohibited or unknown field {unknown[0]}"
        )

    previous_path = getattr(args, "previous", None)
    if args.command == "correct-contract":
        previous_path = args.previous
        prior = load_contract(previous_path)
        contract = create_correction(
            prior,
            version=draft["version"],
            effective_at=draft["effective_at"],
            provenance=_required_mapping(draft, "provenance"),
            payload=_required_mapping(draft, "payload"),
            actor=args.actor,
            signed_at=args.signed_at,
        )
    elif previous_path is not None:
        prior = load_contract(previous_path)
        requested_kind = ContractKind.parse(args.kind)
        if prior.contract_kind is not requested_kind:
            raise ContractValidationError(
                "signing command kind does not match the previous contract kind"
            )
        contract = create_correction(
            prior,
            version=draft["version"],
            effective_at=draft["effective_at"],
            provenance=_required_mapping(draft, "provenance"),
            payload=_required_mapping(draft, "payload"),
            actor=args.actor,
            signed_at=args.signed_at,
        )
    else:
        contract = sign_contract(
            kind=args.kind,
            version=draft["version"],
            effective_at=draft["effective_at"],
            provenance=_required_mapping(draft, "provenance"),
            payload=_required_mapping(draft, "payload"),
            actor=args.actor,
            signed_at=args.signed_at,
        )
    write_contract(contract, args.output)
    return contract


def _read_draft(path: Path) -> Mapping[str, object]:
    raw = path.read_text(encoding="utf-8")
    try:
        value = json.loads(raw, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ContractValidationError(f"invalid draft JSON at {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ContractValidationError("contract draft must be a JSON object")
    return value


def _required_mapping(value: Mapping[str, object], field: str) -> Mapping[str, object]:
    item = value[field]
    if not isinstance(item, Mapping):
        raise ContractValidationError(f"draft {field} must be a JSON object")
    return item


def _summary(contract: SignedContract, *, path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "contract_kind": contract.contract_kind.value,
        "version": contract.version,
        "contract_hash": contract.contract_hash,
        "actor": contract.actor,
        "signed_at": contract.signed_at.isoformat(timespec="microseconds"),
        "effective_at": contract.effective_at.isoformat(timespec="microseconds"),
        "supersedes_version": contract.supersedes_version,
        "supersedes_hash": contract.supersedes_hash,
    }


def _failure(
    *,
    code: int,
    error_code: str,
    message: str,
    stdout: TextIO,
    stderr: TextIO,
    json_output: bool,
) -> int:
    response = {
        "ok": False,
        "error": {"code": error_code, "message": message},
    }
    if json_output:
        _emit(response, stdout, json_output=True)
    else:
        stderr.write(f"{error_code}: {message}\n")
        stderr.flush()
    return code


def _emit(value: Mapping[str, object], stream: TextIO, *, json_output: bool) -> None:
    if json_output:
        stream.write(
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
    else:
        contract = value["contract"]
        assert isinstance(contract, Mapping)
        stream.write(
            f"VALID {contract['contract_kind']} {contract['version']} "
            f"{contract['contract_hash']}\n"
        )
    stream.flush()


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number {value!r} is prohibited")


if __name__ == "__main__":  # pragma: no cover - exercised via python -m
    raise SystemExit(main())


__all__ = [
    "EXIT_EXISTS",
    "EXIT_INVALID",
    "EXIT_IO",
    "EXIT_NOT_FOUND",
    "EXIT_OK",
    "EXIT_USAGE",
    "build_parser",
    "main",
]
