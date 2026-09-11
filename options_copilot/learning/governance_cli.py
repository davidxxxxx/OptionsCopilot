"""Interactive human-only P9 governance signing and verification CLI.

There is intentionally no non-interactive confirmation flag, environment
bypass, API/UI signer, or callable exported signing helper.  Signing always
displays the canonical document hash and requires a human to type the exact
one-time confirmation.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import sys
from typing import TextIO

from options_copilot.storage.canonical import canonical_hash, canonical_json

from .markers import (
    AGradeAuthority,
    AuthoritySignatureVerifier,
    AuthorityValidationError,
    NO_TRUSTED_HUMAN_SIGNER,
    PromotionAuthority,
    RollbackAuthority,
    verify_a_grade_authority,
    verify_promotion_authority,
    verify_rollback_authority,
)
from .policy_authority import (
    AuthorityConflict,
    PolicyAuthorityLedger,
    PolicyAuthorityTampered,
)


EXIT_OK = 0
EXIT_USAGE = 2
EXIT_NOT_FOUND = 3
EXIT_INVALID = 4
EXIT_EXISTS = 5
EXIT_IO = 6
DEFAULT_INITIAL_POLICY = (
    Path(__file__).parents[1]
    / "governance"
    / "initial_champion_scenario_policy.v1.json"
)


class InteractiveHumanRequired(AuthorityValidationError):
    pass


class TrustedHumanSignerUnavailable(AuthorityValidationError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.learning.governance_cli",
        description="Explicit human P9 policy and proposal-risk governance",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("sign-promotion", "sign-a-grade", "sign-rollback"):
        sub = commands.add_parser(command)
        sub.add_argument("--draft", required=True, type=Path)
        sub.add_argument("--authority-db", required=True, type=Path)
        sub.add_argument(
            "--initial-policy",
            type=Path,
            default=DEFAULT_INITIAL_POLICY,
        )
        sub.add_argument("--output", required=True, type=Path)
        if command == "sign-promotion":
            sub.add_argument("--policy-payload", required=True, type=Path)
        sub.add_argument("--json", action="store_true")
    for command in (
        "verify-promotion-decision",
        "verify-a-grade-decision",
        "verify-rollback-decision",
    ):
        sub = commands.add_parser(command)
        sub.add_argument("--path", required=True, type=Path)
        sub.add_argument("--authority-db", required=True, type=Path)
        sub.add_argument(
            "--initial-policy",
            type=Path,
            default=DEFAULT_INITIAL_POLICY,
        )
        sub.add_argument("--json", action="store_true")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    signature_verifier: AuthoritySignatureVerifier | None = None,
) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    input_stream = stdin or sys.stdin
    output_stream = stdout or sys.stdout
    error_stream = stderr or sys.stderr
    try:
        if args.command.startswith("sign-"):
            response = _sign_interactively(
                args,
                input_stream,
                output_stream,
                signature_verifier=signature_verifier,
            )
        else:
            response = _verify_decision(
                args,
                signature_verifier=signature_verifier,
            )
        _emit(response, output_stream, json_output=bool(args.json))
        return EXIT_OK
    except FileNotFoundError as exc:
        return _fail(EXIT_NOT_FOUND, "AUTHORITY_INPUT_NOT_FOUND", str(exc), output_stream, error_stream, bool(args.json))
    except FileExistsError as exc:
        return _fail(EXIT_EXISTS, "AUTHORITY_OUTPUT_EXISTS", str(exc), output_stream, error_stream, bool(args.json))
    except InteractiveHumanRequired as exc:
        return _fail(EXIT_INVALID, "INTERACTIVE_TTY_REQUIRED", str(exc), output_stream, error_stream, bool(args.json))
    except TrustedHumanSignerUnavailable as exc:
        return _fail(EXIT_INVALID, NO_TRUSTED_HUMAN_SIGNER, str(exc), output_stream, error_stream, bool(args.json))
    except AuthorityValidationError as exc:
        error_code = (
            NO_TRUSTED_HUMAN_SIGNER
            if NO_TRUSTED_HUMAN_SIGNER in str(exc)
            else "AUTHORITY_INVALID"
        )
        return _fail(EXIT_INVALID, error_code, str(exc), output_stream, error_stream, bool(args.json))
    except (AuthorityConflict, PolicyAuthorityTampered, TypeError, ValueError) as exc:
        return _fail(EXIT_INVALID, "AUTHORITY_INVALID", str(exc), output_stream, error_stream, bool(args.json))
    except OSError as exc:
        return _fail(EXIT_IO, "AUTHORITY_IO_ERROR", str(exc), output_stream, error_stream, bool(args.json))


def _sign_interactively(
    args: argparse.Namespace,
    stdin: TextIO,
    stdout: TextIO,
    *,
    signature_verifier: AuthoritySignatureVerifier | None,
) -> dict[str, object]:
    if not _is_tty(stdin) or not _is_tty(stdout):
        raise InteractiveHumanRequired(
            "signing requires a real interactive input and output terminal"
        )
    if args.output.exists():
        raise FileExistsError(f"authority output already exists: {args.output}")
    draft = _read_object(args.draft)
    forbidden = {"governance_signature", "content_hash"}.intersection(draft)
    if forbidden:
        raise AuthorityValidationError(
            f"unsigned draft contains prohibited field {sorted(forbidden)[0]}"
        )
    actor = draft.get("actor")
    if not isinstance(actor, str) or not actor.startswith("human:"):
        raise AuthorityValidationError("signing requires an explicit human:* actor")
    with PolicyAuthorityLedger(
        args.authority_db,
        initial_policy_path=args.initial_policy,
        signature_verifier=signature_verifier,
    ) as ledger:
        head = ledger.current_state()
    challenge = {
        "schema": "options_copilot.learning.human_signing_challenge.v1",
        "command": args.command,
        "actor": actor,
        "draft_hash": canonical_hash(draft),
        "authority_sequence": head.sequence,
        "authority_head_hash": head.authority_head_hash,
        "current_policy_version": head.policy_version,
        "current_policy_hash": head.policy_hash,
        "policy_authority_marker_hash": head.policy_marker_hash,
    }
    challenge_hash = canonical_hash(challenge)
    stdout.write("P9 HUMAN GOVERNANCE REVIEW\n")
    stdout.write(canonical_json(challenge) + "\n")
    stdout.write(f"CHALLENGE_HASH {challenge_hash}\n")
    stdout.write(f"STATUS {NO_TRUSTED_HUMAN_SIGNER}\n")
    stdout.flush()
    raise TrustedHumanSignerUnavailable(
        "no independently approved human signing credential is configured; "
        f"challenge {challenge_hash} was not signed and the ledger was not changed"
    )


def _verify_decision(
    args: argparse.Namespace,
    *,
    signature_verifier: AuthoritySignatureVerifier | None,
) -> dict[str, object]:
    document = _read_object(args.path)
    marker = _parse_for_command(
        args.command,
        document,
        signature_verifier=signature_verifier,
    )
    with PolicyAuthorityLedger(
        args.authority_db,
        initial_policy_path=args.initial_policy,
        signature_verifier=signature_verifier,
    ) as ledger:
        ledger.verify_integrity()
        row = ledger.connection.execute(
            "SELECT sequence, kind FROM authority_events WHERE content_hash=?",
            (marker.content_hash,),
        ).fetchone()
        if row is None:
            raise AuthorityValidationError("verified artifact is not present in the authority ledger")
        active = (
            ledger.is_a_grade_active(marker.content_hash)
            if isinstance(marker, AGradeAuthority)
            else None
        )
        state = ledger.current_state()
    return {
        "ok": True,
        "command": args.command,
        "path": str(args.path),
        "actor": marker.actor,
        "signed_at": marker.signed_at.isoformat(),
        "sequence": int(row["sequence"]),
        "content_hash": marker.content_hash,
        "authority_head_hash": state.authority_head_hash,
        "current_policy_version": state.policy_version,
        "current_policy_hash": state.policy_hash,
        "policy_authority_marker_hash": state.policy_marker_hash,
        "a_grade_active": active,
    }


def _parse_for_command(
    command: str,
    value: Mapping[str, object],
    *,
    signature_verifier: AuthoritySignatureVerifier | None,
) -> PromotionAuthority | AGradeAuthority | RollbackAuthority:
    if command in {"sign-promotion", "verify-promotion-decision"}:
        return verify_promotion_authority(
            value,
            signature_verifier=signature_verifier,
        )
    if command in {"sign-a-grade", "verify-a-grade-decision"}:
        return verify_a_grade_authority(
            value,
            signature_verifier=signature_verifier,
        )
    if command in {"sign-rollback", "verify-rollback-decision"}:
        return verify_rollback_authority(
            value,
            signature_verifier=signature_verifier,
        )
    raise ValueError("unknown governance command")


def _read_object(path: Path) -> Mapping[str, object]:
    raw = path.read_text(encoding="utf-8")
    try:
        value = json.loads(raw, parse_constant=_reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise AuthorityValidationError(f"invalid JSON at {path}: {exc}") from exc
    if not isinstance(value, Mapping):
        raise AuthorityValidationError(f"JSON at {path} must be an object")
    return value


def _write_exclusive(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    with path.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(rendered)
        stream.flush()


def _emit(value: Mapping[str, object], stream: TextIO, *, json_output: bool) -> None:
    if json_output:
        stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")
    else:
        stream.write(f"VALID {value['command']} {value['content_hash']}\n")
    stream.flush()


def _fail(code: int, error_code: str, message: str, stdout: TextIO, stderr: TextIO, json_output: bool) -> int:
    if json_output:
        stdout.write(json.dumps({"ok": False, "error": {"code": error_code, "message": message}}, sort_keys=True, separators=(",", ":")) + "\n")
        stdout.flush()
    else:
        stderr.write(f"{error_code}: {message}\n")
        stderr.flush()
    return code


def _reject_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number {value!r} is prohibited")


def _is_tty(stream: TextIO) -> bool:
    checker = getattr(stream, "isatty", None)
    try:
        return bool(checker()) if callable(checker) else False
    except OSError:
        return False


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "EXIT_EXISTS", "EXIT_INVALID", "EXIT_IO", "EXIT_NOT_FOUND", "EXIT_OK",
    "EXIT_USAGE", "DEFAULT_INITIAL_POLICY", "InteractiveHumanRequired",
    "TrustedHumanSignerUnavailable", "build_parser", "main",
]
