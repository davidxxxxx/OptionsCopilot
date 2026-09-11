"""Local metadata-only discovery CLI for creator transport capabilities.

No Connector call is made here.  The local Python process has no managed
authentication and no browser or order authority.  If the host runtime does
not inject a sanitized operation inventory through the Python API, discovery
therefore reports ``CREATOR_TRANSPORT_UNAVAILABLE``.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from uuid import UUID, uuid4

import options_copilot.bridge.creator_transport as creator_transport
from options_copilot.bridge.creator_transport import (
    CreatorTransportValidationError,
    build_capability_probe_evidence,
    validate_creator_transport_contract,
)


EVIDENCE_FILENAME = "creator_transport_discovery.v1.json"
INSTALLED_CAPABILITY_FILENAME = "installed_capability_evidence.v2.json"


def discover_installed_operations() -> tuple[Mapping[str, object], ...]:
    """Return sanitized runtime-injected metadata, or fail closed locally.

    Managed Connector inventories and authentication are runtime-owned and are
    not exposed to this application process.  This deliberately empty local
    provider prevents config scraping, credential discovery, or an implicit
    browser fallback.  A managed host may call :func:`run_discovery` with its
    already-sanitized metadata descriptors.
    """

    return ()


def run_discovery(
    *,
    evidence_dir: str | Path,
    operations: Iterable[Mapping[str, object]] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Validate metadata and exclusively create one immutable attempt artifact."""

    inventory = (
        discover_installed_operations() if operations is None else tuple(operations)
    )
    now = (clock or (lambda: datetime.now(timezone.utc)))()
    evidence = build_capability_probe_evidence(inventory, observed_at=now)
    _persist_immutable_evidence(
        evidence,
        evidence_dir=evidence_dir,
        filename=EVIDENCE_FILENAME,
        observed_at=now,
    )
    return evidence


def run_installed_capability_capture(
    *,
    evidence_dir: str | Path,
    clock: Callable[[], datetime] | None = None,
    uuid_factory: Callable[[], UUID] = uuid4,
) -> dict[str, object]:
    """Capture only canonical host sources without invoking their Connector."""

    evidence = creator_transport._build_installed_capability_evidence()
    observed_at = datetime.fromisoformat(str(evidence["observed_at"]).replace("Z", "+00:00"))
    persistence_time = (clock or (lambda: observed_at))()
    _persist_immutable_evidence(
        evidence,
        evidence_dir=evidence_dir,
        filename=INSTALLED_CAPABILITY_FILENAME,
        observed_at=persistence_time,
        uuid_factory=uuid_factory,
    )
    return evidence


def _persist_immutable_evidence(
    evidence: Mapping[str, object],
    *,
    evidence_dir: str | Path,
    filename: str,
    observed_at: datetime,
    uuid_factory: Callable[[], UUID] = uuid4,
) -> Path:
    directory = Path(evidence_dir)
    directory.mkdir(parents=True, exist_ok=True)
    attempt_name = (
        observed_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        + "-"
        + uuid_factory().hex
    )
    attempt_directory = directory / attempt_name
    try:
        attempt_directory.mkdir(exist_ok=False)
        destination = attempt_directory / filename
        payload = json.dumps(
            evidence,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
        with destination.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise CreatorTransportValidationError(
            "immutable evidence persistence collision"
        ) from exc
    return destination


def verify_contract(
    path: str | Path,
    *,
    capability_evidence_path: str | Path,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Validate one local contract against fresh capability evidence."""

    contract_path = Path(path)
    try:
        decoded = _read_json_object(contract_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise CreatorTransportValidationError(
            "contract file is unavailable or is not valid JSON"
        ) from exc
    evidence_path = Path(capability_evidence_path)
    try:
        capability_evidence = _read_json_object(evidence_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise CreatorTransportValidationError(
            "capability evidence file is unavailable or is not valid JSON"
        ) from exc
    checked_at = (clock or (lambda: datetime.now(timezone.utc)))()
    validated = validate_creator_transport_contract(
        decoded,
        capability_evidence=capability_evidence,
        checked_at=checked_at,
    )
    return {
        **validated,
        "instruction_created": False,
        "connector_invoked": False,
    }


def _read_json_object(path: Path) -> dict[str, object]:
    def reject_duplicate_keys(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant {value!r}")

    decoded = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )
    if not isinstance(decoded, dict):
        raise ValueError("creator metadata document must be a JSON object")
    return decoded


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover or validate a review-only creator transport"
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--discover",
        action="store_true",
        help="perform a local metadata-only capability discovery",
    )
    action.add_argument(
        "--verify-contract",
        metavar="PATH",
        help="structurally inspect an unsigned contract candidate without invoking it",
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        help="directory for redacted discovery evidence",
    )
    parser.add_argument(
        "--capability-evidence",
        type=Path,
        help="fresh discovery evidence required with --verify-contract",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.discover:
            if args.evidence_dir is None:
                raise CreatorTransportValidationError(
                    "--evidence-dir is required with --discover"
                )
            result = run_discovery(evidence_dir=args.evidence_dir)
        else:
            if args.capability_evidence is None:
                raise CreatorTransportValidationError(
                    "--capability-evidence is required with --verify-contract"
                )
            result = verify_contract(
                args.verify_contract,
                capability_evidence_path=args.capability_evidence,
            )
    except CreatorTransportValidationError as exc:
        result = {
            "status": "CREATOR_TRANSPORT_INVALID",
            "reason": str(exc),
            "instruction_created": False,
            "connector_invoked": False,
        }
        if args.json:
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        else:
            print(f"{result['status']}: {result['reason']}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(str(result["status"]))
        for operation in result.get("supported_operations", []):
            if isinstance(operation, Mapping):
                print(f"{operation['connector_id']}::{operation['tool_name']}")
    if result.get("reason") == "WAITING_GENUINE_HUMAN_SIGNATURE":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EVIDENCE_FILENAME",
    "INSTALLED_CAPABILITY_FILENAME",
    "discover_installed_operations",
    "main",
    "run_discovery",
    "run_installed_capability_capture",
    "verify_contract",
]
