"""Validate one exclusive, canonical Phase 2 provider checkpoint."""
from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any


_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from options_copilot.storage.canonical import (  # noqa: E402
    canonical_hash,
    canonical_json,
)


EXPECTED_SOURCE_IDS = (
    "sec",
    "nasdaq",
    "company_ir",
    "finnhub",
    "alpha_vantage",
    "jin10",
)
_EXPECTED_METHODS = {
    "sec": ("GET",),
    "nasdaq": ("GET",),
    "company_ir": ("GET",),
    "finnhub": ("GET",),
    "alpha_vantage": ("GET",),
    "jin10": ("POST", "DELETE"),
}
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema",
        "generated_at",
        "status",
        "decision",
        "decision_authority",
        "read_only",
        "instruction_creation_allowed",
        "approval_eligible",
        "order_submission_allowed",
        "contract",
        "sources",
        "conflicts",
        "redaction",
        "canonical_sha256",
    }
)
_SOURCE_FIELDS = frozenset(
    {
        "source_id",
        "configured",
        "readiness",
        "status",
        "reason",
        "observed_at",
        "as_of",
        "last_success_at",
        "freshness_age_seconds",
        "provenance",
        "pacing",
        "request_bytes",
        "response_bytes",
        "event_count",
        "active_count",
        "conflicted_count",
        "request_methods",
        "transport_verified",
        "read_only",
        "decision_authority",
    }
)
_SOURCE_STATES = frozenset(
    {"READY", "DEGRADED", "UNAVAILABLE", "UNCONFIGURED", "LIMITED", "STALE"}
)
_SAFE_REASON_VALUES = frozenset(
    {
        "BAD_JSON",
        "CREDENTIAL_NOT_ACTIVATED",
        "NOT_CONFIGURED",
        "NOT_OBSERVED",
        "PACING_LIMITED",
        "PACING_UNVERIFIED",
        "PROBE_FAILED",
        "RATE_LIMITED",
        "REQUEST_FAILED",
        "REQUEST_TIMEOUT",
        "TRANSPORT_UNVERIFIED",
        "UNCONFIGURED",
    }
)
_SAFE_AUTHORITY_KEYS = frozenset(
    {
        "approval_eligible",
        "instruction_creation_allowed",
        "order_submission_allowed",
    }
)
_FORBIDDEN_KEY = re.compile(
    r"(?:authorization|cookie|headers?|password|secret|token|api[_-]?key|"
    r"credential|account|broker|position|creator|raw[_-]?(?:body|error)|"
    r"exception|redirect|(?:^|_)url(?:$|_)|(?:^|_)path(?:$|_)|query|tool|"
    r"order|instruction)",
    re.IGNORECASE,
)
_FORBIDDEN_VALUE = re.compile(
    r"(?:bearer\s|apikey\s*[:=]|api[_-]?key\s*[:=]|x-finnhub-token|"
    r"authorization\s*:|https?://[^\s?]+\?|[a-z]:[\\/]|"
    r"sentinel[^\s]*(?:secret|private)|raw[\s_-]*(?:body|error)|"
    r"redirect[\s_-]*target|\b(?:account|broker|position|creator|order|"
    r"instruction|exception|tool|password|secret|token|credential|query)\b)",
    re.IGNORECASE,
)
_CHECKPOINT_NAME = re.compile(r"provider_probe_checkpoint\.([0-9a-f]{64})\.json")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MAX_CHECKPOINT_BYTES = 4 * 1024 * 1024


class CheckpointValidationError(RuntimeError):
    """A live checkpoint failed a bounded safety or integrity contract."""


@dataclass(frozen=True, slots=True)
class _JsonIdentity:
    device: int
    inode: int
    size: int
    modified_ns: int
    sha256: str


def _repository_root() -> Path:
    return _REPOSITORY_ROOT


def _require_within(path: Path, root: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError:
        raise CheckpointValidationError("checkpoint path escapes its root") from None


def _assert_no_reparse_ancestors(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError:
        raise CheckpointValidationError("checkpoint path escapes its root") from None
    candidates = [root]
    cursor = root
    for part in relative.parts:
        cursor /= part
        candidates.append(cursor)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    for candidate in candidates:
        try:
            attributes = os.lstat(candidate)
        except FileNotFoundError:
            continue
        except OSError:
            raise CheckpointValidationError(
                "checkpoint path inspection failed"
            ) from None
        if (
            stat.S_ISLNK(attributes.st_mode)
            or getattr(attributes, "st_file_attributes", 0) & reparse_flag
        ):
            raise CheckpointValidationError(
                "checkpoint path contains a reparse point"
            )


def _resolved_live_directory(evidence_dir: str | Path) -> Path:
    repository_root = _repository_root()
    live_root = (
        repository_root
        / "data"
        / "options_copilot"
        / "evidence"
        / "phase-02"
        / "live"
    )
    raw = Path(evidence_dir)
    if any(part in {".", ".."} for part in raw.parts):
        raise CheckpointValidationError("checkpoint directory is invalid")
    lexical = Path(
        os.path.abspath(raw if raw.is_absolute() else repository_root / raw)
    )
    for candidate in (repository_root, live_root, lexical):
        if candidate.drive.upper() != "G:":
            raise CheckpointValidationError("checkpoint evidence must remain on G drive")
    _require_within(lexical, live_root)
    _assert_no_reparse_ancestors(repository_root, lexical)
    resolved_root = live_root.resolve(strict=False)
    resolved = lexical.resolve(strict=False)
    _require_within(resolved, resolved_root)
    if lexical.exists() and not lexical.is_dir():
        raise CheckpointValidationError("checkpoint directory is invalid")
    return lexical


def _file_digest(path: Path) -> str:
    try:
        size = path.stat().st_size
        if size > _MAX_CHECKPOINT_BYTES:
            raise CheckpointValidationError("checkpoint exceeds the size bound")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(64 * 1024):
                digest.update(chunk)
        return digest.hexdigest()
    except CheckpointValidationError:
        raise
    except OSError:
        raise CheckpointValidationError("checkpoint identity read failed") from None


def snapshot_json_identities(evidence_dir: str | Path) -> dict[Path, _JsonIdentity]:
    """Snapshot direct JSON paths and immutable identity/content attributes."""

    directory = _resolved_live_directory(evidence_dir)
    if not directory.exists():
        return {}
    result: dict[Path, _JsonIdentity] = {}
    try:
        paths = sorted(directory.glob("*.json"), key=lambda item: item.name)
    except OSError:
        raise CheckpointValidationError("checkpoint directory scan failed") from None
    for path in paths:
        _assert_no_reparse_ancestors(directory, path)
        try:
            resolved = path.resolve(strict=True)
            _require_within(resolved, directory.resolve(strict=True))
            metadata = os.lstat(path)
        except (OSError, RuntimeError):
            raise CheckpointValidationError("checkpoint identity read failed") from None
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or getattr(metadata, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        ):
            raise CheckpointValidationError("checkpoint is not a regular file")
        result[resolved] = _JsonIdentity(
            device=metadata.st_dev,
            inode=metadata.st_ino,
            size=metadata.st_size,
            modified_ns=metadata.st_mtime_ns,
            sha256=_file_digest(resolved),
        )
    return result


def _reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CheckpointValidationError("checkpoint contains a duplicate JSON key")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> None:
    del value
    raise CheckpointValidationError("checkpoint contains a non-finite JSON number")


def _load_document(path: Path) -> tuple[dict[str, object], str]:
    try:
        raw = path.read_text(encoding="utf-8")
        document = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except CheckpointValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError):
        raise CheckpointValidationError("checkpoint JSON is invalid") from None
    if not isinstance(document, dict):
        raise CheckpointValidationError("checkpoint JSON root is invalid")
    try:
        rendered = canonical_json(document) + "\n"
    except (TypeError, ValueError, RecursionError):
        raise CheckpointValidationError("checkpoint canonicalization failed") from None
    if raw != rendered:
        raise CheckpointValidationError("checkpoint bytes are not canonical")
    return document, rendered


def _require_exact_fields(
    value: object,
    fields: frozenset[str],
    *,
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, dict) or frozenset(value) != fields:
        raise CheckpointValidationError(f"checkpoint {label} schema is invalid")
    return value


def _require_bool(value: object, *, label: str, expected: bool | None = None) -> bool:
    if not isinstance(value, bool) or (expected is not None and value is not expected):
        raise CheckpointValidationError(f"checkpoint {label} is invalid")
    return value


def _require_string(value: object, *, label: str, maximum: int = 500) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise CheckpointValidationError(f"checkpoint {label} is invalid")
    return value


def _require_count(value: object, *, label: str, optional: bool = False) -> int | None:
    if optional and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CheckpointValidationError(f"checkpoint {label} is invalid")
    return value


def _require_utc_timestamp(
    value: object,
    *,
    label: str,
    optional: bool = False,
) -> str | None:
    if optional and value is None:
        return None
    checked = _require_string(value, label=label, maximum=64)
    try:
        parsed = datetime.fromisoformat(checked.replace("Z", "+00:00"))
    except ValueError:
        raise CheckpointValidationError(f"checkpoint {label} is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise CheckpointValidationError(f"checkpoint {label} is invalid")
    return checked


def _assert_redacted(value: object, *, path: str = "checkpoint") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise CheckpointValidationError("checkpoint redaction failed")
            if _FORBIDDEN_KEY.search(key):
                if key not in _SAFE_AUTHORITY_KEYS or item is not False:
                    raise CheckpointValidationError("checkpoint redaction failed")
            _assert_redacted(item, path=f"{path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        for index, item in enumerate(value):
            _assert_redacted(item, path=f"{path}[{index}]")
        return
    if isinstance(value, float) and not math.isfinite(value):
        raise CheckpointValidationError("checkpoint redaction failed")
    if isinstance(value, str):
        if len(value) > 500:
            raise CheckpointValidationError("checkpoint redaction failed")
        if path.endswith(".reason") and value in _SAFE_REASON_VALUES:
            return
        normalized = re.sub(r"[_-]+", " ", value)
        if _FORBIDDEN_VALUE.search(normalized):
            raise CheckpointValidationError("checkpoint redaction failed")


def _validate_contract(value: object) -> None:
    contract = _require_exact_fields(
        value,
        frozenset({"provider_names", "requested_symbols", "limit", "read_only"}),
        label="contract",
    )
    if contract["provider_names"] != list(EXPECTED_SOURCE_IDS):
        raise CheckpointValidationError("checkpoint provider contract is invalid")
    if contract["requested_symbols"] != ["SPY"]:
        raise CheckpointValidationError("checkpoint symbol contract is invalid")
    if contract["limit"] != 1 or isinstance(contract["limit"], bool):
        raise CheckpointValidationError("checkpoint limit contract is invalid")
    _require_bool(contract["read_only"], label="contract read-only", expected=True)


def _validate_sources(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list) or len(value) != len(EXPECTED_SOURCE_IDS):
        raise CheckpointValidationError("checkpoint source inventory is invalid")
    states: list[dict[str, str]] = []
    for expected_source_id, item in zip(EXPECTED_SOURCE_IDS, value, strict=True):
        row = _require_exact_fields(item, _SOURCE_FIELDS, label="source row")
        source_id = _require_string(row["source_id"], label="source id", maximum=32)
        if source_id != expected_source_id:
            raise CheckpointValidationError("checkpoint source order is invalid")
        _require_bool(row["configured"], label="configured")
        readiness = _require_string(row["readiness"], label="readiness", maximum=32)
        status_value = _require_string(row["status"], label="status", maximum=32)
        if readiness not in _SOURCE_STATES or status_value != readiness:
            raise CheckpointValidationError("checkpoint source status is invalid")
        reason = row["reason"]
        if reason is not None:
            _require_string(reason, label="reason", maximum=200)
        _require_utc_timestamp(row["observed_at"], label="observed_at")
        _require_utc_timestamp(row["as_of"], label="as_of", optional=True)
        _require_utc_timestamp(
            row["last_success_at"],
            label="last_success_at",
            optional=True,
        )
        freshness = row["freshness_age_seconds"]
        if freshness is not None and (
            isinstance(freshness, bool)
            or not isinstance(freshness, (int, float))
            or not math.isfinite(freshness)
            or freshness < 0
        ):
            raise CheckpointValidationError("checkpoint freshness is invalid")
        provenance = row["provenance"]
        if not isinstance(provenance, list) or any(
            not isinstance(item_value, str) or not item_value or len(item_value) > 500
            for item_value in provenance
        ):
            raise CheckpointValidationError("checkpoint provenance is invalid")
        _require_string(row["pacing"], label="pacing", maximum=64)
        _require_count(row["request_bytes"], label="request_bytes", optional=True)
        _require_count(row["response_bytes"], label="response_bytes", optional=True)
        _require_count(row["event_count"], label="event_count")
        _require_count(row["active_count"], label="active_count")
        _require_count(row["conflicted_count"], label="conflicted_count")
        if row["request_methods"] != list(_EXPECTED_METHODS[source_id]):
            raise CheckpointValidationError("checkpoint request methods are invalid")
        _require_bool(row["transport_verified"], label="transport_verified")
        _require_bool(row["read_only"], label="source read-only", expected=True)
        if row["decision_authority"] != "SUPPORTING_ONLY":
            raise CheckpointValidationError("checkpoint source authority is invalid")
        states.append({"source_id": source_id, "status": status_value})
    return states


def _validate_conflicts(value: object) -> None:
    if not isinstance(value, list) or len(value) > len(EXPECTED_SOURCE_IDS):
        raise CheckpointValidationError("checkpoint conflicts are invalid")
    seen: set[str] = set()
    for item in value:
        row = _require_exact_fields(
            item,
            frozenset({"source_id", "identity_hashes"}),
            label="conflict row",
        )
        source_id = row["source_id"]
        hashes = row["identity_hashes"]
        if (
            source_id not in EXPECTED_SOURCE_IDS
            or source_id in seen
            or not isinstance(hashes, list)
            or not hashes
            or any(not isinstance(item_hash, str) or not _SHA256.fullmatch(item_hash) for item_hash in hashes)
            or len(set(hashes)) != len(hashes)
        ):
            raise CheckpointValidationError("checkpoint conflicts are invalid")
        seen.add(source_id)


def _validate_document(path: Path) -> dict[str, object]:
    document, rendered = _load_document(path)
    _require_exact_fields(document, _TOP_LEVEL_FIELDS, label="top-level")
    if document["schema"] != "options_copilot.provider_probe_checkpoint.v2":
        raise CheckpointValidationError("checkpoint schema identifier is invalid")
    generated_at = _require_utc_timestamp(
        document["generated_at"],
        label="generated_at",
    )
    if document["status"] not in {"READY", "DEGRADED"}:
        raise CheckpointValidationError("checkpoint aggregate status is invalid")
    expected_decision = (
        "OBSERVATION_ONLY" if document["status"] == "READY" else "NO_TRADE"
    )
    if document["decision"] != expected_decision:
        raise CheckpointValidationError("checkpoint decision is invalid")
    if document["decision_authority"] != "SUPPORTING_ONLY":
        raise CheckpointValidationError("checkpoint authority is invalid")
    _require_bool(document["read_only"], label="read-only", expected=True)
    _require_bool(
        document["instruction_creation_allowed"],
        label="instruction creation",
        expected=False,
    )
    _require_bool(
        document["approval_eligible"],
        label="approval eligibility",
        expected=False,
    )
    _require_bool(
        document["order_submission_allowed"],
        label="order submission",
        expected=False,
    )
    _validate_contract(document["contract"])
    source_states = _validate_sources(document["sources"])
    _validate_conflicts(document["conflicts"])
    if document["redaction"] != {"status": "PASS", "finding_count": 0}:
        raise CheckpointValidationError("checkpoint redaction declaration is invalid")
    _assert_redacted(document)

    declared_hash = document["canonical_sha256"]
    if not isinstance(declared_hash, str) or not _SHA256.fullmatch(declared_hash):
        raise CheckpointValidationError("checkpoint self-hash is invalid")
    without_self_hash = {
        key: value
        for key, value in document.items()
        if key != "canonical_sha256"
    }
    if declared_hash != canonical_hash(without_self_hash):
        raise CheckpointValidationError("checkpoint self-hash does not match")
    full_hash = canonical_hash(document)
    match = _CHECKPOINT_NAME.fullmatch(path.name)
    if match is None or match.group(1) != full_hash:
        raise CheckpointValidationError("checkpoint filename hash does not match")
    if hashlib.sha256(rendered.encode("utf-8")).hexdigest() != _file_digest(path):
        raise CheckpointValidationError("checkpoint content digest does not match")
    return {
        "marker": "PHASE2_LIVE_CHECKPOINT_SAFE",
        "filename": path.name,
        "canonical_sha256": declared_hash,
        "generated_at": generated_at,
        "source_states": source_states,
    }


def validate_checkpoint_transition(
    evidence_dir: str | Path,
    before: Mapping[Path, _JsonIdentity],
    after: Mapping[Path, _JsonIdentity],
) -> dict[str, object]:
    """Require one new file while every pre-existing identity stays unchanged."""

    current = snapshot_json_identities(evidence_dir)
    if dict(after) != current:
        raise CheckpointValidationError("checkpoint after-snapshot is stale")
    before_paths = set(before)
    after_paths = set(after)
    if not before_paths.issubset(after_paths):
        raise CheckpointValidationError("a pre-existing checkpoint was removed")
    for path in before_paths:
        if before[path] != after[path]:
            raise CheckpointValidationError("a pre-existing checkpoint was replaced")
    new_paths = after_paths - before_paths
    if len(new_paths) != 1:
        raise CheckpointValidationError("exactly one new checkpoint is required")
    path = next(iter(new_paths))
    report = _validate_document(path)
    if snapshot_json_identities(evidence_dir) != current:
        raise CheckpointValidationError("checkpoint changed during validation")
    return report


def run_probe_and_validate(
    *,
    root: str | Path,
    evidence_dir: str | Path,
    command_runner: Callable[..., object] = subprocess.run,
) -> dict[str, object]:
    """Invoke the bounded provider probe once and validate its only new file."""

    repository_root = Path(root).resolve(strict=True)
    if repository_root != _repository_root() or repository_root.drive.upper() != "G:":
        raise CheckpointValidationError("repository root is invalid")
    directory = _resolved_live_directory(evidence_dir)
    before = snapshot_json_identities(directory)
    command = [
        str(repository_root / ".venv" / "Scripts" / "python.exe"),
        "-m",
        "options_copilot.providers.cli",
        "probe",
        "--providers",
        "sec,nasdaq,company_ir,finnhub,alpha_vantage,jin10",
        "--symbols",
        "SPY",
        "--limit",
        "1",
        "--evidence-dir",
        str(directory),
        "--json",
    ]
    try:
        completed = command_runner(
            command,
            cwd=repository_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=180,
        )
    except Exception:
        raise CheckpointValidationError("provider probe execution failed") from None
    returncode = getattr(completed, "returncode", None)
    if isinstance(returncode, bool) or not isinstance(returncode, int) or returncode != 0:
        raise CheckpointValidationError("provider probe did not complete safely")
    after = snapshot_json_identities(directory)
    return validate_checkpoint_transition(directory, before, after)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate one bounded Phase 2 provider checkpoint",
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--run-probe", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not args.run_probe:
        parser.error("--run-probe is required")
    try:
        report = run_probe_and_validate(
            root=args.root,
            evidence_dir=args.evidence_dir,
        )
    except CheckpointValidationError:
        print(
            json.dumps(
                {
                    "marker": "PHASE2_LIVE_CHECKPOINT_UNSAFE",
                    "status": "BLOCKED",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EXPECTED_SOURCE_IDS",
    "CheckpointValidationError",
    "main",
    "run_probe_and_validate",
    "snapshot_json_identities",
    "validate_checkpoint_transition",
]
