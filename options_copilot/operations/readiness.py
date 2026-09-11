"""Shared fail-closed readiness probes for the Options Copilot CLI and API.

The default probe path is intentionally observation-only.  It never connects
to a broker, starts a process, changes a listener, or invokes Tailscale.  Live
facts may be supplied by an injected composition root or a previously captured
sanitized fixture; absent facts remain ``MISSING``.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any

from options_copilot.config import OptionsCopilotConfig
from options_copilot.operations.capabilities import (
    CapabilityRecord,
    CapabilityStatus,
    MarketDataPacingCapability,
    ReadinessReport,
)
from options_copilot.operations.pacing_authority import (
    PACING_EXPECTED_ACTOR,
    load_approved_pacing_capability,
    load_pacing_authority_verifier,
)
from options_copilot.storage.canonical import (
    canonical_json,
    freeze_json,
    thaw_json,
    utc_datetime,
)


PROBE_NAMES = (
    "environment_identity",
    "broker_session",
    "market_data_entitlements",
    "market_data_pacing",
    "providers",
    "creator_transport",
    "scanner_heartbeat",
    "learning_governance",
    "process_ports",
    "tailscale_route",
)

_OPERATIONAL_CONTROL_MAX_AGE = timedelta(seconds=15)
_EXECUTABLE_QUOTE_MAX_AGE = timedelta(seconds=5)
_CAPABILITY_DOCUMENT_MAX_AGE = timedelta(hours=24)
_SECRET_KEY_PARTS = (
    "password",
    "secret",
    "token",
    "apikey",
    "authorizationheader",
    "credential",
    "sessioncookie",
)
_SECRET_VALUE_PATTERNS = (
    re.compile(r"\bBearer\s+\S+", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{12,}"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{12,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
)
_ENVIRONMENT_IDENTITY_FIELDS = frozenset(
    {
        "interpreter_path",
        "python_version",
        "lock_filename",
        "lock_type",
        "lock_sha256",
        "dependency_check",
    }
)
_ENVIRONMENT_FAILURE_REASONS = {
    "INTERPRETER_MISSING": "ENVIRONMENT_INTERPRETER_MISSING",
    "LOCK_MISSING": "ENVIRONMENT_LOCK_MISSING",
    "VERIFIER_MISSING": "ENVIRONMENT_VERIFIER_MISSING",
    "INTERPRETER_OUTSIDE_PROJECT_VENV": (
        "ENVIRONMENT_INTERPRETER_OUTSIDE_PROJECT_VENV"
    ),
    "PROJECT_VENV_REPARSE_POINT": "ENVIRONMENT_PROJECT_VENV_REPARSE_POINT",
    "PYTHON_UNSUPPORTED": "ENVIRONMENT_PYTHON_UNSUPPORTED",
    "LOCK_HASH_MISMATCH": "ENVIRONMENT_LOCK_HASH_MISMATCH",
    "ATTESTATION_MISMATCH": "ENVIRONMENT_IDENTITY_ATTESTATION_MISMATCH",
    "INVENTORY_MISMATCH": "ENVIRONMENT_INVENTORY_MISMATCH",
    "PIP_CHECK_FAILED": "ENVIRONMENT_PIP_CHECK_FAILED",
}
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_VENV = _PROJECT_ROOT / ".venv"
_PACING_AUTHORITY_RESOLUTION_SCHEMA = (
    "options_copilot.pacing_authority_resolution.v1"
)


class ReadinessInputError(ValueError):
    """A probe fixture is malformed or contains an unsupported field."""


class SecretLikeFieldError(ReadinessInputError):
    """A readiness input or output contains possible credential material."""


@dataclass(frozen=True, slots=True)
class ReadinessProbeInputs:
    """Sanitized observations supplied to the pure readiness evaluator."""

    environment_identity: Mapping[str, object] | None = None
    broker_session: Mapping[str, object] | None = None
    market_data_entitlements: Mapping[str, object] | None = None
    market_data_pacing: Mapping[str, object] | None = None
    providers: Mapping[str, object] | None = None
    creator_transport: Mapping[str, object] | None = None
    scanner_heartbeat: Mapping[str, object] | None = None
    learning_governance: Mapping[str, object] | None = None
    process_ports: Mapping[str, object] | None = None
    tailscale_route: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            if value is None:
                continue
            assert_no_secret_like(value, path=f"$.{item.name}")
            frozen = freeze_json(value)
            if not isinstance(frozen, Mapping):
                raise ReadinessInputError(f"{item.name} must be a mapping")
            object.__setattr__(self, item.name, frozen)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "ReadinessProbeInputs":
        if not isinstance(value, Mapping):
            raise ReadinessInputError("readiness inputs must be a mapping")
        assert_no_secret_like(value)
        allowed = {item.name for item in fields(cls)}
        keys = {str(key) for key in value}
        unknown = sorted(keys - allowed)
        if unknown:
            raise ReadinessInputError(
                f"unknown readiness input fields: {', '.join(unknown)}"
            )
        kwargs: dict[str, Mapping[str, object] | None] = {}
        for name in allowed:
            raw = value.get(name)
            if raw is None:
                kwargs[name] = None
            elif isinstance(raw, Mapping):
                kwargs[name] = {str(key): item for key, item in raw.items()}
            else:
                raise ReadinessInputError(f"{name} must be a mapping or null")
        return cls(**kwargs)

    def as_dict(self) -> dict[str, object]:
        return {
            item.name: (
                None
                if (value := getattr(self, item.name)) is None
                else thaw_json(value)
            )
            for item in fields(self)
        }


def build_readiness_report(
    inputs: ReadinessProbeInputs | None = None,
    *,
    probes: Sequence[str] | None = None,
    now: datetime | None = None,
) -> ReadinessReport:
    """Evaluate selected observations without importing not-yet-built modules."""

    supplied = inputs or ReadinessProbeInputs()
    observed_at = utc_datetime(
        now or datetime.now(timezone.utc),
        field="now",
    )
    selected = _selected_probes(probes)
    records = tuple(
        _PROBES[name](getattr(supplied, name), observed_at)
        for name in selected
    )
    report = ReadinessReport(observed_at=observed_at, records=records)
    assert_no_secret_like(report.as_dict())
    return report


def probe_environment_identity(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    """Project interpreter and exact-lock identity with fixed public fields."""

    if observation is None:
        return _missing(
            "environment_identity",
            now,
            "ENVIRONMENT_INTERPRETER_MISSING",
        )
    keys = {str(key) for key in observation}
    unknown = sorted(keys - _ENVIRONMENT_IDENTITY_FIELDS)
    if unknown:
        raise ReadinessInputError(
            "unknown environment identity fields: " + ", ".join(unknown)
        )
    details = _environment_identity_details(observation)
    dependency_check = str(observation.get("dependency_check") or "").strip()
    failure_reason = _ENVIRONMENT_FAILURE_REASONS.get(dependency_check)
    if failure_reason is not None:
        status = (
            CapabilityStatus.MISSING
            if dependency_check
            in {"INTERPRETER_MISSING", "LOCK_MISSING", "VERIFIER_MISSING"}
            else CapabilityStatus.FORBIDDEN
        )
        return _record(
            "environment_identity",
            status,
            details,
            now,
            failure_reason,
        )

    trusted_details = _environment_identity_details(_observe_environment_identity())
    trusted_dependency_check = str(
        trusted_details.get("dependency_check") or ""
    ).strip()
    trusted_failure_reason = _ENVIRONMENT_FAILURE_REASONS.get(
        trusted_dependency_check
    )
    if trusted_failure_reason is not None:
        status = (
            CapabilityStatus.MISSING
            if trusted_dependency_check
            in {"INTERPRETER_MISSING", "LOCK_MISSING", "VERIFIER_MISSING"}
            else CapabilityStatus.FORBIDDEN
        )
        return _record(
            "environment_identity",
            status,
            trusted_details,
            now,
            trusted_failure_reason,
        )
    if details != trusted_details:
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            trusted_details,
            now,
            "ENVIRONMENT_IDENTITY_ATTESTATION_MISMATCH",
        )
    details = trusted_details

    interpreter_path = details.get("interpreter_path")
    if not isinstance(interpreter_path, str) or not interpreter_path:
        return _record(
            "environment_identity",
            CapabilityStatus.MISSING,
            details,
            now,
            "ENVIRONMENT_INTERPRETER_MISSING",
        )
    candidate_interpreter = Path(interpreter_path).expanduser()
    trusted_interpreter, interpreter_status = _validated_project_venv_interpreter()
    if interpreter_status == "INTERPRETER_MISSING":
        return _record(
            "environment_identity",
            CapabilityStatus.MISSING,
            details,
            now,
            "ENVIRONMENT_INTERPRETER_MISSING",
        )
    if interpreter_status == "PROJECT_VENV_REPARSE_POINT":
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            details,
            now,
            "ENVIRONMENT_PROJECT_VENV_REPARSE_POINT",
        )
    if trusted_interpreter is None:
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            details,
            now,
            "ENVIRONMENT_INTERPRETER_OUTSIDE_PROJECT_VENV",
        )
    try:
        resolved_interpreter = candidate_interpreter.resolve(strict=True)
        if resolved_interpreter != trusted_interpreter:
            raise ValueError("interpreter does not match the trusted project venv")
    except (OSError, RuntimeError, ValueError):
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            details,
            now,
            "ENVIRONMENT_INTERPRETER_OUTSIDE_PROJECT_VENV",
        )
    details["interpreter_path"] = str(resolved_interpreter)

    python_version = details.get("python_version")
    version_match = (
        re.fullmatch(r"(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)", python_version)
        if isinstance(python_version, str)
        else None
    )
    if version_match is None or (
        int(version_match.group("major")),
        int(version_match.group("minor")),
    ) < (3, 12):
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            details,
            now,
            "ENVIRONMENT_PYTHON_UNSUPPORTED",
        )

    lock_filename = details.get("lock_filename")
    lock_type = details.get("lock_type")
    if not lock_filename:
        return _record(
            "environment_identity",
            CapabilityStatus.MISSING,
            details,
            now,
            "ENVIRONMENT_LOCK_MISSING",
        )
    if lock_filename != "requirements-dev.lock" or lock_type != "development":
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            details,
            now,
            "ENVIRONMENT_LOCK_HASH_MISMATCH",
        )

    lock_sha256 = details.get("lock_sha256")
    if not isinstance(lock_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}",
        lock_sha256,
    ):
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            details,
            now,
            "ENVIRONMENT_LOCK_HASH_INVALID",
        )
    if dependency_check != "EXACT_LOCK_MATCH":
        return _record(
            "environment_identity",
            CapabilityStatus.FORBIDDEN,
            details,
            now,
            "ENVIRONMENT_DEPENDENCY_CHECK_INVALID",
        )
    return _ready("environment_identity", details, now)


def probe_broker_session(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    missing = _missing("broker_session", now, "BROKER_SESSION_MISSING")
    if observation is None:
        return missing
    freshness = _freshness(
        "broker_session",
        observation,
        now,
        "BROKER_SESSION",
        max_age=_OPERATIONAL_CONTROL_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("connected") is not True:
        return _record(
            "broker_session",
            CapabilityStatus.DEGRADED,
            observation,
            observed_at,
            "BROKER_SESSION_DISCONNECTED",
        )
    if observation.get("read_only") is not True:
        return _record(
            "broker_session",
            CapabilityStatus.FORBIDDEN,
            observation,
            observed_at,
            "BROKER_SESSION_NOT_READ_ONLY",
        )
    return _ready("broker_session", observation, observed_at)


def probe_market_data_entitlements(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing(
            "market_data_entitlements",
            now,
            "MARKET_DATA_ENTITLEMENT_MISSING",
        )
    freshness = _freshness(
        "market_data_entitlements",
        observation,
        now,
        "MARKET_DATA_ENTITLEMENT",
        max_age=_EXECUTABLE_QUOTE_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("entitled") is not True:
        return _record(
            "market_data_entitlements",
            CapabilityStatus.MISSING,
            observation,
            observed_at,
            "MARKET_DATA_ENTITLEMENT_MISSING",
        )
    quote_mode = str(observation.get("quote_mode") or "").strip().lower()
    if quote_mode in {"delayed", "frozen_delayed", "delayed_frozen"}:
        return _record(
            "market_data_entitlements",
            CapabilityStatus.DEGRADED,
            observation,
            observed_at,
            "MARKET_DATA_DELAYED",
        )
    if quote_mode != "live":
        return _record(
            "market_data_entitlements",
            CapabilityStatus.DEGRADED,
            observation,
            observed_at,
            "MARKET_DATA_MODE_UNKNOWN",
        )
    return _ready("market_data_entitlements", observation, observed_at)


def probe_market_data_pacing(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing(
            "market_data_pacing",
            now,
            "PACING_CAPABILITY_MISSING",
        )
    if observation.get("schema") == _PACING_AUTHORITY_RESOLUTION_SCHEMA:
        return _pacing_authority_resolution_record(observation, now)
    payload = _pacing_payload(observation)
    return MarketDataPacingCapability.inspect(payload, now=now)


def probe_providers(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing("providers", now, "PROVIDER_CAPABILITY_MISSING")
    freshness = _freshness(
        "providers",
        observation,
        now,
        "PROVIDER",
        max_age=_CAPABILITY_DOCUMENT_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("available") is not True:
        return _record(
            "providers",
            CapabilityStatus.MISSING,
            observation,
            observed_at,
            "PROVIDER_CAPABILITY_MISSING",
        )
    if observation.get("conflicted") is True:
        return _record(
            "providers",
            CapabilityStatus.FORBIDDEN,
            observation,
            observed_at,
            "PROVIDER_EVIDENCE_CONFLICTED",
        )
    return _ready("providers", observation, observed_at)


def probe_creator_transport(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing(
            "creator_transport",
            now,
            "CREATOR_TRANSPORT_MISSING",
        )
    freshness = _freshness(
        "creator_transport",
        observation,
        now,
        "CREATOR_TRANSPORT",
        max_age=_OPERATIONAL_CONTROL_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("available") is not True:
        return _record(
            "creator_transport",
            CapabilityStatus.MISSING,
            observation,
            observed_at,
            "CREATOR_TRANSPORT_MISSING",
        )
    if (
        observation.get("review_only") is not True
        or observation.get("direct_order_submission") is not False
    ):
        return _record(
            "creator_transport",
            CapabilityStatus.FORBIDDEN,
            observation,
            observed_at,
            "CREATOR_TRANSPORT_AUTHORITY_FORBIDDEN",
        )
    return _ready("creator_transport", observation, observed_at)


def probe_scanner_heartbeat(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing(
            "scanner_heartbeat",
            now,
            "SCANNER_HEARTBEAT_MISSING",
        )
    freshness = _freshness(
        "scanner_heartbeat",
        observation,
        now,
        "SCANNER_HEARTBEAT",
        max_age=_OPERATIONAL_CONTROL_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("available") is not True:
        return _record(
            "scanner_heartbeat",
            CapabilityStatus.MISSING,
            observation,
            observed_at,
            "SCANNER_HEARTBEAT_MISSING",
        )
    if observation.get("stale") is True:
        return _record(
            "scanner_heartbeat",
            CapabilityStatus.STALE,
            observation,
            observed_at,
            "SCANNER_HEARTBEAT_STALE",
        )
    return _ready("scanner_heartbeat", observation, observed_at)


def probe_learning_governance(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing(
            "learning_governance",
            now,
            "LEARNING_GOVERNANCE_MISSING",
        )
    freshness = _freshness(
        "learning_governance",
        observation,
        now,
        "LEARNING_GOVERNANCE",
        max_age=_CAPABILITY_DOCUMENT_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("available") is not True:
        return _record(
            "learning_governance",
            CapabilityStatus.MISSING,
            observation,
            observed_at,
            "LEARNING_GOVERNANCE_MISSING",
        )
    if (
        observation.get("human_promotion_required") is not True
        or observation.get("automatic_promotion") is not False
    ):
        return _record(
            "learning_governance",
            CapabilityStatus.FORBIDDEN,
            observation,
            observed_at,
            "LEARNING_AUTOMATIC_PROMOTION_FORBIDDEN",
        )
    return _ready("learning_governance", observation, observed_at)


def probe_process_ports(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing("process_ports", now, "PROCESS_PORTS_MISSING")
    freshness = _freshness(
        "process_ports",
        observation,
        now,
        "PROCESS_PORTS",
        max_age=_OPERATIONAL_CONTROL_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("protected_ports_unchanged") is not True:
        return _record(
            "process_ports",
            CapabilityStatus.FORBIDDEN,
            observation,
            observed_at,
            "PROTECTED_PORT_STATE_CHANGED",
        )
    if observation.get("listener_known") is not True:
        return _record(
            "process_ports",
            CapabilityStatus.DEGRADED,
            observation,
            observed_at,
            "PROCESS_LISTENER_UNKNOWN",
        )
    return _ready("process_ports", observation, observed_at)


def probe_tailscale_route(
    observation: Mapping[str, object] | None,
    now: datetime,
) -> CapabilityRecord:
    if observation is None:
        return _missing("tailscale_route", now, "TAILSCALE_ROUTE_MISSING")
    freshness = _freshness(
        "tailscale_route",
        observation,
        now,
        "TAILSCALE_ROUTE",
        max_age=_OPERATIONAL_CONTROL_MAX_AGE,
    )
    if isinstance(freshness, CapabilityRecord):
        return freshness
    observed_at = freshness
    if observation.get("protected_root_unchanged") is not True:
        return _record(
            "tailscale_route",
            CapabilityStatus.FORBIDDEN,
            observation,
            observed_at,
            "PROTECTED_TAILSCALE_ROUTE_CHANGED",
        )
    if observation.get("remote_route_present") is not True:
        return _record(
            "tailscale_route",
            CapabilityStatus.MISSING,
            observation,
            observed_at,
            "REMOTE_ROUTE_MISSING",
        )
    return _ready("tailscale_route", observation, observed_at)


_PROBES: dict[
    str,
    Callable[[Mapping[str, object] | None, datetime], CapabilityRecord],
] = {
    "environment_identity": probe_environment_identity,
    "broker_session": probe_broker_session,
    "market_data_entitlements": probe_market_data_entitlements,
    "market_data_pacing": probe_market_data_pacing,
    "providers": probe_providers,
    "creator_transport": probe_creator_transport,
    "scanner_heartbeat": probe_scanner_heartbeat,
    "learning_governance": probe_learning_governance,
    "process_ports": probe_process_ports,
    "tailscale_route": probe_tailscale_route,
}


def default_readiness_report() -> ReadinessReport:
    """Build a truthful report from explicitly configured local evidence only."""

    now = datetime.now(timezone.utc)
    return build_readiness_report(load_default_probe_inputs(now=now), now=now)


def load_default_probe_inputs(
    *,
    now: datetime | None = None,
) -> ReadinessProbeInputs:
    """Observe environment identity and the installed pacing authority."""

    environment_identity = _observe_environment_identity()
    raw_path = os.getenv("OPTIONS_COPILOT_PACING_CAPABILITY_PATH", "").strip()
    if raw_path:
        path = Path(raw_path).expanduser().resolve()
        payload = _load_json_mapping(path)
        return ReadinessProbeInputs(
            environment_identity=environment_identity,
            market_data_pacing=payload,
        )

    checked_at = utc_datetime(
        now or datetime.now(timezone.utc),
        field="pacing readiness checked_at",
    )
    config = OptionsCopilotConfig.from_env()
    if config.pacing_authority_dir is None:
        return ReadinessProbeInputs(environment_identity=environment_identity)
    try:
        verifier = load_pacing_authority_verifier(
            config.pacing_authority_keyring_path
        )
        resolution = load_approved_pacing_capability(
            config.pacing_authority_dir,
            now=checked_at,
            expected_actor=PACING_EXPECTED_ACTOR,
            signature_verifier=verifier,
        )
        pacing = _pacing_authority_resolution_observation(resolution.record)
    except (OSError, TypeError, ValueError):
        pacing = _pacing_authority_resolution_observation(
            CapabilityRecord(
                name="market_data_pacing",
                status=CapabilityStatus.FORBIDDEN,
                observed_at=checked_at,
                reason_codes=(
                    "PACING_AUTHORITY_VALIDATION_FAILED",
                    "PACING_CAPABILITY_MISSING",
                ),
                details={
                    "review_only": True,
                    "direct_order_submission": False,
                },
            )
        )
    return ReadinessProbeInputs(
        environment_identity=environment_identity,
        market_data_pacing=pacing,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.operations.readiness",
        description="Emit observation-only Options Copilot readiness evidence.",
    )
    parser.add_argument(
        "--probe",
        action="append",
        choices=PROBE_NAMES,
        help="Named probe to run; repeat to select more than one.",
    )
    parser.add_argument("--json", action="store_true", help="Emit canonical JSON.")
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        help="Create one immutable run directory containing readiness.json.",
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        help="Read sanitized offline probe observations from a JSON fixture.",
    )
    return parser


def run_cli(
    argv: Sequence[str] | None = None,
    *,
    inputs: ReadinessProbeInputs | None = None,
    now: Callable[[], datetime] | datetime | None = None,
) -> dict[str, object]:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if inputs is not None and args.fixture is not None:
        raise ReadinessInputError("fixture and injected inputs are mutually exclusive")
    observed_at = _resolve_now(now)
    if inputs is None:
        if args.fixture is not None:
            inputs = ReadinessProbeInputs.from_mapping(
                _load_json_mapping(args.fixture.expanduser().resolve())
            )
        else:
            inputs = load_default_probe_inputs(now=observed_at)
    report = build_readiness_report(inputs, probes=args.probe, now=observed_at)
    payload = report.as_dict()
    assert_no_secret_like(payload)
    if args.evidence_dir is not None:
        write_readiness_evidence(payload, args.evidence_dir, observed_at=observed_at)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(f"{payload['status']} {payload['content_hash']}")
    return payload


def write_readiness_evidence(
    payload: Mapping[str, object],
    evidence_dir: Path,
    *,
    observed_at: datetime,
) -> Path:
    """Write exactly one non-overwriting readiness artifact for a probe run."""

    assert_no_secret_like(payload)
    content_hash = payload.get("content_hash")
    if not isinstance(content_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", content_hash):
        raise ReadinessInputError("readiness payload has no valid content hash")
    root = evidence_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    stamp = utc_datetime(observed_at).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = root / f"{stamp}-{content_hash[:12]}"
    run_dir.mkdir(exist_ok=False)
    destination = run_dir / "readiness.json"
    temporary = run_dir / "readiness.json.tmp"
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, destination)
    return destination


def assert_no_secret_like(value: object, *, path: str = "$") -> None:
    """Reject likely credential keys and recognizable raw credential values."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            normalized = re.sub(r"[^a-z0-9]", "", name.lower())
            if any(part in normalized for part in _SECRET_KEY_PARTS):
                raise SecretLikeFieldError(f"secret-like field is prohibited at {path}.{name}")
            assert_no_secret_like(item, path=f"{path}.{name}")
        return
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for index, item in enumerate(value):
            assert_no_secret_like(item, path=f"{path}[{index}]")
        return
    if isinstance(value, str) and any(
        pattern.search(value) for pattern in _SECRET_VALUE_PATTERNS
    ):
        raise SecretLikeFieldError(f"secret-like value is prohibited at {path}")


def _selected_probes(probes: Sequence[str] | None) -> tuple[str, ...]:
    if probes is None:
        return PROBE_NAMES
    selected: list[str] = []
    for raw in probes:
        name = str(raw).strip()
        if name not in _PROBES:
            raise ReadinessInputError(f"unknown readiness probe: {name}")
        if name not in selected:
            selected.append(name)
    if not selected:
        raise ReadinessInputError("at least one readiness probe is required")
    return tuple(selected)


def _environment_identity_details(
    observation: Mapping[str, object],
) -> dict[str, object]:
    details: dict[str, object] = {}
    for name in _ENVIRONMENT_IDENTITY_FIELDS:
        value = observation.get(name)
        if isinstance(value, str):
            normalized = value.strip()
            if name == "lock_sha256":
                normalized = normalized.lower()
            details[name] = normalized
        elif value is not None:
            raise ReadinessInputError(
                f"environment identity field must be a string: {name}"
            )
    assert_no_secret_like(details, path="$.environment_identity")
    return details


def _validated_project_venv_interpreter() -> tuple[Path | None, str | None]:
    lexical_root = Path(os.path.abspath(_PROJECT_ROOT))
    lexical_venv = Path(os.path.abspath(_PROJECT_VENV))
    scripts_name = "Scripts" if os.name == "nt" else "bin"
    lexical_scripts = lexical_venv / scripts_name
    lexical_interpreter = Path(
        os.path.abspath(Path(sys.executable).expanduser())
    )
    expected_paths = (
        (lexical_venv, True),
        (lexical_scripts, True),
        (lexical_interpreter, False),
    )
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    for candidate, must_be_directory in expected_paths:
        try:
            attributes = os.lstat(candidate)
        except (FileNotFoundError, NotADirectoryError):
            return None, "INTERPRETER_MISSING"
        except OSError:
            return None, "INTERPRETER_OUTSIDE_PROJECT_VENV"
        file_attributes = getattr(attributes, "st_file_attributes", 0)
        if stat.S_ISLNK(attributes.st_mode) or file_attributes & reparse_flag:
            return None, "PROJECT_VENV_REPARSE_POINT"
        if must_be_directory and not stat.S_ISDIR(attributes.st_mode):
            return None, "INTERPRETER_OUTSIDE_PROJECT_VENV"
        if not must_be_directory and not stat.S_ISREG(attributes.st_mode):
            return None, "INTERPRETER_OUTSIDE_PROJECT_VENV"

    try:
        lexical_venv.relative_to(lexical_root)
        lexical_interpreter.relative_to(lexical_scripts)
        canonical_root = lexical_root.resolve(strict=True)
        canonical_venv = lexical_venv.resolve(strict=True)
        canonical_scripts = lexical_scripts.resolve(strict=True)
        canonical_interpreter = lexical_interpreter.resolve(strict=True)
        canonical_venv.relative_to(canonical_root)
        canonical_scripts.relative_to(canonical_venv)
        canonical_interpreter.relative_to(canonical_scripts)
    except (OSError, RuntimeError, ValueError):
        return None, "INTERPRETER_OUTSIDE_PROJECT_VENV"
    if (
        os.path.normcase(str(canonical_venv))
        != os.path.normcase(str(lexical_venv))
        or os.path.normcase(str(canonical_scripts))
        != os.path.normcase(str(lexical_scripts))
    ):
        return None, "PROJECT_VENV_REPARSE_POINT"
    return canonical_interpreter, None


def _observe_environment_identity() -> dict[str, object]:
    interpreter = Path(os.path.abspath(Path(sys.executable).expanduser()))
    lock_path = _PROJECT_ROOT / "requirements-dev.lock"
    verifier_path = _PROJECT_ROOT / "scripts" / "verify_locked_environment.py"
    identity: dict[str, object] = {
        "interpreter_path": str(interpreter),
        "lock_filename": "requirements-dev.lock",
        "lock_type": "development",
    }
    trusted_interpreter, interpreter_status = _validated_project_venv_interpreter()
    if interpreter_status is not None or trusted_interpreter is None:
        identity["dependency_check"] = (
            interpreter_status or "INTERPRETER_OUTSIDE_PROJECT_VENV"
        )
        return identity
    identity["interpreter_path"] = str(trusted_interpreter)
    try:
        version_probe = subprocess.run(
            [
                str(trusted_interpreter),
                "-c",
                (
                    "import json, platform, sys; "
                    "print(json.dumps({'implementation': "
                    "platform.python_implementation(), 'version': "
                    "platform.python_version(), 'executable': sys.executable}, "
                    "sort_keys=True))"
                ),
            ],
            cwd=_PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=30,
        )
        version_payload = json.loads(version_probe.stdout)
        if not isinstance(version_payload, Mapping):
            raise ValueError("interpreter identity payload is not an object")
        observed_executable = Path(str(version_payload.get("executable") or ""))
        if (
            version_probe.returncode != 0
            or version_payload.get("implementation") != "CPython"
            or observed_executable.resolve(strict=True)
            != trusted_interpreter
        ):
            raise ValueError("interpreter identity mismatch")
        identity["python_version"] = str(version_payload.get("version") or "")
    except (
        OSError,
        RuntimeError,
        ValueError,
        json.JSONDecodeError,
        subprocess.SubprocessError,
        UnicodeError,
    ):
        identity["dependency_check"] = "PYTHON_UNSUPPORTED"
        return identity
    if not re.fullmatch(r"\d+\.\d+\.\d+", str(identity["python_version"])):
        identity["dependency_check"] = "PYTHON_UNSUPPORTED"
        return identity
    if not lock_path.is_file():
        identity["dependency_check"] = "LOCK_MISSING"
        return identity
    if not verifier_path.is_file():
        identity["dependency_check"] = "VERIFIER_MISSING"
        return identity
    try:
        resolved_lock = lock_path.resolve(strict=True)
        resolved_verifier = verifier_path.resolve(strict=True)
    except OSError:
        identity["dependency_check"] = "LOCK_MISSING"
        return identity
    if resolved_lock != lock_path or resolved_verifier != verifier_path:
        identity["dependency_check"] = "LOCK_HASH_MISMATCH"
        return identity
    try:
        identity["lock_sha256"] = hashlib.sha256(lock_path.read_bytes()).hexdigest()
    except OSError:
        identity["dependency_check"] = "LOCK_HASH_MISMATCH"
        return identity
    try:
        verifier = subprocess.run(
            [
                str(trusted_interpreter),
                str(verifier_path),
                "--lock",
                str(lock_path),
                "--allow-bootstrap",
                "pip",
                "--allow-bootstrap",
                "setuptools",
            ],
            cwd=_PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError):
        identity["dependency_check"] = "INVENTORY_MISMATCH"
        return identity
    if verifier.returncode != 0:
        identity["dependency_check"] = "INVENTORY_MISMATCH"
        return identity
    try:
        pip_check = subprocess.run(
            [str(trusted_interpreter), "-m", "pip", "check"],
            cwd=_PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError):
        identity["dependency_check"] = "PIP_CHECK_FAILED"
        return identity
    identity["dependency_check"] = (
        "EXACT_LOCK_MATCH" if pip_check.returncode == 0 else "PIP_CHECK_FAILED"
    )
    assert_no_secret_like(identity, path="$.environment_identity")
    return identity


def _pacing_payload(observation: Mapping[str, object]) -> Mapping[str, object]:
    keys = {str(key) for key in observation}
    direct_fields = {
        "version",
        "observed_at",
        "source",
        "request_classes",
        "signer",
    }
    if "content_hash" in keys:
        return {str(key): thaw_json(item) for key, item in observation.items()}
    if keys <= direct_fields:
        try:
            return MarketDataPacingCapability.create(
                version=str(observation.get("version") or ""),
                observed_at=_parse_datetime(observation.get("observed_at")),
                source=str(observation.get("source") or ""),
                request_classes=observation.get("request_classes"),  # type: ignore[arg-type]
                signer=observation.get("signer"),  # type: ignore[arg-type]
            ).as_dict()
        except (TypeError, ValueError):
            return {str(key): thaw_json(item) for key, item in observation.items()}

    source_key: str | None = None
    if isinstance(observation.get("broker_disclosed"), Mapping):
        source_key = "broker_disclosed"
    elif isinstance(observation.get("bounded_observations"), Mapping):
        source_key = "bounded_observations"
    if source_key is None:
        return {str(key): thaw_json(item) for key, item in observation.items()}
    request_classes = observation[source_key]
    source = "broker_disclosed" if source_key == "broker_disclosed" else "observed"
    try:
        return MarketDataPacingCapability.create(
            version=str(observation.get("version") or ""),
            observed_at=_parse_datetime(observation.get("observed_at")),
            source=source,
            request_classes=request_classes,  # type: ignore[arg-type]
            signer=observation.get("signer"),  # type: ignore[arg-type]
        ).as_dict()
    except (TypeError, ValueError):
        return {str(key): thaw_json(item) for key, item in observation.items()}


def _pacing_authority_resolution_observation(
    record: CapabilityRecord,
) -> Mapping[str, object]:
    """Wrap one loader-validated authority record for the pure probe layer."""

    return {
        "schema": _PACING_AUTHORITY_RESOLUTION_SCHEMA,
        "record": record.as_dict(),
    }


def _pacing_authority_resolution_record(
    observation: Mapping[str, object],
    now: datetime,
) -> CapabilityRecord:
    """Project a signed-authority loader result without reclassifying its age."""

    try:
        if set(observation) != {"schema", "record"}:
            raise ValueError("authority resolution fields are invalid")
        raw = observation.get("record")
        if not isinstance(raw, Mapping) or set(raw) != {
            "name",
            "status",
            "observed_at",
            "reason_codes",
            "details",
            "content_hash",
        }:
            raise ValueError("authority record fields are invalid")
        reasons = raw.get("reason_codes")
        details = raw.get("details")
        if (
            not isinstance(reasons, Sequence)
            or isinstance(reasons, (str, bytes))
            or not isinstance(details, Mapping)
        ):
            raise TypeError("authority record shape is invalid")
        record = CapabilityRecord(
            name=str(raw.get("name") or ""),
            status=CapabilityStatus(str(raw.get("status") or "")),
            observed_at=_parse_datetime(raw.get("observed_at")),
            reason_codes=tuple(str(item) for item in reasons),
            details={str(key): thaw_json(item) for key, item in details.items()},
        )
        if (
            record.name != "market_data_pacing"
            or raw.get("content_hash") != record.content_hash
            or record.observed_at > now
        ):
            raise ValueError("authority record identity is invalid")
        if record.status is CapabilityStatus.READY_FOR_REVIEW:
            projected = record.details
            keyring_hash = projected.get("approval_keyring_hash")
            signer_key_id = projected.get("approval_signer_key_id")
            if (
                record.reason_codes
                or projected.get("approval_actor") != PACING_EXPECTED_ACTOR
                or projected.get("approval_decision") != "APPROVED"
                or projected.get("approval_signature_authenticated") is not True
                or not isinstance(signer_key_id, str)
                or not signer_key_id.strip()
                or not isinstance(keyring_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", keyring_hash) is None
                or projected.get("review_only") is not True
                or projected.get("direct_order_submission") is not False
            ):
                raise ValueError("approved authority evidence is incomplete")
        return record
    except (TypeError, ValueError):
        return CapabilityRecord(
            name="market_data_pacing",
            status=CapabilityStatus.FORBIDDEN,
            observed_at=now,
            reason_codes=(
                "PACING_AUTHORITY_RESOLUTION_INVALID",
                "PACING_CAPABILITY_MISSING",
            ),
            details={
                "review_only": True,
                "direct_order_submission": False,
            },
        )


def _freshness(
    name: str,
    observation: Mapping[str, object],
    now: datetime,
    prefix: str,
    *,
    max_age: timedelta,
) -> CapabilityRecord | datetime:
    if "observed_at" not in observation:
        return _record(
            name,
            CapabilityStatus.MISSING,
            observation,
            now,
            f"{prefix}_OBSERVATION_MISSING",
        )
    try:
        observed_at = _parse_datetime(observation["observed_at"])
    except (TypeError, ValueError):
        return _record(
            name,
            CapabilityStatus.FORBIDDEN,
            observation,
            now,
            f"{prefix}_OBSERVATION_INVALID",
        )
    if observed_at > now:
        return _record(
            name,
            CapabilityStatus.FORBIDDEN,
            observation,
            observed_at,
            f"{prefix}_OBSERVATION_FUTURE",
        )
    if now - observed_at > max_age:
        return _record(
            name,
            CapabilityStatus.STALE,
            observation,
            observed_at,
            f"{prefix}_STALE",
        )
    return observed_at


def _ready(
    name: str,
    observation: Mapping[str, object],
    now: datetime,
) -> CapabilityRecord:
    return _record(name, CapabilityStatus.READY_FOR_REVIEW, observation, now)


def _missing(name: str, now: datetime, reason: str) -> CapabilityRecord:
    return CapabilityRecord(
        name=name,
        status=CapabilityStatus.MISSING,
        observed_at=now,
        reason_codes=(reason,),
        details={},
    )


def _record(
    name: str,
    status: CapabilityStatus,
    observation: Mapping[str, object],
    observed_at: datetime,
    *reasons: str,
) -> CapabilityRecord:
    details = {str(key): thaw_json(item) for key, item in observation.items()}
    return CapabilityRecord(
        name=name,
        status=status,
        observed_at=observed_at,
        reason_codes=tuple(reasons),
        details=details,
    )


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return utc_datetime(value)
    if not isinstance(value, str) or not value.strip():
        raise TypeError("timestamp must be a timezone-aware datetime")
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return utc_datetime(datetime.fromisoformat(raw))


def _resolve_now(value: Callable[[], datetime] | datetime | None) -> datetime:
    if callable(value):
        value = value()
    return utc_datetime(value or datetime.now(timezone.utc), field="now")


def _load_json_mapping(path: Path) -> dict[str, object]:
    try:
        value: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReadinessInputError(f"unable to load readiness JSON: {path}") from exc
    if not isinstance(value, Mapping):
        raise ReadinessInputError("readiness JSON must contain an object")
    assert_no_secret_like(value)
    return {str(key): item for key, item in value.items()}


def main(argv: Sequence[str] | None = None) -> int:
    try:
        run_cli(argv)
    except (OSError, ReadinessInputError, TypeError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "status": "FORBIDDEN",
                    "reason_codes": ["READINESS_PROBE_FAILED"],
                    "message": str(exc)[:240],
                    "review_only": True,
                    "direct_order_submission": False,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())


__all__ = [
    "PROBE_NAMES",
    "ReadinessInputError",
    "ReadinessProbeInputs",
    "SecretLikeFieldError",
    "assert_no_secret_like",
    "build_parser",
    "build_readiness_report",
    "default_readiness_report",
    "load_default_probe_inputs",
    "probe_environment_identity",
    "probe_market_data_pacing",
    "run_cli",
    "write_readiness_evidence",
]
