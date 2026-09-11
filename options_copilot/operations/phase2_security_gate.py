"""Validate Phase 2 ASVS traceability and residual-risk evidence."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from types import MappingProxyType
from typing import Mapping, Sequence


SCHEMA_VERSION = 1
REGISTER_KIND = "options_copilot.phase2_security_controls.v1"
ASVS_SOURCE_URL = (
    "https://raw.githubusercontent.com/OWASP/ASVS/v5.0.0/5.0/docs_en/"
    "OWASP_Application_Security_Verification_Standard_5.0.0_en.flat.json"
)
APPLICABLE_L1_IDS = (
    "V2.1.1",
    "V2.2.1",
    "V2.2.2",
    "V2.3.1",
    "V4.1.1",
    "V5.3.2",
    "V8.3.1",
    "V11.4.1",
    "V12.1.1",
    "V12.2.1",
    "V12.2.2",
    "V14.2.1",
    "V15.3.1",
)
PROJECT_L2_IDS = (
    "V1.3.6",
    "V13.2.4",
    "V15.3.2",
    "V16.5.2",
)
_ASVS_CONTROL_ROWS = {
    "V1.3.6": (
        "L2",
        "Verify that the application protects against Server-side Request Forgery "
        "(SSRF) attacks, by validating untrusted data against an allowlist of "
        "protocols, domains, paths and ports and sanitizing potentially dangerous "
        "characters before using the data to call another service.",
    ),
    "V2.1.1": (
        "L1",
        "Verify that the application's documentation defines input validation rules "
        "for how to check the validity of data items against an expected structure. "
        "This could be common data formats such as credit card numbers, email "
        "addresses, telephone numbers, or it could be an internal data format.",
    ),
    "V2.2.1": (
        "L1",
        "Verify that input is validated to enforce business or functional "
        "expectations for that input. This should either use positive validation "
        "against an allow list of values, patterns, and ranges, or be based on "
        "comparing the input to an expected structure and logical limits according "
        "to predefined rules. For L1, this can focus on input which is used to make "
        "specific business or security decisions. For L2 and up, this should apply "
        "to all input.",
    ),
    "V2.2.2": (
        "L1",
        "Verify that the application is designed to enforce input validation at a "
        "trusted service layer. While client-side validation improves usability and "
        "should be encouraged, it must not be relied upon as a security control.",
    ),
    "V2.3.1": (
        "L1",
        "Verify that the application will only process business logic flows for the "
        "same user in the expected sequential step order and without skipping steps.",
    ),
    "V4.1.1": (
        "L1",
        "Verify that every HTTP response with a message body contains a Content-Type "
        "header field that matches the actual content of the response, including the "
        "charset parameter to specify safe character encoding (e.g., UTF-8, "
        "ISO-8859-1) according to IANA Media Types, such as \"text/\", \"/+xml\" "
        "and \"/xml\".",
    ),
    "V5.3.2": (
        "L1",
        "Verify that when the application creates file paths for file operations, "
        "instead of user-submitted filenames, it uses internally generated or "
        "trusted data, or if user-submitted filenames or file metadata must be used, "
        "strict validation and sanitization must be applied. This is to protect "
        "against path traversal, local or remote file inclusion (LFI, RFI), and "
        "server-side request forgery (SSRF) attacks.",
    ),
    "V8.3.1": (
        "L1",
        "Verify that the application enforces authorization rules at a trusted "
        "service layer and doesn't rely on controls that an untrusted consumer could "
        "manipulate, such as client-side JavaScript.",
    ),
    "V11.4.1": (
        "L1",
        "Verify that only approved hash functions are used for general cryptographic "
        "use cases, including digital signatures, HMAC, KDF, and random bit "
        "generation. Disallowed hash functions, such as MD5, must not be used for "
        "any cryptographic purpose.",
    ),
    "V12.1.1": (
        "L1",
        "Verify that only the latest recommended versions of the TLS protocol are "
        "enabled, such as TLS 1.2 and TLS 1.3. The latest version of the TLS protocol "
        "must be the preferred option.",
    ),
    "V12.2.1": (
        "L1",
        "Verify that TLS is used for all connectivity between a client and external "
        "facing, HTTP-based services, and does not fall back to insecure or "
        "unencrypted communications.",
    ),
    "V12.2.2": (
        "L1",
        "Verify that external facing services use publicly trusted TLS certificates.",
    ),
    "V13.2.4": (
        "L2",
        "Verify that an allowlist is used to define the external resources or "
        "systems with which the application is permitted to communicate (e.g., for "
        "outbound requests, data loads, or file access). This allowlist can be "
        "implemented at the application layer, web server, firewall, or a "
        "combination of different layers.",
    ),
    "V14.2.1": (
        "L1",
        "Verify that sensitive data is only sent to the server in the HTTP message "
        "body or header fields, and that the URL and query string do not contain "
        "sensitive information, such as an API key or session token.",
    ),
    "V15.3.1": (
        "L1",
        "Verify that the application only returns the required subset of fields from "
        "a data object. For example, it should not return an entire data object, as "
        "some individual fields should not be accessible to users.",
    ),
    "V15.3.2": (
        "L2",
        "Verify that where the application backend makes calls to external URLs, it "
        "is configured to not follow redirects unless it is intended functionality.",
    ),
    "V16.5.2": (
        "L2",
        "Verify that the application continues to operate securely when external "
        "resource access fails, for example, by using patterns such as circuit "
        "breakers or graceful degradation.",
    ),
}
ASVS_CONTROLS = MappingProxyType(
    {
        control_id: MappingProxyType(
            {"id": control_id, "level": level, "description": description}
        )
        for control_id, (level, description) in _ASVS_CONTROL_ROWS.items()
    }
)
_EXPECTED_PLAN_NAMES = tuple(f"02-{number:02d}-PLAN.md" for number in range(1, 11))
_PLAN_DIRECTORY = ".planning/phases/02-advisory-api-and-secondary-evidence"
_SEVERITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
_CONTROL_ID = re.compile(r"V\d+\.\d+\.\d+")
_THREAT_RECORD_KEYS = {
    "asvs_controls",
    "category",
    "component",
    "disposition",
    "initial_severity",
    "mitigation_command_ids",
    "owner_plan",
    "plan_asvs",
    "rationale",
    "residual_severity",
    "threat_id",
}


@dataclass(frozen=True, slots=True)
class SecurityGateViolation:
    kind: str
    message: str
    path: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "message": self.message,
            "path": self.path,
        }


@dataclass(frozen=True, slots=True)
class SecurityGateReport:
    root: Path
    register_path: Path
    threat_count: int
    mitigation_command_count: int
    highest_residual: str
    unresolved: tuple[dict[str, str], ...]
    violations: tuple[SecurityGateViolation, ...]

    @property
    def ok(self) -> bool:
        return not self.violations and not self.unresolved

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "ok": self.ok,
            "root": self.root.as_posix(),
            "register": self.register_path.as_posix(),
            "threat_count": self.threat_count,
            "mitigation_command_count": self.mitigation_command_count,
            "highest_residual": self.highest_residual,
            "unresolved": [dict(item) for item in self.unresolved],
            "violations": [item.as_dict() for item in self.violations],
        }


class _RegisterError(ValueError):
    def __init__(self, kind: str, message: str, path: str = "") -> None:
        super().__init__(message)
        self.kind = kind
        self.path = path


def evaluate_security_gate(
    root: str | Path,
    register_path: str | Path,
    *,
    check_structure: bool = False,
    run_mitigations: bool = False,
) -> SecurityGateReport:
    """Evaluate the checked register without fetching or mutating external state."""

    root_path = Path(root).resolve()
    checked_path = Path(register_path)
    if not checked_path.is_absolute():
        checked_path = root_path / checked_path
    checked_path = checked_path.resolve()
    violations: list[SecurityGateViolation] = []
    threat_count = 0
    command_count = 0
    highest_residual = "LOW"
    unresolved: tuple[dict[str, str], ...] = ()
    try:
        register = _load_register(checked_path)
        commands = _validate_register_shape(register)
        command_count = len(commands)
        raw_threats = register.get("threats")
        if not isinstance(raw_threats, list):
            raise _RegisterError(
                "threat_inventory_mismatch",
                "threats must be an array",
            )
        prevalidated_threats = tuple(
            _validate_threat_record(raw_threat, commands)
            for raw_threat in raw_threats
        )
        threat_count = len(prevalidated_threats)
        highest_residual, unresolved = _residual_status(prevalidated_threats)
        threats = _validate_threats(root_path, register, commands)
        threat_count = len(threats)
        highest_residual, unresolved = _residual_status(threats)
        if check_structure or run_mitigations:
            _check_mitigation_structure(root_path, commands)
        if run_mitigations:
            _run_mitigation_commands(root_path, commands)
    except _RegisterError as exc:
        violations.append(
            SecurityGateViolation(exc.kind, str(exc), exc.path)
        )
    return SecurityGateReport(
        root=root_path,
        register_path=checked_path,
        threat_count=threat_count,
        mitigation_command_count=command_count,
        highest_residual=highest_residual,
        unresolved=unresolved,
        violations=tuple(violations),
    )


def _load_register(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise _RegisterError(
            "register_invalid",
            "Phase 2 security register is not strict JSON",
            path.as_posix(),
        ) from exc
    if not isinstance(payload, dict):
        raise _RegisterError("register_invalid", "security register must be an object")
    return payload


def _validate_register_shape(
    register: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    expected_keys = {
        "asvs",
        "mitigation_commands",
        "register_kind",
        "schema_version",
        "threats",
    }
    if set(register) != expected_keys:
        raise _RegisterError("register_invalid", "security register fields are not exact")
    if register.get("schema_version") != SCHEMA_VERSION:
        raise _RegisterError("register_invalid", "security register schema is not exact")
    if register.get("register_kind") != REGISTER_KIND:
        raise _RegisterError("register_invalid", "security register kind is not exact")
    _validate_asvs(register.get("asvs"))
    raw_commands = register.get("mitigation_commands")
    if not isinstance(raw_commands, list) or not raw_commands:
        raise _RegisterError(
            "mitigation_command_missing",
            "mitigation command inventory must be a non-empty array",
        )
    commands: dict[str, dict[str, object]] = {}
    for raw_command in raw_commands:
        command = _validate_command(raw_command)
        command_id = str(command["id"])
        if command_id in commands:
            raise _RegisterError(
                "mitigation_command_invalid",
                f"duplicate mitigation command: {command_id}",
            )
        commands[command_id] = command
    if tuple(commands) != tuple(sorted(commands)):
        raise _RegisterError(
            "mitigation_command_invalid",
            "mitigation commands must be sorted by ID",
        )
    return commands


def _validate_asvs(raw_asvs: object) -> None:
    if not isinstance(raw_asvs, dict) or set(raw_asvs) != {
        "applicable_l1_ids",
        "controls",
        "project_l2_ids",
        "source_url",
        "version",
    }:
        raise _RegisterError(
            "asvs_control_inventory_invalid",
            "ASVS register fields are not exact",
        )
    if raw_asvs.get("version") != "5.0.0" or raw_asvs.get("source_url") != ASVS_SOURCE_URL:
        raise _RegisterError(
            "asvs_control_inventory_invalid",
            "ASVS version or tagged source URL is not exact",
        )
    if tuple(raw_asvs.get("applicable_l1_ids", ())) != APPLICABLE_L1_IDS:
        raise _RegisterError(
            "asvs_control_inventory_invalid",
            "applicable ASVS L1 inventory is not exact",
        )
    if tuple(raw_asvs.get("project_l2_ids", ())) != PROJECT_L2_IDS:
        raise _RegisterError(
            "asvs_control_inventory_invalid",
            "ASVS L2 project-control inventory is not exact",
        )
    expected_controls = [dict(ASVS_CONTROLS[item]) for item in sorted(ASVS_CONTROLS)]
    if raw_asvs.get("controls") != expected_controls:
        raise _RegisterError(
            "asvs_control_inventory_invalid",
            "ASVS control IDs, levels, or descriptions are not exact",
        )


def _validate_command(raw_command: object) -> dict[str, object]:
    if not isinstance(raw_command, dict):
        raise _RegisterError("mitigation_command_invalid", "command must be an object")
    kind = raw_command.get("kind")
    expected = (
        {"description", "id", "kind", "targets"}
        if kind == "pytest"
        else {"arguments", "description", "id", "kind", "module"}
    )
    if kind not in {"pytest", "static"} or set(raw_command) != expected:
        raise _RegisterError(
            "mitigation_command_invalid",
            "mitigation command fields or kind are not exact",
        )
    command_id = raw_command.get("id")
    description = raw_command.get("description")
    if not isinstance(command_id, str) or not command_id or not isinstance(description, str) or not description:
        raise _RegisterError(
            "mitigation_command_invalid",
            "mitigation command ID and description must be non-empty strings",
        )
    if kind == "pytest":
        targets = raw_command.get("targets")
        if not isinstance(targets, list) or not targets or any(
            not isinstance(target, str) or not target for target in targets
        ):
            raise _RegisterError(
                "mitigation_command_invalid",
                f"pytest command {command_id} has invalid targets",
            )
    else:
        module = raw_command.get("module")
        arguments = raw_command.get("arguments")
        if not isinstance(module, str) or not module.startswith("options_copilot."):
            raise _RegisterError(
                "mitigation_command_invalid",
                f"static command {command_id} has an invalid module",
            )
        if not isinstance(arguments, list) or any(
            not isinstance(argument, str) for argument in arguments
        ):
            raise _RegisterError(
                "mitigation_command_invalid",
                f"static command {command_id} has invalid arguments",
            )
    return dict(raw_command)


def _validate_threats(
    root: Path,
    register: Mapping[str, object],
    commands: Mapping[str, Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    expected = _read_plan_threats(root)
    raw_threats = register.get("threats")
    if not isinstance(raw_threats, list):
        raise _RegisterError("threat_inventory_mismatch", "threats must be an array")
    actual: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for raw_threat in raw_threats:
        threat = _validate_threat_record(raw_threat, commands)
        key = _threat_key(threat)
        if key in actual:
            raise _RegisterError(
                "threat_inventory_mismatch",
                f"duplicate threat record: {'/'.join(key)}",
            )
        actual[key] = threat
    if tuple(actual) != tuple(sorted(actual)):
        raise _RegisterError(
            "threat_inventory_mismatch",
            "threat records must be sorted by plan, ID, category, and component",
        )
    if set(actual) != set(expected):
        missing = sorted(set(expected).difference(actual))
        surplus = sorted(set(actual).difference(expected))
        raise _RegisterError(
            "threat_inventory_mismatch",
            f"plan threat inventory mismatch; missing={missing}, surplus={surplus}",
        )
    for key, plan_row in expected.items():
        threat = actual[key]
        for field in (
            "category",
            "component",
            "disposition",
            "owner_plan",
            "plan_asvs",
            "residual_severity",
        ):
            if threat[field] != plan_row[field]:
                raise _RegisterError(
                    "threat_inventory_mismatch",
                    f"{key[0]} {key[1]} field {field} differs from immutable plan",
                )
        if threat["rationale"] != plan_row["rationale"]:
            raise _RegisterError(
                "threat_inventory_mismatch",
                f"{key[0]} {key[1]} rationale differs from immutable plan",
            )
        mapped_ids = {
            str(item["id"])
            for item in threat["asvs_controls"]
            if isinstance(item, dict)
        }
        explicit_ids = set(_CONTROL_ID.findall(str(plan_row["plan_asvs"])))
        if not explicit_ids.issubset(mapped_ids):
            raise _RegisterError(
                "threat_inventory_mismatch",
                f"{key[0]} {key[1]} omits an ASVS control named by its plan",
            )
        if "relevant L2 project controls" in str(plan_row["plan_asvs"]) and not set(
            PROJECT_L2_IDS
        ).issubset(mapped_ids):
            raise _RegisterError(
                "threat_inventory_mismatch",
                f"{key[0]} {key[1]} does not enumerate all relevant L2 controls",
            )
    return tuple(actual.values())


def _validate_threat_record(
    raw_threat: object,
    commands: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    if not isinstance(raw_threat, dict) or set(raw_threat) != _THREAT_RECORD_KEYS:
        raise _RegisterError(
            "threat_inventory_mismatch",
            "threat record fields are not exact",
        )
    for field in (
        "category",
        "component",
        "disposition",
        "owner_plan",
        "plan_asvs",
        "rationale",
        "threat_id",
    ):
        if not isinstance(raw_threat.get(field), str) or not raw_threat[field]:
            raise _RegisterError(
                "threat_inventory_mismatch",
                f"threat field {field} must be a non-empty string",
            )
    initial = raw_threat.get("initial_severity")
    residual = raw_threat.get("residual_severity")
    if initial not in _SEVERITY_ORDER or residual not in _SEVERITY_ORDER:
        raise _RegisterError(
            "threat_inventory_mismatch",
            "threat severity must be LOW, MEDIUM, HIGH, or CRITICAL",
        )
    if _SEVERITY_ORDER[str(initial)] < _SEVERITY_ORDER[str(residual)]:
        raise _RegisterError(
            "threat_inventory_mismatch",
            "initial severity cannot be lower than residual severity",
        )
    raw_controls = raw_threat.get("asvs_controls")
    if not isinstance(raw_controls, list) or not raw_controls:
        raise _RegisterError(
            "threat_inventory_mismatch",
            "every threat must map at least one exact ASVS control",
        )
    normalized_controls: list[dict[str, str]] = []
    for raw_control in raw_controls:
        if not isinstance(raw_control, dict) or set(raw_control) != {"id", "level"}:
            raise _RegisterError(
                "threat_inventory_mismatch",
                "threat ASVS mappings must contain exact ID and level",
            )
        control_id = raw_control.get("id")
        level = raw_control.get("level")
        expected = ASVS_CONTROLS.get(str(control_id))
        if expected is None or level != expected["level"]:
            raise _RegisterError(
                "asvs_control_inventory_invalid",
                f"unknown or mislabeled threat control: {control_id} {level}",
            )
        normalized_controls.append({"id": str(control_id), "level": str(level)})
    if normalized_controls != sorted(normalized_controls, key=lambda item: item["id"]):
        raise _RegisterError(
            "threat_inventory_mismatch",
            "threat ASVS mappings must be sorted by ID",
        )
    raw_command_ids = raw_threat.get("mitigation_command_ids")
    if not isinstance(raw_command_ids, list) or not raw_command_ids or any(
        not isinstance(command_id, str) for command_id in raw_command_ids
    ):
        raise _RegisterError(
            "mitigation_command_missing",
            "every threat must name at least one mitigation command",
        )
    missing_commands = sorted(set(raw_command_ids).difference(commands))
    if missing_commands:
        raise _RegisterError(
            "mitigation_command_missing",
            f"threat references missing mitigation commands: {missing_commands}",
        )
    return dict(raw_threat)


def _read_plan_threats(root: Path) -> dict[tuple[str, str, str, str], dict[str, str]]:
    phase_directory = root / _PLAN_DIRECTORY
    actual_names = tuple(
        path.name for path in sorted(phase_directory.glob("02-??-PLAN.md"))
    )
    if actual_names != _EXPECTED_PLAN_NAMES:
        raise _RegisterError(
            "threat_inventory_mismatch",
            "final Phase 2 plan inventory is not exactly 02-01 through 02-10",
        )
    rows: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for name in _EXPECTED_PLAN_NAMES:
        owner_plan = name.removesuffix("-PLAN.md")
        text = (phase_directory / name).read_text(encoding="utf-8")
        marker = "## STRIDE Threat Register"
        if marker not in text:
            raise _RegisterError(
                "threat_inventory_mismatch",
                f"{name} has no STRIDE threat register",
            )
        section = text.split(marker, 1)[1]
        section = section.split("\n## ", 1)[0]
        plan_rows = 0
        for line in section.splitlines():
            if not line.startswith("| T-02-"):
                continue
            fields = [field.strip() for field in line.strip().strip("|").split("|")]
            if len(fields) != 7:
                raise _RegisterError(
                    "threat_inventory_mismatch",
                    f"{name} contains a malformed threat row",
                )
            threat_id, category, component, disposition, asvs, residual, rationale = fields
            row = {
                "threat_id": threat_id,
                "category": category,
                "component": component,
                "disposition": disposition,
                "owner_plan": owner_plan,
                "plan_asvs": asvs,
                "residual_severity": residual.upper(),
                "rationale": rationale,
            }
            key = _threat_key(row)
            if key in rows:
                raise _RegisterError(
                    "threat_inventory_mismatch",
                    f"duplicate immutable plan threat row: {'/'.join(key)}",
                )
            rows[key] = row
            plan_rows += 1
        if not plan_rows:
            raise _RegisterError(
                "threat_inventory_mismatch",
                f"{name} has an empty STRIDE threat register",
            )
    return dict(sorted(rows.items()))


def _threat_key(threat: Mapping[str, object]) -> tuple[str, str, str, str]:
    return (
        str(threat["owner_plan"]),
        str(threat["threat_id"]),
        str(threat["category"]),
        str(threat["component"]),
    )


def _residual_status(
    threats: Sequence[Mapping[str, object]],
) -> tuple[str, tuple[dict[str, str], ...]]:
    highest = max(
        (str(threat["residual_severity"]) for threat in threats),
        key=_SEVERITY_ORDER.__getitem__,
        default="LOW",
    )
    unresolved = tuple(
        {
            "owner_plan": str(threat["owner_plan"]),
            "threat_id": str(threat["threat_id"]),
            "component": str(threat["component"]),
            "residual_severity": str(threat["residual_severity"]),
        }
        for threat in threats
        if _SEVERITY_ORDER[str(threat["residual_severity"])] >= _SEVERITY_ORDER["HIGH"]
    )
    return highest, unresolved


def _check_mitigation_structure(
    root: Path,
    commands: Mapping[str, Mapping[str, object]],
) -> None:
    pytest_targets: list[str] = []
    for command_id, command in commands.items():
        if command["kind"] == "pytest":
            for target in command["targets"]:
                target_text = str(target)
                relative = target_text.split("::", 1)[0]
                target_path = (root / relative).resolve()
                try:
                    target_path.relative_to(root)
                except ValueError as exc:
                    raise _RegisterError(
                        "mitigation_target_missing",
                        f"mitigation target escapes repository: {target_text}",
                        target_text,
                    ) from exc
                if not relative.startswith("tests/options_copilot/") or not target_path.is_file():
                    raise _RegisterError(
                        "mitigation_target_missing",
                        f"mitigation target does not exist: {target_text}",
                        target_text,
                    )
                pytest_targets.append(target_text)
        else:
            module = str(command["module"])
            module_path = root / (module.replace(".", "/") + ".py")
            package_path = root / module.replace(".", "/") / "__init__.py"
            if not module_path.is_file() and not package_path.is_file():
                raise _RegisterError(
                    "mitigation_target_missing",
                    f"static mitigation module does not exist: {module}",
                    module,
                )
    if not pytest_targets:
        return
    environment = dict(os.environ)
    environment["OPTIONS_COPILOT_NETWORK_DENIED"] = "1"
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q", *pytest_targets],
            cwd=root,
            capture_output=True,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=45,
            env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        raise _RegisterError(
            "mitigation_collection_failed",
            "mitigation test collection exceeded 45 seconds",
        ) from exc
    if completed.returncode != 0:
        raise _RegisterError(
            "mitigation_collection_failed",
            f"mitigation test collection failed with exit {completed.returncode}",
        )


def _run_mitigation_commands(
    root: Path,
    commands: Mapping[str, Mapping[str, object]],
) -> None:
    environment = dict(os.environ)
    environment["OPTIONS_COPILOT_NETWORK_DENIED"] = "1"
    for command_id, command in commands.items():
        if command["kind"] == "pytest":
            invocation = [
                sys.executable,
                str(root / "scripts" / "verify_phase2_red_contracts.py"),
                "--root",
                str(root),
                "--expect-green",
                "--timeout-seconds",
                "59",
                "--tests",
                *(str(target) for target in command["targets"]),
            ]
        else:
            invocation = [
                sys.executable,
                "-m",
                str(command["module"]),
                *(str(argument) for argument in command["arguments"]),
            ]
        try:
            completed = subprocess.run(
                invocation,
                cwd=root,
                capture_output=True,
                check=False,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=180,
                env=environment,
            )
        except subprocess.TimeoutExpired as exc:
            raise _RegisterError(
                "mitigation_execution_failed",
                f"mitigation command timed out: {command_id}",
            ) from exc
        if completed.returncode != 0:
            # Captured test/static output can contain hostile provider fixtures;
            # expose only the immutable command ID and status at this boundary.
            raise _RegisterError(
                "mitigation_execution_failed",
                f"mitigation command failed: {command_id}",
            )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant: {value}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify Phase 2 ASVS traceability and residual risk"
    )
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--register", type=Path)
    parser.add_argument("--json", action="store_true")
    execution = parser.add_mutually_exclusive_group()
    execution.add_argument("--check-structure", action="store_true")
    execution.add_argument("--run-mitigations", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _build_parser().parse_args(argv)
    root = arguments.root.resolve()
    register = arguments.register or (
        root / "options_copilot" / "operations" / "phase2_security_controls.json"
    )
    report = evaluate_security_gate(
        root,
        register,
        check_structure=arguments.check_structure,
        run_mitigations=arguments.run_mitigations,
    )
    payload = report.as_dict()
    if arguments.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    elif report.ok:
        print(
            "Phase 2 security gate passed: "
            f"{report.threat_count} threats, residual {report.highest_residual}"
        )
    else:
        for violation in report.violations:
            print(f"{violation.kind}: {violation.message}")
        for unresolved in report.unresolved:
            print(
                "unresolved_residual: "
                f"{unresolved['owner_plan']} {unresolved['threat_id']} "
                f"{unresolved['residual_severity']}"
            )
    return 0 if report.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "APPLICABLE_L1_IDS",
    "ASVS_CONTROLS",
    "ASVS_SOURCE_URL",
    "PROJECT_L2_IDS",
    "SecurityGateReport",
    "SecurityGateViolation",
    "evaluate_security_gate",
    "main",
]
