"""Contract tests for the Phase 2 ASVS residual-risk gate."""
from __future__ import annotations

from copy import deepcopy
import importlib
import json
from pathlib import Path
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTER_PATH = (
    REPO_ROOT / "options_copilot" / "operations" / "phase2_security_controls.json"
)
ASVS_SOURCE_URL = (
    "https://raw.githubusercontent.com/OWASP/ASVS/v5.0.0/5.0/docs_en/"
    "OWASP_Application_Security_Verification_Standard_5.0.0_en.flat.json"
)
EXPECTED_L1_IDS = (
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
EXPECTED_PROJECT_L2_IDS = (
    "V1.3.6",
    "V13.2.4",
    "V15.3.2",
    "V16.5.2",
)


def _gate_module():
    try:
        return importlib.import_module(
            "options_copilot.operations.phase2_security_gate"
        )
    except ModuleNotFoundError:
        pytest.fail("Phase 2 security gate module is missing")


def _checked_register() -> dict[str, object]:
    assert REGISTER_PATH.is_file(), "Phase 2 security register is missing"
    payload = json.loads(REGISTER_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _candidate_register(
    tmp_path: Path,
    payload: dict[str, object],
) -> Path:
    path = tmp_path / "phase2_security_controls.json"
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return path


def _report(
    register_path: Path,
    *,
    check_structure: bool = False,
) -> dict[str, object]:
    report = _gate_module().evaluate_security_gate(
        REPO_ROOT,
        register_path,
        check_structure=check_structure,
    )
    payload = report.as_dict()
    assert isinstance(payload, dict)
    return payload


def _violation_kinds(report: dict[str, object]) -> set[str]:
    violations = report["violations"]
    assert isinstance(violations, list)
    return {
        str(violation["kind"])
        for violation in violations
        if isinstance(violation, dict)
    }


def test_phase2_security_register_pins_exact_asvs_500_inventory() -> None:
    payload = _checked_register()
    asvs = payload["asvs"]
    assert isinstance(asvs, dict)
    assert asvs["version"] == "5.0.0"
    assert asvs["source_url"] == ASVS_SOURCE_URL
    assert tuple(asvs["applicable_l1_ids"]) == EXPECTED_L1_IDS
    assert tuple(asvs["project_l2_ids"]) == EXPECTED_PROJECT_L2_IDS
    controls = asvs["controls"]
    assert isinstance(controls, list)
    assert {
        str(control["id"]): str(control["level"])
        for control in controls
        if isinstance(control, dict)
    } == {
        **{control_id: "L1" for control_id in EXPECTED_L1_IDS},
        **{control_id: "L2" for control_id in EXPECTED_PROJECT_L2_IDS},
    }
    assert all(
        isinstance(control, dict)
        and isinstance(control.get("description"), str)
        and bool(control["description"].strip())
        for control in controls
    )


def test_phase2_security_gate_clean_register_is_nonblocking_and_collects() -> None:
    report = _report(REGISTER_PATH, check_structure=True)

    assert report["ok"] is True
    assert report["unresolved"] == []
    assert report["highest_residual"] in {"LOW", "MEDIUM"}
    assert report["violations"] == []
    assert int(report["threat_count"]) > 0
    assert int(report["mitigation_command_count"]) > 0


@pytest.mark.parametrize(
    ("mutation", "expected_kind"),
    (
        ("invented_control", "asvs_control_inventory_invalid"),
        ("l2_mislabeled_l1", "asvs_control_inventory_invalid"),
        ("missing_threat", "threat_inventory_mismatch"),
        ("missing_command", "mitigation_command_missing"),
        ("missing_target", "mitigation_target_missing"),
    ),
)
def test_phase2_security_gate_rejects_structural_mismatch(
    tmp_path: Path,
    mutation: str,
    expected_kind: str,
) -> None:
    payload = deepcopy(_checked_register())
    asvs = payload["asvs"]
    threats = payload["threats"]
    commands = payload["mitigation_commands"]
    assert isinstance(asvs, dict)
    assert isinstance(threats, list)
    assert isinstance(commands, list)
    if mutation == "invented_control":
        controls = asvs["controls"]
        assert isinstance(controls, list)
        controls.append(
            {
                "id": "V99.9.9",
                "level": "L1",
                "description": "Invented control",
            }
        )
    elif mutation == "l2_mislabeled_l1":
        controls = asvs["controls"]
        assert isinstance(controls, list)
        control = next(
            item
            for item in controls
            if isinstance(item, dict) and item.get("id") == "V1.3.6"
        )
        control["level"] = "L1"
    elif mutation == "missing_threat":
        threats.pop()
    elif mutation == "missing_command":
        threat = threats[0]
        assert isinstance(threat, dict)
        threat["mitigation_command_ids"] = ["does_not_exist"]
    else:
        command = next(
            item
            for item in commands
            if isinstance(item, dict) and item.get("kind") == "pytest"
        )
        command["targets"] = [
            "tests/options_copilot/test_missing_phase2_security_target.py"
        ]

    report = _report(
        _candidate_register(tmp_path, payload),
        check_structure=mutation == "missing_target",
    )

    assert report["ok"] is False
    assert expected_kind in _violation_kinds(report)


@pytest.mark.parametrize("severity", ("HIGH", "CRITICAL"))
def test_phase2_security_gate_blocks_high_or_critical_residual(
    tmp_path: Path,
    severity: str,
) -> None:
    payload = deepcopy(_checked_register())
    threats = payload["threats"]
    assert isinstance(threats, list)
    threat = threats[0]
    assert isinstance(threat, dict)
    threat["initial_severity"] = severity
    threat["residual_severity"] = severity
    candidate = _candidate_register(tmp_path, payload)

    report = _report(candidate)
    exit_code = _gate_module().main(
        [
            "--root",
            str(REPO_ROOT),
            "--register",
            str(candidate),
            "--json",
        ]
    )

    assert report["ok"] is False
    assert report["highest_residual"] == severity
    assert report["unresolved"]
    assert exit_code == 2


def test_phase2_security_cli_emits_clean_machine_readable_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = _gate_module().main(
        [
            "--root",
            str(REPO_ROOT),
            "--register",
            str(REGISTER_PATH),
            "--json",
            "--check-structure",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["ok"] is True
    assert output["unresolved"] == []
    assert output["highest_residual"] in {"LOW", "MEDIUM"}


def test_phase2_security_cli_run_mitigations_executes_exact_inventory(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _gate_module()
    calls: list[tuple[list[str], dict[str, object]]] = []

    def completed(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", completed)
    exit_code = module.main(
        [
            "--root",
            str(REPO_ROOT),
            "--register",
            str(REGISTER_PATH),
            "--json",
            "--run-mitigations",
        ]
    )
    output = json.loads(capsys.readouterr().out)
    commands = _checked_register()["mitigation_commands"]
    assert isinstance(commands, list)

    assert exit_code == 0
    assert output["ok"] is True
    assert len(calls) == len(commands) + 1
    assert calls[0][0][2:5] == ["pytest", "--collect-only", "-q"]
    assert all(
        isinstance(kwargs["env"], dict)
        and kwargs["env"]["OPTIONS_COPILOT_NETWORK_DENIED"] == "1"
        and kwargs["capture_output"] is True
        for _command, kwargs in calls
    )
    for (command, _kwargs), item in zip(calls[1:], commands, strict=True):
        assert isinstance(item, dict)
        if item["kind"] == "pytest":
            assert command[1].endswith("scripts\\verify_phase2_red_contracts.py")
            assert "--expect-green" in command
            assert command[-len(item["targets"]):] == item["targets"]
        else:
            assert command[2] == item["module"]
