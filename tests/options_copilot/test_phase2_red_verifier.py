"""Contract tests for the import-safe Phase 2 expected-RED verifier."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
VERIFIER = REPO_ROOT / "scripts" / "verify_phase2_red_contracts.py"


def _test_module(tmp_path: Path, source: str, *, name: str = "test_contract.py") -> Path:
    # Isolate node IDs from the repository even when basetemp is on project G:.
    (tmp_path / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    return path


def _run_verifier(
    tmp_path: Path,
    *tests: Path,
    mode: str,
    markers: tuple[str, ...] = (),
    timeout_seconds: float = 10.0,
) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
    assert VERIFIER.is_file(), "Phase 2 expected-RED verifier is missing"
    arguments = [
        sys.executable,
        str(VERIFIER),
        "--root",
        str(tmp_path),
        f"--expect-{mode}",
        "--timeout-seconds",
        str(timeout_seconds),
    ]
    for marker in markers:
        arguments.extend(("--marker", marker))
    arguments.append("--tests")
    arguments.extend(path.name for path in tests)
    completed = subprocess.run(
        arguments,
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    payload = json.loads(completed.stdout)
    assert isinstance(payload, dict)
    return completed, payload


def test_expected_red_accepts_only_executed_named_assertion(tmp_path: Path) -> None:
    test_path = _test_module(
        tmp_path,
        """
def test_future_contract():
    assert False, "PHASE2_EXPECTED_RED:ADVISORY_CONTRACT"
""",
    )

    completed, payload = _run_verifier(
        tmp_path,
        test_path,
        mode="red",
        markers=("ADVISORY_CONTRACT",),
    )

    assert completed.returncode == 0, completed.stderr
    assert payload == {
        "collected": 1,
        "executed": 1,
        "failed": 1,
        "marker_coverage": ["ADVISORY_CONTRACT"],
        "mode": "expected-red",
        "node_ids": ["test_contract.py::test_future_contract"],
        "ok": True,
        "reason": "EXPECTED_RED_CONFIRMED",
    }


def test_expected_red_requires_every_requested_marker(tmp_path: Path) -> None:
    first = _test_module(
        tmp_path,
        """
def test_first_contract():
    assert False, "PHASE2_EXPECTED_RED:ADVISORY_CONTRACT"

def test_second_contract():
    assert False, "PHASE2_EXPECTED_RED:ZERO_INFLUENCE"
""",
    )

    completed, payload = _run_verifier(
        tmp_path,
        first,
        mode="red",
        markers=("ADVISORY_CONTRACT", "ZERO_INFLUENCE"),
    )

    assert completed.returncode == 0
    assert payload["marker_coverage"] == [
        "ADVISORY_CONTRACT",
        "ZERO_INFLUENCE",
    ]
    assert payload["failed"] == 2


@pytest.mark.parametrize(
    ("name", "source", "reason"),
    (
        (
            "test_import.py",
            "import phase2_dependency_that_does_not_exist\n",
            "IMPORT_ERROR",
        ),
        (
            "test_syntax.py",
            "def test_broken(\n",
            "SYNTAX_ERROR",
        ),
        (
            "test_fixture.py",
            "def test_fixture_contract(missing_phase2_fixture):\n    pass\n",
            "FIXTURE_ERROR",
        ),
        (
            "test_unexpected.py",
            "def test_unexpected():\n    assert False, 'ordinary failure'\n",
            "UNEXPECTED_FAILURE",
        ),
        (
            "test_skip.py",
            "import pytest\ndef test_skip():\n    pytest.skip('not ready')\n",
            "SKIP_FORBIDDEN",
        ),
        (
            "test_xfail.py",
            (
                "import pytest\n"
                "@pytest.mark.xfail(reason='not ready')\n"
                "def test_xfail():\n    assert False\n"
            ),
            "XFAIL_FORBIDDEN",
        ),
    ),
)
def test_expected_red_rejects_infrastructure_or_accidental_failures(
    tmp_path: Path,
    name: str,
    source: str,
    reason: str,
) -> None:
    test_path = _test_module(tmp_path, source, name=name)

    completed, payload = _run_verifier(
        tmp_path,
        test_path,
        mode="red",
        markers=("ADVISORY_CONTRACT",),
    )

    assert completed.returncode == 2
    assert payload["ok"] is False
    assert payload["reason"] == reason
    assert "traceback" not in json.dumps(payload).casefold()


def test_expected_red_rejects_accidental_network_attempt(tmp_path: Path) -> None:
    test_path = _test_module(
        tmp_path,
        """
import os
import socket

def test_network_attempt():
    assert os.environ["OPTIONS_COPILOT_NETWORK_DENIED"] == "1"
    socket.create_connection(("example.com", 443), timeout=0.1)
""",
    )

    completed, payload = _run_verifier(
        tmp_path,
        test_path,
        mode="red",
        markers=("ADVISORY_CONTRACT",),
    )

    assert completed.returncode == 2
    assert payload["reason"] == "NETWORK_ATTEMPT"


def test_expected_red_rejects_timeout_below_sixty_seconds(tmp_path: Path) -> None:
    test_path = _test_module(
        tmp_path,
        "import time\ndef test_hang():\n    time.sleep(5)\n",
    )

    completed, payload = _run_verifier(
        tmp_path,
        test_path,
        mode="red",
        markers=("ADVISORY_CONTRACT",),
        timeout_seconds=0.25,
    )

    assert completed.returncode == 2
    assert payload["reason"] == "TIMEOUT"
    assert payload["timeout_seconds"] == 0.25


def test_expected_red_rejects_passing_or_incomplete_marker_set(
    tmp_path: Path,
) -> None:
    test_path = _test_module(tmp_path, "def test_green():\n    assert True\n")

    completed, payload = _run_verifier(
        tmp_path,
        test_path,
        mode="red",
        markers=("ADVISORY_CONTRACT",),
    )

    assert completed.returncode == 2
    assert payload["reason"] == "EXPECTED_RED_MISSING"


def test_green_mode_requires_all_selected_tests_to_execute_and_pass(
    tmp_path: Path,
) -> None:
    test_path = _test_module(
        tmp_path,
        "def test_one():\n    assert True\n\ndef test_two():\n    assert 2 + 2 == 4\n",
    )

    completed, payload = _run_verifier(
        tmp_path,
        test_path,
        mode="green",
    )

    assert completed.returncode == 0
    assert payload == {
        "collected": 2,
        "executed": 2,
        "failed": 0,
        "marker_coverage": [],
        "mode": "green",
        "node_ids": [
            "test_contract.py::test_one",
            "test_contract.py::test_two",
        ],
        "ok": True,
        "reason": "GREEN_CONFIRMED",
    }


def test_green_mode_rejects_failure_skip_and_xfail(tmp_path: Path) -> None:
    failure = _test_module(
        tmp_path,
        "def test_failure():\n    assert False, 'not green'\n",
    )
    completed, payload = _run_verifier(tmp_path, failure, mode="green")
    assert completed.returncode == 2
    assert payload["reason"] == "TEST_FAILURE"

    skipped = _test_module(
        tmp_path,
        "import pytest\ndef test_skip():\n    pytest.skip('not green')\n",
    )
    completed, payload = _run_verifier(tmp_path, skipped, mode="green")
    assert completed.returncode == 2
    assert payload["reason"] == "SKIP_FORBIDDEN"
