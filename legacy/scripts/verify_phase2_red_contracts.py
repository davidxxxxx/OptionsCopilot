"""Classify Phase 2 pytest contracts as expected RED or complete GREEN."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Mapping, Sequence


NETWORK_DENIED_ENV = "OPTIONS_COPILOT_NETWORK_DENIED"
DEFAULT_TIMEOUT_SECONDS = 45.0
MAX_TIMEOUT_SECONDS = 59.0
_MARKER = re.compile(r"^[A-Z][A-Z0-9_]*$")
_REPORT_PREFIX = "__PHASE2_PYTEST_REPORT__="

_PYTEST_HARNESS = r'''
from __future__ import annotations

import json
import os
import re
import socket
import sys

import pytest


NETWORK_TOKEN = "PHASE2_NETWORK_DENIED"
REPORT_PREFIX = "__PHASE2_PYTEST_REPORT__="
MARKER = re.compile(r"PHASE2_EXPECTED_RED:([A-Z][A-Z0-9_]*)")
os.environ["OPTIONS_COPILOT_NETWORK_DENIED"] = "1"


def _network_denied(*args, **kwargs):
    raise RuntimeError(NETWORK_TOKEN)


class _DeniedSocket(socket.socket):
    def connect(self, *args, **kwargs):
        raise RuntimeError(NETWORK_TOKEN)

    def connect_ex(self, *args, **kwargs):
        raise RuntimeError(NETWORK_TOKEN)


socket.create_connection = _network_denied
socket.socket = _DeniedSocket


class Recorder:
    def __init__(self):
        self.collected = []
        self.executed = []
        self.failed_calls = []
        self.setup_errors = []
        self.collection_errors = []
        self.skipped = []
        self.xfailed = []
        self.xpassed = []
        self.internal_errors = []

    def pytest_collection_modifyitems(self, session, config, items):
        self.collected = [item.nodeid for item in items]

    def pytest_collectreport(self, report):
        if report.failed:
            self.collection_errors.append(
                {"nodeid": report.nodeid, "detail": str(report.longrepr)}
            )

    def pytest_runtest_logreport(self, report):
        was_xfail = getattr(report, "wasxfail", None)
        if report.when == "call":
            self.executed.append(report.nodeid)
        if was_xfail is not None:
            target = self.xpassed if report.passed else self.xfailed
            target.append(report.nodeid)
            return
        if report.skipped:
            self.skipped.append(report.nodeid)
            return
        if not report.failed:
            return
        detail = str(report.longrepr)
        record = {
            "nodeid": report.nodeid,
            "markers": sorted(set(MARKER.findall(detail))),
            "network": NETWORK_TOKEN in detail,
            "detail": detail,
        }
        if report.when == "call":
            self.failed_calls.append(record)
        else:
            self.setup_errors.append(record)

    def pytest_internalerror(self, excrepr, excinfo):
        self.internal_errors.append(str(excrepr))


recorder = Recorder()
exit_code = pytest.main(sys.argv[1:], plugins=[recorder])
payload = {
    "exit_code": int(exit_code),
    "collected": recorder.collected,
    "executed": recorder.executed,
    "failed_calls": recorder.failed_calls,
    "setup_errors": recorder.setup_errors,
    "collection_errors": recorder.collection_errors,
    "skipped": recorder.skipped,
    "xfailed": recorder.xfailed,
    "xpassed": recorder.xpassed,
    "internal_errors": recorder.internal_errors,
}
print(REPORT_PREFIX + json.dumps(payload, ensure_ascii=False, sort_keys=True))
raise SystemExit(int(exit_code))
'''


class VerifierInputError(ValueError):
    """The verifier command line does not name a bounded test contract."""


def verify_contracts(
    root: str | Path,
    tests: Sequence[str],
    *,
    mode: str,
    markers: Sequence[str] = (),
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Run selected tests once and return a deterministic classification."""

    root_path = Path(root).resolve()
    if not root_path.is_dir():
        raise VerifierInputError("verifier root must be an existing directory")
    if mode not in {"expected-red", "green"}:
        raise VerifierInputError("mode must be expected-red or green")
    marker_set = _validate_markers(markers, mode=mode)
    selected_tests = _validate_test_targets(root_path, tests)
    timeout = _validate_timeout(timeout_seconds)
    environment = dict(os.environ)
    environment[NETWORK_DENIED_ENV] = "1"
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                _PYTEST_HARNESS,
                "-q",
                "--tb=short",
                "-ra",
                *selected_tests,
            ],
            cwd=root_path,
            capture_output=True,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=environment,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "mode": mode,
            "reason": "TIMEOUT",
            "timeout_seconds": timeout,
        }
    report = _extract_report(completed.stdout)
    if report is None:
        return _failure(mode, "INTERNAL_PYTEST_ERROR")
    return _classify_report(report, mode=mode, requested_markers=marker_set)


def _validate_markers(markers: Sequence[str], *, mode: str) -> tuple[str, ...]:
    normalized = tuple(str(marker).strip() for marker in markers)
    if len(normalized) != len(set(normalized)) or any(
        _MARKER.fullmatch(marker) is None for marker in normalized
    ):
        raise VerifierInputError("markers must be unique UPPER_SNAKE_CASE names")
    if mode == "expected-red" and not normalized:
        raise VerifierInputError("expected-red mode requires at least one marker")
    if mode == "green" and normalized:
        raise VerifierInputError("green mode does not accept expected-RED markers")
    return tuple(sorted(normalized))


def _validate_test_targets(root: Path, tests: Sequence[str]) -> tuple[str, ...]:
    if not tests:
        raise VerifierInputError("at least one explicit pytest target is required")
    normalized: list[str] = []
    for raw_target in tests:
        target = str(raw_target).strip().replace("\\", "/")
        if not target or any(character in target for character in "*?[]"):
            raise VerifierInputError("pytest targets must be explicit paths or node IDs")
        relative = target.split("::", 1)[0]
        relative_path = Path(relative)
        if relative_path.is_absolute() or relative_path.suffix.casefold() != ".py":
            raise VerifierInputError("pytest targets must be relative Python test files")
        resolved = (root / relative_path).resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise VerifierInputError("pytest target escapes verifier root") from exc
        if not resolved.is_file():
            raise VerifierInputError(f"pytest target does not exist: {relative}")
        normalized.append(target)
    if len(normalized) != len(set(normalized)):
        raise VerifierInputError("pytest targets must not be duplicated")
    return tuple(normalized)


def _validate_timeout(value: float) -> float:
    try:
        timeout = float(value)
    except (TypeError, ValueError) as exc:
        raise VerifierInputError("timeout must be a finite number below 60 seconds") from exc
    if not 0 < timeout <= MAX_TIMEOUT_SECONDS:
        raise VerifierInputError("timeout must be greater than zero and below 60 seconds")
    return timeout


def _extract_report(stdout: str) -> dict[str, object] | None:
    for line in reversed(stdout.splitlines()):
        if not line.startswith(_REPORT_PREFIX):
            continue
        try:
            payload = json.loads(line.removeprefix(_REPORT_PREFIX))
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None
    return None


def _classify_report(
    report: Mapping[str, object],
    *,
    mode: str,
    requested_markers: Sequence[str],
) -> dict[str, object]:
    collected = _strings(report.get("collected"))
    executed = _strings(report.get("executed"))
    failed_calls = _records(report.get("failed_calls"))
    setup_errors = _records(report.get("setup_errors"))
    collection_errors = _records(report.get("collection_errors"))
    skipped = _strings(report.get("skipped"))
    xfailed = _strings(report.get("xfailed"))
    xpassed = _strings(report.get("xpassed"))
    internal_errors = _strings(report.get("internal_errors"))
    exit_code = report.get("exit_code")
    if not isinstance(exit_code, int):
        return _failure(mode, "INTERNAL_PYTEST_ERROR")
    base = {
        "collected": len(collected),
        "executed": len(executed),
        "failed": len(failed_calls),
        "marker_coverage": _marker_coverage(failed_calls),
        "node_ids": sorted(
            str(item["nodeid"]) for item in failed_calls
        ) if mode == "expected-red" else sorted(executed),
    }
    if internal_errors:
        return _failure(mode, "INTERNAL_PYTEST_ERROR", base)
    if collection_errors:
        details = "\n".join(str(item.get("detail", "")) for item in collection_errors)
        if "SyntaxError" in details:
            return _failure(mode, "SYNTAX_ERROR", base)
        if "ImportError" in details or "ModuleNotFoundError" in details:
            return _failure(mode, "IMPORT_ERROR", base)
        return _failure(mode, "COLLECTION_ERROR", base)
    if setup_errors:
        details = "\n".join(str(item.get("detail", "")) for item in setup_errors)
        if "fixture" in details.casefold() and "not found" in details.casefold():
            return _failure(mode, "FIXTURE_ERROR", base)
        return _failure(mode, "SETUP_ERROR", base)
    if any(bool(item.get("network")) for item in failed_calls):
        return _failure(mode, "NETWORK_ATTEMPT", base)
    if xfailed or xpassed:
        return _failure(mode, "XFAIL_FORBIDDEN", base)
    if skipped:
        return _failure(mode, "SKIP_FORBIDDEN", base)
    if not collected:
        return _failure(mode, "NO_TESTS_COLLECTED", base)
    if mode == "green":
        if failed_calls or exit_code != 0:
            return _failure(mode, "TEST_FAILURE", base)
        if len(executed) != len(collected):
            return _failure(mode, "TESTS_NOT_EXECUTED", base)
        return _success(mode, "GREEN_CONFIRMED", base)
    requested = set(requested_markers)
    observed = set(base["marker_coverage"])
    if exit_code == 0 or not failed_calls:
        return _failure(mode, "EXPECTED_RED_MISSING", base)
    if exit_code != 1:
        return _failure(mode, "PYTEST_EXIT_INVALID", base)
    for failed in failed_calls:
        markers = set(_strings(failed.get("markers")))
        if not markers or not markers.issubset(requested):
            return _failure(mode, "UNEXPECTED_FAILURE", base)
    if observed != requested:
        return _failure(mode, "EXPECTED_RED_MISSING", base)
    return _success(mode, "EXPECTED_RED_CONFIRMED", base)


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        return ()
    return tuple(value)


def _records(value: object) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        return ()
    return tuple(dict(item) for item in value)


def _marker_coverage(records: Sequence[Mapping[str, object]]) -> list[str]:
    return sorted(
        {
            marker
            for record in records
            for marker in _strings(record.get("markers"))
        }
    )


def _success(
    mode: str,
    reason: str,
    base: Mapping[str, object],
) -> dict[str, object]:
    return {
        "collected": base["collected"],
        "executed": base["executed"],
        "failed": base["failed"],
        "marker_coverage": base["marker_coverage"],
        "mode": mode,
        "node_ids": base["node_ids"],
        "ok": True,
        "reason": reason,
    }


def _failure(
    mode: str,
    reason: str,
    base: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "ok": False,
        "mode": mode,
        "reason": reason,
    }
    if base is not None:
        payload.update(
            {
                "collected": base["collected"],
                "executed": base["executed"],
                "failed": base["failed"],
                "marker_coverage": base["marker_coverage"],
                "node_ids": base["node_ids"],
            }
        )
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify Phase 2 expected-RED or complete-GREEN pytest contracts"
    )
    parser.add_argument("--root", type=Path, default=Path("."))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--expect-red", action="store_true")
    mode.add_argument("--expect-green", action="store_true")
    parser.add_argument("--marker", action="append", default=[])
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument("--tests", nargs="+", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _build_parser().parse_args(argv)
    mode = "expected-red" if arguments.expect_red else "green"
    try:
        payload = verify_contracts(
            arguments.root,
            arguments.tests,
            mode=mode,
            markers=arguments.marker,
            timeout_seconds=arguments.timeout_seconds,
        )
    except (OSError, VerifierInputError, TypeError, ValueError):
        payload = {
            "ok": False,
            "mode": mode,
            "reason": "INVALID_INPUT",
        }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload.get("ok") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_TIMEOUT_SECONDS",
    "NETWORK_DENIED_ENV",
    "VerifierInputError",
    "main",
    "verify_contracts",
]
