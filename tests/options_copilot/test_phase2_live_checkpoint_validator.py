"""Hermetic contracts for the Phase 2 live checkpoint validator."""
from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import ModuleType

import pytest

from options_copilot.storage.canonical import canonical_hash, canonical_json


ROOT = Path(__file__).resolve().parents[2]
LIVE_ROOT = (
    ROOT / "data" / "options_copilot" / "evidence" / "phase-02" / "live"
)
SOURCE_IDS = (
    "sec",
    "nasdaq",
    "company_ir",
    "finnhub",
    "alpha_vantage",
    "jin10",
)


def _require_validator() -> ModuleType:
    try:
        module = importlib.import_module("scripts.verify_phase2_live_checkpoint")
    except ModuleNotFoundError as exc:
        if exc.name == "scripts.verify_phase2_live_checkpoint":
            assert False, (
                "PHASE2_EXPECTED_RED:LIVE_CHECKPOINT_VALIDATOR "
                "validator module is missing"
            )
        raise
    required = (
        "EXPECTED_SOURCE_IDS",
        "CheckpointValidationError",
        "snapshot_json_identities",
        "validate_checkpoint_transition",
        "run_probe_and_validate",
    )
    missing = [name for name in required if not hasattr(module, name)]
    assert not missing, (
        "PHASE2_EXPECTED_RED:LIVE_CHECKPOINT_VALIDATOR missing API: "
        + ",".join(missing)
    )
    return module


@pytest.fixture
def evidence_dir() -> Iterator[Path]:
    LIVE_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="validator-test-",
        dir=LIVE_ROOT,
    ) as value:
        directory = Path(value).resolve()
        assert directory.drive.upper() == "G:"
        yield directory


def _source_row(source_id: str, *, state: str = "UNAVAILABLE") -> dict[str, object]:
    return {
        "source_id": source_id,
        "configured": source_id in {"sec", "nasdaq"},
        "readiness": state,
        "status": state,
        "reason": None if state == "READY" else "PACING_UNVERIFIED",
        "observed_at": "2026-08-08T19:30:00+00:00",
        "as_of": None,
        "last_success_at": None,
        "freshness_age_seconds": None,
        "provenance": [f"source:{source_id}"],
        "pacing": "PACING_UNVERIFIED",
        "request_bytes": None,
        "response_bytes": None,
        "event_count": 0,
        "active_count": 0,
        "conflicted_count": 0,
        "request_methods": ["POST", "DELETE"] if source_id == "jin10" else ["GET"],
        "transport_verified": source_id != "jin10",
        "read_only": True,
        "decision_authority": "SUPPORTING_ONLY",
    }


def _document() -> dict[str, object]:
    document: dict[str, object] = {
        "schema": "options_copilot.provider_probe_checkpoint.v2",
        "generated_at": "2026-08-08T19:30:00+00:00",
        "status": "DEGRADED",
        "decision": "NO_TRADE",
        "decision_authority": "SUPPORTING_ONLY",
        "read_only": True,
        "instruction_creation_allowed": False,
        "approval_eligible": False,
        "order_submission_allowed": False,
        "contract": {
            "provider_names": list(SOURCE_IDS),
            "requested_symbols": ["SPY"],
            "limit": 1,
            "read_only": True,
        },
        "sources": [_source_row(source_id) for source_id in SOURCE_IDS],
        "conflicts": [],
        "redaction": {"status": "PASS", "finding_count": 0},
    }
    document["canonical_sha256"] = canonical_hash(document)
    return document


def _write_checkpoint(directory: Path, document: dict[str, object]) -> Path:
    path = directory / f"provider_probe_checkpoint.{canonical_hash(document)}.json"
    path.write_text(canonical_json(document) + "\n", encoding="utf-8", newline="\n")
    return path


def _assert_rejected(
    action: Callable[[], object],
    *,
    absent: tuple[str, ...] = (),
) -> None:
    module = _require_validator()
    error = module.CheckpointValidationError
    with pytest.raises(error) as raised:
        action()
    rendered = str(raised.value).casefold()
    for sentinel in absent:
        assert sentinel.casefold() not in rendered


def test_validator_contract_exposes_exact_six_source_ids() -> None:
    module = _require_validator()
    assert tuple(module.EXPECTED_SOURCE_IDS) == SOURCE_IDS


def test_direct_script_entrypoint_imports_from_repo_root_without_pythonpath() -> None:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "verify_phase2_live_checkpoint.py"),
            "--help",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Validate one bounded Phase 2 provider checkpoint" in completed.stdout
    assert "ModuleNotFoundError" not in completed.stderr


def test_exactly_one_new_canonical_checkpoint_is_accepted(evidence_dir: Path) -> None:
    module = _require_validator()
    before = module.snapshot_json_identities(evidence_dir)
    expected = _write_checkpoint(evidence_dir, _document())
    after = module.snapshot_json_identities(evidence_dir)

    report = module.validate_checkpoint_transition(evidence_dir, before, after)

    assert report == {
        "marker": "PHASE2_LIVE_CHECKPOINT_SAFE",
        "filename": expected.name,
        "canonical_sha256": _document()["canonical_sha256"],
        "generated_at": "2026-08-08T19:30:00+00:00",
        "source_states": [
            {"source_id": source_id, "status": "UNAVAILABLE"}
            for source_id in SOURCE_IDS
        ],
    }


def test_preexisting_or_replaced_file_cannot_satisfy_transition(
    evidence_dir: Path,
) -> None:
    module = _require_validator()
    existing = _write_checkpoint(evidence_dir, _document())
    before = module.snapshot_json_identities(evidence_dir)
    _assert_rejected(
        lambda: module.validate_checkpoint_transition(
            evidence_dir,
            before,
            module.snapshot_json_identities(evidence_dir),
        )
    )

    existing.write_text("{}\n", encoding="utf-8", newline="\n")
    _write_checkpoint(
        evidence_dir,
        {
            **_document(),
            "generated_at": "2026-08-08T19:31:00+00:00",
        },
    )
    _assert_rejected(
        lambda: module.validate_checkpoint_transition(
            evidence_dir,
            before,
            module.snapshot_json_identities(evidence_dir),
        )
    )


@pytest.mark.parametrize(
    "mutation",
    ("extra_top_level", "missing_source", "duplicate_source", "extra_row_field"),
)
def test_schema_and_six_rows_are_exact(
    evidence_dir: Path,
    mutation: str,
) -> None:
    module = _require_validator()
    before = module.snapshot_json_identities(evidence_dir)
    document = _document()
    if mutation == "extra_top_level":
        document["unexpected"] = False
    elif mutation == "missing_source":
        document["sources"] = document["sources"][:-1]
    elif mutation == "duplicate_source":
        document["sources"][-1]["source_id"] = "sec"
    else:
        document["sources"][0]["unexpected"] = 0
    document["canonical_sha256"] = canonical_hash(
        {key: value for key, value in document.items() if key != "canonical_sha256"}
    )
    _write_checkpoint(evidence_dir, document)

    _assert_rejected(
        lambda: module.validate_checkpoint_transition(
            evidence_dir,
            before,
            module.snapshot_json_identities(evidence_dir),
        )
    )


def test_duplicate_nonfinite_and_canonical_hash_fail_closed(evidence_dir: Path) -> None:
    module = _require_validator()
    before = module.snapshot_json_identities(evidence_dir)
    document = _document()
    document["canonical_sha256"] = "0" * 64
    _write_checkpoint(evidence_dir, document)
    _assert_rejected(
        lambda: module.validate_checkpoint_transition(
            evidence_dir,
            before,
            module.snapshot_json_identities(evidence_dir),
        )
    )

    for raw in (
        '{"schema":"x","schema":"y"}\n',
        '{"value":NaN}\n',
    ):
        for path in evidence_dir.glob("*.json"):
            path.unlink()
        before = module.snapshot_json_identities(evidence_dir)
        (evidence_dir / "provider_probe_checkpoint.invalid.json").write_text(
            raw,
            encoding="utf-8",
            newline="\n",
        )
        _assert_rejected(
            lambda: module.validate_checkpoint_transition(
                evidence_dir,
                before,
                module.snapshot_json_identities(evidence_dir),
            )
        )


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("authorization_header", "redacted"),
        ("safe_note", "Bearer sentinel-live-secret"),
        ("safe_note", "C:\\Private\\checkpoint.json"),
        ("safe_note", "https://provider.test/read?api_key=sentinel"),
        ("raw_error", "fixed"),
        ("safe_note", "creator instruction approved"),
    ),
)
def test_recursive_forbidden_key_and_value_scan_rejects_sentinels(
    evidence_dir: Path,
    key: str,
    value: str,
) -> None:
    module = _require_validator()
    before = module.snapshot_json_identities(evidence_dir)
    document = _document()
    document["sources"][0]["provenance"] = [
        {"nested": {key: value}},
    ]
    document["canonical_sha256"] = canonical_hash(
        {item: content for item, content in document.items() if item != "canonical_sha256"}
    )
    _write_checkpoint(evidence_dir, document)

    _assert_rejected(
        lambda: module.validate_checkpoint_transition(
            evidence_dir,
            before,
            module.snapshot_json_identities(evidence_dir),
        ),
        absent=("sentinel-live-secret", "c:\\private", "api_key=sentinel"),
    )


def test_runner_invokes_exact_probe_once_and_printable_report_is_redacted(
    evidence_dir: Path,
) -> None:
    module = _require_validator()
    calls: list[tuple[object, ...]] = []

    def command_runner(command: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        _write_checkpoint(evidence_dir, _document())
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"credential":"sentinel-live-secret"}',
            stderr="raw provider error sentinel-live-private",
        )

    report = module.run_probe_and_validate(
        root=ROOT,
        evidence_dir=evidence_dir,
        command_runner=command_runner,
    )

    assert len(calls) == 1
    command, kwargs = calls[0]
    assert command == [
        str(ROOT / ".venv" / "Scripts" / "python.exe"),
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
        str(evidence_dir),
        "--json",
    ]
    assert kwargs["cwd"] == ROOT
    rendered = json.dumps(report, sort_keys=True).casefold()
    assert report["marker"] == "PHASE2_LIVE_CHECKPOINT_SAFE"
    assert "sentinel-live" not in rendered
    assert [row["source_id"] for row in report["source_states"]] == list(SOURCE_IDS)


def test_runner_failure_never_renders_probe_output(evidence_dir: Path) -> None:
    module = _require_validator()
    calls = 0

    def command_runner(command: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(
            command,
            2,
            stdout="Bearer sentinel-live-secret",
            stderr="raw error sentinel-live-private",
        )

    _assert_rejected(
        lambda: module.run_probe_and_validate(
            root=ROOT,
            evidence_dir=evidence_dir,
            command_runner=command_runner,
        ),
        absent=("sentinel-live-secret", "sentinel-live-private", "raw error"),
    )
    assert calls == 1


def test_runner_rejects_noncanonical_live_root_before_subprocess(
    evidence_dir: Path,
) -> None:
    module = _require_validator()
    calls = 0

    def command_runner(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("unsafe root must stop before subprocess")

    _assert_rejected(
        lambda: module.run_probe_and_validate(
            root=ROOT,
            evidence_dir=evidence_dir.parent.parent,
            command_runner=command_runner,
        )
    )
    assert calls == 0


def test_valid_report_timestamp_is_aware_utc(evidence_dir: Path) -> None:
    module = _require_validator()
    before = module.snapshot_json_identities(evidence_dir)
    _write_checkpoint(evidence_dir, _document())
    report = module.validate_checkpoint_transition(
        evidence_dir,
        before,
        module.snapshot_json_identities(evidence_dir),
    )
    parsed = datetime.fromisoformat(report["generated_at"])
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timezone.utc.utcoffset(parsed)
