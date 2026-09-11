from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

import options_copilot.operations.workspace_guard as workspace_guard

from options_copilot.operations.workspace_guard import (
    ScopePathError,
    build_phase_allowlists,
    build_standalone_phase_allowlist,
    load_standalone_phase_allowlist,
    parse_plan,
    render_phase_allowlists,
    render_standalone_phase_allowlist,
    validate_phase_allowlist,
    validate_plan_allowlist,
    validate_scope_path,
    verify_allowlist_consistency,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
MASTER_PLAN = REPO_ROOT / "options_copilot" / "IMPLEMENTATION_PLAN.md"
PLANS_DIR = REPO_ROOT / "options_copilot" / "plans"
CHECKED_MANIFEST = REPO_ROOT / "options_copilot" / "operations" / "phase_allowlists.json"
STANDALONE_MANIFEST = (
    REPO_ROOT / "options_copilot" / "operations" / "standalone_phase_allowlists.json"
)
PHASE_01_DIR = (
    REPO_ROOT / ".planning" / "phases" / "01-hermetic-standalone-foundation"
)
PHASE_02_DIR = (
    REPO_ROOT / ".planning" / "phases" / "02-advisory-api-and-secondary-evidence"
)


def _write_plan(
    path: Path,
    *,
    files_modified: tuple[str, ...],
    task_files: tuple[str, ...],
    evidence_prefixes: tuple[str, ...] = (),
) -> None:
    modified = "\n".join(f"  - {item}" for item in files_modified)
    evidence = (
        "[]"
        if not evidence_prefixes
        else "\n" + "\n".join(f"  - {item}" for item in evidence_prefixes)
    )
    tasks = "\n".join(
        f"<task type=\"auto\"><files>{item}</files></task>" for item in task_files
    )
    path.write_text(
        f"""---
phase: P0-test
plan: "99"
files_modified:
{modified}
evidence_prefixes: {evidence}
---
<tasks>
{tasks}
</tasks>
""",
        encoding="utf-8",
    )


def test_checked_manifest_exactly_matches_all_plans_and_master() -> None:
    report = verify_allowlist_consistency(
        MASTER_PLAN,
        PLANS_DIR,
        manifest_path=CHECKED_MANIFEST,
    )

    assert report.ok, [issue.as_dict() for issue in report.issues]
    expected = render_phase_allowlists(
        build_phase_allowlists(MASTER_PLAN, PLANS_DIR)
    )
    assert CHECKED_MANIFEST.read_text(encoding="utf-8") == expected


def test_standalone_manifest_is_exact_union_with_path_provenance() -> None:
    expected_manifest = build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR)
    expected_text = render_standalone_phase_allowlist(expected_manifest)

    checked = load_standalone_phase_allowlist(STANDALONE_MANIFEST)

    assert checked == expected_manifest
    assert STANDALONE_MANIFEST.read_text(encoding="utf-8") == expected_text
    assert checked["execution_baseline_commit"] == "7237246"
    assert workspace_guard.STANDALONE_BASELINE_FULL_COMMIT == (
        "72372464e99dda342af5934bb8641900970dc013"
    )
    assert set(checked["phases"]) == {"01", "02"}
    phase_01 = checked["phases"]["01"]
    phase_02 = checked["phases"]["02"]
    assert phase_01["authority_commit"] == workspace_guard.STANDALONE_PLAN_AUTHORITY_COMMIT
    assert phase_02["authority_commit"] == workspace_guard.PHASE2_PLAN_AUTHORITY_COMMIT
    supplemental = phase_02["supplemental_authority"]
    assert supplemental["authority_commit"] == (
        workspace_guard.PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT
    )
    assert supplemental["parent_plan_authority_commit"] == (
        workspace_guard.PHASE2_PLAN_AUTHORITY_COMMIT
    )
    assert supplemental["execution_baseline_commit"] == (
        workspace_guard.STANDALONE_BASELINE_FULL_COMMIT
    )
    assert [plan["plan_id"] for plan in phase_01["plans"]] == [
        "01-01",
        "01-02",
        "01-03",
        "01-04",
    ]
    assert [plan["plan_id"] for plan in phase_02["plans"]] == [
        f"02-{number:02d}" for number in range(1, 11)
    ]
    for phase in (phase_01, phase_02):
        expected_union = set().union(*map(set, phase["buckets"].values()))
        assert set(phase["allowlist"]) == expected_union
        assert set(phase["provenance"]) == expected_union
        assert all(phase["provenance"][path] for path in expected_union)
    assert set(phase_01["allowlist"]).issubset(phase_02["allowlist"])
    assert phase_02["buckets"]["ignored_evidence_prefixes"] == [
        "data/options_copilot/evidence/phase-02/live/"
    ]
    assert phase_01["supplemental_authority"] is None
    assert phase_01["buckets"]["supplemental_authority"] == []
    assert phase_02["buckets"]["supplemental_authority"] == [
        (
            ".planning/phases/02-advisory-api-and-secondary-evidence/"
            "02-09-SUPPLEMENTAL-AUTHORITY.json"
        ),
        "options_copilot/operations/authority_audit.py",
        "tests/options_copilot/test_authority_audit.py",
    ]


def test_standalone_plan_authority_is_the_preimplementation_commit() -> None:
    first_implementation = subprocess.run(
        [
            "git",
            "log",
            "--reverse",
            "--format=%H",
            f"{workspace_guard.STANDALONE_PLAN_AUTHORITY_COMMIT}..HEAD",
            "--",
            "options_copilot",
            "scripts",
            "tests",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.splitlines()[0]
    assert first_implementation == "a20bb6b74d6040d65b7ba53cbee7dccefbc3532f"

    manifest = build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR)
    phase_contracts = (
        ("01", PHASE_01_DIR, workspace_guard.STANDALONE_PLAN_AUTHORITY_COMMIT),
        ("02", PHASE_02_DIR, workspace_guard.PHASE2_PLAN_AUTHORITY_COMMIT),
    )
    for phase, directory, authority_commit in phase_contracts:
        for record in manifest["phases"][phase]["plans"]:
            plan_name = f"{record['plan_id']}-PLAN.md"
            relative = directory.relative_to(REPO_ROOT).as_posix() + "/" + plan_name
            anchored = subprocess.run(
                ["git", "cat-file", "blob", f"{authority_commit}:{relative}"],
                cwd=REPO_ROOT,
                check=True,
                capture_output=True,
            ).stdout
            checked_in_text = (directory / plan_name).read_bytes()
            assert checked_in_text == anchored
            assert record["content_sha256"] == hashlib.sha256(anchored).hexdigest()


def test_standalone_manifest_uses_only_fixed_audited_exceptions() -> None:
    manifest = build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR)

    assert manifest["phases"]["01"]["buckets"]["audited_exceptions"] == [
        "AGENTS.md",
        "tests/options_copilot/test_bridge_coordinator.py",
    ]
    assert "committed_since_baseline" not in manifest["phases"]["01"]["buckets"]
    assert manifest["phases"]["02"]["buckets"]["audited_exceptions"] == []
    supplemental_paths = set(
        manifest["phases"]["02"]["buckets"]["supplemental_authority"]
    )
    assert supplemental_paths.isdisjoint(
        manifest["phases"]["02"]["buckets"]["audited_exceptions"]
    )


def test_phase2_supplemental_authority_chain_diff_and_blobs_are_exact() -> None:
    manifest = build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR)
    supplemental = manifest["phases"]["02"]["supplemental_authority"]
    anchor = workspace_guard.PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT

    for ancestor, descendant in (
        (workspace_guard.PHASE2_PLAN_AUTHORITY_COMMIT, anchor),
        (anchor, "HEAD"),
    ):
        completed = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, descendant],
            cwd=REPO_ROOT,
            check=False,
        )
        assert completed.returncode == 0

    raw_diff = subprocess.run(
        [
            "git",
            "diff",
            "--name-status",
            "--no-renames",
            f"{anchor}^..{anchor}",
            "--",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.splitlines()
    assert raw_diff == [
        (
            "A\t.planning/phases/02-advisory-api-and-secondary-evidence/"
            "02-09-SUPPLEMENTAL-AUTHORITY.json"
        ),
        "M\toptions_copilot/operations/authority_audit.py",
        "M\ttests/options_copilot/test_authority_audit.py",
    ]

    record_path = supplemental["record_path"]
    record_blob = subprocess.run(
        ["git", "cat-file", "blob", f"{anchor}:{record_path}"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    assert hashlib.sha256(record_blob).hexdigest() == supplemental["record_sha256"]
    record = json.loads(record_blob.decode("utf-8"))
    assert record["authorized_paths"] == supplemental["authorized_paths"]
    assert record["content_sha256"] == supplemental["content_sha256"]
    for path, expected_sha256 in supplemental["anchor_blob_sha256"].items():
        blob = subprocess.run(
            ["git", "cat-file", "blob", f"{anchor}:{path}"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
        ).stdout
        assert hashlib.sha256(blob).hexdigest() == expected_sha256


@pytest.mark.parametrize(
    "anchor",
    (
        "f" * 40,
        workspace_guard.PHASE2_PLAN_AUTHORITY_COMMIT,
    ),
)
def test_phase2_supplemental_authority_rejects_missing_or_wrong_anchor(
    monkeypatch: pytest.MonkeyPatch,
    anchor: str,
) -> None:
    monkeypatch.setattr(
        workspace_guard,
        "PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT",
        anchor,
    )

    with pytest.raises(workspace_guard.AllowlistError):
        build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR)


@pytest.mark.parametrize(
    "mutation",
    ("missing_record", "wrong_anchor", "extra_path", "wrong_hash"),
)
def test_phase2_supplemental_manifest_edits_fail_closed(
    tmp_path: Path,
    mutation: str,
) -> None:
    payload = deepcopy(build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR))
    phase_02 = payload["phases"]["02"]
    supplemental = phase_02["supplemental_authority"]
    if mutation == "missing_record":
        phase_02.pop("supplemental_authority")
    elif mutation == "wrong_anchor":
        supplemental["authority_commit"] = workspace_guard.PHASE2_PLAN_AUTHORITY_COMMIT
    elif mutation == "extra_path":
        supplemental["authorized_paths"].append("options_copilot/extra.py")
    else:
        supplemental["anchor_blob_sha256"][
            "options_copilot/operations/authority_audit.py"
        ] = "0" * 64
    candidate = tmp_path / f"supplemental-{mutation}.json"
    candidate.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises((ScopePathError, ValueError)):
        load_standalone_phase_allowlist(candidate)


def test_standalone_manifest_authority_never_reads_the_current_head_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_run_git = workspace_guard._run_git
    immutable_anchor_range = (
        f"{workspace_guard.PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT}^.."
        f"{workspace_guard.PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT}"
    )

    def rejecting_diff(root: Path, arguments: tuple[str, ...]) -> str:
        if (
            arguments
            and arguments[0] == "diff"
            and immutable_anchor_range not in arguments
        ):
            raise AssertionError("current HEAD diff must not grant authorization")
        return original_run_git(root, arguments)

    monkeypatch.setattr(workspace_guard, "_run_git", rejecting_diff)

    manifest = workspace_guard.build_standalone_phase_allowlist(
        REPO_ROOT,
        PHASE_01_DIR,
    )

    assert manifest["phases"]["02"]["allowlist"]


@pytest.mark.parametrize(
    "mutation",
    (
        "missing_plan",
        "duplicate_plan",
        "altered_plan_order",
        "historical_plan_label",
        "missing_path",
        "surplus_path",
    ),
)
def test_standalone_manifest_rejects_inexact_plans_and_paths(
    tmp_path: Path,
    mutation: str,
) -> None:
    payload = deepcopy(build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR))
    phase_02 = payload["phases"]["02"]
    if mutation == "missing_plan":
        phase_02["plans"].pop()
    elif mutation == "duplicate_plan":
        phase_02["plans"].append(deepcopy(phase_02["plans"][-1]))
    elif mutation == "altered_plan_order":
        phase_02["plans"][0], phase_02["plans"][1] = (
            phase_02["plans"][1],
            phase_02["plans"][0],
        )
    elif mutation == "historical_plan_label":
        phase_02["plans"][0]["plan_id"] = "P0-01"
    elif mutation == "missing_path":
        removed = phase_02["allowlist"].pop()
        phase_02["provenance"].pop(removed)
    else:
        phase_02["allowlist"].append("options_copilot/surplus.py")
        phase_02["provenance"]["options_copilot/surplus.py"] = [
            "audited_exceptions"
        ]
    candidate = tmp_path / f"{mutation}.json"
    candidate.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises((ScopePathError, ValueError)):
        load_standalone_phase_allowlist(candidate)


@pytest.mark.parametrize("mutation", ("wrong_authority", "phase_crossover"))
def test_phase_02_manifest_rejects_wrong_authority_or_phase(
    tmp_path: Path,
    mutation: str,
) -> None:
    payload = deepcopy(build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR))
    phase_02 = payload["phases"]["02"]
    if mutation == "wrong_authority":
        phase_02["authority_commit"] = "a" * 40
    else:
        phase_02["plans"][0]["plan_id"] = "01-01"
    candidate = tmp_path / f"{mutation}.json"
    candidate.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises((ScopePathError, ValueError)):
        load_standalone_phase_allowlist(candidate)


def test_plan_frontmatter_rejects_a_missing_task_file(tmp_path: Path) -> None:
    plan_path = tmp_path / "P0-99-PLAN.md"
    _write_plan(
        plan_path,
        files_modified=("options_copilot/a.py",),
        task_files=("options_copilot/a.py, options_copilot/b.py",),
    )

    difference = validate_plan_allowlist(parse_plan(plan_path))

    assert difference.missing == ("options_copilot/b.py",)
    assert difference.surplus == ()


def test_plan_frontmatter_rejects_a_surplus_file(tmp_path: Path) -> None:
    plan_path = tmp_path / "P0-99-PLAN.md"
    _write_plan(
        plan_path,
        files_modified=("options_copilot/a.py", "options_copilot/surplus.py"),
        task_files=("options_copilot/a.py",),
    )

    difference = validate_plan_allowlist(parse_plan(plan_path))

    assert difference.missing == ()
    assert difference.surplus == ("options_copilot/surplus.py",)


def test_evidence_prefix_is_part_of_the_exact_plan_union(tmp_path: Path) -> None:
    plan_path = tmp_path / "P0-99-PLAN.md"
    _write_plan(
        plan_path,
        files_modified=("options_copilot/a.py",),
        task_files=("options_copilot/a.py",),
        evidence_prefixes=("data/options_copilot/evidence/checkpoints/P0/test/",),
    )

    difference = validate_plan_allowlist(parse_plan(plan_path))

    assert difference.missing == (
        "data/options_copilot/evidence/checkpoints/P0/test/",
    )


def test_master_phase_rejects_both_missing_and_surplus_entries(tmp_path: Path) -> None:
    first = tmp_path / "P0-01-PLAN.md"
    second = tmp_path / "P0-02-PLAN.md"
    _write_plan(
        first,
        files_modified=("options_copilot/a.py",),
        task_files=("options_copilot/a.py",),
    )
    _write_plan(
        second,
        files_modified=("options_copilot/b.py",),
        task_files=("options_copilot/b.py",),
    )
    plans = (parse_plan(first), parse_plan(second))

    difference = validate_phase_allowlist(
        "P0",
        ("options_copilot/a.py", "options_copilot/surplus.py"),
        plans,
    )

    assert difference.missing == ("options_copilot/b.py",)
    assert difference.surplus == ("options_copilot/surplus.py",)


@pytest.mark.parametrize(
    "path",
    (
        "*",
        "**/*.py",
        ".planning/STATE.md",
        "options_copilot/../trade_copilot/app.py",
        "trade_copilot/app.py",
        "rithmic/sidecar.py",
        "services/paper_server/app.py",
        "scripts/",
        "scripts/*.ps1",
        "scripts/unnamed",
        ".venv/Lib/site-packages/pip/__init__.py",
        "venv/Scripts/python.exe",
        ".pytest_cache/v/cache/nodeids",
        "options_copilot/__pycache__/runtime.cpython-312.pyc",
        "migration_backups/snapshot.json",
        "logs/options_copilot/server.log",
        "data/options_copilot/api_keys.local.json",
        "data/options_copilot/provider-key.json",
        "data/options_copilot/credentials.json",
        "data/options_copilot/runtime.sqlite",
        "data/options_copilot/runtime.sqlite3",
        "data/options_copilot/runtime.sqlite3-wal",
        "data/options_copilot/runtime.sqlite3-shm",
        "data/options_copilot/evidence/workspace/phase-01/baseline.json",
        "reports/output.json.tmp",
        "reports/output.temp",
        "reports/output.bak",
    ),
)
def test_prohibited_or_unnamed_scope_paths_fail_closed(path: str) -> None:
    with pytest.raises(ScopePathError):
        validate_scope_path(path)


def test_curated_evidence_path_is_not_denied_wholesale() -> None:
    path = "data/options_copilot/evidence/checkpoints/phase-01/report.json"

    assert validate_scope_path(path) == path


def test_code_denials_override_an_edited_standalone_manifest(tmp_path: Path) -> None:
    payload = deepcopy(build_standalone_phase_allowlist(REPO_ROOT, PHASE_01_DIR))
    prohibited = "data/options_copilot/evidence/private.sqlite3-wal"
    phase_02 = payload["phases"]["02"]
    phase_02["buckets"]["audited_exceptions"].append(prohibited)
    phase_02["buckets"]["audited_exceptions"].sort()
    phase_02["allowlist"].append(prohibited)
    phase_02["allowlist"].sort()
    phase_02["provenance"][prohibited] = ["audited_exceptions"]
    candidate = tmp_path / "edited.json"
    candidate.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ScopePathError):
        load_standalone_phase_allowlist(candidate)
