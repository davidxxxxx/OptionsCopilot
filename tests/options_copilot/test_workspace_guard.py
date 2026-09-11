from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

from options_copilot.operations import workspace_guard
from options_copilot.operations.workspace_guard import (
    EXPECTED_PHASES,
    BaselineError,
    capture_workspace_baseline,
    hash_workspace_path,
    load_workspace_baseline,
    verify_workspace,
)


def _git(repo: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout


def _repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "workspace-guard@example.invalid")
    _git(repo, "config", "user.name", "Workspace Guard Test")
    return repo


def _commit(repo: Path, files: dict[str, str]) -> str:
    for relative, content in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    _git(repo, "add", "--all")
    _git(repo, "commit", "-m", "baseline")
    return _git(repo, "rev-parse", "HEAD").strip()


def _baseline(repo: Path, path_hashes: tuple[dict[str, object], ...] = ()) -> Path:
    commands = []
    for arguments in (
        ("rev-parse", "HEAD"),
        ("status", "--porcelain=v2", "--untracked-files=all"),
        ("diff", "--name-status"),
        ("diff", "--cached", "--name-status"),
    ):
        commands.append(
            {
                "command": "git " + " ".join(arguments),
                "stdout": _git(repo, *arguments),
                "stderr": "",
                "exit_code": 0,
            }
        )
    path = repo.parent / f"{repo.name}-baseline.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "captured_at_utc": "2026-08-03T00:00:00Z",
                "repository_root": repo.resolve().as_posix(),
                "commands": commands,
                "dirty_path_hashes": list(path_hashes),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _manifest(repo: Path, p0_allowlist: tuple[str, ...]) -> Path:
    phases = {
        phase: {
            "plans": [],
            "allowlist": sorted(p0_allowlist if phase == "P0" else ()),
        }
        for phase in EXPECTED_PHASES
    }
    path = repo.parent / f"{repo.name}-phase-allowlists.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "master_plan": "options_copilot/IMPLEMENTATION_PLAN.md",
                "plans_directory": "options_copilot/plans",
                "phases": phases,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _capture_destination(repo: Path) -> Path:
    return (
        repo
        / "data"
        / "options_copilot"
        / "evidence"
        / "workspace"
        / "phase-01"
        / "pre_edit_baseline.json"
    )


def _capture_repository(tmp_path: Path) -> tuple[Path, str, str]:
    repo = _repository(tmp_path)
    authority = _commit(
        repo,
        {
            ".gitignore": "data/options_copilot/\n",
            "protected.txt": "committed\n",
            "stable.py": "stable\n",
        },
    )
    captured_head = _commit(repo, {"committed.py": "committed after authority\n"})
    return repo, authority, captured_head


def _create_windows_junction(link: Path, target: Path) -> None:
    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def _standalone_plan(plan_number: str, *files_modified: str) -> str:
    files = "\n".join(f"  - {path}" for path in files_modified)
    return (
        "---\n"
        "phase: 01-hermetic-standalone-foundation\n"
        f'plan: "{plan_number}"\n'
        "files_modified:\n"
        f"{files}\n"
        "---\n"
    )


def _phase_plan(
    phase: str,
    plan_number: str,
    *files_modified: str,
    evidence_prefixes: tuple[str, ...] = (),
) -> str:
    files = "\n".join(f"  - {path}" for path in files_modified)
    evidence = "\n".join(f"  - {path}" for path in evidence_prefixes)
    return (
        "---\n"
        f"phase: {phase}\n"
        f'plan: "{plan_number}"\n'
        "files_modified:\n"
        f"{files}\n"
        "evidence_prefixes:\n"
        f"{evidence}\n"
        "---\n"
    )


def _phase_02_authority_repository(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path, str, str, Path]:
    repo = _repository(tmp_path)
    baseline_commit = _commit(
        repo,
        {
            ".gitignore": (
                "data/options_copilot/evidence/workspace/\n"
                "data/options_copilot/evidence/phase-02/live/\n"
            )
        },
    )
    phase_01_relative = ".planning/phases/01-hermetic-standalone-foundation"
    phase_01_files = {
        f"{phase_01_relative}/01-{number:02d}-PLAN.md": _phase_plan(
            "01-hermetic-standalone-foundation",
            f"{number:02d}",
            f"options_copilot/phase1_{number:02d}.py",
        )
        for number in range(1, 5)
    }
    phase_01_authority = _commit(repo, phase_01_files)
    phase_02_relative = ".planning/phases/02-advisory-api-and-secondary-evidence"
    phase_02_files = {
        f"{phase_02_relative}/02-{number:02d}-PLAN.md": _phase_plan(
            "02-advisory-api-and-secondary-evidence",
            f"{number:02d}",
            *(
                (
                    "data/options_copilot/evidence/phase-02/live/",
                    "scripts/phase2_live.py",
                )
                if number == 9
                else (f"options_copilot/phase2_{number:02d}.py",)
            ),
            evidence_prefixes=(
                ("data/options_copilot/evidence/phase-02/live/",)
                if number == 9
                else ()
            ),
        )
        for number in range(1, 11)
    }
    phase_02_files.update(
        {
            "options_copilot/operations/authority_audit.py": "before\n",
            "tests/options_copilot/test_authority_audit.py": "before\n",
        }
    )
    phase_02_authority = _commit(repo, phase_02_files)
    authorized_paths = workspace_guard._PHASE2_SUPPLEMENTAL_AUTHORIZED_PATHS
    supplemental_contents = {
        authorized_paths[0]: "after production audit\n",
        authorized_paths[1]: "after authority tests\n",
    }
    for relative, content in supplemental_contents.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    content_sha256 = {
        relative: hashlib.sha256((repo / relative).read_bytes()).hexdigest()
        for relative in authorized_paths
    }
    supplemental_record = {
        "authorized_paths": list(authorized_paths),
        "content_sha256": content_sha256,
        "execution_baseline_commit": baseline_commit,
        "kind": workspace_guard._PHASE2_SUPPLEMENTAL_AUTHORITY_KIND,
        "parent_plan_authority_commit": phase_02_authority,
        "phase": "02",
        "plan": "02-09",
        "reason": workspace_guard._PHASE2_SUPPLEMENTAL_REASON,
        "scan_contract": dict(workspace_guard._PHASE2_SUPPLEMENTAL_SCAN_CONTRACT),
        "schema_version": 1,
    }
    record_path = repo / workspace_guard._PHASE2_SUPPLEMENTAL_AUTHORITY_PATH
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text(
        json.dumps(
            supplemental_record,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _git(repo, "add", "--all")
    _git(repo, "commit", "-m", "supplemental authority")
    supplemental_authority = _git(repo, "rev-parse", "HEAD").strip()
    anchor_blob_sha256 = {
        relative: hashlib.sha256(
            subprocess.run(
                [
                    "git",
                    "cat-file",
                    "blob",
                    f"{supplemental_authority}:{relative}",
                ],
                cwd=repo,
                check=True,
                capture_output=True,
            ).stdout
        ).hexdigest()
        for relative in authorized_paths
    }
    record_blob = subprocess.run(
        [
            "git",
            "cat-file",
            "blob",
            (
                f"{supplemental_authority}:"
                f"{workspace_guard._PHASE2_SUPPLEMENTAL_AUTHORITY_PATH}"
            ),
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    ).stdout
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", baseline_commit)
    monkeypatch.setattr(
        workspace_guard,
        "STANDALONE_BASELINE_FULL_COMMIT",
        baseline_commit,
    )
    monkeypatch.setattr(
        workspace_guard,
        "STANDALONE_PLAN_AUTHORITY_COMMIT",
        phase_01_authority,
    )
    monkeypatch.setattr(
        workspace_guard,
        "PHASE2_PLAN_AUTHORITY_COMMIT",
        phase_02_authority,
        raising=False,
    )
    monkeypatch.setattr(
        workspace_guard,
        "PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT",
        supplemental_authority,
    )
    monkeypatch.setattr(
        workspace_guard,
        "_PHASE2_SUPPLEMENTAL_CONTENT_SHA256",
        content_sha256,
    )
    monkeypatch.setattr(
        workspace_guard,
        "_PHASE2_SUPPLEMENTAL_ANCHOR_BLOB_SHA256",
        anchor_blob_sha256,
    )
    monkeypatch.setattr(
        workspace_guard,
        "_PHASE2_SUPPLEMENTAL_RECORD_SHA256",
        hashlib.sha256(record_blob).hexdigest(),
    )
    baseline = _capture_destination(repo)
    capture_workspace_baseline(repo, baseline)
    manifest_path = repo.parent / "standalone-phase-02-manifest.json"
    manifest = workspace_guard.build_standalone_phase_allowlist(
        repo,
        repo / phase_01_relative,
    )
    manifest_path.write_text(
        workspace_guard.render_standalone_phase_allowlist(manifest),
        encoding="utf-8",
    )
    return (
        repo,
        repo / phase_01_relative,
        repo / phase_02_relative,
        phase_01_authority,
        phase_02_authority,
        manifest_path,
    )


def test_capture_round_trip_keeps_fixed_authority_and_observes_head_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, captured_head = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    status_before = _git(repo, "status", "--porcelain=v2", "--untracked-files=all")

    baseline = capture_workspace_baseline(
        repo,
        destination,
        source_baseline_commit=authority,
        execution_baseline_commit=authority,
    )

    assert baseline.source_baseline_commit == authority
    assert baseline.schema_version == workspace_guard.STANDALONE_BASELINE_SCHEMA_VERSION
    assert baseline.execution_baseline_commit == authority
    assert baseline.captured_head_commit == captured_head
    assert baseline.commit == authority
    assert tuple(baseline.command_stdout) == (
        "git rev-parse HEAD",
        "git status --porcelain=v2 --untracked-files=all",
        "git diff --name-status",
        "git diff --cached --name-status",
    )
    assert tuple(baseline.dirty_path_hashes) == ()
    assert _git(repo, "status", "--porcelain=v2", "--untracked-files=all") == status_before

    loaded = load_workspace_baseline(destination, expected_root=repo)
    assert loaded == baseline
    (repo / "planned.py").write_text("planned\n", encoding="utf-8")
    manifest = _manifest(repo, ("committed.py", "planned.py"))

    report = verify_workspace(repo, "P0", destination, manifest)

    assert report.ok, [violation.as_dict() for violation in report.violations]
    assert report.baseline_commit == authority
    assert report.changed_paths == ("committed.py", "planned.py")
    assert report.preserved_dirty_paths == ()


def test_capture_rejects_baseline_promotion_and_missing_or_nonancestor_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, captured_head = _capture_repository(tmp_path)
    destination = _capture_destination(repo)
    status_before = _git(repo, "status", "--porcelain=v2", "--untracked-files=all")
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)

    with pytest.raises(BaselineError, match="fixed standalone baseline"):
        capture_workspace_baseline(
            repo,
            destination,
            source_baseline_commit=authority,
            execution_baseline_commit=captured_head,
        )
    with pytest.raises(BaselineError, match="fixed standalone baseline"):
        capture_workspace_baseline(
            repo,
            destination,
            source_baseline_commit=captured_head,
            execution_baseline_commit=authority,
        )

    missing = "f" * 40
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", missing)
    with pytest.raises(BaselineError, match="does not name an existing commit"):
        capture_workspace_baseline(
            repo,
            destination,
            source_baseline_commit=missing,
            execution_baseline_commit=missing,
        )

    side_branch = _git(repo, "rev-parse", "HEAD~1").strip()
    _git(repo, "checkout", "--detach", side_branch)
    unrelated = _commit(repo, {"unrelated.py": "sibling history\n"})
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", captured_head)
    with pytest.raises(BaselineError, match="ancestor"):
        capture_workspace_baseline(
            repo,
            destination,
            source_baseline_commit=captured_head,
            execution_baseline_commit=captured_head,
        )

    assert unrelated != captured_head
    assert not destination.exists()
    assert _git(repo, "status", "--porcelain=v2", "--untracked-files=all") == status_before.replace(
        captured_head,
        unrelated,
    )


def test_standalone_baseline_schema_is_exact_and_hashes_are_strict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    capture_workspace_baseline(repo, destination)
    original = destination.read_bytes()
    payload = json.loads(original)

    payload["surplus"] = True
    destination.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(BaselineError, match="fields are not exact"):
        load_workspace_baseline(destination, expected_root=repo)

    payload.pop("surplus")
    payload["commands"] = list(reversed(payload["commands"]))
    destination.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(BaselineError, match="commands or ordering changed"):
        load_workspace_baseline(destination, expected_root=repo)

    payload["commands"] = list(reversed(payload["commands"]))
    payload["dirty_path_hashes"] = [
        {"path": "missing.txt", "kind": "missing", "size": 0, "sha256": "bad"}
    ]
    destination.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(BaselineError, match="SHA-256"):
        load_workspace_baseline(destination, expected_root=repo)

    assert original != destination.read_bytes()


def test_capture_refuses_overwrite_and_uses_same_directory_atomic_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    links: list[tuple[Path, Path]] = []
    original_link = os.link

    def recording_link(source: str | Path, target: str | Path) -> None:
        links.append((Path(source), Path(target)))
        original_link(source, target)

    monkeypatch.setattr(workspace_guard.os, "link", recording_link)
    capture_workspace_baseline(repo, destination)

    assert links
    temporary, target = links[-1]
    assert temporary.parent == destination.parent
    assert target == destination
    evidence_before = destination.read_bytes()
    status_before = _git(repo, "status", "--porcelain=v2", "--untracked-files=all")

    with pytest.raises(BaselineError, match="already exists"):
        capture_workspace_baseline(repo, destination)

    assert destination.read_bytes() == evidence_before
    assert _git(repo, "status", "--porcelain=v2", "--untracked-files=all") == status_before


def test_atomic_publication_never_overwrites_a_concurrent_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    concurrent_bytes = b"concurrent authority checkpoint\n"
    original_link = os.link

    def create_destination_before_link(
        source: str | Path,
        target: str | Path,
    ) -> None:
        Path(target).write_bytes(concurrent_bytes)
        original_link(source, target)

    monkeypatch.setattr(workspace_guard.os, "link", create_destination_before_link)

    with pytest.raises(BaselineError, match="already exists"):
        capture_workspace_baseline(repo, destination)

    assert destination.read_bytes() == concurrent_bytes


@pytest.mark.skipif(os.name != "nt", reason="Windows junction semantics are required")
def test_capture_rejects_junctioned_workspace_root_without_external_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    workspace_root = destination.parents[1]
    outside_root = tmp_path / "junction-outside"
    workspace_root.parent.mkdir(parents=True)
    outside_root.mkdir()
    _create_windows_junction(workspace_root, outside_root)
    external_destination = outside_root / destination.relative_to(workspace_root)

    try:
        with pytest.raises(BaselineError, match="reparse point"):
            capture_workspace_baseline(repo, destination)

        assert not external_destination.exists()
    finally:
        if workspace_root.is_junction():
            workspace_root.rmdir()


@pytest.mark.skipif(os.name != "nt", reason="Windows junction semantics are required")
def test_capture_rechecks_ancestors_after_temp_write_before_atomic_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    workspace_root = destination.parents[1]
    outside_root = tmp_path / "path-swap-outside"
    original_named_temporary_file = workspace_guard.tempfile.NamedTemporaryFile

    @contextmanager
    def swap_workspace_root_after_temp_write(*args: object, **kwargs: object):
        with original_named_temporary_file(*args, **kwargs) as handle:
            yield handle
        workspace_root.rename(outside_root)
        _create_windows_junction(workspace_root, outside_root)

    monkeypatch.setattr(
        workspace_guard.tempfile,
        "NamedTemporaryFile",
        swap_workspace_root_after_temp_write,
    )
    external_destination = outside_root / destination.relative_to(workspace_root)

    try:
        with pytest.raises(BaselineError, match="reparse point"):
            capture_workspace_baseline(repo, destination)

        assert not external_destination.exists()
    finally:
        if workspace_root.is_junction():
            workspace_root.rmdir()


@pytest.mark.skipif(os.name != "nt", reason="Windows junction semantics are required")
def test_capture_blocks_workspace_root_swap_inside_atomic_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    workspace_root = destination.parents[1]
    outside_root = tmp_path / "publish-swap-outside"
    original_link = os.link

    def swap_workspace_root_during_link(
        source: str | Path,
        target: str | Path,
    ) -> None:
        workspace_root.rename(outside_root)
        _create_windows_junction(workspace_root, outside_root)
        original_link(source, target)

    monkeypatch.setattr(workspace_guard.os, "link", swap_workspace_root_during_link)
    external_destination = outside_root / destination.relative_to(workspace_root)

    try:
        with pytest.raises(BaselineError, match="could not be published atomically"):
            capture_workspace_baseline(repo, destination)

        assert not external_destination.exists()
    finally:
        if workspace_root.is_junction():
            workspace_root.rmdir()


def test_capture_rejects_unanchored_dirty_exemptions_without_creating_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    (repo / "user-note.txt").write_text("untracked user bytes\n", encoding="utf-8")
    status_before = _git(repo, "status", "--porcelain=v2", "--untracked-files=all")

    with pytest.raises(BaselineError, match="no authenticated anchor"):
        capture_workspace_baseline(repo, destination)

    assert not destination.exists()
    assert _git(repo, "status", "--porcelain=v2", "--untracked-files=all") == status_before


def test_coherently_tampered_standalone_dirty_exemption_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, authority, _ = _capture_repository(tmp_path)
    monkeypatch.setattr(workspace_guard, "STANDALONE_BASELINE_COMMIT", authority)
    destination = _capture_destination(repo)
    capture_workspace_baseline(repo, destination)
    payload = json.loads(destination.read_text(encoding="utf-8"))
    digest = "a" * 64
    payload["commands"][1]["stdout"] = "? exempted.py\n"
    payload["dirty_path_hashes"] = [
        {
            "path": "exempted.py",
            "kind": "file",
            "size": 1,
            "sha256": digest,
        }
    ]
    destination.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(BaselineError, match="no authenticated anchor"):
        load_workspace_baseline(destination, expected_root=repo)


def test_phase_01_rejects_legacy_baseline_with_current_head_dirty_exemption(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    _commit(repo, {"stable.py": "stable\n"})
    (repo / "unauthorized.py").write_text("unauthorized\n", encoding="utf-8")
    dirty_hash = hash_workspace_path(repo, "unauthorized.py").as_dict()
    legacy = _baseline(repo, (dirty_hash,))

    report = verify_workspace(
        repo,
        "01",
        legacy,
        repo.parent / "untrusted-standalone-manifest.json",
    )

    assert not report.ok
    assert report.baseline_commit == ""
    assert any(
        violation.kind == "baseline_invalid"
        and "schema-v2" in violation.message
        for violation in report.violations
    )


def test_standalone_manifest_schema_mismatch_reports_actual_required_version() -> None:
    manifest = {
        "schema_version": 1,
        "manifest_kind": "standalone",
        "execution_baseline_commit": "unused",
        "phases": {},
    }

    with pytest.raises(
        workspace_guard.AllowlistError,
        match=(
            "standalone manifest schema_version must be "
            f"{workspace_guard.STANDALONE_ALLOWLIST_SCHEMA_VERSION}"
        ),
    ):
        workspace_guard._validate_standalone_manifest(manifest)


def test_phase_01_plan_and_manifest_cannot_self_authorize_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, phase_dir, _, _, _, checked_manifest = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    baseline = _capture_destination(repo)
    manifest = json.loads(checked_manifest.read_text(encoding="utf-8"))

    first_plan = phase_dir / "01-01-PLAN.md"
    first_plan.write_text(
        _standalone_plan(
            "01",
            "options_copilot/phase1_01.py",
            "options_copilot/unauthorized.py",
        ),
        encoding="utf-8",
    )
    unauthorized = "options_copilot/unauthorized.py"
    phase_01 = manifest["phases"]["01"]
    phase_01["plans"][0]["files_modified"].append(unauthorized)
    phase_01["plans"][0]["content_sha256"] = hashlib.sha256(
        first_plan.read_bytes()
    ).hexdigest()
    phase_01["buckets"]["declared_plan_files"].append(unauthorized)
    phase_01["buckets"]["declared_plan_files"].sort()
    phase_01["allowlist"].append(unauthorized)
    phase_01["allowlist"].sort()
    phase_01["provenance"][unauthorized] = [
        "declared_plan_files",
        "plan:01-01",
    ]
    checked_manifest.write_text(json.dumps(manifest), encoding="utf-8")
    source = repo / unauthorized
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("unauthorized = True\n", encoding="utf-8")

    report = verify_workspace(repo, "01", baseline, checked_manifest)

    assert not report.ok
    assert any(
        violation.kind == "manifest_invalid" for violation in report.violations
    )
    assert any(
        violation.kind == "out_of_scope" and violation.path == unauthorized
        for violation in report.violations
    )


def test_phase_02_plan_and_manifest_coedit_cannot_self_authorize_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, phase_01_dir, phase_02_dir, _, _, manifest_path = (
        _phase_02_authority_repository(tmp_path, monkeypatch)
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    first_plan = phase_02_dir / "02-01-PLAN.md"
    unauthorized = "options_copilot/unauthorized.py"
    first_plan.write_text(
        _phase_plan(
            "02-advisory-api-and-secondary-evidence",
            "01",
            "options_copilot/phase2_01.py",
            unauthorized,
        ),
        encoding="utf-8",
    )
    phase_02 = manifest["phases"]["02"]
    phase_02["plans"][0]["files_modified"].append(unauthorized)
    phase_02["plans"][0]["content_sha256"] = hashlib.sha256(
        first_plan.read_bytes()
    ).hexdigest()
    phase_02["buckets"]["declared_plan_files"].append(unauthorized)
    phase_02["buckets"]["declared_plan_files"].sort()
    phase_02["allowlist"].append(unauthorized)
    phase_02["allowlist"].sort()
    phase_02["provenance"][unauthorized] = [
        "declared_plan_files",
        "plan:02-01",
    ]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    source = repo / unauthorized
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("unauthorized = True\n", encoding="utf-8")

    report = verify_workspace(repo, "02", _capture_destination(repo), manifest_path)

    assert phase_01_dir.is_dir()
    assert not report.ok
    assert any(
        violation.kind == "manifest_invalid" for violation in report.violations
    )
    assert any(
        violation.kind == "out_of_scope" and violation.path == unauthorized
        for violation in report.violations
    )


def test_phase_02_manifest_rejects_wrong_immutable_plan_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _, _, _, _, manifest_path = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["phases"]["02"]["plans"][0]["content_sha256"] = "b" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    report = verify_workspace(repo, "02", _capture_destination(repo), manifest_path)

    assert not report.ok
    assert any(
        violation.kind == "manifest_invalid" for violation in report.violations
    )


def test_phase_02_rejects_future_phase_and_undeclared_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _, _, _, _, manifest_path = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    future = repo / ".planning/phases/03-future/03-01-PLAN.md"
    future.parent.mkdir(parents=True)
    future.write_text("future\n", encoding="utf-8")
    undeclared = repo / "options_copilot/undeclared.py"
    undeclared.parent.mkdir(parents=True, exist_ok=True)
    undeclared.write_text("undeclared = True\n", encoding="utf-8")

    report = verify_workspace(repo, "02", _capture_destination(repo), manifest_path)

    assert not report.ok
    assert {
        violation.path
        for violation in report.violations
        if violation.kind in {"out_of_scope", "unsafe_changed_path"}
    } == {
        ".planning/phases/03-future/03-01-PLAN.md",
        "options_copilot/undeclared.py",
    }


def test_phase_02_ignored_evidence_prefix_is_never_commit_eligible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _, _, _, _, manifest_path = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    evidence = repo / "data/options_copilot/evidence/phase-02/live/checkpoint.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text("{}\n", encoding="utf-8")
    _git(repo, "add", "--force", evidence.relative_to(repo).as_posix())

    report = verify_workspace(repo, "02", _capture_destination(repo), manifest_path)

    assert not report.ok
    assert any(
        violation.kind == "ignored_evidence_commit_forbidden"
        and violation.path == evidence.relative_to(repo).as_posix()
        for violation in report.violations
    )


def test_phase_02_current_head_plan_substitution_does_not_grant_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, phase_01_dir, phase_02_dir, _, _, manifest_path = (
        _phase_02_authority_repository(tmp_path, monkeypatch)
    )
    pinned = json.loads(manifest_path.read_text(encoding="utf-8"))
    pinned_hash = pinned["phases"]["02"]["plans"][0]["content_sha256"]
    unauthorized = "options_copilot/head_substitution.py"
    _commit(
        repo,
        {
            phase_02_dir.relative_to(repo).as_posix() + "/02-01-PLAN.md": _phase_plan(
                "02-advisory-api-and-secondary-evidence",
                "01",
                "options_copilot/phase2_01.py",
                unauthorized,
            ),
            unauthorized: "head_substitution = True\n",
        },
    )

    rebuilt = workspace_guard.build_standalone_phase_allowlist(repo, phase_01_dir)
    report = verify_workspace(repo, "02", _capture_destination(repo), manifest_path)

    assert rebuilt["phases"]["02"]["plans"][0]["content_sha256"] == pinned_hash
    assert unauthorized not in rebuilt["phases"]["02"]["allowlist"]
    assert not report.ok
    assert any(
        violation.kind == "out_of_scope" and violation.path == unauthorized
        for violation in report.violations
    )


@pytest.mark.parametrize("mutation", ("missing", "surplus", "phase_crossover"))
def test_phase_02_authority_tree_rejects_inexact_plan_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    repo, phase_01_dir, phase_02_dir, _, _, _ = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    if mutation == "missing":
        (phase_02_dir / "02-10-PLAN.md").unlink()
        bad_authority = _commit(repo, {})
    elif mutation == "surplus":
        bad_authority = _commit(
            repo,
            {
                phase_02_dir.relative_to(repo).as_posix() + "/02-11-PLAN.md": _phase_plan(
                    "02-advisory-api-and-secondary-evidence",
                    "11",
                    "options_copilot/surplus_plan.py",
                )
            },
        )
    else:
        bad_authority = _commit(
            repo,
            {
                phase_02_dir.relative_to(repo).as_posix() + "/02-01-PLAN.md": _phase_plan(
                    "03-future",
                    "01",
                    "options_copilot/phase2_01.py",
                )
            },
        )
    monkeypatch.setattr(
        workspace_guard,
        "PHASE2_PLAN_AUTHORITY_COMMIT",
        bad_authority,
        raising=False,
    )

    with pytest.raises(workspace_guard.AllowlistError):
        workspace_guard.build_standalone_phase_allowlist(repo, phase_01_dir)


def test_phase_01_allows_only_exact_auto_review_iteration_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, phase_dir, _, _, _, manifest_path = (
        _phase_02_authority_repository(
            tmp_path,
            monkeypatch,
        )
    )
    monkeypatch.setattr(
        workspace_guard,
        "STANDALONE_BASELINE_COMMIT",
        workspace_guard.PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT,
    )
    baseline = (
        repo
        / "data"
        / "options_copilot"
        / "evidence"
        / "workspace"
        / "phase-01"
        / "review_iteration_baseline.json"
    )
    capture_workspace_baseline(repo, baseline)
    manifest = workspace_guard.build_standalone_phase_allowlist(repo, phase_dir)
    manifest_path.write_text(
        workspace_guard.render_standalone_phase_allowlist(manifest),
        encoding="utf-8",
    )
    phase_relative = ".planning/phases/01-hermetic-standalone-foundation"

    accepted_paths = (
        f"{phase_relative}/01-REVIEW.iter2.md",
        f"{phase_relative}/01-REVIEW-FIX.iter17.md",
    )
    for relative in accepted_paths:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("review evidence\n", encoding="utf-8")

    accepted = verify_workspace(repo, "01", baseline, manifest_path)

    assert accepted.ok, [violation.as_dict() for violation in accepted.violations]
    assert set(accepted_paths).issubset(accepted.changed_paths)

    rejected_paths = (
        f"{phase_relative}/01-REVIEW.iter0.md",
        f"{phase_relative}/01-REVIEW.iter02.md",
        f"{phase_relative}/01-REVIEW-FIX.iterx.md",
        f"{phase_relative}/nested/01-REVIEW.iter2.md",
        ".planning/phases/02-other/01-REVIEW.iter2.md",
    )
    for relative in rejected_paths:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not approved metadata\n", encoding="utf-8")

    rejected = verify_workspace(repo, "01", baseline, manifest_path)

    assert not rejected.ok
    assert {
        violation.path
        for violation in rejected.violations
        if violation.kind == "out_of_scope"
    } == set(rejected_paths)


def test_phase_02_allows_prior_and_current_review_iteration_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _, _, _, _, manifest_path = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    baseline = _capture_destination(repo)
    accepted_paths = (
        (
            ".planning/phases/01-hermetic-standalone-foundation/"
            "01-REVIEW.iter2.md"
        ),
        (
            ".planning/phases/02-advisory-api-and-secondary-evidence/"
            "02-REVIEW-FIX.iter3.md"
        ),
    )
    for relative in accepted_paths:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("review evidence\n", encoding="utf-8")

    accepted = verify_workspace(repo, "02", baseline, manifest_path)

    assert accepted.ok, [violation.as_dict() for violation in accepted.violations]
    assert set(accepted_paths).issubset(accepted.changed_paths)

    rejected_paths = (
        (
            ".planning/phases/01-hermetic-standalone-foundation/"
            "02-REVIEW.iter2.md"
        ),
        (
            ".planning/phases/02-advisory-api-and-secondary-evidence/"
            "01-REVIEW-FIX.iter3.md"
        ),
        ".planning/phases/03-future/03-REVIEW.iter2.md",
    )
    for relative in rejected_paths:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not approved metadata\n", encoding="utf-8")

    rejected = verify_workspace(repo, "02", baseline, manifest_path)

    assert not rejected.ok
    assert {
        violation.path
        for violation in rejected.violations
        if violation.kind == "out_of_scope"
    } == set(rejected_paths)


def test_phase_02_allows_only_the_exact_non_authorizing_deferred_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _, _, _, _, manifest_path = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    baseline = _capture_destination(repo)
    exact = workspace_guard._PHASE2_DEFERRED_METADATA_PATH
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    phase_02 = manifest["phases"]["02"]
    assert exact not in phase_02["allowlist"]
    assert all(exact not in paths for paths in phase_02["buckets"].values())

    accepted_path = repo / exact
    accepted_path.parent.mkdir(parents=True, exist_ok=True)
    accepted_path.write_text("pre-existing deferred metadata\n", encoding="utf-8")

    accepted = verify_workspace(repo, "02", baseline, manifest_path)

    assert accepted.ok, [violation.as_dict() for violation in accepted.violations]
    assert exact in accepted.changed_paths

    rejected_paths = (
        (
            ".planning/phases/02-advisory-api-and-secondary-evidence/"
            "deferred_items.md"
        ),
        (
            ".planning/phases/02-advisory-api-and-secondary-evidence/nested/"
            "deferred-items.md"
        ),
        (
            ".planning/phases/01-hermetic-standalone-foundation/"
            "deferred-items.md"
        ),
        "options_copilot/deferred-items.md",
    )
    for relative in rejected_paths:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not authorized metadata\n", encoding="utf-8")

    rejected = verify_workspace(repo, "02", baseline, manifest_path)

    assert not rejected.ok
    assert {
        violation.path
        for violation in rejected.violations
        if violation.kind in {"out_of_scope", "unsafe_changed_path"}
    } == set(rejected_paths)


def test_phase_02_allows_only_finite_non_authorizing_omx_project_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, _, _, _, _, manifest_path = _phase_02_authority_repository(
        tmp_path,
        monkeypatch,
    )
    baseline = _capture_destination(repo)
    accepted_paths = (
        ".gitignore",
        ".codex/agents/executor.toml",
        ".codex/prompts/code-reviewer.md",
        ".codex/skills/ultragoal/SKILL.md",
        ".codex/skills/prometheus-strict/README.md",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    phase_02 = manifest["phases"]["02"]
    assert all(path not in phase_02["allowlist"] for path in accepted_paths)
    for relative in accepted_paths:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        content = (
            path.read_text(encoding="utf-8") + "# OMX project metadata\n"
            if relative == ".gitignore"
            else "project tooling metadata\n"
        )
        path.write_text(content, encoding="utf-8")

    accepted = verify_workspace(repo, "02", baseline, manifest_path)

    assert accepted.ok, [violation.as_dict() for violation in accepted.violations]
    assert set(accepted_paths).issubset(accepted.changed_paths)

    rejected_paths = (
        ".gitignore.bak",
        ".codex/agents/executor.py",
        ".codex/agents/nested/executor.toml",
        ".codex/prompts/nested/reviewer.md",
        ".codex/skills/ultragoal/scripts/run.py",
        ".codex/skills/ultragoal/INDEX.md",
        ".codexx/agents/executor.toml",
        "options_copilot/.codex/agents/executor.toml",
    )
    for relative in rejected_paths:
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not project metadata\n", encoding="utf-8")

    rejected = verify_workspace(repo, "02", baseline, manifest_path)

    assert not rejected.ok
    assert {
        violation.path
        for violation in rejected.violations
        if violation.kind in {"out_of_scope", "unsafe_changed_path"}
    } == set(rejected_paths)


def test_guard_allows_only_phase_paths_and_never_mutates_git(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    _commit(repo, {"allowed.py": "before\n", "stable.py": "stable\n"})
    baseline = _baseline(repo)
    manifest = _manifest(repo, ("allowed.py",))
    (repo / "allowed.py").write_text("after\n", encoding="utf-8")
    status_before = _git(repo, "status", "--porcelain=v2", "--untracked-files=all")
    evidence_before = baseline.read_bytes()

    allowed = verify_workspace(repo, "P0", baseline, manifest)

    assert allowed.ok, [violation.as_dict() for violation in allowed.violations]
    assert allowed.changed_paths == ("allowed.py",)
    assert _git(repo, "status", "--porcelain=v2", "--untracked-files=all") == status_before
    assert baseline.read_bytes() == evidence_before

    (repo / "outside.txt").write_text("not allowed\n", encoding="utf-8")
    rejected = verify_workspace(repo, "P0", baseline, manifest)

    assert not rejected.ok
    assert any(
        violation.kind == "out_of_scope" and violation.path == "outside.txt"
        for violation in rejected.violations
    )


def test_baseline_accepts_linked_worktree_but_rejects_different_repository(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    _commit(repo, {"stable.py": "stable\n"})
    baseline = _baseline(repo)

    linked_worktree = tmp_path / "linked-worktree"
    _git(repo, "worktree", "add", "--detach", str(linked_worktree), "HEAD")
    linked_manifest = _manifest(linked_worktree, ())

    accepted = verify_workspace(linked_worktree, "P0", baseline, linked_manifest)

    assert accepted.ok, [violation.as_dict() for violation in accepted.violations]
    assert accepted.changed_paths == ()

    different_parent = tmp_path / "different"
    different_parent.mkdir()
    different_repo = _repository(different_parent)
    _commit(different_repo, {"stable.py": "stable\n"})
    different_manifest = _manifest(different_repo, ())

    rejected = verify_workspace(different_repo, "P0", baseline, different_manifest)

    assert not rejected.ok
    assert any(
        violation.kind == "baseline_invalid"
        and "different repository" in violation.message
        for violation in rejected.violations
    )


def test_unchanged_preexisting_dirty_file_passes_but_byte_change_fails(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    _commit(repo, {"protected.txt": "committed\n", "planned.py": "before\n"})
    (repo / "protected.txt").write_text("user bytes\n", encoding="utf-8")
    protected_hash = hash_workspace_path(repo, "protected.txt").as_dict()
    baseline = _baseline(repo, (protected_hash,))
    manifest = _manifest(repo, ("planned.py",))
    (repo / "planned.py").write_text("after\n", encoding="utf-8")

    unchanged = verify_workspace(repo, "P0", baseline, manifest)

    assert unchanged.ok, [violation.as_dict() for violation in unchanged.violations]
    assert unchanged.preserved_dirty_paths == ("protected.txt",)
    assert unchanged.changed_paths == ("planned.py",)

    (repo / "protected.txt").write_text("user bytes changed\n", encoding="utf-8")
    changed = verify_workspace(repo, "P0", baseline, manifest)

    assert not changed.ok
    assert any(
        violation.kind == "baseline_path_changed"
        and violation.path == "protected.txt"
        for violation in changed.violations
    )


def test_baseline_requires_a_hash_for_every_preexisting_dirty_path(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    _commit(repo, {"protected.txt": "committed\n"})
    (repo / "protected.txt").write_text("dirty\n", encoding="utf-8")
    incomplete = _baseline(repo)
    manifest = _manifest(repo, ())

    report = verify_workspace(repo, "P0", incomplete, manifest)

    assert not report.ok
    assert any(violation.kind == "baseline_invalid" for violation in report.violations)


def test_matching_standard_plan_summary_is_metadata_not_implementation_scope(
    tmp_path: Path,
) -> None:
    repo = _repository(tmp_path)
    _commit(
        repo,
        {
            "options_copilot/plans/P0-02-PLAN.md": "<output>summary required</output>\n",
        },
    )
    baseline = _baseline(repo)
    manifest = _manifest(repo, ())
    summary = repo / "options_copilot" / "plans" / "P0-02-SUMMARY.md"
    summary.write_text("# P0-02 Summary\n", encoding="utf-8")

    accepted = verify_workspace(repo, "P0", baseline, manifest)

    assert accepted.ok, [violation.as_dict() for violation in accepted.violations]
    assert accepted.changed_paths == ("options_copilot/plans/P0-02-SUMMARY.md",)

    unmatched = repo / "options_copilot" / "plans" / "P0-99-SUMMARY.md"
    unmatched.write_text("# Unmatched\n", encoding="utf-8")
    rejected = verify_workspace(repo, "P0", baseline, manifest)

    assert not rejected.ok
    assert any(
        violation.kind == "out_of_scope"
        and violation.path == "options_copilot/plans/P0-99-SUMMARY.md"
        for violation in rejected.violations
    )
