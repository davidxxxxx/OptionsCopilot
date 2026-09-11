"""Read-only workspace and exact allowlist enforcement.

The guard has two independent jobs:

* prove that executable plan declarations, task file lists, the master phase
  declarations, and the checked JSON manifest are exact set-equal views; and
* compare the current worktree with an immutable pre-edit baseline without
  invoking any Git command that can mutate user state.

All paths are repository-relative POSIX paths.  Directory permissions are
explicit prefixes ending in ``/``; wildcard permissions are never accepted.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
from typing import Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
STANDALONE_BASELINE_SCHEMA_VERSION = 2
STANDALONE_BASELINE_COMMIT = "7237246"
STANDALONE_BASELINE_FULL_COMMIT = "72372464e99dda342af5934bb8641900970dc013"
STANDALONE_ALLOWLIST_SCHEMA_VERSION = 3
STANDALONE_PLAN_AUTHORITY_COMMIT = "a20fc3488b63282248b7cc9b6748d56186ad1202"
PHASE2_PLAN_AUTHORITY_COMMIT = "cc4089c9d5e02a1ff13a8b3df35b2565e0d15f45"
PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT = (
    "1b0b6c98f19d5a5528f401374d4ebfbf8df78297"
)
EXPECTED_PHASES = tuple(f"P{number}" for number in range(11))
EXPECTED_STANDALONE_PLANS = tuple(f"01-{number:02d}" for number in range(1, 5))
EXPECTED_PHASE2_PLANS = tuple(f"02-{number:02d}" for number in range(1, 11))
_PLAN_NAME = re.compile(r"(P(?:10|[0-9]))-([0-9]{2})-PLAN\.md\Z")
_SUMMARY_PATH = re.compile(
    r"options_copilot/plans/(P(?:10|[0-9]))-([0-9]{2})-SUMMARY\.md\Z"
)
_PHASE_HEADING = re.compile(
    r"^### (P(?:10|[0-9])) declared phase allowlist\s*$"
)
_TASK_FILES = re.compile(r"<files>(.*?)</files>", re.DOTALL)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40,64}\Z")
_GLOB_CHARACTERS = frozenset("*?[]{}")
_SCRIPT_SUFFIXES = frozenset({".ps1", ".py", ".sh", ".cmd", ".bat"})
_PROHIBITED_DIRECTORY_NAMES = frozenset(
    {
        ".mypy_cache",
        ".nox",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        ".venv",
        "__pycache__",
        "env",
        "htmlcov",
        "logs",
        "migration_backups",
        "node_modules",
        "site-packages",
        "venv",
        "virtualenv",
    }
)
_PROHIBITED_FILE_SUFFIXES = (
    ".bak",
    ".log",
    ".pyc",
    ".pyo",
    ".sqlite",
    ".sqlite3",
    ".swp",
    ".swo",
    ".temp",
    ".tmp",
    "-shm",
    "-wal",
    "~",
)
_SECRET_FILE_MARKERS = frozenset(
    {"apikey", "credential", "providerkey", "secret"}
)
_SECRET_FILE_SUFFIXES = frozenset(
    {".cfg", ".env", ".ini", ".json", ".key", ".pem", ".toml", ".txt", ".yaml", ".yml"}
)
_STANDALONE_MANIFEST_KIND = "options-copilot-standalone-phase-allowlist"
_STANDALONE_PHASE_BUCKETS = (
    "audited_exceptions",
    "declared_plan_files",
    "gsd_metadata",
    "ignored_evidence_prefixes",
    "prior_phase_allowlist",
    "supplemental_authority",
)
_STANDALONE_AUDITED_EXCEPTIONS = (
    "AGENTS.md",
    "tests/options_copilot/test_bridge_coordinator.py",
)
_STANDALONE_GSD_METADATA = (
    ".planning/PROJECT.md",
    ".planning/REQUIREMENTS.md",
    ".planning/ROADMAP.md",
    ".planning/STATE.md",
    ".planning/config.json",
    ".planning/phases/01-hermetic-standalone-foundation/01-01-PLAN.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-01-SUMMARY.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-02-PLAN.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-02-SUMMARY.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-03-PLAN.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-03-SUMMARY.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-04-PLAN.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-04-SUMMARY.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-CONTEXT.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-PATTERNS.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-RESEARCH.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-REVIEW-FIX.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-REVIEW.md",
    ".planning/phases/01-hermetic-standalone-foundation/01-VALIDATION.md",
)
_PHASE2_GSD_METADATA = tuple(
    sorted(
        {
            ".planning/phases/01-hermetic-standalone-foundation/01-VERIFICATION.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-AI-SPEC.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-CONTEXT.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-PATTERNS.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-RESEARCH.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-REVIEW-FIX.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-REVIEW.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-VALIDATION.md",
            ".planning/phases/02-advisory-api-and-secondary-evidence/02-VERIFICATION.md",
            *(
                f".planning/phases/02-advisory-api-and-secondary-evidence/"
                f"02-{number:02d}-{kind}.md"
                for number in range(1, 11)
                for kind in ("PLAN", "SUMMARY")
            ),
        }
    )
)
_PHASE2_IGNORED_EVIDENCE_PREFIXES = (
    "data/options_copilot/evidence/phase-02/live/",
)
_PHASE2_SUPPLEMENTAL_AUTHORITY_PATH = (
    ".planning/phases/02-advisory-api-and-secondary-evidence/"
    "02-09-SUPPLEMENTAL-AUTHORITY.json"
)
_PHASE2_SUPPLEMENTAL_AUTHORITY_KIND = (
    "options_copilot.phase2_supplemental_plan_authority.v1"
)
_PHASE2_SUPPLEMENTAL_AUTHORIZED_PATHS = (
    "options_copilot/operations/authority_audit.py",
    "tests/options_copilot/test_authority_audit.py",
)
_PHASE2_SUPPLEMENTAL_CONTENT_SHA256 = {
    "options_copilot/operations/authority_audit.py": (
        "e29055b626c0f80e2b943cf51a970b02c1ba443ce584bbf056993d736d757d2b"
    ),
    "tests/options_copilot/test_authority_audit.py": (
        "360577934dce5fea88b7957e3378839b0fe1a5c8ff9ea5d8e7b7fbbb63316213"
    ),
}
_PHASE2_SUPPLEMENTAL_ANCHOR_BLOB_SHA256 = {
    "options_copilot/operations/authority_audit.py": (
        "7ba27de877d2cdd31bf050e916648dc00507c393dca0d26af516432a52bfdf5b"
    ),
    "tests/options_copilot/test_authority_audit.py": (
        "60cff798f4171d772f0157ef093c4d2a609e479818b52167c84c6b294b2212a2"
    ),
}
_PHASE2_SUPPLEMENTAL_RECORD_SHA256 = (
    "82621b7ab6fc039ef63cc6ee27b9e720ddce430a80d80c4d3a0a50d8802b1532"
)
_PHASE2_SUPPLEMENTAL_REASON = (
    "Rule 3 release-gate repair for repository-root production authority "
    "audit scoping"
)
_PHASE2_SUPPLEMENTAL_SCAN_CONTRACT = {
    "excluded_nonproduction_roots": [
        ".venv/",
        "migration_backups/",
        "tests/",
    ],
    "production_roots": [
        "options_copilot/",
        "scripts/",
    ],
}
_PHASE2_DEFERRED_METADATA_PATH = (
    ".planning/phases/02-advisory-api-and-secondary-evidence/deferred-items.md"
)
_OMX_PROJECT_METADATA_PATH = re.compile(
    r"^\.codex/(?:"
    r"agents/[a-z0-9][a-z0-9-]*\.toml|"
    r"prompts/[a-z0-9][a-z0-9-]*\.md|"
    r"skills/[a-z0-9][a-z0-9-]*/(?:SKILL|README)\.md"
    r")$"
)
_STANDALONE_REVIEW_ITERATION_PATH = re.compile(
    r"^\.planning/phases/(01-hermetic-standalone-foundation|"
    r"02-advisory-api-and-secondary-evidence)/"
    r"(01|02)-REVIEW(?:-FIX)?\.iter[1-9][0-9]*\.md$"
)
_BASELINE_COMMANDS = (
    "git rev-parse HEAD",
    "git status --porcelain=v2 --untracked-files=all",
    "git diff --name-status",
    "git diff --cached --name-status",
)
_READ_ONLY_GIT_COMMANDS = frozenset(
    {"cat-file", "diff", "ls-files", "merge-base", "rev-parse", "status"}
)
_MISSING_DIGEST = hashlib.sha256(b"workspace-guard:missing:v1").hexdigest()


class WorkspaceGuardError(RuntimeError):
    pass


class ScopePathError(WorkspaceGuardError, ValueError):
    pass


class PlanParseError(WorkspaceGuardError, ValueError):
    pass


class AllowlistError(WorkspaceGuardError, ValueError):
    pass


class BaselineError(WorkspaceGuardError, ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PlanScope:
    plan_id: str
    phase: str
    phase_title: str
    path: Path
    files_modified: tuple[str, ...]
    evidence_prefixes: tuple[str, ...]
    task_files: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ScopeDifference:
    source: str
    missing: tuple[str, ...]
    surplus: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.missing and not self.surplus

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "missing": list(self.missing),
            "surplus": list(self.surplus),
        }


@dataclass(frozen=True, slots=True)
class ConsistencyIssue:
    kind: str
    source: str
    message: str
    missing: tuple[str, ...] = ()
    surplus: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "source": self.source,
            "message": self.message,
            "missing": list(self.missing),
            "surplus": list(self.surplus),
        }


@dataclass(frozen=True, slots=True)
class AllowlistConsistencyReport:
    plan_count: int
    manifest: Mapping[str, object]
    issues: tuple[ConsistencyIssue, ...]

    @property
    def ok(self) -> bool:
        return not self.issues

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "ok": self.ok,
            "plan_count": self.plan_count,
            "issues": [issue.as_dict() for issue in self.issues],
            "manifest": dict(self.manifest),
        }


@dataclass(frozen=True, slots=True)
class WorkspacePathHash:
    path: str
    kind: str
    size: int
    sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "kind": self.kind,
            "size": self.size,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceBaseline:
    path: Path
    schema_version: int
    repository_root: Path
    captured_at_utc: datetime
    source_baseline_commit: str
    execution_baseline_commit: str
    captured_head_commit: str
    command_stdout: Mapping[str, str]
    dirty_path_hashes: Mapping[str, WorkspacePathHash]

    @property
    def commit(self) -> str:
        """Return the execution authority for legacy report consumers."""

        return self.execution_baseline_commit


@dataclass(frozen=True, slots=True)
class WorkspaceViolation:
    kind: str
    path: str
    message: str
    expected: str | None = None
    actual: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "path": self.path,
            "message": self.message,
            "expected": self.expected,
            "actual": self.actual,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceReport:
    root: Path
    phase: str
    baseline_commit: str
    changed_paths: tuple[str, ...]
    preserved_dirty_paths: tuple[str, ...]
    violations: tuple[WorkspaceViolation, ...]

    @property
    def ok(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "ok": self.ok,
            "root": self.root.as_posix(),
            "phase": self.phase,
            "baseline_commit": self.baseline_commit,
            "changed_paths": list(self.changed_paths),
            "preserved_dirty_paths": list(self.preserved_dirty_paths),
            "violations": [violation.as_dict() for violation in self.violations],
        }


def validate_scope_path(value: str) -> str:
    """Validate one exact implementation permission and return it unchanged."""

    normalized = _normalize_relative_path(value)
    _reject_prohibited_artifact(normalized)
    return _validate_implementation_path(normalized)


def _validate_implementation_path(normalized: str) -> str:
    body = normalized[:-1] if normalized.endswith("/") else normalized
    segments = body.split("/")
    compact_segments = tuple(
        re.sub(r"[^a-z0-9]", "", segment.casefold()) for segment in segments
    )
    if any(segment.casefold() == ".planning" for segment in segments):
        raise ScopePathError(".planning paths are never implementation scope")
    if any(segment == "tradecopilot" for segment in compact_segments):
        raise ScopePathError("trade_copilot paths are outside Options Copilot scope")
    if any("rithmic" in segment for segment in compact_segments):
        raise ScopePathError("Rithmic paths are outside Options Copilot scope")
    if any("paperserver" in segment for segment in compact_segments):
        raise ScopePathError("paper-server paths are outside Options Copilot scope")
    if segments[0].casefold() == "scripts":
        if normalized.endswith("/"):
            raise ScopePathError("script permissions must name an exact file")
        suffix = Path(segments[-1]).suffix.casefold()
        if suffix not in _SCRIPT_SUFFIXES:
            raise ScopePathError("script permissions require a named script file")
    return normalized


def parse_plan(path: str | Path) -> PlanScope:
    plan_path = Path(path)
    match = _PLAN_NAME.fullmatch(plan_path.name)
    if match is None:
        raise PlanParseError(f"invalid executable plan filename: {plan_path.name}")
    try:
        text = plan_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PlanParseError(f"cannot read plan {plan_path}: {exc}") from exc
    frontmatter = _frontmatter(text, plan_path)
    phase_title = _scalar(frontmatter, "phase", plan_path)
    phase = match.group(1)
    if not phase_title.startswith(phase + "-"):
        raise PlanParseError(
            f"{plan_path.name} phase frontmatter does not begin with {phase}-"
        )
    files_modified = _frontmatter_paths(
        frontmatter, "files_modified", plan_path, required=True
    )
    evidence_prefixes = _frontmatter_paths(
        frontmatter, "evidence_prefixes", plan_path, required=False
    )
    for prefix in evidence_prefixes:
        if not prefix.endswith("/"):
            raise PlanParseError(
                f"{plan_path.name} evidence prefix must end in '/': {prefix}"
            )
    task_files: list[str] = []
    for raw_group in _TASK_FILES.findall(text):
        for raw in re.split(r",|\r?\n", raw_group):
            candidate = raw.strip()
            if candidate.startswith("- "):
                candidate = candidate[2:].strip()
            candidate = _unquote(candidate)
            if candidate:
                task_files.append(_plan_path(candidate, plan_path))
    if not task_files:
        raise PlanParseError(f"{plan_path.name} contains no task <files> entries")
    return PlanScope(
        plan_id=f"{phase}-{match.group(2)}",
        phase=phase,
        phase_title=phase_title,
        path=plan_path.resolve(),
        files_modified=files_modified,
        evidence_prefixes=evidence_prefixes,
        task_files=tuple(sorted(set(task_files))),
    )


def parse_master_allowlists(path: str | Path) -> dict[str, tuple[str, ...]]:
    master_path = Path(path)
    try:
        lines = master_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise PlanParseError(f"cannot read master plan {master_path}: {exc}") from exc
    values: dict[str, list[str]] = {}
    current: str | None = None
    for line_number, line in enumerate(lines, start=1):
        heading = _PHASE_HEADING.fullmatch(line)
        if heading is not None:
            current = heading.group(1)
            if current in values:
                raise PlanParseError(f"duplicate {current} master allowlist heading")
            values[current] = []
            continue
        if line.startswith("##"):
            current = None
            continue
        if current is None or not line.startswith("-"):
            continue
        item = re.fullmatch(r"- `([^`]+)`\s*", line)
        if item is None:
            raise PlanParseError(
                f"{master_path.name}:{line_number} has a non-exact allowlist item"
            )
        values[current].append(_plan_path(item.group(1), master_path))
    if set(values) != set(EXPECTED_PHASES):
        missing = sorted(set(EXPECTED_PHASES).difference(values), key=_phase_key)
        surplus = sorted(set(values).difference(EXPECTED_PHASES), key=_phase_key)
        raise PlanParseError(
            "master phase headings are not exactly P0-P10; "
            f"missing={missing}, surplus={surplus}"
        )
    result: dict[str, tuple[str, ...]] = {}
    for phase in EXPECTED_PHASES:
        paths = values[phase]
        if not paths:
            raise PlanParseError(f"{phase} master allowlist is empty")
        if len(paths) != len(set(paths)):
            raise PlanParseError(f"{phase} master allowlist contains duplicates")
        result[phase] = tuple(sorted(paths))
    return result


def validate_plan_allowlist(plan: PlanScope) -> ScopeDifference:
    declared = set(plan.files_modified)
    expected = set(plan.task_files).union(plan.evidence_prefixes)
    return ScopeDifference(
        source=plan.plan_id,
        missing=tuple(sorted(expected.difference(declared))),
        surplus=tuple(sorted(declared.difference(expected))),
    )


def validate_phase_allowlist(
    phase: str,
    declared: Iterable[str],
    plans: Iterable[PlanScope],
) -> ScopeDifference:
    if phase not in EXPECTED_PHASES:
        raise AllowlistError(f"unknown phase: {phase}")
    declared_set = {_validate_historical_scope_path(item) for item in declared}
    phase_plans = tuple(plan for plan in plans if plan.phase == phase)
    expected = {
        item for plan in phase_plans for item in plan.files_modified
    }
    return ScopeDifference(
        source=phase,
        missing=tuple(sorted(expected.difference(declared_set))),
        surplus=tuple(sorted(declared_set.difference(expected))),
    )


def build_phase_allowlists(
    master_plan: str | Path,
    plans_directory: str | Path,
) -> dict[str, object]:
    report = verify_allowlist_consistency(master_plan, plans_directory)
    if not report.ok:
        rendered = "; ".join(issue.message for issue in report.issues)
        raise AllowlistError(f"source allowlists are inconsistent: {rendered}")
    return dict(report.manifest)


def render_phase_allowlists(manifest: Mapping[str, object]) -> str:
    _validate_manifest(manifest)
    return json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"


def load_phase_allowlists(path: str | Path) -> dict[str, object]:
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AllowlistError(f"cannot read phase allowlist manifest: {exc}") from exc
    if not isinstance(payload, dict):
        raise AllowlistError("phase allowlist manifest must be a JSON object")
    _validate_manifest(payload)
    return payload


def build_standalone_phase_allowlist(
    root: str | Path,
    plans_directory: str | Path,
) -> dict[str, object]:
    """Build cumulative Phase 01/02 scope from fixed immutable plan blobs."""

    root_path = Path(root).resolve()
    _git_repository_identity(root_path)
    phase_01_directory = Path(plans_directory).resolve()
    expected_phase_01_directory = (
        root_path
        / ".planning"
        / "phases"
        / "01-hermetic-standalone-foundation"
    ).resolve()
    if phase_01_directory != expected_phase_01_directory:
        raise AllowlistError("standalone Phase 01 plans directory is not exact")
    phase_02_directory = (
        root_path
        / ".planning"
        / "phases"
        / "02-advisory-api-and-secondary-evidence"
    ).resolve()
    supplemental_authority = _phase2_supplemental_authority_record(root_path)
    phase_01_plans = _standalone_plan_records(
        root_path,
        phase="01",
        phase_title="01-hermetic-standalone-foundation",
        plans_directory=phase_01_directory,
        expected_plan_ids=EXPECTED_STANDALONE_PLANS,
        authority_commit=STANDALONE_PLAN_AUTHORITY_COMMIT,
    )
    phase_01_record = _build_standalone_phase_record(
        phase="01",
        authority_commit=STANDALONE_PLAN_AUTHORITY_COMMIT,
        plans=phase_01_plans,
        audited_exceptions=_STANDALONE_AUDITED_EXCEPTIONS,
        gsd_metadata=_STANDALONE_GSD_METADATA,
        ignored_evidence_prefixes=(),
        prior_phase_allowlist=(),
        supplemental_authority=None,
    )
    phase_02_plans = _standalone_plan_records(
        root_path,
        phase="02",
        phase_title="02-advisory-api-and-secondary-evidence",
        plans_directory=phase_02_directory,
        expected_plan_ids=EXPECTED_PHASE2_PLANS,
        authority_commit=PHASE2_PLAN_AUTHORITY_COMMIT,
    )
    phase_02_record = _build_standalone_phase_record(
        phase="02",
        authority_commit=PHASE2_PLAN_AUTHORITY_COMMIT,
        plans=phase_02_plans,
        audited_exceptions=(),
        gsd_metadata=_PHASE2_GSD_METADATA,
        ignored_evidence_prefixes=_PHASE2_IGNORED_EVIDENCE_PREFIXES,
        prior_phase_allowlist=tuple(phase_01_record["allowlist"]),
        supplemental_authority=supplemental_authority,
    )
    manifest: dict[str, object] = {
        "schema_version": STANDALONE_ALLOWLIST_SCHEMA_VERSION,
        "manifest_kind": _STANDALONE_MANIFEST_KIND,
        "execution_baseline_commit": STANDALONE_BASELINE_COMMIT,
        "phases": {
            "01": phase_01_record,
            "02": phase_02_record,
        },
    }
    _validate_standalone_manifest(manifest)
    return manifest


def _build_standalone_phase_record(
    *,
    phase: str,
    authority_commit: str,
    plans: Sequence[Mapping[str, object]],
    audited_exceptions: Sequence[str],
    gsd_metadata: Sequence[str],
    ignored_evidence_prefixes: Sequence[str],
    prior_phase_allowlist: Sequence[str],
    supplemental_authority: Mapping[str, object] | None,
) -> dict[str, object]:
    declared = {
        path
        for plan in plans
        for path in plan["files_modified"]
    }
    bucket_sets = {
        "audited_exceptions": set(audited_exceptions),
        "declared_plan_files": declared,
        "gsd_metadata": set(gsd_metadata),
        "ignored_evidence_prefixes": set(ignored_evidence_prefixes),
        "prior_phase_allowlist": set(prior_phase_allowlist),
        "supplemental_authority": (
            set(_supplemental_authority_paths(supplemental_authority))
            if supplemental_authority is not None
            else set()
        ),
    }
    buckets = {
        name: sorted(bucket_sets[name]) for name in _STANDALONE_PHASE_BUCKETS
    }
    allowlist = sorted(set().union(*bucket_sets.values()))
    provenance: dict[str, list[str]] = {}
    for path in allowlist:
        labels: list[str] = []
        for bucket_name in _STANDALONE_PHASE_BUCKETS:
            if path in bucket_sets[bucket_name]:
                labels.append(bucket_name)
        if path in declared:
            labels.extend(
                f"plan:{plan['plan_id']}"
                for plan in plans
                if path in plan["files_modified"]
            )
        provenance[path] = sorted(labels)
    return {
        "authority_commit": authority_commit,
        "plans": [dict(plan) for plan in plans],
        "buckets": buckets,
        "allowlist": allowlist,
        "provenance": provenance,
        "supplemental_authority": (
            dict(supplemental_authority)
            if supplemental_authority is not None
            else None
        ),
    }


def render_standalone_phase_allowlist(manifest: Mapping[str, object]) -> str:
    _validate_standalone_manifest(manifest)
    return json.dumps(
        manifest,
        indent=2,
        ensure_ascii=False,
        sort_keys=True,
    ) + "\n"


def load_standalone_phase_allowlist(path: str | Path) -> dict[str, object]:
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AllowlistError(f"cannot read standalone phase allowlist: {exc}") from exc
    if not isinstance(payload, dict):
        raise AllowlistError("standalone phase allowlist must be a JSON object")
    _validate_standalone_manifest(payload)
    return payload


def verify_allowlist_consistency(
    master_plan: str | Path,
    plans_directory: str | Path,
    *,
    manifest_path: str | Path | None = None,
) -> AllowlistConsistencyReport:
    issues: list[ConsistencyIssue] = []
    try:
        master = parse_master_allowlists(master_plan)
    except WorkspaceGuardError as exc:
        return AllowlistConsistencyReport(
            plan_count=0,
            manifest={},
            issues=(
                ConsistencyIssue(
                    kind="master_parse_error",
                    source=str(master_plan),
                    message=str(exc),
                ),
            ),
        )
    plans_path = Path(plans_directory)
    plan_files = tuple(sorted(plans_path.glob("P*-PLAN.md"), key=_plan_file_key))
    plans: list[PlanScope] = []
    seen_ids: set[str] = set()
    for path in plan_files:
        try:
            plan = parse_plan(path)
        except WorkspaceGuardError as exc:
            issues.append(
                ConsistencyIssue(
                    kind="plan_parse_error",
                    source=path.name,
                    message=str(exc),
                )
            )
            continue
        if plan.plan_id in seen_ids:
            issues.append(
                ConsistencyIssue(
                    kind="duplicate_plan",
                    source=plan.plan_id,
                    message=f"duplicate executable plan ID {plan.plan_id}",
                )
            )
            continue
        seen_ids.add(plan.plan_id)
        plans.append(plan)
        difference = validate_plan_allowlist(plan)
        if not difference.ok:
            issues.append(
                ConsistencyIssue(
                    kind="plan_scope_mismatch",
                    source=plan.plan_id,
                    message=f"{plan.plan_id} files_modified is not its exact task/evidence union",
                    missing=difference.missing,
                    surplus=difference.surplus,
                )
            )
    for phase in EXPECTED_PHASES:
        phase_plans = tuple(plan for plan in plans if plan.phase == phase)
        if not phase_plans:
            issues.append(
                ConsistencyIssue(
                    kind="phase_has_no_plans",
                    source=phase,
                    message=f"{phase} has no executable plans",
                )
            )
        difference = validate_phase_allowlist(phase, master[phase], plans)
        if not difference.ok:
            issues.append(
                ConsistencyIssue(
                    kind="master_scope_mismatch",
                    source=phase,
                    message=f"{phase} master allowlist is not its exact plan union",
                    missing=difference.missing,
                    surplus=difference.surplus,
                )
            )
    manifest = _manifest_from_sources(master_plan, plans_directory, master, plans)
    if manifest_path is not None:
        checked_path = Path(manifest_path)
        try:
            checked = load_phase_allowlists(checked_path)
        except WorkspaceGuardError as exc:
            issues.append(
                ConsistencyIssue(
                    kind="manifest_invalid",
                    source=str(checked_path),
                    message=str(exc),
                )
            )
        else:
            if checked != manifest:
                issues.append(
                    ConsistencyIssue(
                        kind="manifest_mismatch",
                        source=str(checked_path),
                        message="checked manifest does not equal current master and plan sources",
                    )
                )
            try:
                checked_text = checked_path.read_text(encoding="utf-8")
            except OSError as exc:
                issues.append(
                    ConsistencyIssue(
                        kind="manifest_invalid",
                        source=str(checked_path),
                        message=str(exc),
                    )
                )
            else:
                if checked_text != render_phase_allowlists(checked):
                    issues.append(
                        ConsistencyIssue(
                            kind="manifest_not_deterministic",
                            source=str(checked_path),
                            message="checked manifest is not canonical deterministic JSON",
                        )
                    )
    return AllowlistConsistencyReport(
        plan_count=len(plans),
        manifest=manifest,
        issues=tuple(issues),
    )


def hash_workspace_path(root: str | Path, relative_path: str) -> WorkspacePathHash:
    root_path = Path(root).resolve()
    normalized = _normalize_relative_path(relative_path)
    path = root_path.joinpath(*normalized.rstrip("/").split("/"))
    try:
        path.relative_to(root_path)
    except ValueError as exc:
        raise BaselineError(f"path escapes repository root: {normalized}") from exc
    if path.is_symlink():
        target = os.readlink(path).encode("utf-8", errors="surrogatepass")
        return WorkspacePathHash(
            path=normalized,
            kind="symlink",
            size=len(target),
            sha256=hashlib.sha256(target).hexdigest(),
        )
    if not path.exists():
        return WorkspacePathHash(
            path=normalized,
            kind="missing",
            size=0,
            sha256=_MISSING_DIGEST,
        )
    if not path.is_file():
        raise BaselineError(f"dirty path is not a file or symlink: {normalized}")
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return WorkspacePathHash(
        path=normalized,
        kind="file",
        size=size,
        sha256=digest.hexdigest(),
    )


def capture_workspace_baseline(
    root: str | Path,
    destination: str | Path,
    *,
    source_baseline_commit: str | None = None,
    execution_baseline_commit: str | None = None,
) -> WorkspaceBaseline:
    """Capture one immutable standalone baseline without modifying Git state."""

    root_path = Path(root).resolve()
    _git_repository_identity(root_path)
    source_commit = _fixed_standalone_commit(
        source_baseline_commit,
        field="source_baseline_commit",
    )
    execution_commit = _fixed_standalone_commit(
        execution_baseline_commit,
        field="execution_baseline_commit",
    )
    captured_head = _run_git(root_path, ("rev-parse", "HEAD")).strip()
    if _COMMIT.fullmatch(captured_head) is None:
        raise BaselineError("captured HEAD is not a Git object ID")
    _require_existing_ancestor(root_path, execution_commit, captured_head)

    destination_path = _standalone_destination(root_path, destination)
    if os.path.lexists(destination_path):
        raise BaselineError(f"workspace baseline destination already exists: {destination_path}")

    command_stdout: dict[str, str] = {}
    command_records: list[dict[str, object]] = []
    command_arguments = (
        ("rev-parse", "HEAD"),
        ("status", "--porcelain=v2", "--untracked-files=all"),
        ("diff", "--name-status"),
        ("diff", "--cached", "--name-status"),
    )
    for command, arguments in zip(_BASELINE_COMMANDS, command_arguments, strict=True):
        stdout = _run_git(root_path, arguments)
        command_stdout[command] = stdout
        command_records.append(
            {
                "command": command,
                "stdout": stdout,
                "stderr": "",
                "exit_code": 0,
            }
        )
    if command_stdout[_BASELINE_COMMANDS[0]].strip() != captured_head:
        raise BaselineError("captured HEAD changed while baseline commands were collected")

    dirty_paths = _parse_status_paths(command_stdout[_BASELINE_COMMANDS[1]])
    if dirty_paths:
        raise BaselineError(
            "standalone dirty-path exemptions have no authenticated anchor; "
            "capture requires a clean workspace"
        )
    hashes: dict[str, WorkspacePathHash] = {}
    for relative in dirty_paths:
        path_hash = hash_workspace_path(root_path, relative)
        if path_hash.kind == "symlink":
            raise BaselineError(
                f"dirty path must be a regular file or missing tracked path: {relative}"
            )
        hashes[relative] = path_hash

    status_after = _run_git(
        root_path,
        ("status", "--porcelain=v2", "--untracked-files=all"),
    )
    if status_after != command_stdout[_BASELINE_COMMANDS[1]]:
        raise BaselineError("workspace status changed while baseline was captured")

    captured_at = datetime.now(timezone.utc)
    payload = {
        "schema_version": STANDALONE_BASELINE_SCHEMA_VERSION,
        "captured_at_utc": captured_at.isoformat().replace("+00:00", "Z"),
        "repository_root": root_path.as_posix(),
        "source_baseline_commit": source_commit,
        "execution_baseline_commit": execution_commit,
        "captured_head_commit": captured_head,
        "commands": command_records,
        "dirty_path_hashes": [
            path_hash.as_dict() for path_hash in hashes.values()
        ],
    }
    rendered = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    _write_new_atomic(root_path, destination_path, rendered)
    return load_workspace_baseline(destination_path, expected_root=root_path)


def load_workspace_baseline(
    path: str | Path,
    *,
    expected_root: str | Path | None = None,
) -> WorkspaceBaseline:
    baseline_path = Path(path).resolve()
    try:
        payload = json.loads(baseline_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BaselineError(f"cannot read workspace baseline: {exc}") from exc
    if not isinstance(payload, dict):
        raise BaselineError("workspace baseline must be a JSON object")
    schema_version = payload.get("schema_version")
    legacy_keys = {
        "schema_version",
        "captured_at_utc",
        "repository_root",
        "commands",
        "dirty_path_hashes",
    }
    standalone_keys = legacy_keys.union(
        {
            "source_baseline_commit",
            "execution_baseline_commit",
            "captured_head_commit",
        }
    )
    if schema_version == SCHEMA_VERSION:
        if not legacy_keys.issubset(payload):
            raise BaselineError("legacy workspace baseline fields are incomplete")
    elif schema_version == STANDALONE_BASELINE_SCHEMA_VERSION:
        if set(payload) != standalone_keys:
            raise BaselineError("standalone workspace baseline fields are not exact")
    else:
        raise BaselineError("workspace baseline schema_version must be 1 or 2")
    raw_root = payload.get("repository_root")
    if not isinstance(raw_root, str) or not raw_root:
        raise BaselineError("workspace baseline requires repository_root")
    repository_root = Path(raw_root).resolve()
    if expected_root is not None:
        expected_root_path = Path(expected_root).resolve()
        if (
            repository_root != expected_root_path
            and _git_repository_identity(repository_root)
            != _git_repository_identity(expected_root_path)
        ):
            raise BaselineError(
                "workspace baseline belongs to a different repository"
            )
    captured = payload.get("captured_at_utc")
    if not isinstance(captured, str):
        raise BaselineError("workspace baseline requires captured_at_utc")
    try:
        captured_at = datetime.fromisoformat(captured.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BaselineError("captured_at_utc is not an ISO datetime") from exc
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise BaselineError("captured_at_utc must be timezone-aware")

    raw_commands = payload.get("commands")
    if not isinstance(raw_commands, list) or len(raw_commands) != len(_BASELINE_COMMANDS):
        raise BaselineError("workspace baseline requires the four exact Git commands")
    command_stdout: dict[str, str] = {}
    observed_commands: list[str] = []
    for raw in raw_commands:
        if not isinstance(raw, dict):
            raise BaselineError("workspace baseline command record must be an object")
        command = raw.get("command")
        stdout = raw.get("stdout")
        stderr = raw.get("stderr")
        exit_code = raw.get("exit_code")
        if not isinstance(command, str) or not isinstance(stdout, str):
            raise BaselineError("workspace baseline command/stdout must be strings")
        if stderr != "" or exit_code != 0:
            raise BaselineError(f"workspace baseline command did not succeed: {command}")
        observed_commands.append(command)
        command_stdout[command] = stdout
    if tuple(observed_commands) != _BASELINE_COMMANDS:
        raise BaselineError("workspace baseline commands or ordering changed")
    captured_head = command_stdout[_BASELINE_COMMANDS[0]].strip()
    if _COMMIT.fullmatch(captured_head) is None:
        raise BaselineError("workspace baseline commit is not a Git object ID")
    if schema_version == STANDALONE_BASELINE_SCHEMA_VERSION:
        source_commit = payload.get("source_baseline_commit")
        execution_commit = payload.get("execution_baseline_commit")
        recorded_head = payload.get("captured_head_commit")
        if source_commit != STANDALONE_BASELINE_COMMIT:
            raise BaselineError(
                "source_baseline_commit is not the fixed standalone baseline"
            )
        if execution_commit != STANDALONE_BASELINE_COMMIT:
            raise BaselineError(
                "execution_baseline_commit is not the fixed standalone baseline"
            )
        if recorded_head != captured_head:
            raise BaselineError("captured_head_commit does not match Git command output")
    else:
        source_commit = captured_head
        execution_commit = captured_head

    raw_hashes = payload.get("dirty_path_hashes")
    if not isinstance(raw_hashes, list):
        raise BaselineError("dirty_path_hashes must be an array")
    hashes: dict[str, WorkspacePathHash] = {}
    for raw in raw_hashes:
        if not isinstance(raw, dict):
            raise BaselineError("dirty path hash must be an object")
        relative = _normalize_relative_path(raw.get("path"))
        kind = raw.get("kind")
        size = raw.get("size")
        digest = raw.get("sha256")
        if kind not in {"file", "symlink", "missing"}:
            raise BaselineError(f"invalid dirty path kind for {relative}")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise BaselineError(f"invalid dirty path size for {relative}")
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise BaselineError(f"invalid dirty path SHA-256 for {relative}")
        if relative in hashes:
            raise BaselineError(f"duplicate dirty path hash: {relative}")
        hashes[relative] = WorkspacePathHash(relative, kind, size, digest)
    status_paths = _parse_status_paths(command_stdout[_BASELINE_COMMANDS[1]])
    if set(status_paths) != set(hashes):
        missing = sorted(set(status_paths).difference(hashes))
        surplus = sorted(set(hashes).difference(status_paths))
        raise BaselineError(
            "dirty path hashes do not exactly match pre-edit status; "
            f"missing={missing}, surplus={surplus}"
        )
    if schema_version == STANDALONE_BASELINE_SCHEMA_VERSION and (
        hashes
        or any(command_stdout[command] for command in _BASELINE_COMMANDS[1:])
    ):
        raise BaselineError(
            "standalone dirty-path exemptions have no authenticated anchor"
        )
    if expected_root is not None:
        _require_existing_ancestor(
            expected_root_path,
            execution_commit,
            captured_head,
        )
    return WorkspaceBaseline(
        path=baseline_path,
        schema_version=schema_version,
        repository_root=repository_root,
        captured_at_utc=captured_at,
        source_baseline_commit=source_commit,
        execution_baseline_commit=execution_commit,
        captured_head_commit=captured_head,
        command_stdout=command_stdout,
        dirty_path_hashes=hashes,
    )


def verify_workspace(
    root: str | Path,
    phase: str,
    baseline_path: str | Path,
    allowlists_path: str | Path,
) -> WorkspaceReport:
    root_path = Path(root).resolve()
    violations: list[WorkspaceViolation] = []
    if phase not in (*EXPECTED_PHASES, "01", "02"):
        violations.append(
            WorkspaceViolation(
                kind="phase_invalid",
                path=phase,
                message="phase must be 01, 02, or one of P0-P10",
            )
        )
        return WorkspaceReport(root_path, phase, "", (), (), tuple(violations))
    try:
        baseline = load_workspace_baseline(baseline_path, expected_root=root_path)
    except WorkspaceGuardError as exc:
        violations.append(
            WorkspaceViolation(
                kind="baseline_invalid",
                path=str(baseline_path),
                message=str(exc),
            )
        )
        return WorkspaceReport(root_path, phase, "", (), (), tuple(violations))
    if phase in {"01", "02"}:
        if baseline.schema_version != STANDALONE_BASELINE_SCHEMA_VERSION:
            violations.append(
                WorkspaceViolation(
                    kind="baseline_invalid",
                    path=str(baseline_path),
                    message=(
                        f"Phase {phase} requires the standalone schema-v2 baseline"
                    ),
                )
            )
        if (
            baseline.source_baseline_commit != STANDALONE_BASELINE_COMMIT
            or baseline.execution_baseline_commit != STANDALONE_BASELINE_COMMIT
        ):
            violations.append(
                WorkspaceViolation(
                    kind="baseline_invalid",
                    path=str(baseline_path),
                    message=(
                        f"Phase {phase} source and execution commits must both equal "
                        f"{STANDALONE_BASELINE_COMMIT}"
                    ),
                )
            )
        if baseline.dirty_path_hashes:
            violations.append(
                WorkspaceViolation(
                    kind="baseline_invalid",
                    path=str(baseline_path),
                    message=f"Phase {phase} dirty-path exemptions must be empty",
                )
            )
        if violations:
            return WorkspaceReport(root_path, phase, "", (), (), tuple(violations))
    if phase in {"01", "02"}:
        checked_manifest: dict[str, object] | None = None
        try:
            checked_manifest = load_standalone_phase_allowlist(allowlists_path)
        except WorkspaceGuardError as exc:
            violations.append(
                WorkspaceViolation(
                    kind="manifest_invalid",
                    path=str(allowlists_path),
                    message=str(exc),
                )
            )
        try:
            manifest = build_standalone_phase_allowlist(
                root_path,
                root_path
                / ".planning"
                / "phases"
                / "01-hermetic-standalone-foundation",
            )
        except WorkspaceGuardError as exc:
            violations.append(
                WorkspaceViolation(
                    kind="manifest_invalid",
                    path=str(allowlists_path),
                    message=f"immutable Phase 01 plan authority is unavailable: {exc}",
                )
            )
            return WorkspaceReport(
                root_path, phase, baseline.commit, (), (), tuple(violations)
            )
        if checked_manifest is not None and checked_manifest != manifest:
            violations.append(
                WorkspaceViolation(
                    kind="manifest_invalid",
                    path=str(allowlists_path),
                    message=(
                        "checked standalone manifest is not the exact immutable "
                        "cumulative Phase 01/02 plan union"
                    ),
                )
            )
    else:
        try:
            manifest = load_phase_allowlists(allowlists_path)
        except WorkspaceGuardError as exc:
            violations.append(
                WorkspaceViolation(
                    kind="manifest_invalid",
                    path=str(allowlists_path),
                    message=str(exc),
                )
            )
            return WorkspaceReport(
                root_path, phase, baseline.commit, (), (), tuple(violations)
            )
    try:
        git_root = Path(_run_git(root_path, ("rev-parse", "--show-toplevel")).strip()).resolve()
        if git_root != root_path:
            raise WorkspaceGuardError("requested root is not the Git repository root")
        current_head = _run_git(root_path, ("rev-parse", "HEAD")).strip()
        _require_existing_ancestor(
            root_path,
            baseline.execution_baseline_commit,
            current_head,
        )
        committed = _parse_name_status_paths(
            _run_git(
                root_path,
                (
                    "diff",
                    "--name-status",
                    "--no-renames",
                    f"{baseline.execution_baseline_commit}..HEAD",
                    "--",
                ),
            )
        )
        working_tree = _parse_name_status_paths(
            _run_git(root_path, ("diff", "--name-status", "--no-renames", "--"))
        )
        index = _parse_name_status_paths(
            _run_git(
                root_path,
                ("diff", "--cached", "--name-status", "--no-renames", "--"),
            )
        )
        untracked = _git_path_lines(
            _run_git(root_path, ("ls-files", "--others", "--exclude-standard"))
        )
    except WorkspaceGuardError as exc:
        violations.append(
            WorkspaceViolation(
                kind="git_read_failed",
                path=root_path.as_posix(),
                message=str(exc),
            )
        )
        return WorkspaceReport(
            root_path, phase, baseline.commit, (), (), tuple(violations)
        )

    changed = set(committed).union(working_tree, index, untracked)
    preserved: list[str] = []
    for relative, expected_hash in sorted(baseline.dirty_path_hashes.items()):
        try:
            current_hash = hash_workspace_path(root_path, relative)
        except WorkspaceGuardError as exc:
            violations.append(
                WorkspaceViolation(
                    kind="baseline_path_changed",
                    path=relative,
                    message=str(exc),
                    expected=expected_hash.sha256,
                )
            )
            continue
        if current_hash != expected_hash:
            violations.append(
                WorkspaceViolation(
                    kind="baseline_path_changed",
                    path=relative,
                    message="pre-existing dirty path bytes or type changed",
                    expected=expected_hash.sha256,
                    actual=current_hash.sha256,
                )
            )
            continue
        preserved.append(relative)
        changed.discard(relative)

    ignored_evidence_prefixes: tuple[str, ...] = ()
    if phase in {"01", "02"}:
        standalone_phases = manifest["phases"]
        assert isinstance(standalone_phases, dict)
        phase_record = standalone_phases[phase]
        assert isinstance(phase_record, dict)
        allowlist = tuple(phase_record["allowlist"])
        raw_buckets = phase_record["buckets"]
        assert isinstance(raw_buckets, dict)
        ignored_evidence_prefixes = tuple(raw_buckets["ignored_evidence_prefixes"])
    else:
        phases = manifest["phases"]
        assert isinstance(phases, dict)
        phase_record = phases[phase]
        assert isinstance(phase_record, dict)
        allowlist = tuple(phase_record["allowlist"])
    for relative in sorted(changed):
        try:
            normalized = (
                _validate_standalone_path(relative)
                if phase in {"01", "02"}
                else validate_scope_path(relative)
            )
        except ScopePathError as exc:
            violations.append(
                WorkspaceViolation(
                    kind="unsafe_changed_path",
                    path=relative,
                    message=str(exc),
                )
            )
            continue
        if _is_standard_summary(normalized, root_path, phase):
            # PLAN.md's output contract authorizes its adjacent execution
            # summary as metadata.  It remains visible in changed_paths but is
            # not an implementation permission and cannot authorize code.
            continue
        if phase in {"01", "02"} and _is_standalone_review_iteration_metadata(
            normalized,
            phase,
        ):
            # Auto-review preserves immutable review evidence beside the active
            # report.  Keep this exception basename- and directory-exact so it
            # cannot authorize arbitrary planning or implementation paths.
            continue
        if phase == "02" and _is_phase2_deferred_metadata(normalized):
            # This exact pre-existing note is planning metadata, not code
            # authority. Keep it visible in changed_paths without admitting a
            # basename, suffix, directory, or pattern-wide exemption.
            continue
        if phase == "02" and _is_omx_project_metadata(normalized):
            # OMX project setup is preserved migration metadata, not Phase 2
            # application authority.  Admit only the exact Git metadata file
            # and the three finite project-tool index shapes; keep every path
            # visible and reject scripts, nested payloads, and near-matches.
            continue
        if not _path_is_allowed(normalized, allowlist):
            violations.append(
                WorkspaceViolation(
                    kind="out_of_scope",
                    path=normalized,
                    message=f"path is not in the exact {phase} allowlist",
                )
            )
    for relative in sorted(set(committed).union(index)):
        if _path_is_allowed(relative, ignored_evidence_prefixes):
            violations.append(
                WorkspaceViolation(
                    kind="ignored_evidence_commit_forbidden",
                    path=relative,
                    message="ignored live evidence may never be staged or committed",
                )
            )
    return WorkspaceReport(
        root=root_path,
        phase=phase,
        baseline_commit=baseline.commit,
        changed_paths=tuple(sorted(changed)),
        preserved_dirty_paths=tuple(preserved),
        violations=tuple(violations),
    )


def _frontmatter(text: str, path: Path) -> dict[str, object]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise PlanParseError(f"{path.name} has no opening frontmatter delimiter")
    try:
        end = next(
            index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---"
        )
    except StopIteration as exc:
        raise PlanParseError(f"{path.name} has no closing frontmatter delimiter") from exc
    result: dict[str, object] = {}
    index = 1
    while index < end:
        line = lines[index]
        if not line.strip() or line.lstrip().startswith("#"):
            index += 1
            continue
        key_match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*):(?:\s*(.*))?", line)
        if key_match is None:
            index += 1
            continue
        key, raw_value = key_match.group(1), (key_match.group(2) or "").strip()
        if key in result:
            raise PlanParseError(f"{path.name} repeats frontmatter key {key}")
        if raw_value == "[]":
            result[key] = []
            index += 1
            continue
        if raw_value.startswith("[") and raw_value.endswith("]"):
            result[key] = [
                _unquote(item.strip())
                for item in raw_value[1:-1].split(",")
                if item.strip()
            ]
            index += 1
            continue
        if raw_value:
            result[key] = _unquote(raw_value)
            index += 1
            continue
        values: list[str] = []
        cursor = index + 1
        while cursor < end:
            item = re.fullmatch(r"\s+-\s+(.+?)\s*", lines[cursor])
            if item is None:
                break
            values.append(_unquote(item.group(1)))
            cursor += 1
        result[key] = values
        index = cursor
    return result


def _scalar(frontmatter: Mapping[str, object], key: str, path: Path) -> str:
    value = frontmatter.get(key)
    if not isinstance(value, str) or not value:
        raise PlanParseError(f"{path.name} requires scalar frontmatter {key}")
    return value


def _frontmatter_paths(
    frontmatter: Mapping[str, object],
    key: str,
    path: Path,
    *,
    required: bool,
) -> tuple[str, ...]:
    raw = frontmatter.get(key, [])
    if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
        raise PlanParseError(f"{path.name} frontmatter {key} must be a list")
    values = tuple(_plan_path(item, path) for item in raw)
    if required and not values:
        raise PlanParseError(f"{path.name} frontmatter {key} cannot be empty")
    if len(values) != len(set(values)):
        raise PlanParseError(f"{path.name} frontmatter {key} contains duplicates")
    return tuple(sorted(values))


def _plan_path(value: str, source: Path) -> str:
    try:
        return _validate_historical_scope_path(value)
    except ScopePathError as exc:
        raise PlanParseError(f"{source.name} has invalid scope path {value!r}: {exc}") from exc


def _normalize_relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ScopePathError("scope path must be a nonblank trimmed string")
    if "\\" in value or ":" in value or any(ord(character) < 32 for character in value):
        raise ScopePathError("scope path must be a repository-relative POSIX path")
    if value.startswith("/") or any(character in value for character in _GLOB_CHARACTERS):
        raise ScopePathError("absolute paths and glob permissions are forbidden")
    directory = value.endswith("/")
    body = value[:-1] if directory else value
    if not body:
        raise ScopePathError("repository-root permissions are forbidden")
    segments = body.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ScopePathError("scope path cannot contain empty, dot, or parent segments")
    normalized = "/".join(segments) + ("/" if directory else "")
    return normalized


def _reject_prohibited_artifact(normalized: str) -> None:
    body = normalized[:-1] if normalized.endswith("/") else normalized
    lowered_segments = tuple(segment.casefold() for segment in body.split("/"))
    if any(segment in _PROHIBITED_DIRECTORY_NAMES for segment in lowered_segments):
        raise ScopePathError("environment, cache, backup, or log paths are prohibited")
    lowered = normalized.casefold()
    if lowered.startswith("data/options_copilot/evidence/workspace/"):
        raise ScopePathError("ignored workspace baseline evidence is runtime-only")
    filename = lowered_segments[-1]
    if filename.startswith(".env"):
        raise ScopePathError("secret or credential filenames are prohibited")
    if any(filename.endswith(suffix) for suffix in _PROHIBITED_FILE_SUFFIXES):
        raise ScopePathError("runtime database, cache, log, or temporary files are prohibited")
    compact_filename = re.sub(r"[^a-z0-9]", "", filename)
    suffix = Path(filename).suffix.casefold()
    if suffix in _SECRET_FILE_SUFFIXES and any(
        marker in compact_filename for marker in _SECRET_FILE_MARKERS
    ):
        raise ScopePathError("secret or credential filenames are prohibited")


def _validate_historical_scope_path(value: str) -> str:
    """Retain legacy P0-P10 evidence semantics without granting new authority."""

    return _validate_implementation_path(_normalize_relative_path(value))


def _validate_standalone_path(value: str) -> str:
    normalized = _normalize_relative_path(value)
    _reject_prohibited_artifact(normalized)
    return normalized


def _manifest_from_sources(
    master_plan: str | Path,
    plans_directory: str | Path,
    master: Mapping[str, tuple[str, ...]],
    plans: Sequence[PlanScope],
) -> dict[str, object]:
    phases: dict[str, object] = {}
    for phase in EXPECTED_PHASES:
        phase_plans = sorted(
            (plan.plan_id for plan in plans if plan.phase == phase),
            key=_plan_id_key,
        )
        phases[phase] = {
            "plans": phase_plans,
            "allowlist": list(master[phase]),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "master_plan": _source_name(Path(master_plan), is_directory=False),
        "plans_directory": _source_name(Path(plans_directory), is_directory=True),
        "phases": phases,
    }


def _validate_manifest(manifest: Mapping[str, object]) -> None:
    expected_keys = {"schema_version", "master_plan", "plans_directory", "phases"}
    if set(manifest) != expected_keys:
        raise AllowlistError("manifest fields are not exact")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise AllowlistError("manifest schema_version must be 1")
    if not isinstance(manifest.get("master_plan"), str) or not isinstance(
        manifest.get("plans_directory"), str
    ):
        raise AllowlistError("manifest source paths must be strings")
    phases = manifest.get("phases")
    if not isinstance(phases, dict) or set(phases) != set(EXPECTED_PHASES):
        raise AllowlistError("manifest phases must be exactly P0-P10")
    for phase in EXPECTED_PHASES:
        record = phases[phase]
        if not isinstance(record, dict) or set(record) != {"plans", "allowlist"}:
            raise AllowlistError(f"{phase} manifest record fields are not exact")
        plans = record["plans"]
        allowlist = record["allowlist"]
        if not isinstance(plans, list) or any(not isinstance(item, str) for item in plans):
            raise AllowlistError(f"{phase} plans must be an array of IDs")
        if len(plans) != len(set(plans)) or plans != sorted(plans, key=_plan_id_key):
            raise AllowlistError(f"{phase} plans are duplicated or non-deterministic")
        if any(not item.startswith(phase + "-") for item in plans):
            raise AllowlistError(f"{phase} contains a plan from another phase")
        if not isinstance(allowlist, list) or any(
            not isinstance(item, str) for item in allowlist
        ):
            raise AllowlistError(f"{phase} allowlist must be an array of paths")
        normalized = [_validate_historical_scope_path(item) for item in allowlist]
        if len(normalized) != len(set(normalized)) or normalized != sorted(normalized):
            raise AllowlistError(f"{phase} allowlist is duplicated or non-deterministic")


def _standalone_plan_records(
    root: Path,
    *,
    phase: str,
    phase_title: str,
    plans_directory: Path,
    expected_plan_ids: Sequence[str],
    authority_commit: str,
) -> list[dict[str, object]]:
    plans_path = Path(plans_directory).resolve()
    expected_directory = (
        root
        / ".planning"
        / "phases"
        / phase_title
    ).resolve()
    if plans_path != expected_directory:
        raise AllowlistError(f"standalone Phase {phase} plans directory is not exact")
    expected_names = tuple(f"{plan_id}-PLAN.md" for plan_id in expected_plan_ids)
    current_head = _run_git(root, ("rev-parse", "HEAD")).strip()
    _require_existing_ancestor(root, authority_commit, current_head)
    relative_directory = plans_path.relative_to(root).as_posix()
    actual_names = _immutable_plan_names(
        root,
        authority_commit=authority_commit,
        relative_directory=relative_directory,
    )
    if actual_names != expected_names:
        raise AllowlistError(
            f"immutable Phase {phase} plan IDs or ordering are not exact; "
            f"expected={list(expected_names)}, actual={list(actual_names)}"
        )
    records: list[dict[str, object]] = []
    for name, expected_plan_id in zip(
        expected_names,
        expected_plan_ids,
        strict=True,
    ):
        relative = relative_directory + "/" + name
        path = Path(relative)
        try:
            raw_blob = _run_git_bytes(
                root,
                (
                    "cat-file",
                    "blob",
                    f"{authority_commit}:{relative}",
                ),
            )
        except WorkspaceGuardError as exc:
            raise AllowlistError(
                f"cannot read immutable standalone plan {relative}: {exc}"
            ) from exc
        try:
            text = raw_blob.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise AllowlistError(
                f"immutable standalone plan is not UTF-8: {relative}"
            ) from exc
        frontmatter = _frontmatter(text, path)
        actual_phase_title = _scalar(frontmatter, "phase", path)
        plan_number = _scalar(frontmatter, "plan", path)
        if actual_phase_title != phase_title:
            raise AllowlistError(f"{path.name} has an unexpected standalone phase")
        if f"{phase}-{plan_number}" != expected_plan_id:
            raise AllowlistError(f"{path.name} plan identity does not match its filename")
        files_modified = _standalone_frontmatter_paths(
            frontmatter,
            "files_modified",
            path,
            required=False,
        )
        evidence_prefixes = _standalone_frontmatter_paths(
            frontmatter,
            "evidence_prefixes",
            path,
            required=False,
        )
        if any(not prefix.endswith("/") for prefix in evidence_prefixes):
            raise AllowlistError(f"{path.name} evidence prefixes must end in '/'")
        if not set(evidence_prefixes).issubset(files_modified):
            raise AllowlistError(
                f"{path.name} evidence prefixes are not included in files_modified"
            )
        _validate_phase_plan_paths(
            phase,
            files_modified,
            evidence_prefixes,
        )
        records.append(
            {
                "plan_id": expected_plan_id,
                "content_sha256": hashlib.sha256(raw_blob).hexdigest(),
                "files_modified": list(files_modified),
                "evidence_prefixes": list(evidence_prefixes),
            }
        )
    return records


def _immutable_plan_names(
    root: Path,
    *,
    authority_commit: str,
    relative_directory: str,
) -> tuple[str, ...]:
    raw_tree = _run_git_bytes(
        root,
        ("cat-file", "-p", f"{authority_commit}:{relative_directory}"),
    )
    try:
        text = raw_tree.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise AllowlistError("immutable plan tree is not UTF-8") from exc
    names: list[str] = []
    for line in text.splitlines():
        try:
            metadata, name = line.split("\t", 1)
            mode, object_type, object_id = metadata.split(" ", 2)
        except ValueError as exc:
            raise AllowlistError("immutable plan tree record is malformed") from exc
        if (
            mode != "100644"
            or object_type != "blob"
            or _COMMIT.fullmatch(object_id) is None
        ):
            continue
        if re.fullmatch(r"[0-9]{2}-[0-9]{2}-PLAN\.md", name):
            names.append(name)
    return tuple(sorted(names))


def _standalone_frontmatter_paths(
    frontmatter: Mapping[str, object],
    key: str,
    path: Path,
    *,
    required: bool,
) -> tuple[str, ...]:
    raw = frontmatter.get(key, [])
    if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
        raise AllowlistError(f"{path.name} frontmatter {key} must be a list")
    values = tuple(_validate_standalone_path(item) for item in raw)
    if required and not values:
        raise AllowlistError(f"{path.name} frontmatter {key} cannot be empty")
    if len(values) != len(set(values)):
        raise AllowlistError(f"{path.name} frontmatter {key} contains duplicates")
    return tuple(sorted(values))


def _validate_phase_plan_paths(
    phase: str,
    files_modified: Sequence[str],
    evidence_prefixes: Sequence[str],
) -> None:
    metadata = (
        set(_STANDALONE_GSD_METADATA)
        if phase == "01"
        else set(_PHASE2_GSD_METADATA)
    )
    for relative in files_modified:
        if relative.startswith(".planning/") and relative not in metadata:
            raise AllowlistError(
                f"Phase {phase} plan declares non-exact GSD metadata: {relative}"
            )
    expected_evidence = () if phase == "01" else _PHASE2_IGNORED_EVIDENCE_PREFIXES
    if tuple(sorted(set(evidence_prefixes))) not in {(), expected_evidence}:
        raise AllowlistError(f"Phase {phase} plan declares an unknown evidence prefix")


def _phase2_supplemental_authority_record(root: Path) -> dict[str, object]:
    """Rebuild the additive Phase 2 authority record from immutable Git objects."""

    current_head = _run_git(root, ("rev-parse", "HEAD")).strip()
    try:
        _require_existing_ancestor(
            root,
            STANDALONE_BASELINE_FULL_COMMIT,
            PHASE2_PLAN_AUTHORITY_COMMIT,
        )
        _require_existing_ancestor(
            root,
            PHASE2_PLAN_AUTHORITY_COMMIT,
            PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT,
        )
        _require_existing_ancestor(
            root,
            PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT,
            current_head,
        )
    except BaselineError as exc:
        raise AllowlistError(
            f"Phase 2 supplemental authority chain is invalid: {exc}"
        ) from exc

    resolved_baseline = _run_git(
        root,
        ("rev-parse", STANDALONE_BASELINE_FULL_COMMIT),
    ).strip()
    if resolved_baseline != STANDALONE_BASELINE_FULL_COMMIT:
        raise AllowlistError("Phase 2 supplemental baseline identity is invalid")
    second_parent = _run_git_result(
        root,
        ("rev-parse", f"{PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT}^2"),
    )
    if second_parent.returncode == 0:
        raise AllowlistError("Phase 2 supplemental authority must not be a merge commit")

    try:
        raw_record = _run_git_bytes(
            root,
            (
                "cat-file",
                "blob",
                (
                    f"{PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT}:"
                    f"{_PHASE2_SUPPLEMENTAL_AUTHORITY_PATH}"
                ),
            ),
        )
    except WorkspaceGuardError as exc:
        raise AllowlistError(
            f"Phase 2 supplemental authority record is unavailable: {exc}"
        ) from exc
    if hashlib.sha256(raw_record).hexdigest() != _PHASE2_SUPPLEMENTAL_RECORD_SHA256:
        raise AllowlistError("Phase 2 supplemental authority record hash is invalid")
    try:
        record = json.loads(
            raw_record.decode("utf-8", errors="strict"),
            object_pairs_hook=_reject_duplicate_json_members,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AllowlistError(
            "Phase 2 supplemental authority record is not strict UTF-8 JSON"
        ) from exc
    expected_payload = _expected_phase2_supplemental_payload()
    if record != expected_payload:
        raise AllowlistError("Phase 2 supplemental authority payload is not exact")

    observed_blob_hashes: dict[str, str] = {}
    for path in _PHASE2_SUPPLEMENTAL_AUTHORIZED_PATHS:
        try:
            raw_blob = _run_git_bytes(
                root,
                (
                    "cat-file",
                    "blob",
                    f"{PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT}:{path}",
                ),
            )
        except WorkspaceGuardError as exc:
            raise AllowlistError(
                f"Phase 2 supplemental authorized blob is unavailable: {path}"
            ) from exc
        observed_blob_hashes[path] = hashlib.sha256(raw_blob).hexdigest()
    if observed_blob_hashes != _PHASE2_SUPPLEMENTAL_ANCHOR_BLOB_SHA256:
        raise AllowlistError("Phase 2 supplemental authorized blob hashes are invalid")

    try:
        raw_diff = _run_git(
            root,
            (
                "diff",
                "--name-status",
                "--no-renames",
                (
                    f"{PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT}^.."
                    f"{PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT}"
                ),
                "--",
            ),
        )
    except WorkspaceGuardError as exc:
        raise AllowlistError(
            f"Phase 2 supplemental authority diff is unavailable: {exc}"
        ) from exc
    observed_diff: dict[str, str] = {}
    for line in raw_diff.splitlines():
        fields = line.split("\t")
        if len(fields) != 2 or fields[0] not in {"A", "M"}:
            raise AllowlistError("Phase 2 supplemental authority diff is malformed")
        status, path = fields
        normalized = _normalize_relative_path(_decode_git_path(path))
        if normalized in observed_diff:
            raise AllowlistError("Phase 2 supplemental authority diff repeats a path")
        observed_diff[normalized] = status
    expected_diff = {
        _PHASE2_SUPPLEMENTAL_AUTHORITY_PATH: "A",
        **{
            path: "M" for path in _PHASE2_SUPPLEMENTAL_AUTHORIZED_PATHS
        },
    }
    if observed_diff != expected_diff:
        raise AllowlistError("Phase 2 supplemental authority diff is not exact")
    return _expected_phase2_supplemental_manifest_record()


def _reject_duplicate_json_members(
    pairs: Sequence[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AllowlistError(
                f"Phase 2 supplemental authority repeats JSON member {key}"
            )
        result[key] = value
    return result


def _expected_phase2_supplemental_payload() -> dict[str, object]:
    return {
        "authorized_paths": list(_PHASE2_SUPPLEMENTAL_AUTHORIZED_PATHS),
        "content_sha256": dict(_PHASE2_SUPPLEMENTAL_CONTENT_SHA256),
        "execution_baseline_commit": STANDALONE_BASELINE_FULL_COMMIT,
        "kind": _PHASE2_SUPPLEMENTAL_AUTHORITY_KIND,
        "parent_plan_authority_commit": PHASE2_PLAN_AUTHORITY_COMMIT,
        "phase": "02",
        "plan": "02-09",
        "reason": _PHASE2_SUPPLEMENTAL_REASON,
        "scan_contract": dict(_PHASE2_SUPPLEMENTAL_SCAN_CONTRACT),
        "schema_version": 1,
    }


def _expected_phase2_supplemental_manifest_record() -> dict[str, object]:
    return {
        **_expected_phase2_supplemental_payload(),
        "anchor_blob_sha256": dict(
            _PHASE2_SUPPLEMENTAL_ANCHOR_BLOB_SHA256
        ),
        "authority_commit": PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT,
        "record_path": _PHASE2_SUPPLEMENTAL_AUTHORITY_PATH,
        "record_sha256": _PHASE2_SUPPLEMENTAL_RECORD_SHA256,
    }


def _supplemental_authority_paths(
    record: Mapping[str, object],
) -> tuple[str, ...]:
    record_path = record.get("record_path")
    authorized_paths = record.get("authorized_paths")
    if not isinstance(record_path, str) or not isinstance(authorized_paths, list):
        raise AllowlistError("standalone supplemental authority paths are invalid")
    if any(not isinstance(path, str) for path in authorized_paths):
        raise AllowlistError("standalone supplemental authorized paths are invalid")
    normalized = tuple(
        sorted(
            {
                _validate_standalone_path(record_path),
                *(
                    _validate_standalone_path(path)
                    for path in authorized_paths
                ),
            }
        )
    )
    if len(normalized) != len(authorized_paths) + 1:
        raise AllowlistError("standalone supplemental authority paths repeat")
    return normalized


def _validate_standalone_manifest(manifest: Mapping[str, object]) -> None:
    expected_keys = {
        "schema_version",
        "manifest_kind",
        "execution_baseline_commit",
        "phases",
    }
    if set(manifest) != expected_keys:
        raise AllowlistError("standalone manifest fields are not exact")
    if manifest.get("schema_version") != STANDALONE_ALLOWLIST_SCHEMA_VERSION:
        raise AllowlistError(
            "standalone manifest schema_version must be "
            f"{STANDALONE_ALLOWLIST_SCHEMA_VERSION}"
        )
    if manifest.get("manifest_kind") != _STANDALONE_MANIFEST_KIND:
        raise AllowlistError("standalone manifest kind is invalid")
    if manifest.get("execution_baseline_commit") != STANDALONE_BASELINE_COMMIT:
        raise AllowlistError("standalone manifest execution baseline must be 7237246")
    raw_phases = manifest.get("phases")
    if not isinstance(raw_phases, dict) or set(raw_phases) != {"01", "02"}:
        raise AllowlistError("standalone manifest phases must be exactly 01 and 02")
    phase_01_allowlist = _validate_standalone_phase_record(
        "01",
        raw_phases["01"],
        expected_authority=STANDALONE_PLAN_AUTHORITY_COMMIT,
        expected_plan_ids=EXPECTED_STANDALONE_PLANS,
        expected_audited_exceptions=_STANDALONE_AUDITED_EXCEPTIONS,
        expected_gsd_metadata=_STANDALONE_GSD_METADATA,
        expected_evidence_prefixes=(),
        expected_prior_allowlist=(),
        expected_supplemental_authority=None,
    )
    _validate_standalone_phase_record(
        "02",
        raw_phases["02"],
        expected_authority=PHASE2_PLAN_AUTHORITY_COMMIT,
        expected_plan_ids=EXPECTED_PHASE2_PLANS,
        expected_audited_exceptions=(),
        expected_gsd_metadata=_PHASE2_GSD_METADATA,
        expected_evidence_prefixes=_PHASE2_IGNORED_EVIDENCE_PREFIXES,
        expected_prior_allowlist=phase_01_allowlist,
        expected_supplemental_authority=(
            _expected_phase2_supplemental_manifest_record()
        ),
    )


def _validate_standalone_phase_record(
    phase: str,
    record: object,
    *,
    expected_authority: str,
    expected_plan_ids: Sequence[str],
    expected_audited_exceptions: Sequence[str],
    expected_gsd_metadata: Sequence[str],
    expected_evidence_prefixes: Sequence[str],
    expected_prior_allowlist: Sequence[str],
    expected_supplemental_authority: Mapping[str, object] | None,
) -> tuple[str, ...]:
    expected_record_keys = {
        "authority_commit",
        "plans",
        "buckets",
        "allowlist",
        "provenance",
        "supplemental_authority",
    }
    if not isinstance(record, dict) or set(record) != expected_record_keys:
        raise AllowlistError(f"standalone Phase {phase} record fields are not exact")
    if record.get("authority_commit") != expected_authority:
        raise AllowlistError(f"standalone Phase {phase} authority commit is invalid")
    raw_plans = record.get("plans")
    if not isinstance(raw_plans, list):
        raise AllowlistError("standalone manifest plans must be an array")
    plan_ids: list[str] = []
    plan_files: dict[str, tuple[str, ...]] = {}
    plan_evidence: dict[str, tuple[str, ...]] = {}
    for raw_plan in raw_plans:
        if not isinstance(raw_plan, dict) or set(raw_plan) != {
            "plan_id",
            "content_sha256",
            "files_modified",
            "evidence_prefixes",
        }:
            raise AllowlistError("standalone plan record fields are not exact")
        plan_id = raw_plan.get("plan_id")
        content_sha256 = raw_plan.get("content_sha256")
        files_modified = raw_plan.get("files_modified")
        evidence_prefixes = raw_plan.get("evidence_prefixes")
        if (
            not isinstance(plan_id, str)
            or not isinstance(content_sha256, str)
            or _SHA256.fullmatch(content_sha256) is None
            or not isinstance(files_modified, list)
            or not isinstance(evidence_prefixes, list)
        ):
            raise AllowlistError("standalone plan record types are invalid")
        if any(not isinstance(path, str) for path in (*files_modified, *evidence_prefixes)):
            raise AllowlistError("standalone plan files must be strings")
        normalized = tuple(_validate_standalone_path(path) for path in files_modified)
        normalized_evidence = tuple(
            _validate_standalone_path(path) for path in evidence_prefixes
        )
        if list(normalized) != sorted(normalized) or len(normalized) != len(set(normalized)):
            raise AllowlistError("standalone plan files are duplicated or non-deterministic")
        if (
            list(normalized_evidence) != sorted(normalized_evidence)
            or len(normalized_evidence) != len(set(normalized_evidence))
            or any(not path.endswith("/") for path in normalized_evidence)
            or not set(normalized_evidence).issubset(normalized)
        ):
            raise AllowlistError("standalone plan evidence prefixes are invalid")
        _validate_phase_plan_paths(phase, normalized, normalized_evidence)
        plan_ids.append(plan_id)
        plan_files[plan_id] = normalized
        plan_evidence[plan_id] = normalized_evidence
    if tuple(plan_ids) != tuple(expected_plan_ids):
        raise AllowlistError(f"standalone Phase {phase} plan IDs or ordering are not exact")

    raw_buckets = record.get("buckets")
    if not isinstance(raw_buckets, dict) or set(raw_buckets) != set(
        _STANDALONE_PHASE_BUCKETS
    ):
        raise AllowlistError("standalone manifest buckets are not exact")
    buckets: dict[str, tuple[str, ...]] = {}
    for bucket_name in _STANDALONE_PHASE_BUCKETS:
        raw_paths = raw_buckets.get(bucket_name)
        if not isinstance(raw_paths, list) or any(
            not isinstance(path, str) for path in raw_paths
        ):
            raise AllowlistError(f"standalone bucket {bucket_name} must be a path array")
        normalized = tuple(_validate_standalone_path(path) for path in raw_paths)
        if list(normalized) != sorted(normalized) or len(normalized) != len(set(normalized)):
            raise AllowlistError(
                f"standalone bucket {bucket_name} is duplicated or non-deterministic"
            )
        buckets[bucket_name] = normalized
    if set(buckets["declared_plan_files"]) != {
        path for files in plan_files.values() for path in files
    }:
        raise AllowlistError("declared standalone bucket is not the exact plan union")
    if buckets["audited_exceptions"] != tuple(expected_audited_exceptions):
        raise AllowlistError("standalone audited exceptions bucket is not exact")
    if buckets["gsd_metadata"] != tuple(expected_gsd_metadata):
        raise AllowlistError("standalone GSD metadata bucket is not exact")
    expected_plan_evidence = {
        path for prefixes in plan_evidence.values() for path in prefixes
    }
    if buckets["ignored_evidence_prefixes"] != tuple(expected_evidence_prefixes):
        raise AllowlistError("standalone ignored evidence prefixes are not exact")
    if set(buckets["ignored_evidence_prefixes"]) != expected_plan_evidence:
        raise AllowlistError("standalone ignored evidence is not the exact plan union")
    if buckets["prior_phase_allowlist"] != tuple(expected_prior_allowlist):
        raise AllowlistError("standalone prior-phase allowlist is not exact")
    raw_supplemental_authority = record.get("supplemental_authority")
    if raw_supplemental_authority != expected_supplemental_authority:
        raise AllowlistError("standalone supplemental authority record is not exact")
    expected_supplemental_paths = (
        _supplemental_authority_paths(expected_supplemental_authority)
        if expected_supplemental_authority is not None
        else ()
    )
    if buckets["supplemental_authority"] != expected_supplemental_paths:
        raise AllowlistError("standalone supplemental authority bucket is not exact")
    bucket_sets = {
        name: set(buckets[name]) for name in _STANDALONE_PHASE_BUCKETS
    }
    expected_allowlist = sorted(set().union(*bucket_sets.values()))

    raw_allowlist = record.get("allowlist")
    if not isinstance(raw_allowlist, list) or any(
        not isinstance(path, str) for path in raw_allowlist
    ):
        raise AllowlistError("standalone allowlist must be a path array")
    normalized_allowlist = [_validate_standalone_path(path) for path in raw_allowlist]
    if normalized_allowlist != expected_allowlist:
        raise AllowlistError("standalone allowlist is not the exact bucket union")

    raw_provenance = record.get("provenance")
    if not isinstance(raw_provenance, dict) or set(raw_provenance) != set(expected_allowlist):
        raise AllowlistError("standalone provenance paths are not exact")
    for path in expected_allowlist:
        labels = raw_provenance.get(path)
        if not isinstance(labels, list) or any(not isinstance(label, str) for label in labels):
            raise AllowlistError(f"standalone provenance for {path} must be an array")
        expected_labels: list[str] = []
        for bucket_name in _STANDALONE_PHASE_BUCKETS:
            if path in bucket_sets[bucket_name]:
                expected_labels.append(bucket_name)
        if path in bucket_sets["declared_plan_files"]:
            expected_labels.extend(
                f"plan:{plan_id}"
                for plan_id in expected_plan_ids
                if path in plan_files[plan_id]
            )
        if labels != sorted(expected_labels):
            raise AllowlistError(f"standalone provenance for {path} is not exact")
    return tuple(normalized_allowlist)


def _source_name(path: Path, *, is_directory: bool) -> str:
    resolved = path.resolve()
    parts = resolved.parts
    lowered = [part.casefold() for part in parts]
    if "options_copilot" in lowered:
        index = len(lowered) - 1 - lowered[::-1].index("options_copilot")
        return "/".join(parts[index:])
    return resolved.name if not is_directory else resolved.name.rstrip("/\\")


def _parse_status_paths(stdout: str) -> tuple[str, ...]:
    paths: set[str] = set()
    for line in stdout.splitlines():
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        raw_paths: list[str]
        if line.startswith("? "):
            raw_paths = [line[2:]]
        elif line.startswith("1 "):
            fields = line.split(" ", 8)
            if len(fields) != 9:
                raise BaselineError("malformed ordinary porcelain-v2 status record")
            raw_paths = [fields[8]]
        elif line.startswith("2 "):
            fields = line.split(" ", 9)
            if len(fields) != 10:
                raise BaselineError("malformed rename porcelain-v2 status record")
            raw_paths = fields[9].split("\t", 1)
        elif line.startswith("u "):
            fields = line.split(" ", 10)
            if len(fields) != 11:
                raise BaselineError("malformed unmerged porcelain-v2 status record")
            raw_paths = [fields[10]]
        else:
            raise BaselineError("unknown porcelain-v2 status record")
        for raw in raw_paths:
            paths.add(_normalize_relative_path(_decode_git_path(raw)))
    return tuple(sorted(paths))


def _decode_git_path(value: str) -> str:
    if not value.startswith('"'):
        return value
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise BaselineError("Git path quoting could not be decoded") from exc
    if not isinstance(decoded, str):
        raise BaselineError("Git path was not a string")
    return decoded


def _parse_name_status_paths(stdout: str) -> tuple[str, ...]:
    paths: set[str] = set()
    for line in stdout.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 2 or fields[0] not in {"A", "C", "D", "M", "T", "U", "X", "B"}:
            raise BaselineError("malformed Git name-status record")
        paths.add(_normalize_relative_path(_decode_git_path(fields[1])))
    return tuple(sorted(paths))


def _fixed_standalone_commit(value: str | None, *, field: str) -> str:
    selected = STANDALONE_BASELINE_COMMIT if value is None else value
    if selected != STANDALONE_BASELINE_COMMIT:
        raise BaselineError(f"{field} is not the fixed standalone baseline")
    return selected


def _require_existing_ancestor(root: Path, baseline: str, head: str) -> None:
    try:
        _run_git(root, ("cat-file", "-e", f"{baseline}^{{commit}}"))
    except WorkspaceGuardError as exc:
        raise BaselineError(
            f"fixed standalone baseline does not name an existing commit: {baseline}"
        ) from exc
    completed = _run_git_result(root, ("merge-base", "--is-ancestor", baseline, head))
    if completed.returncode == 1:
        raise BaselineError(
            f"fixed standalone baseline {baseline} is not an ancestor of {head}"
        )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "unknown Git ancestry error"
        raise BaselineError(f"cannot validate standalone baseline ancestry: {detail}")


def _standalone_destination(root: Path, destination: str | Path) -> Path:
    lexical_root = Path(os.path.abspath(root))
    raw_destination = Path(destination)
    destination_path = (
        raw_destination
        if raw_destination.is_absolute()
        else lexical_root / raw_destination
    )
    destination_path = Path(os.path.abspath(destination_path))
    workspace_root = Path(
        os.path.abspath(
            lexical_root / "data" / "options_copilot" / "evidence" / "workspace"
        )
    )
    try:
        destination_path.relative_to(workspace_root)
    except ValueError as exc:
        raise BaselineError(
            "standalone baseline destination must be under "
            "data/options_copilot/evidence/workspace/"
        ) from exc
    if destination_path.suffix.casefold() != ".json":
        raise BaselineError("standalone baseline destination must be a JSON file")
    _assert_no_reparse_point_ancestors(lexical_root, destination_path.parent)
    return destination_path


def _assert_no_reparse_point_ancestors(root: Path, path: Path) -> None:
    candidates = _baseline_ancestor_candidates(root, path)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    for candidate in candidates:
        try:
            attributes = os.lstat(candidate)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise BaselineError(
                f"workspace baseline ancestor could not be inspected: {candidate}"
            ) from exc
        file_attributes = getattr(attributes, "st_file_attributes", 0)
        if stat.S_ISLNK(attributes.st_mode) or file_attributes & reparse_flag:
            raise BaselineError(
                f"workspace baseline path contains a reparse point ancestor: {candidate}"
            )


def _baseline_ancestor_candidates(root: Path, path: Path) -> tuple[Path, ...]:
    lexical_root = Path(os.path.abspath(root))
    lexical_path = Path(os.path.abspath(path))
    try:
        relative = lexical_path.relative_to(lexical_root)
    except ValueError as exc:
        raise BaselineError("workspace baseline path escapes the repository root") from exc

    cursor = lexical_root
    candidates = [cursor]
    for part in relative.parts:
        cursor /= part
        candidates.append(cursor)
    return tuple(candidates)


@dataclass(frozen=True, slots=True)
class _PinnedWindowsDirectory:
    path: Path
    canonical_path: str
    handle: int


class _BaselineAncestorPins:
    def __init__(self, pins: Sequence[_PinnedWindowsDirectory] = ()) -> None:
        self._pins = tuple(pins)

    def __enter__(self) -> _BaselineAncestorPins:
        return self

    def __exit__(self, *_exc: object) -> None:
        if os.name != "nt":
            return
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int
        for pin in reversed(self._pins):
            kernel32.CloseHandle(pin.handle)

    def verify(self) -> None:
        for pin in self._pins:
            _verify_pinned_windows_directory(pin)


def _pin_baseline_ancestors(root: Path, path: Path) -> _BaselineAncestorPins:
    if os.name != "nt":
        return _BaselineAncestorPins()
    import ctypes
    from ctypes import wintypes

    create_file = ctypes.WinDLL("kernel32", use_last_error=True).CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    invalid_handle = wintypes.HANDLE(-1).value
    pins: list[_PinnedWindowsDirectory] = []
    try:
        for candidate in _baseline_ancestor_candidates(root, path):
            handle = create_file(
                str(candidate),
                0x0080,  # FILE_READ_ATTRIBUTES
                0x0001 | 0x0002,  # FILE_SHARE_READ | FILE_SHARE_WRITE
                None,
                3,  # OPEN_EXISTING
                0x02000000 | 0x00200000,  # BACKUP_SEMANTICS | OPEN_REPARSE_POINT
                None,
            )
            if handle == invalid_handle:
                raise ctypes.WinError(ctypes.get_last_error())
            pin = _PinnedWindowsDirectory(
                path=candidate,
                canonical_path=_normalize_windows_handle_path(str(candidate)),
                handle=int(handle),
            )
            pins.append(pin)
            _verify_pinned_windows_directory(pin)
    except (OSError, RuntimeError) as exc:
        with _BaselineAncestorPins(pins):
            pass
        raise BaselineError(
            "workspace baseline ancestors could not be pinned for publication"
        ) from exc
    return _BaselineAncestorPins(pins)


def _verify_pinned_windows_directory(pin: _PinnedWindowsDirectory) -> None:
    import ctypes
    from ctypes import wintypes

    class FileAttributeTagInfo(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("reparse_tag", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_information = kernel32.GetFileInformationByHandleEx
    get_information.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    get_information.restype = wintypes.BOOL
    information = FileAttributeTagInfo()
    if not get_information(
        pin.handle,
        9,  # FileAttributeTagInfo
        ctypes.byref(information),
        ctypes.sizeof(information),
    ):
        raise BaselineError(
            f"workspace baseline pinned ancestor could not be inspected: {pin.path}"
        ) from ctypes.WinError(ctypes.get_last_error())
    if information.file_attributes & 0x0400:
        raise BaselineError(
            f"workspace baseline path contains a reparse point ancestor: {pin.path}"
        )
    if not information.file_attributes & 0x0010:
        raise BaselineError(
            f"workspace baseline ancestor is not a directory: {pin.path}"
        )

    get_final_path = kernel32.GetFinalPathNameByHandleW
    get_final_path.argtypes = [
        wintypes.HANDLE,
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    ]
    get_final_path.restype = wintypes.DWORD
    required = get_final_path(pin.handle, None, 0, 0)
    if required == 0:
        raise BaselineError(
            f"workspace baseline pinned ancestor path could not be resolved: {pin.path}"
        ) from ctypes.WinError(ctypes.get_last_error())
    buffer = ctypes.create_unicode_buffer(required + 1)
    returned = get_final_path(pin.handle, buffer, len(buffer), 0)
    if returned == 0 or returned >= len(buffer):
        raise BaselineError(
            f"workspace baseline pinned ancestor path could not be resolved: {pin.path}"
        ) from ctypes.WinError(ctypes.get_last_error())
    if _normalize_windows_handle_path(buffer.value) != pin.canonical_path:
        raise BaselineError(
            f"workspace baseline pinned ancestor resolved outside its lexical path: {pin.path}"
        )


def _normalize_windows_handle_path(value: str) -> str:
    normalized = value
    if normalized.startswith("\\\\?\\UNC\\"):
        normalized = "\\\\" + normalized[8:]
    elif normalized.startswith("\\\\?\\"):
        normalized = normalized[4:]
    return os.path.normcase(os.path.normpath(os.path.abspath(normalized)))


def _assert_atomic_publication(source: Path, destination: Path) -> None:
    try:
        source_attributes = os.lstat(source)
        destination_attributes = os.lstat(destination)
    except OSError as exc:
        raise BaselineError(
            f"workspace baseline publication could not be verified: {destination}"
        ) from exc
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    if (
        stat.S_ISLNK(destination_attributes.st_mode)
        or getattr(destination_attributes, "st_file_attributes", 0) & reparse_flag
        or not stat.S_ISREG(destination_attributes.st_mode)
        or not os.path.samestat(source_attributes, destination_attributes)
    ):
        raise BaselineError(
            f"workspace baseline publication did not preserve the temporary file: {destination}"
        )


def _write_new_atomic(root: Path, destination: Path, rendered: str) -> None:
    _assert_no_reparse_point_ancestors(root, destination.parent)
    if os.path.lexists(destination):
        raise BaselineError(f"workspace baseline destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    _assert_no_reparse_point_ancestors(root, destination.parent)
    with _pin_baseline_ancestors(root, destination.parent) as ancestor_pins:
        ancestor_pins.verify()
        if os.path.lexists(destination):
            raise BaselineError(
                f"workspace baseline destination already exists: {destination}"
        )
        temporary_path: Path | None = None
        try:
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    newline="\n",
                    dir=destination.parent,
                    prefix=f".{destination.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as handle:
                    temporary_path = Path(handle.name)
                    handle.write(rendered)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:
                raise BaselineError(
                    "workspace baseline reparse point replacement or temporary "
                    "write was blocked during publication"
                ) from exc
            ancestor_pins.verify()
            if os.path.lexists(destination):
                raise BaselineError(
                    f"workspace baseline destination already exists: {destination}"
                )
            try:
                os.link(temporary_path, destination)
            except FileExistsError as exc:
                raise BaselineError(
                    f"workspace baseline destination already exists: {destination}"
                ) from exc
            except OSError as exc:
                raise BaselineError(
                    f"workspace baseline could not be published atomically: {destination}"
                ) from exc
            ancestor_pins.verify()
            _assert_atomic_publication(temporary_path, destination)
            temporary_path.unlink()
            temporary_path = None
        finally:
            if temporary_path is not None and os.path.lexists(temporary_path):
                try:
                    ancestor_pins.verify()
                except BaselineError:
                    pass
                else:
                    temporary_path.unlink()


def _run_git_result(root: Path, arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    if not arguments or arguments[0] not in _READ_ONLY_GIT_COMMANDS:
        raise WorkspaceGuardError("workspace guard attempted a non-read-only Git command")
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        check=False,
    )


def _run_git(root: Path, arguments: Sequence[str]) -> str:
    completed = _run_git_result(root, arguments)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "unknown Git read error"
        raise WorkspaceGuardError(f"git {' '.join(arguments)} failed: {detail}")
    return completed.stdout


def _run_git_bytes(root: Path, arguments: Sequence[str]) -> bytes:
    if not arguments or arguments[0] not in _READ_ONLY_GIT_COMMANDS:
        raise WorkspaceGuardError("workspace guard attempted a non-read-only Git command")
    completed = subprocess.run(
        ["git", *arguments],
        cwd=root,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode(
            "utf-8",
            errors="replace",
        ).strip() or "unknown Git read error"
        raise WorkspaceGuardError(f"git {' '.join(arguments)} failed: {detail}")
    return completed.stdout


def _git_repository_identity(root: Path) -> Path:
    """Return the canonical common Git directory for one exact worktree root."""

    if not root.is_dir():
        raise BaselineError(
            f"cannot establish Git repository identity: root is not a directory: {root}"
        )
    try:
        git_root_raw = _run_git(root, ("rev-parse", "--show-toplevel")).strip()
        common_dir_raw = _run_git(
            root,
            ("rev-parse", "--path-format=absolute", "--git-common-dir"),
        ).strip()
    except (OSError, WorkspaceGuardError) as exc:
        raise BaselineError(
            f"cannot establish Git repository identity for {root}: {exc}"
        ) from exc
    if not git_root_raw or not common_dir_raw:
        raise BaselineError(
            f"cannot establish Git repository identity for {root}: empty Git metadata"
        )
    git_root = Path(git_root_raw).resolve()
    if git_root != root:
        raise BaselineError(
            f"cannot establish Git repository identity: not a repository root: {root}"
        )
    common_dir = Path(common_dir_raw).resolve()
    if not common_dir.is_dir():
        raise BaselineError(
            "cannot establish Git repository identity: "
            f"common directory is missing: {common_dir}"
        )
    return common_dir


def _git_path_lines(stdout: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                _normalize_relative_path(line)
                for line in stdout.splitlines()
                if line
            }
        )
    )


def _path_is_allowed(path: str, allowlist: Sequence[str]) -> bool:
    for allowed in allowlist:
        if allowed.endswith("/"):
            if path.startswith(allowed):
                return True
        elif path == allowed:
            return True
    return False


def _is_phase2_deferred_metadata(path: str) -> bool:
    return path == _PHASE2_DEFERRED_METADATA_PATH


def _is_omx_project_metadata(path: str) -> bool:
    return path == ".gitignore" or _OMX_PROJECT_METADATA_PATH.fullmatch(path) is not None


def _is_standalone_review_iteration_metadata(path: str, phase: str) -> bool:
    match = _STANDALONE_REVIEW_ITERATION_PATH.fullmatch(path)
    if match is None:
        return False
    review_phase = match.group(2)
    expected_directory = (
        "01-hermetic-standalone-foundation"
        if review_phase == "01"
        else "02-advisory-api-and-secondary-evidence"
    )
    return match.group(1) == expected_directory and int(review_phase) <= int(phase)


def _is_standard_summary(path: str, root: Path, phase: str) -> bool:
    match = _SUMMARY_PATH.fullmatch(path)
    if match is None or match.group(1) != phase:
        return False
    plan = root / "options_copilot" / "plans" / (
        f"{match.group(1)}-{match.group(2)}-PLAN.md"
    )
    return plan.is_file()


def _unquote(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {'"', "'", "`"}:
        return stripped[1:-1]
    return stripped


def _phase_key(value: str) -> int:
    match = re.fullmatch(r"P([0-9]+)", value)
    return int(match.group(1)) if match is not None else 10_000


def _plan_id_key(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"P([0-9]+)-([0-9]+)", value)
    if match is None:
        return (10_000, 10_000)
    return int(match.group(1)), int(match.group(2))


def _plan_file_key(path: Path) -> tuple[int, int]:
    match = _PLAN_NAME.fullmatch(path.name)
    if match is None:
        return (10_000, 10_000)
    return _phase_key(match.group(1)), int(match.group(2))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify Options Copilot workspace scope")
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--phase", choices=(*EXPECTED_PHASES, "01", "02"))
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("data/options_copilot/evidence/workspace/P0/pre_edit_baseline.json"),
    )
    parser.add_argument(
        "--allowlists",
        type=Path,
        default=Path("options_copilot/operations/phase_allowlists.json"),
    )
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--source-baseline-commit")
    parser.add_argument("--execution-baseline-commit")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    arguments = parser.parse_args(argv)
    if arguments.capture:
        try:
            baseline = capture_workspace_baseline(
                arguments.root,
                arguments.baseline,
                source_baseline_commit=arguments.source_baseline_commit,
                execution_baseline_commit=arguments.execution_baseline_commit,
            )
        except WorkspaceGuardError as exc:
            print(f"baseline_invalid: {arguments.baseline}: {exc}")
            return 1
        payload = {
            "captured_head_commit": baseline.captured_head_commit,
            "execution_baseline_commit": baseline.execution_baseline_commit,
            "path": baseline.path.as_posix(),
            "source_baseline_commit": baseline.source_baseline_commit,
        }
        if arguments.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        else:
            print(
                "workspace baseline captured with fixed execution authority "
                f"{baseline.execution_baseline_commit}: {baseline.path}"
            )
        return 0
    if arguments.phase is None:
        parser.error("--phase is required unless --capture is used")
    report = verify_workspace(
        arguments.root,
        arguments.phase,
        arguments.baseline,
        arguments.allowlists,
    )
    if arguments.json:
        print(json.dumps(report.as_dict(), indent=2, ensure_ascii=False))
    elif report.ok:
        print(
            f"workspace guard passed for {report.phase}: "
            f"{len(report.changed_paths)} planned paths"
        )
    else:
        for violation in report.violations:
            print(f"{violation.kind}: {violation.path}: {violation.message}")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EXPECTED_PHASES",
    "EXPECTED_PHASE2_PLANS",
    "EXPECTED_STANDALONE_PLANS",
    "PHASE2_PLAN_AUTHORITY_COMMIT",
    "PHASE2_SUPPLEMENTAL_AUTHORITY_COMMIT",
    "AllowlistConsistencyReport",
    "AllowlistError",
    "BaselineError",
    "ConsistencyIssue",
    "PlanParseError",
    "PlanScope",
    "ScopeDifference",
    "ScopePathError",
    "WorkspaceBaseline",
    "WorkspacePathHash",
    "WorkspaceReport",
    "WorkspaceViolation",
    "build_phase_allowlists",
    "build_standalone_phase_allowlist",
    "capture_workspace_baseline",
    "hash_workspace_path",
    "load_phase_allowlists",
    "load_standalone_phase_allowlist",
    "load_workspace_baseline",
    "main",
    "parse_master_allowlists",
    "parse_plan",
    "render_phase_allowlists",
    "render_standalone_phase_allowlist",
    "validate_phase_allowlist",
    "validate_plan_allowlist",
    "validate_scope_path",
    "verify_allowlist_consistency",
    "verify_workspace",
]
