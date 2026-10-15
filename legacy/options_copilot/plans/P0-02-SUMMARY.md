---
phase: P0-capability-baseline
plan: "02"
subsystem: operations-safety
tags: [ast, call-graph, workspace-guard, allowlists, git-baseline, review-only]

# Dependency graph
requires:
  - phase: P0-capability-baseline
    provides: verified safe baseline at commit 608820feae405b4b21421bc26eb113830f38bcfa
provides:
  - immutable pre-edit workspace evidence with exact Git command output
  - repository-wide transitive broker-authority audit
  - exact P0-P10 plan and phase allowlist enforcement
affects: [P0-03, P0-04, P1-P10 execution]

# Tech tracking
tech-stack:
  added: []
  patterns: [AST-only static analysis, fixed-point call-graph propagation, exact set-equality manifests, read-only Git workspace verification]

key-files:
  created:
    - data/options_copilot/evidence/workspace/P0/pre_edit_baseline.json
    - options_copilot/operations/authority_audit.py
    - options_copilot/operations/workspace_guard.py
    - options_copilot/operations/phase_allowlists.json
    - tests/options_copilot/test_authority_audit.py
    - tests/options_copilot/test_workspace_guard.py
    - tests/options_copilot/test_plan_allowlist_consistency.py
  modified: []

key-decisions:
  - "Audit Python syntax without importing production modules or broker SDKs."
  - "Propagate unsafe authority to a fixed point across aliases, wrappers, and inherited methods."
  - "Treat task files plus evidence prefixes as an exact set, rejecting both omissions and surplus paths."
  - "Exclude only byte-identical pre-existing dirty paths from phase scope checks."

patterns-established:
  - "Future options_copilot Python files enter the authority audit automatically through recursive discovery."
  - "Phase manifests are canonical checked JSON generated from all executable plans and the master declaration."
  - "Adjacent PLAN/SUMMARY pairs are executor metadata and do not grant implementation authority."

requirements-completed: [OC-SAFE-01, OC-OPS-01, OC-VAL-01]

# Metrics
duration: ~31min
completed: 2026-08-03
---

# P0 Plan 02: Capability and Workspace Guard Summary

**The Options Copilot worktree now has an immutable pre-edit baseline, a repository-wide review-only authority proof, and exact mechanically checked P0-P10 scope boundaries.**

## Performance

- **Duration:** ~31 min
- **Started:** 2026-08-03T05:09:43Z
- **Completed:** 2026-08-03T05:40:37Z
- **Tasks:** 3 of 3
- **Implementation files created:** 7

## Accomplishments

- Captured the clean pre-edit worktree at `608820feae405b4b21421bc26eb113830f38bcfa`, including the four exact Git outputs and an exact empty dirty-path hash set.
- Added an AST/import/call-graph authority audit covering direct calls, import and callable aliases, literal or named-constant `getattr`, Protocol surfaces, inheritance, cross-module calls, and arbitrary wrapper depth.
- Preserved the valid `ReviewInstructionCreator.create_review_instruction` contract and explicit false result fields while rejecting submit, transmit, and order-placement callables.
- Parsed all 29 executable plans and all P0-P10 master declarations into a deterministic checked manifest; missing and surplus plan or phase entries fail closed.
- Added a read-only workspace guard that compares the current tree with the immutable baseline, permits only declared files or prefixes, and rejects byte changes to pre-existing dirty paths.

## Task Commits

Each task was committed atomically:

1. **Task P0-02-01: Capture immutable pre-edit workspace evidence** — `2be64de`
2. **Task P0-02-02: Build repository authority audit** — `67640ea`
3. **Task P0-02-03: Enforce exact phase and plan allowlists** — `29263b3`
4. **Task P0-02-03 follow-up: Classify adjacent execution summaries as metadata** — `90419b1`

**Plan metadata:** this summary is committed separately.

## Files Created/Modified

- `data/options_copilot/evidence/workspace/P0/pre_edit_baseline.json` — immutable pre-edit commit, command output, root, UTC timestamp, and dirty-path hashes.
- `options_copilot/operations/authority_audit.py` — recursive AST authority audit and JSON CLI.
- `options_copilot/operations/workspace_guard.py` — plan/master parsers, deterministic manifest support, baseline validation, and read-only Git guard CLI.
- `options_copilot/operations/phase_allowlists.json` — checked exact P0-P10 manifest generated from 29 plans and the master declarations.
- `tests/options_copilot/test_authority_audit.py` — direct, alias, constant-`getattr`, Protocol, inheritance, future-file, and transitive-wrapper coverage.
- `tests/options_copilot/test_workspace_guard.py` — read-only behavior, exact phase scope, baseline byte preservation, completeness, and summary-metadata coverage.
- `tests/options_copilot/test_plan_allowlist_consistency.py` — missing/surplus plan and master checks plus prohibited-path enforcement.

## Decisions Made

- The audit uses syntax only, so optional broker adapters are inspected without importing dependencies or opening connections.
- Callable danger propagates until stable rather than stopping at a fixed wrapper depth; this covers the required three levels and deeper future wrappers.
- `order_submitted=False`, `transmitted_to_broker=False`, and negative assertions are data or validation expressions, not callable authority.
- The workspace guard invokes only read-only Git subcommands and validates that every status path in the baseline has exactly one content hash.
- Standard summaries are accepted only beside a matching executable plan for the same phase; they remain visible in the changed-path report and cannot authorize code.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical behavior] Preserve standard summary metadata without expanding implementation scope**

- **Found during:** final plan handoff
- **Issue:** The plan requires a standard summary outside `files_modified`; a strictly literal phase allowlist would reject that required metadata artifact.
- **Fix:** Recognize only `options_copilot/plans/PX-NN-SUMMARY.md` with a matching `PX-NN-PLAN.md` in the same phase. The path stays visible and grants no production permission.
- **Files modified:** `options_copilot/operations/workspace_guard.py`, `tests/options_copilot/test_workspace_guard.py`
- **Verification:** Matching summary passes; an unmatched summary remains out of scope.
- **Committed in:** `90419b1`

---

**Total deviations:** 1 auto-fixed missing behavior
**Impact on plan:** Required executor metadata is supported without broadening any implementation allowlist.

## Issues Encountered

- The evidence prefix is ignored by the repository's broad `data/` rule, so the immutable baseline was intentionally force-added as the sole Task 1 artifact.
- No live services, broker connections, ports, credentials, Tailscale routes, or unrelated worktree files were touched.

## User Setup Required

None.

## Verification

- `python -m pytest -q tests/options_copilot` — 231 passed.
- Focused authority tests — 8 passed.
- Focused workspace and allowlist tests — 19 passed.
- Authority CLI — 41 files, 507 callables, zero violations.
- Workspace guard — baseline `608820f`, all plan changes in P0 scope, zero violations.

## Next Phase Readiness

- P0-03 can consume the checked scope manifest and permanent authority audit.
- Later phase executors can use the same immutable baseline to prove unrelated user bytes remain unchanged.
- No blocker or user setup remains for the next P0 plan.

---
*Phase: P0-capability-baseline*
*Completed: 2026-08-03*
