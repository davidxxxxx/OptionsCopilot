---
phase: P0-capability-baseline
plan: "02"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/operations/authority_audit.py
  - options_copilot/operations/workspace_guard.py
  - options_copilot/operations/phase_allowlists.json
  - tests/options_copilot/test_authority_audit.py
  - tests/options_copilot/test_workspace_guard.py
  - tests/options_copilot/test_plan_allowlist_consistency.py
  - data/options_copilot/evidence/workspace/P0/
evidence_prefixes:
  - data/options_copilot/evidence/workspace/P0/
autonomous: true
requirements: [OC-SAFE-01, OC-OPS-01, OC-VAL-01]
must_haves:
  truths:
    - "A pre-edit Git baseline preserves all user changes and never cleans, resets, checks out, or stashes them."
    - "Authority audit detects direct and indirect broker-write callables while allowing explicit negative result fields."
    - "Every phase and plan allowlist equals its task-file union plus declared evidence prefixes, with missing and surplus entries rejected."
  artifacts:
    - path: "options_copilot/operations/workspace_guard.py"
      provides: "Read-only workspace baseline and allowlist enforcement"
      contains: "verify"
    - path: "options_copilot/operations/authority_audit.py"
      provides: "Repository-scoped broker-authority audit"
      contains: "call_graph"
    - path: "tests/options_copilot/test_plan_allowlist_consistency.py"
      provides: "Missing-and-surplus plan allowlist test"
  key_links:
    - from: "tests/options_copilot/test_plan_allowlist_consistency.py"
      to: "options_copilot/plans/*-PLAN.md"
      via: "frontmatter and XML task-file extraction"
      pattern: "files_modified"
---

<objective>
Freeze the workspace boundary and mechanically enforce permanent review-only authority plus exact plan allowlists.

Purpose: Protect unrelated dirty files and prevent silent scope or authority expansion during P0-P10 execution.
Output: Authority audit, workspace guard, phase allowlists, evidence baseline, and consistency tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/IMPLEMENTATION_PLAN.md
@options_copilot/IMPLEMENTATION_PATTERNS.md
@options_copilot/bridge/coordinator.py
@options_copilot/bridge/store.py
@options_copilot/gateway/ibkr_readonly.py
</context>

<tasks>

<task type="auto">
  <name>Task P0-02-01: Capture immutable pre-edit workspace evidence</name>
  <read_first>
    - options_copilot/IMPLEMENTATION_PLAN.md
    - options_copilot/IMPLEMENTATION_PATTERNS.md
  </read_first>
  <files>data/options_copilot/evidence/workspace/P0/</files>
  <action>Before any implementation edit, capture `git rev-parse HEAD`, `git status --porcelain=v2 --untracked-files=all`, `git diff --name-status`, and `git diff --cached --name-status`, with UTC time, repository root, exact stdout, and content hashes for all dirty or untracked paths. Store immutable preflight evidence under this prefix. Never run a cleanup, reset, checkout, stash, or restore operation. If the timing cannot be proven, stop and request a new human-approved baseline.</action>
  <verify>
    <automated>git rev-parse HEAD; git status --porcelain=v2 --untracked-files=all; git diff --name-status; git diff --cached --name-status</automated>
  </verify>
  <done>The original worktree state is recorded before implementation and remains untouched.</done>
  <acceptance_criteria>
    - Preflight evidence contains the four exact commands, UTC time, commit, and path hashes
    - Existing dirty and untracked paths are preserved byte-for-byte
    - Evidence contains no cleanup or mutation command
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P0-02-02: Build repository authority audit</name>
  <read_first>
    - options_copilot/bridge/coordinator.py
    - options_copilot/bridge/store.py
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/IMPLEMENTATION_PATTERNS.md
  </read_first>
  <files>options_copilot/operations/authority_audit.py, tests/options_copilot/test_authority_audit.py</files>
  <action>Parse every production `options_copilot/**/*.py` file and build import-alias, attribute-call, constant-getattr, Protocol, inheritance, and local function call graphs. Reject direct broker-write callable definitions/imports/calls and propagate rejection through two- and three-level wrappers. Explicit false-valued status fields and negative audit assertions are allowed, but no implemented callable may submit, transmit, or place an order. Future creator adapter files are included automatically.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_authority_audit.py &amp;&amp; python -m options_copilot.operations.authority_audit --root options_copilot --json</automated>
  </verify>
  <done>Direct and indirect broker-write surfaces fail the audit while review-only status fields pass.</done>
  <acceptance_criteria>
    - Fixtures catch aliases, constant getattr, Protocol exposure, inheritance, and three wrapper levels
    - The repository audit scans future `bridge/creator_adapter.py` without a hardcoded file list
    - Review-only creator and false submission/transmission fields pass
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P0-02-03: Enforce exact phase and plan allowlists</name>
  <read_first>
    - options_copilot/IMPLEMENTATION_PLAN.md
    - options_copilot/plans/P0-01-PLAN.md
    - options_copilot/operations/authority_audit.py
    - data/options_copilot/evidence/workspace/P0/
  </read_first>
  <files>options_copilot/operations/workspace_guard.py, options_copilot/operations/phase_allowlists.json, tests/options_copilot/test_workspace_guard.py, tests/options_copilot/test_plan_allowlist_consistency.py</files>
  <action>Implement a read-only workspace guard that imports the immutable P0 baseline and compares later changes against exact P0-P10 phase allowlists. Generate `phase_allowlists.json` from the master phase declarations and executable plan frontmatter, but require a checked-in deterministic representation. The consistency test must parse every plan's `files_modified`, every task `files`, and `evidence_prefixes`; it fails when a plan or master phase has either a missing entry or a surplus entry. Repo-root globs, `.planning`, `trade_copilot`, Rithmic, paper server, and unnamed scripts are always rejected. Pre-existing dirty unchanged passes; changing it fails.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_workspace_guard.py tests/options_copilot/test_plan_allowlist_consistency.py</automated>
  </verify>
  <done>Workspace and plan scope are exact, mechanically checked, and preserve every unrelated user change.</done>
  <acceptance_criteria>
    - Tests reject both a missing and a surplus file in plan frontmatter
    - Tests reject both a missing and a surplus file in the master phase declaration
    - Unchanged pre-existing dirty paths pass; modified pre-existing dirty paths fail
    - No guard operation mutates Git or deletes evidence
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Plan artifacts to executor | Declared files become the only authorized implementation scope. |
| Source code to authority audit | Local code is treated as untrusted until its call graph is proven review-only. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P0-02-01 | Tampering | worktree | mitigate | Immutable pre-edit hashes and fail-closed path comparison. |
| T-P0-02-02 | Elevation of Privilege | local broker wrappers | mitigate | Alias-aware transitive call-graph audit. |
| T-P0-02-03 | Repudiation | plan scope | mitigate | Checked-in exact allowlist and missing/surplus tests. |
</threat_model>

<verification>
Run all three focused tests plus the authority audit; inspect workspace evidence without changing the worktree.
</verification>

<success_criteria>
Every implementation mutation is attributable to an exact plan entry and no local code has broker execution authority.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
