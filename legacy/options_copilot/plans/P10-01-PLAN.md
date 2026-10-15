---
phase: P10-production-readiness
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/operations/runbook.md
  - options_copilot/operations/monitoring.py
  - options_copilot/operations/migrations.md
  - options_copilot/operations/readiness.py
  - options_copilot/api/app.py
  - scripts/start_options_copilot.ps1
  - tests/options_copilot/test_process_operations.py
  - tests/options_copilot/test_readiness_cli.py
evidence_prefixes: []
autonomous: true
requirements: [OC-OPS-01, OC-VAL-01, OC-SAFE-01, OC-SCAN-01, OC-DATA-01]
must_haves:
  truths:
    - "Default launch refuses every existing 8891 listener and never stops an unknown process."
    - "Explicit owned restart requires complete identity proof and restores the exact prior owned state on failed start."
    - "Runbook verifies broker/account/orders/instructions, exact processes/ports, logs, APIs, Tailscale routes, databases, and no leftovers before and after operations."
    - "Implementation and automated tests never run a Tailscale command; they validate supplied snapshots and prove that only a separately human-approved 8891 mapping created by P10 is eligible for targeted rollback."
  artifacts:
    - path: "options_copilot/operations/runbook.md"
      provides: "Exact startup, shutdown, reconciliation, backup, recovery, and rollout operations"
    - path: "options_copilot/operations/monitoring.py"
      provides: "Safety/reliability metrics and approval-disable policy"
    - path: "tests/options_copilot/test_process_operations.py"
      provides: "Ownership, PID reuse, restore, and protected-service tests"
    - path: "options_copilot/operations/readiness.py"
      provides: "Pure Tailscale/listener snapshot comparison, route adoption, ownership, health-evidence, and targeted restoration validation"
  key_links:
    - from: "scripts/start_options_copilot.ps1"
      to: "options_copilot/operations/readiness.py"
      via: "exact owned-instance proof and health verification"
      pattern: "RestartOwnedInstance"
    - from: "options_copilot/operations/readiness.py"
      to: "data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/"
      via: "before/after route hashes and P10-created ownership marker validation"
      pattern: "CREATED_BY_P10_02_01"
---

<objective>
Implement safe process lifecycle, snapshot-only remote-route ownership validation, migrations/backups, monitoring, and a complete review-only operations runbook.

Purpose: Protect existing ports/services/Tailscale and make failures recoverable without touching unknown processes or pre-existing routes.
Output: Runbook, monitoring/migration docs, launcher/readiness integration, and process/route ownership/restoration tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@scripts/start_options_copilot.ps1
@options_copilot/operations/readiness.py
@options_copilot/IMPLEMENTATION_PATTERNS.md
@options_copilot/README.md
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P10-01-01: Enforce fail-on-listener and exact owned restart recovery</name>
  <read_first>
    - scripts/start_options_copilot.ps1
    - options_copilot/operations/readiness.py
    - options_copilot/runtime.py
    - options_copilot/IMPLEMENTATION_PATTERNS.md
  </read_first>
  <files>scripts/start_options_copilot.ps1, options_copilot/operations/readiness.py, tests/options_copilot/test_process_operations.py, tests/options_copilot/test_readiness_cli.py</files>
  <action>Keep ordinary launch fail-on-any-existing-8891-listener, including an owned listener. Only explicit `-RestartOwnedInstance` may stop anything, and before stopping it must prove exact loopback socket, PID, process creation time, executable, script/module, canonical repo path, data directory, and instance-marker nonce. Unknown/missing evidence, PID reuse, same name in another repo, wildcard command line, or protected port identity mismatch yields stop call count zero. Save a secret-free prior-state/config/launch-args hash before an owned stop. If new start fails, restore that exact prior state and verify PID/listener/health; if restore fails, disable approval and report both failures without touching other services. In `readiness.py`, add pure parsers/comparators for externally captured Tailscale Serve JSON and listener snapshots; implementation/tests must inject fixture text/files and must never spawn the Tailscale executable. Represent the exact desired route as tailnet-only `https://davidxu.tail6885a3.ts.net:8891/ -> http://127.0.0.1:8891` while treating the user-reported live mapping as drift-prone until the P10-02 checkpoint re-queries it. Protect `https://davidxu.tail6885a3.ts.net/ -> http://127.0.0.1:8878` and all unrelated handlers byte-for-byte. A route decision is either `ADOPTED_PREEXISTING` with before hash and no mutation ownership, or `CREATED_BY_P10_02_01` with before/after hashes, exact handler key/target, human approval/actor/time, and an ownership nonce. Targeted rollback eligibility is true only when the before snapshot proves the exact 8891 handler absent and the signed ownership marker plus current snapshot prove this checkpoint created that same handler; adopted/pre-existing/ambiguous/drifted routes always yield zero remove/reset calls.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_process_operations.py tests/options_copilot/test_readiness_cli.py</automated>
  </verify>
  <done>Launcher refuses unknown listeners and can safely restore only a fully proven owned instance.</done>
  <acceptance_criteria>
    - Unknown listener, same-name other repo, PID reuse, and wildcard command fixtures have zero stop calls
    - Default launch refuses even an owned listener
    - Explicit fully owned restart can stop once
    - New-start failure restores prior state or hard-disables approval without touching protected services
    - Snapshot fixtures classify exact existing 8891 as ADOPTED_PREEXISTING with zero mutation ownership
    - Missing-route creation, partial failure, adopted-route failure, ownership tamper, protected-root drift, and targeted restoration mocks never invoke a real Tailscale process
    - Only a proven P10-created exact 8891 handler is eligible for targeted off; generic reset and root/443 replacement are unrepresentable
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P10-01-02: Implement monitoring, backup, and directed migration operations</name>
  <read_first>
    - options_copilot/operations/readiness.py
    - options_copilot/storage/evidence.py
    - options_copilot/ranking/store.py
    - options_copilot/bridge/store.py
    - options_copilot/approval/store.py
  </read_first>
  <files>options_copilot/operations/monitoring.py, options_copilot/operations/migrations.md, options_copilot/api/app.py, tests/options_copilot/test_process_operations.py</files>
  <action>Implement safety indicators for broker-write attempts, naked/unknown-risk acceptance, stale quote approval, duplicate creator attempt, UNKNOWN retry, GLD entry, rank bypass, workspace/authority audit, and contract/hash drift; any violation disables approval/creator and preserves read-only GUI. Add reliability metrics for scan uniqueness, per-class budgets, subscription baseline, provider health, quote quality, costs, ranking/outcomes, database integrity, local/remote health, and protected-port identity. Document migrations as backup to `G:\quantumtrading\backups\options_copilot\TIMESTAMP`, integrity check, directed user_version migration, post-check, and recover-on-failure; never overwrite the original on failure.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_process_operations.py tests/options_copilot/test_readiness_cli.py tests/options_copilot/test_evidence_store.py tests/options_copilot/test_ranking_store.py</automated>
  </verify>
  <done>Safety violations fail closed and database operations are backed up, directed, verified, and recoverable on G:.</done>
  <acceptance_criteria>
    - Each safety violation disables approval/creator in tests
    - Backup path is exactly under G: and contains timestamp plus hashes
    - Unknown schema version and failed integrity check never migrate
    - Failed migration preserves/restores original and retains evidence
  </acceptance_criteria>
</task>

<task type="auto">
  <name>Task P10-01-03: Write complete operational runbook</name>
  <read_first>
    - options_copilot/README.md
    - scripts/start_options_copilot.ps1
    - options_copilot/operations/readiness.py
    - options_copilot/operations/monitoring.py
    - options_copilot/operations/migrations.md
    - options_copilot/plans/P10-02-PLAN.md
  </read_first>
  <files>options_copilot/operations/runbook.md</files>
  <action>Write exact post-implementation procedures for pre/post process and listener capture, command-line ownership, broker session/account/positions/working orders/unsubmitted instructions reconciliation, logs, local API, databases/integrity, scheduler/pacing, creator UNKNOWN reconciliation, backup/restore, and Tailscale status plus actual remote HTTPS endpoints. Only operate on the exact owned 8891 process. Require protected 8766/8788/8878 PID and command hashes plus `https://davidxu.tail6885a3.ts.net/ -> http://127.0.0.1:8878` to remain unchanged. Transcribe the exact Tailscale command sequence from the `autonomous: false` P10-02 human checkpoint into a clearly boxed `POST-IMPLEMENTATION ONLY — HUMAN ACTION` runbook section; no command is run by this implementation task or its tests. The runbook must first capture raw route/listener state. If the exact separate tailnet-only `:8891 -> http://127.0.0.1:8891` mapping already exists, adopt it and record evidence without mutation. If absent, stop for explicit human approval before the separate :8891 creation; never replace root/443 or perform a global reset. Record an ownership marker/before-after diff only for a mapping proven created by that checkpoint. Verify local and remote `/health`, `/api/bootstrap`, `/api/positions`, `/api/candidates`, and `/api/learning`. On failure, an adopted/pre-existing route is never removed; a P10-created route may run only the P10-02 targeted-off sequence after the pure ownership verifier passes, followed by exact before-state restoration proof. Always perform a final no-leftover process/listener check.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_process_operations.py tests/options_copilot/test_readiness_cli.py</automated>
  </verify>
  <done>Runbook gives exact safe, recoverable, remote-verified operations without touching unrelated services.</done>
  <acceptance_criteria>
    - Runbook includes every named pre/post broker/process/port/log/API/database/Tailscale check
    - Runbook prohibits stopping unknown PIDs and changing protected routes
    - Runbook requires local and actual remote health/read-API checks, not localhost alone
    - Runbook has exact adopt/create/targeted-rollback branches with before/after hashes and forbids reset/root replacement
    - Every Tailscale command is post-implementation human action; implementation/tests use supplied snapshots/mocks only
    - Runbook ends with no-leftover verification
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| OS listener/process data to launcher | Process identity determines whether an explicit restart is allowed. |
| Local service to Tailscale remote route | Remote availability must coexist with protected existing service routing. |
| Human-captured route snapshot to restoration | Ownership evidence determines whether the separate 8891 mapping may be removed. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P10-01-01 | Spoofing | owned process | mitigate | Socket/PID/time/executable/path/data/nonce proof. |
| T-P10-01-02 | Denial of Service | failed restart | mitigate | Prior-state capture and exact restore. |
| T-P10-01-03 | Tampering | database migration | mitigate | G: backup, integrity checks, directed versions, recovery. |
| T-P10-01-04 | Elevation of Privilege | safety violation | mitigate | Automatic approval/creator disable, read-only remains. |
| T-P10-01-05 | Tampering | route ownership marker | mitigate | Before/after hashes, exact handler identity, human actor/time, nonce, and targeted-only restoration tests. |
</threat_model>

<verification>
Run process ownership, recovery, route snapshot/ownership/restoration mocks, monitoring, and database migration suites; do not operate services or invoke Tailscale in this implementation plan.
</verification>

<success_criteria>
Operations are safe, reversible, remote-aware, and cannot stop or alter an unknown service.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
