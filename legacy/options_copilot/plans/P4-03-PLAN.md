---
phase: P4-universe-scheduler
plan: "03"
type: execute
wave: 2
depends_on: ["P4-01", "P4-02"]
files_modified:
  - data/options_copilot/evidence/checkpoints/P4/scanner-live/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P4/scanner-live/
autonomous: false
requirements: [OC-DATA-02, OC-SCAN-01, OC-RISK-02, OC-VAL-01]
must_haves:
  truths:
    - "A real slot uses signed pacing limits, returns subscriptions to baseline, and creates no entry while GLD remains."
    - "Two ticks for the same slot produce one scan run and one explicit duplicate reason."
  artifacts:
    - path: "data/options_copilot/evidence/checkpoints/P4/scanner-live/"
      provides: "Live run, duplicate-tick, budget, cache, and subscription evidence"
  key_links:
    - from: "options_copilot/scanner/cli.py"
      to: "options_copilot/scanner/scheduler.py"
      via: "two exact ticks for one slot"
      pattern: "inspect-slot"
---

<objective>
Verify one real market slot end to end, including exact duplicate-tick behavior and subscription cleanup.

Purpose: Convert scheduler and pacing tests into named live read-only evidence before P5 ranking.
Output: Immutable P4 scanner checkpoint artifacts only.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/scanner/cli.py
@options_copilot/scanner/scheduler.py
@options_copilot/scanner/universe.py
@options_copilot/scanner/pacing.py
</context>

<tasks>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task P4-03-01: Run one live slot and duplicate tick</name>
  <read_first>
    - options_copilot/scanner/cli.py
    - options_copilot/scanner/scheduler.py
    - options_copilot/scanner/universe.py
    - options_copilot/scanner/pacing.py
    - options_copilot/operations/readiness.py
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P4/scanner-live/</files>
  <action>Post-implementation only: choose the current authoritative ET slot and run exactly `python -m options_copilot.scanner.cli tick --slot SLOT_ISO --json --evidence-dir data/options_copilot/evidence/checkpoints/P4/scanner-live`, then run the identical command a second time, then run `python -m options_copilot.scanner.cli inspect-slot --slot SLOT_ISO --expect-run-count 1 --expect-duplicate-reason SLOT_ALREADY_COMPLETED --json --evidence-dir data/options_copilot/evidence/checkpoints/P4/scanner-live`. Human-check signed capability version/hash, per-class planned/used/rejected budgets, cache provenance, funnel reasons, GLD management-only result, before/after subscriptions, zero creator/bridge attempts, one scan_run_id, and the duplicate reason. Pacing or entitlement degradation fails live acceptance.</action>
  <verify>
    <automated>python -m options_copilot.scanner.cli inspect-slot --slot SLOT_ISO --expect-run-count 1 --expect-duplicate-reason SLOT_ALREADY_COMPLETED --json --evidence-dir data/options_copilot/evidence/checkpoints/P4/scanner-live</automated>
  </verify>
  <done>One real slot has one run, one duplicate reason, bounded requests, clean subscriptions, and no authority side effect.</done>
  <acceptance_criteria>
    - Exactly one completed scan run exists for the slot
    - Second tick records `SLOT_ALREADY_COMPLETED`
    - Used requests never exceed signed per-class limits
    - Active subscriptions return to baseline and GLD entry_count is zero
    - Manifest includes command, UTC, Git/workspace, capability, run, budget, cache, subscription, and human hashes
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Live broker data to scanner evidence | Real entitlements, pacing, and subscriptions are observed. |
| Human review to checkpoint result | Live acceptance requires evidence review rather than CLI exit alone. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P4-03-01 | Repudiation | duplicate tick | mitigate | Durable run count and explicit duplicate-reason artifact. |
| T-P4-03-02 | Denial of Service | live subscriptions | mitigate | Before/after counts and cancel receipts. |
</threat_model>

<verification>
Run two exact ticks and the exact inspect-slot assertion during the post-implementation live checkpoint.
</verification>

<success_criteria>
P4 has named live evidence for one idempotent, bounded, authority-free scan slot.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
