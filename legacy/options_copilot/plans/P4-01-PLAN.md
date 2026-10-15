---
phase: P4-universe-scheduler
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/scanner/__init__.py
  - options_copilot/scanner/scheduler.py
  - options_copilot/scanner/service.py
  - options_copilot/scanner/cli.py
  - tests/options_copilot/test_scan_scheduler.py
  - tests/options_copilot/test_scanner_authority.py
evidence_prefixes: []
autonomous: true
requirements: [OC-SCAN-01, OC-TIME-01, OC-OPS-01, OC-VAL-01]
must_haves:
  truths:
    - "A fixed two-hour market slot has at most one durable scan run across processes, restarts, and duplicate ticks."
    - "Scheduler service owns no approval, bridge, creator, or broker-write authority."
    - "The scheduler calls one DecisionPipeline port whose concrete P5 implementation follows the frozen module order."
  artifacts:
    - path: "options_copilot/scanner/scheduler.py"
      provides: "SQLite/WAL scan-run lease and fixed-slot policy"
      contains: "ScanRunStore"
    - path: "options_copilot/scanner/service.py"
      provides: "Heartbeat service and DecisionPipeline port"
      contains: "DecisionPipelinePort"
  key_links:
    - from: "options_copilot/scanner/service.py"
      to: "options_copilot/scanner/scheduler.py"
      via: "slot acquisition before one pipeline invocation"
      pattern: "acquire"
---

<objective>
Implement the durable two-hour scheduler, run ledger, heartbeat service, and authority boundary.

Purpose: Produce exactly one read-only decision run per authoritative market slot without catch-up storms or creator access.
Output: Scheduler/store/service/CLI and concurrency, calendar, restart, and authority tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/market/session_calendar.py
@options_copilot/bridge/store.py
@trade_copilot/advisor/scheduler.py
@options_copilot/IMPLEMENTATION_PATTERNS.md
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P4-01-01: Build durable scan run ledger and fixed slots</name>
  <read_first>
    - options_copilot/bridge/store.py
    - options_copilot/market/session_calendar.py
    - trade_copilot/advisor/scheduler.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/scanner/scheduler.py, tests/options_copilot/test_scan_scheduler.py</files>
  <action>Use `trade_copilot/advisor/scheduler.py` only as a read-only scheduling-pattern reference. Implement a separate SQLite/WAL `ScanRunStore` with BEGIN IMMEDIATE, unique `(trading_date, slot_at, pipeline_version)`, lease owner/expiry, heartbeat, complete, fail, and duplicate reason. Normal authoritative slots are 09:30, 11:30, 13:30, and 15:30 America/New_York, but keep only slots earlier than broker-published close; a 13:00 early close keeps 09:30 and 11:30. Restart may recover the newest incomplete read-only slot only when age is at most two hours; all older missed slots persist `MISSED_NOT_REPLAYED` and never burst-run.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_scan_scheduler.py</automated>
  </verify>
  <done>Slots, leases, duplicate reasons, and bounded recovery are durable and deterministic.</done>
  <acceptance_criteria>
    - Two processes competing for one slot produce one run owner
    - DST, holiday, early-close, and Shanghai-Saturday fixtures are exact
    - Restart recovers at most one eligible slot and records every skipped older slot
    - Repeating a tick returns a fixed duplicate reason and does not create a second run
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P4-01-02: Implement heartbeat service and DecisionPipeline port</name>
  <read_first>
    - options_copilot/scanner/scheduler.py
    - options_copilot/market/session_calendar.py
    - options_copilot/state/snapshot.py
    - options_copilot/bridge/coordinator.py
  </read_first>
  <files>options_copilot/scanner/__init__.py, options_copilot/scanner/service.py, options_copilot/scanner/cli.py, tests/options_copilot/test_scan_scheduler.py, tests/options_copilot/test_scanner_authority.py</files>
  <action>Implement a 30-second heartbeat and CLI `serve`, `tick`, and `inspect-slot`. After acquiring a due slot, call exactly one `DecisionPipelinePort.run_slot(scan_run_id, slot_at)`; P5 supplies the concrete pipeline. The service stores pipeline result/hash and completion or fixed failure reason. Scheduler modules may read and write scan/evidence/decision data only and must not import or call approval, bridge, creator, or any broker-write surface. Add AST/import tests and fail-on-import sentinels for those forbidden modules.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_scan_scheduler.py tests/options_copilot/test_scanner_authority.py</automated>
  </verify>
  <done>The service invokes one decision pipeline per acquired slot and has no external-review authority.</done>
  <acceptance_criteria>
    - Continuous heartbeats invoke the port once per slot
    - CLI tick emits scan_run_id, duplicate reason, and pipeline result hash
    - Forbidden-module import sentinels never trigger
    - Authority audit remains green
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Market clock to run ledger | External calendar state determines whether a slot is due. |
| Scheduler to DecisionPipeline | One leased read-only slot triggers one deterministic decision run. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P4-01-01 | Denial of Service | catch-up | mitigate | At most one recent recovery and persistent skipped reasons. |
| T-P4-01-02 | Tampering | duplicate run | mitigate | Unique index plus immediate transaction. |
| T-P4-01-03 | Elevation of Privilege | scheduler imports | mitigate | Deny-architecture tests and authority audit. |
</threat_model>

<verification>
Run scheduler concurrency, calendar, restart, and authority tests.
</verification>

<success_criteria>
Each due slot can trigger one and only one authority-free DecisionPipeline execution.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
