---
phase: P5-strategy-ranking
plan: "04"
type: execute
wave: 3
depends_on: ["P5-03"]
files_modified:
  - options_copilot/decision/__init__.py
  - options_copilot/decision/pipeline.py
  - options_copilot/execution_cost.py
  - options_copilot/scanner/service.py
  - tests/options_copilot/test_decision_pipeline.py
  - tests/options_copilot/test_execution_cost_resolver.py
evidence_prefixes: []
autonomous: true
requirements: [OC-DATA-02, OC-ANALYTICS-01, OC-STRAT-01, OC-SCAN-01, OC-VAL-01]
must_haves:
  truths:
    - "DecisionPipeline uses the exact declared module order from scheduler slot through immutable ranking store."
    - "Every dependency is constructor-injected and every output binds scan, evidence, snapshot, policy, cost, candidate, and ranking hashes."
    - "Pipeline concurrency and tamper tests produce one immutable Top-3 snapshot or structured NO_TRADE."
  artifacts:
    - path: "options_copilot/decision/pipeline.py"
      provides: "Named end-to-end decision orchestration"
      contains: "DecisionPipeline"
    - path: "options_copilot/execution_cost.py"
      provides: "Signed deterministic execution-cost and after-cost EV resolver"
      contains: "SignedExecutionCostResolver"
  key_links:
    - from: "options_copilot/scanner/service.py"
      to: "options_copilot/decision/pipeline.py"
      via: "one acquired scheduler slot calls run_slot"
      pattern: "DecisionPipeline"
---

<objective>
Wire the exact scheduler-to-Top3 DecisionPipeline and prove full-pipeline concurrency/tamper behavior.

Purpose: Eliminate missing-module ambiguity before runtime/GUI integration.
Output: DecisionPipeline, scheduler adapter wiring, and deterministic full-pipeline tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/scanner/service.py
@options_copilot/scanner/universe.py
@options_copilot/gateway/broker_snapshot.py
@options_copilot/strategies/templates.py
@options_copilot/analytics/scenarios.py
@options_copilot/ranking/store.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P5-04-01: Implement exact DecisionPipeline wiring</name>
  <read_first>
    - options_copilot/scanner/service.py
    - options_copilot/scanner/universe.py
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/strategies/templates.py
    - options_copilot/analytics/scenarios.py
    - options_copilot/governance/execution_cost_contract.v1.json
    - options_copilot/ranking/store.py
  </read_first>
  <files>options_copilot/decision/__init__.py, options_copilot/decision/pipeline.py, options_copilot/execution_cost.py, options_copilot/scanner/service.py, tests/options_copilot/test_decision_pipeline.py, tests/options_copilot/test_execution_cost_resolver.py</files>
  <action>Define constructor-injected `DecisionPipeline` and wire exactly: scheduler slot -> UniverseFunnel -> broker/options evidence acquisition -> strategy templates and generator -> volatility/scenario engine -> signed Execution Cost Contract application -> hard eligibility/risk/DTE/one-combination gates -> PortfolioRanker -> immutable Top3 RankingStore -> result reference for runtime/GUI. Implement the local `SignedExecutionCostResolver` as the sole checked-in cost authority: verify the human-signed contract identity/effective head, recompute Decimal commission/slippage and each candidate's expiry-scenario EV from frozen executable legs, reject quote/scenario/candidate binding drift, and recheck the same head before persistence. No step may be skipped or reordered. Preserve P2 management priority and emit structured NO_TRADE when any binding is missing. Return scan_run_id, ranking_snapshot_id or NO_TRADE reasons, and canonical input/evidence/snapshot/policy/cost/result hashes. Adapt `scanner.service` to the concrete port without importing approval, bridge, or creator.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_decision_pipeline.py tests/options_copilot/test_scanner_authority.py</automated>
  </verify>
  <done>The named pipeline executes every required module in order and stores immutable Top-3 or NO_TRADE.</done>
  <acceptance_criteria>
    - A spy fixture asserts the exact call order
    - Every constructor dependency is explicit
    - Missing policy/cost/snapshot/evidence yields NO_TRADE and no ranking authorization
    - Cost contract tamper, stale quotes, per-candidate scenario mismatch, or head change yields NO_TRADE and zero ranking rows
    - Scanner authority tests remain green
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-04-02: Prove full pipeline, concurrency, and tamper rejection</name>
  <read_first>
    - options_copilot/decision/pipeline.py
    - options_copilot/scanner/scheduler.py
    - options_copilot/ranking/store.py
    - tests/options_copilot/test_ranking_store.py
  </read_first>
  <files>tests/options_copilot/test_decision_pipeline.py</files>
  <action>Add end-to-end fixtures for management-only GLD, eligible Top 3, structured NO_TRADE, two workers on one scan run, concurrent ranking supersession, and tampering of input, evidence, snapshot, policy, cost, candidate, score, or chain hashes. Assert one ranking snapshot per scan run, ranks 1-3 uniqueness, stable replay output, and zero approval/bridge mutation in the decision plane.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_decision_pipeline.py tests/options_copilot/test_ranking_store.py</automated>
  </verify>
  <done>Full-pipeline tests prove deterministic immutable output and fail-closed concurrent/tampered behavior.</done>
  <acceptance_criteria>
    - One scan run has one ranking snapshot under concurrency
    - Tampering each named hash fails before authorization
    - GLD fixture has no entry candidate
    - Decision plane adds no approval, reservation, creator, or instruction row
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Scheduler slot to decision pipeline | A durable slot begins an authority-free evidence and ranking run. |
| Pipeline result to runtime/GUI | Only immutable ranking references leave the decision plane. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P5-04-01 | Tampering | pipeline order/input | mitigate | Explicit constructor graph, spy order tests, and hash propagation. |
| T-P5-04-02 | Race condition | duplicate ranking | mitigate | Scan-run uniqueness and ranking transaction. |
| T-P5-04-03 | Elevation of Privilege | decision plane | mitigate | No approval/bridge/creator imports or writes. |
</threat_model>

<verification>
Run the exact full-pipeline, authority, concurrency, and tamper suites.
</verification>

<success_criteria>
One acquired slot deterministically yields one immutable Top-3/NO_TRADE result through the exact required pipeline.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
