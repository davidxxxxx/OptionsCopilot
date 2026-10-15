---
phase: P2-gld-position-management
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/positions/models.py
  - options_copilot/positions/manager.py
  - options_copilot/positions/generator.py
  - options_copilot/positions/cli.py
  - tests/options_copilot/test_position_manager.py
  - tests/options_copilot/test_gld_management_pipeline.py
evidence_prefixes: []
autonomous: true
requirements: [OC-RISK-02, OC-SAFE-02, OC-DATA-01, OC-EXIT-01, OC-VAL-01]
must_haves:
  truths:
    - "GLD permits only CLOSE_ALL or mathematically proven REDUCE_RISK; it never opens, rolls, crosses zero, or increases any leg."
    - "Transition proof binds authoritative before/after positions, secdefs, quotes, immutable broker snapshot, Exit Contract, and Execution Cost Contract."
    - "Existing position normalization follows the read-only timestamp/sign conventions of trade_copilot/state/position_state.py without modifying it."
  artifacts:
    - path: "options_copilot/positions/manager.py"
      provides: "Before/after transition proof"
      contains: "prove_transition"
    - path: "options_copilot/positions/generator.py"
      provides: "Snapshot-derived GLD close/reduce candidate generator"
      contains: "ManagementCandidateGenerator"
  key_links:
    - from: "options_copilot/positions/generator.py"
      to: "options_copilot/gateway/broker_snapshot.py"
      via: "authoritative positions, secdefs, and coherent quote batch"
      pattern: "broker_snapshot_hash"
---

<objective>
Create a complete GLD close/reduce-only core that proves every proposed transition from authoritative broker state.

Purpose: Make the existing GLD combination manageable without opening a second combination or creating hidden risk.
Output: Position models, proof engine, generator, dry-run CLI, and property/integration tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/BASELINE.md
@options_copilot/gateway/broker_snapshot.py
@options_copilot/governance/execution_cost_contract.v1.json
@trade_copilot/state/position_state.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P2-01-01: Define authoritative position normalization and transition proof</name>
  <read_first>
    - trade_copilot/state/position_state.py
    - options_copilot/domain/models.py
    - options_copilot/risk/payoff.py
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/governance/execution_cost_contract.v1.json
  </read_first>
  <files>options_copilot/positions/models.py, options_copilot/positions/manager.py, tests/options_copilot/test_position_manager.py</files>
  <action>Use `trade_copilot/state/position_state.py` strictly as a read-only analog for normalization, UTC timestamps, and signed quantities; never modify or import broker authority from it. Define `PositionManagementKind` with only CLOSE_ALL and REDUCE_RISK, immutable authoritative positions, capital-usage proof, and transition proof. Recompute after positions from before plus exact conId execution deltas. CLOSE_ALL requires same conIds, exact opposite actions, equal absolute quantities, and zero after. REDUCE_RISK forbids a new conId, absolute quantity increase, zero crossing, net-short increase, max-loss increase, exposure increase, or capital-usage increase. Bind before/after values, P1 snapshot hash, secdef identity hashes, coherent quote batch, Exit Contract, and signed Execution Cost Contract version/hash. Unknown inputs fail closed.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_position_manager.py</automated>
  </verify>
  <done>Position transitions are normalized, reproducible, and limited to exact close or strict reduction.</done>
  <acceptance_criteria>
    - Current GLD 1/-2/1 exact opposite deltas produce zero after position
    - New conId, increased quantity, crossing zero, net-short increase, max-loss increase, or capital increase of 0.01 rejects
    - Cost contract, exit contract, snapshot, secdef, and quote hashes are all part of proof identity
    - `git diff -- trade_copilot/state/position_state.py` is empty
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P2-01-02: Generate GLD management candidates from atomic snapshots</name>
  <read_first>
    - options_copilot/positions/models.py
    - options_copilot/positions/manager.py
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/proposals.py
    - options_copilot/BASELINE.md
  </read_first>
  <files>options_copilot/positions/generator.py, options_copilot/positions/cli.py, tests/options_copilot/test_gld_management_pipeline.py</files>
  <action>Implement `ManagementCandidateGenerator.generate(snapshot, exit_contract, cost_contract)`. Identify the current combination only from authoritative positions and standard secdefs; do not hardcode conIds. Derive opposite sides, ratios, combo quantity, and signed executable close value from the coherent quote batch. Apply commissions, fees, per-leg spread/slippage, assignment/early-exercise/ex-dividend assumptions from the signed cost contract. Generate CLOSE_ALL and only valid REDUCE_RISK candidates with immutable transition proofs and complete exit rules. Add a dry-run CLI that writes a preview only and cannot reserve or invoke a creator. Missing quote, secdef, contract, hash, or proof returns structured NO_TRADE.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_position_manager.py tests/options_copilot/test_gld_management_pipeline.py</automated>
  </verify>
  <done>GLD candidates are derived from current broker truth and remain preview-only until later approval gates.</done>
  <acceptance_criteria>
    - Fixture covers GLD +375C/-2x385C/+390C shape with conIds supplied by snapshot
    - Signed cost contract version/hash appears in candidate and transition proof
    - Stale or incoherent quotes and nonstandard contracts produce NO_TRADE
    - Dry-run leaves external attempt and instruction counts at zero
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Broker positions to normalized combination | Signed quantities and timestamps must preserve actual exposure. |
| Proposed deltas to transition proof | Untrusted intent labels are replaced by mathematical before/after proof. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P2-01-01 | Tampering | position intent | mitigate | Recompute from conId quantities; ignore self-reported reduce labels. |
| T-P2-01-02 | Elevation of Privilege | management path | mitigate | Exact close/reduce enum and no new/cross-zero/increase behavior. |
| T-P2-01-03 | Repudiation | transition costs | mitigate | Signed cost-contract and source hashes in proof. |
</threat_model>

<verification>
Run position property tests and the full GLD snapshot-to-preview fixture.
</verification>

<success_criteria>
GLD can be closed or strictly reduced from authoritative fresh inputs, while every new-risk path remains blocked.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
