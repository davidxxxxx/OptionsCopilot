---
phase: P8-replay-outcomes
plan: "03"
type: execute
wave: 2
depends_on: ["P8-01"]
files_modified:
  - options_copilot/learning/outcomes.py
  - options_copilot/storage/canonical.py
  - options_copilot/storage/ledger.py
  - tests/options_copilot/test_outcomes.py
  - tests/options_copilot/test_learning_storage_approval.py
evidence_prefixes: []
autonomous: true
requirements: [OC-LEARN-01, OC-EXIT-01, OC-ANALYTICS-01, OC-VAL-01]
must_haves:
  truths:
    - "Outcomes preserve frozen decision, ranking, policy, cost, and independence identities."
    - "Late data creates a superseding version and never overwrites the original."
    - "Only one representative per signed independence cluster contributes a count weight of one."
  artifacts:
    - path: "options_copilot/learning/outcomes.py"
      provides: "Immutable forward outcome and clustered aggregate ledger"
      contains: "OutcomeRecorder"
  key_links:
    - from: "options_copilot/learning/outcomes.py"
      to: "options_copilot/ranking/store.py"
      via: "frozen decision and candidate/ranking hashes"
      pattern: "ranking_snapshot_id"
---

<objective>
Record immutable cost-adjusted outcomes and apply the signed independence specification to scenario counts.

Purpose: Produce auditable learning data without overwrites, duplicate scene inflation, or policy/cost drift.
Output: Outcome ledger, canonical/hash support, and independence/dedup/late-data tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/governance/independence_spec.v1.json
@options_copilot/ranking/store.py
@options_copilot/governance/execution_cost_contract.v1.json
@options_copilot/learning/governance.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P8-03-01: Implement immutable cost-adjusted OutcomeRecorder</name>
  <read_first>
    - options_copilot/ranking/store.py
    - options_copilot/storage/ledger.py
    - options_copilot/storage/canonical.py
    - options_copilot/governance/execution_cost_contract.v1.json
  </read_first>
  <files>options_copilot/learning/outcomes.py, options_copilot/storage/canonical.py, options_copilot/storage/ledger.py, tests/options_copilot/test_outcomes.py</files>
  <action>Store one versioned outcome identity per decision ID, candidate hash, ranking snapshot ID, and horizon. Record MFE, MAE, mark and executable exit PnL separately, signed commission/fees/spread/slippage/assignment/exercise/dividend costs, exit-rule hits, quote quality, timestamps, and canonical hash. Bind input/evidence/broker/policy/cost/exit/independence hashes. Late data appends a superseding version and leaves the original unchanged. Cost contract absence/mismatch prevents the outcome from entering evaluation.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_outcomes.py</automated>
  </verify>
  <done>Forward outcomes are immutable, fully cost-adjusted, and tied to their frozen decision/ranking identity.</done>
  <acceptance_criteria>
    - Cost-before-positive/cost-after-negative is explicitly classified
    - Late data creates a new version with original preserved
    - Changed candidate/ranking/policy/cost/exit hash creates a distinct or invalid record
    - Missing execution quote quality is reported, not filled
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P8-03-02: Apply signed clustering and independent count rules</name>
  <read_first>
    - options_copilot/learning/outcomes.py
    - options_copilot/governance/independence_spec.v1.json
    - options_copilot/learning/governance.py
  </read_first>
  <files>options_copilot/learning/outcomes.py, tests/options_copilot/test_outcomes.py, tests/options_copilot/test_learning_storage_approval.py</files>
  <action>Generate cluster ID, representative decision, and count weight exactly from the signed spec. Bind spec version/hash into every outcome, aggregate counter, and evaluation input. Same ticker/event, adjacent same-ticker two-hour slots, overlapping windows, correlated tickers in one macro event, and cross-provider duplicates count once. Truly independent ticker/event/window examples count separately. UNKNOWN evidence has weight zero and a fixed exclusion reason. Spec v1/v2 never mix in one report.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_outcomes.py tests/options_copilot/test_learning_storage_approval.py</automated>
  </verify>
  <done>Independent scenario counts are deterministic, spec-bound, and cannot be inflated by duplicates or correlation.</done>
  <acceptance_criteria>
    - All named duplicate/correlation cases count one
    - Independent cases count separately
    - UNKNOWN counts zero
    - Spec version/hash is present in outcome, aggregate, and report identity
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Market outcomes to learning ledger | Later prices and quote quality become immutable evaluation evidence. |
| Outcomes to independent counter | Signed cluster rules determine count weight. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P8-03-01 | Tampering | outcome history | mitigate | Append-only supersession and canonical hash. |
| T-P8-03-02 | Spoofing | independent count | mitigate | Signed deterministic cluster rules. |
| T-P8-03-03 | Repudiation | cost attribution | mitigate | Signed cost version/hash and component ledger. |
</threat_model>

<verification>
Run outcome, late-data, cost, clustering, and independent-count suites.
</verification>

<success_criteria>
Learning receives immutable, cost-adjusted, correctly deduplicated outcomes only.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
