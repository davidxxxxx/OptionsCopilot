---
phase: P8-replay-outcomes
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/governance/independence_spec.v1.json
  - data/options_copilot/evidence/checkpoints/P8/independence-spec/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P8/independence-spec/
autonomous: false
requirements: [OC-LEARN-01, OC-ANALYTICS-01, OC-VAL-01]
must_haves:
  truths:
    - "No replay sample counting or outcome aggregation starts without a signed immutable independence specification."
    - "The signed initial Champion/scenario policy and Execution Cost Contract are frozen prerequisites, not P8 tuning inputs."
  artifacts:
    - path: "options_copilot/governance/independence_spec.v1.json"
      provides: "Human-approved scenario clustering and count rules"
    - path: "data/options_copilot/evidence/checkpoints/P8/independence-spec/"
      provides: "Signed decision evidence and prerequisite contract hashes"
  key_links:
    - from: "options_copilot/governance/independence_spec.v1.json"
      to: "options_copilot/governance/initial_champion_scenario_policy.v1.json"
      via: "frozen prerequisite hash recorded in decision evidence"
      pattern: "initial_policy_hash"
---

<objective>
Freeze the scenario independence and clustering rules before replay or outcome counting.

Purpose: Prevent correlated scans/events from inflating the 30-scenario Discovery gate.
Output: Signed immutable independence specification and decision evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/IMPLEMENTATION_RESEARCH.md
@options_copilot/learning/governance.py
@options_copilot/governance/initial_champion_scenario_policy.v1.json
@options_copilot/governance/execution_cost_contract.v1.json
</context>

<tasks>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P8-01-01: Sign Independence and Clustering Specification</name>
  <read_first>
    - options_copilot/IMPLEMENTATION_RESEARCH.md
    - options_copilot/learning/governance.py
    - options_copilot/storage/canonical.py
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
    - options_copilot/governance/execution_cost_contract.v1.json
  </read_first>
  <files>options_copilot/governance/independence_spec.v1.json, data/options_copilot/evidence/checkpoints/P8/independence-spec/</files>
  <action>Post-implementation only: before any P8 count, require the user to decide and sign deterministic rules for same ticker/catalyst, corporate family and correlated ticker clusters, overlapping event windows, adjacent two-hour slots, overlapping holdings/thesis/structure, cross-provider duplicate/conflict, representative selection, count weight restricted to 0 or 1, boundaries, and missing evidence. UNKNOWN defaults to not counted. Record version, effective_at, actor, signed_at, rationale, content hash, and the verified frozen initial-policy and execution-cost contract versions/hashes. Later changes create v2 and never rewrite v1 outcomes.</action>
  <verify>
    <automated>python -m options_copilot.governance.cli verify-contract --path options_copilot/governance/independence_spec.v1.json --json</automated>
  </verify>
  <done>The user has frozen reproducible independence/count rules, or P8 counting remains disabled.</done>
  <acceptance_criteria>
    - Every named clustering case has a deterministic key and count result
    - UNKNOWN is explicitly excluded from count
    - Initial policy and cost contract hashes are verified and recorded
    - Missing signature/hash blocks P8-02 and P8-03
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Human clustering decision to outcome counter | Signed rules determine which scenes are independent. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P8-01-01 | Tampering | independence rules | mitigate | Signed version/hash and immutable prior versions. |
| T-P8-01-02 | Repudiation | scene count | mitigate | Deterministic cluster keys and human actor/time. |
</threat_model>

<verification>
Verify the signed spec and prerequisite policy/cost hashes at the blocking checkpoint.
</verification>

<success_criteria>
Replay and outcome plans have one signed, deterministic independence source.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
