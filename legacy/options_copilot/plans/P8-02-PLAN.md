---
phase: P8-replay-outcomes
plan: "02"
type: execute
wave: 2
depends_on: ["P8-01"]
files_modified:
  - options_copilot/replay/engine.py
  - options_copilot/replay/dataset.py
  - tests/options_copilot/test_replay.py
evidence_prefixes: []
autonomous: true
requirements: [OC-LEARN-01, OC-ANALYTICS-01, OC-STRAT-01, OC-EXIT-01, OC-VAL-01]
must_haves:
  truths:
    - "Replay sees only evidence first seen by decision time and preserves all exclusions."
    - "Replay identity binds frozen ranking snapshot, initial policy, execution cost, dataset, and independence hashes."
    - "Time windows are rolling train/calibration/test and never randomly shuffled."
  artifacts:
    - path: "options_copilot/replay/engine.py"
      provides: "Point-in-time DecisionPipeline replay"
      contains: "PointInTimeReplay"
    - path: "options_copilot/replay/dataset.py"
      provides: "Versioned dataset manifest and exclusion ledger"
  key_links:
    - from: "options_copilot/replay/engine.py"
      to: "options_copilot/decision/pipeline.py"
      via: "same frozen decision pipeline at historical cutoff"
      pattern: "decision_at"
---

<objective>
Implement point-in-time replay and walk-forward datasets against the same frozen pipeline and contracts.

Purpose: Measure historical behavior without future leakage, policy drift, or cost-model substitution.
Output: Replay/dataset modules and leakage, hash, exclusion, and time-split tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/storage/evidence.py
@options_copilot/decision/pipeline.py
@options_copilot/ranking/store.py
@options_copilot/governance/independence_spec.v1.json
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P8-02-01: Build point-in-time dataset and exclusion manifest</name>
  <read_first>
    - options_copilot/storage/evidence.py
    - options_copilot/ranking/store.py
    - options_copilot/governance/independence_spec.v1.json
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
    - options_copilot/governance/execution_cost_contract.v1.json
  </read_first>
  <files>options_copilot/replay/dataset.py, tests/options_copilot/test_replay.py</files>
  <action>Create a versioned dataset manifest with decision cutoffs, universe membership, included/excluded evidence IDs and reasons, quote/secdef availability, scan and ranking IDs, initial-policy version/hash, execution-cost version/hash, independence version/hash, source versions, and canonical dataset hash. Every evidence record must have first_seen_at at or before decision_at. Missing executable bid/ask remains `MISSING_EXECUTION_DATA`; never backfill last/close/mid. Build rolling chronological train, calibration, and test windows without random shuffle.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_replay.py</automated>
  </verify>
  <done>Replay datasets are point-in-time, exclusion-complete, chronological, and contract-bound.</done>
  <acceptance_criteria>
    - Future-injected evidence cannot change an earlier manifest
    - Missing execution data stays missing
    - Every exclusion has a reason and source ID/hash
    - Changed policy, cost, independence, source, or window changes dataset hash
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P8-02-02: Replay the frozen DecisionPipeline and ranking ledger</name>
  <read_first>
    - options_copilot/replay/dataset.py
    - options_copilot/decision/pipeline.py
    - options_copilot/ranking/store.py
    - options_copilot/governance/independence_spec.v1.json
  </read_first>
  <files>options_copilot/replay/engine.py, tests/options_copilot/test_replay.py</files>
  <action>Implement `PointInTimeReplay.run` using the same DecisionPipeline order and frozen signed policy/cost inputs. Persist input/evidence/broker-snapshot simulation/policy/cost/candidate/ranking hashes, Top 3 or NO_TRADE, exclusion log, and replay result hash. The output ranking schema matches the live immutable ledger, including ranking_snapshot_id and candidate hashes, but carries REPLAY authority only and cannot create an approval. Same manifest and contract hashes produce the same output.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_replay.py tests/options_copilot/test_decision_pipeline.py</automated>
  </verify>
  <done>Replay reproduces the frozen live pipeline without future leakage or approval authority.</done>
  <acceptance_criteria>
    - Exact same inputs produce exact same replay hash
    - Future evidence injection leaves earlier outputs unchanged
    - Replay ranking rows cannot pass live authorization
    - All policy and cost versions/hashes propagate to candidates and rankings
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Historical evidence to replay | Late data must not appear before first seen. |
| Replay ranking to governance | Evaluation data has no live approval authority. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P8-02-01 | Tampering | cutoff data | mitigate | First-seen cutoff and dataset hash. |
| T-P8-02-02 | Elevation of Privilege | replay ranking | mitigate | REPLAY authority tag and live authorization rejection. |
| T-P8-02-03 | Repudiation | exclusions | mitigate | Complete reasoned exclusion ledger. |
</threat_model>

<verification>
Run replay, leakage, chronology, immutable hash, and no-approval-authority tests.
</verification>

<success_criteria>
Historical evaluation uses only then-visible evidence and the same frozen decision contracts.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
