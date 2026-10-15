---
phase: P9-champion-challenger
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/learning/similarity.py
  - options_copilot/learning/evaluation.py
  - options_copilot/learning/markers.py
  - options_copilot/learning/governance.py
  - options_copilot/learning/policy_authority.py
  - options_copilot/learning/governance_cli.py
  - options_copilot/risk/authorization.py
  - tests/options_copilot/test_learning_evaluation.py
  - tests/options_copilot/test_a_grade_marker.py
  - tests/options_copilot/test_policy_authority.py
evidence_prefixes: []
autonomous: true
requirements: [OC-LEARN-01, OC-RISK-01, OC-STRAT-01, OC-BRIDGE-01, OC-SEC-01, OC-VAL-01]
must_haves:
  truths:
    - "Challenger writes shadow predictions only and cannot mutate the signed initial Champion/scenario policy."
    - "Thirty independent scenarios unlock Discovery only, never production or A-grade."
    - "PromotionAuthority, RollbackAuthority, and A-grade authority are separate durable human-signed artifacts; no runtime, UI, model, or evaluation path can sign them."
    - "CurrentPolicyResolver replays the verified authority ledger, falls back to the immutable initial policy, and atomically returns to the prior signed policy on rollback."
    - "A-grade requires a separately signed P9 marker bound to evaluation, proposal, current policy, cost, ranking basis, dataset, independence, and risk-contract hashes."
    - "Rollback atomically changes the current policy head and revokes every A-grade marker bound to the rolled-back policy so stale rankings and approvals cannot proceed."
  artifacts:
    - path: "options_copilot/learning/evaluation.py"
      provides: "Cost-adjusted Champion/Challenger evaluation reports"
      contains: "EvaluationReport"
    - path: "options_copilot/learning/markers.py"
      provides: "Signed PromotionAuthority, RollbackAuthority, and proposal-bound A-grade artifact schemas/verifiers"
      contains: "PromotionAuthority"
    - path: "options_copilot/learning/policy_authority.py"
      provides: "Durable append-only authority ledger and versioned CurrentPolicyResolver"
      contains: "CurrentPolicyResolver"
    - path: "options_copilot/learning/governance_cli.py"
      provides: "Human-only promotion, A-grade, and rollback signing/verifying commands"
      contains: "sign-promotion"
  key_links:
    - from: "options_copilot/risk/authorization.py"
      to: "options_copilot/learning/markers.py"
      via: "strict P9 A-grade marker verification"
      pattern: "risk_authority_marker_hash"
    - from: "options_copilot/learning/policy_authority.py"
      to: "options_copilot/governance/initial_champion_scenario_policy.v1.json"
      via: "immutable fallback/source-of-truth when no valid promotion is current"
      pattern: "CurrentPolicyResolver"
    - from: "options_copilot/learning/governance_cli.py"
      to: "options_copilot/learning/policy_authority.py"
      via: "explicit human signing and atomic append-only authority transactions"
      pattern: "sign-rollback"
---

<objective>
Implement shadow evaluation, durable signed promotion/rollback resolution, and proposal-bound A-grade authority.

Purpose: Enable learning while preventing automatic policy or risk-tier changes and make restart/concurrency/rollback behavior deterministic.
Output: Similarity/evaluation modules, signed authority artifacts and CLI, durable resolvers, and adversarial authority tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/governance/initial_champion_scenario_policy.v1.json
@options_copilot/governance/independence_spec.v1.json
@options_copilot/learning/outcomes.py
@options_copilot/learning/governance.py
@options_copilot/risk/authorization.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P9-01-01: Implement read-only similarity and evaluation reports</name>
  <read_first>
    - options_copilot/learning/outcomes.py
    - options_copilot/learning/governance.py
    - options_copilot/storage/canonical.py
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
  </read_first>
  <files>options_copilot/learning/similarity.py, options_copilot/learning/evaluation.py, tests/options_copilot/test_learning_evaluation.py</files>
  <action>Implement `SimilarityIndex.query` returning evidence IDs, distance, index version/hash, and no mutation capability. Implement immutable EvaluationReport with cost-adjusted EV, Brier/calibration, coverage, maximum drawdown, tail loss, independent count, scenario breakdown, reference dataset hash, initial Champion policy hash, Challenger version/hash, execution cost hash, and P8 independence hash. Challenger produces shadow_prediction only. Reaching 30 independent scenes changes governance to DISCOVERY and cannot change production policy, risk, ranking, or creator behavior.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_learning_evaluation.py tests/options_copilot/test_learning_storage_approval.py</automated>
  </verify>
  <done>Similarity and evaluation are immutable, cost/spec bound, and authority-free.</done>
  <acceptance_criteria>
    - Similarity results cannot modify candidate, risk, policy, or ranking state
    - Reports contain every named metric and hash
    - 30-scene fixture ends DISCOVERY/SHADOW_ONLY
    - Initial policy artifact remains byte-for-byte unchanged
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P9-01-02: Implement durable signed policy authorities, resolver, and human-only CLI</name>
  <read_first>
    - options_copilot/learning/evaluation.py
    - options_copilot/learning/governance.py
    - options_copilot/storage/canonical.py
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
  </read_first>
  <files>options_copilot/learning/markers.py, options_copilot/learning/governance.py, options_copilot/learning/policy_authority.py, options_copilot/learning/governance_cli.py, tests/options_copilot/test_policy_authority.py</files>
  <action>Define immutable signed `PromotionAuthority` and `RollbackAuthority` artifacts with phase/decision, monotonic sequence, actor, signed_at, prior/current policy version/hash, immutable initial-policy source hash, evaluation/report/dataset/independence/cost/risk-contract hashes, reason, governance signature, previous authority hash, and canonical content hash. Implement an append-only SQLite/WAL/FULL authority ledger with directed schema versioning and `BEGIN IMMEDIATE`; the current head is derived only from a verified hash/signature chain. `CurrentPolicyResolver` returns version, policy hash, and the current promotion/rollback authority marker hash, or the verified immutable Initial Champion/Scenario Policy as fallback/source of truth when no promotion is current. A promotion appends a new policy reference and never overwrites the initial artifact. A rollback transaction appends `RollbackAuthority`, returns the resolver head to the explicitly bound prior signed policy, and appends revocations for all A-grade markers bound to the rolled-back head before commit; readers never observe a rolled-back policy with live A-grade authority. Implement exactly these explicit human commands in `python -m options_copilot.learning.governance_cli`: `sign-promotion`, `sign-a-grade`, `sign-rollback`, `verify-promotion-decision`, `verify-a-grade-decision`, and `verify-rollback-decision`. Signing commands require an interactive human confirmation of canonical hashes and the existing human governance signer; runtime, UI, API, model, evaluator, scheduler, and tests have no production signing entry point and cannot auto-confirm. Add restart/replay, missing/corrupt/tampered chain, wrong prior head, monotonic version, two-process concurrent promotion, promotion-versus-rollback, rollback-versus-rollback, crash-before-commit, and immutable-initial-file tests.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_policy_authority.py tests/options_copilot/test_learning_evaluation.py</automated>
  </verify>
  <done>Only explicit human CLI signatures can change the durable current-policy head, restart reproduces it, and rollback atomically restores the prior signed policy while revoking its A-grade authority.</done>
  <acceptance_criteria>
    - All six required CLI verbs exist and signing has no non-interactive/automatic caller
    - Initial policy bytes remain unchanged and are the deterministic fallback/source of truth
    - Restart resolves the same signed head; tamper or ambiguous concurrent head fails closed
    - Atomic rollback exposes the prior policy and revoked A-grade state together, never separately
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P9-01-03: Implement proposal-bound versioned RiskTierAuthority resolution</name>
  <read_first>
    - options_copilot/learning/markers.py
    - options_copilot/learning/policy_authority.py
    - options_copilot/risk/authorization.py
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
  </read_first>
  <files>options_copilot/learning/markers.py, options_copilot/risk/authorization.py, tests/options_copilot/test_a_grade_marker.py, tests/options_copilot/test_policy_authority.py</files>
  <action>Extend the versioned `RiskTierAuthority` resolver so every resolution has `risk_authority_version`, tier, current policy version/hash, policy-authority marker hash, risk-contract hash, and a non-null canonical `risk_authority_marker_hash`. NORMAL is the immutable default authority state and allows at most 10.00 percent. `APPROVE_A_GRADE` is a distinct human-signed P9 artifact and its content hash becomes `risk_authority_marker_hash`; it must bind the exact proposal/candidate hash, current policy version/hash, policy-authority marker hash, signed cost version/hash, canonical ranking-basis hash, evaluation report, reference dataset, independence spec, risk contract, actor, signed_at, expiry/revocation state, signature, and content hash. It permits 10.01 through 15.00 percent only while all bound heads are current. Unsigned, wrong phase, ordinary promotion, wrong proposal/policy/cost/ranking basis, stale, expired, tampered, superseded, revoked, or rolled-back markers resolve NORMAL/locked and cannot authorize the existing frozen row. Enforce Decimal boundaries exactly: 10.00 passes NORMAL; 10.01 and 15.00 pass only with the exact current signed marker; 15.01, 20.00, and 20.01 reject even with a marker, with 20.00 the absolute hard rejection line. Add marker tamper/revocation and rollback-atomicity tests; no code path signs automatically.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_a_grade_marker.py tests/options_copilot/test_policy_authority.py tests/options_copilot/test_proposal_gate.py</automated>
  </verify>
  <done>Risk-tier resolution is versioned and current-head bound; only an exact human-signed proposal authority permits 10.01-15.00 and rollback immediately locks it.</done>
  <acceptance_criteria>
    - Boundary matrix covers 10.00, 10.01, 15.00, 15.01, 20.00, and 20.01
    - Stale, tampered, wrong-binding, expired, revoked, and rolled-back markers keep A-grade false
    - Marker content hash is the propagated risk_authority_marker_hash for an A-grade resolution
    - NORMAL has a versioned canonical authority-state hash and never exceeds 10.00 percent
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Outcomes to Challenger evaluation | Immutable historical evidence informs shadow comparison only. |
| Human CLI to policy/risk authority | Signed P9 decisions can change Champion reference or proposal-specific A-grade state. |
| Authority ledger to runtime readers | Restart and concurrent writes must resolve one verified atomic head. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P9-01-01 | Elevation of Privilege | Challenger output | mitigate | Shadow-only records and no policy/risk mutation method. |
| T-P9-01-02 | Tampering | A-grade marker | mitigate | Full current-artifact hash and signature verification. |
| T-P9-01-03 | Repudiation | rollback/promotion | mitigate | Append-only actor/time/versioned marker ledger. |
| T-P9-01-04 | Tampering | concurrent authority head | mitigate | SQLite immediate transaction, hash chain, monotonic sequence, and restart/concurrency tests. |
</threat_model>

<verification>
Run evaluation, 30-scene, authority-ledger restart/tamper/concurrency, human-CLI surface, invalid-marker, exact-risk-boundary, and atomic rollback tests.
</verification>

<success_criteria>
Learning can recommend change, but only explicit human-signed P9 authorities change the current policy or proposal-specific A-grade state, and rollback is atomic.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
