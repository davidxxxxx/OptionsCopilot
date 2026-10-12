---
phase: P0-capability-baseline
plan: "03"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/governance/contracts.py
  - options_copilot/governance/cli.py
  - options_copilot/governance/strategy_nav_contract.v1.json
  - options_copilot/governance/execution_cost_contract.v1.json
  - options_copilot/governance/initial_champion_scenario_policy.v1.json
  - tests/options_copilot/test_governance_contracts.py
  - data/options_copilot/evidence/checkpoints/P0/strategy-nav-contract/
  - data/options_copilot/evidence/checkpoints/P0/execution-cost-contract/
  - data/options_copilot/evidence/checkpoints/P0/initial-policy/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P0/strategy-nav-contract/
  - data/options_copilot/evidence/checkpoints/P0/execution-cost-contract/
  - data/options_copilot/evidence/checkpoints/P0/initial-policy/
autonomous: false
requirements: [OC-RISK-01, OC-ANALYTICS-01, OC-STRAT-01, OC-LEARN-01, OC-VAL-01]
must_haves:
  truths:
    - "Strategy NAV is governed by a signed cash-flow contract and never falls back to account NLV."
    - "No GLD, entry, ranking, replay, outcome, approval, or creator payload exists without the frozen execution-cost contract version and hash."
    - "P5, P7, and P8 consume one signed initial Champion and scenario policy; P9 may propose later promotion but cannot mutate it automatically."
  artifacts:
    - path: "options_copilot/governance/contracts.py"
      provides: "Canonical signed-contract validation and correction/versioning"
      contains: "SignedContract"
    - path: "options_copilot/governance/execution_cost_contract.v1.json"
      provides: "Approved real-account execution-cost model"
    - path: "options_copilot/governance/initial_champion_scenario_policy.v1.json"
      provides: "Frozen baseline probability and ranking policy"
  key_links:
    - from: "options_copilot/governance/cli.py"
      to: "options_copilot/governance/contracts.py"
      via: "canonical validation and human signature evidence"
      pattern: "verify-contract"
---

<objective>
Freeze the three irreversible governance inputs required by risk, ranking, replay, and external review creation.

Purpose: Prevent executors or later learning code from guessing NAV attribution, trading costs, or baseline probability policy.
Output: Contract validator/CLI, tests, three signed versioned contracts, and immutable decision evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/BASELINE.md
@options_copilot/README.md
@options_copilot/IMPLEMENTATION_RESEARCH.md
@options_copilot/performance/campaign.py
@options_copilot/learning/governance.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P0-03-01: Implement signed governance contract validation</name>
  <read_first>
    - options_copilot/BASELINE.md
    - options_copilot/performance/campaign.py
    - options_copilot/learning/governance.py
    - options_copilot/storage/canonical.py
  </read_first>
  <files>options_copilot/governance/contracts.py, options_copilot/governance/cli.py, tests/options_copilot/test_governance_contracts.py</files>
  <action>Implement canonical JSON hashing, actor/time signatures, immutable version identifiers, provenance validation, and append-only correction rules for Strategy NAV, Execution Cost, and Initial Champion and Scenario Policy contracts. A correction creates a new version with `supersedes_version` and a new hash; it never overwrites a prior contract. Provide `python -m options_copilot.governance.cli verify-contract --path PATH --json` and signing subcommands that never accept secrets. Consumers must validate expected contract kind, version, hash, signer, and effective time.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_governance_contracts.py</automated>
  </verify>
  <done>All three contract types are canonically hashed, signed, versioned, and fail closed on mutation or missing provenance.</done>
  <acceptance_criteria>
    - Tests reject field mutation, hash mismatch, missing actor/time, wrong contract kind, and overwrite attempts
    - Correction tests require `supersedes_version` and preserve the prior artifact
    - CLI returns fixed nonzero codes for missing or invalid contracts
  </acceptance_criteria>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P0-03-02: Sign Strategy NAV cash-flow ledger contract</name>
  <read_first>
    - options_copilot/BASELINE.md
    - options_copilot/README.md
    - options_copilot/performance/campaign.py
    - options_copilot/governance/contracts.py
  </read_first>
  <files>options_copilot/governance/strategy_nav_contract.v1.json, data/options_copilot/evidence/checkpoints/P0/strategy-nav-contract/</files>
  <action>Post-implementation only: before P1, require the user to decide and sign the anchor amount/time/account/source hash, STRATEGY versus NON_STRATEGY attribution for positions/fills/fees/PnL, deposit and withdrawal signs/effective times, idempotent flow identity, conflict behavior, append-only corrections, exact Decimal equation, and rounding. State explicitly that account NLV is observation/reconciliation only and is never a risk input or fallback. Persist the canonical contract and decision evidence with actor, signed_at, version, and matching hash.</action>
  <verify>
    <automated>python -m options_copilot.governance.cli verify-contract --path options_copilot/governance/strategy_nav_contract.v1.json --json</automated>
  </verify>
  <done>A complete signed Strategy NAV contract exists and P1 has no discretion to substitute NLV.</done>
  <acceptance_criteria>
    - Every listed attribution and correction decision is explicit
    - Contract and decision evidence hashes match
    - The contract contains an explicit prohibition on NLV fallback
    - Missing approval blocks P1
  </acceptance_criteria>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P0-03-03: Calibrate and sign Execution Cost Contract</name>
  <read_first>
    - options_copilot/BASELINE.md
    - options_copilot/IMPLEMENTATION_RESEARCH.md
    - options_copilot/risk/payoff.py
    - options_copilot/governance/contracts.py
  </read_first>
  <files>options_copilot/governance/execution_cost_contract.v1.json, data/options_copilot/evidence/checkpoints/P0/execution-cost-contract/</files>
  <action>Post-implementation only: calibrate and require human approval of a real-account Execution Cost Contract covering the commission schedule, exchange and regulatory fees, per-leg bid/ask spread and slippage, assignment, early-exercise, and ex-dividend risk, plus a conservative fallback. Record calibration provenance, date, sample identifiers, sample size, assumptions, source schedule, correction/versioning rule, actor, signed_at, version, and content hash. Conservative fallback values must be explicit and approved. The contract must declare that GLD candidates, transition proofs, strategy candidates, ranking ledger rows, replay/outcomes, approvals, and creator payloads bind its version/hash; absence or mismatch prohibits candidate generation and approval.</action>
  <verify>
    <automated>python -m options_copilot.governance.cli verify-contract --path options_copilot/governance/execution_cost_contract.v1.json --json</automated>
  </verify>
  <done>A signed calibrated cost contract exists and every downstream candidate path is required to bind it.</done>
  <acceptance_criteria>
    - Contract covers every named fee, spread/slippage, assignment, exercise, and dividend risk field
    - Provenance/date/sample/fallback/correction/version/hash are present
    - Missing or unsigned fallback blocks candidate and approval paths
    - Downstream binding targets are enumerated in the contract
  </acceptance_criteria>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P0-03-04: Sign Initial Champion and Scenario Policy</name>
  <read_first>
    - options_copilot/IMPLEMENTATION_RESEARCH.md
    - options_copilot/learning/governance.py
    - options_copilot/ranking/engine.py
    - options_copilot/governance/contracts.py
  </read_first>
  <files>options_copilot/governance/initial_champion_scenario_policy.v1.json, data/options_copilot/evidence/checkpoints/P0/initial-policy/</files>
  <action>Post-implementation only: before P5, require the user to sign the baseline probability methodology, calibration dataset and provenance, scenario definitions, baseline feature weights, caps for each supporting-evidence family, hard `NO_TRADE` thresholds, policy version/hash, effective time, and append-only correction rules. The policy must say supporting evidence cannot create eligibility and P9 can only evaluate or propose a later Challenger promotion through separate signed governance; P9 cannot mutate this baseline automatically. P5, P7, and P8 must verify this frozen contract.</action>
  <verify>
    <automated>python -m options_copilot.governance.cli verify-contract --path options_copilot/governance/initial_champion_scenario_policy.v1.json --json</automated>
  </verify>
  <done>A signed baseline Champion and scenario policy exists for P5, P7, and P8, with automatic mutation prohibited.</done>
  <acceptance_criteria>
    - Probability methodology, calibration provenance, weights, caps, and NO_TRADE thresholds are explicit
    - Contract carries actor, signed_at, version, hash, and correction rules
    - Policy states P9 cannot overwrite or auto-promote the baseline
    - Missing approval blocks P5
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Human policy decisions to signed artifacts | Irreversible financial assumptions become code-consumable contracts. |
| Signed artifacts to candidate pipeline | Hash-bound contracts gate risk, ranking, replay, and creator behavior. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P0-03-01 | Tampering | governance contracts | mitigate | Canonical hash, signature, immutable prior versions, and explicit supersession. |
| T-P0-03-02 | Repudiation | human approval | mitigate | Actor, signed_at, source provenance, and decision evidence. |
| T-P0-03-03 | Elevation of Privilege | learning policy | mitigate | P9 cannot mutate the initial policy or risk constants automatically. |
</threat_model>

<verification>
Run focused contract tests, then verify each signed artifact at its blocking decision checkpoint.
</verification>

<success_criteria>
P1, P5, P7, and P8 begin only with complete signed NAV, cost, and initial policy contracts.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
