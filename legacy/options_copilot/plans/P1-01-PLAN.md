---
phase: P1-strategy-nav-broker-snapshot
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/performance/nav_ledger.py
  - options_copilot/performance/campaign.py
  - options_copilot/risk/authorization.py
  - options_copilot/risk/policy.py
  - tests/options_copilot/test_nav_ledger.py
  - tests/options_copilot/test_proposal_gate.py
evidence_prefixes: []
autonomous: true
requirements: [OC-RISK-01, OC-SAFE-02, OC-VAL-01]
must_haves:
  truths:
    - "A valid signed Strategy NAV contract and ledger head are the only risk base."
    - "NORMAL permits exactly 10.00 percent and rejects 10.01 percent; A-grade requires a valid P9 marker and never exceeds 15.00 percent."
    - "Account NLV and 10K campaign progress cannot change an authorization result."
  artifacts:
    - path: "options_copilot/performance/nav_ledger.py"
      provides: "Append-only cash-flow-attributed Strategy NAV ledger"
      contains: "StrategyNavSnapshot"
    - path: "options_copilot/risk/authorization.py"
      provides: "Risk-tier marker and contract authority validation"
      contains: "RiskTierAuthority"
  key_links:
    - from: "options_copilot/risk/policy.py"
      to: "options_copilot/performance/nav_ledger.py"
      via: "validated StrategyNavSnapshot and hash"
      pattern: "strategy_nav"
---

<objective>
Implement the signed Strategy NAV ledger as the sole risk base and lock exact risk-tier boundaries.

Purpose: Eliminate NLV fallback and campaign-driven risk drift before any new candidate path.
Output: NAV ledger, risk authority, policy integration, and boundary tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/governance/strategy_nav_contract.v1.json
@options_copilot/performance/campaign.py
@options_copilot/risk/policy.py
@options_copilot/BASELINE.md
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P1-01-01: Implement append-only Strategy NAV ledger</name>
  <read_first>
    - options_copilot/governance/strategy_nav_contract.v1.json
    - options_copilot/performance/campaign.py
    - options_copilot/storage/ledger.py
    - options_copilot/BASELINE.md
  </read_first>
  <files>options_copilot/performance/nav_ledger.py, options_copilot/performance/campaign.py, tests/options_copilot/test_nav_ledger.py</files>
  <action>Implement the signed contract field by field. Store an immutable anchor and append-only flow versions. Exact duplicate `flow_id` plus content hash returns the same receipt; same identity with different content records CONFLICT and invalidates risk. Corrections reference the prior version and never overwrite. Emit `StrategyNavSnapshot` with Decimal strategy NAV, attribution totals, ledger head hash, contract version/hash, observed account NLV only as reconciliation, validity, and content hash. Missing or conflicting contract/ledger data fails closed without NLV fallback.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_nav_ledger.py tests/options_copilot/test_campaign.py</automated>
  </verify>
  <done>Strategy NAV is reproducible from signed attribution and invalid states cannot reach risk authorization.</done>
  <acceptance_criteria>
    - Duplicate imports are idempotent and conflicting flow identities invalidate the snapshot
    - Deposits, withdrawals, non-strategy PnL, and corrections follow the signed equation
    - Changing observed NLV alone leaves strategy NAV and risk cap unchanged
    - Missing contract or ledger head returns a fixed NO_TRADE reason
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P1-01-02: Enforce immutable risk-tier authority</name>
  <read_first>
    - options_copilot/performance/nav_ledger.py
    - options_copilot/risk/policy.py
    - options_copilot/proposals.py
    - options_copilot/learning/governance.py
  </read_first>
  <files>options_copilot/risk/authorization.py, options_copilot/risk/policy.py, tests/options_copilot/test_proposal_gate.py</files>
  <action>Change RiskEngine construction to require a valid `StrategyNavSnapshot` and matching contract/head hashes. Add `RiskTierAuthority`: NORMAL is default; A-grade is true only for a valid append-only marker with `phase=P9`, approved report/policy/reference-dataset/independence/risk-contract hashes, actor, signed_at, and valid governance signature. Proposal booleans, API fields, model output, environment variables, NLV, and 10K progress have no authority. Enforce Decimal boundaries: NORMAL 10.00 percent passes and 10.01 rejects; valid A-grade 15.00 passes; 15.01 through 20.00 inclusive and above reject.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_nav_ledger.py tests/options_copilot/test_proposal_gate.py</automated>
  </verify>
  <done>Risk authorization uses Strategy NAV only and exactly enforces NORMAL, A-grade, and absolute rejection boundaries.</done>
  <acceptance_criteria>
    - Tests cover 10.00, 10.01, 15.00, 15.01, 20.00, and 20.01 percent
    - Unsigned, wrong-phase, wrong-hash, rolled-back, and ordinary promotion markers keep A-grade false
    - NLV, rank, evidence score, model output, and campaign progress cannot change the result
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Cash-flow evidence to NAV ledger | Attributed flows determine the only capital base used by risk. |
| Governance marker to risk tier | A signed P9 decision is the sole A-grade authority. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P1-01-01 | Tampering | NAV flows | mitigate | Append-only identities, conflicts, head hash, and signed contract. |
| T-P1-01-02 | Elevation of Privilege | A-grade flag | mitigate | Strict marker verification outside proposal/API/model inputs. |
</threat_model>

<verification>
Run NAV, campaign, and proposal boundary tests with Decimal exactness.
</verification>

<success_criteria>
Strategy NAV is the sole risk base and no unsigned input can expand risk.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
