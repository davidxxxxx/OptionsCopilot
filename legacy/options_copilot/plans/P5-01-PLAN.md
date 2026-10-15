---
phase: P5-strategy-ranking
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/strategies/__init__.py
  - options_copilot/strategies/templates.py
  - options_copilot/strategies/generator.py
  - options_copilot/domain/models.py
  - tests/options_copilot/test_strategy_generator.py
  - tests/options_copilot/test_strategy_templates.py
evidence_prefixes: []
autonomous: true
requirements: [OC-SAFE-02, OC-STRAT-01, OC-EXIT-01, OC-TIME-01, OC-RISK-01, OC-VAL-01]
must_haves:
  truths:
    - "Entry generation uses only explicit finite-risk templates and independently recomputed payoff."
    - "Every strategy candidate binds coherent quotes, secdefs, Strategy NAV, signed cost contract, signed initial policy, and complete exit rules."
    - "GLD management remains outside the entry generator and a live GLD position leaves entry ineligible."
  artifacts:
    - path: "options_copilot/strategies/templates.py"
      provides: "Finite-risk strategy registry"
      contains: "StrategyTemplateRegistry"
    - path: "options_copilot/strategies/generator.py"
      provides: "Finalist-chain to validated strategy candidates"
      contains: "StrategyCandidateGenerator"
  key_links:
    - from: "options_copilot/strategies/generator.py"
      to: "options_copilot/risk/payoff.py"
      via: "independent local payoff and maximum-loss calculation"
      pattern: "max_loss"
---

<objective>
Generate only finite-risk, cost-bound, exit-complete strategy candidates from P4 finalists.

Purpose: Replace free-form strategy suggestions with deterministic templates and independent risk calculation.
Output: Strategy registry/generator, domain contracts, and template/property tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/domain/models.py
@options_copilot/risk/payoff.py
@options_copilot/risk/time_policy.py
@options_copilot/governance/execution_cost_contract.v1.json
@options_copilot/governance/initial_champion_scenario_policy.v1.json
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P5-01-01: Define finite-risk template registry and candidate contracts</name>
  <read_first>
    - options_copilot/domain/models.py
    - options_copilot/risk/payoff.py
    - options_copilot/proposals.py
    - options_copilot/governance/execution_cost_contract.v1.json
  </read_first>
  <files>options_copilot/strategies/__init__.py, options_copilot/strategies/templates.py, options_copilot/domain/models.py, tests/options_copilot/test_strategy_generator.py, tests/options_copilot/test_strategy_templates.py</files>
  <action>Register DEBIT_VERTICAL, CREDIT_VERTICAL, CALENDAR, DIAGONAL, BUTTERFLY, IRON_CONDOR, and LONG_OPTION. Templates output complete conId-bound legs and integer ratios, scenario applicability, assignment/exercise/dividend flags, and exit constraints; they never accept free-text legs. Permanently reject naked calls/puts, uncovered ratio shorts, adjusted or unknown contracts, and payoff whose maximum loss is not exactly computable. Define immutable strategy candidate and ExitPlan contracts with thesis invalidation, risk stop, profit take, time stop, maximum holding date, and bad-quote action.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_strategy_generator.py tests/options_copilot/test_strategy_templates.py tests/options_copilot/test_domain_risk_ranking.py</automated>
  </verify>
  <done>All registered structures have exact finite payoff and complete exit metadata.</done>
  <acceptance_criteria>
    - Each template has finite max loss under property tests
    - Uncovered/unknown/adjusted structures always reject
    - ExitPlan fields are mandatory and hash-bound
    - LONG_OPTION remains an exception after combination candidates, not the default
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-01-02: Generate independently validated cost-bound candidates</name>
  <read_first>
    - options_copilot/strategies/templates.py
    - options_copilot/risk/payoff.py
    - options_copilot/risk/time_policy.py
    - options_copilot/proposals.py
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
  </read_first>
  <files>options_copilot/strategies/generator.py, tests/options_copilot/test_strategy_generator.py</files>
  <action>Generate from authoritative finalist chain references, standard secdefs, and coherent executable quote batches. Use ask for buys and bid for sells, then apply the signed Execution Cost Contract commission, fees, spread/slippage, assignment, early-exercise, and ex-dividend assumptions. Independently recompute debit/credit, maximum loss/profit, breakevens, liquidity, and exit plan through local validators. Bind Strategy NAV, calendar/DTE, broker snapshot, evidence, cost contract, and initial policy version/hash. Normal entry is 14-35 DTE; signed exception may allow 7-13; below 7 rejects. If GLD is open or any binding is missing, return structured NO_TRADE.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_strategy_generator.py tests/options_copilot/test_proposal_gate.py</automated>
  </verify>
  <done>Every entry candidate is locally recomputed, cost/policy bound, finite risk, and DTE compliant.</done>
  <acceptance_criteria>
    - Candidate hash changes for any leg, quote, cost, policy, NAV, calendar, evidence, or exit change
    - Cost contract absence/mismatch creates no candidate or approval
    - DTE 13/14/35/36 and exception 6/7 are exact
    - GLD-open fixture emits zero entry candidates
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Finalist chain to template generator | External contract data becomes a finite-risk structure only after local validation. |
| Signed cost/policy to candidate | Human-approved assumptions become immutable candidate inputs. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P5-01-01 | Tampering | candidate payoff | mitigate | Independent local payoff and hash binding. |
| T-P5-01-02 | Elevation of Privilege | free-form legs | mitigate | Closed template registry and no text-defined structure. |
| T-P5-01-03 | Repudiation | execution cost | mitigate | Signed cost contract version/hash in candidate identity. |
</threat_model>

<verification>
Run template, payoff, DTE, cost-binding, and GLD-open property tests.
</verification>

<success_criteria>
Only finite-risk, independently validated, fully bound entry candidates leave the generator.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
