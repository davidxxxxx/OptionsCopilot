---
phase: P5-strategy-ranking
plan: "02"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/analytics/__init__.py
  - options_copilot/analytics/positioning_runtime.py
  - options_copilot/analytics/volatility.py
  - options_copilot/analytics/scenarios.py
  - options_copilot/learning_shadow.py
  - options_copilot/news/__init__.py
  - options_copilot/news/calendar.py
  - options_copilot/news/classifier.py
  - options_copilot/news/composition.py
  - options_copilot/news/deepseek.py
  - options_copilot/news/learning.py
  - options_copilot/news/models.py
  - options_copilot/news/preselection.py
  - options_copilot/news/reaction.py
  - options_copilot/news/scoring.py
  - options_copilot/news/service.py
  - tests/options_copilot/test_news_composition.py
  - tests/options_copilot/test_news_core.py
  - tests/options_copilot/test_news_deepseek.py
  - tests/options_copilot/test_news_reaction.py
  - tests/options_copilot/test_learning_shadow.py
  - tests/options_copilot/test_positioning_runtime.py
  - tests/options_copilot/test_scenarios.py
  - tests/options_copilot/test_volatility_scenarios.py
evidence_prefixes: []
autonomous: true
requirements: [OC-ANALYTICS-01, OC-EVENT-01, OC-STRAT-01, OC-RISK-01, OC-LEARN-01, OC-VAL-01]
must_haves:
  truths:
    - "Eligibility requires independent MARKET, VOLATILITY, and LIQUIDITY evidence plus all hard gates."
    - "News, earnings, Max Pain, walls, PCR, and estimated GEX remain capped SUPPORTING_ONLY evidence."
    - "Optional LLM output is schema-bound explanatory classification only; deterministic fallback, local IBKR evidence, signed cost, payoff, and hard gates remain authoritative."
    - "Phase-local Top-10 and Top-3 news pools remain bounded SUPPORTING_ONLY read models; the separate immutable Top-10 preselection ledger and post-event reaction state machine cannot create eligibility, approval, instructions, or orders."
    - "Positioning projection remains core-only; the durable shadow-learning ledger is later composed by P6 through read-only runtime/API/GUI surfaces and still cannot affect production eligibility, approval, promotion, instructions, or orders."
    - "Scenario probabilities and thresholds come from a verified versioned policy resolver; before any signed promotion, the immutable Initial Champion and Scenario Policy is the current fallback and source of truth."
    - "Every scenario result carries current_policy_version, current_policy_hash, and policy_authority_marker_hash so later promotion or rollback cannot reuse stale output."
  artifacts:
    - path: "options_copilot/analytics/volatility.py"
      provides: "Immutable volatility feature calculations"
    - path: "options_copilot/analytics/scenarios.py"
      provides: "Resolver-bound scenario probabilities, cost-adjusted EV, and initial-policy fallback"
      contains: "PolicyResolver"
    - path: "options_copilot/news/deepseek.py"
      provides: "Optional constrained explanatory-only DeepSeek classifier"
      contains: "DeepSeekNewsClassifier"
    - path: "options_copilot/news/service.py"
      provides: "Transparent supporting-only news scoring and bounded research pools"
      contains: "NewsAnalysisService"
    - path: "options_copilot/learning_shadow.py"
      provides: "Standalone append-only point-in-time Champion/Challenger research ledger"
      contains: "ShadowLearningLedger"
    - path: "options_copilot/news/reaction.py"
      provides: "Immutable hash-chained official-event reaction state machine"
      contains: "EventReactionLedger"
  key_links:
    - from: "options_copilot/analytics/scenarios.py"
      to: "options_copilot/governance/initial_champion_scenario_policy.v1.json"
      via: "verified initial fallback plus versioned current-policy resolution"
      pattern: "current_policy_hash"
---

<objective>
Implement immutable volatility, versioned current-policy scenarios, and explanatory-only news analytics with strict separation between hard and supporting evidence.

Purpose: Prevent event or positioning signals from manufacturing eligibility, keep probability assumptions human-governed, and make later signed promotion/rollback visible to every consumer.
Output: Volatility/scenario engines, a current-policy resolver port with immutable initial fallback, constrained news classification/scoring/preselection contracts, an authority-free post-event reaction state machine, and supporting-only/current-policy tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/analytics/positioning.py
@options_copilot/providers/events.py
@options_copilot/ranking/engine.py
@options_copilot/governance/initial_champion_scenario_policy.v1.json
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P5-02-01: Implement immutable volatility evidence</name>
  <read_first>
    - options_copilot/analytics/positioning.py
    - options_copilot/providers/events.py
    - options_copilot/domain/models.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/analytics/volatility.py, tests/options_copilot/test_scenarios.py</files>
  <action>Implement immutable IV percentile/rank, term structure, skew, expected move, and earnings IV-crush evidence with input/evidence/secdef/quote hashes and UTC timestamps. Label feature evidence by source class and role. News, earnings, max pain, walls, PCR, and estimated GEX are fixed SUPPORTING_ONLY. Volatility hard evidence requires executable, complete broker-derived options inputs rather than a supplemental news value. Conflicted or stale inputs are ineligible.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_positioning.py tests/options_copilot/test_scenarios.py</automated>
  </verify>
  <done>Volatility features are immutable, provenance-bound, and distinct from supporting evidence.</done>
  <acceptance_criteria>
    - Every output binds input and source hashes
    - Supporting evidence cannot be relabeled hard evidence
    - Conflicted, stale, missing, or non-executable inputs fail closed
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-02-02: Implement current-policy scenario engine and hard eligibility gates</name>
  <read_first>
    - options_copilot/analytics/volatility.py
    - options_copilot/analytics/positioning.py
    - options_copilot/providers/events.py
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
    - options_copilot/governance/execution_cost_contract.v1.json
  </read_first>
  <files>options_copilot/analytics/scenarios.py, tests/options_copilot/test_scenarios.py</files>
  <action>Define a `PolicyResolver` contract returning one immutable `ResolvedPolicy` with `current_policy_version`, `current_policy_hash`, `policy_authority_marker_hash`, effective time, verified probability methodology, calibration provenance, weights, supporting caps, and NO_TRADE thresholds. Implement the pre-P9 resolver as a verified immutable Initial Champion/Scenario Policy fallback; an absent, stale, tampered, ambiguous, or future-version authority must fail closed rather than silently selecting a policy. `ScenarioEngine` resolves once per decision, computes probabilities exactly from that resolution, requires probabilities to sum to one, and copies all three current-policy identity fields into every scenario/NO_TRADE output. Eligibility requires independent hash-bound MARKET, VOLATILITY, and LIQUIDITY evidence plus secdef, coherent quote, finite payoff, signed cost-adjusted EV, DTE, one-combination, Strategy NAV, and current risk-tier gates. Supporting evidence may change only a capped explanatory/ranking component and can never flip ineligible to eligible. Bind cost version/hash and the resolver result into scenario identity. This port is intentionally implemented with the immutable initial fallback here; P9 supplies the durable signed `CurrentPolicyResolver` without changing the scenario contract.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_scenarios.py</automated>
  </verify>
  <done>Scenario probabilities and eligibility are driven by one verified current-policy resolution, the initial policy remains the fallback/source of truth, and supporting evidence remains non-authoritative.</done>
  <acceptance_criteria>
    - News-only, earnings-only, combined events, positioning-only, and all-supporting fixtures remain NO_TRADE
    - Missing any MARKET/VOLATILITY/LIQUIDITY class remains NO_TRADE
    - Cost-adjusted EV includes all signed cost fields
    - Every output carries current_policy_version, current_policy_hash, and policy_authority_marker_hash
    - Changed policy, policy-authority marker, or cost hash changes result identity; stale/tampered resolver state returns NO_TRADE
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-02-03: Implement constrained explanatory-only news analysis</name>
  <read_first>
    - options_copilot/providers/events.py
    - options_copilot/providers/official.py
    - options_copilot/analytics/volatility.py
    - options_copilot/governance/execution_cost_contract.v1.json
    - trade_copilot/llm/deepseek.py
    - trade_copilot/llm/cost_meter.py
  </read_first>
  <files>options_copilot/analytics/__init__.py, options_copilot/news/__init__.py, options_copilot/news/calendar.py, options_copilot/news/classifier.py, options_copilot/news/composition.py, options_copilot/news/deepseek.py, options_copilot/news/learning.py, options_copilot/news/models.py, options_copilot/news/preselection.py, options_copilot/news/reaction.py, options_copilot/news/scoring.py, options_copilot/news/service.py, tests/options_copilot/test_news_composition.py, tests/options_copilot/test_news_core.py, tests/options_copilot/test_news_deepseek.py, tests/options_copilot/test_news_reaction.py, tests/options_copilot/test_volatility_scenarios.py</files>
  <action>Define immutable timezone-aware news, calendar, classification, market-confirmation, score, and conditional-option-preselection records. Keep three transparent numeric scores separate: event impact, IBKR-backed option tradability, and combined opportunity. Tradability and conditional option rows require fresh symbol-matched IBKR contract/quote/liquidity/payoff/cost fields; incomplete, conflicting, stale, non-finite, or source-orphaned inputs remain research-only/NO_TRADE. Build a deterministic classifier plus an explicitly enabled, independently cost-capped DeepSeek adapter whose JSON schema forbids invented symbols/evidence and all option legs, trades, approvals, instructions, and orders. Model/secret/transport/schema failure falls back to sanitized deterministic classification and cannot disable the research runtime. Produce bounded pre-market/open research pools with `decision_authority=SUPPORTING_ONLY`, `approval_eligible=false`, and `instruction_creation_allowed=false`. The phase-local `options_copilot.news.learning.ShadowLearningLedger` remains in-memory human-review scaffolding; the separate durable `learning_shadow.py` ledger is composed read-only in P6 but does not satisfy P8 promotion/rollback acceptance. Implement `news/reaction.py` as an immutable, hash-chained `SCHEDULED` through `OPTION_REEVALUATED` state machine with terminal DEGRADED/CONFLICTED/NO_TRADE states, strict event/evidence/time binding, and fixed SUPPORTING_ONLY authority. The displayed Top 10 remains distinct from the independent immutable preselection ledger implemented in P5-03.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_news_core.py tests/options_copilot/test_news_composition.py tests/options_copilot/test_news_deepseek.py tests/options_copilot/test_news_reaction.py tests/options_copilot/test_volatility_scenarios.py</automated>
  </verify>
  <done>News analysis is auditable and explanatory-only, with deterministic failover and zero eligibility, approval, instruction, or order authority.</done>
  <acceptance_criteria>
    - Structured model output cannot invent symbols/evidence or contain option legs, trades, approvals, instructions, or orders
    - Disabled/missing/failing model paths use bounded deterministic fallback without leaking exception or secret material
    - Separate numeric scores are stable; incomplete or stale IBKR hard data produces zero tradability and no action eligibility
    - Top-10/Top-3 pool labels never imply an independent ledger or approval surface
    - Shadow records never auto-promote and are not described as completed learning or outcomes
    - Reaction transitions are ordered, hash-bound, time-consistent, terminal on failure, and permanently SUPPORTING_ONLY
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-02-04: Implement positioning projection and durable shadow-learning ledger core</name>
  <read_first>
    - options_copilot/analytics/positioning.py
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/storage/canonical.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/analytics/positioning_runtime.py, options_copilot/learning_shadow.py, tests/options_copilot/test_learning_shadow.py, tests/options_copilot/test_positioning_runtime.py</files>
  <action>Implement two authority-free core modules. Keep `positioning_runtime.py` uncomposed from broker acquisition and all eligibility/approval/bridge/order paths; it wraps deterministic Max Pain, call/put walls, PCR, and estimated GEX in an immutable data-quality projection with oldest-source timestamp, age, quote/OI/Greeks coverage, delayed-data flags, explicit estimator assumptions/limitations, and fixed `SUPPORTING_ONLY` authority. Mixed chains, stale/future rows, duplicates, missing denominators, and incomplete fields degrade or become unavailable rather than being guessed. `learning_shadow.py` implements a local append-only SQLite/WAL/FULL Champion/Challenger research ledger with strict thesis/evidence/prediction/outcome chronology, exact point-in-time evidence hashes, immutable triggers, global hash-chain verification, deterministic as-of replay/similarity, and challenger-scoped independent-key counting. P6 may compose this ledger only through read-only runtime/API/GUI queries. Thirty independent resolved samples may change only the descriptive stage to `DISCOVERY`; operation remains `SHADOW_ONLY`, human promotion remains external, and no production weights/rules mutate. This does not complete P8 replay/evaluation or human promotion/rollback acceptance.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_positioning_runtime.py tests/options_copilot/test_learning_shadow.py</automated>
  </verify>
  <done>The positioning and shadow-learning cores are deterministic, auditable, and authority-free; later read-only learning composition cannot mutate production policy or authority.</done>
  <acceptance_criteria>
    - Positioning output always declares SUPPORTING_ONLY and its prohibited eligibility/approval/instruction/order uses
    - Missing/stale/delayed/partial positioning data is visible and cannot be presented as hard evidence
    - Shadow rows are append-only, hash-bound, point-in-time replayable, and tamper-detected across restart
    - Thirty independent resolved samples unlock DISCOVERY only and never production promotion
    - The ledger exposes no automatic promotion or production-mutation path; later composition is GET/read-model only
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Provider/market evidence to analytics | Supporting and hard evidence enter separate roles. |
| Signed policy to scenario probabilities | Human-frozen methodology controls probabilities and thresholds. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P5-02-01 | Elevation of Privilege | supporting evidence | mitigate | Fixed roles, caps, and negative eligibility tests. |
| T-P5-02-02 | Tampering | scenario weights | mitigate | Signed initial-policy version/hash. |
| T-P5-02-03 | Repudiation | probability output | mitigate | Calibration provenance and canonical input/result hashes. |
</threat_model>

<verification>
Run volatility/scenario tests including all supporting-only negative combinations.
</verification>

<success_criteria>
Only independent hard evidence and frozen policy can make a scenario eligible.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
