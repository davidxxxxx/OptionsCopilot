---
phase: P6-runtime-gui
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/api/__init__.py
  - options_copilot/runtime.py
  - options_copilot/api/app.py
  - options_copilot/approval/__init__.py
  - options_copilot/approval/proofs.py
  - options_copilot/bridge/__init__.py
  - options_copilot/bridge/store.py
  - options_copilot/config.py
  - options_copilot/decision/pipeline.py
  - options_copilot/gateway/__init__.py
  - options_copilot/gateway/ibkr_readonly.py
  - options_copilot/market/__init__.py
  - options_copilot/market/session_calendar.py
  - options_copilot/news_runtime.py
  - options_copilot/production_runtime.py
  - options_copilot/ranking/__init__.py
  - options_copilot/ranking/basis.py
  - options_copilot/ranking/evidence_manifest.py
  - options_copilot/ranking/portfolio.py
  - options_copilot/ranking/store.py
  - options_copilot/risk/__init__.py
  - options_copilot/risk/resolver.py
  - options_copilot/approval/store.py
  - tests/options_copilot/test_approval_store.py
  - tests/options_copilot/test_learning_runtime_api.py
  - tests/options_copilot/test_news_preselection_runtime.py
  - tests/options_copilot/test_news_runtime.py
  - tests/options_copilot/test_news_runtime_official_calendar.py
  - tests/options_copilot/test_news_runtime_reaction.py
  - tests/options_copilot/test_production_candidate_e2e.py
  - tests/options_copilot/test_production_runtime.py
  - tests/options_copilot/test_risk_authority_resolver.py
  - tests/options_copilot/test_runtime.py
  - tests/options_copilot/test_runtime_pipeline.py
  - tests/options_copilot/test_runtime_wiring_completeness.py
  - tests/options_copilot/test_api_frontend.py
  - tests/options_copilot/test_ranking_store.py
evidence_prefixes: []
autonomous: true
requirements: [OC-SCAN-01, OC-STRAT-01, OC-EVENT-01, OC-EXIT-01, OC-BRIDGE-01, OC-RISK-01, OC-LEARN-01, OC-SEC-01, OC-VAL-01]
must_haves:
  truths:
    - "RuntimeServices constructor includes every data, evidence, strategy, analytics, cost, policy, gate, ranking, management, approval, and bridge-status dependency."
    - "Runtime consumes immutable DecisionPipeline and RankingStore records rather than external candidate JSON."
    - "API authorization transactionally rereads current frozen rank 1; ranks 2-3 and stale/tampered/concurrent snapshots reject."
    - "Decision, scenario, ranking, and approval records carry the same current policy and risk-authority identities; a promotion or rollback makes prior records stale immediately."
    - "Production composition owns one read-only IBKR gateway and cannot connect until the signed pacing authority is current; Creator remains unavailable and direct order submission is always false."
    - "News runtime, official calendar, post-event reactions, optional LLM classification, and ledger-backed option preselections remain SUPPORTING_ONLY research; they never create approvals or instructions."
    - "Runtime owns durable shadow_learning.sqlite3 and news_preselection.sqlite3 stores; restart, integrity, lineage, and coverage are explicit while every learning endpoint is GET-only observation authority."
    - "The open-repricing observation store is implemented, but its production producer remains UNAVAILABLE pending a complete frozen eight-field IBKR identity writer."
    - "The GLD production adapter reuses the P2 proof core but remains NO_TRADE whenever the authoritative Exit Contract or any broker/contract binding is unavailable."
  artifacts:
    - path: "options_copilot/runtime.py"
      provides: "Fully injected runtime services, immutable read models, and owned durable shadow-learning/Top-10 stores"
      contains: "RuntimeServices"
    - path: "options_copilot/api/app.py"
      provides: "Scan/evidence/ranking/management APIs, GET-only learning queries, and rank-one challenge gate"
    - path: "options_copilot/ranking/store.py"
      provides: "Immutable ranking-basis/final-row chain with current-policy and risk-authority hashes"
    - path: "options_copilot/approval/store.py"
      provides: "Approval rows bound to current policy and risk-tier authority"
    - path: "options_copilot/production_runtime.py"
      provides: "Pacing-gated, read-only IBKR production adapters and lifecycle"
      contains: "ProductionLifecycle"
    - path: "options_copilot/news_runtime.py"
      provides: "Point-in-time news/calendar, reaction-provider, and ledger-backed preselection read models"
      contains: "NewsCoordinator"
  key_links:
    - from: "options_copilot/runtime.py"
      to: "options_copilot/decision/pipeline.py"
      via: "immutable scan/ranking result consumption"
      pattern: "DecisionPipeline"
    - from: "options_copilot/api/app.py"
      to: "options_copilot/ranking/store.py"
      via: "transactional rank-one authorization reread"
      pattern: "authorize_frozen_rank_one"
    - from: "options_copilot/decision/pipeline.py"
      to: "options_copilot/analytics/scenarios.py"
      via: "one resolved current policy shared by scenario and ranker"
      pattern: "current_policy_hash"
    - from: "options_copilot/ranking/store.py"
      to: "options_copilot/approval/store.py"
      via: "ranking-basis, policy-authority, and risk-authority hash propagation"
      pattern: "risk_authority_marker_hash"
    - from: "options_copilot/runtime.py"
      to: "options_copilot/learning_shadow.py"
      via: "read-only durable shadow-learning services"
      pattern: "shadow_learning.sqlite3"
    - from: "options_copilot/runtime.py"
      to: "options_copilot/news/preselection_store.py"
      via: "owned independent Top-10 store and provider"
      pattern: "news_preselection.sqlite3"
    - from: "options_copilot/news_runtime.py"
      to: "options_copilot/news/reaction.py"
      via: "scoped official-event reaction provider and verified compact read model"
      pattern: "EventReactionProvider"
---

<objective>
Replace snapshot-only runtime behavior with the current-authority-aware DecisionPipeline, read-only production/news/learning composition, and immutable ranking, management, and independent preselection stores.

Purpose: Make server-side truth authoritative, carry signed policy/risk authority to approval, and close every direct API rank bypass.
Output: Pipeline/ranker/ledger contracts, pacing-gated production runtime, point-in-time news/calendar/reaction runtime, durable shadow-learning and Top-10 store composition, read-only API integration, and promotion/rollback/rank/staleness tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/runtime.py
@options_copilot/api/app.py
@options_copilot/decision/pipeline.py
@options_copilot/analytics/scenarios.py
@options_copilot/ranking/portfolio.py
@options_copilot/ranking/store.py
@options_copilot/positions/manager.py
@options_copilot/risk/authorization.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P6-01-01: Bind current policy and risk authority through pipeline and ranking</name>
  <read_first>
    - options_copilot/decision/pipeline.py
    - options_copilot/analytics/scenarios.py
    - options_copilot/ranking/portfolio.py
    - options_copilot/ranking/store.py
    - options_copilot/risk/authorization.py
    - options_copilot/risk/resolver.py
  </read_first>
  <files>options_copilot/decision/pipeline.py, options_copilot/ranking/__init__.py, options_copilot/ranking/basis.py, options_copilot/ranking/evidence_manifest.py, options_copilot/ranking/portfolio.py, options_copilot/ranking/store.py, options_copilot/risk/__init__.py, options_copilot/risk/resolver.py, tests/options_copilot/test_risk_authority_resolver.py, tests/options_copilot/test_runtime_pipeline.py, tests/options_copilot/test_ranking_store.py</files>
  <action>Change `DecisionPipeline` to resolve `PolicyResolver` and the versioned `RiskTierAuthority` resolver exactly once under the decision boundary, pass the same immutable resolutions into `ScenarioEngine` and `PortfolioRanker`, and reject disagreement or a head change before persistence. `CurrentRiskAuthorityResolver` may read only the explicitly injected current marker source; environment, model, proposal, candidate, rank, campaign, or news state cannot manufacture authority. `PortfolioRanker` must hard-filter before scoring and expose a canonical `ranking_basis_hash` over proposal/candidate, current policy version/hash, policy-authority marker hash, signed cost version/hash, risk-contract hash, and all evidence inputs. The candidate-evidence manifest must keep primary IBKR/local deterministic proof separate from capped supporting news/calendar evidence. This pre-authority basis avoids a self-hash cycle: a P9 A-grade marker may bind the basis, then the final immutable ranking row includes that signed marker hash and receives its own row/snapshot hash. Without a current A-grade marker, 10.01-15.00 can exist only as non-authorizable `A_GRADE_PENDING` governance evidence and never in the approvable Top 3. Persist every scenario, NO_TRADE, ranking-basis, and final ranking output with `current_policy_version`, `current_policy_hash`, `policy_authority_marker_hash`, `risk_authority_version`, and non-null `risk_authority_marker_hash`; for A-grade the latter is exactly the signed P9 A-grade marker content hash, while NORMAL uses the canonical versioned NORMAL authority-state hash bound to the immutable risk contract. Any resolver promotion/rollback, marker revocation, stale/tampered head, or concurrent authority change rejects finalization and leaves no approvable row.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_runtime_pipeline.py tests/options_copilot/test_risk_authority_resolver.py tests/options_copilot/test_ranking_store.py tests/options_copilot/test_portfolio_ranking.py</automated>
  </verify>
  <done>Pipeline, scenarios, ranker, and ranking ledger share one current policy/risk authority resolution and cannot finalize stale authority.</done>
  <acceptance_criteria>
    - Every output carries identical current-policy and risk-authority identities
    - Ranking basis precedes human A-grade signing; final row hash includes the resulting marker without circular hashing
    - NORMAL 10.00 can rank, while unsigned 10.01-15.00 is non-authorizable A_GRADE_PENDING
    - Promotion, rollback, revocation, tamper, restart, and concurrent head-change fixtures reject stale finalization
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P6-01-02: Inject the complete resolver-aware RuntimeServices graph</name>
  <read_first>
    - options_copilot/runtime.py
    - options_copilot/decision/pipeline.py
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/storage/evidence.py
    - options_copilot/ranking/store.py
    - options_copilot/positions/manager.py
  </read_first>
  <files>options_copilot/config.py, options_copilot/runtime.py, tests/options_copilot/test_learning_runtime_api.py, tests/options_copilot/test_runtime.py, tests/options_copilot/test_runtime_pipeline.py, tests/options_copilot/test_runtime_wiring_completeness.py</files>
  <action>Define `RuntimeServices` with explicit constructor fields for BrokerSnapshotBuilder, EvidenceStore, ScanRunStore, UniverseFunnel, broker/options evidence acquisition, StrategyTemplateRegistry, StrategyCandidateGenerator, VolatilityEngine, ScenarioEngine, versioned current `PolicyResolver`, versioned current `RiskTierAuthority` resolver, ExecutionCostContract verifier, hard eligibility/risk/DTE/single-combination gates, PortfolioRanker, RankingStore, DecisionPipeline, PositionManager, ApprovalStore, and bridge status/reconciliation reader. Configuration keeps all owned databases/evidence/cost state on G:, locks the bounded news cadence/core universe/model enablement, and cannot enable live instruction or loosen quote/risk gates through environment variables. Before P9, instantiate only the verified immutable initial-policy fallback and canonical NORMAL authority state; the ports must accept P9's durable signed resolvers without another consumer rewrite. Remove external candidate JSON as decision authority. Runtime owns `shadow_learning.sqlite3`, verifies its append-only chain at startup/restart, keeps independent counts challenger-scoped, and exposes summary/record/replay/similarity providers only as `SHADOW_ONLY` read models with automatic promotion, production-weight mutation, approval, bridge, and order authority false. Read models reference immutable scan, evidence, broker, current policy/policy-authority, cost, risk-authority, ranking, transition, and exit hashes. Missing, placeholder, orphaned, or incapable dependencies, invalid contracts, Creator unavailability, or resolver disagreement reports DEGRADED/NO_TRADE and disables approval without disabling safe research.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_learning_runtime_api.py tests/options_copilot/test_runtime_pipeline.py tests/options_copilot/test_runtime_wiring_completeness.py tests/options_copilot/test_runtime.py tests/options_copilot/test_config_security.py</automated>
  </verify>
  <done>Runtime is fully injected, consumes the named resolver-aware pipeline, and has no implicit fallback dependency.</done>
  <acceptance_criteria>
    - Constructor test names every listed dependency, including both resolvers
    - Missing any required dependency disables approval
    - Creator unavailability disables approval while preserving read-only research and direct_order_submission=false
    - Runtime read models contain policy/cost/risk-authority/ranking/snapshot/evidence hashes
    - External candidate JSON cannot become authorization truth
    - Shadow learning survives restart, never combines independent samples across challengers, and cannot mutate production policy or authority
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P6-01-03: Compose pacing-gated production and news runtimes</name>
  <read_first>
    - options_copilot/config.py
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/market/session_calendar.py
    - options_copilot/news_runtime.py
    - options_copilot/news/preselection_store.py
    - options_copilot/news/reaction.py
    - options_copilot/positions/runtime_adapter.py
    - options_copilot/providers/official_sources.py
    - options_copilot/operations/pacing_authority.py
    - options_copilot/runtime.py
  </read_first>
  <files>options_copilot/gateway/__init__.py, options_copilot/gateway/ibkr_readonly.py, options_copilot/market/__init__.py, options_copilot/market/session_calendar.py, options_copilot/news_runtime.py, options_copilot/production_runtime.py, options_copilot/runtime.py, tests/options_copilot/test_news_preselection_runtime.py, tests/options_copilot/test_news_runtime.py, tests/options_copilot/test_news_runtime_official_calendar.py, tests/options_copilot/test_news_runtime_reaction.py, tests/options_copilot/test_production_runtime.py, tests/options_copilot/test_runtime_wiring_completeness.py</files>
  <action>Build one owned production composition around a shared `IBKRReadOnlyGateway`, atomic snapshot builder, broker-published session calendar, two-hour scanner loop, strict signed-pacing guard, local deterministic payoff/cost/risk gates, P2 `ProductionManagementCoordinator`, and `NewsCoordinator`. `ProductionLifecycle.start()` is the only read-only connection boundary; missing/stale/future/tampered/ambiguous pacing authority prevents connection and scanner start. Underlying quotes may select nearby strikes but never replace executable per-leg option quotes. The news runtime persists append-only point-in-time provider/official-calendar evidence, retains conflicts and four timestamps, refreshes on bounded cadence, rebuilds safely after restart, and projects conditional option research only when exact fresh IBKR bindings exist. Runtime owns and verifies `news_preselection.sqlite3`, injects its `LedgerBackedPreselectionProvider`, binds every projected row one-to-one to immutable run/head/row lineage, restores the same lineage after restart, reports exact Top-10 coverage, and clears all pools on corruption or binding mismatch. The store supports typed open observations, but production has no open-reprice producer: coverage must remain `OPEN_REPRICE_PRODUCER_UNAVAILABLE` with `PENDING_FULL_IBKR_8_FIELD_IDENTITY_FREEZE` until that writer is separately implemented. Official calendar rows receive only a verified compact post-event reaction projection from the scoped `EventReactionProvider`; missing, malformed, duplicate, tampered, or misbound ledgers preserve calendar facts but become UNAVAILABLE/CONFLICTED and `NO_TRADE`. No production reaction provider is inferred merely because the state machine/seam exists. Official-source failure keeps valid rows visible but marks the envelope DEGRADED/NO_TRADE. Optional LLM output remains explanatory SUPPORTING_ONLY and fail-safe. Production must not compose Jin10 merely because a secret exists; Jin10 remains disabled until the human revocation/replacement checkpoint and a verified transport contract are complete. Production must also set `CREATOR_TRANSPORT_UNAVAILABLE`, `review_only=true`, and `direct_order_submission=false`; no approval, instruction, connector, or order primitive may be added. The P2 management adapter is composed, but an unavailable authoritative Exit Contract keeps its current production read model NO_TRADE.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_production_runtime.py tests/options_copilot/test_news_runtime.py tests/options_copilot/test_news_runtime_official_calendar.py tests/options_copilot/test_news_runtime_reaction.py tests/options_copilot/test_news_preselection_runtime.py tests/options_copilot/test_runtime_wiring_completeness.py tests/options_copilot/test_position_management_runtime_adapter.py tests/options_copilot/test_pacing_authority.py</automated>
  </verify>
  <done>The production/news graph starts only behind signed pacing, remains read-only and Creator-unavailable, and fails incomplete broker, provider, calendar, model, or management authority to NO_TRADE.</done>
  <acceptance_criteria>
    - Invalid pacing authority causes zero gateway connections and zero scanner starts
    - Start/close are idempotent and every owned store/subscription returns to its baseline
    - News/calendar evidence survives restart with provenance/conflicts and cannot create approval or instruction state
    - Stale or incomplete IBKR option bindings empty the action pool without hiding research evidence
    - Jin10 is not called or advertised as production-wired solely from a configured secret
    - Creator unavailable and missing GLD Exit Contract remain explicit approval/management blockers with no order submission
    - Independent Top-10 lineage/coverage and post-event reaction state are durable/readable without gaining eligibility or authorization authority
    - Open-repricing producer, live reaction provider, P8 evaluation, and human promotion/rollback are not represented as complete
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P6-01-04: Implement immutable scan, evidence, rank-one, and approval authority gates</name>
  <read_first>
    - options_copilot/runtime.py
    - options_copilot/api/app.py
    - options_copilot/ranking/store.py
    - options_copilot/approval/store.py
    - options_copilot/bridge/store.py
  </read_first>
  <files>options_copilot/api/__init__.py, options_copilot/api/app.py, options_copilot/approval/__init__.py, options_copilot/approval/proofs.py, options_copilot/approval/store.py, options_copilot/bridge/__init__.py, options_copilot/bridge/store.py, options_copilot/runtime.py, tests/options_copilot/test_approval_store.py, tests/options_copilot/test_learning_runtime_api.py, tests/options_copilot/test_production_candidate_e2e.py, tests/options_copilot/test_runtime_pipeline.py, tests/options_copilot/test_api_frontend.py</files>
  <action>Add scan-latest, candidate-evidence, ranking, management, and challenge read/action endpoints. Add only GET `/api/learning`, record/detail, replay, and similarity queries over the durable shadow ledger; force every response to read-only observation authority with automatic production promotion, production weight/rule mutation, A-grade unlock, approval, bridge, and order authority false. Challenge request supplies identifiers only; server transactionally calls `RankingStore.authorize_frozen_rank_one`, re-resolves the current policy and risk-tier authority, verifies current head/expiry plus every immutable hash, and validates strict canonical broker and Strategy NAV proof schemas before creating a five-minute double-confirm challenge. Persist the ranking basis/final snapshot/evidence-manifest hashes, `current_policy_version/hash`, `policy_authority_marker_hash`, cost version/hash, `risk_authority_version`, and `risk_authority_marker_hash` in the challenge and approved intent; never trust request-body copies. Ignore request-body rank. Ranks 2-3 and A_GRADE_PENDING have read models only and no challenge/approve/reserve transition. Candidate swap, direct POST, stale ranking, newer scan, broker state change, cost/policy/risk-authority correction, promotion, rollback, A-grade revocation, transition change, missing/relabeled primary evidence, chain tamper, or concurrent supersession returns fixed 403/409 and adds no approval/bridge row. Confirmation consumes the persisted challenge once and may create only a local review-only handoff row; the production composition's Creator-unavailable blocker prevents this path from becoming an external instruction or order. Management remains a separate endpoint and proof path.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_runtime_pipeline.py tests/options_copilot/test_production_candidate_e2e.py tests/options_copilot/test_approval_store.py tests/options_copilot/test_learning_runtime_api.py tests/options_copilot/test_api_frontend.py tests/options_copilot/test_ranking_store.py</automated>
  </verify>
  <done>Server-side immutable ledger plus current policy/risk authority truth is the only rank-one authorization path.</done>
  <acceptance_criteria>
    - Rank 1 valid path can create a challenge
    - Rank 2/3, A_GRADE_PENDING, forged rank, swap, direct bypass, stale, tampered, revoked, rolled-back, and concurrent fixtures reject
    - Rejections add zero approval and bridge rows
    - Learning routes are GET-only and every projection remains observation-only with all production/approval/bridge/order authority false
    - Challenge and approved intent copy every current-policy/cost/risk-authority hash from the transactionally reread row
    - Strict broker/NAV/evidence proofs reject missing, extra, stale, tampered, or relabeled fields before challenge consumption
    - API outputs no provider secret, bridge token, or managed auth material
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Decision stores to runtime | Immutable scan/ranking/management records become user read models. |
| API request to approval | Untrusted identifiers are revalidated against current ledger truth. |
| Provider/calendar/model inputs to news runtime | Untrusted and optional supporting evidence must never become hard eligibility or approval authority. |
| Signed pacing artifact to production lifecycle | Read-only IBKR connection and scanner start remain closed until the exact current human authority validates. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P6-01-01 | Elevation of Privilege | rank request | mitigate | Ignore request rank and transactionally reread current rank 1. |
| T-P6-01-02 | Tampering | stale challenge | mitigate | Bind current head plus all input hashes and expiry. |
| T-P6-01-03 | Information Disclosure | API payload | mitigate | Secret/auth field rejection and sanitized read models. |
| T-P6-01-04 | Elevation of Privilege | news/model score | mitigate | SUPPORTING_ONLY labels, schema restrictions, independent IBKR hard gates, and no news approval handler. |
| T-P6-01-05 | Denial of Service | broker pacing | mitigate | Fail-closed signed pacing guard before connection plus bounded request budgets. |
</threat_model>

<verification>
Run production lifecycle, news/calendar, runtime, API, ranking, bypass, staleness, tamper, and concurrency tests.
</verification>

<success_criteria>
Runtime exposes the named read-only decision/news/reaction/shadow-learning components; only current frozen rank 1 or valid P2 management proof can reach the local challenge boundary, while the production composition remains Creator-unavailable and has no instruction or order submission authority. The independent Top-10 ledger, reaction state machine, and durable shadow-learning query surface are implemented as non-authoritative evidence. Open-repricing production, a live reaction provider, P8 evaluation/replay acceptance, human promotion/rollback, and production Jin10 remain pending.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
