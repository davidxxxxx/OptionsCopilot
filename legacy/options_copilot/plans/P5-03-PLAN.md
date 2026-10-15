---
phase: P5-strategy-ranking
plan: "03"
type: execute
wave: 2
depends_on: ["P5-01", "P5-02"]
files_modified:
  - options_copilot/news/preselection_store.py
  - options_copilot/ranking/__init__.py
  - options_copilot/ranking/basis.py
  - options_copilot/ranking/evidence_manifest.py
  - options_copilot/ranking/portfolio.py
  - options_copilot/ranking/store.py
  - options_copilot/ranking/engine.py
  - tests/options_copilot/test_portfolio_ranking.py
  - tests/options_copilot/test_news_preselection_store.py
  - tests/options_copilot/test_ranking_store.py
evidence_prefixes: []
autonomous: true
requirements: [OC-STRAT-01, OC-RISK-01, OC-ANALYTICS-01, OC-BRIDGE-01, OC-VAL-01]
must_haves:
  truths:
    - "PortfolioRanker hard-filters first, applies signed policy and cost model, then stores at most three diverse candidates."
    - "Every ranking snapshot is immutable SQLite/WAL evidence with a versioned hash chain."
    - "Authorization transactionally rereads frozen rank 1 and rejects ranks 2-3, stale, tampered, or concurrently superseded snapshots."
    - "The independent immutable Top-10 news-preselection ledger is a separate SUPPORTING_ONLY store and can never become Top-3/rank-one authorization truth."
  artifacts:
    - path: "options_copilot/ranking/store.py"
      provides: "Immutable Top-3 ranking ledger and rank-one authorization transaction"
      contains: "RankingStore"
    - path: "options_copilot/ranking/portfolio.py"
      provides: "Hard-gated diverse PortfolioRanker"
      contains: "PortfolioRanker"
    - path: "options_copilot/ranking/evidence_manifest.py"
      provides: "Strict primary-versus-supporting candidate evidence manifest"
      contains: "resolve_candidate_evidence_manifest"
    - path: "options_copilot/news/preselection_store.py"
      provides: "Independent append-only Top-10 premarket ledger and lineage-bound open observations"
      contains: "NewsPreselectionStore"
  key_links:
    - from: "options_copilot/ranking/store.py"
      to: "options_copilot/ranking/portfolio.py"
      via: "immutable rank rows and candidate hashes"
      pattern: "ranking_snapshot_id"
    - from: "options_copilot/news/preselection_store.py"
      to: "options_copilot/news/preselection.py"
      via: "frozen strategy/leg identity and supporting-only projection"
      pattern: "strategy_structure_hash"
---

<objective>
Implement deterministic Top-3 ranking and its rank-one authority ledger, plus a strictly separate immutable supporting-only Top-10 news-preselection ledger.

Purpose: Prevent GUI/API rank spoofing, stale authorization, and mutable ranking evidence.
Output: PortfolioRanker, SQLite/WAL ranking store, authorization transaction, independent Top-10 preselection store, and tamper/concurrency tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/strategies/generator.py
@options_copilot/analytics/scenarios.py
@options_copilot/ranking/engine.py
@options_copilot/approval/store.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P5-03-01: Implement hard-gated diverse PortfolioRanker</name>
  <read_first>
    - options_copilot/strategies/generator.py
    - options_copilot/analytics/scenarios.py
    - options_copilot/ranking/engine.py
    - options_copilot/risk/policy.py
  </read_first>
  <files>options_copilot/ranking/__init__.py, options_copilot/ranking/basis.py, options_copilot/ranking/portfolio.py, options_copilot/ranking/engine.py, tests/options_copilot/test_portfolio_ranking.py</files>
  <action>Implement `PortfolioRanker.rank(candidates, limit=3)`. First enforce every hard gate, then use signed initial-policy score components, signed cost-adjusted EV, liquidity, exact max loss, strategy priority, diversity, and stable candidate ID. Build the proposal/candidate/evidence ranking basis through one canonical pure helper so pipeline, ranker, approval, and durable store share byte-for-byte hashes. Do not let supporting evidence, NLV, campaign progress, or rank alter risk eligibility. Diversify repeated underlying/thesis/structure. Return structured NO_TRADE reasons when empty. Persist full score components and input/policy/cost hashes for storage.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_portfolio_ranking.py tests/options_copilot/test_proposal_gate.py</automated>
  </verify>
  <done>Ranking is deterministic, hard-gated, policy/cost bound, and returns at most three diverse candidates.</done>
  <acceptance_criteria>
    - At most three unique candidate IDs are returned
    - Stable tie-break produces identical output for reordered inputs
    - Empty eligibility returns fixed NO_TRADE reasons
    - Risk boundaries remain exact and independent of score/rank/supporting evidence
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-03-02: Persist immutable ranking snapshots and hash chain</name>
  <read_first>
    - options_copilot/ranking/portfolio.py
    - options_copilot/storage/ledger.py
    - options_copilot/approval/store.py
    - options_copilot/bridge/store.py
  </read_first>
  <files>options_copilot/ranking/evidence_manifest.py, options_copilot/ranking/store.py, tests/options_copilot/test_ranking_store.py</files>
  <action>Create a dedicated SQLite/WAL/FULL/foreign-key ranking ledger. Persist `scan_run_id`, `ranking_snapshot_id`, input hash, evidence hash, broker snapshot hash, initial policy version/hash, execution cost version/hash, candidate hashes, ranks 1-3, score-component canonical JSON/hash, created_at, valid_until, schema version, previous snapshot hash, and row/snapshot hash. Embed one strict candidate-evidence manifest that separates allowlisted primary IBKR/local-deterministic proof rows from capped supporting rows and binds cutoff, symbol, candidate, and manifest hashes. Rows are append-only and one snapshot has unique ranks and candidates. Directed migrations reject unknown versions. Missing primary evidence, supporting-as-primary relabeling, tampered manifests/rows, or broken chain fail closed.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_ranking_store.py</automated>
  </verify>
  <done>Top-3 rankings are immutable, versioned, hash-chained, and recoverable across restart.</done>
  <acceptance_criteria>
    - Schema contains every named identity/hash/time/rank field
    - WAL/FULL/foreign-key settings are read back and tested
    - Tampering any rank, candidate, score, contract hash, timestamp, or chain field is detected
    - Restart and concurrent append preserve one valid head
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-03-03: Add transactional rank-one authorization reread</name>
  <read_first>
    - options_copilot/ranking/store.py
    - options_copilot/approval/store.py
    - options_copilot/bridge/store.py
    - tests/options_copilot/test_bridge_coordinator.py
  </read_first>
  <files>options_copilot/ranking/store.py, tests/options_copilot/test_ranking_store.py, tests/options_copilot/test_portfolio_ranking.py</files>
  <action>Add `authorize_frozen_rank_one(ranking_snapshot_id, candidate_id, expected_hashes, now)` under an immediate transaction. Reread and verify chain/head, expiry, exact snapshot/input/evidence/broker/policy/cost hashes, candidate hash, and `rank=1`; then return a frozen authorization record tied to that row. Rank 2/3, request-body rank, candidate swap, stale snapshot, chain tamper, concurrent supersession, or a newer ranking head all reject with fixed reasons and no approval/bridge mutation.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_ranking_store.py tests/options_copilot/test_portfolio_ranking.py</automated>
  </verify>
  <done>Only the transactionally reread current rank-one row can become an authorization input.</done>
  <acceptance_criteria>
    - Rank 2 and 3 always reject even when request claims rank 1
    - Expired, tampered, and concurrently superseded snapshots reject
    - Successful record binds every expected hash and the ledger row hash
    - Rejected calls add no approval or bridge row
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P5-03-04: Persist the independent supporting-only Top-10 preselection ledger</name>
  <read_first>
    - options_copilot/news/preselection.py
    - options_copilot/storage/canonical.py
    - options_copilot/ranking/store.py
  </read_first>
  <files>options_copilot/news/preselection_store.py, tests/options_copilot/test_news_preselection_store.py</files>
  <action>Create `NewsPreselectionStore` at the runtime-owned `news_preselection.sqlite3` path as a dedicated SQLite/WAL/FULL/foreign-key append-only ledger. A premarket run requests ten rows, accepts at most ten unique complete `PRE_MARKET` structures, records available-versus-requested coverage, and binds every row into a global hash chain with update/delete triggers and restart verification. Open observations must bind the exact premarket run/head/row and freeze underlying, strategy type, conId, expiry, strike, right, side, ratio, and quantity; only fresh quote/economics/evidence fields may change. Stale or incomplete observations remain audited with blockers and cannot enter the action-observation pool. `LedgerBackedPreselectionProvider` exposes rows, redacted lineage, and explicit coverage with fixed `SUPPORTING_ONLY`, approval-false, instruction-false, and order-false authority. Keep `open_reprice_producer_status=UNAVAILABLE` and `open_reprice_writer=PENDING_FULL_IBKR_8_FIELD_IDENTITY_FREEZE` until a real producer is separately implemented; tests may append typed observations but do not prove that producer exists. This ledger never feeds Top-3 authorization.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_news_preselection_store.py</automated>
  </verify>
  <done>The independent Top-10 ledger is durable, restart-safe, lineage-bound, and non-authoritative while missing open-reprice production remains explicit.</done>
  <acceptance_criteria>
    - WAL/FULL/foreign-key settings, append-only triggers, hash-chain integrity, replay, and restart are tested
    - Premarket runs reject more than ten, duplicates, incomplete structure identity, and wrong phase
    - Open observations reject every frozen identity/leg drift and wrong-parent lineage
    - Coverage distinguishes no run, incomplete Top-10 coverage, and unavailable open-reprice producer
    - Every projection remains SUPPORTING_ONLY with all approval/instruction/order flags false
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Ranked candidates to immutable store | Computed scores become authorization evidence only after durable append. |
| API request to rank-one transaction | Untrusted candidate/rank claims are ignored in favor of ledger truth. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P5-03-01 | Tampering | ranking ledger | mitigate | Hash chain, immutable rows, and transactional verification. |
| T-P5-03-02 | Elevation of Privilege | rank request | mitigate | Server-side frozen rank-one reread. |
| T-P5-03-03 | Race condition | concurrent ranking | mitigate | Immediate transaction and head/supersession checks. |
| T-P5-03-04 | Elevation of Privilege | Top-10 preselection ledger | mitigate | Separate store/projection, fixed SUPPORTING_ONLY authority, and no authorization link. |
</threat_model>

<verification>
Run ranking, both ledger, migration, tamper, lineage, restart, and concurrency tests.
</verification>

<success_criteria>
Top 3 is durable and only the current untampered rank 1 can proceed; the independent Top-10 preselection ledger remains read-only supporting evidence.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
