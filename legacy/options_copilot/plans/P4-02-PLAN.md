---
phase: P4-universe-scheduler
plan: "02"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/scanner/universe.py
  - options_copilot/scanner/pacing.py
  - options_copilot/config.py
  - options_copilot/gateway/ibkr_readonly.py
  - options_copilot/operations/pacing_authority.py
  - options_copilot/operations/readiness.py
  - tests/options_copilot/test_universe_funnel.py
  - tests/options_copilot/test_scanner_pacing.py
  - tests/options_copilot/test_pacing_authority.py
evidence_prefixes: []
autonomous: true
requirements: [OC-DATA-02, OC-SCAN-01, OC-TIME-01, OC-RISK-02, OC-VAL-01]
must_haves:
  truths:
    - "UniverseFunnel processes positions, core ETFs, events, scanner results, coarse contracts, and finalists without skipping a level."
    - "P4 request budgets derive only from the signed P0 MarketDataPacingCapability version/hash."
    - "GLD forces POSITION_MANAGEMENT_ONLY and leaves entry finalists empty."
  artifacts:
    - path: "options_copilot/scanner/universe.py"
      provides: "Layered universe with inclusion/exclusion evidence"
      contains: "UniverseFunnel"
    - path: "options_copilot/scanner/pacing.py"
      provides: "Per-class budgets, cache provenance, and subscription leases"
      contains: "RequestBudgetByClass"
    - path: "options_copilot/operations/pacing_authority.py"
      provides: "Fail-closed reader for the current signed pacing authority artifact"
      contains: "load_approved_pacing_capability"
  key_links:
    - from: "options_copilot/scanner/pacing.py"
      to: "options_copilot/operations/capabilities.py"
      via: "validated P0 pacing capability only"
      pattern: "MarketDataPacingCapability"
---

<objective>
Implement the bounded UniverseFunnel and market-data pacing/cache/subscription contract.

Purpose: Avoid all-options request storms and ensure GLD management remains first priority.
Output: Universe, per-class pacing, gateway/readiness integration, and boundary tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/operations/capabilities.py
@options_copilot/gateway/ibkr_readonly.py
@options_copilot/market/session_calendar.py
@options_copilot/positions/manager.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P4-02-01: Implement layered UniverseFunnel</name>
  <read_first>
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/market/session_calendar.py
    - options_copilot/risk/time_policy.py
    - options_copilot/positions/manager.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/scanner/universe.py, options_copilot/config.py, tests/options_copilot/test_universe_funnel.py</files>
  <action>Implement `UniverseFunnel` in the fixed sequence `positions -> core_etfs -> event_pool -> scanner -> coarse_contracts -> finalists`. Persist included and excluded items, reason codes, evidence hashes, and per-level request usage. Entry normally keeps 14-35 DTE; signed exceptions may retain 7-13; below 7 always rejects. Position-management underlyings are first and may be below entry DTE only for P2 proof. While GLD is open, set `POSITION_MANAGEMENT_ONLY` and emit zero entry finalists. Incomplete evidence or exhausted budget returns PARTIAL/NO_TRADE rather than guessing.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_universe_funnel.py</automated>
  </verify>
  <done>The universe is layered, auditable, DTE-safe, and GLD management-first.</done>
  <acceptance_criteria>
    - No fixture can jump a layer
    - Every exclusion has a fixed reason and evidence hash
    - DTE 13/14/35/36 and exception 6/7 boundaries match P1
    - GLD produces management seeds and zero entry finalists
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P4-02-02: Derive per-class budgets only from signed P0 capability</name>
  <read_first>
    - options_copilot/operations/capabilities.py
    - options_copilot/scanner/universe.py
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/operations/readiness.py
  </read_first>
  <files>options_copilot/scanner/pacing.py, options_copilot/gateway/ibkr_readonly.py, options_copilot/operations/pacing_authority.py, options_copilot/operations/readiness.py, tests/options_copilot/test_scanner_pacing.py, tests/options_copilot/test_pacing_authority.py</files>
  <action>Implement independent budgets for exactly `scanner`, `secdef`, `snapshot_quote`, `streaming_quote`, and `historical`. Initialize each limit only from a verified signed P0 `MarketDataPacingCapability` version/hash. The operational reader must require one current, canonical, human-signed authority artifact with the expected actor, schema, content hash, per-class coverage, and freshness; ambiguous directories, stale/future/tampered artifacts, duplicate JSON keys, wrong actors, partial classes, and non-finite values fail closed. P4 must not define or infer conservative defaults; invalid authority returns `PACING_CAPABILITY_MISSING` and no gateway connection or requests. Cache records bind request key, class, source, observed/expiry times, entitlement hash, capability hash, and payload hash. Expired cache cannot establish eligibility. Every temporary streaming/snapshot request uses a lease with cancel-exactly-once across success, timeout, parse error, cancellation, budget exhaustion, and keyboard interruption, without cancelling a pre-existing subscription baseline.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_scanner_pacing.py tests/options_copilot/test_pacing_authority.py tests/options_copilot/test_ibkr_readonly_gateway.py</automated>
  </verify>
  <done>All requests are capability-derived, class-bounded, provenance-cached, and leak-free.</done>
  <acceptance_criteria>
    - Each class has independent 0/limit/limit+1 tests
    - Missing or invalid P0 capability issues zero broker requests
    - Ambiguous, stale, future, tampered, wrong-actor, or partial authority artifacts keep the production gateway disconnected
    - TTL plus 1 ms misses cache
    - All exception paths cancel temporary subscriptions exactly once and return to baseline
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Signed pacing contract to request scheduler | Human-approved broker capability becomes a hard request budget. |
| Cache to candidate funnel | Cached payloads can affect eligibility only while fresh and provenance-bound. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P4-02-01 | Denial of Service | request pacing | mitigate | Per-class hard budgets from signed capability. |
| T-P4-02-02 | Tampering | cache | mitigate | Capability/entitlement/payload hashes and TTL. |
| T-P4-02-03 | Resource exhaustion | subscriptions | mitigate | Cancel-exactly-once leases and baseline reconciliation. |
</threat_model>

<verification>
Run funnel and pacing/gateway suites, including all failure and cancellation paths.
</verification>

<success_criteria>
P4 cannot request market data without signed limits and cannot leak a subscription or open an entry while GLD remains.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
