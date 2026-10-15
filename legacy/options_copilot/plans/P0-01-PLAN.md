---
phase: P0-capability-baseline
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/operations/capabilities.py
  - options_copilot/operations/readiness.py
  - options_copilot/gateway/ibkr_readonly.py
  - options_copilot/api/app.py
  - tests/options_copilot/test_readiness.py
  - tests/options_copilot/test_readiness_cli.py
  - data/options_copilot/evidence/checkpoints/P0/market-data-pacing/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P0/market-data-pacing/
autonomous: false
requirements: [OC-SAFE-01, OC-DATA-02, OC-OPS-01, OC-VAL-01]
must_haves:
  truths:
    - "Readiness never upgrades missing, stale, degraded, or forbidden capability evidence to READY_FOR_REVIEW."
    - "P0 emits one signed, versioned MarketDataPacingCapability covering scanner, secdef, snapshot_quote, streaming_quote, and historical request classes."
    - "Unavailable broker-disclosed limits require a human-approved conservative configuration; missing approval fails closed."
  artifacts:
    - path: "options_copilot/operations/capabilities.py"
      provides: "Immutable readiness and pacing capability contracts"
      contains: "MarketDataPacingCapability"
    - path: "options_copilot/operations/readiness.py"
      provides: "Readiness and capability probe CLI"
      contains: "market_data_pacing"
    - path: "data/options_copilot/evidence/checkpoints/P0/market-data-pacing/"
      provides: "Signed pacing capability evidence"
  key_links:
    - from: "options_copilot/operations/readiness.py"
      to: "options_copilot/gateway/ibkr_readonly.py"
      via: "read-only entitlement and pacing observations"
      pattern: "MarketDataPacingCapability"
    - from: "options_copilot/api/app.py"
      to: "options_copilot/operations/readiness.py"
      via: "shared readiness report"
      pattern: "api/readiness"
---

<objective>
Create truthful machine-readable readiness and a versioned market-data pacing contract before any scanner work.

Purpose: Prevent mocks, missing entitlements, or guessed broker limits from being treated as production capability.
Output: Deterministic readiness records, CLI/API probes, tests, and a human-approved pacing evidence artifact.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/IMPLEMENTATION_PLAN.md
@options_copilot/IMPLEMENTATION_RESEARCH.md
@options_copilot/IMPLEMENTATION_PATTERNS.md
@options_copilot/README.md
@options_copilot/gateway/ibkr_readonly.py
@options_copilot/api/app.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P0-01-01: Define readiness and pacing capability contracts</name>
  <read_first>
    - options_copilot/config.py
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/api/app.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/operations/capabilities.py, tests/options_copilot/test_readiness.py</files>
  <action>Define frozen `CapabilityStatus`, `CapabilityRecord`, and `ReadinessReport`. Define `MarketDataPacingCapability(version, observed_at, source, request_classes, content_hash, signer)` where `request_classes` has exactly `scanner`, `secdef`, `snapshot_quote`, `streaming_quote`, and `historical`; each class contains `max_concurrency`, `request_window`, `max_requests`, and `cooldown`. Restrict `source` to `broker_disclosed`, `observed`, or `conservative_default`. Canonicalize and hash every field. Unknown fields, missing request classes, non-positive limits, stale observation, or unsigned `conservative_default` must return fixed fail-closed reason codes. Keep all broker access read-only.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_readiness.py</automated>
  </verify>
  <done>The immutable readiness and pacing schemas validate all five classes and reject guessed or unsigned limits.</done>
  <acceptance_criteria>
    - `options_copilot/operations/capabilities.py` contains `class MarketDataPacingCapability`
    - Tests cover all three source values and all five request classes
    - Tests reject a missing class, zero limit, stale observation, changed hash, and unsigned conservative defaults
    - No test or production path grants broker-write authority
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P0-01-02: Implement shared CLI and API readiness probes</name>
  <read_first>
    - options_copilot/operations/capabilities.py
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/api/app.py
    - scripts/start_options_copilot.ps1
  </read_first>
  <files>options_copilot/operations/readiness.py, options_copilot/gateway/ibkr_readonly.py, options_copilot/api/app.py, tests/options_copilot/test_readiness_cli.py</files>
  <action>Implement named probes for broker session, market-data entitlements and pacing, providers, creator transport, scanner heartbeat, learning governance, process/ports, and Tailscale route. The market-data probe must collect broker-disclosed limits when available and bounded observations otherwise; it must never invent a limit. Add `python -m options_copilot.operations.readiness --probe market_data_pacing --json --evidence-dir PATH` and GET `/api/readiness`, both returning the same canonical report and fixed reason codes. Missing later modules return MISSING rather than an import failure. API output includes `review_only=true` and `direct_order_submission=false` and rejects secret-like fields.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_readiness.py tests/options_copilot/test_readiness_cli.py tests/options_copilot/test_api_frontend.py</automated>
  </verify>
  <done>CLI and API expose identical truthful readiness and pacing evidence without secrets or broker-write behavior.</done>
  <acceptance_criteria>
    - CLI help lists `market_data_pacing`
    - Fixture-identical CLI and API reports have the same content hash
    - Missing entitlement, delayed quotes, unknown listener, missing creator, and missing remote route are not READY_FOR_REVIEW
    - Probe artifacts contain no token, password, authorization header, or raw credential value
  </acceptance_criteria>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P0-01-03: Approve observed or conservative pacing capability</name>
  <read_first>
    - options_copilot/operations/capabilities.py
    - options_copilot/operations/readiness.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P0/market-data-pacing/</files>
  <action>Post-implementation only: run the pacing probe and present each request class, provenance, observation time, and proposed limits to the user. If broker-disclosed or sufficiently observed limits exist, the user signs that exact version and hash. If not, require the user to approve explicit execution-time `conservative_default` values for every class; do not select them silently. Record actor, signed_at, source, full capability, version, and hash in an immutable run directory. If any class is missing or approval is withheld, record a fail-closed decision and block P4.</action>
  <verify>
    <automated>python -m options_copilot.operations.readiness --probe market_data_pacing --json --evidence-dir data/options_copilot/evidence/checkpoints/P0/market-data-pacing</automated>
  </verify>
  <done>A signed complete pacing capability exists, or P4 is explicitly blocked with no fallback.</done>
  <acceptance_criteria>
    - Evidence records actor, signed_at, observed_at, source, version, and content hash
    - All five request classes include the four required limit fields
    - Conservative defaults are present only after explicit human approval
    - Missing or rejected limits produce `PACING_CAPABILITY_MISSING` and P4 cannot start
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Broker and OS probes to readiness | External and local runtime facts enter a machine-readable capability report. |
| Human pacing approval to scanner | A signed immutable contract becomes the only source for P4 budgets. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P0-01-01 | Spoofing | readiness probe | mitigate | Canonical source identifiers, observation times, evidence paths, and content hashes. |
| T-P0-01-02 | Tampering | pacing capability | mitigate | Signed version/hash and fail-closed validation for every request class. |
| T-P0-01-03 | Elevation of Privilege | broker gateway | mitigate | Read-only adapter only; capability collection cannot create instructions or orders. |
</threat_model>

<verification>
Run the task-specific pytest commands, then run the post-implementation pacing probe only at its blocking checkpoint.
</verification>

<success_criteria>
P0 readiness is truthful and P4 has exactly one signed, complete pacing capability source.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
