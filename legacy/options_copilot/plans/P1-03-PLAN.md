---
phase: P1-strategy-nav-broker-snapshot
plan: "03"
type: execute
wave: 2
depends_on: ["P1-01", "P1-02"]
files_modified:
  - options_copilot/bridge/coordinator.py
  - options_copilot/bridge/store.py
  - options_copilot/proposals.py
  - options_copilot/market/session_calendar.py
  - options_copilot/risk/time_policy.py
  - tests/options_copilot/test_bridge_coordinator.py
  - tests/options_copilot/test_market_session_calendar.py
  - tests/options_copilot/test_proposal_gate.py
  - data/options_copilot/evidence/checkpoints/P1/ibkr-live-read/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P1/ibkr-live-read/
autonomous: false
requirements: [OC-DATA-01, OC-TIME-01, OC-RISK-01, OC-BRIDGE-01, OC-VAL-01]
must_haves:
  truths:
    - "Reserve-time requery uses the identical component-specific snapshot protocol and binds its immutable hash before durable attempt creation."
    - "Entry normally uses 14-35 DTE, audited exceptions never go below 7 DTE, and management below entry floors still requires P2 transition proof."
    - "A real-market read-only checkpoint proves the atomic protocol without creating an instruction or order."
  artifacts:
    - path: "options_copilot/market/session_calendar.py"
      provides: "Authoritative US options calendar normalization"
      contains: "UsOptionsSessionCalendar"
    - path: "data/options_copilot/evidence/checkpoints/P1/ibkr-live-read/"
      provides: "Real read-only snapshot and entitlement evidence"
  key_links:
    - from: "options_copilot/bridge/coordinator.py"
      to: "options_copilot/gateway/broker_snapshot.py"
      via: "reserve-lock requery using identical protocol"
      pattern: "BrokerSnapshotBuilder"
---

<objective>
Bind the atomic broker and Strategy NAV proofs through reservation, add authoritative session/DTE policy, and verify a real read-only market snapshot.

Purpose: Close the last approval-time race and time-policy ambiguity before GLD management.
Output: Reserve requery, calendar/time policy, tests, and P1 live evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/gateway/broker_snapshot.py
@options_copilot/performance/nav_ledger.py
@options_copilot/bridge/coordinator.py
@options_copilot/bridge/store.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P1-03-01: Requery and bind broker proof inside reservation</name>
  <read_first>
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/performance/nav_ledger.py
    - options_copilot/bridge/coordinator.py
    - options_copilot/bridge/store.py
    - options_copilot/proposals.py
  </read_first>
  <files>options_copilot/bridge/coordinator.py, options_copilot/bridge/store.py, options_copilot/proposals.py, tests/options_copilot/test_bridge_coordinator.py</files>
  <action>After obtaining the SQLite reservation write lock, rebuild account, positions, working orders, unsubmitted instructions, every secdef identity, and the complete quote batch with the exact P1-02 protocol. Compare the proposal-bound prior immutable snapshot and the reserve-time snapshot for frozen identity, risk, Strategy NAV contract/head, cost contract, and policy inputs. State or secdef mutation returns `MUTATED_DURING_BUILD`; stale, missing, or over-skew quote batch returns `QUOTE_INCOHERENT`. Bind the reserve-time immutable snapshot hash and all component proofs into the broker gate row before creating the unique external-attempt reservation. Reusing an object or only rereading the clock is forbidden.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_bridge_coordinator.py tests/options_copilot/test_proposal_gate.py</automated>
  </verify>
  <done>Reservation persists only after an identical fresh atomic requery and immutable hash binding.</done>
  <acceptance_criteria>
    - Lock-time position/order/instruction/secdef/NAV changes fail before external attempt count changes
    - Quote price movement is accepted only through a coherent fresh batch, never equality comparison
    - Reserve proof stores the new immutable snapshot hash and contract hashes
    - Concurrent and tampered requests produce zero creator calls
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P1-03-02: Implement authoritative market session and DTE policy</name>
  <read_first>
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/config.py
    - options_copilot/proposals.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/market/session_calendar.py, options_copilot/risk/time_policy.py, tests/options_copilot/test_market_session_calendar.py, tests/options_copilot/test_proposal_gate.py</files>
  <action>Normalize broker-published liquid/trading hours and timezone to America/New_York with UTC evidence and source hash. Missing or stale calendar data is DEGRADED and cannot create entry. Implement entry normal band 14-35 DTE; 7-13 requires an append-only signed exception with actor, reason, version, policy hash, and approval time; below 7 always rejects. Position management can operate below 14 only when P2 proves close or strict reduction without crossing zero or increasing exposure, risk, or capital. Persist ET trading date, UTC time, calendar hash, and exception hash.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_market_session_calendar.py tests/options_copilot/test_proposal_gate.py</automated>
  </verify>
  <done>Session and DTE decisions are authoritative, reproducible, and cannot be relaxed below the permanent floor.</done>
  <acceptance_criteria>
    - Tests cover DST, weekends, holidays, 13:00 ET early close, and Shanghai Saturday mapping
    - Entry boundaries 13/14/35/36 and exception 6/7 are exact
    - Management below entry floor requires a P2 proof and cannot increase or cross zero
  </acceptance_criteria>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task P1-03-03: Verify real-market read-only broker snapshot</name>
  <read_first>
    - options_copilot/operations/readiness.py
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/README.md
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P1/ibkr-live-read/</files>
  <action>Post-implementation only: during a real open-market window and user-confirmed read-only IBKR session, run `python -m options_copilot.operations.readiness --probe broker_session --probe market_data_entitlements --json --evidence-dir data/options_copilot/evidence/checkpoints/P1/ibkr-live-read`. Human-check account, GLD positions, working orders, unsubmitted instructions, all standard secdef identities, OPRA entitlement, quote-batch IDs/timestamps, oldest quote at most 5 seconds, maximum leg skew at most 2 seconds, stable pre/post state/secdefs, and authority-audit PASS. Do not create a review instruction or order.</action>
  <verify>
    <automated>python -m options_copilot.operations.readiness --probe broker_session --probe market_data_entitlements --json --evidence-dir data/options_copilot/evidence/checkpoints/P1/ibkr-live-read</automated>
  </verify>
  <done>Immutable live evidence proves one complete read-only atomic broker snapshot, or P2 remains blocked.</done>
  <acceptance_criteria>
    - Manifest hashes snapshot, entitlement, quotes, readiness, command, UTC time, Git commit, workspace baseline, and human actor
    - Broker and market-data probes are READY rather than DEGRADED
    - No instruction/order count increases
    - MUTATED_DURING_BUILD or QUOTE_INCOHERENT evidence fails the checkpoint
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Approved proposal to reserve transaction | Frozen intent crosses into the one-attempt durable boundary. |
| Broker calendar to time policy | External session metadata controls entry eligibility. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P1-03-01 | Tampering | reserve snapshot | mitigate | Identical lock-time protocol and immutable hash comparison. |
| T-P1-03-02 | Race condition | concurrent broker state | mitigate | Write lock, full requery, and attempt creation only after proof. |
| T-P1-03-03 | Spoofing | session calendar | mitigate | Broker provenance, timezone normalization, and calendar hash. |
</threat_model>

<verification>
Run focused bridge/calendar tests, then perform the named post-implementation live checkpoint.
</verification>

<success_criteria>
Reserve-time state is fresh and atomic, and P2 begins only after a real read-only proof.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
