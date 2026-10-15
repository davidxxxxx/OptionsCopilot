---
phase: P1-strategy-nav-broker-snapshot
plan: "02"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/gateway/broker_snapshot.py
  - options_copilot/gateway/ibkr_readonly.py
  - tests/options_copilot/test_broker_snapshot.py
  - tests/options_copilot/test_ibkr_readonly_gateway.py
evidence_prefixes: []
autonomous: true
requirements: [OC-DATA-01, OC-SAFE-01, OC-VAL-01]
must_haves:
  truths:
    - "Account, positions, working orders, and unsubmitted instructions have matching canonical pre/post state hashes."
    - "Every secdef identity is stable across pre/post reads using conId, localSymbol, tradingClass, multiplier, exchange, expiry, strike, and right."
    - "Quotes are one coherent request batch with request IDs, timestamps, freshness, and bounded leg skew; changing prices are not compared for equality."
  artifacts:
    - path: "options_copilot/gateway/broker_snapshot.py"
      provides: "Atomic broker snapshot builder and immutable snapshot hash"
      contains: "BrokerSnapshotBuilder"
  key_links:
    - from: "options_copilot/gateway/broker_snapshot.py"
      to: "options_copilot/gateway/ibkr_readonly.py"
      via: "read-only state, secdef, and quote-batch protocols"
      pattern: "quote_batch"
---

<objective>
Implement a broker snapshot protocol that distinguishes state mutation, contract-identity mutation, and quote incoherence.

Purpose: Prevent mixed-time account snapshots without demanding impossible equality from live prices.
Output: Snapshot builder, read-only gateway support, and deterministic mutation/skew/freshness tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/gateway/ibkr_readonly.py
@options_copilot/bridge/coordinator.py
@options_copilot/IMPLEMENTATION_PATTERNS.md
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P1-02-01: Implement component-specific atomic snapshot protocol</name>
  <read_first>
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/bridge/coordinator.py
    - options_copilot/domain/models.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/gateway/broker_snapshot.py, options_copilot/gateway/ibkr_readonly.py</files>
  <action>Define immutable `BrokerSnapshotBuilder`. For account, positions, working orders, and unsubmitted instructions, canonicalize pre-read and post-read state and require equal hashes. For every option leg, pre-read and post-read a secdef identity tuple containing exactly `conId`, `localSymbol`, `tradingClass`, `multiplier`, `exchange`, `expiry`, `strike`, and `right`; any difference is state mutation. Fetch executable bid/ask through one quote batch whose record includes batch ID, per-leg request ID, requested_at, observed_at, completed_at, and source. Do not compare changing price values pre/post. Validate oldest quote age at most 5 seconds and maximum leg observation skew at most 2 seconds. State or secdef change returns `MUTATED_DURING_BUILD`; quote age, missing bid/ask, missing batch identity, or leg skew failure returns `QUOTE_INCOHERENT`. Bind all state hashes, secdef identity hashes, quote batch metadata/values, completeness, and timestamps into one immutable snapshot hash.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_broker_snapshot.py tests/options_copilot/test_ibkr_readonly_gateway.py</automated>
  </verify>
  <done>Broker snapshots fail for state/secdef mutation or quote incoherence while allowing legitimate live price changes.</done>
  <acceptance_criteria>
    - Empty orders/instructions are distinguishable from UNKNOWN
    - Position, order, instruction, and every secdef identity-field mutation returns MUTATED_DURING_BUILD
    - Quote values may change, but freshness 5.00/5.01 seconds and skew 2.000/2.001 seconds have exact pass/fail boundaries
    - Snapshot hash changes when any state, identity, quote, request ID, timestamp, or provenance changes
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P1-02-02: Prove canonical ordering and read-only gateway behavior</name>
  <read_first>
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/gateway/ibkr_readonly.py
    - tests/options_copilot/test_ibkr_readonly_gateway.py
  </read_first>
  <files>tests/options_copilot/test_broker_snapshot.py, tests/options_copilot/test_ibkr_readonly_gateway.py</files>
  <action>Add table-driven and property tests for canonical ordering, explicit empty versus unknown components, repeated conId rejection, adjusted or nonstandard contracts, multiplier not equal to 100, batch cancellation, timeout, and partial quotes. Assert gateway sessions remain read-only and expose no broker-write callable. Include a test that independently changing all bid/ask values inside a coherent batch can still pass when IDs, freshness, and skew satisfy the protocol.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_broker_snapshot.py tests/options_copilot/test_ibkr_readonly_gateway.py</automated>
  </verify>
  <done>Tests distinguish legitimate quote movement from incoherent quotes and prove canonical immutable identity.</done>
  <acceptance_criteria>
    - Reordering components keeps hashes stable
    - Partial or duplicate legs fail closed
    - A coherent changed-price fixture passes without price equality assumptions
    - Read-only gateway assertions pass
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Broker state to snapshot | Mutable external state is reconciled before becoming immutable evidence. |
| Quote batch to candidate gates | Live changing prices require coherence, not equality. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P1-02-01 | Tampering | account/secdef state | mitigate | Canonical pre/post hashes and exact identity fields. |
| T-P1-02-02 | Repudiation | quote provenance | mitigate | Batch/request IDs and per-leg timestamps in snapshot hash. |
| T-P1-02-03 | Denial of Service | partial quote batch | mitigate | Fixed QUOTE_INCOHERENT failure without guessed values. |
</threat_model>

<verification>
Run snapshot and gateway suites; inspect exact error-code boundaries.
</verification>

<success_criteria>
Every approval-grade snapshot is immutable, state-stable, secdef-stable, and quote-coherent.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
