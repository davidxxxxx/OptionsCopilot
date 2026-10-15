---
phase: P7-external-creator
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/bridge/creator_adapter.py
  - options_copilot/bridge/coordinator.py
  - options_copilot/bridge/store.py
  - options_copilot/bridge/__main__.py
  - tests/options_copilot/test_creator_adapter.py
  - tests/options_copilot/test_bridge_coordinator.py
  - tests/options_copilot/test_gld_creator_e2e.py
evidence_prefixes: []
autonomous: true
requirements: [OC-SAFE-01, OC-BRIDGE-01, OC-DATA-01, OC-RISK-01, OC-LEARN-01, OC-SEC-01, OC-STRAT-01, OC-VAL-01]
must_haves:
  truths:
    - "Pure prepare_and_validate_payload completes before any durable external-attempt reservation."
    - "External create_review_instruction is callable only after a durable reservation and only through the signed concrete transport contract."
    - "Each approval has at most one external attempt; uncertainty is terminal UNKNOWN_OUTCOME with no retry."
    - "NORMAL creator handoff is capped at 10.00%; 10.01-15.00 requires the current human-signed P9 A-grade authority for that exact proposal/policy/cost/ranking basis, and 15.01/20.00/20.01 always reject."
    - "Frozen ranking, approval, bridge intent, reservation, and creator payload carry identical current-policy and risk-authority marker hashes."
  artifacts:
    - path: "options_copilot/bridge/creator_adapter.py"
      provides: "Pure payload preparation and exact signed-transport external adapter"
      contains: "prepare_and_validate_payload"
    - path: "options_copilot/bridge/store.py"
      provides: "Durable one-attempt reservation and UNKNOWN terminal state"
  key_links:
    - from: "options_copilot/bridge/creator_adapter.py"
      to: "options_copilot/governance/creator_transport_contract.v1.json"
      via: "connector_id/tool_name and exact JSON schema execution"
      pattern: "create_review_instruction"
    - from: "options_copilot/bridge/coordinator.py"
      to: "options_copilot/ranking/store.py"
      via: "transactional frozen rank-one reread before payload preparation"
      pattern: "ranking_snapshot_id"
    - from: "options_copilot/bridge/creator_adapter.py"
      to: "options_copilot/risk/authorization.py"
      via: "versioned current RiskTierAuthority resolution and exact A-grade marker hash"
      pattern: "risk_authority_marker_hash"
    - from: "options_copilot/bridge/coordinator.py"
      to: "options_copilot/learning/policy_authority.py"
      via: "transactional current Champion resolution before reservation"
      pattern: "current_policy_version"
---

<objective>
Implement the exact signed creator adapter boundary with pure current-authority-aware pre-reservation validation and one post-reservation external call.

Purpose: Close the review-instruction handoff without local order authority, stale Champion/A-grade authority, retries, or auth leakage.
Output: Adapter, coordinator/store/CLI integration, and payload/authority/concurrency/UNKNOWN/GLD tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/governance/creator_transport_contract.v1.json
@options_copilot/governance/execution_cost_contract.v1.json
@options_copilot/governance/initial_champion_scenario_policy.v1.json
@options_copilot/ranking/store.py
@options_copilot/bridge/coordinator.py
@options_copilot/bridge/store.py
@options_copilot/risk/authorization.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P7-01-01: Implement pure pre-reservation payload preparation</name>
  <read_first>
    - options_copilot/governance/creator_transport_contract.v1.json
    - options_copilot/ranking/store.py
    - options_copilot/positions/manager.py
    - options_copilot/strategies/generator.py
    - options_copilot/bridge/coordinator.py
    - options_copilot/risk/authorization.py
  </read_first>
  <files>options_copilot/bridge/creator_adapter.py, tests/options_copilot/test_creator_adapter.py</files>
  <action>Implement pure `prepare_and_validate_payload(approved_intent, rank_row, transition_proof, reserve_snapshot, cost_contract, current_policy_resolution, risk_tier_authority_resolution, transport_contract)`. The coordinator obtains both immutable versioned resolutions transactionally from the current resolvers; the pure function performs no database read/write and no external call. Transactionally supplied frozen rank must be 1 for entry; ranks 2-3 and A_GRADE_PENDING reject. Derive every conId/action/ratio/combo quantity and signed limit from frozen leg deltas and cash-flow convention, never hardcode side or quantity. Require matching input/evidence/broker/secdef/quote/Strategy NAV/proposal/candidate/current-policy/policy-authority/cost/ranking-basis/final-ranking/risk-authority/exit/transition hashes across the rank row and approved intent, plus a fresh coherent reserve snapshot. NORMAL permits maximum loss at or below 10.00 percent. From 10.01 through 15.00 percent, require a current unrevoked human-signed P9 `APPROVE_A_GRADE` marker whose content hash equals `risk_authority_marker_hash` and whose signed fields bind the exact proposal hash, current policy version/hash, signed cost version/hash, canonical ranking-basis hash, risk contract, evaluation, dataset, independence spec, actor, and signed time. A rollback, revocation, promotion to a different policy, stale resolver head, tamper, or ordinary promotion marker rejects. 15.01 through 20.00 percent inclusive and every value at or above 20.00 percent reject; specifically test 15.01, 20.00, and 20.01 even with a forged or otherwise valid marker. Strict P2 close/reduce remains a separate proof path. Validate exact signed connector input schema and `review_only=true`. Return a canonical payload carrying `current_policy_version/hash`, `policy_authority_marker_hash`, `risk_authority_version`, and `risk_authority_marker_hash`, or a fixed rejection.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_creator_adapter.py tests/options_copilot/test_gld_creator_e2e.py</automated>
  </verify>
  <done>Payload preparation is pure, dynamic, exact-schema, rank-one/cost/current-policy/risk-authority bound, and side-effect free.</done>
  <acceptance_criteria>
    - Tests cover debit, credit, combo quantity above one, CLOSE_ALL, REDUCE_RISK, and GLD 1/-2/1 close
    - Every individual identity/side/ratio/quantity/sign/hash tamper rejects before reservation
    - Rank 2/3, A_GRADE_PENDING, stale/tampered/revoked authority, and 10.01 percent without a current bound P9 marker reject
    - 10.00 NORMAL passes; 15.01, 20.00, and 20.01 always reject independent of marker fields
    - Payload copies the exact current-policy and risk-authority identities from the frozen row and approved intent
    - Preparation changes no bridge row and calls no external operation
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P7-01-02: Execute signed transport exactly once after durable reservation</name>
  <read_first>
    - options_copilot/bridge/creator_adapter.py
    - options_copilot/bridge/coordinator.py
    - options_copilot/bridge/store.py
    - options_copilot/governance/creator_transport_contract.v1.json
  </read_first>
  <files>options_copilot/bridge/coordinator.py, options_copilot/bridge/store.py, options_copilot/bridge/__main__.py, tests/options_copilot/test_creator_adapter.py, tests/options_copilot/test_bridge_coordinator.py</files>
  <action>After pure preparation succeeds, reacquire the current policy and risk-tier authority under the same durable write transaction, require exact equality with the frozen ranking/approval/bridge-intent resolutions, then acquire the one-attempt reservation. Store approval ID, payload hash, immutable reserve snapshot hash, ranking-basis/final-row hashes, current policy version/hash, policy-authority marker hash, cost version/hash, risk-authority version/marker hash, signed transport contract version/hash, connector_id/tool_name, and attempt number. A concurrent promotion, rollback, A-grade revocation, or resolver-head change aborts before reservation with zero connector calls. Only then call `create_review_instruction` through the selected operation and exact schema. Validate output reconciliation, instruction ID, review-only status, false local submission/transmission fields, and allowlisted HTTPS host/path with no query/fragment. Timeout, crash, malformed response, uncertain reconciliation, or process death becomes terminal UNKNOWN_OUTCOME; repeated dispatch for that approval is rejected and never calls the connector again. There is no browser, credential-copy, or order-capable fallback.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_creator_adapter.py tests/options_copilot/test_bridge_coordinator.py tests/options_copilot/test_gld_creator_e2e.py</automated>
  </verify>
  <done>The exact signed connector operation runs once after reservation and uncertainty cannot retry.</done>
  <acceptance_criteria>
    - Concurrent callers produce one connector call
    - Concurrent promotion/rollback or A-grade revocation before reservation produces zero connector calls
    - Crash/timeout/malformed/uncertain fixtures end UNKNOWN_OUTCOME and retry call count remains zero
    - Unsafe host/path/query/fragment and non-review response fail closed
    - Managed auth material never enters app state, evidence, logs, or payload snapshots
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P7-01-03: Re-run repository authority and end-to-end tamper tests</name>
  <read_first>
    - options_copilot/operations/authority_audit.py
    - options_copilot/bridge/creator_adapter.py
    - tests/options_copilot/test_gld_creator_e2e.py
  </read_first>
  <files>tests/options_copilot/test_creator_adapter.py, tests/options_copilot/test_bridge_coordinator.py, tests/options_copilot/test_gld_creator_e2e.py</files>
  <action>Add the first full frozen-ranking-to-fake-review result for NORMAL entry at exactly 10.00 percent and GLD close/reduce; this is the only pre-P9 creator acceptance path. Assert 10.01, 15.00, 15.01, 20.00, and 20.01 all reject without a current human-signed P9 A-grade authority. Add transactional concurrency and tamper cases spanning ranking basis/final row, approval, reserve snapshot, secdefs, quote batch, cost/current-policy/policy-authority/risk-authority, transition, payload, and response. Stale, tampered, revoked, rolled-back, or wrong-proposal marker fixtures call the connector zero times. The post-P9 10.01/15.00 permitted E2E branch is deliberately executed only by P9-02 after the human signing checkpoint. Run the repository authority audit including creator_adapter and require zero implemented broker-write callable. Confirm no local code can supply managed authentication.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_creator_adapter.py tests/options_copilot/test_bridge_coordinator.py tests/options_copilot/test_gld_creator_e2e.py &amp;&amp; python -m options_copilot.operations.authority_audit --root options_copilot --json</automated>
  </verify>
  <done>Full pipeline and repository audit prove one review-only external call and no local execution authority.</done>
  <acceptance_criteria>
    - NORMAL 10.00 and management happy paths bind every frozen hash
    - Before P9 human signing, 10.01/15.00/15.01/20.00/20.01 all produce zero connector calls
    - Each tamper case yields zero connector calls
    - Authority audit passes with creator_adapter included
    - No credential or broker execution primitive exists locally
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Frozen approval to payload | Untrusted identifiers are reconstructed and validated before reservation. |
| Signed policy/risk authority to creator | Current resolver heads must match ranking, approval, intent, reservation, and payload. |
| Durable reservation to Managed Connector | Exactly one external review-only operation crosses the app boundary. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P7-01-01 | Tampering | creator payload | mitigate | Pure reconstruction and all-hash validation. |
| T-P7-01-02 | Repudiation | external outcome | mitigate | Durable attempt and terminal UNKNOWN reconciliation. |
| T-P7-01-03 | Elevation of Privilege | creator operation | mitigate | Signed exact review-only transport and repository authority audit. |
| T-P7-01-04 | Information Disclosure | managed auth | mitigate | Auth isolation and no local secret field. |
| T-P7-01-05 | Elevation of Privilege | stale A-grade authority | mitigate | Exact proposal/policy/cost/ranking-basis binding and transaction-time resolver reread. |
</threat_model>

<verification>
Run creator/bridge/GLD suites and repository authority audit; no live connector call occurs in this plan.
</verification>

<success_criteria>
The adapter can create one review-only instruction after durable reservation and can never retry or execute an order.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
