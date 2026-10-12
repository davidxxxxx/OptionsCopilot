---
phase: P2-gld-position-management
plan: "02"
type: execute
wave: 2
depends_on: ["P2-01"]
files_modified:
  - options_copilot/positions/__init__.py
  - options_copilot/positions/runtime_adapter.py
  - options_copilot/approval/store.py
  - options_copilot/bridge/coordinator.py
  - options_copilot/proposals.py
  - options_copilot/api/app.py
  - options_copilot/frontend/index.html
  - options_copilot/frontend/app.js
  - options_copilot/frontend/styles.css
  - tests/options_copilot/test_runtime.py
  - tests/options_copilot/test_bridge_coordinator.py
  - tests/options_copilot/test_api_frontend.py
  - tests/options_copilot/test_position_management_runtime_adapter.py
  - data/options_copilot/evidence/checkpoints/P2/gld-dry-run/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P2/gld-dry-run/
autonomous: false
requirements: [OC-RISK-02, OC-BRIDGE-01, OC-EXIT-01, OC-DATA-01, OC-VAL-01]
must_haves:
  truths:
    - "Management intent uses a separate approval/API/GUI state from entry and cannot bypass the single-combination gate."
    - "The production management adapter can publish only a snapshot-derived preview or structured NO_TRADE; it has no approval, bridge, creator, instruction, or order-submission surface."
    - "Bridge authorization revalidates the frozen transition, cost, exit, and reserve-time broker snapshot hashes."
    - "The live GLD checkpoint produces only a dry-run approval/bridge preview with external attempt count zero."
  artifacts:
    - path: "options_copilot/api/app.py"
      provides: "Separate management read model and challenge endpoint"
      contains: "management"
    - path: "options_copilot/positions/runtime_adapter.py"
      provides: "Fail-closed read-only production adapter for GLD management previews"
      contains: "ProductionManagementCoordinator"
    - path: "data/options_copilot/evidence/checkpoints/P2/gld-dry-run/"
      provides: "Real-market GLD close proof without creator invocation"
  key_links:
    - from: "options_copilot/bridge/coordinator.py"
      to: "options_copilot/positions/manager.py"
      via: "hash-bound transition proof revalidation"
      pattern: "transition_proof"
---

<objective>
Wire GLD close/reduce proof into approval, bridge preview, API, GUI, and a real-market dry-run checkpoint.

Purpose: Deliver a usable management workflow while preserving permanent review-only authority and the one-combination block.
Output: Separate management state, a read-only production preview adapter, strict authorization integration, UI, tests, and immutable dry-run evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/positions/manager.py
@options_copilot/positions/generator.py
@options_copilot/approval/store.py
@options_copilot/bridge/coordinator.py
@options_copilot/api/app.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P2-02-01: Integrate management proof with approval and bridge preview</name>
  <read_first>
    - options_copilot/positions/models.py
    - options_copilot/positions/manager.py
    - options_copilot/positions/generator.py
    - options_copilot/approval/store.py
    - options_copilot/bridge/coordinator.py
  </read_first>
  <files>options_copilot/approval/store.py, options_copilot/bridge/coordinator.py, options_copilot/proposals.py, tests/options_copilot/test_runtime.py, tests/options_copilot/test_bridge_coordinator.py</files>
  <action>Add a distinct POSITION_MANAGEMENT intent and approval state. Approval and bridge preview must verify proof validity, same broker snapshot or a valid reserve-time requery, exact conIds/sides/ratios/quantities, Strategy NAV hash, signed cost contract version/hash, Exit Contract hash, zero authoritative working orders and unsubmitted instructions, and no after-state exposure/risk/capital increase. Entry remains blocked whenever GLD occupies the combination slot. Hand-entered legs, labels, or replaced hashes have no authority. This phase ends at preview: no external reservation attempt or creator invocation.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_runtime.py tests/options_copilot/test_bridge_coordinator.py tests/options_copilot/test_gld_management_pipeline.py</automated>
  </verify>
  <done>Only a fresh, hash-matched GLD close/reduce proof can create a management approval and bridge preview.</done>
  <acceptance_criteria>
    - Complete CLOSE_ALL and strict REDUCE_RISK fixtures reach preview
    - Entry remains NO_TRADE while GLD is open
    - Tampering any identity, quote, cost, exit, or proof field is rejected before an external attempt
    - Concurrent orders/instructions fail closed
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P2-02-02: Compose a fail-closed production management preview adapter</name>
  <read_first>
    - options_copilot/positions/models.py
    - options_copilot/positions/manager.py
    - options_copilot/positions/generator.py
    - options_copilot/gateway/broker_snapshot.py
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/governance/contracts.py
  </read_first>
  <files>options_copilot/positions/__init__.py, options_copilot/positions/runtime_adapter.py, tests/options_copilot/test_position_management_runtime_adapter.py</files>
  <action>Implement `ProductionManagementCoordinator` as a read-only adapter around the same `PositionManager` and `ManagementCandidateGenerator` used by the proof core. Build one fresh atomic broker snapshot, discover only a complete standard-multiplier GLD combination, require authoritative zero working orders and zero unsubmitted instructions, verify the signed Execution Cost Contract and the full Exit Contract, and publish either the immutable CLOSE_ALL/REDUCE_RISK preview or a structured sanitized NO_TRADE. Missing or unknown instruction state, stale/incomplete broker truth, a non-GLD or ambiguous position, unavailable/tampered contracts, or pacing denial must stop before candidate generation. The adapter and its package export must not import approval, bridge, creator, instruction-creation, or order-submission surfaces. Production composition may wire the adapter while deliberately supplying an unavailable Exit Contract provider; that state is NO_TRADE and is not live GLD readiness.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_position_management_runtime_adapter.py tests/options_copilot/test_gld_management_pipeline.py tests/options_copilot/test_position_manager.py</automated>
  </verify>
  <done>The production-facing adapter reuses the mathematical proof core and cannot turn incomplete broker or contract evidence into an actionable management preview.</done>
  <acceptance_criteria>
    - Dynamic conIds and the same PositionManager produce the complete preview fixture
    - Unknown instructions, incomplete broker components, non-GLD positions, duplicate conIds, and missing signed contracts produce NO_TRADE before generation
    - Provider exceptions are sanitized and never expose credentials or broker internals
    - Direct order submission, instruction creation, approval, and bridge authority remain absent
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P2-02-03: Expose separate GLD management API and GUI</name>
  <read_first>
    - options_copilot/api/app.py
    - options_copilot/frontend/index.html
    - options_copilot/frontend/app.js
    - options_copilot/frontend/styles.css
  </read_first>
  <files>options_copilot/api/app.py, options_copilot/frontend/index.html, options_copilot/frontend/app.js, options_copilot/frontend/styles.css, tests/options_copilot/test_api_frontend.py</files>
  <action>Add a dedicated Position Management section and endpoint. Display authoritative before/after legs, quote batch age/skew, executable close value, signed costs, max-loss/capital changes, and exit rules. The action text is `Create IBKR review instruction` and remains disabled without fresh executable quotes and valid proof. Entry and management handlers, badges, and state transitions are separate. Read models always expose review-only status and false submission/transmission results.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_api_frontend.py &amp;&amp; node --check options_copilot/frontend/app.js</automated>
  </verify>
  <done>Users can distinguish and inspect GLD management without gaining a hidden entry or broker execution path.</done>
  <acceptance_criteria>
    - Missing quotes show `NO_EXECUTABLE_EXIT_QUOTE` and no actionable control
    - Management and entry have separate API routes and DOM handlers
    - Review-only and false submission/transmission fields remain visible
    - Frontend syntax and tests pass
  </acceptance_criteria>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task P2-02-04: Verify real-market GLD close dry run</name>
  <read_first>
    - options_copilot/positions/cli.py
    - options_copilot/positions/generator.py
    - options_copilot/positions/manager.py
    - options_copilot/BASELINE.md
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P2/gld-dry-run/</files>
  <action>Post-implementation only: while GLD has coherent quotes no older than 5 seconds, run `python -m options_copilot.positions.cli dry-run --symbol GLD --kind CLOSE_ALL --json --evidence-dir data/options_copilot/evidence/checkpoints/P2/gld-dry-run`. Human-check the authoritative 1/-2/1 legs, standard multiplier, derived opposite sides/ratios/combo quantity/signed value, signed cost contract, exit rules, zero after position, non-increasing capital/risk/exposure, reserve-preview hash, and zero external attempts. The command must not reserve, invoke a creator, or create an instruction.</action>
  <verify>
    <automated>python -m options_copilot.positions.cli dry-run --symbol GLD --kind CLOSE_ALL --json --evidence-dir data/options_copilot/evidence/checkpoints/P2/gld-dry-run</automated>
  </verify>
  <done>Real GLD evidence proves a complete close preview with no external side effect, or P5 entry remains blocked.</done>
  <acceptance_criteria>
    - Manifest binds command, UTC, Git/workspace, snapshot, cost, exit, transition, preview, and human actor hashes
    - After position is zero and all non-increase predicates pass
    - Working orders and instructions are authoritative zero
    - External attempt and creator call counts remain zero
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Transition proof to approval | A local management intent becomes a short-lived human challenge. |
| API/GUI to bridge preview | User-visible state must not create hidden authority. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P2-02-01 | Elevation of Privilege | management endpoint | mitigate | Separate intent and revalidated mathematical proof. |
| T-P2-02-02 | Tampering | challenge hashes | mitigate | Bind all snapshot/cost/exit/proof fields and reread at preview. |
| T-P2-02-03 | Spoofing | GUI state | mitigate | API is authoritative; disabled UI cannot bypass server gates. |
</threat_model>

<verification>
Run integration/UI tests, then perform the post-implementation GLD dry run with human evidence review.
</verification>

<success_criteria>
GLD has a real close/reduce review path while every new-entry and execution capability remains unavailable.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
