---
phase: P0-capability-baseline
plan: "04"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/bridge/creator_transport.py
  - options_copilot/operations/creator_probe.py
  - options_copilot/governance/creator_transport_contract.v1.json
  - tests/options_copilot/test_creator_transport.py
  - data/options_copilot/evidence/checkpoints/P0/creator-transport/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P0/creator-transport/
autonomous: false
requirements: [OC-SAFE-01, OC-BRIDGE-01, OC-SEC-01, OC-VAL-01]
must_haves:
  truths:
    - "One concrete installed Managed Connector operation is discovered, probed, selected, and signed before P7."
    - "The signed transport contract contains exact input/output JSON schemas and no application-visible authentication material."
    - "No supported connector means P7 fails closed; browser automation and order submission are not fallback paths."
  artifacts:
    - path: "options_copilot/governance/creator_transport_contract.v1.json"
      provides: "Signed executable creator transport boundary"
    - path: "options_copilot/operations/creator_probe.py"
      provides: "Capability discovery and non-order probe CLI"
  key_links:
    - from: "options_copilot/bridge/creator_transport.py"
      to: "options_copilot/governance/creator_transport_contract.v1.json"
      via: "exact connector operation and JSON schema validation"
      pattern: "connector_id"
---

<objective>
Discover and freeze the only allowed external review-instruction creator transport before adapter implementation.

Purpose: Replace an abstract creator assumption with an installed, probed, signed, auth-isolated operation contract.
Output: Discovery/probe code, tests, selected transport contract, and human decision evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/README.md
@options_copilot/bridge/coordinator.py
@options_copilot/bridge/store.py
@options_copilot/IMPLEMENTATION_RESEARCH.md
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P0-04-01: Build creator transport discovery and contract validator</name>
  <read_first>
    - options_copilot/bridge/coordinator.py
    - options_copilot/bridge/store.py
    - options_copilot/README.md
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/bridge/creator_transport.py, options_copilot/operations/creator_probe.py, tests/options_copilot/test_creator_transport.py</files>
  <action>Implement discovery for installed Managed Connector operations and a non-order capability probe. The validated contract must contain `connector_id`, `tool_name`, exact input and output JSON schemas, `auth_isolation=no_secret_in_app_snapshot_or_log`, HTTPS deep-link host and path rules, reconciliation fields, idempotency behavior, capability-probe evidence hash, version, and signer/time. Reject wildcard schemas, unknown deep-link hosts or paths, query/fragment credentials, order-capable operations, and any contract that exposes auth material. Provide a CLI that lists supported operations and writes redacted probe evidence without creating an instruction.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_creator_transport.py</automated>
  </verify>
  <done>Only exact review-only Managed Connector operations can produce a valid creator transport contract.</done>
  <acceptance_criteria>
    - Tests require connector_id, tool_name, exact schemas, auth isolation, deep-link rules, reconciliation, idempotency, version, hash, and signer/time
    - Tests reject browser automation and any order-capable operation
    - Probe evidence contains no secret-like key or value
  </acceptance_criteria>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P0-04-02: Select and sign one installed creator operation</name>
  <read_first>
    - options_copilot/bridge/creator_transport.py
    - options_copilot/operations/creator_probe.py
    - options_copilot/README.md
  </read_first>
  <files>options_copilot/governance/creator_transport_contract.v1.json, data/options_copilot/evidence/checkpoints/P0/creator-transport/</files>
  <action>Post-implementation only: run `python -m options_copilot.operations.creator_probe --discover --json --evidence-dir data/options_copilot/evidence/checkpoints/P0/creator-transport`, present supported exact operations, and require the user to select one. Probe the selected operation's capability without creating an instruction, capture schema/reconciliation/idempotency evidence, and sign the exact contract with actor and signed_at. If no operation satisfies the review-only and auth-isolation contract, write `CREATOR_TRANSPORT_UNAVAILABLE`, leave the contract absent, and block P7. Do not substitute browser automation, local credential extraction, or an order operation.</action>
  <verify>
    <automated>python -m options_copilot.operations.creator_probe --verify-contract options_copilot/governance/creator_transport_contract.v1.json --json --evidence-dir data/options_copilot/evidence/checkpoints/P0/creator-transport</automated>
  </verify>
  <done>One supported operation is signed and executable through its exact contract, or P7 is explicitly blocked.</done>
  <acceptance_criteria>
    - Contract binds a concrete installed connector_id and tool_name
    - Input/output schemas and deep-link host/path are exact
    - Human actor, signed_at, capability evidence hash, version, and content hash are present
    - No supported operation yields fail-closed evidence and no fallback
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Installed connector inventory to local contract | External operation metadata is untrusted until probed and signed. |
| Managed authentication to local app | Authentication must remain isolated from snapshots, evidence, and logs. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P0-04-01 | Spoofing | connector operation | mitigate | Concrete connector_id/tool_name and capability-evidence hash. |
| T-P0-04-02 | Information Disclosure | managed authentication | mitigate | Contractual auth isolation and redaction scans. |
| T-P0-04-03 | Elevation of Privilege | fallback transport | mitigate | Fail closed; no browser or order-capable fallback. |
</threat_model>

<verification>
Run transport tests and the post-implementation discovery/verification commands; never create an external instruction in P0.
</verification>

<success_criteria>
P7 has one exact signed creator operation contract or remains blocked.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
