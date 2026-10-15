---
phase: P7-external-creator
plan: "02"
type: execute
wave: 2
depends_on: ["P7-01"]
files_modified:
  - options_copilot/README.md
  - data/options_copilot/evidence/checkpoints/P7/creator-live/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P7/creator-live/
autonomous: false
requirements: [OC-SAFE-01, OC-BRIDGE-01, OC-DATA-01, OC-SEC-01, OC-VAL-01]
must_haves:
  truths:
    - "P7 cannot begin without a valid signed concrete creator transport contract."
    - "One live checkpoint creates at most one pending review instruction and reconciles uncertainty without retry."
    - "The human, not the application or agent, performs the final confirm or reject action inside IBKR."
  artifacts:
    - path: "data/options_copilot/evidence/checkpoints/P7/creator-live/"
      provides: "Approved intent, reservation, connector request hash, review result, and human reconciliation evidence"
    - path: "options_copilot/README.md"
      provides: "Exact post-implementation review and final-human-confirmation runbook"
  key_links:
    - from: "data/options_copilot/evidence/checkpoints/P7/creator-live/"
      to: "options_copilot/governance/creator_transport_contract.v1.json"
      via: "signed transport version/hash in manifest"
      pattern: "transport_contract_hash"
---

<objective>
Perform one controlled live review-instruction checkpoint and record the user's final IBKR decision boundary.

Purpose: Validate the real transport without ever automating the final broker confirmation.
Output: Updated runbook and immutable live creator/human reconciliation evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/README.md
@options_copilot/governance/creator_transport_contract.v1.json
@options_copilot/bridge/creator_adapter.py
@options_copilot/bridge/store.py
</context>

<tasks>

<task type="auto">
  <name>Task P7-02-01: Document exact live-review and final-human boundary</name>
  <read_first>
    - options_copilot/README.md
    - options_copilot/governance/creator_transport_contract.v1.json
    - options_copilot/bridge/__main__.py
    - options_copilot/bridge/store.py
  </read_first>
  <files>options_copilot/README.md</files>
  <action>Document the exact post-implementation command, prerequisites, evidence prefix, one-attempt semantics, UNKNOWN reconciliation procedure, and selected connector operation contract. State that the command creates a review instruction only. The user must inspect all legs, quantity, side, limit, costs, maximum loss, and status inside IBKR and personally choose confirm or reject. The application, agent, browser automation, and local scripts never perform that final action. Do not include connector credentials or any secret material.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_creator_adapter.py tests/options_copilot/test_api_frontend.py</automated>
  </verify>
  <done>The runbook gives an exact review-only command and an unambiguous human-final-confirmation boundary.</done>
  <acceptance_criteria>
    - README labels every command post-implementation only
    - README states one attempt and UNKNOWN no-retry behavior
    - README assigns final confirm/reject exclusively to the human inside IBKR
    - No credential or secret appears
  </acceptance_criteria>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task P7-02-02: Create one live review instruction and reconcile</name>
  <read_first>
    - options_copilot/governance/creator_transport_contract.v1.json
    - options_copilot/bridge/creator_adapter.py
    - options_copilot/bridge/store.py
    - options_copilot/README.md
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P7/creator-live/</files>
  <action>Post-implementation only: first verify the signed transport contract. Select a fresh, hash-bound, double-confirmed, unexpired rank-one NORMAL entry at no more than 10.00 percent, or while GLD is open a P2 strict CLOSE_ALL/REDUCE_RISK approval. Run exactly `python -m options_copilot.bridge create-review-checkpoint --approval-id APPROVAL_ID --json --evidence-dir data/options_copilot/evidence/checkpoints/P7/creator-live`. Require coherent quotes at most 5 seconds old and authoritative zero working orders/instructions. Human-check the pending review instruction in IBKR against the approved intent and save reconciliation evidence. If outcome is uncertain, record UNKNOWN_OUTCOME and do not rerun.</action>
  <verify>
    <automated>python -m options_copilot.bridge status --approval-id APPROVAL_ID --json</automated>
  </verify>
  <done>One real review-only creator attempt is reconciled, or terminal UNKNOWN evidence is recorded without retry.</done>
  <acceptance_criteria>
    - Connector call count is exactly one
    - Request hash matches approval, rank/transition, reserve snapshot, cost, policy, and transport hashes
    - Result is pending review with false local submission/transmission status, or UNKNOWN_OUTCOME
    - Evidence has no credential or deep-link query/fragment
  </acceptance_criteria>
</task>

<task type="checkpoint:human-action" gate="blocking">
  <name>Task P7-02-03: Record final IBKR human decision</name>
  <read_first>
    - options_copilot/README.md
    - data/options_copilot/evidence/checkpoints/P7/creator-live/
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P7/creator-live/</files>
  <action>Post-implementation only: the human opens the allowlisted IBKR review deep link, independently checks legs, sides, ratios, quantity, limit, costs, maximum loss, and account state, then personally confirms or rejects inside IBKR. The agent and application must not click, automate, or invoke the final broker action. Record only actor, signed_at, instruction ID, decision=`CONFIRMED_BY_HUMAN` or `REJECTED_BY_HUMAN`, post-decision reconciliation hash, and source screenshot/read-model hash. If the user does not act or outcome cannot be reconciled, record `HUMAN_DECISION_UNCONFIRMED` and keep rollout blocked.</action>
  <verify>
    <automated>python -m options_copilot.bridge status --approval-id APPROVAL_ID --json</automated>
  </verify>
  <done>The user's final IBKR decision and reconciliation are recorded without any automated final action.</done>
  <acceptance_criteria>
    - Actor and signed_at are present
    - Decision is one of the three explicit human outcomes
    - Reconciliation hash matches the current IBKR instruction read model
    - No local final-confirmation callable or automation exists
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Local review result to IBKR UI | A review-only deep link exposes a pending human decision. |
| Human decision to reconciliation evidence | The final broker action occurs exclusively in IBKR and is recorded afterward. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P7-02-01 | Spoofing | review deep link | mitigate | Signed transport host/path and screenshot/read-model reconciliation. |
| T-P7-02-02 | Repudiation | final decision | mitigate | Human actor/time plus post-decision instruction hash. |
| T-P7-02-03 | Elevation of Privilege | final confirmation | mitigate | No local callable or browser automation; human-only checkpoint. |
</threat_model>

<verification>
Run contract/status checks and complete the live review plus separate human-final-decision checkpoints.
</verification>

<success_criteria>
The system proves one real review-only handoff and preserves exclusive human final IBKR authority.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
