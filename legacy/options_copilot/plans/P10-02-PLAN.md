---
phase: P10-production-readiness
plan: "02"
type: execute
wave: 2
depends_on: ["P10-01"]
files_modified:
  - data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/
  - data/options_copilot/evidence/checkpoints/P10/rollout/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/
  - data/options_copilot/evidence/checkpoints/P10/rollout/
autonomous: false
requirements: [OC-OPS-01, OC-VAL-01, OC-SAFE-01, OC-SCAN-01, OC-DATA-01]
must_haves:
  truths:
    - "Protected processes/ports and Tailscale 443-to-8878 remain unchanged while local and remote Options Copilot health pass."
    - "The separate 8891 route is re-queried at checkpoint time: an exact pre-existing mapping is adopted without mutation, and an absent mapping is created only after explicit human approval."
    - "A failed check can remove only the exact 8891 mapping proven created by this checkpoint; an adopted/pre-existing mapping and root/443 are never removed or reset."
    - "Staged rollout advances only through named evidence and explicit human decisions; no elapsed-time auto-promotion exists."
    - "Every rollout stage remains review-only and human-final-confirmed in IBKR."
  artifacts:
    - path: "data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/"
      provides: "Before/after process, listener, raw route, adopt/create decision, ownership, restoration, and local/remote health evidence"
    - path: "data/options_copilot/evidence/checkpoints/P10/rollout/"
      provides: "Evidence-linked staged rollout decisions"
  key_links:
    - from: "data/options_copilot/evidence/checkpoints/P10/rollout/"
      to: "data/options_copilot/evidence/checkpoints/P7/creator-live/"
      via: "required named review-only and human reconciliation evidence"
      pattern: "P7-CREATOR-LIVE"
    - from: "data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/route-decision.json"
      to: "data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/route-ownership.json"
      via: "creation-only before/after diff and human approval; adopted mapping has no removal ownership"
      pattern: "CREATED_BY_P10_02_01"
---

<objective>
Re-query and safely adopt or human-create the separate 8891 route, verify process/remote safety, and make the evidence-gated staged rollout decision.

Purpose: Production readiness is decided from real named evidence while preserving all existing services, the protected root route, and any pre-existing 8891 mapping.
Output: Immutable route ownership/health and rollout checkpoint artifacts only.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/operations/runbook.md
@options_copilot/operations/readiness.py
@scripts/start_options_copilot.ps1
@options_copilot/AI_SPEC.md
</context>

<tasks>

<task type="checkpoint:human-action" gate="blocking">
  <name>Task P10-02-01: Re-query, adopt or explicitly create, and verify the remote 8891 route</name>
  <read_first>
    - options_copilot/operations/runbook.md
    - options_copilot/operations/readiness.py
    - scripts/start_options_copilot.ps1
    - options_copilot/README.md
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/</files>
  <action>POST-IMPLEMENTATION ONLY — HUMAN ACTION. Treat the reported live state as drift-prone and re-query it; no implementation task or automated test may run a Tailscale command. Set `$p10EvidenceRoot='G:\quantumtrading\data\options_copilot\evidence\checkpoints\P10\process-ports-tailscale'`, create `before`, `after`, `health`, and `restore` below it, then capture `tailscale serve status --json` verbatim to `before/tailscale-serve-status.json`, `Get-NetTCPConnection -State Listen` plus owning PID/process creation-time/executable/command line to `before/listeners-processes.json`, protected 8766/8788/8878 identities to `before/protected-identities.json`, and broker/account/position/working-order/unsubmitted-instruction state to `before/broker-reconciliation.json`. Run `python -m options_copilot.operations.readiness classify-route --tailscale-status-file "$p10EvidenceRoot\before\tailscale-serve-status.json" --listener-file "$p10EvidenceRoot\before\listeners-processes.json" --expected-url https://davidxu.tail6885a3.ts.net:8891/ --expected-target http://127.0.0.1:8891 --protected-url https://davidxu.tail6885a3.ts.net/ --protected-target http://127.0.0.1:8878 --output "$p10EvidenceRoot\route-decision.json"`. If the exact tailnet-only 8891 mapping already exists, record `ADOPTED_PREEXISTING`, the before hashes, actor/time, and no removal ownership; do not run any serve mutation. If it is absent, stop and request a separate explicit human approval that names the exact mapping. Only after approval may the human run `tailscale serve --bg --https=8891 http://127.0.0.1:8891`; never use `reset`, never target 443/root, and never replace another handler. Immediately capture `after/tailscale-serve-status.json` and create `route-ownership.json` through `python -m options_copilot.operations.readiness record-created-route --before ... --after ... --decision ... --created-by P10-02-01 --output ...`; it must bind the before/after hashes, exact handler key/target, approval hash, actor/time, and nonce. Run the ordinary launcher only if 8891 has no listener; require its existing fail-on-listener behavior and do not force restart here. Verify `http://127.0.0.1:8891/health`, `/api/bootstrap`, `/api/positions`, `/api/candidates`, `/api/learning` and the same five paths at `https://davidxu.tail6885a3.ts.net:8891`; save status/body hashes under `health/local` and `health/remote`. Also verify the protected root URL remains healthy and its status mapping is unchanged. Capture final listeners/processes, protected identities, broker reconciliation, and raw Tailscale status under `after`. On any route/remote-health failure, never remove an `ADOPTED_PREEXISTING`, pre-existing, ambiguous, or drifted mapping. Only when `python -m options_copilot.operations.readiness verify-route-ownership --marker "$p10EvidenceRoot\route-ownership.json" --before ... --current ... --created-by P10-02-01 --json` proves this task created the exact handler may the human run targeted `tailscale serve --https=8891 off`; then capture `restore/tailscale-serve-status.json` and prove byte-equivalent handler state to the before snapshot. A generic reset is forbidden. Finish with no-leftover listener/process checks and human signature.</action>
  <verify>
    <automated>python -m options_copilot.operations.readiness verify-route-evidence --evidence-dir data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale --expected-url https://davidxu.tail6885a3.ts.net:8891/ --expected-target http://127.0.0.1:8891 --protected-url https://davidxu.tail6885a3.ts.net/ --protected-target http://127.0.0.1:8878 --require-read-paths /health,/api/bootstrap,/api/positions,/api/candidates,/api/learning --json</automated>
  </verify>
  <done>Before/after evidence proves the 8891 route was adopted without mutation or human-created with exclusive rollback ownership, remote read paths are healthy, and protected state/no-leftovers are unchanged.</done>
  <acceptance_criteria>
    - Protected PID/command hashes and 443-to-8878 route are identical before/after
    - Exact pre-existing 8891 mapping is ADOPTED_PREEXISTING with zero serve mutation/removal ownership
    - Absent 8891 mapping requires a separately recorded human approval before the exact :8891 create command
    - Local and actual remote 8891 health plus all four read APIs return 200 and matching safe schemas
    - Unknown listener stop count is zero
    - Failure removes only a mapping with valid CREATED_BY_P10_02_01 proof and restores the exact before handler state; adopted/root mappings have zero off/reset calls
    - Human actor signs remote verification and no-leftover result
  </acceptance_criteria>
  <resume-signal>Type approved after the evidence verifier passes, or provide the failing evidence path; if creation is needed, first explicitly approve the exact separate :8891 mapping.</resume-signal>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P10-02-02: Sign evidence-gated staged rollout</name>
  <read_first>
    - options_copilot/operations/readiness.py
    - options_copilot/operations/runbook.md
    - options_copilot/AI_SPEC.md
    - data/options_copilot/evidence/checkpoints/P7/creator-live/
    - data/options_copilot/evidence/checkpoints/P9/a-grade-decision/
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P10/rollout/</files>
  <action>Post-implementation only: present Offline Replay, Live Read, Shadow, and Human Review NORMAL_10 stages in order, each with required P0-P9 named evidence hashes plus the verified P10 route decision (`ADOPTED_PREEXISTING` or `CREATED_BY_P10_02_01`), local/remote read-path evidence, protected root identity, and any restoration evidence. Require explicit human approval for each stage and never use elapsed time as promotion. While GLD remains, Human Review permits only P2 management. A-grade Review is a separate optional stage only with a current valid proposal-bound P9 marker and the post-signing P9 creator E2E evidence; otherwise A_GRADE_LOCKED. Any Critical/High threat, authority/workspace failure, route ownership ambiguity, protected-root drift, live checkpoint failure, contract/hash drift, or UNKNOWN_OUTCOME blocks advancement. Every stage remains review-only, one creator attempt, and human final IBKR confirmation.</action>
  <verify>
    <automated>python -m options_copilot.operations.readiness --json --evidence-dir data/options_copilot/evidence/checkpoints/P10/rollout</automated>
  </verify>
  <done>Human-signed rollout stops at the highest fully evidenced review-only stage.</done>
  <acceptance_criteria>
    - Each stage records report hashes, actor, signed_at, and prior-stage evidence links
    - No fixed-day automatic advancement exists
    - NORMAL remains 10.00 percent and A-grade requires current P9 marker
    - Rollout consumes route adoption/creation ownership evidence and never treats localhost-only health as remote acceptance
    - Final IBKR authority remains human-only at every stage
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Runtime process/route evidence to human | Live operational state must be independently reviewed. |
| Human route command to ownership evidence | Only an explicitly approved creation can acquire targeted rollback ownership. |
| Named phase evidence to rollout | Signed stage decisions consume immutable prerequisite hashes. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P10-02-01 | Denial of Service | protected services | mitigate | Before/after exact identities and remote-route checks. |
| T-P10-02-02 | Elevation of Privilege | rollout | mitigate | Evidence-gated human stages and permanent review-only boundary. |
| T-P10-02-03 | Repudiation | stage approval | mitigate | Actor/time and linked immutable hashes. |
| T-P10-02-04 | Tampering | adopted route mislabeled owned | mitigate | Raw before snapshot, exact handler diff, human approval hash, nonce, and pure verifier. |
</threat_model>

<verification>
Perform the autonomous:false post-implementation route adopt/create/process/remote checkpoint, then the evidence-gated staged rollout decision; no Tailscale command runs in implementation or automated tests.
</verification>

<success_criteria>
Production readiness preserves all existing services/routes and advances only as far as signed named evidence allows.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
