---
phase: P9-champion-challenger
plan: "02"
type: execute
wave: 2
depends_on: ["P9-01"]
files_modified:
  - options_copilot/AI_SPEC.md
  - options_copilot/runtime.py
  - options_copilot/api/app.py
  - options_copilot/frontend/app.js
  - tests/options_copilot/test_api_frontend.py
  - tests/options_copilot/test_policy_authority.py
  - tests/options_copilot/test_a_grade_creator_e2e.py
  - data/options_copilot/evidence/checkpoints/P9/a-grade-decision/
  - data/options_copilot/governance/a_grade_markers/
  - data/options_copilot/governance/promotion_authorities/
  - data/options_copilot/governance/rollback_authorities/
  - data/options_copilot/governance/policy_authority.sqlite3
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P9/a-grade-decision/
  - data/options_copilot/governance/a_grade_markers/
  - data/options_copilot/governance/promotion_authorities/
  - data/options_copilot/governance/rollback_authorities/
autonomous: false
requirements: [OC-LEARN-01, OC-RISK-01, OC-SEC-01, OC-VAL-01]
must_haves:
  truths:
    - "AI-SPEC and GUI expose policy/report/spec hashes, independent count, promotion, rollback, and separate A-grade status."
    - "The UI cannot generate a governance marker or auto-promote a Challenger."
    - "Only the explicit human CLI can append PromotionAuthority, proposal-bound A-grade authority, or RollbackAuthority; no automatic signing path exists."
    - "Human denial leaves A-grade locked; human approval writes one append-only verified P9 marker bound to the exact proposal/current-policy/cost/ranking basis."
    - "After human signing, creator E2E proves 10.00/10.01/15.00/15.01/20.00/20.01 plus stale/tampered/revoked authority behavior; before signing only NORMAL 10.00 is accepted."
    - "Promotion and rollback are reflected end to end in scenarios, ranking, approval, bridge intent, and creator validation after restart and concurrent-write tests."
  artifacts:
    - path: "options_copilot/AI_SPEC.md"
      provides: "Frozen learning framework, rubrics, guardrails, and monitoring contract"
    - path: "data/options_copilot/evidence/checkpoints/P9/a-grade-decision/"
      provides: "Human evaluation and A-grade decision evidence"
    - path: "data/options_copilot/governance/a_grade_markers/"
      provides: "Append-only approved A-grade markers"
    - path: "data/options_copilot/governance/promotion_authorities/"
      provides: "Human-signed immutable promotion authorities"
    - path: "data/options_copilot/governance/rollback_authorities/"
      provides: "Human-signed immutable rollback authorities"
    - path: "data/options_copilot/governance/policy_authority.sqlite3"
      provides: "Atomic durable current-policy and A-grade revocation ledger"
  key_links:
    - from: "options_copilot/runtime.py"
      to: "options_copilot/learning/policy_authority.py"
      via: "durable CurrentPolicyResolver and current RiskTierAuthority resolver injection"
      pattern: "current_policy_hash"
    - from: "tests/options_copilot/test_a_grade_creator_e2e.py"
      to: "options_copilot/bridge/creator_adapter.py"
      via: "post-human-signing full authority propagation and boundary matrix"
      pattern: "risk_authority_marker_hash"
---

<objective>
Expose transparent learning governance, perform separate human policy/risk decisions, and verify their live creator handoff.

Purpose: Make shadow/current-policy status visible without giving the UI or model signing authority, and prove signed promotion/rollback/A-grade reach every authorization boundary.
Output: AI-SPEC, runtime/API/GUI read models, human-signed authority artifacts, and post-signing creator E2E evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/learning/evaluation.py
@options_copilot/learning/markers.py
@options_copilot/learning/policy_authority.py
@options_copilot/learning/governance_cli.py
@options_copilot/runtime.py
@options_copilot/api/app.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P9-02-01: Define AI governance spec and resolver-backed read-only UI status</name>
  <read_first>
    - options_copilot/IMPLEMENTATION_RESEARCH.md
    - options_copilot/learning/evaluation.py
    - options_copilot/learning/markers.py
    - options_copilot/learning/policy_authority.py
    - options_copilot/runtime.py
    - options_copilot/api/app.py
  </read_first>
  <files>options_copilot/AI_SPEC.md, options_copilot/runtime.py, options_copilot/api/app.py, options_copilot/frontend/app.js, tests/options_copilot/test_api_frontend.py</files>
  <action>AI-SPEC fixes the immutable initial Champion policy reference/source-of-truth, Challenger model family, reference dataset, P8 independence spec, evaluation rubrics, guardrails, signed `PromotionAuthority`/`RollbackAuthority` schemas, proposal-bound A-grade schema, atomic rollback/revocation behavior, and monitoring. Replace the pre-P9 fallback instances in `RuntimeServices` with the durable `CurrentPolicyResolver` and current `RiskTierAuthority` resolver from P9-01; the already-wired DecisionPipeline, ScenarioEngine, PortfolioRanker, RankingStore, ApprovalStore, bridge intent, and creator adapter must receive one verified resolution each and fail closed on a head mismatch. Runtime/API/GUI display current Champion version/hash, policy-authority marker hash, Challenger version/hash, immutable initial policy hash, evaluation report hash, independent count, SHADOW_ONLY/DISCOVERY status, promotion/rollback evidence, risk-authority version/marker hash, and separate A_GRADE_LOCKED/APPROVED status. UI is read-only for authorities: it may request a human decision workflow but cannot sign, append, write, or synthesize an authority. Model or ordinary promotion self-report cannot change status.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_learning_evaluation.py tests/options_copilot/test_api_frontend.py &amp;&amp; node --check options_copilot/frontend/app.js</automated>
  </verify>
  <done>Learning governance and current resolver heads are transparent, fully injected, and no UI/model path can sign or auto-promote.</done>
  <acceptance_criteria>
    - Unsigned state visibly remains SHADOW_ONLY and A_GRADE_LOCKED
    - UI displays all required hashes and independent count
    - API/LLM promotion claims do not create a marker
    - AI-SPEC states the initial policy cannot be automatically mutated
    - Restart and resolver-head mismatch tests keep stale runtime output non-authorizable
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P9-02-02: Build locked-first authority-to-creator E2E harness</name>
  <read_first>
    - options_copilot/learning/policy_authority.py
    - options_copilot/risk/authorization.py
    - options_copilot/decision/pipeline.py
    - options_copilot/ranking/store.py
    - options_copilot/bridge/creator_adapter.py
    - options_copilot/bridge/coordinator.py
  </read_first>
  <files>tests/options_copilot/test_policy_authority.py, tests/options_copilot/test_a_grade_creator_e2e.py</files>
  <action>Create an end-to-end test harness spanning CurrentPolicyResolver -> ScenarioEngine -> DecisionPipeline -> PortfolioRanker -> RankingStore -> ApprovalStore -> bridge intent/store -> `prepare_and_validate_payload` -> RecordingCreator. Before any human authority is supplied, run only the locked-first cases: NORMAL 10.00 passes; 10.01, 15.00, 15.01, 20.00, and 20.01 all reject with zero reservation/call. Use immutable pre-signed test fixtures only for schema/verifier unit cases; tests must not call any production signing verb or manufacture a production authority. Add restart, ledger tamper, promotion/current-policy mismatch, concurrent promotion/rollback, stale ranking, stale approval, and atomic rollback/revocation fixtures. Mark the valid human-authority creator branch so it is excluded here and executed only after Task P9-02-03 records an explicit human decision.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_policy_authority.py tests/options_copilot/test_a_grade_creator_e2e.py -m "not human_authority"</automated>
  </verify>
  <done>The E2E harness proves the unsigned/locked state and cannot exercise an A-grade permitted creator path before human signing.</done>
  <acceptance_criteria>
    - Locked-first matrix covers all six exact boundaries and zero-call rejection evidence
    - Every layer carries current_policy_version/hash, policy_authority_marker_hash, and risk_authority_marker_hash
    - Restart/tamper/concurrent promotion-rollback tests fail closed
    - Production sign-promotion/sign-a-grade/sign-rollback are never invoked by a test
  </acceptance_criteria>
</task>

<task type="checkpoint:decision" gate="blocking">
  <name>Task P9-02-03: Sign promotion, A-grade, and rollback decisions only by explicit human action</name>
  <read_first>
    - options_copilot/AI_SPEC.md
    - options_copilot/learning/evaluation.py
    - options_copilot/learning/markers.py
    - options_copilot/learning/policy_authority.py
    - options_copilot/learning/governance_cli.py
    - options_copilot/governance/independence_spec.v1.json
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P9/a-grade-decision/, data/options_copilot/governance/a_grade_markers/, data/options_copilot/governance/promotion_authorities/, data/options_copilot/governance/rollback_authorities/, data/options_copilot/governance/policy_authority.sqlite3</files>
  <action>Post-implementation only: present the frozen evaluation report, current Champion and immutable initial-policy hashes, policy-authority head, Challenger hash, exact proposal/candidate and canonical ranking-basis hashes, signed cost version/hash, reference dataset, P8 independence spec, signed risk contract, calibration, tail loss, drawdown, scenario breakdown, and rollback plan. Require independent explicit human decisions: Challenger promotion or keep shadow, and `APPROVE_A_GRADE` for this exact proposal/ranking basis or `DENY/KEEP_LOCKED`; promotion never implies A-grade. The human alone invokes `python -m options_copilot.learning.governance_cli sign-promotion ...`, `sign-a-grade ...`, or when actually choosing rollback `sign-rollback ...`, with canonical inputs and the P9 authority database/output directories shown above. Immediately run the matching `verify-promotion-decision`, `verify-a-grade-decision`, or `verify-rollback-decision` command and record stdout/stderr hashes, actor, signed_at, authority sequence/head, and output artifact hash under the checkpoint prefix. Only `APPROVE_A_GRADE` writes one proposal-bound append-only marker; denial writes decision evidence only. A rollback command must atomically show the prior signed policy head and revoked A-grade marker in one committed verifier result. Missing/failed rubric evidence forces denial. The implementer, model, runtime, UI, API, scheduler, and tests never sign; no `--yes`, environment flag, or automatic signing mode exists, and 30 scenes never auto-approve.</action>
  <verify>
    <automated>python -m options_copilot.learning.governance_cli verify-promotion-decision --path data/options_copilot/evidence/checkpoints/P9/a-grade-decision/latest-promotion.json --authority-db data/options_copilot/governance/policy_authority.sqlite3 --json &amp;&amp; python -m options_copilot.learning.governance_cli verify-a-grade-decision --path data/options_copilot/evidence/checkpoints/P9/a-grade-decision/latest-a-grade.json --authority-db data/options_copilot/governance/policy_authority.sqlite3 --json</automated>
  </verify>
  <done>Human decisions are independently signed/verified, rollback is available only as an explicit human command, and only a current proposal-bound APPROVE_A_GRADE marker can unlock 10.01-15.00 percent.</done>
  <acceptance_criteria>
    - Decision evidence records actor/time, authority sequence/head, and every proposal/policy/cost/ranking/evaluation hash
    - Promotion, A-grade, and rollback artifacts/commands are separate
    - Denial creates no A-grade marker
    - Approval marker passes verifier; actual rollback atomically verifies prior head plus marker revocation
    - No automatic or non-human signing caller exists
  </acceptance_criteria>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task P9-02-04: Verify post-signing A-grade creator and rollback behavior</name>
  <read_first>
    - tests/options_copilot/test_a_grade_creator_e2e.py
    - tests/options_copilot/test_policy_authority.py
    - data/options_copilot/evidence/checkpoints/P9/a-grade-decision/
    - data/options_copilot/governance/policy_authority.sqlite3
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P9/a-grade-decision/</files>
  <action>Post-implementation only and strictly after Task P9-02-03: if the human signed `APPROVE_A_GRADE`, run the marked E2E suite against that verified authority database/decision path. It must prove NORMAL 10.00 passes without A-grade; 10.01 and 15.00 pass only for the exact current signed proposal/policy/cost/ranking basis; 15.01, 20.00, and 20.01 always reject; stale, tampered, wrong-binding, revoked, and rolled-back markers produce zero reservation and creator calls. It must also prove promotion changes ScenarioEngine/DecisionPipeline/PortfolioRanker/ranking outputs to the signed current policy, every ranking/approval/bridge-intent/payload carries identical current-policy and authority-marker hashes, restart preserves the head, and an atomic rollback in an isolated copy immediately restores the prior policy/revokes A-grade so old rankings and approvals reject. Record the command, verified input hashes, result hash, and human actor under the checkpoint prefix. If the human denied A-grade, run the same suite in locked mode, record that the permitted 10.01/15.00 branch is intentionally not accepted, and keep every A-grade rollout stage blocked; denial never manufactures a signing fixture.</action>
  <verify>
    <automated>$env:OPTIONS_COPILOT_P9_AUTHORITY_DB='data/options_copilot/governance/policy_authority.sqlite3'; $env:OPTIONS_COPILOT_P9_DECISION_PATH='data/options_copilot/evidence/checkpoints/P9/a-grade-decision/latest-a-grade.json'; python -m pytest -q tests/options_copilot/test_a_grade_creator_e2e.py tests/options_copilot/test_policy_authority.py -m human_authority; $exitCode=$LASTEXITCODE; Remove-Item Env:OPTIONS_COPILOT_P9_AUTHORITY_DB,Env:OPTIONS_COPILOT_P9_DECISION_PATH -ErrorAction SilentlyContinue; exit $exitCode</automated>
  </verify>
  <done>Post-human-decision evidence proves the exact A-grade creator boundary or records a locked denial with no A-grade rollout acceptance.</done>
  <acceptance_criteria>
    - Approved branch proves 10.00/10.01/15.00/15.01/20.00/20.01 exactly
    - Stale, tampered, wrong-binding, revoked, and rolled-back authority yields zero creator calls
    - Promotion/rollback current-policy identities propagate through every named output and stale approval rejects
    - Denied branch remains A_GRADE_LOCKED and cannot satisfy an A-grade rollout checkpoint
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Evaluation report to human decision | Model evidence is advisory until independently signed. |
| Human CLI to authority ledger | Only explicit signed commands may append promotion, A-grade, or rollback authority. |
| Governance resolver to runtime/UI/creator | Verified current heads are displayed/consumed but cannot be created by those consumers. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P9-02-01 | Spoofing | UI governance status | mitigate | Server-side marker verifier and hash display. |
| T-P9-02-02 | Elevation of Privilege | model/UI signer | mitigate | Human-only governance signing path. |
| T-P9-02-03 | Repudiation | A-grade decision | mitigate | Separate actor/time/report-bound evidence and marker. |
| T-P9-02-04 | Tampering | stale ranking after rollback | mitigate | Atomic resolver/revocation transaction plus end-to-end stale approval/creator tests. |
</threat_model>

<verification>
Run UI/governance and locked-first tests, perform separate human promotion/A-grade/rollback decisions, then run the post-signing creator/rollback E2E checkpoint.
</verification>

<success_criteria>
Production promotion, proposal-specific A-grade, and rollback are separately human-governed, atomically resolved, end-to-end verified, and never automatic.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
