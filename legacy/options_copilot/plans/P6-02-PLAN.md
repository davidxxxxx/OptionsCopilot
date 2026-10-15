---
phase: P6-runtime-gui
plan: "02"
type: execute
wave: 2
depends_on: ["P6-01"]
files_modified:
  - options_copilot/UI_SPEC.md
  - options_copilot/api/app.py
  - options_copilot/frontend/index.html
  - options_copilot/frontend/app.js
  - options_copilot/frontend/styles.css
  - tests/options_copilot/test_api_frontend.py
  - tests/options_copilot/test_api.py
  - tests/options_copilot/test_frontend_contract.py
  - tests/options_copilot/test_learning_frontend.py
  - tests/options_copilot/test_news_option_frontend.py
  - tests/options_copilot/test_news_reaction_frontend.py
  - tests/options_copilot/test_official_calendar_api_projection.py
  - tests/options_copilot/test_p6_api_gui.py
  - data/options_copilot/evidence/checkpoints/P6/browser/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P6/browser/
autonomous: false
requirements: [OC-STRAT-01, OC-EVENT-01, OC-LEARN-01, OC-EXIT-01, OC-BRIDGE-01, OC-OPS-01, OC-VAL-01]
must_haves:
  truths:
    - "GUI visibly distinguishes readiness, GLD management, Top 3, evidence/exit plan, and final review."
    - "Only rank 1 has an authorization action; ranks 2-3 contain no action handler."
    - "Deep link is unavailable before READY_FOR_IBKR_REVIEW and NLV is labeled observation-only."
    - "News and calendar use a separate read-only view; every event, reaction, option-impact/preselection row, and official-calendar row remains SUPPORTING_ONLY with no approval or instruction handler."
    - "The GUI faithfully exposes independent Top-10 lineage/coverage, post-event reaction state, and durable shadow-learning status without implying production authority or completion of the unavailable open-repricing producer, live reaction provider, P8 evaluation, or human promotion/rollback."
  artifacts:
    - path: "options_copilot/UI_SPEC.md"
      provides: "Locked five-region UI and authority contract"
    - path: "data/options_copilot/evidence/checkpoints/P6/browser/"
      provides: "Desktop/mobile screenshots and human/API hash evidence"
  key_links:
    - from: "options_copilot/frontend/app.js"
      to: "options_copilot/api/app.py"
      via: "polling immutable read models and rank-one challenge endpoint"
      pattern: "VIEW_ONLY"
---

<objective>
Implement and visibly verify the explanatory rank-one-only GUI plus the separate read-only news/calendar workbench.

Purpose: Make degraded state, evidence, exit rules, and review-only authority obvious to the user.
Output: UI spec, authority/news/calendar API projections, frontend, tests, and desktop/mobile browser evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/api/app.py
@options_copilot/frontend/index.html
@options_copilot/frontend/app.js
@options_copilot/IMPLEMENTATION_RESEARCH.md
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P6-02-01: Implement five-region authority-aware UI</name>
  <read_first>
    - options_copilot/api/app.py
    - options_copilot/frontend/index.html
    - options_copilot/frontend/app.js
    - options_copilot/frontend/styles.css
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/UI_SPEC.md, options_copilot/frontend/index.html, options_copilot/frontend/app.js, options_copilot/frontend/styles.css, tests/options_copilot/test_api_frontend.py, tests/options_copilot/test_learning_frontend.py</files>
  <action>Lock and implement five regions: Readiness, Position Management, Top 3, Evidence and Exit Plan, and Final Review. Display scan/ranking IDs, quote age/skew, secdef, maximum loss/risk fraction, Strategy NAV, signed cost/policy versions, primary versus supporting evidence, NO_TRADE reasons, and complete exit plan. Management and entry use different wording/state. Rank 1 alone renders the challenge action; ranks 2-3 render VIEW_ONLY and have no hidden or disabled handler. Deep link appears only for READY_FOR_IBKR_REVIEW and must omit query/fragment. Label account NLV `observable/reconciliation only`, never risk base. The learning region consumes the durable shadow summary, displays champion/challenger, challenger-scoped independent samples, COLLECTING/DISCOVERY, record count, and ledger integrity, and always states SHADOW_ONLY plus external human approval; missing or malicious fields fall back visibly without enabling promotion or production mutation.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_api_frontend.py tests/options_copilot/test_learning_frontend.py &amp;&amp; node --check options_copilot/frontend/app.js</automated>
  </verify>
  <done>The UI is explanatory, responsive, and contains no client-side rank or authority bypass.</done>
  <acceptance_criteria>
    - DOM tests find one rank-one action and zero rank-two/rank-three handlers
    - Management and entry text/state are distinct
    - DEGRADED state disables challenge and hides deep link
    - Desktop/mobile layout has no horizontal overflow
    - Learning display is SHADOW_ONLY, fail-safe on partial fields, and contains no promotion/approval/order control
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P6-02-02: Expose a separate read-only news and official-calendar workbench</name>
  <read_first>
    - options_copilot/api/app.py
    - options_copilot/news_runtime.py
    - options_copilot/UI_SPEC.md
    - options_copilot/frontend/index.html
    - options_copilot/frontend/app.js
    - options_copilot/frontend/styles.css
  </read_first>
  <files>options_copilot/api/app.py, options_copilot/frontend/index.html, options_copilot/frontend/app.js, options_copilot/frontend/styles.css, tests/options_copilot/test_api.py, tests/options_copilot/test_frontend_contract.py, tests/options_copilot/test_news_option_frontend.py, tests/options_copilot/test_news_reaction_frontend.py, tests/options_copilot/test_official_calendar_api_projection.py, tests/options_copilot/test_p6_api_gui.py</files>
  <action>Add provider-agnostic `/api/news` and `/api/calendar` read-only projections with strict field allowlists, bounded collection sizes, public-HTTPS-only links, four timestamps, provider health, structured provenance, official source precision/timezone/window membership, and fixed SUPPORTING_ONLY/approval-false/instruction-false/order-false fields. Project independent Top-10 rows only when their run/head/row lineage validates one-to-one, expose exact requested/available/open coverage, and preserve the explicit `OPEN_REPRICE_PRODUCER_UNAVAILABLE` state rather than treating typed store observations or empty arrays as a completed producer. Project post-event reaction provider status/coverage and only compact identity-bound reaction fields; discard internal provenance, credentials, model results, broker/order fields, and provider-claimed authority. Missing/invalid/misbound reaction data becomes UNAVAILABLE/CONFLICTED plus `NO_TRADE` while the official calendar fact remains visible. The UI must expose a dedicated News navigation/view with discovery, ledger-backed pre-market Top 10, open-repriced observations, action-observation Top 3, transparent event-impact/tradability/combined scores, filters, evidence/conflicts/counter-evidence, conditional contract legs, hard blockers, latency, and an official calendar two-week window. Each calendar event shows current reaction stage, expectation versus official actual, surprise, market-reaction window, option re-evaluation, and explicit NO_TRADE reasons with unavailable placeholders and no inferred values. Model unavailable, Jin10 disabled, missing open producer/reaction provider, source conflict, stale/incomplete IBKR bindings, or degraded official source remains visible and fails closed without an approval, review-instruction, creator, or order handler.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_api.py tests/options_copilot/test_frontend_contract.py tests/options_copilot/test_news_option_frontend.py tests/options_copilot/test_news_reaction_frontend.py tests/options_copilot/test_official_calendar_api_projection.py tests/options_copilot/test_p6_api_gui.py &amp;&amp; node --check options_copilot/frontend/app.js</automated>
  </verify>
  <done>The browser shows transparent news/calendar research and contract-level observations without adding authorization, instruction, Creator, or order authority.</done>
  <acceptance_criteria>
    - API strips unknown/provider-claimed authority fields and derives pool counts/ranks from validated rows
    - News detail shows four timestamps, latency, provenance/conflicts, three named scores, classifier/model state, and SUPPORTING_ONLY authority
    - Official calendar preserves exact-versus-date-only timing, timezone, source hashes/URLs, and server-provided window membership
    - Reaction provider status/coverage and every compact event stage/evidence/reason remain identity-bound, sanitized, unavailable-aware, and SUPPORTING_ONLY
    - Top-10 coverage/lineage is visible while the missing open-repricing producer remains explicitly UNAVAILABLE
    - Premarket/open Top 10 and action-observation Top 3 contain no approval, instruction, or order event handler
    - Creator unavailable and Jin10 disabled/degraded states remain visible and non-actionable
  </acceptance_criteria>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task P6-02-03: Verify desktop and mobile browser result</name>
  <read_first>
    - options_copilot/UI_SPEC.md
    - options_copilot/api/app.py
    - options_copilot/frontend/app.js
    - scripts/start_options_copilot.ps1
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P6/browser/</files>
  <action>Post-implementation only: start the isolated GUI with `pwsh.exe -NoProfile -File scripts/start_options_copilot.ps1 -Background -OpenBrowser`, capture `/api/readiness`, `/api/learning`, `/api/news`, and `/api/calendar`, and use real Chrome at desktop and mobile widths to verify all five authority regions plus the separate News view. Save exact screenshots and a human verification record. Confirm rank 1 has the sole action, ranks 2-3 are VIEW_ONLY without action wiring, GLD uses management wording, degraded state disables actions/deep link, NLV is not risk base, and review-only/false submission status is visible. Confirm the learning surface is SHADOW_ONLY and shows its current ledger integrity/counts without a promotion control. In the News view confirm official-calendar provenance/precision, reaction provider status/coverage, stage/expectation/actual/surprise/market-window/option-re-evaluation/NO_TRADE placeholders, three named scores, ledger-backed Top-10 coverage/lineage, the explicit unavailable open-repricing producer, SUPPORTING_ONLY authority, model/Jin10/provider health, and zero approval/instruction/order controls. This is not creator, provider-contract, live reaction-provider, P8 evaluation/promotion, open-repricing-producer, or broker live acceptance.</action>
  <verify>
    <automated>Invoke-RestMethod http://127.0.0.1:8891/api/readiness | ConvertTo-Json -Depth 12</automated>
  </verify>
  <done>Human browser evidence matches the same-run API hash on desktop and mobile, or P7 remains blocked.</done>
  <acceptance_criteria>
    - Evidence includes manifest, API readiness, desktop image, mobile image, and human verification hashes
    - UI and API state hashes match
    - No horizontal overflow or rank 2/3 action exists
    - News/calendar screenshots match their same-run API projections and contain no approval/instruction/order handler
    - Learning summary and Top-10/reaction unavailable states match their same-run API projections without overstating completion
    - Actor and signed_at are recorded
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| API read models to browser | User-visible content must faithfully represent server authority. |
| Browser action to API | Client controls are untrusted and server gates remain decisive. |
| News/provider/model fields to browser | Supporting-only research must not render an approval, instruction, Creator, or order control. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P6-02-01 | Spoofing | GUI rank/status | mitigate | Same-run API hash and server-side rank gate. |
| T-P6-02-02 | Information Disclosure | review link | mitigate | READY-only, allowlisted path, no query/fragment. |
| T-P6-02-03 | Elevation of Privilege | news Top-10/Top-3/reaction row | mitigate | Strict lineage/field projections plus zero news action handlers and explicit SUPPORTING_ONLY labels. |
| T-P6-02-04 | Spoofing | learning/reaction status | mitigate | Allowlisted read models, fail-closed placeholders, and no promotion/approval/order controls. |
</threat_model>

<verification>
Run authority/learning/news/calendar API and frontend tests plus syntax check, then capture named desktop/mobile evidence.
</verification>

<success_criteria>
The visible user outcome communicates exact readiness, risk, rank, and review-only authority while news/calendar remains a separate non-actionable supporting-evidence surface.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
