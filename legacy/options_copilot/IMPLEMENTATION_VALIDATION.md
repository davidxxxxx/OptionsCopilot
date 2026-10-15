---
artifact_type: PROGRAM_VALIDATION_STRATEGY
program: options-copilot-productionization
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-08-03
executable_plan_count: 29
task_count: 72
requirements_count: 17
---

# Options Copilot Implementation Validation Strategy

> All implementation, test, CLI, browser, broker, provider, creator, process, port, and Tailscale commands in this document are **POST-IMPLEMENTATION ONLY** unless a row explicitly says **planning-artifact validation**. This document does not claim those modules, commands, or live capabilities exist today.

## 1. Test infrastructure and sampling contract

| Property | Contract |
|---|---|
| Unit/integration runner | `python -m pytest` |
| Existing fast baseline | `python -m pytest -q tests/options_copilot` |
| Syntax | `python -m compileall -q options_copilot`; `node --check options_copilot/frontend/app.js` |
| Planning structure | `gsd-sdk query frontmatter.validate PLAN --schema plan`; `gsd-sdk query verify.plan-structure PLAN` |
| Task feedback | Run the task row's focused command after its RED/GREEN/refactor cycle. |
| Local phase-wave feedback | Run the plan aggregate command before a dependent local wave starts. |
| Program-wave feedback | Run all completed phase aggregates plus authority/workspace guard before the next macro wave. |
| Live/manual acceptance | Named immutable evidence prefix plus actor/time/hash; CLI exit alone never suffices. |
| Maximum task feedback target | Focused automated command should finish within 60 seconds on normal fixtures. |

## 2. Wave-0 validation prerequisites

Wave 0 here means validation prerequisites that must exist before production implementation in the relevant executable task. A `tdd="true"` task performs its RED scaffold first within its already declared test-file allowlist.

| ID | Prerequisite | Automated gate | Evidence path | Expected result | Status |
|---|---|---|---|---|---|
| W0-A | All 29 planning files parse and pass standard schema/structure. | Current planning-artifact validation loop over `options_copilot/plans/*-PLAN.md` | `options_copilot/plans/*-PLAN.md` | 29 valid frontmatters; 29 valid structures; zero XML errors. | planning-time |
| W0-B | Existing Options Copilot test runner and syntax tools work before new code. | `python -m pytest -q tests/options_copilot`; compileall; node check | Existing `tests/options_copilot/`, `options_copilot/`, frontend JS | Baseline result captured; failures stop implementation and are not hidden. | post-implementation preflight |
| W0-C | Immutable pre-edit Git/workspace baseline exists. | Four read-only Git commands from P0-02-01 | `data/options_copilot/evidence/workspace/P0/` | Commit, exact status/diffs, dirty/untracked hashes captured before edits. | P0 blocking |
| W0-D | Strategy NAV, Execution Cost, and Initial Champion/Scenario contracts are signed and valid. | `python -m options_copilot.governance.cli verify-contract ...` for all three | `options_copilot/governance/*.v1.json`; P0 contract checkpoint prefixes | Actor/time/version/hash/provenance complete; no NLV fallback; no candidate without cost/policy. | P0 blocking |
| W0-E | Complete signed MarketDataPacingCapability exists. | P0 pacing probe and contract verifier | `data/options_copilot/evidence/checkpoints/P0/market-data-pacing/` | All five classes and four limits; allowed source; actor/time/version/hash. | blocks P4 |
| W0-F | One exact installed creator transport is signed. | Creator transport probe/contract verifier | `options_copilot/governance/creator_transport_contract.v1.json`; P0 creator prefix | Exact connector/tool schemas, auth isolation, deep-link/reconciliation/idempotency, signer/hash. | blocks P7 |
| W0-G | Task-local RED scaffold exists before production edits. | Focused task command must initially fail for the intended missing behavior, then pass after implementation. | The exact `tests/options_copilot/...` path in that task | Test file is collected; failure reason matches missing behavior; no production edit precedes RED. | per TDD task |
| W0-H | Live evidence manifest contract exists before any named checkpoint. | Readiness/evidence manifest schema tests | Each declared checkpoint prefix | Command, UTC, Git/workspace, inputs/outputs, actor where required, and canonical hashes; no secret. | blocks live checkpoints |

## 3. Executable plan and local-wave map

Every row is an executable plan. `W0` references the prerequisite table; checkpoint evidence is manual even when an automated verifier is listed.

| Plan | Program/local wave | Tasks | Depends on | W0 | Aggregate fast command | Named live/manual gate |
|---|---|---|---|---|---|---|
| P0-01 | 0/1 | 01-03 | — | A,B,G,H | `pytest -q test_readiness.py test_readiness_cli.py test_api_frontend.py` | P0 market-data pacing decision |
| P0-02 | 0/1 | 01-03 | — | A,B,C,G | `pytest -q test_authority_audit.py test_workspace_guard.py test_plan_allowlist_consistency.py` | Workspace preflight evidence |
| P0-03 | 0/1 | 01-04 | — | A,B,G,H | `pytest -q test_governance_contracts.py` plus three contract verifiers | Three human contract decisions |
| P0-04 | 0/1 | 01-02 | — | A,B,G,H | `pytest -q test_creator_transport.py` | Creator transport selection |
| P1-01 | 1/1 | 01-02 | — | C,D,G | `pytest -q test_nav_ledger.py test_campaign.py test_proposal_gate.py` | none |
| P1-02 | 1/1 | 01-02 | — | C,G | `pytest -q test_broker_snapshot.py test_ibkr_readonly_gateway.py` | none |
| P1-03 | 1/2 | 01-03 | P1-01,P1-02 | C,D,G,H | `pytest -q test_bridge_coordinator.py test_market_session_calendar.py test_proposal_gate.py` | P1 IBKR live read |
| P2-01 | 2/1 | 01-02 | — | C,D,G | `pytest -q test_position_manager.py test_gld_management_pipeline.py` | none |
| P2-02 | 2/2 | 01-03 | P2-01 | C,D,G,H | `pytest -q test_runtime.py test_bridge_coordinator.py test_api_frontend.py test_gld_management_pipeline.py` | P2 GLD dry run |
| P3-01 | 1/1 | 01-02 | — | C,G | `pytest -q test_evidence_store.py` | none |
| P3-02 | 1/2 | 01-04 | P3-01 | C,G,H | `pytest -q test_event_providers.py test_config_security.py` | Jin10 revocation/rotation and provider live |
| P4-01 | 2/1 | 01-02 | — | C,G | `pytest -q test_scan_scheduler.py test_scanner_authority.py` | none |
| P4-02 | 2/1 | 01-02 | — | C,E,G | `pytest -q test_universe_funnel.py test_scanner_pacing.py test_ibkr_readonly_gateway.py` | none |
| P4-03 | 2/2 | 01 | P4-01,P4-02 | C,E,H | exact `inspect-slot --expect-run-count 1 --expect-duplicate-reason SLOT_ALREADY_COMPLETED` | P4 scanner live |
| P5-01 | 3/1 | 01-02 | — | C,D,G | `pytest -q test_strategy_generator.py test_domain_risk_ranking.py test_proposal_gate.py` | none |
| P5-02 | 3/1 | 01-02 | — | C,D,G | `pytest -q test_positioning.py test_scenarios.py` | none |
| P5-03 | 3/2 | 01-03 | P5-01,P5-02 | C,D,G | `pytest -q test_portfolio_ranking.py test_ranking_store.py test_proposal_gate.py` | none |
| P5-04 | 3/3 | 01-02 | P5-03 | C,D,E,G | `pytest -q test_decision_pipeline.py test_ranking_store.py test_scanner_authority.py` | none |
| P6-01 | 4/1 | 01-03 | — | C,D,G | `pytest -q test_runtime_pipeline.py test_api_frontend.py test_ranking_store.py test_portfolio_ranking.py` | none |
| P6-02 | 4/2 | 01-02 | P6-01 | C,D,G,H | `pytest -q test_api_frontend.py`; node check | P6 desktop/mobile browser |
| P7-01 | 5/1 | 01-03 | — | C,D,F,G | `pytest -q test_creator_adapter.py test_bridge_coordinator.py test_gld_creator_e2e.py`; authority audit | NORMAL/management automated only; A-grade remains locked pending P9 |
| P7-02 | 5/2 | 01-03 | P7-01 | C,D,F,H | creator status verifier | First real creator E2E is NORMAL <=10.00 plus separate human IBKR decision |
| P8-01 | 4/1 | 01 | — | C,D,H | independence contract verifier | P8 independence decision |
| P8-02 | 4/2 | 01-02 | P8-01 | C,D,G | `pytest -q test_replay.py test_decision_pipeline.py` | none |
| P8-03 | 4/2 | 01-02 | P8-01 | C,D,G | `pytest -q test_outcomes.py test_learning_storage_approval.py` | none |
| P9-01 | 6/1 | 01-03 | — | C,D,G | `pytest -q test_learning_evaluation.py test_policy_authority.py test_a_grade_marker.py test_proposal_gate.py` | none |
| P9-02 | 6/2 | 01-04 | P9-01 | C,D,G,H | governance CLI verifiers; locked-first and post-sign creator E2E; frontend tests/node check | Human promotion/A-grade/rollback decisions then post-signing creator verification |
| P10-01 | 7/1 | 01-03 | — | C,G | `pytest -q test_process_operations.py test_readiness_cli.py test_evidence_store.py test_ranking_store.py` | none; Tailscale command count must remain zero |
| P10-02 | 7/2 | 01-02 | P10-01 | C,D,E,F,H | pure saved-snapshot route evidence and full readiness verifier | Human-only re-query/adopt-or-approved-create, remote APIs, and rollout decisions |

## 4. Per-task Nyquist map

Status values: `AUTO` means fully automated post-implementation validation; `MANUAL+AUTO` means a human/live checkpoint whose artifact is also machine-verified. All rows are pending until execution.

### P0 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P0-01-01 | 0/1 | SAFE-01,DATA-02,OPS-01,VAL-01 | A,B,G | `pytest -q tests/options_copilot/test_readiness.py` | — | `options_copilot/operations/capabilities.py`; test file | Five classes validate; missing/unsigned/stale limits reject. | AUTO |
| P0-01-02 | 0/1 | same | A,B,G | `pytest -q tests/options_copilot/test_readiness.py tests/options_copilot/test_readiness_cli.py tests/options_copilot/test_api_frontend.py` | — | readiness CLI/API tests | CLI/API same hash; degraded inputs never READY. | AUTO |
| P0-01-03 | 0/1 | same | H | pacing probe/verifier | Human approves observed or explicit conservative values. | `data/options_copilot/evidence/checkpoints/P0/market-data-pacing/` | Signed complete capability or P4 blocked. | MANUAL+AUTO |
| P0-02-01 | 0/1 | SAFE-01,OPS-01,VAL-01 | A | four read-only Git commands | Baseline timing is reviewed if uncertain. | `data/options_copilot/evidence/workspace/P0/` | Pre-edit commit/status/diffs/path hashes. | AUTO |
| P0-02-02 | 0/1 | same | B,G | `pytest -q tests/options_copilot/test_authority_audit.py` plus authority CLI | — | authority audit source/test JSON output | Direct/indirect sinks reject; negative fields pass. | AUTO |
| P0-02-03 | 0/1 | same | C,G | `pytest -q tests/options_copilot/test_workspace_guard.py tests/options_copilot/test_plan_allowlist_consistency.py` | — | allowlist JSON and tests | Missing and surplus plan/master entries reject. | AUTO |
| P0-03-01 | 0/1 | RISK-01,ANALYTICS-01,STRAT-01,LEARN-01,VAL-01 | B,G | `pytest -q tests/options_copilot/test_governance_contracts.py` | — | contracts module/CLI/test | Tamper, overwrite, wrong kind, missing signer reject. | AUTO |
| P0-03-02 | 0/1 | same | H | Strategy NAV contract verifier | Human signs ledger attribution/equation. | Strategy NAV JSON and checkpoint prefix | Complete contract; explicit no-NLV fallback. | MANUAL+AUTO |
| P0-03-03 | 0/1 | same | H | Execution Cost contract verifier | Human calibrates/approves real-account costs. | cost JSON and checkpoint prefix | All fee/risk/provenance/fallback/version/hash fields. | MANUAL+AUTO |
| P0-03-04 | 0/1 | same | H | Initial policy contract verifier | Human signs probability/weights/caps/NO_TRADE policy. | initial-policy JSON and checkpoint prefix | Frozen valid baseline; P5 permitted to start. | MANUAL+AUTO |
| P0-04-01 | 0/1 | SAFE-01,BRIDGE-01,SEC-01,VAL-01 | B,G | `pytest -q tests/options_copilot/test_creator_transport.py` | — | creator transport module/probe/test | Exact schemas/auth isolation; unsafe/fallback rejects. | AUTO |
| P0-04-02 | 0/1 | same | H | creator contract verifier | Human selects one installed operation. | creator contract JSON and checkpoint prefix | Signed operation or P7 blocked. | MANUAL+AUTO |

### P1 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P1-01-01 | 1/1 | RISK-01,SAFE-02,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_nav_ledger.py tests/options_copilot/test_campaign.py` | — | nav ledger/test | Idempotent flows; conflicts invalidate; NLV does not change risk. | AUTO |
| P1-01-02 | 1/1 | same | C,D,G | `pytest -q tests/options_copilot/test_nav_ledger.py tests/options_copilot/test_proposal_gate.py` | — | risk authorization/policy tests | Exact 10/15/20 boundaries; invalid marker false. | AUTO |
| P1-02-01 | 1/1 | DATA-01,SAFE-01,VAL-01 | C,G | `pytest -q tests/options_copilot/test_broker_snapshot.py tests/options_copilot/test_ibkr_readonly_gateway.py` | — | broker snapshot/gateway tests | State/secdef mutation and quote incoherence distinguished. | AUTO |
| P1-02-02 | 1/1 | same | C,G | same focused suite | — | canonical/read-only test cases | Changed coherent prices pass; partial/unknown reject. | AUTO |
| P1-03-01 | 1/2 | DATA-01,TIME-01,RISK-01,BRIDGE-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_bridge_coordinator.py tests/options_copilot/test_proposal_gate.py` | — | bridge proof/test rows | Lock-time identical requery binds immutable snapshot; zero call on drift. | AUTO |
| P1-03-02 | 1/2 | same | C,G | `pytest -q tests/options_copilot/test_market_session_calendar.py tests/options_copilot/test_proposal_gate.py` | — | calendar/time-policy tests | DST/holiday/early-close and DTE boundaries exact. | AUTO |
| P1-03-03 | 1/2 | same | C,D,H | broker/entitlement readiness probe | Human verifies real read-only IBKR state. | `data/options_copilot/evidence/checkpoints/P1/ibkr-live-read/` | READY, atomic, coherent, no instruction/order change. | MANUAL+AUTO |

### P2 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P2-01-01 | 2/1 | RISK-02,SAFE-02,DATA-01,EXIT-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_position_manager.py` | — | position models/manager/test | Exact close or strict reduction; analog file unchanged. | AUTO |
| P2-01-02 | 2/1 | same | C,D,G | `pytest -q tests/options_copilot/test_position_manager.py tests/options_copilot/test_gld_management_pipeline.py` | — | generator/CLI/integration test | Snapshot-derived GLD preview; cost/exit hashes; zero attempts. | AUTO |
| P2-02-01 | 2/2 | RISK-02,BRIDGE-01,EXIT-01,DATA-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_runtime.py tests/options_copilot/test_bridge_coordinator.py tests/options_copilot/test_gld_management_pipeline.py` | — | approval/bridge tests | Valid management preview only; entry blocked; tamper zero call. | AUTO |
| P2-02-02 | 2/2 | same | C,G | `pytest -q tests/options_copilot/test_api_frontend.py`; node check | — | API/frontend/test | Separate management state; stale quote disables action. | AUTO |
| P2-02-03 | 2/2 | same | C,D,H | GLD dry-run CLI | Human compares live GLD legs/proof. | `data/options_copilot/evidence/checkpoints/P2/gld-dry-run/` | After zero/non-increasing risk; external attempts zero. | MANUAL+AUTO |

### P3 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P3-01-01 | 1/1 | EVENT-01,TIME-01,DATA-02,VAL-01 | C,G | `pytest -q tests/options_copilot/test_evidence_store.py` | — | evidence store/test | WAL/FULL; point-in-time; conflict preserved; no secret. | AUTO |
| P3-01-02 | 1/1 | same | C,G | same suite | — | v1 DB fixture and migration tests | Directed migration/integrity/concurrency/restart pass. | AUTO |
| P3-02-01 | 1/2 | EVENT-01,SEC-01,TIME-01,ANALYTICS-01,VAL-01 | C,G | `pytest -q tests/options_copilot/test_event_providers.py` | — | provider modules/CLI/test | Official precedence; faults mapped; SUPPORTING_ONLY. | AUTO |
| P3-02-02 | 1/2 | same | C,G | `pytest -q tests/options_copilot/test_config_security.py` | — | revocation/setter/security tests | Setter refuses absent/tampered attestation; zero token material. | AUTO |
| P3-02-03 | 1/2 | same | C,H | revocation verifier | Human revokes prior credential then enters replacement at hidden prompt. | `data/options_copilot/evidence/checkpoints/P3/jin10-rotation/` | Signed old_token_revoked attestation bound to DPAPI rotation manifest. | MANUAL+AUTO |
| P3-02-04 | 1/2 | same | C,H | provider live probe | Human reviews live network/license/redaction. | `data/options_copilot/evidence/checkpoints/P3/provider-live/` | Contract/coverage/conflict/redaction evidence; no secret. | MANUAL+AUTO |

### P4 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P4-01-01 | 2/1 | SCAN-01,TIME-01,OPS-01,VAL-01 | C,G | `pytest -q tests/options_copilot/test_scan_scheduler.py` | — | scheduler/run-store test | One owner; exact slots; bounded recovery; duplicate reason. | AUTO |
| P4-01-02 | 2/1 | same | C,G | `pytest -q tests/options_copilot/test_scan_scheduler.py tests/options_copilot/test_scanner_authority.py` | — | service/CLI/authority tests | One pipeline call per slot; no authority imports. | AUTO |
| P4-02-01 | 2/1 | DATA-02,SCAN-01,TIME-01,RISK-02,VAL-01 | C,E,G | `pytest -q tests/options_copilot/test_universe_funnel.py` | — | universe/test | No skipped layer; DTE exact; GLD entry zero. | AUTO |
| P4-02-02 | 2/1 | same | C,E,G | `pytest -q tests/options_copilot/test_scanner_pacing.py tests/options_copilot/test_ibkr_readonly_gateway.py` | — | pacing/gateway/readiness tests | P0-only budgets; TTL; cancel exactly once; baseline restored. | AUTO |
| P4-03-01 | 2/2 | DATA-02,SCAN-01,RISK-02,VAL-01 | C,E,H | exact inspect-slot command | Human runs same tick twice and reviews live budgets/subscriptions. | `data/options_copilot/evidence/checkpoints/P4/scanner-live/` | One run; second duplicate; no entry/creator; subscriptions baseline. | MANUAL+AUTO |

### P5 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P5-01-01 | 3/1 | SAFE-02,STRAT-01,EXIT-01,TIME-01,RISK-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_strategy_generator.py tests/options_copilot/test_domain_risk_ranking.py` | — | template/domain tests | Finite templates; uncovered/unknown rejects; full ExitPlan. | AUTO |
| P5-01-02 | 3/1 | same | C,D,G | `pytest -q tests/options_copilot/test_strategy_generator.py tests/options_copilot/test_proposal_gate.py` | — | generator/test | Coherent quote/cost/policy/NAV/DTE bindings; GLD entry zero. | AUTO |
| P5-02-01 | 3/1 | ANALYTICS-01,EVENT-01,STRAT-01,RISK-01,LEARN-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_positioning.py tests/options_copilot/test_scenarios.py` | — | volatility/scenario tests | Immutable hard versus supporting evidence roles. | AUTO |
| P5-02-02 | 3/1 | same | C,D,G | `pytest -q tests/options_copilot/test_scenarios.py` | — | scenario/resolver tests | Initial fallback is source of truth; outputs carry current policy/authority identities; stale/tampered resolver is NO_TRADE. | AUTO |
| P5-03-01 | 3/2 | STRAT-01,RISK-01,ANALYTICS-01,BRIDGE-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_portfolio_ranking.py tests/options_copilot/test_proposal_gate.py` | — | ranker tests | At most three diverse stable candidates; hard gates first. | AUTO |
| P5-03-02 | 3/2 | same | C,D,G | `pytest -q tests/options_copilot/test_ranking_store.py` | — | ranking DB/test | Exact schema, WAL/FULL, hash chain, tamper/restart/concurrency pass. | AUTO |
| P5-03-03 | 3/2 | same | C,D,G | `pytest -q tests/options_copilot/test_ranking_store.py tests/options_copilot/test_portfolio_ranking.py` | — | rank-one transaction tests | Rank2/3/stale/tampered/superseded reject with no side effect. | AUTO |
| P5-04-01 | 3/3 | DATA-02,ANALYTICS-01,STRAT-01,SCAN-01,VAL-01 | C,D,E,G | `pytest -q tests/options_copilot/test_decision_pipeline.py tests/options_copilot/test_scanner_authority.py` | — | pipeline/service tests | Spy confirms exact pipeline order; NO_TRADE on missing binding. | AUTO |
| P5-04-02 | 3/3 | same | C,D,E,G | `pytest -q tests/options_copilot/test_decision_pipeline.py tests/options_copilot/test_ranking_store.py` | — | full pipeline tests | One ranking per run; all hash tamper rejects; no approval writes. | AUTO |

### P6 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P6-01-01 | 4/1 | SCAN-01,STRAT-01,EXIT-01,BRIDGE-01,RISK-01,LEARN-01,SEC-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_runtime_pipeline.py tests/options_copilot/test_ranking_store.py tests/options_copilot/test_portfolio_ranking.py` | — | pipeline/ranker/ledger tests | One policy/risk resolution; ranking basis then marker-bound final row; stale authority rejects. | AUTO |
| P6-01-02 | 4/1 | same | C,D,G | `pytest -q tests/options_copilot/test_runtime_pipeline.py tests/options_copilot/test_runtime.py` | — | runtime/tests | Both resolvers and all named dependencies injected; no external JSON authority. | AUTO |
| P6-01-03 | 4/1 | same | C,D,G | `pytest -q tests/options_copilot/test_runtime_pipeline.py tests/options_copilot/test_api_frontend.py tests/options_copilot/test_ranking_store.py` | — | API/approval/rank tests | Rank1 only; policy/risk identities propagate; rollback/revocation/bypass/stale/tamper/concurrency reject. | AUTO |
| P6-02-01 | 4/2 | STRAT-01,EXIT-01,BRIDGE-01,OPS-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_api_frontend.py`; node check | — | UI spec/frontend/test | Five regions; rank2/3 no handler; NLV observation only. | AUTO |
| P6-02-02 | 4/2 | same | C,D,H | readiness API command | Human verifies real Chrome desktop/mobile. | `data/options_copilot/evidence/checkpoints/P6/browser/` | Same-run API/screenshots; visible correct authority; no overflow. | MANUAL+AUTO |

### P7 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P7-01-01 | 5/1 | SAFE-01,BRIDGE-01,DATA-01,RISK-01,LEARN-01,SEC-01,STRAT-01,VAL-01 | C,D,F,G | `pytest -q tests/options_copilot/test_creator_adapter.py tests/options_copilot/test_gld_creator_e2e.py` | — | pure adapter tests | Dynamic exact payload; current policy/risk authority hashes; exact boundaries; no write/call. | AUTO |
| P7-01-02 | 5/1 | same | C,D,F,G | `pytest -q tests/options_copilot/test_creator_adapter.py tests/options_copilot/test_bridge_coordinator.py` | — | coordinator/store/adapter tests | Resolver reread before reservation; one call; concurrent promotion/rollback zero call; UNKNOWN terminal. | AUTO |
| P7-01-03 | 5/1 | same | C,D,F,G | creator/bridge/GLD suites plus authority audit | — | test files and authority JSON | NORMAL 10.00 first E2E; unsigned 10.01/15.00 and 15.01/20.00/20.01 reject; post-sign permit branch deferred to P9-02. | AUTO |
| P7-02-01 | 5/2 | SAFE-01,BRIDGE-01,DATA-01,SEC-01,VAL-01 | C,D,F | creator/API tests | — | `options_copilot/README.md` | Exact post-implementation one-attempt and human-final runbook. | AUTO |
| P7-02-02 | 5/2 | same | C,D,F,H | bridge status verifier | Human reconciles one live pending review or UNKNOWN. | `data/options_copilot/evidence/checkpoints/P7/creator-live/` | Connector count one; exact hashes; no secret; review-only. | MANUAL+AUTO |
| P7-02-03 | 5/2 | same | H | bridge status verifier | Human alone confirms/rejects inside IBKR. | same P7 creator prefix | Actor/time/final decision/reconciliation; no automated final action. | MANUAL+AUTO |

### P8 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P8-01-01 | 4/1 | LEARN-01,ANALYTICS-01,VAL-01 | C,D,H | independence contract verifier | Human signs deterministic clustering/count rules. | independence JSON and checkpoint prefix | Complete spec; UNKNOWN weight zero; prerequisite hashes bound. | MANUAL+AUTO |
| P8-02-01 | 4/2 | LEARN-01,ANALYTICS-01,STRAT-01,EXIT-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_replay.py` | — | dataset/test | Point-in-time cutoff, exclusion log, chronological windows. | AUTO |
| P8-02-02 | 4/2 | same | C,D,G | `pytest -q tests/options_copilot/test_replay.py tests/options_copilot/test_decision_pipeline.py` | — | replay engine/tests | Frozen pipeline reproducible; replay has no live authority. | AUTO |
| P8-03-01 | 4/2 | LEARN-01,EXIT-01,ANALYTICS-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_outcomes.py` | — | outcome ledger/test | Immutable cost-adjusted versions; no missing-data fill. | AUTO |
| P8-03-02 | 4/2 | same | C,D,G | `pytest -q tests/options_copilot/test_outcomes.py tests/options_copilot/test_learning_storage_approval.py` | — | clustering/count tests | Correlated/duplicate count once; independent count separately. | AUTO |

### P9 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P9-01-01 | 6/1 | LEARN-01,RISK-01,STRAT-01,BRIDGE-01,SEC-01,VAL-01 | C,D,G | `pytest -q tests/options_copilot/test_learning_evaluation.py tests/options_copilot/test_learning_storage_approval.py` | — | similarity/evaluation tests | Shadow-only; 30 scenes Discovery; initial policy unchanged. | AUTO |
| P9-01-02 | 6/1 | same | C,D,G | `pytest -q tests/options_copilot/test_policy_authority.py tests/options_copilot/test_learning_evaluation.py` | — | authority ledger/resolver/CLI tests | Restart-safe signed head; six human-only verbs; atomic rollback/revocation; concurrent/tamper fail closed. | AUTO |
| P9-01-03 | 6/1 | same | C,D,G | `pytest -q tests/options_copilot/test_a_grade_marker.py tests/options_copilot/test_policy_authority.py tests/options_copilot/test_proposal_gate.py` | — | risk authority tests | Exact six boundaries; proposal/policy/cost/ranking binding; rollback invalidates. | AUTO |
| P9-02-01 | 6/2 | LEARN-01,RISK-01,SEC-01,VAL-01 | C,D,G | learning/API tests plus node check | — | AI-SPEC/runtime/API/frontend tests | Durable resolvers injected; transparent hashes/status; UI/model cannot sign. | AUTO |
| P9-02-02 | 6/2 | same | C,D,G | locked-first E2E marker selection | — | policy/creator E2E tests | Before human sign, only NORMAL 10.00 passes; promotion/rollback/tamper/restart paths covered. | AUTO |
| P9-02-03 | 6/2 | same | C,D,H | promotion and A-grade decision verifiers | Human separately signs or denies promotion/A-grade; rollback remains explicit human command. | P9 decision, promotion, A-grade, rollback, and authority DB paths | No auto signing; approval is proposal-bound; atomic rollback verifies revocation. | MANUAL+AUTO |
| P9-02-04 | 6/2 | same | C,D,H | marked post-human authority creator E2E | Human verifies approved or locked branch only after P9 decision. | P9 decision prefix | Approved: 10.01/15.00 pass only exact marker; invalid/15.01/20/20.01 zero call. Denied: A-grade remains blocked. | MANUAL+AUTO |

### P10 tasks

| Task | Program/local wave | Requirements | W0 | Fast automated command | Live/manual checkpoint | Evidence artifact path | Expected result | Mode |
|---|---|---|---|---|---|---|---|---|
| P10-01-01 | 7/1 | OPS-01,VAL-01,SAFE-01,SCAN-01,DATA-01 | C,G | `pytest -q tests/options_copilot/test_process_operations.py tests/options_copilot/test_readiness_cli.py` | — | launcher/readiness tests | Unknown listener zero stop; exact process restore; pure route snapshots classify adopted/created ownership; Tailscale calls zero. | AUTO |
| P10-01-02 | 7/1 | same | C,G | process/readiness/evidence/ranking suites | — | monitoring/migration tests | Safety disables approval; G: backup; directed recoverable migration. | AUTO |
| P10-01-03 | 7/1 | same | C | process/readiness tests | — | `options_copilot/operations/runbook.md` | Exact post-implementation human adopt/create/targeted-rollback plus local/remote read APIs; root protected. | AUTO |
| P10-02-01 | 7/2 | OPS-01,VAL-01,SAFE-01,SCAN-01,DATA-01 | C,E,F,H | saved-snapshot route evidence verifier | Human re-queries; adopts exact existing :8891 or explicitly approves separate creation; verifies endpoints/restoration. | `data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/` | Protected root unchanged; adopted route unmutated; created route exclusively owned; local/remote read paths 200; no leftovers. | MANUAL+AUTO |
| P10-02-02 | 7/2 | same | C,D,E,F,H | full readiness verifier | Human signs each evidence-gated rollout stage. | `data/options_copilot/evidence/checkpoints/P10/rollout/` | Highest fully evidenced review-only stage only; no time auto-promotion. | MANUAL+AUTO |

## 5. Requirement validation coverage

| Requirement | Fast automated command family | Named live/manual checkpoint | Evidence artifact | Expected result |
|---|---|---|---|---|
| OC-SAFE-01 | authority audit; creator/bridge tests | P7 creator and final-human decision; P10 rollout | P0 authority, P7/P10 prefixes | Zero local execution authority; human final action. |
| OC-SAFE-02 | payoff/template/position property tests | P2 GLD dry run | P2 proof prefix | No naked/unlimited/unknown-loss or hidden short leg. |
| OC-RISK-01 | NAV/current-policy/risk-authority/creator boundary tests | P0 NAV/cost/policy; P9 human authorities and post-sign creator E2E | P0/P9 prefixes | Strategy NAV only; NORMAL 10.00; exact bound 10.01/15.00; 15.01/20.00/20.01 reject. |
| OC-RISK-02 | position/generator/runtime tests | P2 GLD dry run; P10 rollout | P2/P10 prefixes | One combination; GLD close/reduce only. |
| OC-TIME-01 | calendar/DTE/provider/replay tests | P1 live broker; P4 live slot | P1/P4 prefixes | ET/DST/holiday/early close; correct four times and DTE. |
| OC-DATA-01 | snapshot/bridge/reserve tamper tests | P1 IBKR live; P2/P7 | P1/P2/P7 prefixes | MUTATED_DURING_BUILD vs QUOTE_INCOHERENT; identical reserve protocol. |
| OC-DATA-02 | readiness/pacing/funnel/subscription tests | P0 pacing; P4 scanner live | P0/P4 prefixes | P0-only budgets; bounded requests; clean subscriptions. |
| OC-EVENT-01 | evidence/provider conflict tests | P3 provider live | P3 provider prefix | Official precedence, provenance, conflict, redaction. |
| OC-ANALYTICS-01 | scenarios supporting-only negative tests | P0 policy; P3 provider | P0/P3 prefixes | Supporting evidence cannot create eligibility. |
| OC-STRAT-01 | generator/ranker/store/pipeline/authority tests | P6 browser; P7 NORMAL creator; P9 post-sign creator | P6/P7/P9 prefixes | Finite candidates, immutable authority-bound Top 3, current rank 1 only. |
| OC-EXIT-01 | generator/position/outcome tests | P2 GLD; P6 browser | P2/P6 prefixes | Complete exit contract is visible and hash-bound. |
| OC-SCAN-01 | scheduler concurrency/recovery/authority tests | P4 duplicate-tick live; P10 rollout | P4/P10 prefixes | One run per slot, explicit duplicate, no creator authority. |
| OC-BRIDGE-01 | approval/bridge/creator concurrency tests | P7 creator/final human | P7 prefix | Double confirm, five minutes, one call, UNKNOWN no retry. |
| OC-LEARN-01 | replay/outcome/evaluation/current-policy/authority tests | P8 independence; P9 promotion/A-grade/rollback decisions | P8/P9 prefixes | No leakage; correct count; restart-safe current head; atomic rollback/revocation; no automatic signing. |
| OC-SEC-01 | secret/redaction/transport/URL tests | P3 rotation; P0/P7 creator | P0/P3/P7 prefixes | DPAPI; signed revocation; auth isolation; trusted path. |
| OC-OPS-01 | process and route ownership/recovery/readiness tests | P10 human route/process checkpoint | P10 process prefix | Existing :8891 adopted, absent route human-approved, protected root unchanged, remote APIs healthy, owned-only rollback, no leftovers. |
| OC-VAL-01 | all 72 task commands and planning validators | all named checkpoints | all declared test/checkpoint paths | 29/29 plans and 72/72 task mappings; mock never substitutes live. |

Coverage: **29/29 executable plans**, **72/72 tasks**, **all local waves**, **8/8 program waves**, and **17/17 requirements**.

## 6. Validation sign-off

- [ ] W0-A planning validation passes for all 29 plans.
- [ ] All task-local RED scaffolds exist before production edits.
- [ ] No three consecutive tasks lack a focused automated command.
- [ ] Every human/live task also has an automated artifact verifier.
- [ ] Every live manifest contains command, UTC, Git/workspace, input/output hashes, and actor where applicable.
- [ ] No evidence path stores credentials or token material.
- [ ] Phase and plan allowlist missing/surplus tests pass.
- [ ] No local/same-wave dependency and no dependency cycle exists.
- [ ] Full authority audit passes after creator adapter implementation.
- [ ] Mock, fixture, localhost, SQLite count, or CLI exit is never accepted instead of its named live/human gate.

**Approval:** pending independent plan verification
