---
artifact_type: MASTER_META_PLAN
program: options-copilot-productionization
executable: false
gsd_sdk_validation_target: options_copilot/plans/*-PLAN.md
status: ready-for-independent-verification
created: 2026-08-03
requirements_count: 17
phase_count: 11
program_wave_count: 8
executable_plan_count: 29
authority: permanent-review-only
profit_guarantee: false
---

# Options Copilot Implementation Master Meta-Plan

> **MASTER META-PLAN — NOT DIRECTLY EXECUTABLE BY `gsd-sdk`.**
>
> This file is the program index, cross-phase dependency contract, invariant registry, and exact phase allowlist. The 29 executable GSD plans are linked under `options_copilot/plans/`; validate and execute those files, never this master.

## 1. Outcome and permanent authority boundary

Options Copilot becomes a human-gated US equity-options research and review surface. It gathers point-in-time broker and event evidence, runs a bounded two-hour DecisionPipeline, produces an immutable Top 3 or structured `NO_TRADE`, supports only proven GLD close/reduce management while the legacy position remains, and may create one review instruction through one signed Managed Connector operation. The user alone performs the final confirm or reject action inside IBKR.

The local package permanently has no broker-order execution authority. Browser automation, credential extraction, retry after uncertain creator outcome, and any order-capable connector operation are prohibited. Explicit negative status fields and authority-audit assertions are not callable authority.

## 2. Locked safety and policy decisions

- Strategy NAV from the signed cash-flow ledger is the only risk base. Account NLV is observation/reconciliation only; 10K Campaign progress cannot alter risk.
- NORMAL maximum loss: exactly 10.00% may pass. From 10.01% through 15.00%, creator authorization requires the current versioned RiskTierAuthority plus a human-signed, unrevoked P9 A-grade marker bound to the exact proposal, current policy, signed cost contract, and canonical ranking basis; 15.01%, 20.00%, and 20.01% reject, with 20.00% the absolute hard rejection line. No environment, API, model, rank, or supporting signal may relax these limits.
- At most one open options combination. While GLD is open, all entry candidates are `NO_TRADE`; only exact `CLOSE_ALL` or mathematically proven `REDUCE_RISK` is eligible.
- Entry normally uses 14–35 DTE. A signed audited exception may use 7–13 DTE; entry below 7 DTE always rejects. Management below entry floors remains close/reduce-only and cannot cross zero or increase exposure, maximum loss, or capital usage.
- Naked options, uncovered ratio shorts, unlimited/unknown maximum loss, adjusted or unidentified contracts, and nonstandard multiplier are permanently rejected.
- News, earnings, Max Pain, walls, PCR, and estimated GEX are capped `SUPPORTING_ONLY`. Eligibility separately requires independent MARKET, VOLATILITY, and LIQUIDITY evidence and every hard gate.
- GUI shows at most three immutable candidates. Only transactionally reread current rank 1 may create a challenge; ranks 2–3 are permanently view-only.
- External creator runs only after pure payload validation and durable single-attempt reservation. Uncertain outcome is terminal `UNKNOWN_OUTCOME`; no retry.
- Thirty independent scenarios unlock Discovery only. Challenger prediction is shadow-only. PromotionAuthority, proposal-bound A-grade authority, and RollbackAuthority are separate durable human-signed artifacts; P9 cannot automatically mutate the signed initial policy, which remains the fallback/source of truth. Rollback atomically restores the prior signed policy and revokes A-grade authority bound to the rolled-back head.
- Options Copilot remains isolated on 8891. Existing 8766/8788/8878 processes and Tailscale root/443→8878 stay unchanged; unknown listeners are never stopped. The separate tailnet-only HTTPS :8891 route is re-queried post-implementation: an exact pre-existing mapping is adopted without mutation, and an absent mapping is created only after explicit human approval with task-owned rollback proof.

## 3. Requirement catalog

| ID | Required outcome |
|---|---|
| OC-SAFE-01 | Permanent review-only authority; no local broker execution primitive. |
| OC-SAFE-02 | Naked, unlimited, or unknown-loss structures reject after independent local payoff calculation. |
| OC-RISK-01 | Signed Strategy NAV only; exact 10/15/20 boundaries and separate P9 A-grade authority. |
| OC-RISK-02 | One combination; GLD only exact close or proven reduction while open. |
| OC-TIME-01 | Authoritative ET calendar, point-in-time timestamps, normal 14–35 entry DTE, audited floor of 7. |
| OC-DATA-01 | Component-specific atomic broker protocol, reserve-time identical requery, immutable snapshot hash, coherent quotes at most 5 seconds old. |
| OC-DATA-02 | Layered universe, signed per-class pacing, entitlement/degraded state, provenance cache, subscription cleanup. |
| OC-EVENT-01 | Official anchors, supplemental providers, provenance, deduplication, visible conflicts, four timestamps. |
| OC-ANALYTICS-01 | Supporting evidence caps plus independent market/volatility/liquidity and cost hard evidence. |
| OC-STRAT-01 | Finite-risk templates, combination priority, immutable diverse Top 3/NO_TRADE, current rank-one-only authorization. |
| OC-EXIT-01 | Every candidate binds invalidation, risk stop, profit take, time stop, holding limit, and bad-quote action. |
| OC-SCAN-01 | Authoritative two-hour slots, lease/idempotency, bounded recovery, positions first, no creator authority. |
| OC-BRIDGE-01 | Double confirmation, five-minute approval, exactly one external attempt, UNKNOWN no retry. |
| OC-LEARN-01 | Point-in-time replay/outcomes, signed independence, shadow learning, human promotion/rollback. |
| OC-SEC-01 | DPAPI, revocation attestation, redaction, auth isolation, signed creator transport, trusted deep link. |
| OC-OPS-01 | 8891 isolation, exact owned restart, protected services/routes, local and actual remote verification, no leftovers. |
| OC-VAL-01 | Unit/property/concurrency/tamper plus named live/human/process evidence; mocks never replace live acceptance. |

## 4. Eight program waves and cross-phase gates

Executable plan `wave` numbers are local to each P-phase, exactly as GSD expects. The program has eight macro waves:

| Program wave | Phases | Cross-phase prerequisite plan IDs | Outcome |
|---:|---|---|---|
| 0 | P0 | none | Truthful capability, workspace/authority guard, signed NAV/cost/initial-policy contracts, signed creator transport. |
| 1 | P1, P3 | P1 after P0-01/P0-03; P3 after P0-02/P0-03 | Strategy NAV and atomic broker truth in parallel with evidence/providers and secure rotation. |
| 2 | P2, P4 | P2 after P1-03; P4 after P0-01/P1-03/P3-02 | GLD close/reduce and bounded two-hour scanner. |
| 3 | P5 | P0-03, P2-02, P4-03 | Finite-risk candidates, scenarios, immutable ranking ledger, exact DecisionPipeline. |
| 4 | P6, P8 | P6 after P2-02/P5-04; P8 after P0-03/P3-02/P5-04 | Runtime/GUI and point-in-time replay/outcomes in parallel. |
| 5 | P7 | P0-04, P1-03, P2-02, P6-02 | First real creator handoff remains NORMAL at or below 10.00%, plus management review-only paths. |
| 6 | P9 | P7-01, P8-02, P8-03 | Durable signed current-policy/risk authorities, human decisions, and post-signing creator A-grade/rollback E2E. |
| 7 | P10 | P7-02, P9-02 | Process/remote-route adoption or approved creation, readiness, and evidence-gated rollout. |

No phase in one program wave depends on another phase in the same program wave. Inside each P-phase, every `depends_on` points to a lower local wave; same-wave plans have disjoint `files_modified` sets.

## 5. Executable plan index

| Phase | Local wave | Executable plan | Depends on | Objective |
|---|---:|---|---|---|
| P0 | 1 | [P0-01](plans/P0-01-PLAN.md) | — | Readiness and signed five-class MarketDataPacingCapability. |
| P0 | 1 | [P0-02](plans/P0-02-PLAN.md) | — | Authority audit, immutable workspace baseline, exact allowlists. |
| P0 | 1 | [P0-03](plans/P0-03-PLAN.md) | — | Signed Strategy NAV, Execution Cost, and Initial Champion/Scenario contracts. |
| P0 | 1 | [P0-04](plans/P0-04-PLAN.md) | — | Discover and sign one exact Managed Connector creator operation. |
| P1 | 1 | [P1-01](plans/P1-01-PLAN.md) | — | Strategy NAV ledger and exact risk-tier authority. |
| P1 | 1 | [P1-02](plans/P1-02-PLAN.md) | — | Component-specific atomic broker snapshot protocol. |
| P1 | 2 | [P1-03](plans/P1-03-PLAN.md) | P1-01, P1-02 | Reserve-time identical requery, calendar/DTE, live read-only proof. |
| P2 | 1 | [P2-01](plans/P2-01-PLAN.md) | — | GLD transition proof and snapshot-derived close/reduce generator. |
| P2 | 2 | [P2-02](plans/P2-02-PLAN.md) | P2-01 | Fail-closed production management adapter, approval/bridge preview, GUI, and real GLD dry run. |
| P3 | 1 | [P3-01](plans/P3-01-PLAN.md) | — | Point-in-time evidence persistence and directed migration. |
| P3 | 2 | [P3-02](plans/P3-02-PLAN.md) | P3-01 | Official event/calendar and supplemental providers, revocation-attested rotation, live provider proof. |
| P4 | 1 | [P4-01](plans/P4-01-PLAN.md) | — | Durable fixed-slot scheduler, run ledger, DecisionPipeline port. |
| P4 | 1 | [P4-02](plans/P4-02-PLAN.md) | — | UniverseFunnel and fail-closed P0-only pacing authority/budgets/cache/subscription hygiene. |
| P4 | 2 | [P4-03](plans/P4-03-PLAN.md) | P4-01, P4-02 | Live slot plus exact duplicate-tick assertion. |
| P5 | 1 | [P5-01](plans/P5-01-PLAN.md) | — | Finite-risk templates and cost/policy-bound candidates. |
| P5 | 1 | [P5-02](plans/P5-02-PLAN.md) | — | Volatility/scenario analytics, explanatory-only news flow, immutable post-event reaction core, positioning, and durable shadow-learning contracts under supporting-only gates. |
| P5 | 2 | [P5-03](plans/P5-03-PLAN.md) | P5-01, P5-02 | Diverse Top 3 and authorization ledger plus a separate immutable SUPPORTING_ONLY Top-10 preselection ledger. |
| P5 | 3 | [P5-04](plans/P5-04-PLAN.md) | P5-03 | Exact DecisionPipeline and full concurrency/tamper tests. |
| P6 | 1 | [P6-01](plans/P6-01-PLAN.md) | — | Resolver-aware DecisionPipeline plus pacing-gated production/news runtime, durable shadow learning/Top-10 composition, post-event reaction projection, and immutable rank-one API gates. |
| P6 | 2 | [P6-02](plans/P6-02-PLAN.md) | P6-01 | Authority-aware GUI, shadow-learning and news/calendar/reaction read models, and browser checkpoint. |
| P7 | 1 | [P7-01](plans/P7-01-PLAN.md) | — | Pure current-policy/risk-authority payload validation and one post-reservation transport call. |
| P7 | 2 | [P7-02](plans/P7-02-PLAN.md) | P7-01 | Live review instruction and separate human final IBKR decision. |
| P8 | 1 | [P8-01](plans/P8-01-PLAN.md) | — | Signed independence/clustering specification. |
| P8 | 2 | [P8-02](plans/P8-02-PLAN.md) | P8-01 | Point-in-time replay and walk-forward dataset. |
| P8 | 2 | [P8-03](plans/P8-03-PLAN.md) | P8-01 | Immutable cost-adjusted outcomes and independent counts. |
| P9 | 1 | [P9-01](plans/P9-01-PLAN.md) | — | Shadow evaluation, durable Promotion/Rollback authorities, current-policy resolver, and proposal-bound A-grade verifier. |
| P9 | 2 | [P9-02](plans/P9-02-PLAN.md) | P9-01 | AI governance UI, explicit human signing, and post-signing creator/rollback E2E. |
| P10 | 1 | [P10-01](plans/P10-01-PLAN.md) | — | Safe lifecycle, monitoring, backup/migration, runbook. |
| P10 | 2 | [P10-02](plans/P10-02-PLAN.md) | P10-01 | Human-gated 8891 route adoption/creation, process/remote evidence, and staged rollout. |

## 5A. Current-tree implementation inventory (2026-08-05)

This inventory synchronizes executable-plan scope and phase allowlists to the files currently present in the dirty shared worktree. File presence, unit coverage, or inclusion below is not phase-completion or live-acceptance evidence. The post-implementation human/live checkpoints in the executable plans remain authoritative.

| Area | Current implementation inventory | Current safety/readiness statement |
|---|---|---|
| GLD management | `positions/runtime_adapter.py` reuses the P2 proof/generator and is composed by `runtime.py`. | Preview-only and review-only. The production Exit Contract provider is intentionally unavailable, so current production management remains structured `NO_TRADE`; no approval, Creator, instruction, or order submission is implied. |
| Pacing and production decision runtime | `operations/pacing_authority.py`, `production_runtime.py`, the read-only IBKR gateway/session calendar, scanner loop, deterministic cost/payoff/risk gates, and production composition are present. | Missing, stale, future, tampered, ambiguous, wrong-actor, or partial pacing authority keeps the gateway/scanner closed. Production sets `CREATOR_TRANSPORT_UNAVAILABLE`, `review_only=true`, and `direct_order_submission=false`. |
| News, official calendar, and post-event reaction runtime | `news_runtime.py`, strict FOMC/BEA/BLS sources, provider/calendar evidence persistence, conflict retention, IBKR research bindings, the hash-chained `news/reaction.py` state machine, and a scoped reaction-provider projection are present. | All event/calendar/reaction/model/option-impact rows are `SUPPORTING_ONLY`. Missing, malformed, duplicate, tampered, or misbound reaction ledgers preserve calendar facts but become `UNAVAILABLE`/`CONFLICTED` and `NO_TRADE`; no live reaction provider is inferred from the state-machine seam. |
| Optional LLM flow | Deterministic classification plus explicitly enabled, cost-bounded, schema-validated DeepSeek classification is present. | The LLM is explanatory only and cannot invent symbols/evidence, option legs, trades, approvals, instructions, or orders. Failure falls back to deterministic research classification. |
| Positioning and durable shadow learning | `analytics/positioning_runtime.py` remains a deterministic supporting-only core. `learning_shadow.py` is an append-only SQLite/WAL/FULL Champion/Challenger ledger now owned by runtime at `shadow_learning.sqlite3`, exposed through GET-only summary/record/replay/similarity APIs, and rendered by the GUI. | Learning remains `SHADOW_ONLY`; challenger samples never combine, thirty independent resolved rows reach `DISCOVERY` only, and automatic promotion, production-weight/rule mutation, A-grade unlock, approval, bridge, and order authority stay false. This does not close P8 evaluation or human promotion/rollback acceptance. |
| Independent Top-10 preselection ledger | `news/preselection_store.py` provides an append-only `news_preselection.sqlite3` premarket ledger, frozen eight-field structure identity, lineage-bound open observations, restart replay, and exact coverage through `NewsCoordinator`. | Every row is `SUPPORTING_ONLY` and separate from Top-3 authorization. The open-observation store exists, but the production open-repricing producer remains `UNAVAILABLE` with `PENDING_FULL_IBKR_8_FIELD_IDENTITY_FREEZE`; tests that append typed observations do not prove that producer exists. |
| Learning/news/calendar API and GUI | `/api/learning`, `/api/news`, `/api/calendar`, strict projections, shadow-learning status, a dedicated news workbench, transparent scores, official-calendar provenance, Top-10 lineage/coverage, and compact post-event reaction cards are present. | The learning/news/calendar surfaces have no promotion/approval/instruction/order handler. Missing fields remain visibly unavailable; provider-claimed authority and internal/credential/model/broker/order fields are discarded. |
| Jin10 revocation gate | Strict human attestation, one-time rotation reservation, CLI/status, DPAPI setter gate, and zero-material tests are present. | No live attestation or replacement is asserted. A token alone never composes a Jin10 provider; production Jin10 wiring remains disabled until human revocation/replacement and a verified transport contract are complete. |

Explicitly unfinished and not represented as complete by this sync:

- the production open-repricing producer and its complete frozen eight-field IBKR writer;
- a configured live post-event reaction provider/ingestion path (the state machine, runtime seam, API, and GUI are present but do not manufacture provider data);
- P8 replay/evaluation acceptance and human promotion/rollback (the composed shadow ledger remains observation-only and cannot mutate production);
- production Jin10 transport/configuration and its named live provider checkpoint.

## 6. Canonical DecisionPipeline and immutable ranking contract

The only decision sequence is:

```text
scheduler slot
  -> UniverseFunnel
  -> broker/options evidence acquisition
  -> finite-risk strategy templates and generator
  -> CurrentPolicyResolver + versioned RiskTierAuthority resolution
  -> volatility/scenario engine
  -> signed Execution Cost Contract
  -> hard eligibility/risk/DTE/one-combination gates
  -> PortfolioRanker
  -> immutable SQLite/WAL Top3 RankingStore
  -> RuntimeServices/API/GUI read models
```

`RuntimeServices` injects every component in that sequence plus Strategy NAV, the immutable initial-policy fallback, durable CurrentPolicyResolver, versioned current RiskTierAuthority resolver, PositionManager, ApprovalStore, and bridge status reader. DecisionPipeline resolves both authorities once and passes the same immutable identities to ScenarioEngine and PortfolioRanker. The ranking ledger stores `scan_run_id`, canonical `ranking_basis_hash`, final `ranking_snapshot_id/hash`, input/evidence/broker-snapshot/candidate/cost hashes, `current_policy_version/hash`, `policy_authority_marker_hash`, `risk_authority_version`, non-null `risk_authority_marker_hash`, ranks 1–3, score components, timestamps/expiry, schema version, previous hash, and row hash. The ranking basis deliberately precedes a proposal-bound A-grade signature; the final row includes the resulting signed marker hash, avoiding a self-hash cycle. Authorization runs in an immediate transaction, rereads current untampered rank 1 and both resolver heads, and copies all authority identities into approval and bridge intent. Ranks 2–3, `A_GRADE_PENDING`, stale, revoked, rolled-back, tampered, or concurrently superseded snapshots add no approval/bridge row.

`ProductionComposition` owns one read-only IBKR gateway and shares it across atomic broker evidence, broker-published session hours, the fixed-slot scanner, GLD management preview, and news option-research bindings. Its lifecycle cannot connect until the current signed pacing authority validates. It deliberately installs a Creator-unavailable approval blocker and no order-submission primitive. `NewsCoordinator` is a separate point-in-time research path: official/provider/calendar/LLM evidence may contribute only capped `SUPPORTING_ONLY` fields and cannot bypass MARKET/VOLATILITY/LIQUIDITY, payoff, cost, DTE, one-combination, rank-one, approval, or Creator gates.

## 7. Atomicity, pacing, cost, policy, creator, and secret contracts

### Broker snapshot atomicity

- Account, positions, working orders, and unsubmitted instructions use canonical pre/post state hashes.
- Every secdef uses a pre/post identity hash of `conId`, `localSymbol`, `tradingClass`, `multiplier`, `exchange`, `expiry`, `strike`, and `right`.
- Quotes use one batch with batch ID, per-leg request IDs, requested/observed/completed timestamps, bid/ask values, source, maximum age, and maximum leg skew. Changing live prices are never required to be equal.
- State or secdef mutation returns `MUTATED_DURING_BUILD`. Missing/stale/over-skew quote evidence returns `QUOTE_INCOHERENT`.
- Reserve-time requery uses the identical protocol after the SQLite write lock and binds the new immutable snapshot hash before the one-attempt reservation.

### Market-data pacing

P0 produces one signed `MarketDataPacingCapability` for exactly scanner, secdef, snapshot_quote, streaming_quote, and historical classes. Each class carries `max_concurrency`, `request_window`, `max_requests`, and `cooldown`; the artifact carries `source=broker_disclosed|observed|conservative_default`, `observed_at`, version, hash, and signer. Broker-unavailable limits require explicit human-approved execution-time conservative values. P4 derives every budget only from that verified artifact and issues zero requests when it is missing, stale, partial, unsigned, or wrong-version.

### Execution Cost Contract

Before P5, a human signs real-account commissions, exchange/regulatory fees, per-leg spread/slippage, assignment, early-exercise, ex-dividend assumptions, conservative fallback, calibration provenance/date/sample, correction/versioning, and hash. GLD candidates, transition proofs, strategy candidates, ranking rows, replay/outcomes, approvals, and creator payloads all bind its version/hash. No candidate or approval exists without it.

### Initial Champion, current policy, and risk-tier authority

Before P5, a human signs probability methodology, calibration data/provenance, baseline weights, supporting-evidence caps, NO_TRADE thresholds, version/hash, and append-only corrections. P5 defines the resolver port and uses this immutable artifact as the current fallback/source of truth. P6 propagates `current_policy_version/hash`, `policy_authority_marker_hash`, and versioned risk-authority identity through decisions, ranking, and approval. P7 validates the same values through bridge intent and creator payload. P9 evaluates Challenger evidence but cannot automatically overwrite the baseline: only the explicit human `sign-promotion`, `sign-a-grade`, or `sign-rollback` CLI verbs append signed authority. Durable `PromotionAuthority` and `RollbackAuthority` records drive a restart-safe CurrentPolicyResolver; rollback atomically restores the prior signed policy and revokes A-grade markers bound to the rolled-back head. A-grade markers are proposal-specific and bind proposal/current-policy/policy-authority/cost/ranking-basis/risk/evaluation/dataset/independence hashes. NORMAL remains at most 10.00%; 10.01–15.00 requires that exact current marker; 15.01, 20.00, and 20.01 reject.

### Creator transport and one-attempt adapter

P0 discovers, probes, selects, and signs one installed Managed Connector operation with connector ID, tool name, exact input/output JSON schemas, auth isolation, HTTPS host/path, reconciliation/idempotency behavior, capability evidence, version/hash, and signer/time. No supported operation blocks P7; there is no browser/order fallback. P7 separates pure `prepare_and_validate_payload` before reservation from external `create_review_instruction` after reservation. Preparation consumes transactionally current policy/risk resolutions and requires identical authority hashes in frozen ranking, approval, bridge intent, reservation, and payload. P7's first real E2E remains NORMAL at or below 10.00%. Only after P9 human signing does P9-02 exercise the 10.01/15.00 A-grade creator path and exact 15.01/20.00/20.01 plus stale/tampered/revoked/rolled-back rejection matrix. Uncertain result is terminal UNKNOWN and the human alone performs final IBKR confirmation.

### Remote 8891 route adoption and ownership

The reported live state is useful but drift-prone: the expected separate tailnet-only route is `https://davidxu.tail6885a3.ts.net:8891/ -> http://127.0.0.1:8891`, while protected root `https://davidxu.tail6885a3.ts.net/ -> http://127.0.0.1:8878` must remain unchanged. P10 implementation and automated tests consume saved route/listener fixtures and never invoke Tailscale. The autonomous:false P10-02 human checkpoint re-runs `tailscale serve status --json` and listener capture. An exact existing 8891 mapping is `ADOPTED_PREEXISTING` with no mutation/removal ownership. An absent mapping requires separate explicit human approval before the exact `tailscale serve --bg --https=8891 http://127.0.0.1:8891` command. Failure may run targeted `tailscale serve --https=8891 off` only when before/after hashes and a `CREATED_BY_P10_02_01` marker prove that checkpoint created the exact handler; adopted/pre-existing/ambiguous/root routes are never removed, and `tailscale serve reset` is forbidden. Acceptance requires local and actual remote `/health`, `/api/bootstrap`, `/api/positions`, `/api/candidates`, and `/api/learning`, protected-route identity, and no-leftover evidence.

### Jin10 revocation attestation

The replacement setter refuses ordinary Jin10 `set` and refuses to prompt without a valid signed `old_token_revoked=true` artifact containing the matching human actor/signer, UTC time, version/hash, and zero token material. Strict parsing rejects duplicate/extra/missing fields, replay, wrong names/actors, stale/future time, and canonical-hash drift. Rotation reservation consumes one attestation once and the manifest binds its hash. The replacement is accepted only through a hidden prompt and stored only with DPAPI; source, logs, evidence, process command lines, and history contain no token value, fragment, fingerprint, or auth header. This local human attestation is not a provider-issued cryptographic revocation receipt and never enables Jin10 by itself. `OptionsCopilotRuntime` continues to omit Jin10 until both the human revocation/replacement checkpoint and a separately verified production transport contract succeed.

## 8. Exact master phase allowlists

These lists are normative and equal the set union of each linked phase plan's `files_modified`. The executable consistency test rejects both omissions and surplus entries in a plan or this master.

### P0 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P0/creator-transport/`
- `data/options_copilot/evidence/checkpoints/P0/execution-cost-contract/`
- `data/options_copilot/evidence/checkpoints/P0/initial-policy/`
- `data/options_copilot/evidence/checkpoints/P0/market-data-pacing/`
- `data/options_copilot/evidence/checkpoints/P0/strategy-nav-contract/`
- `data/options_copilot/evidence/workspace/P0/`
- `options_copilot/api/app.py`
- `options_copilot/bridge/creator_transport.py`
- `options_copilot/gateway/ibkr_readonly.py`
- `options_copilot/governance/cli.py`
- `options_copilot/governance/contracts.py`
- `options_copilot/governance/creator_transport_contract.v1.json`
- `options_copilot/governance/execution_cost_contract.v1.json`
- `options_copilot/governance/initial_champion_scenario_policy.v1.json`
- `options_copilot/governance/strategy_nav_contract.v1.json`
- `options_copilot/operations/authority_audit.py`
- `options_copilot/operations/capabilities.py`
- `options_copilot/operations/creator_probe.py`
- `options_copilot/operations/phase_allowlists.json`
- `options_copilot/operations/readiness.py`
- `options_copilot/operations/workspace_guard.py`
- `tests/options_copilot/test_authority_audit.py`
- `tests/options_copilot/test_creator_transport.py`
- `tests/options_copilot/test_governance_contracts.py`
- `tests/options_copilot/test_plan_allowlist_consistency.py`
- `tests/options_copilot/test_readiness_cli.py`
- `tests/options_copilot/test_readiness.py`
- `tests/options_copilot/test_workspace_guard.py`

### P1 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P1/ibkr-live-read/`
- `options_copilot/bridge/coordinator.py`
- `options_copilot/bridge/store.py`
- `options_copilot/gateway/broker_snapshot.py`
- `options_copilot/gateway/ibkr_readonly.py`
- `options_copilot/market/session_calendar.py`
- `options_copilot/performance/campaign.py`
- `options_copilot/performance/nav_ledger.py`
- `options_copilot/proposals.py`
- `options_copilot/risk/authorization.py`
- `options_copilot/risk/policy.py`
- `options_copilot/risk/time_policy.py`
- `tests/options_copilot/test_bridge_coordinator.py`
- `tests/options_copilot/test_broker_snapshot.py`
- `tests/options_copilot/test_ibkr_readonly_gateway.py`
- `tests/options_copilot/test_market_session_calendar.py`
- `tests/options_copilot/test_nav_ledger.py`
- `tests/options_copilot/test_proposal_gate.py`

### P2 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P2/gld-dry-run/`
- `options_copilot/api/app.py`
- `options_copilot/approval/store.py`
- `options_copilot/bridge/coordinator.py`
- `options_copilot/frontend/app.js`
- `options_copilot/frontend/index.html`
- `options_copilot/frontend/styles.css`
- `options_copilot/positions/__init__.py`
- `options_copilot/positions/cli.py`
- `options_copilot/positions/generator.py`
- `options_copilot/positions/manager.py`
- `options_copilot/positions/models.py`
- `options_copilot/positions/runtime_adapter.py`
- `options_copilot/proposals.py`
- `tests/options_copilot/test_api_frontend.py`
- `tests/options_copilot/test_bridge_coordinator.py`
- `tests/options_copilot/test_gld_management_pipeline.py`
- `tests/options_copilot/test_position_management_runtime_adapter.py`
- `tests/options_copilot/test_position_manager.py`
- `tests/options_copilot/test_runtime.py`

### P3 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P3/jin10-rotation/`
- `data/options_copilot/evidence/checkpoints/P3/provider-live/`
- `options_copilot/providers/__init__.py`
- `options_copilot/providers/cli.py`
- `options_copilot/providers/events.py`
- `options_copilot/providers/jin10.py`
- `options_copilot/providers/official.py`
- `options_copilot/providers/official_sources.py`
- `options_copilot/security/cli.py`
- `options_copilot/security/revocation.py`
- `options_copilot/storage/evidence.py`
- `scripts/set_options_copilot_secret.ps1`
- `tests/options_copilot/fixtures/evidence_v1.db`
- `tests/options_copilot/test_config_security.py`
- `tests/options_copilot/test_event_providers.py`
- `tests/options_copilot/test_evidence_store.py`
- `tests/options_copilot/test_official_calendar_provider.py`
- `tests/options_copilot/test_official_sources.py`

### P4 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P4/scanner-live/`
- `options_copilot/config.py`
- `options_copilot/gateway/ibkr_readonly.py`
- `options_copilot/operations/pacing_authority.py`
- `options_copilot/operations/readiness.py`
- `options_copilot/scanner/__init__.py`
- `options_copilot/scanner/cli.py`
- `options_copilot/scanner/pacing.py`
- `options_copilot/scanner/scheduler.py`
- `options_copilot/scanner/service.py`
- `options_copilot/scanner/universe.py`
- `tests/options_copilot/test_pacing_authority.py`
- `tests/options_copilot/test_scan_scheduler.py`
- `tests/options_copilot/test_scanner_authority.py`
- `tests/options_copilot/test_scanner_pacing.py`
- `tests/options_copilot/test_universe_funnel.py`

### P5 declared phase allowlist

- `options_copilot/analytics/__init__.py`
- `options_copilot/analytics/positioning_runtime.py`
- `options_copilot/analytics/scenarios.py`
- `options_copilot/analytics/volatility.py`
- `options_copilot/decision/__init__.py`
- `options_copilot/decision/pipeline.py`
- `options_copilot/domain/models.py`
- `options_copilot/execution_cost.py`
- `options_copilot/learning_shadow.py`
- `options_copilot/news/__init__.py`
- `options_copilot/news/calendar.py`
- `options_copilot/news/classifier.py`
- `options_copilot/news/composition.py`
- `options_copilot/news/deepseek.py`
- `options_copilot/news/learning.py`
- `options_copilot/news/models.py`
- `options_copilot/news/preselection.py`
- `options_copilot/news/preselection_store.py`
- `options_copilot/news/reaction.py`
- `options_copilot/news/scoring.py`
- `options_copilot/news/service.py`
- `options_copilot/ranking/__init__.py`
- `options_copilot/ranking/basis.py`
- `options_copilot/ranking/engine.py`
- `options_copilot/ranking/evidence_manifest.py`
- `options_copilot/ranking/portfolio.py`
- `options_copilot/ranking/store.py`
- `options_copilot/scanner/service.py`
- `options_copilot/strategies/__init__.py`
- `options_copilot/strategies/generator.py`
- `options_copilot/strategies/templates.py`
- `tests/options_copilot/test_decision_pipeline.py`
- `tests/options_copilot/test_execution_cost_resolver.py`
- `tests/options_copilot/test_learning_shadow.py`
- `tests/options_copilot/test_news_composition.py`
- `tests/options_copilot/test_news_core.py`
- `tests/options_copilot/test_news_deepseek.py`
- `tests/options_copilot/test_news_preselection_store.py`
- `tests/options_copilot/test_news_reaction.py`
- `tests/options_copilot/test_portfolio_ranking.py`
- `tests/options_copilot/test_positioning_runtime.py`
- `tests/options_copilot/test_ranking_store.py`
- `tests/options_copilot/test_scenarios.py`
- `tests/options_copilot/test_strategy_generator.py`
- `tests/options_copilot/test_strategy_templates.py`
- `tests/options_copilot/test_volatility_scenarios.py`

### P6 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P6/browser/`
- `options_copilot/UI_SPEC.md`
- `options_copilot/api/__init__.py`
- `options_copilot/api/app.py`
- `options_copilot/approval/__init__.py`
- `options_copilot/approval/proofs.py`
- `options_copilot/approval/store.py`
- `options_copilot/bridge/__init__.py`
- `options_copilot/bridge/store.py`
- `options_copilot/config.py`
- `options_copilot/decision/pipeline.py`
- `options_copilot/frontend/app.js`
- `options_copilot/frontend/index.html`
- `options_copilot/frontend/styles.css`
- `options_copilot/gateway/__init__.py`
- `options_copilot/gateway/ibkr_readonly.py`
- `options_copilot/market/__init__.py`
- `options_copilot/market/session_calendar.py`
- `options_copilot/news_runtime.py`
- `options_copilot/production_runtime.py`
- `options_copilot/ranking/__init__.py`
- `options_copilot/ranking/basis.py`
- `options_copilot/ranking/evidence_manifest.py`
- `options_copilot/ranking/portfolio.py`
- `options_copilot/ranking/store.py`
- `options_copilot/risk/__init__.py`
- `options_copilot/risk/resolver.py`
- `options_copilot/runtime.py`
- `tests/options_copilot/test_api.py`
- `tests/options_copilot/test_api_frontend.py`
- `tests/options_copilot/test_approval_store.py`
- `tests/options_copilot/test_frontend_contract.py`
- `tests/options_copilot/test_learning_frontend.py`
- `tests/options_copilot/test_learning_runtime_api.py`
- `tests/options_copilot/test_news_option_frontend.py`
- `tests/options_copilot/test_news_preselection_runtime.py`
- `tests/options_copilot/test_news_reaction_frontend.py`
- `tests/options_copilot/test_news_runtime.py`
- `tests/options_copilot/test_news_runtime_official_calendar.py`
- `tests/options_copilot/test_news_runtime_reaction.py`
- `tests/options_copilot/test_official_calendar_api_projection.py`
- `tests/options_copilot/test_p6_api_gui.py`
- `tests/options_copilot/test_production_candidate_e2e.py`
- `tests/options_copilot/test_production_runtime.py`
- `tests/options_copilot/test_ranking_store.py`
- `tests/options_copilot/test_risk_authority_resolver.py`
- `tests/options_copilot/test_runtime.py`
- `tests/options_copilot/test_runtime_pipeline.py`
- `tests/options_copilot/test_runtime_wiring_completeness.py`

### P7 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P7/creator-live/`
- `options_copilot/README.md`
- `options_copilot/bridge/__main__.py`
- `options_copilot/bridge/coordinator.py`
- `options_copilot/bridge/creator_adapter.py`
- `options_copilot/bridge/store.py`
- `tests/options_copilot/test_bridge_coordinator.py`
- `tests/options_copilot/test_creator_adapter.py`
- `tests/options_copilot/test_gld_creator_e2e.py`

### P8 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P8/independence-spec/`
- `options_copilot/governance/independence_spec.v1.json`
- `options_copilot/learning/outcomes.py`
- `options_copilot/replay/dataset.py`
- `options_copilot/replay/engine.py`
- `options_copilot/storage/canonical.py`
- `options_copilot/storage/ledger.py`
- `tests/options_copilot/test_learning_storage_approval.py`
- `tests/options_copilot/test_outcomes.py`
- `tests/options_copilot/test_replay.py`

### P9 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P9/a-grade-decision/`
- `data/options_copilot/governance/a_grade_markers/`
- `data/options_copilot/governance/policy_authority.sqlite3`
- `data/options_copilot/governance/promotion_authorities/`
- `data/options_copilot/governance/rollback_authorities/`
- `options_copilot/AI_SPEC.md`
- `options_copilot/api/app.py`
- `options_copilot/frontend/app.js`
- `options_copilot/learning/evaluation.py`
- `options_copilot/learning/governance.py`
- `options_copilot/learning/governance_cli.py`
- `options_copilot/learning/markers.py`
- `options_copilot/learning/policy_authority.py`
- `options_copilot/learning/similarity.py`
- `options_copilot/risk/authorization.py`
- `options_copilot/runtime.py`
- `tests/options_copilot/test_a_grade_creator_e2e.py`
- `tests/options_copilot/test_a_grade_marker.py`
- `tests/options_copilot/test_api_frontend.py`
- `tests/options_copilot/test_learning_evaluation.py`
- `tests/options_copilot/test_policy_authority.py`

### P10 declared phase allowlist

- `data/options_copilot/evidence/checkpoints/P10/process-ports-tailscale/`
- `data/options_copilot/evidence/checkpoints/P10/rollout/`
- `options_copilot/api/app.py`
- `options_copilot/operations/migrations.md`
- `options_copilot/operations/monitoring.py`
- `options_copilot/operations/readiness.py`
- `options_copilot/operations/runbook.md`
- `scripts/start_options_copilot.ps1`
- `tests/options_copilot/test_process_operations.py`
- `tests/options_copilot/test_readiness_cli.py`

## 9. Requirement-to-plan coverage

| Requirement | Executable plans |
|---|---|
| OC-SAFE-01 | P0-01, P0-02, P0-04, P1-02, P7-01, P7-02, P10-01, P10-02 |
| OC-SAFE-02 | P1-01, P2-01, P5-01 |
| OC-RISK-01 | P0-03, P1-01, P1-03, P5-01, P5-02, P5-03, P6-01, P7-01, P9-01, P9-02 |
| OC-RISK-02 | P2-01, P2-02, P4-02, P4-03 |
| OC-TIME-01 | P1-03, P3-01, P3-02, P4-01, P4-02, P5-01 |
| OC-DATA-01 | P1-02, P1-03, P2-01, P2-02, P7-01, P7-02, P10-01, P10-02 |
| OC-DATA-02 | P0-01, P3-01, P4-02, P4-03, P5-04, P6-01 |
| OC-EVENT-01 | P3-01, P3-02, P5-02, P6-01, P6-02 |
| OC-ANALYTICS-01 | P0-03, P3-02, P5-02, P5-03, P5-04, P6-01, P6-02, P8-01, P8-02, P8-03 |
| OC-STRAT-01 | P0-03, P5-01, P5-02, P5-03, P5-04, P6-01, P6-02, P7-01, P8-02 |
| OC-EXIT-01 | P2-01, P2-02, P5-01, P6-01, P6-02, P8-02, P8-03 |
| OC-SCAN-01 | P4-01, P4-02, P4-03, P5-04, P6-01, P10-01, P10-02 |
| OC-BRIDGE-01 | P0-04, P1-03, P2-02, P5-03, P6-01, P6-02, P7-01, P7-02, P9-01, P9-02 |
| OC-LEARN-01 | P0-03, P5-02, P6-01, P7-01, P8-01, P8-02, P8-03, P9-01, P9-02 |
| OC-SEC-01 | P0-04, P3-02, P6-01, P7-01, P7-02, P9-01, P9-02 |
| OC-OPS-01 | P0-01, P0-02, P4-01, P6-02, P10-01, P10-02 |
| OC-VAL-01 | P0-01 through P10-02; every executable plan contains automated verification and named live/manual status where applicable. |

Coverage: **17/17 requirements** and **P0–P10 all covered**.

## 10. Retained prior-finding closure audit

| Closed safety finding | Retained closure |
|---|---|
| 1. Review-only authority | P0 repository transitive authority audit, P7 exact review-only contract, human final IBKR action. |
| 2. Strategy NAV and risk boundaries | Signed NAV contract/ledger; versioned current RiskTierAuthority; proposal/policy/cost/ranking-bound P9 marker; exact 10.00/10.01, 15.00/15.01, 20.00/20.01 creator E2E. |
| 3. GLD management | Authoritative snapshot-derived CLOSE_ALL/REDUCE_RISK proof; entry remains blocked. |
| 4. Dynamic creator payload | ConIds/sides/ratios/quantity/signed limit are derived and all hashes validated. |
| 5. Atomic broker truth | State/secdef pre/post plus coherent quote batch; identical reserve-time protocol. |
| 6. Real two-hour scheduler | Fixed authoritative slots, lease/idempotency, bounded recovery, exact duplicate tick proof. |
| 7. Calendar and DTE | Broker-published calendar, DST/holiday/early-close, normal 14–35 and permanent 7 floor. |
| 8. Supporting-only analytics | News/earnings/calendar/reaction/model/positioning caps, independent Top-10 lineage/coverage, strict API/GUI projections, and independent MARKET/VOLATILITY/LIQUIDITY negative tests. |
| 9. Rank-one authorization | Immutable SQLite/WAL Top3 ledger, ranking-basis/final-row authority binding, and transactional current rank-one/resolver reread. |
| 10. Independent learning samples | Durable runtime-owned shadow ledger, challenger-scoped counts, GET-only API/GUI status, signed clustering spec, duplicates/correlation count once, 30 scenes Discovery only. |
| 11. Named live evidence | P0 pacing/creator, P1 IBKR, P2 GLD, P3 rotation/providers, P4 scanner, P6 browser, P7 creator/human, P9 decision, P10 operations/rollout. |
| 12. Pacing/cache/subscription hygiene | Signed per-class limits, TTL provenance, cancel exactly once, subscription baseline restored. |
| 13. Workspace and operations isolation | Pre-edit baseline, exact missing/surplus allowlists, process and route ownership proof, adopted-vs-created 8891 handling, protected root/ports, G: backups, remote read APIs, and no leftovers. |

## 11. Nyquist validation and future commands

The task/wave/requirement validation contract is [IMPLEMENTATION_VALIDATION.md](IMPLEMENTATION_VALIDATION.md). Every command inside executable task `<verify>` blocks and every live/operational command in this master is explicitly **post-implementation only** unless it validates these planning artifacts themselves. No current runtime, broker, provider, creator, process, port, or Tailscale state is changed by planning.

Planning-artifact validation commands that may run now:

```powershell
Get-ChildItem options_copilot/plans/*-PLAN.md | ForEach-Object {
  gsd-sdk query frontmatter.validate $_.FullName --schema plan
  gsd-sdk query verify.plan-structure $_.FullName
}
```

Future phase execution begins only after independent verification and human approval. The master does not authorize automatic execution.

## 12. Multi-source coverage audit

| Source | Item | Executable coverage | Status |
|---|---|---|---|
| GOAL | Human-gated evidence-driven options research and IBKR review instruction, never local execution | P0, P5, P6, P7, P10 | COVERED |
| REQ | 17 OC requirements | Requirement table above | COVERED 17/17 |
| RESEARCH | Data/decision/approval/bridge/learning separation | P3, P5, P6, P7, P8, P9 | COVERED |
| RESEARCH | Atomic broker truth, pacing, provider provenance, replay, operations | P0, P1, P3, P4, P8, P10 | COVERED |
| CONTEXT | Separate news/calendar GUI, transparent scores, official provenance, and explanatory-only LLM | P3-02, P5-02, P6-01, P6-02 | COVERED IN PLAN; SUPPORTING_ONLY |
| CONTEXT | Strategy NAV sizing and exact risk/DTE rules | P0-03, P1, P2, P5, P9 | COVERED |
| CONTEXT | GLD close/reduce-only and one combination | P2, P4, P5, P10 | COVERED |
| CONTEXT | Cost contract, immutable initial fallback, and signed current-policy/risk authority | P0-03 with bindings through P2/P5/P6/P7/P8/P9 | COVERED |
| CONTEXT | Concrete creator transport and human final confirmation | P0-04, P7-01, P7-02 | COVERED |
| CONTEXT | Secure Jin10 revocation and replacement | P3-02 | COVERED IN PLAN; production transport disabled pending human/live gates |
| CONTEXT | Exact workspace allowlists and named validation | P0-02 plus IMPLEMENTATION_VALIDATION.md | COVERED |
| CONTEXT | Existing-or-approved separate :8891 route without changing protected root/443 | P10-01 pure ownership/restoration tests plus P10-02 autonomous:false human checkpoint | COVERED |

No deferred order authority or unplanned required item exists. Real entitlements, provider contracts, executable GLD quotes and Exit Contract, pacing, creator behavior, human-signed promotion/A-grade/rollback, browser outcome, and process/route/remote health remain named post-implementation checkpoints; mocks never satisfy them. The independent Top-10 ledger, post-event state machine/seam, and durable shadow-learning read surface are present but remain non-authoritative. The open-repricing producer, live reaction-provider ingestion, P8 evaluation/promotion acceptance, and production Jin10 are explicitly pending rather than silently counted as delivered.

---

## PLANNING COMPLETE

- Master artifact: non-executable meta-plan
- Executable phases: P0–P10
- Executable plans: 29
- Program waves: 8
- Requirements: 17/17
- Automatic execution: disabled
