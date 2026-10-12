---
phase: P1-strategy-nav-broker-snapshot
plan: "01"
subsystem: strategy-nav-risk-authority
tags: [strategy-nav, append-only-ledger, risk-authority, decimal-boundaries, fail-closed]

# Dependency graph
requires:
  - phase: P0-capability-baseline
    provides: signed Strategy NAV contract v1 and locked 10/15/20 policy
provides:
  - immutable cash-flow-attributed Strategy NAV snapshots
  - NORMAL and P9 A-grade RiskTierAuthority resolution
  - production risk assessment bound to Strategy NAV contract and ledger head hashes
affects: [P2, P5, P6, P7, P8, P9]

# Tech tracking
tech-stack:
  added: []
  patterns: [SQLite WAL FULL, append-only triggers, canonical SHA-256 chain, Decimal-only risk]

key-files:
  created:
    - options_copilot/performance/nav_ledger.py
    - options_copilot/risk/authorization.py
    - tests/options_copilot/test_nav_ledger.py
  modified:
    - options_copilot/performance/campaign.py
    - options_copilot/proposals.py
    - options_copilot/risk/policy.py
    - tests/options_copilot/test_campaign.py
    - tests/options_copilot/test_proposal_gate.py

key-decisions:
  - "Signed Strategy NAV is the only production risk denominator; observed account NLV is reconciliation only."
  - "Campaign progress is display-only and carries no risk authority."
  - "NORMAL permits exactly 10.00 percent; a valid current P9 marker is required through 15.00 percent."
  - "Proposal booleans, environment variables, model output, rank, and campaign progress cannot unlock A-grade."
  - "Research payoff/risk calls retain their explicit account-equity compatibility path, while production mode requires hash-bound Strategy NAV."

requirements-completed: [OC-RISK-01, OC-SAFE-02, OC-VAL-01]

# Metrics
completed: 2026-08-04
tasks: 2 of 2
tests: 331 passed
---

# P1 Plan 01: Strategy NAV Ledger and Risk Authority Summary

P1-01 now provides the immutable capital base and exact risk-tier boundary needed by every later candidate, approval, and creator path.

## Accomplishments

- Added an immutable Strategy NAV anchor backed by the signed v1 contract, SQLite WAL/FULL durability, directed schema versioning, no-update/no-delete triggers, and a canonical hash chain.
- Added idempotent broker-flow identities, explicit conflict records, append-only corrections, correction-fork prevention, latest-per-position unrealized marks, and tamper detection.
- Strategy deposits, withdrawals, realized PnL, latest unrealized PnL, fees, and signed correction deltas use full Decimal precision and round only at the final cent snapshot.
- NON_STRATEGY activity and fill principal contribute exactly zero. Account NLV is retained only as `observed_account_nlv` plus a reconciliation difference.
- Campaign presentation can bind a valid Strategy NAV snapshot and exposes its snapshot, contract, and ledger-head hashes. The campaign has no API that changes risk ceilings.
- Added canonical NORMAL authority and strict P9 A-grade marker validation for phase, decision, append-only state, report/policy/dataset/independence/risk-contract hashes, human actor, time, expiry, revocation, rollback, governance signature, and content hash.
- Added production-bound RiskEngine mode. It validates Strategy NAV content, contract hash, ledger head, and RiskTierAuthority before assessment; passing `account_equity` into that mode is rejected.
- Preserved the existing explicit account-equity risk path for research compatibility. Proposal validation accepts A-grade only from a production-bound RiskEngine with valid authority; `a_grade_unlocked` remains a compatibility input with zero authority.

## Exact Boundaries

- NORMAL: 10.00% passes; 10.01% rejects.
- Valid P9 A-grade: 15.00% passes; 15.01% rejects.
- Hard line: 20.00% and 20.01% reject even with valid A-grade authority.
- Unsigned, wrong-phase, ordinary-promotion, wrong-report, revoked, rolled-back, expired, or hash-mismatched markers resolve to NORMAL and cannot expand risk.

## Task Commits

1. Strategy NAV ledger and Campaign binding: `51060a8`.
2. RiskTierAuthority and production risk binding: `ab9a67a`.

## Verification

- `python -m pytest -q tests/options_copilot` — 331 passed.
- Strategy NAV contract verifier — v1, signer `human:xujie`, and hash `536a938e...dbedd` passed.
- Authority audit — 49 production files, 689 callables, zero unsafe-authority violations.
- Targeted NAV, Campaign, proposal, payoff, ranking, and boundary suite — 85 passed.

## Runtime Status Observed at Completion

- Existing listeners remained unchanged on 8766, 8788, 8878, and 8891; no service or Tailscale route was restarted or replaced.
- All five Options GUI endpoints returned HTTP 200.
- `/health` remained DEGRADED because the managed IBKR snapshot was stale at `2026-08-02T22:58:59.193424+00:00`.
- `/api/candidates` remained `NO_TRADE`; the GLD 375C/-2x385C/390C combination still occupies the only open-combination slot.
- Creator transport remained handoff-only/unavailable. No instruction or order was created, modified, or submitted.

## Deviations and Limitations

- P1 establishes the production-bound risk component while retaining the older research call signature. P6 must inject the same immutable Strategy NAV and current RiskTierAuthority resolution through the complete DecisionPipeline and ranking/approval path.
- No live P9 A-grade marker exists. Production therefore remains NORMAL-only; tests use canonically signed isolated fixtures solely to prove the locked boundary behavior.
- This plan does not refresh the stale IBKR snapshot and does not authorize trading.

## Next Phase Readiness

- P1-02 may build one atomic broker snapshot whose account/order/secdef hashes and coherent quote batch become immutable risk inputs.
- P1-03 may add reserve-time rereads, US options calendar/DTE enforcement, and the real-market read-only checkpoint.

---
*Phase: P1-strategy-nav-broker-snapshot*
*Completed: 2026-08-04*
