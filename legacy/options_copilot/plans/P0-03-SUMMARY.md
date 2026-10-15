---
phase: P0-capability-baseline
plan: "03"
subsystem: governance-contracts
tags: [strategy-nav, execution-cost, champion-policy, signed-contracts, fail-closed]

# Dependency graph
requires:
  - phase: P0-capability-baseline
    provides: capability, authority, and workspace guards from P0-01 and P0-02
provides:
  - signed Strategy NAV cash-flow authority with no account-NLV fallback
  - signed real-account Execution Cost Contract with conservative quote, fee, slippage, assignment, exercise, and dividend rules
  - signed frozen Initial Champion and Scenario Policy with supporting-only evidence caps and no automatic promotion
affects: [P1, P2, P5, P6, P7, P8, P9]

# Tech tracking
tech-stack:
  added: []
  patterns: [canonical JSON SHA-256, append-only contract correction, source-bound decision evidence, Decimal conservative fallback]

key-files:
  created:
    - options_copilot/governance/strategy_nav_contract.v1.json
    - options_copilot/governance/execution_cost_contract.v1.json
    - options_copilot/governance/initial_champion_scenario_policy.v1.json
    - data/options_copilot/evidence/checkpoints/P0/strategy-nav-contract/decision-evidence.v1.json
    - data/options_copilot/evidence/checkpoints/P0/execution-cost-contract/calibration-source.v1.json
    - data/options_copilot/evidence/checkpoints/P0/execution-cost-contract/decision-evidence.v1.json
    - data/options_copilot/evidence/checkpoints/P0/initial-policy/calibration-source.v1.json
    - data/options_copilot/evidence/checkpoints/P0/initial-policy/decision-evidence.v1.json
  modified:
    - tests/options_copilot/test_governance_contracts.py

key-decisions:
  - "Strategy NAV is the only risk denominator; account NLV is observation and reconciliation only."
  - "Observed IBKR trades calibrate commission, while missing NBBO and tail evidence use explicit conservative fail-closed fallbacks."
  - "New candidates reserve full round-trip costs; existing-position management charges only future costs."
  - "The initial Champion is a deterministic conservative rules policy, not an empirically calibrated profit model."
  - "News, earnings, Max Pain, walls, PCR, and estimated GEX are supporting-only and cannot affect probability, eligibility, or risk."
  - "Thirty independent samples permit Discovery only; production promotion and 15% A-grade risk remain separately human-approved."

requirements-completed: [OC-RISK-01, OC-ANALYTICS-01, OC-STRAT-01, OC-LEARN-01, OC-VAL-01]

# Metrics
completed: 2026-08-03
tasks: 4 of 4
tests: 311 passed
---

# P0 Plan 03: Signed Governance Inputs Summary

P0-03 now has all three immutable human-attested inputs required by later risk, ranking, replay, approval, and review-instruction phases.

## Accomplishments

- Implemented canonical signed-contract validation, version identity, consumer expectations, source provenance, exclusive creation, and append-only hash-linked corrections.
- Signed Strategy NAV v1 as `536a938ed773c606ac8c80e6c6a2046f68dff3ee41d43ae599743194ef2dbedd`.
- Calibrated Execution Cost v1 from nine IBKR option fills, twelve contract sides, three grouped execution events, and official IBKR fee schedules. Signed hash: `d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b`.
- Froze Initial Champion and Scenario Policy v1 with deterministic five-scenario probabilities, hard NO_TRADE thresholds, structure priorities, capped supporting evidence, and explicit zero eligible causal labels. Signed hash: `b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c`.
- Bound every approval evidence artifact to its contract hash, actor `human:xujie`, signing time, calibration source, and append-only correction policy.

## Execution Cost Policy

- Commission and third-party fallback per order is `max(1.00 USD, 1.25 USD * contract sides)`; a complete fresh broker estimate is used only when it is greater.
- Buy legs use ask and sell legs use bid. Additional adverse slippage reserves entry, planned exit, and event/stop stress; stale, missing, zero, locked, crossed, or incoherent quotes produce NO_TRADE.
- Planned assignment and exercise are prohibited. Unknown settlement exposure, missing dividend/calendar evidence, or an early-exercise trigger blocks entry or requires risk reduction.
- All arithmetic is Decimal. Costs retain full intermediate precision and round upward to cents at signed aggregation boundaries.
- A revision after at least 30 independently captured execution events is Discovery review only and still requires a new signed version.

## Initial Champion Policy

- The baseline uses bounded point-in-time market and volatility features, temperature-softmax, and 20% uniform shrinkage over STRONG_DOWN, DOWN, RANGE, UP, and STRONG_UP.
- There are zero eligible historical causal labels. WDC lacks decision-time contract/thesis/probability/NBBO evidence; GLD remains open. The policy therefore makes no empirical-calibration or profit claim.
- Hard eligibility requires MARKET, VOLATILITY, and LIQUIDITY evidence, standard secdefs, fresh coherent quotes, exact finite loss, signed costs, positive normal and nonnegative stress EV, DTE, Strategy NAV, portfolio, order, and instruction gates.
- Supporting evidence has at most three aggregate ranking points and no probability, eligibility, or risk effect.
- Only immutable rank one may enter approval. P9 may evaluate and propose a separately signed replacement, but cannot mutate or auto-promote this baseline.

## Task Commits

1. Contract tests and generic signed governance implementation: `c9e17d6`, `fde1ef9`, merged as `e4fd80d`.
2. Strategy NAV decision and signature: `dee66cc`, merged as `ed98f43`.
3. Execution Cost calibration, decision evidence, signature, and artifact tests: `a70a8df`.
4. Initial Champion policy, calibration evidence, signature, and artifact tests: `1a2e1b9`.

## Verification

- `python -m pytest -q tests/options_copilot` — 311 passed.
- Each of the three checked-in contracts passes the governance CLI verifier with its frozen kind, version, signer, effective time, and hash.
- Authority audit — 47 production files, 634 callables, zero unsafe-authority violations.
- P0 workspace guard — zero violations under `options_copilot/`, `tests/options_copilot/`, or `data/options_copilot/`. The shared worktree separately contains 992 out-of-scope non-options changes; none were cleaned, reverted, or staged by this plan.
- Listener and process inspection confirmed the existing services remained on 8766, 8788, 8878, and 8891; no service or Tailscale route was restarted or replaced.

## Runtime Status Observed at Completion

- Options GUI endpoints on 8891 returned HTTP 200.
- `/health` remained DEGRADED because the last managed IBKR snapshot was stale, not because of the governance changes.
- `/api/candidates` remained NO_TRADE with zero candidates.
- The legacy GLD 375C/-2x385C/390C combination still occupies the single allowed open-combination slot.
- Creator transport remains `CREATOR_TRANSPORT_UNAVAILABLE`/handoff-only. No instruction or order was created or submitted.

## Deviations and Limitations

- The connector trade history has no contemporaneous NBBO, so it cannot measure arrival-price slippage or spread capture. The signed fallback is deliberately conservative.
- The account pricing plan and exact venue-specific fee decomposition remain unknown; the all-in fallback governs until complete broker evidence exists.
- The initial probability policy is a frozen transparent bootstrap, not proof of edge. Learning cannot loosen risk or reach production automatically.

## Next Phase Readiness

- P1 may now implement Strategy NAV ledger, atomic broker snapshot, and time/risk gates against all three frozen contracts.
- P5 may later implement scenario generation and ranking, but current stale broker data and the open GLD combination continue to require NO_TRADE for new entries.
- P7 remains blocked by unavailable creator transport and requires a separate live review-instruction checkpoint.

---
*Phase: P0-capability-baseline*
*Completed: 2026-08-03*
