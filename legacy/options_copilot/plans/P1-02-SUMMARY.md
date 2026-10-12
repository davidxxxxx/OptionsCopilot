---
phase: P1-strategy-nav-broker-snapshot
plan: "02"
subsystem: atomic-broker-snapshot
tags: [ibkr, read-only, atomic-snapshot, secdef, quote-coherence, fail-closed]

# Dependency graph
requires:
  - phase: P0-capability-baseline
    provides: read-only IBKR gateway and permanent no-order-authority contract
provides:
  - component-specific atomic broker snapshot builder and immutable snapshot hash
  - authoritative eight-field option secdef identity proof
  - one-batch executable quote evidence with exact age and leg-skew boundaries
affects: [P1-03, P2, P5, P6, P7, P10]

# Tech tracking
tech-stack:
  added: []
  patterns: [frozen dataclasses, canonical SHA-256 evidence, pre-post reconciliation, read-only IBKR batch]

key-files:
  created:
    - options_copilot/gateway/broker_snapshot.py
    - tests/options_copilot/test_broker_snapshot.py
  modified:
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/gateway/__init__.py
    - tests/options_copilot/test_ibkr_readonly_gateway.py

key-decisions:
  - "Account, positions, working orders, and unsubmitted instructions require equal canonical pre/post hashes; known empty is distinct from UNKNOWN."
  - "Secdef identity is exactly conId, localSymbol, tradingClass, multiplier, exchange, expiry, strike, and right, and must match both pre/post reads and the requested leg."
  - "Live prices are acquired once and never compared for equality; coherence is proven through batch/request identity, provenance, timestamps, executable bid/ask, age, and leg skew."
  - "State or secdef drift is MUTATED_DURING_BUILD, unknown state is STATE_INCOMPLETE, and missing/stale/partial/cancelled/timed-out quote evidence is QUOTE_INCOHERENT."

requirements-completed: [OC-DATA-01, OC-SAFE-01, OC-VAL-01]

# Metrics
completed: 2026-08-04
tasks: 2 of 2
tests: 373 passed
---

# P1 Plan 02: Atomic Broker Snapshot Summary

P1-02 now turns mutable IBKR account, inventory, order, contract-definition, and quote reads into one immutable approval-grade evidence object without requiring impossible equality from moving prices.

## Accomplishments

- Added `BrokerSnapshotBuilder` with canonical pre/post evidence for account, positions, working orders, and managed-connector unsubmitted instructions.
- Known empty orders or instructions remain authoritative zero-length state; an unavailable instruction reader produces UNKNOWN and `STATE_INCOMPLETE` rather than guessing empty.
- Added authoritative option secdef reads and per-leg evidence for exactly eight locked identity fields. Duplicate, missing, adjusted, non-USD, non-OPT, multiplier-not-100, request-mismatched, or mutated definitions fail closed.
- Added one-call quote batches with a batch ID plus per-leg batch ID, request ID, requested/observed/completed timestamps, source, bid/ask, Greeks, volume, OI, and market-data metadata.
- Quote price values may move independently and still pass. Missing/crossed/nonpositive executable prices, duplicate or partial ticker identities, absent provenance, timeout, cancellation, stale data, or excessive leg skew fail closed.
- Bound all canonical state hashes, secdef identities and provenance, quote metadata and values, completeness state, reason codes, and timestamps into a deterministic immutable snapshot hash with self-verification.
- Extended `IBKRReadOnlyGateway` only with read operations: working orders, managed instruction state, contract details, and snapshot quote batches. The authority audit confirms no broker-write callable was introduced.
- Exported the atomic snapshot and quote/secdef contracts through `options_copilot.gateway`.

## Exact Boundaries

- Oldest quote age: 5.000 seconds passes; 5.001 seconds returns `QUOTE_INCOHERENT`.
- Maximum leg observation skew: 2.000 seconds passes; 2.001 seconds returns `QUOTE_INCOHERENT`.
- Reordered state rows, secdefs, and quote rows retain the same canonical hash.
- Any account/position/order/instruction mutation or any of the eight secdef identity-field mutations returns `MUTATED_DURING_BUILD`.
- Complete empty working orders/instructions are accepted as known state; UNKNOWN state returns `STATE_INCOMPLETE`.
- Repeated requested conIds, adjusted contracts, nonstandard multipliers, and partial/duplicate quote legs fail closed.

## Task Commits

1. Component-specific atomic snapshot protocol: `81dcdbb`.
2. Read-only gateway contract proof and public exports: `ef96763`.

## Verification

- `python -m pytest -q tests/options_copilot/test_broker_snapshot.py tests/options_copilot/test_ibkr_readonly_gateway.py` — 46 passed.
- `python -m pytest -q tests/options_copilot` — 373 passed.
- `python -m pytest -q tests/options_copilot/test_authority_audit.py` — 8 passed.
- Authority audit — 50 production files, 727 callables, zero unsafe-authority violations.
- Black formatting check and staged whitespace checks passed for all P1-02 files.

## Runtime Status Observed at Completion

- Existing listeners remained unchanged on 8766, 8788, 8878, and 8891; no service or Tailscale route was restarted, replaced, or otherwise disturbed.
- Options GUI logs show PID 33852 started normally and the five read endpoints continued returning HTTP 200.
- `/health` remained `DEGRADED` because the managed IBKR snapshot was still stale at `2026-08-02T22:58:59.193424+00:00`.
- `/api/candidates` remained `NO_TRADE`; the GLD 375C/-2x385C/390C combination still occupies the sole open-combination slot.
- Creator transport remained unavailable/handoff-only. No instruction or order was created, modified, cancelled, or submitted.

## Deviations and Limitations

- P1-02 provides the immutable protocol and read-only gateway primitives; it does not yet replace the stale runtime snapshot source or wire reserve-time reconstruction into approval/bridge execution.
- Real IBKR entitlement, pacing, OPRA quote quality, disconnect behavior, and live secdef/quote evidence remain the explicit P1-03 read-only checkpoint. Until that proof exists, live approval remains blocked.
- The managed connector must supply the authoritative unsubmitted-instruction reader. Absence remains UNKNOWN by design.
- This plan does not authorize trading, create an IBKR instruction, or relax any risk/DTE/single-combination rule.

## Next Phase Readiness

- P1-03 can rebuild the same snapshot under the reservation lock, compare immutable proposal and reserve-time bindings, and enforce the US options calendar/DTE contract.
- P1-03 can run the separate real-market read-only checkpoint without adding local order authority.

---
*Phase: P1-strategy-nav-broker-snapshot*
*Completed: 2026-08-04*
