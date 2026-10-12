---
phase: P0-capability-baseline
plan: "01"
subsystem: operations
tags: [readiness, pacing, canonical-hash, human-approval, review-only]

requires:
  - phase: P0-safe-baseline
    provides: Immutable workspace baseline and isolated Options Copilot scope
provides:
  - Immutable fail-closed readiness and five-class market-data pacing contracts
  - Shared observation-only CLI and API readiness probes
  - Human-approved conservative pacing evidence with canonical hashes
affects: [P4-universe-scanner, operations-readiness, market-data-budgeting]

tech-stack:
  added: []
  patterns: [frozen capability records, canonical JSON hashes, immutable evidence runs]

key-files:
  created:
    - options_copilot/operations/capabilities.py
    - options_copilot/operations/readiness.py
    - tests/options_copilot/test_readiness.py
    - tests/options_copilot/test_readiness_cli.py
    - data/options_copilot/evidence/checkpoints/P0/market-data-pacing/20260803T142822655851Z-edbcfe34fb60/approval.json
    - data/options_copilot/evidence/checkpoints/P0/market-data-pacing/20260803T142822655851Z-edbcfe34fb60/capability.json
    - data/options_copilot/evidence/checkpoints/P0/market-data-pacing/20260803T142822655851Z-edbcfe34fb60/readiness.json
  modified:
    - options_copilot/gateway/ibkr_readonly.py
    - options_copilot/api/app.py

key-decisions:
  - "human:xujie explicitly approved the complete conservative_default pacing values for all five request classes."
  - "The capability signer is part of the canonical capability hash; no second hash-confirmation gate is required by the implemented signature semantics."
  - "Readiness remains observation-only, review_only=true, and direct_order_submission=false."

patterns-established:
  - "Fail closed: missing, stale, partial, unsigned, or hash-mismatched pacing evidence never becomes READY_FOR_REVIEW."
  - "Immutable checkpoint runs bind the approved capability, approval decision, probe output, actor, UTC times, and canonical hashes."

requirements-completed: [OC-SAFE-01, OC-DATA-02, OC-OPS-01, OC-VAL-01]

duration: 9h 11m elapsed (includes blocking human checkpoint)
completed: 2026-08-03
---

# Phase P0 Plan 01: Readiness and Market-Data Pacing Summary

**Fail-closed readiness contracts and shared probes now bind a human-approved, five-class conservative market-data pacing capability without broker-write authority.**

## Performance

- **Duration:** 9h 11m elapsed, including the blocking human decision checkpoint
- **Started:** 2026-08-03T05:24:17Z
- **Completed:** 2026-08-03T14:35:46Z
- **Tasks:** 3/3
- **Files modified:** 10, including this summary

## Accomplishments

- Added immutable `CapabilityRecord`, `ReadinessReport`, and `MarketDataPacingCapability` contracts with exact source, class, limit, freshness, signer, and hash validation.
- Exposed the same observation-only readiness model through the CLI and `/api/readiness`, with secret-like field rejection and fixed fail-closed reason codes.
- Recorded explicit `human:xujie` approval of all five conservative pacing classes in a unique non-overwriting run directory and verified it as `READY_FOR_REVIEW`.

## Task Commits

Each task was committed atomically:

1. **Task P0-01-01: Define readiness and pacing capability contracts**
   - RED: `9c3f09d` (`test`)
   - GREEN: `f960261` (`feat`)
2. **Task P0-01-02: Implement shared CLI and API readiness probes**
   - RED: `f1f0e19` (`test`)
   - GREEN: `f7fc710` (`feat`)
3. **Task P0-01-03: Approve observed or conservative pacing capability**
   - Evidence: `1349032` (`docs`)

## Files Created/Modified

- `options_copilot/operations/capabilities.py` - Frozen readiness and pacing contracts with canonical validation.
- `options_copilot/operations/readiness.py` - Shared pure probes, CLI, immutable evidence writer, and secret rejection.
- `options_copilot/gateway/ibkr_readonly.py` - Injected read-only pacing observation without inferred limits.
- `options_copilot/api/app.py` - Canonical review-only `/api/readiness` surface.
- `tests/options_copilot/test_readiness.py` - Contract, tamper, source, freshness, and authority tests.
- `tests/options_copilot/test_readiness_cli.py` - CLI/API parity, degraded-state, secret, and read-only gateway tests.
- `data/options_copilot/evidence/checkpoints/P0/market-data-pacing/20260803T142822655851Z-edbcfe34fb60/` - Immutable approved capability, approval manifest, and probe output.

## Signed Evidence

- **Actor/signer:** `human:xujie`
- **Source:** `conservative_default`
- **Version:** `market-data-pacing.v1`
- **Observed at:** `2026-08-03T14:27:50.740422+00:00`
- **Signed at:** `2026-08-03T14:29:03.522812+00:00`
- **Capability hash:** `bd552754ca7571b8056819d8e2ebf5d79ddd0acd570a406d8e3471e60db21f2c`
- **Approval hash:** `fa8d78c43c76d247adc4e44ef9c218f1af26135eb2c77649f4feb03010c7c170`
- **Readiness hash:** `edbcfe34fb608bbfbd48fe0f902433f5137012d968765231fe396bcc23e597da`
- **Probe result:** `READY_FOR_REVIEW`, no reason codes, `review_only=true`, `direct_order_submission=false`

Approved limits:

| Request class | Max concurrency | Window (s) | Max requests | Cooldown (s) |
|---|---:|---:|---:|---:|
| scanner | 1 | 60 | 4 | 15 |
| secdef | 2 | 60 | 30 | 2 |
| snapshot_quote | 10 | 60 | 30 | 2 |
| streaming_quote | 50 | 60 | 30 | 2 |
| historical | 2 | 600 | 30 | 15 |

## Decisions Made

- Applied the user's explicit value-level approval exactly; no values were inferred or silently selected.
- Used the implemented signer-plus-canonical-hash contract. It does not define a separate cryptographic signature or require the human to approve a derived hash after approving the exact values.
- Preserved the earlier missing-capability checkpoint artifact without overwriting or staging it.

## Verification

- Required pacing CLI: exit `0`, status `READY_FOR_REVIEW`, zero reason codes.
- Evidence integrity script: capability, approval, readiness, file SHA-256, exact values, timezone-aware UTC timestamps, and secret scan all passed.
- Focused plan suite: `34 passed` across readiness contract, CLI/API, and frontend API tests.
- Full Options Copilot suite: `229 passed`.
- Bytecode compilation: `python -m compileall -q options_copilot` passed.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration or credentials were used.

## Next Phase Readiness

- P4 has one complete, signed pacing source to validate by version and capability hash.
- Consumers must continue enforcing the 24-hour freshness contract. A stale artifact fails closed and requires a newly observed and explicitly approved capability; it must never be silently refreshed or reused.

## Self-Check: PASSED

- All three task outcomes and atomic commits exist.
- All key files and the immutable evidence run exist.
- The approval, capability, and readiness hashes recompute exactly.
- No `.planning` file, broker connection, live service, port, Tailscale route, credential, or order-capable path was modified.

---
*Phase: P0-capability-baseline*
*Completed: 2026-08-03*
