# Options Copilot P6 UI contract

Status: locked for P6 implementation (2026-08-04)

## Authority boundary

- The browser is a read model and human-confirmation surface. It never decides rank, reconstructs a proposal, supplies policy/cost/risk hashes, claims a bridge request, or submits an order.
- The only entry action is an identifier-only challenge for the current immutable Rank 1. Rank 2 and Rank 3 render `VIEW_ONLY` and receive no action element or event handler.
- The challenge request body is `{}`. The second request carries only the server-issued challenge response and the three explicit human confirmation signals.
- An IBKR deep link is absent until the server reports `READY_FOR_IBKR_REVIEW`. A displayed link must be absolute HTTPS and contain no query string or fragment.
- Account NLV is labelled `observable/reconciliation only`. Strategy NAV is the sole displayed risk denominator.

## Five required regions

1. **Readiness** — `READY` or `DEGRADED / NO_TRADE`, reasons, scan ID, ranking ID, policy identity, cost identity, Strategy NAV, and observation-only account NLV.
2. **Position Management** — existing exposure and management-only state. It never reuses the entry-ranking authority path. Until PositionManager exists it displays `NO_TRADE`/unavailable.
3. **Top 3** — current immutable ranks, candidate/proposal IDs, SecDef state, per-leg executable bid/ask and quote times, quote age/skew, maximum loss, risk fraction, Strategy NAV, cost-after EV, and authority identities.
4. **Evidence & Exit Plan** — primary evidence, supporting-only news/earnings/positioning evidence, thesis invalidation, profit target, stop, time exit, and event exit. Missing exit data is visible and keeps the candidate non-actionable.
5. **Final Review** — explains the local double confirmation, safe bridge revalidation, and the separate user confirmation inside IBKR. It never implies that an order has been submitted.

## News and calendar

- News is a separate read-only view. It shows transparent event-impact, option-tradability, and combined-opportunity scores.
- The pre-market research pool is capped at 10. The action-observation pool is capped at 3 and remains zero unless a fresh, symbol-bound IBKR option snapshot exists.
- The news view also renders up to 10 contract-bound pre-market option preselections and up to 10 open-market repriced rows. Each row exposes every leg's conId, expiry, strike, right, side, ratio, bid/ask, quote batch/time, IV, Greeks, volume, OI and DTE together with finite maximum loss, estimated cost, cost-after EV, blockers and all four entry/exit conditions.
- Contract-level rows are permanently `SUPPORTING_ONLY`; even a complete Top-3 action-observation row has no approval handler and cannot create an IBKR instruction from the news page.
- Jin10 and other news feeds are discovery/supporting evidence only. Provider timestamps, first-seen time, analysis time, latency, category, source, and pool/rank labels remain visible.
- Every analyzed row names its classifier. `STRUCTURED_LLM` is permitted only through the evidence-bound JSON schema; `DETERMINISTIC_RULES` is the visible fallback when the optional model is disabled, unavailable, over budget, or invalid. Neither classifier may create hard option data or trading authority.
- The calendar exposes this week, next week, and two-week windows for earnings, FOMC and other material events.
- Official calendar rows retain source URL/hash, exact-versus-date-only precision, timezone, first-seen/observed times, and window membership. Any unavailable or structurally changed required source makes the calendar envelope `DEGRADED / NO_TRADE` while valid rows remain visible as `SUPPORTING_ONLY`.

## Responsive acceptance

- Desktop and mobile layouts have no page-level horizontal overflow. The option-leg table may scroll inside its own container.
- Degraded or stale state removes the Rank 1 challenge control and hides the IBKR link.
- Browser evidence must be captured against the same-run API payload hash before P6 is considered visually accepted.
