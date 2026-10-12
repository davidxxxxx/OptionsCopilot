# Initial account and trade baseline

This file freezes the evidence available when Options Copilot v0.1 began. It is
not a live account statement and must not be used as a current quote source.

## Account snapshot

Managed IBKR connector read at `2026-08-02T22:58:59Z`:

- Net liquidation value: **$2,012.44**
- Available funds: **$1,786.63**
- Day trades remaining: **3**
- Active new-order instructions created by Options Copilot: **0**
- The orders read model contained one terminal `REPLACED` GLD order record; it
  was not treated as a working order.
- Local TWS/IB Gateway API listeners on 4001, 4002, 7496, and 7497: **none**

The `10K Campaign` baseline is therefore $2,012.44. Later deposits and
withdrawals must be removed from strategy NAV before performance is attributed
to the agent.

## Closed legacy trade: WDC

IBKR's 30-day trade read model shows a three-leg option combination opened on
2026-07-27 and closed on 2026-07-29:

- Opening gross debit: **$337.00**
- Closing gross credit: **$220.00**
- Gross loss: **$117.00**
- Total commissions represented by the fills: about **$5.68**
- Connector-reported realized P&L summed across the three closing legs:
  **-$122.68**

The trade feed did not expose enough contract metadata to reconstruct strikes,
expiry, the contemporaneous news/earnings thesis, or the original probability
estimate. It is recorded as an external legacy outcome, not as a valid training
example for causal strategy selection. It may be used for execution-cost and
post-trade discipline analysis only.

## Open legacy trade: GLD

IBKR shows one Aug 21 2026 call combination:

- Long 1 GLD 375 Call
- Short 2 GLD 385 Calls
- Long 1 GLD 390 Call
- Gross entry debit from fills: **$313.00**
- Entry cost including connector-reported commissions: **$315.09**
- Indicative position value in the account snapshot: **$224.73**
- Indicative unrealized P&L: **-$90.36**

The expiration payoff is bounded. Using the actual entry cost and ignoring
future exit commissions:

- Maximum loss: **$315.09**
- Maximum profit at GLD 385: **$684.91**
- Profit above GLD 390 at expiration: **$184.91**
- Entry maximum loss as a share of the current $2,012.44 NLV: **15.66%**

That risk is above the new system's locked normal 10% limit and also above the
15% A-grade ceiling. The position is accepted only as a pre-existing legacy
holding. It occupies the single allowed combination slot, so the agent must not
propose or approve another opening combination while it remains open.

At the time of the read, the US options market was closed. Each contract's
bid/ask response was empty, `last` was explicitly a close, and reported IV was
invalid. Those values cannot support an exit instruction; all legs must be
repriced during an open, liquid session first.
