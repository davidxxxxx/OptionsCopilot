# Options Copilot collaboration contract

- Use multi-agent decomposition for substantial Options Copilot features, audits, and refactors. Small, clear, low-risk edits may remain local.
- Keep one primary agent responsible for architecture, trading-safety contracts, integration, and final verification.
- Give each worker explicit file ownership. Workers share the checkout, must preserve unrelated edits, and must never revert another worker's changes.
- Prefer parallel workstreams for independent data/provider, analysis/ranking, GUI/API, and verification tasks, followed by primary-agent integration review.
- No worker may start, stop, replace, or interfere with ports 8766, 8788, 8878, or 8891, Tailscale routes, broker sessions, instructions, or orders unless the user explicitly authorizes that exact operational action.
- News, earnings, LLM output, Max Pain, walls, PCR, and GEX remain supporting evidence only. Local deterministic code and fresh IBKR read-only data own contract identity, executable quotes, payoff, maximum loss, liquidity, cost-after expected value, and eligibility.
- Preserve review-only behavior: never submit an order and never replace the user's final IBKR confirmation.
