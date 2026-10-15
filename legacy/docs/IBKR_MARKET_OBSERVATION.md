# IBKR market observation

This command performs one bounded, market-data-only research observation. It
uses the existing local IB Gateway listener, qualifies exactly one underlying
and exactly the one or two option rights named by `--rights`, subscribes briefly, cancels every
subscription, and disconnects. It is not a daemon and does not start the main
OptionsCopilot runtime.

```powershell
.\.venv\Scripts\python.exe -m options_copilot.market_observation_cli `
    --symbol QQQ `
    --expiration 20261016 `
    --strike 752 `
    --rights C,P `
    --seconds 8 `
    --client-id 4818
```

The adapter is locked to `127.0.0.1:4001`. The client ID must differ from the
main OptionsCopilot client ID. Collection time is bounded to 1–15 seconds.

Output contains an `options_copilot.ibkr_market_observation.v1` document and an
`options_copilot.ibkr_market_research_input.v1` projection. Bid, ask, last, and
option-computation ticks 10–13 retain their own callback receipt times,
market-data types, validity flags, and update counts. `source_event_time` is
always unavailable because `reqMktData` does not supply an exchange event time.
The callbacks are explicitly asynchronous and are never represented as an
atomic source snapshot.

Delayed price ticks 66–68 and delayed option-computation ticks 80–83 retain
their original tick numbers and per-callback market-data types in bounded
history arrays. A switch between live and delayed data therefore remains
visible instead of overwriting its earlier evidence.

Each contract retains at most 256 price ticks and 512 option-computation ticks,
plus explicit truncation counts; the complete observation is capped at
2,000,000 canonical JSON bytes. Any mixed live/delayed history or truncation
degrades the standalone research input.
The producer checks the final document including its content hash. If rows,
history, or broker-error evidence would exceed the cap, it returns a compact
`UNAVAILABLE` observation with `MARKET_OBSERVATION_SIZE_LIMIT_EXCEEDED` and
discarded record counts; it never truncates evidence and continues as
`OBSERVED`.

Both documents are observation-only and permanently set production,
approval, and instruction eligibility to false. The command does not request
account, position, order, or execution data. Error output is reduced to stable
reason codes and never includes provider exception text.

The research projection verifies the observation schema, content hash, source
status, authority boundary, and timestamps before carrying forward a strict
allowlist of prices and Greeks. Invalid, stale, non-live, partial, unavailable,
or future-dated source evidence cannot be upgraded to research-ready status.
It also verifies that the result contains exactly one matching stock contract
and exactly the requested C and/or P legs, with no extra, missing, or duplicate
contract identity.

The research projection is a standalone handoff. It is not wired into the
weekly brief, ranking pipeline, production eligibility gates, GUI, scheduler,
or any persistent store.
