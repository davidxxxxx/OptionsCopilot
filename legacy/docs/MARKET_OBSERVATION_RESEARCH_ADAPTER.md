# Market Observation Research Adapter

`MarketObservationResearchAdapter` is the only supported bridge from a bounded
IBKR market observation into the HTTP research surface. It performs no broker
request and owns no background process.

## Entry point

`POST /api/research/market-observation` accepts one complete
`options_copilot.ibkr_market_observation.v1` document. The normal standalone
runtime wires this endpoint to the adapter. Tests may inject the handler through
`OptionsCopilotServices.market_observation_research_handler`.

The route has no independent authentication mechanism. It relies on the existing
launcher contract that binds the application only to `127.0.0.1`; it must not be
exposed on a public or non-loopback interface.

Before JSON parsing, the route enforces a 2,065,536-byte request limit (the
2,000,000-byte canonical observation maximum plus 65,536 bytes of bounded JSON
transport overhead). It rejects an oversized declared `Content-Length` and also
counts streamed chunks when no length is declared. Either case returns HTTP 413
without calling the adapter.

The route accepts only `application/json`; an optional UTF-8 charset parameter is
allowed. Missing types, form types, `text/plain`, `+json` variants, and non-UTF-8
charsets return HTTP 415 before the body is read. JSON nesting is scanned before
parsing and capped at 64 object/array levels. Excessive integer conversion,
malformed JSON, parser recursion, and depth violations converge to HTTP 422
without calling the adapter.

The bounded body is decoded strictly as UTF-8 before depth scanning or JSON
parsing. UTF-16/UTF-32 bodies, with or without a byte-order mark, and malformed
UTF-8 return HTTP 422 without calling the adapter. Valid UTF-8 non-ASCII text and
JSON Unicode escapes remain supported.

The endpoint returns
`options_copilot.market_observation_research_read_model.v1`. It contains only
allowlisted contract identity, price, option-computation, receipt-time, data-type,
history-bound, and reason-code fields. Account, position, order, instruction,
credential, and provider-private fields are not projected.
`update_count` is intentionally absent because the upstream research projection
does not carry a verified source count; the adapter never invents one.

## Authority

Every successful response is fixed to:

- `decision_authority: OBSERVATION_ONLY`
- `research_only: true`
- `model_input_complete: false`
- `production_eligible: false`
- `entry_authority: false`
- `approval_eligible: false`
- `instruction_creation_allowed: false`
- `order_allowed: false`
- `requires_fresh_executable_evidence: true`

`FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED` is always present. The adapter is
not a `BrokerSnapshotSource`, does not construct `AtomicBrokerSnapshot`, and is
not wired to strategy generation, eligibility gates, approvals, or ranking.

## Freshness and time meaning

Freshness is recalculated at request time with the existing hard maximum of 15
seconds. An old otherwise-valid observation becomes `RESEARCH_DEGRADED`; the
adapter never rewrites timestamps or relaxes the gate. Callback receipt time is
retained as receipt time. Because native TWS option-computation callbacks do not
provide a source event timestamp, `source_event_time` remains unavailable and
the evidence remains non-atomic.

## Fail-closed behavior

The adapter rejects hash tampering, invalid authority flags, future or malformed
times, contract topology mismatch, oversized history, and output permission
injection. Delayed or mixed data, truncated history, missing fields, and stale
fields remain visible as degradation reasons. The API validates the exact
read-model field set and hash again before returning it.

Synthetic fixtures test these contracts only; they cannot produce a production
candidate or recommendation. A real offline drill requires the original saved
observation document, not values reconstructed from a transcript.
