# Options Copilot v0.1

Options Copilot is the isolated, human-gated US equity-options research surface
for this workspace. Version 0.1 is a safety and evidence foundation: it records
point-in-time inputs, ranks only bounded-risk combinations, shows proposals in a
local GUI, and defines a human-gated handoff to an external Codex bridge. It does
not submit an order and it does not promise profits.

For the current module inventory and known incomplete production paths, see
[the feature catalog](../docs/FEATURES.md) and [current status](../docs/STATUS.md).
The bridge sequence below describes the intended contract, not evidence that
creator transport or live recommendation eligibility is enabled in this source snapshot.

## Locked operating policy

- Naked calls, naked puts, unlimited-loss structures, and any structure whose
  maximum loss cannot be calculated before instruction creation are prohibited.
- Normal maximum loss is 10% of cash-flow-adjusted strategy NAV. A separately
  validated A-grade model may use at most 15% only after a written report and
  explicit human approval. 20% is an absolute rejection line, never a target.
- At most one open options combination is allowed. Current positions must be
  reconciled from the operator's own fresh read-only broker snapshot; no personal
  holdings are distributed with this repository.
- 0-3 DTE is prohibited. The hard minimum is 7 DTE; the normal research band is
  14-35 DTE. Positions are normally managed within five trading days.
- Max Pain, call/put walls, put-call ratio, and estimated GEX are supporting
  signals only. They can never override liquidity, bounded loss, cost-after EV,
  or portfolio risk gates.
- Learning is Champion/Challenger. Thirty independent scenarios unlock
  discovery only. No model or risk-tier promotion reaches production without a
  review report and explicit human approval.

## Approval and IBKR boundary

The intended flow is:

1. A sanitized, time-stamped account/market snapshot is ingested.
2. The GUI shows no more than three candidates and permits approval of rank 1
   only.
3. GUI approval creates a five-minute, hash-bound SQLite handoff.
4. Codex uses the managed IBKR connector to fetch every leg's current bid/ask,
   rejects quotes older than five seconds, recalculates executable combination
   cost and maximum loss, and rejects an adverse move above $5.
5. Codex creates an **IBKR review instruction**, never a live order. The GUI
   displays the returned IBKR deep link.
6. The user reviews and submits (or rejects) the instruction inside IBKR.

The local JSON bridge is run with `python -m options_copilot.bridge` and owns
only durable state; it cannot access or copy Managed Connector authentication.
Its production sequence is `claim` -> `authorize` -> exactly one external
connector call -> `complete` (or a fixed failure code). The `authorize` CLI
sequence durably persists the current account/position/order/instruction gates,
then reserves the one external attempt before returning a dispatch payload.
There is deliberately no second `dispatch` or retry command.

Authorization also requires connector-derived definitions for every option
contract. Contract id, underlying, OPT type, expiration, strike, right,
multiplier 100, USD currency, and standard-contract status must all match the
proposal. Adjusted contracts, unknown multipliers, incomplete definitions, or
metadata mismatches fail to `NO_TRADE` before the external boundary.

The managed IBKR instruction tool does not currently accept an idempotency key.
The approval id is recorded locally, but it cannot make that external API
idempotent. Therefore a timeout, crash, malformed response, or other uncertain
result is `UNKNOWN_OUTCOME`: the program must not call the connector again and
the operator must reconcile the IBKR instruction list manually. Only a
review-only result with strict false submission/transmission flags, an
instruction id, and an allow-listed HTTPS deep link may become
`READY_FOR_IBKR_REVIEW`.

The local application never copies or reverse-engineers the managed connector's
credentials. Local `ib_insync` support is read-only and is a separate optional
path for a user-operated TWS/IB Gateway API session. The default is the live
IB Gateway listener at `127.0.0.1:4001`; the adapter always connects with
`readonly=True`, has no order API, and never stores the IBKR username, password,
or 2FA material. The user starts and signs in to Gateway manually with its
Read-Only API setting enabled.

To check only whether the configured listener and local client dependency are
present, without starting Gateway or performing an IB API login/handshake:

```powershell
python scripts/probe_options_copilot_ib_gateway.py
```

`LISTENER_READY` is intentionally narrow: it does not prove authentication,
subscriptions, live market-data type, secdefs, quotes, Greeks, open interest, or
volume. Those fields must still arrive through the normal atomic read-only flow;
any missing field keeps the candidate fail-closed at `NO_TRADE`.

## Start the GUI

From PowerShell 7 in `G:\OptionsCopilot`:

```powershell
pwsh.exe -File scripts/start_options_copilot.ps1 -Background -OpenBrowser
```

The default local URL is `http://127.0.0.1:8891/`. The launcher refuses to
replace an existing listener. Runtime data and logs stay on G: under:

- `G:\OptionsCopilot\data\options_copilot`
- `G:\OptionsCopilot\logs\options_copilot`

Useful read-only checks:

```powershell
Invoke-RestMethod http://127.0.0.1:8891/health
Invoke-RestMethod http://127.0.0.1:8891/api/bootstrap
Invoke-RestMethod http://127.0.0.1:8891/api/positions
Invoke-RestMethod http://127.0.0.1:8891/api/candidates
Invoke-RestMethod http://127.0.0.1:8891/api/learning
```

## Configure free/low-cost event feeds

All optional provider credentials are read from one local plaintext file:

`G:\OptionsCopilot\data\options_copilot\api_keys.local.json`

The file is explicitly ignored by Git and its values are never returned by the
GUI or API. It is still plaintext on disk, so protect the Windows account and
do not paste the file or any value into chat, logs, screenshots, shell
arguments, source code, or runtime snapshots. The schema is fixed; an unknown,
missing, duplicated, non-string, or whitespace-padded field fails closed:

```json
{
  "jin10_mcp_token": "",
  "finnhub_api_key": "",
  "alpha_vantage_api_key": "",
  "deepseek_api_key": ""
}
```

Edit only the values. An empty string disables that provider. The GUI endpoint
`/api/configuration/providers` exposes only `CONFIGURED`, `DISABLED`, or
`ERROR`; it never exposes credential values or the local file path. IBKR login,
password, and 2FA material never belong in this file. Changes made while the
runtime is open are shown as `restart_required`; restart only the owned Options
Copilot instance before the provider is considered loaded.

If a Jin10 credential has been exposed, revoke it in the provider administration
UI before enabling Jin10, and place its replacement only in `jin10_mcp_token`
in the local JSON file. Never reuse an exposed credential.

Create and verify a zero-material revocation attestation, then bind the exact
local replacement generation to that attestation. Use the immutable path
printed by the first command; there is no mutable `latest.json` alias. The
activation and rotation artifacts contain no token:

```powershell
python -m options_copilot.security.cli attest-revocation --name JIN10_MCP_TOKEN --old-token-revoked --actor human:operator --evidence-dir G:\OptionsCopilot\data\options_copilot\evidence\checkpoints\P3\jin10-rotation
python -m options_copilot.security.cli verify-revocation --path G:\OptionsCopilot\data\options_copilot\evidence\checkpoints\P3\jin10-rotation\revocation-<timestamp>-<hash>.json --json
python -m options_copilot.security.cli activate-local-jin10 --revocation-attestation G:\OptionsCopilot\data\options_copilot\evidence\checkpoints\P3\jin10-rotation\revocation-<timestamp>-<hash>.json
```

The activation command creates a random opaque generation and stores its token
binding only inside the existing Windows DPAPI-encrypted internal sidecar. That
sidecar is not a second user-managed API-key source. Neither the token nor a
reproducible token fingerprint is written to the public activation/rotation
evidence. Restart the owned Options Copilot instance after activation; the GUI
then distinguishes configured, activated, runtime-loaded, and restart-required
state while still displaying credential status only as
`CONFIGURED`/`DISABLED`/`ERROR`.

Finnhub can provide the primary company-news and earnings-calendar feed;
Alpha Vantage is a low-rate cross-check. IBKR remains the source of record for
tradable contracts, positions, and executable option quotes. Free feeds have
coverage, delay, and rate-limit gaps, so missing or conflicting data fails to
`NO_TRADE` rather than being guessed.

The current v0.1 tree also contains a bounded, read-only client for the official
Jin10 Streamable HTTP MCP endpoint. It calls only `list_flash` and `list_news`,
uses short sessions with an overall deadline, forbids redirects, and projects
every Jin10 item as `SUPPORTING_ONLY`. A replacement credential is usable only
when its local-file generation, zero-material activation manifest, and the
latest rotation manifest agree exactly; the runtime Jin10 source health must
report `activation_status: ACTIVATED`. Jin10 source failure never grants a
fallback authority and is shown independently in the news GUI.

Jin10 entity-to-symbol links are deterministic and restricted to the configured
core universe: explicit provider metadata, cashtags/exchange tickers, then a
versioned company-alias catalog. Ambiguous or unrecognised items remain
market-level news; the LLM cannot invent a ticker. Point-in-time analysis output
is frozen beside the evidence ledger so an unchanged evidence/classifier/IBKR
input fingerprint keeps its original result and completion time across refresh
and restart.

Credential and activation status are properties of each local installation.
The source repository contains neither a credential nor an activation grant;
inspect the local runtime's provider status instead of inferring it from this document.

## Ingest a managed-connector snapshot

The ingestion boundary accepts a sanitized JSON object and rejects fields whose
names look like tokens, passwords, secrets, or API keys:

```powershell
python -m options_copilot.ingest --input G:\path\to\sanitized_snapshot.json
```

Every snapshot needs a timezone-aware `observed_at`, positive
`account.net_liquidation`, position and candidate arrays, and at most three
candidates. Candidate approval additionally requires a current executable quote
for every leg and all locked risk gates to pass.

## Verification

For this source-only snapshot, use the portable profile and exclusions in
[the testing guide](../docs/TESTING.md). The unrestricted suite also contains
tests of private signed contracts and local migration evidence that are not
distributed; it is only applicable to the original authorized installation.

```powershell
python -m compileall -q options_copilot
node --check options_copilot/frontend/app.js
```

## Current v0.1 limitations

- This is not yet an unattended full-market production scanner. IBKR market-data
  entitlements and pacing limits require a staged universe/liquidity prefilter;
  attempting to subscribe to every listed option simultaneously is neither
  reliable nor permitted by typical retail data limits.
- The first live market session begins evidence collection immediately, but
  learning cannot silently change production behavior.
- Closed-market marks are displayed as stale/indicative and cannot create an
  instruction.
- Existing positions can be observed, but v0.1 will not invent a
  close instruction from stale marks. A close-specific, bounded and freshly
  quoted proposal must pass the same human approval and connector gates first.
- A verified creator transport and an active external review session are
  prerequisites for any future review-instruction handoff. They are not granted
  by cloning this repository. The GUI alone has no broker-write primitive.
