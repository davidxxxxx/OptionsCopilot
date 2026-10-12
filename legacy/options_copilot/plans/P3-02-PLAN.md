---
phase: P3-evidence-providers
plan: "02"
type: execute
wave: 2
depends_on: ["P3-01"]
files_modified:
  - options_copilot/providers/__init__.py
  - options_copilot/providers/official.py
  - options_copilot/providers/official_sources.py
  - options_copilot/providers/jin10.py
  - options_copilot/providers/events.py
  - options_copilot/providers/cli.py
  - options_copilot/security/cli.py
  - options_copilot/security/revocation.py
  - scripts/set_options_copilot_secret.ps1
  - tests/options_copilot/test_event_providers.py
  - tests/options_copilot/test_official_calendar_provider.py
  - tests/options_copilot/test_official_sources.py
  - tests/options_copilot/test_config_security.py
  - data/options_copilot/evidence/checkpoints/P3/jin10-rotation/
  - data/options_copilot/evidence/checkpoints/P3/provider-live/
evidence_prefixes:
  - data/options_copilot/evidence/checkpoints/P3/jin10-rotation/
  - data/options_copilot/evidence/checkpoints/P3/provider-live/
autonomous: false
requirements: [OC-EVENT-01, OC-SEC-01, OC-TIME-01, OC-ANALYTICS-01, OC-VAL-01]
must_haves:
  truths:
    - "Official filings, company IR, and declared FOMC/BEA/BLS calendar sources anchor facts; supplemental providers retain provenance and conflicts."
    - "The replacement Jin10 secret cannot be set until a signed zero-token-material revocation attestation exists."
    - "A configured Jin10 secret never invents a production transport; Jin10 remains disabled until revocation, replacement, and a separately verified MCP/HTTP contract are complete."
    - "Every provider event is SUPPORTING_ONLY and cannot create candidate eligibility without independent hard evidence."
  artifacts:
    - path: "options_copilot/security/revocation.py"
      provides: "Signed old-token-revoked attestation validation"
      contains: "RevocationAttestation"
    - path: "options_copilot/providers/official_sources.py"
      provides: "Credential-free allowlisted HTTPS transports and strict FOMC/BEA/BLS calendar parsers"
      contains: "build_official_calendar_provider"
    - path: "data/options_copilot/evidence/checkpoints/P3/jin10-rotation/"
      provides: "Immutable revocation and rotation evidence without token material"
    - path: "data/options_copilot/evidence/checkpoints/P3/provider-live/"
      provides: "Live provider contract, coverage, conflict, and redaction evidence"
  key_links:
    - from: "scripts/set_options_copilot_secret.ps1"
      to: "options_copilot/security/revocation.py"
      via: "mandatory signed attestation hash before replacement write"
      pattern: "RevocationAttestation"
---

<objective>
Implement official event/calendar anchors, supplemental providers, and a revocation-attested secret rotation boundary.

Purpose: Gain point-in-time event coverage without leaking credentials or granting news trading authority.
Output: Official source/calendar providers, supplemental providers, CLI, attestation enforcement, tests, rotation evidence, and live provider evidence.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/storage/evidence.py
@options_copilot/providers/events.py
@options_copilot/security/dpapi.py
@scripts/set_options_copilot_secret.ps1
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P3-02-01: Implement official and supplemental provider contracts</name>
  <read_first>
    - options_copilot/storage/evidence.py
    - options_copilot/providers/events.py
    - options_copilot/security/dpapi.py
    - options_copilot/IMPLEMENTATION_RESEARCH.md
  </read_first>
  <files>options_copilot/providers/__init__.py, options_copilot/providers/official.py, options_copilot/providers/official_sources.py, options_copilot/providers/jin10.py, options_copilot/providers/events.py, options_copilot/providers/cli.py, tests/options_copilot/test_event_providers.py, tests/options_copilot/test_official_calendar_provider.py, tests/options_copilot/test_official_sources.py</files>
  <action>Add SEC/company-IR fact anchors and a Jin10 adapter using injectable transports, fixed timeouts, bounded read-only retries, provenance, source IDs, four timestamps, and content hashes. Implement credential-free allowlisted GET-only HTTPS sources for the declared Federal Reserve FOMC, BEA, and BLS calendars. Parse only explicit official dates/times/timezones; preserve exact-versus-date-only precision, source URL/hash, first-seen/observed times, and structurally distinct fallback provenance. Changed or partial markup, duplicate JSON keys, floating/ambiguous time, redirect, oversize response, 403/429, timeout, bad JSON, missing fields, and licensing gaps become fixed DEGRADED/NO_TRADE health without invented rows. Cross-source events merge only when identity and content agree; conflicts remain CONFLICTED and block eligibility. All news, earnings, and event/calendar records contain `decision_authority=SUPPORTING_ONLY`. Add provider probe CLI that writes contract, coverage/freshness, conflict, and redaction artifacts without authorization headers or secret material. Merely possessing a Jin10 token must not compose an unverified production endpoint.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_event_providers.py tests/options_copilot/test_official_calendar_provider.py tests/options_copilot/test_official_sources.py</automated>
  </verify>
  <done>Providers are point-in-time, conflict-preserving, supporting-only, and fail closed under network/data faults.</done>
  <acceptance_criteria>
    - Official facts are never overwritten by supplemental content
    - FOMC/BEA/BLS fixtures preserve explicit precision/provenance and fail closed on structural drift or transport faults
    - Conflict, 429, timeout, bad JSON, and missing-field fixtures have fixed outcomes
    - Every emitted event is SUPPORTING_ONLY
    - Captured requests, logs, stdout/stderr, and evidence contain no secret material
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P3-02-02: Enforce signed revocation attestation before replacement secret</name>
  <read_first>
    - options_copilot/security/dpapi.py
    - options_copilot/security/cli.py
    - scripts/set_options_copilot_secret.ps1
    - options_copilot/README.md
  </read_first>
  <files>options_copilot/security/cli.py, options_copilot/security/revocation.py, scripts/set_options_copilot_secret.ps1, tests/options_copilot/test_config_security.py</files>
  <action>Implement an immutable `RevocationAttestation` containing secret name, `old_token_revoked=true`, human actor, signed_at, version, signer, and canonical hash, with zero token value, fragment, fingerprint, authorization header, or ciphertext. Strict JSON parsing rejects duplicate keys, non-finite constants, extra/missing fields, non-UTC/future/expired timestamps, non-human actors, wrong names, false flags, hash drift, and replay. Add `security.cli attest-revocation`, verification, zero-material status, and exclusive one-time rotation reservation. The PowerShell setter must require `-RevocationAttestation PATH` for Jin10, verify and consume it before the hidden prompt, and refuse ordinary Jin10 `set`; other allowlisted secrets retain hidden double-entry. Store only DPAPI ciphertext. Rotation manifest and secret-status output include the attestation hash and UTC metadata, not token material. The previously exposed credential is never read, migrated, logged, or repeated. This local human attestation is not represented as a cryptographic receipt from Jin10 and does not itself enable a production Jin10 transport.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_config_security.py</automated>
  </verify>
  <done>Replacement storage is impossible until a valid signed old-token-revoked attestation exists.</done>
  <acceptance_criteria>
    - Setter rejects missing, false, wrong-name, wrong-hash, unsigned, and replayed attestation fixtures
    - Ordinary Jin10 set is rejected before any prompt, while non-Jin10 secrets retain hidden double-entry
    - Valid attestation hash appears in the rotation manifest
    - Source, logs, evidence, command line, and tests contain zero token material
    - Replacement is entered only through a hidden prompt and stored only with DPAPI
  </acceptance_criteria>
</task>

<task type="checkpoint:human-action" gate="blocking">
  <name>Task P3-02-03: Attest revocation and set replacement secret</name>
  <read_first>
    - options_copilot/security/revocation.py
    - options_copilot/security/cli.py
    - scripts/set_options_copilot_secret.ps1
    - options_copilot/README.md
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P3/jin10-rotation/</files>
  <action>Post-implementation only: after the human has revoked the prior credential in the provider administration UI, run `python -m options_copilot.security.cli attest-revocation --name JIN10_MCP_TOKEN --old-token-revoked --actor HUMAN --evidence-dir data/options_copilot/evidence/checkpoints/P3/jin10-rotation`. Verify the signed artifact, then run `pwsh.exe -NoProfile -File scripts/set_options_copilot_secret.ps1 -Name JIN10_MCP_TOKEN -RevocationAttestation data/options_copilot/evidence/checkpoints/P3/jin10-rotation/latest.json` and enter the replacement only at the hidden prompt. Finally run `python -m options_copilot.security.cli secret-status --name JIN10_MCP_TOKEN --json --evidence-dir data/options_copilot/evidence/checkpoints/P3/jin10-rotation`. If revocation is not complete, do not run the setter and leave Jin10 disabled.</action>
  <verify>
    <automated>python -m options_copilot.security.cli verify-revocation --path data/options_copilot/evidence/checkpoints/P3/jin10-rotation/latest.json --json</automated>
  </verify>
  <done>Signed zero-material revocation evidence precedes the DPAPI replacement, or Jin10 remains disabled.</done>
  <acceptance_criteria>
    - Attestation has old_token_revoked=true, actor, signed_at, version, and hash
    - Rotation manifest binds the attestation hash
    - Secret status records configured/backend/time only
    - Redaction scan reports zero token-like material
  </acceptance_criteria>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task P3-02-04: Verify live provider contracts and redaction</name>
  <read_first>
    - options_copilot/providers/cli.py
    - options_copilot/providers/official.py
    - options_copilot/providers/jin10.py
    - options_copilot/storage/evidence.py
  </read_first>
  <files>data/options_copilot/evidence/checkpoints/P3/provider-live/</files>
  <action>Post-implementation only: run `python -m options_copilot.providers.cli probe --providers official,finnhub,alpha_vantage,jin10 --json --evidence-dir data/options_copilot/evidence/checkpoints/P3/provider-live`. Human-check live license/coverage, four timestamps, official precedence, conflicts, rate-limit/timeout mappings, and redaction. Provider failures must become DEGRADED/NO_TRADE and free sources must never be execution-quote authority.</action>
  <verify>
    <automated>python -m options_copilot.providers.cli probe --providers official,finnhub,alpha_vantage,jin10 --json --evidence-dir data/options_copilot/evidence/checkpoints/P3/provider-live</automated>
  </verify>
  <done>Named live provider evidence is signed and redacted, or affected providers remain disabled.</done>
  <acceptance_criteria>
    - Manifest links provider contracts, coverage/freshness, conflicts, and redaction hashes
    - Redaction report has zero findings
    - Human actor signs observed network/license results
    - Every event remains SUPPORTING_ONLY
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Provider networks to evidence | Untrusted remote content and timing enter local storage. |
| Human revocation to secret setter | A signed zero-material attestation gates the only replacement path. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P3-02-01 | Information Disclosure | provider secret | mitigate | DPAPI, hidden prompt, attestation-only manifest, and redaction tests. |
| T-P3-02-02 | Tampering | event conflict | mitigate | Official anchors and append-only conflicted versions. |
| T-P3-02-03 | Elevation of Privilege | event authority | mitigate | SUPPORTING_ONLY invariant and downstream independent-evidence gates. |
</threat_model>

<verification>
Run provider/security tests, then perform revocation/rotation and provider-live checkpoints in that order.
</verification>

<success_criteria>
Provider evidence is trustworthy and supporting-only, and secret rotation has a signed revocation prerequisite with zero secret material.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
