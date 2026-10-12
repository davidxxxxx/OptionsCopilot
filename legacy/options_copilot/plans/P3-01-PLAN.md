---
phase: P3-evidence-providers
plan: "01"
type: execute
wave: 1
depends_on: []
files_modified:
  - options_copilot/storage/evidence.py
  - tests/options_copilot/test_evidence_store.py
  - tests/options_copilot/fixtures/evidence_v1.db
evidence_prefixes: []
autonomous: true
requirements: [OC-EVENT-01, OC-TIME-01, OC-DATA-02, OC-VAL-01]
must_haves:
  truths:
    - "Evidence is append-only, point-in-time queryable, hash-chained, and never silently overwritten."
    - "Published, first-seen, ingested, and observed times are distinct UTC values."
    - "Identity conflicts remain visible and cannot make a candidate eligible."
  artifacts:
    - path: "options_copilot/storage/evidence.py"
      provides: "SQLite/WAL point-in-time evidence ledger"
      contains: "EvidenceStore"
    - path: "tests/options_copilot/fixtures/evidence_v1.db"
      provides: "Directed migration fixture"
  key_links:
    - from: "options_copilot/storage/evidence.py"
      to: "options_copilot/storage/canonical.py"
      via: "canonical payload and hash-chain identity"
      pattern: "content_hash"
---

<objective>
Build the immutable point-in-time evidence ledger used by providers, scans, ranking, and replay.

Purpose: Preserve what was knowable at decision time and make conflicts or late data auditable.
Output: Evidence store, directed migration fixture, and persistence/concurrency tests.
</objective>

<execution_context>
@C:/Users/xujie/.codex/get-shit-done/workflows/execute-plan.md
@C:/Users/xujie/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@options_copilot/storage/ledger.py
@options_copilot/storage/canonical.py
@options_copilot/approval/store.py
@options_copilot/IMPLEMENTATION_PATTERNS.md
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task P3-01-01: Implement append-only EvidenceStore</name>
  <read_first>
    - options_copilot/storage/ledger.py
    - options_copilot/storage/canonical.py
    - options_copilot/approval/store.py
    - options_copilot/providers/events.py
  </read_first>
  <files>options_copilot/storage/evidence.py, tests/options_copilot/test_evidence_store.py</files>
  <action>Create a dedicated SQLite store with WAL, synchronous FULL, foreign keys ON, directed `user_version`, and explicit transactions. Store evidence identity, kind, symbol, provider/source ID, published_at, first_seen_at, ingested_at, observed_at, canonical payload, content hash, status, prior hash, row hash, and supersedes ID. Exact duplicate identity/hash is idempotent; same identity/different hash creates a visible conflict version. Point-in-time queries require first_seen_at at or before cutoff and never substitute later data. Secret-like fields are rejected.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_evidence_store.py</automated>
  </verify>
  <done>Evidence is immutable, point-in-time safe, conflict-preserving, and migration-aware.</done>
  <acceptance_criteria>
    - SQLite PRAGMAs are read back and asserted
    - Duplicate content is idempotent; identity conflict remains a separate version
    - Future first_seen data is absent from earlier queries
    - Hash-chain and secret-field tampering fail tests
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task P3-01-02: Add directed migration and concurrency coverage</name>
  <read_first>
    - options_copilot/storage/evidence.py
    - options_copilot/approval/store.py
    - tests/options_copilot/test_evidence_store.py
  </read_first>
  <files>tests/options_copilot/test_evidence_store.py, tests/options_copilot/fixtures/evidence_v1.db</files>
  <action>Add a real v1 fixture and explicit migration tests. Unknown old/new versions fail closed; migration never masks a malformed schema. Concurrent writers preserve one identity head and append conflicts deterministically. Restart and `PRAGMA integrity_check` preserve the chain. Late corrections supersede rather than overwrite and keep the original decision-time view.</action>
  <verify>
    <automated>python -m pytest -q tests/options_copilot/test_evidence_store.py</automated>
  </verify>
  <done>Persistence, migration, restart, and multi-process invariants are covered by deterministic tests.</done>
  <acceptance_criteria>
    - v1 migration ends at expected user_version and integrity_check is ok
    - Unknown versions do not auto-upgrade
    - Concurrent conflicts do not overwrite a row
    - Original cutoff queries remain stable after late data
  </acceptance_criteria>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|---|---|
| Provider payload to evidence DB | External content and timestamps are untrusted. |
| Evidence DB to replay/ranking | Stored records become decision inputs only through point-in-time queries. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|---|---|---|---|---|
| T-P3-01-01 | Tampering | evidence rows | mitigate | Canonical hashes, chain linkage, and integrity checks. |
| T-P3-01-02 | Repudiation | late correction | mitigate | Append-only supersession and four timestamps. |
| T-P3-01-03 | Information Disclosure | payload | mitigate | Secret-like key/value rejection. |
</threat_model>

<verification>
Run the dedicated persistence, migration, restart, and concurrency suite.
</verification>

<success_criteria>
Every downstream decision can reconstruct exactly which immutable evidence existed at its cutoff.
</success_criteria>

<output>
After completion, the executor records its standard summary outside the implementation allowlist; this element authorizes no additional production path.
</output>
