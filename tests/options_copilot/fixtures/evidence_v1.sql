-- Synthetic v1 migration fixture: one public-news-shaped test record, no account data.
BEGIN TRANSACTION;
CREATE TABLE evidence_records (
    sequence INTEGER PRIMARY KEY, evidence_id TEXT NOT NULL UNIQUE,
    identity TEXT NOT NULL, kind TEXT NOT NULL, symbol TEXT, provider TEXT NOT NULL,
    source_id TEXT NOT NULL, published_at TEXT NOT NULL, first_seen_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL, payload_json TEXT NOT NULL, immutable_json TEXT NOT NULL,
    content_hash TEXT NOT NULL UNIQUE, previous_hash TEXT NOT NULL, row_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL, supersedes_id TEXT
);
INSERT INTO evidence_records VALUES(1,'ev_v1_fixture_0001','sec:AAPL:10-Q:2026-07-31','filing','AAPL','sec','fixture-0001','2026-07-31T12:00:00.000000+00:00','2026-07-31T12:01:00.000000+00:00','2026-07-31T12:02:00.000000+00:00','{"fact":"fixture-revenue","value":1}','{"decision_authority":"SUPPORTING_ONLY","first_seen_at":"2026-07-31T12:01:00.000000+00:00","identity":"sec:AAPL:10-Q:2026-07-31","ingested_at":"2026-07-31T12:02:00.000000+00:00","kind":"filing","observed_at":"2026-07-31T12:02:00.000000+00:00","payload":{"fact":"fixture-revenue","value":1},"provider":"sec","published_at":"2026-07-31T12:00:00.000000+00:00","source_id":"fixture-0001","status":"ACTIVE","supersedes_id":null,"symbol":"AAPL"}','207a712ebb611c3a04807af9e0734ad4a4228184e5a7cc3e03b07601726080fb','0000000000000000000000000000000000000000000000000000000000000000','301ddd3a5cad4779b4b0ffb46f84f788840b4be14578249d5046436a883359de','ACTIVE',NULL);
PRAGMA user_version = 1;
COMMIT;
