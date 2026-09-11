from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

import pytest

from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore, EvidenceStoreCorruption


NOW = datetime(2026, 8, 4, 9, 0, tzinfo=timezone.utc)


def record(
    *,
    identity: str = "sec:AAPL:10-Q:2026-08-01",
    kind: str = "filing",
    payload=None,
    first_seen=None,
):
    seen = first_seen or NOW - timedelta(minutes=2)
    return EvidenceRecord(
        identity=identity,
        kind=kind,
        symbol="AAPL",
        provider="sec",
        source_id="0000001",
        published_at=seen - timedelta(minutes=1),
        first_seen_at=seen,
        ingested_at=seen + timedelta(minutes=1),
        observed_at=seen + timedelta(minutes=2),
        payload=payload or {"fact": "revenue", "value": 1},
    )


def test_append_is_idempotent_and_identity_conflicts_are_visible(tmp_path) -> None:
    with EvidenceStore(tmp_path / "evidence.db") as store:
        first, inserted = store.append(record())
        duplicate, duplicate_inserted = store.append(record())
        conflict, conflict_inserted = store.append(record(payload={"fact": "revenue", "value": 2}))

        assert inserted is True and duplicate_inserted is False and conflict_inserted is True
        assert first.evidence_id == duplicate.evidence_id
        assert conflict.evidence_id != first.evidence_id
        rows = store.query()
        assert len(rows) == 2
        assert {row.status for row in rows} == {"CONFLICTED"}
        assert all(row.decision_authority == "SUPPORTING_ONLY" for row in rows)
        assert store.journal_mode == "wal" and store.synchronous == "full"
        assert store.verify_integrity() is True


def test_point_in_time_never_substitutes_later_first_seen_data(tmp_path) -> None:
    with EvidenceStore(tmp_path / "evidence.db") as store:
        store.append(record(identity="ir:AAPL:one", first_seen=NOW - timedelta(minutes=2)))
        store.append(record(identity="ir:AAPL:two", first_seen=NOW + timedelta(minutes=2)))
        rows = store.query(first_seen_at_or_before=NOW)
        assert [row.identity for row in rows] == ["ir:AAPL:one"]


def test_query_filters_kinds_before_applying_the_bounded_recent_window(tmp_path) -> None:
    path = tmp_path / "kind-filter.db"
    with EvidenceStore(path) as store:
        store.append(record(identity="news:one"))
        store.append(record(identity="calendar:one", kind="CALENDAR"))
        store.append(record(identity="news:two"))

        news = store.query(kinds=("filing",), limit=1)
        calendar = store.query(kinds=("CALENDAR",), limit=1)

        assert [row.identity for row in news] == ["news:two"]
        assert [row.identity for row in calendar] == ["calendar:one"]
        with pytest.raises(TypeError, match="sequence"):
            store.query(kinds="CALENDAR")
        with pytest.raises(ValueError, match="empty"):
            store.query(kinds=())
        with pytest.raises(ValueError, match="duplicates"):
            store.query(kinds=("CALENDAR", "CALENDAR"))


def test_verified_cursor_pages_preserve_complete_chain_order_beyond_five_thousand(
    tmp_path,
) -> None:
    with EvidenceStore(tmp_path / "cursor-pages.db") as store:
        for sequence in range(5003):
            store.append(
                record(
                    identity=f"cursor:item:{sequence}",
                    kind="OUTCOME_CAPTURE_SPEC",
                    payload={"sequence": sequence},
                )
            )

        first = store.query_page(
            after_sequence=0,
            kinds=("OUTCOME_CAPTURE_SPEC",),
            limit=5000,
        )
        second = store.query_page(
            after_sequence=first[-1].sequence,
            kinds=("OUTCOME_CAPTURE_SPEC",),
            limit=5000,
        )
        complete = tuple(
            store.iter_verified(
                kinds=("OUTCOME_CAPTURE_SPEC",),
                page_size=997,
            )
        )

        assert len(first) == 5000
        assert len(second) == 3
        assert [item.sequence for item in complete] == list(range(1, 5004))
        assert complete[0].prior_hash == "0" * 64
        assert all(
            current.prior_hash == prior.row_hash
            for prior, current in zip(complete, complete[1:])
        )


def test_secret_like_payload_and_hash_tampering_fail_closed(tmp_path) -> None:
    path = tmp_path / "evidence.db"
    with EvidenceStore(path) as store:
        with pytest.raises(ValueError, match="secret-like"):
            store.append(record(payload={"authorization": "redacted"}))
        stored, _ = store.append(record())
        store._connection.execute("DROP TRIGGER evidence_records_no_update")
        store._connection.execute(
            "UPDATE evidence_records SET content_hash = ? WHERE evidence_id = ?",
            ("0" * 64, stored.evidence_id),
        )
        with pytest.raises(EvidenceStoreCorruption):
            store.assert_integrity()


def test_directed_v1_migration_and_unknown_versions_fail_closed(tmp_path) -> None:
    path = tmp_path / "v1.db"
    fixture = Path(__file__).parent / "fixtures" / "evidence_v1.sql"
    assert fixture.is_file()
    # Rebuild a synthetic v1 database locally; no binary ledger is distributed.
    with sqlite3.connect(path) as connection:
        connection.executescript(fixture.read_text(encoding="utf-8"))
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM evidence_records").fetchone()[0] == 1
    with EvidenceStore(path) as store:
        assert store.schema_version == 2
        migrated = store.query()
        assert len(migrated) == 1
        assert migrated[0].identity == "sec:AAPL:10-Q:2026-07-31"
        assert migrated[0].record.observed_at == migrated[0].record.ingested_at
        assert migrated[0].decision_authority == "SUPPORTING_ONLY"
        assert store.append(record()).inserted is True
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    future = tmp_path / "future.db"
    connection = sqlite3.connect(future)
    connection.execute("PRAGMA user_version=99")
    connection.close()
    with pytest.raises(RuntimeError, match="newer"):
        EvidenceStore(future)
