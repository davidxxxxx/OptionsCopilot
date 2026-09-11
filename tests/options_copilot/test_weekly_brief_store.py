from __future__ import annotations

from datetime import date, datetime, timedelta
import sqlite3
import threading

import pytest

from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.news.weekly_brief import (
    SourceHealthStatus,
    WeeklyBrief,
    WeeklyBriefSourceHealth,
    evaluate_weekly_brief_slot,
)
from options_copilot.news.weekly_brief_store import (
    WeeklyBriefStore,
    WeeklyBriefStoreConflict,
    WeeklyBriefStoreCorruption,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


CUTOFF = datetime(2026, 9, 8, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)


def _brief(*, calendar_label: str = "calendar") -> WeeklyBrief:
    calendar_hash = canonical_hash({"label": calendar_label})
    slot = evaluate_weekly_brief_slot(
        scheduled_for=CUTOFF,
        evaluated_at=CUTOFF + timedelta(seconds=10),
        official_session_dates=(date(2026, 9, 8), date(2026, 9, 9)),
        calendar_hash=calendar_hash,
    )
    health = WeeklyBriefSourceHealth.build(
        source="OFFICIAL_EVENTS",
        status=SourceHealthStatus.READY,
        mandatory=True,
        observed_at=CUTOFF - timedelta(seconds=1),
        source_hash=canonical_hash({"source": "official-events"}),
    )
    return WeeklyBrief.build(
        slot=slot,
        evidence_items=(),
        source_health=(health,),
        watch_items=(),
    )


def test_store_is_wal_full_append_only_and_restart_idempotent(tmp_path) -> None:
    path = tmp_path / "weekly.sqlite3"
    now = datetime(2026, 9, 8, 12, 30, tzinfo=US_OPTIONS_TIMEZONE)
    brief = _brief()
    with WeeklyBriefStore(path, clock=lambda: now) as store:
        first = store.append(brief)
        retry = store.append(brief)
        assert first.inserted is True
        assert retry.inserted is False
        assert first.record.row_hash == retry.record.row_hash
        assert store.count == 1
        assert store.journal_mode == "wal"
        assert store.synchronous == "full"
        store.assert_integrity()

    with WeeklyBriefStore(path, clock=lambda: now) as reopened:
        latest = reopened.latest()
        assert latest is not None
        assert latest.content_hash == brief.content_hash
        assert canonical_json(latest.payload) == canonical_json(brief.append_payload())
        assert reopened.count == 1

    connection = sqlite3.connect(path)
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute("UPDATE weekly_briefs SET content_hash='x' WHERE sequence=1")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute("DELETE FROM weekly_briefs WHERE sequence=1")
    connection.close()


def test_same_idempotency_key_with_changed_content_fails_closed(tmp_path) -> None:
    path = tmp_path / "weekly.sqlite3"
    original = _brief()
    with WeeklyBriefStore(path) as store:
        store.append(original)
        store._connection.execute("DROP TRIGGER weekly_briefs_no_update")
        store._connection.execute(
            "UPDATE weekly_briefs SET content_hash=? WHERE sequence=1",
            (canonical_hash({"forged": True}),),
        )
        with pytest.raises(WeeklyBriefStoreConflict):
            store.append(original)


def test_two_connections_preserve_first_writer_for_same_weekly_brief(tmp_path) -> None:
    path = tmp_path / "weekly-race.sqlite3"
    brief = _brief()
    first = WeeklyBriefStore(path)
    second = WeeklyBriefStore(path)
    barrier = threading.Barrier(2)
    results = []
    failures: list[BaseException] = []
    lock = threading.Lock()

    def append(store: WeeklyBriefStore) -> None:
        try:
            barrier.wait(timeout=5)
            result = store.append(brief)
            with lock:
                results.append(result)
        except Exception as exc:
            with lock:
                failures.append(exc)

    threads = [
        threading.Thread(target=append, args=(store,))
        for store in (first, second)
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        assert failures == []
        assert len(results) == 2
        assert sorted(result.inserted for result in results) == [False, True]
        assert len({result.record.row_hash for result in results}) == 1
        assert first.count == 1
        first.assert_integrity()
        second.assert_integrity()
    finally:
        first.close()
        second.close()


def test_reopen_rejects_tampered_hash_chain(tmp_path) -> None:
    path = tmp_path / "weekly.sqlite3"
    with WeeklyBriefStore(path) as store:
        store.append(_brief())
        store._connection.execute("DROP TRIGGER weekly_briefs_no_update")
        store._connection.execute(
            "UPDATE weekly_briefs SET row_hash=? WHERE sequence=1",
            (canonical_hash({"tampered": True}),),
        )

    with pytest.raises(WeeklyBriefStoreCorruption, match="row hash"):
        WeeklyBriefStore(path)
