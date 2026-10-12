from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from options_copilot.news.reaction import MarketReactionEvidence
from options_copilot.news.reaction_runtime import (
    Jin10MacroObservation,
    MacroReactionError,
    ProductionMacroReactionProvider,
    ReactionEvidenceStore,
)
from options_copilot.providers.official_reaction_sources import (
    CapturedOfficialRelease,
    DiscoveryRecord,
    OfficialDocument,
    ParsedMeasure,
    ParsedOfficialRelease,
)
from options_copilot.news.reaction_specs import EventFamily, EventRole


NOW = datetime(2026, 9, 9, 2, 0, tzinfo=timezone.utc)


def _append(store: ReactionEvidenceStore, event_id: str, event_hash: str, marker: int) -> None:
    store.append(
        event_id=event_id,
        official_event_hash=event_hash,
        kind="MARKET_REACTION",
        observed_at=NOW + timedelta(seconds=marker),
        document={"marker": marker, "decision_authority": "SUPPORTING_ONLY"},
    )


def _official_event(index: int) -> object:
    return SimpleNamespace(
        event_id=f"cpi-{index}",
        source="Bureau of Labor Statistics",
        source_id=f"cpi-{index}",
        source_url="https://www.bls.gov/news.release/cpi.nr0.htm",
        title=f"Consumer Price Index August 2026 {index}",
        category="MACRO",
        scheduled_at=NOW + timedelta(minutes=index),
        published_at=NOW - timedelta(days=30),
        first_seen_at=NOW - timedelta(days=20),
        observed_at=NOW - timedelta(days=1),
        symbols=(),
    )


def test_projection_and_coverage_validate_once_independent_of_event_count(
    tmp_path: Path,
) -> None:
    store = ReactionEvidenceStore(tmp_path / "batch.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: NOW,
    )
    try:
        events = tuple(_official_event(index) for index in range(1, 8))
        provider.restore_official_identities(events)
        original = store.assert_integrity
        calls = 0

        def counted() -> None:
            nonlocal calls
            calls += 1
            original()

        store.assert_integrity = counted  # type: ignore[method-assign]
        projection = provider.projection()
        assert calls == 1
        assert projection["supported_event_ids"] == [event.event_id for event in events]

        calls = 0
        coverage = provider.coverage(tuple(event.event_id for event in events))
        assert calls == 1
        assert tuple(coverage) == tuple(event.event_id for event in events)

        calls = 0
        assert provider.child_reactions(tuple(event.event_id for event in events)) == {}
        assert calls == 1

        calls = 0
        assert provider.revision_views(tuple(event.event_id for event in events)) == {}
        assert calls == 1
    finally:
        provider.close()


def test_child_and_revision_batches_select_populated_evidence_once(
    tmp_path: Path,
) -> None:
    store = ReactionEvidenceStore(tmp_path / "populated-tree.sqlite3")
    event = _official_event(1)
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: event.scheduled_at + timedelta(minutes=10),
    )

    def capture(
        value: str,
        captured_at: datetime,
        *,
        revision_of: str | None = None,
    ) -> None:
        spec = provider._state().specs[event.event_id]
        document = OfficialDocument(
            EventFamily.CPI,
            EventRole.OFFICIAL_RELEASE_DOCUMENT,
            event.source_url,
            captured_at,
            "text/html",
            f"official CPI {value} {captured_at.isoformat()}".encode(),
            declared_release_at=event.scheduled_at,
            first_observed_release_at=captured_at,
        )
        parsed = ParsedOfficialRelease(
            EventFamily.CPI,
            "2026-08",
            None,
            (
                ParsedMeasure(
                    "headline_cpi_yoy_pct",
                    Decimal(value),
                    "PERCENT",
                    "NOT_SEASONALLY_ADJUSTED",
                    "Headline CPI YoY",
                ),
            ),
            (),
            document.raw_hash,
            declared_release_at=event.scheduled_at,
            first_observed_release_at=captured_at,
            revision_of=revision_of,
        )
        store.append_verified_capture(
            event_id=event.event_id,
            captured=CapturedOfficialRelease(
                spec,
                DiscoveryRecord(
                    EventFamily.CPI,
                    "https://www.bls.gov/schedule/news_release/cpi.htm",
                    captured_at,
                    None,
                    document.url,
                ),
                document,
                parsed,
            ),
        )

    try:
        provider.restore_official_identities((event,))
        identity = provider._state().identities[event.event_id]
        observation = Jin10MacroObservation(
            title="US August CPI YoY",
            scheduled_at=event.scheduled_at,
            metric="headline_cpi_yoy_pct",
            unit="PERCENT",
            period="2026-08",
            basis="NOT_SEASONALLY_ADJUSTED",
            series_id="CUUR0000SA0",
            calculation="YOY",
            consensus=Decimal("3.0"),
            reported_actual=None,
            previous=Decimal("2.9"),
            source_id="jin10-cpi-populated",
            observed_at=event.scheduled_at - timedelta(minutes=20),
        )
        store.append(
            event_id=event.event_id,
            official_event_hash=identity.event_hash,
            kind="JIN10_EXPECTATION",
            observed_at=observation.observed_at,
            document=observation.as_dict(),
        )
        capture("3.1", event.scheduled_at + timedelta(seconds=1))
        initial = provider.child_reactions((event.event_id,))[event.event_id][0]
        initial_release = initial.release_chain[0]
        market = MarketReactionEvidence(
            event_hash=initial.identity.event_hash,
            release_hash=initial_release.content_hash,
            source="LOCAL_IBKR_READ_ONLY",
            window_start=event.scheduled_at,
            window_end=event.scheduled_at + timedelta(minutes=5),
            evidence_asof=event.scheduled_at + timedelta(minutes=5),
            observed_at=event.scheduled_at + timedelta(minutes=5, seconds=1),
            metrics={"SPY_return": "0.002"},
        )
        store.append(
            event_id=initial.identity.event_id,
            official_event_hash=identity.event_hash,
            kind="MARKET_REACTION",
            observed_at=market.observed_at,
            document=market.as_dict(),
        )
        capture(
            "3.2",
            event.scheduled_at + timedelta(seconds=2),
            revision_of=initial_release.content_hash,
        )

        original = store.assert_integrity
        calls = 0

        def counted() -> None:
            nonlocal calls
            calls += 1
            original()

        store.assert_integrity = counted  # type: ignore[method-assign]
        replayed = provider.child_reactions((event.event_id,))[event.event_id][0]
        assert calls == 1
        assert replayed.market_reaction == market
        assert replayed.release_chain[0].actual_value == Decimal("3.1")

        calls = 0
        view = provider.revision_views((event.event_id,))[event.event_id][0]
        assert calls == 1
        assert view["initial_release"]["actual_value"] == Decimal("3.1")
        assert view["revised_release"]["actual_value"] == Decimal("3.2")
        assert [row["actual_value"] for row in view["revision_history"]] == [
            Decimal("3.2")
        ]
        assert view["initial_reaction_immutable"] is True
    finally:
        provider.close()


def test_verified_batch_matches_legacy_rows_order_and_exact_keys(tmp_path: Path) -> None:
    store = ReactionEvidenceStore(tmp_path / "equality.sqlite3")
    first = ("first", "a" * 64)
    second = ("second", "b" * 64)
    try:
        _append(store, *first, 1)
        _append(store, *second, 2)
        _append(store, *first, 3)
        expected_first = store.records((first,))
        expected_second = store.records((second,))

        batch = store.verified_event_batch((second, first, second))

        assert batch.event_keys == (second, first)
        assert batch.records(*first) == expected_first
        assert batch.records(*second) == expected_second
        assert batch.release_vintages(*first) == store.release_vintages(*first)
        assert batch.records("absent", "c" * 64) == ()
    finally:
        store.close()


def test_reaction_tree_ignores_malformed_expectation_child_hint(tmp_path: Path) -> None:
    store = ReactionEvidenceStore(tmp_path / "malformed-expectation.sqlite3")
    parent = ("parent", "a" * 64)
    child = ("parent:unverified_metric", parent[1])
    try:
        store.append(
            event_id=parent[0],
            official_event_hash=parent[1],
            kind="JIN10_EXPECTATION",
            observed_at=NOW,
            document={"metric": "unverified_metric"},
        )
        _append(store, *child, 1)

        batch = store.verified_reaction_tree_batch((parent,))

        assert batch.event_keys == (parent,)
        assert batch.records(*child) == ()
    finally:
        store.close()


def test_verified_batch_bounds_unique_keys_and_rejects_invalid_identity(
    tmp_path: Path,
) -> None:
    store = ReactionEvidenceStore(tmp_path / "bounds.sqlite3")
    try:
        keys = tuple((f"event-{index}", f"{index:064x}") for index in range(500))
        assert store.verified_event_batch(keys + keys).event_keys == keys
        with pytest.raises(ValueError, match="at most 500"):
            store.verified_event_batch(keys + (("overflow", "f" * 64),))
        with pytest.raises(ValueError, match="identity is invalid"):
            store.verified_event_batch((("", "a" * 64),))
        with pytest.raises(ValueError, match="identity is invalid"):
            store.verified_event_batch((("event", "not-a-hash"),))
    finally:
        store.close()


def test_coverage_rejects_more_than_500_unique_ids_without_silent_truncation(
    tmp_path: Path,
) -> None:
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "coverage-bounds.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: NOW,
    )
    try:
        boundary = tuple(f"event-{index}" for index in range(500))
        assert provider.coverage(boundary) == {}
        assert provider.coverage((boundary[0],) * 501) == {}
        with pytest.raises(ValueError, match="at most 500"):
            provider.coverage(boundary + ("overflow",))
        with pytest.raises(ValueError, match="event id is invalid"):
            provider.coverage((" ",))
    finally:
        provider.close()


def test_corrupt_unrequested_evidence_fails_whole_verified_batch(tmp_path: Path) -> None:
    path = tmp_path / "corrupt-unrelated.sqlite3"
    store = ReactionEvidenceStore(path)
    wanted = ("wanted", "a" * 64)
    unrelated = ("outside-window", "b" * 64)
    try:
        _append(store, *wanted, 1)
        _append(store, *unrelated, 2)
        external = sqlite3.connect(path)
        try:
            external.execute("DROP TRIGGER macro_reaction_no_update")
            external.execute(
                "UPDATE macro_reaction_evidence SET document_json='{}' WHERE event_id=?",
                (unrelated[0],),
            )
            external.commit()
        finally:
            external.close()

        with pytest.raises(MacroReactionError, match="REACTION_EVIDENCE_INTEGRITY_FAILED"):
            store.verified_event_batch((wanted,))
    finally:
        store.close()


@pytest.mark.parametrize("corruption", ["raw", "cache"])
def test_corrupt_raw_or_cache_chain_fails_verified_batch(
    tmp_path: Path,
    corruption: str,
) -> None:
    path = tmp_path / f"corrupt-{corruption}.sqlite3"
    store = ReactionEvidenceStore(path)
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: NOW,
    )
    try:
        event = _official_event(1)
        provider.restore_official_identities((event,))
        identity = provider._state().identities[event.event_id]
        if corruption == "raw":
            store.append_raw_document(
                event_id=event.event_id,
                official_event_hash=identity.event_hash,
                source_role="OFFICIAL_RELEASE_DOCUMENT",
                official_url="https://www.bls.gov/news.release/cpi.nr0.htm",
                received_at=NOW,
                media_type="text/html",
                raw_bytes=b"verified official bytes",
            )
        else:
            store._append_cache_row(
                "macro_reaction_schedule_cache",
                key_name="stable_event_key",
                key_value="unrelated-cache-row",
                observed_at=NOW,
                document={
                    "schema": "options_copilot.reaction_schedule_cache.v1",
                    "stable_event_key": "unrelated-cache-row",
                    "decision_authority": "SUPPORTING_ONLY",
                },
            )
        external = sqlite3.connect(path)
        try:
            if corruption == "raw":
                external.execute("DROP TRIGGER macro_reaction_raw_no_update")
                external.execute(
                    "UPDATE macro_reaction_raw_documents SET raw_bytes=?",
                    (b"tampered",),
                )
            else:
                external.execute("DROP TRIGGER macro_reaction_schedule_cache_no_update")
                external.execute(
                    "UPDATE macro_reaction_schedule_cache SET document_json='{}'",
                )
            external.commit()
        finally:
            external.close()

        with pytest.raises(MacroReactionError):
            store.verified_event_batch(((event.event_id, identity.event_hash),))
    finally:
        provider.close()


def test_external_append_is_detected_between_verified_batch_calls(tmp_path: Path) -> None:
    path = tmp_path / "external-append.sqlite3"
    store = ReactionEvidenceStore(path)
    key = ("event", "a" * 64)
    try:
        _append(store, *key, 1)
        first = store.verified_event_batch((key,))
        external = ReactionEvidenceStore(path)
        try:
            _append(external, *key, 2)
        finally:
            external.close()
        second = store.verified_event_batch((key,))
        assert len(first.records(*key)) == 1
        assert len(second.records(*key)) == 2
    finally:
        store.close()


def test_same_connection_writer_is_excluded_from_verified_snapshot(tmp_path: Path) -> None:
    store = ReactionEvidenceStore(tmp_path / "writer-exclusion.sqlite3")
    key = ("event", "a" * 64)
    _append(store, *key, 1)
    entered = threading.Event()
    release = threading.Event()
    original = store.assert_integrity
    result: list[object] = []

    def blocked_integrity() -> None:
        entered.set()
        assert release.wait(timeout=2)
        original()

    def read_batch() -> None:
        result.append(store.verified_event_batch((key,)))

    def append_row() -> None:
        _append(store, *key, 2)

    store.assert_integrity = blocked_integrity  # type: ignore[method-assign]
    reader = threading.Thread(target=read_batch, daemon=True)
    writer = threading.Thread(target=append_row, daemon=True)
    try:
        reader.start()
        assert entered.wait(timeout=2)
        writer.start()
        assert writer.is_alive()
        release.set()
        reader.join(timeout=2)
        writer.join(timeout=2)
        assert not reader.is_alive() and not writer.is_alive()
        batch = result[0]
        assert len(batch.records(*key)) == 1  # type: ignore[union-attr]
        store.assert_integrity = original  # type: ignore[method-assign]
        assert len(store.verified_event_batch((key,)).records(*key)) == 2
    finally:
        release.set()
        store.assert_integrity = original  # type: ignore[method-assign]
        reader.join(timeout=2)
        writer.join(timeout=2)
        store.close()


def test_verified_batch_exception_releases_transaction_and_lock(tmp_path: Path) -> None:
    store = ReactionEvidenceStore(tmp_path / "exception-release.sqlite3")
    key = ("event", "a" * 64)
    original = store.assert_integrity
    try:
        def failed_integrity() -> None:
            raise MacroReactionError("EXPECTED_TEST_FAILURE")

        store.assert_integrity = failed_integrity  # type: ignore[method-assign]
        with pytest.raises(MacroReactionError, match="EXPECTED_TEST_FAILURE"):
            store.verified_event_batch((key,))
        assert store._db.in_transaction is False

        store.assert_integrity = original  # type: ignore[method-assign]
        _append(store, *key, 1)
        assert len(store.verified_event_batch((key,)).records(*key)) == 1
    finally:
        store.close()
