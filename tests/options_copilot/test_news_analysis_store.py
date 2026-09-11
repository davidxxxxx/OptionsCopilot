from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import threading

import pytest

from options_copilot.news.analysis_store import (
    NewsAnalysisStore,
    NewsAnalysisStoreCorruption,
    NewsAnalysisStoreError,
    analysis_contract,
    analysis_input_document,
)
from options_copilot.news.classifier import DeterministicNewsClassifier
from options_copilot.news.models import NewsAuthority, NewsInput
from options_copilot.news.service import NewsAnalysisService


NOW = datetime(2026, 8, 6, 12, 30, tzinfo=timezone.utc)


def _news(event_id: str = "evt-aapl-guidance") -> NewsInput:
    return NewsInput(
        event_id=event_id,
        headline="Apple raises guidance",
        summary="Management raised full-year guidance.",
        source="Company IR",
        source_url="https://example.test/aapl",
        published_at=NOW - timedelta(minutes=2),
        first_seen_at=NOW - timedelta(minutes=1),
        evidence_ids=("ev_fixture",),
        symbols=("AAPL",),
        authority=NewsAuthority.ANCHORED,
    )


def test_store_is_wal_full_append_only_and_reuses_exact_fingerprint(
    tmp_path: Path,
) -> None:
    classifier = DeterministicNewsClassifier()
    service = NewsAnalysisService(classifier=classifier, now=lambda: NOW)
    input_document = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("b" * 64, "a" * 64),
        analyzer_contract=analysis_contract(classifier),
        ibkr_binding=None,
    )
    calls = 0

    def analyze():
        nonlocal calls
        calls += 1
        return service.analyze(_news())

    path = tmp_path / "news-analysis.sqlite3"
    with NewsAnalysisStore(path) as store:
        assert store.integrity_verified is True
        first = store.resolve(input_document, analyze)
        second = store.resolve(input_document, analyze)

        assert calls == 1
        assert second == first
        assert second.analyzed_at == NOW
        assert store.count == 1
        assert store.journal_mode == "wal"
        assert store.synchronous == "full"
        assert store.verify_integrity() is True
        with pytest.raises(sqlite3.DatabaseError, match="immutable news analysis"):
            store._connection.execute(  # noqa: SLF001 - verifies the DB guard itself
                "UPDATE news_analysis_records SET analysis_id='changed' WHERE sequence=1"
            )


def test_two_store_instances_resolve_same_fingerprint_to_validated_winner(
    tmp_path: Path,
) -> None:
    classifier = DeterministicNewsClassifier()
    input_document = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("a" * 64,),
        analyzer_contract=analysis_contract(classifier),
        ibkr_binding=None,
    )
    path = tmp_path / "concurrent-news-analysis.sqlite3"
    factory_barrier = threading.Barrier(2)
    factory_calls: list[datetime] = []
    factory_calls_lock = threading.Lock()

    def resolve_at(store: NewsAnalysisStore, analyzed_at: datetime):
        def analyze():
            with factory_calls_lock:
                factory_calls.append(analyzed_at)
            factory_barrier.wait(timeout=5)
            return NewsAnalysisService(
                classifier=classifier,
                now=lambda: analyzed_at,
            ).analyze(_news())

        return store.resolve(input_document, analyze)

    with NewsAnalysisStore(path) as first_store, NewsAnalysisStore(path) as second_store:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(resolve_at, first_store, NOW)
            second_future = executor.submit(
                resolve_at,
                second_store,
                NOW + timedelta(seconds=1),
            )
            first_result = first_future.result(timeout=10)
            second_result = second_future.result(timeout=10)

        assert sorted(factory_calls) == [NOW, NOW + timedelta(seconds=1)]
        assert first_result == second_result
        assert first_result.analyzed_at in {
            NOW,
            NOW + timedelta(seconds=1),
        }
        assert first_store.count == second_store.count == 1
        assert first_store.verify_integrity() is True
        assert second_store.verify_integrity() is True
        persisted_authority = first_store._connection.execute(  # noqa: SLF001
            "SELECT decision_authority FROM news_analysis_records"
        ).fetchone()[0]
        assert persisted_authority == "SUPPORTING_ONLY"


def test_deferred_integrity_verification_is_bounded_and_blocks_resolution(
    tmp_path: Path,
) -> None:
    classifier = DeterministicNewsClassifier()
    contract = analysis_contract(classifier)
    path = tmp_path / "deferred-news-analysis.sqlite3"
    inputs: list[dict[str, object]] = []
    with NewsAnalysisStore(path) as store:
        for index in range(5):
            news = _news(f"evt-deferred-{index}")
            input_document = analysis_input_document(
                news=news,
                evidence_content_hashes=(f"{index + 1:064x}",),
                analyzer_contract=contract,
                ibkr_binding=None,
            )
            inputs.append(input_document)
            store.resolve(
                input_document,
                lambda news=news: NewsAnalysisService(
                    classifier=classifier,
                    now=lambda: NOW,
                ).analyze(news),
            )

    factory_called = False

    def forbidden_factory():
        nonlocal factory_called
        factory_called = True
        raise AssertionError("unverified ledger must not invoke the classifier")

    with NewsAnalysisStore(path, defer_integrity_check=True) as store:
        assert store.integrity_verified is False
        with pytest.raises(NewsAnalysisStoreError, match="verification is incomplete"):
            store.lookup(inputs[0])
        with pytest.raises(NewsAnalysisStoreError, match="verification is incomplete"):
            store.resolve(inputs[0], forbidden_factory)
        assert factory_called is False

        first = store.verify_integrity_batch(2)
        assert (
            first.batch_rows,
            first.verified_rows,
            first.remaining_rows,
            first.complete,
        ) == (2, 2, 3, False)
        with pytest.raises(NewsAnalysisStoreError, match="verification is incomplete"):
            store.resolve(inputs[0], forbidden_factory)

        second = store.verify_integrity_batch(2)
        assert (
            second.batch_rows,
            second.verified_rows,
            second.remaining_rows,
            second.complete,
        ) == (2, 4, 1, False)
        final = store.verify_integrity_batch(2)
        assert (
            final.batch_rows,
            final.verified_rows,
            final.remaining_rows,
            final.complete,
        ) == (1, 5, 0, True)
        assert store.integrity_verified is True
        assert store.lookup(inputs[0]) is not None
        assert store.resolve(inputs[0], forbidden_factory) == store.lookup(inputs[0])
        assert factory_called is False
        missing_news = _news("evt-deferred-missing")
        missing_input = analysis_input_document(
            news=missing_news,
            evidence_content_hashes=("f" * 64,),
            analyzer_contract=contract,
            ibkr_binding=None,
        )
        assert store.lookup(missing_input) is None

        complete = store.verify_integrity_batch(2)
        assert (
            complete.batch_rows,
            complete.verified_rows,
            complete.remaining_rows,
            complete.complete,
        ) == (0, 5, 0, True)


def test_deferred_integrity_fails_closed_in_the_batch_containing_corruption(
    tmp_path: Path,
) -> None:
    classifier = DeterministicNewsClassifier()
    contract = analysis_contract(classifier)
    path = tmp_path / "deferred-corrupt-news-analysis.sqlite3"
    inputs: list[dict[str, object]] = []
    with NewsAnalysisStore(path) as store:
        for index in range(4):
            news = _news(f"evt-corrupt-{index}")
            input_document = analysis_input_document(
                news=news,
                evidence_content_hashes=(f"{index + 1:064x}",),
                analyzer_contract=contract,
                ibkr_binding=None,
            )
            inputs.append(input_document)
            store.resolve(
                input_document,
                lambda news=news: NewsAnalysisService(
                    classifier=classifier,
                    now=lambda: NOW,
                ).analyze(news),
            )

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER news_analysis_records_no_update")
        connection.execute(
            "UPDATE news_analysis_records SET content_hash=? WHERE sequence=3",
            ("0" * 64,),
        )
        connection.execute(
            """CREATE TRIGGER news_analysis_records_no_update
            BEFORE UPDATE ON news_analysis_records
            BEGIN SELECT RAISE(ABORT, 'immutable news analysis: update forbidden'); END"""
        )

    factory_called = False

    def forbidden_factory():
        nonlocal factory_called
        factory_called = True
        raise AssertionError("corrupt ledger must not invoke the classifier")

    with NewsAnalysisStore(path, defer_integrity_check=True) as store:
        first = store.verify_integrity_batch(2)
        assert (first.verified_rows, first.remaining_rows, first.complete) == (
            2,
            2,
            False,
        )
        with pytest.raises(
            NewsAnalysisStoreCorruption,
            match="content hash mismatch at sequence 3",
        ):
            store.verify_integrity_batch(2)
        assert store.integrity_verified is False
        with pytest.raises(NewsAnalysisStoreError, match="verification is incomplete"):
            store.resolve(inputs[0], forbidden_factory)
        assert factory_called is False


def test_deferred_batches_avoid_full_scans_and_explicit_audit_runs_pragma(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    classifier = DeterministicNewsClassifier()
    contract = analysis_contract(classifier)
    path = tmp_path / "bounded-integrity-news-analysis.sqlite3"
    with NewsAnalysisStore(path) as store:
        for index in range(3):
            news = _news(f"evt-bounded-{index}")
            input_document = analysis_input_document(
                news=news,
                evidence_content_hashes=(f"{index + 1:064x}",),
                analyzer_contract=contract,
                ibkr_binding=None,
            )
            store.resolve(
                input_document,
                lambda news=news: NewsAnalysisService(
                    classifier=classifier,
                    now=lambda: NOW,
                ).analyze(news),
            )

    with NewsAnalysisStore(path, defer_integrity_check=True) as store:
        batch_statements: list[str] = []
        store._connection.set_trace_callback(  # noqa: SLF001 - SQL shape is the contract
            batch_statements.append
        )
        try:
            while not store.verify_integrity_batch(1).complete:
                pass
        finally:
            store._connection.set_trace_callback(None)  # noqa: SLF001

        normalized_batch_sql = [statement.upper() for statement in batch_statements]
        assert any("MAX(SEQUENCE)" in statement for statement in normalized_batch_sql)
        assert not any("COUNT(" in statement for statement in normalized_batch_sql)
        assert not any(
            "PRAGMA INTEGRITY_CHECK" in statement
            for statement in normalized_batch_sql
        )

        audit_statements: list[str] = []
        store._connection.set_trace_callback(  # noqa: SLF001 - SQL shape is the contract
            audit_statements.append
        )
        try:
            store.assert_integrity()
        finally:
            store._connection.set_trace_callback(None)  # noqa: SLF001
        assert any(
            "PRAGMA INTEGRITY_CHECK" in statement.upper()
            for statement in audit_statements
        )

        def fail_sqlite_audit() -> None:
            raise NewsAnalysisStoreCorruption("SQLite integrity check failed")

        monkeypatch.setattr(
            store,
            "_assert_sqlite_integrity_locked",
            fail_sqlite_audit,
        )
        with pytest.raises(
            NewsAnalysisStoreCorruption,
            match="SQLite integrity check failed",
        ):
            store.assert_integrity()
        assert store.integrity_verified is False


def test_fingerprint_sorts_evidence_hashes_and_binds_contract_and_ibkr_input() -> None:
    classifier = DeterministicNewsClassifier()
    contract = analysis_contract(classifier)
    first = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("b" * 64, "a" * 64),
        analyzer_contract=contract,
        ibkr_binding=None,
    )
    reordered = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("a" * 64, "b" * 64),
        analyzer_contract=contract,
        ibkr_binding=None,
    )
    changed_binding = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("a" * 64, "b" * 64),
        analyzer_contract=contract,
        ibkr_binding={
            "symbol": "AAPL",
            "quote_snapshot_id": "ibkr-quote-2",
            "tradability": {
                "symbol": "AAPL",
                "source": "IBKR",
                "observed_at": NOW.isoformat(),
                "bid": "1.00",
                "ask": "1.04",
                "volume": 250,
                "open_interest": 1000,
            },
            "confirmation": {
                "source": "IBKR",
                "observed_at": NOW.isoformat(),
                "direction": "BULLISH",
                "evidence_ids": ["ibkr-quote-2"],
            },
        },
    )
    changed_evidence = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("a" * 64, "c" * 64),
        analyzer_contract=contract,
        ibkr_binding=None,
    )

    assert first == reordered
    assert first["evidence_content_hashes"] == ["a" * 64, "b" * 64]
    assert first["fingerprint"] == reordered["fingerprint"]
    assert changed_binding["fingerprint"] != first["fingerprint"]
    assert changed_evidence["fingerprint"] != first["fingerprint"]


def test_classifier_contract_binds_deepseek_model_without_repr_or_secretish_class_name() -> None:
    from options_copilot.news.deepseek import DeepSeekNewsClassifier

    class Client:
        @staticmethod
        def complete(**_kwargs):
            raise AssertionError("building a contract must not invoke the model")

    deepseek = DeepSeekNewsClassifier(Client())
    contract = analysis_contract(deepseek)
    classifier_contract = contract["classifier"]

    assert classifier_contract["model_id"] == deepseek._model  # noqa: SLF001
    assert "Client" not in repr(contract)

    SecretTokenClassifier = type(
        "SecretTokenClassifier",
        (),
        {"classify": lambda self, news: news},
    )
    secretish_contract = analysis_contract(SecretTokenClassifier())["classifier"]
    assert secretish_contract["class_id"].startswith("class:sha256:")
    assert "SecretTokenClassifier" not in secretish_contract["class_id"]


def test_corrupt_analysis_store_fails_closed_without_invoking_factory(
    tmp_path: Path,
) -> None:
    classifier = DeterministicNewsClassifier()
    input_document = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("a" * 64,),
        analyzer_contract=analysis_contract(classifier),
        ibkr_binding=None,
    )
    path = tmp_path / "news-analysis.sqlite3"
    with NewsAnalysisStore(path) as store:
        store.resolve(
            input_document,
            lambda: NewsAnalysisService(
                classifier=classifier,
                now=lambda: NOW,
            ).analyze(_news()),
        )

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER news_analysis_records_no_update")
        connection.execute(
            "UPDATE news_analysis_records SET content_hash=? WHERE sequence=1",
            ("0" * 64,),
        )

    called = False

    def forbidden_factory():
        nonlocal called
        called = True
        raise AssertionError("corruption must not trigger re-analysis")

    with pytest.raises(
        NewsAnalysisStoreCorruption,
        match="append-only triggers|content hash mismatch",
    ):
        with NewsAnalysisStore(path) as store:
            store.resolve(input_document, forbidden_factory)
    assert called is False


def test_invalid_factory_binding_is_rejected_before_immutable_append(
    tmp_path: Path,
) -> None:
    classifier = DeterministicNewsClassifier()
    input_document = analysis_input_document(
        news=_news(),
        evidence_content_hashes=("a" * 64,),
        analyzer_contract=analysis_contract(classifier),
        ibkr_binding={
            "symbol": "AAPL",
            "quote_snapshot_id": "ibkr-quote-1",
            "tradability": {
                "symbol": "AAPL",
                "source": "IBKR",
                "observed_at": NOW.isoformat(),
                "bid": "1.00",
                "ask": "1.04",
                "volume": 250,
                "open_interest": 1000,
            },
            "confirmation": {
                "source": "IBKR",
                "observed_at": NOW.isoformat(),
                "direction": "BULLISH",
                "evidence_ids": ["ibkr-quote-1"],
            },
        },
    )
    path = tmp_path / "invalid-factory.sqlite3"
    with NewsAnalysisStore(path) as store:
        with pytest.raises(
            NewsAnalysisStoreCorruption,
            match="tradability output binding mismatch",
        ):
            store.resolve(
                input_document,
                lambda: NewsAnalysisService(
                    classifier=classifier,
                    now=lambda: NOW,
                ).analyze(_news()),
            )
        assert store.count == 0
