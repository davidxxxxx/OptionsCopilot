"""Truthful durable shadow evaluation collection tests."""
from __future__ import annotations

from datetime import datetime, timezone
from datetime import timedelta
import sqlite3
import time
from decimal import Decimal

import pytest

import options_copilot.learning.evaluation_runtime as evaluation_runtime_module
from options_copilot.learning.evaluation_runtime import (
    ShadowEvaluationCorruption,
    ShadowEvaluationStore,
)
from options_copilot.learning_shadow import ShadowLearningLedger
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)


def test_zero_sample_evaluation_is_collecting_and_hash_bound(tmp_path) -> None:
    ledger = ShadowLearningLedger(
        tmp_path / "shadow.sqlite3", clock=lambda: NOW + timedelta(minutes=1)
    )
    store = ShadowEvaluationStore(tmp_path / "evaluation.sqlite3")
    try:
        first = store.refresh(
            ledger,
            generated_at=datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc),
        )
        second = store.refresh(
            ledger,
            generated_at=datetime(2026, 8, 22, 12, 1, tzinfo=timezone.utc),
        )
        assert first == second
        assert first["status"] == "COLLECTING"
        assert first["reason"] == "ZERO_INDEPENDENT_SAMPLES"
        assert first["independent_count"] == 0
        assert first["total_sample_count"] == 0
        assert first["comparison_complete"] is False
        assert first["can_change_production_weights"] is False
        assert first["can_change_ranking"] is False
        assert first["can_change_risk"] is False
        assert len(str(first["dataset_hash"])) == 64
        assert len(str(first["report_hash"])) == 64
        count = store._connection.execute(
            "SELECT COUNT(*) FROM shadow_evaluation_reports"
        ).fetchone()[0]
        assert count == 1
    finally:
        store.close()
        ledger.close()


def test_deadline_crossing_during_locked_integrity_work_prevents_insert(
    tmp_path,
    monkeypatch,
) -> None:
    clock = {"now": NOW}
    deadline = NOW + timedelta(seconds=1)
    ledger = ShadowLearningLedger(
        tmp_path / "shadow-deadline.sqlite3",
        clock=lambda: NOW + timedelta(minutes=1),
    )
    store = ShadowEvaluationStore(tmp_path / "evaluation-deadline.sqlite3")
    original_assert_integrity = store.assert_integrity

    def integrity_then_cross_deadline() -> None:
        original_assert_integrity()
        clock["now"] = deadline

    monkeypatch.setattr(store, "assert_integrity", integrity_then_cross_deadline)
    try:
        report = store.refresh(
            ledger,
            generated_at=NOW,
            deadline_at=deadline,
            clock=lambda: clock["now"],
        )

        assert report["status"] == "CANCELLED"
        assert report["persisted"] is False
        count = store._connection.execute(
            "SELECT COUNT(*) FROM shadow_evaluation_reports"
        ).fetchone()[0]
        assert count == 0
    finally:
        store.close()
        ledger.close()


def test_deadline_crossing_during_row_projection_stops_remaining_work(
    tmp_path,
    monkeypatch,
) -> None:
    clock = {"now": NOW}
    deadline = NOW + timedelta(seconds=1)
    ledger = ShadowLearningLedger(
        tmp_path / "shadow-row-deadline.sqlite3",
        clock=lambda: NOW + timedelta(minutes=1),
    )
    store = ShadowEvaluationStore(tmp_path / "evaluation-row-deadline.sqlite3")
    _seed_resolved_prediction(
        ledger,
        suffix="deadline-first",
        with_champion=True,
    )
    _seed_resolved_prediction(
        ledger,
        suffix="deadline-second",
        with_champion=True,
    )
    original_evaluation_row = evaluation_runtime_module._evaluation_row
    projected: list[str] = []

    def project_then_cross_deadline(prediction, outcome):
        projected.append(prediction.prediction_id)
        row = original_evaluation_row(prediction, outcome)
        clock["now"] = deadline
        return row

    monkeypatch.setattr(
        evaluation_runtime_module,
        "_evaluation_row",
        project_then_cross_deadline,
    )
    try:
        report = store.refresh(
            ledger,
            generated_at=NOW,
            deadline_at=deadline,
            clock=lambda: clock["now"],
        )

        assert report["status"] == "CANCELLED"
        assert report["reason"] == "SHADOW_EVALUATION_DEFERRED_DEADLINE"
        assert report["persisted"] is False
        assert len(projected) == 1
        count = store._connection.execute(
            "SELECT COUNT(*) FROM shadow_evaluation_reports"
        ).fetchone()[0]
        assert count == 0
    finally:
        store.close()
        ledger.close()


def test_writer_contention_cannot_commit_after_shadow_deadline(tmp_path) -> None:
    ledger = ShadowLearningLedger(
        tmp_path / "shadow-writer-contention.sqlite3",
        clock=lambda: NOW + timedelta(minutes=1),
    )
    path = tmp_path / "evaluation-writer-contention.sqlite3"
    store = ShadowEvaluationStore(path)
    blocker = sqlite3.connect(path, isolation_level=None, timeout=1)
    _seed_resolved_prediction(
        ledger,
        suffix="writer-contention",
        with_champion=True,
    )
    blocker.execute("PRAGMA journal_mode=WAL")
    blocker.execute("BEGIN IMMEDIATE")
    started = time.perf_counter()
    try:
        report = store.refresh(
            ledger,
            generated_at=NOW,
            deadline_at=datetime.now(timezone.utc) + timedelta(milliseconds=75),
        )
        elapsed = time.perf_counter() - started

        assert report["status"] == "CANCELLED"
        assert report["reason"] == "SHADOW_EVALUATION_DEFERRED_DEADLINE"
        assert report["persisted"] is False
        assert elapsed < 1
        count = store._connection.execute(
            "SELECT COUNT(*) FROM shadow_evaluation_reports"
        ).fetchone()[0]
        assert count == 0
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()
        store.close()
        ledger.close()


def _seed_resolved_prediction(
    ledger: ShadowLearningLedger,
    *,
    suffix: str,
    with_champion: bool,
    challenger_classification: object = None,
    champion_classification_override: object = None,
    tamper_baseline: bool = False,
    underlying: object = None,
) -> None:
    thesis = ledger.record_thesis(
        f"thesis-{suffix}",
        champion_version="deterministic-news-research-v1",
        challenger_version="deepseek-news-advisory-v2",
        thesis={"purpose": "comparison"},
        created_at=NOW - timedelta(minutes=5),
    )
    evidence = ledger.record_evidence(
        f"evidence-{suffix}",
        thesis.thesis_id,
        source="TEST_ONLY",
        evidence={"symbol": "SPY"},
        published_at=NOW - timedelta(minutes=4),
        first_seen_at=NOW - timedelta(minutes=3),
    )
    champion_classification = (
        {"direction": "BEARISH", "confidence": "0.60"}
        if champion_classification_override is None
        else champion_classification_override
    )
    baseline_body = {
        "schema": "options_copilot.deterministic_champion_baseline.v1",
        "champion_version": "deterministic-news-research-v1",
        "analysis_cutoff_at": (NOW - timedelta(minutes=2)).isoformat(),
        "classification": champion_classification,
        "classification_hash": canonical_hash(champion_classification),
    }
    baseline = (
        {**baseline_body, "baseline_hash": canonical_hash(baseline_body)}
        if with_champion
        else None
    )
    if tamper_baseline and isinstance(baseline, dict):
        baseline["baseline_hash"] = "f" * 64
    prediction = ledger.record_prediction(
        f"prediction-{suffix}",
        thesis.thesis_id,
        evidence_ids=(evidence.evidence_id,),
        prediction={
            "schema": "options_copilot.news_shadow_prediction.v2",
            "symbol": "SPY",
            "classification": (
                {"direction": "BULLISH", "confidence": "0.80"}
                if challenger_classification is None
                else challenger_classification
            ),
            "model_visible_snapshot_hash": "a" * 64,
            "champion_baseline": baseline,
        },
        predicted_at=NOW - timedelta(minutes=2),
        independence_key=f"event:{suffix}",
    )
    ledger.resolve_outcome(
        prediction.prediction_id,
        outcome={
            "underlying": (
                {"baseline_price": "100", "price": "102"}
                if underlying is None
                else underlying
            ),
            "thesis_validity": {"status": "VALID", "valid": True},
        },
        observed_at=NOW,
        resolved_at=NOW,
        prediction_hash=prediction.content_hash,
    )


def test_nonzero_samples_compare_champion_and_challenger_by_independent_key(
    tmp_path,
) -> None:
    ledger = ShadowLearningLedger(
        tmp_path / "shadow.sqlite3", clock=lambda: NOW + timedelta(minutes=1)
    )
    store = ShadowEvaluationStore(tmp_path / "evaluation.sqlite3")
    try:
        _seed_resolved_prediction(ledger, suffix="eligible", with_champion=True)
        _seed_resolved_prediction(ledger, suffix="legacy", with_champion=False)
        report = store.refresh(ledger, generated_at=NOW)
        assert report["status"] == "AVAILABLE"
        assert report["reason"] is None
        assert report["comparison_complete"] is True
        assert report["total_sample_count"] == 2
        assert report["independent_count"] == 1
        assert report["excluded_sample_count"] == 1
        assert report["champion_accuracy"] == Decimal("0")
        assert report["challenger_accuracy"] == Decimal("1")
        assert report["challenger_accuracy_delta"] == Decimal("1")
        assert report["challenger_brier_improvement"] == Decimal("0.32")
    finally:
        store.close()
        ledger.close()


def test_missing_champion_baseline_stays_explicitly_collecting(tmp_path) -> None:
    ledger = ShadowLearningLedger(
        tmp_path / "shadow.sqlite3", clock=lambda: NOW + timedelta(minutes=1)
    )
    store = ShadowEvaluationStore(tmp_path / "evaluation.sqlite3")
    try:
        _seed_resolved_prediction(ledger, suffix="legacy", with_champion=False)
        report = store.refresh(ledger, generated_at=NOW)
        assert report["status"] == "COLLECTING"
        assert report["reason"] == "CHAMPION_BASELINE_UNAVAILABLE"
        assert report["independent_count"] == 0
        assert report["excluded_sample_count"] == 1
        assert report["comparison_complete"] is False
        assert report["exclusion_reason_counts"] == {
            "CHAMPION_BASELINE_UNAVAILABLE": 1
        }
    finally:
        store.close()
        ledger.close()


@pytest.mark.parametrize(
    ("seed_kwargs", "expected_reason"),
    (
        ({"challenger_classification": "bad"}, "CHALLENGER_CLASSIFICATION_INVALID"),
        ({"with_champion": False}, "CHAMPION_BASELINE_UNAVAILABLE"),
        ({"tamper_baseline": True}, "CHAMPION_BASELINE_INVALID"),
        ({"underlying": {"baseline_price": "100", "price": "100"}}, "MARKET_DIRECTION_UNAVAILABLE"),
        (
            {"challenger_classification": {"direction": "BULLISH", "confidence": "NaN"}},
            "CLASSIFICATION_PROBABILITY_INVALID",
        ),
    ),
)
def test_zero_eligible_report_uses_truthful_exclusion_reason(
    tmp_path,
    seed_kwargs,
    expected_reason,
) -> None:
    ledger = ShadowLearningLedger(
        tmp_path / f"shadow-{expected_reason}.sqlite3",
        clock=lambda: NOW + timedelta(minutes=1),
    )
    store = ShadowEvaluationStore(tmp_path / f"evaluation-{expected_reason}.sqlite3")
    try:
        kwargs = {"with_champion": True, **seed_kwargs}
        _seed_resolved_prediction(
            ledger,
            suffix=expected_reason.lower(),
            **kwargs,
        )
        report = store.refresh(ledger, generated_at=NOW)
        assert report["status"] == "COLLECTING"
        assert report["reason"] == expected_reason
        assert report["exclusion_reason_counts"] == {expected_reason: 1}
        assert report["independent_count"] == 0
    finally:
        store.close()
        ledger.close()


def test_evaluation_store_is_append_only_and_tamper_fails_closed(tmp_path) -> None:
    ledger = ShadowLearningLedger(tmp_path / "shadow.sqlite3")
    store = ShadowEvaluationStore(tmp_path / "evaluation.sqlite3")
    try:
        store.refresh(ledger, generated_at=NOW)
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(
                "UPDATE shadow_evaluation_reports SET report_hash=? WHERE sequence=1",
                ("f" * 64,),
            )
        store._connection.execute("DROP TRIGGER shadow_evaluation_no_update")
        store._connection.execute(
            "UPDATE shadow_evaluation_reports SET report_hash=? WHERE sequence=1",
            ("f" * 64,),
        )
        with pytest.raises(ShadowEvaluationCorruption, match="trigger"):
            store.assert_integrity()
    finally:
        store.close()
        ledger.close()
