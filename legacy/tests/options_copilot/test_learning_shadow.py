from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import sqlite3
from pathlib import Path

import pytest

from options_copilot.learning_shadow import (
    DISCOVERY_SAMPLE_THRESHOLD,
    DuplicateRecordError,
    GovernanceStage,
    GovernanceState,
    LedgerTampered,
    ShadowLearningLedger,
    TimeTravelError,
)


BASE = datetime(2026, 8, 5, 1, 0, tzinfo=timezone.utc)
NOW = BASE + timedelta(days=7)


def _ledger(tmp_path: Path) -> ShadowLearningLedger:
    return ShadowLearningLedger(tmp_path / "shadow-learning.sqlite3", clock=lambda: NOW)


def _seed_prediction(
    ledger: ShadowLearningLedger,
    *,
    suffix: str = "1",
    independence_key: str | None = None,
    prediction_tags: tuple[str, ...] = ("rates", "bullish"),
):
    thesis = ledger.record_thesis(
        f"thesis-{suffix}",
        champion_version="champion-v1",
        challenger_version="challenger-v2",
        thesis={"claim": "falling real yields support GLD", "direction": "UP"},
        created_at=BASE,
        tags=("GLD", "macro"),
    )
    evidence = ledger.record_evidence(
        f"evidence-{suffix}",
        thesis.thesis_id,
        source="official.release",
        evidence={"real_yield_bps": -5, "headline": "Treasury release"},
        published_at=BASE + timedelta(minutes=1),
        first_seen_at=BASE + timedelta(minutes=2),
        tags=("rates",),
    )
    prediction = ledger.record_prediction(
        f"prediction-{suffix}",
        thesis.thesis_id,
        evidence_ids=(evidence.evidence_id,),
        prediction={"probability": "0.62", "direction": "UP"},
        predicted_at=BASE + timedelta(minutes=3),
        horizon_at=BASE + timedelta(hours=1),
        independence_key=independence_key or f"macro-window-{suffix}",
        tags=prediction_tags,
    )
    return thesis, evidence, prediction


def test_hash_bound_records_are_immutable_replayable_and_durable(tmp_path: Path) -> None:
    path = tmp_path / "shadow-learning.sqlite3"
    with ShadowLearningLedger(path, clock=lambda: NOW) as ledger:
        thesis, evidence, prediction = _seed_prediction(ledger)
        outcome = ledger.resolve_outcome(
            prediction.prediction_id,
            outcome={"realized_return": "0.013", "grade": "correct"},
            observed_at=BASE + timedelta(hours=2),
            resolved_at=BASE + timedelta(hours=2, minutes=1),
        )

        assert ledger.journal_mode == "wal"
        assert ledger.synchronous == "full"
        assert prediction.thesis_hash == thesis.content_hash
        assert prediction.evidence_bindings[0].evidence_id == evidence.evidence_id
        assert prediction.evidence_bindings[0].evidence_hash == evidence.content_hash
        assert outcome.prediction_hash == prediction.content_hash
        assert outcome.independence_key == prediction.independence_key
        assert ledger.verify_integrity() is True

        replay = ledger.replay(prediction.prediction_id)
        assert replay.thesis == thesis
        assert replay.evidence == (evidence,)
        assert replay.prediction == prediction
        assert replay.outcome == outcome

        with pytest.raises(TypeError):
            thesis.thesis["direction"] = "DOWN"  # type: ignore[index]
        with pytest.raises(FrozenInstanceError):
            prediction.content_hash = "0" * 64  # type: ignore[misc]

    with ShadowLearningLedger(path, clock=lambda: NOW) as reopened:
        assert reopened.replay("prediction-1").outcome == outcome
        assert reopened.verify_integrity() is True


def test_duplicate_ids_and_sql_mutation_are_rejected(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        _seed_prediction(ledger)
        with pytest.raises(DuplicateRecordError, match="thesis-1"):
            ledger.record_thesis(
                "thesis-1",
                champion_version="champion-v1",
                challenger_version="challenger-v2",
                thesis={"claim": "duplicate"},
                created_at=BASE,
            )

        with sqlite3.connect(ledger.path) as connection:
            with pytest.raises(sqlite3.IntegrityError, match="update forbidden"):
                connection.execute(
                    "UPDATE shadow_learning_records SET record_id='changed' "
                    "WHERE record_id='thesis-1'"
                )
            with pytest.raises(sqlite3.IntegrityError, match="delete forbidden"):
                connection.execute(
                    "DELETE FROM shadow_learning_records WHERE record_id='evidence-1'"
                )


def test_point_in_time_and_outcome_chronology_fail_closed(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        thesis = ledger.record_thesis(
            "thesis-time",
            champion_version="champion-v1",
            challenger_version="challenger-v2",
            thesis={"claim": "time ordering"},
            created_at=BASE,
        )
        future_evidence = ledger.record_evidence(
            "evidence-future",
            thesis.thesis_id,
            source="official.release",
            evidence={"value": 1},
            published_at=BASE + timedelta(minutes=4),
            first_seen_at=BASE + timedelta(minutes=5),
        )
        with pytest.raises(TimeTravelError, match="not knowable"):
            ledger.record_prediction(
                "prediction-backdated",
                thesis.thesis_id,
                evidence_ids=(future_evidence.evidence_id,),
                prediction={"direction": "UP"},
                predicted_at=BASE + timedelta(minutes=3),
                independence_key="window-backdated",
            )

        _, _, prediction = _seed_prediction(ledger, suffix="valid")
        with pytest.raises(TimeTravelError, match="precede"):
            ledger.resolve_outcome(
                prediction.prediction_id,
                outcome={"return": "0.01"},
                observed_at=prediction.predicted_at - timedelta(microseconds=1),
                resolved_at=prediction.predicted_at + timedelta(minutes=1),
            )


def test_tampered_sqlite_content_is_detected_on_reopen_and_read(tmp_path: Path) -> None:
    path = tmp_path / "shadow-learning.sqlite3"
    with ShadowLearningLedger(path, clock=lambda: NOW) as ledger:
        _seed_prediction(ledger)

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER shadow_learning_records_no_update")
        connection.execute(
            "UPDATE shadow_learning_records "
            "SET document_json=replace(document_json, 'bullish', 'bearish') "
            "WHERE record_id='prediction-1'"
        )
        connection.commit()

    with ShadowLearningLedger(path, clock=lambda: NOW) as reopened:
        with pytest.raises(LedgerTampered, match="content hash"):
            reopened.replay("prediction-1")


def test_similarity_is_deterministic_read_only_and_respects_as_of(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        _, _, first = _seed_prediction(
            ledger,
            suffix="rates",
            prediction_tags=("GLD", "rates", "bullish"),
        )
        ledger.resolve_outcome(
            first.prediction_id,
            outcome={"return": "0.02"},
            observed_at=BASE + timedelta(hours=2),
            resolved_at=BASE + timedelta(hours=2, minutes=1),
        )
        _, _, second = _seed_prediction(
            ledger,
            suffix="earnings",
            prediction_tags=("GLD", "earnings"),
        )

        matches = ledger.query_similar(("GLD", "rates", "bullish"), limit=2)
        assert [item.replay.prediction.prediction_id for item in matches] == [
            first.prediction_id,
            second.prediction_id,
        ]
        assert matches[0].similarity == pytest.approx(1.0)
        assert matches[0].distance == pytest.approx(0.0)
        assert matches[0].replay.outcome is not None
        assert matches[1].similarity < matches[0].similarity

        before_resolution = ledger.replay(
            first.prediction_id,
            as_of=BASE + timedelta(hours=2),
        )
        assert before_resolution.outcome is None
        after_resolution = ledger.replay(
            first.prediction_id,
            as_of=BASE + timedelta(hours=2, minutes=1),
        )
        assert after_resolution.outcome is not None
        assert ledger.query_similar(
            ("GLD",), resolved_only=True
        )[0].replay.prediction.prediction_id == first.prediction_id


def test_thirty_independent_resolved_samples_reach_discovery_only(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        thesis = ledger.record_thesis(
            "thesis-governance",
            champion_version="champion-v1",
            challenger_version="challenger-v2",
            thesis={"claim": "shadow cohort"},
            created_at=BASE,
            tags=("cohort",),
        )
        evidence = ledger.record_evidence(
            "evidence-governance",
            thesis.thesis_id,
            source="official.release",
            evidence={"fixture": True},
            published_at=BASE + timedelta(minutes=1),
            first_seen_at=BASE + timedelta(minutes=2),
        )

        # Thirty-one resolved predictions, but two share one independence key.
        for index in range(DISCOVERY_SAMPLE_THRESHOLD + 1):
            key_index = 0 if index == 1 else index
            predicted_at = BASE + timedelta(minutes=3, seconds=index)
            prediction = ledger.record_prediction(
                f"prediction-governance-{index}",
                thesis.thesis_id,
                evidence_ids=(evidence.evidence_id,),
                prediction={"index": index},
                predicted_at=predicted_at,
                horizon_at=BASE + timedelta(hours=1, seconds=index),
                independence_key=f"independent-window-{key_index}",
            )
            ledger.resolve_outcome(
                prediction.prediction_id,
                outcome={"index": index},
                observed_at=BASE + timedelta(hours=2, seconds=index),
                resolved_at=BASE + timedelta(hours=3, seconds=index),
            )
            if index == DISCOVERY_SAMPLE_THRESHOLD - 1:
                collecting = ledger.governance_state("challenger-v2")
                assert collecting.independent_samples == DISCOVERY_SAMPLE_THRESHOLD - 1
                assert collecting.stage is GovernanceStage.COLLECTING

        state = ledger.governance_state("challenger-v2")
        assert state.independent_samples == DISCOVERY_SAMPLE_THRESHOLD
        assert state.stage is GovernanceStage.DISCOVERY
        assert state.mode == "SHADOW_ONLY"
        assert state.grade == "DISCOVERY"
        assert state.can_auto_promote is False
        assert state.can_change_production_weights is False
        assert state.can_change_production_rules is False
        assert state.a_grade_15_percent_unlocked is False
        assert state.promotion_requires_external_human_approval is True
        assert state.a_grade_requires_external_human_approval is True
        assert not any(
            hasattr(ledger, name)
            for name in ("promote", "approve_promotion", "unlock_a_grade", "submit_order")
        )
        with pytest.raises(ValueError, match="production authority"):
            GovernanceState(
                challenger_version="challenger-v2",
                independent_samples=DISCOVERY_SAMPLE_THRESHOLD,
                discovery_threshold=DISCOVERY_SAMPLE_THRESHOLD,
                stage=GovernanceStage.DISCOVERY,
                grade="DISCOVERY",
                can_auto_promote=True,
            )
