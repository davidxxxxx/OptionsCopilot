from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from options_copilot.learning_shadow import ShadowLearningLedger
from options_copilot.news.models import (
    ClassifiedEvent,
    EventCategory,
    ImpactDirection,
    ImpactHorizon,
    NewsAuthority,
    NewsInput,
)
from options_copilot.news.shadow_research import (
    PredictionSpec,
    ResearchAdvisoryProjection,
)
from options_copilot.news.shadow_store import (
    CHALLENGER_VERSION,
    NewsShadowLearningWriter,
)
from options_copilot.news.shadow_prediction import (
    NEWS_SHADOW_PREDICTION_SCHEMA,
    news_shadow_prediction_id,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 8, 1, 0, tzinfo=timezone.utc)


def _news() -> NewsInput:
    return NewsInput(
        event_id="event-new-1",
        headline="Company raises guidance",
        summary="Demand exceeded the prior range.",
        source="Company IR",
        source_url="https://example.test/news",
        published_at=NOW + timedelta(minutes=1),
        first_seen_at=NOW + timedelta(minutes=2),
        evidence_ids=("source-evidence-1",),
        symbols=("AAPL",),
        authority=NewsAuthority.ANCHORED,
    )


def _advisory() -> ResearchAdvisoryProjection:
    classification = ClassifiedEvent(
        category=EventCategory.GUIDANCE,
        symbols=("AAPL",),
        direction=ImpactDirection.BULLISH,
        horizon=ImpactHorizon.DAYS_1_3,
        confidence=Decimal("0.82"),
        counter_evidence=("Demand could normalize",),
        evidence_ids=("source-evidence-1",),
        classifier="STRUCTURED_LLM",
    )
    advisory_id = "news-advisory:" + "a" * 64
    independence_key = "news-event:" + "b" * 64
    horizons = (
        ("30M", "PREDICTED_AT_PLUS_30_MINUTES", "30m"),
        ("SESSION_CLOSE", "NEXT_ELIGIBLE_SESSION_CLOSE", "session-close"),
        ("1D", "SESSION_CLOSE_PLUS_1_TRADING_DAY", "1d"),
        ("3D", "SESSION_CLOSE_PLUS_3_TRADING_DAYS", "3d"),
        ("5D", "SESSION_CLOSE_PLUS_5_TRADING_DAYS", "5d"),
    )
    return ResearchAdvisoryProjection(
        advisory_id=advisory_id,
        event_id="event-new-1",
        symbol="AAPL",
        classification=classification,
        research_priority_score=Decimal("88.50"),
        prediction_specs=tuple(
            PredictionSpec(
                prediction_id=f"{advisory_id}:{slug}",
                advisory_id=advisory_id,
                event_id="event-new-1",
                symbol="AAPL",
                horizon=horizon,
                target_rule=rule,
                independence_key=independence_key,
            )
            for horizon, rule, slug in horizons
        ),
    )


def test_writer_appends_five_idempotent_supporting_only_predictions(tmp_path) -> None:
    with ShadowLearningLedger(tmp_path / "shadow.sqlite3", clock=lambda: NOW + timedelta(minutes=4)) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        first = writer.record(
            _advisory(),
            _news(),
            recorded_at=NOW + timedelta(minutes=3),
        )
        second = writer.record(
            _advisory(),
            _news(),
            recorded_at=NOW + timedelta(minutes=3),
        )

        assert first.appended_predictions == 5
        assert second.appended_predictions == 0
        assert ledger.record_counts() == {
            "theses": 1,
            "evidence": 1,
            "predictions": 5,
            "outcomes": 0,
        }
        predictions = ledger.query_replays(
            challenger_version=CHALLENGER_VERSION,
            limit=10,
        )
        assert len(predictions) == 5
        assert len({item.prediction.independence_key for item in predictions}) == 1
        assert all(
            item.prediction.prediction["approval_eligible"] is False
            and item.prediction.prediction["instruction_creation_allowed"] is False
            and item.prediction.prediction["order_allowed"] is False
            for item in predictions
        )
        assert ledger.independent_sample_count(CHALLENGER_VERSION) == 0
        evidence = ledger.get_evidence(first.evidence_id).evidence
        expected_model_hash = evidence["model_visible_snapshot_hash"]
        assert evidence["model_visible_snapshot_schema"] == (
            "options_copilot.deepseek_public_news_snapshot.v1"
        )
        assert len(expected_model_hash) == 64
        assert all(
            replay.prediction.prediction["model_visible_snapshot_hash"]
            == expected_model_hash
            for replay in predictions
        )

        restored = writer.advisory_projection("event-new-1")
        assert restored is not None
        assert restored["research_priority_score"] == "88.50"
        assert restored["shadow_prediction_count"] == 5
        assert restored["decision_authority"] == "SUPPORTING_ONLY"
        assert restored["approval_eligible"] is False


def test_new_predictions_bind_versioned_point_in_time_champion_baseline(tmp_path) -> None:
    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW + timedelta(minutes=4),
    ) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        champion = replace(_advisory().classification, classifier="DETERMINISTIC")
        writer.record(
            _advisory(),
            _news(),
            recorded_at=NOW + timedelta(minutes=3),
            champion_classification=champion,
        )
        rows = ledger.query_replays(limit=10)
        assert len(rows) == 5
        for row in rows:
            baseline = row.prediction.prediction["champion_baseline"]
            assert baseline["schema"] == (
                "options_copilot.deterministic_champion_baseline.v1"
            )
            body = {key: value for key, value in baseline.items() if key != "baseline_hash"}
            assert baseline["baseline_hash"] == canonical_hash(body)
            assert baseline["classification_hash"] == canonical_hash(
                baseline["classification"]
            )


def test_zero_prediction_crash_reuses_evidence_bound_champion_baseline(
    tmp_path,
    monkeypatch,
) -> None:
    with ShadowLearningLedger(
        tmp_path / "shadow-zero-crash.sqlite3",
        clock=lambda: NOW + timedelta(minutes=6),
    ) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        original = replace(_advisory().classification, classifier="DETERMINISTIC")
        original_challenger = _advisory().classification
        changed = replace(
            original,
            direction=ImpactDirection.BEARISH,
            confidence=Decimal("0.55"),
        )
        real_record_prediction = ledger.record_prediction

        def crash_before_first_prediction(*args, **kwargs):
            raise RuntimeError("simulated zero-prediction crash")

        monkeypatch.setattr(ledger, "record_prediction", crash_before_first_prediction)
        with pytest.raises(RuntimeError, match="zero-prediction crash"):
            writer.record(
                _advisory(),
                _news(),
                recorded_at=NOW + timedelta(minutes=3),
                champion_classification=original,
            )
        assert ledger.record_counts()["evidence"] == 1
        assert ledger.record_counts()["predictions"] == 0

        monkeypatch.setattr(ledger, "record_prediction", real_record_prediction)
        changed_advisory = replace(
            _advisory(),
            classification=replace(
                original_challenger,
                direction=ImpactDirection.BEARISH,
                confidence=Decimal("0.51"),
            ),
        )
        writer.record(
            changed_advisory,
            _news(),
            recorded_at=NOW + timedelta(minutes=5),
            champion_classification=changed,
        )
        rows = ledger.query_replays(limit=10)
        assert len(rows) == 5
        assert all(
            canonical_hash(
                row.prediction.prediction["champion_baseline"]["classification"]
            )
            == canonical_hash(original.as_dict())
            for row in rows
        )
        assert all(
            row.prediction.predicted_at == NOW + timedelta(minutes=3)
            for row in rows
        )
        assert all(
            canonical_hash(row.prediction.prediction["classification"])
            == canonical_hash(original_challenger.as_dict())
            for row in rows
        )


def test_writer_reuses_original_activation_fence_after_restart(tmp_path) -> None:
    path = tmp_path / "shadow.sqlite3"
    with ShadowLearningLedger(path, clock=lambda: NOW + timedelta(minutes=4)) as ledger:
        first = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        assert first.enabled_at == NOW

    restarted_at = NOW + timedelta(days=2)
    with ShadowLearningLedger(path, clock=lambda: restarted_at) as ledger:
        restarted = NewsShadowLearningWriter(ledger, enabled_at=restarted_at)
        assert restarted.enabled_at == NOW
        result = restarted.record(
            _advisory(),
            _news(),
            recorded_at=restarted_at,
        )

        assert result.appended_predictions == 5
        assert ledger.record_counts() == {
            "theses": 1,
            "evidence": 1,
            "predictions": 5,
            "outcomes": 0,
        }


def test_revised_model_visible_text_creates_new_bound_evidence(tmp_path) -> None:
    with ShadowLearningLedger(
        tmp_path / "shadow-revision.sqlite3",
        clock=lambda: NOW + timedelta(minutes=5),
    ) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        original_news = _news()
        original_advisory = _advisory()
        first = writer.record(
            original_advisory,
            original_news,
            recorded_at=NOW + timedelta(minutes=3),
        )
        revised_news = replace(
            original_news,
            summary="Demand exceeded the prior range and guidance was revised.",
        )
        revised_advisory_id = "news-advisory:" + "c" * 64
        revised_advisory = replace(
            original_advisory,
            advisory_id=revised_advisory_id,
            prediction_specs=tuple(
                replace(
                    spec,
                    advisory_id=revised_advisory_id,
                    prediction_id=(
                        revised_advisory_id
                        + spec.prediction_id[len(original_advisory.advisory_id):]
                    ),
                )
                for spec in original_advisory.prediction_specs
            ),
        )

        second = writer.record(
            revised_advisory,
            revised_news,
            recorded_at=NOW + timedelta(minutes=4),
        )

        assert second.evidence_id != first.evidence_id
        first_evidence = ledger.get_evidence(first.evidence_id)
        second_evidence = ledger.get_evidence(second.evidence_id)
        assert first_evidence.evidence["model_visible_snapshot_hash"] != (
            second_evidence.evidence["model_visible_snapshot_hash"]
        )
        prediction = ledger.get_prediction(second.prediction_ids[0])
        assert prediction.evidence_ids == (second.evidence_id,)
        assert prediction.prediction["model_visible_snapshot_hash"] == (
            second_evidence.evidence["model_visible_snapshot_hash"]
        )


def test_advisory_restore_pages_beyond_first_five_thousand_predictions(
    tmp_path,
    monkeypatch,
) -> None:
    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW,
    ) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        irrelevant = SimpleNamespace(
            prediction=SimpleNamespace(
                sequence=1,
                prediction_id="irrelevant:30m",
                prediction={
                    "schema": "options_copilot.news_shadow_prediction.v1",
                    "event_id": "irrelevant",
                    "advisory_id": "irrelevant",
                    "horizon": "30M",
                },
            )
        )
        first_page_tail = SimpleNamespace(
            prediction=SimpleNamespace(
                sequence=5000,
                prediction_id="irrelevant:5d",
                prediction={
                    "schema": "options_copilot.news_shadow_prediction.v1",
                    "event_id": "irrelevant",
                    "advisory_id": "irrelevant",
                    "horizon": "5D",
                },
            )
        )
        advisory = _advisory()
        second_page = tuple(
                SimpleNamespace(
                    prediction=SimpleNamespace(
                        sequence=5001 + index,
                            prediction_id=news_shadow_prediction_id(
                                advisory.advisory_id,
                                spec.prediction_id.rsplit(":", 1)[-1],
                            ),
                            independence_key=spec.independence_key,
                            predicted_at=NOW,
                            prediction={
                                "schema": NEWS_SHADOW_PREDICTION_SCHEMA,
                            "advisory_id": advisory.advisory_id,
                            "event_id": advisory.event_id,
                            "symbol": advisory.symbol,
                            "horizon": spec.horizon,
                            "target_rule": spec.target_rule,
                            "research_priority_score": str(
                                advisory.research_priority_score
                            ),
                            "classification": advisory.classification.as_dict(),
                            "symbol_binding": None,
                                "model_visible_snapshot_hash": "f" * 64,
                                "prediction_set_predicted_at": NOW.isoformat(),
                                "prediction_baseline_hash": canonical_hash(
                                    {
                                        "schema": "options_copilot.news_prediction_baseline.v2",
                                        "advisory_id": advisory.advisory_id,
                                        "event_id": advisory.event_id,
                                        "symbol": advisory.symbol,
                                        "model_visible_snapshot_hash": "f" * 64,
                                        "prediction_set_predicted_at": NOW,
                                    }
                                ),
                            "decision_authority": "SUPPORTING_ONLY",
                            "approval_eligible": False,
                            "instruction_creation_allowed": False,
                            "order_allowed": False,
                        },
                    )
                )
            for index, spec in enumerate(advisory.prediction_specs)
        )
        calls: list[int] = []

        def paged_query(*, after_sequence=0, **kwargs):
            calls.append(after_sequence)
            if after_sequence == 0:
                return (irrelevant,) * 4999 + (first_page_tail,)
            if after_sequence == 5000:
                return second_page
            return ()

        monkeypatch.setattr(ledger, "query_replays", paged_query)

        restored = writer.advisory_projection("event-new-1")

        assert calls == [0, 5000]
        assert restored is not None
        assert restored["shadow_prediction_count"] == 5
        assert restored["prediction_set_complete"] is True


@pytest.mark.parametrize(
    ("mutation", "expected_count"),
    (
        ("symbol", 4),
        ("target_rule", 4),
        ("mapping_version", 4),
        ("model_hash", 4),
        ("independence_key", 5),
        ("decision_authority", 4),
        ("approval_flag", 4),
        ("instruction_flag", 4),
        ("order_flag", 4),
    ),
)
def test_restore_rejects_each_inconsistent_prediction_field(
    tmp_path,
    monkeypatch,
    mutation,
    expected_count,
) -> None:
    with ShadowLearningLedger(
        tmp_path / "shadow-inconsistent.sqlite3",
        clock=lambda: NOW + timedelta(minutes=4),
    ) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        writer.record(
            _advisory(),
            _news(),
            recorded_at=NOW + timedelta(minutes=3),
        )
        original_query = ledger.query_predictions_by_ids

        def inconsistent_query(prediction_ids, *, challenger_version):
            rows = list(
                original_query(
                    prediction_ids,
                    challenger_version=challenger_version,
                )
            )
            payload = dict(rows[-1].prediction)
            if mutation == "symbol":
                payload["symbol"] = "MSFT"
            elif mutation == "target_rule":
                payload["target_rule"] = "WRONG_TARGET_RULE"
            elif mutation == "mapping_version":
                payload["symbol_binding"] = {
                    "event_category": "US_INFLATION",
                    "source": "JIN10",
                    "proxy_symbol": "SPY",
                    "mapping_version": "STALE",
                    "mapping_hash": "0" * 64,
                    "method": "DETERMINISTIC_KEYWORD_CATEGORY_MAP",
                    "binding_role": "MARKET_PROXY",
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                }
            elif mutation == "model_hash":
                payload["model_visible_snapshot_hash"] = "e" * 64
            elif mutation == "decision_authority":
                payload["decision_authority"] = "PRIMARY"
            elif mutation == "approval_flag":
                payload["approval_eligible"] = True
            elif mutation == "instruction_flag":
                payload["instruction_creation_allowed"] = True
            elif mutation == "order_flag":
                payload["order_allowed"] = True
            record_changes = {"prediction": payload}
            if mutation == "independence_key":
                record_changes["independence_key"] = "different-event-key"
            rows[-1] = replace(rows[-1], **record_changes)
            return tuple(rows)

        monkeypatch.setattr(
            ledger,
            "query_predictions_by_ids",
            inconsistent_query,
        )

        restored = writer.advisory_projections(
            ("event-new-1",),
            expected_advisory_ids={
                "event-new-1": _advisory().advisory_id,
            },
        )["event-new-1"]

        assert restored["shadow_prediction_count"] == expected_count
        assert restored["prediction_set_complete"] is False


def test_exact_identity_restore_uses_bounded_targeted_sql_lookup(
    tmp_path,
    monkeypatch,
) -> None:
    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW + timedelta(minutes=4),
    ) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=NOW)
        writer.record(
            _advisory(),
            _news(),
            recorded_at=NOW + timedelta(minutes=3),
        )
        original_query = ledger.query_predictions_by_ids
        requested: list[tuple[str, ...]] = []

        def targeted_query(prediction_ids, *, challenger_version):
            requested.append(tuple(prediction_ids))
            return original_query(
                prediction_ids,
                challenger_version=challenger_version,
            )

        def reject_full_scan(*args, **kwargs):
            raise AssertionError("exact restore must not scan the full replay ledger")

        monkeypatch.setattr(ledger, "query_predictions_by_ids", targeted_query)
        monkeypatch.setattr(ledger, "query_replays", reject_full_scan)

        restored = writer.advisory_projections(
            ("event-new-1",),
            expected_advisory_ids={
                "event-new-1": _advisory().advisory_id,
            },
        )

        assert len(requested) == 1
        assert set(requested[0]) == {
            news_shadow_prediction_id(
                _advisory().advisory_id,
                spec.prediction_id.rsplit(":", 1)[-1],
            )
            for spec in _advisory().prediction_specs
        }
        assert restored["event-new-1"]["prediction_set_complete"] is True
