from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Callable

import pytest

from options_copilot.llm.deepseek import DeepSeekError

from options_copilot.news.models import (
    ClassifiedEvent,
    EventCategory,
    ImpactDirection,
    ImpactHorizon,
    NewsInput,
)
from options_copilot.news.shadow_research import (
    ResearchAdvisoryInput,
    ShadowResearchAdvisory,
)


ENABLED_AT = datetime(2026, 8, 8, 0, 0, tzinfo=timezone.utc)
PHASE2_FIXTURE_ROOT = (
    Path(__file__).parent / "fixtures" / "phase2_eval" / "candidate_v1"
)


def _phase2_case(case_id: str) -> dict[str, object]:
    cases = json.loads(
        (PHASE2_FIXTURE_ROOT / "cases.json").read_text(encoding="utf-8")
    )
    return next(case for case in cases if case["case_id"] == case_id)


def _news(
    event_id: str = "event-1",
    *,
    first_seen_at: datetime | None = None,
    symbols: tuple[str, ...] = ("AAPL",),
    complete: bool = True,
    conflicts: tuple[str, ...] = (),
    evidence_ids: tuple[str, ...] = ("evidence-1",),
) -> NewsInput:
    seen_at = first_seen_at or ENABLED_AT + timedelta(minutes=5)
    return NewsInput(
        event_id=event_id,
        headline=f"Headline for {event_id}",
        summary=f"Summary for {event_id}",
        source="TEST",
        source_url=f"https://example.test/{event_id}",
        published_at=seen_at - timedelta(minutes=1),
        first_seen_at=seen_at,
        evidence_ids=evidence_ids,
        symbols=symbols,
        conflicting_evidence_ids=conflicts,
        is_complete=complete,
    )


def _classification(
    news: NewsInput,
    *,
    classifier: str = "STRUCTURED_LLM",
    symbols: tuple[str, ...] | None = None,
    evidence_ids: tuple[str, ...] | None = None,
) -> ClassifiedEvent:
    return ClassifiedEvent(
        category=EventCategory.EARNINGS,
        symbols=news.symbols if symbols is None else symbols,
        direction=ImpactDirection.BULLISH,
        horizon=ImpactHorizon.DAYS_1_3,
        confidence=Decimal("0.80"),
        counter_evidence=(),
        evidence_ids=news.evidence_ids if evidence_ids is None else evidence_ids,
        classifier=classifier,
    )


class _Classifier:
    def __init__(self, behavior: Callable[[NewsInput, int], object] | None = None):
        self.calls: list[str] = []
        self._behavior = behavior or (lambda news, _attempt: _classification(news))

    def classify(self, news: NewsInput) -> object:
        self.calls.append(news.event_id)
        return self._behavior(news, len(self.calls))


def _input(
    news: NewsInput | None = None,
    *,
    eligible: bool = True,
    rank: int | None = 1,
) -> ResearchAdvisoryInput:
    return ResearchAdvisoryInput(
        news=news or _news(),
        eligible=eligible,
        pre_model_priority_rank=rank,
    )


def test_only_fully_eligible_post_enable_priority_event_calls_classifier() -> None:
    classifier = _Classifier()
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)
    inputs = (
        _input(_news("good"), rank=1),
        _input(_news("not-explicitly-eligible"), eligible=False, rank=2),
        _input(_news("at-enable", first_seen_at=ENABLED_AT), rank=3),
        _input(_news("before-enable", first_seen_at=ENABLED_AT - timedelta(seconds=1)), rank=4),
        _input(_news("multi-symbol", symbols=("AAPL", "MSFT")), rank=5),
        _input(_news("incomplete", complete=False), rank=6),
        _input(_news("conflict", conflicts=("conflicting-evidence",)), rank=7),
        _input(_news("outside-priority-set"), rank=None),
    )

    batch = component.process(inputs)

    assert classifier.calls == ["good"]
    assert batch.attempted_count == 1
    assert [item.event_id for item in batch.advisories] == ["good"]
    assert batch.failures == ()
    assert batch.skipped_count == 7
    assert dict(batch.skipped_reasons) == {
        "SHADOW_INPUT_BEFORE_ENABLEMENT": 2,
        "SHADOW_INPUT_CONFLICTED": 1,
        "SHADOW_INPUT_INCOMPLETE": 1,
        "SHADOW_INPUT_NOT_ELIGIBLE": 1,
        "SHADOW_INPUT_PRIORITY_RANK_MISSING": 1,
        "SHADOW_INPUT_SYMBOL_COUNT_INVALID": 1,
    }


def test_batch_attempts_at_most_three_in_pre_model_priority_order() -> None:
    classifier = _Classifier()
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)
    inputs = tuple(
        _input(_news(f"event-{rank}"), rank=rank)
        for rank in (5, 2, 4, 1, 3, 6)
    )

    batch = component.process(inputs)

    assert classifier.calls == ["event-1", "event-2", "event-3"]
    assert batch.attempted_count == 3
    assert batch.deferred_count == 3
    assert [item.event_id for item in batch.advisories] == [
        "event-1",
        "event-2",
        "event-3",
    ]


def test_same_immutable_news_input_is_idempotent_across_rank_changes() -> None:
    classifier = _Classifier()
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)
    candidate = _input()

    first = component.process((candidate, candidate))
    second = component.process((candidate,))
    changed = component.process((replace(candidate, pre_model_priority_rank=2),))

    assert classifier.calls == ["event-1"]
    assert first.advisories == second.advisories
    assert len(first.advisories) == 1
    assert first.attempted_count == 1
    assert second.attempted_count == 0
    assert changed.attempted_count == 0
    assert changed.advisories == first.advisories


def test_success_is_supporting_only_and_emits_five_stable_prediction_specs() -> None:
    classifier = _Classifier()
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)

    advisory = component.process((_input(),)).advisories[0]

    assert advisory.research_priority_score == Decimal("93.00")
    assert advisory.advisory_id.startswith("news-advisory:")
    assert advisory.decision_authority == "SUPPORTING_ONLY"
    assert advisory.approval_eligible is False
    assert advisory.instruction_creation_allowed is False
    assert advisory.order_allowed is False
    assert [spec.horizon for spec in advisory.prediction_specs] == [
        "30M",
        "SESSION_CLOSE",
        "1D",
        "3D",
        "5D",
    ]
    assert [spec.target_rule for spec in advisory.prediction_specs] == [
        "PREDICTED_AT_PLUS_30_MINUTES",
        "NEXT_ELIGIBLE_SESSION_CLOSE",
        "SESSION_CLOSE_PLUS_1_TRADING_DAY",
        "SESSION_CLOSE_PLUS_3_TRADING_DAYS",
        "SESSION_CLOSE_PLUS_5_TRADING_DAYS",
    ]
    assert len({spec.independence_key for spec in advisory.prediction_specs}) == 1
    assert len({spec.prediction_id for spec in advisory.prediction_specs}) == 5
    for spec in advisory.prediction_specs:
        assert spec.decision_authority == "SUPPORTING_ONLY"
        assert spec.approval_eligible is False
        assert spec.instruction_creation_allowed is False
        assert spec.order_allowed is False


def test_transient_failure_is_retryable_and_not_cached() -> None:
    def behavior(news: NewsInput, attempt: int) -> object:
        if attempt == 1:
            raise RuntimeError("temporary provider outage with sensitive detail")
        return _classification(news)

    classifier = _Classifier(behavior)
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)

    failed = component.process((_input(),))
    recovered = component.process((_input(),))

    assert classifier.calls == ["event-1", "event-1"]
    assert failed.advisories == ()
    assert failed.failures[0].reason_code == "CLASSIFIER_TRANSIENT_FAILURE"
    assert failed.failures[0].retryable is True
    assert recovered.failures == ()
    assert len(recovered.advisories) == 1
    assert "sensitive detail" not in str(failed.as_dict())


def test_deepseek_fixed_failure_reason_is_projected_without_raw_detail() -> None:
    classifier = _Classifier(
        lambda _news, _attempt: (_ for _ in ()).throw(
            DeepSeekError("FLASH_DAILY_CALL_CAP")
        )
    )
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)

    failed = component.process((_input(),))

    assert failed.failures[0].reason_code == "DEEPSEEK_FLASH_DAILY_CALL_CAP"
    assert failed.failures[0].retryable is True
    assert "provider" not in str(failed.as_dict()).casefold()


@pytest.mark.parametrize(
    ("error", "reason_code"),
    [
        (ValueError("schema detail must not escape"), "CLASSIFIER_VALUE_ERROR"),
        (TypeError("type detail must not escape"), "CLASSIFIER_TYPE_ERROR"),
    ],
)
def test_schema_or_type_failure_is_permanent_and_cached(
    error: Exception,
    reason_code: str,
) -> None:
    def behavior(_news: NewsInput, _attempt: int) -> object:
        raise error

    classifier = _Classifier(behavior)
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)

    first = component.process((_input(),))
    second = component.process((_input(),))

    assert classifier.calls == ["event-1"]
    assert first.failures == second.failures
    assert first.failures[0].reason_code == reason_code
    assert first.failures[0].retryable is False
    assert str(error) not in str(first.as_dict())


@pytest.mark.parametrize(
    ("behavior", "reason_code"),
    [
        (lambda _news, _attempt: {"category": "EARNINGS"}, "CLASSIFIER_RESULT_TYPE_INVALID"),
        (
            lambda news, _attempt: _classification(news, symbols=("INVENTED",)),
            "CLASSIFIER_INVENTED_SYMBOL",
        ),
        (
            lambda news, _attempt: _classification(news, evidence_ids=("invented-evidence",)),
            "CLASSIFIER_INVENTED_EVIDENCE",
        ),
    ],
)
def test_invalid_or_invented_model_output_is_permanently_rejected(
    behavior: Callable[[NewsInput, int], object],
    reason_code: str,
) -> None:
    classifier = _Classifier(behavior)
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)

    first = component.process((_input(),))
    second = component.process((_input(),))

    assert classifier.calls == ["event-1"]
    assert first.failures == second.failures
    assert first.failures[0].reason_code == reason_code
    assert first.failures[0].retryable is False


def test_deterministic_fallback_is_retryable_and_never_becomes_advisory() -> None:
    def behavior(news: NewsInput, attempt: int) -> object:
        classifier = "DETERMINISTIC_RULES" if attempt == 1 else "STRUCTURED_LLM"
        return _classification(news, classifier=classifier)

    classifier = _Classifier(behavior)
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)

    fallback = component.process((_input(),))
    structured = component.process((_input(),))

    assert fallback.advisories == ()
    assert fallback.failures[0].reason_code == "CLASSIFIER_NOT_STRUCTURED_LLM"
    assert fallback.failures[0].retryable is True
    assert len(structured.advisories) == 1
    assert classifier.calls == ["event-1", "event-1"]


def test_projection_serialization_contains_no_execution_objects_or_error_details() -> None:
    component = ShadowResearchAdvisory(classifier=_Classifier(), enabled_at=ENABLED_AT)

    payload = component.process((_input(),)).as_dict()
    rendered = str(payload).lower()

    assert "approval_id" not in rendered
    assert "bridge_payload" not in rendered
    assert "order_instruction" not in rendered
    assert "exception" not in rendered
    assert "traceback" not in rendered
    assert payload["advisories"][0]["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["advisories"][0]["approval_eligible"] is False
    assert payload["advisories"][0]["instruction_creation_allowed"] is False
    assert payload["advisories"][0]["order_allowed"] is False


def test_independent_components_produce_identical_ids_and_specs() -> None:
    candidate = _input()
    first = ShadowResearchAdvisory(
        classifier=_Classifier(),
        enabled_at=ENABLED_AT,
    ).process((candidate,)).advisories[0]
    second = ShadowResearchAdvisory(
        classifier=_Classifier(),
        enabled_at=ENABLED_AT,
    ).process((candidate,)).advisories[0]

    assert first.advisory_id == second.advisory_id
    assert first.prediction_specs == second.prediction_specs


def test_phase2_p2_15_transient_retry_and_valid_only_exact_cache() -> None:
    candidate = _phase2_case("P2-15")
    assert candidate["details"]["repetitions"] == 3

    def behavior(news: NewsInput, attempt: int) -> object:
        if attempt == 1:
            raise RuntimeError("synthetic transient detail must not be retained")
        return _classification(news)

    classifier = _Classifier(behavior)
    component = ShadowResearchAdvisory(classifier=classifier, enabled_at=ENABLED_AT)
    research_input = _input()

    failed = component.process((research_input,))
    recovered = component.process((research_input,))
    cached = component.process((research_input,))

    assert classifier.calls == ["event-1", "event-1"]
    assert failed.failures[0].reason_code == "CLASSIFIER_TRANSIENT_FAILURE"
    assert recovered.advisories == cached.advisories
    assert recovered.attempted_count == 1
    assert cached.attempted_count == 0


def test_phase2_shadow_batch_and_projection_are_supporting_only_action_false() -> None:
    batch = ShadowResearchAdvisory(
        classifier=_Classifier(),
        enabled_at=ENABLED_AT,
    ).process((_input(),))
    advisory = batch.advisories[0]

    assert batch.decision_authority == "SUPPORTING_ONLY"
    assert batch.approval_eligible is False
    assert batch.instruction_creation_allowed is False
    assert batch.order_allowed is False
    assert advisory.decision_authority == "SUPPORTING_ONLY"
    assert advisory.approval_eligible is False
    assert advisory.instruction_creation_allowed is False
    assert advisory.order_allowed is False


def test_phase2_candidate_manifest_keeps_model_evaluation_pending() -> None:
    manifest = json.loads(
        (PHASE2_FIXTURE_ROOT / "manifest.json").read_text(encoding="utf-8")
    )

    assert manifest["review_status"] == "PENDING_HUMAN_REVIEW"
    assert manifest["formal_gold"] is False
    assert manifest["model_enablement_state"] == "MODEL_EVALUATION_PENDING"
    assert manifest["live_model_calls_allowed"] is False
