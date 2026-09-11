from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import threading

import pytest

from options_copilot.learning_shadow import ShadowLearningLedger
from options_copilot.llm.deepseek import DeepSeekError
from options_copilot.news.models import (
    ClassifiedEvent,
    EventCategory,
    ImpactDirection,
    ImpactHorizon,
    NewsInput,
)
from options_copilot.news.service import NewsAnalysisService
from options_copilot.news.shadow_research import ShadowResearchAdvisory
from options_copilot.news.shadow_store import NewsShadowLearningWriter
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.production_runtime import (
    ProductionPipelineInputs,
    _balanced_discovery_symbols,
    _research_allocation_evidence,
)
from options_copilot.research_allocation import (
    RESEARCH_ALLOCATION_INPUT_INVALID,
    ResearchAllocationInputError,
    build_research_allocation_evidence,
    canonical_research_symbol,
    normalise_research_allocation_evidence,
)
from options_copilot.providers import NewsEvent
from options_copilot.runtime import _normalise_research_allocation


NOW = datetime(2026, 8, 4, 12, 45, tzinfo=timezone.utc)


class _Provider:
    health = "READY"
    health_reason = None

    def __init__(
        self,
        *,
        summary: str = "Management raised guidance above the prior range.",
    ) -> None:
        self._summary = summary

    def news(self, symbols: tuple[str, ...], *, limit: int = 50):
        assert symbols == ("AAPL", "MSFT")
        assert limit == 50
        return (
            NewsEvent(
                event_id="evt-aapl-guidance",
                symbol="AAPL",
                source="trusted_wire",
                headline="Apple raises revenue guidance after strong demand",
                summary=self._summary,
                url="https://example.test/aapl-guidance",
                published_at=NOW - timedelta(minutes=2),
                first_seen_at=NOW - timedelta(minutes=1),
                ingested_at=NOW - timedelta(seconds=30),
                observed_at=NOW - timedelta(seconds=20),
                source_rank=1,
            ),
        )


class _StructuredClassifier:
    def __init__(self) -> None:
        self.calls = 0

    def classify(self, news):
        self.calls += 1
        return ClassifiedEvent(
            category=EventCategory.GUIDANCE,
            symbols=news.symbols,
            direction=ImpactDirection.BULLISH,
            horizon=ImpactHorizon.DAYS_1_3,
            confidence=Decimal("0.91"),
            counter_evidence=("Demand could normalize",),
            evidence_ids=news.evidence_ids,
            classifier="STRUCTURED_LLM",
        )


class _FailingClassifier:
    def __init__(self) -> None:
        self.calls = 0

    def classify(self, news):
        self.calls += 1
        raise RuntimeError("simulated classifier failure")


class _CappedDeepSeekClassifier:
    def classify(self, news):
        raise DeepSeekError("FLASH_DAILY_CALL_CAP")


def _analyzed_news(
    event_id: str,
    *,
    symbol: str,
    summary: str,
    evidence_id: str,
):
    news = NewsInput(
        event_id=event_id,
        headline=f"{symbol} material update",
        summary=summary,
        source="trusted_wire",
        source_url=f"https://example.test/{event_id}",
        published_at=NOW - timedelta(minutes=2),
        first_seen_at=NOW - timedelta(minutes=1),
        evidence_ids=(evidence_id,),
        symbols=(symbol,),
    )
    return NewsAnalysisService(now=lambda: NOW).analyze(news)


def test_runtime_keeps_deterministic_primary_and_persists_shadow_predictions(
    tmp_path,
) -> None:
    classifier = _StructuredClassifier()
    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW + timedelta(seconds=1),
    ) as ledger:
        enabled_at = NOW - timedelta(minutes=2)
        runtime = NewsCoordinator(
            tmp_path / "evidence.sqlite3",
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(
                ledger,
                enabled_at=enabled_at,
            ),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            runtime.refresh_once()
            first = runtime.news_payload()
            runtime.refresh_once()
            second = runtime.news_payload()
        finally:
            runtime.close()

        assert classifier.calls == 1
        assert first["count"] == second["count"] == 1
        row = second["news"][0]
        assert row["classifier"] == "DETERMINISTIC_RULES"
        assert row["research_advisory"]["classifier"] == "STRUCTURED_LLM"
        assert row["research_advisory"]["decision_authority"] == "SUPPORTING_ONLY"
        assert row["research_advisory"]["approval_eligible"] is False
        assert row["action_pool_eligible"] is False
        assert row["shadow_prediction_count"] == 5
        assert ledger.record_counts() == {
            "theses": 1,
            "evidence": 1,
            "predictions": 5,
            "outcomes": 0,
        }


def test_jin10_macro_proxy_advances_shadow_without_rebinding_original_news(
    tmp_path,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    news = NewsInput(
        event_id="evt-jin10-cpi",
        headline="美国 CPI 同比低于预期",
        summary="核心 CPI 同比放缓。",
        source="Jin10",
        source_url="https://flash.jin10.com/detail/cpi",
        published_at=NOW - timedelta(minutes=1),
        first_seen_at=NOW - timedelta(seconds=30),
        evidence_ids=("evidence-jin10-cpi",),
        symbols=(),
    )
    analysis = NewsAnalysisService(now=lambda: NOW).analyze(news)
    ranked = (replace(analysis, rank=1, rank_one=True),)
    classifier = _StructuredClassifier()

    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW,
    ) as ledger:
        runtime = NewsCoordinator(
            tmp_path / "evidence.sqlite3",
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("SPY", "QQQ", "TLT", "GLD"),
            clock=lambda: NOW,
        )
        try:
            overlays, status = runtime._shadow_advisory_overlays(
                (analysis,),
                ranked,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            runtime.close()

        assert analysis.news.symbols == ()
        assert classifier.calls == 1
        assert status["attempted_count"] == 1
        advisory = overlays[news.event_id]
        assert advisory["symbol"] == "SPY"
        assert advisory["symbol_binding"]["binding_role"] == "MARKET_PROXY"
        assert advisory["symbol_binding"]["event_category"] == "US_INFLATION"
        assert advisory["decision_authority"] == "SUPPORTING_ONLY"
        assert advisory["approval_eligible"] is False
        assert advisory["instruction_creation_allowed"] is False
        assert advisory["order_allowed"] is False
        assert ledger.record_counts()["predictions"] == 5


def test_restart_does_not_reclassify_unchanged_persisted_shadow_input(
    tmp_path,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    ledger_path = tmp_path / "shadow.sqlite3"
    evidence_path = tmp_path / "evidence.sqlite3"

    with ShadowLearningLedger(ledger_path, clock=lambda: NOW) as ledger:
        first_classifier = _StructuredClassifier()
        first_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=first_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            first_runtime.refresh_once()
        finally:
            first_runtime.close()
        counts_before_restart = ledger.record_counts()
        second_classifier = _StructuredClassifier()
        second_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=second_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            second_runtime.refresh_once()
            payload = second_runtime.news_payload()
        finally:
            second_runtime.close()

        assert first_classifier.calls == 1
        assert second_classifier.calls == 0
        assert ledger.record_counts() == counts_before_restart
        assert payload["news"][0]["research_advisory"]["classifier"] == "STRUCTURED_LLM"


def test_restart_does_not_restore_advisory_after_event_leaves_research_pool(
    tmp_path,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    analysis = _analyzed_news(
        "evt-eligibility-regression",
        symbol="AAPL",
        summary="An unchanged event that later leaves the current research pool.",
        evidence_id="eligibility-evidence",
    )
    ranked = (replace(analysis, rank=1, rank_one=True),)

    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW,
    ) as ledger:
        first_classifier = _StructuredClassifier()
        first_runtime = NewsCoordinator(
            tmp_path / "evidence.sqlite3",
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=first_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            first, _ = first_runtime._shadow_advisory_overlays(
                (analysis,),
                ranked,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            first_runtime.close()

        restart_classifier = _StructuredClassifier()
        restart_runtime = NewsCoordinator(
            tmp_path / "evidence.sqlite3",
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=restart_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            restored, status = restart_runtime._shadow_advisory_overlays(
                (analysis,),
                (),
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            restart_runtime.close()

        assert first_classifier.calls == 1
        assert "evt-eligibility-regression" in first
        assert restart_classifier.calls == 0
        assert restored == {}
        assert status["status"] == "UNAVAILABLE"
        assert status["advisory_count"] == 0


def test_restart_reclassifies_materially_changed_shadow_input_once(
    tmp_path,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    ledger_path = tmp_path / "shadow.sqlite3"
    evidence_path = tmp_path / "evidence.sqlite3"

    with ShadowLearningLedger(ledger_path, clock=lambda: NOW) as ledger:
        first_classifier = _StructuredClassifier()
        first_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=first_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            first_runtime.refresh_once()
        finally:
            first_runtime.close()
        old_advisory = ledger.query_replays(
            challenger_version="deepseek-news-advisory-v2",
            limit=1,
        )[0].prediction.prediction["advisory_id"]

        second_classifier = _StructuredClassifier()
        changed_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=second_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            changed_news = NewsInput(
                event_id="evt-aapl-guidance",
                headline="Apple raises revenue guidance after strong demand",
                summary="Management raised guidance again after stronger demand.",
                source="trusted_wire",
                source_url="https://example.test/aapl-guidance",
                published_at=NOW - timedelta(minutes=2),
                first_seen_at=NOW - timedelta(minutes=1),
                evidence_ids=("changed-immutable-evidence",),
                symbols=("AAPL",),
            )
            service = NewsAnalysisService(now=lambda: NOW)
            changed_analysis = service.analyze(changed_news)
            changed_pool = service.pre_market_research_pool((changed_analysis,))
            changed_overlays, _ = changed_runtime._shadow_advisory_overlays(
                (changed_analysis,),
                changed_pool,
                now=NOW,
                allow_model_calls=True,
            )
            changed_runtime._shadow_advisory_overlays(
                (changed_analysis,),
                changed_pool,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            changed_runtime.close()

        third_classifier = _StructuredClassifier()
        replay_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=third_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            replay_runtime._shadow_advisory_overlays(
                (changed_analysis,),
                changed_pool,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            replay_runtime.close()

        assert first_classifier.calls == 1
        assert second_classifier.calls == 1
        assert third_classifier.calls == 0
        assert changed_overlays["evt-aapl-guidance"]["advisory_id"] != old_advisory
        assert ledger.record_counts() == {
            "theses": 1,
            "evidence": 2,
            "predictions": 10,
            "outcomes": 0,
        }


def test_restart_restores_exact_identity_across_rank_reversion(tmp_path) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    ledger_path = tmp_path / "shadow.sqlite3"
    evidence_path = tmp_path / "evidence.sqlite3"
    analysis = _analyzed_news(
        "evt-rank-reversion",
        symbol="AAPL",
        summary="The immutable news input remains unchanged.",
        evidence_id="rank-reversion-evidence",
    )

    with ShadowLearningLedger(ledger_path, clock=lambda: NOW) as ledger:
        def run_fresh(rank: int) -> tuple[int, str]:
            classifier = _StructuredClassifier()
            runtime = NewsCoordinator(
                evidence_path,
                news_providers=(),
                classifier=None,
                shadow_advisory=ShadowResearchAdvisory(
                    classifier=classifier,
                    enabled_at=enabled_at,
                ),
                shadow_writer=NewsShadowLearningWriter(
                    ledger,
                    enabled_at=enabled_at,
                ),
                core_symbols=("AAPL", "MSFT"),
                clock=lambda: NOW,
            )
            ranked = (replace(analysis, rank=rank, rank_one=rank == 1),)
            try:
                overlays, _ = runtime._shadow_advisory_overlays(
                    (analysis,),
                    ranked,
                    now=NOW,
                    allow_model_calls=True,
                )
            finally:
                runtime.close()
            return classifier.calls, str(
                overlays["evt-rank-reversion"]["advisory_id"]
            )

        calls_a1, advisory_a1 = run_fresh(1)
        calls_b, advisory_b = run_fresh(2)
        calls_a2, advisory_a2 = run_fresh(1)
        calls_a3, advisory_a3 = run_fresh(1)

        assert (calls_a1, calls_b, calls_a2, calls_a3) == (1, 0, 0, 0)
        assert advisory_a1 == advisory_b == advisory_a2 == advisory_a3
        assert ledger.record_counts() == {
            "theses": 1,
            "evidence": 1,
            "predictions": 5,
            "outcomes": 0,
        }


def test_empty_runtime_input_never_falls_back_to_full_ledger_scan(
    tmp_path,
    monkeypatch,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW,
    ) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=enabled_at)

        def reject_full_scan(*args, **kwargs):
            raise AssertionError("empty targeted restore must not scan replay ledger")

        monkeypatch.setattr(ledger, "query_replays", reject_full_scan)
        runtime = NewsCoordinator(
            tmp_path / "evidence.sqlite3",
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=_StructuredClassifier(),
                enabled_at=enabled_at,
            ),
            shadow_writer=writer,
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            overlays, status = runtime._shadow_advisory_overlays(
                (),
                (),
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            runtime.close()

        assert overlays == {}
        assert status["attempted_count"] == 0


def test_inner_eligibility_skip_is_visible_and_zero_attempt_is_not_ready(
    tmp_path,
) -> None:
    analysis = _analyzed_news(
        "evt-before-shadow-enablement",
        symbol="AAPL",
        summary="Material guidance update before shadow activation.",
        evidence_id="evidence-before-shadow-enablement",
    )
    ranked = (replace(analysis, rank=1, rank_one=True),)
    classifier = _StructuredClassifier()
    runtime = NewsCoordinator(
        tmp_path / "zero-attempt-evidence.sqlite3",
        news_providers=(),
        classifier=None,
        shadow_advisory=ShadowResearchAdvisory(
            classifier=classifier,
            enabled_at=NOW,
        ),
        shadow_writer=None,
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        overlays, status = runtime._shadow_advisory_overlays(
            (analysis,),
            ranked,
            now=NOW,
            allow_model_calls=True,
        )
    finally:
        runtime.close()

    assert overlays == {}
    assert classifier.calls == 0
    assert status["status"] == "UNAVAILABLE"
    assert status["reason"] == "SHADOW_ADVISORY_NO_ATTEMPT"
    assert status["input_count"] == 1
    assert status["eligible_input_count"] == 1
    assert status["attempted_count"] == 0
    assert status["advisory_count"] == 0
    assert status["skipped_count"] == 1
    assert status["skipped_reasons"] == {
        "SHADOW_INPUT_BEFORE_ENABLEMENT": 1,
    }


def test_runtime_reports_sanitized_deepseek_failure_reason(tmp_path) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    analysis = _analyzed_news(
        "evt-aapl-cap",
        symbol="AAPL",
        summary="A material guidance update.",
        evidence_id="evidence-cap",
    )
    ranked = (replace(analysis, rank=1, rank_one=True),)
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(),
        classifier=None,
        shadow_advisory=ShadowResearchAdvisory(
            classifier=_CappedDeepSeekClassifier(),
            enabled_at=enabled_at,
        ),
        shadow_writer=None,
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        overlays, status = runtime._shadow_advisory_overlays(
            (analysis,),
            ranked,
            now=NOW,
            allow_model_calls=True,
        )
    finally:
        runtime.close()

    assert overlays == {}
    assert status["status"] == "DEGRADED"
    assert status["failure_reasons"] == {
        "DEEPSEEK_FLASH_DAILY_CALL_CAP": 1,
    }


def test_mismatched_restored_overlay_is_never_returned_on_failure_paths(
    tmp_path,
    monkeypatch,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    ledger_path = tmp_path / "shadow.sqlite3"
    evidence_path = tmp_path / "evidence.sqlite3"
    with ShadowLearningLedger(ledger_path, clock=lambda: NOW) as ledger:
        seed_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=_StructuredClassifier(),
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            seed_runtime.refresh_once()
        finally:
            seed_runtime.close()

        changed = _analyzed_news(
            "evt-aapl-guidance",
            symbol="AAPL",
            summary="A materially changed immutable event input.",
            evidence_id="changed-evidence",
        )
        changed_ranked = (replace(changed, rank=1, rank_one=True),)

        disabled_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=_StructuredClassifier(),
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            disabled, _ = disabled_runtime._shadow_advisory_overlays(
                (changed,),
                changed_ranked,
                now=NOW,
                allow_model_calls=False,
            )
            ineligible, _ = disabled_runtime._shadow_advisory_overlays(
                (changed,),
                (),
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            disabled_runtime.close()
        assert disabled == {}
        assert ineligible == {}

        failing_classifier = _FailingClassifier()
        classifier_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=failing_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            failed, _ = classifier_runtime._shadow_advisory_overlays(
                (changed,),
                changed_ranked,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            classifier_runtime.close()
        assert failed == {}
        assert failing_classifier.calls == 1

        original_record = NewsShadowLearningWriter.record

        def fail_record(self, *args, **kwargs):
            raise RuntimeError("simulated writer failure")

        monkeypatch.setattr(NewsShadowLearningWriter, "record", fail_record)
        writer_classifier = _StructuredClassifier()
        writer_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=writer_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            writer_failed, _ = writer_runtime._shadow_advisory_overlays(
                (changed,),
                changed_ranked,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            writer_runtime.close()
            monkeypatch.setattr(NewsShadowLearningWriter, "record", original_record)
        assert writer_failed == {}
        assert writer_classifier.calls == 1

        deferred_classifier = _StructuredClassifier()
        deferred_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=deferred_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT", "NVDA", "TSLA"),
            clock=lambda: NOW,
        )
        analyses = tuple(
            _analyzed_news(
                event_id,
                symbol=symbol,
                summary=f"Material update for {symbol}.",
                evidence_id=f"evidence-{index}",
            )
            for index, (event_id, symbol) in enumerate(
                (
                    ("evt-msft", "MSFT"),
                    ("evt-nvda", "NVDA"),
                    ("evt-tsla", "TSLA"),
                    ("evt-aapl-guidance", "AAPL"),
                ),
                start=1,
            )
        )
        ranked = tuple(
            replace(item, rank=index, rank_one=index == 1)
            for index, item in enumerate(analyses, start=1)
        )
        try:
            deferred, status = deferred_runtime._shadow_advisory_overlays(
                analyses,
                ranked,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            deferred_runtime.close()
        assert "evt-aapl-guidance" not in deferred
        assert status["deferred_count"] == 1
        assert deferred_classifier.calls == 3


@pytest.mark.parametrize("persisted_before_crash", (1, 2, 3, 4))
def test_restart_repairs_partial_shadow_prediction_set_without_reclassification(
    tmp_path,
    monkeypatch,
    persisted_before_crash: int,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    ledger_path = tmp_path / "shadow.sqlite3"
    evidence_path = tmp_path / "evidence.sqlite3"

    with ShadowLearningLedger(ledger_path, clock=lambda: NOW) as ledger:
        writer = NewsShadowLearningWriter(ledger, enabled_at=enabled_at)
        original_record_prediction = ShadowLearningLedger.record_prediction
        appended = 0

        def crash_after_prefix(self, *args, **kwargs):
            nonlocal appended
            if self is ledger:
                if appended >= persisted_before_crash:
                    raise RuntimeError("simulated prediction append crash")
                appended += 1
            return original_record_prediction(self, *args, **kwargs)

        monkeypatch.setattr(
            ShadowLearningLedger,
            "record_prediction",
            crash_after_prefix,
        )
        first_classifier = _StructuredClassifier()
        first_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=first_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=writer,
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            first_runtime.refresh_once()
        finally:
            first_runtime.close()
            monkeypatch.setattr(
                ShadowLearningLedger,
                "record_prediction",
                original_record_prediction,
            )

        partial = writer.advisory_projection("evt-aapl-guidance")
        assert partial is not None
        assert partial["shadow_prediction_count"] == persisted_before_crash
        assert partial["prediction_set_complete"] is False

        repair_classifier = _StructuredClassifier()
        repair_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=repair_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            repair_runtime.refresh_once()
            payload = repair_runtime.news_payload()
        finally:
            repair_runtime.close()

        repaired = writer.advisory_projection("evt-aapl-guidance")
        assert repaired is not None
        assert repaired["shadow_prediction_count"] == 5
        assert repaired["prediction_set_complete"] is True
        assert payload["news"][0]["shadow_prediction_count"] == 5
        assert repair_classifier.calls == 0

        final_classifier = _StructuredClassifier()
        final_runtime = NewsCoordinator(
            evidence_path,
            news_providers=(_Provider(),),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=final_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            final_runtime.refresh_once()
        finally:
            final_runtime.close()

        predictions = ledger.query_replays(
            challenger_version="deepseek-news-advisory-v2",
            limit=10,
        )
        assert len(predictions) == 5
        assert len({item.prediction.prediction_id for item in predictions}) == 5
        assert final_classifier.calls == 0


def test_failed_partial_advisory_repair_does_not_reclassify(
    tmp_path,
    monkeypatch,
) -> None:
    enabled_at = NOW - timedelta(minutes=2)
    analysis = _analyzed_news(
        "evt-repair-write-failure",
        symbol="AAPL",
        summary="Persisted classification must survive a repair write failure.",
        evidence_id="repair-write-evidence",
    )
    ranked = (replace(analysis, rank=1, rank_one=True),)

    with ShadowLearningLedger(
        tmp_path / "shadow.sqlite3",
        clock=lambda: NOW,
    ) as ledger:
        original_record_prediction = ShadowLearningLedger.record_prediction
        appended = 0

        def crash_after_two(self, *args, **kwargs):
            nonlocal appended
            if self is ledger:
                if appended >= 2:
                    raise RuntimeError("simulated prediction append crash")
                appended += 1
            return original_record_prediction(self, *args, **kwargs)

        monkeypatch.setattr(
            ShadowLearningLedger,
            "record_prediction",
            crash_after_two,
        )
        seed_runtime = NewsCoordinator(
            tmp_path / "evidence.sqlite3",
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=_StructuredClassifier(),
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            seed_runtime._shadow_advisory_overlays(
                (analysis,),
                ranked,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            seed_runtime.close()
            monkeypatch.setattr(
                ShadowLearningLedger,
                "record_prediction",
                original_record_prediction,
            )

        partial = NewsShadowLearningWriter(
            ledger,
            enabled_at=enabled_at,
        ).advisory_projection("evt-repair-write-failure")
        assert partial is not None
        assert partial["shadow_prediction_count"] == 2
        assert partial["prediction_set_complete"] is False

        def fail_repair_record(self, *args, **kwargs):
            raise RuntimeError("simulated repair write failure")

        monkeypatch.setattr(NewsShadowLearningWriter, "record", fail_repair_record)
        repair_classifier = _StructuredClassifier()
        repair_runtime = NewsCoordinator(
            tmp_path / "evidence.sqlite3",
            news_providers=(),
            classifier=None,
            shadow_advisory=ShadowResearchAdvisory(
                classifier=repair_classifier,
                enabled_at=enabled_at,
            ),
            shadow_writer=NewsShadowLearningWriter(ledger, enabled_at=enabled_at),
            core_symbols=("AAPL", "MSFT"),
            clock=lambda: NOW,
        )
        try:
            overlays, status = repair_runtime._shadow_advisory_overlays(
                (analysis,),
                ranked,
                now=NOW,
                allow_model_calls=True,
            )
        finally:
            repair_runtime.close()

        assert repair_classifier.calls == 0
        assert overlays == {}
        assert status["status"] == "DEGRADED"
        assert status["reason"] == "SHADOW_ADVISORY_LEDGER_REPAIR_FAILED"
        assert status["failure_count"] == 1


def test_production_symbol_priority_consumes_only_safe_shadow_score() -> None:
    payload = {
        "news": [
            {
                "symbols": ["AAPL"],
                "event_impact_score": "40",
                "research_advisory": {
                    "research_priority_score": "91",
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                },
            },
            {
                "symbols": ["MSFT"],
                "event_impact_score": "55",
                "research_advisory": {
                    "research_priority_score": "99",
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": True,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                },
            },
        ]
    }
    pipeline = object.__new__(ProductionPipelineInputs)
    pipeline._reader_lock = threading.RLock()
    pipeline._event_pool_reader = lambda: payload

    rows = pipeline._event_rows(payload)

    assert rows[0] == {
        "symbol": "AAPL",
        "score": Decimal("91"),
        "source": "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY",
        "deterministic_score": Decimal("40"),
        "advisory_score": Decimal("91"),
        "selected_research_priority_score": Decimal("91"),
        "selected_research_priority_source": "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY",
        "influence_scope": "RESEARCH_SCHEDULING_HINT_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "eligibility_effect": "NONE",
        "risk_effect": "NONE",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    assert rows[1]["symbol"] == "MSFT"
    assert rows[1]["score"] == Decimal("55")
    assert rows[1]["source"] == "NEWS_SUPPORTING_ONLY"
    assert rows[1]["deterministic_score"] == Decimal("55")
    assert rows[1]["advisory_score"] is None
    assert rows[1]["selected_research_priority_score"] == Decimal("55")
    assert rows[1]["eligibility_effect"] == "NONE"
    assert rows[1]["risk_effect"] == "NONE"


def test_safe_shadow_priority_changes_only_bounded_research_allocation() -> None:
    payload = {
        "news": [
            {
                "symbols": ["AAPL"],
                "event_impact_score": "40",
                "research_advisory": {
                    "research_priority_score": "99",
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                },
            },
            {"symbols": ["MSFT"], "event_impact_score": "80"},
            {"symbols": ["GOOG"], "event_impact_score": "70"},
        ]
    }
    pipeline = object.__new__(ProductionPipelineInputs)
    event_rows = pipeline._event_rows(payload)
    core_rows = (
        {"symbol": "SPY", "score": Decimal("60"), "source": "CORE_UNIVERSE"},
    )
    selected = _balanced_discovery_symbols(event_rows, core_rows, limit=3)
    evidence = _research_allocation_evidence(
        event_rows=event_rows,
        scanner_rows=(),
        core_rows=core_rows,
        selected_symbols=selected,
        limit=3,
    )

    assert selected == ("SPY", "AAPL", "MSFT")
    assert evidence["deterministic_baseline_symbols"] == ("SPY", "MSFT", "GOOG")
    assert evidence["advisory_available_count"] == 1
    assert evidence["advisory_selected_count"] == 1
    assert evidence["advisory_coverage_count"] == 1
    assert evidence["advisory_coverage_ratio"] == "0.333333"
    assert evidence["advisory_order_changed_count"] == 2
    assert evidence["advisory_selection_displacement_count"] == 1
    assert evidence["advisory_promoted_symbols"] == ("AAPL",)
    assert evidence["event_symbols"] == ("AAPL", "GOOG", "MSFT")
    assert evidence["influence_scope"] == "RESEARCH_SCHEDULING_HINT_ONLY"
    assert evidence["decision_authority"] == "SUPPORTING_ONLY"
    assert evidence["eligibility_effect"] == "NONE"
    assert evidence["risk_effect"] == "NONE"
    assert evidence["approval_eligible"] is False
    assert evidence["instruction_creation_allowed"] is False
    assert evidence["order_allowed"] is False
    assert evidence["schema"] == "options_copilot.research_allocation_evidence.v3"
    assert evidence["score_evidence"][0] == {
        "symbol": "AAPL",
        "deterministic_score": "40",
        "advisory_score": "99",
        "selected_research_priority_score": "99",
        "selected_research_priority_source": "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY",
    }
    assert tuple(row["symbol"] for row in evidence["score_evidence"]) == (
        "AAPL",
        "GOOG",
        "MSFT",
    )
    assert evidence["scanner_score_inputs"] == ()
    assert evidence["core_score_inputs"] == ({"symbol": "SPY", "score": "60"},)
    assert _normalise_research_allocation(evidence) == evidence


def test_shadow_allocation_v3_retains_complete_evidence_above_ten_symbols() -> None:
    payload = {
        "news": [
            {
                "symbols": [f"S{index:02d}"],
                "event_impact_score": str(40 + index),
                "research_advisory": {
                    "research_priority_score": str(90 - index),
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                },
            }
            for index in range(15)
        ]
    }
    pipeline = object.__new__(ProductionPipelineInputs)
    event_rows = pipeline._event_rows(payload)
    selected = _balanced_discovery_symbols(event_rows, (), limit=15)

    evidence = _research_allocation_evidence(
        event_rows=event_rows,
        scanner_rows=(),
        core_rows=(),
        selected_symbols=selected,
        limit=15,
    )

    assert evidence["schema"] == "options_copilot.research_allocation_evidence.v3"
    assert evidence["advisory_coverage_count"] == 15
    assert evidence["advisory_available_count"] == 15
    assert len(evidence["score_evidence"]) == 15
    assert _normalise_research_allocation(evidence) == evidence


def test_research_allocation_v3_duplicate_symbol_preserves_true_winner() -> None:
    evidence = build_research_allocation_evidence(
        event_rows=(
            {
                "symbol": "AAPL",
                "deterministic_score": Decimal("95"),
                "advisory_score": None,
            },
            {
                "symbol": "AAPL",
                "deterministic_score": Decimal("10"),
                "advisory_score": Decimal("90"),
            },
            {
                "symbol": "MSFT",
                "deterministic_score": Decimal("20"),
                "advisory_score": Decimal("96"),
            },
            {
                "symbol": "GOOG",
                "deterministic_score": Decimal("80"),
                "advisory_score": Decimal("80"),
            },
        ),
        scanner_rows=(),
        core_rows=(),
        limit=3,
    )

    rows = {str(row["symbol"]): row for row in evidence["score_evidence"]}
    assert rows["AAPL"] == {
        "symbol": "AAPL",
        "deterministic_score": "95",
        "advisory_score": "90",
        "selected_research_priority_score": "95",
        "selected_research_priority_source": "NEWS_SUPPORTING_ONLY",
    }
    assert rows["MSFT"]["selected_research_priority_source"] == (
        "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
    )
    assert rows["GOOG"]["selected_research_priority_source"] == (
        "NEWS_SUPPORTING_ONLY"
    )
    assert evidence["selected_symbols"] == ("MSFT", "AAPL", "GOOG")
    assert evidence["advisory_promoted_symbols"] == ("MSFT",)


def test_research_allocation_v3_json_arrays_normalise_to_canonical_tuples() -> None:
    evidence = build_research_allocation_evidence(
        event_rows=(
            {
                "symbol": "AAPL",
                "deterministic_score": "40",
                "advisory_score": "90",
            },
        ),
        scanner_rows=({"symbol": "MSFT", "score": "80"},),
        core_rows=({"symbol": "SPY", "score": "60"},),
        limit=3,
    )
    json_shaped = {
        key: list(value) if isinstance(value, tuple) else value
        for key, value in evidence.items()
    }

    assert normalise_research_allocation_evidence(json_shaped) == evidence


def test_research_allocation_v3_scores_are_gui_safe_decimal_strings() -> None:
    evidence = build_research_allocation_evidence(
        event_rows=(
            {
                "symbol": "AAPL",
                "deterministic_score": Decimal("-0"),
                "advisory_score": Decimal("90.50"),
            },
        ),
        scanner_rows=({"symbol": "MSFT", "score": Decimal("88.00")},),
        core_rows=({"symbol": "SPY", "score": Decimal("60.0")},),
        limit=3,
    )

    assert evidence["event_symbols"] == tuple(
        row["symbol"] for row in evidence["score_evidence"]
    )
    assert evidence["score_evidence"][0]["deterministic_score"] == "0"
    for row in evidence["score_evidence"]:
        assert isinstance(row["selected_research_priority_score"], str)
        assert row["advisory_score"] is None or isinstance(
            row["advisory_score"], str
        )
    for key in ("scanner_score_inputs", "core_score_inputs"):
        assert all(isinstance(row["score"], str) for row in evidence[key])


@pytest.mark.parametrize("invalid_score", ("bad", "NaN", "Infinity", "-1", "100.0001"))
@pytest.mark.parametrize("source", ("event", "scanner", "core"))
def test_research_allocation_v3_rejects_entire_invalid_score_input(
    invalid_score: str,
    source: str,
) -> None:
    event_rows = ({"symbol": "EVBAD", "deterministic_score": invalid_score},) if source == "event" else ()
    scanner_rows = ({"symbol": "SCBAD", "score": invalid_score},) if source == "scanner" else ()
    core_rows = ({"symbol": "COBAD", "score": invalid_score},) if source == "core" else ()

    with pytest.raises(
        ResearchAllocationInputError,
        match=RESEARCH_ALLOCATION_INPUT_INVALID,
    ):
        build_research_allocation_evidence(
            event_rows=event_rows,
            scanner_rows=scanner_rows,
            core_rows=core_rows,
            limit=30,
        )


def test_research_allocation_v3_preserves_real_zero_and_omits_invalid_symbols() -> None:
    evidence = build_research_allocation_evidence(
        event_rows=(
            {"symbol": "EVZERO", "deterministic_score": "0"},
            {"symbol": "123", "deterministic_score": "100"},
        ),
        scanner_rows=(
            {"symbol": "SCZERO", "score": 0},
            {"symbol": 123, "score": 100},
        ),
        core_rows=(
            {"symbol": "COZERO", "score": Decimal("0")},
            {"symbol": "core", "score": 100},
        ),
        limit=30,
    )

    assert evidence["score_evidence"] == (
        {
            "symbol": "EVZERO",
            "deterministic_score": "0",
            "advisory_score": None,
            "selected_research_priority_score": "0",
            "selected_research_priority_source": "NEWS_SUPPORTING_ONLY",
        },
    )
    assert evidence["scanner_score_inputs"] == (
        {"symbol": "SCZERO", "score": "0"},
    )
    assert evidence["core_score_inputs"] == (
        {"symbol": "COZERO", "score": "0"},
    )
    assert set(evidence["selected_symbols"]) == {
        "COZERO",
        "EVZERO",
        "SCZERO",
    }
    assert not any("BAD" in symbol for symbol in evidence["selected_symbols"])


@pytest.mark.parametrize(
    "invalid_advisory",
    ("bad", "NaN", "Infinity", "-1", "101"),
)
def test_research_allocation_v3_rejects_invalid_optional_advisory(
    invalid_advisory: str,
) -> None:
    with pytest.raises(
        ResearchAllocationInputError,
        match=RESEARCH_ALLOCATION_INPUT_INVALID,
    ):
        build_research_allocation_evidence(
            event_rows=(
                {
                    "symbol": "AAPL",
                    "deterministic_score": "40",
                    "advisory_score": invalid_advisory,
                },
            ),
            scanner_rows=(),
            core_rows=(),
            limit=1,
        )


def test_research_allocation_symbol_and_bounds_are_fail_closed() -> None:
    assert canonical_research_symbol("AAPL") == "AAPL"
    assert canonical_research_symbol("123") is None
    assert canonical_research_symbol(123) is None
    assert canonical_research_symbol("aapl") is None
    assert canonical_research_symbol(" AAPL") is None

    evidence = build_research_allocation_evidence(
        event_rows=tuple(
            {"symbol": f"E{index:02d}", "deterministic_score": index}
            for index in range(50)
        ),
        scanner_rows=tuple(
            {"symbol": f"S{index:02d}", "score": 100 - index}
            for index in range(40)
        ),
        core_rows=tuple(
            {"symbol": f"C{index:02d}", "score": 100 - index}
            for index in range(40)
        ),
        limit=30,
    )
    assert len(evidence["event_symbols"]) == 50
    assert len(evidence["scanner_score_inputs"]) == 30
    assert len(evidence["core_score_inputs"]) == 30
    assert len(evidence["selected_symbols"]) == 30

    with pytest.raises(ValueError, match="event rows exceed"):
        build_research_allocation_evidence(
            event_rows=tuple(
                {"symbol": f"E{index:02d}", "deterministic_score": index}
                for index in range(51)
            ),
            scanner_rows=(),
            core_rows=(),
            limit=30,
        )


@pytest.mark.parametrize("symbol", ("123", "aapl", " AAPL", 123))
def test_news_models_share_the_strict_research_symbol_domain(symbol: object) -> None:
    with pytest.raises(ValueError, match="invalid symbol"):
        NewsInput(
            event_id="evt-invalid-symbol",
            headline="Invalid symbol domain test",
            summary="The input must fail before entering news research.",
            source="trusted_wire",
            source_url="https://example.test/invalid-symbol",
            published_at=NOW - timedelta(minutes=2),
            first_seen_at=NOW - timedelta(minutes=1),
            evidence_ids=("evidence-invalid-symbol",),
            symbols=(symbol,),  # type: ignore[arg-type]
        )


def test_event_rows_reject_noncanonical_and_lower_shadow_scores() -> None:
    pipeline = object.__new__(ProductionPipelineInputs)
    rows = pipeline._event_rows(
        {
            "news": [
                {
                    "symbols": ["AAPL", "aapl", "123", 123, " AAPL"],
                    "event_impact_score": "95",
                    "research_advisory": {
                        "research_priority_score": "90",
                        "decision_authority": "SUPPORTING_ONLY",
                        "approval_eligible": False,
                        "instruction_creation_allowed": False,
                        "order_allowed": False,
                    },
                }
            ]
        }
    )

    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["score"] == Decimal("95")
    assert rows[0]["advisory_score"] == Decimal("90")
    assert rows[0]["selected_research_priority_source"] == (
        "NEWS_SUPPORTING_ONLY"
    )


@pytest.mark.parametrize("invalid_score", ("bad", "NaN", "Infinity", "-1", "101"))
def test_event_rows_reject_invalid_required_score(invalid_score: str) -> None:
    pipeline = object.__new__(ProductionPipelineInputs)
    with pytest.raises(
        ResearchAllocationInputError,
        match=RESEARCH_ALLOCATION_INPUT_INVALID,
    ):
        pipeline._event_rows(
            {
                "news": [
                    {
                        "symbols": ["BAD"],
                        "event_impact_score": invalid_score,
                    },
                ]
            }
        )


def test_event_rows_preserve_real_zero_without_advisory_fallback() -> None:
    pipeline = object.__new__(ProductionPipelineInputs)
    rows = pipeline._event_rows(
        {
            "news": [
                {
                    "symbols": ["ZERO"],
                    "event_impact_score": "0",
                },
            ]
        }
    )

    assert rows == (
        {
            "symbol": "ZERO",
            "score": Decimal("0"),
            "source": "NEWS_SUPPORTING_ONLY",
            "deterministic_score": Decimal("0"),
            "advisory_score": None,
            "selected_research_priority_score": Decimal("0"),
            "selected_research_priority_source": "NEWS_SUPPORTING_ONLY",
            "influence_scope": "RESEARCH_SCHEDULING_HINT_ONLY",
            "decision_authority": "SUPPORTING_ONLY",
            "eligibility_effect": "NONE",
            "risk_effect": "NONE",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        },
    )


def test_research_allocation_rejects_caller_selected_symbol_drift() -> None:
    with pytest.raises(ValueError, match="must match replayed producer output"):
        _research_allocation_evidence(
            event_rows=(
                {
                    "symbol": "AAPL",
                    "deterministic_score": Decimal("90"),
                    "advisory_score": None,
                },
            ),
            scanner_rows=(),
            core_rows=(),
            selected_symbols=("MSFT",),
            limit=1,
        )
