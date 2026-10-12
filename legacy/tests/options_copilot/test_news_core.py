from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.news.analysis_store import analysis_contract
from options_copilot.news.calendar import CalendarWindow, EventCalendar
from options_copilot.news.classifier import (
    CachedNewsClassifier,
    DeterministicNewsClassifier,
    FailSafeNewsClassifier,
    StructuredLlmAdapter,
)
from options_copilot.news.learning import OutcomeObservation, ShadowLearningLedger
from options_copilot.news.models import (
    CalendarEvent,
    EventCategory,
    ImpactDirection,
    ImpactHorizon,
    MarketConfirmation,
    NewsAuthority,
    NewsInput,
    OptionTradabilityInput,
    ScoreBand,
)
from options_copilot.news.service import NewsAnalysisService


NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)


def _news(**overrides: object) -> NewsInput:
    values: dict[str, object] = {
        "event_id": "news-1",
        "headline": "Company raises full-year guidance after strong demand",
        "summary": "Management increased revenue guidance.",
        "source": "Company IR",
        "source_url": "https://example.test/news-1",
        "published_at": NOW,
        "first_seen_at": NOW + timedelta(seconds=10),
        "evidence_ids": ("ir-2026-08-03",),
        "symbols": ("AAPL",),
        "authority": NewsAuthority.ANCHORED,
    }
    values.update(overrides)
    return NewsInput(**values)  # type: ignore[arg-type]


def _ibkr(**overrides: object) -> OptionTradabilityInput:
    values: dict[str, object] = {
        "symbol": "AAPL",
        "source": "IBKR",
        "observed_at": NOW,
        "bid": Decimal("1.00"),
        "ask": Decimal("1.04"),
        "volume": 250,
        "open_interest": 1000,
    }
    values.update(overrides)
    return OptionTradabilityInput(**values)  # type: ignore[arg-type]


def test_deterministic_classifier_is_timezone_aware_and_contains_no_option_legs() -> None:
    result = DeterministicNewsClassifier().classify(_news())

    assert result.category is EventCategory.GUIDANCE
    assert result.direction is ImpactDirection.BULLISH
    assert result.evidence_ids == ("ir-2026-08-03",)
    assert "leg" not in result.as_dict()


def test_deterministic_classifier_does_not_treat_sector_as_sec_regulation() -> None:
    classifier = DeterministicNewsClassifier()
    result = classifier.classify(
        _news(
            event_id="avgo-sector-valuation",
            headline="Broadcom: The Market Has This One Wrong",
            summary="Broadcom trades at a discount to the sector median despite strong demand.",
            symbols=("AVGO",),
        )
    )

    assert result.category is EventCategory.ANALYST
    assert result.direction is ImpactDirection.BULLISH
    assert analysis_contract(classifier)["classifier"]["contract_version"] == "4"


def test_deterministic_classifier_keeps_real_sec_filing_regulatory() -> None:
    result = DeterministicNewsClassifier().classify(
        _news(
            event_id="sec-filing",
            headline="Company discloses SEC filing",
            summary="The SEC filing describes an investigation by the regulator.",
        )
    )

    assert result.category is EventCategory.REGULATORY


def test_deterministic_classifier_understands_chinese_earnings_guidance() -> None:
    result = DeterministicNewsClassifier().classify(
        _news(
            event_id="jin10-nvda-results",
            headline="英伟达交出超预期财报，明年营收预计再增70%，股价盘后由跌转涨",
            summary=(
                "英伟达最新季度营收和净利润双双超过市场预期，公司上调下一财年"
                "销售额增长指引；供应限制仍是风险。"
            ),
            source="Jin10",
            symbols=("NVDA",),
        )
    )

    assert result.category is EventCategory.EARNINGS
    assert result.direction is ImpactDirection.BULLISH
    assert result.confidence == Decimal("0.65")


@pytest.mark.parametrize(
    ("headline", "expected_direction"),
    (
        ("公司季度业绩显示成本下降，净利润超预期", ImpactDirection.BULLISH),
        ("公司财报显示净亏损同比增长70%", ImpactDirection.BEARISH),
        ("公司财报显示营收增长放缓且低于预期", ImpactDirection.BEARISH),
        ("公司财报显示营收增长明显放缓", ImpactDirection.BEARISH),
        ("公司财报显示营收增长显著放缓", ImpactDirection.BEARISH),
        ("公司财报显示营收增长大幅放缓", ImpactDirection.BEARISH),
        ("公司财报显示营收增长 放缓", ImpactDirection.BEARISH),
        ("公司财报显示营收同比增长70%", ImpactDirection.BULLISH),
        ("公司财报显示成本同比增长70%", ImpactDirection.BEARISH),
    ),
)
def test_deterministic_classifier_uses_metric_aware_chinese_direction(
    headline: str,
    expected_direction: ImpactDirection,
) -> None:
    result = DeterministicNewsClassifier().classify(
        _news(
            event_id="jin10-corporate-metric",
            headline=headline,
            summary="公司披露最新经营数据。",
            source="Jin10",
            symbols=("NVDA",),
        )
    )

    assert result.category is EventCategory.EARNINGS
    assert result.direction is expected_direction


def test_deterministic_classifier_understands_standalone_sales_growth_guidance() -> None:
    result = DeterministicNewsClassifier().classify(
        _news(
            event_id="jin10-sales-guidance",
            headline="公司上调下一财年销售额增长指引",
            summary="公司发布最新业务展望。",
            source="Jin10",
            symbols=("NVDA",),
        )
    )

    assert result.category is EventCategory.GUIDANCE
    assert result.direction is ImpactDirection.BULLISH


@pytest.mark.parametrize(
    ("headline", "expected_direction"),
    (
        ("美国CPI低于预期，通胀回落", ImpactDirection.BULLISH),
        ("美国CPI高于预期，通胀回升", ImpactDirection.BEARISH),
        ("美国CPI年率低于预期", ImpactDirection.BULLISH),
        ("美国核心CPI同比高于市场预期", ImpactDirection.BEARISH),
        ("美国消费者价格指数年率低于市场预期", ImpactDirection.BULLISH),
    ),
)
def test_deterministic_classifier_applies_chinese_macro_surprise_semantics(
    headline: str,
    expected_direction: ImpactDirection,
) -> None:
    result = DeterministicNewsClassifier().classify(
        _news(
            event_id="jin10-cpi-surprise",
            headline=headline,
            summary="美国消费者价格指数公布。",
            source="Jin10",
            symbols=("SPY", "QQQ"),
        )
    )

    assert result.category is EventCategory.MACRO
    assert result.direction is expected_direction
    assert result.horizon is ImpactHorizon.INTRADAY


def test_structured_llm_rejects_any_option_leg_or_unexpected_schema_field() -> None:
    adapter = StructuredLlmAdapter(
        lambda _request: {
            "category": "EARNINGS",
            "symbols": ["AAPL"],
            "direction": "BULLISH",
            "horizon": "DAYS_1_3",
            "confidence": "0.8",
            "counter_evidence": ["valuation is elevated"],
            "evidence_ids": ["ir-2026-08-03"],
            "option_legs": ["buy call"],
        }
    )
    with pytest.raises(ValueError, match="not permitted"):
        adapter.classify(_news())


def test_structured_llm_cannot_invent_symbols_or_evidence() -> None:
    adapter = StructuredLlmAdapter(
        lambda _request: {
            "category": "EARNINGS",
            "symbols": ["MSFT"],
            "direction": "BULLISH",
            "horizon": "DAYS_1_3",
            "confidence": "0.8",
            "counter_evidence": [],
            "evidence_ids": ["invented"],
        }
    )
    with pytest.raises(ValueError, match="invent"):
        adapter.classify(_news())


def test_fail_safe_classifier_downgrades_model_failure_without_leaking_details() -> None:
    class _BrokenClassifier:
        def classify(self, news: NewsInput):
            del news
            raise RuntimeError("secret-shaped provider failure")

    result = FailSafeNewsClassifier(_BrokenClassifier()).classify(_news())

    assert result.classifier == "DETERMINISTIC_RULES"
    assert result.category is EventCategory.GUIDANCE
    assert result.counter_evidence == (
        "Deterministic fallback; model corroboration unavailable",
    )
    assert "secret-shaped" not in str(result.as_dict())


def test_successful_model_classification_is_cached_by_immutable_news_input() -> None:
    calls = 0

    class _CountingClassifier:
        def classify(self, news: NewsInput):
            nonlocal calls
            calls += 1
            return DeterministicNewsClassifier().classify(news)

    cached = CachedNewsClassifier(_CountingClassifier(), maximum_entries=2)

    assert cached.classify(_news()) == cached.classify(_news())
    assert calls == 1


def test_scores_are_separate_and_tradability_requires_complete_ibkr_hard_data() -> None:
    service = NewsAnalysisService(now=lambda: NOW)
    result = service.analyze(_news(), _ibkr())

    assert result.event_impact is ScoreBand.HIGH
    assert result.option_tradability is ScoreBand.HIGH
    assert result.combined_opportunity is ScoreBand.HIGH
    assert result.event_impact_score == Decimal("87.75")
    assert result.option_tradability_score == Decimal("96.08")
    assert result.combined_opportunity_score == Decimal("91.08")
    assert result.approval_eligible is False

    missing = service.analyze(_news(event_id="news-2"), None)
    assert missing.option_tradability is ScoreBand.LOW
    assert missing.combined_opportunity is ScoreBand.LOW
    assert missing.action_pool_eligible is False

    stale = service.analyze(_news(event_id="news-3"), _ibkr(observed_at=NOW - timedelta(minutes=6)))
    assert stale.option_tradability is ScoreBand.LOW

    boundary = service.analyze(
        _news(event_id="news-boundary"),
        _ibkr(observed_at=NOW - timedelta(seconds=5)),
    )
    just_stale = service.analyze(
        _news(event_id="news-just-stale"),
        _ibkr(observed_at=NOW - timedelta(seconds=5, microseconds=1)),
    )
    assert boundary.option_tradability is ScoreBand.HIGH
    assert just_stale.option_tradability is ScoreBand.LOW


def test_tradability_symbol_must_match_news_before_scoring_or_action_pool() -> None:
    service = NewsAnalysisService(now=lambda: NOW)

    mismatched = service.analyze(_news(), _ibkr(symbol="MSFT"))

    assert mismatched.tradability_data is None
    assert mismatched.option_tradability_score == Decimal("0")
    assert mismatched.combined_opportunity_score == Decimal("0")
    assert mismatched.action_pool_eligible is False


def test_numeric_scores_are_stable_and_sort_before_band_ties() -> None:
    service = NewsAnalysisService(now=lambda: NOW)
    lower = service.analyze(_news(event_id="lower"), _ibkr(volume=100, open_interest=500))
    higher = service.analyze(_news(event_id="higher"), _ibkr(volume=500, open_interest=2_000))

    assert lower.combined_opportunity is higher.combined_opportunity is ScoreBand.HIGH
    assert higher.combined_opportunity_score > lower.combined_opportunity_score
    assert service.pre_market_research_pool([lower, higher])[0].analysis_id == higher.analysis_id
    payload = service.analysis_payload(higher)
    assert payload["combined_opportunity_score"] == "92.65"


def test_models_reject_naive_timestamps_and_non_ibkr_hard_data() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _news(published_at=datetime(2026, 8, 3, 12, 0), first_seen_at=datetime(2026, 8, 3, 12, 1))
    with pytest.raises(ValueError, match="IBKR"):
        _ibkr(source="MODEL")


def test_supporting_conflicting_and_incomplete_news_never_enters_action_pool() -> None:
    service = NewsAnalysisService(now=lambda: NOW)
    supporting = service.analyze(
        _news(event_id="support", authority=NewsAuthority.SUPPORTING_ONLY), _ibkr()
    )
    conflicting = service.analyze(
        _news(event_id="conflict", conflicting_evidence_ids=("wire-2",)), _ibkr()
    )
    incomplete = service.analyze(
        _news(event_id="incomplete", is_complete=False), _ibkr()
    )

    assert not supporting.action_pool_eligible
    assert not conflicting.action_pool_eligible
    assert not incomplete.action_pool_eligible


def test_two_stage_confirmation_and_pools_mark_rank_one_without_approval() -> None:
    service = NewsAnalysisService(now=lambda: NOW)
    first = service.analyze(_news(event_id="1"), _ibkr())
    second = service.analyze(_news(event_id="2"), _ibkr(volume=120, open_interest=600))
    confirmation = MarketConfirmation(
        source="IBKR",
        observed_at=NOW + timedelta(minutes=5),
        direction=ImpactDirection.BULLISH,
        evidence_ids=("ibkr-tick-1",),
    )
    first = service.market_confirm(first, confirmation)
    second = service.market_confirm(second, confirmation)

    research = service.pre_market_research_pool([first] * 11)
    action = service.open_market_action_pool([second, first])
    assert len(research) == 10
    assert action[0].rank == 1 and action[0].rank_one
    assert all(item.approval_eligible is False for item in action)


def test_event_calendar_windows_are_timezone_aware_and_include_macro_fomc_and_earnings() -> None:
    calendar = EventCalendar(
        [
            CalendarEvent("fomc", EventCategory.FOMC, NOW + timedelta(days=2), "FOMC"),
            CalendarEvent("macro", EventCategory.MACRO, NOW + timedelta(days=8), "CPI"),
            CalendarEvent("earn", EventCategory.EARNINGS, NOW + timedelta(days=12), "AAPL earnings", symbols=("AAPL",)),
        ]
    )
    assert [item.event_id for item in calendar.events(CalendarWindow.THIS_WEEK, NOW)] == ["fomc"]
    assert [item.event_id for item in calendar.events(CalendarWindow.NEXT_WEEK, NOW)] == ["macro", "earn"]
    assert len(calendar.events(CalendarWindow.FUTURE_TWO_WEEKS, NOW)) == 3


def test_shadow_learning_freezes_prediction_and_never_auto_promotes() -> None:
    service = NewsAnalysisService(now=lambda: NOW)
    analysis = service.analyze(_news(), _ibkr())
    ledger = ShadowLearningLedger()
    record = ledger.freeze(analysis)
    updated = ledger.record_outcome(
        record.record_id,
        OutcomeObservation("ONE_DAY", NOW + timedelta(days=1), Decimal("0.023")),
    )

    assert updated.promotion is False
    assert updated.outcomes[0].observed_at.tzinfo is not None
    assert updated.as_dict()["frozen_prediction"]["analysis_id"] == analysis.analysis_id
