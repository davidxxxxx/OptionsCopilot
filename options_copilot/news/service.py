"""Read-only orchestration for classification, scoring, confirmation, and research pools."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Callable, Iterable

from .classifier import DeterministicNewsClassifier, NewsClassifier
from .models import (
    AnalyzedNews,
    AnalysisStage,
    MarketConfirmation,
    NewsInput,
    OptionTradabilityInput,
)
from .scoring import (
    combined_opportunity_score,
    event_impact_score,
    option_tradability_score,
    score_band,
)


class NewsAnalysisService:
    """Pure analysis service: no provider calls, broker calls, proposals, or approvals."""

    def __init__(self, *, classifier: NewsClassifier | None = None, now: Callable[[], datetime] | None = None) -> None:
        self._classifier = classifier or DeterministicNewsClassifier()
        self._now = now or (lambda: datetime.now(timezone.utc))

    def analyze(self, news: NewsInput, tradability_data: OptionTradabilityInput | None = None) -> AnalyzedNews:
        analyzed_at = self._checked_now()
        classification = self._classifier.classify(news)
        # A quote for another symbol (or only one member of a multi-symbol
        # event) cannot be used to score this event.  Drop it to the existing
        # zero-score path rather than raising or guessing a binding.
        if tradability_data is not None and news.symbols != (tradability_data.symbol,):
            tradability_data = None
        event_score = event_impact_score(news, classification)
        tradability_score = option_tradability_score(tradability_data, now=analyzed_at)
        combined_score = combined_opportunity_score(event_score, tradability_score)
        return AnalyzedNews(
            analysis_id=f"analysis:{news.event_id}:{analyzed_at.isoformat()}",
            news=news,
            classification=classification,
            analyzed_at=analyzed_at,
            stage=AnalysisStage.PROVISIONAL,
            event_impact=score_band(event_score),
            option_tradability=score_band(tradability_score),
            combined_opportunity=score_band(combined_score),
            event_impact_score=event_score,
            option_tradability_score=tradability_score,
            combined_opportunity_score=combined_score,
            tradability_data=tradability_data,
        )

    def market_confirm(self, analysis: AnalyzedNews, confirmation: MarketConfirmation) -> AnalyzedNews:
        """Record an observed IBKR confirmation; it does not submit or approve anything."""
        if confirmation.observed_at < analysis.news.published_at:
            raise ValueError("confirmation cannot precede news publication")
        return replace(
            analysis,
            stage=AnalysisStage.MARKET_CONFIRMED,
            market_confirmation=confirmation,
            rank=None,
            rank_one=False,
        )

    def pre_market_research_pool(self, analyses: Iterable[AnalyzedNews]) -> tuple[AnalyzedNews, ...]:
        """Return at most ten auditable research candidates, including provisional events."""
        return self._rank(analyses, limit=10, action_only=False)

    def open_market_action_pool(self, analyses: Iterable[AnalyzedNews]) -> tuple[AnalyzedNews, ...]:
        """Return at most three market-confirmed research items; this remains non-approving."""
        return self._rank(analyses, limit=3, action_only=True)

    @staticmethod
    def analysis_payload(analysis: AnalyzedNews) -> dict[str, object]:
        """Runtime/API-safe JSON data, explicitly recording that approval is unavailable."""
        payload = analysis.as_dict()
        payload["action_pool_eligible"] = analysis.action_pool_eligible
        payload["approval_eligible"] = False
        return payload

    def _rank(self, analyses: Iterable[AnalyzedNews], *, limit: int, action_only: bool) -> tuple[AnalyzedNews, ...]:
        eligible = [item for item in analyses if not action_only or item.action_pool_eligible]
        ordered = sorted(
            eligible,
            key=lambda item: (
                -item.combined_opportunity_score,
                -item.event_impact_score,
                -item.option_tradability_score,
                item.analyzed_at,
                item.analysis_id,
            ),
        )[:limit]
        return tuple(replace(item, rank=index, rank_one=index == 1) for index, item in enumerate(ordered, start=1))

    def _checked_now(self) -> datetime:
        now = self._now()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("news analysis clock must return timezone-aware datetime")
        return now
