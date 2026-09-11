"""Frozen prediction and shadow-learning records; automated promotion is prohibited."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any

from .models import AnalyzedNews, AuditJson


def _aware(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


@dataclass(frozen=True, slots=True)
class FrozenPrediction(AuditJson):
    analysis_id: str
    frozen_at: datetime
    event_id: str
    classification: dict[str, Any]
    scores: dict[str, str]
    stage: str

    def __post_init__(self) -> None:
        if not isinstance(self.analysis_id, str) or not self.analysis_id.strip():
            raise ValueError("analysis_id cannot be blank")
        _aware(self.frozen_at, "frozen_at")
        if not isinstance(self.event_id, str) or not self.event_id.strip():
            raise ValueError("event_id cannot be blank")
        if not self.classification or not self.scores:
            raise ValueError("frozen prediction must include classification and scores")

    @classmethod
    def from_analysis(cls, analysis: AnalyzedNews) -> "FrozenPrediction":
        return cls(
            analysis_id=analysis.analysis_id,
            frozen_at=analysis.analyzed_at,
            event_id=analysis.news.event_id,
            classification=analysis.classification.as_dict(),
            scores={
                "event_impact": analysis.event_impact.value,
                "option_tradability": analysis.option_tradability.value,
                "combined_opportunity": analysis.combined_opportunity.value,
            },
            stage=analysis.stage.value,
        )


@dataclass(frozen=True, slots=True)
class OutcomeObservation(AuditJson):
    window: str
    observed_at: datetime
    underlying_return: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.window, str) or not self.window.strip():
            raise ValueError("window cannot be blank")
        _aware(self.observed_at, "observed_at")
        if not isinstance(self.underlying_return, Decimal) or not self.underlying_return.is_finite():
            raise ValueError("underlying_return must be a finite Decimal")


@dataclass(frozen=True, slots=True)
class ShadowLearningRecord(AuditJson):
    record_id: str
    frozen_prediction: FrozenPrediction
    outcomes: tuple[OutcomeObservation, ...] = ()
    promotion: bool = False

    def __post_init__(self) -> None:
        if self.promotion is not False:
            raise ValueError("automatic model promotion is prohibited")
        if len({item.window for item in self.outcomes}) != len(self.outcomes):
            raise ValueError("only one outcome per time window is allowed")
        if any(item.observed_at < self.frozen_prediction.frozen_at for item in self.outcomes):
            raise ValueError("outcome cannot precede frozen prediction")


class ShadowLearningLedger:
    """In-memory record keeper for a human-reviewed shadow-learning pipeline."""

    def __init__(self) -> None:
        self._records: dict[str, ShadowLearningRecord] = {}

    def freeze(self, analysis: AnalyzedNews) -> ShadowLearningRecord:
        record_id = f"shadow:{analysis.analysis_id}"
        if record_id in self._records:
            return self._records[record_id]
        record = ShadowLearningRecord(record_id=record_id, frozen_prediction=FrozenPrediction.from_analysis(analysis))
        self._records[record_id] = record
        return record

    def record_outcome(self, record_id: str, outcome: OutcomeObservation) -> ShadowLearningRecord:
        try:
            record = self._records[record_id]
        except KeyError as exc:
            raise KeyError(f"unknown shadow record: {record_id}") from exc
        if outcome.window in {item.window for item in record.outcomes}:
            raise ValueError(f"outcome window already recorded: {outcome.window}")
        updated = replace(record, outcomes=record.outcomes + (outcome,))
        self._records[record_id] = updated
        return updated

    def records(self) -> tuple[ShadowLearningRecord, ...]:
        return tuple(self._records.values())
