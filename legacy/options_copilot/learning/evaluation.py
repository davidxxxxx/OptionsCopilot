"""Cost-adjusted, authority-free Champion/Challenger shadow evaluation."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, localcontext
from enum import Enum
import re
from typing import ClassVar

from options_copilot.storage.canonical import canonical_hash, datetime_text, utc_datetime


MINIMUM_DISCOVERY_SAMPLES = 30
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
ZERO = Decimal("0")
ONE = Decimal("1")


class LearningEvaluationStage(str, Enum):
    COLLECTING = "COLLECTING"
    DISCOVERY = "DISCOVERY"


def _decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, Decimal):
        raise TypeError(f"{field} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{field} must be finite")
    return value


def _probability(value: object, *, field: str) -> Decimal:
    result = _decimal(value, field=field)
    if result < ZERO or result > ONE:
        raise ValueError(f"{field} must be between 0 and 1")
    return result


def _digest(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase 64-character hash")
    return value


def _identity(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _VERSION_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is invalid")
    return value


@dataclass(frozen=True, slots=True)
class ShadowPrediction:
    prediction_id: str
    challenger_version: str
    scenario_id: str
    probability: Decimal
    expected_value_usd: Decimal
    produced_at: datetime
    prediction_hash: str
    record_kind: str = "shadow_prediction"
    shadow_only: bool = True
    can_promote: bool = False
    can_change_risk: bool = False
    can_change_ranking: bool = False
    can_create_instruction: bool = False

    def __post_init__(self) -> None:
        _identity(self.prediction_id, field="prediction_id")
        _identity(self.challenger_version, field="challenger_version")
        _identity(self.scenario_id, field="scenario_id")
        _probability(self.probability, field="probability")
        _decimal(self.expected_value_usd, field="expected_value_usd")
        object.__setattr__(self, "produced_at", utc_datetime(self.produced_at, field="produced_at"))
        expected = canonical_hash(self.hash_payload())
        if _digest(self.prediction_hash, field="prediction_hash") != expected:
            raise ValueError("shadow prediction hash mismatch")

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.learning.shadow_prediction.v1",
            "record_kind": self.record_kind,
            "prediction_id": self.prediction_id,
            "challenger_version": self.challenger_version,
            "scenario_id": self.scenario_id,
            "probability": self.probability,
            "expected_value_usd": self.expected_value_usd,
            "produced_at": datetime_text(self.produced_at),
            "shadow_only": True,
            "authority": {
                "promotion": False,
                "risk": False,
                "ranking": False,
                "creator": False,
            },
        }


@dataclass(frozen=True, slots=True)
class EvaluationSample:
    scenario_id: str
    independence_key: str
    scenario_bucket: str
    outcome: Decimal
    champion_probability: Decimal | None
    challenger_probability: Decimal
    challenger_expected_value_usd: Decimal
    actual_gross_pnl_usd: Decimal
    execution_cost_usd: Decimal
    covered: bool
    economic_time: datetime
    champion_expected_value_usd: Decimal | None = None

    def __post_init__(self) -> None:
        _identity(self.scenario_id, field="scenario_id")
        _identity(self.independence_key, field="independence_key")
        _identity(self.scenario_bucket, field="scenario_bucket")
        _probability(self.outcome, field="outcome")
        if self.champion_probability is not None:
            _probability(self.champion_probability, field="champion_probability")
        _probability(self.challenger_probability, field="challenger_probability")
        _decimal(self.challenger_expected_value_usd, field="challenger_expected_value_usd")
        if self.champion_expected_value_usd is not None:
            _decimal(self.champion_expected_value_usd, field="champion_expected_value_usd")
        _decimal(self.actual_gross_pnl_usd, field="actual_gross_pnl_usd")
        cost = _decimal(self.execution_cost_usd, field="execution_cost_usd")
        if cost < ZERO:
            raise ValueError("execution_cost_usd cannot be negative")
        if not isinstance(self.covered, bool):
            raise TypeError("covered must be a boolean")
        object.__setattr__(
            self,
            "economic_time",
            utc_datetime(self.economic_time, field="economic_time"),
        )


@dataclass(frozen=True, slots=True)
class ScenarioEvaluation:
    scenario_bucket: str
    sample_count: int
    cost_adjusted_expected_value_usd: Decimal
    brier_score: Decimal
    actual_after_cost_pnl_usd: Decimal
    champion_cost_adjusted_expected_value_usd: Decimal | None
    champion_brier_score: Decimal | None
    challenger_cost_adjusted_ev_delta_usd: Decimal | None
    challenger_brier_improvement: Decimal | None


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    generated_at: datetime
    stage: LearningEvaluationStage
    independent_count: int
    total_sample_count: int
    cost_adjusted_expected_value_usd: Decimal
    brier_score: Decimal
    calibration_error: Decimal
    champion_cost_adjusted_expected_value_usd: Decimal | None
    champion_brier_score: Decimal | None
    champion_calibration_error: Decimal | None
    challenger_cost_adjusted_ev_delta_usd: Decimal | None
    challenger_brier_improvement: Decimal | None
    comparison_complete: bool
    promotion_blocked_reasons: tuple[str, ...]
    coverage: Decimal
    maximum_drawdown_usd: Decimal
    tail_loss_usd: Decimal
    scenario_breakdown: tuple[ScenarioEvaluation, ...]
    reference_dataset_hash: str
    initial_champion_policy_hash: str
    challenger_version: str
    challenger_hash: str
    execution_cost_hash: str
    independence_hash: str
    report_hash: str
    shadow_only: ClassVar[bool] = True
    can_promote: ClassVar[bool] = False
    can_change_risk: ClassVar[bool] = False
    can_change_ranking: ClassVar[bool] = False
    can_create_instruction: ClassVar[bool] = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "generated_at", utc_datetime(self.generated_at, field="generated_at"))
        if not isinstance(self.stage, LearningEvaluationStage):
            raise TypeError("stage must be LearningEvaluationStage")
        if self.independent_count < 0 or self.total_sample_count <= 0:
            raise ValueError("evaluation counts are invalid")
        for field in (
            "cost_adjusted_expected_value_usd", "brier_score", "calibration_error",
            "coverage", "maximum_drawdown_usd", "tail_loss_usd",
        ):
            _decimal(getattr(self, field), field=field)
        for field in (
            "champion_cost_adjusted_expected_value_usd",
            "champion_brier_score",
            "champion_calibration_error",
            "challenger_cost_adjusted_ev_delta_usd",
            "challenger_brier_improvement",
        ):
            value = getattr(self, field)
            if value is not None:
                _decimal(value, field=field)
        if not isinstance(self.comparison_complete, bool):
            raise TypeError("comparison_complete must be a boolean")
        if (
            self.shadow_only is not True
            or self.can_promote is not False
            or self.can_change_risk is not False
            or self.can_change_ranking is not False
            or self.can_create_instruction is not False
        ):
            raise ValueError("evaluation authority flags are immutable and shadow-only")
        object.__setattr__(
            self,
            "promotion_blocked_reasons",
            tuple(sorted(set(self.promotion_blocked_reasons))),
        )
        for field in (
            "reference_dataset_hash", "initial_champion_policy_hash", "challenger_hash",
            "execution_cost_hash", "independence_hash",
        ):
            _digest(getattr(self, field), field=field)
        _identity(self.challenger_version, field="challenger_version")
        expected = canonical_hash(self.hash_payload())
        if _digest(self.report_hash, field="report_hash") != expected:
            raise ValueError("evaluation report hash mismatch")

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.learning.evaluation_report.v1",
            "generated_at": datetime_text(self.generated_at),
            "stage": self.stage.value,
            "independent_count": self.independent_count,
            "total_sample_count": self.total_sample_count,
            "cost_adjusted_expected_value_usd": self.cost_adjusted_expected_value_usd,
            "brier_score": self.brier_score,
            "calibration_error": self.calibration_error,
            "champion_cost_adjusted_expected_value_usd": self.champion_cost_adjusted_expected_value_usd,
            "champion_brier_score": self.champion_brier_score,
            "champion_calibration_error": self.champion_calibration_error,
            "challenger_cost_adjusted_ev_delta_usd": self.challenger_cost_adjusted_ev_delta_usd,
            "challenger_brier_improvement": self.challenger_brier_improvement,
            "comparison_complete": self.comparison_complete,
            "promotion_blocked_reasons": self.promotion_blocked_reasons,
            "coverage": self.coverage,
            "maximum_drawdown_usd": self.maximum_drawdown_usd,
            "tail_loss_usd": self.tail_loss_usd,
            "scenario_breakdown": [
                {
                    "scenario_bucket": row.scenario_bucket,
                    "sample_count": row.sample_count,
                    "cost_adjusted_expected_value_usd": row.cost_adjusted_expected_value_usd,
                    "brier_score": row.brier_score,
                    "actual_after_cost_pnl_usd": row.actual_after_cost_pnl_usd,
                    "champion_cost_adjusted_expected_value_usd": row.champion_cost_adjusted_expected_value_usd,
                    "champion_brier_score": row.champion_brier_score,
                    "challenger_cost_adjusted_ev_delta_usd": row.challenger_cost_adjusted_ev_delta_usd,
                    "challenger_brier_improvement": row.challenger_brier_improvement,
                }
                for row in self.scenario_breakdown
            ],
            "bindings": {
                "reference_dataset_hash": self.reference_dataset_hash,
                "initial_champion_policy_hash": self.initial_champion_policy_hash,
                "challenger_version": self.challenger_version,
                "challenger_hash": self.challenger_hash,
                "execution_cost_hash": self.execution_cost_hash,
                "independence_hash": self.independence_hash,
            },
            "authority": {
                "shadow_only": True,
                "promotion": False,
                "risk": False,
                "ranking": False,
                "creator": False,
            },
        }

    def to_dict(self) -> Mapping[str, object]:
        return {**self.hash_payload(), "report_hash": self.report_hash}


def build_shadow_prediction(
    *,
    prediction_id: str,
    challenger_version: str,
    scenario_id: str,
    probability: Decimal,
    expected_value_usd: Decimal,
    produced_at: datetime,
) -> ShadowPrediction:
    values = {
        "prediction_id": prediction_id,
        "challenger_version": challenger_version,
        "scenario_id": scenario_id,
        "probability": probability,
        "expected_value_usd": expected_value_usd,
        "produced_at": produced_at,
    }
    provisional_payload = {
        "schema": "options_copilot.learning.shadow_prediction.v1",
        "record_kind": "shadow_prediction",
        "prediction_id": prediction_id,
        "challenger_version": challenger_version,
        "scenario_id": scenario_id,
        "probability": probability,
        "expected_value_usd": expected_value_usd,
        "produced_at": datetime_text(utc_datetime(produced_at, field="produced_at")),
        "shadow_only": True,
        "authority": {"promotion": False, "risk": False, "ranking": False, "creator": False},
    }
    return ShadowPrediction(**values, prediction_hash=canonical_hash(provisional_payload))


def build_evaluation_report(
    *,
    samples: Sequence[EvaluationSample],
    generated_at: datetime,
    reference_dataset_hash: str,
    initial_champion_policy_hash: str,
    challenger_version: str,
    challenger_hash: str,
    execution_cost_hash: str,
    independence_hash: str,
) -> EvaluationReport:
    if isinstance(samples, (str, bytes, bytearray)) or not isinstance(samples, Sequence) or not samples:
        raise ValueError("evaluation requires at least one sample")
    if any(not isinstance(sample, EvaluationSample) for sample in samples):
        raise TypeError("samples must contain EvaluationSample values")
    ordered_samples = tuple(
        sorted(samples, key=lambda sample: (sample.economic_time, sample.scenario_id))
    )
    independence_keys: set[str] = set()
    seen_scenarios: set[str] = set()
    for sample in ordered_samples:
        if sample.scenario_id in seen_scenarios:
            raise ValueError("scenario_id values must be unique")
        if sample.independence_key in independence_keys:
            raise ValueError("independence_key values must be unique")
        seen_scenarios.add(sample.scenario_id)
        independence_keys.add(sample.independence_key)
    independent = ordered_samples
    count = len(independent)
    stage = LearningEvaluationStage.DISCOVERY if count >= MINIMUM_DISCOVERY_SAMPLES else LearningEvaluationStage.COLLECTING
    net_expected = tuple(sample.challenger_expected_value_usd - sample.execution_cost_usd for sample in independent)
    net_actual = tuple(sample.actual_gross_pnl_usd - sample.execution_cost_usd for sample in independent)
    briers = tuple((sample.challenger_probability - sample.outcome) ** 2 for sample in independent)
    champion_probabilities = tuple(
        sample.champion_probability
        for sample in independent
        if sample.champion_probability is not None
    )
    champion_expected_values = tuple(
        sample.champion_expected_value_usd
        for sample in independent
        if sample.champion_expected_value_usd is not None
    )
    champion_probability_complete = len(champion_probabilities) == count
    champion_expected_complete = len(champion_expected_values) == count
    if champion_probability_complete:
        champion_brier = _mean(
            tuple(
                (sample.champion_probability - sample.outcome) ** 2
                for sample in independent
                if sample.champion_probability is not None
            )
        )
        champion_calibration = abs(
            _mean(champion_probabilities)
            - _mean(tuple(sample.outcome for sample in independent))
        )
    else:
        champion_brier = None
        champion_calibration = None
    champion_cost_adjusted_ev = (
        _mean(
            tuple(
                expected - sample.execution_cost_usd
                for expected, sample in zip(champion_expected_values, independent)
            )
        )
        if champion_expected_complete
        else None
    )
    challenger_ev = _mean(net_expected)
    challenger_brier = _mean(briers)
    ev_delta = (
        challenger_ev - champion_cost_adjusted_ev
        if champion_cost_adjusted_ev is not None
        else None
    )
    brier_improvement = (
        champion_brier - challenger_brier
        if champion_brier is not None
        else None
    )
    blocked: list[str] = []
    if not champion_probability_complete:
        blocked.append("CHAMPION_PROBABILITY_BASELINE_MISSING")
    if not champion_expected_complete:
        blocked.append("CHAMPION_EXPECTED_VALUE_BASELINE_MISSING")
    if ev_delta is not None and ev_delta <= ZERO:
        blocked.append("CHALLENGER_NOT_BETTER_AFTER_COST")
    if brier_improvement is not None and brier_improvement <= ZERO:
        blocked.append("CHALLENGER_BRIER_NOT_BETTER")
    comparison_complete = champion_probability_complete and champion_expected_complete
    report_values: dict[str, object] = {
        "generated_at": utc_datetime(generated_at, field="generated_at"),
        "stage": stage,
        "independent_count": count,
        "total_sample_count": len(samples),
        "cost_adjusted_expected_value_usd": challenger_ev,
        "brier_score": challenger_brier,
        "calibration_error": abs(_mean(tuple(sample.challenger_probability for sample in independent)) - _mean(tuple(sample.outcome for sample in independent))),
        "champion_cost_adjusted_expected_value_usd": champion_cost_adjusted_ev,
        "champion_brier_score": champion_brier,
        "champion_calibration_error": champion_calibration,
        "challenger_cost_adjusted_ev_delta_usd": ev_delta,
        "challenger_brier_improvement": brier_improvement,
        "comparison_complete": comparison_complete,
        "promotion_blocked_reasons": tuple(sorted(blocked)),
        "coverage": _mean(tuple(ONE if sample.covered else ZERO for sample in independent)),
        "maximum_drawdown_usd": _maximum_drawdown(net_actual),
        "tail_loss_usd": max(ZERO, -min(net_actual)),
        "scenario_breakdown": _breakdown(independent),
        "reference_dataset_hash": _digest(reference_dataset_hash, field="reference_dataset_hash"),
        "initial_champion_policy_hash": _digest(initial_champion_policy_hash, field="initial_champion_policy_hash"),
        "challenger_version": _identity(challenger_version, field="challenger_version"),
        "challenger_hash": _digest(challenger_hash, field="challenger_hash"),
        "execution_cost_hash": _digest(execution_cost_hash, field="execution_cost_hash"),
        "independence_hash": _digest(independence_hash, field="independence_hash"),
    }
    report_hash = canonical_hash(_evaluation_payload(report_values))
    return EvaluationReport(**report_values, report_hash=report_hash)


def _evaluation_payload(values: Mapping[str, object]) -> dict[str, object]:
    rows = values["scenario_breakdown"]
    assert isinstance(rows, tuple)
    stage = values["stage"]
    assert isinstance(stage, LearningEvaluationStage)
    generated_at = values["generated_at"]
    assert isinstance(generated_at, datetime)
    return {
        "schema": "options_copilot.learning.evaluation_report.v1",
        "generated_at": datetime_text(generated_at),
        "stage": stage.value,
        "independent_count": values["independent_count"],
        "total_sample_count": values["total_sample_count"],
        "cost_adjusted_expected_value_usd": values["cost_adjusted_expected_value_usd"],
        "brier_score": values["brier_score"],
        "calibration_error": values["calibration_error"],
        "champion_cost_adjusted_expected_value_usd": values["champion_cost_adjusted_expected_value_usd"],
        "champion_brier_score": values["champion_brier_score"],
        "champion_calibration_error": values["champion_calibration_error"],
        "challenger_cost_adjusted_ev_delta_usd": values["challenger_cost_adjusted_ev_delta_usd"],
        "challenger_brier_improvement": values["challenger_brier_improvement"],
        "comparison_complete": values["comparison_complete"],
        "promotion_blocked_reasons": values["promotion_blocked_reasons"],
        "coverage": values["coverage"],
        "maximum_drawdown_usd": values["maximum_drawdown_usd"],
        "tail_loss_usd": values["tail_loss_usd"],
        "scenario_breakdown": [
            {
                "scenario_bucket": row.scenario_bucket,
                "sample_count": row.sample_count,
                "cost_adjusted_expected_value_usd": row.cost_adjusted_expected_value_usd,
                "brier_score": row.brier_score,
                "actual_after_cost_pnl_usd": row.actual_after_cost_pnl_usd,
                "champion_cost_adjusted_expected_value_usd": row.champion_cost_adjusted_expected_value_usd,
                "champion_brier_score": row.champion_brier_score,
                "challenger_cost_adjusted_ev_delta_usd": row.challenger_cost_adjusted_ev_delta_usd,
                "challenger_brier_improvement": row.challenger_brier_improvement,
            }
            for row in rows
        ],
        "bindings": {
            "reference_dataset_hash": values["reference_dataset_hash"],
            "initial_champion_policy_hash": values["initial_champion_policy_hash"],
            "challenger_version": values["challenger_version"],
            "challenger_hash": values["challenger_hash"],
            "execution_cost_hash": values["execution_cost_hash"],
            "independence_hash": values["independence_hash"],
        },
        "authority": {
            "shadow_only": True,
            "promotion": False,
            "risk": False,
            "ranking": False,
            "creator": False,
        },
    }


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    with localcontext() as context:
        context.prec = 40
        return sum(values, ZERO) / Decimal(len(values))


def _maximum_drawdown(values: tuple[Decimal, ...]) -> Decimal:
    cumulative = ZERO
    peak = ZERO
    maximum = ZERO
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        maximum = max(maximum, peak - cumulative)
    return maximum


def _breakdown(samples: tuple[EvaluationSample, ...]) -> tuple[ScenarioEvaluation, ...]:
    buckets: dict[str, list[EvaluationSample]] = {}
    for sample in samples:
        buckets.setdefault(sample.scenario_bucket, []).append(sample)
    return tuple(
        _bucket_evaluation(bucket, tuple(rows))
        for bucket, rows in sorted(buckets.items())
    )


def _bucket_evaluation(
    bucket: str,
    rows: tuple[EvaluationSample, ...],
) -> ScenarioEvaluation:
    challenger_ev = _mean(
        tuple(row.challenger_expected_value_usd - row.execution_cost_usd for row in rows)
    )
    challenger_brier = _mean(
        tuple((row.challenger_probability - row.outcome) ** 2 for row in rows)
    )
    complete_ev = all(row.champion_expected_value_usd is not None for row in rows)
    complete_probability = all(row.champion_probability is not None for row in rows)
    champion_ev = (
        _mean(
            tuple(
                row.champion_expected_value_usd - row.execution_cost_usd
                for row in rows
                if row.champion_expected_value_usd is not None
            )
        )
        if complete_ev
        else None
    )
    champion_brier = (
        _mean(
            tuple(
                (row.champion_probability - row.outcome) ** 2
                for row in rows
                if row.champion_probability is not None
            )
        )
        if complete_probability
        else None
    )
    return ScenarioEvaluation(
        scenario_bucket=bucket,
        sample_count=len(rows),
        cost_adjusted_expected_value_usd=challenger_ev,
        brier_score=challenger_brier,
        actual_after_cost_pnl_usd=sum(
            (row.actual_gross_pnl_usd - row.execution_cost_usd for row in rows),
            ZERO,
        ),
        champion_cost_adjusted_expected_value_usd=champion_ev,
        champion_brier_score=champion_brier,
        challenger_cost_adjusted_ev_delta_usd=(
            challenger_ev - champion_ev if champion_ev is not None else None
        ),
        challenger_brier_improvement=(
            champion_brier - challenger_brier
            if champion_brier is not None
            else None
        ),
    )


__all__ = [
    "EvaluationReport", "EvaluationSample", "LearningEvaluationStage",
    "MINIMUM_DISCOVERY_SAMPLES", "ScenarioEvaluation", "ShadowPrediction",
    "build_evaluation_report", "build_shadow_prediction",
]
