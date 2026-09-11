"""Deterministic G035 equity thesis scoring."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.canonical import utc_datetime

from .models import (
    DirectionLabel,
    EquityPoolInput,
    EquityScore,
    FactorKind,
    FactorStatus,
)


FACTOR_WEIGHTS: Mapping[FactorKind, Decimal] = MappingProxyType(
    {
        FactorKind.NEWS: Decimal("0.30"),
        FactorKind.FUNDAMENTALS: Decimal("0.20"),
        FactorKind.REGIME: Decimal("0.15"),
        FactorKind.TREND_VOLATILITY: Decimal("0.25"),
        FactorKind.POSITIONING: Decimal("0.10"),
    }
)
SCORING_VERSION = "equity-scoring.v1"
LIQUIDITY_MAX_AGE = timedelta(minutes=15)
CONFLICT_THRESHOLD = Decimal("0.20")
MINIMUM_COVERAGE = Decimal("0.35")
MAXIMUM_UNCERTAINTY = Decimal("0.65")
DIRECTION_THRESHOLD = Decimal("15")
SCORING_POLICY_BODY = {
        "version": SCORING_VERSION,
        "weights": {kind.value: value for kind, value in FACTOR_WEIGHTS.items()},
        "direction": "100*sum(weight*signed_signal*confidence)",
        "coverage": "sum(weight*confidence)",
        "conflict": "2*min(positive_evidence_mass,negative_evidence_mass)",
        "uncertainty": "clamp(1-coverage_confidence+conflict_penalty,0,1)",
        "opportunity": "clamp(.75*abs(direction_score)+.25*liquidity-25*uncertainty,0,100)",
        "factor_timing": "observed_at<=as_of and effective_at<=as_of and (valid_until is null or valid_until>=as_of)",
        "liquidity_max_age_seconds": int(LIQUIDITY_MAX_AGE.total_seconds()),
        "conflict_threshold": CONFLICT_THRESHOLD,
        "minimum_coverage": MINIMUM_COVERAGE,
        "maximum_uncertainty": MAXIMUM_UNCERTAINTY,
        "direction_threshold": DIRECTION_THRESHOLD,
        "algorithm": "PIT_FILTER_THEN_WEIGHTED_DIRECTION_AND_LIQUIDITY_V2",
}
SCORING_HASH = canonical_hash(SCORING_POLICY_BODY)


def score_equity(item: EquityPoolInput, *, as_of: datetime | None = None) -> EquityScore:
    """Score canonical evidence; missing values remain absent, never numeric zero."""

    if not isinstance(item, EquityPoolInput):
        raise TypeError("item must be an EquityPoolInput")
    frozen_as_of = utc_datetime(as_of or item.captured_at, field="as_of")
    signed_mass = Decimal("0")
    coverage_confidence = Decimal("0")
    positive_mass = Decimal("0")
    negative_mass = Decimal("0")
    for evidence in item.factors:
        if evidence.status is not FactorStatus.AVAILABLE:
            continue
        if evidence.observed_at > frozen_as_of or evidence.effective_at > frozen_as_of:
            continue
        if evidence.valid_until is not None and evidence.valid_until < frozen_as_of:
            continue
        if evidence.signed_signal is None or evidence.confidence is None:
            raise ValueError("AVAILABLE factor has missing numeric evidence")
        weighted = (
            FACTOR_WEIGHTS[evidence.factor]
            * evidence.signed_signal
            * evidence.confidence
        )
        signed_mass += weighted
        coverage_confidence += FACTOR_WEIGHTS[evidence.factor] * evidence.confidence
        if weighted > 0:
            positive_mass += weighted
        elif weighted < 0:
            negative_mass += -weighted

    direction_score = Decimal("100") * signed_mass
    conflict_penalty = Decimal("2") * min(positive_mass, negative_mass)
    uncertainty = _clamp(
        Decimal("1") - coverage_confidence + conflict_penalty,
        Decimal("0"),
        Decimal("1"),
    )
    liquidity_score = None
    if (
        item.liquidity.status is FactorStatus.AVAILABLE
        and item.liquidity.observed_at <= frozen_as_of
        and frozen_as_of - item.liquidity.observed_at <= LIQUIDITY_MAX_AGE
    ):
        liquidity_score = item.liquidity.score
    opportunity_score = None
    if liquidity_score is not None:
        opportunity_score = _clamp(
            Decimal("0.75") * abs(direction_score)
            + Decimal("0.25") * liquidity_score
            - Decimal("25") * uncertainty,
            Decimal("0"),
            Decimal("100"),
        )

    if conflict_penalty >= CONFLICT_THRESHOLD:
        direction_label = DirectionLabel.MIXED
    elif coverage_confidence < MINIMUM_COVERAGE or uncertainty > MAXIMUM_UNCERTAINTY:
        direction_label = DirectionLabel.UNCERTAIN
    elif direction_score >= DIRECTION_THRESHOLD:
        direction_label = DirectionLabel.BULLISH
    elif direction_score <= -DIRECTION_THRESHOLD:
        direction_label = DirectionLabel.BEARISH
    else:
        direction_label = DirectionLabel.NEUTRAL

    return EquityScore(
        symbol=item.symbol,
        direction_score=direction_score,
        coverage_confidence=coverage_confidence,
        positive_evidence_mass=positive_mass,
        negative_evidence_mass=negative_mass,
        conflict_penalty=conflict_penalty,
        uncertainty=uncertainty,
        liquidity_score=liquidity_score,
        opportunity_score=opportunity_score,
        direction_label=direction_label,
    )


def _clamp(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal:
    return min(max(value, minimum), maximum)


__all__ = [
    "FACTOR_WEIGHTS",
    "LIQUIDITY_MAX_AGE",
    "SCORING_HASH",
    "SCORING_POLICY_BODY",
    "SCORING_VERSION",
    "score_equity",
]
