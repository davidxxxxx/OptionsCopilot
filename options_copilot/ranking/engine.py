"""Cost-after probability-weighted ranking with a fail-closed NO_TRADE result."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Iterable

from options_copilot.domain import StrategyCandidate
from options_copilot.risk import RiskAssessment, RiskEngine


ZERO = Decimal("0")


class RankingAction(str, Enum):
    TRADE = "TRADE"
    NO_TRADE = "NO_TRADE"


class RankingRejection(str, Enum):
    MISSING_PROBABILITY_DISTRIBUTION = "MISSING_PROBABILITY_DISTRIBUTION"
    NON_POSITIVE_EXPECTED_VALUE = "NON_POSITIVE_EXPECTED_VALUE"
    RISK_REJECTED = "RISK_REJECTED"


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    candidate: StrategyCandidate
    risk: RiskAssessment
    qualified: bool
    expected_value_before_costs: Decimal | None
    expected_value_after_costs: Decimal | None
    ranking_rejections: tuple[RankingRejection, ...]

    @property
    def total_estimated_costs(self) -> Decimal:
        return self.candidate.estimated_execution_costs


@dataclass(frozen=True, slots=True)
class RankingDecision:
    action: RankingAction
    selected_candidate: StrategyCandidate | None
    evaluations: tuple[CandidateEvaluation, ...]

    @property
    def selected_candidates(self) -> tuple[StrategyCandidate, ...]:
        """A tuple-shaped interface that can never contain more than one combo."""

        return () if self.selected_candidate is None else (self.selected_candidate,)


class CandidateRanker:
    def __init__(
        self,
        risk_engine: RiskEngine | None = None,
        *,
        minimum_expected_value_after_costs: Decimal = ZERO,
    ) -> None:
        if not isinstance(minimum_expected_value_after_costs, Decimal):
            raise TypeError("minimum_expected_value_after_costs must be a Decimal")
        if (
            not minimum_expected_value_after_costs.is_finite()
            or minimum_expected_value_after_costs < ZERO
        ):
            raise ValueError(
                "minimum_expected_value_after_costs must be finite and nonnegative"
            )
        self.risk_engine = risk_engine or RiskEngine()
        self.minimum_expected_value_after_costs = minimum_expected_value_after_costs

    def rank(
        self,
        candidates: Iterable[StrategyCandidate],
        *,
        account_equity: Decimal,
        open_combinations: int = 0,
    ) -> RankingDecision:
        candidate_tuple = tuple(candidates)
        if not all(isinstance(item, StrategyCandidate) for item in candidate_tuple):
            raise TypeError("candidates must contain only StrategyCandidate values")

        evaluations = tuple(
            self._evaluate(
                candidate,
                account_equity=account_equity,
                open_combinations=open_combinations,
            )
            for candidate in candidate_tuple
        )
        qualified = [item for item in evaluations if item.qualified]
        if not qualified:
            return RankingDecision(
                action=RankingAction.NO_TRADE,
                selected_candidate=None,
                evaluations=evaluations,
            )

        # Expected value wins; lower maximum loss and candidate id provide a
        # stable, reviewable tie break.  Exactly one combination is returned.
        qualified.sort(
            key=lambda item: (
                -_present_decimal(item.expected_value_after_costs),
                _present_decimal(item.risk.payoff.max_loss),
                item.candidate.candidate_id,
            )
        )
        selected = qualified[0].candidate
        return RankingDecision(
            action=RankingAction.TRADE,
            selected_candidate=selected,
            evaluations=evaluations,
        )

    def _evaluate(
        self,
        candidate: StrategyCandidate,
        *,
        account_equity: Decimal,
        open_combinations: int,
    ) -> CandidateEvaluation:
        risk = self.risk_engine.assess(
            candidate,
            account_equity=account_equity,
            open_combinations=open_combinations,
        )
        ranking_rejections: list[RankingRejection] = []
        if not risk.approved:
            ranking_rejections.append(RankingRejection.RISK_REJECTED)
        if not candidate.terminal_scenarios:
            ranking_rejections.append(
                RankingRejection.MISSING_PROBABILITY_DISTRIBUTION
            )

        before_costs: Decimal | None = None
        after_costs: Decimal | None = None
        if risk.payoff.calculable and candidate.terminal_scenarios:
            after_costs = sum(
                (
                    scenario.probability
                    * risk.payoff.pnl_at(scenario.terminal_underlying_price)
                    for scenario in candidate.terminal_scenarios
                ),
                ZERO,
            )
            before_costs = after_costs + candidate.estimated_execution_costs
            if after_costs <= self.minimum_expected_value_after_costs:
                ranking_rejections.append(
                    RankingRejection.NON_POSITIVE_EXPECTED_VALUE
                )
        return CandidateEvaluation(
            candidate=candidate,
            risk=risk,
            qualified=not ranking_rejections,
            expected_value_before_costs=before_costs,
            expected_value_after_costs=after_costs,
            ranking_rejections=tuple(ranking_rejections),
        )


def _present_decimal(value: Decimal | None) -> Decimal:
    if value is None:
        raise RuntimeError("qualified candidate is missing a ranking value")
    return value


def rank_candidates(
    candidates: Iterable[StrategyCandidate],
    *,
    account_equity: Decimal,
    open_combinations: int = 0,
) -> RankingDecision:
    return CandidateRanker().rank(
        candidates,
        account_equity=account_equity,
        open_combinations=open_combinations,
    )


__all__ = [
    "CandidateEvaluation",
    "CandidateRanker",
    "RankingAction",
    "RankingDecision",
    "RankingRejection",
    "rank_candidates",
]
