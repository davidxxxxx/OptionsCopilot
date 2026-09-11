"""Exact Decimal piecewise-linear expiration payoff analysis."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, DecimalException, localcontext
from enum import Enum

from options_copilot.domain import (
    OptionRight,
    PositionSide,
    StrategyCandidate,
)


ZERO = Decimal("0")


class PayoffStatus(str, Enum):
    CALCULATED = "CALCULATED"
    REJECTED = "REJECTED"


class RiskRejection(str, Enum):
    MISSING_EXECUTABLE_QUOTE = "MISSING_EXECUTABLE_QUOTE"
    MIXED_CURRENCY = "MIXED_CURRENCY"
    MIXED_EXPIRATION = "MIXED_EXPIRATION"
    MIXED_UNDERLYING = "MIXED_UNDERLYING"
    NAKED_SHORT_CALL = "NAKED_SHORT_CALL"
    NAKED_SHORT_PUT = "NAKED_SHORT_PUT"
    UNBOUNDED_MAX_LOSS = "UNBOUNDED_MAX_LOSS"
    UNKNOWN_MAX_LOSS = "UNKNOWN_MAX_LOSS"
    INVALID_ACCOUNT_EQUITY = "INVALID_ACCOUNT_EQUITY"
    NORMAL_RISK_LIMIT_EXCEEDED = "NORMAL_RISK_LIMIT_EXCEEDED"
    VALIDATED_RISK_LIMIT_EXCEEDED = "VALIDATED_RISK_LIMIT_EXCEEDED"
    HARD_RISK_LIMIT_REACHED = "HARD_RISK_LIMIT_REACHED"
    MAX_OPEN_COMBINATIONS = "MAX_OPEN_COMBINATIONS"


class PayoffUnavailableError(RuntimeError):
    pass


def _checked_decimal(value: object, name: str, *, nonnegative: bool = False) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if nonnegative and value < ZERO:
        raise ValueError(f"{name} cannot be negative")
    return value


@dataclass(frozen=True, slots=True)
class PayoffSegment:
    """``slope * price + intercept`` over a closed/open price interval."""

    lower_bound: Decimal
    upper_bound: Decimal | None
    slope: Decimal
    intercept: Decimal

    def __post_init__(self) -> None:
        _checked_decimal(self.lower_bound, "lower_bound", nonnegative=True)
        _checked_decimal(self.slope, "slope")
        _checked_decimal(self.intercept, "intercept")
        if self.upper_bound is not None:
            _checked_decimal(self.upper_bound, "upper_bound", nonnegative=True)
            if self.upper_bound <= self.lower_bound:
                raise ValueError("upper_bound must exceed lower_bound")

    def pnl_at(self, underlying_price: Decimal) -> Decimal:
        price = _checked_decimal(
            underlying_price,
            "underlying_price",
            nonnegative=True,
        )
        if price < self.lower_bound or (
            self.upper_bound is not None and price > self.upper_bound
        ):
            raise ValueError("underlying price is outside this payoff segment")
        return self.slope * price + self.intercept


@dataclass(frozen=True, slots=True)
class BreakEvenRegion:
    """A point (equal bounds), finite interval, or unbounded zero-P&L region."""

    lower_bound: Decimal
    upper_bound: Decimal | None

    def __post_init__(self) -> None:
        _checked_decimal(self.lower_bound, "lower_bound", nonnegative=True)
        if self.upper_bound is not None:
            _checked_decimal(self.upper_bound, "upper_bound", nonnegative=True)
            if self.upper_bound < self.lower_bound:
                raise ValueError("break-even upper bound cannot be below lower bound")

    @property
    def is_point(self) -> bool:
        return self.upper_bound == self.lower_bound


@dataclass(frozen=True, slots=True)
class ExpirationPayoff:
    candidate_id: str
    status: PayoffStatus
    segments: tuple[PayoffSegment, ...]
    net_opening_cashflow: Decimal | None
    estimated_execution_costs: Decimal
    max_loss: Decimal | None
    max_profit: Decimal | None
    unbounded_profit: bool
    breakevens: tuple[Decimal, ...]
    break_even_regions: tuple[BreakEvenRegion, ...]
    rejections: tuple[RiskRejection, ...]

    @property
    def calculable(self) -> bool:
        return self.status is PayoffStatus.CALCULATED

    @property
    def opening_cashflow_before_costs(self) -> Decimal | None:
        if self.net_opening_cashflow is None:
            return None
        return self.net_opening_cashflow + self.estimated_execution_costs

    def pnl_at(self, terminal_underlying_price: Decimal) -> Decimal:
        price = _checked_decimal(
            terminal_underlying_price,
            "terminal_underlying_price",
            nonnegative=True,
        )
        if not self.calculable:
            codes = ", ".join(item.value for item in self.rejections)
            raise PayoffUnavailableError(f"expiration payoff was rejected: {codes}")
        for segment in self.segments:
            if price >= segment.lower_bound and (
                segment.upper_bound is None or price <= segment.upper_bound
            ):
                return segment.slope * price + segment.intercept
        raise PayoffUnavailableError("no payoff segment covers the terminal price")


def _ordered_rejections(values: set[RiskRejection]) -> tuple[RiskRejection, ...]:
    return tuple(sorted(values, key=lambda item: item.value))


def _rejected(
    candidate: StrategyCandidate,
    rejections: set[RiskRejection],
) -> ExpirationPayoff:
    if not rejections:
        rejections.add(RiskRejection.UNKNOWN_MAX_LOSS)
    return ExpirationPayoff(
        candidate_id=candidate.candidate_id,
        status=PayoffStatus.REJECTED,
        segments=(),
        net_opening_cashflow=None,
        estimated_execution_costs=candidate.estimated_execution_costs,
        max_loss=None,
        max_profit=None,
        unbounded_profit=False,
        breakevens=(),
        break_even_regions=(),
        rejections=_ordered_rejections(rejections),
    )


def _weighted_exposure(candidate: StrategyCandidate, right: OptionRight) -> tuple[Decimal, Decimal]:
    long_exposure = ZERO
    short_exposure = ZERO
    for quoted_leg in candidate.leg_quotes:
        leg = quoted_leg.leg
        if leg.contract.right is not right:
            continue
        exposure = Decimal(leg.quantity) * leg.contract.multiplier
        if leg.side is PositionSide.LONG:
            long_exposure += exposure
        else:
            short_exposure += exposure
    return long_exposure, short_exposure


def _structural_rejections(candidate: StrategyCandidate) -> set[RiskRejection]:
    rejections: set[RiskRejection] = set()
    contracts = tuple(item.contract for item in candidate.leg_quotes)
    if len({item.underlying for item in contracts}) != 1:
        rejections.add(RiskRejection.MIXED_UNDERLYING)
    if len({item.expiration for item in contracts}) != 1:
        # A later-expiring leg still has time value at the nearer expiry, so a
        # terminal intrinsic-only calculation would understate or overstate risk.
        rejections.add(RiskRejection.MIXED_EXPIRATION)
    if len({item.currency for item in contracts}) != 1:
        rejections.add(RiskRejection.MIXED_CURRENCY)
    if any(item.executable_price is None for item in candidate.leg_quotes):
        rejections.add(RiskRejection.MISSING_EXECUTABLE_QUOTE)

    long_calls, short_calls = _weighted_exposure(candidate, OptionRight.CALL)
    long_puts, short_puts = _weighted_exposure(candidate, OptionRight.PUT)
    if short_calls > long_calls:
        rejections.add(RiskRejection.NAKED_SHORT_CALL)
        rejections.add(RiskRejection.UNBOUNDED_MAX_LOSS)
    if short_puts > long_puts:
        # A short put has a finite mathematical loss because spot cannot go
        # below zero, but remains a prohibited naked short under portfolio policy.
        rejections.add(RiskRejection.NAKED_SHORT_PUT)
    return rejections


def _opening_cashflow(candidate: StrategyCandidate) -> Decimal:
    cashflow = -candidate.estimated_execution_costs
    for quoted_leg in candidate.leg_quotes:
        price = quoted_leg.executable_price
        if price is None:
            raise ValueError("missing executable quote")
        leg = quoted_leg.leg
        signed_exposure = (
            Decimal(leg.side.payoff_sign)
            * Decimal(leg.quantity)
            * leg.contract.multiplier
        )
        # Long premium is paid; short premium is received.
        cashflow -= signed_exposure * price
    return cashflow


def _segment_at(
    candidate: StrategyCandidate,
    lower_bound: Decimal,
    upper_bound: Decimal | None,
    opening_cashflow: Decimal,
) -> PayoffSegment:
    slope = ZERO
    intercept = opening_cashflow
    for quoted_leg in candidate.leg_quotes:
        leg = quoted_leg.leg
        contract = leg.contract
        exposure = (
            Decimal(leg.side.payoff_sign)
            * Decimal(leg.quantity)
            * contract.multiplier
        )
        if contract.right is OptionRight.CALL and contract.strike <= lower_bound:
            slope += exposure
            intercept -= exposure * contract.strike
        elif contract.right is OptionRight.PUT and contract.strike > lower_bound:
            slope -= exposure
            intercept += exposure * contract.strike
    return PayoffSegment(
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        slope=slope,
        intercept=intercept,
    )


def _break_even_regions(segments: tuple[PayoffSegment, ...]) -> tuple[BreakEvenRegion, ...]:
    raw: list[BreakEvenRegion] = []
    with localcontext() as context:
        context.prec = 50
        for segment in segments:
            pnl_at_lower = segment.slope * segment.lower_bound + segment.intercept
            if segment.slope == ZERO:
                if pnl_at_lower == ZERO:
                    raw.append(
                        BreakEvenRegion(segment.lower_bound, segment.upper_bound)
                    )
                continue
            root = -segment.intercept / segment.slope
            if root < segment.lower_bound:
                continue
            if segment.upper_bound is not None and root > segment.upper_bound:
                continue
            raw.append(BreakEvenRegion(root, root))

    if not raw:
        return ()
    raw.sort(key=lambda item: item.lower_bound)
    merged: list[BreakEvenRegion] = []
    for region in raw:
        if not merged:
            merged.append(region)
            continue
        previous = merged[-1]
        if previous.upper_bound is None:
            continue
        if region.lower_bound <= previous.upper_bound:
            if region.upper_bound is None:
                upper_bound = None
            else:
                upper_bound = max(previous.upper_bound, region.upper_bound)
            merged[-1] = BreakEvenRegion(previous.lower_bound, upper_bound)
        else:
            merged.append(region)
    return tuple(merged)


def analyze_expiration_payoff(candidate: StrategyCandidate) -> ExpirationPayoff:
    """Return exact intrinsic expiration geometry or a fail-closed rejection.

    A strategy may have unlimited *profit* (for example, a long call), but its
    maximum loss must be finite and exactly computable.  Naked option shorts are
    rejected even when a naked put's loss is mathematically finite.
    """

    if not isinstance(candidate, StrategyCandidate):
        raise TypeError("candidate must be a StrategyCandidate")
    rejections = _structural_rejections(candidate)
    if rejections:
        return _rejected(candidate, rejections)

    try:
        opening_cashflow = _opening_cashflow(candidate)
        breakpoints = sorted(
            {ZERO, *(item.contract.strike for item in candidate.leg_quotes)}
        )
        segments = tuple(
            _segment_at(
                candidate,
                lower_bound,
                breakpoints[index + 1] if index + 1 < len(breakpoints) else None,
                opening_cashflow,
            )
            for index, lower_bound in enumerate(breakpoints)
        )
        tail_slope = segments[-1].slope
        if tail_slope < ZERO:
            return _rejected(candidate, {RiskRejection.UNBOUNDED_MAX_LOSS})

        values_at_breakpoints = tuple(
            segment.slope * segment.lower_bound + segment.intercept
            for segment in segments
        )
        minimum_pnl = min(values_at_breakpoints)
        maximum_pnl = max(values_at_breakpoints)
        max_loss = max(ZERO, -minimum_pnl)
        unbounded_profit = tail_slope > ZERO
        max_profit = None if unbounded_profit else maximum_pnl
        regions = _break_even_regions(segments)
        point_breakevens = tuple(
            region.lower_bound for region in regions if region.is_point
        )
    except (ArithmeticError, DecimalException, ValueError, TypeError):
        return _rejected(candidate, {RiskRejection.UNKNOWN_MAX_LOSS})

    if not max_loss.is_finite() or (
        max_profit is not None and not max_profit.is_finite()
    ):
        return _rejected(candidate, {RiskRejection.UNKNOWN_MAX_LOSS})
    return ExpirationPayoff(
        candidate_id=candidate.candidate_id,
        status=PayoffStatus.CALCULATED,
        segments=segments,
        net_opening_cashflow=opening_cashflow,
        estimated_execution_costs=candidate.estimated_execution_costs,
        max_loss=max_loss,
        max_profit=max_profit,
        unbounded_profit=unbounded_profit,
        breakevens=point_breakevens,
        break_even_regions=regions,
        rejections=(),
    )


__all__ = [
    "BreakEvenRegion",
    "ExpirationPayoff",
    "PayoffSegment",
    "PayoffStatus",
    "PayoffUnavailableError",
    "RiskRejection",
    "analyze_expiration_payoff",
]
