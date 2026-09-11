"""Closed finite-risk strategy templates.

Templates accept only conId-bound legs.  They intentionally do not parse a
human/LLM description of a position: identity and quotes are resolved later
from authoritative broker evidence by :mod:`options_copilot.strategies.generator`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Iterable

from options_copilot.domain import OptionLeg, OptionRight, PositionSide


class StrategyKind(str, Enum):
    DEBIT_VERTICAL = "DEBIT_VERTICAL"
    CREDIT_VERTICAL = "CREDIT_VERTICAL"
    CALENDAR = "CALENDAR"
    DIAGONAL = "DIAGONAL"
    BUTTERFLY = "BUTTERFLY"
    IRON_CONDOR = "IRON_CONDOR"
    LONG_OPTION = "LONG_OPTION"


class TemplateValidationError(ValueError):
    """A requested combination is outside the permanently closed registry."""


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be a nonblank string")
    return value.strip()


@dataclass(frozen=True, slots=True)
class TemplateLeg:
    """An integer-ratio leg before resolving its authoritative contract."""

    con_id: int
    side: PositionSide
    ratio: int = 1

    def __post_init__(self) -> None:
        if isinstance(self.con_id, bool) or not isinstance(self.con_id, int) or self.con_id <= 0:
            raise ValueError("con_id must be a positive integer")
        if not isinstance(self.side, PositionSide):
            try:
                object.__setattr__(self, "side", PositionSide(str(self.side).upper()))
            except ValueError as exc:
                raise ValueError("side must be LONG or SHORT") from exc
        if isinstance(self.ratio, bool) or not isinstance(self.ratio, int) or self.ratio <= 0:
            raise ValueError("ratio must be a positive integer")


@dataclass(frozen=True, slots=True)
class ExitPlan:
    """All entry candidates must bind every deterministic exit decision."""

    thesis_invalidation: str
    risk_stop: str
    profit_take: str
    time_stop: str
    maximum_holding_date: date
    bad_quote_action: str

    def __post_init__(self) -> None:
        for field in (
            "thesis_invalidation",
            "risk_stop",
            "profit_take",
            "time_stop",
            "bad_quote_action",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if not isinstance(self.maximum_holding_date, date):
            raise TypeError("maximum_holding_date must be a date")

    def as_dict(self) -> dict[str, str]:
        return {
            "thesis_invalidation": self.thesis_invalidation,
            "risk_stop": self.risk_stop,
            "profit_take": self.profit_take,
            "time_stop": self.time_stop,
            "maximum_holding_date": self.maximum_holding_date.isoformat(),
            "bad_quote_action": self.bad_quote_action,
        }


@dataclass(frozen=True, slots=True)
class StrategyTemplate:
    kind: StrategyKind
    finite_risk_only: bool = True

    @property
    def scenario_applicability(self) -> tuple[str, ...]:
        """Closed scenario labels consumed by the deterministic ranking layer."""

        if self.kind in {StrategyKind.DEBIT_VERTICAL, StrategyKind.LONG_OPTION}:
            return ("DIRECTIONAL", "EVENT_OR_LONG_VOLATILITY")
        if self.kind in {StrategyKind.BUTTERFLY, StrategyKind.IRON_CONDOR}:
            return ("RANGE",)
        return ("EVENT_OR_LONG_VOLATILITY", "RANGE")

    @property
    def assignment_exercise_dividend_flags(self) -> dict[str, bool]:
        """Conservative flags; no template assumes exercise or assignment."""

        has_short = self.kind is not StrategyKind.LONG_OPTION
        return {
            "short_leg_assignment_possible": has_short,
            "early_exercise_review_required": has_short,
            "ex_dividend_calendar_required": has_short,
            "planned_exercise_allowed": False,
        }

    def validate(self, legs: Iterable[OptionLeg]) -> tuple[OptionLeg, ...]:
        values = tuple(legs)
        if not values or not all(isinstance(leg, OptionLeg) for leg in values):
            raise TemplateValidationError("template legs must be resolved OptionLeg values")
        contracts = tuple(leg.contract for leg in values)
        if any(contract.broker_contract_id is None for contract in contracts):
            raise TemplateValidationError("every template leg requires a broker conId")
        if len({contract.broker_contract_id for contract in contracts}) != len(contracts):
            raise TemplateValidationError("duplicate conId is not a valid strategy leg set")
        if len({contract.underlying for contract in contracts}) != 1:
            raise TemplateValidationError("all template legs must share an underlying")
        if len({contract.currency for contract in contracts}) != 1:
            raise TemplateValidationError("all template legs must share a currency")
        if any(contract.multiplier != 100 for contract in contracts):
            raise TemplateValidationError("only standard 100-share option contracts are eligible")

        if self.kind is StrategyKind.LONG_OPTION:
            self._long_option(values)
        elif self.kind in {StrategyKind.DEBIT_VERTICAL, StrategyKind.CREDIT_VERTICAL}:
            self._vertical(values)
        elif self.kind is StrategyKind.BUTTERFLY:
            self._butterfly(values)
        elif self.kind is StrategyKind.IRON_CONDOR:
            self._iron_condor(values)
        elif self.kind is StrategyKind.CALENDAR:
            self._calendar(values, diagonal=False)
        elif self.kind is StrategyKind.DIAGONAL:
            self._calendar(values, diagonal=True)
        else:  # pragma: no cover - enum exhaustiveness guard
            raise TemplateValidationError("unregistered strategy kind")
        return values

    @staticmethod
    def _same_expiration(legs: tuple[OptionLeg, ...]) -> None:
        if len({leg.contract.expiration for leg in legs}) != 1:
            raise TemplateValidationError("template requires one expiration")

    @staticmethod
    def _same_right(legs: tuple[OptionLeg, ...]) -> OptionRight:
        rights = {leg.contract.right for leg in legs}
        if len(rights) != 1:
            raise TemplateValidationError("template requires a single option right")
        return next(iter(rights))

    @staticmethod
    def _long_option(legs: tuple[OptionLeg, ...]) -> None:
        if len(legs) != 1 or legs[0].side is not PositionSide.LONG or legs[0].quantity != 1:
            raise TemplateValidationError("LONG_OPTION requires exactly one long contract")

    def _vertical(self, legs: tuple[OptionLeg, ...]) -> None:
        if len(legs) != 2 or {leg.side for leg in legs} != {PositionSide.LONG, PositionSide.SHORT}:
            raise TemplateValidationError("vertical requires one long and one short")
        self._same_expiration(legs)
        right = self._same_right(legs)
        long_leg = next(leg for leg in legs if leg.side is PositionSide.LONG)
        short_leg = next(leg for leg in legs if leg.side is PositionSide.SHORT)
        if long_leg.quantity != short_leg.quantity:
            raise TemplateValidationError("vertical short ratio must be fully covered")
        long_strike, short_strike = long_leg.contract.strike, short_leg.contract.strike
        debit = (right is OptionRight.CALL and long_strike < short_strike) or (
            right is OptionRight.PUT and long_strike > short_strike
        )
        credit = (right is OptionRight.CALL and short_strike < long_strike) or (
            right is OptionRight.PUT and short_strike > long_strike
        )
        if self.kind is StrategyKind.DEBIT_VERTICAL and not debit:
            raise TemplateValidationError("DEBIT_VERTICAL strike ordering is invalid")
        if self.kind is StrategyKind.CREDIT_VERTICAL and not credit:
            raise TemplateValidationError("CREDIT_VERTICAL strike ordering is invalid")

    def _butterfly(self, legs: tuple[OptionLeg, ...]) -> None:
        if len(legs) != 3:
            raise TemplateValidationError("BUTTERFLY requires three legs")
        self._same_expiration(legs)
        self._same_right(legs)
        ordered = tuple(sorted(legs, key=lambda leg: leg.contract.strike))
        if not (
            ordered[0].side is PositionSide.LONG
            and ordered[1].side is PositionSide.SHORT
            and ordered[2].side is PositionSide.LONG
            and ordered[0].quantity == ordered[2].quantity
            and ordered[1].quantity == ordered[0].quantity * 2
        ):
            raise TemplateValidationError("BUTTERFLY requires 1:-2:1 covered integer ratios")
        if ordered[1].contract.strike - ordered[0].contract.strike != ordered[2].contract.strike - ordered[1].contract.strike:
            raise TemplateValidationError("BUTTERFLY wings must be equally wide")

    def _iron_condor(self, legs: tuple[OptionLeg, ...]) -> None:
        if len(legs) != 4:
            raise TemplateValidationError("IRON_CONDOR requires four legs")
        self._same_expiration(legs)
        puts = tuple(sorted((leg for leg in legs if leg.contract.right is OptionRight.PUT), key=lambda leg: leg.contract.strike))
        calls = tuple(sorted((leg for leg in legs if leg.contract.right is OptionRight.CALL), key=lambda leg: leg.contract.strike))
        if len(puts) != 2 or len(calls) != 2:
            raise TemplateValidationError("IRON_CONDOR requires two puts and two calls")
        valid_put = puts[0].side is PositionSide.LONG and puts[1].side is PositionSide.SHORT and puts[0].quantity == puts[1].quantity
        valid_call = calls[0].side is PositionSide.SHORT and calls[1].side is PositionSide.LONG and calls[0].quantity == calls[1].quantity
        if not (valid_put and valid_call and puts[1].contract.strike < calls[0].contract.strike):
            raise TemplateValidationError("IRON_CONDOR must have fully covered non-overlapping short wings")

    def _calendar(self, legs: tuple[OptionLeg, ...], *, diagonal: bool) -> None:
        if len(legs) != 2 or {leg.side for leg in legs} != {PositionSide.LONG, PositionSide.SHORT}:
            raise TemplateValidationError("calendar/diagonal requires one long and one short")
        self._same_right(legs)
        long_leg = next(leg for leg in legs if leg.side is PositionSide.LONG)
        short_leg = next(leg for leg in legs if leg.side is PositionSide.SHORT)
        if long_leg.quantity != short_leg.quantity or long_leg.contract.expiration <= short_leg.contract.expiration:
            raise TemplateValidationError("calendar/diagonal short ratio must be covered by a later long")
        same_strike = long_leg.contract.strike == short_leg.contract.strike
        if diagonal == same_strike:
            label = "DIAGONAL" if diagonal else "CALENDAR"
            raise TemplateValidationError(f"{label} strike relationship is invalid")


class StrategyTemplateRegistry:
    """The only supported source of strategy structures."""

    def __init__(self, templates: Iterable[StrategyTemplate] | None = None) -> None:
        values = tuple(templates) if templates is not None else tuple(StrategyTemplate(kind) for kind in StrategyKind)
        if len({template.kind for template in values}) != len(values):
            raise ValueError("duplicate strategy template")
        if {template.kind for template in values} != set(StrategyKind):
            raise ValueError("registry must contain every approved finite-risk template")
        self._templates = {template.kind: template for template in values}

    @property
    def kinds(self) -> tuple[StrategyKind, ...]:
        return tuple(StrategyKind)

    @property
    def registered_templates(self) -> tuple[StrategyTemplate, ...]:
        return tuple(self._templates[kind] for kind in StrategyKind)

    def get(self, kind: StrategyKind | str) -> StrategyTemplate:
        try:
            normalized = kind if isinstance(kind, StrategyKind) else StrategyKind(str(kind).strip().upper())
        except ValueError as exc:
            raise TemplateValidationError("unregistered or free-text strategy") from exc
        return self._templates[normalized]

    def validate(self, kind: StrategyKind | str, legs: Iterable[OptionLeg]) -> tuple[OptionLeg, ...]:
        return self.get(kind).validate(legs)


__all__ = [
    "ExitPlan",
    "StrategyKind",
    "StrategyTemplate",
    "StrategyTemplateRegistry",
    "TemplateLeg",
    "TemplateValidationError",
]
