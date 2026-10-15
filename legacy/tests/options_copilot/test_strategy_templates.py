from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from options_copilot.domain import OptionContract, OptionLeg, OptionRight, PositionSide
from options_copilot.strategies import StrategyKind, StrategyTemplateRegistry, TemplateValidationError


EXPIRY = date(2026, 8, 21)
LATER_EXPIRY = date(2026, 9, 18)


def _leg(con_id: int, strike: str, right: OptionRight, side: PositionSide, *, expiry: date = EXPIRY, quantity: int = 1) -> OptionLeg:
    return OptionLeg(OptionContract(str(con_id), "SPY", expiry, Decimal(strike), right, broker_contract_id=con_id), side, quantity)


@pytest.mark.parametrize(
    ("kind", "legs"),
    [
        (StrategyKind.LONG_OPTION, (_leg(1, "100", OptionRight.CALL, PositionSide.LONG),)),
        (StrategyKind.DEBIT_VERTICAL, (_leg(1, "100", OptionRight.CALL, PositionSide.LONG), _leg(2, "105", OptionRight.CALL, PositionSide.SHORT))),
        (StrategyKind.CREDIT_VERTICAL, (_leg(1, "100", OptionRight.CALL, PositionSide.SHORT), _leg(2, "105", OptionRight.CALL, PositionSide.LONG))),
        (StrategyKind.BUTTERFLY, (_leg(1, "95", OptionRight.CALL, PositionSide.LONG), _leg(2, "100", OptionRight.CALL, PositionSide.SHORT, quantity=2), _leg(3, "105", OptionRight.CALL, PositionSide.LONG))),
        (StrategyKind.IRON_CONDOR, (_leg(1, "90", OptionRight.PUT, PositionSide.LONG), _leg(2, "95", OptionRight.PUT, PositionSide.SHORT), _leg(3, "105", OptionRight.CALL, PositionSide.SHORT), _leg(4, "110", OptionRight.CALL, PositionSide.LONG))),
        (StrategyKind.CALENDAR, (_leg(1, "100", OptionRight.CALL, PositionSide.SHORT), _leg(2, "100", OptionRight.CALL, PositionSide.LONG, expiry=LATER_EXPIRY))),
        (StrategyKind.DIAGONAL, (_leg(1, "100", OptionRight.CALL, PositionSide.SHORT), _leg(2, "105", OptionRight.CALL, PositionSide.LONG, expiry=LATER_EXPIRY))),
    ],
)
def test_every_registered_template_accepts_its_closed_canonical_shape(kind: StrategyKind, legs: tuple[OptionLeg, ...]) -> None:
    assert StrategyTemplateRegistry().validate(kind, legs) == legs


def test_uncovered_ratio_short_is_permanently_rejected() -> None:
    legs = (_leg(1, "100", OptionRight.CALL, PositionSide.LONG), _leg(2, "105", OptionRight.CALL, PositionSide.SHORT, quantity=2))
    with pytest.raises(TemplateValidationError, match="fully covered"):
        StrategyTemplateRegistry().validate(StrategyKind.DEBIT_VERTICAL, legs)


def test_free_text_structure_never_enters_registry() -> None:
    with pytest.raises(TemplateValidationError, match="unregistered"):
        StrategyTemplateRegistry().get("sell whatever looks good")
