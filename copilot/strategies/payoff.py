"""Expiry payoff of a same-expiry option combination: max loss/profit and breakevens."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from copilot.market.models import OptionQuote


@dataclass(frozen=True)
class Leg:
    quote: OptionQuote
    quantity: int  # positive = long, negative = short; |quantity| is the ratio
    price: float  # estimated per-share fill

    @property
    def right(self) -> str:
        return self.quote.right

    @property
    def strike(self) -> float:
        return self.quote.strike


@dataclass(frozen=True)
class PayoffProfile:
    net_debit: float  # per share; negative means a net credit
    max_loss: float  # dollars per combination, positive
    max_profit: float | None  # dollars per combination; None = unlimited
    breakevens: tuple[float, ...]
    fees: float


def intrinsic(legs: Sequence[Leg], prices: np.ndarray) -> np.ndarray:
    total = np.zeros_like(prices, dtype=float)
    for leg in legs:
        if leg.right == "C":
            total += leg.quantity * np.maximum(prices - leg.strike, 0.0)
        else:
            total += leg.quantity * np.maximum(leg.strike - prices, 0.0)
    return total


def pnl(legs: Sequence[Leg], prices: np.ndarray, *, multiplier: int, fees: float) -> np.ndarray:
    """Dollar P&L per combination at expiry, after the entry debit/credit and fees."""

    net_debit = sum(leg.quantity * leg.price for leg in legs)
    return (intrinsic(legs, prices) - net_debit) * multiplier - fees


def analyze_payoff(
    legs: Sequence[Leg], *, multiplier: int, commission_per_contract: float
) -> PayoffProfile | None:
    """Exact piecewise-linear analysis; None if the loss is unbounded or data is incoherent."""

    if not legs or len({leg.quote.expiry for leg in legs}) != 1:
        return None
    contracts = sum(abs(leg.quantity) for leg in legs)
    fees = commission_per_contract * contracts * 2  # open and close
    net_debit = sum(leg.quantity * leg.price for leg in legs)
    knots = np.array(sorted({0.0, *(leg.strike for leg in legs)}), dtype=float)
    values = pnl(legs, knots, multiplier=multiplier, fees=fees)
    upper_slope = sum(leg.quantity for leg in legs if leg.right == "C")
    if upper_slope < 0:
        return None  # net short calls: unlimited upside loss
    max_loss = -float(values.min())
    max_profit: float | None = None if upper_slope > 0 else float(values.max())
    if max_loss <= 0.005:
        return None  # a riskless profit means stale or crossed quotes
    breakevens: list[float] = []
    for (x0, v0), (x1, v1) in zip(zip(knots, values), zip(knots[1:], values[1:])):
        if (v0 < 0 < v1) or (v0 > 0 > v1):
            breakevens.append(float(x0 + (x1 - x0) * (-v0) / (v1 - v0)))
    last_value = float(values[-1])
    if upper_slope > 0 and last_value < 0:
        breakevens.append(float(knots[-1] + (-last_value) / (upper_slope * multiplier)))
    return PayoffProfile(
        net_debit=net_debit,
        max_loss=max_loss,
        max_profit=max_profit,
        breakevens=tuple(round(value, 2) for value in breakevens),
        fees=fees,
    )


__all__ = ["Leg", "PayoffProfile", "analyze_payoff", "intrinsic", "pnl"]
