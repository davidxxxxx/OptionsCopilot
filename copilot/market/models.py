"""Market data records produced by the IBKR adapter (or test fakes)."""
from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import date, datetime


def valid_price(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and value > 0


@dataclass(frozen=True)
class OptionQuote:
    symbol: str
    expiry: date
    strike: float
    right: str  # "C" or "P"
    con_id: int = 0
    multiplier: int = 100
    trading_class: str = ""
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    volume: int | None = None
    open_interest: int | None = None
    quote_time: datetime | None = None
    market_data_type: int | None = None
    model_price: float | None = None
    indicative: bool = False  # bid/ask synthesised from a reference price (market closed)
    source: str = "ibkr"  # "ibkr" or "cboe"

    @property
    def mid(self) -> float | None:
        if valid_price(self.bid) and valid_price(self.ask) and self.ask >= self.bid:
            return (self.bid + self.ask) / 2
        return None

    @property
    def spread(self) -> float | None:
        if self.mid is None:
            return None
        return self.ask - self.bid

    @property
    def label(self) -> str:
        return f"{self.expiry:%m/%d} {self.strike:g}{self.right}"


@dataclass(frozen=True)
class Underlying:
    symbol: str
    con_id: int | None = None
    price: float | None = None
    bid: float | None = None
    ask: float | None = None
    close: float | None = None
    change_pct: float | None = None
    market_data_type: int | None = None
    quote_time: datetime | None = None
    iv30: float | None = None
    iv_rank: float | None = None
    iv_percentile: float | None = None
    hv20: float | None = None
    ret_5d: float | None = None
    ret_20d: float | None = None

    def with_history(self, **values: float | None) -> "Underlying":
        return replace(self, **values)


@dataclass(frozen=True)
class ChainInfo:
    symbol: str
    trading_class: str
    multiplier: int
    exchange: str
    expiries: tuple[date, ...]
    strikes: tuple[float, ...]


@dataclass(frozen=True)
class OptionChain:
    underlying: Underlying
    quotes: tuple[OptionQuote, ...]

    def expiries(self) -> list[date]:
        return sorted({quote.expiry for quote in self.quotes})

    def for_expiry(self, expiry: date) -> list[OptionQuote]:
        return [quote for quote in self.quotes if quote.expiry == expiry]


@dataclass(frozen=True)
class Position:
    symbol: str
    sec_type: str
    quantity: float
    avg_cost: float
    multiplier: int = 1
    expiry: date | None = None
    strike: float | None = None
    right: str | None = None
    con_id: int = 0

    @property
    def label(self) -> str:
        if self.sec_type == "OPT" and self.expiry and self.strike is not None:
            return f"{self.symbol} {self.expiry:%Y-%m-%d} {self.strike:g}{self.right}"
        return self.symbol


@dataclass(frozen=True)
class AccountSnapshot:
    observed_at: datetime
    net_liquidation: float | None = None
    currency: str = "USD"
    positions: tuple[Position, ...] = field(default_factory=tuple)

    @property
    def held_symbols(self) -> set[str]:
        return {position.symbol for position in self.positions if position.quantity}


__all__ = [
    "AccountSnapshot",
    "ChainInfo",
    "OptionChain",
    "OptionQuote",
    "Position",
    "Underlying",
    "valid_price",
]
