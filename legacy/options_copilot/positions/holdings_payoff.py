"""Exact intrinsic geometry and explicit liquidation-based holding economics.

This pure model neither groups observed holdings into a strategy nor models an
atomic fill, broker margin, assignment, exercise, or intervening settlement.
Current liquidation cashflow and a single future exit reserve are explicit
inputs. Historical entry cost and hypothetical reopening premiums are absent.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, Inexact, ROUND_HALF_EVEN, Rounded, localcontext
from fractions import Fraction
import re

from options_copilot.storage.canonical import canonical_hash


ZERO = Decimal("0")
SCHEMA = "options_copilot.holdings_payoff.v1"
VALUATION_BASIS = "GROSS_COMPONENT_NATURAL_LIQUIDATION"
PAYOFF_MODEL = "SAME_EXPIRY_INTRINSIC_STANDARD_100"
_PRECISION = 512
_ROOT_DISPLAY_PRECISION = 50


class HoldingsPayoffError(ValueError):
    """A bounded holding-payoff contract or coverage rejection."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _require(condition: bool, reason: str = "HOLDINGS_PAYOFF_INVALID") -> None:
    if not condition:
        raise HoldingsPayoffError(reason)


def _decimal(value: object, *, nonnegative: bool = False) -> Decimal:
    _require(isinstance(value, Decimal) and value.is_finite(), "HOLDINGS_PAYOFF_DECIMAL_INVALID")
    _require(len(value.as_tuple().digits) <= 128 and -128 <= value.as_tuple().exponent <= 128,
             "HOLDINGS_PAYOFF_DECIMAL_BOUND_EXCEEDED")
    _require(not nonnegative or value >= ZERO, "HOLDINGS_PAYOFF_NEGATIVE_INPUT")
    return value


def _text(value: Decimal) -> str:
    # Decimal.normalize() depends on the caller's decimal context and can
    # silently round hash inputs. Fixed-point formatting preserves every digit.
    if value == ZERO:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


@dataclass(frozen=True, slots=True)
class HoldingPayoffLeg:
    contract_id: int
    symbol: str
    expiration: date
    strike: Decimal
    right: str
    signed_quantity: int
    multiplier: int = 100
    currency: str = "USD"

    def __post_init__(self) -> None:
        _require(type(self.contract_id) is int and 1 <= self.contract_id <= 2**63 - 1,
                 "HOLDINGS_PAYOFF_CONTRACT_ID_INVALID")
        _require(isinstance(self.symbol, str) and re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,15}", self.symbol) is not None,
                 "HOLDINGS_PAYOFF_SYMBOL_INVALID")
        _require(isinstance(self.expiration, date) and not isinstance(self.expiration, datetime),
                 "HOLDINGS_PAYOFF_EXPIRATION_INVALID")
        _require(_decimal(self.strike) > ZERO, "HOLDINGS_PAYOFF_STRIKE_INVALID")
        _require(self.right in ("C", "P") and isinstance(self.right, str), "HOLDINGS_PAYOFF_RIGHT_INVALID")
        _require(type(self.signed_quantity) is int and 1 <= abs(self.signed_quantity) <= 2**31 - 1,
                 "HOLDINGS_PAYOFF_QUANTITY_INVALID")
        _require(type(self.multiplier) is int and self.multiplier == 100,
                 "HOLDINGS_PAYOFF_MULTIPLIER_UNSUPPORTED")
        _require(self.currency == "USD", "HOLDINGS_PAYOFF_CURRENCY_UNSUPPORTED")

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id, "symbol": self.symbol,
            "expiration": self.expiration.isoformat(), "strike": _text(self.strike),
            "right": self.right, "signed_quantity": self.signed_quantity,
            "multiplier": self.multiplier, "currency": self.currency,
        }


@dataclass(frozen=True, slots=True)
class HoldingsPayoffSegment:
    """Cost-adjusted holding P/L and intrinsic value over the same interval."""

    lower_bound: Decimal
    upper_bound: Decimal | None
    slope: Decimal
    intercept: Decimal
    intrinsic_intercept: Decimal

    def as_dict(self) -> dict[str, object]:
        return {
            "lower_bound": _text(self.lower_bound),
            "upper_bound": None if self.upper_bound is None else _text(self.upper_bound),
            "slope": _text(self.slope), "intercept": _text(self.intercept),
            "intrinsic_intercept": _text(self.intrinsic_intercept),
        }


@dataclass(frozen=True, slots=True)
class HoldingsBreakEvenRegion:
    """Exact rational bounds; equal bounds are a point, None is an open tail."""

    lower_bound: Fraction
    upper_bound: Fraction | None

    @property
    def is_point(self) -> bool:
        return self.upper_bound == self.lower_bound

    def as_dict(self) -> dict[str, object]:
        def bound(value: Fraction) -> dict[str, str]:
            return {"numerator": str(value.numerator), "denominator": str(value.denominator)}

        return {
            "lower_bound": bound(self.lower_bound),
            "upper_bound": None if self.upper_bound is None else bound(self.upper_bound),
        }


def _regions(segments: tuple[HoldingsPayoffSegment, ...]) -> tuple[HoldingsBreakEvenRegion, ...]:
    raw: list[HoldingsBreakEvenRegion] = []
    for segment in segments:
        lower = Fraction(segment.lower_bound)
        upper = None if segment.upper_bound is None else Fraction(segment.upper_bound)
        if segment.slope == ZERO:
            if segment.intercept == ZERO:
                raw.append(HoldingsBreakEvenRegion(lower, upper))
            continue
        root = -Fraction(segment.intercept) / Fraction(segment.slope)
        if root >= lower and (upper is None or root <= upper):
            raw.append(HoldingsBreakEvenRegion(root, root))
    raw.sort(key=lambda row: row.lower_bound)
    merged: list[HoldingsBreakEvenRegion] = []
    for row in raw:
        if not merged or (merged[-1].upper_bound is not None and row.lower_bound > merged[-1].upper_bound):
            merged.append(row)
        elif merged[-1].upper_bound is not None:
            previous = merged.pop()
            upper = None if row.upper_bound is None else max(previous.upper_bound, row.upper_bound)
            merged.append(HoldingsBreakEvenRegion(previous.lower_bound, upper))
    return tuple(merged)


def _root_display(root: Fraction) -> tuple[Decimal, bool]:
    denominator = root.denominator
    for factor in (2, 5):
        while denominator % factor == 0:
            denominator //= factor
    exact = denominator == 1
    with localcontext() as context:
        context.prec = _PRECISION if exact else _ROOT_DISPLAY_PRECISION
        context.rounding = ROUND_HALF_EVEN
        context.traps[Inexact] = False
        context.traps[Rounded] = False
        return Decimal(root.numerator) / Decimal(root.denominator), exact


@dataclass(frozen=True, slots=True)
class HoldingsPayoff:
    """Derived fields are calculated from original legs and one explicit basis."""

    legs: tuple[HoldingPayoffLeg, ...]
    gross_liquidation_cashflow_usd: Decimal
    estimated_future_exit_cost_usd: Decimal
    min_intrinsic_usd: Decimal = field(init=False)
    max_intrinsic_usd: Decimal | None = field(init=False)
    gross_holding_max_loss_usd: Decimal = field(init=False)
    max_loss_usd: Decimal = field(init=False)
    max_profit_usd: Decimal | None = field(init=False)
    unbounded_profit: bool = field(init=False)
    segments: tuple[HoldingsPayoffSegment, ...] = field(init=False)
    breakevens: tuple[Decimal, ...] = field(init=False)
    breakevens_exact: bool = field(init=False)
    break_even_regions: tuple[HoldingsBreakEvenRegion, ...] = field(init=False)
    geometry_hash: str = field(init=False)
    payoff_hash: str = field(init=False)
    schema: str = field(default=SCHEMA, init=False)
    valuation_basis: str = field(default=VALUATION_BASIS, init=False)
    model: str = field(default=PAYOFF_MODEL, init=False)

    def __post_init__(self) -> None:
        _require(isinstance(self.legs, (tuple, list)) and 1 <= len(self.legs) <= 8,
                 "HOLDINGS_PAYOFF_LEG_BOUND_EXCEEDED")
        _require(all(isinstance(leg, HoldingPayoffLeg) for leg in self.legs), "HOLDINGS_PAYOFF_LEG_INVALID")
        legs = tuple(sorted(self.legs, key=lambda leg: leg.contract_id))
        _require(len({leg.contract_id for leg in legs}) == len(legs), "HOLDINGS_PAYOFF_DUPLICATE_CONTRACT")
        _require(len({leg.symbol for leg in legs}) == 1, "HOLDINGS_PAYOFF_MIXED_UNDERLYING")
        _require(len({leg.expiration for leg in legs}) == 1, "HOLDINGS_PAYOFF_MIXED_EXPIRATION")
        for right, code in (("C", "HOLDINGS_PAYOFF_NAKED_SHORT_CALL"), ("P", "HOLDINGS_PAYOFF_NAKED_SHORT_PUT")):
            _require(sum(leg.signed_quantity * leg.multiplier for leg in legs if leg.right == right) >= 0, code)
        baseline = _decimal(self.gross_liquidation_cashflow_usd)
        reserve = _decimal(self.estimated_future_exit_cost_usd, nonnegative=True)
        object.__setattr__(self, "legs", legs)
        # Input digit/exponent/quantity bounds make 512 digits sufficient for
        # every addition and multiplication, independently of caller context.
        with localcontext() as context:
            context.prec = _PRECISION
            context.rounding = ROUND_HALF_EVEN
            context.traps[Inexact] = True
            breakpoints = sorted({ZERO, *(leg.strike for leg in legs)})
            segments = []
            for index, lower in enumerate(breakpoints):
                slope, intrinsic_intercept = ZERO, ZERO
                for leg in legs:
                    exposure = Decimal(leg.signed_quantity * leg.multiplier)
                    if leg.right == "C" and leg.strike <= lower:
                        slope += exposure
                        intrinsic_intercept -= exposure * leg.strike
                    elif leg.right == "P" and leg.strike > lower:
                        slope -= exposure
                        intrinsic_intercept += exposure * leg.strike
                segments.append(HoldingsPayoffSegment(
                    lower, breakpoints[index + 1] if index + 1 < len(breakpoints) else None,
                    slope, intrinsic_intercept - baseline - reserve, intrinsic_intercept,
                ))
            _require(segments[-1].slope >= ZERO, "HOLDINGS_PAYOFF_UNBOUNDED_LOSS")
            values = tuple(row.slope * row.lower_bound + row.intrinsic_intercept for row in segments)
            minimum = min(values)
            unlimited = segments[-1].slope > ZERO
            maximum = None if unlimited else max(values)
            derived = {
                "segments": tuple(segments), "min_intrinsic_usd": minimum,
                "max_intrinsic_usd": maximum, "unbounded_profit": unlimited,
                "gross_holding_max_loss_usd": max(ZERO, baseline - minimum),
                "max_loss_usd": max(ZERO, baseline + reserve - minimum),
                "max_profit_usd": None if maximum is None else maximum - baseline - reserve,
            }
        for name, value in derived.items():
            object.__setattr__(self, name, value)
        regions = _regions(self.segments)
        points = tuple(_root_display(row.lower_bound) for row in regions if row.is_point)
        object.__setattr__(self, "break_even_regions", regions)
        object.__setattr__(self, "breakevens", tuple(value for value, _exact in points))
        object.__setattr__(self, "breakevens_exact", all(exact for _value, exact in points))
        object.__setattr__(self, "geometry_hash", canonical_hash(self.geometry_payload()))
        object.__setattr__(self, "payoff_hash", canonical_hash(self.hash_payload()))

    @property
    def cost_adjusted_holding_max_loss_usd(self) -> Decimal:
        return self.max_loss_usd

    def geometry_payload(self) -> dict[str, object]:
        return {
            "model": self.model, "legs": [leg.as_dict() for leg in self.legs],
            "intrinsic_segments": [
                {"lower_bound": _text(row.lower_bound),
                 "upper_bound": None if row.upper_bound is None else _text(row.upper_bound),
                 "slope": _text(row.slope), "intercept": _text(row.intrinsic_intercept)}
                for row in self.segments
            ],
        }

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema, "valuation_basis": self.valuation_basis, "model": self.model,
            "legs": [leg.as_dict() for leg in self.legs],
            "gross_liquidation_cashflow_usd": _text(self.gross_liquidation_cashflow_usd),
            "estimated_future_exit_cost_usd": _text(self.estimated_future_exit_cost_usd),
            "future_exit_cost_included_in_max_loss": True,
            "historical_entry_cost_included": False, "reopening_premium_included": False,
            "min_intrinsic_usd": _text(self.min_intrinsic_usd),
            "max_intrinsic_usd": None if self.max_intrinsic_usd is None else _text(self.max_intrinsic_usd),
            "gross_holding_max_loss_usd": _text(self.gross_holding_max_loss_usd),
            "cost_adjusted_holding_max_loss_usd": _text(self.max_loss_usd),
            "max_loss_usd": _text(self.max_loss_usd),
            "max_profit_usd": None if self.max_profit_usd is None else _text(self.max_profit_usd),
            "unbounded_profit": self.unbounded_profit,
            "segments": [row.as_dict() for row in self.segments],
            "breakevens": [_text(value) for value in self.breakevens],
            "breakevens_exact": self.breakevens_exact,
            "breakeven_display_precision": _ROOT_DISPLAY_PRECISION,
            "break_even_regions": [row.as_dict() for row in self.break_even_regions],
            "geometry_hash": self.geometry_hash,
            "risk_scope": "TERMINAL_INTRINSIC_WITH_SINGLE_FIXED_FUTURE_EXIT_RESERVE",
            "atomic_fill_verified": False, "assignment_settlement_risk_verified": False,
            "broker_margin_verified": False, "strategy_grouping_verified": False,
            "instruction_creation_allowed": False, "order_allowed": False,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "payoff_hash": self.payoff_hash}

    def verify_hash(self) -> bool:
        return (canonical_hash(self.geometry_payload()) == self.geometry_hash
                and canonical_hash(self.hash_payload()) == self.payoff_hash)

    def pnl_at(self, terminal_underlying_price: Decimal, *, include_future_exit_cost: bool = True) -> Decimal:
        price = _decimal(terminal_underlying_price, nonnegative=True)
        _require(type(include_future_exit_cost) is bool, "HOLDINGS_PAYOFF_COST_FLAG_INVALID")
        with localcontext() as context:
            context.prec = _PRECISION
            for row in self.segments:
                if price >= row.lower_bound and (row.upper_bound is None or price <= row.upper_bound):
                    return row.slope * price + row.intercept + (ZERO if include_future_exit_cost else self.estimated_future_exit_cost_usd)
        raise HoldingsPayoffError("HOLDINGS_PAYOFF_PRICE_OUTSIDE_DOMAIN")

    def intrinsic_at(self, terminal_underlying_price: Decimal) -> Decimal:
        price = _decimal(terminal_underlying_price, nonnegative=True)
        with localcontext() as context:
            context.prec = _PRECISION
            for row in self.segments:
                if price >= row.lower_bound and (row.upper_bound is None or price <= row.upper_bound):
                    return row.slope * price + row.intrinsic_intercept
        raise HoldingsPayoffError("HOLDINGS_PAYOFF_PRICE_OUTSIDE_DOMAIN")


def analyze_holdings_payoff(
    legs: Sequence[HoldingPayoffLeg],
    *,
    gross_liquidation_cashflow_usd: Decimal,
    estimated_future_exit_cost_usd: Decimal,
) -> HoldingsPayoff:
    """Analyze a bounded full observed set; the caller owns quote authority."""

    _require(isinstance(legs, (tuple, list)) and 1 <= len(legs) <= 8, "HOLDINGS_PAYOFF_LEG_BOUND_EXCEEDED")
    return HoldingsPayoff(tuple(legs), gross_liquidation_cashflow_usd, estimated_future_exit_cost_usd)


__all__ = [
    "HoldingPayoffLeg", "HoldingsPayoff", "HoldingsPayoffSegment", "HoldingsBreakEvenRegion",
    "HoldingsPayoffError", "analyze_holdings_payoff",
]
