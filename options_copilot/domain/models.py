"""Immutable, Decimal-only contracts for option strategy evaluation.

The domain layer intentionally carries no broker or pricing-engine behavior.
Every price and probability is a :class:`~decimal.Decimal`; accepting binary
floating point at this boundary would make the risk ceiling non-deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum


ZERO = Decimal("0")
ONE = Decimal("1")


class OptionRight(str, Enum):
    CALL = "CALL"
    PUT = "PUT"


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def payoff_sign(self) -> int:
        return 1 if self is PositionSide.LONG else -1


class CandidateRiskTier(str, Enum):
    """Risk eligibility, not an analyst's display grade.

    ``VALIDATED_A_GRADE`` must only be set after the separate model-governance
    process has unlocked the 15% ceiling.
    """

    NORMAL = "NORMAL"
    VALIDATED_A_GRADE = "VALIDATED_A_GRADE"


def _require_decimal(
    value: object,
    field_name: str,
    *,
    minimum: Decimal | None = None,
    strictly_positive: bool = False,
) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if strictly_positive and value <= ZERO:
        raise ValueError(f"{field_name} must be positive")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _nonblank(value: object, field_name: str, *, uppercase: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be blank")
    return normalized.upper() if uppercase else normalized


def _coerce_enum(value: object, enum_type: type[Enum], field_name: str) -> Enum:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        try:
            return enum_type(value.strip().upper())
        except ValueError as exc:
            raise ValueError(f"invalid {field_name}: {value!r}") from exc
    raise TypeError(f"{field_name} must be a {enum_type.__name__}")


@dataclass(frozen=True, slots=True)
class OptionContract:
    """A single US equity/ETF ``OPT`` contract."""

    contract_id: str
    underlying: str
    expiration: date
    strike: Decimal
    right: OptionRight
    multiplier: Decimal = Decimal("100")
    currency: str = "USD"
    exchange: str = "SMART"
    broker_contract_id: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_id", _nonblank(self.contract_id, "contract_id"))
        object.__setattr__(
            self,
            "underlying",
            _nonblank(self.underlying, "underlying", uppercase=True),
        )
        if isinstance(self.expiration, datetime) or not isinstance(self.expiration, date):
            raise TypeError("expiration must be a date, not a datetime")
        _require_decimal(self.strike, "strike", strictly_positive=True)
        object.__setattr__(
            self,
            "right",
            _coerce_enum(self.right, OptionRight, "right"),
        )
        _require_decimal(self.multiplier, "multiplier", strictly_positive=True)
        object.__setattr__(
            self,
            "currency",
            _nonblank(self.currency, "currency", uppercase=True),
        )
        object.__setattr__(
            self,
            "exchange",
            _nonblank(self.exchange, "exchange", uppercase=True),
        )
        if self.broker_contract_id is not None:
            if isinstance(self.broker_contract_id, bool) or not isinstance(
                self.broker_contract_id, int
            ):
                raise TypeError("broker_contract_id must be an integer")
            if self.broker_contract_id <= 0:
                raise ValueError("broker_contract_id must be positive")

    @property
    def security_type(self) -> str:
        return "OPT"


@dataclass(frozen=True, slots=True)
class OptionLeg:
    contract: OptionContract
    side: PositionSide
    quantity: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.contract, OptionContract):
            raise TypeError("contract must be an OptionContract")
        object.__setattr__(
            self,
            "side",
            _coerce_enum(self.side, PositionSide, "side"),
        )
        if isinstance(self.quantity, bool) or not isinstance(self.quantity, int):
            raise TypeError("quantity must be an integer")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")

    @property
    def signed_contracts(self) -> int:
        return self.side.payoff_sign * self.quantity


@dataclass(frozen=True, slots=True)
class OptionLegQuote:
    """A quote frozen for one strategy leg at a single observation time.

    Missing fields remain representable so an upstream market-data snapshot can
    be journaled faithfully.  The risk engine rejects a missing executable ask
    for a long leg or bid for a short leg.
    """

    leg: OptionLeg
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    implied_volatility: Decimal | None
    volume: int | None
    open_interest: int | None
    observed_at: datetime
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    exchange_time: datetime | None = None
    requested_at: datetime | None = None
    completed_at: datetime | None = None
    market_data_type: int | None = None
    quote_age_seconds: Decimal | None = None
    freshness_basis: str | None = None
    short_leg_risk_evidence_status: str = "NOT_APPLICABLE"
    short_leg_risk_evidence_reason_codes: tuple[str, ...] = ()
    short_leg_risk_evidence_hash: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.leg, OptionLeg):
            raise TypeError("leg must be an OptionLeg")
        for field_name in ("bid", "ask", "last", "implied_volatility"):
            value = getattr(self, field_name)
            if value is not None:
                _require_decimal(value, field_name, minimum=ZERO)
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("bid cannot exceed ask")
        for field_name in ("volume", "open_interest"):
            value = getattr(self, field_name)
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, int):
                    raise TypeError(f"{field_name} must be an integer")
                if value < 0:
                    raise ValueError(f"{field_name} cannot be negative")
        for field_name in ("delta", "gamma", "theta", "vega"):
            value = getattr(self, field_name)
            if value is None:
                continue
            minimum = ZERO if field_name in {"gamma", "vega"} else None
            _require_decimal(value, field_name, minimum=minimum)
        if self.delta is not None and not -ONE <= self.delta <= ONE:
            raise ValueError("delta must be between -1 and 1")
        for field_name in (
            "observed_at",
            "exchange_time",
            "requested_at",
            "completed_at",
        ):
            value = getattr(self, field_name)
            if value is None and field_name != "observed_at":
                continue
            if not isinstance(value, datetime):
                raise TypeError(f"{field_name} must be a datetime")
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if (
            self.requested_at is not None
            and self.completed_at is not None
            and not self.requested_at <= self.observed_at <= self.completed_at
        ):
            raise ValueError("quote timestamps are not chronologically coherent")
        if self.exchange_time is not None and self.exchange_time > self.observed_at:
            raise ValueError("exchange_time cannot follow observed_at")
        if self.market_data_type is not None:
            if isinstance(self.market_data_type, bool) or not isinstance(
                self.market_data_type, int
            ):
                raise TypeError("market_data_type must be an integer")
            if self.market_data_type <= 0:
                raise ValueError("market_data_type must be positive")
        if self.quote_age_seconds is not None:
            _require_decimal(
                self.quote_age_seconds,
                "quote_age_seconds",
                minimum=ZERO,
            )
            if self.exchange_time is None or self.freshness_basis != "EXCHANGE_TIME":
                raise ValueError(
                    "quote age requires exchange_time freshness basis"
                )
        if self.freshness_basis is not None and self.freshness_basis != "EXCHANGE_TIME":
            raise ValueError("freshness_basis must be EXCHANGE_TIME")
        status = str(self.short_leg_risk_evidence_status).strip().upper()
        if status not in {"NOT_APPLICABLE", "SUPPORTED", "UNSUPPORTED"}:
            raise ValueError("short-leg risk evidence status is invalid")
        object.__setattr__(self, "short_leg_risk_evidence_status", status)
        reasons = tuple(
            dict.fromkeys(
                str(item).strip().upper()
                for item in self.short_leg_risk_evidence_reason_codes
                if str(item).strip()
            )
        )
        object.__setattr__(self, "short_leg_risk_evidence_reason_codes", reasons)
        evidence_hash = self.short_leg_risk_evidence_hash
        if evidence_hash is not None and (
            not isinstance(evidence_hash, str)
            or len(evidence_hash) != 64
            or any(char not in "0123456789abcdefABCDEF" for char in evidence_hash)
        ):
            raise ValueError("short-leg risk evidence hash must be SHA-256")
        if status == "SUPPORTED" and evidence_hash is None:
            raise ValueError("supported short-leg risk evidence requires a hash")
        if status == "UNSUPPORTED" and not reasons:
            raise ValueError("unsupported short-leg risk evidence requires reasons")
        if status == "NOT_APPLICABLE" and (reasons or evidence_hash is not None):
            raise ValueError("not-applicable short-leg evidence cannot carry proof")

    @property
    def contract(self) -> OptionContract:
        return self.leg.contract

    @property
    def executable_price(self) -> Decimal | None:
        return self.ask if self.leg.side is PositionSide.LONG else self.bid


@dataclass(frozen=True, slots=True)
class TerminalScenario:
    """One mutually exclusive expiry-price outcome and its probability."""

    terminal_underlying_price: Decimal
    probability: Decimal

    def __post_init__(self) -> None:
        _require_decimal(
            self.terminal_underlying_price,
            "terminal_underlying_price",
            minimum=ZERO,
        )
        _require_decimal(self.probability, "probability", strictly_positive=True)
        if self.probability > ONE:
            raise ValueError("probability cannot exceed 1")


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    """A fully quoted option combination offered to risk and ranking engines."""

    candidate_id: str
    leg_quotes: tuple[OptionLegQuote, ...]
    terminal_scenarios: tuple[TerminalScenario, ...] = ()
    estimated_commissions: Decimal = ZERO
    estimated_slippage: Decimal = ZERO
    risk_tier: CandidateRiskTier = CandidateRiskTier.NORMAL

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", _nonblank(self.candidate_id, "candidate_id"))
        try:
            leg_quotes = tuple(self.leg_quotes)
        except TypeError as exc:
            raise TypeError("leg_quotes must be an iterable of OptionLegQuote") from exc
        if not leg_quotes:
            raise ValueError("a strategy candidate requires at least one option leg")
        if not all(isinstance(item, OptionLegQuote) for item in leg_quotes):
            raise TypeError("leg_quotes must contain only OptionLegQuote values")
        object.__setattr__(self, "leg_quotes", leg_quotes)

        try:
            terminal_scenarios = tuple(self.terminal_scenarios)
        except TypeError as exc:
            raise TypeError(
                "terminal_scenarios must be an iterable of TerminalScenario"
            ) from exc
        if not all(isinstance(item, TerminalScenario) for item in terminal_scenarios):
            raise TypeError(
                "terminal_scenarios must contain only TerminalScenario values"
            )
        if terminal_scenarios:
            total_probability = sum(
                (item.probability for item in terminal_scenarios),
                ZERO,
            )
            if total_probability != ONE:
                raise ValueError("terminal scenario probabilities must sum exactly to 1")
        object.__setattr__(self, "terminal_scenarios", terminal_scenarios)

        _require_decimal(
            self.estimated_commissions,
            "estimated_commissions",
            minimum=ZERO,
        )
        _require_decimal(
            self.estimated_slippage,
            "estimated_slippage",
            minimum=ZERO,
        )
        object.__setattr__(
            self,
            "risk_tier",
            _coerce_enum(self.risk_tier, CandidateRiskTier, "risk_tier"),
        )

    @property
    def estimated_execution_costs(self) -> Decimal:
        return self.estimated_commissions + self.estimated_slippage

    @property
    def legs(self) -> tuple[OptionLeg, ...]:
        return tuple(item.leg for item in self.leg_quotes)

    @property
    def is_validated_a_grade(self) -> bool:
        return self.risk_tier is CandidateRiskTier.VALIDATED_A_GRADE


# Concise aliases used by adapters while retaining explicit public names.
LegQuote = OptionLegQuote
LegSide = PositionSide
ProbabilityScenario = TerminalScenario


__all__ = [
    "CandidateRiskTier",
    "LegQuote",
    "LegSide",
    "OptionContract",
    "OptionLeg",
    "OptionLegQuote",
    "OptionRight",
    "PositionSide",
    "ProbabilityScenario",
    "StrategyCandidate",
    "TerminalScenario",
]
