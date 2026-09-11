"""Auditable contracts for a strictly read-only news research workflow."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from options_copilot.research_allocation import canonical_research_symbol
from options_copilot.storage.canonical import canonical_hash


UNDERLYING_QUOTE_BASIS_SCHEMA = "options_copilot.underlying_quote_basis.v1"


class EventCategory(str, Enum):
    EARNINGS = "EARNINGS"
    FOMC = "FOMC"
    MACRO = "MACRO"
    GUIDANCE = "GUIDANCE"
    REGULATORY = "REGULATORY"
    M_AND_A = "M_AND_A"
    PRODUCT = "PRODUCT"
    ANALYST = "ANALYST"
    OTHER = "OTHER"


class ImpactDirection(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    MIXED = "MIXED"
    NEUTRAL = "NEUTRAL"
    UNKNOWN = "UNKNOWN"


class ImpactHorizon(str, Enum):
    INTRADAY = "INTRADAY"
    DAYS_1_3 = "DAYS_1_3"
    DAYS_4_10 = "DAYS_4_10"
    WEEKS_2_4 = "WEEKS_2_4"


class NewsAuthority(str, Enum):
    ANCHORED = "ANCHORED"
    SUPPORTING_ONLY = "SUPPORTING_ONLY"


class AnalysisStage(str, Enum):
    PROVISIONAL = "PROVISIONAL"
    MARKET_CONFIRMED = "MARKET_CONFIRMED"


class ScoreBand(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class OptionRight(str, Enum):
    CALL = "CALL"
    PUT = "PUT"


class OptionLegSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class PreselectionPhase(str, Enum):
    PRE_MARKET = "PRE_MARKET"
    OPEN_REPRICED = "OPEN_REPRICED"


def _aware(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _nonblank(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not (normalized := value.strip()):
        raise ValueError(f"{field_name} cannot be blank")
    return normalized


def _digest(value: str, field_name: str) -> str:
    normalized = _nonblank(value, field_name).lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return normalized


def _symbols(values: tuple[str, ...]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        symbol = canonical_research_symbol(value)
        if symbol is None:
            raise ValueError("invalid symbol")
        if symbol not in result:
            result.append(symbol)
    return tuple(result)


def _enum(value: Any, enum_type: type[Enum], field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().upper())
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value


class AuditJson:
    """Provides stable JSON-safe output without concealing source evidence."""

    def as_dict(self) -> dict[str, Any]:
        return _json_value(asdict(self))


@dataclass(frozen=True, slots=True)
class UnderlyingQuoteBasis(AuditJson):
    """Exact live IBKR stock quote that selected one option direction."""

    symbol: str
    contract_id: int
    exchange: str
    source: str
    observed_at: datetime
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    close: Decimal
    market_data_type: int
    schema: str = field(default=UNDERLYING_QUOTE_BASIS_SCHEMA, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbols((self.symbol,))[0])
        if (
            isinstance(self.contract_id, bool)
            or not isinstance(self.contract_id, int)
            or self.contract_id <= 0
        ):
            raise ValueError("contract_id must be a positive integer")
        object.__setattr__(
            self,
            "exchange",
            _nonblank(self.exchange, "exchange").upper(),
        )
        source = _nonblank(self.source, "source").upper()
        if source != "IBKR_REQ_TICKERS_READONLY":
            raise ValueError("underlying quote source must be live read-only IBKR")
        object.__setattr__(self, "source", source)
        _aware(self.observed_at, "observed_at")
        for name in ("bid", "ask", "last", "close"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, Decimal)
                or not value.is_finite()
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive finite Decimal or None")
        if self.bid is not None and self.ask is not None and self.ask < self.bid:
            raise ValueError("underlying quote ask cannot be below bid")
        if self.close is None:
            raise ValueError("underlying quote close is required")
        if (
            isinstance(self.market_data_type, bool)
            or not isinstance(self.market_data_type, int)
            or self.market_data_type != 1
        ):
            raise ValueError("underlying quote market data must be live")
        if self.market_price is None:
            raise ValueError("underlying quote market price is unavailable")

    @property
    def market_price(self) -> Decimal | None:
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / Decimal("2")
        return self.last if self.last is not None else self.close

    @property
    def basis_hash(self) -> str:
        return canonical_hash(self.as_dict())


@dataclass(frozen=True, slots=True)
class OptionContractRef(AuditJson):
    """Exact IBKR option-contract identity, independent of a live quote."""

    con_id: int
    local_symbol: str
    trading_class: str
    multiplier: int
    exchange: str
    expiry: date
    strike: Decimal
    right: OptionRight

    def __post_init__(self) -> None:
        if isinstance(self.con_id, bool) or not isinstance(self.con_id, int) or self.con_id <= 0:
            raise ValueError("con_id must be a positive integer")
        if isinstance(self.multiplier, bool) or not isinstance(self.multiplier, int) or self.multiplier <= 0:
            raise ValueError("multiplier must be a positive integer")
        for name in ("local_symbol", "trading_class", "exchange"):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        object.__setattr__(self, "exchange", self.exchange.upper())
        if not isinstance(self.expiry, date) or isinstance(self.expiry, datetime):
            raise TypeError("expiry must be a date")
        if not isinstance(self.strike, Decimal) or not self.strike.is_finite() or self.strike <= 0:
            raise ValueError("strike must be a positive finite Decimal")
        object.__setattr__(self, "right", _enum(self.right, OptionRight, "right"))


@dataclass(frozen=True, slots=True)
class NewsInput(AuditJson):
    event_id: str
    headline: str
    summary: str
    source: str
    source_url: str
    published_at: datetime
    first_seen_at: datetime
    evidence_ids: tuple[str, ...]
    symbols: tuple[str, ...] = ()
    authority: NewsAuthority = NewsAuthority.SUPPORTING_ONLY
    conflicting_evidence_ids: tuple[str, ...] = ()
    is_complete: bool = True

    def __post_init__(self) -> None:
        for name in ("event_id", "headline", "source", "source_url"):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        _aware(self.published_at, "published_at")
        _aware(self.first_seen_at, "first_seen_at")
        if self.first_seen_at < self.published_at:
            raise ValueError("first_seen_at cannot precede published_at")
        object.__setattr__(self, "symbols", _symbols(tuple(self.symbols)))
        object.__setattr__(self, "authority", _enum(self.authority, NewsAuthority, "authority"))
        evidence = tuple(dict.fromkeys(_nonblank(item, "evidence_id") for item in self.evidence_ids))
        if not evidence:
            raise ValueError("at least one evidence_id is required")
        object.__setattr__(self, "evidence_ids", evidence)
        object.__setattr__(
            self, "conflicting_evidence_ids",
            tuple(dict.fromkeys(_nonblank(item, "conflicting_evidence_id") for item in self.conflicting_evidence_ids)),
        )
        if not isinstance(self.is_complete, bool):
            raise TypeError("is_complete must be a bool")


@dataclass(frozen=True, slots=True)
class ClassifiedEvent(AuditJson):
    category: EventCategory
    symbols: tuple[str, ...]
    direction: ImpactDirection
    horizon: ImpactHorizon
    confidence: Decimal
    counter_evidence: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    classifier: str

    def __post_init__(self) -> None:
        for name, enum_type in (("category", EventCategory), ("direction", ImpactDirection), ("horizon", ImpactHorizon)):
            object.__setattr__(self, name, _enum(getattr(self, name), enum_type, name))
        object.__setattr__(self, "symbols", _symbols(tuple(self.symbols)))
        if not isinstance(self.confidence, Decimal) or not self.confidence.is_finite():
            raise TypeError("confidence must be a finite Decimal")
        if not Decimal("0") <= self.confidence <= Decimal("1"):
            raise ValueError("confidence must be between 0 and 1")
        object.__setattr__(self, "counter_evidence", tuple(_nonblank(item, "counter_evidence") for item in self.counter_evidence))
        evidence = tuple(dict.fromkeys(_nonblank(item, "evidence_id") for item in self.evidence_ids))
        if not evidence:
            raise ValueError("at least one evidence_id is required")
        object.__setattr__(self, "evidence_ids", evidence)
        object.__setattr__(self, "classifier", _nonblank(self.classifier, "classifier"))


@dataclass(frozen=True, slots=True)
class OptionTradabilityInput(AuditJson):
    """Hard, point-in-time IBKR data; no LLM or news signal is accepted here."""
    symbol: str
    source: str
    observed_at: datetime
    bid: Decimal | None
    ask: Decimal | None
    volume: int | None
    open_interest: int | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbols((self.symbol,))[0])
        if _nonblank(self.source, "source").upper() != "IBKR":
            raise ValueError("option tradability hard data source must be IBKR")
        object.__setattr__(self, "source", "IBKR")
        _aware(self.observed_at, "observed_at")
        for name in ("bid", "ask"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite() or value < 0):
                raise ValueError(f"{name} must be a non-negative finite Decimal or None")
        if self.bid is not None and self.ask is not None and self.bid > self.ask:
            raise ValueError("bid cannot exceed ask")
        for name in ("volume", "open_interest"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError(f"{name} must be a non-negative integer or None")

    @property
    def complete(self) -> bool:
        return all(value is not None for value in (self.bid, self.ask, self.volume, self.open_interest))


@dataclass(frozen=True, slots=True)
class MarketConfirmation(AuditJson):
    source: str
    observed_at: datetime
    direction: ImpactDirection
    evidence_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if _nonblank(self.source, "source").upper() != "IBKR":
            raise ValueError("market confirmation source must be IBKR")
        object.__setattr__(self, "source", "IBKR")
        _aware(self.observed_at, "observed_at")
        object.__setattr__(self, "direction", _enum(self.direction, ImpactDirection, "direction"))
        evidence = tuple(dict.fromkeys(_nonblank(item, "evidence_id") for item in self.evidence_ids))
        if not evidence:
            raise ValueError("at least one evidence_id is required")
        object.__setattr__(self, "evidence_ids", evidence)


@dataclass(frozen=True, slots=True)
class ConditionalOptionLeg(AuditJson):
    """One read-only option leg with an exact contract and quote binding.

    Fields are optional so an incomplete discovery candidate can remain visible
    as research.  Missing values are never inferred and are hard blockers when
    the candidate is evaluated for the display-only action pool.
    """

    underlying: str
    con_id: int | None
    expiry: date | None
    strike: Decimal | None
    right: OptionRight | None
    side: OptionLegSide | None
    ratio: int | None
    quantity: int | None
    bid: Decimal | None
    ask: Decimal | None
    quote_asof: datetime | None
    quote_batch_id: str | None
    implied_volatility: Decimal | None
    delta: Decimal | None
    gamma: Decimal | None
    theta: Decimal | None
    vega: Decimal | None
    volume: int | None
    open_interest: int | None
    dte: int | None
    local_symbol: str | None = None
    trading_class: str | None = None
    multiplier: int | None = None
    exchange: str | None = None

    _IBKR_IDENTITY_FIELDS = (
        "con_id",
        "local_symbol",
        "trading_class",
        "multiplier",
        "exchange",
        "expiry",
        "strike",
        "right",
    )
    _STRATEGY_IDENTITY_FIELDS = (
        "side",
        "ratio",
        "quantity",
    )
    _DYNAMIC_QUOTE_FIELDS = (
        "bid",
        "ask",
        "quote_asof",
        "quote_batch_id",
        "implied_volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "volume",
        "open_interest",
    )
    _EXECUTION_FIELDS = (
        *_IBKR_IDENTITY_FIELDS,
        *_STRATEGY_IDENTITY_FIELDS,
        *_DYNAMIC_QUOTE_FIELDS,
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "underlying", _symbols((self.underlying,))[0])
        for name in ("con_id", "ratio", "quantity", "multiplier"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
            ):
                raise ValueError(f"{name} must be a positive integer or None")
        for name in ("local_symbol", "trading_class", "exchange"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _nonblank(value, name))
        if self.exchange is not None:
            object.__setattr__(self, "exchange", self.exchange.upper())
        for name in ("volume", "open_interest", "dte"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer or None")
        if self.expiry is not None and (
            not isinstance(self.expiry, date) or isinstance(self.expiry, datetime)
        ):
            raise TypeError("expiry must be a date or None")
        if self.right is not None:
            object.__setattr__(self, "right", _enum(self.right, OptionRight, "right"))
        if self.side is not None:
            object.__setattr__(self, "side", _enum(self.side, OptionLegSide, "side"))
        for name in (
            "strike",
            "bid",
            "ask",
            "implied_volatility",
            "delta",
            "gamma",
            "theta",
            "vega",
        ):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, Decimal) or not value.is_finite()
            ):
                raise ValueError(f"{name} must be a finite Decimal or None")
        if self.strike is not None and self.strike <= 0:
            raise ValueError("strike must be positive")
        for name in ("bid", "ask", "implied_volatility"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")
        # A crossed quote remains representable as point-in-time evidence.  It
        # is rejected by the fail-closed evaluator so the malformed market
        # input can be recorded without ever becoming action-pool eligible.
        if self.quote_asof is not None:
            _aware(self.quote_asof, "quote_asof")
        if self.quote_batch_id is not None:
            object.__setattr__(
                self,
                "quote_batch_id",
                _nonblank(self.quote_batch_id, "quote_batch_id"),
            )

    @property
    def missing_execution_fields(self) -> tuple[str, ...]:
        return tuple(name for name in self._EXECUTION_FIELDS if getattr(self, name) is None)

    @property
    def contract_ref(self) -> OptionContractRef | None:
        """Return a typed IBKR identity only when all eight fields are present."""

        if any(getattr(self, name) is None for name in self._IBKR_IDENTITY_FIELDS):
            return None
        return OptionContractRef(
            con_id=self.con_id,  # type: ignore[arg-type]
            local_symbol=self.local_symbol,  # type: ignore[arg-type]
            trading_class=self.trading_class,  # type: ignore[arg-type]
            multiplier=self.multiplier,  # type: ignore[arg-type]
            exchange=self.exchange,  # type: ignore[arg-type]
            expiry=self.expiry,  # type: ignore[arg-type]
            strike=self.strike,  # type: ignore[arg-type]
            right=self.right,  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class PreselectionTerminalScenario(AuditJson):
    """One frozen expiry outcome used only for deterministic EV recomputation."""

    terminal_underlying_price: Decimal
    probability: Decimal

    def __post_init__(self) -> None:
        for name in ("terminal_underlying_price", "probability"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if self.terminal_underlying_price < 0:
            raise ValueError("terminal_underlying_price must be non-negative")
        if not Decimal("0") < self.probability <= Decimal("1"):
            raise ValueError("probability must be greater than zero and at most one")


@dataclass(frozen=True, slots=True)
class ConditionalOptionPreselection(AuditJson):
    """A conditional option idea that never carries approval or order authority."""

    preselection_id: str
    underlying: str
    strategy_type: str
    phase: PreselectionPhase
    legs: tuple[ConditionalOptionLeg, ...]
    risk_defined: bool
    maximum_loss_usd: Decimal | None
    estimated_cost_usd: Decimal | None
    cost_after_ev_usd: Decimal | None
    entry_condition: str | None
    invalidation_condition: str | None
    profit_target_condition: str | None
    stop_loss_condition: str | None
    evidence_ids: tuple[str, ...]
    evidence_hashes: tuple[str, ...]
    strategy_hash: str
    research_summary: str
    underlying_quote_basis: UnderlyingQuoteBasis | None = None
    underlying_quote_basis_hash: str | None = None
    terminal_scenarios: tuple[PreselectionTerminalScenario, ...] = ()
    scenario_asof: datetime | None = None
    scenario_hash: str | None = None
    execution_cost_contract_version: str | None = None
    execution_cost_contract_hash: str | None = None
    risk_policy_version: str | None = None
    risk_policy_hash: str | None = None
    broker_snapshot_hash: str | None = None
    account_snapshot_hash: str | None = None
    strategy_nav_usd: Decimal | None = None
    strategy_nav_post_hash: str | None = None
    economics_quote_batch_id: str | None = None
    economics_quote_asof: datetime | None = None
    ranking_snapshot_id: str | None = None
    ranking_candidate_hash: str | None = None
    payoff_hash: str | None = None
    economics_calculation_hash: str | None = None
    debit_usd: Decimal | None = None
    credit_usd: Decimal | None = None
    net_entry_cost_usd: Decimal | None = None
    estimated_commission_usd: Decimal | None = None
    estimated_entry_slippage_usd: Decimal | None = None
    estimated_exit_slippage_usd: Decimal | None = None
    estimated_slippage_usd: Decimal | None = None
    expected_value_before_costs_usd: Decimal | None = None
    risk_fraction: Decimal | None = None
    decision_authority: NewsAuthority = field(
        default=NewsAuthority.SUPPORTING_ONLY, init=False
    )
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "preselection_id", _nonblank(self.preselection_id, "preselection_id")
        )
        object.__setattr__(self, "underlying", _symbols((self.underlying,))[0])
        object.__setattr__(
            self, "strategy_type", _nonblank(self.strategy_type, "strategy_type").upper()
        )
        object.__setattr__(self, "phase", _enum(self.phase, PreselectionPhase, "phase"))
        legs = tuple(self.legs)
        if any(not isinstance(item, ConditionalOptionLeg) for item in legs):
            raise TypeError("legs must contain ConditionalOptionLeg values")
        object.__setattr__(self, "legs", legs)
        if not isinstance(self.risk_defined, bool):
            raise TypeError("risk_defined must be a bool")
        for name in ("maximum_loss_usd", "estimated_cost_usd", "cost_after_ev_usd"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, Decimal) or not value.is_finite()
            ):
                raise ValueError(f"{name} must be a finite Decimal or None")
        if self.maximum_loss_usd is not None and self.maximum_loss_usd < 0:
            raise ValueError("maximum_loss_usd must be non-negative")
        if self.estimated_cost_usd is not None and self.estimated_cost_usd < 0:
            raise ValueError("estimated_cost_usd must be non-negative")
        for name in (
            "entry_condition",
            "invalidation_condition",
            "profit_target_condition",
            "stop_loss_condition",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _nonblank(value, name))
        object.__setattr__(
            self,
            "evidence_ids",
            tuple(dict.fromkeys(_nonblank(item, "evidence_id") for item in self.evidence_ids)),
        )
        hashes = tuple(
            dict.fromkeys(_digest(item, "evidence_hash") for item in self.evidence_hashes)
        )
        object.__setattr__(self, "evidence_hashes", hashes)
        object.__setattr__(self, "strategy_hash", _digest(self.strategy_hash, "strategy_hash"))
        object.__setattr__(
            self, "research_summary", _nonblank(self.research_summary, "research_summary")
        )
        if (self.underlying_quote_basis is None) != (
            self.underlying_quote_basis_hash is None
        ):
            raise ValueError("underlying quote basis and hash must be provided together")
        if self.underlying_quote_basis is not None:
            if not isinstance(self.underlying_quote_basis, UnderlyingQuoteBasis):
                raise TypeError(
                    "underlying_quote_basis must be an UnderlyingQuoteBasis"
                )
            checked_basis_hash = _digest(
                self.underlying_quote_basis_hash,
                "underlying_quote_basis_hash",
            )
            if checked_basis_hash != self.underlying_quote_basis.basis_hash:
                raise ValueError("underlying quote basis hash does not match")
            if checked_basis_hash not in self.evidence_hashes:
                raise ValueError("underlying quote basis hash is missing from evidence")
            object.__setattr__(
                self,
                "underlying_quote_basis_hash",
                checked_basis_hash,
            )
        scenarios = tuple(self.terminal_scenarios)
        if any(not isinstance(item, PreselectionTerminalScenario) for item in scenarios):
            raise TypeError(
                "terminal_scenarios must contain PreselectionTerminalScenario values"
            )
        if scenarios:
            if sum((item.probability for item in scenarios), Decimal("0")) != Decimal("1"):
                raise ValueError("terminal scenario probabilities must sum exactly to one")
            prices = tuple(item.terminal_underlying_price for item in scenarios)
            if len(set(prices)) != len(prices):
                raise ValueError("terminal scenario prices must be unique")
        object.__setattr__(self, "terminal_scenarios", scenarios)
        if self.scenario_asof is not None:
            _aware(self.scenario_asof, "scenario_asof")
        if self.scenario_hash is not None:
            object.__setattr__(
                self,
                "scenario_hash",
                _digest(self.scenario_hash, "scenario_hash"),
            )
        if self.execution_cost_contract_version is not None:
            object.__setattr__(
                self,
                "execution_cost_contract_version",
                _nonblank(
                    self.execution_cost_contract_version,
                    "execution_cost_contract_version",
                ),
            )
        if self.execution_cost_contract_hash is not None:
            object.__setattr__(
                self,
                "execution_cost_contract_hash",
                _digest(
                    self.execution_cost_contract_hash,
                    "execution_cost_contract_hash",
                ),
            )
        if (self.execution_cost_contract_version is None) != (
            self.execution_cost_contract_hash is None
        ):
            raise ValueError("execution-cost contract version and hash must be paired")
        if self.risk_policy_version is not None:
            object.__setattr__(
                self,
                "risk_policy_version",
                _nonblank(self.risk_policy_version, "risk_policy_version"),
            )
        if self.risk_policy_hash is not None:
            object.__setattr__(
                self,
                "risk_policy_hash",
                _digest(self.risk_policy_hash, "risk_policy_hash"),
            )
        if (self.risk_policy_version is None) != (self.risk_policy_hash is None):
            raise ValueError("risk-policy version and hash must be paired")
        for name in (
            "broker_snapshot_hash",
            "account_snapshot_hash",
            "strategy_nav_post_hash",
            "payoff_hash",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _digest(value, name))
        if self.economics_quote_batch_id is not None:
            object.__setattr__(
                self,
                "economics_quote_batch_id",
                _nonblank(
                    self.economics_quote_batch_id,
                    "economics_quote_batch_id",
                ),
            )
        if self.economics_quote_asof is not None:
            _aware(self.economics_quote_asof, "economics_quote_asof")
        if self.ranking_snapshot_id is not None:
            object.__setattr__(
                self,
                "ranking_snapshot_id",
                _nonblank(self.ranking_snapshot_id, "ranking_snapshot_id"),
            )
        if self.ranking_candidate_hash is not None:
            object.__setattr__(
                self,
                "ranking_candidate_hash",
                _digest(self.ranking_candidate_hash, "ranking_candidate_hash"),
            )
        if (self.ranking_snapshot_id is None) != (
            self.ranking_candidate_hash is None
        ):
            raise ValueError("ranking snapshot id and candidate hash must be paired")
        if self.economics_calculation_hash is not None:
            object.__setattr__(
                self,
                "economics_calculation_hash",
                _digest(
                    self.economics_calculation_hash,
                    "economics_calculation_hash",
                ),
            )
        for name in (
            "strategy_nav_usd",
            "debit_usd",
            "credit_usd",
            "net_entry_cost_usd",
            "estimated_commission_usd",
            "estimated_entry_slippage_usd",
            "estimated_exit_slippage_usd",
            "estimated_slippage_usd",
            "expected_value_before_costs_usd",
            "risk_fraction",
        ):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, Decimal) or not value.is_finite()
            ):
                raise ValueError(f"{name} must be a finite Decimal or None")
        for name in (
            "strategy_nav_usd",
            "debit_usd",
            "credit_usd",
            "estimated_commission_usd",
            "estimated_entry_slippage_usd",
            "estimated_exit_slippage_usd",
            "estimated_slippage_usd",
            "risk_fraction",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True, slots=True)
class AnalyzedNews(AuditJson):
    analysis_id: str
    news: NewsInput
    classification: ClassifiedEvent
    analyzed_at: datetime
    stage: AnalysisStage
    event_impact: ScoreBand
    option_tradability: ScoreBand
    combined_opportunity: ScoreBand
    event_impact_score: Decimal
    option_tradability_score: Decimal
    combined_opportunity_score: Decimal
    tradability_data: OptionTradabilityInput | None
    market_confirmation: MarketConfirmation | None = None
    rank: int | None = None
    rank_one: bool = False
    approval_eligible: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        _nonblank(self.analysis_id, "analysis_id")
        _aware(self.analyzed_at, "analyzed_at")
        object.__setattr__(self, "stage", _enum(self.stage, AnalysisStage, "stage"))
        for name in ("event_impact", "option_tradability", "combined_opportunity"):
            object.__setattr__(self, name, _enum(getattr(self, name), ScoreBand, name))
        for name in ("event_impact_score", "option_tradability_score", "combined_opportunity_score"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise TypeError(f"{name} must be a finite Decimal")
            if not Decimal("0") <= value <= Decimal("100"):
                raise ValueError(f"{name} must be between 0 and 100")
        if self.market_confirmation is not None and self.market_confirmation.observed_at < self.news.published_at:
            raise ValueError("market confirmation cannot precede publication")
        if self.stage is AnalysisStage.MARKET_CONFIRMED and self.market_confirmation is None:
            raise ValueError("market-confirmed analysis requires IBKR confirmation")
        if self.rank is not None and (isinstance(self.rank, bool) or self.rank <= 0):
            raise ValueError("rank must be a positive integer")
        if self.rank_one != (self.rank == 1):
            raise ValueError("rank_one must exactly reflect rank == 1")

    @property
    def action_pool_eligible(self) -> bool:
        return bool(
            self.stage is AnalysisStage.MARKET_CONFIRMED
            and self.news.authority is NewsAuthority.ANCHORED
            and self.news.is_complete
            and not self.news.conflicting_evidence_ids
            and self.tradability_data is not None
            and self.tradability_data.complete
            and self.option_tradability is not ScoreBand.LOW
            and self.combined_opportunity is not ScoreBand.LOW
        )


@dataclass(frozen=True, slots=True)
class CalendarEvent(AuditJson):
    event_id: str
    category: EventCategory
    scheduled_at: datetime
    title: str
    symbols: tuple[str, ...] = ()
    source: str = "calendar"

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _nonblank(self.event_id, "event_id"))
        object.__setattr__(self, "category", _enum(self.category, EventCategory, "category"))
        _aware(self.scheduled_at, "scheduled_at")
        object.__setattr__(self, "title", _nonblank(self.title, "title"))
        object.__setattr__(self, "symbols", _symbols(tuple(self.symbols)))
        object.__setattr__(self, "source", _nonblank(self.source, "source"))


LLM_CLASSIFICATION_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["category", "symbols", "direction", "horizon", "confidence", "counter_evidence", "evidence_ids"],
    "properties": {
        "category": {"enum": [item.value for item in EventCategory]},
        "symbols": {"type": "array", "items": {"type": "string"}},
        "direction": {"enum": [item.value for item in ImpactDirection]},
        "horizon": {"enum": [item.value for item in ImpactHorizon]},
        "confidence": {"type": "string", "description": "Decimal from 0 through 1"},
        "counter_evidence": {"type": "array", "items": {"type": "string"}},
        "evidence_ids": {"type": "array", "items": {"type": "string"}},
    },
}
