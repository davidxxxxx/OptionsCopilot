"""Hash-bound 09:35 economics for supporting-only option preselections.

The resolver is analytical only.  It accepts one immutable broker snapshot,
one frozen scenario contract, and a NAV binding; it cannot approve, create an
instruction, or submit an order.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import datetime
from decimal import Decimal, ROUND_CEILING
from enum import Enum

from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
)
from options_copilot.domain import (
    OptionContract,
    OptionLeg,
    OptionLegQuote,
    OptionRight,
    PositionSide,
    StrategyCandidate,
    TerminalScenario,
)
from options_copilot.execution_cost import (
    CENT,
    ENTRY_SPREAD_FACTOR,
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
    EXIT_SPREAD_FACTOR,
    FALLBACK_PER_CONTRACT_SIDE,
    MINIMUM_ENTRY_SLIPPAGE,
    MINIMUM_EXIT_SLIPPAGE,
    MINIMUM_PER_ORDER,
    SignedExecutionCostResolver,
)
from options_copilot.gateway.broker_snapshot import BrokerSnapshotStatus
from options_copilot.gateway.ibkr_readonly import QuoteBatchStatus
from options_copilot.risk import PayoffStatus, analyze_expiration_payoff
from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .models import PreselectionTerminalScenario


ZERO = Decimal("0")
ONE = Decimal("1")
MAXIMUM_QUOTE_AGE_SECONDS = Decimal("5")
NORMAL_RISK_FRACTION = Decimal("0.10")
HARD_RISK_FRACTION = Decimal("0.20")
_DIGEST_CHARS = frozenset("0123456789abcdef")


class OpenRepriceEconomicsError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class TrustedTerminalScenario:
    terminal_underlying_price: Decimal
    probability: Decimal

    def __post_init__(self) -> None:
        _finite_decimal(self.terminal_underlying_price, "terminal_underlying_price")
        probability = _finite_decimal(self.probability, "probability")
        if self.terminal_underlying_price < ZERO or not ZERO < probability <= ONE:
            raise ValueError("terminal scenario values are invalid")

    def as_dict(self) -> dict[str, object]:
        return {
            "terminal_underlying_price": self.terminal_underlying_price,
            "probability": self.probability,
        }


@dataclass(frozen=True, slots=True)
class TrustedTerminalScenarioSet:
    candidate_id: str
    strategy_hash: str
    scenario_asof: datetime
    scenarios: tuple[TrustedTerminalScenario, ...]
    current_policy_version: str
    current_policy_hash: str
    scenario_hash: str

    @classmethod
    def create(
        cls,
        *,
        candidate_id: str,
        strategy_hash: str,
        scenario_asof: datetime,
        scenarios: Sequence[TrustedTerminalScenario],
        current_policy_version: str,
        current_policy_hash: str,
    ) -> "TrustedTerminalScenarioSet":
        provisional = cls(
            candidate_id=_text(candidate_id, "candidate_id"),
            strategy_hash=_digest(strategy_hash, "strategy_hash"),
            scenario_asof=utc_datetime(scenario_asof, field="scenario_asof"),
            scenarios=tuple(scenarios),
            current_policy_version=_text(
                current_policy_version, "current_policy_version"
            ),
            current_policy_hash=_digest(
                current_policy_hash, "current_policy_hash"
            ),
            scenario_hash="0" * 64,
        )
        return replace(provisional, scenario_hash=canonical_hash(provisional.hash_payload()))

    def __post_init__(self) -> None:
        _text(self.candidate_id, "candidate_id")
        _digest(self.strategy_hash, "strategy_hash")
        utc_datetime(self.scenario_asof, field="scenario_asof")
        values = tuple(self.scenarios)
        if not values or any(not isinstance(item, TrustedTerminalScenario) for item in values):
            raise ValueError("terminal scenarios are missing or invalid")
        object.__setattr__(self, "scenarios", values)
        _text(self.current_policy_version, "current_policy_version")
        _digest(self.current_policy_hash, "current_policy_hash")
        _digest(self.scenario_hash, "scenario_hash")

    def hash_payload(self) -> dict[str, object]:
        return {
            # This schema is shared with the external Top-10 source.  Keeping
            # the exact payload identity here prevents a source-valid
            # scenario set from being re-hashed under a different contract at
            # the 09:35 economics boundary.
            "schema": "options_copilot.trusted_terminal_scenario_set.v1",
            "candidate_id": self.candidate_id,
            "strategy_hash": self.strategy_hash,
            "scenario_asof": self.scenario_asof,
            "scenarios": tuple(item.as_dict() for item in self.scenarios),
            "current_policy_version": self.current_policy_version,
            "current_policy_hash": self.current_policy_hash,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.scenario_hash


def scenario_contract_hash(
    scenarios: tuple[PreselectionTerminalScenario, ...],
) -> str:
    """Compatibility hash for the typed news-model scenario tuple."""

    return canonical_hash(
        {
            "schema": "options_copilot.preselection_terminal_scenarios.v1",
            "scenarios": tuple(
                {
                    "terminal_underlying_price": item.terminal_underlying_price,
                    "probability": item.probability,
                }
                for item in scenarios
            ),
        }
    )


def strategy_nav_post_hash(
    *,
    candidate_id: str,
    strategy_hash: str,
    snapshot_hash: str,
    strategy_nav_usd: Decimal,
) -> str:
    return canonical_hash(
        {
            "schema": "options_copilot.open_strategy_nav_binding.v1",
            "candidate_id": _text(candidate_id, "candidate_id"),
            "strategy_hash": _digest(strategy_hash, "strategy_hash"),
            "snapshot_hash": _digest(snapshot_hash, "snapshot_hash"),
            "strategy_nav_usd": _positive_decimal(
                strategy_nav_usd, "strategy_nav_usd"
            ),
        }
    )


@dataclass(frozen=True, slots=True)
class OpenRepriceEconomics:
    candidate_id: str
    strategy_hash: str
    broker_snapshot_hash: str
    quote_batch_id: str
    quote_asof: datetime
    scenario_hash: str
    scenario_asof: datetime
    cost_contract_version: str
    cost_contract_hash: str
    policy_version: str
    policy_hash: str
    strategy_nav_usd: Decimal
    strategy_nav_post_hash: str
    debit_usd: Decimal
    credit_usd: Decimal
    commission_usd: Decimal
    entry_slippage_usd: Decimal
    exit_slippage_usd: Decimal
    total_slippage_usd: Decimal
    all_in_cost_usd: Decimal
    maximum_loss_usd: Decimal
    before_cost_expected_value_usd: Decimal
    after_cost_expected_value_usd: Decimal
    payoff_hash: str
    risk_fraction: Decimal
    economics_hash: str

    @property
    def calculation_hash(self) -> str:
        return self.economics_hash

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.open_reprice_economics.v1",
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "economics_hash"
            },
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.economics_hash

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "economics_hash": self.economics_hash}


class OpenRepriceEconomicsResolver:
    def __init__(
        self,
        cost_resolver: SignedExecutionCostResolver | None = None,
    ) -> None:
        self._cost_resolver = cost_resolver or SignedExecutionCostResolver()

    def resolve(
        self,
        candidate: object,
        *,
        snapshot: object,
        scenario_set: TrustedTerminalScenarioSet | None,
        now: datetime,
    ) -> OpenRepriceEconomics:
        checked_at = utc_datetime(now, field="now")
        candidate_id = _text(_field(candidate, "candidate_id", "preselection_id"), "candidate_id")
        strategy_hash = _digest(_field(candidate, "strategy_hash"), "strategy_hash")
        snapshot_hash = self._validate_snapshot(snapshot)
        quote_batch_id, quote_asof, snapshot_quotes = self._snapshot_quotes(
            snapshot,
            now=checked_at,
        )

        if scenario_set is None:
            raise OpenRepriceEconomicsError("SCENARIO_SET_MISSING")
        if not isinstance(scenario_set, TrustedTerminalScenarioSet):
            raise OpenRepriceEconomicsError("SCENARIO_SET_INVALID")
        if not scenario_set.verify_hash():
            raise OpenRepriceEconomicsError("SCENARIO_HASH_INVALID")
        if (
            scenario_set.candidate_id != candidate_id
            or scenario_set.strategy_hash != strategy_hash
        ):
            raise OpenRepriceEconomicsError("SCENARIO_BINDING_INVALID")
        if (
            scenario_set.current_policy_version != INITIAL_POLICY_VERSION
            or scenario_set.current_policy_hash != INITIAL_POLICY_HASH
        ):
            raise OpenRepriceEconomicsError("SCENARIO_POLICY_BINDING_INVALID")
        if sum((item.probability for item in scenario_set.scenarios), ZERO) != ONE:
            raise OpenRepriceEconomicsError("SCENARIO_PROBABILITY_INVALID")
        prices = tuple(item.terminal_underlying_price for item in scenario_set.scenarios)
        if len(set(prices)) != len(prices):
            raise OpenRepriceEconomicsError("SCENARIO_PRICE_DUPLICATE")
        if utc_datetime(scenario_set.scenario_asof, field="scenario_asof") > quote_asof:
            raise OpenRepriceEconomicsError("SCENARIO_FUTURE_INFORMATION")

        try:
            cost_identity = self._cost_resolver.resolve(now=checked_at)
        except Exception as exc:
            raise OpenRepriceEconomicsError("COST_CONTRACT_UNAVAILABLE") from exc
        if (
            _field(candidate, "execution_cost_contract_version")
            != cost_identity.cost_version
            or _field(candidate, "execution_cost_contract_hash")
            != cost_identity.cost_hash
            or cost_identity.cost_version != EXECUTION_COST_VERSION
            or cost_identity.cost_hash != EXECUTION_COST_HASH
        ):
            raise OpenRepriceEconomicsError("COST_CONTRACT_BINDING_INVALID")
        if (
            _field(candidate, "current_policy_version", "risk_policy_version")
            != INITIAL_POLICY_VERSION
            or _field(candidate, "current_policy_hash", "risk_policy_hash")
            != INITIAL_POLICY_HASH
        ):
            raise OpenRepriceEconomicsError("POLICY_CONTRACT_BINDING_INVALID")

        nav = _positive_decimal(_field(candidate, "strategy_nav_usd"), "strategy_nav_usd")
        expected_nav_hash = strategy_nav_post_hash(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            snapshot_hash=snapshot_hash,
            strategy_nav_usd=nav,
        )
        if _field(candidate, "strategy_nav_post_hash", "account_snapshot_hash") != expected_nav_hash:
            raise OpenRepriceEconomicsError("NAV_BINDING_INVALID")

        quote_rows = self._candidate_quotes(
            candidate,
            snapshot_quotes=snapshot_quotes,
            quote_batch_id=quote_batch_id,
            quote_asof=quote_asof,
        )
        total_sides = sum(item.leg.quantity for item in quote_rows)
        commission = (
            Decimal("2")
            * max(MINIMUM_PER_ORDER, FALLBACK_PER_CONTRACT_SIDE * Decimal(total_sides))
        ).quantize(CENT, rounding=ROUND_CEILING)
        entry_slippage = sum(
            max(MINIMUM_ENTRY_SLIPPAGE, ENTRY_SPREAD_FACTOR * (item.ask - item.bid))
            * item.contract.multiplier
            * item.leg.quantity
            for item in quote_rows
            if item.ask is not None and item.bid is not None
        ).quantize(CENT, rounding=ROUND_CEILING)
        exit_slippage = sum(
            max(MINIMUM_EXIT_SLIPPAGE, EXIT_SPREAD_FACTOR * (item.ask - item.bid))
            * item.contract.multiplier
            * item.leg.quantity
            for item in quote_rows
            if item.ask is not None and item.bid is not None
        ).quantize(CENT, rounding=ROUND_CEILING)
        total_slippage = (entry_slippage + exit_slippage).quantize(
            CENT, rounding=ROUND_CEILING
        )
        domain_scenarios = tuple(
            TerminalScenario(item.terminal_underlying_price, item.probability)
            for item in scenario_set.scenarios
        )
        try:
            strategy = StrategyCandidate(
                candidate_id=candidate_id,
                leg_quotes=tuple(quote_rows),
                terminal_scenarios=domain_scenarios,
                estimated_commissions=commission,
                estimated_slippage=total_slippage,
            )
            payoff = analyze_expiration_payoff(strategy)
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise OpenRepriceEconomicsError("UNBOUNDED_OR_UNKNOWN_MAX_LOSS") from exc
        if (
            payoff.status is not PayoffStatus.CALCULATED
            or payoff.max_loss is None
            or payoff.max_loss <= ZERO
        ):
            raise OpenRepriceEconomicsError("UNBOUNDED_OR_UNKNOWN_MAX_LOSS")

        debit = sum(
            (
                item.ask * item.contract.multiplier * item.leg.quantity
                for item in quote_rows
                if item.leg.side is PositionSide.LONG and item.ask is not None
            ),
            ZERO,
        ).quantize(CENT, rounding=ROUND_CEILING)
        credit = sum(
            (
                item.bid * item.contract.multiplier * item.leg.quantity
                for item in quote_rows
                if item.leg.side is PositionSide.SHORT and item.bid is not None
            ),
            ZERO,
        ).quantize(CENT, rounding=ROUND_CEILING)
        all_in_cost = (debit - credit + commission + total_slippage).quantize(
            CENT, rounding=ROUND_CEILING
        )
        if all_in_cost < ZERO:
            raise OpenRepriceEconomicsError("NET_CREDIT_UNSUPPORTED")
        after_cost_ev = sum(
            item.probability * payoff.pnl_at(item.terminal_underlying_price)
            for item in domain_scenarios
        )
        if not after_cost_ev.is_finite() or after_cost_ev <= ZERO:
            raise OpenRepriceEconomicsError("NON_POSITIVE_AFTER_COST_EV")
        before_cost_ev = after_cost_ev + commission + total_slippage
        risk_fraction = payoff.max_loss / nav
        if risk_fraction > NORMAL_RISK_FRACTION:
            raise OpenRepriceEconomicsError("NORMAL_RISK_LIMIT_EXCEEDED")

        payoff_hash = canonical_hash(
            {
                "schema": "options_copilot.open_payoff.v1",
                "candidate_id": candidate_id,
                "snapshot_hash": snapshot_hash,
                "quote_batch_id": quote_batch_id,
                "maximum_loss_usd": payoff.max_loss,
                "maximum_profit_usd": payoff.max_profit,
                "all_in_cost_usd": all_in_cost,
            }
        )
        provisional = OpenRepriceEconomics(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            broker_snapshot_hash=snapshot_hash,
            quote_batch_id=quote_batch_id,
            quote_asof=quote_asof,
            scenario_hash=scenario_set.scenario_hash,
            scenario_asof=scenario_set.scenario_asof,
            cost_contract_version=cost_identity.cost_version,
            cost_contract_hash=cost_identity.cost_hash,
            policy_version=INITIAL_POLICY_VERSION,
            policy_hash=INITIAL_POLICY_HASH,
            strategy_nav_usd=nav,
            strategy_nav_post_hash=expected_nav_hash,
            debit_usd=debit,
            credit_usd=credit,
            commission_usd=commission,
            entry_slippage_usd=entry_slippage,
            exit_slippage_usd=exit_slippage,
            total_slippage_usd=total_slippage,
            all_in_cost_usd=all_in_cost,
            maximum_loss_usd=payoff.max_loss,
            before_cost_expected_value_usd=before_cost_ev,
            after_cost_expected_value_usd=after_cost_ev,
            payoff_hash=payoff_hash,
            risk_fraction=risk_fraction,
            economics_hash="0" * 64,
        )
        return replace(provisional, economics_hash=canonical_hash(provisional.hash_payload()))

    @staticmethod
    def _validate_snapshot(snapshot: object) -> str:
        status = _field(snapshot, "status")
        if status is not BrokerSnapshotStatus.COMPLETE:
            raise OpenRepriceEconomicsError("BROKER_SNAPSHOT_INVALID")
        verify_hash = getattr(snapshot, "verify_hash", None)
        snapshot_hash = _field(snapshot, "snapshot_hash")
        if not callable(verify_hash) or not _is_digest(snapshot_hash):
            raise OpenRepriceEconomicsError("BROKER_SNAPSHOT_INVALID")
        try:
            valid = verify_hash()
        except Exception as exc:
            raise OpenRepriceEconomicsError("BROKER_SNAPSHOT_INVALID") from exc
        if valid is not True:
            raise OpenRepriceEconomicsError("BROKER_SNAPSHOT_INVALID")
        return str(snapshot_hash)

    @staticmethod
    def _snapshot_quotes(
        snapshot: object,
        *,
        now: datetime,
    ) -> tuple[str, datetime, dict[int, object]]:
        batch_id = _field(snapshot, "quote_batch_id")
        batch_status = _field(snapshot, "quote_batch_status")
        quote_asof = _field(snapshot, "quote_batch_observed_at")
        quotes = _field(snapshot, "quotes")
        if (
            batch_status is not QuoteBatchStatus.COMPLETE
            or not isinstance(batch_id, str)
            or not batch_id
            or not isinstance(quote_asof, datetime)
            or not isinstance(quotes, Sequence)
            or not quotes
        ):
            raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
        checked_asof = utc_datetime(quote_asof, field="quote_batch_observed_at")
        age = Decimal(str((now - checked_asof).total_seconds()))
        if age < ZERO or age > MAXIMUM_QUOTE_AGE_SECONDS:
            raise OpenRepriceEconomicsError("QUOTE_STALE_OR_FUTURE")
        by_id: dict[int, object] = {}
        for quote in quotes:
            con_id = _positive_integer(_field(quote, "contract_id"), "contract_id")
            if con_id in by_id or _field(quote, "batch_id") != batch_id:
                raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
            observed_at = utc_datetime(_field(quote, "observed_at"), field="observed_at")
            if observed_at != checked_asof:
                raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
            by_id[con_id] = quote
        secdefs = _field(snapshot, "secdef_evidence")
        if not isinstance(secdefs, Sequence) or {
            _field(item, "contract_id") for item in secdefs
        } != set(by_id):
            raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
        if any(
            _field(item, "stable") is not True
            or _field(item, "standard_contract") is not True
            or _field(item, "adjusted") is not False
            for item in secdefs
        ):
            raise OpenRepriceEconomicsError("BROKER_SNAPSHOT_INVALID")
        return batch_id, checked_asof, by_id

    @staticmethod
    def _candidate_quotes(
        candidate: object,
        *,
        snapshot_quotes: Mapping[int, object],
        quote_batch_id: str,
        quote_asof: datetime,
    ) -> tuple[OptionLegQuote, ...]:
        raw_legs = _field(candidate, "legs")
        if not isinstance(raw_legs, Sequence) or not raw_legs:
            raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
        con_ids = {_field(item, "con_id") for item in raw_legs}
        if (
            not con_ids
            or not con_ids.issubset(snapshot_quotes)
            or len(con_ids) != len(raw_legs)
        ):
            raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
        values: list[OptionLegQuote] = []
        for raw in raw_legs:
            con_id = _positive_integer(_field(raw, "con_id"), "con_id")
            quote = snapshot_quotes[con_id]
            if (
                _field(raw, "quote_batch_id") != quote_batch_id
                or utc_datetime(_field(raw, "quote_asof"), field="quote_asof")
                != quote_asof
            ):
                raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
            bid = _positive_decimal(_field(raw, "bid"), "bid")
            ask = _positive_decimal(_field(raw, "ask"), "ask")
            if bid >= ask:
                raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
            if bid != _field(quote, "bid") or ask != _field(quote, "ask"):
                raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH")
            right_text = _enum_text(_field(raw, "right"))
            side_text = _enum_text(_field(raw, "side"))
            try:
                right = OptionRight("CALL" if right_text in {"CALL", "C"} else "PUT")
                side = PositionSide(
                    "LONG" if side_text in {"BUY", "LONG"} else "SHORT"
                )
                contract = OptionContract(
                    contract_id=f"{con_id}@{_field(raw, 'exchange')}",
                    underlying=_text(_field(raw, "underlying"), "underlying"),
                    expiration=_field(raw, "expiry", "expiration"),
                    strike=_positive_decimal(_field(raw, "strike"), "strike"),
                    right=right,
                    multiplier=Decimal(
                        _positive_integer(_field(raw, "multiplier"), "multiplier")
                    ),
                    currency="USD",
                    exchange=_text(_field(raw, "exchange"), "exchange"),
                    broker_contract_id=con_id,
                )
                leg = OptionLeg(
                    contract=contract,
                    side=side,
                    quantity=_positive_integer(_field(raw, "quantity"), "quantity"),
                )
                values.append(
                    OptionLegQuote(
                        leg=leg,
                        bid=bid,
                        ask=ask,
                        last=None,
                        implied_volatility=None,
                        volume=None,
                        open_interest=None,
                        observed_at=quote_asof,
                    )
                )
            except (TypeError, ValueError) as exc:
                raise OpenRepriceEconomicsError("QUOTE_BATCH_MISMATCH") from exc
        return tuple(values)


def _field(value: object, *names: str) -> object:
    for name in names:
        if isinstance(value, Mapping) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _enum_text(value: object) -> str:
    return str(value.value if isinstance(value, Enum) else value).strip().upper()


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OpenRepriceEconomicsError(f"{field.upper()}_INVALID")
    return value.strip()


def _digest(value: object, field: str) -> str:
    if not _is_digest(value):
        raise OpenRepriceEconomicsError(f"{field.upper()}_INVALID")
    return str(value)


def _is_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _DIGEST_CHARS for character in value)
    )


def _finite_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise OpenRepriceEconomicsError(f"{field.upper()}_INVALID")
    return value


def _positive_decimal(value: object, field: str) -> Decimal:
    parsed = _finite_decimal(value, field)
    if parsed <= ZERO:
        raise OpenRepriceEconomicsError(f"{field.upper()}_INVALID")
    return parsed


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise OpenRepriceEconomicsError(f"{field.upper()}_INVALID")
    return value


__all__ = [
    "OpenRepriceEconomics",
    "OpenRepriceEconomicsError",
    "OpenRepriceEconomicsResolver",
    "TrustedTerminalScenario",
    "TrustedTerminalScenarioSet",
    "scenario_contract_hash",
    "strategy_nav_post_hash",
]
