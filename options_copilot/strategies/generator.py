"""Pure, fail-closed construction of cost-bound finite-risk candidates.

The generator receives already-read broker evidence.  It deliberately has no
gateway, network, model, approval, or order-placement dependency.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_EVEN
from types import MappingProxyType
from typing import Any

from options_copilot.domain import (
    OptionContract,
    OptionLeg,
    OptionLegQuote,
    OptionRight,
    PositionSide,
    StrategyCandidate,
    TerminalScenario,
)
from options_copilot.gateway.broker_snapshot import (
    AtomicBrokerSnapshot,
    atomic_account_nlv,
)
from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)
from options_copilot.governance.contracts import (
    ContractKind,
    ContractValidationError,
    verify_contract,
)
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.risk import DteEntryExceptionAuthority, OptionTimePolicy, PayoffStatus, analyze_expiration_payoff
from options_copilot.storage.canonical import canonical_hash, freeze_json

from .templates import ExitPlan, StrategyKind, StrategyTemplateRegistry, TemplateLeg, TemplateValidationError


ZERO = Decimal("0")
ONE = Decimal("1")
CENT = Decimal("0.01")
MAX_QUOTE_AGE_SECONDS = Decimal("5")
MINIMUM_VOLUME = 10
MINIMUM_OPEN_INTEREST = 100
MAX_ABSOLUTE_SPREAD = Decimal("0.50")
MAX_RELATIVE_SPREAD = Decimal("0.20")
HARD_RISK_REJECT_FRACTION = Decimal("0.20")


def option_quote_liquidity_assessment(
    value: BatchedOptionQuote,
) -> Mapping[str, object]:
    """Explain the exact per-leg liquidity verdict used by generation."""

    if not isinstance(value, BatchedOptionQuote):
        raise TypeError("option quote must be BatchedOptionQuote")
    bid = value.bid
    ask = value.ask
    reasons: list[str] = []
    spread_absolute: Decimal | None = None
    spread_relative: Decimal | None = None
    if (
        not isinstance(bid, Decimal)
        or not isinstance(ask, Decimal)
        or not bid.is_finite()
        or not ask.is_finite()
        or bid <= ZERO
        or ask <= ZERO
        or bid >= ask
    ):
        reasons.append("QUOTE_NOT_EXECUTABLE")
    else:
        spread_absolute = ask - bid
        spread_relative = spread_absolute / ((ask + bid) / Decimal("2"))
        if (
            spread_absolute > MAX_ABSOLUTE_SPREAD
            or spread_relative > MAX_RELATIVE_SPREAD
        ):
            reasons.append("QUOTE_LIQUIDITY_SPREAD_REJECTED")
    if (
        value.volume is None
        or value.volume < MINIMUM_VOLUME
        or value.open_interest is None
        or value.open_interest < MINIMUM_OPEN_INTEREST
    ):
        reasons.append("QUOTE_LIQUIDITY_VOLUME_OR_OI_REJECTED")
    return MappingProxyType(
        {
            "status": "ELIGIBLE" if not reasons else "REJECTED",
            "reason_codes": tuple(reasons),
            "spread_absolute": spread_absolute,
            "spread_relative": spread_relative,
            "maximum_absolute_spread": MAX_ABSOLUTE_SPREAD,
            "maximum_relative_spread": MAX_RELATIVE_SPREAD,
            "minimum_volume": MINIMUM_VOLUME,
            "minimum_open_interest": MINIMUM_OPEN_INTEREST,
        }
    )


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise ValueError(f"{field} must be a SHA-256 hash")
    return value.lower()


def _string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonblank string")
    return value.strip()


def _decimal(value: object, field: str, *, positive: bool = False) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or (positive and value <= ZERO):
        qualifier = "finite positive Decimal" if positive else "finite Decimal"
        raise ValueError(f"{field} must be a {qualifier}")
    return value


def _input_decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, str)):
        raise ValueError(f"{field} must be Decimal-compatible without float coercion")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite Decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite Decimal")
    return result


def _finalized_scenario(value: object) -> TerminalScenario:
    if isinstance(value, TerminalScenario):
        return value
    if isinstance(value, Mapping):
        price = value.get(
            "terminal_underlying_price",
            value.get("terminal_price"),
        )
        probability = value.get("probability")
    else:
        price = getattr(
            value,
            "terminal_underlying_price",
            getattr(value, "terminal_price", None),
        )
        probability = getattr(value, "probability", None)
    return TerminalScenario(
        _input_decimal(price, "terminal_underlying_price"),
        _input_decimal(probability, "probability"),
    )


def _freeze_underlying_quote_basis(
    value: Mapping[str, object] | None,
    supplied_hash: str | None,
) -> tuple[Mapping[str, object] | None, str | None]:
    """Freeze one live IBKR stock basis into candidate identity evidence."""

    if value is None and supplied_hash is None:
        return None, None
    if value is None or supplied_hash is None:
        raise ValueError(
            "underlying quote basis and hash must be provided together"
        )
    if not isinstance(value, Mapping):
        raise TypeError("underlying quote basis must be a mapping")
    frozen = freeze_json(value)
    if not isinstance(frozen, Mapping):
        raise TypeError("underlying quote basis must freeze to a mapping")
    checked_hash = _hash(supplied_hash, "underlying quote basis hash")
    if canonical_hash(frozen) != checked_hash:
        raise ValueError("underlying quote basis hash does not match")
    if (
        frozen.get("schema") != "options_copilot.underlying_quote_basis.v1"
        or str(frozen.get("source", "")).strip().upper()
        != "IBKR_REQ_TICKERS_READONLY"
        or str(frozen.get("symbol", "")).strip().upper() == ""
        or str(frozen.get("exchange", "")).strip().upper() == ""
    ):
        raise ValueError("underlying quote basis identity is invalid")
    contract_id = frozen.get("contract_id")
    market_data_type = frozen.get("market_data_type")
    if (
        isinstance(contract_id, bool)
        or not isinstance(contract_id, int)
        or contract_id <= 0
        or isinstance(market_data_type, bool)
        or market_data_type != 1
    ):
        raise ValueError("underlying quote basis is not live broker evidence")
    observed_at = frozen.get("observed_at")
    try:
        observed = datetime.fromisoformat(
            str(observed_at).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("underlying quote basis timestamp is invalid") from exc
    _aware(observed, "underlying quote basis observed_at")
    prices: dict[str, Decimal | None] = {}
    for name in ("bid", "ask", "last", "close"):
        raw = frozen.get(name)
        if raw is None:
            prices[name] = None
            continue
        if isinstance(raw, (bool, float)) or not isinstance(
            raw,
            (Decimal, int, str),
        ):
            raise ValueError(f"underlying quote basis {name} is invalid")
        try:
            parsed = raw if isinstance(raw, Decimal) else Decimal(str(raw))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(
                f"underlying quote basis {name} is invalid"
            ) from exc
        if not parsed.is_finite() or parsed <= ZERO:
            raise ValueError(f"underlying quote basis {name} is invalid")
        prices[name] = parsed
    if prices["close"] is None or all(
        prices[name] is None for name in ("bid", "ask", "last")
    ):
        raise ValueError("underlying quote basis prices are incomplete")
    if (
        prices["bid"] is not None
        and prices["ask"] is not None
        and prices["ask"] < prices["bid"]
    ):
        raise ValueError("underlying quote basis market is crossed")
    return frozen, checked_hash


@dataclass(frozen=True, slots=True)
class StrategyGenerationRequest:
    """A finalist expressed only as approved template legs and exit metadata."""

    candidate_id: str
    structure: StrategyKind
    legs: tuple[TemplateLeg, ...]
    exit_plan: ExitPlan
    terminal_scenarios: tuple[TerminalScenario, ...] = ()
    thesis: str = "deterministic finalist"
    outcome_capture_baseline: Mapping[str, object] | None = None
    event_evidence_status: str = "UNAVAILABLE"
    earnings_overlap: bool | None = None
    event_defined: bool = False
    event_evidence_hash: str | None = None
    event_supporting_overlap: bool = False
    event_supporting_hash: str | None = None
    fundamental_supporting_status: str = "DEGRADED"
    fundamental_supporting_hash: str | None = None
    fundamental_supporting_payload: Mapping[str, object] | None = None
    fundamental_supporting_reason_codes: tuple[str, ...] = (
        "FUNDAMENTALS_UNAVAILABLE",
    )
    underlying_quote_basis: Mapping[str, object] | None = None
    underlying_quote_basis_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", _string(self.candidate_id, "candidate_id"))
        if not isinstance(self.structure, StrategyKind):
            try:
                object.__setattr__(self, "structure", StrategyKind(str(self.structure).strip().upper()))
            except ValueError as exc:
                raise ValueError("structure must be a registered StrategyKind") from exc
        try:
            legs = tuple(self.legs)
        except TypeError as exc:
            raise TypeError("legs must be iterable") from exc
        if not legs or not all(isinstance(item, TemplateLeg) for item in legs):
            raise ValueError("legs must contain TemplateLeg values only")
        object.__setattr__(self, "legs", legs)
        if not isinstance(self.exit_plan, ExitPlan):
            raise TypeError("exit_plan must be an ExitPlan")
        scenarios = tuple(self.terminal_scenarios)
        if not all(isinstance(item, TerminalScenario) for item in scenarios):
            raise TypeError("terminal_scenarios must contain TerminalScenario values")
        if scenarios and sum((item.probability for item in scenarios), ZERO) != ONE:
            raise ValueError("terminal scenario probabilities must sum exactly to one")
        object.__setattr__(self, "terminal_scenarios", scenarios)
        object.__setattr__(self, "thesis", _string(self.thesis, "thesis"))
        if self.outcome_capture_baseline is not None:
            if not isinstance(self.outcome_capture_baseline, Mapping):
                raise TypeError("outcome_capture_baseline must be a mapping")
            frozen = freeze_json(self.outcome_capture_baseline)
            assert isinstance(frozen, Mapping)
            object.__setattr__(self, "outcome_capture_baseline", frozen)
        event_status = str(self.event_evidence_status).strip().upper()
        if event_status not in {"AVAILABLE", "UNAVAILABLE"}:
            raise ValueError("event_evidence_status must be AVAILABLE or UNAVAILABLE")
        object.__setattr__(self, "event_evidence_status", event_status)
        if not isinstance(self.event_defined, bool):
            raise TypeError("event_defined must be bool")
        if event_status == "AVAILABLE":
            if not isinstance(self.earnings_overlap, bool):
                raise TypeError("available event evidence requires earnings_overlap bool")
            _hash(self.event_evidence_hash, "event evidence hash")
        elif self.earnings_overlap is not None or self.event_evidence_hash is not None:
            raise ValueError("unavailable event evidence cannot carry derived facts")
        if not isinstance(self.event_supporting_overlap, bool):
            raise TypeError("event_supporting_overlap must be bool")
        if self.event_supporting_hash is not None:
            _hash(self.event_supporting_hash, "event supporting hash")
        if self.event_supporting_overlap and self.event_supporting_hash is None:
            raise ValueError("supporting event overlap requires a supporting hash")
        fundamental_status = str(self.fundamental_supporting_status).strip().upper()
        if fundamental_status not in {"AVAILABLE", "DEGRADED"}:
            raise ValueError("fundamental supporting status is invalid")
        object.__setattr__(self, "fundamental_supporting_status", fundamental_status)
        fundamental_hash = self.fundamental_supporting_hash
        if fundamental_hash is not None:
            _hash(fundamental_hash, "fundamental supporting hash")
        payload = self.fundamental_supporting_payload or {}
        if not isinstance(payload, Mapping):
            raise TypeError("fundamental supporting payload must be a mapping")
        frozen_payload = freeze_json(payload)
        assert isinstance(frozen_payload, Mapping)
        reasons = tuple(
            dict.fromkeys(
                _string(item, "fundamental supporting reason").upper()
                for item in self.fundamental_supporting_reason_codes
            )
        )
        if not reasons:
            raise ValueError("fundamental supporting reasons cannot be empty")
        if fundamental_status == "AVAILABLE" and (
            fundamental_hash is None
            or frozen_payload.get("decision_authority") != "SUPPORTING_ONLY"
        ):
            raise ValueError("available fundamentals require bound supporting evidence")
        object.__setattr__(self, "fundamental_supporting_payload", frozen_payload)
        object.__setattr__(self, "fundamental_supporting_reason_codes", reasons)
        basis, basis_hash = _freeze_underlying_quote_basis(
            self.underlying_quote_basis,
            self.underlying_quote_basis_hash,
        )
        object.__setattr__(self, "underlying_quote_basis", basis)
        object.__setattr__(self, "underlying_quote_basis_hash", basis_hash)


@dataclass(frozen=True, slots=True)
class GeneratedStrategyCandidate:
    """Immutable candidate whose identity includes all authoritative bindings."""

    candidate_id: str
    structure: StrategyKind
    candidate: StrategyCandidate
    debit_usd: Decimal
    credit_usd: Decimal
    all_in_cost_usd: Decimal
    max_loss_usd: Decimal
    max_profit_usd: Decimal | None
    breakevens: tuple[Decimal, ...]
    liquidity_score: Decimal
    exit_plan: ExitPlan
    dte: int
    strategy_nav_usd: Decimal
    strategy_nav_hash: str
    strategy_nav_content_hash: str
    strategy_nav_contract_hash: str
    strategy_nav_ledger_head_hash: str
    strategy_nav_observed_account_nlv: Decimal
    strategy_nav_reconciliation_difference: Decimal
    strategy_nav_asof: datetime
    broker_snapshot_hash: str
    quote_batch_id: str
    secdef_hash: str
    evidence_hashes: Mapping[str, str]
    execution_cost_contract_version: str
    execution_cost_contract_hash: str
    policy_version: str
    policy_hash: str
    dte_exception_hash: str | None
    outcome_capture_plan: Mapping[str, object]
    candidate_hash: str
    event_evidence_status: str = "UNAVAILABLE"
    earnings_overlap: bool | None = None
    event_defined: bool = False
    event_evidence_hash: str | None = None
    event_supporting_overlap: bool = False
    event_supporting_hash: str | None = None
    fundamental_supporting_status: str = "DEGRADED"
    fundamental_supporting_hash: str | None = None
    fundamental_supporting_payload: Mapping[str, object] | None = None
    fundamental_supporting_reason_codes: tuple[str, ...] = (
        "FUNDAMENTALS_UNAVAILABLE",
    )
    underlying_quote_basis: Mapping[str, object] | None = None
    underlying_quote_basis_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_hashes", MappingProxyType(dict(self.evidence_hashes)))
        capture_plan = freeze_json(self.outcome_capture_plan)
        if not isinstance(capture_plan, Mapping):
            raise TypeError("outcome_capture_plan must be a mapping")
        object.__setattr__(self, "outcome_capture_plan", capture_plan)
        event_status = str(self.event_evidence_status).strip().upper()
        if event_status not in {"AVAILABLE", "UNAVAILABLE"}:
            raise ValueError("event_evidence_status must be AVAILABLE or UNAVAILABLE")
        object.__setattr__(self, "event_evidence_status", event_status)
        if not isinstance(self.event_defined, bool):
            raise TypeError("event_defined must be bool")
        if event_status == "AVAILABLE":
            if not isinstance(self.earnings_overlap, bool):
                raise TypeError("available event evidence requires earnings_overlap bool")
            _hash(self.event_evidence_hash, "event evidence hash")
        elif self.earnings_overlap is not None or self.event_evidence_hash is not None:
            raise ValueError("unavailable event evidence cannot carry derived facts")
        if not isinstance(self.event_supporting_overlap, bool):
            raise TypeError("event_supporting_overlap must be bool")
        if self.event_supporting_hash is not None:
            _hash(self.event_supporting_hash, "event supporting hash")
        if self.event_supporting_overlap and self.event_supporting_hash is None:
            raise ValueError("supporting event overlap requires a supporting hash")
        fundamental_status = str(self.fundamental_supporting_status).strip().upper()
        if fundamental_status not in {"AVAILABLE", "DEGRADED"}:
            raise ValueError("fundamental supporting status is invalid")
        object.__setattr__(self, "fundamental_supporting_status", fundamental_status)
        if self.fundamental_supporting_hash is not None:
            _hash(self.fundamental_supporting_hash, "fundamental supporting hash")
        fundamental_payload = self.fundamental_supporting_payload or {}
        if not isinstance(fundamental_payload, Mapping):
            raise TypeError("fundamental supporting payload must be a mapping")
        frozen_fundamentals = freeze_json(fundamental_payload)
        assert isinstance(frozen_fundamentals, Mapping)
        fundamental_reasons = tuple(
            dict.fromkeys(
                _string(item, "fundamental supporting reason").upper()
                for item in self.fundamental_supporting_reason_codes
            )
        )
        if not fundamental_reasons:
            raise ValueError("fundamental supporting reasons cannot be empty")
        if fundamental_status == "AVAILABLE" and (
            self.fundamental_supporting_hash is None
            or frozen_fundamentals.get("decision_authority") != "SUPPORTING_ONLY"
        ):
            raise ValueError("available fundamentals require bound supporting evidence")
        object.__setattr__(self, "fundamental_supporting_payload", frozen_fundamentals)
        object.__setattr__(self, "fundamental_supporting_reason_codes", fundamental_reasons)
        basis, basis_hash = _freeze_underlying_quote_basis(
            self.underlying_quote_basis,
            self.underlying_quote_basis_hash,
        )
        if basis is not None:
            symbols = {
                item.leg.contract.underlying.upper()
                for item in self.candidate.leg_quotes
            }
            if symbols != {str(basis.get("symbol", "")).strip().upper()}:
                raise ValueError(
                    "underlying quote basis does not match candidate legs"
                )
        object.__setattr__(self, "underlying_quote_basis", basis)
        object.__setattr__(self, "underlying_quote_basis_hash", basis_hash)
        if self.candidate_hash != "0" * 64:
            if _hash(self.candidate_hash, "candidate hash") != canonical_hash(self.hash_payload()):
                raise ValueError("candidate hash does not bind candidate content")

    def hash_payload(self) -> dict[str, object]:
        symbols = {
            item.leg.contract.underlying for item in self.candidate.leg_quotes
        }
        if len(symbols) != 1:
            raise ValueError("candidate legs must share one underlying symbol")
        return {
            "candidate_id": self.candidate_id,
            "symbol": next(iter(symbols)),
            "structure": self.structure.value,
            "legs": [
                {
                    "con_id": item.leg.contract.broker_contract_id,
                    "contract_id_ex": (
                        f"{item.leg.contract.broker_contract_id}@"
                        f"{item.leg.contract.exchange}"
                    ),
                    "underlying": item.leg.contract.underlying,
                    "security_type": item.leg.contract.security_type,
                    "expiration": item.leg.contract.expiration.isoformat(),
                    "strike": str(item.leg.contract.strike),
                    "right": item.leg.contract.right.value,
                    "side": item.leg.side.value,
                    "ratio": item.leg.quantity,
                    "multiplier": str(item.leg.contract.multiplier),
                    "currency": item.leg.contract.currency,
                    "exchange": item.leg.contract.exchange,
                    "bid": str(item.bid),
                    "ask": str(item.ask),
                    "last": None if item.last is None else str(item.last),
                    "implied_volatility": (
                        None
                        if item.implied_volatility is None
                        else str(item.implied_volatility)
                    ),
                    "volume": item.volume,
                    "open_interest": item.open_interest,
                    "observed_at": item.observed_at.isoformat(),
                    "delta": None if item.delta is None else str(item.delta),
                    "gamma": None if item.gamma is None else str(item.gamma),
                    "theta": None if item.theta is None else str(item.theta),
                    "vega": None if item.vega is None else str(item.vega),
                    "exchange_time": (
                        None
                        if item.exchange_time is None
                        else item.exchange_time.isoformat()
                    ),
                    "requested_at": (
                        None
                        if item.requested_at is None
                        else item.requested_at.isoformat()
                    ),
                    "completed_at": (
                        None
                        if item.completed_at is None
                        else item.completed_at.isoformat()
                    ),
                    "market_data_type": item.market_data_type,
                    "quote_age_seconds": (
                        None
                        if item.quote_age_seconds is None
                        else str(item.quote_age_seconds)
                    ),
                    "freshness_basis": item.freshness_basis,
                    "short_leg_risk_evidence": {
                        "status": item.short_leg_risk_evidence_status,
                        "reason_codes": list(
                            item.short_leg_risk_evidence_reason_codes
                        ),
                        "evidence_hash": item.short_leg_risk_evidence_hash,
                    },
                }
                for item in self.candidate.leg_quotes
            ],
            "terminal_scenarios": [
                {
                    "terminal_underlying_price": str(item.terminal_underlying_price),
                    "probability": str(item.probability),
                }
                for item in self.candidate.terminal_scenarios
            ],
            "risk_tier": self.candidate.risk_tier.value,
            "estimated_commissions_usd": str(
                self.candidate.estimated_commissions
            ),
            "estimated_slippage_usd": str(self.candidate.estimated_slippage),
            "debit_usd": str(self.debit_usd),
            "credit_usd": str(self.credit_usd),
            "all_in_cost_usd": str(self.all_in_cost_usd),
            "max_loss_usd": str(self.max_loss_usd),
            "max_profit_usd": None if self.max_profit_usd is None else str(self.max_profit_usd),
            "max_profit_type": "UNBOUNDED" if self.max_profit_usd is None else "FINITE",
            "breakevens": [str(item) for item in self.breakevens],
            "liquidity_score": str(self.liquidity_score),
            "exit_plan": self.exit_plan.as_dict(),
            "dte": self.dte,
            "strategy_nav_usd": str(self.strategy_nav_usd),
            "strategy_nav_hash": self.strategy_nav_hash,
            "strategy_nav_content_hash": self.strategy_nav_content_hash,
            "strategy_nav_contract_hash": self.strategy_nav_contract_hash,
            "strategy_nav_ledger_head_hash": self.strategy_nav_ledger_head_hash,
            "strategy_nav_observed_account_nlv": str(
                self.strategy_nav_observed_account_nlv
            ),
            "strategy_nav_reconciliation_difference": str(
                self.strategy_nav_reconciliation_difference
            ),
            "strategy_nav_asof": self.strategy_nav_asof.isoformat(),
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "quote_batch_id": self.quote_batch_id,
            "secdef_hash": self.secdef_hash,
            "evidence_hashes": dict(sorted(self.evidence_hashes.items())),
            "execution_cost_contract_version": self.execution_cost_contract_version,
            "execution_cost_contract_hash": self.execution_cost_contract_hash,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "dte_exception_hash": self.dte_exception_hash,
            "outcome_capture_plan": self.outcome_capture_plan,
            "event_evidence_status": self.event_evidence_status,
            "earnings_overlap": self.earnings_overlap,
            "event_defined": self.event_defined,
            "event_evidence_hash": self.event_evidence_hash,
            "event_supporting_overlap": self.event_supporting_overlap,
            "event_supporting_hash": self.event_supporting_hash,
            "fundamental_supporting_status": self.fundamental_supporting_status,
            "fundamental_supporting_hash": self.fundamental_supporting_hash,
            "fundamental_supporting_payload": self.fundamental_supporting_payload,
            "fundamental_supporting_reason_codes": self.fundamental_supporting_reason_codes,
            "underlying_quote_basis": self.underlying_quote_basis,
            "underlying_quote_basis_hash": self.underlying_quote_basis_hash,
        }

    def finalize_scenarios(
        self,
        scenarios: Iterable[TerminalScenario | Mapping[str, object] | object],
    ) -> "GeneratedStrategyCandidate":
        """Return a new candidate bound to trusted scenario-engine output.

        The generator runs before scenario analysis, so finalist-supplied
        terminal probabilities cannot be the final authority.  This pure seam
        preserves every non-scenario field, replaces only the terminal
        distribution, and recomputes the candidate hash without mutating the
        generator result.
        """

        if (
            self.candidate_hash == "0" * 64
            or canonical_hash(self.hash_payload()) != self.candidate_hash
        ):
            raise ValueError("candidate hash does not bind pre-scenario content")
        try:
            normalized = tuple(_finalized_scenario(value) for value in scenarios)
        except TypeError as exc:
            raise TypeError("scenarios must be iterable") from exc
        if not normalized:
            raise ValueError("trusted terminal scenarios are incomplete")
        if len({item.terminal_underlying_price for item in normalized}) != len(
            normalized
        ):
            raise ValueError("trusted terminal scenarios contain duplicate prices")
        if normalized == self.candidate.terminal_scenarios:
            return self

        finalized_candidate = replace(
            self.candidate,
            terminal_scenarios=normalized,
        )
        provisional = replace(
            self,
            candidate=finalized_candidate,
            candidate_hash="0" * 64,
        )
        return replace(
            provisional,
            candidate_hash=canonical_hash(provisional.hash_payload()),
        )

    def proposal_payload(self) -> dict[str, object]:
        """Build one detached, deterministic, review-only proposal.

        This is deliberately not an instruction or an order intent.  Every
        executable price, contract identity, and risk number is regenerated
        from the immutable candidate and then checked against its candidate
        hash before any proposal is returned.
        """

        expected_value = self._require_authoritative_source()
        quoted_legs = self.candidate.leg_quotes
        symbol = quoted_legs[0].contract.underlying
        expiration = quoted_legs[0].contract.expiration
        reference_cost = self.debit_usd - self.credit_usd
        execution_costs = (
            self.candidate.estimated_commissions
            + self.candidate.estimated_slippage
        )
        risk_fraction = self.max_loss_usd / self.strategy_nav_usd
        pricing: dict[str, object] = {
            "reference_cost_usd": _decimal_text(reference_cost),
            "estimated_commissions_usd": _decimal_text(
                self.candidate.estimated_commissions
            ),
            "estimated_slippage_usd": _decimal_text(
                self.candidate.estimated_slippage
            ),
            "estimated_execution_costs_usd": _decimal_text(execution_costs),
            "all_in_executable_cost_usd": _decimal_text(self.all_in_cost_usd),
        }
        if reference_cost >= ZERO:
            pricing["net_debit_usd"] = _decimal_text(reference_cost)
        else:
            pricing["net_credit_usd"] = _decimal_text(-reference_cost)
        legs: list[dict[str, object]] = []
        for quoted_leg in quoted_legs:
            contract = quoted_leg.contract
            leg = quoted_leg.leg
            assert contract.broker_contract_id is not None
            row: dict[str, object] = {
                "con_id": contract.broker_contract_id,
                "contract_id_ex": (
                    f"{contract.broker_contract_id}@{contract.exchange}"
                ),
                "underlying": contract.underlying,
                "security_type": contract.security_type,
                "expiration": contract.expiration.isoformat(),
                "strike": _decimal_text(contract.strike),
                "right": contract.right.value,
                "side": "BUY" if leg.side is PositionSide.LONG else "SELL",
                "quantity": leg.quantity,
                "ratio": leg.quantity,
                "multiplier": _decimal_text(contract.multiplier),
                "currency": contract.currency,
                "exchange": contract.exchange,
                "bid": _decimal_text(quoted_leg.bid),
                "ask": _decimal_text(quoted_leg.ask),
                "quote_time": _datetime_text(quoted_leg.observed_at),
                "quote_snapshot_id": self.quote_batch_id,
                "delta": _decimal_text(quoted_leg.delta),
                "gamma": _decimal_text(quoted_leg.gamma),
                "theta": _decimal_text(quoted_leg.theta),
                "vega": _decimal_text(quoted_leg.vega),
                "exchange_time": _datetime_text(quoted_leg.exchange_time),
                "requested_at": _datetime_text(quoted_leg.requested_at),
                "completed_at": _datetime_text(quoted_leg.completed_at),
                "market_data_type": quoted_leg.market_data_type,
                "quote_age_seconds": _decimal_text(
                    quoted_leg.quote_age_seconds
                ),
                "freshness_basis": quoted_leg.freshness_basis,
                "short_leg_risk_evidence": {
                    "status": quoted_leg.short_leg_risk_evidence_status,
                    "reason_codes": list(
                        quoted_leg.short_leg_risk_evidence_reason_codes
                    ),
                    "evidence_hash": quoted_leg.short_leg_risk_evidence_hash,
                },
            }
            if quoted_leg.last is not None:
                row["last"] = _decimal_text(quoted_leg.last)
            if quoted_leg.implied_volatility is not None:
                row["implied_volatility"] = _decimal_text(
                    quoted_leg.implied_volatility
                )
            if quoted_leg.volume is not None:
                row["volume"] = quoted_leg.volume
            if quoted_leg.open_interest is not None:
                row["open_interest"] = quoted_leg.open_interest
            legs.append(row)

        return {
            "schema": "options_copilot.proposal.v1",
            "review_only": True,
            # The immutable proposal validator accepts rank-one payloads only.
            # RankingStore remains the sole authority that decides which one
            # of these review-only candidates actually occupies rank one.
            "rank": 1,
            "eligible_to_send": True,
            "proposal_id": self.candidate_id,
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "symbol": symbol,
            "underlying": symbol,
            "structure": self.structure.value,
            "expiration": expiration.isoformat(),
            "dte": self.dte,
            "quote_snapshot_id": self.quote_batch_id,
            "expected_value_usd": _decimal_text(expected_value),
            "expected_value_before_costs_usd": _decimal_text(
                expected_value + execution_costs
            ),
            "terminal_scenarios": [
                {
                    "terminal_underlying_price": _decimal_text(
                        item.terminal_underlying_price
                    ),
                    "probability": _decimal_text(item.probability),
                }
                for item in self.candidate.terminal_scenarios
            ],
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "secdef_hash": self.secdef_hash,
            "strategy_nav": {
                "strategy_nav_usd": _decimal_text(self.strategy_nav_usd),
                "authority_hash": self.strategy_nav_hash,
                "content_hash": self.strategy_nav_content_hash,
                "contract_hash": self.strategy_nav_contract_hash,
                "ledger_head_hash": self.strategy_nav_ledger_head_hash,
                "observed_account_nlv": _decimal_text(
                    self.strategy_nav_observed_account_nlv
                ),
                "reconciliation_difference": _decimal_text(
                    self.strategy_nav_reconciliation_difference
                ),
                "asof": _datetime_text(self.strategy_nav_asof),
            },
            "policy": {
                "version": self.policy_version,
                "hash": self.policy_hash,
                "dte_exception_hash": self.dte_exception_hash,
            },
            "execution_cost_contract": {
                "version": self.execution_cost_contract_version,
                "hash": self.execution_cost_contract_hash,
            },
            "evidence_hashes": dict(sorted(self.evidence_hashes.items())),
            "legs": legs,
            "pricing": pricing,
            "risk": {
                "defined_risk": True,
                "risk_tier": self.candidate.risk_tier.value,
                "maximum_loss_usd": _decimal_text(self.max_loss_usd),
                "maximum_profit_usd": (
                    None
                    if self.max_profit_usd is None
                    else _decimal_text(self.max_profit_usd)
                ),
                "risk_fraction": _decimal_text(risk_fraction),
                "breakevens": [
                    _decimal_text(item) for item in self.breakevens
                ],
            },
            "exit_plan": self.exit_plan.as_dict(),
        }

    def _require_authoritative_source(self) -> Decimal:
        if self.candidate_hash == "0" * 64:
            raise ValueError("candidate hash is incomplete")
        supplied_hash = _hash(self.candidate_hash, "candidate hash")
        if supplied_hash != canonical_hash(self.hash_payload()):
            raise ValueError("candidate hash does not bind candidate content")
        if self.candidate.candidate_id != self.candidate_id:
            raise ValueError("candidate identity is inconsistent")
        if not isinstance(self.structure, StrategyKind):
            raise ValueError("candidate structure is invalid")
        if (
            isinstance(self.dte, bool)
            or not isinstance(self.dte, int)
            or self.dte < 7
        ):
            raise ValueError("candidate DTE violates the permanent floor")
        nav = _decimal(self.strategy_nav_usd, "strategy NAV", positive=True)
        _hash(self.strategy_nav_hash, "strategy NAV hash")
        _hash(self.strategy_nav_content_hash, "strategy NAV content hash")
        _hash(self.strategy_nav_contract_hash, "strategy NAV contract hash")
        _hash(self.strategy_nav_ledger_head_hash, "strategy NAV ledger head hash")
        _decimal(
            self.strategy_nav_observed_account_nlv,
            "strategy NAV observed account NLV",
            positive=True,
        )
        _decimal(
            self.strategy_nav_reconciliation_difference,
            "strategy NAV reconciliation difference",
        )
        _aware(self.strategy_nav_asof, "strategy NAV asof")
        _hash(self.broker_snapshot_hash, "broker snapshot hash")
        _hash(self.secdef_hash, "secdef hash")
        _hash(self.execution_cost_contract_hash, "execution cost contract hash")
        _hash(self.policy_hash, "policy hash")
        _string(self.quote_batch_id, "quote batch id")
        _string(
            self.execution_cost_contract_version,
            "execution cost contract version",
        )
        _string(self.policy_version, "policy version")
        if self.dte_exception_hash is not None:
            _hash(self.dte_exception_hash, "DTE exception hash")
        self._validate_evidence_hashes()

        quoted_legs = self.candidate.leg_quotes
        if not quoted_legs:
            raise ValueError("candidate legs are incomplete")
        if len({item.contract.expiration for item in quoted_legs}) != 1:
            raise ValueError(
                "candidate expirations cannot pass the current proposal validator"
            )
        contract_ids: set[int] = set()
        for index, quoted_leg in enumerate(quoted_legs):
            contract = quoted_leg.contract
            con_id = contract.broker_contract_id
            if (
                isinstance(con_id, bool)
                or not isinstance(con_id, int)
                or con_id <= 0
                or con_id in contract_ids
            ):
                raise ValueError("candidate broker contract identities are invalid")
            contract_ids.add(con_id)
            if (
                contract.security_type != "OPT"
                or contract.multiplier != Decimal("100")
                or contract.currency != "USD"
            ):
                raise ValueError("candidate contains a nonstandard option contract")
            bid = _decimal(quoted_leg.bid, f"legs[{index}].bid", positive=True)
            ask = _decimal(quoted_leg.ask, f"legs[{index}].ask", positive=True)
            if bid >= ask:
                raise ValueError("candidate executable quote is invalid")
            _aware(quoted_leg.observed_at, f"legs[{index}].observed_at")
        StrategyTemplateRegistry().validate(
            self.structure,
            tuple(item.leg for item in quoted_legs),
        )

        debit, credit = StrategyCandidateGenerator._premium(quoted_legs)
        if debit != _decimal(self.debit_usd, "debit_usd"):
            raise ValueError("candidate debit does not match executable legs")
        if credit != _decimal(self.credit_usd, "credit_usd"):
            raise ValueError("candidate credit does not match executable legs")
        execution_costs = (
            self.candidate.estimated_commissions
            + self.candidate.estimated_slippage
        )
        if debit - credit + execution_costs != _decimal(
            self.all_in_cost_usd, "all_in_cost_usd"
        ):
            raise ValueError("candidate all-in cost does not match executable legs")

        payoff = analyze_expiration_payoff(self.candidate)
        if payoff.status is not PayoffStatus.CALCULATED or payoff.max_loss is None:
            raise ValueError("candidate maximum loss is not exactly computable")
        maximum_loss = _decimal(self.max_loss_usd, "maximum loss")
        if payoff.max_loss != maximum_loss:
            raise ValueError("candidate maximum loss does not match exact payoff")
        if payoff.max_profit != self.max_profit_usd:
            raise ValueError("candidate maximum profit does not match exact payoff")
        if payoff.breakevens != self.breakevens:
            raise ValueError("candidate breakevens do not match exact payoff")
        if not self.candidate.terminal_scenarios:
            raise ValueError("candidate terminal scenarios are incomplete")
        expected_value = sum(
            (
                item.probability
                * payoff.pnl_at(item.terminal_underlying_price)
                for item in self.candidate.terminal_scenarios
            ),
            ZERO,
        )
        if expected_value <= ZERO:
            raise ValueError("candidate terminal scenarios have nonpositive after-cost EV")
        liquidity = _decimal(self.liquidity_score, "liquidity score")
        if liquidity < ZERO or liquidity > ONE:
            raise ValueError("candidate liquidity score is outside zero to one")
        if maximum_loss / nav >= HARD_RISK_REJECT_FRACTION:
            raise ValueError("candidate risk reaches the absolute 20 percent reject line")
        return expected_value

    def _validate_evidence_hashes(self) -> None:
        if not isinstance(self.evidence_hashes, Mapping):
            raise ValueError("candidate evidence hashes are unavailable")
        normalized = {
            str(key).upper(): _hash(value, f"evidence hash {key}")
            for key, value in self.evidence_hashes.items()
        }
        if not {"MARKET", "VOLATILITY", "LIQUIDITY"}.issubset(normalized):
            raise ValueError("candidate evidence hashes are incomplete")


@dataclass(frozen=True, slots=True)
class GenerationResult:
    status: str
    candidates: tuple[GeneratedStrategyCandidate, ...]
    reason_codes: tuple[str, ...]

    @property
    def no_trade(self) -> bool:
        return self.status == "NO_TRADE"


class StrategyCandidateGenerator:
    """Validate P4 finalists without talking to any external system."""

    def __init__(self, registry: StrategyTemplateRegistry | None = None, time_policy: OptionTimePolicy | None = None) -> None:
        self.registry = registry or StrategyTemplateRegistry()
        self.time_policy = time_policy or OptionTimePolicy()

    def generate(
        self,
        finalists: Iterable[StrategyGenerationRequest | Mapping[str, object]],
        *,
        secdefs: Iterable[OptionSecDefSnapshot],
        quote_batch: OptionQuoteBatch,
        nav_snapshot: StrategyNavSnapshot,
        broker_snapshot: AtomicBrokerSnapshot,
        execution_cost_contract: Mapping[str, object],
        policy_contract: Mapping[str, object],
        evidence_hashes: Mapping[str, str],
        now: datetime,
        positions: Iterable[object] = (),
        dte_exception_authority: DteEntryExceptionAuthority | None = None,
    ) -> GenerationResult:
        """Return candidates or structured ``NO_TRADE`` without partial output."""

        try:
            checked_now = _aware(now, "now")
            if self._option_position_open(positions):
                return self._no_trade("POSITION_MANAGEMENT_ONLY")
            requests = tuple(self._request(value) for value in finalists)
            if not requests:
                return self._no_trade("FINALIST_CHAIN_EMPTY")
            binding = self._bindings(
                secdefs=tuple(secdefs), quote_batch=quote_batch, nav_snapshot=nav_snapshot,
                broker_snapshot=broker_snapshot, execution_cost_contract=execution_cost_contract,
                policy_contract=policy_contract, evidence_hashes=evidence_hashes, now=checked_now,
            )
        except (TypeError, ValueError) as exc:
            return self._no_trade(self._reason(exc))

        candidates: list[GeneratedStrategyCandidate] = []
        errors: list[str] = []
        # A long option is a bounded fallback, never a competing default when a
        # finite-risk combination has survived the identical hard gates.
        combinations = tuple(item for item in requests if item.structure is not StrategyKind.LONG_OPTION)
        long_options = tuple(item for item in requests if item.structure is StrategyKind.LONG_OPTION)
        for request in combinations:
            try:
                candidates.append(self._generate_one(request, binding, checked_now, dte_exception_authority))
            except (TypeError, ValueError, TemplateValidationError) as exc:
                errors.append(self._reason(exc))
        if not candidates:
            for request in long_options:
                try:
                    candidates.append(self._generate_one(request, binding, checked_now, dte_exception_authority))
                except (TypeError, ValueError, TemplateValidationError) as exc:
                    errors.append(self._reason(exc))
        if not candidates:
            return GenerationResult("NO_TRADE", (), tuple(sorted(set(errors))) or ("NO_ELIGIBLE_TEMPLATE",))
        return GenerationResult("CANDIDATES", tuple(candidates), ())

    generate_from_finalists = generate

    def _generate_one(self, request: StrategyGenerationRequest, binding: Mapping[str, Any], now: datetime, authority: DteEntryExceptionAuthority | None) -> GeneratedStrategyCandidate:
        secdefs: Mapping[int, OptionSecDefSnapshot] = binding["secdefs"]
        quotes: Mapping[int, BatchedOptionQuote] = binding["quotes"]
        legs: list[OptionLeg] = []
        quoted_legs: list[OptionLegQuote] = []
        for item in request.legs:
            secdef = secdefs.get(item.con_id)
            quote = quotes.get(item.con_id)
            if secdef is None or quote is None:
                raise ValueError("MISSING_CONID_SECDEF_OR_QUOTE")
            if not secdef.standard_contract or secdef.adjusted or secdef.security_type != "OPT" or secdef.currency != "USD" or secdef.multiplier != 100:
                raise ValueError("SECDEF_NONSTANDARD_OR_ADJUSTED")
            secdef_evidence = binding["secdef_evidence"].get(item.con_id)
            if secdef_evidence is None or not secdef_evidence.stable or not secdef_evidence.standard_contract or secdef_evidence.adjusted:
                raise ValueError("SECDEF_EVIDENCE_MISSING_OR_UNSTABLE")
            quote_age = self._quote_ok(quote, now)
            contract = self._contract(secdef)
            leg = OptionLeg(contract, item.side, item.ratio)
            legs.append(leg)
            short_leg_evidence = binding["short_leg_risk_evidence"]
            if (
                item.side is PositionSide.SHORT
                and short_leg_evidence["status"] != "SUPPORTED"
            ):
                raise ValueError(
                    "ASSIGNMENT_EXERCISE_EX_DIVIDEND_EVIDENCE_UNAVAILABLE"
                )
            quoted_legs.append(
                OptionLegQuote(
                    leg=leg,
                    bid=quote.bid,
                    ask=quote.ask,
                    last=quote.last,
                    implied_volatility=quote.implied_volatility,
                    volume=quote.volume,
                    open_interest=quote.open_interest,
                    observed_at=quote.observed_at,
                    delta=quote.delta,
                    gamma=quote.gamma,
                    theta=quote.theta,
                    vega=quote.vega,
                    exchange_time=quote.exchange_time,
                    requested_at=quote.requested_at,
                    completed_at=quote.completed_at,
                    market_data_type=quote.market_data_type,
                    quote_age_seconds=quote_age,
                    freshness_basis="EXCHANGE_TIME",
                    short_leg_risk_evidence_status=(
                        short_leg_evidence["status"]
                        if item.side is PositionSide.SHORT
                        else "NOT_APPLICABLE"
                    ),
                    short_leg_risk_evidence_reason_codes=(
                        short_leg_evidence["reason_codes"]
                        if item.side is PositionSide.SHORT
                        else ()
                    ),
                    short_leg_risk_evidence_hash=(
                        short_leg_evidence["evidence_hash"]
                        if item.side is PositionSide.SHORT
                        else None
                    ),
                )
            )
        self.registry.validate(request.structure, legs)
        expiration = min(leg.contract.expiration for leg in legs)
        dte = self.time_policy.evaluate_dte(proposal_id=request.candidate_id, expiration=expiration, now=now, exception_authority=authority)
        if not dte.allowed:
            raise ValueError("DTE_" + "_".join(dte.reason_codes))
        if request.exit_plan.maximum_holding_date < now.date() or request.exit_plan.maximum_holding_date > expiration:
            raise ValueError("EXIT_PLAN_HOLDING_DATE_INVALID")
        costs = self._costs(quoted_legs)
        candidate = StrategyCandidate(request.candidate_id, tuple(quoted_legs), request.terminal_scenarios, costs["commission"], costs["slippage"])
        payoff = analyze_expiration_payoff(candidate)
        if payoff.status is not PayoffStatus.CALCULATED or payoff.max_loss is None:
            raise ValueError("PAYOFF_NOT_EXACTLY_COMPUTABLE")
        debit, credit = self._premium(quoted_legs)
        if request.structure is StrategyKind.DEBIT_VERTICAL and debit <= credit:
            raise ValueError("DEBIT_VERTICAL_MUST_HAVE_NET_DEBIT")
        if request.structure is StrategyKind.CREDIT_VERTICAL and credit <= debit:
            raise ValueError("CREDIT_VERTICAL_MUST_HAVE_NET_CREDIT")
        liquidity = self._liquidity(quoted_legs)
        exception_hash = None if dte.exception_hash is None else _hash(dte.exception_hash, "dte exception hash")
        provisional = {
            "candidate_id": request.candidate_id, "structure": request.structure, "candidate": candidate,
            "debit_usd": debit, "credit_usd": credit, "all_in_cost_usd": debit - credit + costs["commission"] + costs["slippage"],
            "max_loss_usd": payoff.max_loss, "max_profit_usd": payoff.max_profit, "breakevens": payoff.breakevens,
            "liquidity_score": liquidity, "exit_plan": request.exit_plan, "dte": dte.dte,
            "strategy_nav_usd": binding["strategy_nav"], "strategy_nav_hash": binding["nav_hash"], "strategy_nav_content_hash": binding["nav_content_hash"], "strategy_nav_contract_hash": binding["nav_contract_hash"],
            "strategy_nav_ledger_head_hash": binding["nav_ledger_head_hash"], "strategy_nav_observed_account_nlv": binding["nav_observed_account_nlv"], "strategy_nav_reconciliation_difference": binding["nav_reconciliation_difference"], "strategy_nav_asof": binding["nav_asof"],
            "broker_snapshot_hash": binding["broker_hash"], "quote_batch_id": binding["quote_batch_id"], "secdef_hash": binding["secdef_hash"],
            "evidence_hashes": binding["evidence_hashes"], "execution_cost_contract_version": binding["cost_version"],
            "execution_cost_contract_hash": binding["cost_hash"], "policy_version": binding["policy_version"], "policy_hash": binding["policy_hash"], "dte_exception_hash": exception_hash,
            "outcome_capture_plan": self._outcome_capture_plan(
                request,
                quoted_legs=tuple(quoted_legs),
                secdefs=secdefs,
                maximum_loss=payoff.max_loss,
                broker_snapshot_hash=binding["broker_hash"],
            ),
            "event_evidence_status": request.event_evidence_status,
            "earnings_overlap": request.earnings_overlap,
            "event_defined": request.event_defined,
            "event_evidence_hash": request.event_evidence_hash,
            "event_supporting_overlap": request.event_supporting_overlap,
            "event_supporting_hash": request.event_supporting_hash,
            "fundamental_supporting_status": request.fundamental_supporting_status,
            "fundamental_supporting_hash": request.fundamental_supporting_hash,
            "fundamental_supporting_payload": request.fundamental_supporting_payload,
            "fundamental_supporting_reason_codes": request.fundamental_supporting_reason_codes,
            "underlying_quote_basis": request.underlying_quote_basis,
            "underlying_quote_basis_hash": request.underlying_quote_basis_hash,
        }
        # Canonical decimal/domain serialization is intentionally constructed above
        # through GeneratedStrategyCandidate.hash_payload rather than accepting input.
        temporary = GeneratedStrategyCandidate(**provisional, candidate_hash="0" * 64)
        return GeneratedStrategyCandidate(**provisional, candidate_hash=canonical_hash(temporary.hash_payload()))

    def _bindings(self, *, secdefs: tuple[OptionSecDefSnapshot, ...], quote_batch: OptionQuoteBatch, nav_snapshot: StrategyNavSnapshot, broker_snapshot: AtomicBrokerSnapshot, execution_cost_contract: Mapping[str, object], policy_contract: Mapping[str, object], evidence_hashes: Mapping[str, str], now: datetime) -> Mapping[str, Any]:
        if not isinstance(broker_snapshot, AtomicBrokerSnapshot) or not broker_snapshot.complete or not broker_snapshot.verify_hash():
            raise ValueError("BROKER_SNAPSHOT_INVALID")
        broker_hash = _hash(broker_snapshot.snapshot_hash, "broker snapshot hash")
        try:
            broker_nlv = atomic_account_nlv(broker_snapshot)
        except (TypeError, ValueError) as exc:
            raise ValueError("BROKER_SNAPSHOT_ACCOUNT_NLV_INVALID") from exc
        if not isinstance(nav_snapshot, StrategyNavSnapshot) or not nav_snapshot.valid or nav_snapshot.strategy_nav is None:
            raise ValueError("STRATEGY_NAV_INVALID")
        if nav_snapshot.asof != broker_snapshot.built_at:
            raise ValueError("STRATEGY_NAV_BROKER_TIME_MISMATCH")
        nav_hash = _hash(nav_snapshot.authority_hash, "strategy NAV authority hash")
        nav_content_hash = _hash(
            nav_snapshot.content_hash,
            "strategy NAV content hash",
        )
        if canonical_hash(nav_snapshot.hash_payload()) != nav_content_hash:
            raise ValueError("STRATEGY_NAV_CONTENT_HASH_INVALID")
        nav_contract_hash = _hash(nav_snapshot.contract_hash, "strategy NAV contract hash")
        nav_ledger_head_hash = _hash(
            nav_snapshot.ledger_head_hash,
            "strategy NAV ledger head hash",
        )
        observed_nlv = _decimal(
            nav_snapshot.observed_account_nlv,
            "strategy NAV observed account NLV",
            positive=True,
        )
        if observed_nlv != broker_nlv:
            raise ValueError("STRATEGY_NAV_BROKER_NLV_MISMATCH")
        reconciliation = _decimal(
            nav_snapshot.reconciliation_difference,
            "strategy NAV reconciliation difference",
        )
        expected_reconciliation = (
            observed_nlv - nav_snapshot.strategy_nav
        ).quantize(CENT, rounding=ROUND_HALF_EVEN)
        if reconciliation != expected_reconciliation:
            raise ValueError("STRATEGY_NAV_RECONCILIATION_INVALID")
        if not isinstance(quote_batch, OptionQuoteBatch) or quote_batch.status is not QuoteBatchStatus.COMPLETE:
            raise ValueError("QUOTE_BATCH_INCOMPLETE")
        quote_batch_id = _string(quote_batch.batch_id, "quote batch id")
        if broker_snapshot.quote_batch_id != quote_batch_id:
            raise ValueError("QUOTE_BATCH_BROKER_SNAPSHOT_MISMATCH")
        if any(item.batch_id != quote_batch_id for item in quote_batch.quotes):
            raise ValueError("QUOTE_BATCH_INCOHERENT")
        if not isinstance(execution_cost_contract, Mapping) or not isinstance(policy_contract, Mapping):
            raise ValueError("SIGNED_CONTRACT_BINDING_MISSING")
        self._signed_contract(execution_cost_contract, "execution cost contract", now)
        self._signed_contract(policy_contract, "policy contract", now)
        cost_version = _string(execution_cost_contract.get("version"), "execution cost contract version")
        cost_hash = _hash(execution_cost_contract.get("contract_hash"), "execution cost contract hash")
        cost_payload = execution_cost_contract.get("payload")
        if not isinstance(cost_payload, Mapping) or not all(
            isinstance(cost_payload.get(key), Mapping)
            for key in ("commission_and_fees", "quote_spread_and_slippage", "assignment_exercise_and_dividend")
        ):
            raise ValueError("EXECUTION_COST_CONTRACT_INVALID")
        assignment_payload = cost_payload["assignment_exercise_and_dividend"]
        assert isinstance(assignment_payload, Mapping)
        assignment_status = str(assignment_payload.get("status", "")).strip().upper()
        assignment_proof_hash = assignment_payload.get("evidence_hash")
        signed_policy_complete = all(
            isinstance(assignment_payload.get(key), Mapping)
            and bool(assignment_payload.get(key))
            for key in ("assignment", "exercise", "early_exercise", "ex_dividend")
        ) and isinstance(assignment_payload.get("short_leg_exit_deadline"), str)
        legacy_v1_verified = False
        if assignment_status == "" and signed_policy_complete and cost_version == "v1":
            try:
                verify_contract(
                    execution_cost_contract,
                    expected_kind=ContractKind.EXECUTION_COST,
                    expected_version="v1",
                    expected_hash=cost_hash,
                    as_of=now,
                )
            except ContractValidationError:
                legacy_v1_verified = False
            else:
                legacy_v1_verified = True
        if assignment_status == "" and signed_policy_complete and legacy_v1_verified:
            # The installed v1 human-signed authority predates the compact
            # status/evidence wrapper.  Its complete bounded policy is itself
            # hash-bound by the execution-cost contract and is not optional
            # provider evidence.
            assignment_status = "SUPPORTED"
            assignment_proof_hash = canonical_hash(assignment_payload)
        if assignment_status == "SUPPORTED":
            try:
                _hash(assignment_proof_hash, "assignment/exercise/dividend evidence hash")
            except ValueError:
                assignment_status = "UNSUPPORTED"
        if assignment_status == "SUPPORTED":
            assignment_reasons: tuple[str, ...] = ()
        else:
            assignment_status = "UNSUPPORTED"
            assignment_reasons = (
                "ASSIGNMENT_EXERCISE_EX_DIVIDEND_EVIDENCE_UNAVAILABLE",
            )
        assignment_binding_hash = canonical_hash(
            {
                "execution_cost_contract_hash": cost_hash,
                "assignment_exercise_and_dividend": assignment_payload,
            }
        )
        policy_version = _string(policy_contract.get("version"), "policy version")
        policy_hash = _hash(policy_contract.get("contract_hash"), "policy hash")
        policy_payload = policy_contract.get("payload")
        if not isinstance(policy_payload, Mapping):
            raise ValueError("POLICY_CONTRACT_INVALID")
        expected_cost = (((policy_payload.get("hard_no_trade_thresholds") or {}) if isinstance(policy_payload.get("hard_no_trade_thresholds"), Mapping) else {}).get("cost_and_expectancy") or {})
        if not isinstance(expected_cost, Mapping) or expected_cost.get("execution_cost_contract_version") != cost_version or expected_cost.get("execution_cost_contract_hash") != cost_hash:
            raise ValueError("COST_POLICY_BINDING_MISMATCH")
        evidence = self._evidence(evidence_hashes)
        secdef_map: dict[int, OptionSecDefSnapshot] = {}
        for secdef in secdefs:
            if not isinstance(secdef, OptionSecDefSnapshot) or secdef.contract_id in secdef_map:
                raise ValueError("SECDEF_IDENTITY_INVALID")
            secdef_map[secdef.contract_id] = secdef
        quote_map: dict[int, BatchedOptionQuote] = {}
        for quote in quote_batch.quotes:
            if quote.contract_id in quote_map:
                raise ValueError("DUPLICATE_QUOTE_CONID")
            quote_map[quote.contract_id] = quote
        evidence_by_id = {item.contract_id: item for item in broker_snapshot.secdef_evidence}
        return MappingProxyType({"secdefs": MappingProxyType(secdef_map), "quotes": MappingProxyType(quote_map), "secdef_evidence": MappingProxyType(evidence_by_id), "strategy_nav": nav_snapshot.strategy_nav, "nav_hash": nav_hash, "nav_content_hash": nav_content_hash, "nav_contract_hash": nav_contract_hash, "nav_ledger_head_hash": nav_ledger_head_hash, "nav_observed_account_nlv": observed_nlv, "nav_reconciliation_difference": reconciliation, "nav_asof": nav_snapshot.asof, "broker_hash": broker_hash, "quote_batch_id": quote_batch_id, "secdef_hash": canonical_hash([self._secdef_document(item) for item in sorted(secdefs, key=lambda value: value.contract_id)]), "evidence_hashes": evidence, "cost_version": cost_version, "cost_hash": cost_hash, "policy_version": policy_version, "policy_hash": policy_hash, "short_leg_risk_evidence": MappingProxyType({"status": assignment_status, "reason_codes": assignment_reasons, "evidence_hash": assignment_binding_hash})})

    @staticmethod
    def _outcome_capture_plan(
        request: StrategyGenerationRequest,
        *,
        quoted_legs: tuple[OptionLegQuote, ...],
        secdefs: Mapping[int, OptionSecDefSnapshot],
        maximum_loss: Decimal,
        broker_snapshot_hash: str,
    ) -> Mapping[str, object]:
        def blocked(reason: str) -> Mapping[str, object]:
            frozen = freeze_json(
                {
                    "schema": "options_copilot.outcome_capture_plan.v1",
                    "status": "BLOCKED",
                    "reason_codes": (reason,),
                }
            )
            assert isinstance(frozen, Mapping)
            return frozen

        baseline = request.outcome_capture_baseline
        if (
            not isinstance(baseline, Mapping)
            or baseline.get("schema")
            != "options_copilot.outcome_capture_baseline.v1"
            or not isinstance(baseline.get("underlying"), Mapping)
            or not isinstance(baseline.get("benchmark"), Mapping)
        ):
            return blocked("OUTCOME_CAPTURE_BASELINE_UNAVAILABLE")
        legs: list[dict[str, object]] = []
        for item in quoted_legs:
            contract_id = item.leg.contract.broker_contract_id
            secdef = None if contract_id is None else secdefs.get(contract_id)
            if (
                contract_id is None
                or secdef is None
                or item.bid is None
                or item.ask is None
                or item.implied_volatility is None
                or item.volume is None
            ):
                return blocked("OUTCOME_CAPTURE_LEG_BASELINE_UNAVAILABLE")
            legs.append(
                {
                    "con_id": contract_id,
                    "side": (
                        "BUY"
                        if item.leg.side is PositionSide.LONG
                        else "SELL"
                    ),
                    "ratio": item.leg.quantity,
                    "multiplier": int(secdef.multiplier),
                    "strike": str(secdef.strike),
                    "bid": str(item.bid),
                    "ask": str(item.ask),
                    "implied_volatility": str(item.implied_volatility),
                    "volume": item.volume,
                    "contract": {
                        "contract_id": secdef.contract_id,
                        "contract_id_ex": (
                            f"{secdef.contract_id}@{secdef.exchange}"
                        ),
                        "symbol": item.leg.contract.underlying,
                        "local_symbol": secdef.local_symbol,
                        "expiration": secdef.expiration,
                        "strike": secdef.strike,
                        "right": secdef.right,
                        "exchange": secdef.exchange,
                        "trading_class": secdef.trading_class,
                        "multiplier": secdef.multiplier,
                        "currency": secdef.currency,
                    },
                }
            )
        plan = {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "READY",
            "reason_codes": (),
            "benchmark_symbol": str(
                baseline.get("benchmark_symbol", "SPY")
            ).upper(),
            "underlying": baseline["underlying"],
            "benchmark": baseline["benchmark"],
            "legs": legs,
            "max_loss_usd": maximum_loss,
            "broker_snapshot_hash": broker_snapshot_hash,
        }
        frozen = freeze_json(plan)
        assert isinstance(frozen, Mapping)
        return frozen

    @staticmethod
    def _request(value: StrategyGenerationRequest | Mapping[str, object]) -> StrategyGenerationRequest:
        if isinstance(value, StrategyGenerationRequest):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("FINALIST_INVALID")
        raw_legs = value.get("legs")
        if not isinstance(raw_legs, Sequence) or isinstance(raw_legs, (str, bytes)):
            raise ValueError("FINALIST_LEGS_MISSING")
        legs = tuple(item if isinstance(item, TemplateLeg) else TemplateLeg(item.get("con_id", item.get("conId")), item.get("side"), item.get("ratio", item.get("quantity", 1))) if isinstance(item, Mapping) else item for item in raw_legs)
        exit_value = value.get("exit_plan")
        if not isinstance(exit_value, ExitPlan):
            if not isinstance(exit_value, Mapping):
                raise ValueError("EXIT_PLAN_MISSING")
            maximum_holding_date = exit_value.get("maximum_holding_date")
            if isinstance(maximum_holding_date, str):
                maximum_holding_date = date.fromisoformat(maximum_holding_date)
            exit_value = ExitPlan(exit_value.get("thesis_invalidation"), exit_value.get("risk_stop"), exit_value.get("profit_take"), exit_value.get("time_stop"), maximum_holding_date, exit_value.get("bad_quote_action"))
        raw_scenarios = value.get("terminal_scenarios", ())
        if not isinstance(raw_scenarios, Sequence) or isinstance(
            raw_scenarios, (str, bytes)
        ):
            raise ValueError("TERMINAL_SCENARIOS_INVALID")
        scenarios = tuple(
            StrategyCandidateGenerator._scenario(item) for item in raw_scenarios
        )
        return StrategyGenerationRequest(
            candidate_id=value.get("candidate_id", value.get("finalist_id")),
            structure=value.get("structure", value.get("template")),
            legs=legs,
            exit_plan=exit_value,
            terminal_scenarios=scenarios,
            thesis=value.get("thesis", "deterministic finalist"),
            outcome_capture_baseline=value.get("outcome_capture_baseline"),
            event_evidence_status=value.get("event_evidence_status", "UNAVAILABLE"),
            earnings_overlap=value.get("earnings_overlap"),
            event_defined=value.get("event_defined", False),
            event_evidence_hash=value.get("event_evidence_hash"),
            event_supporting_overlap=value.get("event_supporting_overlap", False),
            event_supporting_hash=value.get("event_supporting_hash"),
            fundamental_supporting_status=value.get(
                "fundamental_supporting_status",
                "DEGRADED",
            ),
            fundamental_supporting_hash=value.get("fundamental_supporting_hash"),
            fundamental_supporting_payload=value.get(
                "fundamental_supporting_payload",
                {},
            ),
            fundamental_supporting_reason_codes=tuple(
                value.get(
                    "fundamental_supporting_reason_codes",
                    ("FUNDAMENTALS_UNAVAILABLE",),
                )
            ),
            underlying_quote_basis=value.get("underlying_quote_basis"),
            underlying_quote_basis_hash=value.get(
                "underlying_quote_basis_hash"
            ),
        )

    @staticmethod
    def _scenario(value: object) -> TerminalScenario:
        if isinstance(value, TerminalScenario):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("TERMINAL_SCENARIO_INVALID")
        return TerminalScenario(
            _input_decimal(
                value.get("terminal_underlying_price"),
                "terminal_underlying_price",
            ),
            _input_decimal(value.get("probability"), "probability"),
        )

    @staticmethod
    def _contract(value: OptionSecDefSnapshot) -> OptionContract:
        right = OptionRight.CALL if value.right == "C" else OptionRight.PUT
        return OptionContract(str(value.contract_id), value.local_symbol.split()[0] if value.local_symbol else value.trading_class, value.expiration, value.strike, right, Decimal(value.multiplier), value.currency, value.exchange, value.contract_id)

    @staticmethod
    def _quote_ok(value: BatchedOptionQuote, now: datetime) -> Decimal:
        bid, ask = value.bid, value.ask
        liquidity = option_quote_liquidity_assessment(value)
        liquidity_reasons = tuple(liquidity["reason_codes"])
        if liquidity_reasons:
            raise ValueError(liquidity_reasons[0])
        assert isinstance(bid, Decimal) and isinstance(ask, Decimal)
        if (
            not isinstance(value.implied_volatility, Decimal)
            or not value.implied_volatility.is_finite()
            or value.implied_volatility <= ZERO
        ):
            raise ValueError("QUOTE_IV_UNAVAILABLE_OR_INVALID")
        if (
            not isinstance(value.delta, Decimal)
            or not value.delta.is_finite()
            or not -ONE <= value.delta <= ONE
            or not isinstance(value.gamma, Decimal)
            or not value.gamma.is_finite()
            or value.gamma < ZERO
            or not isinstance(value.theta, Decimal)
            or not value.theta.is_finite()
            or not isinstance(value.vega, Decimal)
            or not value.vega.is_finite()
            or value.vega < ZERO
        ):
            raise ValueError("QUOTE_GREEKS_INCOMPLETE_OR_INVALID")
        requested = _aware(value.requested_at, "quote requested_at")
        observed = _aware(value.observed_at, "quote observed_at")
        completed = _aware(value.completed_at, "quote completed_at")
        if not requested <= observed <= completed:
            raise ValueError("QUOTE_TIMESTAMPS_INCOHERENT")
        if value.exchange_time is None:
            raise ValueError("QUOTE_EXCHANGE_TIME_MISSING_OR_INVALID")
        exchange_time = _aware(value.exchange_time, "quote exchange_time")
        if exchange_time > observed:
            raise ValueError("QUOTE_EXCHANGE_TIME_AFTER_OBSERVATION")
        if value.market_data_type != 1:
            raise ValueError("QUOTE_MARKET_DATA_NOT_LIVE")
        age = Decimal(str((now - exchange_time).total_seconds()))
        if age < ZERO or age > MAX_QUOTE_AGE_SECONDS:
            raise ValueError("QUOTE_STALE_OR_FUTURE")
        return age

    @staticmethod
    def _costs(legs: Sequence[OptionLegQuote]) -> Mapping[str, Decimal]:
        sides = sum((item.leg.quantity for item in legs), 0)
        commission = max(ONE, Decimal("1.25") * Decimal(sides)) * Decimal("2")
        slippage = ZERO
        for item in legs:
            assert item.bid is not None and item.ask is not None
            spread = item.ask - item.bid
            entry = max(Decimal("0.01"), Decimal("0.25") * spread)
            exit_cost = max(Decimal("0.02"), Decimal("0.50") * spread)
            slippage += (entry + exit_cost) * item.leg.contract.multiplier * item.leg.quantity
        return MappingProxyType({"commission": commission.quantize(CENT, rounding=ROUND_CEILING), "slippage": slippage.quantize(CENT, rounding=ROUND_CEILING)})

    @staticmethod
    def _premium(legs: Sequence[OptionLegQuote]) -> tuple[Decimal, Decimal]:
        debit = sum((item.ask * item.leg.contract.multiplier * item.leg.quantity for item in legs if item.leg.side is PositionSide.LONG and item.ask is not None), ZERO)
        credit = sum((item.bid * item.leg.contract.multiplier * item.leg.quantity for item in legs if item.leg.side is PositionSide.SHORT and item.bid is not None), ZERO)
        return debit, credit

    @staticmethod
    def _liquidity(legs: Sequence[OptionLegQuote]) -> Decimal:
        scores = []
        for item in legs:
            assert item.bid is not None and item.ask is not None and item.volume is not None and item.open_interest is not None
            relative_spread = (item.ask - item.bid) / ((item.ask + item.bid) / Decimal("2"))
            score = (ONE - relative_spread / MAX_RELATIVE_SPREAD) * Decimal("0.50") + min(ONE, Decimal(item.volume) / Decimal("100")) * Decimal("0.25") + min(ONE, Decimal(item.open_interest) / Decimal("1000")) * Decimal("0.25")
            scores.append(score)
        return min(scores)

    @staticmethod
    def _evidence(value: Mapping[str, str]) -> Mapping[str, str]:
        if not isinstance(value, Mapping):
            raise ValueError("EVIDENCE_BINDING_MISSING")
        result = {str(key).upper(): _hash(item, f"evidence hash {key}") for key, item in value.items()}
        if not {"MARKET", "VOLATILITY", "LIQUIDITY"}.issubset(result):
            raise ValueError("EVIDENCE_BINDING_INCOMPLETE")
        return MappingProxyType(result)

    @staticmethod
    def _signed_contract(value: Mapping[str, object], field: str, now: datetime) -> None:
        if value.get("schema") != "options_copilot.governance.signed_contract.v1":
            raise ValueError(f"{field} schema invalid")
        actor = _string(value.get("actor"), f"{field} actor")
        if not actor.lower().startswith("human:"):
            raise ValueError(f"{field} actor is not human signed")
        signed_at = value.get("signed_at")
        effective_at = value.get("effective_at")
        try:
            signed = datetime.fromisoformat(str(signed_at).replace("Z", "+00:00"))
            effective = datetime.fromisoformat(str(effective_at).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} signature time invalid") from exc
        signed = _aware(signed, f"{field} signed_at")
        effective = _aware(effective, f"{field} effective_at")
        if signed > now or effective > now:
            raise ValueError(f"{field} is not effective")

    @staticmethod
    def _option_position_open(positions: Iterable[object]) -> bool:
        for position in positions:
            if isinstance(position, Mapping):
                security_type = position.get(
                    "security_type",
                    position.get("secType", ""),
                )
                quantity = position.get("quantity", position.get("position"))
            else:
                security_type = getattr(
                    position,
                    "security_type",
                    getattr(position, "secType", ""),
                )
                quantity = getattr(
                    position,
                    "quantity",
                    getattr(position, "position", None),
                )
            if str(security_type or "").upper() not in {
                "OPT",
                "OPTION",
                "BAG",
                "COMBO",
            }:
                continue
            try:
                if Decimal(str(quantity)) != ZERO:
                    return True
            except (InvalidOperation, TypeError, ValueError):
                # Unknown derivative exposure must fail closed into management.
                return True
        return False

    @staticmethod
    def _secdef_document(item: OptionSecDefSnapshot) -> dict[str, object]:
        return {"con_id": item.contract_id, "local_symbol": item.local_symbol, "expiration": item.expiration, "strike": item.strike, "right": item.right, "exchange": item.exchange, "multiplier": item.multiplier, "currency": item.currency, "standard": item.standard_contract, "adjusted": item.adjusted}

    @staticmethod
    def _no_trade(code: str) -> GenerationResult:
        return GenerationResult("NO_TRADE", (), (code,))

    @staticmethod
    def _reason(exc: Exception) -> str:
        text = str(exc).strip().upper().replace(" ", "_")
        return text if text else "GENERATION_REJECTED"


def _aware(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal_text(value: Decimal | None) -> str:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("proposal contains an invalid Decimal")
    if value == ZERO:
        return "0"
    return format(value.normalize(), "f")


def _datetime_text(value: datetime) -> str:
    return _aware(value, "proposal quote time").isoformat().replace("+00:00", "Z")


__all__ = [
    "GeneratedStrategyCandidate",
    "GenerationResult",
    "StrategyCandidateGenerator",
    "StrategyGenerationRequest",
    "option_quote_liquidity_assessment",
]
