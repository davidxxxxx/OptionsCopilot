"""Conditional full-holdings close mathematics, without inferred strategy intent."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING, localcontext
from enum import Enum
from types import MappingProxyType
from typing import Protocol

from options_copilot.gateway.broker_snapshot import AtomicBrokerSnapshot
from options_copilot.governance.contracts import ContractKind, SignedContract, verify_contract
from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .holdings_payoff import HoldingPayoffLeg, HoldingsPayoff, analyze_holdings_payoff
from .manager import PositionManager, TransitionRejected
from .models import (
    AuthoritativePosition, CapitalUsageProof, PositionDelta, PositionManagementKind,
    PositionRiskMetrics, PositionTransitionProof, SecDefBinding,
)


ZERO = Decimal("0")
MULTIPLIER = Decimal("100")
SCHEMA = "options_copilot.holdings_close_preview.v1"
AFTER_CONDITION = "ALL_LEGS_FILLED_AND_RECONCILED"
UNVERIFIED_REASONS = (
    "HOLDINGS_GROUPING_NOT_INFERRED",
    "CALENDAR_EVIDENCE_UNVERIFIED",
    "EXTRINSIC_VALUE_PATH_UNVERIFIED",
    "ASSIGNMENT_RISK_UNVERIFIED",
    "ATOMIC_BASKET_FILL_UNVERIFIED",
    "LEGGING_RISK_UNVERIFIED",
    "BROKER_MARGIN_UNVERIFIED",
)


class CloseCostPolicy(Protocol):
    """The generator's already-validated immutable execution-cost policy."""

    contract: SignedContract
    per_contract_side: Decimal
    minimum_per_order: Decimal
    planned_exit_floor: Decimal
    planned_exit_spread_factor: Decimal
    stress_multiplier: Decimal
    quantum: Decimal
    assignment_assumption_hash: str


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return utc_datetime(value).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_safe(item) for item in value]
    raise TypeError("unsupported holdings-close preview value")


def _mechanical_intent(snapshot_hash: str, positions_hash: str, deltas: tuple[PositionDelta, ...]) -> dict[str, object]:
    return {
        "schema": "options_copilot.mechanical_holdings_close_intent.v1",
        "scope": "ALL_OBSERVED_OPTION_HOLDINGS",
        "grouping_status": "NOT_INFERRED",
        "broker_snapshot_hash": snapshot_hash,
        "positions_state_hash": positions_hash,
        "deltas": [item.as_dict() for item in deltas],
        "after_risk_condition": AFTER_CONDITION,
        "provenance": "DETERMINISTIC_PREVIEW_ONLY_NOT_USER_INTENT_OR_SIGNATURE",
        "approval_enabled": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "affects_eligibility": False,
    }


@dataclass(frozen=True, slots=True)
class HoldingsClosePreview:
    """A hash-bound conditional calculation that can never confer eligibility."""

    candidate_id: str
    symbol: str
    expiration: date
    execution_legs: tuple[Mapping[str, object], ...]
    gross_component_liquidation_cashflow_usd: Decimal
    estimated_commission_usd: Decimal
    normal_slippage_usd: Decimal
    stress_slippage_usd: Decimal
    before_payoff: HoldingsPayoff
    transition_proof: PositionTransitionProof
    assignment_assumption_hash: str
    generated_at: datetime
    oldest_quote_at: datetime
    oldest_quote_age_seconds: Decimal
    maximum_leg_skew_seconds: Decimal
    candidate_hash: str
    schema: str = field(default=SCHEMA, init=False)
    scope: str = field(default="ALL_OBSERVED_OPTION_HOLDINGS", init=False)
    grouping_status: str = field(default="NOT_INFERRED", init=False)
    review_state: str = field(default="PREVIEW_UNVERIFIED", init=False)
    management_kind: PositionManagementKind = field(default=PositionManagementKind.CLOSE_ALL, init=False)
    reason_codes: tuple[str, ...] = field(default=UNVERIFIED_REASONS, init=False)
    review_only: bool = field(default=True, init=False)
    dry_run_only: bool = field(default=True, init=False)
    approval_enabled: bool = field(default=False, init=False)
    instruction_enabled: bool = field(default=False, init=False)
    direct_order_submission: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)
    affects_eligibility: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        with localcontext() as context:
            context.prec = 512
            self._validate()

    def _validate(self) -> None:
        object.__setattr__(self, "execution_legs", tuple(MappingProxyType(dict(item)) for item in self.execution_legs))
        object.__setattr__(self, "generated_at", utc_datetime(self.generated_at))
        object.__setattr__(self, "oldest_quote_at", utc_datetime(self.oldest_quote_at))
        proof = self.transition_proof
        if not isinstance(proof, PositionTransitionProof) or not proof.verify_hash():
            raise ValueError("holdings close proof is missing or invalid")
        if not isinstance(self.before_payoff, HoldingsPayoff) or not self.before_payoff.verify_hash():
            raise ValueError("holdings payoff is missing or invalid")
        if proof.management_kind is not PositionManagementKind.CLOSE_ALL or proof.after_positions:
            raise ValueError("holdings preview only permits complete exact close")
        before = {item.contract_id: item.signed_quantity for item in proof.before_positions}
        before_by_id = {item.contract_id: item for item in proof.before_positions}
        secdef_hashes = {item.contract_id: item.identity_hash for item in proof.secdef_bindings}
        deltas = {item.contract_id: item.signed_quantity_delta for item in proof.deltas}
        if deltas != {contract_id: -quantity for contract_id, quantity in before.items()}:
            raise ValueError("holdings close proof must invert every observed holding")
        if not 1 <= len(self.execution_legs) <= 8 or len(before) != len(self.execution_legs):
            raise ValueError("holdings close requires one through eight complete legs")
        if len({item["contract_id"] for item in self.execution_legs}) != len(before):
            raise ValueError("holdings close has duplicate execution legs")
        gross = ZERO
        exposure = self.estimated_execution_cost_usd
        source_times: list[datetime] = []
        payoff_by_id = {item.contract_id: item for item in self.before_payoff.legs}
        if set(payoff_by_id) != set(before) or set(secdef_hashes) != set(before):
            raise ValueError("holdings payoff and secdefs must bind exactly the complete position set")
        for leg in self.execution_legs:
            contract_id = leg["contract_id"]
            quantity = before.get(contract_id)
            if quantity is None or leg["current_signed_quantity"] != quantity:
                raise ValueError("execution leg does not bind the observed holding")
            if (
                self.symbol != before_by_id[contract_id].symbol
                or leg["local_symbol"] != before_by_id[contract_id].local_symbol
                or leg["secdef_identity_hash"] != secdef_hashes[contract_id]
                or leg["expiration"] != self.expiration
                or leg["right"] not in {"CALL", "PUT"}
            ):
                raise ValueError("execution leg identity does not bind the observed holding")
            if leg["signed_quantity_delta"] != -quantity or leg["action_quantity"] != abs(quantity):
                raise ValueError("execution leg does not close its entire holding")
            bid, ask = leg["bid"], leg["ask"]
            if not isinstance(bid, Decimal) or not isinstance(ask, Decimal) or not ZERO < bid < ask:
                raise ValueError("execution leg needs positive uncrossed Decimal prices")
            price = bid if quantity > 0 else ask
            if leg["executable_price"] != price or leg["multiplier"] != MULTIPLIER:
                raise ValueError("execution leg has wrong natural close price or multiplier")
            source_identity = {
                "conId": contract_id, "localSymbol": leg["local_symbol"],
                "tradingClass": leg["trading_class"], "multiplier": 100,
                "exchange": leg["exchange"], "expiry": leg["expiration"],
                "strike": leg["strike"], "right": "C" if leg["right"] == "CALL" else "P",
            }
            if canonical_hash(source_identity) != secdef_hashes[contract_id]:
                raise ValueError("execution geometry does not match its source SECDEF hash")
            if leg["action"] != ("SELL_TO_CLOSE" if quantity > 0 else "BUY_TO_CLOSE"):
                raise ValueError("execution leg has wrong close direction")
            source_times.append(utc_datetime(leg["quote_observed_at"]))
            payoff_leg = payoff_by_id.get(contract_id)
            if payoff_leg is None or (
                payoff_leg.signed_quantity != quantity
                or payoff_leg.symbol != self.symbol
                or payoff_leg.expiration != self.expiration
                or payoff_leg.strike != leg["strike"]
                or payoff_leg.right != ("C" if leg["right"] == "CALL" else "P")
            ):
                raise ValueError("holdings payoff leg does not bind execution geometry")
            gross += Decimal(quantity) * MULTIPLIER * price
            exposure += Decimal(abs(quantity)) * MULTIPLIER * ask
        for value in (self.estimated_commission_usd, self.normal_slippage_usd, self.stress_slippage_usd):
            if not isinstance(value, Decimal) or not value.is_finite() or value < ZERO:
                raise ValueError("holdings close costs must be finite nonnegative Decimal values")
        if gross != self.gross_component_liquidation_cashflow_usd:
            raise ValueError("holdings close cashflow does not bind its exact component prices")
        if self.before_payoff.gross_liquidation_cashflow_usd != gross:
            raise ValueError("holdings payoff has a different liquidation basis")
        if self.before_payoff.estimated_future_exit_cost_usd != self.estimated_execution_cost_usd:
            raise ValueError("holdings payoff must include one future exit cost reserve")
        if proof.before_risk != PositionRiskMetrics(self.before_payoff.max_loss_usd, exposure, self.before_payoff.max_loss_usd):
            raise ValueError("holdings close risk metrics do not match the stated proxies")
        if proof.after_risk != PositionRiskMetrics(ZERO, ZERO, ZERO):
            raise ValueError("conditional holdings close after risk must be zero")
        if canonical_hash(self.mechanical_close_intent) != proof.exit_contract_hash:
            raise ValueError("mechanical close provenance is not bound to the transition proof")
        if not ZERO <= self.oldest_quote_age_seconds <= Decimal("5"):
            raise ValueError("holdings close exchange quote is stale")
        if not ZERO <= self.maximum_leg_skew_seconds <= Decimal("2"):
            raise ValueError("holdings close exchange quotes are incoherent")
        if self.generated_at != proof.verified_at:
            raise ValueError("holdings close generation time differs from its proof")
        if min(source_times) != self.oldest_quote_at or max(source_times) > self.generated_at:
            raise ValueError("holdings close timestamps do not bind source leg times")
        if self.oldest_quote_age_seconds != Decimal(str((self.generated_at - self.oldest_quote_at).total_seconds())):
            raise ValueError("holdings close quote age does not match source time")
        if self.maximum_leg_skew_seconds != Decimal(str((max(source_times) - min(source_times)).total_seconds())):
            raise ValueError("holdings close skew does not match source times")
        if self.candidate_hash != "0" * 64 and not self.verify_hash():
            raise ValueError("holdings close candidate hash is invalid")

    @property
    def kind(self) -> PositionManagementKind:
        return self.management_kind

    @property
    def estimated_execution_cost_usd(self) -> Decimal:
        # Stress replaces normal slippage; normal is a comparison, not a second charge.
        with localcontext() as context:
            context.prec = 512
            return self.estimated_commission_usd + self.stress_slippage_usd

    @property
    def all_in_close_cashflow_usd(self) -> Decimal:
        # Signed proceeds are exact; rounding upwards would overstate a credit.
        with localcontext() as context:
            context.prec = 512
            return self.gross_component_liquidation_cashflow_usd - self.estimated_execution_cost_usd

    @property
    def mechanical_close_intent(self) -> dict[str, object]:
        proof = self.transition_proof
        return _mechanical_intent(proof.broker_snapshot_hash, proof.positions_state_hash, proof.deltas)

    def hash_payload(self) -> dict[str, object]:
        proof = self.transition_proof
        payload = {
            "schema": self.schema,
            "candidate_id": self.candidate_id,
            "symbol": self.symbol,
            "expiration": self.expiration,
            "scope": self.scope,
            "grouping_status": self.grouping_status,
            "review_state": self.review_state,
            "management_kind": self.management_kind.value,
            "execution_legs": list(self.execution_legs),
            "gross_component_liquidation_cashflow_usd": self.gross_component_liquidation_cashflow_usd,
            "estimated_commission_usd": self.estimated_commission_usd,
            "normal_slippage_usd": self.normal_slippage_usd,
            "stress_slippage_usd": self.stress_slippage_usd,
            "estimated_execution_cost_usd": self.estimated_execution_cost_usd,
            "cost_aggregation": "COMMISSION_PLUS_ONE_STRESS_FUTURE_EXIT_RESERVE_NORMAL_NOT_ADDITIVE",
            "all_in_close_cashflow_usd": self.all_in_close_cashflow_usd,
            "before_payoff": self.before_payoff.as_dict(),
            "after_payoff": {"max_loss_usd": ZERO, "max_profit_usd": ZERO, "condition": AFTER_CONDITION},
            "before_risk": proof.before_risk.as_dict(),
            "after_risk": proof.after_risk.as_dict(),
            "proof_scope": "CONDITIONAL_ALL_HOLDINGS_FLAT",
            "after_risk_condition": AFTER_CONDITION,
            "atomic_basket_fill_status": "UNVERIFIED",
            "legging_risk_status": "UNVERIFIED",
            "assignment_risk_status": "UNVERIFIED",
            "calendar_evidence_status": "UNVERIFIED",
            "extrinsic_value_path_status": "UNVERIFIED",
            "broker_margin_status": "UNVERIFIED",
            "exposure_basis": "GROSS_ASK_NOTIONAL_PLUS_EXIT_RESERVE_PROXY",
            "capital_usage_basis": "COST_ADJUSTED_HOLDING_MAX_LOSS_PROXY_NOT_BROKER_MARGIN",
            "mechanical_close_intent": self.mechanical_close_intent,
            "exit_contract_hash": proof.exit_contract_hash,
            "assignment_assumption_hash": self.assignment_assumption_hash,
            "transition_proof_hash": proof.proof_hash,
            "transition_proof": {**proof.hash_payload(), "proof_hash": proof.proof_hash},
            "broker_snapshot_hash": proof.broker_snapshot_hash,
            "positions_state_hash": proof.positions_state_hash,
            "quote_batch_id": proof.quote_batch_id,
            "quote_batch_hash": proof.quote_batch_hash,
            "secdef_hashes": tuple(item.identity_hash for item in proof.secdef_bindings),
            "execution_cost_contract_version": proof.execution_cost_contract_version,
            "execution_cost_contract_hash": proof.execution_cost_contract_hash,
            "generated_at": self.generated_at,
            "oldest_quote_at": self.oldest_quote_at,
            "oldest_quote_age_seconds": self.oldest_quote_age_seconds,
            "maximum_leg_skew_seconds": self.maximum_leg_skew_seconds,
            "reason_codes": self.reason_codes,
            "review_only": self.review_only,
            "dry_run_only": self.dry_run_only,
            "approval_enabled": self.approval_enabled,
            "instruction_enabled": self.instruction_enabled,
            "instruction_creation_allowed": self.instruction_creation_allowed,
            "order_allowed": self.order_allowed,
            "affects_eligibility": self.affects_eligibility,
            "direct_order_submission": self.direct_order_submission,
        }
        # Bind the exact wire document, including nested proof values.  The API
        # can check the complete checksum without guessing Decimal field types.
        return _json_safe(payload)  # type: ignore[return-value]

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.candidate_hash

    def preview_payload(self) -> dict[str, object]:
        return {**self.hash_payload(), "candidate_hash": self.candidate_hash}


def build_holdings_close_preview(
    snapshot: AtomicBrokerSnapshot,
    *,
    position_manager: PositionManager,
    cost_policy: CloseCostPolicy,
) -> HoldingsClosePreview:
    """Preserve all manager gates and calculate only an exact full-set inverse."""

    with localcontext() as context:
        context.prec = 512
        return _build_holdings_close_preview(snapshot, position_manager=position_manager, cost_policy=cost_policy)


def _build_holdings_close_preview(
    snapshot: AtomicBrokerSnapshot,
    *,
    position_manager: PositionManager,
    cost_policy: CloseCostPolicy,
) -> HoldingsClosePreview:
    before = position_manager.normalize_snapshot(snapshot)
    if not 1 <= len(before) <= 8:
        raise TransitionRejected("HOLDINGS_CLOSE_LEG_COUNT_UNSUPPORTED", "requires one through eight complete holdings")
    contract = verify_contract(cost_policy.contract, expected_kind=ContractKind.EXECUTION_COST, as_of=snapshot.built_at)
    identities = {item.contract_id: item.post_identity for item in snapshot.secdef_evidence}
    quotes = {item.contract_id: item for item in snapshot.quotes}
    expirations = {identities[item.contract_id]["expiry"] for item in before}
    if len(expirations) != 1:
        raise TransitionRejected("HOLDINGS_CLOSE_MIXED_EXPIRATION", "terminal payoff requires one expiry")
    expiration = next(iter(expirations))
    if not isinstance(expiration, date) or isinstance(expiration, datetime) or expiration < snapshot.built_at.date():
        raise TransitionRejected("POSITION_EXPIRED", "holdings close requires an unexpired exact expiry")
    legs: list[Mapping[str, object]] = []
    payoff_legs: list[HoldingPayoffLeg] = []
    exchange_times: list[datetime] = []
    gross = ZERO
    normal = ZERO
    exposure = ZERO
    for position in before:
        identity = identities[position.contract_id]
        quote = quotes[position.contract_id]
        if not isinstance(quote.exchange_time, datetime):
            raise TransitionRejected("QUOTE_SOURCE_TIME_UNAVAILABLE", "exchange quote time is required")
        exchange_time = utc_datetime(quote.exchange_time)
        age = Decimal(str((snapshot.built_at - exchange_time).total_seconds()))
        if not ZERO <= age <= Decimal("5"):
            raise TransitionRejected("QUOTE_STALE", "exchange quote exceeds the five-second hard gate")
        exchange_times.append(exchange_time)
        assert quote.bid is not None and quote.ask is not None
        quantity = position.signed_quantity
        price = quote.bid if quantity > 0 else quote.ask
        gross += Decimal(quantity) * MULTIPLIER * price
        exposure += Decimal(abs(quantity)) * MULTIPLIER * quote.ask
        normal += max(cost_policy.planned_exit_floor, cost_policy.planned_exit_spread_factor * (quote.ask - quote.bid)) * MULTIPLIER * Decimal(abs(quantity))
        payoff_legs.append(HoldingPayoffLeg(
            contract_id=position.contract_id, symbol=position.symbol, expiration=expiration,
            strike=identity["strike"], right=identity["right"], signed_quantity=quantity,
            multiplier=100, currency="USD",
        ))
        legs.append({
            "contract_id": position.contract_id, "local_symbol": str(identity["localSymbol"]),
            "trading_class": str(identity["tradingClass"]), "exchange": str(identity["exchange"]),
            "expiration": expiration, "strike": identity["strike"],
            "right": "CALL" if identity["right"] == "C" else "PUT",
            "current_signed_quantity": quantity, "signed_quantity_delta": -quantity,
            "action": "SELL_TO_CLOSE" if quantity > 0 else "BUY_TO_CLOSE",
            "action_quantity": abs(quantity), "multiplier": MULTIPLIER,
            "bid": quote.bid, "ask": quote.ask, "executable_price": price,
            "quote_observed_at": exchange_time,
            "secdef_identity_hash": next(item.post_hash for item in snapshot.secdef_evidence if item.contract_id == position.contract_id),
        })
    oldest = min(exchange_times)
    skew = Decimal(str((max(exchange_times) - oldest).total_seconds()))
    if skew > Decimal("2"):
        raise TransitionRejected("QUOTE_INCOHERENT", "exchange leg skew exceeds two seconds")
    round_cost = lambda value: value.quantize(cost_policy.quantum, rounding=ROUND_CEILING)
    sides = sum(abs(item.signed_quantity) for item in before)
    commission = round_cost(max(cost_policy.minimum_per_order, cost_policy.per_contract_side * Decimal(sides)))
    stress = round_cost(normal * cost_policy.stress_multiplier)
    normal = round_cost(normal)
    reserve = commission + stress
    payoff = analyze_holdings_payoff(
        tuple(payoff_legs), gross_liquidation_cashflow_usd=gross,
        estimated_future_exit_cost_usd=reserve,
    )
    deltas = tuple(PositionDelta(item.contract_id, -item.signed_quantity) for item in before)
    positions_hash = snapshot.state_evidence["positions"].post_hash
    intent = _mechanical_intent(snapshot.snapshot_hash, positions_hash, deltas)
    proof = position_manager.prove_transition(
        snapshot, kind=PositionManagementKind.CLOSE_ALL, deltas=deltas,
        before_risk=PositionRiskMetrics(payoff.max_loss_usd, exposure + reserve, payoff.max_loss_usd),
        after_risk=PositionRiskMetrics(ZERO, ZERO, ZERO),
        exit_contract_hash=canonical_hash(intent), execution_cost_contract=contract,
    )
    preview = HoldingsClosePreview(
        candidate_id=f"{before[0].symbol.lower()}-holdings-close-all-{snapshot.snapshot_hash[:12]}",
        symbol=before[0].symbol, expiration=expiration, execution_legs=tuple(legs),
        gross_component_liquidation_cashflow_usd=gross, estimated_commission_usd=commission,
        normal_slippage_usd=normal, stress_slippage_usd=stress, before_payoff=payoff,
        transition_proof=proof, assignment_assumption_hash=cost_policy.assignment_assumption_hash,
        generated_at=snapshot.built_at, oldest_quote_at=oldest,
        oldest_quote_age_seconds=Decimal(str((snapshot.built_at - oldest).total_seconds())),
        maximum_leg_skew_seconds=skew, candidate_hash="0" * 64,
    )
    return replace(preview, candidate_hash=canonical_hash(preview.hash_payload()))


def verify_holdings_close_preview_payload(raw: Mapping[str, object]) -> bool:
    """Check wire integrity and no-authority invariants, never grant authority."""

    try:
        with localcontext() as context:
            context.prec = 512
            return _verify_payload(raw)
    except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation, OverflowError):
        return False


def _verify_payload(raw: Mapping[str, object]) -> bool:
    if not isinstance(raw, Mapping) or raw.get("schema") != SCHEMA:
        return False
    body = {key: value for key, value in raw.items() if key != "candidate_hash"}
    if canonical_hash(body) != raw.get("candidate_hash"):
        return False
    proof = _proof_from_payload(raw["transition_proof"])
    payoff_raw = raw["before_payoff"]
    payoff = analyze_holdings_payoff(
        tuple(HoldingPayoffLeg(
            contract_id=row["contract_id"], symbol=row["symbol"],
            expiration=date.fromisoformat(row["expiration"]), strike=_wire_decimal(row["strike"]),
            right=row["right"], signed_quantity=row["signed_quantity"],
            multiplier=row["multiplier"], currency=row["currency"],
        ) for row in payoff_raw["legs"]),
        gross_liquidation_cashflow_usd=_wire_decimal(payoff_raw["gross_liquidation_cashflow_usd"]),
        estimated_future_exit_cost_usd=_wire_decimal(payoff_raw["estimated_future_exit_cost_usd"]),
    )
    if payoff.as_dict() != payoff_raw:
        return False
    legs = []
    for row in raw["execution_legs"]:
        leg = dict(row)
        for name in ("strike", "multiplier", "bid", "ask", "executable_price"):
            leg[name] = _wire_decimal(leg[name])
        leg["expiration"] = date.fromisoformat(leg["expiration"])
        leg["quote_observed_at"] = _wire_time(leg["quote_observed_at"])
        legs.append(leg)
    preview = HoldingsClosePreview(
        candidate_id=raw["candidate_id"], symbol=raw["symbol"],
        expiration=date.fromisoformat(raw["expiration"]), execution_legs=tuple(legs),
        gross_component_liquidation_cashflow_usd=_wire_decimal(raw["gross_component_liquidation_cashflow_usd"]),
        estimated_commission_usd=_wire_decimal(raw["estimated_commission_usd"]),
        normal_slippage_usd=_wire_decimal(raw["normal_slippage_usd"]),
        stress_slippage_usd=_wire_decimal(raw["stress_slippage_usd"]),
        before_payoff=payoff, transition_proof=proof,
        assignment_assumption_hash=raw["assignment_assumption_hash"],
        generated_at=_wire_time(raw["generated_at"]), oldest_quote_at=_wire_time(raw["oldest_quote_at"]),
        oldest_quote_age_seconds=_wire_decimal(raw["oldest_quote_age_seconds"]),
        maximum_leg_skew_seconds=_wire_decimal(raw["maximum_leg_skew_seconds"]),
        candidate_hash=raw["candidate_hash"],
    )
    # Reconstructing immutable contracts verifies nested proof hashes, geometry,
    # numerical consistency and every fixed no-authority field, not just a hash.
    return preview.preview_payload() == raw


def _wire_decimal(value: object) -> Decimal:
    if not isinstance(value, str) or len(value) > 512:
        raise ValueError("holdings wire Decimal must be bounded exact text")
    result = Decimal(value)
    if not result.is_finite() or not -256 <= result.as_tuple().exponent <= 256:
        raise ValueError("holdings wire Decimal is out of bounds")
    return result


def _wire_time(value: object) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("holdings wire time must be bounded UTC text")
    return utc_datetime(datetime.fromisoformat(value), field="holdings_wire_time")


def _proof_from_payload(raw: Mapping[str, object]) -> PositionTransitionProof:
    fields = dict(raw)
    for name in ("before_positions", "after_positions"):
        fields[name] = tuple(AuthoritativePosition(
            **{**row, "observed_at": _wire_time(row["observed_at"])}
        ) for row in raw[name])
    fields["deltas"] = tuple(PositionDelta(**row) for row in raw["deltas"])
    fields["secdef_bindings"] = tuple(SecDefBinding(**row) for row in raw["secdef_bindings"])
    for name in ("before_risk", "after_risk"):
        fields[name] = PositionRiskMetrics(**{
            key: _wire_decimal(value) for key, value in raw[name].items()
        })
    fields["capital_usage"] = CapitalUsageProof(**{
        key: _wire_decimal(value) if key.endswith("_usd") else value
        for key, value in raw["capital_usage"].items()
    })
    fields["verified_at"] = _wire_time(raw["verified_at"])
    return PositionTransitionProof(**fields)


__all__ = [
    "HoldingsClosePreview", "build_holdings_close_preview",
    "verify_holdings_close_preview_payload",
]
