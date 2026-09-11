"""Snapshot-derived, preview-only defined-risk position candidates.

This module is a pure analytical boundary.  It accepts an already-built P1
broker snapshot and immutable contracts; it has no gateway, network, approval,
bridge, creator, instruction, or order-placement dependency.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from enum import Enum
import json
from math import gcd
from types import MappingProxyType

from options_copilot.domain import (
    OptionContract,
    OptionLeg,
    OptionLegQuote,
    OptionRight,
    PositionSide,
    StrategyCandidate,
)
from options_copilot.gateway.broker_snapshot import AtomicBrokerSnapshot
from options_copilot.gateway.ibkr_readonly import BatchedOptionQuote
from options_copilot.governance.contracts import (
    ContractKind,
    ContractValidationError,
    SignedContract,
    verify_contract,
)
from options_copilot.risk import PayoffStatus, analyze_expiration_payoff
from options_copilot.storage.canonical import canonical_hash
from options_copilot.strategies import ExitPlan

from .manager import PositionManager, TransitionRejected
from .holdings_close import HoldingsClosePreview, build_holdings_close_preview
from .models import (
    AuthoritativePosition,
    PositionDelta,
    PositionManagementKind,
    PositionRiskMetrics,
    PositionTransitionProof,
)


ZERO = Decimal("0")
CENT = Decimal("0.01")
STANDARD_MULTIPLIER = Decimal("100")
RESULT_SCHEMA = "options_copilot.management_generation.v1"
CANDIDATE_SCHEMA = "options_copilot.management_candidate.v1"
PAYOFF_SCHEMA = "options_copilot.management_payoff.v1"
EXIT_SCHEMA = "options_copilot.management_exit_contract.v1"


class ManagementGenerationStatus(str, Enum):
    CANDIDATES = "CANDIDATES"
    NO_TRADE = "NO_TRADE"


class _GenerationRejected(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _reject(code: str, detail: str) -> None:
    raise _GenerationRejected(code, detail)


def _digest(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        _reject("HASH_INVALID", f"{field} must be a lowercase SHA-256 hash")
    return value


def _document(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _reject("CONTRACT_INVALID", f"{field} must be an object")
    return value


def _decimal(value: object, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        _reject("EXECUTION_COST_CONTRACT_INVALID", f"{field} must not use binary float")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise _GenerationRejected(
            "EXECUTION_COST_CONTRACT_INVALID",
            f"{field} must be a finite decimal",
        ) from exc
    if not result.is_finite() or (positive and result <= ZERO):
        _reject("EXECUTION_COST_CONTRACT_INVALID", f"{field} is out of range")
    return result


def _date(value: object, field: str) -> date:
    if isinstance(value, datetime):
        _reject("EXIT_CONTRACT_INVALID", f"{field} must be a date")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise _GenerationRejected(
                "EXIT_CONTRACT_INVALID",
                f"{field} must be an ISO date",
            ) from exc
    _reject("EXIT_CONTRACT_INVALID", f"{field} must be a date")


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_CEILING)


def _json_safe(value: object) -> object:
    """Return detached JSON data with Decimal values rendered exactly."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        normalized = value.normalize()
        return "0" if not normalized else format(normalized, "f")
    if isinstance(value, datetime):
        rendered = value.isoformat(timespec="microseconds")
        return rendered.replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [_json_safe(item) for item in value]
    raise TypeError(f"unsupported preview value {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class ManagementPayoffMetrics:
    max_loss_usd: Decimal
    max_profit_usd: Decimal | None
    unbounded_profit: bool
    net_opening_cashflow_usd: Decimal
    estimated_future_exit_cost_usd: Decimal
    breakevens: tuple[Decimal, ...]
    geometry_hash: str
    payoff_hash: str
    schema: str = PAYOFF_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(self, "breakevens", tuple(self.breakevens))
        if not isinstance(self.max_loss_usd, Decimal) or self.max_loss_usd < ZERO:
            raise ValueError("max_loss_usd must be a nonnegative Decimal")
        if self.max_profit_usd is not None and not isinstance(
            self.max_profit_usd, Decimal
        ):
            raise TypeError("max_profit_usd must be Decimal or None")
        _digest(self.geometry_hash, "geometry_hash")
        _digest(self.payoff_hash, "payoff_hash")
        if self.schema != PAYOFF_SCHEMA:
            raise ValueError("management payoff schema mismatch")
        if self.payoff_hash != "0" * 64 and not self.verify_hash():
            raise ValueError("payoff_hash does not bind payoff metrics")

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "max_loss_usd": self.max_loss_usd,
            "max_profit_usd": self.max_profit_usd,
            "unbounded_profit": self.unbounded_profit,
            "net_opening_cashflow_usd": self.net_opening_cashflow_usd,
            "estimated_future_exit_cost_usd": self.estimated_future_exit_cost_usd,
            "breakevens": self.breakevens,
            "geometry_hash": self.geometry_hash,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.payoff_hash

    def as_dict(self) -> dict[str, object]:
        return _json_safe(  # type: ignore[return-value]
            {**self.hash_payload(), "payoff_hash": self.payoff_hash}
        )


@dataclass(frozen=True, slots=True)
class ManagementExecutionLeg:
    contract_id: int
    local_symbol: str
    expiration: date
    strike: Decimal
    right: str
    current_signed_quantity: int
    signed_quantity_delta: int
    action: str
    action_quantity: int
    multiplier: Decimal
    bid: Decimal
    ask: Decimal
    executable_price: Decimal
    quote_observed_at: datetime
    secdef_identity_hash: str

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "local_symbol": self.local_symbol,
            "expiration": self.expiration,
            "strike": self.strike,
            "right": self.right,
            "current_signed_quantity": self.current_signed_quantity,
            "signed_quantity_delta": self.signed_quantity_delta,
            "action": self.action,
            "action_quantity": self.action_quantity,
            "multiplier": self.multiplier,
            "bid": self.bid,
            "ask": self.ask,
            "executable_price": self.executable_price,
            "quote_observed_at": self.quote_observed_at,
            "secdef_identity_hash": self.secdef_identity_hash,
        }


@dataclass(frozen=True, slots=True)
class ManagementCandidate:
    candidate_id: str
    symbol: str
    structure: str
    review_state: str
    thesis_invalidation_state: str
    risk_stop_state: str
    profit_take_state: str
    time_stop_state: str
    entry_net_cost_usd: Decimal | None
    entry_net_credit_usd: Decimal | None
    entry_max_loss_usd: Decimal | None
    entry_max_profit_usd: Decimal | None
    stop_review_cashflow_usd: Decimal | None
    profit_review_cashflow_usd: Decimal | None
    management_kind: PositionManagementKind
    expiration: date
    combo_quantity_before: int
    combo_quantity_after: int
    unit_ratio: tuple[int, ...]
    execution_legs: tuple[ManagementExecutionLeg, ...]
    executable_close_cashflow_usd: Decimal
    estimated_commission_usd: Decimal
    normal_slippage_usd: Decimal
    stress_slippage_usd: Decimal
    estimated_execution_cost_usd: Decimal
    all_in_close_cashflow_usd: Decimal
    before_payoff: ManagementPayoffMetrics
    after_payoff: ManagementPayoffMetrics
    transition_proof: PositionTransitionProof
    exit_plan: ExitPlan
    exit_contract_hash: str
    assignment_assumption_hash: str
    broker_snapshot_hash: str
    quote_batch_id: str
    quote_batch_hash: str
    oldest_quote_age_seconds: Decimal
    maximum_leg_skew_seconds: Decimal
    secdef_hashes: tuple[str, ...]
    positions_state_hash: str
    execution_cost_contract_version: str
    execution_cost_contract_hash: str
    candidate_hash: str
    review_only: bool = True
    dry_run_only: bool = True
    approval_enabled: bool = False
    direct_order_submission: bool = False
    schema: str = CANDIDATE_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(self, "unit_ratio", tuple(self.unit_ratio))
        object.__setattr__(self, "execution_legs", tuple(self.execution_legs))
        object.__setattr__(self, "secdef_hashes", tuple(self.secdef_hashes))
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValueError("management symbol must be nonblank")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        if self.structure not in {
            "BULL_CALL_DEBIT_VERTICAL",
            "BEAR_CALL_CREDIT_VERTICAL",
            "BEAR_PUT_DEBIT_VERTICAL",
            "BULL_PUT_CREDIT_VERTICAL",
            "LONG_CALL_BUTTERFLY",
        }:
            raise ValueError("management structure is unsupported")
        if self.review_state not in {
            "HOLD_MONITOR",
            "EXIT_REVIEW",
            "TAKE_PROFIT_REVIEW",
            "TIME_EXIT_REVIEW",
            "THESIS_INVALIDATION_REVIEW",
        }:
            raise ValueError("management review state is unsupported")
        for field in (
            "thesis_invalidation_state",
            "risk_stop_state",
            "profit_take_state",
            "time_stop_state",
        ):
            if getattr(self, field) not in {"CLEAR", "TRIGGERED", "NOT_EVALUATED"}:
                raise ValueError(f"{field} is unsupported")
        for field in ("oldest_quote_age_seconds", "maximum_leg_skew_seconds"):
            value = _decimal(getattr(self, field), field)
            if value < ZERO:
                raise ValueError(f"{field} must be non-negative")
            object.__setattr__(self, field, value)
        if not isinstance(self.transition_proof, PositionTransitionProof):
            raise TypeError("transition_proof must be a PositionTransitionProof")
        if not self.transition_proof.verify_hash():
            raise ValueError("transition proof hash is invalid")
        if not isinstance(self.exit_plan, ExitPlan):
            raise TypeError("exit_plan must be an ExitPlan")
        for field in (
            "exit_contract_hash",
            "assignment_assumption_hash",
            "broker_snapshot_hash",
            "quote_batch_hash",
            "positions_state_hash",
            "execution_cost_contract_hash",
            "candidate_hash",
        ):
            _digest(getattr(self, field), field)
        for item in self.secdef_hashes:
            _digest(item, "secdef_hash")
        if not (
            self.review_only
            and self.dry_run_only
            and not self.approval_enabled
            and not self.direct_order_submission
        ):
            raise ValueError("management candidate must remain preview-only")
        if self.schema != CANDIDATE_SCHEMA:
            raise ValueError("management candidate schema mismatch")
        if self.candidate_hash != "0" * 64 and not self.verify_hash():
            raise ValueError("candidate_hash does not bind management candidate")

    @property
    def kind(self) -> PositionManagementKind:
        return self.management_kind

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "candidate_id": self.candidate_id,
            "management_kind": self.management_kind.value,
            "symbol": self.symbol,
            "structure": self.structure,
            "review_state": self.review_state,
            "thesis_invalidation_state": self.thesis_invalidation_state,
            "risk_stop_state": self.risk_stop_state,
            "profit_take_state": self.profit_take_state,
            "time_stop_state": self.time_stop_state,
            "entry_net_cost_usd": self.entry_net_cost_usd,
            "entry_net_credit_usd": self.entry_net_credit_usd,
            "entry_max_loss_usd": self.entry_max_loss_usd,
            "entry_max_profit_usd": self.entry_max_profit_usd,
            "stop_review_cashflow_usd": self.stop_review_cashflow_usd,
            "profit_review_cashflow_usd": self.profit_review_cashflow_usd,
            "expiration": self.expiration,
            "combo_quantity_before": self.combo_quantity_before,
            "combo_quantity_after": self.combo_quantity_after,
            "unit_ratio": self.unit_ratio,
            "execution_legs": [item.as_dict() for item in self.execution_legs],
            "executable_close_cashflow_usd": self.executable_close_cashflow_usd,
            "estimated_commission_usd": self.estimated_commission_usd,
            "normal_slippage_usd": self.normal_slippage_usd,
            "stress_slippage_usd": self.stress_slippage_usd,
            "estimated_execution_cost_usd": self.estimated_execution_cost_usd,
            "all_in_close_cashflow_usd": self.all_in_close_cashflow_usd,
            "before_payoff_hash": self.before_payoff.payoff_hash,
            "after_payoff_hash": self.after_payoff.payoff_hash,
            "before_risk": self.transition_proof.before_risk.as_dict(),
            "after_risk": self.transition_proof.after_risk.as_dict(),
            "transition_proof_hash": self.transition_proof.proof_hash,
            "exit_plan": self.exit_plan.as_dict(),
            "exit_contract_hash": self.exit_contract_hash,
            "assignment_assumption_hash": self.assignment_assumption_hash,
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "quote_batch_id": self.quote_batch_id,
            "quote_batch_hash": self.quote_batch_hash,
            "oldest_quote_age_seconds": self.oldest_quote_age_seconds,
            "maximum_leg_skew_seconds": self.maximum_leg_skew_seconds,
            "secdef_hashes": self.secdef_hashes,
            "positions_state_hash": self.positions_state_hash,
            "execution_cost_contract_version": self.execution_cost_contract_version,
            "execution_cost_contract_hash": self.execution_cost_contract_hash,
            "review_only": self.review_only,
            "dry_run_only": self.dry_run_only,
            "approval_enabled": self.approval_enabled,
            "direct_order_submission": self.direct_order_submission,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.candidate_hash

    def preview_payload(self) -> dict[str, object]:
        payload = {
            **self.hash_payload(),
            "candidate_hash": self.candidate_hash,
            "before_payoff": self.before_payoff.as_dict(),
            "after_payoff": self.after_payoff.as_dict(),
            "transition_proof": _proof_preview(self.transition_proof),
        }
        return _json_safe(payload)  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class ManagementGenerationResult:
    status: str
    candidates: tuple[ManagementCandidate | HoldingsClosePreview, ...]
    reason_codes: tuple[str, ...]
    suppressed_reason_codes: tuple[str, ...]
    generated_at: datetime | None
    broker_snapshot_hash: str | None
    result_hash: str
    approval_enabled: bool = False
    review_only: bool = True
    direct_order_submission: bool = False
    schema: str = RESULT_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))
        object.__setattr__(
            self,
            "suppressed_reason_codes",
            tuple(self.suppressed_reason_codes),
        )

    @property
    def no_trade(self) -> bool:
        return self.status == ManagementGenerationStatus.NO_TRADE.value

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "status": self.status,
            "candidate_hashes": [item.candidate_hash for item in self.candidates],
            "reason_codes": self.reason_codes,
            "suppressed_reason_codes": self.suppressed_reason_codes,
            "generated_at": self.generated_at,
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "approval_enabled": self.approval_enabled,
            "review_only": self.review_only,
            "direct_order_submission": self.direct_order_submission,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.result_hash

    def as_dict(self) -> dict[str, object]:
        payload = {
            **self.hash_payload(),
            "result_hash": self.result_hash,
            "candidates": [item.preview_payload() for item in self.candidates],
            "mode": "POSITION_MANAGEMENT",
            "available": True,
            "decision": "NO_TRADE" if self.no_trade else "PREVIEW_ONLY",
            "reason": (
                self.reason_codes[0]
                if self.reason_codes
                else "SNAPSHOT_DERIVED_MANAGEMENT_PREVIEW"
            ),
        }
        return _json_safe(payload)  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class _CostPolicy:
    contract: SignedContract
    per_contract_side: Decimal
    minimum_per_order: Decimal
    planned_exit_floor: Decimal
    planned_exit_spread_factor: Decimal
    stress_multiplier: Decimal
    quantum: Decimal
    assignment_assumption_hash: str


@dataclass(frozen=True, slots=True)
class _CostEstimate:
    commission: Decimal
    normal_slippage: Decimal
    stress_slippage: Decimal

    @property
    def total(self) -> Decimal:
        return self.commission + self.stress_slippage


@dataclass(frozen=True, slots=True)
class _ExitBinding:
    plan: ExitPlan
    contract_hash: str
    calendar_complete: bool
    short_leg_exit_deadline: date | None
    document: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _Structure:
    symbol: str
    structure: str
    positions: tuple[AuthoritativePosition, ...]
    unit_ratio: tuple[int, ...]
    combo_quantity: int
    expiration: date
    identities: Mapping[int, Mapping[str, object]]
    quotes: Mapping[int, BatchedOptionQuote]
    entry_net_cost_usd: Decimal | None
    entry_net_credit_usd: Decimal | None
    entry_max_loss_usd: Decimal | None
    entry_max_profit_usd: Decimal | None


class ManagementCandidateGenerator:
    """Generate immutable defined-risk previews from broker truth only."""

    def __init__(self, position_manager: PositionManager | None = None) -> None:
        self.position_manager = position_manager or PositionManager()

    def generate(
        self,
        snapshot: AtomicBrokerSnapshot,
        exit_contract: ExitPlan | Mapping[str, object],
        cost_contract: SignedContract | Mapping[str, object],
    ) -> ManagementGenerationResult:
        suppressed: list[str] = []
        try:
            before = self.position_manager.normalize_snapshot(snapshot)
            try:
                structure = self._structure(snapshot, before)
            except _GenerationRejected as shape_error:
                if shape_error.code != "UNSUPPORTED_DEFINED_RISK_POSITION_SHAPE":
                    raise
                preview = build_holdings_close_preview(
                    snapshot,
                    position_manager=self.position_manager,
                    cost_policy=self._cost_policy(cost_contract, as_of=snapshot.built_at),
                )
                # Mathematical full-set closure is not a strategy recommendation,
                # calendar clearance, execution guarantee, or eligibility authority.
                result = _result(
                    status=ManagementGenerationStatus.NO_TRADE,
                    candidates=(preview,),
                    reason_codes=preview.reason_codes,
                    suppressed_reason_codes=("REDUCE_RISK_GROUPING_NOT_INFERRED",),
                    generated_at=snapshot.built_at,
                    broker_snapshot_hash=snapshot.snapshot_hash,
                )
                self.position_manager.publish_read_model(result.as_dict())
                return result
            exit_binding = self._exit_binding(
                exit_contract,
                now=snapshot.built_at,
                expiration=structure.expiration,
            )
            cost_policy = self._cost_policy(cost_contract, as_of=snapshot.built_at)
            before_payoff, before_risk = self._position_risk(
                f"{structure.symbol.lower()}-before",
                structure.positions,
                structure,
                cost_policy,
            )
            close = self._candidate(
                snapshot=snapshot,
                structure=structure,
                kind=PositionManagementKind.CLOSE_ALL,
                deltas=tuple(
                    PositionDelta(item.contract_id, -item.signed_quantity)
                    for item in structure.positions
                ),
                before_payoff=before_payoff,
                before_risk=before_risk,
                after_positions=(),
                after_payoff=_flat_payoff(),
                after_risk=PositionRiskMetrics(ZERO, ZERO, ZERO),
                combo_quantity_after=0,
                exit_binding=exit_binding,
                cost_policy=cost_policy,
                candidate_suffix="all",
            )
        except (
            _GenerationRejected,
            TransitionRejected,
            ContractValidationError,
            TypeError,
            ValueError,
        ) as exc:
            result = self._no_trade(exc, snapshot=snapshot)
            self.position_manager.publish_read_model(result.as_dict())
            return result

        candidates = [close]
        if structure.combo_quantity <= 1:
            suppressed.append("REDUCE_RISK_COMBO_QUANTITY_ONE")
        elif not exit_binding.calendar_complete:
            suppressed.append("REDUCE_RISK_CALENDAR_EVIDENCE_INCOMPLETE")
        elif (
            exit_binding.short_leg_exit_deadline is None
            or exit_binding.plan.maximum_holding_date
            > exit_binding.short_leg_exit_deadline
        ):
            suppressed.append("REDUCE_RISK_SHORT_LEG_DEADLINE_INVALID")
        else:
            for units in range(1, structure.combo_quantity):
                try:
                    deltas = tuple(
                        PositionDelta(
                            position.contract_id,
                            -structure.unit_ratio[index] * units,
                        )
                        for index, position in enumerate(structure.positions)
                    )
                    after_positions = tuple(
                        replace(
                            position,
                            signed_quantity=(
                                position.signed_quantity
                                + deltas[index].signed_quantity_delta
                            ),
                        )
                        for index, position in enumerate(structure.positions)
                    )
                    after_payoff, after_risk = self._position_risk(
                        f"{structure.symbol.lower()}-after-{units}",
                        after_positions,
                        structure,
                        cost_policy,
                    )
                    candidates.append(
                        self._candidate(
                            snapshot=snapshot,
                            structure=structure,
                            kind=PositionManagementKind.REDUCE_RISK,
                            deltas=deltas,
                            before_payoff=before_payoff,
                            before_risk=before_risk,
                            after_positions=after_positions,
                            after_payoff=after_payoff,
                            after_risk=after_risk,
                            combo_quantity_after=(
                                structure.combo_quantity - units
                            ),
                            exit_binding=exit_binding,
                            cost_policy=cost_policy,
                            candidate_suffix=str(units),
                        )
                    )
                except (TransitionRejected, _GenerationRejected, TypeError, ValueError):
                    suppressed.append("REDUCE_RISK_REJECTED")

        result = _result(
            status=ManagementGenerationStatus.CANDIDATES,
            candidates=tuple(candidates),
            reason_codes=(),
            suppressed_reason_codes=tuple(sorted(set(suppressed))),
            generated_at=snapshot.built_at,
            broker_snapshot_hash=snapshot.snapshot_hash,
        )
        self.position_manager.publish_read_model(result.as_dict())
        return result

    generate_from_snapshot = generate

    def read_model(self) -> Mapping[str, object]:
        return self.position_manager.read_model()

    latest = read_model
    management = read_model

    @staticmethod
    def _structure(
        snapshot: AtomicBrokerSnapshot,
        positions: tuple[AuthoritativePosition, ...],
    ) -> _Structure:
        evidence_by_id = {item.contract_id: item for item in snapshot.secdef_evidence}
        quote_by_id = {item.contract_id: item for item in snapshot.quotes}
        rows: list[tuple[Decimal, AuthoritativePosition, Mapping[str, object]]] = []
        for position in positions:
            evidence = evidence_by_id.get(position.contract_id)
            if evidence is None or not isinstance(evidence.post_identity, Mapping):
                _reject("SECDEF_INCOMPLETE", "position secdef is missing")
            identity = evidence.post_identity
            strike = identity.get("strike")
            if not isinstance(strike, Decimal):
                _reject("SECDEF_INCOMPLETE", "position strike is missing")
            rows.append((strike, position, identity))
        rows.sort(key=lambda item: item[0])
        if len(rows) not in {2, 3} or len({item[0] for item in rows}) != len(rows):
            _reject(
                "UNSUPPORTED_DEFINED_RISK_POSITION_SHAPE",
                "management requires a two-leg vertical or three-leg call butterfly",
            )
        ordered = tuple(item[1] for item in rows)
        quantities = tuple(item.signed_quantity for item in ordered)
        combo_quantity = abs(quantities[0])
        for quantity in quantities[1:]:
            combo_quantity = gcd(combo_quantity, abs(quantity))
        unit_ratio = tuple(item // combo_quantity for item in quantities)
        expirations = {item[2].get("expiry") for item in rows}
        rights = {item[2].get("right") for item in rows}
        symbols = {item.symbol for item in ordered}
        if len(expirations) != 1 or len(rights) != 1 or len(symbols) != 1:
            _reject(
                "UNSUPPORTED_DEFINED_RISK_POSITION_SHAPE",
                "all legs must share one underlying, expiration, and option right",
            )
        right = next(iter(rights))
        if len(rows) == 2:
            if right == "C" and unit_ratio == (1, -1):
                structure_name = "BULL_CALL_DEBIT_VERTICAL"
            elif right == "C" and unit_ratio == (-1, 1):
                structure_name = "BEAR_CALL_CREDIT_VERTICAL"
            elif right == "P" and unit_ratio == (-1, 1):
                structure_name = "BEAR_PUT_DEBIT_VERTICAL"
            elif right == "P" and unit_ratio == (1, -1):
                structure_name = "BULL_PUT_CREDIT_VERTICAL"
            else:
                _reject(
                    "UNSUPPORTED_DEFINED_RISK_POSITION_SHAPE",
                    "two-leg position must be a standard same-expiry vertical",
                )
        elif right == "C" and unit_ratio == (1, -2, 1):
            structure_name = "LONG_CALL_BUTTERFLY"
        else:
            _reject(
                "UNSUPPORTED_DEFINED_RISK_POSITION_SHAPE",
                "three-leg position must be a long call butterfly",
            )
        expiration = next(iter(expirations))
        if not isinstance(expiration, date) or isinstance(expiration, datetime):
            _reject("SECDEF_INCOMPLETE", "expiration must be a date")
        if expiration < snapshot.built_at.date():
            _reject("POSITION_EXPIRED", "expired options cannot form a close preview")
        identities = MappingProxyType(
            {item[1].contract_id: item[2] for item in rows}
        )
        entry_net_cost = None
        entry_net_credit = None
        entry_max_loss = None
        entry_max_profit = None
        if len(rows) == 2:
            positions_evidence = snapshot.state_evidence.get("positions")
            raw_state = None if positions_evidence is None else positions_evidence.state
            average_costs: dict[int, Decimal] = {}
            if isinstance(raw_state, tuple):
                for raw in raw_state:
                    if not isinstance(raw, Mapping):
                        continue
                    con_id = raw.get("contract_id", raw.get("conId"))
                    average_cost = raw.get("average_cost", raw.get("averageCost"))
                    if (
                        isinstance(con_id, int)
                        and not isinstance(con_id, bool)
                        and isinstance(average_cost, Decimal)
                        and average_cost.is_finite()
                        and average_cost >= ZERO
                    ):
                        average_costs[con_id] = average_cost
            if set(average_costs) >= {item.contract_id for item in ordered}:
                raw_entry = sum(
                    average_costs[item.contract_id]
                    * Decimal(item.signed_quantity)
                    for item in ordered
                )
                width = (rows[1][0] - rows[0][0]) * STANDARD_MULTIPLIER
                width *= Decimal(combo_quantity)
                if raw_entry > ZERO and width > raw_entry:
                    entry_net_cost = _money(raw_entry)
                    entry_max_loss = entry_net_cost
                    entry_max_profit = _money(width - raw_entry)
                elif raw_entry < ZERO and width > -raw_entry:
                    entry_net_credit = _money(-raw_entry)
                    entry_max_loss = _money(width - entry_net_credit)
                    entry_max_profit = entry_net_credit
        return _Structure(
            symbol=next(iter(symbols)),
            structure=structure_name,
            positions=ordered,
            unit_ratio=unit_ratio,
            combo_quantity=combo_quantity,
            expiration=expiration,
            identities=identities,
            quotes=MappingProxyType(quote_by_id),
            entry_net_cost_usd=entry_net_cost,
            entry_net_credit_usd=entry_net_credit,
            entry_max_loss_usd=entry_max_loss,
            entry_max_profit_usd=entry_max_profit,
        )

    @staticmethod
    def _exit_binding(
        value: ExitPlan | Mapping[str, object],
        *,
        now: datetime,
        expiration: date,
    ) -> _ExitBinding:
        if isinstance(value, ExitPlan):
            raw: Mapping[str, object] = value.as_dict()
            plan = value
        elif isinstance(value, Mapping):
            raw = value
            plan_value = raw.get("exit_plan", raw)
            if not isinstance(plan_value, Mapping):
                _reject("EXIT_CONTRACT_INVALID", "exit_plan must be an object")
            try:
                plan = ExitPlan(
                    thesis_invalidation=plan_value.get(  # type: ignore[arg-type]
                        "thesis_invalidation"
                    ),
                    risk_stop=plan_value.get("risk_stop"),  # type: ignore[arg-type]
                    profit_take=plan_value.get("profit_take"),  # type: ignore[arg-type]
                    time_stop=plan_value.get("time_stop"),  # type: ignore[arg-type]
                    maximum_holding_date=_date(
                        plan_value.get("maximum_holding_date"),
                        "maximum_holding_date",
                    ),
                    bad_quote_action=plan_value.get("bad_quote_action"),  # type: ignore[arg-type]
                )
            except (TypeError, ValueError) as exc:
                raise _GenerationRejected("EXIT_CONTRACT_INVALID", str(exc)) from exc
        else:
            _reject("EXIT_CONTRACT_INVALID", "exit_contract is required")
        if plan.maximum_holding_date < now.date() or plan.maximum_holding_date > expiration:
            _reject(
                "EXIT_CONTRACT_INVALID",
                "maximum_holding_date must be current and no later than expiration",
            )
        expiration_hash = raw.get("expiration_calendar_hash")
        dividend_hash = raw.get("ex_dividend_calendar_hash")
        early_exercise_risk = raw.get("early_exercise_risk")
        deadline_raw = raw.get("short_leg_exit_deadline")
        deadline = None if deadline_raw is None else _date(deadline_raw, "short_leg_exit_deadline")
        calendar_complete = bool(
            _is_digest(expiration_hash)
            and _is_digest(dividend_hash)
            and isinstance(early_exercise_risk, str)
            and early_exercise_risk.strip().upper() == "CLEAR"
            and deadline is not None
        )
        document = {
            "schema": EXIT_SCHEMA,
            "exit_plan": plan.as_dict(),
            "expiration_calendar_hash": expiration_hash,
            "ex_dividend_calendar_hash": dividend_hash,
            "early_exercise_risk": early_exercise_risk,
            "short_leg_exit_deadline": None if deadline is None else deadline.isoformat(),
        }
        computed_hash = canonical_hash(document)
        supplied_hash = raw.get("exit_contract_hash", raw.get("contract_hash"))
        if supplied_hash is not None and supplied_hash != computed_hash:
            _reject("EXIT_CONTRACT_HASH_MISMATCH", "exit contract hash was tampered")
        return _ExitBinding(
            plan=plan,
            contract_hash=computed_hash,
            calendar_complete=calendar_complete,
            short_leg_exit_deadline=deadline,
            document=MappingProxyType(document),
        )

    @staticmethod
    def _cost_policy(
        value: SignedContract | Mapping[str, object],
        *,
        as_of: datetime,
    ) -> _CostPolicy:
        try:
            contract = verify_contract(
                value,
                expected_kind=ContractKind.EXECUTION_COST,
                as_of=as_of,
            )
        except (ContractValidationError, TypeError, ValueError) as exc:
            raise _GenerationRejected(
                "EXECUTION_COST_CONTRACT_INVALID",
                str(exc),
            ) from exc
        payload = _document(contract.payload, "execution cost payload")
        application = _document(payload.get("application_scope"), "application_scope")
        scope = application.get("existing_position_management")
        if not isinstance(scope, str) or "never charge historical entry cost twice" not in scope:
            _reject(
                "EXECUTION_COST_CONTRACT_INVALID",
                "existing-position cost scope is missing",
            )
        commission = _document(payload.get("commission_and_fees"), "commission_and_fees")
        quote_policy = _document(
            payload.get("quote_spread_and_slippage"),
            "quote_spread_and_slippage",
        )
        adverse = _document(quote_policy.get("adverse_slippage"), "adverse_slippage")
        precision = _document(payload.get("precision_and_aggregation"), "precision")
        assignment = _document(
            payload.get("assignment_exercise_and_dividend"),
            "assignment_exercise_and_dividend",
        )
        planned_formula = adverse.get("planned_exit_per_option_share")
        if planned_formula != "max(0.02 USD, 0.50 * displayed_spread)":
            _reject(
                "EXECUTION_COST_CONTRACT_INVALID",
                "planned-exit slippage formula is unsupported",
            )
        if (
            precision.get("binary_float_allowed") is not False
            or precision.get("rounding") != "ROUND_CEILING"
            or commission.get("negative_fee_or_rebate_credit_allowed") is not False
        ):
            _reject("EXECUTION_COST_CONTRACT_INVALID", "cost precision semantics mismatch")
        assignment_policy = _document(assignment.get("assignment"), "assignment")
        exercise_policy = _document(assignment.get("exercise"), "exercise")
        ex_dividend_policy = _document(assignment.get("ex_dividend"), "ex_dividend")
        if (
            assignment_policy.get("planned_assignment_allowed") is not False
            or exercise_policy.get("planned_exercise_allowed") is not False
            or exercise_policy.get("creator_authority") != "NONE"
            or ex_dividend_policy.get("authoritative_calendar_required") is not True
            or assignment.get("unknown_expiration_calendar") != "NO_TRADE"
        ):
            _reject(
                "EXECUTION_COST_CONTRACT_INVALID",
                "assignment/exercise/dividend assumptions mismatch",
            )
        policy = _CostPolicy(
            contract=contract,
            per_contract_side=_decimal(
                commission.get("fallback_usd_per_contract_side"),
                "fallback_usd_per_contract_side",
                positive=True,
            ),
            minimum_per_order=_decimal(
                commission.get("minimum_usd_per_order"),
                "minimum_usd_per_order",
                positive=True,
            ),
            planned_exit_floor=Decimal("0.02"),
            planned_exit_spread_factor=Decimal("0.50"),
            stress_multiplier=_decimal(
                adverse.get("event_or_stop_exit_multiplier"),
                "event_or_stop_exit_multiplier",
                positive=True,
            ),
            quantum=_decimal(
                precision.get("cost_quantum_usd"),
                "cost_quantum_usd",
                positive=True,
            ),
            assignment_assumption_hash=canonical_hash(assignment),
        )
        if policy.quantum != CENT:
            _reject("EXECUTION_COST_CONTRACT_INVALID", "cost quantum must be one cent")
        return policy

    def _candidate(
        self,
        *,
        snapshot: AtomicBrokerSnapshot,
        structure: _Structure,
        kind: PositionManagementKind,
        deltas: tuple[PositionDelta, ...],
        before_payoff: ManagementPayoffMetrics,
        before_risk: PositionRiskMetrics,
        after_positions: tuple[AuthoritativePosition, ...],
        after_payoff: ManagementPayoffMetrics,
        after_risk: PositionRiskMetrics,
        combo_quantity_after: int,
        exit_binding: _ExitBinding,
        cost_policy: _CostPolicy,
        candidate_suffix: str,
    ) -> ManagementCandidate:
        proof = self.position_manager.prove_transition(
            snapshot,
            kind=kind,
            deltas=deltas,
            before_risk=before_risk,
            after_risk=after_risk,
            exit_contract_hash=exit_binding.contract_hash,
            execution_cost_contract=cost_policy.contract,
        )
        expected_after = {
            item.contract_id: item.signed_quantity for item in after_positions
        }
        proof_after = {
            item.contract_id: item.signed_quantity for item in proof.after_positions
        }
        if expected_after != proof_after:
            _reject("TRANSITION_PROOF_MISMATCH", "manager after state disagrees")
        costs = self._costs(deltas, structure, cost_policy)
        cashflow = self._close_cashflow(deltas, structure)
        execution_legs = self._execution_legs(deltas, structure, proof)
        candidate_id = (
            f"{structure.symbol.lower()}-{kind.value.lower().replace('_', '-')}-{candidate_suffix}-"
            f"{snapshot.snapshot_hash[:12]}"
        )
        time_stop_state = (
            "TRIGGERED"
            if snapshot.built_at.date() >= exit_binding.plan.maximum_holding_date
            else "CLEAR"
        )
        all_in_cashflow = _money(cashflow - costs.total)
        closed_combo_quantity = structure.combo_quantity - combo_quantity_after
        if not 1 <= closed_combo_quantity <= structure.combo_quantity:
            _reject(
                "MANAGEMENT_QUANTITY_INVALID",
                "candidate must close at least one and at most all combinations",
            )
        candidate_fraction = Decimal(closed_combo_quantity) / Decimal(
            structure.combo_quantity
        )
        if (
            structure.entry_net_cost_usd is not None
            and structure.entry_max_profit_usd is not None
        ):
            stop_threshold = _money(
                structure.entry_net_cost_usd
                * candidate_fraction
                * Decimal("0.60")
            )
            profit_threshold = _money(
                structure.entry_net_cost_usd * candidate_fraction
                + structure.entry_max_profit_usd
                * candidate_fraction
                * Decimal("0.60")
            )
        elif (
            structure.entry_net_credit_usd is not None
            and structure.entry_max_loss_usd is not None
        ):
            # Close cashflow is negative for a credit vertical.  A larger
            # debit-to-close is therefore a more negative number.
            stop_threshold = -_money(
                structure.entry_net_credit_usd * candidate_fraction
                + structure.entry_max_loss_usd
                * candidate_fraction
                * Decimal("0.60")
            )
            profit_threshold = -_money(
                structure.entry_net_credit_usd
                * candidate_fraction
                * Decimal("0.40")
            )
        else:
            stop_threshold = None
            profit_threshold = None
        risk_stop_state = (
            "NOT_EVALUATED"
            if stop_threshold is None
            else "TRIGGERED" if all_in_cashflow <= stop_threshold else "CLEAR"
        )
        profit_take_state = (
            "NOT_EVALUATED"
            if profit_threshold is None
            else "TRIGGERED" if all_in_cashflow >= profit_threshold else "CLEAR"
        )
        if time_stop_state == "TRIGGERED":
            review_state = "TIME_EXIT_REVIEW"
        elif risk_stop_state == "TRIGGERED":
            review_state = "EXIT_REVIEW"
        elif profit_take_state == "TRIGGERED":
            review_state = "TAKE_PROFIT_REVIEW"
        else:
            review_state = "HOLD_MONITOR"
        fields = dict(
            candidate_id=candidate_id,
            symbol=structure.symbol,
            structure=structure.structure,
            review_state=review_state,
            # These textual exit rules are displayed and hash-bound, but they
            # are not executable predicates.  Do not fabricate a trigger from
            # prose or a position mark.  A future signed predicate evaluator
            # may replace NOT_EVALUATED without changing this authority wall.
            thesis_invalidation_state="NOT_EVALUATED",
            risk_stop_state=risk_stop_state,
            profit_take_state=profit_take_state,
            time_stop_state=time_stop_state,
            entry_net_cost_usd=structure.entry_net_cost_usd,
            entry_net_credit_usd=structure.entry_net_credit_usd,
            entry_max_loss_usd=structure.entry_max_loss_usd,
            entry_max_profit_usd=structure.entry_max_profit_usd,
            stop_review_cashflow_usd=stop_threshold,
            profit_review_cashflow_usd=profit_threshold,
            management_kind=kind,
            expiration=structure.expiration,
            combo_quantity_before=structure.combo_quantity,
            combo_quantity_after=combo_quantity_after,
            unit_ratio=structure.unit_ratio,
            execution_legs=execution_legs,
            executable_close_cashflow_usd=_money(cashflow),
            estimated_commission_usd=costs.commission,
            normal_slippage_usd=costs.normal_slippage,
            stress_slippage_usd=costs.stress_slippage,
            estimated_execution_cost_usd=costs.total,
            all_in_close_cashflow_usd=all_in_cashflow,
            before_payoff=before_payoff,
            after_payoff=after_payoff,
            transition_proof=proof,
            exit_plan=exit_binding.plan,
            exit_contract_hash=exit_binding.contract_hash,
            assignment_assumption_hash=cost_policy.assignment_assumption_hash,
            broker_snapshot_hash=snapshot.snapshot_hash,
            quote_batch_id=snapshot.quote_batch_id or "",
            quote_batch_hash=proof.quote_batch_hash,
            oldest_quote_age_seconds=(
                snapshot.oldest_quote_age_seconds
                if snapshot.oldest_quote_age_seconds is not None
                else ZERO
            ),
            maximum_leg_skew_seconds=(
                snapshot.maximum_leg_skew_seconds
                if snapshot.maximum_leg_skew_seconds is not None
                else ZERO
            ),
            secdef_hashes=tuple(item.identity_hash for item in proof.secdef_bindings),
            positions_state_hash=proof.positions_state_hash,
            execution_cost_contract_version=cost_policy.contract.version,
            execution_cost_contract_hash=cost_policy.contract.contract_hash,
        )
        provisional = ManagementCandidate(**fields, candidate_hash="0" * 64)
        return ManagementCandidate(
            **fields,
            candidate_hash=canonical_hash(provisional.hash_payload()),
        )

    def _position_risk(
        self,
        candidate_id: str,
        positions: tuple[AuthoritativePosition, ...],
        structure: _Structure,
        cost_policy: _CostPolicy,
    ) -> tuple[ManagementPayoffMetrics, PositionRiskMetrics]:
        deltas = tuple(
            PositionDelta(item.contract_id, -item.signed_quantity)
            for item in positions
        )
        costs = self._costs(deltas, structure, cost_policy)
        leg_quotes: list[OptionLegQuote] = []
        exposure = costs.total
        for position in positions:
            identity = structure.identities[position.contract_id]
            quote = structure.quotes[position.contract_id]
            contract = _option_contract(
                identity,
                position.contract_id,
                underlying=structure.symbol,
            )
            side = (
                PositionSide.LONG
                if position.signed_quantity > 0
                else PositionSide.SHORT
            )
            leg = OptionLeg(contract, side, abs(position.signed_quantity))
            leg_quotes.append(
                OptionLegQuote(
                    leg=leg,
                    bid=quote.bid,
                    ask=quote.ask,
                    last=quote.last,
                    implied_volatility=quote.implied_volatility,
                    volume=quote.volume,
                    open_interest=quote.open_interest,
                    observed_at=quote.observed_at,
                )
            )
            assert quote.ask is not None
            exposure += (
                quote.ask * contract.multiplier * Decimal(abs(position.signed_quantity))
            )
        candidate = StrategyCandidate(
            candidate_id=candidate_id,
            leg_quotes=tuple(leg_quotes),
            estimated_commissions=costs.commission,
            estimated_slippage=costs.stress_slippage,
        )
        payoff = analyze_expiration_payoff(candidate)
        if payoff.status is not PayoffStatus.CALCULATED or payoff.max_loss is None:
            _reject("PAYOFF_NOT_EXACT", "position maximum loss is not exactly computable")
        geometry_hash = canonical_hash(
            [
                {
                    "lower_bound": item.lower_bound,
                    "upper_bound": item.upper_bound,
                    "slope": item.slope,
                    "intercept": item.intercept,
                }
                for item in payoff.segments
            ]
        )
        payoff_fields = dict(
            max_loss_usd=_money(payoff.max_loss),
            max_profit_usd=(
                None if payoff.max_profit is None else _money(payoff.max_profit)
            ),
            unbounded_profit=payoff.unbounded_profit,
            net_opening_cashflow_usd=_money(payoff.net_opening_cashflow or ZERO),
            estimated_future_exit_cost_usd=costs.total,
            breakevens=payoff.breakevens,
            geometry_hash=geometry_hash,
        )
        provisional = ManagementPayoffMetrics(**payoff_fields, payoff_hash="0" * 64)
        metrics = ManagementPayoffMetrics(
            **payoff_fields,
            payoff_hash=canonical_hash(provisional.hash_payload()),
        )
        risk = PositionRiskMetrics(
            max_loss_usd=metrics.max_loss_usd,
            exposure_usd=_money(exposure),
            capital_usage_usd=metrics.max_loss_usd,
        )
        return metrics, risk

    @staticmethod
    def _costs(
        deltas: tuple[PositionDelta, ...],
        structure: _Structure,
        policy: _CostPolicy,
    ) -> _CostEstimate:
        sides = sum(abs(item.signed_quantity_delta) for item in deltas)
        if sides <= 0:
            _reject("EXECUTION_COST_UNKNOWN", "management order has no contract sides")
        commission = _money(
            max(policy.minimum_per_order, policy.per_contract_side * Decimal(sides))
        )
        normal = ZERO
        for delta in deltas:
            quote = structure.quotes.get(delta.contract_id)
            identity = structure.identities.get(delta.contract_id)
            if quote is None or identity is None or quote.bid is None or quote.ask is None:
                _reject("EXECUTION_COST_UNKNOWN", "delta quote or secdef is unavailable")
            spread = quote.ask - quote.bid
            per_share = max(
                policy.planned_exit_floor,
                policy.planned_exit_spread_factor * spread,
            )
            normal += (
                per_share
                * STANDARD_MULTIPLIER
                * Decimal(abs(delta.signed_quantity_delta))
            )
        normal = _money(normal)
        stress = _money(normal * policy.stress_multiplier)
        return _CostEstimate(
            commission=commission,
            normal_slippage=normal,
            stress_slippage=stress,
        )

    @staticmethod
    def _close_cashflow(
        deltas: tuple[PositionDelta, ...],
        structure: _Structure,
    ) -> Decimal:
        value = ZERO
        for delta in deltas:
            quote = structure.quotes[delta.contract_id]
            if quote.bid is None or quote.ask is None:
                _reject("EXECUTABLE_PRICE_UNKNOWN", "management leg quote is missing")
            price = quote.ask if delta.signed_quantity_delta > 0 else quote.bid
            value -= (
                Decimal(delta.signed_quantity_delta)
                * price
                * STANDARD_MULTIPLIER
            )
        return value

    @staticmethod
    def _execution_legs(
        deltas: tuple[PositionDelta, ...],
        structure: _Structure,
        proof: PositionTransitionProof,
    ) -> tuple[ManagementExecutionLeg, ...]:
        before_by_id = {item.contract_id: item for item in structure.positions}
        secdef_hashes = {
            item.contract_id: item.identity_hash for item in proof.secdef_bindings
        }
        legs: list[ManagementExecutionLeg] = []
        for delta in deltas:
            identity = structure.identities[delta.contract_id]
            quote = structure.quotes[delta.contract_id]
            assert quote.bid is not None and quote.ask is not None
            buying = delta.signed_quantity_delta > 0
            legs.append(
                ManagementExecutionLeg(
                    contract_id=delta.contract_id,
                    local_symbol=str(identity["localSymbol"]),
                    expiration=identity["expiry"],  # type: ignore[arg-type]
                    strike=identity["strike"],  # type: ignore[arg-type]
                    right="CALL" if identity["right"] == "C" else "PUT",
                    current_signed_quantity=before_by_id[
                        delta.contract_id
                    ].signed_quantity,
                    signed_quantity_delta=delta.signed_quantity_delta,
                    action="BUY_TO_CLOSE" if buying else "SELL_TO_CLOSE",
                    action_quantity=abs(delta.signed_quantity_delta),
                    multiplier=STANDARD_MULTIPLIER,
                    bid=quote.bid,
                    ask=quote.ask,
                    executable_price=quote.ask if buying else quote.bid,
                    quote_observed_at=quote.observed_at,
                    secdef_identity_hash=secdef_hashes[delta.contract_id],
                )
            )
        return tuple(legs)

    @staticmethod
    def _no_trade(
        error: Exception,
        *,
        snapshot: object,
    ) -> ManagementGenerationResult:
        if isinstance(error, (_GenerationRejected, TransitionRejected)):
            code = error.code
        elif isinstance(error, ContractValidationError):
            code = "CONTRACT_INVALID"
        else:
            text = str(error).strip().upper()
            code = "_".join(text.split())[:160] or "MANAGEMENT_GENERATION_FAILED"
        built_at = snapshot.built_at if isinstance(snapshot, AtomicBrokerSnapshot) else None
        snapshot_hash = (
            snapshot.snapshot_hash
            if isinstance(snapshot, AtomicBrokerSnapshot)
            and _is_digest(snapshot.snapshot_hash)
            else None
        )
        return _result(
            status=ManagementGenerationStatus.NO_TRADE,
            candidates=(),
            reason_codes=(code,),
            suppressed_reason_codes=(),
            generated_at=built_at,
            broker_snapshot_hash=snapshot_hash,
        )


def _is_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _option_contract(
    identity: Mapping[str, object],
    contract_id: int,
    *,
    underlying: str,
) -> OptionContract:
    return OptionContract(
        contract_id=f"{contract_id}@{identity['exchange']}",
        underlying=underlying,
        expiration=identity["expiry"],  # type: ignore[arg-type]
        strike=identity["strike"],  # type: ignore[arg-type]
        right=OptionRight.CALL if identity["right"] == "C" else OptionRight.PUT,
        multiplier=STANDARD_MULTIPLIER,
        currency="USD",
        exchange=str(identity["exchange"]),
        broker_contract_id=contract_id,
    )


def _flat_payoff() -> ManagementPayoffMetrics:
    fields = dict(
        max_loss_usd=ZERO,
        max_profit_usd=ZERO,
        unbounded_profit=False,
        net_opening_cashflow_usd=ZERO,
        estimated_future_exit_cost_usd=ZERO,
        breakevens=(),
        geometry_hash=canonical_hash([]),
    )
    provisional = ManagementPayoffMetrics(**fields, payoff_hash="0" * 64)
    return ManagementPayoffMetrics(
        **fields,
        payoff_hash=canonical_hash(provisional.hash_payload()),
    )


def _proof_preview(proof: PositionTransitionProof) -> dict[str, object]:
    return {
        "management_kind": proof.management_kind.value,
        "before_positions": [item.as_dict() for item in proof.before_positions],
        "deltas": [item.as_dict() for item in proof.deltas],
        "after_positions": [item.as_dict() for item in proof.after_positions],
        "before_risk": proof.before_risk.as_dict(),
        "after_risk": proof.after_risk.as_dict(),
        "capital_usage": proof.capital_usage.as_dict(),
        "broker_snapshot_hash": proof.broker_snapshot_hash,
        "positions_state_hash": proof.positions_state_hash,
        "secdef_bindings": [item.as_dict() for item in proof.secdef_bindings],
        "quote_batch_id": proof.quote_batch_id,
        "quote_batch_hash": proof.quote_batch_hash,
        "exit_contract_hash": proof.exit_contract_hash,
        "execution_cost_contract_version": proof.execution_cost_contract_version,
        "execution_cost_contract_hash": proof.execution_cost_contract_hash,
        "verified_at": proof.verified_at,
        "proof_hash": proof.proof_hash,
    }


def _result(
    *,
    status: ManagementGenerationStatus,
    candidates: tuple[ManagementCandidate | HoldingsClosePreview, ...],
    reason_codes: tuple[str, ...],
    suppressed_reason_codes: tuple[str, ...],
    generated_at: datetime | None,
    broker_snapshot_hash: str | None,
) -> ManagementGenerationResult:
    fields = dict(
        status=status.value,
        candidates=candidates,
        reason_codes=reason_codes,
        suppressed_reason_codes=suppressed_reason_codes,
        generated_at=generated_at,
        broker_snapshot_hash=broker_snapshot_hash,
    )
    provisional = ManagementGenerationResult(**fields, result_hash="0" * 64)
    return ManagementGenerationResult(
        **fields,
        result_hash=canonical_hash(provisional.hash_payload()),
    )


def preview_json(result: ManagementGenerationResult) -> str:
    if not isinstance(result, ManagementGenerationResult) or not result.verify_hash():
        raise ValueError("management result is missing or hash-invalid")
    return json.dumps(
        result.as_dict(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


__all__ = [
    "ManagementCandidate",
    "ManagementCandidateGenerator",
    "ManagementExecutionLeg",
    "ManagementGenerationResult",
    "ManagementGenerationStatus",
    "ManagementPayoffMetrics",
    "preview_json",
]
