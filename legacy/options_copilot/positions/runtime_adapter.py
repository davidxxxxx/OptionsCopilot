"""Fail-closed production adapter for defined-risk position previews.

This module is deliberately read-only.  It discovers one coherent standard
USD option combination, builds one atomic broker snapshot through the existing
``BrokerSnapshotBuilder``, and only then delegates to
``ManagementCandidateGenerator``.  It has no broker lifecycle, approval,
instruction, bridge, or order-submission surface.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Protocol

from options_copilot.gateway.broker_snapshot import (
    AtomicBrokerSnapshot,
    BrokerSnapshotBuilder,
    BrokerSnapshotSource,
    BrokerSnapshotStatus,
)
from options_copilot.gateway.ibkr_readonly import (
    OptionContractRef,
    PositionSnapshot,
)
from options_copilot.governance.contracts import (
    ContractKind,
    ContractValidationError,
    SignedContract,
    verify_contract,
)
from options_copilot.storage.canonical import canonical_hash, utc_datetime
from options_copilot.strategies import ExitPlan

from .generator import (
    ManagementCandidateGenerator,
    ManagementGenerationResult,
    ManagementGenerationStatus,
)
from .manager import PositionManager, TransitionRejected


MAX_POSITION_AGE_SECONDS = Decimal("5")


class SnapshotBuilder(Protocol):
    """The narrow builder port used by the coordinator and its tests."""

    def build(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> AtomicBrokerSnapshot:
        ...


class ExitContractProvider(Protocol):
    """Resolve an externally sourced exit contract for one exact snapshot."""

    def __call__(
        self,
        snapshot: AtomicBrokerSnapshot,
    ) -> ExitPlan | Mapping[str, object] | None:
        ...


class ExecutionCostContractProvider(Protocol):
    """Resolve the signed execution-cost authority for one exact snapshot."""

    def __call__(
        self,
        snapshot: AtomicBrokerSnapshot,
    ) -> SignedContract | Mapping[str, object] | None:
        ...


class MarketDataGate(Protocol):
    """Authorize the bounded secdef/quote reads needed for one refresh.

    The gate owns request accounting only.  It cannot make a management
    candidate eligible and an empty result is the sole success value.
    """

    def __call__(self) -> Sequence[str]:
        ...


@dataclass(frozen=True, slots=True)
class _DiscoveryRejected(ValueError):
    code: str
    detail: str

    def __str__(self) -> str:
        return f"{self.code}: {self.detail}"


class ProductionManagementCoordinator:
    """Publish review-only close/reduce previews from live broker truth.

    ``gateway`` is assumed to be connected by the composition root.  This
    coordinator never calls ``connect`` or ``disconnect`` and never starts a
    worker thread.  A local lock merely prevents two callers from interleaving
    the multi-read atomic snapshot protocol.
    """

    def __init__(
        self,
        gateway: BrokerSnapshotSource,
        *,
        exit_contract_provider: ExitContractProvider,
        cost_contract_provider: ExecutionCostContractProvider,
        position_manager: PositionManager | None = None,
        generator: ManagementCandidateGenerator | None = None,
        snapshot_builder: SnapshotBuilder | None = None,
        market_data_gate: MarketDataGate | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not callable(exit_contract_provider):
            raise TypeError("exit_contract_provider must be callable")
        if not callable(cost_contract_provider):
            raise TypeError("cost_contract_provider must be callable")

        if generator is None:
            selected_manager = position_manager or PositionManager()
            generator = ManagementCandidateGenerator(
                position_manager=selected_manager
            )
        else:
            if not isinstance(generator, ManagementCandidateGenerator):
                raise TypeError("generator must be a ManagementCandidateGenerator")
            selected_manager = generator.position_manager
            if position_manager is not None and position_manager is not selected_manager:
                raise ValueError(
                    "generator and coordinator must share the same PositionManager"
                )

        self.gateway = gateway
        self.exit_contract_provider = exit_contract_provider
        self.cost_contract_provider = cost_contract_provider
        self.position_manager = selected_manager
        self.generator = generator
        self.market_data_gate = market_data_gate
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.snapshot_builder = snapshot_builder or BrokerSnapshotBuilder(
            gateway,
            clock=self._clock,
        )
        self._refresh_lock = threading.RLock()

    def refresh(self) -> ManagementGenerationResult:
        """Refresh one read-only preview, or publish a structured ``NO_TRADE``."""

        with self._refresh_lock:
            return self._refresh_locked()

    run = refresh
    scan = refresh

    def read_model(self) -> Mapping[str, object]:
        return self.position_manager.read_model()

    latest = read_model
    management = read_model

    def _refresh_locked(self) -> ManagementGenerationResult:
        try:
            # The gateway timestamps the returned position rows when its broker
            # read completes.  Capture discovery time afterwards so normal I/O
            # latency cannot make fresh rows appear future-dated.
            raw_positions = self.gateway.positions()
            discovered_at = utc_datetime(
                self._clock(),
                field="management discovery clock",
            )
            contracts = _contracts_from_positions(
                raw_positions,
                discovered_at=discovered_at,
            )
        except _DiscoveryRejected as exc:
            return self._publish_no_trade((exc.code,))
        except Exception:
            return self._publish_no_trade(("POSITION_DISCOVERY_FAILED",))

        order_state_reasons = _preflight_order_and_instruction_state(self.gateway)
        if order_state_reasons:
            return self._publish_no_trade(order_state_reasons)

        if self.market_data_gate is not None:
            try:
                gate_reasons = tuple(self.market_data_gate())
            except Exception:
                return self._publish_no_trade(("MARKET_DATA_GATE_FAILED",))
            if gate_reasons:
                return self._publish_no_trade(gate_reasons)

        try:
            snapshot = self.snapshot_builder.build(contracts)
        except Exception:
            return self._publish_no_trade(("BROKER_SNAPSHOT_BUILD_FAILED",))

        if not isinstance(snapshot, AtomicBrokerSnapshot):
            return self._publish_no_trade(("BROKER_SNAPSHOT_INVALID",))
        if (
            snapshot.status is not BrokerSnapshotStatus.COMPLETE
            or snapshot.reason_codes
        ):
            reasons = snapshot.reason_codes or ("BROKER_SNAPSHOT_INCOMPLETE",)
            return self._publish_no_trade(reasons, snapshot=snapshot)

        # This preflight uses the same manager later used by the generator.  It
        # closes the gap where a syntactically COMPLETE snapshot still contains
        # working orders, an unknown instruction state, mixed underlyings,
        # duplicate conIds, stale quotes, or incomplete contract definitions.
        try:
            self.position_manager.normalize_snapshot(snapshot)
        except TransitionRejected as exc:
            return self._publish_no_trade((exc.code,), snapshot=snapshot)
        except Exception:
            return self._publish_no_trade(
                ("BROKER_SNAPSHOT_VALIDATION_FAILED",),
                snapshot=snapshot,
            )

        try:
            exit_contract = self.exit_contract_provider(snapshot)
        except Exception:
            return self._publish_no_trade(
                ("EXIT_CONTRACT_PROVIDER_FAILED",),
                snapshot=snapshot,
            )
        if exit_contract is None:
            return self._publish_no_trade(
                ("EXIT_CONTRACT_UNAVAILABLE",),
                snapshot=snapshot,
            )
        if not isinstance(exit_contract, (ExitPlan, Mapping)):
            return self._publish_no_trade(
                ("EXIT_CONTRACT_INVALID",),
                snapshot=snapshot,
            )

        try:
            raw_cost_contract = self.cost_contract_provider(snapshot)
        except Exception:
            return self._publish_no_trade(
                ("EXECUTION_COST_CONTRACT_PROVIDER_FAILED",),
                snapshot=snapshot,
            )
        if raw_cost_contract is None:
            return self._publish_no_trade(
                ("EXECUTION_COST_CONTRACT_UNAVAILABLE",),
                snapshot=snapshot,
            )
        try:
            cost_contract = verify_contract(
                raw_cost_contract,
                expected_kind=ContractKind.EXECUTION_COST,
                as_of=snapshot.built_at,
            )
        except (ContractValidationError, TypeError, ValueError):
            return self._publish_no_trade(
                ("EXECUTION_COST_CONTRACT_INVALID",),
                snapshot=snapshot,
            )

        try:
            result = self.generator.generate(
                snapshot,
                exit_contract,
                cost_contract,
            )
        except Exception:
            return self._publish_no_trade(
                ("MANAGEMENT_GENERATION_FAILED",),
                snapshot=snapshot,
            )

        if (
            not isinstance(result, ManagementGenerationResult)
            or not result.verify_hash()
            or result.approval_enabled
            or not result.review_only
            or result.direct_order_submission
        ):
            return self._publish_no_trade(
                ("MANAGEMENT_AUTHORITY_VIOLATION",),
                snapshot=snapshot,
            )
        # ManagementCandidateGenerator already published this exact projection
        # to the shared PositionManager.  Publishing it a second time would add
        # no authority or freshness and is intentionally avoided.
        return result

    def _publish_no_trade(
        self,
        reason_codes: Sequence[object],
        *,
        snapshot: AtomicBrokerSnapshot | None = None,
    ) -> ManagementGenerationResult:
        reasons = tuple(
            dict.fromkeys(
                str(item).strip().upper()
                for item in reason_codes
                if str(item).strip()
            )
        ) or ("MANAGEMENT_RUNTIME_UNAVAILABLE",)
        generated_at = snapshot.built_at if snapshot is not None else self._safe_now()
        snapshot_hash = (
            snapshot.snapshot_hash
            if snapshot is not None and _is_digest(snapshot.snapshot_hash)
            else None
        )
        fields = dict(
            status=ManagementGenerationStatus.NO_TRADE.value,
            candidates=(),
            reason_codes=reasons,
            suppressed_reason_codes=(),
            generated_at=generated_at,
            broker_snapshot_hash=snapshot_hash,
        )
        provisional = ManagementGenerationResult(
            **fields,
            result_hash="0" * 64,
        )
        result = ManagementGenerationResult(
            **fields,
            result_hash=canonical_hash(provisional.hash_payload()),
        )
        self.position_manager.publish_read_model(result.as_dict())
        return result

    def _safe_now(self) -> datetime | None:
        try:
            return utc_datetime(self._clock(), field="management clock")
        except (TypeError, ValueError):
            return None


def _contracts_from_positions(
    raw_positions: object,
    *,
    discovered_at: datetime,
) -> tuple[OptionContractRef, ...]:
    if isinstance(raw_positions, (str, bytes, bytearray, memoryview)) or not isinstance(
        raw_positions,
        Sequence,
    ):
        raise _DiscoveryRejected(
            "POSITION_DISCOVERY_INVALID",
            "gateway positions must be an immutable-compatible sequence",
        )

    contracts: list[OptionContractRef] = []
    seen: set[int] = set()
    for row in raw_positions:
        if not isinstance(row, PositionSnapshot):
            raise _DiscoveryRejected(
                "POSITION_FIELDS_INCOMPLETE",
                "gateway position row must be PositionSnapshot",
            )
        quantity = row.quantity
        if (
            not isinstance(quantity, Decimal)
            or not quantity.is_finite()
            or quantity != quantity.to_integral_value()
        ):
            raise _DiscoveryRejected(
                "POSITION_QUANTITY_INVALID",
                "position quantity must be a finite exact integer",
            )
        if quantity == 0:
            continue

        contract_id = row.contract_id
        if isinstance(contract_id, bool) or not isinstance(contract_id, int) or contract_id <= 0:
            raise _DiscoveryRejected(
                "POSITION_FIELDS_INCOMPLETE",
                "position conId must be a positive integer",
            )
        if contract_id in seen:
            raise _DiscoveryRejected(
                "DUPLICATE_POSITION_CONID",
                f"duplicate nonzero position conId {contract_id}",
            )
        seen.add(contract_id)

        symbol = _required_text(row.symbol, "symbol", uppercase=True)
        security_type = _required_text(
            row.security_type,
            "security_type",
            uppercase=True,
        )
        currency = _required_text(row.currency, "currency", uppercase=True)
        if security_type != "OPT" or currency != "USD":
            raise _DiscoveryRejected(
                "UNSUPPORTED_POSITION",
                "all nonzero positions must be standard USD options",
            )

        try:
            observed_at = utc_datetime(row.asof, field="position.asof")
        except (TypeError, ValueError) as exc:
            raise _DiscoveryRejected(
                "POSITION_FIELDS_INCOMPLETE",
                "position asof must be timezone-aware",
            ) from exc
        age = Decimal(str((discovered_at - observed_at).total_seconds()))
        if age < 0 or age > MAX_POSITION_AGE_SECONDS:
            raise _DiscoveryRejected(
                "POSITION_SNAPSHOT_STALE_OR_FUTURE",
                "position snapshot must be no more than five seconds old",
            )

        expiration = row.expiration
        strike = row.strike
        right = row.right
        multiplier = row.multiplier
        if (
            not isinstance(expiration, date)
            or isinstance(expiration, datetime)
            or not isinstance(strike, Decimal)
            or not strike.is_finite()
            or strike <= 0
            or right not in {"C", "P"}
            or isinstance(multiplier, bool)
            or multiplier != 100
        ):
            raise _DiscoveryRejected(
                "POSITION_FIELDS_INCOMPLETE",
                "option expiry, strike, right, and standard multiplier are required",
            )

        local_symbol = _required_text(row.local_symbol, "local_symbol")
        exchange = _required_text(row.exchange, "exchange", uppercase=True)
        trading_class = _required_text(
            row.trading_class,
            "trading_class",
            uppercase=True,
        )
        contracts.append(
            OptionContractRef(
                contract_id=contract_id,
                contract_id_ex=f"{contract_id}@{exchange}",
                symbol=symbol,
                local_symbol=local_symbol,
                expiration=expiration,
                strike=strike,
                right=right,
                exchange=exchange,
                trading_class=trading_class,
                multiplier=multiplier,
                currency=currency,
            )
        )

    if not contracts:
        raise _DiscoveryRejected(
            "NO_OPEN_OPTION_POSITION",
            "no current nonzero option position exists",
        )
    symbols = {item.symbol for item in contracts}
    if len(symbols) != 1:
        raise _DiscoveryRejected(
            "MULTIPLE_OPEN_COMBINATIONS",
            "management requires one coherent option underlying",
        )
    return tuple(sorted(contracts, key=lambda item: item.contract_id))


def _preflight_order_and_instruction_state(
    gateway: BrokerSnapshotSource,
) -> tuple[str, ...]:
    """Reject known blockers before any secdef or quote request is attempted.

    ``BrokerSnapshotBuilder`` still rereads and hash-compares these components
    around the market-data batch.  This first pass only prevents needless
    quote traffic when the state is already unsafe or unknowable.
    """

    try:
        working_orders = gateway.working_orders()
    except Exception:
        return ("WORKING_ORDERS_READ_FAILED",)
    if not _is_sequence(working_orders):
        return ("WORKING_ORDERS_UNKNOWN",)
    if working_orders:
        return ("WORKING_ORDERS_PRESENT",)

    try:
        instructions = gateway.unsubmitted_instructions()
    except Exception:
        return ("UNSUBMITTED_INSTRUCTIONS_READ_FAILED",)
    if not _is_sequence(instructions):
        return ("UNSUBMITTED_INSTRUCTIONS_UNKNOWN",)
    if instructions:
        return ("UNSUBMITTED_INSTRUCTIONS_PRESENT",)
    return ()


def _is_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    )


def _required_text(value: object, field: str, *, uppercase: bool = False) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise _DiscoveryRejected(
            "POSITION_FIELDS_INCOMPLETE",
            f"position {field} must be a trimmed nonblank string",
        )
    return value.upper() if uppercase else value


def _is_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


__all__ = [
    "ExecutionCostContractProvider",
    "ExitContractProvider",
    "MAX_POSITION_AGE_SECONDS",
    "MarketDataGate",
    "ProductionManagementCoordinator",
    "SnapshotBuilder",
]
