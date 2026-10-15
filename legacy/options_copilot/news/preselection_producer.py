"""Fail-closed producer for the independent supporting-only Top-10 ledger.

The producer has two exact America/New_York slots.  At 09:20 it freezes at
most ten independently resolved structures.  At 09:35 it reprices only the
exact structures recovered from that day's 09:20 ledger head.  It has no
authorization or execution seam.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import MappingProxyType
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from options_copilot.gateway.broker_snapshot import BrokerSnapshotStatus
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.gateway.ibkr_readonly import (
    OptionContractRef as GatewayOptionContractRef,
    QuoteBatchStatus,
)
from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    NewsAuthority,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
    UNDERLYING_QUOTE_BASIS_SCHEMA,
    UnderlyingQuoteBasis,
)
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomicsError,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)


NEW_YORK = ZoneInfo("America/New_York")
PREMARKET_SLOT = time(9, 20)
OPEN_REPRICE_SLOT = time(9, 35)
TOP10_LIMIT = 10
MAXIMUM_QUOTE_AGE_SECONDS = Decimal("5")
_DERIVATIVE_SECURITY_TYPES = frozenset({"OPT", "BAG", "COMBO"})
_QUOTE_FIELDS = (
    "bid",
    "ask",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "volume",
    "open_interest",
)
_IDENTITY_FIELDS = (
    "conId",
    "localSymbol",
    "tradingClass",
    "multiplier",
    "exchange",
    "expiry",
    "strike",
    "right",
)


class ProducerSlot(str, Enum):
    PREMARKET_0920 = "PREMARKET_0920"
    OPEN_REPRICE_0935 = "OPEN_REPRICE_0935"


class ProducerStatus(str, Enum):
    PREMARKET_FROZEN = "PREMARKET_FROZEN"
    OPEN_REPRICED = "OPEN_REPRICED"
    POSITION_MANAGEMENT_ONLY = "POSITION_MANAGEMENT_ONLY"
    NO_TRADE = "NO_TRADE"


@dataclass(frozen=True, slots=True)
class ResolvedStructure:
    """One independent exact structure, with no rank or action authority."""

    candidate: ConditionalOptionPreselection

    def __post_init__(self) -> None:
        if not isinstance(self.candidate, ConditionalOptionPreselection):
            raise TypeError("candidate must be a ConditionalOptionPreselection")


@dataclass(frozen=True, slots=True)
class Top10StructureResolution:
    """One source read plus explicit fail-closed acquisition evidence."""

    structures: tuple[ResolvedStructure, ...]
    reason_codes: tuple[str, ...] = ()
    missing_symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        structures = tuple(self.structures)
        if any(not isinstance(item, ResolvedStructure) for item in structures):
            raise TypeError("structures must contain ResolvedStructure values")
        reasons = tuple(
            dict.fromkeys(_required_text(item) for item in self.reason_codes)
        )
        missing = tuple(
            dict.fromkeys(
                _required_text(item).upper() for item in self.missing_symbols
            )
        )
        object.__setattr__(self, "structures", structures)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "missing_symbols", missing)

    def __iter__(self):
        return iter(self.structures)

    def __len__(self) -> int:
        return len(self.structures)

    def __getitem__(self, index: int):
        return self.structures[index]


@dataclass(frozen=True, slots=True)
class ProducerResult:
    status: ProducerStatus
    reason_codes: tuple[str, ...]
    observed_at: datetime | None
    scheduled_for: datetime | None
    slot: ProducerSlot | None
    trading_date: date | None
    run_id: str | None = None
    parent_head_hash: str | None = None
    quote_batch_id: str | None = None
    written_count: int = 0
    missing_symbols: tuple[str, ...] = ()
    decision_authority: NewsAuthority = field(
        default=NewsAuthority.SUPPORTING_ONLY,
        init=False,
    )
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reason_codes",
            tuple(sorted(set(self.reason_codes))),
        )
        object.__setattr__(
            self,
            "missing_symbols",
            tuple(
                sorted(
                    {
                        _required_text(item).upper()
                        for item in self.missing_symbols
                    }
                )
            ),
        )
        if self.written_count < 0 or self.written_count > TOP10_LIMIT:
            raise ValueError("written_count must be between zero and ten")

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "observed_at": (
                None if self.observed_at is None else self.observed_at.isoformat()
            ),
            "scheduled_for": (
                None if self.scheduled_for is None else self.scheduled_for.isoformat()
            ),
            "slot": None if self.slot is None else self.slot.value,
            "trading_date": (
                None if self.trading_date is None else self.trading_date.isoformat()
            ),
            "run_id": self.run_id,
            "parent_head_hash": self.parent_head_hash,
            "quote_batch_id": self.quote_batch_id,
            "written_count": self.written_count,
            "missing_symbols": list(self.missing_symbols),
            "decision_authority": self.decision_authority.value,
            "approval_eligible": self.approval_eligible,
            "instruction_creation_allowed": self.instruction_creation_allowed,
            "order_allowed": self.order_allowed,
        }


class BrokerAccountStateReader(Protocol):
    """Minimal read-only preflight surface; positions must be called first."""

    def positions(self) -> object:
        ...

    def working_orders(self) -> object:
        ...

    def unsubmitted_instructions(self) -> object:
        ...


class IndependentTop10StructureSource(Protocol):
    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> Sequence[ResolvedStructure] | Top10StructureResolution:
        ...


class BrokerSnapshotProvider(Protocol):
    def build(
        self,
        contracts: Sequence[GatewayOptionContractRef],
    ) -> object:
        ...


class NewsPreselectionStore(Protocol):
    def append_premarket_run(
        self,
        run_id: str,
        candidates: Sequence[ConditionalOptionPreselection],
        *,
        now: datetime | None = None,
        source_batch_purpose: str | None = None,
        source_batch_id: str | None = None,
        source_batch_hash: str | None = None,
    ) -> object:
        ...

    def latest_premarket(self) -> object | None:
        ...

    def append_open_batch(
        self,
        parent_head_hash: str,
        candidates: Sequence[ConditionalOptionPreselection],
        *,
        batch_id: str,
        scheduled_for: datetime,
        observed_at: datetime,
        batch_blockers: Sequence[str] = (),
        source_batch_purpose: str | None = None,
        source_batch_id: str | None = None,
        source_batch_hash: str | None = None,
    ) -> object:
        ...


class TradingSessionGate(Protocol):
    """Broker-published session decision required by production composition."""

    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        ...


class OpenEconomicsResolver(Protocol):
    def resolve(
        self,
        candidate: object,
        *,
        snapshot: object,
        scenario_set: TrustedTerminalScenarioSet | None,
        now: datetime,
    ) -> object:
        ...


class Top10PreselectionProducer:
    """Produce two all-or-nothing, read-only Top-10 ledger batches."""

    def __init__(
        self,
        *,
        account_state_reader: BrokerAccountStateReader,
        structure_source: IndependentTop10StructureSource,
        snapshot_provider: BrokerSnapshotProvider,
        store: NewsPreselectionStore,
        clock: Callable[[], datetime],
        session_gate: TradingSessionGate | None,
        premarket_account_only: bool = False,
        require_exact_top10: bool = False,
        open_economics_resolver: OpenEconomicsResolver | None = None,
        strategy_nav_reader: Callable[[object], Decimal] | None = None,
        source_batch_purpose: str | None = None,
        source_batch_id: str | None = None,
        source_batch_hash: str | None = None,
    ) -> None:
        for name, value in (
            ("account_state_reader", account_state_reader),
            ("structure_source", structure_source),
            ("snapshot_provider", snapshot_provider),
            ("store", store),
        ):
            if value is None:
                raise TypeError(f"{name} is required")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if not isinstance(premarket_account_only, bool):
            raise TypeError("premarket_account_only must be a bool")
        if not isinstance(require_exact_top10, bool):
            raise TypeError("require_exact_top10 must be a bool")
        self._account_state_reader = account_state_reader
        self._structure_source = structure_source
        self._snapshot_provider = snapshot_provider
        self._store = store
        self._clock = clock
        self._session_gate = session_gate
        self._premarket_account_only = premarket_account_only
        self._require_exact_top10 = require_exact_top10
        self._open_economics_resolver = open_economics_resolver
        self._strategy_nav_reader = strategy_nav_reader
        try:
            self._source_binding = _checked_source_binding(
                source_batch_purpose,
                source_batch_id,
                source_batch_hash,
            )
            self._source_binding_valid = True
        except (TypeError, ValueError):
            self._source_binding = None
            self._source_binding_valid = False

    def tick(self, *, scheduled_for: datetime | None = None) -> ProducerResult:
        """Evaluate one exact slot while keeping broker timestamps wall-clock true."""

        try:
            observed_at = _aware_datetime(self._clock(), "clock result")
        except (TypeError, ValueError):
            return _result(
                ProducerStatus.NO_TRADE,
                "CLOCK_INVALID",
                observed_at=None,
            )
        observed_et = observed_at.astimezone(NEW_YORK)
        if scheduled_for is None:
            scheduled_et = observed_et
        else:
            try:
                scheduled_et = _aware_datetime(
                    scheduled_for,
                    "scheduled_for",
                ).astimezone(NEW_YORK)
            except (TypeError, ValueError):
                return _result(
                    ProducerStatus.NO_TRADE,
                    "SCHEDULED_SLOT_INVALID",
                    observed_at=observed_at,
                    trading_date=observed_et.date(),
                )
        slot = _exact_slot(scheduled_et)
        if slot is None:
            return _result(
                ProducerStatus.NO_TRADE,
                "NON_EXACT_SLOT",
                observed_at=observed_at,
                trading_date=observed_et.date(),
            )
        scheduled_for = datetime.combine(
            scheduled_et.date(),
            PREMARKET_SLOT
            if slot is ProducerSlot.PREMARKET_0920
            else OPEN_REPRICE_SLOT,
            tzinfo=NEW_YORK,
        )
        if not (
            scheduled_for <= observed_et < scheduled_for + timedelta(minutes=1)
        ):
            return _result(
                ProducerStatus.NO_TRADE,
                "SLOT_WINDOW_EXPIRED_OR_NOT_STARTED",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        if not self._source_binding_valid:
            return _result(
                ProducerStatus.NO_TRADE,
                "SOURCE_BATCH_BINDING_INVALID",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        expected_source_purpose = (
            PREMARKET_ACCOUNT_PURPOSE
            if slot is ProducerSlot.PREMARKET_0920
            else OPEN_REPRICE_PURPOSE
        )
        if (
            self._source_binding is not None
            and self._source_binding[0] != expected_source_purpose
        ):
            return _result(
                ProducerStatus.NO_TRADE,
                "SOURCE_BATCH_PURPOSE_MISMATCH",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )

        preflight = self._account_preflight(
            observed_at=observed_at,
            scheduled_for=scheduled_for,
            slot=slot,
        )
        if preflight is not None:
            return preflight

        if self._session_gate is None:
            return _result(
                ProducerStatus.NO_TRADE,
                "SESSION_GATE_UNAVAILABLE",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        try:
            session_ok = self._session_gate.is_trading_session(
                scheduled_for=scheduled_for
            )
        except Exception:
            session_ok = None
        if session_ok is not True:
            return _result(
                ProducerStatus.NO_TRADE,
                "SESSION_CLOSED_OR_UNTRADABLE",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )

        if slot is ProducerSlot.PREMARKET_0920:
            return self._freeze_premarket(
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        return self._reprice_open(
            observed_at=observed_at,
            scheduled_for=scheduled_for,
            slot=slot,
        )

    def _account_preflight(
        self,
        *,
        observed_at: datetime,
        scheduled_for: datetime,
        slot: ProducerSlot,
    ) -> ProducerResult | None:
        # This call is deliberately the first broker interaction.
        try:
            raw_positions = self._account_state_reader.positions()
        except Exception:
            raw_positions = None
        positions, invalid = validate_entry_positions(raw_positions)
        if invalid:
            return _result(
                ProducerStatus.NO_TRADE,
                "POSITIONS_UNKNOWN_OR_INVALID",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        assert positions is not None
        if any(
            security_type in _DERIVATIVE_SECURITY_TYPES and quantity != 0
            for _, security_type, quantity in positions
        ):
            return _result(
                ProducerStatus.POSITION_MANAGEMENT_ONLY,
                "DERIVATIVE_POSITION_PRESENT",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )

        try:
            working = self._account_state_reader.working_orders()
        except Exception:
            working = None
        working_count = _known_sequence_count(working)
        if working_count is None:
            return _result(
                ProducerStatus.NO_TRADE,
                "WORKING_ORDERS_UNKNOWN",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        if working_count:
            return _result(
                ProducerStatus.NO_TRADE,
                "WORKING_ORDERS_PRESENT",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )

        try:
            instructions = self._account_state_reader.unsubmitted_instructions()
        except Exception:
            instructions = None
        instruction_count = _known_sequence_count(instructions)
        if instruction_count is None:
            return _result(
                ProducerStatus.NO_TRADE,
                "UNSUBMITTED_INSTRUCTIONS_UNKNOWN",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        if instruction_count:
            return _result(
                ProducerStatus.NO_TRADE,
                "UNSUBMITTED_INSTRUCTIONS_PRESENT",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        return None

    def _freeze_premarket(
        self,
        *,
        observed_at: datetime,
        scheduled_for: datetime,
        slot: ProducerSlot,
    ) -> ProducerResult:
        run_id = _premarket_run_id(scheduled_for.date())
        latest, read_error = _latest_premarket(self._store)
        if read_error:
            return _result(
                ProducerStatus.NO_TRADE,
                "PREMARKET_LEDGER_READ_FAILED",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
            )
        if latest is not None and _run_matches_slot(latest, scheduled_for):
            if not _stored_source_binding_matches(latest, self._source_binding):
                return _result(
                    ProducerStatus.NO_TRADE,
                    "SOURCE_BATCH_BINDING_MISMATCH",
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=run_id,
                    parent_head_hash=_optional_text(
                        getattr(latest, "head_hash", None)
                    ),
                )
            return _result(
                ProducerStatus.PREMARKET_FROZEN,
                "SLOT_ALREADY_RECORDED",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=str(getattr(latest, "run_id", run_id)),
                parent_head_hash=_optional_text(getattr(latest, "head_hash", None)),
            )
        if latest is not None and _run_et_date(latest) == scheduled_for.date():
            return _result(
                ProducerStatus.NO_TRADE,
                "SAME_DAY_PREMARKET_PARENT_INVALID",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
            )

        try:
            source_resolution = self._structure_source.resolve_top10(
                scheduled_for=scheduled_for
            )
            if isinstance(source_resolution, Top10StructureResolution):
                source_rows = source_resolution.structures
                source_reason_codes = source_resolution.reason_codes
                source_missing_symbols = source_resolution.missing_symbols
            else:
                source_rows = tuple(source_resolution)
                source_reason_codes = ()
                source_missing_symbols = ()
        except Exception:
            return _result(
                ProducerStatus.NO_TRADE,
                "STRUCTURE_SOURCE_FAILED",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
            )
        if self._require_exact_top10 and len(source_rows) != TOP10_LIMIT:
            return _result(
                ProducerStatus.NO_TRADE,
                (
                    "STRUCTURE_SOURCE_OVERFLOW"
                    if len(source_rows) > TOP10_LIMIT
                    else "TOP10_ELIGIBLE_COUNT_SHORTFALL"
                ),
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
                additional_reasons=source_reason_codes,
                missing_symbols=source_missing_symbols,
            )
        if source_reason_codes or source_missing_symbols:
            return _result(
                ProducerStatus.NO_TRADE,
                "STRUCTURE_SOURCE_INCOMPLETE",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
                additional_reasons=source_reason_codes,
                missing_symbols=source_missing_symbols,
            )
        structures = source_rows[:TOP10_LIMIT]
        source_reason = "SOURCE_CAPPED_AT_TEN" if len(source_rows) > TOP10_LIMIT else None
        reason = _validate_structures(
            structures,
            expected_phase=PreselectionPhase.PRE_MARKET,
        )
        if reason is not None:
            return _result(
                ProducerStatus.NO_TRADE,
                reason,
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
            )

        snapshot: object | None = None
        if self._premarket_account_only:
            quoted = tuple(item.candidate for item in structures)
            if any(
                any(
                    getattr(leg, field) is not None
                    for field in ConditionalOptionLeg._DYNAMIC_QUOTE_FIELDS
                )
                for candidate in quoted
                for leg in candidate.legs
            ):
                return _result(
                    ProducerStatus.NO_TRADE,
                    "PREMARKET_DYNAMIC_QUOTE_FIELDS_FORBIDDEN",
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=run_id,
                )
        else:
            quoted, snapshot, failure = self._quote_batch(
                structures,
                phase=PreselectionPhase.PRE_MARKET,
                scheduled_for=scheduled_for,
            )
            if failure is not None:
                return _result(
                    failure[0],
                    failure[1],
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=run_id,
                )
            assert quoted is not None and snapshot is not None
        try:
            verification_now = _aware_datetime(self._clock(), "clock result")
            verification_et = verification_now.astimezone(NEW_YORK)
            if not (
                scheduled_for
                <= verification_et
                < scheduled_for + timedelta(minutes=1)
            ):
                return _result(
                    ProducerStatus.NO_TRADE,
                    "SLOT_WINDOW_EXPIRED_DURING_SNAPSHOT",
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=run_id,
                    quote_batch_id=(
                        None if snapshot is None else _snapshot_batch_id(snapshot)
                    ),
                )
            stored = self._store.append_premarket_run(
                run_id,
                quoted,
                now=verification_now,
                source_batch_purpose=_source_batch_purpose(
                    self._source_binding
                ),
                source_batch_id=_source_batch_id(self._source_binding),
                source_batch_hash=_source_batch_hash(self._source_binding),
            )
        except Exception:
            raced, raced_error = _latest_premarket(self._store)
            if not raced_error and raced is not None and _run_matches_slot(
                raced, scheduled_for
            ):
                if not _stored_source_binding_matches(
                    raced,
                    self._source_binding,
                ):
                    return _result(
                        ProducerStatus.NO_TRADE,
                        "SOURCE_BATCH_BINDING_MISMATCH",
                        observed_at=observed_at,
                        scheduled_for=scheduled_for,
                        slot=slot,
                        run_id=run_id,
                        parent_head_hash=_optional_text(
                            getattr(raced, "head_hash", None)
                        ),
                        quote_batch_id=(
                            None
                            if snapshot is None
                            else _snapshot_batch_id(snapshot)
                        ),
                    )
                return _result(
                    ProducerStatus.PREMARKET_FROZEN,
                    "SLOT_ALREADY_RECORDED",
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=str(getattr(raced, "run_id", run_id)),
                    parent_head_hash=_optional_text(
                        getattr(raced, "head_hash", None)
                    ),
                    quote_batch_id=(
                        None if snapshot is None else _snapshot_batch_id(snapshot)
                    ),
                )
            return _result(
                ProducerStatus.NO_TRADE,
                "PREMARKET_BATCH_APPEND_FAILED",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
                quote_batch_id=(
                    None if snapshot is None else _snapshot_batch_id(snapshot)
                ),
            )
        if not _stored_source_binding_matches(stored, self._source_binding):
            return _result(
                ProducerStatus.NO_TRADE,
                "SOURCE_BATCH_BINDING_MISMATCH",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=run_id,
                parent_head_hash=_optional_text(getattr(stored, "head_hash", None)),
                quote_batch_id=(
                    None if snapshot is None else _snapshot_batch_id(snapshot)
                ),
            )
        reasons = () if source_reason is None else (source_reason,)
        return ProducerResult(
            status=ProducerStatus.PREMARKET_FROZEN,
            reason_codes=reasons,
            observed_at=observed_at,
            scheduled_for=scheduled_for,
            slot=slot,
            trading_date=scheduled_for.date(),
            run_id=str(getattr(stored, "run_id", run_id)),
            parent_head_hash=_optional_text(getattr(stored, "head_hash", None)),
            quote_batch_id=(
                None if snapshot is None else _snapshot_batch_id(snapshot)
            ),
            written_count=len(quoted),
        )

    def _reprice_open(
        self,
        *,
        observed_at: datetime,
        scheduled_for: datetime,
        slot: ProducerSlot,
    ) -> ProducerResult:
        batch_id = _open_batch_id(scheduled_for.date())
        parent, read_error = _latest_premarket(self._store)
        if read_error:
            return _result(
                ProducerStatus.NO_TRADE,
                "PREMARKET_LEDGER_READ_FAILED",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        premarket_scheduled = scheduled_for.replace(
            hour=PREMARKET_SLOT.hour,
            minute=PREMARKET_SLOT.minute,
        )
        if parent is None or not _run_matches_slot(parent, premarket_scheduled):
            return _result(
                ProducerStatus.NO_TRADE,
                "TODAY_0920_PARENT_MISSING",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        parent_head_hash = _optional_text(getattr(parent, "head_hash", None))
        if parent_head_hash is None:
            return _result(
                ProducerStatus.NO_TRADE,
                "TODAY_0920_PARENT_INVALID",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
            )
        existing_batch = _existing_open_batch(
            self._store,
            batch_id=batch_id,
            parent_head_hash=parent_head_hash,
        )
        if existing_batch is not None:
            if not _stored_source_binding_matches(
                existing_batch,
                self._source_binding,
            ):
                return _result(
                    ProducerStatus.NO_TRADE,
                    "SOURCE_BATCH_BINDING_MISMATCH",
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=_optional_text(getattr(parent, "run_id", None)),
                    parent_head_hash=parent_head_hash,
                )
            return _existing_batch_result(
                existing_batch,
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=_optional_text(getattr(parent, "run_id", None)),
                parent_head_hash=parent_head_hash,
            )

        structures, parent_reason = _structures_from_parent(parent)
        if parent_reason is not None:
            return _result(
                ProducerStatus.NO_TRADE,
                parent_reason,
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=_optional_text(getattr(parent, "run_id", None)),
                parent_head_hash=parent_head_hash,
            )
        quoted, snapshot, failure = self._quote_batch(
            structures,
            phase=PreselectionPhase.OPEN_REPRICED,
            scheduled_for=scheduled_for,
        )
        if failure is not None:
            return _result(
                failure[0],
                failure[1],
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=_optional_text(getattr(parent, "run_id", None)),
                parent_head_hash=parent_head_hash,
            )
        assert quoted is not None and snapshot is not None
        economics_blockers: tuple[str, ...]
        quoted, economics_blockers = self._recompute_open_economics(
            quoted,
            snapshot=snapshot,
            now=observed_at,
        )
        try:
            verification_now = _aware_datetime(self._clock(), "clock result")
            verification_et = verification_now.astimezone(NEW_YORK)
            if not (
                scheduled_for
                <= verification_et
                < scheduled_for + timedelta(minutes=1)
            ):
                return _result(
                    ProducerStatus.NO_TRADE,
                    "SLOT_WINDOW_EXPIRED_DURING_SNAPSHOT",
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=_optional_text(getattr(parent, "run_id", None)),
                    parent_head_hash=parent_head_hash,
                    quote_batch_id=_snapshot_batch_id(snapshot),
                )
            stored = self._store.append_open_batch(
                parent_head_hash,
                quoted,
                batch_id=batch_id,
                scheduled_for=scheduled_for,
                observed_at=verification_now,
                batch_blockers=economics_blockers,
                source_batch_purpose=_source_batch_purpose(
                    self._source_binding
                ),
                source_batch_id=_source_batch_id(self._source_binding),
                source_batch_hash=_source_batch_hash(self._source_binding),
            )
        except Exception:
            existing_batch = _existing_open_batch(
                self._store,
                batch_id=batch_id,
                parent_head_hash=parent_head_hash,
            )
            if existing_batch is not None:
                if not _stored_source_binding_matches(
                    existing_batch,
                    self._source_binding,
                ):
                    return _result(
                        ProducerStatus.NO_TRADE,
                        "SOURCE_BATCH_BINDING_MISMATCH",
                        observed_at=observed_at,
                        scheduled_for=scheduled_for,
                        slot=slot,
                        run_id=_optional_text(getattr(parent, "run_id", None)),
                        parent_head_hash=parent_head_hash,
                        quote_batch_id=_snapshot_batch_id(snapshot),
                    )
                return _existing_batch_result(
                    existing_batch,
                    observed_at=observed_at,
                    scheduled_for=scheduled_for,
                    slot=slot,
                    run_id=_optional_text(getattr(parent, "run_id", None)),
                    parent_head_hash=parent_head_hash,
                    fallback_quote_batch_id=_snapshot_batch_id(snapshot),
                )
            return _result(
                ProducerStatus.NO_TRADE,
                "OPEN_BATCH_APPEND_FAILED",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=_optional_text(getattr(parent, "run_id", None)),
                parent_head_hash=parent_head_hash,
                quote_batch_id=_snapshot_batch_id(snapshot),
            )
        if not _stored_source_binding_matches(stored, self._source_binding):
            return _result(
                ProducerStatus.NO_TRADE,
                "SOURCE_BATCH_BINDING_MISMATCH",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=_optional_text(getattr(parent, "run_id", None)),
                parent_head_hash=parent_head_hash,
                quote_batch_id=_snapshot_batch_id(snapshot),
            )
        stored_blockers = getattr(stored, "blockers", None)
        stored_eligible = getattr(stored, "action_pool_eligible", None)
        stored_rows = getattr(stored, "rows", None)
        if (
            not isinstance(stored_blockers, Sequence)
            or isinstance(stored_blockers, (str, bytes, bytearray, memoryview))
            or not all(isinstance(item, str) and item for item in stored_blockers)
            or not isinstance(stored_eligible, bool)
            or _known_sequence_count(stored_rows) != len(quoted)
            or stored_eligible != (len(stored_blockers) == 0)
        ):
            return _result(
                ProducerStatus.NO_TRADE,
                "OPEN_BATCH_RESULT_INVALID",
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                run_id=_optional_text(getattr(parent, "run_id", None)),
                parent_head_hash=parent_head_hash,
                quote_batch_id=_snapshot_batch_id(snapshot),
            )
        if not stored_eligible:
            return ProducerResult(
                status=ProducerStatus.NO_TRADE,
                reason_codes=tuple(stored_blockers),
                observed_at=observed_at,
                scheduled_for=scheduled_for,
                slot=slot,
                trading_date=scheduled_for.date(),
                run_id=_optional_text(getattr(parent, "run_id", None)),
                parent_head_hash=parent_head_hash,
                quote_batch_id=_optional_text(
                    getattr(stored, "quote_batch_id", None)
                ) or _snapshot_batch_id(snapshot),
                written_count=len(quoted),
            )
        return ProducerResult(
            status=ProducerStatus.OPEN_REPRICED,
            reason_codes=(),
            observed_at=observed_at,
            scheduled_for=scheduled_for,
            slot=slot,
            trading_date=scheduled_for.date(),
            run_id=_optional_text(getattr(parent, "run_id", None)),
            parent_head_hash=parent_head_hash,
            quote_batch_id=_optional_text(
                getattr(stored, "quote_batch_id", None)
            ) or _snapshot_batch_id(snapshot),
            written_count=len(quoted),
        )

    def _recompute_open_economics(
        self,
        candidates: tuple[ConditionalOptionPreselection, ...],
        *,
        snapshot: object,
        now: datetime,
    ) -> tuple[tuple[ConditionalOptionPreselection, ...], tuple[str, ...]]:
        if (
            self._open_economics_resolver is None
            or self._strategy_nav_reader is None
        ):
            return candidates, ("OPEN_ECONOMICS_RECALCULATION_UNAVAILABLE",)
        try:
            strategy_nav = self._strategy_nav_reader(snapshot)
        except Exception:
            return candidates, ("STRATEGY_NAV_UNAVAILABLE",)
        if (
            not isinstance(strategy_nav, Decimal)
            or not strategy_nav.is_finite()
            or strategy_nav <= 0
        ):
            return candidates, ("STRATEGY_NAV_INVALID",)

        repriced: list[ConditionalOptionPreselection] = []
        blockers: list[str] = []
        for candidate in candidates:
            try:
                if (
                    not candidate.terminal_scenarios
                    or candidate.scenario_asof is None
                    or candidate.scenario_hash is None
                    or candidate.risk_policy_version is None
                    or candidate.risk_policy_hash is None
                ):
                    raise OpenRepriceEconomicsError("SCENARIO_SET_MISSING")
                scenario_set = TrustedTerminalScenarioSet.create(
                    candidate_id=candidate.preselection_id,
                    strategy_hash=candidate.strategy_hash,
                    scenario_asof=candidate.scenario_asof,
                    scenarios=tuple(
                        TrustedTerminalScenario(
                            item.terminal_underlying_price,
                            item.probability,
                        )
                        for item in candidate.terminal_scenarios
                    ),
                    current_policy_version=candidate.risk_policy_version,
                    current_policy_hash=candidate.risk_policy_hash,
                )
                if scenario_set.scenario_hash != candidate.scenario_hash:
                    raise OpenRepriceEconomicsError("SCENARIO_HASH_INVALID")
                snapshot_hash = _required_text(
                    getattr(snapshot, "snapshot_hash", None)
                )
                nav_hash = strategy_nav_post_hash(
                    candidate_id=candidate.preselection_id,
                    strategy_hash=candidate.strategy_hash,
                    snapshot_hash=snapshot_hash,
                    strategy_nav_usd=strategy_nav,
                )
                bound_candidate = replace(
                    candidate,
                    strategy_nav_usd=strategy_nav,
                    strategy_nav_post_hash=nav_hash,
                    broker_snapshot_hash=snapshot_hash,
                )
                resolution = self._open_economics_resolver.resolve(
                    bound_candidate,
                    snapshot=snapshot,
                    scenario_set=scenario_set,
                    now=now,
                )
                verify_hash = getattr(resolution, "verify_hash", None)
                if not callable(verify_hash) or verify_hash() is not True:
                    raise OpenRepriceEconomicsError(
                        "OPEN_ECONOMICS_RESULT_INVALID"
                    )
                if (
                    getattr(resolution, "candidate_id", None)
                    != candidate.preselection_id
                    or getattr(resolution, "strategy_hash", None)
                    != candidate.strategy_hash
                    or getattr(resolution, "broker_snapshot_hash", None)
                    != snapshot_hash
                ):
                    raise OpenRepriceEconomicsError(
                        "OPEN_ECONOMICS_RESULT_INVALID"
                    )
                resolved_candidate = replace(
                    bound_candidate,
                    risk_defined=True,
                    maximum_loss_usd=resolution.maximum_loss_usd,
                    estimated_cost_usd=resolution.all_in_cost_usd,
                    cost_after_ev_usd=resolution.after_cost_expected_value_usd,
                    scenario_asof=resolution.scenario_asof,
                    scenario_hash=resolution.scenario_hash,
                    execution_cost_contract_version=resolution.cost_contract_version,
                    execution_cost_contract_hash=resolution.cost_contract_hash,
                    risk_policy_version=resolution.policy_version,
                    risk_policy_hash=resolution.policy_hash,
                    broker_snapshot_hash=resolution.broker_snapshot_hash,
                    strategy_nav_usd=resolution.strategy_nav_usd,
                    strategy_nav_post_hash=resolution.strategy_nav_post_hash,
                    economics_quote_batch_id=resolution.quote_batch_id,
                    economics_quote_asof=resolution.quote_asof,
                    payoff_hash=resolution.payoff_hash,
                    economics_calculation_hash=resolution.economics_hash,
                    debit_usd=resolution.debit_usd,
                    credit_usd=resolution.credit_usd,
                    net_entry_cost_usd=resolution.all_in_cost_usd,
                    estimated_commission_usd=resolution.commission_usd,
                    estimated_entry_slippage_usd=resolution.entry_slippage_usd,
                    estimated_exit_slippage_usd=resolution.exit_slippage_usd,
                    estimated_slippage_usd=resolution.total_slippage_usd,
                    expected_value_before_costs_usd=(
                        resolution.before_cost_expected_value_usd
                    ),
                    risk_fraction=resolution.risk_fraction,
                )
            except OpenRepriceEconomicsError as exc:
                blockers.append(
                    f"CANDIDATE:{candidate.preselection_id}:{exc.reason_code}"
                )
                repriced.append(candidate)
                continue
            except Exception:
                blockers.append(
                    f"CANDIDATE:{candidate.preselection_id}:OPEN_ECONOMICS_FAILED"
                )
                repriced.append(candidate)
                continue
            if resolved_candidate.phase is not PreselectionPhase.OPEN_REPRICED:
                blockers.append(
                    f"CANDIDATE:{candidate.preselection_id}:OPEN_ECONOMICS_RESULT_INVALID"
                )
                repriced.append(candidate)
                continue
            repriced.append(resolved_candidate)
        return tuple(repriced), tuple(dict.fromkeys(blockers))

    def _quote_batch(
        self,
        structures: tuple[ResolvedStructure, ...],
        *,
        phase: PreselectionPhase,
        scheduled_for: datetime,
    ) -> tuple[
        tuple[ConditionalOptionPreselection, ...] | None,
        object | None,
        tuple[ProducerStatus, str] | None,
    ]:
        contracts, reason = _contracts_for_structures(structures)
        if reason is not None:
            return None, None, (ProducerStatus.NO_TRADE, reason)
        try:
            snapshot = self._snapshot_provider.build(contracts)
        except Exception:
            return None, None, (
                ProducerStatus.NO_TRADE,
                "BROKER_SNAPSHOT_BUILD_FAILED",
            )
        try:
            verification_now = _aware_datetime(self._clock(), "clock result")
        except (TypeError, ValueError):
            return None, snapshot, (ProducerStatus.NO_TRADE, "CLOCK_INVALID")
        snapshot_status, snapshot_reason = _validate_snapshot(
            snapshot,
            contracts=contracts,
            verified_at=verification_now,
        )
        if snapshot_reason is not None:
            return None, snapshot, (snapshot_status, snapshot_reason)
        try:
            quoted = _bind_snapshot_quotes(
                structures,
                snapshot=snapshot,
                phase=phase,
                trading_date=scheduled_for.date(),
            )
        except (AttributeError, KeyError, TypeError, ValueError):
            return None, snapshot, (
                ProducerStatus.NO_TRADE,
                "SNAPSHOT_QUOTE_BINDING_FAILED",
            )
        return quoted, snapshot, None


def _result(
    status: ProducerStatus,
    reason: str,
    *,
    observed_at: datetime | None,
    scheduled_for: datetime | None = None,
    slot: ProducerSlot | None = None,
    trading_date: date | None = None,
    run_id: str | None = None,
    parent_head_hash: str | None = None,
    quote_batch_id: str | None = None,
    additional_reasons: Sequence[str] = (),
    missing_symbols: Sequence[str] = (),
) -> ProducerResult:
    return ProducerResult(
        status=status,
        reason_codes=(reason, *additional_reasons),
        observed_at=observed_at,
        scheduled_for=scheduled_for,
        slot=slot,
        trading_date=(
            scheduled_for.date() if scheduled_for is not None else trading_date
        ),
        run_id=run_id,
        parent_head_hash=parent_head_hash,
        quote_batch_id=quote_batch_id,
        missing_symbols=tuple(missing_symbols),
    )


def _exact_slot(value: datetime) -> ProducerSlot | None:
    wall = value.timetz().replace(tzinfo=None)
    if wall == PREMARKET_SLOT:
        return ProducerSlot.PREMARKET_0920
    if wall == OPEN_REPRICE_SLOT:
        return ProducerSlot.OPEN_REPRICE_0935
    return None


def validate_entry_positions(
    value: object,
) -> tuple[tuple[tuple[int, str, Decimal], ...] | None, bool]:
    """Validate a read-only IBKR position snapshot without coercing unknowns.

    Runtime composition uses this public seam before it reads working orders or
    unsubmitted instructions so an open derivative position can short-circuit
    the Top-10 discovery path immediately.
    """
    if _known_sequence_count(value) is None:
        return None, True
    assert isinstance(value, Sequence)
    result: list[tuple[int, str, Decimal]] = []
    seen: set[int] = set()
    for row in value:
        contract_id = _field(row, "contract_id", "con_id", "conId")
        security_type = _field(row, "security_type", "sec_type", "secType")
        symbol = _field(row, "symbol")
        quantity = _field(row, "quantity", "position")
        if (
            isinstance(contract_id, bool)
            or not isinstance(contract_id, int)
            or contract_id <= 0
            or contract_id in seen
            or not isinstance(security_type, str)
            or not security_type.strip()
            or not isinstance(symbol, str)
            or not symbol.strip()
        ):
            return None, True
        try:
            checked_quantity = _finite_decimal(quantity, "position quantity")
            for numeric_name in (
                "average_cost",
                "market_price",
                "market_value",
                "unrealized_pnl",
                "realized_pnl",
                "strike",
            ):
                numeric_value = _field(row, numeric_name)
                if numeric_value is not None:
                    _finite_decimal(numeric_value, f"position {numeric_name}")
        except (TypeError, ValueError):
            return None, True
        seen.add(contract_id)
        result.append((contract_id, security_type.strip().upper(), checked_quantity))
    return tuple(result), False


def _known_sequence_count(value: object) -> int | None:
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        return len(value)
    return None


def _field(value: object, *names: str) -> object:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return None
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _validate_structures(
    structures: tuple[ResolvedStructure, ...],
    *,
    expected_phase: PreselectionPhase,
) -> str | None:
    if not structures:
        return "STRUCTURE_SOURCE_EMPTY"
    if len(structures) > TOP10_LIMIT:
        return "STRUCTURE_SOURCE_OVERFLOW"
    if any(not isinstance(item, ResolvedStructure) for item in structures):
        return "STRUCTURE_SOURCE_INVALID"
    candidates = tuple(item.candidate for item in structures)
    if any(item.phase is not expected_phase for item in candidates):
        return "STRUCTURE_PHASE_INVALID"
    if len({item.preselection_id for item in candidates}) != len(candidates):
        return "DUPLICATE_PRESELECTION_ID"
    if len({item.strategy_hash for item in candidates}) != len(candidates):
        return "DUPLICATE_STRATEGY_HASH"
    for candidate in candidates:
        if not candidate.legs:
            return "STRUCTURE_LEGS_MISSING"
        con_ids: set[int] = set()
        for leg in candidate.legs:
            contract = leg.contract_ref
            if (
                contract is None
                or leg.underlying != candidate.underlying
                or leg.side is None
                or leg.ratio is None
                or leg.quantity is None
                or contract.multiplier != 100
                or contract.exchange.upper() == ""
            ):
                return "STRUCTURE_IDENTITY_INCOMPLETE"
            if contract.con_id in con_ids:
                return "DUPLICATE_STRUCTURE_LEG"
            con_ids.add(contract.con_id)
    return None


def _contracts_for_structures(
    structures: tuple[ResolvedStructure, ...],
) -> tuple[tuple[GatewayOptionContractRef, ...], str | None]:
    contracts: dict[int, GatewayOptionContractRef] = {}
    for structure in structures:
        for leg in structure.candidate.legs:
            identity = leg.contract_ref
            if identity is None:
                return (), "STRUCTURE_IDENTITY_INCOMPLETE"
            gateway = GatewayOptionContractRef(
                contract_id=identity.con_id,
                contract_id_ex=str(identity.con_id),
                symbol=leg.underlying,
                local_symbol=identity.local_symbol,
                expiration=identity.expiry,
                strike=identity.strike,
                right="C" if identity.right is OptionRight.CALL else "P",
                exchange=identity.exchange,
                trading_class=identity.trading_class,
                multiplier=identity.multiplier,
                currency="USD",
            )
            previous = contracts.get(gateway.contract_id)
            if previous is not None and previous != gateway:
                return (), "CROSS_STRUCTURE_CONTRACT_IDENTITY_CONFLICT"
            contracts[gateway.contract_id] = gateway
    if not contracts:
        return (), "STRUCTURE_LEGS_MISSING"
    return tuple(contracts[key] for key in sorted(contracts)), None


def _validate_snapshot(
    snapshot: object,
    *,
    contracts: tuple[GatewayOptionContractRef, ...],
    verified_at: datetime,
) -> tuple[ProducerStatus, str | None]:
    status = getattr(snapshot, "status", None)
    status_value = status.value if isinstance(status, Enum) else status
    complete = getattr(snapshot, "complete", None)
    if status_value != BrokerSnapshotStatus.COMPLETE.value or complete is not True:
        return ProducerStatus.NO_TRADE, "BROKER_SNAPSHOT_INCOMPLETE"
    snapshot_hash = getattr(snapshot, "snapshot_hash", None)
    verify_hash = getattr(snapshot, "verify_hash", None)
    if (
        not isinstance(snapshot_hash, str)
        or len(snapshot_hash) != 64
        or any(character not in "0123456789abcdef" for character in snapshot_hash)
        or not callable(verify_hash)
    ):
        return ProducerStatus.NO_TRADE, "BROKER_SNAPSHOT_HASH_INVALID"
    try:
        if verify_hash() is not True:
            return ProducerStatus.NO_TRADE, "BROKER_SNAPSHOT_HASH_INVALID"
    except Exception:
        return ProducerStatus.NO_TRADE, "BROKER_SNAPSHOT_HASH_INVALID"
    try:
        batch_observed_at = _aware_datetime(
            getattr(snapshot, "quote_batch_observed_at", None),
            "snapshot quote_batch_observed_at",
        )
    except (TypeError, ValueError):
        return ProducerStatus.NO_TRADE, "QUOTE_BATCH_TIMESTAMP_INVALID"

    state = getattr(snapshot, "state_evidence", None)
    if not isinstance(state, Mapping):
        return ProducerStatus.NO_TRADE, "BROKER_STATE_EVIDENCE_INCOMPLETE"
    for name in ("account", "positions", "working_orders", "unsubmitted_instructions"):
        evidence = state.get(name)
        if (
            evidence is None
            or getattr(evidence, "known", None) is not True
            or getattr(evidence, "stable", None) is not True
        ):
            return ProducerStatus.NO_TRADE, "BROKER_STATE_EVIDENCE_INCOMPLETE"
    positions, invalid_positions = validate_entry_positions(
        getattr(state["positions"], "state", None)
    )
    if invalid_positions:
        return ProducerStatus.NO_TRADE, "BROKER_STATE_EVIDENCE_INCOMPLETE"
    assert positions is not None
    if any(
        security_type in _DERIVATIVE_SECURITY_TYPES and quantity != 0
        for _, security_type, quantity in positions
    ):
        return ProducerStatus.POSITION_MANAGEMENT_ONLY, "DERIVATIVE_POSITION_PRESENT"
    for name in ("working_orders", "unsubmitted_instructions"):
        if getattr(state[name], "count", None) != 0:
            return ProducerStatus.NO_TRADE, "BROKER_STATE_NOT_FLAT"

    expected = {item.contract_id: item for item in contracts}
    secdefs = getattr(snapshot, "secdef_evidence", None)
    if not isinstance(secdefs, Sequence) or isinstance(
        secdefs, (str, bytes, bytearray, memoryview)
    ):
        return ProducerStatus.NO_TRADE, "SECDEF_IDENTITY_INCOMPLETE"
    seen_secdefs: set[int] = set()
    for evidence in secdefs:
        contract_id = getattr(evidence, "contract_id", None)
        if (
            contract_id not in expected
            or contract_id in seen_secdefs
            or getattr(evidence, "stable", None) is not True
            or getattr(evidence, "standard_contract", None) is not True
            or getattr(evidence, "adjusted", None) is not False
        ):
            return ProducerStatus.NO_TRADE, "SECDEF_IDENTITY_INCOMPLETE"
        wanted = _gateway_identity(expected[contract_id])
        for identity in (
            getattr(evidence, "pre_identity", None),
            getattr(evidence, "post_identity", None),
        ):
            if not isinstance(identity, Mapping) or set(identity) != set(
                _IDENTITY_FIELDS
            ):
                return ProducerStatus.NO_TRADE, "SECDEF_IDENTITY_INCOMPLETE"
            if dict(identity) != wanted:
                return ProducerStatus.NO_TRADE, "SECDEF_IDENTITY_MISMATCH"
        seen_secdefs.add(contract_id)
    if seen_secdefs != set(expected):
        return ProducerStatus.NO_TRADE, "SECDEF_IDENTITY_INCOMPLETE"

    batch_status = getattr(snapshot, "quote_batch_status", None)
    batch_status_value = (
        batch_status.value if isinstance(batch_status, Enum) else batch_status
    )
    batch_id = _snapshot_batch_id(snapshot)
    if batch_status_value != QuoteBatchStatus.COMPLETE.value or batch_id is None:
        return ProducerStatus.NO_TRADE, "QUOTE_BATCH_INCOMPLETE"
    quotes = getattr(snapshot, "quotes", None)
    if not isinstance(quotes, Sequence) or isinstance(
        quotes, (str, bytes, bytearray, memoryview)
    ):
        return ProducerStatus.NO_TRADE, "QUOTE_BATCH_INCOMPLETE"
    quote_map: dict[int, object] = {}
    quote_times: set[datetime] = set()
    for quote in quotes:
        contract_id = getattr(quote, "contract_id", None)
        if contract_id not in expected or contract_id in quote_map:
            return ProducerStatus.NO_TRADE, "QUOTE_BATCH_PARTIAL_OR_DUPLICATE"
        if getattr(quote, "batch_id", None) != batch_id:
            return ProducerStatus.NO_TRADE, "QUOTE_BATCH_IDENTITY_MISMATCH"
        try:
            quote_asof = _aware_datetime(
                getattr(quote, "observed_at", None),
                "quote observed_at",
            )
        except (TypeError, ValueError):
            return ProducerStatus.NO_TRADE, "QUOTE_TIMESTAMP_INVALID"
        if quote_asof != batch_observed_at:
            return ProducerStatus.NO_TRADE, "QUOTE_BATCH_TIMESTAMP_MISMATCH"
        quote_times.add(quote_asof)
        age = Decimal(str((verified_at - quote_asof).total_seconds()))
        if age < 0 or age > MAXIMUM_QUOTE_AGE_SECONDS:
            return ProducerStatus.NO_TRADE, "QUOTE_STALE_OR_FUTURE"
        market_data_type = getattr(quote, "market_data_type", None)
        if (
            isinstance(market_data_type, bool)
            or not isinstance(market_data_type, int)
            or market_data_type != 1
        ):
            return ProducerStatus.NO_TRADE, "QUOTE_MARKET_DATA_NOT_LIVE"
        field_reason = _validate_quote_fields(quote)
        if field_reason is not None:
            return ProducerStatus.NO_TRADE, field_reason
        quote_map[contract_id] = quote
    if set(quote_map) != set(expected):
        return ProducerStatus.NO_TRADE, "QUOTE_BATCH_PARTIAL_OR_DUPLICATE"
    if len(quote_times) != 1:
        return ProducerStatus.NO_TRADE, "QUOTE_BATCH_TIMESTAMP_MISMATCH"
    return ProducerStatus.NO_TRADE, None


def _validate_quote_fields(quote: object) -> str | None:
    for name in _QUOTE_FIELDS[:7]:
        try:
            value = _finite_decimal(getattr(quote, name, None), name)
        except (TypeError, ValueError):
            return "QUOTE_FIELDS_INCOMPLETE"
        if name in {"bid", "ask", "implied_volatility"} and value < 0:
            return "QUOTE_FIELDS_INVALID"
    bid = getattr(quote, "bid")
    ask = getattr(quote, "ask")
    if bid > ask:
        return "QUOTE_CROSSED"
    for name in ("volume", "open_interest"):
        value = getattr(quote, name, None)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return "QUOTE_FIELDS_INCOMPLETE"
    return None


def _bind_snapshot_quotes(
    structures: tuple[ResolvedStructure, ...],
    *,
    snapshot: object,
    phase: PreselectionPhase,
    trading_date: date,
) -> tuple[ConditionalOptionPreselection, ...]:
    quote_map = {
        getattr(item, "contract_id"): item for item in getattr(snapshot, "quotes")
    }
    batch_id = _snapshot_batch_id(snapshot)
    assert batch_id is not None
    result: list[ConditionalOptionPreselection] = []
    for structure in structures:
        candidate = structure.candidate
        legs: list[ConditionalOptionLeg] = []
        for leg in candidate.legs:
            assert leg.con_id is not None and leg.expiry is not None
            quote = quote_map[leg.con_id]
            legs.append(
                replace(
                    leg,
                    bid=getattr(quote, "bid"),
                    ask=getattr(quote, "ask"),
                    quote_asof=getattr(quote, "observed_at"),
                    quote_batch_id=batch_id,
                    implied_volatility=getattr(quote, "implied_volatility"),
                    delta=getattr(quote, "delta"),
                    gamma=getattr(quote, "gamma"),
                    theta=getattr(quote, "theta"),
                    vega=getattr(quote, "vega"),
                    volume=getattr(quote, "volume"),
                    open_interest=getattr(quote, "open_interest"),
                    dte=(leg.expiry - trading_date).days,
                )
            )
        if any(item.dte is None or item.dte < 0 for item in legs):
            raise ValueError("expired structure")
        replacements: dict[str, object] = {"phase": phase, "legs": tuple(legs)}
        if phase is PreselectionPhase.OPEN_REPRICED:
            replacements.update(
                {
                    "maximum_loss_usd": None,
                    "estimated_cost_usd": None,
                    "cost_after_ev_usd": None,
                    "broker_snapshot_hash": None,
                    "account_snapshot_hash": None,
                    "strategy_nav_usd": None,
                    "strategy_nav_post_hash": None,
                    "economics_quote_batch_id": None,
                    "economics_quote_asof": None,
                    "payoff_hash": None,
                    "economics_calculation_hash": None,
                    "debit_usd": None,
                    "credit_usd": None,
                    "net_entry_cost_usd": None,
                    "estimated_commission_usd": None,
                    "estimated_entry_slippage_usd": None,
                    "estimated_exit_slippage_usd": None,
                    "estimated_slippage_usd": None,
                    "expected_value_before_costs_usd": None,
                    "risk_fraction": None,
                }
            )
        result.append(replace(candidate, **replacements))
    return tuple(result)


def _structures_from_parent(
    parent: object,
) -> tuple[tuple[ResolvedStructure, ...], str | None]:
    rows = getattr(parent, "rows", None)
    if not isinstance(rows, Sequence) or isinstance(
        rows, (str, bytes, bytearray, memoryview)
    ) or not rows or len(rows) > TOP10_LIMIT:
        return (), "TODAY_0920_PARENT_INVALID"
    available_count = getattr(parent, "available_count", len(rows))
    if available_count != len(rows):
        return (), "TODAY_0920_PARENT_INVALID"
    ordered: list[tuple[int, ResolvedStructure]] = []
    seen_ranks: set[int] = set()
    for row in rows:
        rank = getattr(row, "research_rank", None)
        if isinstance(rank, bool) or not isinstance(rank, int) or rank <= 0:
            return (), "TODAY_0920_PARENT_INVALID"
        if rank in seen_ranks:
            return (), "TODAY_0920_PARENT_INVALID"
        seen_ranks.add(rank)
        value = getattr(row, "candidate", None)
        try:
            candidate = (
                value
                if isinstance(value, ConditionalOptionPreselection)
                else _candidate_from_mapping(value)
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return (), "TODAY_0920_PARENT_INVALID"
        if (
            candidate.phase is not PreselectionPhase.PRE_MARKET
            or getattr(row, "preselection_id", candidate.preselection_id)
            != candidate.preselection_id
            or getattr(row, "strategy_hash", candidate.strategy_hash)
            != candidate.strategy_hash
        ):
            return (), "TODAY_0920_PARENT_INVALID"
        ordered.append((rank, ResolvedStructure(candidate)))
    ordered.sort(key=lambda item: item[0])
    if [item[0] for item in ordered] != list(range(1, len(ordered) + 1)):
        return (), "TODAY_0920_PARENT_INVALID"
    structures = tuple(item[1] for item in ordered)
    reason = _validate_structures(
        structures,
        expected_phase=PreselectionPhase.PRE_MARKET,
    )
    return structures, reason


def _candidate_from_mapping(value: object) -> ConditionalOptionPreselection:
    if not isinstance(value, Mapping):
        raise TypeError("stored candidate must be a mapping")
    phase = PreselectionPhase(str(value["phase"]))
    if phase is not PreselectionPhase.PRE_MARKET:
        raise ValueError("stored parent must be PRE_MARKET")
    raw_legs = value["legs"]
    if not isinstance(raw_legs, Sequence) or isinstance(
        raw_legs, (str, bytes, bytearray, memoryview)
    ):
        raise TypeError("stored legs must be an array")
    legs = tuple(_leg_from_mapping(item) for item in raw_legs)
    return ConditionalOptionPreselection(
        preselection_id=_required_text(value["preselection_id"]),
        underlying=_required_text(value["underlying"]),
        strategy_type=_required_text(value["strategy_type"]),
        phase=phase,
        legs=legs,
        risk_defined=_strict_bool(value["risk_defined"]),
        maximum_loss_usd=_optional_decimal(value.get("maximum_loss_usd")),
        estimated_cost_usd=_optional_decimal(value.get("estimated_cost_usd")),
        cost_after_ev_usd=_optional_decimal(value.get("cost_after_ev_usd")),
        entry_condition=_optional_text(value.get("entry_condition")),
        invalidation_condition=_optional_text(value.get("invalidation_condition")),
        profit_target_condition=_optional_text(value.get("profit_target_condition")),
        stop_loss_condition=_optional_text(value.get("stop_loss_condition")),
        evidence_ids=_text_tuple(value["evidence_ids"]),
        evidence_hashes=_text_tuple(value["evidence_hashes"]),
        strategy_hash=_required_text(value["strategy_hash"]),
        research_summary=_required_text(value["research_summary"]),
        underlying_quote_basis=_underlying_quote_basis_from_mapping(
            value.get("underlying_quote_basis")
        ),
        underlying_quote_basis_hash=_optional_text(
            value.get("underlying_quote_basis_hash")
        ),
        terminal_scenarios=_terminal_scenarios_from_mapping(
            value.get("terminal_scenarios")
        ),
        scenario_asof=_optional_datetime(value.get("scenario_asof")),
        scenario_hash=_optional_text(value.get("scenario_hash")),
        execution_cost_contract_version=_optional_text(
            value.get("execution_cost_contract_version")
        ),
        execution_cost_contract_hash=_optional_text(
            value.get("execution_cost_contract_hash")
        ),
        risk_policy_version=_optional_text(value.get("risk_policy_version")),
        risk_policy_hash=_optional_text(value.get("risk_policy_hash")),
        broker_snapshot_hash=_optional_text(value.get("broker_snapshot_hash")),
        account_snapshot_hash=_optional_text(value.get("account_snapshot_hash")),
        strategy_nav_usd=_optional_decimal(value.get("strategy_nav_usd")),
        strategy_nav_post_hash=_optional_text(
            value.get("strategy_nav_post_hash")
        ),
        economics_quote_batch_id=_optional_text(
            value.get("economics_quote_batch_id")
        ),
        economics_quote_asof=_optional_datetime(
            value.get("economics_quote_asof")
        ),
        payoff_hash=_optional_text(value.get("payoff_hash")),
        economics_calculation_hash=_optional_text(
            value.get("economics_calculation_hash")
        ),
        debit_usd=_optional_decimal(value.get("debit_usd")),
        credit_usd=_optional_decimal(value.get("credit_usd")),
        net_entry_cost_usd=_optional_decimal(value.get("net_entry_cost_usd")),
        estimated_commission_usd=_optional_decimal(
            value.get("estimated_commission_usd")
        ),
        estimated_entry_slippage_usd=_optional_decimal(
            value.get("estimated_entry_slippage_usd")
        ),
        estimated_exit_slippage_usd=_optional_decimal(
            value.get("estimated_exit_slippage_usd")
        ),
        estimated_slippage_usd=_optional_decimal(
            value.get("estimated_slippage_usd")
        ),
        expected_value_before_costs_usd=_optional_decimal(
            value.get("expected_value_before_costs_usd")
        ),
        risk_fraction=_optional_decimal(value.get("risk_fraction")),
    )


def _underlying_quote_basis_from_mapping(
    value: object,
) -> UnderlyingQuoteBasis | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "symbol",
        "contract_id",
        "exchange",
        "source",
        "observed_at",
        "bid",
        "ask",
        "last",
        "close",
        "market_data_type",
        "schema",
    }:
        raise ValueError("stored underlying quote basis has an invalid shape")
    if value["schema"] != UNDERLYING_QUOTE_BASIS_SCHEMA:
        raise ValueError("stored underlying quote basis schema is invalid")
    contract_id = value["contract_id"]
    market_data_type = value["market_data_type"]
    if isinstance(contract_id, bool) or not isinstance(contract_id, int):
        raise TypeError("stored underlying contract_id must be an integer")
    if isinstance(market_data_type, bool) or not isinstance(market_data_type, int):
        raise TypeError("stored underlying market_data_type must be an integer")
    close = _optional_decimal(value.get("close"))
    if close is None:
        raise ValueError("stored underlying close is required")
    observed_at = _optional_datetime(value["observed_at"])
    if observed_at is None:
        raise ValueError("stored underlying observed_at is required")
    return UnderlyingQuoteBasis(
        symbol=_required_text(value["symbol"]),
        contract_id=contract_id,
        exchange=_required_text(value["exchange"]),
        source=_required_text(value["source"]),
        observed_at=observed_at,
        bid=_optional_decimal(value.get("bid")),
        ask=_optional_decimal(value.get("ask")),
        last=_optional_decimal(value.get("last")),
        close=close,
        market_data_type=market_data_type,
    )


def _terminal_scenarios_from_mapping(
    value: object,
) -> tuple[PreselectionTerminalScenario, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raise TypeError("terminal_scenarios must be an array")
    result: list[PreselectionTerminalScenario] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {
            "terminal_underlying_price",
            "probability",
        }:
            raise ValueError("terminal scenario fields are invalid")
        result.append(
            PreselectionTerminalScenario(
                terminal_underlying_price=_finite_decimal(
                    item["terminal_underlying_price"],
                    "terminal_underlying_price",
                ),
                probability=_finite_decimal(item["probability"], "probability"),
            )
        )
    return tuple(result)


def _leg_from_mapping(value: object) -> ConditionalOptionLeg:
    if not isinstance(value, Mapping):
        raise TypeError("stored leg must be a mapping")
    return ConditionalOptionLeg(
        underlying=_required_text(value["underlying"]),
        con_id=_optional_int(value.get("con_id")),
        expiry=_optional_date(value.get("expiry")),
        strike=_optional_decimal(value.get("strike")),
        right=_optional_enum(value.get("right"), OptionRight),
        side=_optional_enum(value.get("side"), OptionLegSide),
        ratio=_optional_int(value.get("ratio")),
        quantity=_optional_int(value.get("quantity")),
        bid=_optional_decimal(value.get("bid")),
        ask=_optional_decimal(value.get("ask")),
        quote_asof=_optional_datetime(value.get("quote_asof")),
        quote_batch_id=_optional_text(value.get("quote_batch_id")),
        implied_volatility=_optional_decimal(value.get("implied_volatility")),
        delta=_optional_decimal(value.get("delta")),
        gamma=_optional_decimal(value.get("gamma")),
        theta=_optional_decimal(value.get("theta")),
        vega=_optional_decimal(value.get("vega")),
        volume=_optional_int(value.get("volume")),
        open_interest=_optional_int(value.get("open_interest")),
        dte=_optional_int(value.get("dte"), nonnegative=True),
        local_symbol=_optional_text(value.get("local_symbol")),
        trading_class=_optional_text(value.get("trading_class")),
        multiplier=_optional_int(value.get("multiplier")),
        exchange=_optional_text(value.get("exchange")),
    )


def _latest_premarket(store: object) -> tuple[object | None, bool]:
    reader = getattr(store, "latest_premarket", None)
    if not callable(reader):
        return None, True
    try:
        return reader(), False
    except Exception:
        return None, True


def _existing_open_batch(
    store: object,
    *,
    batch_id: str,
    parent_head_hash: str,
) -> object | None:
    reader = getattr(store, "read_open_batch", None)
    if callable(reader):
        try:
            value = reader(batch_id)
        except (KeyError, LookupError):
            value = None
        except Exception:
            return None
        if value is not None and (
            getattr(value, "parent_head_hash", parent_head_hash) == parent_head_hash
        ):
            return value
    reader = getattr(store, "latest_open_batch", None)
    if callable(reader):
        try:
            value = reader()
        except Exception:
            return None
        if (
            value is not None
            and getattr(value, "batch_id", None) == batch_id
            and getattr(value, "parent_head_hash", None) == parent_head_hash
        ):
            return value
    reader = getattr(store, "latest_open", None)
    if callable(reader):
        try:
            rows = tuple(reader())
        except Exception:
            return None
        if (
            rows
            and all(
                getattr(item, "parent_head_hash", None) == parent_head_hash
                for item in rows
            )
        ):
            return rows[0]
    return None


def _stored_quote_batch_id(value: object) -> str | None:
    direct = getattr(value, "quote_batch_id", None)
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    evaluation = getattr(value, "evaluation", None)
    if isinstance(evaluation, Mapping):
        nested = evaluation.get("quote_batch_id")
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    return None


def _existing_batch_result(
    value: object,
    *,
    observed_at: datetime,
    scheduled_for: datetime,
    slot: ProducerSlot,
    run_id: str | None,
    parent_head_hash: str,
    fallback_quote_batch_id: str | None = None,
) -> ProducerResult:
    blockers = getattr(value, "blockers", None)
    eligible = getattr(value, "action_pool_eligible", None)
    rows = getattr(value, "rows", None)
    row_count = _known_sequence_count(rows)
    if (
        not isinstance(blockers, Sequence)
        or isinstance(blockers, (str, bytes, bytearray, memoryview))
        or not all(isinstance(item, str) and item for item in blockers)
        or not isinstance(eligible, bool)
        or row_count is None
        or eligible != (len(blockers) == 0)
    ):
        return _result(
            ProducerStatus.NO_TRADE,
            "OPEN_BATCH_RESULT_INVALID",
            observed_at=observed_at,
            scheduled_for=scheduled_for,
            slot=slot,
            run_id=run_id,
            parent_head_hash=parent_head_hash,
            quote_batch_id=fallback_quote_batch_id,
        )
    return ProducerResult(
        status=(
            ProducerStatus.OPEN_REPRICED if eligible else ProducerStatus.NO_TRADE
        ),
        reason_codes=("SLOT_ALREADY_RECORDED",) if eligible else tuple(blockers),
        observed_at=observed_at,
        scheduled_for=scheduled_for,
        slot=slot,
        trading_date=scheduled_for.date(),
        run_id=run_id,
        parent_head_hash=parent_head_hash,
        quote_batch_id=_stored_quote_batch_id(value) or fallback_quote_batch_id,
        written_count=0,
    )


def _run_matches_slot(run: object, scheduled_for: datetime) -> bool:
    created_at = getattr(run, "created_at", None)
    try:
        checked = _aware_datetime(created_at, "run.created_at").astimezone(NEW_YORK)
    except (TypeError, ValueError):
        return False
    return checked.replace(second=0, microsecond=0) == scheduled_for


def _run_et_date(run: object) -> date | None:
    try:
        return _aware_datetime(
            getattr(run, "created_at", None),
            "run.created_at",
        ).astimezone(NEW_YORK).date()
    except (TypeError, ValueError):
        return None


def _gateway_identity(value: GatewayOptionContractRef) -> dict[str, object]:
    return {
        "conId": value.contract_id,
        "localSymbol": value.local_symbol,
        "tradingClass": value.trading_class,
        "multiplier": value.multiplier,
        "exchange": value.exchange,
        "expiry": value.expiration,
        "strike": value.strike,
        "right": value.right,
    }


def _snapshot_batch_id(snapshot: object) -> str | None:
    return _optional_text(getattr(snapshot, "quote_batch_id", None))


def _premarket_run_id(trading_date: date) -> str:
    return f"top10-premarket-{trading_date.isoformat()}"


def _open_batch_id(trading_date: date) -> str:
    return f"top10-open-{trading_date.isoformat()}"


def _aware_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _finite_decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be numeric")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (int, float, str)):
        try:
            result = Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} must be numeric") from exc
    else:
        raise TypeError(f"{field_name} must be numeric")
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else _finite_decimal(value, "decimal")


def _optional_int(value: object, *, nonnegative: bool = False) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("integer field must be an int or None")
    if nonnegative and value < 0:
        raise ValueError("integer field must be nonnegative")
    return value


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise TypeError("date field cannot be a datetime")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError("date field must be a date, ISO string, or None")


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return _aware_datetime(value, "datetime field")


def _optional_enum(value: object, enum_type: type[Enum]) -> Any:
    if value is None:
        return None
    if isinstance(value, enum_type):
        return value
    return enum_type(str(value))


def _required_text(value: object) -> str:
    checked = _optional_text(value)
    if checked is None:
        raise ValueError("text field cannot be blank")
    return checked


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("text field must be a nonblank string or None")
    return value.strip()


def _checked_source_binding(
    purpose: object,
    batch_id: object,
    content_hash: object,
) -> tuple[str, str, str] | None:
    """Return one immutable source triple; no component may stand alone."""

    if purpose is None and batch_id is None and content_hash is None:
        return None
    if (
        purpose not in {PREMARKET_ACCOUNT_PURPOSE, OPEN_REPRICE_PURPOSE}
        or not isinstance(batch_id, str)
        or not batch_id
        or batch_id != batch_id.strip()
        or len(batch_id) > 128
        or batch_id[0]
        not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        or any(
            character
            not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-"
            for character in batch_id
        )
        or not isinstance(content_hash, str)
        or len(content_hash) != 64
        or any(character not in "0123456789abcdef" for character in content_hash)
    ):
        raise ValueError("source batch binding is invalid")
    assert isinstance(purpose, str)
    return purpose, batch_id, content_hash


def _source_batch_purpose(binding: tuple[str, str, str] | None) -> str | None:
    return None if binding is None else binding[0]


def _source_batch_id(binding: tuple[str, str, str] | None) -> str | None:
    return None if binding is None else binding[1]


def _source_batch_hash(binding: tuple[str, str, str] | None) -> str | None:
    return None if binding is None else binding[2]


def _stored_source_binding_matches(
    value: object,
    expected: tuple[str, str, str] | None,
) -> bool:
    try:
        actual = _checked_source_binding(
            getattr(value, "source_batch_purpose", None),
            getattr(value, "source_batch_id", None),
            getattr(value, "source_batch_hash", None),
        )
    except (TypeError, ValueError):
        return False
    return actual == expected


def _text_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raise TypeError("text collection must be an array")
    return tuple(_required_text(item) for item in value)


def _strict_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("bool field must be a bool")
    return value


__all__ = [
    "BrokerAccountStateReader",
    "BrokerSnapshotProvider",
    "IndependentTop10StructureSource",
    "NewsPreselectionStore",
    "ProducerResult",
    "ProducerSlot",
    "ProducerStatus",
    "ResolvedStructure",
    "Top10StructureResolution",
    "Top10PreselectionProducer",
    "TradingSessionGate",
    "validate_entry_positions",
]
