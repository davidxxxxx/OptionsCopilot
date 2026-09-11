"""Restart-safe Top-10 producer bound to one external broker batch per tick.

The external connector owns acquisition.  This adapter reads its immutable
file once, captures that exact :class:`ExternalReadonlyBatch`, and gives the
same object to account preflight, ``BrokerSnapshotBuilder``, and the NAV
reader.  The 09:35 parent comes only from the append-only SQLite ledger, so a
process restart cannot silently replace the 09:20 structure set.

This module is supporting-only and exposes no approval, instruction, bridge,
or order surface.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Protocol
from zoneinfo import ZoneInfo

from options_copilot.gateway.broker_snapshot import BrokerSnapshotBuilder
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
    ExternalReadonlyBatch,
)
from options_copilot.storage.canonical import utc_datetime

from .external_tick_runner import (
    ParentContractIdentity,
    external_contract_identities,
    validate_external_readonly_batch,
)
from .open_reprice_economics import OpenRepriceEconomicsResolver
from .preselection_producer import (
    ProducerResult,
    ProducerSlot,
    ProducerStatus,
    ResolvedStructure,
    Top10PreselectionProducer,
)


NEW_YORK = ZoneInfo("America/New_York")
PREMARKET_SLOT = time(9, 20)
OPEN_REPRICE_SLOT = time(9, 35)
TOP10_COUNT = 10


class ExternalReadonlyFeedReader(Protocol):
    def read(self) -> ExternalReadonlyBatch:
        raise NotImplementedError


class ExternalTop10StructureSource(Protocol):
    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> Sequence[ResolvedStructure]:
        raise NotImplementedError


class TradingSessionGate(Protocol):
    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        raise NotImplementedError


class NewsPreselectionStore(Protocol):
    def latest_premarket(self) -> object | None:
        raise NotImplementedError

    def append_premarket_run(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError

    def append_open_batch(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class _ConfirmedSessionGate:
    scheduled_for: datetime

    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        try:
            requested = utc_datetime(scheduled_for, field="scheduled_for")
        except (TypeError, ValueError):
            return None
        return requested == self.scheduled_for


@dataclass(frozen=True, slots=True)
class _FrozenStructureSource:
    scheduled_for: datetime
    structures: tuple[ResolvedStructure, ...]

    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> tuple[ResolvedStructure, ...]:
        requested = utc_datetime(scheduled_for, field="scheduled_for")
        if requested != self.scheduled_for:
            raise ValueError("frozen Top-10 slot mismatch")
        return self.structures


class _OpenOnlyStructureSource:
    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> tuple[ResolvedStructure, ...]:
        raise RuntimeError("09:35 must recover structures from the SQLite parent")


class ExternalBatchBoundTop10Producer:
    """Adapt connector-owned external files to the durable Top-10 producer."""

    def __init__(
        self,
        *,
        session_gate: TradingSessionGate,
        feed_reader: ExternalReadonlyFeedReader,
        structure_source: ExternalTop10StructureSource,
        store: NewsPreselectionStore,
        clock: Callable[[], datetime],
        open_economics_resolver: OpenRepriceEconomicsResolver | None = None,
    ) -> None:
        for name, value, method in (
            ("session_gate", session_gate, "is_trading_session"),
            ("feed_reader", feed_reader, "read"),
            ("structure_source", structure_source, "resolve_top10"),
            ("store", store, "latest_premarket"),
        ):
            if not callable(getattr(value, method, None)):
                raise TypeError(f"{name} must expose {method}")
        if not all(
            callable(getattr(store, method, None))
            for method in ("append_premarket_run", "append_open_batch")
        ):
            raise TypeError("store must expose append-only Top-10 ledger methods")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._session_gate = session_gate
        self._feed_reader = feed_reader
        self._structure_source = structure_source
        self._store = store
        self._clock = clock
        self._open_economics_resolver = (
            open_economics_resolver or OpenRepriceEconomicsResolver()
        )

    def tick(self, *, scheduled_for: datetime) -> ProducerResult:
        slot_values = _scheduled_slot(scheduled_for)
        if slot_values is None:
            return _blocked(
                "NON_EXACT_EXTERNAL_SLOT",
                clock=self._clock,
                scheduled_for=None,
                slot=None,
            )
        scheduled, scheduled_et, slot, expected_purpose = slot_values

        try:
            session = self._session_gate.is_trading_session(
                scheduled_for=scheduled
            )
        except Exception:
            session = None
        if session is not True:
            return _blocked(
                "SESSION_CLOSED" if session is False else "SESSION_GATE_UNKNOWN",
                clock=self._clock,
                scheduled_for=scheduled,
                slot=slot,
            )

        try:
            batch = self._feed_reader.read()
        except Exception:
            return _blocked(
                "EXTERNAL_BATCH_UNAVAILABLE",
                clock=self._clock,
                scheduled_for=scheduled,
                slot=slot,
            )
        if not isinstance(batch, ExternalReadonlyBatch):
            return _blocked(
                "EXTERNAL_BATCH_INVALID",
                clock=self._clock,
                scheduled_for=scheduled,
                slot=slot,
            )
        batch_values, batch_reason = validate_external_readonly_batch(
            batch,
            expected_purpose=expected_purpose,
            scheduled_for=scheduled,
        )
        if batch_reason is not None:
            return _blocked(
                batch_reason,
                clock=self._clock,
                scheduled_for=scheduled,
                slot=slot,
            )
        assert batch_values is not None
        source_batch_id, source_batch_hash, secdef_rows, quote_rows = batch_values

        if slot is ProducerSlot.PREMARKET_0920:
            try:
                structures = tuple(
                    self._structure_source.resolve_top10(
                        scheduled_for=scheduled
                    )
                )
            except Exception:
                return _blocked(
                    "EXTERNAL_TOP10_INVALID",
                    clock=self._clock,
                    scheduled_for=scheduled,
                    slot=slot,
                )
            structure_source: object = _FrozenStructureSource(
                scheduled,
                structures,
            )
        else:
            expected_contracts, parent_reason = _stored_parent_contracts(
                self._store,
                trading_date=scheduled_et.date(),
            )
            if parent_reason is not None:
                return _blocked(
                    parent_reason,
                    clock=self._clock,
                    scheduled_for=scheduled,
                    slot=slot,
                )
            try:
                secdefs = external_contract_identities(secdef_rows)
                quotes = external_contract_identities(quote_rows)
            except Exception:
                secdefs = quotes = ()
            if secdefs != expected_contracts or quotes != expected_contracts:
                return _blocked(
                    "OPEN_PARENT_STRUCTURE_MISMATCH",
                    clock=self._clock,
                    scheduled_for=scheduled,
                    slot=slot,
                )
            structure_source = _OpenOnlyStructureSource()

        snapshot_builder = BrokerSnapshotBuilder(batch, clock=self._clock)
        producer = Top10PreselectionProducer(
            account_state_reader=batch,
            structure_source=structure_source,  # type: ignore[arg-type]
            snapshot_provider=snapshot_builder,
            store=self._store,
            clock=self._clock,
            session_gate=_ConfirmedSessionGate(scheduled),
            premarket_account_only=True,
            require_exact_top10=True,
            open_economics_resolver=self._open_economics_resolver,
            strategy_nav_reader=_strategy_nav_reader(batch),
            source_batch_purpose=batch.purpose,
            source_batch_id=source_batch_id,
            source_batch_hash=source_batch_hash,
        )
        return producer.tick(scheduled_for=scheduled)


def _scheduled_slot(
    value: object,
) -> tuple[datetime, datetime, ProducerSlot, str] | None:
    try:
        scheduled = utc_datetime(value, field="scheduled_for")
    except (TypeError, ValueError):
        return None
    scheduled_et = scheduled.astimezone(NEW_YORK)
    slot_time = scheduled_et.timetz().replace(tzinfo=None)
    if scheduled_et.second or scheduled_et.microsecond:
        return None
    if slot_time == PREMARKET_SLOT:
        return (
            scheduled,
            scheduled_et,
            ProducerSlot.PREMARKET_0920,
            PREMARKET_ACCOUNT_PURPOSE,
        )
    if slot_time == OPEN_REPRICE_SLOT:
        return (
            scheduled,
            scheduled_et,
            ProducerSlot.OPEN_REPRICE_0935,
            OPEN_REPRICE_PURPOSE,
        )
    return None


def _stored_parent_contracts(
    store: NewsPreselectionStore,
    *,
    trading_date: date,
) -> tuple[tuple[ParentContractIdentity, ...], str | None]:
    try:
        parent = store.latest_premarket()
    except Exception:
        return (), "PREMARKET_LEDGER_READ_FAILED"
    if parent is None:
        return (), "TODAY_0920_PARENT_MISSING"
    if getattr(parent, "run_id", None) != f"top10-premarket-{trading_date.isoformat()}":
        return (), "TODAY_0920_PARENT_MISSING"
    rows = getattr(parent, "rows", None)
    if (
        not isinstance(rows, Sequence)
        or isinstance(rows, (str, bytes, bytearray, memoryview))
        or len(rows) != TOP10_COUNT
        or getattr(parent, "available_count", None) != TOP10_COUNT
    ):
        return (), "TODAY_0920_PARENT_INVALID"

    identities: list[ParentContractIdentity] = []
    seen_contract_ids: set[int] = set()
    try:
        for row in rows:
            refs = getattr(row, "contract_refs", None)
            if (
                not isinstance(refs, Sequence)
                or isinstance(refs, (str, bytes, bytearray, memoryview))
                or not refs
            ):
                raise ValueError("parent contract refs are missing")
            for ref in refs:
                identity = _parent_identity(ref)
                if identity.con_id in seen_contract_ids:
                    raise ValueError("duplicate parent contract id")
                seen_contract_ids.add(identity.con_id)
                identities.append(identity)
    except (AttributeError, TypeError, ValueError, InvalidOperation):
        return (), "TODAY_0920_PARENT_INVALID"
    return tuple(sorted(identities)), None


def _parent_identity(ref: object) -> ParentContractIdentity:
    contract_id = getattr(ref, "contract_id", getattr(ref, "con_id", None))
    expiration = getattr(ref, "expiration", getattr(ref, "expiry", None))
    local_symbol = getattr(ref, "local_symbol", None)
    trading_class = getattr(ref, "trading_class", None)
    multiplier = getattr(ref, "multiplier", None)
    exchange = getattr(ref, "exchange", None)
    try:
        strike = Decimal(str(getattr(ref, "strike", None)))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("parent strike is invalid") from exc
    if (
        isinstance(contract_id, bool)
        or not isinstance(contract_id, int)
        or contract_id <= 0
        or not isinstance(expiration, date)
        or isinstance(expiration, datetime)
        or not isinstance(local_symbol, str)
        or not local_symbol.strip()
        or not isinstance(trading_class, str)
        or not trading_class.strip()
        or isinstance(multiplier, bool)
        or multiplier != 100
        or not isinstance(exchange, str)
        or not exchange.strip()
        or not strike.is_finite()
        or strike <= 0
    ):
        raise ValueError("parent contract identity is invalid")
    raw_right = getattr(ref, "right", None)
    right_value = getattr(raw_right, "value", raw_right)
    if right_value in {"C", "CALL"}:
        right = "CALL"
    elif right_value in {"P", "PUT"}:
        right = "PUT"
    else:
        raise ValueError("parent option right is invalid")
    return ParentContractIdentity(
        con_id=contract_id,
        local_symbol=local_symbol,
        trading_class=trading_class,
        multiplier=multiplier,
        exchange=exchange.upper(),
        expiry=expiration,
        strike=strike,
        right=right,
    )


def _strategy_nav_reader(
    batch: ExternalReadonlyBatch,
) -> Callable[[object], Decimal]:
    def read(snapshot: object) -> Decimal:
        if (
            getattr(snapshot, "quote_batch_id", None) != batch.batch_id
            or batch.verify_hash() is not True
        ):
            raise ValueError("NAV batch binding is invalid")
        nav = batch.strategy_nav()
        try:
            raw_value = nav["strategy_nav"]
            if isinstance(raw_value, Mapping) and set(raw_value) == {"$decimal"}:
                raw_value = raw_value["$decimal"]
            if isinstance(raw_value, bool):
                raise ValueError("strategy NAV is invalid")
            value = Decimal(str(raw_value))
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("strategy NAV is invalid") from exc
        if not value.is_finite() or value <= 0:
            raise ValueError("strategy NAV is invalid")
        return value

    return read


def _blocked(
    reason: str,
    *,
    clock: Callable[[], datetime],
    scheduled_for: datetime | None,
    slot: ProducerSlot | None,
) -> ProducerResult:
    try:
        observed_at = utc_datetime(clock(), field="clock result")
    except (TypeError, ValueError):
        observed_at = None
    trading_date = (
        None
        if scheduled_for is None
        else scheduled_for.astimezone(NEW_YORK).date()
    )
    return ProducerResult(
        status=ProducerStatus.NO_TRADE,
        reason_codes=(reason,),
        observed_at=observed_at,
        scheduled_for=scheduled_for,
        slot=slot,
        trading_date=trading_date,
    )


__all__ = ["ExternalBatchBoundTop10Producer"]
