"""Fail-closed aggregation of local and creator review-instruction state.

IBKR working orders are read only to prove that broker order state is known.
They are not an authoritative source for review instructions that have not
been submitted to IBKR, so creator state must be supplied independently.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from options_copilot.storage.canonical import freeze_json, thaw_json

from .store import BridgeRecord, BridgeStatus


_NON_ARRAY_TYPES = (str, bytes, bytearray, memoryview)


class InstructionStateStatus(str, Enum):
    KNOWN = "KNOWN"
    UNKNOWN = "UNKNOWN"


class PendingBridgeStore(Protocol):
    """Read-only surface required from :class:`CodexBridgeStore`."""

    def list_pending(self) -> object:
        ...


@dataclass(frozen=True, slots=True)
class InstructionStateSnapshot:
    """One aggregate read with an explicit known-versus-unknown outcome."""

    status: InstructionStateStatus
    reason: str
    instructions: tuple[dict[str, object], ...]
    working_order_count: int | None

    @property
    def known(self) -> bool:
        return self.status is InstructionStateStatus.KNOWN


class InstructionStateReader:
    """Combine local handoffs with authoritative creator instruction state.

    ``__call__`` is intentionally compatible with
    ``IBKRReadOnlyGateway.instruction_reader``: known state returns an array,
    while any missing, malformed, or failed source returns ``None``.  No
    bridge transition or external connector operation is available here.
    """

    def __init__(
        self,
        bridge_store: PendingBridgeStore,
        *,
        broker_working_order_reader: Callable[[], object] | None = None,
        creator_state_reader: Callable[[], object] | None = None,
        creator_transport_enabled: bool = True,
    ) -> None:
        if not isinstance(creator_transport_enabled, bool):
            raise TypeError("creator_transport_enabled must be a bool")
        self._bridge_store = bridge_store
        self._broker_working_order_reader = broker_working_order_reader
        self._creator_state_reader = creator_state_reader
        self._creator_transport_enabled = creator_transport_enabled

    def read(self) -> InstructionStateSnapshot:
        if self._creator_state_reader is None and self._creator_transport_enabled:
            return _unknown("CREATOR_STATE_READER_UNAVAILABLE")
        if self._broker_working_order_reader is None:
            return _unknown("BROKER_WORKING_ORDER_READER_UNAVAILABLE")

        try:
            local_records = _pending_records(self._bridge_store.list_pending())
        except Exception:
            return _unknown("LOCAL_BRIDGE_STATE_UNKNOWN")

        try:
            working_orders = _mapping_array(self._broker_working_order_reader())
        except Exception:
            return _unknown("BROKER_WORKING_ORDER_STATE_UNKNOWN")
        working_order_count = len(working_orders)

        if self._creator_state_reader is None:
            # This is not an empty-state guess: the caller has explicitly
            # attested that no creator transport capability exists.  Active
            # local handoffs and broker-side orders are still projected.
            creator_rows = ()
        else:
            try:
                creator_rows = _mapping_array(self._creator_state_reader())
            except Exception:
                return _unknown(
                    "CREATOR_INSTRUCTION_STATE_UNKNOWN",
                    working_order_count=working_order_count,
                )

        instructions = [_local_handoff_row(record) for record in local_records]
        for row in creator_rows:
            row["instruction_state_source"] = "CREATOR_REVIEW_INSTRUCTION"
            instructions.append(row)
        return InstructionStateSnapshot(
            status=InstructionStateStatus.KNOWN,
            reason=(
                "INSTRUCTION_STATE_KNOWN"
                if self._creator_transport_enabled
                else "INSTRUCTION_STATE_KNOWN_CREATOR_TRANSPORT_DISABLED"
            ),
            instructions=tuple(instructions),
            working_order_count=working_order_count,
        )

    def __call__(self) -> tuple[dict[str, object], ...] | None:
        snapshot = self.read()
        return snapshot.instructions if snapshot.known else None


def _unknown(
    reason: str,
    *,
    working_order_count: int | None = None,
) -> InstructionStateSnapshot:
    return InstructionStateSnapshot(
        status=InstructionStateStatus.UNKNOWN,
        reason=reason,
        instructions=(),
        working_order_count=working_order_count,
    )


def _array(value: object) -> Sequence[object]:
    if isinstance(value, _NON_ARRAY_TYPES) or not isinstance(value, Sequence):
        raise TypeError("state reader must return a known array")
    return value


def _pending_records(value: object) -> tuple[BridgeRecord, ...]:
    rows = _array(value)
    records: list[BridgeRecord] = []
    for row in rows:
        if not isinstance(row, BridgeRecord):
            raise TypeError("bridge pending row must be a BridgeRecord")
        if row.status not in {BridgeStatus.CLAIMED, BridgeStatus.AUTHORIZED}:
            raise ValueError("bridge pending row must be active")
        records.append(row)
    return tuple(records)


def _mapping_array(value: object) -> tuple[dict[str, object], ...]:
    rows = _array(value)
    detached: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError("state reader array rows must be objects")
        copy = thaw_json(freeze_json(row))
        if not isinstance(copy, dict):  # pragma: no cover - Mapping guard
            raise TypeError("state reader array rows must detach as objects")
        detached.append(copy)
    return tuple(detached)


def _local_handoff_row(record: BridgeRecord) -> dict[str, object]:
    return {
        "instruction_state_source": "LOCAL_BRIDGE_HANDOFF",
        "sequence": record.sequence,
        "approval_id": record.approval_id,
        "status": record.status.value,
        "instruction_id": record.instruction_id,
        "external_call_reserved": record.external_call_reserved,
        "unknown_outcome": record.unknown_outcome,
        "current_authority_verified": record.current_authority_verified,
    }


__all__ = [
    "InstructionStateReader",
    "InstructionStateSnapshot",
    "InstructionStateStatus",
    "PendingBridgeStore",
]
