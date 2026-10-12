from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from options_copilot.bridge.instruction_reader import (
    InstructionStateReader,
    InstructionStateStatus,
)
from options_copilot.bridge.store import BridgeRecord, BridgeStatus


NOW = datetime(2026, 8, 8, 4, 0, tzinfo=timezone.utc)


class _BridgeStore:
    def __init__(self, rows: object = ()) -> None:
        self.rows = rows
        self.calls = 0

    def list_pending(self) -> object:
        self.calls += 1
        if isinstance(self.rows, BaseException):
            raise self.rows
        return self.rows


def _reader(
    *,
    bridge_rows: object = (),
    working_orders: object = (),
    creator_rows: object = (),
) -> InstructionStateReader:
    store = _BridgeStore(bridge_rows)

    def read_orders() -> object:
        if isinstance(working_orders, BaseException):
            raise working_orders
        return working_orders

    def read_creator() -> object:
        if isinstance(creator_rows, BaseException):
            raise creator_rows
        return creator_rows

    return InstructionStateReader(
        store,
        broker_working_order_reader=read_orders,
        creator_state_reader=read_creator,
    )


def _record(
    approval_id: str,
    status: BridgeStatus,
    *,
    instruction_id: str | None = None,
    external_call_started_at: datetime | None = None,
) -> BridgeRecord:
    return BridgeRecord(
        sequence=1,
        approval_id=approval_id,
        status=status,
        claimed_at=NOW,
        authorized_at=NOW if status is BridgeStatus.AUTHORIZED else None,
        instruction_id=instruction_id,
        external_call_started_at=external_call_started_at,
        current_authority_proof_hash=(
            "a" * 64 if status is BridgeStatus.AUTHORIZED else None
        ),
    )


def test_missing_creator_reader_is_unknown_and_gateway_callable_returns_none() -> None:
    store = _BridgeStore()
    reader = InstructionStateReader(
        store,
        broker_working_order_reader=lambda: (),
    )

    snapshot = reader.read()

    assert snapshot.status is InstructionStateStatus.UNKNOWN
    assert snapshot.reason == "CREATOR_STATE_READER_UNAVAILABLE"
    assert snapshot.instructions == ()
    assert snapshot.working_order_count is None
    assert reader() is None
    assert store.calls == 0


def test_explicitly_disabled_creator_transport_uses_local_and_broker_authority() -> None:
    store = _BridgeStore()
    reader = InstructionStateReader(
        store,
        broker_working_order_reader=lambda: (),
        creator_transport_enabled=False,
    )

    snapshot = reader.read()

    assert snapshot.status is InstructionStateStatus.KNOWN
    assert snapshot.reason == (
        "INSTRUCTION_STATE_KNOWN_CREATOR_TRANSPORT_DISABLED"
    )
    assert snapshot.instructions == ()
    assert snapshot.working_order_count == 0
    assert reader() == ()


def test_creator_none_is_unknown_not_known_empty() -> None:
    reader = _reader(creator_rows=None)

    snapshot = reader.read()

    assert snapshot.status is InstructionStateStatus.UNKNOWN
    assert snapshot.reason == "CREATOR_INSTRUCTION_STATE_UNKNOWN"
    assert snapshot.working_order_count == 0
    assert reader() is None


@pytest.mark.parametrize(
    "working_orders",
    (None, RuntimeError("offline"), "not-an-array", b"not-an-array", ({"ok": 1}, 2)),
)
def test_unknown_or_invalid_broker_working_orders_fail_closed(
    working_orders: object,
) -> None:
    snapshot = _reader(working_orders=working_orders).read()

    assert snapshot.status is InstructionStateStatus.UNKNOWN
    assert snapshot.reason == "BROKER_WORKING_ORDER_STATE_UNKNOWN"
    assert snapshot.instructions == ()
    assert snapshot.working_order_count is None


@pytest.mark.parametrize(
    "bridge_rows",
    (
        None,
        RuntimeError("locked"),
        "not-an-array",
        (SimpleNamespace(status=BridgeStatus.CLAIMED),),
        (_record("terminal", BridgeStatus.COMPLETED),),
    ),
)
def test_unknown_or_invalid_local_bridge_state_fails_closed(
    bridge_rows: object,
) -> None:
    snapshot = _reader(bridge_rows=bridge_rows).read()

    assert snapshot.status is InstructionStateStatus.UNKNOWN
    assert snapshot.reason == "LOCAL_BRIDGE_STATE_UNKNOWN"
    assert snapshot.instructions == ()


@pytest.mark.parametrize(
    "creator_rows",
    (
        "not-an-array",
        bytearray(b"not-an-array"),
        ({"instruction_id": "safe"}, object()),
        RuntimeError("connector unavailable"),
    ),
)
def test_invalid_creator_state_fails_closed(creator_rows: object) -> None:
    snapshot = _reader(creator_rows=creator_rows).read()

    assert snapshot.status is InstructionStateStatus.UNKNOWN
    assert snapshot.reason == "CREATOR_INSTRUCTION_STATE_UNKNOWN"
    assert snapshot.instructions == ()


def test_three_known_empty_sources_return_authoritative_empty_tuple() -> None:
    reader = _reader()

    snapshot = reader.read()

    assert snapshot.status is InstructionStateStatus.KNOWN
    assert snapshot.reason == "INSTRUCTION_STATE_KNOWN"
    assert snapshot.instructions == ()
    assert snapshot.working_order_count == 0
    assert reader() == ()


def test_claimed_and_authorized_local_handoffs_are_projected_as_blockers() -> None:
    claimed = _record("approval-claimed", BridgeStatus.CLAIMED)
    authorized = _record(
        "approval-authorized",
        BridgeStatus.AUTHORIZED,
        external_call_started_at=NOW,
    )
    snapshot = _reader(bridge_rows=(claimed, authorized)).read()

    assert snapshot.status is InstructionStateStatus.KNOWN
    assert len(snapshot.instructions) == 2
    assert snapshot.instructions[0] == {
        "instruction_state_source": "LOCAL_BRIDGE_HANDOFF",
        "sequence": 1,
        "approval_id": "approval-claimed",
        "status": "CLAIMED",
        "instruction_id": None,
        "external_call_reserved": False,
        "unknown_outcome": False,
        "current_authority_verified": False,
    }
    assert snapshot.instructions[1]["status"] == "AUTHORIZED"
    assert snapshot.instructions[1]["external_call_reserved"] is True
    assert snapshot.instructions[1]["current_authority_verified"] is True


def test_creator_rows_are_merged_as_detached_copies() -> None:
    source = {
        "instruction_id": "review-1",
        "status": "PENDING_REVIEW",
        "legs": [{"contract_id": 101}],
    }
    reader = _reader(creator_rows=(source,))

    snapshot = reader.read()
    output = snapshot.instructions[0]
    source["status"] = "CHANGED_AT_SOURCE"
    source["legs"][0]["contract_id"] = 999  # type: ignore[index]
    output["status"] = "CHANGED_BY_CALLER"
    output["legs"][0]["contract_id"] = 202  # type: ignore[index]

    assert output["instruction_state_source"] == "CREATOR_REVIEW_INSTRUCTION"
    assert reader.read().instructions[0]["status"] == "CHANGED_AT_SOURCE"
    assert source["status"] == "CHANGED_AT_SOURCE"
    assert source["legs"][0]["contract_id"] == 999  # type: ignore[index]


def test_working_orders_are_counted_but_not_mislabeled_as_creator_instructions() -> None:
    snapshot = _reader(
        working_orders=({"order_id": 7, "status": "Submitted"},),
    ).read()

    assert snapshot.status is InstructionStateStatus.KNOWN
    assert snapshot.working_order_count == 1
    assert snapshot.instructions == ()
