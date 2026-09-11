"""Control recovery requires fresh, generation-bound response-end evidence."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from options_copilot.gateway.control_authority import ControlAuthority, ControlAuthorityError


NOW = datetime(2026, 9, 9, 1, 0, tzinfo=timezone.utc)
TAGS = ("NetLiquidation", "EquityWithLoanValue", "AvailableFunds", "BuyingPower",
        "InitMarginReq", "MaintMarginReq", "ExcessLiquidity")


class Event:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        if handler in self.handlers:
            self.handlers.remove(handler)
        return self

    def emit(self, *args):
        for handler in tuple(self.handlers):
            handler(*args)


class Wrapper:
    def __init__(self):
        self.original_calls = []
        self.trades = {1: SimpleNamespace(orderStatus=SimpleNamespace(status="Cancelled"))}
        self.positions = {"A1": {123: "stale SDK position"}}
        self.portfolio = {"A1": {123: "stale SDK valuation"}}

    def accountSummary(self, *args): self.original_calls.append(("accountSummary", args))
    def accountSummaryEnd(self, *args): self.original_calls.append(("accountSummaryEnd", args))
    def position(self, *args): self.original_calls.append(("position", args))
    def positionEnd(self, *args): self.original_calls.append(("positionEnd", args))
    def openOrder(self, *args): self.original_calls.append(("openOrder", args))
    def openOrderEnd(self, *args): self.original_calls.append(("openOrderEnd", args))


class Client:
    def __init__(self, ib):
        self.ib = ib
        self.calls = []
        self.next_id = 100

    def getReqId(self):
        self.next_id += 1
        return self.next_id

    def reqAccountSummary(self, req_id, group, tags):
        self.calls.append(("account", req_id, group, tags))
        self.ib.request_id = req_id
        asyncio.get_running_loop().call_soon(self.ib.respond, "account")

    def reqPositions(self):
        self.calls.append(("positions",))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self.ib.wrapper.positionEnd()
        else:
            loop.call_soon(self.ib.respond, "positions")

    def reqOpenOrders(self):
        self.calls.append(("client_orders",))
        asyncio.get_running_loop().call_soon(self.ib.respond, "orders")

    def reqAllOpenOrders(self):
        self.calls.append(("orders",))
        asyncio.get_running_loop().call_soon(self.ib.respond, "orders")

    def cancelAccountSummary(self, req_id):
        self.calls.append(("cleanup", req_id))

    def cancelPositions(self):
        self.calls.append(("cleanup_positions",))


class FakeIB:
    def __init__(self):
        self.wrapper = Wrapper()
        self.client = Client(self)
        self.errorEvent, self.connectedEvent, self.disconnectedEvent = Event(), Event(), Event()
        self.errorEvent += self._onError
        self.sdk_summary_requests = 0
        self.accounts = ["A1"]
        self.request_id = None
        self.socket_connected = True
        self.reply = None
        self.now = NOW

    def _onError(self, _req_id, code, *_args):
        if code == 1102:
            self.sdk_summary_requests += 1

    def isConnected(self): return self.socket_connected
    def managedAccounts(self): return self.accounts
    def run(self, task): return asyncio.run(task)
    def accountSummary(self): raise AssertionError("SDK cache read")
    def portfolio(self): raise AssertionError("SDK cache read")
    def positions(self): raise AssertionError("SDK cache read")
    def openTrades(self): raise AssertionError("SDK cache read")

    def respond(self, kind):
        if self.reply is not None:
            return self.reply(kind)
        self.respond_normally(kind)

    def respond_normally(self, kind):
        if kind == "account":
            for tag in TAGS:
                self.wrapper.accountSummary(self.request_id, "A1", tag, "10000", "USD")
            self.wrapper.accountSummaryEnd(self.request_id)
        elif kind == "positions":
            self.wrapper.positionEnd()
        elif kind == "orders":
            self.wrapper.openOrderEnd()

    def initial_connect(self):
        self.client.reqPositions()
        self.connectedEvent.emit()
        self.client.calls.clear()


@pytest.fixture
def control():
    ib = FakeIB()
    authority = ControlAuthority(ib, clock=lambda: ib.now)
    ib.initial_connect()
    try:
        yield ib, authority
    finally:
        authority.close()


def test_initial_connect_is_pending_until_all_three_real_response_ends(control):
    ib, authority = control
    assert authority.health()["status"] == "RECOVERY_PENDING"
    with pytest.raises(ControlAuthorityError):
        authority.require_ready()
    batch = authority.read(timeout_seconds=0.2)
    assert batch.generation == 1 and batch.verified_at == NOW
    assert len(batch.account_rows) == 7 and batch.positions == () and batch.working_orders == ()
    assert [row[0] for row in ib.client.calls] == ["account", "positions", "orders", "cleanup", "cleanup_positions"]
    assert authority.health() == {"status": "READY", "generation": 1,
                                 "verified_at": NOW.isoformat(), "reason_codes": []}
    authority.require_ready()


def test_empty_response_does_not_reuse_or_erase_old_sdk_positions(control):
    ib, authority = control
    batch = authority.read(timeout_seconds=0.2)
    assert batch.positions == ()
    assert ib.wrapper.positions == {"A1": {123: "stale SDK position"}}
    assert ib.wrapper.portfolio == {"A1": {123: "stale SDK valuation"}}


def test_five_second_batch_cache_preserves_original_time_and_detaches_values(control):
    ib, authority = control
    original = authority.read(timeout_seconds=0.2)
    original.account_rows[0]["value"] = "999999"
    ib.now += timedelta(seconds=5)
    cached = authority.read(timeout_seconds=0.2)
    assert cached.verified_at == NOW and cached.account_rows[0]["value"] == "10000"
    assert len(ib.client.calls) == 5
    ib.now += timedelta(microseconds=1)
    fresh = authority.read(timeout_seconds=0.2)
    assert fresh.verified_at == ib.now and len(ib.client.calls) == 10


@pytest.mark.parametrize("recovery", [1101, 1102])
def test_1100_immediately_revokes_and_recovery_event_never_grants_ready(control, recovery):
    ib, authority = control
    first = authority.read(timeout_seconds=0.2)
    ib.errorEvent.emit(-1, 1100, "private broker text", None)
    assert authority.health()["status"] == "LOST"
    assert authority.health()["generation"] == first.generation + 1
    with pytest.raises(ControlAuthorityError): authority.require_ready()
    with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
    assert len(ib.client.calls) == 5
    ib.errorEvent.emit(-1, recovery, "private broker text", None)
    assert authority.health()["status"] == "RECOVERY_PENDING"
    with pytest.raises(ControlAuthorityError): authority.require_ready()
    assert ib.sdk_summary_requests == 0
    recovered = authority.read(timeout_seconds=0.2)
    assert recovered.generation > first.generation and len(ib.client.calls) == 10


@pytest.mark.parametrize("missing", ["account", "positions", "orders"])
def test_missing_real_end_ack_times_out_even_if_sdk_future_would_return_empty(control, missing):
    ib, authority = control
    ib.reply = lambda kind: None if kind == missing else ib.respond_normally(kind)
    with pytest.raises(ControlAuthorityError, match="CONTROL_SYNC_TIMEOUT"):
        authority.read(timeout_seconds=0.01)
    assert authority.health()["status"] == "RECONNECT_REQUIRED"
    count = len(ib.client.calls)
    ib.respond_normally(missing)
    ib.errorEvent.emit(-1, 1102, "restored", None)
    assert authority.health()["status"] == "RECONNECT_REQUIRED"
    with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
    assert len(ib.client.calls) == count


def test_only_matching_summary_reqid_and_end_are_accepted(control):
    ib, authority = control
    def reply(kind):
        if kind == "account":
            for tag in TAGS:
                ib.wrapper.accountSummary(999, "A1", tag, "90000", "USD")
            ib.wrapper.accountSummaryEnd(999)
        else:
            ib.respond_normally(kind)
    ib.reply = reply
    with pytest.raises(ControlAuthorityError, match="CONTROL_SYNC_TIMEOUT"):
        authority.read(timeout_seconds=0.01)


def test_request_error_cannot_become_empty_success_even_with_end_ack(control):
    ib, authority = control
    def reply(kind):
        if kind == "account":
            ib.errorEvent.emit(ib.request_id, 321, "secret-looking broker error", None)
        ib.respond_normally(kind)
    ib.reply = reply
    with pytest.raises(ControlAuthorityError, match="CONTROL_BROKER_RESPONSE_ERROR"):
        authority.read(timeout_seconds=0.2)
    assert authority.health()["verified_at"] is None
    assert "secret" not in str(authority.health())


def test_raw_current_order_status_replaces_no_old_trade_state(control):
    ib, authority = control
    contract = SimpleNamespace(conId=42)
    order = SimpleNamespace(account="A1", clientId=7, permId=18, totalQuantity=2)
    def reply(kind):
        if kind == "orders":
            ib.wrapper.openOrder(1, contract, order, SimpleNamespace(status="Submitted"))
        ib.respond_normally(kind)
    ib.reply = reply
    batch = authority.read(timeout_seconds=0.2)
    assert batch.working_orders[0]["order_state_status"] == "Submitted"
    assert ib.wrapper.trades[1].orderStatus.status == "Cancelled"
    contract.conId = 999
    assert batch.working_orders[0]["contract"].conId == 42


def test_positions_use_current_response_quantity_and_average_cost(control):
    ib, authority = control
    def reply(kind):
        if kind == "positions":
            ib.wrapper.position("A1", SimpleNamespace(conId=42), 3, 123.5)
            ib.wrapper.position("A1", SimpleNamespace(conId=123), 0, 100)
        ib.respond_normally(kind)
    ib.reply = reply
    batch = authority.read(timeout_seconds=0.2)
    assert len(batch.positions) == 1
    assert batch.positions[0]["position"] == 3 and batch.positions[0]["avgCost"] == 123.5
    assert "marketPrice" not in batch.positions[0]


@pytest.mark.parametrize("accounts", [[], ["A1", "A2"], [""]])
def test_unknown_or_multiple_accounts_fail_before_wire(control, accounts):
    ib, authority = control
    ib.accounts = accounts
    with pytest.raises(ControlAuthorityError, match="CONTROL_SINGLE_ACCOUNT_REQUIRED"):
        authority.read(timeout_seconds=0.2)
    assert ib.client.calls == []


@pytest.mark.parametrize("kind", ["account", "positions", "orders"])
def test_cross_account_callback_rejects_entire_batch(control, kind):
    ib, authority = control
    def reply(current):
        if current == kind:
            if kind == "account":
                ib.wrapper.accountSummary(ib.request_id, "A2", "NetLiquidation", "1", "USD")
            elif kind == "positions":
                ib.wrapper.position("A2", SimpleNamespace(conId=42), 1, 2)
            else:
                ib.wrapper.openOrder(1, SimpleNamespace(conId=42),
                    SimpleNamespace(account="A2", clientId=7, permId=18, totalQuantity=2),
                    SimpleNamespace(status="Submitted"))
        ib.respond_normally(current)
    ib.reply = reply
    with pytest.raises(ControlAuthorityError, match="CONTROL_RESPONSE_INVALID"):
        authority.read(timeout_seconds=0.2)
    assert authority.health()["status"] == "RECONNECT_REQUIRED"


@pytest.mark.parametrize("change", ["missing", "nonfinite", "currency", "duplicate_currency"])
def test_required_account_fields_never_default_to_zero(control, change):
    ib, authority = control
    def reply(kind):
        if kind != "account":
            return ib.respond_normally(kind)
        for tag in TAGS:
            if change == "missing" and tag == "NetLiquidation": continue
            value = "NaN" if change == "nonfinite" and tag == "NetLiquidation" else "10000"
            currency = "EUR" if change == "currency" and tag == "NetLiquidation" else "USD"
            ib.wrapper.accountSummary(ib.request_id, "A1", tag, value, currency)
        if change == "duplicate_currency":
            ib.wrapper.accountSummary(ib.request_id, "A1", "NetLiquidation", "10", "EUR")
        ib.wrapper.accountSummaryEnd(ib.request_id)
    ib.reply = reply
    with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
    assert authority.health()["status"] == "RECONNECT_REQUIRED"


def test_1100_during_batch_and_late_old_end_cannot_publish(control):
    ib, authority = control
    def reply(kind):
        if kind == "positions": ib.errorEvent.emit(-1, 1100, "lost", None)
        ib.respond_normally(kind)
    ib.reply = reply
    with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
    assert authority.health()["status"] == "LOST"
    assert authority.health()["verified_at"] is None
    ib.wrapper.positionEnd()
    ib.wrapper.openOrderEnd()
    assert authority.health()["status"] == "LOST"


def test_close_restores_callbacks_and_sdk_handler_without_disconnect_or_requests(control):
    ib, authority = control
    before = len(ib.client.calls)
    authority.close()
    authority.close()
    assert authority.health()["status"] == "DISCONNECTED"
    assert ib.socket_connected is True and len(ib.client.calls) == before
    assert ib.wrapper.position.__func__ is Wrapper.position
    ib.errorEvent.emit(-1, 1102, "restore", None)
    assert ib.sdk_summary_requests == 1


def test_health_is_cache_only_and_works_without_ib_access(control):
    ib, authority = control
    authority.read(timeout_seconds=0.2)
    ib.isConnected = lambda: (_ for _ in ()).throw(AssertionError("owner access"))
    assert authority.health()["status"] == "READY"
    assert len(ib.client.calls) == 5


def test_cleanup_failure_cannot_publish_ready_and_still_cleans_positions(control):
    ib, authority = control
    def fail_cleanup(req_id):
        ib.client.calls.append(("cleanup", req_id))
        raise RuntimeError("private subscription failure")
    ib.client.cancelAccountSummary = fail_cleanup
    with pytest.raises(ControlAuthorityError, match="CONTROL_REQUEST_CLEANUP_FAILED"):
        authority.read(timeout_seconds=0.2)
    assert authority.health()["status"] == "RECONNECT_REQUIRED"
    assert authority.health()["verified_at"] is None
    assert [row[0] for row in ib.client.calls].count("cleanup") == 1
    assert [row[0] for row in ib.client.calls].count("cleanup_positions") == 1


def test_no_new_request_after_synchronous_invalidation():
    ib = FakeIB()
    authority = ControlAuthority(ib, clock=lambda: ib.now)
    ib.initial_connect()
    def lose_during_account(req_id, group, tags):
        ib.client.calls.append(("account", req_id, group, tags))
        ib.errorEvent.emit(-1, 1100, "upstream lost before other requests", None)
    ib.client.reqAccountSummary = lose_during_account
    try:
        with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
        assert [row[0] for row in ib.client.calls] == ["account", "cleanup"]
        assert authority.health()["status"] == "LOST"
    finally:
        authority.close()


def test_generation_invalidated_during_cleanup_cannot_publish(control):
    ib, authority = control
    original_cleanup = ib.client.cancelAccountSummary
    def invalidate(req_id):
        original_cleanup(req_id)
        ib.errorEvent.emit(-1, 1100, "lost", None)
    ib.client.cancelAccountSummary = invalidate
    with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
    assert authority.health()["status"] == "LOST"
    assert authority.health()["verified_at"] is None


def test_total_deadline_is_checked_before_every_wire_request():
    ib = FakeIB()
    clock = [0.0]
    authority = ControlAuthority(ib, clock=lambda: ib.now, monotonic=lambda: clock[0])
    ib.initial_connect()
    original_request = ib.client.reqAccountSummary
    def exhaust(req_id, group, tags):
        original_request(req_id, group, tags)
        clock[0] = 1.0
    ib.client.reqAccountSummary = exhaust
    try:
        with pytest.raises(ControlAuthorityError, match="CONTROL_SYNC_TIMEOUT"):
            authority.read(timeout_seconds=0.2)
        assert [row[0] for row in ib.client.calls] == ["account", "cleanup"]
    finally:
        authority.close()


def test_initial_sdk_timeout_and_late_position_end_cannot_start_new_enumeration():
    ib = FakeIB()
    # Model SDK timeout: connect still emits connectedEvent without positionEnd.
    ib.client.reqPositions = lambda: ib.client.calls.append(("initial_positions",))
    authority = ControlAuthority(ib, clock=lambda: ib.now)
    try:
        ib.client.reqPositions()
        ib.connectedEvent.emit()
        assert authority.health()["status"] == "RECONNECT_REQUIRED"
        with pytest.raises(ControlAuthorityError, match="CONTROL_INITIAL_ENUMERATION_UNVERIFIED"):
            authority.read(timeout_seconds=0.2)
        ib.wrapper.position("A1", SimpleNamespace(conId=42), 1, 100)
        ib.wrapper.positionEnd()
        ib.errorEvent.emit(-1, 1102, "restored", None)
        with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
        assert ib.client.calls == [("initial_positions",)]
        assert authority.health()["verified_at"] is None
    finally:
        authority.close()


def test_connected_event_without_observed_initial_request_and_end_is_not_proof():
    ib = FakeIB()
    authority = ControlAuthority(ib, clock=lambda: ib.now)
    try:
        ib.wrapper.positionEnd()
        ib.connectedEvent.emit()
        assert authority.health()["status"] == "RECONNECT_REQUIRED"
        with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
        assert ib.client.calls == []
    finally:
        authority.close()


def test_connected_event_cannot_overwrite_prior_upstream_loss():
    ib = FakeIB()
    authority = ControlAuthority(ib, clock=lambda: ib.now)
    try:
        ib.client.reqPositions()
        ib.errorEvent.emit(-1, 1100, "upstream lost during startup", None)
        generation = authority.health()["generation"]
        ib.connectedEvent.emit()
        assert authority.health()["status"] == "LOST"
        assert authority.health()["generation"] == generation
        with pytest.raises(ControlAuthorityError): authority.read(timeout_seconds=0.2)
        assert authority.health()["reason_codes"] != ["CONTROL_INITIAL_SYNC_REQUIRED"]
    finally:
        authority.close()


@pytest.mark.parametrize("method", ["reqPositions", "reqOpenOrders", "reqAllOpenOrders"])
def test_other_sdk_unkeyed_enumeration_cannot_overlap_control_owner(control, method):
    ib, authority = control
    authority.read(timeout_seconds=0.2)
    count = len(ib.client.calls)
    with pytest.raises(ControlAuthorityError, match="CONTROL_UNSCOPED_ENUMERATION_REQUEST"):
        getattr(ib.client, method)()
    assert authority.health()["status"] == "RECONNECT_REQUIRED"
    assert len(ib.client.calls) == count
