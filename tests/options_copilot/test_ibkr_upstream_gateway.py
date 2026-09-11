"""Offline end-to-end gateway tests for real response-ended control batches."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway.ibkr_readonly import (
    BrokerConnectionError,
    BrokerControlAuthorityError,
    IBKRReadOnlyGateway,
)


NOW = datetime(2026, 9, 9, 1, 0, tzinfo=timezone.utc)


class Event:
    def __init__(self) -> None:
        self.handlers: list[object] = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.handlers.remove(handler)
        return self

    def emit(self, *args: object) -> None:
        for handler in tuple(self.handlers):
            handler(*args)


class ResponseIB:
    """Only explicit raw callbacks can provide control data in this fake SDK."""

    def __init__(self) -> None:
        self.connected = False
        self.connect_calls = 0
        self.market_reads = 0
        self.sdk_auto_summary_calls = 0
        self.request_id = 0
        self.wire_calls: list[str] = []
        self.cancelled: list[int] = []
        self.errorEvent = Event()
        self.connectedEvent = Event()
        self.disconnectedEvent = Event()
        self.errorEvent += self._onError
        self.wrapper = SimpleNamespace(**{
            name: lambda *_args: None
            for name in (
                "accountSummary", "accountSummaryEnd", "position", "positionEnd",
                "openOrder", "openOrderEnd",
            )
        })
        self.client = self
        self.clock = NOW
        self.monotonic = 0.0
        self.mode = "complete"
        self.initializing = False
        self.position_rows = [("DU_TEST", SimpleNamespace(
            conId=101, symbol="SPY", localSymbol="SPY C", secType="OPT",
            currency="USD", exchange="SMART", lastTradeDateOrContractMonth="20261016",
            right="C", strike=650.0, multiplier="100", tradingClass="SPY",
        ), 1, 125.0)]
        self.order_rows = [(77, self.position_rows[0][1], SimpleNamespace(
            account="DU_TEST", orderId=77, permId=707, clientId=17, action="BUY",
            orderType="LMT", totalQuantity=1, lmtPrice=1.25, tif="DAY", transmit=False,
        ), SimpleNamespace(status="Submitted"))]

    def _onError(self, _request_id, code, *_args) -> None:
        if code == 1102:
            self.sdk_auto_summary_calls += 1

    def connect(self, *_args, **kwargs) -> None:
        assert kwargs["readonly"] is True
        self.connect_calls += 1
        self.connected = True
        self.initializing = True
        try:
            self.client.reqPositions()
        finally:
            self.initializing = False
        self.connectedEvent.emit()

    def disconnect(self) -> None:
        self.connected = False
        self.disconnectedEvent.emit()

    def isConnected(self) -> bool:
        return self.connected

    def managedAccounts(self):
        return ["DU_TEST"]

    def run(self, awaitable):
        return asyncio.get_event_loop().run_until_complete(awaitable)

    def getReqId(self) -> int:
        self.request_id += 1
        return self.request_id

    def reqAccountSummary(self, request_id, _group, _tags) -> None:
        self.wire_calls.append("account")
        if self.mode != "empty_account":
            for tag, value in (
                ("NetLiquidation", "25000.5"), ("EquityWithLoanValue", "24000"),
                ("AvailableFunds", "23000"), ("BuyingPower", "20000"),
                ("InitMarginReq", "0"), ("MaintMarginReq", "0"),
                ("ExcessLiquidity", "22000"), ("DayTradesRemaining", "3"),
            ):
                self.wrapper.accountSummary(request_id, "DU_TEST", tag, value, "USD")
        self.wrapper.accountSummaryEnd(request_id)

    def cancelAccountSummary(self, request_id) -> None:
        self.cancelled.append(request_id)

    def reqPositions(self) -> None:
        if not self.initializing:
            self.wire_calls.append("positions")
        for row in self.position_rows:
            self.wrapper.position(*row)
        if self.initializing or self.mode != "missing_position_end":
            self.wrapper.positionEnd()

    def cancelPositions(self) -> None:
        pass

    def reqAllOpenOrders(self) -> None:
        self.wire_calls.append("orders")
        for row in self.order_rows:
            self.wrapper.openOrder(*row)
        self.wrapper.openOrderEnd()
        if self.mode == "missing_position_end":
            self.monotonic += 10.0

    def reqOpenOrders(self) -> None:
        self.wrapper.openOrderEnd()

    def accountSummary(self):
        raise AssertionError("SDK cached account rows must never be authority")

    def portfolio(self):
        raise AssertionError("SDK cached portfolio marks must never be authority")

    def openTrades(self):
        raise AssertionError("SDK cached open trades must never be authority")

    def market_observation(self):
        self.market_reads += 1
        return {"test_read": True}


def gateway_for(tmp_path: Path, fake: ResponseIB, *, factory=None) -> IBKRReadOnlyGateway:
    return IBKRReadOnlyGateway(
        OptionsCopilotConfig(data_dir=tmp_path / "data", log_dir=tmp_path / "logs"),
        ib_factory=factory or (lambda: fake), now=lambda: fake.clock,
        monotonic=lambda: fake.monotonic,
        pacing_observer=lambda ib: ib.market_observation(),
    )


def test_initial_socket_connect_is_not_control_authority(tmp_path: Path) -> None:
    fake = ResponseIB()
    with gateway_for(tmp_path, fake) as gateway:
        assert gateway.connected is True
        assert gateway.upstream_health()["status"] == "RECOVERY_PENDING"
        assert gateway.upstream_health()["verified_at"] is None
        assert fake.wire_calls == []
        assert gateway.market_data_pacing_observation() == {"test_read": True}
        assert gateway.upstream_health()["status"] == "RECOVERY_PENDING"


def test_control_accessors_share_one_completed_unrestamped_batch(tmp_path: Path) -> None:
    fake = ResponseIB()
    with gateway_for(tmp_path, fake) as gateway:
        account = gateway.account_snapshot()
        initial_identity = gateway.upstream_health()
        fake.clock += timedelta(seconds=4)
        positions = gateway.positions()
        orders = gateway.working_orders()
        assert account.net_liquidation == Decimal("25000.5")
        assert account.initial_margin == 0
        assert positions[0].asof == account.asof == NOW
        assert positions[0].quantity == Decimal("1")
        assert positions[0].average_cost == Decimal("125.0")
        assert all(getattr(positions[0], field) is None for field in (
            "market_price", "market_value", "unrealized_pnl", "realized_pnl",
        ))
        assert orders[0]["order_id"] == 77
        assert gateway.upstream_health() == initial_identity
        assert fake.wire_calls == ["account", "positions", "orders"]
        fake.clock = NOW + timedelta(seconds=5, microseconds=1)
        assert gateway.account_snapshot().asof == fake.clock
        assert fake.wire_calls == ["account", "positions", "orders"] * 2
        assert len(fake.cancelled) == 2


@pytest.mark.parametrize("recovery_code", [1101, 1102])
def test_upstream_loss_and_recovery_never_promote_sdk_caches(
    tmp_path: Path, recovery_code: int,
) -> None:
    fake = ResponseIB()
    with gateway_for(tmp_path, fake) as gateway:
        gateway.account_snapshot()
        first = gateway.upstream_health()
        fake.errorEvent.emit(-1, 1100, "private broker text")
        assert gateway.connected is True
        assert gateway.upstream_health()["status"] == "LOST"
        assert gateway.upstream_health()["generation"] > first["generation"]
        for read in (gateway.account_snapshot, gateway.positions, gateway.working_orders,
                     gateway.market_data_pacing_observation):
            with pytest.raises(BrokerControlAuthorityError):
                read()
        assert fake.market_reads == 0
        assert fake.connect_calls == 1
        fake.errorEvent.emit(-1, recovery_code, "not synchronized yet")
        assert gateway.upstream_health()["status"] == "RECOVERY_PENDING"
        assert fake.wire_calls == ["account", "positions", "orders"]
        with pytest.raises(BrokerControlAuthorityError):
            gateway.market_data_pacing_observation()
        assert fake.sdk_auto_summary_calls == 0
        fake.clock += timedelta(seconds=1)
        assert gateway.account_snapshot().asof == fake.clock
        assert gateway.upstream_health()["status"] == "READY"
        assert fake.wire_calls == ["account", "positions", "orders"] * 2
        assert fake.connect_calls == 1


@pytest.mark.parametrize("mode", ["empty_account", "missing_position_end"])
def test_false_empty_or_uncertain_end_poisons_without_same_session_retry(
    tmp_path: Path, mode: str,
) -> None:
    fake = ResponseIB()
    fake.mode = mode
    with gateway_for(tmp_path, fake) as gateway:
        with pytest.raises(BrokerControlAuthorityError) as caught:
            gateway.account_snapshot()
        assert caught.value.reason_code in {
            "CONTROL_ACCOUNT_FIELDS_INCOMPLETE", "CONTROL_SYNC_TIMEOUT",
        }
        assert gateway.upstream_health()["status"] == "RECONNECT_REQUIRED"
        assert gateway.upstream_health()["verified_at"] is None
        calls = list(fake.wire_calls)
        fake.mode = "complete"
        fake.wrapper.positionEnd()  # A delayed uncorrelated end cannot repair authority.
        fake.errorEvent.emit(-1, 1102, "recovered")
        with pytest.raises(BrokerControlAuthorityError):
            gateway.working_orders()
        assert fake.wire_calls == calls
        assert fake.connect_calls == 1


def test_real_completed_empty_positions_and_orders_are_known_empty(tmp_path: Path) -> None:
    fake = ResponseIB()
    fake.position_rows = []
    fake.order_rows = []
    with gateway_for(tmp_path, fake) as gateway:
        assert gateway.positions() == ()
        assert gateway.working_orders() == ()
        assert gateway.upstream_health()["status"] == "READY"


def test_health_is_cache_only_while_owner_and_gateway_lock_are_busy(tmp_path: Path) -> None:
    fake = ResponseIB()
    started = threading.Event()
    release = threading.Event()
    with gateway_for(tmp_path, fake) as gateway:
        gateway.account_snapshot()

        def blocked() -> None:
            with gateway._lock:
                started.set()
                release.wait(2)

        caller = threading.Thread(target=lambda: gateway._call_on_owner(blocked))
        caller.start()
        try:
            assert started.wait(1)
            fake.errorEvent.emit(-1, 1100, "upstream lost")
            began = time.monotonic()
            assert gateway.upstream_health()["status"] == "LOST"
            assert time.monotonic() - began < 0.2
        finally:
            release.set()
            caller.join(2)
        assert not caller.is_alive()


def test_owned_callbacks_are_cleaned_and_old_client_cannot_invalidate_new(
    tmp_path: Path,
) -> None:
    first = ResponseIB()
    second = ResponseIB()
    instances = iter((first, second))
    gateway = gateway_for(tmp_path, first, factory=lambda: next(instances))
    original = first.wrapper.positionEnd
    gateway.connect()
    gateway.account_snapshot()
    previous_identity = gateway.upstream_health()
    old_handlers = tuple(first.errorEvent.handlers)
    gateway.disconnect()
    assert first.wrapper.positionEnd is original
    assert first.errorEvent.handlers == [first._onError]
    assert first.connectedEvent.handlers == first.disconnectedEvent.handlers == []
    gateway.connect()
    try:
        gateway.account_snapshot()
        current = gateway.upstream_health()
        assert current["generation"] > previous_identity["generation"]
        for handler in old_handlers:
            handler(-1, 1100, "stale old SDK callback")
        assert gateway.upstream_health() == current
    finally:
        gateway.disconnect()


def test_missing_response_protocol_never_falls_back_to_cached_sdk(tmp_path: Path) -> None:
    fake = ResponseIB()
    del fake.wrapper.positionEnd
    gateway = gateway_for(tmp_path, fake)
    with pytest.raises(BrokerConnectionError):
        gateway.connect()
    assert fake.connect_calls == 0
    assert fake.errorEvent.handlers == [fake._onError]
    assert gateway.upstream_health()["status"] == "DISCONNECTED"
