from __future__ import annotations

import asyncio
from dataclasses import dataclass
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


@dataclass(frozen=True)
class _CallIdentity:
    operation: str
    thread_id: int
    loop: asyncio.AbstractEventLoop


class _AsyncSensitiveFailure(RuntimeError):
    pass


class _ControlEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.handlers.remove(handler)
        return self

    def emit(self, *args):
        for handler in tuple(self.handlers):
            handler(*args)


class _AsyncSensitiveIB:
    """Model an ib_insync-style synchronous client bound to one local loop."""

    def __init__(
        self,
        events: list[_CallIdentity],
        *,
        fail_account: bool = False,
        disconnect_failures: int = 0,
        block_account_once: bool = False,
    ) -> None:
        self.events = events
        self.fail_account = fail_account
        self.disconnect_failures = disconnect_failures
        self.block_account_once = block_account_once
        self.account_entered = threading.Event()
        self.account_release = threading.Event()
        self.account_finished = threading.Event()
        self.connected = False
        self.errorEvent = _ControlEvent()
        self.connectedEvent = _ControlEvent()
        self.disconnectedEvent = _ControlEvent()
        self.wrapper = SimpleNamespace(**{
            name: lambda *_args: None
            for name in (
                "accountSummary", "accountSummaryEnd", "position", "positionEnd",
                "openOrder", "openOrderEnd",
            )
        })
        self.client = self
        self.request_id = 0
        self._record("factory")

    def _record(self, operation: str) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        self.events.append(
            _CallIdentity(
                operation=operation,
                thread_id=threading.get_ident(),
                loop=loop,
            )
        )
        return loop

    def _sync_async_roundtrip(self, operation: str) -> None:
        """Fail exactly as sync ib_insync does on an already-running ASGI loop."""

        loop = self._record(operation)
        coroutine = asyncio.sleep(0)
        try:
            loop.run_until_complete(coroutine)
        except BaseException:
            coroutine.close()
            raise

    def connect(self, _host: str, _port: int, **_kwargs: object) -> None:
        self._sync_async_roundtrip("connect")
        self.connected = True
        self.client.reqPositions()
        self.connectedEvent.emit()

    def isConnected(self) -> bool:
        return self.connected

    def disconnect(self) -> None:
        self._sync_async_roundtrip("disconnect")
        if self.disconnect_failures:
            self.disconnect_failures -= 1
            raise _AsyncSensitiveFailure("sanitized disconnect failure")
        self.connected = False
        self.disconnectedEvent.emit()

    def managedAccounts(self):
        return ["DU_TEST"]

    def run(self, awaitable):
        return asyncio.get_event_loop().run_until_complete(awaitable)

    def getReqId(self):
        self.request_id += 1
        return self.request_id

    def reqAccountSummary(self, request_id, _group, _tags):
        for row in self.accountSummary():
            self.wrapper.accountSummary(
                request_id, "DU_TEST", row.tag, row.value, row.currency,
            )
        self.wrapper.accountSummaryEnd(request_id)

    def cancelAccountSummary(self, _request_id):
        pass

    def reqPositions(self):
        self._record("portfolio")
        self.wrapper.positionEnd()

    def cancelPositions(self):
        pass

    def reqAllOpenOrders(self):
        self._record("openTrades")
        self.wrapper.openOrderEnd()

    def reqOpenOrders(self):
        self.wrapper.openOrderEnd()

    def accountSummary(self) -> list[SimpleNamespace]:
        self._record("accountSummary")
        if self.block_account_once:
            self.block_account_once = False
            self.account_entered.set()
            self.account_release.wait(timeout=2.0)
            self.account_finished.set()
        if self.fail_account:
            raise _AsyncSensitiveFailure("sanitized account failure")
        return [
            SimpleNamespace(tag="NetLiquidation", value="2207.51", currency="USD"),
            SimpleNamespace(tag="EquityWithLoanValue", value="2207.51", currency="USD"),
            SimpleNamespace(tag="AvailableFunds", value="2207.51", currency="USD"),
            SimpleNamespace(tag="BuyingPower", value="8830.04", currency="USD"),
            SimpleNamespace(tag="InitMarginReq", value="0", currency="USD"),
            SimpleNamespace(tag="MaintMarginReq", value="0", currency="USD"),
            SimpleNamespace(tag="ExcessLiquidity", value="2207.51", currency="USD"),
        ]

    def portfolio(self) -> list[object]:
        self._sync_async_roundtrip("portfolio")
        return []

    def openTrades(self) -> list[object]:
        self._sync_async_roundtrip("openTrades")
        return []


def _config(tmp_path: Path) -> OptionsCopilotConfig:
    return OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )


def _owner_threads(client_id: int) -> tuple[threading.Thread, ...]:
    prefix = f"options-copilot-ibkr-{client_id}"
    return tuple(
        thread
        for thread in threading.enumerate()
        if thread.is_alive() and thread.name.startswith(prefix)
    )


def _assert_one_owner(
    events: list[_CallIdentity],
    *,
    caller_thread: int,
    caller_loop: asyncio.AbstractEventLoop,
) -> None:
    assert events
    assert {item.thread_id for item in events}.isdisjoint({caller_thread})
    assert {id(item.loop) for item in events}.isdisjoint({id(caller_loop)})
    assert len({item.thread_id for item in events}) == 1
    assert len({id(item.loop) for item in events}) == 1


def test_running_asgi_loop_uses_one_persistent_gateway_owner_and_reconnects(
    tmp_path: Path,
) -> None:
    clients: list[_AsyncSensitiveIB] = []

    def factory() -> _AsyncSensitiveIB:
        client = _AsyncSensitiveIB([])
        clients.append(client)
        return client

    gateway = IBKRReadOnlyGateway(_config(tmp_path), ib_factory=factory)

    async def exercise() -> tuple[int, asyncio.AbstractEventLoop]:
        caller_thread = threading.get_ident()
        caller_loop = asyncio.get_running_loop()

        gateway.connect()
        assert gateway.account_snapshot().net_liquidation > 0
        assert gateway.positions() == ()
        assert gateway.working_orders() == ()
        gateway.disconnect()

        # Explicit reconnect is allowed, but it must create a fresh owner loop
        # after the prior owner thread has exited cleanly.
        gateway.connect()
        assert gateway.account_snapshot().net_liquidation > 0
        gateway.disconnect()
        return caller_thread, caller_loop

    caller_thread, caller_loop = asyncio.run(exercise())

    assert len(clients) == 2
    for client in clients:
        _assert_one_owner(
            client.events,
            caller_thread=caller_thread,
            caller_loop=caller_loop,
        )
        assert client.events[-1].operation == "disconnect"
        assert client.events[0].loop.is_closed()
        owner_thread = client.events[0].thread_id
        assert owner_thread not in {
            thread.ident for thread in threading.enumerate() if thread.is_alive()
        }
    assert clients[0].events[0].loop is not clients[1].events[0].loop


def test_gateway_worker_propagates_call_exception_and_still_closes(
    tmp_path: Path,
) -> None:
    clients: list[_AsyncSensitiveIB] = []

    def factory() -> _AsyncSensitiveIB:
        client = _AsyncSensitiveIB([], fail_account=True)
        clients.append(client)
        return client

    gateway = IBKRReadOnlyGateway(_config(tmp_path), ib_factory=factory)

    async def exercise() -> tuple[int, asyncio.AbstractEventLoop]:
        caller_thread = threading.get_ident()
        caller_loop = asyncio.get_running_loop()
        gateway.connect()
        try:
            with pytest.raises(
                BrokerControlAuthorityError,
                match="CONTROL_RESPONSE_INVALID",
            ):
                gateway.account_snapshot()
        finally:
            gateway.disconnect()
        return caller_thread, caller_loop

    caller_thread, caller_loop = asyncio.run(exercise())

    assert len(clients) == 1
    client = clients[0]
    _assert_one_owner(
        client.events,
        caller_thread=caller_thread,
        caller_loop=caller_loop,
    )
    assert [item.operation for item in client.events] == [
        "factory",
        "connect",
        "portfolio",
        "accountSummary",
        "disconnect",
    ]
    assert client.events[0].loop.is_closed()
    owner_thread = client.events[0].thread_id
    assert owner_thread not in {
        thread.ident for thread in threading.enumerate() if thread.is_alive()
    }


def test_unconnected_read_fails_without_creating_an_owner_thread(
    tmp_path: Path,
) -> None:
    factory_calls = 0

    def factory() -> _AsyncSensitiveIB:
        nonlocal factory_calls
        factory_calls += 1
        return _AsyncSensitiveIB([])

    config = _config(tmp_path)
    gateway = IBKRReadOnlyGateway(config, ib_factory=factory)

    try:
        with pytest.raises(
            BrokerConnectionError,
            match="IBKR read-only gateway is not connected",
        ):
            gateway.account_snapshot()

        assert factory_calls == 0
        assert _owner_threads(config.ibkr_client_id) == ()
    finally:
        # Current failing implementations may create the owner before checking
        # connection state.  Always clean it up so the regression test itself
        # cannot leak a worker into the remaining suite.
        gateway.disconnect()


def test_disconnect_failure_preserves_live_client_and_owner_for_retry(
    tmp_path: Path,
) -> None:
    clients: list[_AsyncSensitiveIB] = []

    def factory() -> _AsyncSensitiveIB:
        client = _AsyncSensitiveIB([], disconnect_failures=1)
        clients.append(client)
        return client

    config = _config(tmp_path)
    gateway = IBKRReadOnlyGateway(config, ib_factory=factory)
    gateway.connect()
    client = clients[0]

    try:
        with pytest.raises(
            _AsyncSensitiveFailure,
            match="sanitized disconnect failure",
        ):
            gateway.disconnect()

        assert client.connected is True
        assert gateway.connected is True
        assert len(_owner_threads(config.ibkr_client_id)) == 1

        gateway.disconnect()
        assert client.connected is False
        assert gateway.connected is False
        assert _owner_threads(config.ibkr_client_id) == ()
    finally:
        client.disconnect_failures = 0
        try:
            gateway.disconnect()
        finally:
            # The assertion path on the broken implementation has already lost
            # its client reference; reset only the fixture's local state.
            client.connected = False


def test_blocked_broker_call_times_out_but_owner_remains_live(
    tmp_path: Path,
) -> None:
    clients: list[_AsyncSensitiveIB] = []

    def factory() -> _AsyncSensitiveIB:
        client = _AsyncSensitiveIB([], block_account_once=True)
        clients.append(client)
        return client

    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        ibkr_timeout_seconds=0.05,
    )
    gateway = IBKRReadOnlyGateway(config, ib_factory=factory)
    gateway.connect()
    client = clients[0]
    release_timer = threading.Timer(0.25, client.account_release.set)
    release_timer.daemon = True
    release_timer.start()

    try:
        started = time.perf_counter()
        with pytest.raises(
            (BrokerConnectionError, TimeoutError),
            match="timed out|timeout",
        ):
            gateway.account_snapshot()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.20
        assert client.account_entered.is_set()
        assert len(_owner_threads(config.ibkr_client_id)) == 1

        assert client.account_finished.wait(timeout=1.0)
        assert gateway.connected is True
        # The owner remains alive, but an uncertain no-request-id enumeration
        # cannot be retried in this SDK session or consume delayed responses.
        with pytest.raises(BrokerControlAuthorityError, match="CONTROL_SYNC_TIMEOUT"):
            gateway.account_snapshot()
        assert gateway.upstream_health()["status"] == "RECONNECT_REQUIRED"
        account_calls = [
            event for event in client.events if event.operation == "accountSummary"
        ]
        assert len(account_calls) == 1
        assert len({event.thread_id for event in account_calls}) == 1
        assert len({id(event.loop) for event in account_calls}) == 1
    finally:
        client.account_release.set()
        release_timer.cancel()
        release_timer.join(timeout=1.0)
        if client.account_entered.is_set():
            client.account_finished.wait(timeout=1.0)
        gateway.disconnect()
