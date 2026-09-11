"""Fail-closed read-only TWS/IB Gateway adapter for US equity options.

This module deliberately contains no order-placement method.  Creation of a
reviewable IBKR instruction is performed by the managed connector only after a
short-lived GUI approval handoff.
"""
from __future__ import annotations

import asyncio
from copy import copy
import importlib.util
import json
import math
import queue
import socket
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, ExitStack, contextmanager, nullcontext
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from functools import wraps
from typing import Callable, Iterable, Literal, Sequence
from zoneinfo import ZoneInfo

from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway.control_authority import (
    ControlAuthority,
    ControlAuthorityBatch,
    ControlAuthorityError,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json, utc_datetime


class BrokerConnectionError(RuntimeError):
    pass


class BrokerControlAuthorityError(BrokerConnectionError):
    """A real broker control batch is unavailable or no longer authoritative."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class SessionCalendarReadError(BrokerConnectionError):
    """Expose a bounded calendar failure without broker exception text."""

    def __init__(self, reason_code: str) -> None:
        if reason_code not in {
            "CALENDAR_BROKER_REQUEST_TIMEOUT",
            "CALENDAR_BROKER_REQUEST_FAILED",
            "CALENDAR_CONTRACT_DETAILS_UNAVAILABLE",
            "CALENDAR_CONTRACT_DETAILS_AMBIGUOUS",
            "CALENDAR_CONTRACT_DETAILS_INCOMPLETE",
            "CALENDAR_CONTRACT_IDENTITY_MISMATCH",
        }:
            raise ValueError("unsupported session calendar failure")
        self.reason_code = reason_code
        super().__init__(reason_code)


class MarketDataPacingError(BrokerConnectionError):
    """A broker wire request was refused before transmission by pacing."""

    def __init__(self, request_class: str, reason: object) -> None:
        checked_class = str(request_class).strip().lower()
        checked_reason = str(reason or "PACING_DENIED").strip().upper()
        if not checked_class:
            checked_class = "unknown"
        if checked_reason not in {
            "PACING_AUTHORIZATION_FAILED",
            "PACING_CAPABILITY_MISSING",
            "PACING_CONCURRENCY_LIMIT",
            "PACING_COOLDOWN_ACTIVE",
            "PACING_REQUEST_WINDOW_EXHAUSTED",
            "PACING_DENIED",
        }:
            checked_reason = "PACING_DENIED"
        self.request_class = checked_class
        self.reason_code = checked_reason
        super().__init__(
            f"IBKR {checked_class} pacing refused the wire request: "
            f"{checked_reason}"
        )


class OptionQualificationError(BrokerConnectionError):
    """Structured fail-closed result for one paced SecDef wire failure."""

    def __init__(
        self,
        reason_code: str,
        *,
        symbol: str,
        expiration: date,
        requested_count: int,
        completed_count: int,
        failed_right: str | None = None,
        failed_strike: Decimal | None = None,
    ) -> None:
        checked_reason = str(reason_code).strip().upper()
        if checked_reason not in {
            "OPTION_QUALIFICATION_BROKER_FAILED",
            "OPTION_QUALIFICATION_CANCELLED",
            "OPTION_QUALIFICATION_DEADLINE_EXPIRED",
            "OPTION_QUALIFICATION_TIMEOUT",
        }:
            checked_reason = "OPTION_QUALIFICATION_BROKER_FAILED"
        self.reason_code = checked_reason
        self.symbol = str(symbol).strip().upper()
        self.expiration = expiration
        self.requested_count = max(int(requested_count), 0)
        self.completed_count = max(int(completed_count), 0)
        self.failed_right = (
            str(failed_right).strip().upper() if failed_right else None
        )
        self.failed_strike = failed_strike
        failed_identity = (
            f" {self.failed_right} {self.failed_strike}"
            if self.failed_right and self.failed_strike is not None
            else ""
        )
        super().__init__(
            f"{checked_reason}: {self.symbol} {self.expiration.isoformat()}"
            f"{failed_identity}; completed {self.completed_count}/"
            f"{self.requested_count} SecDef requests"
        )


class _HistoricalDeadlineExpired(TimeoutError):
    """The aggregate owner deadline expired before a broker request began."""


class QuoteBatchStatus(str, Enum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"


class GatewayReadinessStatus(str, Enum):
    """Bounded listener-probe result; never an authenticated broker status."""

    LISTENER_READY = "LISTENER_READY"
    UNAVAILABLE = "UNAVAILABLE"
    MISCONFIGURED = "MISCONFIGURED"


@dataclass(frozen=True, slots=True)
class IBKRGatewayReadiness:
    """Non-authenticating readiness evidence for a user-operated Gateway."""

    checked_at: datetime
    host: str
    port: int
    client_id: int
    readonly: bool
    listener_reachable: bool
    client_library_available: bool
    status: GatewayReadinessStatus
    blockers: tuple[str, ...]
    scope: str = "TCP_LISTENER_AND_CLIENT_LIBRARY_ONLY"
    authenticated: bool = False
    market_data_verified: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
            "readonly": self.readonly,
            "listener_reachable": self.listener_reachable,
            "client_library_available": self.client_library_available,
            "status": self.status.value,
            "blockers": list(self.blockers),
            "scope": self.scope,
            "authenticated": self.authenticated,
            "market_data_verified": self.market_data_verified,
        }


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    asof: datetime
    currency: str
    net_liquidation: Decimal
    equity_with_loan_value: Decimal
    available_funds: Decimal
    buying_power: Decimal
    initial_margin: Decimal
    maintenance_margin: Decimal
    excess_liquidity: Decimal
    day_trades_remaining: int | None
    connected: bool = True


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    asof: datetime
    contract_id: int
    symbol: str
    local_symbol: str
    security_type: str
    currency: str
    exchange: str
    quantity: Decimal
    average_cost: Decimal
    market_price: Decimal | None
    market_value: Decimal | None
    unrealized_pnl: Decimal | None
    realized_pnl: Decimal | None
    expiration: date | None = None
    strike: Decimal | None = None
    right: Literal["C", "P"] | None = None
    trading_class: str | None = None
    multiplier: int | None = None


@dataclass(frozen=True, slots=True)
class UnderlyingIvHistoryPoint:
    """One completed daily IBKR underlying-IV observation."""

    trading_date: date
    close: Decimal


@dataclass(frozen=True, slots=True)
class UnderlyingIvHistory:
    """Hash-bound historical IV series for one qualified stock underlying."""

    symbol: str
    contract_id: int
    request_exchange: str
    currency: str
    observed_at: datetime
    end_at: datetime
    duration: str
    bar_size: str
    what_to_show: str
    use_rth: bool
    points: tuple[UnderlyingIvHistoryPoint, ...]
    basis_hash: str
    content_hash: str

    def hash_payload(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "contract_id": self.contract_id,
            "request_exchange": self.request_exchange,
            "currency": self.currency,
            "observed_at": self.observed_at,
            "end_at": self.end_at,
            "duration": self.duration,
            "bar_size": self.bar_size,
            "what_to_show": self.what_to_show,
            "use_rth": self.use_rth,
            "points": tuple(
                {"trading_date": item.trading_date, "close": item.close}
                for item in self.points
            ),
            "basis_hash": self.basis_hash,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.content_hash


@dataclass(frozen=True, slots=True)
class OptionExpiration:
    expiration: date
    trading_class: str
    exchange: str
    multiplier: int
    strikes: tuple[Decimal, ...]
    regular: bool | None = None

    @property
    def dte(self) -> int:
        return (self.expiration - datetime.now(timezone.utc).date()).days


@dataclass(frozen=True, slots=True)
class OptionContractRef:
    contract_id: int
    contract_id_ex: str
    symbol: str
    local_symbol: str
    expiration: date
    strike: Decimal
    right: Literal["C", "P"]
    exchange: str
    trading_class: str
    multiplier: int
    currency: str = "USD"


@dataclass(frozen=True, slots=True)
class OptionSecDefSnapshot:
    """Read-only standard-option contract identity evidence."""

    contract_id: int
    local_symbol: str
    trading_class: str
    multiplier: int
    exchange: str
    expiration: date
    strike: Decimal
    right: Literal["C", "P"]
    security_type: str
    currency: str
    standard_contract: bool
    adjusted: bool
    source: str

    def identity_dict(self) -> dict[str, object]:
        """Return exactly the eight fields locked by the P1 identity contract."""

        return {
            "conId": self.contract_id,
            "localSymbol": self.local_symbol,
            "tradingClass": self.trading_class,
            "multiplier": self.multiplier,
            "exchange": self.exchange,
            "expiry": self.expiration,
            "strike": self.strike,
            "right": self.right,
        }


@dataclass(frozen=True, slots=True)
class BatchedOptionQuote:
    contract_id: int
    batch_id: str
    request_id: str
    requested_at: datetime
    observed_at: datetime
    completed_at: datetime
    source: str
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None = None
    close: Decimal | None = None
    exchange_time: datetime | None = None
    volume: int | None = None
    open_interest: int | None = None
    implied_volatility: Decimal | None = None
    delta: Decimal | None = None
    gamma: Decimal | None = None
    theta: Decimal | None = None
    vega: Decimal | None = None
    market_data_type: int | None = None
    research_price_basis: str | None = None


@dataclass(frozen=True, slots=True)
class OptionMarketDataRequestDiagnostic:
    """Observation-only evidence for one temporary IBKR option subscription."""

    contract_id: int
    broker_request_id: int | None
    transport: str
    generic_ticks: tuple[int, ...]
    received_fields: tuple[str, ...]
    missing_fields: tuple[str, ...]
    error_codes: tuple[int, ...]
    deadline_expired: bool
    timeout_reason: str | None
    broker_timed_bbo_request_id: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "generic_ticks", tuple(self.generic_ticks))
        object.__setattr__(self, "received_fields", tuple(self.received_fields))
        object.__setattr__(self, "missing_fields", tuple(self.missing_fields))
        object.__setattr__(self, "error_codes", tuple(self.error_codes))


@dataclass(frozen=True, slots=True)
class OptionQuoteBatch:
    batch_id: str
    status: QuoteBatchStatus
    requested_at: datetime
    completed_at: datetime
    source: str
    quotes: tuple[BatchedOptionQuote, ...]
    observed_at: datetime | None = None
    blockers: tuple[str, ...] = ()
    request_diagnostics: tuple[OptionMarketDataRequestDiagnostic, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.status, QuoteBatchStatus):
            object.__setattr__(self, "status", QuoteBatchStatus(str(self.status)))
        object.__setattr__(self, "quotes", tuple(self.quotes))
        object.__setattr__(self, "blockers", tuple(self.blockers))
        object.__setattr__(
            self,
            "request_diagnostics",
            tuple(self.request_diagnostics),
        )


@dataclass(frozen=True, slots=True)
class OptionQuoteSnapshot:
    contract: OptionContractRef
    observed_at: datetime
    exchange_time: datetime | None
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    close: Decimal | None
    volume: int | None
    open_interest: int | None
    implied_volatility: Decimal | None
    delta: Decimal | None
    gamma: Decimal | None
    theta: Decimal | None
    vega: Decimal | None
    market_data_type: int | None

    @property
    def midpoint(self) -> Decimal | None:
        if self.bid is None or self.ask is None or self.ask < self.bid:
            return None
        return (self.bid + self.ask) / Decimal("2")

    @property
    def has_executable_market(self) -> bool:
        return self.bid is not None and self.ask is not None and self.ask >= self.bid

    @property
    def is_delayed(self) -> bool:
        return self.market_data_type in {3, 4}


@dataclass(frozen=True, slots=True)
class UnderlyingScanResult:
    rank: int
    symbol: str
    contract_id: int
    exchange: str
    source_scan: str
    industry: str | None = None
    category: str | None = None
    subcategory: str | None = None


@dataclass(frozen=True, slots=True)
class UnderlyingQuoteSnapshot:
    """One read-only IBKR quote used only to select nearby option strikes."""

    symbol: str
    contract_id: int
    exchange: str
    observed_at: datetime
    source: str
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    close: Decimal | None
    volume: int | None
    market_data_type: int | None

    @property
    def market_price(self) -> Decimal | None:
        if self.bid is not None and self.ask is not None and self.ask >= self.bid:
            return (self.bid + self.ask) / Decimal("2")
        return self.last if self.last is not None else self.close


@dataclass(frozen=True, slots=True)
class OptionsSessionHours:
    """Raw broker-published US-options session evidence for calendar normalization."""

    symbol: str
    contract_id: int
    observed_at: datetime
    source: str
    liquid_hours: str
    trading_hours: str
    timezone_id: str


def probe_ibkr_gateway_readiness(
    config: OptionsCopilotConfig,
    *,
    socket_connector: Callable[[tuple[str, int], float], object] | None = None,
    dependency_check: Callable[[], bool] | None = None,
    now: Callable[[], datetime] | None = None,
    timeout_seconds: float = 1.0,
) -> IBKRGatewayReadiness:
    """Probe only the configured TCP listener and local client dependency.

    The probe deliberately does not instantiate ``IB``, perform an API
    handshake, authenticate, request account data, or start/stop Gateway.  A
    ``LISTENER_READY`` result therefore proves only that a TCP listener and the
    local client library are available; authenticated market-data readiness
    must still be established by the normal fail-closed acquisition flow.
    """

    checked_at = (now or (lambda: datetime.now(timezone.utc)))()
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("readiness probe time must be timezone-aware")
    connector = socket_connector or socket.create_connection
    has_dependency = dependency_check or (
        lambda: importlib.util.find_spec("ib_insync") is not None
    )
    blockers: list[str] = []
    host = str(config.ibkr_host or "").strip()
    port = config.ibkr_port
    client_id = config.ibkr_client_id
    readonly = config.ibkr_readonly is True
    if not host:
        blockers.append("IBKR_GATEWAY_HOST_MISSING")
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        blockers.append("IBKR_GATEWAY_PORT_INVALID")
    if (
        not isinstance(client_id, int)
        or isinstance(client_id, bool)
        or client_id <= 0
    ):
        blockers.append("IBKR_GATEWAY_CLIENT_ID_INVALID")
    if not readonly:
        blockers.append("IBKR_GATEWAY_READONLY_POLICY_DISABLED")

    try:
        client_library_available = bool(has_dependency())
    except Exception:
        client_library_available = False
    if not client_library_available:
        blockers.append("IB_INSYNC_CLIENT_UNAVAILABLE")

    misconfigured = any(
        item
        in {
            "IBKR_GATEWAY_HOST_MISSING",
            "IBKR_GATEWAY_PORT_INVALID",
            "IBKR_GATEWAY_CLIENT_ID_INVALID",
            "IBKR_GATEWAY_READONLY_POLICY_DISABLED",
        }
        for item in blockers
    )
    listener_reachable = False
    if not misconfigured:
        connection: object | None = None
        try:
            bounded_timeout = min(max(float(timeout_seconds), 0.05), 5.0)
            connection = connector((host, port), bounded_timeout)
            listener_reachable = True
        except (OSError, TimeoutError):
            blockers.append("IBKR_GATEWAY_LISTENER_UNREACHABLE")
        except Exception:
            blockers.append("IBKR_GATEWAY_LISTENER_PROBE_FAILED")
        finally:
            close = getattr(connection, "close", None)
            if callable(close):
                close()

    if misconfigured:
        status = GatewayReadinessStatus.MISCONFIGURED
    elif blockers:
        status = GatewayReadinessStatus.UNAVAILABLE
    else:
        status = GatewayReadinessStatus.LISTENER_READY
    return IBKRGatewayReadiness(
        checked_at=checked_at,
        host=host,
        port=port,
        client_id=client_id,
        readonly=readonly,
        listener_reachable=listener_reachable,
        client_library_available=client_library_available,
        status=status,
        blockers=tuple(blockers),
    )


class _GatewayOwnerThread:
    """Own one persistent thread and asyncio loop for an IB client lifetime."""

    def __init__(self, *, name: str) -> None:
        self._queue: queue.Queue[
            tuple[Callable[[], object], Future[object]] | None
        ] = queue.Queue()
        self._state_lock = threading.Lock()
        self._thread_id: int | None = None
        self._state = "RUNNING"
        self._close_error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=name,
            daemon=True,
        )
        self._thread.start()

    @property
    def is_current(self) -> bool:
        return self._thread_id == threading.get_ident()

    @property
    def state(self) -> str:
        with self._state_lock:
            return self._state

    def call(
        self,
        operation: Callable[[], object],
        *,
        timeout_seconds: float,
    ) -> object:
        if self.is_current:
            return operation()
        with self._state_lock:
            if self._state != "RUNNING":
                raise RuntimeError(
                    f"IBKR gateway owner thread is {self._state.lower()}"
                )
            future: Future[object] = Future()
            self._queue.put((operation, future))
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError as exc:
            raise TimeoutError("IBKR gateway owner call timed out") from exc

    def close(self, *, timeout_seconds: float) -> bool:
        if self.is_current:
            raise RuntimeError("IBKR gateway owner cannot join itself")
        with self._state_lock:
            if self._state == "CLOSED":
                return True
            if self._state == "RUNNING":
                self._state = "CLOSING"
                self._queue.put(None)
        self._thread.join(timeout=max(float(timeout_seconds), 0.0))
        with self._state_lock:
            return self._state == "CLOSED" and not self._thread.is_alive()

    def _run(self) -> None:
        self._thread_id = threading.get_ident()
        _ensure_ib_event_loop()
        try:
            while True:
                item = self._queue.get()
                if item is None:
                    break
                operation, future = item
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    result = operation()
                except BaseException as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)
                finally:
                    # Do not let the idle worker's frame retain the completed
                    # closure (and therefore the gateway) between requests.
                    del item, operation, future
        finally:
            try:
                self._close_event_loop()
            except BaseException as exc:
                with self._state_lock:
                    self._close_error = exc
                    self._state = "CLOSE_FAILED"
            else:
                with self._state_lock:
                    self._state = "CLOSED"

    @staticmethod
    def _close_event_loop() -> None:
        try:
            loop = asyncio.get_event_loop_policy().get_event_loop()
        except RuntimeError:
            return
        if loop.is_running():
            raise RuntimeError("IBKR gateway owner loop is still running")
        pending = tuple(task for task in asyncio.all_tasks(loop) if not task.done())
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        loop.close()
        asyncio.set_event_loop(None)


def _gateway_owner_call(method: Callable[..., object]) -> Callable[..., object]:
    """Marshal one synchronous gateway method to its persistent owner."""

    @wraps(method)
    def wrapped(self: "IBKRReadOnlyGateway", *args: object, **kwargs: object) -> object:
        return self._call_on_owner(lambda: method(self, *args, **kwargs))

    return wrapped


class IBKRReadOnlyGateway:
    """Synchronous read-only gateway with a dedicated persistent IB loop."""

    def __init__(
        self,
        config: OptionsCopilotConfig,
        *,
        ib_factory: Callable[[], object] | None = None,
        pacing_observer: Callable[[object], Mapping[str, object] | None] | None = None,
        historical_request_lease_factory: Callable[
            [], AbstractContextManager[object]
        ]
        | None = None,
        historical_request_max_concurrency: int = 1,
        market_data_request_lease_factory: Callable[
            [str], AbstractContextManager[object]
        ]
        | None = None,
        instruction_reader: Callable[[], Sequence[Mapping[str, object]] | None]
        | None = None,
        batch_id_factory: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.config = config
        self._ib_factory = ib_factory or _default_ib_factory
        self._pacing_observer = pacing_observer
        self._historical_request_lease_factory = historical_request_lease_factory
        if (
            isinstance(historical_request_max_concurrency, bool)
            or not isinstance(historical_request_max_concurrency, int)
            or not 1 <= historical_request_max_concurrency <= 50
        ):
            raise ValueError(
                "historical_request_max_concurrency must be between 1 and 50"
            )
        self._historical_request_max_concurrency = (
            historical_request_max_concurrency
        )
        self._market_data_request_lease_factory = (
            market_data_request_lease_factory
        )
        self._instruction_reader = instruction_reader
        self._batch_id_factory = batch_id_factory or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic or time.monotonic
        self._ib: object | None = None
        # One atomic pointer keeps cache-only health independent of the owner
        # queue and preserves epoch monotonicity across SDK instance replacement.
        self._control_state: tuple[ControlAuthority | None, int] = (None, 0)
        self._lock = threading.RLock()
        self._owner_gate = threading.RLock()
        self._owner: _GatewayOwnerThread | None = None
        self._underlying_identity_cache_day: date | None = None
        self._underlying_identity_cache: dict[str, object] = {}

    @property
    def market_data_pacing_enabled(self) -> bool:
        return self._market_data_request_lease_factory is not None

    def _market_data_lease(
        self,
        request_class: str,
    ) -> AbstractContextManager[object | None]:
        factory = self._market_data_request_lease_factory
        if factory is None:
            return nullcontext(None)
        try:
            return factory(request_class)
        except Exception as exc:
            raise MarketDataPacingError(
                request_class,
                "PACING_AUTHORIZATION_FAILED",
            ) from exc

    @staticmethod
    def _require_market_data_lease(
        decision: object | None,
        request_class: str,
    ) -> None:
        if decision is None:
            return
        if not bool(getattr(decision, "allowed", False)):
            raise MarketDataPacingError(
                request_class,
                getattr(decision, "reason", None),
            )

    def _request_tickers(
        self,
        ib: object,
        contracts: Sequence[object],
    ) -> tuple[object, ...]:
        """Pace the SDK's per-contract ``reqMktData`` fan-out exactly."""

        if not self.market_data_pacing_enabled:
            return tuple(ib.reqTickers(*contracts))  # type: ignore[attr-defined]

        # reqTickers fans out one market-data request per contract.  Reserve
        # those requests individually, then let ib-insync await the complete
        # batch concurrently within the single owner-thread deadline.
        with ExitStack() as leases:
            for _contract in contracts:
                decision = leases.enter_context(
                    self._market_data_lease("snapshot_quote")
                )
                self._require_market_data_lease(decision, "snapshot_quote")
            return tuple(  # type: ignore[attr-defined]
                ib.reqTickers(*contracts)
            )

    def _request_streaming_option_tickers(
        self,
        ib: object,
        contracts: Sequence[object],
        *,
        deadline: float,
    ) -> tuple[
        tuple[object, ...],
        tuple[str, ...],
        tuple[OptionMarketDataRequestDiagnostic, ...],
        dict[int, tuple[datetime, Decimal, Decimal]],
    ]:
        """Read option fields and broker-timed BBOs in one streaming window."""

        requested: list[tuple[object, object]] = []
        market_data_request_ids: dict[int, int] = {}
        request_contracts: dict[int, int] = {}
        raw_errors: list[tuple[int | None, int | None, int]] = []

        def on_error(
            req_id: object,
            error_code: object,
            _error_string: object,
            contract: object | None = None,
        ) -> None:
            code = _integer(error_code)
            if code is None or code in {2104, 2106, 2107, 2108, 2119, 2158}:
                return
            request_id = _integer(req_id)
            contract_id = _integer(getattr(contract, "conId", None))
            raw_errors.append((request_id, contract_id, code))

        error_event = getattr(ib, "errorEvent", None)
        handler_attached = False
        if error_event is not None:
            try:
                error_event += on_error
                handler_attached = True
            except Exception:
                handler_attached = False
        try:
            with ExitStack() as leases:
                for contract in contracts:
                    decision = leases.enter_context(
                        self._market_data_lease("streaming_quote")
                    )
                    self._require_market_data_lease(decision, "streaming_quote")
                    ticker = ib.reqMktData(  # type: ignore[attr-defined]
                        contract,
                        "100,101,106",
                        False,
                        False,
                    )
                    requested.append((contract, ticker))
                    request_id = _ticker_market_data_request_id(ib, ticker)
                    contract_id = _integer(getattr(contract, "conId", None))
                    if request_id is not None and contract_id is not None:
                        request_contracts[request_id] = contract_id
                        market_data_request_ids[contract_id] = request_id
                while self._monotonic() < deadline:
                    if all(
                        _streaming_option_ticker_ready(ticker, contract)
                        for contract, ticker in requested
                    ):
                        break
                    remaining = deadline - self._monotonic()
                    if remaining <= 0:
                        break
                    ib.sleep(min(0.05, remaining))  # type: ignore[attr-defined]
        finally:
            for contract, _ticker in requested:
                try:
                    ib.cancelMktData(contract)  # type: ignore[attr-defined]
                except Exception:
                    pass
            if handler_attached:
                try:
                    error_event -= on_error
                except Exception:
                    pass
        errors_by_contract: dict[int, list[int]] = {}
        api_blockers: list[str] = []
        for request_id, event_contract_id, code in raw_errors:
            contract_id = event_contract_id
            if contract_id is None and request_id is not None:
                contract_id = request_contracts.get(request_id)
            identity = contract_id if contract_id is not None else request_id
            api_blockers.append(
                _ibkr_market_data_error_blocker(code, identity)
            )
            if contract_id is not None:
                errors_by_contract.setdefault(contract_id, []).append(code)

        request_diagnostics: list[OptionMarketDataRequestDiagnostic] = []
        for contract, ticker in requested:
            contract_id = _integer(getattr(contract, "conId", None))
            if contract_id is None:
                continue
            received_fields = _received_option_ticker_fields(
                ticker,
                contract,
            )
            missing_fields = tuple(
                field
                for field in _OPTION_MARKET_DATA_DIAGNOSTIC_FIELDS
                if field not in received_fields
            )
            error_codes = tuple(
                dict.fromkeys(errors_by_contract.get(contract_id, ()))
            )
            deadline_expired = not _streaming_option_ticker_ready(ticker, contract)
            timeout_reason = None
            if deadline_expired:
                timeout_reason = (
                    "STREAMING_REQUIRED_FIELDS_DEADLINE_EXPIRED_AFTER_API_ERROR"
                    if error_codes
                    else "STREAMING_REQUIRED_FIELDS_DEADLINE_EXPIRED"
                )
            request_diagnostics.append(
                OptionMarketDataRequestDiagnostic(
                    contract_id=contract_id,
                    broker_request_id=market_data_request_ids.get(contract_id),
                    transport="REQ_MKT_DATA_STREAMING",
                    generic_ticks=(100, 101, 106),
                    received_fields=received_fields,
                    missing_fields=missing_fields,
                    error_codes=error_codes,
                    deadline_expired=deadline_expired,
                    timeout_reason=timeout_reason,
                    broker_timed_bbo_request_id=None,
                )
            )
        return (
            tuple(ticker for _contract, ticker in requested),
            tuple(dict.fromkeys(api_blockers)),
            tuple(request_diagnostics),
            {},
        )

    def __enter__(self) -> "IBKRReadOnlyGateway":
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.disconnect()

    def __del__(self) -> None:
        # Deterministic contexts and production lifecycle call disconnect()
        # explicitly.  This bounded fallback prevents abandoned test/probe
        # gateways from retaining a daemon owner thread until process exit.
        try:
            self.disconnect()
        except BaseException:
            pass

    @property
    def connected(self) -> bool:
        if self._owner is None:
            return False
        try:
            return bool(self._call_on_owner(self._connected_on_owner))
        except (RuntimeError, TimeoutError):
            return False

    def _connected_on_owner(self) -> bool:
        with self._lock:
            return bool(self._ib is not None and self._ib.isConnected())  # type: ignore[attr-defined]

    def upstream_health(self) -> dict[str, object]:
        """Read upstream/control authority without waiting for the SDK owner."""

        authority, generation_offset = self._control_state
        if authority is None:
            return {
                "status": "DISCONNECTED",
                "generation": generation_offset,
                "verified_at": None,
                "reason_codes": ["IBKR_CONTROL_DISCONNECTED"],
            }
        health = dict(authority.health())
        health["generation"] = generation_offset + int(health["generation"])
        return health

    def connect(self) -> None:
        try:
            self._call_on_owner(self._connect_on_owner, create=True)
        except TimeoutError:
            # The owner may still be completing the timed-out call.  Retain it
            # and its client reference so a later diagnostic/close can observe
            # the true state rather than manufacturing a clean shutdown.
            raise
        except BaseException:
            self._close_disconnected_owner()
            raise

    def _connect_on_owner(self) -> None:
        with self._lock:
            if self._ib is not None and self._ib.isConnected():  # type: ignore[attr-defined]
                return
            if self.config.ibkr_readonly is not True:
                raise BrokerConnectionError(
                    "IBKR Gateway connection refused: read-only policy is disabled"
                )
            ib = self._ib_factory()
            prior_authority, prior_offset = self._control_state
            next_offset = prior_offset
            if prior_authority is not None:
                prior_authority.close()
                next_offset += int(prior_authority.health()["generation"]) + 1
            authority: ControlAuthority | None = None
            try:
                authority = ControlAuthority(
                    ib,
                    clock=self._aware_now,
                    monotonic=self._monotonic,
                )
                self._control_state = (authority, next_offset)
                # ib_insync defaults RequestTimeout to zero (unbounded).  Every
                # synchronous broker request must instead inherit our bounded
                # read-only timeout.
                # Keep the SDK timeout inside the owner-thread deadline so
                # ib_insync can cancel and clean up a slow request before the
                # caller gives up on the owner future.
                setattr(
                    ib,
                    "RequestTimeout",
                    max(self.config.ibkr_timeout_seconds - 1.0, 0.01),
                )
                ib.connect(  # type: ignore[attr-defined]
                    self.config.ibkr_host,
                    self.config.ibkr_port,
                    clientId=self.config.ibkr_client_id,
                    readonly=True,
                    timeout=self.config.ibkr_timeout_seconds,
                )
                if not ib.isConnected():  # type: ignore[attr-defined]
                    raise BrokerConnectionError(
                        "IBKR read-only gateway did not become connected"
                    )
            except Exception as exc:
                try:
                    if ib.isConnected():  # type: ignore[attr-defined]
                        ib.disconnect()  # type: ignore[attr-defined]
                except Exception:
                    pass
                if authority is not None:
                    authority.close()
                if isinstance(exc, BrokerConnectionError):
                    raise
                raise BrokerConnectionError(
                    "unable to connect to IBKR read-only gateway"
                ) from exc
            self._ib = ib
            self._clear_underlying_identity_cache()

    def disconnect(self) -> None:
        owner = self._owner
        if owner is None:
            return
        if owner.is_current:
            self._disconnect_on_owner()
            return
        with self._owner_gate:
            owner = self._owner
            if owner is None:
                return
            if owner.state != "RUNNING":
                if owner.close(timeout_seconds=self._owner_timeout_seconds):
                    if self._owner is owner:
                        self._owner = None
                    return
                raise TimeoutError(
                    f"IBKR gateway owner close did not complete: {owner.state}"
                )
            owner.call(
                self._disconnect_on_owner,
                timeout_seconds=self._owner_timeout_seconds,
            )
            if not owner.close(timeout_seconds=self._owner_timeout_seconds):
                raise TimeoutError(
                    f"IBKR gateway owner close did not complete: {owner.state}"
                )
            if self._owner is owner:
                self._owner = None

    def _disconnect_on_owner(self) -> None:
        with self._lock:
            ib = self._ib
            if ib is not None and ib.isConnected():  # type: ignore[attr-defined]
                ib.disconnect()  # type: ignore[attr-defined]
                if ib.isConnected():  # type: ignore[attr-defined]
                    raise BrokerConnectionError(
                        "IBKR read-only gateway remained connected after disconnect"
                    )
            self._ib = None
            authority, _generation_offset = self._control_state
            if authority is not None:
                authority.close()
            self._clear_underlying_identity_cache()

    @_gateway_owner_call
    def market_data_pacing_observation(self) -> dict[str, object] | None:
        """Return an injected broker observation without inferring any limit."""

        if self._pacing_observer is None:
            return None
        with self._lock:
            raw = self._pacing_observer(self._require_ib())
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise TypeError("pacing observer must return a mapping or null")
        detached = json.loads(canonical_json(raw))
        if not isinstance(detached, dict):
            raise TypeError("pacing observer must return a JSON object")
        return detached

    @_gateway_owner_call
    def account_snapshot(self) -> AccountSnapshot:
        with self._lock:
            batch = self._control_batch_on_owner()
        values: dict[str, tuple[str, str]] = {}
        for row in batch.account_rows:
            values[str(row["tag"])] = (
                str(row["value"]),
                str(row["currency"]),
            )
        currency = values["NetLiquidation"][1]

        def required_amount(tag: str) -> Decimal:
            value = _decimal(values.get(tag, ("", ""))[0])
            if value is None:
                raise BrokerControlAuthorityError("IBKR_CONTROL_ACCOUNT_INCOMPLETE")
            return value

        return AccountSnapshot(
            asof=batch.verified_at,
            currency=currency,
            net_liquidation=required_amount("NetLiquidation"),
            equity_with_loan_value=required_amount("EquityWithLoanValue"),
            available_funds=required_amount("AvailableFunds"),
            buying_power=required_amount("BuyingPower"),
            initial_margin=required_amount("InitMarginReq"),
            maintenance_margin=required_amount("MaintMarginReq"),
            excess_liquidity=required_amount("ExcessLiquidity"),
            day_trades_remaining=_integer(
                values.get("DayTradesRemaining", ("", ""))[0]
            ),
        )

    @_gateway_owner_call
    def positions(self) -> tuple[PositionSnapshot, ...]:
        with self._lock:
            batch = self._control_batch_on_owner()
        snapshots: list[PositionSnapshot] = []
        for item in batch.positions:
            contract = item["contract"]
            snapshots.append(
                PositionSnapshot(
                    asof=batch.verified_at,
                    contract_id=int(getattr(contract, "conId", 0) or 0),
                    symbol=str(getattr(contract, "symbol", "") or ""),
                    local_symbol=str(getattr(contract, "localSymbol", "") or ""),
                    security_type=str(getattr(contract, "secType", "") or ""),
                    currency=str(getattr(contract, "currency", "USD") or "USD"),
                    exchange=str(
                        getattr(contract, "primaryExchange", "")
                        or getattr(contract, "exchange", "")
                        or "SMART"
                    ),
                    quantity=_decimal(item["position"]) or Decimal("0"),
                    average_cost=_decimal(item["avgCost"])
                    or Decimal("0"),
                    # reqPositions has no fresh marks/PnL.  The SDK portfolio
                    # cache must never fill holes in a response-end-bound batch.
                    market_price=None,
                    market_value=None,
                    unrealized_pnl=None,
                    realized_pnl=None,
                    expiration=_ib_date_or_none(
                        getattr(contract, "lastTradeDateOrContractMonth", None)
                    ),
                    strike=_decimal(getattr(contract, "strike", None)),
                    right=(
                        str(getattr(contract, "right", "") or "").upper()
                        if str(getattr(contract, "right", "") or "").upper()
                        in {"C", "P"}
                        else None
                    ),
                    trading_class=(
                        str(getattr(contract, "tradingClass", "") or "").strip()
                        or None
                    ),
                    multiplier=_integer(getattr(contract, "multiplier", None)),
                )
            )
        return tuple(snapshots)

    @_gateway_owner_call
    def working_orders(self) -> tuple[dict[str, object], ...]:
        """Return the current IBKR working-order read model without mutation."""

        with self._lock:
            batch = self._control_batch_on_owner()
        rows: list[dict[str, object]] = []
        for item in batch.working_orders:
            order = item["order"]
            contract = item["contract"]
            status_text = str(item["order_state_status"] or "")
            if status_text.upper() in {
                "FILLED",
                "CANCELLED",
                "APICANCELLED",
                "INACTIVE",
            }:
                continue
            rows.append(
                {
                    "order_id": int(getattr(order, "orderId", 0) or 0),
                    "perm_id": int(getattr(order, "permId", 0) or 0),
                    "client_id": int(getattr(order, "clientId", 0) or 0),
                    "contract_id": int(getattr(contract, "conId", 0) or 0),
                    "local_symbol": str(getattr(contract, "localSymbol", "") or ""),
                    "action": str(getattr(order, "action", "") or "").upper(),
                    "order_type": str(getattr(order, "orderType", "") or ""),
                    "total_quantity": _decimal(getattr(order, "totalQuantity", None)),
                    "limit_price": _market_decimal(getattr(order, "lmtPrice", None)),
                    "aux_price": _market_decimal(getattr(order, "auxPrice", None)),
                    "time_in_force": str(getattr(order, "tif", "") or ""),
                    "status": status_text,
                    "transmit": bool(getattr(order, "transmit", False)),
                }
            )
        return tuple(rows)

    def unsubmitted_instructions(self) -> tuple[dict[str, object], ...] | None:
        """Read managed-connector review instructions, or return UNKNOWN.

        IBKR's open-order feed is not authoritative for review instructions
        that have not yet been submitted to IBKR.  An absent connector reader
        therefore returns ``None`` rather than guessing that the set is empty.
        """

        if self._instruction_reader is None:
            return None
        raw = self._instruction_reader()
        if raw is None:
            return None
        if isinstance(raw, (str, bytes, bytearray, memoryview)) or not isinstance(
            raw, Sequence
        ):
            raise TypeError("instruction reader must return an array or null")
        rows: list[dict[str, object]] = []
        for index, item in enumerate(raw):
            if not isinstance(item, Mapping):
                raise TypeError(f"instruction reader row {index} must be an object")
            rows.append(dict(item))
        return tuple(rows)

    @_gateway_owner_call
    def option_expirations(
        self,
        symbol: str,
        *,
        min_dte: int | None = None,
        max_dte: int | None = None,
    ) -> tuple[OptionExpiration, ...]:
        symbol = _symbol(symbol)
        min_days = self.config.min_dte if min_dte is None else min_dte
        max_days = self.config.normal_max_dte if max_dte is None else max_dte
        if min_days < self.config.min_dte or max_days < min_days:
            raise ValueError("invalid or prohibited DTE range")
        _ensure_ib_event_loop()
        from ib_insync import Stock

        with self._lock:
            ib = self._require_ib()
            underlying = self._qualified_underlying_identity(
                ib,
                symbol,
                query=Stock(symbol, "SMART", "USD"),
                allow_missing=True,
            )
            if underlying is None:
                return ()
            with self._market_data_lease("secdef") as decision:
                self._require_market_data_lease(decision, "secdef")
                chains = ib.reqSecDefOptParams(  # type: ignore[attr-defined]
                    underlying.symbol,
                    "",
                    underlying.secType,
                    underlying.conId,
                )
        today = self._aware_now().date()
        rows: dict[tuple[date, str, str], OptionExpiration] = {}
        for chain in chains:
            exchange = str(getattr(chain, "exchange", "") or "")
            if exchange not in {"SMART", "CBOE", "ISE", "BOX", "AMEX", "ARCA"}:
                continue
            trading_class = str(getattr(chain, "tradingClass", symbol) or symbol)
            multiplier = int(str(getattr(chain, "multiplier", "100") or "100"))
            strikes = tuple(
                sorted(
                    value
                    for raw in getattr(chain, "strikes", ())
                    if (value := _decimal(raw)) is not None and value > 0
                )
            )
            for raw_expiration in getattr(chain, "expirations", ()):
                expiration = _ib_date(raw_expiration)
                dte = (expiration - today).days
                if dte < min_days or dte > max_days:
                    continue
                key = (expiration, trading_class, exchange)
                rows[key] = OptionExpiration(
                    expiration=expiration,
                    trading_class=trading_class,
                    exchange=exchange or "SMART",
                    multiplier=multiplier,
                    strikes=strikes,
                )
        return tuple(
            sorted(rows.values(), key=lambda item: (item.expiration, item.exchange))
        )

    def qualify_option_contracts(
        self,
        symbol: str,
        expiration: date,
        strikes: Sequence[Decimal | float | int | str],
        *,
        exchange: str = "SMART",
        trading_class: str | None = None,
        rights: Sequence[Literal["C", "P"]] = ("C", "P"),
    ) -> tuple[OptionContractRef, ...]:
        strike_items = tuple(strikes)
        right_items = tuple(rights)
        request_count = len(strike_items) * len(right_items)
        result = self._call_on_owner(
            lambda: self._qualify_option_contracts_on_owner(
                symbol,
                expiration,
                strike_items,
                exchange=exchange,
                trading_class=trading_class,
                rights=right_items,
            ),
            timeout_seconds=self._option_qualification_owner_timeout_seconds(
                request_count
            ),
        )
        return tuple(result)  # type: ignore[arg-type]

    def _qualify_option_contracts_on_owner(
        self,
        symbol: str,
        expiration: date,
        strikes: Sequence[Decimal | float | int | str],
        *,
        exchange: str = "SMART",
        trading_class: str | None = None,
        rights: Sequence[Literal["C", "P"]] = ("C", "P"),
    ) -> tuple[OptionContractRef, ...]:
        symbol = _symbol(symbol)
        strike_values = tuple(_required_decimal(value) for value in strikes)
        if not strike_values or len(strike_values) * len(rights) > 100:
            raise ValueError("request must contain between 1 and 100 option contracts")
        if set(rights) - {"C", "P"}:
            raise ValueError("option right must be C or P")
        _ensure_ib_event_loop()
        from ib_insync import Option

        requested = [
            Option(
                symbol=symbol,
                lastTradeDateOrContractMonth=expiration.strftime("%Y%m%d"),
                strike=float(strike),
                right=right,
                exchange=exchange,
                currency="USD",
                tradingClass=trading_class or "",
            )
            for strike in strike_values
            for right in rights
        ]
        with self._lock:
            ib = self._require_ib()
            if not self.market_data_pacing_enabled:
                qualified = list(  # type: ignore[attr-defined]
                    ib.qualifyContracts(*requested)
                )
            else:
                # A multi-contract ib-insync call fans out one secdef request
                # per contract, while the signed authority may allow fewer
                # concurrent requests than the bounded strike window.  Keep
                # exact per-wire accounting but release each reservation
                # before acquiring the next one; never manufacture extra
                # concurrency or change the human-approved rolling limit.
                qualified = []
                requested_count = len(requested)
                completed_count = 0
                owner_deadline = (
                    self._monotonic()
                    + max(
                        self._option_qualification_owner_timeout_seconds(
                            requested_count
                        )
                        - 1.0,
                        0.01,
                    )
                    if requested_count > 1
                    else None
                )
                for query in requested:
                    remaining = (
                        max(self._owner_timeout_seconds - 1.0, 0.01)
                        if owner_deadline is None
                        else owner_deadline - self._monotonic()
                    )
                    if remaining <= 0.01:
                        raise OptionQualificationError(
                            "OPTION_QUALIFICATION_DEADLINE_EXPIRED",
                            symbol=symbol,
                            expiration=expiration,
                            requested_count=requested_count,
                            completed_count=completed_count,
                            failed_right=str(getattr(query, "right", "") or ""),
                            failed_strike=_decimal(getattr(query, "strike", None)),
                        )
                    with self._market_data_lease("secdef") as decision:
                        self._require_market_data_lease(decision, "secdef")
                        prior_timeout = getattr(ib, "RequestTimeout", None)
                        try:
                            per_request_timeout = max(
                                self._owner_timeout_seconds - 1.0,
                                0.01,
                            )
                            setattr(
                                ib,
                                "RequestTimeout",
                                max(
                                    min(per_request_timeout, remaining - 0.01),
                                    0.01,
                                ),
                            )
                            resolved = tuple(  # type: ignore[attr-defined]
                                ib.qualifyContracts(query)
                            )
                        except (TimeoutError, asyncio.TimeoutError) as exc:
                            raise OptionQualificationError(
                                "OPTION_QUALIFICATION_TIMEOUT",
                                symbol=symbol,
                                expiration=expiration,
                                requested_count=requested_count,
                                completed_count=completed_count,
                                failed_right=str(
                                    getattr(query, "right", "") or ""
                                ),
                                failed_strike=_decimal(
                                    getattr(query, "strike", None)
                                ),
                            ) from exc
                        except asyncio.CancelledError as exc:
                            raise OptionQualificationError(
                                "OPTION_QUALIFICATION_CANCELLED",
                                symbol=symbol,
                                expiration=expiration,
                                requested_count=requested_count,
                                completed_count=completed_count,
                                failed_right=str(
                                    getattr(query, "right", "") or ""
                                ),
                                failed_strike=_decimal(
                                    getattr(query, "strike", None)
                                ),
                            ) from exc
                        except Exception as exc:
                            raise OptionQualificationError(
                                "OPTION_QUALIFICATION_BROKER_FAILED",
                                symbol=symbol,
                                expiration=expiration,
                                requested_count=requested_count,
                                completed_count=completed_count,
                                failed_right=str(
                                    getattr(query, "right", "") or ""
                                ),
                                failed_strike=_decimal(
                                    getattr(query, "strike", None)
                                ),
                            ) from exc
                        finally:
                            if prior_timeout is not None:
                                setattr(ib, "RequestTimeout", prior_timeout)
                        qualified.extend(resolved)
                        completed_count += 1
        results: list[OptionContractRef] = []
        for contract in qualified:
            con_id = int(contract.conId)
            contract_exchange = str(contract.exchange or exchange)
            results.append(
                OptionContractRef(
                    contract_id=con_id,
                    contract_id_ex=f"{con_id}@{contract_exchange}",
                    symbol=str(contract.symbol),
                    local_symbol=str(contract.localSymbol),
                    expiration=_ib_date(contract.lastTradeDateOrContractMonth),
                    strike=_required_decimal(contract.strike),
                    right=str(contract.right).upper(),  # type: ignore[arg-type]
                    exchange=contract_exchange,
                    trading_class=str(contract.tradingClass or symbol),
                    multiplier=int(str(contract.multiplier or "100")),
                    currency=str(contract.currency or "USD"),
                )
            )
        return tuple(results)

    @_gateway_owner_call
    def option_contract_definitions(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> tuple[OptionSecDefSnapshot, ...]:
        """Read one authoritative secdef for every requested option contract."""

        if not contracts or len(contracts) > 50:
            raise ValueError("secdef batch must contain between 1 and 50 contracts")
        con_ids = [item.contract_id for item in contracts]
        if len(set(con_ids)) != len(con_ids):
            raise ValueError("secdef batch contains duplicate conId")
        _ensure_ib_event_loop()
        from ib_insync import Option

        results: list[OptionSecDefSnapshot] = []
        with self._lock:
            ib = self._require_ib()
            for requested in contracts:
                query = Option(
                    symbol=requested.symbol,
                    lastTradeDateOrContractMonth=requested.expiration.strftime(
                        "%Y%m%d"
                    ),
                    strike=float(requested.strike),
                    right=requested.right,
                    exchange=requested.exchange,
                    currency=requested.currency,
                    tradingClass=requested.trading_class,
                    multiplier=str(requested.multiplier),
                    conId=requested.contract_id,
                    localSymbol=requested.local_symbol,
                )
                with self._market_data_lease("secdef") as decision:
                    self._require_market_data_lease(decision, "secdef")
                    details_rows = tuple(  # type: ignore[attr-defined]
                        ib.reqContractDetails(query)
                    )
                matching = [
                    item
                    for item in details_rows
                    if int(getattr(item.contract, "conId", 0) or 0)
                    == requested.contract_id
                ]
                if len(matching) != 1:
                    continue
                details = matching[0]
                contract = details.contract
                try:
                    multiplier = int(
                        str(getattr(contract, "multiplier", 0) or 0)
                    )
                    security_type = str(
                        getattr(contract, "secType", "") or ""
                    ).upper()
                    currency = str(
                        getattr(contract, "currency", "") or ""
                    ).upper()
                    local_symbol = str(
                        getattr(contract, "localSymbol", "") or ""
                    ).strip()
                    trading_class = str(
                        getattr(contract, "tradingClass", "") or ""
                    ).strip()
                    adjusted = bool(
                        getattr(details, "adjusted", False)
                        or getattr(contract, "adjusted", False)
                    )
                    standard = bool(
                        security_type == "OPT"
                        and currency == "USD"
                        and multiplier == 100
                        and local_symbol
                        and trading_class
                        and not adjusted
                    )
                    definition = OptionSecDefSnapshot(
                        contract_id=int(contract.conId),
                        local_symbol=local_symbol,
                        trading_class=trading_class,
                        multiplier=multiplier,
                        exchange=str(
                            getattr(contract, "exchange", "") or requested.exchange
                        ).upper(),
                        expiration=_ib_date(contract.lastTradeDateOrContractMonth),
                        strike=_required_decimal(contract.strike),
                        right=str(contract.right).upper(),  # type: ignore[arg-type]
                        security_type=security_type,
                        currency=currency,
                        standard_contract=standard,
                        adjusted=adjusted,
                        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
                    )
                except (AttributeError, TypeError, ValueError):
                    continue
                if not _secdef_matches_request(definition, requested):
                    continue
                results.append(definition)
        if (
            len(results) != len(contracts)
            or {item.contract_id for item in results} != set(con_ids)
        ):
            raise BrokerConnectionError(
                "IBKR secdef batch is incomplete, ambiguous, or identity-mismatched"
            )
        return tuple(results)

    def option_quote_batch(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> OptionQuoteBatch:
        """Fetch one quote batch within a bounded, batch-aware owner timeout."""

        requested = tuple(contracts)
        result = self._call_on_owner(
            lambda: self._option_quote_batch_on_owner(requested),
            timeout_seconds=self._option_quote_batch_owner_timeout_seconds(
                len(requested)
            ),
        )
        if not isinstance(result, OptionQuoteBatch):
            raise BrokerConnectionError("IBKR quote batch returned an invalid result")
        return result

    def _option_quote_batch_on_owner(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> OptionQuoteBatch:
        """Fetch one quote batch and bind missing BBO times without guessing.

        ``ib_insync.Ticker.time`` is only the local packet-arrival clock.  When
        the wrapper has no explicit exchange time, a separately paced
        historical Bid/Ask request may supply it, but only when its BBO exactly
        matches the live ticker and its UTC time passes the five-second gate.
        """

        if not contracts or len(contracts) > 50:
            raise ValueError("quote batch must contain between 1 and 50 contracts")
        con_ids = [item.contract_id for item in contracts]
        if len(set(con_ids)) != len(con_ids):
            raise ValueError("quote batch contains duplicate conId")
        _ensure_ib_event_loop()
        from ib_insync import Option

        batch_id = str(self._batch_id_factory()).strip()
        requested_at = self._aware_now()
        operation_timeout = self._option_quote_batch_owner_timeout_seconds(
            len(contracts)
        )
        owner_deadline = self._monotonic() + max(
            operation_timeout - 1.0,
            0.01,
        )
        streaming_supported = (
            self.market_data_pacing_enabled
            and callable(getattr(self._ib, "reqMktData", None))
            and callable(getattr(self._ib, "cancelMktData", None))
            and callable(getattr(self._ib, "sleep", None))
        )
        source = (
            "IBKR_REQ_MKT_DATA_READONLY"
            if streaming_supported
            else "IBKR_REQ_TICKERS_READONLY"
        )
        streaming_api_blockers: tuple[str, ...] = ()
        request_diagnostics: tuple[OptionMarketDataRequestDiagnostic, ...] = ()
        broker_timed_bbo: dict[
            int,
            tuple[datetime, Decimal, Decimal],
        ] = {}
        live_mode_blocker: str | None = None
        ib_contracts = [
            Option(
                symbol=item.symbol,
                lastTradeDateOrContractMonth=item.expiration.strftime("%Y%m%d"),
                strike=float(item.strike),
                right=item.right,
                exchange=item.exchange,
                currency=item.currency,
                tradingClass=item.trading_class,
                multiplier=str(item.multiplier),
                conId=item.contract_id,
                localSymbol=item.local_symbol,
            )
            for item in contracts
        ]
        try:
            with self._lock:
                ib = self._require_ib()
                live_mode_request = getattr(ib, "reqMarketDataType", None)
                if callable(live_mode_request):
                    try:
                        # The hard Gate accepts only live type 1.  Request that
                        # mode explicitly for this client instead of inheriting
                        # a prior delayed/frozen selection from SDK state.
                        live_mode_request(1)
                    except Exception:
                        live_mode_blocker = (
                            "IBKR_LIVE_MARKET_DATA_MODE_REQUEST_FAILED"
                        )
                else:
                    live_mode_blocker = (
                        "IBKR_LIVE_MARKET_DATA_MODE_CONTROL_UNAVAILABLE"
                    )
                if streaming_supported:
                    streaming_collection_seconds = min(
                        4.0 + (max(len(ib_contracts), 1) - 1) * 2.0,
                        12.0,
                    )
                    (
                        tickers,
                        streaming_api_blockers,
                        request_diagnostics,
                        broker_timed_bbo,
                    ) = self._request_streaming_option_tickers(
                        ib,
                        ib_contracts,
                        deadline=min(
                            owner_deadline - 2.0,
                            self._monotonic() + streaming_collection_seconds,
                        ),
                    )
                else:
                    tickers = self._request_tickers(ib, ib_contracts)
        except MarketDataPacingError:
            raise
        except (TimeoutError, asyncio.TimeoutError):
            completed_at = self._aware_now()
            return OptionQuoteBatch(
                batch_id=batch_id,
                status=QuoteBatchStatus.TIMEOUT,
                requested_at=requested_at,
                completed_at=completed_at,
                source=source,
                quotes=(),
                observed_at=completed_at,
                blockers=("QUOTE_BATCH_TIMEOUT",),
            )
        except asyncio.CancelledError:
            completed_at = self._aware_now()
            return OptionQuoteBatch(
                batch_id=batch_id,
                status=QuoteBatchStatus.CANCELLED,
                requested_at=requested_at,
                completed_at=completed_at,
                source=source,
                quotes=(),
                observed_at=completed_at,
                blockers=("QUOTE_BATCH_CANCELLED",),
            )
        except Exception as exc:
            raise BrokerConnectionError("IBKR quote batch failed") from exc

        ticker_ids = [int(getattr(item.contract, "conId", 0) or 0) for item in tickers]
        duplicate_ticker_ids = len(set(ticker_ids)) != len(ticker_ids)
        by_con_id = {
            int(getattr(ticker.contract, "conId", 0) or 0): ticker for ticker in tickers
        }
        observed_at = self._aware_now()
        historical_resolutions: dict[
            int,
            tuple[
                datetime | None,
                Decimal | None,
                Decimal | None,
                tuple[str, ...],
            ],
        ] = {}
        historical_request_made = False
        if callable(getattr(ib, "reqHistoricalTicksAsync", None)):
            historical_specs: list[
                tuple[int, object, Decimal, Decimal]
            ] = []
            for contract, ib_contract in zip(contracts, ib_contracts, strict=True):
                ticker = by_con_id.get(contract.contract_id)
                if (
                    ticker is None
                    or contract.contract_id in broker_timed_bbo
                    or _aware_or_none(getattr(ticker, "exchangeTime", None))
                    is not None
                ):
                    continue
                bid = _market_decimal(getattr(ticker, "bid", None))
                ask = _market_decimal(getattr(ticker, "ask", None))
                if bid is None or ask is None:
                    continue
                historical_specs.append(
                    (contract.contract_id, ib_contract, bid, ask)
                )
            if historical_specs:
                (
                    historical_resolutions,
                    historical_request_made,
                ) = self._matching_historical_bbo_times_batch(
                    historical_specs,
                    observed_at=observed_at,
                    deadline=owner_deadline,
                )
        quotes: list[BatchedOptionQuote] = []
        historical_blockers: list[str] = []
        for index, contract in enumerate(contracts):
            ticker = by_con_id.get(contract.contract_id)
            if ticker is None:
                continue
            greeks = getattr(ticker, "modelGreeks", None)
            is_call = contract.right == "C"
            volume = _option_volume(ticker, is_call=is_call)
            bid = _market_decimal(getattr(ticker, "bid", None))
            ask = _market_decimal(getattr(ticker, "ask", None))
            timed_bbo = broker_timed_bbo.get(contract.contract_id)
            if timed_bbo is not None:
                exchange_time, bid, ask = timed_bbo
            else:
                exchange_time = _aware_or_none(
                    getattr(ticker, "exchangeTime", None)
                )
            if exchange_time is None:
                resolved = historical_resolutions.get(contract.contract_id)
                if resolved is not None:
                    (
                        exchange_time,
                        historical_bid,
                        historical_ask,
                        request_blockers,
                    ) = resolved
                    if (
                        exchange_time is not None
                        and historical_bid is not None
                        and historical_ask is not None
                    ):
                        # Keep the historical executable BBO and its exchange
                        # timestamp indivisible; never attach a timestamp to a
                        # different live-stream price.
                        bid = historical_bid
                        ask = historical_ask
                else:
                    exchange_time, requested, request_blockers = (
                        self._matching_historical_bbo_time(
                            ib_contracts[index],
                            bid=bid,
                            ask=ask,
                            observed_at=observed_at,
                            deadline=owner_deadline,
                        )
                    )
                    historical_request_made = historical_request_made or requested
                historical_blockers.extend(
                    f"{blocker}:{contract.contract_id}"
                    for blocker in request_blockers
                )
            quotes.append(
                BatchedOptionQuote(
                    contract_id=contract.contract_id,
                    batch_id=batch_id,
                    request_id=f"{batch_id}:{index}:{contract.contract_id}",
                    requested_at=requested_at,
                    observed_at=observed_at,
                    completed_at=observed_at,
                    source=source,
                    bid=bid,
                    ask=ask,
                    last=_market_decimal(getattr(ticker, "last", None)),
                    close=_market_decimal(getattr(ticker, "close", None)),
                    exchange_time=exchange_time,
                    volume=volume,
                    open_interest=_nonnegative_integer(
                        getattr(
                            ticker,
                            "callOpenInterest" if is_call else "putOpenInterest",
                            None,
                        )
                    ),
                    implied_volatility=_market_decimal(
                        getattr(greeks, "impliedVol", None)
                        if greeks is not None
                        else None
                    ),
                    delta=_delta_decimal(
                        getattr(greeks, "delta", None) if greeks is not None else None
                    ),
                    gamma=_market_decimal(
                        getattr(greeks, "gamma", None) if greeks is not None else None
                    ),
                    theta=_finite_signed_decimal(
                        getattr(greeks, "theta", None) if greeks is not None else None
                    ),
                    vega=_market_decimal(
                        getattr(greeks, "vega", None) if greeks is not None else None
                    ),
                    market_data_type=_integer(getattr(ticker, "marketDataType", None)),
                )
            )
        completed_at = self._aware_now()
        if historical_request_made:
            source = (
                "IBKR_REQ_MKT_DATA_PLUS_HISTORICAL_TICKS_READONLY"
                if streaming_supported
                else "IBKR_REQ_TICKERS_PLUS_HISTORICAL_TICKS_READONLY"
            )
        finalized = tuple(
            replace(item, completed_at=completed_at, source=source) for item in quotes
        )
        finalized_by_contract = {item.contract_id: item for item in finalized}
        request_diagnostics = tuple(
            replace(
                diagnostic,
                received_fields=_received_option_quote_fields(
                    finalized_by_contract[diagnostic.contract_id]
                ),
                missing_fields=tuple(
                    field
                    for field in _OPTION_MARKET_DATA_DIAGNOSTIC_FIELDS
                    if field
                    not in _received_option_quote_fields(
                        finalized_by_contract[diagnostic.contract_id]
                    )
                ),
            )
            if diagnostic.contract_id in finalized_by_contract
            else diagnostic
            for diagnostic in request_diagnostics
        )
        blockers: list[str] = []
        blockers.extend(streaming_api_blockers)
        blockers.extend(historical_blockers)
        if live_mode_blocker is not None and (
            not finalized
            or not all(item.market_data_type == 1 for item in finalized)
        ):
            blockers.append(live_mode_blocker)
        if finalized and all(
            _option_quote_has_no_executable_ticks(item) for item in finalized
        ):
            blockers.append("IBKR_OPTION_EXECUTABLE_TICKS_UNAVAILABLE")
        if duplicate_ticker_ids:
            blockers.append("QUOTE_BATCH_DUPLICATE_CONID")
        if len(ticker_ids) != len(contracts) or len(finalized) != len(contracts):
            blockers.append("QUOTE_BATCH_CARDINALITY_MISMATCH")
        if (
            set(ticker_ids) != set(con_ids)
            or set(item.contract_id for item in finalized) != set(con_ids)
        ):
            blockers.append("QUOTE_BATCH_IDENTITY_MISMATCH")
        for item in finalized:
            blockers.extend(_required_option_quote_blockers(item))
        complete = not blockers
        return OptionQuoteBatch(
            batch_id=batch_id,
            status=(
                QuoteBatchStatus.COMPLETE if complete else QuoteBatchStatus.PARTIAL
            ),
            requested_at=requested_at,
            completed_at=completed_at,
            source=source,
            quotes=finalized,
            observed_at=observed_at,
            blockers=tuple(blockers),
            request_diagnostics=request_diagnostics,
        )

    @_gateway_owner_call
    def option_indicative_quote_batch(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> OptionQuoteBatch:
        """Read frozen/delayed option marks for after-hours research only.

        This deliberately skips every executable-quote freshness check and
        always restores live market-data type 1 before returning.  Callers must
        retain SUPPORTING_ONLY and NO_TRADE authority.
        """

        if not contracts or len(contracts) > 50:
            raise ValueError("quote batch must contain between 1 and 50 contracts")
        con_ids = [item.contract_id for item in contracts]
        if len(set(con_ids)) != len(con_ids):
            raise ValueError("quote batch contains duplicate conId")
        _ensure_ib_event_loop()
        from ib_insync import Option

        batch_id = str(self._batch_id_factory()).strip()
        requested_at = self._aware_now()
        owner_deadline = self._monotonic() + max(
            self._owner_timeout_seconds - 1.0,
            0.01,
        )
        ib_contracts = [
            Option(
                symbol=item.symbol,
                lastTradeDateOrContractMonth=item.expiration.strftime("%Y%m%d"),
                strike=float(item.strike),
                right=item.right,
                exchange=item.exchange,
                currency=item.currency,
                tradingClass=item.trading_class,
                multiplier=str(item.multiplier),
                conId=item.contract_id,
                localSymbol=item.local_symbol,
            )
            for item in contracts
        ]
        source = "IBKR_AFTER_HOURS_INDICATIVE_READONLY"
        request_blockers: list[str] = []
        tickers: tuple[object, ...] = ()
        requested_mode: int | None = None
        with self._lock:
            ib = self._require_ib()
            market_data_type = getattr(ib, "reqMarketDataType", None)
            try:
                # An entitled account needs explicit frozen type 2 after the
                # close; delayed-frozen type 4 is for contracts without live
                # entitlement and may be promoted back to an empty type 1
                # ticker.  Use one mode only so twenty legs stay inside the
                # signed 30-request streaming window.
                for mode in (2,):
                    requested_mode = mode
                    if callable(market_data_type):
                        market_data_type(mode)
                    try:
                        tickers, api_blockers = self._request_indicative_option_tickers(
                            ib,
                            ib_contracts,
                            deadline=min(
                                owner_deadline,
                                self._monotonic() + 3.0,
                            ),
                        )
                    except MarketDataPacingError:
                        raise
                    request_blockers.extend(api_blockers)
                    if tickers and all(
                        _indicative_option_ticker_ready(ticker)
                        for ticker in tickers
                    ):
                        break
            finally:
                if callable(market_data_type):
                    try:
                        market_data_type(1)
                    except Exception:
                        request_blockers.append(
                            "IBKR_LIVE_MARKET_DATA_MODE_RESTORE_FAILED"
                        )
        ticker_ids = [int(getattr(item.contract, "conId", 0) or 0) for item in tickers]
        by_con_id = {
            int(getattr(ticker.contract, "conId", 0) or 0): ticker for ticker in tickers
        }
        observed_at = self._aware_now()
        quotes: list[BatchedOptionQuote] = []
        for index, contract in enumerate(contracts):
            ticker = by_con_id.get(contract.contract_id)
            quotes.append(
                BatchedOptionQuote(
                    contract_id=contract.contract_id,
                    batch_id=batch_id,
                    request_id=f"{batch_id}:{index}:{contract.contract_id}",
                    requested_at=requested_at,
                    observed_at=observed_at,
                    completed_at=observed_at,
                    source=source,
                    bid=_market_decimal(getattr(ticker, "bid", None)),
                    ask=_market_decimal(getattr(ticker, "ask", None)),
                    last=_market_decimal(getattr(ticker, "last", None)),
                    close=_market_decimal(getattr(ticker, "close", None)),
                    exchange_time=_aware_or_none(
                        getattr(ticker, "exchangeTime", None)
                    ),
                    market_data_type=_integer(
                        getattr(ticker, "marketDataType", None)
                    ),
                )
            )
        missing_close_specs = tuple(
            (contract, ib_contract)
            for contract, ib_contract, quote in zip(
                contracts,
                ib_contracts,
                quotes,
                strict=True,
            )
            if not _indicative_option_quote_available(quote)
        )
        historical_blockers: tuple[str, ...] = ()
        historical_requested = False
        if missing_close_specs:
            (
                historical_trades,
                trade_blockers,
                trade_requested,
            ) = self._previous_option_trades_batch(
                missing_close_specs,
                deadline=owner_deadline,
            )
            historical_requested = historical_requested or trade_requested
            quotes = [
                replace(
                    quote,
                    last=historical_trades[quote.contract_id][0],
                    exchange_time=historical_trades[quote.contract_id][1],
                    research_price_basis="PREVIOUS_SESSION_LAST_TRADE",
                )
                if quote.contract_id in historical_trades
                else quote
                for quote in quotes
            ]
            unresolved_close_specs = tuple(
                spec
                for spec in missing_close_specs
                if spec[0].contract_id not in historical_trades
            )
            (
                historical_closes,
                close_blockers,
                close_requested,
            ) = self._previous_option_closes_batch(
                unresolved_close_specs,
                deadline=owner_deadline,
            )
            historical_requested = historical_requested or close_requested
            resolved_ids = set(historical_trades) | set(historical_closes)
            historical_blockers = tuple(
                blocker
                for blocker in (*trade_blockers, *close_blockers)
                if not _blocker_identity_is_resolved(blocker, resolved_ids)
            )
            quotes = [
                replace(
                    quote,
                    close=historical_closes[quote.contract_id][0],
                    exchange_time=historical_closes[quote.contract_id][1],
                    research_price_basis="PREVIOUS_CLOSE",
                )
                if quote.contract_id in historical_closes
                else quote
                for quote in quotes
            ]
        completed_at = self._aware_now()
        blockers = list(dict.fromkeys((*request_blockers, *historical_blockers)))
        if len(ticker_ids) != len(contracts):
            blockers.append("INDICATIVE_QUOTE_CARDINALITY_MISMATCH")
        if (
            len(set(ticker_ids)) != len(ticker_ids)
            or set(ticker_ids) != set(con_ids)
            or {item.contract_id for item in quotes} != set(con_ids)
        ):
            blockers.append("INDICATIVE_QUOTE_IDENTITY_MISMATCH")
        for quote in quotes:
            if not _indicative_option_quote_available(quote):
                blockers.append(
                    f"INDICATIVE_OPTION_PRICE_UNAVAILABLE:{quote.contract_id}"
                )
        if requested_mode is not None:
            source = f"{source}:REQUESTED_TYPE_{requested_mode}"
        if historical_requested:
            source = f"{source}+HISTORICAL_OPTION_MARK_READONLY"
        return OptionQuoteBatch(
            batch_id=batch_id,
            status=(
                QuoteBatchStatus.COMPLETE if not blockers else QuoteBatchStatus.PARTIAL
            ),
            requested_at=requested_at,
            completed_at=completed_at,
            source=source,
            quotes=tuple(
                replace(item, completed_at=completed_at, source=source)
                for item in quotes
            ),
            observed_at=observed_at,
            blockers=tuple(dict.fromkeys(blockers)),
        )

    def _previous_option_trades_batch(
        self,
        specs: Sequence[tuple[OptionContractRef, object]],
        *,
        deadline: float,
    ) -> tuple[
        dict[int, tuple[Decimal, datetime]],
        tuple[str, ...],
        bool,
    ]:
        """Read the last regular-hours trade before today's New York session."""

        if not specs:
            return {}, (), False
        if len(specs) > 20:
            return {}, ("HISTORICAL_OPTION_TRADE_LEG_LIMIT_EXCEEDED",), False
        if self._historical_request_lease_factory is None:
            return {}, ("HISTORICAL_PACING_AUTHORITY_UNAVAILABLE",), False
        if self._monotonic() >= deadline:
            return {}, ("HISTORICAL_OPTION_TRADE_DEADLINE_EXCEEDED",), False

        resolutions: dict[int, tuple[Decimal, datetime]] = {}
        blockers: list[str] = []
        requested = False
        current_new_york_date = self._aware_now().astimezone(
            ZoneInfo("America/New_York")
        ).date()
        query_end = datetime(
            current_new_york_date.year,
            current_new_york_date.month,
            current_new_york_date.day,
            tzinfo=ZoneInfo("America/New_York"),
        )

        with self._lock:
            ib = self._require_ib()
            request_async = getattr(ib, "reqHistoricalTicksAsync", None)
            if not callable(request_async):
                return {}, ("HISTORICAL_OPTION_TRADE_API_UNAVAILABLE",), False

            api_blockers: list[str] = []

            def on_error(
                req_id: object,
                error_code: object,
                _error_string: object,
                contract: object | None = None,
            ) -> None:
                code = _integer(error_code)
                if code is None or code in {2104, 2106, 2107, 2108, 2119, 2158}:
                    return
                contract_id = _integer(getattr(contract, "conId", None))
                identity = contract_id if contract_id is not None else _integer(req_id)
                api_blockers.append(
                    _ibkr_historical_option_error_blocker(code, identity)
                )

            error_event = getattr(ib, "errorEvent", None)
            handler_attached = False
            if error_event is not None:
                try:
                    error_event += on_error
                    handler_attached = True
                except Exception:
                    handler_attached = False
            try:
                for start in range(0, len(specs), 2):
                    chunk = tuple(specs[start : start + 2])
                    remaining = deadline - self._monotonic()
                    if remaining <= 0:
                        blockers.extend(
                            f"HISTORICAL_OPTION_TRADE_DEADLINE_EXCEEDED:{item.contract_id}"
                            for item, _contract in specs[start:]
                        )
                        break
                    with ExitStack() as leases:
                        chunk_allowed = True
                        for item, _contract in chunk:
                            try:
                                lease = self._historical_request_lease_factory()
                                decision = leases.enter_context(lease)
                            except Exception:
                                blockers.append(
                                    f"HISTORICAL_PACING_AUTHORIZATION_FAILED:{item.contract_id}"
                                )
                                chunk_allowed = False
                                break
                            if not bool(getattr(decision, "allowed", False)):
                                blockers.append(
                                    f"{_historical_pacing_reason(getattr(decision, 'reason', None))}:{item.contract_id}"
                                )
                                chunk_allowed = False
                                break
                        if not chunk_allowed:
                            blockers.extend(
                                f"HISTORICAL_OPTION_TRADE_NOT_REQUESTED:{item.contract_id}"
                                for item, _contract in specs[start + 1 :]
                            )
                            break

                        requested = True
                        awaitables = tuple(
                            request_async(
                                contract,
                                "",
                                query_end,
                                1,
                                "Trades",
                                True,
                                True,
                                [],
                            )
                            for _item, contract in chunk
                        )
                        try:
                            rows_by_contract = asyncio.get_event_loop().run_until_complete(
                                asyncio.wait_for(
                                    asyncio.gather(
                                        *awaitables,
                                        return_exceptions=True,
                                    ),
                                    timeout=remaining,
                                )
                            )
                        except (TimeoutError, asyncio.TimeoutError):
                            blockers.extend(
                                f"HISTORICAL_OPTION_TRADE_TIMEOUT:{item.contract_id}"
                                for item, _contract in chunk
                            )
                            continue
                        except Exception:
                            blockers.extend(
                                f"HISTORICAL_OPTION_TRADE_READ_FAILED:{item.contract_id}"
                                for item, _contract in chunk
                            )
                            continue

                    for (item, _contract), rows in zip(
                        chunk,
                        rows_by_contract,
                        strict=True,
                    ):
                        if isinstance(rows, BaseException):
                            blockers.append(
                                f"HISTORICAL_OPTION_TRADE_READ_FAILED:{item.contract_id}"
                            )
                            continue
                        eligible: list[tuple[datetime, Decimal]] = []
                        for row in rows:
                            exchange_time = _aware_or_none(getattr(row, "time", None))
                            price = _market_decimal(getattr(row, "price", None))
                            if exchange_time is None or price is None or price <= 0:
                                continue
                            if (
                                exchange_time.astimezone(
                                    ZoneInfo("America/New_York")
                                ).date()
                                >= current_new_york_date
                            ):
                                continue
                            eligible.append((exchange_time, price))
                        if not eligible:
                            blockers.append(
                                f"HISTORICAL_OPTION_TRADE_UNAVAILABLE:{item.contract_id}"
                            )
                            continue
                        exchange_time, price = max(
                            eligible,
                            key=lambda value: value[0],
                        )
                        resolutions[item.contract_id] = (price, exchange_time)
            finally:
                if handler_attached:
                    try:
                        error_event -= on_error
                    except Exception:
                        pass

        blockers.extend(api_blockers)
        return resolutions, tuple(dict.fromkeys(blockers)), requested

    def _previous_option_closes_batch(
        self,
        specs: Sequence[tuple[OptionContractRef, object]],
        *,
        deadline: float,
    ) -> tuple[
        dict[int, tuple[Decimal, datetime]],
        tuple[str, ...],
        bool,
    ]:
        """Read completed prior-session option closes in two-request chunks.

        These daily bars are research marks, never executable quotes.  Every
        wire request owns its own approved historical lease, and no more than
        two leases are held concurrently under the signed P0 limit.
        """

        if not specs:
            return {}, (), False
        if len(specs) > 20:
            return {}, ("HISTORICAL_OPTION_CLOSE_LEG_LIMIT_EXCEEDED",), False
        if self._historical_request_lease_factory is None:
            return {}, ("HISTORICAL_PACING_AUTHORITY_UNAVAILABLE",), False
        if self._monotonic() >= deadline:
            return {}, ("HISTORICAL_OPTION_CLOSE_DEADLINE_EXCEEDED",), False

        resolutions: dict[int, tuple[Decimal, datetime]] = {}
        blockers: list[str] = []
        requested = False
        current_new_york_date = self._aware_now().astimezone(
            ZoneInfo("America/New_York")
        ).date()
        query_end = datetime(
            current_new_york_date.year,
            current_new_york_date.month,
            current_new_york_date.day,
            tzinfo=ZoneInfo("America/New_York"),
        )

        with self._lock:
            ib = self._require_ib()
            request_async = getattr(ib, "reqHistoricalDataAsync", None)
            if not callable(request_async):
                return {}, ("HISTORICAL_OPTION_CLOSE_API_UNAVAILABLE",), False

            api_blockers: list[str] = []

            def on_error(
                req_id: object,
                error_code: object,
                _error_string: object,
                contract: object | None = None,
            ) -> None:
                code = _integer(error_code)
                if code is None or code in {2104, 2106, 2107, 2108, 2119, 2158}:
                    return
                contract_id = _integer(getattr(contract, "conId", None))
                identity = contract_id if contract_id is not None else _integer(req_id)
                api_blockers.append(
                    _ibkr_historical_option_error_blocker(code, identity)
                )

            error_event = getattr(ib, "errorEvent", None)
            handler_attached = False
            if error_event is not None:
                try:
                    error_event += on_error
                    handler_attached = True
                except Exception:
                    handler_attached = False
            try:
                for start in range(0, len(specs), 2):
                    chunk = tuple(specs[start : start + 2])
                    remaining = deadline - self._monotonic()
                    if remaining <= 0:
                        blockers.extend(
                            f"HISTORICAL_OPTION_CLOSE_DEADLINE_EXCEEDED:{item.contract_id}"
                            for item, _contract in specs[start:]
                        )
                        break
                    with ExitStack() as leases:
                        chunk_allowed = True
                        for item, _contract in chunk:
                            try:
                                lease = self._historical_request_lease_factory()
                                decision = leases.enter_context(lease)
                            except Exception:
                                blockers.append(
                                    f"HISTORICAL_PACING_AUTHORIZATION_FAILED:{item.contract_id}"
                                )
                                chunk_allowed = False
                                break
                            if not bool(getattr(decision, "allowed", False)):
                                blockers.append(
                                    f"{_historical_pacing_reason(getattr(decision, 'reason', None))}:{item.contract_id}"
                                )
                                chunk_allowed = False
                                break
                        if not chunk_allowed:
                            blockers.extend(
                                f"HISTORICAL_OPTION_CLOSE_NOT_REQUESTED:{item.contract_id}"
                                for item, _contract in specs[start + 1 :]
                            )
                            break

                        requested = True
                        awaitables = tuple(
                            request_async(
                                contract,
                                endDateTime=query_end,
                                durationStr="2 D",
                                barSizeSetting="1 day",
                                whatToShow="TRADES",
                                useRTH=True,
                                formatDate=1,
                                keepUpToDate=False,
                                chartOptions=[],
                                timeout=max(min(remaining, 3.0), 0.01),
                            )
                            for _item, contract in chunk
                        )
                        try:
                            rows_by_contract = asyncio.get_event_loop().run_until_complete(
                                asyncio.wait_for(
                                    asyncio.gather(
                                        *awaitables,
                                        return_exceptions=True,
                                    ),
                                    timeout=remaining,
                                )
                            )
                        except (TimeoutError, asyncio.TimeoutError):
                            blockers.extend(
                                f"HISTORICAL_OPTION_CLOSE_TIMEOUT:{item.contract_id}"
                                for item, _contract in chunk
                            )
                            continue
                        except Exception:
                            blockers.extend(
                                f"HISTORICAL_OPTION_CLOSE_READ_FAILED:{item.contract_id}"
                                for item, _contract in chunk
                            )
                            continue

                    for (item, _contract), rows in zip(
                        chunk,
                        rows_by_contract,
                        strict=True,
                    ):
                        if isinstance(rows, BaseException):
                            blockers.append(
                                f"HISTORICAL_OPTION_CLOSE_READ_FAILED:{item.contract_id}"
                            )
                            continue
                        eligible: list[tuple[date, Decimal]] = []
                        invalid_bar = False
                        for row in rows:
                            trading_date = _historical_bar_date(
                                getattr(row, "date", None)
                            )
                            close = _market_decimal(getattr(row, "close", None))
                            if trading_date is None or close is None or close <= 0:
                                invalid_bar = True
                                continue
                            if trading_date >= current_new_york_date:
                                continue
                            eligible.append((trading_date, close))
                        dates = tuple(trading_date for trading_date, _close in eligible)
                        if len(set(dates)) != len(dates):
                            blockers.append(
                                f"HISTORICAL_OPTION_CLOSE_DUPLICATE_DATE:{item.contract_id}"
                            )
                            continue
                        if not eligible:
                            blockers.append(
                                f"HISTORICAL_OPTION_CLOSE_INVALID_BAR:{item.contract_id}"
                                if invalid_bar
                                else f"HISTORICAL_OPTION_CLOSE_UNAVAILABLE:{item.contract_id}"
                            )
                            continue
                        trading_date, close = max(eligible, key=lambda value: value[0])
                        resolutions[item.contract_id] = (
                            close,
                            datetime(
                                trading_date.year,
                                trading_date.month,
                                trading_date.day,
                                16,
                                tzinfo=ZoneInfo("America/New_York"),
                            ).astimezone(timezone.utc),
                        )
            finally:
                if handler_attached:
                    try:
                        error_event -= on_error
                    except Exception:
                        pass

        blockers.extend(api_blockers)
        return resolutions, tuple(dict.fromkeys(blockers)), requested

    def _request_indicative_option_tickers(
        self,
        ib: object,
        contracts: Sequence[object],
        *,
        deadline: float,
        readiness: Callable[[object], bool] | None = None,
    ) -> tuple[tuple[object, ...], tuple[str, ...]]:
        """Read short-lived non-live ticks under the normal streaming budget."""

        ticker_ready = readiness or _indicative_option_ticker_ready
        if not (
            callable(getattr(ib, "reqMktData", None))
            and callable(getattr(ib, "cancelMktData", None))
            and callable(getattr(ib, "sleep", None))
        ):
            return self._request_tickers(ib, contracts), ()
        requested: list[tuple[object, object]] = []
        api_blockers: list[str] = []

        def on_error(
            req_id: object,
            error_code: object,
            _error_string: object,
            contract: object | None = None,
        ) -> None:
            code = _integer(error_code)
            if code is None or code in {2104, 2106, 2107, 2108, 2119, 2158}:
                return
            contract_id = _integer(getattr(contract, "conId", None))
            identity = contract_id if contract_id is not None else _integer(req_id)
            api_blockers.append(_ibkr_market_data_error_blocker(code, identity))

        error_event = getattr(ib, "errorEvent", None)
        handler_attached = False
        if error_event is not None:
            try:
                error_event += on_error
                handler_attached = True
            except Exception:
                handler_attached = False
        try:
            with ExitStack() as leases:
                for contract in contracts:
                    decision = leases.enter_context(
                        self._market_data_lease("streaming_quote")
                    )
                    self._require_market_data_lease(decision, "streaming_quote")
                    ticker = ib.reqMktData(  # type: ignore[attr-defined]
                        contract,
                        "",
                        False,
                        False,
                    )
                    requested.append((contract, ticker))
                while self._monotonic() < deadline:
                    if all(
                        ticker_ready(ticker)
                        for _contract, ticker in requested
                    ):
                        break
                    remaining = deadline - self._monotonic()
                    if remaining <= 0:
                        break
                    ib.sleep(min(0.05, remaining))  # type: ignore[attr-defined]
        finally:
            for contract, _ticker in requested:
                try:
                    ib.cancelMktData(contract)  # type: ignore[attr-defined]
                except Exception:
                    pass
            if handler_attached:
                try:
                    error_event -= on_error
                except Exception:
                    pass
        return (
            tuple(ticker for _contract, ticker in requested),
            tuple(dict.fromkeys(api_blockers)),
        )

    def _matching_historical_bbo_times_batch(
        self,
        specs: Sequence[tuple[int, object, Decimal, Decimal]],
        *,
        observed_at: datetime,
        deadline: float,
    ) -> tuple[
        dict[
            int,
            tuple[
                datetime | None,
                Decimal | None,
                Decimal | None,
                tuple[str, ...],
            ],
        ],
        bool,
    ]:
        """Resolve exact BBO timestamps concurrently under per-leg leases."""

        contract_ids = tuple(item[0] for item in specs)
        if self._historical_request_lease_factory is None:
            return (
                {
                    contract_id: (
                        None,
                        None,
                        None,
                        ("HISTORICAL_PACING_AUTHORITY_UNAVAILABLE",),
                    )
                    for contract_id in contract_ids
                },
                False,
            )
        if self._monotonic() >= deadline:
            return (
                {
                    contract_id: (
                        None,
                        None,
                        None,
                        ("HISTORICAL_BBO_DEADLINE_EXCEEDED",),
                    )
                    for contract_id in contract_ids
                },
                False,
            )

        resolutions: dict[
            int,
            tuple[
                datetime | None,
                Decimal | None,
                Decimal | None,
                tuple[str, ...],
            ],
        ] = {}
        pending_specs = list(specs)
        request_made = False
        while pending_specs:
            if self._monotonic() >= deadline:
                for contract_id, _contract, _bid, _ask in pending_specs:
                    resolutions[contract_id] = (
                        None,
                        None,
                        None,
                        ("HISTORICAL_BBO_DEADLINE_EXCEEDED",),
                    )
                break

            chunk: list[tuple[int, object, Decimal, Decimal]] = []
            terminal_pacing_blockers: tuple[str, ...] | None = None
            with ExitStack() as leases:
                while pending_specs:
                    try:
                        lease = self._historical_request_lease_factory()
                        decision = leases.enter_context(lease)
                    except Exception:
                        terminal_pacing_blockers = (
                            "HISTORICAL_PACING_AUTHORIZATION_FAILED",
                        )
                        break
                    if not bool(getattr(decision, "allowed", False)):
                        reason = _historical_pacing_reason(
                            getattr(decision, "reason", None)
                        )
                        # A concurrency denial after at least one reservation
                        # defines the safe chunk boundary.  Dispatch that chunk,
                        # release its leases, then continue without changing the
                        # human-approved limit or manufacturing extra capacity.
                        if (
                            reason == "HISTORICAL_PACING_CONCURRENCY_LIMIT"
                            and chunk
                        ):
                            break
                        terminal_pacing_blockers = tuple(
                            dict.fromkeys(("HISTORICAL_PACING_DENIED", reason))
                        )
                        break
                    chunk.append(pending_specs.pop(0))

                if not chunk:
                    blockers = terminal_pacing_blockers or (
                        "HISTORICAL_PACING_DENIED",
                    )
                    for contract_id, _contract, _bid, _ask in pending_specs:
                        resolutions[contract_id] = (
                            None,
                            None,
                            None,
                            blockers,
                        )
                    break

                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    for contract_id, _contract, _bid, _ask in (
                        *chunk,
                        *pending_specs,
                    ):
                        resolutions[contract_id] = (
                            None,
                            None,
                            None,
                            ("HISTORICAL_BBO_DEADLINE_EXCEEDED",),
                        )
                    break
                with self._lock:
                    ib = self._require_ib()
                    awaitables = tuple(
                        ib.reqHistoricalTicksAsync(  # type: ignore[attr-defined]
                            contract,
                            "",
                            observed_at,
                            1,
                            "Bid_Ask",
                            False,
                            False,
                            [],
                        )
                        for _contract_id, contract, _bid, _ask in chunk
                    )
                    request_made = True

                    async def collect_outcomes() -> tuple[
                        tuple[bool, object], ...
                    ]:
                        tasks = tuple(
                            asyncio.ensure_future(awaitable)
                            for awaitable in awaitables
                        )
                        completed_tasks: set[asyncio.Future[object]] = set()
                        try:
                            done, raw_pending = await asyncio.wait(
                                tasks,
                                timeout=remaining,
                            )
                            completed_tasks = set(done)
                            pending = set(raw_pending)
                            outcomes: list[tuple[bool, object]] = []
                            for task in tasks:
                                if task in pending:
                                    outcomes.append((False, None))
                                    continue
                                try:
                                    outcomes.append((True, task.result()))
                                except BaseException as exc:
                                    outcomes.append((True, exc))
                            return tuple(outcomes)
                        finally:
                            unresolved = tuple(
                                task
                                for task in tasks
                                if task not in completed_tasks
                            )
                            for task in unresolved:
                                if not task.done():
                                    task.cancel()
                            if unresolved:
                                await asyncio.gather(
                                    *unresolved,
                                    return_exceptions=True,
                                )

                    try:
                        outcomes_by_contract = (
                            asyncio.get_event_loop().run_until_complete(
                                collect_outcomes()
                            )
                        )
                    except (TimeoutError, asyncio.TimeoutError):
                        for contract_id, _contract, _bid, _ask in (
                            *chunk,
                            *pending_specs,
                        ):
                            resolutions[contract_id] = (
                                None,
                                None,
                                None,
                                ("HISTORICAL_BBO_TIMEOUT",),
                            )
                        return resolutions, request_made
                    except Exception:
                        for contract_id, _contract, _bid, _ask in (
                            *chunk,
                            *pending_specs,
                        ):
                            resolutions[contract_id] = (
                                None,
                                None,
                                None,
                                ("HISTORICAL_BBO_READ_FAILED",),
                            )
                        return resolutions, request_made

            chunk_timed_out = False
            for spec, outcome in zip(
                chunk,
                outcomes_by_contract,
                strict=True,
            ):
                contract_id, _contract, _streaming_bid, _streaming_ask = spec
                completed, rows = outcome
                if not completed:
                    chunk_timed_out = True
                    resolutions[contract_id] = (
                        None,
                        None,
                        None,
                        ("HISTORICAL_BBO_TIMEOUT",),
                    )
                    continue
                if isinstance(rows, BaseException):
                    resolutions[contract_id] = (
                        None,
                        None,
                        None,
                        ("HISTORICAL_BBO_READ_FAILED",),
                    )
                    continue
                eligible: list[tuple[datetime, Decimal, Decimal]] = []
                for row in rows:
                    exchange_time = _aware_or_none(getattr(row, "time", None))
                    historical_bid = _market_decimal(
                        getattr(row, "priceBid", None)
                    )
                    historical_ask = _market_decimal(
                        getattr(row, "priceAsk", None)
                    )
                    if (
                        exchange_time is None
                        or historical_bid is None
                        or historical_ask is None
                        or historical_bid <= 0
                        or historical_ask < historical_bid
                    ):
                        continue
                    age_seconds = Decimal(
                        str((observed_at - exchange_time).total_seconds())
                    )
                    if Decimal("0") <= age_seconds <= Decimal(
                        str(self.config.quote_fresh_seconds)
                    ):
                        eligible.append(
                            (exchange_time, historical_bid, historical_ask)
                        )
                if eligible:
                    exchange_time, historical_bid, historical_ask = max(
                        eligible,
                        key=lambda item: item[0],
                    )
                    resolutions[contract_id] = (
                        exchange_time,
                        historical_bid,
                        historical_ask,
                        (),
                    )
                else:
                    resolutions[contract_id] = (
                        None,
                        None,
                        None,
                        ("HISTORICAL_BBO_NO_FRESH_EXECUTABLE_QUOTE",),
                    )

            if chunk_timed_out:
                for contract_id, _contract, _bid, _ask in pending_specs:
                    resolutions[contract_id] = (
                        None,
                        None,
                        None,
                        ("HISTORICAL_BBO_DEADLINE_EXCEEDED",),
                    )
                return resolutions, request_made

            if terminal_pacing_blockers is not None:
                for contract_id, _contract, _bid, _ask in pending_specs:
                    resolutions[contract_id] = (
                        None,
                        None,
                        None,
                        terminal_pacing_blockers,
                    )
                break
        return resolutions, request_made

    def _matching_historical_bbo_time(
        self,
        contract: object,
        *,
        bid: Decimal | None,
        ask: Decimal | None,
        observed_at: datetime,
        deadline: float | None = None,
    ) -> tuple[datetime | None, bool, tuple[str, ...]]:
        """Return a fresh exchange time only for an exact live-BBO match."""

        if bid is None or ask is None:
            return None, False, ()
        if deadline is not None and self._monotonic() >= deadline:
            return None, False, ("HISTORICAL_BBO_DEADLINE_EXCEEDED",)

        lease: AbstractContextManager[object] | None = None
        if self._historical_request_lease_factory is not None:
            try:
                lease = self._historical_request_lease_factory()
            except Exception:
                return None, False, ("HISTORICAL_PACING_AUTHORIZATION_FAILED",)
        else:
            return None, False, ("HISTORICAL_PACING_AUTHORITY_UNAVAILABLE",)

        try:
            assert lease is not None
            with lease as decision:
                if not bool(getattr(decision, "allowed", False)):
                    reason = _historical_pacing_reason(
                        getattr(decision, "reason", None)
                    )
                    return None, False, tuple(
                        dict.fromkeys(("HISTORICAL_PACING_DENIED", reason))
                    )
                rows = self._request_historical_bbo_rows(
                    contract,
                    observed_at=observed_at,
                    deadline=deadline,
                )
        except _HistoricalDeadlineExpired:
            return None, False, ("HISTORICAL_BBO_DEADLINE_EXCEEDED",)
        except (TimeoutError, asyncio.TimeoutError):
            return None, True, ("HISTORICAL_BBO_TIMEOUT",)
        except Exception:
            return None, True, ("HISTORICAL_BBO_READ_FAILED",)

        matching_times: list[datetime] = []
        for row in rows:
            exchange_time = _aware_or_none(getattr(row, "time", None))
            if (
                exchange_time is None
                or _market_decimal(getattr(row, "priceBid", None)) != bid
                or _market_decimal(getattr(row, "priceAsk", None)) != ask
            ):
                continue
            age_seconds = Decimal(str((observed_at - exchange_time).total_seconds()))
            if Decimal("0") <= age_seconds <= Decimal(
                str(self.config.quote_fresh_seconds)
            ):
                matching_times.append(exchange_time)
        if not matching_times:
            return None, True, ("HISTORICAL_BBO_NO_EXACT_FRESH_MATCH",)
        return max(matching_times), True, ()

    def _request_historical_bbo_rows(
        self,
        contract: object,
        *,
        observed_at: datetime,
        deadline: float | None,
    ) -> tuple[object, ...]:
        if deadline is not None:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise _HistoricalDeadlineExpired(
                    "historical BBO aggregate deadline expired"
                )
        else:
            remaining = self._owner_timeout_seconds - 1.0
        with self._lock:
            ib = self._require_ib()
            prior_timeout = getattr(ib, "RequestTimeout", None)
            timeout_changed = isinstance(prior_timeout, (int, float)) and not isinstance(
                prior_timeout,
                bool,
            )
            if timeout_changed:
                setattr(
                    ib,
                    "RequestTimeout",
                    max(min(float(prior_timeout), remaining), 0.01),
                )
            try:
                return tuple(
                    ib.reqHistoricalTicks(  # type: ignore[attr-defined]
                        contract,
                        "",
                        observed_at,
                        1,
                        "Bid_Ask",
                        False,
                        False,
                    )
                )
            finally:
                if timeout_changed:
                    setattr(ib, "RequestTimeout", prior_timeout)

    @_gateway_owner_call
    def option_quotes(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> tuple[OptionQuoteSnapshot, ...]:
        batch = self.option_quote_batch(contracts)
        if batch.status is not QuoteBatchStatus.COMPLETE:
            return tuple(
                _empty_quote(contract, batch.completed_at) for contract in contracts
            )
        by_con_id = {item.contract_id: item for item in batch.quotes}
        snapshots: list[OptionQuoteSnapshot] = []
        for contract in contracts:
            quote = by_con_id.get(contract.contract_id)
            if quote is None:
                snapshots.append(_empty_quote(contract, batch.completed_at))
                continue
            snapshots.append(
                OptionQuoteSnapshot(
                    contract=contract,
                    observed_at=quote.observed_at,
                    exchange_time=quote.exchange_time,
                    bid=quote.bid,
                    ask=quote.ask,
                    last=quote.last,
                    close=quote.close,
                    volume=quote.volume,
                    open_interest=quote.open_interest,
                    implied_volatility=quote.implied_volatility,
                    delta=quote.delta,
                    gamma=quote.gamma,
                    theta=quote.theta,
                    vega=quote.vega,
                    market_data_type=quote.market_data_type,
                )
            )
        return tuple(snapshots)

    @_gateway_owner_call
    def underlying_quotes(
        self,
        symbols: Sequence[str],
    ) -> tuple[UnderlyingQuoteSnapshot, ...]:
        """Fetch one coherent, identity-checked stock quote batch.

        These quotes select nearby strikes only.  They can never replace the
        per-leg option quote batch used for payoff, costs, or eligibility.
        """

        return self._underlying_quote_batch(symbols, frozen=False)

    @_gateway_owner_call
    def underlying_indicative_quotes(
        self,
        symbols: Sequence[str],
    ) -> tuple[UnderlyingQuoteSnapshot, ...]:
        """Fetch frozen closing stock marks for SUPPORTING_ONLY research."""

        return self._underlying_quote_batch(symbols, frozen=True)

    def _underlying_quote_batch(
        self,
        symbols: Sequence[str],
        *,
        frozen: bool,
    ) -> tuple[UnderlyingQuoteSnapshot, ...]:
        tickers = tuple(_symbol(value) for value in symbols)
        if not tickers or len(tickers) > 50 or len(set(tickers)) != len(tickers):
            raise ValueError("underlying quote request must contain 1-50 unique symbols")
        _ensure_ib_event_loop()
        from ib_insync import Stock

        with self._lock:
            ib = self._require_ib()
            if self.market_data_pacing_enabled:
                qualified = tuple(
                    self._qualified_underlying_identity(
                        ib,
                        symbol,
                        query=Stock(symbol, "SMART", "USD"),
                    )
                    for symbol in tickers
                )
            else:
                cached_by_symbol = {
                    symbol: self._cached_underlying_identity(symbol)
                    for symbol in tickers
                }
                missing_symbols = tuple(
                    symbol
                    for symbol in tickers
                    if cached_by_symbol[symbol] is None
                )
                if missing_symbols:
                    missing_queries = tuple(
                        Stock(symbol, "SMART", "USD")
                        for symbol in missing_symbols
                    )
                    resolved = tuple(  # type: ignore[attr-defined]
                        ib.qualifyContracts(*missing_queries)
                    )
                    if len(resolved) != len(missing_queries):
                        raise BrokerConnectionError(
                            "IBKR underlying qualification was incomplete"
                        )
                    for symbol, contract in zip(
                        missing_symbols,
                        resolved,
                        strict=True,
                    ):
                        self._remember_underlying_identity(symbol, contract)
                        cached_by_symbol[symbol] = contract
                qualified = tuple(cached_by_symbol[symbol] for symbol in tickers)
            market_data_type = getattr(ib, "reqMarketDataType", None)
            owner_deadline = (
                self._monotonic()
                + max(self._owner_timeout_seconds - 1.0, 0.01)
            )
            try:
                if frozen and callable(market_data_type):
                    market_data_type(2)
                if frozen:
                    rows, _blockers = self._request_indicative_option_tickers(
                        ib,
                        qualified,
                        deadline=min(
                            owner_deadline,
                            self._monotonic() + 3.0,
                        ),
                        readiness=_indicative_underlying_ticker_ready,
                    )
                else:
                    rows = self._request_tickers(ib, qualified)
            except MarketDataPacingError:
                raise
            except (TimeoutError, asyncio.TimeoutError) as exc:
                raise BrokerConnectionError("IBKR underlying quote batch timed out") from exc
            except asyncio.CancelledError as exc:
                raise BrokerConnectionError("IBKR underlying quote batch was cancelled") from exc
            except Exception as exc:
                raise BrokerConnectionError("IBKR underlying quote batch failed") from exc
            finally:
                if frozen and callable(market_data_type):
                    try:
                        market_data_type(1)
                    except Exception as exc:
                        raise BrokerConnectionError(
                            "IBKR live market-data mode restore failed"
                        ) from exc

        historical_closes: dict[int, tuple[Decimal, Decimal]] = {}
        if frozen:
            historical_closes = self._previous_underlying_closes_batch(
                qualified,
                deadline=owner_deadline,
            )
        requested_ids = tuple(int(getattr(item, "conId", 0) or 0) for item in qualified)
        row_ids = tuple(
            int(getattr(getattr(item, "contract", None), "conId", 0) or 0)
            for item in rows
        )
        if (
            any(value <= 0 for value in requested_ids)
            or len(set(requested_ids)) != len(requested_ids)
            or len(rows) != len(qualified)
            or len(set(row_ids)) != len(row_ids)
            or set(row_ids) != set(requested_ids)
        ):
            raise BrokerConnectionError("IBKR underlying quote batch identity mismatch")
        by_id = {
            int(getattr(getattr(item, "contract", None), "conId", 0) or 0): item
            for item in rows
        }
        observed_at = self._aware_now()
        results: list[UnderlyingQuoteSnapshot] = []
        for symbol, contract in zip(tickers, qualified, strict=True):
            con_id = int(getattr(contract, "conId", 0) or 0)
            row = by_id[con_id]
            streamed_close = _market_decimal(getattr(row, "close", None))
            closing_pair = historical_closes.get(con_id)
            latest_close = closing_pair[0] if closing_pair is not None else None
            prior_close = closing_pair[1] if closing_pair is not None else None
            results.append(
                UnderlyingQuoteSnapshot(
                    symbol=symbol,
                    contract_id=con_id,
                    exchange=str(
                        getattr(contract, "primaryExchange", "")
                        or getattr(contract, "exchange", "")
                        or "SMART"
                    ),
                    observed_at=observed_at,
                    source=(
                        "IBKR_AFTER_HOURS_UNDERLYING_READONLY+"
                        "HISTORICAL_TWO_CLOSES"
                        if frozen and closing_pair is not None
                        else "IBKR_AFTER_HOURS_UNDERLYING_READONLY+"
                        "HISTORICAL_PREVIOUS_CLOSE"
                        if frozen and streamed_close is None and latest_close is not None
                        else "IBKR_AFTER_HOURS_UNDERLYING_READONLY"
                        if frozen
                        else "IBKR_REQ_TICKERS_READONLY"
                    ),
                    bid=(
                        None
                        if closing_pair is not None
                        else _market_decimal(getattr(row, "bid", None))
                    ),
                    ask=(
                        None
                        if closing_pair is not None
                        else _market_decimal(getattr(row, "ask", None))
                    ),
                    last=(
                        latest_close
                        if closing_pair is not None
                        else _market_decimal(getattr(row, "last", None))
                    ),
                    close=prior_close or streamed_close or latest_close,
                    volume=_nonnegative_integer(getattr(row, "volume", None)),
                    market_data_type=_integer(getattr(row, "marketDataType", None)),
                )
            )
        return tuple(results)

    def _previous_underlying_closes_batch(
        self,
        contracts: Sequence[object],
        *,
        deadline: float,
    ) -> dict[int, tuple[Decimal, Decimal]]:
        """Read the latest two completed stock closes under pacing authority."""

        if (
            not contracts
            or self._historical_request_lease_factory is None
            or self._monotonic() >= deadline
        ):
            return {}
        current_new_york_date = self._aware_now().astimezone(
            ZoneInfo("America/New_York")
        ).date()
        query_end = datetime(
            current_new_york_date.year,
            current_new_york_date.month,
            current_new_york_date.day,
            tzinfo=ZoneInfo("America/New_York"),
        )
        resolutions: dict[int, tuple[Decimal, Decimal]] = {}
        with self._lock:
            ib = self._require_ib()
            request_async = getattr(ib, "reqHistoricalDataAsync", None)
            if not callable(request_async):
                return {}
            for start in range(0, len(contracts), 2):
                chunk = tuple(contracts[start : start + 2])
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    break
                with ExitStack() as leases:
                    allowed = True
                    for _contract in chunk:
                        try:
                            decision = leases.enter_context(
                                self._historical_request_lease_factory()
                            )
                        except Exception:
                            allowed = False
                            break
                        if not bool(getattr(decision, "allowed", False)):
                            allowed = False
                            break
                    if not allowed:
                        break
                    awaitables = tuple(
                        request_async(
                            contract,
                            endDateTime=query_end,
                            durationStr="5 D",
                            barSizeSetting="1 day",
                            whatToShow="TRADES",
                            useRTH=True,
                            formatDate=1,
                            keepUpToDate=False,
                            chartOptions=[],
                            timeout=max(min(remaining, 3.0), 0.01),
                        )
                        for contract in chunk
                    )
                    try:
                        rows_by_contract = asyncio.get_event_loop().run_until_complete(
                            asyncio.wait_for(
                                asyncio.gather(*awaitables, return_exceptions=True),
                                timeout=remaining,
                            )
                        )
                    except Exception:
                        continue
                for contract, rows in zip(chunk, rows_by_contract, strict=True):
                    if isinstance(rows, BaseException):
                        continue
                    completed = tuple(
                        (
                            bar_date,
                            close,
                        )
                        for row in rows
                        if (bar_date := _historical_bar_date(getattr(row, "date", None)))
                        is not None
                        and bar_date < current_new_york_date
                        and (close := _market_decimal(getattr(row, "close", None)))
                        is not None
                        and close > 0
                    )
                    ordered = tuple(
                        sorted(
                            completed,
                            key=lambda value: value[0],
                            reverse=True,
                        )
                    )
                    if len(ordered) >= 2:
                        resolutions[int(getattr(contract, "conId", 0) or 0)] = (
                            ordered[0][1],
                            ordered[1][1],
                        )
        return resolutions

    @_gateway_owner_call
    def prepare_native_history(
        self, symbol: str, *, kind: str, cutoff: Mapping[str, object], incremental: bool,
    ) -> dict[str, object]:
        """Freeze a native request using only this day's qualified cache."""

        from options_copilot.gateway.native_history import cached_native_identity
        from options_copilot.history_source_contracts import (
            NativeHistoryContractError,
            build_native_history_request,
        )

        with self._lock:
            self._require_ib()
            contract = self._cached_underlying_identity(_symbol(symbol))
            if contract is None:
                raise NativeHistoryContractError("NATIVE_HISTORY_CACHED_IDENTITY_UNAVAILABLE")
            return build_native_history_request(
                contract=cached_native_identity(contract), kind=kind, cutoff=cutoff,
                incremental=incremental, prepared_at=self._aware_now(),
            )

    @_gateway_owner_call
    def read_native_history(
        self, prepared: Mapping[str, object], *, before_send: Callable[[], str],
        operation_guard: Callable[[], bool], remaining_seconds: Callable[[], float],
    ) -> dict[str, object]:
        """Execute one claimed native request, never an implicit qualification."""

        from options_copilot.gateway.native_history import (
            cached_native_identity,
            read_native_history_on_owner,
        )
        from options_copilot.history_source_contracts import validate_native_history_request

        request = validate_native_history_request(prepared)

        def identity_matches() -> bool:
            contract = self._cached_underlying_identity(request["symbol"])
            return contract is not None and cached_native_identity(contract) == request["contract"]

        with self._lock:
            ib = self._require_ib_socket()
            return read_native_history_on_owner(
                ib, request, clock=self._aware_now, monotonic=self._monotonic,
                upstream_health=self.upstream_health, identity_matches=identity_matches,
                historical_lease_factory=self._historical_request_lease_factory,
                before_send=before_send, operation_guard=operation_guard,
                remaining_seconds=remaining_seconds,
            )

    @_gateway_owner_call
    def feature_price_history(
        self, symbol: str, *, end_at: datetime,
    ) -> Mapping[str, object]:
        """Observe bounded adjusted prices without producing model inputs."""
        return self._feature_history_diagnostic(
            symbol, end_at=end_at, kind="PRICE_HISTORY",
            what_to_show="ADJUSTED_LAST", duration="1 Y", required_count=60,
        )

    @_gateway_owner_call
    def feature_iv_history(
        self, symbol: str, *, end_at: datetime,
    ) -> Mapping[str, object]:
        """Observe native daily IV separately from the production 30-day path."""
        return self._feature_history_diagnostic(
            symbol, end_at=end_at, kind="IV_HISTORY",
            what_to_show="OPTION_IMPLIED_VOLATILITY", duration="2 Y",
            required_count=252,
        )

    def _feature_history_diagnostic(
        self, symbol: str, *, end_at: datetime, kind: str,
        what_to_show: str, duration: str, required_count: int,
    ) -> Mapping[str, object]:
        payload = self._feature_diagnostic_base(symbol, end_at=end_at, kind=kind)
        deadline = self._monotonic() + max(self._owner_timeout_seconds - 1, 0.01)
        cutoff = utc_datetime(end_at, field="end_at")
        cutoff_date = cutoff.astimezone(ZoneInfo("America/New_York")).date()
        # ADJUSTED_LAST is a current provider vintage. Its empty request end
        # is retained; filtering earlier dates does not manufacture PIT history.
        query_end = "" if what_to_show == "ADJUSTED_LAST" else cutoff.astimezone(
            ZoneInfo("America/New_York")
        ).replace(hour=0, minute=0, second=0, microsecond=0)
        payload["request_parameters"] = {
            "endDateTime": query_end if isinstance(query_end, str) else query_end.isoformat(),
            "durationStr": duration, "barSizeSetting": "1 day",
            "whatToShow": what_to_show, "useRTH": True, "formatDate": 1,
            "keepUpToDate": False,
        }
        rows: tuple[object, ...] = ()
        reasons: list[str] = []
        try:
            with self._lock:
                ib, contract = self._feature_diagnostic_identity(payload)
                if self._historical_request_lease_factory is None:
                    raise MarketDataPacingError("historical", "PACING_CAPABILITY_MISSING")
                with self._historical_request_lease_factory() as decision:
                    self._require_market_data_lease(decision, "historical")
                    remaining = min(4.0, deadline - self._monotonic())
                    if remaining <= 0:
                        raise TimeoutError
                    with _feature_error_capture(ib) as errors:
                        payload["request_sent"] = True
                        raw = ib.reqHistoricalData(  # type: ignore[attr-defined]
                            contract, endDateTime=query_end, durationStr=duration,
                            barSizeSetting="1 day", whatToShow=what_to_show,
                            useRTH=True, formatDate=1, keepUpToDate=False,
                            timeout=remaining,
                        )
                        rows = tuple(raw)
                        request_id = _integer(getattr(raw, "reqId", None))
                        payload["broker_request_id"] = request_id
                        payload["broker_error_codes"] = _feature_error_codes(
                            errors, request_id, int(getattr(contract, "conId")),
                        )
        except MarketDataPacingError as exc:
            reasons.append(f"FEATURE_PACING_{exc.reason_code}")
        except (TimeoutError, asyncio.TimeoutError):
            reasons.append("FEATURE_SOURCE_REQUEST_TIMEOUT")
        except Exception:
            reasons.append("FEATURE_SOURCE_REQUEST_FAILED")
        bars: list[dict[str, object]] = []
        complete_dates: list[date] = []
        invalid_count = excluded_count = 0
        for row in rows[:800]:
            raw_date = getattr(row, "date", None)
            trading_date = _historical_bar_date(raw_date)
            close = _market_decimal(getattr(row, "close", None))
            valid = bool(
                trading_date is not None and close is not None and close > 0
                and (kind != "IV_HISTORY" or close <= Decimal("5"))
            )
            prior = bool(trading_date is not None and trading_date < cutoff_date)
            if not valid:
                invalid_count += 1
            if trading_date is not None and not prior:
                excluded_count += 1
            if valid and prior:
                complete_dates.append(trading_date)
            bars.append({
                "raw_date": str(raw_date)[:40],
                "trading_date": None if trading_date is None else trading_date.isoformat(),
                **{
                    name: None if (value := _market_decimal(getattr(row, name, None))) is None else str(value)
                    for name in ("open", "high", "low", "close", "volume")
                },
                "prior_date_row": prior, "valid_close": valid,
            })
        unique_count = len(set(complete_dates))
        payload.update({
            "bars": bars, "received_bar_count": len(rows),
            "prior_completed_bar_count": unique_count,
            "excluded_current_or_future_bar_count": excluded_count,
            "invalid_bar_count": invalid_count,
            "duplicate_prior_date_count": len(complete_dates) - unique_count,
            "required_prior_bar_count": required_count,
            "enough_prior_bars": unique_count >= required_count,
            "calendar_coverage_verified": False,
        })
        if len(rows) > 800:
            reasons.append("FEATURE_SOURCE_ROW_LIMIT_EXCEEDED")
        if invalid_count:
            reasons.append("FEATURE_SOURCE_INVALID_BARS")
        if len(complete_dates) != unique_count:
            reasons.append("FEATURE_SOURCE_DUPLICATE_BAR_DATES")
        if not rows and not reasons:
            # ib-insync returns an empty BarDataList after its own timeout.
            reasons.append("FEATURE_HISTORY_EMPTY_OR_TIMEOUT")
        elif unique_count < required_count:
            reasons.append("FEATURE_HISTORY_OBSERVED_ROW_COUNT_INSUFFICIENT")
        return self._finish_feature_diagnostic(payload, reasons, delivered=unique_count > 0)

    @_gateway_owner_call
    def feature_current_iv(
        self, symbol: str, *, end_at: datetime,
    ) -> Mapping[str, object]:
        """Observe only a new tick24 from this temporary underlying request."""
        payload = self._feature_diagnostic_base(symbol, end_at=end_at, kind="CURRENT_IV")
        payload.update({
            "request_parameters": {"genericTickList": "106", "snapshot": False, "regulatorySnapshot": False},
            "tick_type": 24, "generic_tick": 106, "value": None,
            "received_at": None, "market_data_type": None,
            "source_event_timestamp": None,
        })
        reasons: list[str] = []
        owned_ticker: object | None = None
        started_at = self._aware_now()

        def on_update(tickers: object) -> None:
            if owned_ticker is None or not any(item is owned_ticker for item in tickers):
                return
            for tick in getattr(owned_ticker, "ticks", ()):
                received_at = _aware_or_none(getattr(tick, "time", None))
                value = _market_decimal(getattr(tick, "price", None))
                if (
                    getattr(tick, "tickType", None) == 24
                    and received_at is not None
                    and started_at <= received_at <= self._aware_now()
                    and value is not None and Decimal("0") < value <= Decimal("5")
                ):
                    payload["value"] = str(value)
                    payload["received_at"] = received_at.isoformat()

        try:
            with self._lock:
                ib, qualified = self._feature_diagnostic_identity(payload)
                # ib-insync keys ticker ownership by object identity. A clone
                # prevents cancellation from touching an existing subscription.
                contract = copy(qualified)
                event = getattr(ib, "pendingTickersEvent", None)
                if event is None:
                    raise RuntimeError("tick observation callback unavailable")
                with self._market_data_lease("streaming_quote") as decision:
                    self._require_market_data_lease(decision, "streaming_quote")
                    with _feature_error_capture(ib) as errors:
                        event += on_update
                        try:
                            started_at = self._aware_now()
                            payload["request_sent"] = True
                            owned_ticker = ib.reqMktData(contract, "106", False, False)  # type: ignore[attr-defined]
                            request_id = _ticker_market_data_request_id(ib, owned_ticker)
                            payload["broker_request_id"] = request_id
                            deadline = self._monotonic() + 3.0
                            while payload["value"] is None and self._monotonic() < deadline:
                                ib.sleep(min(0.05, max(deadline - self._monotonic(), 0.0)))  # type: ignore[attr-defined]
                            payload["market_data_type"] = _integer(getattr(owned_ticker, "marketDataType", None))
                            payload["broker_error_codes"] = _feature_error_codes(errors, request_id, int(getattr(contract, "conId")))
                        finally:
                            try:
                                if owned_ticker is not None:
                                    ib.cancelMktData(contract)  # type: ignore[attr-defined]
                            finally:
                                event -= on_update
        except MarketDataPacingError as exc:
            reasons.append(f"FEATURE_PACING_{exc.reason_code}")
        except (TimeoutError, asyncio.TimeoutError):
            reasons.append("FEATURE_SOURCE_REQUEST_TIMEOUT")
        except Exception:
            reasons.append("FEATURE_SOURCE_REQUEST_FAILED")
        if payload["value"] is None:
            reasons.append("FEATURE_CURRENT_IV_NEW_TICK24_UNAVAILABLE")
        if payload["market_data_type"] != 1:
            reasons.append("FEATURE_CURRENT_IV_NOT_LIVE")
        return self._finish_feature_diagnostic(payload, reasons, delivered=payload["value"] is not None)

    def _feature_diagnostic_base(
        self, symbol: str, *, end_at: datetime, kind: str,
    ) -> dict[str, object]:
        checked_symbol = _symbol(symbol)
        cutoff = utc_datetime(end_at, field="end_at")
        requested_at = self._aware_now()
        if cutoff > requested_at:
            raise ValueError("end_at must not be in the future")
        return {
            "schema": "options_copilot.feature_source_diagnostic.v1",
            "kind": kind, "symbol": checked_symbol, "source": "IBKR",
            "requested_at": requested_at.isoformat(), "cutoff_at": cutoff.isoformat(),
            "basis_status": "PROVIDER_NATIVE_UNRESOLVED",
            "decision_authority": "OBSERVATION_ONLY", "production_eligible": False,
            "model_input_complete": False, "point_in_time_verified": False,
            "request_sent": False, "broker_request_id": None,
            "broker_error_codes": [], "contract": None,
        }

    def _feature_diagnostic_identity(
        self, payload: dict[str, object],
    ) -> tuple[object, object]:
        from ib_insync import Stock

        if not self.market_data_pacing_enabled:
            raise MarketDataPacingError("secdef", "PACING_CAPABILITY_MISSING")
        ib = self._require_ib()
        symbol = str(payload["symbol"])
        prior_timeout = getattr(ib, "RequestTimeout", None)
        try:
            # Leave time for the single data request inside the owner deadline.
            setattr(ib, "RequestTimeout", min(2.0, float(prior_timeout or 2.0)))
            contract = self._qualified_underlying_identity(ib, symbol, query=Stock(symbol, "SMART", "USD"))
        finally:
            if prior_timeout is not None:
                setattr(ib, "RequestTimeout", prior_timeout)
        payload["contract"] = {
            "con_id": int(getattr(contract, "conId")), "symbol": symbol,
            "sec_type": "STK", "currency": "USD",
            "exchange": str(getattr(contract, "exchange", "")),
            "primary_exchange": str(getattr(contract, "primaryExchange", "")),
        }
        return ib, contract

    def _finish_feature_diagnostic(
        self, payload: dict[str, object], reasons: list[str], *, delivered: bool,
    ) -> Mapping[str, object]:
        for code in payload["broker_error_codes"]:
            reasons.append(
                f"FEATURE_SOURCE_NOT_SUBSCRIBED:{code}"
                if code in {354, 10089, 10168, 10189}
                else f"FEATURE_SOURCE_BROKER_ERROR:{code}"
            )
        payload["available_at"] = self._aware_now().isoformat()
        payload["reason_codes"] = list(dict.fromkeys(reasons))
        payload["status"] = "PARTIAL" if delivered and reasons else "DELIVERED" if delivered else "UNAVAILABLE"
        payload["content_hash"] = canonical_hash(payload)
        return payload

    @_gateway_owner_call
    def underlying_iv_history(
        self,
        symbol: str,
        *,
        end_at: datetime,
    ) -> UnderlyingIvHistory:
        """Read one bounded daily IV history for a qualified stock basis.

        IBKR exposes ``OPTION_IMPLIED_VOLATILITY`` history on the underlying
        stock/ETF contract, not on individual option strikes.  Fixed request
        parameters and completed, unique trading dates prevent a current
        cross-section from being mistaken for a time series.
        """

        ticker = _symbol(symbol)
        checked_end = utc_datetime(end_at, field="end_at")
        if checked_end > utc_datetime(self._aware_now(), field="clock result"):
            raise ValueError("end_at must not be in the future")
        if self._historical_request_lease_factory is None:
            raise MarketDataPacingError(
                "historical",
                "PACING_CAPABILITY_MISSING",
            )
        _ensure_ib_event_loop()
        from ib_insync import Stock

        request_exchange = "SMART"
        currency = "USD"
        duration = "30 D"
        bar_size = "1 day"
        what_to_show = "OPTION_IMPLIED_VOLATILITY"
        use_rth = True
        # An intraday daily-bar request may include today's unfinished bar.
        # Query from New York midnight so the returned series contains only
        # completed trading dates while evidence remains bound to checked_end.
        query_end = checked_end.astimezone(
            ZoneInfo("America/New_York")
        ).replace(hour=0, minute=0, second=0, microsecond=0)
        with self._lock:
            ib = self._require_ib()
            basis = self._qualified_underlying_identity(
                ib,
                ticker,
                query=Stock(ticker, request_exchange, currency),
            )
            contract_id = int(getattr(basis, "conId", 0) or 0)
            if (
                contract_id <= 0
                or str(getattr(basis, "symbol", "") or "").upper() != ticker
                or str(getattr(basis, "secType", "") or "").upper() != "STK"
                or str(getattr(basis, "currency", "") or "").upper() != currency
            ):
                raise BrokerConnectionError(
                    "IBKR underlying IV basis identity mismatch"
                )
            try:
                lease = self._historical_request_lease_factory()
            except Exception as exc:
                raise MarketDataPacingError(
                    "historical",
                    "PACING_AUTHORIZATION_FAILED",
                ) from exc
            try:
                with lease as decision:
                    self._require_market_data_lease(decision, "historical")
                    bars = tuple(  # type: ignore[attr-defined]
                        ib.reqHistoricalData(
                            basis,
                            endDateTime=query_end,
                            durationStr=duration,
                            barSizeSetting=bar_size,
                            whatToShow=what_to_show,
                            useRTH=use_rth,
                            formatDate=1,
                            keepUpToDate=False,
                            timeout=self._owner_timeout_seconds,
                        )
                    )
            except MarketDataPacingError:
                raise
            except (TimeoutError, asyncio.TimeoutError) as exc:
                raise BrokerConnectionError(
                    "IBKR underlying IV history timed out"
                ) from exc
            except Exception as exc:
                raise BrokerConnectionError(
                    "IBKR underlying IV history failed"
                ) from exc

        cutoff_date = checked_end.astimezone(ZoneInfo("America/New_York")).date()
        points: list[UnderlyingIvHistoryPoint] = []
        for bar in bars:
            trading_date = _historical_bar_date(getattr(bar, "date", None))
            close = _market_decimal(getattr(bar, "close", None))
            if (
                trading_date is None
                or close is None
                or close <= 0
                or close > Decimal("5")
            ):
                raise BrokerConnectionError(
                    "IBKR underlying IV history contained an invalid bar"
                )
            if trading_date >= cutoff_date:
                raise BrokerConnectionError(
                    "IBKR underlying IV history contained an unfinished bar"
                )
            points.append(UnderlyingIvHistoryPoint(trading_date, close))
        dates = tuple(item.trading_date for item in points)
        if (
            len(points) < 10
            or len(set(dates)) != len(dates)
            or dates != tuple(sorted(dates))
        ):
            raise BrokerConnectionError(
                "IBKR underlying IV history was insufficient or non-monotonic"
            )

        observed_at = utc_datetime(self._aware_now(), field="clock result")
        basis_payload = {
            "symbol": ticker,
            "contract_id": contract_id,
            "security_type": "STK",
            "request_exchange": request_exchange,
            "currency": currency,
        }
        provisional = UnderlyingIvHistory(
            symbol=ticker,
            contract_id=contract_id,
            request_exchange=request_exchange,
            currency=currency,
            observed_at=observed_at,
            end_at=checked_end,
            duration=duration,
            bar_size=bar_size,
            what_to_show=what_to_show,
            use_rth=use_rth,
            points=tuple(points),
            basis_hash=canonical_hash(basis_payload),
            content_hash="",
        )
        return replace(
            provisional,
            content_hash=canonical_hash(provisional.hash_payload()),
        )

    @_gateway_owner_call
    def options_session_hours(self, symbol: str = "SPY") -> OptionsSessionHours:
        """Read raw broker-published liquid/trading hours for calendar gates."""

        ticker = _symbol(symbol)
        _ensure_ib_event_loop()
        from ib_insync import Stock

        with self._lock:
            ib = self._require_ib()
            query = self._cached_underlying_identity(ticker)
            if query is None:
                query = Stock(ticker, "SMART", "USD")
            with self._market_data_lease("secdef") as decision:
                self._require_market_data_lease(decision, "secdef")
                try:
                    details = tuple(  # type: ignore[attr-defined]
                        ib.reqContractDetails(query)
                    )
                except (TimeoutError, asyncio.TimeoutError):
                    raise SessionCalendarReadError(
                        "CALENDAR_BROKER_REQUEST_TIMEOUT"
                    ) from None
                except Exception:
                    raise SessionCalendarReadError(
                        "CALENDAR_BROKER_REQUEST_FAILED"
                    ) from None
        if not details:
            raise SessionCalendarReadError("CALENDAR_CONTRACT_DETAILS_UNAVAILABLE")
        if len(details) != 1:
            raise SessionCalendarReadError("CALENDAR_CONTRACT_DETAILS_AMBIGUOUS")
        contract = getattr(details[0], "contract", query)
        contract_id = int(getattr(contract, "conId", 0) or 0)
        liquid_hours = str(getattr(details[0], "liquidHours", "") or "").strip()
        trading_hours = str(getattr(details[0], "tradingHours", "") or "").strip()
        timezone_id = str(getattr(details[0], "timeZoneId", "") or "").strip()
        if contract_id <= 0 or not liquid_hours or not trading_hours or not timezone_id:
            raise SessionCalendarReadError("CALENDAR_CONTRACT_DETAILS_INCOMPLETE")
        try:
            self._remember_underlying_identity(ticker, contract)
        except BrokerConnectionError:
            raise SessionCalendarReadError(
                "CALENDAR_CONTRACT_IDENTITY_MISMATCH"
            ) from None
        return OptionsSessionHours(
            symbol=ticker,
            contract_id=contract_id,
            observed_at=self._aware_now(),
            source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
            liquid_hours=liquid_hours,
            trading_hours=trading_hours,
            timezone_id=timezone_id,
        )

    @_gateway_owner_call
    def scan_underlyings(
        self,
        *,
        scan_codes: Sequence[str] = ("MOST_ACTIVE", "TOP_PERC_GAIN", "TOP_PERC_LOSE"),
        rows_per_scan: int = 20,
    ) -> tuple[UnderlyingScanResult, ...]:
        if rows_per_scan <= 0 or rows_per_scan > 50:
            raise ValueError("rows_per_scan must be between 1 and 50")
        _ensure_ib_event_loop()
        from ib_insync import ScannerSubscription

        seen: set[int] = set()
        results: list[UnderlyingScanResult] = []
        with self._lock:
            ib = self._require_ib()
            for code in scan_codes:
                subscription = ScannerSubscription(
                    instrument="STK",
                    locationCode="STK.US.MAJOR",
                    scanCode=str(code),
                    numberOfRows=rows_per_scan,
                )
                with self._market_data_lease("scanner") as decision:
                    self._require_market_data_lease(decision, "scanner")
                    rows = ib.reqScannerData(subscription)  # type: ignore[attr-defined]
                for row in rows:
                    details = row.contractDetails
                    contract = details.contract
                    con_id = int(contract.conId)
                    if con_id in seen:
                        continue
                    seen.add(con_id)
                    results.append(
                        UnderlyingScanResult(
                            rank=int(getattr(row, "rank", len(results))),
                            symbol=str(contract.symbol),
                            contract_id=con_id,
                            exchange=str(
                                getattr(contract, "primaryExchange", "")
                                or getattr(contract, "exchange", "")
                                or "SMART"
                            ),
                            source_scan=str(code),
                            industry=_optional_bounded_text(
                                getattr(details, "industry", None),
                                maximum=80,
                            ),
                            category=_optional_bounded_text(
                                getattr(details, "category", None),
                                maximum=80,
                            ),
                            subcategory=_optional_bounded_text(
                                getattr(details, "subcategory", None),
                                maximum=80,
                            ),
                        )
                    )
        return tuple(results)

    def _require_ib_socket(self) -> object:
        if self._ib is None or not self._ib.isConnected():  # type: ignore[attr-defined]
            raise BrokerConnectionError("IBKR read-only gateway is not connected")
        return self._ib

    def _control_batch_on_owner(self) -> ControlAuthorityBatch:
        self._require_ib_socket()
        authority, _generation_offset = self._control_state
        if authority is None:
            raise BrokerControlAuthorityError("IBKR_CONTROL_DISCONNECTED")
        try:
            return authority.read(
                timeout_seconds=min(max(self._owner_timeout_seconds - 2.0, 0.01), 6.0),
            )
        except ControlAuthorityError as exc:
            raise BrokerControlAuthorityError(exc.reason_code) from None

    def _require_ib(self) -> object:
        ib = self._require_ib_socket()
        authority, _generation_offset = self._control_state
        if authority is None:
            raise BrokerControlAuthorityError("IBKR_CONTROL_DISCONNECTED")
        health = authority.health()
        if (
            health["status"] == "RECOVERY_PENDING"
            and health["reason_codes"] == ["CONTROL_INITIAL_SYNC_REQUIRED"]
        ):
            # Initial market research can read from the socket, but only a
            # separate completed control batch can ever confer account authority.
            return ib
        try:
            authority.require_ready()
        except ControlAuthorityError as exc:
            raise BrokerControlAuthorityError(exc.reason_code) from None
        return ib

    def _clear_underlying_identity_cache(self) -> None:
        self._underlying_identity_cache_day = None
        self._underlying_identity_cache.clear()

    def _underlying_identity_day(self) -> date:
        return self._aware_now().astimezone(ZoneInfo("America/New_York")).date()

    def _cached_underlying_identity(self, symbol: str) -> object | None:
        day = self._underlying_identity_day()
        if self._underlying_identity_cache_day != day:
            self._underlying_identity_cache_day = day
            self._underlying_identity_cache.clear()
        return self._underlying_identity_cache.get(symbol)

    def _remember_underlying_identity(self, symbol: str, contract: object) -> None:
        ticker = _symbol(symbol)
        contract_id = int(getattr(contract, "conId", 0) or 0)
        if (
            contract_id <= 0
            or str(getattr(contract, "symbol", "") or "").strip().upper()
            != ticker
            or str(getattr(contract, "secType", "") or "").strip().upper()
            != "STK"
            or str(getattr(contract, "currency", "") or "").strip().upper()
            != "USD"
        ):
            raise BrokerConnectionError("IBKR underlying qualification identity mismatch")
        self._cached_underlying_identity(ticker)
        self._underlying_identity_cache[ticker] = contract

    def _qualified_underlying_identity(
        self,
        ib: object,
        symbol: str,
        *,
        query: object,
        allow_missing: bool = False,
    ) -> object | None:
        ticker = _symbol(symbol)
        cached = self._cached_underlying_identity(ticker)
        if cached is not None:
            return cached
        with self._market_data_lease("secdef") as decision:
            self._require_market_data_lease(decision, "secdef")
            qualified = tuple(  # type: ignore[attr-defined]
                ib.qualifyContracts(query)
            )
        if allow_missing and not qualified:
            return None
        if len(qualified) != 1:
            raise BrokerConnectionError("IBKR underlying qualification was incomplete")
        contract = qualified[0]
        self._remember_underlying_identity(ticker, contract)
        return contract

    def _call_on_owner(
        self,
        operation: Callable[[], object],
        *,
        create: bool = False,
        timeout_seconds: float | None = None,
    ) -> object:
        owner = self._owner
        if owner is not None and owner.is_current:
            return operation()
        # External callers are serialized across disconnect/reconnect.  Nested
        # calls made by the owner bypass this gate above, avoiding self-deadlock.
        with self._owner_gate:
            owner = self._owner
            if owner is None:
                if not create:
                    raise BrokerConnectionError(
                        "IBKR read-only gateway is not connected"
                    )
                owner = _GatewayOwnerThread(
                    name=f"options-copilot-ibkr-{self.config.ibkr_client_id}"
                )
                self._owner = owner
            return owner.call(
                operation,
                timeout_seconds=(
                    self._owner_timeout_seconds
                    if timeout_seconds is None
                    else max(float(timeout_seconds), 0.01)
                ),
            )

    @property
    def _owner_timeout_seconds(self) -> float:
        return max(float(self.config.ibkr_timeout_seconds), 0.01)

    def _option_qualification_owner_timeout_seconds(
        self,
        request_count: int,
    ) -> float:
        """Bound the serialized SecDef batch without abandoning its owner task."""

        base_timeout = self._owner_timeout_seconds
        if not self.market_data_pacing_enabled or request_count <= 1:
            return base_timeout
        per_request_timeout = max(base_timeout - 1.0, 0.01)
        return min(
            base_timeout + ((max(int(request_count), 1) - 1) * per_request_timeout),
            60.0,
        )

    def _option_quote_batch_owner_timeout_seconds(
        self,
        request_count: int,
    ) -> float:
        """Bound sequential historical-BBO chunks under signed concurrency."""

        base_timeout = self._owner_timeout_seconds
        if self._historical_request_lease_factory is None or request_count <= 1:
            return base_timeout
        bounded_count = max(int(request_count), 1)
        chunk_count = math.ceil(
            bounded_count / self._historical_request_max_concurrency
        )
        per_chunk_timeout = max(base_timeout - 1.0, 0.01)
        return min(
            base_timeout + ((chunk_count - 1) * per_chunk_timeout),
            60.0,
        )

    def _close_disconnected_owner(self) -> None:
        with self._lock:
            if self._ib is not None:
                return
        with self._owner_gate:
            owner = self._owner
            if owner is None:
                return
            if owner.close(timeout_seconds=self._owner_timeout_seconds):
                if self._owner is owner:
                    self._owner = None

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value


@contextmanager
def _feature_error_capture(
    ib: object,
) -> Iterator[list[tuple[int | None, int | None, int]]]:
    """Capture numeric request diagnostics, never broker error text."""
    rows: list[tuple[int | None, int | None, int]] = []

    def on_error(
        req_id: object, error_code: object, _error_string: object,
        contract: object | None = None,
    ) -> None:
        code = _integer(error_code)
        if code is not None and code not in {2104, 2106, 2107, 2108, 2119, 2158}:
            rows.append((_integer(req_id), _integer(getattr(contract, "conId", None)), code))

    event = getattr(ib, "errorEvent", None)
    if event is not None:
        event += on_error
    try:
        yield rows
    finally:
        if event is not None:
            event -= on_error


def _feature_error_codes(
    rows: Sequence[tuple[int | None, int | None, int]],
    request_id: int | None,
    contract_id: int,
) -> list[int]:
    return list(dict.fromkeys(
        code for row_request, row_contract, code in rows
        if (request_id is not None and row_request == request_id)
        or (request_id is None and row_contract == contract_id)
    ))


def _default_ib_factory() -> object:
    _ensure_ib_event_loop()
    from ib_insync import IB

    return IB()


def _ensure_ib_event_loop() -> None:
    """ib_insync/eventkit still expects a current loop on Python 3.12."""

    try:
        asyncio.get_event_loop_policy().get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _symbol(value: str) -> str:
    cleaned = value.strip().upper()
    if not cleaned or len(cleaned) > 12 or not cleaned.replace(".", "").isalnum():
        raise ValueError("invalid US underlying symbol")
    return cleaned


def _optional_bounded_text(value: object, *, maximum: int) -> str | None:
    text = str(value or "").strip()
    return text[:maximum] if text else None


def _decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _required_decimal(value: object) -> Decimal:
    result = _decimal(value)
    if result is None:
        raise ValueError("finite decimal value required")
    return result


def _market_decimal(value: object) -> Decimal | None:
    result = _decimal(value)
    if result is None or result < 0:
        return None
    return result


def _finite_signed_decimal(value: object) -> Decimal | None:
    """Parse a finite signed value without applying market-price bounds."""

    return _decimal(value)


def _delta_decimal(value: object) -> Decimal | None:
    """Parse a finite option delta in its mathematically valid range."""

    result = _finite_signed_decimal(value)
    if result is None or result < Decimal("-1") or result > Decimal("1"):
        return None
    return result


def _integer(value: object) -> int | None:
    result = _decimal(value)
    if result is None or result != result.to_integral_value():
        return None
    try:
        return int(result)
    except (ValueError, OverflowError):
        return None


def _nonnegative_integer(value: object) -> int | None:
    result = _integer(value)
    return result if result is not None and result >= 0 else None


def _option_volume(ticker: object, *, is_call: bool) -> int | None:
    """Use generic volume only when IBKR marks side volume as unset."""

    raw = getattr(
        ticker,
        "callVolume" if is_call else "putVolume",
        None,
    )
    if (
        raw is None
        or (isinstance(raw, float) and math.isnan(raw))
        or (isinstance(raw, Decimal) and raw.is_nan())
    ):
        raw = getattr(ticker, "volume", None)
    return _nonnegative_integer(raw)


_OPTION_MARKET_DATA_DIAGNOSTIC_FIELDS = (
    "bid",
    "ask",
    "exchange_time",
    "market_data_type",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "volume",
    "open_interest",
)


def _ticker_market_data_request_id(ib: object, ticker: object) -> int | None:
    """Recover ib-insync's exact temporary subscription reqId by identity."""

    return _ticker_request_id(ib, ticker, "mktData")


def _ticker_request_id(
    ib: object,
    ticker: object,
    tick_type: str,
) -> int | None:
    """Recover one exact ib-insync ticker request id by transport key."""

    wrapper = getattr(ib, "wrapper", None)
    ticker_to_request = getattr(wrapper, "ticker2ReqId", None)
    if isinstance(ticker_to_request, Mapping):
        market_data_requests = ticker_to_request.get(tick_type)
        if isinstance(market_data_requests, Mapping):
            try:
                request_id = _integer(market_data_requests.get(ticker))
            except TypeError:
                request_id = None
            if request_id is not None:
                return request_id

    request_to_ticker = getattr(wrapper, "reqId2Ticker", None)
    if not isinstance(request_to_ticker, Mapping):
        return None
    matches = sorted(
        request_id
        for raw_request_id, candidate in request_to_ticker.items()
        if candidate is ticker and (request_id := _integer(raw_request_id)) is not None
    )
    return matches[-1] if matches else None


def _received_option_ticker_fields(
    ticker: object,
    contract: object,
    *,
    exchange_time_override: datetime | None = None,
) -> tuple[str, ...]:
    """Name each hard field actually present on the temporary subscription."""

    right = str(getattr(contract, "right", "")).strip().upper()
    is_call = right == "C"
    greeks = getattr(ticker, "modelGreeks", None)
    present = {
        "bid": _market_decimal(getattr(ticker, "bid", None)) is not None,
        "ask": _market_decimal(getattr(ticker, "ask", None)) is not None,
        "exchange_time": exchange_time_override is not None
        or _aware_or_none(getattr(ticker, "exchangeTime", None)) is not None,
        "market_data_type": _integer(
            getattr(ticker, "marketDataType", None)
        )
        is not None,
        "implied_volatility": greeks is not None
        and _market_decimal(getattr(greeks, "impliedVol", None)) is not None,
        "delta": greeks is not None
        and _delta_decimal(getattr(greeks, "delta", None)) is not None,
        "gamma": greeks is not None
        and _market_decimal(getattr(greeks, "gamma", None)) is not None,
        "theta": greeks is not None
        and _finite_signed_decimal(getattr(greeks, "theta", None)) is not None,
        "vega": greeks is not None
        and _market_decimal(getattr(greeks, "vega", None)) is not None,
        "volume": _option_volume(ticker, is_call=is_call) is not None,
        "open_interest": _nonnegative_integer(
            getattr(
                ticker,
                "callOpenInterest" if is_call else "putOpenInterest",
                None,
            )
        )
        is not None,
    }
    return tuple(
        field
        for field in _OPTION_MARKET_DATA_DIAGNOSTIC_FIELDS
        if present[field]
    )


def _received_option_quote_fields(
    quote: BatchedOptionQuote,
) -> tuple[str, ...]:
    """Name each hard field retained in the final atomic quote row."""

    present = {
        "bid": quote.bid is not None,
        "ask": quote.ask is not None,
        "exchange_time": quote.exchange_time is not None,
        "market_data_type": quote.market_data_type is not None,
        "implied_volatility": quote.implied_volatility is not None,
        "delta": quote.delta is not None,
        "gamma": quote.gamma is not None,
        "theta": quote.theta is not None,
        "vega": quote.vega is not None,
        "volume": quote.volume is not None,
        "open_interest": quote.open_interest is not None,
    }
    return tuple(
        field
        for field in _OPTION_MARKET_DATA_DIAGNOSTIC_FIELDS
        if present[field]
    )


def _streaming_option_ticker_ready(ticker: object, contract: object) -> bool:
    """Return true once every hard option field has arrived on live ticks."""

    bid = _market_decimal(getattr(ticker, "bid", None))
    ask = _market_decimal(getattr(ticker, "ask", None))
    if bid is None or ask is None or bid <= 0 or ask < bid:
        return False
    right = str(getattr(contract, "right", "")).strip().upper()
    is_call = right == "C"
    volume = _option_volume(ticker, is_call=is_call)
    open_interest = getattr(
        ticker,
        "callOpenInterest" if is_call else "putOpenInterest",
        None,
    )
    greeks = getattr(ticker, "modelGreeks", None)
    return (
        volume is not None
        and _nonnegative_integer(open_interest) is not None
        and _integer(getattr(ticker, "marketDataType", None)) == 1
        and greeks is not None
        and _market_decimal(getattr(greeks, "impliedVol", None)) is not None
        and _delta_decimal(getattr(greeks, "delta", None)) is not None
        and _market_decimal(getattr(greeks, "gamma", None)) is not None
        and _finite_signed_decimal(getattr(greeks, "theta", None)) is not None
        and _market_decimal(getattr(greeks, "vega", None)) is not None
    )


def _indicative_option_ticker_ready(ticker: object) -> bool:
    """Accept an explicit frozen BBO or a last/close research mark."""

    bid = _market_decimal(getattr(ticker, "bid", None))
    ask = _market_decimal(getattr(ticker, "ask", None))
    if bid is not None and ask is not None and ask >= bid:
        return True
    last = _market_decimal(getattr(ticker, "last", None))
    close = _market_decimal(getattr(ticker, "close", None))
    return (last is not None and last > 0) or (close is not None and close > 0)


def _indicative_underlying_ticker_ready(ticker: object) -> bool:
    """Wait for both a frozen stock mark and its prior-session close.

    IBKR can publish a stock's frozen bid/ask or last before it publishes the
    ``close`` tick.  Direction selection compares those two independent values,
    so accepting the early mark would leave the after-hours scan without its
    required reference close.
    """

    close = _market_decimal(getattr(ticker, "close", None))
    if close is None or close <= 0:
        return False
    bid = _market_decimal(getattr(ticker, "bid", None))
    ask = _market_decimal(getattr(ticker, "ask", None))
    last = _market_decimal(getattr(ticker, "last", None))
    return (
        bid is not None
        and ask is not None
        and ask >= bid
        or last is not None
        and last > 0
    )


def _indicative_option_quote_available(quote: BatchedOptionQuote) -> bool:
    if quote.bid is not None and quote.ask is not None and quote.ask >= quote.bid:
        return True
    return (
        quote.last is not None
        and quote.last > 0
        or quote.close is not None
        and quote.close > 0
    )


def _ibkr_market_data_error_blocker(
    error_code: int,
    identity: int | None,
) -> str:
    suffix = "UNKNOWN" if identity is None else str(identity)
    if error_code in {354, 10089, 10090, 10167, 10168, 10189}:
        return f"IBKR_OPTION_MARKET_DATA_NOT_SUBSCRIBED:{error_code}:{suffix}"
    if error_code == 10197:
        return f"IBKR_COMPETING_LIVE_SESSION:{error_code}:{suffix}"
    return f"IBKR_OPTION_MARKET_DATA_ERROR:{error_code}:{suffix}"


def _ibkr_historical_option_error_blocker(
    error_code: int,
    identity: int | None,
) -> str:
    suffix = "UNKNOWN" if identity is None else str(identity)
    if error_code in {354, 10089, 10167, 10168, 10189}:
        return f"IBKR_HISTORICAL_OPTION_DATA_NOT_SUBSCRIBED:{error_code}:{suffix}"
    if error_code == 165:
        return f"IBKR_HISTORICAL_OPTION_QUERY_EMPTY:{error_code}:{suffix}"
    if error_code == 162:
        return f"IBKR_HISTORICAL_OPTION_DATA_ERROR:{error_code}:{suffix}"
    return f"IBKR_HISTORICAL_OPTION_ERROR:{error_code}:{suffix}"


def _blocker_identity_is_resolved(
    blocker: str,
    resolved_ids: set[int],
) -> bool:
    try:
        identity = int(str(blocker).rsplit(":", 1)[1])
    except (IndexError, ValueError):
        return False
    return identity in resolved_ids


def _ib_date(value: object) -> date:
    raw = str(value).replace("-", "")[:8]
    return datetime.strptime(raw, "%Y%m%d").date()


def _ib_date_or_none(value: object) -> date | None:
    try:
        return _ib_date(value)
    except (TypeError, ValueError):
        return None


def _historical_bar_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return _ib_date(value)
    except (TypeError, ValueError):
        return None


def _aware_or_none(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return None
    return value


def _secdef_matches_request(
    definition: OptionSecDefSnapshot,
    requested: OptionContractRef,
) -> bool:
    """Require the broker response to stay bound to the requested option.

    The authoritative multiplier is intentionally preserved even when it
    differs from the requested standard contract: callers must see that value
    and reject the resulting ``standard_contract=False`` definition.
    """

    return bool(
        definition.contract_id == requested.contract_id
        and definition.local_symbol == requested.local_symbol
        and definition.trading_class == requested.trading_class
        and definition.exchange.upper() == requested.exchange.upper()
        and definition.expiration == requested.expiration
        and definition.strike == requested.strike
        and definition.right == requested.right
    )


def _required_option_quote_blockers(quote: BatchedOptionQuote) -> tuple[str, ...]:
    """Return every missing hard-data field that prevents executable use."""

    suffix = str(quote.contract_id)
    blockers: list[str] = []
    if quote.bid is None:
        blockers.append(f"QUOTE_BID_UNAVAILABLE:{suffix}")
    if quote.ask is None:
        blockers.append(f"QUOTE_ASK_UNAVAILABLE:{suffix}")
    if quote.bid is not None and quote.ask is not None and quote.ask < quote.bid:
        blockers.append(f"QUOTE_MARKET_CROSSED:{suffix}")
    if quote.exchange_time is None:
        blockers.append(f"QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE:{suffix}")
    if quote.market_data_type != 1:
        blockers.append(f"QUOTE_LIVE_MARKET_DATA_TYPE_UNAVAILABLE:{suffix}")
    if quote.implied_volatility is None:
        blockers.append(f"QUOTE_IV_UNAVAILABLE:{suffix}")
    if quote.delta is None:
        blockers.append(f"QUOTE_DELTA_UNAVAILABLE:{suffix}")
    if quote.gamma is None:
        blockers.append(f"QUOTE_GAMMA_UNAVAILABLE:{suffix}")
    if quote.theta is None:
        blockers.append(f"QUOTE_THETA_UNAVAILABLE:{suffix}")
    if quote.vega is None:
        blockers.append(f"QUOTE_VEGA_UNAVAILABLE:{suffix}")
    if quote.volume is None:
        blockers.append(f"QUOTE_VOLUME_UNAVAILABLE:{suffix}")
    if quote.open_interest is None:
        blockers.append(f"QUOTE_OPEN_INTEREST_UNAVAILABLE:{suffix}")
    return tuple(blockers)


def _option_quote_has_no_executable_ticks(quote: BatchedOptionQuote) -> bool:
    """Detect a broker ticker that carried no decision-usable option fields."""

    return all(
        value is None
        for value in (
            quote.bid,
            quote.ask,
            quote.implied_volatility,
            quote.delta,
            quote.gamma,
            quote.theta,
            quote.vega,
            quote.volume,
            quote.open_interest,
        )
    )


def _historical_pacing_reason(value: object) -> str:
    reason = str(value or "").strip().upper()
    allowed = {
        "PACING_CAPABILITY_MISSING",
        "PACING_CONCURRENCY_LIMIT",
        "PACING_COOLDOWN_ACTIVE",
        "PACING_REQUEST_WINDOW_EXHAUSTED",
    }
    if reason in allowed:
        return f"HISTORICAL_{reason}"
    return "HISTORICAL_PACING_DENIED"


def _empty_quote(
    contract: OptionContractRef, observed_at: datetime
) -> OptionQuoteSnapshot:
    return OptionQuoteSnapshot(
        contract=contract,
        observed_at=observed_at,
        exchange_time=None,
        bid=None,
        ask=None,
        last=None,
        close=None,
        volume=None,
        open_interest=None,
        implied_volatility=None,
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
        market_data_type=None,
    )
