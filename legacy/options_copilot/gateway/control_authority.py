"""Response-ended control observations on one existing read-only IB owner."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import threading
import time


_ACCOUNT_TAGS = (
    "NetLiquidation", "EquityWithLoanValue", "AvailableFunds", "BuyingPower",
    "InitMarginReq", "MaintMarginReq", "ExcessLiquidity",
)
_HOOKS = (
    "accountSummary", "accountSummaryEnd", "position", "positionEnd",
    "openOrder", "openOrderEnd",
)
_INFORMATIONAL_ERRORS = frozenset({2103, 2104, 2105, 2106, 2107, 2108, 2158})


class ControlAuthorityError(RuntimeError):
    """A stable control-state failure without broker/private exception text."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True, slots=True)
class ControlAuthorityBatch:
    generation: int
    verified_at: datetime
    account_rows: tuple[dict[str, object], ...]
    positions: tuple[dict[str, object], ...]
    working_orders: tuple[dict[str, object], ...]


@dataclass(slots=True)
class _Attempt:
    generation: int
    account: str
    request_id: int
    deadline: float
    future: asyncio.Future[None]
    loop: asyncio.AbstractEventLoop
    sent: set[str] = field(default_factory=set)
    ended: set[str] = field(default_factory=set)
    account_rows: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    positions: dict[int, dict[str, object]] = field(default_factory=dict)
    orders: dict[tuple[int, int, int], dict[str, object]] = field(default_factory=dict)
    failure: str | None = None


def _number(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("invalid control number")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite control number")
    return result


class ControlAuthority:
    """No connections, threads, cache reads, model authority or order writes.

    Construct before ``IB.connect`` and call ``read`` on the existing IB owner.
    Callback containers belong to one response-ended generation, not SDK caches.
    An unsuccessful no-request-id enumeration poisons this instance: late end
    callbacks cannot complete a later attempt on the same stream.
    """

    def __init__(
        self, ib: object, *, clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None, max_age_seconds: float = 5.0,
    ) -> None:
        if isinstance(max_age_seconds, bool) or not 0 < max_age_seconds <= 5:
            raise ValueError("control cache maximum age must be within five seconds")
        self._ib = ib
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic or time.monotonic
        self._max_age_seconds = max_age_seconds
        self._lock = threading.RLock()
        self._generation = 0
        self._status = "RECOVERY_PENDING"
        self._reasons = ("CONTROL_INITIAL_SYNC_REQUIRED",)
        self._batch: ControlAuthorityBatch | None = None
        self._attempt: _Attempt | None = None
        self._poisoned = False
        self._closed = False
        self._originals: dict[str, object] = {}
        self._installed: dict[str, object] = {}
        self._client_originals: dict[str, object] = {}
        self._client_installed: dict[str, object] = {}
        self._connect_seen = False
        self._upstream_event_seen = False
        self._startup_positions_sent = False
        self._startup_positions_ended = False
        self._unkeyed_requests: dict[str, tuple[str, int]] = {}
        self._sending_kind: str | None = None
        self._event_handlers: list[tuple[object, object]] = []
        self._sdk_error_handler: object | None = None
        self._sdk_error_removed = False
        try:
            wrapper = getattr(ib, "wrapper")
            for name in _HOOKS:
                original = getattr(wrapper, name)
                if not callable(original):
                    raise TypeError("unsupported response callback")
                self._originals[name] = original

                def observed(*args: object, _name: str = name, _original: object = original) -> object:
                    self._capture(_name, args)
                    return _original(*args)  # type: ignore[operator]

                self._installed[name] = observed
                setattr(wrapper, name, observed)
            client = getattr(ib, "client")
            for name, kind in (("reqPositions", "positions"), ("reqOpenOrders", "orders"),
                               ("reqAllOpenOrders", "orders")):
                original = getattr(client, name)
                if not callable(original):
                    raise TypeError("unsupported enumeration request")
                self._client_originals[name] = original

                def scoped_request(*args: object, _kind: str = kind, _original: object = original) -> object:
                    self._enumeration_started(_kind)
                    return _original(*args)  # type: ignore[operator]

                self._client_installed[name] = scoped_request
                setattr(client, name, scoped_request)
            error_event = getattr(ib, "errorEvent")
            # The pinned SDK's default 1102 callback independently requests a
            # summary. Only our generation-bound owner may resubscribe here.
            sdk_handler = getattr(ib, "_onError", None)
            if callable(sdk_handler):
                error_event -= sdk_handler
                self._sdk_error_handler = sdk_handler
                self._sdk_error_removed = True
            for name, handler in (
                ("errorEvent", self._on_error),
                ("connectedEvent", self._on_connected),
                ("disconnectedEvent", self._on_disconnected),
            ):
                event = getattr(ib, name)
                event += handler
                self._event_handlers.append((event, handler))
        except Exception:
            self.close()
            raise ControlAuthorityError("CONTROL_RESPONSE_API_UNAVAILABLE") from None

    def health(self) -> dict[str, object]:
        """Read only private state; never marshal to or call the IB owner."""
        with self._lock:
            return {
                "status": self._status, "generation": self._generation,
                "verified_at": None if self._batch is None else self._batch.verified_at.isoformat(),
                "reason_codes": list(self._reasons),
            }

    def require_ready(self) -> None:
        with self._lock:
            if self._closed or self._status != "READY":
                raise ControlAuthorityError(self._reasons[0] if self._reasons else "CONTROL_NOT_READY")

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ControlAuthorityError("CONTROL_CLOCK_INVALID")
        return value.astimezone(timezone.utc)

    def read(self, *, timeout_seconds: float) -> ControlAuthorityBatch:
        """Return one unchanged recent batch or acquire exactly three bounded reads."""
        if (isinstance(timeout_seconds, bool) or not math.isfinite(timeout_seconds)
                or not 0 < timeout_seconds <= 7):
            raise ValueError("control timeout must be within the eight-second owner window")
        now = self._now()
        with self._lock:
            if self._closed or self._status in {"LOST", "RECONNECT_REQUIRED", "DISCONNECTED"}:
                raise ControlAuthorityError(self._reasons[0])
            if not self._connect_seen:
                raise ControlAuthorityError("CONTROL_INITIAL_ENUMERATION_UNVERIFIED")
            if self._attempt is not None:
                raise ControlAuthorityError("CONTROL_SYNC_IN_PROGRESS")
            if self._status == "READY" and self._batch is not None:
                age = (now - self._batch.verified_at).total_seconds()
                if 0 <= age <= self._max_age_seconds:
                    return deepcopy(self._batch)
            generation = self._generation
        try:
            return self._ib.run(self._read_async(generation, timeout_seconds))  # type: ignore[attr-defined,no-any-return]
        except ControlAuthorityError:
            raise
        except Exception:
            self._failed("CONTROL_SYNC_FAILED")
            raise ControlAuthorityError("CONTROL_SYNC_FAILED") from None

    async def _read_async(self, generation: int, timeout_seconds: float) -> ControlAuthorityBatch:
        attempt: _Attempt | None = None
        try:
            if not self._ib.isConnected():  # type: ignore[attr-defined]
                raise ControlAuthorityError("CONTROL_SOCKET_DISCONNECTED")
            accounts = tuple(self._ib.managedAccounts())  # type: ignore[attr-defined]
            if len(accounts) != 1 or not isinstance(accounts[0], str) or not accounts[0].strip():
                raise ControlAuthorityError("CONTROL_SINGLE_ACCOUNT_REQUIRED")
            client = self._ib.client  # type: ignore[attr-defined]
            request_id = client.getReqId()
            if type(request_id) is not int or request_id < 0:
                raise ControlAuthorityError("CONTROL_REQUEST_ID_INVALID")
            loop = asyncio.get_running_loop()
            attempt = _Attempt(generation, accounts[0], request_id,
                               self._monotonic() + timeout_seconds, loop.create_future(), loop)
            with self._lock:
                self._check_generation(generation)
                self._attempt = attempt
            for kind in ("account", "positions", "orders"):
                with self._lock:
                    self._check_generation(generation)
                    if attempt.failure is not None:
                        raise ControlAuthorityError(attempt.failure)
                    if self._monotonic() >= attempt.deadline:
                        raise ControlAuthorityError("CONTROL_SYNC_TIMEOUT")
                    attempt.sent.add(kind)
                if kind == "account":
                    client.reqAccountSummary(request_id, "All", ",".join((*_ACCOUNT_TAGS, "DayTradesRemaining")))
                else:
                    self._sending_kind = kind
                    try:
                        if kind == "positions":
                            client.reqPositions()
                        else:
                            client.reqAllOpenOrders()
                    finally:
                        self._sending_kind = None
            await asyncio.wait_for(attempt.future, max(attempt.deadline - self._monotonic(), 0.001))
            with self._lock:
                self._check_generation(generation)
                if attempt.failure is not None:
                    raise ControlAuthorityError(attempt.failure)
                if attempt.ended != {"account", "positions", "orders"}:
                    raise ControlAuthorityError("CONTROL_END_ACK_MISSING")
                if self._monotonic() >= attempt.deadline:
                    raise ControlAuthorityError("CONTROL_SYNC_TIMEOUT")
                account_rows = tuple(attempt.account_rows.values())
                self._validate_account(account_rows)
                verified_at = self._now()
                batch = ControlAuthorityBatch(
                    generation, verified_at, account_rows,
                    tuple(row for row in attempt.positions.values() if _number(row["position"]) != 0),
                    tuple(attempt.orders.values()),
                )
            # Read-subscription cleanup belongs to this request, and must
            # succeed before we publish a recovered control observation.
            self._cleanup(attempt)
            with self._lock:
                self._check_generation(generation)
                if attempt.failure is not None or self._monotonic() >= attempt.deadline:
                    raise ControlAuthorityError(attempt.failure or "CONTROL_SYNC_TIMEOUT")
                self._batch = deepcopy(batch)
                self._status = "READY"
                self._reasons = ()
                return deepcopy(batch)
        except (asyncio.TimeoutError, TimeoutError):
            self._failed("CONTROL_SYNC_TIMEOUT")
            raise ControlAuthorityError("CONTROL_SYNC_TIMEOUT") from None
        except ControlAuthorityError as exc:
            self._failed(exc.reason_code)
            raise
        except Exception:
            self._failed("CONTROL_RESPONSE_INVALID")
            raise ControlAuthorityError("CONTROL_RESPONSE_INVALID") from None
        finally:
            if attempt is not None:
                try:
                    self._cleanup(attempt)
                except Exception:
                    self._failed("CONTROL_REQUEST_CLEANUP_FAILED")
                if not attempt.future.done():
                    attempt.future.cancel()
                with self._lock:
                    if self._attempt is attempt:
                        self._attempt = None

    @staticmethod
    def _validate_account(rows: tuple[dict[str, object], ...]) -> None:
        if any(sum(row["tag"] == tag for row in rows) != 1 for tag in _ACCOUNT_TAGS):
            raise ControlAuthorityError("CONTROL_ACCOUNT_FIELDS_INCOMPLETE")
        required = {str(row["tag"]): row for row in rows if row["tag"] in _ACCOUNT_TAGS}
        if set(required) != set(_ACCOUNT_TAGS):
            raise ControlAuthorityError("CONTROL_ACCOUNT_FIELDS_INCOMPLETE")
        currencies = {row["currency"] for row in required.values()}
        if len(currencies) != 1 or not all(isinstance(value, str) and value for value in currencies):
            raise ControlAuthorityError("CONTROL_ACCOUNT_CURRENCY_INVALID")
        for row in required.values():
            _number(row["value"])

    def _check_generation(self, generation: int) -> None:
        if (self._closed or self._generation != generation
                or self._status in {"LOST", "RECONNECT_REQUIRED", "DISCONNECTED"}):
            raise ControlAuthorityError("CONTROL_GENERATION_INVALIDATED")

    def _cleanup(self, attempt: _Attempt) -> None:
        # Remove before calling so even a failing cleanup is never retried.
        failed = False
        if "account" in attempt.sent:
            attempt.sent.remove("account")
            try:
                self._ib.client.cancelAccountSummary(attempt.request_id)  # type: ignore[attr-defined]
            except Exception:
                failed = True
        if "positions" in attempt.sent:
            attempt.sent.remove("positions")
            try:
                # This owner's bounded enumeration uses the connection's
                # singleton read subscription, never an order-cancel API.
                self._ib.client.cancelPositions()  # type: ignore[attr-defined]
            except Exception:
                failed = True
        if failed:
            raise ControlAuthorityError("CONTROL_REQUEST_CLEANUP_FAILED")

    def _wake(self, attempt: _Attempt) -> None:
        def settle() -> None:
            if not attempt.future.done():
                attempt.future.set_result(None)
        attempt.loop.call_soon_threadsafe(settle)

    def _capture(self, name: str, args: tuple[object, ...]) -> None:
        with self._lock:
            unkeyed_kind = "positions" if name in {"position", "positionEnd"} else "orders"
            scope = self._unkeyed_requests.get(unkeyed_kind)
            if name in {"positionEnd", "openOrderEnd"}:
                if scope is not None:
                    self._unkeyed_requests.pop(unkeyed_kind, None)
                    if scope[0] == "startup" and name == "positionEnd":
                        self._startup_positions_ended = True
            attempt = self._attempt
            if attempt is None or attempt.generation != self._generation or self._closed:
                return
            if name in {"position", "positionEnd", "openOrder", "openOrderEnd"}:
                if scope != ("helper", attempt.generation):
                    return
            try:
                if name == "accountSummary":
                    request_id, account, tag, value, currency = args
                    if request_id != attempt.request_id or "account" not in attempt.sent:
                        return
                    if account != attempt.account:
                        raise ValueError("account mismatch")
                    if not all(isinstance(item, str) for item in (tag, value, currency)):
                        raise ValueError("account field shape")
                    if len(value) > 128 or len(currency) > 8:
                        raise ValueError("account field bounds")
                    if tag not in (*_ACCOUNT_TAGS, "DayTradesRemaining"):
                        raise ValueError("unrequested account field")
                    attempt.account_rows[(tag, currency)] = dict(account=account, tag=tag, value=value, currency=currency)
                elif name == "accountSummaryEnd":
                    if args[0] == attempt.request_id and "account" in attempt.sent:
                        attempt.ended.add("account")
                elif name == "position" and "positions" in attempt.sent:
                    account, contract, position, average = args
                    if account != attempt.account:
                        raise ValueError("position account mismatch")
                    con_id = getattr(contract, "conId", None)
                    if type(con_id) is not int or con_id <= 0:
                        raise ValueError("position identity invalid")
                    _number(position)
                    _number(average)
                    attempt.positions[con_id] = dict(account=account, contract=deepcopy(contract), position=position, avgCost=average)
                    if len(attempt.positions) > 5000:
                        raise ValueError("position response limit")
                elif name == "positionEnd" and "positions" in attempt.sent:
                    attempt.ended.add("positions")
                elif name == "openOrder" and "orders" in attempt.sent:
                    order_id, contract, order, order_state = args
                    account = getattr(order, "account", None)
                    if account != attempt.account:
                        raise ValueError("order account mismatch")
                    client_id, perm_id = getattr(order, "clientId", None), getattr(order, "permId", None)
                    con_id = getattr(contract, "conId", None)
                    if (any(type(value) is not int for value in (client_id, order_id, perm_id, con_id))
                            or con_id <= 0 or client_id < 0 or perm_id < 0):
                        raise ValueError("order identity invalid")
                    _number(getattr(order, "totalQuantity", None))
                    key = (client_id, order_id, perm_id)
                    status = getattr(order_state, "status", None)
                    if not isinstance(status, str) or not status:
                        raise ValueError("order status missing")
                    attempt.orders[key] = dict(account=account, contract=deepcopy(contract), order=deepcopy(order), order_state_status=status)
                    if len(attempt.orders) > 5000:
                        raise ValueError("order response limit")
                elif name == "openOrderEnd" and "orders" in attempt.sent:
                    attempt.ended.add("orders")
                if attempt.ended == {"account", "positions", "orders"}:
                    self._wake(attempt)
            except (TypeError, ValueError, ArithmeticError, AttributeError, InvalidOperation):
                attempt.failure = "CONTROL_RESPONSE_INVALID"
                self._wake(attempt)

    def _failed(self, reason: str) -> None:
        with self._lock:
            self._poisoned = True
            if not self._closed and self._status != "LOST":
                self._status = "RECONNECT_REQUIRED"
            self._reasons = (reason,)
            if self._attempt is not None:
                self._attempt.failure = reason
                self._wake(self._attempt)

    def _enumeration_started(self, kind: str) -> None:
        with self._lock:
            if self._closed or kind in self._unkeyed_requests:
                self._failed("CONTROL_ENUMERATION_OVERLAP")
                raise ControlAuthorityError("CONTROL_ENUMERATION_OVERLAP")
            if self._attempt is not None and self._sending_kind == kind:
                self._unkeyed_requests[kind] = ("helper", self._generation)
                return
            if not self._connect_seen and kind == "positions" and not self._startup_positions_sent:
                self._startup_positions_sent = True
                self._unkeyed_requests[kind] = ("startup", self._generation)
                return
            self._failed("CONTROL_UNSCOPED_ENUMERATION_REQUEST")
            raise ControlAuthorityError("CONTROL_UNSCOPED_ENUMERATION_REQUEST")

    def _invalidate(self, status: str, reason: str) -> None:
        with self._lock:
            if self._closed:
                return
            self._generation += 1
            if self._attempt is not None:
                self._poisoned = True
                self._attempt.failure = reason
                self._wake(self._attempt)
            self._status = "RECONNECT_REQUIRED" if self._poisoned and status == "RECOVERY_PENDING" else status
            self._reasons = (reason,)

    def _on_error(self, request_id: int, code: int, *_: object) -> None:
        if code in {1100, 1101, 1102}:
            with self._lock:
                self._upstream_event_seen = True
        if code == 1100:
            self._invalidate("LOST", "IBKR_UPSTREAM_CONNECTIVITY_LOST")
        elif code in {1101, 1102}:
            self._invalidate("RECOVERY_PENDING", "IBKR_CONTROL_RESYNC_REQUIRED")
        else:
            with self._lock:
                attempt = self._attempt
                if attempt is not None and (request_id == attempt.request_id
                        or request_id < 0 and code not in _INFORMATIONAL_ERRORS):
                    attempt.failure = "CONTROL_BROKER_RESPONSE_ERROR"
                    self._wake(attempt)

    def _on_connected(self, *_: object) -> None:
        with self._lock:
            self._connect_seen = True
            if not self._startup_positions_ended or self._unkeyed_requests:
                self._failed("CONTROL_INITIAL_ENUMERATION_UNVERIFIED")
                return
            if self._upstream_event_seen:
                # Socket startup completion cannot override a server-loss or
                # recovery event observed earlier during the SDK handshake.
                return
            self._invalidate("RECOVERY_PENDING", "CONTROL_INITIAL_SYNC_REQUIRED")

    def _on_disconnected(self, *_: object) -> None:
        self._invalidate("LOST", "IBKR_SOCKET_DISCONNECTED")

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._generation += 1
            self._closed = True
            self._status = "DISCONNECTED"
            self._reasons = ("CONTROL_AUTHORITY_CLOSED",)
            if self._attempt is not None:
                self._attempt.failure = "CONTROL_AUTHORITY_CLOSED"
                self._wake(self._attempt)
        for event, handler in self._event_handlers:
            event -= handler
        self._event_handlers.clear()
        wrapper = getattr(self._ib, "wrapper", None)
        for name, installed in self._installed.items():
            if getattr(wrapper, name, None) is installed:
                setattr(wrapper, name, self._originals[name])
        client = getattr(self._ib, "client", None)
        for name, installed in self._client_installed.items():
            if getattr(client, name, None) is installed:
                setattr(client, name, self._client_originals[name])
        if self._sdk_error_removed:
            self._ib.errorEvent += self._sdk_error_handler  # type: ignore[attr-defined]
            self._sdk_error_removed = False


__all__ = ["ControlAuthority", "ControlAuthorityBatch", "ControlAuthorityError"]
