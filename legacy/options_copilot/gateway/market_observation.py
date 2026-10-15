"""Bounded, market-only IBKR observations for offline research inputs."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import threading
from types import MethodType
from typing import Protocol

from options_copilot.storage.canonical import canonical_hash, canonical_json, utc_datetime


OBSERVATION_SCHEMA = "options_copilot.ibkr_market_observation.v1"
RESEARCH_INPUT_SCHEMA = "options_copilot.ibkr_market_research_input.v1"
MAX_PRICE_TICKS_PER_CONTRACT = 256
MAX_COMPUTATION_TICKS_PER_CONTRACT = 512
MAX_OBSERVATION_BYTES = 2_000_000
_TICK_NAMES = {
    10: "bid", 11: "ask", 12: "last", 13: "model",
    80: "bid", 81: "ask", 82: "last", 83: "model",
}
_PRICE_TICK_NAMES = {1: "bid", 2: "ask", 4: "last", 66: "bid", 67: "ask", 68: "last"}
_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class OptionObservationRequest:
    symbol: str
    expiration: str
    strike: Decimal
    rights: tuple[str, ...] = ("C", "P")
    collection_seconds: float = 8.0

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        rights = tuple(str(value).strip().upper() for value in self.rights)
        if not symbol or len(symbol) > 12 or not symbol.replace(".", "").isalnum():
            raise ValueError("symbol is invalid")
        if len(self.expiration) != 8 or not self.expiration.isdigit():
            raise ValueError("expiration must be YYYYMMDD")
        if not self.strike.is_finite() or self.strike <= 0:
            raise ValueError("strike must be positive and finite")
        if not rights or len(rights) > 2 or len(set(rights)) != len(rights):
            raise ValueError("one or two unique option rights are required")
        if any(value not in {"C", "P"} for value in rights):
            raise ValueError("rights must be C or P")
        if not 1.0 <= self.collection_seconds <= 15.0:
            raise ValueError("collection_seconds must be between 1 and 15")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "rights", rights)


class MarketObservationSession(Protocol):
    error_codes: list[dict[str, int | None]]

    async def connect(self) -> None: ...
    async def qualify(self, request: OptionObservationRequest) -> list[object]: ...
    def subscribe(self, contracts: Sequence[object]) -> None: ...
    async def wait(self, seconds: float) -> None: ...
    def snapshot(self, contracts: Sequence[object]) -> list[dict[str, object]]: ...
    def cancel_all(self) -> None: ...
    def disconnect(self) -> None: ...
    def connection_document(self) -> dict[str, object]: ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _text_number(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None
    return format(parsed, "f")


def _value_valid(name: str, value: str | None) -> bool:
    if value is None:
        return False
    number = Decimal(value)
    if name == "bid":
        return number >= 0
    if name == "option_price":
        return number >= 0
    if name in {"ask", "last", "underlying_price"}:
        return number > 0
    if name == "implied_volatility":
        return Decimal("0") < number <= Decimal("5")
    if name == "delta":
        return Decimal("-1") <= number <= Decimal("1")
    if name in {"gamma", "vega"}:
        return number >= 0
    if name == "theta":
        return number != Decimal("-2")
    return True


class _Recorder:
    def __init__(self, clock: Callable[[], datetime]) -> None:
        self.clock = clock
        self.prices: dict[int, dict[str, dict[str, object]]] = {}
        self.computations: dict[int, dict[str, dict[str, object]]] = {}
        self.price_ticks: dict[int, list[dict[str, object]]] = {}
        self.computation_ticks: dict[int, list[dict[str, object]]] = {}
        self.truncated_counts: dict[int, dict[str, int]] = {}
        self.data_types: dict[int, dict[str, object]] = {}

    def _time(self, value: object) -> datetime:
        return utc_datetime(value if isinstance(value, datetime) else self.clock())

    def price(self, con_id: int, tick_type: int, value: object, received: object, market_type: int | None) -> None:
        name = _PRICE_TICK_NAMES.get(tick_type)
        if name is None:
            return
        rendered = _text_number(value)
        prior = self.prices.setdefault(con_id, {}).get(name, {})
        row = {
            "tick_type": tick_type,
            "tick_variant": "DELAYED" if tick_type in {66, 67, 68} else "LIVE_FAMILY",
            "value": rendered,
            "valid": _value_valid(name, rendered),
            "callback_received_at": self._time(received).isoformat(),
            "market_data_type": market_type,
            "source_event_time": None,
            "source_event_time_status": "UNAVAILABLE",
            "update_count": int(prior.get("update_count", 0)) + 1,
        }
        self.prices[con_id][name] = row
        self._append_history(con_id, "price", self.price_ticks, row, MAX_PRICE_TICKS_PER_CONTRACT)

    def computation(self, con_id: int, tick_type: int, values: Mapping[str, object], received: object, market_type: int | None) -> None:
        name = _TICK_NAMES.get(tick_type)
        if name is None:
            return
        rendered = {key: _text_number(value) for key, value in values.items()}
        validity = {key: _value_valid(key, value) for key, value in rendered.items()}
        prior = self.computations.setdefault(con_id, {}).get(name, {})
        row = {
            "tick_type": tick_type,
            "tick_variant": "DELAYED" if tick_type in {80, 81, 82, 83} else "LIVE_FAMILY",
            "values": rendered,
            "validity": validity,
            "valid": all(validity.values()),
            "callback_received_at": self._time(received).isoformat(),
            "market_data_type": market_type,
            "source_event_time": None,
            "source_event_time_status": "UNAVAILABLE",
            "update_count": int(prior.get("update_count", 0)) + 1,
        }
        self.computations[con_id][name] = row
        self._append_history(
            con_id, "option_computation", self.computation_ticks, row,
            MAX_COMPUTATION_TICKS_PER_CONTRACT,
        )

    def _append_history(
        self,
        con_id: int,
        kind: str,
        store: dict[int, list[dict[str, object]]],
        row: Mapping[str, object],
        maximum: int,
    ) -> None:
        history = store.setdefault(con_id, [])
        if len(history) >= maximum:
            del history[0]
            counts = self.truncated_counts.setdefault(
                con_id, {"price": 0, "option_computation": 0},
            )
            counts[kind] += 1
        history.append(dict(row))

    def data_type(self, con_id: int, value: int, received: object) -> None:
        self.data_types[con_id] = {
            "value": value,
            "callback_received_at": self._time(received).isoformat(),
        }


class IBMarketObservationSession:
    """Low-level ib-insync session that deliberately skips account synchronization."""

    def __init__(self, *, host: str, port: int, client_id: int, timeout: float = 8.0, clock: Callable[[], datetime] = _now) -> None:
        if host != "127.0.0.1" or port != 4001:
            raise ValueError("market observation is locked to 127.0.0.1:4001")
        if not 1 <= client_id <= 999999:
            raise ValueError("client_id is invalid")
        from ib_insync import IB

        self.host, self.port, self.client_id, self.timeout = host, port, client_id, timeout
        self.ib = IB()
        self.recorder = _Recorder(clock)
        self.error_codes: list[dict[str, int | None]] = []
        self._requested: list[object] = []
        self._install_callbacks()

    def _install_callbacks(self) -> None:
        wrapper = self.ib.wrapper
        original_price = wrapper.priceSizeTick
        original_computation = wrapper.tickOptionComputation
        original_type = wrapper.marketDataType

        def market_type(current: object, req_id: int) -> int | None:
            ticker = current.reqId2Ticker.get(req_id)
            value = getattr(ticker, "marketDataType", None)
            return value if type(value) is int else None

        def price(current: object, req_id: int, tick_type: int, value: float, size: float) -> None:
            original_price(req_id, tick_type, value, size)
            ticker = current.reqId2Ticker.get(req_id)
            con_id = getattr(getattr(ticker, "contract", None), "conId", None)
            if type(con_id) is int and con_id > 0:
                self.recorder.price(con_id, tick_type, value, current.lastTime, market_type(current, req_id))

        def computation(current: object, req_id: int, tick_type: int, tick_attrib: int, implied_vol: float, delta: float, option_price: float, pv_dividend: float, gamma: float, vega: float, theta: float, underlying_price: float) -> None:
            original_computation(req_id, tick_type, tick_attrib, implied_vol, delta, option_price, pv_dividend, gamma, vega, theta, underlying_price)
            ticker = current.reqId2Ticker.get(req_id)
            con_id = getattr(getattr(ticker, "contract", None), "conId", None)
            if type(con_id) is int and con_id > 0:
                self.recorder.computation(con_id, tick_type, {
                    "implied_volatility": implied_vol, "delta": delta, "gamma": gamma,
                    "vega": vega, "theta": theta, "option_price": option_price,
                    "underlying_price": underlying_price,
                }, current.lastTime, market_type(current, req_id))

        def data_type(current: object, req_id: int, value: int) -> None:
            original_type(req_id, value)
            ticker = current.reqId2Ticker.get(req_id)
            con_id = getattr(getattr(ticker, "contract", None), "conId", None)
            if type(con_id) is int and con_id > 0:
                self.recorder.data_type(con_id, value, current.lastTime)

        wrapper.priceSizeTick = MethodType(price, wrapper)
        wrapper.tickOptionComputation = MethodType(computation, wrapper)
        wrapper.marketDataType = MethodType(data_type, wrapper)

        def on_error(req_id: object, code: object, _text: object, contract: object | None = None) -> None:
            if type(code) is not int or code in {2104, 2106, 2107, 2108, 2119, 2158}:
                return
            con_id = getattr(contract, "conId", None)
            self.error_codes.append({
                "request_id": req_id if type(req_id) is int else None,
                "contract_id": con_id if type(con_id) is int and con_id > 0 else None,
                "code": code,
            })

        self.ib.errorEvent += on_error

    async def connect(self) -> None:
        await asyncio.wait_for(
            self.ib.client.connectAsync(self.host, self.port, self.client_id, timeout=self.timeout),
            timeout=self.timeout + 1.0,
        )
        if not self.ib.client.isReady():
            raise ConnectionError("IBKR_CONNECTION_NOT_READY")

    async def qualify(self, request: OptionObservationRequest) -> list[object]:
        from ib_insync import Option, Stock

        definitions = [Stock(request.symbol, "SMART", "USD")]
        definitions.extend(
            Option(request.symbol, request.expiration, float(request.strike), right, "SMART", currency="USD", multiplier="100")
            for right in request.rights
        )
        contracts = []
        for definition in definitions:
            details = await asyncio.wait_for(self.ib.reqContractDetailsAsync(definition), timeout=self.timeout)
            if len(details) != 1:
                raise ValueError("IBKR_CONTRACT_IDENTITY_NOT_UNIQUE")
            contract = details[0].contract
            if not self._identity_matches(definition, contract):
                raise ValueError("IBKR_CONTRACT_IDENTITY_MISMATCH")
            if int(contract.conId) in {int(item.conId) for item in contracts}:
                raise ValueError("IBKR_CONTRACT_IDENTITY_DUPLICATE")
            contracts.append(contract)
        return contracts

    @staticmethod
    def _identity_matches(requested: object, returned: object) -> bool:
        if type(getattr(returned, "conId", None)) is not int or returned.conId <= 0:
            return False
        common = (
            str(getattr(returned, "symbol", "")).upper() == str(getattr(requested, "symbol", "")).upper()
            and str(getattr(returned, "secType", "")).upper() == str(getattr(requested, "secType", "")).upper()
            and str(getattr(returned, "currency", "")).upper() == "USD"
            and str(getattr(returned, "exchange", "")).upper() == "SMART"
        )
        if not common:
            return False
        if str(getattr(requested, "secType", "")).upper() == "STK":
            return True
        try:
            same_strike = Decimal(str(returned.strike)) == Decimal(str(requested.strike))
        except (AttributeError, InvalidOperation):
            return False
        return (
            str(getattr(returned, "lastTradeDateOrContractMonth", "")) == str(getattr(requested, "lastTradeDateOrContractMonth", ""))
            and same_strike
            and str(getattr(returned, "right", "")).upper() == str(getattr(requested, "right", "")).upper()
            and str(getattr(returned, "multiplier", "")) == "100"
        )

    def subscribe(self, contracts: Sequence[object]) -> None:
        self.ib.reqMarketDataType(1)
        for contract in contracts:
            ticks = "106" if getattr(contract, "secType", None) == "OPT" else ""
            self.ib.reqMktData(contract, ticks, False, False)
            self._requested.append(contract)

    async def wait(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def snapshot(self, contracts: Sequence[object]) -> list[dict[str, object]]:
        rows = []
        for contract in contracts:
            con_id = int(contract.conId)
            identity = {
                "contract_id": con_id, "symbol": contract.symbol,
                "security_type": contract.secType, "currency": contract.currency,
                "exchange": contract.exchange, "primary_exchange": contract.primaryExchange or None,
                "expiration": contract.lastTradeDateOrContractMonth or None,
                "strike": _text_number(contract.strike), "right": contract.right or None,
                "trading_class": contract.tradingClass or None, "multiplier": contract.multiplier or None,
            }
            rows.append({
                "contract": identity,
                "market_data_type": self.recorder.data_types.get(con_id),
                "prices": self.recorder.prices.get(con_id, {}),
                "price_tick_history": self.recorder.price_ticks.get(con_id, []),
                "option_computations": self.recorder.computations.get(con_id, {}),
                "option_computation_tick_history": self.recorder.computation_ticks.get(con_id, []),
                "history_limits": {
                    "price": MAX_PRICE_TICKS_PER_CONTRACT,
                    "option_computation": MAX_COMPUTATION_TICKS_PER_CONTRACT,
                },
                "history_truncated": self.recorder.truncated_counts.get(
                    con_id, {"price": 0, "option_computation": 0},
                ),
                "asynchronous_callbacks": True,
                "atomic_source_snapshot": False,
            })
        return rows

    def cancel_all(self) -> None:
        failures = 0
        for contract in self._requested:
            try:
                self.ib.cancelMktData(contract)
            except Exception:
                failures += 1
        self._requested.clear()
        if failures:
            raise RuntimeError("IBKR_MARKET_DATA_CANCEL_FAILED")

    def disconnect(self) -> None:
        if self.ib.client.isConnected():
            self.ib.client.disconnect()

    def connection_document(self) -> dict[str, object]:
        return {
            "host": self.host, "port": self.port, "client_id": self.client_id,
            "readonly": True, "market_data_only": True,
            "server_version": int(self.ib.client.serverVersion()),
        }


def collect_market_observation(
    request: OptionObservationRequest,
    *,
    session_factory: Callable[[], MarketObservationSession],
    clock: Callable[[], datetime] = _now,
) -> dict[str, object]:
    """Collect once and always cancel subscriptions and disconnect."""

    started = utc_datetime(clock())
    session = session_factory()
    contracts: list[object] = []
    status, reasons = "UNAVAILABLE", []
    rows: list[dict[str, object]] = []
    connection: dict[str, object] = {"readonly": True, "market_data_only": True}
    if not _LOCK.acquire(blocking=False):
        return _finish_observation(request, started, utc_datetime(clock()), connection, [], [], "UNAVAILABLE", ["COLLECTOR_BUSY"])
    try:
        async def run() -> None:
            nonlocal contracts, rows, connection
            try:
                await session.connect()
                connection = session.connection_document()
                contracts = await session.qualify(request)
                session.subscribe(contracts)
                await session.wait(request.collection_seconds)
                rows = session.snapshot(contracts)
            finally:
                # ib-insync owns an asyncio transport; close it before
                # asyncio.run() tears down that loop.
                try:
                    session.cancel_all()
                except Exception:
                    reasons.append("IBKR_MARKET_OBSERVATION_CANCEL_FAILED")
                try:
                    session.disconnect()
                except Exception:
                    reasons.append("IBKR_MARKET_OBSERVATION_DISCONNECT_FAILED")

        try:
            asyncio.run(run())
            option_rows = [row for row in rows if row["contract"]["security_type"] == "OPT"]
            complete = []
            live = []
            for row in option_rows:
                prices = row.get("prices", {})
                computations = row.get("option_computations", {})
                required = [prices.get(name) for name in ("bid", "ask", "last")]
                required.extend(computations.get(name) for name in ("bid", "ask", "last", "model"))
                complete.append(all(isinstance(value, Mapping) and value.get("valid") is True for value in required))
                live.append(all(isinstance(value, Mapping) and value.get("market_data_type") == 1 for value in required))
            status = "OBSERVED" if option_rows and all(complete) and all(live) else "PARTIAL"
            if not option_rows or not all(complete):
                reasons.append("OPTION_COMPUTATION_INCOMPLETE")
            if option_rows and not all(live):
                reasons.append("MARKET_DATA_NOT_LIVE")
            if any(
                any(int(value) > 0 for value in row.get("history_truncated", {}).values())
                for row in rows
            ):
                status = "PARTIAL"
                reasons.append("MARKET_DATA_HISTORY_TRUNCATED")
        except TimeoutError:
            reasons.append("IBKR_MARKET_OBSERVATION_TIMEOUT")
        except ConnectionError:
            reasons.append("IBKR_MARKET_OBSERVATION_DISCONNECTED")
        except ValueError as exc:
            reason = str(exc)
            reasons.append(reason if reason.isupper() and " " not in reason else "IBKR_MARKET_OBSERVATION_INVALID")
        except Exception:
            reasons.append("IBKR_MARKET_OBSERVATION_FAILED")
    finally:
        _LOCK.release()
    codes = list(session.error_codes)
    if any(row.get("code") == 326 for row in codes):
        reasons.append("IBKR_CLIENT_ID_IN_USE")
    completed = utc_datetime(clock())
    return _finish_observation(request, started, completed, connection, rows, codes, status, reasons)


def _finish_observation(request: OptionObservationRequest, started: datetime, completed: datetime, connection: Mapping[str, object], rows: Sequence[Mapping[str, object]], codes: Sequence[Mapping[str, object]], status: str, reasons: Sequence[str]) -> dict[str, object]:
    safe_connection = {
        name: connection.get(name)
        for name in (
            "host", "port", "client_id", "readonly", "market_data_only",
            "server_version",
        )
        if name in connection
    }
    body = {
        "schema": OBSERVATION_SCHEMA, "status": status,
        "scope": "BOUNDED_MARKET_DATA_ONLY", "source": "IBKR",
        "requested_at": started.isoformat(), "completed_at": completed.isoformat(),
        "request": {"symbol": request.symbol, "expiration": request.expiration, "strike": format(request.strike, "f"), "rights": list(request.rights), "collection_seconds": request.collection_seconds},
        "connection": safe_connection, "contracts": list(rows), "broker_error_codes": list(codes),
        "reason_codes": list(dict.fromkeys(reasons)), "decision_authority": "OBSERVATION_ONLY",
        "production_eligible": False, "approval_eligible": False,
        "instruction_creation_allowed": False, "source_event_time_available": False,
        "atomic_source_snapshot": False, "output_size_limit_bytes": MAX_OBSERVATION_BYTES,
    }
    result = {**body, "content_hash": canonical_hash(body)}
    if len(canonical_json(result).encode("utf-8")) <= MAX_OBSERVATION_BYTES:
        return result
    bounded_body = {
        **body,
        "status": "UNAVAILABLE",
        "contracts": [],
        "broker_error_codes": [],
        "reason_codes": ["MARKET_OBSERVATION_SIZE_LIMIT_EXCEEDED"],
        "discarded_counts": {
            "contracts": len(rows),
            "broker_errors": len(codes),
        },
    }
    bounded = {**bounded_body, "content_hash": canonical_hash(bounded_body)}
    if len(canonical_json(bounded).encode("utf-8")) > MAX_OBSERVATION_BYTES:
        raise RuntimeError("MARKET_OBSERVATION_BOUNDED_FAILURE_INVALID")
    return bounded


def build_research_market_input(observation: Mapping[str, object], *, now: datetime | None = None, max_age_seconds: float = 15.0) -> dict[str, object]:
    """Project an observation for research while retaining every uncertainty."""

    checked_at = utc_datetime(now or _now())
    reasons: list[str] = []
    if not isinstance(observation, Mapping) or observation.get("schema") != OBSERVATION_SCHEMA:
        raise ValueError("MARKET_OBSERVATION_SCHEMA_INVALID")
    supplied_hash = observation.get("content_hash")
    if not isinstance(supplied_hash, str) or len(supplied_hash) != 64:
        raise ValueError("MARKET_OBSERVATION_HASH_INVALID")
    source_body = dict(observation)
    source_body.pop("content_hash", None)
    if canonical_hash(source_body) != supplied_hash:
        raise ValueError("MARKET_OBSERVATION_HASH_MISMATCH")
    if len(canonical_json(observation).encode("utf-8")) > MAX_OBSERVATION_BYTES:
        raise ValueError("MARKET_OBSERVATION_SIZE_INVALID")
    if (
        observation.get("scope") != "BOUNDED_MARKET_DATA_ONLY"
        or observation.get("source") != "IBKR"
        or observation.get("decision_authority") != "OBSERVATION_ONLY"
        or observation.get("production_eligible") is not False
        or observation.get("approval_eligible") is not False
        or observation.get("instruction_creation_allowed") is not False
        or observation.get("source_event_time_available") is not False
        or observation.get("atomic_source_snapshot") is not False
    ):
        raise ValueError("MARKET_OBSERVATION_AUTHORITY_INVALID")
    try:
        requested_at = utc_datetime(datetime.fromisoformat(str(observation["requested_at"])))
        completed_at = utc_datetime(datetime.fromisoformat(str(observation["completed_at"])))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("MARKET_OBSERVATION_TIME_INVALID") from exc
    if requested_at > completed_at or completed_at > checked_at:
        raise ValueError("MARKET_OBSERVATION_TIME_INVALID")
    source_status = observation.get("status")
    if source_status not in {"OBSERVED", "PARTIAL", "UNAVAILABLE"}:
        raise ValueError("MARKET_OBSERVATION_STATUS_INVALID")
    if source_status != "OBSERVED":
        reasons.append(f"SOURCE_OBSERVATION_{source_status}")
    raw_contracts = observation.get("contracts")
    if not isinstance(raw_contracts, Sequence) or isinstance(raw_contracts, (str, bytes)):
        raise ValueError("MARKET_OBSERVATION_CONTRACTS_INVALID")
    _validate_contract_topology(
        observation.get("request"), raw_contracts,
        allow_empty=source_status == "UNAVAILABLE",
    )
    contracts = []
    for raw in raw_contracts:
        if not isinstance(raw, Mapping):
            raise ValueError("MARKET_OBSERVATION_CONTRACT_INVALID")
        timestamps = []
        missing = []
        stale = []
        non_live = []
        fields = []
        market_fields: dict[str, object] = {
            "prices": {}, "option_computations": {},
            "price_tick_history": [], "option_computation_tick_history": [],
        }
        prices = raw.get("prices", {})
        computations = raw.get("option_computations", {})
        data_types: list[int] = []
        market_type = raw.get("market_data_type")
        if isinstance(market_type, Mapping):
            _receipt_time(
                market_type.get("callback_received_at"), requested_at,
                completed_at, checked_at,
            )
            if type(market_type.get("value")) is int:
                data_types.append(int(market_type["value"]))
        required = ("bid", "ask", "last")
        for name in required:
            row = prices.get(name) if isinstance(prices, Mapping) else None
            if not isinstance(row, Mapping):
                missing.append(name)
                continue
            market_fields["prices"][name] = _project_price_field(row)
            received_at = _receipt_time(
                row.get("callback_received_at"), requested_at,
                completed_at, checked_at,
            )
            timestamps.append(received_at.isoformat())
            if (checked_at - received_at).total_seconds() > max_age_seconds:
                stale.append(name)
            if row.get("market_data_type") != 1:
                non_live.append(name)
            if type(row.get("market_data_type")) is int:
                data_types.append(int(row["market_data_type"]))
            if row.get("valid") is True:
                fields.append(name)
            else:
                missing.append(name)
        security_type = raw.get("contract", {}).get("security_type") if isinstance(raw.get("contract"), Mapping) else None
        if security_type == "OPT":
            for name in ("bid", "ask", "last", "model"):
                row = computations.get(name) if isinstance(computations, Mapping) else None
                label = f"option_computation.{name}"
                if not isinstance(row, Mapping):
                    missing.append(label)
                    continue
                market_fields["option_computations"][name] = _project_computation_field(row)
                received_at = _receipt_time(
                    row.get("callback_received_at"), requested_at,
                    completed_at, checked_at,
                )
                timestamps.append(received_at.isoformat())
                if (checked_at - received_at).total_seconds() > max_age_seconds:
                    stale.append(label)
                if row.get("market_data_type") != 1:
                    non_live.append(label)
                if type(row.get("market_data_type")) is int:
                    data_types.append(int(row["market_data_type"]))
                if row.get("valid") is True:
                    fields.append(label)
                else:
                    missing.append(label)
        histories = (
            ("price_tick_history", MAX_PRICE_TICKS_PER_CONTRACT, _project_price_field),
            (
                "option_computation_tick_history",
                MAX_COMPUTATION_TICKS_PER_CONTRACT,
                _project_computation_field,
            ),
        )
        delayed_history = False
        for history_name, maximum, projector in histories:
            history = raw.get(history_name)
            if (
                not isinstance(history, Sequence)
                or isinstance(history, (str, bytes))
                or len(history) > maximum
            ):
                raise ValueError("MARKET_OBSERVATION_HISTORY_INVALID")
            projected_history = market_fields[history_name]
            for history_row in history:
                if not isinstance(history_row, Mapping):
                    raise ValueError("MARKET_OBSERVATION_HISTORY_INVALID")
                received_at = _receipt_time(
                    history_row.get("callback_received_at"), requested_at,
                    completed_at, checked_at,
                )
                timestamps.append(received_at.isoformat())
                projected_history.append(projector(history_row))
                tick_type = history_row.get("tick_type")
                delayed_history = delayed_history or tick_type in {
                    66, 67, 68, 80, 81, 82, 83,
                }
                value = history_row.get("market_data_type")
                if type(value) is int:
                    data_types.append(int(value))
            if source_status == "OBSERVED" and history_name == "price_tick_history" and not history:
                raise ValueError("MARKET_OBSERVATION_HISTORY_INVALID")
            if (
                source_status == "OBSERVED"
                and security_type == "OPT"
                and history_name == "option_computation_tick_history"
                and not history
            ):
                raise ValueError("MARKET_OBSERVATION_HISTORY_INVALID")
        limits = raw.get("history_limits")
        truncated = raw.get("history_truncated")
        if (
            not isinstance(limits, Mapping)
            or limits.get("price") != MAX_PRICE_TICKS_PER_CONTRACT
            or limits.get("option_computation") != MAX_COMPUTATION_TICKS_PER_CONTRACT
            or not isinstance(truncated, Mapping)
            or type(truncated.get("price")) is not int
            or type(truncated.get("option_computation")) is not int
            or int(truncated["price"]) < 0
            or int(truncated["option_computation"]) < 0
        ):
            raise ValueError("MARKET_OBSERVATION_HISTORY_INVALID")
        history_truncated = {
            "price": int(truncated["price"]),
            "option_computation": int(truncated["option_computation"]),
        }
        mixed_data = len(set(data_types)) > 1
        if mixed_data:
            reasons.append("RESEARCH_MARKET_DATA_MIXED")
        if any(value != 1 for value in data_types):
            reasons.append("RESEARCH_MARKET_DATA_NOT_LIVE")
        if delayed_history:
            reasons.extend((
                "RESEARCH_MARKET_DATA_DELAYED_HISTORY",
                "RESEARCH_MARKET_DATA_NOT_LIVE",
            ))
        if any(history_truncated.values()):
            reasons.append("RESEARCH_MARKET_HISTORY_TRUNCATED")
        if missing:
            reasons.append("RESEARCH_MARKET_FIELDS_MISSING")
        if stale:
            reasons.append("RESEARCH_MARKET_FIELDS_STALE")
        if non_live:
            reasons.append("RESEARCH_MARKET_DATA_NOT_LIVE")
        if len(set(timestamps)) > 1:
            reasons.append("RESEARCH_MARKET_CALLBACKS_ASYNCHRONOUS")
        contracts.append({
            "contract": _project_contract(raw.get("contract")),
            "market_fields": market_fields, "observed_fields": fields,
            "missing_or_invalid_fields": missing, "stale_fields": stale,
            "non_live_fields": non_live, "distinct_callback_receipt_times": sorted(set(timestamps)),
            "market_data_types_observed": sorted(set(data_types)),
            "mixed_market_data_types": mixed_data,
            "history_truncated": history_truncated,
            "source_event_time": None, "source_event_time_status": "UNAVAILABLE",
            "atomic_source_snapshot": False,
        })
    body = {
        "schema": RESEARCH_INPUT_SCHEMA,
        "status": "RESEARCH_READY" if source_status == "OBSERVED" and contracts and not any(code in reasons for code in {"RESEARCH_MARKET_FIELDS_MISSING", "RESEARCH_MARKET_FIELDS_STALE", "RESEARCH_MARKET_DATA_NOT_LIVE", "RESEARCH_MARKET_DATA_MIXED", "RESEARCH_MARKET_DATA_DELAYED_HISTORY", "RESEARCH_MARKET_HISTORY_TRUNCATED"}) else "RESEARCH_DEGRADED",
        "checked_at": checked_at.isoformat(), "observation_hash": observation.get("content_hash"),
        "contracts": contracts, "reason_codes": list(dict.fromkeys(reasons + ["SOURCE_EVENT_TIME_UNAVAILABLE", "NOT_AN_ATOMIC_SOURCE_SNAPSHOT"])),
        "decision_authority": "OBSERVATION_ONLY", "production_eligible": False,
        "approval_eligible": False, "instruction_creation_allowed": False,
    }
    return {**body, "content_hash": canonical_hash(body)}


def _receipt_time(
    value: object,
    requested_at: datetime,
    completed_at: datetime,
    checked_at: datetime,
) -> datetime:
    try:
        parsed = utc_datetime(datetime.fromisoformat(str(value)))
    except (TypeError, ValueError) as exc:
        raise ValueError("MARKET_OBSERVATION_FIELD_TIME_INVALID") from exc
    if not requested_at <= parsed <= completed_at or parsed > checked_at:
        raise ValueError("MARKET_OBSERVATION_FIELD_TIME_INVALID")
    return parsed


def _validate_contract_topology(
    request: object,
    raw_contracts: Sequence[object],
    *,
    allow_empty: bool = False,
) -> None:
    if not isinstance(request, Mapping):
        raise ValueError("MARKET_OBSERVATION_REQUEST_INVALID")
    symbol = request.get("symbol")
    expiration = request.get("expiration")
    strike = request.get("strike")
    rights = request.get("rights")
    if (
        not isinstance(symbol, str)
        or not isinstance(expiration, str)
        or not isinstance(strike, str)
        or not isinstance(rights, Sequence)
        or isinstance(rights, (str, bytes))
    ):
        raise ValueError("MARKET_OBSERVATION_REQUEST_INVALID")
    checked_rights = tuple(str(value) for value in rights)
    if (
        not checked_rights
        or len(checked_rights) > 2
        or len(set(checked_rights)) != len(checked_rights)
        or any(value not in {"C", "P"} for value in checked_rights)
    ):
        raise ValueError("MARKET_OBSERVATION_TOPOLOGY_INVALID")
    if allow_empty and not raw_contracts:
        return
    if len(raw_contracts) != 1 + len(checked_rights):
        raise ValueError("MARKET_OBSERVATION_TOPOLOGY_INVALID")
    try:
        requested_strike = Decimal(strike)
    except InvalidOperation as exc:
        raise ValueError("MARKET_OBSERVATION_REQUEST_INVALID") from exc
    con_ids: set[int] = set()
    stocks = 0
    found_rights: set[str] = set()
    for raw in raw_contracts:
        if not isinstance(raw, Mapping) or not isinstance(raw.get("contract"), Mapping):
            raise ValueError("MARKET_OBSERVATION_CONTRACT_INVALID")
        contract = raw["contract"]
        con_id = contract.get("contract_id")
        if type(con_id) is not int or con_id <= 0 or con_id in con_ids:
            raise ValueError("MARKET_OBSERVATION_TOPOLOGY_INVALID")
        con_ids.add(con_id)
        if (
            contract.get("symbol") != symbol
            or contract.get("currency") != "USD"
            or contract.get("exchange") != "SMART"
        ):
            raise ValueError("MARKET_OBSERVATION_CONTRACT_IDENTITY_INVALID")
        security_type = contract.get("security_type")
        if security_type == "STK":
            stocks += 1
            if contract.get("right") is not None or contract.get("expiration") is not None:
                raise ValueError("MARKET_OBSERVATION_CONTRACT_IDENTITY_INVALID")
            continue
        if security_type != "OPT":
            raise ValueError("MARKET_OBSERVATION_CONTRACT_IDENTITY_INVALID")
        right = contract.get("right")
        try:
            actual_strike = Decimal(str(contract.get("strike")))
        except InvalidOperation as exc:
            raise ValueError("MARKET_OBSERVATION_CONTRACT_IDENTITY_INVALID") from exc
        if (
            right not in checked_rights
            or right in found_rights
            or contract.get("expiration") != expiration
            or actual_strike != requested_strike
            or contract.get("multiplier") != "100"
        ):
            raise ValueError("MARKET_OBSERVATION_CONTRACT_IDENTITY_INVALID")
        found_rights.add(str(right))
    if stocks != 1 or found_rights != set(checked_rights):
        raise ValueError("MARKET_OBSERVATION_TOPOLOGY_INVALID")


def _project_contract(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("MARKET_OBSERVATION_CONTRACT_INVALID")
    names = (
        "contract_id", "symbol", "security_type", "currency", "exchange",
        "primary_exchange", "expiration", "strike", "right", "trading_class", "multiplier",
    )
    return {name: value.get(name) for name in names}


def _project_price_field(value: Mapping[str, object]) -> dict[str, object]:
    return {
        "tick_type": value.get("tick_type"), "tick_variant": value.get("tick_variant"),
        "value": value.get("value"), "valid": value.get("valid") is True,
        "callback_received_at": value.get("callback_received_at"),
        "market_data_type": value.get("market_data_type"),
        "source_event_time": None, "source_event_time_status": "UNAVAILABLE",
    }


def _project_computation_field(value: Mapping[str, object]) -> dict[str, object]:
    raw_values = value.get("values") if isinstance(value.get("values"), Mapping) else {}
    raw_validity = value.get("validity") if isinstance(value.get("validity"), Mapping) else {}
    names = (
        "implied_volatility", "delta", "gamma", "vega", "theta",
        "option_price", "underlying_price",
    )
    return {
        "tick_type": value.get("tick_type"), "tick_variant": value.get("tick_variant"),
        "values": {name: raw_values.get(name) for name in names},
        "validity": {name: raw_validity.get(name) is True for name in names},
        "valid": value.get("valid") is True,
        "callback_received_at": value.get("callback_received_at"),
        "market_data_type": value.get("market_data_type"),
        "source_event_time": None, "source_event_time_status": "UNAVAILABLE",
    }


__all__ = [
    "IBMarketObservationSession", "MarketObservationSession", "OBSERVATION_SCHEMA",
    "OptionObservationRequest", "RESEARCH_INPUT_SCHEMA", "build_research_market_input",
    "collect_market_observation",
]
