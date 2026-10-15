"""One bounded native historical request on an existing read-only SDK owner."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import math
import re

from options_copilot.history_source_contracts import (
    MAXIMUM_HISTORY_ROWS,
    NativeHistoryContractError,
    make_native_history_result,
    native_history_session_date,
    native_history_wire_parameters,
    validate_native_history_request,
)


class _ReadBlocked(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def cached_native_identity(contract: object) -> dict[str, object]:
    """Project only the existing cache identity; never qualify or resolve it."""

    return {
        "con_id": getattr(contract, "conId", None), "symbol": getattr(contract, "symbol", None),
        "sec_type": getattr(contract, "secType", None), "currency": getattr(contract, "currency", None),
        "exchange": str(getattr(contract, "exchange", "") or ""),
        "primary_exchange": str(getattr(contract, "primaryExchange", "") or ""),
    }


def _number(raw: object) -> str | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float, Decimal, str)):
        return None
    try:
        value = Decimal(str(raw))
        if (not value.is_finite() or len(value.as_tuple().digits) > 64
                or not -64 <= value.as_tuple().exponent <= 64):
            return None
        return str(value)
    except (InvalidOperation, ValueError):
        return None


def _bar(raw: object, prepared: Mapping[str, object]) -> dict[str, object]:
    raw_date = getattr(raw, "date", None)
    if isinstance(raw_date, (date, datetime)):
        date_text = raw_date.isoformat()
    elif isinstance(raw_date, str):
        date_text = "".join(c for c in raw_date[:40] if ord(c) >= 32)
    else:
        date_text = ""
    session = native_history_session_date(date_text)
    values = {name: _number(getattr(raw, name, None)) for name in ("open", "high", "low", "close", "volume")}
    close = None if values["close"] is None else Decimal(values["close"])
    return {
        "raw_date": date_text, "session_date": None if session is None else session.isoformat(), **values,
        "valid_close": bool(close is not None and close > 0 and (prepared["kind"] != "IV_HISTORY" or close <= Decimal("5"))),
        "date_eligible": bool(session is not None and session <= date.fromisoformat(prepared["cutoff"]["completed_session"])),
    }


def read_native_history_on_owner(
    ib: object, prepared_request: Mapping[str, object], *,
    clock: Callable[[], datetime], monotonic: Callable[[], float],
    upstream_health: Callable[[], Mapping[str, object]], identity_matches: Callable[[], bool],
    historical_lease_factory: Callable[[], AbstractContextManager[object]] | None,
    before_send: Callable[[], str], operation_guard: Callable[[], bool],
    remaining_seconds: Callable[[], float],
) -> dict[str, object]:
    """Guard, pace, claim once, then observe reqId-bound raw bars/end/errors."""

    prepared = validate_native_history_request(prepared_request)
    requested_at = clock()
    if datetime.fromisoformat(prepared["prepared_at"]) > requested_at:
        raise NativeHistoryContractError("NATIVE_HISTORY_PREPARATION_NOT_YET_AVAILABLE")
    initial_health = upstream_health()
    generation = initial_health["generation"]
    response: dict[str, object] = {
        "broker_request_id": None, "send_state": "NOT_SENT", "response_end_received": False,
        "response_end": None, "wire_parameters": native_history_wire_parameters(prepared),
        "start_generation": generation, "end_generation": generation, "broker_error_codes": [],
        "bars": [], "received_bar_count": 0, "timed_out": False, "epoch_valid": True,
    }
    reasons: list[str] = []
    claim_id: str | None = None
    deadline = monotonic() + 3.0

    def remaining() -> float:
        value = remaining_seconds()
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise _ReadBlocked("NATIVE_HISTORY_DEADLINE_INVALID")
        bounded = min(float(value), deadline - monotonic(), 3.0)
        if bounded <= 0:
            raise _ReadBlocked("NATIVE_HISTORY_DEADLINE_EXPIRED")
        return bounded

    def epoch_matches() -> bool:
        health = upstream_health()
        return bool(health["generation"] == generation and health["status"] == "READY" and ib.isConnected())  # type: ignore[attr-defined]

    def require_guard() -> None:
        try:
            if operation_guard() is not True:
                raise _ReadBlocked("NATIVE_HISTORY_OPERATION_INACTIVE")
        except _ReadBlocked:
            raise
        except Exception:
            raise _ReadBlocked("NATIVE_HISTORY_OPERATION_INACTIVE") from None
        if not epoch_matches():
            raise _ReadBlocked("NATIVE_HISTORY_EPOCH_INVALIDATED")
        if identity_matches() is not True:
            raise _ReadBlocked("NATIVE_HISTORY_CACHED_IDENTITY_CHANGED")
        remaining()

    async def request_once() -> None:
        nonlocal claim_id
        client = ib.client  # type: ignore[attr-defined]
        wrapper = ib.wrapper  # type: ignore[attr-defined]
        loop = asyncio.get_running_loop()
        finished = loop.create_future()
        originals: dict[str, object] = {}
        installed: dict[str, object] = {}
        active = True
        handler_attached = False
        request_id = client.getReqId()
        if type(request_id) is not int or request_id < 0:
            raise _ReadBlocked("NATIVE_HISTORY_REQUEST_ID_INVALID")
        response["broker_request_id"] = request_id

        def wake() -> None:
            def settle() -> None:
                if not finished.done():
                    finished.set_result(None)
            loop.call_soon_threadsafe(settle)

        def error(req_id: object, code: object, *_args: object) -> None:
            if not active or type(code) is not int or code < 0:
                return
            if req_id != request_id and code not in {1100, 1101, 1102}:
                return
            if response["send_state"] in {"NOT_SENT", "INTENT_RECORDED"} and code not in {1100, 1101, 1102}:
                return
            codes = response["broker_error_codes"]
            if code not in codes and len(codes) < 32:
                codes.append(code)
            wake()

        def capture(name: str, args: tuple[object, ...]) -> None:
            if (not active or not args or args[0] != request_id
                    or response["send_state"] not in {"DISPATCHED", "DISPATCH_UNCERTAIN"}):
                return
            if name == "historicalData":
                if response["response_end_received"]:
                    reasons.append("NATIVE_HISTORY_RESPONSE_AFTER_END")
                    wake()
                    return
                response["received_bar_count"] += 1
                if len(response["bars"]) < MAXIMUM_HISTORY_ROWS:
                    response["bars"].append(_bar(args[1], prepared))
            else:
                if response["response_end_received"]:
                    reasons.append("NATIVE_HISTORY_DUPLICATE_END")
                response["response_end_received"] = True
                response["response_end"] = {
                    "start": "".join(c for c in str(args[1])[:80] if ord(c) >= 32),
                    "end": "".join(c for c in str(args[2])[:80] if ord(c) >= 32),
                }
                wake()

        try:
            for name in ("historicalData", "historicalDataEnd"):
                original = getattr(wrapper, name)
                if not callable(original):
                    raise _ReadBlocked("NATIVE_HISTORY_RESPONSE_API_UNAVAILABLE")
                originals[name] = original

                def observed(*args: object, _name: str = name, _original: object = original) -> object:
                    owned = active and bool(args) and args[0] == request_id
                    if not owned:
                        return _original(*args)  # type: ignore[operator]
                    try:
                        capture(_name, args)
                        return _original(*args)  # type: ignore[operator]
                    except Exception:
                        reasons.append("NATIVE_HISTORY_RESPONSE_INVALID")
                        wake()
                        return None

                installed[name] = observed
                setattr(wrapper, name, observed)
            error_event = ib.errorEvent  # type: ignore[attr-defined]
            error_event += error
            handler_attached = True
            from ib_insync import Contract

            identity = prepared["contract"]
            contract = Contract(
                conId=identity["con_id"], symbol=identity["symbol"], secType=identity["sec_type"],
                currency=identity["currency"], exchange=identity["exchange"], primaryExchange=identity["primary_exchange"],
            )
            wire = response["wire_parameters"]
            require_guard()
            # The permit records one durable intent.  It is never called again,
            # including when a later guard rejects the already-recorded intent.
            try:
                permit = before_send()
            except Exception:
                raise _ReadBlocked("NATIVE_HISTORY_SEND_CLAIM_FAILED") from None
            if not isinstance(permit, str) or re.fullmatch(r"[A-Za-z0-9._:-]{1,160}", permit) is None:
                raise _ReadBlocked("NATIVE_HISTORY_SEND_CLAIM_INVALID")
            claim_id = permit
            response["send_state"] = "INTENT_RECORDED"
            require_guard()
            response["send_state"] = "DISPATCH_UNCERTAIN"
            try:
                client.reqHistoricalData(
                    request_id, contract, wire["endDateTime"], wire["durationStr"], wire["barSizeSetting"],
                    wire["whatToShow"], wire["useRTH"], wire["formatDate"], wire["keepUpToDate"], [],
                )
            except Exception:
                raise _ReadBlocked("NATIVE_HISTORY_WIRE_FAILED") from None
            response["send_state"] = "DISPATCHED"
            try:
                await asyncio.wait_for(finished, timeout=remaining())
            except _ReadBlocked as exc:
                if exc.reason == "NATIVE_HISTORY_DEADLINE_EXPIRED":
                    response["timed_out"] = True
                    reasons.append("NATIVE_HISTORY_TIMEOUT")
                raise
            except asyncio.TimeoutError:
                response["timed_out"] = True
                reasons.append("NATIVE_HISTORY_TIMEOUT")
            if not response["timed_out"]:
                require_guard()
        finally:
            active = False
            if response["send_state"] in {"DISPATCHED", "DISPATCH_UNCERTAIN"} and not response["response_end_received"]:
                try:
                    client.cancelHistoricalData(request_id)
                except Exception:
                    reasons.append("NATIVE_HISTORY_CLEANUP_FAILED")
            if handler_attached:
                try:
                    error_event -= error
                except Exception:
                    reasons.append("NATIVE_HISTORY_CLEANUP_FAILED")
            for name, installed_method in installed.items():
                if getattr(wrapper, name, None) is installed_method:
                    setattr(wrapper, name, originals[name])
            if not finished.done():
                finished.cancel()

    try:
        require_guard()
        if historical_lease_factory is None:
            raise _ReadBlocked("NATIVE_HISTORY_PACING_UNAVAILABLE")
        with historical_lease_factory() as decision:
            if getattr(decision, "allowed", None) is not True:
                raise _ReadBlocked("NATIVE_HISTORY_PACING_DENIED")
            require_guard()
            ib.run(request_once())  # type: ignore[attr-defined]
    except _ReadBlocked as exc:
        reasons.append(exc.reason)
    except Exception:
        reasons.append("NATIVE_HISTORY_REQUEST_FAILED")

    final_health = upstream_health()
    response["end_generation"] = final_health["generation"]
    response["epoch_valid"] = epoch_matches()
    if not response["epoch_valid"]:
        reasons.append("NATIVE_HISTORY_EPOCH_INVALIDATED")
    if response["broker_error_codes"]:
        reasons.append("NATIVE_HISTORY_BROKER_ERROR")
    if response["send_state"] in {"DISPATCHED", "DISPATCH_UNCERTAIN"} and not response["response_end_received"]:
        reasons.append("NATIVE_HISTORY_END_UNVERIFIED")
    bars = response["bars"]
    if response["received_bar_count"] > MAXIMUM_HISTORY_ROWS:
        reasons.append("NATIVE_HISTORY_ROW_LIMIT_EXCEEDED")
    if any(not row["valid_close"] for row in bars):
        reasons.append("NATIVE_HISTORY_INVALID_BARS")
    if any(not row["date_eligible"] for row in bars):
        reasons.append("NATIVE_HISTORY_DATE_EXCLUDED")
    dates = [row["session_date"] for row in bars if row["valid_close"] and row["date_eligible"]]
    if len(dates) != len(set(dates)):
        reasons.append("NATIVE_HISTORY_DUPLICATE_DATES")
    if not dates and response["send_state"] in {"DISPATCHED", "DISPATCH_UNCERTAIN"}:
        reasons.append("NATIVE_HISTORY_EMPTY_ROWS")
    return make_native_history_result(
        prepared, claim_id=claim_id, requested_at=requested_at, available_at=clock(),
        response=response, reason_codes=reasons,
    )


__all__ = ["cached_native_identity", "read_native_history_on_owner"]
