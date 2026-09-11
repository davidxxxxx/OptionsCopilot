"""Versioned native history observations, separate from model history contracts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import re

from options_copilot.market.session_calendar import (
    CalendarStatus,
    US_OPTIONS_TIMEZONE,
    UsOptionsCalendarSnapshot,
    UsOptionsSession,
)
from options_copilot.operations.readiness import assert_no_secret_like
from options_copilot.storage.canonical import canonical_hash, canonical_json, freeze_json, thaw_json


REQUEST_SCHEMA = "options_copilot.native_history_request.v1"
RESULT_SCHEMA = "options_copilot.native_history_result.v1"
NATIVE_HISTORY_KINDS = ("PRICE_HISTORY", "IV_HISTORY")
MAXIMUM_HISTORY_ROWS = 800
_IDENTITY_STATUS = "CACHE_QUALIFIED_NOT_PERSISTED_SECDEF"
_IDENTITY_KEYS = {"con_id", "symbol", "sec_type", "currency", "exchange", "primary_exchange"}
_AUTHORITY = {
    "decision_authority": "OBSERVATION_ONLY", "production_eligible": False,
    "model_input_complete": False, "point_in_time_verified": False,
}
_RESPONSE_KEYS = {
    "broker_request_id", "send_state", "response_end_received", "response_end",
    "wire_parameters", "start_generation", "end_generation", "broker_error_codes",
    "bars", "received_bar_count", "timed_out", "epoch_valid",
}


class NativeHistoryContractError(ValueError):
    """A stable rejection without private source or transport exception text."""

    def __init__(self, reason_code: str = "NATIVE_HISTORY_CONTRACT_INVALID") -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _require(condition: bool, reason: str = "NATIVE_HISTORY_CONTRACT_INVALID") -> None:
    if not condition:
        raise NativeHistoryContractError(reason)


def _keys(raw: object, expected: set[str]) -> Mapping[str, object]:
    _require(isinstance(raw, Mapping) and set(raw) == expected)
    return raw


def _time(raw: object) -> datetime:
    _require(isinstance(raw, str) and 0 < len(raw) <= 40)
    try:
        instant = datetime.fromisoformat(raw)
        _require(instant.tzinfo is not None and instant.utcoffset() is not None)
        return instant.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise NativeHistoryContractError() from None


def _instant(raw: datetime) -> str:
    _require(isinstance(raw, datetime) and raw.tzinfo is not None and raw.utcoffset() is not None)
    return raw.astimezone(timezone.utc).isoformat()


def _date(raw: object) -> date:
    _require(isinstance(raw, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw) is not None)
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise NativeHistoryContractError() from None


def _digest(raw: object) -> bool:
    return isinstance(raw, str) and re.fullmatch(r"[a-f0-9]{64}", raw) is not None


def _number(raw: object) -> Decimal | None:
    if raw is None:
        return None
    _require(isinstance(raw, str) and 0 < len(raw) <= 96)
    try:
        value = Decimal(raw)
        _require(value.is_finite() and len(value.as_tuple().digits) <= 64
                 and -64 <= value.as_tuple().exponent <= 64)
        return value
    except (InvalidOperation, ValueError, TypeError):
        raise NativeHistoryContractError() from None


def native_history_session_date(raw: str) -> date | None:
    """Parse only native daily-date representations, never synthesize a close."""

    try:
        if re.fullmatch(r"\d{8}", raw):
            return datetime.strptime(raw, "%Y%m%d").date()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            return date.fromisoformat(raw)
    except ValueError:
        pass
    return None


def _detached(raw: Mapping[str, object]) -> dict[str, object]:
    try:
        assert_no_secret_like(raw)
        _require(len(canonical_json(raw).encode("utf-8")) <= 1_200_000)
        return thaw_json(freeze_json(raw))
    except (TypeError, ValueError, OverflowError):
        raise NativeHistoryContractError() from None


def _sealed(raw: Mapping[str, object]) -> dict[str, object]:
    body = dict(raw)
    supplied = body.pop("content_hash", None)
    _require(_digest(supplied) and supplied == canonical_hash(body))
    return _detached(raw)


def _identity(raw: object) -> dict[str, object]:
    raw = _keys(raw, _IDENTITY_KEYS)
    _require(type(raw["con_id"]) is int and raw["con_id"] > 0)
    _require(isinstance(raw["symbol"], str) and re.fullmatch(r"[A-Z0-9.]{1,12}", raw["symbol"]) is not None)
    _require(raw["sec_type"] == "STK" and raw["currency"] == "USD")
    for name in ("exchange", "primary_exchange"):
        _require(isinstance(raw[name], str) and re.fullmatch(r"[A-Z0-9. _-]{0,32}", raw[name]) is not None)
    _require(bool(raw["exchange"]))
    return dict(raw)


def _calendar(raw: object) -> UsOptionsCalendarSnapshot:
    """Verify the retained calendar document; do not invent missing sessions."""

    raw = _keys(raw, {
        "status", "reason_codes", "observed_at", "normalized_at", "source",
        "broker_timezone_id", "normalized_timezone_id", "liquid_hours", "trading_hours",
        "source_hash", "sessions", "closed_dates", "calendar_hash", "entry_eligible",
    })
    _require(raw["status"] == "READY" and raw["source"] == "IBKR_REQ_CONTRACT_DETAILS_READONLY",
             "NATIVE_HISTORY_CALENDAR_UNVERIFIED")
    _require(isinstance(raw["sessions"], (list, tuple)) and 0 < len(raw["sessions"]) <= 32)
    _require(isinstance(raw["closed_dates"], (list, tuple)) and len(raw["closed_dates"]) <= 32)
    _require(isinstance(raw["reason_codes"], (list, tuple)) and not raw["reason_codes"])
    _require(_digest(raw["source_hash"]) and _digest(raw["calendar_hash"]))
    sessions = []
    try:
        for item in raw["sessions"]:
            row = _keys(item, {"trading_date", "open_et", "close_et", "open_utc", "close_utc", "early_close"})
            _require(type(row["early_close"]) is bool)
            sessions.append(UsOptionsSession(
                trading_date=_date(row["trading_date"]),
                open_et=_time(row["open_et"]).astimezone(US_OPTIONS_TIMEZONE),
                close_et=_time(row["close_et"]).astimezone(US_OPTIONS_TIMEZONE),
                open_utc=_time(row["open_utc"]), close_utc=_time(row["close_utc"]),
                early_close=row["early_close"],
            ))
        _require(len({row.trading_date for row in sessions}) == len(sessions))
        snapshot = UsOptionsCalendarSnapshot(
            status=CalendarStatus.READY, reason_codes=(), observed_at=_time(raw["observed_at"]),
            normalized_at=_time(raw["normalized_at"]), source=raw["source"],
            broker_timezone_id=raw["broker_timezone_id"], normalized_timezone_id=raw["normalized_timezone_id"],
            liquid_hours=raw["liquid_hours"], trading_hours=raw["trading_hours"], source_hash=raw["source_hash"],
            sessions=tuple(sessions), closed_dates=tuple(_date(row) for row in raw["closed_dates"]),
            calendar_hash=raw["calendar_hash"],
        )
        _require(snapshot.verify_hash() and raw["entry_eligible"] is snapshot.entry_eligible,
                 "NATIVE_HISTORY_CALENDAR_UNVERIFIED")
        return snapshot
    except (TypeError, ValueError, KeyError):
        raise NativeHistoryContractError("NATIVE_HISTORY_CALENDAR_UNVERIFIED") from None


def _cutoff(raw: object) -> dict[str, object]:
    raw = _keys(raw, {"calendar", "scheduled_for"})
    calendar = _calendar(raw["calendar"])
    scheduled = _time(raw["scheduled_for"])
    session = calendar.session_for(scheduled.astimezone(US_OPTIONS_TIMEZONE).date())
    _require(session is not None and session.close_utc <= scheduled,
             "NATIVE_HISTORY_COMPLETED_SESSION_UNVERIFIED")
    return {
        "scheduled_for": scheduled.isoformat(), "completed_session": session.trading_date.isoformat(),
        "session_close_at": session.close_utc.isoformat(), "calendar_hash": calendar.calendar_hash,
        "calendar": _detached(raw["calendar"]),
    }


def _basis(kind: str) -> dict[str, object]:
    return {
        "schema": "options_copilot.native_history_basis.v1", "kind": kind, "source": "IBKR",
        "status": "NATIVE_UNRESOLVED", "historical_session_coverage_verified": False,
        "adjustment_history_verified": False, "methodology_verified": False,
    }


def build_native_history_request(
    *, contract: Mapping[str, object], kind: str, cutoff: Mapping[str, object],
    incremental: bool, prepared_at: datetime,
) -> dict[str, object]:
    """Prepare a pure native request from already-observed identity and calendar."""

    identity = _identity(contract)
    _require(kind in NATIVE_HISTORY_KINDS and type(incremental) is bool)
    ending = _cutoff(cutoff)
    prepared = _instant(prepared_at)
    _require(_time(ending["scheduled_for"]) <= _time(prepared))
    _require(
        _time(ending["calendar"]["observed_at"]) <= _time(ending["calendar"]["normalized_at"]) <= _time(prepared),
        "NATIVE_HISTORY_CALENDAR_NOT_YET_AVAILABLE",
    )
    request_contract = {
        "schema": "options_copilot.native_history_wire_request.v1", "source": "IBKR",
        "adapter_version": "native-history.1", "contract": identity,
        "source_cutoff_at": ending["scheduled_for"], "completed_session": ending["completed_session"],
        "calendar_hash": ending["calendar_hash"], "incremental": incremental,
        "endDateTime": "" if kind == "PRICE_HISTORY" else ending["scheduled_for"],
        "durationStr": "7 D" if incremental else "1 Y" if kind == "PRICE_HISTORY" else "2 Y",
        "barSizeSetting": "1 day", "whatToShow": "ADJUSTED_LAST" if kind == "PRICE_HISTORY" else "OPTION_IMPLIED_VOLATILITY",
        "useRTH": True, "formatDate": 1, "keepUpToDate": False, "maximum_rows": MAXIMUM_HISTORY_ROWS,
    }
    basis = _basis(kind)
    body = {
        "schema": REQUEST_SCHEMA, "kind": kind, "symbol": identity["symbol"], "con_id": identity["con_id"],
        "contract": identity, "identity_status": _IDENTITY_STATUS, "prepared_at": prepared,
        "cutoff": ending, "request_contract": request_contract, "request_hash": canonical_hash(request_contract),
        "basis_contract": basis, "basis_hash": canonical_hash(basis), **_AUTHORITY,
    }
    return _detached({**body, "content_hash": canonical_hash(body)})


def validate_native_history_request(raw: object) -> dict[str, object]:
    """Validate all wire semantics without relabeling a FeatureHistoryBatch."""

    raw = _keys(raw, {
        "schema", "kind", "symbol", "con_id", "contract", "identity_status", "prepared_at",
        "cutoff", "request_contract", "request_hash", "basis_contract", "basis_hash", "content_hash", *_AUTHORITY,
    })
    _require(raw["schema"] == REQUEST_SCHEMA and raw["identity_status"] == _IDENTITY_STATUS)
    cutoff = _keys(raw["cutoff"], {"scheduled_for", "completed_session", "session_close_at", "calendar_hash", "calendar"})
    request = raw["request_contract"]
    _require(isinstance(request, Mapping) and "incremental" in request)
    expected = build_native_history_request(
        contract=raw["contract"], kind=raw["kind"],
        cutoff={"calendar": cutoff["calendar"], "scheduled_for": cutoff["scheduled_for"]},
        incremental=request["incremental"], prepared_at=_time(raw["prepared_at"]),
    )
    _require(_detached(raw) == expected)
    return _sealed(raw)


def native_history_wire_parameters(prepared_request: Mapping[str, object]) -> dict[str, object]:
    """Return actual SDK wire values, including the adjusted-last empty end."""

    request = prepared_request["request_contract"]
    ending = request["endDateTime"]
    return {
        "endDateTime": "" if ending == "" else _time(ending).strftime("%Y%m%d %H:%M:%S UTC"),
        **{name: request[name] for name in ("durationStr", "barSizeSetting", "whatToShow", "useRTH", "formatDate", "keepUpToDate")},
    }


def _validate_response(raw: object, prepared: Mapping[str, object]) -> tuple[dict[str, object], int]:
    raw = _keys(raw, _RESPONSE_KEYS)
    _require(raw["wire_parameters"] == native_history_wire_parameters(prepared))
    _require(raw["send_state"] in {"NOT_SENT", "INTENT_RECORDED", "DISPATCHED", "DISPATCH_UNCERTAIN"})
    _require(raw["broker_request_id"] is None or type(raw["broker_request_id"]) is int and raw["broker_request_id"] >= 0)
    for name in ("response_end_received", "timed_out", "epoch_valid"):
        _require(type(raw[name]) is bool)
    for name in ("start_generation", "end_generation"):
        _require(type(raw[name]) is int and raw[name] >= 0)
    _require(not raw["epoch_valid"] or raw["start_generation"] == raw["end_generation"])
    if raw["response_end_received"]:
        end = _keys(raw["response_end"], {"start", "end"})
        _require(all(isinstance(value, str) and len(value) <= 80 and all(ord(c) >= 32 for c in value) for value in end.values()))
        _require(raw["broker_request_id"] is not None and raw["send_state"] in {"DISPATCHED", "DISPATCH_UNCERTAIN"})
    else:
        _require(raw["response_end"] is None)
    codes = raw["broker_error_codes"]
    _require(isinstance(codes, (list, tuple)) and len(codes) <= 32
             and all(type(code) is int and 0 <= code <= 2**31 - 1 for code in codes)
             and len(set(codes)) == len(codes))
    bars = raw["bars"]
    _require(isinstance(bars, (list, tuple)) and len(bars) <= MAXIMUM_HISTORY_ROWS)
    _require(type(raw["received_bar_count"]) is int and raw["received_bar_count"] >= 0)
    _require(len(bars) == min(raw["received_bar_count"], MAXIMUM_HISTORY_ROWS))
    accepted = 0
    completed = _date(prepared["cutoff"]["completed_session"])
    for item in bars:
        row = _keys(item, {"raw_date", "session_date", "open", "high", "low", "close", "volume", "valid_close", "date_eligible"})
        _require(isinstance(row["raw_date"], str) and len(row["raw_date"]) <= 40 and all(ord(c) >= 32 for c in row["raw_date"]))
        session = None if row["session_date"] is None else _date(row["session_date"])
        _require(session == native_history_session_date(row["raw_date"]))
        numbers = {name: _number(row[name]) for name in ("open", "high", "low", "close", "volume")}
        close = numbers["close"]
        valid = bool(close is not None and close > 0 and (prepared["kind"] != "IV_HISTORY" or close <= Decimal("5")))
        eligible = session is not None and session <= completed
        _require(row["valid_close"] is valid and row["date_eligible"] is eligible)
        accepted += int(valid and eligible)
    if raw["send_state"] in {"NOT_SENT", "INTENT_RECORDED"}:
        _require(not raw["response_end_received"] and not raw["timed_out"] and not bars
                 and all(code in {1100, 1101, 1102} for code in codes))
    return dict(raw), accepted


def make_native_history_result(
    prepared_request: Mapping[str, object], *, claim_id: str | None,
    requested_at: datetime, available_at: datetime, response: Mapping[str, object],
    reason_codes: tuple[str, ...] | list[str],
) -> dict[str, object]:
    """Seal a native transport observation; store first-seen is never forged here."""

    prepared = validate_native_history_request(prepared_request)
    response, accepted = _validate_response(response, prepared)
    reasons = list(dict.fromkeys(reason_codes))
    status = (
        "NOT_SENT" if response["send_state"] in {"NOT_SENT", "INTENT_RECORDED"}
        else "PARTIAL" if accepted and reasons
        else "DELIVERED" if accepted else "UNAVAILABLE"
    )
    body = {
        "schema": RESULT_SCHEMA, "kind": prepared["kind"], "symbol": prepared["symbol"], "con_id": prepared["con_id"],
        "request_hash": prepared["request_hash"], "basis_hash": prepared["basis_hash"],
        "prepared_request_hash": prepared["content_hash"], "claim_id": claim_id,
        "requested_at": _instant(requested_at), "available_at": _instant(available_at),
        "status": status, "reason_codes": reasons, "response": response, **_AUTHORITY,
    }
    return validate_native_history_result({**body, "content_hash": canonical_hash(body)}, prepared_request=prepared)


def validate_native_history_result(raw: object, *, prepared_request: Mapping[str, object]) -> dict[str, object]:
    prepared = validate_native_history_request(prepared_request)
    raw = _keys(raw, {
        "schema", "kind", "symbol", "con_id", "request_hash", "basis_hash", "prepared_request_hash",
        "claim_id", "requested_at", "available_at", "status", "reason_codes", "response", "content_hash", *_AUTHORITY,
    })
    _require(raw["schema"] == RESULT_SCHEMA)
    for name in ("kind", "symbol", "con_id", "request_hash", "basis_hash"):
        _require(raw[name] == prepared[name])
    _require(raw["prepared_request_hash"] == prepared["content_hash"])
    for name, expected in _AUTHORITY.items():
        _require(type(raw[name]) is type(expected) and raw[name] == expected)
    _require(raw["claim_id"] is None or isinstance(raw["claim_id"], str)
             and re.fullmatch(r"[A-Za-z0-9._:-]{1,160}", raw["claim_id"]) is not None)
    _require(_time(prepared["prepared_at"]) <= _time(raw["requested_at"]) <= _time(raw["available_at"]))
    response, accepted = _validate_response(raw["response"], prepared)
    _require(response["send_state"] == "NOT_SENT" or raw["claim_id"] is not None)
    reasons = raw["reason_codes"]
    _require(isinstance(reasons, (list, tuple)) and len(reasons) <= 96
             and all(isinstance(code, str) and re.fullmatch(r"[A-Z0-9_:]{1,160}", code) is not None for code in reasons)
             and len(set(reasons)) == len(reasons))
    expected = (
        "NOT_SENT" if response["send_state"] in {"NOT_SENT", "INTENT_RECORDED"}
        else "PARTIAL" if accepted and reasons
        else "DELIVERED" if accepted else "UNAVAILABLE"
    )
    _require(raw["status"] == expected)
    if expected == "DELIVERED":
        _require(response["response_end_received"] and response["epoch_valid"] and not response["timed_out"]
                 and not response["broker_error_codes"] and response["send_state"] == "DISPATCHED")
    if response["timed_out"]:
        _require("NATIVE_HISTORY_TIMEOUT" in reasons)
    if not response["epoch_valid"]:
        _require("NATIVE_HISTORY_EPOCH_INVALIDATED" in reasons)
    if response["broker_error_codes"]:
        _require("NATIVE_HISTORY_BROKER_ERROR" in reasons)
    if response["send_state"] in {"DISPATCHED", "DISPATCH_UNCERTAIN"} and not response["response_end_received"]:
        _require("NATIVE_HISTORY_END_UNVERIFIED" in reasons)
    if response["received_bar_count"] > MAXIMUM_HISTORY_ROWS:
        _require("NATIVE_HISTORY_ROW_LIMIT_EXCEEDED" in reasons)
    bars = response["bars"]
    if any(not row["valid_close"] for row in bars):
        _require("NATIVE_HISTORY_INVALID_BARS" in reasons)
    if any(not row["date_eligible"] for row in bars):
        _require("NATIVE_HISTORY_DATE_EXCLUDED" in reasons)
    dates = [row["session_date"] for row in bars if row["valid_close"] and row["date_eligible"]]
    if len(dates) != len(set(dates)):
        _require("NATIVE_HISTORY_DUPLICATE_DATES" in reasons)
    if expected != "DELIVERED":
        _require(bool(reasons))
    return _sealed(raw)


__all__ = [
    "MAXIMUM_HISTORY_ROWS", "NATIVE_HISTORY_KINDS", "NativeHistoryContractError",
    "build_native_history_request", "make_native_history_result", "native_history_wire_parameters",
    "native_history_session_date",
    "validate_native_history_request", "validate_native_history_result",
]
