"""Strict, shared decoders for observation-only feature source diagnostics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
import re
from zoneinfo import ZoneInfo

from options_copilot.operations.readiness import assert_no_secret_like
from options_copilot.storage.canonical import canonical_hash, canonical_json, freeze_json, thaw_json, utc_datetime


SOURCE_KINDS = ("PRICE_HISTORY", "IV_HISTORY", "CURRENT_IV")
_SCHEMA = "options_copilot.feature_source_diagnostic.v1"
_FAILURE_REASONS = frozenset({
    "FEATURE_SOURCE_DIAGNOSTIC_RUNNING", "FEATURE_SOURCE_DIAGNOSTIC_COOLDOWN",
    "IBKR_READONLY_GATEWAY_NOT_CONNECTED", "FEATURE_SOURCE_METHOD_UNAVAILABLE",
    "FEATURE_SOURCE_PACING_DENIED", "FEATURE_SOURCE_TIMEOUT",
    "FEATURE_SOURCE_UNAVAILABLE", "FEATURE_SOURCE_RESPONSE_INVALID",
})
_SOURCE_COMMON = {
    "schema", "kind", "status", "symbol", "source", "contract", "requested_at",
    "cutoff_at", "available_at", "request_parameters", "basis_status",
    "decision_authority", "production_eligible", "point_in_time_verified",
    "model_input_complete", "reason_codes", "broker_error_codes", "broker_request_id",
    "content_hash", "request_sent",
}
_HISTORY_FIELDS = {
    "bars", "received_bar_count", "prior_completed_bar_count", "invalid_bar_count",
    "duplicate_prior_date_count", "excluded_current_or_future_bar_count",
    "required_prior_bar_count", "enough_prior_bars", "calendar_coverage_verified",
}
_CURRENT_FIELDS = {
    "value", "tick_type", "generic_tick", "received_at", "market_data_type",
    "source_event_timestamp",
}


class FeatureSourceDiagnosticInvalid(ValueError):
    """A diagnostic response violated its bounded observation contract."""


def _require(condition: bool) -> None:
    if not condition:
        raise FeatureSourceDiagnosticInvalid("FEATURE_SOURCE_DIAGNOSTIC_RESPONSE_INVALID")


def _keys(value: object, expected: set[str]) -> Mapping[str, object]:
    _require(isinstance(value, Mapping) and set(value) == expected)
    return value


def _sequence(value: object, maximum: int) -> Sequence[object]:
    _require(isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and len(value) <= maximum)
    return value


def _integer(value: object, *, maximum: int = 2**31 - 1) -> int:
    _require(type(value) is int and 0 <= value <= maximum)
    return value


def _time(value: object) -> datetime:
    _require(isinstance(value, str) and len(value) <= 40)
    return utc_datetime(datetime.fromisoformat(value))


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    _require(isinstance(value, str) and 0 < len(value) <= 96)
    parsed = Decimal(value)
    _require(parsed.is_finite() and len(parsed.as_tuple().digits) <= 64 and -64 <= parsed.as_tuple().exponent <= 64)
    return parsed


def _reasons(value: object) -> tuple[str, ...]:
    rows = _sequence(value, 96)
    _require(all(isinstance(row, str) and re.fullmatch(r"[A-Z0-9_:]{1,200}", row) is not None for row in rows))
    _require(len(set(rows)) == len(rows))
    return tuple(rows)


def _hash_and_detach(value: Mapping[str, object]) -> dict[str, object]:
    assert_no_secret_like(value)
    body = dict(value)
    supplied_hash = body.pop("content_hash")
    _require(isinstance(supplied_hash, str) and re.fullmatch(r"[a-f0-9]{64}", supplied_hash) is not None)
    _require(canonical_hash(body) == supplied_hash)
    _require(len(canonical_json(value).encode("utf-8")) <= 1_100_000)
    return thaw_json(freeze_json(value))


def _observation_boundary(raw: Mapping[str, object]) -> None:
    _require(raw.get("basis_status") == "PROVIDER_NATIVE_UNRESOLVED")
    _require(raw.get("decision_authority") == "OBSERVATION_ONLY")
    for name in ("model_input_complete", "production_eligible", "point_in_time_verified"):
        _require(raw.get(name) is False)


def _failure(raw: Mapping[str, object], kind: str) -> None:
    expected = {
        "schema", "kind", "symbol", "source", "status", "reason_codes",
        "request_sent", "available_at", "basis_status", "model_input_complete",
        "production_eligible", "point_in_time_verified", "decision_authority", "content_hash",
    } | ({"value", "received_at", "source_event_timestamp"} if kind == "CURRENT_IV" else {
        "bars", "received_bar_count", "prior_completed_bar_count",
    })
    _keys(raw, expected)
    reasons = _reasons(raw["reason_codes"])
    _require(len(reasons) == 1 and reasons[0] in _FAILURE_REASONS)
    _require(raw["status"] in {"UNAVAILABLE", "NOT_REQUESTED"})
    _require(raw["request_sent"] is None or raw["request_sent"] is False)
    if raw["status"] == "NOT_REQUESTED":
        _require(raw["request_sent"] is False)
    nulls = ("available_at", "value", "received_at", "source_event_timestamp") if kind == "CURRENT_IV" else (
        "available_at", "bars", "received_bar_count", "prior_completed_bar_count",
    )
    _require(all(raw[name] is None for name in nulls))


def _request_parameters(raw: Mapping[str, object], kind: str, cutoff: datetime) -> None:
    if kind == "CURRENT_IV":
        expected = {"genericTickList": "106", "snapshot": False, "regulatorySnapshot": False}
        parameters = _keys(raw["request_parameters"], set(expected))
    else:
        expected = {
            "durationStr": "1 Y" if kind == "PRICE_HISTORY" else "2 Y",
            "barSizeSetting": "1 day",
            "whatToShow": "ADJUSTED_LAST" if kind == "PRICE_HISTORY" else "OPTION_IMPLIED_VOLATILITY",
            "useRTH": True, "formatDate": 1, "keepUpToDate": False,
        }
        parameters = _keys(raw["request_parameters"], set(expected) | {"endDateTime"})
        if kind == "PRICE_HISTORY":
            _require(parameters["endDateTime"] == "")
        else:
            expected_end = cutoff.astimezone(ZoneInfo("America/New_York")).replace(hour=0, minute=0, second=0, microsecond=0)
            _require(_time(parameters["endDateTime"]) == expected_end)
    for name, value in expected.items():
        _require(type(parameters[name]) is type(value) and parameters[name] == value)


def _history(raw: Mapping[str, object], kind: str, cutoff: datetime) -> bool:
    rows = _sequence(raw["bars"], 800)
    prior_dates: list[date] = []
    invalid = excluded = 0
    cutoff_date = cutoff.astimezone(ZoneInfo("America/New_York")).date()
    for value in rows:
        row = _keys(value, {
            "raw_date", "trading_date", "open", "high", "low", "close", "volume",
            "prior_date_row", "valid_close",
        })
        _require(isinstance(row["raw_date"], str) and len(row["raw_date"]) <= 40 and all(ord(c) >= 32 for c in row["raw_date"]))
        trading_date = None
        if row["trading_date"] is not None:
            _require(isinstance(row["trading_date"], str) and len(row["trading_date"]) == 10)
            trading_date = date.fromisoformat(row["trading_date"])
        parsed = {name: _decimal(row[name]) for name in ("open", "high", "low", "close", "volume")}
        close = parsed["close"]
        valid = trading_date is not None and close is not None and close > 0 and (kind != "IV_HISTORY" or close <= Decimal("5"))
        prior = trading_date is not None and trading_date < cutoff_date
        _require(row["prior_date_row"] is prior and row["valid_close"] is valid)
        invalid += int(not valid)
        excluded += int(trading_date is not None and not prior)
        if valid and prior:
            prior_dates.append(trading_date)
    received = _integer(raw["received_bar_count"])
    _require(len(rows) == min(received, 800))
    if received > 800:
        _require("FEATURE_SOURCE_ROW_LIMIT_EXCEEDED" in raw["reason_codes"])
    unique = len(set(prior_dates))
    expected_counts = {
        "prior_completed_bar_count": unique, "invalid_bar_count": invalid,
        "excluded_current_or_future_bar_count": excluded,
        "duplicate_prior_date_count": len(prior_dates) - unique,
        "required_prior_bar_count": 60 if kind == "PRICE_HISTORY" else 252,
    }
    for name, value in expected_counts.items():
        _require(_integer(raw[name]) == value)
    _require(raw["enough_prior_bars"] is (unique >= expected_counts["required_prior_bar_count"]))
    _require(raw["calendar_coverage_verified"] is False)
    return unique > 0


def validate_feature_source_observation(
    raw: object, *, kind: str, symbol: str, cutoff: datetime, allow_failure: bool = False,
) -> dict[str, object]:
    """Validate exact request identity, raw fields, counts and observation scope."""

    _require(kind in SOURCE_KINDS and isinstance(raw, Mapping))
    _require(raw.get("schema") == _SCHEMA and raw.get("kind") == kind)
    _require(raw.get("symbol") == symbol and raw.get("source") == "IBKR")
    _observation_boundary(raw)
    if allow_failure and "requested_at" not in raw:
        _failure(raw, kind)
        return _hash_and_detach(raw)
    _keys(raw, _SOURCE_COMMON | (_CURRENT_FIELDS if kind == "CURRENT_IV" else _HISTORY_FIELDS))
    requested, available = _time(raw["requested_at"]), _time(raw["available_at"])
    _require(_time(raw["cutoff_at"]) == cutoff and cutoff <= requested <= available)
    _require(type(raw["request_sent"]) is bool)
    if raw["broker_request_id"] is not None:
        _integer(raw["broker_request_id"])
    for code in _sequence(raw["broker_error_codes"], 32):
        _integer(code)
    contract = raw["contract"]
    if contract is None:
        _require(raw["status"] == "UNAVAILABLE")
    else:
        contract = _keys(contract, {"con_id", "symbol", "sec_type", "currency", "exchange", "primary_exchange"})
        _require(_integer(contract["con_id"]) > 0 and contract["symbol"] == symbol)
        _require(contract["sec_type"] == "STK" and contract["currency"] == "USD")
        for name in ("exchange", "primary_exchange"):
            _require(isinstance(contract[name], str) and re.fullmatch(r"[A-Z0-9. _-]{0,32}", contract[name]) is not None)
    _request_parameters(raw, kind, cutoff)
    reasons = _reasons(raw["reason_codes"])
    if kind == "CURRENT_IV":
        _require(type(raw["tick_type"]) is int and raw["tick_type"] == 24)
        _require(type(raw["generic_tick"]) is int and raw["generic_tick"] == 106)
        _require(raw["source_event_timestamp"] is None)
        if raw["market_data_type"] is not None:
            _require(_integer(raw["market_data_type"], maximum=4) >= 1)
        value = _decimal(raw["value"])
        if value is None:
            _require(raw["received_at"] is None)
        else:
            _require(0 < value <= Decimal("5") and requested <= _time(raw["received_at"]) <= available)
        delivered = value is not None
    else:
        delivered = _history(raw, kind, cutoff)
    if delivered:
        _require(raw["request_sent"] is True)
    expected_status = "PARTIAL" if delivered and reasons else "DELIVERED" if delivered else "UNAVAILABLE"
    _require(raw["status"] == expected_status)
    return _hash_and_detach(raw)


def validate_feature_sources_response(raw: object, *, symbol: str) -> dict[str, object]:
    """Verify the complete public diagnostic, including every nested source."""

    raw = _keys(raw, {
        "schema", "status", "scope", "symbol", "requested_at", "completed_at",
        "cooldown_scope", "cooldown_until", "sources", "reason_codes", "basis_status",
        "model_input_complete", "production_eligible", "point_in_time_verified",
        "decision_authority", "review_only", "approval_eligible",
        "instruction_creation_allowed", "direct_order_submission", "broker_write_authority", "content_hash",
    })
    _require(raw["schema"] == "options_copilot.feature_sources_diagnostic.v1")
    _require(raw["scope"] == "SINGLE_UNDERLYING" and raw["symbol"] == symbol)
    _require(isinstance(symbol, str) and re.fullmatch(r"[A-Z0-9.]{1,12}", symbol) is not None)
    _observation_boundary(raw)
    _require(raw["review_only"] is True and raw["cooldown_scope"] == "RUNTIME_INSTANCE")
    for name in ("approval_eligible", "instruction_creation_allowed", "direct_order_submission", "broker_write_authority"):
        _require(raw[name] is False)
    requested, completed = _time(raw["requested_at"]), _time(raw["completed_at"])
    _require(requested <= completed)
    if raw["cooldown_until"] is not None:
        _time(raw["cooldown_until"])
    sources = _keys(raw["sources"], set(SOURCE_KINDS))
    statuses: list[str] = []
    source_reasons: list[str] = []
    for kind in SOURCE_KINDS:
        source = validate_feature_source_observation(sources[kind], kind=kind, symbol=symbol, cutoff=requested, allow_failure=True)
        if source["available_at"] is not None:
            _require(_time(source["available_at"]) <= completed)
        statuses.append(source["status"])
        source_reasons.extend(source["reason_codes"])
    reasons = _reasons(raw["reason_codes"])
    _require(reasons == tuple(dict.fromkeys(source_reasons)))
    if raw["status"] == "WAIT":
        _require(all(value == "NOT_REQUESTED" for value in statuses))
        _require(reasons in {("FEATURE_SOURCE_DIAGNOSTIC_RUNNING",), ("FEATURE_SOURCE_DIAGNOSTIC_COOLDOWN",)})
    else:
        expected_status = "OBSERVED" if all(value == "DELIVERED" for value in statuses) else "PARTIAL" if any(value in {"DELIVERED", "PARTIAL"} for value in statuses) else "UNAVAILABLE"
        _require(raw["status"] == expected_status)
    return _hash_and_detach(raw)


__all__ = ["FeatureSourceDiagnosticInvalid", "SOURCE_KINDS", "validate_feature_source_observation", "validate_feature_sources_response"]
