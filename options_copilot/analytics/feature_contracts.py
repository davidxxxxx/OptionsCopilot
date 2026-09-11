"""Immutable historical input contracts, never source or trading authority.

These types validate shape, identity and coverage. Authentic source/calendar
resolution and model authorization belong to application-layer resolvers.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime
from decimal import Decimal
import re
from types import MappingProxyType
from zoneinfo import ZoneInfo

from options_copilot.storage.canonical import canonical_hash, utc_datetime


SCHEMA = "options_copilot.feature_history.v1"
_REQUEST_SCHEMA = "options_copilot.history_request_contract.v1"
_BASIS_SCHEMA = "options_copilot.series_basis_contract.v1"
_PRICE_BASES = {
    "TRADES": "SPLIT_ADJUSTED",
    "ADJUSTED_LAST": "SPLIT_DIVIDEND_ADJUSTED",
    "OPTION_IMPLIED_VOLATILITY": "NOT_APPLICABLE",
}
FEATURE_WINDOWS = MappingProxyType({
    "RETURN1": 2, "RETURN5": 6, "RETURN20": 21, "REALIZED_VOLATILITY20": 21,
    "RELATIVE_VOLUME20": 21, "MEDIAN_DOLLAR_VOLUME20": 20,
    "BENCHMARK_RETURN20": 21, "EMA20": 60, "IV_PERCENTILE": 252,
})


class FeatureContractError(ValueError):
    """A stable missing, mismatched or malformed input reason."""


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise FeatureContractError(reason)


def _text(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 160 and value == value.strip() and all(ord(c) >= 32 for c in value)


def _digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _date(value: object) -> bool:
    return type(value) is date


def _time(value: datetime) -> datetime:
    try:
        return utc_datetime(value)
    except (TypeError, ValueError) as exc:
        raise FeatureContractError("FEATURE_TIMESTAMP_INVALID") from exc


def _number(value: object, *, zero_allowed: bool = False) -> bool:
    return (isinstance(value, Decimal) and value.is_finite()
            and len(value.as_tuple().digits) <= 64 and -64 <= value.as_tuple().exponent <= 64
            and (value >= 0 if zero_allowed else value > 0))


def _parse_time(value: object) -> datetime:
    _require(isinstance(value, str), "FEATURE_TIMESTAMP_INVALID")
    return _time(datetime.fromisoformat(value))


def _parse_date(value: object) -> date:
    _require(isinstance(value, str), "FEATURE_DATE_INVALID")
    return date.fromisoformat(value)


def _parse_number(value: object) -> Decimal:
    _require(isinstance(value, str) and len(value) <= 128, "FEATURE_DECIMAL_INVALID")
    return Decimal(value)


def _keys(value: object, expected: set[str]) -> None:
    _require(isinstance(value, Mapping) and set(value) == expected, "FEATURE_DOCUMENT_SHAPE_INVALID")


@dataclass(frozen=True, slots=True, kw_only=True)
class HistoryRequestContract:
    source: str
    adapter_version: str
    con_id: int
    symbol: str
    secdef_hash: str
    what_to_show: str
    bar_size: str
    use_rth: bool
    exchange_timezone: str
    request_end: datetime
    duration: str
    start_session: date
    end_session: date
    adjustment_basis: str
    value_unit: str
    volume_unit: str

    def __post_init__(self) -> None:
        _require(self.source == "IBKR" and _text(self.adapter_version), "FEATURE_REQUEST_SOURCE_INVALID")
        _require(type(self.con_id) is int and self.con_id > 0 and _digest(self.secdef_hash), "FEATURE_REQUEST_IDENTITY_INVALID")
        _require(isinstance(self.symbol, str) and re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,31}", self.symbol) is not None, "FEATURE_REQUEST_IDENTITY_INVALID")
        _require(type(self.use_rth) is bool and self.bar_size == "1 day", "FEATURE_REQUEST_PARAMETERS_INVALID")
        _require(isinstance(self.duration, str) and re.fullmatch(r"[1-9][0-9]{0,2} [DWMY]", self.duration) is not None, "FEATURE_REQUEST_PARAMETERS_INVALID")
        _require(_text(self.exchange_timezone), "FEATURE_REQUEST_TIMEZONE_INVALID")
        try:
            zone = ZoneInfo(self.exchange_timezone)
        except (KeyError, ValueError) as exc:
            raise FeatureContractError("FEATURE_REQUEST_TIMEZONE_INVALID") from exc
        object.__setattr__(self, "request_end", _time(self.request_end))
        _require(_date(self.start_session) and _date(self.end_session)
                 and self.start_session <= self.end_session <= self.request_end.astimezone(zone).date(), "FEATURE_REQUEST_RANGE_INVALID")
        _require(isinstance(self.what_to_show, str) and self.what_to_show in _PRICE_BASES
                 and self.adjustment_basis == _PRICE_BASES[self.what_to_show], "FEATURE_REQUEST_BASIS_INVALID")
        iv = self.what_to_show == "OPTION_IMPLIED_VOLATILITY"
        _require(self.value_unit == ("ANNUALIZED_FRACTION" if iv else "USD_PER_SHARE"), "FEATURE_REQUEST_UNIT_INVALID")
        _require(self.volume_unit in ("SHARES", "LOTS", "UNKNOWN", "NOT_APPLICABLE"), "FEATURE_REQUEST_UNIT_INVALID")
        _require(not iv or self.volume_unit == "NOT_APPLICABLE", "FEATURE_REQUEST_UNIT_INVALID")

    def as_dict(self) -> dict[str, object]:
        return {**asdict(self), "schema": _REQUEST_SCHEMA,
                "request_end": self.request_end.isoformat(),
                "start_session": self.start_session.isoformat(), "end_session": self.end_session.isoformat(),
                "format_date": 2, "keep_up_to_date": False}

    @property
    def contract_hash(self) -> str:
        return canonical_hash(self.as_dict())

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> HistoryRequestContract:
        _keys(document, {f.name for f in fields(cls)} | {"schema", "format_date", "keep_up_to_date"})
        _require(document["schema"] == _REQUEST_SCHEMA and type(document["format_date"]) is int
                 and document["format_date"] == 2 and document["keep_up_to_date"] is False, "FEATURE_REQUEST_PARAMETERS_INVALID")
        values = {f.name: document[f.name] for f in fields(cls)}
        values["request_end"] = _parse_time(values["request_end"])
        for key in ("start_session", "end_session"):
            values[key] = _parse_date(values[key])
        return cls(**values)


@dataclass(frozen=True, slots=True, kw_only=True)
class SeriesBasisContract:
    series_kind: str
    calendar_hash: str
    methodology_id: str
    methodology_version: str
    adjustment_basis: str
    value_unit: str
    sampling: str
    horizon: str
    tenor_days: int | None
    rolling_rule: str
    moneyness: str
    option_side: str
    interpolation: str

    def __post_init__(self) -> None:
        _require(self.series_kind in ("PRICE", "IV") and _digest(self.calendar_hash), "FEATURE_SERIES_BASIS_INVALID")
        for field in fields(self):
            if field.name != "tenor_days":
                _require(_text(getattr(self, field.name)), "FEATURE_SERIES_BASIS_INVALID")
        _require(self.tenor_days is None or (type(self.tenor_days) is int and 0 < self.tenor_days <= 730), "FEATURE_SERIES_TENOR_INVALID")
        if self.series_kind == "IV":
            _require(self.adjustment_basis == "NOT_APPLICABLE" and self.value_unit == "ANNUALIZED_FRACTION", "FEATURE_SERIES_BASIS_INVALID")
            _require(self.horizon in ("CONSTANT_TENOR", "ROLLING", "PROVIDER_NATIVE_UNRESOLVED"), "FEATURE_SERIES_HORIZON_INVALID")
            if self.horizon == "CONSTANT_TENOR":
                _require(self.tenor_days is not None, "FEATURE_SERIES_TENOR_INVALID")
            if self.horizon == "ROLLING":
                _require(self.rolling_rule not in ("NONE", "NOT_APPLICABLE", "UNKNOWN"), "FEATURE_SERIES_ROLLING_RULE_INVALID")
            if self.horizon != "PROVIDER_NATIVE_UNRESOLVED":
                _require(self.moneyness not in ("NONE", "NOT_APPLICABLE", "UNKNOWN")
                         and self.option_side in ("CALL", "PUT", "CALL_PUT"), "FEATURE_SERIES_BASIS_INVALID")
        else:
            _require(self.adjustment_basis in ("SPLIT_ADJUSTED", "SPLIT_DIVIDEND_ADJUSTED")
                     and self.value_unit == "USD_PER_SHARE", "FEATURE_SERIES_BASIS_INVALID")

    def as_dict(self) -> dict[str, object]:
        return {"schema": _BASIS_SCHEMA, **asdict(self)}

    @property
    def contract_hash(self) -> str:
        return canonical_hash(self.as_dict())

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> SeriesBasisContract:
        _keys(document, {f.name for f in fields(cls)} | {"schema"})
        _require(document["schema"] == _BASIS_SCHEMA, "FEATURE_SERIES_SCHEMA_INVALID")
        return cls(**{f.name: document[f.name] for f in fields(cls)})


@dataclass(frozen=True, slots=True, kw_only=True)
class FeatureHistoryPoint:
    session_date: date
    session_close_at: datetime
    close: Decimal
    volume: Decimal | None

    def __post_init__(self) -> None:
        _require(_date(self.session_date), "FEATURE_DATE_INVALID")
        object.__setattr__(self, "session_close_at", _time(self.session_close_at))
        _require(_number(self.close), "FEATURE_PRICE_INVALID")
        _require(self.volume is None or _number(self.volume, zero_allowed=True), "FEATURE_VOLUME_INVALID")

    def as_dict(self) -> dict[str, object]:
        # Decimal strings avoid context-dependent normalization of source values.
        return {"session_date": self.session_date.isoformat(),
                "session_close_at": self.session_close_at.isoformat(),
                "close": format(self.close, "f"),
                "volume": None if self.volume is None else format(self.volume, "f")}

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> FeatureHistoryPoint:
        _keys(document, {f.name for f in fields(cls)})
        return cls(session_date=_parse_date(document["session_date"]),
                   session_close_at=_parse_time(document["session_close_at"]),
                   close=_parse_number(document["close"]),
                   volume=None if document["volume"] is None else _parse_number(document["volume"]))


@dataclass(frozen=True, slots=True, kw_only=True)
class FeatureHistoryBatch:
    request: HistoryRequestContract
    basis: SeriesBasisContract
    points: tuple[FeatureHistoryPoint, ...]
    available_at: datetime
    source_revision_hash: str
    request_fingerprint: str

    def __post_init__(self) -> None:
        _require(isinstance(self.request, HistoryRequestContract) and isinstance(self.basis, SeriesBasisContract), "FEATURE_BATCH_CONTRACT_INVALID")
        _require(isinstance(self.points, (list, tuple)) and 0 < len(self.points) <= 600
                 and all(isinstance(row, FeatureHistoryPoint) for row in self.points), "FEATURE_BATCH_POINTS_INVALID")
        object.__setattr__(self, "points", tuple(self.points))
        object.__setattr__(self, "available_at", _time(self.available_at))
        _require(_digest(self.source_revision_hash), "FEATURE_SOURCE_REVISION_INVALID")
        _require(_digest(self.request_fingerprint), "FEATURE_REQUEST_FINGERPRINT_INVALID")
        _require(self.request.adjustment_basis == self.basis.adjustment_basis
                 and self.request.value_unit == self.basis.value_unit, "FEATURE_BATCH_BASIS_MISMATCH")
        _require((self.basis.series_kind == "IV") == (self.request.what_to_show == "OPTION_IMPLIED_VOLATILITY"), "FEATURE_BATCH_BASIS_MISMATCH")
        days = tuple(row.session_date for row in self.points)
        _require(days == tuple(sorted(set(days))), "FEATURE_SESSION_ALIGNMENT_INVALID")
        zone = ZoneInfo(self.request.exchange_timezone)
        for row in self.points:
            _require(self.request.start_session <= row.session_date <= self.request.end_session
                     and row.session_close_at.astimezone(zone).date() == row.session_date, "FEATURE_SESSION_ALIGNMENT_INVALID")
            _require(row.session_close_at <= self.request.request_end <= self.available_at, "FEATURE_POINT_NOT_AVAILABLE")

    def _payload(self) -> dict[str, object]:
        return {"schema": SCHEMA, "status": "RESEARCH_ONLY", "decision_authority": "OBSERVATION_ONLY",
                "request_contract": self.request.as_dict(), "request_contract_hash": self.request.contract_hash,
                "basis_contract": self.basis.as_dict(), "basis_contract_hash": self.basis.contract_hash,
                "points": [row.as_dict() for row in self.points], "available_at": self.available_at.isoformat(),
                "source_revision_hash": self.source_revision_hash, "request_fingerprint": self.request_fingerprint}

    @property
    def batch_hash(self) -> str:
        return canonical_hash(self._payload())

    def as_dict(self) -> dict[str, object]:
        return {**self._payload(), "batch_hash": self.batch_hash}

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> FeatureHistoryBatch:
        _require(isinstance(document, Mapping), "FEATURE_DOCUMENT_SHAPE_INVALID")
        _require("request_contract_hash" in document and "basis_contract_hash" in document, "LEGACY_BASIS_UNRESOLVED")
        _keys(document, {"schema", "status", "decision_authority", "request_contract", "request_contract_hash",
                         "basis_contract", "basis_contract_hash", "points", "available_at", "source_revision_hash", "request_fingerprint", "batch_hash"})
        _require(document["schema"] == SCHEMA and document["status"] == "RESEARCH_ONLY"
                 and document["decision_authority"] == "OBSERVATION_ONLY", "FEATURE_BATCH_AUTHORITY_INVALID")
        _require(isinstance(document["points"], (list, tuple)) and 0 < len(document["points"]) <= 600, "FEATURE_BATCH_POINTS_INVALID")
        try:
            result = cls(request=HistoryRequestContract.from_document(document["request_contract"]),
                         basis=SeriesBasisContract.from_document(document["basis_contract"]),
                         points=tuple(FeatureHistoryPoint.from_document(row) for row in document["points"]),
                         available_at=_parse_time(document["available_at"]), source_revision_hash=document["source_revision_hash"],
                         request_fingerprint=document["request_fingerprint"])
        except (TypeError, ValueError, ArithmeticError) as exc:
            if isinstance(exc, FeatureContractError):
                raise
            raise FeatureContractError("FEATURE_DOCUMENT_VALUE_INVALID") from exc
        _require(result.request.contract_hash == document["request_contract_hash"]
                 and result.basis.contract_hash == document["basis_contract_hash"], "FEATURE_CONTRACT_HASH_MISMATCH")
        _require(result.batch_hash == document["batch_hash"], "FEATURE_BATCH_HASH_MISMATCH")
        return result


def select_feature_window(
    points: Sequence[FeatureHistoryPoint], expected_sessions: Sequence[date], *, feature: str,
) -> tuple[FeatureHistoryPoint, ...]:
    """Select one precise window against resolved dates, not a calendar authority."""
    _require(isinstance(feature, str) and feature in FEATURE_WINDOWS, "FEATURE_WINDOW_UNSUPPORTED")
    _require(isinstance(points, (tuple, list)) and all(isinstance(row, FeatureHistoryPoint) for row in points), "FEATURE_BATCH_POINTS_INVALID")
    _require(isinstance(expected_sessions, (tuple, list)) and all(_date(day) for day in expected_sessions), "FEATURE_SESSION_ALIGNMENT_INVALID")
    dates = tuple(expected_sessions)
    _require(dates == tuple(sorted(set(dates))), "FEATURE_SESSION_ALIGNMENT_INVALID")
    count = FEATURE_WINDOWS[feature]
    reason = {"EMA20": "FEATURE_EMA_WARMUP_INSUFFICIENT", "IV_PERCENTILE": "IV_HISTORY_INSUFFICIENT"}.get(feature, "FEATURE_HISTORY_INSUFFICIENT")
    _require(len(points) >= count and len(dates) >= count, reason)
    all_days = tuple(row.session_date for row in points)
    _require(all_days == tuple(sorted(set(all_days))), "FEATURE_SESSION_ALIGNMENT_INVALID")
    selected = tuple(points[-count:])
    _require(tuple(row.session_date for row in selected) == dates[-count:], "FEATURE_SESSION_ALIGNMENT_INVALID")
    return selected


__all__ = ["FEATURE_WINDOWS", "FeatureContractError", "FeatureHistoryBatch", "FeatureHistoryPoint",
           "HistoryRequestContract", "SeriesBasisContract", "select_feature_window"]
