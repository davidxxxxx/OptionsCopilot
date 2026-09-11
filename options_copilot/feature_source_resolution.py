"""Cache-only source bindings; observations cannot grant model or order authority."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
import re
from typing import Protocol

from options_copilot.analytics.benchmark import benchmark_convention
from options_copilot.analytics.ema20 import ema20_convention
from options_copilot.analytics.iv_percentile import iv_percentile_convention
from options_copilot.feature_source_diagnostic import SOURCE_KINDS, validate_feature_source_observation
from options_copilot.storage.canonical import canonical_hash, canonical_json, freeze_json, thaw_json, utc_datetime
from options_copilot.storage.feature_sources import FeatureSourceStoreError


_SCHEMA = "options_copilot.feature_source_binding.v1"
_SCHEDULED_SCHEMA = "options_copilot.feature_source_binding.v2"
_CONVENTION_SCHEMA = "options_copilot.feature_source_binding.v3"
_IV_CONVENTION_SCHEMA = "options_copilot.feature_source_binding.v4"
_BENCHMARK_CONVENTION_SCHEMA = "options_copilot.feature_source_binding.v5"
_UNRESOLVED = (
    "FEATURE_HISTORY_CALENDAR_UNVERIFIED",
    "FEATURE_HISTORY_SECDEF_UNRESOLVED",
    "EMA_CONVENTION_UNAPPROVED",
    "IV_BASIS_UNRESOLVED",
    "FEATURE_BENCHMARK_MAPPING_UNRESOLVED",
    "OPTION_SURFACE_UNBOUND",
    "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
)
_CONVENTION_UNRESOLVED = tuple(
    "EMA_PRODUCTION_POLICY_UNVERIFIED" if reason == "EMA_CONVENTION_UNAPPROVED" else reason
    for reason in _UNRESOLVED
)
_IV_CONVENTION_UNRESOLVED = (*_CONVENTION_UNRESOLVED, "IV_PRODUCTION_POLICY_UNVERIFIED")
_BENCHMARK_CONVENTION_UNRESOLVED = tuple(
    "FEATURE_BENCHMARK_SOURCE_UNVERIFIED"
    if reason == "FEATURE_BENCHMARK_MAPPING_UNRESOLVED" else reason
    for reason in _IV_CONVENTION_UNRESOLVED
) + ("BENCHMARK_PRODUCTION_POLICY_UNVERIFIED",)
_REFERENCE_FIELDS = {
    "observation_id", "sequence", "row_hash", "source_hash", "request_hash", "basis_hash",
    "first_seen_at", "available_at", "source_cutoff_at", "source_status",
    "prior_completed_bar_count", "projection_age_seconds",
}
_CACHE_FAILURE_REASONS = frozenset({
    "FEATURE_SOURCE_STORE_VERIFICATION_BUDGET_EXCEEDED", "FEATURE_SOURCE_STORE_CLOSED",
    "FEATURE_SOURCE_STORE_INVALID", "FEATURE_SOURCE_STORE_CLOCK_REGRESSED",
})


class FeatureSourceReader(Protocol):
    def read(self, *, symbol: str, con_id: int, cutoff: datetime) -> tuple[dict[str, object], ...]: ...


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("FEATURE_SOURCE_BINDING_INVALID")


def _identity(symbol: str, con_id: int, expiration: date, cutoff: datetime) -> datetime:
    _require(isinstance(symbol, str) and re.fullmatch(r"[A-Z0-9.]{1,12}", symbol) is not None)
    _require(type(con_id) is int and con_id > 0 and type(expiration) is date)
    return utc_datetime(cutoff)


def _time(value: object) -> datetime:
    _require(isinstance(value, str) and len(value) <= 40)
    return utc_datetime(datetime.fromisoformat(value))


def validate_feature_source_binding(
    raw: object, *, symbol: str, con_id: int, expiration: date, cutoff: datetime,
    require_current_convention: bool = False,
) -> dict[str, object]:
    """Validate archive formats or the server-required current producer contract."""

    instant = _identity(symbol, con_id, expiration, cutoff)
    _require(type(require_current_convention) is bool)
    _require(isinstance(raw, Mapping))
    convention_bound = raw.get("schema") in {
        _CONVENTION_SCHEMA, _IV_CONVENTION_SCHEMA, _BENCHMARK_CONVENTION_SCHEMA,
    }
    iv_convention_bound = raw.get("schema") in {_IV_CONVENTION_SCHEMA, _BENCHMARK_CONVENTION_SCHEMA}
    benchmark_convention_bound = raw.get("schema") == _BENCHMARK_CONVENTION_SCHEMA
    convention = ema20_convention(instant)
    iv_convention = iv_percentile_convention(instant)
    benchmark_spec = benchmark_convention(symbol, instant)
    if require_current_convention:
        # Archive compatibility must not allow a current producer to omit the
        # now-known convention. This mode is server-owned, never a payload flag.
        _require(convention_bound == (convention is not None))
        _require(iv_convention_bound == (iv_convention is not None))
        _require(benchmark_convention_bound == (benchmark_spec is not None))
    scheduled = raw.get("schema") == _SCHEDULED_SCHEMA or (
        convention_bound and raw.get("scheduled_history") is not None
    )
    fields = {
        "schema", "symbol", "con_id", "expiration", "cutoff", "status", "sources",
        "reason_codes", "market_score", "volatility_score", "model_input_complete",
        "production_eligible", "decision_authority", "content_hash",
    }
    extra_fields = {"ema20_convention", "scheduled_history"} if convention_bound else (
        {"scheduled_history"} if scheduled else set()
    )
    if iv_convention_bound:
        extra_fields.add("iv_percentile_convention")
    if benchmark_convention_bound:
        extra_fields.add("benchmark_convention")
    _require(set(raw) == fields | extra_fields)
    _require(raw["schema"] in {
        _SCHEMA, _SCHEDULED_SCHEMA, _CONVENTION_SCHEMA, _IV_CONVENTION_SCHEMA, _BENCHMARK_CONVENTION_SCHEMA,
    }
             and raw["status"] == "INCOMPLETE")
    _require(raw["symbol"] == symbol and type(raw["con_id"]) is int and raw["con_id"] == con_id)
    _require(raw["expiration"] == expiration.isoformat() and _time(raw["cutoff"]) == instant)
    _require(raw["decision_authority"] == "OBSERVATION_ONLY")
    _require(raw["model_input_complete"] is False and raw["production_eligible"] is False)
    _require(raw["market_score"] is None and raw["volatility_score"] is None)
    reasons = raw["reason_codes"]
    _require(isinstance(reasons, (tuple, list)) and 1 <= len(reasons) <= 96)
    _require(all(isinstance(value, str) and re.fullmatch(r"[A-Z0-9_:]{1,200}", value) for value in reasons))
    required_reasons = _BENCHMARK_CONVENTION_UNRESOLVED if benchmark_convention_bound else (
        _IV_CONVENTION_UNRESOLVED if iv_convention_bound else (
            _CONVENTION_UNRESOLVED if convention_bound else _UNRESOLVED
        )
    )
    _require(len(set(reasons)) == len(reasons) and set(required_reasons).issubset(reasons))
    _require({reason for reason in reasons if reason.startswith("EMA_")} == {
        "EMA_PRODUCTION_POLICY_UNVERIFIED" if convention_bound else "EMA_CONVENTION_UNAPPROVED",
    })
    expected_iv_reasons = {"IV_BASIS_UNRESOLVED"}
    if iv_convention_bound:
        expected_iv_reasons.add("IV_PRODUCTION_POLICY_UNVERIFIED")
    _require({reason for reason in reasons if reason.startswith((
        "IV_BASIS_", "IV_PRODUCTION_", "IV_CONVENTION_",
    ))} == expected_iv_reasons)
    expected_benchmark_reasons = {
        "FEATURE_BENCHMARK_SOURCE_UNVERIFIED", "BENCHMARK_PRODUCTION_POLICY_UNVERIFIED",
    } if benchmark_convention_bound else {"FEATURE_BENCHMARK_MAPPING_UNRESOLVED"}
    _require({reason for reason in reasons if reason.startswith((
        "FEATURE_BENCHMARK_", "BENCHMARK_",
    ))} == expected_benchmark_reasons)
    if convention_bound:
        # A current semantic decision cannot be backdated or self-rehashed into
        # production policy, source proof or a human signature.
        _require(convention is not None and isinstance(raw["ema20_convention"], Mapping))
        _require(canonical_json(raw["ema20_convention"]) == canonical_json(convention))
    if iv_convention_bound:
        # Selecting native IV semantics is not evidence that current/history
        # streams are comparable; the source basis must remain unresolved.
        _require(iv_convention is not None and isinstance(raw["iv_percentile_convention"], Mapping))
        _require(canonical_json(raw["iv_percentile_convention"]) == canonical_json(iv_convention))
    if benchmark_convention_bound:
        # Confirming QQQ's comparison symbol neither approves other ETFs nor
        # establishes benchmark history, model policy or a human signature.
        _require(benchmark_spec is not None and isinstance(raw["benchmark_convention"], Mapping))
        _require(canonical_json(raw["benchmark_convention"]) == canonical_json(benchmark_spec))
    sources = raw["sources"]
    _require(isinstance(sources, Mapping) and set(sources).issubset(SOURCE_KINDS))
    for kind, reference in sources.items():
        _require(isinstance(reference, Mapping) and set(reference) == _REFERENCE_FIELDS)
        for name in ("observation_id", "row_hash", "source_hash", "request_hash", "basis_hash"):
            _require(isinstance(reference[name], str) and re.fullmatch(r"[a-f0-9]{64}", reference[name]) is not None)
        _require(type(reference["sequence"]) is int and reference["sequence"] > 0)
        available, first_seen = _time(reference["available_at"]), _time(reference["first_seen_at"])
        _require(_time(reference["source_cutoff_at"]) <= available <= first_seen <= instant)
        _require(reference["source_status"] in {"DELIVERED", "PARTIAL", "UNAVAILABLE"})
        count = reference["prior_completed_bar_count"]
        _require(count is None if kind == "CURRENT_IV" else type(count) is int and 0 <= count <= 800)
        age = reference["projection_age_seconds"]
        _require(isinstance(age, str) and len(age) <= 64)
        _require(Decimal(age) == Decimal(str((instant - available).total_seconds())))
    if scheduled:
        _validate_scheduled_references(raw["scheduled_history"], instant)
    body = dict(raw)
    supplied = body.pop("content_hash")
    _require(canonical_hash(body) == supplied and len(canonical_json(raw).encode("utf-8")) <= (180_000 if scheduled else 32_000))
    return thaw_json(freeze_json(raw))


def _validate_scheduled_references(raw: object, cutoff: datetime) -> None:
    _require(isinstance(raw, Mapping) and set(raw) == {
        "status", "fragments", "historical_session_coverage_verified", "adjustment_vintages_mergeable",
        "has_more", "next_before_sequence",
    })
    _require(raw["status"] in {"NATIVE_FRAGMENTS_ONLY", "MISSING", "UNAVAILABLE"})
    _require(raw["historical_session_coverage_verified"] is False)
    _require(raw["adjustment_vintages_mergeable"] is False)
    _require(type(raw["has_more"]) is bool)
    _require(type(raw["next_before_sequence"]) is int and raw["next_before_sequence"] > 0
             if raw["has_more"] else raw["next_before_sequence"] is None)
    rows = raw["fragments"]
    _require(isinstance(rows, (tuple, list)) and len(rows) <= 128)
    _require(bool(rows) == (raw["status"] == "NATIVE_FRAGMENTS_ONLY"))
    _require(not raw["has_more"] or bool(rows))
    seen: set[str] = set()
    for row in rows:
        _require(isinstance(row, Mapping) and set(row) == {
            "claim_id", "kind", "request_hash", "basis_hash", "prepared_request_hash", "source_hash",
            "event_id", "sequence", "row_hash", "first_seen_at", "available_at", "source_status",
        })
        _require(isinstance(row["claim_id"], str) and 1 <= len(row["claim_id"]) <= 128)
        _require(row["claim_id"] not in seen)
        seen.add(row["claim_id"])
        _require(row["kind"] in {"PRICE_HISTORY", "IV_HISTORY"})
        for name in ("request_hash", "basis_hash", "prepared_request_hash", "source_hash", "event_id", "row_hash"):
            _require(isinstance(row[name], str) and re.fullmatch(r"[a-f0-9]{64}", row[name]) is not None)
        _require(type(row["sequence"]) is int and row["sequence"] > 0)
        _require(_time(row["available_at"]) <= _time(row["first_seen_at"]) <= cutoff)
        _require(row["source_status"] in {"DELIVERED", "PARTIAL", "UNAVAILABLE", "NOT_SENT"})
    if raw["has_more"]:
        _require(raw["next_before_sequence"] == min(row["sequence"] for row in rows))


class FeatureSourceResolver:
    """Resolve locally observed source references, never fetch or compute D/V."""

    def __init__(self, store: FeatureSourceReader, *, history_store: object | None = None) -> None:
        self._store = store
        self._history_store = history_store

    def _scheduled_history(self, symbol: str, con_id: int, cutoff: datetime) -> dict[str, object]:
        from options_copilot.history_source_contracts import (
            validate_native_history_request,
            validate_native_history_result,
        )

        references: list[dict[str, object]] = []
        status = "MISSING"
        try:
            page_reader = getattr(self._history_store, "find_fragments_page", None)
            if callable(page_reader):
                page = page_reader(symbol=symbol, con_id=con_id, cutoff=cutoff, limit=128)
                rows = page["fragments"]
                has_more, next_sequence = page["has_more"], page["next_before_sequence"]
            else:
                rows = self._history_store.find_fragments(symbol=symbol, con_id=con_id, cutoff=cutoff)
                has_more, next_sequence = False, None
            _require(isinstance(rows, (tuple, list)) and len(rows) <= 128)
            for row in rows:
                prepared = validate_native_history_request(row["prepared_request"])
                fragment = validate_native_history_result(row["fragment"], prepared_request=prepared)
                _require(prepared["symbol"] == symbol and prepared["con_id"] == con_id)
                _require(fragment["claim_id"] == row["claim_id"])
                references.append({
                    "claim_id": row["claim_id"], "kind": prepared["kind"],
                    "request_hash": prepared["request_hash"], "basis_hash": prepared["basis_hash"],
                    "prepared_request_hash": prepared["content_hash"], "source_hash": fragment["content_hash"],
                    **row["reference"], "available_at": fragment["available_at"],
                    "source_status": fragment["status"],
                })
            status = "NATIVE_FRAGMENTS_ONLY" if references else "MISSING"
            result = {
                "status": status, "fragments": references,
                "has_more": has_more, "next_before_sequence": next_sequence,
                "historical_session_coverage_verified": False, "adjustment_vintages_mergeable": False,
            }
            _validate_scheduled_references(result, cutoff)
            return result
        except Exception:
            return {
                "status": "UNAVAILABLE", "fragments": [],
                "has_more": False, "next_before_sequence": None,
                "historical_session_coverage_verified": False, "adjustment_vintages_mergeable": False,
            }

    def resolve(
        self, *, symbol: str, con_id: int, expiration: date, cutoff: datetime,
    ) -> dict[str, object]:
        instant = _identity(symbol, con_id, expiration, cutoff)
        convention = ema20_convention(instant)
        iv_convention = iv_percentile_convention(instant)
        benchmark_spec = benchmark_convention(symbol, instant)
        reasons = list(_BENCHMARK_CONVENTION_UNRESOLVED if benchmark_spec is not None else (
            _IV_CONVENTION_UNRESOLVED if iv_convention is not None else (
                _CONVENTION_UNRESOLVED if convention is not None else _UNRESOLVED
            )
        ))
        sources: dict[str, object] = {}
        try:
            envelopes = self._store.read(symbol=symbol, con_id=con_id, cutoff=instant)
            _require(isinstance(envelopes, (tuple, list)) and len(envelopes) <= 3)
            for envelope in envelopes:
                source = envelope["source"]
                kind = source["kind"]
                _require(kind not in sources)
                source = validate_feature_source_observation(
                    source, kind=kind, symbol=symbol, cutoff=_time(source["cutoff_at"]),
                )
                _require(source["contract"]["con_id"] == con_id)
                reference = envelope["reference"]
                _require(reference["source_hash"] == source["content_hash"])
                available = _time(source["available_at"])
                sources[kind] = {
                    **{name: reference[name] for name in (
                        "observation_id", "sequence", "row_hash", "source_hash",
                        "request_hash", "basis_hash", "first_seen_at",
                    )},
                    "available_at": source["available_at"],
                    "source_cutoff_at": source["cutoff_at"],
                    "source_status": source["status"],
                    "prior_completed_bar_count": source.get("prior_completed_bar_count"),
                    "projection_age_seconds": str(Decimal(str((instant - available).total_seconds()))),
                }
                if source["status"] != "DELIVERED":
                    reasons.append(f"{kind}_OBSERVATION_INCOMPLETE")
                if kind != "CURRENT_IV" and not source["enough_prior_bars"]:
                    reasons.append(f"{kind}_OBSERVATION_COUNT_INSUFFICIENT")
                if kind == "CURRENT_IV":
                    received = source["received_at"]
                    if received is None or (instant - _time(received)).total_seconds() > 5:
                        reasons.append("CURRENT_IV_OBSERVATION_STALE_OR_UNAVAILABLE")
                # A current provider vintage cannot be backdated to bar dates.
                if kind == "PRICE_HISTORY":
                    reasons.append("ADJUSTED_HISTORY_VINTAGE_NOT_BACKTEST_AUTHORITY")
        except FeatureSourceStoreError as exc:
            sources = {}
            reasons.append(exc.reason_code if exc.reason_code in _CACHE_FAILURE_REASONS
                           else "FEATURE_SOURCE_CACHE_UNAVAILABLE_OR_INVALID")
        except Exception:
            # Never leak database/provider exception details or retain partial
            # references after an integrity or identity failure.
            sources = {}
            reasons.append("FEATURE_SOURCE_CACHE_UNAVAILABLE_OR_INVALID")
        for kind in SOURCE_KINDS:
            if kind not in sources:
                reasons.append(f"{kind}_OBSERVATION_MISSING")
        body = {
            "schema": _SCHEMA, "symbol": symbol, "con_id": con_id,
            "expiration": expiration.isoformat(), "cutoff": instant.isoformat(),
            "status": "INCOMPLETE", "sources": sources,
            "reason_codes": list(dict.fromkeys(reasons)),
            "market_score": None, "volatility_score": None,
            "model_input_complete": False, "production_eligible": False,
            "decision_authority": "OBSERVATION_ONLY",
        }
        if self._history_store is not None:
            scheduled = self._scheduled_history(symbol, con_id, instant)
            body["schema"] = _SCHEDULED_SCHEMA
            body["scheduled_history"] = scheduled
            body["reason_codes"].append({
                "NATIVE_FRAGMENTS_ONLY": "NATIVE_HISTORY_FRAGMENTS_NOT_MODEL_AUTHORITY",
                "MISSING": "SCHEDULED_HISTORY_FRAGMENTS_MISSING",
                "UNAVAILABLE": "SCHEDULED_HISTORY_CACHE_UNAVAILABLE_OR_INVALID",
            }[scheduled["status"]])
        if convention is not None:
            body["schema"] = _CONVENTION_SCHEMA
            body["ema20_convention"] = convention
            body.setdefault("scheduled_history", None)
        if iv_convention is not None:
            body["schema"] = _IV_CONVENTION_SCHEMA
            body["iv_percentile_convention"] = iv_convention
        if benchmark_spec is not None:
            body["schema"] = _BENCHMARK_CONVENTION_SCHEMA
            body["benchmark_convention"] = benchmark_spec
        return validate_feature_source_binding(
            {**body, "content_hash": canonical_hash(body)},
            symbol=symbol, con_id=con_id, expiration=expiration, cutoff=instant,
            require_current_convention=True,
        )


__all__ = ["FeatureSourceReader", "FeatureSourceResolver", "validate_feature_source_binding"]
