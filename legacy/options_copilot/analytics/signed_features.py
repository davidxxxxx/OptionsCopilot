"""Offline unverified D/V preview; transport and authority remain external.

Conventions: prior completed daily bars, fixed-60 SMA-seeded EMA20, sample daily log
return volatility annualized by sqrt(252), empirical IV CDF with <= ties,
and bracketed linear surface interpolation without extrapolation.  This
module never requests data, substitutes research scores, or grants eligibility.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from types import MappingProxyType
from zoneinfo import ZoneInfo

from options_copilot.analytics.benchmark import benchmark_convention
from options_copilot.analytics.ema20 import calculate_ema20, ema20_convention
from options_copilot.analytics.iv_percentile import (
    calculate_iv_percentile,
    iv_percentile_convention,
)
from options_copilot.analytics.scenarios import ResolvedPolicy
from options_copilot.analytics.feature_contracts import SeriesBasisContract
from options_copilot.storage.canonical import canonical_hash, freeze_json, utc_datetime


FORMULA_VERSION = "offline-market-features.ema60.v2"
CONFIRMED_EMA20_FORMULA_VERSION = "offline-market-features.ema60.v3"
CONFIRMED_IV_PERCENTILE_FORMULA_VERSION = "offline-market-features.ema60.iv252.v4"
CONFIRMED_QQQ_SPY_FORMULA_VERSION = "offline-market-features.ema60.iv252.qqq-spy.v5"
SECTOR_BENCHMARKS = MappingProxyType({
    "COMMUNICATION_SERVICES": "XLC", "CONSUMER_DISCRETIONARY": "XLY",
    "CONSUMER_STAPLES": "XLP", "ENERGY": "XLE", "FINANCIALS": "XLF",
    "HEALTH_CARE": "XLV", "INDUSTRIALS": "XLI", "INFORMATION_TECHNOLOGY": "XLK",
    "MATERIALS": "XLB", "REAL_ESTATE": "XLRE", "UTILITIES": "XLU",
})
_D_WEIGHTS = {
    "trend_20d": "0.35", "momentum_5d": "0.25",
    "sector_relative_strength_20d": "0.20", "price_volume_confirmation": "0.20",
}
_V_WEIGHTS = {
    "iv_percentile": "0.30", "term_structure": "0.25",
    "skew_tail_pressure": "0.25", "realized_implied_gap": "0.20",
}
_NORMALIZATION = {
    "common_rule": "all inputs are computed point in time from hash-bound prior-only observations and clipped to [-1,1]; missing inputs are not imputed",
    "iv_percentile": "2*point_in_time_252_session_percentile-1",
    "momentum_5d": "clip(return_5d/(1.5*realized_volatility_20d*sqrt(5/252)),-1,1)",
    "price_volume_confirmation": "sign(return_1d)*clip(relative_volume_20d-1,0,1)",
    "realized_implied_gap": "clip((realized_volatility_20d-atm_iv)/max(atm_iv,0.01),-1,1)",
    "sector_relative_strength_20d": "clip((underlying_return_20d-sector_return_20d)/(2*realized_volatility_20d),-1,1)",
    "skew_tail_pressure": "clip(abs(put_25_delta_iv-call_25_delta_iv)/max(atm_iv,0.01),-1,1)",
    "term_structure": "clip((near_atm_iv-next_regular_atm_iv)/max(next_regular_atm_iv,0.01),-1,1)",
    "trend_20d": "clip(log(spot/ema_20d)/(2*realized_volatility_20d),-1,1)",
    "zero_or_nonfinite_denominator": "NO_TRADE",
}


class FeatureUnavailable(ValueError):
    """Exact missing/invalid input; callers must retain NO_TRADE."""


@dataclass(frozen=True, slots=True)
class DailyFeatureBar:
    trading_date: date
    close: Decimal
    volume: Decimal | None


@dataclass(frozen=True, slots=True)
class FeatureSurfacePoint:
    contract_id: int
    symbol: str
    expiration: date
    strike: Decimal
    right: str
    iv: Decimal
    delta: Decimal
    observed_at: datetime
    exchange_time: datetime
    bid: Decimal
    ask: Decimal
    market_data_type: int
    secdef_hash: str


@dataclass(frozen=True, slots=True)
class MarketFeaturePreview:
    symbol: str
    expiration: date
    market_score: Decimal
    volatility_score: Decimal
    atm_iv: Decimal
    manifest: Mapping[str, object]
    manifest_hash: str


def build_signed_market_features(**kwargs: object) -> MarketFeaturePreview:
    """Retain a fail-closed legacy entry point until production authority exists."""
    raise FeatureUnavailable("FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED")


def build_market_feature_preview(**kwargs: object) -> MarketFeaturePreview:
    """Calculate only a preview; policy hashes do not approve these conventions."""
    with localcontext() as ctx:
        ctx.prec = 28
        return _build_market_feature_preview(**kwargs)


def _build_market_feature_preview(
    *, symbol: str, sector: str, benchmark_symbol: str,
    prices: Sequence[DailyFeatureBar], benchmark: Sequence[DailyFeatureBar],
    iv_history: Sequence[tuple[date, Decimal]],
    percentile_current_iv: Decimal, percentile_observed_at: datetime,
    iv_history_basis: SeriesBasisContract, percentile_basis: SeriesBasisContract,
    surface: Sequence[FeatureSurfacePoint],
    spot: Decimal, spot_observed_at: datetime, expiration: date,
    next_expiration: date, cutoff: datetime, policy: ResolvedPolicy,
    source_manifest: Mapping[str, object],
) -> MarketFeaturePreview:
    """Compute experimental formulas from unverified, content-checked inputs.

    `source_manifest` describes the adapter's acquisition proof.  Its hash is
    not a human signature or a substitute for validation at the broker port.
    Calendar and basis contents are rehashed, but their provenance is unresolved.
    A future cache-only resolver must establish source and calendar authority;
    this function is deliberately unavailable to production callers.
    """
    cutoff = utc_datetime(cutoff, field="feature cutoff")
    confirmed_ema20_convention = ema20_convention(cutoff)
    confirmed_iv_percentile_convention = iv_percentile_convention(cutoff)
    confirmed_benchmark_convention = benchmark_convention(symbol, cutoff)
    if not _bounded_identifier(sector):
        raise FeatureUnavailable("SECTOR_BENCHMARK_UNDEFINED")
    if confirmed_benchmark_convention is None:
        expected_benchmark = SECTOR_BENCHMARKS.get(sector)
        if expected_benchmark is None:
            raise FeatureUnavailable("SECTOR_BENCHMARK_UNDEFINED")
    else:
        expected_benchmark = confirmed_benchmark_convention["benchmark_symbol"]
    if not symbol or symbol != symbol.strip().upper() or benchmark_symbol != expected_benchmark:
        raise FeatureUnavailable("SECTOR_BENCHMARK_MISMATCH")
    _validate_policy(policy)
    for field in ("price_hash", "benchmark_hash", "iv_history_hash", "surface_hash", "calendar_hash", "spot_hash"):
        if not _digest(source_manifest.get(field)):
            raise FeatureUnavailable("FEATURE_SOURCE_MANIFEST_INCOMPLETE")
    if source_manifest.get("source") != "IBKR" or source_manifest.get("price_basis") != "ADJUSTED_LAST":
        raise FeatureUnavailable("FEATURE_PRICE_BASIS_UNSUPPORTED")
    if source_manifest.get("iv_basis") != "OPTION_IMPLIED_VOLATILITY":
        raise FeatureUnavailable("FEATURE_IV_BASIS_UNSUPPORTED")
    expected_sessions = _calendar_sessions(source_manifest, cutoff, expiration, next_expiration)
    local_day = cutoff.astimezone(ZoneInfo("America/New_York")).date()
    if not 60 <= len(prices) <= 600:
        raise FeatureUnavailable("FEATURE_EMA_WARMUP_INSUFFICIENT")
    if not 21 <= len(benchmark) <= 600:
        raise FeatureUnavailable("FEATURE_BENCHMARK_HISTORY_INSUFFICIENT")
    if len(iv_history) != 252:
        raise FeatureUnavailable("IV_HISTORY_INSUFFICIENT")
    for series, count in ((prices, 60), (benchmark, 21)):
        days = tuple(row.trading_date for row in series)
        if (any(type(day) is not date for day in days) or days != tuple(sorted(set(days)))
                or days[-count:] != expected_sessions[-count:]):
            raise FeatureUnavailable("FEATURE_PRICE_SESSIONS_MISMATCH")
    if tuple(day for day, _value in iv_history) != expected_sessions:
        raise FeatureUnavailable("FEATURE_IV_SESSIONS_MISMATCH")
    for series in (prices, benchmark):
        for row in series:
            _positive(row.close, "FEATURE_PRICE_INVALID")
    for row in prices[-21:]:
        if not _bounded_decimal(row.volume) or row.volume < 0:
            raise FeatureUnavailable("FEATURE_VOLUME_INVALID")
    for _day, value in iv_history:
        _positive(value, "FEATURE_IV_HISTORY_INVALID")
    _positive(spot, "FEATURE_SPOT_INVALID")
    _fresh(spot_observed_at, cutoff)
    _positive(percentile_current_iv, "FEATURE_PERCENTILE_CURRENT_IV_INVALID")
    _fresh(percentile_observed_at, cutoff)
    if not isinstance(iv_history_basis, SeriesBasisContract) or not isinstance(percentile_basis, SeriesBasisContract):
        raise FeatureUnavailable("IV_BASIS_UNRESOLVED")
    if (iv_history_basis.series_kind != "IV" or percentile_basis.series_kind != "IV"
            or iv_history_basis.contract_hash != percentile_basis.contract_hash
            or iv_history_basis.calendar_hash != source_manifest["calendar_hash"]):
        raise FeatureUnavailable("IV_BASIS_MISMATCH")
    if iv_history_basis.horizon == "PROVIDER_NATIVE_UNRESOLVED":
        raise FeatureUnavailable("IV_BASIS_UNRESOLVED")
    if (type(expiration) is not date or type(next_expiration) is not date
            or not local_day < expiration < next_expiration):
        raise FeatureUnavailable("FEATURE_REGULAR_EXPIRATION_INVALID")
    if not surface or len(surface) > 50:
        raise FeatureUnavailable("FEATURE_SURFACE_UNAVAILABLE")
    seen: set[int] = set()
    for point in surface:
        if (type(point.contract_id) is not int or point.contract_id <= 0 or point.contract_id in seen
                or point.symbol != symbol or point.expiration not in (expiration, next_expiration)
                or point.right not in {"C", "P"} or type(point.market_data_type) is not int
                or point.market_data_type != 1 or not _digest(point.secdef_hash)):
            raise FeatureUnavailable("FEATURE_SURFACE_IDENTITY_INVALID")
        seen.add(point.contract_id)
        _fresh(point.observed_at, cutoff)
        _fresh(point.exchange_time, cutoff)
        if point.exchange_time > point.observed_at:
            raise FeatureUnavailable("FEATURE_QUOTE_SOURCE_AFTER_RECEIPT")
        for value in (point.strike, point.iv, point.bid, point.ask):
            _positive(value, "FEATURE_SURFACE_VALUE_INVALID")
        if point.ask <= point.bid or not _bounded_decimal(point.delta):
            raise FeatureUnavailable("FEATURE_SURFACE_VALUE_INVALID")
        if not (Decimal(0) < (point.delta if point.right == "C" else -point.delta) < Decimal(1)):
            raise FeatureUnavailable("FEATURE_SURFACE_DELTA_INVALID")
    actual_hashes = {
        "price_hash": canonical_hash(tuple(asdict(row) for row in prices)),
        "benchmark_hash": canonical_hash(tuple(asdict(row) for row in benchmark)),
        "iv_history_hash": canonical_hash(tuple(iv_history)),
        "surface_hash": canonical_hash(tuple(asdict(row) for row in surface)),
        "spot_hash": canonical_hash({"symbol": symbol, "spot": spot, "observed_at": spot_observed_at}),
        "percentile_current_hash": canonical_hash({"value": percentile_current_iv, "observed_at": percentile_observed_at,
                                                   "basis_contract_hash": percentile_basis.contract_hash}),
        "iv_basis_hash": iv_history_basis.contract_hash,
    }
    if any(source_manifest.get(key) != value for key, value in actual_hashes.items()):
        raise FeatureUnavailable("FEATURE_SOURCE_HASH_MISMATCH")
    with localcontext() as ctx:
        ctx.prec = 28
        near = tuple(point for point in surface if point.expiration == expiration)
        nxt = tuple(point for point in surface if point.expiration == next_expiration)
        # Use the same right for both ATM term points; never selected-leg mean IV.
        atm = _interpolate([(p.strike, p.iv) for p in near if p.right == "C"], spot)
        next_atm = _interpolate([(p.strike, p.iv) for p in nxt if p.right == "C"], spot)
        call25 = _interpolate([(p.delta, p.iv) for p in near if p.right == "C"], Decimal("0.25"))
        put25 = _interpolate([(-p.delta, p.iv) for p in near if p.right == "P"], Decimal("0.25"))
        closes = tuple(row.close for row in prices[-60:])
        log_returns = tuple((closes[i] / closes[i-1]).ln() for i in range(len(closes)-20, len(closes)))
        mean = sum(log_returns) / Decimal(20)
        rv = (sum((value-mean)**2 for value in log_returns) / Decimal(19) * Decimal(252)).sqrt()
        _positive(rv, "FEATURE_REALIZED_VOLATILITY_ZERO")
        ema = calculate_ema20(closes)
        ret1 = closes[-1] / closes[-2] - 1
        ret5 = closes[-1] / closes[-6] - 1
        ret20 = closes[-1] / closes[-21] - 1
        sector_return = benchmark[-1].close / benchmark[-21].close - 1
        average_volume = sum(row.volume for row in prices[-21:-1]) / Decimal(20)
        _positive(average_volume, "FEATURE_VOLUME_DENOMINATOR_ZERO")
        relative_volume = prices[-1].volume / average_volume
        percentile = calculate_iv_percentile(
            percentile_current_iv,
            tuple(value for _day, value in iv_history),
        )
        features = {
            "trend_20d": _clip((spot / ema).ln() / (2*rv)),
            "momentum_5d": _clip(ret5 / (Decimal("1.5")*rv*(Decimal(5)/252).sqrt())),
            "sector_relative_strength_20d": _clip((ret20-sector_return)/(2*rv)),
            "price_volume_confirmation": (Decimal(1) if ret1 > 0 else Decimal(-1) if ret1 < 0 else Decimal(0)) * max(Decimal(0), min(Decimal(1), relative_volume-1)),
            "iv_percentile": 2*percentile-1,
            "term_structure": _clip((atm-next_atm)/max(next_atm, Decimal("0.01"))),
            "skew_tail_pressure": _clip(abs(put25-call25)/max(atm, Decimal("0.01"))),
            "realized_implied_gap": _clip((rv-atm)/max(atm, Decimal("0.01"))),
        }
        direction = sum(features[key]*Decimal(weight) for key, weight in _D_WEIGHTS.items())
        volatility = sum(features[key]*Decimal(weight) for key, weight in _V_WEIGHTS.items())
    manifest_body = {
        "schema": (
            "options_copilot.market_feature_preview.v5"
            if confirmed_benchmark_convention is not None
            else "options_copilot.market_feature_preview.v2"
            if confirmed_ema20_convention is None
            else "options_copilot.market_feature_preview.v3"
            if confirmed_iv_percentile_convention is None
            else "options_copilot.market_feature_preview.v4"
        ),
        "calculation_status": "COMPLETE", "feature_status": "RESEARCH_ONLY",
        "input_mode": "OFFLINE_UNVERIFIED", "point_in_time_verified": False,
        "acquisition_contracts_verified": False,
        "decision_authority": "SUPPORTING_ONLY", "affects_eligibility": False,
        "model_input_complete": False, "production_eligible": False,
        "unresolved_decisions": (
            ("D1_EMA_SEMANTICS", "D2_IV_BASIS")
            if confirmed_ema20_convention is None
            else ("D2_IV_BASIS",)
            if confirmed_iv_percentile_convention is None
            else ()
        ),
        "source_authority_status": "UNRESOLVED", "calendar_authority_status": "UNRESOLVED",
        "formula_version": (
            CONFIRMED_QQQ_SPY_FORMULA_VERSION
            if confirmed_benchmark_convention is not None
            else FORMULA_VERSION
            if confirmed_ema20_convention is None
            else CONFIRMED_EMA20_FORMULA_VERSION
            if confirmed_iv_percentile_convention is None
            else CONFIRMED_IV_PERCENTILE_FORMULA_VERSION
        ), "symbol": symbol, "sector": sector,
        "benchmark_symbol": benchmark_symbol,
        "benchmark_mapping_hash": (
            canonical_hash({
                "symbol": symbol,
                "benchmark_symbol": benchmark_symbol,
                "benchmark_convention": confirmed_benchmark_convention,
            })
            if confirmed_benchmark_convention is not None
            else canonical_hash(dict(SECTOR_BENCHMARKS))
        ),
        "expiration": expiration, "next_expiration": next_expiration, "cutoff": cutoff,
        "compared_policy_hash": policy.current_policy_hash, "compared_policy_marker_hash": policy.policy_authority_marker_hash,
        "policy_compatibility": "UNRESOLVED",
        "windows": {"prices": 60, "benchmark": 21, "iv_history": 252, "ema_seed": "FIRST20_SMA_THEN40_UPDATES"},
        "percentile_current_iv": percentile_current_iv, "iv_basis_contract": iv_history_basis.as_dict(),
        "source_manifest": dict(source_manifest), "features": features,
        "market_score": direction, "volatility_score": volatility, "atm_iv": atm,
        "realized_volatility_20d": rv, "ema_20d": ema,
    }
    if confirmed_ema20_convention is not None:
        manifest_body["ema20_convention"] = confirmed_ema20_convention
    if confirmed_iv_percentile_convention is not None:
        manifest_body["iv_percentile_convention"] = confirmed_iv_percentile_convention
        manifest_body["convention_application_status"] = "NOT_APPLIED"
        manifest_body["iv_native_comparability_verified"] = False
    if confirmed_benchmark_convention is not None:
        manifest_body["benchmark_convention"] = confirmed_benchmark_convention
    manifest = freeze_json(manifest_body)
    return MarketFeaturePreview(symbol, expiration, direction, volatility, atm, manifest, canonical_hash(manifest))


def _calendar_sessions(
    manifest: Mapping[str, object], cutoff: datetime, expiration: date, next_expiration: date,
) -> tuple[date, ...]:
    """Validate preview calendar content, never authenticate an exchange source."""
    calendar = manifest.get("calendar")
    if not isinstance(calendar, Mapping) or calendar.get("schema") != "options_copilot.feature_calendar_preview.v1":
        raise FeatureUnavailable("FEATURE_SESSION_CALENDAR_INCOMPLETE")
    if canonical_hash(calendar) != manifest.get("calendar_hash"):
        raise FeatureUnavailable("FEATURE_CALENDAR_HASH_MISMATCH")
    try:
        available = utc_datetime(calendar.get("available_at"))
        rows = calendar.get("sessions")
        if available > cutoff or not isinstance(rows, (tuple, list)) or len(rows) != 252:
            raise ValueError("calendar unavailable")
        dates: list[date] = []
        for row in rows:
            day = row["session_date"]
            close = utc_datetime(row["close_at"])
            if (type(day) is not date or close >= cutoff
                    or close.astimezone(ZoneInfo("America/New_York")).date() != day):
                raise ValueError("session unfinished or misaligned")
            dates.append(day)
        if dates != sorted(set(dates)):
            raise ValueError("sessions unordered")
        expirations = calendar.get("regular_expirations")
        if (not isinstance(expirations, (tuple, list)) or not 2 <= len(expirations) <= 24
                or any(type(day) is not date for day in expirations)
                or tuple(expirations) != tuple(sorted(set(expirations)))):
            raise ValueError("expiration calendar unavailable")
    except (TypeError, ValueError, KeyError) as exc:
        raise FeatureUnavailable("FEATURE_SESSION_CALENDAR_INVALID") from exc
    if expiration not in expirations or next_expiration not in expirations:
        raise FeatureUnavailable("FEATURE_REGULAR_EXPIRATION_INVALID")
    if expirations.index(next_expiration) != expirations.index(expiration) + 1:
        raise FeatureUnavailable("FEATURE_REGULAR_EXPIRATION_INVALID")
    return tuple(dates)


def _validate_policy(policy: ResolvedPolicy) -> None:
    if not isinstance(policy, ResolvedPolicy) or not _digest(policy.current_policy_hash) or not _digest(policy.policy_authority_marker_hash):
        raise FeatureUnavailable("SIGNED_FEATURE_POLICY_UNAVAILABLE")
    if not isinstance(policy.payload, Mapping):
        raise FeatureUnavailable("SIGNED_FEATURE_POLICY_UNSUPPORTED")
    weights = policy.payload.get("baseline_feature_weights", {})
    if (not isinstance(weights, Mapping)
            or not isinstance(weights.get("market_direction_score"), Mapping)
            or not isinstance(weights.get("volatility_state_score"), Mapping)):
        raise FeatureUnavailable("SIGNED_FEATURE_POLICY_UNSUPPORTED")
    if (not isinstance(weights, Mapping)
            or weights.get("market_direction_score", {}).get("weights") != _D_WEIGHTS
            or weights.get("volatility_state_score", {}).get("weights") != _V_WEIGHTS
            or policy.payload.get("feature_normalization") != _NORMALIZATION):
        raise FeatureUnavailable("SIGNED_FEATURE_POLICY_UNSUPPORTED")


def _interpolate(points: Sequence[tuple[Decimal, Decimal]], target: Decimal) -> Decimal:
    by_x: dict[Decimal, Decimal] = {}
    for x, y in points:
        if x in by_x and by_x[x] != y:
            raise FeatureUnavailable("FEATURE_SURFACE_CONFLICT")
        by_x[x] = y
    if target in by_x:
        return by_x[target]
    lower = [(x, y) for x, y in by_x.items() if x < target]
    upper = [(x, y) for x, y in by_x.items() if x > target]
    if not lower or not upper:
        raise FeatureUnavailable("FEATURE_SURFACE_BRACKET_MISSING")
    x0, y0 = max(lower)
    x1, y1 = min(upper)
    return y0+(y1-y0)*(target-x0)/(x1-x0)


def _positive(value: object, reason: str) -> None:
    if not _bounded_decimal(value) or value <= 0:
        raise FeatureUnavailable(reason)


def _bounded_decimal(value: object) -> bool:
    return (isinstance(value, Decimal) and value.is_finite()
            and len(value.as_tuple().digits) <= 64 and -64 <= value.as_tuple().exponent <= 64)


def _fresh(value: datetime, cutoff: datetime) -> None:
    try:
        at = utc_datetime(value, field="feature timestamp")
    except (TypeError, ValueError) as exc:
        raise FeatureUnavailable("FEATURE_TIMESTAMP_INVALID") from exc
    if not timedelta(0) <= cutoff-at <= timedelta(seconds=5):
        raise FeatureUnavailable("FEATURE_QUOTE_STALE_OR_FUTURE")


def _digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _bounded_identifier(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 64
        and "A" <= value[0] <= "Z"
        and all("A" <= character <= "Z" or "0" <= character <= "9" or character == "_"
                for character in value)
    )


def _clip(value: Decimal) -> Decimal:
    return max(Decimal(-1), min(Decimal(1), value))


__all__ = ["DailyFeatureBar", "FeatureSurfacePoint", "FeatureUnavailable", "MarketFeaturePreview",
           "build_market_feature_preview", "build_signed_market_features"]
