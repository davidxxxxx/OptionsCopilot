"""Bayesian calibration of news-ledger drift against next-day underlying moves."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence

from copilot.analysis.models import View
from copilot.config import NewsCalibrationSettings
from copilot.market.calendar import is_trading_day
from copilot.market.history import realized_vol
from copilot.store import Store


@dataclass(frozen=True)
class CalibrationSample:
    day: date
    symbol: str
    conviction: int
    x: float
    y: float
    volatility_source: str = "iv30"


@dataclass(frozen=True)
class SampleBuild:
    samples: tuple[CalibrationSample, ...]
    candidates: int
    skipped_missing_close: int
    skipped_missing_iv: int


def build_samples(
    candidates: Sequence[Mapping[str, Any]],
    closes_by_symbol: Mapping[str, Sequence[tuple[date, float]]],
) -> SampleBuild:
    """Turn selected pre-close ledger views into standardized next-day samples."""

    samples: list[CalibrationSample] = []
    eligible = skipped_close = skipped_iv = 0
    for row in candidates:
        view = row.get("view") or {}
        if (
            view.get("source", "ledger") != "ledger"
            or view.get("direction") not in {"bullish", "bearish"}
            or view.get("drift_sigma") is None
            or not view.get("horizon_days")
        ):
            continue
        eligible += 1
        day = date.fromisoformat(str(row["trading_date"]))
        symbol = str(row["symbol"])
        close_rows = sorted(closes_by_symbol.get(symbol, ()))
        closes = {close_day: float(close) for close_day, close in close_rows}
        iv = _positive_number((row.get("underlying") or {}).get("iv30"))
        volatility_source = "iv30"
        if iv is None:
            iv = _positive_number((row.get("market_state") or {}).get("iv_30d"))
            volatility_source = "market_state"
        if iv is None:
            iv = _realized_vol_on(close_rows, day)
            volatility_source = "hv20"
        if iv is None:
            skipped_iv += 1
            continue
        next_day = _next_trading_day(day)
        close = _positive_number(closes.get(day))
        next_close = _positive_number(closes.get(next_day))
        if close is None or next_close is None:
            skipped_close += 1
            continue
        horizon = int(view["horizon_days"])
        daily_sigma = iv / math.sqrt(252)
        predicted = float(view["drift_sigma"]) * iv * math.sqrt(horizon / 365) / horizon
        sign = 1.0 if view["direction"] == "bullish" else -1.0
        samples.append(CalibrationSample(
            day=day,
            symbol=symbol,
            conviction=int(view.get("conviction") or 1),
            x=predicted / daily_sigma,
            y=sign * math.log(next_close / close) / daily_sigma,
            volatility_source=volatility_source,
        ))
    return SampleBuild(tuple(samples), eligible, skipped_close, skipped_iv)


def estimate_calibration(
    samples: Sequence[CalibrationSample],
    settings: NewsCalibrationSettings,
    *,
    computed_at: datetime | None = None,
) -> dict[str, Any]:
    """Fit ``y = k*x + error`` with a clustered Bayesian ridge prior."""

    core = _estimate(samples, settings)
    buckets = {
        str(conviction): _diagnostics(
            [sample for sample in samples if sample.conviction == conviction], settings
        )
        for conviction in range(2, 6)
    }
    moment = computed_at or datetime.now(timezone.utc)
    return {
        **core,
        "by_conviction": buckets,
        "prior": {"k0": settings.k0, "tau": settings.tau, "rho": settings.rho},
        "computed_at": moment.astimezone(timezone.utc).isoformat(timespec="seconds"),
    }


def compute_calibration(
    store: Store,
    settings: NewsCalibrationSettings,
    *,
    today: date,
    index_history: Callable[[str, date], list[tuple[date, float]]] | None = None,
    computed_at: datetime | None = None,
) -> tuple[dict[str, Any], SampleBuild]:
    """Build samples from stored runs, using cached or supplied Nasdaq closes."""

    candidates = store.news_calibration_candidates()
    closes_by_symbol: dict[str, list[tuple[date, float]]] = {}
    for symbol in sorted({str(row["symbol"]) for row in candidates}):
        exact = store.get_daily_cache(symbol, "NASDAQ_CLOSE", today)
        cached = exact if exact is not None else store.latest_daily_cache(symbol, "NASDAQ_CLOSE")
        closes = _parse_closes(cached or ())
        if exact is None and index_history is not None:
            try:
                fresh = index_history(symbol, today)
            except Exception:
                fresh = []
            if fresh:
                closes = _merge_closes(closes, fresh)
                store.put_daily_cache(
                    symbol,
                    "NASDAQ_CLOSE",
                    today,
                    [[day.isoformat(), close] for day, close in closes],
                )
        closes_by_symbol[symbol] = closes
    built = build_samples(candidates, closes_by_symbol)
    return estimate_calibration(built.samples, settings, computed_at=computed_at), built


def current_calibration(
    stored: Mapping[str, Any] | None,
    settings: NewsCalibrationSettings,
) -> dict[str, Any]:
    """Normalize stored state and derive its verification flag."""

    if not settings.enabled:
        payload = {
            "k": 1.0,
            "k_raw": 1.0,
            "sd": 0.0,
            "samples": 0,
            "days": 0,
            "hit_rate": None,
            "mean_x": None,
            "mean_y": None,
            "volatility_sources": {"iv30": 0, "market_state": 0, "hv20": 0},
            "by_conviction": {},
            "prior": {"k0": settings.k0, "tau": settings.tau, "rho": settings.rho},
            "computed_at": None,
        }
    elif stored:
        payload = dict(stored)
    else:
        payload = estimate_calibration((), settings)
        payload["computed_at"] = None
    payload["verified"] = not settings.enabled or bool(
        int(payload.get("days") or 0) >= settings.min_days_verified
        and float(payload.get("sd") or math.inf) <= settings.max_sd_verified
    )
    return payload


def calibrated_view(view: View, factor: float) -> View:
    """Return a pricing-only copy; non-ledger views and stored views stay unchanged."""

    if view.source != "ledger" or view.drift_sigma is None:
        return view
    return replace(view, drift_sigma=view.drift_sigma * factor)


def _estimate(
    samples: Sequence[CalibrationSample], settings: NewsCalibrationSettings
) -> dict[str, Any]:
    sample_count = len(samples)
    days = len({sample.day for sample in samples})
    mean_per_day = sample_count / days if days else 0.0
    design_effect = 1 + max(mean_per_day - 1, 0.0) * settings.rho
    sxx = sum(sample.x * sample.x for sample in samples) / design_effect
    sxy = sum(sample.x * sample.y for sample in samples) / design_effect
    prior_precision = 1 / (settings.tau * settings.tau)
    posterior_precision = prior_precision + sxx
    raw = (settings.k0 * prior_precision + sxy) / posterior_precision
    return {
        "k": min(max(raw, 0.0), 1.0),
        "k_raw": raw,
        "sd": 1 / math.sqrt(posterior_precision),
        "samples": sample_count,
        "days": days,
        "hit_rate": (
            sum(sample.y > 0 for sample in samples) / sample_count if sample_count else None
        ),
        "mean_x": sum(sample.x for sample in samples) / sample_count if sample_count else None,
        "mean_y": sum(sample.y for sample in samples) / sample_count if sample_count else None,
        "volatility_sources": {
            source: sum(sample.volatility_source == source for sample in samples)
            for source in ("iv30", "market_state", "hv20")
        },
    }


def _diagnostics(
    samples: Sequence[CalibrationSample], settings: NewsCalibrationSettings
) -> dict[str, Any]:
    return _estimate(samples, settings)


def _next_trading_day(day: date) -> date:
    candidate = day + timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _positive_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _realized_vol_on(
    closes: Sequence[tuple[date, float]], day: date
) -> float | None:
    history = [
        (close_day, float(close))
        for close_day, close in closes
        if close_day <= day and _positive_number(close) is not None
    ]
    if not history or history[-1][0] != day:
        return None
    return realized_vol([close for _close_day, close in history], window=20)


def _parse_closes(rows: Sequence[Sequence[Any]]) -> list[tuple[date, float]]:
    parsed: list[tuple[date, float]] = []
    for row in rows:
        try:
            day = date.fromisoformat(str(row[0]))
            close = float(row[1])
        except (IndexError, TypeError, ValueError):
            continue
        if close > 0 and math.isfinite(close):
            parsed.append((day, close))
    return sorted(dict(parsed).items())


def _merge_closes(
    cached: Sequence[tuple[date, float]], fresh: Sequence[tuple[date, float]]
) -> list[tuple[date, float]]:
    merged = dict(cached)
    merged.update(fresh)
    return sorted(merged.items())


__all__ = [
    "CalibrationSample",
    "SampleBuild",
    "build_samples",
    "calibrated_view",
    "compute_calibration",
    "current_calibration",
    "estimate_calibration",
]
