"""Bayesian calibration of news-ledger drift against next-day underlying moves."""
from __future__ import annotations

import math
import random
import statistics
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
    volatility: str = "neutral"
    absolute_move: float = 0.0


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
            volatility=str(view.get("volatility") or "neutral"),
            absolute_move=abs(math.log(next_close / close)) / daily_sigma,
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


def estimate_vol_calibration(
    samples: Sequence[CalibrationSample],
    settings: NewsCalibrationSettings | None = None,
    *,
    computed_at: datetime | None = None,
    bootstrap_draws: int = 1_000,
) -> dict[str, Any]:
    """Estimate expand/contract multipliers with a fixed-seed day bootstrap.

    Days, rather than individual symbols, are resampled so same-day observations remain
    clustered. The raw implied calibration weight is shrunk toward N(0.5, 0.5²).
    """

    config = settings or NewsCalibrationSettings()
    usable = [
        sample for sample in samples
        if sample.volatility in {"expand", "contract", "neutral"}
        and math.isfinite(sample.absolute_move)
        and sample.absolute_move >= 0
    ]
    expand = _vol_side(
        usable, "expand", 1.15, bootstrap_draws, config.min_vol_days
    )
    contract = _vol_side(
        usable, "contract", 0.85, bootstrap_draws, config.min_vol_days
    )
    moment = computed_at or datetime.now(timezone.utc)
    return {
        "k_vol_expand": expand["k"],
        "k_vol_contract": contract["k"],
        "expand": expand,
        "contract": contract,
        "samples": len(usable),
        "days": len({sample.day for sample in usable}),
        "computed_at": moment.astimezone(timezone.utc).isoformat(timespec="seconds"),
        "method": "fixed-seed day bootstrap",
        "min_vol_days": config.min_vol_days,
    }


def current_vol_calibration(
    stored: Mapping[str, Any] | None,
    settings: NewsCalibrationSettings | None = None,
) -> dict[str, Any]:
    if stored:
        return dict(stored)
    payload = estimate_vol_calibration((), settings)
    payload["computed_at"] = None
    return payload


def calibrated_view(
    view: View,
    factor: float,
    vol_calibration: Mapping[str, Any] | None = None,
) -> View:
    """Return a pricing-only copy; non-ledger views and stored views stay unchanged."""

    if view.source != "ledger":
        return view
    changes: dict[str, Any] = {}
    if view.drift_sigma is not None:
        changes["drift_sigma"] = view.drift_sigma * factor
    if view.volatility in {"expand", "contract"}:
        base = {"expand": 1.15, "contract": 0.85}[view.volatility]
        key = f"k_vol_{view.volatility}"
        weight = float((vol_calibration or {}).get(key, 0.5))
        changes["volatility_multiplier"] = 1 + min(max(weight, 0.0), 1.0) * (base - 1)
    return replace(view, **changes) if changes else view


def _vol_side(
    samples: Sequence[CalibrationSample],
    side: str,
    base_multiplier: float,
    bootstrap_draws: int,
    min_days: int,
) -> dict[str, Any]:
    side_samples = [sample for sample in samples if sample.volatility == side]
    neutral = [sample for sample in samples if sample.volatility == "neutral"]
    relevant = [*side_samples, *neutral]
    days = sorted({sample.day for sample in relevant})
    ratio = _move_ratio(side_samples, neutral)
    raw = (
        (ratio - 1) / (base_multiplier - 1)
        if ratio is not None
        else None
    )
    if ratio is None or len(days) < min_days:
        return _vol_prior(side_samples, neutral, ratio, raw)
    grouped = {day: [sample for sample in relevant if sample.day == day] for day in days}
    rng = random.Random(20261014)
    ratios: list[float] = []
    for _ in range(max(bootstrap_draws, 1)):
        draw = [rng.choice(days) for _day in days]
        resampled = [sample for day in draw for sample in grouped[day]]
        value = _move_ratio(
            [sample for sample in resampled if sample.volatility == side],
            [sample for sample in resampled if sample.volatility == "neutral"],
        )
        if value is not None:
            ratios.append(value)
    ratio_se = statistics.pstdev(ratios) if len(ratios) > 1 else None
    if ratio_se is None:
        return _vol_prior(side_samples, neutral, ratio, raw)
    k_se = max(ratio_se / abs(base_multiplier - 1), 0.25)
    prior_precision = 1 / (0.5 * 0.5)
    sample_precision = 1 / (k_se * k_se)
    assert raw is not None
    posterior = (0.5 * prior_precision + raw * sample_precision) / (
        prior_precision + sample_precision
    )
    posterior_sd = 1 / math.sqrt(prior_precision + sample_precision)
    return {
        "k": min(max(posterior, 0.0), 1.0),
        "k_raw": raw,
        "k_posterior_raw": posterior,
        "ratio": ratio,
        "ratio_se": ratio_se,
        "k_se": k_se,
        "sd": posterior_sd,
        "samples": len(side_samples),
        "neutral_samples": len(neutral),
        "days": len(days),
    }


def _vol_prior(
    side_samples: Sequence[CalibrationSample],
    neutral: Sequence[CalibrationSample],
    ratio: float | None,
    raw: float | None = None,
) -> dict[str, Any]:
    return {
        "k": 0.5,
        "k_raw": 0.5 if raw is None else raw,
        "k_posterior_raw": 0.5,
        "ratio": ratio,
        "ratio_se": None,
        "k_se": None,
        "sd": 0.5,
        "samples": len(side_samples),
        "neutral_samples": len(neutral),
        "days": len({sample.day for sample in (*side_samples, *neutral)}),
    }


def _move_ratio(
    side_samples: Sequence[CalibrationSample],
    neutral: Sequence[CalibrationSample],
) -> float | None:
    if not side_samples or not neutral:
        return None
    neutral_mean = sum(sample.absolute_move for sample in neutral) / len(neutral)
    if neutral_mean <= 0:
        return None
    return (
        sum(sample.absolute_move for sample in side_samples) / len(side_samples)
    ) / neutral_mean


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
    "current_vol_calibration",
    "estimate_calibration",
    "estimate_vol_calibration",
]
