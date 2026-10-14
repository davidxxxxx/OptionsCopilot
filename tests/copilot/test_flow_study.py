from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from copilot.analysis.flow_study import compute_flow_stats

NEW_YORK = ZoneInfo("America/New_York")


def _bar(day: date, at: time, open_: float, high: float, low: float, close: float) -> list[float]:
    stamp = int(datetime.combine(day, at, NEW_YORK).timestamp())
    return [stamp, open_, high, low, close, 1_000]


def test_negative_gamma_momentum_is_detected_only_in_negative_regime() -> None:
    start = date(2026, 1, 5)
    states = {"SPY": []}
    intraday = {}
    daily = {"SPY": []}
    previous = 100.0
    for index in range(21):
        day = start + timedelta(days=index)
        sign = -1 if index <= 10 else 1
        close_1530 = previous * (1.01 if index % 2 else 0.99)
        if sign < 0:
            close_1600 = close_1530 * (1.005 if close_1530 > previous else 0.995)
        else:
            close_1600 = close_1530 * (0.995 if close_1530 > previous else 1.005)
        daily["SPY"].append([day, previous, max(previous, close_1600), min(previous, close_1600), close_1600])
        if index:
            states["SPY"].append({"as_of": day.isoformat(), "gex_sign": sign, "iv_30d": 0.20})
            intraday[("SPY", day)] = [
                _bar(day, time(15, 30), previous, close_1530, close_1530, close_1530),
                _bar(day, time(15, 59), close_1530, close_1600, close_1600, close_1600),
            ]
        previous = close_1600
    stats = compute_flow_stats(states, intraday, daily)
    assert stats["gex"]["regimes"]["negative"]["agreement_rate"] == 1.0
    assert stats["gex"]["regimes"]["positive"]["agreement_rate"] == 0.0
    assert stats["gex"]["verdict"] == "负伽马动量更强"


def test_pinned_expiry_series_has_smaller_distance() -> None:
    start = date(2026, 2, 2)
    states = {"SPY": []}
    daily = {"SPY": []}
    for index in range(20):
        day = start + timedelta(days=index)
        expiry = index < 10
        close = 100.05 if expiry else 103.0
        daily["SPY"].append([day, close, close, close, close])
        feature = {"strike": 100.0}
        states["SPY"].append({
            "as_of": day.isoformat(),
            "iv_30d": 0.20,
            "is_expiry_day": expiry,
            "max_oi_strike": feature,
            "max_pain": feature,
            "nearest_listed_strike": feature,
        })
    stats = compute_flow_stats(states, {}, daily)
    assert stats["pinning"]["expiry"]["mean_distance_sigma"] < stats["pinning"]["non_expiry"]["mean_distance_sigma"]
    assert stats["pinning"]["verdict"] == "到期日更接近关键行权价"


def test_flow_studies_report_insufficient_sample_below_twenty_days() -> None:
    day = date(2026, 3, 2)
    feature = {"strike": 100.0}
    states = {"SPY": [{
        "as_of": day.isoformat(), "gex_sign": -1, "iv_30d": 0.20,
        "spot": 100.0, "is_expiry_day": True,
        "call_wall": {"strike": 102.0}, "put_wall": {"strike": 98.0},
        "max_oi_strike": feature, "max_pain": feature, "nearest_listed_strike": feature,
    }]}
    daily = {"SPY": [
        [day - timedelta(days=1), 100.0, 101.0, 99.0, 100.0],
        [day, 100.0, 101.0, 99.0, 100.0],
        [day + timedelta(days=1), 100.0, 101.0, 99.0, 100.0],
    ]}
    intraday = {("SPY", day): [
        _bar(day, time(15, 30), 100.0, 100.5, 99.5, 100.2),
        _bar(day, time(15, 59), 100.2, 100.6, 100.0, 100.4),
    ]}
    stats = compute_flow_stats(states, intraday, daily)
    assert stats["gex"]["verdict"] == "样本不足"
    assert stats["pinning"]["verdict"] == "样本不足"
    assert stats["walls"]["verdict"] == "样本不足"
