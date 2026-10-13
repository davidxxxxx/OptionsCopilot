from __future__ import annotations

import statistics
from types import SimpleNamespace
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from copilot.analysis.event_study import build_macro_views, compute_event_stats, surprise_rows
from copilot.config import EventSettings
from copilot.news.macro_calendar import MacroEvent
from copilot.runtime import Runtime
from copilot.scheduler import RunManager
from copilot.secrets import Secrets

NY = ZoneInfo("America/New_York")


def _trading_days(start: date, count: int) -> list[date]:
    days = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _events(days: list[date], *, category: str = "cpi", release_time: time = time(8, 30)) -> list[MacroEvent]:
    surprises = (-6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6)
    title = "Fed Interest Rate Decision" if category == "fomc_decision" else "Core Inflation Rate MoM"
    return [
        MacroEvent(
            source="tradingview",
            source_id=f"{category}-{index}",
            event_time=datetime.combine(day, release_time, NY),
            country="US",
            title=title,
            category=category,
            importance=3,
            actual=str(surprise),
            forecast="0",
        )
        for index, (day, surprise) in enumerate(zip(days, surprises, strict=True))
    ]


def _bars_with_known_reaction(
    days: list[date], events: list[MacroEvent], *, beta_gap: float, beta_after: float
) -> list[tuple[date, float, float, float, float]]:
    z_by_day = {
        row["event"].event_time.astimezone(NY).date(): float(row["z"])
        for row in surprise_rows(events)
    }
    bars = []
    close = 100.0
    for index, day in enumerate(days):
        previous = close
        if day in z_by_day and index >= 21:
            returns = [bars[position][4] / bars[position - 1][4] - 1 for position in range(index - 20, index)]
            vol = statistics.stdev(returns)
            z = z_by_day[day]
            open_price = previous * (1 + beta_gap * z * vol)
            close = open_price * (1 + beta_after * z * vol)
        else:
            close = previous * (1 + (0.01 if index % 2 else -0.01))
            open_price = previous
        bars.append((day, open_price, max(open_price, close), min(open_price, close), close))
    return bars


def test_event_study_recovers_known_slope_t_and_shrinkage() -> None:
    days = _trading_days(date(2024, 1, 2), 340)
    event_days = days[30:330:25]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.2, beta_after=0.6)

    stats = compute_event_stats("cpi", "SPY", events, bars, shrink_k=10)

    assert stats is not None
    assert stats["n"] == 12
    assert stats["beta_gap"] == pytest.approx(0.2, abs=0.02)
    assert stats["beta_after"] == pytest.approx(0.6, abs=0.03)
    assert stats["t_after"] > 10
    assert stats["beta_shrunk_after"] == pytest.approx(stats["beta_after"] * 12 / 22)
    assert stats["mean_trailing_vol"] > 0
    assert stats["abs_move_ratio"] > 0


def test_event_study_uses_next_day_for_in_session_release() -> None:
    days = _trading_days(date(2024, 1, 2), 340)
    event_days = days[30:330:25]
    events = _events(event_days, release_time=time(14, 0))
    bars = _bars_with_known_reaction(days, events, beta_gap=0.3, beta_after=0.0)
    by_day = {day: index for index, day in enumerate(days)}
    z_by_day = {
        row["event"].event_time.astimezone(NY).date(): float(row["z"])
        for row in surprise_rows(events)
    }
    mutable = list(bars)
    for day, z in z_by_day.items():
        index = by_day[day]
        returns = [mutable[position][4] / mutable[position - 1][4] - 1 for position in range(index - 20, index)]
        vol = statistics.stdev(returns)
        current = mutable[index]
        next_row = mutable[index + 1]
        next_close = current[4] * (1 + 0.5 * z * vol)
        mutable[index + 1] = (next_row[0], current[4], max(current[4], next_close), min(current[4], next_close), next_close)

    stats = compute_event_stats("cpi", "QQQ", events, mutable, shrink_k=0)

    assert stats is not None
    assert stats["after_label"] == "次日"
    assert stats["beta_after"] == pytest.approx(0.5, abs=0.05)


def test_fomc_reports_reaction_ratio_without_surprise_slope() -> None:
    days = _trading_days(date(2024, 1, 2), 340)
    events = _events(days[30:330:25], category="fomc_decision", release_time=time(14, 0))
    bars = _bars_with_known_reaction(days, events, beta_gap=0.2, beta_after=0.4)

    stats = compute_event_stats("fomc_decision", "TLT", events, bars, shrink_k=10)

    assert stats is not None
    assert stats["beta_gap"] is None
    assert stats["beta_after"] is None
    assert stats["t_after"] is None
    assert stats["abs_move_ratio"] is not None


def test_macro_view_filters_n_and_t_and_sets_origin_key() -> None:
    now = datetime(2026, 10, 13, 10, 0, tzinfo=NY)
    historical_days = _trading_days(date(2025, 1, 2), 12)
    events = _events(historical_days[:-1] + [now.date()])
    base = {
        "category": "cpi",
        "n": 12,
        "beta_shrunk_gap": 0.1,
        "beta_shrunk_after": 0.5,
        "t_after": 2.5,
        "after_label": "开盘后",
        "mean_trailing_vol": 0.01,
        "abs_move_ratio": 1.4,
    }
    stats = [
        {**base, "symbol": "SPY"},
        {**base, "symbol": "QQQ", "n": 11},
        {**base, "symbol": "TLT", "t_after": 1.9},
    ]

    views, payload = build_macro_views(events, stats, now=now, settings=EventSettings())

    assert set(views) == {("macro", "SPY")}
    assert views[("macro", "SPY")].source == "macro"
    assert payload is not None
    assert {row["symbol"]: row["generated"] for row in payload["rows"]} == {
        "SPY": True,
        "QQQ": False,
        "TLT": False,
    }
    spy = next(row for row in payload["rows"] if row["symbol"] == "SPY")
    assert spy["expected_after_pct"] == pytest.approx(spy["expected_after_sigma"] * 0.01)


def test_premarket_macro_view_expires_at_same_day_close() -> None:
    release_day = date(2026, 10, 13)
    days = _trading_days(date(2025, 1, 2), 12)
    events = _events(days[:-1] + [release_day])
    stats = [{
        "category": "cpi", "symbol": "SPY", "n": 12,
        "beta_shrunk_after": 0.5, "t_after": 3.0, "mean_trailing_vol": 0.01,
    }]

    active, payload = build_macro_views(
        events, stats, now=datetime(2026, 10, 13, 12, 0, tzinfo=NY), settings=EventSettings()
    )
    after_close, closed_payload = build_macro_views(
        events, stats, now=datetime(2026, 10, 13, 16, 30, tzinfo=NY), settings=EventSettings()
    )
    next_morning, next_payload = build_macro_views(
        events, stats, now=datetime(2026, 10, 14, 9, 0, tzinfo=NY), settings=EventSettings()
    )

    assert set(active) == {("macro", "SPY")} and payload is not None
    assert after_close == {} and closed_payload is None
    assert next_morning == {} and next_payload is None


def test_in_session_macro_view_expires_at_next_trading_day_close() -> None:
    friday = date(2026, 10, 9)
    monday = date(2026, 10, 12)
    days = _trading_days(date(2025, 1, 2), 12)
    events = _events(days[:-1] + [friday], release_time=time(14, 0))
    stats = [{
        "category": "cpi", "symbol": "SPY", "n": 12,
        "beta_shrunk_after": 0.5, "t_after": 3.0, "mean_trailing_vol": 0.01,
    }]

    friday_views, _ = build_macro_views(
        events, stats, now=datetime.combine(friday, time(14, 30), NY), settings=EventSettings()
    )
    monday_views, _ = build_macro_views(
        events, stats, now=datetime.combine(monday, time(12, 0), NY), settings=EventSettings()
    )
    expired, payload = build_macro_views(
        events, stats, now=datetime.combine(monday, time(16, 30), NY), settings=EventSettings()
    )

    assert set(friday_views) == {("macro", "SPY")}
    assert set(monday_views) == {("macro", "SPY")}
    assert expired == {} and payload is None


def test_event_study_refresh_refetches_recent_revised_actual(settings, store) -> None:
    today = datetime.now(NY).date()
    event_day = today - timedelta(days=7)

    class RevisingCalendar:
        revised = False

        def fetch(self, start: date, end: date) -> list[MacroEvent]:
            if not start <= event_day <= end:
                return []
            return [MacroEvent(
                source="tradingview",
                source_id="revised-cpi",
                event_time=datetime.combine(event_day, time(8, 30), NY),
                country="US",
                title="Core Inflation Rate MoM",
                category="cpi",
                importance=3,
                actual="0.4" if self.revised else None,
                forecast="0.2",
            )]

    source = RevisingCalendar()
    store.set_kv("event_study_macro_watermark", (today - timedelta(days=15)).isoformat())
    runtime = Runtime(
        replace(settings, events=replace(settings.events, study_symbols=())),
        Secrets({}),
        store,
        SimpleNamespace(news_sources=SimpleNamespace(tradingview=source)),
        RunManager(lambda trigger, slot, reason=None: 0),
        None,
    )
    start = datetime.combine(event_day, time.min, timezone.utc) - timedelta(days=1)
    end = start + timedelta(days=3)

    runtime._refresh_event_study_now()
    assert store.macro_events_between(start, end)[0].actual is None

    source.revised = True
    runtime._refresh_event_study_now()
    assert store.macro_events_between(start, end)[0].actual == "0.4"
