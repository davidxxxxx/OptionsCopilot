from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from copilot.analysis.event_study import (
    EVENT_STATS_VERSION,
    build_macro_views,
    compute_event_stats,
    walk_forward_evaluation,
)
from copilot.config import EventSettings
from copilot.market.nasdaq_history import merge_daily_bars, weekly_cache_date
from copilot.pipeline import _record_live_event_reactions
from copilot.runtime import (
    Runtime,
    backfill_event_reactions,
    finalize_event_reactions,
    sync_event_reactions,
)
from copilot.scheduler import RunManager
from copilot.secrets import Secrets

from .test_event_study import _bars_with_known_reaction, _events, _trading_days

NY = ZoneInfo("America/New_York")


def _ledger_row(event_date: date, prediction: dict | None) -> dict:
    return {
        "event_date": event_date.isoformat(),
        "category": "cpi",
        "symbol": "SPY",
        "event_time": datetime.combine(event_date, time(8, 30), NY).isoformat(),
        "title": "Core Inflation Rate MoM",
        "actual": 0.3,
        "forecast": 0.2,
        "surprise": 0.1,
        "z": 1.0,
        "prediction": prediction,
        "reaction": None,
        "vol": None,
        "status": "predicted",
    }


def test_event_reaction_first_prediction_wins(store) -> None:
    event_date = date(2026, 10, 13)
    first = {"source": "live", "expected_after_sigma": 0.4}
    store.upsert_event_reaction(_ledger_row(event_date, first))
    store.upsert_event_reaction(_ledger_row(
        event_date, {"source": "live", "expected_after_sigma": -9.0}
    ))

    row = store.event_reactions(category="cpi", symbol="SPY")[0]
    assert row["prediction"] == first
    assert row["z"] == 1.0


def test_live_release_creates_all_symbol_rows_when_prediction_is_null(store) -> None:
    now = datetime(2026, 10, 13, 9, 0, tzinfo=NY)
    events = _events([now.date()])

    views, payload = build_macro_views(events, [], now=now, settings=EventSettings())
    assert views == {} and payload is not None
    _record_live_event_reactions(store, payload, ("SPY", "QQQ"))

    rows = store.event_reactions(category="cpi", limit=10)
    assert {row["symbol"] for row in rows} == {"SPY", "QQQ"}
    assert all(row["prediction"] is None for row in rows)


def test_premarket_reaction_completes_with_same_day_bar(store) -> None:
    days = _trading_days(date(2026, 8, 3), 30)
    event = _events([days[-1]])
    bars = _bars_with_known_reaction(days, event, beta_gap=0.2, beta_after=0.6)
    store.upsert_event_reaction(_ledger_row(days[-1], {"source": "live"}))

    finalize_event_reactions(store, event, {"SPY": bars})

    row = store.event_reactions(symbol="SPY")[0]
    assert row["status"] == "complete"
    assert row["reaction"]["after_label"] == "开盘后"
    assert row["reaction"]["day"] is not None


def test_in_session_reaction_is_partial_then_complete_next_trading_day(store) -> None:
    days = _trading_days(date(2026, 8, 3), 31)
    event_day = days[-2]
    events = _events([event_day], release_time=time(14, 0))
    event_day_bars = _bars_with_known_reaction(
        days[:-1], events, beta_gap=0.2, beta_after=0.0
    )
    store.upsert_event_reaction({
        **_ledger_row(event_day, {"source": "live"}),
        "event_time": datetime.combine(event_day, time(14, 0), NY).isoformat(),
    })

    finalize_event_reactions(store, events, {"SPY": event_day_bars})
    partial = store.event_reactions(symbol="SPY")[0]
    assert partial["status"] == "partial"
    assert partial["reaction"]["after"] is None

    last_close = event_day_bars[-1][4]
    complete_bars = [
        *event_day_bars,
        (days[-1], last_close, last_close * 1.01, last_close * 0.99, last_close * 1.005),
    ]
    finalize_event_reactions(store, events, {"SPY": complete_bars})
    complete = store.event_reactions(symbol="SPY")[0]
    assert complete["status"] == "complete"
    assert complete["reaction"]["after"] is not None
    assert complete["reaction"]["after_label"] == "次日"


def test_missing_nasdaq_bar_defers_reaction_completion(store) -> None:
    days = _trading_days(date(2026, 8, 3), 30)
    event_date = days[-1]
    events = _events([event_date])
    store.upsert_event_reaction(_ledger_row(event_date, {"source": "live"}))

    finalize_event_reactions(store, events, {"SPY": _bars_with_known_reaction(
        days[:-1], events, beta_gap=0.2, beta_after=0.6
    )})

    assert store.event_reactions(symbol="SPY")[0]["status"] == "predicted"


def test_recent_bar_merge_replaces_overlap_and_adds_new_day() -> None:
    cached = [
        (date(2026, 10, 9), 100.0, 102.0, 99.0, 101.0),
        (date(2026, 10, 12), 101.0, 103.0, 100.0, 102.0),
    ]
    recent = [
        (date(2026, 10, 12), 101.0, 104.0, 100.0, 103.0),
        (date(2026, 10, 13), 103.0, 105.0, 102.0, 104.0),
    ]

    merged = merge_daily_bars(cached, recent)

    assert [row[0] for row in merged] == [date(2026, 10, 9), date(2026, 10, 12), date(2026, 10, 13)]
    assert merged[1][4] == 103.0


def test_refresh_fetches_recent_window_and_merges_weekly_cache(settings, store, monkeypatch) -> None:
    today = datetime.now(NY).date()
    cache_day = weekly_cache_date(today)
    old_day = today - timedelta(days=8)
    new_day = today - timedelta(days=1)
    store.put_daily_cache(
        "SPY", "NASDAQ_OHLC", cache_day, [[old_day.isoformat(), 100, 101, 99, 100]]
    )
    calls: list[date] = []

    def fake_daily_bars(symbol: str, *, today: date, start: date, **_kwargs):
        calls.append(start)
        return [(new_day, 100.0, 102.0, 99.0, 101.0)]

    monkeypatch.setattr("copilot.runtime.daily_bars", fake_daily_bars)
    runtime = Runtime(
        replace(settings, events=replace(settings.events, study_symbols=("SPY",))),
        Secrets({}),
        store,
        SimpleNamespace(news_sources=None),
        RunManager(lambda trigger, slot, reason=None: 0),
        None,
    )

    runtime._refresh_event_study_now()

    cached = store.get_daily_cache("SPY", "NASDAQ_OHLC", cache_day)
    assert calls == [today - timedelta(days=15)]
    assert [row[0] for row in cached] == [old_day.isoformat(), new_day.isoformat()]


def test_weighted_ols_half_life_zero_equals_unweighted() -> None:
    days = _trading_days(date(2023, 1, 3), 500)
    event_days = days[30:480:20]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.2, beta_after=0.6)

    stats = compute_event_stats(
        "cpi", "SPY", events, bars,
        shrink_k=0, min_half_t=1, recency_half_life=0,
    )

    assert stats is not None
    assert stats["beta_after"] == pytest.approx(stats["beta_after_unweighted"])
    assert stats["se_after"] == pytest.approx(stats["se_after_unweighted"])
    assert stats["t_after"] == pytest.approx(stats["t_after_unweighted"])


def test_recency_weighted_beta_follows_recent_regime_sign() -> None:
    days = _trading_days(date(2022, 1, 3), 700)
    event_days = days[30:670:20]
    events = _events(event_days)
    split = len(event_days) - 8
    regimes = {
        day: (1.0 if index < split else -0.4)
        for index, day in enumerate(event_days)
    }
    bars = _bars_with_known_reaction(
        days, events, beta_gap=0.1, beta_after=0.0, beta_after_by_day=regimes
    )

    stats = compute_event_stats(
        "cpi", "SPY", events, bars,
        shrink_k=0, min_half_t=0, recency_half_life=2,
    )

    assert stats is not None
    assert stats["beta_after_unweighted"] > 0
    assert stats["beta_after"] < 0


def test_walk_forward_prediction_has_no_look_ahead_under_future_mutation() -> None:
    days = _trading_days(date(2021, 1, 4), 900)
    event_days = days[30:850:20]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.1, beta_after=0.7)
    arguments = {
        "shrink_k": 5,
        "min_events": 8,
        "min_t": 1,
        "min_half_t": 0.5,
        "recency_half_life": 12,
    }
    baseline = walk_forward_evaluation("cpi", events, bars, **arguments)
    target_index = 20
    target_date = event_days[target_index]
    baseline_prediction = baseline["predictions"][target_index]["prediction"]
    assert baseline_prediction is not None

    mutated_events = [
        replace(event, actual=str(10_000 + index)) if index > target_index else event
        for index, event in enumerate(events)
    ]
    mutated_bars = list(bars)
    bar_index = next(index for index, row in enumerate(mutated_bars) if row[0] == target_date)
    day, open_price, high, low, close = mutated_bars[bar_index]
    mutated_bars[bar_index] = (day, open_price * 1.5, high * 1.5, low * 0.5, close * 0.5)

    mutated = walk_forward_evaluation("cpi", mutated_events, mutated_bars, **arguments)

    assert mutated["predictions"][target_index]["prediction"] == baseline_prediction


def test_walk_forward_metrics_recover_known_directional_relationship() -> None:
    days = _trading_days(date(2021, 1, 4), 900)
    event_days = days[30:850:20]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.1, beta_after=0.8)

    result = walk_forward_evaluation(
        "cpi", events, bars,
        shrink_k=5, min_events=8, min_t=1, min_half_t=0.5, recency_half_life=12,
    )
    metrics = result["metrics"]

    assert metrics["wf_n"] >= 25
    assert metrics["wf_hit_rate"] == pytest.approx(1.0)
    assert metrics["wf_corr"] > 0.9
    assert metrics["wf_views"] >= 8
    assert metrics["wf_view_hit_rate"] == pytest.approx(1.0)
    assert metrics["wf_view_mean"] > 0


def test_good_in_sample_t_but_poor_walk_forward_blocks_live_view() -> None:
    now = datetime(2026, 10, 13, 12, 0, tzinfo=NY)
    days = _trading_days(date(2025, 1, 2), 12)
    events = _events(days[:-1] + [now.date()])
    stats = [{
        "stats_version": EVENT_STATS_VERSION,
        "category": "cpi",
        "symbol": "SPY",
        "n": 30,
        "beta_shrunk_after": 0.8,
        "t_after": 6.0,
        "stable_after": True,
        "mean_trailing_vol": 0.01,
        "wf_views": 20,
        "wf_view_hit_rate": 0.40,
        "wf_view_mean": 0.20,
    }]

    views, payload = build_macro_views(events, stats, now=now, settings=replace(
        EventSettings(),
        min_wf_views=8,
        min_wf_hit_rate=0.55,
    ))

    assert views == {}
    assert payload is not None and payload["rows"][0]["wf_pass"] is False


def test_backfill_uses_walk_forward_prediction_source(settings, store) -> None:
    days = _trading_days(date(2022, 1, 3), 600)
    event_days = days[30:560:20]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.1, beta_after=0.8)
    settings = replace(settings, events=replace(
        settings.events,
        study_symbols=("SPY",),
        min_events=8,
        min_t=1,
        min_half_t=0.5,
    ))

    backfill_event_reactions(store, events, {"SPY": bars}, settings)

    rows = store.event_reactions(category="cpi", symbol="SPY", limit=100)
    assert len(rows) == len(events)
    assert any(
        row["prediction"] and row["prediction"]["source"] == "walk_forward"
        for row in rows
    )


def test_daily_sync_backfills_missed_release_with_walk_forward_prediction(settings, store) -> None:
    days = _trading_days(date(2022, 1, 3), 600)
    event_days = days[30:560:20]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.1, beta_after=0.8)
    settings = replace(settings, events=replace(
        settings.events,
        study_symbols=("SPY",),
        min_events=8,
        min_t=1,
        min_half_t=0.5,
    ))
    store.upsert_event_reaction({
        **_ledger_row(event_days[12], {"source": "live", "expected_after_sigma": 0.4}),
        "reaction": {"after": 0.01},
        "status": "complete",
    })

    sync_event_reactions(store, events, {"SPY": bars}, settings)

    missed = next(
        row for row in store.event_reactions(category="cpi", symbol="SPY", limit=100)
        if row["event_date"] == event_days[20].isoformat()
    )
    assert missed["prediction"]["source"] == "walk_forward"


def test_daily_sync_preserves_existing_live_prediction(settings, store) -> None:
    days = _trading_days(date(2022, 1, 3), 600)
    event_days = days[30:560:20]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.1, beta_after=0.8)
    settings = replace(settings, events=replace(
        settings.events,
        study_symbols=("SPY",),
        min_events=8,
        min_t=1,
        min_half_t=0.5,
    ))
    store.upsert_event_reaction({
        **_ledger_row(event_days[12], {"source": "live", "expected_after_sigma": 0.4}),
        "reaction": {"after": 0.01},
        "status": "complete",
    })
    before = next(
        row for row in store.event_reactions(category="cpi", symbol="SPY", limit=100)
        if row["event_date"] == event_days[12].isoformat()
    )

    sync_event_reactions(store, events, {"SPY": bars}, settings)

    after = next(
        row for row in store.event_reactions(category="cpi", symbol="SPY", limit=100)
        if row["event_date"] == event_days[12].isoformat()
    )
    assert after == before


def test_daily_sync_is_idempotent_without_duplicate_rows(settings, store) -> None:
    days = _trading_days(date(2022, 1, 3), 600)
    event_days = days[30:560:20]
    events = _events(event_days)
    bars = _bars_with_known_reaction(days, events, beta_gap=0.1, beta_after=0.8)
    settings = replace(settings, events=replace(
        settings.events,
        study_symbols=("SPY",),
        min_events=8,
        min_t=1,
        min_half_t=0.5,
    ))
    store.upsert_event_reaction({
        **_ledger_row(event_days[12], {"source": "live", "expected_after_sigma": 0.4}),
        "reaction": {"after": 0.01},
        "status": "complete",
    })

    sync_event_reactions(store, events, {"SPY": bars}, settings)
    first_rows = store.event_reactions(category="cpi", symbol="SPY", limit=100)
    sync_event_reactions(store, events, {"SPY": bars}, settings)
    second_rows = store.event_reactions(category="cpi", symbol="SPY", limit=100)
    first_keys = {
        (row["event_date"], row["category"], row["symbol"]) for row in first_rows
    }
    second_keys = {
        (row["event_date"], row["category"], row["symbol"]) for row in second_rows
    }

    assert len(first_rows) == len(second_rows) == len(second_keys)
    assert second_keys == first_keys


def test_replace_event_stats_removes_stale_pairs(store) -> None:
    store.replace_event_stats([
        {"category": "cpi", "symbol": "SPY", "stats_version": EVENT_STATS_VERSION},
        {"category": "ppi", "symbol": "QQQ", "stats_version": EVENT_STATS_VERSION},
    ])

    store.replace_event_stats([
        {"category": "cpi", "symbol": "SPY", "stats_version": EVENT_STATS_VERSION},
    ])

    assert [(row["category"], row["symbol"]) for row in store.event_stats()] == [("cpi", "SPY")]
