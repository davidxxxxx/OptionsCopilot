from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from copilot.analysis.models import View
from copilot.analysis.news_calibration import (
    CalibrationSample,
    build_samples,
    calibrated_view,
    current_calibration,
    estimate_calibration,
)
from copilot.pipeline import PipelineDeps, run_pipeline
from copilot.runtime import Runtime
from copilot.scheduler import RunManager
from copilot.secrets import Secrets
from copilot.web.app import create_app

from .conftest import FakeLlm, FakeMarket
from .test_pipeline_web import NOW, RULES, SPOTS, seed_news


def samples(k: float, count: int, *, same_day: bool = False) -> list[CalibrationSample]:
    start = date(2026, 1, 2)
    return [
        CalibrationSample(
            day=start if same_day else start + timedelta(days=index),
            symbol=f"S{index}",
            conviction=2 + index % 4,
            x=1.0,
            y=k,
        )
        for index in range(count)
    ]


def ledger_view(symbol: str = "SPY", **overrides) -> dict:
    payload = View(
        symbol=symbol,
        direction="bullish",
        conviction=4,
        expected_move_pct=0.03,
        horizon_days=7,
        drift_sigma=0.4,
    ).to_dict()
    payload.update(overrides)
    return payload


def pipeline_deps(settings, store, model) -> PipelineDeps:
    market = FakeMarket(SPOTS)
    return PipelineDeps(
        settings=settings,
        store=store,
        news_sources=None,
        llm=model,
        market_factory=lambda: market,
        clock=lambda: NOW,
    )


def test_estimator_recovers_one_and_clamps_at_one(settings) -> None:
    result = estimate_calibration(samples(1.0, 400), settings.news_calibration)
    assert result["k"] == pytest.approx(1.0, abs=0.01)
    amplified = estimate_calibration(samples(2.0, 400), settings.news_calibration)
    assert amplified["k"] == 1.0 and amplified["k_raw"] > 1.0


def test_estimator_zero_no_data_negative_and_design_effect(settings) -> None:
    zero = estimate_calibration(samples(0.0, 400), settings.news_calibration)
    assert zero["k"] < 0.01
    empty = estimate_calibration([], settings.news_calibration)
    assert empty["k"] == settings.news_calibration.k0
    negative = estimate_calibration(samples(-1.0, 100), settings.news_calibration)
    assert negative["k"] == 0.0 and negative["k_raw"] < 0.0
    independent = estimate_calibration(samples(1.0, 20), settings.news_calibration)
    clustered = estimate_calibration(samples(1.0, 20, same_day=True), settings.news_calibration)
    assert clustered["sd"] > independent["sd"]


def test_build_samples_filters_sources_counts_skips_and_skips_holiday() -> None:
    day = date(2026, 7, 2)
    candidates = [
        {"trading_date": day.isoformat(), "symbol": "SPY", "view": ledger_view(), "underlying": {"iv30": 0.2}},
        {"trading_date": day.isoformat(), "symbol": "QQQ", "view": ledger_view("QQQ", direction="neutral"), "underlying": {"iv30": 0.2}},
        {"trading_date": day.isoformat(), "symbol": "IWM", "view": ledger_view("IWM", source="ladder"), "underlying": {"iv30": 0.2}},
        {"trading_date": day.isoformat(), "symbol": "TLT", "view": ledger_view("TLT", source="macro"), "underlying": {"iv30": 0.2}},
        {"trading_date": day.isoformat(), "symbol": "DIA", "view": ledger_view("DIA"), "underlying": {}, "market_state": {"iv_30d": 0.22}},
        {"trading_date": day.isoformat(), "symbol": "XLK", "view": ledger_view("XLK"), "underlying": {}},
        {"trading_date": day.isoformat(), "symbol": "XLF", "view": ledger_view("XLF"), "underlying": {"iv30": 0.2}},
    ]
    closes = {
        "SPY": [(day, 100.0), (date(2026, 7, 6), 101.0)],
        "DIA": [(day, 100.0), (date(2026, 7, 6), 99.0)],
        "XLF": [(day, 100.0)],
    }
    built = build_samples(candidates, closes)
    assert len(built.samples) == 2
    assert built.samples[0].day == day
    assert {sample.volatility_source for sample in built.samples} == {"iv30", "market_state"}
    assert built.candidates == 4
    assert built.skipped_missing_iv == 1
    assert built.skipped_missing_close == 1


def test_build_samples_uses_hv20_when_iv_is_missing() -> None:
    day = date(2026, 8, 31)
    history = [
        (day - timedelta(days=20 - index), 100.0 + index + (0.8 if index % 2 else -0.6))
        for index in range(21)
    ]
    history.append((date(2026, 9, 1), 122.0))
    built = build_samples(
        [{"trading_date": day.isoformat(), "symbol": "SPY", "view": ledger_view(), "underlying": {}}],
        {"SPY": history},
    )
    assert len(built.samples) == 1
    assert built.samples[0].volatility_source == "hv20"
    assert built.skipped_missing_iv == 0


def test_build_samples_skips_hv20_with_fewer_than_21_closes() -> None:
    day = date(2026, 8, 31)
    history = [
        (day - timedelta(days=19 - index), 100.0 + index)
        for index in range(20)
    ]
    history.append((date(2026, 9, 1), 121.0))
    built = build_samples(
        [{"trading_date": day.isoformat(), "symbol": "SPY", "view": ledger_view(), "underlying": {}}],
        {"SPY": history},
    )
    assert built.samples == ()
    assert built.skipped_missing_iv == 1


def test_estimator_reports_volatility_source_counts(settings) -> None:
    rows = [
        CalibrationSample(date(2026, 1, 2), "A", 3, 1.0, 0.5, "iv30"),
        CalibrationSample(date(2026, 1, 3), "B", 3, 1.0, 0.5, "market_state"),
        CalibrationSample(date(2026, 1, 4), "C", 3, 1.0, 0.5, "hv20"),
        CalibrationSample(date(2026, 1, 5), "D", 3, 1.0, 0.5, "hv20"),
    ]
    result = estimate_calibration(rows, settings.news_calibration)
    assert result["volatility_sources"] == {"iv30": 1, "market_state": 1, "hv20": 2}
    assert result["by_conviction"]["3"]["volatility_sources"] == result["volatility_sources"]


def test_store_selects_last_finished_run_before_1600_et(store) -> None:
    day = date(2026, 10, 8)

    def add_run(hour_et: int, drift: float, status: str = "ok") -> int:
        run_id = store.create_run(trading_date=day, slot="manual", trigger="test")
        store.save_views(run_id, [ledger_view(drift_sigma=drift)])
        store.save_snapshot(run_id, "underlying", "SPY", {"iv30": 0.2})
        store.finish_run(run_id, status=status, stages={})
        started = datetime(2026, 10, 8, hour_et + 4, tzinfo=timezone.utc).isoformat()
        with store._tx() as db:
            db.execute("UPDATE runs SET started_at=?, finished_at=? WHERE id=?", (started, started, run_id))
        return run_id

    add_run(10, 0.1)
    selected = add_run(15, 0.2)
    add_run(16, 0.3)
    add_run(14, 0.4, status="failed")
    rows = store.news_calibration_candidates()
    assert len(rows) == 1
    assert rows[0]["run_id"] == selected
    assert rows[0]["view"]["drift_sigma"] == 0.2
    assert rows[0]["underlying"]["iv30"] == 0.2


def test_calibrated_view_changes_only_news_pricing_copy() -> None:
    news = View.from_dict(ledger_view())
    ladder = replace(news, source="ladder")
    macro = replace(news, source="macro")
    priced = calibrated_view(news, 0.25)
    assert priced.drift_sigma == pytest.approx(0.1)
    assert news.drift_sigma == 0.4
    assert calibrated_view(ladder, 0.25) is ladder
    assert calibrated_view(macro, 0.25) is macro


def test_pipeline_scales_news_copy_records_stage_and_keeps_stored_view(settings, store) -> None:
    store.set_kv("news_calibration", {
        "k": 0.5, "k_raw": 0.5, "sd": 0.3, "samples": 12, "days": 8,
        "hit_rate": 0.4, "mean_x": 0.2, "mean_y": -0.1, "by_conviction": {},
        "prior": {}, "computed_at": NOW.isoformat(),
    })
    seed_news(store)
    run = store.get_run(run_pipeline(
        pipeline_deps(settings, store, FakeLlm(RULES)), trigger="test", slot="manual"
    ))
    stored = next(view for view in run["views"] if view["symbol"] == "NVDA")
    idea = next(idea for idea in run["ideas"] if idea["symbol"] == "NVDA")
    assert stored["drift_sigma"] == pytest.approx(0.4)
    assert idea["view"]["drift_sigma"] == pytest.approx(0.2)
    assert run["stages"]["calibration"] == {
        "k": 0.5, "sd": 0.3, "samples": 12, "days": 8, "verified": False,
    }
    assert idea["calibration_k"] == 0.5
    assert idea["calibration_verified"] is False
    assert idea["news_unverified"] is True


def test_pipeline_k_zero_allows_successful_run_with_no_news_ideas(settings, store) -> None:
    store.set_kv("news_calibration", {
        "k": 0.0, "k_raw": -0.2, "sd": 0.1, "samples": 100, "days": 30,
        "hit_rate": 0.2, "mean_x": 0.2, "mean_y": -0.3, "by_conviction": {},
        "prior": {}, "computed_at": NOW.isoformat(),
    })
    seed_news(store)
    run = store.get_run(run_pipeline(
        pipeline_deps(settings, store, FakeLlm(RULES)), trigger="test", slot="manual"
    ))
    assert run["status"] == "ok"
    assert [idea for idea in run["ideas"] if idea.get("origin") == "news"] == []


def test_news_calibration_api_and_static_ui(settings, store) -> None:
    store.set_kv("news_calibration", {
        "k": 0.4, "k_raw": 0.4, "sd": 0.25, "samples": 10, "days": 5,
        "hit_rate": 0.3, "mean_x": 0.1, "mean_y": -0.2,
        "volatility_sources": {"iv30": 4, "market_state": 3, "hv20": 3},
        "by_conviction": {"2": {"samples": 2, "days": 2, "k": 0.3, "sd": 0.4, "hit_rate": 0.5}},
        "prior": {}, "computed_at": NOW.isoformat(),
    })
    deps = PipelineDeps(settings, store, None, None, None)
    runtime = Runtime(settings, Secrets({}), store, deps, RunManager(lambda trigger, slot: 0), None)
    payload = TestClient(create_app(runtime)).get("/api/news-calibration").json()
    assert payload["k"] == 0.4 and payload["verified"] is False
    assert payload["volatility_sources"]["hv20"] == 3
    assert payload["min_days_verified"] == settings.news_calibration.min_days_verified
    root = Path(__file__).parents[2]
    script = (root / "copilot/web/static/app.js").read_text(encoding="utf-8")
    html = (root / "copilot/web/static/index.html").read_text(encoding="utf-8")
    assert "新闻观点未经验证：校准系数" in script
    assert "波动率来源" in script
    assert "新闻观点校准" in html


def test_current_calibration_disabled_is_verified(settings) -> None:
    disabled = replace(settings.news_calibration, enabled=False)
    payload = current_calibration(None, disabled)
    assert payload["k"] == 1.0 and payload["verified"] is True
