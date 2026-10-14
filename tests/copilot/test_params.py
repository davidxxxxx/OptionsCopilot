from __future__ import annotations

from dataclasses import replace

import pytest

from copilot.config import load_settings
from copilot.params import SCORING_REVISION, params_payload, params_version


def test_params_version_is_stable_and_tracks_only_scoring_settings(settings) -> None:
    assert SCORING_REVISION == 7
    version = params_version(settings)
    assert params_payload(settings)["calendar"] == {
        "catalyst_symbols": list(settings.calendar.catalyst_symbols),
    }
    assert params_payload(settings)["events"] == {
        "study_symbols": list(settings.events.study_symbols),
        "min_events": settings.events.min_events,
        "min_t": settings.events.min_t,
        "min_half_t": settings.events.min_half_t,
        "shrink_k": settings.events.shrink_k,
        "recency_half_life": settings.events.recency_half_life,
        "min_wf_views": settings.events.min_wf_views,
        "min_wf_hit_rate": settings.events.min_wf_hit_rate,
    }
    assert params_payload(settings)["news_calibration"] == {
        "enabled": True,
        "k0": 0.5,
        "tau": 0.5,
        "rho": 0.3,
        "min_days_verified": 20,
        "max_sd_verified": 0.2,
        "min_vol_days": 10,
    }
    assert params_payload(settings)["vrp"] == {
        "enabled": True,
        "symbols": ("SPY", "QQQ"),
        "dte_min": 30,
        "dte_max": 45,
        "short_delta": 0.2,
        "min_iv_minus_hv20": 0.02,
        "event_blackout_days": 3,
        "max_ideas": 2,
        "take_profit_pct": 0.5,
        "stop_credit_multiple": 2.0,
        "time_exit_dte": 21,
    }
    assert params_version(settings) == version
    assert params_version(replace(
        settings,
        strategy=replace(settings.strategy, keep_margin=settings.strategy.keep_margin + 0.01),
    )) != version
    assert params_version(replace(
        settings,
        rules=replace(
            settings.rules,
            portfolio_risk_cap_pct=settings.rules.portfolio_risk_cap_pct + 0.01,
        ),
    )) != version
    assert params_version(replace(
        settings,
        web=replace(settings.web, port=settings.web.port + 1),
    )) == version
    assert params_version(replace(
        settings,
        ibkr=replace(settings.ibkr, port=settings.ibkr.port + 1),
    )) == version
    assert params_version(replace(
        settings,
        calendar=replace(settings.calendar, catalyst_symbols=("SPY",)),
    )) != version
    assert params_version(replace(
        settings,
        calendar=replace(
            settings.calendar,
            days_ahead=settings.calendar.days_ahead + 1,
            refresh_hours=settings.calendar.refresh_hours + 1,
            banner_days=settings.calendar.banner_days + 1,
            alert_days=settings.calendar.alert_days + 1,
        ),
    )) == version
    assert params_version(replace(
        settings,
        events=replace(settings.events, cooldown_minutes=settings.events.cooldown_minutes + 1),
    )) == version
    assert params_version(replace(
        settings,
        events=replace(settings.events, min_t=settings.events.min_t + 0.5),
    )) != version
    assert params_version(replace(
        settings,
        events=replace(settings.events, min_half_t=settings.events.min_half_t + 0.5),
    )) != version
    assert params_version(replace(
        settings,
        events=replace(
            settings.events,
            recency_half_life=settings.events.recency_half_life + 1,
        ),
    )) != version
    assert params_version(replace(
        settings,
        events=replace(settings.events, min_wf_views=settings.events.min_wf_views + 1),
    )) != version
    assert params_version(replace(
        settings,
        events=replace(
            settings.events,
            min_wf_hit_rate=settings.events.min_wf_hit_rate + 0.01,
        ),
    )) != version
    assert params_version(replace(
        settings,
        news_calibration=replace(settings.news_calibration, k0=0.6),
    )) != version
    assert params_version(replace(
        settings,
        vrp=replace(settings.vrp, min_iv_minus_hv20=0.03),
    )) != version


def test_event_settings_load_symbols_and_validate_active_window(tmp_path) -> None:
    loaded = load_settings(
        local_path=tmp_path / "none.toml",
        overrides={"events": {"study_symbols": ["spy", "qqq"], "min_events": 8}},
    )
    assert loaded.events.study_symbols == ("SPY", "QQQ")
    assert loaded.events.min_events == 8
    with pytest.raises(ValueError, match="events active window"):
        load_settings(
            local_path=tmp_path / "none.toml",
            overrides={"events": {"active_start_et": "20:00", "active_end_et": "07:00"}},
        )
