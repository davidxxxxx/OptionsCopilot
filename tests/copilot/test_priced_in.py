from __future__ import annotations

import math
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from copilot.analysis.models import View
from copilot.analysis.priced_in import apply_priced_in, price_at_or_before


NOW = datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc)


def _view(*, volatility: str = "neutral", multiplier: float | None = None) -> View:
    return View(
        symbol="SPY",
        direction="bullish",
        conviction=4,
        expected_move_pct=0.0,
        horizon_days=14,
        volatility=volatility,
        volatility_multiplier=multiplier,
        drift_sigma=0.5,
    )


def _contributor() -> dict[str, object]:
    return {
        "news_id": 7,
        "value": 2.0,
        "published_at": (NOW - timedelta(days=1)).isoformat(),
    }


def test_price_at_or_before_uses_latest_available_minute() -> None:
    published = NOW - timedelta(minutes=1)
    bars = [
        [int((published - timedelta(minutes=2)).timestamp()), 99, 99, 99, 99, None],
        [int(published.timestamp()), 101, 101, 101, 101, None],
        [int((published + timedelta(minutes=1)).timestamp()), 103, 103, 103, 103, None],
    ]
    assert price_at_or_before(bars, published) == 101


def test_full_expected_move_leaves_nearly_zero_drift() -> None:
    current_iv = 0.20
    publication_price = 100.0
    current_spot = publication_price * math.exp(current_iv * math.sqrt(14 / 365))
    adjusted, diagnostic = apply_priced_in(
        _view(),
        [_contributor()],
        current_spot=current_spot,
        current_iv=current_iv,
        now=NOW,
        publication_prices={7: (publication_price, "nasdaq_live")},
        raw_drift_sigma=1.0,
    )
    assert diagnostic["moved_sigma"] == pytest.approx(1.0, abs=1e-4)
    assert diagnostic["priced_in"] == pytest.approx(1.0)
    assert adjusted.drift_sigma == pytest.approx(0.0)


def test_recent_move_is_compared_with_expected_horizon_return() -> None:
    current_iv = 0.25
    raw_drift_sigma = 0.4
    horizon_days = 8
    view = replace(_view(), horizon_days=horizon_days)
    contributor = {
        "news_id": 7,
        "value": 2.0,
        "published_at": (NOW - timedelta(hours=1)).isoformat(),
    }
    adjusted, diagnostic = apply_priced_in(
        view,
        [contributor],
        current_spot=101.0,
        current_iv=current_iv,
        now=NOW,
        publication_prices={7: (100.0, "nasdaq_live")},
        raw_drift_sigma=raw_drift_sigma,
    )
    expected_move = raw_drift_sigma * current_iv * math.sqrt(horizon_days / 365)
    expected_priced_in = math.log(1.01) / expected_move
    assert diagnostic["moved_sigma"] == pytest.approx(
        math.log(1.01) / (current_iv * math.sqrt(horizon_days / 365)),
        abs=1e-4,
    )
    assert diagnostic["priced_in"] == pytest.approx(expected_priced_in, abs=1e-4)
    assert diagnostic["priced_in"] == pytest.approx(0.672, abs=0.002)
    assert adjusted.drift_sigma == pytest.approx(
        view.drift_sigma * (1 - expected_priced_in),
        abs=1e-4,
    )


def test_no_move_leaves_drift_unchanged() -> None:
    view = _view()
    adjusted, diagnostic = apply_priced_in(
        view,
        [_contributor()],
        current_spot=100.0,
        current_iv=0.20,
        now=NOW,
        publication_prices={7: (100.0, "nasdaq_archive")},
        raw_drift_sigma=1.0,
    )
    assert diagnostic["priced_in"] == 0.0
    assert adjusted.drift_sigma == view.drift_sigma


def test_opposite_move_is_clamped_at_zero_priced_in() -> None:
    view = _view()
    adjusted, diagnostic = apply_priced_in(
        view,
        [_contributor()],
        current_spot=95.0,
        current_iv=0.20,
        now=NOW,
        publication_prices={7: (100.0, "run_snapshot")},
        raw_drift_sigma=1.0,
    )
    assert diagnostic["moved_sigma"] < 0
    assert diagnostic["priced_in"] == 0.0
    assert adjusted.drift_sigma == view.drift_sigma


def test_missing_publication_price_is_unknown_and_unchanged() -> None:
    view = _view()
    adjusted, diagnostic = apply_priced_in(
        view,
        [_contributor()],
        current_spot=105.0,
        current_iv=0.20,
        now=NOW,
        publication_prices={7: None},
        raw_drift_sigma=1.0,
    )
    assert diagnostic["status"] == "unknown"
    assert adjusted == view


def test_iv_expansion_already_realised_neutralises_expand_view() -> None:
    adjusted, diagnostic = apply_priced_in(
        _view(volatility="expand", multiplier=1.20),
        [_contributor()],
        current_spot=100.0,
        current_iv=0.25,
        now=NOW,
        publication_prices={7: (100.0, "previous_close")},
        publication_ivs={7: 0.20},
        raw_drift_sigma=1.0,
    )
    assert diagnostic["iv_change"] == pytest.approx(0.05)
    assert diagnostic["volatility_neutralized"] is True
    assert adjusted.volatility == "neutral"
    assert adjusted.volatility_multiplier is None
