"""Released earnings must not remain active pricing or risk catalysts."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from copilot.analysis.ledger import Evidence, update_ledger
from copilot.analysis.models import View
from copilot.config import RulesSettings, StrategySettings, ViewsSettings
from copilot.market.models import Position
from copilot.news.models import EarningsEvent, is_upcoming, release_time
from copilot.pipeline import _pricing_view, _upcoming_earnings
from copilot.strategies.builder import build_candidates
from copilot.strategies.portfolio import PositionGroup, exit_alerts

from .conftest import make_chain

NEW_YORK = ZoneInfo("America/New_York")
TODAY = date(2026, 10, 14)


def _ny(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 14, hour, minute, tzinfo=NEW_YORK)


def test_earnings_release_time_boundaries_by_hour() -> None:
    bmo = EarningsEvent("BAC", TODAY, "bmo")
    amc = EarningsEvent("MSFT", TODAY, "amc")
    unknown = EarningsEvent("NVDA", TODAY, "")
    dmh = EarningsEvent("JPM", TODAY, "dmh")

    assert release_time(bmo) == _ny(9, 30)
    assert is_upcoming(bmo, _ny(8))
    assert not is_upcoming(bmo, _ny(9, 30))
    assert is_upcoming(amc, _ny(15, 59))
    assert not is_upcoming(amc, _ny(16))
    assert is_upcoming(unknown, _ny(15, 59))
    assert not is_upcoming(unknown, _ny(16))
    assert is_upcoming(dmh, _ny(11, 59))
    assert not is_upcoming(dmh, _ny(12))


def test_bmo_release_controls_vol_gate_short_premium_and_position_alert() -> None:
    event = EarningsEvent("BAC", TODAY, "bmo")
    before_dates = _upcoming_earnings([event], _ny(8))["BAC"]
    after_dates = _upcoming_earnings([event], _ny(13, 42)).get("BAC", ())
    expiry = TODAY + timedelta(days=21)
    chain = make_chain(
        symbol="BAC", spot=600, iv=0.25, expiries=(expiry,), today=TODAY
    )
    expanding = View(
        symbol="BAC",
        direction="bearish",
        conviction=3,
        expected_move_pct=0.05,
        horizon_days=14,
        volatility="expand",
        catalyst_date=TODAY,
    )

    before = build_candidates(
        _pricing_view(expanding, today=TODAY, event_dates=before_dates),
        chain,
        StrategySettings(),
        today=TODAY,
        event_dates=before_dates,
    )
    after = build_candidates(
        _pricing_view(expanding, today=TODAY, event_dates=after_dates),
        chain,
        StrategySettings(),
        today=TODAY,
        event_dates=after_dates,
    )

    assert before and all(candidate.view_sigma == pytest.approx(0.25 * 1.15) for candidate in before)
    assert after and all(candidate.view_sigma == pytest.approx(0.25) for candidate in after)

    neutral = View("BAC", "neutral", 1, 0.0, 14)
    premium_chain = make_chain(
        symbol="BAC", spot=600, iv=0.25, hv20=0.12,
        expiries=(expiry,), today=TODAY,
    )
    before_structures = {
        candidate.structure for candidate in build_candidates(
            neutral, premium_chain, StrategySettings(), today=TODAY, event_dates=before_dates
        )
    }
    after_structures = {
        candidate.structure for candidate in build_candidates(
            neutral, premium_chain, StrategySettings(), today=TODAY, event_dates=after_dates
        )
    }
    assert "iron_condor" not in before_structures
    assert "iron_condor" in after_structures

    group = PositionGroup(
        symbol="BAC",
        legs=[Position("BAC", "OPT", -1, 2.0, 100, expiry, 50.0, "P")],
        multiplier=100,
        entry_cost=-200.0,
        max_loss=300.0,
        max_profit=200.0,
    )
    before_alerts = exit_alerts(
        group, today=TODAY, rules=RulesSettings(), earnings=before_dates
    )
    after_alerts = exit_alerts(
        group, today=TODAY, rules=RulesSettings(), earnings=after_dates
    )
    assert any(alert["code"] == "earnings" for alert in before_alerts)
    assert not any(alert["code"] == "earnings" for alert in after_alerts)


def test_pricing_view_clears_released_today_catalyst_without_mutating_stored_view() -> None:
    stored = View(
        symbol="BAC",
        direction="bearish",
        conviction=3,
        expected_move_pct=0.05,
        horizon_days=14,
        volatility="expand",
        catalyst="财报 10/14盘前",
        catalyst_date=TODAY,
    )

    pricing = _pricing_view(stored, today=TODAY, event_dates=())

    assert pricing is not stored
    assert pricing.catalyst_date is None
    assert stored.catalyst_date == TODAY
    assert stored.catalyst == "财报 10/14盘前"


def test_ledger_catalyst_selection_skips_released_earnings() -> None:
    evidence = {"BAC": Evidence(score=-2.0, vol_score=2.0)}
    event = EarningsEvent("BAC", TODAY, "bmo")

    before = update_ledger(
        {}, evidence, earnings=[event], core=[], now=_ny(8), settings=ViewsSettings()
    )
    after = update_ledger(
        {}, evidence, earnings=[event], core=[], now=_ny(13, 42), settings=ViewsSettings()
    )

    assert before.book["BAC"]["catalyst_date"] == TODAY.isoformat()
    assert "盘前" in before.book["BAC"]["catalyst"]
    assert after.book["BAC"]["catalyst_date"] is None
    assert after.book["BAC"]["catalyst"] == ""
