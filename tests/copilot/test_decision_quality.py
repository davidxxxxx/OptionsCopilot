from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from copilot.analysis.models import View
from copilot.config import OptionsSettings, StrategySettings
from copilot.market.models import AccountSnapshot
from copilot.pipeline import _refresh_live_quotes
from copilot.strategies.builder import Candidate, build_candidates
from copilot.strategies.decision_quality import (
    annotate_validity,
    is_admissible,
    limit_net_price,
    reprice_metrics,
)
from copilot.strategies.ranking import (
    dropped_ideas,
    rank_candidates,
    to_idea,
    watch_entry_from_idea,
    watchlist_from_candidates,
)

from .conftest import TODAY, make_chain

NEW_YORK = ZoneInfo("America/New_York")
SETTINGS = StrategySettings()


def _view(
    direction: str = "bullish", *, symbol: str = "SPY", volatility: str = "neutral"
) -> View:
    return View(
        symbol=symbol,
        direction=direction,
        conviction=5 if direction != "neutral" else 1,
        expected_move_pct=0.06 if direction != "neutral" else 0.0,
        horizon_days=14,
        volatility=volatility,
    )


def _candidate(structure: str = "bull_call_spread") -> Candidate:
    directions = {
        "long_put": "bearish",
        "bear_put_spread": "bearish",
        "iron_condor": "neutral",
        "call_calendar": "neutral",
    }
    direction = directions.get(structure, "bullish")
    chain_kwargs = {}
    if structure == "call_calendar":
        chain_kwargs["expiries"] = (
            TODAY + timedelta(days=21),
            TODAY + timedelta(days=49),
        )
    chain = make_chain(
        spot=600,
        iv=0.30 if structure == "iron_condor" else 0.18,
        hv20=0.12,
        **chain_kwargs,
    )
    expiries = None
    if structure == "call_calendar":
        expiries = [min({quote.expiry for quote in chain.quotes})]
    found = build_candidates(
        _view(direction), chain, SETTINGS, today=TODAY, expiries=expiries
    )
    return next(candidate for candidate in found if candidate.structure == structure)


def _with_returns(candidate: Candidate, *, edge_pct: float, view_pct: float) -> Candidate:
    loss = candidate.profile.max_loss
    return replace(candidate, edge=edge_pct * loss, view_ev=view_pct * loss)


@pytest.mark.parametrize(
    ("edge_pct", "view_pct"),
    [(0.041, -0.012), (0.004, -0.001)],
)
def test_admission_rejects_positive_edge_with_negative_view_ev(
    edge_pct: float, view_pct: float
) -> None:
    candidate = _with_returns(_candidate(), edge_pct=edge_pct, view_pct=view_pct)
    assert candidate.edge > 0 and candidate.view_ev < 0
    assert not is_admissible(candidate, SETTINGS)
    assert rank_candidates(
        [candidate], views={"SPY": _view()}, settings=SETTINGS
    ) == []


def test_admission_accepts_exact_five_percent_boundary() -> None:
    candidate = _with_returns(_candidate(), edge_pct=0.05, view_pct=0.05)
    assert is_admissible(candidate, SETTINGS)
    idea = rank_candidates(
        [candidate], views={"SPY": _view()}, settings=SETTINGS
    )[0]
    assert idea["expected_return_pct"] == pytest.approx(0.05)


def test_drop_reason_names_conservative_expected_profit_gate() -> None:
    candidate = _with_returns(_candidate(), edge_pct=0.06, view_pct=0.01)
    previous = [
        to_idea(
            candidate,
            1,
            view=_view(),
            settings=replace(SETTINGS, min_expected_return_pct=0.0),
            account=None,
        )
    ]
    dropped = dropped_ideas(
        previous,
        [],
        [candidate],
        settings=SETTINGS,
        account=None,
        analyzed={"SPY"},
    )
    assert dropped[0]["reason"] == "按观点预计净收益不足（含模型误差余量）"


@pytest.mark.parametrize("origin", ["event", "vrp"])
def test_admission_applies_to_event_and_vrp_origins(origin: str) -> None:
    candidate = replace(
        _with_returns(_candidate(), edge_pct=0.06, view_pct=0.01), origin=origin
    )
    assert rank_candidates(
        [candidate],
        views={},
        rule_views={(origin, "SPY"): _view()},
        settings=SETTINGS,
    ) == []


@pytest.mark.parametrize(
    "structure",
    [
        "long_call",
        "long_put",
        "bull_call_spread",
        "bull_put_spread",
        "iron_condor",
        "call_calendar",
    ],
)
def test_limit_price_matches_brute_force_boundary_for_each_structure(
    structure: str,
) -> None:
    candidate = _with_returns(_candidate(structure), edge_pct=0.10, view_pct=0.12)
    limit = limit_net_price(candidate, SETTINGS.min_expected_return_pct)
    low = candidate.profile.net_debit - 50.0
    high = candidate.profile.net_debit + 50.0
    for _ in range(100):
        midpoint = (low + high) / 2
        metrics = reprice_metrics(
            net_price_now=candidate.profile.net_debit,
            net_price=midpoint,
            multiplier=candidate.multiplier,
            view_ev_now=candidate.view_ev,
            edge_now=candidate.edge,
            max_loss_now=candidate.profile.max_loss,
        )
        margin = metrics["expected_return"] - (
            SETTINGS.min_expected_return_pct * metrics["max_loss"]
        )
        if margin >= 0:
            low = midpoint
        else:
            high = midpoint
    assert limit == pytest.approx((low + high) / 2, abs=1e-9)
    boundary = reprice_metrics(
        net_price_now=candidate.profile.net_debit,
        net_price=limit,
        multiplier=candidate.multiplier,
        view_ev_now=candidate.view_ev,
        edge_now=candidate.edge,
        max_loss_now=candidate.profile.max_loss,
    )
    assert boundary["expected_return_pct"] == pytest.approx(0.05, abs=1e-12)
    assert limit > candidate.profile.net_debit
    if candidate.profile.net_debit < 0:
        assert abs(limit) < abs(candidate.profile.net_debit)


def test_validity_uses_quote_window_in_session_and_opening_recheck_overnight() -> None:
    options = OptionsSettings(quote_validity_minutes=30)
    quote_time = datetime(2026, 10, 14, 14, 0, tzinfo=NEW_YORK)
    regular = {"quote_time": quote_time.isoformat()}
    annotate_validity(
        regular, now=quote_time + timedelta(minutes=5), options=options
    )
    assert regular["opening_recheck"] is False
    assert datetime.fromisoformat(regular["valid_until"]) == quote_time + timedelta(minutes=30)

    overnight = {"quote_time": quote_time.isoformat()}
    annotate_validity(
        overnight,
        now=datetime(2026, 10, 16, 20, 0, tzinfo=NEW_YORK),
        options=options,
    )
    assert overnight["opening_recheck"] is True
    assert datetime.fromisoformat(overnight["valid_until"]) == datetime(
        2026, 10, 19, 9, 45, tzinfo=NEW_YORK
    )


def test_expired_quote_label_is_rendered_by_the_gui() -> None:
    root = Path(__file__).parents[2]
    script = (root / "copilot/web/static/app.js").read_text(encoding="utf-8")
    styles = (root / "copilot/web/static/styles.css").read_text(encoding="utf-8")
    assert "报价已过期，需重新运行" in script
    assert "quote-expired" in script and ".quote-expired" in styles


def test_live_price_beyond_limit_is_removed_and_can_enter_watchlist() -> None:
    candidate = _with_returns(_candidate(), edge_pct=0.10, view_pct=0.12)
    view = _view()
    account = AccountSnapshot(
        observed_at=datetime.now(timezone.utc), net_liquidation=20_000.0
    )
    idea = to_idea(
        candidate, 1, view=view, settings=SETTINGS, account=account
    )
    idea["delayed_data"] = True
    target = float(idea["limit_net_price"]) + 0.25
    current = float(idea["net_debit"])
    first = idea["legs"][0]
    quantity = int(first["signed_quantity"])
    desired_first_fill = float(first["fill"]) + (target - current) / quantity
    live = {}
    for index, leg in enumerate(idea["legs"]):
        spec = (
            idea["symbol"],
            datetime.fromisoformat(leg["expiry"]).date(),
            float(leg["strike"]),
            leg["right"],
        )
        fill = desired_first_fill if index == 0 else float(leg["fill"])
        live[spec] = (fill, fill)

    class Market:
        def live_quotes(self, specs):
            return {spec: live[spec] for spec in specs}

    ideas = [idea]
    result, rejected = _refresh_live_quotes(
        Market(), [ideas], strategy=SETTINGS, account=account, portfolio=None
    )
    assert result["price_invalid"] == 1
    assert ideas == [] and rejected[0]["live_status"] == "price_invalid"
    assert rejected[0]["live_net_price"] > rejected[0]["limit_net_price"]
    assert rejected[0]["suggested_quantity"] <= idea["suggested_quantity"]
    watch = watch_entry_from_idea(
        rejected[0], view=view, now=datetime.now(timezone.utc), reason="live_price_invalid"
    )
    assert watch["reason"] == "live_price_invalid"
    assert watch["current_net_price"] == rejected[0]["live_net_price"]


def test_watchlist_filters_price_gap_and_sets_invalidation_and_watch_until() -> None:
    view = replace(_view(), horizon_days=30, catalyst="财报")
    near = _with_returns(_candidate(), edge_pct=0.049, view_pct=0.049)
    far = _with_returns(_candidate("long_call"), edge_pct=-2.0, view_pct=-2.0)
    entries = watchlist_from_candidates(
        [near, far], views={"SPY": view}, settings=SETTINGS,
        now=datetime(2026, 10, 14, 12, 0, tzinfo=NEW_YORK),
    )
    assert len(entries) == 1
    assert entries[0]["structure"] == near.structure
    assert "观点结束或反转" in entries[0]["invalidation"]
    assert "催化剂已过" in entries[0]["invalidation"]
    assert entries[0]["watch_until"] == "2026-10-21"


def test_watchlist_card_states_entries_are_not_recommendations() -> None:
    root = Path(__file__).parents[2]
    html = (root / "copilot/web/static/index.html").read_text(encoding="utf-8")
    assert "观察清单（价格到位再评估）" in html
    assert "不是推荐，也不计入纸面跟踪" in html
