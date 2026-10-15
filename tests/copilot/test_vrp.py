from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from copilot.strategies.builder import build_vrp_candidate
from copilot.strategies.vrp import blackout_event, candidate_block_reason, entry_block_reason

from .conftest import TODAY, make_chain


def test_vrp_selects_expiry_and_short_delta(settings) -> None:
    chain = make_chain(
        expiries=(
            TODAY + timedelta(days=32),
            TODAY + timedelta(days=39),
            TODAY + timedelta(days=44),
        ),
        iv=0.25,
        hv20=0.12,
        count=40,
    )

    candidate = build_vrp_candidate(
        chain,
        settings.strategy,
        today=TODAY,
        dte_min=30,
        dte_max=45,
        short_delta=0.20,
        risk_budget=None,
    )

    assert candidate is not None
    assert candidate.dte == 39
    short_leg = next(leg for leg in candidate.legs if leg.quantity < 0)
    assert short_leg.quote.delta is not None
    assert 0.15 <= abs(short_leg.quote.delta) <= 0.25
    assert candidate.origin == "vrp"


def test_vrp_width_uses_budget_and_unknown_nlv_uses_narrowest(settings) -> None:
    chain = make_chain(
        expiries=(TODAY + timedelta(days=39),),
        iv=0.25,
        hv20=0.12,
        count=40,
    )

    narrow = build_vrp_candidate(
        chain,
        settings.strategy,
        today=TODAY,
        dte_min=30,
        dte_max=45,
        short_delta=0.20,
        risk_budget=None,
    )
    budgeted = build_vrp_candidate(
        chain,
        settings.strategy,
        today=TODAY,
        dte_min=30,
        dte_max=45,
        short_delta=0.20,
        risk_budget=1_000.0,
    )

    assert narrow is not None and budgeted is not None
    narrow_width = narrow.legs[0].strike - narrow.legs[1].strike
    budgeted_width = budgeted.legs[0].strike - budgeted.legs[1].strike
    assert narrow_width == 5.0
    assert budgeted_width == 10.0
    assert budgeted.profile.max_loss <= 1_000.0


def test_vrp_entry_gates_blackout_low_premium_and_unknown_hv(settings) -> None:
    vrp = settings.vrp
    events = [{"category": "cpi", "date": (TODAY + timedelta(days=2)).isoformat()}]
    blackout = blackout_event(events, TODAY, vrp.event_blackout_days)

    assert blackout == events[0]
    assert entry_block_reason(iv30=0.25, hv20=0.15, blackout=blackout, settings=vrp) == "重大宏观事件静默期"
    assert entry_block_reason(iv30=0.25, hv20=None, blackout=None, settings=vrp) == "HV20 未知"
    assert entry_block_reason(iv30=0.16, hv20=0.15, blackout=None, settings=vrp) == "IV−HV20 不足"
    assert entry_block_reason(iv30=0.25, hv20=0.15, blackout=None, settings=vrp) is None


def test_vrp_candidate_gates_positive_edge_and_portfolio_cap(settings) -> None:
    chain = make_chain(
        expiries=(TODAY + timedelta(days=39),),
        iv=0.25,
        hv20=0.12,
        count=40,
    )
    candidate = build_vrp_candidate(
        chain,
        settings.strategy,
        today=TODAY,
        dte_min=30,
        dte_max=45,
        short_delta=0.20,
        risk_budget=1_000.0,
    )

    assert candidate is not None and candidate.edge > 0
    assert candidate_block_reason(candidate, candidate.profile.max_loss - 0.01) == "组合风险余额不足"
    assert candidate_block_reason(candidate, candidate.profile.max_loss) is None
    assert candidate_block_reason(replace(candidate, edge=0.0), None) == "期望优势不为正"

