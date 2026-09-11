from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
)
from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomics,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)
from options_copilot.storage.canonical import canonical_hash
from options_copilot.news.preselection import (
    evaluate_preselection,
    evaluate_preselection_batch,
    strategy_structure_hash,
)


NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)


def _candidate(identifier: str, *, offset: int = 0, age_ms: int = 0):
    leg = ConditionalOptionLeg(
        underlying="AAPL",
        con_id=7000 + offset,
        expiry=date(2026, 8, 21),
        strike=Decimal(200 + offset),
        right=OptionRight.CALL,
        side=OptionLegSide.BUY,
        ratio=1,
        quantity=1,
        bid=Decimal("4.90"),
        ask=Decimal("5.00"),
        quote_asof=NOW - timedelta(milliseconds=age_ms),
        quote_batch_id="quote-open-1",
        implied_volatility=Decimal("0.25"),
        delta=Decimal("0.50"),
        gamma=Decimal("0.03"),
        theta=Decimal("-0.08"),
        vega=Decimal("0.12"),
        volume=1200,
        open_interest=9000,
        dte=16,
        local_symbol=f"AAPL  260821C00{200 + offset}000",
        trading_class="AAPL",
        multiplier=100,
        exchange="SMART",
    )
    strategy_hash = strategy_structure_hash("AAPL", "LONG_CALL", (leg,))
    scenario_asof = leg.quote_asof - timedelta(minutes=1)
    scenarios = (
        PreselectionTerminalScenario(Decimal("190"), Decimal("0.5")),
        PreselectionTerminalScenario(Decimal("220"), Decimal("0.5")),
    )
    scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=identifier,
        strategy_hash=strategy_hash,
        scenario_asof=scenario_asof,
        scenarios=tuple(
            TrustedTerminalScenario(
                item.terminal_underlying_price,
                item.probability,
            )
            for item in scenarios
        ),
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )
    snapshot_hash = "b" * 64
    nav = Decimal("10000")
    nav_hash = strategy_nav_post_hash(
        candidate_id=identifier,
        strategy_hash=strategy_hash,
        snapshot_hash=snapshot_hash,
        strategy_nav_usd=nav,
    )
    provisional = OpenRepriceEconomics(
        candidate_id=identifier,
        strategy_hash=strategy_hash,
        broker_snapshot_hash=snapshot_hash,
        quote_batch_id="quote-open-1",
        quote_asof=leg.quote_asof,
        scenario_hash=scenario_set.scenario_hash,
        scenario_asof=scenario_asof,
        cost_contract_version=EXECUTION_COST_VERSION,
        cost_contract_hash=EXECUTION_COST_HASH,
        policy_version=INITIAL_POLICY_VERSION,
        policy_hash=INITIAL_POLICY_HASH,
        strategy_nav_usd=nav,
        strategy_nav_post_hash=nav_hash,
        debit_usd=Decimal("500"),
        credit_usd=Decimal("0"),
        commission_usd=Decimal("2.5"),
        entry_slippage_usd=Decimal("2.5"),
        exit_slippage_usd=Decimal("5"),
        total_slippage_usd=Decimal("7.5"),
        all_in_cost_usd=Decimal("510"),
        maximum_loss_usd=Decimal("510"),
        before_cost_expected_value_usd=Decimal("60"),
        after_cost_expected_value_usd=Decimal("50"),
        payoff_hash="c" * 64,
        risk_fraction=Decimal("0.051"),
        economics_hash="0" * 64,
    )
    return ConditionalOptionPreselection(
        preselection_id=identifier,
        underlying="AAPL",
        strategy_type="LONG_CALL",
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=(leg,),
        risk_defined=True,
        maximum_loss_usd=Decimal("510"),
        estimated_cost_usd=Decimal("510"),
        cost_after_ev_usd=Decimal("50"),
        entry_condition="Display-only condition.",
        invalidation_condition="Display-only invalidation.",
        profit_target_condition="Display-only target.",
        stop_loss_condition="Display-only stop.",
        evidence_ids=("evidence",),
        evidence_hashes=("a" * 64,),
        strategy_hash=strategy_hash,
        research_summary="Supporting-only fixture.",
        terminal_scenarios=scenarios,
        scenario_asof=scenario_asof,
        scenario_hash=scenario_set.scenario_hash,
        execution_cost_contract_version=EXECUTION_COST_VERSION,
        execution_cost_contract_hash=EXECUTION_COST_HASH,
        risk_policy_version=INITIAL_POLICY_VERSION,
        risk_policy_hash=INITIAL_POLICY_HASH,
        broker_snapshot_hash=snapshot_hash,
        strategy_nav_usd=nav,
        strategy_nav_post_hash=nav_hash,
        economics_quote_batch_id="quote-open-1",
        economics_quote_asof=leg.quote_asof,
        payoff_hash="c" * 64,
        economics_calculation_hash=canonical_hash(provisional.hash_payload()),
        debit_usd=Decimal("500"),
        credit_usd=Decimal("0"),
        net_entry_cost_usd=Decimal("510"),
        estimated_commission_usd=Decimal("2.5"),
        estimated_entry_slippage_usd=Decimal("2.5"),
        estimated_exit_slippage_usd=Decimal("5"),
        estimated_slippage_usd=Decimal("7.5"),
        expected_value_before_costs_usd=Decimal("60"),
        risk_fraction=Decimal("0.051"),
    )


@pytest.mark.parametrize(
    "field",
    (
        "con_id",
        "local_symbol",
        "trading_class",
        "multiplier",
        "exchange",
        "expiry",
        "strike",
        "right",
        "side",
        "ratio",
        "quantity",
    ),
)
def test_every_frozen_identity_field_missing_blocks(field: str) -> None:
    candidate = _candidate("identity")
    broken = replace(candidate, legs=(replace(candidate.legs[0], **{field: None}),))
    assert not evaluate_preselection_batch((broken,), now=NOW).action_pool_eligible


@pytest.mark.parametrize(
    "field",
    (
        "bid",
        "ask",
        "quote_asof",
        "quote_batch_id",
        "implied_volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "volume",
        "open_interest",
    ),
)
def test_every_dynamic_quote_field_missing_blocks_whole_batch(field: str) -> None:
    candidate = _candidate("dynamic")
    broken = replace(candidate, legs=(replace(candidate.legs[0], **{field: None}),))
    evaluated = evaluate_preselection_batch((broken,), now=NOW)
    assert evaluated.outcome == "NO_TRADE"
    assert evaluated.action_pool == ()


def test_quote_freshness_boundary_is_exact() -> None:
    assert evaluate_preselection(_candidate("fresh", age_ms=5000), now=NOW).action_pool_eligible
    assert not evaluate_preselection(_candidate("stale", age_ms=5001), now=NOW).action_pool_eligible


def test_ten_succeeds_and_eleven_fails_closed() -> None:
    ten = tuple(_candidate(f"candidate-{index}", offset=index) for index in range(10))
    eleven = (*ten, _candidate("candidate-10", offset=10))
    assert evaluate_preselection_batch(ten, now=NOW).action_pool_eligible
    assert not evaluate_preselection_batch(eleven, now=NOW).action_pool_eligible


def test_one_crossed_or_cross_batch_quote_blocks_every_candidate() -> None:
    first = _candidate("first")
    crossed = replace(
        _candidate("second", offset=1),
        legs=(replace(_candidate("second", offset=1).legs[0], bid=Decimal("6")),),
    )
    assert evaluate_preselection_batch((first, crossed), now=NOW).action_pool == ()
    other_batch = replace(
        crossed,
        legs=(replace(crossed.legs[0], bid=Decimal("4.90"), quote_batch_id="other"),),
    )
    assert evaluate_preselection_batch((first, other_batch), now=NOW).action_pool == ()


def test_parent_set_must_be_complete() -> None:
    candidate = _candidate("first")
    result = evaluate_preselection_batch(
        (candidate,), now=NOW, expected_preselection_ids=("first", "second")
    )
    assert result.outcome == "NO_TRADE"
    assert result.action_pool == ()
