from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
)
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
)
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomics,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)
from options_copilot.news.preselection import (
    build_preselection_pools,
    evaluate_preselection,
    evaluate_preselection_batch,
    strategy_structure_hash,
)
from options_copilot.storage.canonical import canonical_hash


UTC = timezone.utc
NOW = datetime(2026, 8, 6, 13, 35, 2, tzinfo=UTC)
QUOTE_ASOF = NOW - timedelta(seconds=1)
SCENARIO_ASOF = QUOTE_ASOF - timedelta(minutes=10)
QUOTE_BATCH = "ibkr-open-batch"
SNAPSHOT_HASH = "a" * 64
PAYOFF_HASH = "b" * 64


def _candidate(
    suffix: int = 1,
    *,
    scenario_asof: datetime = SCENARIO_ASOF,
) -> ConditionalOptionPreselection:
    underlying = f"T{suffix}"
    leg = ConditionalOptionLeg(
        underlying=underlying,
        con_id=1000 + suffix,
        expiry=NOW.date() + timedelta(days=21),
        strike=Decimal("100"),
        right=OptionRight.CALL,
        side=OptionLegSide.BUY,
        ratio=1,
        quantity=1,
        bid=Decimal("0.60"),
        ask=Decimal("0.70"),
        quote_asof=QUOTE_ASOF,
        quote_batch_id=QUOTE_BATCH,
        implied_volatility=Decimal("0.25"),
        delta=Decimal("0.40"),
        gamma=Decimal("0.03"),
        theta=Decimal("-0.02"),
        vega=Decimal("0.11"),
        volume=100,
        open_interest=500,
        dte=21,
        local_symbol=f"{underlying}  260827C00100000",
        trading_class=underlying,
        multiplier=100,
        exchange="SMART",
    )
    strategy_hash = strategy_structure_hash(underlying, "LONG_CALL", (leg,))
    scenarios = (
        PreselectionTerminalScenario(Decimal("90"), Decimal("0.50")),
        PreselectionTerminalScenario(Decimal("110"), Decimal("0.50")),
    )
    scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=f"candidate-{suffix}",
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
    nav = Decimal("10000")
    maximum_loss = Decimal("100")
    debit = Decimal("70.00")
    credit = Decimal("0.00")
    commission = Decimal("2.50")
    entry_slippage = Decimal("1.00")
    exit_slippage = Decimal("2.00")
    total_slippage = entry_slippage + exit_slippage
    all_in_cost = debit - credit + commission + total_slippage
    after_cost_ev = Decimal("10.00")
    before_cost_ev = after_cost_ev + commission + total_slippage
    risk_fraction = maximum_loss / nav
    nav_hash = strategy_nav_post_hash(
        candidate_id=f"candidate-{suffix}",
        strategy_hash=strategy_hash,
        snapshot_hash=SNAPSHOT_HASH,
        strategy_nav_usd=nav,
    )
    economics = OpenRepriceEconomics(
        candidate_id=f"candidate-{suffix}",
        strategy_hash=strategy_hash,
        broker_snapshot_hash=SNAPSHOT_HASH,
        quote_batch_id=QUOTE_BATCH,
        quote_asof=QUOTE_ASOF,
        scenario_hash=scenario_set.scenario_hash,
        scenario_asof=scenario_asof,
        cost_contract_version=EXECUTION_COST_VERSION,
        cost_contract_hash=EXECUTION_COST_HASH,
        policy_version=INITIAL_POLICY_VERSION,
        policy_hash=INITIAL_POLICY_HASH,
        strategy_nav_usd=nav,
        strategy_nav_post_hash=nav_hash,
        debit_usd=debit,
        credit_usd=credit,
        commission_usd=commission,
        entry_slippage_usd=entry_slippage,
        exit_slippage_usd=exit_slippage,
        total_slippage_usd=total_slippage,
        all_in_cost_usd=all_in_cost,
        maximum_loss_usd=maximum_loss,
        before_cost_expected_value_usd=before_cost_ev,
        after_cost_expected_value_usd=after_cost_ev,
        payoff_hash=PAYOFF_HASH,
        risk_fraction=risk_fraction,
        economics_hash="0" * 64,
    )
    economics_hash = canonical_hash(economics.hash_payload())
    return ConditionalOptionPreselection(
        preselection_id=f"candidate-{suffix}",
        underlying=underlying,
        strategy_type="LONG_CALL",
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=(leg,),
        risk_defined=True,
        maximum_loss_usd=maximum_loss,
        estimated_cost_usd=all_in_cost,
        cost_after_ev_usd=after_cost_ev,
        entry_condition="The current executable quote remains valid.",
        invalidation_condition="The frozen thesis is invalidated.",
        profit_target_condition="The defined profit target is reached.",
        stop_loss_condition="The defined risk stop is reached.",
        evidence_ids=(f"evidence-{suffix}",),
        evidence_hashes=(canonical_hash({"evidence": suffix}),),
        strategy_hash=strategy_hash,
        research_summary="Hash-bound supporting-only open repricing.",
        terminal_scenarios=scenarios,
        scenario_asof=scenario_asof,
        scenario_hash=scenario_set.scenario_hash,
        execution_cost_contract_version=EXECUTION_COST_VERSION,
        execution_cost_contract_hash=EXECUTION_COST_HASH,
        risk_policy_version=INITIAL_POLICY_VERSION,
        risk_policy_hash=INITIAL_POLICY_HASH,
        broker_snapshot_hash=SNAPSHOT_HASH,
        strategy_nav_usd=nav,
        strategy_nav_post_hash=nav_hash,
        economics_quote_batch_id=QUOTE_BATCH,
        economics_quote_asof=QUOTE_ASOF,
        payoff_hash=PAYOFF_HASH,
        economics_calculation_hash=economics_hash,
        debit_usd=debit,
        credit_usd=credit,
        net_entry_cost_usd=all_in_cost,
        estimated_commission_usd=commission,
        estimated_entry_slippage_usd=entry_slippage,
        estimated_exit_slippage_usd=exit_slippage,
        estimated_slippage_usd=total_slippage,
        expected_value_before_costs_usd=before_cost_ev,
        risk_fraction=risk_fraction,
    )


def _blockers(candidate: ConditionalOptionPreselection) -> tuple[str, ...]:
    return evaluate_preselection(candidate, now=NOW).blockers


def test_valid_hash_bound_open_economics_is_display_action_eligible() -> None:
    evaluated = evaluate_preselection(_candidate(), now=NOW)

    assert evaluated.blockers == ()
    assert evaluated.action_pool_eligible is True


def test_legacy_open_row_remains_visible_but_cannot_enter_action_pool() -> None:
    candidate = _candidate()
    legacy = replace(
        candidate,
        terminal_scenarios=(),
        scenario_asof=None,
        scenario_hash=None,
        execution_cost_contract_version=None,
        execution_cost_contract_hash=None,
        risk_policy_version=None,
        risk_policy_hash=None,
        broker_snapshot_hash=None,
        strategy_nav_usd=None,
        strategy_nav_post_hash=None,
        economics_quote_batch_id=None,
        economics_quote_asof=None,
        payoff_hash=None,
        economics_calculation_hash=None,
        debit_usd=None,
        credit_usd=None,
        net_entry_cost_usd=None,
        estimated_commission_usd=None,
        estimated_entry_slippage_usd=None,
        estimated_exit_slippage_usd=None,
        estimated_slippage_usd=None,
        expected_value_before_costs_usd=None,
        risk_fraction=None,
    )

    _, repriced, action_pool = build_preselection_pools((legacy,), now=NOW)

    assert len(repriced) == 1
    assert action_pool == ()
    assert "OPEN_REPRICE_ECONOMICS_LINEAGE_MISSING" in repriced[0].blockers


def test_current_quote_batch_and_asof_must_match_economics_lineage() -> None:
    candidate = _candidate()

    wrong_batch = replace(candidate, economics_quote_batch_id="other-batch")
    wrong_asof = replace(
        candidate,
        economics_quote_asof=QUOTE_ASOF - timedelta(seconds=1),
    )

    assert "ECONOMICS_QUOTE_BATCH_MISMATCH" in _blockers(wrong_batch)
    assert "ECONOMICS_QUOTE_ASOF_MISMATCH" in _blockers(wrong_asof)


def test_scenario_cost_policy_and_nav_lineage_are_revalidated() -> None:
    candidate = _candidate()

    assertions = (
        (
            replace(candidate, scenario_hash="c" * 64),
            "TERMINAL_SCENARIO_HASH_MISMATCH",
        ),
        (
            replace(candidate, execution_cost_contract_hash="c" * 64),
            "EXECUTION_COST_CONTRACT_HASH_MISMATCH",
        ),
        (
            replace(candidate, risk_policy_hash="c" * 64),
            "RISK_POLICY_CONTRACT_HASH_MISMATCH",
        ),
        (
            replace(candidate, strategy_nav_post_hash="c" * 64),
            "STRATEGY_NAV_LINEAGE_INVALID",
        ),
        (
            replace(candidate, broker_snapshot_hash="c" * 64),
            "STRATEGY_NAV_LINEAGE_INVALID",
        ),
    )
    for changed, reason in assertions:
        assert reason in _blockers(changed)


def test_future_scenario_is_rejected_even_with_self_consistent_hashes() -> None:
    candidate = _candidate(scenario_asof=QUOTE_ASOF + timedelta(microseconds=1))

    assert "TERMINAL_SCENARIO_FROM_FUTURE" in _blockers(candidate)


def test_internal_cost_relationships_and_economics_hash_are_revalidated() -> None:
    candidate = _candidate()
    inconsistent_cost = replace(
        candidate,
        estimated_slippage_usd=Decimal("4.00"),
    )
    tampered_economics = replace(
        candidate,
        maximum_loss_usd=Decimal("101.00"),
        risk_fraction=Decimal("0.0101"),
    )

    assert "OPEN_REPRICE_ECONOMICS_VALUE_MISMATCH" in _blockers(
        inconsistent_cost
    )
    assert "OPEN_REPRICE_ECONOMICS_HASH_MISMATCH" in _blockers(
        tampered_economics
    )


def test_one_invalid_member_blocks_the_entire_open_action_pool() -> None:
    valid = _candidate(1)
    invalid = replace(_candidate(2), economics_calculation_hash="c" * 64)

    batch = evaluate_preselection_batch((valid, invalid), now=NOW)

    assert batch.action_pool_eligible is False
    assert batch.action_pool == ()
    assert any(
        reason
        == "CANDIDATE:candidate-2:OPEN_REPRICE_ECONOMICS_HASH_MISMATCH"
        for reason in batch.blockers
    )
