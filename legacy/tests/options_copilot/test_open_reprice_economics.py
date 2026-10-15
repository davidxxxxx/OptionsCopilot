from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.gateway.broker_snapshot import (
    AtomicBrokerSnapshot,
    BrokerSnapshotStatus,
    SecDefEvidence,
)
from options_copilot.gateway.ibkr_readonly import BatchedOptionQuote, QuoteBatchStatus
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomicsError,
    OpenRepriceEconomicsResolver,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 6, 13, 35, 2, tzinfo=timezone.utc)
QUOTE_ASOF = NOW - timedelta(seconds=1)
STRATEGY_HASH = "1" * 64


@dataclass(frozen=True, slots=True)
class _Leg:
    underlying: str
    con_id: int
    expiry: date
    strike: Decimal
    right: str
    side: str
    ratio: int
    quantity: int
    bid: Decimal
    ask: Decimal
    quote_asof: datetime
    quote_batch_id: str
    multiplier: int
    exchange: str


@dataclass(frozen=True, slots=True)
class _Candidate:
    candidate_id: str
    strategy_hash: str
    legs: tuple[_Leg, ...]
    execution_cost_contract_version: str
    execution_cost_contract_hash: str
    current_policy_version: str
    current_policy_hash: str
    strategy_nav_usd: Decimal
    strategy_nav_post_hash: str
    maximum_loss_usd: Decimal | None = None
    estimated_cost_usd: Decimal | None = None
    cost_after_ev_usd: Decimal | None = None


def _leg(
    con_id: int = 101,
    *,
    strike: str = "100",
    right: str = "CALL",
    side: str = "BUY",
    bid: str = "1.00",
    ask: str = "1.10",
    ratio: int = 1,
    quantity: int = 1,
    quote_asof: datetime = QUOTE_ASOF,
    quote_batch_id: str = "open-1",
) -> _Leg:
    return _Leg(
        underlying="SPY",
        con_id=con_id,
        expiry=date(2026, 8, 21),
        strike=Decimal(strike),
        right=right,
        side=side,
        ratio=ratio,
        quantity=quantity,
        bid=Decimal(bid),
        ask=Decimal(ask),
        quote_asof=quote_asof,
        quote_batch_id=quote_batch_id,
        multiplier=100,
        exchange="SMART",
    )


def _snapshot(
    legs: tuple[_Leg, ...],
    *,
    quote_batch_id: str = "open-1",
    quote_batch_status: QuoteBatchStatus = QuoteBatchStatus.COMPLETE,
) -> AtomicBrokerSnapshot:
    requested_at = min(item.quote_asof for item in legs) - timedelta(milliseconds=100)
    completed_at = max(item.quote_asof for item in legs) + timedelta(milliseconds=100)
    quotes = tuple(
        BatchedOptionQuote(
            contract_id=item.con_id,
            batch_id=item.quote_batch_id,
            request_id=f"request-{index}",
            requested_at=requested_at,
            observed_at=item.quote_asof,
            completed_at=completed_at,
            source="IBKR",
            bid=item.bid,
            ask=item.ask,
            exchange_time=item.quote_asof,
        )
        for index, item in enumerate(legs)
    )
    secdefs = tuple(
        SecDefEvidence(
            contract_id=item.con_id,
            pre_identity={
                "conId": item.con_id,
                "localSymbol": f"SPY-{item.con_id}",
                "tradingClass": "SPY",
                "multiplier": item.multiplier,
                "exchange": item.exchange,
                "expiry": item.expiry,
                "strike": item.strike,
                "right": "C" if item.right == "CALL" else "P",
            },
            post_identity={
                "conId": item.con_id,
                "localSymbol": f"SPY-{item.con_id}",
                "tradingClass": "SPY",
                "multiplier": item.multiplier,
                "exchange": item.exchange,
                "expiry": item.expiry,
                "strike": item.strike,
                "right": "C" if item.right == "CALL" else "P",
            },
            pre_hash="2" * 64,
            post_hash="2" * 64,
            stable=True,
            standard_contract=True,
            adjusted=False,
            pre_source="IBKR",
            post_source="IBKR",
        )
        for item in legs
    )
    fields = dict(
        built_at=NOW,
        status=BrokerSnapshotStatus.COMPLETE,
        reason_codes=(),
        state_evidence={},
        secdef_evidence=secdefs,
        quote_batch_id=quote_batch_id,
        quote_batch_status=quote_batch_status,
        quote_batch_source="IBKR",
        quote_batch_requested_at=requested_at,
        quote_batch_completed_at=completed_at,
        quote_batch_observed_at=(
            legs[0].quote_asof
            if len({item.quote_asof for item in legs}) == 1
            else None
        ),
        quotes=quotes,
        oldest_quote_age_seconds=max(
            Decimal(str((NOW - item.quote_asof).total_seconds())) for item in legs
        ),
        maximum_leg_skew_seconds=Decimal(
            str((max(item.quote_asof for item in legs) - min(item.quote_asof for item in legs)).total_seconds())
        ),
    )
    provisional = AtomicBrokerSnapshot(**fields, snapshot_hash="0" * 64)
    return replace(provisional, snapshot_hash=canonical_hash(provisional.hash_payload()))


def _candidate(
    snapshot: AtomicBrokerSnapshot,
    legs: tuple[_Leg, ...],
    *,
    nav: str = "10000",
    strategy_hash: str = STRATEGY_HASH,
) -> _Candidate:
    nav_value = Decimal(nav)
    return _Candidate(
        candidate_id="spy-open-1",
        strategy_hash=strategy_hash,
        legs=legs,
        execution_cost_contract_version=EXECUTION_COST_VERSION,
        execution_cost_contract_hash=EXECUTION_COST_HASH,
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
        strategy_nav_usd=nav_value,
        strategy_nav_post_hash=strategy_nav_post_hash(
            candidate_id="spy-open-1",
            strategy_hash=strategy_hash,
            snapshot_hash=snapshot.snapshot_hash,
            strategy_nav_usd=nav_value,
        ),
        maximum_loss_usd=Decimal("999999"),
        estimated_cost_usd=Decimal("888888"),
        cost_after_ev_usd=Decimal("777777"),
    )


def _scenario_set(
    *,
    scenario_asof: datetime = QUOTE_ASOF,
    scenarios: tuple[TrustedTerminalScenario, ...] | None = None,
) -> TrustedTerminalScenarioSet:
    return TrustedTerminalScenarioSet.create(
        candidate_id="spy-open-1",
        strategy_hash=STRATEGY_HASH,
        scenario_asof=scenario_asof,
        scenarios=scenarios
        or (
            TrustedTerminalScenario(Decimal("90"), Decimal("0.50")),
            TrustedTerminalScenario(Decimal("110"), Decimal("0.50")),
        ),
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )


def _resolve(
    candidate: _Candidate,
    snapshot: AtomicBrokerSnapshot,
    scenarios: TrustedTerminalScenarioSet | None = None,
):
    return OpenRepriceEconomicsResolver().resolve(
        candidate,
        snapshot=snapshot,
        scenario_set=scenarios or _scenario_set(),
        now=NOW,
    )


def test_quote_change_recomputes_all_in_cost_max_loss_and_after_cost_ev() -> None:
    first_legs = (_leg(),)
    first_snapshot = _snapshot(first_legs)
    first = _resolve(_candidate(first_snapshot, first_legs), first_snapshot)

    second_legs = (_leg(ask="1.30"),)
    second_snapshot = _snapshot(second_legs)
    second = _resolve(_candidate(second_snapshot, second_legs), second_snapshot)

    assert second.all_in_cost_usd != first.all_in_cost_usd
    assert second.maximum_loss_usd != first.maximum_loss_usd
    assert second.after_cost_expected_value_usd != first.after_cost_expected_value_usd


def test_legacy_candidate_money_claims_are_ignored() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    candidate = _candidate(snapshot, legs)
    baseline = _resolve(candidate, snapshot)
    poisoned = replace(
        candidate,
        maximum_loss_usd=Decimal("0"),
        estimated_cost_usd=Decimal("0"),
        cost_after_ev_usd=Decimal("-999999"),
    )

    assert _resolve(poisoned, snapshot) == baseline


def test_commission_and_both_slippages_enter_max_loss_and_ev() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    result = _resolve(_candidate(snapshot, legs), snapshot)

    assert result.debit_usd == Decimal("110.00")
    assert result.credit_usd == Decimal("0.00")
    assert result.commission_usd == Decimal("2.50")
    assert result.entry_slippage_usd == Decimal("2.50")
    assert result.exit_slippage_usd == Decimal("5.00")
    assert result.total_slippage_usd == Decimal("7.50")
    assert result.all_in_cost_usd == Decimal("120.00")
    assert result.maximum_loss_usd == Decimal("120.00")
    assert result.before_cost_expected_value_usd == Decimal("390.000")
    assert result.after_cost_expected_value_usd == Decimal("380.000")


def test_missing_scenario_set_fails_closed() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        OpenRepriceEconomicsResolver().resolve(
            _candidate(snapshot, legs), snapshot=snapshot, scenario_set=None, now=NOW
        )
    assert raised.value.reason_code == "SCENARIO_SET_MISSING"


def test_scenario_probability_sum_must_equal_one() -> None:
    invalid = _scenario_set(
        scenarios=(
            TrustedTerminalScenario(Decimal("90"), Decimal("0.60")),
            TrustedTerminalScenario(Decimal("110"), Decimal("0.50")),
        )
    )
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot, invalid)
    assert raised.value.reason_code == "SCENARIO_PROBABILITY_INVALID"


def test_scenario_asof_cannot_be_later_than_quote() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    future = _scenario_set(scenario_asof=QUOTE_ASOF + timedelta(microseconds=1))
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot, future)
    assert raised.value.reason_code == "SCENARIO_FUTURE_INFORMATION"


@pytest.mark.parametrize(
    ("field", "reason"),
    (
        ("execution_cost_contract_version", "COST_CONTRACT_BINDING_INVALID"),
        ("execution_cost_contract_hash", "COST_CONTRACT_BINDING_INVALID"),
        ("current_policy_version", "POLICY_CONTRACT_BINDING_INVALID"),
        ("current_policy_hash", "POLICY_CONTRACT_BINDING_INVALID"),
    ),
)
def test_candidate_cost_and_policy_bindings_are_exact(field: str, reason: str) -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    candidate = _candidate(snapshot, legs)
    replacement = "wrong" if field.endswith("version") else "f" * 64
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(replace(candidate, **{field: replacement}), snapshot)
    assert raised.value.reason_code == reason


def test_snapshot_hash_tamper_fails_closed() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    candidate = _candidate(snapshot, legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(candidate, replace(snapshot, snapshot_hash="f" * 64))
    assert raised.value.reason_code == "BROKER_SNAPSHOT_INVALID"


def test_partial_or_mixed_quote_batch_fails_closed() -> None:
    legs = (_leg(101), _leg(102, strike="105", quote_batch_id="other-batch"))
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot)
    assert raised.value.reason_code == "QUOTE_BATCH_MISMATCH"


@pytest.mark.parametrize("age_seconds", (Decimal("5.000001"), Decimal("-0.000001")))
def test_stale_or_future_quotes_fail_closed(age_seconds: Decimal) -> None:
    quote_asof = NOW - timedelta(seconds=float(age_seconds))
    legs = (_leg(quote_asof=quote_asof),)
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot)
    assert raised.value.reason_code == "QUOTE_STALE_OR_FUTURE"


def test_candidate_and_snapshot_contract_sets_must_match_exactly() -> None:
    candidate_legs = (_leg(101),)
    snapshot = _snapshot((_leg(102),))
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, candidate_legs), snapshot)
    assert raised.value.reason_code == "QUOTE_BATCH_MISMATCH"


def test_one_candidate_can_resolve_from_a_complete_multi_structure_batch() -> None:
    candidate_legs = (_leg(101),)
    snapshot = _snapshot((
        *candidate_legs,
        _leg(102, strike="105"),
    ))

    result = _resolve(_candidate(snapshot, candidate_legs), snapshot)

    assert result.quote_batch_id == "open-1"
    assert result.broker_snapshot_hash == snapshot.snapshot_hash


def test_naked_or_unbounded_short_is_rejected() -> None:
    legs = (_leg(side="SELL", bid="2.00", ask="2.10"),)
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot)
    assert raised.value.reason_code == "UNBOUNDED_OR_UNKNOWN_MAX_LOSS"


def test_bounded_net_credit_structure_is_still_unsupported() -> None:
    legs = (
        _leg(201, strike="100", side="SELL", bid="2.00", ask="2.10"),
        _leg(202, strike="110", side="BUY", bid="1.00", ask="1.10"),
    )
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot)
    assert raised.value.reason_code == "NET_CREDIT_UNSUPPORTED"


def test_non_positive_after_cost_ev_is_rejected() -> None:
    scenarios = _scenario_set(
        scenarios=(
            TrustedTerminalScenario(Decimal("90"), Decimal("0.50")),
            TrustedTerminalScenario(Decimal("95"), Decimal("0.50")),
        )
    )
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot, scenarios)
    assert raised.value.reason_code == "NON_POSITIVE_AFTER_COST_EV"


def test_normal_ten_percent_risk_limit_is_enforced_from_bound_nav() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs, nav="500"), snapshot)
    assert raised.value.reason_code == "NORMAL_RISK_LIMIT_EXCEEDED"


def test_nav_value_and_post_hash_are_bound_to_snapshot_and_strategy() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    candidate = replace(
        _candidate(snapshot, legs),
        strategy_nav_post_hash="f" * 64,
    )
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(candidate, snapshot)
    assert raised.value.reason_code == "NAV_BINDING_INVALID"


def test_scenario_hash_must_verify() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    invalid = replace(_scenario_set(), scenario_hash="f" * 64)
    with pytest.raises(OpenRepriceEconomicsError) as raised:
        _resolve(_candidate(snapshot, legs), snapshot, invalid)
    assert raised.value.reason_code == "SCENARIO_HASH_INVALID"


def test_economics_hash_is_repeatable_and_detects_tampering() -> None:
    legs = (_leg(),)
    snapshot = _snapshot(legs)
    candidate = _candidate(snapshot, legs)
    first = _resolve(candidate, snapshot)
    second = _resolve(candidate, snapshot)

    assert first == second
    assert first.verify_hash()
    assert first.as_dict()["economics_hash"] == first.economics_hash
    assert not replace(first, maximum_loss_usd=Decimal("0")).verify_hash()
