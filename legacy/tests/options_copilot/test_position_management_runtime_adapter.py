from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    PositionSnapshot,
    QuoteBatchStatus,
)
from options_copilot.governance.contracts import load_contract
from options_copilot.positions.generator import ManagementCandidateGenerator
from options_copilot.positions.manager import PositionManager
from options_copilot.positions.runtime_adapter import (
    ProductionManagementCoordinator,
)


NOW = datetime(2026, 8, 4, 17, 0, tzinfo=timezone.utc)
COST_CONTRACT_PATH = "options_copilot/governance/execution_cost_contract.v1.json"


def _position(
    contract_id: int,
    strike: int,
    quantity: int,
    *,
    symbol: str = "GLD",
    average_cost: str = "100",
    right: str = "C",
) -> PositionSnapshot:
    return PositionSnapshot(
        asof=NOW - timedelta(seconds=1),
        contract_id=contract_id,
        symbol=symbol,
        local_symbol=f"{symbol}   260821{right}00{strike}000",
        security_type="OPT",
        currency="USD",
        exchange="SMART",
        quantity=Decimal(quantity),
        average_cost=Decimal(average_cost),
        market_price=Decimal("1"),
        market_value=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        realized_pnl=Decimal("0"),
        expiration=date(2026, 8, 21),
        strike=Decimal(strike),
        right=right,
        trading_class=symbol,
        multiplier=100,
    )


def _positions(middle_id: int = 900_000_101) -> tuple[PositionSnapshot, ...]:
    return (
        _position(middle_id - 11, 375, 1),
        _position(middle_id, 385, -2),
        _position(middle_id + 17, 390, 1),
    )


def _qqq_vertical() -> tuple[PositionSnapshot, ...]:
    return (
        _position(
            910_000_724,
            724,
            1,
            symbol="QQQ",
            average_cost="817.10",
        ),
        _position(
            910_000_727,
            727,
            -1,
            symbol="QQQ",
            average_cost="651.00",
        ),
    )


def _standard_vertical(
    *,
    right: str,
    quantities: tuple[int, int],
    average_costs: tuple[str, str],
) -> tuple[PositionSnapshot, ...]:
    return tuple(
        _position(
            920_000_000 + strike,
            strike,
            quantity,
            symbol="QQQ",
            average_cost=average_cost,
            right=right,
        )
        for strike, quantity, average_cost in zip(
            (724, 727),
            quantities,
            average_costs,
        )
    )


def _secdefs(
    contracts: tuple[OptionContractRef, ...],
) -> tuple[OptionSecDefSnapshot, ...]:
    return tuple(
        OptionSecDefSnapshot(
            contract_id=item.contract_id,
            local_symbol=item.local_symbol,
            trading_class=item.trading_class,
            multiplier=item.multiplier,
            exchange=item.exchange,
            expiration=item.expiration,
            strike=item.strike,
            right=item.right,
            security_type="OPT",
            currency="USD",
            standard_contract=True,
            adjusted=False,
            source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        )
        for item in contracts
    )


def _quote_batch(
    contracts: tuple[OptionContractRef, ...],
    *,
    observed_at: datetime | None = None,
) -> OptionQuoteBatch:
    quote_time = observed_at or NOW - timedelta(seconds=1)
    requested_at = min(NOW - timedelta(seconds=2), quote_time)
    completed_at = min(NOW - timedelta(milliseconds=100), quote_time)
    prices = (
        (("8.59", "8.67"), ("6.97", "7.02"))
        if contracts and contracts[0].symbol == "QQQ"
        else (("8.00", "8.20"), ("3.00", "3.20"), ("1.00", "1.20"))
    )
    quotes = tuple(
        BatchedOptionQuote(
            contract_id=item.contract_id,
            batch_id="runtime-management-batch",
            request_id=f"request-{item.contract_id}",
            requested_at=requested_at,
            observed_at=quote_time,
            completed_at=completed_at,
            source="IBKR_REQ_TICKERS_READONLY",
            bid=Decimal(prices[index][0]),
            ask=Decimal(prices[index][1]),
            exchange_time=quote_time,
            volume=200,
            open_interest=2000,
            implied_volatility=Decimal("0.25"),
        )
        for index, item in enumerate(contracts)
    )
    return OptionQuoteBatch(
        batch_id="runtime-management-batch",
        status=QuoteBatchStatus.COMPLETE,
        requested_at=requested_at,
        completed_at=completed_at,
        source="IBKR_REQ_TICKERS_READONLY",
        quotes=quotes,
    )


class _Gateway:
    def __init__(
        self,
        *,
        positions: tuple[PositionSnapshot, ...] | None = None,
        working_orders: object = (),
        instructions: object = (),
        secdef_complete: bool = True,
        quote_observed_at: datetime | None = None,
    ) -> None:
        self.current_positions = positions or _positions()
        self.current_working_orders = working_orders
        self.current_instructions = instructions
        self.secdef_complete = secdef_complete
        self.quote_observed_at = quote_observed_at
        self.secdef_requests: list[tuple[OptionContractRef, ...]] = []
        self.quote_requests: list[tuple[OptionContractRef, ...]] = []
        self.connect_calls = 0
        self.disconnect_calls = 0

    def connect(self) -> None:  # pragma: no cover - a call fails the test first
        self.connect_calls += 1
        raise AssertionError("coordinator must not connect the broker")

    def disconnect(self) -> None:  # pragma: no cover - a call fails the test first
        self.disconnect_calls += 1
        raise AssertionError("coordinator must not disconnect the broker")

    def account_snapshot(self) -> object:
        return {"currency": "USD", "net_liquidation": Decimal("5000")}

    def positions(self) -> object:
        return self.current_positions

    def working_orders(self) -> object:
        return self.current_working_orders

    def unsubmitted_instructions(self) -> object:
        return self.current_instructions

    def option_contract_definitions(
        self,
        contracts: tuple[OptionContractRef, ...],
    ) -> object:
        requested = tuple(contracts)
        self.secdef_requests.append(requested)
        values = _secdefs(requested)
        return values if self.secdef_complete else values[:-1]

    def option_quote_batch(
        self,
        contracts: tuple[OptionContractRef, ...],
    ) -> OptionQuoteBatch:
        requested = tuple(contracts)
        self.quote_requests.append(requested)
        return _quote_batch(requested, observed_at=self.quote_observed_at)


class _CountingGenerator(ManagementCandidateGenerator):
    def __init__(self, manager: PositionManager) -> None:
        super().__init__(position_manager=manager)
        self.calls = 0

    def generate(self, *args, **kwargs):
        self.calls += 1
        return super().generate(*args, **kwargs)


class _ExplodingBuilder:
    def build(self, _contracts) -> object:
        raise RuntimeError("deliberate builder failure")


def _exit_contract() -> dict[str, object]:
    return {
        "thesis_invalidation": "close immediately when the thesis is invalid",
        "risk_stop": "close at the signed risk threshold",
        "profit_take": "close at the signed profit threshold",
        "time_stop": "close before the authoritative short-leg deadline",
        "maximum_holding_date": "2026-08-17",
        "bad_quote_action": "NO_TRADE until coherent quotes return",
        "expiration_calendar_hash": "c" * 64,
        "ex_dividend_calendar_hash": "d" * 64,
        "early_exercise_risk": "CLEAR",
        "short_leg_exit_deadline": "2026-08-19",
    }


def _cost_contract():
    return load_contract(COST_CONTRACT_PATH, as_of=NOW)


def _coordinator(
    gateway: _Gateway,
    *,
    exit_provider=lambda _snapshot: _exit_contract(),
    cost_provider=lambda _snapshot: _cost_contract(),
    builder=None,
    market_data_gate=None,
):
    manager = PositionManager()
    generator = _CountingGenerator(manager)
    coordinator = ProductionManagementCoordinator(
        gateway,
        exit_contract_provider=exit_provider,
        cost_contract_provider=cost_provider,
        position_manager=manager,
        generator=generator,
        snapshot_builder=builder,
        market_data_gate=market_data_gate,
        clock=lambda: NOW,
    )
    return coordinator, manager, generator


def _assert_no_authority(result, manager: PositionManager) -> None:
    assert result.approval_enabled is False
    assert result.review_only is True
    assert result.direct_order_submission is False
    projection = manager.read_model()
    assert projection["approval_enabled"] is False
    assert projection["review_only"] is True
    assert projection["direct_order_submission"] is False


def test_mixed_right_holdings_preview_runs_through_actual_coordinator_without_authority() -> None:
    from tests.options_copilot.test_holdings_close import HoldingsGateway

    gateway = HoldingsGateway()
    coordinator, manager, generator = _coordinator(gateway)
    result = coordinator.refresh()
    assert result.status == "NO_TRADE"
    assert len(result.candidates) == 1
    assert result.candidates[0].schema == "options_copilot.holdings_close_preview.v1"
    assert generator.calls == 1
    assert manager.read_model()["candidates"][0]["grouping_status"] == "NOT_INFERRED"
    assert coordinator.read_model()["result_hash"] == result.result_hash
    assert gateway.connect_calls == gateway.disconnect_calls == 0
    _assert_no_authority(result, manager)


def test_mixed_right_holdings_missing_quotes_stays_empty_in_actual_coordinator() -> None:
    from tests.options_copilot.test_holdings_close import HoldingsGateway

    gateway = HoldingsGateway()
    gateway.missing_bid = True
    coordinator, manager, _generator = _coordinator(gateway)
    result = coordinator.refresh()
    assert result.no_trade and result.candidates == ()
    _assert_no_authority(result, manager)


def test_complete_preview_uses_dynamic_conids_and_the_same_position_manager() -> None:
    middle_id = 998_877_001
    gateway = _Gateway(positions=_positions(middle_id))
    coordinator, manager, generator = _coordinator(gateway)

    result = coordinator.refresh()

    assert result.status == "CANDIDATES"
    assert generator.calls == 1
    expected_ids = [middle_id - 11, middle_id, middle_id + 17]
    assert [item.contract_id for item in gateway.secdef_requests[0]] == expected_ids
    assert [item.contract_id for item in gateway.quote_requests[0]] == expected_ids
    close = result.candidates[0]
    assert [item.contract_id for item in close.transition_proof.before_positions] == expected_ids
    assert [item.signed_quantity_delta for item in close.transition_proof.deltas] == [
        -1,
        2,
        -1,
    ]
    assert coordinator.position_manager is manager
    assert coordinator.generator.position_manager is manager
    assert manager.read_model()["result_hash"] == result.result_hash
    assert gateway.connect_calls == 0
    assert gateway.disconnect_calls == 0
    _assert_no_authority(result, manager)


def test_qqq_bull_call_vertical_uses_same_formal_snapshot_and_proof_path() -> None:
    gateway = _Gateway(positions=_qqq_vertical())
    coordinator, manager, generator = _coordinator(gateway)

    result = coordinator.refresh()

    assert result.status == "CANDIDATES"
    assert generator.calls == 1
    assert len(result.candidates) == 1
    close = result.candidates[0]
    assert close.symbol == "QQQ"
    assert close.structure == "BULL_CALL_DEBIT_VERTICAL"
    assert close.review_state == "HOLD_MONITOR"
    assert [item.contract_id for item in close.execution_legs] == [
        910_000_724,
        910_000_727,
    ]
    assert [item.signed_quantity_delta for item in close.execution_legs] == [-1, 1]
    assert close.executable_close_cashflow_usd == Decimal("157.00")
    assert close.all_in_close_cashflow_usd == Decimal("144.75")
    assert close.entry_net_cost_usd == Decimal("166.10")
    assert close.entry_net_credit_usd is None
    assert close.entry_max_loss_usd == Decimal("166.10")
    assert close.entry_max_profit_usd == Decimal("133.90")
    assert close.stop_review_cashflow_usd == Decimal("99.66")
    assert close.profit_review_cashflow_usd == Decimal("246.44")
    assert close.risk_stop_state == "CLEAR"
    assert close.profit_take_state == "CLEAR"
    assert close.time_stop_state == "CLEAR"
    assert close.thesis_invalidation_state == "NOT_EVALUATED"
    assert close.oldest_quote_age_seconds == Decimal("1")
    assert close.maximum_leg_skew_seconds == Decimal("0")
    assert close.transition_proof.verify_hash() is True
    assert close.review_only is True
    assert close.approval_enabled is False
    assert close.direct_order_submission is False
    _assert_no_authority(result, manager)


def test_qqq_mark_churn_still_builds_fresh_whole_combination_management() -> None:
    class MarkChurnGateway(_Gateway):
        def __init__(self) -> None:
            super().__init__(positions=_qqq_vertical())
            self.position_reads = 0

        def positions(self) -> object:
            self.position_reads += 1
            if self.position_reads < 3:
                return self.current_positions
            return tuple(
                replace(
                    position,
                    asof=NOW,
                    market_price=Decimal("2.40"),
                    market_value=Decimal("240"),
                    unrealized_pnl=Decimal("73.90"),
                    realized_pnl=Decimal("1.25"),
                )
                for position in self.current_positions
            )

    gateway = MarkChurnGateway()
    coordinator, manager, generator = _coordinator(gateway)

    result = coordinator.refresh()

    assert result.status == "CANDIDATES"
    assert "POSITIONS_MUTATED" not in result.reason_codes
    assert generator.calls == 1
    assert len(gateway.quote_requests) == 1
    close = result.candidates[0]
    assert close.stop_review_cashflow_usd == Decimal("99.66")
    assert close.profit_review_cashflow_usd == Decimal("246.44")
    assert close.time_stop_state == "CLEAR"
    _assert_no_authority(result, manager)


def test_position_discovery_uses_request_completion_time() -> None:
    current = [NOW]

    class CompletingGateway(_Gateway):
        def positions(self) -> object:
            current[0] = NOW + timedelta(seconds=1)
            return tuple(
                replace(item, asof=current[0]) for item in self.current_positions
            )

    gateway = CompletingGateway(positions=_qqq_vertical())
    manager = PositionManager()
    coordinator = ProductionManagementCoordinator(
        gateway,
        exit_contract_provider=lambda _snapshot: _exit_contract(),
        cost_contract_provider=lambda _snapshot: _cost_contract(),
        position_manager=manager,
        clock=lambda: current[0],
    )

    result = coordinator.refresh()

    assert result.status == "CANDIDATES"
    assert "POSITION_SNAPSHOT_STALE_OR_FUTURE" not in result.reason_codes


def test_qqq_vertical_requires_fresh_whole_combination_quotes() -> None:
    gateway = _Gateway(
        positions=_qqq_vertical(),
        quote_observed_at=NOW - timedelta(seconds=6),
    )
    coordinator, manager, generator = _coordinator(gateway)

    result = coordinator.refresh()

    assert result.status == "NO_TRADE"
    assert result.candidates == ()
    assert result.reason_codes == ("STALE_OR_FUTURE_QUOTE",)
    assert generator.calls == 0
    _assert_no_authority(result, manager)


def test_qqq_vertical_time_exit_is_a_review_state_never_order_authority() -> None:
    exit_contract = _exit_contract()
    exit_contract["maximum_holding_date"] = NOW.date().isoformat()
    coordinator, manager, generator = _coordinator(
        _Gateway(positions=_qqq_vertical()),
        exit_provider=lambda _snapshot: exit_contract,
    )

    result = coordinator.refresh()

    close = result.candidates[0]
    assert close.review_state == "TIME_EXIT_REVIEW"
    assert close.time_stop_state == "TRIGGERED"
    assert close.approval_enabled is False
    assert close.direct_order_submission is False
    _assert_no_authority(result, manager)


@pytest.mark.parametrize(
    ("right", "quantities", "average_costs", "structure"),
    (
        ("C", (1, -1), ("817.10", "651.00"), "BULL_CALL_DEBIT_VERTICAL"),
        ("C", (-1, 1), ("800", "600"), "BEAR_CALL_CREDIT_VERTICAL"),
        ("P", (-1, 1), ("500", "650"), "BEAR_PUT_DEBIT_VERTICAL"),
        ("P", (1, -1), ("400", "550"), "BULL_PUT_CREDIT_VERTICAL"),
    ),
)
def test_all_standard_defined_risk_vertical_directions_use_the_formal_manager(
    right: str,
    quantities: tuple[int, int],
    average_costs: tuple[str, str],
    structure: str,
) -> None:
    positions = _standard_vertical(
        right=right,
        quantities=quantities,
        average_costs=average_costs,
    )
    coordinator, manager, generator = _coordinator(_Gateway(positions=positions))

    result = coordinator.refresh()

    assert result.status == "CANDIDATES"
    close = result.candidates[0]
    assert close.symbol == "QQQ"
    assert close.structure == structure
    assert close.transition_proof.verify_hash() is True
    assert close.review_only is True
    assert close.approval_enabled is False
    assert close.direct_order_submission is False
    if "CREDIT" in structure:
        assert close.entry_net_credit_usd is not None
        assert close.entry_net_cost_usd is None
        assert close.entry_max_loss_usd is not None
    else:
        assert close.entry_net_cost_usd is not None
        assert close.entry_net_credit_usd is None
    _assert_no_authority(result, manager)


@pytest.mark.parametrize(
    ("quantities", "average_costs", "structure"),
    (
        ((2, -2), ("817.10", "651.00"), "BULL_CALL_DEBIT_VERTICAL"),
        ((-2, 2), ("800", "600"), "BEAR_CALL_CREDIT_VERTICAL"),
    ),
)
def test_partial_reduce_scales_review_thresholds_to_closed_combo_units(
    quantities: tuple[int, int],
    average_costs: tuple[str, str],
    structure: str,
) -> None:
    positions = _standard_vertical(
        right="C",
        quantities=quantities,
        average_costs=average_costs,
    )
    coordinator, manager, _generator = _coordinator(_Gateway(positions=positions))

    result = coordinator.refresh()

    assert result.status == "CANDIDATES"
    close = next(
        item for item in result.candidates
        if item.management_kind.value == "CLOSE_ALL"
    )
    reduce = next(
        item for item in result.candidates
        if item.management_kind.value == "REDUCE_RISK"
    )
    assert close.structure == reduce.structure == structure
    assert reduce.combo_quantity_before == 2
    assert reduce.combo_quantity_after == 1
    assert reduce.stop_review_cashflow_usd * 2 == close.stop_review_cashflow_usd
    assert reduce.profit_review_cashflow_usd * 2 == close.profit_review_cashflow_usd
    assert reduce.review_state == close.review_state == "HOLD_MONITOR"
    _assert_no_authority(result, manager)


def test_unknown_unsubmitted_instruction_state_fails_before_generator() -> None:
    gateway = _Gateway(instructions=None)
    coordinator, manager, generator = _coordinator(gateway)

    result = coordinator.refresh()

    assert result.status == "NO_TRADE"
    assert "UNSUBMITTED_INSTRUCTIONS_UNKNOWN" in result.reason_codes
    assert generator.calls == 0
    assert result.candidates == ()
    assert gateway.secdef_requests == []
    assert gateway.quote_requests == []
    _assert_no_authority(result, manager)


def test_market_data_gate_blocks_snapshot_before_secdef_or_quote_reads() -> None:
    gateway = _Gateway()
    coordinator, manager, generator = _coordinator(
        gateway,
        market_data_gate=lambda: ("PACING_BUDGET_EXHAUSTED:snapshot_quote",),
    )

    result = coordinator.refresh()

    assert result.reason_codes == ("PACING_BUDGET_EXHAUSTED:SNAPSHOT_QUOTE",)
    assert generator.calls == 0
    assert gateway.secdef_requests == []
    assert gateway.quote_requests == []
    _assert_no_authority(result, manager)


def test_missing_exit_contract_fails_before_cost_or_generator() -> None:
    gateway = _Gateway()
    cost_calls = 0

    def cost_provider(_snapshot):
        nonlocal cost_calls
        cost_calls += 1
        return _cost_contract()

    coordinator, manager, generator = _coordinator(
        gateway,
        exit_provider=lambda _snapshot: None,
        cost_provider=cost_provider,
    )

    result = coordinator.refresh()

    assert result.reason_codes == ("EXIT_CONTRACT_UNAVAILABLE",)
    assert cost_calls == 0
    assert generator.calls == 0
    _assert_no_authority(result, manager)


def test_missing_signed_cost_contract_fails_before_generator() -> None:
    coordinator, manager, generator = _coordinator(
        _Gateway(),
        cost_provider=lambda _snapshot: None,
    )

    result = coordinator.refresh()

    assert result.reason_codes == ("EXECUTION_COST_CONTRACT_UNAVAILABLE",)
    assert generator.calls == 0
    _assert_no_authority(result, manager)


def test_builder_exception_is_sanitized_and_published_no_trade() -> None:
    coordinator, manager, generator = _coordinator(
        _Gateway(),
        builder=_ExplodingBuilder(),
    )

    result = coordinator.refresh()

    assert result.reason_codes == ("BROKER_SNAPSHOT_BUILD_FAILED",)
    assert "deliberate" not in str(manager.read_model()).lower()
    assert generator.calls == 0
    _assert_no_authority(result, manager)


@pytest.mark.parametrize(
    ("gateway", "reason"),
    [
        (
            _Gateway(working_orders=({"order_id": 7},)),
            "WORKING_ORDERS_PRESENT",
        ),
        (
            _Gateway(secdef_complete=False),
            "SECDEF_INCOMPLETE",
        ),
        (
            _Gateway(quote_observed_at=NOW - timedelta(seconds=6)),
            "STALE_OR_FUTURE_QUOTE",
        ),
    ],
)
def test_incomplete_broker_authority_never_reaches_generator(
    gateway: _Gateway,
    reason: str,
) -> None:
    coordinator, manager, generator = _coordinator(gateway)

    result = coordinator.refresh()

    assert reason in result.reason_codes
    assert generator.calls == 0
    _assert_no_authority(result, manager)


@pytest.mark.parametrize(
    ("positions", "reason"),
    [
        (
            (replace(_positions()[0], symbol="SPY"), *_positions()[1:]),
            "MULTIPLE_OPEN_COMBINATIONS",
        ),
        (
            (replace(_positions()[0], expiration=None), *_positions()[1:]),
            "POSITION_FIELDS_INCOMPLETE",
        ),
        (
            (
                _positions()[0],
                replace(_positions()[1], contract_id=_positions()[0].contract_id),
                _positions()[2],
            ),
            "DUPLICATE_POSITION_CONID",
        ),
    ],
)
def test_position_discovery_rejects_mixed_underlying_missing_fields_and_duplicate_conids(
    positions: tuple[PositionSnapshot, ...],
    reason: str,
) -> None:
    coordinator, manager, generator = _coordinator(_Gateway(positions=positions))

    result = coordinator.refresh()

    assert result.reason_codes == (reason,)
    assert generator.calls == 0
    _assert_no_authority(result, manager)
