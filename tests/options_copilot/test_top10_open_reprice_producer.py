from __future__ import annotations

import ast
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from options_copilot.gateway.broker_snapshot import BrokerSnapshotBuilder
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
)
from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
)
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
)
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomicsResolver,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
)
from options_copilot.news.preselection_producer import (
    ProducerStatus,
    ResolvedStructure,
    Top10StructureResolution,
    Top10PreselectionProducer,
    _contracts_for_structures,
    _validate_snapshot,
)
from options_copilot.news.preselection import strategy_structure_hash
from options_copilot.news.preselection_store import (
    NewsPreselectionStore as SQLiteNewsPreselectionStore,
)


ET = ZoneInfo("America/New_York")
PREMARKET = datetime(2026, 8, 6, 9, 20, tzinfo=ET)
OPEN = datetime(2026, 8, 6, 9, 35, tzinfo=ET)
PREMARKET_SOURCE_ID = "external-premarket-20260806-0920"
PREMARKET_SOURCE_HASH = "1" * 64
OPEN_SOURCE_ID = "external-open-20260806-0935"
OPEN_SOURCE_HASH = "2" * 64


class FixedClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class FakeSessionGate:
    def __init__(self, eligible: bool | None = True) -> None:
        self.eligible = eligible
        self.calls = 0

    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        self.calls += 1
        assert scheduled_for.tzinfo == ET
        return self.eligible


class FakeStructureSource:
    def __init__(self, structures: tuple[ResolvedStructure, ...]) -> None:
        self.structures = structures
        self.calls = 0

    def resolve_top10(self, *, scheduled_for: datetime):
        self.calls += 1
        assert scheduled_for == PREMARKET
        return self.structures


class FakeEconomicsResolver(OpenRepriceEconomicsResolver):
    """Use the production resolver so the producer test proves full lineage."""


class FakeBroker:
    def __init__(self, clock: FixedClock) -> None:
        self.clock = clock
        self.call_log: list[str] = []
        self.positions_value: object = ()
        self.working_value: object = ()
        self.instructions_value: object = ()
        self.quote_age = Decimal("0.1")
        self.missing_quote_field: str | None = None
        self.partial = False
        self.cross_batch = False
        self.secdef_partial = False
        self.market_data_type: int | None = 1
        self.omit_batch_observed_at = False
        self.quote_calls = 0
        self.secdef_calls = 0

    def positions(self):
        self.call_log.append("positions")
        return self.positions_value

    def working_orders(self):
        self.call_log.append("working_orders")
        return self.working_value

    def unsubmitted_instructions(self):
        self.call_log.append("unsubmitted_instructions")
        return self.instructions_value

    def account_snapshot(self):
        self.call_log.append("account_snapshot")
        return {"connected": True, "currency": "USD"}

    def option_contract_definitions(self, contracts):
        self.call_log.append("option_contract_definitions")
        self.secdef_calls += 1
        values = tuple(
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
        return values[:-1] if self.secdef_partial else values

    def option_quote_batch(self, contracts):
        self.call_log.append("option_quote_batch")
        self.quote_calls += 1
        now = self.clock.value
        age = timedelta(seconds=float(self.quote_age))
        requested_at = now - age - timedelta(milliseconds=100)
        batch_id = "ibkr-batch"
        quotes = []
        for index, item in enumerate(contracts):
            values = {
                "bid": Decimal("1.00"),
                "ask": Decimal("1.10"),
                "implied_volatility": Decimal("0.25"),
                "delta": Decimal("0.40"),
                "gamma": Decimal("0.03"),
                "theta": Decimal("-0.02"),
                "vega": Decimal("0.11"),
                "volume": 100,
                "open_interest": 500,
            }
            if self.missing_quote_field is not None and index == 0:
                values[self.missing_quote_field] = None
            quotes.append(
                BatchedOptionQuote(
                    contract_id=item.contract_id,
                    batch_id=("other-batch" if self.cross_batch and index == 0 else batch_id),
                    request_id=f"request-{item.contract_id}",
                    requested_at=requested_at,
                    observed_at=now - age,
                    completed_at=now,
                    source="IBKR_READONLY",
                    exchange_time=now - age,
                    market_data_type=self.market_data_type,
                    **values,
                )
            )
        if self.partial:
            quotes = quotes[:-1]
        return OptionQuoteBatch(
            batch_id=batch_id,
            status=QuoteBatchStatus.COMPLETE,
            requested_at=requested_at,
            completed_at=now,
            source="IBKR_READONLY",
            quotes=tuple(quotes),
            observed_at=None if self.omit_batch_observed_at else now - age,
        )


class FakeStore:
    def __init__(self) -> None:
        self.premarket = None
        self.open_batch = None
        self.calls: list[str] = []
        self.premarket_writes: list[tuple[ConditionalOptionPreselection, ...]] = []
        self.open_writes: list[tuple[ConditionalOptionPreselection, ...]] = []
        self.open_blockers: tuple[str, ...] = ()

    def latest_premarket(self):
        self.calls.append("latest_premarket")
        return self.premarket

    def append_premarket_run(
        self,
        run_id,
        candidates,
        *,
        now=None,
        source_batch_purpose=None,
        source_batch_id=None,
        source_batch_hash=None,
    ):
        self.calls.append("append_premarket_run")
        checked = tuple(candidates)
        self.premarket_writes.append(checked)
        rows = tuple(
            SimpleNamespace(
                research_rank=index,
                preselection_id=item.preselection_id,
                strategy_hash=item.strategy_hash,
                candidate=item,
                row_hash=f"row-{index}",
            )
            for index, item in enumerate(checked, start=1)
        )
        self.premarket = SimpleNamespace(
            run_id=run_id,
            created_at=now,
            available_count=len(rows),
            rows=rows,
            head_hash="a" * 64,
            source_batch_purpose=source_batch_purpose,
            source_batch_id=source_batch_id,
            source_batch_hash=source_batch_hash,
        )
        return self.premarket

    def latest_open_batch(self):
        self.calls.append("latest_open_batch")
        return self.open_batch

    def append_open_batch(
        self,
        parent_head_hash,
        candidates,
        *,
        batch_id,
        scheduled_for,
        observed_at,
        batch_blockers=(),
        source_batch_purpose=None,
        source_batch_id=None,
        source_batch_hash=None,
    ):
        self.calls.append("append_open_batch")
        assert scheduled_for == OPEN
        assert observed_at.tzinfo is not None
        checked = tuple(candidates)
        self.open_writes.append(checked)
        self.open_batch = SimpleNamespace(
            batch_id=batch_id,
            parent_head_hash=parent_head_hash,
            candidates=checked,
            rows=checked,
            blockers=self.open_blockers or tuple(batch_blockers),
            action_pool_eligible=not (self.open_blockers or tuple(batch_blockers)),
            quote_batch_id="ibkr-batch",
            source_batch_purpose=source_batch_purpose,
            source_batch_id=source_batch_id,
            source_batch_hash=source_batch_hash,
        )
        return self.open_batch


def _candidate(index: int) -> ConditionalOptionPreselection:
    expiry = date(2026, 9, 18)
    con_id = 1000 + index
    leg = ConditionalOptionLeg(
        underlying=f"T{index}",
        con_id=con_id,
        expiry=expiry,
        strike=Decimal(100 + index),
        right=OptionRight.CALL,
        side=OptionLegSide.BUY,
        ratio=1,
        quantity=1,
        bid=None,
        ask=None,
        quote_asof=None,
        quote_batch_id=None,
        implied_volatility=None,
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
        volume=None,
        open_interest=None,
        dte=None,
        local_symbol=f"T{index}  260918C00100000",
        trading_class=f"T{index}",
        multiplier=100,
        exchange="SMART",
    )
    strategy_type = "LONG_CALL"
    strategy_hash = strategy_structure_hash(f"T{index}", strategy_type, (leg,))
    scenarios = (
        PreselectionTerminalScenario(
            terminal_underlying_price=Decimal(100 + index - 1),
            probability=Decimal("0.10"),
        ),
        PreselectionTerminalScenario(
            terminal_underlying_price=Decimal(100 + index + 3),
            probability=Decimal("0.90"),
        ),
    )
    scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=f"pre-{index}",
        strategy_hash=strategy_hash,
        scenario_asof=PREMARKET,
        scenarios=tuple(
            TrustedTerminalScenario(
                terminal_underlying_price=item.terminal_underlying_price,
                probability=item.probability,
            )
            for item in scenarios
        ),
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )
    return ConditionalOptionPreselection(
        preselection_id=f"pre-{index}",
        underlying=f"T{index}",
        strategy_type=strategy_type,
        phase=PreselectionPhase.PRE_MARKET,
        legs=(leg,),
        risk_defined=True,
        maximum_loss_usd=Decimal("110"),
        estimated_cost_usd=Decimal("110"),
        cost_after_ev_usd=Decimal("5"),
        entry_condition="Fresh quote remains executable.",
        invalidation_condition="Quote becomes stale.",
        profit_target_condition="Research target reached.",
        stop_loss_condition="Research invalidation reached.",
        evidence_ids=(f"evidence-{index}",),
        evidence_hashes=(f"{index:064x}",),
        strategy_hash=strategy_hash,
        research_summary="Independent supporting-only fixture.",
        terminal_scenarios=scenarios,
        scenario_asof=PREMARKET,
        scenario_hash=scenario_set.scenario_hash,
        execution_cost_contract_version=EXECUTION_COST_VERSION,
        execution_cost_contract_hash=EXECUTION_COST_HASH,
        risk_policy_version=INITIAL_POLICY_VERSION,
        risk_policy_hash=INITIAL_POLICY_HASH,
    )


def _structures(count: int) -> tuple[ResolvedStructure, ...]:
    return tuple(ResolvedStructure(_candidate(index)) for index in range(1, count + 1))


def _producer(
    *,
    now: datetime = PREMARKET,
    count: int = 2,
    store: FakeStore | None = None,
    gate: FakeSessionGate | None = None,
    premarket_account_only: bool = False,
    require_exact_top10: bool = False,
    economics_resolver: object | None = ...,
    source_batch_purpose: str | None = None,
    source_batch_id: str | None = None,
    source_batch_hash: str | None = None,
    structure_source: object | None = None,
):
    clock = FixedClock(now)
    broker = FakeBroker(clock)
    source = structure_source or FakeStructureSource(_structures(count))
    checked_store = store or FakeStore()
    checked_gate = gate or FakeSessionGate()
    checked_resolver = (
        FakeEconomicsResolver() if economics_resolver is ... else economics_resolver
    )
    producer = Top10PreselectionProducer(
        account_state_reader=broker,
        structure_source=source,
        snapshot_provider=BrokerSnapshotBuilder(broker, clock=clock),
        store=checked_store,
        clock=clock,
        session_gate=checked_gate,
        premarket_account_only=premarket_account_only,
        require_exact_top10=require_exact_top10,
        open_economics_resolver=checked_resolver,
        strategy_nav_reader=(
            (lambda _snapshot: Decimal("10000"))
            if checked_resolver is not None
            else None
        ),
        source_batch_purpose=source_batch_purpose,
        source_batch_id=source_batch_id,
        source_batch_hash=source_batch_hash,
    )
    return producer, broker, source, checked_store, checked_gate, clock


def test_0920_account_only_mode_freezes_exactly_ten_without_fake_quotes() -> None:
    producer, broker, _, store, _, _ = _producer(
        count=10,
        premarket_account_only=True,
        require_exact_top10=True,
    )

    result = producer.tick()

    assert result.status is ProducerStatus.PREMARKET_FROZEN
    assert result.written_count == 10
    assert result.quote_batch_id is None
    assert broker.secdef_calls == broker.quote_calls == 0
    assert all(
        leg.quote_batch_id is None and leg.bid is None and leg.ask is None
        for candidate in store.premarket_writes[0]
        for leg in candidate.legs
    )


def test_0920_account_only_parent_persists_near_deadline_without_snapshot() -> None:
    producer, broker, source, store, _, clock = _producer(
        count=10,
        premarket_account_only=True,
        require_exact_top10=True,
    )

    class NearDeadlineStructureSource:
        def resolve_top10(self, *, scheduled_for: datetime):
            structures = source.resolve_top10(scheduled_for=scheduled_for)
            clock.value = PREMARKET + timedelta(seconds=59)
            return structures

    class SnapshotMustNotRun:
        def build(self, contracts):  # pragma: no cover - safety tripwire
            pytest.fail(f"09:20 account-only freeze requested {len(contracts)} quotes")

    producer._structure_source = NearDeadlineStructureSource()
    producer._snapshot_provider = SnapshotMustNotRun()

    result = producer.tick(scheduled_for=PREMARKET)

    assert result.status is ProducerStatus.PREMARKET_FROZEN
    assert result.written_count == 10
    assert result.quote_batch_id is None
    assert store.premarket.created_at == PREMARKET + timedelta(seconds=59)
    assert broker.secdef_calls == broker.quote_calls == 0
    assert all(
        leg.quote_batch_id is None and leg.bid is None and leg.ask is None
        for candidate in store.premarket_writes[0]
        for leg in candidate.legs
    )


def test_each_slot_persists_its_exact_source_batch_pair() -> None:
    morning, _, _, store, _, _ = _producer(
        count=2,
        source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
        source_batch_id=PREMARKET_SOURCE_ID,
        source_batch_hash=PREMARKET_SOURCE_HASH,
    )
    assert morning.tick().status is ProducerStatus.PREMARKET_FROZEN
    assert (
        store.premarket.source_batch_purpose,
        store.premarket.source_batch_id,
        store.premarket.source_batch_hash,
    ) == (
        PREMARKET_ACCOUNT_PURPOSE,
        PREMARKET_SOURCE_ID,
        PREMARKET_SOURCE_HASH,
    )

    opener, _, _, _, _, _ = _producer(
        now=OPEN,
        store=store,
        source_batch_purpose=OPEN_REPRICE_PURPOSE,
        source_batch_id=OPEN_SOURCE_ID,
        source_batch_hash=OPEN_SOURCE_HASH,
    )
    assert opener.tick().status is ProducerStatus.OPEN_REPRICED
    assert (
        store.open_batch.source_batch_purpose,
        store.open_batch.source_batch_id,
        store.open_batch.source_batch_hash,
    ) == (OPEN_REPRICE_PURPOSE, OPEN_SOURCE_ID, OPEN_SOURCE_HASH)
    assert store.open_batch.source_batch_id != store.open_batch.quote_batch_id


@pytest.mark.parametrize(
    ("source_batch_purpose", "source_batch_id", "source_batch_hash"),
    (
        (None, PREMARKET_SOURCE_ID, PREMARKET_SOURCE_HASH),
        (PREMARKET_ACCOUNT_PURPOSE, PREMARKET_SOURCE_ID, None),
        (PREMARKET_ACCOUNT_PURPOSE, None, PREMARKET_SOURCE_HASH),
        ("UNKNOWN", PREMARKET_SOURCE_ID, PREMARKET_SOURCE_HASH),
        (PREMARKET_ACCOUNT_PURPOSE, " external-batch", PREMARKET_SOURCE_HASH),
        (PREMARKET_ACCOUNT_PURPOSE, PREMARKET_SOURCE_ID, "A" * 64),
        (PREMARKET_ACCOUNT_PURPOSE, PREMARKET_SOURCE_ID, "not-a-sha256"),
    ),
)
def test_invalid_or_partial_source_binding_fails_before_broker_or_write(
    source_batch_purpose: str | None,
    source_batch_id: str | None,
    source_batch_hash: str | None,
) -> None:
    producer, broker, source, store, gate, _ = _producer(
        source_batch_purpose=source_batch_purpose,
        source_batch_id=source_batch_id,
        source_batch_hash=source_batch_hash,
    )

    result = producer.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("SOURCE_BATCH_BINDING_INVALID",)
    assert broker.call_log == []
    assert source.calls == gate.calls == 0
    assert store.premarket_writes == []


def test_source_purpose_must_match_the_exact_slot() -> None:
    morning, broker, source, store, gate, _ = _producer(
        source_batch_purpose=OPEN_REPRICE_PURPOSE,
        source_batch_id=PREMARKET_SOURCE_ID,
        source_batch_hash=PREMARKET_SOURCE_HASH,
    )

    result = morning.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("SOURCE_BATCH_PURPOSE_MISMATCH",)
    assert broker.call_log == []
    assert source.calls == gate.calls == 0
    assert store.premarket_writes == []


def test_duplicate_slots_fail_closed_when_source_pair_differs() -> None:
    morning, _, _, store, _, _ = _producer(
        count=2,
        source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
        source_batch_id=PREMARKET_SOURCE_ID,
        source_batch_hash=PREMARKET_SOURCE_HASH,
    )
    assert morning.tick().status is ProducerStatus.PREMARKET_FROZEN

    wrong_morning, _, wrong_source, _, _, _ = _producer(
        count=2,
        store=store,
        source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
        source_batch_id="external-premarket-replacement",
        source_batch_hash="3" * 64,
    )
    wrong_morning_result = wrong_morning.tick()
    assert wrong_morning_result.status is ProducerStatus.NO_TRADE
    assert wrong_morning_result.reason_codes == ("SOURCE_BATCH_BINDING_MISMATCH",)
    assert wrong_source.calls == 0
    assert len(store.premarket_writes) == 1

    opener, _, _, _, _, _ = _producer(
        now=OPEN,
        store=store,
        source_batch_purpose=OPEN_REPRICE_PURPOSE,
        source_batch_id=OPEN_SOURCE_ID,
        source_batch_hash=OPEN_SOURCE_HASH,
    )
    assert opener.tick().status is ProducerStatus.OPEN_REPRICED

    wrong_open, wrong_broker, wrong_open_source, _, _, _ = _producer(
        now=OPEN,
        store=store,
        source_batch_purpose=OPEN_REPRICE_PURPOSE,
        source_batch_id="external-open-replacement",
        source_batch_hash="4" * 64,
    )
    wrong_open_result = wrong_open.tick()
    assert wrong_open_result.status is ProducerStatus.NO_TRADE
    assert wrong_open_result.reason_codes == ("SOURCE_BATCH_BINDING_MISMATCH",)
    assert wrong_broker.quote_calls == wrong_open_source.calls == 0
    assert len(store.open_writes) == 1


def test_premarket_append_race_rechecks_exact_source_pair() -> None:
    class RacingStore(FakeStore):
        def append_premarket_run(self, run_id, candidates, **kwargs):
            kwargs["source_batch_purpose"] = PREMARKET_ACCOUNT_PURPOSE
            kwargs["source_batch_id"] = "competing-premarket-batch"
            kwargs["source_batch_hash"] = "5" * 64
            super().append_premarket_run(run_id, candidates, **kwargs)
            raise RuntimeError("lost append race")

    store = RacingStore()
    producer, _, _, _, _, _ = _producer(
        count=2,
        store=store,
        source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
        source_batch_id=PREMARKET_SOURCE_ID,
        source_batch_hash=PREMARKET_SOURCE_HASH,
    )

    result = producer.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("SOURCE_BATCH_BINDING_MISMATCH",)
    assert result.reason_codes != ("SLOT_ALREADY_RECORDED",)


def test_open_append_race_rechecks_exact_source_pair() -> None:
    class RacingStore(FakeStore):
        def append_open_batch(self, parent_head_hash, candidates, **kwargs):
            kwargs["source_batch_purpose"] = OPEN_REPRICE_PURPOSE
            kwargs["source_batch_id"] = "competing-open-batch"
            kwargs["source_batch_hash"] = "6" * 64
            super().append_open_batch(parent_head_hash, candidates, **kwargs)
            raise RuntimeError("lost append race")

    store = RacingStore()
    morning, _, _, _, _, _ = _producer(
        count=2,
        store=store,
        source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
        source_batch_id=PREMARKET_SOURCE_ID,
        source_batch_hash=PREMARKET_SOURCE_HASH,
    )
    assert morning.tick().status is ProducerStatus.PREMARKET_FROZEN
    opener, _, _, _, _, _ = _producer(
        now=OPEN,
        store=store,
        source_batch_purpose=OPEN_REPRICE_PURPOSE,
        source_batch_id=OPEN_SOURCE_ID,
        source_batch_hash=OPEN_SOURCE_HASH,
    )

    result = opener.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("SOURCE_BATCH_BINDING_MISMATCH",)
    assert result.reason_codes != ("SLOT_ALREADY_RECORDED",)


def test_0920_exact_top10_mode_rejects_shortfall_without_filling() -> None:
    producer, broker, _, store, _, _ = _producer(
        count=9,
        premarket_account_only=True,
        require_exact_top10=True,
    )

    result = producer.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("TOP10_ELIGIBLE_COUNT_SHORTFALL",)
    assert broker.secdef_calls == broker.quote_calls == 0
    assert store.premarket_writes == []


def test_exact_top10_shortfall_preserves_source_failure_and_missing_symbols() -> None:
    class IncompleteSource:
        def resolve_top10(self, *, scheduled_for: datetime):
            assert scheduled_for == PREMARKET
            return Top10StructureResolution(
                (),
                reason_codes=(
                    "UNDERLYING_QUOTE_BATCH_FAILED",
                    "UNDERLYING_QUOTE_BATCH_INCOMPLETE",
                ),
                missing_symbols=("AAPL", "MSFT"),
            )

    producer, broker, _, store, _, _ = _producer(
        premarket_account_only=True,
        require_exact_top10=True,
        structure_source=IncompleteSource(),
    )

    result = producer.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == (
        "TOP10_ELIGIBLE_COUNT_SHORTFALL",
        "UNDERLYING_QUOTE_BATCH_FAILED",
        "UNDERLYING_QUOTE_BATCH_INCOMPLETE",
    )
    assert result.missing_symbols == ("AAPL", "MSFT")
    assert result.as_dict()["missing_symbols"] == ["AAPL", "MSFT"]
    assert broker.secdef_calls == broker.quote_calls == 0
    assert store.premarket_writes == []


def test_positions_are_first_and_derivative_position_short_circuits_everything() -> None:
    producer, broker, source, store, gate, _ = _producer()
    broker.positions_value = (
        {
            "contract_id": 7,
            "symbol": "GLD",
            "security_type": "OPT",
            "quantity": Decimal("1"),
        },
    )

    result = producer.tick()

    assert result.status is ProducerStatus.POSITION_MANAGEMENT_ONLY
    assert broker.call_log == ["positions"]
    assert broker.secdef_calls == broker.quote_calls == source.calls == gate.calls == 0
    assert store.calls == []


@pytest.mark.parametrize(
    "positions",
    [
        None,
        ({"contract_id": 1, "symbol": "SPY", "security_type": "OPT"},),
        (
            {"contract_id": 1, "symbol": "SPY", "security_type": "OPT", "quantity": 0},
            {"contract_id": 1, "symbol": "SPY", "security_type": "OPT", "quantity": 0},
        ),
        ({"contract_id": 1, "symbol": "SPY", "security_type": "OPT", "quantity": float("nan")},),
        ({"contract_id": 1, "symbol": "SPY", "security_type": "OPT", "quantity": 0, "average_cost": Decimal("NaN")},),
    ],
)
def test_invalid_positions_fail_before_chain(positions: object) -> None:
    producer, broker, source, store, _, _ = _producer()
    broker.positions_value = positions
    result = producer.tick()
    assert result.status is ProducerStatus.NO_TRADE
    assert broker.call_log == ["positions"]
    assert broker.quote_calls == broker.secdef_calls == source.calls == 0
    assert store.calls == []


@pytest.mark.parametrize("field,value", [("working_value", None), ("working_value", ({"id": 1},)), ("instructions_value", None), ("instructions_value", ({"id": 1},))])
def test_unknown_or_present_working_state_blocks_before_source(field: str, value: object) -> None:
    producer, broker, source, store, gate, _ = _producer()
    setattr(broker, field, value)
    result = producer.tick()
    assert result.status is ProducerStatus.NO_TRADE
    assert broker.call_log[0] == "positions"
    assert broker.quote_calls == broker.secdef_calls == source.calls == gate.calls == 0
    assert store.calls == []


@pytest.mark.parametrize("count,expected", [(10, 10), (11, 10)])
def test_0920_freezes_exactly_or_at_most_ten(count: int, expected: int) -> None:
    producer, broker, source, store, _, _ = _producer(count=count)
    result = producer.tick()
    assert result.status is ProducerStatus.PREMARKET_FROZEN
    assert result.written_count == expected
    assert len(store.premarket_writes[0]) == expected
    assert broker.call_log[0] == "positions"
    assert broker.quote_calls == 1
    assert source.calls == 1


def test_0935_reprices_exact_parent_set_once_without_source_fallback() -> None:
    morning, _, _, store, _, _ = _producer(count=3)
    assert morning.tick().status is ProducerStatus.PREMARKET_FROZEN
    parent = store.premarket
    before = tuple(row.candidate for row in parent.rows)

    opener, broker, source, _, _, _ = _producer(now=OPEN, count=9, store=store)
    result = opener.tick()

    assert result.status is ProducerStatus.OPEN_REPRICED
    assert result.written_count == 3
    assert broker.quote_calls == 1
    assert source.calls == 0
    assert len(store.open_writes) == 1
    for old, new in zip(before, store.open_writes[0], strict=True):
        assert new.preselection_id == old.preselection_id
        assert new.strategy_hash == old.strategy_hash
        assert new.phase is PreselectionPhase.OPEN_REPRICED
        assert tuple(leg.contract_ref for leg in new.legs) == tuple(
            leg.contract_ref for leg in old.legs
        )
        assert {leg.quote_batch_id for leg in new.legs} == {"ibkr-batch"}


def test_0935_returns_no_trade_when_persisted_batch_fails_final_freshness() -> None:
    morning, _, _, store, _, _ = _producer(count=2)
    assert morning.tick().status is ProducerStatus.PREMARKET_FROZEN
    store.open_blockers = ("QUOTE_STALE",)

    opener, _, _, _, _, _ = _producer(now=OPEN, store=store)
    result = opener.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("QUOTE_STALE",)
    assert result.written_count == 2
    assert len(store.open_writes) == 1

    duplicate, _, _, _, _, _ = _producer(now=OPEN, store=store)
    duplicate_result = duplicate.tick()
    assert duplicate_result.status is ProducerStatus.NO_TRADE
    assert duplicate_result.reason_codes == ("QUOTE_STALE",)
    assert duplicate_result.written_count == 0


def test_0935_without_economics_resolver_records_quotes_but_stays_no_trade() -> None:
    morning, _, _, store, _, _ = _producer(count=2)
    assert morning.tick().status is ProducerStatus.PREMARKET_FROZEN

    opener, _, _, _, _, _ = _producer(
        now=OPEN,
        store=store,
        economics_resolver=None,
    )
    result = opener.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("OPEN_ECONOMICS_RECALCULATION_UNAVAILABLE",)
    assert store.open_batch.action_pool_eligible is False
    assert all(
        candidate.maximum_loss_usd is None
        and candidate.estimated_cost_usd is None
        and candidate.cost_after_ev_usd is None
        for candidate in store.open_batch.candidates
    )
    assert result.written_count == 2
    assert len(store.open_writes) == 1


def test_real_sqlite_store_roundtrip_uses_atomic_open_batch_interface(tmp_path: Path) -> None:
    clock = FixedClock(PREMARKET)
    broker = FakeBroker(clock)
    source = FakeStructureSource(_structures(3))
    store_path = tmp_path / "top10.sqlite3"
    with SQLiteNewsPreselectionStore(store_path, clock=clock) as store:
        morning = Top10PreselectionProducer(
            account_state_reader=broker,
            structure_source=source,
            snapshot_provider=BrokerSnapshotBuilder(broker, clock=clock),
            store=store,
            clock=clock,
            session_gate=FakeSessionGate(),
            open_economics_resolver=FakeEconomicsResolver(),
            strategy_nav_reader=lambda _snapshot: Decimal("10000"),
        )
        frozen = morning.tick()
        assert frozen.status is ProducerStatus.PREMARKET_FROZEN
        assert store.latest_premarket().available_count == 3

    clock.value = OPEN
    with SQLiteNewsPreselectionStore(store_path, clock=clock) as store:
        opener = Top10PreselectionProducer(
            account_state_reader=broker,
            structure_source=source,
            snapshot_provider=BrokerSnapshotBuilder(broker, clock=clock),
            store=store,
            clock=clock,
            session_gate=FakeSessionGate(),
            open_economics_resolver=FakeEconomicsResolver(),
            strategy_nav_reader=lambda _snapshot: Decimal("10000"),
        )
        repriced = opener.tick()
        stored = store.latest_open_batch()

        assert repriced.status is ProducerStatus.OPEN_REPRICED
        assert repriced.written_count == 3
        assert repriced.quote_batch_id == "ibkr-batch"
        assert stored is not None
        assert stored.parent_head_hash == frozen.parent_head_hash
        assert stored.quote_batch_id == "ibkr-batch"
        assert len(stored.rows) == 3
        assert source.calls == 1  # 09:20 only; no 09:35 fallback.


@pytest.mark.parametrize(
    "mutation",
    ["bid", "ask", "implied_volatility", "delta", "gamma", "theta", "vega", "volume", "open_interest", "stale", "cross_batch", "partial", "secdef_partial"],
)
def test_any_incomplete_stale_crossbatch_or_partial_snapshot_writes_nothing(mutation: str) -> None:
    producer, broker, _, store, _, _ = _producer(count=2)
    if mutation in {"bid", "ask", "implied_volatility", "delta", "gamma", "theta", "vega", "volume", "open_interest"}:
        broker.missing_quote_field = mutation
    elif mutation == "stale":
        broker.quote_age = Decimal("5.001")
    else:
        setattr(broker, mutation, True)
    result = producer.tick()
    assert result.status is ProducerStatus.NO_TRADE
    assert store.premarket_writes == []


def test_missing_structure_identity_blocks_before_secdef_or_quote() -> None:
    producer, broker, source, store, _, _ = _producer(count=1)
    candidate = source.structures[0].candidate
    broken_leg = replace(candidate.legs[0], local_symbol=None)
    source.structures = (ResolvedStructure(replace(candidate, legs=(broken_leg,))),)

    result = producer.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert broker.secdef_calls == broker.quote_calls == 0
    assert store.premarket_writes == []


def test_quote_age_exactly_five_seconds_is_accepted() -> None:
    producer, broker, _, store, _, _ = _producer(count=1)
    broker.quote_age = Decimal("5")
    assert producer.tick().status is ProducerStatus.PREMARKET_FROZEN
    assert len(store.premarket_writes) == 1


@pytest.mark.parametrize("market_data_type", [None, 2, 3, 4])
def test_non_live_or_unknown_market_data_type_writes_nothing(
    market_data_type: int | None,
) -> None:
    producer, broker, _, store, _, _ = _producer(count=1)
    broker.market_data_type = market_data_type

    result = producer.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert "QUOTE_MARKET_DATA_NOT_LIVE" in result.reason_codes
    assert store.premarket_writes == []


def test_missing_batch_observation_writes_nothing() -> None:
    producer, broker, _, store, _, _ = _producer(count=1)
    broker.omit_batch_observed_at = True

    result = producer.tick()

    assert result.status is ProducerStatus.NO_TRADE
    assert "QUOTE_BATCH_TIMESTAMP_INVALID" in result.reason_codes
    assert store.premarket_writes == []


def test_snapshot_hash_and_verifier_are_mandatory() -> None:
    producer, _, source, _, _, clock = _producer(count=1)
    contracts, reason = _contracts_for_structures(source.structures)
    assert reason is None
    snapshot = producer._snapshot_provider.build(contracts)

    invalid_hash = replace(snapshot, snapshot_hash="")
    assert _validate_snapshot(
        invalid_hash,
        contracts=contracts,
        verified_at=clock.value,
    ) == (ProducerStatus.NO_TRADE, "BROKER_SNAPSHOT_HASH_INVALID")

    unsigned = SimpleNamespace(
        **{
            name: getattr(snapshot, name)
            for name in snapshot.__dataclass_fields__
        },
        complete=True,
    )
    assert _validate_snapshot(
        unsigned,
        contracts=contracts,
        verified_at=clock.value,
    ) == (ProducerStatus.NO_TRADE, "BROKER_SNAPSHOT_HASH_INVALID")


def test_duplicate_slots_are_idempotent_across_restart() -> None:
    first, broker, source, store, _, _ = _producer(count=2)
    assert first.tick().written_count == 2
    quote_calls = broker.quote_calls
    source_calls = source.calls
    duplicate = first.tick()
    assert duplicate.status is ProducerStatus.PREMARKET_FROZEN
    assert duplicate.written_count == 0
    assert broker.quote_calls == quote_calls
    assert source.calls == source_calls
    assert len(store.premarket_writes) == 1

    opened, open_broker, open_source, _, _, _ = _producer(now=OPEN, store=store)
    assert opened.tick().written_count == 2
    restarted, restart_broker, restart_source, _, _, _ = _producer(now=OPEN, store=store)
    duplicate_open = restarted.tick()
    assert duplicate_open.status is ProducerStatus.OPEN_REPRICED
    assert duplicate_open.written_count == 0
    assert duplicate_open.quote_batch_id == "ibkr-batch"
    assert restart_broker.quote_calls == restart_source.calls == 0
    assert open_broker.quote_calls == 1
    assert open_source.calls == 0
    assert len(store.open_writes) == 1


def test_0935_without_exact_parent_is_no_trade_and_zero_source_or_quote() -> None:
    producer, broker, source, store, _, _ = _producer(now=OPEN)
    result = producer.tick()
    assert result.status is ProducerStatus.NO_TRADE
    assert "TODAY_0920_PARENT_MISSING" in result.reason_codes
    assert broker.quote_calls == broker.secdef_calls == source.calls == 0
    assert store.open_writes == []


def test_nonexact_closed_and_missing_session_gate_fail_closed() -> None:
    late, broker, source, store, _, _ = _producer(now=PREMARKET + timedelta(seconds=1))
    assert late.tick().status is ProducerStatus.NO_TRADE
    assert broker.call_log == []
    assert source.calls == 0 and store.calls == []

    closed_gate = FakeSessionGate(False)
    closed, closed_broker, closed_source, closed_store, _, _ = _producer(gate=closed_gate)
    assert closed.tick().status is ProducerStatus.NO_TRADE
    assert closed_broker.quote_calls == closed_source.calls == 0
    assert closed_store.calls == []

    missing, missing_broker, missing_source, missing_store, _, clock = _producer()
    missing = Top10PreselectionProducer(
        account_state_reader=missing_broker,
        structure_source=missing_source,
        snapshot_provider=BrokerSnapshotBuilder(missing_broker, clock=clock),
        store=missing_store,
        clock=clock,
        session_gate=None,
    )
    result = missing.tick()
    assert "SESSION_GATE_UNAVAILABLE" in result.reason_codes
    assert missing_broker.quote_calls == missing_source.calls == 0
    assert missing_store.calls == []


def test_scheduler_slot_uses_real_clock_for_broker_freshness_within_minute() -> None:
    actual = PREMARKET + timedelta(seconds=30)
    producer, broker, _, store, _, _ = _producer(now=actual, count=1)

    result = producer.tick(scheduled_for=PREMARKET)

    assert result.status is ProducerStatus.PREMARKET_FROZEN
    assert result.observed_at == actual
    assert result.scheduled_for == PREMARKET
    assert broker.call_log[0] == "positions"
    assert len(store.premarket_writes) == 1
    assert store.premarket.created_at == actual


def test_scheduler_slot_never_runs_outside_its_one_minute_window() -> None:
    producer, broker, source, store, _, _ = _producer(
        now=PREMARKET + timedelta(minutes=1),
    )

    result = producer.tick(scheduled_for=PREMARKET)

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("SLOT_WINDOW_EXPIRED_OR_NOT_STARTED",)
    assert broker.call_log == []
    assert source.calls == 0
    assert store.calls == []


@pytest.mark.parametrize("phase", ("premarket", "open"))
def test_snapshot_finishing_after_slot_window_writes_nothing(phase: str) -> None:
    store = FakeStore()
    if phase == "open":
        morning, _, _, _, _, _ = _producer(store=store, count=1)
        assert morning.tick().status is ProducerStatus.PREMARKET_FROZEN
        producer, _, _, _, _, clock = _producer(now=OPEN, store=store, count=1)
        scheduled = OPEN
    else:
        producer, _, _, _, _, clock = _producer(store=store, count=1)
        scheduled = PREMARKET
    wrapped = producer._snapshot_provider

    class SnapshotThatFinishesLate:
        def build(self, contracts):
            snapshot = wrapped.build(contracts)
            clock.value = scheduled + timedelta(minutes=1)
            return snapshot

    producer._snapshot_provider = SnapshotThatFinishesLate()
    result = producer.tick(scheduled_for=scheduled)

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes in {
        ("QUOTE_STALE_OR_FUTURE",),
        ("SLOT_WINDOW_EXPIRED_DURING_SNAPSHOT",),
    }
    if phase == "open":
        assert store.open_writes == []
    else:
        assert store.premarket_writes == []


def test_producer_has_no_forbidden_imports_or_mutating_authority_methods() -> None:
    path = Path("options_copilot/news/preselection_producer.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name.lower() for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append((node.module or "").lower())
    assert not any(
        forbidden in name
        for name in imported
        for forbidden in ("ranking", "approval", "bridge")
    )
    methods = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert methods.isdisjoint(
        {"approve", "authorize", "create_instruction", "submit_order", "place_order"}
    )
