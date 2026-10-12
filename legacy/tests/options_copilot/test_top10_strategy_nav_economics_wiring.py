from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from options_copilot.gateway.broker_snapshot import BrokerSnapshotBuilder
from options_copilot.news.open_reprice_economics import OpenRepriceEconomicsResolver
from options_copilot.news.preselection_producer import (
    ProducerStatus,
    Top10PreselectionProducer,
)
from options_copilot.performance.nav_ledger import StrategyNavLedger
from tests.options_copilot.test_top10_open_reprice_producer import (
    ET,
    OPEN,
    PREMARKET,
    FakeBroker,
    FakeSessionGate,
    FakeStore,
    FakeStructureSource,
    FixedClock,
    _structures,
)


CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "strategy_nav_contract.v1.json"
)


def _morning(
    *,
    clock: FixedClock,
    broker: FakeBroker,
    source: FakeStructureSource,
    store: FakeStore,
) -> None:
    producer = Top10PreselectionProducer(
        account_state_reader=broker,
        structure_source=source,
        snapshot_provider=BrokerSnapshotBuilder(broker, clock=clock),
        store=store,
        clock=clock,
        session_gate=FakeSessionGate(),
        premarket_account_only=True,
        open_economics_resolver=OpenRepriceEconomicsResolver(),
        strategy_nav_reader=lambda _snapshot: Decimal("1"),
    )
    result = producer.tick(scheduled_for=PREMARKET)
    assert result.status is ProducerStatus.PREMARKET_FROZEN


def _open(
    *,
    clock: FixedClock,
    broker: FakeBroker,
    source: FakeStructureSource,
    store: FakeStore,
    strategy_nav_reader,
):
    return Top10PreselectionProducer(
        account_state_reader=broker,
        structure_source=source,
        snapshot_provider=BrokerSnapshotBuilder(broker, clock=clock),
        store=store,
        clock=clock,
        session_gate=FakeSessionGate(),
        premarket_account_only=True,
        open_economics_resolver=OpenRepriceEconomicsResolver(),
        strategy_nav_reader=strategy_nav_reader,
    ).tick(scheduled_for=OPEN)


def _strict_nav_reader(ledger: StrategyNavLedger, *, asof: datetime):
    def read(_broker_snapshot: object) -> Decimal:
        snapshot = ledger.snapshot(asof=asof)
        if not snapshot.valid or snapshot.strategy_nav is None:
            raise RuntimeError("signed Strategy NAV is unavailable")
        return snapshot.strategy_nav

    return read


def test_signed_strategy_nav_ledger_enables_direct_open_economics(
    tmp_path: Path,
) -> None:
    clock = FixedClock(PREMARKET)
    broker = FakeBroker(clock)
    source = FakeStructureSource(_structures(2))
    store = FakeStore()
    _morning(clock=clock, broker=broker, source=source, store=store)

    clock.value = OPEN
    with StrategyNavLedger(
        tmp_path / "strategy-nav.sqlite3",
        contract=CONTRACT_PATH,
        clock=clock,
    ) as ledger:
        signed = ledger.snapshot(asof=OPEN)
        assert signed.valid
        assert signed.strategy_nav is not None
        assert signed.contract_hash is not None
        assert signed.ledger_head_hash is not None

        result = _open(
            clock=clock,
            broker=broker,
            source=source,
            store=store,
            strategy_nav_reader=_strict_nav_reader(ledger, asof=OPEN),
        )

    assert result.status is ProducerStatus.OPEN_REPRICED
    assert result.reason_codes == ()
    assert result.written_count == 2
    assert len(store.open_writes) == 1
    assert all(
        candidate.strategy_nav_usd == signed.strategy_nav
        and candidate.strategy_nav_post_hash is not None
        and candidate.broker_snapshot_hash is not None
        and candidate.payoff_hash is not None
        and candidate.economics_calculation_hash is not None
        for candidate in store.open_writes[0]
    )


@pytest.mark.parametrize("ledger_case", ("missing_contract", "invalid_asof"))
def test_missing_or_invalid_strategy_nav_ledger_fails_entire_batch_closed(
    tmp_path: Path,
    ledger_case: str,
) -> None:
    clock = FixedClock(PREMARKET)
    broker = FakeBroker(clock)
    source = FakeStructureSource(_structures(2))
    store = FakeStore()
    _morning(clock=clock, broker=broker, source=source, store=store)

    clock.value = OPEN
    contract = None if ledger_case == "missing_contract" else CONTRACT_PATH
    snapshot_asof = (
        OPEN
        if ledger_case == "missing_contract"
        else datetime(2020, 1, 1, tzinfo=timezone.utc).astimezone(ET)
    )
    with StrategyNavLedger(
        tmp_path / f"strategy-nav-{ledger_case}.sqlite3",
        contract=contract,
        clock=clock,
    ) as ledger:
        invalid = ledger.snapshot(asof=snapshot_asof)
        assert not invalid.valid
        assert invalid.strategy_nav is None

        result = _open(
            clock=clock,
            broker=broker,
            source=source,
            store=store,
            strategy_nav_reader=_strict_nav_reader(ledger, asof=snapshot_asof),
        )

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("STRATEGY_NAV_UNAVAILABLE",)
    assert result.written_count == 2
    assert store.open_batch is not None
    assert store.open_batch.action_pool_eligible is False
    assert store.open_batch.blockers == ("STRATEGY_NAV_UNAVAILABLE",)
    assert len(store.open_writes) == 1
    assert all(
        candidate.strategy_nav_usd is None
        and candidate.strategy_nav_post_hash is None
        and candidate.economics_calculation_hash is None
        for candidate in store.open_writes[0]
    )
