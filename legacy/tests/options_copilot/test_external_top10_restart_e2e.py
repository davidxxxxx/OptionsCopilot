from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.config import OptionsCopilotConfig
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.external_input_cli import publish_external_input_bundle
from options_copilot.gateway.broker_snapshot import BrokerSnapshotBuilder
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
    ExternalReadonlyFeedPublisher,
    ExternalReadonlyFeedReader,
)
from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)
from options_copilot.news.external_top10_source import (
    ExternalTop10Publisher,
    ExternalTop10StructureSource,
    trusted_terminal_scenario_set_hash,
)
from options_copilot.news.external_batch_producer import (
    ExternalBatchBoundTop10Producer,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
)
from options_copilot.news.open_reprice_economics import OpenRepriceEconomicsResolver
from options_copilot.news.preselection import strategy_structure_hash
from options_copilot.news.preselection_producer import (
    ProducerStatus,
    Top10PreselectionProducer,
)
from options_copilot.news.preselection_store import (
    LedgerBackedPreselectionProvider,
    NewsPreselectionStore,
)
from options_copilot.market import (
    EXTERNAL_SESSION_CALENDAR_SOURCE,
    ExternalSessionCalendarPublisher,
)
from options_copilot.runtime import build_external_top10_composition
from options_copilot.storage.canonical import canonical_hash


NEW_YORK = ZoneInfo("America/New_York")
MORNING_SLOT = datetime(2026, 8, 6, 9, 20, tzinfo=NEW_YORK)
MORNING_NOW = MORNING_SLOT + timedelta(seconds=10)
SCENARIO_ASOF = MORNING_SLOT - timedelta(seconds=30)
WRITTEN_AT = MORNING_SLOT - timedelta(seconds=20)
OPEN_SLOT = datetime(2026, 8, 6, 9, 35, tzinfo=NEW_YORK)
OPEN_NOW = OPEN_SLOT + timedelta(seconds=2)
QUOTE_ASOF = OPEN_NOW - timedelta(seconds=1)
EXPIRY = MORNING_SLOT.date() + timedelta(days=15)
STRATEGY_NAV = Decimal("100000.00")
PREMARKET_BATCH_ID = "external-premarket-20260806-0920"
QUOTE_BATCH_ID = "external-open-20260806-0935"


class _SessionGate:
    def is_trading_session(self, *, scheduled_for: datetime) -> bool:
        return True


class _FixedSessionGate:
    def __init__(self, value: bool | None) -> None:
        self.value = value

    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        return self.value


class _Broker:
    """One deterministic, read-only broker used by preflight and snapshot build."""

    def account_snapshot(self) -> dict[str, object]:
        return {
            "currency": "USD",
            "net_liquidation": STRATEGY_NAV,
            "connected": True,
        }

    def positions(self) -> tuple[object, ...]:
        return ()

    def working_orders(self) -> tuple[object, ...]:
        return ()

    def unsubmitted_instructions(self) -> tuple[object, ...]:
        return ()

    def option_contract_definitions(
        self,
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
                source="IBKR",
            )
            for item in contracts
        )

    def option_quote_batch(
        self,
        contracts: tuple[OptionContractRef, ...],
    ) -> OptionQuoteBatch:
        requested_at = QUOTE_ASOF - timedelta(milliseconds=200)
        completed_at = QUOTE_ASOF + timedelta(milliseconds=200)
        quotes = tuple(
            BatchedOptionQuote(
                contract_id=item.contract_id,
                batch_id=QUOTE_BATCH_ID,
                request_id=f"request-{item.contract_id}",
                requested_at=requested_at,
                observed_at=QUOTE_ASOF,
                completed_at=completed_at,
                source="IBKR",
                bid=Decimal("1.00"),
                ask=Decimal("1.10"),
                last=Decimal("1.05"),
                close=Decimal("1.00"),
                exchange_time=QUOTE_ASOF,
                volume=100,
                open_interest=1000,
                implied_volatility=Decimal("0.20"),
                delta=Decimal("0.50"),
                gamma=Decimal("0.02"),
                theta=Decimal("-0.05"),
                vega=Decimal("0.10"),
                market_data_type=1,
            )
            for item in contracts
        )
        return OptionQuoteBatch(
            batch_id=QUOTE_BATCH_ID,
            status=QuoteBatchStatus.COMPLETE,
            requested_at=requested_at,
            completed_at=completed_at,
            observed_at=QUOTE_ASOF,
            source="IBKR",
            quotes=quotes,
        )


def _identity(index: int) -> dict[str, object]:
    symbol = f"T{index:02d}"
    return {
        "conId": 100_000 + index,
        "localSymbol": f"{symbol}  {EXPIRY:%y%m%d}C00100000",
        "tradingClass": symbol,
        "multiplier": 100,
        "exchange": "SMART",
        "expiry": EXPIRY.isoformat(),
        "strike": "100",
        "right": "CALL",
    }


def _typed_leg(index: int) -> ConditionalOptionLeg:
    identity = _identity(index)
    return ConditionalOptionLeg(
        underlying=f"T{index:02d}",
        con_id=int(identity["conId"]),
        expiry=EXPIRY,
        strike=Decimal("100"),
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
        local_symbol=str(identity["localSymbol"]),
        trading_class=str(identity["tradingClass"]),
        multiplier=100,
        exchange="SMART",
    )


def _external_structure(index: int) -> dict[str, object]:
    symbol = f"T{index:02d}"
    candidate_id = f"external-parent-{index:02d}"
    typed_leg = _typed_leg(index)
    strategy_hash = strategy_structure_hash(symbol, "LONG_CALL", (typed_leg,))
    scenarios = (
        PreselectionTerminalScenario(Decimal("90"), Decimal("0.50")),
        PreselectionTerminalScenario(Decimal("110"), Decimal("0.50")),
    )
    scenario_hash = trusted_terminal_scenario_set_hash(
        candidate_id=candidate_id,
        strategy_hash=strategy_hash,
        scenario_asof=SCENARIO_ASOF,
        scenarios=scenarios,
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )
    raw_leg = {
        "underlying": symbol,
        "identity": _identity(index),
        "side": "BUY",
        "ratio": 1,
        "quantity": 1,
        "bid": None,
        "ask": None,
        "quote_asof": None,
        "quote_batch_id": None,
        "implied_volatility": None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "volume": None,
        "open_interest": None,
        "dte": None,
    }
    return {
        "preselection_id": candidate_id,
        "underlying": symbol,
        "strategy_type": "LONG_CALL",
        "legs": [raw_leg],
        "risk_defined": True,
        "maximum_loss_usd": "500.00",
        "estimated_cost_usd": "250.00",
        "cost_after_ev_usd": "25.00",
        "entry_condition": "Open quote remains executable after repricing.",
        "invalidation_condition": "Catalyst thesis invalidates.",
        "profit_target_condition": "Research target is reached.",
        "stop_loss_condition": "Defined risk threshold is reached.",
        "evidence_ids": [f"evidence-{index:02d}"],
        "evidence_hashes": [canonical_hash({"evidence": index})],
        "strategy_hash": strategy_hash,
        "research_summary": "External supporting-only pre-market structure.",
        "scenario_asof": SCENARIO_ASOF,
        "terminal_scenarios": [item.as_dict() for item in scenarios],
        "scenario_hash": scenario_hash,
        "execution_cost_contract_version": EXECUTION_COST_VERSION,
        "execution_cost_contract_hash": EXECUTION_COST_HASH,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _external_source_payload() -> dict[str, object]:
    structures = [_external_structure(index) for index in range(1, 11)]
    return {
        "batch_id": PREMARKET_BATCH_ID,
        "scheduled_for": MORNING_SLOT,
        "observed_at": SCENARIO_ASOF,
        "strategy_nav_usd": STRATEGY_NAV,
        "current_policy_version": INITIAL_POLICY_VERSION,
        "current_policy_hash": INITIAL_POLICY_HASH,
        "normal_risk_fraction": "0.10",
        "hard_risk_fraction": "0.20",
        "a_grade_enabled": False,
        "evidence_hashes": [
            evidence_hash
            for item in structures
            for evidence_hash in item["evidence_hashes"]
        ],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "structures": structures,
    }


def _publish_external_source(path: Path) -> ExternalTop10StructureSource:
    ExternalTop10Publisher(path, clock=lambda: WRITTEN_AT).publish(
        _external_source_payload()
    )
    return ExternalTop10StructureSource(path, clock=lambda: MORNING_NOW)


def _feed_identity(index: int) -> dict[str, object]:
    identity = dict(_identity(index))
    identity["right"] = "C"
    return identity


def _external_feed_payload(
    *,
    purpose: str,
    completed_at: datetime,
) -> dict[str, object]:
    requested_at = completed_at - timedelta(seconds=2)
    include_contracts = purpose == OPEN_REPRICE_PURPOSE
    secdefs: list[dict[str, object]] = []
    quotes: list[dict[str, object]] = []
    if include_contracts:
        quote_asof = completed_at
        for index in range(1, 11):
            identity = _feed_identity(index)
            secdefs.append(
                {
                    "batch_id": QUOTE_BATCH_ID,
                    "requested_at": requested_at,
                    "observed_at": quote_asof,
                    "completed_at": completed_at,
                    "identity": identity,
                    "security_type": "OPT",
                    "currency": "USD",
                    "standard_contract": True,
                    "adjusted": False,
                    "source": "IBKR_EXTERNAL_READONLY",
                }
            )
            quotes.append(
                {
                    "batch_id": QUOTE_BATCH_ID,
                    "request_id": f"request-{index}",
                    "requested_at": requested_at,
                    "observed_at": quote_asof,
                    "completed_at": completed_at,
                    "identity": identity,
                    "source": "IBKR_EXTERNAL_READONLY",
                    "bid": "1.00",
                    "ask": "1.10",
                    "last": "1.05",
                    "close": "1.00",
                    "volume": 100,
                    "open_interest": 1000,
                    "implied_volatility": "0.20",
                    "delta": "0.50",
                    "gamma": "0.02",
                    "theta": "-0.05",
                    "vega": "0.10",
                    "market_data_type": 1,
                }
            )
    return {
        "purpose": purpose,
        "batch_id": (
            PREMARKET_BATCH_ID
            if purpose == PREMARKET_ACCOUNT_PURPOSE
            else QUOTE_BATCH_ID
        ),
        "requested_at": requested_at,
        "completed_at": completed_at,
        "account": {
            "asof": completed_at,
            "currency": "USD",
            "net_liquidation": STRATEGY_NAV,
            "equity_with_loan_value": STRATEGY_NAV,
            "available_funds": STRATEGY_NAV,
            "buying_power": STRATEGY_NAV,
            "initial_margin": "0",
            "maintenance_margin": "0",
            "excess_liquidity": STRATEGY_NAV,
            "day_trades_remaining": 3,
            "connected": True,
        },
        "nav": {
            "asof": completed_at,
            "currency": "USD",
            "strategy_nav": STRATEGY_NAV,
            "source": "IBKR_EXTERNAL_READONLY",
        },
        "positions": [],
        "working_orders": [],
        "unsubmitted_instructions": [],
        "secdefs": secdefs,
        "quotes": quotes,
    }


def _session_calendar_payload(*, now: datetime) -> dict[str, object]:
    return {
        "calendar_id": "ibkr-options-session-20260806",
        "observed_at": now,
        "liquid_hours": "20260806:0930-1600",
        "trading_hours": "20260806:0930-1600",
        "timezone_id": "America/New_York",
        "source": EXTERNAL_SESSION_CALENDAR_SOURCE,
    }


def _publish_session_calendar(path: Path, *, now: datetime) -> None:
    ExternalSessionCalendarPublisher(path, clock=lambda: now).publish(
        _session_calendar_payload(now=now)
    )


class _CountingReader:
    def __init__(self, reader: ExternalReadonlyFeedReader) -> None:
        self.reader = reader
        self.calls = 0

    def read(self):
        self.calls += 1
        return self.reader.read()


class _ForbiddenReader:
    def __init__(self) -> None:
        self.calls = 0

    def read(self):
        self.calls += 1
        raise AssertionError("session failure must precede external file reads")


class _CountingSource:
    def __init__(self, source: ExternalTop10StructureSource) -> None:
        self.source = source
        self.calls = 0

    def resolve_top10(self, *, scheduled_for: datetime):
        self.calls += 1
        return self.source.resolve_top10(scheduled_for=scheduled_for)


def _provider_source_lineage(
    provider: LedgerBackedPreselectionProvider,
    *,
    phase: PreselectionPhase,
) -> tuple[dict[str, object], ...]:
    return tuple(
        value
        for (_preselection_id, candidate_phase), value in provider.lineage().items()
        if candidate_phase == phase.value
    )


def _producer(
    *,
    broker: _Broker,
    source: ExternalTop10StructureSource,
    store: NewsPreselectionStore,
    now: datetime,
) -> Top10PreselectionProducer:
    return Top10PreselectionProducer(
        account_state_reader=broker,
        structure_source=source,
        snapshot_provider=BrokerSnapshotBuilder(broker, clock=lambda: OPEN_NOW),
        store=store,
        clock=lambda: now,
        session_gate=_SessionGate(),
        premarket_account_only=True,
        require_exact_top10=True,
        open_economics_resolver=OpenRepriceEconomicsResolver(),
        strategy_nav_reader=lambda _snapshot: STRATEGY_NAV,
    )


def test_external_source_survives_store_restart_and_reprices_all_ten_parents(
    tmp_path: Path,
) -> None:
    source = _publish_external_source(tmp_path / "external-top10.json")
    broker = _Broker()
    store_path = tmp_path / "top10.sqlite3"

    morning_store = NewsPreselectionStore(store_path, clock=lambda: MORNING_NOW)
    morning = _producer(
        broker=broker,
        source=source,
        store=morning_store,
        now=MORNING_NOW,
    ).tick(scheduled_for=MORNING_SLOT)
    frozen = morning_store.latest_premarket()
    assert morning.status is ProducerStatus.PREMARKET_FROZEN
    assert morning.written_count == 10
    assert frozen is not None
    assert frozen.available_count == 10
    assert len(frozen.rows) == 10
    parent_rows = {row.preselection_id: row for row in frozen.rows}
    parent_head_hash = frozen.head_hash
    parent_run_id = frozen.run_id
    morning_store.close()

    reopened = NewsPreselectionStore(store_path, clock=lambda: OPEN_NOW)
    opened = _producer(
        broker=broker,
        source=source,
        store=reopened,
        now=OPEN_NOW,
    ).tick(scheduled_for=OPEN_SLOT)

    assert opened.status is ProducerStatus.OPEN_REPRICED
    assert opened.reason_codes == ()
    assert opened.written_count == 10
    assert opened.parent_head_hash == parent_head_hash
    assert reopened.verify_integrity()

    batch = reopened.latest_open_batch()
    replay = reopened.latest_replay()
    assert batch is not None
    assert replay is not None
    assert batch.action_pool_eligible
    assert batch.blockers == ()
    assert batch.parent_run_id == parent_run_id
    assert batch.parent_head_hash == parent_head_hash
    assert batch.quote_batch_id == QUOTE_BATCH_ID
    assert len(batch.rows) == len(replay.open_observations) == 10

    seen_parent_hashes: set[str] = set()
    seen_economics_hashes: set[str] = set()
    for rank, observation in enumerate(batch.rows, start=1):
        parent = parent_rows[observation.preselection_id]
        candidate = observation.candidate
        seen_parent_hashes.add(observation.parent_row_hash)
        seen_economics_hashes.add(str(candidate["economics_calculation_hash"]))

        assert observation.parent_run_id == parent_run_id
        assert observation.parent_head_hash == parent_head_hash
        assert observation.parent_row_hash == parent.row_hash
        assert observation.premarket_rank == parent.research_rank == rank
        assert observation.strategy_hash == parent.strategy_hash
        assert observation.structure_identity == parent.structure_identity
        assert candidate["scenario_asof"] == parent.candidate["scenario_asof"]
        assert candidate["scenario_hash"] == parent.candidate["scenario_hash"]
        assert candidate["risk_policy_version"] == INITIAL_POLICY_VERSION
        assert candidate["risk_policy_hash"] == INITIAL_POLICY_HASH
        assert candidate["execution_cost_contract_version"] == EXECUTION_COST_VERSION
        assert candidate["execution_cost_contract_hash"] == EXECUTION_COST_HASH
        assert candidate["broker_snapshot_hash"] is not None
        assert Decimal(str(candidate["strategy_nav_usd"])) == STRATEGY_NAV
        assert candidate["strategy_nav_post_hash"] is not None
        assert candidate["economics_quote_batch_id"] == QUOTE_BATCH_ID
        assert datetime.fromisoformat(
            str(candidate["economics_quote_asof"])
        ) == QUOTE_ASOF.astimezone(timezone.utc)
        assert candidate["payoff_hash"] is not None
        assert candidate["economics_calculation_hash"] is not None
        assert Decimal(str(candidate["maximum_loss_usd"])) == Decimal("120.00")
        assert Decimal(str(candidate["estimated_commission_usd"])) == Decimal("2.50")
        assert Decimal(str(candidate["estimated_entry_slippage_usd"])) == Decimal("2.50")
        assert Decimal(str(candidate["estimated_exit_slippage_usd"])) == Decimal("5.00")
        assert Decimal(str(candidate["cost_after_ev_usd"])) == Decimal("380.000")
        assert Decimal(str(candidate["risk_fraction"])) == Decimal("0.0012")

    assert seen_parent_hashes == {row.row_hash for row in frozen.rows}
    assert len(seen_parent_hashes) == 10
    assert len(seen_economics_hashes) == 10
    reopened.close()


def test_external_batch_adapter_reads_once_and_recovers_parent_after_restart(
    tmp_path: Path,
) -> None:
    source = _CountingSource(
        _publish_external_source(tmp_path / "external-top10-adapter.json")
    )
    feed_path = tmp_path / "external-readonly.json"
    store_path = tmp_path / "adapter-top10.sqlite3"

    morning_source_batch = ExternalReadonlyFeedPublisher(
        feed_path,
        clock=lambda: MORNING_NOW,
    ).publish(
        _external_feed_payload(
            purpose=PREMARKET_ACCOUNT_PURPOSE,
            completed_at=MORNING_NOW,
        )
    )
    morning_reader = _CountingReader(
        ExternalReadonlyFeedReader(feed_path, clock=lambda: MORNING_NOW)
    )
    morning_store = NewsPreselectionStore(store_path, clock=lambda: MORNING_NOW)
    morning = ExternalBatchBoundTop10Producer(
        session_gate=_SessionGate(),
        feed_reader=morning_reader,
        structure_source=source,
        store=morning_store,
        clock=lambda: MORNING_NOW,
    ).tick(scheduled_for=MORNING_SLOT)

    assert morning.status is ProducerStatus.PREMARKET_FROZEN
    assert morning.written_count == 10
    assert morning_reader.calls == 1
    assert source.calls == 1
    frozen = morning_store.latest_premarket()
    assert frozen is not None
    assert (
        frozen.source_batch_purpose,
        frozen.source_batch_id,
        frozen.source_batch_hash,
    ) == (
        PREMARKET_ACCOUNT_PURPOSE,
        morning_source_batch.batch_id,
        morning_source_batch.content_hash,
    )
    morning_provider = LedgerBackedPreselectionProvider(
        morning_store,
        require_external_source_lineage=True,
    )
    morning_lineage = _provider_source_lineage(
        morning_provider,
        phase=PreselectionPhase.PRE_MARKET,
    )
    assert len(morning_lineage) == 10
    assert {
        (
            item["source_batch_purpose"],
            item["source_batch_id"],
            item["source_batch_hash"],
        )
        for item in morning_lineage
    } == {
        (
            PREMARKET_ACCOUNT_PURPOSE,
            morning_source_batch.batch_id,
            morning_source_batch.content_hash,
        )
    }
    parent_head_hash = morning.parent_head_hash
    morning_store.close()

    open_source_batch = ExternalReadonlyFeedPublisher(
        feed_path,
        clock=lambda: OPEN_NOW,
    ).publish(
        _external_feed_payload(
            purpose=OPEN_REPRICE_PURPOSE,
            completed_at=OPEN_NOW,
        )
    )
    open_reader = _CountingReader(
        ExternalReadonlyFeedReader(feed_path, clock=lambda: OPEN_NOW)
    )
    reopened = NewsPreselectionStore(store_path, clock=lambda: OPEN_NOW)
    opened = ExternalBatchBoundTop10Producer(
        session_gate=_SessionGate(),
        feed_reader=open_reader,
        structure_source=source,
        store=reopened,
        clock=lambda: OPEN_NOW,
    ).tick(scheduled_for=OPEN_SLOT)

    assert opened.status is ProducerStatus.OPEN_REPRICED
    assert opened.reason_codes == ()
    assert opened.written_count == 10
    assert opened.parent_head_hash == parent_head_hash
    assert opened.quote_batch_id == QUOTE_BATCH_ID
    assert open_reader.calls == 1
    assert source.calls == 1
    batch = reopened.latest_open_batch()
    assert batch is not None and batch.action_pool_eligible
    recovered_parent = reopened.latest_premarket()
    assert recovered_parent is not None
    assert (
        recovered_parent.source_batch_purpose,
        recovered_parent.source_batch_id,
        recovered_parent.source_batch_hash,
    ) == (
        PREMARKET_ACCOUNT_PURPOSE,
        morning_source_batch.batch_id,
        morning_source_batch.content_hash,
    )
    assert (
        batch.source_batch_purpose,
        batch.source_batch_id,
        batch.source_batch_hash,
    ) == (
        OPEN_REPRICE_PURPOSE,
        open_source_batch.batch_id,
        open_source_batch.content_hash,
    )
    assert batch.source_batch_id == QUOTE_BATCH_ID
    assert batch.quote_batch_id == QUOTE_BATCH_ID
    assert batch.source_batch_hash not in {
        str(row.candidate["broker_snapshot_hash"])
        for row in batch.rows
    }
    assert (
        recovered_parent.source_batch_purpose,
        recovered_parent.source_batch_id,
        recovered_parent.source_batch_hash,
    ) != (
        batch.source_batch_purpose,
        batch.source_batch_id,
        batch.source_batch_hash,
    )
    reopened_provider = LedgerBackedPreselectionProvider(
        reopened,
        require_external_source_lineage=True,
    )
    recovered_premarket_lineage = _provider_source_lineage(
        reopened_provider,
        phase=PreselectionPhase.PRE_MARKET,
    )
    open_lineage = _provider_source_lineage(
        reopened_provider,
        phase=PreselectionPhase.OPEN_REPRICED,
    )
    assert len(recovered_premarket_lineage) == len(open_lineage) == 10
    assert {
        (
            item["source_batch_purpose"],
            item["source_batch_id"],
            item["source_batch_hash"],
        )
        for item in recovered_premarket_lineage
    } == {
        (
            PREMARKET_ACCOUNT_PURPOSE,
            morning_source_batch.batch_id,
            morning_source_batch.content_hash,
        )
    }
    assert {
        (
            item["source_batch_purpose"],
            item["source_batch_id"],
            item["source_batch_hash"],
        )
        for item in open_lineage
    } == {
        (
            OPEN_REPRICE_PURPOSE,
            open_source_batch.batch_id,
            open_source_batch.content_hash,
        )
    }
    assert all(
        Decimal(str(row.candidate["strategy_nav_usd"])) == STRATEGY_NAV
        and row.candidate["broker_snapshot_hash"] is not None
        and row.candidate["economics_calculation_hash"] is not None
        for row in batch.rows
    )
    reopened.close()


@pytest.mark.parametrize(
    ("session", "reason"),
    ((False, "SESSION_CLOSED"), (None, "SESSION_GATE_UNKNOWN")),
)
def test_external_batch_adapter_checks_session_before_reading_or_writing(
    tmp_path: Path,
    session: bool | None,
    reason: str,
) -> None:
    reader = _ForbiddenReader()
    store = NewsPreselectionStore(tmp_path / f"session-{session}.sqlite3")
    result = ExternalBatchBoundTop10Producer(
        session_gate=_FixedSessionGate(session),
        feed_reader=reader,
        structure_source=_CountingSource(
            _publish_external_source(tmp_path / f"top10-{session}.json")
        ),
        store=store,
        clock=lambda: MORNING_NOW,
    ).tick(scheduled_for=MORNING_SLOT)

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == (reason,)
    assert reader.calls == 0
    assert store.latest_premarket() is None
    store.close()


def test_external_batch_adapter_rejects_wrong_purpose_without_ledger_write(
    tmp_path: Path,
) -> None:
    feed_path = tmp_path / "wrong-purpose.json"
    ExternalReadonlyFeedPublisher(
        feed_path,
        clock=lambda: MORNING_NOW,
    ).publish(
        _external_feed_payload(
            purpose=OPEN_REPRICE_PURPOSE,
            completed_at=MORNING_NOW,
        )
    )
    source = _CountingSource(
        _publish_external_source(tmp_path / "wrong-purpose-top10.json")
    )
    reader = _CountingReader(
        ExternalReadonlyFeedReader(feed_path, clock=lambda: MORNING_NOW)
    )
    store = NewsPreselectionStore(tmp_path / "wrong-purpose.sqlite3")
    result = ExternalBatchBoundTop10Producer(
        session_gate=_SessionGate(),
        feed_reader=reader,
        structure_source=source,
        store=store,
        clock=lambda: MORNING_NOW,
    ).tick(scheduled_for=MORNING_SLOT)

    assert result.status is ProducerStatus.NO_TRADE
    assert result.reason_codes == ("EXTERNAL_BATCH_PURPOSE_MISMATCH",)
    assert reader.calls == 1
    assert source.calls == 0
    assert store.latest_premarket() is None
    store.close()


def test_external_runtime_composition_dispatches_both_slots_without_ibkr_gateway(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "external-runtime"
    feed_path = data_dir / "readonly.json"
    top10_path = data_dir / "top10.json"
    calendar_path = data_dir / "session.json"
    store_path = data_dir / "news-preselection.sqlite3"
    config = OptionsCopilotConfig(
        data_dir=data_dir,
        log_dir=data_dir / "logs",
        broker_acquisition_mode="EXTERNAL",
        external_readonly_feed_path=feed_path,
        external_top10_path=top10_path,
        external_session_calendar_path=calendar_path,
    )
    config.validate()
    config.ensure_runtime_directories()

    publish_external_input_bundle(
        {
            "schema": "options_copilot.external_input_bundle",
            "version": 1,
            "scheduled_for": MORNING_SLOT,
            "readonly_feed": _external_feed_payload(
                purpose=PREMARKET_ACCOUNT_PURPOSE,
                completed_at=MORNING_NOW,
            ),
            "top10": _external_source_payload(),
            "session_calendar": _session_calendar_payload(now=MORNING_NOW),
        },
        readonly_feed_path=feed_path,
        top10_path=top10_path,
        session_calendar_path=calendar_path,
        clock=lambda: MORNING_NOW,
    )
    morning_store = NewsPreselectionStore(store_path, clock=lambda: MORNING_NOW)
    morning_composition = build_external_top10_composition(
        config,
        news_preselection_store=morning_store,
        clock=lambda: MORNING_NOW,
    )
    morning = morning_composition.scheduler_loop.tick_once()

    assert morning.producer_status == "PREMARKET_FROZEN"
    assert morning.producer_reason_codes == ()
    assert morning_composition.health()["connector_count"] == 0
    assert morning_composition.health()["connected"] is False
    parent = morning_store.latest_premarket()
    assert parent is not None and parent.available_count == 10
    parent_head_hash = parent.head_hash
    morning_composition.close()
    morning_store.close()

    publish_external_input_bundle(
        {
            "schema": "options_copilot.external_input_bundle",
            "version": 1,
            "scheduled_for": OPEN_SLOT,
            "readonly_feed": _external_feed_payload(
                purpose=OPEN_REPRICE_PURPOSE,
                completed_at=OPEN_NOW,
            ),
            "top10": None,
            "session_calendar": _session_calendar_payload(now=OPEN_NOW),
        },
        readonly_feed_path=feed_path,
        top10_path=top10_path,
        session_calendar_path=calendar_path,
        clock=lambda: OPEN_NOW,
    )
    open_store = NewsPreselectionStore(store_path, clock=lambda: OPEN_NOW)
    open_composition = build_external_top10_composition(
        config,
        news_preselection_store=open_store,
        clock=lambda: OPEN_NOW,
    )
    opened = open_composition.scheduler_loop.tick_once()

    assert opened.producer_status == "OPEN_REPRICED"
    assert opened.producer_reason_codes == ()
    batch = open_store.latest_open_batch()
    assert batch is not None and batch.parent_head_hash == parent_head_hash
    assert batch.action_pool_eligible
    assert len(batch.rows) == 10
    assert open_composition.health()["connector_count"] == 0
    open_composition.close()
    open_store.close()
