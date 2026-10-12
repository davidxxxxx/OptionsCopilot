from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import options_copilot.news.preselection_store as preselection_store_module
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
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
    strategy_structure_hash,
)
from options_copilot.news.preselection_store import (
    LedgerBackedPreselectionProvider,
    NewsPreselectionStore,
    NewsPreselectionStoreConflict,
    NewsPreselectionStoreCorruption,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


PREMARKET_NOW = datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc)
OPEN_NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)
EXPIRY = date(2026, 8, 21)
PREMARKET_SOURCE_BATCH_ID = "premarket-account-20260805-0920"
PREMARKET_SOURCE_BATCH_HASH = "a" * 64
OPEN_SOURCE_BATCH_ID = "open-reprice-20260805-0935"
OPEN_SOURCE_BATCH_HASH = "b" * 64

_OPTIONAL_ECONOMICS_FIELDS = (
    "terminal_scenarios",
    "scenario_asof",
    "scenario_hash",
    "execution_cost_contract_version",
    "execution_cost_contract_hash",
    "risk_policy_version",
    "risk_policy_hash",
    "broker_snapshot_hash",
    "account_snapshot_hash",
    "strategy_nav_usd",
    "strategy_nav_post_hash",
    "economics_quote_batch_id",
    "economics_quote_asof",
    "payoff_hash",
    "economics_calculation_hash",
    "debit_usd",
    "credit_usd",
    "net_entry_cost_usd",
    "estimated_commission_usd",
    "estimated_entry_slippage_usd",
    "estimated_exit_slippage_usd",
    "estimated_slippage_usd",
    "expected_value_before_costs_usd",
    "risk_fraction",
)


def _candidate(
    identifier: str,
    *,
    phase: PreselectionPhase = PreselectionPhase.PRE_MARKET,
    now: datetime = PREMARKET_NOW,
    con_id_offset: int = 0,
    strike_offset: Decimal = Decimal("0"),
    quote_batch_id: str = "batch-1",
    quote_age: timedelta = timedelta(0),
    incomplete_quotes: bool = False,
    underlying: str = "AAPL",
    strategy_type: str = "DEBIT_VERTICAL",
    ratio: int = 1,
    quantity: int = 1,
    summary: str = "Frozen supporting-only conditional option research.",
    evidence_suffix: str = "a",
) -> ConditionalOptionPreselection:
    quote_time = now - quote_age
    low_strike = Decimal("200") + strike_offset
    high_strike = Decimal("205") + strike_offset
    common = {
        "underlying": underlying,
        "expiry": EXPIRY,
        "right": OptionRight.CALL,
        "trading_class": underlying,
        "multiplier": 100,
        "exchange": "SMART",
        "ratio": ratio,
        "quantity": quantity,
        "quote_asof": quote_time,
        "quote_batch_id": quote_batch_id,
        "implied_volatility": None if incomplete_quotes else Decimal("0.25"),
        "delta": None if incomplete_quotes else Decimal("0.50"),
        "gamma": None if incomplete_quotes else Decimal("0.03"),
        "theta": None if incomplete_quotes else Decimal("-0.08"),
        "vega": None if incomplete_quotes else Decimal("0.12"),
        "volume": None if incomplete_quotes else 1200,
        "open_interest": None if incomplete_quotes else 9000,
        "dte": 16,
    }
    legs = (
        ConditionalOptionLeg(
            **common,
            con_id=1001 + con_id_offset,
            local_symbol=f"{underlying} {EXPIRY:%y%m%d}C{low_strike}",
            strike=low_strike,
            side=OptionLegSide.BUY,
            bid=Decimal("4.90"),
            ask=Decimal("5.00"),
        ),
        ConditionalOptionLeg(
            **common,
            con_id=1002 + con_id_offset,
            local_symbol=f"{underlying} {EXPIRY:%y%m%d}C{high_strike}",
            strike=high_strike,
            side=OptionLegSide.SELL,
            bid=Decimal("2.40"),
            ask=Decimal("2.50"),
        ),
    )
    structure_hash = strategy_structure_hash(underlying, strategy_type, legs)
    return ConditionalOptionPreselection(
        preselection_id=identifier,
        underlying=underlying,
        strategy_type=strategy_type,
        phase=phase,
        legs=legs,
        risk_defined=True,
        maximum_loss_usd=Decimal("280"),
        estimated_cost_usd=Decimal("260"),
        cost_after_ev_usd=Decimal("40"),
        entry_condition="Observe only while all current hard gates remain valid.",
        invalidation_condition="Underlying invalidates the directional thesis.",
        profit_target_condition="Human-defined research target is observed.",
        stop_loss_condition="Human-defined research stop is observed.",
        evidence_ids=(f"evidence-{evidence_suffix}",),
        evidence_hashes=(hashlib.sha256(evidence_suffix.encode("utf-8")).hexdigest(),),
        strategy_hash=structure_hash,
        research_summary=summary,
    )


def _open_from(
    premarket: ConditionalOptionPreselection,
    *,
    now: datetime = OPEN_NOW,
    quote_age: timedelta = timedelta(0),
    incomplete_quotes: bool = False,
    observation_suffix: str = "b",
) -> ConditionalOptionPreselection:
    legs = tuple(
        replace(
            leg,
            bid=(None if incomplete_quotes else (leg.bid or Decimal("0")) + Decimal("0.10")),
            ask=(None if incomplete_quotes else (leg.ask or Decimal("0")) + Decimal("0.10")),
            quote_asof=now - quote_age,
            quote_batch_id=f"open-batch-{observation_suffix}",
            implied_volatility=None if incomplete_quotes else Decimal("0.27"),
            delta=None if incomplete_quotes else leg.delta,
            gamma=None if incomplete_quotes else leg.gamma,
            theta=None if incomplete_quotes else leg.theta,
            vega=None if incomplete_quotes else leg.vega,
            volume=None if incomplete_quotes else (leg.volume or 0) + 100,
            open_interest=None if incomplete_quotes else (leg.open_interest or 0) + 50,
        )
        for leg in premarket.legs
    )
    return ConditionalOptionPreselection(
        preselection_id=premarket.preselection_id,
        underlying=premarket.underlying,
        strategy_type=premarket.strategy_type,
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=legs,
        risk_defined=True,
        maximum_loss_usd=Decimal("290"),
        estimated_cost_usd=Decimal("270"),
        cost_after_ev_usd=Decimal("35"),
        entry_condition="Repriced at the open; supporting-only observation.",
        invalidation_condition="Updated invalidation condition.",
        profit_target_condition="Updated research target.",
        stop_loss_condition="Updated research stop.",
        evidence_ids=(f"open-evidence-{observation_suffix}",),
        evidence_hashes=(
            hashlib.sha256(observation_suffix.encode("utf-8")).hexdigest(),
        ),
        strategy_hash=premarket.strategy_hash,
        research_summary="Fresh quotes and economics; the frozen structure did not change.",
    )


def _open_with_complete_economics(
    premarket: ConditionalOptionPreselection,
) -> ConditionalOptionPreselection:
    opened = _open_from(premarket)
    scenarios = (
        PreselectionTerminalScenario(Decimal("190"), Decimal("0.25")),
        PreselectionTerminalScenario(Decimal("210"), Decimal("0.50")),
        PreselectionTerminalScenario(Decimal("230"), Decimal("0.25")),
    )
    scenario_asof = OPEN_NOW - timedelta(seconds=1)
    scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=opened.preselection_id,
        strategy_hash=opened.strategy_hash,
        scenario_asof=scenario_asof,
        scenarios=tuple(
            TrustedTerminalScenario(
                scenario.terminal_underlying_price,
                scenario.probability,
            )
            for scenario in scenarios
        ),
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )
    broker_snapshot_hash = "4" * 64
    strategy_nav_usd = Decimal("5000.00")
    nav_hash = strategy_nav_post_hash(
        candidate_id=opened.preselection_id,
        strategy_hash=opened.strategy_hash,
        snapshot_hash=broker_snapshot_hash,
        strategy_nav_usd=strategy_nav_usd,
    )
    provisional = OpenRepriceEconomics(
        candidate_id=opened.preselection_id,
        strategy_hash=opened.strategy_hash,
        broker_snapshot_hash=broker_snapshot_hash,
        quote_batch_id="open-batch-b",
        quote_asof=OPEN_NOW,
        scenario_hash=scenario_set.scenario_hash,
        scenario_asof=scenario_asof,
        cost_contract_version=EXECUTION_COST_VERSION,
        cost_contract_hash=EXECUTION_COST_HASH,
        policy_version=INITIAL_POLICY_VERSION,
        policy_hash=INITIAL_POLICY_HASH,
        strategy_nav_usd=strategy_nav_usd,
        strategy_nav_post_hash=nav_hash,
        debit_usd=Decimal("260.00"),
        credit_usd=Decimal("0.00"),
        commission_usd=Decimal("2.00"),
        entry_slippage_usd=Decimal("3.00"),
        exit_slippage_usd=Decimal("5.00"),
        total_slippage_usd=Decimal("8.00"),
        all_in_cost_usd=Decimal("270.00"),
        maximum_loss_usd=Decimal("290"),
        before_cost_expected_value_usd=Decimal("45.00"),
        after_cost_expected_value_usd=Decimal("35"),
        payoff_hash="7" * 64,
        risk_fraction=Decimal("0.058"),
        economics_hash="0" * 64,
    )
    economics = replace(
        provisional,
        economics_hash=canonical_hash(provisional.hash_payload()),
    )
    return replace(
        opened,
        terminal_scenarios=scenarios,
        scenario_asof=scenario_asof,
        scenario_hash=scenario_set.scenario_hash,
        execution_cost_contract_version=EXECUTION_COST_VERSION,
        execution_cost_contract_hash=EXECUTION_COST_HASH,
        risk_policy_version=INITIAL_POLICY_VERSION,
        risk_policy_hash=INITIAL_POLICY_HASH,
        broker_snapshot_hash=broker_snapshot_hash,
        account_snapshot_hash="5" * 64,
        strategy_nav_usd=economics.strategy_nav_usd,
        strategy_nav_post_hash=economics.strategy_nav_post_hash,
        economics_quote_batch_id=economics.quote_batch_id,
        economics_quote_asof=economics.quote_asof,
        payoff_hash=economics.payoff_hash,
        economics_calculation_hash=economics.economics_hash,
        debit_usd=economics.debit_usd,
        credit_usd=economics.credit_usd,
        net_entry_cost_usd=economics.all_in_cost_usd,
        estimated_commission_usd=economics.commission_usd,
        estimated_entry_slippage_usd=economics.entry_slippage_usd,
        estimated_exit_slippage_usd=economics.exit_slippage_usd,
        estimated_slippage_usd=economics.total_slippage_usd,
        expected_value_before_costs_usd=economics.before_cost_expected_value_usd,
        risk_fraction=economics.risk_fraction,
    )


def test_store_uses_wal_full_sync_and_forbids_update_delete(tmp_path: Path) -> None:
    path = tmp_path / "news-preselection.sqlite3"
    with NewsPreselectionStore(path, clock=lambda: PREMARKET_NOW) as store:
        stored = store.append_premarket_run("run-1", (_candidate("pre-1"),))
        assert store.journal_mode == "wal"
        assert store.synchronous == "full"
        assert store.schema_version == 3
        assert stored.available_count == 1
        assert stored.requested_count == 10
        assert stored.source_batch_purpose is None
        assert stored.source_batch_id is None
        assert stored.source_batch_hash is None
        assert stored.as_dict()["decision_authority"] == "SUPPORTING_ONLY"
        assert stored.as_dict()["approval_eligible"] is False
        assert stored.as_dict()["instruction_creation_allowed"] is False
        assert stored.as_dict()["order_allowed"] is False

        raw = sqlite3.connect(path)
        try:
            raw.execute("PRAGMA foreign_keys=ON")
            with pytest.raises(sqlite3.IntegrityError, match="update forbidden"):
                raw.execute(
                    "UPDATE premarket_runs SET available_count=0 WHERE run_id='run-1'"
                )
            raw.rollback()
            with pytest.raises(sqlite3.IntegrityError, match="delete forbidden"):
                raw.execute("DELETE FROM ledger_entries WHERE sequence=1")
            raw.rollback()
        finally:
            raw.close()


def test_source_batch_lineage_round_trips_in_heads_chain_and_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-lineage-round-trip.sqlite3"
    premarket = _candidate("source-lineage")
    with NewsPreselectionStore(path) as store:
        run = store.append_premarket_run(
            "source-lineage-run",
            (premarket,),
            now=PREMARKET_NOW,
            source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
            source_batch_id=PREMARKET_SOURCE_BATCH_ID,
            source_batch_hash=PREMARKET_SOURCE_BATCH_HASH,
        )
        assert run.source_batch_id == PREMARKET_SOURCE_BATCH_ID
        assert run.source_batch_hash == PREMARKET_SOURCE_BATCH_HASH
        assert run.source_batch_purpose == PREMARKET_ACCOUNT_PURPOSE
        assert run.as_dict()["source_batch_purpose"] == PREMARKET_ACCOUNT_PURPOSE
        assert run.as_dict()["source_batch_id"] == PREMARKET_SOURCE_BATCH_ID
        premarket_head = store._connection.execute(
            "SELECT payload_json,content_hash FROM ledger_entries "
            "WHERE entry_type='PREMARKET_HEAD'"
        ).fetchone()
        assert premarket_head is not None
        premarket_payload = json.loads(str(premarket_head["payload_json"]))
        assert premarket_payload["schema"] == "options_copilot.news_premarket_head.v3"
        assert premarket_payload["source_batch_purpose"] == PREMARKET_ACCOUNT_PURPOSE
        assert premarket_payload["source_batch_id"] == PREMARKET_SOURCE_BATCH_ID
        assert premarket_payload["source_batch_hash"] == PREMARKET_SOURCE_BATCH_HASH
        assert canonical_hash(premarket_payload) == premarket_head["content_hash"]

        batch = store.append_open_batch(
            run.head_hash,
            (_open_from(premarket),),
            batch_id="source-lineage-open",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
            source_batch_purpose=OPEN_REPRICE_PURPOSE,
            source_batch_id=OPEN_SOURCE_BATCH_ID,
            source_batch_hash=OPEN_SOURCE_BATCH_HASH,
        )
        assert batch.source_batch_id == OPEN_SOURCE_BATCH_ID
        assert batch.source_batch_hash == OPEN_SOURCE_BATCH_HASH
        assert batch.source_batch_purpose == OPEN_REPRICE_PURPOSE
        assert batch.as_dict()["source_batch_hash"] == OPEN_SOURCE_BATCH_HASH
        open_head = store._connection.execute(
            "SELECT payload_json,content_hash FROM ledger_entries "
            "WHERE entry_type='OPEN_BATCH_HEAD'"
        ).fetchone()
        assert open_head is not None
        open_payload = json.loads(str(open_head["payload_json"]))
        assert open_payload["schema"] == "options_copilot.news_open_batch_head.v3"
        assert open_payload["source_batch_purpose"] == OPEN_REPRICE_PURPOSE
        assert open_payload["source_batch_id"] == OPEN_SOURCE_BATCH_ID
        assert open_payload["source_batch_hash"] == OPEN_SOURCE_BATCH_HASH
        assert canonical_hash(open_payload) == open_head["content_hash"]

    with NewsPreselectionStore(path) as reopened:
        assert reopened.verify_integrity()
        restored_run = reopened.latest_premarket()
        restored_batch = reopened.latest_open_batch()
        assert restored_run is not None
        assert restored_batch is not None
        assert restored_run.source_batch_purpose == PREMARKET_ACCOUNT_PURPOSE
        assert restored_run.source_batch_id == PREMARKET_SOURCE_BATCH_ID
        assert restored_run.source_batch_hash == PREMARKET_SOURCE_BATCH_HASH
        assert restored_batch.source_batch_id == OPEN_SOURCE_BATCH_ID
        assert restored_batch.source_batch_hash == OPEN_SOURCE_BATCH_HASH
        assert restored_batch.source_batch_purpose == OPEN_REPRICE_PURPOSE


@pytest.mark.parametrize(
    ("source_batch_purpose", "source_batch_id", "source_batch_hash"),
    (
        (None, PREMARKET_SOURCE_BATCH_ID, PREMARKET_SOURCE_BATCH_HASH),
        (PREMARKET_ACCOUNT_PURPOSE, PREMARKET_SOURCE_BATCH_ID, None),
        (PREMARKET_ACCOUNT_PURPOSE, None, PREMARKET_SOURCE_BATCH_HASH),
        (OPEN_REPRICE_PURPOSE, PREMARKET_SOURCE_BATCH_ID, PREMARKET_SOURCE_BATCH_HASH),
        (PREMARKET_ACCOUNT_PURPOSE, "invalid/source", PREMARKET_SOURCE_BATCH_HASH),
        (PREMARKET_ACCOUNT_PURPOSE, "x" * 129, PREMARKET_SOURCE_BATCH_HASH),
        (PREMARKET_ACCOUNT_PURPOSE, PREMARKET_SOURCE_BATCH_ID, "A" * 64),
        (PREMARKET_ACCOUNT_PURPOSE, PREMARKET_SOURCE_BATCH_ID, "a" * 63),
    ),
)
def test_premarket_source_batch_lineage_rejects_half_or_invalid_pairs(
    tmp_path: Path,
    source_batch_purpose: object,
    source_batch_id: object,
    source_batch_hash: object,
) -> None:
    with NewsPreselectionStore(tmp_path / "invalid-premarket-source.sqlite3") as store:
        with pytest.raises(ValueError, match="source"):
            store.append_premarket_run(
                "invalid-source-run",
                (_candidate("invalid-source"),),
                now=PREMARKET_NOW,
                source_batch_purpose=source_batch_purpose,  # type: ignore[arg-type]
                source_batch_id=source_batch_id,  # type: ignore[arg-type]
                source_batch_hash=source_batch_hash,  # type: ignore[arg-type]
            )
        assert store.latest_premarket() is None


def test_open_source_batch_lineage_rejects_half_or_invalid_pairs(
    tmp_path: Path,
) -> None:
    premarket = _candidate("invalid-open-source")
    with NewsPreselectionStore(tmp_path / "invalid-open-source.sqlite3") as store:
        run = store.append_premarket_run(
            "invalid-open-parent", (premarket,), now=PREMARKET_NOW
        )
        for index, (
            source_batch_purpose,
            source_batch_id,
            source_batch_hash,
        ) in enumerate(
            (
                (None, OPEN_SOURCE_BATCH_ID, OPEN_SOURCE_BATCH_HASH),
                (OPEN_REPRICE_PURPOSE, OPEN_SOURCE_BATCH_ID, None),
                (OPEN_REPRICE_PURPOSE, None, OPEN_SOURCE_BATCH_HASH),
                (
                    PREMARKET_ACCOUNT_PURPOSE,
                    OPEN_SOURCE_BATCH_ID,
                    OPEN_SOURCE_BATCH_HASH,
                ),
                (OPEN_REPRICE_PURPOSE, "invalid/source", OPEN_SOURCE_BATCH_HASH),
                (OPEN_REPRICE_PURPOSE, OPEN_SOURCE_BATCH_ID, "B" * 64),
            )
        ):
            with pytest.raises(ValueError, match="source"):
                store.append_open_batch(
                    run.head_hash,
                    (_open_from(premarket),),
                    batch_id=f"invalid-open-source-{index}",
                    scheduled_for=OPEN_NOW,
                    observed_at=OPEN_NOW,
                    source_batch_purpose=source_batch_purpose,  # type: ignore[arg-type]
                    source_batch_id=source_batch_id,  # type: ignore[arg-type]
                    source_batch_hash=source_batch_hash,  # type: ignore[arg-type]
                )
        assert store.latest_open_batch() is None
        assert store.verify_integrity() is True


def test_premarket_rejects_wrong_phase_duplicates_and_more_than_ten(
    tmp_path: Path,
) -> None:
    path = tmp_path / "limits.sqlite3"
    with NewsPreselectionStore(path) as store:
        with pytest.raises(ValueError, match="PRE_MARKET"):
            store.append_premarket_run(
                "wrong-phase",
                (_candidate("open", phase=PreselectionPhase.OPEN_REPRICED),),
                now=PREMARKET_NOW,
            )
        with pytest.raises(ValueError, match="preselection_id"):
            store.append_premarket_run(
                "duplicate-id",
                (
                    _candidate("same", con_id_offset=0),
                    _candidate("same", con_id_offset=10, strike_offset=Decimal("10")),
                ),
                now=PREMARKET_NOW,
            )
        with pytest.raises(ValueError, match="strategy_hash"):
            store.append_premarket_run(
                "duplicate-structure",
                (_candidate("first"), _candidate("second")),
                now=PREMARKET_NOW,
            )
        with pytest.raises(ValueError, match="more than ten"):
            store.append_premarket_run(
                "too-many",
                tuple(
                    _candidate(
                        f"pre-{index}",
                        con_id_offset=index * 10,
                        strike_offset=Decimal(index * 10),
                    )
                    for index in range(11)
                ),
                now=PREMARKET_NOW,
            )
        candidate = _candidate("unique")
        store.append_premarket_run("once", (candidate,), now=PREMARKET_NOW)
        with pytest.raises(NewsPreselectionStoreConflict, match="run_id"):
            store.append_premarket_run("once", (candidate,), now=PREMARKET_NOW)


def test_premarket_requires_complete_exact_structure_identity(tmp_path: Path) -> None:
    candidate = _candidate("incomplete-structure")
    broken_leg = replace(candidate.legs[0], con_id=None)
    broken_legs = (broken_leg, candidate.legs[1])
    broken = replace(
        candidate,
        legs=broken_legs,
        strategy_hash=strategy_structure_hash(
            candidate.underlying, candidate.strategy_type, broken_legs
        ),
    )
    with NewsPreselectionStore(tmp_path / "identity.sqlite3") as store:
        with pytest.raises(ValueError, match="structure identity is incomplete"):
            store.append_premarket_run("run", (broken,), now=PREMARKET_NOW)


def test_open_observation_allows_only_nonstructural_changes(tmp_path: Path) -> None:
    premarket = _candidate("pre-1")
    with NewsPreselectionStore(tmp_path / "open.sqlite3") as store:
        run = store.append_premarket_run("run-1", (premarket,), now=PREMARKET_NOW)
        parent = run.rows[0]
        repriced = _open_with_complete_economics(premarket)
        observation = store.append_open_observation(
            run.head_hash,
            parent.row_hash,
            repriced,
            observation_id="open-1",
            now=OPEN_NOW,
        )
        payload = observation.as_dict()
        assert payload["phase"] == "OPEN_REPRICED"
        assert payload["quote_batch_id"] == "open-batch-b"
        assert payload["oldest_quote_asof"] == OPEN_NOW.isoformat()
        assert payload["blockers"] == [
            "LEGACY_SINGLE_OBSERVATION_NOT_BATCH_ELIGIBLE"
        ]
        assert payload["action_pool_eligible"] is False
        assert payload["decision_authority"] == "SUPPORTING_ONLY"
        assert payload["approval_eligible"] is False
        assert payload["instruction_creation_allowed"] is False
        assert payload["order_allowed"] is False
        assert observation.strategy_hash == parent.strategy_hash
        assert observation.structure_identity == parent.structure_identity
        assert store.latest_open()[0].observation_id == "open-1"


@pytest.mark.parametrize(
    "field",
    (
        "con_id", "local_symbol", "trading_class", "multiplier", "exchange",
        "expiry", "strike", "right", "side", "ratio", "quantity",
    ),
)
def test_open_rejects_every_leg_structure_drift(tmp_path: Path, field: str) -> None:
    premarket = _candidate("pre-drift")
    with NewsPreselectionStore(tmp_path / f"drift-{field}.sqlite3") as store:
        run = store.append_premarket_run("run", (premarket,), now=PREMARKET_NOW)
        opened = _open_from(premarket)
        first = opened.legs[0]
        changes = {
            "con_id": (first.con_id or 0) + 999,
            "local_symbol": f"{first.local_symbol}-DRIFT",
            "trading_class": "AAPL2",
            "multiplier": 10,
            "exchange": "CBOE",
            "expiry": date(2026, 8, 28),
            "strike": (first.strike or Decimal("0")) + Decimal("1"),
            "right": OptionRight.PUT,
            "side": OptionLegSide.SELL,
            "ratio": (first.ratio or 0) + 1,
            "quantity": (first.quantity or 0) + 1,
        }
        changed_legs = (replace(first, **{field: changes[field]}), opened.legs[1])
        drifted = replace(
            opened,
            legs=changed_legs,
            strategy_hash=strategy_structure_hash(
                opened.underlying, opened.strategy_type, changed_legs
            ),
        )
        with pytest.raises(NewsPreselectionStoreConflict, match="frozen"):
            store.append_open_observation(
                run.head_hash,
                run.rows[0].row_hash,
                drifted,
                observation_id=f"drift-{field}",
                now=OPEN_NOW,
            )

def test_open_rejects_top_level_identity_drift_and_wrong_parent(tmp_path: Path) -> None:
    first = _candidate("same-id")
    second = _candidate(
        "same-id",
        underlying="MSFT",
        con_id_offset=100,
        strike_offset=Decimal("100"),
    )
    with NewsPreselectionStore(tmp_path / "parents.sqlite3") as store:
        run_a = store.append_premarket_run("run-a", (first,), now=PREMARKET_NOW)
        run_b = store.append_premarket_run("run-b", (second,), now=PREMARKET_NOW)
        with pytest.raises(NewsPreselectionStoreConflict, match="parent"):
            store.append_open_observation(
                run_a.head_hash,
                run_b.rows[0].row_hash,
                _open_from(second),
                observation_id="mixed-parent",
                now=OPEN_NOW,
            )

        opened = _open_from(first)
        msft_legs = tuple(replace(leg, underlying="MSFT") for leg in opened.legs)
        top_level_drift = replace(
            opened,
            underlying="MSFT",
            legs=msft_legs,
            strategy_hash=strategy_structure_hash("MSFT", opened.strategy_type, msft_legs),
        )
        with pytest.raises(NewsPreselectionStoreConflict, match="frozen"):
            store.append_open_observation(
                run_a.head_hash,
                run_a.rows[0].row_hash,
                top_level_drift,
                observation_id="symbol-drift",
                now=OPEN_NOW,
            )

        strategy_drift = replace(
            opened,
            strategy_type="CALENDAR",
            strategy_hash=strategy_structure_hash(
                opened.underlying, "CALENDAR", opened.legs
            ),
        )
        with pytest.raises(NewsPreselectionStoreConflict, match="frozen"):
            store.append_open_observation(
                run_a.head_hash,
                run_a.rows[0].row_hash,
                strategy_drift,
                observation_id="strategy-drift",
                now=OPEN_NOW,
            )


def test_stale_and_incomplete_open_observations_are_audited_not_eligible(
    tmp_path: Path,
) -> None:
    premarket = _candidate("audit")
    with NewsPreselectionStore(tmp_path / "audit.sqlite3") as store:
        run = store.append_premarket_run("run", (premarket,), now=PREMARKET_NOW)
        parent = run.rows[0]
        stale = store.append_open_observation(
            run.head_hash,
            parent.row_hash,
            _open_from(premarket, quote_age=timedelta(seconds=6), observation_suffix="c"),
            observation_id="stale",
            now=OPEN_NOW,
        )
        incomplete = store.append_open_observation(
            run.head_hash,
            parent.row_hash,
            _open_from(premarket, incomplete_quotes=True, observation_suffix="d"),
            observation_id="incomplete",
            now=OPEN_NOW,
        )
        assert "QUOTE_STALE" in stale.evaluation["blockers"]
        assert stale.evaluation["quote_batch_id"] == "open-batch-c"
        assert stale.evaluation["oldest_quote_asof"] == (
            OPEN_NOW - timedelta(seconds=6)
        ).isoformat()
        assert stale.evaluation["action_pool_eligible"] is False
        assert any(
            str(reason).startswith("LEG_EXECUTION_FIELD_MISSING")
            for reason in incomplete.evaluation["blockers"]
        )
        assert incomplete.evaluation["action_pool_eligible"] is False
        assert store.read_open_observations("run") == (stale, incomplete)


def test_restart_replays_run_and_all_observations_and_latest_is_per_row(
    tmp_path: Path,
) -> None:
    path = tmp_path / "restart.sqlite3"
    first = _candidate("first")
    second = _candidate(
        "second", con_id_offset=10, strike_offset=Decimal("10")
    )
    with NewsPreselectionStore(path) as store:
        run = store.append_premarket_run(
            "run-replay", (first, second), now=PREMARKET_NOW
        )
        parents = {item.preselection_id: item for item in run.rows}
        store.append_open_observation(
            run.head_hash,
            parents["first"].row_hash,
            _open_from(first, observation_suffix="e"),
            observation_id="first-old",
            now=OPEN_NOW,
        )
        store.append_open_observation(
            run.head_hash,
            parents["second"].row_hash,
            _open_from(second, observation_suffix="f"),
            observation_id="second-only",
            now=OPEN_NOW,
        )
        store.append_open_observation(
            run.head_hash,
            parents["first"].row_hash,
            _open_from(first, observation_suffix="g"),
            observation_id="first-new",
            now=OPEN_NOW + timedelta(seconds=1),
        )

    with NewsPreselectionStore(path) as reopened:
        assert reopened.verify_integrity() is True
        assert reopened.latest_premarket().run_id == "run-replay"  # type: ignore[union-attr]
        replay = reopened.replay("run-replay")
        assert [item.observation_id for item in replay.open_observations] == [
            "first-old",
            "second-only",
            "first-new",
        ]
        latest = reopened.latest_open()
        assert {item.observation_id for item in latest} == {
            "first-new",
            "second-only",
        }
        assert reopened.read_run(head_hash=replay.premarket.head_hash).run_id == (
            "run-replay"
        )


def test_complete_open_economics_round_trip_through_restart_replay_and_provider(
    tmp_path: Path,
) -> None:
    path = tmp_path / "economics-round-trip.sqlite3"
    premarket = _candidate("economics")
    opened = _open_with_complete_economics(premarket)
    expected_document = opened.as_dict()

    with NewsPreselectionStore(path) as store:
        run = store.append_premarket_run(
            "economics-run", (premarket,), now=PREMARKET_NOW
        )
        store.append_open_batch(
            run.head_hash,
            (opened,),
            batch_id="economics-open-batch",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
        )

    with NewsPreselectionStore(path) as reopened:
        replay = reopened.replay("economics-run")
        assert canonical_json(replay.open_observations[0].candidate) == canonical_json(
            expected_document
        )

        snapshot = LedgerBackedPreselectionProvider(reopened).read_snapshot()
        restored = next(
            candidate
            for candidate in snapshot.preselections
            if candidate.phase is PreselectionPhase.OPEN_REPRICED
        )
        assert restored == opened
        assert restored.terminal_scenarios == opened.terminal_scenarios
        assert all(
            isinstance(scenario.terminal_underlying_price, Decimal)
            and isinstance(scenario.probability, Decimal)
            for scenario in restored.terminal_scenarios
        )
        assert restored.scenario_asof == opened.scenario_asof
        assert restored.economics_quote_asof == opened.economics_quote_asof
        for field in _OPTIONAL_ECONOMICS_FIELDS:
            assert getattr(restored, field) == getattr(opened, field)


def test_restart_reads_legacy_candidate_documents_without_economics_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "legacy-candidate-document.sqlite3"
    premarket = _candidate("legacy")
    opened = _open_from(premarket)
    original_as_dict = ConditionalOptionPreselection.as_dict

    def legacy_as_dict(self: ConditionalOptionPreselection) -> dict[str, object]:
        document = original_as_dict(self)
        for field in _OPTIONAL_ECONOMICS_FIELDS:
            document.pop(field)
        return document

    with monkeypatch.context() as scoped:
        scoped.setattr(ConditionalOptionPreselection, "as_dict", legacy_as_dict)
        with NewsPreselectionStore(path) as store:
            run = store.append_premarket_run(
                "legacy-run", (premarket,), now=PREMARKET_NOW
            )
            store.append_open_batch(
                run.head_hash,
                (opened,),
                batch_id="legacy-open-batch",
                scheduled_for=OPEN_NOW,
                observed_at=OPEN_NOW,
            )

    with NewsPreselectionStore(path) as reopened:
        replay = reopened.replay("legacy-run")
        assert all(
            field not in replay.open_observations[0].candidate
            for field in _OPTIONAL_ECONOMICS_FIELDS
        )

        snapshot = LedgerBackedPreselectionProvider(reopened).read_snapshot()
        assert snapshot.preselections == (premarket, opened)
        for candidate in snapshot.preselections:
            assert candidate.terminal_scenarios == ()
            for field in _OPTIONAL_ECONOMICS_FIELDS[1:]:
                assert getattr(candidate, field) is None


@pytest.mark.parametrize(
    ("field", "tampered_value"),
    (
        ("economics_calculation_hash", "f" * 64),
        ("economics_quote_batch_id", "tampered-quote-batch"),
    ),
)
def test_economics_projection_tamper_is_rejected_on_restart(
    tmp_path: Path,
    field: str,
    tampered_value: str,
) -> None:
    path = tmp_path / f"economics-projection-tamper-{field}.sqlite3"
    premarket = _candidate("tamper-economics")
    opened = _open_with_complete_economics(premarket)
    with NewsPreselectionStore(path) as store:
        run = store.append_premarket_run(
            "tamper-economics-run", (premarket,), now=PREMARKET_NOW
        )
        store.append_open_batch(
            run.head_hash,
            (opened,),
            batch_id="tamper-economics-open-batch",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
        )

    raw = sqlite3.connect(path)
    try:
        raw.execute("DROP TRIGGER open_observations_no_update")
        document = json.loads(
            raw.execute("SELECT candidate_json FROM open_observations").fetchone()[0]
        )
        document[field] = tampered_value
        raw.execute(
            "UPDATE open_observations SET candidate_json=?",
            (json.dumps(document, sort_keys=True, separators=(",", ":")),),
        )
        raw.commit()
    finally:
        raw.close()

    with pytest.raises(
        NewsPreselectionStoreCorruption,
        match="open observation immutable parent binding mismatch",
    ):
        NewsPreselectionStore(path)


def test_ledger_provider_empty_store_is_readable_but_coverage_unavailable(
    tmp_path: Path,
) -> None:
    with NewsPreselectionStore(tmp_path / "empty-provider.sqlite3") as store:
        provider = LedgerBackedPreselectionProvider(store)

        assert provider.health == "READY"
        assert provider.preselections() == ()
        assert provider.lineage() == {}
        assert provider.coverage() == {
            "requested_count": 10,
            "available_count": 0,
            "source": "INDEPENDENT_TOP10_LEDGER",
            "status": "UNAVAILABLE",
            "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
            "ledger_reason": "NO_PREMARKET_LEDGER_RUN",
            "latest_run_id": None,
            "latest_head_hash": None,
            "freeze_slot": None,
            "open_count": 0,
            "latest_open_batch_id": None,
            "latest_open_batch_head_hash": None,
            "reprice_slot": None,
            "open_reprice_producer_status": "UNAVAILABLE",
            "open_reprice_writer": "INDEPENDENT_TOP10_PRODUCER_V1",
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }


def test_ledger_provider_rebuilds_latest_run_and_open_with_bound_lineage(
    tmp_path: Path,
) -> None:
    first = _candidate("first")
    older = _candidate(
        "older", con_id_offset=50, strike_offset=Decimal("50")
    )
    path = tmp_path / "provider-replay.sqlite3"
    with NewsPreselectionStore(path) as store:
        store.append_premarket_run("older-run", (older,), now=PREMARKET_NOW)
        latest = store.append_premarket_run("latest-run", (first,), now=PREMARKET_NOW)
        opened = _open_from(first)
        batch = store.append_open_batch(
            latest.head_hash,
            (opened,),
            batch_id="latest-open-batch",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
        )
        observation = batch.rows[0]
        provider = LedgerBackedPreselectionProvider(store)

        snapshot = provider.read_snapshot()

        assert snapshot.preselections == (first, opened)
        assert set(snapshot.lineage) == {
            ("first", "PRE_MARKET"),
            ("first", "OPEN_REPRICED"),
        }
        premarket_lineage = snapshot.lineage[("first", "PRE_MARKET")]
        open_lineage = snapshot.lineage[("first", "OPEN_REPRICED")]
        assert premarket_lineage["run_id"] == "latest-run"
        assert premarket_lineage["head_hash"] == latest.head_hash
        assert premarket_lineage["row_hash"] == latest.rows[0].row_hash
        assert premarket_lineage["source_batch_purpose"] is None
        assert premarket_lineage["source_batch_id"] is None
        assert premarket_lineage["source_batch_hash"] is None
        assert open_lineage["run_id"] == "latest-run"
        assert open_lineage["head_hash"] == latest.head_hash
        assert open_lineage["row_hash"] == latest.rows[0].row_hash
        assert open_lineage["observation_hash"] == observation.observation_hash
        assert open_lineage["batch_id"] == "latest-open-batch"
        assert open_lineage["batch_head_hash"] == batch.head_hash
        assert open_lineage["scheduled_for"] == OPEN_NOW.isoformat()
        assert open_lineage["quote_batch_id"] == "open-batch-b"
        assert open_lineage["source_batch_purpose"] is None
        assert open_lineage["source_batch_id"] is None
        assert open_lineage["source_batch_hash"] is None
        assert "structure_identity" not in premarket_lineage
        assert snapshot.coverage["status"] == "PARTIAL"
        assert snapshot.coverage["reason"] == "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
        assert snapshot.coverage["ledger_reason"] == (
            "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
        )
        assert snapshot.coverage["open_count"] == 1
        assert snapshot.coverage["open_reprice_producer_status"] == "AVAILABLE"
        assert snapshot.coverage["latest_open_batch_id"] == "latest-open-batch"
        assert snapshot.coverage["latest_open_batch_head_hash"] == batch.head_hash
        assert snapshot.coverage["freeze_slot"] == PREMARKET_NOW.isoformat()
        assert snapshot.coverage["reprice_slot"] == OPEN_NOW.isoformat()


def test_ledger_provider_required_external_source_lineage_accepts_complete_triples(
    tmp_path: Path,
) -> None:
    premarket = _candidate("strict-source")
    with NewsPreselectionStore(tmp_path / "strict-source-complete.sqlite3") as store:
        run = store.append_premarket_run(
            "strict-source-run",
            (premarket,),
            now=PREMARKET_NOW,
            source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
            source_batch_id=PREMARKET_SOURCE_BATCH_ID,
            source_batch_hash=PREMARKET_SOURCE_BATCH_HASH,
        )
        store.append_open_batch(
            run.head_hash,
            (_open_from(premarket),),
            batch_id="strict-source-open",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
            source_batch_purpose=OPEN_REPRICE_PURPOSE,
            source_batch_id=OPEN_SOURCE_BATCH_ID,
            source_batch_hash=OPEN_SOURCE_BATCH_HASH,
        )

        snapshot = LedgerBackedPreselectionProvider(
            store,
            require_external_source_lineage=True,
        ).read_snapshot()

        assert snapshot.preselections == (premarket, _open_from(premarket))
        assert ("strict-source", "OPEN_REPRICED") in snapshot.lineage
        premarket_lineage = snapshot.lineage[("strict-source", "PRE_MARKET")]
        open_lineage = snapshot.lineage[("strict-source", "OPEN_REPRICED")]
        assert premarket_lineage["source_batch_purpose"] == PREMARKET_ACCOUNT_PURPOSE
        assert premarket_lineage["source_batch_id"] == PREMARKET_SOURCE_BATCH_ID
        assert premarket_lineage["source_batch_hash"] == PREMARKET_SOURCE_BATCH_HASH
        assert open_lineage["source_batch_purpose"] == OPEN_REPRICE_PURPOSE
        assert open_lineage["source_batch_id"] == OPEN_SOURCE_BATCH_ID
        assert open_lineage["source_batch_hash"] == OPEN_SOURCE_BATCH_HASH
        assert snapshot.coverage["open_count"] == 1
        assert snapshot.coverage["ledger_reason"] == (
            "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
        )


@pytest.mark.parametrize("missing_source", ("premarket", "open"))
def test_ledger_provider_required_external_source_lineage_hides_unbound_open_rows(
    tmp_path: Path,
    missing_source: str,
) -> None:
    premarket = _candidate(f"strict-missing-{missing_source}")
    premarket_binding = (
        {}
        if missing_source == "premarket"
        else {
            "source_batch_purpose": PREMARKET_ACCOUNT_PURPOSE,
            "source_batch_id": PREMARKET_SOURCE_BATCH_ID,
            "source_batch_hash": PREMARKET_SOURCE_BATCH_HASH,
        }
    )
    open_binding = (
        {}
        if missing_source == "open"
        else {
            "source_batch_purpose": OPEN_REPRICE_PURPOSE,
            "source_batch_id": OPEN_SOURCE_BATCH_ID,
            "source_batch_hash": OPEN_SOURCE_BATCH_HASH,
        }
    )
    with NewsPreselectionStore(
        tmp_path / f"strict-source-missing-{missing_source}.sqlite3"
    ) as store:
        run = store.append_premarket_run(
            f"strict-missing-{missing_source}-run",
            (premarket,),
            now=PREMARKET_NOW,
            **premarket_binding,
        )
        store.append_open_batch(
            run.head_hash,
            (_open_from(premarket),),
            batch_id=f"strict-missing-{missing_source}-open",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
            **open_binding,
        )

        default_snapshot = LedgerBackedPreselectionProvider(store).read_snapshot()
        strict_snapshot = LedgerBackedPreselectionProvider(
            store,
            require_external_source_lineage=True,
        ).read_snapshot()

        assert any(
            candidate.phase is PreselectionPhase.OPEN_REPRICED
            for candidate in default_snapshot.preselections
        )
        assert all(
            candidate.phase is PreselectionPhase.PRE_MARKET
            for candidate in strict_snapshot.preselections
        )
        assert not any(
            key[1] == "OPEN_REPRICED" for key in strict_snapshot.lineage
        )
        assert strict_snapshot.coverage["status"] == "UNAVAILABLE"
        assert strict_snapshot.coverage["ledger_reason"] == (
            "EXTERNAL_SOURCE_LINEAGE_INCOMPLETE"
        )
        assert strict_snapshot.coverage["open_count"] == 0


def test_ledger_provider_never_projects_partial_single_open_rows_to_action_pool(
    tmp_path: Path,
) -> None:
    first = _candidate("first")
    second = _candidate("second", con_id_offset=10, strike_offset=Decimal("10"))
    with NewsPreselectionStore(tmp_path / "single-open.sqlite3") as store:
        run = store.append_premarket_run(
            "run", (first, second), now=PREMARKET_NOW
        )
        store.append_open_observation(
            run.head_hash,
            run.rows[0].row_hash,
            _open_from(first),
            observation_id="legacy-single",
            now=OPEN_NOW,
        )

        snapshot = LedgerBackedPreselectionProvider(store).read_snapshot()
        _, repriced, action_pool = build_preselection_pools(
            snapshot.preselections,
            now=OPEN_NOW,
        )

        assert repriced == ()
        assert action_pool == ()
        assert snapshot.coverage["status"] == "UNAVAILABLE"
        assert snapshot.coverage["ledger_reason"] == (
            "OPEN_REPRICE_ATOMIC_BATCH_INCOMPLETE"
        )
        assert snapshot.coverage["open_count"] == 0


def test_ledger_provider_reports_open_reprice_producer_pending(tmp_path: Path) -> None:
    with NewsPreselectionStore(tmp_path / "premarket-only.sqlite3") as store:
        run = store.append_premarket_run(
            "premarket-only", (_candidate("pending-open"),), now=PREMARKET_NOW
        )
        provider = LedgerBackedPreselectionProvider(store)

        assert store.latest_replay().premarket.head_hash == run.head_hash  # type: ignore[union-attr]
        assert provider.preselections() == (_candidate("pending-open"),)
        coverage = provider.coverage()
        assert coverage["status"] == "PARTIAL"
        assert coverage["reason"] == "OPEN_REPRICE_NOT_STARTED"
        assert coverage["open_count"] == 0
        assert coverage["open_reprice_producer_status"] == "NOT_STARTED"
        assert coverage["open_reprice_writer"] == (
            "INDEPENDENT_TOP10_PRODUCER_V1"
        )


def test_ledger_provider_fails_closed_when_projection_is_corrupted(
    tmp_path: Path,
) -> None:
    path = tmp_path / "provider-corrupt.sqlite3"
    with NewsPreselectionStore(path) as store:
        store.append_premarket_run("run", (_candidate("pre"),), now=PREMARKET_NOW)
        provider = LedgerBackedPreselectionProvider(store)
        raw = sqlite3.connect(path)
        try:
            raw.execute("DROP TRIGGER premarket_rows_no_update")
            raw.execute("UPDATE premarket_rows SET candidate_json='{}'")
            raw.commit()
        finally:
            raw.close()

        with pytest.raises(NewsPreselectionStoreCorruption):
            provider.preselections()
        assert provider.health == "DEGRADED"
        assert provider.coverage()["status"] == "UNAVAILABLE"
        assert provider.coverage()["reason"] == "PRESELECTION_LEDGER_UNREADABLE"


def test_chain_tamper_is_detected_on_restart(tmp_path: Path) -> None:
    path = tmp_path / "tamper.sqlite3"
    with NewsPreselectionStore(path) as store:
        store.append_premarket_run("run", (_candidate("pre"),), now=PREMARKET_NOW)

    raw = sqlite3.connect(path)
    try:
        raw.execute("DROP TRIGGER ledger_entries_no_update")
        raw.execute("UPDATE ledger_entries SET payload_json='{}' WHERE sequence=1")
        raw.commit()
    finally:
        raw.close()
    with pytest.raises(NewsPreselectionStoreCorruption, match="content hash"):
        NewsPreselectionStore(path)


def test_store_exposes_no_authority_api_or_authority_imports(tmp_path: Path) -> None:
    forbidden_methods = {
        "promote",
        "authorize",
        "approve",
        "reserve",
        "create_instruction",
        "submit_order",
    }
    assert forbidden_methods.isdisjoint(dir(NewsPreselectionStore))
    source = inspect.getsource(preselection_store_module)
    for forbidden_import in (
        "options_copilot.ranking.store",
        "options_copilot.ranking.portfolio",
        "options_copilot.approval",
        "options_copilot.bridge",
    ):
        assert forbidden_import not in source

    with NewsPreselectionStore(tmp_path / "authority.sqlite3") as store:
        run = store.append_premarket_run(
            "run", (_candidate("pre"),), now=PREMARKET_NOW
        )
        observation = store.append_open_observation(
            run.head_hash,
            run.rows[0].row_hash,
            _open_from(_candidate("pre")),
            observation_id="open",
            now=OPEN_NOW,
        )
        for read_model in (
            run.as_dict(),
            run.rows[0].as_dict(),
            observation.as_dict(),
            store.replay("run").as_dict(),
        ):
            assert read_model["decision_authority"] == "SUPPORTING_ONLY"
            assert read_model["approval_eligible"] is False
            assert read_model["instruction_creation_allowed"] is False
            assert read_model["order_allowed"] is False


def test_open_batch_is_atomic_replayable_and_exposes_typed_contract_refs(
    tmp_path: Path,
) -> None:
    path = tmp_path / "open-batch.sqlite3"
    first = _candidate("first")
    second = _candidate("second", con_id_offset=10, strike_offset=Decimal("10"))
    with NewsPreselectionStore(path) as store:
        run = store.append_premarket_run("run", (first, second), now=PREMARKET_NOW)
        assert all(row.production_parent_eligible for row in run.rows)
        assert {ref.con_id for row in run.rows for ref in row.contract_refs} == {
            1001, 1002, 1011, 1012
        }
        batch = store.append_open_batch(
            run.head_hash,
            (
                _open_with_complete_economics(first),
                _open_with_complete_economics(second),
            ),
            batch_id="open-0935",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
        )
        assert batch.action_pool_eligible
        assert len(batch.rows) == 2
        assert {row.batch_head_hash for row in batch.rows} == {batch.head_hash}
        assert store.verify_integrity()

    with NewsPreselectionStore(path) as reopened:
        replayed = reopened.read_open_batch("open-0935")
        assert replayed.head_hash == batch.head_hash
        assert [row.preselection_id for row in replayed.rows] == ["first", "second"]


def test_open_batch_rejects_incomplete_duplicate_and_cross_batch_without_writes(
    tmp_path: Path,
) -> None:
    first = _candidate("first")
    second = _candidate("second", con_id_offset=10, strike_offset=Decimal("10"))
    with NewsPreselectionStore(tmp_path / "batch-reject.sqlite3") as store:
        run = store.append_premarket_run("run", (first, second), now=PREMARKET_NOW)
        with pytest.raises(NewsPreselectionStoreConflict, match="exactly match"):
            store.append_open_batch(
                run.head_hash,
                (_open_from(first),),
                batch_id="missing",
                scheduled_for=OPEN_NOW,
                observed_at=OPEN_NOW,
            )
        with pytest.raises(ValueError, match="unique"):
            store.append_open_batch(
                run.head_hash,
                (_open_from(first), _open_from(first)),
                batch_id="duplicate",
                scheduled_for=OPEN_NOW,
                observed_at=OPEN_NOW,
            )
        opened_second = _open_from(second)
        cross_batch = replace(
            opened_second,
            legs=tuple(
                replace(leg, quote_batch_id="different") for leg in opened_second.legs
            ),
        )
        with pytest.raises(ValueError, match="quote_batch_id"):
            store.append_open_batch(
                run.head_hash,
                (_open_from(first), cross_batch),
                batch_id="cross-batch",
                scheduled_for=OPEN_NOW,
                observed_at=OPEN_NOW,
            )
        assert store.latest_open_batch() is None
        assert store.read_open_observations("run") == ()


def test_open_batch_nth_row_failure_rolls_back_rows_and_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _candidate("first")
    second = _candidate("second", con_id_offset=10, strike_offset=Decimal("10"))
    with NewsPreselectionStore(tmp_path / "batch-rollback.sqlite3") as store:
        run = store.append_premarket_run("run", (first, second), now=PREMARKET_NOW)
        original = store._insert_entry
        calls = 0

        def fail_second(entry):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected row failure")
            original(entry)

        monkeypatch.setattr(store, "_insert_entry", fail_second)
        with pytest.raises(RuntimeError, match="injected"):
            store.append_open_batch(
                run.head_hash,
                (_open_from(first), _open_from(second)),
                batch_id="rollback",
                scheduled_for=OPEN_NOW,
                observed_at=OPEN_NOW,
            )
        assert store.latest_open_batch() is None
        assert store.read_open_observations("run") == ()
        assert store.verify_integrity()


@pytest.mark.parametrize(
    ("table", "identifier_column", "column", "tampered_value"),
    (
        ("premarket_runs", "run_id", "source_batch_hash", "c" * 64),
        ("open_batches", "batch_id", "source_batch_hash", "c" * 64),
        (
            "premarket_runs",
            "run_id",
            "source_batch_purpose",
            OPEN_REPRICE_PURPOSE,
        ),
        (
            "open_batches",
            "batch_id",
            "source_batch_purpose",
            PREMARKET_ACCOUNT_PURPOSE,
        ),
    ),
)
def test_source_batch_projection_tamper_is_rejected_on_restart(
    tmp_path: Path,
    table: str,
    identifier_column: str,
    column: str,
    tampered_value: str,
) -> None:
    path = tmp_path / f"source-lineage-tamper-{table}-{column}.sqlite3"
    premarket = _candidate("source-tamper")
    with NewsPreselectionStore(path) as store:
        run = store.append_premarket_run(
            "source-tamper-run",
            (premarket,),
            now=PREMARKET_NOW,
            source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
            source_batch_id=PREMARKET_SOURCE_BATCH_ID,
            source_batch_hash=PREMARKET_SOURCE_BATCH_HASH,
        )
        store.append_open_batch(
            run.head_hash,
            (_open_from(premarket),),
            batch_id="source-tamper-open",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
            source_batch_purpose=OPEN_REPRICE_PURPOSE,
            source_batch_id=OPEN_SOURCE_BATCH_ID,
            source_batch_hash=OPEN_SOURCE_BATCH_HASH,
        )

    raw = sqlite3.connect(path)
    blocked_by_sql_constraint = column == "source_batch_purpose"
    try:
        raw.execute(f"DROP TRIGGER {table}_no_update")
        if blocked_by_sql_constraint:
            with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
                raw.execute(
                    f"UPDATE {table} SET {column}=? "
                    f"WHERE {identifier_column} IS NOT NULL",
                    (tampered_value,),
                )
            raw.rollback()
        else:
            raw.execute(
                f"UPDATE {table} SET {column}=? "
                f"WHERE {identifier_column} IS NOT NULL",
                (tampered_value,),
            )
            raw.commit()
    finally:
        raw.close()

    if blocked_by_sql_constraint:
        with NewsPreselectionStore(path) as reopened:
            assert reopened.verify_integrity()
        return

    with pytest.raises(
        NewsPreselectionStoreCorruption,
        match="immutable document|stored source batch lineage",
    ):
        NewsPreselectionStore(path)


def _downgrade_v3_database_to_v2(path: Path) -> None:
    """Build a true v2 projection around an unchanged valid legacy chain."""

    raw = sqlite3.connect(path)
    raw.row_factory = sqlite3.Row
    try:
        head = raw.execute(
            "SELECT * FROM ledger_entries WHERE entry_type='PREMARKET_HEAD'"
        ).fetchone()
        assert head is not None
        payload = json.loads(str(head["payload_json"]))
        payload["schema"] = "options_copilot.news_premarket_head.v2"
        payload.pop("source_batch_purpose")
        payload.pop("source_batch_id")
        payload.pop("source_batch_hash")
        payload_json = canonical_json(payload)
        content_hash = canonical_hash(payload)
        entry_hash = preselection_store_module._chain_hash(
            int(head["sequence"]),
            str(head["entry_type"]),
            str(head["record_id"]),
            content_hash,
            str(head["previous_hash"]),
        )

        raw.execute("DROP TRIGGER ledger_entries_no_update")
        raw.execute("DROP TRIGGER premarket_runs_no_update")
        raw.execute(
            "UPDATE ledger_entries SET payload_json=?,content_hash=?,entry_hash=? "
            "WHERE sequence=?",
            (payload_json, content_hash, entry_hash, head["sequence"]),
        )
        raw.execute(
            "UPDATE premarket_runs SET content_hash=?,head_hash=?",
            (content_hash, entry_hash),
        )
        raw.commit()

        raw.execute("PRAGMA foreign_keys=OFF")
        raw.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE premarket_runs_v2 (
                run_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                requested_count INTEGER NOT NULL CHECK(requested_count=10),
                available_count INTEGER NOT NULL CHECK(available_count BETWEEN 0 AND 10),
                head_sequence INTEGER NOT NULL UNIQUE,
                head_hash TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL,
                UNIQUE(run_id,head_hash),
                FOREIGN KEY(head_sequence) REFERENCES ledger_entries(sequence),
                FOREIGN KEY(head_hash) REFERENCES ledger_entries(entry_hash)
            );
            INSERT INTO premarket_runs_v2
                SELECT run_id,created_at,requested_count,available_count,
                       head_sequence,head_hash,content_hash
                FROM premarket_runs;
            DROP TABLE premarket_runs;
            ALTER TABLE premarket_runs_v2 RENAME TO premarket_runs;

            CREATE TABLE open_batches_v2 (
                batch_id TEXT PRIMARY KEY,
                parent_run_id TEXT NOT NULL,
                parent_head_hash TEXT NOT NULL,
                scheduled_for TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                quote_batch_id TEXT NOT NULL,
                quote_asof TEXT NOT NULL,
                available_count INTEGER NOT NULL CHECK(available_count BETWEEN 0 AND 10),
                blockers_json TEXT NOT NULL,
                head_sequence INTEGER NOT NULL UNIQUE,
                head_hash TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL,
                FOREIGN KEY(parent_run_id,parent_head_hash)
                    REFERENCES premarket_runs(run_id,head_hash),
                FOREIGN KEY(head_sequence) REFERENCES ledger_entries(sequence),
                FOREIGN KEY(head_hash) REFERENCES ledger_entries(entry_hash)
            );
            INSERT INTO open_batches_v2
                SELECT batch_id,parent_run_id,parent_head_hash,scheduled_for,
                       observed_at,quote_batch_id,quote_asof,available_count,
                       blockers_json,head_sequence,head_hash,content_hash
                FROM open_batches;
            DROP TABLE open_batches;
            ALTER TABLE open_batches_v2 RENAME TO open_batches;
            CREATE INDEX open_batches_parent_idx
                ON open_batches(parent_run_id,head_sequence);
            PRAGMA user_version=2;
            COMMIT;
            """
        )
        raw.execute("PRAGMA foreign_keys=ON")
        assert raw.execute("PRAGMA foreign_key_check").fetchone() is None
    finally:
        raw.close()


def test_v2_migration_preserves_legacy_chain_and_restores_null_source_lineage(
    tmp_path: Path,
) -> None:
    path = tmp_path / "migrate-v2.sqlite3"
    with NewsPreselectionStore(path) as store:
        store.append_premarket_run(
            "legacy-v2-run", (_candidate("legacy-v2-parent"),), now=PREMARKET_NOW
        )
    _downgrade_v3_database_to_v2(path)

    raw = sqlite3.connect(path)
    try:
        assert raw.execute("PRAGMA user_version").fetchone()[0] == 2
        before = raw.execute(
            "SELECT sequence,payload_json,content_hash,previous_hash,entry_hash "
            "FROM ledger_entries ORDER BY sequence"
        ).fetchall()
        legacy_columns = {
            row[1] for row in raw.execute("PRAGMA table_info(premarket_runs)")
        }
        assert "source_batch_purpose" not in legacy_columns
        assert "source_batch_id" not in legacy_columns
        assert "source_batch_hash" not in legacy_columns
    finally:
        raw.close()

    with NewsPreselectionStore(path) as migrated:
        assert migrated.schema_version == 3
        assert migrated.verify_integrity()
        restored = migrated.latest_premarket()
        assert restored is not None
        assert restored.source_batch_purpose is None
        assert restored.source_batch_id is None
        assert restored.source_batch_hash is None
        strict_snapshot = LedgerBackedPreselectionProvider(
            migrated,
            require_external_source_lineage=True,
        ).read_snapshot()
        assert strict_snapshot.coverage["status"] == "UNAVAILABLE"
        assert strict_snapshot.coverage["ledger_reason"] == (
            "EXTERNAL_SOURCE_LINEAGE_INCOMPLETE"
        )
        assert not any(
            key[1] == "OPEN_REPRICED" for key in strict_snapshot.lineage
        )

    check = sqlite3.connect(path)
    try:
        after = check.execute(
            "SELECT sequence,payload_json,content_hash,previous_hash,entry_hash "
            "FROM ledger_entries ORDER BY sequence"
        ).fetchall()
    finally:
        check.close()
    assert after == before


def test_v1_migration_preserves_existing_hash_chain(tmp_path: Path) -> None:
    path = tmp_path / "migrate-v1.sqlite3"
    with NewsPreselectionStore(path) as store:
        store.append_premarket_run("run", (_candidate("parent"),), now=PREMARKET_NOW)
    _downgrade_v3_database_to_v2(path)
    raw = sqlite3.connect(path)
    try:
        before = raw.execute(
            "SELECT sequence,payload_json,content_hash,previous_hash,entry_hash "
            "FROM ledger_entries ORDER BY sequence"
        ).fetchall()
        raw.execute("PRAGMA foreign_keys=OFF")
        raw.executescript(
            """
            DROP TABLE open_observations;
            DROP TABLE open_batches;
            CREATE TABLE open_observations (
                observation_id TEXT PRIMARY KEY,
                parent_run_id TEXT NOT NULL,
                parent_head_hash TEXT NOT NULL,
                parent_row_hash TEXT NOT NULL,
                premarket_rank INTEGER NOT NULL CHECK(premarket_rank BETWEEN 1 AND 10),
                preselection_id TEXT NOT NULL,
                strategy_hash TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                structure_json TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                evaluation_json TEXT NOT NULL,
                quote_batch_id TEXT,
                oldest_quote_asof TEXT,
                entry_sequence INTEGER NOT NULL UNIQUE,
                observation_hash TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL
            );
            PRAGMA user_version=1;
            """
        )
        raw.commit()
    finally:
        raw.close()

    with NewsPreselectionStore(path) as migrated:
        assert migrated.schema_version == 3
        assert migrated.verify_integrity()
        restored = migrated.latest_premarket()
        assert restored is not None
        assert restored.source_batch_purpose is None
        assert restored.source_batch_id is None
        assert restored.source_batch_hash is None
    check = sqlite3.connect(path)
    try:
        after = check.execute(
            "SELECT sequence,payload_json,content_hash,previous_hash,entry_hash "
            "FROM ledger_entries ORDER BY sequence"
        ).fetchall()
    finally:
        check.close()
    assert after == before


def test_future_schema_version_is_rejected_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "future-schema.sqlite3"
    with NewsPreselectionStore(path):
        pass
    raw = sqlite3.connect(path)
    try:
        raw.execute("PRAGMA user_version=99")
        raw.commit()
        before = {
            str(row[0])
            for row in raw.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        }
    finally:
        raw.close()

    with pytest.raises(RuntimeError, match="newer than supported"):
        NewsPreselectionStore(path)

    check = sqlite3.connect(path)
    try:
        assert check.execute("PRAGMA user_version").fetchone()[0] == 99
        after = {
            str(row[0])
            for row in check.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        }
    finally:
        check.close()
    assert after == before
