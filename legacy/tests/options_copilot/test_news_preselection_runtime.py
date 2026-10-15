from __future__ import annotations

import asyncio
from dataclasses import fields
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from options_copilot.config import OptionsCopilotConfig
from options_copilot.api import create_app
from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
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
from options_copilot.news.preselection import strategy_structure_hash
from options_copilot.news.preselection_store import NewsPreselectionStoreCorruption
from options_copilot.runtime import OptionsCopilotRuntime, RuntimeServices
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.storage.canonical import canonical_hash


PREMARKET_NOW = datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc)
OPEN_NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _isolate_preselection_tests_from_live_event_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep ledger tests deterministic and independent of public HTTP feeds."""

    import options_copilot.runtime as runtime_module

    monkeypatch.setattr(
        runtime_module,
        "_configured_event_providers",
        lambda _config: ((), ()),
    )
    monkeypatch.setattr(
        runtime_module,
        "build_official_calendar_provider",
        lambda: None,
    )


def _config(tmp_path: Path) -> OptionsCopilotConfig:
    return OptionsCopilotConfig(data_dir=tmp_path / "data", log_dir=tmp_path / "logs")


def _services() -> RuntimeServices:
    return RuntimeServices(**{field.name: None for field in fields(RuntimeServices)})


def _candidate(
    identifier: str,
    *,
    phase: PreselectionPhase = PreselectionPhase.PRE_MARKET,
    quote_time: datetime = PREMARKET_NOW,
) -> ConditionalOptionPreselection:
    leg = ConditionalOptionLeg(
        underlying="AAPL",
        con_id=81001,
        local_symbol="AAPL  260821C00225000",
        trading_class="AAPL",
        multiplier=100,
        exchange="SMART",
        expiry=date(2026, 8, 21),
        strike=Decimal("225"),
        right=OptionRight.CALL,
        side=OptionLegSide.BUY,
        ratio=1,
        quantity=1,
        bid=Decimal("2.10"),
        ask=Decimal("2.16"),
        quote_asof=quote_time,
        quote_batch_id=(
            "premarket-batch"
            if phase is PreselectionPhase.PRE_MARKET
            else "open-batch"
        ),
        implied_volatility=Decimal("0.31"),
        delta=Decimal("0.42"),
        gamma=Decimal("0.021"),
        theta=Decimal("-0.08"),
        vega=Decimal("0.11"),
        volume=240,
        open_interest=1800,
        dte=16,
    )
    strategy_hash = strategy_structure_hash("AAPL", "LONG_CALL", (leg,))
    economics: dict[str, object] = {}
    if phase is PreselectionPhase.OPEN_REPRICED:
        scenarios = (
            PreselectionTerminalScenario(Decimal("200"), Decimal("0.50")),
            PreselectionTerminalScenario(Decimal("250"), Decimal("0.50")),
        )
        scenario_asof = quote_time - timedelta(minutes=1)
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
        strategy_nav = Decimal("10000")
        maximum_loss = Decimal("216")
        debit = Decimal("204")
        credit = Decimal("0")
        commission = Decimal("2")
        entry_slippage = Decimal("1")
        exit_slippage = Decimal("3")
        total_slippage = entry_slippage + exit_slippage
        all_in_cost = debit - credit + commission + total_slippage
        after_cost_ev = Decimal("48")
        before_cost_ev = after_cost_ev + commission + total_slippage
        snapshot_hash = canonical_hash(
            {"schema": "test.atomic_broker_snapshot.v1", "candidate_id": identifier}
        )
        payoff_hash = canonical_hash(
            {"schema": "test.open_payoff.v1", "candidate_id": identifier}
        )
        nav_hash = strategy_nav_post_hash(
            candidate_id=identifier,
            strategy_hash=strategy_hash,
            snapshot_hash=snapshot_hash,
            strategy_nav_usd=strategy_nav,
        )
        resolved = OpenRepriceEconomics(
            candidate_id=identifier,
            strategy_hash=strategy_hash,
            broker_snapshot_hash=snapshot_hash,
            quote_batch_id="open-batch",
            quote_asof=quote_time,
            scenario_hash=scenario_set.scenario_hash,
            scenario_asof=scenario_asof,
            cost_contract_version=EXECUTION_COST_VERSION,
            cost_contract_hash=EXECUTION_COST_HASH,
            policy_version=INITIAL_POLICY_VERSION,
            policy_hash=INITIAL_POLICY_HASH,
            strategy_nav_usd=strategy_nav,
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
            payoff_hash=payoff_hash,
            risk_fraction=maximum_loss / strategy_nav,
            economics_hash="0" * 64,
        )
        economics = {
            "terminal_scenarios": scenarios,
            "scenario_asof": scenario_asof,
            "scenario_hash": scenario_set.scenario_hash,
            "execution_cost_contract_version": EXECUTION_COST_VERSION,
            "execution_cost_contract_hash": EXECUTION_COST_HASH,
            "risk_policy_version": INITIAL_POLICY_VERSION,
            "risk_policy_hash": INITIAL_POLICY_HASH,
            "broker_snapshot_hash": snapshot_hash,
            "strategy_nav_usd": strategy_nav,
            "strategy_nav_post_hash": nav_hash,
            "economics_quote_batch_id": "open-batch",
            "economics_quote_asof": quote_time,
            "payoff_hash": payoff_hash,
            "economics_calculation_hash": canonical_hash(resolved.hash_payload()),
            "debit_usd": debit,
            "credit_usd": credit,
            "net_entry_cost_usd": all_in_cost,
            "estimated_commission_usd": commission,
            "estimated_entry_slippage_usd": entry_slippage,
            "estimated_exit_slippage_usd": exit_slippage,
            "estimated_slippage_usd": total_slippage,
            "expected_value_before_costs_usd": before_cost_ev,
            "risk_fraction": maximum_loss / strategy_nav,
        }
    return ConditionalOptionPreselection(
        preselection_id=identifier,
        underlying="AAPL",
        strategy_type="LONG_CALL",
        phase=phase,
        legs=(leg,),
        risk_defined=True,
        maximum_loss_usd=Decimal("216"),
        estimated_cost_usd=Decimal("210"),
        cost_after_ev_usd=Decimal("48"),
        entry_condition="Supporting-only conditional entry.",
        invalidation_condition="Research thesis invalidated.",
        profit_target_condition="Human review target.",
        stop_loss_condition="Human review stop.",
        evidence_ids=("evidence-a",),
        evidence_hashes=("a" * 64,),
        strategy_hash=strategy_hash,
        research_summary="Independent Top-10 ledger fixture.",
        **economics,
    )


class _SnapshotProvider:
    health = "READY"

    def __init__(self, snapshot: object) -> None:
        self.snapshot = snapshot

    def read_snapshot(self) -> object:
        return self.snapshot


def _atomic_snapshot(
    *,
    pre_ids: tuple[str, ...] = ("atomic-a", "atomic-b"),
    open_ids: tuple[str, ...] = ("atomic-a", "atomic-b"),
    production_parent_eligible: bool = True,
    source_lineage: bool = True,
) -> SimpleNamespace:
    run_id = "run-atomic"
    head_hash = "a" * 64
    batch_id = "ledger-open-batch"
    batch_head_hash = "f" * 64
    row_hashes = {
        identifier: chr(ord("b") + index) * 64
        for index, identifier in enumerate(pre_ids)
    }
    preselections = tuple(_candidate(identifier) for identifier in pre_ids) + tuple(
        _candidate(
            identifier,
            phase=PreselectionPhase.OPEN_REPRICED,
            quote_time=OPEN_NOW,
        )
        for identifier in open_ids
    )
    lineage: dict[tuple[str, str], dict[str, object]] = {}
    for rank, identifier in enumerate(pre_ids, start=1):
        lineage[(identifier, PreselectionPhase.PRE_MARKET.value)] = {
            "source": "INDEPENDENT_TOP10_LEDGER",
            "source_batch_purpose": (
                "PREMARKET_ACCOUNT" if source_lineage else None
            ),
            "source_batch_id": (
                "premarket-account-batch" if source_lineage else None
            ),
            "source_batch_hash": "1" * 64 if source_lineage else None,
            "preselection_id": identifier,
            "phase": PreselectionPhase.PRE_MARKET.value,
            "run_id": run_id,
            "run_created_at": PREMARKET_NOW.isoformat(),
            "head_hash": head_hash,
            "row_id": f"row-{identifier}",
            "row_hash": row_hashes[identifier],
            "premarket_rank": rank,
            "production_parent_eligible": production_parent_eligible,
            "production_parent_blocker": (
                None
                if production_parent_eligible
                else "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
            ),
        }
    for index, identifier in enumerate(open_ids, start=1):
        parent = lineage[(identifier, PreselectionPhase.PRE_MARKET.value)]
        lineage[(identifier, PreselectionPhase.OPEN_REPRICED.value)] = {
            "source": "INDEPENDENT_TOP10_LEDGER",
            "source_batch_purpose": "OPEN_REPRICE" if source_lineage else None,
            "source_batch_id": "open-batch" if source_lineage else None,
            "source_batch_hash": "2" * 64 if source_lineage else None,
            "preselection_id": identifier,
            "phase": PreselectionPhase.OPEN_REPRICED.value,
            "run_id": run_id,
            "run_created_at": PREMARKET_NOW.isoformat(),
            "head_hash": head_hash,
            "row_id": parent["row_id"],
            "row_hash": parent["row_hash"],
            "premarket_rank": parent["premarket_rank"],
            "observation_id": f"observation-{identifier}",
            "observed_at": OPEN_NOW.isoformat(),
            "observation_hash": chr(ord("d") + index - 1) * 64,
            "batch_id": batch_id,
            "batch_head_hash": batch_head_hash,
            "scheduled_for": OPEN_NOW.isoformat(),
            "quote_batch_id": "open-batch",
        }
    coverage = {
        "requested_count": 10,
        "available_count": len(pre_ids),
        "source": "INDEPENDENT_TOP10_LEDGER",
        "status": "AVAILABLE" if len(pre_ids) == 10 else "PARTIAL",
        "reason": None if len(pre_ids) == 10 else "TOP10_PREMARKET_COVERAGE_INCOMPLETE",
        "ledger_reason": (
            None if len(pre_ids) == 10 else "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
        ),
        "latest_run_id": run_id,
        "latest_head_hash": head_hash,
        "freeze_slot": PREMARKET_NOW.isoformat(),
        "open_count": len(open_ids),
        "latest_open_batch_id": batch_id if open_ids else None,
        "latest_open_batch_head_hash": batch_head_hash if open_ids else None,
        "reprice_slot": OPEN_NOW.isoformat() if open_ids else None,
        "open_reprice_producer_status": "AVAILABLE" if open_ids else "NOT_STARTED",
        "open_reprice_writer": "IBKR_READONLY_OPEN_REPRICE_PRODUCER",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    return SimpleNamespace(
        preselections=preselections,
        lineage=lineage,
        coverage=coverage,
    )


def test_runtime_owns_independent_ledger_and_restores_same_lineage_after_restart(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    premarket = _candidate("pre-1")
    runtime = OptionsCopilotRuntime(config, runtime_services=_services())
    try:
        run = runtime.news_preselection_store.append_premarket_run(
            "run-1",
            (premarket,),
            now=PREMARKET_NOW,
            source_batch_purpose=PREMARKET_ACCOUNT_PURPOSE,
            source_batch_id="premarket-account-batch",
            source_batch_hash="1" * 64,
        )
        opened = _candidate(
            "pre-1", phase=PreselectionPhase.OPEN_REPRICED, quote_time=OPEN_NOW
        )
        batch = runtime.news_preselection_store.append_open_batch(
            run.head_hash,
            (opened,),
            batch_id="open-batch-1",
            scheduled_for=OPEN_NOW,
            observed_at=OPEN_NOW,
            source_batch_purpose=OPEN_REPRICE_PURPOSE,
            source_batch_id="open-batch",
            source_batch_hash="2" * 64,
        )
        observation = batch.rows[0]
        runtime.news.refresh_once()
        before = runtime.news.news_payload()
        before_pre = before["pre_market_preselections"][0]["ledger_lineage"]
        before_open = before["open_market_repriced"][0]["ledger_lineage"]
        assert before_pre["run_id"] == before_open["run_id"] == "run-1"
        assert before_pre["head_hash"] == before_open["head_hash"] == run.head_hash
        assert before_pre["row_hash"] == before_open["row_hash"] == run.rows[0].row_hash
        assert before_open["observation_hash"] == observation.observation_hash
        assert before_pre["production_parent_eligible"] is True
        assert before_pre["production_parent_blocker"] is None
        assert "production_parent_eligible" not in before_open
        assert "production_parent_blocker" not in before_open
        assert "structure_identity" not in before_pre
        assert "structure_identity" not in before_open
        api = create_app(runtime.services())
        endpoint = next(route.endpoint for route in api.routes if route.path == "/api/news")
        projected = asyncio.run(endpoint())
        assert projected["pre_market_preselection_count"] == 1
        assert projected["open_market_repriced_count"] == 1
        # This fixture's frozen clock is independent of the runtime wall clock,
        # so it remains a visible observation rather than an action-pool row.
        assert projected["option_action_pool_count"] == 0
        assert projected["preselection_coverage"]["source"] == (
            "INDEPENDENT_TOP10_LEDGER"
        )
        assert projected["preselection_coverage"]["open_reprice_producer_status"] == (
            "AVAILABLE"
        )
        assert projected["open_market_repriced"][0]["ledger_lineage"][
            "observation_hash"
        ] == observation.observation_hash
    finally:
        runtime.close()

    reopened = OptionsCopilotRuntime(config, runtime_services=_services())
    try:
        after = reopened.news.news_payload()
        assert after["pre_market_preselections"][0]["ledger_lineage"] == before_pre
        assert after["open_market_repriced"][0]["ledger_lineage"] == before_open
        assert after["preselection_coverage"]["latest_run_id"] == "run-1"
        assert after["preselection_coverage"]["latest_head_hash"] == run.head_hash
        assert after["preselection_coverage"]["open_count"] == 1
        assert after["preselection_coverage"]["source"] == "INDEPENDENT_TOP10_LEDGER"
        assert after["preselection_coverage"]["status"] == "PARTIAL"
        assert after["preselection_coverage"]["reason"] == (
            "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
        )
        assert after["preselection_coverage"]["open_reprice_producer_status"] == (
            "AVAILABLE"
        )
        assert after["preselection_coverage"]["open_reprice_writer"] == (
            "INDEPENDENT_TOP10_PRODUCER_V1"
        )
    finally:
        reopened.close()


def test_runtime_empty_ledger_is_explicitly_unavailable_and_close_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = OptionsCopilotRuntime(_config(tmp_path), runtime_services=_services())
    calls = 0
    original_close = runtime.news_preselection_store.close

    def counted_close() -> None:
        nonlocal calls
        calls += 1
        original_close()

    monkeypatch.setattr(runtime.news_preselection_store, "close", counted_close)
    runtime.news.refresh_once()
    payload = runtime.news.news_payload()
    assert payload["preselection_coverage"]["status"] == "UNAVAILABLE"
    assert payload["preselection_coverage"]["reason"] == (
        "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
    )
    assert payload["preselection_coverage"]["ledger_reason"] == (
        "NO_PREMARKET_LEDGER_RUN"
    )
    assert payload["preselection_coverage"]["open_reprice_producer_status"] == (
        "UNAVAILABLE"
    )
    ledger_health = runtime.health()["dependencies"]["top10_preselection_ledger"]
    assert ledger_health["health"] == "READY"
    assert ledger_health["readable"] is True
    assert ledger_health["status"] == "UNAVAILABLE"
    assert ledger_health["reason"] == "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
    assert ledger_health["ledger_reason"] == "NO_PREMARKET_LEDGER_RUN"
    runtime.close()
    runtime.close()
    assert calls == 1


def test_corrupt_preselection_ledger_aborts_runtime_before_production_composition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import options_copilot.runtime as runtime_module

    config = _config(tmp_path)
    runtime = OptionsCopilotRuntime(config, runtime_services=_services())
    runtime.news_preselection_store.append_premarket_run(
        "run-corrupt", (_candidate("pre-corrupt"),), now=PREMARKET_NOW
    )
    runtime.close()
    path = config.data_dir / "news_preselection.sqlite3"
    raw = sqlite3.connect(path)
    try:
        raw.execute("DROP TRIGGER ledger_entries_no_update")
        raw.execute("UPDATE ledger_entries SET payload_json='{}' WHERE sequence=1")
        raw.commit()
    finally:
        raw.close()

    production_calls = 0

    def forbidden_production(*_args, **_kwargs):
        nonlocal production_calls
        production_calls += 1
        raise AssertionError("production composition must not run")

    monkeypatch.setattr(runtime_module, "build_production_composition", forbidden_production)
    with pytest.raises(NewsPreselectionStoreCorruption):
        OptionsCopilotRuntime(config)
    assert production_calls == 0


def test_news_coordinator_clears_pools_when_ledger_lineage_cannot_bind_one_to_one(
    tmp_path: Path,
) -> None:
    candidate = _candidate("lineage-mismatch")

    class Snapshot:
        preselections = (candidate,)
        lineage = {
            (candidate.preselection_id, candidate.phase.value): {
                "source": "INDEPENDENT_TOP10_LEDGER",
                "preselection_id": candidate.preselection_id,
                "phase": candidate.phase.value,
                "run_id": "run-a",
                "run_created_at": PREMARKET_NOW.isoformat(),
                "head_hash": "a" * 64,
                "row_id": "row-a",
                # Deliberately not a digest: no row may be projected without
                # a strict, one-to-one immutable lineage binding.
                "row_hash": "wrong-parent",
                "premarket_rank": 1,
                "production_parent_eligible": True,
                "production_parent_blocker": None,
            }
        }
        coverage = {
            "requested_count": 10,
            "available_count": 1,
            "source": "INDEPENDENT_TOP10_LEDGER",
            "status": "PARTIAL",
            "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
            "latest_run_id": "run-a",
            "latest_head_hash": "a" * 64,
            "open_count": 0,
        }

    class Provider:
        health = "READY"

        @staticmethod
        def read_snapshot():
            return Snapshot()

        @staticmethod
        def coverage():
            return dict(Snapshot.coverage)

        @staticmethod
        def preselections():
            return Snapshot.preselections

    coordinator = NewsCoordinator(
        tmp_path / "lineage-mismatch-evidence.sqlite3",
        preselection_provider=Provider(),
        clock=lambda: PREMARKET_NOW,
    )
    try:
        coordinator.refresh_once()
        payload = coordinator.news_payload()
        assert payload["pre_market_preselections"] == []
        assert payload["open_market_repriced"] == []
        assert payload["option_action_pool"] == []
        assert payload["preselection_coverage"]["status"] == "UNAVAILABLE"
        assert payload["preselection_coverage"]["reason"] == (
            "PRESELECTION_LINEAGE_BINDING_INVALID"
        )
    finally:
        coordinator.close()


@pytest.mark.parametrize(
    ("production_parent_eligible", "production_parent_blocker"),
    (
        (True, "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"),
        (False, None),
        (False, "UNKNOWN_PARENT_BLOCKER"),
        ("true", None),
    ),
)
def test_news_coordinator_fails_closed_on_invalid_parent_eligibility_lineage(
    tmp_path: Path,
    production_parent_eligible: object,
    production_parent_blocker: object,
) -> None:
    candidate = _candidate("invalid-parent-eligibility")

    class Snapshot:
        preselections = (candidate,)
        lineage = {
            (candidate.preselection_id, candidate.phase.value): {
                "source": "INDEPENDENT_TOP10_LEDGER",
                "preselection_id": candidate.preselection_id,
                "phase": candidate.phase.value,
                "run_id": "run-parent-eligibility",
                "run_created_at": PREMARKET_NOW.isoformat(),
                "head_hash": "a" * 64,
                "row_id": "row-parent-eligibility",
                "row_hash": "b" * 64,
                "premarket_rank": 1,
                "production_parent_eligible": production_parent_eligible,
                "production_parent_blocker": production_parent_blocker,
            }
        }
        coverage = {
            "requested_count": 10,
            "available_count": 1,
            "source": "INDEPENDENT_TOP10_LEDGER",
            "status": "PARTIAL",
            "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
            "latest_run_id": "run-parent-eligibility",
            "latest_head_hash": "a" * 64,
            "open_count": 0,
        }

    class Provider:
        health = "READY"

        @staticmethod
        def read_snapshot():
            return Snapshot()

        @staticmethod
        def coverage():
            return dict(Snapshot.coverage)

        @staticmethod
        def preselections():
            return Snapshot.preselections

    coordinator = NewsCoordinator(
        tmp_path / "invalid-parent-eligibility-evidence.sqlite3",
        preselection_provider=Provider(),
        clock=lambda: PREMARKET_NOW,
    )
    try:
        coordinator.refresh_once()
        payload = coordinator.news_payload()
        assert payload["pre_market_preselections"] == []
        assert payload["open_market_repriced"] == []
        assert payload["option_action_pool"] == []
        assert payload["preselection_coverage"]["status"] == "UNAVAILABLE"
        assert payload["preselection_coverage"]["reason"] == (
            "PRESELECTION_LINEAGE_BINDING_INVALID"
        )
    finally:
        coordinator.close()


def test_news_coordinator_preserves_and_binds_one_atomic_open_batch(
    tmp_path: Path,
) -> None:
    snapshot = _atomic_snapshot()
    coordinator = NewsCoordinator(
        tmp_path / "atomic-batch-evidence.sqlite3",
        preselection_provider=_SnapshotProvider(snapshot),
        clock=lambda: OPEN_NOW,
    )
    try:
        coordinator.refresh_once()
        payload = coordinator.news_payload()
        coverage = payload["preselection_coverage"]
        assert payload["pre_market_preselection_count"] == 2
        assert payload["open_market_repriced_count"] == 2
        assert payload["option_action_pool_count"] == 2
        assert {
            key: coverage[key]
            for key in (
                "open_reprice_producer_status",
                "latest_open_batch_id",
                "latest_open_batch_head_hash",
                "freeze_slot",
                "reprice_slot",
                "latest_run_id",
                "latest_head_hash",
                "available_count",
                "open_count",
            )
        } == {
            key: snapshot.coverage[key]
            for key in (
                "open_reprice_producer_status",
                "latest_open_batch_id",
                "latest_open_batch_head_hash",
                "freeze_slot",
                "reprice_slot",
                "latest_run_id",
                "latest_head_hash",
                "available_count",
                "open_count",
            )
        }
        for row in payload["open_market_repriced"]:
            lineage = row["ledger_lineage"]
            assert lineage["source_batch_purpose"] == "OPEN_REPRICE"
            assert lineage["source_batch_id"] == "open-batch"
            assert lineage["source_batch_hash"] == "2" * 64
            assert lineage["source_batch_hash"] != row["broker_snapshot_hash"]
            assert lineage["batch_id"] == "ledger-open-batch"
            assert lineage["batch_head_hash"] == "f" * 64
            assert lineage["scheduled_for"] == OPEN_NOW.isoformat()
            assert lineage["quote_batch_id"] == "open-batch"
            assert lineage["row_id"] == f"row-{row['preselection_id']}"
        for row in payload["pre_market_preselections"]:
            lineage = row["ledger_lineage"]
            assert lineage["source_batch_purpose"] == "PREMARKET_ACCOUNT"
            assert lineage["source_batch_id"] == "premarket-account-batch"
            assert lineage["source_batch_hash"] == "1" * 64
    finally:
        coordinator.close()


def test_news_coordinator_keeps_legacy_v1_premarket_read_only(
    tmp_path: Path,
) -> None:
    snapshot = _atomic_snapshot(
        pre_ids=("legacy-v1",),
        open_ids=(),
        production_parent_eligible=False,
        source_lineage=False,
    )
    snapshot.coverage.update(
        {
            "status": "UNAVAILABLE",
            "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
            "ledger_reason": "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE",
            "open_reprice_producer_status": "UNAVAILABLE",
        }
    )
    coordinator = NewsCoordinator(
        tmp_path / "legacy-v1-evidence.sqlite3",
        preselection_provider=_SnapshotProvider(snapshot),
        clock=lambda: OPEN_NOW,
    )
    try:
        coordinator.refresh_once()
        payload = coordinator.news_payload()
        assert [
            row["preselection_id"] for row in payload["pre_market_preselections"]
        ] == ["legacy-v1"]
        assert payload["pre_market_preselections"][0]["ledger_lineage"][
            "production_parent_eligible"
        ] is False
        assert payload["open_market_repriced"] == []
        assert payload["option_action_pool"] == []
        assert payload["preselection_coverage"]["status"] == "UNAVAILABLE"
        assert payload["preselection_coverage"]["reason"] == (
            "SOURCE_LINEAGE_MISSING_LEGACY"
        )
    finally:
        coordinator.close()


def test_news_coordinator_keeps_missing_source_open_rows_no_trade(
    tmp_path: Path,
) -> None:
    snapshot = _atomic_snapshot(source_lineage=False)
    coordinator = NewsCoordinator(
        tmp_path / "missing-source-open-evidence.sqlite3",
        preselection_provider=_SnapshotProvider(snapshot),
        clock=lambda: OPEN_NOW,
    )
    try:
        coordinator.refresh_once()
        payload = coordinator.news_payload()

        assert payload["pre_market_preselection_count"] == 2
        assert payload["open_market_repriced_count"] == 2
        assert payload["option_action_pool"] == []
        assert payload["preselection_coverage"]["reason"] == (
            "SOURCE_LINEAGE_MISSING_LEGACY"
        )
        for row in payload["open_market_repriced"]:
            assert row["action_pool_eligible"] is False
            assert row["action_rank"] is None
            assert row["research_only"] is True
            assert "SOURCE_LINEAGE_MISSING_LEGACY" in row["blockers"]
    finally:
        coordinator.close()


@pytest.mark.parametrize(
    "broken_binding",
    (
        "open_id_set",
        "available_count",
        "open_count",
        "parent_row",
        "batch_id",
        "batch_head_hash",
        "scheduled_for",
        "quote_batch_id",
        "coverage_run_id",
        "coverage_head_hash",
        "coverage_freeze_slot",
        "coverage_batch_id",
        "coverage_batch_head_hash",
        "coverage_reprice_slot",
        "producer_status",
        "parent_ineligible",
        "decision_authority",
        "approval_eligible",
        "source_half",
        "source_purpose",
        "source_hash",
        "source_unknown",
        "source_quote_batch",
    ),
)
def test_news_coordinator_rejects_non_atomic_open_binding_before_action_pool(
    tmp_path: Path,
    broken_binding: str,
) -> None:
    snapshot = (
        _atomic_snapshot(open_ids=("atomic-a",))
        if broken_binding == "open_id_set"
        else _atomic_snapshot()
    )
    open_a = snapshot.lineage[("atomic-a", PreselectionPhase.OPEN_REPRICED.value)]
    open_b = snapshot.lineage.get(
        ("atomic-b", PreselectionPhase.OPEN_REPRICED.value)
    )
    if broken_binding == "available_count":
        snapshot.coverage["available_count"] = 3
    elif broken_binding == "open_count":
        snapshot.coverage["open_count"] = 1
    elif broken_binding == "parent_row":
        open_a["row_id"] = "wrong-parent-row"
    elif broken_binding == "batch_id":
        assert open_b is not None
        open_b["batch_id"] = "different-ledger-batch"
    elif broken_binding == "batch_head_hash":
        assert open_b is not None
        open_b["batch_head_hash"] = "9" * 64
    elif broken_binding == "scheduled_for":
        assert open_b is not None
        open_b["scheduled_for"] = PREMARKET_NOW.isoformat()
    elif broken_binding == "quote_batch_id":
        open_a["quote_batch_id"] = "different-quote-batch"
        assert open_b is not None
        open_b["quote_batch_id"] = "different-quote-batch"
    elif broken_binding == "coverage_run_id":
        snapshot.coverage["latest_run_id"] = "different-run"
    elif broken_binding == "coverage_head_hash":
        snapshot.coverage["latest_head_hash"] = "9" * 64
    elif broken_binding == "coverage_freeze_slot":
        snapshot.coverage["freeze_slot"] = OPEN_NOW.isoformat()
    elif broken_binding == "coverage_batch_id":
        snapshot.coverage["latest_open_batch_id"] = "different-ledger-batch"
    elif broken_binding == "coverage_batch_head_hash":
        snapshot.coverage["latest_open_batch_head_hash"] = "9" * 64
    elif broken_binding == "coverage_reprice_slot":
        snapshot.coverage["reprice_slot"] = PREMARKET_NOW.isoformat()
    elif broken_binding == "producer_status":
        snapshot.coverage["open_reprice_producer_status"] = "UNAVAILABLE"
    elif broken_binding == "parent_ineligible":
        pre = snapshot.lineage[("atomic-a", PreselectionPhase.PRE_MARKET.value)]
        pre["production_parent_eligible"] = False
        pre["production_parent_blocker"] = "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
    elif broken_binding == "decision_authority":
        snapshot.coverage["decision_authority"] = "EXECUTION"
    elif broken_binding == "approval_eligible":
        snapshot.coverage["approval_eligible"] = True
    elif broken_binding == "source_half":
        open_a["source_batch_hash"] = None
    elif broken_binding == "source_purpose":
        open_a["source_batch_purpose"] = "PREMARKET_ACCOUNT"
    elif broken_binding == "source_hash":
        open_a["source_batch_hash"] = "not-a-digest"
    elif broken_binding == "source_unknown":
        open_a["unknown_source_field"] = "reject-me"
    elif broken_binding == "source_quote_batch":
        open_a["source_batch_id"] = "different-source-batch"
        assert open_b is not None
        open_b["source_batch_id"] = "different-source-batch"

    coordinator = NewsCoordinator(
        tmp_path / f"non-atomic-{broken_binding}.sqlite3",
        preselection_provider=_SnapshotProvider(snapshot),
        clock=lambda: OPEN_NOW,
    )
    try:
        coordinator.refresh_once()
        payload = coordinator.news_payload()
        assert payload["pre_market_preselections"] == []
        assert payload["open_market_repriced"] == []
        assert payload["option_action_pool"] == []
        assert payload["preselection_coverage"]["status"] == "UNAVAILABLE"
        assert payload["preselection_coverage"]["reason"] == (
            "PRESELECTION_LINEAGE_BINDING_INVALID"
            if broken_binding
            in {
                "parent_row",
                "source_half",
                "source_purpose",
                "source_hash",
                "source_unknown",
            }
            else "PRESELECTION_ATOMIC_BINDING_INVALID"
        )
    finally:
        coordinator.close()
