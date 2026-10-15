from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

import options_copilot.runtime as runtime_module
from options_copilot.approval import ProposalApprovalStore
from options_copilot.bridge import CodexBridgeStore
from options_copilot.config import OptionsCopilotConfig
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.ranking import RankingStore
from options_copilot.runtime import OptionsCopilotRuntime, build_production_composition


MORNING = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)  # 09:20 ET
OPEN = datetime(2026, 8, 4, 13, 35, tzinfo=timezone.utc)  # 09:35 ET
INTRADAY = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)  # 10:00 ET


def _calendar(now: datetime):
    return UsOptionsSessionCalendar().normalize(
        liquid_hours="20260804:0930-1600",
        trading_hours="20260804:0930-1600",
        timezone_id="America/New_York",
        observed_at=now,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=now,
    )


class _Source:
    def __init__(self) -> None:
        self.calls = 0

    def resolve_top10(self, *, scheduled_for: datetime) -> tuple[object, ...]:
        self.calls += 1
        return ()


class _Store:
    def __init__(self) -> None:
        self.write_calls = 0

    def append_premarket_run(self, *_args: object, **_kwargs: object) -> object:
        self.write_calls += 1
        return object()

    def latest_premarket(self) -> None:
        return None

    def append_open_batch(self, *_args: object, **_kwargs: object) -> object:
        self.write_calls += 1
        return object()


class _SessionGate:
    def is_trading_session(self, *, scheduled_for: datetime) -> bool:
        return True


class _ForbiddenIbFactory:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> object:
        self.calls += 1
        raise AssertionError("composition tests must not connect to IBKR")


def _stores(config: OptionsCopilotConfig):
    approvals = ProposalApprovalStore(config.data_dir / "approvals.sqlite3")
    bridge = CodexBridgeStore(
        config.data_dir / "codex_bridge.sqlite3",
        approvals,
    )
    return approvals, bridge


def test_top10_preflight_reads_one_fresh_cached_control_snapshot() -> None:
    current = [MORNING]
    calls = [0]

    def snapshot():
        calls[0] += 1
        return {
            "status": "CURRENT",
            "stale": False,
            "reason": None,
            "observed_at": MORNING.isoformat(),
            "positions": ({"symbol": "SPY", "quantity": 0},),
            "working_order_count": 0,
            "unsubmitted_instruction_count": 0,
        }

    reader = runtime_module._LifecycleControlAccountStateReader(
        clock=lambda: current[0]
    )
    reader.bind(snapshot)

    assert reader.positions() == ({"symbol": "SPY", "quantity": 0},)
    assert reader.working_orders() == ()
    assert reader.unsubmitted_instructions() == ()
    assert calls[0] == 1

    current[0] = MORNING.replace(second=16)
    assert reader.positions() is None
    assert calls[0] == 2


@pytest.mark.parametrize(
    "missing",
    (
        "news_preselection_store",
        "top10_structure_source",
        "top10_session_gate",
        "top10_instruction_reader",
    ),
)
def test_missing_top10_dependency_never_constructs_or_dispatches_producer(
    tmp_path: Path,
    missing: str,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / missing / "data",
        log_dir=tmp_path / missing / "logs",
    )
    config.ensure_runtime_directories()
    approvals, bridge = _stores(config)
    source = _Source()
    store = _Store()
    ib_factory = _ForbiddenIbFactory()
    kwargs: dict[str, object] = {
        "news_preselection_store": store,
        "top10_structure_source": source,
        "top10_session_gate": _SessionGate(),
        "top10_instruction_reader": lambda: (),
    }
    kwargs[missing] = None

    composition = build_production_composition(
        config,
        approval_store=approvals,
        bridge_reader=bridge,
        clock=lambda: MORNING,
        ib_factory=ib_factory,
        **kwargs,
    )
    try:
        result = composition.top10_scheduler_service.tick(
            _calendar(MORNING),
            now=MORNING,
        )

        assert composition.top10_preselection_producer is None
        assert composition.top10_producer_blocker == (
            "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
        )
        assert result.duplicate_reason == "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
        assert source.calls == 0
        assert store.write_calls == 0
        assert ib_factory.calls == 0
        assert composition.services.approval_blockers == (
            "CREATOR_TRANSPORT_UNAVAILABLE",
        )
    finally:
        composition.lifecycle.close()
        bridge.close()
        approvals.close()


def test_ranking_store_is_never_accepted_as_independent_top10_ledger(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    config.ensure_runtime_directories()
    approvals, bridge = _stores(config)
    ranking = RankingStore(tmp_path / "not-a-top10-ledger.sqlite3")
    composition = build_production_composition(
        config,
        approval_store=approvals,
        bridge_reader=bridge,
        clock=lambda: MORNING,
        news_preselection_store=ranking,
        top10_structure_source=_Source(),
        top10_session_gate=_SessionGate(),
        top10_instruction_reader=lambda: (),
    )
    try:
        assert composition.top10_preselection_producer is None
        assert composition.top10_producer_blocker == (
            "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
        )
    finally:
        composition.lifecycle.close()
        ranking.close()
        bridge.close()
        approvals.close()


def test_default_runtime_composes_direct_top10_readonly_seams(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    runtime = OptionsCopilotRuntime(config)
    try:
        composition = runtime.production_composition
        assert composition is not None
        assert composition.top10_preselection_producer is not None
        assert composition.top10_producer_blocker is None
        assert composition.top10_scheduler_service.available is True
        health = composition.lifecycle.health()["scanner"]["top10_producer"]
        assert health["status"] == "READY"
        assert health["last_reason"] is None
        account_reader = composition.top10_preselection_producer._account_state_reader
        assert isinstance(
            account_reader,
            runtime_module._LifecycleControlAccountStateReader,
        )
        assert account_reader._snapshot_reader.__self__ is composition.lifecycle
        assert composition.top10_preselection_producer._session_gate is not None
        assert composition.top10_preselection_producer._premarket_account_only is True
        structure_source = composition.top10_preselection_producer._structure_source
        assert structure_source._maximum_snapshot_contracts == 14
        fallback = structure_source._fallback
        assert fallback.maximum_optionability_attempts == 5
        assert fallback.maximum_optionable == 5
        assert fallback.maximum_structures == 5
        assert composition.gateway._instruction_reader is not None
        assert runtime.news_preselection_store is not (
            composition.services.ranking_store
        )
        event_pool_reader = composition.pipeline_inputs._event_pool_reader
        assert event_pool_reader == runtime.news.decision_event_payload
        assert event_pool_reader != runtime.news.news_payload
        assert runtime.outcome_processor is not None
        assert runtime.outcome_capture_loop is not None
        assert composition.outcome_market_adapter is not None
        assert (
            composition.lifecycle.scanner_loop._daily_callbacks[
                "OUTCOME_PROCESSING"
            ]
            == runtime._process_outcomes
        )
        assert (
            composition.lifecycle.scanner_loop._daily_callbacks[
                "RESEARCH_REFRESH"
            ]
            == runtime.news.refresh_research
        )
        assert runtime.learning_status()["outcome_processing"]["status"] == "NOT_RUN"
    finally:
        runtime.close()


def test_external_mode_never_builds_or_starts_the_direct_ibkr_composition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "external" / "data",
        log_dir=tmp_path / "external" / "logs",
        broker_acquisition_mode="EXTERNAL",
        external_readonly_feed_path=tmp_path / "external" / "readonly.json",
        external_top10_path=tmp_path / "external" / "top10.json",
    )
    direct_build_calls = 0

    def forbidden_direct_build(*_args: object, **_kwargs: object) -> object:
        nonlocal direct_build_calls
        direct_build_calls += 1
        raise AssertionError("EXTERNAL mode must not build a direct IBKR gateway")

    monkeypatch.setattr(
        runtime_module,
        "build_production_composition",
        forbidden_direct_build,
    )
    runtime = OptionsCopilotRuntime(config)
    try:
        runtime.start()

        assert direct_build_calls == 0
        assert runtime.production_composition is None
        assert runtime._production_composition_reason == (
            "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
        )
        scanner = runtime.health()["dependencies"]["production_scanner"]
        assert scanner["decision"] == "NO_TRADE"
        assert scanner["reasons"] == ("OPEN_REPRICE_PRODUCER_UNAVAILABLE",)
    finally:
        runtime.close()


def test_direct_composition_rejects_external_mode_before_ibkr_factory(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "external-build" / "data",
        log_dir=tmp_path / "external-build" / "logs",
        broker_acquisition_mode="EXTERNAL",
        external_readonly_feed_path=tmp_path / "external-build" / "readonly.json",
        external_top10_path=tmp_path / "external-build" / "top10.json",
    )
    config.validate()
    config.ensure_runtime_directories()
    approvals, bridge = _stores(config)
    ib_factory = _ForbiddenIbFactory()
    try:
        with pytest.raises(ValueError, match="forbidden in EXTERNAL"):
            build_production_composition(
                config,
                approval_store=approvals,
                bridge_reader=bridge,
                ib_factory=ib_factory,
            )
        assert ib_factory.calls == 0
    finally:
        bridge.close()
        approvals.close()


def test_complete_injection_routes_exact_slots_once_across_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    config.ensure_runtime_directories()
    approvals, bridge = _stores(config)
    source = _Source()
    store = _Store()
    current = [MORNING]
    calls: list[datetime] = []
    scheduled_calls: list[datetime] = []

    def fake_tick(
        _producer: object,
        *,
        scheduled_for: datetime,
    ) -> dict[str, object]:
        calls.append(current[0])
        scheduled_calls.append(scheduled_for)
        return {
            "status": (
                "PREMARKET_FROZEN"
                if current[0] == MORNING
                else "OPEN_REPRICED"
            ),
            "reason_codes": (),
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }

    common = {
        "approval_store": approvals,
        "bridge_reader": bridge,
        "clock": lambda: current[0],
        "ib_factory": _ForbiddenIbFactory(),
        "news_preselection_store": store,
        "top10_structure_source": source,
        "top10_session_gate": _SessionGate(),
        "top10_instruction_reader": lambda: (),
    }
    first = build_production_composition(config, **common)
    assert first.top10_preselection_producer is not None
    producer_type = type(first.top10_preselection_producer)
    monkeypatch.setattr(producer_type, "tick", fake_tick)
    try:
        assert first.top10_preselection_producer._store is store
        assert first.top10_preselection_producer._structure_source is source
        account_reader = first.top10_preselection_producer._account_state_reader
        assert isinstance(
            account_reader,
            runtime_module._LifecycleControlAccountStateReader,
        )
        assert account_reader._snapshot_reader.__self__ is first.lifecycle
        assert first.top10_preselection_producer._store is not first.services.ranking_store
        assert first.top10_preselection_producer._clock is first.gateway._now
        snapshot_provider = first.top10_preselection_producer._snapshot_provider
        assert snapshot_provider.pacing is first.pipeline_inputs.pacing
        assert (
            snapshot_provider.broker_snapshot_builder
            is first.services.broker_snapshot_builder
        )
        assert snapshot_provider._batch_lock is first.news_adapter._batch_lock
        morning = first.top10_scheduler_service.tick(
            _calendar(MORNING),
            now=MORNING,
        )
        duplicate = first.top10_scheduler_service.tick(
            _calendar(MORNING),
            now=MORNING,
        )
        assert morning.producer_status == "PREMARKET_FROZEN"
        assert duplicate.duplicate_reason == "SLOT_ALREADY_COMPLETED"
    finally:
        first.lifecycle.close()

    restarted = build_production_composition(config, **common)
    try:
        restarted_duplicate = restarted.top10_scheduler_service.tick(
            _calendar(MORNING),
            now=MORNING,
        )
        current[0] = OPEN
        opened = restarted.top10_scheduler_service.tick(_calendar(OPEN), now=OPEN)
        current[0] = INTRADAY
        intraday = restarted.top10_scheduler_service.tick(
            _calendar(INTRADAY),
            now=INTRADAY,
        )

        assert restarted_duplicate.duplicate_reason == "SLOT_ALREADY_COMPLETED"
        assert opened.producer_status == "OPEN_REPRICED"
        assert intraday.duplicate_reason == "NO_TOP10_SLOT_DUE"
        assert calls == [MORNING, OPEN]
        assert scheduled_calls == [MORNING, OPEN]
    finally:
        restarted.lifecycle.close()
        bridge.close()
        approvals.close()
