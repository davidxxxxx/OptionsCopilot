from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from options_copilot.approval import ProposalApprovalStore
from options_copilot.bridge import CodexBridgeStore
from options_copilot.config import OptionsCopilotConfig
from options_copilot.decision.pipeline import normalize_funnel_trace
from options_copilot.analytics import VolatilityEngine
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.governance.contracts import load_contract
from options_copilot.gateway import (
    AtomicBrokerSnapshot,
    BatchedOptionQuote,
    BrokerSnapshotStatus,
    BrokerSnapshotBuilder,
    MarketDataPacingError,
    OptionContractRef,
    OptionQualificationError,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
    UnderlyingIvHistory,
    UnderlyingIvHistoryPoint,
)
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.news.preselection import evaluate_preselection
from options_copilot.news.reaction import (
    ConsensusExpectation,
    EventReactionLedger,
    MarketReactionEvidence,
    OfficialRelease,
    ScheduledEventIdentity,
)
from options_copilot.news.reaction_runtime import ProductionReactionObserver
from options_copilot.news_runtime import _deterministic_decision_news_row
from options_copilot.news.macro_proxy import ResearchProxyBinding
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.operations.capabilities import (
    MarketDataPacingCapability,
    PACING_REQUEST_CLASSES,
)
from options_copilot.production_runtime import (
    DirectTop10StructureSource,
    GuardedRequestBudget,
    IBKRNewsResearchAdapter,
    ProductionBrokerEvidenceAcquisition,
    ProductionLifecycle,
    ProductionOutcomeMarketAdapter,
    ProductionOptionsEvidenceAcquisition,
    ProductionPipelineInputs,
    ProductionRiskGate,
    _news_candidate,
)
import options_copilot.production_runtime as production_runtime_module
from options_copilot.scanner.pacing import RequestBudgetByClass
from options_copilot.scanner.scheduler import ScanRunStore
from options_copilot.scanner.service import ScanSchedulerLoop, ScanSchedulerService
from options_copilot.scanner.universe import UniverseFunnel
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.evidence import EvidenceStore


NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
OPEN_NOW = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)


def _bullish_equity_theses(*symbols: str) -> dict[str, object]:
    return {
        "schema": "options_copilot.equity_theses.v1",
        "rows": tuple(
            {
                "symbol": symbol,
                "direction_label": "BULLISH",
                "direction_score": "40",
                "uncertainty": "0.20",
            }
            for symbol in symbols
        ),
    }


def test_structure_plans_reach_credit_and_range_templates_without_more_requests() -> None:
    strikes = tuple(Decimal(value) for value in ("90", "95", "100", "105", "110"))

    bullish = production_runtime_module._planned_structure_requests(
        strikes,
        Decimal("100"),
        direction_label="BULLISH",
        uncertainty=Decimal("0.40"),
    )
    bearish = production_runtime_module._planned_structure_requests(
        strikes,
        Decimal("100"),
        direction_label="BEARISH",
        uncertainty=Decimal("0.40"),
    )
    neutral = production_runtime_module._planned_structure_requests(
        strikes,
        Decimal("100"),
        direction_label="NEUTRAL",
        uncertainty=Decimal("0.40"),
    )

    assert tuple(plan.structure for plan in bullish) == (
        "DEBIT_VERTICAL",
        "CREDIT_VERTICAL",
    )
    assert tuple(plan.structure for plan in bearish) == (
        "DEBIT_VERTICAL",
        "CREDIT_VERTICAL",
    )
    assert tuple(plan.structure for plan in neutral) == (
        "BUTTERFLY",
        "IRON_CONDOR",
    )
    assert all(len(plans) == 2 for plans in (bullish, bearish, neutral))


def test_structure_plan_uncertainty_boundary_and_rejection_reasons_are_explicit() -> None:
    strikes = tuple(Decimal(value) for value in ("90", "95", "100", "105", "110"))

    at_limit = production_runtime_module._planned_structure_requests(
        strikes,
        Decimal("100"),
        direction_label="BULLISH",
        uncertainty=Decimal("0.55"),
    )
    above_limit = production_runtime_module._planned_structure_requests(
        strikes,
        Decimal("100"),
        direction_label="BULLISH",
        uncertainty=Decimal("0.5501"),
    )

    assert at_limit
    assert above_limit == ()
    assert production_runtime_module._structure_plan_rejection_reason(
        strikes,
        direction_label="BULLISH",
        uncertainty=Decimal("0.5501"),
    ) == "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT"
    assert production_runtime_module._structure_plan_rejection_reason(
        strikes,
        direction_label="MIXED",
        uncertainty=Decimal("0.20"),
    ) == "EQUITY_THESIS_DIRECTION_UNSUPPORTED"
    assert production_runtime_module._structure_plan_rejection_reason(
        (),
        direction_label="BULLISH",
        uncertainty=Decimal("0.20"),
    ) == "EQUITY_THESIS_HAS_NO_SUPPORTED_TEMPLATE"


def test_complex_paced_theses_reserve_one_optionable_underlying() -> None:
    low_uncertainty = {
        "SPY": {"direction_label": "BULLISH", "uncertainty": "0.20"},
        "QQQ": {"direction_label": "BEARISH", "uncertainty": "0.25"},
    }
    moderate = {
        **low_uncertainty,
        "QQQ": {"direction_label": "BEARISH", "uncertainty": "0.2501"},
    }
    neutral = {
        **low_uncertainty,
        "QQQ": {"direction_label": "NEUTRAL", "uncertainty": "0.20"},
    }

    assert not production_runtime_module._pacing_requires_single_optionable_underlying(
        ("SPY", "QQQ"),
        low_uncertainty,
    )
    assert production_runtime_module._pacing_requires_single_optionable_underlying(
        ("SPY", "QQQ"),
        moderate,
    )
    assert production_runtime_module._pacing_requires_single_optionable_underlying(
        ("SPY", "QQQ"),
        neutral,
    )


def test_complex_paced_thesis_reserves_bounded_second_preflight_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_limits: list[tuple[int | None, int | None]] = []

    def preflight(
        _gateway: object,
        _pacing: object,
        _symbols: object,
        *,
        asof: date,
        max_attempts: int | None = None,
        max_optionable: int | None = None,
    ) -> object:
        assert isinstance(asof, date)
        observed_limits.append((max_attempts, max_optionable))
        return production_runtime_module._OptionabilityRead(())

    monkeypatch.setattr(
        production_runtime_module,
        "_preflight_optionable_underlyings",
        preflight,
    )

    class Gateway:
        market_data_pacing_enabled = True

    class Pacing:
        ready = True
        reason = None

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )
    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-complex-pacing-bound",
        slot_at=NOW,
        symbols=("SPY", "QQQ"),
        equity_theses={
            "rows": (
                {
                    "symbol": "SPY",
                    "direction_label": "NEUTRAL",
                    "uncertainty": "0.20",
                },
                {
                    "symbol": "QQQ",
                    "direction_label": "NEUTRAL",
                    "uncertainty": "0.20",
                },
            ),
        },
    )

    assert outcome.candidates == ()
    assert observed_limits == [(2, 2)]


def test_ready_deep_scan_symbols_skip_unrelated_equity_enrichment() -> None:
    assert production_runtime_module._secondary_equity_evidence_targets(
        ("SPY",),
        ("TSLA", "NVDA", "TSLA"),
    ) == ()
    assert production_runtime_module._secondary_equity_evidence_targets(
        (),
        ("tsla", "NVDA", "TSLA"),
    ) == ("TSLA", "NVDA")


def test_optionability_preflight_exposes_budget_deferred_symbols() -> None:
    calls: list[str] = []

    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs: object) -> tuple[object, ...]:
            calls.append(symbol)
            return (
                SimpleNamespace(
                    expiration=date(2026, 8, 21),
                    multiplier=100,
                    strikes=(Decimal("100"),),
                ),
            )

    result = production_runtime_module._preflight_optionable_underlyings(
        Gateway(),
        object(),
        ("SPY", "QQQ", "IWM"),
        asof=date(2026, 8, 3),
        max_optionable=1,
    )

    assert result.complete
    assert calls == ["SPY"]
    assert result.attempted_symbols == ("SPY",)
    assert result.deferred_symbols == ("QQQ", "IWM")
    assert tuple(result.as_mapping()) == ("SPY",)


def test_equity_pool_projection_does_not_spend_deep_scan_on_ineligible_theses() -> None:
    result = _formal_equity_pool_result("PASS", "TOO_UNCERTAIN", "MIXED")
    result.stored.snapshot.selected[1].score.uncertainty = Decimal("0.5501")
    result.stored.snapshot.selected[2].score.direction_label = SimpleNamespace(
        value="MIXED"
    )

    selected, _targets, evidence, _reference, _theses = (
        production_runtime_module._equity_pool_projection(result)
    )

    assert selected == ("PASS",)
    assert evidence is not None
    assert evidence["deep_scan_symbols"] == ("PASS",)
    assert evidence["deep_scan_count"] == 1
    assert evidence["deep_scan_exclusions"] == (
        {
            "symbol": "TOO_UNCERTAIN",
            "reason_code": "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT",
        },
        {
            "symbol": "MIXED",
            "reason_code": "EQUITY_THESIS_DIRECTION_UNSUPPORTED",
        },
    )


def test_pipeline_surfaces_formal_equity_thesis_routing_blocker() -> None:
    def build_equity_pool(**_kwargs: object) -> object:
        result = _formal_equity_pool_result("TLT")
        result.stored.snapshot.selected[0].score.uncertainty = Decimal("0.617")
        return result

    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs: object) -> tuple[object, ...]:
            raise AssertionError("manual core-only scan must not use scanner pacing")

        def option_expirations(self, *_args: object, **_kwargs: object) -> tuple[object, ...]:
            raise AssertionError("ineligible thesis must stop before option discovery")

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, object]:
            return {}

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("TLT",),
        equity_pool_builder=build_equity_pool,
        clock=lambda: NOW,
    )

    with inputs.manual_core_only():
        payload = inputs.run(scan_run_id="scan-thesis-routing", slot_at=NOW)

    assert payload["reasons"] == (
        "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT",
    )
    assert payload["funnel_trace"]["deep_scan_requested"] == 0


def test_quote_batch_rebuild_preserves_atomic_observation_timestamp() -> None:
    observed_at = NOW - timedelta(milliseconds=250)
    snapshot = AtomicBrokerSnapshot(
        built_at=NOW,
        status=BrokerSnapshotStatus.COMPLETE,
        reason_codes=(),
        state_evidence={},
        secdef_evidence=(),
        quote_batch_id="batch-observed-at",
        quote_batch_status=QuoteBatchStatus.COMPLETE,
        quote_batch_source="IBKR_REQMKT_DATA_READONLY",
        quote_batch_requested_at=NOW - timedelta(seconds=1),
        quote_batch_completed_at=NOW,
        quotes=(),
        oldest_quote_age_seconds=Decimal("0.25"),
        maximum_leg_skew_seconds=Decimal("0"),
        snapshot_hash="a" * 64,
        quote_batch_observed_at=observed_at,
    )

    batch = production_runtime_module._quote_batch_from_snapshot(snapshot)

    assert batch is not None
    assert batch.observed_at == observed_at
    assert batch.blockers == ()


def _formal_equity_pool_result(
    *symbols: str,
    slot: datetime = NOW,
) -> object:
    reference = {
        "schema": "options_copilot.equity_pool_reference.v1",
        "snapshot_id": "1" * 64,
        "snapshot_hash": "2" * 64,
        "input_manifest_hash": "3" * 64,
        "rows_hash": "4" * 64,
        "policy_hash": "5" * 64,
        "taxonomy_hash": "6" * 64,
        "scoring_hash": "7" * 64,
        "selected_symbols": tuple(symbols),
        "discovery_count": len(symbols),
        "selected_count": len(symbols),
        "excluded_count": 0,
        "exclusion_stats": {},
    }
    selected = tuple(
        SimpleNamespace(
            symbol=symbol,
            score=SimpleNamespace(
                direction_label=SimpleNamespace(value="BULLISH"),
                direction_score=Decimal("40"),
                uncertainty=Decimal("0.20"),
            ),
            canonical_input_hash=f"{index:x}" * 64,
            selected_rank=index,
        )
        for index, symbol in enumerate(symbols, 1)
    )
    snapshot = SimpleNamespace(slot=slot, selected=selected)
    return SimpleNamespace(
        selected_symbols=tuple(symbols),
        acquisition_targets=(),
        stored=SimpleNamespace(snapshot=snapshot),
        as_dict=lambda: {"equity_pool_reference": reference},
    )


def _live_underlying_quote(
    symbol: str,
    *,
    observed_at: datetime = NOW,
    contract_id: int | None = None,
    close: Decimal = Decimal("99"),
    market_price: Decimal = Decimal("100"),
    market_data_type: int = 1,
) -> object:
    return SimpleNamespace(
        symbol=symbol,
        contract_id=(
            contract_id
            if contract_id is not None
            else 20_000
            + sum(
                (index + 1) * ord(character)
                for index, character in enumerate(symbol)
            )
        ),
        exchange="NASDAQ",
        observed_at=observed_at,
        source="IBKR_REQ_TICKERS_READONLY",
        bid=market_price - Decimal("0.10"),
        ask=market_price + Decimal("0.10"),
        last=market_price,
        close=close,
        market_data_type=market_data_type,
    )


@pytest.mark.parametrize(
    ("direction", "expected_structures"),
    (
        ("BULLISH", {"DEBIT_VERTICAL", "CREDIT_VERTICAL"}),
        ("BEARISH", {"DEBIT_VERTICAL", "CREDIT_VERTICAL"}),
        ("NEUTRAL", {"BUTTERFLY", "IRON_CONDOR"}),
    ),
)
def test_coarse_candidate_path_materializes_bounded_multi_strategy_choices(
    direction: str,
    expected_structures: set[str],
) -> None:
    class Gateway:
        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=tuple(
                        Decimal(value)
                        for value in ("90", "95", "100", "105", "110")
                    ),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, ...],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            right = rights[0]
            offset = 0 if right == "C" else 1_000
            return tuple(
                OptionContractRef(
                    contract_id=30_000 + offset + int(strike),
                    contract_id_ex=f"{30_000 + offset + int(strike)}@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{right}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=right,  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for strike in strikes
            )

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        execution_cost_contract=load_contract(
            Path("options_copilot/governance/execution_cost_contract.v1.json")
        ).to_dict(),
        clock=lambda: NOW,
    )
    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id=f"scan-{direction.lower()}-structures",
        slot_at=NOW,
        symbols=("SPY",),
        equity_theses={
            "schema": "options_copilot.equity_theses.v1",
            "rows": (
                {
                    "symbol": "SPY",
                    "direction_label": direction,
                    "direction_score": "40",
                    "uncertainty": "0.40",
                },
            ),
        },
    )

    assert len(outcome.candidates) == (2 if direction == "NEUTRAL" else 4)
    assert {str(item["structure"]) for item in outcome.candidates} == expected_structures
    assert all(item["equity_thesis_evidence"]["direction_label"] == direction for item in outcome.candidates)
    assert all(
        item["invalidation_evidence"]["status"] == "BOUND"
        and item["invalidation_evidence"]["equity_thesis_hash"]
        == item["equity_thesis_hash"]
        for item in outcome.candidates
    )
    for item in outcome.candidates:
        short_legs = tuple(
            leg
            for leg in item["legs"]
            if str(leg["side"]).upper() in {"SHORT", "SELL"}
        )
        expected_status = "SUPPORTED" if short_legs else "NOT_APPLICABLE"
        assert item["assignment_evidence"]["status"] == expected_status
        assert item["ex_dividend_evidence"]["status"] == expected_status
        assert all(
            leg["short_leg_risk_evidence"]["status"] == "SUPPORTED"
            and len(leg["short_leg_risk_evidence"]["evidence_hash"]) == 64
            for leg in short_legs
        )


def test_tampered_execution_cost_contract_cannot_supply_short_leg_risk_evidence() -> None:
    contract = load_contract(
        Path("options_copilot/governance/execution_cost_contract.v1.json")
    ).to_dict()
    payload = dict(contract["payload"])
    assignment = dict(payload["assignment_exercise_and_dividend"])
    assignment["short_leg_exit_deadline"] = "TAMPERED"
    payload["assignment_exercise_and_dividend"] = assignment
    contract["payload"] = payload

    evidence = production_runtime_module._execution_cost_short_leg_research_evidence(
        contract,
        as_of=NOW,
    )

    assert evidence["status"] == "UNSUPPORTED"
    assert evidence["reason_codes"] == (
        "ASSIGNMENT_EXERCISE_EX_DIVIDEND_EVIDENCE_UNAVAILABLE",
    )
    assert evidence["evidence_hash"] is None


def _authoritative_iv_history(
    symbol: str,
    *,
    end_at: datetime,
    contract_id: int = 756733,
) -> UnderlyingIvHistory:
    points = tuple(
        UnderlyingIvHistoryPoint(
            trading_date=end_at.date() - timedelta(days=20 - index),
            close=Decimal("0.18") + Decimal(index) / Decimal("1000"),
        )
        for index in range(10)
    )
    basis_hash = canonical_hash(
        {
            "symbol": symbol,
            "contract_id": contract_id,
            "security_type": "STK",
            "request_exchange": "SMART",
            "currency": "USD",
        }
    )
    provisional = UnderlyingIvHistory(
        symbol=symbol,
        contract_id=contract_id,
        request_exchange="SMART",
        currency="USD",
        observed_at=end_at,
        end_at=end_at,
        duration="30 D",
        bar_size="1 day",
        what_to_show="OPTION_IMPLIED_VOLATILITY",
        use_rth=True,
        points=points,
        basis_hash=basis_hash,
        content_hash="",
    )
    return replace(
        provisional,
        content_hash=canonical_hash(provisional.hash_payload()),
    )


def test_outcome_market_adapter_reuses_shared_readonly_batch_and_pacing(
    monkeypatch,
) -> None:
    class FakeAtomicSnapshot:
        complete = True
        snapshot_hash = "f" * 64
        quote_batch_id = "outcome-batch"

        def __init__(self) -> None:
            self.quotes = (
                BatchedOptionQuote(
                    contract_id=101,
                    batch_id="outcome-batch",
                    request_id="request-101",
                    requested_at=NOW,
                    observed_at=NOW + timedelta(seconds=1),
                    completed_at=NOW + timedelta(seconds=1),
                    source="IBKR_READ_ONLY",
                    bid=Decimal("1.40"),
                    ask=Decimal("1.50"),
                    exchange_time=NOW + timedelta(seconds=1),
                    implied_volatility=Decimal("0.21"),
                    volume=140,
                ),
            )

        def verify_hash(self) -> bool:
            return True

    class Gateway:
        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            assert symbols == ("SPY",)
            return (
                SimpleNamespace(
                    symbol="SPY",
                    contract_id=756733,
                    observed_at=NOW + timedelta(seconds=1),
                    source="IBKR_READ_ONLY",
                    market_price=Decimal("101"),
                ),
            )

    class Builder:
        def __init__(self) -> None:
            self.calls: list[tuple[OptionContractRef, ...]] = []

        def build(self, contracts: tuple[OptionContractRef, ...]) -> object:
            self.calls.append(contracts)
            return FakeAtomicSnapshot()

    class Pacing:
        ready = True

        def __init__(self) -> None:
            self.calls: list[str] = []

        def decision(self, request_class: str) -> object:
            self.calls.append(request_class)
            return SimpleNamespace(allowed=True)

    monkeypatch.setattr(
        production_runtime_module,
        "AtomicBrokerSnapshot",
        FakeAtomicSnapshot,
    )
    builder = Builder()
    pacing = Pacing()
    adapter = ProductionOutcomeMarketAdapter(
        Gateway(),  # type: ignore[arg-type]
        builder,
        pacing,  # type: ignore[arg-type]
        batch_lock=threading.RLock(),
    )
    spec = {
        "symbol": "SPY",
        "capture_plan": {
            "benchmark_symbol": "SPY",
            "legs": (
                {
                    "contract": {
                        "contract_id": 101,
                        "contract_id_ex": "101@SMART",
                        "symbol": "SPY",
                        "local_symbol": "SPY   260821C00500000",
                        "expiration": date(2026, 8, 21),
                        "strike": Decimal("500"),
                        "right": "C",
                        "exchange": "SMART",
                        "trading_class": "SPY",
                        "multiplier": 100,
                        "currency": "USD",
                    },
                },
            ),
        },
    }

    batch = adapter.observe((spec,), expected_at=NOW)

    assert pacing.calls == ["snapshot_quote"]
    assert len(builder.calls) == 1
    assert builder.calls[0][0].contract_id == 101
    assert batch["schema"] == "options_copilot.outcome_market_batch.v1"
    assert batch["observed_at"] == NOW + timedelta(seconds=1)
    assert batch["source_hash"] == "f" * 64
    assert batch["underlyings"][0]["price"] == Decimal("101")  # type: ignore[index]


def test_outcome_market_adapter_captures_prediction_baseline_without_option_snapshot() -> None:
    class Gateway:
        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            assert symbols == ("QQQ", "SPY")
            return tuple(
                SimpleNamespace(
                    symbol=symbol,
                    contract_id=index,
                    observed_at=NOW + timedelta(seconds=1),
                    source="IBKR_READ_ONLY",
                    market_price=Decimal("101") + index,
                )
                for index, symbol in enumerate(symbols, start=1)
            )

    class Builder:
        def __init__(self) -> None:
            self.calls = 0

        def build(self, contracts: tuple[OptionContractRef, ...]) -> object:
            self.calls += 1
            raise AssertionError("baseline-only capture must not build an option snapshot")

    class Pacing:
        ready = True

        def __init__(self) -> None:
            self.calls: list[str] = []

        def decision(self, request_class: str) -> object:
            self.calls.append(request_class)
            return SimpleNamespace(allowed=True)

    builder = Builder()
    pacing = Pacing()
    adapter = ProductionOutcomeMarketAdapter(
        Gateway(),  # type: ignore[arg-type]
        builder,
        pacing,  # type: ignore[arg-type]
        batch_lock=threading.RLock(),
    )
    spec = {
        "symbol": "QQQ",
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "WAITING",
            "reason_codes": ("OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE",),
        },
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
    }

    first = adapter.observe((spec,), expected_at=NOW)
    second = adapter.observe((spec,), expected_at=NOW)

    assert pacing.calls == ["snapshot_quote", "snapshot_quote"]
    assert builder.calls == 0
    assert first["quotes"] == ()
    assert first["source_hash"] == second["source_hash"]
    assert first["source_id"] == second["source_id"]
    assert {item["symbol"] for item in first["underlyings"]} == {"QQQ", "SPY"}  # type: ignore[index]


class _Guard:
    def __init__(self, *, ready: bool, reasons: tuple[str, ...] = ()) -> None:
        self.ready = ready
        self._reasons = reasons

    def reasons(self) -> tuple[str, ...]:
        return self._reasons


class _Gateway:
    def __init__(self) -> None:
        self.connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.account_calls = 0
        self.positions_calls = 0
        self.working_order_calls = 0
        self.instruction_calls = 0
        self.connect_failures = 0
        self.control_failure = False
        self._active_connects = 0
        self.maximum_concurrent_connects = 0
        self._connect_lock = threading.Lock()

    def connect(self) -> None:
        with self._connect_lock:
            self.connect_calls += 1
            self._active_connects += 1
            self.maximum_concurrent_connects = max(
                self.maximum_concurrent_connects,
                self._active_connects,
            )
        try:
            if self.connect_failures:
                self.connect_failures -= 1
                raise RuntimeError("sanitized connect failure")
            self.connected = True
        finally:
            with self._connect_lock:
                self._active_connects -= 1

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False

    def account_snapshot(self) -> object:
        self.account_calls += 1
        if self.control_failure:
            raise RuntimeError("sanitized control snapshot failure")
        return SimpleNamespace(asof=NOW, net_liquidation=Decimal("2000"))

    def positions(self) -> tuple[object, ...]:
        self.positions_calls += 1
        return ()

    def working_orders(self) -> tuple[object, ...]:
        self.working_order_calls += 1
        return ()

    def unsubmitted_instructions(self) -> tuple[object, ...]:
        self.instruction_calls += 1
        return ()


class _Scanner:
    def __init__(self) -> None:
        self.start_calls = 0
        self.close_calls = 0

    def start(self) -> None:
        self.start_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    def health(self) -> dict[str, object]:
        return {"status": "READY" if self.start_calls else "STOPPED"}


class _BlockingHealthScanner(_Scanner):
    def __init__(self) -> None:
        super().__init__()
        self.health_entered = threading.Event()
        self.health_release = threading.Event()

    def health(self) -> dict[str, object]:
        self.health_entered.set()
        self.health_release.wait(timeout=2.0)
        return super().health()


class _LifecyclePipeline:
    def __init__(self) -> None:
        self.calls: list[tuple[str, datetime]] = []

    def run_slot(self, scan_run_id: str, slot_at: datetime) -> dict[str, str]:
        self.calls.append((scan_run_id, slot_at))
        return {"decision": "NO_TRADE"}


class _LifecycleCalendarProvider:
    def __init__(self, now: datetime) -> None:
        self.calendar = UsOptionsSessionCalendar().normalize(
            liquid_hours="20260804:0930-1600",
            trading_hours="20260804:0930-1600",
            timezone_id="America/New_York",
            observed_at=now,
            source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
            now=now,
        )

    def snapshot(self, *, now: datetime) -> object:
        del now
        return self.calendar


class _Store:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class _MutableClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value
        self._lock = threading.Lock()

    def __call__(self) -> datetime:
        with self._lock:
            return self.value

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.value += timedelta(seconds=seconds)


def _wait_until(predicate, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached before timeout")


def test_stale_pacing_never_connects_gateway_or_starts_scanner() -> None:
    gateway = _Gateway()
    scanner = _Scanner()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=scanner,
        stores=(),
        pacing_guard=_Guard(  # type: ignore[arg-type]
            ready=False,
            reasons=("PACING_OBSERVATION_STALE", "PACING_CAPABILITY_MISSING"),
        ),
    )

    lifecycle.start()
    lifecycle.start()

    assert gateway.connect_calls == 0
    assert scanner.start_calls == 0
    assert lifecycle.health()["decision"] == "NO_TRADE"
    assert set(lifecycle.health()["reasons"]) == {
        "PACING_OBSERVATION_STALE",
        "PACING_CAPABILITY_MISSING",
    }
    lifecycle.close()


def test_slow_scanner_health_does_not_hold_control_snapshot_lock() -> None:
    scanner = _BlockingHealthScanner()
    lifecycle = ProductionLifecycle(
        gateway=_Gateway(),  # type: ignore[arg-type]
        scanner_loop=scanner,
        stores=(),
        pacing_guard=_Guard(ready=False),  # type: ignore[arg-type]
    )
    health_thread = threading.Thread(target=lifecycle.health, daemon=True)
    control_complete = threading.Event()

    def read_control() -> None:
        lifecycle.control_snapshot()
        control_complete.set()

    health_thread.start()
    assert scanner.health_entered.wait(timeout=0.5)
    control_thread = threading.Thread(target=read_control, daemon=True)
    control_thread.start()
    try:
        assert control_complete.wait(timeout=0.2)
    finally:
        scanner.health_release.set()
        health_thread.join(timeout=1.0)
        control_thread.join(timeout=1.0)


def test_production_lifecycle_start_and_close_are_idempotent() -> None:
    gateway = _Gateway()
    scanner = _Scanner()
    store = _Store()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=scanner,
        stores=(store,),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
    )

    lifecycle.start()
    lifecycle.start()
    assert gateway.connect_calls == 1
    assert scanner.start_calls == 1
    assert lifecycle.health()["decision"] == "READY"

    lifecycle.close()
    lifecycle.close()
    lifecycle.start()
    assert gateway.connect_calls == 1
    assert gateway.disconnect_calls == 1
    assert scanner.start_calls == 1
    assert scanner.close_calls == 1
    assert store.close_calls == 1
    assert "PRODUCTION_LIFECYCLE_CLOSED" in lifecycle.health()["reasons"]


def test_production_lifecycle_starts_scheduler_when_gateway_is_unavailable() -> None:
    gateway = _Gateway()
    gateway.connect_failures = 1
    scanner = _Scanner()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=scanner,
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        supervisor_wait=lambda _stop, _seconds: True,
    )

    lifecycle.start()
    health = lifecycle.health()

    assert gateway.connect_calls == 1
    assert scanner.start_calls == 1
    assert health["scheduler"]["status"] == "READY"
    assert health["api_session"]["status"] == "DISCONNECTED"
    assert health["decision"] == "NO_TRADE"
    assert "IBKR_READONLY_CONNECTION_UNAVAILABLE" in health["reasons"]
    lifecycle.close()


def test_disconnected_gateway_does_not_block_durable_1615_outcome_job(
    tmp_path,
) -> None:
    now = datetime(2026, 8, 4, 20, 15, 30, tzinfo=timezone.utc)
    callback_calls: list[datetime] = []
    gateway = _Gateway()
    gateway.connect_failures = 1
    store = ScanRunStore(tmp_path / "runs.sqlite")
    scanner = ScanSchedulerLoop(
        ScanSchedulerService(
            store,
            _LifecyclePipeline(),
            pipeline_version="p4",
        ),
        _LifecycleCalendarProvider(now),
        clock=lambda: now,
    )
    scanner.bind_daily_callbacks(
        outcome_process=lambda: callback_calls.append(now)
        or {
            "status": "WAITING",
            "checked_at": now.isoformat(),
            "due_count": 0,
            "records_appended": 0,
            "reason_codes": ("NO_OUTCOMES_DUE",),
        },
    )
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=scanner,
        stores=(store,),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=lambda: now,
        supervisor_wait=lambda _stop, _seconds: True,
    )

    try:
        lifecycle.start()
        _wait_until(
            lambda: store.latest_daily_result("OUTCOME_PROCESSING") is not None,
        )
        durable = store.latest_daily_result("OUTCOME_PROCESSING")
        health = lifecycle.health()

        assert callback_calls == [now]
        assert durable is not None
        assert durable.status == "COMPLETED"
        assert durable.payload["status"] == "WAITING"
        assert durable.payload["reason_codes"] == ["NO_OUTCOMES_DUE"]
        assert health["api_session"]["status"] == "DISCONNECTED"
        assert health["scheduler"]["daily_operations"]["outcome_processing"][
            "status"
        ] == "WAITING"
        assert gateway.account_calls == 0
        assert gateway.positions_calls == 0
        assert gateway.working_order_calls == 0
        assert gateway.instruction_calls == 0
    finally:
        assert lifecycle.close() is True


def test_production_lifecycle_exposes_layered_fresh_control_health() -> None:
    gateway = _Gateway()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )

    lifecycle.start()
    health = lifecycle.health()

    assert gateway.connect_calls == 1
    assert gateway.maximum_concurrent_connects == 1
    assert gateway.account_calls == 1
    assert gateway.positions_calls == 1
    assert gateway.working_order_calls == 1
    assert gateway.instruction_calls == 1
    assert health["scheduler"]["status"] == "READY"
    assert health["api_session"] == {
        "status": "CONNECTED",
        "connected": True,
        "read_only": True,
        "connect_timeout_seconds": 8.0,
    }
    assert health["broker_upstream"]["status"] == "UP"
    assert health["control_snapshot"] == {
        "status": "CURRENT",
        "stale": False,
        "observed_at": NOW.isoformat(),
        "age_ms": 0,
        "refresh_seconds": 5.0,
        "stale_after_seconds": 15.0,
        "positions_count": 0,
        "working_order_count": 0,
        "unsubmitted_instruction_count": 0,
    }
    assert health["executable_quote"] == {
        "status": "ON_DEMAND",
        "stale": None,
        "maximum_age_seconds": 5.0,
        "decision_authority": False,
    }
    lifecycle.close()


def test_production_lifecycle_fails_closed_when_instruction_state_is_unknown() -> None:
    class UnknownInstructionGateway(_Gateway):
        def unsubmitted_instructions(self) -> None:
            self.instruction_calls += 1
            return None

    gateway = UnknownInstructionGateway()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=lambda: NOW,
        supervisor_wait=lambda _stop, _seconds: True,
    )

    lifecycle.start()
    health = lifecycle.health()

    assert gateway.instruction_calls == 1
    assert health["status"] == "DEGRADED"
    assert health["decision"] == "NO_TRADE"
    assert health["broker_upstream"]["status"] == "DEGRADED"
    assert health["broker_upstream"]["last_error"] == (
        "UNSUBMITTED_INSTRUCTIONS_UNKNOWN"
    )
    assert health["control_snapshot"]["status"] == "PARTIAL"
    assert health["control_snapshot"]["positions_count"] == 0
    assert health["control_snapshot"]["working_order_count"] == 0
    assert health["control_snapshot"]["unsubmitted_instruction_count"] is None
    projection = lifecycle.control_snapshot()
    assert projection["account"]["net_liquidation"] == Decimal("2000")
    assert projection["positions"] == ()
    assert projection["decision_authority"] == "LAST_KNOWN_ONLY"
    assert "UNSUBMITTED_INSTRUCTIONS_UNKNOWN" in health["reasons"]
    lifecycle.close()


def test_production_supervisor_refreshes_control_snapshot_every_five_seconds() -> None:
    gateway = _Gateway()
    clock = _MutableClock()
    refreshed = threading.Event()

    def advancing_wait(_stop: threading.Event, seconds: float) -> bool:
        clock.advance(seconds)
        if gateway.account_calls >= 2:
            refreshed.set()
            return True
        return False

    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=clock,
        supervisor_wait=advancing_wait,
    )

    lifecycle.start()
    assert refreshed.wait(timeout=1)

    assert gateway.connect_calls == 1
    assert gateway.account_calls == 2
    assert lifecycle.health()["control_snapshot"]["observed_at"] == (
        NOW + timedelta(seconds=5)
    ).isoformat()
    lifecycle.close()


def test_control_position_fingerprint_refreshes_management_once_per_change() -> None:
    class PositionGateway(_Gateway):
        def __init__(self) -> None:
            super().__init__()
            self.rows = (
                {
                    "contract_id": 101,
                    "symbol": "QQQ",
                    "quantity": Decimal("1"),
                    "average_cost": Decimal("166.10"),
                    "market_price": Decimal("2.31"),
                    "unrealized_pnl": Decimal("65.08"),
                },
            )

        def positions(self) -> tuple[object, ...]:
            self.positions_calls += 1
            return self.rows

    gateway = PositionGateway()
    gateway.connected = True
    refreshes: list[str] = []
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=lambda: NOW,
        management_refresher=lambda: refreshes.append("refresh"),
    )

    lifecycle._refresh_control_snapshot()
    lifecycle._refresh_management_if_pending()
    gateway.rows = (
        {
            **gateway.rows[0],
            "market_price": Decimal("2.40"),
            "unrealized_pnl": Decimal("73.90"),
        },
    )
    lifecycle._refresh_control_snapshot()
    lifecycle._refresh_management_if_pending()

    assert refreshes == ["refresh"]

    gateway.rows = ({**gateway.rows[0], "quantity": Decimal("2")},)
    lifecycle._refresh_control_snapshot()
    lifecycle._refresh_management_if_pending()

    assert refreshes == ["refresh", "refresh"]


def test_guarded_budget_honors_long_lived_policy_without_restamping() -> None:
    capability = MarketDataPacingCapability.create(
        version="P0",
        observed_at=NOW,
        source="conservative_default",
        request_classes={
            request_class: {
                "max_concurrency": 1,
                "request_window": 60,
                "max_requests": 2,
                "cooldown": 1,
            }
            for request_class in PACING_REQUEST_CLASSES
        },
        signer="human:xujie",
    )
    later = NOW + timedelta(days=3)
    guard = SimpleNamespace(
        capability=capability,
        initial_resolution=SimpleNamespace(policy_authority=object()),
        ready=True,
        now=lambda: later,
    )

    budget = GuardedRequestBudget(guard, now=later)  # type: ignore[arg-type]

    with budget.lease("secdef") as decision:
        assert decision.allowed is True
    assert budget.ready is True


def test_guarded_budget_preserves_legacy_capability_expiry() -> None:
    capability = MarketDataPacingCapability.create(
        version="P0",
        observed_at=NOW,
        source="conservative_default",
        request_classes={
            request_class: {
                "max_concurrency": 1,
                "request_window": 60,
                "max_requests": 2,
                "cooldown": 1,
            }
            for request_class in PACING_REQUEST_CLASSES
        },
        signer="human:xujie",
    )
    later = NOW + timedelta(days=3)
    guard = SimpleNamespace(
        capability=capability,
        initial_resolution=SimpleNamespace(policy_authority=None),
        ready=True,
        now=lambda: later,
    )

    budget = GuardedRequestBudget(guard, now=later)  # type: ignore[arg-type]

    with budget.lease("secdef") as decision:
        assert decision.allowed is False
        assert decision.reason == "PACING_CAPABILITY_MISSING"
    assert budget.ready is False


def test_production_supervisor_uses_bounded_reconnect_backoff_without_overlap() -> None:
    gateway = _Gateway()
    gateway.connect_failures = 99
    delays: list[float] = []
    exhausted = threading.Event()

    def recording_wait(_stop: threading.Event, seconds: float) -> bool:
        delays.append(seconds)
        if len(delays) == 7:
            exhausted.set()
            return True
        return False

    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=lambda: NOW,
        supervisor_wait=recording_wait,
    )

    lifecycle.start()
    assert exhausted.wait(timeout=1)

    assert delays == [1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0]
    assert gateway.connect_calls == 7
    assert gateway.maximum_concurrent_connects == 1
    health = lifecycle.health()
    assert health["api_session"]["status"] == "DISCONNECTED"
    assert health["decision"] == "NO_TRADE"
    lifecycle.close()


def test_control_snapshot_becomes_stale_after_fifteen_seconds() -> None:
    gateway = _Gateway()
    clock = _MutableClock()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=clock,
        supervisor_wait=lambda _stop, _seconds: True,
    )

    lifecycle.start()
    clock.advance(16)
    health = lifecycle.health()

    assert health["control_snapshot"]["status"] == "STALE"
    assert health["control_snapshot"]["stale"] is True
    assert health["control_snapshot"]["age_ms"] == 16000
    assert health["broker_upstream"]["status"] == "DEGRADED"
    assert health["decision"] == "NO_TRADE"
    assert "CONTROL_SNAPSHOT_STALE" in health["reasons"]
    lifecycle.close()


def test_production_lifecycle_keeps_gateway_and_stores_open_until_scanner_exits() -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingScanner:
        def __init__(self) -> None:
            self.close_calls = 0
            self._status = "STOPPED"
            self._thread: threading.Thread | None = None

        def start(self) -> None:
            self._status = "READY"
            self._thread = threading.Thread(target=self._tick, daemon=True)
            self._thread.start()

        def _tick(self) -> None:
            entered.set()
            try:
                release.wait(timeout=2)
            finally:
                self._status = "STOPPED"

        def close(self) -> bool:
            self.close_calls += 1
            assert self._thread is not None
            self._status = "STOPPING"
            self._thread.join(timeout=0.01)
            return not self._thread.is_alive()

        def health(self) -> dict[str, object]:
            return {"status": self._status}

    gateway = _Gateway()
    scanner = BlockingScanner()
    store = _Store()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=scanner,
        stores=(store,),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
    )

    lifecycle.start()
    assert entered.wait(timeout=1)
    lifecycle.close()
    disconnects_while_blocked = gateway.disconnect_calls
    store_closes_while_blocked = store.close_calls

    release.set()
    assert scanner._thread is not None
    scanner._thread.join(timeout=1)
    lifecycle.close()

    assert disconnects_while_blocked == 0
    assert store_closes_while_blocked == 0
    assert gateway.disconnect_calls == 1
    assert store.close_calls == 1


def test_production_lifecycle_connection_loss_reconnects_through_one_supervisor() -> None:
    gateway = _Gateway()
    gateway.connect_failures = 0
    reconnected = threading.Event()

    def reconnect_wait(_stop: threading.Event, _seconds: float) -> bool:
        if gateway.connect_calls == 1 and gateway.connected:
            gateway.connected = False
            gateway.connect_failures = 1
            return False
        if gateway.connect_calls >= 3 and gateway.connected:
            reconnected.set()
            return True
        return False

    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
        clock=lambda: NOW,
        supervisor_wait=reconnect_wait,
    )
    lifecycle.start()
    assert reconnected.wait(timeout=1)

    health = lifecycle.health()

    assert gateway.connect_calls == 3
    assert gateway.maximum_concurrent_connects == 1
    assert health["status"] == "READY"
    assert health["decision"] == "READY"
    assert health["api_session"]["status"] == "CONNECTED"
    assert health["broker_upstream"]["status"] == "UP"
    lifecycle.close()


def test_production_lifecycle_store_close_failure_is_retryable_no_trade() -> None:
    class RetryStore(_Store):
        def __init__(self) -> None:
            super().__init__()
            self.failures = 1

        def close(self) -> None:
            self.close_calls += 1
            if self.failures:
                self.failures -= 1
                raise RuntimeError("sanitized store close failure")

    gateway = _Gateway()
    retry_store = RetryStore()
    already_closed = _Store()
    lifecycle = ProductionLifecycle(
        gateway=gateway,  # type: ignore[arg-type]
        scanner_loop=_Scanner(),
        stores=(retry_store, already_closed),
        pacing_guard=_Guard(ready=True),  # type: ignore[arg-type]
    )
    lifecycle.start()

    assert lifecycle.close() is False
    health = lifecycle.health()
    assert health["status"] == "DEGRADED"
    assert health["decision"] == "NO_TRADE"
    assert "STORE_CLOSE_FAILED" in health["reasons"]
    assert already_closed.close_calls == 1

    assert lifecycle.close() is True
    assert already_closed.close_calls == 1
    assert retry_store.close_calls == 2


def test_pipeline_input_management_refresh_failure_stops_after_account_preflight() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def positions(self) -> tuple[object, ...]:
            self.calls.append("positions")
            return ()

        def working_orders(self) -> tuple[object, ...]:
            self.calls.append("working_orders")
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            self.calls.append("unsubmitted_instructions")
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

    class Nav:
        content_hash = "b" * 64
        contract_hash = "c" * 64

        def snapshot(self, *, asof: datetime) -> "Nav":
            assert asof == NOW
            return self

    gateway = Gateway()
    inputs = ProductionPipelineInputs(
        gateway,  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )

    def fail_refresh() -> None:
        raise RuntimeError("sanitized")

    inputs.bind_management_refresher(fail_refresh)

    payload = inputs.run(scan_run_id="scan-management-failure", slot_at=NOW)

    assert payload["reasons"] == ("MANAGEMENT_REFRESH_FAILED",)
    assert gateway.calls == [
        "positions",
        "working_orders",
        "unsubmitted_instructions",
    ]
    assert payload["positions"] == ()
    assert payload["universe"]["coarse_contracts"] == ()


def test_pipeline_inputs_refresh_management_for_an_open_derivative_position() -> None:
    class Gateway:
        def positions(self) -> tuple[dict[str, object], ...]:
            return (
                {
                    "contract_id": 902732952,
                    "symbol": "QQQ",
                    "security_type": "OPT",
                    "quantity": Decimal("1"),
                },
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

    class ManagementCandidates:
        status = "CANDIDATES"
        reason_codes: tuple[str, ...] = ()

    refresh_calls = 0

    def refresh_management() -> ManagementCandidates:
        nonlocal refresh_calls
        refresh_calls += 1
        return ManagementCandidates()

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    inputs.bind_management_refresher(refresh_management)

    payload = inputs.run(scan_run_id="scan-open-qqq", slot_at=NOW)

    assert refresh_calls == 1
    assert payload["status"] == "POSITION_MANAGEMENT_ONLY"
    assert payload["reasons"] == (
        "POSITION_MANAGEMENT_ONLY",
        "OPEN_POSITION_MANAGEMENT_ONLY",
    )
    assert payload["positions"] == (
        {
            "contract_id": 902732952,
            "symbol": "QQQ",
            "security_type": "OPT",
            "quantity": Decimal("1"),
        },
    )
    assert payload["universe"]["coarse_contracts"] == ()


def test_production_composition_binds_one_shared_management_coordinator(
    tmp_path: Path,
) -> None:
    from options_copilot.runtime import build_production_composition

    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    config.ensure_runtime_directories()
    approvals = ProposalApprovalStore(config.data_dir / "approvals.sqlite3")
    bridge = CodexBridgeStore(
        config.data_dir / "codex_bridge.sqlite3",
        approvals,
    )
    composition = build_production_composition(
        config,
        approval_store=approvals,
        bridge_reader=bridge,
        clock=lambda: NOW,
    )
    try:
        coordinator = composition.management_coordinator
        assert coordinator.gateway is composition.gateway
        assert coordinator.position_manager is composition.services.position_manager
        assert composition.pipeline_inputs.position_manager is coordinator.position_manager
        assert composition.services.management()["decision"] == "NO_TRADE"
        assert composition.services.management()["review_only"] is True
        assert composition.services.management()["direct_order_submission"] is False
        feature_readiness = composition.services.readiness()["feature_data_chain"]
        assert feature_readiness["status"] == "INCOMPLETE"
        assert feature_readiness["model_input_complete"] is False
        assert feature_readiness["reason_codes"] == (
            "FEATURE_HISTORY_PRODUCER_UNWIRED",
            "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
            "CANDIDATE_FEATURE_BINDING_UNWIRED",
        )
        assert feature_readiness["affects_decision"] is False
    finally:
        composition.lifecycle.close()
        bridge.close()
        approvals.close()


def test_pipeline_inputs_fail_before_market_data_when_instruction_state_is_unknown() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.positions_calls = 0

        def positions(self) -> tuple[object, ...]:
            self.positions_calls += 1
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> None:
            return None

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            raise AssertionError("market-data budget must not be consumed")

    class Nav:
        content_hash = "b" * 64
        contract_hash = "c" * 64

        def snapshot(self, *, asof: datetime) -> "Nav":
            assert asof == NOW
            return self

    class NoPositionManagement:
        status = "NO_TRADE"
        reason_codes = ("NO_OPEN_GLD_POSITION",)

    gateway = Gateway()
    inputs = ProductionPipelineInputs(
        gateway,  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    inputs.bind_management_refresher(lambda: NoPositionManagement())

    payload = inputs.run(scan_run_id="scan-unknown-instruction", slot_at=NOW)

    assert payload["reasons"] == ("UNSUBMITTED_INSTRUCTIONS_UNKNOWN",)
    assert gateway.positions_calls == 1
    assert payload["universe"]["coarse_contracts"] == ()


def test_pipeline_inputs_never_research_a_second_combo_when_management_exists() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

    class Nav:
        content_hash = "b" * 64
        contract_hash = "c" * 64

        def snapshot(self, *, asof: datetime) -> "Nav":
            assert asof == NOW
            return self

    class ManagementCandidates:
        status = "CANDIDATES"
        reason_codes = ()

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    inputs.bind_management_refresher(lambda: ManagementCandidates())

    payload = inputs.run(scan_run_id="scan-management-only", slot_at=NOW)

    assert payload["reasons"] == ("OPEN_POSITION_MANAGEMENT_ONLY",)
    assert payload["positions"] == ()


def test_pipeline_inputs_record_the_bounded_150_to_30_funnel() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            assert rows_per_scan == 50
            prefix = {
                "MOST_ACTIVE": 0,
                "TOP_PERC_GAIN": 50,
                "TOP_PERC_LOSE": 100,
            }[scan_codes[0]]
            return tuple(
                SimpleNamespace(
                    symbol=f"S{prefix + index:03d}",
                    rank=index,
                    source_scan=scan_codes[0],
                    contract_id=prefix + index + 1,
                )
                for index in range(50)
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def __init__(self) -> None:
            self.calls: list[str] = []

        def decision(self, request_class: str) -> object:
            self.calls.append(request_class)
            return SimpleNamespace(allowed=True)

        def usage(self) -> dict[str, dict[str, int]]:
            return {"scanner": {"used": 3, "limit": 3}}

    class Nav:
        authority_hash = "b" * 64

        def snapshot(self, *, asof: datetime) -> "Nav":
            return self

    class NoPositionManagement:
        status = "NO_TRADE"
        reason_codes = ("NO_OPEN_GLD_POSITION",)

    pacing = Pacing()
    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )
    inputs.bind_management_refresher(lambda: NoPositionManagement())
    coarse_calls: list[tuple[str, ...]] = []

    def coarse_candidate_outcome(**kwargs: object):
        coarse_calls.append(tuple(kwargs["symbols"]))  # type: ignore[arg-type]
        return production_runtime_module._CoarseCandidateRead(
            tuple({"symbol": f"S{index:03d}", "dte": 21} for index in range(30))
        )

    inputs._coarse_candidate_outcome = coarse_candidate_outcome  # type: ignore[method-assign]

    payload = inputs.run(scan_run_id="scan-funnel", slot_at=NOW)

    assert pacing.calls[:3] == ["scanner", "scanner", "scanner"]
    funnel_trace = payload["funnel_trace"]
    assert {
        key: value
        for key, value in funnel_trace.items()
        if key != "research_allocation"
    } == {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": "scan-funnel",
        "discovered_underlyings": 150,
        "deep_scan_requested": 30,
        "deep_scan_attempted": 30,
        "deep_scan_completed": 30,
        "deep_scan_deferred": 0,
        "ranked_limit": 10,
        "ranked_count": 0,
        "filler_candidates": 0,
        "pacing_capability_hash": "a" * 64,
        "pacing_usage": {"scanner": {"used": 3, "limit": 3}},
        "scanner_confirmed_charged_requests": 3,
        "scanner_completed_scan_codes": (
            "MOST_ACTIVE",
            "TOP_PERC_GAIN",
            "TOP_PERC_LOSE",
        ),
        "scanner_failed_scan_codes": (),
        "scanner_source_row_counts": (
            {"scan_code": "MOST_ACTIVE", "row_count": 50},
            {"scan_code": "TOP_PERC_GAIN", "row_count": 50},
            {"scan_code": "TOP_PERC_LOSE", "row_count": 50},
        ),
    }
    allocation = funnel_trace["research_allocation"]
    assert allocation["schema"] == "options_copilot.research_allocation_evidence.v3"
    assert allocation["limit"] == 30
    assert allocation["decision_authority"] == "SUPPORTING_ONLY"
    assert allocation["score_evidence"] == ()
    assert len(allocation["scanner_score_inputs"]) == 30
    assert allocation["core_score_inputs"] == ()
    assert allocation["approval_eligible"] is False
    assert allocation["instruction_creation_allowed"] is False
    assert allocation["order_allowed"] is False
    assert allocation["event_symbols"] == ()
    assert allocation["deterministic_baseline_symbols"] == allocation["selected_symbols"]
    assert len(allocation["selected_symbols"]) == 30
    assert coarse_calls == [allocation["selected_symbols"]]
    assert payload["universe"]["funnel_trace"] == payload["funnel_trace"]


def test_pipeline_freezes_scanner_pacing_usage_at_discovery_boundary() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            assert rows_per_scan == 50
            return (
                SimpleNamespace(
                    symbol=scan_codes[0],
                    rank=1,
                    source_scan=scan_codes[0],
                    contract_id={
                        "MOST_ACTIVE": 1,
                        "TOP_PERC_GAIN": 2,
                        "TOP_PERC_LOSE": 3,
                    }[scan_codes[0]],
                ),
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def __init__(self) -> None:
            self.usage_calls = 0

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, dict[str, int]]:
            self.usage_calls += 1
            return {
                "scanner": {
                    "used": 4 if self.usage_calls == 1 else 0,
                    "limit": 4,
                }
            }

    pacing = Pacing()
    builder_usage: list[Mapping[str, object]] = []

    def build_equity_pool(**kwargs: object) -> object:
        builder_usage.append(kwargs["pacing_usage"])  # type: ignore[arg-type]
        return _formal_equity_pool_result("MOST_ACTIVE")

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=build_equity_pool,
        clock=lambda: NOW,
    )
    inputs._coarse_candidate_outcome = lambda **_kwargs: (  # type: ignore[method-assign]
        production_runtime_module._CoarseCandidateRead(
            ({"symbol": "MOST_ACTIVE", "dte": 21},),
        )
    )

    payload = inputs.run(
        scan_run_id="scan-expired-pacing-window",
        slot_at=NOW,
    )
    funnel_trace = payload["funnel_trace"]

    assert pacing.usage_calls == 1
    assert builder_usage[0] is funnel_trace["pacing_usage"]
    with pytest.raises(TypeError):
        builder_usage[0]["scanner"]["used"] = 0  # type: ignore[index]
    assert funnel_trace["pacing_usage"]["scanner"] == {
        "used": 4,
        "limit": 4,
    }
    normalized = normalize_funnel_trace(
        funnel_trace,
        scan_run_id="scan-expired-pacing-window",
    )
    assert normalized["discovered_underlyings"] == 3


@pytest.mark.parametrize(
    ("failure_stage", "expected_reason"),
    (
        ("event", "RESEARCH_ALLOCATION_INPUT_INVALID"),
        ("pool", "EQUITY_POOL_UNAVAILABLE"),
    ),
)
def test_post_discovery_failures_reuse_the_frozen_pacing_witness(
    failure_stage: str,
    expected_reason: str,
) -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            assert rows_per_scan == 50
            return (
                SimpleNamespace(
                    symbol=scan_codes[0],
                    rank=1,
                    source_scan=scan_codes[0],
                    contract_id={
                        "MOST_ACTIVE": 1,
                        "TOP_PERC_GAIN": 2,
                        "TOP_PERC_LOSE": 3,
                    }[scan_codes[0]],
                ),
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def __init__(self) -> None:
            self.usage_calls = 0

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, dict[str, int]]:
            self.usage_calls += 1
            return {
                "scanner": {
                    "used": 3 if self.usage_calls == 1 else 0,
                    "limit": 4,
                }
            }

    def unavailable_pool(**_kwargs: object) -> object:
        raise RuntimeError("pool unavailable")

    pacing = Pacing()
    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=(
            unavailable_pool
            if failure_stage == "pool"
            else None
        ),
        clock=lambda: NOW,
    )
    if failure_stage == "event":
        inputs.bind_event_pool_reader(
            lambda: {
                "news": (
                    {
                        "symbols": ("SPY",),
                        "event_impact_score": "invalid",
                    },
                )
            }
        )

    payload = inputs.run(
        scan_run_id=f"scan-post-discovery-{failure_stage}",
        slot_at=NOW,
    )

    assert pacing.usage_calls == 1
    assert payload["reasons"] == (expected_reason,)
    assert payload["funnel_trace"]["discovered_underlyings"] == 3
    assert payload["funnel_trace"]["scanner_confirmed_charged_requests"] == 3
    assert payload["funnel_trace"]["pacing_usage"]["scanner"] == {
        "used": 3,
        "limit": 4,
    }
    normalize_funnel_trace(
        payload["funnel_trace"],
        scan_run_id=f"scan-post-discovery-{failure_stage}",
    )


@pytest.mark.parametrize(
    ("usage_result", "expected_reason"),
    (
        (RuntimeError("usage unavailable"), "SCANNER_PACING_USAGE_UNAVAILABLE"),
        (
            {"scanner": {"used": "3", "limit": 3}},
            "SCANNER_PACING_USAGE_INVALID",
        ),
    ),
)
def test_pipeline_rejects_missing_scanner_pacing_witness_before_pool_build(
    usage_result: object,
    expected_reason: str,
) -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            assert rows_per_scan == 50
            return (
                SimpleNamespace(
                    symbol=scan_codes[0],
                    rank=1,
                    source_scan=scan_codes[0],
                    contract_id={
                        "MOST_ACTIVE": 1,
                        "TOP_PERC_GAIN": 2,
                        "TOP_PERC_LOSE": 3,
                    }[scan_codes[0]],
                ),
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def __init__(self) -> None:
            self.usage_calls = 0

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> object:
            self.usage_calls += 1
            if isinstance(usage_result, Exception):
                raise usage_result
            return usage_result

    pool_build_calls = 0

    def build_equity_pool(**_kwargs: object) -> object:
        nonlocal pool_build_calls
        pool_build_calls += 1
        return _formal_equity_pool_result("MOST_ACTIVE")

    pacing = Pacing()
    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=build_equity_pool,
        clock=lambda: NOW,
    )

    payload = inputs.run(
        scan_run_id=f"scan-{expected_reason.lower()}",
        slot_at=NOW,
    )

    assert pool_build_calls == 0
    assert pacing.usage_calls == 1
    assert payload["status"] == "NO_TRADE"
    assert payload["decision"] == "NO_TRADE"
    assert payload["reasons"] == (expected_reason,)
    assert payload["funnel_trace"]["discovered_underlyings"] == 0
    normalize_funnel_trace(
        payload["funnel_trace"],
        scan_run_id=f"scan-{expected_reason.lower()}",
    )


def test_pipeline_rejects_zero_usage_after_three_empty_scanner_requests() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs: object) -> tuple[object, ...]:
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 0, "limit": 3}}

    pool_build_calls = 0

    def build_equity_pool(**_kwargs: object) -> object:
        nonlocal pool_build_calls
        pool_build_calls += 1
        return _formal_equity_pool_result()

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=build_equity_pool,
        clock=lambda: NOW,
    )

    payload = inputs.run(
        scan_run_id="scan-empty-zero-usage",
        slot_at=NOW,
    )

    assert pool_build_calls == 0
    assert payload["status"] == "NO_TRADE"
    assert payload["reasons"] == ("SCANNER_PACING_USAGE_INVALID",)
    assert payload["funnel_trace"]["discovered_underlyings"] == 0


def test_position_mode_rejects_missing_scanner_pacing_witness_before_pool_build() -> None:
    class Gateway:
        def positions(self) -> tuple[dict[str, object], ...]:
            return (
                {
                    "contract_id": 902732952,
                    "symbol": "QQQ",
                    "security_type": "OPT",
                    "quantity": Decimal("1"),
                },
            )

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            assert rows_per_scan == 50
            return (
                SimpleNamespace(
                    symbol=scan_codes[0],
                    rank=1,
                    source_scan=scan_codes[0],
                    contract_id={
                        "MOST_ACTIVE": 1,
                        "TOP_PERC_GAIN": 2,
                        "TOP_PERC_LOSE": 3,
                    }[scan_codes[0]],
                ),
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> object:
            raise RuntimeError("usage unavailable")

    pool_build_calls = 0
    management_calls = 0

    def build_equity_pool(**_kwargs: object) -> object:
        nonlocal pool_build_calls
        pool_build_calls += 1
        return _formal_equity_pool_result("MOST_ACTIVE")

    def refresh_management() -> object:
        nonlocal management_calls
        management_calls += 1
        return SimpleNamespace(status="NO_TRADE", reason_codes=())

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=build_equity_pool,
        clock=lambda: NOW,
    )
    inputs.bind_management_refresher(refresh_management)

    payload = inputs.run(
        scan_run_id="scan-position-missing-pacing-witness",
        slot_at=NOW,
    )

    assert pool_build_calls == 0
    assert management_calls == 1
    assert payload["status"] == "POSITION_MANAGEMENT_ONLY"
    assert payload["reasons"] == (
        "POSITION_MANAGEMENT_ONLY",
        "SCANNER_PACING_USAGE_UNAVAILABLE",
        "MANAGEMENT_STATE_UNAVAILABLE",
    )
    assert payload["funnel_trace"]["discovered_underlyings"] == 0
    normalize_funnel_trace(
        payload["funnel_trace"],
        scan_run_id="scan-position-missing-pacing-witness",
    )


def test_scanner_source_failure_is_explicit_and_other_sources_continue() -> None:
    class Gateway:
        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            code = scan_codes[0]
            if code == "TOP_PERC_GAIN":
                raise RuntimeError("bounded scanner source unavailable")
            return (
                SimpleNamespace(
                    symbol=code,
                    rank=1,
                    source_scan=code,
                    contract_id=(1 if code == "MOST_ACTIVE" else 2),
                ),
            )

    class Pacing:
        def lease(self, request_class: str):
            return nullcontext(SimpleNamespace(allowed=True, reason=None))

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 3, "limit": 3}}

    result = production_runtime_module._discover_underlyings(
        Gateway(),
        Pacing(),
    )

    assert tuple(row.symbol for row in result.rows) == (
        "MOST_ACTIVE",
        "TOP_PERC_LOSE",
    )
    assert result.completed_scan_codes == ("MOST_ACTIVE", "TOP_PERC_LOSE")
    assert result.failed_scan_codes == ("TOP_PERC_GAIN",)
    assert result.reason_codes == (
        "IBKR_SCANNER_TOP_PERC_GAIN_UNAVAILABLE",
    )
    assert result.confirmed_charged_scanner_requests == 3


def test_scanner_pacing_denial_names_every_unattempted_source() -> None:
    class Gateway:
        def scan_underlyings(self, **_kwargs: object) -> tuple[object, ...]:
            raise AssertionError("denied pacing must prevent the scanner call")

    class Pacing:
        def lease(self, request_class: str):
            return nullcontext(
                SimpleNamespace(
                    allowed=False,
                    reason="PACING_REQUEST_WINDOW_EXHAUSTED",
                )
            )

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 0, "limit": 3}}

    result = production_runtime_module._discover_underlyings(
        Gateway(),
        Pacing(),
    )

    assert result.rows == ()
    assert result.completed_scan_codes == ()
    assert result.failed_scan_codes == (
        "MOST_ACTIVE",
        "TOP_PERC_GAIN",
        "TOP_PERC_LOSE",
    )
    assert result.reason_codes == (
        "IBKR_SCANNER_PACING_DENIED",
        "IBKR_SCANNER_PACING_REQUEST_WINDOW_EXHAUSTED",
    )
    assert result.source_row_counts == (
        ("MOST_ACTIVE", 0),
        ("TOP_PERC_GAIN", 0),
        ("TOP_PERC_LOSE", 0),
    )
    assert result.confirmed_charged_scanner_requests == 0


def test_scanner_gateway_pacing_retains_prior_source_rows() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def __init__(self) -> None:
            self.calls: list[str] = []

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            code = scan_codes[0]
            self.calls.append(code)
            if code == "TOP_PERC_GAIN":
                raise MarketDataPacingError(
                    "scanner",
                    "PACING_REQUEST_WINDOW_EXHAUSTED",
                )
            return (
                SimpleNamespace(
                    symbol="SPY",
                    rank=1,
                    source_scan=code,
                    contract_id=1,
                ),
            )

    class Pacing:
        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 2, "limit": 3}}

    gateway = Gateway()
    result = production_runtime_module._discover_underlyings(
        gateway,
        Pacing(),
    )

    assert gateway.calls == ["MOST_ACTIVE", "TOP_PERC_GAIN"]
    assert tuple(row.symbol for row in result.rows) == ("SPY",)
    assert result.completed_scan_codes == ("MOST_ACTIVE",)
    assert result.failed_scan_codes == ("TOP_PERC_GAIN", "TOP_PERC_LOSE")
    assert result.reason_codes == (
        "IBKR_SCANNER_PACING_DENIED",
        "IBKR_SCANNER_PACING_REQUEST_WINDOW_EXHAUSTED",
    )
    assert result.source_row_counts == (
        ("MOST_ACTIVE", 1),
        ("TOP_PERC_GAIN", 0),
        ("TOP_PERC_LOSE", 0),
    )
    assert result.confirmed_charged_scanner_requests == 1


def test_gateway_paced_generic_failures_do_not_claim_unattested_charges() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            assert rows_per_scan == 50
            raise RuntimeError(f"pre-lease disconnect: {scan_codes[0]}")

    class Pacing:
        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 1, "limit": 4}}

    result = production_runtime_module._discover_underlyings(
        Gateway(),
        Pacing(),
    )

    assert result.rows == ()
    assert result.completed_scan_codes == ()
    assert result.failed_scan_codes == (
        "MOST_ACTIVE",
        "TOP_PERC_GAIN",
        "TOP_PERC_LOSE",
    )
    assert result.confirmed_charged_scanner_requests == 0
    assert result.pacing_usage == {
        "scanner": {"used": 1, "limit": 4},
    }


def test_pipeline_surfaces_scanner_source_failure_as_no_trade() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            if scan_codes[0] == "TOP_PERC_GAIN":
                raise RuntimeError("bounded scanner source unavailable")
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def lease(self, request_class: str):
            return nullcontext(SimpleNamespace(allowed=True, reason=None))

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 3, "limit": 3}}

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=lambda **_kwargs: _formal_equity_pool_result(),
        clock=lambda: NOW,
    )

    payload = inputs.run(scan_run_id="scan-source-failure", slot_at=NOW)

    assert payload["status"] == "NO_TRADE"
    assert payload["decision"] == "NO_TRADE"
    assert payload["reasons"] == (
        "IBKR_SCANNER_TOP_PERC_GAIN_UNAVAILABLE",
    )
    assert payload["funnel_trace"]["discovered_underlyings"] == 0
    assert payload["funnel_trace"]["scanner_completed_scan_codes"] == (
        "MOST_ACTIVE",
        "TOP_PERC_LOSE",
    )
    assert payload["funnel_trace"]["scanner_failed_scan_codes"] == (
        "TOP_PERC_GAIN",
    )
    assert payload["funnel_trace"]["scanner_source_row_counts"] == (
        {"scan_code": "MOST_ACTIVE", "row_count": 0},
        {"scan_code": "TOP_PERC_GAIN", "row_count": 0},
        {"scan_code": "TOP_PERC_LOSE", "row_count": 0},
    )


def test_pipeline_all_empty_scanner_sources_are_explicitly_completed() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs: object) -> tuple[object, ...]:
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def lease(self, request_class: str):
            return nullcontext(SimpleNamespace(allowed=True, reason=None))

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 3, "limit": 3}}

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=lambda **_kwargs: _formal_equity_pool_result(),
        clock=lambda: NOW,
    )

    payload = inputs.run(scan_run_id="scan-all-empty-sources", slot_at=NOW)

    assert payload["funnel_trace"]["discovered_underlyings"] == 0
    assert payload["funnel_trace"]["scanner_completed_scan_codes"] == (
        "MOST_ACTIVE",
        "TOP_PERC_GAIN",
        "TOP_PERC_LOSE",
    )
    assert payload["funnel_trace"]["scanner_failed_scan_codes"] == ()
    assert payload["funnel_trace"]["scanner_source_row_counts"] == (
        {"scan_code": "MOST_ACTIVE", "row_count": 0},
        {"scan_code": "TOP_PERC_GAIN", "row_count": 0},
        {"scan_code": "TOP_PERC_LOSE", "row_count": 0},
    )


def test_pipeline_retains_partial_scanner_research_while_action_stays_blocked() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            code = scan_codes[0]
            if code == "TOP_PERC_GAIN":
                raise RuntimeError("bounded scanner source unavailable")
            symbol = "SPY" if code == "MOST_ACTIVE" else "QQQ"
            return (
                SimpleNamespace(
                    symbol=symbol,
                    rank=1,
                    source_scan=code,
                    contract_id=(1 if symbol == "SPY" else 2),
                    exchange="SMART",
                    industry="Diversified",
                    category="ETF",
                    subcategory="INDEX",
                ),
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def lease(self, request_class: str):
            return nullcontext(SimpleNamespace(allowed=True, reason=None))

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 3, "limit": 3}}

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=lambda **_kwargs: _formal_equity_pool_result("SPY"),
        clock=lambda: NOW,
    )
    inputs._coarse_candidate_outcome = lambda **_kwargs: (
        production_runtime_module._CoarseCandidateRead(
            ({"symbol": "SPY", "dte": 21},),
        )
    )

    payload = inputs.run(scan_run_id="scan-partial-source", slot_at=NOW)

    assert payload["status"] == "NO_TRADE"
    assert payload["decision"] == "NO_TRADE"
    assert payload["reasons"] == (
        "IBKR_SCANNER_TOP_PERC_GAIN_UNAVAILABLE",
    )
    assert tuple(row["symbol"] for row in payload["universe"]["scanner"]) == (
        "SPY",
        "QQQ",
    )
    assert payload["universe"]["coarse_contracts"] == (
        {"symbol": "SPY", "dte": 21},
    )
    assert payload["funnel_trace"]["deep_scan_requested"] == 1
    assert payload["funnel_trace"]["scanner_completed_scan_codes"] == (
        "MOST_ACTIVE",
        "TOP_PERC_LOSE",
    )
    assert payload["funnel_trace"]["scanner_failed_scan_codes"] == (
        "TOP_PERC_GAIN",
    )
    assert payload["funnel_trace"]["scanner_source_row_counts"] == (
        {"scan_code": "MOST_ACTIVE", "row_count": 1},
        {"scan_code": "TOP_PERC_GAIN", "row_count": 0},
        {"scan_code": "TOP_PERC_LOSE", "row_count": 1},
    )


def test_pipeline_retains_partial_research_after_gateway_scanner_pacing() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            code = scan_codes[0]
            if code == "TOP_PERC_GAIN":
                raise MarketDataPacingError(
                    "scanner",
                    "PACING_REQUEST_WINDOW_EXHAUSTED",
                )
            return (
                SimpleNamespace(
                    symbol="SPY",
                    rank=1,
                    source_scan=code,
                    contract_id=1,
                    exchange="SMART",
                    industry="Diversified",
                    category="ETF",
                    subcategory="INDEX",
                ),
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 2, "limit": 3}}

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        equity_pool_builder=lambda **_kwargs: _formal_equity_pool_result("SPY"),
        clock=lambda: NOW,
    )
    inputs._coarse_candidate_outcome = lambda **_kwargs: (
        production_runtime_module._CoarseCandidateRead(
            ({"symbol": "SPY", "dte": 21},),
        )
    )

    payload = inputs.run(scan_run_id="scan-partial-pacing", slot_at=NOW)

    assert payload["status"] == "NO_TRADE"
    assert payload["decision"] == "NO_TRADE"
    assert payload["reasons"] == (
        "IBKR_SCANNER_PACING_DENIED",
        "IBKR_SCANNER_PACING_REQUEST_WINDOW_EXHAUSTED",
    )
    assert tuple(row["symbol"] for row in payload["universe"]["scanner"]) == (
        "SPY",
    )
    assert payload["universe"]["coarse_contracts"] == (
        {"symbol": "SPY", "dte": 21},
    )
    assert payload["funnel_trace"]["scanner_completed_scan_codes"] == (
        "MOST_ACTIVE",
    )
    assert payload["funnel_trace"]["scanner_failed_scan_codes"] == (
        "TOP_PERC_GAIN",
        "TOP_PERC_LOSE",
    )
    assert payload["funnel_trace"]["scanner_source_row_counts"] == (
        {"scan_code": "MOST_ACTIVE", "row_count": 1},
        {"scan_code": "TOP_PERC_GAIN", "row_count": 0},
        {"scan_code": "TOP_PERC_LOSE", "row_count": 0},
    )


def test_pipeline_invalid_allocation_score_is_no_trade_without_deep_scan() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs: object) -> tuple[object, ...]:
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True)

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 3, "limit": 3}}

    class NoPositionManagement:
        status = "NO_TRADE"
        reason_codes = ("NO_OPEN_OPTION_POSITION",)

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY",),
        event_pool_reader=lambda: {
            "news": [
                {
                    "symbols": ["AAPL"],
                    "event_impact_score": "NaN",
                }
            ]
        },
        clock=lambda: NOW,
    )
    inputs.bind_management_refresher(lambda: NoPositionManagement())

    def forbidden_deep_scan(**_kwargs: object):
        raise AssertionError("invalid allocation must stop before deep scan")

    inputs._coarse_candidate_outcome = forbidden_deep_scan  # type: ignore[method-assign]

    payload = inputs.run(scan_run_id="scan-invalid-allocation", slot_at=NOW)

    assert payload["status"] == "NO_TRADE"
    assert payload["decision"] == "NO_TRADE"
    assert payload["reasons"] == ("RESEARCH_ALLOCATION_INPUT_INVALID",)
    assert payload["funnel_trace"]["deep_scan_requested"] == 0
    assert "research_allocation" not in payload["funnel_trace"]
    assert payload["universe"]["coarse_contracts"] == ()


def test_event_rows_skip_scoreless_unbound_news_but_reject_bound_bad_score() -> None:
    rows = ProductionPipelineInputs._event_rows(
        None,  # type: ignore[arg-type]
        {
            "news": (
                {
                    "symbols": (),
                    "symbol_binding": {"status": "UNBOUND"},
                },
                {
                    "symbols": ("SPY",),
                    "event_impact_score": "87.75",
                },
            )
        },
    )

    assert tuple(row["symbol"] for row in rows) == ("SPY",)
    assert rows[0]["deterministic_score"] == Decimal("87.75")

    with pytest.raises(
        production_runtime_module.ResearchAllocationInputError,
        match="RESEARCH_ALLOCATION_INPUT_INVALID",
    ):
        ProductionPipelineInputs._event_rows(
            None,  # type: ignore[arg-type]
            {"news": ({"symbols": ("SPY",), "event_impact_score": None},)},
        )


def test_event_rows_accept_only_current_deterministic_macro_research_proxy() -> None:
    binding = ResearchProxyBinding(
        event_category="US_INFLATION",
        source="JIN10",
        proxy_symbol="SPY",
    )

    rows = ProductionPipelineInputs._event_rows(
        None,  # type: ignore[arg-type]
        {
            "news": (
                {
                    "symbols": (),
                    "symbol_binding": {"status": "UNBOUND"},
                    "research_proxy_binding": binding.as_dict(),
                    "event_impact_score": "88.5",
                },
            ),
        },
    )

    assert tuple(row["symbol"] for row in rows) == ("SPY",)
    assert rows[0]["deterministic_score"] == Decimal("88.5")
    assert rows[0]["source"] == "MACRO_RESEARCH_PROXY_SUPPORTING_ONLY"
    assert rows[0]["eligibility_effect"] == "NONE"
    assert rows[0]["risk_effect"] == "NONE"

    tampered = dict(binding.as_dict())
    tampered["mapping_hash"] = "0" * 64
    assert ProductionPipelineInputs._event_rows(
        None,  # type: ignore[arg-type]
        {
            "news": (
                {
                    "symbols": (),
                    "research_proxy_binding": tampered,
                    "event_impact_score": "99",
                },
            ),
        },
    ) == ()


def test_pipeline_passes_verified_news_into_equity_pool_discovery() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs: object) -> tuple[object, ...]:
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True)

        def usage(self) -> dict[str, object]:
            return {"scanner": {"used": 3, "limit": 3}}

    captured_rows: list[Mapping[str, object]] = []

    def build_pool(**kwargs: object) -> object:
        captured_rows.extend(kwargs["scanner_rows"])  # type: ignore[arg-type]
        return _formal_equity_pool_result()

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY",),
        event_pool_reader=lambda: {
            "news": (
                {
                    "symbols": ("TSLA",),
                    "event_impact_score": "90",
                    "research_rank": 1,
                    "symbol_binding": {
                        "status": "VERIFIED_PROVIDER_RELATED",
                    },
                },
            ),
        },
        equity_pool_builder=build_pool,
        clock=lambda: NOW,
    )

    payload = inputs.run(scan_run_id="scan-news-discovery", slot_at=NOW)

    assert captured_rows[0]["symbol"] == "TSLA"
    assert captured_rows[0]["source_scan"] == "NEWS_EVENT_POOL"
    assert payload["candidate_evidence_references"] == {}


def test_underlying_quote_batches_isolate_one_failure_with_omission_evidence() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.quote_calls: list[tuple[str, ...]] = []

        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs) -> tuple[object, ...]:
            return ()

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            self.quote_calls.append(symbols)
            if "B3" in symbols:
                raise RuntimeError("one poisoned broker batch")
            return tuple(
                _live_underlying_quote(
                    symbol,
                    close=Decimal("499"),
                    market_price=Decimal("500"),
                )
                for symbol in symbols
            )

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("500"), Decimal("505")),
                ),
            )

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            **_kwargs,
        ) -> tuple[OptionContractRef, ...]:
            return tuple(
                OptionContractRef(
                    contract_id=9000 + index,
                    contract_id_ex=f"{9000 + index}@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right="C",
                    exchange="SMART",
                    trading_class=symbol,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(strikes, start=1)
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def __init__(self) -> None:
            self.calls: list[str] = []

        def decision(self, request_class: str) -> object:
            self.calls.append(request_class)
            return SimpleNamespace(allowed=True)

        def usage(self) -> dict[str, dict[str, int]]:
            return {"scanner": {"used": 3, "limit": 3}}

    class NoPositionManagement:
        status = "NO_TRADE"
        reason_codes = ("NO_OPEN_GLD_POSITION",)

    gateway = Gateway()
    pacing = Pacing()
    inputs = ProductionPipelineInputs(
        gateway,  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        object(),
        core_symbols=("B1", "B2", "B3", "B4", "QQQ"),
        equity_pool_builder=lambda **_kwargs: _formal_equity_pool_result(
            "B1", "B2", "B3", "B4", "QQQ",
        ),
        clock=lambda: NOW,
    )

    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-batched-underlyings",
        slot_at=NOW,
        symbols=("B1", "B2", "B3", "B4", "QQQ"),
        equity_theses=_bullish_equity_theses("B1", "B2", "B3", "B4", "QQQ"),
    )

    assert gateway.quote_calls == [
        ("B1", "B2", "B3", "B4"),
        ("B1", "B2"),
        ("B3", "B4"),
        ("B3",),
        ("B4",),
        ("QQQ", "SPY"),
    ]
    assert len(outcome.candidates) == 8
    assert {item["symbol"] for item in outcome.candidates} == {
        "B1",
        "B2",
        "B4",
        "QQQ",
    }
    assert {item["structure"] for item in outcome.candidates} == {
        "LONG_OPTION",
        "DEBIT_VERTICAL",
    }
    assert outcome.missing_symbols == ()
    assert outcome.reason_codes == ()
    assert outcome.quote_excluded_symbols == ("B3",)
    assert {
        tuple(leg["side"] for leg in item["legs"])
        for item in outcome.candidates
    } == {("LONG",), ("LONG", "SHORT")}
    assert pacing.calls[:11] == [
        "secdef",
        "secdef",
        "secdef",
        "secdef",
        "secdef",
        "snapshot_quote",
        "snapshot_quote",
        "snapshot_quote",
        "snapshot_quote",
        "snapshot_quote",
        "snapshot_quote",
    ]

    inputs.bind_management_refresher(lambda: NoPositionManagement())
    payload = inputs.run(
        scan_run_id="scan-partial-underlyings",
        slot_at=NOW,
    )

    assert "status" not in payload
    assert "decision" not in payload
    assert len(payload["universe"]["coarse_contracts"]) == 8
    assert {
        item["structure"]
        for item in payload["universe"]["coarse_contracts"]
    } == {"LONG_OPTION", "DEBIT_VERTICAL"}
    assert payload["reasons"] == ()
    assert payload["funnel_trace"]["underlying_quote_excluded_symbols"] == (
        "B3",
    )


def test_underlying_quote_isolation_releases_real_single_concurrency_lease() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.calls: list[tuple[str, ...]] = []

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            self.calls.append(symbols)
            if "B3" in symbols:
                raise RuntimeError("one poisoned broker symbol")
            return tuple(
                SimpleNamespace(
                    symbol=symbol,
                    market_price=Decimal("100"),
                    close=Decimal("99"),
                )
                for symbol in symbols
            )

    capability = MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=NOW,
        source="broker_disclosed",
        request_classes={
            name: {
                "max_concurrency": 1,
                "request_window": 60,
                "max_requests": 30,
                "cooldown": 1,
            }
            for name in PACING_REQUEST_CLASSES
        },
        signer="human:xujie",
    )
    pacing = RequestBudgetByClass(capability, now=NOW)
    gateway = Gateway()

    result = production_runtime_module._read_underlying_quotes(
        gateway,
        pacing,
        ("B1", "B2", "B3", "B4"),
    )

    assert gateway.calls == [
        ("B1", "B2", "B3", "B4"),
        ("B1", "B2"),
        ("B3", "B4"),
        ("B3",),
        ("B4",),
    ]
    assert tuple(row.symbol for row in result.rows) == ("B1", "B2", "B4")
    assert result.observed_symbols == ("B1", "B2", "B4")
    assert result.missing_symbols == ("B3",)
    assert result.failed_symbols == ("B3",)
    assert result.reason_codes == ()
    assert pacing.usage()["snapshot_quote"] == {"used": 5, "limit": 30}


def test_mid_loop_underlying_pacing_exhaustion_blocks_the_complete_pipeline() -> None:
    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs) -> tuple[object, ...]:
            return ()

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(
                SimpleNamespace(symbol=symbol, market_price=Decimal("100"))
                for symbol in symbols
            )

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def __init__(self) -> None:
            self.snapshot_calls = 0

        def lease(self, request_class: str):
            if request_class == "snapshot_quote":
                self.snapshot_calls += 1
                allowed = self.snapshot_calls == 1
                reason = None if allowed else "PACING_REQUEST_WINDOW_EXHAUSTED"
                return nullcontext(SimpleNamespace(allowed=allowed, reason=reason))
            return nullcontext(SimpleNamespace(allowed=True, reason=None))

        def decision(self, request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, dict[str, int]]:
            return {"scanner": {"used": 3, "limit": 3}}

    class NoPositionManagement:
        status = "NO_TRADE"
        reason_codes = ("NO_OPEN_GLD_POSITION",)

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("B1", "B2", "B3", "B4", "QQQ"),
        equity_pool_builder=lambda **_kwargs: _formal_equity_pool_result(
            "B1", "B2", "B3", "B4", "QQQ",
        ),
        clock=lambda: NOW,
    )
    inputs.bind_management_refresher(lambda: NoPositionManagement())

    payload = inputs.run(scan_run_id="scan-pacing-exhausted", slot_at=NOW)

    assert payload["status"] == "NO_TRADE"
    assert payload["decision"] == "NO_TRADE"
    assert payload["universe"]["coarse_contracts"] == ()
    assert "UNDERLYING_QUOTE_PACING_DENIED" in payload["reasons"]
    assert "UNDERLYING_QUOTE_PACING_REQUEST_WINDOW_EXHAUSTED" in payload["reasons"]
    assert payload["funnel_trace"]["underlying_quote_missing_symbols"] == (
        "QQQ",
        "SPY",
    )


def test_pipeline_optionability_rechecks_dte_boundaries_before_quotes() -> None:
    asof = date(2026, 8, 4)
    crossover_slot = datetime(2026, 8, 5, 1, 0, tzinfo=timezone.utc)

    class Gateway:
        def __init__(self) -> None:
            self.quote_calls: list[tuple[str, ...]] = []

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            dte = int(symbol[1:])
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=asof + timedelta(days=dte),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            self.quote_calls.append(symbols)
            return tuple(
                _live_underlying_quote(
                    symbol,
                    observed_at=crossover_slot,
                )
                for symbol in symbols
            )

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            right = rights[0]
            return tuple(
                OptionContractRef(
                    contract_id=int(symbol[1:]) * 100 + index,
                    contract_id_ex=f"{int(symbol[1:]) * 100 + index}@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{right}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=right,  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(strikes, start=1)
            )

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    gateway = Gateway()
    inputs = ProductionPipelineInputs(
        gateway,  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: crossover_slot,
    )

    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-dte-boundaries",
        slot_at=crossover_slot,
        symbols=("B13", "B14", "B35", "B36"),
        equity_theses=_bullish_equity_theses("B13", "B14", "B35", "B36"),
    )

    assert gateway.quote_calls == [("B14", "B35", "SPY")]
    assert len(outcome.candidates) == 4
    assert {item["symbol"] for item in outcome.candidates} == {"B14", "B35"}
    assert {item["dte"] for item in outcome.candidates} == {14, 35}
    assert {item["structure"] for item in outcome.candidates} == {
        "LONG_OPTION",
        "DEBIT_VERTICAL",
    }
    assert outcome.excluded_symbols == ("B13", "B36")


def test_pipeline_reserves_broker_evidence_within_approved_secdef_budget() -> None:
    symbols = tuple(f"B{index:02d}" for index in range(18))
    optionable = set(symbols[:2])

    class Gateway:
        market_data_pacing_enabled = True

        def __init__(self, pacing: RequestBudgetByClass) -> None:
            self.pacing = pacing
            self.snapshot_secdef_calls = 0

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            for _ in range(2):
                with self.pacing.lease("secdef") as decision:
                    assert decision.allowed
            if symbol not in optionable:
                return ()
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, requested: tuple[str, ...]) -> tuple[object, ...]:
            for _ in requested:
                with self.pacing.lease("secdef") as decision:
                    assert decision.allowed
            for _ in requested:
                with self.pacing.lease("snapshot_quote") as decision:
                    assert decision.allowed
            return tuple(
                _live_underlying_quote(symbol)
                for symbol in requested
            )

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            symbol_number = int(symbol[1:])
            contracts = tuple(
                OptionContractRef(
                    contract_id=(
                        symbol_number * 1000
                        + (100 if right == "C" else 200)
                        + index
                    ),
                    contract_id_ex=(
                        f"{symbol_number * 1000 + (100 if right == 'C' else 200) + index}@SMART"
                    ),
                    symbol=symbol,
                    local_symbol=f"{symbol}-{right}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=right,
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for right in rights
                for index, strike in enumerate(strikes, start=1)
            )
            for _ in contracts:
                with self.pacing.lease("secdef") as decision:
                    assert decision.allowed
            return contracts

        def account_snapshot(self) -> dict[str, object]:
            return {"currency": "USD", "net_liquidation": Decimal("2500")}

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
            for _ in contracts:
                with self.pacing.lease("secdef") as decision:
                    assert decision.allowed
                self.snapshot_secdef_calls += 1
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
                    source="IBKR_REQ_CONTRACT_DETAILS",
                )
                for item in contracts
            )

        def option_quote_batch(
            self,
            contracts: tuple[OptionContractRef, ...],
        ) -> OptionQuoteBatch:
            for _ in contracts:
                with self.pacing.lease("snapshot_quote") as decision:
                    assert decision.allowed
            quotes = tuple(
                BatchedOptionQuote(
                    contract_id=item.contract_id,
                    batch_id="secdef-budget-batch",
                    request_id=f"request-{item.contract_id}",
                    requested_at=NOW - timedelta(seconds=2),
                    observed_at=NOW - timedelta(seconds=1),
                    completed_at=NOW,
                    source="IBKR_REQ_TICKERS_READONLY",
                    bid=Decimal("1.00"),
                    ask=Decimal("1.10"),
                    exchange_time=NOW - timedelta(seconds=1),
                    implied_volatility=Decimal("0.20"),
                    volume=100,
                    open_interest=1000,
                )
                for item in contracts
            )
            return OptionQuoteBatch(
                batch_id="secdef-budget-batch",
                status=QuoteBatchStatus.COMPLETE,
                requested_at=NOW - timedelta(seconds=2),
                completed_at=NOW,
                source="IBKR_REQ_TICKERS_READONLY",
                quotes=quotes,
                observed_at=NOW - timedelta(seconds=1),
            )

        def underlying_iv_history(
            self,
            symbol: str,
            *,
            end_at: datetime,
        ) -> UnderlyingIvHistory:
            with self.pacing.lease("secdef") as decision:
                assert decision.allowed
            with self.pacing.lease("historical") as decision:
                assert decision.allowed
            return _authoritative_iv_history(symbol, end_at=end_at)

    capability = MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=NOW,
        source="conservative_default",
        request_classes={
            name: {
                "max_concurrency": 2,
                "request_window": 60,
                "max_requests": 30,
                "cooldown": 2,
            }
            for name in PACING_REQUEST_CLASSES
        },
        signer="human:xujie",
    )
    pacing = RequestBudgetByClass(capability, now=NOW)
    gateway = Gateway(pacing)
    # Consecutive 30-second heartbeats retain both broker-calendar reads in the
    # same rolling 60-second SECDEF window: two requests per heartbeat.
    for _ in range(4):
        with pacing.lease("secdef") as decision:
            assert decision.allowed
    inputs = ProductionPipelineInputs(
        gateway,  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )

    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-secdef-budget-boundary",
        slot_at=NOW,
        symbols=symbols,
        equity_theses=_bullish_equity_theses(*symbols),
    )

    secdef_usage = pacing.usage()["secdef"]
    assert secdef_usage["used"] == 15
    assert secdef_usage["used"] <= secdef_usage["limit"] == 30
    assert len(outcome.candidates) == 4
    assert {item["symbol"] for item in outcome.candidates} == optionable
    assert {item["structure"] for item in outcome.candidates} == {
        "LONG_OPTION",
        "DEBIT_VERTICAL",
    }
    assert outcome.reason_codes == ()
    assert outcome.missing_symbols == ()

    universe = UniverseFunnel(pacing).run(
        positions=(),
        core_etfs=(),
        event_pool=(),
        scanner=(),
        coarse_contracts=outcome.candidates,
    )
    assert len(universe.finalists) == 4
    assert {item["structure"] for item in universe.finalists} == {
        "LONG_OPTION",
        "DEBIT_VERTICAL",
    }
    assert pacing.usage()["secdef"] == {"used": 15, "limit": 30}

    class PolicyResolver:
        def policy_contract_document(self, _resolved_policy: object) -> Mapping[str, object]:
            return {"schema": "test-policy"}

    builder = BrokerSnapshotBuilder(
        gateway,  # type: ignore[arg-type]
        clock=lambda: NOW,
    )
    acquisition = ProductionBrokerEvidenceAcquisition(
        builder,  # type: ignore[arg-type]
        ProductionOptionsEvidenceAcquisition(),
        object(),  # type: ignore[arg-type]
        pacing,
        object(),
        execution_cost_contract={},
        policy_resolver=PolicyResolver(),
        clock=lambda: NOW,
    )
    evidence = acquisition.acquire(
        scan_run_id="scan-secdef-budget-boundary",
        universe={"finalists": universe.finalists},
        context={},
    )

    assert gateway.snapshot_secdef_calls == 8
    assert isinstance(evidence["broker_snapshot"], AtomicBrokerSnapshot)
    assert evidence["broker_snapshot"].complete, evidence["broker_snapshot"].reason_codes
    assert evidence["reasons"] == ("STRATEGY_NAV_SOURCE_UNAVAILABLE",)
    assert pacing.usage()["secdef"] == {"used": 25, "limit": 30}
    assert pacing.usage()["snapshot_quote"] == {"used": 7, "limit": 30}
    assert pacing.usage()["historical"] == {"used": 2, "limit": 30}


def test_direct_top10_fallback_fits_installed_wire_secdef_limit() -> None:
    capability = MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=NOW,
        source="conservative_default",
        request_classes={
            name: {
                "max_concurrency": 2,
                "request_window": 60,
                "max_requests": 30,
                "cooldown": 2,
            }
            for name in PACING_REQUEST_CLASSES
        },
        signer="human:xujie",
    )
    pacing = RequestBudgetByClass(capability, now=NOW)

    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            for _ in range(2):
                with pacing.lease("secdef") as decision:
                    assert decision.allowed
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_indicative_quotes(
            self,
            symbols: tuple[str, ...],
        ) -> tuple[object, ...]:
            for _ in symbols:
                with pacing.lease("secdef") as decision:
                    assert decision.allowed
                with pacing.lease("snapshot_quote") as decision:
                    assert decision.allowed
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            symbol_number = int(symbol[1:])
            contracts = tuple(
                OptionContractRef(
                    contract_id=symbol_number * 10_000 + (index + 1) * 1000 + int(strike),
                    contract_id_ex=(
                        f"{symbol_number * 10_000 + (index + 1) * 1000 + int(strike)}@SMART"
                    ),
                    symbol=symbol,
                    local_symbol=f"{symbol}-{rights[0]}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=rights[0],  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(strikes)
            )
            for _ in contracts:
                with pacing.lease("secdef") as decision:
                    assert decision.allowed
            return contracts

    symbols = tuple(f"S{index:02d}" for index in range(5))
    result = DirectTop10StructureSource(
        Gateway(),  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        clock=lambda: NOW,
        core_symbols=symbols,
        include_scanner=False,
        maximum_optionability_attempts=5,
        maximum_optionable=5,
        maximum_structures=5,
        indicative_underlyings=True,
    ).resolve_top10(scheduled_for=NOW)

    assert len(result.structures) == 5
    assert result.reason_codes == ()
    assert result.missing_symbols == ()
    assert pacing.usage()["secdef"] == {"used": 25, "limit": 30}
    assert pacing.usage()["snapshot_quote"] == {"used": 5, "limit": 30}


@pytest.mark.parametrize(
    ("close", "expected_right"),
    ((Decimal("99"), "C"), (Decimal("101"), "P")),
)
def test_pipeline_selects_one_direction_from_current_price_versus_close(
    close: Decimal,
    expected_right: str,
) -> None:
    selected = production_runtime_module._preferred_directional_vertical(
        (Decimal("95"), Decimal("100"), Decimal("105")),
        Decimal("100"),
        close=close,
    )

    assert selected is not None
    assert selected[0] == expected_right


@pytest.mark.parametrize(
    ("strikes", "close"),
    (
        ((Decimal("95"), Decimal("100"), Decimal("105")), None),
        ((Decimal("95"), Decimal("100"), Decimal("105")), Decimal("0")),
        ((Decimal("95"), Decimal("100"), Decimal("105")), Decimal("100")),
        ((Decimal("100"), Decimal("105")), Decimal("101")),
        ((Decimal("95"), Decimal("100")), Decimal("99")),
    ),
)
def test_pipeline_direction_selection_fails_closed_without_matching_evidence(
    strikes: tuple[Decimal, ...],
    close: Decimal | None,
) -> None:
    assert production_runtime_module._preferred_directional_vertical(
        strikes,
        Decimal("100"),
        close=close,
    ) is None


def test_wire_level_quote_pacing_denial_is_not_reported_as_complete() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def underlying_quotes(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            raise MarketDataPacingError(
                "snapshot_quote",
                "PACING_REQUEST_WINDOW_EXHAUSTED",
            )

    result = production_runtime_module._read_underlying_quotes(
        Gateway(),
        object(),
        ("SPY",),
    )

    assert result.complete is False
    assert result.rows == ()
    assert result.reason_codes == (
        "UNDERLYING_QUOTE_PACING_DENIED",
        "UNDERLYING_QUOTE_PACING_REQUEST_WINDOW_EXHAUSTED",
    )


def test_wire_level_optionability_pacing_denial_is_not_transport_failure() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, _symbol: str, **_kwargs) -> tuple[object, ...]:
            raise MarketDataPacingError(
                "secdef",
                "PACING_REQUEST_WINDOW_EXHAUSTED",
            )

    result = production_runtime_module._preflight_optionable_underlyings(
        Gateway(),
        object(),
        ("SPY",),
        asof=date(2026, 8, 5),
    )

    assert result.complete is False
    assert result.unresolved_symbols == ("SPY",)
    assert result.reason_codes == (
        "OPTIONABILITY_PACING_DENIED",
        "OPTIONABILITY_PACING_REQUEST_WINDOW_EXHAUSTED",
    )


def test_wire_level_optionability_defaults_to_two_atomic_snapshot_candidates() -> None:
    calls: list[str] = []

    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            calls.append(symbol)
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

    symbols = ("SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA")
    result = production_runtime_module._preflight_optionable_underlyings(
        Gateway(),
        object(),
        symbols,
        asof=date(2026, 8, 5),
    )

    assert tuple(dict(result.expirations)) == symbols[:2]
    assert calls == list(symbols[:2])


def test_pipeline_qualification_pacing_denial_discards_partial_candidates() -> None:
    class Gateway:
        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(
                _live_underlying_quote(symbol)
                for symbol in symbols
            )

        def qualify_option_contracts(self, *_args, **_kwargs) -> tuple[object, ...]:
            raise AssertionError("denied pacing must prevent the broker call")

    class Pacing:
        ready = True

        def __init__(self) -> None:
            self.secdef_calls = 0

        def decision(self, request_class: str) -> object:
            if request_class == "secdef":
                self.secdef_calls += 1
                allowed = self.secdef_calls == 1
                reason = None if allowed else "PACING_REQUEST_WINDOW_EXHAUSTED"
                return SimpleNamespace(allowed=allowed, reason=reason)
            return SimpleNamespace(allowed=True, reason=None)

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )

    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-qualification-denied",
        slot_at=NOW,
        symbols=("B1",),
        equity_theses=_bullish_equity_theses("B1"),
    )

    assert outcome.candidates == ()
    assert outcome.missing_symbols == ("B1",)
    assert outcome.reason_codes == (
        "OPTION_QUALIFICATION_PACING_DENIED",
        "OPTION_QUALIFICATION_PACING_REQUEST_WINDOW_EXHAUSTED",
    )


def test_wire_level_pipeline_qualification_pacing_denial_is_not_transport_failure() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(
                _live_underlying_quote(symbol)
                for symbol in symbols
            )

        def qualify_option_contracts(self, *_args, **_kwargs) -> tuple[object, ...]:
            raise MarketDataPacingError(
                "secdef",
                "PACING_REQUEST_WINDOW_EXHAUSTED",
            )

    class Pacing:
        ready = True

        def usage(self) -> dict[str, dict[str, int]]:
            return {
                "secdef": {"used": 0, "limit": 30},
                "streaming_quote": {"used": 0, "limit": 30},
            }

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )

    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-wire-qualification-denied",
        slot_at=NOW,
        symbols=("SPY",),
        equity_theses=_bullish_equity_theses("SPY"),
    )

    assert outcome.candidates == ()
    assert outcome.missing_symbols == ("SPY",)
    assert outcome.reason_codes == (
        "OPTION_QUALIFICATION_PACING_DENIED",
        "OPTION_QUALIFICATION_PACING_REQUEST_WINDOW_EXHAUSTED",
    )


def test_pipeline_preserves_exact_option_qualification_timeout_reason() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, ...],
            *,
            rights: tuple[str, ...],
            **_kwargs: object,
        ) -> tuple[object, ...]:
            raise OptionQualificationError(
                "OPTION_QUALIFICATION_TIMEOUT",
                symbol=symbol,
                expiration=expiration,
                requested_count=len(strikes),
                completed_count=0,
                failed_right=rights[0],
                failed_strike=strikes[0],
            )

    class Pacing:
        ready = True

        def usage(self) -> dict[str, dict[str, int]]:
            return {
                "secdef": {"used": 0, "limit": 30},
                "streaming_quote": {"used": 0, "limit": 30},
            }

    outcome = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-qualification-timeout",
        slot_at=NOW,
        symbols=("SPY",),
        equity_theses=_bullish_equity_theses("SPY"),
    )

    assert outcome.reason_codes == ("OPTION_QUALIFICATION_TIMEOUT",)
    assert outcome.quote_exclusion_reasons == (
        ("SPY", "OPTION_QUALIFICATION_TIMEOUT"),
    )


def test_pipeline_partial_qualification_keeps_surviving_candidate() -> None:
    class Gateway:
        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(
                _live_underlying_quote(symbol)
                for symbol in symbols
            )

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            first = OptionContractRef(
                contract_id=9001 if symbol == "B1" else 9101,
                contract_id_ex=f"{9001 if symbol == 'B1' else 9101}@SMART",
                symbol=symbol,
                local_symbol=f"{symbol}-{rights[0]}-{strikes[0]}",
                expiration=expiration,
                strike=strikes[0],
                right=rights[0],  # type: ignore[arg-type]
                exchange=exchange,
                trading_class=trading_class,
                multiplier=100,
                currency="USD",
            )
            if symbol == "B1":
                return (first,)
            return (
                first,
                OptionContractRef(
                    contract_id=9102,
                    contract_id_ex="9102@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{rights[0]}-{strikes[1]}",
                    expiration=expiration,
                    strike=strikes[1],
                    right=rights[0],  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                ),
            )

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )

    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-partial-qualification",
        slot_at=NOW,
        symbols=("B1", "B2"),
        equity_theses=_bullish_equity_theses("B1", "B2"),
    )

    assert tuple(row["symbol"] for row in outcome.candidates) == ("B1", "B2", "B2")
    assert tuple(row["structure"] for row in outcome.candidates) == (
        "LONG_OPTION",
        "LONG_OPTION",
        "DEBIT_VERTICAL",
    )
    assert outcome.reason_codes == ()
    assert outcome.missing_symbols == ()
    assert outcome.quote_excluded_symbols == ()
    assert (
        "B1",
        "DEBIT_VERTICAL_QUALIFICATION_INCOMPLETE",
    ) in outcome.quote_exclusion_reasons


@pytest.mark.parametrize(
    ("fail_first", "expected_symbol", "expected_attempted", "expected_deferred"),
    (
        (True, "B2", ("B1", "B2"), ()),
        (False, "B1", ("B1",), ("B2",)),
    ),
)
def test_complex_paced_qualification_uses_second_symbol_only_after_first_failure(
    fail_first: bool,
    expected_symbol: str,
    expected_attempted: tuple[str, ...],
    expected_deferred: tuple[str, ...],
) -> None:
    qualification_calls: list[tuple[str, str]] = []

    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(
                        Decimal("90"),
                        Decimal("95"),
                        Decimal("100"),
                        Decimal("105"),
                        Decimal("110"),
                    ),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, ...],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            right = rights[0]
            qualification_calls.append((symbol, right))
            if fail_first and symbol == "B1":
                return ()
            if not fail_first and symbol == "B2":
                raise AssertionError("second symbol must remain deferred after first success")
            symbol_offset = 0 if symbol == "B1" else 10_000
            right_offset = 0 if right == "C" else 1_000
            return tuple(
                OptionContractRef(
                    contract_id=30_000 + symbol_offset + right_offset + int(strike),
                    contract_id_ex=(
                        f"{30_000 + symbol_offset + right_offset + int(strike)}@SMART"
                    ),
                    symbol=symbol,
                    local_symbol=f"{symbol}-{right}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=right,  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for strike in strikes
            )

    theses = {
        "rows": tuple(
            {
                "symbol": symbol,
                "direction_label": "BULLISH",
                "uncertainty": "0.30",
            }
            for symbol in ("B1", "B2")
        )
    }

    class Pacing:
        ready = True

        def usage(self) -> dict[str, dict[str, int]]:
            return {
                "secdef": {"used": 0, "limit": 30},
                "streaming_quote": {"used": 0, "limit": 30},
            }

    outcome = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-complex-pacing-fallback",
        slot_at=NOW,
        symbols=("B1", "B2"),
        equity_theses=theses,
    )

    assert {str(row["symbol"]) for row in outcome.candidates} == {expected_symbol}
    assert outcome.attempted_symbols == expected_attempted
    assert outcome.deferred_symbols == expected_deferred
    if fail_first:
        assert qualification_calls == [
            ("B1", "C"),
            ("B1", "P"),
            ("B2", "C"),
            ("B2", "P"),
        ]
    else:
        assert qualification_calls == [("B1", "C"), ("B1", "P")]


def test_paced_pipeline_rebuilds_structure_from_actual_qualified_strike_grid() -> None:
    qualification_requests: list[tuple[Decimal, ...]] = []

    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("100"), Decimal("105"), Decimal("110")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, ...],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            qualification_requests.append(strikes)
            # SecDef advertised 100/105/110 as one aggregate set, but the
            # selected monthly expiration only qualifies the latter pair.
            return tuple(
                OptionContractRef(
                    contract_id=50_000 + index,
                    contract_id_ex=f"{50_000 + index}@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{rights[0]}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=rights[0],  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(
                    (Decimal("105"), Decimal("110")),
                    start=1,
                )
                if strike in strikes
            )

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, dict[str, int]]:
            return {
                "secdef": {"used": 1, "limit": 30},
                # Two exact legs need two standard option subscriptions.  The
                # production path must not reserve two additional unsupported
                # tick-by-tick BidAsk requests.
                "streaming_quote": {"used": 28, "limit": 30},
            }

    outcome = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-actual-strike-grid",
        slot_at=NOW,
        symbols=("SPY",),
        equity_theses=_bullish_equity_theses("SPY"),
    )

    # Only two streaming slots remain, so the adjacent-strike fallback is not
    # requested unless its downstream AtomicBrokerSnapshot can also fit.
    assert qualification_requests == [
        (Decimal("100"), Decimal("105")),
    ]
    assert tuple(row["structure"] for row in outcome.candidates) == (
        "LONG_OPTION",
    )
    assert tuple(
        tuple(Decimal(str(leg["strike"])) for leg in row["legs"])
        for row in outcome.candidates
    ) == (
        (Decimal("105"),),
    )
    assert outcome.reason_codes == ()
    assert outcome.missing_symbols == ()


def test_paced_pipeline_skips_symbol_without_downstream_evidence_headroom() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("100"), Decimal("105"), Decimal("110")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(self, *_args, **_kwargs) -> tuple[object, ...]:
            raise AssertionError("qualification must not consume reserved headroom")

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, dict[str, int]]:
            return {
                "secdef": {"used": 25, "limit": 30},
                "streaming_quote": {"used": 0, "limit": 30},
            }

    outcome = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-insufficient-evidence-headroom",
        slot_at=NOW,
        symbols=("SPY",),
        equity_theses=_bullish_equity_theses("SPY"),
    )

    assert outcome.candidates == ()
    assert outcome.reason_codes == (
        "OPTION_PIPELINE_PACING_HEADROOM_INSUFFICIENT",
    )
    assert outcome.attempted_symbols == ("SPY",)
    assert outcome.deferred_symbols == ()


def test_paced_pipeline_reserves_snapshot_headroom_across_underlyings() -> None:
    qualification_symbols: list[str] = []

    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, ...],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            qualification_symbols.append(symbol)
            return tuple(
                OptionContractRef(
                    contract_id=(10_000 if symbol == "B1" else 20_000) + index,
                    contract_id_ex=(
                        f"{(10_000 if symbol == 'B1' else 20_000) + index}@SMART"
                    ),
                    symbol=symbol,
                    local_symbol=f"{symbol}-{rights[0]}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=rights[0],  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(strikes, start=1)
            )

    class Pacing:
        ready = True

        def usage(self) -> dict[str, dict[str, int]]:
            # One two-leg underlying fits.  Two independently look as if they
            # fit, but their combined future AtomicBrokerSnapshot does not.
            return {
                "secdef": {"used": 21, "limit": 30},
                "streaming_quote": {"used": 21, "limit": 30},
            }

    theses = {
        "rows": tuple(
            {
                "symbol": symbol,
                "direction_label": "BULLISH",
                "uncertainty": "0.20",
            }
            for symbol in ("B1", "B2")
        )
    }
    outcome = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-cumulative-snapshot-headroom",
        slot_at=NOW,
        symbols=("B1", "B2"),
        equity_theses=theses,
    )

    assert qualification_symbols == ["B1"]
    assert {str(row["symbol"]) for row in outcome.candidates} == {"B1"}
    assert (
        "B2",
        "OPTION_PIPELINE_PACING_HEADROOM_INSUFFICIENT",
    ) in outcome.quote_exclusion_reasons


@pytest.mark.parametrize(
    ("usage_mode", "expected_reason"),
    (
        ("missing", "OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE"),
        ("raises", "OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE"),
        ("missing_class", "OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE"),
        ("invalid_bool", "OPTION_PIPELINE_PACING_USAGE_INVALID"),
        ("invalid_row", "OPTION_PIPELINE_PACING_USAGE_INVALID"),
    ),
)
def test_paced_pipeline_requires_proved_usage_before_qualification(
    usage_mode: str,
    expected_reason: str,
) -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(self, *_args, **_kwargs) -> tuple[object, ...]:
            raise AssertionError("unproved pacing usage must stop qualification")

    class Pacing:
        ready = True

        def usage(self) -> object:
            if usage_mode == "raises":
                raise RuntimeError("usage unavailable")
            if usage_mode == "missing_class":
                return {"secdef": {"used": 1, "limit": 30}}
            if usage_mode == "invalid_bool":
                return {
                    "secdef": {"used": True, "limit": 30},
                    "streaming_quote": {"used": 0, "limit": 30},
                }
            if usage_mode == "invalid_row":
                return {
                    "secdef": "invalid",
                    "streaming_quote": {"used": 0, "limit": 30},
                }
            return {
                "secdef": {"used": 1, "limit": 30},
                "streaming_quote": {"used": 0, "limit": 30},
            }

    pacing: object = (
        SimpleNamespace(ready=True)
        if usage_mode == "missing"
        else Pacing()
    )
    outcome = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        pacing,  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id=f"scan-pacing-usage-{usage_mode}",
        slot_at=NOW,
        symbols=("SPY",),
        equity_theses=_bullish_equity_theses("SPY"),
    )

    assert outcome.candidates == ()
    assert outcome.reason_codes == (expected_reason,)


def test_ordinary_pipeline_prefers_standard_friday_expiration() -> None:
    selected_expirations: list[date] = []

    class Gateway:
        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 25),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 28),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            selected_expirations.append(expiration)
            return tuple(
                OptionContractRef(
                    contract_id=40_000 + index,
                    contract_id_ex=f"{40_000 + index}@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{rights[0]}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=rights[0],  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(strikes, start=1)
            )

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    outcome = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-friday-expiration",
        slot_at=NOW,
        symbols=("SPY",),
        equity_theses=_bullish_equity_theses("SPY"),
    )

    assert selected_expirations == [date(2026, 8, 21)]
    assert len(outcome.candidates) == 2
    assert {item["structure"] for item in outcome.candidates} == {
        "LONG_OPTION",
        "DEBIT_VERTICAL",
    }
    assert {item["dte"] for item in outcome.candidates} == {16}


def test_qualification_strike_window_adds_enough_nearby_expiration_fallbacks() -> None:
    available = tuple(
        Decimal(value)
        for value in ("90", "95", "100", "105", "110")
    )

    assert production_runtime_module._qualification_strike_window(
        (Decimal("100"), Decimal("105")),
        available,
        spot=Decimal("100"),
        right="C",
    ) == (
        Decimal("100"),
        Decimal("105"),
        Decimal("110"),
    )
    assert production_runtime_module._qualification_strike_window(
        (Decimal("100"), Decimal("95")),
        available,
        spot=Decimal("100"),
        right="P",
    ) == (
        Decimal("100"),
        Decimal("95"),
        Decimal("90"),
    )

    sparse_expiration_grid = tuple(
        Decimal(value)
        for value in ("90", "95", "110", "115")
    )
    assert production_runtime_module._qualification_strike_window(
        (Decimal("100"), Decimal("105")),
        sparse_expiration_grid,
        spot=Decimal("100"),
        right="C",
    ) == (
        Decimal("100"),
        Decimal("105"),
        Decimal("110"),
        Decimal("115"),
    )
    assert production_runtime_module._qualification_strike_window(
        (Decimal("100"), Decimal("95")),
        sparse_expiration_grid,
        spot=Decimal("100"),
        right="P",
    ) == (
        Decimal("100"),
        Decimal("95"),
        Decimal("90"),
    )


def test_paced_qualification_requests_only_planned_contracts() -> None:
    planned = (Decimal("100"), Decimal("105"))
    available = tuple(
        Decimal(value) for value in ("95", "100", "105", "110", "115")
    )

    assert production_runtime_module._qualification_strike_request(
        planned,
        available,
        spot=Decimal("100"),
        right="C",
        paced=True,
    ) == planned
    assert production_runtime_module._qualification_strike_request(
        planned,
        available,
        spot=Decimal("100"),
        right="C",
        paced=True,
        fallback_allowed=True,
    ) == (
        Decimal("100"),
        Decimal("105"),
        Decimal("110"),
        Decimal("115"),
    )
    assert production_runtime_module._qualification_strike_request(
        planned,
        available,
        spot=Decimal("100"),
        right="C",
        paced=False,
    ) == (
        Decimal("100"),
        Decimal("105"),
        Decimal("110"),
        Decimal("115"),
    )


def test_manual_core_only_scope_skips_scanner_subscriptions() -> None:
    optionability_calls: list[str] = []
    pool_build_calls = 0

    def build_equity_pool(**_kwargs) -> object:
        nonlocal pool_build_calls
        pool_build_calls += 1
        symbols = ("SPY", "QQQ") if pool_build_calls <= 2 else ("QQQ", "SPY")
        return _formal_equity_pool_result(*symbols)

    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs) -> tuple[object, ...]:
            raise AssertionError("manual core-only scan must not use scanner pacing")

        def option_expirations(self, _symbol: str, **_kwargs) -> tuple[object, ...]:
            optionability_calls.append(_symbol)
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, object]:
            raise RuntimeError("manual core-only has no scanner pacing witness")

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY", "QQQ"),
        equity_pool_builder=build_equity_pool,
        clock=lambda: NOW,
    )

    with inputs.manual_core_only() as first_scope:
        payload = inputs.run(scan_run_id="scan-manual-core", slot_at=NOW)
    first_order = tuple(optionability_calls)
    optionability_calls.clear()
    with inputs.manual_core_only() as second_scope:
        inputs.run(scan_run_id="scan-manual-core-next", slot_at=NOW)
    second_order = tuple(optionability_calls)
    optionability_calls.clear()
    with inputs.manual_core_only() as third_scope:
        inputs.run(scan_run_id="scan-manual-core-rotated", slot_at=NOW)

    assert payload["universe"]["scanner"] == ()
    assert payload["funnel_trace"]["pacing_usage"] == {}
    assert first_order == ("SPY", "QQQ")
    assert second_order == ("SPY", "QQQ")
    assert tuple(optionability_calls) == ("QQQ", "SPY")
    assert first_scope == {
        "target_symbol": "SPY",
        "attempt_number": 1,
        "attempt_kind": "WARMUP",
        "core_index": 0,
        "core_count": 2,
        "cycle": 1,
        "next_symbol": "SPY",
    }
    assert second_scope == {
        "target_symbol": "SPY",
        "attempt_number": 2,
        "attempt_kind": "REEVALUATION",
        "core_index": 0,
        "core_count": 2,
        "cycle": 1,
        "next_symbol": "QQQ",
    }
    assert third_scope == {
        "target_symbol": "QQQ",
        "attempt_number": 1,
        "attempt_kind": "WARMUP",
        "core_index": 1,
        "core_count": 2,
        "cycle": 1,
        "next_symbol": "QQQ",
    }


def test_manual_core_only_supplies_one_rotating_core_row_to_formal_pool() -> None:
    pool_rows: list[tuple[Mapping[str, object], ...]] = []
    optionability_calls: list[str] = []

    def build_equity_pool(**kwargs: object) -> object:
        rows = tuple(kwargs["scanner_rows"])  # type: ignore[arg-type]
        pool_rows.append(rows)
        return _formal_equity_pool_result(str(rows[0]["symbol"]))

    class Gateway:
        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def scan_underlyings(self, **_kwargs: object) -> tuple[object, ...]:
            raise AssertionError("manual core-only scan must not use scanner pacing")

        def option_expirations(self, symbol: str, **_kwargs: object) -> tuple[object, ...]:
            optionability_calls.append(symbol)
            return ()

    class Pacing:
        ready = True
        capability_hash = "a" * 64

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

        def usage(self) -> dict[str, object]:
            return {}

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY", "QQQ"),
        equity_pool_builder=build_equity_pool,
        clock=lambda: NOW,
    )

    for index in range(3):
        with inputs.manual_core_only():
            inputs.run(scan_run_id=f"scan-manual-formal-{index}", slot_at=NOW)

    assert tuple(tuple(row["symbol"] for row in rows) for rows in pool_rows) == (
        ("SPY",),
        ("SPY",),
        ("QQQ",),
    )
    assert all(rows[0]["source_scan"] == "CORE_UNIVERSE" for rows in pool_rows)
    assert tuple(optionability_calls) == ("SPY", "SPY", "QQQ")


def test_coarse_scan_records_each_terminal_symbol_and_exact_rejection() -> None:
    class Gateway:
        def option_expirations(self, symbol: str, **_kwargs: object) -> tuple[object, ...]:
            if symbol == "LGCL":
                return ()
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(self, *_args: object, **_kwargs: object) -> tuple[object, ...]:
            raise AssertionError("uncertain theses must stop before option qualification")

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )
    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-terminal-trace",
        slot_at=NOW,
        symbols=("LGCL", "NVDA", "GOOGL"),
        equity_theses={
            "rows": (
                {
                    "symbol": "NVDA",
                    "direction_label": "BEARISH",
                    "uncertainty": "0.5826",
                },
                {
                    "symbol": "GOOGL",
                    "direction_label": "NEUTRAL",
                    "uncertainty": "0.6202",
                },
            ),
        },
    )

    assert outcome.candidates == ()
    assert outcome.completed_symbols == ("NVDA", "GOOGL")
    assert outcome.reason_codes == (
        "OPTIONABILITY_NO_ELIGIBLE_EXPIRATION",
        "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT",
    )
    assert outcome.optionability_exclusion_reasons == (
        ("LGCL", "OPTIONABILITY_NO_ELIGIBLE_EXPIRATION"),
    )
    assert outcome.quote_excluded_symbols == ("NVDA", "GOOGL")
    assert outcome.quote_exclusion_reasons == (
        ("NVDA", "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT"),
        ("GOOGL", "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT"),
    )


def test_manual_core_only_can_retry_an_unconsumed_attempt() -> None:
    class Gateway:
        pass

    class Pacing:
        ready = True
        capability_hash = "a" * 64

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY", "QQQ"),
        clock=lambda: NOW,
    )

    with inputs.manual_core_only() as first_scope:
        pass

    assert inputs.retry_manual_core_attempt(first_scope) is True

    with inputs.manual_core_only() as retried_scope:
        pass

    assert retried_scope == first_scope


def test_manual_core_only_resets_sequence_on_new_york_trading_date() -> None:
    class Gateway:
        pass

    class Pacing:
        ready = True
        capability_hash = "a" * 64

    clock = [datetime(2026, 8, 11, 20, 0, tzinfo=timezone.utc)]
    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("SPY", "QQQ"),
        clock=lambda: clock[0],
    )

    with inputs.manual_core_only() as warmup:
        pass
    with inputs.manual_core_only() as reevaluation:
        pass

    clock[0] = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)
    assert inputs.retry_manual_core_attempt(reevaluation) is False
    with inputs.manual_core_only() as next_day:
        pass

    assert warmup["attempt_kind"] == "WARMUP"
    assert reevaluation["attempt_kind"] == "REEVALUATION"
    assert next_day == warmup


def test_ordinary_scan_isolates_unusable_underlying_quote_bases() -> None:
    class Gateway:
        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            rows = {
                "GOOD": _live_underlying_quote("GOOD"),
                "STALE": _live_underlying_quote(
                    "STALE",
                    observed_at=NOW - timedelta(seconds=6),
                ),
                "DELAY": _live_underlying_quote(
                    "DELAY",
                    market_data_type=3,
                ),
                "DUP": _live_underlying_quote(
                    "DUP",
                    contract_id=_live_underlying_quote("GOOD").contract_id,
                ),
                "SPY": _live_underlying_quote("SPY"),
            }
            return tuple(rows[symbol] for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            assert symbol == "GOOD"
            return tuple(
                OptionContractRef(
                    contract_id=30_000 + index,
                    contract_id_ex=f"{30_000 + index}@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{rights[0]}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=rights[0],  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(strikes, start=1)
            )

    class Pacing:
        ready = True

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    inputs = ProductionPipelineInputs(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=(),
        clock=lambda: NOW,
    )

    outcome = inputs._coarse_candidate_outcome(  # type: ignore[attr-defined]
        scan_run_id="scan-underlying-basis-isolation",
        slot_at=NOW,
        symbols=("GOOD", "STALE", "DELAY", "DUP"),
        equity_theses=_bullish_equity_theses("GOOD", "STALE", "DELAY", "DUP"),
    )

    assert tuple(row["symbol"] for row in outcome.candidates) == ("GOOD", "GOOD")
    assert {row["structure"] for row in outcome.candidates} == {
        "LONG_OPTION",
        "DEBIT_VERTICAL",
    }
    candidate = outcome.candidates[0]
    basis = candidate["underlying_quote_basis"]
    basis_hash = candidate["underlying_quote_basis_hash"]
    assert isinstance(basis, Mapping)
    assert basis["symbol"] == "GOOD"
    assert basis["source"] == "IBKR_REQ_TICKERS_READONLY"
    assert basis["market_data_type"] == 1
    assert canonical_hash(basis) == basis_hash
    assert outcome.reason_codes == ()
    assert outcome.quote_excluded_symbols == ("STALE", "DELAY", "DUP")
    assert outcome.quote_exclusion_reasons == (
        ("STALE", "UNDERLYING_QUOTE_STALE_OR_FUTURE"),
        ("DELAY", "UNDERLYING_QUOTE_MARKET_DATA_DELAYED"),
        ("DUP", "UNDERLYING_QUOTE_IDENTITY_MISMATCH"),
    )


def test_balanced_ordinary_discovery_tries_liquid_core_before_dominant_scanner() -> None:
    scanner = tuple(
        {"symbol": f"S{index:02d}", "score": Decimal(100 - index)}
        for index in range(30)
    )
    core = (
        {"symbol": "SPY", "score": Decimal("1")},
        {"symbol": "QQQ", "score": Decimal("0")},
    )

    selected = production_runtime_module._balanced_discovery_symbols(
        scanner,
        core,
        limit=8,
    )

    assert selected[:4] == ("SPY", "S00", "QQQ", "S01")
    assert len(selected) == 8


def test_sector_diverse_scanner_rows_round_robin_industries() -> None:
    rows = (
        {"symbol": "A", "score": Decimal("100"), "industry": "Technology"},
        {"symbol": "B", "score": Decimal("99"), "industry": "Technology"},
        {"symbol": "C", "score": Decimal("98"), "industry": "Financial"},
        {"symbol": "D", "score": Decimal("97"), "industry": "Energy"},
    )

    selected = production_runtime_module._sector_diverse_scanner_rows(rows)

    assert [item["symbol"] for item in selected] == ["A", "C", "D", "B"]
    interleaved = production_runtime_module._balanced_discovery_symbols(
        selected,
        ({"symbol": "SPY", "score": Decimal("1")},),
        limit=5,
        preserve_scanner_order=True,
    )
    assert interleaved == ("SPY", "A", "C", "D", "B")


def test_verified_deterministic_news_can_seed_bounded_equity_discovery() -> None:
    rows = production_runtime_module._verified_news_discovery_rows(
        {
            "news": (
                {
                    "symbols": ("TSLA",),
                    "event_impact_score": "90",
                    "research_rank": 2,
                    "symbol_binding": {
                        "status": "VERIFIED_PROVIDER_RELATED",
                    },
                },
                {
                    "symbols": ("AAPL", "TSLA"),
                    "event_impact_score": "95",
                    "research_rank": 1,
                    "symbol_binding": {
                        "status": "PROVIDER_VERIFIED",
                    },
                },
                {
                    "symbols": ("MSFT",),
                    "event_impact_score": "100",
                    "research_rank": 1,
                    "symbol_binding": {
                        "status": "SOURCE_DECLARED",
                    },
                },
            ),
        },
        limit=2,
    )

    assert tuple(row["symbol"] for row in rows) == ("AAPL", "TSLA")
    assert tuple(row["source_scan"] for row in rows) == (
        "NEWS_EVENT_POOL",
        "NEWS_EVENT_POOL",
    )
    assert tuple(row["rank"] for row in rows) == (0, 1)


def test_deterministic_macro_research_proxy_can_seed_bounded_etf_discovery() -> None:
    binding = ResearchProxyBinding(
        event_category="US_MONETARY_POLICY",
        source="JIN10",
        proxy_symbol="TLT",
    )

    rows = production_runtime_module._verified_news_discovery_rows(
        {
            "news": (
                {
                    "symbols": (),
                    "event_impact_score": "92",
                    "research_rank": 1,
                    "symbol_binding": {"status": "UNBOUND"},
                    "research_proxy_binding": binding.as_dict(),
                },
            ),
        },
        limit=30,
    )

    assert tuple(row["symbol"] for row in rows) == ("TLT",)
    assert rows[0]["source_scan"] == "NEWS_EVENT_POOL"
    assert rows[0]["event_impact_score"] == Decimal("92")

    assert production_runtime_module._verified_news_discovery_rows(
        {
            "news": (
                {
                    "symbols": ("AAPL",),
                    "event_impact_score": "100",
                    "symbol_binding": {"status": "UNVERIFIED_LEGACY_PROVIDER"},
                    "research_proxy_binding": binding.as_dict(),
                },
            ),
        }
    ) == ()


def test_public_nested_news_score_reaches_only_verified_equity_discovery() -> None:
    projected = tuple(
        _deterministic_decision_news_row(row)
        for row in (
            {
                "symbols": ("NVDA",),
                "scores": {"event_impact_score": 87.75},
                "research_rank": 1,
                "symbol_binding": {
                    "status": "VERIFIED_PROVIDER_RELATED",
                },
            },
            {
                "symbols": ("UNVERIFIED",),
                "scores": {"event_impact_score": 99.0},
                "research_rank": 1,
                "symbol_binding": {
                    "status": "UNVERIFIED_LEGACY_PROVIDER",
                },
            },
        )
    )

    rows = production_runtime_module._verified_news_discovery_rows(
        {"news": projected},
        limit=30,
    )

    assert tuple(row["symbol"] for row in rows) == ("NVDA",)
    assert rows[0]["event_impact_score"] == Decimal("87.75")
    assert projected[0]["event_impact_score"] == "87.75"
    assert projected[1]["event_impact_score"] == "99.0"


def test_internal_top_level_news_score_crosses_strict_allocation_boundary() -> None:
    projected = _deterministic_decision_news_row(
        {
            "symbols": ("NVDA",),
            "event_impact_score": 87.75,
            "symbol_binding": {
                "status": "VERIFIED_PROVIDER_RELATED",
            },
        }
    )

    assert projected["event_impact_score"] == "87.75"
    rows = ProductionPipelineInputs._event_rows(
        None,  # type: ignore[arg-type]
        {"news": (projected,)},
    )
    assert rows[0]["symbol"] == "NVDA"
    assert rows[0]["deterministic_score"] == Decimal("87.75")


@pytest.mark.parametrize("invalid_score", ("bad", "NaN", "Infinity", "-1", "101"))
def test_preserved_scanner_order_rejects_invalid_score(
    invalid_score: str,
) -> None:
    with pytest.raises(ValueError, match="RESEARCH_ALLOCATION_INPUT_INVALID"):
        production_runtime_module._balanced_discovery_symbols(
            ({"symbol": "BAD", "score": invalid_score},),
            (),
            limit=30,
            preserve_scanner_order=True,
        )

    assert production_runtime_module._balanced_discovery_symbols(
        (
            {"symbol": "ZERO", "score": "0"},
            {"symbol": "123", "score": "100"},
        ),
        (),
        limit=30,
        preserve_scanner_order=True,
    ) == ("ZERO",)


def test_direct_top10_bad_underlying_batch_returns_structured_omission_evidence() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.quote_calls: list[tuple[str, ...]] = []

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                        strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            self.quote_calls.append(symbols)
            if "B3" in symbols:
                raise RuntimeError("sanitized batch failure")
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            right = rights[0]
            return tuple(
                OptionContractRef(
                    contract_id=9000 + index,
                    contract_id_ex=f"{9000 + index}@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{right}-{strike}",
                    expiration=expiration,
                    strike=strike,
                    right=right,  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                )
                for index, strike in enumerate(strikes, start=1)
            )

    class Pacing:
        ready = True
        reason = None

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    gateway = Gateway()
    source = DirectTop10StructureSource(
        gateway,  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        clock=lambda: NOW,
        core_symbols=("B1", "B2", "B3", "B4", "B5"),
        include_scanner=False,
    )

    resolution = source.resolve_top10(scheduled_for=NOW)

    assert gateway.quote_calls == [
        ("B1", "B2", "B3", "B4"),
        ("B1", "B2"),
        ("B3", "B4"),
        ("B3",),
        ("B4",),
        ("B5",),
    ]
    assert len(resolution.structures) == 4
    assert {
        item.candidate.underlying for item in resolution.structures
    } == {"B1", "B2", "B4", "B5"}
    assert resolution.reason_codes == ()
    assert resolution.missing_symbols == ()
    for item in resolution.structures:
        candidate = item.candidate
        assert candidate.underlying_quote_basis is not None
        assert candidate.underlying_quote_basis_hash is not None
        assert candidate.evidence_ids == (
            (
                f"IBKR_DIRECT_DISCOVERY:{candidate.underlying}:"
                "2026-08-21"
            ),
            (
                "IBKR_UNDERLYING_QUOTE_BASIS:"
                f"{candidate.underlying}:"
                f"{candidate.underlying_quote_basis.contract_id}"
            ),
                "IBKR_DIRECT_SYMBOL_EXCLUDED:B3",
        )
        assert candidate.evidence_hashes == (
            canonical_hash(
                {
                    "schema": "options_copilot.direct_top10_discovery.v1",
                    "scheduled_for": NOW,
                    "underlying": candidate.underlying,
                    "source_scan": "CORE_UNIVERSE",
                    "underlying_quote_basis": (
                        candidate.underlying_quote_basis.as_dict()
                    ),
                    "underlying_quote_basis_hash": (
                        candidate.underlying_quote_basis_hash
                    ),
                    "strategy_hash": candidate.strategy_hash,
                    "underlying_quote_excluded_symbols": ("B3",),
                }
            ),
            candidate.underlying_quote_basis_hash,
            canonical_hash(
                {
                    "schema": (
                        "options_copilot.direct_top10_symbol_exclusion.v1"
                    ),
                    "scheduled_for": NOW,
                    "excluded_symbol": "B3",
                    "reason_code": "UNDERLYING_QUOTE_FAILED",
                }
            ),
        )


def test_direct_top10_partial_qualification_discards_all_structures() -> None:
    class Gateway:
        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                        strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_live_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(
            self,
            symbol: str,
            expiration: date,
            strikes: tuple[Decimal, Decimal],
            *,
            exchange: str,
            trading_class: str,
            rights: tuple[str, ...],
        ) -> tuple[OptionContractRef, ...]:
            return (
                OptionContractRef(
                    contract_id=9001,
                    contract_id_ex="9001@SMART",
                    symbol=symbol,
                    local_symbol=f"{symbol}-{rights[0]}-{strikes[0]}",
                    expiration=expiration,
                    strike=strikes[0],
                    right=rights[0],  # type: ignore[arg-type]
                    exchange=exchange,
                    trading_class=trading_class,
                    multiplier=100,
                    currency="USD",
                ),
            )

    class Pacing:
        ready = True
        reason = None

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    source = DirectTop10StructureSource(
        Gateway(),  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        clock=lambda: NOW,
        core_symbols=("B1", "B2"),
        include_scanner=False,
    )

    resolution = source.resolve_top10(scheduled_for=NOW)

    assert resolution.structures == ()
    assert resolution.reason_codes == ("OPTION_QUALIFICATION_INCOMPLETE",)
    assert resolution.missing_symbols == ("B1", "B2")


def test_direct_top10_mid_loop_pacing_denial_returns_missing_symbols() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.calls: list[tuple[str, ...]] = []

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            self.calls.append(symbols)
            return tuple(
                SimpleNamespace(symbol=symbol, market_price=Decimal("100"))
                for symbol in symbols
            )

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100")),
                ),
            )

    class Pacing:
        ready = True
        reason = None

        def __init__(self) -> None:
            self.calls = 0

        def decision(self, request_class: str) -> object:
            assert request_class == "secdef"
            return SimpleNamespace(allowed=True, reason=None)

        def lease(self, request_class: str):
            if request_class == "secdef":
                return nullcontext(SimpleNamespace(allowed=True, reason=None))
            assert request_class == "snapshot_quote"
            self.calls += 1
            allowed = self.calls == 1
            reason = None if allowed else "PACING_REQUEST_WINDOW_EXHAUSTED"
            return nullcontext(SimpleNamespace(allowed=allowed, reason=reason))

    gateway = Gateway()
    source = DirectTop10StructureSource(
        gateway,  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        clock=lambda: NOW,
        core_symbols=("B1", "B2", "B3", "B4", "B5"),
        include_scanner=False,
    )

    resolution = source.resolve_top10(scheduled_for=NOW)

    assert gateway.calls == [("B1", "B2", "B3", "B4")]
    assert resolution.structures == ()
    assert resolution.reason_codes == (
        "UNDERLYING_QUOTE_PACING_DENIED",
        "UNDERLYING_QUOTE_PACING_REQUEST_WINDOW_EXHAUSTED",
    )
    assert resolution.missing_symbols == ("B5",)


def test_direct_top10_invalid_underlying_response_discards_partial_output() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.qualification_calls = 0

        def option_expirations(self, symbol: str, **_kwargs) -> tuple[object, ...]:
            return (
                SimpleNamespace(
                    exchange="SMART",
                    expiration=date(2026, 8, 21),
                    trading_class=symbol,
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return (
                *tuple(
                    SimpleNamespace(symbol=symbol, market_price=Decimal("100"))
                    for symbol in symbols
                ),
                SimpleNamespace(symbol="EXTRA", market_price=Decimal("100")),
            )

        def qualify_option_contracts(self, *_args, **_kwargs) -> tuple[object, ...]:
            self.qualification_calls += 1
            raise AssertionError("invalid quote output must stop before qualification")

    class Pacing:
        ready = True
        reason = None

        def decision(self, _request_class: str) -> object:
            return SimpleNamespace(allowed=True, reason=None)

    gateway = Gateway()
    source = DirectTop10StructureSource(
        gateway,  # type: ignore[arg-type]
        Pacing(),  # type: ignore[arg-type]
        clock=lambda: NOW,
        core_symbols=("B1", "B2"),
        include_scanner=False,
    )

    resolution = source.resolve_top10(scheduled_for=NOW)

    assert resolution.structures == ()
    assert resolution.reason_codes == ("UNDERLYING_QUOTE_INVALID_RESPONSE",)
    assert resolution.missing_symbols == ("B1", "B2")
    assert gateway.qualification_calls == 0


def _event_gate_snapshot(
    *,
    overlap: bool = False,
    health_asof: datetime = NOW,
    include_legacy_overlap: bool = False,
) -> dict[str, object]:
    source_health = [
        {
            "source": source,
            "source_kind": "CALENDAR",
            "status": "READY",
            "success_count": 1,
            "failure_date_count": 0,
            "asof": health_asof.isoformat(),
        }
        for source in ("NASDAQ", "FINNHUB")
    ]
    window_start = NOW - timedelta(minutes=1)
    window_end = NOW + timedelta(days=10)
    source_batches: list[dict[str, object]] = []
    calendar: list[dict[str, object]] = []
    for index, source in enumerate(("NASDAQ", "FINNHUB"), start=1):
        row_symbol = "SPY" if overlap and source == "NASDAQ" else f"T{index}"
        member = {
            "identity": f"calendar:event-{index}",
            "source": f"{source} EARNINGS CALENDAR",
            "batch_source": source,
            "source_id": f"event-{index}",
            "source_content_hash": f"{index}" * 64,
            "record_hash": f"{index + 2}" * 64,
            "row_hash": f"{index + 4}" * 64,
            "observed_at": health_asof.isoformat(),
        }
        batch_payload = {
            "schema": "options_copilot.calendar_source_batch.v1",
            "source": source,
            "status": "READY",
            "reason": None,
            "success_count": 1,
            "failure_date_count": 0,
            "observed_at": health_asof.isoformat(),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "members": [member],
        }
        source_batches.append(
            {**batch_payload, "batch_hash": canonical_hash(batch_payload)}
        )
        calendar.append(
            {
                "category": "EARNINGS",
                "symbols": [row_symbol],
                "event_date": (NOW.date() + timedelta(days=2)).isoformat(),
                "source": member["source"],
                "source_id": member["source_id"],
                "content_hash": member["source_content_hash"],
                "record_hash": member["record_hash"],
                "evidence_identity": member["identity"],
                "evidence_row_hash": member["row_hash"],
                "observed_at": member["observed_at"],
                "status": "PROVISIONAL",
                "current_generation": True,
                "calendar_generation_source": source,
                "calendar_generation_member_hash": canonical_hash(member),
            }
        )
    envelope_payload = {
        "schema": "options_copilot.calendar_generation_envelope.v1",
        "observed_at": health_asof.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "source_batches": source_batches,
        "current_members": [
            member
            for batch in source_batches
            for member in batch["members"]
        ],
        "current_member_count": 2,
    }
    envelope = {
        **envelope_payload,
        "envelope_hash": canonical_hash(envelope_payload),
    }
    for row in calendar:
        row["calendar_envelope_hash"] = envelope["envelope_hash"]
    if include_legacy_overlap:
        calendar.append(
            {
                "category": "EARNINGS",
                "symbols": ["SPY"],
                "event_date": (NOW.date() + timedelta(days=2)).isoformat(),
                "source": "NASDAQ EARNINGS CALENDAR",
                "source_id": "removed-event",
                "content_hash": "a" * 64,
                "record_hash": "b" * 64,
                "evidence_identity": "calendar:removed-event",
                "evidence_row_hash": "c" * 64,
                "observed_at": (NOW - timedelta(days=3)).isoformat(),
                "status": "PROVISIONAL",
                "current_generation": False,
                "calendar_envelope_hash": None,
                "calendar_generation_source": None,
                "calendar_generation_member_hash": None,
            }
        )
    snapshot = {
        "news": [],
        "news_asof": health_asof.isoformat(),
        "calendar": calendar,
        "source_health": source_health,
        "calendar_asof": health_asof.isoformat(),
        "calendar_snapshot_hash": "a" * 64,
        "calendar_window_start": window_start.isoformat(),
        "calendar_window_end": window_end.isoformat(),
        "calendar_envelope": envelope,
    }
    snapshot["event_generation_hash"] = canonical_hash(
        {
            "news_asof": snapshot["news_asof"],
            "source_health": snapshot["source_health"],
            "calendar_asof": snapshot["calendar_asof"],
            "calendar_snapshot_hash": snapshot["calendar_snapshot_hash"],
            "calendar_window_start": snapshot["calendar_window_start"],
            "calendar_window_end": snapshot["calendar_window_end"],
            "calendar_envelope": snapshot["calendar_envelope"],
            "calendar": snapshot["calendar"],
        }
    )
    return snapshot


def test_event_gate_fields_require_current_dual_calendar_coverage() -> None:
    base = _event_gate_snapshot()

    clear = production_runtime_module._event_gate_fields(
        base,
        symbol="SPY",
        slot_at=NOW,
        holding_end=NOW.date() + timedelta(days=5),
    )
    overlap = production_runtime_module._event_gate_fields(
        _event_gate_snapshot(overlap=True),
        symbol="SPY",
        slot_at=NOW,
        holding_end=NOW.date() + timedelta(days=5),
    )
    stale = production_runtime_module._event_gate_fields(
        _event_gate_snapshot(health_asof=NOW - timedelta(hours=1)),
        symbol="SPY",
        slot_at=NOW,
        holding_end=NOW.date() + timedelta(days=5),
    )

    assert clear["event_evidence_status"] == "AVAILABLE"
    assert clear["earnings_overlap"] is False
    assert overlap["event_evidence_status"] == "AVAILABLE"
    assert overlap["earnings_overlap"] is True
    assert stale == {
        "event_evidence_status": "UNAVAILABLE",
        "earnings_overlap": None,
        "event_defined": False,
        "event_evidence_hash": None,
        "event_supporting_overlap": False,
        "event_supporting_hash": None,
    }


def test_stale_positive_event_is_supporting_only_and_never_hard_available() -> None:
    result = production_runtime_module._event_gate_fields(
        _event_gate_snapshot(
            health_asof=NOW - timedelta(hours=1),
            overlap=True,
        ),
        symbol="SPY",
        slot_at=NOW,
        holding_end=NOW.date() + timedelta(days=5),
    )

    assert result["event_evidence_status"] == "UNAVAILABLE"
    assert result["earnings_overlap"] is None
    assert result["event_evidence_hash"] is None
    assert result["event_supporting_overlap"] is True
    assert result["event_supporting_hash"]


def test_removed_legacy_event_cannot_borrow_fresh_generation_health() -> None:
    result = production_runtime_module._event_gate_fields(
        _event_gate_snapshot(include_legacy_overlap=True),
        symbol="SPY",
        slot_at=NOW,
        holding_end=NOW.date() + timedelta(days=5),
    )

    assert result["event_evidence_status"] == "AVAILABLE"
    assert result["earnings_overlap"] is False
    assert result["event_supporting_overlap"] is True
    assert result["event_supporting_hash"]


def test_broker_evidence_publishes_only_partial_supporting_positioning_sample() -> None:
    expiration = date(2026, 8, 21)
    contracts = tuple(
        OptionContractRef(
            contract_id=con_id,
            contract_id_ex=f"{con_id}@SMART",
            symbol="GLD",
            local_symbol=f"GLD-{strike}-{right}",
            expiration=expiration,
            strike=Decimal(strike),
            right=right,
            exchange="SMART",
            trading_class="GLD",
            multiplier=100,
        )
        for con_id, strike, right in (
            (101, "375", "P"),
            (102, "385", "C"),
        )
    )
    quotes = tuple(
        BatchedOptionQuote(
            contract_id=contract.contract_id,
            batch_id="batch-positioning",
            request_id=f"request-{contract.contract_id}",
            requested_at=OPEN_NOW - timedelta(milliseconds=100),
            observed_at=OPEN_NOW,
            completed_at=OPEN_NOW,
            source="IBKR_REQ_TICKERS_READONLY",
            bid=Decimal("1.00"),
            ask=Decimal("1.10"),
            exchange_time=OPEN_NOW,
            open_interest=1000,
            gamma=Decimal("0.02"),
            delta=Decimal("0.40"),
            theta=Decimal("-0.05"),
            vega=Decimal("0.10"),
            market_data_type=1,
        )
        for contract in contracts
    )
    acquisition = ProductionBrokerEvidenceAcquisition(
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),
        execution_cost_contract={},
        policy_resolver=object(),
    )
    snapshot = SimpleNamespace(
        quotes=quotes,
        built_at=OPEN_NOW,
        snapshot_hash="d" * 64,
    )

    acquisition._publish_positioning_sample(
        scan_run_id="scan-positioning",
        universe={"finalists": ({"symbol": "GLD", "spot": "380"},)},
        contracts=contracts,
        snapshot=snapshot,  # type: ignore[arg-type]
    )

    payload = acquisition.positioning()
    assert payload["status"] == "DEGRADED"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_allowed"] is False
    assert payload["instruction_allowed"] is False
    row = payload["positioning"][0]
    assert row["underlying"] == "GLD"
    assert row["chain_scope"] == "FROZEN_FINALIST_LEGS_ONLY"
    assert row["option_chain_coverage_rate"] is None
    assert "OPTION_CHAIN_COVERAGE_UNKNOWN" in row["reasons"]


def test_broker_evidence_reconciles_nav_after_same_atomic_snapshot_and_guards_append(
    tmp_path: Path,
) -> None:
    expiration = date(2026, 8, 21)
    contracts = tuple(
        OptionContractRef(
            contract_id=con_id,
            contract_id_ex=f"{con_id}@SMART",
            symbol="SPY",
            local_symbol=f"SPY-{strike}-C",
            expiration=expiration,
            strike=Decimal(strike),
            right="C",
            exchange="SMART",
            trading_class="SPY",
            multiplier=100,
        )
        for con_id, strike in ((101, "100"), (102, "105"))
    )
    secdefs = tuple(
        OptionSecDefSnapshot(
            item.contract_id,
            item.local_symbol,
            item.trading_class,
            item.multiplier,
            item.exchange,
            item.expiration,
            item.strike,
            item.right,
            "OPT",
            "USD",
            True,
            False,
            "IBKR",
        )
        for item in contracts
    )

    class Source:
        def account_snapshot(self) -> dict[str, object]:
            return {"currency": "USD", "net_liquidation": Decimal("2500")}

        def positions(self) -> tuple[object, ...]:
            return ()

        def working_orders(self) -> tuple[object, ...]:
            return ()

        def unsubmitted_instructions(self) -> tuple[object, ...]:
            return ()

        def option_contract_definitions(self, _contracts):
            return secdefs

        def option_quote_batch(self, _contracts):
            quotes = tuple(
                BatchedOptionQuote(
                    contract_id=item.contract_id,
                    batch_id="atomic-nav-batch",
                    request_id=f"request-{item.contract_id}",
                    requested_at=NOW - timedelta(seconds=2),
                    observed_at=NOW - timedelta(seconds=1),
                    completed_at=NOW,
                    source="IBKR",
                    bid=Decimal("1.00"),
                    ask=Decimal("1.10"),
                    exchange_time=NOW - timedelta(seconds=1),
                    implied_volatility=Decimal("0.20"),
                    volume=100,
                    open_interest=1000,
                    delta=Decimal("0.50"),
                    gamma=Decimal("0.02"),
                    theta=Decimal("-0.08"),
                    vega=Decimal("0.11"),
                    market_data_type=1,
                )
                for item in contracts
            )
            return OptionQuoteBatch(
                "atomic-nav-batch",
                QuoteBatchStatus.COMPLETE,
                NOW - timedelta(seconds=2),
                NOW,
                "IBKR",
                quotes,
            )

        def underlying_iv_history(
            self,
            symbol: str,
            *,
            end_at: datetime,
        ) -> UnderlyingIvHistory:
            calls.append("iv_history")
            return _authoritative_iv_history(symbol, end_at=end_at)

    source = Source()
    snapshot = BrokerSnapshotBuilder(source, clock=lambda: NOW).build(contracts)
    assert isinstance(snapshot, AtomicBrokerSnapshot)
    assert snapshot.complete
    calls: list[str] = []

    class Builder:
        def __init__(self, snapshot_source: object) -> None:
            self.source = snapshot_source

        def build(self, requested):
            assert tuple(requested) == contracts
            calls.append("broker_snapshot")
            return snapshot

    class OptionsEvidence:
        def resolve_contracts(self, **_kwargs):
            return contracts

    class Pacing:
        ready = True

        def decision(self, _request_class: str):
            return SimpleNamespace(allowed=True)

    class PolicyResolver:
        def policy_contract_document(self, _resolved_policy):
            return {"schema": "test-policy"}

    class NavSource:
        def snapshot(self, *, asof, observed_account_nlv):
            calls.append("strategy_nav")
            assert asof == snapshot.built_at
            assert observed_account_nlv == Decimal("2500")
            fields = {
                "asof": asof,
                "strategy_nav": Decimal("2000"),
                "strategy_deposits": Decimal("2000"),
                "strategy_withdrawals": Decimal("0"),
                "realized_pnl": Decimal("0"),
                "open_position_unrealized_pnl": Decimal("0"),
                "fees": Decimal("0"),
                "signed_corrections": Decimal("0"),
                "non_strategy_contribution": Decimal("0"),
                "fill_principal_contribution": Decimal("0"),
                "observed_account_nlv": observed_account_nlv,
                "reconciliation_difference": Decimal("500"),
                "contract_version": "v1",
                "contract_hash": "a" * 64,
                "ledger_head_hash": "b" * 64,
                "valid": True,
                "no_trade_reasons": (),
            }
            return StrategyNavSnapshot(
                **fields,
                content_hash=canonical_hash(fields),
            )

        def guard_current(self, nav, *, callback):
            assert isinstance(nav, StrategyNavSnapshot)
            calls.append("guard_current")
            return callback()

    with EvidenceStore(tmp_path / "broker-evidence.sqlite3") as store:
        acquisition = ProductionBrokerEvidenceAcquisition(
            Builder(source),  # type: ignore[arg-type]
            OptionsEvidence(),  # type: ignore[arg-type]
            store,
            Pacing(),  # type: ignore[arg-type]
            NavSource(),
            execution_cost_contract={},
            policy_resolver=PolicyResolver(),
            clock=lambda: NOW,
        )
        result = acquisition.acquire(
            scan_run_id="scan-atomic-nav",
            universe={},
            context={},
        )

        assert calls == [
            "iv_history",
            "broker_snapshot",
            "strategy_nav",
            "guard_current",
        ]
        assert result["nav_snapshot"].observed_account_nlv == Decimal("2500")
        assert result["nav_snapshot"].asof == snapshot.built_at
        assert result["broker_snapshot"] is snapshot
        assert result["iv_history"] == tuple(
            item.close for item in _authoritative_iv_history(
                "SPY",
                end_at=NOW,
            ).points
        )
        assert result["iv_history_content_hash"] == source.underlying_iv_history(
            "SPY",
            end_at=NOW,
        ).content_hash
        broker_row = store.query(kinds=("BROKER_SNAPSHOT",), limit=1)[0]
        contract_evidence = broker_row.record.payload["contract_quote_evidence"]
        assert len(contract_evidence) == 2
        first_contract = contract_evidence[0]
        assert first_contract["contract"]["conId"] == 101
        assert first_contract["quote"]["request_id"] == "request-101"
        assert first_contract["quote"]["exchange_time"] == (
            NOW - timedelta(seconds=1)
        ).isoformat(timespec="microseconds")
        assert first_contract["quote"]["delta"] == Decimal("0.5")
        assert first_contract["quote"]["market_data_type"] == 1
        assert first_contract["liquidity"]["status"] == "ELIGIBLE"
        assert first_contract["liquidity"]["spread_absolute"] == Decimal("0.1")
        assert first_contract["liquidity"]["maximum_absolute_spread"] == Decimal("0.5")
        assert first_contract["liquidity"]["maximum_relative_spread"] == Decimal("0.2")
        assert store.verify_integrity() is True


def _candidate(risk_fraction: str) -> dict[str, object]:
    return {
        "candidate_id": f"risk-{risk_fraction}",
        "max_loss_usd": Decimal(risk_fraction) * Decimal("10000"),
        "strategy_nav_usd": Decimal("10000"),
    }


def test_production_risk_gate_enforces_exact_percentage_boundaries() -> None:
    gate = ProductionRiskGate()

    normal = gate.evaluate(candidates=(_candidate("0.10"),))
    exact_a_ceiling = gate.evaluate(candidates=(_candidate("0.15"),))
    over_a_ceiling = gate.evaluate(candidates=(_candidate("0.1501"),))
    hard_line = gate.evaluate(candidates=(_candidate("0.20"),))

    assert normal["eligible"] is True
    assert normal["risk_fraction"] == Decimal("0.10")
    assert exact_a_ceiling["eligible"] is True
    assert exact_a_ceiling["risk_fraction"] == Decimal("0.15")
    assert over_a_ceiling["eligible"] is False
    assert over_a_ceiling["reasons"] == (
        "RISK_ABOVE_15_PERCENT_REJECT_LINE",
    )
    assert hard_line["eligible"] is False
    assert hard_line["reasons"] == (
        "RISK_ABOVE_15_PERCENT_REJECT_LINE",
        "HARD_20_PERCENT_REJECT_LINE",
    )


def test_missing_authoritative_iv_history_is_always_no_trade() -> None:
    evidence = VolatilityEngine().evaluate(
        {
            "source": "IBKR",
            "observed_at": NOW,
            "secdef_hash": "a" * 64,
            "quote_hash": "b" * 64,
            "quotes": (
                {
                    "bid": Decimal("1.00"),
                    "ask": Decimal("1.10"),
                    "implied_volatility": Decimal("0.20"),
                    "volume": 20,
                    "open_interest": 200,
                },
            ),
            "atm_iv": Decimal("0.20"),
        },
        now=NOW,
    )

    assert evidence.eligible is False
    assert "MISSING_IV_HISTORY" in evidence.reasons


def test_serialized_snapshot_provider_preserves_read_only_source_port() -> None:
    source = SimpleNamespace(underlying_iv_history=lambda *_args, **_kwargs: ())
    raw_builder = SimpleNamespace(
        source=source,
        build=lambda contracts: tuple(contracts),
    )

    provider = production_runtime_module.SerializedBrokerSnapshotProvider(
        raw_builder,  # type: ignore[arg-type]
        batch_lock=threading.RLock(),
    )

    assert provider.source is source


def test_authoritative_iv_history_rejects_hash_tamper_and_scale_mismatch() -> None:
    snapshot = SimpleNamespace(built_at=NOW)
    history = _authoritative_iv_history("SPY", end_at=NOW)

    assert production_runtime_module._validate_underlying_iv_history(
        replace(history, content_hash="f" * 64),
        symbol="SPY",
        snapshot=snapshot,  # type: ignore[arg-type]
        atm_iv=Decimal("0.20"),
    ) == ("IV_HISTORY_HASH_INVALID",)
    assert production_runtime_module._validate_underlying_iv_history(
        history,
        symbol="SPY",
        snapshot=snapshot,  # type: ignore[arg-type]
        atm_iv=Decimal("0.001"),
    ) == ("IV_HISTORY_SCALE_MISMATCH",)


def _quote(
    con_id: int,
    *,
    bid: str,
    ask: str,
) -> BatchedOptionQuote:
    return BatchedOptionQuote(
        contract_id=con_id,
        batch_id="batch-open-1",
        request_id=f"request-{con_id}",
        requested_at=OPEN_NOW - timedelta(milliseconds=100),
        observed_at=OPEN_NOW,
        completed_at=OPEN_NOW,
        source="IBKR_REQMKT_DATA_READONLY",
        bid=Decimal(bid),
        ask=Decimal(ask),
        last=(Decimal(bid) + Decimal(ask)) / Decimal("2"),
        exchange_time=OPEN_NOW,
        volume=500,
        open_interest=5000,
        implied_volatility=Decimal("0.25"),
        delta=Decimal("0.40"),
        gamma=Decimal("0.02"),
        theta=Decimal("-0.05"),
        vega=Decimal("0.10"),
        market_data_type=1,
    )


def test_news_open_reprice_without_full_lineage_fails_closed() -> None:
    expiry = date(2026, 8, 21)
    body: dict[str, object] = {
        "candidate_id": "candidate-open-1",
        "symbol": "SPY",
        "structure": "DEBIT_VERTICAL",
        "legs": (
            {
                "con_id": 101,
                "contract_id_ex": "101@SMART",
                "underlying": "SPY",
                "local_symbol": "SPY   260821C00100000",
                "trading_class": "SPY",
                "security_type": "OPT",
                "expiration": expiry.isoformat(),
                "strike": "100",
                "right": "CALL",
                "side": "LONG",
                "ratio": 1,
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
            },
            {
                "con_id": 102,
                "contract_id_ex": "102@SMART",
                "underlying": "SPY",
                "local_symbol": "SPY   260821C00105000",
                "trading_class": "SPY",
                "security_type": "OPT",
                "expiration": expiry.isoformat(),
                "strike": "105",
                "right": "CALL",
                "side": "SHORT",
                "ratio": 1,
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
            },
        ),
        "terminal_scenarios": (
            {"terminal_underlying_price": "100", "probability": "0.5"},
            {"terminal_underlying_price": "105", "probability": "0.5"},
        ),
        "execution_cost_contract_version": EXECUTION_COST_VERSION,
        "execution_cost_contract_hash": EXECUTION_COST_HASH,
        # Deliberately stale economics must never survive the fresh reprice.
        "all_in_cost_usd": "999.00",
        "max_loss_usd": "999.00",
        "exit_plan": {
            "thesis_invalidation": "trend reverses",
            "risk_stop": "review at bounded loss threshold",
            "profit_take": "review at bounded profit threshold",
        },
    }
    row = {
        "candidate_id": body["candidate_id"],
        "candidate_hash": canonical_hash(body),
        "candidate_body": body,
        "after_cost_expected_value": Decimal("-999"),
    }
    quotes = (_quote(101, bid="1.90", ask="2.00"), _quote(102, bid="0.90", ask="1.00"))
    batch = OptionQuoteBatch(
        batch_id="batch-open-1",
        status=QuoteBatchStatus.COMPLETE,
        requested_at=OPEN_NOW - timedelta(milliseconds=100),
        completed_at=OPEN_NOW,
        source="IBKR_REQMKT_DATA_READONLY",
        quotes=quotes,
    )

    built = _news_candidate(
        row,
        {item.contract_id: item for item in quotes},
        batch,
        now=OPEN_NOW,
    )

    assert built is not None
    binding, candidate = built
    assert binding.tradability.bid == Decimal("0.90")
    assert binding.tradability.ask == Decimal("1.10")
    assert candidate.maximum_loss_usd == Decimal("130.00")
    assert candidate.estimated_cost_usd == Decimal("130.00")
    assert candidate.cost_after_ev_usd == Decimal("120.000")
    assert len(candidate.evidence_ids) == len(candidate.evidence_hashes) == 1
    assert candidate.broker_snapshot_hash is None
    assert candidate.strategy_nav_usd is None
    assert candidate.strategy_nav_post_hash is None
    assert candidate.payoff_hash is None
    assert candidate.economics_calculation_hash is None
    evaluated = evaluate_preselection(candidate, now=OPEN_NOW)
    assert evaluated.blockers == ("OPEN_REPRICE_ECONOMICS_LINEAGE_MISSING",)
    assert evaluated.action_pool_eligible is False

    complete_body = {
        **body,
        "strategy_nav_hash": "2" * 64,
        "strategy_nav_usd": "5000",
    }
    complete_row = {
        "candidate_id": complete_body["candidate_id"],
        "candidate_hash": canonical_hash(complete_body),
        "candidate_body": complete_body,
    }
    ranking_snapshot = {
        "ranking_snapshot_id": "ranking-open-1",
        "broker_snapshot_hash": "1" * 64,
        "current_policy_version": "risk-v1",
        "current_policy_hash": "3" * 64,
        "cost_version": EXECUTION_COST_VERSION,
        "cost_hash": EXECUTION_COST_HASH,
        "candidates": (complete_row,),
    }
    complete = _news_candidate(
        complete_row,
        {item.contract_id: item for item in quotes},
        batch,
        now=OPEN_NOW,
        ranking_snapshot=ranking_snapshot,
    )
    assert complete is not None
    _, complete_candidate = complete
    assert complete_candidate.ranking_snapshot_id == "ranking-open-1"
    assert complete_candidate.ranking_candidate_hash == complete_row["candidate_hash"]
    assert complete_candidate.broker_snapshot_hash == "1" * 64
    assert complete_candidate.account_snapshot_hash == "2" * 64
    assert complete_candidate.risk_policy_hash == "3" * 64
    assert complete_candidate.strategy_nav_usd == Decimal("5000")
    assert complete_candidate.strategy_nav_post_hash is not None
    assert complete_candidate.economics_calculation_hash == (
        complete_candidate.evidence_hashes[0]
    )
    assert complete_candidate.economics_quote_batch_id == batch.batch_id
    assert complete_candidate.economics_quote_asof == OPEN_NOW

    release_at = OPEN_NOW - timedelta(minutes=5)
    identity = ScheduledEventIdentity(
        event_id="production-option-event",
        official_source="Bureau of Labor Statistics",
        official_source_id="production-option-event",
        title="Consumer Price Index July 2026",
        category="MACRO",
        scheduled_at=release_at,
        schedule_published_at=release_at - timedelta(days=30),
        schedule_first_seen_at=release_at - timedelta(days=20),
        schedule_observed_at=release_at - timedelta(minutes=10),
        symbols=("SPY",),
    )
    expectation = ConsensusExpectation(
        identity.content_hash,
        "headline_cpi_yoy_pct",
        Decimal("3.0"),
        "PERCENT",
        "Jin10 point-in-time calendar",
        "expectation",
        release_at - timedelta(minutes=5),
        release_at - timedelta(minutes=5),
        release_at - timedelta(minutes=5),
        "v1",
        period="2026-07",
        basis="NOT_SEASONALLY_ADJUSTED",
    )
    release = OfficialRelease(
        identity.content_hash,
        expectation.metric,
        Decimal("3.1"),
        expectation.unit,
        identity.official_source,
        "official-release",
        release_at,
        release_at + timedelta(seconds=1),
        release_at + timedelta(seconds=1),
        period=expectation.period,
        basis=expectation.basis,
    )
    ledger = EventReactionLedger.schedule(
        identity,
        expectation,
        recorded_at=expectation.observed_at,
    ).await_release(recorded_at=release_at)
    ledger = ledger.capture_release(
        release,
        recorded_at=release.captured_at,
    ).assess_surprise(recorded_at=release.captured_at)
    market = MarketReactionEvidence(
        identity.event_hash,
        release.content_hash,
        "IBKR_READ_ONLY",
        release_at,
        OPEN_NOW,
        OPEN_NOW,
        OPEN_NOW,
        {"delta": {"SPY": "1"}},
    )
    gate_row = {
        "candidate_id": complete_candidate.preselection_id,
        "candidate_hash": complete_candidate.ranking_candidate_hash,
        "layers": [
            {"gate_id": gate_id, "status": "PASS"}
            for gate_id in (
                "GATE_1_AUTHORITY_DATA",
                "GATE_2_MARKET_CREDIT_REGIME",
                "GATE_3_UNDERLYING_EVENT",
                "GATE_4_OPTION_EDGE_LIQUIDITY",
                "GATE_5_STRUCTURE_ACCOUNT_RISK",
                "GATE_6_RANKING_REVIEWABILITY",
            )
        ],
    }
    gate_payload = {
        "schema": "options_copilot.gate_bundle.v1",
        "candidates": {"production-option": gate_row},
    }
    gate_hash = canonical_hash(gate_payload)
    gate_bundle = {**gate_payload, "gate_bundle_hash": gate_hash}
    observer_snapshot = {
        **ranking_snapshot,
        "gate_bundle_hash": gate_hash,
        "decision_records": [{"record": {"gate_bundle": gate_bundle}}],
    }

    class RankingStore:
        def read_snapshot(self, snapshot_id):
            assert snapshot_id == ranking_snapshot["ranking_snapshot_id"]
            return observer_snapshot

    observer = ProductionReactionObserver(
        SimpleNamespace(
            ranking_store=RankingStore(),
            preselections=lambda: (complete_candidate,),
        ),
        SimpleNamespace(ready=True),
    )
    option = observer.reevaluate_option(ledger, market, now=OPEN_NOW)
    assert option is not None
    assert option.option_id == complete_candidate.preselection_id
    assert option.result["reaction_gate_bundle"]["gate_bundle_hash"] == gate_hash


def test_news_adapter_never_reprices_outside_us_weekday_market_windows() -> None:
    class Gateway:
        def __init__(self) -> None:
            self.calls = 0

        def option_quote_batch(self, _contracts: object) -> object:
            self.calls += 1
            raise AssertionError("IBKR must not be called outside the research window")

    class Ranking:
        def latest(self) -> object:
            raise AssertionError("ranking must not be read outside the research window")

        def read_snapshot(self, _snapshot_id: str) -> object:
            raise AssertionError("ranking must not be read outside the research window")

    class Pacing:
        ready = True

    gateway = Gateway()
    adapter = IBKRNewsResearchAdapter(
        gateway,  # type: ignore[arg-type]
        Ranking(),
        Pacing(),  # type: ignore[arg-type]
        # Saturday in New York.
        clock=lambda: datetime(2026, 8, 8, 15, 0, tzinfo=timezone.utc),
    )

    assert tuple(adapter.bindings(("SPY",))) == ()
    assert gateway.calls == 0
    assert adapter.health == "READY"
    assert adapter.health_reason == "OUTSIDE_US_EQUITY_RESEARCH_WINDOW"
    assert adapter.coverage() == {
        "requested_count": 10,
        "available_count": 0,
        "source": "FROZEN_RANKING_TOP10",
        "status": "PARTIAL",
        "reason": "INDEPENDENT_TOP10_RESEARCH_LEDGER_UNAVAILABLE",
        "decision_authority": "SUPPORTING_ONLY",
    }


def test_news_adapter_preserves_wire_level_quote_pacing_reason() -> None:
    class Gateway:
        market_data_pacing_enabled = True

        def option_quote_batch(self, _contracts: object) -> object:
            raise MarketDataPacingError(
                "snapshot_quote",
                "PACING_REQUEST_WINDOW_EXHAUSTED",
            )

    class Ranking:
        def latest(self) -> object:
            return SimpleNamespace(ranking_snapshot_id="ranking-1")

        def read_snapshot(self, _snapshot_id: str) -> object:
            return {
                "candidates": (
                    {
                        "underlying": "SPY",
                        "candidate_body": {
                            "symbol": "SPY",
                            "legs": (
                                {
                                    "contract_id_ex": "101@SMART",
                                    "underlying": "SPY",
                                    "expiration": "2026-08-21",
                                    "strike": "100",
                                    "right": "CALL",
                                    "exchange": "SMART",
                                    "multiplier": 100,
                                },
                            ),
                        },
                    },
                ),
            }

    adapter = IBKRNewsResearchAdapter(
        Gateway(),  # type: ignore[arg-type]
        Ranking(),
        SimpleNamespace(ready=True),  # type: ignore[arg-type]
        clock=lambda: OPEN_NOW,
    )

    assert tuple(adapter.bindings(("SPY",))) == ()
    assert adapter.health == "DEGRADED"
    assert (
        adapter.health_reason
        == "IBKR_OPTION_REPRICE_PACING_REQUEST_WINDOW_EXHAUSTED"
    )


def test_news_adapter_reaction_underlying_basket_is_atomic_and_option_separate() -> None:
    symbols = ("SPY", "QQQ", "IWM", "TLT", "GLD", "UUP")
    observed_at = OPEN_NOW

    class Gateway:
        market_data_pacing_enabled = True

        def __init__(self) -> None:
            self.underlying_calls = 0
            self.option_calls = 0

        def underlying_quotes(self, requested):
            self.underlying_calls += 1
            return tuple(
                SimpleNamespace(
                    symbol=symbol,
                    observed_at=observed_at,
                    bid=Decimal("99"),
                    ask=Decimal("101"),
                )
                for symbol in requested
            )

        def option_quote_batch(self, _contracts):
            self.option_calls += 1
            raise AssertionError("reaction underlying sampling used option legs")

    gateway = Gateway()
    adapter = IBKRNewsResearchAdapter(
        gateway,  # type: ignore[arg-type]
        object(),
        SimpleNamespace(ready=True),  # type: ignore[arg-type]
        clock=lambda: observed_at,
    )
    rows = adapter.reaction_underlying_quotes(symbols)
    assert tuple(item.symbol for item in rows) == symbols
    assert adapter.cached_reaction_underlying_quotes(symbols) == rows
    assert gateway.underlying_calls == 1
    assert gateway.option_calls == 0


def test_news_adapter_reaction_underlying_denial_and_partial_never_publish_cache() -> None:
    symbols = ("SPY", "QQQ", "IWM", "TLT", "GLD", "UUP")

    class Gateway:
        market_data_pacing_enabled = True

        def __init__(self) -> None:
            self.calls = 0

        def underlying_quotes(self, requested):
            self.calls += 1
            return tuple(
                SimpleNamespace(
                    symbol=symbol,
                    observed_at=OPEN_NOW,
                    bid=Decimal("99"),
                    ask=Decimal("101"),
                )
                for symbol in requested[:-1]
            )

    gateway = Gateway()
    adapter = IBKRNewsResearchAdapter(
        gateway,  # type: ignore[arg-type]
        object(),
        SimpleNamespace(ready=True),  # type: ignore[arg-type]
    )
    assert adapter.reaction_underlying_quotes(symbols) == ()
    assert adapter.cached_reaction_underlying_quotes(symbols) == ()
    assert gateway.calls == 1

    adapter.pacing = SimpleNamespace(ready=False)  # type: ignore[assignment]
    assert adapter.reaction_underlying_quotes(symbols) == ()
    assert gateway.calls == 1
