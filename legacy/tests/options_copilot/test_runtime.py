from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace

from fastapi import FastAPI
import pytest

from dataclasses import fields

import options_copilot.runtime as runtime_module
from options_copilot.api import ProposalApprovalConflict
from options_copilot.after_hours_indicative import AfterHoursIndicativeStore
from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway import (
    BatchedOptionQuote,
    OptionMarketDataRequestDiagnostic,
    OptionQuoteBatch,
    QuoteBatchStatus,
)
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.production_runtime import DurableOptionPoolTop10StructureSource
from options_copilot.runtime import (
    BASELINE_MODEL_VERSION,
    OptionsCopilotRuntime,
    RuntimeServices,
)
from options_copilot.state import ManagedSnapshotStore, RuntimeSnapshot
from options_copilot.storage.canonical import canonical_hash


def _config(tmp_path: Path) -> OptionsCopilotConfig:
    return OptionsCopilotConfig(data_dir=tmp_path / "data", log_dir=tmp_path / "logs")


def test_option_market_data_diagnostic_uses_runtime_owned_gateway() -> None:
    observed_at = datetime(2026, 9, 2, 14, 5, tzinfo=timezone.utc)
    received_contracts: list[object] = []

    class Gateway:
        connected = True

        def option_quote_batch(self, contracts):
            received_contracts.extend(contracts)
            contract = contracts[0]
            return OptionQuoteBatch(
                batch_id="batch-diagnostic",
                status=QuoteBatchStatus.PARTIAL,
                requested_at=observed_at,
                completed_at=observed_at,
                source="IBKR_REQ_MKT_DATA_READONLY",
                quotes=(
                    BatchedOptionQuote(
                        contract_id=contract.contract_id,
                        batch_id="batch-diagnostic",
                        request_id="batch-diagnostic:0:732648726",
                        requested_at=observed_at,
                        observed_at=observed_at,
                        completed_at=observed_at,
                        source="IBKR_REQ_MKT_DATA_READONLY",
                        bid=None,
                        ask=None,
                    ),
                ),
                observed_at=observed_at,
                blockers=("QUOTE_BID_UNAVAILABLE:732648726",),
                request_diagnostics=(
                    OptionMarketDataRequestDiagnostic(
                        contract_id=732648726,
                        broker_request_id=881,
                        transport="REQ_MKT_DATA_STREAMING",
                        generic_ticks=(100, 101, 106),
                        received_fields=(),
                        missing_fields=("bid", "ask"),
                        error_codes=(10090,),
                        deadline_expired=True,
                        timeout_reason=(
                            "STREAMING_REQUIRED_FIELDS_DEADLINE_EXPIRED_AFTER_API_ERROR"
                        ),
                    ),
                ),
            )

    runtime = object.__new__(OptionsCopilotRuntime)
    runtime.production_composition = SimpleNamespace(gateway=Gateway())

    payload = runtime.option_market_data_diagnostic(
        {
            "contract_id": 732648726,
            "symbol": "NVDA",
            "local_symbol": "NVDA  260918C00230000",
            "expiration": date(2026, 9, 18),
            "strike": "230",
            "right": "C",
            "exchange": "SMART",
            "trading_class": "NVDA",
            "multiplier": 100,
            "currency": "USD",
        }
    )

    assert len(received_contracts) == 1
    assert received_contracts[0].contract_id == 732648726
    assert payload["decision_authority"] == "OBSERVATION_ONLY"
    assert payload["broker_write_authority"] is False
    assert payload["request_diagnostics"][0]["broker_request_id"] == 881
    assert (
        payload["request_diagnostics"][0]["broker_timed_bbo_request_id"]
        is None
    )


def _proposal(
    observed_at: datetime,
    *,
    snapshot_id: str = "ibkr-quotes-1",
) -> dict[str, object]:
    expiration = (observed_at.astimezone(timezone.utc).date() + timedelta(days=18))
    return {
        "proposal_id": "proposal-1",
        "rank": 1,
        "eligible_to_send": True,
        "underlying": "SPY",
        "expiration": expiration.isoformat(),
        "quote_snapshot_id": snapshot_id,
        "expected_value_usd": "20.00",
        "estimated_commissions": "2.00",
        "estimated_slippage": "1.00",
        "terminal_scenarios": [
            {"terminal_underlying_price": "100", "probability": "0.734"},
            {"terminal_underlying_price": "105", "probability": "0.266"},
        ],
        "risk": {"maximum_loss_usd": "113.00"},
        "legs": [
            {
                "contract_id_ex": "101@SMART",
                "underlying": "SPY",
                "security_type": "OPT",
                "expiration": expiration.isoformat(),
                "strike": "100",
                "right": "CALL",
                "side": "BUY",
                "quantity": 1,
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
                "bid": "1.90",
                "ask": "2.00",
                "quote_time": observed_at.isoformat(),
                "quote_snapshot_id": snapshot_id,
            },
            {
                "contract_id_ex": "102@SMART",
                "underlying": "SPY",
                "security_type": "OPT",
                "expiration": expiration.isoformat(),
                "strike": "105",
                "right": "CALL",
                "side": "SELL",
                "quantity": 1,
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
                "bid": "0.90",
                "ask": "1.00",
                "quote_time": observed_at.isoformat(),
                "quote_snapshot_id": snapshot_id,
            },
        ],
    }


def test_after_hours_campaign_hash_binds_strategy_and_leg_ratios() -> None:
    payload = {
        "campaign": {"target_underlyings": 1},
        "candidates": (
            {
                "research_id": "research.spy",
                "underlying": "SPY",
                "strategy_type": "BUTTERFLY",
                "source_scan": "CORE_UNIVERSE",
                "underlying_quote_basis_hash": "1" * 64,
                "legs": (
                    {
                        "contract_id": 101,
                        "expiration": "2026-09-04",
                        "strike": "100",
                        "right": "C",
                        "side": "BUY",
                        "ratio": 1,
                    },
                    {
                        "contract_id": 102,
                        "expiration": "2026-09-04",
                        "strike": "105",
                        "right": "C",
                        "side": "SELL",
                        "ratio": 2,
                    },
                ),
            },
        ),
    }

    original = runtime_module._after_hours_campaign_hash(payload)
    changed_strategy = json.loads(json.dumps(payload))
    changed_strategy["candidates"][0]["strategy_type"] = "DEBIT_VERTICAL"
    changed_ratio = json.loads(json.dumps(payload))
    changed_ratio["candidates"][0]["legs"][1]["ratio"] = 1

    assert runtime_module._after_hours_campaign_hash(changed_strategy) != original
    assert runtime_module._after_hours_campaign_hash(changed_ratio) != original


def _snapshot(
    *,
    observed_at: datetime | None = None,
    positions: tuple[dict[str, object], ...] = (),
    candidates: tuple[dict[str, object], ...] | None = None,
    nlv: object = 5000,
    account: dict[str, object] | None = None,
    snapshot_id: str = "ibkr-quotes-1",
    broker_snapshot_complete: bool = True,
    working_order_count: int | None = 0,
    unsubmitted_instruction_count: int | None = 0,
) -> RuntimeSnapshot:
    observed = observed_at or datetime.now(timezone.utc)
    return RuntimeSnapshot(
        observed_at=observed,
        source="managed_ibkr_connector",
        account={"net_liquidation": nlv} if account is None else account,
        positions=positions,
        candidates=(
            (_proposal(observed, snapshot_id=snapshot_id),)
            if candidates is None
            else candidates
        ),
        warnings=(),
        campaign={"strategy_nav_usd": nlv},
        broker_snapshot_complete=broker_snapshot_complete,
        working_order_count=working_order_count,
        unsubmitted_instruction_count=unsubmitted_instruction_count,
    )


def test_runtime_is_no_trade_and_review_only_without_snapshot(tmp_path: Path) -> None:
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        assert runtime.candidates()["decision"] == "NO_TRADE"
        assert runtime.bootstrap()["safety"]["direct_order_submission"] is False
        assert runtime.learning_status()["champion"] == BASELINE_MODEL_VERSION
        assert runtime.learning_status()["a_grade_unlocked"] is False
        services = runtime.services()
        assert services.news_provider is not None
        assert services.calendar_provider is not None
        assert services.news_provider()["action_pool_count"] == 0
        assert services.news_provider()["approval_eligible"] is False
        assert services.calendar_provider()["approval_eligible"] is False
        assert (
            runtime.equity_pool.news_reader
            == runtime.news.decision_equity_news_payload
        )
        assert services.latest_scan_provider is not None
        assert services.latest_ranking_provider is not None
        assert services.ranking_provider is not None
        assert services.candidate_evidence_provider is not None
        assert services.management_provider is not None
        management = services.management_provider()
        assert management["available"] is True
        assert management["mode"] == "POSITION_MANAGEMENT"
        assert management["decision"] == "NO_TRADE"
        assert management["reason"] == "NO_SNAPSHOT_DERIVED_MANAGEMENT_PREVIEW"
        assert management["approval_enabled"] is False
        assert management["review_only"] is True
        assert management["direct_order_submission"] is False
        assert services.rank_one_challenge_handler is not None
        assert services.challenge_confirmation_handler is not None
        assert services.provider_configuration_provider is not None
        provider_state = services.provider_configuration_provider()
        assert {
            key: value["status"]
            for key, value in provider_state.items()
        } == {
            "jin10_mcp_token": "DISABLED",
            "finnhub_api_key": "DISABLED",
            "alpha_vantage_api_key": "DISABLED",
            "deepseek_api_key": "DISABLED",
        }
        assert all(
            item["runtime_loaded"] is False
            for item in provider_state.values()
        )
        news_health = runtime.health()["dependencies"]["news_research"]
        assert news_health["status"] == "PENDING"
        assert news_health["stale"] is True
        readiness = runtime.health()["dependencies"]["decision_runtime"]
        assert readiness["status"] == "DEGRADED"
        assert readiness["decision"] == "NO_TRADE"
        assert readiness["approval_enabled"] is False
        assert runtime.runtime_services.position_manager is not None
    finally:
        runtime.close()


def test_next_session_preparation_degrades_when_research_pools_are_empty(
    tmp_path: Path,
) -> None:
    checked_at = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours="20260804:0930-1600;20260805:0930-1600",
        trading_hours="20260804:0930-1600;20260805:0930-1600",
        timezone_id="America/New_York",
        observed_at=checked_at,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=checked_at,
    )
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        payload = runtime.next_session_preparation(
            calendar,
            checked_at,
            checked_at,
        )
    finally:
        runtime.close()

    assert payload["next_trading_date"] == "2026-08-05"
    assert payload["status"] == "DEGRADED"
    assert payload["reason_codes"] == (
        "EQUITY_POOL_EMPTY_OR_UNAVAILABLE",
        "OPTION_POOL_EMPTY_OR_UNAVAILABLE",
        "PREMARKET_PARENT_ELIGIBLE_STRUCTURE_UNAVAILABLE",
    )
    assert payload["premarket_parent_eligible_structure_count"] == 0


def test_partial_after_hours_campaign_retains_real_contract_research_without_thesis(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checked_at = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    observed_at = checked_at - timedelta(minutes=1)

    def candidate(rank: int, symbol: str) -> dict[str, object]:
        basis = {
            "schema": "options_copilot.indicative_underlying_quote_basis.v1",
            "symbol": symbol,
            "contract_id": 30_000 + rank,
            "exchange": "SMART",
            "source": "IBKR_AFTER_HOURS_UNDERLYING_READONLY",
            "observed_at": observed_at,
            "bid": Decimal("99"),
            "ask": Decimal("101"),
            "last": Decimal("100"),
            "close": Decimal("100"),
            "market_data_type": 4,
            "decision_authority": "SUPPORTING_ONLY",
        }
        return {
            "research_id": f"partial.{symbol.lower()}",
            "rank": rank,
            "underlying": symbol,
            "sector": symbol,
            "source_scan": "CORE_UNIVERSE",
            "underlying_quote_basis": {
                **basis,
                "observed_at": observed_at.isoformat(),
                "bid": "99",
                "ask": "101",
                "last": "100",
                "close": "100",
            },
            "underlying_quote_basis_hash": canonical_hash(basis),
            "legs": (
                {
                    "side": "BUY",
                    "contract_id": 40_000 + (rank * 2),
                    "contract_id_ex": f"{40_000 + (rank * 2)}@SMART",
                    "local_symbol": f"{symbol}  260904C00100000",
                    "expiration": "2026-09-04",
                    "strike": "100",
                    "right": "C",
                    "exchange": "SMART",
                    "trading_class": symbol,
                    "multiplier": 100,
                },
                {
                    "side": "SELL",
                    "contract_id": 40_001 + (rank * 2),
                    "contract_id_ex": f"{40_001 + (rank * 2)}@SMART",
                    "local_symbol": f"{symbol}  260904C00101000",
                    "expiration": "2026-09-04",
                    "strike": "101",
                    "right": "C",
                    "exchange": "SMART",
                    "trading_class": symbol,
                    "multiplier": 100,
                },
            ),
        }

    after_hours = {
        "schema": "options_copilot.after_hours_indicative.v1",
        "status": "DEGRADED",
        "observed_at": observed_at.isoformat(),
        "priced_count": 1,
        "requested_count": 2,
        "reason_codes": ["AFTER_HOURS_INDICATIVE_PARTIAL"],
        "candidates": [candidate(1, "SPY"), candidate(2, "XLF")],
        "campaign": {
            "completed_underlyings": 2,
            "target_underlyings": 10,
            "remaining_underlyings": 8,
            "continue_after_pacing_window": True,
        },
        "pacing_usage": {
            "schema": "options_copilot.after_hours_pacing_usage.v1",
            "status": "APPROVED",
            "capability_hash": "a" * 64,
            "authority": "READ_ONLY_MARKET_DATA",
        },
    }
    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours="20260804:0930-1600;20260805:0930-1600",
        trading_hours="20260804:0930-1600;20260805:0930-1600",
        timezone_id="America/New_York",
        observed_at=checked_at,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=checked_at,
    )
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        runtime._after_hours_indicative_best = after_hours
        runtime._after_hours_store.write(after_hours)
        prepared = runtime.next_session_preparation(
            calendar,
            checked_at,
            checked_at,
        )
        verified = runtime.verified_after_hours_payload()
        option = runtime.option_pool.latest_payload()
        replay = dict(verified)
        replay.pop("formal_research_pools")
        monkeypatch.setattr(
            runtime.equity_pool,
            "build",
            lambda **_kwargs: pytest.fail(
                "an exact append-only materialization must be reused"
            ),
        )
        recovered_formal = runtime._materialize_after_hours_research_pools(
            replay,
            now=checked_at,
        )
        replay["reason_codes"] = [
            "AFTER_HOURS_INDICATIVE_PARTIAL",
            "AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT",
        ]
        runtime._after_hours_indicative_best = replay
        runtime._after_hours_store.write(replay)
        runtime._restore_after_hours_formal_pools()
        restored = runtime._after_hours_store.read()
    finally:
        runtime.close()

    assert prepared["status"] == "DEGRADED"
    assert prepared["reason_codes"] == (
        "PREMARKET_PARENT_ELIGIBLE_STRUCTURE_UNAVAILABLE",
        "AFTER_HOURS_INDICATIVE_PARTIAL",
    )
    assert prepared["premarket_parent_eligible_structure_count"] == 0
    assert prepared["equity_research_count"] == 2
    assert prepared["option_research_structure_count"] == 2
    assert prepared["executable_count"] == 0
    assert prepared["decision_authority"] == "SUPPORTING_ONLY"
    assert prepared["approval_eligible"] is False
    assert prepared["instruction_creation_allowed"] is False
    assert prepared["order_allowed"] is False
    assert verified["formal_research_pools"]["option_structure_count"] == 2
    assert recovered_formal is not None
    assert recovered_formal["descriptor_hash"] == verified[
        "formal_research_pools"
    ]["descriptor_hash"]
    assert restored is not None
    assert restored["formal_research_pools"]["descriptor_hash"] == recovered_formal[
        "descriptor_hash"
    ]
    assert "AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT" not in restored[
        "reason_codes"
    ]
    assert option["research_only_count"] == 2
    assert option["exact_count"] == 0
    assert all(
        row["candidate_identity"] is not None
        and row["thesis_class"] == "UNCERTAIN"
        and "EQUITY_THESIS_EVIDENCE_UNAVAILABLE" in row["reason_codes"]
        for row in option["decisions"]
    )


def test_after_hours_formal_pool_retains_exact_identity_as_research_without_equity_thesis(
    tmp_path: Path,
) -> None:
    observed_at = datetime(2026, 8, 27, 20, 40, tzinfo=timezone.utc)
    basis = {
        "schema": "options_copilot.indicative_underlying_quote_basis.v1",
        "symbol": "XLU",
        "contract_id": 42_152_35,
        "exchange": "ARCA",
        "source": "IBKR_AFTER_HOURS_UNDERLYING_READONLY+HISTORICAL_TWO_CLOSES",
        "observed_at": observed_at,
        "bid": None,
        "ask": None,
        "last": Decimal("43.51"),
        "close": Decimal("43.31"),
        "market_data_type": 1,
        "decision_authority": "SUPPORTING_ONLY",
    }
    candidate = {
        "research_id": "after-hours.xlu",
        "rank": 1,
        "underlying": "XLU",
        "sector": "UTILITIES",
        "source_scan": "CORE_UNIVERSE",
        "underlying_quote_basis": {
            **basis,
            "observed_at": observed_at.isoformat(),
            "last": "43.51",
            "close": "43.31",
        },
        "underlying_quote_basis_hash": canonical_hash(basis),
        "legs": (
            {
                "side": "BUY",
                "contract_id": 40_100,
                "contract_id_ex": "40100@SMART",
                "local_symbol": "XLU   260911C00044000",
                "expiration": "2026-09-11",
                "strike": "44",
                "right": "C",
                "exchange": "SMART",
                "trading_class": "XLU",
                "multiplier": 100,
            },
            {
                "side": "SELL",
                "contract_id": 40_101,
                "contract_id_ex": "40101@SMART",
                "local_symbol": "XLU   260911C00044500",
                "expiration": "2026-09-11",
                "strike": "44.5",
                "right": "C",
                "exchange": "SMART",
                "trading_class": "XLU",
                "multiplier": 100,
            },
        ),
    }
    campaign = {
        "schema": "options_copilot.after_hours_indicative.v1",
        "status": "DEGRADED",
        "observed_at": observed_at.isoformat(),
        "priced_count": 1,
        "requested_count": 1,
        "reason_codes": ["AFTER_HOURS_INDICATIVE_PARTIAL"],
        "candidates": [candidate],
        "campaign": {
            "completed_underlyings": 1,
            "target_underlyings": 1,
            "remaining_underlyings": 0,
            "continue_after_pacing_window": False,
        },
        "pacing_usage": {
            "schema": "options_copilot.after_hours_pacing_usage.v1",
            "status": "APPROVED",
            "capability_hash": "a" * 64,
            "authority": "READ_ONLY_MARKET_DATA",
        },
    }

    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        formal = runtime._materialize_after_hours_research_pools(
            campaign,
            now=observed_at + timedelta(minutes=1),
        )
        equity = runtime.equity_pool.latest_payload()
        option = runtime.option_pool.latest_payload()
    finally:
        runtime.close()

    assert formal is not None
    assert equity["selected_count"] == 0
    assert equity["excluded"][0]["reasons"] == ("LIQUIDITY_MISSING",)
    assert len(option["decisions"]) == 1
    decision = option["decisions"][0]
    assert decision["disposition"] == "RESEARCH_ONLY"
    assert decision["candidate_identity"] is not None
    assert decision["equity_thesis_evidence"] is None
    assert "EQUITY_THESIS_EVIDENCE_UNAVAILABLE" in decision["reason_codes"]
    assert option["generation_reason_codes"] == [
        "AFTER_HOURS_RESEARCH_ONLY",
        "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED",
        "EQUITY_EXCLUDED_STRUCTURES_RESEARCH_ONLY",
    ]
    assert formal["option_structure_count"] == 1


def test_position_research_honors_cooperative_cancel_before_provider_work(
    tmp_path: Path,
) -> None:
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    cancelled = threading.Event()
    cancelled.set()
    try:
        payload = runtime.position_research_top10(
            cancel_event=cancelled,
            deadline_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        )
    finally:
        runtime.close()

    assert payload["status"] == "NO_TRADE"
    assert payload["reason_codes"] == ("POSITION_RESEARCH_CANCELLED",)
    assert payload["action_pool_count"] == 0


def test_completed_after_hours_campaign_materializes_formal_pools_and_restarts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checked_at = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    observed_at = checked_at - timedelta(minutes=1)
    symbols = ("SPY", "XLF", "XLE", "XLV", "XLI", "XLP", "XLU", "GLD", "TLT", "IWM")
    candidates: list[dict[str, object]] = []
    for rank, symbol in enumerate(symbols, start=1):
        basis = {
            "schema": "options_copilot.indicative_underlying_quote_basis.v1",
            "symbol": symbol,
            "contract_id": 10_000 + rank,
            "exchange": "SMART",
            "source": "IBKR_AFTER_HOURS_UNDERLYING_READONLY",
            "observed_at": observed_at,
            "bid": Decimal("109"),
            "ask": Decimal("111"),
            "last": Decimal("110"),
            "close": Decimal("100"),
            "market_data_type": 4,
            "decision_authority": "SUPPORTING_ONLY",
        }
        candidates.append(
            {
                "research_id": f"after-hours.{symbol.lower()}",
                "rank": rank,
                "underlying": symbol,
                "sector": symbol,
                "source_scan": "CORE_UNIVERSE",
                "underlying_quote_basis": {
                    **basis,
                    "observed_at": observed_at.isoformat(),
                    "bid": "109",
                    "ask": "111",
                    "last": "110",
                    "close": "100",
                },
                "underlying_quote_basis_hash": canonical_hash(basis),
                    "legs": (
                        {
                            "side": "BUY",
                            "contract_id": 20_000 + (rank * 2),
                            "contract_id_ex": f"{20_000 + (rank * 2)}@SMART",
                            "local_symbol": f"{symbol}  260904C00100000",
                            "expiration": "2026-09-04",
                            "strike": "100",
                            "right": "C",
                            "exchange": "SMART",
                            "trading_class": symbol,
                            "multiplier": 100,
                        },
                        {
                            "side": "SELL",
                            "contract_id": 20_001 + (rank * 2),
                            "contract_id_ex": f"{20_001 + (rank * 2)}@SMART",
                            "local_symbol": f"{symbol}  260904C00105000",
                            "expiration": "2026-09-04",
                            "strike": "105",
                            "right": "C",
                            "exchange": "SMART",
                            "trading_class": symbol,
                            "multiplier": 100,
                        },
                    ),
            }
        )
    after_hours = {
        "schema": "options_copilot.after_hours_indicative.v1",
        "status": "AVAILABLE",
        "observed_at": observed_at.isoformat(),
        "candidates": candidates,
        "campaign": {
            "completed_underlyings": 10,
            "target_underlyings": 10,
            "remaining_underlyings": 0,
            "continue_after_pacing_window": False,
        },
        "pacing_usage": {
            "schema": "options_copilot.after_hours_pacing_usage.v1",
            "status": "APPROVED",
            "capability_hash": "a" * 64,
            "authority": "READ_ONLY_MARKET_DATA",
        },
    }
    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours="20260804:0930-1600;20260805:0930-1600",
        trading_hours="20260804:0930-1600;20260805:0930-1600",
        timezone_id="America/New_York",
        observed_at=checked_at,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=checked_at,
    )
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        runtime._after_hours_indicative_best = after_hours
        runtime._after_hours_store.write(after_hours)
        prepared = runtime.next_session_preparation(
            calendar,
            checked_at,
            checked_at,
        )
        equity = runtime.equity_pool.latest_payload()
        option = runtime.option_pool.latest_payload()
        verified = runtime.verified_after_hours_payload()
        cache_path = tmp_path / "data" / "after_hours_indicative.json"
        tampered = json.loads(cache_path.read_text(encoding="utf-8"))
        tampered["payload"]["formal_research_pools"][
            "equity_research_count"
        ] = 99
        cache_path.write_text(json.dumps(tampered), encoding="utf-8")
        assert runtime.verified_after_hours_payload() == {}
        runtime._after_hours_store.write(verified)
        class ForbiddenFallback:
            def resolve_top10(self, *, scheduled_for):
                raise AssertionError(
                    f"thesis-bound durable pool unexpectedly fell back: {scheduled_for}"
                )

        premarket = DurableOptionPoolTop10StructureSource(
            runtime.option_pool_store,
            fallback=ForbiddenFallback(),
        ).resolve_top10(
            scheduled_for=datetime(2026, 8, 5, 13, 20, tzinfo=timezone.utc)
        )
    finally:
        runtime.close()

    formal = prepared["after_hours_formal_research_pools"]
    legacy_revision_hash = canonical_hash({
        "schema": "options_copilot.after_hours_materialization_revision.v1",
        "campaign_hash": formal["campaign_hash"],
        "campaign_observed_at": observed_at,
    })
    assert formal["materialization_revision_hash"] != legacy_revision_hash
    assert verified["formal_research_pools"]["descriptor_hash"] == formal[
        "descriptor_hash"
    ]
    assert prepared["equity_research_count"] == 10
    assert formal["equity_research_count"] == 10
    assert formal["equity_selected_count"] == equity["selected_count"] > 0
    assert formal["equity_pool_hash"] == prepared["equity_pool_hash"]
    assert formal["option_pool_hash"] == prepared["option_pool_hash"]
    assert formal["option_structure_count"] == len(option["decisions"]) > 0
    assert formal["option_structure_count"] == equity["discovery_count"]
    assert prepared["option_research_structure_count"] == equity["discovery_count"]
    assert prepared["premarket_parent_eligible_structure_count"] == 7
    assert prepared["premarket_parent_eligible_structure_count"] < equity[
        "selected_count"
    ]
    assert prepared["executable_count"] == 0
    assert option["research_only_count"] == equity["discovery_count"]
    assert option["exact_count"] == 0
    assert "EQUITY_EXCLUDED_STRUCTURES_RESEARCH_ONLY" in option[
        "generation_reason_codes"
    ]
    assert sum(
        item["equity_thesis_evidence"] is None
        for item in option["decisions"]
    ) == equity["excluded_count"]
    assert all(item["candidate_identity"] for item in option["decisions"])
    assert all(item["exact_economics"]["legs"] for item in option["decisions"])
    thesis_bound = tuple(
        item
        for item in option["decisions"]
        if item["equity_thesis_evidence"] is not None
    )
    assert thesis_bound
    assert len(premarket.structures) == equity["selected_count"]
    assert premarket.reason_codes == ()
    assert premarket.missing_symbols == ()
    assert all(
        item["exact_economics"]["equity_thesis_hash"]
        == item["equity_thesis_hash"]
        and item["exact_economics"]["invalidation_evidence"]["status"]
        == "BOUND"
        and item["exact_economics"]["assignment_evidence"]["status"]
        == "SUPPORTED"
        and item["exact_economics"]["ex_dividend_evidence"]["status"]
        == "SUPPORTED"
        for item in thesis_bound
    )
    assert all(
        leg["short_leg_risk_evidence"]["status"] == "SUPPORTED"
        for item in thesis_bound
        for leg in item["exact_economics"]["legs"]
        if leg["side"] in {"SELL", "SHORT"}
    )
    assert all(
        {leg["right"] for leg in item["exact_economics"]["legs"]}
        <= {"CALL", "PUT"}
        for item in option["decisions"]
    )
    assert all(
        "EXACT_CONTRACT_IDENTITY_INCOMPLETE" not in item["reason_codes"]
        and "STRUCTURE_TEMPLATE_SEMANTICS_INVALID" not in item["reason_codes"]
        for item in option["decisions"]
    )
    assert all(
        "EXECUTABLE_LEG_QUOTE_INCOMPLETE" in item["reason_codes"]
        and "OPTION_GREEKS_INCOMPLETE" in item["reason_codes"]
        and "OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE" in item["reason_codes"]
        and "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE" in item["reason_codes"]
        and "AFTER_COST_ECONOMICS_INCOMPLETE" in item["reason_codes"]
        for item in option["decisions"]
    )
    assert all(
        decision["disposition"] != "EXACT_EVIDENCE_CAPTURED"
        for decision in option["decisions"]
    )
    assert prepared["decision"] == "NO_TRADE"
    assert prepared["order_allowed"] is False

    class _AdvancedWallClock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = checked_at + timedelta(days=3)
            return value if tz is None else value.astimezone(tz)

    first_candidate = runtime_module._after_hours_option_pool_candidate(
        candidates[0],
        campaign_hash=formal["campaign_hash"],
        observed_at=observed_at,
    )
    monkeypatch.setattr(runtime_module, "_after_hours_cache_fresh", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime_module, "datetime", _AdvancedWallClock)
    replay_candidate = runtime_module._after_hours_option_pool_candidate(
        candidates[0],
        campaign_hash=formal["campaign_hash"],
        observed_at=observed_at,
    )
    assert replay_candidate == first_candidate
    monkeypatch.setattr(runtime_module, "datetime", datetime)
    restarted = OptionsCopilotRuntime(_config(tmp_path))
    try:
        restored = restarted.after_hours_latest()
        restored_equity = restarted.equity_pool.latest_payload()
        restored_option = restarted.option_pool.latest_payload()
    finally:
        restarted.close()
    assert restored["formal_research_pools"]["campaign_hash"] == formal["campaign_hash"]
    assert restored["formal_research_pools"]["descriptor_hash"] == formal["descriptor_hash"]
    assert restored_equity["snapshot_hash"] == formal["equity_pool_hash"]
    assert restored_option["snapshot_hash"] == formal["option_pool_hash"]
    with sqlite3.connect(tmp_path / "data" / "equity_pool.sqlite3") as connection:
        equity_rows = connection.execute(
            "SELECT COUNT(*) FROM equity_pool_snapshots"
        ).fetchone()[0]
    with sqlite3.connect(
        tmp_path / "data" / "option_structure_pool.sqlite3"
    ) as connection:
        option_rows = connection.execute(
            "SELECT COUNT(*) FROM option_structure_pools"
        ).fetchone()[0]

    restarted_again = OptionsCopilotRuntime(_config(tmp_path))
    try:
        restored_again = restarted_again.after_hours_latest()
    finally:
        restarted_again.close()
    assert (
        restored_again["formal_research_pools"]["descriptor_hash"]
        == formal["descriptor_hash"]
    )
    with sqlite3.connect(tmp_path / "data" / "equity_pool.sqlite3") as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM equity_pool_snapshots"
        ).fetchone()[0] == equity_rows
    with sqlite3.connect(
        tmp_path / "data" / "option_structure_pool.sqlite3"
    ) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM option_structure_pools"
        ).fetchone()[0] == option_rows


def test_next_session_fails_closed_on_conflicting_formal_pool_lineage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checked_at = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
    observed_at = checked_at - timedelta(minutes=1)
    fixed_slot = observed_at.replace(microsecond=481_516)

    def campaign(*, symbol: str, sector: str) -> dict[str, object]:
        basis = {
            "schema": "options_copilot.indicative_underlying_quote_basis.v1",
            "symbol": symbol,
            "contract_id": 10_001,
            "exchange": "SMART",
            "source": "IBKR_AFTER_HOURS_UNDERLYING_READONLY",
            "observed_at": observed_at,
            "bid": Decimal("109"),
            "ask": Decimal("111"),
            "last": Decimal("110"),
            "close": Decimal("100"),
            "market_data_type": 4,
            "decision_authority": "SUPPORTING_ONLY",
        }
        return {
            "schema": "options_copilot.after_hours_indicative.v1",
            "status": "AVAILABLE",
            "observed_at": observed_at.isoformat(),
            "candidates": [
                {
                    "research_id": f"after-hours.{symbol.lower()}.{sector.lower()}",
                    "rank": 1,
                    "underlying": symbol,
                    "sector": sector,
                    "source_scan": "CORE_UNIVERSE",
                    "underlying_quote_basis": {
                        **basis,
                        "observed_at": observed_at.isoformat(),
                        "bid": "109",
                        "ask": "111",
                        "last": "110",
                        "close": "100",
                    },
                    "underlying_quote_basis_hash": canonical_hash(basis),
                    "legs": (
                        {
                            "side": "BUY",
                            "contract_id": 20_001,
                            "contract_id_ex": "20001@SMART",
                            "local_symbol": f"{symbol}   260904C00100000",
                            "expiration": "2026-09-04",
                            "strike": "100",
                            "right": "C",
                            "exchange": "SMART",
                            "trading_class": symbol,
                            "multiplier": 100,
                        },
                        {
                            "side": "SELL",
                            "contract_id": 20_002,
                            "contract_id_ex": "20002@SMART",
                            "local_symbol": f"{symbol}   260904C00105000",
                            "expiration": "2026-09-04",
                            "strike": "105",
                            "right": "C",
                            "exchange": "SMART",
                            "trading_class": symbol,
                            "multiplier": 100,
                        },
                    ),
                }
            ],
            "campaign": {
                "completed_underlyings": 1,
                "target_underlyings": 1,
                "remaining_underlyings": 0,
                "continue_after_pacing_window": False,
            },
            "pacing_usage": {
                "schema": "options_copilot.after_hours_pacing_usage.v1",
                "status": "APPROVED",
                "capability_hash": "a" * 64,
                "authority": "READ_ONLY_MARKET_DATA",
            },
        }

    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours="20260804:0930-1600;20260805:0930-1600",
        trading_hours="20260804:0930-1600;20260805:0930-1600",
        timezone_id="America/New_York",
        observed_at=checked_at,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=checked_at,
    )
    monkeypatch.setattr(
        runtime_module,
        "_after_hours_materialization_slot",
        lambda *_args, **_kwargs: fixed_slot,
    )
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        unrelated = campaign(symbol="SPY", sector="BROAD_MARKET")
        formal = runtime._materialize_after_hours_research_pools(
            unrelated,
            now=checked_at,
        )
        assert formal is not None
        assert runtime.equity_pool.latest_payload()["selected_count"] > 0
        option_payload = runtime.option_pool.latest_payload()
        assert len(option_payload["decisions"]) > 0
        captured = tuple(
            decision
            for decision in option_payload["decisions"]
            if decision["candidate_id"] is not None
        )
        assert captured
        assert captured[0]["thesis_class"] == "DIRECTIONAL_BULLISH"
        assert captured[0]["equity_thesis_evidence"] is not None
        runtime._after_hours_indicative_best = campaign(
            symbol="XLF",
            sector="UNRELATED_SECTOR",
        )

        with sqlite3.connect(
            tmp_path / "data" / "equity_pool.sqlite3"
        ) as connection:
            equity_rows_before = connection.execute(
                "SELECT COUNT(*) FROM equity_pool_snapshots"
            ).fetchone()[0]
        with sqlite3.connect(
            tmp_path / "data" / "option_structure_pool.sqlite3"
        ) as connection:
            option_rows_before = connection.execute(
                "SELECT COUNT(*) FROM option_structure_pools"
            ).fetchone()[0]

        prepared = runtime.next_session_preparation(
            calendar,
            checked_at,
            checked_at,
        )

        with sqlite3.connect(
            tmp_path / "data" / "equity_pool.sqlite3"
        ) as connection:
            equity_rows_after = connection.execute(
                "SELECT COUNT(*) FROM equity_pool_snapshots"
            ).fetchone()[0]
        with sqlite3.connect(
            tmp_path / "data" / "option_structure_pool.sqlite3"
        ) as connection:
            option_rows_after = connection.execute(
                "SELECT COUNT(*) FROM option_structure_pools"
            ).fetchone()[0]
    finally:
        runtime.close()

    assert prepared["status"] == "DEGRADED"
    assert prepared["decision"] == "NO_TRADE"
    assert prepared["reason_codes"] == (
        "AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT",
    )
    assert prepared["equity_selected_count"] == 0
    assert prepared["option_structure_count"] == 0
    assert prepared["equity_pool_hash"] is None
    assert prepared["option_pool_hash"] is None
    assert prepared["after_hours_campaign_hash"] is None
    assert prepared["after_hours_formal_research_pools"] is None
    assert equity_rows_after == equity_rows_before
    assert option_rows_after == option_rows_before


def test_legacy_completed_cache_formalizes_exclusions_then_merges_basis_recovery(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixed_now = datetime(2026, 8, 22, 5, 0, tzinfo=timezone.utc)
    symbols = ("HOWL", "XLF", "XLE", "XLV", "XLI", "XLP", "XLU", "GLD", "TLT", "IWM")

    def legacy_candidate(rank: int, symbol: str) -> dict[str, object]:
        return {
            "research_id": f"legacy.{symbol.lower()}",
            "rank": rank,
            "underlying": symbol,
            "sector": symbol,
            "source_scan": "MOST_ACTIVE" if symbol == "HOWL" else "CORE_UNIVERSE",
            "pricing_status": "AVAILABLE" if rank <= 8 else "UNAVAILABLE",
            "legs": (
                {
                    "side": "BUY",
                    "contract_id": rank * 100 + 1,
                    "contract_id_ex": f"{rank * 100 + 1}@SMART",
                    "expiration": "2026-09-18",
                    "strike": "100",
                    "right": "C",
                    "exchange": "SMART",
                    "trading_class": symbol,
                    "multiplier": 100,
                },
                {
                    "side": "SELL",
                    "contract_id": rank * 100 + 2,
                    "contract_id_ex": f"{rank * 100 + 2}@SMART",
                    "expiration": "2026-09-18",
                    "strike": "105",
                    "right": "C",
                    "exchange": "SMART",
                    "trading_class": symbol,
                    "multiplier": 100,
                },
            ),
        }

    legacy = {
        "schema": "options_copilot.after_hours_indicative.v1",
        "status": "DEGRADED",
        "observed_at": (fixed_now - timedelta(minutes=1)).isoformat(),
        "priced_count": 8,
        "requested_count": 10,
        "reason_codes": ["AFTER_HOURS_INDICATIVE_PARTIAL"],
        "candidates": [
            legacy_candidate(rank, symbol)
            for rank, symbol in enumerate(symbols, start=1)
        ],
        "campaign": {
            "completed_underlyings": 10,
            "target_underlyings": 10,
            "remaining_underlyings": 0,
            "continue_after_pacing_window": False,
        },
    }
    config = _config(tmp_path)
    AfterHoursIndicativeStore(
        config.data_dir / "after_hours_indicative.json"
    ).write(legacy)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is None else fixed_now.astimezone(tz)

    monkeypatch.setattr(runtime_module, "datetime", FixedDatetime)
    runtime = OptionsCopilotRuntime(config)
    try:
        initial = runtime.after_hours_latest()
        initial_formal = initial["formal_research_pools"]
        assert initial_formal["migration_status"] == "PENDING"
        assert initial_formal["underlying_basis_bound_count"] == 0
        assert initial_formal["underlying_basis_missing_count"] == 10
        assert runtime.equity_pool.latest_payload()["discovery_count"] == 10
        assert runtime.equity_pool.latest_payload()["selected_count"] == 0
        assert runtime.option_pool.latest_payload()["status"] == "READY"

        recovered = []
        for row in legacy["candidates"][:2]:
            symbol = str(row["underlying"])
            basis = {
                "schema": "options_copilot.indicative_underlying_quote_basis.v1",
                "symbol": symbol,
                "contract_id": 20_000 + int(row["rank"]),
                "exchange": "SMART",
                "source": "IBKR_AFTER_HOURS_UNDERLYING_READONLY",
                "observed_at": fixed_now,
                "bid": Decimal("109"),
                "ask": Decimal("111"),
                "last": Decimal("110"),
                "close": Decimal("100"),
                "market_data_type": 4,
                "decision_authority": "SUPPORTING_ONLY",
            }
            recovered.append(
                {
                    **row,
                    "underlying_quote_basis": {
                        **basis,
                        "observed_at": fixed_now.isoformat(),
                        "bid": "109",
                        "ask": "111",
                        "last": "110",
                        "close": "100",
                    },
                    "underlying_quote_basis_hash": canonical_hash(basis),
                }
            )

        class Source:
            def __init__(self, *_args, **_kwargs):
                self.discovery_metadata = ()

            def resolve_top10(self, *, scheduled_for):
                return SimpleNamespace(reason_codes=())

        monkeypatch.setattr(runtime_module, "DirectTop10StructureSource", Source)
        monkeypatch.setattr(
            runtime_module,
            "_after_hours_research_from_resolution",
            lambda *_args, **_kwargs: {
                "candidates": recovered,
                "reason_codes": (),
            },
        )

        def build_read_model(research, **_kwargs):
            return {
                "schema": "options_copilot.after_hours_indicative.v1",
                "status": "DEGRADED",
                "observed_at": fixed_now.isoformat(),
                "priced_count": 0,
                "requested_count": 10,
                "reason_codes": ["AFTER_HOURS_INDICATIVE_PARTIAL"],
                "candidates": [dict(row) for row in research["candidates"]],
            }

        monkeypatch.setattr(
            runtime_module,
            "build_after_hours_indicative_read_model",
            build_read_model,
        )
        runtime.production_composition = SimpleNamespace(
            gateway=object(),
            pipeline_inputs=SimpleNamespace(
                pacing=SimpleNamespace(capability_hash="a" * 64)
            ),
            lifecycle=SimpleNamespace(close=lambda: True),
        )
        runtime._current_control_projection = lambda: None  # type: ignore[method-assign]

        refreshed = runtime.after_hours_indicative()
        refreshed_formal = refreshed["formal_research_pools"]
        persisted = runtime._after_hours_store.read()
    finally:
        runtime.close()

    assert "MORE_COMPLETE_RUNTIME_BATCH_RETAINED" in refreshed["reason_codes"]
    assert refreshed_formal["underlying_basis_bound_count"] == 2
    assert refreshed_formal["underlying_basis_missing_count"] == 8
    assert refreshed_formal["migration_status"] == "PENDING"
    assert persisted is not None
    assert sum(
        row.get("underlying_quote_basis") is not None
        for row in persisted["candidates"]
    ) == 2
    assert (
        refreshed_formal["materialization_revision_hash"]
        != initial_formal["materialization_revision_hash"]
    )
    with sqlite3.connect(config.data_dir / "equity_pool.sqlite3") as connection:
        equity_rows = connection.execute(
            "SELECT COUNT(*) FROM equity_pool_snapshots"
        ).fetchone()[0]
    with sqlite3.connect(
        config.data_dir / "option_structure_pool.sqlite3"
    ) as connection:
        option_rows = connection.execute(
            "SELECT COUNT(*) FROM option_structure_pools"
        ).fetchone()[0]

    for _ in range(2):
        restarted = OptionsCopilotRuntime(config)
        try:
            restored = restarted.after_hours_latest()
            assert (
                restored["formal_research_pools"]["descriptor_hash"]
                == refreshed_formal["descriptor_hash"]
            )
            assert (
                restarted.equity_pool.latest_payload()["snapshot_hash"]
                == refreshed_formal["equity_pool_hash"]
            )
            assert (
                restarted.option_pool.latest_payload()["snapshot_hash"]
                == refreshed_formal["option_pool_hash"]
            )
        finally:
            restarted.close()
    with sqlite3.connect(config.data_dir / "equity_pool.sqlite3") as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM equity_pool_snapshots"
        ).fetchone()[0] == equity_rows
    with sqlite3.connect(
        config.data_dir / "option_structure_pool.sqlite3"
    ) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM option_structure_pools"
        ).fetchone()[0] == option_rows


def test_runtime_provider_configuration_reads_only_fixed_non_secret_states(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    key_path = config.data_dir / "api_keys.local.json"
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_text(
        json.dumps(
            {
                "jin10_mcp_token": "",
                "finnhub_api_key": "runtime-secret-must-not-cross-api",
                "alpha_vantage_api_key": "",
                "deepseek_api_key": "",
            }
        ),
        encoding="utf-8",
    )

    runtime = OptionsCopilotRuntime(config)
    try:
        provider = runtime.services().provider_configuration_provider
        assert provider is not None
        payload = provider()
        assert payload["finnhub_api_key"]["status"] == "CONFIGURED"
        assert payload["finnhub_api_key"]["composed"] is True
        assert payload["finnhub_api_key"]["runtime_loaded"] is True
        assert {
            item["status"] for item in payload.values()
        } == {"CONFIGURED", "DISABLED"}
        assert "runtime-secret-must-not-cross-api" not in repr(payload)

        key_path.write_text(
            json.dumps(
                {
                    "jin10_mcp_token": "",
                    "finnhub_api_key": "",
                    "alpha_vantage_api_key": "",
                    "deepseek_api_key": "",
                }
            ),
            encoding="utf-8",
        )
        changed = provider()
        assert changed["finnhub_api_key"]["status"] == "DISABLED"
        assert changed["finnhub_api_key"]["restart_required"] is True
        assert changed["finnhub_api_key"]["runtime_loaded"] is False
    finally:
        runtime.close()


def test_runtime_exposes_the_injected_production_services(tmp_path: Path) -> None:
    injected = RuntimeServices(
        **{field.name: None for field in fields(RuntimeServices)}
    )
    runtime = OptionsCopilotRuntime(_config(tmp_path), runtime_services=injected)
    try:
        assert runtime.runtime_services is injected
        assert runtime.latest_ranking()["decision"] == "NO_TRADE"
        assert runtime.services().readiness_provider is not None
        assert runtime.services().readiness_provider()["status"] == "DEGRADED"
    finally:
        runtime.close()


def test_control_bootstrap_projects_only_fully_reconciled_current_strategy_nav(
    tmp_path: Path,
) -> None:
    observed_at = datetime(2026, 8, 27, 1, 35, tzinfo=timezone.utc)

    class NavSource:
        def snapshot(self, *, asof, observed_account_nlv):
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

        def guard_current(self, _snapshot, *, callback):
            return callback()

    dependencies = {field.name: None for field in fields(RuntimeServices)}
    dependencies["strategy_nav_source"] = NavSource()
    runtime = OptionsCopilotRuntime(
        _config(tmp_path),
        runtime_services=RuntimeServices(**dependencies),
    )
    try:
        payload = runtime._control_bootstrap(
            {
                "status": "CURRENT",
                "observed_at": observed_at.isoformat(),
                "account": {
                    "asof": observed_at.isoformat(),
                    "net_liquidation": Decimal("2500"),
                    "connected": True,
                },
            }
        )

        assert payload["account"]["reconciled"] is True
        assert payload["campaign"]["strategy_nav_usd"] == Decimal("2000")
        assert payload["campaign"]["account_nlv_usd"] == Decimal("2500")
        assert payload["campaign"]["reconciliation_difference_usd"] == Decimal(
            "500"
        )
        assert payload["campaign"]["strategy_nav_asof"] == observed_at.isoformat()
        assert payload["campaign"]["account_observed_at"] == observed_at.isoformat()
        assert payload["account"]["strategy_nav_asof"] == observed_at.isoformat()
        assert payload["account"]["observed_at"] == observed_at.isoformat()
        assert payload["account"]["reconciliation_difference_usd"] == Decimal(
            "500"
        )
        assert payload["account"]["strategy_nav_content_hash"]
        assert payload["warnings"] == []

        stale = runtime._control_bootstrap(
            {
                "status": "STALE",
                "observed_at": observed_at.isoformat(),
                "account": {
                    "asof": observed_at.isoformat(),
                    "net_liquidation": Decimal("2500"),
                    "connected": True,
                    "reconciled": True,
                },
            }
        )
        assert stale["account"]["reconciled"] is None
        assert stale["account"]["decision_authority"] == "LAST_KNOWN_ONLY"
    finally:
        runtime.close()


def test_control_bootstrap_never_claims_reconciliation_for_mismatched_nav(
    tmp_path: Path,
) -> None:
    observed_at = datetime(2026, 8, 27, 1, 35, tzinfo=timezone.utc)

    class MismatchedNavSource:
        def snapshot(self, *, asof, observed_account_nlv):
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
                "observed_account_nlv": observed_account_nlv + Decimal("1"),
                "reconciliation_difference": Decimal("501"),
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

        def guard_current(self, _snapshot, *, callback):
            return callback()

    dependencies = {field.name: None for field in fields(RuntimeServices)}
    dependencies["strategy_nav_source"] = MismatchedNavSource()
    runtime = OptionsCopilotRuntime(
        _config(tmp_path),
        runtime_services=RuntimeServices(**dependencies),
    )
    try:
        payload = runtime._control_bootstrap(
            {
                "status": "CURRENT",
                "observed_at": observed_at.isoformat(),
                "account": {
                    "asof": observed_at.isoformat(),
                    "net_liquidation": Decimal("2500"),
                    "connected": True,
                },
            }
        )

        assert payload["account"].get("reconciled") is not True
        assert "strategy_nav_usd" not in payload["campaign"]
        assert payload["warnings"] == ["STRATEGY_NAV_RECONCILIATION_INVALID"]
    finally:
        runtime.close()


def test_runtime_snapshot_remains_atomic_observation_only(tmp_path: Path) -> None:
    config = _config(tmp_path)
    snapshot = _snapshot(
        positions=({"asset_class": "OPT", "symbol": "GLD", "position": 1},),
    )
    runtime = OptionsCopilotRuntime(config)
    try:
        runtime.ingest(snapshot)
        assert runtime.positions()["positions"][0]["symbol"] == "GLD"
        candidate = runtime.candidates()["candidates"][0]
        assert candidate["eligible_to_send"] is False
        assert candidate["decision_authority"] == "OBSERVATION_ONLY"
        assert "approval_challenge" not in candidate
        assert runtime.latest_ranking()["candidates"] == ()
        with pytest.raises(ProposalApprovalConflict, match="observation-only"):
            runtime.approve_proposal("proposal-1", {})
    finally:
        runtime.close()
    restored = ManagedSnapshotStore(config.data_dir / "runtime_snapshot.json").read()
    assert restored is not None and restored.account["net_liquidation"] == 5000
    assert restored.broker_snapshot_complete is True


def test_stale_runtime_snapshot_is_projected_as_last_known_only(
    tmp_path: Path,
) -> None:
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        runtime.ingest(
            _snapshot(
                observed_at=datetime.now(timezone.utc) - timedelta(seconds=16),
                account={
                    "net_liquidation": 5000,
                    "connected": True,
                    "reconciled": True,
                    "market_data_status": "ACCOUNT_LIVE",
                },
                positions=(
                    {"asset_class": "OPT", "symbol": "GLD", "position": 1},
                ),
            )
        )

        bootstrap = runtime.bootstrap()
        positions = runtime.positions()
        candidates = runtime.candidates()

        assert bootstrap["account"]["net_liquidation"] == 5000
        assert bootstrap["account"]["status"] == "STALE"
        assert bootstrap["account"]["decision_authority"] == "LAST_KNOWN_ONLY"
        assert bootstrap["account"]["connected"] is None
        assert bootstrap["account"]["reconciled"] is None
        assert bootstrap["account"]["market_data_status"] is None
        assert bootstrap["campaign"]["strategy_nav_usd"] == 5000
        assert bootstrap["campaign"]["status"] == "STALE"
        assert "SNAPSHOT_STALE" in bootstrap["warnings"]
        assert positions == {
            "positions": [
                {"asset_class": "OPT", "symbol": "GLD", "position": 1}
            ],
            "status": "STALE",
            "position_state_known": False,
            "decision_authority": "LAST_KNOWN_ONLY",
            "asof": bootstrap["asof"],
            "reason": "SNAPSHOT_STALE",
        }
        assert candidates["candidates"]
        assert candidates["candidates"][0]["freshness_status"] == "STALE"
        assert candidates["candidates"][0]["eligible_to_send"] is False
        assert candidates["reason"] == "SNAPSHOT_STALE"
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("snapshot_kwargs", "expected_reason"),
    [
        ({"broker_snapshot_complete": False}, "SNAPSHOT_INCOMPLETE"),
        ({"working_order_count": None}, "WORKING_ORDER_STATE_UNKNOWN"),
        (
            {"unsubmitted_instruction_count": None},
            "SAVED_INSTRUCTION_STATE_UNKNOWN",
        ),
    ],
)
def test_incomplete_runtime_snapshot_never_implies_empty_current_broker_state(
    tmp_path: Path,
    snapshot_kwargs: dict[str, object],
    expected_reason: str,
) -> None:
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        runtime.ingest(_snapshot(**snapshot_kwargs))

        bootstrap = runtime.bootstrap()
        positions = runtime.positions()
        if expected_reason == "SAVED_INSTRUCTION_STATE_UNKNOWN":
            assert bootstrap["account"]["status"] == "PARTIAL"
            assert positions["status"] == "PARTIAL"
            assert positions["position_state_known"] is True
            assert positions["decision_authority"] == "LAST_KNOWN_ONLY"
        else:
            assert bootstrap["account"] == {"status": "UNAVAILABLE"}
            assert positions["positions"] == []
            assert positions["position_state_known"] is False
        assert positions["reason"] == expected_reason
    finally:
        runtime.close()


def test_external_snapshot_nlv_cannot_change_ranking_or_risk_authority(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = OptionsCopilotRuntime(config)
    try:
        runtime.ingest(_snapshot(nlv=5000, snapshot_id="quotes-a"))
        before = runtime.latest_ranking()
        ManagedSnapshotStore(config.data_dir / "runtime_snapshot.json").write(
            _snapshot(nlv=9_999_999, snapshot_id="quotes-b")
        )
        after = runtime.latest_ranking()

        assert before == after
        assert after["decision"] == "NO_TRADE"
        assert runtime.bootstrap()["account"]["net_liquidation"] == 9_999_999
        assert runtime.bootstrap()["safety"]["risk_basis"] == (
            "STRATEGY_NAV_SNAPSHOT_ONLY"
        )
        assert runtime.bootstrap()["safety"]["account_nlv_authority"] == (
            "OBSERVATION_AND_RECONCILIATION_ONLY"
        )
    finally:
        runtime.close()


def test_runtime_reloads_external_snapshot_without_issuing_legacy_challenge(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    runtime = OptionsCopilotRuntime(config)
    try:
        runtime.ingest(_snapshot(snapshot_id="quotes-a"))
        first = runtime.candidates()["candidates"][0]
        replacement = _snapshot(snapshot_id="quotes-b", nlv=6000)
        ManagedSnapshotStore(config.data_dir / "runtime_snapshot.json").write(replacement)
        assert runtime.bootstrap()["account"]["net_liquidation"] == 6000
        second = runtime.candidates()["candidates"][0]
        assert second["quote_snapshot_id"] == "quotes-b"
        assert "approval_challenge" not in first
        assert "approval_challenge" not in second
        assert first["decision_authority"] == second["decision_authority"] == (
            "OBSERVATION_ONLY"
        )
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "snapshot",
    [
        lambda now: _snapshot(observed_at=now - timedelta(seconds=6)),
        lambda now: _snapshot(observed_at=now + timedelta(minutes=1)),
        lambda now: _snapshot(broker_snapshot_complete=False),
        lambda now: _snapshot(working_order_count=1),
        lambda now: _snapshot(unsubmitted_instruction_count=1),
    ],
)
def test_every_legacy_snapshot_variant_remains_observation_only(
    tmp_path: Path,
    snapshot,
) -> None:
    now = datetime.now(timezone.utc)
    runtime = OptionsCopilotRuntime(_config(tmp_path))
    try:
        runtime.ingest(snapshot(now))
        payload = runtime.candidates()
        assert payload["decision"] == "NO_TRADE"
        assert payload["approval_enabled"] is False
        assert all(
            item["decision_authority"] == "OBSERVATION_ONLY"
            and item["eligible_to_send"] is False
            and "approval_challenge" not in item
            for item in payload["candidates"]
        )
    finally:
        runtime.close()


def test_build_app_registers_lifecycle_on_locked_fastapi_router(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class StubRuntime:
        def __init__(self, config: object) -> None:
            self.config = config

        def services(self) -> object:
            return object()

        def start(self) -> None:
            events.append("startup")

        def close(self) -> None:
            events.append("shutdown")

    monkeypatch.setattr(runtime_module, "OptionsCopilotRuntime", StubRuntime)
    monkeypatch.setattr(
        runtime_module,
        "create_app",
        lambda services: FastAPI(),
    )

    app = runtime_module.build_app(object())

    assert hasattr(app, "add_event_handler") is False
    assert app.router.on_startup == [app.state.runtime.start]
    assert len(app.router.on_shutdown) == 1
    assert app.router.on_shutdown[0] is not app.state.runtime.close

    app.router.on_startup[0]()
    app.router.on_shutdown[0]()
    assert events == ["startup", "shutdown"]
