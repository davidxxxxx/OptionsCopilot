from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import os
import sqlite3
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
)
from options_copilot.api.app import OptionsCopilotServices, create_app
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
)
from options_copilot.news import ImpactDirection, OptionTradabilityInput
from options_copilot.news.analysis_store import IntegrityVerificationProgress
from options_copilot.news.classifier import StructuredLlmAdapter
from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    MarketConfirmation,
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
    evaluate_preselection,
    strategy_structure_hash,
)
from options_copilot.news_runtime import (
    IbkrNewsBinding,
    NewsCoordinator,
    _equity_news_rows,
    _remaining_poll_wait_seconds,
    _source_reason,
    _verified_symbol_binding_proof,
)
from options_copilot.providers import EarningsEvent, NewsEvent, SymbolBindingProof
from options_copilot.providers.entity_linking import (
    ENTITY_LINK_CATALOG_HASH,
    ENTITY_LINK_CATALOG_VERSION,
)
from options_copilot.providers.official import (
    OfficialCalendarEvent,
    OfficialCalendarProvider,
    OfficialCalendarSnapshot,
    OfficialCalendarSource,
    OfficialEventProvenance,
    OfficialSourceHealth,
)
from options_copilot.providers.jin10 import Jin10EventProvider
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.evidence import (
    EvidenceRecord,
    EvidenceStore,
    EvidenceStoreCorruption,
)


NOW = datetime(2026, 8, 4, 12, 45, tzinfo=timezone.utc)
SOURCE_API_SOURCE_IDS = (
    "sec",
    "nasdaq",
    "company_ir",
    "finnhub",
    "alpha_vantage",
    "jin10",
)


def test_news_poller_cadence_does_not_add_provider_work_latency() -> None:
    assert _remaining_poll_wait_seconds(
        100.0,
        90.0,
        clock=lambda: 147.0,
    ) == 43.0
    assert _remaining_poll_wait_seconds(
        100.0,
        90.0,
        clock=lambda: 195.0,
    ) == 0.0


def _run(awaitable: object) -> object:
    """Run a route coroutine while keeping the RED verifier's network trap active."""

    if os.environ.get("OPTIONS_COPILOT_NETWORK_DENIED") != "1":
        return asyncio.run(awaitable)  # type: ignore[arg-type]
    denied_socket = socket.socket
    bases = getattr(denied_socket, "__bases__", ())
    if not bases:
        return asyncio.run(awaitable)  # type: ignore[arg-type]
    socket.socket = bases[0]  # type: ignore[assignment,misc]
    runner = asyncio.Runner()
    try:
        runner.get_loop()
    finally:
        socket.socket = denied_socket  # type: ignore[assignment]
    try:
        return runner.run(awaitable)  # type: ignore[arg-type]
    finally:
        runner.close()


def _source_api_contract_red(detail: str) -> None:
    assert False, f"PHASE2_EXPECTED_RED:SOURCE_API_CONTRACT {detail}"


def _source_evidence_payload(runtime: NewsCoordinator) -> dict[str, object]:
    reader = getattr(runtime, "source_evidence_payload", None)
    if not callable(reader):
        _source_api_contract_red("NewsCoordinator.source_evidence_payload is missing")
    payload = reader()
    assert isinstance(payload, dict)
    return payload


def test_decision_event_payload_copies_news_and_calendar_as_one_generation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = NewsCoordinator(tmp_path / "event-generation.sqlite3", clock=lambda: NOW)
    try:
        news = {
            "news": [
                {
                    "id": "news-1",
                    "scores": {"event_impact_score": 87.75},
                    "times": {
                        "observed_at": NOW.isoformat(),
                        "published_at": (NOW - timedelta(minutes=5)).isoformat(),
                    },
                }
            ],
            "source_health": [{"source": "NASDAQ", "status": "READY"}],
            "asof": NOW.isoformat(),
        }
        calendar = {
            "calendar": [{"id": "event-1"}],
            "asof": NOW.isoformat(),
            "snapshot_hash": "a" * 64,
            "window_start": NOW.isoformat(),
            "window_end": (NOW + timedelta(days=14)).isoformat(),
        }
        with runtime._state_lock:
            runtime._news_payload = news
            runtime._calendar_payload = calendar

        def forbidden_separate_read():
            raise AssertionError("decision event payload must not perform split reads")

        monkeypatch.setattr(runtime, "news_payload", forbidden_separate_read)
        monkeypatch.setattr(runtime, "calendar_payload", forbidden_separate_read)

        payload = runtime.decision_event_payload()

        assert payload["news"] == [
            {
                "id": "news-1",
                "scores": {"event_impact_score": 87.75},
                "event_impact_score": "87.75",
                "times": {
                    "observed_at": NOW.isoformat(),
                    "published_at": (NOW - timedelta(minutes=5)).isoformat(),
                },
                "observed_at": NOW.isoformat(),
                "published_at": (NOW - timedelta(minutes=5)).isoformat(),
            }
        ]
        assert payload["calendar"] == calendar["calendar"]
        assert payload["event_generation_hash"] == canonical_hash(
            {
                "news_asof": news["asof"],
                "source_health": news["source_health"],
                "calendar_asof": calendar["asof"],
                "calendar_snapshot_hash": calendar["snapshot_hash"],
                "calendar_window_start": calendar["window_start"],
                "calendar_window_end": calendar["window_end"],
                "calendar_envelope": None,
                "calendar": calendar["calendar"],
            }
        )
    finally:
        runtime.close()


def test_equity_news_rows_preserve_verified_symbols_beyond_public_output_limit() -> None:
    rows = [
        {
            "id": f"verified-{index:04d}",
            "symbols": ["NVDA" if index == 500 else f"S{index:04d}"],
            "symbol_binding": {
                "status": "VERIFIED_PROVIDER_RELATED",
                "provider_adapter": "FINNHUB",
                "decision_authority": "SUPPORTING_ONLY",
            },
            "observed_at": (NOW - timedelta(seconds=index)).isoformat(),
            "published_at": (NOW - timedelta(minutes=5, seconds=index)).isoformat(),
            "event_impact_score": 75.0,
            "combined_opportunity_score": 60.0,
        }
        for index in range(501)
    ]
    rows.append(
        {
            "id": "unverified-nvda",
            "symbols": ["NVDA"],
            "symbol_binding": {
                "status": "SOURCE_DECLARED",
                "provider_adapter": "FINNHUB",
                "decision_authority": "SUPPORTING_ONLY",
            },
            "observed_at": NOW.isoformat(),
            "published_at": (NOW - timedelta(minutes=1)).isoformat(),
            "event_impact_score": 100.0,
            "combined_opportunity_score": 100.0,
        }
    )

    retained = _equity_news_rows(rows)

    assert len(retained) == 501
    assert any(row["id"] == "verified-0500" for row in retained)
    assert all(row["id"] != "unverified-nvda" for row in retained)


def test_decision_equity_news_payload_removes_shadow_fields(tmp_path: Path) -> None:
    runtime = NewsCoordinator(tmp_path / "equity-decision-news.sqlite3", clock=lambda: NOW)
    try:
        with runtime._state_lock:
            runtime._equity_news_payload = {
                "news": [
                    {
                        "id": "verified-nvda",
                        "symbols": ["NVDA"],
                        "symbol_binding": {
                            "status": "VERIFIED_PROVIDER_RELATED",
                            "provider_adapter": "FINNHUB",
                            "decision_authority": "SUPPORTING_ONLY",
                        },
                        "observed_at": NOW.isoformat(),
                        "published_at": (NOW - timedelta(minutes=5)).isoformat(),
                        "event_impact_score": 87.75,
                        "research_advisory": {"direction": "BULLISH"},
                        "shadow_suggested_rank": 1,
                    }
                ],
                "asof": NOW.isoformat(),
                "source_health": [{"source": "FINNHUB", "status": "READY"}],
            }

        payload = runtime.decision_equity_news_payload()
    finally:
        runtime.close()

    assert payload["news"][0]["event_impact_score"] == "87.75"
    assert "research_advisory" not in payload["news"][0]
    assert "shadow_suggested_rank" not in payload["news"][0]
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False


def test_reaction_overlay_states_are_metamorphically_invariant_for_gate_three_input(
    tmp_path: Path,
) -> None:
    runtime = NewsCoordinator(tmp_path / "reaction-gate-invariance.sqlite3", clock=lambda: NOW)
    try:
        base_row = {
            "id": "event-1",
            "event_id": "event-1",
            "source": "NASDAQ",
            "category": "EARNINGS",
            "symbols": ["SPY"],
            "event_date": NOW.date().isoformat(),
            "content_hash": "a" * 64,
            "record_hash": "b" * 64,
        }
        news = {
            "news": [{"id": "news-1"}],
            "source_health": [{"source": "NASDAQ", "status": "READY"}],
            "asof": NOW.isoformat(),
        }
        projections = []
        for status, reason in (
            ("UNAVAILABLE", "REACTION_PROVIDER_UNAVAILABLE"),
            ("WAITING", "WAITING_DECLARED_RELEASE_TIME"),
            ("AVAILABLE", None),
        ):
            calendar = {
                "calendar": [
                    {
                        **base_row,
                        "reaction": {
                            "status": status,
                            "reason": reason,
                            "mutable_supporting_hash": canonical_hash(
                                {"status": status, "reason": reason}
                            ),
                        },
                        "reaction_provider": {"status": status},
                        "reaction_decision": "NO_TRADE",
                    }
                ],
                "asof": NOW.isoformat(),
                "snapshot_hash": "c" * 64,
                "window_start": NOW.isoformat(),
                "window_end": (NOW + timedelta(days=14)).isoformat(),
            }
            with runtime._state_lock:
                runtime._news_payload = news
                runtime._calendar_payload = calendar
            payload = runtime.decision_event_payload()
            projections.append(
                (
                    payload["event_generation_hash"],
                    payload["calendar"],
                    canonical_hash(
                        {
                            "schema": "test.gate_three_ranking_action_projection.v1",
                            "news": payload["news"],
                            "calendar": payload["calendar"],
                            "event_generation_hash": payload["event_generation_hash"],
                        }
                    ),
                )
            )

        assert projections[0] == projections[1] == projections[2]
        assert projections[0][1] == [base_row]
    finally:
        runtime.close()


def test_news_payload_returns_cached_fail_closed_state_during_refresh(tmp_path: Path) -> None:
    runtime = NewsCoordinator(tmp_path / "evidence")
    lock_acquired = threading.Event()

    def hold_refresh_lock() -> None:
        with runtime._refresh_lock:
            lock_acquired.set()
            time.sleep(0.20)

    holder = threading.Thread(target=hold_refresh_lock, daemon=True)
    holder.start()
    assert lock_acquired.wait(timeout=1)

    started = time.perf_counter()
    payload = runtime.news_payload()
    elapsed = time.perf_counter() - started

    holder.join(timeout=1)
    runtime.close()
    assert elapsed < 0.10
    assert payload["option_action_pool_count"] == 0
    assert payload["option_approval_eligible"] is False


def test_close_does_not_close_news_stores_while_refresh_worker_is_blocked(
    tmp_path: Path,
    monkeypatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingProvider:
        health = "READY"
        health_reason = None

        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            entered.set()
            release.wait(timeout=2)
            return ()

    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(BlockingProvider(),),
    )
    original_join = threading.Thread.join
    runtime.start()
    assert entered.wait(timeout=1)
    worker = runtime._thread
    assert worker is not None

    def timed_out_join(thread, timeout=None):
        original_join(thread, timeout=0.01)

    monkeypatch.setattr(threading.Thread, "join", timed_out_join)
    try:
        runtime.close()
        alive_after_close = worker.is_alive()
        closed_after_timeout = runtime._closed
        try:
            evidence_store_alive = runtime.evidence_store.verify_integrity() is True
        except (RuntimeError, sqlite3.Error):
            evidence_store_alive = False
        try:
            analysis_store_alive = runtime.analysis_store.verify_integrity() is True
        except (RuntimeError, sqlite3.Error):
            analysis_store_alive = False
    finally:
        release.set()
        original_join(worker, timeout=1)
        monkeypatch.setattr(threading.Thread, "join", original_join)
        runtime.close()

    assert alive_after_close is True
    assert closed_after_timeout is False
    assert evidence_store_alive is True
    assert analysis_store_alive is True


class _Provider:
    health = "READY"
    health_reason = None

    def news(self, symbols: tuple[str, ...], *, limit: int = 50):
        assert symbols == ("AAPL", "MSFT")
        assert limit == 50
        return (
            NewsEvent(
                event_id="evt-aapl-guidance",
                symbol="AAPL",
                source="trusted_wire",
                headline="Apple raises revenue guidance after strong demand",
                summary="Management raised guidance above the prior range.",
                url="https://example.test/aapl-guidance",
                published_at=NOW - timedelta(minutes=2),
                first_seen_at=NOW - timedelta(minutes=1),
                ingested_at=NOW - timedelta(seconds=30),
                observed_at=NOW - timedelta(seconds=20),
                source_rank=1,
            ),
        )


def test_jin10_macro_news_publishes_separate_deterministic_research_proxy(
    tmp_path: Path,
) -> None:
    class Jin10MacroProvider:
        health = "READY"
        health_reason = None

        @staticmethod
        def news(symbols: tuple[str, ...], *, limit: int = 50):
            assert symbols == ("SPY", "TLT", "GLD")
            assert limit == 50
            return (
                NewsEvent(
                    event_id="evt-jin10-cpi-research-proxy",
                    symbol=None,
                    source="Jin10",
                    headline="US CPI came in below expectations",
                    summary="Core CPI also slowed versus consensus.",
                    url="https://flash.jin10.com/detail/cpi-research-proxy",
                    published_at=NOW - timedelta(minutes=2),
                    first_seen_at=NOW - timedelta(minutes=1),
                    ingested_at=NOW - timedelta(seconds=30),
                    observed_at=NOW - timedelta(seconds=20),
                    source_rank=1,
                    provider_adapter="JIN10",
                ),
            )

    runtime = NewsCoordinator(
        tmp_path / "jin10-deterministic-research-proxy.sqlite3",
        news_providers=(Jin10MacroProvider(),),
        core_symbols=("SPY", "TLT", "GLD"),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        public_row = runtime.news_payload()["news"][0]
        decision_row = runtime.decision_event_payload()["news"][0]
    finally:
        runtime.close()

    assert public_row["symbols"] == []
    assert public_row["symbol_binding"]["status"] == "UNBOUND"
    assert public_row["research_proxy_binding"]["proxy_symbol"] == "SPY"
    assert public_row["research_proxy_binding"]["binding_role"] == (
        "DETERMINISTIC_RESEARCH_PROXY"
    )
    assert public_row["research_proxy_binding"]["eligibility_effect"] == "NONE"
    assert public_row["research_proxy_binding"]["risk_effect"] == "NONE"
    assert public_row["approval_eligible"] is False
    assert public_row["instruction_creation_allowed"] is False
    assert decision_row["symbols"] == []
    assert decision_row["research_proxy_binding"] == public_row[
        "research_proxy_binding"
    ]


def test_high_frequency_news_cannot_starve_recent_macro_research_proxy(
    tmp_path: Path,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "macro-proxy-provider-fair-window.sqlite3",
        core_symbols=("SPY", "TLT", "GLD"),
        clock=lambda: NOW,
    )
    macro = NewsEvent(
        event_id="evt-jin10-cpi-before-feed-burst",
        symbol=None,
        source="Jin10",
        headline="US CPI came in below expectations",
        summary="Core CPI also slowed versus consensus.",
        url="https://flash.jin10.com/detail/cpi-before-feed-burst",
        published_at=NOW - timedelta(minutes=4),
        first_seen_at=NOW - timedelta(minutes=3),
        ingested_at=NOW - timedelta(minutes=3),
        observed_at=NOW - timedelta(minutes=3),
        source_rank=1,
        provider_adapter="JIN10",
    )
    try:
        runtime._append_news(macro)
        assert runtime.analysis_store.verify_integrity_batch(1).complete is True
        macro_group = runtime.evidence_store.query(kinds=("NEWS",), limit=1)
        assert runtime._analyze_group(macro_group, now=NOW) is not None

        for index in range(501):
            runtime._append_news(
                NewsEvent(
                    event_id=f"evt-feed-burst-{index:04d}",
                    symbol=None,
                    source="Jin10",
                    headline=f"Unrelated global market bulletin {index:04d}",
                    summary="No bounded US macro mapping applies.",
                    url=f"https://flash.jin10.com/detail/feed-burst-{index:04d}",
                    published_at=NOW - timedelta(minutes=2),
                    first_seen_at=NOW - timedelta(minutes=1),
                    ingested_at=NOW - timedelta(seconds=30),
                    observed_at=NOW - timedelta(seconds=20),
                    source_rank=1,
                    provider_adapter="JIN10",
                )
            )

        # The first bounded lookup advances past the newest 500 records.  The
        # second resolves the already-persisted macro analysis without model or
        # provider work.
        runtime._rebuild_read_model(asof=NOW, analysis_budget=0)
        runtime._rebuild_read_model(asof=NOW, analysis_budget=0)
        payload = runtime.news_payload()
    finally:
        runtime.close()

    macro_rows = [
        row
        for row in payload["news"]
        if row["id"] == "evt-jin10-cpi-before-feed-burst"
    ]
    assert payload["count"] <= 500
    assert len(macro_rows) == 1
    assert macro_rows[0]["symbols"] == []
    assert macro_rows[0]["research_proxy_binding"]["proxy_symbol"] == "SPY"


class _CalendarProvider:
    health = "READY"
    health_reason = None

    def earnings_calendar(self, start: date, end: date):
        assert start == NOW.date()
        assert end == NOW.date() + timedelta(days=14)
        return (
            EarningsEvent(
                event_id="earnings-msft",
                symbol="MSFT",
                report_date=NOW.date() + timedelta(days=2),
                hour="amc",
                eps_estimate=None,
                revenue_estimate=None,
                source="finnhub",
                first_seen_at=NOW,
                ingested_at=NOW,
                observed_at=NOW,
            ),
        )


class _FailingProvider:
    health = "DEGRADED"
    health_reason = "fixture_failure"

    def news(self, symbols: tuple[str, ...], *, limit: int = 50):
        raise TimeoutError("credential-shaped details must not escape")


def test_news_poller_restores_local_backfill_before_provider_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "startup-restore.sqlite3",
        news_providers=(_FailingProvider(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    local_restore_started = threading.Event()
    provider_refresh_started = threading.Event()
    release_remote = threading.Event()

    def restore_local() -> None:
        local_restore_started.set()

    def refresh_remote() -> None:
        assert local_restore_started.is_set()
        provider_refresh_started.set()
        release_remote.wait(timeout=1.0)

    monkeypatch.setattr(runtime, "_finish_local_analysis_restore", restore_local)
    monkeypatch.setattr(runtime, "refresh_once", refresh_remote)
    try:
        runtime.start()
        assert local_restore_started.wait(timeout=1.0)
        assert provider_refresh_started.wait(timeout=1.0)
        release_remote.set()
    finally:
        release_remote.set()
        runtime.close()


def test_local_analysis_restore_advances_multiple_progressing_batches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "multi-batch-local-restore.sqlite3",
        clock=lambda: NOW,
    )
    verification_calls: list[int] = []
    rebuild_calls: list[int] = []
    runtime._analysis_backfill_status = "PENDING"
    runtime._analysis_integrity = {
        "status": "PENDING",
        "batch_rows": 0,
        "verified_rows": 0,
        "remaining_rows": 10_000,
        "complete": False,
    }

    progresses = iter(
        (
            IntegrityVerificationProgress(
                batch_rows=5_000,
                verified_rows=5_000,
                remaining_rows=5_000,
                complete=False,
            ),
            IntegrityVerificationProgress(
                batch_rows=5_000,
                verified_rows=10_000,
                remaining_rows=0,
                complete=True,
            ),
        )
    )

    def verify(limit: int) -> IntegrityVerificationProgress:
        verification_calls.append(limit)
        return next(progresses)

    def rebuild(**_: object) -> None:
        rebuild_calls.append(len(rebuild_calls) + 1)
        runtime._analysis_backfill_status = "READY"

    monkeypatch.setattr(runtime.analysis_store, "verify_integrity_batch", verify)
    monkeypatch.setattr(runtime, "_rebuild_read_model", rebuild)
    try:
        runtime._finish_local_analysis_restore()
        assert verification_calls == [5_000, 5_000]
        assert rebuild_calls == [1]
        assert runtime._analysis_backfill_status == "READY"
        assert runtime._analysis_integrity == {
            "status": "VERIFIED",
            "batch_rows": 5_000,
            "verified_rows": 10_000,
            "remaining_rows": 0,
            "complete": True,
        }
    finally:
        runtime.close()


def test_local_analysis_restore_stops_when_a_batch_makes_no_progress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "stalled-local-restore.sqlite3",
        clock=lambda: NOW,
    )
    verification_calls: list[int] = []
    rebuild_calls: list[bool] = []
    runtime._analysis_backfill_status = "PENDING"
    runtime._analysis_integrity = {
        "status": "PENDING",
        "batch_rows": 0,
        "verified_rows": 0,
        "remaining_rows": 10_000,
        "complete": False,
    }

    def verify(limit: int) -> IntegrityVerificationProgress:
        verification_calls.append(limit)
        return IntegrityVerificationProgress(
            batch_rows=0,
            verified_rows=0,
            remaining_rows=10_000,
            complete=False,
        )

    monkeypatch.setattr(runtime.analysis_store, "verify_integrity_batch", verify)
    monkeypatch.setattr(
        runtime,
        "_rebuild_read_model",
        lambda **_: rebuild_calls.append(True),
    )
    try:
        runtime._finish_local_analysis_restore()
        assert verification_calls == [5_000]
        assert rebuild_calls == []
        assert runtime._analysis_backfill_status == "PENDING"
    finally:
        runtime.close()


def _seed_news_records(path: Path, *, count: int) -> None:
    with EvidenceStore(path, clock=lambda: NOW) as store:
        for index in range(count):
            store.append(
                EvidenceRecord(
                    identity=f"news:bounded-{index:03d}",
                    kind="NEWS",
                    symbol="AAPL",
                    provider="fixture",
                    source_id=f"bounded-{index:03d}",
                    published_at=NOW - timedelta(minutes=1),
                    first_seen_at=NOW - timedelta(seconds=30),
                    ingested_at=NOW,
                    observed_at=NOW,
                    payload={
                        "event_id": f"bounded-{index:03d}",
                        "symbol": "AAPL",
                        "source": "fixture",
                        "headline": f"Apple guidance update {index:03d}",
                        "summary": "Bounded backfill fixture.",
                        "url": f"https://example.test/bounded/{index:03d}",
                        "source_rank": 1,
                    },
                )
            )


def _conditional_leg(
    *,
    con_id: int | None = 101,
    quote_asof: datetime | None = NOW,
    dte: int | None = 17,
    gamma: Decimal | None = Decimal("0.021"),
    side: OptionLegSide | None = OptionLegSide.BUY,
    strike: Decimal | None = Decimal("225"),
) -> ConditionalOptionLeg:
    return ConditionalOptionLeg(
        underlying="AAPL",
        con_id=con_id,
        local_symbol="AAPL  260821C00225000",
        trading_class="AAPL",
        multiplier=100,
        exchange="SMART",
        expiry=date(2026, 8, 21),
        strike=strike,
        right=OptionRight.CALL,
        side=side,
        ratio=1,
        quantity=1,
        bid=Decimal("2.10"),
        ask=Decimal("2.16"),
        quote_asof=quote_asof,
        quote_batch_id="ibkr-batch-1",
        implied_volatility=Decimal("0.31"),
        delta=Decimal("0.42"),
        gamma=gamma,
        theta=Decimal("-0.08"),
        vega=Decimal("0.11"),
        volume=240,
        open_interest=1800,
        dte=dte,
    )


def _conditional_preselection(
    identifier: str,
    *,
    phase: PreselectionPhase = PreselectionPhase.PRE_MARKET,
    legs: tuple[ConditionalOptionLeg, ...] | None = None,
    maximum_loss_usd: Decimal | None = Decimal("216"),
    risk_defined: bool = True,
    estimated_cost_usd: Decimal | None = Decimal("2.40"),
    cost_after_ev_usd: Decimal | None = Decimal("48"),
) -> ConditionalOptionPreselection:
    selected_legs = legs if legs is not None else (_conditional_leg(),)
    return ConditionalOptionPreselection(
        preselection_id=identifier,
        underlying="AAPL",
        strategy_type="LONG_CALL",
        phase=phase,
        legs=selected_legs,
        risk_defined=risk_defined,
        maximum_loss_usd=maximum_loss_usd,
        estimated_cost_usd=estimated_cost_usd,
        cost_after_ev_usd=cost_after_ev_usd,
        entry_condition="Only enter at or below the displayed executable debit.",
        invalidation_condition="Cancel if the event thesis is contradicted.",
        profit_target_condition="Review at +50% of debit.",
        stop_loss_condition="Review at -35% of debit.",
        evidence_ids=("evt-aapl-guidance", "ibkr-batch-1"),
        evidence_hashes=("a" * 64, "b" * 64),
        strategy_hash=strategy_structure_hash("AAPL", "LONG_CALL", selected_legs),
        research_summary="Conditional, human-reviewed AAPL call research.",
    )


def _bind_open_economics_fixture(
    candidate: ConditionalOptionPreselection,
    *,
    scenarios: tuple[PreselectionTerminalScenario, ...],
    scenario_set: TrustedTerminalScenarioSet,
    scenario_asof: datetime,
    broker_snapshot_hash: str,
    strategy_nav_usd: Decimal,
    strategy_nav_post_hash_value: str,
    economics: OpenRepriceEconomics,
) -> ConditionalOptionPreselection:
    return replace(
        candidate,
        terminal_scenarios=scenarios,
        scenario_asof=scenario_asof,
        scenario_hash=scenario_set.scenario_hash,
        execution_cost_contract_version=EXECUTION_COST_VERSION,
        execution_cost_contract_hash=EXECUTION_COST_HASH,
        risk_policy_version=INITIAL_POLICY_VERSION,
        risk_policy_hash=INITIAL_POLICY_HASH,
        broker_snapshot_hash=broker_snapshot_hash,
        strategy_nav_usd=strategy_nav_usd,
        strategy_nav_post_hash=strategy_nav_post_hash_value,
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


def _open_economics_fixture(
    candidate: ConditionalOptionPreselection,
    *,
    scenario_set: TrustedTerminalScenarioSet,
    scenario_asof: datetime,
    broker_snapshot_hash: str,
    strategy_nav_usd: Decimal,
    strategy_nav_post_hash_value: str,
) -> OpenRepriceEconomics:
    provisional = OpenRepriceEconomics(
        candidate_id=candidate.preselection_id,
        strategy_hash=candidate.strategy_hash,
        broker_snapshot_hash=broker_snapshot_hash,
        quote_batch_id="ibkr-batch-1",
        quote_asof=NOW,
        scenario_hash=scenario_set.scenario_hash,
        scenario_asof=scenario_asof,
        cost_contract_version=EXECUTION_COST_VERSION,
        cost_contract_hash=EXECUTION_COST_HASH,
        policy_version=INITIAL_POLICY_VERSION,
        policy_hash=INITIAL_POLICY_HASH,
        strategy_nav_usd=strategy_nav_usd,
        strategy_nav_post_hash=strategy_nav_post_hash_value,
        debit_usd=Decimal("2.00"),
        credit_usd=Decimal("0"),
        commission_usd=Decimal("0.10"),
        entry_slippage_usd=Decimal("0.10"),
        exit_slippage_usd=Decimal("0.20"),
        total_slippage_usd=Decimal("0.30"),
        all_in_cost_usd=Decimal("2.40"),
        maximum_loss_usd=Decimal("216"),
        before_cost_expected_value_usd=Decimal("48.40"),
        after_cost_expected_value_usd=Decimal("48"),
        payoff_hash="d" * 64,
        risk_fraction=Decimal("0.0216"),
        economics_hash="0" * 64,
    )
    return replace(
        provisional,
        economics_hash=canonical_hash(provisional.hash_payload()),
    )


def _with_complete_open_economics(
    candidate: ConditionalOptionPreselection,
) -> ConditionalOptionPreselection:
    scenarios = (
        PreselectionTerminalScenario(Decimal("200"), Decimal("0.50")),
        PreselectionTerminalScenario(Decimal("250"), Decimal("0.50")),
    )
    scenario_asof = NOW - timedelta(seconds=1)
    scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=candidate.preselection_id,
        strategy_hash=candidate.strategy_hash,
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
    broker_snapshot_hash = "c" * 64
    strategy_nav_usd = Decimal("10000")
    nav_hash = strategy_nav_post_hash(
        candidate_id=candidate.preselection_id,
        strategy_hash=candidate.strategy_hash,
        snapshot_hash=broker_snapshot_hash,
        strategy_nav_usd=strategy_nav_usd,
    )
    economics = _open_economics_fixture(
        candidate,
        scenario_set=scenario_set,
        scenario_asof=scenario_asof,
        broker_snapshot_hash=broker_snapshot_hash,
        strategy_nav_usd=strategy_nav_usd,
        strategy_nav_post_hash_value=nav_hash,
    )
    return _bind_open_economics_fixture(
        candidate,
        scenarios=scenarios,
        scenario_set=scenario_set,
        scenario_asof=scenario_asof,
        broker_snapshot_hash=broker_snapshot_hash,
        strategy_nav_usd=strategy_nav_usd,
        strategy_nav_post_hash_value=nav_hash,
        economics=economics,
    )


class _PreselectionProvider:
    health = "READY"
    health_reason = None

    def __init__(self, rows: tuple[ConditionalOptionPreselection, ...]) -> None:
        self.rows = rows

    def preselections(self):
        return self.rows


class _AtomicPreselectionProvider:
    health = "READY"
    health_reason = None

    def __init__(
        self,
        premarket: ConditionalOptionPreselection,
        opened: ConditionalOptionPreselection,
    ) -> None:
        identifier = premarket.preselection_id
        run_id = "run-expiring"
        head_hash = "1" * 64
        row_hash = "2" * 64
        self.snapshot = SimpleNamespace(
            preselections=(premarket, opened),
            lineage={
                (identifier, "PRE_MARKET"): {
                    "source": "INDEPENDENT_TOP10_LEDGER",
                    "source_batch_purpose": "PREMARKET_ACCOUNT",
                    "source_batch_id": "premarket-account-batch",
                    "source_batch_hash": "3" * 64,
                    "preselection_id": identifier,
                    "phase": "PRE_MARKET",
                    "run_id": run_id,
                    "run_created_at": NOW.isoformat(),
                    "head_hash": head_hash,
                    "row_id": "row-expiring",
                    "row_hash": row_hash,
                    "premarket_rank": 1,
                    "production_parent_eligible": True,
                    "production_parent_blocker": None,
                },
                (identifier, "OPEN_REPRICED"): {
                    "source": "INDEPENDENT_TOP10_LEDGER",
                    "source_batch_purpose": "OPEN_REPRICE",
                    "source_batch_id": "ibkr-batch-1",
                    "source_batch_hash": "4" * 64,
                    "preselection_id": identifier,
                    "phase": "OPEN_REPRICED",
                    "run_id": run_id,
                    "run_created_at": NOW.isoformat(),
                    "head_hash": head_hash,
                    "row_id": "row-expiring",
                    "row_hash": row_hash,
                    "premarket_rank": 1,
                    "observation_id": "observation-expiring",
                    "observed_at": NOW.isoformat(),
                    "observation_hash": "5" * 64,
                    "batch_id": "ledger-open-batch",
                    "batch_head_hash": "6" * 64,
                    "scheduled_for": NOW.isoformat(),
                    "quote_batch_id": "ibkr-batch-1",
                },
            },
            coverage={
                "requested_count": 10,
                "available_count": 1,
                "source": "INDEPENDENT_TOP10_LEDGER",
                "status": "PARTIAL",
                "reason": "TOP10_PREMARKET_COVERAGE_INCOMPLETE",
                "ledger_reason": "TOP10_PREMARKET_COVERAGE_INCOMPLETE",
                "latest_run_id": run_id,
                "latest_head_hash": head_hash,
                "freeze_slot": NOW.isoformat(),
                "open_count": 1,
                "latest_open_batch_id": "ledger-open-batch",
                "latest_open_batch_head_hash": "6" * 64,
                "reprice_slot": NOW.isoformat(),
                "open_reprice_producer_status": "AVAILABLE",
                "open_reprice_writer": "IBKR_READONLY_OPEN_REPRICE_PRODUCER",
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            },
        )

    def read_snapshot(self) -> object:
        return self.snapshot


def test_refresh_persists_news_and_calendar_as_read_only_research(tmp_path: Path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(_Provider(),),
        calendar_providers=(_CalendarProvider(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        news = runtime.news_payload()
        calendar = runtime.calendar_payload()

        assert result["status"] == "READY"
        assert news["count"] == 1
        assert news["news"][0]["id"] == "evt-aapl-guidance"
        assert news["news"][0]["category"] == "GUIDANCE"
        assert 0 <= news["news"][0]["event_impact_score"] <= 100
        assert news["news"][0]["option_tradability_score"] == 0
        assert news["news"][0]["approval_eligible"] is False
        assert news["news"][0]["research_rank"] == 1
        assert news["news"][0]["action_rank"] is None
        assert news["news"][0]["research_pool"] is True
        assert news["news"][0]["action_pool"] is False
        assert news["news"][0]["analysis_completed_at"] == NOW.isoformat()
        assert news["news"][0]["published_to_first_seen_ms"] == 60_000.0
        assert news["news"][0]["first_seen_to_analysis_ms"] == 60_000.0
        assert [item["source"] for item in news["news"][0]["provenance"]] == [
            "trusted_wire"
        ]
        assert news["research_pool_count"] == 1
        assert news["action_pool_count"] == 0
        assert news["top3_count"] == 0
        assert calendar["count"] == 1
        assert calendar["calendar"][0]["symbols"] == ["MSFT"]
        assert runtime.evidence_store.verify_integrity() is True
    finally:
        runtime.close()


def test_read_model_folds_exact_story_fanout_and_merges_verified_symbols(
    tmp_path: Path,
) -> None:
    headline = "$AAPL, $MSFT, $GOOG, and $META announce a shared cloud update"

    def event(
        symbol: str,
        event_id: str,
        *,
        url: str = "https://example.test/shared?b=2&a=1",
        published_at: datetime = NOW - timedelta(minutes=2),
        provider_story_id: str | None = None,
    ) -> NewsEvent:
        return NewsEvent(
            event_id=event_id,
            symbol=symbol,
            source="Reuters",
            headline=headline,
            summary="The companies announced the same shared cloud update.",
            url=url,
            published_at=published_at,
            first_seen_at=NOW - timedelta(minutes=1),
            ingested_at=NOW,
            observed_at=NOW,
            source_rank=2,
            provenance=("FINNHUB", "Reuters"),
            provider_adapter="FINNHUB",
            provider_story_id=provider_story_id,
            symbol_binding_status="VERIFIED_PROVIDER_RELATED",
            symbol_binding_proof=SymbolBindingProof(
                schema_version=1,
                method="PROVIDER_RELATED_PLUS_ENTITY_LINK",
                provider_adapter="FINNHUB",
                requested_symbol=symbol,
                provider_symbols=(symbol,),
                corroborating_terms=(
                    symbol,
                    "METHOD=EXPLICIT_CASHTAG",
                    f"CATALOG_VERSION={ENTITY_LINK_CATALOG_VERSION}",
                    f"CATALOG_HASH={ENTITY_LINK_CATALOG_HASH.upper()}",
                ),
                verified=True,
            ),
        )

    class FanoutProvider:
        health = "READY"
        health_reason = None

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            assert limit == 50
            return (
                event("AAPL", "fanout-aapl"),
                event(
                    "MSFT",
                    "fanout-msft",
                    url="https://EXAMPLE.test/shared?a=1&b=2#fragment",
                    provider_story_id="FINNHUB:998877",
                ),
                event("GOOG", "different-url", url="https://example.test/other"),
                event(
                    "META",
                    "different-publication-time",
                    published_at=NOW - timedelta(minutes=5),
                ),
            )

    path = tmp_path / "exact-story-fold.sqlite3"
    runtime = NewsCoordinator(
        path,
        news_providers=(FanoutProvider(),),
        core_symbols=("AAPL", "MSFT", "GOOG", "META"),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.news_payload()
        shared = next(
            row for row in payload["news"] if row["merged_event_count"] == 2
        )

        assert payload["count"] == 3
        assert shared["deduplicated"] is True
        assert shared["evidence_count"] == 2
        assert shared["provider_story_id"] == "FINNHUB:998877"
        assert shared["symbols"] == ["AAPL", "MSFT"]
        assert shared["symbol_binding"] == {
            "status": "VERIFIED_PROVIDER_RELATED",
            "provider_adapter": "FINNHUB",
            "decision_authority": "SUPPORTING_ONLY",
        }
        assert len(shared["provenance"]) == 2
        with EvidenceStore(path, clock=lambda: NOW) as store:
            assert len(store.query(kinds=("NEWS",), limit=10)) == 4
    finally:
        runtime.close()


def test_calendar_uses_eastern_dates_and_filters_exact_declared_window(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 6, 1, 0, tzinfo=timezone.utc)

    class BoundaryCalendar:
        health = "READY"
        health_reason = None

        def earnings_calendar(self, start: date, end: date):
            # The UTC date is already August 6, but New York is still August 5.
            assert start == date(2026, 8, 5)
            assert end == date(2026, 8, 19)
            return (
                EarningsEvent(
                    event_id="past-same-eastern-day",
                    symbol="PAST",
                    report_date=date(2026, 8, 5),
                    hour="amc",
                    eps_estimate=None,
                    revenue_estimate=None,
                    source="nasdaq",
                    first_seen_at=now,
                    ingested_at=now,
                    observed_at=now,
                ),
                EarningsEvent(
                    event_id="inside-declared-window",
                    symbol="KEEP",
                    report_date=date(2026, 8, 6),
                    hour="bmo",
                    eps_estimate=None,
                    revenue_estimate=None,
                    source="nasdaq",
                    first_seen_at=now,
                    ingested_at=now,
                    observed_at=now,
                ),
                EarningsEvent(
                    event_id="after-declared-window",
                    symbol="LATE",
                    report_date=date(2026, 8, 20),
                    hour="bmo",
                    eps_estimate=None,
                    revenue_estimate=None,
                    source="nasdaq",
                    first_seen_at=now,
                    ingested_at=now,
                    observed_at=now,
                ),
            )

    runtime = NewsCoordinator(
        tmp_path / "calendar-boundary.sqlite3",
        calendar_providers=(BoundaryCalendar(),),
        clock=lambda: now,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()

        assert payload["window_start"] == now.isoformat()
        assert payload["window_end"] == (now + timedelta(days=14)).isoformat()
        assert [row["id"] for row in payload["calendar"]] == [
            "inside-declared-window"
        ]
        event_at = datetime.fromisoformat(payload["calendar"][0]["event_at"])
        assert now <= event_at < now + timedelta(days=14)
    finally:
        runtime.close()


def test_calendar_splits_provider_dates_across_spring_forward(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 23, 4, 30, tzinfo=timezone.utc)
    calls: list[tuple[date, date]] = []

    def event(event_id: str, symbol: str, report_date: date, hour: str) -> EarningsEvent:
        return EarningsEvent(
            event_id=event_id,
            symbol=symbol,
            report_date=report_date,
            hour=hour,
            eps_estimate=None,
            revenue_estimate=None,
            source="nasdaq",
            first_seen_at=now,
            ingested_at=now,
            observed_at=now,
        )

    candidates = (
        event("before-spring-window", "PAST", date(2026, 2, 22), "amc"),
        event("inside-spring-window", "KEEP", date(2026, 3, 8), "amc"),
        event("after-spring-window", "LATE", date(2026, 3, 9), "bmo"),
    )

    class DstCalendar:
        health = "READY"
        health_reason = None

        def earnings_calendar(self, start: date, end: date):
            calls.append((start, end))
            assert (end - start).days <= 14
            return tuple(item for item in candidates if start <= item.report_date <= end)

    runtime = NewsCoordinator(
        tmp_path / "calendar-spring-forward.sqlite3",
        calendar_providers=(DstCalendar(),),
        clock=lambda: now,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()

        assert calls == [
            (date(2026, 2, 22), date(2026, 3, 8)),
            (date(2026, 3, 9), date(2026, 3, 9)),
        ]
        assert [row["id"] for row in payload["calendar"]] == [
            "inside-spring-window"
        ]
        assert all(
            now
            <= datetime.fromisoformat(row["event_at"])
            < now + timedelta(days=14)
            for row in payload["calendar"]
        )
    finally:
        runtime.close()


def test_official_calendar_drops_persisted_events_after_window_and_restart(
    tmp_path: Path,
) -> None:
    initial = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
    current = {"now": initial}
    evidence_path = tmp_path / "official-window.sqlite3"
    source_url = "https://www.bls.gov/schedule/news_release/bls.ics"
    source = OfficialCalendarSource(
        source="Bureau of Labor Statistics",
        source_url=source_url,
        category="MACRO",
        parser=lambda payload: payload["events"],
        timezone_name="America/New_York",
    )
    provider = OfficialCalendarProvider(
        sources=(source,),
        transport=lambda _url, **_kwargs: {
            "events": [
                {
                    "id": "official-window-boundary",
                    "title": "Official release",
                    "scheduled_at": (initial + timedelta(hours=1)).isoformat(),
                    "published_at": initial.isoformat(),
                }
            ]
        },
        now=lambda: current["now"],
    )

    runtime = NewsCoordinator(
        evidence_path,
        official_calendar_provider=provider,
        clock=lambda: current["now"],
    )
    try:
        runtime.refresh_once()
        first_rows = runtime.calendar_payload()["calendar"]
        assert len(first_rows) == 1

        current["now"] = initial + timedelta(days=15)
        runtime.refresh_once()
        assert runtime.calendar_payload()["calendar"] == []
    finally:
        runtime.close()

    restarted = NewsCoordinator(evidence_path, clock=lambda: current["now"])
    try:
        assert restarted.calendar_payload()["calendar"] == []
    finally:
        restarted.close()


def test_failed_official_refresh_keeps_evidence_but_not_the_global_window(
    tmp_path: Path,
) -> None:
    initial = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
    current = {"now": initial}
    official_url = "https://www.bls.gov/schedule/news_release/bls.ics"
    provenance = OfficialEventProvenance(
        source="Bureau of Labor Statistics",
        source_url=official_url,
        source_id="last-valid-official",
        source_payload_hash="a" * 64,
        published_at=initial - timedelta(days=1),
        first_seen_at=initial,
        ingested_at=initial,
        observed_at=initial,
    )
    official_event = OfficialCalendarEvent(
        event_id="last-valid-official",
        source="Bureau of Labor Statistics",
        source_id="last-valid-official",
        source_url=official_url,
        title="Last valid official event",
        category="MACRO",
        scheduled_at=initial + timedelta(days=1),
        published_at=initial - timedelta(days=1),
        first_seen_at=initial,
        ingested_at=initial,
        observed_at=initial,
        timezone_name="America/New_York",
        schedule_precision="EXACT",
        provenance=(provenance,),
    )
    official_snapshot = OfficialCalendarSnapshot(
        status="READY",
        decision="OBSERVATION_ONLY",
        window_start=initial,
        window_end=initial + timedelta(days=14),
        observed_at=initial,
        events=(official_event,),
        sources=(
            OfficialSourceHealth(
                source="Bureau of Labor Statistics",
                source_url=official_url,
                status="READY",
                reason=None,
                observed_at=initial,
                event_count=1,
            ),
        ),
        reasons=(),
    )

    class SuccessThenFailure:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime):
            self.calls += 1
            if self.calls == 1:
                return official_snapshot
            self.health = "DEGRADED"
            self.health_reason = "OFFICIAL_SOURCE_DEGRADED"
            raise TimeoutError("redacted official provider failure")

    class LegacyTailEvent:
        health = "READY"
        health_reason = None

        def earnings_calendar(self, start: date, end: date):
            return (
                EarningsEvent(
                    event_id="legacy-current-tail",
                    symbol="TAIL",
                    report_date=date(2026, 8, 19),
                    hour="bmo",
                    eps_estimate=None,
                    revenue_estimate=None,
                    source="nasdaq",
                    first_seen_at=current["now"],
                    ingested_at=current["now"],
                    observed_at=current["now"],
                ),
            )

    official = SuccessThenFailure()
    runtime = NewsCoordinator(
        tmp_path / "official-last-valid.sqlite3",
        calendar_providers=(LegacyTailEvent(),),
        official_calendar_provider=official,
        clock=lambda: current["now"],
    )
    try:
        runtime.refresh_once()
        current["now"] = initial + timedelta(minutes=16)
        runtime.refresh_once()
        payload = runtime.calendar_payload()

        assert official.calls == 2
        assert payload["window_start"] == current["now"].isoformat()
        assert payload["window_end"] == (
            current["now"] + timedelta(days=14)
        ).isoformat()
        assert {row["id"] for row in payload["calendar"]} == {
            "last-valid-official",
            "legacy-current-tail",
        }
        assert payload["provider"]["status"] == "DEGRADED"
        assert payload["decision"] == "NO_TRADE"
    finally:
        runtime.close()


def test_source_health_keeps_success_counts_while_degraded_reasons_stay_fixed(
    tmp_path: Path,
) -> None:
    class PartialSec(_Provider):
        health = "DEGRADED"
        health_reason = "ticker_resolution_failed"

        def health_snapshot(self):
            return {
                "source": "SEC",
                "status": self.health,
                "reason": self.health_reason,
                "asof": NOW.isoformat(),
                "Authorization": "Bearer must-not-escape",
            }

    class NasdaqCalendar(_CalendarProvider):
        # A provider cannot look READY when it exposes failed dates for the
        # same observation window.
        health = "READY"
        health_reason = "NASDAQ_EARNINGS_PARTIAL_WINDOW"
        failed_dates = (date(2026, 8, 8), date(2026, 8, 9))

    runtime = NewsCoordinator(
        tmp_path / "source-health.sqlite3",
        news_providers=(PartialSec(),),
        calendar_providers=(NasdaqCalendar(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        source_health = runtime.news_payload()["source_health"]

        assert result["status"] == "DEGRADED"
        assert source_health == [
            {
                "source": "SEC",
                "source_kind": "NEWS",
                "status": "DEGRADED",
                "reason": "TICKER_RESOLUTION_FAILED",
                "success_count": 1,
                "failure_date_count": 0,
                "asof": NOW.isoformat(),
                "decision_authority": "SUPPORTING_ONLY",
            },
            {
                "source": "NASDAQ",
                "source_kind": "CALENDAR",
                "status": "DEGRADED",
                "reason": "NASDAQ_EARNINGS_PARTIAL_WINDOW",
                "success_count": 1,
                "failure_date_count": 2,
                "asof": NOW.isoformat(),
                "decision_authority": "SUPPORTING_ONLY",
            },
        ]
        assert "must-not-escape" not in repr(source_health)
    finally:
        runtime.close()


def test_verified_related_partial_parse_reason_survives_coordinator_projection(
    tmp_path: Path,
) -> None:
    class PartiallyParsedProvider(_Provider):
        health = "PARTIAL_PARSE"
        health_reason = "VERIFIED_RELATED_RECORD_REJECTED"

    runtime = NewsCoordinator(
        tmp_path / "verified-related-partial.sqlite3",
        news_providers=(PartiallyParsedProvider(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        source_health = runtime.news_payload()["source_health"]

        assert source_health[0]["status"] == "DEGRADED"
        assert source_health[0]["reason"] == "VERIFIED_RELATED_RECORD_REJECTED"
        assert source_health[0]["decision_authority"] == "SUPPORTING_ONLY"
    finally:
        runtime.close()


def test_jin10_down_health_is_canonical_and_does_not_taint_other_sources(
    tmp_path: Path,
) -> None:
    class HealthySec(_Provider):
        def health_snapshot(self):
            return {"source": "SEC", "status": "READY"}

    class Jin10Unavailable:
        health = "DOWN"
        health_reason = "authentication_failed"

        def health_snapshot(self):
            return {
                "source": "Jin10",
                "status": self.health,
                "reason": self.health_reason,
                "Authorization": "Bearer must-not-escape",
            }

        @staticmethod
        def news(_symbols: tuple[str, ...], *, limit: int = 50):
            assert limit == 50
            return ()

    class NasdaqCalendar(_CalendarProvider):
        pass

    official_snapshot = OfficialCalendarSnapshot(
        status="READY",
        decision="OBSERVATION_ONLY",
        window_start=NOW,
        window_end=NOW + timedelta(days=14),
        observed_at=NOW,
        events=(),
        sources=(
            OfficialSourceHealth(
                source="Federal Reserve",
                source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
                status="READY",
                reason=None,
                observed_at=NOW,
                event_count=0,
            ),
        ),
        reasons=(),
    )

    runtime = NewsCoordinator(
        tmp_path / "jin10-source-health.sqlite3",
        news_providers=(HealthySec(), Jin10Unavailable()),
        calendar_providers=(NasdaqCalendar(),),
        official_calendar_snapshot=official_snapshot,
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        rows = {
            item["source"]: item
            for item in runtime.news_payload()["source_health"]
        }

        assert result["approval_eligible"] is False
        assert runtime.news_payload()["instruction_creation_allowed"] is False
        assert rows["SEC"]["status"] == "READY"
        assert rows["NASDAQ"]["status"] == "READY"
        assert rows["OFFICIAL_CALENDAR"]["status"] == "READY"
        assert rows["JIN10"] == {
            "source": "JIN10",
            "source_kind": "NEWS",
            "status": "DOWN",
            "reason": "AUTHENTICATION_FAILED",
            "success_count": 0,
            "failure_date_count": 0,
            "asof": NOW.isoformat(),
            "decision_authority": "SUPPORTING_ONLY",
        }
        assert "must-not-escape" not in repr(rows)
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "reason",
    (
        "rate_limited",
        "cooldown_active",
        "authentication_failed",
        "credential_not_activated",
        "bad_json",
        "partial_tool_failure",
    ),
)
def test_jin10_source_health_reason_codes_are_fixed(reason: str) -> None:
    assert _source_reason(reason) == reason.upper()


def test_unknown_source_health_reason_is_redacted() -> None:
    assert _source_reason("Authorization: Bearer must-not-escape") == "PROVIDER_DEGRADED"


def test_source_evidence_snapshot_always_contains_exactly_six_independent_rows(
    tmp_path: Path,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "source-evidence-defaults.sqlite3",
        clock=lambda: NOW,
    )
    try:
        payload = _source_evidence_payload(runtime)
        rows = payload["sources"]
        assert isinstance(rows, list)
        assert tuple(row["source_id"] for row in rows) == SOURCE_API_SOURCE_IDS
        assert len(rows) == len(SOURCE_API_SOURCE_IDS)
        assert len({row["source_id"] for row in rows}) == len(SOURCE_API_SOURCE_IDS)
        required = {
            "source_id",
            "configured",
            "readiness",
            "status",
            "observed_at",
            "as_of",
            "last_success_at",
            "freshness_age_seconds",
            "provenance",
            "pacing",
            "reason",
            "decision_authority",
        }
        for row in rows:
            assert set(row) == required
            assert row["decision_authority"] == "SUPPORTING_ONLY"
            assert row["pacing"] == "PACING_UNVERIFIED"
        by_id = {row["source_id"]: row for row in rows}
        assert by_id["company_ir"]["configured"] is False
        assert by_id["company_ir"]["readiness"] == "UNCONFIGURED"
        assert by_id["company_ir"]["status"] == "UNCONFIGURED"
        assert by_id["company_ir"]["reason"] == "UNCONFIGURED"
        assert by_id["jin10"]["configured"] is False
        assert by_id["jin10"]["readiness"] == "NOT_CONFIGURED"
        assert by_id["jin10"]["status"] == "NOT_CONFIGURED"
        assert by_id["jin10"]["reason"] == "NOT_CONFIGURED"
    finally:
        runtime.close()


def test_source_evidence_keeps_ready_stale_limited_and_failed_rows_independent(
    tmp_path: Path,
) -> None:
    class SourceNews:
        def __init__(self, snapshot: dict[str, object]) -> None:
            self.snapshot = snapshot
            self.health = str(snapshot["readiness"])
            self.health_reason = snapshot["reason"]
            self.calls = 0

        def health_snapshot(self) -> dict[str, object]:
            return dict(self.snapshot)

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            assert limit == 50
            self.calls += 1
            return ()

    class SourceCalendar:
        health = "DEGRADED"
        health_reason = "SOURCE_STALE"

        def __init__(self) -> None:
            self.calls = 0

        @staticmethod
        def health_snapshot() -> dict[str, object]:
            return {
                "source_id": "nasdaq",
                "configured": True,
                "readiness": "READY",
                "status": "STALE",
                "observed_at": (NOW - timedelta(minutes=20)).isoformat(),
                "as_of": NOW.isoformat(),
                "last_success_at": (NOW - timedelta(minutes=20)).isoformat(),
                "freshness_age_seconds": 1200,
                "provenance": ("nasdaq-calendar-observation",),
                "pacing": "VERIFIED",
                "reason": "SOURCE_STALE",
            }

        def earnings_calendar(self, _start: date, _end: date):
            self.calls += 1
            return ()

    def source(
        source_id: str,
        *,
        readiness: str,
        status: str,
        reason: str | None,
        pacing: str,
        last_success_at: datetime | None,
    ) -> SourceNews:
        return SourceNews(
            {
                "source_id": source_id,
                "configured": True,
                "readiness": readiness,
                "status": status,
                "observed_at": NOW.isoformat(),
                "as_of": NOW.isoformat(),
                "last_success_at": (
                    None if last_success_at is None else last_success_at.isoformat()
                ),
                "freshness_age_seconds": (
                    None
                    if last_success_at is None
                    else int((NOW - last_success_at).total_seconds())
                ),
                "provenance": (f"{source_id}-observation",),
                "pacing": pacing,
                "reason": reason,
                "raw_error": "Authorization: Bearer must-not-escape",
            }
        )

    sec = source(
        "sec",
        readiness="READY",
        status="READY",
        reason=None,
        pacing="VERIFIED",
        last_success_at=NOW,
    )
    finnhub = source(
        "finnhub",
        readiness="DEGRADED",
        status="RATE_LIMITED",
        reason="RATE_LIMITED",
        pacing="RATE_LIMITED",
        last_success_at=NOW - timedelta(minutes=2),
    )
    alpha_vantage = source(
        "alpha_vantage",
        readiness="DEGRADED",
        status="FAILED",
        reason="REQUEST_FAILED",
        pacing="VERIFIED",
        last_success_at=None,
    )
    nasdaq = SourceCalendar()
    runtime = NewsCoordinator(
        tmp_path / "source-evidence-isolation.sqlite3",
        news_providers=(sec, finnhub, alpha_vantage),
        calendar_providers=(nasdaq,),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = _source_evidence_payload(runtime)
        rows = {row["source_id"]: row for row in payload["sources"]}

        assert tuple(row["source_id"] for row in payload["sources"]) == (
            SOURCE_API_SOURCE_IDS
        )
        assert rows["sec"]["readiness"] == rows["sec"]["status"] == "READY"
        assert rows["sec"]["reason"] is None
        assert rows["nasdaq"]["readiness"] == "READY"
        assert rows["nasdaq"]["status"] == "STALE"
        assert rows["nasdaq"]["reason"] == "SOURCE_STALE"
        assert rows["company_ir"]["status"] == "UNCONFIGURED"
        assert rows["company_ir"]["reason"] == "UNCONFIGURED"
        assert rows["finnhub"]["status"] == "RATE_LIMITED"
        assert rows["finnhub"]["reason"] == "RATE_LIMITED"
        assert rows["alpha_vantage"]["status"] == "FAILED"
        assert rows["alpha_vantage"]["reason"] == "REQUEST_FAILED"
        assert rows["jin10"]["status"] == "NOT_CONFIGURED"
        assert rows["jin10"]["reason"] == "NOT_CONFIGURED"
        assert rows["sec"]["freshness_age_seconds"] == 0
        assert rows["nasdaq"]["freshness_age_seconds"] == 1200
        assert rows["finnhub"]["freshness_age_seconds"] == 120
        assert rows["alpha_vantage"]["last_success_at"] is None
        assert "must-not-escape" not in repr(payload)
        assert payload["decision_authority"] == "SUPPORTING_ONLY"
        assert payload["approval_eligible"] is False
        assert payload["instruction_creation_allowed"] is False
        assert payload["order_allowed"] is False
    finally:
        runtime.close()


def test_source_evidence_does_not_let_ready_calendar_hide_failed_news_lane(
    tmp_path: Path,
) -> None:
    class FinnhubNews:
        health = "DEGRADED"
        health_reason = "PROVIDER_RELATED_ENTITY_PROOF_MISSING"

        @staticmethod
        def health_snapshot() -> dict[str, object]:
            return {
                "source_id": "finnhub",
                "configured": True,
                "readiness": "DEGRADED",
                "status": "DEGRADED",
                "observed_at": None,
                "as_of": NOW.isoformat(),
                "last_success_at": None,
                "freshness_age_seconds": None,
                "provenance": ("finnhub-news",),
                "pacing": "VERIFIED",
                "reason": "PROVIDER_RELATED_ENTITY_PROOF_MISSING",
            }

        @staticmethod
        def news(_symbols: tuple[str, ...], *, limit: int = 50):
            assert limit == 50
            return ()

    class FinnhubCalendar:
        health = "READY"
        health_reason = None

        @staticmethod
        def health_snapshot() -> dict[str, object]:
            return {
                "source_id": "finnhub",
                "configured": True,
                "readiness": "READY",
                "status": "READY",
                "observed_at": NOW.isoformat(),
                "as_of": NOW.isoformat(),
                "last_success_at": NOW.isoformat(),
                "freshness_age_seconds": 0,
                "provenance": ("finnhub-calendar",),
                "pacing": "VERIFIED",
                "reason": None,
            }

        @staticmethod
        def earnings_calendar(_start: date, _end: date):
            return ()

    runtime = NewsCoordinator(
        tmp_path / "source-evidence-duplicate-lanes.sqlite3",
        news_providers=(FinnhubNews(),),
        calendar_providers=(FinnhubCalendar(),),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        row = next(
            item
            for item in _source_evidence_payload(runtime)["sources"]
            if item["source_id"] == "finnhub"
        )

        assert row["readiness"] == "DEGRADED"
        assert row["status"] == "DEGRADED"
        assert row["reason"] == "PROVIDER_RELATED_ENTITY_PROOF_MISSING"
        assert row["last_success_at"] == NOW.isoformat()
        assert row["provenance"] == ("finnhub-news", "finnhub-calendar")
    finally:
        runtime.close()


def test_source_evidence_retains_cadence_success_time_when_lane_is_not_due(
    tmp_path: Path,
) -> None:
    current = {"now": NOW}

    class AlphaVantage:
        health = "READY"
        health_reason = None
        calls = 0

        @staticmethod
        def health_snapshot() -> dict[str, object]:
            return {
                "source_id": "alpha_vantage",
                "configured": True,
                "readiness": "READY",
                "status": "READY",
                "provenance": ("alpha-vantage",),
                "pacing": "VERIFIED",
                "reason": None,
            }

        @classmethod
        def news(cls, _symbols: tuple[str, ...], *, limit: int = 50):
            assert limit == 50
            cls.calls += 1
            return ()

    runtime = NewsCoordinator(
        tmp_path / "source-evidence-cadence.sqlite3",
        news_providers=(AlphaVantage(),),
        core_symbols=("SPY",),
        clock=lambda: current["now"],
        cadence_path=tmp_path / "source-evidence-cadence.json",
    )
    try:
        runtime.refresh_once()
        current["now"] = NOW + timedelta(minutes=5)
        runtime.refresh_once()
        row = next(
            item
            for item in _source_evidence_payload(runtime)["sources"]
            if item["source_id"] == "alpha_vantage"
        )

        assert AlphaVantage.calls == 1
        assert row["readiness"] == row["status"] == "READY"
        assert row["observed_at"] == NOW.isoformat()
        assert row["last_success_at"] == NOW.isoformat()
        assert row["freshness_age_seconds"] == 300
        assert row["reason"] is None
    finally:
        runtime.close()


def test_source_evidence_keeps_declared_unconfigured_lane_canonical(
    tmp_path: Path,
) -> None:
    class CompanyIrUnavailable:
        health = "UNAVAILABLE"
        health_reason = "PROVIDER_DEGRADED"

        @staticmethod
        def health_snapshot() -> dict[str, object]:
            return {
                "source_id": "company_ir",
                "configured": False,
                "readiness": "DEGRADED",
                "status": "UNAVAILABLE",
                "reason": "PROVIDER_DEGRADED",
            }

        @staticmethod
        def news(_symbols: tuple[str, ...], *, limit: int = 50):
            raise AssertionError("unconfigured provider must not be called")

    runtime = NewsCoordinator(
        tmp_path / "source-evidence-unconfigured.sqlite3",
        news_providers=(CompanyIrUnavailable(),),
        core_symbols=("SPY",),
        clock=lambda: NOW,
        cadence_path=tmp_path / "source-evidence-unconfigured.json",
    )
    try:
        runtime.refresh_once()
        row = next(
            item
            for item in _source_evidence_payload(runtime)["sources"]
            if item["source_id"] == "company_ir"
        )

        assert row["configured"] is False
        assert row["readiness"] == "UNCONFIGURED"
        assert row["status"] == "UNCONFIGURED"
        assert row["reason"] == "UNCONFIGURED"
        assert row["observed_at"] is None
        assert row["last_success_at"] is None
        source_health = runtime.news_payload()["source_health"]
        assert source_health == [
            {
                "source": "COMPANY_IR",
                "source_kind": "NEWS",
                "status": "UNCONFIGURED",
                "reason": "UNCONFIGURED",
                "success_count": 0,
                "failure_date_count": 0,
                "asof": NOW.isoformat(),
                "decision_authority": "SUPPORTING_ONLY",
            }
        ]
    finally:
        runtime.close()


def test_jin10_success_cycle_projects_true_freshness_and_event_provenance(
    tmp_path: Path,
) -> None:
    class Jin10Ready:
        __module__ = "options_copilot.providers.jin10"
        transport_verified = True
        health = "READY"
        health_reason = None

        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            assert symbols == ("SPY",)
            assert limit == 50
            return (
                NewsEvent(
                    event_id="jin10-source-evidence-event",
                    symbol="SPY",
                    source="Jin10",
                    headline="US CPI release update",
                    summary="Observed through the verified Jin10 MCP feed.",
                    url="https://flash.jin10.com/detail/source-evidence",
                    published_at=NOW - timedelta(minutes=1),
                    first_seen_at=NOW,
                    ingested_at=NOW,
                    observed_at=NOW,
                    source_rank=9,
                    source_id="jin10-source-evidence-source",
                    provenance=("Jin10 MCP", "list_flash"),
                ),
            )

    runtime = NewsCoordinator(
        tmp_path / "jin10-source-evidence-success.sqlite3",
        news_providers=(Jin10Ready(),),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = _source_evidence_payload(runtime)
        row = next(
            item for item in payload["sources"] if item["source_id"] == "jin10"
        )

        assert row["status"] == "READY"
        assert row["observed_at"] == NOW.isoformat()
        assert row["last_success_at"] == NOW.isoformat()
        assert row["freshness_age_seconds"] == 0
        assert row["provenance"] == ("Jin10_MCP", "list_flash")
        assert row["decision_authority"] == "SUPPORTING_ONLY"
    finally:
        runtime.close()


def test_source_evidence_reader_uses_cached_state_without_provider_work(
    tmp_path: Path,
) -> None:
    class NetworkTrap:
        health = "READY"
        health_reason = None
        calls = 0

        @classmethod
        def news(cls, _symbols: tuple[str, ...], *, limit: int = 50):
            del limit
            cls.calls += 1
            raise AssertionError("snapshot GET must not invoke a provider")

    runtime = NewsCoordinator(
        tmp_path / "source-evidence-cached.sqlite3",
        news_providers=(NetworkTrap(),),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    try:
        first = _source_evidence_payload(runtime)
        second = _source_evidence_payload(runtime)
        assert first == second
        assert NetworkTrap.calls == 0
    finally:
        runtime.close()


def test_first_calendar_refresh_rebuilds_after_provider_observation(
    tmp_path: Path,
) -> None:
    current = {"now": NOW}
    provider_observed_at = NOW + timedelta(seconds=3)

    class LaterCalendar:
        health = "DEGRADED"
        health_reason = "NASDAQ_EARNINGS_PARTIAL_WINDOW"
        failed_dates = (date(2026, 8, 8),)

        def earnings_calendar(self, start: date, end: date):
            assert start == NOW.date()
            assert end == NOW.date() + timedelta(days=14)
            current["now"] = provider_observed_at
            return (
                EarningsEvent(
                    event_id="earnings-first-refresh",
                    symbol="AAPL",
                    report_date=NOW.date() + timedelta(days=2),
                    hour="amc",
                    eps_estimate=None,
                    revenue_estimate=None,
                    source="nasdaq",
                    first_seen_at=provider_observed_at,
                    ingested_at=provider_observed_at,
                    observed_at=provider_observed_at,
                ),
            )

    runtime = NewsCoordinator(
        tmp_path / "first-calendar-refresh.sqlite3",
        calendar_providers=(LaterCalendar(),),
        clock=lambda: current["now"],
    )
    try:
        result = runtime.refresh_once()
        calendar = runtime.calendar_payload()

        assert result["status"] == "DEGRADED"
        assert result["calendar_count"] == 1
        assert result["asof"] == provider_observed_at.isoformat()
        assert calendar["count"] == 1
        assert calendar["calendar"][0]["id"] == "earnings-first-refresh"
        assert calendar["provider"]["status"] == "DEGRADED"
    finally:
        runtime.close()


def test_calendar_generation_envelope_tracks_reschedule_and_removal(
    tmp_path: Path,
) -> None:
    state = {"revision": 1}

    class NasdaqCalendar:
        health = "READY"
        health_reason = None

        def earnings_calendar(self, start: date, end: date):
            assert start == NOW.date()
            assert end == NOW.date() + timedelta(days=14)
            revision = state["revision"]
            if revision == 3:
                event_id = "replacement-event"
                symbol = "AAPL"
                report_date = NOW.date() + timedelta(days=4)
            else:
                event_id = "moving-event"
                symbol = "SPY"
                report_date = NOW.date() + timedelta(days=revision + 1)
            return (
                EarningsEvent(
                    event_id=event_id,
                    symbol=symbol,
                    report_date=report_date,
                    hour="amc",
                    eps_estimate=None,
                    revenue_estimate=None,
                    source="nasdaq",
                    first_seen_at=NOW,
                    ingested_at=NOW,
                    observed_at=NOW,
                ),
            )

    runtime = NewsCoordinator(
        tmp_path / "calendar-generation.sqlite3",
        calendar_providers=(NasdaqCalendar(),),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        first = runtime.decision_event_payload()
        first_row = next(
            row for row in first["calendar"] if row["id"] == "moving-event"
        )
        first_member_hash = first_row["calendar_generation_member_hash"]
        assert first_row["current_generation"] is True
        assert first_row["event_date"] == (NOW.date() + timedelta(days=2)).isoformat()

        state["revision"] = 2
        runtime.refresh_once()
        rescheduled = runtime.decision_event_payload()
        rescheduled_row = next(
            row
            for row in rescheduled["calendar"]
            if row["id"] == "moving-event"
        )
        assert rescheduled_row["current_generation"] is True
        assert rescheduled_row["event_date"] == (
            NOW.date() + timedelta(days=3)
        ).isoformat()
        assert rescheduled_row["calendar_generation_member_hash"] != first_member_hash

        state["revision"] = 3
        runtime.refresh_once()
        removed = runtime.decision_event_payload()
        removed_row = next(
            row for row in removed["calendar"] if row["id"] == "moving-event"
        )
        replacement_row = next(
            row
            for row in removed["calendar"]
            if row["id"] == "replacement-event"
        )
        assert removed_row["current_generation"] is False
        assert removed_row["calendar_generation_member_hash"] is None
        assert replacement_row["current_generation"] is True
        assert replacement_row["calendar_envelope_hash"] == removed[
            "calendar_envelope"
        ]["envelope_hash"]
    finally:
        runtime.close()


def test_runtime_uses_an_injected_structured_classifier(tmp_path: Path) -> None:
    classifier = StructuredLlmAdapter(
        lambda request: {
            "category": "GUIDANCE",
            "symbols": list(request["symbols"]),
            "direction": "BULLISH",
            "horizon": "DAYS_1_3",
            "confidence": "0.82",
            "counter_evidence": ["Demand could normalize"],
            "evidence_ids": list(request["evidence_ids"]),
        }
    )
    runtime = NewsCoordinator(
        tmp_path / "structured-classifier.sqlite3",
        news_providers=(_Provider(),),
        core_symbols=("AAPL", "MSFT"),
        classifier=classifier,
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.news_payload()
        row = payload["news"][0]

        assert row["classifier"] == "STRUCTURED_LLM"
        assert row["classification"]["classifier"] == "STRUCTURED_LLM"
        assert row["classification"]["confidence"] == 0.82
        assert row["approval_eligible"] is False
        assert row["instruction_creation_allowed"] is False
        assert payload["instruction_creation_allowed"] is False
        assert row["decision_authority"] == "SUPPORTING_ONLY"
    finally:
        runtime.close()


def test_restart_rebuilds_read_model_from_point_in_time_evidence(tmp_path: Path) -> None:
    path = tmp_path / "evidence.sqlite3"
    first = NewsCoordinator(
        path,
        news_providers=(_Provider(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    first.refresh_once()
    first.close()

    second = NewsCoordinator(path, core_symbols=("AAPL", "MSFT"), clock=lambda: NOW)
    try:
        assert second.news_payload()["count"] == 0
        assert second.news_payload()["analysis_backfill"]["status"] == "PENDING"
        second.refresh_once()
        assert second.news_payload()["count"] == 1
        assert second.news_payload()["provider"]["status"] == "UNCONFIGURED"
    finally:
        second.close()


def test_refresh_and_restart_reuse_frozen_analysis_without_classifier_recall(
    tmp_path: Path,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    current = [NOW]

    class CountingClassifier:
        contract_version = "fixture-contract-v1"
        model_id = "fixture-model-v1"

        def __init__(self) -> None:
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    classifier = CountingClassifier()
    path = tmp_path / "frozen-analysis-evidence.sqlite3"
    first = NewsCoordinator(
        path,
        news_providers=(_Provider(),),
        core_symbols=("AAPL", "MSFT"),
        classifier=classifier,
        clock=lambda: current[0],
    )
    try:
        first.refresh_once()
        frozen_at = first.news_payload()["news"][0]["analysis_completed_at"]
        assert frozen_at == NOW.isoformat()
        assert classifier.calls == 1

        current[0] = NOW + timedelta(minutes=1)
        first.refresh_once()
        assert classifier.calls == 1
        assert first.news_payload()["news"][0]["analysis_completed_at"] == frozen_at
        assert first.analysis_store.count == 1
    finally:
        first.close()

    current[0] = NOW + timedelta(hours=1)
    restarted = NewsCoordinator(
        path,
        core_symbols=("AAPL", "MSFT"),
        classifier=classifier,
        clock=lambda: current[0],
    )
    try:
        assert classifier.calls == 1
        assert restarted.news_payload()["count"] == 0
        restarted.refresh_once()
        row = restarted.news_payload()["news"][0]
        assert classifier.calls == 1
        assert row["analysis_completed_at"] == frozen_at
        assert row["first_seen_to_analysis_ms"] == 60_000.0
        assert restarted.analysis_store.count == 1
        assert row["decision_authority"] == "SUPPORTING_ONLY"
        assert row["approval_eligible"] is False
        assert row["instruction_creation_allowed"] is False
    finally:
        restarted.close()


def test_classifier_contract_change_appends_new_point_in_time_analysis(
    tmp_path: Path,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    current = [NOW]

    class VersionedClassifier:
        model_id = "fixture-model"

        def __init__(self, contract_version: str) -> None:
            self.contract_version = contract_version
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    path = tmp_path / "versioned-analysis-evidence.sqlite3"
    first_classifier = VersionedClassifier("v1")
    first = NewsCoordinator(
        path,
        news_providers=(_Provider(),),
        core_symbols=("AAPL", "MSFT"),
        classifier=first_classifier,
        clock=lambda: current[0],
    )
    try:
        first.refresh_once()
        assert first_classifier.calls == 1
        assert first.analysis_store.count == 1
    finally:
        first.close()

    current[0] = NOW + timedelta(minutes=5)
    second_classifier = VersionedClassifier("v2")
    second = NewsCoordinator(
        path,
        core_symbols=("AAPL", "MSFT"),
        classifier=second_classifier,
        clock=lambda: current[0],
    )
    try:
        assert second_classifier.calls == 0
        assert second.news_payload()["count"] == 0
        second.refresh_once()
        row = second.news_payload()["news"][0]
        assert second_classifier.calls == 1
        assert second.analysis_store.count == 2
        assert row["analysis_completed_at"] == current[0].isoformat()
        assert row["approval_eligible"] is False
    finally:
        second.close()


def test_constructor_defers_classification_and_refresh_backfill_is_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    class CountingClassifier:
        contract_version = "bounded-v1"
        model_id = "bounded-fixture"

        def __init__(self) -> None:
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    path = tmp_path / "bounded-evidence.sqlite3"
    _seed_news_records(path, count=11)
    classifier = CountingClassifier()
    runtime = NewsCoordinator(path, classifier=classifier, clock=lambda: NOW)
    try:
        startup = runtime.news_payload()
        assert classifier.calls == 0
        assert startup["count"] == 0
        assert startup["action_pool_count"] == 0
        assert startup["analysis_backfill"]["pending_count"] == 11

        def forbidden_full_scan() -> None:
            raise AssertionError("refresh must not rescan the permanent analysis chain")

        monkeypatch.setattr(runtime.analysis_store, "assert_integrity", forbidden_full_scan)

        runtime.refresh_once()
        first = runtime.news_payload()
        assert classifier.calls == 10
        assert first["count"] == 10
        assert first["analysis_backfill"]["status"] == "PENDING"
        assert first["analysis_backfill"]["model_batch_limit"] == 10
        assert first["analysis_backfill"]["lookup_batch_limit"] == 500
        assert first["analysis_backfill"]["pending_count"] == 1
        assert first["action_pool_count"] == 0

        runtime.refresh_once()
        completed = runtime.news_payload()
        assert classifier.calls == 11
        assert completed["count"] == 11
        assert completed["analysis_backfill"]["status"] == "READY"
    finally:
        runtime.close()


def test_restart_restores_five_hundred_persisted_hits_without_classifier_calls(
    tmp_path: Path,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    class CountingClassifier:
        contract_version = "persisted-500-v1"
        model_id = "persisted-500-fixture"

        def __init__(self) -> None:
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    path = tmp_path / "persisted-500-evidence.sqlite3"
    _seed_news_records(path, count=500)
    classifier = CountingClassifier()
    first = NewsCoordinator(path, classifier=classifier, clock=lambda: NOW)
    try:
        for _ in range(50):
            first.refresh_once()
        assert classifier.calls == 500
        assert first.news_payload()["analysis_backfill"]["status"] == "READY"
    finally:
        first.close()

    classifier.calls = 0
    restarted = NewsCoordinator(path, classifier=classifier, clock=lambda: NOW)
    try:
        assert classifier.calls == 0
        restarted.refresh_once()
        payload = restarted.news_payload()
        assert classifier.calls == 0
        assert payload["count"] == 500
        assert payload["analysis_backfill"]["status"] == "READY"
        assert payload["analysis_backfill"]["persisted_hits_this_cycle"] == 500
        assert payload["analysis_backfill"]["model_misses_this_cycle"] == 0
        assert payload["analysis_backfill"]["integrity"] == {
            "status": "VERIFIED",
            "batch_rows": 500,
            "verified_rows": 500,
            "remaining_rows": 0,
            "complete": True,
        }
    finally:
        restarted.close()


def test_incomplete_integrity_batch_never_classifies_or_exposes_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import options_copilot.news_runtime as news_runtime_module
    from options_copilot.news.classifier import DeterministicNewsClassifier

    class CountingClassifier:
        contract_version = "integrity-pending-v1"
        model_id = "integrity-pending-fixture"

        def __init__(self) -> None:
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    path = tmp_path / "integrity-pending-evidence.sqlite3"
    _seed_news_records(path, count=2)
    classifier = CountingClassifier()
    first = NewsCoordinator(path, classifier=classifier, clock=lambda: NOW)
    try:
        first.refresh_once()
        assert classifier.calls == 2
    finally:
        first.close()

    classifier.calls = 0
    monkeypatch.setattr(news_runtime_module, "_NEWS_ANALYSIS_INTEGRITY_BATCH_SIZE", 1)
    restarted = NewsCoordinator(path, classifier=classifier, clock=lambda: NOW)
    try:
        result = restarted.refresh_once()
        pending = restarted.news_payload()
        assert result["status"] == "PENDING"
        assert classifier.calls == 0
        assert pending["count"] == 0
        assert pending["action_pool_count"] == 0
        assert pending["analysis_backfill"]["reason"] == (
            "ANALYSIS_LEDGER_INTEGRITY_PENDING"
        )
        assert pending["analysis_backfill"]["integrity"] == {
            "status": "PENDING",
            "batch_rows": 1,
            "verified_rows": 1,
            "remaining_rows": 1,
            "complete": False,
        }

        restarted.refresh_once()
        recovered = restarted.news_payload()
        assert classifier.calls == 0
        assert recovered["count"] == 2
        assert recovered["analysis_backfill"]["status"] == "READY"
        assert recovered["analysis_backfill"]["persisted_hits_this_cycle"] == 2
    finally:
        restarted.close()


def test_background_poller_finishes_local_integrity_restore_without_waiting_for_next_provider_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import options_copilot.news_runtime as news_runtime_module

    path = tmp_path / "background-integrity-restore.sqlite3"
    _seed_news_records(path, count=2)
    first = NewsCoordinator(path, clock=lambda: NOW)
    try:
        first.refresh_once()
        assert first.news_payload()["count"] == 2
    finally:
        first.close()

    provider_called = threading.Event()

    class CountingProvider(_Provider):
        calls = 0

        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            self.calls += 1
            provider_called.set()
            return super().news(symbols, limit=limit)

    provider = CountingProvider()
    monkeypatch.setattr(news_runtime_module, "_NEWS_ANALYSIS_INTEGRITY_BATCH_SIZE", 1)
    restarted = NewsCoordinator(
        path,
        news_providers=(provider,),
        core_symbols=("AAPL", "MSFT"),
        poll_interval_seconds=60,
        clock=lambda: NOW,
    )
    try:
        restarted.start()
        assert provider_called.wait(timeout=1)
        deadline = time.monotonic() + 1
        payload = restarted.news_payload()
        while (
            payload["analysis_backfill"]["integrity"]["complete"] is not True
            or payload["count"] != 3
        ):
            if time.monotonic() >= deadline:
                break
            time.sleep(0.01)
            payload = restarted.news_payload()

        assert provider.calls == 1
        assert payload["analysis_backfill"]["integrity"]["complete"] is True
        assert payload["count"] == 3
    finally:
        restarted.close()


def test_startup_local_restore_precedes_blocking_provider_acquisition(
    tmp_path: Path,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    class VersionedClassifier:
        model_id = "startup-provider-order-fixture"

        def __init__(self, version: str) -> None:
            self.contract_version = version
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    path = tmp_path / "startup-provider-order.sqlite3"
    _seed_news_records(path, count=2)
    original = VersionedClassifier("startup-provider-order-v1")
    first = NewsCoordinator(path, classifier=original, clock=lambda: NOW)
    try:
        first.refresh_once()
        assert first.news_payload()["analysis_backfill"]["status"] == "READY"
    finally:
        first.close()

    provider_entered = threading.Event()
    release_provider = threading.Event()

    class BlockingProvider(_Provider):
        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            provider_entered.set()
            release_provider.wait(timeout=2)
            return super().news(symbols, limit=limit)

    upgraded = VersionedClassifier("startup-provider-order-v2")
    restarted = NewsCoordinator(
        path,
        classifier=upgraded,
        news_providers=(BlockingProvider(),),
        core_symbols=("AAPL", "MSFT"),
        poll_interval_seconds=60,
        clock=lambda: NOW,
    )
    try:
        restarted.start()
        assert provider_entered.wait(timeout=1)
        payload = restarted.news_payload()

        assert upgraded.calls == 2
        assert payload["analysis_backfill"]["status"] == "READY"
        assert payload["analysis_backfill"]["pending_count"] == 0
        assert payload["count"] == 2
    finally:
        release_provider.set()
        restarted.close()


def test_startup_local_restore_crosses_lookup_batches_before_provider_acquisition(
    tmp_path: Path,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    class CountingClassifier:
        model_id = "startup-multibatch-fixture"
        contract_version = "startup-multibatch-v1"

        def __init__(self) -> None:
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    path = tmp_path / "startup-multibatch.sqlite3"
    # Exceed the persisted-analysis lookup batch so local restart work must
    # advance more than once before the first network provider call.
    _seed_news_records(path, count=525)
    provider_entered = threading.Event()
    release_provider = threading.Event()

    class BlockingProvider(_Provider):
        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            provider_entered.set()
            release_provider.wait(timeout=5)
            return super().news(symbols, limit=limit)

    classifier = CountingClassifier()
    runtime = NewsCoordinator(
        path,
        classifier=classifier,
        news_providers=(BlockingProvider(),),
        core_symbols=("AAPL", "MSFT"),
        poll_interval_seconds=60,
        clock=lambda: NOW,
    )
    try:
        runtime.start()
        assert provider_entered.wait(timeout=5)
        payload = runtime.news_payload()

        assert classifier.calls == 525
        assert payload["analysis_backfill"]["status"] == "READY"
        assert payload["analysis_backfill"]["pending_count"] == 0
        assert payload["count"] == 500
    finally:
        release_provider.set()
        runtime.close()


def test_background_poller_finishes_local_classifier_backfill_without_provider_wait(
    tmp_path: Path,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    last_classification_entered = threading.Event()
    release_last_classification = threading.Event()

    class VersionedClassifier:
        model_id = "startup-contract-backfill-fixture"

        def __init__(self, version: str) -> None:
            self.contract_version = version
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            if self.contract_version == "startup-contract-v2" and self.calls == 26:
                last_classification_entered.set()
                assert release_last_classification.wait(timeout=2), "publication barrier was not released"
            return self._delegate.classify(news)

    path = tmp_path / "background-contract-backfill.sqlite3"
    _seed_news_records(path, count=25)
    original = VersionedClassifier("startup-contract-v1")
    first = NewsCoordinator(path, classifier=original, clock=lambda: NOW)
    try:
        for _ in range(3):
            first.refresh_once()
        assert original.calls == 25
        assert first.news_payload()["analysis_backfill"]["status"] == "READY"
    finally:
        first.close()

    provider_called = threading.Event()

    class CountingProvider(_Provider):
        calls = 0

        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            self.calls += 1
            provider_called.set()
            return super().news(symbols, limit=limit)

    provider = CountingProvider()
    upgraded = VersionedClassifier("startup-contract-v2")
    restarted = NewsCoordinator(
        path,
        classifier=upgraded,
        news_providers=(provider,),
        core_symbols=("AAPL", "MSFT"),
        poll_interval_seconds=60,
        clock=lambda: NOW,
    )
    try:
        restarted.start()
        assert provider_called.wait(timeout=1)
        assert last_classification_entered.wait(timeout=1)
        cached = restarted.news_payload()
        # Entering classify() is not a published generation. During the last
        # classification the complete local-restore snapshot remains visible,
        # with all action authority cleared by the concurrent-refresh reader.
        assert upgraded.calls == 26
        assert cached["count"] == len(cached["news"]) == 25
        assert cached["analysis_backfill"]["status"] == "READY"
        assert cached["analysis_backfill"]["pending_count"] == 0
        assert cached["action_pool_count"] == 0
        assert cached["option_action_pool"] == []
        assert cached["approval_eligible"] is False
        assert cached["option_approval_eligible"] is False
        assert cached["instruction_creation_allowed"] is False
        assert cached["order_creation_allowed"] is False
        assert all(row["action_pool_eligible"] is False for row in cached["news"])
        release_last_classification.set()
        deadline = time.monotonic() + 2
        payload = restarted.news_payload()
        while (
            payload["count"] != 26
            or payload["analysis_backfill"]["status"] != "READY"
            or payload["analysis_backfill"]["pending_count"] != 0
        ):
            if time.monotonic() >= deadline:
                break
            time.sleep(0.01)
            payload = restarted.news_payload()

        assert provider.calls == 1
        assert upgraded.calls == 26
        assert payload["analysis_backfill"]["status"] == "READY"
        assert payload["analysis_backfill"]["pending_count"] == 0
        assert payload["analysis_backfill"]["local_restore_batch_limit"] == 5_000
        assert payload["count"] == len(payload["news"]) == 26
    finally:
        release_last_classification.set()
        restarted.close()


def test_continuous_new_news_does_not_starve_existing_model_backlog(
    tmp_path: Path,
) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    class CountingClassifier:
        contract_version = "fair-backlog-v1"
        model_id = "fair-backlog-fixture"

        def __init__(self) -> None:
            self.calls = 0
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            return self._delegate.classify(news)

    path = tmp_path / "fair-backlog-evidence.sqlite3"
    _seed_news_records(path, count=11)
    classifier = CountingClassifier()
    runtime = NewsCoordinator(path, classifier=classifier, clock=lambda: NOW)
    try:
        runtime.refresh_once()
        assert classifier.calls == 10
        assert runtime.news_payload()["analysis_backfill"]["pending_count"] == 1

        _seed_news_records(path, count=21)
        runtime.refresh_once()
        assert classifier.calls == 20
        assert "bounded-000" in {
            row["id"] for row in runtime.news_payload()["news"]
        }
    finally:
        runtime.close()


def test_calendar_volume_cannot_evict_news_or_earlier_current_calendar(
    tmp_path: Path,
) -> None:
    path = tmp_path / "kind-isolated-evidence.sqlite3"

    def calendar_record(index: int, *, event_id: str) -> EvidenceRecord:
        return EvidenceRecord(
            identity=f"calendar:{event_id}",
            kind="CALENDAR",
            symbol="MSFT",
            provider="fixture-calendar",
            source_id=event_id,
            published_at=NOW - timedelta(minutes=2),
            first_seen_at=NOW - timedelta(minutes=1),
            ingested_at=NOW - timedelta(seconds=30),
            observed_at=NOW,
            payload={
                "event_id": event_id,
                "symbol": "MSFT",
                "report_date": (NOW + timedelta(days=1 + index % 2)).date().isoformat(),
                "hour": "amc",
                "source": "fixture-calendar",
            },
        )

    with EvidenceStore(path, clock=lambda: NOW) as store:
        store.append(calendar_record(0, event_id="early-current-event"))
    _seed_news_records(path, count=1)
    with EvidenceStore(path, clock=lambda: NOW) as store:
        for index in range(501):
            store.append(calendar_record(index, event_id=f"later-{index:03d}"))

    runtime = NewsCoordinator(path, clock=lambda: NOW)
    try:
        startup = runtime.news_payload()
        calendar_ids = {
            row["event_id"] for row in runtime.calendar_payload()["calendar"]
        }
        assert startup["analysis_backfill"]["pending_count"] == 1
        assert "early-current-event" in calendar_ids
        assert len(calendar_ids) == 502

        runtime.refresh_once()
        assert runtime.news_payload()["count"] == 1
        assert runtime.news_payload()["analysis_backfill"]["status"] == "READY"
        assert "early-current-event" in {
            row["event_id"] for row in runtime.calendar_payload()["calendar"]
        }
    finally:
        runtime.close()


def test_analysis_failure_is_redacted_fail_closed_and_retryable(tmp_path: Path) -> None:
    from options_copilot.news.classifier import DeterministicNewsClassifier

    class FailingOnceClassifier:
        contract_version = "bounded-failure-v1"
        model_id = "bounded-failure-fixture"

        def __init__(self) -> None:
            self.calls = 0
            self.fail = True
            self._delegate = DeterministicNewsClassifier()

        def classify(self, news):
            self.calls += 1
            if self.fail:
                self.fail = False
                raise RuntimeError("credential-shaped classifier detail")
            return self._delegate.classify(news)

    path = tmp_path / "bounded-failure-evidence.sqlite3"
    _seed_news_records(path, count=11)
    classifier = FailingOnceClassifier()
    runtime = NewsCoordinator(path, classifier=classifier, clock=lambda: NOW)
    try:
        result = runtime.refresh_once()
        failed = runtime.news_payload()
        assert result["status"] == "DEGRADED"
        assert classifier.calls == 10
        assert failed["analysis_backfill"]["status"] == "DEGRADED"
        assert failed["analysis_backfill"]["failed_count"] == 1
        assert failed["action_pool_count"] == 0
        assert "credential" not in str(failed).lower()

        runtime.refresh_once()
        recovered = runtime.news_payload()
        assert classifier.calls == 12
        assert recovered["analysis_backfill"]["status"] == "READY"
        assert recovered["analysis_backfill"]["failed_count"] == 0
        assert recovered["count"] == 11
    finally:
        runtime.close()


def test_structured_provenance_survives_runtime_to_api_projection(
    tmp_path: Path,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "provenance-api.sqlite3",
        news_providers=(_Provider(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        app = create_app(
            OptionsCopilotServices(
                health_provider=lambda: {},
                bootstrap_provider=lambda: {},
                candidates_provider=lambda: (),
                positions_provider=lambda: (),
                learning_provider=lambda: {},
                news_provider=runtime.news_payload,
            )
        )
        endpoint = next(
            route.endpoint
            for route in app.routes
            if getattr(route, "path", None) == "/api/news"
        )

        payload = _run(endpoint())
        provenance = payload["news"][0]["provenance"]

        assert len(provenance) == 1
        assert provenance[0]["source"] == "trusted_wire"
        assert provenance[0]["source_rank"] == 1
        assert provenance[0]["published_at"] == (
            NOW - timedelta(minutes=2)
        ).isoformat()
        assert provenance[0]["first_seen_at"] == (
            NOW - timedelta(minutes=1)
        ).isoformat()
        assert provenance[0]["observed_at"] == (
            NOW - timedelta(seconds=20)
        ).isoformat()
        assert len(provenance[0]["content_hash"]) == 64
        assert provenance[0]["decision_authority"] == "SUPPORTING_ONLY"
    finally:
        runtime.close()


def test_provider_failure_is_redacted_and_fails_closed(tmp_path: Path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(_FailingProvider(),),
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        payload = runtime.news_payload()

        assert result["status"] == "DEGRADED"
        assert payload["count"] == 0
        assert payload["action_pool_count"] == 0
        assert "credential" not in str(payload).lower()
        assert payload["provider"]["message"] == "one or more providers are degraded"
    finally:
        runtime.close()


def test_legacy_finnhub_news_is_unbound_until_versioned_binding_proof_is_persisted(
    tmp_path: Path,
) -> None:
    legacy = NewsEvent(
        event_id=f"evt_{'a' * 32}",
        symbol="NVDA",
        source="Yahoo",
        headline="Atmus Filtration reports quarterly earnings",
        summary="This item is not about Nvidia.",
        url="https://example.test/atmus",
        published_at=NOW - timedelta(minutes=2),
        first_seen_at=NOW - timedelta(minutes=1),
        ingested_at=NOW,
        observed_at=NOW,
        source_rank=2,
    )

    class LegacyFinnhub:
        health = "READY"
        health_reason = None

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            assert limit == 50
            return (legacy,)

    path = tmp_path / "legacy-finnhub-binding.sqlite3"
    first = NewsCoordinator(
        path,
        news_providers=(LegacyFinnhub(),),
        core_symbols=("NVDA",),
        clock=lambda: NOW,
    )
    try:
        first.refresh_once()
        row = first.news_payload()["news"][0]

        assert row["symbols"] == []
        assert row["classification"]["symbols"] == []
        assert row["symbol_binding"] == {
            "status": "UNVERIFIED_LEGACY_PROVIDER",
            "provider_adapter": None,
            "decision_authority": "SUPPORTING_ONLY",
        }
        assert row["watch_rank"] is None
        assert row["action_rank"] is None
        assert row["ibkr_provenance"] is None
    finally:
        first.close()

    verified = replace(
        legacy,
        provider_adapter="FINNHUB",
        symbol_binding_status="VERIFIED_PROVIDER_RELATED",
        symbol_binding_proof=SymbolBindingProof(
            schema_version=1,
            method="PROVIDER_RELATED_PLUS_ENTITY_LINK",
            provider_adapter="FINNHUB",
            requested_symbol="NVDA",
            provider_symbols=("NVDA",),
            corroborating_terms=(
                "NVDA",
                "METHOD=CONTROLLED_ALIAS",
                f"CATALOG_VERSION={ENTITY_LINK_CATALOG_VERSION}",
                f"CATALOG_HASH={ENTITY_LINK_CATALOG_HASH.upper()}",
            ),
            verified=True,
        ),
        provenance=("FINNHUB", "Yahoo"),
    )

    class VerifiedFinnhub:
        health = "READY"
        health_reason = None

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            assert limit == 50
            return (verified,)

    second = NewsCoordinator(
        path,
        news_providers=(VerifiedFinnhub(),),
        core_symbols=("NVDA",),
        clock=lambda: NOW,
    )
    try:
        second.refresh_once()
        row = second.news_payload()["news"][0]

        assert row["symbols"] == ["NVDA"]
        assert row["symbol_binding"]["status"] == "VERIFIED_PROVIDER_RELATED"
        assert row["symbol_binding"]["provider_adapter"] == "FINNHUB"
        with EvidenceStore(path, clock=lambda: NOW) as store:
            assert len(store.query(kinds=("NEWS",), limit=10)) == 2
    finally:
        second.close()


def test_legacy_verified_binding_marker_without_proof_fails_closed() -> None:
    assert _verified_symbol_binding_proof(
        None,
        symbol="AMZN",
        provider_adapter="FINNHUB",
    ) is False
    assert _verified_symbol_binding_proof(
        {
            "schema_version": 1,
            "method": "PROVIDER_RELATED_PLUS_ENTITY_LINK",
            "provider_adapter": "FINNHUB",
            "requested_symbol": "AMZN",
            "provider_symbols": ["AMZN"],
            "corroborating_terms": [
                "AMZN",
                "METHOD=CONTROLLED_ALIAS",
                f"CATALOG_VERSION={ENTITY_LINK_CATALOG_VERSION}",
                "CATALOG_HASH=" + "0" * 64,
            ],
            "verified": True,
        },
        symbol="AMZN",
        provider_adapter="FINNHUB",
    ) is False


def test_conflicting_identity_remains_visible_and_never_enters_action_pool(tmp_path: Path) -> None:
    class Conflicts:
        health = "READY"
        health_reason = None

        def news(self, symbols: tuple[str, ...], *, limit: int = 50):
            base = dict(
                event_id="evt-conflict",
                symbol="AAPL",
                source="source_a",
                headline="Apple guidance update",
                summary="first version",
                url="https://example.test/one",
                published_at=NOW - timedelta(minutes=2),
                first_seen_at=NOW - timedelta(minutes=1),
                ingested_at=NOW,
                observed_at=NOW,
            )
            return (
                NewsEvent(**base),
                NewsEvent(**{**base, "source": "source_b", "summary": "conflicting version"}),
            )

    runtime = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(Conflicts(),),
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.news_payload()
        assert payload["count"] == 1
        assert payload["news"][0]["status"] == "CONFLICTED"
        assert payload["action_pool_count"] == 0
    finally:
        runtime.close()


def test_evidence_limit_keeps_newest_news_visible_in_chain_order(tmp_path: Path) -> None:
    path = tmp_path / "latest-evidence.sqlite3"
    with EvidenceStore(path, clock=lambda: NOW) as store:
        for index in range(3):
            store.append(
                EvidenceRecord(
                    identity=f"news-{index}",
                    kind="NEWS",
                    symbol="AAPL",
                    provider="fixture",
                    source_id=f"source-{index}",
                    published_at=NOW,
                    first_seen_at=NOW,
                    ingested_at=NOW,
                    observed_at=NOW,
                    payload={"index": index},
                )
            )

        newest = store.query(limit=2)

    assert [item.sequence for item in newest] == [2, 3]
    assert [item.record.payload["index"] for item in newest] == [1, 2]


def test_startup_rejects_tampered_evidence_before_rebuild(tmp_path: Path) -> None:
    path = tmp_path / "tampered.sqlite3"
    first = NewsCoordinator(
        path,
        news_providers=(_Provider(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    first.refresh_once()
    first.close()

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER evidence_records_no_update")
        connection.execute(
            "UPDATE evidence_records SET row_hash=? WHERE sequence=1",
            ("0" * 64,),
        )

    with pytest.raises(EvidenceStoreCorruption, match="row hash mismatch"):
        NewsCoordinator(path, core_symbols=("AAPL",), clock=lambda: NOW)


class _OfficialNewsProvider:
    health = "READY"
    health_reason = None

    def news(self, symbols: tuple[str, ...], *, limit: int = 50):
        del symbols, limit
        return (
            NewsEvent(
                event_id="official-aapl-guidance",
                symbol="AAPL",
                source="Company IR",
                headline="Apple raises full-year guidance after strong demand",
                summary="Management raised its published guidance range.",
                url="https://investor.example.test/aapl-guidance",
                published_at=NOW - timedelta(minutes=2),
                first_seen_at=NOW - timedelta(minutes=1),
                ingested_at=NOW - timedelta(seconds=30),
                observed_at=NOW - timedelta(seconds=20),
                source_rank=1,
                provenance=("Company IR",),
            ),
        )


class _BindingProvider:
    health = "READY"
    health_reason = None

    def __init__(self, observed_at: datetime) -> None:
        self.observed_at = observed_at

    def bindings(self, symbols: tuple[str, ...]):
        assert symbols == ("AAPL",)
        snapshot_id = "ibkr-quotes-aapl-1"
        return (
            IbkrNewsBinding(
                symbol="AAPL",
                quote_snapshot_id=snapshot_id,
                tradability=OptionTradabilityInput(
                    symbol="AAPL",
                    source="IBKR",
                    observed_at=self.observed_at,
                    bid=Decimal("1.00"),
                    ask=Decimal("1.04"),
                    volume=250,
                    open_interest=1000,
                ),
                confirmation=MarketConfirmation(
                    source="IBKR",
                    observed_at=self.observed_at,
                    direction=ImpactDirection.BULLISH,
                    evidence_ids=(snapshot_id,),
                ),
            ),
        )


def test_fresh_exact_ibkr_binding_populates_research_and_action_ranks(tmp_path: Path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "fresh-binding.sqlite3",
        news_providers=(_OfficialNewsProvider(),),
        ibkr_binding_provider=_BindingProvider(NOW),
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.news_payload()
        row = payload["news"][0]

        assert payload["research_pool_count"] == 1
        assert payload["action_pool_count"] == payload["top3_count"] == 1
        assert row["research_rank"] == row["action_rank"] == 1
        assert row["rank_one"] is True
        assert row["option_tradability_score"] > 0
        assert row["combined_opportunity_score"] > 0
        assert [item["source"] for item in row["provenance"]] == ["Company IR"]
        assert row["ibkr_provenance"]["quote_snapshot_id"] == "ibkr-quotes-aapl-1"
        assert row["decision_authority"] == "SUPPORTING_ONLY"
        assert row["approval_eligible"] is False
    finally:
        runtime.close()


def test_stale_ibkr_binding_forces_action_pool_and_top3_to_zero(tmp_path: Path) -> None:
    runtime = NewsCoordinator(
        tmp_path / "stale-binding.sqlite3",
        news_providers=(_OfficialNewsProvider(),),
        ibkr_binding_provider=_BindingProvider(NOW - timedelta(minutes=6)),
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.news_payload()
        row = payload["news"][0]

        assert payload["action_pool_count"] == payload["top3_count"] == 0
        assert row["action_rank"] is None
        assert row["option_tradability_score"] == 0
        assert row["action_pool"] is False
    finally:
        runtime.close()


def test_degraded_ibkr_binding_provider_forces_action_pool_and_top3_to_zero(
    tmp_path: Path,
) -> None:
    class DegradedBindingProvider(_BindingProvider):
        health = "DEGRADED"
        health_reason = "partial_snapshot"

    runtime = NewsCoordinator(
        tmp_path / "degraded-binding.sqlite3",
        news_providers=(_OfficialNewsProvider(),),
        ibkr_binding_provider=DegradedBindingProvider(NOW),
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        payload = runtime.news_payload()
        row = payload["news"][0]

        assert result["status"] == "DEGRADED"
        assert payload["action_pool_count"] == payload["top3_count"] == 0
        assert row["action_rank"] is None
        assert row["action_pool"] is False
        assert row["option_tradability_score"] == 0
        assert row["ibkr_provenance"] is None
    finally:
        runtime.close()


def test_cached_action_pool_expires_without_waiting_for_next_provider_poll(tmp_path: Path) -> None:
    current = [NOW]
    runtime = NewsCoordinator(
        tmp_path / "expiring-binding.sqlite3",
        news_providers=(_OfficialNewsProvider(),),
        ibkr_binding_provider=_BindingProvider(NOW),
        core_symbols=("AAPL",),
        clock=lambda: current[0],
    )
    try:
        runtime.refresh_once()
        assert runtime.news_payload()["top3_count"] == 1

        current[0] = NOW + timedelta(seconds=6)
        expired = runtime.news_payload()

        assert expired["action_pool_count"] == expired["top3_count"] == 0
        assert expired["news"][0]["option_tradability_score"] == 0
        assert expired["news"][0]["action_rank"] is None
    finally:
        runtime.close()


def test_unverified_jin10_transport_remains_disabled_without_network_call(tmp_path: Path) -> None:
    calls: list[str] = []

    class Secrets:
        @staticmethod
        def get(_name: str) -> str:
            return "fixture-only-secret"

    provider = Jin10EventProvider(
        Secrets(),
        transport=lambda url, **_kwargs: calls.append(url) or {"data": []},
        now=lambda: NOW,
    )
    runtime = NewsCoordinator(
        tmp_path / "jin10-disabled.sqlite3",
        news_providers=(provider,),
        core_symbols=("AAPL",),
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        assert calls == []
        assert result["status"] == "DEGRADED"
        assert runtime.news_payload()["action_pool_count"] == 0
    finally:
        runtime.close()


def test_conditional_option_preselections_bind_contract_quotes_and_cap_premarket_to_ten(
    tmp_path: Path,
) -> None:
    rows = tuple(
        _conditional_preselection(
            f"pre-{index}",
            cost_after_ev_usd=Decimal(index),
        )
        for index in range(1, 12)
    )
    runtime = NewsCoordinator(
        tmp_path / "conditional-preselection.sqlite3",
        news_providers=(_Provider(),),
        preselection_provider=_PreselectionProvider(rows),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.news_payload()

        assert payload["pre_market_preselection_count"] == 10
        assert payload["open_market_repriced_count"] == 0
        assert payload["option_action_pool_count"] == 0
        assert len(payload["pre_market_preselections"]) == 10
        assert payload["pre_market_preselections"][0]["preselection_id"] == "pre-11"
        assert payload["pre_market_preselections"][-1]["preselection_id"] == "pre-2"

        first = payload["pre_market_preselections"][0]
        assert first["phase"] == "PRE_MARKET"
        assert first["decision_authority"] == "SUPPORTING_ONLY"
        assert first["approval_eligible"] is False
        assert first["instruction_creation_allowed"] is False
        assert first["action_pool_eligible"] is False
        assert first["research_only"] is True
        assert first["blockers"] == ["PREMARKET_RESEARCH_ONLY"]
        assert first["strategy_hash"] == rows[-1].strategy_hash
        assert first["evidence_hashes"] == ["a" * 64, "b" * 64]
        assert first["legs"] == [
            {
                "underlying": "AAPL",
                "con_id": 101,
                "local_symbol": "AAPL  260821C00225000",
                "trading_class": "AAPL",
                "multiplier": 100,
                "exchange": "SMART",
                "expiry": "2026-08-21",
                "strike": "225",
                "right": "CALL",
                "side": "BUY",
                "ratio": 1,
                "quantity": 1,
                "bid": "2.10",
                "ask": "2.16",
                "quote_asof": NOW.isoformat(),
                "quote_batch_id": "ibkr-batch-1",
                "implied_volatility": "0.31",
                "delta": "0.42",
                "gamma": "0.021",
                "theta": "-0.08",
                "vega": "0.11",
                "volume": 240,
                "open_interest": 1800,
                "dte": 17,
            }
        ]
        assert len(payload["news"][0]["related_options"]) == 10
        assert payload["news"][0]["related_options"][0]["preselection_id"] == "pre-11"
    finally:
        runtime.close()


def test_open_reprice_keeps_research_but_action_pool_fails_closed_on_every_hard_field(
    tmp_path: Path,
) -> None:
    good = _with_complete_open_economics(
        _conditional_preselection(
            "open-good",
            phase=PreselectionPhase.OPEN_REPRICED,
        )
    )
    stale = _conditional_preselection(
        "open-stale",
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=(_conditional_leg(quote_asof=NOW - timedelta(seconds=6)),),
    )
    missing_greek = _conditional_preselection(
        "open-missing-greek",
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=(_conditional_leg(gamma=None),),
    )
    unknown_loss = _conditional_preselection(
        "open-unknown-loss",
        phase=PreselectionPhase.OPEN_REPRICED,
        maximum_loss_usd=None,
    )
    unlimited = _conditional_preselection(
        "open-unlimited",
        phase=PreselectionPhase.OPEN_REPRICED,
        risk_defined=False,
        maximum_loss_usd=None,
    )
    below_floor = _conditional_preselection(
        "open-six-dte",
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=(_conditional_leg(dte=6),),
    )
    naked_short = _conditional_preselection(
        "open-naked-short",
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=(_conditional_leg(side=OptionLegSide.SELL),),
    )
    runtime = NewsCoordinator(
        tmp_path / "conditional-reprice.sqlite3",
        news_providers=(_Provider(),),
        preselection_provider=_PreselectionProvider(
            (good, stale, missing_greek, unknown_loss, unlimited, below_floor, naked_short)
        ),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.news_payload()
        repriced = {
            item["preselection_id"]: item for item in payload["open_market_repriced"]
        }

        assert payload["open_market_repriced_count"] == 7
        assert payload["option_action_pool_count"] == 0
        assert payload["option_action_pool"] == []
        assert repriced["open-good"]["action_pool_eligible"] is False
        assert repriced["open-good"]["research_only"] is True
        assert "SOURCE_LINEAGE_MISSING_LEGACY" in repriced["open-good"][
            "blockers"
        ]
        assert "QUOTE_STALE" in repriced["open-stale"]["blockers"]
        assert any(
            reason.startswith("LEG_EXECUTION_FIELD_MISSING:1:gamma")
            for reason in repriced["open-missing-greek"]["blockers"]
        )
        assert "MAX_LOSS_UNKNOWN" in repriced["open-unknown-loss"]["blockers"]
        assert "UNLIMITED_OR_NAKED_RISK" in repriced["open-unlimited"]["blockers"]
        assert "DTE_BELOW_PERMANENT_FLOOR" in repriced["open-six-dte"]["blockers"]
        assert "UNLIMITED_OR_NAKED_RISK" in repriced["open-naked-short"]["blockers"]
        for item in repriced.values():
            assert item["decision_authority"] == "SUPPORTING_ONLY"
            assert item["approval_eligible"] is False
            assert item["instruction_creation_allowed"] is False
    finally:
        runtime.close()


def test_open_preselection_quote_expires_without_waiting_for_next_provider_poll(
    tmp_path: Path,
) -> None:
    current = [NOW]
    premarket = _conditional_preselection("open-expiring")
    opened = _with_complete_open_economics(
        _conditional_preselection(
            "open-expiring",
            phase=PreselectionPhase.OPEN_REPRICED,
        )
    )
    runtime = NewsCoordinator(
        tmp_path / "conditional-expiry.sqlite3",
        news_providers=(_Provider(),),
        preselection_provider=_AtomicPreselectionProvider(premarket, opened),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: current[0],
    )
    try:
        runtime.refresh_once()
        assert runtime.news_payload()["option_action_pool_count"] == 1

        current[0] = NOW + timedelta(seconds=6)
        expired = runtime.news_payload()

        assert expired["option_action_pool_count"] == 0
        assert expired["open_market_repriced"][0]["action_pool_eligible"] is False
        assert "QUOTE_STALE" in expired["open_market_repriced"][0]["blockers"]
    finally:
        runtime.close()


def test_conditional_preselection_rejects_nonfinite_money_before_projection() -> None:
    with pytest.raises(ValueError, match="maximum_loss_usd"):
        _conditional_preselection(
            "nonfinite-risk",
            maximum_loss_usd=Decimal("Infinity"),
        )


@pytest.mark.parametrize(
    "field",
    (
        "con_id",
        "local_symbol",
        "trading_class",
        "multiplier",
        "exchange",
        "expiry",
        "strike",
        "right",
        "side",
        "ratio",
        "quantity",
        "bid",
        "ask",
        "quote_asof",
        "quote_batch_id",
        "implied_volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "volume",
        "open_interest",
        "dte",
    ),
)
def test_every_missing_leg_execution_field_is_a_named_action_pool_blocker(
    field: str,
) -> None:
    leg = replace(_conditional_leg(), **{field: None})
    candidate = _conditional_preselection(
        f"missing-{field}",
        phase=PreselectionPhase.OPEN_REPRICED,
        legs=(leg,),
    )

    evaluated = evaluate_preselection(candidate, now=NOW)

    assert evaluated.action_pool_eligible is False
    assert f"LEG_EXECUTION_FIELD_MISSING:1:{field}" in evaluated.blockers


def test_strategy_and_evidence_hashes_are_bound_before_action_pool_entry() -> None:
    valid = _conditional_preselection(
        "hash-binding",
        phase=PreselectionPhase.OPEN_REPRICED,
    )
    wrong_strategy = replace(valid, strategy_hash="d" * 64)
    mismatched_evidence = replace(valid, evidence_hashes=("a" * 64,))

    assert "STRATEGY_HASH_MISMATCH" in evaluate_preselection(
        wrong_strategy, now=NOW
    ).blockers
    assert "EVIDENCE_BINDING_MISMATCH" in evaluate_preselection(
        mismatched_evidence, now=NOW
    ).blockers
