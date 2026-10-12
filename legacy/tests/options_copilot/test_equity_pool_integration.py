"""Production scanner-schema integration for the G035 equity pool."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from options_copilot.equity_pool import (
    EquityPoolService,
    EquityPoolStore,
    FactorEvidence,
    FactorKind,
    FactorStatus,
    LiquidityEvidence,
)
from options_copilot.gateway import UnderlyingScanResult
from options_copilot.fundamentals import (
    FundamentalMetric,
    FundamentalObservation,
    FundamentalsService,
    FundamentalsStore,
)
from options_copilot.production_runtime import ProductionPipelineInputs
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.news.macro_proxy import ResearchProxyBinding
from options_copilot.decision import normalize_funnel_trace
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 21, 13, 30, tzinfo=timezone.utc)


class _Gateway:
    def positions(self):
        return ()

    def working_orders(self):
        return ()

    def unsubmitted_instructions(self):
        return ()


class _Pacing:
    ready = True
    capability_hash = "a" * 64

    def usage(self):
        return {"scanner": {"used": 3, "limit": 3}}


def _supporting_factor(kind: FactorKind, symbol: str, slot: datetime) -> FactorEvidence:
    return FactorEvidence(
        factor=kind,
        status=FactorStatus.AVAILABLE,
        signed_signal=Decimal("0.5"),
        confidence=Decimal("0.8"),
        horizon="5D",
        observed_at=slot,
        effective_at=slot,
        valid_until=slot + timedelta(days=1),
        source_hashes=(canonical_hash({"kind": kind.value, "symbol": symbol}),),
        reasons=(f"HASH_BOUND_{kind.value}_READ_MODEL",),
        payload_hash=canonical_hash({"payload": kind.value, "symbol": symbol}),
    )


def _measured_liquidity(symbol: str, slot: datetime) -> LiquidityEvidence:
    source_hash = canonical_hash({"measured-liquidity": symbol, "slot": slot})
    return LiquidityEvidence(
        status=FactorStatus.AVAILABLE, score=Decimal("90"), observed_at=slot,
        source_hashes=(source_hash,), reasons=("MEASURED_UNDERLYING_BID_ASK_SPREAD",),
        payload_hash=canonical_hash({"symbol": symbol, "source_hash": source_hash}),
    )


def test_real_scanner_result_flows_to_nonempty_bounded_pool(monkeypatch, tmp_path) -> None:
    import options_copilot.production_runtime as production_runtime

    sectors = ("Energy", "Financials", "Health Care", "Industrials", "Utilities", "Materials")
    scan_codes = ("MOST_ACTIVE", "TOP_PERC_GAIN", "TOP_PERC_LOSE")
    rows = tuple(
        UnderlyingScanResult(
            rank=index % 50,
            symbol=f"S{index:03d}",
            contract_id=1000 + index,
            exchange="NYSE",
            source_scan=scan_codes[index // 50],
            industry=sectors[index % len(sectors)],
            category="US STOCK",
            subcategory="COMMON",
        )
        for index in range(150)
    )
    monkeypatch.setattr(
        production_runtime,
        "_discover_underlyings",
            lambda *_: production_runtime._UnderlyingDiscoveryRead(
                rows,
                scan_codes,
                (),
                (),
                tuple((scan_code, 50) for scan_code in scan_codes),
                {"scanner": {"used": 3, "limit": 4}},
                3,
            ),
        )
    with EquityPoolStore(tmp_path / "equity.sqlite3") as store:
        service = EquityPoolService(
            store,
            factor_readers={
                FactorKind.REGIME: lambda symbol, slot: _supporting_factor(FactorKind.REGIME, symbol, slot),
                FactorKind.POSITIONING: lambda symbol, slot: _supporting_factor(FactorKind.POSITIONING, symbol, slot),
                FactorKind.TREND_VOLATILITY: lambda symbol, slot: _supporting_factor(FactorKind.TREND_VOLATILITY, symbol, slot),
            },
            liquidity_reader=_measured_liquidity,
            clock=lambda: NOW,
        )
        builds = []

        def build_pool(**kwargs):
            result = service.build(**kwargs)
            builds.append(result)
            return result

        inputs = ProductionPipelineInputs(
            _Gateway(), _Pacing(), object(), core_symbols=("AAPL",), clock=lambda: NOW,
            equity_pool_builder=build_pool,
        )
        inputs._coarse_candidate_outcome = lambda **_: SimpleNamespace(
            candidates=(), reason_codes=(), missing_symbols=(), excluded_symbols=(),
            quote_excluded_symbols=(), quote_exclusion_reasons={},
        )
        payload = inputs.run(scan_run_id="real-schema", slot_at=NOW)
        latest = service.latest_payload()
        normalized_trace = normalize_funnel_trace(
            payload["funnel_trace"], scan_run_id="real-schema"
        )
        stored = builds[0].stored
        replayed = store.replay(stored.snapshot.pool_id, allocator=service.allocator)
        invalid = service.build(
            scanner_rows=(UnderlyingScanResult(-1, "BAD", 9999, "NYSE", "MOST_ACTIVE"),),
            slot=NOW + timedelta(days=1),
            pacing_usage=_Pacing().usage(),
        )

    assert payload["universe"]["scanner"][0]["rank"] == 0
    assert payload["universe"]["scanner"][0]["exchange"] == "NYSE"
    assert payload["universe"]["scanner"][0]["industry"] == "Energy"
    assert latest["discovery_count"] == 150
    assert 0 < latest["selected_count"] <= 30
    assert "AAPL" not in latest["selected_symbols"]
    assert payload["funnel_trace"]["filler_candidates"] == 0
    assert all(
        count <= 5
        for group, count in latest["concentration_counts"].items()
        if group.startswith("SECTOR:")
    )
    assert builds[0].stored.normalized_inputs[0].liquidity.reasons == (
        "MEASURED_UNDERLYING_BID_ASK_SPREAD",
    )
    assert len(builds[0].scanner_input_hashes) == 150
    assert len(set(builds[0].scanner_input_hashes)) == 150
    assert builds[0].pacing_usage_hash == latest["pacing_usage_hash"]
    assert replayed.snapshot_hash == stored.snapshot.snapshot_hash
    assert tuple(row.symbol for row in replayed.selected) == tuple(latest["selected_symbols"])
    assert invalid.stored.snapshot.discovery_count == 0
    assert invalid.stored.snapshot.selected == ()
    assert latest["decision_authority"] == "SUPPORTING_ONLY"
    assert latest["instruction_creation_allowed"] is False
    assert latest["order_allowed"] is False
    assert (
        normalized_trace["equity_pool_reference"]
        == builds[0].as_dict()["equity_pool_reference"]
    )


def test_absent_pre_option_regime_and_positioning_are_missing_not_neutral(tmp_path) -> None:
    row = UnderlyingScanResult(0, "XOM", 101, "NYSE", "TOP_PERC_GAIN", "Energy")
    with EquityPoolStore(tmp_path / "missing.sqlite3") as store:
        service = EquityPoolService(store, clock=lambda: NOW)
        result = service.build(scanner_rows=(row,), slot=NOW, pacing_usage={})
    factors = {factor.factor: factor for factor in result.stored.normalized_inputs[0].factors}
    for kind in (FactorKind.REGIME, FactorKind.POSITIONING):
        assert factors[kind].status is FactorStatus.MISSING
        assert factors[kind].signed_signal is None
        assert factors[kind].confidence is None
        assert factors[kind].reasons == (f"{kind.value}_EVIDENCE_UNAVAILABLE",)
    assert result.stored.normalized_inputs[0].liquidity.reasons == (
        "MEASURED_LIQUIDITY_EVIDENCE_UNAVAILABLE",
    )
    assert result.stored.snapshot.selected == ()


    def failed_reader(_symbol, _slot):
        raise RuntimeError("offline")

    with EquityPoolStore(tmp_path / "failed-liquidity.sqlite3") as failed_store:
        failed = EquityPoolService(
            failed_store, liquidity_reader=failed_reader, clock=lambda: NOW,
        ).build(scanner_rows=(row,), slot=NOW, pacing_usage={})
    assert failed.stored.normalized_inputs[0].liquidity.reasons == ("LIQUIDITY_READ_FAILED",)
    assert failed.stored.snapshot.selected == ()

    with EquityPoolStore(tmp_path / "invalid-liquidity.sqlite3") as invalid_store:
        invalid = EquityPoolService(
            invalid_store, liquidity_reader=lambda *_: {"score": 100}, clock=lambda: NOW,
        ).build(scanner_rows=(row,), slot=NOW, pacing_usage={})
    assert invalid.stored.normalized_inputs[0].liquidity.reasons == ("LIQUIDITY_EVIDENCE_INVALID",)
    assert invalid.stored.snapshot.selected == ()


def test_production_news_percentage_confidence_flows_into_equity_factor(tmp_path) -> None:
    news = {
        "news": (
            {
                "id": "news.xom",
                "symbols": ("XOM",),
                "symbol_binding": {"status": "VERIFIED_PROVIDER_RELATED"},
                "classification": {
                    "direction": "BULLISH",
                    "confidence": 65.0,
                },
                "event_impact_score": 80.0,
                "observed_at": NOW.isoformat(),
            },
        ),
    }
    row = {
        "rank": 0,
        "symbol": "XOM",
        "contract_id": None,
        "exchange": None,
        "source_scan": "NEWS_EVENT_POOL",
        "industry": "Energy",
    }
    with EquityPoolStore(tmp_path / "news-percentage.sqlite3") as store:
        result = EquityPoolService(
            store,
            news_reader=lambda: news,
            clock=lambda: NOW,
        ).build(scanner_rows=(row,), slot=NOW, pacing_usage={})

    factors = {
        factor.factor: factor
        for factor in result.stored.normalized_inputs[0].factors
    }
    news_factor = factors[FactorKind.NEWS]
    assert news_factor.status is FactorStatus.AVAILABLE
    assert news_factor.signed_signal == Decimal("0.65")
    assert news_factor.confidence == Decimal("0.65")
    assert news_factor.reasons == ("G034_DETERMINISTIC_NEWS",)


def test_deterministic_macro_research_proxy_flows_into_etf_factor(tmp_path) -> None:
    binding = ResearchProxyBinding(
        event_category="US_INFLATION",
        source="JIN10",
        proxy_symbol="SPY",
    )
    news = {
        "news": (
            {
                "id": "news.cpi.proxy",
                "symbols": (),
                "symbol_binding": {"status": "UNBOUND"},
                "research_proxy_binding": binding.as_dict(),
                "classification": {
                    "direction": "BULLISH",
                    "confidence": 75.0,
                },
                "event_impact_score": 80.0,
                "observed_at": NOW.isoformat(),
            },
        ),
    }
    row = {
        "rank": 0,
        "symbol": "SPY",
        "contract_id": None,
        "exchange": None,
        "source_scan": "NEWS_EVENT_POOL",
        "security_type": "ETF",
    }
    with EquityPoolStore(tmp_path / "macro-research-proxy.sqlite3") as store:
        result = EquityPoolService(
            store,
            news_reader=lambda: news,
            clock=lambda: NOW,
        ).build(scanner_rows=(row,), slot=NOW, pacing_usage={})

    factors = {
        factor.factor: factor
        for factor in result.stored.normalized_inputs[0].factors
    }
    news_factor = factors[FactorKind.NEWS]
    assert news_factor.status is FactorStatus.AVAILABLE
    assert news_factor.signed_signal == Decimal("0.75")
    assert news_factor.confidence == Decimal("0.75")
    assert news_factor.reasons == (
        "G034_DETERMINISTIC_MACRO_RESEARCH_PROXY",
    )

    tampered = dict(binding.as_dict())
    tampered["mapping_hash"] = "0" * 64
    news["news"][0]["research_proxy_binding"] = tampered
    with EquityPoolStore(tmp_path / "tampered-macro-research-proxy.sqlite3") as store:
        rejected = EquityPoolService(
            store,
            news_reader=lambda: news,
            clock=lambda: NOW,
        ).build(scanner_rows=(row,), slot=NOW, pacing_usage={})
    rejected_factors = {
        factor.factor: factor
        for factor in rejected.stored.normalized_inputs[0].factors
    }
    assert rejected_factors[FactorKind.NEWS].status is FactorStatus.MISSING
    assert rejected_factors[FactorKind.NEWS].reasons == (
        "NEWS_DIRECTION_EVIDENCE_UNAVAILABLE",
    )


def test_live_news_read_model_timestamps_reach_equity_factor(tmp_path) -> None:
    coordinator = NewsCoordinator(
        tmp_path / "nested-news-time.sqlite3",
        clock=lambda: NOW,
    )
    news = {
        "news": [
            {
                "id": "news.nvda",
                "symbols": ["NVDA"],
                "symbol_binding": {"status": "VERIFIED_PROVIDER_RELATED"},
                "direction": "BULLISH",
                "confidence": 65.0,
                "scores": {"event_impact_score": 87.75},
                "times": {
                    "published_at": (NOW - timedelta(minutes=10)).isoformat(),
                    "observed_at": NOW.isoformat(),
                },
            }
        ],
        "source_health": [],
        "asof": NOW.isoformat(),
    }
    calendar = {
        "calendar": [],
        "asof": NOW.isoformat(),
        "snapshot_hash": "a" * 64,
        "window_start": NOW.isoformat(),
        "window_end": (NOW + timedelta(days=14)).isoformat(),
    }
    row = {
        "rank": 0,
        "symbol": "NVDA",
        "contract_id": None,
        "exchange": None,
        "source_scan": "NEWS_EVENT_POOL",
        "industry": "Technology",
    }
    try:
        with coordinator._state_lock:
            coordinator._news_payload = news
            coordinator._calendar_payload = calendar
        with EquityPoolStore(tmp_path / "nested-news-time-pool.sqlite3") as store:
            result = EquityPoolService(
                store,
                news_reader=coordinator.decision_event_payload,
                clock=lambda: NOW,
            ).build(scanner_rows=(row,), slot=NOW, pacing_usage={})
    finally:
        coordinator.close()

    factors = {
        factor.factor: factor
        for factor in result.stored.normalized_inputs[0].factors
    }
    news_factor = factors[FactorKind.NEWS]
    assert news_factor.status is FactorStatus.AVAILABLE
    assert news_factor.observed_at == NOW
    assert news_factor.signed_signal == Decimal("0.65")
    assert news_factor.confidence == Decimal("0.65")
    assert news_factor.reasons == ("G034_DETERMINISTIC_NEWS",)


def test_core_universe_provenance_is_retained_without_becoming_scanner_evidence(
    tmp_path,
) -> None:
    row = {
        "symbol": "XLF",
        "rank": 0,
        "source_scan": "CORE_UNIVERSE",
        "contract_id": 101,
        "exchange": "SMART",
        "security_type": "ETF",
    }
    with EquityPoolStore(tmp_path / "core.sqlite3") as store:
        result = EquityPoolService(store, clock=lambda: NOW).build(
            scanner_rows=(row,),
            slot=NOW,
            pacing_usage={},
        )
        payload = result.as_dict()

    assert result.stored.snapshot.discovery_count == 1
    assert result.stored.normalized_inputs[0].discovery_source == "CORE_UNIVERSE"
    assert result.stored.snapshot.selected == ()
    assert result.stored.snapshot.excluded[0].reasons == ("LIQUIDITY_MISSING",)
    assert payload["decision_authority"] == "SUPPORTING_ONLY"


def test_actual_fundamentals_service_hash_binds_runtime_equity_factor(tmp_path) -> None:
    fundamentals_store = FundamentalsStore(tmp_path / "fundamentals.sqlite3")
    fundamentals_store.append(FundamentalObservation(
        symbol="XOM", cik="0000034088", metric=FundamentalMetric.REVENUE,
        value=Decimal("100"), unit="USD", basis="SEC_XBRL_ACTUAL",
        period_end=date(2026, 6, 30), fiscal_period="Q2", fiscal_year=2026,
        source="SEC_XBRL", source_id="xom:q2:first",
        source_url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000034088.json",
        source_filed_date=date(2026, 8, 18),
        observed_at=NOW - timedelta(days=2), taxonomy="us-gaap",
        tag="RevenueFromContractWithCustomerExcludingAssessedTax", form="10-Q",
    ))
    fundamentals_store.append(FundamentalObservation(
        symbol="XOM", cik="0000034088", metric=FundamentalMetric.REVENUE,
        value=Decimal("110"), unit="USD", basis="SEC_XBRL_ACTUAL",
        period_end=date(2026, 6, 30), fiscal_period="Q2", fiscal_year=2026,
        source="SEC_XBRL", source_id="xom:q2:revision",
        source_url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000034088.json",
        source_filed_date=date(2026, 8, 19),
        observed_at=NOW - timedelta(days=1), taxonomy="us-gaap",
        tag="RevenueFromContractWithCustomerExcludingAssessedTax", form="10-Q",
    ))
    fundamentals = FundamentalsService(
        fundamentals_store,
        providers=(),
        symbols=("XOM",),
    )
    try:
        supporting = fundamentals.supporting_evidence("XOM", as_of=NOW)
        with EquityPoolStore(tmp_path / "fundamental-equity.sqlite3") as store:
            result = EquityPoolService(
                store,
                fundamentals_reader=lambda symbol, slot: fundamentals.supporting_evidence(
                    symbol,
                    as_of=slot,
                ),
                clock=lambda: NOW,
            ).build(
                scanner_rows=(UnderlyingScanResult(
                    0, "XOM", 101, "NYSE", "TOP_PERC_GAIN", "Energy",
                ),),
                slot=NOW,
                pacing_usage={},
            )
    finally:
        fundamentals.close()

    factor = next(
        item for item in result.stored.normalized_inputs[0].factors
        if item.factor is FactorKind.FUNDAMENTALS
    )
    assert supporting["status"] == "DEGRADED"
    assert "FUNDAMENTALS_REFRESH_NOT_OBSERVED" in supporting["reason_codes"]
    assert isinstance(supporting["source_hash"], str)
    assert factor.status is FactorStatus.AVAILABLE
    assert factor.source_hashes[0] == supporting["source_hash"]
    assert factor.reasons == ("POINT_IN_TIME_FUNDAMENTAL_REVISIONS",)
