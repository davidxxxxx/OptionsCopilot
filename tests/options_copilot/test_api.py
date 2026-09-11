from __future__ import annotations

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import time

from options_copilot.api import (
    AfterHoursIndicativeRequest,
    ImmediateReadOnlyScanRequest,
    OptionsCopilotServices,
    ReadOnlyOptionMarketDataDiagnosticRequest,
    create_app,
)
from options_copilot.api.app import _normalise_feed_item
from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomics,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)
from options_copilot.news.macro_proxy import ResearchProxyBinding
from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.news.weekly_brief import (
    SourceHealthStatus,
    WeeklyBrief,
    WeeklyBriefSourceHealth,
    evaluate_weekly_brief_slot,
)
from options_copilot.storage.canonical import canonical_hash


def _weekly_brief_payload() -> dict[str, object]:
    cutoff = datetime(2026, 9, 8, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)
    calendar_hash = canonical_hash({"calendar": "holiday-week"})
    slot = evaluate_weekly_brief_slot(
        scheduled_for=cutoff,
        evaluated_at=cutoff + timedelta(seconds=10),
        official_session_dates=(date(2026, 9, 8), date(2026, 9, 9)),
        calendar_hash=calendar_hash,
    )
    health = WeeklyBriefSourceHealth.build(
        source="OFFICIAL_EVENTS",
        status=SourceHealthStatus.READY,
        mandatory=True,
        observed_at=cutoff - timedelta(seconds=1),
        source_hash=canonical_hash({"source": "official-events"}),
    )
    return WeeklyBrief.build(
        slot=slot,
        evidence_items=(),
        source_health=(health,),
        watch_items=(),
    ).as_dict()


def test_weekly_brief_api_defaults_to_honest_not_run() -> None:
    payload = asyncio.run(_route(create_app(_services()), "/api/weekly-brief")())

    assert payload["status"] == "NOT_RUN"
    assert payload["reason_codes"] == ["WEEKLY_BRIEF_NOT_AVAILABLE"]
    assert payload["execution_allowed"] is False
    assert payload["watch_items"] == []


def test_news_api_exposes_only_current_unbound_macro_research_proxy() -> None:
    binding = ResearchProxyBinding(
        event_category="US_INFLATION",
        source="JIN10",
        proxy_symbol="SPY",
    )
    raw = {
        "id": "news-cpi-research-proxy",
        "title": "US CPI came in below expectations",
        "source": "JIN10",
        "symbols": (),
        "symbol_binding": {"status": "UNBOUND"},
        "research_proxy_binding": binding.as_dict(),
    }

    projected = _normalise_feed_item(raw, key="news", index=0)

    assert projected["symbols"] == []
    assert projected["research_proxy_binding"] == binding.as_dict()
    assert projected["research_proxy_binding"]["eligibility_effect"] == "NONE"
    assert projected["research_proxy_binding"]["risk_effect"] == "NONE"

    tampered = deepcopy(raw)
    tampered["research_proxy_binding"]["mapping_hash"] = "0" * 64
    assert "research_proxy_binding" not in _normalise_feed_item(
        tampered,
        key="news",
        index=0,
    )

    issuer_bound = deepcopy(raw)
    issuer_bound["symbols"] = ("AAPL",)
    issuer_bound["symbol_binding"] = {
        "status": "VERIFIED_PROVIDER_RELATED",
        "provider_adapter": "FINNHUB",
    }
    assert "research_proxy_binding" not in _normalise_feed_item(
        issuer_bound,
        key="news",
        index=0,
    )


def test_equity_pool_latest_strictly_projects_public_allowlist() -> None:
    secret = "provider-secret-must-not-leak"
    digest = "a" * 64
    raw = {
        "schema": "options_copilot.equity_pool_read_model.v1",
        "status": "READY",
        "decision": "RESEARCH_ONLY",
        "reason_codes": [],
        "pool_id": digest,
        "slot": "2026-08-21T13:30:00+00:00",
        "snapshot_hash": digest,
        "chain_hash": digest,
        "normalized_inputs_hash": digest,
        "policy_version": "equity-pool-policy.v1",
        "policy_hash": digest,
        "taxonomy_version": "equity-taxonomy.v1",
        "taxonomy_hash": digest,
        "position_mode": "CLEAR",
        "discovery_count": 1,
        "considered_count": 1,
        "selected_count": 1,
        "excluded_count": 0,
        "selected_symbols": ["AAPL"],
        "selected": [
            {
                "symbol": "AAPL",
                "disposition": "SELECTED",
                "score": {
                    "symbol": "AAPL",
                    "direction_score": "0.5",
                    "coverage_confidence": "0.8",
                    "positive_evidence_mass": "1",
                    "negative_evidence_mass": "0",
                    "conflict_penalty": "0",
                    "uncertainty": "0.2",
                    "liquidity_score": "90",
                    "opportunity_score": "70",
                    "direction_label": "BULLISH",
                    "account_number": "DU123456",
                },
                "classification": {
                    "symbol": "AAPL",
                    "category": "INFORMATION_TECHNOLOGY",
                    "source": "LOCAL_EXACT",
                    "mega_cap_tech": True,
                    "concentration_group": "SECTOR:INFORMATION_TECHNOLOGY",
                    "taxonomy_version": "equity-taxonomy.v1",
                    "taxonomy_hash": digest,
                    "provider_secret": secret,
                },
                "reasons": ["SELECTED_BY_SCORE"],
                "canonical_input_hash": digest,
                "selected_rank": 1,
                "provider_secret": secret,
            }
        ],
        "excluded": [],
        "concentration_counts": {"SECTOR:INFORMATION_TECHNOLOGY": 1},
        "scanner_sources": ["MOST_ACTIVE"],
        "scanner_input_hashes": [digest],
        "pacing_usage_hash": digest,
        "research_only": True,
        "entry_authority": False,
        "approval_eligible": False,
        "decision_authority": "SUPPORTING_ONLY",
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "review_only": True,
        "direct_order_submission": False,
        "account_number": "DU123456",
        "provider_secret": secret,
    }

    payload = asyncio.run(
        _route(
            create_app(_services(equity_pool_provider=lambda: raw)),
            "/api/equity-pool/latest",
        )()
    )
    serialized = json.dumps(payload, sort_keys=True)

    assert payload["selected"][0]["symbol"] == "AAPL"
    assert payload["selected"][0]["score"]["opportunity_score"] == "70"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["entry_authority"] is False
    assert "account_number" not in serialized
    assert "provider_secret" not in serialized
    assert secret not in serialized
    assert "DU123456" not in serialized


def test_immediate_read_only_scan_requires_explicit_token_and_returns_projection() -> None:
    expected = {
        "decision": "NO_TRADE",
        "approval_enabled": False,
        "reasons": ["NO_ELIGIBLE_COMBINATIONS"],
        "candidates": [],
        "manual_read_only_scan": True,
        "review_only": True,
        "direct_order_submission": False,
    }
    route = _route(
        create_app(_services(immediate_scan_handler=lambda: expected)),
        "/api/scans/run-now",
    )

    payload = asyncio.run(
        route(
            ImmediateReadOnlyScanRequest(
                confirmation_token="RUN_READ_ONLY_SCAN_NOW"
            )
        )
    )

    assert payload == expected


def test_immediate_read_only_scan_can_request_bounded_market_scope() -> None:
    received: list[str] = []

    def handler(scope: str) -> dict[str, object]:
        received.append(scope)
        return {
            "decision": "NO_TRADE",
            "approval_enabled": False,
            "review_only": True,
            "direct_order_submission": False,
        }

    route = _route(
        create_app(_services(immediate_scan_handler=handler)),
        "/api/scans/run-now",
    )

    payload = asyncio.run(
        route(
            ImmediateReadOnlyScanRequest(
                confirmation_token="RUN_READ_ONLY_SCAN_NOW",
                scope="BOUNDED_MARKET",
            )
        )
    )

    assert received == ["BOUNDED_MARKET"]
    assert payload["approval_enabled"] is False


def test_option_market_data_diagnostic_requires_exact_contract_and_stays_read_only() -> None:
    received: list[dict[str, object]] = []

    def handler(request: Mapping[str, object]) -> dict[str, object]:
        received.append(dict(request))
        return {
            "schema": "options_copilot.option_market_data_diagnostic.v1",
            "decision_authority": "OBSERVATION_ONLY",
            "review_only": True,
            "direct_order_submission": False,
            "contract_id": request["contract_id"],
        }

    route = _route(
        create_app(_services(option_market_data_diagnostic_handler=handler)),
        "/api/diagnostics/option-market-data",
    )

    payload = asyncio.run(
        route(
            ReadOnlyOptionMarketDataDiagnosticRequest(
                confirmation_token="READ_OPTION_MARKET_DATA_DIAGNOSTIC",
                scope="SINGLE_EXACT_CONTRACT",
                contract_id=732648726,
                symbol="NVDA",
                expiration="2026-09-18",
                strike="230",
                right="C",
                exchange="SMART",
                currency="USD",
                trading_class="NVDA",
                multiplier=100,
                local_symbol="NVDA  260918C00230000",
            )
        )
    )

    assert payload["contract_id"] == 732648726
    assert payload["decision_authority"] == "OBSERVATION_ONLY"
    assert received == [
        {
            "confirmation_token": "READ_OPTION_MARKET_DATA_DIAGNOSTIC",
            "scope": "SINGLE_EXACT_CONTRACT",
            "contract_id": 732648726,
            "symbol": "NVDA",
            "expiration": "2026-09-18",
            "strike": "230",
            "right": "C",
            "exchange": "SMART",
            "currency": "USD",
            "trading_class": "NVDA",
            "multiplier": 100,
            "local_symbol": "NVDA  260918C00230000",
        }
    ]


def test_immediate_scan_campaign_api_exposes_observation_only_evidence() -> None:
    expected = {
        "schema_version": "options_copilot.immediate_scan_campaign.v1",
        "status": "RUNNING_NO_TRADE",
        "decision": "NO_TRADE",
        "campaign_id": "manual-scan-campaign.1234",
        "attempt_count": 1,
        "completed_symbol_count": 0,
        "target_symbol_count": 21,
        "next_symbol": "SPY",
        "attempts": [
            {
                "target_symbol": "SPY",
                "attempt_kind": "WARMUP",
                "scan_run_id": "scan.1",
                "stopped_at_gate": "GATE_4_OPTION_EDGE_LIQUIDITY",
                "reason_codes": ["QUOTE_LIQUIDITY_SPREAD_REJECTED"],
            }
        ],
        "decision_authority": "OBSERVATION_ONLY",
        "approval_enabled": False,
        "review_only": True,
        "direct_order_submission": False,
    }
    route = _route(
        create_app(
            _services(immediate_scan_campaign_provider=lambda: expected)
        ),
        "/api/scans/campaign",
    )

    assert asyncio.run(route()) == expected


def test_after_hours_indicative_api_strictly_projects_nested_public_fields() -> None:
    secret_marker = "sk-must-not-leak-anywhere"
    raw = {
        "schema": "options_copilot.after_hours_indicative.v1",
        "status": "AVAILABLE",
        "freshness_status": "CURRENT",
        "decision": "BUY_NOW",
        "mode": "BROKER_WRITE",
        "observed_at": "2026-08-19T20:10:00+00:00",
        "quote_batch_id": "batch-safe-1",
        "quote_batch_status": "COMPLETE",
        "quote_source": "IBKR_READONLY",
        "quote_batch_count": 1,
        "attempted_candidate_count": 1,
        "requested_count": 1,
        "priced_count": 1,
        "reason_codes": ["PREVIOUS_CLOSE_NON_EXECUTABLE"],
        "strategy_nav_usd": "2116.09",
        "normal_risk_fraction": "0.10",
        "discovery_mode": "IBKR_BOUNDED_MARKET_PROGRESSIVE",
        "sector_coverage": {
            "distinct_count": 1,
            "counts": {"TECHNOLOGY": 1},
            "concentration_warning": False,
        },
        "strategy_coverage": {
            "distinct_count": 1,
            "counts": {"BULL_CALL_VERTICAL": 1},
            "single_structure_warning": True,
        },
        "selection_factors": {
            "broad_ibkr_scanner": True,
            "event_and_news": "SUPPORTING_ONLY",
        },
        "campaign": {
            "completed_underlyings": 1,
            "target_underlyings": 10,
            "remaining_underlyings": 9,
            "continue_after_pacing_window": True,
        },
        "api_key": secret_marker,
        "order_id": "top-order-must-not-leak",
        "credentials": {"username": "private"},
        "candidates": [
            {
                "research_id": "research.qqq.724-727",
                "rank": 1,
                "underlying": "QQQ",
                "sector": "TECHNOLOGY",
                "source_scan": "IBKR_BOUNDED_MARKET",
                "strategy_type": "BULL_CALL_VERTICAL",
                "direction": "BULLISH",
                "research_summary": "Defined-risk closing-mark research.",
                "entry_condition": "Reprice after the regular-session open.",
                "invalidation_condition": "Abandon if the thesis fails.",
                "expiration": "2026-08-21",
                "dte": 16,
                "quantity": 1,
                "pricing_status": "AVAILABLE",
                "freshness_status": "CURRENT",
                "quote_status": "INDICATIVE_ONLY",
                "greeks_status": "AVAILABLE",
                "liquidity_status": "AVAILABLE",
                "indicative_entry_debit_usd": "83.00",
                "execution_cost_cap_usd": "3.00",
                "indicative_maximum_loss_usd": "86.00",
                "indicative_maximum_profit_usd": "414.00",
                "breakeven_price": "724.86",
                "indicative_cost_after_ev_usd": "12.50",
                "strategy_nav_fraction": "0.04064",
                "indicative_price_basis": "PREVIOUS_CLOSE",
                "blockers": ["AFTER_HOURS_INDICATIVE"],
                "approval_eligible": True,
                "instruction_creation_allowed": True,
                "order_allowed": True,
                "order_id": "candidate-order-must-not-leak",
                "credential_blob": secret_marker,
                "account_id": "account-must-not-leak",
                "legs": [
                    {
                        "underlying": "QQQ",
                        "side": "BUY",
                        "contract_id": 724001,
                        "contract_id_ex": "724001@SMART",
                        "local_symbol": "QQQ   260821C00724000",
                        "trading_class": "QQQ",
                        "strike": "724",
                        "right": "C",
                        "expiration": "2026-08-21",
                        "dte": 16,
                        "exchange": "SMART",
                        "currency": "USD",
                        "multiplier": 100,
                        "bid": "2.03",
                        "ask": "2.05",
                        "last": "2.04",
                        "close": "2.01",
                        "indicative_mark": "2.04",
                        "price_basis": "PREVIOUS_CLOSE",
                        "market_data_type": 2,
                        "quote_asof": "2026-08-19T20:09:58+00:00",
                        "implied_volatility": "0.29",
                        "delta": "0.42",
                        "gamma": "0.021",
                        "theta": "-0.08",
                        "vega": "0.11",
                        "volume": 240,
                        "open_interest": 1800,
                        "liquidity_status": "AVAILABLE",
                        "client_secret": secret_marker,
                        "order_id": "leg-order-must-not-leak",
                        "account": "account-must-not-leak",
                    }
                ],
            }
        ],
    }
    route = _route(
        create_app(_services(after_hours_indicative_provider=lambda: raw)),
        "/api/research-top10/indicative",
    )

    payload = asyncio.run(
        route(
            AfterHoursIndicativeRequest(
                confirmation_token="READ_AFTER_HOURS_OPTION_MARKS"
            )
        )
    )

    candidate = payload["candidates"][0]
    leg = candidate["legs"][0]
    assert payload["schema"] == "options_copilot.after_hours_indicative.v1"
    assert payload["quote_batch_id"] == "batch-safe-1"
    assert payload["sector_coverage"]["counts"] == {"TECHNOLOGY": 1}
    assert payload["decision"] == "NO_TRADE"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_allowed"] is False
    assert payload["review_only"] is True
    assert payload["direct_order_submission"] is False
    assert candidate["research_id"] == "research.qqq.724-727"
    assert candidate["dte"] == 16
    assert candidate["quote_status"] == "INDICATIVE_ONLY"
    assert candidate["greeks_status"] == "AVAILABLE"
    assert candidate["liquidity_status"] == "AVAILABLE"
    assert candidate["indicative_entry_debit_usd"] == "83.00"
    assert candidate["indicative_maximum_loss_usd"] == "86.00"
    assert candidate["indicative_maximum_profit_usd"] == "414.00"
    assert candidate["breakeven_price"] == "724.86"
    assert candidate["indicative_cost_after_ev_usd"] == "12.50"
    assert candidate["blockers"] == ["AFTER_HOURS_INDICATIVE"]
    assert candidate["trade_status"] == "NO_TRADE"
    assert candidate["approval_eligible"] is False
    assert leg["contract_id"] == 724001
    assert leg["dte"] == 16
    assert leg["local_symbol"] == "QQQ   260821C00724000"
    assert leg["indicative_mark"] == "2.04"
    assert leg["implied_volatility"] == "0.29"
    assert leg["delta"] == "0.42"
    assert leg["volume"] == 240
    assert leg["open_interest"] == 1800
    assert leg["liquidity_status"] == "AVAILABLE"
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in (
        secret_marker,
        "api_key",
        "credentials",
        "credential_blob",
        "client_secret",
        "order_id",
        "account_id",
        "account-must-not-leak",
    ):
        assert forbidden not in serialized


def test_weekly_brief_api_exposes_only_hash_verified_provisional_projection() -> None:
    raw = _weekly_brief_payload()
    raw["read_model_schema"] = "options_copilot.weekly_brief_read_model.v1"
    raw["persistence"] = {
        "sequence": 1,
        "row_hash": canonical_hash({"row": 1}),
        "append_only": True,
        "inserted": True,
    }
    payload = asyncio.run(
        _route(
            create_app(_services(weekly_brief_provider=lambda: raw)),
            "/api/weekly-brief",
        )()
    )

    assert payload["status"] == "PROVISIONAL"
    assert payload["decision"] == "OBSERVATION_ONLY"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["execution_allowed"] is False
    assert payload["review_allowed"] is False
    assert payload["combination_generation_allowed"] is False
    assert payload["watch_count"] == 0
    assert payload["persistence"]["append_only"] is True
    assert "inserted" not in payload["persistence"]


def test_weekly_brief_api_requires_every_unique_named_mandatory_source_ready() -> None:
    cutoff = datetime(2026, 9, 8, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)
    unavailable = WeeklyBriefSourceHealth.build(
        source="SECOND_MANDATORY",
        status=SourceHealthStatus.UNAVAILABLE,
        mandatory=True,
        observed_at=cutoff - timedelta(seconds=1),
        source_hash=canonical_hash({"source": "second-mandatory"}),
        reason_codes=("SOURCE_UNAVAILABLE",),
    ).as_dict()
    cases: list[dict[str, object]] = []

    mixed = _weekly_brief_payload()
    mixed["source_health"] = [*mixed["source_health"], unavailable]
    cases.append(mixed)

    blank = _weekly_brief_payload()
    blank["source_health"][0]["source"] = ""
    cases.append(blank)

    duplicate = _weekly_brief_payload()
    duplicate["source_health"] = [
        *duplicate["source_health"],
        deepcopy(duplicate["source_health"][0]),
    ]
    cases.append(duplicate)

    for raw in cases:
        raw["content_hash"] = canonical_hash(
            {key: value for key, value in raw.items() if key != "content_hash"}
        )
        payload = asyncio.run(
            _route(
                create_app(_services(weekly_brief_provider=lambda raw=raw: raw)),
                "/api/weekly-brief",
            )()
        )
        assert payload["status"] == "NOT_RUN"
        assert payload["reason_codes"] == [
            "WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"
        ]


def test_weekly_brief_api_validates_mandatory_sources_beyond_display_limit() -> None:
    cutoff = datetime(2026, 9, 8, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)
    raw = _weekly_brief_payload()
    padding = [
        WeeklyBriefSourceHealth.build(
            source=f"OPTIONAL_{index:02d}",
            status=SourceHealthStatus.READY,
            mandatory=False,
            observed_at=cutoff - timedelta(seconds=1),
            source_hash=canonical_hash({"source": f"optional-{index:02d}"}),
        ).as_dict()
        for index in range(15)
    ]
    unavailable = WeeklyBriefSourceHealth.build(
        source="SEVENTEENTH_MANDATORY",
        status=SourceHealthStatus.UNAVAILABLE,
        mandatory=True,
        observed_at=cutoff - timedelta(seconds=1),
        source_hash=canonical_hash({"source": "seventeenth-mandatory"}),
        reason_codes=("SOURCE_UNAVAILABLE",),
    ).as_dict()
    raw["source_health"] = [*raw["source_health"], *padding, unavailable]
    raw["content_hash"] = canonical_hash(
        {key: value for key, value in raw.items() if key != "content_hash"}
    )

    payload = asyncio.run(
        _route(
            create_app(_services(weekly_brief_provider=lambda: raw)),
            "/api/weekly-brief",
        )()
    )

    assert payload["status"] == "NOT_RUN"
    assert payload["reason_codes"] == [
        "WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"
    ]


def test_weekly_brief_api_rejects_authority_or_extra_field_injection() -> None:
    raw = _weekly_brief_payload()
    raw["execution_allowed"] = True
    raw["api_key"] = "must-not-leak"
    payload = asyncio.run(
        _route(
            create_app(_services(weekly_brief_provider=lambda: raw)),
            "/api/weekly-brief",
        )()
    )

    assert payload["status"] == "NOT_RUN"
    assert payload["reason_codes"] == ["WEEKLY_BRIEF_API_PROJECTION_INVALID"]
    assert "must-not-leak" not in repr(payload)


def test_news_api_does_not_block_the_event_loop_for_a_sync_provider() -> None:
    def slow_news_provider() -> dict[str, object]:
        time.sleep(0.20)
        return {"news": [], "asof": None, "provider": {"status": "READY"}}

    app = create_app(_services(news_provider=slow_news_provider))

    async def exercise() -> tuple[float, dict[str, object]]:
        task = asyncio.create_task(_route(app, "/api/news")())
        started = time.perf_counter()
        await asyncio.sleep(0.02)
        event_loop_delay = time.perf_counter() - started
        return event_loop_delay, await task

    event_loop_delay, payload = asyncio.run(exercise())

    assert event_loop_delay < 0.10
    assert payload["count"] == 0


def test_health_api_does_not_block_the_event_loop_for_a_sync_provider() -> None:
    def slow_health_provider() -> dict[str, object]:
        time.sleep(0.20)
        return {"dependencies": {}}

    app = create_app(_services(health_provider=slow_health_provider))

    async def exercise() -> tuple[float, dict[str, object]]:
        task = asyncio.create_task(_route(app, "/health")())
        started = time.perf_counter()
        await asyncio.sleep(0.02)
        event_loop_delay = time.perf_counter() - started
        return event_loop_delay, await task

    event_loop_delay, payload = asyncio.run(exercise())

    assert event_loop_delay < 0.10
    assert payload["status"] == "DEGRADED"


def test_news_and_calendar_default_to_a_safe_unconfigured_empty_projection() -> None:
    app = create_app(_services())

    news = asyncio.run(_route(app, "/api/news")())
    assert news == {
        "news": [],
        "count": 0,
        "research_pool_count": 0,
        "action_pool_count": 0,
        "approval_eligible": False,
        "analysis_backfill": {
            "status": "UNAVAILABLE",
            "reason": "ANALYSIS_BACKFILL_UNAVAILABLE",
            "pending_count": 0,
            "failed_count": 0,
            "persisted_hits_this_cycle": 0,
            "model_misses_this_cycle": 0,
            "model_batch_limit": 0,
            "local_restore_batch_limit": 0,
            "lookup_batch_limit": 0,
            "integrity": {
                "status": "PENDING",
                "batch_rows": 0,
                "verified_rows": 0,
                "remaining_rows": 0,
                "complete": False,
            },
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
        },
        "shadow_advisory": {
            "status": "UNAVAILABLE",
            "reason": "SHADOW_ADVISORY_UNAVAILABLE",
            "advisory_count": 0,
            "attempted_count": 0,
            "failure_count": 0,
            "deferred_count": 0,
            "failure_reasons": {},
            "input_count": 0,
            "eligible_input_count": 0,
            "skipped_count": 0,
            "skipped_reasons": {},
            "maximum_batch_size": 0,
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        },
        "pre_market_preselections": [],
        "pre_market_preselection_count": 0,
        "open_market_repriced": [],
        "open_market_repriced_count": 0,
        "option_action_pool": [],
        "option_action_pool_count": 0,
        "option_approval_eligible": False,
        "source_health": [],
        "source_runtime": [],
        "preselection_coverage": {
            "requested_count": 10,
            "available_count": 0,
            "open_count": 0,
            "source": "INDEPENDENT_TOP10_LEDGER",
            "status": "UNAVAILABLE",
            "reason": "TOP10_PREMARKET_COVERAGE_UNAVAILABLE",
            "ledger_reason": "PRESELECTION_PROVIDER_UNCONFIGURED",
            "latest_run_id": None,
            "latest_head_hash": None,
            "freeze_slot": None,
            "latest_open_batch_id": None,
            "latest_open_batch_head_hash": None,
            "reprice_slot": None,
            "open_reprice_producer_status": "UNAVAILABLE",
            "open_observation_status": "UNAVAILABLE",
            "atomic_batch_available": False,
            "atomic_batch_blocker": "TOP10_PREMARKET_COVERAGE_UNAVAILABLE",
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        },
        "asof": None,
        "provider": {
            "name": "unconfigured",
            "status": "UNCONFIGURED",
            "latency_ms": None,
            "asof": None,
            "message": None,
        },
    }
    assert asyncio.run(_route(app, "/api/calendar")()) == {
        "calendar": [],
        "count": 0,
        "source_runtime": [],
        "asof": None,
        "provider": {
            "name": "unconfigured",
            "status": "UNCONFIGURED",
            "latency_ms": None,
            "asof": None,
            "message": None,
        },
    }


def test_news_api_exposes_backfill_progress_and_fails_action_pool_closed() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [
                    {
                        "id": "pending-analysis",
                        "headline": "Pending analysis must remain research only",
                        "source": "fixture",
                        "published_at": "2026-08-04T08:00:00Z",
                        "research_pool": True,
                        "research_rank": 1,
                        "action_pool": True,
                        "action_pool_eligible": True,
                        "action_rank": 1,
                        "ibkr_provenance": {
                            "source": "IBKR",
                            "symbol": "AAPL",
                            "quote_snapshot_id": "quote-1",
                            "observed_at": "2026-08-04T08:00:00Z",
                        },
                    }
                ],
                "analysis_backfill": {
                    "status": "PENDING",
                    "reason": "ANALYSIS_LEDGER_INTEGRITY_PENDING",
                    "pending_count": 500,
                    "failed_count": 0,
                    "persisted_hits_this_cycle": 0,
                    "model_misses_this_cycle": 0,
                    "model_batch_limit": 10,
                    "lookup_batch_limit": 500,
                    "integrity": {
                        "status": "PENDING",
                        "batch_rows": 250,
                        "verified_rows": 250,
                        "remaining_rows": 250,
                        "complete": False,
                    },
                },
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())

    assert payload["analysis_backfill"] == {
        "status": "PENDING",
        "reason": "ANALYSIS_LEDGER_INTEGRITY_PENDING",
        "pending_count": 500,
        "failed_count": 0,
        "persisted_hits_this_cycle": 0,
        "model_misses_this_cycle": 0,
        "model_batch_limit": 10,
        "local_restore_batch_limit": 0,
        "lookup_batch_limit": 500,
        "integrity": {
            "status": "PENDING",
            "batch_rows": 250,
            "verified_rows": 250,
            "remaining_rows": 250,
            "complete": False,
        },
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
    }
    assert payload["action_pool_count"] == 0
    assert payload["news"][0]["action_pool"] is False
    assert payload["news"][0]["action_pool_eligible"] is False


def test_news_api_exposes_sanitized_shadow_repair_failure_status() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [],
                "shadow_advisory": {
                    "status": "DEGRADED",
                    "reason": "SHADOW_ADVISORY_LEDGER_REPAIR_FAILED",
                    "advisory_count": 2,
                    "attempted_count": 0,
                    "failure_count": 1,
                    "deferred_count": 0,
                    "failure_reasons": {
                        "DEEPSEEK_FLASH_DAILY_CALL_CAP": 1,
                        "Authorization: Bearer must-not-leak": 99,
                    },
                    "input_count": 9,
                    "eligible_input_count": 7,
                    "skipped_count": 2,
                    "skipped_reasons": {
                        "SHADOW_INPUT_BEFORE_ENABLEMENT": 1,
                        "SHADOW_INPUT_CONFLICTED": 1,
                        "Authorization: Bearer must-not-leak": 99,
                    },
                    "maximum_batch_size": 3,
                    "decision_authority": "EXECUTE",
                    "approval_eligible": True,
                    "instruction_creation_allowed": True,
                    "order_allowed": True,
                    "raw_error": "Authorization: Bearer must-not-leak",
                },
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())

    assert payload["shadow_advisory"] == {
        "status": "DEGRADED",
        "reason": "SHADOW_ADVISORY_LEDGER_REPAIR_FAILED",
        "advisory_count": 2,
        "attempted_count": 0,
        "failure_count": 1,
        "deferred_count": 0,
        "failure_reasons": {"DEEPSEEK_FLASH_DAILY_CALL_CAP": 1},
        "input_count": 9,
        "eligible_input_count": 7,
        "skipped_count": 2,
        "skipped_reasons": {
            "SHADOW_INPUT_BEFORE_ENABLEMENT": 1,
            "SHADOW_INPUT_CONFLICTED": 1,
        },
        "maximum_batch_size": 3,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    assert "must-not-leak" not in repr(payload)


def test_news_api_preserves_truthful_shadow_no_attempt_and_skipped_statuses() -> None:
    cases = (
        (
            "SHADOW_ADVISORY_NO_ATTEMPT",
            "UNAVAILABLE",
            {
                "SHADOW_INPUT_BEFORE_ENABLEMENT": 2,
                "SHADOW_INPUT_SYMBOL_COUNT_INVALID": 1,
            },
        ),
        (
            "SHADOW_ADVISORY_SKIPPED",
            "PENDING",
            {
                "SHADOW_INPUT_DUPLICATE": 1,
                "SHADOW_INPUT_INCOMPLETE": 1,
            },
        ),
    )

    for reason, status, skipped_reasons in cases:
        app = create_app(
            _services(
                news_provider=lambda reason=reason,
                status=status,
                skipped_reasons=skipped_reasons: {
                    "news": [],
                    "shadow_advisory": {
                        "status": status,
                        "reason": reason,
                        "advisory_count": 0,
                        "attempted_count": 0,
                        "failure_count": 0,
                        "deferred_count": 0,
                        "failure_reasons": {},
                        "input_count": 4,
                        "eligible_input_count": 3,
                        "skipped_count": (
                            3 if reason == "SHADOW_ADVISORY_NO_ATTEMPT" else 2
                        ),
                        "skipped_reasons": skipped_reasons,
                        "maximum_batch_size": 3,
                        "decision_authority": "EXECUTE",
                        "approval_eligible": True,
                        "instruction_creation_allowed": True,
                        "order_allowed": True,
                    },
                }
            )
        )

        projected = asyncio.run(_route(app, "/api/news")())["shadow_advisory"]

        assert projected["status"] == status
        assert projected["reason"] == reason
        assert projected["input_count"] == 4
        assert projected["eligible_input_count"] == 3
        assert projected["skipped_count"] == (
            3 if reason == "SHADOW_ADVISORY_NO_ATTEMPT" else 2
        )
        assert projected["skipped_reasons"] == skipped_reasons
        assert projected["decision_authority"] == "SUPPORTING_ONLY"
        assert projected["approval_eligible"] is False
        assert projected["instruction_creation_allowed"] is False
        assert projected["order_allowed"] is False


def test_news_projection_whitelists_fields_and_normalises_serializable_values() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "asof": "2026-08-04T08:00:00Z",
                "provider": {
                    "name": "wire-a",
                    "status": "healthy",
                    "latency_ms": 14,
                    "api_token": "must-not-leak",
                },
                "source_health": [
                    {
                        "source": "SEC",
                        "source_kind": "NEWS",
                        "status": "degraded",
                        "reason": "ticker_resolution_failed",
                        "success_count": 50,
                        "failure_date_count": 0,
                        "asof": "2026-08-04T08:00:00Z",
                        "Authorization": "Bearer must-not-leak",
                    },
                    {
                        "source": "Nasdaq",
                        "source_kind": "calendar",
                        "status": "ready",
                        "reason": "NASDAQ_EARNINGS_PARTIAL_WINDOW",
                        "success_count": 1906,
                        "failure_date_count": 4,
                    },
                    {
                        "source": "Jin10",
                        "source_kind": "news",
                        "status": "down",
                        "reason": "authentication_failed",
                        "success_count": 0,
                        "failure_date_count": 0,
                    },
                    {
                        "source": "unsafe",
                        "source_kind": "news",
                        "status": "error",
                        "reason": "Authorization: Bearer must-not-leak",
                        "success_count": -1,
                        "failure_date_count": -2,
                    },
                    {
                        "source": "CompanyIrEventProvider",
                        "source_kind": "news",
                        "status": "unconfigured",
                        "reason": "unconfigured",
                        "success_count": 0,
                        "failure_date_count": 0,
                    },
                ],
                "news": [
                    {
                        "id": "wire-1",
                        "headline": "SPY macro catalyst",
                        "description": "A source observation",
                        "source": "Wire A",
                        "symbols": ["spy", "SPY", "not a ticker!"],
                        "symbol_binding": {
                            "status": "verified_provider_related",
                            "provider_adapter": "finnhub",
                            "raw_related": "must-not-project",
                        },
                        "stage": "market_confirmed",
                        "category": "MACRO",
                        "event_impact_score": 41,
                        "option_tradability_score": 12.5,
                        "combined_opportunity_score": 0.83,
                        "published_at": "2026-08-04T07:59:00Z",
                        "received_at": "2026-08-04T07:59:02Z",
                        "observed_at": "2026-08-04T07:59:03Z",
                        "approval_id": "must-not-leak",
                        "order_id": "must-not-leak",
                        "evidence": [
                            {
                                "publisher": "SEC",
                                "headline": "Primary source",
                                "url": "https://example.test/evidence?token=drop-me",
                            }
                        ],
                        "related_options": [
                            {
                                "underlying": "SPY",
                                "thesis": "Observe IV and liquidity before research refresh.",
                                "order_id": "must-not-leak",
                            }
                        ],
                    }
                ],
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())

    assert payload["provider"] == {
        "name": "wire-a",
        "status": "HEALTHY",
        "latency_ms": 14.0,
        "asof": None,
        "message": None,
    }
    assert payload["source_health"] == [
        {
            "source": "SEC",
            "source_kind": "NEWS",
            "status": "DEGRADED",
            "reason": "TICKER_RESOLUTION_FAILED",
            "success_count": 50,
            "failure_date_count": 0,
            "asof": "2026-08-04T08:00:00Z",
            "decision_authority": "SUPPORTING_ONLY",
        },
        {
            "source": "NASDAQ",
            "source_kind": "CALENDAR",
            "status": "DEGRADED",
            "reason": "NASDAQ_EARNINGS_PARTIAL_WINDOW",
            "success_count": 1906,
            "failure_date_count": 4,
            "asof": None,
            "decision_authority": "SUPPORTING_ONLY",
        },
        {
            "source": "JIN10",
            "source_kind": "NEWS",
            "status": "DOWN",
            "reason": "AUTHENTICATION_FAILED",
            "success_count": 0,
            "failure_date_count": 0,
            "asof": None,
            "decision_authority": "SUPPORTING_ONLY",
        },
        {
            "source": "UNSAFE",
            "source_kind": "NEWS",
            "status": "DEGRADED",
            "reason": "PROVIDER_DEGRADED",
            "success_count": 0,
            "failure_date_count": 0,
            "asof": None,
            "decision_authority": "SUPPORTING_ONLY",
        },
        {
            "source": "COMPANYIREVENTPROVIDER",
            "source_kind": "NEWS",
            "status": "UNCONFIGURED",
            "reason": "UNCONFIGURED",
            "success_count": 0,
            "failure_date_count": 0,
            "asof": None,
            "decision_authority": "SUPPORTING_ONLY",
        },
    ]
    assert payload["source_runtime"] == []
    legacy_news_item = dict(payload["news"][0])
    additive = {
        key: legacy_news_item.pop(key)
        for key in (
            "deterministic_research_rank",
            "shadow_suggested_rank",
            "rank_displacement",
            "shadow_action_effect",
            "shadow_risk_effect",
            "shadow_eligibility_effect",
            "intelligence",
        )
    }
    assert additive["deterministic_research_rank"] is None
    assert additive["shadow_suggested_rank"] is None
    assert additive["rank_displacement"] is None
    assert additive["shadow_action_effect"] == "NONE"
    assert additive["shadow_risk_effect"] == "NONE"
    assert additive["shadow_eligibility_effect"] == "NONE"
    intelligence = additive["intelligence"]
    intelligence_payload = {
        key: value
        for key, value in intelligence.items()
        if key != "intelligence_hash"
    }
    assert intelligence["intelligence_hash"] == canonical_hash(intelligence_payload)
    assert intelligence["primary_category"] == "MACRO"
    assert intelligence["decision_authority"] == "SUPPORTING_ONLY"
    assert intelligence["approval_eligible"] is False
    assert set(additive) == {
        "deterministic_research_rank",
        "shadow_suggested_rank",
        "rank_displacement",
        "shadow_action_effect",
        "shadow_risk_effect",
        "shadow_eligibility_effect",
        "intelligence",
    }
    assert [legacy_news_item] == [
        {
            "id": "wire-1",
            "title": "SPY macro catalyst",
            "summary": "A source observation",
            "source": "Wire A",
            "symbols": ["SPY"],
            "status": "MARKET_CONFIRMED",
            "category": "MACRO",
                "classifier": None,
                "scores": {
                    "event_impact_score": 41.0,
                    "option_tradability_score": 12.5,
                    "combined_opportunity_score": 83.0,
                },
                "direction": None,
                "horizon": None,
                "confidence": None,
                "rank": None,
                "research_rank": None,
                "watch_rank": None,
                "action_rank": None,
                "rank_one": False,
                "research_pool": False,
                "action_pool": False,
                "action_pool_eligible": False,
                "decision_authority": "SUPPORTING_ONLY",
                "symbol_binding": {
                    "status": "VERIFIED_PROVIDER_RELATED",
                    "provider_adapter": "FINNHUB",
                    "decision_authority": "SUPPORTING_ONLY",
                },
                "times": {
                    "event_at": None,
                    "published_at": "2026-08-04T07:59:00Z",
                    "first_seen_at": None,
                    "received_at": "2026-08-04T07:59:02Z",
                    "observed_at": "2026-08-04T07:59:03Z",
                    "analysis_completed_at": None,
                },
                "latency": {
                    "published_to_first_seen_ms": None,
                    "first_seen_to_analysis_ms": None,
                },
            "evidence": [
                {
                    "source": "SEC",
                    "title": "Primary source",
                    "url": "https://example.test/evidence",
                    "observed_at": None,
                }
                ],
                "provenance": [],
                "counter_evidence": [],
                "ibkr_provenance": None,
                "related_options": [
                {
                    "symbol": "SPY",
                    "summary": "Observe IV and liquidity before research refresh.",
                    "asof": None,
                }
            ],
        }
    ]


def test_news_projection_exposes_only_sanitized_deepseek_shadow_advisory() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [
                    {
                        "id": "shadow-1",
                        "headline": "SPY macro catalyst",
                        "source": "SEC",
                        "published_at": "2026-08-10T08:00:00Z",
                        "classifier": "DETERMINISTIC_RULES",
                        "research_advisory": {
                            "classifier": "STRUCTURED_LLM",
                            "research_priority_score": "91.25",
                            "classification": {
                                "category": "macro",
                                "symbols": ["spy", "not a ticker!"],
                                "direction": "bullish",
                                "horizon": "days_1_3",
                                "confidence": "0.82",
                                "counter_evidence": [
                                    "Rates could reverse the move.",
                                    "Authorization: Bearer must-not-leak",
                                ],
                                "classifier": "STRUCTURED_LLM",
                                "prompt": "must-not-project",
                            },
                            "shadow_prediction_count": 5,
                            "approval_eligible": True,
                            "instruction_creation_allowed": True,
                            "order_allowed": True,
                            "raw_response": "must-not-project",
                            "api_key": "must-not-leak",
                        },
                    }
                ]
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())
    advisory = payload["news"][0]["research_advisory"]

    assert advisory == {
        "classifier": "STRUCTURED_LLM",
        "research_priority_score": 91.25,
        "classification": {
            "category": "MACRO",
            "symbols": ["SPY"],
            "direction": "BULLISH",
            "horizon": "DAYS_1_3",
            "confidence": 82.0,
            "counter_evidence": ["Rates could reverse the move."],
            "classifier": "STRUCTURED_LLM",
        },
        "shadow_prediction_count": 5,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    serialized = json.dumps(payload, sort_keys=True)
    assert "must-not-project" not in serialized
    assert "must-not-leak" not in serialized
    assert "raw_response" not in serialized
    assert "api_key" not in serialized
    assert "approval_id" not in str(payload)
    assert "order_id" not in str(payload)
    assert "api_token" not in str(payload)


def test_news_projection_uses_only_named_score_fields_and_filter_metadata() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [{
                    "id": "event-1", "title": "Fed decision", "source": "Jin10",
                    "symbols": ["SPY"], "classification": {"category": "FOMC"},
                    "status": "MARKET_CONFIRMED", "event_impact_score": 70,
                    "option_tradability_score": 80, "combined_opportunity_score": 75,
                    "opportunity_score": 99, "risk_score": 1,
                }]
            }
        )
    )

    item = asyncio.run(_route(app, "/api/news")())["news"][0]

    assert item["category"] == "FOMC"
    assert item["source"] == "Jin10"
    assert item["symbols"] == ["SPY"]
    assert item["status"] == "MARKET_CONFIRMED"
    assert item["scores"] == {
        "event_impact_score": 70.0,
        "option_tradability_score": 80.0,
        "combined_opportunity_score": 75.0,
    }


def test_news_projection_repairs_only_proven_display_mojibake() -> None:
    mojibake_title = "NVIDIA\u00e2\u0080\u0099s earnings setup"
    mojibake_summary = "Ahead of \u00e2\u0080\u009cAI demand\u00e2\u0080\u009d."
    source_rows = [
        {
            "id": "finnhub-mojibake",
            "title": mojibake_title,
            "summary": mojibake_summary,
            "source": "SeekingAlpha",
        },
        {
            "id": "legitimate-latin-letter",
            "title": "Research on \u00e2 proteins",
            "summary": "Legitimate text must remain unchanged.",
            "source": "Example",
        },
        {
            "id": "cp1252-mojibake",
            "title": "Market\u00e2\u20ac\u2122s close",
            "summary": "Strict CP1252 round-trip is also display-only.",
            "source": "Example",
        },
    ]
    app = create_app(_services(news_provider=lambda: {"news": source_rows}))

    projected = asyncio.run(_route(app, "/api/news")())["news"]

    assert projected[0]["title"] == "NVIDIA\u2019s earnings setup"
    assert projected[0]["summary"] == "Ahead of \u201cAI demand\u201d."
    assert projected[1]["title"] == "Research on \u00e2 proteins"
    assert projected[2]["title"] == "Market\u2019s close"
    assert source_rows[0]["title"] == mojibake_title
    assert source_rows[0]["summary"] == mojibake_summary


def test_news_api_downgrades_incomplete_verified_symbol_binding() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [
                    {
                        "id": "event-unproven",
                        "title": "Unproven binding",
                        "symbols": ["NVDA"],
                        "watch_rank": 1,
                        "action_rank": 1,
                        "action_pool": True,
                        "action_pool_eligible": True,
                        "symbol_binding": {
                            "status": "VERIFIED_PROVIDER_RELATED",
                            "provider_adapter": None,
                        },
                        "ibkr_provenance": {
                            "symbol": "NVDA",
                            "quote_snapshot_id": "quote-unproven",
                            "observed_at": "2026-08-04T08:00:00Z",
                        },
                        "related_options": [
                            {
                                "underlying": "NVDA",
                                "thesis": "Must be removed with the false binding.",
                            }
                        ],
                    }
                ]
            }
        )
    )

    item = asyncio.run(_route(app, "/api/news")())["news"][0]

    assert item["symbols"] == []
    assert item["symbol_binding"]["status"] == "UNVERIFIED_PROVIDER_BINDING"
    assert item["ibkr_provenance"] is None
    assert item["watch_rank"] is None
    assert item["action_rank"] is None
    assert item["action_pool"] is False
    assert item["action_pool_eligible"] is False
    assert item["related_options"] == []


def test_news_api_quarantines_provider_related_unverified_symbol_binding() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [
                    {
                        "id": "event-provider-related-unverified",
                        "title": "Provider related field lacks exact corroboration",
                        "symbols": ["AMZN"],
                        "research_rank": 1,
                        "research_pool": True,
                        "watch_rank": 1,
                        "action_rank": 1,
                        "action_pool": True,
                        "action_pool_eligible": True,
                        "symbol_binding": {
                            "status": "PROVIDER_RELATED_UNVERIFIED",
                            "provider_adapter": "FINNHUB",
                        },
                        "ibkr_provenance": {
                            "symbol": "AMZN",
                            "quote_snapshot_id": "quote-unverified-related",
                            "observed_at": "2026-08-04T08:00:00Z",
                        },
                        "related_options": [
                            {
                                "underlying": "AMZN",
                                "thesis": "Must not survive an unverified binding.",
                            }
                        ],
                    }
                ]
            }
        )
    )

    item = asyncio.run(_route(app, "/api/news")())["news"][0]

    assert item["symbol_binding"] == {
        "status": "PROVIDER_RELATED_UNVERIFIED",
        "provider_adapter": "FINNHUB",
        "decision_authority": "SUPPORTING_ONLY",
    }
    assert item["symbols"] == []
    assert item["ibkr_provenance"] is None
    assert item["watch_rank"] is None
    assert item["action_rank"] is None
    assert item["action_pool"] is False
    assert item["action_pool_eligible"] is False
    assert item["related_options"] == []


def test_news_envelope_ignores_unverified_pool_counts_and_forces_no_approval() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [],
                "research_pool_count": 10,
                "action_pool_count": 3,
                "approval_eligible": True,
                "creator_token": "must-not-leak",
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())

    assert payload["research_pool_count"] == 0
    assert payload["action_pool_count"] == 0
    assert payload["approval_eligible"] is False
    assert "creator_token" not in payload


def test_news_pool_counts_are_derived_and_duplicate_ranks_fail_closed() -> None:
    rows = [
        {
            "id": f"event-{index}",
            "title": "Bound event",
            "symbols": ["SPY"],
            "research_rank": 1,
            "watch_rank": 1,
            "action_rank": 1,
            "research_pool": True,
            "action_pool": True,
            "action_pool_eligible": True,
            "ibkr_provenance": {
                "symbol": "SPY",
                "quote_snapshot_id": f"quote-{index}",
                "observed_at": "2026-08-04T08:00:00Z",
            },
        }
        for index in (1, 2)
    ]
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": rows,
                "analysis_backfill": {
                    "status": "READY",
                    "integrity": {"status": "VERIFIED", "complete": True},
                },
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())

    assert payload["research_pool_count"] == 1
    assert payload["action_pool_count"] == 1
    assert payload["news"][0]["research_pool"] is True
    assert payload["news"][0]["watch_rank"] == 1
    assert payload["news"][0]["action_pool"] is True
    assert payload["news"][1]["research_pool"] is False
    assert payload["news"][1]["watch_rank"] is None
    assert payload["news"][1]["action_pool"] is False


def test_news_top10_coverage_is_derived_and_open_observation_is_not_started() -> None:
    head_hash = "a" * 64
    row = _premarket_ledger_row("pre-1", rank=1, head_hash=head_hash, row_hash="b" * 64)
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [],
                "pre_market_preselections": [row],
                "open_market_repriced": [],
                "preselection_coverage": {
                    "source": "INDEPENDENT_TOP10_LEDGER",
                    "status": "PARTIAL",
                    "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
                    "ledger_reason": "TOP10_PREMARKET_COVERAGE_INCOMPLETE",
                    "requested_count": 10,
                    "available_count": 10,
                    "open_count": 10,
                    "latest_run_id": "run-1",
                    "latest_head_hash": head_hash,
                },
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())

    assert payload["pre_market_preselection_count"] == 1
    assert payload["open_market_repriced_count"] == 0
    assert payload["preselection_coverage"]["available_count"] == 1
    assert payload["preselection_coverage"]["open_count"] == 0
    assert payload["preselection_coverage"]["open_observation_status"] == "NOT_STARTED"
    assert payload["preselection_coverage"]["reason"] == (
        "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
    )
    assert payload["preselection_coverage"]["decision_authority"] == (
        "SUPPORTING_ONLY"
    )


def test_news_top10_duplicate_or_overflow_rows_fail_closed_without_top3_fallback() -> None:
    head_hash = "c" * 64
    rows = [
        _premarket_ledger_row("duplicate", rank=1, head_hash=head_hash, row_hash="d" * 64),
        _premarket_ledger_row("duplicate", rank=2, head_hash=head_hash, row_hash="e" * 64),
    ]
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [],
                "pre_market_preselections": rows,
                "open_market_repriced": [],
                "option_action_pool": [{"preselection_id": "ranking-top3-fallback"}],
                "preselection_coverage": {
                    "source": "INDEPENDENT_TOP10_LEDGER",
                    "status": "PARTIAL",
                    "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
                    "requested_count": 10,
                    "available_count": 2,
                    "open_count": 0,
                    "latest_run_id": "run-1",
                    "latest_head_hash": head_hash,
                },
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/news")())

    assert payload["pre_market_preselections"] == []
    assert payload["open_market_repriced"] == []
    assert payload["option_action_pool"] == []
    assert payload["preselection_coverage"]["status"] == "UNAVAILABLE"
    assert payload["preselection_coverage"]["reason"] == (
        "PRESELECTION_LINEAGE_PROJECTION_INVALID"
    )

    overflow_rows = [
        _premarket_ledger_row(
            f"row-{index}",
            rank=1,
            head_hash=head_hash,
            row_hash=f"{index + 1:064x}",
        )
        for index in range(11)
    ]
    overflow_app = create_app(
        _services(
            news_provider=lambda: _ledger_news_payload(
                overflow_rows, [], head_hash=head_hash
            )
        )
    )
    overflow_payload = asyncio.run(_route(overflow_app, "/api/news")())
    assert overflow_payload["pre_market_preselections"] == []
    assert overflow_payload["preselection_coverage"]["status"] == "UNAVAILABLE"


def test_news_top10_crossed_quote_and_leg_overflow_fail_closed() -> None:
    head_hash = "6" * 64
    premarket = _premarket_ledger_row(
        "strict-row", rank=1, head_hash=head_hash, row_hash="7" * 64
    )
    opened = _open_ledger_row(premarket, observation_hash="8" * 64)

    crossed = deepcopy(opened)
    crossed["legs"][0]["bid"] = "9"  # type: ignore[index]
    crossed["legs"][0]["ask"] = "1"  # type: ignore[index]
    crossed_payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        [premarket], [crossed], head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )
    assert crossed_payload["pre_market_preselection_count"] == 1
    assert crossed_payload["open_market_repriced_count"] == 1
    assert crossed_payload["option_action_pool"] == []
    assert crossed_payload["open_market_repriced"][0]["action_pool_eligible"] is False

    overflow = deepcopy(premarket)
    overflow["legs"] = [deepcopy(premarket["legs"][0]) for _ in range(9)]  # type: ignore[index]
    overflow_payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        [overflow], [], head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )
    assert overflow_payload["pre_market_preselections"] == []
    assert overflow_payload["preselection_coverage"]["reason"] == (
        "PRESELECTION_LINEAGE_PROJECTION_INVALID"
    )


def test_news_projection_accepts_a_serialized_analysis_record() -> None:
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [{
                    "analysis_id": "analysis:fed", "stage": "MARKET_CONFIRMED",
                    "news": {
                        "event_id": "fed", "headline": "Fed decision", "summary": "Statement",
                        "source": "Jin10", "symbols": ["SPY"],
                        "published_at": "2026-08-04T08:00:00Z", "first_seen_at": "2026-08-04T08:00:01Z",
                    },
                    "classification": {"category": "FOMC"},
                    "event_impact_score": "70.25", "option_tradability_score": "80.50",
                    "combined_opportunity_score": "75.38",
                }]
            }
        )
    )

    item = asyncio.run(_route(app, "/api/news")())["news"][0]

    assert item["id"] == "analysis:fed"
    assert item["title"] == "Fed decision"
    assert item["scores"]["combined_opportunity_score"] == 75.38


def _premarket_ledger_row(
    identifier: str,
    *,
    rank: int,
    head_hash: str,
    row_hash: str,
) -> dict[str, object]:
    leg = {
        "underlying": "AAPL",
        "con_id": 101,
        "expiry": "2026-08-21",
        "strike": "225",
        "right": "CALL",
        "side": "BUY",
        "ratio": 1,
        "quantity": 1,
        "bid": "2.10",
        "ask": "2.16",
        "quote_asof": "2026-08-05T13:00:00+00:00",
        "quote_batch_id": "premarket-batch",
        "implied_volatility": "0.31",
        "delta": "0.42",
        "gamma": "0.021",
        "theta": "-0.08",
        "vega": "0.11",
        "volume": 240,
        "open_interest": 1800,
        "dte": 16,
    }
    strategy_hash = canonical_hash(
        {
            "schema": "options_copilot.conditional_option_strategy.v1",
            "underlying": "AAPL",
            "strategy_type": "LONG_CALL",
            "legs": [
                {
                    "underlying": "AAPL",
                    "con_id": 101,
                    "expiry": date(2026, 8, 21),
                    "strike": Decimal("225"),
                    "right": "CALL",
                    "side": "BUY",
                    "ratio": 1,
                    "quantity": 1,
                }
            ],
        }
    )
    return {
        "preselection_id": identifier,
        "underlying": "AAPL",
        "strategy_type": "LONG_CALL",
        "phase": "PRE_MARKET",
        "research_rank": rank,
        "risk_defined": True,
        "maximum_loss_usd": "200",
        "estimated_cost_usd": "200",
        "cost_after_ev_usd": "20",
        "entry_condition": "Research only.",
        "invalidation_condition": "Thesis invalidated.",
        "profit_target_condition": "Human review.",
        "stop_loss_condition": "Human review.",
        "research_summary": "Independent ledger fixture.",
        "strategy_hash": strategy_hash,
        "evidence_ids": ["evidence-1"],
        "evidence_hashes": ["f" * 64],
        "quote_batch_id": "premarket-batch",
        "oldest_quote_asof": "2026-08-05T13:00:00+00:00",
        "maximum_quote_age_seconds": 1.0,
        "legs": [leg],
        "ledger_lineage": {
            "source": "INDEPENDENT_TOP10_LEDGER",
            "source_batch_purpose": None,
            "source_batch_id": None,
            "source_batch_hash": None,
            "preselection_id": identifier,
            "phase": "PRE_MARKET",
            "run_id": "run-1",
            "run_created_at": "2026-08-05T13:00:00+00:00",
            "head_hash": head_hash,
            "row_id": f"row-{rank}",
            "row_hash": row_hash,
            "premarket_rank": rank,
            "production_parent_eligible": False,
            "production_parent_blocker": "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE",
        },
    }


def _open_ledger_row(
    premarket: dict[str, object],
    *,
    observation_hash: str,
) -> dict[str, object]:
    opened = deepcopy(premarket)
    lineage = deepcopy(premarket["ledger_lineage"])
    assert isinstance(lineage, dict)
    lineage.update(
        {
            "phase": "OPEN_REPRICED",
            "source_batch_purpose": "OPEN_REPRICE",
            "source_batch_id": "ibkr-open-batch-1",
            "source_batch_hash": "b" * 64,
            "observation_id": f"observation-{premarket['preselection_id']}",
            "observed_at": "2026-08-05T13:00:00+00:00",
            "observation_hash": observation_hash,
            "batch_id": "open-batch-1",
            "batch_head_hash": "2" * 64,
            "scheduled_for": "2026-08-05T13:00:00+00:00",
            "quote_batch_id": "ibkr-open-batch-1",
        }
    )
    lineage.pop("production_parent_eligible", None)
    lineage.pop("production_parent_blocker", None)
    rank = int(premarket["research_rank"])
    legs = deepcopy(premarket["legs"])
    assert isinstance(legs, list)
    for leg in legs:
        assert isinstance(leg, dict)
        leg["quote_batch_id"] = "ibkr-open-batch-1"
    opened.update(
        {
            "phase": "OPEN_REPRICED",
            "research_rank": None,
            "repriced_rank": rank,
            "action_rank": rank if rank <= 3 else None,
            "action_pool_eligible": rank <= 3,
            "research_only": rank > 3,
            "risk_adjusted_ev": "0.1",
            "blockers": [],
            "quote_batch_id": "ibkr-open-batch-1",
            "legs": legs,
            "ledger_lineage": lineage,
        }
    )
    _bind_open_economics(opened)
    return opened


def _bind_direct_underlying_quote_basis(row: dict[str, object]) -> dict[str, object]:
    basis = {
        "symbol": "AAPL",
        "contract_id": 265598,
        "exchange": "SMART",
        "source": "IBKR_REQ_TICKERS_READONLY",
        "observed_at": "2026-08-05T13:00:00+00:00",
        "bid": "224.90",
        "ask": "225.10",
        "last": "225.00",
        "close": "223.50",
        "market_data_type": 1,
        "schema": "options_copilot.underlying_quote_basis.v1",
    }
    basis_hash = canonical_hash(basis)
    evidence_ids = row["evidence_ids"]
    evidence_hashes = row["evidence_hashes"]
    assert isinstance(evidence_ids, list)
    assert isinstance(evidence_hashes, list)
    evidence_ids.append("IBKR_DIRECT_DISCOVERY:AAPL:2026-08-21")
    evidence_hashes.append(basis_hash)
    row["underlying_quote_basis"] = basis
    row["underlying_quote_basis_hash"] = basis_hash
    return row


def _bind_open_economics(opened: dict[str, object]) -> None:
    candidate_id = str(opened["preselection_id"])
    strategy_hash = str(opened["strategy_hash"])
    quote_asof = datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc)
    scenario_asof = datetime(2026, 8, 5, 12, 59, tzinfo=timezone.utc)
    scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=candidate_id,
        strategy_hash=strategy_hash,
        scenario_asof=scenario_asof,
        scenarios=(
            TrustedTerminalScenario(Decimal("220"), Decimal("0.5")),
            TrustedTerminalScenario(Decimal("230"), Decimal("0.5")),
        ),
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )
    snapshot_hash = "c" * 64
    nav = Decimal("10000")
    nav_hash = strategy_nav_post_hash(
        candidate_id=candidate_id,
        strategy_hash=strategy_hash,
        snapshot_hash=snapshot_hash,
        strategy_nav_usd=nav,
    )
    provisional = OpenRepriceEconomics(
        candidate_id=candidate_id,
        strategy_hash=strategy_hash,
        broker_snapshot_hash=snapshot_hash,
        quote_batch_id="ibkr-open-batch-1",
        quote_asof=quote_asof,
        scenario_hash=scenario_set.scenario_hash,
        scenario_asof=scenario_asof,
        cost_contract_version=EXECUTION_COST_VERSION,
        cost_contract_hash=EXECUTION_COST_HASH,
        policy_version=INITIAL_POLICY_VERSION,
        policy_hash=INITIAL_POLICY_HASH,
        strategy_nav_usd=nav,
        strategy_nav_post_hash=nav_hash,
        debit_usd=Decimal("216"),
        credit_usd=Decimal("0"),
        commission_usd=Decimal("2.5"),
        entry_slippage_usd=Decimal("2.5"),
        exit_slippage_usd=Decimal("5"),
        total_slippage_usd=Decimal("7.5"),
        all_in_cost_usd=Decimal("226"),
        maximum_loss_usd=Decimal("226"),
        before_cost_expected_value_usd=Decimal("30"),
        after_cost_expected_value_usd=Decimal("20"),
        payoff_hash="d" * 64,
        risk_fraction=Decimal("0.0226"),
        economics_hash="0" * 64,
    )
    economics_hash = canonical_hash(provisional.hash_payload())
    opened.update(
        {
            "maximum_loss_usd": "226",
            "estimated_cost_usd": "226",
            "cost_after_ev_usd": "20",
            "terminal_scenarios": [
                {
                    "terminal_underlying_price": "220",
                    "probability": "0.5",
                },
                {
                    "terminal_underlying_price": "230",
                    "probability": "0.5",
                },
            ],
            "scenario_asof": scenario_asof.isoformat(),
            "scenario_hash": scenario_set.scenario_hash,
            "execution_cost_contract_version": EXECUTION_COST_VERSION,
            "execution_cost_contract_hash": EXECUTION_COST_HASH,
            "risk_policy_version": INITIAL_POLICY_VERSION,
            "risk_policy_hash": INITIAL_POLICY_HASH,
            "broker_snapshot_hash": snapshot_hash,
            "strategy_nav_usd": "10000",
            "strategy_nav_post_hash": nav_hash,
            "economics_quote_batch_id": "ibkr-open-batch-1",
            "economics_quote_asof": quote_asof.isoformat(),
            "payoff_hash": "d" * 64,
            "economics_calculation_hash": economics_hash,
            "debit_usd": "216",
            "credit_usd": "0",
            "net_entry_cost_usd": "226",
            "estimated_commission_usd": "2.5",
            "estimated_entry_slippage_usd": "2.5",
            "estimated_exit_slippage_usd": "5",
            "estimated_slippage_usd": "7.5",
            "expected_value_before_costs_usd": "30",
            "risk_fraction": "0.0226",
        }
    )


def _ledger_news_payload(
    premarket: list[dict[str, object]],
    opened: list[dict[str, object]],
    *,
    head_hash: str,
) -> dict[str, object]:
    has_complete_open_set = bool(premarket) and len(opened) == len(premarket)
    status = (
        "UNAVAILABLE"
        if not premarket
        else "AVAILABLE"
        if has_complete_open_set and len(premarket) == 10
        else "PARTIAL"
    )
    reason = (
        "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
        if not premarket
        else "OPEN_REPRICE_NOT_STARTED"
        if not opened
        else "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
        if has_complete_open_set and len(premarket) < 10
        else "OPEN_REPRICE_ATOMIC_SET_INCOMPLETE"
        if not has_complete_open_set
        else None
    )
    return {
        "news": [],
        "pre_market_preselections": premarket,
        "open_market_repriced": opened,
        "preselection_coverage": {
            "source": "INDEPENDENT_TOP10_LEDGER",
            "status": status,
            "reason": reason,
            "ledger_reason": (
                "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
                if has_complete_open_set and len(premarket) < 10
                else None
            ),
            "requested_count": 10,
            "available_count": len(premarket),
            "open_count": len(opened),
            "latest_run_id": "run-1",
            "latest_head_hash": head_hash,
            "freeze_slot": "2026-08-05T13:00:00+00:00",
            "latest_open_batch_id": "open-batch-1" if opened else None,
            "latest_open_batch_head_hash": "2" * 64 if opened else None,
            "reprice_slot": "2026-08-05T13:00:00+00:00" if opened else None,
            "open_reprice_producer_status": (
                "AVAILABLE"
                if has_complete_open_set
                else "NOT_STARTED"
                if premarket and not opened
                else "UNAVAILABLE"
            ),
        },
    }


def _v2_premarket_ledger_row(
    identifier: str,
    *,
    rank: int,
    head_hash: str,
    row_hash: str,
    schema: str = "options_copilot.conditional_option_strategy.v2",
) -> dict[str, object]:
    row = _premarket_ledger_row(
        identifier,
        rank=rank,
        head_hash=head_hash,
        row_hash=row_hash,
    )
    lineage = row["ledger_lineage"]
    assert isinstance(lineage, dict)
    lineage.update(
        {
            "source_batch_purpose": "PREMARKET_ACCOUNT",
            "source_batch_id": "premarket-account-batch-1",
            "source_batch_hash": "a" * 64,
        }
    )
    legs = row["legs"]
    assert isinstance(legs, list)
    leg = legs[0]
    assert isinstance(leg, dict)
    leg.update(
        {
            "local_symbol": "AAPL  260821C00225000",
            "trading_class": "AAPL",
            "multiplier": 100,
            "exchange": "SMART",
        }
    )
    row["strategy_hash"] = canonical_hash(
        {
            "schema": schema,
            "underlying": "AAPL",
            "strategy_type": "LONG_CALL",
            "legs": [
                {
                    "con_id": 101,
                    "local_symbol": "AAPL  260821C00225000",
                    "trading_class": "AAPL",
                    "multiplier": 100,
                    "exchange": "SMART",
                    "expiry": date(2026, 8, 21),
                    "strike": Decimal("225"),
                    "right": "CALL",
                    "side": "BUY",
                    "ratio": 1,
                    "quantity": 1,
                }
            ],
        }
    )
    lineage = row["ledger_lineage"]
    assert isinstance(lineage, dict)
    lineage["production_parent_eligible"] = True
    lineage["production_parent_blocker"] = None
    return row


def test_news_top10_v2_hash_projects_exact_identity_fields() -> None:
    head_hash = "9" * 64
    premarket = _v2_premarket_ledger_row(
        "v2-row",
        rank=1,
        head_hash=head_hash,
        row_hash="a" * 64,
    )
    opened = _open_ledger_row(premarket, observation_hash="b" * 64)

    payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        [premarket], [opened], head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )

    assert payload["pre_market_preselection_count"] == 1
    assert payload["open_market_repriced_count"] == 1
    assert payload["option_action_pool_count"] == 1
    assert payload["preselection_coverage"]["status"] == "PARTIAL"
    assert payload["preselection_coverage"]["atomic_batch_available"] is True
    assert payload["preselection_coverage"]["atomic_batch_blocker"] is None
    assert payload["preselection_coverage"]["latest_open_batch_id"] == (
        "open-batch-1"
    )
    assert payload["preselection_coverage"]["latest_open_batch_head_hash"] == (
        "2" * 64
    )
    assert payload["preselection_coverage"]["freeze_slot"] == (
        "2026-08-05T13:00:00+00:00"
    )
    assert payload["preselection_coverage"]["reprice_slot"] == (
        "2026-08-05T13:00:00+00:00"
    )
    projected_leg = payload["pre_market_preselections"][0]["legs"][0]
    assert projected_leg["local_symbol"] == "AAPL  260821C00225000"
    assert projected_leg["trading_class"] == "AAPL"
    assert projected_leg["multiplier"] == 100
    assert projected_leg["exchange"] == "SMART"
    projected_open = payload["open_market_repriced"][0]
    assert projected_open["quote_batch_id"] == "ibkr-open-batch-1"
    assert projected_open["legs"][0]["quote_batch_id"] == "ibkr-open-batch-1"
    assert projected_open["ledger_lineage"]["batch_id"] == "open-batch-1"
    assert projected_open["ledger_lineage"]["batch_head_hash"] == "2" * 64
    assert projected_open["ledger_lineage"]["quote_batch_id"] == (
        "ibkr-open-batch-1"
    )


def test_news_top10_direct_basis_is_hash_bound_before_action_projection() -> None:
    head_hash = "8" * 64
    premarket = _bind_direct_underlying_quote_basis(
        _v2_premarket_ledger_row(
            "direct-basis-row",
            rank=1,
            head_hash=head_hash,
            row_hash="7" * 64,
        )
    )
    opened = _open_ledger_row(premarket, observation_hash="6" * 64)

    payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        [premarket], [opened], head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )

    projected = payload["open_market_repriced"][0]
    assert payload["option_action_pool_count"] == 1
    assert projected["action_pool_eligible"] is True
    assert projected["underlying_quote_basis"] == opened[
        "underlying_quote_basis"
    ]
    assert projected["underlying_quote_basis_hash"] == opened[
        "underlying_quote_basis_hash"
    ]


def test_news_top10_direct_basis_mutations_fail_closed_before_action_projection() -> None:
    mutations = {
        "close": lambda basis: basis.__setitem__("close", "222.00"),
        "source": lambda basis: basis.__setitem__("source", "UNTRUSTED"),
        "contract_id": lambda basis: basis.__setitem__("contract_id", 0),
        "market_data_type": lambda basis: basis.__setitem__("market_data_type", 3),
    }

    for index, mutate in enumerate(mutations.values(), start=1):
        head_hash = f"{index + 20:064x}"
        premarket = _bind_direct_underlying_quote_basis(
            _v2_premarket_ledger_row(
                f"direct-basis-mutation-{index}",
                rank=1,
                head_hash=head_hash,
                row_hash=f"{index + 30:064x}",
            )
        )
        opened = _open_ledger_row(
            premarket,
            observation_hash=f"{index + 40:064x}",
        )
        basis = opened["underlying_quote_basis"]
        assert isinstance(basis, dict)
        mutate(basis)

        payload = asyncio.run(
            _route(
                create_app(
                    _services(
                        news_provider=lambda: _ledger_news_payload(
                            [premarket], [opened], head_hash=head_hash
                        )
                    )
                ),
                "/api/news",
            )()
        )

        projected = payload["open_market_repriced"][0]
        assert payload["option_action_pool"] == []
        assert projected["action_pool_eligible"] is False
        assert projected["action_rank"] is None
        assert "API_FAIL_CLOSED_INCOMPLETE" in projected["blockers"]
        assert projected["underlying_quote_basis"] is None
        assert projected["underlying_quote_basis_hash"] is None


def test_news_top10_external_candidate_remains_eligible_without_direct_basis() -> None:
    head_hash = "5" * 64
    premarket = _v2_premarket_ledger_row(
        "external-no-direct-basis",
        rank=1,
        head_hash=head_hash,
        row_hash="4" * 64,
    )
    opened = _open_ledger_row(premarket, observation_hash="3" * 64)

    payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        [premarket], [opened], head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )

    projected = payload["open_market_repriced"][0]
    assert payload["option_action_pool_count"] == 1
    assert projected["action_pool_eligible"] is True
    assert projected["underlying_quote_basis"] is None
    assert projected["underlying_quote_basis_hash"] is None


def test_news_top10_atomic_batch_fails_closed_on_mixed_batch_or_quote() -> None:
    head_hash = "3" * 64
    premarket = [
        _v2_premarket_ledger_row(
            f"v2-row-{rank}",
            rank=rank,
            head_hash=head_hash,
            row_hash=f"{rank + 10:064x}",
        )
        for rank in (1, 2)
    ]
    opened = [
        _open_ledger_row(item, observation_hash=f"{rank + 20:064x}")
        for rank, item in enumerate(premarket, start=1)
    ]
    second_lineage = opened[1]["ledger_lineage"]
    assert isinstance(second_lineage, dict)
    second_lineage["batch_head_hash"] = "4" * 64
    second_leg = opened[1]["legs"]
    assert isinstance(second_leg, list)
    assert isinstance(second_leg[0], dict)
    second_leg[0]["quote_batch_id"] = "mixed-quote-batch"

    payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        premarket, opened, head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )

    assert payload["pre_market_preselection_count"] == 2
    assert payload["open_market_repriced_count"] == 2
    assert payload["option_action_pool"] == []
    assert payload["preselection_coverage"]["atomic_batch_available"] is False
    assert payload["preselection_coverage"]["status"] == "PARTIAL"
    assert payload["preselection_coverage"]["reason"] in {
        "PRESELECTION_LINEAGE_BINDING_INVALID",
        "OPEN_REPRICE_ATOMIC_BATCH_INCOMPLETE",
    }
    assert all(
        row["action_pool_eligible"] is False
        for row in payload["open_market_repriced"]
    )


def test_news_top10_source_lineage_is_strict_and_quote_bound() -> None:
    head_hash = "4" * 64
    mutations = {
        "half": lambda lineage: lineage.__setitem__("source_batch_hash", None),
        "purpose": lambda lineage: lineage.__setitem__(
            "source_batch_purpose", "PREMARKET_ACCOUNT"
        ),
        "hash": lambda lineage: lineage.__setitem__(
            "source_batch_hash", "not-a-digest"
        ),
        "unknown": lambda lineage: lineage.__setitem__(
            "unknown_source_field", "reject-me"
        ),
        "quote": lambda lineage: lineage.__setitem__(
            "source_batch_id", "different-open-source-batch"
        ),
    }
    for name, mutate in mutations.items():
        premarket = _v2_premarket_ledger_row(
            f"source-{name}",
            rank=1,
            head_hash=head_hash,
            row_hash="5" * 64,
        )
        opened = _open_ledger_row(premarket, observation_hash="6" * 64)
        lineage = opened["ledger_lineage"]
        assert isinstance(lineage, dict)
        mutate(lineage)

        payload = asyncio.run(
            _route(
                create_app(
                    _services(
                        news_provider=lambda pre=premarket, opened_row=opened: (
                            _ledger_news_payload(
                                [pre], [opened_row], head_hash=head_hash
                            )
                        )
                    )
                ),
                "/api/news",
            )()
        )

        assert payload["option_action_pool"] == []
        assert payload["preselection_coverage"]["atomic_batch_available"] is False
        assert payload["preselection_coverage"]["atomic_batch_blocker"] == (
            "PRESELECTION_LINEAGE_BINDING_INVALID"
            if name == "quote"
            else "PRESELECTION_LINEAGE_PROJECTION_INVALID"
        )


def test_news_top10_partial_open_set_and_count_mismatch_remain_research_only() -> None:
    head_hash = "5" * 64
    premarket = [
        _v2_premarket_ledger_row(
            f"partial-row-{rank}",
            rank=rank,
            head_hash=head_hash,
            row_hash=f"{rank + 30:064x}",
        )
        for rank in (1, 2)
    ]
    opened = [_open_ledger_row(premarket[0], observation_hash="6" * 64)]
    partial_payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        premarket, opened, head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )
    assert partial_payload["pre_market_preselection_count"] == 2
    assert partial_payload["open_market_repriced_count"] == 1
    assert partial_payload["option_action_pool"] == []
    assert partial_payload["preselection_coverage"]["atomic_batch_available"] is False

    complete = _ledger_news_payload(
        [premarket[0]],
        [_open_ledger_row(premarket[0], observation_hash="7" * 64)],
        head_hash=head_hash,
    )
    coverage = complete["preselection_coverage"]
    assert isinstance(coverage, dict)
    coverage["available_count"] = 2
    coverage["open_count"] = 2
    mismatch_payload = asyncio.run(
        _route(
            create_app(_services(news_provider=lambda: complete)),
            "/api/news",
        )()
    )
    assert mismatch_payload["pre_market_preselection_count"] == 1
    assert mismatch_payload["open_market_repriced_count"] == 1
    assert mismatch_payload["option_action_pool"] == []
    assert mismatch_payload["preselection_coverage"]["reason"] == (
        "PRESELECTION_COVERAGE_COUNT_MISMATCH"
    )


def test_news_top10_legacy_v1_remains_visible_but_research_only() -> None:
    head_hash = "c" * 64
    premarket = _premarket_ledger_row(
        "legacy-v1-row",
        rank=1,
        head_hash=head_hash,
        row_hash="d" * 64,
    )
    opened = _open_ledger_row(premarket, observation_hash="e" * 64)
    opened.update(
        {
            "action_rank": None,
            "action_pool_eligible": False,
            "research_only": True,
            "blockers": ["LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"],
        }
    )

    payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        [premarket], [opened], head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )

    assert payload["pre_market_preselection_count"] == 1
    assert payload["open_market_repriced_count"] == 1
    assert payload["option_action_pool_count"] == 0
    projected = payload["open_market_repriced"][0]
    assert projected["action_pool_eligible"] is False
    assert projected["research_only"] is True
    assert payload["preselection_coverage"]["atomic_batch_available"] is False
    assert payload["preselection_coverage"]["atomic_batch_blocker"] == (
        "SOURCE_LINEAGE_MISSING_LEGACY"
    )
    assert payload["pre_market_preselections"][0]["ledger_lineage"][
        "source_batch_id"
    ] is None


def test_news_top10_unknown_structure_schema_fails_closed() -> None:
    head_hash = "f" * 64
    unknown = _v2_premarket_ledger_row(
        "unknown-schema-row",
        rank=1,
        head_hash=head_hash,
        row_hash="1" * 64,
        schema="options_copilot.conditional_option_strategy.v3",
    )

    payload = asyncio.run(
        _route(
            create_app(
                _services(
                    news_provider=lambda: _ledger_news_payload(
                        [unknown], [], head_hash=head_hash
                    )
                )
            ),
            "/api/news",
        )()
    )

    assert payload["pre_market_preselections"] == []
    assert payload["open_market_repriced"] == []
    assert payload["option_action_pool"] == []
    assert payload["preselection_coverage"]["reason"] == (
        "PRESELECTION_LINEAGE_PROJECTION_INVALID"
    )


def test_provider_configuration_reports_presence_without_values() -> None:
    raw_marker = "must-never-cross-api"
    app = create_app(
        _services(
            provider_configuration_provider=lambda: {
                "jin10_mcp_token": "DISABLED",
                "finnhub_api_key": "CONFIGURED",
                "alpha_vantage_api_key": "DISABLED",
                "deepseek_api_key": "ERROR",
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/configuration/providers")())

    assert payload == {
        "schema": "options_copilot.provider_configuration.v1",
        "source": "LOCAL_API_KEYS_FILE",
        "providers": [
            {
                "provider": "JIN10",
                "status": "DISABLED",
                "configured": False,
                "activation_required": True,
                "activated": False,
                "composed": False,
                "runtime_loaded": False,
                "restart_required": False,
                "decision_authority": "SUPPORTING_ONLY",
            },
            {
                "provider": "FINNHUB",
                "status": "CONFIGURED",
                "configured": True,
                "activation_required": False,
                "activated": True,
                "composed": False,
                "runtime_loaded": False,
                "restart_required": True,
                "decision_authority": "SUPPORTING_ONLY",
            },
            {
                "provider": "ALPHA_VANTAGE",
                "status": "DISABLED",
                "configured": False,
                "activation_required": False,
                "activated": True,
                "composed": False,
                "runtime_loaded": False,
                "restart_required": False,
                "decision_authority": "SUPPORTING_ONLY",
            },
            {
                "provider": "DEEPSEEK",
                "status": "ERROR",
                "configured": False,
                "activation_required": False,
                "activated": True,
                "composed": False,
                "runtime_loaded": False,
                "restart_required": False,
                "decision_authority": "SUPPORTING_ONLY",
            },
        ],
        "values_exposed": False,
        "read_only": True,
        "decision_authority": "OBSERVATION_ONLY",
    }
    assert raw_marker not in str(payload)
    assert "value" not in {key for row in payload["providers"] for key in row}


def test_provider_configuration_fails_closed_on_unknown_internal_fields() -> None:
    app = create_app(
        _services(provider_configuration_provider=lambda: {"unexpected": "value"})
    )

    payload = asyncio.run(_route(app, "/api/configuration/providers")())

    assert {item["status"] for item in payload["providers"]} == {"ERROR"}
    assert payload["values_exposed"] is False


def test_provider_configuration_projects_runtime_load_and_restart_state() -> None:
    app = create_app(
        _services(
            provider_configuration_provider=lambda: {
                "jin10_mcp_token": {
                    "status": "CONFIGURED",
                    "activated": True,
                    "composed": False,
                    "runtime_loaded": False,
                    "restart_required": True,
                },
                "finnhub_api_key": {
                    "status": "CONFIGURED",
                    "activated": True,
                    "composed": True,
                    "runtime_loaded": True,
                    "restart_required": False,
                },
                "alpha_vantage_api_key": "DISABLED",
                "deepseek_api_key": "DISABLED",
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/configuration/providers")())
    rows = {item["provider"]: item for item in payload["providers"]}

    assert rows["JIN10"]["status"] == "CONFIGURED"
    assert rows["JIN10"]["activated"] is True
    assert rows["JIN10"]["composed"] is False
    assert rows["JIN10"]["runtime_loaded"] is False
    assert rows["JIN10"]["restart_required"] is True
    assert rows["FINNHUB"]["runtime_loaded"] is True


def _services(**overrides: object) -> OptionsCopilotServices:
    values: dict[str, object] = {
        "health_provider": lambda: {},
        "bootstrap_provider": lambda: {},
        "candidates_provider": lambda: [],
        "positions_provider": lambda: [],
        "learning_provider": lambda: {},
    }
    values.update(overrides)
    return OptionsCopilotServices(**values)  # type: ignore[arg-type]


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)
