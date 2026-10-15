"""G034 deterministic intelligence and restart-safe source cadence contracts."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import MethodType

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.news.cadence import SourceCadenceStore, cadence_policy
from options_copilot.news.intelligence import project_event_intelligence
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers import EarningsEvent, NewsEvent


NOW = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)
HASH_A = "a" * 64
HASH_B = "b" * 64


def test_intelligence_uses_structured_precedence_facets_and_no_symbol_invention() -> None:
    projected = project_event_intelligence(
        {
            "title": "Earnings amid regulatory inflation and sanctions",
            "category": "REGULATORY",
            "direction": "BEARISH",
            "horizon": "DAYS_1_3",
            "confidence": 0.8,
            "event_impact_score": 75,
            "symbols": ["AAPL"],
            "symbol_binding": {"status": "UNVERIFIED_LEGACY_PROVIDER"},
        }
    )

    assert projected["primary_category"] == "REGULATORY"
    assert projected["facets"] == ["REGULATORY"]
    assert projected["affected_assets"] == {
        "values": [],
        "binding": "UNVERIFIED_LEGACY_PROVIDER",
        "reason": "SYMBOL_BINDING_UNVERIFIED",
    }
    assert projected["decision_authority"] == "SUPPORTING_ONLY"
    assert projected["action_effect"] == "NONE"
    assert len(projected["intelligence_hash"]) == 64


def test_intelligence_fallback_facets_follow_fixed_precedence_and_unknown_reasons() -> None:
    facets = project_event_intelligence(
        {"title": "Earnings regulator inflation sanctions exchange halt sector update"}
    )
    assert facets["facets"] == [
        "EARNINGS",
        "REGULATORY",
        "MACRO",
        "GEOPOLITICAL",
        "MARKET_STRUCTURE",
        "SECTOR",
    ]
    assert facets["primary_category"] == "EARNINGS"

    unknown = project_event_intelligence({"title": "General update"})
    assert unknown["primary_category"] == "UNKNOWN"
    assert unknown["category_reason"] == "CATEGORY_UNAVAILABLE"
    assert unknown["direction"]["reason"] == "DIRECTION_UNAVAILABLE"
    assert unknown["timing"]["expected"]["reason"] == "EXPECTED_TIME_UNAVAILABLE"
    assert unknown["affected_assets"]["reason"] == "AFFECTED_ASSETS_UNAVAILABLE"

    legacy_other = project_event_intelligence({
        "category": "OTHER",
        "title": "Sector sanctions trigger exchange halt",
    })
    assert legacy_other["facets"] == [
        "GEOPOLITICAL",
        "MARKET_STRUCTURE",
        "SECTOR",
    ]
    assert legacy_other["category_reason"] == "DETERMINISTIC_TEXT_FALLBACK"

    unrecognized = project_event_intelligence({
        "classification": {"category": "LEGACY_MISC"},
        "title": "Geopolitical ceasefire changes sector outlook",
    })
    assert unrecognized["facets"] == ["GEOPOLITICAL", "SECTOR"]
    assert unrecognized["category_reason"] == "DETERMINISTIC_TEXT_FALLBACK"


def test_intelligence_binds_surprise_and_reaction_to_existing_hashes() -> None:
    projected = project_event_intelligence(
        {
            "category": "MACRO",
            "reaction": {
                "status": "READY",
                "event_hash": HASH_A,
                "head_hash": HASH_B,
                "surprise": {
                    "delta": "0.2",
                    "content_hash": "c" * 64,
                    "release_hash": "d" * 64,
                },
            },
        }
    )
    assert projected["surprise"]["reason"] == "HASH_BOUND_REACTION"
    assert projected["surprise"]["event_hash"] == HASH_A
    assert projected["reaction"]["reaction_hash"] == HASH_B


def test_reaction_requires_ready_status_and_chain_hash() -> None:
    ready_without_chain = project_event_intelligence({
        "reaction": {"status": "READY", "event_hash": HASH_A},
    })
    assert ready_without_chain["reaction"]["reason"] == "REACTION_CHAIN_HASH_UNAVAILABLE"

    ready_without_event = project_event_intelligence({
        "reaction": {"status": "READY", "head_hash": HASH_B},
    })
    assert ready_without_event["reaction"]["reason"] == "REACTION_EVENT_HASH_UNAVAILABLE"

    conflicted = project_event_intelligence({
        "reaction": {
            "status": "CONFLICTED",
            "event_hash": HASH_A,
            "head_hash": HASH_B,
        },
    })
    assert conflicted["reaction"]["reason"] == "REACTION_CONFLICTED"

    unavailable = project_event_intelligence({"reaction": {"status": "UNAVAILABLE"}})
    assert unavailable["reaction"]["reason"] == "REACTION_UNAVAILABLE"


def test_cadence_restart_due_skip_failure_retention_and_lane_isolation(tmp_path: Path) -> None:
    path = tmp_path / "cadence.json"
    store = SourceCadenceStore(path)
    store.register("SEC", "NEWS")
    store.register("ALPHA_VANTAGE", "NEWS")
    store.record("SEC", "NEWS", now=NOW, success=False, failure_code="BAD_JSON")
    store.record("ALPHA_VANTAGE", "NEWS", now=NOW, success=True)

    restarted = SourceCadenceStore(path)
    restarted.register("SEC", "NEWS")
    restarted.register("ALPHA_VANTAGE", "NEWS")
    assert restarted.due("SEC", "NEWS", now=NOW + timedelta(seconds=89)) == (
        False,
        "CADENCE_NOT_DUE",
    )
    rows = {
        (row["source_id"], row["source_kind"]): row
        for row in restarted.projections(now=NOW + timedelta(seconds=89))
    }
    assert rows[("SEC", "NEWS")]["failure_code"] == "BAD_JSON"
    assert rows[("SEC", "NEWS")]["last_success"] is None
    assert rows[("SEC", "NEWS")]["skip_count"] == 1
    assert rows[("ALPHA_VANTAGE", "NEWS")]["freshness"] == "CURRENT"
    assert restarted.due("SEC", "NEWS", now=NOW + timedelta(seconds=90))[0] is True
    assert restarted.due(
        "ALPHA_VANTAGE", "NEWS", now=NOW + timedelta(seconds=90)
    )[0] is False
    assert cadence_policy("OFFICIAL_CALENDAR", "OFFICIAL_CALENDAR").failure_retry_seconds == 300


def test_corrupt_cadence_state_fails_closed_without_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "cadence.json"
    path.write_text("{broken", encoding="utf-8")
    store = SourceCadenceStore(path)
    store.register("SEC", "NEWS")

    assert store.corrupt is True
    assert store.due("SEC", "NEWS", now=NOW) == (
        False,
        "CADENCE_STATE_CORRUPT",
    )
    assert store.projections(now=NOW)[0]["failure_code"] == "CADENCE_STATE_CORRUPT"
    assert path.read_text(encoding="utf-8") == "{broken"


def test_structurally_corrupt_cadence_discards_partial_rows_without_projection_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cadence.json"
    path.write_text(
        json.dumps({
            "schema": "options_copilot.source_cadence.v1",
            "sources": {
                "SEC:NEWS": {
                    "source_kind": "NEWS",
                    "configured": True,
                    "interval_seconds": 90,
                },
            },
        }),
        encoding="utf-8",
    )

    store = SourceCadenceStore(path)
    assert store.corrupt is True
    assert store.projections(now=NOW) == [{
        "schema": "options_copilot.source_cadence.v1",
        "source_id": "ALL",
        "source_kind": "ALL",
        "configured": False,
        "authority": "SUPPORTING_ONLY",
        "cadence_status": "SUPPRESSED",
        "freshness": "UNAVAILABLE",
        "failure_code": "CADENCE_STATE_CORRUPT",
        "last_attempt": None,
        "last_success": None,
        "next_due": None,
        "attempt_count": 0,
        "success_count": 0,
        "skip_count": 0,
    }]


def test_runtime_cadence_suppresses_only_not_due_lane_across_restart(tmp_path: Path) -> None:
    current = {"now": NOW}

    class SecCurrent8KProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            self.calls += 1
            return ()

    class AlphaVantageProvider(SecCurrent8KProvider):
        pass

    sec = SecCurrent8KProvider()
    alpha = AlphaVantageProvider()
    evidence = tmp_path / "news.sqlite3"
    cadence = tmp_path / "cadence.json"
    runtime = NewsCoordinator(
        evidence,
        news_providers=(sec, alpha),
        clock=lambda: current["now"],
        cadence_path=cadence,
    )
    try:
        runtime.refresh_once()
        runtime.refresh_once()
        assert (sec.calls, alpha.calls) == (1, 1)
        current["now"] += timedelta(seconds=90)
        runtime.refresh_once()
        assert (sec.calls, alpha.calls) == (2, 1)
    finally:
        runtime.close()

    restarted_sec = SecCurrent8KProvider()
    restarted_alpha = AlphaVantageProvider()
    restarted = NewsCoordinator(
        evidence,
        news_providers=(restarted_sec, restarted_alpha),
        clock=lambda: current["now"],
        cadence_path=cadence,
    )
    try:
        restarted.refresh_once()
        assert (restarted_sec.calls, restarted_alpha.calls) == (0, 0)
        runtime_rows = restarted.news_payload()["source_runtime"]
        assert {row["freshness"] for row in runtime_rows} <= {"CURRENT", "NEVER"}
    finally:
        restarted.close()


def test_cadence_restart_marks_removed_provider_suppressed_and_preserves_history(
    tmp_path: Path,
) -> None:
    class SecCurrent8KProvider:
        health = "READY"
        health_reason = None

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            return ()

    class AlphaVantageProvider(SecCurrent8KProvider):
        pass

    evidence = tmp_path / "news.sqlite3"
    cadence = tmp_path / "cadence.json"
    first = NewsCoordinator(
        evidence,
        news_providers=(SecCurrent8KProvider(), AlphaVantageProvider()),
        clock=lambda: NOW,
        cadence_path=cadence,
    )
    try:
        first.refresh_once()
    finally:
        first.close()

    restarted = NewsCoordinator(
        evidence,
        news_providers=(SecCurrent8KProvider(),),
        clock=lambda: NOW + timedelta(seconds=1),
        cadence_path=cadence,
    )
    try:
        rows = {
            (row["source_id"], row["source_kind"]): row
            for row in restarted.news_payload()["source_runtime"]
        }
        removed = rows[("ALPHA_VANTAGE", "NEWS")]
        assert removed["configured"] is False
        assert removed["cadence_status"] == "SUPPRESSED"
        assert removed["last_attempt"] == NOW.isoformat()
        assert removed["last_success"] == NOW.isoformat()
        assert removed["attempt_count"] == 1
        assert rows[("SEC", "NEWS")]["configured"] is True
    finally:
        restarted.close()


def test_runtime_corrupt_cadence_suppresses_provider_call(tmp_path: Path) -> None:
    class SecCurrent8KProvider:
        health = "READY"
        calls = 0

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            self.calls += 1
            return ()

    cadence = tmp_path / "cadence.json"
    cadence.write_text("not-json", encoding="utf-8")
    provider = SecCurrent8KProvider()
    runtime = NewsCoordinator(
        tmp_path / "news.sqlite3",
        news_providers=(provider,),
        clock=lambda: NOW,
        cadence_path=cadence,
    )
    try:
        runtime.refresh_once()
        assert provider.calls == 0
        assert runtime.news_payload()["source_runtime"][0]["failure_code"] == "CADENCE_STATE_CORRUPT"
    finally:
        runtime.close()


def test_skipped_calendar_lane_retains_generation_without_fake_freshness(tmp_path: Path) -> None:
    current = {"now": NOW}

    class NasdaqCalendarProvider:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.calls = 0

        def earnings_calendar(self, _start, _end):
            self.calls += 1
            return (
                EarningsEvent(
                    event_id="event-1",
                    symbol="AAPL",
                    report_date=NOW.date() + timedelta(days=1),
                    hour="amc",
                    eps_estimate=None,
                    revenue_estimate=None,
                    source="nasdaq",
                    first_seen_at=NOW,
                    ingested_at=NOW,
                    observed_at=NOW,
                ),
            )

    provider = NasdaqCalendarProvider()
    runtime = NewsCoordinator(
        tmp_path / "news.sqlite3",
        calendar_providers=(provider,),
        clock=lambda: current["now"],
        cadence_path=tmp_path / "cadence.json",
    )
    try:
        runtime.refresh_once()
        first = runtime.calendar_payload()
        first_row = next(row for row in first["calendar"] if row["id"] == "event-1")
        first_asof = first["provider"]["asof"]
        first_envelope = first_row["calendar_envelope_hash"]

        current["now"] += timedelta(seconds=90)
        runtime.refresh_once()
        skipped = runtime.calendar_payload()
        skipped_row = next(row for row in skipped["calendar"] if row["id"] == "event-1")

        assert provider.calls == 1
        assert skipped["provider"]["asof"] == first_asof
        assert skipped_row["current_generation"] is True
        assert skipped_row["calendar_envelope_hash"] == first_envelope
    finally:
        runtime.close()


def test_coordinator_deepseek_on_off_preserves_deterministic_pools_and_decision_input(
    tmp_path: Path,
) -> None:
    events = (
        NewsEvent(
            event_id="event-a",
            symbol="AAPL",
            source="fixture",
            headline="AAPL earnings beat expectations",
            summary="Quarterly results",
            url="https://example.test/a",
            published_at=NOW - timedelta(minutes=2),
            first_seen_at=NOW - timedelta(minutes=1),
            ingested_at=NOW,
            observed_at=NOW,
            source_rank=1,
        ),
        NewsEvent(
            event_id="event-b",
            symbol="MSFT",
            source="fixture",
            headline="MSFT product launch",
            summary="Company update",
            url="https://example.test/b",
            published_at=NOW - timedelta(minutes=2),
            first_seen_at=NOW - timedelta(minutes=1),
            ingested_at=NOW,
            observed_at=NOW,
            source_rank=1,
        ),
    )

    class FixtureNewsProvider:
        health = "READY"
        health_reason = None

        def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
            return events

    runtime = NewsCoordinator(
        tmp_path / "news.sqlite3",
        news_providers=(FixtureNewsProvider(),),
        core_symbols=("AAPL", "MSFT"),
        clock=lambda: NOW,
    )

    def shadow_overlay(self, analyses, research_pool, *, now, allow_model_calls):
        del self, research_pool, now, allow_model_calls
        return (
            {
                analysis.news.event_id: {
                    "research_priority_score": "99" if index == 1 else "1",
                    "shadow_prediction_count": 1,
                    "decision_authority": "SUPPORTING_ONLY",
                }
                for index, analysis in enumerate(analyses)
            },
            {
                "status": "READY",
                "reason": None,
                "advisory_count": len(analyses),
                "decision_authority": "SUPPORTING_ONLY",
            },
        )

    try:
        runtime.refresh_once()
        off_payload = runtime.news_payload()
        off_decision = runtime.decision_event_payload()
        runtime._shadow_advisory_overlays = MethodType(  # type: ignore[method-assign]
            shadow_overlay,
            runtime,
        )
        runtime._rebuild_read_model(asof=NOW, analysis_budget=0)
        on_payload = runtime.news_payload()
        deterministic_keys = (
            "id",
            "symbols",
            "research_pool",
            "research_rank",
            "watch_rank",
            "action_rank",
        )
        assert [
            {key: row.get(key) for key in deterministic_keys}
            for row in off_payload["news"]
        ] == [
            {key: row.get(key) for key in deterministic_keys}
            for row in on_payload["news"]
        ]
        assert off_payload["research_pool_count"] == on_payload["research_pool_count"]
        assert off_payload["action_pool_count"] == on_payload["action_pool_count"]
        assert any(row["shadow_suggested_rank"] is not None for row in on_payload["news"])
        assert off_decision == runtime.decision_event_payload()
    finally:
        runtime.close()


def test_api_sanitizes_intelligence_cadence_and_keeps_shadow_rank_non_authoritative() -> None:
    services = OptionsCopilotServices(
        health_provider=lambda: {},
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: [],
        positions_provider=lambda: [],
        learning_provider=lambda: {},
        approval_handler=None,
        approval_status_provider=None,
        news_provider=lambda: {
            "news": [{
                "id": "event-1",
                "title": "AAPL earnings",
                "category": "EARNINGS",
                "symbols": ["AAPL", "../../secret"],
                "symbol_binding": {
                    "status": "VERIFIED_PROVIDER_RELATED",
                    "provider_adapter": "FINNHUB",
                },
                "research_rank": 2,
                "shadow_suggested_rank": 1,
                "action_pool": False,
            }],
            "source_runtime": [{
                "source_id": "SEC",
                "source_kind": "NEWS",
                "configured": True,
                "cadence_status": "WAITING",
                "freshness": "CURRENT",
                "interval_seconds": 90,
                "last_success": NOW.isoformat(),
                "next_due": (NOW + timedelta(seconds=90)).isoformat(),
                "failure_code": None,
            }],
        },
    )
    app = create_app(services)
    endpoint = next(route.endpoint for route in app.routes if route.path == "/api/news")
    payload = asyncio.run(endpoint())

    row = payload["news"][0]
    assert row["symbols"] == ["AAPL"]
    assert row["deterministic_research_rank"] == 2
    assert row["shadow_suggested_rank"] == 1
    assert row["rank_displacement"] == -1
    assert row["shadow_action_effect"] == "NONE"
    assert row["shadow_risk_effect"] == "NONE"
    assert row["shadow_eligibility_effect"] == "NONE"
    assert row["intelligence"]["primary_category"] == "EARNINGS"
    assert payload["source_runtime"][0]["authority"] == "SUPPORTING_ONLY"
    json.dumps(payload)


def test_frontend_exposes_important_first_intelligence_and_cadence_without_inner_html() -> None:
    app_js = (
        Path(__file__).resolve().parents[2]
        / "options_copilot"
        / "frontend"
        / "app.js"
    ).read_text(encoding="utf-8")

    assert "normalizeSourceRuntime" in app_js
    assert "intelligenceSummary" in app_js
    assert "最近成功" in app_js
    assert "事件智能" in app_js
    assert "innerHTML" not in app_js
