from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess

import pytest
from fastapi import HTTPException
from fastapi.responses import FileResponse
from pydantic import ValidationError

from options_copilot.api import (
    APPROVAL_CONFIRMATION_TOKEN,
    AfterHoursIndicativeRequest,
    ApprovalConfirmationRequest,
    OptionsCopilotServices,
    RankOneChallengeRequest,
    create_app,
)
from options_copilot.api.app import (
    _normalise_bootstrap,
    _normalise_daily_operations,
    _normalise_ranking,
)
from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.news.open_reprice_economics import (
    OpenRepriceEconomics,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)
from options_copilot.research_allocation import build_research_allocation_evidence
from options_copilot.storage.canonical import canonical_hash


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"
APPROVAL_CHALLENGE = "challenge-0123456789abcdef0123456789abcdef"


def test_after_hours_indicative_api_strips_any_claimed_trade_authority() -> None:
    app = create_app(
        _services(
            after_hours_indicative_provider=lambda: {
                "status": "AVAILABLE",
                "decision": "BUY_NOW",
                "approval_eligible": True,
                "instruction_creation_allowed": True,
                "order_allowed": True,
                "candidates": [
                    {
                        "research_id": "research-1",
                        "trade_status": "READY",
                        "decision_authority": "PRODUCTION",
                        "approval_eligible": True,
                        "instruction_creation_allowed": True,
                        "order_allowed": True,
                    }
                ],
            }
        )
    )

    payload = asyncio.run(
        _route(app, "/api/research-top10/indicative")(
            AfterHoursIndicativeRequest(
                confirmation_token="READ_AFTER_HOURS_OPTION_MARKS"
            )
        )
    )

    assert payload["decision"] == "NO_TRADE"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_allowed"] is False
    assert payload["review_only"] is True
    assert payload["direct_order_submission"] is False
    candidate = payload["candidates"][0]
    assert candidate["trade_status"] == "NO_TRADE"
    assert candidate["decision_authority"] == "SUPPORTING_ONLY"
    assert candidate["approval_eligible"] is False
    assert candidate["instruction_creation_allowed"] is False
    assert candidate["order_allowed"] is False


def test_after_hours_indicative_get_reads_cache_without_invoking_quote_provider() -> None:
    calls = {"cache": 0, "quotes": 0}

    def cache_provider() -> dict[str, object]:
        calls["cache"] += 1
        return {
            "status": "AVAILABLE",
            "observed_at": "2026-08-24T00:00:00+00:00",
            "reason_codes": ["PREVIOUS_CLOSE_NON_EXECUTABLE"],
            "candidates": [],
        }

    def quote_provider() -> dict[str, object]:
        calls["quotes"] += 1
        raise AssertionError("passive GET must not invoke the broker quote provider")

    app = create_app(
        _services(
            after_hours_indicative_provider=quote_provider,
            after_hours_latest_provider=cache_provider,
        )
    )
    route = next(
        route.endpoint
        for route in app.routes
        if route.path == "/api/research-top10/indicative"
        and "GET" in (route.methods or set())
    )

    payload = asyncio.run(route())

    assert calls == {"cache": 1, "quotes": 0}
    assert payload["decision"] == "NO_TRADE"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_allowed"] is False


def test_gui_auto_refresh_reads_after_hours_cache_and_post_requires_click() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    passive_fetch = script.split(
        "async function fetchAfterHoursIndicative()",
        1,
    )[1].split("async function readAfterHoursIndicative()", 1)[0]
    explicit_read = script.split(
        "async function readAfterHoursIndicative()",
        1,
    )[1].split("function renderAfterHoursIndicative", 1)[0]
    refresh_all = script.split("async function refreshAll()", 1)[1].split(
        "function renderAll",
        1,
    )[0]
    refresh_news = script.split("async function refreshNewsData()", 1)[1].split(
        "function renderFundamentals",
        1,
    )[0]

    assert "fetchResearchJson(RESEARCH_TOP10_ROUTES.afterHoursIndicative)" in passive_fetch
    assert 'method: "POST"' not in passive_fetch
    assert "fetchAfterHoursIndicative()" in refresh_all
    assert "fetchAfterHoursIndicative()" in refresh_news
    assert "readAfterHoursIndicative()" not in refresh_all
    assert "readAfterHoursIndicative()" not in refresh_news
    assert 'method: "POST"' in explicit_read
    assert 'confirmation_token: "READ_AFTER_HOURS_OPTION_MARKS"' in explicit_read
    assert (
        'byId("read-after-hours-marks").addEventListener("click", readAfterHoursIndicative);'
        in script
    )


def test_empty_ranking_cannot_advertise_approval_authority() -> None:
    payload = _normalise_ranking(
        {
            "decision": "CANDIDATES_AVAILABLE",
            "approval_enabled": True,
            "candidates": [],
        }
    )

    assert payload["decision"] == "NO_TRADE"
    assert payload["approval_enabled"] is False
    assert payload["candidates"] == []


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


def _phase2_read_services(**overrides: object) -> OptionsCopilotServices:
    fields = set(OptionsCopilotServices.__dataclass_fields__)
    required = {"advisory_provider", "source_evidence_provider"}
    missing = sorted(required - fields)
    if missing:
        _source_api_contract_red("missing API service ports: " + ",".join(missing))
    return _services(**overrides)


def test_dependency_injected_api_exposes_complete_review_only_contract() -> None:
    research_allocation = build_research_allocation_evidence(
        event_rows=(),
        scanner_rows=(),
        core_rows=(),
        limit=10,
    )

    async def candidates_provider() -> dict[str, object]:
        return {
            "asof": "2026-08-03T02:00:00Z",
            "candidates": [
                {"id": f"proposal-{index}", "underlying": symbol}
                for index, symbol in enumerate(("SPY", "QQQ", "IWM", "GLD"), start=1)
            ],
        }

    async def approval_status(approval_id: str) -> dict[str, object]:
        assert approval_id == "approval-7"
        return {
            "status": "READY_FOR_IBKR_REVIEW",
            "expires_at": "2026-08-03T02:05:00Z",
            "instruction_id": "review-7",
            "ibkr_deep_link": "https://chatgpt.com/connector/ibkr/review/review-7",
            "redirect_provenance": "TRUSTED_NO_REDIRECT",
            "review_only": True,
            "order_submitted": False,
            "transmitted_to_broker": False,
        }

    services = OptionsCopilotServices(
        health_provider=lambda: {
            "ibkr": {
                "status": "UP",
                "age_ms": 12,
                "paper": True,
                "account_id": "U1234567",
                "api_token": "must-not-leak",
            },
            "scanner": {"status": "READY", "stale": False},
        },
        health_summary_provider=lambda: {
            "ibkr": {"status": "DEGRADED", "stale": True},
            "production_scanner": {
                "status": "READY",
                "control_snapshot": {
                    "status": "CURRENT",
                    "stale": False,
                    "observed_at": "2026-08-03T02:00:00Z",
                    "age_ms": 321,
                    "positions_count": 0,
                    "working_order_count": 0,
                    "unsubmitted_instruction_count": 0,
                    "account_id": "must-not-leak",
                },
            },
        },
        bootstrap_provider=lambda: {
            "campaign": {
                "current_nlv_usd": Decimal("2012.44"),
                "strategy_nav_usd": Decimal("2121.24"),
            },
            "account": {
                "account_id": "U1234567",
                "net_liquidation_usd": Decimal("9999.0"),
                "api_token": "must-not-leak",
            },
            "safety": {"direct_order_submission": True},
        },
        candidates_provider=candidates_provider,
        positions_provider=lambda: [
            {
                "underlying": "GLD",
                "legacy": True,
                "quantity": 1,
            }
        ],
        learning_provider=lambda: {
            "champion": {"name": "baseline-v1"},
            "challenger": {"name": "shadow-v2"},
            "gate_status": "SHADOW_ONLY",
        },
        approval_status_provider=approval_status,
        latest_scan_provider=lambda: {
            "scan_run_id": "scan-7",
            "decision": "NO_TRADE",
            "funnel_trace": {"research_allocation": research_allocation},
        },
        latest_ranking_provider=lambda: {
            "scan_run_id": "scan-7",
            "ranking_snapshot_id": "ranking-7",
            "approval_enabled": False,
            "decision": "NO_TRADE",
            "candidates": [],
        },
    )
    app = create_app(services)

    health = _run(_route(app, "/health")())
    health_summary = _run(_route(app, "/api/health/summary")())
    bootstrap = _run(_route(app, "/api/bootstrap")())
    candidates = _run(_route(app, "/api/candidates")())
    positions = _run(_route(app, "/api/positions")())
    learning = _run(_route(app, "/api/learning")())
    scan = _run(_route(app, "/api/scans/latest")())
    ranking = _run(_route(app, "/api/rankings/latest")())
    with pytest.raises(HTTPException) as unavailable_destination:
        _run(_route(app, "/api/approvals/{approval_id}")("approval-7"))

    assert health["status"] == "UP"
    assert health_summary == {
        "status": "DEGRADED",
        "dependencies": {
            "ibkr": {"status": "DEGRADED", "stale": True},
            "production_scanner": {
                "status": "READY",
                "control_snapshot": {
                    "status": "CURRENT",
                    "stale": False,
                    "observed_at": "2026-08-03T02:00:00+00:00",
                    "age_ms": 321,
                    "positions_count": 0,
                    "working_order_count": 0,
                    "unsubmitted_instruction_count": 0,
                    "decision_authority": "OBSERVATION_ONLY",
                    "review_only": True,
                    "direct_order_submission": False,
                },
            },
        },
    }
    assert "account_id" not in str(health_summary)
    assert health["dependencies"]["ibkr"] == {
        "status": "UP",
        "age_ms": 12,
        "paper": True,
    }
    assert "account_id" not in str(health)
    assert "api_token" not in str(health)
    assert bootstrap["campaign"]["target_nlv_usd"] == 10_000.0
    assert bootstrap["campaign"]["strategy_nav_usd"] == 2_121.24
    assert "current_nlv_usd" not in bootstrap["campaign"]
    assert bootstrap["account"] == {
        "account_masked": "U1••••67",
        "net_liquidation_usd": 9_999.0,
        "status": None,
        "connected": None,
        "reconciled": None,
        "reconciliation_status": "UNAVAILABLE",
        "strategy_nav_usd": 2_121.24,
        "strategy_nav_asof": None,
        "reconciliation_difference_usd": None,
        "strategy_nav_content_hash": None,
        "strategy_nav_authority_hash": None,
        "strategy_nav_contract_hash": None,
        "strategy_nav_ledger_head_hash": None,
        "market_data_status": None,
        "observed_at": None,
        "decision_authority": None,
    }
    assert "api_token" not in str(bootstrap)
    assert bootstrap["safety"] == {
        "review_only": True,
        "direct_order_submission": False,
        "approval_confirmation_required": True,
        "max_candidates": 10,
    }
    assert [item["underlying"] for item in candidates["candidates"]] == [
        "SPY",
        "QQQ",
        "IWM",
        "GLD",
    ]
    assert candidates["decision"] == "CANDIDATES_AVAILABLE"
    assert candidates["count"] == 4
    assert candidates["total_count"] == 4
    assert candidates["truncated"] is False
    assert positions["positions"][0]["underlying"] == "GLD"
    assert learning["challenger"]["name"] == "shadow-v2"
    assert scan["scan_run_id"] == "scan-7"
    assert scan["decision"] == "NO_TRADE"
    assert scan["funnel_trace"]["research_allocation"] == research_allocation
    assert ranking["decision"] == "NO_TRADE"
    assert unavailable_destination.value.status_code == 502
    assert "destination contract is unavailable" in str(
        unavailable_destination.value.detail
    )
    paths = {route.path for route in app.routes}
    assert paths >= {
        "/",
        "/health",
        "/api/bootstrap",
        "/api/candidates",
        "/api/positions",
        "/api/learning",
        "/api/scans/latest",
        "/api/scans/campaign",
        "/api/rankings/latest",
        "/api/rankings/{ranking_snapshot_id}",
        "/api/scans/{scan_run_id}/candidates/{candidate_id}/evidence",
        "/api/management/current",
        "/api/positioning",
        "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
        "/api/approval-challenges/{challenge_id}/confirm",
        "/api/approvals/{approval_id}",
        "/assets",
    }
    assert "/api/proposals/{proposal_id}/approve" not in paths


def test_bootstrap_rechecks_complete_strategy_nav_reconciliation_proof() -> None:
    observed_at = "2026-08-27T13:35:00+00:00"
    campaign = {
        "strategy_nav_usd": Decimal("2500"),
        "account_nlv_usd": Decimal("2400"),
        "reconciliation_difference_usd": Decimal("-100"),
        "strategy_nav_asof": observed_at,
        "account_observed_at": observed_at,
        "strategy_nav_content_hash": "a" * 64,
        "strategy_nav_authority_hash": "b" * 64,
        "strategy_nav_contract_hash": "c" * 64,
        "strategy_nav_ledger_head_hash": "d" * 64,
    }
    raw = {
        "asof": observed_at,
        "campaign": campaign,
        "account": {
            "status": "CURRENT",
            "observed_at": observed_at,
            "net_liquidation": Decimal("2400"),
            "reconciled": True,
            "decision_authority": "OBSERVATION_ONLY",
        },
    }

    verified = _normalise_bootstrap(raw)

    assert verified["account"]["reconciled"] is True
    assert verified["account"]["reconciliation_status"] == "VERIFIED"
    assert verified["account"]["strategy_nav_asof"] == observed_at
    assert verified["account"]["observed_at"] == observed_at
    assert verified["account"]["reconciliation_difference_usd"] == -100.0
    assert verified["campaign"]["account_nlv_usd"] == 2400.0
    assert verified["warnings"] == []

    invalid = _normalise_bootstrap(
        {
            **raw,
            "campaign": {
                **campaign,
                "account_nlv_usd": Decimal("2401"),
            },
        }
    )

    assert invalid["account"]["reconciled"] is None
    assert invalid["account"]["reconciliation_status"] == "INVALID"
    assert invalid["warnings"] == ["BOOTSTRAP_RECONCILIATION_PROOF_INVALID"]


@pytest.mark.parametrize(
    "allocation",
    (
        {
            "schema": "options_copilot.research_allocation_evidence.v1",
            "decision_authority": "SUPPORTING_ONLY",
        },
        {
            "schema": "options_copilot.research_allocation_evidence.v2",
            "decision_authority": "SUPPORTING_ONLY",
        },
        {
            "schema": "options_copilot.research_allocation_evidence.v3",
            "decision_authority": "PRIMARY",
        },
    ),
)
def test_api_omits_legacy_or_invalid_research_allocation_from_all_read_paths(
    allocation: dict[str, object],
) -> None:
    def ranking_payload() -> dict[str, object]:
        return {
            "ranking_snapshot_id": "ranking-allocation",
            "scan_run_id": "scan-allocation",
            "decision": "NO_TRADE",
            "approval_enabled": False,
            "candidates": [],
            "funnel_trace": {"research_allocation": allocation},
            "immutable_inputs": {
                "funnel_trace": {"research_allocation": allocation},
            },
        }

    app = create_app(
        _services(
            latest_scan_provider=lambda: {
                "scan_run_id": "scan-allocation",
                "funnel_trace": {"research_allocation": allocation},
            },
            latest_ranking_provider=ranking_payload,
            ranking_provider=lambda _snapshot_id: ranking_payload(),
        )
    )

    scan = _run(_route(app, "/api/scans/latest")())
    latest = _run(_route(app, "/api/rankings/latest")())
    by_id = _run(
        _route(app, "/api/rankings/{ranking_snapshot_id}")(
            "ranking-allocation"
        )
    )

    for payload in (scan, latest, by_id):
        assert "research_allocation" not in payload["funnel_trace"]
    for payload in (latest, by_id):
        assert "research_allocation" not in payload["immutable_inputs"][
            "funnel_trace"
        ]


def test_latest_scan_projects_bounded_observation_only_operational_timing() -> None:
    timing = {
        "schema": "options_copilot.scan_operational_timing.v1",
        "scan_run_id": "scan-timed",
        "total_duration_ms": 1_234,
        "stages": (
            {"stage": "INPUT_ACQUISITION", "duration_ms": 234},
            {"stage": "BROKER_EVIDENCE", "duration_ms": 1_000},
        ),
        "decision_authority": "OBSERVATION_ONLY",
        "affects_decision": False,
        "timing_hash": "a" * 64,
        "recorded_at": "2026-08-31T13:37:00+00:00",
        "private_detail": "must-not-leak",
    }
    app = create_app(
        _services(
            latest_scan_provider=lambda: {
                "scan_run_id": "scan-timed",
                "decision": "NO_TRADE",
                "operational_timing": timing,
            }
        )
    )

    payload = _run(_route(app, "/api/scans/latest")())

    expected = {
        key: value
        for key, value in timing.items()
        if key not in {"private_detail", "stages"}
    }
    expected["stages"] = [
        {"stage": "INPUT_ACQUISITION", "duration_ms": 234},
        {"stage": "BROKER_EVIDENCE", "duration_ms": 1_000},
    ]
    assert payload["operational_timing"] == expected
    assert payload["operational_timing"]["stages"] == [
        {"stage": "INPUT_ACQUISITION", "duration_ms": 234},
        {"stage": "BROKER_EVIDENCE", "duration_ms": 1_000},
    ]


@pytest.mark.parametrize(
    "mutation",
    (
        {"scan_run_id": "different-scan"},
        {"affects_decision": True},
        {"decision_authority": "PRIMARY"},
        {"total_duration_ms": 1},
        {"stages": ({"stage": "BROKER EVIDENCE", "duration_ms": 1_234},)},
        {"timing_hash": None},
        {"timing_hash": "not-a-hash"},
        {"recorded_at": None},
        {"recorded_at": "2026-08-31T13:37:00"},
    ),
)
def test_latest_scan_drops_invalid_operational_timing(
    mutation: dict[str, object],
) -> None:
    timing = {
        "schema": "options_copilot.scan_operational_timing.v1",
        "scan_run_id": "scan-invalid-timing",
        "timing_hash": "a" * 64,
        "recorded_at": "2026-08-31T13:37:00+00:00",
        "total_duration_ms": 1_234,
        "stages": (
            {"stage": "BROKER_EVIDENCE", "duration_ms": 1_234},
        ),
        "decision_authority": "OBSERVATION_ONLY",
        "affects_decision": False,
        **mutation,
    }
    app = create_app(
        _services(
            latest_scan_provider=lambda: {
                "scan_run_id": "scan-invalid-timing",
                "decision": "NO_TRADE",
                "operational_timing": timing,
            }
        )
    )

    payload = _run(_route(app, "/api/scans/latest")())

    assert "operational_timing" not in payload


def test_api_preserves_canonical_v3_allocation_after_json_round_trip() -> None:
    allocation = build_research_allocation_evidence(
        event_rows=(
            {
                "symbol": "QQQ",
                "deterministic_score": "60",
                "advisory_score": "80",
            },
        ),
        scanner_rows=(),
        core_rows=(),
        limit=1,
    )
    json_allocation = json.loads(json.dumps(allocation))

    def ranking_payload() -> dict[str, object]:
        return {
            "ranking_snapshot_id": "ranking-v3",
            "scan_run_id": "scan-v3",
            "decision": "NO_TRADE",
            "approval_enabled": False,
            "candidates": [],
            "funnel_trace": {"research_allocation": json_allocation},
            "immutable_inputs": {
                "funnel_trace": {"research_allocation": json_allocation},
            },
        }

    app = create_app(
        _services(
            latest_scan_provider=lambda: {
                "scan_run_id": "scan-v3",
                "funnel_trace": {"research_allocation": json_allocation},
            },
            latest_ranking_provider=ranking_payload,
            ranking_provider=lambda _snapshot_id: ranking_payload(),
        )
    )

    scan = _run(_route(app, "/api/scans/latest")())
    latest = _run(_route(app, "/api/rankings/latest")())
    by_id = _run(
        _route(app, "/api/rankings/{ranking_snapshot_id}")("ranking-v3")
    )

    for payload in (scan, latest, by_id):
        assert payload["funnel_trace"]["research_allocation"] == allocation
    for payload in (latest, by_id):
        assert payload["immutable_inputs"]["funnel_trace"][
            "research_allocation"
        ] == allocation


def test_learning_api_uses_an_explicit_public_allowlist() -> None:
    private_value = "must-not-escape-learning-private"
    services = _services(
        learning_provider=lambda: {
            "champion": {"name": "baseline-v1", "account_id": private_value},
            "challenger": {"name": "shadow-v2", "raw_payload": private_value},
            "stage": "DISCOVERY",
            "decision_records": 12,
            "minimum_discovery_scenarios": 30,
            "message": "shadow evidence only",
            "outcome_capture": {
                "status": "BLOCKED",
                "checked_at": "2026-08-04T20:15:02Z",
                "specs_seen": 1560,
                "specs_due": 10,
                "observations_appended": 0,
                "records_blocked": 10,
                "records_skipped": 0,
                "reason_codes": ["OUTCOME_OBSERVATION_NOT_AVAILABLE"],
                "durable_status_counts": {"BLOCKED": 1550, "WAITING": 10},
                "durable_blocker_counts": {
                    "OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED": 1550,
                },
                "direction_outcomes_enabled": True,
                "option_economics_requires_bound_candidate": True,
                "account_id": private_value,
            },
            "shadow_learning": {
                "status": "VERIFIED",
                "stage": "DISCOVERY",
                "grade": "B",
                "independent_samples": 31,
                "minimum_discovery_scenarios": 30,
                "discovery_ready": True,
                "selected_challenger": "shadow-v2",
                "challengers": ["shadow-v2"],
                "record_counts": {
                    "THESIS": 1,
                    "EVIDENCE": 2,
                    "PREDICTION": 3,
                    "OUTCOME": 4,
                    "positions": private_value,
                },
                "record_count": 10,
                "ledger": {
                    "schema_version": 1,
                    "journal_mode": "wal",
                    "integrity_verified": True,
                    "filesystem_path": private_value,
                },
                "working_orders": [private_value],
                "net_liquidation": private_value,
            },
            "account": {"account_id": private_value},
            "positions": [private_value],
            "working_orders": [private_value],
            "instruction_id": private_value,
            "filesystem_path": private_value,
            "raw_payload": {"provider": private_value},
            "provider_extension": {"arbitrary": private_value},
        }
    )
    payload = _run(_route(create_app(services), "/api/learning")())
    serialized = json.dumps(payload, sort_keys=True)

    assert payload["champion"] == {"name": "baseline-v1"}
    assert payload["challenger"] == {"name": "shadow-v2"}
    assert payload["stage"] == "DISCOVERY"
    assert payload["shadow_learning"]["record_counts"] == {
        "THESIS": 1,
        "EVIDENCE": 2,
        "PREDICTION": 3,
        "OUTCOME": 4,
    }
    assert payload["outcome_capture"] == {
        "status": "BLOCKED",
        "checked_at": "2026-08-04T20:15:02+00:00",
        "specs_seen": 1560,
        "specs_due": 10,
        "observations_appended": 0,
        "records_blocked": 10,
        "records_skipped": 0,
        "reason_codes": ["OUTCOME_OBSERVATION_NOT_AVAILABLE"],
        "durable_status_counts": {"BLOCKED": 1550, "WAITING": 10},
        "durable_blocker_counts": {
            "OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED": 1550,
        },
        "direction_outcomes_enabled": True,
        "option_economics_requires_bound_candidate": True,
        "decision_authority": "SUPPORTING_ONLY",
        "affects_production_weights": False,
        "affects_eligibility": False,
        "affects_risk": False,
        "affects_ranking": False,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    assert payload["shadow_learning"]["ledger"] == {
        "schema_version": 1,
        "journal_mode": "wal",
        "integrity_verified": True,
    }
    assert payload["shadow_learning"]["authority"]["order_authority"] is False
    assert private_value not in serialized
    for forbidden in (
        "account_id",
        "positions",
        "net_liquidation",
        "working_orders",
        "instruction_id",
        "filesystem_path",
        "raw_payload",
        "provider_extension",
    ):
        assert forbidden not in serialized


def test_positions_api_preserves_explicit_unavailable_state() -> None:
    services = OptionsCopilotServices(
        health_provider=lambda: {},
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: {"candidates": []},
        positions_provider=lambda: {
            "positions": [],
            "status": "UNAVAILABLE",
            "position_state_known": False,
            "asof": "2026-08-02T22:58:59Z",
            "reason": "SNAPSHOT_STALE",
        },
        learning_provider=lambda: {},
        approval_status_provider=lambda _approval_id: {},
    )

    payload = _run(_route(create_app(services), "/api/positions")())

    assert payload["positions"] == []
    assert payload["status"] == "UNAVAILABLE"
    assert payload["position_state_known"] is False
    assert payload["reason"] == "SNAPSHOT_STALE"
    assert payload["count"] == 0


def test_health_distinguishes_readable_preselection_ledger_waiting_from_corruption() -> None:
    waiting_services = OptionsCopilotServices(
        health_provider=lambda: {
            "dependencies": {
                "top10_preselection_ledger": {
                    "status": "UNAVAILABLE",
                    "health": "READY",
                    "readable": True,
                    "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
                    "ledger_reason": "NO_PREMARKET_LEDGER_RUN",
                    "requested_count": 10,
                    "available_count": 0,
                    "open_count": 0,
                    "source": "DURABLE_PREMARKET_LEDGER_V1",
                    "latest_run_id": None,
                    "latest_head_hash": None,
                    "freeze_slot": None,
                    "latest_open_batch_id": None,
                    "latest_open_batch_head_hash": None,
                    "reprice_slot": None,
                    "open_reprice_producer_status": "UNAVAILABLE",
                    "open_reprice_writer": "INDEPENDENT_TOP10_PRODUCER_V1",
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                    "api_token": "must-not-leak",
                }
            }
        },
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: {"candidates": []},
        positions_provider=lambda: {"positions": []},
        learning_provider=lambda: {},
        approval_status_provider=lambda _approval_id: {},
    )
    corrupt_services = OptionsCopilotServices(
        health_provider=lambda: {
            "dependencies": {
                "top10_preselection_ledger": {
                    "status": "UNAVAILABLE",
                    "health": "DEGRADED",
                    "readable": False,
                    "reason": "PRESELECTION_LEDGER_UNREADABLE",
                    "ledger_reason": None,
                    "requested_count": 10,
                    "available_count": 0,
                    "open_count": 0,
                    "api_token": "must-not-leak",
                }
            }
        },
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: {"candidates": []},
        positions_provider=lambda: {"positions": []},
        learning_provider=lambda: {},
        approval_status_provider=lambda _approval_id: {},
    )

    waiting = _run(_route(create_app(waiting_services), "/health")())
    corrupt = _run(_route(create_app(corrupt_services), "/health")())

    assert waiting["status"] == "DEGRADED"
    assert waiting["dependencies"]["top10_preselection_ledger"] == {
        "status": "UNAVAILABLE",
        "health": "READY",
        "readable": True,
        "reason": "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
        "ledger_reason": "NO_PREMARKET_LEDGER_RUN",
        "requested_count": 10,
        "available_count": 0,
        "open_count": 0,
        "source": "DURABLE_PREMARKET_LEDGER_V1",
        "latest_run_id": None,
        "latest_head_hash": None,
        "freeze_slot": None,
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
    assert corrupt["status"] == "DEGRADED"
    assert corrupt["dependencies"]["top10_preselection_ledger"] == {
        "status": "UNAVAILABLE",
        "health": "DEGRADED",
        "readable": False,
        "reason": "PRESELECTION_LEDGER_UNREADABLE",
        "ledger_reason": None,
        "requested_count": 10,
        "available_count": 0,
        "open_count": 0,
    }
    assert "api_token" not in str(waiting)
    assert "api_token" not in str(corrupt)


def test_health_projects_sanitized_daily_operation_state() -> None:
    services = OptionsCopilotServices(
        health_provider=lambda: {
            "dependencies": {
                "production_scanner": {
                    "status": "READY",
                    "scheduler": {
                        "top10_producer": {
                            "status": "DEGRADED",
                            "last_tick_status": "COMPLETED",
                            "last_producer_status": "NO_TRADE",
                            "last_reason": "NO_TOP10_SLOT_DUE",
                            "last_reason_codes": ["UNDERLYING_QUOTE_EMPTY"],
                            "last_missing_symbols": ["SPY"],
                            "last_written_count": 0,
                            "last_producer_slot": "PREMARKET_0920",
                            "last_producer_run_id": "top10-premarket-2026-08-04",
                            "last_producer_evidence_hash": "f" * 64,
                        },
                        "daily_operations": {
                            "timezone": "America/New_York",
                            "calendar_refresh": {
                                "schedule": "EVERY_HEARTBEAT",
                                "status": "COMPLETED",
                                "last_run_at": "2026-08-04T14:05:00Z",
                                "secret": "must-not-leak",
                            },
                            "runs": [
                                {
                                    "operation": "ORDINARY_SCAN",
                                    "scheduled_at": "2026-08-04T10:00:00-04:00",
                                    "status": "COMPLETED",
                                    "recovery_policy": "LATEST_ONLY_WITHIN_TWO_HOURS_FRESH_EVIDENCE",
                                    "scan_run_id": "scan.safe",
                                    "handler_status": None,
                                    "account_id": "U1234567",
                                },
                                {
                                    "operation": "AFTER_HOURS_DISCOVERY",
                                    "scheduled_at": "2026-08-04T16:20:00-04:00",
                                    "status": "FAILED",
                                    "recovery_policy": "EXACT_ONLY_NO_REPLAY",
                                    "scan_run_id": "scan.discovery",
                                    "handler_status": "READY",
                                    "reason_codes": [
                                        "AFTER_HOURS_DISCOVERY_FAILED"
                                    ],
                                    "recorded_at": "2026-08-04T20:20:02Z",
                                },
                                {
                                    "operation": "TOP10_REPRICE",
                                    "scheduled_at": "2026-08-04T09:35:00-04:00",
                                    "status": "COMPLETED",
                                    "recovery_policy": "EXACT_ONLY_NO_REPLAY",
                                    "scan_run_id": "scan.top10",
                                    "handler_status": "READY",
                                    "producer_status": "NO_TRADE",
                                    "producer_written_count": 0,
                                    "producer_missing_symbols": ["SPY"],
                                    "producer_evidence_hash": "e" * 64,
                                    "reason_codes": ["QUOTE_STALE_OR_FUTURE"],
                                }
                            ],
                            "outcome_processing": {
                                "status": "COMPLETED",
                                "checked_at": "2026-08-04T20:15:02Z",
                                "due_count": 7,
                                "records_appended": 4,
                                "records_superseded": 1,
                                "records_skipped": 2,
                                "records_blocked": 3,
                                "records_rejected": 0,
                                "reason_codes": ["OUTCOME_OBSERVATION_NOT_AVAILABLE"],
                                "candidate_ledger_head_hash": "a" * 64,
                                "shadow_ledger_head_hash": "b" * 64,
                                "manifest_hash": "c" * 64,
                                "processing_hash": "d" * 64,
                                "account_id": "U1234567",
                            },
                            "next_session_preparation": {
                                "status": "DEGRADED",
                                "prepared_at": "2026-08-04T20:40:02Z",
                                "next_trading_date": "2026-08-05",
                                "equity_research_count": 10,
                                "equity_selected_count": 0,
                                "option_research_structure_count": 10,
                                "option_structure_count": 0,
                                "executable_count": 7,
                                "research_watchlist_count": 0,
                                "reason_codes": [
                                    "EQUITY_POOL_EMPTY_OR_UNAVAILABLE",
                                    "OPTION_POOL_EMPTY_OR_UNAVAILABLE",
                                ],
                            },
                            "today": {
                                "trading_date": "2026-08-04",
                                "market_status": "TRADING_SESSION",
                                "next_trading_date": "2026-08-05",
                            },
                            "api_token": "must-not-leak",
                        }
                    },
                }
            }
        },
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: {"candidates": []},
        positions_provider=lambda: {"positions": []},
        learning_provider=lambda: {},
        approval_status_provider=lambda _approval_id: {},
    )

    payload = _run(_route(create_app(services), "/health")())
    scanner = payload["dependencies"]["production_scanner"]
    daily = scanner["daily_operations"]

    assert scanner["top10_producer"]["last_written_count"] == 0

    assert daily["calendar_refresh"] == {
        "schedule": "EVERY_HEARTBEAT",
        "status": "COMPLETED",
        "last_run_at": "2026-08-04T14:05:00+00:00",
        "reason_codes": [],
    }
    assert daily["runs"] == [
        {
            "operation": "ORDINARY_SCAN",
            "scheduled_at": "2026-08-04T10:00:00-04:00",
            "status": "COMPLETED",
            "recovery_policy": "LATEST_ONLY_WITHIN_TWO_HOURS_FRESH_EVIDENCE",
            "scan_run_id": "scan.safe",
            "handler_status": None,
        },
        {
            "operation": "AFTER_HOURS_DISCOVERY",
            "scheduled_at": "2026-08-04T16:20:00-04:00",
            "status": "FAILED",
            "recovery_policy": "EXACT_ONLY_NO_REPLAY",
            "scan_run_id": "scan.discovery",
            "handler_status": "READY",
            "reason_codes": ["AFTER_HOURS_DISCOVERY_FAILED"],
            "recorded_at": "2026-08-04T20:20:02+00:00",
        },
        {
            "operation": "TOP10_REPRICE",
            "scheduled_at": "2026-08-04T09:35:00-04:00",
            "status": "COMPLETED",
            "recovery_policy": "EXACT_ONLY_NO_REPLAY",
            "scan_run_id": "scan.top10",
            "handler_status": "READY",
            "producer_status": "NO_TRADE",
            "producer_written_count": 0,
            "producer_missing_symbols": ["SPY"],
            "producer_evidence_hash": "e" * 64,
            "reason_codes": ["QUOTE_STALE_OR_FUTURE"],
        },
    ]
    assert daily["outcome_processing"] == {
        "status": "COMPLETED",
        "checked_at": "2026-08-04T20:15:02+00:00",
        "due_count": 7,
        "recorded_count": 5,
        "skipped_count": 2,
        "blocked_count": 3,
        "error_count": 0,
        "reason_codes": ["OUTCOME_OBSERVATION_NOT_AVAILABLE"],
        "candidate_ledger_head_hash": "a" * 64,
        "shadow_ledger_head_hash": "b" * 64,
        "manifest_hash": "c" * 64,
        "processing_hash": "d" * 64,
        "prediction_cursor": 0,
        "candidate_cursor": 0,
        "remaining_count": 0,
        "progress_sequence": 0,
        "progress_hash": None,
        "bounded": False,
        "decision_authority": "SUPPORTING_ONLY",
        "affects_production_weights": False,
        "affects_eligibility": False,
        "affects_risk": False,
        "affects_ranking": False,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    projected_json = json.dumps(daily["outcome_processing"])
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ outcomeProcessingSummary }} = await import("{script_uri}");
console.log(outcomeProcessingSummary({projected_json}));'''
    rendered = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert rendered.startswith("COMPLETED")
    for token in (
        "recorded 5",
        "skipped 2",
        "blocked 3",
        "errors 0",
        "remaining 0",
        "SUPPORTING_ONLY",
    ):
        assert token in rendered
    assert daily["position_research"]["status"] == "UNAVAILABLE"
    assert daily["after_hours_reprice"]["status"] == "UNAVAILABLE"
    assert daily["next_session_preparation"]["status"] == "DEGRADED"
    assert daily["next_session_preparation"]["equity_research_count"] == 10
    assert daily["next_session_preparation"]["equity_selected_count"] == 0
    assert (
        daily["next_session_preparation"]["option_research_structure_count"]
        == 10
    )
    assert daily["next_session_preparation"]["executable_count"] == 0
    assert daily["next_session_preparation"]["decision"] == "NO_TRADE"
    assert (
        daily["next_session_preparation"]["decision_authority"]
        == "SUPPORTING_ONLY"
    )
    assert daily["next_session_preparation"]["reason_codes"] == [
        "EQUITY_POOL_EMPTY_OR_UNAVAILABLE",
        "OPTION_POOL_EMPTY_OR_UNAVAILABLE",
    ]
    assert daily["today"] == {
        "trading_date": "2026-08-04",
        "market_status": "TRADING_SESSION",
        "next_trading_date": "2026-08-05",
        "runs": daily["runs"],
    }
    assert "account_id" not in str(payload)
    assert "api_token" not in str(payload)


def test_daily_operation_projection_rejects_invalid_market_dates_and_status() -> None:
    projected = _normalise_daily_operations(
        {
            "today": {
                "trading_date": "2026-02-30",
                "market_status": "HOLIDAY",
                "next_trading_date": "not-a-date",
            },
            "runs": [],
        }
    )

    assert projected is not None
    assert projected["today"] == {
        "trading_date": None,
        "market_status": "UNVERIFIED",
        "next_trading_date": None,
        "runs": [],
    }


@pytest.mark.parametrize(
    ("raw_count", "expected_count"),
    (
        (0, 0),
        (10, 10),
        (-1, None),
        (11, None),
        (True, None),
        (None, None),
    ),
)
def test_daily_operation_projection_preserves_only_bounded_producer_counts(
    raw_count,
    expected_count,
) -> None:
    projected = _normalise_daily_operations(
        {
            "runs": [
                {
                    "operation": "TOP10_REPRICE",
                    "scheduled_at": "2026-08-04T09:35:00-04:00",
                    "status": "COMPLETED",
                    "recovery_policy": "EXACT_ONLY_NO_REPLAY",
                    "producer_status": "NO_TRADE",
                    "producer_written_count": raw_count,
                }
            ]
        }
    )

    assert projected is not None
    assert projected["runs"][0]["producer_written_count"] == expected_count


def test_pool_provider_failures_return_explicit_safe_payloads() -> None:
    def failed_provider():
        raise RuntimeError("private provider failure")

    services = OptionsCopilotServices(
        health_provider=lambda: {},
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: {"candidates": []},
        positions_provider=lambda: {"positions": []},
        learning_provider=lambda: {},
        approval_status_provider=lambda _approval_id: {},
        equity_pool_provider=failed_provider,
        option_pool_provider=failed_provider,
    )
    app = create_app(services)

    equity = _run(_route(app, "/api/equity-pool/latest")())
    option = _run(_route(app, "/api/option-pool/latest")())

    assert equity["status"] == "UNAVAILABLE"
    assert equity["reason_codes"] == ["EQUITY_POOL_PROVIDER_FAILED"]
    assert equity["approval_eligible"] is False
    assert equity["order_allowed"] is False
    assert option["status"] == "UNAVAILABLE"
    assert option["reason_codes"] == ["OPTION_STRUCTURE_POOL_PROVIDER_FAILED"]
    assert option["approval_eligible"] is False
    assert option["order_allowed"] is False


def test_empty_candidate_collection_is_an_explicit_no_trade_decision() -> None:
    app = create_app(_services(candidates_provider=lambda: []))

    payload = _run(_route(app, "/api/candidates")())

    assert payload == {
        "candidates": [],
        "count": 0,
        "total_count": 0,
        "truncated": False,
        "decision": "NO_TRADE",
    }


def test_positioning_api_is_strictly_supporting_only_and_discloses_chain_scope() -> None:
    app = create_app(
        _services(
            positioning_provider=lambda: {
                "status": "DEGRADED",
                "reasons": ("PARTIAL_OPTION_CHAIN_COVERAGE",),
                "decision_authority": "EXECUTE",
                "approval_allowed": True,
                "positioning": (
                    {
                        "underlying": "GLD",
                        "expiration": "2026-08-21",
                        "max_pain": "385",
                        "call_wall": "390",
                        "put_wall": "375",
                        "put_call_open_interest_ratio": "0.8",
                        "estimated_net_gex_usd_per_one_percent": "12345",
                        "option_chain_coverage_rate": "0.10",
                        "chain_scope": "FROZEN_FINALIST_LEGS_ONLY",
                        "coverage_limitation": "partial sample",
                        "approval_allowed": True,
                        "order_id": "must-be-dropped",
                    },
                ),
            }
        )
    )

    payload = _run(_route(app, "/api/positioning")())

    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["supporting_only"] is True
    assert payload["affects_eligibility"] is False
    assert payload["approval_allowed"] is False
    assert payload["instruction_allowed"] is False
    assert payload["order_allowed"] is False
    assert payload["count"] == 1
    row = payload["positioning"][0]
    assert row["chain_scope"] == "FROZEN_FINALIST_LEGS_ONLY"
    assert row["option_chain_coverage_rate"] == "0.10"
    assert "approval_allowed" not in row
    assert "order_id" not in row


def test_management_api_preserves_generic_vertical_review_evidence_only() -> None:
    app = create_app(
        _services(
            management_provider=lambda: {
                "status": "CANDIDATES",
                "decision": "PREVIEW_ONLY",
                "candidates": (
                    {
                        "candidate_id": "qqq-close-all",
                        "symbol": "QQQ",
                        "structure": "BULL_CALL_DEBIT_VERTICAL",
                        "review_state": "HOLD_MONITOR",
                        "thesis_invalidation_state": "NOT_EVALUATED",
                        "risk_stop_state": "CLEAR",
                        "profit_take_state": "CLEAR",
                        "time_stop_state": "CLEAR",
                        "entry_net_cost_usd": "166.10",
                        "entry_max_profit_usd": "133.90",
                        "stop_review_cashflow_usd": "99.66",
                        "profit_review_cashflow_usd": "246.44",
                        "approval_enabled": True,
                        "direct_order_submission": True,
                        "order_id": "must-be-dropped",
                    },
                ),
            }
        )
    )

    payload = _run(_route(app, "/api/management/current")())

    assert payload["review_only"] is True
    assert payload["approval_enabled"] is False
    assert payload["direct_order_submission"] is False
    candidate = payload["candidates"][0]
    assert candidate["symbol"] == "QQQ"
    assert candidate["structure"] == "BULL_CALL_DEBIT_VERTICAL"
    assert candidate["review_state"] == "HOLD_MONITOR"
    assert candidate["entry_net_cost_usd"] == "166.10"
    assert candidate["stop_review_cashflow_usd"] == "99.66"
    assert candidate["review_only"] is True
    assert candidate["approval_enabled"] is False
    assert candidate["direct_order_submission"] is False
    assert "order_id" not in candidate


def test_approval_requires_empty_first_request_and_strict_second_confirmation() -> None:
    assert RankOneChallengeRequest.model_validate({}).model_dump() == {}
    for forbidden in (
        {"rank": 1},
        {"proposal_hash": "a" * 64},
        {"quote_snapshot_id": "quotes-44"},
    ):
        with pytest.raises(ValidationError):
            RankOneChallengeRequest.model_validate(forbidden)

    valid = {
        "challenge_response": APPROVAL_CHALLENGE,
        "risk_acknowledged": True,
        "second_confirmation": True,
        "confirmation_token": APPROVAL_CONFIRMATION_TOKEN,
    }
    ApprovalConfirmationRequest.model_validate(valid)
    for key, value in (
        ("risk_acknowledged", False),
        ("second_confirmation", False),
        ("confirmation_token", "PLACE_ORDER_NOW"),
    ):
        invalid = dict(valid)
        invalid[key] = value
        with pytest.raises(ValidationError):
            ApprovalConfirmationRequest.model_validate(invalid)

    unavailable = create_app(_services())
    with pytest.raises(HTTPException) as unavailable_error:
        _run(
            _route(
                unavailable,
                "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
            )("ranking-1", "candidate-1", RankOneChallengeRequest())
        )
    assert unavailable_error.value.status_code == 503

    violating = create_app(
        _services(
            challenge_confirmation_handler=lambda _challenge_id, _request: {
                "status": "PENDING_CODEX_BRIDGE",
                "review_only": True,
                "order_submitted": True,
                "transmitted_to_broker": False,
            }
        )
    )
    with pytest.raises(HTTPException) as contract_error:
        _run(
            _route(
                violating,
                "/api/approval-challenges/{challenge_id}/confirm",
            )("challenge-1", ApprovalConfirmationRequest.model_validate(valid))
        )
    assert contract_error.value.status_code == 502
    assert "review-only" in str(contract_error.value.detail)


def test_unknown_external_outcome_is_safe_and_exposes_no_instruction() -> None:
    app = create_app(
        _services(
            approval_status_provider=lambda approval_id: {
                "approval_id": approval_id,
                "status": "UNKNOWN_OUTCOME",
                "expires_at": "2026-08-03T02:05:00Z",
                "instruction_id": None,
                "ibkr_deep_link": None,
                "failure_reason": "automatic retry is forbidden",
                "order_submitted": False,
                "transmitted_to_broker": False,
            }
        )
    )

    payload = _run(_route(app, "/api/approvals/{approval_id}")("approval-1"))

    assert payload["status"] == "UNKNOWN_OUTCOME"
    assert payload["instruction_id"] is None
    assert payload["ibkr_deep_link"] is None
    assert payload["review_only"] is True
    assert payload["order_submitted"] is False
    assert payload["transmitted_to_broker"] is False


@pytest.mark.parametrize(
    "deep_link",
    (
        "https://chatgpt.com/connector/ibkr/review/review-7",
        "https://evil.example/connector/ibkr/review/7",
        "https://user:pass@chatgpt.com/connector/ibkr/review/7",
        "https://chatgpt.com/connector/ibkr/review/7?token=secret",
        "https://chatgpt.com/connector/ibkr/review/7#secret",
        "https://chatgpt.com/not-contracted/review/7",
        "https://127.0.0.1/connector/ibkr/review/7",
    ),
)
def test_ready_review_handoff_rejects_every_url_without_authoritative_contract(
    deep_link: str,
) -> None:
    app = create_app(
        _services(
            approval_status_provider=lambda approval_id: {
                "approval_id": approval_id,
                "status": "READY_FOR_IBKR_REVIEW",
                "expires_at": "2026-08-03T02:05:00Z",
                "instruction_id": "review-7",
                "ibkr_deep_link": deep_link,
                "redirect_provenance": "TRUSTED_NO_REDIRECT",
                "order_submitted": False,
                "transmitted_to_broker": False,
            }
        )
    )

    with pytest.raises(HTTPException) as failure:
        _run(_route(app, "/api/approvals/{approval_id}")("approval-7"))

    assert failure.value.status_code == 502
    assert failure.value.detail == "creator review destination contract is unavailable"


def test_ready_review_handoff_rejects_missing_redirect_provenance() -> None:
    app = create_app(
        _services(
            approval_status_provider=lambda approval_id: {
                "approval_id": approval_id,
                "status": "READY_FOR_IBKR_REVIEW",
                "expires_at": "2026-08-03T02:05:00Z",
                "instruction_id": "review-7",
                "ibkr_deep_link": (
                    "https://chatgpt.com/connector/ibkr/review/review-7"
                ),
                "order_submitted": False,
                "transmitted_to_broker": False,
            }
        )
    )

    with pytest.raises(HTTPException) as failure:
        _run(_route(app, "/api/approvals/{approval_id}")("approval-7"))

    assert failure.value.status_code == 502
    assert failure.value.detail == "creator review destination contract is unavailable"


def test_frontend_index_is_served_from_the_isolated_options_package() -> None:
    app = create_app(_services())

    response = _run(_route(app, "/")())

    assert isinstance(response, FileResponse)
    assert Path(response.path).resolve() == (FRONTEND / "index.html").resolve()


def test_frontend_module_url_is_versioned_for_runtime_cache_invalidation() -> None:
    html = _read("index.html")

    assert '<script type="module" src="/assets/app.js?v=20260909.6"></script>' in html


class _ContractParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()
        self.buttons: list[dict[str, str | None]] = []
        self.inputs: list[dict[str, str | None]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if values.get("id"):
            self.ids.add(str(values["id"]))
        if tag == "button":
            self.buttons.append(values)
        if tag == "input":
            self.inputs.append(values)


def test_chinese_workbench_exposes_all_account_candidate_learning_and_review_surfaces() -> None:
    html = _read("index.html")
    parser = _ContractParser()
    parser.feed(html)

    assert {
        "app-shell",
        "page-status",
        "last-refresh",
        "refresh-data",
        "ibkr-status",
        "ibkr-account",
        "ibkr-review-mode",
        "nlv-current",
        "nlv-target",
        "campaign-progress",
        "campaign-progress-bar",
        "campaign-remaining",
        "nav-reconciliation-evidence",
        "legacy-position-list",
        "position-plan-list",
        "positioning-status",
        "positioning-list",
        "no-trade-banner",
        "no-trade-title",
        "no-trade-reason",
        "candidate-list",
        "candidate-count",
        "learning-champion",
        "learning-challenger",
        "learning-gate",
        "approval-status",
        "ibkr-review-link",
        "candidate-template",
    } <= parser.ids
    assert all(button.get("title") or button.get("aria-label") for button in parser.buttons)
    assert not parser.inputs  # Rank 1 acknowledgement is created dynamically only.
    for text in (
            "IBKR 审核模式",
            "仅供审核 · 不可创建指令",
            "10K Campaign",
            "持仓管理",
        "NO_TRADE",
        "Champion / Challenger",
        "Max Pain / 期权墙 / PCR / GEX",
        "最大亏损",
        "最大盈利",
        "成本后 EV",
        "Bid",
        "Ask",
        "Last",
        "IV",
        "OI",
        "Volume",
        "报价时间",
        "IBKR 审核入口（等待 READY_FOR_IBKR_REVIEW）",
    ):
        assert text in html


def test_frontend_caps_candidates_and_requires_checkbox_plus_second_confirmation() -> None:
    script = _read("app.js")

    assert "const MAX_CANDIDATES = 10" in script
    assert ".slice(0, MAX_CANDIDATES)" in script
    assert "window.confirm(" in script
    assert "risk_acknowledged: true" in script
    assert "second_confirmation: true" in script
    assert 'confirmation_token: APPROVAL_CONFIRMATION_TOKEN' in script
    assert "appendRankOneChallengeAction" in script
    assert 'interaction === "CHALLENGE_ALLOWED"' in script
    assert "appState.strategyNavUsd !== null" in script
    assert "VIEW_ONLY" in script
    assert "challenge_response: challenge.challenge_response" in script
    assert "result.review_only !== true" in script
    assert "result.order_submitted !== false" in script
    assert "result.transmitted_to_broker !== false" in script
    assert 'result.status !== "PENDING_CODEX_BRIDGE"' in script
    assert "/api/proposals/" not in script
    assert "/api/approval-challenges/" in script
    assert "fetchJson(handoff.status_url)" in script
    assert 'result.status === "READY_FOR_IBKR_REVIEW"' in script
    assert 'result.status === "FAILED" || result.status === "EXPIRED"' in script
    assert "已批准，等待Codex重新报价" in script
    assert "审核指令已创建" in script
    assert "审批失败" in script
    assert "审批已过期" in script
    assert 'ALLOWED_IBKR_REVIEW_HOSTS' not in script
    assert 'ALLOWED_IBKR_REVIEW_PATH_PREFIXES' not in script
    assert "https://chatgpt.com/connector/ibkr/" not in script
    assert "return false;" in script.split("function isAbsoluteHttpsUrl", 1)[1].split(
        "function configureReviewLink", 1
    )[0]
    assert "appState.reviewLinkHref = null" in script
    assert "审核目标合同不可用" in script
    assert 'firstValue(campaign, ["strategy_nav_usd", "strategy_nav"])' in script
    campaign_renderer = script.split("function renderCampaign", 1)[1].split(
        "function shortHash", 1
    )[0]
    current_assignment = campaign_renderer.split("const current =", 1)[1].split(";", 1)[0]
    assert "strategy_nav_usd" in current_assignment
    assert "account" not in current_assignment
    assert 'account.reconciliation_status || "UNAVAILABLE"' in script
    assert "health.dependencies?.ibkr_snapshot" in script
    assert 'createElement("span", "countdown"' in script
    assert "报价已过期" in script
    assert "innerHTML" not in script


def test_frontend_fetches_only_local_read_models_and_has_no_direct_order_primitive() -> None:
    combined = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(FRONTEND.glob("*.*"))
    )
    for endpoint in (
        'health: "/api/health/summary"',
        'bootstrap: "/api/bootstrap"',
        'rankings: "/api/rankings/latest"',
        'scans: "/api/scans/latest"',
        'management: "/api/management/current"',
        'positions: "/api/positions"',
        'learning: "/api/learning"',
        'positioning: "/api/positioning"',
    ):
        assert endpoint in combined
    assert "/api/orders" not in combined
    assert "/place-order" not in combined
    assert "placeOrder" not in combined
    assert not re.search(r"\bsk-[A-Za-z0-9]{16,}\b", combined)
    assert "rithmic_password" not in combined.lower()
    assert not re.search(r"https?://[^\s'\"]+", combined)


def test_frontend_distinguishes_unknown_positions_from_a_confirmed_empty_account() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert 'payload.position_state_known !== false' in script
    assert 'positionStatus === "STALE"' in script
    assert "断线前最后已知仓位" in script
    assert "账户仓位状态不可用" in script
    assert "当前没有开放仓位" in script


def test_news_api_preserves_conditional_option_contract_and_forces_supporting_only() -> None:
    run_id = "run-api-1"
    head_hash = "d" * 64
    row_hash = "e" * 64
    exclusion_evidence_id = "IBKR_UNDERLYING_QUOTE_EXCLUDED:B3"
    preselection = {
        "preselection_id": "open-aapl-call",
        "underlying": "AAPL",
        "strategy_type": "LONG_CALL",
        "phase": "OPEN_REPRICED",
        "strategy_hash": "c" * 64,
        "evidence_ids": ["evt-aapl", "ibkr-batch-1", exclusion_evidence_id],
        "evidence_hashes": ["a" * 64, "b" * 64, "6" * 64],
        "risk_defined": True,
        "maximum_loss_usd": "216",
        "estimated_cost_usd": "2.40",
        "cost_after_ev_usd": "48",
        "risk_adjusted_ev": "0.22222222",
        "entry_condition": "Enter only at or below the displayed debit.",
        "invalidation_condition": "Cancel if thesis is contradicted.",
        "profit_target_condition": "Review at +50%.",
        "stop_loss_condition": "Review at -35%.",
        "research_summary": "Human-reviewed conditional research.",
        "quote_batch_id": "ibkr-batch-1",
        "oldest_quote_asof": "2026-08-05T13:30:00+00:00",
        "maximum_quote_age_seconds": 1.0,
        "blockers": [],
        "research_rank": None,
        "repriced_rank": 1,
        "action_rank": 1,
        "research_only": False,
        "action_pool_eligible": True,
        "decision_authority": "EXECUTE",
        "approval_eligible": True,
        "instruction_creation_allowed": True,
        "approval_id": "must-be-dropped",
            "ledger_lineage": {
                "source": "INDEPENDENT_TOP10_LEDGER",
                "source_batch_purpose": "OPEN_REPRICE",
                "source_batch_id": "ibkr-batch-1",
                "source_batch_hash": "7" * 64,
                "preselection_id": "open-aapl-call",
            "phase": "OPEN_REPRICED",
            "run_id": run_id,
            "run_created_at": "2026-08-05T13:30:00+00:00",
            "head_hash": head_hash,
            "row_id": "row-api-1",
            "row_hash": row_hash,
            "premarket_rank": 1,
            "observation_id": "observation-api-1",
            "observed_at": "2026-08-05T13:30:00+00:00",
            "observation_hash": "f" * 64,
            "batch_id": "open-batch-api-1",
            "batch_head_hash": "9" * 64,
                "scheduled_for": "2026-08-05T13:30:00+00:00",
                "quote_batch_id": "ibkr-batch-1",
            },
        "legs": [
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
                "quote_asof": "2026-08-05T13:30:00+00:00",
                "quote_batch_id": "ibkr-batch-1",
                "implied_volatility": "0.31",
                "delta": "0.42",
                "gamma": "0.021",
                "theta": "-0.08",
                "vega": "0.11",
                "volume": 240,
                "open_interest": 1800,
                "dte": 16,
                "order_id": "must-be-dropped",
            }
        ],
    }
    preselection["strategy_hash"] = _api_strategy_hash(preselection)
    _attach_api_economics_lineage(preselection)
    premarket = {
        **preselection,
        "phase": "PRE_MARKET",
        "research_rank": 1,
        "repriced_rank": None,
        "action_rank": None,
        "action_pool_eligible": False,
            "ledger_lineage": {
                "source": "INDEPENDENT_TOP10_LEDGER",
                "source_batch_purpose": "PREMARKET_ACCOUNT",
                "source_batch_id": "premarket-account-batch-1",
                "source_batch_hash": "8" * 64,
                "preselection_id": "open-aapl-call",
            "phase": "PRE_MARKET",
            "run_id": run_id,
            "run_created_at": "2026-08-05T13:30:00+00:00",
            "head_hash": head_hash,
            "row_id": "row-api-1",
            "row_hash": row_hash,
            "premarket_rank": 1,
            "production_parent_eligible": True,
            "production_parent_blocker": None,
        },
    }
    app = create_app(
        _services(
            news_provider=lambda: {
                "news": [
                    {
                        "id": "evt-aapl",
                        "title": "Apple event",
                        "symbols": ["AAPL"],
                        "related_options": [preselection],
                    }
                ],
                "pre_market_preselections": [premarket],
                "open_market_repriced": [preselection],
                "option_action_pool": [preselection],
                "preselection_coverage": {
                    "requested_count": 10,
                    "available_count": 1,
                    "open_count": 1,
                    "source": "INDEPENDENT_TOP10_LEDGER",
                    "status": "PARTIAL",
                    "reason": "TOP10_PREMARKET_COVERAGE_INCOMPLETE",
                    "ledger_reason": "TOP10_PREMARKET_COVERAGE_INCOMPLETE",
                    "latest_run_id": run_id,
                    "latest_head_hash": head_hash,
                    "freeze_slot": "2026-08-05T13:30:00+00:00",
                    "latest_open_batch_id": "open-batch-api-1",
                    "latest_open_batch_head_hash": "9" * 64,
                    "reprice_slot": "2026-08-05T13:30:00+00:00",
                    "open_reprice_producer_status": "AVAILABLE",
                    "open_reprice_writer": "must-not-be-projected",
                },
            }
        )
    )

    payload = _run(_route(app, "/api/news")())
    projected = payload["open_market_repriced"][0]

    assert payload["open_market_repriced_count"] == 1
    assert payload["option_action_pool_count"] == 1
    assert payload["news"][0]["related_options"][0] == projected
    assert projected["decision_authority"] == "SUPPORTING_ONLY"
    assert projected["approval_eligible"] is False
    assert projected["instruction_creation_allowed"] is False
    assert projected["order_creation_allowed"] is False
    assert projected["maximum_loss_usd"] == "222"
    assert projected["cost_after_ev_usd"] == "48"
    assert projected["legs"][0]["con_id"] == 101
    assert projected["legs"][0]["local_symbol"] == "AAPL  260821C00225000"
    assert projected["legs"][0]["gamma"] == "0.021"
    assert projected["ledger_lineage"] == {
        "source": "INDEPENDENT_TOP10_LEDGER",
        "source_batch_purpose": "OPEN_REPRICE",
        "source_batch_id": "ibkr-batch-1",
        "source_batch_hash": "7" * 64,
        "run_id": run_id,
        "run_created_at": "2026-08-05T13:30:00+00:00",
        "head_hash": head_hash,
        "row_id": "row-api-1",
        "row_hash": row_hash,
        "premarket_rank": 1,
        "observation_id": "observation-api-1",
        "observed_at": "2026-08-05T13:30:00+00:00",
        "observation_hash": "f" * 64,
        "batch_id": "open-batch-api-1",
        "batch_head_hash": "9" * 64,
        "scheduled_for": "2026-08-05T13:30:00+00:00",
        "quote_batch_id": "ibkr-batch-1",
    }
    assert payload["preselection_coverage"]["available_count"] == 1
    assert payload["preselection_coverage"]["open_count"] == 1
    assert payload["preselection_coverage"]["open_observation_status"] == "AVAILABLE"
    assert payload["preselection_coverage"]["atomic_batch_available"] is True
    assert payload["preselection_coverage"]["open_reprice_producer_status"] == (
        "AVAILABLE"
    )
    assert "approval_id" not in projected
    assert "strategy_hash" not in projected
    assert "evidence_hashes" not in projected
    assert exclusion_evidence_id in projected["evidence_ids"]
    assert projected["quote_batch_id"] == "ibkr-batch-1"
    assert projected["ledger_lineage"]["source_batch_hash"] != projected[
        "broker_snapshot_hash"
    ]
    assert "content_hash" not in projected["ledger_lineage"]
    assert "order_id" not in projected["legs"][0]
    assert projected["legs"][0]["quote_batch_id"] == "ibkr-batch-1"
    assert "open_reprice_writer" not in payload["preselection_coverage"]


def test_advisory_api_projects_four_slices_and_server_owned_read_only_authority() -> None:
    calls = {"snapshot": 0, "network": 0}
    evidence_id = "evidence-synthetic-2026-q2"

    def network_trap() -> None:
        calls["network"] += 1
        raise AssertionError("GET /api/advisory must not perform provider work")

    def advisory_snapshot() -> dict[str, object]:
        calls["snapshot"] += 1
        return {
            "schema_version": "options_copilot.phase2_advisory.v1",
            "symbol": "SYNX",
            "consensus_state": "BEAT",
            "model_state": "FALLBACK",
            "fallback_reason": "MODEL_EVALUATION_PENDING",
            "as_of": "2026-08-07T14:00:00+00:00",
            "observations": [
                {
                    "evidence_id": evidence_id,
                    "evidence_sha256": "a" * 64,
                    "source_tier": "OFFICIAL",
                    "published_at": "2026-08-07T13:58:00+00:00",
                    "first_seen_at": "2026-08-07T13:59:00+00:00",
                    "observed_at": "2026-08-07T14:00:00+00:00",
                    "value": "1.25",
                    "unit": "USD_PER_SHARE",
                    "period": "2026-Q2",
                    "basis": "GAAP",
                    "account_id": "must-not-escape-private",
                }
            ],
            "provenance_ids": [evidence_id],
            "event_news_facts": [
                {
                    "statement": "Synthetic reported EPS was 1.25 USD per share.",
                    "status": "OBSERVED",
                    "evidence_ids": [evidence_id],
                    "raw_body": "must-not-escape-private",
                }
            ],
            "fundamental_support": {
                "status": "SUPPORTS",
                "direction": "BULLISH",
                "summary": "Comparable EPS exceeded the point-in-time consensus.",
                "evidence_ids": [evidence_id],
            },
            "expected_price_impact": {
                "status": "UNCERTAIN",
                "direction": "UNCERTAIN",
                "summary": "The supplied fact may support the underlying price.",
                "evidence_ids": [evidence_id],
            },
            "options_volatility_impact": {
                "status": "UNCERTAIN",
                "direction": "UNCERTAIN",
                "summary": "Option repricing can diverge because of IV and decay.",
                "evidence_ids": [evidence_id],
            },
            "decision_authority": "EXECUTE",
            "approval_eligible": True,
            "instruction_creation_allowed": True,
            "order_allowed": True,
            "unknown_nested": {
                "raw_error": "Authorization: Bearer must-not-escape-private",
                "redirect_target": (
                    "https://evil.example/redirect?api_key=must-not-escape-private"
                ),
                "broker": {"positions": ["must-not-escape-private"]},
                "instruction_id": "must-not-escape-private",
                "local_path": "C:\\Users\\xujie\\must-not-escape-private.json",
                "network_client": network_trap,
            },
        }

    services = _phase2_read_services(
        advisory_provider=advisory_snapshot,
        source_evidence_provider=lambda: {},
    )
    app = create_app(services)

    payload = _run(_route(app, "/api/advisory")())
    serialized = json.dumps(payload, sort_keys=True)

    assert calls == {"snapshot": 1, "network": 0}
    assert payload["schema_version"] == "options_copilot.phase2_advisory.v1"
    assert payload["symbol"] == "SYNX"
    assert payload["consensus_state"] == "BEAT"
    assert payload["model_state"] == "FALLBACK"
    assert payload["fallback_reason"] == "MODEL_EVALUATION_PENDING"
    assert payload["as_of"] == "2026-08-07T14:00:00+00:00"
    assert payload["provenance_ids"] == [evidence_id]
    assert len(payload["observations"]) == 1
    assert set(payload["observations"][0]) == {
        "evidence_id",
        "evidence_sha256",
        "source_tier",
        "published_at",
        "first_seen_at",
        "observed_at",
        "value",
        "unit",
        "period",
        "basis",
    }
    assert len(payload["event_news_facts"]) == 1
    assert payload["fundamental_support"]["status"] == "SUPPORTS"
    assert payload["expected_price_impact"]["status"] == "UNCERTAIN"
    assert payload["options_volatility_impact"]["status"] == "UNCERTAIN"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_allowed"] is False
    assert "must-not-escape-private" not in serialized
    assert "raw_body" not in serialized
    assert "raw_error" not in serialized
    assert "Authorization" not in serialized
    assert "redirect_target" not in serialized
    assert "api_key" not in serialized
    assert "account_id" not in serialized
    assert "positions" not in serialized
    assert "instruction_id" not in serialized
    assert "local_path" not in serialized
    assert "network_client" not in serialized


def test_source_evidence_api_projects_six_rows_and_conflicts_without_health_laundering() -> None:
    source_ids = (
        "sec",
        "nasdaq",
        "company_ir",
        "finnhub",
        "alpha_vantage",
        "jin10",
    )
    states = (
        ("READY", None, True, "VERIFIED"),
        ("STALE", "SOURCE_STALE", True, "VERIFIED"),
        ("UNCONFIGURED", "UNCONFIGURED", False, "PACING_UNVERIFIED"),
        ("RATE_LIMITED", "RATE_LIMITED", True, "RATE_LIMITED"),
        ("FAILED", "REQUEST_FAILED", True, "VERIFIED"),
        ("NOT_CONFIGURED", "NOT_CONFIGURED", False, "PACING_UNVERIFIED"),
    )
    calls = {"snapshot": 0}

    def source_snapshot() -> dict[str, object]:
        calls["snapshot"] += 1
        rows = []
        for index, (source_id, state) in enumerate(zip(source_ids, states, strict=True)):
            status, reason, configured, pacing = state
            rows.append(
                {
                    "source_id": source_id,
                    "configured": configured,
                    "readiness": "READY" if status in {"READY", "STALE"} else status,
                    "status": status,
                    "observed_at": "2026-08-07T14:00:00+00:00",
                    "as_of": "2026-08-07T14:00:00+00:00",
                    "last_success_at": (
                        None
                        if status in {"UNCONFIGURED", "FAILED", "NOT_CONFIGURED"}
                        else "2026-08-07T13:59:00+00:00"
                    ),
                    "freshness_age_seconds": (
                        None
                        if status in {"UNCONFIGURED", "FAILED", "NOT_CONFIGURED"}
                        else 60 + index
                    ),
                    "provenance": [f"{source_id}-evidence"],
                    "pacing": pacing,
                    "reason": reason,
                    "decision_authority": "EXECUTE",
                    "raw_error": "Bearer must-not-escape-private",
                    "source_url": (
                        "https://evil.example/data?token=must-not-escape-private"
                    ),
                }
            )
        return {
            "schema": "options_copilot.source_evidence.v1",
            "as_of": "2026-08-07T14:00:00+00:00",
            "sources": rows,
            "conflicts": [
                {
                    "conflict_id": "conflict-sec-finnhub",
                    "source_ids": ["sec", "finnhub"],
                    "evidence_ids": ["sec-evidence", "finnhub-evidence"],
                    "reason": "INDEPENDENT_SOURCE_CONFLICT",
                    "raw_body": "must-not-escape-private",
                }
            ],
            "account": {"account_id": "must-not-escape-private"},
            "broker_positions": ["must-not-escape-private"],
            "creator_instruction": "must-not-escape-private",
            "approval_eligible": True,
            "instruction_creation_allowed": True,
            "order_allowed": True,
        }

    services = _phase2_read_services(
        advisory_provider=lambda: {},
        source_evidence_provider=source_snapshot,
    )
    app = create_app(services)

    payload = _run(_route(app, "/api/source-evidence")())
    serialized = json.dumps(payload, sort_keys=True)
    rows = payload["sources"]

    assert calls == {"snapshot": 1}
    assert payload["schema"] == "options_copilot.source_evidence.v1"
    assert tuple(row["source_id"] for row in rows) == source_ids
    assert len(rows) == 6
    assert [row["status"] for row in rows] == [state[0] for state in states]
    assert rows[0]["status"] == "READY"
    assert rows[1]["status"] == "STALE"
    assert rows[3]["status"] == "RATE_LIMITED"
    assert rows[4]["status"] == "FAILED"
    assert rows[2]["pacing"] == rows[5]["pacing"] == "PACING_UNVERIFIED"
    assert all(row["decision_authority"] == "SUPPORTING_ONLY" for row in rows)
    assert payload["conflicts"] == [
        {
            "conflict_id": "conflict-sec-finnhub",
            "source_ids": ["sec", "finnhub"],
            "evidence_ids": ["sec-evidence", "finnhub-evidence"],
            "reason": "INDEPENDENT_SOURCE_CONFLICT",
        }
    ]
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_allowed"] is False
    assert "must-not-escape-private" not in serialized
    assert "raw_error" not in serialized
    assert "raw_body" not in serialized
    assert "source_url" not in serialized
    assert "token=" not in serialized
    assert "account_id" not in serialized
    assert "broker_positions" not in serialized
    assert "creator_instruction" not in serialized


def test_frontend_assets_are_syntactically_valid_and_responsive() -> None:
    css = _read("styles.css")
    assert "@media (max-width: 1180px)" in css
    assert "@media (max-width: 760px)" in css
    assert "--green:" in css
    assert "--amber:" in css
    assert "--red:" in css
    assert "gradient(" not in css.lower()

    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run(
        [node, "--check", str(FRONTEND / "app.js")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _api_strategy_hash(preselection: dict[str, object]) -> str:
    legs = preselection["legs"]
    assert isinstance(legs, list)
    return canonical_hash(
        {
            "schema": "options_copilot.conditional_option_strategy.v2",
            "underlying": str(preselection["underlying"]).upper(),
            "strategy_type": str(preselection["strategy_type"]).upper(),
            "legs": [
                {
                    "con_id": int(leg["con_id"]),
                    "local_symbol": str(leg["local_symbol"]),
                    "trading_class": str(leg["trading_class"]),
                    "multiplier": int(leg["multiplier"]),
                    "exchange": str(leg["exchange"]),
                    "expiry": date.fromisoformat(str(leg["expiry"])),
                    "strike": Decimal(str(leg["strike"])),
                    "right": str(leg["right"]),
                    "side": str(leg["side"]),
                    "ratio": int(leg["ratio"]),
                    "quantity": int(leg["quantity"]),
                }
                for leg in legs
                if isinstance(leg, dict)
            ],
        }
    )


def _attach_api_economics_lineage(preselection: dict[str, object]) -> None:
    candidate_id = str(preselection["preselection_id"])
    strategy_hash = str(preselection["strategy_hash"])
    quote_batch_id = str(preselection["quote_batch_id"])
    quote_asof = datetime.fromisoformat(str(preselection["oldest_quote_asof"]))
    scenario_asof = quote_asof - timedelta(minutes=1)
    scenarios = (
        TrustedTerminalScenario(Decimal("200"), Decimal("0.50")),
        TrustedTerminalScenario(Decimal("250"), Decimal("0.50")),
    )
    scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=candidate_id,
        strategy_hash=strategy_hash,
        scenario_asof=scenario_asof,
        scenarios=scenarios,
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )
    strategy_nav = Decimal("10000")
    maximum_loss = Decimal("222")
    debit = Decimal("216")
    credit = Decimal("0")
    commission = Decimal("2.50")
    entry_slippage = Decimal("1.00")
    exit_slippage = Decimal("2.50")
    total_slippage = entry_slippage + exit_slippage
    all_in_cost = debit - credit + commission + total_slippage
    after_cost_ev = Decimal("48")
    before_cost_ev = after_cost_ev + commission + total_slippage
    snapshot_hash = canonical_hash(
        {"schema": "test.atomic_broker_snapshot.v1", "candidate_id": candidate_id}
    )
    payoff_hash = canonical_hash(
        {"schema": "test.open_payoff.v1", "candidate_id": candidate_id}
    )
    nav_hash = strategy_nav_post_hash(
        candidate_id=candidate_id,
        strategy_hash=strategy_hash,
        snapshot_hash=snapshot_hash,
        strategy_nav_usd=strategy_nav,
    )
    economics = OpenRepriceEconomics(
        candidate_id=candidate_id,
        strategy_hash=strategy_hash,
        broker_snapshot_hash=snapshot_hash,
        quote_batch_id=quote_batch_id,
        quote_asof=quote_asof,
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
    preselection.update(
        {
            "maximum_loss_usd": maximum_loss,
            "estimated_cost_usd": all_in_cost,
            "cost_after_ev_usd": after_cost_ev,
            "risk_adjusted_ev": after_cost_ev / maximum_loss,
            "terminal_scenarios": [item.as_dict() for item in scenarios],
            "scenario_asof": scenario_asof.isoformat(),
            "scenario_hash": scenario_set.scenario_hash,
            "execution_cost_contract_version": EXECUTION_COST_VERSION,
            "execution_cost_contract_hash": EXECUTION_COST_HASH,
            "risk_policy_version": INITIAL_POLICY_VERSION,
            "risk_policy_hash": INITIAL_POLICY_HASH,
            "broker_snapshot_hash": snapshot_hash,
            "strategy_nav_usd": strategy_nav,
            "strategy_nav_post_hash": nav_hash,
            "economics_quote_batch_id": quote_batch_id,
            "economics_quote_asof": quote_asof.isoformat(),
            "payoff_hash": payoff_hash,
            "economics_calculation_hash": canonical_hash(economics.hash_payload()),
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
    )


def _services(**overrides: object) -> OptionsCopilotServices:
    values: dict[str, object] = {
        "health_provider": lambda: {},
        "bootstrap_provider": lambda: {},
        "candidates_provider": lambda: [],
        "positions_provider": lambda: [],
        "learning_provider": lambda: {},
        "approval_handler": None,
        "approval_status_provider": None,
    }
    values.update(overrides)
    return OptionsCopilotServices(**values)  # type: ignore[arg-type]


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)


def _read(name: str) -> str:
    return (FRONTEND / name).read_text(encoding="utf-8")
