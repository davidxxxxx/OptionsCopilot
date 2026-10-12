from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from html.parser import HTMLParser
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from options_copilot.api.app import (
    APPROVAL_CONFIRMATION_TOKEN,
    ApprovalConfirmationRequest,
    OptionsCopilotServices,
    RankOneAuthorizationForbidden,
    RankOneChallengeRequest,
    create_app,
)
from options_copilot.ranking.evidence_manifest import (
    build_candidate_evidence_manifest,
)


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"


def test_p6_identifiers_only_rank_one_challenge_and_persistent_confirm_contract() -> None:
    calls: list[tuple[object, ...]] = []

    def challenge(snapshot_id: str, candidate_id: str) -> dict[str, object]:
        calls.append(("challenge", snapshot_id, candidate_id))
        return {
            "challenge_id": "challenge-1",
            "challenge_response": "nonce-0123456789abcdef0123456789abcdef",
            "status": "PENDING_SECOND_CONFIRMATION",
            "expires_at": "2026-08-04T15:05:00Z",
            "review_only": True,
            "order_submitted": False,
            "transmitted_to_broker": False,
        }

    def confirm(
        challenge_id: str, request: dict[str, object]
    ) -> dict[str, object]:
        calls.append(("confirm", challenge_id, request))
        return {
            "approval_id": "approval-1",
            "proposal_hash": "a" * 64,
            "status": "PENDING_CODEX_BRIDGE",
            "expires_at": "2026-08-04T15:05:00Z",
            "status_url": "/api/approvals/approval-1",
            "instruction_id": None,
            "ibkr_deep_link": None,
            "review_only": True,
            "order_submitted": False,
            "transmitted_to_broker": False,
        }

    app = create_app(
        _services(
            latest_scan_provider=lambda: {
                "scan_run_id": "scan-1",
                "decision": "TRADE",
                "record_hash": "b" * 64,
            },
            latest_ranking_provider=lambda: _ranking(),
            ranking_provider=lambda snapshot_id: {
                **_ranking(),
                "ranking_snapshot_id": snapshot_id,
            },
            candidate_evidence_provider=lambda scan_id, candidate_id: {
                "status": "READY",
                "decision": "OBSERVATION_ONLY",
                "scan_run_id": scan_id,
                "candidate_id": candidate_id,
                "ranking_snapshot_id": "ranking-1",
                "schema": "options_copilot.candidate_evidence_manifest.v1",
                "symbol": "SPY",
                "cutoff_at": "2026-08-04T15:00:00+00:00",
                "manifest_hash": "a" * 64,
                "primary": [
                    {
                        "kind": "BROKER_SNAPSHOT",
                        "source": "IBKR_READ_ONLY",
                        "record": {
                            "symbol": "SPY",
                            "observed_at": "2026-08-04T14:59:59+00:00",
                        },
                        "record_hash": "d" * 64,
                    }
                ],
                "supporting": [
                    {
                        "evidence_id": "evidence-1",
                        "content_hash": "b" * 64,
                        "row_hash": "c" * 64,
                        "source": "JIN10",
                        "payload": {"title": "Frozen headline"},
                    }
                ],
                "contradicting": [],
            },
            management_provider=lambda: {
                "status": "NO_TRADE",
                "available": True,
                "mode": "POSITION_MANAGEMENT",
                "actions": [],
            },
            rank_one_challenge_handler=challenge,
            challenge_confirmation_handler=confirm,
        )
    )

    scans = asyncio.run(_route(app, "/api/scans/latest")())
    latest = asyncio.run(_route(app, "/api/rankings/latest")())
    ranking = asyncio.run(
        _route(app, "/api/rankings/{ranking_snapshot_id}")("ranking-1")
    )
    evidence = asyncio.run(
        _route(
            app,
            "/api/scans/{scan_run_id}/candidates/{candidate_id}/evidence",
        )("scan-1", "candidate-1")
    )
    management = asyncio.run(_route(app, "/api/management/current")())
    challenge_result = asyncio.run(
        _route(
            app,
            "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
        )("ranking-1", "candidate-1", RankOneChallengeRequest())
    )
    confirm_result = asyncio.run(
        _route(app, "/api/approval-challenges/{challenge_id}/confirm")(
            "challenge-1",
            ApprovalConfirmationRequest(
                challenge_response="nonce-0123456789abcdef0123456789abcdef",
                risk_acknowledged=True,
                second_confirmation=True,
                confirmation_token=APPROVAL_CONFIRMATION_TOKEN,
            ),
        )
    )

    assert scans["scan_run_id"] == "scan-1"
    assert latest["candidates"][0]["interaction"] == "CHALLENGE_ALLOWED"
    assert latest["candidates"][0]["alternatives"][0]["interaction"] == "VIEW_ONLY"
    assert ranking["ranking_snapshot_id"] == "ranking-1"
    assert evidence["supporting"][0]["source"] == "JIN10"
    assert management["mode"] == "POSITION_MANAGEMENT"
    assert challenge_result["status"] == "PENDING_SECOND_CONFIRMATION"
    assert challenge_result["approval_id"] is None
    assert challenge_result["ibkr_deep_link"] is None
    assert confirm_result["status"] == "PENDING_CODEX_BRIDGE"
    assert confirm_result["instruction_id"] is None
    assert confirm_result["ibkr_deep_link"] is None
    assert calls == [
        ("challenge", "ranking-1", "candidate-1"),
        (
            "confirm",
            "challenge-1",
            {
                "challenge_response": "nonce-0123456789abcdef0123456789abcdef",
                "risk_acknowledged": True,
                "second_confirmation": True,
                "confirmation_token": APPROVAL_CONFIRMATION_TOKEN,
            },
        ),
    ]

    paths = {route.path for route in app.routes}
    assert "/api/proposals/{proposal_id}/approve" not in paths


def test_p6_candidate_evidence_api_uses_strict_nested_display_whitelists() -> None:
    app = create_app(
        _services(
            candidate_evidence_provider=lambda scan_id, candidate_id: {
                "status": "READY",
                "decision": "OBSERVATION_ONLY",
                "decision_authority": "EXECUTION",
                "scan_run_id": scan_id,
                "candidate_id": candidate_id,
                "ranking_snapshot_id": "ranking-1",
                "schema": "options_copilot.candidate_evidence_manifest.v1",
                "symbol": "SPY",
                "cutoff_at": "2026-08-04T15:00:00+00:00",
                "manifest_hash": "a" * 64,
                "approval_enabled": True,
                "direct_order_submission": True,
                "secret_token": "must-not-leak",
                "transport": {"authorization": "must-not-leak"},
                "order": {"order_id": "must-not-leak"},
                "unknown": "must-not-leak",
                "primary": [
                    {
                        "kind": "BROKER_SNAPSHOT",
                        "source": "IBKR_READ_ONLY",
                        "record_hash": "b" * 64,
                        "record": {
                            "symbol": "SPY",
                            "observed_at": "2026-08-04T14:59:59+00:00",
                            "snapshot_hash": "c" * 64,
                            "bid": "1.20",
                            "ask": "1.25",
                            "account_nlv_usd": "999999",
                            "api_key": "must-not-leak",
                            "order_id": "must-not-leak",
                            "transport": {"host": "must-not-leak"},
                        },
                        "unknown": "must-not-leak",
                    }
                ],
                "supporting": [
                    {
                        "sequence": 4,
                        "evidence_id": "ev-supporting",
                        "identity": "news-spy",
                        "kind": "COMPANY_NEWS",
                        "symbol": "SPY",
                        "provider": "SEC",
                        "source_id": "sec-1",
                        "published_at": "2026-08-04T14:50:00+00:00",
                        "first_seen_at": "2026-08-04T14:51:00+00:00",
                        "ingested_at": "2026-08-04T14:52:00+00:00",
                        "observed_at": "2026-08-04T14:53:00+00:00",
                        "content_hash": "d" * 64,
                        "prior_hash": "e" * 64,
                        "row_hash": "f" * 64,
                        "decision_authority": "EXECUTION",
                        "payload": {
                            "title": "Frozen supporting filing",
                            "summary": "Material event",
                            "url": (
                                "https://www.sec.gov/example"
                                "?tracking=remove#fragment"
                            ),
                            "category": "FILING",
                            "event_type": "8-K",
                            "sentiment": {"label": "neutral", "score": 0.1},
                            "authorization": "must-not-leak",
                            "raw_provider_payload": "must-not-leak",
                        },
                    }
                ],
                "contradicting": [
                    {
                        "evidence_id": "ev-contradicting",
                        "content_hash": "1" * 64,
                        "row_hash": "2" * 64,
                        "provider": "COMPANY_IR",
                        "payload": {
                            "title": "Counter evidence",
                            "url": "https://user:password@example.com/private",
                        },
                    }
                ],
            }
        )
    )

    evidence = asyncio.run(
        _route(
            app,
            "/api/scans/{scan_run_id}/candidates/{candidate_id}/evidence",
        )("scan-1", "candidate-1")
    )

    assert evidence["status"] == "READY"
    assert evidence["decision"] == "OBSERVATION_ONLY"
    assert evidence["decision_authority"] == "OBSERVATION_ONLY"
    assert evidence["approval_enabled"] is False
    assert evidence["direct_order_submission"] is False
    assert set(evidence) == {
        "status",
        "decision",
        "decision_authority",
        "scan_run_id",
        "candidate_id",
        "ranking_snapshot_id",
        "schema",
        "symbol",
        "cutoff_at",
        "manifest_hash",
        "primary",
        "supporting",
        "contradicting",
        "reason",
        "reasons",
        "review_only",
        "direct_order_submission",
        "approval_enabled",
    }
    assert set(evidence["primary"][0]) == {
        "kind",
        "source",
        "record",
        "record_hash",
    }
    assert set(evidence["primary"][0]["record"]) == {
        "symbol",
        "observed_at",
        "snapshot_hash",
        "bid",
        "ask",
    }
    external = evidence["supporting"][0]
    assert set(external) == {
        "evidence_id",
        "content_hash",
        "row_hash",
        "identity",
        "kind",
        "symbol",
        "source",
        "source_id",
        "published_at",
        "first_seen_at",
        "ingested_at",
        "observed_at",
        "effective_status",
        "decision_authority",
        "payload",
    }
    assert external["decision_authority"] == "SUPPORTING_ONLY"
    assert external["payload"]["public_url"] == "https://www.sec.gov/example"
    assert "public_url" not in evidence["contradicting"][0]["payload"]
    rendered = repr(evidence).lower()
    for forbidden in (
        "secret_token",
        "authorization",
        "api_key",
        "order_id",
        "transport",
        "raw_provider_payload",
        "must-not-leak",
        "tracking",
        "fragment",
        "prior_hash",
        "sequence",
    ):
        assert forbidden not in rendered


@pytest.mark.parametrize(
    "mutation",
    (
        "empty_primary",
        "missing_scan_run_id",
        "missing_candidate_id",
        "missing_ranking_snapshot_id",
        "missing_symbol",
        "missing_cutoff_at",
        "malformed_scan_run_id",
        "naive_cutoff_at",
    ),
)
def test_p6_candidate_evidence_claimed_ready_requires_complete_identity_and_primary(
    mutation: str,
) -> None:
    payload: dict[str, object] = {
        "status": "READY",
        "decision": "OBSERVATION_ONLY",
        "scan_run_id": "scan-1",
        "candidate_id": "candidate-1",
        "ranking_snapshot_id": "ranking-1",
        "schema": "options_copilot.candidate_evidence_manifest.v1",
        "symbol": "SPY",
        "cutoff_at": "2026-08-04T15:00:00+00:00",
        "manifest_hash": "a" * 64,
        "primary": [
            {
                "kind": "BROKER_SNAPSHOT",
                "source": "IBKR_READ_ONLY",
                "record": {
                    "symbol": "SPY",
                    "observed_at": "2026-08-04T14:59:59+00:00",
                },
                "record_hash": "b" * 64,
            }
        ],
        "supporting": [],
        "contradicting": [],
    }
    if mutation == "empty_primary":
        payload["primary"] = []
    elif mutation == "malformed_scan_run_id":
        payload["scan_run_id"] = "bad scan id"
    elif mutation == "naive_cutoff_at":
        payload["cutoff_at"] = "2026-08-04T15:00:00"
    else:
        payload.pop(mutation.removeprefix("missing_"))

    app = create_app(
        _services(
            candidate_evidence_provider=lambda _scan, _candidate: payload
        )
    )
    evidence = asyncio.run(
        _route(
            app,
            "/api/scans/{scan_run_id}/candidates/{candidate_id}/evidence",
        )("scan-1", "candidate-1")
    )

    assert evidence["status"] == "DEGRADED"
    assert evidence["decision"] == "NO_TRADE"
    assert evidence["reason"] == "CANDIDATE_EVIDENCE_API_PROJECTION_INVALID"
    assert "CANDIDATE_EVIDENCE_API_PROJECTION_INVALID" in evidence["reasons"]
    assert evidence["primary"] == []
    assert evidence["supporting"] == []
    assert evidence["contradicting"] == []
    assert evidence["decision_authority"] == "OBSERVATION_ONLY"
    assert evidence["approval_enabled"] is False


def test_p6_challenge_payload_forbids_client_rank_proposal_and_authority_hashes() -> None:
    for forbidden in (
        {"rank": 1},
        {"proposal": {"candidate_id": "candidate-1"}},
        {"current_policy_hash": "a" * 64},
        {"cost_hash": "b" * 64},
        {"risk_authority_marker_hash": "c" * 64},
    ):
        with pytest.raises(ValidationError):
            RankOneChallengeRequest.model_validate(forbidden)

    with pytest.raises(ValidationError):
        ApprovalConfirmationRequest.model_validate(
            {
                "challenge_response": "nonce-0123456789abcdef0123456789abcdef",
                "risk_acknowledged": True,
                "second_confirmation": True,
                "confirmation_token": APPROVAL_CONFIRMATION_TOKEN,
                "ranking_snapshot_id": "ranking-1",
            }
        )


def test_p6_rank_two_and_missing_management_fail_closed() -> None:
    app = create_app(
        _services(
            rank_one_challenge_handler=lambda _snapshot, _candidate: (
                _ for _ in ()
            ).throw(RankOneAuthorizationForbidden("VIEW_ONLY"))
        )
    )

    with pytest.raises(HTTPException) as denied:
        asyncio.run(
            _route(
                app,
                "/api/rankings/{ranking_snapshot_id}/candidates/{candidate_id}/challenge",
            )("ranking-1", "candidate-2", RankOneChallengeRequest())
        )
    assert denied.value.status_code == 403
    assert denied.value.detail == "VIEW_ONLY"

    with pytest.raises(HTTPException) as unavailable:
        asyncio.run(_route(app, "/api/management/current")())
    assert unavailable.value.status_code == 503
    assert "NO_TRADE" in str(unavailable.value.detail)


def test_management_api_is_a_strict_preview_only_projection() -> None:
    app = create_app(
        _services(
            management_provider=lambda: {
                "status": "READY",
                "decision": "PREVIEW_ONLY",
                "available": True,
                "approval_enabled": True,
                "direct_order_submission": True,
                "order_submitted": True,
                "transmitted_to_broker": True,
                "external_attempt_count": 99,
                "instruction_count": 99,
                "action_enabled": True,
                "untrusted_top_level": "must-not-project",
                "candidates": [
                    {
                        "candidate_id": "gld-close-all",
                        "management_kind": "CLOSE_ALL",
                        "candidate_hash": "a" * 64,
                        "transition_proof_hash": "b" * 64,
                        "broker_snapshot_hash": "c" * 64,
                        "quote_batch_hash": "d" * 64,
                        "oldest_quote_age_seconds": "1",
                        "maximum_leg_skew_seconds": "0.2",
                        "all_in_close_cashflow_usd": "195",
                        "execution_legs": [
                            {
                                "contract_id": 101,
                                "local_symbol": "GLD  260821C00375000",
                                "expiration": "2026-08-21",
                                "strike": "375",
                                "right": "CALL",
                                "current_signed_quantity": 1,
                                "signed_quantity_delta": -1,
                                "action": "SELL",
                                "action_quantity": 1,
                                "multiplier": "100",
                                "bid": "12.10",
                                "ask": "12.30",
                                "executable_price": "12.10",
                                "quote_observed_at": "2026-08-04T14:30:00Z",
                                "secdef_identity_hash": "e" * 64,
                                "side": "must-not-project",
                                "account_id": "must-not-project",
                            }
                        ],
                        "approval_enabled": True,
                        "direct_order_submission": True,
                        "untrusted_candidate_field": "must-not-project",
                        "transition_proof": {
                            "management_kind": "CLOSE_ALL",
                            "proof_hash": "b" * 64,
                            "before_positions": [
                                {
                                    "contract_id": 101,
                                    "local_symbol": "GLD  260821C00375000",
                                    "signed_quantity": 1,
                                    "account_id": "must-not-project",
                                }
                            ],
                            "after_positions": [],
                            "before_risk": {"max_loss_usd": "1000"},
                            "after_risk": {"max_loss_usd": "0"},
                        },
                    }
                ],
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/management/current")())

    assert payload["mode"] == "POSITION_MANAGEMENT"
    assert payload["review_only"] is True
    assert payload["approval_enabled"] is False
    assert payload["direct_order_submission"] is False
    assert payload["order_submitted"] is False
    assert payload["transmitted_to_broker"] is False
    assert payload["external_attempt_count"] == 0
    assert payload["instruction_count"] == 0
    assert payload["action_enabled"] is False
    assert payload["creator_transport_status"] == "CREATOR_TRANSPORT_UNAVAILABLE"
    candidate = payload["candidates"][0]
    assert candidate["approval_enabled"] is False
    assert candidate["direct_order_submission"] is False
    assert candidate["execution_legs"] == [
        {
            "contract_id": 101,
            "local_symbol": "GLD  260821C00375000",
            "expiration": "2026-08-21",
            "strike": "375",
            "right": "CALL",
            "current_signed_quantity": 1,
            "signed_quantity_delta": -1,
            "action": "SELL",
            "action_quantity": 1,
            "multiplier": "100",
            "bid": "12.10",
            "ask": "12.30",
            "executable_price": "12.10",
            "quote_observed_at": "2026-08-04T14:30:00Z",
            "secdef_identity_hash": "e" * 64,
        }
    ]
    assert candidate["transition_proof"]["before_positions"][0] == {
        "contract_id": 101,
        "local_symbol": "GLD  260821C00375000",
        "signed_quantity": 1,
    }
    rendered = repr(payload)
    assert "must-not-project" not in rendered
    assert "untrusted_" not in rendered


class _TemplateParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()

    def handle_starttag(self, _tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if values.get("id"):
            self.ids.add(str(values["id"]))


def test_p6_frontend_has_five_regions_and_rank_one_only_action_wiring() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    parser = _TemplateParser()
    parser.feed(html)

    assert {
        "readiness-region",
        "position-management-region",
        "management-preview-list",
        "management-review-action",
        "management-action-reason",
        "top-three-region",
        "evidence-exit-region",
        "final-review-region",
    } <= parser.ids
    assert "observable/reconciliation only" in html
    assert "Strategy NAV" in html
    assert 'rankings: "/api/rankings/latest"' in script
    assert 'scans: "/api/scans/latest"' in script
    assert 'management: "/api/management/current"' in script
    assert "appendRankOneChallengeAction" in script
    assert "if (rank === 1" in script
    assert 'interaction === "CHALLENGE_ALLOWED"' in script
    assert "VIEW_ONLY" in script
    assert "/api/proposals/" not in script
    assert "/challenge`" in script
    assert "/api/approval-challenges/" in script
    assert "innerHTML" not in script
    assert "buildManagementPreview" in script
    assert "Create IBKR review instruction" in html
    assert "CREATOR_TRANSPORT_UNAVAILABLE" in html


def _ranking() -> dict[str, object]:
    cutoff_at = datetime.now(timezone.utc)
    candidate_body = {
        "candidate_id": "candidate-1",
        "symbol": "SPY",
        "structure": "DEBIT_VERTICAL",
        "legs": (
            {
                "contract_id_ex": "OPT:101",
                "underlying": "SPY",
                "security_type": "OPT",
                "expiration": "2026-08-21",
                "strike": "500",
                "right": "CALL",
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
                "local_symbol": "SPY  260821C00500000",
                "trading_class": "SPY",
                "con_id": 101,
                "side": "LONG",
                "ratio": 1,
                "bid": "2.00",
                "ask": "2.10",
                "observed_at": cutoff_at.isoformat(),
            },
            {
                "contract_id_ex": "OPT:102",
                "underlying": "SPY",
                "security_type": "OPT",
                "expiration": "2026-08-21",
                "strike": "505",
                "right": "CALL",
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
                "local_symbol": "SPY  260821C00505000",
                "trading_class": "SPY",
                "con_id": 102,
                "side": "SHORT",
                "ratio": 1,
                "bid": "1.00",
                "ask": "1.10",
                "observed_at": cutoff_at.isoformat(),
            },
        ),
        "debit_usd": "100",
        "credit_usd": "0",
        "all_in_cost_usd": "100",
        "max_loss_usd": "100",
        "max_profit_usd": "100",
        "breakevens": ("100",),
        "liquidity_score": "8",
        "dte": 20,
        "broker_snapshot_hash": "b" * 64,
        "quote_batch_id": "batch-1",
        "secdef_hash": "c" * 64,
        "execution_cost_contract_version": "cost-v1",
        "execution_cost_contract_hash": "e" * 64,
        "dte_exception_hash": None,
        "strategy_nav_usd": "1000",
        "risk_fraction": "0.1",
    }
    proposal_body = {
        "candidate_id": "candidate-1",
        "proposal_id": "candidate-1",
        "symbol": "SPY",
        "underlying": "SPY",
        "structure": "DEBIT_VERTICAL",
        "dte": 20,
        "quote_snapshot_id": "batch-1",
        "expected_value_usd": "10",
        "broker_snapshot_hash": "b" * 64,
        "secdef_hash": "c" * 64,
        "execution_cost_contract": {"version": "cost-v1", "hash": "e" * 64},
        "policy": {"dte_exception_hash": None},
        "pricing": {
            "reference_cost_usd": "100",
            "estimated_execution_costs_usd": "0",
            "all_in_executable_cost_usd": "100",
        },
        "risk": {
            "maximum_loss_usd": "100",
            "maximum_profit_usd": "100",
            "breakevens": ["100"],
            "risk_fraction": "0.1",
        },
        "legs": [
            {
                **{
                    key: leg[key]
                    for key in (
                        "contract_id_ex",
                        "underlying",
                        "security_type",
                        "expiration",
                        "strike",
                        "right",
                        "multiplier",
                        "currency",
                        "exchange",
                        "local_symbol",
                        "trading_class",
                        "con_id",
                        "ratio",
                        "bid",
                        "ask",
                    )
                },
                "side": "BUY" if leg["side"] == "LONG" else "SELL",
                "quote_snapshot_id": "batch-1",
                "quote_time": leg["observed_at"],
            }
            for leg in candidate_body["legs"]
        ],
    }
    manifest = build_candidate_evidence_manifest(
        candidate_body,
        after_cost_expected_value=Decimal("10"),
        cutoff_at=cutoff_at,
        ranking_valid_until=cutoff_at + timedelta(minutes=5),
        now=cutoff_at,
    )
    candidates = []
    for rank in range(1, 4):
        candidates.append(
            {
                "rank": rank,
                "candidate_id": f"candidate-{rank}",
                "authorizable": True,
                "authority_status": "NORMAL",
                "candidate_body": {
                    "candidate_id": f"candidate-{rank}",
                    "symbol": "SPY",
                    "max_loss_usd": 100.0,
                    "strategy_nav_usd": 1000.0,
                    "risk_fraction": 0.1,
                },
                "proposal_body": {"candidate_id": f"candidate-{rank}"},
                "score_components": {"after_cost_expected_value": "10"},
            }
        )
    candidates[0]["candidate_body"] = candidate_body
    candidates[0]["proposal_body"] = proposal_body
    return {
        "scan_run_id": "scan-1",
        "ranking_snapshot_id": "ranking-1",
        "approval_enabled": True,
        "decision": "TRADE",
        "valid_until": (cutoff_at + timedelta(minutes=5)).isoformat(),
        "broker_snapshot_hash": "b" * 64,
        "current_policy_version": "initial-v1",
        "current_policy_hash": "d" * 64,
        "cost_version": "cost-v1",
        "cost_hash": "e" * 64,
        "risk_authority_version": "normal-v1",
        "risk_authority_marker_hash": "f" * 64,
        "candidates": candidates,
        "immutable_inputs": {
            "candidate_evidence_manifests": {
                "candidate-1": manifest,
            }
        },
    }


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
