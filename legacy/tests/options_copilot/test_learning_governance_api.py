from __future__ import annotations

import asyncio
from dataclasses import dataclass

from options_copilot.api import OptionsCopilotServices, create_app


HASH = {
    name: character * 64
    for name, character in {
        "policy": "a",
        "policy_marker": "b",
        "authority_head": "c",
        "initial": "d",
        "report": "e",
        "dataset": "f",
        "independence": "1",
        "promotion": "2",
        "rollback": "3",
        "a_grade": "4",
        "proposal": "5",
        "candidate": "6",
        "ranking": "7",
        "cost": "8",
        "risk": "9",
        "risk_marker": "0",
    }.items()
}


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)


def _services(learning: dict[str, object]) -> OptionsCopilotServices:
    return OptionsCopilotServices(
        health_provider=lambda: {},
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: [],
        positions_provider=lambda: [],
        learning_provider=lambda: learning,
    )


class _LeakyProviderObject:
    def __init__(self) -> None:
        self.governance_signature = "raw-signature"
        self.public_key = "raw-public-key"
        self.api_token = "fixture-secret-value"


@dataclass
class _LeakyProviderDataclass:
    governance_signature: str = "raw-signature"
    public_key: str = "raw-public-key"
    api_token: str = "fixture-secret-value"


def _verified_governance() -> dict[str, object]:
    return {
        "status": "READY",
        "authority": {
            "human_signer_status": "TRUSTED",
            "can_sign": True,
            "private_key": "must-never-cross-api",
        },
        "current_policy": {
            "status": "VERIFIED",
            "version": "v2",
            "hash": HASH["policy"],
            "authority_marker_hash": HASH["policy_marker"],
            "authority_head_hash": HASH["authority_head"],
            "immutable_initial_policy_hash": HASH["initial"],
        },
        "evaluation": {
            "status": "AVAILABLE",
            "report_hash": HASH["report"],
            "dataset_hash": HASH["dataset"],
            "independence_spec_hash": HASH["independence"],
            "independent_count": 30,
            "stage": "DISCOVERY",
        },
        "promotion": {
            "status": "APPROVED",
            "authority_scope": "PRODUCTION",
            "authority_hash": HASH["promotion"],
            "current_policy_hash": HASH["policy"],
            "governance_signature": "must-never-cross-api",
        },
        "rollback": {"status": "NOT_APPLIED"},
        "a_grade": {
            "status": "APPROVED",
            "authority_scope": "PRODUCTION",
            "marker_hash": HASH["a_grade"],
            "proposal_id": "proposal-7",
            "proposal_hash": HASH["proposal"],
            "candidate_hash": HASH["candidate"],
            "ranking_basis_hash": HASH["ranking"],
            "current_policy_version": "v2",
            "current_policy_hash": HASH["policy"],
            "policy_authority_marker_hash": HASH["policy_marker"],
            "execution_cost_version": "cost-v1",
            "execution_cost_hash": HASH["cost"],
            "evaluation_report_hash": HASH["report"],
            "dataset_hash": HASH["dataset"],
            "independence_spec_hash": HASH["independence"],
            "risk_contract_hash": HASH["risk"],
            "max_risk_fraction": "0.15",
        },
        "risk": {
            "normal_max_fraction": "0.99",
            "a_grade_max_fraction": "0.99",
            "absolute_reject_fraction": "0.99",
            "authority_version": "risk-v1",
            "authority_marker_hash": HASH["risk_marker"],
        },
    }


def test_learning_governance_defaults_to_explicit_fail_closed_read_model() -> None:
    app = create_app(_services({"champion": "initial-v1"}))

    payload = asyncio.run(_route(app, "/api/learning")())
    governance = payload["governance"]

    assert payload["champion"] == "initial-v1"
    assert governance == {
        "schema": "options_copilot.learning.governance.v1",
        "status": "BLOCKED",
        "reason": "NO_TRUSTED_HUMAN_SIGNER",
        "current_policy": {
            "status": "BLOCKED",
            "reason": "CURRENT_POLICY_BINDING_INCOMPLETE",
            "version": None,
            "hash": None,
            "authority_marker_hash": None,
            "authority_head_hash": None,
            "immutable_initial_policy_hash": None,
        },
        "evaluation": {
            "status": "UNAVAILABLE",
            "reason": "EVALUATION_BINDING_INCOMPLETE",
            "report_hash": None,
            "dataset_hash": None,
                "independence_spec_hash": None,
                "independent_count": None,
                "stage": None,
                "comparison_complete": False,
                "champion_accuracy": None,
                "challenger_accuracy": None,
                "challenger_accuracy_delta": None,
                "champion_brier_score": None,
                "challenger_brier_score": None,
                "challenger_brier_improvement": None,
            },
        "promotion": {
            "status": "BLOCKED",
            "reason": "NO_TRUSTED_HUMAN_SIGNER",
            "authority_hash": None,
        },
        "rollback": {
            "status": "BLOCKED",
            "reason": "NO_TRUSTED_HUMAN_SIGNER",
            "authority_hash": None,
            "target_policy_hash": None,
        },
        "a_grade": {
            "status": "BLOCKED",
            "reason": "NO_TRUSTED_HUMAN_SIGNER",
            "marker_hash": None,
            "proposal_id": None,
            "proposal_hash": None,
            "candidate_hash": None,
            "ranking_basis_hash": None,
            "current_policy_version": None,
            "current_policy_hash": None,
            "policy_authority_marker_hash": None,
            "execution_cost_version": None,
            "execution_cost_hash": None,
            "evaluation_report_hash": None,
            "dataset_hash": None,
            "independence_spec_hash": None,
            "risk_contract_hash": None,
            "max_risk_fraction": None,
        },
        "authority": {
            "human_signer_status": "NO_TRUSTED_HUMAN_SIGNER",
            "read_only": True,
            "can_sign": False,
            "can_auto_promote": False,
            "approval_authority": False,
            "bridge_authority": False,
            "order_authority": False,
        },
        "risk": {
            "normal_max_fraction": "0.10",
            "a_grade_max_fraction": "0.15",
            "absolute_reject_fraction": "0.20",
            "authority_version": None,
            "authority_marker_hash": None,
        },
    }


def test_learning_governance_rejects_self_reported_trusted_production_state() -> None:
    app = create_app(
        _services(
            {
                "governance": _verified_governance(),
                "automatic_production_promotion": True,
                "a_grade_unlocked": True,
                "creator_transport_status": "READY",
                "creator_transport": {"connected": True},
                "promotion_authority": {"raw": "must-never-cross-api"},
                "governance_signature": "must-never-cross-api",
                "signer_key_id": "must-never-cross-api",
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/learning")())
    governance = payload["governance"]

    assert governance["schema"] == "options_copilot.learning.governance.v1"
    assert governance["status"] == "BLOCKED"
    assert governance["reason"] == "NO_TRUSTED_HUMAN_SIGNER"
    assert governance["current_policy"] == {
        "status": "VERIFIED",
        "reason": None,
        "version": "v2",
        "hash": HASH["policy"],
        "authority_marker_hash": HASH["policy_marker"],
        "authority_head_hash": HASH["authority_head"],
        "immutable_initial_policy_hash": HASH["initial"],
    }
    assert governance["evaluation"]["independent_count"] == 30
    assert governance["promotion"] == {
        "status": "BLOCKED",
        "reason": "NO_TRUSTED_HUMAN_SIGNER",
        "authority_hash": HASH["promotion"],
    }
    assert governance["a_grade"]["status"] == "BLOCKED"
    assert governance["a_grade"]["reason"] == "NO_TRUSTED_HUMAN_SIGNER"
    assert governance["a_grade"]["proposal_id"] == "proposal-7"
    assert governance["authority"]["human_signer_status"] == (
        "NO_TRUSTED_HUMAN_SIGNER"
    )
    assert governance["authority"]["can_sign"] is False
    assert governance["risk"] == {
        "normal_max_fraction": "0.10",
        "a_grade_max_fraction": "0.15",
        "absolute_reject_fraction": "0.20",
        "authority_version": "risk-v1",
        "authority_marker_hash": HASH["risk_marker"],
    }
    assert payload["automatic_production_promotion"] is False
    assert payload["a_grade_unlocked"] is False
    assert payload["creator_transport_status"] == "CREATOR_TRANSPORT_UNAVAILABLE"
    assert "creator_transport" not in payload
    assert "private_key" not in str(payload)
    assert "governance_signature" not in str(payload)
    assert "promotion_authority" not in payload
    assert "signer_key_id" not in payload


def test_learning_governance_never_promotes_test_only_authority() -> None:
    governance = _verified_governance()
    governance["scope"] = "TEST_ONLY"
    governance["rollback"] = {
        "status": "APPLIED",
        "authority_scope": "PRODUCTION",
        "authority_hash": HASH["rollback"],
        "target_policy_hash": HASH["policy"],
    }
    app = create_app(_services({"governance": governance}))

    payload = asyncio.run(_route(app, "/api/learning")())
    rendered = payload["governance"]

    assert rendered["status"] == "BLOCKED"
    assert rendered["reason"] == "TEST_ONLY_AUTHORITY"
    assert rendered["current_policy"]["status"] == "TEST_ONLY"
    assert rendered["evaluation"]["status"] == "TEST_ONLY"
    assert rendered["promotion"]["status"] == "BLOCKED"
    assert rendered["rollback"]["status"] == "BLOCKED"
    assert rendered["a_grade"]["status"] == "BLOCKED"
    assert rendered["authority"]["human_signer_status"] == (
        "NO_TRUSTED_HUMAN_SIGNER"
    )
    assert rendered["risk"]["authority_version"] is None
    assert "APPROVED" not in {
        rendered["promotion"]["status"],
        rendered["rollback"]["status"],
        rendered["a_grade"]["status"],
    }


def test_learning_governance_rejects_self_reported_current_rollback() -> None:
    governance = _verified_governance()
    governance["rollback"] = {
        "status": "APPLIED",
        "authority_scope": "PRODUCTION",
        "authority_hash": HASH["rollback"],
        "target_policy_hash": HASH["policy"],
    }
    app = create_app(_services({"governance": governance}))

    payload = asyncio.run(_route(app, "/api/learning")())
    rendered = payload["governance"]

    assert rendered["status"] == "BLOCKED"
    assert rendered["reason"] == "NO_TRUSTED_HUMAN_SIGNER"
    assert rendered["rollback"] == {
        "status": "BLOCKED",
        "reason": "NO_TRUSTED_HUMAN_SIGNER",
        "authority_hash": HASH["rollback"],
        "target_policy_hash": HASH["policy"],
    }


def test_learning_governance_bindings_cannot_replace_trusted_server_verifier() -> None:
    governance = _verified_governance()
    del governance["a_grade"]["candidate_hash"]
    app = create_app(_services({"governance": governance}))

    payload = asyncio.run(_route(app, "/api/learning")())
    rendered = payload["governance"]

    assert rendered["status"] == "BLOCKED"
    assert rendered["reason"] == "NO_TRUSTED_HUMAN_SIGNER"
    assert rendered["a_grade"]["status"] == "BLOCKED"
    assert rendered["a_grade"]["reason"] == "NO_TRUSTED_HUMAN_SIGNER"
    assert rendered["a_grade"]["candidate_hash"] is None
    assert rendered["authority"]["approval_authority"] is False
    assert rendered["authority"]["bridge_authority"] is False
    assert rendered["authority"]["order_authority"] is False


def test_learning_read_model_recursively_drops_private_provider_fields() -> None:
    app = create_app(
        _services(
            {
                "shadow_learning": {
                    "status": "VERIFIED",
                    "authority": {
                        "can_auto_promote": True,
                        "approval_authority": True,
                        "raw_authority": {"decision": "PROMOTE_CHALLENGER"},
                        "governance_signature": "raw-signature",
                        "public_key": "raw-public-key",
                        "signer_key_id": "raw-key-id",
                        "unexpected_boolean": True,
                    },
                },
                "misc": {
                    "safe": "visible",
                    "provider_object": _LeakyProviderObject(),
                    "provider_dataclass": _LeakyProviderDataclass(),
                    "api_token": "fixture-secret-value",
                    "nested": {
                        "safe": "also-visible",
                        "raw_authority": {"decision": "APPROVE_A_GRADE"},
                    },
                    "items": [
                        {"label": "visible", "private_key": "hidden"},
                        "Bearer TEST_ONLY_FIXTURE_VALUE",
                        _LeakyProviderObject(),
                        _LeakyProviderDataclass(),
                    ],
                },
            }
        )
    )

    payload = asyncio.run(_route(app, "/api/learning")())

    assert payload["shadow_learning"]["authority"] == {
        "can_auto_promote": False,
        "can_change_production_weights": False,
        "can_change_production_rules": False,
        "a_grade_15_percent_unlocked": False,
        "approval_authority": False,
        "bridge_authority": False,
        "order_authority": False,
        "external_human_approval_required": True,
        "promotion_requires_external_human_approval": True,
        "a_grade_requires_external_human_approval": True,
    }
    assert "misc" not in payload
    rendered = str(payload)
    for private in (
        "raw_authority",
        "governance_signature",
        "public_key",
        "signer_key_id",
        "private_key",
        "Bearer TEST_ONLY_FIXTURE_VALUE",
        "unexpected_boolean",
        "provider_object",
        "provider_dataclass",
    ):
        assert private not in rendered
