"""Fail-closed contracts for the production Options Copilot composition graph."""
from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.runtime import RuntimeServices, _unavailable_runtime_services
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)


class _CapablePort:
    """Narrow test double that satisfies every RuntimeServices capability."""

    def acquire(self, **_: object) -> dict[str, object]:
        return {}

    def append_decisions(self, **_: object) -> tuple[object, ...]:
        return ()

    def append_snapshot(self, **_: object) -> dict[str, object]:
        return {}

    def assert_current(self, *_: object, **__: object) -> bool:
        return True

    def authorize_frozen_rank_one(self, *_: object, **__: object) -> None:
        return None

    def build(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def confirm_challenge(self, *_: object, **__: object) -> None:
        return None

    def current_terminal_for_snapshot(self, *_: object, **__: object) -> bool:
        return True

    def create_challenge(self, *_: object, **__: object) -> None:
        return None

    def evaluate(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def evaluate_pre_cost(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def generate(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def get(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def get_by_scan_run(self, *_: object, **__: object) -> None:
        return None

    def guard_current(self, value: object, *, callback):
        return callback()

    def get_challenge(self, *_: object, **__: object) -> None:
        return None

    def is_current(self, *_: object, **__: object) -> bool:
        return True

    def latest(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def list_pending(self, *_: object, **__: object) -> tuple[object, ...]:
        return ()

    def management(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def query(self, *_: object, **__: object) -> tuple[object, ...]:
        return ()

    def rank(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def read_model(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def read_snapshot(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def reconciliation_status(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def resolve(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def resolve_contracts(self, **_: object) -> tuple[object, ...]:
        return ()

    def run(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def runs_for_slot(self, *_: object, **__: object) -> tuple[object, ...]:
        return ()

    def snapshot(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def validate(self, *_: object, **__: object) -> dict[str, object]:
        return {}

    def verify_integrity(self) -> bool:
        return True


class _PipelineInputs(_CapablePort):
    def __init__(self, position_manager: object) -> None:
        self.position_manager = position_manager


class _BrokerEvidence(_CapablePort):
    def __init__(
        self,
        broker_snapshot_builder: object,
        options_evidence_acquisition: object,
        evidence_store: object,
        policy_resolver: object,
        strategy_nav_source: object,
    ) -> None:
        self.broker_snapshot_builder = broker_snapshot_builder
        self.options_evidence_acquisition = options_evidence_acquisition
        self.evidence_store = evidence_store
        self.policy_resolver = policy_resolver
        self.strategy_nav_source = strategy_nav_source


class _Eligibility(_CapablePort):
    def __init__(
        self, risk_gate: object, dte_gate: object, single_combination_gate: object
    ) -> None:
        self.risk_gate = risk_gate
        self.dte_gate = dte_gate
        self.single_combination_gate = single_combination_gate


class _Pipeline:
    def __init__(self, graph: dict[str, object]) -> None:
        self.inputs = graph["pipeline_inputs"]
        self.universe_funnel = graph["universe_funnel"]
        self.broker_evidence = graph["broker_evidence_acquisition"]
        self.strategy_registry = graph["strategy_registry"]
        self.strategy_generator = graph["strategy_candidate_generator"]
        self.volatility_engine = graph["volatility_engine"]
        self.scenario_engine = graph["scenario_engine"]
        self.policy_resolver = graph["policy_resolver"]
        self.risk_authority_resolver = graph["risk_authority_resolver"]
        self.cost_contract = graph["execution_cost_contract"]
        self.eligibility_gate = graph["eligibility_gate"]
        self.portfolio_ranker = graph["portfolio_ranker"]
        self.ranking_store = graph["ranking_store"]

    def run_slot(self, *_: object, **__: object) -> dict[str, object]:
        return {"decision": "NO_TRADE"}


def _complete_graph() -> dict[str, object]:
    names = (
        "broker_snapshot_builder",
        "evidence_store",
        "scan_run_store",
        "universe_funnel",
        "options_evidence_acquisition",
        "strategy_registry",
        "strategy_candidate_generator",
        "volatility_engine",
        "scenario_engine",
        "policy_resolver",
        "risk_authority_resolver",
        "execution_cost_contract",
        "risk_gate",
        "dte_gate",
        "single_combination_gate",
        "portfolio_ranker",
        "ranking_store",
        "strategy_nav_source",
        "position_manager",
        "approval_store",
        "bridge_status_reader",
        "bridge_reconciliation_reader",
    )
    graph = {name: _CapablePort() for name in names}
    graph["pipeline_inputs"] = _PipelineInputs(graph["position_manager"])
    graph["broker_evidence_acquisition"] = _BrokerEvidence(
        graph["broker_snapshot_builder"],
        graph["options_evidence_acquisition"],
        graph["evidence_store"],
        graph["policy_resolver"],
        graph["strategy_nav_source"],
    )
    graph["eligibility_gate"] = _Eligibility(
        graph["risk_gate"], graph["dte_gate"], graph["single_combination_gate"]
    )
    pipeline = _Pipeline(graph)
    graph["decision_pipeline"] = pipeline
    return graph


def test_runtime_services_names_pipeline_inputs() -> None:
    parameters = inspect.signature(RuntimeServices).parameters

    assert "pipeline_inputs" in parameters

    services = RuntimeServices(**_complete_graph())
    assert services.wiring_disagreements == ()
    assert services.invalid_dependencies == ()
    assert services.approval_enabled is True
    assert services.readiness()["decision"] == "READY"


def test_creator_transport_blocker_disables_approval_without_disabling_research() -> None:
    services = RuntimeServices(
        **_complete_graph(),
        approval_blockers=("CREATOR_TRANSPORT_UNAVAILABLE",),
    )

    readiness = services.readiness()
    assert services.decision_enabled is True
    assert services.approval_enabled is False
    assert readiness["status"] == "DEGRADED"
    assert readiness["decision"] == "READY"
    assert readiness["research_enabled"] is True
    assert readiness["approval_enabled"] is False
    assert readiness["approval_blockers"] == (
        "CREATOR_TRANSPORT_UNAVAILABLE",
    )
    assert readiness["direct_order_submission"] is False


def test_feature_diagnostics_preserve_scan_and_approval_gate_semantics(monkeypatch) -> None:
    graph = _complete_graph()
    calls: list[str] = []
    monkeypatch.setattr(
        graph["decision_pipeline"],
        "run_slot",
        lambda scan_run_id, _slot_at: calls.append(scan_run_id)
        or {"decision": "NO_TRADE", "reasons": ("MISSING_MARKET_DIRECTION_INPUT",)},
    )
    monkeypatch.setattr(
        RuntimeServices,
        "_strategy_nav",
        lambda *_args, **_kwargs: (
            SimpleNamespace(authority_hash="a" * 64, contract_hash="b" * 64),
            (),
        ),
    )
    services = RuntimeServices(
        **graph,
        feature_data_chain_reasons=("CANDIDATE_FEATURE_BINDING_UNWIRED",),
    )
    readiness = services.readiness()

    assert readiness["status"] == "DEGRADED"
    assert readiness["readiness_scope"] == "DEPENDENCY_WIRING_ONLY"
    assert readiness["decision"] == "READY"
    assert readiness["research_enabled"] is True
    assert services.decision_enabled is True
    assert services.approval_enabled is True
    assert readiness["decision_reasons"] == ()
    assert readiness["approval_blockers"] == ()
    assert readiness["feature_data_chain"]["model_input_complete"] is False
    assert readiness["reasons"] == ("CANDIDATE_FEATURE_BINDING_UNWIRED",)
    assert calls == []

    result = services.run_slot("scan-feature-diagnostics", NOW)
    assert calls == ["scan-feature-diagnostics"]
    assert result["reasons"] == ("MISSING_MARKET_DIRECTION_INPUT",)


def test_readiness_api_exposes_feature_gaps_without_acquiring_data(monkeypatch) -> None:
    def forbid(*_args, **_kwargs):
        raise AssertionError("readiness must not acquire data or run the pipeline")

    for name in ("acquire", "build", "resolve", "run", "evaluate"):
        monkeypatch.setattr(_CapablePort, name, forbid)
    monkeypatch.setattr(_Pipeline, "run_slot", forbid)
    services = RuntimeServices(
        **_complete_graph(),
        approval_blockers=("CREATOR_TRANSPORT_UNAVAILABLE",),
        feature_data_chain_reasons=(
            "FEATURE_HISTORY_PRODUCER_UNWIRED",
            "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
            "CANDIDATE_FEATURE_BINDING_UNWIRED",
        ),
    )
    app = create_app(OptionsCopilotServices(
        health_provider=forbid,
        bootstrap_provider=forbid,
        candidates_provider=forbid,
        positions_provider=forbid,
        learning_provider=forbid,
        readiness_provider=services.readiness,
    ))

    async def read():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            return await client.get("/api/readiness")

    response = asyncio.run(read())
    assert response.status_code == 200
    payload = response.json()
    assert payload["feature_data_chain"] == {
        "scope": "PRODUCTION_SCENARIO_INPUTS",
        "status": "INCOMPLETE",
        "model_input_complete": False,
        "reason_codes": [
            "FEATURE_HISTORY_PRODUCER_UNWIRED",
            "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
            "CANDIDATE_FEATURE_BINDING_UNWIRED",
        ],
        "decision_authority": "OBSERVATION_ONLY",
        "affects_decision": False,
    }
    assert payload["approval_blockers"] == ["CREATOR_TRANSPORT_UNAVAILABLE"]
    assert payload["decision_reasons"] == []
    assert payload["research_enabled"] is True
    assert payload["approval_enabled"] is False
    supplied_hash = payload.pop("content_hash")
    assert canonical_hash(payload) == supplied_hash


@pytest.mark.parametrize("reasons", [None, (), []])
def test_injected_graph_does_not_claim_feature_readiness_without_assessment(reasons) -> None:
    services = RuntimeServices(**_complete_graph(), feature_data_chain_reasons=reasons)
    payload = services.readiness()["feature_data_chain"]
    assert payload["status"] == "NOT_ASSESSED"
    assert payload["model_input_complete"] is None
    assert payload["reason_codes"] == ()


@pytest.mark.parametrize("reasons", ["READY", ("READY",), (None,), ({},)])
def test_invalid_feature_diagnostic_cannot_claim_complete_or_change_gates(reasons) -> None:
    services = RuntimeServices(**_complete_graph(), feature_data_chain_reasons=reasons)
    payload = services.readiness()
    assert payload["feature_data_chain"]["status"] == "INCOMPLETE"
    assert payload["feature_data_chain"]["reason_codes"] == (
        "FEATURE_DATA_CHAIN_DIAGNOSTIC_INVALID",
    )
    assert services.decision_enabled is True
    assert services.approval_enabled is True


@pytest.mark.parametrize(
    "service_field",
    (
        "pipeline_inputs",
        "broker_evidence_acquisition",
        "options_evidence_acquisition",
        "risk_gate",
        "dte_gate",
        "single_combination_gate",
        "strategy_nav_source",
        "position_manager",
    ),
)
def test_capable_but_orphaned_pipeline_dependency_fails_closed(
    service_field: str,
) -> None:
    graph = _complete_graph()

    # The replacement still satisfies the declared port.  It must nevertheless
    # remain unusable because the pipeline or its production adapter graph is
    # wired to a different object.
    graph[service_field] = _CapablePort()
    services = RuntimeServices(**graph)
    readiness = services.readiness()

    assert service_field not in services.invalid_dependencies
    assert services.wiring_disagreements
    assert services.approval_enabled is False
    assert readiness["status"] == "DEGRADED"
    assert readiness["decision"] == "NO_TRADE"
    assert any(
        reason.startswith("RESOLVER_OR_PIPELINE_DISAGREEMENT:")
        for reason in readiness["reasons"]
    )


@pytest.mark.parametrize(
    "service_field",
    (
        "pipeline_inputs",
        "broker_evidence_acquisition",
        "options_evidence_acquisition",
        "risk_gate",
        "dte_gate",
        "single_combination_gate",
        "strategy_nav_source",
        "position_manager",
    ),
)
def test_capability_placeholder_never_enables_approval(service_field: str) -> None:
    graph = _complete_graph()
    graph[service_field] = object()
    services = RuntimeServices(**graph)
    readiness = services.readiness()

    assert service_field in services.invalid_dependencies
    assert services.approval_enabled is False
    assert readiness["status"] == "DEGRADED"
    assert readiness["decision"] == "NO_TRADE"


def test_default_unavailable_graph_keeps_pipeline_inputs_fail_closed() -> None:
    services = _unavailable_runtime_services(
        approval_store=_CapablePort(),
        bridge_reader=_CapablePort(),
    )
    readiness = services.readiness()

    assert services.pipeline_inputs is None
    assert services.approval_enabled is False
    assert readiness["status"] == "DEGRADED"
    assert readiness["decision"] == "NO_TRADE"
    assert "pipeline_inputs" in readiness["missing_dependencies"]
    assert readiness["approval_enabled"] is False
