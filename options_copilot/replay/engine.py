"""Sealed, authority-free historical DecisionPipeline execution."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from options_copilot.decision import DecisionPipeline, normalize_funnel_trace
from options_copilot.ranking.joint import JointRankingSnapshot
from options_copilot.ranking.portfolio import PortfolioRanker
from options_copilot.ranking.store import RankingStoreConflict
from options_copilot.replay.dataset import (
    DatasetManifest,
    IndependenceSpecArtifactError,
    IndependenceSpecArtifactStore,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


REPLAY_SCHEMA = "options_copilot.replay.result.v3"
REPLAY_AUTHORITY = "REPLAY"
HISTORICAL_ARTIFACT_SCHEMA_V1 = "options_copilot.replay.pipeline_artifact.v1"
HISTORICAL_ARTIFACT_SCHEMA = "options_copilot.replay.pipeline_artifact.v2"
HISTORICAL_PIPELINE_VERSION_V1 = "decision-pipeline-v1"
HISTORICAL_PIPELINE_VERSION_V2 = "decision-pipeline-v2"
_FACTORY_TOKEN = object()
_FORBIDDEN_PORT_NAMES = (
    "broker_write",
    "approval",
    "bridge",
    "creator",
    "instruction_write",
    "order_write",
)
_PIPELINE_RESULT_REQUIRED_FIELDS = frozenset(
    {
        "scan_run_id",
        "status",
        "ranking_snapshot_id",
        "reasons",
        "input_hash",
        "evidence_hash",
        "broker_snapshot_hash",
        "policy_version",
        "policy_hash",
        "current_policy_version",
        "current_policy_hash",
        "policy_authority_marker_hash",
        "cost_version",
        "cost_hash",
        "risk_contract_hash",
        "risk_authority_version",
        "risk_authority_marker_hash",
        "candidate_hashes",
        "ranking_basis_hashes",
        "ranking_snapshot_hash",
        "gate_bundle_hash",
        "result_hash",
    }
)
_PIPELINE_RESULT_OPTIONAL_FIELDS = frozenset(
    {
        "execution_cost_contract_version",
        "execution_cost_contract_hash",
        "funnel_trace",
        "authority",
        "authorizable",
        "approval_eligible",
        "instruction_eligible",
        "order_eligible",
    }
)
_SOURCE_AUTHORITY_BOOLEAN_FIELDS = (
    "authorizable",
    "approval_eligible",
    "instruction_eligible",
    "order_eligible",
)
_DANGEROUS_AUTHORITY_BOOLEAN_ALIASES = (
    "approval",
    "approval_allowed",
    "instruction",
    "instruction_creation_allowed",
    "order",
    "order_submission_allowed",
    "eligible_to_send",
    "live_authority",
)


class ReplayError(RuntimeError):
    pass


class ReplaySafetyError(ReplayError):
    pass


class ReplayBindingError(ReplayError):
    pass


def _verify_artifact_pipeline_version(
    artifact: "HistoricalPipelineArtifact",
    pipeline_version: object,
) -> None:
    expected = (
        HISTORICAL_PIPELINE_VERSION_V2
        if artifact.schema == HISTORICAL_ARTIFACT_SCHEMA
        else HISTORICAL_PIPELINE_VERSION_V1
    )
    if pipeline_version != expected:
        raise ReplayBindingError("PIPELINE_ARTIFACT_VERSION_MISMATCH")


@dataclass(frozen=True, slots=True)
class FrozenHistoricalClock:
    at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "at", utc_datetime(self.at, field="historical clock"))

    def __call__(self) -> datetime:
        return self.at


@dataclass(frozen=True, slots=True)
class HistoricalPipelineArtifact:
    """Canonical data-only recipe for one historical DecisionPipeline run."""

    inputs: Mapping[str, object]
    universe: Mapping[str, object]
    broker_evidence: Mapping[str, object]
    candidates: tuple[Mapping[str, object], ...]
    volatility: Mapping[str, object]
    scenarios: tuple[Mapping[str, object], ...]
    policy: Mapping[str, object]
    risk_authority: Mapping[str, object]
    cost: Mapping[str, object]
    gate: Mapping[str, object]
    artifact_hash: str
    funnel_trace: Mapping[str, object] = field(default_factory=dict)
    schema: str = HISTORICAL_ARTIFACT_SCHEMA_V1

    def __post_init__(self) -> None:
        for name in (
            "inputs",
            "universe",
            "broker_evidence",
            "volatility",
            "policy",
            "risk_authority",
            "cost",
            "gate",
            "funnel_trace",
        ):
            raw = getattr(self, name)
            _require_plain_artifact_data(raw, field=name)
            frozen = freeze_json(raw)
            if not isinstance(frozen, Mapping):
                raise ReplayBindingError(f"{name} must be a mapping")
            object.__setattr__(self, name, frozen)
        for name in ("candidates", "scenarios"):
            raw = getattr(self, name)
            _require_plain_artifact_data(raw, field=name)
            frozen = freeze_json(raw)
            if not isinstance(frozen, tuple) or any(
                not isinstance(item, Mapping) for item in frozen
            ):
                raise ReplayBindingError(f"{name} must be a sequence of mappings")
            object.__setattr__(self, name, frozen)
        object.__setattr__(self, "artifact_hash", _digest("artifact_hash", self.artifact_hash))
        self.verify()

    @classmethod
    def build(
        cls,
        *,
        inputs: Mapping[str, object],
        universe: Mapping[str, object],
        broker_evidence: Mapping[str, object],
        candidates: Sequence[Mapping[str, object]],
        volatility: Mapping[str, object],
        scenarios: Sequence[Mapping[str, object]],
        policy: Mapping[str, object],
        risk_authority: Mapping[str, object],
        cost: Mapping[str, object],
        gate: Mapping[str, object],
        funnel_trace: Mapping[str, object] | None = None,
        schema: str = HISTORICAL_ARTIFACT_SCHEMA_V1,
    ) -> "HistoricalPipelineArtifact":
        raw_values: tuple[tuple[str, object], ...] = (
            ("inputs", inputs),
            ("universe", universe),
            ("broker_evidence", broker_evidence),
            ("candidates", candidates),
            ("volatility", volatility),
            ("scenarios", scenarios),
            ("policy", policy),
            ("risk_authority", risk_authority),
            ("cost", cost),
            ("gate", gate),
            ("funnel_trace", {} if funnel_trace is None else funnel_trace),
        )
        for name, value in raw_values:
            _require_plain_artifact_data(value, field=name)
        if type(candidates) not in {list, tuple} or type(scenarios) not in {
            list,
            tuple,
        }:
            raise TypeError("candidates and scenarios must be exact list or tuple")
        values = {
            "inputs": inputs,
            "universe": universe,
            "broker_evidence": broker_evidence,
            "candidates": tuple(candidates),
            "volatility": volatility,
            "scenarios": tuple(scenarios),
            "policy": policy,
            "risk_authority": risk_authority,
            "cost": cost,
            "gate": gate,
            "funnel_trace": {} if funnel_trace is None else funnel_trace,
        }
        identity = {
            "schema": schema,
            **values,
        }
        return cls(**values, schema=schema, artifact_hash=canonical_hash(identity))

    def identity_document(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "inputs": thaw_json(self.inputs),
            "universe": thaw_json(self.universe),
            "broker_evidence": thaw_json(self.broker_evidence),
            "candidates": thaw_json(self.candidates),
            "volatility": thaw_json(self.volatility),
            "scenarios": thaw_json(self.scenarios),
            "policy": thaw_json(self.policy),
            "risk_authority": thaw_json(self.risk_authority),
            "cost": thaw_json(self.cost),
            "gate": thaw_json(self.gate),
            "funnel_trace": thaw_json(self.funnel_trace),
        }

    def verify(self) -> "HistoricalPipelineArtifact":
        if self.schema not in {
            HISTORICAL_ARTIFACT_SCHEMA_V1,
            HISTORICAL_ARTIFACT_SCHEMA,
        }:
            raise ReplayBindingError("HISTORICAL_ARTIFACT_SCHEMA_MISMATCH")
        if len(self.scenarios) != len(self.candidates):
            raise ReplayBindingError("HISTORICAL_SCENARIO_COUNT_MISMATCH")
        for item in self.candidates:
            if set(item) != {"candidate_body", "proposal_body"}:
                raise ReplayBindingError("HISTORICAL_CANDIDATE_SCHEMA_INVALID")
            body = item.get("candidate_body")
            proposal = item.get("proposal_body")
            if not isinstance(body, Mapping) or not isinstance(proposal, Mapping):
                raise ReplayBindingError("HISTORICAL_CANDIDATE_SCHEMA_INVALID")
        self._verify_funnel_trace()
        if self.schema == HISTORICAL_ARTIFACT_SCHEMA:
            self._verify_joint_ranking()
        if canonical_hash(self.identity_document()) != self.artifact_hash:
            raise ReplayBindingError("HISTORICAL_ARTIFACT_HASH_MISMATCH")
        return self

    @property
    def requires_joint_ranking(self) -> bool:
        return self.schema == HISTORICAL_ARTIFACT_SCHEMA

    def _verify_joint_ranking(self) -> None:
        joint = self.funnel_trace.get("joint_ranking")
        joint_input = self.funnel_trace.get("joint_ranking_input")
        if not isinstance(joint, Mapping):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_REQUIRED")
        if not isinstance(joint_input, Mapping):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INPUT_REQUIRED")
        try:
            JointRankingSnapshot.from_dict(joint)
        except (TypeError, ValueError) as exc:
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INVALID") from exc
        snapshot_hash = joint.get("snapshot_hash")
        body = {key: value for key, value in joint.items() if key != "snapshot_hash"}
        if (
            joint.get("schema") != "options_copilot.joint_ranking.v1"
            or not isinstance(snapshot_hash, str)
            or canonical_hash(body) != snapshot_hash
        ):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INVALID")
        input_hash = joint_input.get("input_hash")
        input_body = {
            key: value
            for key, value in joint_input.items()
            if key not in {"schema", "input_hash"}
        }
        if (
            joint_input.get("schema")
            != "options_copilot.joint_ranking_input.v1"
            or not isinstance(input_hash, str)
            or canonical_hash(input_body) != input_hash
            or joint.get("input_hash") != input_hash
            or joint_input.get("scan_run_id") != joint.get("scan_run_id")
            or joint_input.get("generated_at") != joint.get("generated_at")
            or joint_input.get("broker_snapshot_hash")
            != joint.get("broker_snapshot_hash")
            or joint_input.get("strategy_nav_hash")
            != joint.get("strategy_nav_hash")
        ):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INPUT_INVALID")
        executable = joint.get("executable")
        research = joint.get("research_watchlist")
        if not isinstance(executable, tuple) or not isinstance(research, tuple):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INVALID")
        rows = executable + research
        finalized = self.funnel_trace.get("finalized_option_candidates")
        if not isinstance(finalized, tuple) or not finalized:
            raise ReplayBindingError("HISTORICAL_FINALIZED_OPTION_POOL_REQUIRED")
        candidate_hashes: dict[str, str] = {}
        for item in finalized:
            if not isinstance(item, Mapping) or not isinstance(item.get("payload"), Mapping):
                raise ReplayBindingError("HISTORICAL_FINALIZED_OPTION_POOL_INVALID")
            candidate_id = str(item.get("candidate_id", ""))
            candidate_hash = item.get("candidate_hash")
            if (
                not candidate_id
                or not isinstance(candidate_hash, str)
                or canonical_hash(item["payload"]) != candidate_hash
                or item["payload"].get("candidate_id") != candidate_id
                or candidate_id in candidate_hashes
            ):
                raise ReplayBindingError("HISTORICAL_FINALIZED_OPTION_POOL_INVALID")
            candidate_hashes[candidate_id] = candidate_hash
        if (
            joint.get("broker_snapshot_hash")
            != self.broker_evidence.get(
                "broker_snapshot_hash",
                self.broker_evidence.get("snapshot_hash"),
            )
            or set(candidate_hashes) != {
                str(row.get("candidate_id"))
                for row in rows
                if isinstance(row, Mapping)
            }
        ):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_BINDING_MISMATCH")
        if tuple(sorted(candidate_hashes.values())) != tuple(
            joint_input.get("candidate_hashes", ())
        ):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INPUT_INVALID")
        theses = self.funnel_trace.get("equity_theses")
        thesis_rows = theses.get("rows") if isinstance(theses, Mapping) else None
        gate_reasons = joint_input.get("gate_reasons_by_candidate")
        if (
            not isinstance(thesis_rows, tuple)
            or tuple(joint_input.get("equity_theses", ())) != thesis_rows
            or not isinstance(gate_reasons, Mapping)
            or set(gate_reasons) != set(candidate_hashes)
            or joint_input.get("limit") != 10
        ):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INPUT_INVALID")
        for row in rows:
            if not isinstance(row, Mapping):
                raise ReplayBindingError("HISTORICAL_JOINT_RANKING_ROW_INVALID")
            row_hash = row.get("row_hash")
            row_body = {key: value for key, value in row.items() if key != "row_hash"}
            if not isinstance(row_hash, str) or canonical_hash(row_body) != row_hash:
                raise ReplayBindingError("HISTORICAL_JOINT_RANKING_ROW_INVALID")
            if candidate_hashes.get(str(row.get("candidate_id"))) != row.get(
                "candidate_hash"
            ):
                raise ReplayBindingError("HISTORICAL_JOINT_RANKING_BINDING_MISMATCH")
        account = self.broker_evidence.get("joint_account_context")
        if not isinstance(account, Mapping):
            raise ReplayBindingError("HISTORICAL_JOINT_ACCOUNT_CONTEXT_REQUIRED")
        account_hash = account.get("context_hash")
        account_body = {key: value for key, value in account.items() if key != "context_hash"}
        if (
            account.get("broker_snapshot_hash")
            != joint.get("broker_snapshot_hash")
            or not isinstance(account_hash, str)
            or canonical_hash(account_body) != account_hash
        ):
            raise ReplayBindingError("HISTORICAL_JOINT_ACCOUNT_CONTEXT_INVALID")
        if (
            tuple(joint_input.get("open_position_underlyings", ()))
            != tuple(account.get("open_position_underlyings", ()))
            or joint_input.get("aggregate_open_risk_usd")
            != account.get("aggregate_open_risk_usd")
            or joint_input.get("concentration_by_underlying")
            != account.get("concentration_by_underlying")
        ):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INPUT_INVALID")

    def _verify_funnel_trace(self) -> None:
        raw_final = self.funnel_trace
        raw_inputs = self.inputs.get("funnel_trace")
        raw_universe = self.universe.get("funnel_trace")
        if not raw_final:
            if raw_inputs is not None or raw_universe is not None:
                raise ReplayBindingError("HISTORICAL_FUNNEL_TRACE_MISSING")
            return
        try:
            source_scan_run_id = _text(
                "funnel_trace.scan_run_id",
                raw_final.get("scan_run_id"),
            )
            final_trace = normalize_funnel_trace(
                raw_final,
                scan_run_id=source_scan_run_id,
            )
            pre_rank = thaw_json(final_trace)
            if not isinstance(pre_rank, dict):
                raise TypeError("funnel trace must thaw to a mapping")
            pre_rank.pop("finalized_option_candidates", None)
            pre_rank.pop("joint_ranking_input", None)
            pre_rank.pop("joint_ranking", None)
            pre_rank["ranked_count"] = 0
            expected_pre_rank = normalize_funnel_trace(
                pre_rank,
                scan_run_id=source_scan_run_id,
            )
            input_trace = normalize_funnel_trace(
                raw_inputs,
                scan_run_id=source_scan_run_id,
            )
            universe_trace = normalize_funnel_trace(
                raw_universe,
                scan_run_id=source_scan_run_id,
            )
        except (TypeError, ValueError) as exc:
            raise ReplayBindingError("HISTORICAL_FUNNEL_TRACE_INVALID") from exc
        if final_trace.get("ranked_count") != len(self.candidates):
            raise ReplayBindingError("HISTORICAL_FUNNEL_RANKED_COUNT_MISMATCH")
        if input_trace != expected_pre_rank or universe_trace != expected_pre_rank:
            raise ReplayBindingError("HISTORICAL_FUNNEL_TRACE_MISMATCH")


def _rebind_funnel_trace(
    value: object,
    *,
    scan_run_id: str,
) -> Mapping[str, object]:
    if value is None or value == {}:
        return {}
    if not isinstance(value, Mapping):
        raise ReplayBindingError("HISTORICAL_FUNNEL_TRACE_INVALID")
    try:
        source_scan_run_id = _text(
            "funnel_trace.scan_run_id",
            value.get("scan_run_id"),
        )
        source = normalize_funnel_trace(
            value,
            scan_run_id=source_scan_run_id,
        )
        rebound = thaw_json(source)
        if not isinstance(rebound, dict):
            raise TypeError("funnel trace must thaw to a mapping")
        rebound["scan_run_id"] = scan_run_id
        return normalize_funnel_trace(rebound, scan_run_id=scan_run_id)
    except (TypeError, ValueError) as exc:
        raise ReplayBindingError("HISTORICAL_FUNNEL_TRACE_INVALID") from exc


def _rebind_payload_funnel_trace(
    payload: Mapping[str, object],
    *,
    scan_run_id: str,
) -> dict[str, object]:
    rebound = dict(payload)
    if "funnel_trace" in rebound:
        rebound["funnel_trace"] = _rebind_funnel_trace(
            rebound["funnel_trace"],
            scan_run_id=scan_run_id,
        )
    nested = rebound.get("universe")
    if isinstance(nested, Mapping) and "funnel_trace" in nested:
        rebound["universe"] = _rebind_payload_funnel_trace(
            nested,
            scan_run_id=scan_run_id,
        )
    return rebound


def _expected_rebound_funnel_trace(
    value: Mapping[str, object],
    *,
    scan_run_id: str,
) -> Mapping[str, object]:
    if not value:
        return {}
    source_scan_run_id = _text("funnel_trace.scan_run_id", value.get("scan_run_id"))
    rebound = thaw_json(_rebind_funnel_trace(value, scan_run_id=scan_run_id))
    if not isinstance(rebound, dict):
        raise ReplayBindingError("HISTORICAL_FUNNEL_TRACE_INVALID")
    if source_scan_run_id == scan_run_id:
        return normalize_funnel_trace(rebound, scan_run_id=scan_run_id)
    source_joint = rebound.get("joint_ranking")
    source_joint_input = rebound.get("joint_ranking_input")
    if isinstance(source_joint, Mapping):
        if not isinstance(source_joint_input, Mapping):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INPUT_REQUIRED")
        rebound_input = thaw_json(source_joint_input)
        rebound_joint = thaw_json(source_joint)
        if not isinstance(rebound_input, dict) or not isinstance(rebound_joint, dict):
            raise ReplayBindingError("HISTORICAL_JOINT_RANKING_INVALID")
        rebound_input["scan_run_id"] = scan_run_id
        rebound_input.pop("input_hash", None)
        input_body = {
            key: item for key, item in rebound_input.items() if key != "schema"
        }
        rebound_input["input_hash"] = canonical_hash(input_body)
        rebound_joint["scan_run_id"] = scan_run_id
        rebound_joint["input_hash"] = rebound_input["input_hash"]
        rebound_joint.pop("snapshot_hash", None)
        rebound_joint["snapshot_hash"] = canonical_hash(rebound_joint)
        rebound["joint_ranking_input"] = rebound_input
        rebound["joint_ranking"] = rebound_joint
    return normalize_funnel_trace(rebound, scan_run_id=scan_run_id)


class _ExplodingMutationPort:
    __slots__ = ("name", "calls")

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

    def __getattr__(self, operation: str):
        def explode(*_: object, **__: object) -> object:
            self.calls += 1
            raise ReplaySafetyError(
                f"FORBIDDEN_REPLAY_PORT_CALLED:{self.name}.{operation}"
            )

        return explode


class _FrozenMappingPort:
    __slots__ = ("_payload", "calls")

    def __init__(self, payload: Mapping[str, object]) -> None:
        frozen = freeze_json(payload)
        if not isinstance(frozen, Mapping):
            raise ReplayBindingError("REPLAY_DEPENDENCY_PAYLOAD_INVALID")
        self._payload = frozen
        self.calls = 0

    def _value(self) -> Mapping[str, object]:
        self.calls += 1
        value = thaw_json(self._payload)
        if not isinstance(value, Mapping):
            raise ReplaySafetyError("REPLAY_DEPENDENCY_PAYLOAD_CORRUPT")
        return value


class _InputsPort(_FrozenMappingPort):
    def run(self, **kwargs: object) -> Mapping[str, object]:
        value = self._value()
        trace = value.get("funnel_trace")
        if trace is None:
            return value
        scan_run_id = _text("scan_run_id", kwargs.get("scan_run_id"))
        return _rebind_payload_funnel_trace(value, scan_run_id=scan_run_id)


class _BrokerEvidencePort(_FrozenMappingPort):
    def acquire(self, **_: object) -> Mapping[str, object]:
        return self._value()


class _VolatilityPort(_FrozenMappingPort):
    def evaluate(self, **_: object) -> Mapping[str, object]:
        return self._value()


class _CostPort(_FrozenMappingPort):
    def resolve(self, **_: object) -> Mapping[str, object]:
        return self._value()

    def is_current(self, value: object) -> bool:
        expected = thaw_json(self._payload)
        return value == expected


class _GatePort(_FrozenMappingPort):
    def evaluate(self, **_: object) -> Mapping[str, object]:
        return self._value()


class _ResolverPort(_FrozenMappingPort):
    def resolve(self, **_: object) -> Mapping[str, object]:
        return self._value()

    def is_current(self, value: object) -> bool:
        return value == thaw_json(self._payload)


@dataclass(frozen=True, slots=True)
class _FrozenCandidate:
    candidate_id: str
    candidate_hash: str
    candidate_body: Mapping[str, object]
    proposal_body: Mapping[str, object]
    proposal_hash: str

    @classmethod
    def from_artifact(cls, value: Mapping[str, object]) -> "_FrozenCandidate":
        body = value.get("candidate_body")
        proposal = value.get("proposal_body")
        if not isinstance(body, Mapping) or not isinstance(proposal, Mapping):
            raise ReplayBindingError("HISTORICAL_CANDIDATE_SCHEMA_INVALID")
        candidate_id = _text("candidate_id", body.get("candidate_id"))
        candidate_hash = canonical_hash(thaw_json(body))
        return cls(
            candidate_id,
            candidate_hash,
            body,
            proposal,
            canonical_hash(thaw_json(proposal)),
        )

    def hash_payload(self) -> Mapping[str, object]:
        value = thaw_json(self.candidate_body)
        if not isinstance(value, Mapping):
            raise ReplaySafetyError("HISTORICAL_CANDIDATE_CORRUPT")
        return value

    def proposal_payload(self) -> Mapping[str, object]:
        value = thaw_json(self.proposal_body)
        if not isinstance(value, Mapping):
            raise ReplaySafetyError("HISTORICAL_PROPOSAL_CORRUPT")
        return value


class _UniversePort:
    __slots__ = ("_payload", "_candidates", "calls")

    def __init__(
        self,
        payload: Mapping[str, object],
        candidates: tuple[_FrozenCandidate, ...],
    ) -> None:
        self._payload = freeze_json(payload)
        self._candidates = candidates
        self.calls = 0

    def run(self, **kwargs: object) -> Mapping[str, object]:
        self.calls += 1
        payload = thaw_json(self._payload)
        if not isinstance(payload, Mapping):
            raise ReplaySafetyError("HISTORICAL_UNIVERSE_CORRUPT")
        if payload.get("funnel_trace") is not None:
            payload = _rebind_payload_funnel_trace(
                payload,
                scan_run_id=_text("scan_run_id", kwargs.get("scan_run_id")),
            )
        return {**payload, "finalists": self._candidates}


class _RegistryPort:
    __slots__ = ("_candidates", "calls")

    def __init__(self, candidates: tuple[_FrozenCandidate, ...]) -> None:
        self._candidates = candidates
        self.calls = 0

    def run(self, **_: object) -> Mapping[str, object]:
        self.calls += 1
        return {"finalists": self._candidates}


class _GeneratorPort:
    __slots__ = ("_candidates", "calls")

    def __init__(self, candidates: tuple[_FrozenCandidate, ...]) -> None:
        self._candidates = candidates
        self.calls = 0

    def generate(self, **_: object) -> Mapping[str, object]:
        self.calls += 1
        return {"candidates": self._candidates}


class _ScenarioPort:
    __slots__ = ("_responses", "calls")

    def __init__(self, responses: tuple[Mapping[str, object], ...]) -> None:
        self._responses = responses
        self.calls = 0

    def evaluate_pre_cost(self, **_: object) -> Mapping[str, object]:
        if self.calls >= len(self._responses):
            raise ReplaySafetyError("HISTORICAL_SCENARIO_RESPONSE_EXHAUSTED")
        response = thaw_json(self._responses[self.calls])
        self.calls += 1
        if not isinstance(response, Mapping):
            raise ReplaySafetyError("HISTORICAL_SCENARIO_CORRUPT")
        return response


class _ReplayJointAccountResolver(_FrozenMappingPort):
    def resolve(self, **_: object) -> Mapping[str, object]:
        return self._value()


class ReplayOnlyRankingStore:
    """Run-local DecisionPipeline sink with no live authorization API."""

    __slots__ = ("_snapshots", "_decisions")

    def __init__(self) -> None:
        self._snapshots: dict[str, tuple[str, dict[str, object]]] = {}
        self._decisions: dict[str, tuple[Mapping[str, object], ...]] = {}

    def append_decisions(
        self,
        *,
        scan_run_id: str,
        records: Sequence[Mapping[str, object] | object],
        now: datetime | None = None,
    ) -> tuple[Mapping[str, object], ...]:
        del now
        scan_id = _text("scan_run_id", scan_run_id)
        frozen = freeze_json(tuple(_document(item) for item in records))
        if not isinstance(frozen, tuple) or not frozen:
            raise ValueError("replay decision batch cannot be empty")
        prior = self._decisions.get(scan_id)
        if prior is not None and prior != frozen:
            raise RankingStoreConflict("replay decision batch conflict")
        self._decisions[scan_id] = frozen
        return frozen

    def append_snapshot(
        self,
        *,
        scan_run_id: str,
        input_hash: str,
        evidence_hash: str,
        broker_snapshot_hash: str,
        policy_version: str,
        policy_hash: str,
        policy_authority_marker_hash: str,
        cost_version: str,
        cost_hash: str,
        risk_contract_hash: str,
        risk_authority_version: str,
        risk_authority_marker_hash: str,
        candidates: Sequence[Mapping[str, object] | object],
        governance_evidence: Sequence[Mapping[str, object] | object],
        decision_records: Sequence[Mapping[str, object] | object],
        valid_until: datetime,
        now: datetime,
        **_: object,
    ) -> Mapping[str, object]:
        scan_id = _text("scan_run_id", scan_run_id)
        payload = {
            "schema": "options_copilot.replay.ranking_snapshot.v2",
            "authority": REPLAY_AUTHORITY,
            "scan_run_id": scan_id,
            "input_hash": _digest("input_hash", input_hash),
            "evidence_hash": _digest("evidence_hash", evidence_hash),
            "broker_snapshot_hash": _digest(
                "broker_snapshot_hash", broker_snapshot_hash
            ),
            "current_policy_version": _text("policy_version", policy_version),
            "current_policy_hash": _digest("policy_hash", policy_hash),
            "policy_authority_marker_hash": _digest(
                "policy_authority_marker_hash", policy_authority_marker_hash
            ),
            "cost_version": _text("cost_version", cost_version),
            "cost_hash": _digest("cost_hash", cost_hash),
            "risk_contract_hash": _digest("risk_contract_hash", risk_contract_hash),
            "risk_authority_version": _text(
                "risk_authority_version", risk_authority_version
            ),
            "risk_authority_marker_hash": _digest(
                "risk_authority_marker_hash", risk_authority_marker_hash
            ),
            "candidates": tuple(_document(item) for item in candidates),
            "governance_evidence": tuple(
                _document(item) for item in governance_evidence
            ),
            "decision_records": tuple(_document(item) for item in decision_records),
            "created_at": datetime_text(utc_datetime(now, field="now")),
            "valid_until": datetime_text(
                utc_datetime(valid_until, field="valid_until")
            ),
        }
        snapshot_hash = canonical_hash(payload)
        result = {
            "ranking_snapshot_id": f"replay-store-{snapshot_hash[:32]}",
            "snapshot_hash": snapshot_hash,
            "authority": REPLAY_AUTHORITY,
        }
        prior = self._snapshots.get(scan_id)
        current = (snapshot_hash, result)
        if prior is not None and prior != current:
            raise RankingStoreConflict("replay snapshot conflict")
        self._snapshots[scan_id] = current
        return result

    append = append_snapshot


@dataclass(slots=True)
class _ReplayDependencyGraph:
    inputs: _InputsPort
    universe: _UniversePort
    broker_evidence: _BrokerEvidencePort
    registry: _RegistryPort
    generator: _GeneratorPort
    volatility: _VolatilityPort
    scenarios: _ScenarioPort
    policy: _ResolverPort
    risk: _ResolverPort
    cost: _CostPort
    gate: _GatePort
    ranker: PortfolioRanker
    ranking_store: ReplayOnlyRankingStore
    forbidden_ports: tuple[_ExplodingMutationPort, ...]
    joint_ranking_required: bool
    joint_account_resolver: _ReplayJointAccountResolver | None

    @classmethod
    def from_artifact(cls, artifact: HistoricalPipelineArtifact) -> "_ReplayDependencyGraph":
        artifact.verify()
        candidates = tuple(
            _FrozenCandidate.from_artifact(item) for item in artifact.candidates
        )
        graph = cls(
            inputs=_InputsPort(artifact.inputs),
            universe=_UniversePort(artifact.universe, candidates),
            broker_evidence=_BrokerEvidencePort(artifact.broker_evidence),
            registry=_RegistryPort(candidates),
            generator=_GeneratorPort(candidates),
            volatility=_VolatilityPort(artifact.volatility),
            scenarios=_ScenarioPort(artifact.scenarios),
            policy=_ResolverPort(artifact.policy),
            risk=_ResolverPort(artifact.risk_authority),
            cost=_CostPort(artifact.cost),
            gate=_GatePort(artifact.gate),
            ranker=PortfolioRanker(),
            ranking_store=ReplayOnlyRankingStore(),
            forbidden_ports=tuple(
                _ExplodingMutationPort(name) for name in _FORBIDDEN_PORT_NAMES
            ),
            joint_ranking_required=(
                artifact.requires_joint_ranking
            ),
            joint_account_resolver=(
                _ReplayJointAccountResolver(
                    artifact.broker_evidence["joint_account_context"]
                )
                if artifact.requires_joint_ranking
                else None
            ),
        )
        graph.verify_closed()
        return graph

    def verify_closed(self) -> None:
        expected = (
            (self.inputs, _InputsPort),
            (self.universe, _UniversePort),
            (self.broker_evidence, _BrokerEvidencePort),
            (self.registry, _RegistryPort),
            (self.generator, _GeneratorPort),
            (self.volatility, _VolatilityPort),
            (self.scenarios, _ScenarioPort),
            (self.policy, _ResolverPort),
            (self.risk, _ResolverPort),
            (self.cost, _CostPort),
            (self.gate, _GatePort),
            (self.ranker, PortfolioRanker),
            (self.ranking_store, ReplayOnlyRankingStore),
        )
        if any(type(value) is not expected_type for value, expected_type in expected):
            raise ReplaySafetyError("REPLAY_DEPENDENCY_GRAPH_NOT_CLOSED")
        if type(self.joint_ranking_required) is not bool:
            raise ReplaySafetyError("REPLAY_JOINT_MODE_INVALID")
        if self.joint_ranking_required != isinstance(
            self.joint_account_resolver, _ReplayJointAccountResolver
        ):
            raise ReplaySafetyError("REPLAY_JOINT_ACCOUNT_RESOLVER_INVALID")
        if (
            len(self.forbidden_ports) != len(_FORBIDDEN_PORT_NAMES)
            or any(type(port) is not _ExplodingMutationPort for port in self.forbidden_ports)
            or tuple(port.name for port in self.forbidden_ports)
            != _FORBIDDEN_PORT_NAMES
        ):
            raise ReplaySafetyError("REPLAY_MUTATION_PORT_GRAPH_INVALID")

    def assert_no_mutation_calls(self) -> None:
        if any(port.calls for port in self.forbidden_ports):
            raise ReplaySafetyError("FORBIDDEN_REPLAY_PORT_WAS_CALLED")

    def decision_pipeline(self, clock: FrozenHistoricalClock) -> DecisionPipeline:
        self.verify_closed()
        if type(clock) is not FrozenHistoricalClock:
            raise ReplaySafetyError("FROZEN_HISTORICAL_CLOCK_REQUIRED")
        return DecisionPipeline(
            inputs=self.inputs,
            universe_funnel=self.universe,
            broker_evidence=self.broker_evidence,
            strategy_registry=self.registry,
            strategy_generator=self.generator,
            volatility_engine=self.volatility,
            scenario_engine=self.scenarios,
            policy_resolver=self.policy,
            risk_authority_resolver=self.risk,
            cost_contract=self.cost,
            eligibility_gate=self.gate,
            portfolio_ranker=self.ranker,
            ranking_store=self.ranking_store,
            joint_ranking_required=self.joint_ranking_required,
            joint_account_context_resolver=self.joint_account_resolver,
            clock=clock,
        )


@dataclass(frozen=True, slots=True)
class HistoricalPipelineRun:
    scan_run_id: str
    slot_at: datetime
    result: Mapping[str, object]
    forbidden_call_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "slot_at", utc_datetime(self.slot_at, field="slot_at"))
        for name in ("result", "forbidden_call_counts"):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, Mapping):
                raise ReplayBindingError(f"{name} must be a mapping")
            object.__setattr__(self, name, frozen)


class _HistoricalPipelineBase:
    __slots__ = (
        "_artifact",
        "_immutable_result",
        "_decision_at",
        "_pipeline_version",
        "_pipeline_hash",
        "_independence_store",
        "_independence_hash",
        "_verification_as_of",
    )

    def __setattr__(self, name: str, value: object) -> None:
        if name not in _HistoricalPipelineBase.__slots__ or hasattr(self, name):
            raise ReplaySafetyError("HISTORICAL_PIPELINE_IMMUTABLE")
        object.__setattr__(self, name, value)

    def __init__(
        self,
        *,
        token: object,
        artifact: HistoricalPipelineArtifact | None,
        immutable_result: Mapping[str, object] | None,
        decision_at: datetime,
        pipeline_version: str,
        pipeline_hash: str,
        independence_store: IndependenceSpecArtifactStore,
        independence_hash: str,
        verification_as_of: datetime,
    ) -> None:
        if token is not _FACTORY_TOKEN:
            raise ReplaySafetyError("HISTORICAL_PIPELINE_FACTORY_REQUIRED")
        if (artifact is None) is (immutable_result is None):
            raise ReplaySafetyError("HISTORICAL_PIPELINE_SOURCE_INVALID")
        if artifact is not None and type(artifact) is not HistoricalPipelineArtifact:
            raise ReplaySafetyError("HISTORICAL_ARTIFACT_REQUIRED")
        if type(independence_store) is not IndependenceSpecArtifactStore:
            raise ReplaySafetyError("INDEPENDENCE_ARTIFACT_STORE_REQUIRED")
        frozen_result = None if immutable_result is None else freeze_json(immutable_result)
        if frozen_result is not None and not isinstance(frozen_result, Mapping):
            raise ReplayBindingError("IMMUTABLE_TEST_RESULT_INVALID")
        self._artifact = artifact
        self._immutable_result = frozen_result
        self._decision_at = utc_datetime(decision_at, field="decision_at")
        self._pipeline_version = _text("pipeline_version", pipeline_version)
        self._pipeline_hash = _digest("pipeline_hash", pipeline_hash)
        self._independence_store = independence_store
        self._independence_hash = _digest("independence_hash", independence_hash)
        self._verification_as_of = utc_datetime(
            verification_as_of, field="verification_as_of"
        )

    @property
    def decision_at(self) -> datetime:
        return self._decision_at

    @property
    def pipeline_version(self) -> str:
        return self._pipeline_version

    @property
    def pipeline_hash(self) -> str:
        return self._pipeline_hash

    @property
    def independence_hash(self) -> str:
        return self._independence_hash

    @property
    def verification_as_of(self) -> datetime:
        return self._verification_as_of

    def _resolve_independence(self, *, allow_test_fixture: bool):
        try:
            return self._independence_store.resolve(
                self._independence_hash,
                allow_test_fixture=allow_test_fixture,
                as_of=self._verification_as_of,
            )
        except IndependenceSpecArtifactError as exc:
            raise ReplaySafetyError(str(exc)) from exc

    def _run_slot(self, scan_run_id: str, slot_at: datetime) -> HistoricalPipelineRun:
        scan_id = _text("scan_run_id", scan_run_id)
        cutoff = utc_datetime(slot_at, field="slot_at")
        if cutoff != self._decision_at:
            raise ReplayBindingError("HISTORICAL_SLOT_MISMATCH")
        if self._immutable_result is not None:
            raw = thaw_json(self._immutable_result)
            counts = {name: 0 for name in _FORBIDDEN_PORT_NAMES}
        else:
            artifact = self._artifact
            if type(artifact) is not HistoricalPipelineArtifact:
                raise ReplaySafetyError("HISTORICAL_ARTIFACT_REQUIRED")
            artifact.verify()
            if artifact.artifact_hash != self._pipeline_hash:
                raise ReplayBindingError("PIPELINE_ARTIFACT_HASH_MISMATCH")
            graph = _ReplayDependencyGraph.from_artifact(artifact)
            graph.assert_no_mutation_calls()
            pipeline = graph.decision_pipeline(FrozenHistoricalClock(cutoff))
            if type(pipeline) is not DecisionPipeline:
                raise ReplaySafetyError("INTERNAL_DECISION_PIPELINE_INVALID")
            raw = pipeline.run_slot(scan_id, cutoff)
            graph.verify_closed()
            graph.assert_no_mutation_calls()
            counts = {port.name: port.calls for port in graph.forbidden_ports}
        if not isinstance(raw, Mapping):
            raise ReplayBindingError("PIPELINE_RESULT_INVALID")
        _verify_pipeline_result_schema(raw)
        _verify_pipeline_result_hash(raw)
        if self._artifact is not None:
            actual_trace = _pipeline_result_funnel_trace(raw)
            expected_trace = _expected_rebound_funnel_trace(
                self._artifact.funnel_trace,
                scan_run_id=scan_id,
            )
            if actual_trace != expected_trace:
                raise ReplayBindingError("PIPELINE_FUNNEL_TRACE_MISMATCH")
        return HistoricalPipelineRun(scan_id, cutoff, raw, counts)


class HistoricalDecisionPipeline(_HistoricalPipelineBase):
    """Production replay type; test independence artifacts are rejected."""

    __slots__ = ()

    @classmethod
    def from_point_in_time_artifact(
        cls,
        artifact: HistoricalPipelineArtifact,
        *,
        decision_at: datetime,
        pipeline_version: str,
        independence_store: IndependenceSpecArtifactStore,
        independence_hash: str,
        verification_as_of: datetime,
    ) -> "HistoricalDecisionPipeline":
        if type(artifact) is not HistoricalPipelineArtifact:
            raise ReplaySafetyError("HISTORICAL_ARTIFACT_REQUIRED")
        _verify_artifact_pipeline_version(artifact, pipeline_version)
        result = cls(
            token=_FACTORY_TOKEN,
            artifact=artifact,
            immutable_result=None,
            decision_at=decision_at,
            pipeline_version=pipeline_version,
            pipeline_hash=artifact.artifact_hash,
            independence_store=independence_store,
            independence_hash=independence_hash,
            verification_as_of=verification_as_of,
        )
        spec = result._resolve_independence(allow_test_fixture=False)
        if spec.test_only:
            raise ReplaySafetyError("PRODUCTION_PIPELINE_REQUIRES_PRODUCTION_SPEC")
        return result

    def resolve_independence(self):
        spec = self._resolve_independence(allow_test_fixture=False)
        if spec.test_only:
            raise ReplaySafetyError("PRODUCTION_PIPELINE_REQUIRES_PRODUCTION_SPEC")
        return spec

    def run_slot(self, scan_run_id: str, slot_at: datetime) -> HistoricalPipelineRun:
        self.resolve_independence()
        return self._run_slot(scan_run_id, slot_at)


class TestHistoricalDecisionPipeline(_HistoricalPipelineBase):
    """Structurally distinct replay type for explicitly allowed TEST_ONLY specs."""

    __test__ = False
    __slots__ = ()

    @classmethod
    def from_point_in_time_artifact(
        cls,
        artifact: HistoricalPipelineArtifact,
        *,
        decision_at: datetime,
        pipeline_version: str,
        independence_store: IndependenceSpecArtifactStore,
        independence_hash: str,
        verification_as_of: datetime,
    ) -> "TestHistoricalDecisionPipeline":
        if type(artifact) is not HistoricalPipelineArtifact:
            raise ReplaySafetyError("HISTORICAL_ARTIFACT_REQUIRED")
        _verify_artifact_pipeline_version(artifact, pipeline_version)
        result = cls(
            token=_FACTORY_TOKEN,
            artifact=artifact,
            immutable_result=None,
            decision_at=decision_at,
            pipeline_version=pipeline_version,
            pipeline_hash=artifact.artifact_hash,
            independence_store=independence_store,
            independence_hash=independence_hash,
            verification_as_of=verification_as_of,
        )
        spec = result._resolve_independence(allow_test_fixture=True)
        if not spec.test_only:
            raise ReplaySafetyError("TEST_PIPELINE_REQUIRES_TEST_SPEC")
        return result

    @classmethod
    def from_immutable_test_result(
        cls,
        result: Mapping[str, object],
        *,
        decision_at: datetime,
        pipeline_version: str,
        pipeline_hash: str,
        independence_store: IndependenceSpecArtifactStore,
        independence_hash: str,
        verification_as_of: datetime,
    ) -> "TestHistoricalDecisionPipeline":
        _verify_pipeline_result_schema(result)
        _verify_pipeline_result_hash(result)
        pipeline = cls(
            token=_FACTORY_TOKEN,
            artifact=None,
            immutable_result=result,
            decision_at=decision_at,
            pipeline_version=pipeline_version,
            pipeline_hash=pipeline_hash,
            independence_store=independence_store,
            independence_hash=independence_hash,
            verification_as_of=verification_as_of,
        )
        spec = pipeline._resolve_independence(allow_test_fixture=True)
        if not spec.test_only:
            raise ReplaySafetyError("TEST_PIPELINE_REQUIRES_TEST_SPEC")
        return pipeline

    def resolve_independence(self):
        spec = self._resolve_independence(allow_test_fixture=True)
        if not spec.test_only:
            raise ReplaySafetyError("TEST_PIPELINE_REQUIRES_TEST_SPEC")
        return spec

    def run_slot(self, scan_run_id: str, slot_at: datetime) -> HistoricalPipelineRun:
        self.resolve_independence()
        return self._run_slot(scan_run_id, slot_at)


@dataclass(frozen=True, slots=True)
class ReplayResult:
    replay_scan_run_id: str
    source_scan_run_id: str
    source_ranking_snapshot_id: str | None
    source_ranking_snapshot_hash: str | None
    decision_at: datetime
    dataset_hash: str
    pipeline_version: str
    pipeline_hash: str
    pipeline_mode: str
    input_hash: str
    evidence_hash: str
    broker_snapshot_hash: str
    initial_policy_version: str
    initial_policy_hash: str
    execution_cost_version: str
    execution_cost_hash: str
    independence_version: str
    independence_hash: str
    independence_test_only: bool
    production_count_eligible: bool
    source_versions_hash: str
    window_hash: str
    status: str
    reasons: tuple[str, ...]
    ranking_snapshot_id: str | None
    ranking_snapshot_hash: str | None
    rankings: tuple[Mapping[str, object], ...]
    exclusions: tuple[Mapping[str, object], ...]
    funnel_trace: Mapping[str, object]
    gate_bundle_hash: str
    pipeline_result_hash: str
    pipeline_projection_hash: str
    replay_hash: str
    authority: str = REPLAY_AUTHORITY
    authorizable: bool = False
    approval_eligible: bool = False
    instruction_eligible: bool = False
    order_eligible: bool = False
    schema: str = REPLAY_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "decision_at", utc_datetime(self.decision_at, field="decision_at")
        )
        for name in ("rankings", "exclusions"):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, tuple):
                raise TypeError(f"{name} must be a sequence")
            object.__setattr__(self, name, frozen)
        funnel_trace = freeze_json(self.funnel_trace)
        if not isinstance(funnel_trace, Mapping):
            raise TypeError("funnel_trace must be a mapping")
        object.__setattr__(self, "funnel_trace", funnel_trace)

    def identity_document(self) -> dict[str, object]:
        return {
            field.name: (
                datetime_text(value)
                if isinstance(value, datetime)
                else thaw_json(value)
                if isinstance(value, (Mapping, tuple))
                else value
            )
            for field in fields(self)
            if field.name != "replay_hash"
            for value in (getattr(self, field.name),)
        }

    def verify(self) -> "ReplayResult":
        if self.schema != REPLAY_SCHEMA or self.authority != REPLAY_AUTHORITY:
            raise ReplaySafetyError("REPLAY_AUTHORITY_INVALID")
        if any(
            (
                self.authorizable,
                self.approval_eligible,
                self.instruction_eligible,
                self.order_eligible,
            )
        ):
            raise ReplaySafetyError("REPLAY_AUTHORITY_ESCALATION")
        if self.pipeline_mode not in {"PRODUCTION", "TEST_ONLY"}:
            raise ReplaySafetyError("REPLAY_PIPELINE_MODE_INVALID")
        expected_production = (
            self.pipeline_mode == "PRODUCTION" and not self.independence_test_only
        )
        if self.production_count_eligible is not expected_production:
            raise ReplaySafetyError("REPLAY_PRODUCTION_COUNT_ELIGIBILITY_INVALID")
        if self.status not in {"TRADE", "NO_TRADE"}:
            raise ReplayBindingError("REPLAY_STATUS_INVALID")
        if self.status == "TRADE" and (
            not self.rankings
            or self.ranking_snapshot_id is None
            or self.ranking_snapshot_hash is None
        ):
            raise ReplayBindingError("REPLAY_RANKING_INCOMPLETE")
        if self.status == "NO_TRADE" and (
            self.rankings
            or self.ranking_snapshot_id is not None
            or self.ranking_snapshot_hash is not None
        ):
            raise ReplayBindingError("NO_TRADE_HAS_RANKING")
        try:
            funnel_trace = normalize_funnel_trace(
                self.funnel_trace,
                scan_run_id=self.replay_scan_run_id,
            )
        except (TypeError, ValueError) as exc:
            raise ReplayBindingError("REPLAY_FUNNEL_TRACE_INVALID") from exc
        if funnel_trace and funnel_trace.get("ranked_count") != len(self.rankings):
            raise ReplayBindingError("REPLAY_FUNNEL_RANKED_COUNT_MISMATCH")
        for name in (
            "dataset_hash",
            "pipeline_hash",
            "input_hash",
            "evidence_hash",
            "broker_snapshot_hash",
            "initial_policy_hash",
            "execution_cost_hash",
            "independence_hash",
            "source_versions_hash",
            "window_hash",
            "pipeline_result_hash",
            "pipeline_projection_hash",
            "replay_hash",
            "gate_bundle_hash",
        ):
            _digest(name, getattr(self, name))
        for index, row in enumerate(self.rankings, start=1):
            if (
                row.get("rank") != index
                or row.get("authority") != REPLAY_AUTHORITY
                or row.get("authorizable") is not False
                or row.get("current_policy_version") != self.initial_policy_version
                or row.get("current_policy_hash") != self.initial_policy_hash
                or row.get("cost_version") != self.execution_cost_version
                or row.get("cost_hash") != self.execution_cost_hash
            ):
                raise ReplaySafetyError("REPLAY_RANKING_BINDING_INVALID")
            _digest("candidate_hash", row.get("candidate_hash"))
            _digest("ranking_basis_hash", row.get("ranking_basis_hash"))
        if canonical_hash(self.identity_document()) != self.replay_hash:
            raise ReplayBindingError("REPLAY_HASH_MISMATCH")
        return self

    def as_dict(self) -> dict[str, object]:
        return {**self.identity_document(), "replay_hash": self.replay_hash}


class PointInTimeReplay:
    """Execute only one of the two exact sealed historical pipeline types."""

    __slots__ = ("_pipeline",)

    def __setattr__(self, name: str, value: object) -> None:
        if hasattr(self, name):
            raise ReplaySafetyError("POINT_IN_TIME_REPLAY_IMMUTABLE")
        object.__setattr__(self, name, value)

    def __init__(
        self,
        historical_pipeline: HistoricalDecisionPipeline
        | TestHistoricalDecisionPipeline,
    ) -> None:
        if type(historical_pipeline) not in {
            HistoricalDecisionPipeline,
            TestHistoricalDecisionPipeline,
        }:
            raise ReplaySafetyError("HISTORICAL_PIPELINE_REQUIRED")
        self._pipeline = historical_pipeline

    def run(self, manifest: DatasetManifest) -> ReplayResult:
        if not isinstance(manifest, DatasetManifest):
            raise TypeError("manifest must be a DatasetManifest")
        pipeline = self._pipeline
        is_test = type(pipeline) is TestHistoricalDecisionPipeline
        spec = pipeline.resolve_independence()
        try:
            manifest.verify(
                independence_store=pipeline._independence_store,
                allow_test_fixture=is_test,
                verification_as_of=pipeline.verification_as_of,
            )
        except Exception as exc:
            raise ReplayBindingError("DATASET_VERIFICATION_FAILED") from exc
        if manifest.independence_hash != pipeline.independence_hash:
            raise ReplayBindingError("INDEPENDENCE_PIPELINE_BINDING_MISMATCH")
        if (
            pipeline.pipeline_version != manifest.pipeline_version
            or pipeline.pipeline_hash != manifest.pipeline_hash
        ):
            raise ReplayBindingError("PIPELINE_BINDING_MISMATCH")
        if pipeline.decision_at != manifest.decision_at:
            raise ReplayBindingError("HISTORICAL_SLOT_MISMATCH")
        scan_id = replay_scan_run_id(manifest)
        envelope = pipeline.run_slot(scan_id, manifest.decision_at)
        if envelope.scan_run_id != scan_id or envelope.slot_at != manifest.decision_at:
            raise ReplayBindingError("PIPELINE_EXECUTION_ENVELOPE_MISMATCH")
        if any(envelope.forbidden_call_counts.values()):
            raise ReplaySafetyError("FORBIDDEN_REPLAY_PORT_WAS_CALLED")
        result = _document(thaw_json(envelope.result))
        _verify_pipeline_result_schema(result)
        pipeline_result_hash = _verify_pipeline_result_hash(result)
        funnel_trace = _pipeline_result_funnel_trace(result)
        gate_bundle_hash = _required_hash(result, "gate_bundle_hash")
        if result.get("scan_run_id") != scan_id:
            raise ReplayBindingError("PIPELINE_SCAN_RUN_ID_MISMATCH")
        status = _required_text(result, "status").upper()
        if status not in {"TRADE", "NO_TRADE"}:
            raise ReplayBindingError("PIPELINE_RESULT_STATUS_INVALID")
        bound = _verify_complete_bindings(result, manifest)
        reasons = _reasons(result)
        candidate_hashes = _hash_sequence(
            "candidate_hashes", result.get("candidate_hashes")
        )
        basis_hashes = _hash_sequence(
            "ranking_basis_hashes", result.get("ranking_basis_hashes")
        )
        if len(candidate_hashes) != len(basis_hashes) or len(candidate_hashes) > 3:
            raise ReplayBindingError("PIPELINE_RANKING_BINDING_INVALID")
        raw_snapshot_id = result.get("ranking_snapshot_id")
        raw_snapshot_hash = result.get("ranking_snapshot_hash")
        if status == "TRADE":
            source_snapshot_id = _text("ranking_snapshot_id", raw_snapshot_id)
            source_snapshot_hash = _digest(
                "ranking_snapshot_hash", raw_snapshot_hash
            )
            if not candidate_hashes:
                raise ReplayBindingError("PIPELINE_RANKING_BINDING_INVALID")
        else:
            if (
                raw_snapshot_id is not None
                or raw_snapshot_hash is not None
                or candidate_hashes
                or basis_hashes
            ):
                raise ReplayBindingError("NO_TRADE_HAS_RANKING")
            source_snapshot_id = None
            source_snapshot_hash = None
        rankings = tuple(
            {
                "schema": "options_copilot.ranking_row.replay.v3",
                "rank": index,
                "candidate_hash": candidate_hash,
                "ranking_basis_hash": basis_hash,
                "current_policy_version": bound["policy_version"],
                "current_policy_hash": bound["policy_hash"],
                "cost_version": bound["cost_version"],
                "cost_hash": bound["cost_hash"],
                "dataset_hash": manifest.dataset_hash,
                "independence_hash": spec.spec_hash,
                "authority": REPLAY_AUTHORITY,
                "authorizable": False,
            }
            for index, (candidate_hash, basis_hash) in enumerate(
                zip(candidate_hashes, basis_hashes, strict=True), start=1
            )
        )
        projection = {
            "schema": "options_copilot.replay.pipeline_projection.v3",
            "scan_run_id": result["scan_run_id"],
            "slot_at": datetime_text(envelope.slot_at),
            "status": status,
            "reasons": reasons,
            **bound,
            "candidate_hashes": candidate_hashes,
            "ranking_basis_hashes": basis_hashes,
            "ranking_snapshot_id": source_snapshot_id,
            "ranking_snapshot_hash": source_snapshot_hash,
            "funnel_trace": funnel_trace,
            "gate_bundle_hash": gate_bundle_hash,
            "pipeline_result_hash": pipeline_result_hash,
        }
        projection_hash = canonical_hash(projection)
        replay_snapshot_hash = (
            None
            if status == "NO_TRADE"
            else canonical_hash(
                {
                    "dataset_hash": manifest.dataset_hash,
                    "pipeline_projection_hash": projection_hash,
                    "rankings": rankings,
                }
            )
        )
        replay_snapshot_id = (
            None
            if replay_snapshot_hash is None
            else f"replay-ranking-{replay_snapshot_hash[:32]}"
        )
        pipeline_mode = "TEST_ONLY" if is_test else "PRODUCTION"
        kwargs = {
            "replay_scan_run_id": scan_id,
            "source_scan_run_id": manifest.scan_run_id,
            "source_ranking_snapshot_id": manifest.ranking_snapshot_id,
            "source_ranking_snapshot_hash": manifest.ranking_snapshot_hash,
            "decision_at": manifest.decision_at,
            "dataset_hash": manifest.dataset_hash,
            "pipeline_version": manifest.pipeline_version,
            "pipeline_hash": manifest.pipeline_hash,
            "pipeline_mode": pipeline_mode,
            "input_hash": bound["input_hash"],
            "evidence_hash": bound["evidence_hash"],
            "broker_snapshot_hash": bound["broker_snapshot_hash"],
            "initial_policy_version": bound["policy_version"],
            "initial_policy_hash": bound["policy_hash"],
            "execution_cost_version": bound["cost_version"],
            "execution_cost_hash": bound["cost_hash"],
            "independence_version": spec.version,
            "independence_hash": spec.spec_hash,
            "independence_test_only": spec.test_only,
            "production_count_eligible": pipeline_mode == "PRODUCTION" and not spec.test_only,
            "source_versions_hash": manifest.source_versions_hash,
            "window_hash": manifest.window_hash,
            "status": status,
            "reasons": reasons,
            "ranking_snapshot_id": replay_snapshot_id,
            "ranking_snapshot_hash": replay_snapshot_hash,
            "rankings": rankings,
            "exclusions": manifest.exclusions,
            "funnel_trace": funnel_trace,
            "gate_bundle_hash": gate_bundle_hash,
            "pipeline_result_hash": pipeline_result_hash,
            "pipeline_projection_hash": projection_hash,
        }
        shell = ReplayResult(**kwargs, replay_hash="0" * 64)
        replay = ReplayResult(
            **kwargs, replay_hash=canonical_hash(shell.identity_document())
        )
        return replay.verify()


def replay_scan_run_id(manifest: DatasetManifest) -> str:
    if not isinstance(manifest, DatasetManifest):
        raise TypeError("manifest must be a DatasetManifest")
    return "replay-" + canonical_hash(
        {
            "dataset_hash": manifest.dataset_hash,
            "decision_at": datetime_text(manifest.decision_at),
            "source_scan_run_id": manifest.scan_run_id,
            "pipeline_version": manifest.pipeline_version,
            "pipeline_hash": manifest.pipeline_hash,
        }
    )[:32]


def _verify_pipeline_result_schema(result: Mapping[str, object]) -> None:
    if not isinstance(result, Mapping):
        raise ReplayBindingError("PIPELINE_RESULT_INVALID")
    keys = set(result)
    missing = sorted(_PIPELINE_RESULT_REQUIRED_FIELDS.difference(keys))
    if missing:
        raise ReplayBindingError(f"PIPELINE_RESULT_FIELD_MISSING:{missing[0]}")
    _pipeline_result_funnel_trace(result)
    for authority_name in ("authority", "decision_authority"):
        authority = result.get(authority_name)
        if authority is None:
            continue
        normalized = _text(authority_name, authority).upper()
        if normalized in {"LIVE", "PRODUCTION"}:
            raise ReplaySafetyError("SOURCE_LIVE_AUTHORITY_FORBIDDEN")
        if authority_name == "authority" and normalized != REPLAY_AUTHORITY:
            raise ReplaySafetyError("SOURCE_AUTHORITY_INVALID")
    for name in _SOURCE_AUTHORITY_BOOLEAN_FIELDS:
        if name not in result:
            continue
        value = result[name]
        if not isinstance(value, bool):
            raise ReplayBindingError(f"SOURCE_AUTHORITY_FIELD_INVALID:{name}")
        if value:
            raise ReplaySafetyError(f"SOURCE_AUTHORITY_ESCALATION:{name}")
    for name in _DANGEROUS_AUTHORITY_BOOLEAN_ALIASES:
        if result.get(name) is True:
            raise ReplaySafetyError(f"SOURCE_AUTHORITY_ESCALATION:{name}")
    unknown = sorted(
        keys.difference(
            _PIPELINE_RESULT_REQUIRED_FIELDS | _PIPELINE_RESULT_OPTIONAL_FIELDS
        )
    )
    if unknown:
        raise ReplayBindingError(f"PIPELINE_RESULT_FIELD_UNKNOWN:{unknown[0]}")


def _pipeline_result_funnel_trace(
    result: Mapping[str, object],
) -> Mapping[str, object]:
    try:
        scan_run_id = _text("scan_run_id", result.get("scan_run_id"))
        trace = normalize_funnel_trace(
            result.get("funnel_trace"),
            scan_run_id=scan_run_id,
        )
    except (TypeError, ValueError) as exc:
        raise ReplayBindingError("PIPELINE_RESULT_FUNNEL_TRACE_INVALID") from exc
    raw_candidates = result.get("candidate_hashes")
    if not isinstance(raw_candidates, Sequence) or isinstance(
        raw_candidates,
        (str, bytes, bytearray, memoryview),
    ):
        raise ReplayBindingError("PIPELINE_RESULT_CANDIDATE_HASHES_INVALID")
    if trace and trace.get("ranked_count") != len(raw_candidates):
        raise ReplayBindingError("PIPELINE_RESULT_FUNNEL_RANKED_COUNT_MISMATCH")
    return trace


def _verify_pipeline_result_hash(result: Mapping[str, object]) -> str:
    supplied = _digest("result_hash", result["result_hash"])
    body = {key: value for key, value in result.items() if key != "result_hash"}
    if canonical_hash(body) != supplied:
        raise ReplayBindingError("PIPELINE_RESULT_HASH_MISMATCH")
    return supplied


def _verify_complete_bindings(
    result: Mapping[str, Any], manifest: DatasetManifest
) -> dict[str, str]:
    input_hash = _required_hash(result, "input_hash")
    evidence_hash = _required_hash(result, "evidence_hash")
    broker_hash = _required_hash(result, "broker_snapshot_hash")
    policy_version = _consistent_alias(
        result, ("policy_version", "current_policy_version"), digest=False
    )
    policy_hash = _consistent_alias(
        result, ("policy_hash", "current_policy_hash"), digest=True
    )
    cost_version = _consistent_alias(
        result,
        ("cost_version", "execution_cost_contract_version"),
        digest=False,
    )
    cost_hash = _consistent_alias(
        result,
        ("cost_hash", "execution_cost_contract_hash"),
        digest=True,
    )
    expected = {
        "input_hash": manifest.input_hash,
        "evidence_hash": manifest.evidence_hash,
        "broker_snapshot_hash": manifest.broker_snapshot_hash,
        "policy_version": manifest.initial_policy_version,
        "policy_hash": manifest.initial_policy_hash,
        "cost_version": manifest.execution_cost_version,
        "cost_hash": manifest.execution_cost_hash,
    }
    actual = {
        "input_hash": input_hash,
        "evidence_hash": evidence_hash,
        "broker_snapshot_hash": broker_hash,
        "policy_version": policy_version,
        "policy_hash": policy_hash,
        "cost_version": cost_version,
        "cost_hash": cost_hash,
    }
    if actual != expected:
        raise ReplayBindingError("PIPELINE_RESULT_BINDING_MISMATCH")
    return actual


def _consistent_alias(
    result: Mapping[str, Any], names: tuple[str, ...], *, digest: bool
) -> str:
    present = [(name, result[name]) for name in names if name in result]
    if not present or any(value is None for _, value in present):
        raise ReplayBindingError("PIPELINE_RESULT_BINDING_INCOMPLETE")
    normalized = tuple(
        _digest(name, value) if digest else _text(name, value)
        for name, value in present
    )
    if len(set(normalized)) != 1:
        raise ReplayBindingError("PIPELINE_RESULT_ALIAS_DISAGREEMENT")
    return normalized[0]


def _required_hash(result: Mapping[str, Any], name: str) -> str:
    if result.get(name) is None:
        raise ReplayBindingError("PIPELINE_RESULT_BINDING_INCOMPLETE")
    return _digest(name, result[name])


def _required_text(result: Mapping[str, Any], name: str) -> str:
    if result.get(name) is None:
        raise ReplayBindingError("PIPELINE_RESULT_BINDING_INCOMPLETE")
    return _text(name, result[name])


def _reasons(result: Mapping[str, Any]) -> tuple[str, ...]:
    raw = result["reasons"]
    if isinstance(raw, str):
        values = (raw,)
    elif isinstance(raw, Sequence) and not isinstance(
        raw, (str, bytes, bytearray, memoryview)
    ):
        values = tuple(_text("reason", item) for item in raw)
    else:
        raise ReplayBindingError("PIPELINE_REASONS_INVALID")
    return tuple(sorted(set(values)))


def _hash_sequence(field: str, value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raise ReplayBindingError("PIPELINE_RESULT_BINDING_INCOMPLETE")
    return tuple(_digest(field, item) for item in value)


def _document(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    return vars(value) if hasattr(value, "__dict__") else {}


def _require_plain_artifact_data(value: object, *, field: str) -> None:
    """Reject active/custom containers before invoking any of their methods."""

    value_type = type(value)
    if value_type in {type(None), str, bool, int, float, Decimal, date, datetime}:
        return
    if value_type is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError(f"{field} artifact keys must be exact str")
            _require_plain_artifact_data(item, field=f"{field}.{key}")
        return
    if value_type in {list, tuple}:
        for index, item in enumerate(value):
            _require_plain_artifact_data(item, field=f"{field}[{index}]")
        return
    raise TypeError(
        f"{field} artifact data must use exact dict/list/tuple or explicit scalars"
    )


def _text(field: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReplayBindingError(f"{field} must be nonblank")
    return value.strip()


def _digest(field: str, value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ReplayBindingError(f"{field} must be lowercase SHA-256")
    return value


__all__ = [
    "FrozenHistoricalClock",
    "HistoricalDecisionPipeline",
    "HistoricalPipelineArtifact",
    "PointInTimeReplay",
    "REPLAY_AUTHORITY",
    "ReplayBindingError",
    "ReplayError",
    "ReplayOnlyRankingStore",
    "ReplayResult",
    "ReplaySafetyError",
    "TestHistoricalDecisionPipeline",
    "replay_scan_run_id",
]
