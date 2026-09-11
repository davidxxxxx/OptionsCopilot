"""Canonical explanatory Gate contracts for the read-only decision pipeline.

The types in this module summarize existing broker, risk, payoff, cost, and
ranking decisions.  They deliberately own no broker access, strategy
generation, approval, instruction, Creator, or order authority.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Any

from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


GATE_BUNDLE_SCHEMA = "options_copilot.gate_bundle.v1"
GATE_ROUTING_SCHEMA = "options_copilot.gate_routing_context.v1"
GATE_APPEND_SCHEMA = "options_copilot.gate_append_payload.v1"
PROVISIONAL_WATCH_SCHEMA = "options_copilot.provisional_watch_gate_preview.v1"
GATE_VERSION = 1


class GateStatus(str, Enum):
    """Candidate Gate status; never a story or provider status."""

    PASS = "PASS"
    BLOCK = "BLOCK"
    UNAVAILABLE = "UNAVAILABLE"


class GateAuthority(str, Enum):
    """Authority carried by one Gate layer."""

    HARD = "HARD"
    ROUTER = "ROUTER"
    SUPPORTING_ONLY = "SUPPORTING_ONLY"


class GateId(str, Enum):
    AUTHORITY_DATA = "GATE_1_AUTHORITY_DATA"
    MARKET_CREDIT_REGIME = "GATE_2_MARKET_CREDIT_REGIME"
    UNDERLYING_EVENT = "GATE_3_UNDERLYING_EVENT"
    OPTION_EDGE_LIQUIDITY = "GATE_4_OPTION_EDGE_LIQUIDITY"
    STRUCTURE_ACCOUNT_RISK = "GATE_5_STRUCTURE_ACCOUNT_RISK"
    RANKING_REVIEWABILITY = "GATE_6_RANKING_REVIEWABILITY"


class PipelineGateOutcome(str, Enum):
    TRADE = "TRADE"
    NO_TRADE = "NO_TRADE"


class SupportingStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    DEGRADED = "DEGRADED"


class SourceComponentStatus(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    BLOCKED_EXTERNAL = "BLOCKED_EXTERNAL"


_GATE_AUTHORITIES = {
    GateId.AUTHORITY_DATA: GateAuthority.HARD,
    GateId.MARKET_CREDIT_REGIME: GateAuthority.ROUTER,
    GateId.UNDERLYING_EVENT: GateAuthority.HARD,
    GateId.OPTION_EDGE_LIQUIDITY: GateAuthority.HARD,
    GateId.STRUCTURE_ACCOUNT_RISK: GateAuthority.HARD,
    GateId.RANKING_REVIEWABILITY: GateAuthority.HARD,
}
_PRE_RANK_HARD_GATES = frozenset(
    {
        GateId.AUTHORITY_DATA,
        GateId.UNDERLYING_EVENT,
        GateId.OPTION_EDGE_LIQUIDITY,
        GateId.STRUCTURE_ACCOUNT_RISK,
    },
)
_HARD_GATES = frozenset(
    gate_id
    for gate_id, authority in _GATE_AUTHORITIES.items()
    if authority is GateAuthority.HARD
)
_GATE_1_PASS_BINDINGS = frozenset(
    {
        "broker_snapshot_hash",
        "strategy_nav_authority_hash",
        "strategy_nav_content_hash",
        "strategy_nav_contract_hash",
        "strategy_nav_ledger_head_hash",
        "proposal_hash",
    },
)
_GATE_6_FORBIDDEN_KEYS = frozenset(
    {
        "evidence_inputs",
        "final_ranking_basis",
        "final_ranking_basis_hash",
        "gate_bundle_hash",
        "ranking_basis",
        "ranking_basis_hash",
        "ranking_row_hash",
        "ranking_snapshot_hash",
        "snapshot_hash",
    },
)
_HEX = frozenset("0123456789abcdef")


def candidate_gate_key(candidate_id: str, proposal_hash: str) -> str:
    """Return the canonical candidate/proposal map key from the approved ADR."""

    return canonical_hash(
        {
            "candidate_id": _identity("candidate_id", candidate_id),
            "proposal_hash": _digest("proposal_hash", proposal_hash),
        },
    )


@dataclass(frozen=True, slots=True)
class SupportingInput:
    """One immutable advisory input that cannot affect decision authority."""

    source: str
    status: SupportingStatus
    reason_codes: tuple[str, ...]
    observed_at: datetime
    source_hash: str | None
    payload: object
    supporting_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _identity("source", self.source))
        object.__setattr__(self, "status", SupportingStatus(self.status))
        object.__setattr__(self, "reason_codes", _codes(self.reason_codes))
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )
        object.__setattr__(
            self,
            "source_hash",
            _optional_digest("source_hash", self.source_hash),
        )
        object.__setattr__(self, "payload", freeze_json(self.payload))
        _digest("supporting_hash", self.supporting_hash)
        if canonical_hash(self.hash_payload()) != self.supporting_hash:
            raise ValueError("supporting_hash does not match the canonical advisory input")

    @classmethod
    def build(
        cls,
        *,
        source: str,
        status: SupportingStatus,
        observed_at: datetime,
        reason_codes: Sequence[str] = (),
        source_hash: str | None = None,
        payload: object = None,
    ) -> SupportingInput:
        values = {
            "source": _identity("source", source),
            "status": SupportingStatus(status),
            "reason_codes": _codes(reason_codes),
            "observed_at": utc_datetime(observed_at, field="observed_at"),
            "source_hash": _optional_digest("source_hash", source_hash),
            "payload": freeze_json({} if payload is None else payload),
        }
        return cls(**values, supporting_hash=canonical_hash(_supporting_payload(values)))

    @classmethod
    def degraded(
        cls,
        *,
        source: str,
        observed_at: datetime,
        reason_code: str,
    ) -> SupportingInput:
        """Build the deterministic optional/DeepSeek unavailable fallback."""

        return cls.build(
            source=source,
            status=SupportingStatus.DEGRADED,
            observed_at=observed_at,
            reason_codes=(reason_code,),
            payload={},
        )

    def hash_payload(self) -> dict[str, object]:
        return _supporting_payload(
            {
                "source": self.source,
                "status": self.status,
                "reason_codes": self.reason_codes,
                "observed_at": self.observed_at,
                "source_hash": self.source_hash,
                "payload": self.payload,
            },
        )

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "supporting_hash": self.supporting_hash}


@dataclass(frozen=True, slots=True)
class SourceDisposition:
    """Separate provider/story readiness from candidate Gate status."""

    source: str
    mandatory: bool
    component_status: SourceComponentStatus
    gate_status: GateStatus | None
    authority: GateAuthority
    reason_codes: tuple[str, ...]
    disposition_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _identity("source", self.source))
        if not isinstance(self.mandatory, bool):
            raise TypeError("mandatory must be a bool")
        object.__setattr__(
            self,
            "component_status",
            SourceComponentStatus(self.component_status),
        )
        object.__setattr__(
            self,
            "gate_status",
            None if self.gate_status is None else GateStatus(self.gate_status),
        )
        object.__setattr__(self, "authority", GateAuthority(self.authority))
        object.__setattr__(self, "reason_codes", _codes(self.reason_codes))
        _digest("disposition_hash", self.disposition_hash)
        if canonical_hash(self.hash_payload()) != self.disposition_hash:
            raise ValueError("disposition_hash does not match source disposition")

    def hash_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "mandatory": self.mandatory,
            "component_status": self.component_status.value,
            "gate_status": None if self.gate_status is None else self.gate_status.value,
            "authority": self.authority.value,
            "reason_codes": self.reason_codes,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "disposition_hash": self.disposition_hash}


def classify_source_availability(
    *,
    source: str,
    mandatory: bool,
    available: bool,
    reason_code: str,
) -> SourceDisposition:
    """Classify optional absence as DEGRADED and mandatory absence as external block."""

    if not isinstance(mandatory, bool) or not isinstance(available, bool):
        raise TypeError("mandatory and available must be bools")
    if available:
        component_status = SourceComponentStatus.READY
        gate_status = GateStatus.PASS if mandatory else None
        authority = GateAuthority.HARD if mandatory else GateAuthority.SUPPORTING_ONLY
        reasons: tuple[str, ...] = ()
    elif mandatory:
        component_status = SourceComponentStatus.BLOCKED_EXTERNAL
        gate_status = GateStatus.UNAVAILABLE
        authority = GateAuthority.HARD
        reasons = _codes((reason_code,))
    else:
        component_status = SourceComponentStatus.DEGRADED
        gate_status = None
        authority = GateAuthority.SUPPORTING_ONLY
        reasons = _codes((reason_code,))
    payload = {
        "source": _identity("source", source),
        "mandatory": mandatory,
        "component_status": component_status,
        "gate_status": gate_status,
        "authority": authority,
        "reason_codes": reasons,
    }
    provisional = _provisional(SourceDisposition, payload)
    return SourceDisposition(
        **payload,
        disposition_hash=canonical_hash(provisional.hash_payload()),
    )


@dataclass(frozen=True, slots=True)
class GateLayerResult:
    """One immutable layer result derived from existing authority decisions."""

    gate_id: GateId
    status: GateStatus
    authority: GateAuthority
    reason_codes: tuple[str, ...]
    observed_at: datetime
    source_hashes: tuple[str, ...]
    input_hash: str
    allowed_strategy_families: tuple[str, ...]
    blocked_strategy_families: tuple[str, ...]
    bindings: Mapping[str, object]
    supporting_inputs: tuple[SupportingInput, ...]
    gate_hash: str

    def __post_init__(self) -> None:
        gate_id = GateId(self.gate_id)
        status = GateStatus(self.status)
        authority = GateAuthority(self.authority)
        if authority is not _GATE_AUTHORITIES[gate_id]:
            raise ValueError(f"{gate_id.value} authority must be {_GATE_AUTHORITIES[gate_id].value}")
        object.__setattr__(self, "gate_id", gate_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "authority", authority)
        object.__setattr__(self, "reason_codes", _codes(self.reason_codes))
        if status is not GateStatus.PASS and not self.reason_codes:
            raise ValueError("BLOCK and UNAVAILABLE Gate results require a stable reason code")
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )
        object.__setattr__(self, "source_hashes", _digests("source_hashes", self.source_hashes))
        _digest("input_hash", self.input_hash)
        allowed = _families(self.allowed_strategy_families)
        blocked = _families(self.blocked_strategy_families)
        if set(allowed) & set(blocked):
            raise ValueError("allowed and blocked strategy families must be disjoint")
        object.__setattr__(self, "allowed_strategy_families", allowed)
        object.__setattr__(self, "blocked_strategy_families", blocked)
        bindings = _canonical_mapping("bindings", self.bindings, allow_empty=True)
        object.__setattr__(self, "bindings", MappingProxyType(bindings))
        supporting = tuple(sorted(self.supporting_inputs, key=lambda item: (item.source, item.supporting_hash)))
        if any(not isinstance(item, SupportingInput) for item in supporting):
            raise TypeError("supporting_inputs must contain SupportingInput values")
        if supporting and gate_id is not GateId.UNDERLYING_EVENT:
            raise ValueError("advisory supporting_inputs belong only to Gate 3")
        object.__setattr__(self, "supporting_inputs", supporting)
        if gate_id is GateId.AUTHORITY_DATA:
            proposal_hash = bindings.get("proposal_hash")
            _digest("bindings.proposal_hash", proposal_hash)
            if status is GateStatus.PASS:
                missing = sorted(_GATE_1_PASS_BINDINGS - set(bindings))
                if missing:
                    raise ValueError(f"PASS Gate 1 is missing authority bindings: {missing}")
                for key in _GATE_1_PASS_BINDINGS:
                    _digest(f"bindings.{key}", bindings[key])
        if gate_id is GateId.RANKING_REVIEWABILITY:
            _reject_forbidden_keys(bindings, path="bindings")
        _digest("gate_hash", self.gate_hash)
        if canonical_hash(self.hash_payload()) != self.gate_hash:
            raise ValueError("gate_hash does not match the canonical layer result")

    @classmethod
    def build(
        cls,
        *,
        gate_id: GateId,
        status: GateStatus,
        observed_at: datetime,
        input_payload: Mapping[str, object],
        reason_codes: Sequence[str] = (),
        source_hashes: Sequence[str] = (),
        allowed_strategy_families: Sequence[str] = (),
        blocked_strategy_families: Sequence[str] = (),
        bindings: Mapping[str, object] | None = None,
        supporting_inputs: Sequence[SupportingInput] = (),
    ) -> GateLayerResult:
        normalized_gate_id = GateId(gate_id)
        canonical_input = _canonical_mapping("input_payload", input_payload)
        if normalized_gate_id is GateId.RANKING_REVIEWABILITY:
            _reject_forbidden_keys(canonical_input, path="input_payload")
        values = {
            "gate_id": normalized_gate_id,
            "status": GateStatus(status),
            "authority": _GATE_AUTHORITIES[normalized_gate_id],
            "reason_codes": _codes(reason_codes),
            "observed_at": utc_datetime(observed_at, field="observed_at"),
            "source_hashes": _digests("source_hashes", source_hashes),
            "input_hash": canonical_hash(canonical_input),
            "allowed_strategy_families": _families(allowed_strategy_families),
            "blocked_strategy_families": _families(blocked_strategy_families),
            "bindings": _canonical_mapping("bindings", bindings or {}, allow_empty=True),
            "supporting_inputs": tuple(
                sorted(supporting_inputs, key=lambda item: (item.source, item.supporting_hash))
            ),
        }
        provisional = _provisional(cls, values)
        return cls(**values, gate_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "gate_id": self.gate_id.value,
            "status": self.status.value,
            "authority": self.authority.value,
            "reason_codes": self.reason_codes,
            "observed_at": datetime_text(self.observed_at),
            "source_hashes": self.source_hashes,
            "input_hash": self.input_hash,
            "allowed_strategy_families": self.allowed_strategy_families,
            "blocked_strategy_families": self.blocked_strategy_families,
            "bindings": thaw_json(self.bindings),
            "supporting_inputs": tuple(item.as_dict() for item in self.supporting_inputs),
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "gate_hash": self.gate_hash}

    def authority_projection(self) -> dict[str, object]:
        """Return fields allowed to influence routing/eligibility/review authority."""

        return {
            "gate_id": self.gate_id.value,
            "status": self.status.value,
            "authority": self.authority.value,
            "reason_codes": self.reason_codes,
            "observed_at": datetime_text(self.observed_at),
            "source_hashes": self.source_hashes,
            "input_hash": self.input_hash,
            "allowed_strategy_families": self.allowed_strategy_families,
            "blocked_strategy_families": self.blocked_strategy_families,
            "bindings": thaw_json(self.bindings),
        }


@dataclass(frozen=True, slots=True)
class GateRoutingContext:
    """Pre-generation immutable Gate 2 routing result."""

    schema: str
    version: int
    scan_run_id: str
    cutoff_at: datetime
    valid_until: datetime
    market_credit_gate: GateLayerResult
    routing_context_hash: str

    def __post_init__(self) -> None:
        if self.schema != GATE_ROUTING_SCHEMA or self.version != GATE_VERSION:
            raise ValueError("unsupported Gate routing schema/version")
        object.__setattr__(self, "scan_run_id", _identity("scan_run_id", self.scan_run_id))
        cutoff = utc_datetime(self.cutoff_at, field="cutoff_at")
        valid_until = utc_datetime(self.valid_until, field="valid_until")
        if valid_until <= cutoff:
            raise ValueError("valid_until must be later than cutoff_at")
        object.__setattr__(self, "cutoff_at", cutoff)
        object.__setattr__(self, "valid_until", valid_until)
        if self.market_credit_gate.gate_id is not GateId.MARKET_CREDIT_REGIME:
            raise ValueError("routing context must carry only Gate 2")
        if (
            self.market_credit_gate.status is not GateStatus.PASS
            and self.market_credit_gate.allowed_strategy_families
        ):
            raise ValueError("an unavailable or blocked route cannot allow strategy families")
        if self.market_credit_gate.observed_at > valid_until:
            raise ValueError("Gate 2 observation cannot be later than valid_until")
        _digest("routing_context_hash", self.routing_context_hash)
        if canonical_hash(self.hash_payload()) != self.routing_context_hash:
            raise ValueError("routing_context_hash does not match routing context")

    @classmethod
    def build(
        cls,
        *,
        scan_run_id: str,
        cutoff_at: datetime,
        valid_until: datetime,
        market_credit_gate: GateLayerResult,
    ) -> GateRoutingContext:
        values = {
            "schema": GATE_ROUTING_SCHEMA,
            "version": GATE_VERSION,
            "scan_run_id": _identity("scan_run_id", scan_run_id),
            "cutoff_at": utc_datetime(cutoff_at, field="cutoff_at"),
            "valid_until": utc_datetime(valid_until, field="valid_until"),
            "market_credit_gate": market_credit_gate,
        }
        provisional = _provisional(cls, values)
        return cls(**values, routing_context_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "scan_run_id": self.scan_run_id,
            "cutoff_at": datetime_text(self.cutoff_at),
            "valid_until": datetime_text(self.valid_until),
            "market_credit_gate": self.market_credit_gate.as_dict(),
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "routing_context_hash": self.routing_context_hash}


@dataclass(frozen=True, slots=True)
class RankingGateContext:
    """Non-circular rank result consumed by Gate 6 before bundle persistence."""

    rank: int | None
    total_ranked_count: int
    pre_gate_rank_order_payload: Mapping[str, object] | None
    pre_gate_rank_order_hash: str | None
    authority_consistent: bool
    view_only: bool
    reviewable: bool
    reason_codes: tuple[str, ...]
    context_hash: str

    def __post_init__(self) -> None:
        if self.rank is not None and (isinstance(self.rank, bool) or self.rank < 1 or self.rank > 10):
            raise ValueError("rank must be None or an integer from 1 through 10")
        if isinstance(self.total_ranked_count, bool) or not 0 <= self.total_ranked_count <= 10:
            raise ValueError("total_ranked_count must be from 0 through 10")
        if self.rank is not None and self.rank > self.total_ranked_count:
            raise ValueError("rank cannot exceed total_ranked_count")
        if self.pre_gate_rank_order_payload is None:
            rank_payload = None
        else:
            rank_payload = _canonical_mapping(
                "pre_gate_rank_order_payload",
                self.pre_gate_rank_order_payload,
            )
            _reject_forbidden_keys(rank_payload, path="pre_gate_rank_order_payload")
        object.__setattr__(
            self,
            "pre_gate_rank_order_payload",
            None if rank_payload is None else MappingProxyType(rank_payload),
        )
        rank_order_hash = _optional_digest(
            "pre_gate_rank_order_hash",
            self.pre_gate_rank_order_hash,
        )
        object.__setattr__(self, "pre_gate_rank_order_hash", rank_order_hash)
        for name in ("authority_consistent", "view_only", "reviewable"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a bool")
        if (self.rank is None or not self.authority_consistent) and not self.view_only:
            raise ValueError("unranked or authority-inconsistent contexts must be view-only")
        if (self.rank is None or not self.authority_consistent) and self.reviewable:
            raise ValueError("unranked or authority-inconsistent contexts cannot be reviewable")
        if self.rank is None:
            if rank_payload is not None or rank_order_hash is not None or self.reviewable:
                raise ValueError("unranked candidates cannot carry pre-Gate rank authority")
        else:
            if rank_payload is None or rank_order_hash is None:
                raise ValueError("ranked candidates require canonical pre-Gate rank order")
            ordered_keys = rank_payload.get("ordered_candidate_keys")
            if not isinstance(ordered_keys, Sequence) or isinstance(
                ordered_keys,
                (str, bytes, bytearray),
            ):
                raise ValueError(
                    "pre_gate_rank_order_payload requires ordered_candidate_keys",
                )
            normalized_ordered_keys = _digests(
                "ordered_candidate_keys",
                tuple(ordered_keys),
                preserve_order=True,
            )
            if len(normalized_ordered_keys) != self.total_ranked_count:
                raise ValueError(
                    "ordered_candidate_keys must match total_ranked_count",
                )
            rank_payload["ordered_candidate_keys"] = list(normalized_ordered_keys)
            object.__setattr__(
                self,
                "pre_gate_rank_order_payload",
                MappingProxyType(rank_payload),
            )
            if canonical_hash(rank_payload) != rank_order_hash:
                raise ValueError("pre_gate_rank_order_hash does not match its canonical payload")
            if self.rank > 1 and (not self.view_only or self.reviewable):
                raise ValueError("ranks 2-10 must be view-only and never reviewable")
            if self.reviewable != (self.rank == 1 and self.authority_consistent and not self.view_only):
                raise ValueError("reviewable must derive only from rank-one consistent authority")
        object.__setattr__(self, "reason_codes", _codes(self.reason_codes))
        _digest("context_hash", self.context_hash)
        if canonical_hash(self.hash_payload()) != self.context_hash:
            raise ValueError("context_hash does not match ranking Gate context")

    @classmethod
    def build(
        cls,
        *,
        rank: int | None,
        total_ranked_count: int,
        pre_gate_rank_order_payload: Mapping[str, object] | None,
        authority_consistent: bool,
        reason_codes: Sequence[str] = (),
        rank_one_view_only: bool = False,
    ) -> RankingGateContext:
        view_only = (
            rank is None
            or rank > 1
            or rank_one_view_only
            or not authority_consistent
        )
        reviewable = bool(rank == 1 and authority_consistent and not view_only)
        if pre_gate_rank_order_payload is None:
            rank_payload = None
            rank_order_hash = None
        else:
            rank_payload = _canonical_mapping(
                "pre_gate_rank_order_payload",
                pre_gate_rank_order_payload,
            )
            _reject_forbidden_keys(rank_payload, path="pre_gate_rank_order_payload")
            rank_order_hash = canonical_hash(rank_payload)
        values = {
            "rank": rank,
            "total_ranked_count": total_ranked_count,
            "pre_gate_rank_order_payload": rank_payload,
            "pre_gate_rank_order_hash": rank_order_hash,
            "authority_consistent": authority_consistent,
            "view_only": view_only,
            "reviewable": reviewable,
            "reason_codes": _codes(reason_codes),
        }
        provisional = _provisional(cls, values)
        return cls(**values, context_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "rank": self.rank,
            "total_ranked_count": self.total_ranked_count,
            "pre_gate_rank_order_payload": (
                None
                if self.pre_gate_rank_order_payload is None
                else thaw_json(self.pre_gate_rank_order_payload)
            ),
            "pre_gate_rank_order_hash": self.pre_gate_rank_order_hash,
            "authority_consistent": self.authority_consistent,
            "view_only": self.view_only,
            "reviewable": self.reviewable,
            "reason_codes": self.reason_codes,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "context_hash": self.context_hash}


@dataclass(frozen=True, slots=True)
class CandidateGateResult:
    """Post-generation six-layer result for one candidate/proposal pair."""

    candidate_id: str
    candidate_hash: str
    proposal_hash: str
    candidate_key: str
    strategy_family: str
    layers: tuple[GateLayerResult, ...]
    ranking: RankingGateContext
    candidate_gate_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", _identity("candidate_id", self.candidate_id))
        object.__setattr__(self, "candidate_hash", _digest("candidate_hash", self.candidate_hash))
        object.__setattr__(self, "proposal_hash", _digest("proposal_hash", self.proposal_hash))
        expected_key = candidate_gate_key(self.candidate_id, self.proposal_hash)
        if _digest("candidate_key", self.candidate_key) != expected_key:
            raise ValueError("candidate_key does not match candidate_id + proposal_hash")
        object.__setattr__(
            self,
            "strategy_family",
            _identity("strategy_family", self.strategy_family).upper(),
        )
        layers = tuple(sorted(self.layers, key=lambda item: list(GateId).index(item.gate_id)))
        if len(layers) != len(GateId) or {item.gate_id for item in layers} != set(GateId):
            raise ValueError("candidate result must contain exactly one of each six Gate layers")
        object.__setattr__(self, "layers", layers)
        for layer in layers:
            if layer.gate_id is GateId.RANKING_REVIEWABILITY:
                continue
            if (
                self.strategy_family in layer.blocked_strategy_families
                and layer.status is GateStatus.PASS
            ):
                raise ValueError(
                    f"{layer.gate_id.value} cannot PASS while blocking candidate strategy_family",
                )
            if (
                layer.status is GateStatus.PASS
                and layer.allowed_strategy_families
                and self.strategy_family not in layer.allowed_strategy_families
            ):
                raise ValueError(
                    f"{layer.gate_id.value} PASS does not allow candidate strategy_family",
                )
        gate_1 = self.layer(GateId.AUTHORITY_DATA)
        if gate_1.bindings.get("proposal_hash") != self.proposal_hash:
            raise ValueError("Gate 1 proposal binding does not match candidate proposal_hash")
        gate_6 = self.layer(GateId.RANKING_REVIEWABILITY)
        if gate_6.input_hash != self.ranking.context_hash:
            raise ValueError("Gate 6 input must be the non-circular RankingGateContext")
        if gate_6.bindings.get("rank_authority_context_hash") != self.ranking.context_hash:
            raise ValueError("Gate 6 must bind the exact RankingGateContext hash")
        if gate_6.bindings.get("pre_gate_rank_order_hash") != self.ranking.pre_gate_rank_order_hash:
            raise ValueError("Gate 6 pre-Gate rank order binding mismatch")
        if self.ranking.pre_gate_rank_order_hash is None:
            if gate_6.source_hashes:
                raise ValueError("unranked Gate 6 cannot claim rank-order source hashes")
        elif self.ranking.pre_gate_rank_order_hash not in gate_6.source_hashes:
            raise ValueError("Gate 6 must cite the canonical pre-Gate rank order hash")
        if self.ranking.rank is None and gate_6.status is GateStatus.PASS:
            raise ValueError("an unranked candidate cannot PASS Gate 6")
        if self.ranking.rank is not None and gate_6.status is not GateStatus.PASS:
            raise ValueError("a ranked candidate must PASS Gate 6")
        if self.ranking.rank is not None and not self.ranking.authority_consistent:
            raise ValueError("a ranked candidate requires consistent rank authority")
        if self.ranking.rank is not None and not self.pre_rank_eligible:
            raise ValueError("a candidate with a pre-rank HARD failure cannot be ranked")
        _digest("candidate_gate_hash", self.candidate_gate_hash)
        if canonical_hash(self.hash_payload()) != self.candidate_gate_hash:
            raise ValueError("candidate_gate_hash does not match candidate Gate result")

    @classmethod
    def build(
        cls,
        *,
        candidate_id: str,
        candidate_hash: str,
        proposal_hash: str,
        strategy_family: str,
        layers: Sequence[GateLayerResult],
        ranking: RankingGateContext,
    ) -> CandidateGateResult:
        normalized_id = _identity("candidate_id", candidate_id)
        normalized_proposal = _digest("proposal_hash", proposal_hash)
        values = {
            "candidate_id": normalized_id,
            "candidate_hash": _digest("candidate_hash", candidate_hash),
            "proposal_hash": normalized_proposal,
            "candidate_key": candidate_gate_key(normalized_id, normalized_proposal),
            "strategy_family": _identity("strategy_family", strategy_family).upper(),
            "layers": tuple(sorted(layers, key=lambda item: list(GateId).index(item.gate_id))),
            "ranking": ranking,
        }
        provisional = _provisional(cls, values)
        return cls(**values, candidate_gate_hash=canonical_hash(provisional.hash_payload()))

    def layer(self, gate_id: GateId) -> GateLayerResult:
        normalized = GateId(gate_id)
        return next(item for item in self.layers if item.gate_id is normalized)

    @property
    def pre_rank_eligible(self) -> bool:
        return all(
            self.layer(gate_id).status is GateStatus.PASS
            for gate_id in _PRE_RANK_HARD_GATES
        )

    @property
    def eligible(self) -> bool:
        return self.ranking.rank is not None and all(
            self.layer(gate_id).status is GateStatus.PASS for gate_id in _HARD_GATES
        )

    @property
    def reviewable(self) -> bool:
        return self.eligible and self.ranking.reviewable

    @property
    def hard_failure_gate_ids(self) -> tuple[str, ...]:
        return tuple(
            gate_id.value
            for gate_id in GateId
            if gate_id in _HARD_GATES and self.layer(gate_id).status is not GateStatus.PASS
        )

    def authority_projection(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "proposal_hash": self.proposal_hash,
            "candidate_key": self.candidate_key,
            "strategy_family": self.strategy_family,
            "layers": tuple(item.authority_projection() for item in self.layers),
            "ranking": self.ranking.as_dict(),
            "pre_rank_eligible": self.pre_rank_eligible,
            "eligible": self.eligible,
            "reviewable": self.reviewable,
        }

    def hash_payload(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "proposal_hash": self.proposal_hash,
            "candidate_key": self.candidate_key,
            "strategy_family": self.strategy_family,
            "layers": tuple(item.as_dict() for item in self.layers),
            "ranking": self.ranking.as_dict(),
            "pre_rank_eligible": self.pre_rank_eligible,
            "eligible": self.eligible,
            "reviewable": self.reviewable,
            "hard_failure_gate_ids": self.hard_failure_gate_ids,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "candidate_gate_hash": self.candidate_gate_hash}


@dataclass(frozen=True, slots=True)
class GateBundle:
    """Final run-level two-phase Gate bundle suitable for append-only evidence."""

    schema: str
    version: int
    scan_run_id: str
    cutoff_at: datetime
    valid_until: datetime
    pipeline_input_hash: str
    routing_context: GateRoutingContext
    candidates: Mapping[str, CandidateGateResult]
    ranked_candidate_keys: tuple[str, ...]
    outcome: PipelineGateOutcome
    run_reason_codes: tuple[str, ...]
    gate_bundle_hash: str

    def __post_init__(self) -> None:
        if self.schema != GATE_BUNDLE_SCHEMA or self.version != GATE_VERSION:
            raise ValueError("unsupported Gate bundle schema/version")
        object.__setattr__(self, "scan_run_id", _identity("scan_run_id", self.scan_run_id))
        cutoff = utc_datetime(self.cutoff_at, field="cutoff_at")
        valid_until = utc_datetime(self.valid_until, field="valid_until")
        if valid_until <= cutoff:
            raise ValueError("valid_until must be later than cutoff_at")
        object.__setattr__(self, "cutoff_at", cutoff)
        object.__setattr__(self, "valid_until", valid_until)
        _digest("pipeline_input_hash", self.pipeline_input_hash)
        if self.routing_context.scan_run_id != self.scan_run_id:
            raise ValueError("routing context scan_run_id mismatch")
        if self.routing_context.cutoff_at != cutoff or self.routing_context.valid_until != valid_until:
            raise ValueError("routing context time window mismatch")
        candidates = dict(sorted(self.candidates.items()))
        route_gate = self.routing_context.market_credit_gate
        if route_gate.status is not GateStatus.PASS and candidates:
            raise ValueError("a non-PASS pre-generation route cannot produce candidates")
        if candidates and not route_gate.allowed_strategy_families:
            raise ValueError("a PASS pre-generation route with candidates must allow families")
        for key, candidate in candidates.items():
            if key != candidate.candidate_key:
                raise ValueError("candidate map key must equal candidate_key")
            if candidate.layer(GateId.MARKET_CREDIT_REGIME).gate_hash != self.routing_context.market_credit_gate.gate_hash:
                raise ValueError("candidate Gate 2 must copy the immutable routing Gate")
            if candidate.strategy_family in route_gate.blocked_strategy_families:
                raise ValueError("candidate strategy_family is explicitly blocked by Gate 2")
            if candidate.strategy_family not in route_gate.allowed_strategy_families:
                raise ValueError("candidate strategy_family is not declared allowed by Gate 2")
        object.__setattr__(self, "candidates", MappingProxyType(candidates))
        ranked = tuple(self.ranked_candidate_keys)
        if len(ranked) > 10 or len(set(ranked)) != len(ranked):
            raise ValueError("ranked_candidate_keys must contain at most ten unique keys")
        if any(key not in candidates for key in ranked):
            raise ValueError("ranked_candidate_keys contains an unknown candidate")
        expected_ranked = tuple(
            candidate.candidate_key
            for candidate in sorted(
                (item for item in candidates.values() if item.ranking.rank is not None),
                key=lambda item: item.ranking.rank or 0,
            )
        )
        if ranked != expected_ranked:
            raise ValueError("ranked_candidate_keys must follow canonical rank order")
        if tuple(range(1, len(ranked) + 1)) != tuple(candidates[key].ranking.rank for key in ranked):
            raise ValueError("ranked candidates must have contiguous ranks starting at one")
        if any(not candidates[key].eligible for key in ranked):
            raise ValueError("every ranked candidate must pass every HARD Gate")
        if any(not candidates[key].ranking.authority_consistent for key in ranked):
            raise ValueError("every ranked survivor requires consistent rank authority")
        if any(candidate.ranking.total_ranked_count != len(ranked) for candidate in candidates.values()):
            raise ValueError("every candidate must bind the final total_ranked_count")
        rank_order_hashes = {
            candidates[key].ranking.pre_gate_rank_order_hash for key in ranked
        }
        if len(rank_order_hashes) > 1:
            raise ValueError("ranked candidates must share one canonical pre-Gate rank order")
        if ranked:
            first_rank_payload = candidates[ranked[0]].ranking.pre_gate_rank_order_payload
            if first_rank_payload is None:
                raise ValueError("ranked candidates require pre-Gate rank order provenance")
            if tuple(first_rank_payload["ordered_candidate_keys"]) != ranked:
                raise ValueError("pre-Gate ordered_candidate_keys must equal canonical rank order")
        object.__setattr__(self, "ranked_candidate_keys", ranked)
        outcome = PipelineGateOutcome(self.outcome)
        object.__setattr__(self, "outcome", outcome)
        run_reasons = _codes(self.run_reason_codes)
        object.__setattr__(self, "run_reason_codes", run_reasons)
        if outcome is PipelineGateOutcome.TRADE and not ranked:
            raise ValueError("TRADE requires at least one ranked candidate")
        if outcome is PipelineGateOutcome.NO_TRADE and ranked:
            raise ValueError("NO_TRADE cannot carry ranked candidates")
        if outcome is PipelineGateOutcome.TRADE and run_reasons:
            raise ValueError("TRADE run_reason_codes must be empty")
        if outcome is PipelineGateOutcome.NO_TRADE and not run_reasons:
            raise ValueError("NO_TRADE requires stable run_reason_codes")
        _digest("gate_bundle_hash", self.gate_bundle_hash)
        if canonical_hash(self.hash_payload()) != self.gate_bundle_hash:
            raise ValueError("gate_bundle_hash does not match Gate bundle")

    @classmethod
    def build(
        cls,
        *,
        scan_run_id: str,
        cutoff_at: datetime,
        valid_until: datetime,
        pipeline_input_hash: str,
        routing_context: GateRoutingContext,
        candidates: Sequence[CandidateGateResult],
        outcome: PipelineGateOutcome,
        run_reason_codes: Sequence[str],
    ) -> GateBundle:
        candidate_map = {item.candidate_key: item for item in candidates}
        if len(candidate_map) != len(candidates):
            raise ValueError("candidate results must have unique canonical keys")
        ranked = tuple(
            item.candidate_key
            for item in sorted(
                (item for item in candidates if item.ranking.rank is not None),
                key=lambda item: item.ranking.rank or 0,
            )
        )
        values = {
            "schema": GATE_BUNDLE_SCHEMA,
            "version": GATE_VERSION,
            "scan_run_id": _identity("scan_run_id", scan_run_id),
            "cutoff_at": utc_datetime(cutoff_at, field="cutoff_at"),
            "valid_until": utc_datetime(valid_until, field="valid_until"),
            "pipeline_input_hash": _digest("pipeline_input_hash", pipeline_input_hash),
            "routing_context": routing_context,
            "candidates": candidate_map,
            "ranked_candidate_keys": ranked,
            "outcome": PipelineGateOutcome(outcome),
            "run_reason_codes": _codes(run_reason_codes),
        }
        provisional = _provisional(cls, values)
        return cls(**values, gate_bundle_hash=canonical_hash(provisional.hash_payload()))

    @property
    def hard_failure_candidate_keys(self) -> tuple[str, ...]:
        return tuple(
            key for key, candidate in self.candidates.items() if candidate.hard_failure_gate_ids
        )

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "scan_run_id": self.scan_run_id,
            "cutoff_at": datetime_text(self.cutoff_at),
            "valid_until": datetime_text(self.valid_until),
            "pipeline_input_hash": self.pipeline_input_hash,
            "routing_context": self.routing_context.as_dict(),
            "candidates": {key: value.as_dict() for key, value in self.candidates.items()},
            "ranked_candidate_keys": self.ranked_candidate_keys,
            "outcome": self.outcome.value,
            "run_reason_codes": self.run_reason_codes,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.hash_payload(),
            "hard_failure_candidate_keys": self.hard_failure_candidate_keys,
            "gate_bundle_hash": self.gate_bundle_hash,
        }

    def append_payload(self) -> dict[str, object]:
        """Return one complete immutable record for TRADE or NO_TRADE persistence."""

        return {
            "schema": GATE_APPEND_SCHEMA,
            "record_type": f"GATE_BUNDLE_{self.outcome.value}",
            "scan_run_id": self.scan_run_id,
            "gate_bundle_hash": self.gate_bundle_hash,
            "gate_bundle": self.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class WatchLayerPreview:
    """Non-authoritative explanatory layer for an underlying-only watch item."""

    gate_id: GateId
    status: GateStatus
    reason_codes: tuple[str, ...]
    observed_at: datetime
    source_hashes: tuple[str, ...]
    preview_layer_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "gate_id", GateId(self.gate_id))
        object.__setattr__(self, "status", GateStatus(self.status))
        object.__setattr__(self, "reason_codes", _codes(self.reason_codes))
        if self.status is not GateStatus.PASS and not self.reason_codes:
            raise ValueError("non-PASS watch layers require a stable reason code")
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )
        object.__setattr__(self, "source_hashes", _digests("source_hashes", self.source_hashes))
        _digest("preview_layer_hash", self.preview_layer_hash)
        if canonical_hash(self.hash_payload()) != self.preview_layer_hash:
            raise ValueError("preview_layer_hash does not match watch layer")

    @classmethod
    def build(
        cls,
        *,
        gate_id: GateId,
        status: GateStatus,
        observed_at: datetime,
        reason_codes: Sequence[str] = (),
        source_hashes: Sequence[str] = (),
    ) -> WatchLayerPreview:
        values = {
            "gate_id": GateId(gate_id),
            "status": GateStatus(status),
            "reason_codes": _codes(reason_codes),
            "observed_at": utc_datetime(observed_at, field="observed_at"),
            "source_hashes": _digests("source_hashes", source_hashes),
        }
        provisional = _provisional(cls, values)
        return cls(**values, preview_layer_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "gate_id": self.gate_id.value,
            "status": self.status.value,
            "reason_codes": self.reason_codes,
            "observed_at": datetime_text(self.observed_at),
            "source_hashes": self.source_hashes,
            "authority": GateAuthority.SUPPORTING_ONLY.value,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "preview_layer_hash": self.preview_layer_hash}


@dataclass(frozen=True, slots=True)
class ProvisionalWatchGatePreview:
    """Six-layer OBSERVATION_ONLY preview with no candidate/rank authority."""

    schema: str
    version: int
    watch_id: str
    symbol: str
    cutoff_at: datetime
    source_bundle_hash: str
    layers: tuple[WatchLayerPreview, ...]
    preview_hash: str

    def __post_init__(self) -> None:
        if self.schema != PROVISIONAL_WATCH_SCHEMA or self.version != GATE_VERSION:
            raise ValueError("unsupported provisional watch schema/version")
        symbol = _identity("symbol", self.symbol).upper()
        object.__setattr__(self, "symbol", symbol)
        cutoff = utc_datetime(self.cutoff_at, field="cutoff_at")
        object.__setattr__(self, "cutoff_at", cutoff)
        source_bundle_hash = _digest("source_bundle_hash", self.source_bundle_hash)
        object.__setattr__(self, "source_bundle_hash", source_bundle_hash)
        expected_watch_id = canonical_hash(
            {
                "symbol": symbol,
                "cutoff_at": datetime_text(cutoff),
                "source_bundle_hash": source_bundle_hash,
            },
        )
        if _digest("watch_id", self.watch_id) != expected_watch_id:
            raise ValueError("watch_id does not match its canonical underlying identity")
        layers = tuple(sorted(self.layers, key=lambda item: list(GateId).index(item.gate_id)))
        if len(layers) != len(GateId) or {item.gate_id for item in layers} != set(GateId):
            raise ValueError("watch preview must contain exactly six explanatory layers")
        for gate_id in (
            GateId.AUTHORITY_DATA,
            GateId.OPTION_EDGE_LIQUIDITY,
            GateId.STRUCTURE_ACCOUNT_RISK,
            GateId.RANKING_REVIEWABILITY,
        ):
            layer = next(item for item in layers if item.gate_id is gate_id)
            if layer.status is not GateStatus.UNAVAILABLE:
                raise ValueError(f"{gate_id.value} must be unavailable in a provisional watch")
        object.__setattr__(self, "layers", layers)
        _digest("preview_hash", self.preview_hash)
        if canonical_hash(self.hash_payload()) != self.preview_hash:
            raise ValueError("preview_hash does not match provisional watch")

    @classmethod
    def build(
        cls,
        *,
        symbol: str,
        cutoff_at: datetime,
        source_bundle_hash: str,
        layers: Sequence[WatchLayerPreview],
    ) -> ProvisionalWatchGatePreview:
        normalized_symbol = _identity("symbol", symbol).upper()
        normalized_cutoff = utc_datetime(cutoff_at, field="cutoff_at")
        normalized_source_hash = _digest("source_bundle_hash", source_bundle_hash)
        watch_id = canonical_hash(
            {
                "symbol": normalized_symbol,
                "cutoff_at": datetime_text(normalized_cutoff),
                "source_bundle_hash": normalized_source_hash,
            },
        )
        values = {
            "schema": PROVISIONAL_WATCH_SCHEMA,
            "version": GATE_VERSION,
            "watch_id": watch_id,
            "symbol": normalized_symbol,
            "cutoff_at": normalized_cutoff,
            "source_bundle_hash": normalized_source_hash,
            "layers": tuple(sorted(layers, key=lambda item: list(GateId).index(item.gate_id))),
        }
        provisional = _provisional(cls, values)
        return cls(**values, preview_hash=canonical_hash(provisional.hash_payload()))

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "watch_id": self.watch_id,
            "symbol": self.symbol,
            "cutoff_at": datetime_text(self.cutoff_at),
            "source_bundle_hash": self.source_bundle_hash,
            "status": "PROVISIONAL",
            "decision": "OBSERVATION_ONLY",
            "decision_authority": GateAuthority.SUPPORTING_ONLY.value,
            "layers": tuple(item.as_dict() for item in self.layers),
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "preview_hash": self.preview_hash}


def _supporting_payload(values: Mapping[str, object]) -> dict[str, object]:
    return {
        "source": values["source"],
        "status": SupportingStatus(values["status"]).value,
        "authority": GateAuthority.SUPPORTING_ONLY.value,
        "reason_codes": values["reason_codes"],
        "observed_at": datetime_text(values["observed_at"]),
        "source_hash": values["source_hash"],
        "payload": thaw_json(values["payload"]),
    }


def _identity(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be blank")
    return value.strip()


def _digest(name: str, value: object) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _HEX for char in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")
    return value


def _optional_digest(name: str, value: object) -> str | None:
    return None if value is None else _digest(name, value)


def _digests(
    name: str,
    values: Sequence[str],
    *,
    preserve_order: bool = False,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be a sequence of hashes")
    normalized = tuple(_digest(name, value) for value in values)
    if preserve_order:
        if len(set(normalized)) != len(normalized):
            raise ValueError(f"{name} must not contain duplicates")
        return normalized
    return tuple(sorted(set(normalized)))


def _codes(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("reason_codes must be a sequence")
    return tuple(sorted({_identity("reason_code", value).upper() for value in values}))


def _families(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("strategy families must be a sequence")
    return tuple(sorted({_identity("strategy_family", value).upper() for value in values}))


def _canonical_mapping(
    name: str,
    value: Mapping[str, object],
    *,
    allow_empty: bool = False,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or (not value and not allow_empty):
        qualifier = "a mapping" if allow_empty else "a non-empty mapping"
        raise ValueError(f"{name} must be {qualifier}")
    frozen = freeze_json(value)
    thawed = thaw_json(frozen)
    if not isinstance(thawed, dict):
        raise TypeError(f"{name} must canonicalize to a mapping")
    return dict(sorted(thawed.items()))


def _reject_forbidden_keys(value: object, *, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            if normalized in _GATE_6_FORBIDDEN_KEYS:
                raise ValueError(f"Gate 6 cannot bind circular field {path}.{key}")
            _reject_forbidden_keys(item, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _reject_forbidden_keys(item, path=f"{path}[{index}]")


def _provisional(cls: type[Any], values: Mapping[str, object]) -> Any:
    """Create an internal normalized value solely to compute its final hash."""

    instance = object.__new__(cls)
    for name, value in values.items():
        object.__setattr__(instance, name, value)
    return instance


__all__ = [
    "CandidateGateResult",
    "GATE_APPEND_SCHEMA",
    "GATE_BUNDLE_SCHEMA",
    "GATE_ROUTING_SCHEMA",
    "GATE_VERSION",
    "GateAuthority",
    "GateBundle",
    "GateId",
    "GateLayerResult",
    "GateRoutingContext",
    "GateStatus",
    "PROVISIONAL_WATCH_SCHEMA",
    "PipelineGateOutcome",
    "ProvisionalWatchGatePreview",
    "RankingGateContext",
    "SourceComponentStatus",
    "SourceDisposition",
    "SupportingInput",
    "SupportingStatus",
    "WatchLayerPreview",
    "candidate_gate_key",
    "classify_source_availability",
]
