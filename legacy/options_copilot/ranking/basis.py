"""Canonical candidate, proposal, and ranking-basis bindings.

This module is deliberately pure so the ranker, pipeline, and durable store
share one byte-for-byte definition of the authority-bearing ranking basis.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from options_copilot.storage.canonical import canonical_hash, canonical_json


PROPOSAL_FROM_CANDIDATE_SCHEMA = "options_copilot.proposal_from_candidate.v1"
RANKING_BASIS_SCHEMA = "options_copilot.ranking_basis.v2"


@dataclass(frozen=True, slots=True)
class CanonicalRankingBasis:
    candidate_body: Mapping[str, Any]
    candidate_body_json: str
    candidate_hash: str
    proposal_body: Mapping[str, Any]
    proposal_body_json: str
    proposal_hash: str
    evidence_inputs: Mapping[str, Any]
    evidence_inputs_json: str
    evidence_inputs_hash: str
    basis_payload: Mapping[str, Any]
    basis_json: str
    ranking_basis_hash: str


def build_ranking_basis(
    *,
    candidate_body: Mapping[str, object],
    current_policy_version: str,
    current_policy_hash: str,
    policy_authority_marker_hash: str,
    cost_version: str,
    cost_hash: str,
    risk_contract_hash: str,
    evidence_inputs: Mapping[str, object],
    proposal_body: Mapping[str, object] | None = None,
    candidate_hash: str | None = None,
    proposal_hash: str | None = None,
) -> CanonicalRankingBasis:
    """Build and optionally verify one complete canonical ranking basis.

    A proposal that has not yet been materialized as its own full document is
    represented by a deterministic proposal-from-candidate envelope.  The two
    hashes are therefore never implicitly treated as interchangeable.
    """

    normalized_candidate, candidate_json = _canonical_mapping(
        "candidate_body", candidate_body
    )
    computed_candidate_hash = canonical_hash(normalized_candidate)
    _verify_optional_hash("candidate_hash", candidate_hash, computed_candidate_hash)

    raw_proposal: Mapping[str, object]
    if proposal_body is None:
        raw_proposal = {
            "schema": PROPOSAL_FROM_CANDIDATE_SCHEMA,
            "candidate_body": normalized_candidate,
        }
    else:
        raw_proposal = proposal_body
    normalized_proposal, proposal_json = _canonical_mapping(
        "proposal_body", raw_proposal
    )
    computed_proposal_hash = canonical_hash(normalized_proposal)
    _verify_optional_hash("proposal_hash", proposal_hash, computed_proposal_hash)

    normalized_evidence, evidence_json = _canonical_mapping(
        "evidence_inputs", evidence_inputs
    )
    payload: dict[str, Any] = {
        "schema": RANKING_BASIS_SCHEMA,
        "candidate_body": normalized_candidate,
        "candidate_hash": computed_candidate_hash,
        "proposal_body": normalized_proposal,
        "proposal_hash": computed_proposal_hash,
        "current_policy_version": _identity(
            "current_policy_version", current_policy_version
        ),
        "current_policy_hash": _digest(
            "current_policy_hash", current_policy_hash
        ),
        "policy_authority_marker_hash": _digest(
            "policy_authority_marker_hash", policy_authority_marker_hash
        ),
        "cost_version": _identity("cost_version", cost_version),
        "cost_hash": _digest("cost_hash", cost_hash),
        "risk_contract_hash": _digest("risk_contract_hash", risk_contract_hash),
        "evidence_inputs": normalized_evidence,
    }
    basis_json = canonical_json(payload)
    return CanonicalRankingBasis(
        candidate_body=normalized_candidate,
        candidate_body_json=candidate_json,
        candidate_hash=computed_candidate_hash,
        proposal_body=normalized_proposal,
        proposal_body_json=proposal_json,
        proposal_hash=computed_proposal_hash,
        evidence_inputs=normalized_evidence,
        evidence_inputs_json=evidence_json,
        evidence_inputs_hash=canonical_hash(normalized_evidence),
        basis_payload=payload,
        basis_json=basis_json,
        ranking_basis_hash=canonical_hash(payload),
    )


def _canonical_mapping(
    name: str, value: Mapping[str, object]
) -> tuple[dict[str, Any], str]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{name} must be a non-empty mapping")
    rendered = canonical_json(value)
    normalized = json.loads(rendered)
    if not isinstance(normalized, dict) or not normalized:
        raise ValueError(f"{name} must be a non-empty canonical object")
    return normalized, rendered


def _verify_optional_hash(name: str, supplied: str | None, computed: str) -> None:
    if supplied is None:
        return
    if _digest(name, supplied) != computed:
        raise ValueError(f"{name} does not match its canonical body")


def _digest(name: str, value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")
    return value


def _identity(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be blank")
    return value.strip()


__all__ = [
    "CanonicalRankingBasis",
    "PROPOSAL_FROM_CANDIDATE_SCHEMA",
    "RANKING_BASIS_SCHEMA",
    "build_ranking_basis",
]
