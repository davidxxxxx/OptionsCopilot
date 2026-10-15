"""Server-owned candidate readiness shared by ranking views and approval gates."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from .evidence_manifest import (
    CandidateEvidenceManifestError,
    validate_candidate_evidence_manifest,
)


@dataclass(frozen=True, slots=True)
class CandidateReadiness:
    """Fail-closed readiness derived only from an immutable ranking candidate."""

    candidate_id: str
    underlying: str
    rank: int
    source_health: Mapping[str, object]
    account_capacity: Mapping[str, object]
    challenge_allowed: bool

    @property
    def interaction(self) -> str:
        return "CHALLENGE_ALLOWED" if self.challenge_allowed else "VIEW_ONLY"


class CandidateReadinessError(ValueError):
    """Raised when immutable candidate identity or rank bindings are invalid."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def evaluate_account_capacity(
    candidate: Mapping[str, object],
) -> Mapping[str, object]:
    """Return the detached fail-closed capacity projection for one candidate."""

    return _account_capacity(candidate)


def evaluate_candidate_readiness(
    ranking: Mapping[str, object],
    candidate: Mapping[str, object],
    *,
    expected_candidate_id: str | None = None,
    now: datetime | None = None,
) -> CandidateReadiness:
    """Evaluate one candidate for display and rank-one challenge authority."""

    rank = candidate.get("rank")
    if (
        not isinstance(rank, int)
        or isinstance(rank, bool)
        or not 1 <= rank <= 10
    ):
        raise CandidateReadinessError("INVALID_IMMUTABLE_RANK")

    candidate_id = _identifier(candidate.get("candidate_id"))
    candidate_body = candidate.get("candidate_body")
    if (
        candidate_id is None
        or not isinstance(candidate_body, Mapping)
        or _identifier(candidate_body.get("candidate_id")) != candidate_id
        or (
            expected_candidate_id is not None
            and candidate_id != expected_candidate_id
        )
    ):
        raise CandidateReadinessError("INVALID_IMMUTABLE_CANDIDATE_IDENTITY")

    proposal_body = candidate.get("proposal_body")
    if isinstance(proposal_body, Mapping):
        if (
            "candidate_id" in proposal_body
            and _identifier(proposal_body.get("candidate_id")) != candidate_id
        ):
            raise CandidateReadinessError("INVALID_IMMUTABLE_CANDIDATE_IDENTITY")
        proposal_rank = proposal_body.get("rank")
        if proposal_rank is not None and proposal_rank != rank:
            raise CandidateReadinessError("INVALID_IMMUTABLE_RANK")
    body_rank = candidate_body.get("rank")
    if body_rank is not None and body_rank != rank:
        raise CandidateReadinessError("INVALID_IMMUTABLE_RANK")

    underlying = _canonical_underlying(candidate_body)
    if underlying is None or not _underlying_repetitions_match(
        candidate,
        canonical_underlying=underlying,
    ):
        raise CandidateReadinessError("INVALID_IMMUTABLE_CANDIDATE_UNDERLYING")

    checked_at = now or datetime.now(timezone.utc)
    source_health = _source_health(
        ranking,
        candidate=candidate,
        candidate_id=candidate_id,
        candidate_symbol=underlying,
        now=checked_at,
    )
    account_capacity = _account_capacity(candidate)
    ranking_decision = str(ranking.get("decision") or "").strip().upper()
    decision_allows_candidates = ranking_decision == "CANDIDATES_AVAILABLE"
    challenge_allowed = (
        rank == 1
        and ranking.get("approval_enabled") is True
        and decision_allows_candidates
        and candidate.get("authorizable") is True
        and str(candidate.get("authority_status") or "").upper()
        != "A_GRADE_PENDING"
        and source_health.get("status") == "READY"
        and account_capacity.get("status") == "READY"
    )
    return CandidateReadiness(
        candidate_id=candidate_id,
        underlying=underlying,
        rank=rank,
        source_health=source_health,
        account_capacity=account_capacity,
        challenge_allowed=challenge_allowed,
    )


def _source_health(
    ranking: Mapping[str, object],
    *,
    candidate: Mapping[str, object],
    candidate_id: str,
    candidate_symbol: str,
    now: datetime,
) -> dict[str, object]:
    projection: dict[str, object] = {
        "status": "DEGRADED",
        "reason": "CANDIDATE_EVIDENCE_MANIFEST_UNAVAILABLE",
        "primary_count": 0,
        "supporting_count": 0,
        "contradicting_count": 0,
        "decision_authority": "SUPPORTING_ONLY",
    }
    immutable_inputs = ranking.get("immutable_inputs")
    manifests = (
        immutable_inputs.get("candidate_evidence_manifests")
        if isinstance(immutable_inputs, Mapping)
        else None
    )
    if not isinstance(manifests, Mapping):
        return projection
    manifest = manifests.get(candidate_id)
    if not isinstance(manifest, Mapping):
        return projection
    candidate_body = candidate.get("candidate_body")
    proposal_body = candidate.get("proposal_body")
    score_components = candidate.get("score_components")
    ranked_after_cost_expected_value = (
        score_components.get("after_cost_expected_value")
        if isinstance(score_components, Mapping)
        else None
    )
    if not isinstance(candidate_body, Mapping) or not isinstance(
        proposal_body, Mapping
    ):
        projection["reason"] = "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        return projection
    try:
        validated = validate_candidate_evidence_manifest(
            manifest,
            candidate_id=candidate_id,
            candidate_symbol=candidate_symbol,
            candidate_body=candidate_body,
            proposal_body=proposal_body,
            ranked_after_cost_expected_value=ranked_after_cost_expected_value,
            ranking_broker_snapshot_hash=ranking.get("broker_snapshot_hash"),
            ranking_cost_version=ranking.get("cost_version"),
            ranking_cost_hash=ranking.get("cost_hash"),
            ranking_valid_until=ranking.get("valid_until"),
            now=now,
        )
    except (CandidateEvidenceManifestError, TypeError, ValueError):
        projection["reason"] = "CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        return projection

    projection.update(
        {
            "primary_count": len(validated.primary),
            "supporting_count": len(validated.supporting_references),
            "contradicting_count": len(validated.contradicting_references),
        }
    )
    if validated.contradicting_references:
        projection["reason"] = "CANDIDATE_EVIDENCE_CONTRADICTED"
        return projection
    projection.update(
        {
            "status": "READY",
            "reason": "CANDIDATE_EVIDENCE_PRIMARY_COMPLETE",
        }
    )
    return projection


def _account_capacity(candidate: Mapping[str, object]) -> dict[str, object]:
    candidate_body = candidate.get("candidate_body")
    body = candidate_body if isinstance(candidate_body, Mapping) else {}
    proposal_body = candidate.get("proposal_body")
    proposal = proposal_body if isinstance(proposal_body, Mapping) else {}
    strategy_nav_body = proposal.get("strategy_nav")
    strategy_nav = (
        strategy_nav_body if isinstance(strategy_nav_body, Mapping) else {}
    )
    risk_body = proposal.get("risk")
    risk = risk_body if isinstance(risk_body, Mapping) else {}

    nav, nav_state = _consistent_decimal(
        [
            *[
                body[name]
                for name in ("strategy_nav_usd", "strategy_nav")
                if name in body
            ],
            *[
                proposal[name]
                for name in ("strategy_nav_usd",)
                if name in proposal
            ],
            *[
                strategy_nav[name]
                for name in ("strategy_nav_usd", "strategy_nav")
                if name in strategy_nav
            ],
        ]
    )
    maximum_loss, loss_state = _consistent_decimal(
        [
            *[
                body[name]
                for name in ("max_loss_usd", "max_loss")
                if name in body
            ],
            *[
                proposal[name]
                for name in ("max_loss_usd", "max_loss")
                if name in proposal
            ],
            *[
                risk[name]
                for name in ("maximum_loss_usd", "max_loss_usd", "max_loss")
                if name in risk
            ],
        ]
    )
    risk_fraction, risk_state = _consistent_decimal(
        [
            *[body[name] for name in ("risk_fraction",) if name in body],
            *[
                proposal[name]
                for name in ("risk_fraction",)
                if name in proposal
            ],
            *[risk[name] for name in ("risk_fraction",) if name in risk],
        ]
    )
    authority_status = str(candidate.get("authority_status") or "").upper()
    projection: dict[str, object] = {
        "status": "BLOCKED",
        "reason": "ACCOUNT_CAPACITY_INCONSISTENT",
        "strategy_nav_usd": _decimal_text(nav),
        "max_loss_usd": _decimal_text(maximum_loss),
        "risk_fraction": _decimal_text(risk_fraction),
        "authority_status": authority_status or None,
        "decision_authority": "SUPPORTING_ONLY",
    }
    if nav_state == "MISSING":
        projection["reason"] = "STRATEGY_NAV_UNAVAILABLE"
        return projection
    if loss_state == "MISSING":
        projection["reason"] = "MAX_LOSS_UNAVAILABLE"
        return projection
    if risk_state == "MISSING":
        projection["reason"] = "RISK_FRACTION_UNAVAILABLE"
        return projection
    if (
        nav_state != "VALID"
        or loss_state != "VALID"
        or risk_state != "VALID"
        or nav is None
        or maximum_loss is None
        or risk_fraction is None
        or nav <= 0
        or maximum_loss <= 0
        or risk_fraction < 0
        or maximum_loss / nav != risk_fraction
    ):
        return projection
    if authority_status == "A_GRADE_PENDING" and candidate.get("authorizable") is False:
        projection["reason"] = "ACCOUNT_CAPACITY_NOT_AUTHORIZABLE"
        return projection
    if candidate.get("authorizable") is not True:
        projection["reason"] = "ACCOUNT_CAPACITY_AUTHORITY_INVALID"
        return projection
    if (
        (authority_status == "NORMAL" and risk_fraction <= Decimal("0.10"))
        or (authority_status == "A_GRADE" and risk_fraction <= Decimal("0.15"))
    ):
        projection.update(
            {"status": "READY", "reason": "ACCOUNT_CAPACITY_CONFIRMED"}
        )
        return projection
    projection["reason"] = "ACCOUNT_CAPACITY_AUTHORITY_INVALID"
    return projection


def _consistent_decimal(values: Sequence[object]) -> tuple[Decimal | None, str]:
    if not values:
        return None, "MISSING"
    parsed: list[Decimal] = []
    for value in values:
        if isinstance(value, Mapping) and set(value) == {"$decimal"}:
            value = value.get("$decimal")
        decimal_value = _decimal(value)
        if decimal_value is None:
            return None, "INVALID"
        parsed.append(decimal_value)
    if any(value != parsed[0] for value in parsed[1:]):
        return None, "INVALID"
    return parsed[0], "VALID"


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalised = value.normalize()
    return "0" if not normalised else format(normalised, "f")


def _identifier(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    identifier = value.strip()
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", identifier):
        return identifier
    return None


def _canonical_underlying(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    symbols: list[str] = []
    for field in ("underlying", "symbol"):
        if field not in value:
            continue
        normalized = _symbols(value.get(field))
        if len(normalized) != 1:
            return None
        symbols.append(normalized[0])
    if not symbols or any(symbol != symbols[0] for symbol in symbols[1:]):
        return None
    return symbols[0]


def _underlying_repetitions_match(
    candidate: Mapping[str, object],
    *,
    canonical_underlying: str,
) -> bool:
    for source in (candidate, candidate.get("proposal_body")):
        if not isinstance(source, Mapping):
            continue
        for field in ("underlying", "symbol"):
            if field in source and _symbols(source.get(field)) != [canonical_underlying]:
                return False
    return True


def _symbols(value: object) -> list[str]:
    raw_values = (
        value
        if isinstance(value, Sequence) and not isinstance(value, str)
        else [value]
    )
    symbols: list[str] = []
    for raw_symbol in raw_values:
        if not isinstance(raw_symbol, str):
            continue
        symbol = raw_symbol.strip().upper()
        if re.fullmatch(r"[A-Z][A-Z0-9.\-/]{0,14}", symbol) and symbol not in symbols:
            symbols.append(symbol)
    return symbols[:12]
