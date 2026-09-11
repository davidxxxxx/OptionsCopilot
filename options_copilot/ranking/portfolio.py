"""Deterministic, hard-gated Top-10 portfolio ranking.

The ranker is deliberately pure.  It receives one already-resolved policy and
risk-authority state from the decision boundary, hard-filters candidates, and
then computes the pre-authority ``ranking_basis_hash``.  The basis excludes the
risk-authority marker to avoid the P9 A-grade marker signing its own hash.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.storage.canonical import canonical_hash, freeze_json


ZERO = Decimal("0")
NORMAL_RISK_CEILING = Decimal("0.10")
A_GRADE_RISK_CEILING = Decimal("0.15")


class PortfolioAction(str, Enum):
    TRADE = "TRADE"
    NO_TRADE = "NO_TRADE"


@dataclass(frozen=True, slots=True)
class RankedPortfolioCandidate:
    candidate_id: str
    candidate_hash: str
    rank: int | None
    score: Decimal
    after_cost_expected_value: Decimal
    liquidity_score: Decimal
    max_loss: Decimal
    structure_priority: int
    score_components: object
    candidate: object
    proposal_hash: str | None = None
    ranking_basis_hash: str | None = None
    authority_status: str = "NORMAL"
    authorizable: bool = True
    current_policy_version: str | None = None
    current_policy_hash: str | None = None
    policy_authority_marker_hash: str | None = None
    cost_version: str | None = None
    cost_hash: str | None = None
    risk_contract_hash: str | None = None
    risk_authority_version: str | None = None
    risk_authority_marker_hash: str | None = None
    evidence_inputs: object = None
    candidate_body: object = None
    proposal_body: object = None


@dataclass(frozen=True, slots=True)
class PortfolioRanking:
    action: PortfolioAction
    candidates: tuple[RankedPortfolioCandidate, ...]
    rejections: tuple[str, ...]
    governance_evidence: tuple[RankedPortfolioCandidate, ...] = ()


class PortfolioRanker:
    """Hard-filter first, then score only current-authority candidates."""

    _priority = {
        "DEBIT_VERTICAL": 7,
        "BUTTERFLY": 6,
        "CREDIT_VERTICAL": 5,
        "IRON_CONDOR": 4,
        "CALENDAR": 3,
        "DIAGONAL": 2,
        "LONG_OPTION": 1,
    }

    def rank(
        self,
        candidates: Iterable[object],
        limit: int = 10,
        *,
        current_policy: object | None = None,
        risk_authority: object | None = None,
        cost_version: str | None = None,
        cost_hash: str | None = None,
        risk_contract_hash: str | None = None,
        evidence_inputs: object | None = None,
    ) -> PortfolioRanking:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 10:
            raise ValueError("limit must be between 1 and 10")

        authority = _authority_context(
            current_policy=current_policy,
            risk_authority=risk_authority,
            cost_version=cost_version,
            cost_hash=cost_hash,
            risk_contract_hash=risk_contract_hash,
            evidence_inputs=evidence_inputs,
        )
        strict = current_policy is not None or risk_authority is not None
        eligible: list[tuple[Any, ...]] = []
        pending: list[tuple[Any, ...]] = []
        rejected: list[str] = []
        seen: set[str] = set()

        for raw in candidates:
            doc = _mapping(raw)
            candidate_id = str(doc.get("candidate_id", "")).strip()
            reason = self._rejection(doc)
            if not candidate_id or candidate_id in seen:
                reason = reason or "DUPLICATE_OR_MISSING_CANDIDATE_ID"
            seen.add(candidate_id)
            if reason:
                rejected.append(reason)
                continue

            risk_fraction = _risk_fraction(doc)
            if strict and risk_fraction is None:
                rejected.append("UNBOUND_RISK_FRACTION")
                continue
            if risk_fraction is not None and risk_fraction > A_GRADE_RISK_CEILING:
                rejected.append("RISK_ABOVE_A_GRADE_CEILING")
                continue
            if _open_combinations(doc) >= 1:
                rejected.append("MAX_OPEN_COMBINATIONS")
                continue

            ev = _decimal(doc["after_cost_expected_value"])
            liquidity = _decimal(doc.get("liquidity_score", ZERO))
            loss = _decimal(doc["max_loss"])
            assert ev is not None and liquidity is not None and loss is not None
            structure = str(doc.get("structure", "")).upper()
            priority = int(
                doc.get("structure_priority", self._priority.get(structure, 0))
            )
            supporting = _capped_supporting(doc.get("supporting_bonus", ZERO))
            joint_score, joint_components = _trusted_joint_score(doc, authority)
            if doc.get("joint_score") is not None and joint_score is None:
                rejected.append("JOINT_RANKING_BINDING_MISMATCH")
                continue
            if joint_score is None:
                components = freeze_json(
                    {
                        "after_cost_expected_value": ev,
                        "liquidity": liquidity,
                        "max_loss": loss,
                        "structure_priority": priority,
                        "supporting_bonus": supporting,
                    }
                )
                score = ev + liquidity + Decimal(priority) + supporting
            else:
                components = joint_components
                score = joint_score
            row_authority = dict(authority)
            candidate_body: object = doc.get("candidate_body")
            proposal_body: object = doc.get("proposal_body")
            if strict:
                if not isinstance(candidate_body, Mapping) or not candidate_body:
                    rejected.append("UNBOUND_CANDIDATE_BODY")
                    continue
                if str(candidate_body.get("candidate_id", "")).strip() != candidate_id:
                    rejected.append("CANDIDATE_ID_BODY_MISMATCH")
                    continue
                if proposal_body is not None and not isinstance(proposal_body, Mapping):
                    rejected.append("INVALID_PROPOSAL_BODY")
                    continue
                supplied_candidate_hash = doc.get("candidate_hash")
                supplied_proposal_hash = doc.get("proposal_hash")
                if supplied_candidate_hash is not None and _valid_hash(supplied_candidate_hash) is None:
                    rejected.append("INVALID_CANDIDATE_HASH")
                    continue
                if supplied_proposal_hash is not None and _valid_hash(supplied_proposal_hash) is None:
                    rejected.append("INVALID_PROPOSAL_HASH")
                    continue
                trusted_evidence = authority.get("evidence_inputs")
                supplied_evidence = doc.get("evidence_inputs")
                try:
                    if supplied_evidence is not None and freeze_json(supplied_evidence) != trusted_evidence:
                        rejected.append("CANDIDATE_EVIDENCE_MISMATCH")
                        continue
                    basis = build_ranking_basis(
                        candidate_body=candidate_body,
                        proposal_body=proposal_body,
                        candidate_hash=_valid_hash(supplied_candidate_hash),
                        proposal_hash=_valid_hash(supplied_proposal_hash),
                        current_policy_version=str(authority.get("current_policy_version") or ""),
                        current_policy_hash=str(authority.get("current_policy_hash") or ""),
                        policy_authority_marker_hash=str(authority.get("policy_authority_marker_hash") or ""),
                        cost_version=str(authority.get("cost_version") or ""),
                        cost_hash=str(authority.get("cost_hash") or ""),
                        risk_contract_hash=str(authority.get("risk_contract_hash") or ""),
                        evidence_inputs=trusted_evidence if isinstance(trusted_evidence, Mapping) else {},
                    )
                except (TypeError, ValueError):
                    rejected.append("CANDIDATE_OR_AUTHORITY_BINDING_INVALID")
                    continue
                candidate_hash = basis.candidate_hash
                proposal_hash = basis.proposal_hash
                candidate_body = basis.candidate_body
                proposal_body = basis.proposal_body
                row_authority["evidence_inputs"] = freeze_json(basis.evidence_inputs)
                basis_hash = basis.ranking_basis_hash
            else:
                candidate_hash = _valid_hash(doc.get("candidate_hash")) or canonical_hash(
                    {
                        "candidate_id": candidate_id,
                        "components": components,
                        "policy_hash": doc.get("policy_hash"),
                        "cost_hash": doc.get("cost_hash"),
                    }
                )
                proposal_hash = _valid_hash(doc.get("proposal_hash")) or candidate_hash
                row_evidence = doc.get("evidence_inputs", authority.get("evidence_inputs"))
                row_authority["evidence_inputs"] = freeze_json(row_evidence or {})
                basis_hash = _ranking_basis_hash(
                    proposal_hash=proposal_hash,
                    candidate_hash=candidate_hash,
                    authority=row_authority,
                    strict=False,
                )

            a_grade_required = bool(
                risk_fraction is not None and risk_fraction > NORMAL_RISK_CEILING
            )
            a_grade_current = _a_grade_current(
                authority,
                proposal_hash=proposal_hash,
                candidate_hash=candidate_hash,
                ranking_basis_hash=basis_hash,
            )
            status = (
                "A_GRADE"
                if a_grade_required and a_grade_current
                else "A_GRADE_PENDING"
                if a_grade_required
                else "NORMAL"
            )
            authorizable = status != "A_GRADE_PENDING"
            item = (
                score,
                ev,
                loss,
                priority,
                candidate_id,
                candidate_hash,
                components,
                raw,
                doc,
                proposal_hash,
                basis_hash,
                status,
                authorizable,
                row_authority,
                candidate_body,
                proposal_body,
                joint_score is not None,
            )
            (eligible if authorizable else pending).append(item)

        ordered = sorted(
            eligible,
            key=lambda item: (
                -(item[0] if item[16] else item[1]),
                item[2],
                -item[3],
                item[4],
            ),
        )
        selected = self._diverse(ordered, limit)
        results = tuple(
            _ranked(item, rank=index) for index, item in enumerate(selected, start=1)
        )
        governance = tuple(
            _ranked(item, rank=None)
            for item in sorted(
                pending,
                key=lambda value: (
                    -(value[0] if value[16] else value[1]),
                    value[2],
                    value[4],
                ),
            )
        )
        return PortfolioRanking(
            PortfolioAction.TRADE if results else PortfolioAction.NO_TRADE,
            results,
            tuple(sorted(set(rejected))) if not results else tuple(sorted(set(rejected))),
            governance,
        )

    def _rejection(self, doc: Mapping[str, Any]) -> str | None:
        if not bool(doc.get("eligible", False)):
            return "HARD_GATES_NOT_PASSED"
        for name in ("after_cost_expected_value", "max_loss"):
            value = _decimal(doc.get(name))
            if value is None or value <= ZERO:
                return (
                    "NON_POSITIVE_AFTER_COST_EV"
                    if name.startswith("after")
                    else "UNKNOWN_MAX_LOSS"
                )
        if bool(doc.get("conflicted", False)) or bool(doc.get("stale", False)):
            return "STALE_OR_CONFLICTED"
        return None

    def _diverse(self, ordered: list[tuple[Any, ...]], limit: int) -> list[tuple[Any, ...]]:
        selected: list[tuple[Any, ...]] = []
        remaining = list(ordered)
        while remaining and len(selected) < limit:
            used = {
                (str(item[8].get(key, "")), key)
                for item in selected
                for key in ("underlying", "thesis", "structure")
            }
            remaining.sort(
                key=lambda item: (
                    -sum(
                        (str(item[8].get(key, "")), key) not in used
                        for key in ("underlying", "thesis", "structure")
                    ),
                    -(item[0] if item[16] else item[1]),
                    item[2],
                    -item[3],
                    item[4],
                )
            )
            selected.append(remaining.pop(0))
        return selected


def _trusted_joint_score(
    candidate: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> tuple[Decimal | None, object]:
    supplied = candidate.get("joint_score")
    if supplied is None:
        return None, None
    score = _decimal(supplied)
    components = candidate.get("joint_score_components")
    row_hash = _valid_hash(candidate.get("joint_row_hash"))
    snapshot_hash = _valid_hash(candidate.get("joint_snapshot_hash"))
    candidate_hash = _valid_hash(candidate.get("joint_candidate_hash"))
    evidence = authority.get("evidence_inputs")
    if (
        score is None
        or score < ZERO
        or score > Decimal("100")
        or not isinstance(components, Mapping)
        or row_hash is None
        or snapshot_hash is None
        or candidate_hash is None
        or not isinstance(evidence, Mapping)
    ):
        return None, None
    snapshot = evidence.get("joint_ranking")
    if not isinstance(snapshot, Mapping) or snapshot.get("snapshot_hash") != snapshot_hash:
        return None, None
    snapshot_body = {key: value for key, value in snapshot.items() if key != "snapshot_hash"}
    if canonical_hash(snapshot_body) != snapshot_hash:
        return None, None
    rows = snapshot.get("executable")
    if not isinstance(rows, (list, tuple)):
        return None, None
    candidate_id = str(candidate.get("candidate_id", "")).strip()
    matching = tuple(
        row
        for row in rows
        if isinstance(row, Mapping) and row.get("candidate_id") == candidate_id
    )
    if len(matching) != 1:
        return None, None
    row = matching[0]
    row_body = {key: value for key, value in row.items() if key != "row_hash"}
    try:
        same_components = freeze_json(row.get("score_components")) == freeze_json(components)
    except (TypeError, ValueError):
        return None, None
    if (
        canonical_hash(row_body) != row_hash
        or row.get("row_hash") != row_hash
        or row.get("candidate_hash") != candidate_hash
        or _decimal(row.get("score")) != score
        or not same_components
        or row.get("disposition") != "EXECUTABLE_REVIEW"
        or row.get("reason_codes") not in ((), [])
    ):
        return None, None
    return score, freeze_json(components)


def _ranked(item: tuple[Any, ...], *, rank: int | None) -> RankedPortfolioCandidate:
    authority = item[13]
    return RankedPortfolioCandidate(
        candidate_id=item[4],
        candidate_hash=item[5],
        rank=rank,
        score=item[0],
        after_cost_expected_value=item[1],
        liquidity_score=_decimal(item[8].get("liquidity_score", ZERO)) or ZERO,
        max_loss=item[2],
        structure_priority=item[3],
        score_components=item[6],
        candidate=item[7],
        proposal_hash=item[9],
        ranking_basis_hash=item[10],
        authority_status=item[11],
        authorizable=item[12],
        current_policy_version=authority.get("current_policy_version"),
        current_policy_hash=authority.get("current_policy_hash"),
        policy_authority_marker_hash=authority.get("policy_authority_marker_hash"),
        cost_version=authority.get("cost_version"),
        cost_hash=authority.get("cost_hash"),
        risk_contract_hash=authority.get("risk_contract_hash"),
        risk_authority_version=authority.get("risk_authority_version"),
        risk_authority_marker_hash=authority.get("risk_authority_marker_hash"),
        evidence_inputs=authority.get("evidence_inputs"),
        candidate_body=item[14],
        proposal_body=item[15],
    )


def _authority_context(
    *,
    current_policy: object | None,
    risk_authority: object | None,
    cost_version: str | None,
    cost_hash: str | None,
    risk_contract_hash: str | None,
    evidence_inputs: object | None,
) -> dict[str, Any]:
    policy = _mapping(current_policy)
    risk = _mapping(risk_authority)
    resolved_risk_contract = risk_contract_hash or risk.get("risk_contract_hash")
    return {
        "current_policy_version": _text(policy.get("current_policy_version")),
        "current_policy_hash": _valid_hash(policy.get("current_policy_hash")),
        "policy_authority_marker_hash": _valid_hash(
            policy.get("policy_authority_marker_hash")
        ),
        "cost_version": _text(cost_version),
        "cost_hash": _valid_hash(cost_hash),
        "risk_contract_hash": _valid_hash(resolved_risk_contract),
        "risk_authority_version": _text(risk.get("version")),
        "risk_authority_marker_hash": _valid_hash(
            risk.get("risk_authority_marker_hash", risk.get("marker_hash"))
        ),
        "risk_tier": str(getattr(risk.get("tier"), "value", risk.get("tier", ""))),
        "a_grade_approved": risk.get("a_grade_approved") is True,
        "risk_current_policy_version": _text(risk.get("current_policy_version")),
        "risk_current_policy_hash": _valid_hash(risk.get("current_policy_hash")),
        "risk_policy_authority_marker_hash": _valid_hash(
            risk.get("policy_authority_marker_hash")
        ),
        "risk_contract_binding_hash": _valid_hash(risk.get("risk_contract_hash")),
        "risk_proposal_hash": _valid_hash(risk.get("proposal_hash")),
        "risk_candidate_hash": _valid_hash(risk.get("candidate_hash")),
        "risk_execution_cost_version": _text(risk.get("execution_cost_version")),
        "risk_execution_cost_hash": _valid_hash(risk.get("execution_cost_hash")),
        "risk_ranking_basis_hash": _valid_hash(risk.get("ranking_basis_hash")),
        "evidence_inputs": freeze_json(evidence_inputs or {}),
    }


def _ranking_basis_hash(
    *,
    proposal_hash: str,
    candidate_hash: str,
    authority: Mapping[str, Any],
    strict: bool,
) -> str | None:
    required_hashes = (
        "current_policy_hash",
        "policy_authority_marker_hash",
        "cost_hash",
        "risk_contract_hash",
    )
    if strict and (
        not authority.get("current_policy_version")
        or not authority.get("cost_version")
        or any(not _valid_hash(authority.get(name)) for name in required_hashes)
    ):
        return None
    if not strict:
        return canonical_hash(
            {
                "schema": "options_copilot.ranking_basis.compat.v1",
                "proposal_hash": proposal_hash,
                "candidate_hash": candidate_hash,
            }
        )
    return canonical_hash(
        {
            "schema": "options_copilot.ranking_basis.v1",
            "proposal_hash": proposal_hash,
            "candidate_hash": candidate_hash,
            "current_policy_version": authority["current_policy_version"],
            "current_policy_hash": authority["current_policy_hash"],
            "policy_authority_marker_hash": authority[
                "policy_authority_marker_hash"
            ],
            "cost_version": authority["cost_version"],
            "cost_hash": authority["cost_hash"],
            "risk_contract_hash": authority["risk_contract_hash"],
            "evidence_inputs": authority["evidence_inputs"],
        }
    )


def _a_grade_current(
    authority: Mapping[str, Any],
    *,
    proposal_hash: str,
    candidate_hash: str,
    ranking_basis_hash: str | None,
) -> bool:
    return bool(
        authority.get("a_grade_approved") is True
        and authority.get("risk_tier") == "A_GRADE"
        and _valid_hash(authority.get("risk_authority_marker_hash"))
        and authority.get("risk_current_policy_version")
        == authority.get("current_policy_version")
        and authority.get("risk_current_policy_hash")
        == authority.get("current_policy_hash")
        and authority.get("risk_policy_authority_marker_hash")
        == authority.get("policy_authority_marker_hash")
        and authority.get("risk_contract_binding_hash")
        == authority.get("risk_contract_hash")
        and authority.get("risk_proposal_hash") == proposal_hash
        and authority.get("risk_candidate_hash") == candidate_hash
        and authority.get("risk_execution_cost_version")
        == authority.get("cost_version")
        and authority.get("risk_execution_cost_hash") == authority.get("cost_hash")
        and authority.get("risk_ranking_basis_hash") == ranking_basis_hash
    )


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    return vars(value) if hasattr(value, "__dict__") else {}


def _decimal(value: object) -> Decimal | None:
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _risk_fraction(doc: Mapping[str, Any]) -> Decimal | None:
    direct = _decimal(doc.get("risk_fraction"))
    if direct is not None:
        return direct
    risk = _mapping(doc.get("risk", {}))
    nested = _decimal(risk.get("risk_fraction"))
    if nested is not None:
        return nested
    max_loss = _decimal(doc.get("max_loss"))
    strategy_nav = _decimal(doc.get("strategy_nav", doc.get("strategy_nav_usd")))
    if max_loss is None or strategy_nav is None or strategy_nav <= ZERO:
        return None
    return max_loss / strategy_nav


def _open_combinations(doc: Mapping[str, Any]) -> int:
    value = doc.get("open_combinations", 0)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 1


def _valid_hash(value: object) -> str | None:
    return (
        value
        if isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
        else None
    )


def _text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _capped_supporting(value: object) -> Decimal:
    decimal = _decimal(value) or ZERO
    return max(Decimal("-3"), min(Decimal("3"), decimal))


__all__ = [
    "A_GRADE_RISK_CEILING",
    "NORMAL_RISK_CEILING",
    "PortfolioAction",
    "PortfolioRanker",
    "PortfolioRanking",
    "RankedPortfolioCandidate",
]
