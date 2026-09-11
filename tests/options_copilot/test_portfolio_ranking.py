from datetime import datetime, timezone
from decimal import Decimal

from options_copilot.analytics.scenarios import ResolvedPolicy
from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.ranking.portfolio import PortfolioAction, PortfolioRanker
from options_copilot.risk.authorization import RiskAuthorityTier, RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash, freeze_json


NOW = datetime(2026, 8, 4, 12, tzinfo=timezone.utc)
POLICY_HASH = "1" * 64
POLICY_MARKER_HASH = "2" * 64
COST_HASH = "3" * 64
RISK_CONTRACT_HASH = "4" * 64
EVIDENCE = {"broker": "5" * 64, "liquidity": "6" * 64}


def _candidate_body(candidate_id: str) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "structure": "DEBIT_VERTICAL",
        "legs": (
            {
                "con_id": 101,
                "side": "LONG",
                "ratio": 1,
                "bid": "2.00",
                "ask": "2.10",
                "observed_at": NOW.isoformat(),
            },
            {
                "con_id": 102,
                "side": "SHORT",
                "ratio": 1,
                "bid": "1.00",
                "ask": "1.10",
                "observed_at": NOW.isoformat(),
            },
        ),
        "max_loss_usd": "100",
        "liquidity_score": "5",
    }


def _policy() -> ResolvedPolicy:
    return ResolvedPolicy(
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        effective_at=NOW,
        payload=freeze_json({"name": "initial"}),
        calibration_provenance=freeze_json({"source": "locked"}),
    )


def _normal_authority() -> RiskTierAuthority:
    return RiskTierAuthority.normal(RISK_CONTRACT_HASH)


def _a_grade_authority(row: dict[str, object]) -> RiskTierAuthority:
    return RiskTierAuthority(
        version="v2",
        tier=RiskAuthorityTier.A_GRADE,
        risk_contract_hash=RISK_CONTRACT_HASH,
        risk_authority_marker_hash="7" * 64,
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        proposal_hash=str(row["proposal_hash"]),
        candidate_hash=str(row["candidate_hash"]),
        execution_cost_version="v1",
        execution_cost_hash=COST_HASH,
        ranking_basis_hash=str(row["ranking_basis_hash"]),
        a_grade_approved=True,
        actor="human:owner",
        signed_at=NOW,
        expires_at=NOW.replace(hour=13),
    )


def _authority_row(candidate_id: str, risk_fraction: str) -> dict[str, object]:
    body = _candidate_body(candidate_id)
    basis = build_ranking_basis(
        candidate_body=body,
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )
    return {
        "candidate_id": candidate_id,
        "proposal_hash": basis.proposal_hash,
        "proposal_body": basis.proposal_body,
        "candidate_hash": basis.candidate_hash,
        "candidate_body": basis.candidate_body,
        "ranking_basis_hash": basis.ranking_basis_hash,
        "eligible": True,
        "after_cost_expected_value": Decimal("12"),
        "liquidity_score": Decimal("5"),
        "max_loss": Decimal("100"),
        "risk_fraction": Decimal(risk_fraction),
        "structure": "DEBIT_VERTICAL",
        "underlying": "SPY",
        "thesis": "UP",
        "cost_version": "v1",
        "cost_hash": COST_HASH,
        "evidence_inputs": EVIDENCE,
        "open_combinations": 0,
    }


def test_portfolio_ranker_hard_filters_and_is_stable() -> None:
    rows = (
        {"candidate_id": "z", "eligible": True, "after_cost_expected_value": Decimal("12"), "liquidity_score": Decimal("5"), "max_loss": Decimal("100"), "structure": "DEBIT_VERTICAL", "underlying": "SPY", "thesis": "UP"},
        {"candidate_id": "a", "eligible": True, "after_cost_expected_value": Decimal("12"), "liquidity_score": Decimal("5"), "max_loss": Decimal("100"), "structure": "DEBIT_VERTICAL", "underlying": "QQQ", "thesis": "UP"},
        {"candidate_id": "bad", "eligible": False, "after_cost_expected_value": Decimal("999"), "liquidity_score": Decimal("99"), "max_loss": Decimal("1")},
    )
    ranked = PortfolioRanker().rank(reversed(rows))
    assert ranked.action is PortfolioAction.TRADE
    assert [row.candidate_id for row in ranked.candidates] == ["a", "z"]
    assert all(row.candidate_id != "bad" for row in ranked.candidates)


def test_authority_bound_basis_precedes_marker_and_normal_ten_percent_ranks() -> None:
    ranked = PortfolioRanker().rank(
        (_authority_row("normal", "0.10"),),
        current_policy=_policy(),
        risk_authority=_normal_authority(),
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )

    assert ranked.action is PortfolioAction.TRADE
    assert len(ranked.candidates) == 1
    row = ranked.candidates[0]
    assert row.authorizable is True
    assert row.authority_status == "NORMAL"
    assert row.risk_authority_marker_hash == _normal_authority().marker_hash
    expected = build_ranking_basis(
        candidate_body=_candidate_body("normal"),
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )
    assert row.ranking_basis_hash == expected.ranking_basis_hash
    assert row.candidate_hash == expected.candidate_hash
    assert row.proposal_hash == expected.proposal_hash
    assert row.proposal_hash != row.candidate_hash


def test_unsigned_ten_point_zero_one_is_governance_only_and_never_top_three() -> None:
    ranked = PortfolioRanker().rank(
        (_authority_row("pending", "0.1001"), _authority_row("normal", "0.10")),
        current_policy=_policy(),
        risk_authority=_normal_authority(),
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )

    assert [row.candidate_id for row in ranked.candidates] == ["normal"]
    assert [row.candidate_id for row in ranked.governance_evidence] == ["pending"]
    pending = ranked.governance_evidence[0]
    assert pending.rank is None
    assert pending.authorizable is False
    assert pending.authority_status == "A_GRADE_PENDING"


def test_signed_a_grade_can_rank_through_fifteen_but_never_above() -> None:
    fifteen = _authority_row("fifteen", "0.15")
    ranked = PortfolioRanker().rank(
        (fifteen, _authority_row("too-high", "0.1501")),
        current_policy=_policy(),
        risk_authority=_a_grade_authority(fifteen),
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )

    assert [row.candidate_id for row in ranked.candidates] == ["fifteen"]
    assert ranked.candidates[0].authority_status == "A_GRADE"
    assert ranked.candidates[0].risk_authority_marker_hash == "7" * 64
    assert all(row.candidate_id != "too-high" for row in ranked.candidates)


def test_signed_a_grade_unlocks_only_the_exact_bound_candidate() -> None:
    bound = _authority_row("bound", "0.15")
    other = _authority_row("other", "0.15")
    other["after_cost_expected_value"] = Decimal("20")

    ranked = PortfolioRanker().rank(
        (other, bound),
        current_policy=_policy(),
        risk_authority=_a_grade_authority(bound),
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )

    assert [row.candidate_id for row in ranked.candidates] == ["bound"]
    assert ranked.candidates[0].authority_status == "A_GRADE"
    assert [row.candidate_id for row in ranked.governance_evidence] == ["other"]
    assert ranked.governance_evidence[0].authority_status == "A_GRADE_PENDING"


def test_strict_ranker_rejects_reused_hash_after_leg_quote_tamper() -> None:
    row = _authority_row("tampered", "0.10")
    body = dict(row["candidate_body"])
    legs = [dict(item) for item in body["legs"]]
    legs[0]["ask"] = "9.99"
    row["candidate_body"] = {**body, "legs": legs}

    ranked = PortfolioRanker().rank(
        (row,),
        current_policy=_policy(),
        risk_authority=_normal_authority(),
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )

    assert ranked.action is PortfolioAction.NO_TRADE
    assert "CANDIDATE_OR_AUTHORITY_BINDING_INVALID" in ranked.rejections


def test_strict_ranker_rejects_candidate_evidence_override() -> None:
    row = _authority_row("override", "0.10")
    row["evidence_inputs"] = {"broker": "f" * 64}

    ranked = PortfolioRanker().rank(
        (row,),
        current_policy=_policy(),
        risk_authority=_normal_authority(),
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=EVIDENCE,
    )

    assert ranked.action is PortfolioAction.NO_TRADE
    assert "CANDIDATE_EVIDENCE_MISMATCH" in ranked.rejections
