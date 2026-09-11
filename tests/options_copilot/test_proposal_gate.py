from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from options_copilot.domain import (
    CandidateRiskTier,
    OptionRight,
    PositionSide,
    StrategyCandidate,
)
from options_copilot.approval import ProposalApprovalStore, proposal_hashes
from options_copilot.market import UsOptionsSessionCalendar
from options_copilot.proposals import ValidatedProposal, validate_proposal
from options_copilot.performance.nav_ledger import (
    NavAttribution,
    NavEventKind,
    StrategyNavLedger,
)
from options_copilot.risk import (
    DTE_EXCEPTION_SCHEMA,
    DteEntryExceptionAuthority,
    OptionTimePolicy,
    P2ManagementTransitionProof,
    RiskAssessment,
    RiskEngine,
    RiskRejection,
)
from options_copilot.risk.authorization import RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash, canonical_json


NOW = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
SNAPSHOT_ID = "ibkr-quotes-20260803T150000Z"
CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "strategy_nav_contract.v1.json"
)
REPORT_HASH = "1" * 64
POLICY_HASH = "2" * 64
DATASET_HASH = "3" * 64
INDEPENDENCE_HASH = "4" * 64
PROPOSAL_AUTHORITY_HASH = "5" * 64
CANDIDATE_AUTHORITY_HASH = "6" * 64
POLICY_AUTHORITY_MARKER_HASH = "7" * 64
EXECUTION_COST_HASH = "8" * 64
RANKING_BASIS_HASH = "9" * 64
FIXTURE_SECRET = b"non-production-proposal-gate-p9-fixture"


class _FixtureSignatureVerifier:
    trust_domain = "TEST_ONLY"

    def verify(
        self,
        *,
        signer_key_id: str,
        signature_algorithm: str,
        message: bytes,
        signature: str,
    ) -> bool:
        return (
            signer_key_id == "test-only:proposal-gate"
            and signature_algorithm == "TEST_ONLY_SHA256"
            and signature == hashlib.sha256(FIXTURE_SECRET + message).hexdigest()
        )


FIXTURE_VERIFIER = _FixtureSignatureVerifier()


def _proposal() -> dict[str, object]:
    quote_time = (NOW - timedelta(seconds=2)).isoformat()
    return {
        "proposal_id": "spy-call-spread-1",
        "rank": 1,
        "eligible_to_send": True,
        "underlying": "SPY",
        "expiration": "2026-08-21",
        "quote_snapshot_id": SNAPSHOT_ID,
        "expected_value_usd": "20.00",
        "estimated_commissions": "2.00",
        "estimated_slippage": "1.00",
        "terminal_scenarios": [
            {"terminal_underlying_price": "100", "probability": "0.734"},
            {"terminal_underlying_price": "105", "probability": "0.266"},
        ],
        "risk": {"maximum_loss_usd": "113.00"},
        "legs": [
            {
                "contract_id_ex": "101@SMART",
                "underlying": "SPY",
                "security_type": "OPT",
                "expiration": "2026-08-21",
                "strike": "100",
                "right": "CALL",
                "side": "BUY",
                "quantity": 1,
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
                "bid": 1.90,
                "ask": 2.00,
                "quote_time": quote_time,
                "quote_snapshot_id": SNAPSHOT_ID,
            },
            {
                "contract_id_ex": "102@SMART",
                "underlying": "SPY",
                "security_type": "OPT",
                "expiration": "2026-08-21",
                "strike": 105,
                "right": "CALL",
                "side": "SELL",
                "quantity": 1,
                "multiplier": 100,
                "currency": "USD",
                "exchange": "SMART",
                "bid": 0.90,
                "ask": 1.00,
                "quote_time": quote_time,
                "quote_snapshot_id": SNAPSHOT_ID,
            },
        ],
    }


def _validate(
    proposal: dict[str, object] | None = None,
    *,
    account_equity: str = "2000",
    open_combinations: int = 0,
    a_grade_unlocked: bool = False,
    quote_fresh_seconds: int | float | str | Decimal = 5,
    now: datetime = NOW,
    risk_engine: RiskEngine | None = None,
    dte_exception_authority: DteEntryExceptionAuthority | None = None,
) -> ValidatedProposal:
    return validate_proposal(
        _proposal() if proposal is None else proposal,
        account_equity=Decimal(account_equity),
        open_combinations=open_combinations,
        now=now,
        quote_fresh_seconds=quote_fresh_seconds,
        expected_quote_snapshot_id=SNAPSHOT_ID,
        a_grade_unlocked=a_grade_unlocked,
        risk_engine=risk_engine,
        dte_exception_authority=dte_exception_authority,
    )


def _strategy_nav(
    tmp_path: Path,
    *,
    strategy_nav: str = "10000",
    observed_nlv: str = "10000",
):
    target = Decimal(strategy_nav)
    adjustment = target - Decimal("2012.44")
    event_kind = NavEventKind.DEPOSIT if adjustment >= 0 else NavEventKind.WITHDRAWAL
    with StrategyNavLedger(
        tmp_path / "strategy-nav.sqlite3",
        contract=CONTRACT_PATH,
        clock=lambda: NOW,
    ) as ledger:
        ledger.append_flow(
            event_kind=event_kind,
            broker_event_identifier="p1-risk-boundary-capital",
            effective_at=NOW - timedelta(minutes=2),
            amount=abs(adjustment),
            attribution=NavAttribution.STRATEGY,
        )
        return ledger.snapshot(
            asof=NOW,
            observed_account_nlv=Decimal(observed_nlv),
        )


def _signed_a_grade_marker(
    risk_contract_hash: str,
    **overrides: object,
) -> dict[str, object]:
    marker: dict[str, object] = {
        "schema": "options_copilot.learning.a_grade_authority.v1",
        "phase": "P9",
        "decision": "APPROVE_A_GRADE",
        "version": "v1",
        "sequence": 1,
        "append_only": True,
        "proposal_hash": PROPOSAL_AUTHORITY_HASH,
        "candidate_hash": CANDIDATE_AUTHORITY_HASH,
        "current_policy_version": "v2",
        "current_policy_hash": POLICY_HASH,
        "policy_authority_marker_hash": POLICY_AUTHORITY_MARKER_HASH,
        "execution_cost_version": "v1",
        "execution_cost_hash": EXECUTION_COST_HASH,
        "ranking_basis_hash": RANKING_BASIS_HASH,
        "evaluation_report_hash": REPORT_HASH,
        "reference_dataset_hash": DATASET_HASH,
        "independence_hash": INDEPENDENCE_HASH,
        "risk_contract_hash": risk_contract_hash,
        "actor": "human:test-fixture",
        "signed_at": (NOW - timedelta(minutes=1)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "previous_authority_hash": None,
        "revoked": False,
        "rolled_back": False,
        "signer_key_id": "test-only:proposal-gate",
        "signature_algorithm": "TEST_ONLY_SHA256",
    }
    marker.update(overrides)
    marker["governance_signature"] = hashlib.sha256(
        FIXTURE_SECRET + canonical_json(marker).encode("utf-8")
    ).hexdigest()
    marker["content_hash"] = canonical_hash(marker)
    return marker


def _risk_authority(nav, marker: dict[str, object] | None = None):
    return RiskTierAuthority.resolve(
        risk_contract_hash=nav.contract_hash,
        marker=marker,
        asof=NOW,
        expected_evaluation_report_hash=REPORT_HASH,
        expected_policy_hash=POLICY_HASH,
        expected_reference_dataset_hash=DATASET_HASH,
        expected_independence_hash=INDEPENDENCE_HASH,
        expected_proposal_hash=PROPOSAL_AUTHORITY_HASH,
        expected_candidate_hash=CANDIDATE_AUTHORITY_HASH,
        expected_current_policy_version="v2",
        expected_policy_authority_marker_hash=POLICY_AUTHORITY_MARKER_HASH,
        expected_execution_cost_version="v1",
        expected_execution_cost_hash=EXECUTION_COST_HASH,
        expected_ranking_basis_hash=RANKING_BASIS_HASH,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )


def _production_engine(nav, authority=None) -> RiskEngine:
    return RiskEngine(
        strategy_nav=nav,
        expected_contract_hash=nav.contract_hash,
        expected_ledger_head_hash=nav.ledger_head_hash,
        risk_tier_authority=authority or _risk_authority(nav),
    )


def _signed_dte_exception(
    policy: OptionTimePolicy,
    *,
    proposal_id: str,
    expiration: date,
    now: datetime,
    **overrides: object,
) -> DteEntryExceptionAuthority:
    marker: dict[str, object] = {
        "schema": DTE_EXCEPTION_SCHEMA,
        "version": "v1",
        "sequence": 1,
        "append_only": True,
        "decision": "APPROVE_7_13_DTE_ENTRY",
        "actor": "human:xujie",
        "reason": "event-defined risk exception",
        "proposal_id": proposal_id,
        "expiration": expiration.isoformat(),
        "policy_hash": policy.policy_hash,
        "approved_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
        "revoked": False,
        "previous_marker_hash": None,
    }
    marker.update(overrides)
    marker["governance_signature"] = canonical_hash(marker)
    marker["content_hash"] = canonical_hash(marker)
    return DteEntryExceptionAuthority.resolve(
        marker=marker,
        expected_policy_hash=policy.policy_hash,
        expected_proposal_id=proposal_id,
        expected_expiration=expiration,
        asof=now,
    )


def _ready_calendar(now: datetime):
    market_date = now.astimezone(ZoneInfo("America/New_York")).date()
    day = market_date.strftime("%Y%m%d")
    return UsOptionsSessionCalendar().normalize(
        liquid_hours=f"{day}:0930-{day}:1600",
        trading_hours=f"{day}:0930-{day}:1600",
        timezone_id="America/New_York",
        observed_at=now,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=now,
    )


def _candidate_with_max_loss(
    maximum_loss: str,
    *,
    tier: CandidateRiskTier = CandidateRiskTier.NORMAL,
) -> StrategyCandidate:
    base = _validate().candidate
    desired = Decimal(maximum_loss)
    return replace(
        base,
        estimated_commissions=base.estimated_commissions
        + desired
        - Decimal("113"),
        risk_tier=tier,
    )


def test_valid_spread_is_rebuilt_reassessed_and_canonicalized() -> None:
    result = _validate()

    assert isinstance(result, ValidatedProposal)
    assert isinstance(result.candidate, StrategyCandidate)
    assert isinstance(result.risk_assessment, RiskAssessment)
    assert result.risk_assessment.approved is True
    assert result.maximum_loss_usd == Decimal("113")
    assert result.executable_cost_usd == Decimal("110")
    assert result.all_in_executable_cost_usd == Decimal("113")
    assert result.expected_value_usd == Decimal("20")
    assert result.expected_value_before_costs_usd == Decimal("23")
    assert result.candidate.estimated_execution_costs == Decimal("3")
    assert result.candidate.leg_quotes[0].bid == Decimal("1.9")
    assert result.candidate.leg_quotes[0].ask == Decimal("2.0")
    assert result.candidate.legs[0].contract.right is OptionRight.CALL
    assert result.candidate.legs[0].side is PositionSide.LONG
    assert result.candidate.legs[1].side is PositionSide.SHORT
    assert result.canonical_proposal["reference_cost_usd"] == "110"
    assert result.canonical_proposal["risk"]["maximum_loss_usd"] == "113"
    assert len(result.proposal_hash) == 64
    assert result.canonical_hash == result.proposal_hash
    assert result.normalized_legs[0]["contract_id_ex"] == "101@SMART"
    assert result.to_dict()["proposal_id"] == "spy-call-spread-1"
    with pytest.raises(TypeError):
        result.canonical_proposal["rank"] = 2


def test_naked_short_call_is_permanently_rejected() -> None:
    proposal = _proposal()
    proposal["legs"] = [proposal["legs"][1]]

    with pytest.raises(ValueError, match="NAKED_SHORT_CALL|UNBOUNDED_MAX_LOSS"):
        _validate(proposal)


def test_forged_declared_maximum_loss_is_rejected() -> None:
    proposal = _proposal()
    proposal["risk"]["maximum_loss_usd"] = "50"

    with pytest.raises(ValueError, match="does not match recomputed maximum loss"):
        _validate(proposal)


def test_missing_scenarios_cannot_hide_forged_positive_ev() -> None:
    proposal = _proposal()
    proposal.pop("terminal_scenarios")
    proposal["expected_value_usd"] = "999999"

    with pytest.raises(ValueError, match="terminal_scenarios is required"):
        _validate(proposal)


def test_declared_ev_must_match_recomputed_cost_after_scenario_ev() -> None:
    proposal = _proposal()
    proposal["expected_value_usd"] = "21"

    with pytest.raises(ValueError, match="cost-after scenario EV"):
        _validate(proposal)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p["legs"][0].pop("bid"), "bid is required"),
        (lambda p: p["legs"][0].pop("ask"), "ask is required"),
        (lambda p: p["legs"][0].pop("quote_time"), "quote_time is required"),
        (
            lambda p: p["legs"][0].__setitem__(
                "quote_time", (NOW - timedelta(seconds=6)).isoformat()
            ),
            "stale",
        ),
        (
            lambda p: p["legs"][0].__setitem__(
                "quote_time", (NOW + timedelta(microseconds=1)).isoformat()
            ),
            "future",
        ),
    ],
)
def test_missing_stale_and_future_leg_quotes_fail_closed(mutation, message: str) -> None:
    proposal = _proposal()
    mutation(proposal)

    with pytest.raises(ValueError, match=message):
        _validate(proposal, quote_fresh_seconds=10)


def test_bid_above_ask_is_rejected() -> None:
    proposal = _proposal()
    proposal["legs"][0]["bid"] = "2.01"

    with pytest.raises(ValueError, match="bid cannot exceed ask"):
        _validate(proposal)


@pytest.mark.parametrize("age_seconds", [0, 5])
def test_quote_freshness_includes_zero_and_five_second_boundaries(
    age_seconds: int,
) -> None:
    proposal = _proposal()
    for leg in proposal["legs"]:
        leg["quote_time"] = (NOW - timedelta(seconds=age_seconds)).isoformat()

    assert _validate(proposal).risk_assessment.approved is True


def test_quote_just_over_five_seconds_is_rejected() -> None:
    proposal = _proposal()
    proposal["legs"][0]["quote_time"] = (
        NOW - timedelta(seconds=5, milliseconds=1)
    ).isoformat()

    with pytest.raises(ValueError, match="stale"):
        _validate(proposal)


@pytest.mark.parametrize("replacement", ["proposal", "leg"])
def test_quote_snapshot_replacement_is_rejected(replacement: str) -> None:
    proposal = _proposal()
    if replacement == "proposal":
        proposal["quote_snapshot_id"] = "replacement-snapshot"
    else:
        proposal["legs"][1]["quote_snapshot_id"] = "replacement-snapshot"

    with pytest.raises(ValueError, match="snapshot replacement"):
        _validate(proposal)


def test_dte_below_seven_is_hard_rejected() -> None:
    proposal = _proposal()
    proposal["expiration"] = "2026-08-09"
    for leg in proposal["legs"]:
        leg["expiration"] = "2026-08-09"

    with pytest.raises(ValueError, match="6 DTE; permanent minimum is 7"):
        _validate(proposal)


def test_exactly_seven_dte_requires_signed_exception_on_et_date_basis() -> None:
    proposal = _proposal()
    # 2026-08-04 UTC is still 2026-08-03 in the US equity market.  Using the
    # caller's or UTC display date would incorrectly reduce this to six DTE.
    boundary_now = datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)
    proposal["expiration"] = "2026-08-10"
    for leg in proposal["legs"]:
        leg["expiration"] = "2026-08-10"
        leg["quote_time"] = (boundary_now - timedelta(seconds=2)).isoformat()

    with pytest.raises(ValueError, match="signed 7-13 DTE exception is required"):
        _validate(
            proposal,
            now=boundary_now.astimezone(timezone(timedelta(hours=8))),
        )

    policy = OptionTimePolicy()
    authority = _signed_dte_exception(
        policy,
        proposal_id="spy-call-spread-1",
        expiration=date(2026, 8, 10),
        now=boundary_now,
    )
    result = _validate(
        proposal,
        now=boundary_now.astimezone(timezone(timedelta(hours=8))),
        dte_exception_authority=authority,
    )
    assert result.canonical_proposal["dte"] == 7


@pytest.mark.parametrize(
    ("expiration", "dte", "allowed_without_exception"),
    [
        (date(2026, 8, 9), 6, False),
        (date(2026, 8, 10), 7, False),
        (date(2026, 8, 16), 13, False),
        (date(2026, 8, 17), 14, True),
        (date(2026, 9, 7), 35, True),
        (date(2026, 9, 8), 36, False),
    ],
)
def test_entry_dte_boundaries_are_exact(
    expiration: date,
    dte: int,
    allowed_without_exception: bool,
) -> None:
    policy = OptionTimePolicy()
    decision = policy.evaluate_dte(
        proposal_id="spy-call-spread-1",
        expiration=expiration,
        now=NOW,
    )

    assert decision.dte == dte
    assert decision.allowed is allowed_without_exception
    if 7 <= dte <= 13:
        authority = _signed_dte_exception(
            policy,
            proposal_id="spy-call-spread-1",
            expiration=expiration,
            now=NOW,
        )
        approved = policy.evaluate_dte(
            proposal_id="spy-call-spread-1",
            expiration=expiration,
            now=NOW,
            exception_authority=authority,
        )
        assert approved.allowed is True
        assert approved.exception_hash == authority.content_hash
    elif dte < 7:
        authority = _signed_dte_exception(
            policy,
            proposal_id="spy-call-spread-1",
            expiration=expiration,
            now=NOW,
        )
        still_rejected = policy.evaluate_dte(
            proposal_id="spy-call-spread-1",
            expiration=expiration,
            now=NOW,
            exception_authority=authority,
        )
        assert still_rejected.allowed is False
        assert "DTE_BELOW_PERMANENT_FLOOR" in still_rejected.reason_codes


def test_entry_time_decision_binds_calendar_et_date_utc_and_exception_hash() -> None:
    policy = OptionTimePolicy()
    calendar = _ready_calendar(NOW)
    expiration = date(2026, 8, 16)
    authority = _signed_dte_exception(
        policy,
        proposal_id="spy-call-spread-1",
        expiration=expiration,
        now=NOW,
    )

    decision = policy.evaluate_entry(
        proposal_id="spy-call-spread-1",
        expiration=expiration,
        now=NOW,
        calendar=calendar,
        exception_authority=authority,
    )

    assert decision.allowed is True
    assert decision.et_trading_date == date(2026, 8, 3)
    assert decision.evaluated_at_utc == NOW
    assert decision.calendar_hash == calendar.calendar_hash
    assert decision.exception_hash == authority.content_hash
    assert decision.policy_hash == policy.policy_hash
    assert decision.verify_hash() is True


def test_entry_rechecks_calendar_freshness_at_decision_time() -> None:
    policy = OptionTimePolicy()
    calendar = _ready_calendar(NOW)

    decision = policy.evaluate_entry(
        proposal_id="spy-call-spread-1",
        expiration=date(2026, 8, 21),
        now=NOW + timedelta(seconds=6),
        calendar=calendar,
    )

    assert decision.allowed is False
    assert "CALENDAR_STALE" in decision.reason_codes
    assert decision.calendar_hash == calendar.calendar_hash
    assert decision.verify_hash() is True


@pytest.mark.parametrize(
    "mutation",
    [
        lambda marker: marker.__setitem__("actor", "model:challenger"),
        lambda marker: marker.__setitem__("append_only", False),
        lambda marker: marker.__setitem__("policy_hash", "f" * 64),
        lambda marker: marker.__setitem__("revoked", True),
    ],
)
def test_untrusted_or_wrong_dte_exception_cannot_relax_entry_policy(mutation) -> None:
    policy = OptionTimePolicy()
    expiration = date(2026, 8, 16)
    marker: dict[str, object] = {
        "schema": DTE_EXCEPTION_SCHEMA,
        "version": "v1",
        "sequence": 1,
        "append_only": True,
        "decision": "APPROVE_7_13_DTE_ENTRY",
        "actor": "human:xujie",
        "reason": "event-defined risk exception",
        "proposal_id": "spy-call-spread-1",
        "expiration": expiration.isoformat(),
        "policy_hash": policy.policy_hash,
        "approved_at": (NOW - timedelta(minutes=1)).isoformat(),
        "expires_at": (NOW + timedelta(minutes=10)).isoformat(),
        "revoked": False,
        "previous_marker_hash": None,
    }
    mutation(marker)
    marker["governance_signature"] = canonical_hash(marker)
    marker["content_hash"] = canonical_hash(marker)
    authority = DteEntryExceptionAuthority.resolve(
        marker=marker,
        expected_policy_hash=policy.policy_hash,
        expected_proposal_id="spy-call-spread-1",
        expected_expiration=expiration,
        asof=NOW,
    )

    decision = policy.evaluate_dte(
        proposal_id="spy-call-spread-1",
        expiration=expiration,
        now=NOW,
        exception_authority=authority,
    )
    assert authority.approved is False
    assert decision.allowed is False
    assert "DTE_EXCEPTION_REQUIRED" in decision.reason_codes


def test_management_below_entry_floor_requires_p2_strict_reduction_proof() -> None:
    policy = OptionTimePolicy()
    expiration = date(2026, 8, 10)
    rejected = policy.evaluate_management(
        position_key="GLD-legacy-combo",
        expiration=expiration,
        now=NOW,
        current_quantity=Decimal("1"),
        proposed_quantity=Decimal("0"),
        current_max_loss_usd=Decimal("100"),
        proposed_max_loss_usd=Decimal("0"),
        current_capital_at_risk_usd=Decimal("100"),
        proposed_capital_at_risk_usd=Decimal("0"),
    )
    assert rejected.allowed is False
    assert "P2_MANAGEMENT_TRANSITION_PROOF_REQUIRED" in rejected.reason_codes

    proof_payload: dict[str, object] = {
        "schema": "options_copilot.p2.management_transition.v1",
        "position_key": "GLD-legacy-combo",
        "action": "CLOSE",
        "current_quantity": "1",
        "proposed_quantity": "0",
        "current_max_loss_usd": "100",
        "proposed_max_loss_usd": "0",
        "current_capital_at_risk_usd": "100",
        "proposed_capital_at_risk_usd": "0",
        "verified_at": NOW.isoformat(),
    }
    proof_payload["content_hash"] = canonical_hash(proof_payload)
    proof = P2ManagementTransitionProof.resolve(proof_payload)
    approved = policy.evaluate_management(
        position_key="GLD-legacy-combo",
        expiration=expiration,
        now=NOW,
        current_quantity=Decimal("1"),
        proposed_quantity=Decimal("0"),
        current_max_loss_usd=Decimal("100"),
        proposed_max_loss_usd=Decimal("0"),
        current_capital_at_risk_usd=Decimal("100"),
        proposed_capital_at_risk_usd=Decimal("0"),
        transition_proof=proof,
    )
    assert proof.valid is True
    assert approved.allowed is True
    assert approved.transition_proof_hash == proof.content_hash

    crossing_payload = dict(proof_payload)
    crossing_payload["proposed_quantity"] = "-1"
    crossing_payload.pop("content_hash")
    crossing_payload["content_hash"] = canonical_hash(crossing_payload)
    crossing = P2ManagementTransitionProof.resolve(crossing_payload)
    assert crossing.valid is False


def test_canonical_snapshots_reprice_without_changing_structural_hashes(
    tmp_path,
) -> None:
    original = _proposal()
    validated_a = _validate(original)
    canonical_a = validated_a.to_dict()

    now_b = NOW + timedelta(seconds=2)
    snapshot_b = "ibkr-quotes-20260803T150002Z"
    repriced = deepcopy(original)
    repriced["quote_snapshot_id"] = snapshot_b
    repriced["legs"][0]["ask"] = "2.02"
    for leg in repriced["legs"]:
        leg["quote_time"] = now_b.isoformat()
        leg["quote_snapshot_id"] = snapshot_b
    # A two-cent move in one 100x long leg adds $2 to debit/max loss and
    # subtracts $2 from the scenario-weighted, cost-after EV.
    repriced["risk"]["maximum_loss_usd"] = "115"
    repriced["expected_value_usd"] = "18"
    validated_b = validate_proposal(
        repriced,
        account_equity=Decimal("2000"),
        open_combinations=0,
        now=now_b,
        quote_fresh_seconds=5,
        expected_quote_snapshot_id=snapshot_b,
    )
    canonical_b = validated_b.to_dict()

    hashes_a, _ = proposal_hashes(canonical_a)
    hashes_b, _ = proposal_hashes(canonical_b)
    assert hashes_a.proposal_hash != hashes_b.proposal_hash
    assert hashes_a.material_hash == hashes_b.material_hash
    assert hashes_a.legs_hash == hashes_b.legs_hash
    assert hashes_a.risk_hash == hashes_b.risk_hash
    assert validated_a.proposal_hash == hashes_a.proposal_hash
    assert validated_b.proposal_hash == hashes_b.proposal_hash
    assert validated_b.executable_cost_usd == Decimal("112")
    assert validated_b.maximum_loss_usd == Decimal("115")
    assert validated_b.expected_value_usd == Decimal("18")

    trusted_now = [NOW]
    with ProposalApprovalStore(
        tmp_path / "proposal-approvals.sqlite3",
        clock=lambda: trusted_now[0],
    ) as store:
        approval = store.approve(
            "spy-call-spread-1",
            canonical_a,
            nonce="canonical-reprice-nonce",
            approved_by="options_copilot_gui",
            approved_at=NOW,
            approval_id="canonical-reprice-approval",
        )
        trusted_now[0] = now_b
        checked = store.validate(
            approval.approval_id,
            canonical_b,
            checked_at=now_b,
        )

    assert checked.valid is True
    assert checked.reasons == ()
    assert checked.adverse_change_usd == Decimal("2")


def test_normal_risk_above_ten_percent_is_rejected() -> None:
    with pytest.raises(ValueError, match="NORMAL_RISK_LIMIT_EXCEEDED"):
        _validate(account_equity="1000")


def test_exactly_ten_percent_normal_risk_is_accepted() -> None:
    result = _validate(account_equity="1130")
    assert result.risk_assessment.risk_fraction == Decimal("0.10")


def test_a_grade_requires_bound_p9_authority_not_unlock_boolean(
    tmp_path: Path,
) -> None:
    proposal = _proposal()
    proposal["risk_tier"] = "VALIDATED_A_GRADE"

    with pytest.raises(ValueError, match="A-grade risk tier is locked"):
        _validate(proposal, account_equity="1000")
    with pytest.raises(ValueError, match="A-grade risk tier is locked"):
        _validate(
            proposal,
            account_equity="1000",
            a_grade_unlocked=True,
        )

    nav = _strategy_nav(tmp_path)
    authority = _risk_authority(nav, _signed_a_grade_marker(nav.contract_hash))
    result = _validate(
        proposal,
        account_equity="1",
        a_grade_unlocked=True,
        risk_engine=_production_engine(nav, authority),
    )
    assert result.candidate.risk_tier is CandidateRiskTier.VALIDATED_A_GRADE
    assert result.risk_assessment.allowed_risk_fraction == Decimal("0.15")
    assert result.risk_assessment.production_bound is True


def test_exactly_fifteen_percent_signed_a_grade_risk_is_accepted(
    tmp_path: Path,
) -> None:
    proposal = _proposal()
    proposal["risk_tier"] = "VALIDATED_A_GRADE"
    proposal["estimated_commissions"] = "9"
    proposal["risk"]["maximum_loss_usd"] = "120"
    proposal["expected_value_usd"] = "13"

    nav = _strategy_nav(tmp_path, strategy_nav="800", observed_nlv="50000")
    authority = _risk_authority(nav, _signed_a_grade_marker(nav.contract_hash))
    result = _validate(
        proposal,
        account_equity="1",
        a_grade_unlocked=True,
        risk_engine=_production_engine(nav, authority),
    )
    assert result.risk_assessment.risk_fraction == Decimal("0.15")


def test_twenty_percent_is_still_hard_rejected_for_signed_a_grade(
    tmp_path: Path,
) -> None:
    proposal = _proposal()
    proposal["grade"] = "A"
    nav = _strategy_nav(tmp_path, strategy_nav="565")
    authority = _risk_authority(nav, _signed_a_grade_marker(nav.contract_hash))

    with pytest.raises(ValueError, match="HARD_RISK_LIMIT_REACHED"):
        _validate(
            proposal,
            account_equity="1",
            a_grade_unlocked=True,
            risk_engine=_production_engine(nav, authority),
        )


def test_production_risk_uses_strategy_nav_not_observed_account_nlv(
    tmp_path: Path,
) -> None:
    low_observed = _strategy_nav(tmp_path, observed_nlv="500")
    with StrategyNavLedger(
        tmp_path / "strategy-nav.sqlite3",
        contract=CONTRACT_PATH,
        clock=lambda: NOW,
    ) as ledger:
        high_observed = ledger.snapshot(
            asof=NOW,
            observed_account_nlv=Decimal("50000"),
        )
    candidate = _candidate_with_max_loss("1000")

    low = _production_engine(low_observed).assess(candidate)
    high = _production_engine(high_observed).assess(candidate)

    assert low.approved is True
    assert high.approved is True
    assert low.risk_fraction == high.risk_fraction == Decimal("0.10")
    assert low.risk_base == high.risk_base == Decimal("10000.00")
    assert low.strategy_nav_snapshot_hash != high.strategy_nav_snapshot_hash
    assert low.ledger_head_hash == high.ledger_head_hash


def test_production_normal_boundary_accepts_10_00_and_rejects_10_01(
    tmp_path: Path,
) -> None:
    nav = _strategy_nav(tmp_path)
    engine = _production_engine(nav)

    exact = engine.assess(_candidate_with_max_loss("1000"))
    over = engine.assess(
        _candidate_with_max_loss(
            "1001",
            tier=CandidateRiskTier.VALIDATED_A_GRADE,
        )
    )

    assert exact.risk_fraction == Decimal("0.10")
    assert exact.approved is True
    assert over.risk_fraction == Decimal("0.1001")
    assert over.approved is False
    assert over.allowed_risk_fraction == Decimal("0.10")
    assert RiskRejection.NORMAL_RISK_LIMIT_EXCEEDED in over.rejections


def test_valid_p9_a_grade_accepts_15_00_but_rejects_15_01_and_hard_line(
    tmp_path: Path,
) -> None:
    nav = _strategy_nav(tmp_path)
    marker = _signed_a_grade_marker(nav.contract_hash)
    authority = _risk_authority(nav, marker)
    engine = _production_engine(nav, authority)

    exact = engine.assess(
        _candidate_with_max_loss(
            "1500",
            tier=CandidateRiskTier.VALIDATED_A_GRADE,
        )
    )
    over = engine.assess(
        _candidate_with_max_loss(
            "1501",
            tier=CandidateRiskTier.VALIDATED_A_GRADE,
        )
    )
    hard = engine.assess(
        _candidate_with_max_loss(
            "2000",
            tier=CandidateRiskTier.VALIDATED_A_GRADE,
        )
    )
    above_hard = engine.assess(
        _candidate_with_max_loss(
            "2001",
            tier=CandidateRiskTier.VALIDATED_A_GRADE,
        )
    )

    assert authority.a_grade_approved is True
    assert exact.risk_fraction == Decimal("0.15")
    assert exact.approved is True
    assert exact.allowed_risk_fraction == Decimal("0.15")
    assert over.risk_fraction == Decimal("0.1501")
    assert RiskRejection.VALIDATED_RISK_LIMIT_EXCEEDED in over.rejections
    assert hard.risk_fraction == Decimal("0.20")
    assert RiskRejection.HARD_RISK_LIMIT_REACHED in hard.rejections
    assert above_hard.risk_fraction == Decimal("0.2001")
    assert RiskRejection.HARD_RISK_LIMIT_REACHED in above_hard.rejections


@pytest.mark.parametrize(
    "mutation",
    [
        lambda marker: marker.__setitem__("phase", "P8"),
        lambda marker: marker.__setitem__("decision", "APPROVE_PROMOTION"),
        lambda marker: marker.__setitem__("evaluation_report_hash", "9" * 64),
        lambda marker: marker.__setitem__("rolled_back", True),
        lambda marker: marker.__setitem__("revoked", True),
    ],
)
def test_signed_but_wrong_or_inactive_marker_cannot_unlock_a_grade(
    tmp_path: Path,
    mutation,
) -> None:
    nav = _strategy_nav(tmp_path)
    marker = _signed_a_grade_marker(nav.contract_hash)
    mutation(marker)
    marker.pop("governance_signature", None)
    marker.pop("content_hash", None)
    marker["governance_signature"] = canonical_hash(marker)
    marker["content_hash"] = canonical_hash(marker)
    authority = _risk_authority(nav, marker)

    assessment = _production_engine(nav, authority).assess(
        _candidate_with_max_loss(
            "1001",
            tier=CandidateRiskTier.VALIDATED_A_GRADE,
        )
    )

    assert authority.a_grade_approved is False
    assert assessment.allowed_risk_fraction == Decimal("0.10")
    assert RiskRejection.NORMAL_RISK_LIMIT_EXCEEDED in assessment.rejections


def test_unsigned_marker_and_untrusted_unlock_inputs_have_no_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nav = _strategy_nav(tmp_path)
    unsigned = _signed_a_grade_marker(nav.contract_hash)
    unsigned.pop("governance_signature")
    unsigned.pop("content_hash")
    monkeypatch.setenv("OPTIONS_COPILOT_A_GRADE_UNLOCKED", "true")
    authority = _risk_authority(nav, unsigned)
    candidate = _candidate_with_max_loss(
        "1001",
        tier=CandidateRiskTier.VALIDATED_A_GRADE,
    )

    assessment = _production_engine(nav, authority).assess(candidate)

    assert authority.a_grade_approved is False
    assert assessment.approved is False
    assert RiskRejection.NORMAL_RISK_LIMIT_EXCEEDED in assessment.rejections


def test_production_engine_rejects_invalid_or_mismatched_nav_bindings(
    tmp_path: Path,
) -> None:
    nav = _strategy_nav(tmp_path)
    with pytest.raises(ValueError, match="contract hash"):
        RiskEngine(
            strategy_nav=nav,
            expected_contract_hash="f" * 64,
            expected_ledger_head_hash=nav.ledger_head_hash,
            risk_tier_authority=_risk_authority(nav),
        )
    with pytest.raises(ValueError, match="ledger head"):
        RiskEngine(
            strategy_nav=nav,
            expected_contract_hash=nav.contract_hash,
            expected_ledger_head_hash="e" * 64,
            risk_tier_authority=_risk_authority(nav),
        )


def test_existing_open_combination_occupies_the_only_slot() -> None:
    with pytest.raises(ValueError, match="MAX_OPEN_COMBINATIONS"):
        _validate(open_combinations=1)


@pytest.mark.parametrize(
    ("declared", "accepted"),
    [("113.01", True), ("113.011", False)],
)
def test_maximum_loss_declaration_has_an_exact_one_cent_tolerance(
    declared: str,
    accepted: bool,
) -> None:
    proposal = _proposal()
    proposal["risk"]["maximum_loss_usd"] = declared

    if accepted:
        assert _validate(proposal).maximum_loss_usd == Decimal("113")
    else:
        with pytest.raises(ValueError, match="within \\$0.01"):
            _validate(proposal)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p["legs"][1].__setitem__("underlying", "QQQ"), "same underlying"),
        (
            lambda p: p["legs"][1].__setitem__("expiration", "2026-08-28"),
            "same expiration",
        ),
        (lambda p: p["legs"][0].__setitem__("currency", "EUR"), "must be USD"),
        (lambda p: p["legs"][0].__setitem__("security_type", "STK"), "must be OPT"),
    ],
)
def test_leg_family_must_be_same_us_equity_option(mutation, message: str) -> None:
    proposal = _proposal()
    mutation(proposal)

    with pytest.raises(ValueError, match=message):
        _validate(proposal)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p.__setitem__("rank", True),
        lambda p: p.__setitem__("rank", "1"),
        lambda p: p.__setitem__("eligible_to_send", 1),
        lambda p: p.__setitem__("eligible_to_send", "true"),
    ],
)
def test_rank_and_eligibility_reject_type_confusion(mutation) -> None:
    proposal = _proposal()
    mutation(proposal)

    with pytest.raises(ValueError):
        _validate(proposal)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p.pop("estimated_commissions"), "estimated_commissions is required"),
        (
            lambda p: p.__setitem__("estimated_slippage", "-0.01"),
            "estimated_slippage cannot be negative",
        ),
        (
            lambda p: p.__setitem__("expected_value_usd", "0"),
            "expected_value_usd must be positive",
        ),
        (
            lambda p: p.__setitem__("estimated_commissions", True),
            "not boolean",
        ),
        (
            lambda p: p["legs"][0].__setitem__("ask", float("nan")),
            "finite decimal number",
        ),
    ],
)
def test_cost_ev_and_decimal_fields_are_explicit_and_finite(mutation, message: str) -> None:
    proposal = deepcopy(_proposal())
    mutation(proposal)

    with pytest.raises(ValueError, match=message):
        _validate(proposal)


@pytest.mark.parametrize(
    "field",
    [
        "contract_id_ex",
        "expiration",
        "strike",
        "right",
        "quantity",
        "multiplier",
        "currency",
        "exchange",
    ],
)
def test_each_leg_requires_explicit_contract_and_order_fields(field: str) -> None:
    proposal = _proposal()
    proposal["legs"][0].pop(field)

    with pytest.raises(ValueError, match=field):
        _validate(proposal)
