"""Locked-first P9 authority-to-creator integration tests.

The production creator transport and production human signer are intentionally
absent.  The only controllable leases and creator in this module are marked
TEST_ONLY; they let the test exercise the durable approval transaction without
claiming that an in-process lock or recording callback is production authority.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from options_copilot.analytics import ScenarioEngine
from options_copilot.approval import (
    ApprovalAuthorityConflict,
    ProposalApprovalStore,
)
from options_copilot.approval.proofs import (
    BROKER_PROOF_SCHEMA,
    STRATEGY_NAV_PROOF_SCHEMA,
)
from options_copilot.bridge import (
    BridgeApprovalRejected,
    BridgeDecisionContext,
    BridgeStatus,
    BridgeValidationError,
    CodexBridgeStore,
    CurrentAuthorityProof,
    LocalCodexBridgeCoordinator,
)
from options_copilot.bridge.store import CURRENT_AUTHORITY_PROOF_SCHEMA
from options_copilot.decision import DecisionPipeline
from options_copilot.execution_cost import SignedExecutionCostResolver
from options_copilot.gateway import BrokerSnapshotBuilder
from options_copilot.governance.contracts import ContractValidationError
from options_copilot.learning.policy_authority import (
    CurrentPolicyResolver,
    PolicyAuthorityLedger,
    PolicyAuthorityTampered,
)
from options_copilot.market import UsOptionsSessionCalendar
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.proposals import validate_proposal
from options_copilot.ranking import PortfolioAction, PortfolioRanker, RankingStore
from options_copilot.risk import CurrentRiskAuthorityResolver, OptionTimePolicy, RiskEngine
from options_copilot.risk.authorization import RiskAuthorityTier, RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash

from tests.options_copilot.test_bridge_coordinator import (
    APPROVAL_CONTRACT_IDS,
    CONTRACT_DEFINITIONS_HASH,
    NAV_CONTRACT_HASH,
    NAV_LEDGER_HASH,
    QUOTES_HASH,
    MutableAtomicSource,
    MutableClock,
    RecordingCreator,
    atomic_contracts,
    broker_snapshot,
    instruction_intent,
    raw_proposal,
)
from tests.options_copilot.test_production_candidate_e2e import _build_harness


NOW = datetime(2026, 8, 6, 15, 0, tzinfo=timezone.utc)
INPUT_HASH = "1" * 64
EVIDENCE_HASH = "2" * 64
BROKER_HASH = "3" * 64
NAV_CONTENT_HASH = "4" * 64
ATOMIC_SNAPSHOT_HASH = "5" * 64
TEST_ONLY_TRANSPORT = "TEST_ONLY_RECORDING_CREATOR"
INITIAL_POLICY_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "initial_champion_scenario_policy.v1.json"
)


class _TestOnlyReadLease:
    """Controllable lease accepted only behind explicit TEST_ONLY gates."""

    test_only = True

    def guard_read(self, callback):
        return callback()


class _TestOnlyNullMarkerSource(_TestOnlyReadLease):
    """A stable, explicitly locked marker head containing no A-grade marker."""

    def read(self) -> None:
        return None


@dataclass(slots=True)
class _LockedHarness:
    root: Path
    clock: MutableClock
    ledger: PolicyAuthorityLedger
    policy_resolver: CurrentPolicyResolver
    policy: object
    risk_resolver: CurrentRiskAuthorityResolver
    risk_authority: RiskTierAuthority
    cost_resolver: SignedExecutionCostResolver
    cost: object
    nav: StrategyNavSnapshot
    rankings: RankingStore
    approvals: ProposalApprovalStore
    bridge: CodexBridgeStore
    coordinator: LocalCodexBridgeCoordinator
    creator: RecordingCreator

    @classmethod
    def open(cls, root: Path) -> "_LockedHarness":
        root.mkdir(parents=True, exist_ok=True)
        clock = MutableClock(NOW)
        initial_policy = root / "initial-policy.v1.json"
        if not initial_policy.exists():
            initial_policy.write_bytes(INITIAL_POLICY_SOURCE.read_bytes())
        ledger = PolicyAuthorityLedger(
            root / "policy-authority.sqlite3",
            initial_policy_path=initial_policy,
        )
        policy_resolver = CurrentPolicyResolver(ledger)
        policy = policy_resolver.resolve(now=clock.current)

        risk_resolver = CurrentRiskAuthorityResolver(
            NAV_CONTRACT_HASH,
            _TestOnlyNullMarkerSource(),
            allow_test_authority=True,
            clock=clock,
        )
        risk_authority = risk_resolver.resolve(
            now=clock.current,
            current_policy=policy,
            resolved_policy=policy,
        )
        assert risk_authority.tier is RiskAuthorityTier.NORMAL
        assert risk_authority.a_grade_approved is False

        cost_resolver = SignedExecutionCostResolver(
            clock=clock,
            authority_read_lease=_TestOnlyReadLease(),
            allow_test_authority_lease=True,
        )
        cost = cost_resolver.resolve(
            now=clock.current,
            current_policy=policy,
            resolved_policy=policy,
        )
        nav = _strategy_nav(clock.current)

        rankings = RankingStore(root / "ranking.sqlite3")
        approvals = ProposalApprovalStore(root / "approvals.sqlite3", clock=clock)
        bridge = CodexBridgeStore(
            root / "bridge.sqlite3",
            approvals,
            clock=clock,
            current_authority_validator=lambda binding, checked_at: _current_proof(
                binding,
                checked_at,
                policy_resolver=policy_resolver,
                risk_resolver=risk_resolver,
                cost_resolver=cost_resolver,
                nav=nav,
            ),
        )
        source = MutableAtomicSource(clock)
        source.account["net_liquidation"] = Decimal("1130")
        source.lock_probe = lambda: bridge._connection.in_transaction
        coordinator = LocalCodexBridgeCoordinator(
            bridge,
            clock=clock,
            broker_snapshot_builder=BrokerSnapshotBuilder(source, clock=clock),
            contract_resolver=lambda _proposal: atomic_contracts(),
            decision_context_resolver=lambda: _decision_context(
                clock.current,
                nav=nav,
                policy_resolver=policy_resolver,
                risk_resolver=risk_resolver,
                cost_resolver=cost_resolver,
            ),
        )
        creator = RecordingCreator(bridge)
        return cls(
            root,
            clock,
            ledger,
            policy_resolver,
            policy,
            risk_resolver,
            risk_authority,
            cost_resolver,
            cost,
            nav,
            rankings,
            approvals,
            bridge,
            coordinator,
            creator,
        )

    def close(self) -> None:
        self.bridge.close()
        self.approvals.close()
        self.rankings.close()
        self.ledger.close()

    def attempt(
        self,
        risk_percent: str,
        *,
        execute: bool = True,
    ) -> dict[str, object]:
        fraction = Decimal(risk_percent) / Decimal("100")
        candidate = _ranker_candidate(self.clock.current, fraction, self.nav)
        ranking = PortfolioRanker().rank(
            (candidate,),
            current_policy=self.policy,
            risk_authority=self.risk_authority,
            cost_version=self.cost.cost_version,
            cost_hash=self.cost.cost_hash,
            risk_contract_hash=NAV_CONTRACT_HASH,
            evidence_inputs=_evidence_inputs(),
        )
        approval_id = f"approval-locked-{risk_percent.replace('.', '-') }"

        if ranking.candidates:
            snapshot = self.rankings.append_snapshot(
                scan_run_id=f"scan-locked-{risk_percent.replace('.', '-')}",
                input_hash=INPUT_HASH,
                evidence_hash=EVIDENCE_HASH,
                broker_snapshot_hash=BROKER_HASH,
                candidates=ranking.candidates,
                governance_evidence=ranking.governance_evidence,
                valid_until=self.clock.current + timedelta(minutes=5),
                current_policy_version=self.policy.current_policy_version,
                current_policy_hash=self.policy.current_policy_hash,
                policy_authority_marker_hash=(
                    self.policy.policy_authority_marker_hash
                ),
                cost_version=self.cost.cost_version,
                cost_hash=self.cost.cost_hash,
                risk_contract_hash=NAV_CONTRACT_HASH,
                risk_authority_version=self.risk_authority.version,
                risk_authority_marker_hash=(
                    self.risk_authority.risk_authority_marker_hash
                ),
                policy_resolver=self.policy_resolver,
                risk_authority_resolver=self.risk_resolver,
                resolved_policy=self.policy,
                risk_authority=self.risk_authority,
                now=self.clock.current,
            )
            ranked = ranking.candidates[0]
            issued = self._issue_challenge(
                snapshot.ranking_snapshot_id,
                ranked.candidate_id,
                ranked.proposal_hash,
            )
            confirmation = self.approvals.confirm_challenge(
                issued.challenge.challenge_id,
                challenge_response=issued.challenge_response,
                risk_acknowledged=True,
                second_confirmation=True,
                confirmation_token="CREATE_IBKR_REVIEW_ONLY",
                ranking_store=self.rankings,
                policy_resolver=self.policy_resolver,
                risk_authority_resolver=self.risk_resolver,
                execution_cost_contract=self.cost_resolver,
                broker_proof=self._broker_proof(
                    snapshot.ranking_snapshot_id,
                    ranked.candidate_id,
                    ranked.proposal_hash,
                ),
                strategy_nav_proof=self._nav_proof(),
                strategy_nav_source=_TestOnlyNavSource(self.nav),
                strategy_nav_snapshot=self.nav,
                now=self.clock.current,
            )
            approval_id = confirmation.approval.approval_id
            if not execute:
                return {
                    "ranking": ranking,
                    "approval_id": approval_id,
                    "confirmation": confirmation,
                }
            token = self.coordinator.claim(approval_id)
            self.coordinator.authorize(
                approval_id,
                token,
                broker_snapshot(now=self.clock.current),
                instruction_intent(),
            )
            try:
                completed = self.coordinator.execute_authorized(
                    approval_id,
                    token,
                    self.creator,
                )
            except BridgeValidationError as exc:
                return {
                    "ranking": ranking,
                    "approval_id": approval_id,
                    "preflight_refusal": str(exc),
                    "authorized": self.bridge.get(approval_id),
                }
            return {
                "ranking": ranking,
                "approval_id": approval_id,
                "completed": completed,
            }

        if ranking.governance_evidence:
            self.rankings.append_snapshot(
                scan_run_id=f"scan-locked-{risk_percent.replace('.', '-')}",
                input_hash=INPUT_HASH,
                evidence_hash=EVIDENCE_HASH,
                broker_snapshot_hash=BROKER_HASH,
                candidates=(),
                governance_evidence=ranking.governance_evidence,
                valid_until=self.clock.current + timedelta(minutes=5),
                current_policy_version=self.policy.current_policy_version,
                current_policy_hash=self.policy.current_policy_hash,
                policy_authority_marker_hash=self.policy.policy_authority_marker_hash,
                cost_version=self.cost.cost_version,
                cost_hash=self.cost.cost_hash,
                risk_contract_hash=NAV_CONTRACT_HASH,
                risk_authority_version=self.risk_authority.version,
                risk_authority_marker_hash=self.risk_authority.risk_authority_marker_hash,
                now=self.clock.current,
            )
        return {"ranking": ranking, "approval_id": approval_id}

    def _issue_challenge(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        proposal_hash: str | None,
        *,
        now: datetime | None = None,
    ):
        assert proposal_hash is not None
        at = now or self.clock.current
        return self.approvals.create_challenge(
            ranking_snapshot_id,
            candidate_id,
            ranking_store=self.rankings,
            policy_resolver=self.policy_resolver,
            risk_authority_resolver=self.risk_resolver,
            execution_cost_contract=self.cost_resolver,
            broker_proof=self._broker_proof(
                ranking_snapshot_id,
                candidate_id,
                proposal_hash,
                observed_at=at,
            ),
            strategy_nav_proof=self._nav_proof(),
            strategy_nav_source=_TestOnlyNavSource(self.nav),
            strategy_nav_snapshot=self.nav,
            now=at,
        )

    def _broker_proof(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        proposal_hash: str,
        *,
        observed_at: datetime | None = None,
    ) -> dict[str, object]:
        at = observed_at or self.clock.current
        return {
            "schema": BROKER_PROOF_SCHEMA,
            "ranking_snapshot_id": ranking_snapshot_id,
            "candidate_id": candidate_id,
            "proposal_hash": proposal_hash,
            "snapshot_hash": ATOMIC_SNAPSHOT_HASH,
            "built_at": at,
            "quote_batch_id": f"locked-quotes-{candidate_id}",
            "oldest_quote_observed_at": at,
            "state_hashes": {
                "account": "6" * 64,
                "positions": "7" * 64,
                "working_orders": "8" * 64,
                "unsubmitted_instructions": "9" * 64,
            },
            "contract_definitions_hash": CONTRACT_DEFINITIONS_HASH,
            "quotes_hash": QUOTES_HASH,
            "contract_ids": list(APPROVAL_CONTRACT_IDS),
            "account_nlv_usd": "1130",
            "open_option_position_count": 0,
            "working_order_count": 0,
            "unsubmitted_instruction_count": 0,
            "status": "COMPLETE",
        }

    def _nav_proof(self) -> dict[str, object]:
        return {
            "schema": STRATEGY_NAV_PROOF_SCHEMA,
            "content_hash": self.nav.content_hash,
            "authority_hash": self.nav.authority_hash,
            "contract_hash": self.nav.contract_hash,
            "ledger_head_hash": self.nav.ledger_head_hash,
            "strategy_nav_usd": "1130",
            "observed_account_nlv": "1130",
            "reconciliation_difference": "0",
            "asof": self.nav.asof,
            "snapshot_payload": self.nav.hash_payload(),
        }


class _TestOnlyNavSource:
    test_only = True

    def __init__(self, snapshot: StrategyNavSnapshot) -> None:
        self.snapshot = snapshot

    def is_current(self, value: object) -> bool:
        return value is self.snapshot

    def guard_current(self, value: object, *, callback):
        if not self.is_current(value):
            return None
        return callback()


def _strategy_nav(now: datetime) -> StrategyNavSnapshot:
    fields = {
        "asof": now,
        "strategy_nav": Decimal("1130"),
        "strategy_deposits": Decimal("1130"),
        "strategy_withdrawals": Decimal("0"),
        "realized_pnl": Decimal("0"),
        "open_position_unrealized_pnl": Decimal("0"),
        "fees": Decimal("0"),
        "signed_corrections": Decimal("0"),
        "non_strategy_contribution": Decimal("0"),
        "fill_principal_contribution": Decimal("0"),
        "observed_account_nlv": Decimal("1130"),
        "reconciliation_difference": Decimal("0"),
        "contract_version": "strategy-nav-v1",
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
        "valid": True,
        "no_trade_reasons": (),
    }
    return StrategyNavSnapshot(**fields, content_hash=canonical_hash(fields))


def _evidence_inputs() -> dict[str, str]:
    return {
        "input_hash": INPUT_HASH,
        "evidence_hash": EVIDENCE_HASH,
        "broker_snapshot_hash": BROKER_HASH,
    }


def _canonical_proposal(now: datetime) -> dict[str, object]:
    validated = validate_proposal(
        raw_proposal(now=now),
        account_equity=Decimal("1130"),
        open_combinations=0,
        now=now,
        quote_fresh_seconds=Decimal("5"),
        expected_quote_snapshot_id="managed-quotes-20260803T150000Z",
    )
    assert validated.risk_assessment.risk_fraction == Decimal("0.10")
    return validated.to_dict()


def _ranker_candidate(
    now: datetime,
    risk_fraction: Decimal,
    nav: StrategyNavSnapshot,
) -> dict[str, object]:
    proposal = _canonical_proposal(now)
    candidate_id = str(proposal["proposal_id"])
    legs = deepcopy(proposal["legs"])
    for leg, contract_id in zip(legs, APPROVAL_CONTRACT_IDS, strict=True):
        leg["con_id"] = contract_id
    candidate_body = {
        "candidate_id": candidate_id,
        "underlying": proposal["underlying"],
        "legs": legs,
        "strategy_nav_hash": nav.authority_hash,
        "strategy_nav_content_hash": nav.content_hash,
        "strategy_nav_contract_hash": nav.contract_hash,
        "strategy_nav_ledger_head_hash": nav.ledger_head_hash,
        "strategy_nav_usd": nav.strategy_nav,
        "strategy_nav_observed_account_nlv": nav.observed_account_nlv,
        "strategy_nav_reconciliation_difference": (
            nav.reconciliation_difference
        ),
        "strategy_nav_asof": nav.asof,
    }
    return {
        "candidate_id": candidate_id,
        "candidate_body": candidate_body,
        "proposal_body": proposal,
        "eligible": True,
        "after_cost_expected_value": Decimal("20"),
        "liquidity_score": Decimal("10"),
        "max_loss": Decimal("113"),
        "risk_fraction": risk_fraction,
        "open_combinations": 0,
        "structure": "DEBIT_VERTICAL",
        "underlying": "SPY",
        "thesis": "locked-first boundary fixture",
        "evidence_inputs": _evidence_inputs(),
    }


def _decision_context(
    now: datetime,
    *,
    nav: StrategyNavSnapshot,
    policy_resolver: CurrentPolicyResolver,
    risk_resolver: CurrentRiskAuthorityResolver,
    cost_resolver: SignedExecutionCostResolver,
) -> BridgeDecisionContext:
    policy = policy_resolver.resolve(now=now)
    risk = risk_resolver.resolve(
        now=now,
        current_policy=policy,
        resolved_policy=policy,
    )
    cost = cost_resolver.resolve(
        now=now,
        current_policy=policy,
        resolved_policy=policy,
    )
    engine = RiskEngine(
        strategy_nav=nav,
        expected_contract_hash=nav.contract_hash,
        expected_ledger_head_hash=nav.ledger_head_hash,
        risk_tier_authority=risk,
    )
    market_date = now.astimezone(ZoneInfo("America/New_York")).date()
    day = market_date.strftime("%Y%m%d")
    hours = f"{day}:0930-{day}:1600"
    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours=hours,
        trading_hours=hours,
        timezone_id="America/New_York",
        observed_at=now,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=now,
    )
    return BridgeDecisionContext(
        risk_engine=engine,
        execution_cost_contract_version=cost.cost_version,
        execution_cost_contract_hash=cost.cost_hash,
        current_policy_version=policy.current_policy_version,
        current_policy_hash=policy.current_policy_hash,
        policy_authority_marker_hash=policy.policy_authority_marker_hash,
        option_time_policy=OptionTimePolicy(),
        market_calendar=calendar,
    )


def _current_proof(
    binding,
    checked_at: datetime,
    *,
    policy_resolver: CurrentPolicyResolver,
    risk_resolver: CurrentRiskAuthorityResolver,
    cost_resolver: SignedExecutionCostResolver,
    nav: StrategyNavSnapshot,
) -> CurrentAuthorityProof:
    policy = policy_resolver.resolve(now=checked_at)
    risk = risk_resolver.resolve(
        now=checked_at,
        current_policy=policy,
        resolved_policy=policy,
        proposal_hash=binding.proposal_hash,
        candidate_hash=binding.candidate_hash,
        execution_cost_version=binding.cost_version,
        execution_cost_hash=binding.cost_hash,
        ranking_basis_hash=binding.ranking_basis_hash,
    )
    cost = cost_resolver.resolve(
        now=checked_at,
        current_policy=policy,
        resolved_policy=policy,
    )
    expected = (
        binding.current_policy_version,
        binding.current_policy_hash,
        binding.policy_authority_marker_hash,
        binding.risk_authority_version,
        binding.risk_authority_marker_hash,
        binding.cost_version,
        binding.cost_hash,
    )
    actual = (
        policy.current_policy_version,
        policy.current_policy_hash,
        policy.policy_authority_marker_hash,
        risk.version,
        risk.risk_authority_marker_hash,
        cost.cost_version,
        cost.cost_hash,
    )
    if actual != expected:
        raise ValueError("current authority head mismatch")
    return CurrentAuthorityProof(
        schema=CURRENT_AUTHORITY_PROOF_SCHEMA,
        status="CURRENT",
        checked_at=checked_at,
        ranking_snapshot_id=binding.ranking_snapshot_id,
        candidate_id=binding.candidate_id,
        proposal_hash=binding.proposal_hash,
        snapshot_hash=binding.snapshot_hash,
        current_policy_version=policy.current_policy_version,
        current_policy_hash=policy.current_policy_hash,
        policy_authority_marker_hash=policy.policy_authority_marker_hash,
        cost_version=cost.cost_version,
        cost_hash=cost.cost_hash,
        risk_contract_hash=NAV_CONTRACT_HASH,
        risk_authority_version=risk.version,
        risk_authority_marker_hash=risk.risk_authority_marker_hash,
        strategy_nav_content_hash=nav.content_hash,
        strategy_nav_contract_hash=nav.contract_hash,
        strategy_nav_ledger_head_hash=nav.ledger_head_hash,
    )


@pytest.fixture
def locked_harness(tmp_path: Path):
    harness = _LockedHarness.open(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


@pytest.mark.parametrize(
    ("risk_percent", "expected_status"),
    [
        ("10.00", "NORMAL"),
        ("10.01", "A_GRADE_PENDING"),
        ("15.00", "A_GRADE_PENDING"),
        ("15.01", "REJECTED"),
        ("20.00", "REJECTED"),
        ("20.01", "REJECTED"),
    ],
)
def test_locked_first_exact_boundary_matrix_has_zero_rejected_calls(
    locked_harness: _LockedHarness,
    risk_percent: str,
    expected_status: str,
) -> None:
    before_attempts = locked_harness.bridge._connection.execute(
        "SELECT COUNT(*) FROM codex_bridge_external_attempts"
    ).fetchone()[0]
    result = locked_harness.attempt(risk_percent)
    ranking = result["ranking"]

    if risk_percent == "10.00":
        assert ranking.action is PortfolioAction.TRADE
        assert ranking.candidates[0].authority_status == expected_status
        authorized = result["authorized"]
        assert authorized.status is BridgeStatus.AUTHORIZED
        assert result["preflight_refusal"] == (
            "creator review destination contract is unavailable"
        )
        assert locked_harness.creator.calls == []
        assert TEST_ONLY_TRANSPORT.startswith("TEST_ONLY")
        after_attempts = locked_harness.bridge._connection.execute(
            "SELECT COUNT(*) FROM codex_bridge_external_attempts"
        ).fetchone()[0]
        assert after_attempts == before_attempts
        assert locked_harness.approvals.validate(
            str(result["approval_id"]),
            authorized.proposal,
        ).valid
        return

    assert ranking.action is PortfolioAction.NO_TRADE
    assert not ranking.candidates
    if expected_status == "A_GRADE_PENDING":
        assert ranking.governance_evidence[0].authority_status == expected_status
    else:
        assert not ranking.governance_evidence
        assert "RISK_ABOVE_A_GRADE_CEILING" in ranking.rejections
    assert locked_harness.approvals.record_counts()["proposal_approvals"] == 0
    assert locked_harness.bridge.get(str(result["approval_id"])) is None
    assert not locked_harness.bridge.has_external_call_attempt(
        str(result["approval_id"])
    )
    assert locked_harness.bridge._connection.execute(
        "SELECT COUNT(*) FROM codex_bridge_external_attempts"
    ).fetchone()[0] == before_attempts
    assert locked_harness.creator.calls == []


def test_restart_preserves_initial_head_and_all_authority_bindings(tmp_path: Path) -> None:
    first = _LockedHarness.open(tmp_path)
    policy_identity = (
        first.policy.current_policy_version,
        first.policy.current_policy_hash,
        first.policy.policy_authority_marker_hash,
    )
    first.close()

    restarted = _LockedHarness.open(tmp_path)
    try:
        assert (
            restarted.policy.current_policy_version,
            restarted.policy.current_policy_hash,
            restarted.policy.policy_authority_marker_hash,
        ) == policy_identity
        result = restarted.attempt("10.00")
        binding = restarted.approvals.get_authority_binding(
            str(result["approval_id"])
        )
        assert binding is not None
        assert (
            binding.current_policy_version,
            binding.current_policy_hash,
            binding.policy_authority_marker_hash,
        ) == policy_identity
        assert binding.risk_authority_marker_hash == (
            restarted.risk_authority.risk_authority_marker_hash
        )
        assert result["preflight_refusal"] == (
            "creator review destination contract is unavailable"
        )
        assert result["authorized"].status is BridgeStatus.AUTHORIZED
        assert not restarted.bridge.has_external_call_attempt(
            str(result["approval_id"])
        )
        assert restarted.creator.calls == []
    finally:
        restarted.close()


def test_tampered_policy_ledger_blocks_before_approval_reservation_or_call(
    locked_harness: _LockedHarness,
) -> None:
    candidate = _ranker_candidate(
        locked_harness.clock.current,
        Decimal("0.10"),
        locked_harness.nav,
    )
    ranking = PortfolioRanker().rank(
        (candidate,),
        current_policy=locked_harness.policy,
        risk_authority=locked_harness.risk_authority,
        cost_version=locked_harness.cost.cost_version,
        cost_hash=locked_harness.cost.cost_hash,
        risk_contract_hash=NAV_CONTRACT_HASH,
        evidence_inputs=_evidence_inputs(),
    )
    snapshot = locked_harness.rankings.append_snapshot(
        scan_run_id="scan-ledger-tamper",
        input_hash=INPUT_HASH,
        evidence_hash=EVIDENCE_HASH,
        broker_snapshot_hash=BROKER_HASH,
        candidates=ranking.candidates,
        valid_until=locked_harness.clock.current + timedelta(minutes=5),
        current_policy_version=locked_harness.policy.current_policy_version,
        current_policy_hash=locked_harness.policy.current_policy_hash,
        policy_authority_marker_hash=(
            locked_harness.policy.policy_authority_marker_hash
        ),
        cost_version=locked_harness.cost.cost_version,
        cost_hash=locked_harness.cost.cost_hash,
        risk_contract_hash=NAV_CONTRACT_HASH,
        risk_authority_version=locked_harness.risk_authority.version,
        risk_authority_marker_hash=(
            locked_harness.risk_authority.risk_authority_marker_hash
        ),
        policy_resolver=locked_harness.policy_resolver,
        risk_authority_resolver=locked_harness.risk_resolver,
        resolved_policy=locked_harness.policy,
        risk_authority=locked_harness.risk_authority,
        now=locked_harness.clock.current,
    )
    locked_harness.ledger.initial_policy_path.write_bytes(b"{}\n")

    with pytest.raises(ApprovalAuthorityConflict):
        locked_harness._issue_challenge(
            snapshot.ranking_snapshot_id,
            ranking.candidates[0].candidate_id,
            ranking.candidates[0].proposal_hash,
        )
    with pytest.raises((PolicyAuthorityTampered, ContractValidationError)):
        locked_harness.policy_resolver.resolve(now=locked_harness.clock.current)
    assert locked_harness.approvals.record_counts()["proposal_approvals"] == 0
    assert locked_harness.bridge._connection.execute(
        "SELECT COUNT(*) FROM codex_bridge_external_attempts"
    ).fetchone()[0] == 0
    assert locked_harness.creator.calls == []


def test_stale_ranking_and_stale_approval_both_block_before_reservation(
    tmp_path: Path,
) -> None:
    stale_ranking = _LockedHarness.open(tmp_path / "ranking")
    try:
        candidate = _ranker_candidate(
            stale_ranking.clock.current,
            Decimal("0.10"),
            stale_ranking.nav,
        )
        ranking = PortfolioRanker().rank(
            (candidate,),
            current_policy=stale_ranking.policy,
            risk_authority=stale_ranking.risk_authority,
            cost_version=stale_ranking.cost.cost_version,
            cost_hash=stale_ranking.cost.cost_hash,
            risk_contract_hash=NAV_CONTRACT_HASH,
            evidence_inputs=_evidence_inputs(),
        )
        snapshot = stale_ranking.rankings.append_snapshot(
            scan_run_id="scan-stale-ranking",
            input_hash=INPUT_HASH,
            evidence_hash=EVIDENCE_HASH,
            broker_snapshot_hash=BROKER_HASH,
            candidates=ranking.candidates,
            valid_until=stale_ranking.clock.current + timedelta(seconds=1),
            current_policy_version=stale_ranking.policy.current_policy_version,
            current_policy_hash=stale_ranking.policy.current_policy_hash,
            policy_authority_marker_hash=stale_ranking.policy.policy_authority_marker_hash,
            cost_version=stale_ranking.cost.cost_version,
            cost_hash=stale_ranking.cost.cost_hash,
            risk_contract_hash=NAV_CONTRACT_HASH,
            risk_authority_version=stale_ranking.risk_authority.version,
            risk_authority_marker_hash=stale_ranking.risk_authority.risk_authority_marker_hash,
            policy_resolver=stale_ranking.policy_resolver,
            risk_authority_resolver=stale_ranking.risk_resolver,
            resolved_policy=stale_ranking.policy,
            risk_authority=stale_ranking.risk_authority,
            now=stale_ranking.clock.current,
        )
        stale_ranking.clock.current += timedelta(seconds=2)
        with pytest.raises(ApprovalAuthorityConflict):
            stale_ranking._issue_challenge(
                snapshot.ranking_snapshot_id,
                ranking.candidates[0].candidate_id,
                ranking.candidates[0].proposal_hash,
                now=stale_ranking.clock.current,
            )
        assert stale_ranking.creator.calls == []
        assert stale_ranking.bridge._connection.execute(
            "SELECT COUNT(*) FROM codex_bridge_external_attempts"
        ).fetchone()[0] == 0
    finally:
        stale_ranking.close()

    stale_approval = _LockedHarness.open(tmp_path / "approval")
    try:
        result = stale_approval.attempt("10.00", execute=False)
        stale_approval.clock.current += timedelta(minutes=6)
        with pytest.raises(BridgeApprovalRejected):
            stale_approval.coordinator.claim(str(result["approval_id"]))
        assert stale_approval.creator.calls == []
        assert stale_approval.bridge._connection.execute(
            "SELECT COUNT(*) FROM codex_bridge_external_attempts"
        ).fetchone()[0] == 0
    finally:
        stale_approval.close()


def test_unsigned_revoked_or_rollback_claims_never_raise_normal_ceiling() -> None:
    policy_hash = "a" * 64
    policy_marker_hash = "b" * 64
    base = {
        "schema": "options_copilot.learning.a_grade_authority.v1",
        "decision": "APPROVE_A_GRADE",
        "revoked": True,
        "rolled_back": True,
    }
    authority = RiskTierAuthority.resolve(
        risk_contract_hash=NAV_CONTRACT_HASH,
        marker=base,
        asof=NOW,
        expected_current_policy_version="v1",
        expected_policy_hash=policy_hash,
        expected_policy_authority_marker_hash=policy_marker_hash,
    )
    assert authority.tier is RiskAuthorityTier.NORMAL
    assert authority.a_grade_approved is False
    assert authority.allows_risk_fraction(Decimal("0.10")) is True
    assert authority.allows_risk_fraction(Decimal("0.1001")) is False
    assert authority.allows_risk_fraction(Decimal("0.15")) is False


def test_current_policy_resolver_flows_through_scenario_pipeline_and_ranker(
    tmp_path: Path,
) -> None:
    """Use the existing production-shaped candidate harness with the P9 head."""

    base = _build_harness(
        tmp_path / "base", real_scenario_and_cost=True,
        observed_scenario_scores=(Decimal("0"), Decimal("0")),
    )
    ledger = PolicyAuthorityLedger(tmp_path / "pipeline-authority.sqlite3")
    resolver = CurrentPolicyResolver(ledger)
    now = base.now
    policy = resolver.resolve(now=now)
    risk = CurrentRiskAuthorityResolver(
        "d" * 64,
        _TestOnlyNullMarkerSource(),
        allow_test_authority=True,
        clock=lambda: now,
    )
    cost = SignedExecutionCostResolver(
        clock=lambda: now,
        authority_read_lease=_TestOnlyReadLease(),
        allow_test_authority_lease=True,
    )
    rankings = RankingStore(tmp_path / "pipeline-ranking.sqlite3")
    try:
        base.runtime.broker_evidence_acquisition.policy_resolver = resolver
        pipeline = DecisionPipeline(
            inputs=base.runtime.pipeline_inputs,
            universe_funnel=base.runtime.universe_funnel,
            broker_evidence=base.runtime.broker_evidence_acquisition,
            strategy_registry=base.runtime.strategy_registry,
            strategy_generator=base.runtime.strategy_candidate_generator,
            volatility_engine=base.runtime.volatility_engine,
            scenario_engine=ScenarioEngine(resolver),
            policy_resolver=resolver,
            risk_authority_resolver=risk,
            cost_contract=cost,
            eligibility_gate=base.runtime.eligibility_gate,
            portfolio_ranker=PortfolioRanker(),
            ranking_store=rankings,
            clock=lambda: now,
        )
        result = pipeline.run_slot("scan-production-e2e", now)
        assert result["status"] == "TRADE"
        assert (
            result["current_policy_version"],
            result["current_policy_hash"],
            result["policy_authority_marker_hash"],
        ) == (
            policy.current_policy_version,
            policy.current_policy_hash,
            policy.policy_authority_marker_hash,
        )
        stored = rankings.read_snapshot(str(result["ranking_snapshot_id"]))
        assert stored["current_policy_hash"] == policy.current_policy_hash
        assert stored["policy_authority_marker_hash"] == (
            policy.policy_authority_marker_hash
        )
        assert stored["candidates"][0]["authority_status"] == "NORMAL"
    finally:
        rankings.close()
        ledger.close()
        base.close()


@pytest.mark.human_authority
@pytest.mark.skip(
    reason=(
        "requires an externally supplied, explicitly human-signed production "
        "P9 decision; this locked-first suite must not create that fixture"
    )
)
def test_post_human_a_grade_and_atomic_rollback_checkpoint() -> None:
    """Reserved for P9-02-04 after a real human governance decision."""
