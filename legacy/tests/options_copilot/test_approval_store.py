from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from options_copilot.analytics.scenarios import ResolvedPolicy
from options_copilot.approval.store import (
    BROKER_PROOF_SCHEMA,
    STRATEGY_NAV_PROOF_SCHEMA,
    ApprovalChallengeRejected,
    ApprovalError,
    ProposalApprovalStore,
)
from options_copilot.bridge.store import (
    BridgeApprovalRejected,
    CodexBridgeStore,
)
from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.ranking.store import RankingStore
from options_copilot.risk.authorization import RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash, freeze_json


NOW = datetime(2026, 8, 5, 4, 0, tzinfo=timezone.utc)
INPUT_HASH = "a" * 64
EVIDENCE_HASH = "b" * 64
BROKER_HASH = "c" * 64
POLICY_HASH = "d" * 64
POLICY_MARKER_HASH = "1" * 64
COST_HASH = "e" * 64
RISK_CONTRACT_HASH = "2" * 64
NAV_CONTRACT_HASH = "4" * 64
NAV_LEDGER_HASH = "5" * 64
NAV_AUTHORITY_HASH = canonical_hash(
    {
        "schema": "options_copilot.strategy_nav_authority.v1",
        "strategy_nav_usd": Decimal("25000"),
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
    }
)
NAV_SNAPSHOT_PAYLOAD = {
    "asof": NOW - timedelta(minutes=30),
    "strategy_nav": Decimal("25000"),
    "strategy_deposits": Decimal("0"),
    "strategy_withdrawals": Decimal("0"),
    "realized_pnl": Decimal("0"),
    "open_position_unrealized_pnl": Decimal("0"),
    "fees": Decimal("0"),
    "signed_corrections": Decimal("0"),
    "non_strategy_contribution": Decimal("0"),
    "fill_principal_contribution": Decimal("0"),
    "observed_account_nlv": Decimal("100000"),
    "reconciliation_difference": Decimal("75000"),
    "contract_version": "v1",
    "contract_hash": NAV_CONTRACT_HASH,
    "ledger_head_hash": NAV_LEDGER_HASH,
    "valid": True,
    "no_trade_reasons": (),
}
NAV_CONTENT_HASH = canonical_hash(NAV_SNAPSHOT_PAYLOAD)
ATOMIC_SNAPSHOT_HASH = "6" * 64
CONTRACT_DEFINITIONS_HASH = "7" * 64
QUOTES_HASH = "8" * 64
CONTRACT_IDS = (101, 102)
CONFIRMATION_TOKEN = "CREATE_IBKR_REVIEW_ONLY"


class _Clock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class _Resolver:
    def __init__(self, value: object) -> None:
        self.value = value
        self.current = True
        self.resolve_calls = 0
        self._guard_lock = threading.RLock()

    def resolve(self, **_: object) -> object:
        with self._guard_lock:
            self.resolve_calls += 1
            return self.value

    def is_current(self, value: object) -> bool:
        with self._guard_lock:
            return self.current and value == self.value

    def guard_current(self, value: object, *, callback):
        with self._guard_lock:
            if not self.is_current(value):
                return None
            return callback()

    def change_head(self) -> None:
        with self._guard_lock:
            self.current = False


class _FlipAfterPrecheckResolver(_Resolver):
    def __init__(self, value: object) -> None:
        super().__init__(value)
        self.current_checks = 0

    def is_current(self, value: object) -> bool:
        with self._guard_lock:
            self.current_checks += 1
            if self.current_checks == 1:
                matches = self.current and value == self.value
                self.current = False
                return matches
            return False


class _ResolveOnly:
    def __init__(self, value: object) -> None:
        self.value = value

    def resolve(self, **_: object) -> object:
        return self.value


class _NavGuard:
    def __init__(self, value: object) -> None:
        self.value = value
        self.current = True
        self.guard_calls = 0

    def guard_current(self, value: object, *, callback):
        self.guard_calls += 1
        if not self.current or value is not self.value:
            return None
        return callback()


class _BlockingNavGuard(_NavGuard):
    def __init__(
        self,
        value: object,
        entered: threading.Event,
        release: threading.Event,
    ) -> None:
        super().__init__(value)
        self.entered = entered
        self.release = release

    def guard_current(self, value: object, *, callback):
        if not self.current or value is not self.value:
            return None
        self.guard_calls += 1
        self.entered.set()
        assert self.release.wait(timeout=5)
        return callback()


def _policy(*, version: str = "v1", content_hash: str = POLICY_HASH) -> ResolvedPolicy:
    return ResolvedPolicy(
        version,
        content_hash,
        POLICY_MARKER_HASH,
        NOW,
        freeze_json({"policy": version}),
        freeze_json({"source": "locked"}),
    )


def _authority() -> RiskTierAuthority:
    return RiskTierAuthority.normal(RISK_CONTRACT_HASH)


def _cost(*, version: str = "v1", content_hash: str = COST_HASH) -> dict[str, str]:
    return {"cost_version": version, "cost_hash": content_hash}


def _proposal(candidate_id: str = "candidate-1") -> dict[str, object]:
    return {
        "schema": "options_copilot.proposal.v1",
        "proposal_id": candidate_id,
        "candidate_id": candidate_id,
        "underlying": "SPY",
        "legs": [
            {
                "con_id": CONTRACT_IDS[0],
                "side": "BUY",
                "quantity": 1,
                "multiplier": 100,
                "bid": Decimal("1.20"),
                "ask": Decimal("1.25"),
            },
            {
                "con_id": CONTRACT_IDS[1],
                "side": "SELL",
                "quantity": 1,
                "multiplier": 100,
                "bid": Decimal("0.50"),
                "ask": Decimal("0.55"),
            },
        ],
        "risk": {
            "defined_risk": True,
            "tier": "NORMAL",
            "max_loss_usd": Decimal("75"),
        },
        "execution_cost_contract_version": "v1",
        "execution_cost_contract_hash": COST_HASH,
    }


def _candidate(
    candidate_id: str = "candidate-1",
    *,
    strategy_nav_changes: dict[str, object] | None = None,
) -> dict[str, object]:
    candidate_body = {
        "candidate_id": candidate_id,
        "underlying": "SPY",
        "structure": "DEBIT_VERTICAL",
        "thesis": "UP",
        "legs": _proposal(candidate_id)["legs"],
        "strategy_nav_hash": NAV_AUTHORITY_HASH,
        "strategy_nav_content_hash": NAV_CONTENT_HASH,
        "strategy_nav_contract_hash": NAV_CONTRACT_HASH,
        "strategy_nav_ledger_head_hash": NAV_LEDGER_HASH,
        "strategy_nav_usd": Decimal("25000"),
        "strategy_nav_observed_account_nlv": Decimal("100000"),
        "strategy_nav_reconciliation_difference": Decimal("75000"),
        "strategy_nav_asof": NOW - timedelta(minutes=30),
    }
    candidate_body.update(strategy_nav_changes or {})
    proposal_body = _proposal(candidate_id)
    evidence = {
        "input_hash": INPUT_HASH,
        "evidence_hash": EVIDENCE_HASH,
        "broker_snapshot_hash": BROKER_HASH,
    }
    basis = build_ranking_basis(
        candidate_body=candidate_body,
        proposal_body=proposal_body,
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=evidence,
    )
    return {
        "candidate_id": candidate_id,
        "candidate_body": candidate_body,
        "proposal_body": proposal_body,
        "candidate_hash": basis.candidate_hash,
        "proposal_hash": basis.proposal_hash,
        "ranking_basis_hash": basis.ranking_basis_hash,
        "evidence_inputs": evidence,
        "authority_status": "NORMAL",
        "authorizable": True,
        "score_components": {"net_ev": Decimal("10")},
    }


def _append(
    store: RankingStore,
    scan_run_id: str = "scan-1",
    *,
    candidate_id: str = "candidate-1",
    now: datetime = NOW,
    strategy_nav_changes: dict[str, object] | None = None,
):
    policy, authority = _policy(), _authority()
    return store.append_snapshot(
        scan_run_id=scan_run_id,
        input_hash=INPUT_HASH,
        evidence_hash=EVIDENCE_HASH,
        broker_snapshot_hash=BROKER_HASH,
        candidates=(
            _candidate(
                candidate_id,
                strategy_nav_changes=strategy_nav_changes,
            ),
        ),
        valid_until=now + timedelta(minutes=10),
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        risk_authority_version="v1",
        risk_authority_marker_hash=authority.marker_hash,
        policy_resolver=_Resolver(policy),
        risk_authority_resolver=_Resolver(authority),
        resolved_policy=policy,
        risk_authority=authority,
        now=now,
    )


def _ports() -> tuple[_Resolver, _Resolver, _Resolver]:
    return _Resolver(_policy()), _Resolver(_authority()), _Resolver(_cost())


def _broker_proof(
    *,
    observed_at: datetime = NOW,
    account_nlv_usd: object = "100000",
    **changes: object,
) -> dict[str, object]:
    proof: dict[str, object] = {
        "schema": BROKER_PROOF_SCHEMA,
        "ranking_snapshot_id": "",
        "candidate_id": "candidate-1",
        "proposal_hash": "",
        "snapshot_hash": ATOMIC_SNAPSHOT_HASH,
        "built_at": observed_at,
        "quote_batch_id": "quote-batch-1",
        "oldest_quote_observed_at": observed_at,
        "state_hashes": {
            "account": "9" * 64,
            "positions": "a" * 64,
            "working_orders": "b" * 64,
            "unsubmitted_instructions": "c" * 64,
        },
        "contract_definitions_hash": CONTRACT_DEFINITIONS_HASH,
        "quotes_hash": QUOTES_HASH,
        "contract_ids": list(CONTRACT_IDS),
        "account_nlv_usd": account_nlv_usd,
        "open_option_position_count": 0,
        "working_order_count": 0,
        "unsubmitted_instruction_count": 0,
        "status": "COMPLETE",
    }
    proof.update(changes)
    return proof


def _strategy_nav_proof(
    *,
    asof: datetime = NOW - timedelta(minutes=30),
    observed_account_nlv: object = "100000",
    **changes: object,
) -> dict[str, object]:
    proof: dict[str, object] = {
        "schema": STRATEGY_NAV_PROOF_SCHEMA,
        "content_hash": NAV_CONTENT_HASH,
        "authority_hash": NAV_AUTHORITY_HASH,
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
        "strategy_nav_usd": "25000",
        "observed_account_nlv": observed_account_nlv,
        "reconciliation_difference": str(
            Decimal(str(observed_account_nlv)) - Decimal("25000")
        ),
        "asof": asof,
    }
    proof.update(changes)
    if "snapshot_payload" not in changes:
        proof["snapshot_payload"] = {
            **NAV_SNAPSHOT_PAYLOAD,
            "asof": proof["asof"],
            "strategy_nav": Decimal(str(proof["strategy_nav_usd"])),
            "observed_account_nlv": Decimal(
                str(proof["observed_account_nlv"])
            ),
            "reconciliation_difference": Decimal(
                str(proof["reconciliation_difference"])
            ),
            "contract_hash": proof["contract_hash"],
            "ledger_head_hash": proof["ledger_head_hash"],
        }
    return proof


def _bound_broker_proof(
    snapshot_id: str,
    *,
    candidate_id: str = "candidate-1",
    observed_at: datetime = NOW,
    **changes: object,
) -> dict[str, object]:
    proof = _broker_proof(
        observed_at=observed_at,
        ranking_snapshot_id=snapshot_id,
        candidate_id=candidate_id,
        proposal_hash=canonical_hash(_proposal(candidate_id)),
    )
    proof.update(changes)
    return proof


def _create_challenge(
    approvals: ProposalApprovalStore,
    rankings: RankingStore,
    snapshot_id: str,
    *,
    broker_proof: dict[str, object] | None = None,
    strategy_nav_proof: dict[str, object] | None = None,
):
    policy, risk, cost = _ports()
    nav_proof = (
        _strategy_nav_proof()
        if strategy_nav_proof is None
        else strategy_nav_proof
    )
    nav_source = _NavGuard(nav_proof)
    issued = approvals.create_challenge(
        snapshot_id,
        "candidate-1",
        ranking_store=rankings,
        policy_resolver=policy,
        risk_authority_resolver=risk,
        execution_cost_contract=cost,
        broker_proof=(
            _bound_broker_proof(snapshot_id)
            if broker_proof is None
            else broker_proof
        ),
        strategy_nav_proof=nav_proof,
        strategy_nav_source=nav_source,
        strategy_nav_snapshot=nav_proof,
        now=NOW,
    )
    assert policy.resolve_calls == risk.resolve_calls == cost.resolve_calls == 1
    return issued


def _confirm(
    approvals: ProposalApprovalStore,
    rankings: RankingStore,
    challenge_id: str,
    challenge_response: str,
    *,
    policy: object | None = None,
    risk: object | None = None,
    cost: object | None = None,
    broker_proof: dict[str, object] | None = None,
    strategy_nav_proof: dict[str, object] | None = None,
):
    challenge = approvals.get_challenge(challenge_id)
    assert challenge is not None
    nav_proof = (
        _strategy_nav_proof()
        if strategy_nav_proof is None
        else strategy_nav_proof
    )
    return approvals.confirm_challenge(
        challenge_id,
        challenge_response=challenge_response,
        risk_acknowledged=True,
        second_confirmation=True,
        confirmation_token=CONFIRMATION_TOKEN,
        ranking_store=rankings,
        policy_resolver=policy or _Resolver(_policy()),
        risk_authority_resolver=risk or _Resolver(_authority()),
        execution_cost_contract=cost or _Resolver(_cost()),
        broker_proof=(
            _bound_broker_proof(challenge.ranking_snapshot_id)
            if broker_proof is None
            else broker_proof
        ),
        strategy_nav_proof=nav_proof,
        strategy_nav_source=_NavGuard(nav_proof),
        strategy_nav_snapshot=nav_proof,
        now=NOW,
    )


def test_challenge_survives_restart_and_confirmation_copies_frozen_authority(
    tmp_path: Path,
) -> None:
    ranking_path = tmp_path / "rankings.sqlite"
    approval_path = tmp_path / "approvals.sqlite"
    with RankingStore(ranking_path) as rankings:
        snapshot = _append(rankings)
        with ProposalApprovalStore(approval_path, clock=_Clock()) as approvals:
            issued = _create_challenge(
                approvals, rankings, snapshot.ranking_snapshot_id
            )
            assert issued.challenge.candidate_body["candidate_id"] == "candidate-1"
            assert issued.challenge.proposal_body["legs"]
            assert issued.challenge.broker_proof["contract_ids"] == CONTRACT_IDS
            assert issued.challenge.strategy_nav_proof["content_hash"] == NAV_CONTENT_HASH

    with RankingStore(ranking_path) as rankings, ProposalApprovalStore(
        approval_path, clock=_Clock()
    ) as approvals:
        persisted = approvals.get_challenge(issued.challenge.challenge_id)
        assert persisted == issued.challenge
        confirmation = _confirm(
            approvals,
            rankings,
            issued.challenge.challenge_id,
            issued.challenge_response,
        )
        binding = confirmation.authority_binding
        assert binding.snapshot_hash == snapshot.snapshot_hash
        assert binding.ranking_basis_hash == issued.challenge.ranking_basis_hash
        assert binding.current_policy_hash == POLICY_HASH
        assert binding.cost_hash == COST_HASH
        assert binding.risk_authority_marker_hash == _authority().marker_hash
        assert binding.challenge_broker_proof == issued.challenge.broker_proof
        assert binding.confirm_broker_proof["snapshot_hash"] == ATOMIC_SNAPSHOT_HASH
        assert binding.strategy_nav_proof == issued.challenge.strategy_nav_proof
        assert confirmation.approval.proposal == issued.challenge.proposal_body
        assert approvals.record_counts() == {
            "proposal_approvals": 1,
            "approval_consumptions": 0,
            "approval_challenges": 1,
            "approval_challenge_consumptions": 1,
            "approval_authority_bindings": 1,
            "approval_challenge_proofs": 1,
            "approval_authority_proofs": 1,
        }


def test_proof_schemas_reject_missing_extra_and_nested_unknown_fields(
    tmp_path: Path,
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        broker = _bound_broker_proof(snapshot.ranking_snapshot_id)
        nav = _strategy_nav_proof()
        cases: list[tuple[dict[str, object], dict[str, object]]] = []
        for field in ("quotes_hash", "status"):
            changed = dict(broker)
            changed.pop(field)
            cases.append((changed, nav))
        changed = dict(broker)
        changed["attacker"] = True
        cases.append((changed, nav))
        changed = dict(broker)
        changed["state_hashes"] = {
            **dict(broker["state_hashes"]),
            "attacker": "d" * 64,
        }
        cases.append((changed, nav))
        for field in ("content_hash", "asof"):
            changed_nav = dict(nav)
            changed_nav.pop(field)
            cases.append((broker, changed_nav))
        changed_nav = dict(nav)
        changed_nav["attacker"] = True
        cases.append((broker, changed_nav))

        for bad_broker, bad_nav in cases:
            with pytest.raises(ApprovalChallengeRejected, match="fields"):
                _create_challenge(
                    approvals,
                    rankings,
                    snapshot.ranking_snapshot_id,
                    broker_proof=bad_broker,
                    strategy_nav_proof=bad_nav,
                )
        assert approvals.record_counts()["approval_challenges"] == 0
        assert approvals.record_counts()["approval_challenge_proofs"] == 0


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"ranking_snapshot_id": "ranking-attacker"}, "ranking_snapshot_id"),
        ({"candidate_id": "candidate-attacker"}, "candidate_id"),
        ({"proposal_hash": "f" * 64}, "proposal_hash"),
        ({"snapshot_hash": "F" * 64}, "snapshot_hash"),
        ({"contract_definitions_hash": "x" * 64}, "contract_definitions_hash"),
        ({"quotes_hash": "0" * 63}, "quotes_hash"),
        ({"contract_ids": [101, 999]}, "contract_ids"),
        ({"contract_ids": [101, 101]}, "contract_ids"),
        ({"contract_ids": [True, 102]}, "contract_ids"),
    ),
)
def test_broker_proof_identifier_hash_and_contract_tampering_fails_closed(
    tmp_path: Path, changes: dict[str, object], message: str
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        proof = _bound_broker_proof(snapshot.ranking_snapshot_id, **changes)
        with pytest.raises(ApprovalChallengeRejected, match=message):
            _create_challenge(
                approvals,
                rankings,
                snapshot.ranking_snapshot_id,
                broker_proof=proof,
            )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("built_at", NOW - timedelta(seconds=6), "stale"),
        ("built_at", NOW + timedelta(microseconds=1), "future"),
        ("oldest_quote_observed_at", NOW - timedelta(seconds=6), "stale"),
        (
            "oldest_quote_observed_at",
            NOW + timedelta(microseconds=1),
            "future",
        ),
        ("open_option_position_count", 1, "integer zero"),
        ("working_order_count", 1, "integer zero"),
        ("unsubmitted_instruction_count", 1, "integer zero"),
        ("working_order_count", False, "integer zero"),
        ("status", "PARTIAL", "COMPLETE"),
    ),
)
def test_broker_proof_freshness_zero_state_and_complete_status_are_required(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        proof = _bound_broker_proof(snapshot.ranking_snapshot_id, **{field: value})
        with pytest.raises(ApprovalChallengeRejected, match=message):
            _create_challenge(
                approvals,
                rankings,
                snapshot.ranking_snapshot_id,
                broker_proof=proof,
            )


@pytest.mark.parametrize(
    ("nav_changes", "broker_changes", "message"),
    (
        ({"authority_hash": "d" * 64}, {}, "authority_hash"),
        ({"contract_hash": "e" * 64}, {}, "authority_hash"),
        ({"asof": NOW + timedelta(microseconds=1)}, {}, "future"),
        ({"observed_account_nlv": "99999"}, {}, "account_nlv"),
        ({}, {"account_nlv_usd": "99999"}, "account_nlv"),
    ),
)
def test_strategy_nav_proof_must_match_candidate_time_and_broker_nlv(
    tmp_path: Path,
    nav_changes: dict[str, object],
    broker_changes: dict[str, object],
    message: str,
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        with pytest.raises(ApprovalChallengeRejected, match=message):
            _create_challenge(
                approvals,
                rankings,
                snapshot.ranking_snapshot_id,
                broker_proof=_bound_broker_proof(
                    snapshot.ranking_snapshot_id, **broker_changes
                ),
                strategy_nav_proof=_strategy_nav_proof(**nav_changes),
            )


@pytest.mark.parametrize(
    ("candidate_field", "candidate_value", "message"),
    (
        ("strategy_nav_content_hash", "6" * 64, "content_hash"),
        ("strategy_nav_hash", "7" * 64, "strategy_nav_hash"),
        ("strategy_nav_contract_hash", "8" * 64, "contract_hash"),
        ("strategy_nav_ledger_head_hash", "9" * 64, "ledger_head_hash"),
        ("strategy_nav_usd", Decimal("25001"), "strategy_nav_usd"),
        (
            "strategy_nav_observed_account_nlv",
            Decimal("100001"),
            "observed_account_nlv",
        ),
        (
            "strategy_nav_reconciliation_difference",
            Decimal("75001"),
            "reconciliation_difference",
        ),
        (
            "strategy_nav_asof",
            NOW - timedelta(minutes=29),
            "strategy_nav_asof",
        ),
    ),
)
def test_challenge_rejects_each_frozen_strategy_nav_identity_field_drift(
    tmp_path: Path,
    candidate_field: str,
    candidate_value: object,
    message: str,
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(
            rankings,
            strategy_nav_changes={candidate_field: candidate_value},
        )
        with pytest.raises(ApprovalChallengeRejected, match=message):
            _create_challenge(
                approvals,
                rankings,
                snapshot.ranking_snapshot_id,
            )


def test_confirmation_rechecks_proofs_rejects_nav_drift_and_persists_new_broker(
    tmp_path: Path,
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        challenge_broker = _bound_broker_proof(
            snapshot.ranking_snapshot_id,
            observed_at=NOW - timedelta(seconds=1),
        )
        issued = _create_challenge(
            approvals,
            rankings,
            snapshot.ranking_snapshot_id,
            broker_proof=challenge_broker,
        )
        with pytest.raises(ApprovalChallengeRejected, match="authority"):
            _confirm(
                approvals,
                rankings,
                issued.challenge.challenge_id,
                issued.challenge_response,
                strategy_nav_proof=_strategy_nav_proof(strategy_nav_usd="25001"),
            )
        with pytest.raises(ApprovalChallengeRejected, match="account_nlv"):
            _confirm(
                approvals,
                rankings,
                issued.challenge.challenge_id,
                issued.challenge_response,
                broker_proof=_bound_broker_proof(
                    snapshot.ranking_snapshot_id,
                    account_nlv_usd="100001",
                ),
            )

        confirm_broker = _bound_broker_proof(
            snapshot.ranking_snapshot_id,
            snapshot_hash="d" * 64,
            quote_batch_id="quote-batch-2",
            state_hashes={
                "account": "e" * 64,
                "positions": "a" * 64,
                "working_orders": "b" * 64,
                "unsubmitted_instructions": "c" * 64,
            },
            account_nlv_usd="100000",
        )
        with pytest.raises(ApprovalChallengeRejected, match="content_hash"):
            _confirm(
                approvals,
                rankings,
                issued.challenge.challenge_id,
                issued.challenge_response,
                broker_proof=confirm_broker,
                strategy_nav_proof=_strategy_nav_proof(content_hash="d" * 64),
            )

        confirm_nav = _strategy_nav_proof()
        confirmed = _confirm(
            approvals,
            rankings,
            issued.challenge.challenge_id,
            issued.challenge_response,
            broker_proof=confirm_broker,
            strategy_nav_proof=confirm_nav,
        )
        assert confirmed.authority_binding.challenge_broker_proof == (
            issued.challenge.broker_proof
        )
        assert confirmed.authority_binding.confirm_broker_proof["snapshot_hash"] == (
            "d" * 64
        )
        assert confirmed.authority_binding.strategy_nav_proof["content_hash"] == (
            NAV_CONTENT_HASH
        )
        assert confirmed.authority_binding.confirm_broker_proof != (
            confirmed.authority_binding.challenge_broker_proof
        )


def test_challenge_wrong_expired_replayed_and_tampered_responses_fail_closed(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=clock
    ) as approvals:
        snapshot = _append(rankings)
        wrong = _create_challenge(approvals, rankings, snapshot.ranking_snapshot_id)
        with pytest.raises(ApprovalChallengeRejected, match="response"):
            _confirm(approvals, rankings, wrong.challenge.challenge_id, "x" * 32)
        assert approvals.record_counts()["proposal_approvals"] == 0

        expired = _create_challenge(approvals, rankings, snapshot.ranking_snapshot_id)
        clock.value = NOW + timedelta(minutes=6)
        expired_nav = _strategy_nav_proof()
        with pytest.raises(ApprovalChallengeRejected, match="expired"):
            approvals.confirm_challenge(
                expired.challenge.challenge_id,
                challenge_response=expired.challenge_response,
                risk_acknowledged=True,
                second_confirmation=True,
                confirmation_token=CONFIRMATION_TOKEN,
                ranking_store=rankings,
                policy_resolver=_Resolver(_policy()),
                risk_authority_resolver=_Resolver(_authority()),
                execution_cost_contract=_Resolver(_cost()),
                broker_proof=_bound_broker_proof(
                    snapshot.ranking_snapshot_id,
                    observed_at=clock.value,
                ),
                strategy_nav_proof=expired_nav,
                strategy_nav_source=_NavGuard(expired_nav),
                strategy_nav_snapshot=expired_nav,
                now=clock.value,
            )
        clock.value = NOW

        replay = _create_challenge(approvals, rankings, snapshot.ranking_snapshot_id)
        _confirm(
            approvals,
            rankings,
            replay.challenge.challenge_id,
            replay.challenge_response,
        )
        with pytest.raises(ApprovalChallengeRejected, match="consumed"):
            _confirm(
                approvals,
                rankings,
                replay.challenge.challenge_id,
                replay.challenge_response,
            )

        with pytest.raises(sqlite3.IntegrityError, match="immutable approval challenge"):
            approvals._connection.execute(
                "UPDATE approval_challenges SET candidate_id='attacker' "
                "WHERE challenge_id=?",
                (replay.challenge.challenge_id,),
            )


@pytest.mark.parametrize(
    "changed_port",
    ("policy", "risk", "cost", "cost_without_current_proof"),
)
def test_confirmation_revalidates_every_current_authority(
    tmp_path: Path, changed_port: str
) -> None:
    with RankingStore(tmp_path / f"rank-{changed_port}.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / f"approval-{changed_port}.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        issued = _create_challenge(approvals, rankings, snapshot.ranking_snapshot_id)
        policy: object = _Resolver(_policy())
        risk: object = _Resolver(_authority())
        cost: object = _Resolver(_cost())
        if changed_port == "policy":
            policy = _Resolver(_policy(version="v2", content_hash="f" * 64))
        elif changed_port == "risk":
            risk = _Resolver(RiskTierAuthority.normal("3" * 64))
        elif changed_port == "cost":
            cost = _Resolver(_cost(version="v2", content_hash="4" * 64))
        else:
            cost = _ResolveOnly(_cost())

        with pytest.raises(ApprovalChallengeRejected, match="current rank-one"):
            _confirm(
                approvals,
                rankings,
                issued.challenge.challenge_id,
                issued.challenge_response,
                policy=policy,
                risk=risk,
                cost=cost,
            )
        assert approvals.record_counts()["proposal_approvals"] == 0
        assert approvals.record_counts()["approval_challenge_consumptions"] == 0


def test_challenge_creation_requires_a_current_matching_cost_contract(
    tmp_path: Path,
) -> None:
    with RankingStore(tmp_path / "rank.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approval.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        for cost in (
            _ResolveOnly(_cost()),
            _Resolver(_cost(version="v2", content_hash="4" * 64)),
        ):
            nav_proof = _strategy_nav_proof()
            with pytest.raises(ApprovalChallengeRejected, match="current rank-one"):
                approvals.create_challenge(
                    snapshot.ranking_snapshot_id,
                    "candidate-1",
                    ranking_store=rankings,
                    policy_resolver=_Resolver(_policy()),
                    risk_authority_resolver=_Resolver(_authority()),
                    execution_cost_contract=cost,
                    broker_proof=_bound_broker_proof(snapshot.ranking_snapshot_id),
                    strategy_nav_proof=nav_proof,
                    strategy_nav_source=_NavGuard(nav_proof),
                    strategy_nav_snapshot=nav_proof,
                    now=NOW,
                )
        assert approvals.record_counts()["approval_challenges"] == 0


def test_new_head_and_no_trade_supersession_reject_confirmation(tmp_path: Path) -> None:
    for terminal in ("new_head", "no_trade"):
        with RankingStore(tmp_path / f"rank-{terminal}.sqlite") as rankings, ProposalApprovalStore(
            tmp_path / f"approval-{terminal}.sqlite", clock=_Clock()
        ) as approvals:
            snapshot = _append(rankings)
            issued = _create_challenge(
                approvals, rankings, snapshot.ranking_snapshot_id
            )
            if terminal == "new_head":
                _append(rankings, "scan-2")
            else:
                rankings.append_decision(
                    "scan-no-trade",
                    "NO_TRADE",
                    {"status": "NO_TRADE", "reasons": ["BROKER_CHANGED"]},
                    now=NOW,
                )
            with pytest.raises(ApprovalChallengeRejected, match="current rank-one"):
                _confirm(
                    approvals,
                    rankings,
                    issued.challenge.challenge_id,
                    issued.challenge_response,
                )
            assert approvals.record_counts()["proposal_approvals"] == 0


def test_confirmation_inserts_consumption_approval_and_binding_atomically(
    tmp_path: Path,
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        issued = _create_challenge(approvals, rankings, snapshot.ranking_snapshot_id)
        approvals._connection.execute(
            """
            CREATE TRIGGER test_reject_binding
            BEFORE INSERT ON approval_authority_bindings
            BEGIN SELECT RAISE(ABORT,'simulated binding failure'); END
            """
        )
        with pytest.raises(sqlite3.IntegrityError, match="simulated binding"):
            _confirm(
                approvals,
                rankings,
                issued.challenge.challenge_id,
                issued.challenge_response,
            )
        counts = approvals.record_counts()
        assert counts["proposal_approvals"] == 0
        assert counts["approval_authority_bindings"] == 0
        assert counts["approval_challenge_consumptions"] == 0
        approvals._connection.execute("DROP TRIGGER test_reject_binding")
        assert _confirm(
            approvals,
            rankings,
            issued.challenge.challenge_id,
            issued.challenge_response,
        ).approval.proposal_id == "candidate-1"


def test_ranking_guard_holds_head_lock_through_callback_and_propagates_failure(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rankings.sqlite"
    left, right = RankingStore(path), RankingStore(path)
    entered, release = threading.Event(), threading.Event()
    try:
        snapshot = _append(left)
        policy, risk, cost = _ports()

        def guarded() -> str | None:
            return left.guard_frozen_rank_one(
                snapshot.ranking_snapshot_id,
                "candidate-1",
                    policy_resolver=policy,
                    risk_authority_resolver=risk,
                    execution_cost_contract=cost,
                    now=NOW,
                callback=lambda authorization: (
                    entered.set(),
                    release.wait(timeout=5),
                    authorization.candidate_id,
                )[-1],
            )

        with ThreadPoolExecutor(max_workers=2) as workers:
            guard_future = workers.submit(guarded)
            assert entered.wait(timeout=5)
            append_future = workers.submit(_append, right, "scan-2")
            assert not append_future.done()
            release.set()
            assert guard_future.result(timeout=5) == "candidate-1"
            assert append_future.result(timeout=5).scan_run_id == "scan-2"

        with pytest.raises(RuntimeError, match="callback failed"):
            left.guard_frozen_rank_one(
                right.latest().ranking_snapshot_id,
                "candidate-1",
                policy_resolver=_Resolver(_policy()),
                risk_authority_resolver=_Resolver(_authority()),
                execution_cost_contract=_Resolver(_cost()),
                now=NOW,
                callback=lambda _: (_ for _ in ()).throw(
                    RuntimeError("callback failed")
                ),
            )
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize("changing_port", ("policy", "risk", "cost"))
def test_authority_head_change_after_precheck_invokes_zero_approval_callbacks(
    tmp_path: Path,
    changing_port: str,
) -> None:
    with RankingStore(tmp_path / f"rank-{changing_port}.sqlite") as rankings, (
        ProposalApprovalStore(
            tmp_path / f"approval-{changing_port}.sqlite",
            clock=_Clock(),
        )
    ) as approvals:
        snapshot = _append(rankings)
        ports: dict[str, _Resolver] = {
            "policy": _Resolver(_policy()),
            "risk": _Resolver(_authority()),
            "cost": _Resolver(_cost()),
        }
        ports[changing_port] = _FlipAfterPrecheckResolver(
            ports[changing_port].value
        )
        nav_proof = _strategy_nav_proof()
        nav_source = _NavGuard(nav_proof)

        with pytest.raises(ApprovalChallengeRejected, match="current rank-one"):
            approvals.create_challenge(
                snapshot.ranking_snapshot_id,
                "candidate-1",
                ranking_store=rankings,
                policy_resolver=ports["policy"],
                risk_authority_resolver=ports["risk"],
                execution_cost_contract=ports["cost"],
                broker_proof=_bound_broker_proof(snapshot.ranking_snapshot_id),
                strategy_nav_proof=nav_proof,
                strategy_nav_source=nav_source,
                strategy_nav_snapshot=nav_proof,
                now=NOW,
            )

        assert nav_source.guard_calls == 0
        assert approvals.record_counts()["approval_challenges"] == 0
        assert approvals.record_counts()["proposal_approvals"] == 0


def test_policy_risk_and_cost_writers_wait_for_nav_and_approval_callback(
    tmp_path: Path,
) -> None:
    with RankingStore(tmp_path / "rank-authority-locks.sqlite") as rankings, (
        ProposalApprovalStore(
            tmp_path / "approval-authority-locks.sqlite",
            clock=_Clock(),
        )
    ) as approvals:
        snapshot = _append(rankings)
        policy, risk, cost = _ports()
        nav_proof = _strategy_nav_proof()
        entered, release = threading.Event(), threading.Event()
        nav_source = _BlockingNavGuard(nav_proof, entered, release)

        def create():
            return approvals.create_challenge(
                snapshot.ranking_snapshot_id,
                "candidate-1",
                ranking_store=rankings,
                policy_resolver=policy,
                risk_authority_resolver=risk,
                execution_cost_contract=cost,
                broker_proof=_bound_broker_proof(snapshot.ranking_snapshot_id),
                strategy_nav_proof=nav_proof,
                strategy_nav_source=nav_source,
                strategy_nav_snapshot=nav_proof,
                now=NOW,
            )

        with ThreadPoolExecutor(max_workers=4) as workers:
            approval_future = workers.submit(create)
            assert entered.wait(timeout=5)
            writers = tuple(
                workers.submit(port.change_head)
                for port in (policy, risk, cost)
            )
            assert all(not writer.done() for writer in writers)
            release.set()
            assert approval_future.result(timeout=5).challenge.candidate_id == (
                "candidate-1"
            )
            for writer in writers:
                writer.result(timeout=5)

        assert approvals.record_counts()["approval_challenges"] == 1


def _create_v1_approval_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        proposal = _proposal("legacy-proposal")
        connection.executescript(
            """
            CREATE TABLE proposal_approvals (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                approval_id TEXT NOT NULL UNIQUE,
                proposal_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                material_hash TEXT NOT NULL,
                legs_hash TEXT NOT NULL,
                risk_hash TEXT NOT NULL,
                approved_by TEXT NOT NULL,
                approved_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                nonce_hash TEXT NOT NULL UNIQUE,
                adverse_tolerance_usd TEXT NOT NULL,
                reference_cost_usd TEXT,
                proposal_json TEXT NOT NULL
            );
            CREATE TABLE approval_consumptions (
                approval_id TEXT PRIMARY KEY,
                consumed_at TEXT NOT NULL,
                execution_hash TEXT NOT NULL
            );
            PRAGMA user_version=1;
            """
        )
        from options_copilot.approval.store import proposal_hashes
        from options_copilot.storage.canonical import canonical_json

        hashes, frozen = proposal_hashes(proposal)
        connection.execute(
            """
            INSERT INTO proposal_approvals VALUES(
                1,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                "legacy-approval",
                "legacy-proposal",
                hashes.proposal_hash,
                hashes.material_hash,
                hashes.legs_hash,
                hashes.risk_hash,
                "legacy-user",
                NOW.isoformat(),
                (NOW + timedelta(minutes=5)).isoformat(),
                canonical_hash("legacy-nonce"),
                "5.00",
                "75.00",
                canonical_json(frozen),
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _downgrade_v3_database_to_v2(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("DROP TABLE approval_authority_proofs")
        connection.execute("DROP TABLE approval_challenge_proofs")
        connection.execute("PRAGMA user_version=2")
        connection.commit()
    finally:
        connection.close()


def test_v1_migration_is_atomic_restart_safe_and_old_unbound_approval_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "legacy.sqlite"
    _create_v1_approval_database(path)
    original = ProposalApprovalStore._create_v2_schema_in_transaction

    def fail_mid_migration(self: ProposalApprovalStore, source_version: int) -> None:
        self._connection.execute("CREATE TABLE migration_should_rollback(value TEXT)")
        raise RuntimeError("simulated migration interruption")

    monkeypatch.setattr(
        ProposalApprovalStore,
        "_create_v2_schema_in_transaction",
        fail_mid_migration,
    )
    with pytest.raises(RuntimeError, match="simulated migration"):
        ProposalApprovalStore(path, clock=_Clock())

    probe = sqlite3.connect(path)
    try:
        assert probe.execute("PRAGMA user_version").fetchone()[0] == 1
        names = {
            row[0]
            for row in probe.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "migration_should_rollback" not in names
        assert "approval_challenges" not in names
    finally:
        probe.close()

    monkeypatch.setattr(
        ProposalApprovalStore,
        "_create_v2_schema_in_transaction",
        original,
    )
    with ProposalApprovalStore(path, clock=_Clock()) as restarted:
        assert restarted.schema_version == 3
        validation = restarted.validate(
            "legacy-approval", _proposal("legacy-proposal"), checked_at=NOW
        )
        assert validation.valid is False
        assert "approval_authority_unbound" in validation.reasons
        assert restarted._connection.execute(
            "SELECT COUNT(*) FROM approval_legacy_unbound"
        ).fetchone()[0] == 1

    with ProposalApprovalStore(path, clock=_Clock()) as restarted_again:
        assert restarted_again.schema_version == 3
        assert restarted_again._connection.execute(
            "SELECT COUNT(*) FROM approval_legacy_unbound"
        ).fetchone()[0] == 1


def test_v2_to_v3_migration_is_atomic_idempotent_and_proofless_rows_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ranking_path = tmp_path / "rankings.sqlite"
    approval_path = tmp_path / "approvals.sqlite"
    with RankingStore(ranking_path) as rankings, ProposalApprovalStore(
        approval_path, clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        issued = _create_challenge(
            approvals, rankings, snapshot.ranking_snapshot_id
        )
        confirmed = _confirm(
            approvals,
            rankings,
            issued.challenge.challenge_id,
            issued.challenge_response,
        )
        unconsumed = _create_challenge(
            approvals, rankings, snapshot.ranking_snapshot_id
        )
        approval_id = confirmed.approval.approval_id
        proposal = confirmed.approval.proposal
    _downgrade_v3_database_to_v2(approval_path)

    original = ProposalApprovalStore._create_v3_schema_in_transaction

    def fail_mid_migration(self: ProposalApprovalStore, source_version: int) -> None:
        self._create_v2_schema_in_transaction(source_version)
        self._connection.execute("CREATE TABLE migration_should_rollback(value TEXT)")
        raise RuntimeError("simulated v3 migration interruption")

    monkeypatch.setattr(
        ProposalApprovalStore, "_create_v3_schema_in_transaction", fail_mid_migration
    )
    with pytest.raises(RuntimeError, match="simulated v3 migration"):
        ProposalApprovalStore(approval_path, clock=_Clock())
    probe = sqlite3.connect(approval_path)
    try:
        assert probe.execute("PRAGMA user_version").fetchone()[0] == 2
        names = {
            str(row[0])
            for row in probe.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "migration_should_rollback" not in names
        assert "approval_challenge_proofs" not in names
    finally:
        probe.close()

    monkeypatch.setattr(
        ProposalApprovalStore, "_create_v3_schema_in_transaction", original
    )
    with RankingStore(ranking_path) as rankings, ProposalApprovalStore(
        approval_path, clock=_Clock()
    ) as migrated:
        assert migrated.schema_version == 3
        assert migrated.get_challenge(unconsumed.challenge.challenge_id) is None
        assert migrated.get_authority_binding(approval_id) is None
        validation = migrated.validate(
            approval_id,
            proposal,
            checked_at=NOW,
            require_authority_binding=True,
        )
        assert validation.valid is False
        assert "approval_authority_unbound" in validation.reasons
        nav_proof = _strategy_nav_proof()
        with pytest.raises(ApprovalChallengeRejected, match="not found"):
            migrated.confirm_challenge(
                unconsumed.challenge.challenge_id,
                challenge_response=unconsumed.challenge_response,
                risk_acknowledged=True,
                second_confirmation=True,
                confirmation_token=CONFIRMATION_TOKEN,
                ranking_store=rankings,
                policy_resolver=_Resolver(_policy()),
                risk_authority_resolver=_Resolver(_authority()),
                execution_cost_contract=_Resolver(_cost()),
                broker_proof=_bound_broker_proof(snapshot.ranking_snapshot_id),
                strategy_nav_proof=nav_proof,
                strategy_nav_source=_NavGuard(nav_proof),
                strategy_nav_snapshot=nav_proof,
                now=NOW,
            )
        assert migrated._connection.execute(
            "SELECT COUNT(*) FROM approval_challenge_proofs"
        ).fetchone()[0] == 0
        assert migrated._connection.execute(
            "SELECT COUNT(*) FROM approval_authority_proofs"
        ).fetchone()[0] == 0

    with ProposalApprovalStore(approval_path, clock=_Clock()) as reopened:
        assert reopened.schema_version == 3
        assert reopened.get_authority_binding(approval_id) is None


def test_proof_rows_are_immutable_and_hash_verified_on_read(tmp_path: Path) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        issued = _create_challenge(
            approvals, rankings, snapshot.ranking_snapshot_id
        )
        confirmed = _confirm(
            approvals,
            rankings,
            issued.challenge.challenge_id,
            issued.challenge_response,
        )
        with pytest.raises(sqlite3.IntegrityError, match="immutable approval challenge proof"):
            approvals._connection.execute(
                "UPDATE approval_challenge_proofs SET broker_proof_hash=? "
                "WHERE challenge_id=?",
                ("f" * 64, issued.challenge.challenge_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable approval authority proof"):
            approvals._connection.execute(
                "DELETE FROM approval_authority_proofs WHERE approval_id=?",
                (confirmed.approval.approval_id,),
            )

        approvals._connection.execute(
            "DROP TRIGGER approval_challenge_proofs_no_update"
        )
        approvals._connection.execute(
            "UPDATE approval_challenge_proofs SET broker_proof_hash=? "
            "WHERE challenge_id=?",
            ("f" * 64, issued.challenge.challenge_id),
        )
        with pytest.raises(ApprovalError, match="proof.*corrupt"):
            approvals.get_challenge(issued.challenge.challenge_id)

        approvals._connection.execute(
            "DROP TRIGGER approval_authority_proofs_no_update"
        )
        approvals._connection.execute(
            "UPDATE approval_authority_proofs SET confirm_broker_proof_hash=? "
            "WHERE approval_id=?",
            ("f" * 64, confirmed.approval.approval_id),
        )
        with pytest.raises(ApprovalError, match="proof.*corrupt"):
            approvals.get_authority_binding(confirmed.approval.approval_id)


def test_create_challenge_callback_failure_leaves_no_challenge_or_approval_rows(
    tmp_path: Path,
) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as rankings, ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=_Clock()
    ) as approvals:
        snapshot = _append(rankings)
        approvals._connection.execute(
            """
            CREATE TRIGGER test_reject_challenge
            BEFORE INSERT ON approval_challenges
            BEGIN SELECT RAISE(ABORT,'simulated challenge failure'); END
            """
        )
        policy, risk, cost = _ports()
        nav_proof = _strategy_nav_proof()
        with pytest.raises(sqlite3.IntegrityError, match="simulated challenge"):
            approvals.create_challenge(
                snapshot.ranking_snapshot_id,
                "candidate-1",
                ranking_store=rankings,
                policy_resolver=policy,
                risk_authority_resolver=risk,
                execution_cost_contract=cost,
                broker_proof=_bound_broker_proof(snapshot.ranking_snapshot_id),
                strategy_nav_proof=nav_proof,
                strategy_nav_source=_NavGuard(nav_proof),
                strategy_nav_snapshot=nav_proof,
                now=NOW,
            )
        assert approvals.record_counts()["approval_challenges"] == 0
        assert approvals.record_counts()["proposal_approvals"] == 0


def test_v2_direct_approval_cannot_create_any_bridge_claim_rows(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    with ProposalApprovalStore(
        tmp_path / "approvals.sqlite", clock=clock
    ) as approvals:
        direct = approvals.approve(
            "candidate-1",
            _proposal(),
            nonce="direct-approval-is-not-authority-bound",
            approved_by="options_copilot_gui",
            approved_at=NOW,
        )
        assert approvals.validate(
            direct.approval_id,
            direct.proposal,
            checked_at=NOW,
        ).valid
        assert not approvals.validate(
            direct.approval_id,
            direct.proposal,
            checked_at=NOW,
            require_authority_binding=True,
        ).valid

        with CodexBridgeStore(
            tmp_path / "bridge.sqlite", approvals, clock=clock
        ) as bridge:
            with pytest.raises(
                BridgeApprovalRejected, match="approval_authority_unbound"
            ):
                bridge.claim(direct.approval_id)
            for table in (
                "codex_bridge_requests",
                "codex_bridge_external_attempts",
                "codex_bridge_broker_gates",
                "codex_bridge_atomic_authorization_gates",
                "codex_bridge_reserve_gates",
            ):
                assert bridge._connection.execute(
                    f'SELECT COUNT(*) FROM "{table}"'
                ).fetchone()[0] == 0
