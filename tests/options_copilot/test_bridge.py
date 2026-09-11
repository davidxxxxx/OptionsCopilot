from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import threading
from unittest.mock import patch

import pytest

from options_copilot.analytics.scenarios import ResolvedPolicy
from options_copilot.approval import (
    ApprovalAuthorityBinding,
    BROKER_PROOF_SCHEMA,
    STRATEGY_NAV_PROOF_SCHEMA,
    ProposalApprovalStore,
)
from options_copilot.bridge import (
    CURRENT_AUTHORITY_PROOF_SCHEMA,
    BridgeActiveHandoffExists,
    BridgeAlreadyClaimed,
    BridgeApprovalRejected,
    BridgeStateError,
    BridgeStatus,
    BridgeTokenError,
    BridgeValidationError,
    CodexBridgeStore,
    CurrentAuthorityProof,
    LocalCodexBridgeCoordinator,
)
from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.ranking.store import RankingStore
from options_copilot.risk.authorization import RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash, freeze_json


BASE = datetime(2026, 8, 3, 2, 3, 4, tzinfo=timezone.utc)
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
ATOMIC_SNAPSHOT_HASH = "6" * 64
CONTRACT_DEFINITIONS_HASH = "7" * 64
QUOTES_HASH = "8" * 64
APPROVAL_CONTRACT_IDS = (101, 102)


class MutableClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current

    def set(self, current: datetime) -> None:
        self.current = current


class CurrentResolver:
    def __init__(self, value: object) -> None:
        self.value = value

    def resolve(self, **_: object) -> object:
        return self.value

    def is_current(self, value: object) -> bool:
        return value == self.value

    def guard_current(self, value: object, *, callback):
        if not self.is_current(value):
            return None
        return callback()


def proposal() -> dict[str, object]:
    return {
        "proposal_id": "proposal-bridge-1",
        "underlying": "SPY",
        "strategy": "DEBIT_CALL_SPREAD",
        "quote_snapshot_id": "store-test-quotes-1",
        "legs": [
            {
                "contract_id_ex": "SPY-20260918-C-500@SMART",
                "side": "BUY",
                "quantity": 1,
                "strike": Decimal("500"),
                "ask": Decimal("5.10"),
            },
            {
                "contract_id_ex": "SPY-20260918-C-510@SMART",
                "side": "SELL",
                "quantity": 1,
                "strike": Decimal("510"),
                "bid": Decimal("4.10"),
            },
        ],
        "risk": {
            "maximum_loss_usd": Decimal("100"),
            "maximum_profit_usd": Decimal("900"),
            "defined_risk": True,
        },
        "pricing": {
            "reference_cost_usd": Decimal("100"),
            "net_debit_usd": Decimal("100"),
        },
    }


def credit_proposal() -> dict[str, object]:
    credit = proposal()
    credit["proposal_id"] = "proposal-credit-1"
    credit["strategy"] = "CREDIT_CALL_SPREAD"
    credit["legs"][0]["side"] = "SELL"
    credit["legs"][0]["bid"] = Decimal("5.10")
    credit["legs"][1]["side"] = "BUY"
    credit["legs"][1]["ask"] = Decimal("4.10")
    credit["pricing"]["reference_cost_usd"] = Decimal("-100")
    credit["pricing"].pop("net_debit_usd")
    credit["pricing"]["net_credit_usd"] = Decimal("100")
    return credit


def second_proposal() -> dict[str, object]:
    second = proposal()
    second["proposal_id"] = "proposal-bridge-2"
    return second


def approval_proofs(
    *,
    ranking_snapshot_id: str,
    candidate_id: str,
    proposal_hash: str,
    observed_at: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    snapshot_payload = _strategy_nav_snapshot_payload(
        asof=observed_at,
        strategy_nav=Decimal("25000"),
        observed_account_nlv=Decimal("100000"),
    )
    broker_proof: dict[str, object] = {
        "schema": BROKER_PROOF_SCHEMA,
        "ranking_snapshot_id": ranking_snapshot_id,
        "candidate_id": candidate_id,
        "proposal_hash": proposal_hash,
        "snapshot_hash": ATOMIC_SNAPSHOT_HASH,
        "built_at": observed_at,
        "quote_batch_id": f"approval-quotes-{candidate_id}",
        "oldest_quote_observed_at": observed_at,
        "state_hashes": {
            "account": "9" * 64,
            "positions": "a" * 64,
            "working_orders": "b" * 64,
            "unsubmitted_instructions": "c" * 64,
        },
        "contract_definitions_hash": CONTRACT_DEFINITIONS_HASH,
        "quotes_hash": QUOTES_HASH,
        "contract_ids": list(APPROVAL_CONTRACT_IDS),
        "account_nlv_usd": "100000",
        "open_option_position_count": 0,
        "working_order_count": 0,
        "unsubmitted_instruction_count": 0,
        "status": "COMPLETE",
    }
    strategy_nav_proof: dict[str, object] = {
        "schema": STRATEGY_NAV_PROOF_SCHEMA,
        "content_hash": canonical_hash(snapshot_payload),
        "authority_hash": NAV_AUTHORITY_HASH,
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
        "strategy_nav_usd": "25000",
        "observed_account_nlv": "100000",
        "reconciliation_difference": "75000",
        "asof": observed_at,
        "snapshot_payload": snapshot_payload,
    }
    return broker_proof, strategy_nav_proof


def _strategy_nav_snapshot_payload(
    *,
    asof: datetime,
    strategy_nav: Decimal,
    observed_account_nlv: Decimal,
) -> dict[str, object]:
    return {
        "asof": asof,
        "strategy_nav": strategy_nav,
        "strategy_deposits": Decimal("0"),
        "strategy_withdrawals": Decimal("0"),
        "realized_pnl": Decimal("0"),
        "open_position_unrealized_pnl": Decimal("0"),
        "fees": Decimal("0"),
        "signed_corrections": Decimal("0"),
        "non_strategy_contribution": Decimal("0"),
        "fill_principal_contribution": Decimal("0"),
        "observed_account_nlv": observed_account_nlv,
        "reconciliation_difference": observed_account_nlv - strategy_nav,
        "contract_version": "v1",
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
        "valid": True,
        "no_trade_reasons": (),
    }


def current_authority_proof(
    binding: ApprovalAuthorityBinding,
    checked_at: datetime,
) -> CurrentAuthorityProof:
    return CurrentAuthorityProof(
        schema=CURRENT_AUTHORITY_PROOF_SCHEMA,
        status="CURRENT",
        checked_at=checked_at,
        ranking_snapshot_id=binding.ranking_snapshot_id,
        candidate_id=binding.candidate_id,
        proposal_hash=binding.proposal_hash,
        snapshot_hash=binding.snapshot_hash,
        current_policy_version=binding.current_policy_version,
        current_policy_hash=binding.current_policy_hash,
        policy_authority_marker_hash=binding.policy_authority_marker_hash,
        cost_version=binding.cost_version,
        cost_hash=binding.cost_hash,
        risk_contract_hash=binding.risk_contract_hash,
        risk_authority_version=binding.risk_authority_version,
        risk_authority_marker_hash=binding.risk_authority_marker_hash,
        strategy_nav_content_hash=str(
            binding.strategy_nav_proof["content_hash"]
        ),
        strategy_nav_contract_hash=str(
            binding.strategy_nav_proof["contract_hash"]
        ),
        strategy_nav_ledger_head_hash=str(
            binding.strategy_nav_proof["ledger_head_hash"]
        ),
    )


def strict_current_authority_validator(
    binding: ApprovalAuthorityBinding,
    checked_at: datetime,
) -> CurrentAuthorityProof:
    return current_authority_proof(binding, checked_at)


def create_bound_approval(
    approvals: ProposalApprovalStore,
    *,
    ranking_path: Path,
    proposal_body: dict[str, object],
    approval_id: str,
    issued_at: datetime,
) -> None:
    """Seed a bridge fixture through the real challenge/confirm authority gate."""

    proposal_id = str(proposal_body["proposal_id"])
    candidate_legs = deepcopy(proposal_body["legs"])
    for leg, con_id in zip(candidate_legs, APPROVAL_CONTRACT_IDS, strict=True):
        leg["con_id"] = con_id
    nav_payload = _strategy_nav_snapshot_payload(
        asof=issued_at,
        strategy_nav=Decimal("25000"),
        observed_account_nlv=Decimal("100000"),
    )
    candidate_body = {
        "candidate_id": proposal_id,
        "underlying": proposal_body["underlying"],
        "structure": proposal_body["strategy"],
        "legs": candidate_legs,
        "strategy_nav_hash": NAV_AUTHORITY_HASH,
        "strategy_nav_content_hash": canonical_hash(nav_payload),
        "strategy_nav_contract_hash": NAV_CONTRACT_HASH,
        "strategy_nav_ledger_head_hash": NAV_LEDGER_HASH,
        "strategy_nav_usd": Decimal("25000"),
        "strategy_nav_observed_account_nlv": Decimal("100000"),
        "strategy_nav_reconciliation_difference": Decimal("75000"),
        "strategy_nav_asof": issued_at,
    }
    evidence_inputs = {
        "input_hash": INPUT_HASH,
        "evidence_hash": EVIDENCE_HASH,
        "broker_snapshot_hash": BROKER_HASH,
    }
    policy = ResolvedPolicy(
        "v1",
        POLICY_HASH,
        POLICY_MARKER_HASH,
        issued_at,
        freeze_json({"policy": "initial"}),
        freeze_json({"source": "test-fixture"}),
    )
    authority = RiskTierAuthority.normal(RISK_CONTRACT_HASH)
    cost = {"cost_version": "v1", "cost_hash": COST_HASH}
    basis = build_ranking_basis(
        candidate_body=candidate_body,
        proposal_body=proposal_body,
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=evidence_inputs,
    )
    candidate = {
        "candidate_id": proposal_id,
        "candidate_body": candidate_body,
        "proposal_body": proposal_body,
        "candidate_hash": basis.candidate_hash,
        "proposal_hash": basis.proposal_hash,
        "ranking_basis_hash": basis.ranking_basis_hash,
        "evidence_inputs": evidence_inputs,
        "authority_status": "NORMAL",
        "authorizable": True,
        "score_components": {"net_ev": Decimal("10")},
    }
    policy_resolver = CurrentResolver(policy)
    risk_resolver = CurrentResolver(authority)
    cost_resolver = CurrentResolver(cost)
    with RankingStore(ranking_path) as rankings:
        snapshot = rankings.append_snapshot(
            scan_run_id=f"scan-{approval_id}",
            input_hash=INPUT_HASH,
            evidence_hash=EVIDENCE_HASH,
            broker_snapshot_hash=BROKER_HASH,
            candidates=(candidate,),
            valid_until=issued_at + timedelta(minutes=10),
            current_policy_version="v1",
            current_policy_hash=POLICY_HASH,
            policy_authority_marker_hash=POLICY_MARKER_HASH,
            cost_version="v1",
            cost_hash=COST_HASH,
            risk_contract_hash=RISK_CONTRACT_HASH,
            risk_authority_version="v1",
            risk_authority_marker_hash=authority.marker_hash,
            policy_resolver=policy_resolver,
            risk_authority_resolver=risk_resolver,
            resolved_policy=policy,
            risk_authority=authority,
            now=issued_at,
        )
        broker_proof, strategy_nav_proof = approval_proofs(
            ranking_snapshot_id=snapshot.ranking_snapshot_id,
            candidate_id=proposal_id,
            proposal_hash=basis.proposal_hash,
            observed_at=issued_at,
        )
        issued = approvals.create_challenge(
            snapshot.ranking_snapshot_id,
            proposal_id,
            ranking_store=rankings,
            policy_resolver=CurrentResolver(policy),
            risk_authority_resolver=CurrentResolver(authority),
            execution_cost_contract=CurrentResolver(cost),
            broker_proof=broker_proof,
            strategy_nav_proof=strategy_nav_proof,
            strategy_nav_source=CurrentResolver(strategy_nav_proof),
            strategy_nav_snapshot=strategy_nav_proof,
            now=issued_at,
        )
        token_suffix = approval_id.removeprefix("approval-")
        with patch(
            "options_copilot.approval.store.secrets.token_urlsafe",
            return_value=token_suffix,
        ):
            confirmation = approvals.confirm_challenge(
                issued.challenge.challenge_id,
                challenge_response=issued.challenge_response,
                risk_acknowledged=True,
                second_confirmation=True,
                confirmation_token="CREATE_IBKR_REVIEW_ONLY",
                ranking_store=rankings,
                policy_resolver=CurrentResolver(policy),
                risk_authority_resolver=CurrentResolver(authority),
                execution_cost_contract=CurrentResolver(cost),
                broker_proof=broker_proof,
                strategy_nav_proof=strategy_nav_proof,
                strategy_nav_source=CurrentResolver(strategy_nav_proof),
                strategy_nav_snapshot=strategy_nav_proof,
                now=issued_at,
            )
        assert confirmation.approval.approval_id == approval_id


def repriced(adverse_usd: Decimal | str = Decimal("4.00")) -> dict[str, object]:
    adverse = Decimal(adverse_usd)
    result = proposal()
    result["legs"][0]["ask"] = Decimal("5.10") + adverse / Decimal("100")
    result["pricing"]["net_debit_usd"] = Decimal("100") + adverse
    return result


def intent(limit_price: Decimal | str = Decimal("1.04")) -> dict[str, object]:
    return {
        "combo_legs": [
            {
                "contract_id_ex": "SPY-20260918-C-500@SMART",
                "side": "BUY",
                "ratio": Decimal("1"),
            },
            {
                "contract_id_ex": "SPY-20260918-C-510@SMART",
                "side": "SELL",
                "ratio": Decimal("1"),
            },
        ],
        "action": "BUY",
        "quantity": Decimal("1"),
        "order_type": "LIMIT",
        "limit_price": Decimal(limit_price),
        "time_in_force": "DAY",
    }


def execution_result(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "review_only": True,
        "order_submitted": False,
        "transmitted_to_broker": False,
        "instruction_id": "instruction-bridge-1",
        "deep_link": "https://chatgpt.com/codex/reviews/instruction-bridge-1",
    }
    result.update(updates)
    return result


def authorize_after_test_gate(
    bridge: CodexBridgeStore,
    approval_id: str,
    token: str,
    current: dict[str, object],
    observed_at: datetime,
    instruction: dict[str, object],
):
    """Exercise store persistence as if the coordinator gate just passed."""

    return bridge._authorize_after_broker_gate(
        approval_id,
        token,
        current,
        observed_at,
        instruction,
        snapshot_observed_at=observed_at,
        quote_snapshot_id=str(current["quote_snapshot_id"]),
        net_liquidation_usd=Decimal("2000"),
        contract_definitions_hash="0" * 64,
    )


@pytest.fixture
def stores(tmp_path: Path):
    clock = MutableClock(BASE)
    approvals = ProposalApprovalStore(tmp_path / "approvals.sqlite3", clock=clock)
    create_bound_approval(
        approvals,
        ranking_path=tmp_path / "ranking-bridge-1.sqlite3",
        proposal_body=proposal(),
        approval_id="approval-bridge-1",
        issued_at=BASE,
    )
    clock.set(BASE - timedelta(minutes=1))
    create_bound_approval(
        approvals,
        ranking_path=tmp_path / "ranking-credit-1.sqlite3",
        proposal_body=credit_proposal(),
        approval_id="approval-credit-1",
        issued_at=clock(),
    )
    clock.set(BASE - timedelta(minutes=2))
    create_bound_approval(
        approvals,
        ranking_path=tmp_path / "ranking-bridge-2.sqlite3",
        proposal_body=second_proposal(),
        approval_id="approval-bridge-2",
        issued_at=clock(),
    )
    clock.set(BASE)
    bridge = CodexBridgeStore(
        tmp_path / "codex-bridge.sqlite3",
        approvals,
        clock=clock,
        current_authority_validator=strict_current_authority_validator,
    )
    try:
        yield clock, approvals, bridge
    finally:
        bridge.close()
        approvals.close()


def authorize(stores) -> tuple[str, dict[str, object], dict[str, object]]:
    clock, _, bridge = stores
    current = repriced()
    instruction = intent()
    token = bridge.claim("approval-bridge-1")
    record = authorize_after_test_gate(
        bridge,
        "approval-bridge-1",
        token,
        current,
        clock(),
        instruction,
    )
    assert record.status is BridgeStatus.AUTHORIZED
    bridge.reserve_external_call("approval-bridge-1", token)
    return token, current, instruction


def test_wal_full_claim_is_single_winner_and_persists_only_token_hash(stores) -> None:
    clock, approvals, bridge = stores
    second = CodexBridgeStore(
        bridge.path,
        approvals,
        clock=clock,
        current_authority_validator=strict_current_authority_validator,
    )
    barrier = threading.Barrier(2)
    outcomes: list[tuple[str, str | None]] = []
    lock = threading.Lock()

    def run(store: CodexBridgeStore) -> None:
        barrier.wait()
        try:
            token = store.claim("approval-bridge-1")
        except BridgeAlreadyClaimed:
            result = ("already_claimed", None)
        else:
            result = ("claimed", token)
        with lock:
            outcomes.append(result)

    threads = [threading.Thread(target=run, args=(store,)) for store in (bridge, second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    second.close()

    assert sorted(item[0] for item in outcomes) == ["already_claimed", "claimed"]
    token = next(item[1] for item in outcomes if item[0] == "claimed")
    assert token is not None and len(token) >= 40
    assert bridge.journal_mode == "wal"
    assert bridge.synchronous == "full"
    with sqlite3.connect(bridge.path) as connection:
        stored = connection.execute(
            "SELECT token_hash FROM codex_bridge_requests"
        ).fetchone()[0]
    assert len(stored) == 64
    assert stored != token
    public = bridge.get("approval-bridge-1")
    assert public is not None
    assert public.status is BridgeStatus.CLAIMED
    assert all("token" not in key for key in public.as_dict())
    assert bridge.list_pending() == (public,)


def test_claim_without_current_authority_validator_fails_before_secret_or_row(
    tmp_path: Path,
    stores,
) -> None:
    clock, approvals, _ = stores
    with CodexBridgeStore(
        tmp_path / "missing-current-authority.sqlite3",
        approvals,
        clock=clock,
    ) as guarded:
        with patch(
            "options_copilot.bridge.store.secrets.token_urlsafe",
            side_effect=AssertionError("claim secret must not be generated"),
        ):
            with pytest.raises(
                BridgeApprovalRejected,
                match="current_authority_validator_unavailable",
            ):
                guarded.claim("approval-bridge-1")
        assert guarded.get("approval-bridge-1") is None
        assert guarded._connection.execute(
            "SELECT COUNT(*) FROM codex_bridge_claim_authority_proofs"
        ).fetchone()[0] == 0


def test_claim_rejects_bare_boolean_current_authority_result_without_rows(
    tmp_path: Path,
    stores,
) -> None:
    clock, approvals, _ = stores
    with CodexBridgeStore(
        tmp_path / "boolean-current-authority.sqlite3",
        approvals,
        clock=clock,
        current_authority_validator=lambda _binding, _checked_at: True,
    ) as guarded:
        with pytest.raises(
            BridgeApprovalRejected,
            match="current_authority_proof_invalid",
        ):
            guarded.claim("approval-bridge-1")
        assert guarded.get("approval-bridge-1") is None
        assert guarded._connection.execute(
            "SELECT COUNT(*) FROM codex_bridge_claim_authority_proofs"
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    ("field", "replacement", "reason"),
    [
        ("schema", "unsupported", "current_authority_proof_schema_invalid"),
        ("status", "STALE", "current_authority_status_not_current"),
        ("ranking_snapshot_id", "different-snapshot", "ranking_snapshot_id_mismatch"),
        ("candidate_id", "different-candidate", "candidate_id_mismatch"),
        ("proposal_hash", "0" * 64, "proposal_hash_mismatch"),
        ("snapshot_hash", "0" * 64, "snapshot_hash_mismatch"),
        ("current_policy_version", "v2", "current_policy_version_mismatch"),
        ("current_policy_hash", "0" * 64, "current_policy_hash_mismatch"),
        (
            "policy_authority_marker_hash",
            "0" * 64,
            "policy_authority_marker_hash_mismatch",
        ),
        ("cost_version", "v2", "cost_version_mismatch"),
        ("cost_hash", "0" * 64, "cost_hash_mismatch"),
        ("risk_contract_hash", "0" * 64, "risk_contract_hash_mismatch"),
        ("risk_authority_version", "v2", "risk_authority_version_mismatch"),
        (
            "risk_authority_marker_hash",
            "0" * 64,
            "risk_authority_marker_hash_mismatch",
        ),
        (
            "strategy_nav_content_hash",
            "0" * 64,
            "strategy_nav_content_hash_mismatch",
        ),
        (
            "strategy_nav_contract_hash",
            "0" * 64,
            "strategy_nav_contract_hash_mismatch",
        ),
        (
            "strategy_nav_ledger_head_hash",
            "0" * 64,
            "strategy_nav_ledger_head_hash_mismatch",
        ),
        ("checked_at", "future", "current_authority_checked_at_future"),
        ("checked_at", "stale", "current_authority_proof_stale"),
    ],
)
def test_claim_rejects_noncurrent_or_mismatched_authority_proof(
    tmp_path: Path,
    stores,
    field: str,
    replacement: object,
    reason: str,
) -> None:
    clock, approvals, _ = stores

    def invalid_validator(
        binding: ApprovalAuthorityBinding,
        checked_at: datetime,
    ) -> dict[str, object]:
        proof = current_authority_proof(binding, checked_at).as_dict()
        proof[field] = (
            checked_at + timedelta(seconds=1)
            if replacement == "future"
            else checked_at - timedelta(seconds=6)
            if replacement == "stale"
            else replacement
        )
        return proof

    with CodexBridgeStore(
        tmp_path / f"invalid-current-authority-{field}.sqlite3",
        approvals,
        clock=clock,
        current_authority_validator=invalid_validator,
    ) as guarded:
        with pytest.raises(BridgeApprovalRejected, match=reason):
            guarded.claim("approval-bridge-1")
        assert guarded.get("approval-bridge-1") is None


def test_claim_validates_inside_write_transaction_and_persists_tamper_checked_audit(
    tmp_path: Path,
    stores,
) -> None:
    clock, approvals, _ = stores
    transaction_observations: list[bool] = []
    holder: dict[str, CodexBridgeStore] = {}

    def observing_validator(
        binding: ApprovalAuthorityBinding,
        checked_at: datetime,
    ) -> CurrentAuthorityProof:
        transaction_observations.append(holder["store"]._connection.in_transaction)
        return current_authority_proof(binding, checked_at)

    with CodexBridgeStore(
        tmp_path / "audited-current-authority.sqlite3",
        approvals,
        clock=clock,
        current_authority_validator=observing_validator,
    ) as guarded:
        holder["store"] = guarded
        token = guarded.claim("approval-bridge-1")
        assert transaction_observations == [True]
        record = guarded.get("approval-bridge-1")
        assert record is not None
        assert record.current_authority_verified is True
        assert record.current_authority_checked_at == clock()
        assert record.current_authority_proof_hash is not None
        with sqlite3.connect(guarded.path) as connection:
            row = connection.execute(
                "SELECT proof_json, proof_hash "
                "FROM codex_bridge_claim_authority_proofs "
                "WHERE approval_id='approval-bridge-1'"
            ).fetchone()
            assert row is not None
            assert token not in str(row)
            with pytest.raises(sqlite3.IntegrityError, match="claim authority proof update"):
                connection.execute(
                    "UPDATE codex_bridge_claim_authority_proofs "
                    "SET proof_json='{}' WHERE approval_id='approval-bridge-1'"
                )
            connection.rollback()
            with pytest.raises(sqlite3.IntegrityError, match="claim authority proof delete"):
                connection.execute(
                    "DELETE FROM codex_bridge_claim_authority_proofs "
                    "WHERE approval_id='approval-bridge-1'"
                )
            connection.rollback()
            connection.execute("DROP TRIGGER codex_bridge_claim_authority_no_update")
            connection.execute(
                "UPDATE codex_bridge_claim_authority_proofs "
                "SET proof_json='{}' WHERE approval_id='approval-bridge-1'"
            )
            connection.commit()
        with pytest.raises(BridgeStateError, match="claim authority proof"):
            guarded.get("approval-bridge-1")


def test_wrong_token_cannot_authorize_or_fail_claim(stores) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    with pytest.raises(BridgeTokenError, match="invalid bridge claim token"):
        authorize_after_test_gate(
            bridge,
            "approval-bridge-1",
            "not-the-real-bridge-token",
            repriced(),
            clock(),
            intent(),
        )
    with pytest.raises(BridgeTokenError):
        bridge.fail("approval-bridge-1", "not-the-real-bridge-token", "cancelled")
    assert bridge.status("approval-bridge-1") is BridgeStatus.CLAIMED
    bridge.fail("approval-bridge-1", token, "operator_cancelled")


def test_claim_rejects_approval_not_issued_by_options_copilot_gui(stores) -> None:
    _, approvals, bridge = stores
    untrusted = proposal()
    untrusted["proposal_id"] = "proposal-untrusted-1"
    approvals.approve(
        "proposal-untrusted-1",
        untrusted,
        nonce="untrusted-local-approval-nonce",
        approved_by="local_helper_module",
        approval_id="approval-untrusted-1",
    )
    with pytest.raises(BridgeApprovalRejected) as caught:
        bridge.claim("approval-untrusted-1")
    assert caught.value.reasons == ("approval_not_from_options_copilot_gui",)
    assert bridge.get("approval-untrusted-1") is None


@pytest.mark.parametrize(
    "observed_at, message",
    [
        (BASE - timedelta(seconds=5, microseconds=1), "0 and 5 seconds"),
        (BASE + timedelta(microseconds=1), "future quotes"),
    ],
)
def test_authorize_rejects_old_and_future_quotes(
    stores, observed_at: datetime, message: str
) -> None:
    _, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    with pytest.raises(BridgeValidationError, match=message):
        authorize_after_test_gate(
            bridge,
            "approval-bridge-1",
            token,
            repriced(),
            observed_at,
            intent(),
        )
    assert bridge.status("approval-bridge-1") is BridgeStatus.CLAIMED


def test_five_second_and_five_dollar_boundaries_are_inclusive(stores) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    current = repriced("5.00")
    record = authorize_after_test_gate(
        bridge,
        "approval-bridge-1",
        token,
        current,
        clock() - timedelta(seconds=5),
        intent("1.05"),
    )
    assert record.status is BridgeStatus.AUTHORIZED
    assert record.limit_price == Decimal("1.05")


def test_reserve_rechecks_quote_freshness_and_gui_approval_ttl(stores) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    clock.set(BASE + timedelta(seconds=299))
    authorize_after_test_gate(
        bridge,
        "approval-bridge-1",
        token,
        repriced(),
        clock(),
        intent(),
    )
    clock.set(BASE + timedelta(seconds=300))
    with pytest.raises(BridgeApprovalRejected) as caught:
        bridge.reserve_external_call("approval-bridge-1", token)
    assert "approval_expired" in caught.value.reasons
    assert not bridge.has_external_call_attempt("approval-bridge-1")


def test_authorize_rejects_adverse_change_of_five_dollars_and_one_cent(stores) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    with pytest.raises(BridgeApprovalRejected) as caught:
        authorize_after_test_gate(
            bridge,
            "approval-bridge-1",
            token,
            repriced("5.01"),
            clock(),
            intent("1.0501"),
        )
    assert "adverse_tolerance_exceeded" in caught.value.reasons
    assert bridge.status("approval-bridge-1") is BridgeStatus.CLAIMED


@pytest.mark.parametrize("mutation", ["leg", "risk", "material"])
def test_authorize_rejects_leg_risk_and_material_changes(stores, mutation: str) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    current = repriced()
    if mutation == "leg":
        current["legs"][0]["strike"] = Decimal("501")
    elif mutation == "risk":
        current["risk"]["defined_risk"] = False
    else:
        current["underlying"] = "QQQ"
    with pytest.raises(BridgeApprovalRejected):
        authorize_after_test_gate(
            bridge,
            "approval-bridge-1", token, current, clock(), intent()
        )
    assert bridge.status("approval-bridge-1") is BridgeStatus.CLAIMED


def test_authorize_requires_real_executable_leg_quotes_not_declared_fallback(stores) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    current = repriced()
    del current["legs"][0]["ask"]
    with pytest.raises(BridgeValidationError, match="executable ask"):
        authorize_after_test_gate(
            bridge,
            "approval-bridge-1", token, current, clock(), intent()
        )


@pytest.mark.parametrize(
    "change, message",
    [
        ({"order_type": "MARKET"}, "MARKET"),
        ({"transmit": False}, "transmit/submit"),
        ({"submit_order": False}, "transmit/submit"),
        ({"time_in_force": "GTC"}, "must be DAY"),
    ],
)
def test_authorize_rejects_unsafe_instruction_intent(
    stores, change: dict[str, object], message: str
) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    unsafe = intent()
    unsafe.update(change)
    with pytest.raises(BridgeValidationError, match=message):
        authorize_after_test_gate(
            bridge,
            "approval-bridge-1", token, repriced(), clock(), unsafe
        )
    assert bridge.status("approval-bridge-1") is BridgeStatus.CLAIMED


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda row: row.update(quantity=100), "exactly 1"),
        (lambda row: row.update(action="SELL"), "must be BUY"),
        (
            lambda row: row["combo_legs"][0].update(action="BUY"),
            "exactly one BUY/SELL side",
        ),
        (
            lambda row: row["combo_legs"][0].update(quantity=1),
            "exactly one positive integral ratio",
        ),
        (
            lambda row: row["combo_legs"][0].update(conid=101),
            "exactly one contract identifier",
        ),
        (
            lambda row: row["combo_legs"][0].update(order_action="SELL"),
            "unsupported fields",
        ),
    ],
)
def test_authorize_rejects_quantity_action_and_alias_smuggling(
    stores, mutation, message: str
) -> None:
    clock, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    unsafe = intent()
    mutation(unsafe)
    with pytest.raises(BridgeValidationError, match=message):
        authorize_after_test_gate(
            bridge,
            "approval-bridge-1",
            token,
            repriced(),
            clock(),
            unsafe,
        )
    assert bridge.status("approval-bridge-1") is BridgeStatus.CLAIMED


def test_complete_rejects_changed_proposal_intent_and_limit(stores) -> None:
    _, _, bridge = stores
    token, current, instruction = authorize(stores)

    changed_proposal = deepcopy(current)
    changed_proposal["legs"][0]["volume"] = 123
    with pytest.raises(BridgeValidationError, match="proposal hash changed"):
        bridge.complete(
            "approval-bridge-1",
            token,
            changed_proposal,
            instruction,
            execution_result(),
        )

    changed_intent = deepcopy(instruction)
    changed_intent["quantity"] = Decimal("2")
    with pytest.raises(BridgeValidationError, match="quantity|intent changed"):
        bridge.complete(
            "approval-bridge-1",
            token,
            current,
            changed_intent,
            execution_result(),
        )

    changed_limit = deepcopy(instruction)
    changed_limit["limit_price"] = Decimal("1.03")
    with pytest.raises(BridgeValidationError, match="limit_price"):
        bridge.complete(
            "approval-bridge-1",
            token,
            current,
            changed_limit,
            execution_result(),
        )
    assert bridge.status("approval-bridge-1") is BridgeStatus.AUTHORIZED


def test_complete_rejects_guessed_destination_without_consuming_or_persisting(
    stores,
) -> None:
    clock, approvals, bridge = stores
    token, current, instruction = authorize(stores)
    clock.set(BASE + timedelta(seconds=30))
    with pytest.raises(BridgeValidationError, match="destination contract is unavailable"):
        bridge.complete(
            "approval-bridge-1",
            token,
            current,
            instruction,
            execution_result(),
        )
    record = bridge.get("approval-bridge-1")
    assert record is not None
    assert record.status is BridgeStatus.AUTHORIZED
    assert record.execution_hash is None
    assert record.execution_result is None
    assert record.instruction_id is None
    assert record.deep_link is None
    assert approvals.validate("approval-bridge-1", current).valid


def test_legacy_completed_row_is_redacted_from_every_public_bridge_projection(
    stores,
) -> None:
    _, _, bridge = stores
    authorize(stores)
    legacy_result = execution_result()
    with bridge._transaction():
        bridge._connection.execute(
            """
            UPDATE codex_bridge_requests
            SET status='COMPLETED', completed_at=?, execution_hash=?,
                execution_json=?, instruction_id=?, deep_link=?
            WHERE approval_id='approval-bridge-1'
            """,
            (
                BASE.isoformat(),
                canonical_hash(legacy_result),
                json.dumps(legacy_result, sort_keys=True),
                "legacy-instruction-secret",
                "https://legacy.example/review/secret",
            ),
        )

    raw = bridge._connection.execute(
        "SELECT execution_json, instruction_id, deep_link "
        "FROM codex_bridge_requests WHERE approval_id='approval-bridge-1'"
    ).fetchone()
    assert raw is not None
    assert all(value is not None for value in raw)

    record = bridge.get("approval-bridge-1")
    assert record is not None
    assert record.execution_result is None
    assert record.instruction_id is None
    assert record.deep_link is None
    assert record.as_dict()["execution_result"] is None
    assert record.as_dict()["instruction_id"] is None
    assert record.as_dict()["deep_link"] is None

    coordinator = LocalCodexBridgeCoordinator(bridge)
    projected = coordinator.get("approval-bridge-1")
    assert projected is not None
    assert projected.execution_result is None
    status = coordinator.status("approval-bridge-1")
    assert status is not None
    public_record = status["record"]
    assert isinstance(public_record, dict)
    assert public_record["execution_result"] is None
    assert public_record["instruction_id"] is None
    assert public_record["deep_link"] is None


def test_store_complete_requires_durable_external_attempt(stores) -> None:
    clock, _, bridge = stores
    current = repriced()
    instruction = intent()
    token = bridge.claim("approval-bridge-1")
    authorize_after_test_gate(
        bridge,
        "approval-bridge-1",
        token,
        current,
        clock(),
        instruction,
    )
    with pytest.raises(BridgeStateError, match="durably reserved"):
        bridge.complete(
            "approval-bridge-1",
            token,
            current,
            instruction,
            execution_result(),
        )


def test_credit_combo_requires_buy_wrapper_and_signed_negative_limit(stores) -> None:
    clock, _, bridge = stores
    credit = credit_proposal()
    token = bridge.claim("approval-credit-1")
    credit_intent = {
        "combo_legs": [
            {
                "contract_id_ex": "SPY-20260918-C-500@SMART",
                "side": "SELL",
                "ratio": 1,
            },
            {
                "contract_id_ex": "SPY-20260918-C-510@SMART",
                "side": "BUY",
                "ratio": 1,
            },
        ],
        "action": "BUY",
        "quantity": 1,
        "order_type": "LIMIT",
        "limit_price": "1.00",
        "time_in_force": "DAY",
    }
    with pytest.raises(BridgeValidationError, match="signed executable"):
        authorize_after_test_gate(
            bridge,
            "approval-credit-1", token, credit, clock(), credit_intent
        )
    credit_intent["limit_price"] = "-1.00"
    authorized = authorize_after_test_gate(
        bridge,
        "approval-credit-1", token, credit, clock(), credit_intent
    )
    assert authorized.status is BridgeStatus.AUTHORIZED
    assert authorized.limit_price == Decimal("-1")


def test_concurrent_different_approvals_allow_only_one_active_handoff(stores) -> None:
    clock, approvals, bridge = stores
    second_store = CodexBridgeStore(
        bridge.path,
        approvals,
        clock=clock,
        current_authority_validator=strict_current_authority_validator,
    )
    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    outcome_lock = threading.Lock()

    def claim(store: CodexBridgeStore, approval_id: str) -> None:
        barrier.wait()
        try:
            store.claim(approval_id)
        except BridgeActiveHandoffExists:
            result = "active_exists"
        else:
            result = "claimed"
        with outcome_lock:
            outcomes.append(result)

    threads = [
        threading.Thread(
            target=claim,
            args=(store, approval_id),
        )
        for store, approval_id in (
            (bridge, "approval-bridge-1"),
            (second_store, "approval-bridge-2"),
        )
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    second_store.close()
    assert sorted(outcomes) == ["active_exists", "claimed"]
    assert len(bridge.list_pending()) == 1


@pytest.mark.parametrize(
    "bad_link",
    [
        "javascript:alert(1)",
        "http://codex.example.com/review/1",
        "https://user:password@codex.example.com/review/1",
        "https://127.0.0.1/review/1",
        "https://codex.example.com:444/review/1",
        "https://codex.example.com\\@evil.example/review/1",
        "https://evil.example/review/1",
        "https://chatgpt.com/codex/review/1?access_token=TOPSECRET",
        "https://chatgpt.com/codex/review/1#bearer=TOPSECRET",
    ],
)
def test_complete_rejects_malicious_or_non_https_deep_links(
    stores, bad_link: str
) -> None:
    _, _, bridge = stores
    token, current, instruction = authorize(stores)
    with pytest.raises(BridgeValidationError, match="destination contract is unavailable"):
        bridge.complete(
            "approval-bridge-1",
            token,
            current,
            instruction,
            execution_result(deep_link=bad_link),
        )
    assert bridge.status("approval-bridge-1") is BridgeStatus.AUTHORIZED


@pytest.mark.parametrize(
    "unsafe_flag",
    [
        {"review_only": False},
        {"order_submitted": True},
        {"transmitted_to_broker": True},
        {"order_submitted": 0},
        {"status": "SUBMITTED"},
    ],
)
def test_complete_rejects_submission_or_non_review_flags(
    stores, unsafe_flag: dict[str, object]
) -> None:
    _, _, bridge = stores
    token, current, instruction = authorize(stores)
    with pytest.raises(BridgeValidationError):
        bridge.complete(
            "approval-bridge-1",
            token,
            current,
            instruction,
            execution_result(**unsafe_flag),
        )
    assert bridge.status("approval-bridge-1") is BridgeStatus.AUTHORIZED


def test_fail_is_terminal_and_database_enforces_strict_transitions(stores) -> None:
    _, _, bridge = stores
    token = bridge.claim("approval-bridge-1")
    with pytest.raises(ValueError, match="allowed failure reason code"):
        bridge.fail(
            "approval-bridge-1", token, f"external_review_failed:{token}"
        )
    failed = bridge.fail("approval-bridge-1", token, "external_review_failed")
    assert failed.status is BridgeStatus.FAILED
    assert failed.failure_reason == "external_review_failed"
    assert token not in str(failed.as_dict())
    assert bridge.list_pending() == ()
    with pytest.raises(BridgeStateError):
        bridge.fail("approval-bridge-1", token, "retry")
    with pytest.raises(BridgeAlreadyClaimed):
        bridge.claim("approval-bridge-1")
    with sqlite3.connect(bridge.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="invalid bridge state transition"):
            connection.execute(
                "UPDATE codex_bridge_requests SET status='CLAIMED' "
                "WHERE approval_id='approval-bridge-1'"
            )


def test_expired_token_lost_claim_can_be_auditably_terminated(stores) -> None:
    clock, _, bridge = stores
    bridge.claim("approval-bridge-1")
    with pytest.raises(BridgeStateError, match="not backed by an expired approval"):
        bridge.expire_stranded_claim("approval-bridge-1")
    clock.set(BASE + timedelta(seconds=300))
    expired = bridge.expire_stranded_claim("approval-bridge-1")
    assert expired.status is BridgeStatus.FAILED
    assert expired.failure_reason == "claim_abandoned_expired"
    assert not expired.external_call_reserved
