from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO
import json
from pathlib import Path
import sqlite3
import threading
from unittest.mock import patch
from zoneinfo import ZoneInfo

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
    BridgeBrokerSnapshotRejected,
    BridgeDecisionContext,
    BridgeExternalCallAlreadyAttempted,
    BridgeStateError,
    BridgeStatus,
    BridgeUnknownOutcomeError,
    BridgeValidationError,
    CodexBridgeStore,
    CurrentAuthorityProof,
    LocalCodexBridgeCoordinator,
)
from options_copilot.bridge.cli import main as cli_main
from options_copilot.gateway import (
    BatchedOptionQuote,
    BrokerSnapshotBuilder,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)
from options_copilot.market import UsOptionsSessionCalendar
from options_copilot.performance.nav_ledger import (
    NavAttribution,
    NavEventKind,
    StrategyNavLedger,
)
from options_copilot.proposals import validate_proposal
from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.ranking.store import RankingStore
from options_copilot.risk import OptionTimePolicy, RiskEngine
from options_copilot.risk.authorization import RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash, freeze_json


NOW = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
SNAPSHOT_ID = "managed-quotes-20260803T150000Z"
CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "strategy_nav_contract.v1.json"
)
EXECUTION_COST_HASH = "a" * 64
CURRENT_POLICY_HASH = "b" * 64
POLICY_AUTHORITY_HASH = "c" * 64
INPUT_HASH = "d" * 64
EVIDENCE_HASH = "e" * 64
BROKER_HASH = "f" * 64
RISK_CONTRACT_HASH = "1" * 64
NAV_CONTRACT_HASH = "3" * 64
NAV_LEDGER_HASH = "4" * 64
NAV_AUTHORITY_HASH = canonical_hash(
    {
        "schema": "options_copilot.strategy_nav_authority.v1",
        "strategy_nav_usd": Decimal("1000"),
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
    }
)
ATOMIC_SNAPSHOT_HASH = "5" * 64
CONTRACT_DEFINITIONS_HASH = "6" * 64
QUOTES_HASH = "7" * 64
APPROVAL_CONTRACT_IDS = (101, 102)


class MutableClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current


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


def atomic_contracts() -> tuple[OptionContractRef, ...]:
    return (
        OptionContractRef(
            contract_id=101,
            contract_id_ex="101@SMART",
            symbol="SPY",
            local_symbol="SPY   260821C00100000",
            expiration=date(2026, 8, 21),
            strike=Decimal("100"),
            right="C",
            exchange="SMART",
            trading_class="SPY",
            multiplier=100,
        ),
        OptionContractRef(
            contract_id=102,
            contract_id_ex="102@SMART",
            symbol="SPY",
            local_symbol="SPY   260821C00105000",
            expiration=date(2026, 8, 21),
            strike=Decimal("105"),
            right="C",
            exchange="SMART",
            trading_class="SPY",
            multiplier=100,
        ),
    )


class MutableAtomicSource:
    def __init__(self, clock: MutableClock) -> None:
        self.clock = clock
        self.account = {"currency": "USD", "net_liquidation": Decimal("2000")}
        self.position_rows: tuple[dict[str, object], ...] = ()
        self.order_rows: tuple[dict[str, object], ...] = ()
        self.instruction_rows: tuple[dict[str, object], ...] | None = ()
        self.secdefs = tuple(
            OptionSecDefSnapshot(
                contract_id=contract.contract_id,
                local_symbol=contract.local_symbol,
                trading_class=contract.trading_class,
                multiplier=contract.multiplier,
                exchange=contract.exchange,
                expiration=contract.expiration,
                strike=contract.strike,
                right=contract.right,
                security_type="OPT",
                currency="USD",
                standard_contract=True,
                adjusted=False,
                source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
            )
            for contract in atomic_contracts()
        )
        self.bids = (Decimal("1.90"), Decimal("0.90"))
        self.asks = (Decimal("2.00"), Decimal("1.00"))
        self.quote_calls = 0
        self.after_quote = False
        self.mutate_during_next_build: str | None = None
        self.advance_during_next_build = timedelta(0)
        self.lock_probe = None
        self.lock_observations: list[bool] = []

    def _observe_lock(self) -> None:
        if self.lock_probe is not None:
            self.lock_observations.append(bool(self.lock_probe()))

    def account_snapshot(self):
        self._observe_lock()
        if self.after_quote and self.mutate_during_next_build == "account":
            return {**self.account, "net_liquidation": Decimal("1999")}
        return dict(self.account)

    def positions(self):
        self._observe_lock()
        if self.after_quote and self.mutate_during_next_build == "positions":
            return ({"contract_id": 999, "quantity": Decimal("1"), "security_type": "OPT"},)
        return tuple(dict(item) for item in self.position_rows)

    def working_orders(self):
        self._observe_lock()
        if self.after_quote and self.mutate_during_next_build == "working_orders":
            return ({"order_id": 77, "status": "Submitted"},)
        return tuple(dict(item) for item in self.order_rows)

    def unsubmitted_instructions(self):
        self._observe_lock()
        if self.after_quote and self.mutate_during_next_build == "unsubmitted_instructions":
            result = ({"instruction_id": "changed"},)
        elif self.instruction_rows is None:
            result = None
        else:
            result = tuple(dict(item) for item in self.instruction_rows)
        if self.after_quote:
            self.after_quote = False
            self.mutate_during_next_build = None
        return result

    def option_contract_definitions(self, _contracts):
        self._observe_lock()
        if self.after_quote and self.mutate_during_next_build == "secdef":
            return (replace(self.secdefs[0], strike=Decimal("101")), self.secdefs[1])
        return self.secdefs

    def option_quote_batch(self, contracts):
        self._observe_lock()
        self.quote_calls += 1
        self.after_quote = True
        if self.advance_during_next_build:
            self.clock.current += self.advance_during_next_build
            self.advance_during_next_build = timedelta(0)
        batch_id = SNAPSHOT_ID if self.quote_calls == 1 else f"reserve-batch-{self.quote_calls}"
        observed_at = self.clock.current - timedelta(seconds=1)
        requested_at = observed_at - timedelta(milliseconds=1)
        completed_at = observed_at + timedelta(milliseconds=1)
        quotes = tuple(
            BatchedOptionQuote(
                contract_id=contract.contract_id,
                batch_id=batch_id,
                request_id=f"{batch_id}:{contract.contract_id}",
                requested_at=requested_at,
                observed_at=observed_at,
                completed_at=completed_at,
                source="IBKR_REQ_TICKERS_READONLY",
                bid=self.bids[index],
                ask=self.asks[index],
                exchange_time=observed_at,
                market_data_type=1,
            )
            for index, contract in enumerate(contracts)
        )
        return OptionQuoteBatch(
            batch_id=batch_id,
            status=QuoteBatchStatus.COMPLETE,
            requested_at=requested_at,
            completed_at=completed_at,
            source="IBKR_REQ_TICKERS_READONLY",
            quotes=quotes,
        )


class MutableDecisionContextResolver:
    def __init__(self, ledger: StrategyNavLedger, clock: MutableClock) -> None:
        self.ledger = ledger
        self.clock = clock
        self.time_policy = OptionTimePolicy()
        self.calendar_age = timedelta(0)
        self.session_close = "1600"

    def __call__(self) -> BridgeDecisionContext:
        nav = self.ledger.snapshot(
            asof=self.clock.current,
            observed_account_nlv=Decimal("2000"),
        )
        authority = RiskTierAuthority.normal(nav.contract_hash)
        engine = RiskEngine(
            strategy_nav=nav,
            expected_contract_hash=nav.contract_hash,
            expected_ledger_head_hash=nav.ledger_head_hash,
            risk_tier_authority=authority,
        )
        market_date = self.clock.current.astimezone(
            ZoneInfo("America/New_York")
        ).date()
        day = market_date.strftime("%Y%m%d")
        hours = f"{day}:0930-{day}:{self.session_close}"
        calendar = UsOptionsSessionCalendar().normalize(
            liquid_hours=hours,
            trading_hours=hours,
            timezone_id="America/New_York",
            observed_at=self.clock.current - self.calendar_age,
            source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
            now=self.clock.current,
        )
        return BridgeDecisionContext(
            risk_engine=engine,
            execution_cost_contract_version="v1",
            execution_cost_contract_hash=EXECUTION_COST_HASH,
            current_policy_version="initial-v1",
            current_policy_hash=CURRENT_POLICY_HASH,
            policy_authority_marker_hash=POLICY_AUTHORITY_HASH,
            option_time_policy=self.time_policy,
            market_calendar=calendar,
        )


def raw_proposal(
    *,
    now: datetime = NOW,
    snapshot_id: str = SNAPSHOT_ID,
    expiration: str = "2026-08-21",
) -> dict[str, object]:
    quote_time = (now - timedelta(seconds=1)).isoformat()
    return {
        "proposal_id": "spy-call-spread-bridge",
        "rank": 1,
        "eligible_to_send": True,
        "underlying": "SPY",
        "expiration": expiration,
        "quote_snapshot_id": snapshot_id,
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
                "expiration": expiration,
                "strike": "100",
                "right": "CALL",
                "side": "BUY",
                "quantity": 1,
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
                "bid": "1.90",
                "ask": "2.00",
                "quote_time": quote_time,
                "quote_snapshot_id": snapshot_id,
            },
            {
                "contract_id_ex": "102@SMART",
                "underlying": "SPY",
                "security_type": "OPT",
                "expiration": expiration,
                "strike": "105",
                "right": "CALL",
                "side": "SELL",
                "quantity": 1,
                "multiplier": "100",
                "currency": "USD",
                "exchange": "SMART",
                "bid": "0.90",
                "ask": "1.00",
                "quote_time": quote_time,
                "quote_snapshot_id": snapshot_id,
            },
        ],
    }


def canonical_proposal(*, now: datetime = NOW) -> dict[str, object]:
    return validate_proposal(
        raw_proposal(now=now),
        account_equity=Decimal("2000"),
        open_combinations=0,
        now=now,
        quote_fresh_seconds=Decimal("5"),
        expected_quote_snapshot_id=SNAPSHOT_ID,
    ).to_dict()


def approval_proofs(
    *,
    ranking_snapshot_id: str,
    candidate_id: str,
    proposal_hash: str,
    observed_at: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    snapshot_payload = _strategy_nav_snapshot_payload(observed_at)
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
            "account": "8" * 64,
            "positions": "9" * 64,
            "working_orders": "a" * 64,
            "unsubmitted_instructions": "b" * 64,
        },
        "contract_definitions_hash": CONTRACT_DEFINITIONS_HASH,
        "quotes_hash": QUOTES_HASH,
        "contract_ids": list(APPROVAL_CONTRACT_IDS),
        "account_nlv_usd": "2000",
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
        "strategy_nav_usd": "1000",
        "observed_account_nlv": "2000",
        "reconciliation_difference": "1000",
        "asof": observed_at,
        "snapshot_payload": snapshot_payload,
    }
    return broker_proof, strategy_nav_proof


def _strategy_nav_snapshot_payload(asof: datetime) -> dict[str, object]:
    return {
        "asof": asof,
        "strategy_nav": Decimal("1000"),
        "strategy_deposits": Decimal("0"),
        "strategy_withdrawals": Decimal("0"),
        "realized_pnl": Decimal("0"),
        "open_position_unrealized_pnl": Decimal("0"),
        "fees": Decimal("0"),
        "signed_corrections": Decimal("0"),
        "non_strategy_contribution": Decimal("0"),
        "fill_principal_contribution": Decimal("0"),
        "observed_account_nlv": Decimal("2000"),
        "reconciliation_difference": Decimal("1000"),
        "contract_version": "v1",
        "contract_hash": NAV_CONTRACT_HASH,
        "ledger_head_hash": NAV_LEDGER_HASH,
        "valid": True,
        "no_trade_reasons": (),
    }


def strict_current_authority_validator(
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


def create_bound_approval(
    approvals: ProposalApprovalStore,
    *,
    ranking_path: Path,
    proposal_body: dict[str, object],
    approval_id: str,
    issued_at: datetime,
) -> None:
    """Seed coordinator fixtures through ranking challenge confirmation."""

    proposal_id = str(proposal_body["proposal_id"])
    candidate_legs = deepcopy(proposal_body["legs"])
    for leg, con_id in zip(candidate_legs, APPROVAL_CONTRACT_IDS, strict=True):
        leg["con_id"] = con_id
    nav_payload = _strategy_nav_snapshot_payload(issued_at)
    candidate_body = {
        "candidate_id": proposal_id,
        "underlying": proposal_body["underlying"],
        "legs": candidate_legs,
        "strategy_nav_hash": NAV_AUTHORITY_HASH,
        "strategy_nav_content_hash": canonical_hash(nav_payload),
        "strategy_nav_contract_hash": NAV_CONTRACT_HASH,
        "strategy_nav_ledger_head_hash": NAV_LEDGER_HASH,
        "strategy_nav_usd": Decimal("1000"),
        "strategy_nav_observed_account_nlv": Decimal("2000"),
        "strategy_nav_reconciliation_difference": Decimal("1000"),
        "strategy_nav_asof": issued_at,
    }
    evidence_inputs = {
        "input_hash": INPUT_HASH,
        "evidence_hash": EVIDENCE_HASH,
        "broker_snapshot_hash": BROKER_HASH,
    }
    policy = ResolvedPolicy(
        "v1",
        CURRENT_POLICY_HASH,
        POLICY_AUTHORITY_HASH,
        issued_at,
        freeze_json({"policy": "initial"}),
        freeze_json({"source": "coordinator-test-fixture"}),
    )
    authority = RiskTierAuthority.normal(RISK_CONTRACT_HASH)
    cost = {"cost_version": "v1", "cost_hash": EXECUTION_COST_HASH}
    basis = build_ranking_basis(
        candidate_body=candidate_body,
        proposal_body=proposal_body,
        current_policy_version="v1",
        current_policy_hash=CURRENT_POLICY_HASH,
        policy_authority_marker_hash=POLICY_AUTHORITY_HASH,
        cost_version="v1",
        cost_hash=EXECUTION_COST_HASH,
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
    with RankingStore(ranking_path) as rankings:
        snapshot = rankings.append_snapshot(
            scan_run_id=f"scan-{approval_id}",
            input_hash=INPUT_HASH,
            evidence_hash=EVIDENCE_HASH,
            broker_snapshot_hash=BROKER_HASH,
            candidates=(candidate,),
            valid_until=issued_at + timedelta(minutes=10),
            current_policy_version="v1",
            current_policy_hash=CURRENT_POLICY_HASH,
            policy_authority_marker_hash=POLICY_AUTHORITY_HASH,
            cost_version="v1",
            cost_hash=EXECUTION_COST_HASH,
            risk_contract_hash=RISK_CONTRACT_HASH,
            risk_authority_version="v1",
            risk_authority_marker_hash=authority.marker_hash,
            policy_resolver=CurrentResolver(policy),
            risk_authority_resolver=CurrentResolver(authority),
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
        with patch(
            "options_copilot.approval.store.secrets.token_urlsafe",
            return_value=approval_id.removeprefix("approval-"),
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


def broker_snapshot(
    *,
    now: datetime = NOW,
    snapshot_id: str = SNAPSHOT_ID,
) -> dict[str, object]:
    proposal_value = raw_proposal(now=now, snapshot_id=snapshot_id)
    contract_definitions = [
        {
            "contract_id_ex": leg["contract_id_ex"],
            "underlying": leg["underlying"],
            "security_type": leg["security_type"],
            "expiration": leg["expiration"],
            "strike": leg["strike"],
            "right": leg["right"],
            "multiplier": leg["multiplier"],
            "currency": leg["currency"],
            "standard_contract": True,
        }
        for leg in proposal_value["legs"]
    ]
    return {
        "broker_snapshot_complete": True,
        "observed_at": now.isoformat(),
        "quote_snapshot_id": snapshot_id,
        "account": {"net_liquidation_usd": "2000"},
        "positions": [],
        "working_orders": [],
        "unsubmitted_instructions": [],
        "contract_definitions_complete": True,
        "contract_definitions_source": "managed_connector",
        "contract_definitions": contract_definitions,
        "proposal": proposal_value,
    }


def instruction_intent() -> dict[str, object]:
    return {
        "combo_legs": [
            {"contract_id_ex": "101@SMART", "side": "BUY", "ratio": 1},
            {"contract_id_ex": "102@SMART", "side": "SELL", "ratio": 1},
        ],
        "action": "BUY",
        "quantity": 1,
        "order_type": "LIMIT",
        "limit_price": "1.10",
        "time_in_force": "DAY",
    }


def safe_result() -> dict[str, object]:
    return {
        "review_only": True,
        "order_submitted": False,
        "transmitted_to_broker": False,
        "instruction_id": "codex-review-bridge-1",
        "deep_link": "https://chatgpt.com/codex/reviews/codex-review-bridge-1",
    }


@pytest.fixture
def coordinated(tmp_path: Path):
    clock = MutableClock(NOW)
    approvals = ProposalApprovalStore(tmp_path / "approvals.db", clock=clock)
    create_bound_approval(
        approvals,
        ranking_path=tmp_path / "ranking-coordinator.db",
        proposal_body=canonical_proposal(),
        approval_id="approval-coordinator-1",
        issued_at=NOW,
    )
    store = CodexBridgeStore(
        tmp_path / "bridge.db",
        approvals,
        clock=clock,
        current_authority_validator=strict_current_authority_validator,
    )
    coordinator = LocalCodexBridgeCoordinator(store, clock=clock)
    try:
        yield clock, approvals, store, coordinator
    finally:
        store.close()
        approvals.close()


@pytest.fixture
def atomic_coordinated(tmp_path: Path):
    clock = MutableClock(NOW)
    approvals = ProposalApprovalStore(tmp_path / "atomic-approvals.db", clock=clock)
    create_bound_approval(
        approvals,
        ranking_path=tmp_path / "ranking-atomic.db",
        proposal_body=canonical_proposal(),
        approval_id="approval-atomic-1",
        issued_at=NOW,
    )
    store = CodexBridgeStore(
        tmp_path / "atomic-bridge.db",
        approvals,
        clock=clock,
        current_authority_validator=strict_current_authority_validator,
    )
    ledger = StrategyNavLedger(
        tmp_path / "atomic-strategy-nav.db",
        contract=CONTRACT_PATH,
        clock=clock,
    )
    ledger.append_flow(
        event_kind=NavEventKind.WITHDRAWAL,
        broker_event_identifier="atomic-initial-capital-adjustment",
        effective_at=NOW - timedelta(minutes=1),
        amount=Decimal("12.44"),
        attribution=NavAttribution.STRATEGY,
    )
    source = MutableAtomicSource(clock)
    source.lock_probe = lambda: store._connection.in_transaction
    context_resolver = MutableDecisionContextResolver(ledger, clock)
    coordinator = LocalCodexBridgeCoordinator(
        store,
        clock=clock,
        broker_snapshot_builder=BrokerSnapshotBuilder(source, clock=clock),
        contract_resolver=lambda _proposal: atomic_contracts(),
        decision_context_resolver=context_resolver,
    )
    try:
        yield (
            clock,
            approvals,
            store,
            coordinator,
            source,
            context_resolver,
            ledger,
        )
    finally:
        ledger.close()
        store.close()
        approvals.close()


def claimed_and_authorized(coordinated):
    _, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    record = coordinator.authorize(
        "approval-coordinator-1",
        token,
        broker_snapshot(),
        instruction_intent(),
    )
    assert record.status is BridgeStatus.AUTHORIZED
    assert not record.external_call_reserved
    assert store.status("approval-coordinator-1") is BridgeStatus.AUTHORIZED
    return token


def atomic_claimed_and_authorized(atomic_coordinated):
    _, _, store, coordinator, _, _, _ = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    record = coordinator.authorize(
        "approval-atomic-1",
        token,
        broker_snapshot(),
        instruction_intent(),
    )
    assert record.status is BridgeStatus.AUTHORIZED
    assert record.authorized_broker_snapshot_hash is not None
    assert not record.external_call_reserved
    assert store.status("approval-atomic-1") is BridgeStatus.AUTHORIZED
    return token


def test_authorize_revalidates_current_snapshot_before_store_authorize(
    coordinated,
) -> None:
    clock, _, store, _ = coordinated
    calls: list[dict[str, object]] = []

    def spy(proposal, **kwargs):
        calls.append(dict(kwargs))
        return validate_proposal(proposal, **kwargs)

    coordinator = LocalCodexBridgeCoordinator(
        store,
        clock=clock,
        proposal_validator=spy,
    )
    token = coordinator.claim("approval-coordinator-1")
    record = coordinator.authorize(
        "approval-coordinator-1",
        token,
        broker_snapshot(),
        instruction_intent(),
    )
    assert record.status is BridgeStatus.AUTHORIZED
    assert len(calls) == 1
    assert calls[0]["account_equity"] == Decimal("2000")
    assert calls[0]["open_combinations"] == 0
    assert calls[0]["quote_fresh_seconds"] == Decimal("5")
    assert calls[0]["expected_quote_snapshot_id"] == SNAPSHOT_ID
    assert record.proposal["risk"]["maximum_loss_usd"] == "113"


def test_new_quote_snapshot_reprices_same_structure_without_material_drift(
    coordinated,
) -> None:
    _, _, _, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    replacement = "managed-quotes-20260803T150001Z"
    authorized = coordinator.authorize(
        "approval-coordinator-1",
        token,
        broker_snapshot(snapshot_id=replacement),
        instruction_intent(),
    )
    assert authorized.status is BridgeStatus.AUTHORIZED
    assert authorized.proposal["quote_snapshot_id"] == replacement
    assert all(
        leg["quote_snapshot_id"] == replacement
        for leg in authorized.proposal["legs"]
    )


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda row: row["account"].update(net_liquidation_usd="0"), "positive"),
        (
            lambda row: row["positions"].append(
                {"asset_class": "OPT", "position": 1}
            ),
            "open option positions",
        ),
        (
            lambda row: row["working_orders"].append({"order_id": "working-1"}),
            "working orders must be empty",
        ),
        (
            lambda row: row["unsubmitted_instructions"].append(
                {"instruction_id": "existing-review"}
            ),
            "unsubmitted instructions must be empty",
        ),
        (
            lambda row: row.update(connector_auth_token="forbidden"),
            "authentication material",
        ),
        (
            lambda row: row.pop("broker_snapshot_complete"),
            "broker_snapshot_complete",
        ),
    ],
)
def test_current_nlv_positions_orders_and_instruction_gates_fail_before_authorize(
    coordinated, mutation, message: str
) -> None:
    _, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    snapshot = broker_snapshot()
    mutation(snapshot)
    with pytest.raises(BridgeBrokerSnapshotRejected, match=message):
        coordinator.authorize(
            "approval-coordinator-1",
            token,
            snapshot,
            instruction_intent(),
        )
    assert store.status("approval-coordinator-1") is BridgeStatus.CLAIMED


@pytest.mark.parametrize("offset_seconds", [-6, 1])
def test_current_snapshot_and_each_leg_quote_must_be_zero_to_five_seconds_old(
    coordinated, offset_seconds: int
) -> None:
    _, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    snapshot = broker_snapshot()
    bad_time = (NOW + timedelta(seconds=offset_seconds)).isoformat()
    snapshot["proposal"]["legs"][1]["quote_time"] = bad_time
    with pytest.raises(BridgeBrokerSnapshotRejected):
        coordinator.authorize(
            "approval-coordinator-1",
            token,
            snapshot,
            instruction_intent(),
        )
    assert store.status("approval-coordinator-1") is BridgeStatus.CLAIMED


def test_stale_broker_snapshot_blocks_even_when_leg_quotes_are_fresh(coordinated) -> None:
    _, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    snapshot = broker_snapshot()
    snapshot["observed_at"] = (NOW - timedelta(seconds=6)).isoformat()
    with pytest.raises(BridgeBrokerSnapshotRejected, match="older than 5 seconds"):
        coordinator.authorize(
            "approval-coordinator-1",
            token,
            snapshot,
            instruction_intent(),
        )
    assert store.status("approval-coordinator-1") is BridgeStatus.CLAIMED


class RecordingCreator:
    def __init__(self, store: CodexBridgeStore, *, result=None, error=None) -> None:
        self.store = store
        self.result = safe_result() if result is None else result
        self.error = error
        self.calls: list[dict[str, object]] = []

    def create_review_instruction(self, **kwargs):
        approval_id = kwargs["idempotency_key"]
        record = self.store.get(approval_id)
        assert record is not None
        assert record.status is BridgeStatus.AUTHORIZED
        assert record.external_call_reserved is True
        assert kwargs["review_only"] is True
        assert "token" not in kwargs
        assert "credential" not in str(kwargs).lower()
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result


def test_reserve_rebuilds_atomic_snapshot_under_write_lock_and_persists_proof(
    atomic_coordinated,
) -> None:
    _, _, store, coordinator, source, _, _ = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    authorized = coordinator.authorize(
        "approval-atomic-1",
        token,
        broker_snapshot(),
        instruction_intent(),
    )

    assert source.quote_calls == 1
    assert authorized.authorized_broker_snapshot_hash is not None
    assert authorized.reserve_broker_snapshot_hash is None
    source.lock_observations.clear()

    reserved = coordinator.reserve_external_call("approval-atomic-1", token)

    assert source.quote_calls == 2
    assert source.lock_observations
    assert all(source.lock_observations)
    assert reserved.external_call_reserved is True
    assert reserved.reserve_broker_snapshot_hash is not None
    assert (
        reserved.reserve_broker_snapshot_hash
        != authorized.authorized_broker_snapshot_hash
    )
    assert reserved.reserve_gate_verified_at == NOW
    assert store.has_external_call_attempt("approval-atomic-1")


def test_authorize_and_reserve_bind_fresh_entry_time_decisions(
    atomic_coordinated,
) -> None:
    clock, _, store, coordinator, _, _, _ = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    coordinator.authorize(
        "approval-atomic-1", token, broker_snapshot(), instruction_intent()
    )
    clock.current += timedelta(seconds=1)

    reserved = coordinator.reserve_external_call("approval-atomic-1", token)

    with sqlite3.connect(store.path) as connection:
        authorization_json = connection.execute(
            "SELECT decision_bindings_json "
            "FROM codex_bridge_atomic_authorization_gates WHERE approval_id=?",
            ("approval-atomic-1",),
        ).fetchone()[0]
        reserve_json = connection.execute(
            "SELECT decision_bindings_json "
            "FROM codex_bridge_reserve_gates WHERE approval_id=?",
            ("approval-atomic-1",),
        ).fetchone()[0]

    authorization = json.loads(authorization_json)
    reserve = json.loads(reserve_json)
    assert authorization["schema"] == "options_copilot.bridge.decision_bindings.v2"
    assert reserve["schema"] == "options_copilot.bridge.decision_bindings.v2"
    authorized_time = authorization["entry_time_decision"]
    reserve_time = reserve["entry_time_decision"]
    for decision in (authorized_time, reserve_time):
        assert decision["mode"] == "ENTRY"
        assert decision["allowed"] is True
        assert decision["reason_codes"] == []
        assert decision["et_trading_date"] == "2026-08-03"
        assert decision["expiration"] == "2026-08-21"
        assert decision["dte"] == 18
        assert len(decision["calendar_hash"]) == 64
        assert len(decision["calendar_source_hash"]) == 64
        assert decision["exception_hash"] is None
        assert decision["session_open_utc"].startswith("2026-08-03T13:30:00")
        assert decision["session_close_utc"].startswith("2026-08-03T20:00:00")
        assert len(decision["decision_hash"]) == 64
    assert authorized_time["evaluated_at_utc"].startswith(
        "2026-08-03T15:00:00"
    )
    assert reserve_time["evaluated_at_utc"].startswith("2026-08-03T15:00:01")
    assert authorized_time["calendar_hash"] != reserve_time["calendar_hash"]
    assert authorized_time["decision_hash"] != reserve_time["decision_hash"]
    assert reserved.external_call_reserved is True


def test_reserve_resamples_trusted_time_after_lock_time_requery(
    atomic_coordinated,
) -> None:
    clock, _, store, coordinator, source, _, _ = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    coordinator.authorize(
        "approval-atomic-1", token, broker_snapshot(), instruction_intent()
    )
    source.advance_during_next_build = timedelta(seconds=1)

    reserved = coordinator.reserve_external_call("approval-atomic-1", token)

    assert clock.current == NOW + timedelta(seconds=1)
    assert reserved.reserve_gate_verified_at == clock.current
    assert reserved.external_call_started_at == clock.current
    assert reserved.external_call_reserved is True


def test_stale_calendar_rejects_authorization_before_attempt(
    atomic_coordinated,
) -> None:
    _, _, store, coordinator, _, context_resolver, _ = atomic_coordinated
    context_resolver.calendar_age = timedelta(seconds=6)
    token = coordinator.claim("approval-atomic-1")

    with pytest.raises(BridgeBrokerSnapshotRejected, match="CALENDAR_DEGRADED"):
        coordinator.authorize(
            "approval-atomic-1", token, broker_snapshot(), instruction_intent()
        )

    assert store.status("approval-atomic-1") is BridgeStatus.CLAIMED
    assert not store.has_external_call_attempt("approval-atomic-1")


def test_reserve_time_session_change_rolls_back_before_attempt(
    atomic_coordinated,
) -> None:
    _, _, store, coordinator, _, context_resolver, _ = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    coordinator.authorize(
        "approval-atomic-1", token, broker_snapshot(), instruction_intent()
    )
    context_resolver.session_close = "1300"

    with pytest.raises(BridgeValidationError, match="time-policy decision binding"):
        coordinator.reserve_external_call("approval-atomic-1", token)

    assert not store.has_external_call_attempt("approval-atomic-1")


@pytest.mark.parametrize(
    "component",
    ["account", "positions", "working_orders", "unsubmitted_instructions", "secdef"],
)
def test_lock_time_state_or_secdef_change_fails_before_attempt(
    atomic_coordinated,
    component: str,
) -> None:
    _, _, store, coordinator, source, _, _ = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    coordinator.authorize(
        "approval-atomic-1", token, broker_snapshot(), instruction_intent()
    )
    if component == "account":
        source.account = {"currency": "USD", "net_liquidation": Decimal("1999")}
    elif component == "positions":
        source.position_rows = (
            {
                "contract_id": 999,
                "quantity": Decimal("1"),
                "security_type": "OPT",
            },
        )
    elif component == "working_orders":
        source.order_rows = ({"order_id": 77, "status": "Submitted"},)
    elif component == "unsubmitted_instructions":
        source.instruction_rows = ({"instruction_id": "pending"},)
    else:
        source.secdefs = (
            replace(source.secdefs[0], strike=Decimal("101")),
            source.secdefs[1],
        )

    with pytest.raises(
        (BridgeBrokerSnapshotRejected, BridgeValidationError, BridgeStateError)
    ):
        coordinator.reserve_external_call("approval-atomic-1", token)

    assert not store.has_external_call_attempt("approval-atomic-1")


def test_incoherent_lock_time_quote_batch_rolls_back_before_attempt(
    atomic_coordinated,
) -> None:
    _, _, store, coordinator, source, _, _ = atomic_coordinated
    token = atomic_claimed_and_authorized(atomic_coordinated)
    source.asks = (Decimal("1.80"), Decimal("1.00"))

    with pytest.raises(BridgeBrokerSnapshotRejected, match="QUOTE_INCOHERENT"):
        coordinator.reserve_external_call("approval-atomic-1", token)

    assert not store.has_external_call_attempt("approval-atomic-1")
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM codex_bridge_reserve_gates"
        ).fetchone()[0]
        == 0
    )


def test_mutation_inside_lock_time_build_and_nav_head_change_fail_before_attempt(
    atomic_coordinated,
) -> None:
    clock, _, store, coordinator, source, _, ledger = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    coordinator.authorize(
        "approval-atomic-1", token, broker_snapshot(), instruction_intent()
    )
    source.mutate_during_next_build = "positions"
    with pytest.raises(BridgeBrokerSnapshotRejected, match="MUTATED_DURING_BUILD"):
        coordinator.reserve_external_call("approval-atomic-1", token)
    assert not store.has_external_call_attempt("approval-atomic-1")

    ledger.append_flow(
        event_kind=NavEventKind.FEE,
        broker_event_identifier="atomic-lock-time-fee",
        effective_at=clock.current,
        amount=Decimal("1"),
        attribution=NavAttribution.STRATEGY,
    )
    with pytest.raises(BridgeValidationError, match="Strategy NAV|decision binding"):
        coordinator.reserve_external_call("approval-atomic-1", token)
    assert not store.has_external_call_attempt("approval-atomic-1")


def test_coherent_lock_time_quote_movement_is_recomputed_and_can_pass(
    atomic_coordinated,
) -> None:
    _, _, store, coordinator, source, _, _ = atomic_coordinated
    token = coordinator.claim("approval-atomic-1")
    coordinator.authorize(
        "approval-atomic-1", token, broker_snapshot(), instruction_intent()
    )
    source.bids = (Decimal("1.88"), Decimal("0.92"))
    source.asks = (Decimal("1.98"), Decimal("1.02"))
    reserved = coordinator.reserve_external_call("approval-atomic-1", token)

    assert reserved.status is BridgeStatus.AUTHORIZED
    assert reserved.reserve_broker_snapshot_hash is not None
    reserve_row = store._connection.execute(
        "SELECT proposal_json FROM codex_bridge_reserve_gates "
        "WHERE approval_id='approval-atomic-1'"
    ).fetchone()
    assert reserve_row is not None
    reserve_proposal = json.loads(str(reserve_row[0]))
    assert reserve_proposal["risk"]["maximum_loss_usd"] == "109"
    assert reserve_proposal["expected_value_usd"] == "24"


def test_atomic_gate_proofs_are_append_only_and_tampering_blocks_attempt(
    atomic_coordinated,
) -> None:
    _, _, store, coordinator, _, _, _ = atomic_coordinated
    token = atomic_claimed_and_authorized(atomic_coordinated)
    with sqlite3.connect(store.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="atomic broker gate update"):
            connection.execute(
                "UPDATE codex_bridge_atomic_authorization_gates "
                "SET snapshot_json='{}' WHERE approval_id='approval-atomic-1'"
            )
        connection.rollback()
        connection.execute("DROP TRIGGER codex_bridge_atomic_gate_no_update")
        connection.execute(
            "UPDATE codex_bridge_atomic_authorization_gates "
            "SET snapshot_json='{}' WHERE approval_id='approval-atomic-1'"
        )
        connection.commit()

    with pytest.raises(BridgeStateError, match="snapshot hash mismatch"):
        coordinator.reserve_external_call("approval-atomic-1", token)
    assert not store.has_external_call_attempt("approval-atomic-1")


def test_schema_four_store_migrates_claim_authority_audit_table(tmp_path: Path) -> None:
    clock = MutableClock(NOW)
    approval_path = tmp_path / "migration-approvals.db"
    bridge_path = tmp_path / "migration-bridge.db"
    with ProposalApprovalStore(approval_path, clock=clock) as approvals:
        with CodexBridgeStore(bridge_path, approvals, clock=clock) as bridge:
            assert bridge.schema_version == 5
        with sqlite3.connect(bridge_path) as connection:
            connection.execute("DROP TABLE codex_bridge_claim_authority_proofs")
            connection.execute("PRAGMA user_version=4")
            connection.commit()
        with CodexBridgeStore(bridge_path, approvals, clock=clock) as migrated:
            assert migrated.schema_version == 5
            objects = {
                str(row[0])
                for row in migrated._connection.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','trigger')"
                )
            }
            assert "codex_bridge_claim_authority_proofs" in objects
            assert "codex_bridge_claim_authority_no_update" in objects
            assert "codex_bridge_claim_authority_no_delete" in objects


def reopen_v4_active_row_without_claim_authority_proof(
    store: CodexBridgeStore,
    approvals: ProposalApprovalStore,
    clock: MutableClock,
) -> CodexBridgeStore:
    bridge_path = store.path
    store.close()
    with sqlite3.connect(bridge_path) as connection:
        connection.execute("DROP TABLE codex_bridge_claim_authority_proofs")
        connection.execute("PRAGMA user_version=4")
        connection.commit()

    def validator_must_not_run(
        _binding: ApprovalAuthorityBinding,
        _checked_at: datetime,
    ) -> CurrentAuthorityProof:
        raise AssertionError("legacy-row guard must not re-run current validator")

    return CodexBridgeStore(
        bridge_path,
        approvals,
        clock=clock,
        current_authority_validator=validator_must_not_run,
    )


def test_v4_active_claim_without_claim_authority_proof_cannot_authorize(
    coordinated,
) -> None:
    clock, approvals, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    migrated = reopen_v4_active_row_without_claim_authority_proof(
        store,
        approvals,
        clock,
    )
    migrated_coordinator = LocalCodexBridgeCoordinator(migrated, clock=clock)
    try:
        with pytest.raises(
            BridgeStateError,
            match="claim_authority_proof_missing",
        ):
            migrated_coordinator.authorize(
                "approval-coordinator-1",
                token,
                broker_snapshot(),
                instruction_intent(),
            )
        assert migrated.status("approval-coordinator-1") is BridgeStatus.CLAIMED
        assert not migrated.has_external_call_attempt("approval-coordinator-1")
    finally:
        migrated.close()


def test_v4_active_authorization_without_claim_authority_proof_cannot_reserve(
    coordinated,
) -> None:
    clock, approvals, store, coordinator = coordinated
    token = claimed_and_authorized(coordinated)
    migrated = reopen_v4_active_row_without_claim_authority_proof(
        store,
        approvals,
        clock,
    )
    try:
        with pytest.raises(
            BridgeStateError,
            match="claim_authority_proof_missing",
        ):
            migrated.reserve_external_call("approval-coordinator-1", token)
        assert migrated.status("approval-coordinator-1") is BridgeStatus.AUTHORIZED
        assert not migrated.has_external_call_attempt("approval-coordinator-1")
    finally:
        migrated.close()


def test_destination_preflight_refuses_before_attempt_call_or_consumption(
    atomic_coordinated,
) -> None:
    _, approvals, store, coordinator, _, _, _ = atomic_coordinated
    creator = RecordingCreator(store)
    token = coordinator.claim("approval-atomic-1")
    with pytest.raises(BridgeStateError, match="expected AUTHORIZED"):
        coordinator.execute_authorized("approval-atomic-1", token, creator)
    assert creator.calls == []

    coordinator.authorize(
        "approval-atomic-1",
        token,
        broker_snapshot(),
        instruction_intent(),
    )
    with pytest.raises(
        BridgeValidationError,
        match="creator review destination contract is unavailable",
    ):
        coordinator.execute_authorized("approval-atomic-1", token, creator)
    assert creator.calls == []
    assert not store.has_external_call_attempt("approval-atomic-1")
    with pytest.raises(BridgeValidationError, match="destination contract"):
        coordinator.execute_authorized("approval-atomic-1", token, creator)
    assert creator.calls == []
    record = store.get("approval-atomic-1")
    assert record is not None
    assert record.status is BridgeStatus.AUTHORIZED
    assert record.external_call_reserved is False
    assert record.instruction_id is None
    assert record.deep_link is None
    assert approvals.validate("approval-atomic-1", record.proposal).valid


def test_direct_store_authorize_cannot_bypass_broker_gate_or_call_creator(
    coordinated,
) -> None:
    clock, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    creator = RecordingCreator(store)

    with pytest.raises(BridgeStateError, match="direct store authorization"):
        store.authorize(
            "approval-coordinator-1",
            token,
            canonical_proposal(),
            clock.current,
            instruction_intent(),
        )
    with pytest.raises(BridgeStateError, match="expected AUTHORIZED"):
        coordinator.execute_authorized(
            "approval-coordinator-1", token, creator
        )
    assert creator.calls == []
    assert store.status("approval-coordinator-1") is BridgeStatus.CLAIMED


def test_persistence_only_authorization_has_no_executable_broker_gate(
    coordinated,
) -> None:
    clock, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    store._authorize_persistence_only_for_test(
        "approval-coordinator-1",
        token,
        canonical_proposal(),
        clock.current - timedelta(seconds=1),
        instruction_intent(),
    )
    creator = RecordingCreator(store)
    with pytest.raises(BridgeStateError, match="atomic reserve-time"):
        coordinator.reserve_external_call("approval-coordinator-1", token)
    assert creator.calls == []
    assert not store.has_external_call_attempt("approval-coordinator-1")


def test_reserve_rechecks_account_snapshot_age_not_only_quote_age(
    coordinated,
) -> None:
    clock, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    snapshot = broker_snapshot()
    snapshot["observed_at"] = (NOW - timedelta(seconds=5)).isoformat()
    coordinator.authorize(
        "approval-coordinator-1", token, snapshot, instruction_intent()
    )
    clock.current = NOW + timedelta(seconds=1)
    with pytest.raises(BridgeValidationError, match="broker snapshot"):
        store.reserve_external_call("approval-coordinator-1", token)
    assert not store.has_external_call_attempt("approval-coordinator-1")


@pytest.mark.parametrize(
    "mutation, message",
    [
        (
            lambda row: row["contract_definitions"][0].update(strike="101"),
            "metadata mismatch",
        ),
        (
            lambda row: row["contract_definitions"][0].update(multiplier="10"),
            "multiplier must be 100",
        ),
        (
            lambda row: row.update(contract_definitions_complete=False),
            "complete must be exactly true",
        ),
    ],
)
def test_authoritative_contract_definition_mismatch_is_no_trade_and_no_call(
    coordinated, mutation, message: str
) -> None:
    _, _, store, coordinator = coordinated
    token = coordinator.claim("approval-coordinator-1")
    snapshot = broker_snapshot()
    mutation(snapshot)
    with pytest.raises(BridgeBrokerSnapshotRejected, match=message):
        coordinator.authorize(
            "approval-coordinator-1", token, snapshot, instruction_intent()
        )
    creator = RecordingCreator(store)
    with pytest.raises(BridgeStateError, match="expected AUTHORIZED"):
        coordinator.execute_authorized(
            "approval-coordinator-1", token, creator
        )
    assert creator.calls == []
    assert store.status("approval-coordinator-1") is BridgeStatus.CLAIMED


def test_stale_authorization_cannot_reserve_or_call_external_creator(
    coordinated,
) -> None:
    clock, _, store, coordinator = coordinated
    token = claimed_and_authorized(coordinated)
    clock.current = NOW + timedelta(seconds=6)
    with pytest.raises(BridgeValidationError, match="0 and 5 seconds"):
        store.reserve_external_call("approval-coordinator-1", token)
    assert not store.has_external_call_attempt("approval-coordinator-1")


def test_persisted_attempt_blocks_second_coordinator_after_crash(
    atomic_coordinated,
) -> None:
    clock, _, store, coordinator, _, _, _ = atomic_coordinated
    token = atomic_claimed_and_authorized(atomic_coordinated)
    reserved = coordinator.reserve_external_call("approval-atomic-1", token)
    assert reserved.external_call_reserved

    restarted = LocalCodexBridgeCoordinator(store, clock=clock)
    creator = RecordingCreator(store)
    with pytest.raises(BridgeValidationError, match="destination contract"):
        restarted.execute_authorized("approval-atomic-1", token, creator)
    assert creator.calls == []
    status = restarted.status("approval-atomic-1")
    assert status is not None
    assert status["status"] == "UNKNOWN_OUTCOME"
    assert status["automatic_retry_allowed"] is False


def test_two_concurrent_coordinators_can_call_creator_only_once(
    atomic_coordinated,
) -> None:
    clock, _, store, coordinator, source, context_resolver, _ = atomic_coordinated
    token = atomic_claimed_and_authorized(atomic_coordinated)
    second = LocalCodexBridgeCoordinator(
        store,
        clock=clock,
        broker_snapshot_builder=BrokerSnapshotBuilder(source, clock=clock),
        contract_resolver=lambda _proposal: atomic_contracts(),
        decision_context_resolver=context_resolver,
    )
    creator = RecordingCreator(store)
    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    outcome_lock = threading.Lock()

    def run(owner: LocalCodexBridgeCoordinator) -> None:
        barrier.wait()
        try:
            owner.execute_authorized("approval-atomic-1", token, creator)
        except BridgeValidationError:
            result = "refused"
        else:
            result = "completed"
        with outcome_lock:
            outcomes.append(result)

    threads = [threading.Thread(target=run, args=(owner,)) for owner in (coordinator, second)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert sorted(outcomes) == ["refused", "refused"]
    assert creator.calls == []
    assert store.status("approval-atomic-1") is BridgeStatus.AUTHORIZED
    assert not store.has_external_call_attempt("approval-atomic-1")


def test_regular_fail_after_attempt_is_forced_to_unknown_outcome(
    atomic_coordinated,
) -> None:
    _, _, store, coordinator, _, _, _ = atomic_coordinated
    token = atomic_claimed_and_authorized(atomic_coordinated)
    coordinator.reserve_external_call("approval-atomic-1", token)
    failed = coordinator.fail(
        "approval-atomic-1",
        token,
        "operator_reported_failure",
    )
    assert failed.status is BridgeStatus.FAILED
    assert failed.unknown_outcome is True
    assert failed.failure_reason == "UNKNOWN_OUTCOME:external_result_uncertain"
    assert store.has_external_call_attempt("approval-atomic-1")


@pytest.mark.parametrize(
    "creator_kwargs",
    [
        {"error": TimeoutError("connector timeout with possible success")},
        {"result": {**safe_result(), "order_submitted": True}},
        {"result": {**safe_result(), "deep_link": "javascript:alert(1)"}},
        {"result": {**safe_result(), "broker_order_id": "should-not-exist"}},
        {"result": {**safe_result(), "connector_auth_token": "forbidden"}},
    ],
)
def test_unavailable_destination_prevents_every_creator_result_path(
    atomic_coordinated, creator_kwargs
) -> None:
    _, _, store, coordinator, _, _, _ = atomic_coordinated
    token = atomic_claimed_and_authorized(atomic_coordinated)
    creator = RecordingCreator(store, **creator_kwargs)
    with pytest.raises(BridgeValidationError, match="destination contract"):
        coordinator.execute_authorized("approval-atomic-1", token, creator)
    assert creator.calls == []
    record = store.get("approval-atomic-1")
    assert record is not None
    assert record.status is BridgeStatus.AUTHORIZED
    assert not record.unknown_outcome
    assert record.failure_reason is None
    assert not store.has_external_call_attempt("approval-atomic-1")
    with pytest.raises(BridgeValidationError, match="destination contract"):
        coordinator.execute_authorized("approval-atomic-1", token, creator)
    assert creator.calls == []


def _run_cli(
    approval_path: Path,
    bridge_path: Path,
    request: dict[str, object],
) -> tuple[int, dict[str, object], str]:
    stdin = StringIO(json.dumps(request, separators=(",", ":")))
    stdout = StringIO()
    code = cli_main(
        [
            "--approval-db",
            str(approval_path),
            "--bridge-db",
            str(bridge_path),
        ],
        stdin=stdin,
        stdout=stdout,
    )
    raw = stdout.getvalue()
    return code, json.loads(raw), raw


def test_json_cli_without_current_authority_validator_cannot_claim_or_leak_token(
    tmp_path: Path,
) -> None:
    now = datetime.now(timezone.utc)
    approval_path = tmp_path / "cli-approvals.db"
    bridge_path = tmp_path / "cli-bridge.db"
    initial = validate_proposal(
        raw_proposal(
            now=now,
            expiration=(now + timedelta(days=18)).date().isoformat(),
        ),
        account_equity=Decimal("2000"),
        open_combinations=0,
        now=now,
        quote_fresh_seconds=Decimal("5"),
        expected_quote_snapshot_id=SNAPSHOT_ID,
    ).to_dict()
    with ProposalApprovalStore(approval_path) as approvals:
        create_bound_approval(
            approvals,
            ranking_path=tmp_path / "ranking-json-cli.db",
            proposal_body=initial,
            approval_id="approval-json-cli-1",
            issued_at=now,
        )

    code, blocked, blocked_raw = _run_cli(
        approval_path,
        bridge_path,
        {"command": "claim", "approval_id": "approval-json-cli-1"},
    )
    assert code == 2
    assert blocked["ok"] is False
    assert blocked["error"]["type"] == "BridgeApprovalRejected"
    assert "current_authority_validator_unavailable" in blocked["error"]["message"]
    assert "token" not in blocked_raw
    with ProposalApprovalStore(approval_path) as approvals:
        with CodexBridgeStore(bridge_path, approvals) as bridge:
            assert bridge.get("approval-json-cli-1") is None
            assert not bridge.has_external_call_attempt("approval-json-cli-1")

    code, status, raw_status = _run_cli(
        approval_path,
        bridge_path,
        {"command": "status", "approval_id": "approval-json-cli-1"},
    )
    assert code == 0
    assert status["result"]["status"] == "NOT_FOUND"
    assert status["result"]["automatic_retry_allowed"] is False
    assert "token" not in raw_status


def test_json_cli_complete_before_dispatch_fails_closed(coordinated) -> None:
    _, _, _, coordinator = coordinated
    token = claimed_and_authorized(coordinated)
    with pytest.raises(BridgeStateError, match="durably reserved"):
        coordinator.complete(
            "approval-coordinator-1",
            token,
            safe_result(),
        )
