from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.gateway.broker_snapshot import (
    BrokerSnapshotBuilder,
    BrokerSnapshotStatus,
)
from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    PositionSnapshot,
    QuoteBatchStatus,
)
from options_copilot.governance.contracts import (
    ContractKind,
    SignedContract,
    sign_contract,
)
from options_copilot.positions import (
    PositionDelta,
    PositionManagementKind,
    PositionManager,
    PositionRiskMetrics,
    TransitionRejected,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 4, 17, 0, tzinfo=timezone.utc)
ZERO_RISK = PositionRiskMetrics(
    max_loss_usd=Decimal("0"),
    exposure_usd=Decimal("0"),
    capital_usage_usd=Decimal("0"),
)
BEFORE_RISK = PositionRiskMetrics(
    max_loss_usd=Decimal("800"),
    exposure_usd=Decimal("1200"),
    capital_usage_usd=Decimal("900"),
)
REDUCED_RISK = PositionRiskMetrics(
    max_loss_usd=Decimal("650"),
    exposure_usd=Decimal("900"),
    capital_usage_usd=Decimal("700"),
)


def _contracts() -> tuple[OptionContractRef, ...]:
    return tuple(
        OptionContractRef(
            contract_id=contract_id,
            contract_id_ex=f"{contract_id}@SMART",
            symbol="GLD",
            local_symbol=f"GLD   260821C00{strike}000",
            expiration=date(2026, 8, 21),
            strike=Decimal(str(strike)),
            right="C",
            exchange="SMART",
            trading_class="GLD",
            multiplier=100,
        )
        for contract_id, strike in ((101, 375), (102, 385), (103, 390))
    )


def _positions() -> tuple[PositionSnapshot, ...]:
    quantities = (Decimal("1"), Decimal("-2"), Decimal("1"))
    return tuple(
        PositionSnapshot(
            asof=NOW - timedelta(seconds=1),
            contract_id=contract.contract_id,
            symbol="GLD",
            local_symbol=contract.local_symbol,
            security_type="OPT",
            currency="USD",
            exchange="SMART",
            quantity=quantities[index],
            average_cost=Decimal("125.50"),
            market_price=Decimal("1.20"),
            market_value=Decimal("120"),
            unrealized_pnl=Decimal("-5"),
            realized_pnl=Decimal("0"),
        )
        for index, contract in enumerate(_contracts())
    )


def _secdefs() -> tuple[OptionSecDefSnapshot, ...]:
    return tuple(
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
            source="IBKR_REQ_CONTRACT_DETAILS",
        )
        for contract in _contracts()
    )


def _quote_batch() -> OptionQuoteBatch:
    requested_at = NOW - timedelta(seconds=2)
    completed_at = NOW - timedelta(milliseconds=200)
    quotes = tuple(
        BatchedOptionQuote(
            contract_id=contract.contract_id,
            batch_id="gld-batch-1",
            request_id=f"request-{contract.contract_id}",
            requested_at=requested_at,
            observed_at=NOW - timedelta(seconds=1),
            completed_at=completed_at,
            source="IBKR_REQ_TICKERS_READONLY",
            bid=Decimal("1.00") + Decimal(index) / Decimal("10"),
            ask=Decimal("1.10") + Decimal(index) / Decimal("10"),
            exchange_time=NOW - timedelta(seconds=1),
            volume=100,
            open_interest=1000,
            implied_volatility=Decimal("0.20"),
        )
        for index, contract in enumerate(_contracts())
    )
    return OptionQuoteBatch(
        batch_id="gld-batch-1",
        status=QuoteBatchStatus.COMPLETE,
        requested_at=requested_at,
        completed_at=completed_at,
        source="IBKR_REQ_TICKERS_READONLY",
        quotes=quotes,
    )


class _BrokerSource:
    def __init__(self) -> None:
        self.position_reads = [_positions(), _positions()]
        self.order_reads: list[object] = [(), ()]
        self.instruction_reads: list[object] = [(), ()]
        self.secdef_reads = [_secdefs(), _secdefs()]

    def account_snapshot(self) -> object:
        return {"currency": "USD", "net_liquidation": Decimal("5000")}

    def positions(self) -> object:
        return self.position_reads.pop(0)

    def working_orders(self) -> object:
        return self.order_reads.pop(0)

    def unsubmitted_instructions(self) -> object:
        return self.instruction_reads.pop(0)

    def option_contract_definitions(self, _contracts) -> object:
        return self.secdef_reads.pop(0)

    def option_quote_batch(self, _contracts) -> OptionQuoteBatch:
        return _quote_batch()


def _snapshot(source: _BrokerSource | None = None):
    return BrokerSnapshotBuilder(source or _BrokerSource(), clock=lambda: NOW).build(
        _contracts()
    )


def test_position_mark_churn_uses_post_sample_without_identity_mutation() -> None:
    source = _BrokerSource()
    source.position_reads[1] = tuple(
        replace(
            position,
            asof=NOW,
            market_price=Decimal("1.35"),
            market_value=Decimal("135"),
            unrealized_pnl=Decimal("10"),
            realized_pnl=Decimal("2"),
        )
        for position in source.position_reads[1]
    )

    snapshot = _snapshot(source)
    evidence = snapshot.state_evidence["positions"]
    normalized = PositionManager().normalize_snapshot(snapshot)

    assert snapshot.status is BrokerSnapshotStatus.COMPLETE
    assert evidence.stable is True
    assert evidence.pre_hash != evidence.post_hash
    assert evidence.post_hash == canonical_hash(evidence.state)
    assert all(row["market_price"] == Decimal("1.35") for row in evidence.state)
    assert tuple(item.contract_id for item in normalized) == (101, 102, 103)


def test_position_average_cost_mutation_remains_fail_closed() -> None:
    source = _BrokerSource()
    source.position_reads[1] = (
        replace(source.position_reads[1][0], average_cost=Decimal("126.50")),
        *source.position_reads[1][1:],
    )

    snapshot = _snapshot(source)

    assert snapshot.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert "POSITIONS_MUTATED" in snapshot.reason_codes


def _cost_contract() -> SignedContract:
    return sign_contract(
        kind=ContractKind.EXECUTION_COST,
        version="v1",
        effective_at=NOW - timedelta(days=1),
        provenance={
            "source": "test-fixture",
            "source_hash": "a" * 64,
            "observed_at": NOW - timedelta(days=1),
        },
        payload={"currency": "USD", "cost_quantum_usd": "0.01"},
        actor="human:test",
        signed_at=NOW - timedelta(hours=1),
    )


def _rehash(snapshot, **changes):
    changed = replace(snapshot, **changes, snapshot_hash="0" * 64)
    return replace(changed, snapshot_hash=canonical_hash(changed.hash_payload()))


def _prove(
    *,
    snapshot=None,
    kind=PositionManagementKind.REDUCE_RISK,
    deltas=(PositionDelta(102, 1),),
    before_risk=BEFORE_RISK,
    after_risk=REDUCED_RISK,
    exit_contract_hash="b" * 64,
    execution_cost_contract=None,
):
    return PositionManager().prove_transition(
        snapshot or _snapshot(),
        kind=kind,
        deltas=deltas,
        before_risk=before_risk,
        after_risk=after_risk,
        exit_contract_hash=exit_contract_hash,
        execution_cost_contract=execution_cost_contract or _cost_contract(),
    )


def test_exact_gld_one_minus_two_one_close_produces_zero_after_state() -> None:
    proof = _prove(
        kind=PositionManagementKind.CLOSE_ALL,
        deltas=(
            PositionDelta(101, -1),
            PositionDelta(102, 2),
            PositionDelta(103, -1),
        ),
        after_risk=ZERO_RISK,
    )

    assert proof.management_kind is PositionManagementKind.CLOSE_ALL
    assert [item.signed_quantity for item in proof.before_positions] == [1, -2, 1]
    assert proof.after_positions == ()
    assert proof.capital_usage.after_capital_usage_usd == Decimal("0")
    assert proof.verify_hash() is True


def test_partial_reduction_is_derived_from_authoritative_before_state() -> None:
    proof = _prove()

    assert [(item.contract_id, item.signed_quantity) for item in proof.after_positions] == [
        (101, 1),
        (102, -1),
        (103, 1),
    ]
    assert proof.capital_usage.before_gross_contracts == 4
    assert proof.capital_usage.after_gross_contracts == 3
    assert proof.positions_state_hash == _snapshot().state_evidence["positions"].post_hash


@pytest.mark.parametrize(
    ("deltas", "after_risk", "code"),
    [
        ((PositionDelta(999, -1),), REDUCED_RISK, "NEW_CONID"),
        ((PositionDelta(102, -1),), REDUCED_RISK, "QUANTITY_INCREASE"),
        ((PositionDelta(102, 3),), REDUCED_RISK, "ZERO_CROSSING"),
        ((PositionDelta(101, -1),), REDUCED_RISK, "NET_SHORT_INCREASE"),
        (
            (PositionDelta(102, 1),),
            replace(REDUCED_RISK, max_loss_usd=Decimal("800.01")),
            "MAX_LOSS_INCREASE",
        ),
        (
            (PositionDelta(102, 1),),
            replace(REDUCED_RISK, exposure_usd=Decimal("1200.01")),
            "EXPOSURE_INCREASE",
        ),
        (
            (PositionDelta(102, 1),),
            replace(REDUCED_RISK, capital_usage_usd=Decimal("900.01")),
            "CAPITAL_USAGE_INCREASE",
        ),
    ],
)
def test_reduce_risk_rejects_every_new_or_increased_risk_path(
    deltas,
    after_risk: PositionRiskMetrics,
    code: str,
) -> None:
    with pytest.raises(TransitionRejected) as caught:
        _prove(deltas=deltas, after_risk=after_risk)

    assert caught.value.code == code


@pytest.mark.parametrize("delta", [True, 1.0, Decimal("1.1"), "1.0", " 1"])
def test_signed_delta_rejects_non_integer_or_ambiguous_values(delta: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        PositionDelta(102, delta)  # type: ignore[arg-type]


def test_incomplete_or_tampered_snapshot_and_evidence_fail_closed() -> None:
    snapshot = _snapshot()
    tampered_hash = replace(snapshot, snapshot_hash="c" * 64)
    secdefs = list(snapshot.secdef_evidence)
    secdefs[0] = replace(secdefs[0], pre_hash="d" * 64)
    tampered_secdef = _rehash(snapshot, secdef_evidence=tuple(secdefs))
    stale_quote = _rehash(snapshot, oldest_quote_age_seconds=Decimal("5.01"))

    for invalid, code in (
        (tampered_hash, "SNAPSHOT_HASH_MISMATCH"),
        (tampered_secdef, "SECDEF_HASH_MISMATCH"),
        (stale_quote, "QUOTE_STALE"),
    ):
        with pytest.raises(TransitionRejected) as caught:
            _prove(snapshot=invalid)
        assert caught.value.code == code


def test_nonstandard_adjusted_quote_incoherence_and_pending_state_fail_closed() -> None:
    snapshot = _snapshot()
    secdefs = list(snapshot.secdef_evidence)
    secdefs[0] = replace(secdefs[0], standard_contract=False, adjusted=True)
    adjusted = _rehash(snapshot, secdef_evidence=tuple(secdefs))

    quotes = list(snapshot.quotes)
    quotes[0] = replace(quotes[0], batch_id="wrong-batch")
    incoherent = _rehash(snapshot, quotes=tuple(quotes))

    source = _BrokerSource()
    source.order_reads = [({"order_id": 7},), ({"order_id": 7},)]
    with_order = _snapshot(source)

    source = _BrokerSource()
    source.instruction_reads = [
        ({"instruction_id": "pending"},),
        ({"instruction_id": "pending"},),
    ]
    with_instruction = _snapshot(source)

    for invalid, code in (
        (adjusted, "NONSTANDARD_SECDEF"),
        (incoherent, "QUOTE_BATCH_MISMATCH"),
        (with_order, "WORKING_ORDERS_PRESENT"),
        (with_instruction, "UNSUBMITTED_INSTRUCTIONS_PRESENT"),
    ):
        with pytest.raises(TransitionRejected) as caught:
            _prove(snapshot=invalid)
        assert caught.value.code == code


def test_missing_or_tampered_exit_and_cost_contract_hashes_reject() -> None:
    with pytest.raises(TransitionRejected) as missing_exit:
        _prove(exit_contract_hash="")
    assert missing_exit.value.code == "EXIT_CONTRACT_HASH_INVALID"

    tampered_cost = _cost_contract().to_dict()
    tampered_cost["payload"]["currency"] = "EUR"
    with pytest.raises(TransitionRejected) as invalid_cost:
        _prove(execution_cost_contract=tampered_cost)
    assert invalid_cost.value.code == "EXECUTION_COST_CONTRACT_INVALID"


def test_proof_hash_binds_all_authorities_and_is_recomputable() -> None:
    first = _prove()
    changed_exit = _prove(exit_contract_hash="c" * 64)

    snapshot = _snapshot()
    account = snapshot.state_evidence["account"]
    changed_account_state = {"currency": "USD", "net_liquidation": Decimal("5001")}
    changed_account_hash = canonical_hash(changed_account_state)
    state_evidence = dict(snapshot.state_evidence)
    state_evidence["account"] = replace(
        account,
        pre_hash=changed_account_hash,
        post_hash=changed_account_hash,
        state=changed_account_state,
    )
    changed_snapshot = _rehash(snapshot, state_evidence=state_evidence)

    quotes = list(snapshot.quotes)
    quotes[0] = replace(quotes[0], bid=Decimal("0.99"))
    changed_quote_snapshot = _rehash(snapshot, quotes=tuple(quotes))

    secdefs = list(snapshot.secdef_evidence)
    changed_identity = dict(secdefs[0].post_identity or {})
    changed_identity["strike"] = Decimal("374")
    changed_identity_hash = canonical_hash(changed_identity)
    secdefs[0] = replace(
        secdefs[0],
        pre_identity=changed_identity,
        post_identity=changed_identity,
        pre_hash=changed_identity_hash,
        post_hash=changed_identity_hash,
    )
    changed_secdef_snapshot = _rehash(snapshot, secdef_evidence=tuple(secdefs))

    changed_cost = sign_contract(
        kind=ContractKind.EXECUTION_COST,
        version="v1",
        effective_at=NOW - timedelta(days=1),
        provenance={
            "source": "second-fixture",
            "source_hash": "d" * 64,
            "observed_at": NOW - timedelta(days=1),
        },
        payload={"currency": "USD", "cost_quantum_usd": "0.02"},
        actor="human:test",
        signed_at=NOW - timedelta(hours=1),
    )
    changed_cost_proof = _prove(execution_cost_contract=changed_cost)

    changed_snapshot_proof = _prove(snapshot=changed_snapshot)
    changed_quote_proof = _prove(snapshot=changed_quote_snapshot)
    changed_secdef_proof = _prove(snapshot=changed_secdef_snapshot)

    assert canonical_hash(first.hash_payload()) == first.proof_hash
    assert first.verify_hash() is True
    assert len(first.quote_batch_hash) == 64
    assert len(
        {
            first.proof_hash,
            changed_snapshot_proof.proof_hash,
            changed_quote_proof.proof_hash,
            changed_secdef_proof.proof_hash,
            changed_exit.proof_hash,
            changed_cost_proof.proof_hash,
        }
    ) == 6
    assert replace(first, broker_snapshot_hash="e" * 64).verify_hash() is False
    assert replace(first, quote_batch_hash="e" * 64).verify_hash() is False
