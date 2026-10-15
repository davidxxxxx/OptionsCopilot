from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json

from options_copilot.gateway.broker_snapshot import BrokerSnapshotBuilder
from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    PositionSnapshot,
    QuoteBatchStatus,
)
from options_copilot.governance.contracts import load_contract
from options_copilot.positions import PositionManagementKind, PositionManager
from options_copilot.positions.cli import (
    load_snapshot_json,
    run_dry_run,
    write_dry_run_preview,
)
from options_copilot.positions.generator import ManagementCandidateGenerator
from options_copilot.storage.canonical import canonical_json


NOW = datetime(2026, 8, 4, 17, 0, tzinfo=timezone.utc)
COST_CONTRACT_PATH = (
    "options_copilot/governance/execution_cost_contract.v1.json"
)


def _contracts(*, middle_id: int = 784523001) -> tuple[OptionContractRef, ...]:
    values = (
        (middle_id - 11, 375),
        (middle_id, 385),
        (middle_id + 17, 390),
    )
    return tuple(
        OptionContractRef(
            contract_id=con_id,
            contract_id_ex=f"{con_id}@SMART",
            symbol="GLD",
            local_symbol=f"GLD   260821C00{strike}000",
            expiration=date(2026, 8, 21),
            strike=Decimal(str(strike)),
            right="C",
            exchange="SMART",
            trading_class="GLD",
            multiplier=100,
        )
        for con_id, strike in values
    )


def _positions(
    contracts: tuple[OptionContractRef, ...],
    *,
    combo_quantity: int = 1,
) -> tuple[PositionSnapshot, ...]:
    return tuple(
        PositionSnapshot(
            asof=NOW - timedelta(seconds=1),
            contract_id=contract.contract_id,
            symbol="GLD",
            local_symbol=contract.local_symbol,
            security_type="OPT",
            currency="USD",
            exchange="SMART",
            quantity=quantity,
            average_cost=Decimal("100"),
            market_price=None,
            market_value=None,
            unrealized_pnl=None,
            realized_pnl=None,
        )
        for contract, quantity in zip(
            contracts,
            (
                Decimal(combo_quantity),
                Decimal(-2 * combo_quantity),
                Decimal(combo_quantity),
            ),
        )
    )


def _secdefs(contracts: tuple[OptionContractRef, ...]) -> tuple[OptionSecDefSnapshot, ...]:
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
        for contract in contracts
    )


def _quote_batch(
    contracts: tuple[OptionContractRef, ...],
    *,
    observed_at: datetime | None = None,
) -> OptionQuoteBatch:
    requested_at = NOW - timedelta(seconds=2)
    completed_at = NOW - timedelta(milliseconds=100)
    prices = (("8.00", "8.20"), ("3.00", "3.20"), ("1.00", "1.20"))
    quotes = tuple(
        BatchedOptionQuote(
            contract_id=contract.contract_id,
            batch_id="gld-management-batch",
            request_id=f"request-{contract.contract_id}",
            requested_at=requested_at,
            observed_at=observed_at or NOW - timedelta(seconds=1),
            completed_at=completed_at,
            source="IBKR_REQ_TICKERS_READONLY",
            bid=Decimal(prices[index][0]),
            ask=Decimal(prices[index][1]),
            exchange_time=observed_at or NOW - timedelta(seconds=1),
            volume=200,
            open_interest=2000,
            implied_volatility=Decimal("0.25"),
        )
        for index, contract in enumerate(contracts)
    )
    return OptionQuoteBatch(
        batch_id="gld-management-batch",
        status=QuoteBatchStatus.COMPLETE,
        requested_at=requested_at,
        completed_at=completed_at,
        source="IBKR_REQ_TICKERS_READONLY",
        quotes=quotes,
    )


class _Source:
    def __init__(
        self,
        *,
        middle_id: int = 784523001,
        combo_quantity: int = 1,
    ) -> None:
        self.contracts = _contracts(middle_id=middle_id)
        positions = _positions(self.contracts, combo_quantity=combo_quantity)
        secdefs = _secdefs(self.contracts)
        self.position_reads: list[object] = [positions, positions]
        self.order_reads: list[object] = [(), ()]
        self.instruction_reads: list[object] = [(), ()]
        self.secdef_reads: list[object] = [secdefs, secdefs]
        self.batch = _quote_batch(self.contracts)

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
        return self.batch


def _snapshot(source: _Source | None = None):
    provider = source or _Source()
    return BrokerSnapshotBuilder(provider, clock=lambda: NOW).build(provider.contracts)


def _exit_contract() -> dict[str, object]:
    return {
        "thesis_invalidation": "close immediately when the management thesis is invalid",
        "risk_stop": "close remaining risk at the signed risk threshold",
        "profit_take": "close remaining risk at the signed profit threshold",
        "time_stop": "close before the authoritative short-leg deadline",
        "maximum_holding_date": "2026-08-17",
        "bad_quote_action": "NO_TRADE until a fresh coherent quote batch exists",
        "expiration_calendar_hash": "c" * 64,
        "ex_dividend_calendar_hash": "d" * 64,
        "early_exercise_risk": "CLEAR",
        "short_leg_exit_deadline": "2026-08-19",
    }


def _generate(*, snapshot=None, exit_contract=None, cost_contract=None, manager=None):
    selected_manager = manager or PositionManager()
    generator = ManagementCandidateGenerator(position_manager=selected_manager)
    result = generator.generate(
        snapshot or _snapshot(),
        exit_contract if exit_contract is not None else _exit_contract(),
        cost_contract
        if cost_contract is not None
        else load_contract(COST_CONTRACT_PATH, as_of=NOW),
    )
    return selected_manager, result


def test_snapshot_derived_close_all_uses_dynamic_conids_and_exact_natural_prices() -> None:
    middle_id = 998877001
    snapshot = _snapshot(_Source(middle_id=middle_id))

    manager, result = _generate(snapshot=snapshot)

    assert result.status == "CANDIDATES"
    assert result.no_trade is False
    close = result.candidates[0]
    assert close.management_kind is PositionManagementKind.CLOSE_ALL
    assert [item.contract_id for item in close.transition_proof.before_positions] == [
        middle_id - 11,
        middle_id,
        middle_id + 17,
    ]
    assert [item.signed_quantity_delta for item in close.transition_proof.deltas] == [
        -1,
        2,
        -1,
    ]
    assert close.executable_close_cashflow_usd == Decimal("260.00")
    assert close.estimated_commission_usd == Decimal("5.00")
    assert close.normal_slippage_usd == Decimal("40.00")
    assert close.stress_slippage_usd == Decimal("60.00")
    assert close.estimated_execution_cost_usd == Decimal("65.00")
    assert close.all_in_close_cashflow_usd == Decimal("195.00")
    assert close.transition_proof.after_positions == ()
    assert close.transition_proof.verify_hash() is True
    assert close.oldest_quote_age_seconds == Decimal("1")
    assert close.maximum_leg_skew_seconds == Decimal("0")
    assert close.preview_payload()["oldest_quote_age_seconds"] == "1"
    assert close.preview_payload()["maximum_leg_skew_seconds"] == "0"
    assert close.verify_hash() is True
    assert close.review_only is True
    assert close.dry_run_only is True
    assert close.approval_enabled is False
    assert close.direct_order_submission is False
    assert [item.management_kind for item in result.candidates] == [
        PositionManagementKind.CLOSE_ALL
    ]
    assert "REDUCE_RISK_COMBO_QUANTITY_ONE" in result.suppressed_reason_codes
    assert manager.read_model()["status"] == "CANDIDATES"


def test_only_mathematically_proven_reduction_survives_payoff_and_capital_checks() -> None:
    _manager, result = _generate(snapshot=_snapshot(_Source(combo_quantity=2)))

    reductions = [
        item
        for item in result.candidates
        if item.management_kind is PositionManagementKind.REDUCE_RISK
    ]
    assert len(reductions) == 1
    reduction = reductions[0]
    assert [
        (item.contract_id, item.signed_quantity_delta)
        for item in reduction.transition_proof.deltas
    ] == [
        (784522990, -1),
        (784523001, 2),
        (784523018, -1),
    ]
    assert reduction.before_payoff.max_loss_usd == Decimal("810.00")
    assert reduction.after_payoff.max_loss_usd == Decimal("405.00")
    assert (
        reduction.transition_proof.after_risk.max_loss_usd
        < reduction.transition_proof.before_risk.max_loss_usd
    )
    assert (
        reduction.transition_proof.after_risk.exposure_usd
        < reduction.transition_proof.before_risk.exposure_usd
    )
    assert (
        reduction.transition_proof.after_risk.capital_usage_usd
        < reduction.transition_proof.before_risk.capital_usage_usd
    )
    assert reduction.executable_close_cashflow_usd == Decimal("260.00")
    assert reduction.estimated_execution_cost_usd == Decimal("65.00")
    assert reduction.all_in_close_cashflow_usd == Decimal("195.00")


def test_shape_or_any_authority_gap_returns_structured_no_trade_without_partial_candidate() -> None:
    wrong_shape_source = _Source()
    wrong = list(wrong_shape_source.position_reads[0])
    wrong[1] = replace(wrong[1], quantity=Decimal("-1"))
    wrong_shape_source.position_reads = [tuple(wrong), tuple(wrong)]

    working_source = _Source()
    working_source.order_reads = [({"order_id": 7},), ({"order_id": 7},)]

    stale_source = _Source()
    stale_source.batch = _quote_batch(
        stale_source.contracts,
        observed_at=NOW - timedelta(seconds=6),
    )

    tampered_cost = load_contract(COST_CONTRACT_PATH, as_of=NOW).to_dict()
    tampered_cost["payload"]["currency"] = "EUR"

    cases = (
        (_snapshot(working_source), _exit_contract(), None),
        (_snapshot(stale_source), _exit_contract(), None),
        (_snapshot(), {"risk_stop": "missing all other rules"}, None),
        (_snapshot(), _exit_contract(), tampered_cost),
    )
    for snapshot, exit_contract, cost_contract in cases:
        _manager, result = _generate(
            snapshot=snapshot,
            exit_contract=exit_contract,
            cost_contract=cost_contract,
        )
        assert result.no_trade is True
        assert result.status == "NO_TRADE"
        assert result.candidates == ()
        assert result.reason_codes
        assert result.approval_enabled is False

    _manager, unsupported = _generate(snapshot=_snapshot(wrong_shape_source))
    assert unsupported.no_trade is True
    assert len(unsupported.candidates) == 1
    assert unsupported.candidates[0].review_state == "PREVIEW_UNVERIFIED"
    assert unsupported.candidates[0].schema == "options_copilot.holdings_close_preview.v1"
    assert unsupported.approval_enabled is False


def test_missing_calendar_evidence_keeps_close_all_but_suppresses_residual_short() -> None:
    exit_contract = _exit_contract()
    exit_contract.pop("ex_dividend_calendar_hash")

    _manager, result = _generate(
        snapshot=_snapshot(_Source(combo_quantity=2)),
        exit_contract=exit_contract,
    )

    assert result.status == "CANDIDATES"
    assert [item.management_kind for item in result.candidates] == [
        PositionManagementKind.CLOSE_ALL
    ]
    assert "REDUCE_RISK_CALENDAR_EVIDENCE_INCOMPLETE" in result.suppressed_reason_codes


def test_hashes_bind_snapshot_secdefs_quotes_exit_cost_payoff_and_transition() -> None:
    _manager, result = _generate()
    close = result.candidates[0]
    payload = close.hash_payload()

    assert payload["broker_snapshot_hash"] == close.transition_proof.broker_snapshot_hash
    assert payload["quote_batch_hash"] == close.transition_proof.quote_batch_hash
    assert payload["exit_contract_hash"] == close.transition_proof.exit_contract_hash
    assert (
        payload["execution_cost_contract_hash"]
        == close.transition_proof.execution_cost_contract_hash
    )
    assert payload["transition_proof_hash"] == close.transition_proof.proof_hash
    assert payload["before_payoff_hash"] == close.before_payoff.payoff_hash
    assert payload["after_payoff_hash"] == close.after_payoff.payoff_hash
    assert payload["secdef_hashes"]


def test_dry_run_cli_projection_writes_only_a_review_preview(tmp_path) -> None:
    snapshot = _snapshot()
    exit_contract = _exit_contract()
    cost_contract = load_contract(COST_CONTRACT_PATH, as_of=NOW)
    generator = ManagementCandidateGenerator()

    snapshot_path = tmp_path / "atomic-snapshot.json"
    snapshot_path.write_text(
        canonical_json(
            {**snapshot.hash_payload(), "snapshot_hash": snapshot.snapshot_hash}
        ),
        encoding="utf-8",
    )
    loaded_snapshot = load_snapshot_json(snapshot_path)
    assert loaded_snapshot.verify_hash() is True

    preview = run_dry_run(
        generator,
        loaded_snapshot,
        exit_contract,
        cost_contract,
    )
    output_path = tmp_path / "gld-management-preview.json"
    written = write_dry_run_preview(preview, output_path)
    decoded = json.loads(written.read_text(encoding="utf-8"))

    assert decoded["mode"] == "DRY_RUN_PREVIEW_ONLY"
    assert decoded["review_only"] is True
    assert decoded["approval_enabled"] is False
    assert decoded["direct_order_submission"] is False
    assert decoded["external_attempt_count"] == 0
    assert decoded["instruction_count"] == 0
    assert decoded["candidates"]
