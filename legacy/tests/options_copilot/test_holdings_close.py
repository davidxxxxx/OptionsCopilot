"""Full-set close previews preserve authority gaps and exact holding economics."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal, localcontext
import json

import pytest

from options_copilot.gateway.broker_snapshot import BrokerSnapshotBuilder
from options_copilot.gateway.ibkr_readonly import BatchedOptionQuote, OptionQuoteBatch, QuoteBatchStatus
from options_copilot.positions.generator import ManagementCandidateGenerator
from options_copilot.positions.holdings_close import HoldingsClosePreview, verify_holdings_close_preview_payload
from options_copilot.positions.manager import PositionManager
from options_copilot.storage.canonical import canonical_hash
from tests.options_copilot.test_position_management_runtime_adapter import (
    NOW, _Gateway, _cost_contract, _exit_contract, _position,
)


def mixed_positions():
    return (
        _position(930000650, 650, -1, symbol="QQQ", right="P"),
        _position(930000680, 680, 1, symbol="QQQ", right="P"),
        _position(930000760, 760, 1, symbol="QQQ", right="C"),
    )


class HoldingsGateway(_Gateway):
    def __init__(self, *, positions=None, exchange_time=None, **kwargs):
        super().__init__(positions=positions or mixed_positions(), **kwargs)
        self.exchange_time = NOW - timedelta(seconds=1) if exchange_time is None else exchange_time
        self.missing_source_time = False
        self.missing_bid = False

    def option_quote_batch(self, contracts):
        self.quote_requests.append(tuple(contracts))
        observed = self.quote_observed_at or NOW - timedelta(seconds=1)
        return OptionQuoteBatch(
            batch_id="holdings-close-batch", status=QuoteBatchStatus.COMPLETE,
            requested_at=min(NOW - timedelta(seconds=2), observed), completed_at=NOW,
            source="IBKR_REQ_TICKERS_READONLY",
            quotes=tuple(BatchedOptionQuote(
                contract_id=item.contract_id, batch_id="holdings-close-batch",
                request_id=f"holding-{item.contract_id}",
                requested_at=min(NOW - timedelta(seconds=2), observed),
                completed_at=NOW, observed_at=observed,
                source="IBKR_REQ_TICKERS_READONLY",
                bid=None if self.missing_bid else Decimal("0.10001"), ask=Decimal("0.15003"),
                exchange_time=None if self.missing_source_time else self.exchange_time,
            ) for item in contracts),
        )


def snapshot(gateway=None):
    source = gateway or HoldingsGateway()
    from options_copilot.positions.runtime_adapter import _contracts_from_positions
    contracts = _contracts_from_positions(source.positions(), discovered_at=NOW)
    return BrokerSnapshotBuilder(source, clock=lambda: NOW).build(contracts)


def generate(gateway=None, *, exit_contract=None, cost_contract=None):
    manager = PositionManager()
    result = ManagementCandidateGenerator(manager).generate(
        snapshot(gateway), {} if exit_contract is None else exit_contract,
        _cost_contract() if cost_contract is None else cost_contract,
    )
    return manager, result


def test_complete_mixed_right_set_is_unverified_no_trade_with_exact_all_close():
    manager, result = generate()
    assert result.status == "NO_TRADE"
    assert result.no_trade
    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert isinstance(candidate, HoldingsClosePreview)
    assert candidate.review_state == "PREVIEW_UNVERIFIED"
    assert candidate.scope == "ALL_OBSERVED_OPTION_HOLDINGS"
    assert candidate.grouping_status == "NOT_INFERRED"
    assert candidate.transition_proof.after_positions == ()
    assert [row.signed_quantity_delta for row in candidate.transition_proof.deltas] == [1, -1, -1]
    assert candidate.transition_proof.verify_hash()
    assert candidate.verify_hash() and result.verify_hash()
    assert manager.read_model()["result_hash"] == result.result_hash
    assert "CALENDAR_EVIDENCE_UNVERIFIED" in result.reason_codes
    assert result.as_dict()["decision"] == "NO_TRADE"


def test_exact_signed_cashflow_and_one_stress_exit_reserve_not_normal_plus_stress():
    _manager, result = generate()
    preview = result.candidates[0]
    assert preview.gross_component_liquidation_cashflow_usd == Decimal("4.99900")
    assert preview.normal_slippage_usd == Decimal("7.51")
    assert preview.stress_slippage_usd == Decimal("11.26")
    assert preview.estimated_commission_usd == Decimal("3.75")
    assert preview.estimated_execution_cost_usd == Decimal("15.01")
    assert preview.all_in_close_cashflow_usd == Decimal("-10.01100")
    assert preview.before_payoff.max_loss_usd == Decimal("20.00900")
    assert preview.before_payoff.min_intrinsic_usd == 0
    assert preview.before_payoff.unbounded_profit


def test_wire_checksum_binds_nested_proof_and_payoff_and_has_no_strategy_fields():
    _manager, result = generate()
    payload = json.loads(json.dumps(result.candidates[0].preview_payload()))
    assert verify_holdings_close_preview_payload(payload)
    assert canonical_hash({key: value for key, value in payload.items() if key != "candidate_hash"}) == payload["candidate_hash"]
    assert not set(payload).intersection({"structure", "unit_ratio", "combo_quantity_before", "combo_quantity_after", "exit_plan", "entry_net_cost_usd"})
    assert payload["after_risk_condition"] == "ALL_LEGS_FILLED_AND_RECONCILED"
    assert payload["order_allowed"] is payload["instruction_creation_allowed"] is payload["affects_eligibility"] is False
    payload["transition_proof"]["deltas"][0]["signed_quantity_delta"] = 99
    assert not verify_holdings_close_preview_payload(payload)


@pytest.mark.parametrize("mutation", ["missing_bid", "missing_source_time", "stale_source", "stale_receipt", "orders", "instructions", "secdefs"])
def test_authority_or_quote_gaps_never_produce_partial_financial_preview(mutation):
    kwargs = {}
    if mutation == "orders":
        kwargs["working_orders"] = ({"order_id": 42},)
    elif mutation == "instructions":
        kwargs["instructions"] = ({"instruction_id": "x"},)
    elif mutation == "secdefs":
        kwargs["secdef_complete"] = False
    elif mutation == "stale_source":
        kwargs["exchange_time"] = NOW - timedelta(seconds=6)
    elif mutation == "stale_receipt":
        kwargs["quote_observed_at"] = NOW - timedelta(seconds=6)
    gateway = HoldingsGateway(**kwargs)
    if mutation in {"missing_bid", "missing_source_time"}:
        setattr(gateway, mutation, True)
    _manager, result = generate(gateway)
    assert result.no_trade and result.candidates == ()
    assert result.reason_codes


@pytest.mark.parametrize("count", [1, 4, 8])
def test_bounded_complete_long_holdings_have_no_arithmetic_grouping(count):
    positions = tuple(_position(940000000 + index, 650 + index, 2, symbol="QQQ") for index in range(count))
    _manager, result = generate(HoldingsGateway(positions=positions))
    assert len(result.candidates) == 1
    payload = result.candidates[0].preview_payload()
    assert len(payload["execution_legs"]) == count
    assert all(leg["signed_quantity_delta"] == -2 for leg in payload["execution_legs"])
    assert payload["grouping_status"] == "NOT_INFERRED"


def test_nine_legs_and_uncovered_short_and_mixed_expiry_remain_empty():
    many = tuple(_position(940000000 + index, 650 + index, 1, symbol="QQQ") for index in range(9))
    short = (mixed_positions()[0],)
    expiry = list(mixed_positions())
    expiry[-1] = replace(expiry[-1], expiration=date(2026, 9, 18))
    for positions in (many, short, tuple(expiry)):
        _manager, result = generate(HoldingsGateway(positions=positions))
        assert result.no_trade and result.candidates == ()


def test_entry_cost_and_exit_thesis_do_not_change_full_set_preview():
    _manager, baseline = generate()
    positions = tuple(replace(row, average_cost=Decimal("999999")) for row in mixed_positions())
    _manager, changed = generate(HoldingsGateway(positions=positions), exit_contract=_exit_contract())
    before, after = baseline.candidates[0], changed.candidates[0]
    assert before.before_payoff.payoff_hash == after.before_payoff.payoff_hash
    assert before.all_in_close_cashflow_usd == after.all_in_close_cashflow_usd
    assert "exit_plan" not in after.preview_payload()


def test_invalid_cost_signature_stays_empty():
    bad = _cost_contract().to_dict()
    bad["payload"]["currency"] = "EUR"
    _manager, result = generate(cost_contract=bad)
    assert result.candidates == () and result.no_trade


def test_preview_properties_and_hash_are_independent_of_ambient_precision():
    _manager, result = generate()
    preview = result.candidates[0]
    payload = preview.preview_payload()
    with localcontext() as context:
        context.prec = 3
        assert preview.all_in_close_cashflow_usd == Decimal("-10.01100")
        assert preview.estimated_execution_cost_usd == Decimal("15.01")
        assert preview.preview_payload() == payload
        assert preview.verify_hash()
        assert verify_holdings_close_preview_payload(payload)


def test_dataclass_rejects_partial_or_repriced_execution_legs():
    _manager, result = generate()
    preview = result.candidates[0]
    with pytest.raises(ValueError):
        replace(preview, execution_legs=preview.execution_legs[:-1])
    legs = [dict(item) for item in preview.execution_legs]
    legs[0]["executable_price"] = Decimal("0.12")
    with pytest.raises(ValueError):
        replace(preview, execution_legs=tuple(legs), candidate_hash="0" * 64)


@pytest.mark.parametrize("change", ["different_strike", "extra_leg", "different_symbol", "different_expiration"])
def test_dataclass_rejects_hash_valid_geometry_from_a_different_holdings_set(change):
    _manager, result = generate()
    preview = result.candidates[0]
    legs = list(preview.before_payoff.legs)
    if change == "different_strike":
        legs[-1] = replace(legs[-1], strike=Decimal("800"))
    elif change == "extra_leg":
        legs.append(replace(legs[-1], contract_id=990000800, strike=Decimal("800")))
    elif change == "different_symbol":
        with pytest.raises(ValueError):
            replace(preview, symbol="GLD", candidate_hash="0" * 64)
        return
    else:
        with pytest.raises(ValueError):
            replace(preview, expiration=date(2026, 9, 18), candidate_hash="0" * 64)
        return
    changed_payoff = replace(preview.before_payoff, legs=tuple(legs))
    assert changed_payoff.verify_hash()
    assert changed_payoff.max_loss_usd == preview.before_payoff.max_loss_usd
    with pytest.raises(ValueError):
        replace(preview, before_payoff=changed_payoff, candidate_hash="0" * 64)


@pytest.mark.parametrize("change", [
    "payoff_max_loss", "before_risk", "proof_risk", "after_risk", "source_time",
    "approval", "order", "grouping", "calendar", "scope",
])
def test_rehashed_internally_inconsistent_wire_documents_are_rejected(change):
    _manager, result = generate()
    payload = result.candidates[0].preview_payload()
    if change == "payoff_max_loss":
        payoff = payload["before_payoff"]
        payoff["max_loss_usd"] = "0"
        payoff["payoff_hash"] = canonical_hash({key: value for key, value in payoff.items() if key != "payoff_hash"})
    elif change == "before_risk":
        payload["before_risk"]["max_loss_usd"] = "0"
    elif change == "proof_risk":
        payload["transition_proof"]["before_risk"]["max_loss_usd"] = "0"
    elif change == "after_risk":
        payload["after_risk"]["max_loss_usd"] = "1"
    elif change == "source_time":
        payload["oldest_quote_at"] = "2026-08-04T16:59:58.000000Z"
    elif change == "approval":
        payload["approval_enabled"] = True
    elif change == "order":
        payload["order_allowed"] = True
    elif change == "grouping":
        payload["grouping_status"] = "CONFIRMED"
    elif change == "calendar":
        payload["calendar_evidence_status"] = "CLEAR"
    else:
        payload["proof_scope"] = "EXECUTION_READY"
    payload["candidate_hash"] = canonical_hash({key: value for key, value in payload.items() if key != "candidate_hash"})
    assert not verify_holdings_close_preview_payload(payload)


def test_synchronized_payoff_and_execution_strike_change_cannot_reuse_original_secdef_hash():
    _manager, result = generate()
    preview = result.candidates[0]
    payload = preview.preview_payload()
    legs = list(preview.before_payoff.legs)
    legs[-1] = replace(legs[-1], strike=Decimal("800"))
    payload["before_payoff"] = replace(preview.before_payoff, legs=tuple(legs)).as_dict()
    payload["execution_legs"][-1]["strike"] = "800"
    payload["candidate_hash"] = canonical_hash({key: value for key, value in payload.items() if key != "candidate_hash"})
    assert not verify_holdings_close_preview_payload(payload)


@pytest.mark.parametrize("key", ["before_risk", "before_positions", "secdef_bindings", "capital_usage"])
def test_malformed_rehashed_nested_proof_never_raises_from_wire_verifier(key):
    _manager, result = generate()
    payload = result.candidates[0].preview_payload()
    payload["transition_proof"][key] = [17]
    payload["candidate_hash"] = canonical_hash({name: value for name, value in payload.items() if name != "candidate_hash"})
    assert verify_holdings_close_preview_payload(payload) is False
