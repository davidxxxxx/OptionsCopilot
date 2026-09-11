from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.approval import proposal_hashes
from options_copilot.bridge.store import _executable_cost_usd
from options_copilot.domain import TerminalScenario
from options_copilot.gateway.broker_snapshot import AtomicBrokerSnapshot, BrokerSnapshotStatus, SecDefEvidence, StateComponentEvidence
from options_copilot.gateway.ibkr_readonly import BatchedOptionQuote, OptionContractRef, OptionQuoteBatch, OptionSecDefSnapshot, QuoteBatchStatus
from options_copilot.governance.contracts import ContractKind, sign_contract
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.proposals import validate_proposal
from options_copilot.runtime import _require_frozen_contract_bindings
from options_copilot.storage.canonical import canonical_hash
from options_copilot.strategies import ExitPlan, StrategyCandidateGenerator, StrategyKind, StrategyTemplateRegistry, TemplateLeg


NOW = datetime(2026, 8, 4, 15, tzinfo=timezone.utc)
HASH = "a" * 64


def _secdef(con_id: int, strike: str, right: str = "C") -> OptionSecDefSnapshot:
    return OptionSecDefSnapshot(con_id, f"SPY 260821{right}{strike}", "SPY", 100, "SMART", date(2026, 8, 21), Decimal(strike), right, "OPT", "USD", True, False, "IBKR")


def _batch(*, with_iv: bool = True) -> OptionQuoteBatch:
    requested = NOW - timedelta(seconds=2)
    quotes = tuple(BatchedOptionQuote(con_id, "batch-1", f"request-{con_id}", requested, NOW - timedelta(seconds=1), NOW, "IBKR", Decimal(bid), Decimal(ask), exchange_time=NOW - timedelta(seconds=1), volume=20, open_interest=200, implied_volatility=(Decimal(iv) if with_iv else None), delta=Decimal(delta), gamma=Decimal("0.02"), theta=Decimal("-0.08"), vega=Decimal("0.11"), market_data_type=1) for con_id, bid, ask, iv, delta in ((101, "2.00", "2.10", "0.20", "0.55"), (102, "1.00", "1.10", "0.22", "0.35")))
    return OptionQuoteBatch("batch-1", QuoteBatchStatus.COMPLETE, requested, NOW, "IBKR", quotes)


def _broker(batch: OptionQuoteBatch) -> AtomicBrokerSnapshot:
    secdefs = (_secdef(101, "100"), _secdef(102, "105"))
    evidence = tuple(SecDefEvidence(value.contract_id, {"conId": value.contract_id}, {"conId": value.contract_id}, "f" * 64, "f" * 64, True, True, False, "IBKR", "IBKR") for value in secdefs)
    account = {"currency": "USD", "net_liquidation": Decimal("2000")}
    fields = dict(built_at=NOW, status=BrokerSnapshotStatus.COMPLETE, reason_codes=(), state_evidence={"account": StateComponentEvidence("account", True, 1, canonical_hash(account), canonical_hash(account), True, account)}, secdef_evidence=evidence, quote_batch_id=batch.batch_id, quote_batch_status=batch.status, quote_batch_source=batch.source, quote_batch_requested_at=batch.requested_at, quote_batch_completed_at=batch.completed_at, quotes=batch.quotes, oldest_quote_age_seconds=Decimal("1"), maximum_leg_skew_seconds=Decimal("0"))
    first = AtomicBrokerSnapshot(**fields, snapshot_hash="0" * 64)
    return AtomicBrokerSnapshot(**fields, snapshot_hash=canonical_hash(first.hash_payload()))


def _nav() -> StrategyNavSnapshot:
    fields = dict(asof=NOW, strategy_nav=Decimal("2000"), strategy_deposits=Decimal("0"), strategy_withdrawals=Decimal("0"), realized_pnl=Decimal("0"), open_position_unrealized_pnl=Decimal("0"), fees=Decimal("0"), signed_corrections=Decimal("0"), non_strategy_contribution=Decimal("0"), fill_principal_contribution=Decimal("0"), observed_account_nlv=Decimal("2000"), reconciliation_difference=Decimal("0"), contract_version="v1", contract_hash=HASH, ledger_head_hash=HASH, valid=True, no_trade_reasons=())
    return StrategyNavSnapshot(**fields, content_hash=canonical_hash(fields))


def _contracts(*, assignment_supported: bool = True) -> tuple[dict[str, object], dict[str, object]]:
    signed = {"schema": "options_copilot.governance.signed_contract.v1", "actor": "human:test", "signed_at": "2026-08-01T00:00:00Z", "effective_at": "2026-08-01T00:00:00Z"}
    assignment = (
        {"status": "SUPPORTED", "evidence_hash": "8" * 64}
        if assignment_supported
        else {}
    )
    cost = {**signed, "version": "v1", "contract_hash": HASH, "payload": {"commission_and_fees": {}, "quote_spread_and_slippage": {}, "assignment_exercise_and_dividend": assignment}}
    policy = {**signed, "version": "v1", "contract_hash": "b" * 64, "payload": {"hard_no_trade_thresholds": {"cost_and_expectancy": {"execution_cost_contract_version": "v1", "execution_cost_contract_hash": HASH}}}}
    return cost, policy


def _finalist() -> dict[str, object]:
    return {"candidate_id": "vertical-1", "structure": "DEBIT_VERTICAL", "legs": ({"con_id": 101, "side": "LONG"}, {"con_id": 102, "side": "SHORT"}), "terminal_scenarios": (TerminalScenario(Decimal("100"), Decimal("0.5")), TerminalScenario(Decimal("105"), Decimal("0.5"))), "exit_plan": {"thesis_invalidation": "trend reverses", "risk_stop": "close at 50 percent loss", "profit_take": "close at 50 percent gain", "time_stop": "close before expiry", "maximum_holding_date": "2026-08-20", "bad_quote_action": "do not trade"}, "event_evidence_status": "AVAILABLE", "earnings_overlap": False, "event_defined": False, "event_evidence_hash": "9" * 64}


def _generate(*, positions=(), finalist: dict[str, object] | None = None, with_iv: bool = True, assignment_supported: bool = True, batch: OptionQuoteBatch | None = None, execution_cost_contract: dict[str, object] | None = None):
    quote_batch = batch or _batch(with_iv=with_iv)
    cost, policy = _contracts(assignment_supported=assignment_supported)
    if execution_cost_contract is not None:
        cost = execution_cost_contract
        policy["payload"]["hard_no_trade_thresholds"]["cost_and_expectancy"] = {
            "execution_cost_contract_version": cost["version"],
            "execution_cost_contract_hash": cost["contract_hash"],
        }
    return StrategyCandidateGenerator().generate((finalist or _finalist(),), secdefs=(_secdef(101, "100"), _secdef(102, "105")), quote_batch=quote_batch, nav_snapshot=_nav(), broker_snapshot=_broker(quote_batch), execution_cost_contract=cost, policy_contract=policy, evidence_hashes={"MARKET": "c" * 64, "VOLATILITY": "d" * 64, "LIQUIDITY": "e" * 64}, now=NOW, positions=positions)


def _legacy_assignment_cost(*, version: str = "v1") -> dict[str, object]:
    payload = {
        "commission_and_fees": {},
        "quote_spread_and_slippage": {},
        "assignment_exercise_and_dividend": {
            "assignment": {"planned_assignment_allowed": False},
            "exercise": {"planned_exercise_allowed": False},
            "early_exercise": {"review_required": True},
            "ex_dividend": {"review_required": True},
            "short_leg_exit_deadline": "close before assignment or dividend exposure",
        },
    }
    first = sign_contract(
        kind=ContractKind.EXECUTION_COST,
        version="v1",
        effective_at=NOW - timedelta(days=2),
        provenance={
            "source": "test-installed-contract",
            "source_hash": "7" * 64,
            "observed_at": NOW - timedelta(days=2),
        },
        payload=payload,
        actor="human:test",
        signed_at=NOW - timedelta(days=1),
    )
    if version == "v1":
        return first.to_dict()
    return sign_contract(
        kind=ContractKind.EXECUTION_COST,
        version=version,
        effective_at=NOW - timedelta(hours=12),
        provenance={
            "source": "test-installed-contract-correction",
            "source_hash": "6" * 64,
            "observed_at": NOW - timedelta(hours=12),
        },
        payload=payload,
        actor="human:test",
        signed_at=NOW - timedelta(hours=6),
        supersedes_version=first.version,
        supersedes_hash=first.contract_hash,
    ).to_dict()


def test_registry_is_closed_and_rejects_uncovered_ratio_short() -> None:
    assert StrategyTemplateRegistry().kinds == tuple(StrategyKind)
    result = _generate()
    assert result.status == "CANDIDATES"
    assert result.candidates[0].hash_payload()["symbol"] == "SPY"
    assert result.candidates[0].max_loss_usd == Decimal("130.00")
    assert result.candidates[0].candidate_hash != "0" * 64
    assert result.candidates[0].event_evidence_status == "AVAILABLE"
    assert result.candidates[0].earnings_overlap is False
    assert result.candidates[0].event_evidence_hash == "9" * 64


def test_complete_per_leg_quote_evidence_is_preserved_and_hash_bound() -> None:
    generated = _generate().candidates[0]
    body = generated.hash_payload()
    proposal = generated.proposal_payload()

    assert body["legs"][0]["delta"] == "0.55"  # type: ignore[index]
    assert body["legs"][0]["gamma"] == "0.02"  # type: ignore[index]
    assert body["legs"][0]["theta"] == "-0.08"  # type: ignore[index]
    assert body["legs"][0]["vega"] == "0.11"  # type: ignore[index]
    assert body["legs"][0]["market_data_type"] == 1  # type: ignore[index]
    assert body["legs"][0]["freshness_basis"] == "EXCHANGE_TIME"  # type: ignore[index]
    assert body["legs"][0]["quote_age_seconds"] == "1.0"  # type: ignore[index]
    assert body["legs"][0]["volume"] == 20  # type: ignore[index]
    assert body["legs"][0]["open_interest"] == 200  # type: ignore[index]
    assert proposal["legs"][1]["short_leg_risk_evidence"]["status"] == "SUPPORTED"  # type: ignore[index]

    mutated_leg = replace(generated.candidate.leg_quotes[0], delta=Decimal("0.54"))
    mutated_candidate = replace(
        generated.candidate,
        leg_quotes=(mutated_leg, generated.candidate.leg_quotes[1]),
    )
    mutated = replace(
        generated,
        candidate=mutated_candidate,
        candidate_hash="0" * 64,
    )
    assert canonical_hash(mutated.hash_payload()) != generated.candidate_hash


def test_wide_primary_vertical_does_not_hide_liquid_adjacent_vertical() -> None:
    requested = NOW - timedelta(seconds=2)
    secdefs = (
        _secdef(101, "100"),
        _secdef(102, "105"),
        _secdef(103, "110"),
    )
    quotes = tuple(
        BatchedOptionQuote(
            con_id,
            "batch-adjacent",
            f"request-{con_id}",
            requested,
            NOW - timedelta(seconds=1),
            NOW,
            "IBKR",
            Decimal(bid),
            Decimal(ask),
            exchange_time=NOW - timedelta(seconds=1),
            volume=100,
            open_interest=1000,
            implied_volatility=Decimal("0.25"),
            delta=Decimal(delta),
            gamma=Decimal("0.02"),
            theta=Decimal("-0.08"),
            vega=Decimal("0.11"),
            market_data_type=1,
        )
        for con_id, bid, ask, delta in (
            (101, "2.00", "2.80", "0.60"),
            (102, "1.45", "1.55", "0.45"),
            (103, "0.75", "0.85", "0.30"),
        )
    )
    batch = OptionQuoteBatch(
        "batch-adjacent",
        QuoteBatchStatus.COMPLETE,
        requested,
        NOW,
        "IBKR",
        quotes,
    )
    evidence = tuple(
        SecDefEvidence(
            value.contract_id,
            {"conId": value.contract_id},
            {"conId": value.contract_id},
            "f" * 64,
            "f" * 64,
            True,
            True,
            False,
            "IBKR",
            "IBKR",
        )
        for value in secdefs
    )
    account = {"currency": "USD", "net_liquidation": Decimal("2000")}
    snapshot_fields = dict(
        built_at=NOW,
        status=BrokerSnapshotStatus.COMPLETE,
        reason_codes=(),
        state_evidence={
            "account": StateComponentEvidence(
                "account",
                True,
                1,
                canonical_hash(account),
                canonical_hash(account),
                True,
                account,
            )
        },
        secdef_evidence=evidence,
        quote_batch_id=batch.batch_id,
        quote_batch_status=batch.status,
        quote_batch_source=batch.source,
        quote_batch_requested_at=batch.requested_at,
        quote_batch_completed_at=batch.completed_at,
        quotes=batch.quotes,
        oldest_quote_age_seconds=Decimal("1"),
        maximum_leg_skew_seconds=Decimal("0"),
    )
    unhashed = AtomicBrokerSnapshot(**snapshot_fields, snapshot_hash="0" * 64)
    snapshot = AtomicBrokerSnapshot(
        **snapshot_fields,
        snapshot_hash=canonical_hash(unhashed.hash_payload()),
    )
    primary = _finalist()
    primary["candidate_id"] = "wide-primary"
    alternate = _finalist()
    alternate["candidate_id"] = "liquid-alternate"
    alternate["legs"] = (
        {"con_id": 102, "side": "LONG"},
        {"con_id": 103, "side": "SHORT"},
    )
    cost, policy = _contracts()

    result = StrategyCandidateGenerator().generate(
        (primary, alternate),
        secdefs=secdefs,
        quote_batch=batch,
        nav_snapshot=_nav(),
        broker_snapshot=snapshot,
        execution_cost_contract=cost,
        policy_contract=policy,
        evidence_hashes={
            "MARKET": "c" * 64,
            "VOLATILITY": "d" * 64,
            "LIQUIDITY": "e" * 64,
        },
        now=NOW,
    )

    assert result.status == "CANDIDATES"
    assert tuple(item.candidate_id for item in result.candidates) == (
        "liquid-alternate",
    )


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    (
        ("delta", None, "QUOTE_GREEKS_INCOMPLETE_OR_INVALID"),
        ("gamma", Decimal("-0.01"), "QUOTE_GREEKS_INCOMPLETE_OR_INVALID"),
        ("implied_volatility", None, "QUOTE_IV_UNAVAILABLE_OR_INVALID"),
        ("market_data_type", 3, "QUOTE_MARKET_DATA_NOT_LIVE"),
        ("exchange_time", NOW - timedelta(seconds=6), "QUOTE_STALE_OR_FUTURE"),
    ),
)
def test_incomplete_or_non_live_quote_evidence_fails_closed(
    field: str,
    value: object,
    reason: str,
) -> None:
    batch = _batch()
    changed = replace(batch.quotes[0], **{field: value})
    changed_batch = replace(batch, quotes=(changed, batch.quotes[1]))

    result = _generate(batch=changed_batch)

    assert result.no_trade
    assert result.reason_codes == (reason,)


def test_missing_short_leg_assignment_exercise_dividend_proof_fails_closed() -> None:
    result = _generate(assignment_supported=False)

    assert result.no_trade
    assert result.reason_codes == (
        "ASSIGNMENT_EXERCISE_EX_DIVIDEND_EVIDENCE_UNAVAILABLE",
    )


def test_verified_legacy_v1_assignment_policy_is_supported() -> None:
    result = _generate(execution_cost_contract=_legacy_assignment_cost())

    assert result.status == "CANDIDATES"
    proposal = result.candidates[0].proposal_payload()
    assert proposal["legs"][1]["short_leg_risk_evidence"]["status"] == "SUPPORTED"


@pytest.mark.parametrize("mutation", ("FORGED", "FUTURE_VERSION"))
def test_unverified_or_non_v1_legacy_assignment_policy_is_not_promoted(
    mutation: str,
) -> None:
    cost = _legacy_assignment_cost(version="v2" if mutation == "FUTURE_VERSION" else "v1")
    if mutation == "FORGED":
        cost["payload"]["assignment_exercise_and_dividend"]["assignment"] = {
            "planned_assignment_allowed": True,
        }

    result = _generate(execution_cost_contract=cost)

    assert result.no_trade
    assert result.reason_codes == (
        "ASSIGNMENT_EXERCISE_EX_DIVIDEND_EVIDENCE_UNAVAILABLE",
    )


def test_any_option_position_is_structured_management_only() -> None:
    result = _generate(
        positions=(
            {
                "symbol": "QQQ",
                "security_type": "OPT",
                "quantity": Decimal("1"),
            },
        )
    )
    assert result.no_trade
    assert result.reason_codes == ("POSITION_MANAGEMENT_ONLY",)


def test_stock_position_does_not_trigger_option_management_only() -> None:
    result = _generate(
        positions=(
            {
                "symbol": "GLD",
                "security_type": "STK",
                "quantity": Decimal("1"),
            },
        )
    )
    assert result.status == "CANDIDATES"


def test_missing_required_binding_never_emits_candidate() -> None:
    batch = _batch()
    cost, policy = _contracts()
    result = StrategyCandidateGenerator().generate((_finalist(),), secdefs=(_secdef(101, "100"), _secdef(102, "105")), quote_batch=batch, nav_snapshot=_nav(), broker_snapshot=_broker(batch), execution_cost_contract=cost, policy_contract=policy, evidence_hashes={"MARKET": "c" * 64}, now=NOW)
    assert result.no_trade
    assert result.candidates == ()


def test_generator_rejects_nav_not_reconciled_to_same_atomic_account() -> None:
    batch = _batch()
    cost, policy = _contracts()
    nav = _nav()
    mismatched = replace(
        nav,
        observed_account_nlv=Decimal("1999"),
        reconciliation_difference=Decimal("-1"),
        content_hash="0" * 64,
    )
    mismatched = replace(
        mismatched,
        content_hash=canonical_hash(mismatched.hash_payload()),
    )

    result = StrategyCandidateGenerator().generate(
        (_finalist(),),
        secdefs=(_secdef(101, "100"), _secdef(102, "105")),
        quote_batch=batch,
        nav_snapshot=mismatched,
        broker_snapshot=_broker(batch),
        execution_cost_contract=cost,
        policy_contract=policy,
        evidence_hashes={
            "MARKET": "c" * 64,
            "VOLATILITY": "d" * 64,
            "LIQUIDITY": "e" * 64,
        },
        now=NOW,
    )

    assert result.no_trade
    assert result.reason_codes == ("STRATEGY_NAV_BROKER_NLV_MISMATCH",)


def test_generated_candidate_emits_complete_deterministic_review_only_proposal() -> None:
    generated = _generate().candidates[0]

    first = generated.proposal_payload()
    second = generated.proposal_payload()

    assert first == second
    assert canonical_hash(first) == canonical_hash(second)
    assert first["schema"] == "options_copilot.proposal.v1"
    assert first["review_only"] is True
    assert first["rank"] == 1
    assert first["eligible_to_send"] is True
    assert first["proposal_id"] == first["candidate_id"] == "vertical-1"
    assert first["symbol"] == first["underlying"] == "SPY"
    assert first["expiration"] == "2026-08-21"
    assert first["quote_snapshot_id"] == "batch-1"
    assert first["expected_value_usd"] == "120"
    assert first["expected_value_before_costs_usd"] == "140"
    assert first["terminal_scenarios"] == [
        {"terminal_underlying_price": "100", "probability": "0.5"},
        {"terminal_underlying_price": "105", "probability": "0.5"},
    ]
    assert first["strategy_nav"] == {
        "strategy_nav_usd": "2000",
        "authority_hash": generated.strategy_nav_hash,
        "content_hash": generated.strategy_nav_content_hash,
        "contract_hash": generated.strategy_nav_contract_hash,
        "ledger_head_hash": generated.strategy_nav_ledger_head_hash,
        "observed_account_nlv": "2000",
        "reconciliation_difference": "0",
        "asof": "2026-08-04T15:00:00Z",
    }
    assert first["pricing"] == {
        "reference_cost_usd": "110",
        "estimated_commissions_usd": "5",
        "estimated_slippage_usd": "15",
        "estimated_execution_costs_usd": "20",
        "all_in_executable_cost_usd": "130",
        "net_debit_usd": "110",
    }
    assert first["risk"]["defined_risk"] is True
    assert first["risk"]["maximum_loss_usd"] == "130"
    assert first["risk"]["risk_fraction"] == "0.065"
    assert first["exit_plan"] == generated.exit_plan.as_dict()

    assert first["legs"] == [
        {
            "con_id": 101,
            "contract_id_ex": "101@SMART",
            "underlying": "SPY",
            "security_type": "OPT",
            "expiration": "2026-08-21",
            "strike": "100",
            "right": "CALL",
            "side": "BUY",
            "quantity": 1,
            "ratio": 1,
            "multiplier": "100",
            "currency": "USD",
            "exchange": "SMART",
            "bid": "2",
            "ask": "2.1",
            "quote_time": "2026-08-04T14:59:59Z",
            "quote_snapshot_id": "batch-1",
            "delta": "0.55",
            "gamma": "0.02",
            "theta": "-0.08",
            "vega": "0.11",
            "exchange_time": "2026-08-04T14:59:59Z",
            "requested_at": "2026-08-04T14:59:58Z",
            "completed_at": "2026-08-04T15:00:00Z",
            "market_data_type": 1,
            "quote_age_seconds": "1",
            "freshness_basis": "EXCHANGE_TIME",
            "short_leg_risk_evidence": {
                "status": "NOT_APPLICABLE",
                "reason_codes": [],
                "evidence_hash": None,
            },
            "implied_volatility": "0.2",
            "volume": 20,
            "open_interest": 200,
        },
        {
            "con_id": 102,
            "contract_id_ex": "102@SMART",
            "underlying": "SPY",
            "security_type": "OPT",
            "expiration": "2026-08-21",
            "strike": "105",
            "right": "CALL",
            "side": "SELL",
            "quantity": 1,
            "ratio": 1,
            "multiplier": "100",
            "currency": "USD",
            "exchange": "SMART",
            "bid": "1",
            "ask": "1.1",
            "quote_time": "2026-08-04T14:59:59Z",
            "quote_snapshot_id": "batch-1",
            "delta": "0.35",
            "gamma": "0.02",
            "theta": "-0.08",
            "vega": "0.11",
            "exchange_time": "2026-08-04T14:59:59Z",
            "requested_at": "2026-08-04T14:59:58Z",
            "completed_at": "2026-08-04T15:00:00Z",
            "market_data_type": 1,
            "quote_age_seconds": "1",
            "freshness_basis": "EXCHANGE_TIME",
            "short_leg_risk_evidence": {
                "status": "SUPPORTED",
                "reason_codes": [],
                "evidence_hash": generated.candidate.leg_quotes[1].short_leg_risk_evidence_hash,
            },
            "implied_volatility": "0.22",
            "volume": 20,
            "open_interest": 200,
        },
    ]

    contracts = tuple(
        OptionContractRef(
            contract_id=secdef.contract_id,
            contract_id_ex=f"{secdef.contract_id}@{secdef.exchange}",
            symbol="SPY",
            local_symbol=secdef.local_symbol,
            expiration=secdef.expiration,
            strike=secdef.strike,
            right=secdef.right,
            exchange=secdef.exchange,
            trading_class=secdef.trading_class,
            multiplier=secdef.multiplier,
            currency=secdef.currency,
        )
        for secdef in (_secdef(101, "100"), _secdef(102, "105"))
    )
    _require_frozen_contract_bindings(
        candidate_id=generated.candidate_id,
        candidate_body=generated.hash_payload(),
        proposal_body=first,
        contracts=contracts,
    )
    validated = validate_proposal(
        first,
        account_equity=Decimal("2000"),
        open_combinations=0,
        now=NOW,
        quote_fresh_seconds=Decimal("5"),
        expected_quote_snapshot_id="batch-1",
    )
    assert validated.maximum_loss_usd == Decimal("130")
    assert validated.expected_value_usd == Decimal("120")
    assert _executable_cost_usd(first) == Decimal("110")
    hashes, frozen = proposal_hashes(first)
    assert hashes.proposal_hash == canonical_hash(first)
    assert frozen["proposal_id"] == "vertical-1"

    forbidden = ("order", "transport", "transmit", "submit", "secret", "token", "credential")

    def keys(value: object) -> tuple[str, ...]:
        if isinstance(value, dict):
            return tuple(str(key).lower() for key in value) + sum((keys(item) for item in value.values()), ())
        if isinstance(value, list):
            return sum((keys(item) for item in value), ())
        return ()

    assert not any(marker in key for key in keys(first) for marker in forbidden)


def test_generated_candidate_hash_binds_prospective_outcome_capture_plan() -> None:
    finalist = _finalist()
    baseline = {
        "symbol": "SPY",
        "price": Decimal("100"),
        "observed_at": NOW - timedelta(seconds=1),
        "source": "IBKR_READ_ONLY_BASELINE",
        "source_id": "underlying:756733:SPY",
        "source_hash": "7" * 64,
    }
    finalist["outcome_capture_baseline"] = {
        "schema": "options_copilot.outcome_capture_baseline.v1",
        "benchmark_symbol": "SPY",
        "underlying": baseline,
        "benchmark": baseline,
    }

    generated = _generate(finalist=finalist, with_iv=True).candidates[0]
    body = generated.hash_payload()
    plan = body["outcome_capture_plan"]

    assert plan["status"] == "READY"  # type: ignore[index]
    assert plan["underlying"]["price"] == Decimal("100")  # type: ignore[index]
    assert plan["benchmark"]["symbol"] == "SPY"  # type: ignore[index]
    assert len(plan["legs"]) == 2  # type: ignore[arg-type,index]
    assert plan["legs"][0]["contract"]["local_symbol"]  # type: ignore[index]
    assert canonical_hash(body) == generated.candidate_hash


def test_generated_candidate_hash_binds_underlying_quote_basis() -> None:
    finalist = _finalist()
    basis = {
        "schema": "options_copilot.underlying_quote_basis.v1",
        "symbol": "SPY",
        "contract_id": 756733,
        "exchange": "NASDAQ",
        "source": "IBKR_REQ_TICKERS_READONLY",
        "observed_at": (NOW - timedelta(seconds=1)).isoformat(),
        "bid": "99.90",
        "ask": "100.10",
        "last": "100",
        "close": "99",
        "market_data_type": 1,
    }
    finalist["underlying_quote_basis"] = basis
    finalist["underlying_quote_basis_hash"] = canonical_hash(basis)

    generated = _generate(finalist=finalist).candidates[0]
    body = generated.hash_payload()

    assert body["underlying_quote_basis"] == basis
    assert body["underlying_quote_basis_hash"] == canonical_hash(basis)
    assert generated.candidate_hash == canonical_hash(body)

    mutated = dict(basis)
    mutated["close"] = "98"
    with pytest.raises(ValueError, match="candidate hash"):
        replace(
            generated,
            underlying_quote_basis=mutated,
            underlying_quote_basis_hash=canonical_hash(mutated),
        )


def test_proposal_payload_is_detached_and_rejects_tampered_candidate() -> None:
    generated = _generate().candidates[0]
    detached = generated.proposal_payload()
    detached["legs"][0]["ask"] = "999"

    assert generated.proposal_payload()["legs"][0]["ask"] == "2.1"

    object.__setattr__(generated, "max_loss_usd", Decimal("999"))
    with pytest.raises(ValueError, match="candidate hash"):
        generated.proposal_payload()


def test_proposal_payload_fails_closed_for_incomplete_candidate_hash() -> None:
    generated = replace(_generate().candidates[0], candidate_hash="0" * 64)

    with pytest.raises(ValueError, match="candidate hash"):
        generated.proposal_payload()


def test_proposal_payload_fails_closed_without_terminal_scenario_authority() -> None:
    finalist = _finalist()
    finalist["terminal_scenarios"] = ()
    generated = _generate(finalist=finalist).candidates[0]

    with pytest.raises(ValueError, match="terminal scenarios"):
        generated.proposal_payload()


def test_scenario_finalization_is_pure_and_rehashes_before_proposal() -> None:
    finalist = _finalist()
    finalist["terminal_scenarios"] = ()
    generated = _generate(finalist=finalist).candidates[0]
    original_hash = generated.candidate_hash

    finalized = generated.finalize_scenarios(
        (
            {"terminal_price": "100", "probability": "0.5"},
            {"terminal_price": "105", "probability": "0.5"},
        )
    )

    assert generated.candidate.terminal_scenarios == ()
    assert generated.candidate_hash == original_hash
    assert finalized is not generated
    assert finalized.candidate_hash != original_hash
    assert finalized.candidate_hash == canonical_hash(finalized.hash_payload())
    assert finalized.proposal_payload()["expected_value_usd"] == "120"


def test_mapping_finalist_scenarios_are_parsed_without_float_coercion() -> None:
    finalist = _finalist()
    finalist["terminal_scenarios"] = (
        {"terminal_underlying_price": Decimal("100"), "probability": Decimal("0.5")},
        {"terminal_underlying_price": Decimal("105"), "probability": Decimal("0.5")},
    )

    result = _generate(finalist=finalist)

    assert result.status == "CANDIDATES"
    assert result.candidates[0].proposal_payload()["expected_value_usd"] == "120"
