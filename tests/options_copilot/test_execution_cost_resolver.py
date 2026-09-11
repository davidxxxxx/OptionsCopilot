from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from decimal import Decimal
import threading

import pytest

from options_copilot.analytics import InitialPolicyResolver, ScenarioEngine
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    ExecutionCostCurrentnessError,
    ExecutionCostResolution,
    ExecutionCostResolutionError,
    SignedExecutionCostResolver,
)
from options_copilot.governance.contracts import create_correction, load_contract
from options_copilot.risk.authorization import RiskTierAuthority


NOW = datetime(2026, 8, 5, 14, 30, tzinfo=timezone.utc)
CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "execution_cost_contract.v1.json"
)


def _resolver(
    path: Path = CONTRACT_PATH,
    *,
    clock_time: datetime = NOW,
) -> SignedExecutionCostResolver:
    return SignedExecutionCostResolver(path, clock=lambda: clock_time)


def _leg(
    *,
    con_id: int,
    strike: str,
    side: str,
    bid: object,
    ask: object,
    batch_id: str = "quotes-1",
    observed_at: datetime = NOW - timedelta(seconds=1),
) -> dict[str, object]:
    return {
        "con_id": con_id,
        "contract_id_ex": f"{con_id}@SMART",
        "underlying": "SPY",
        "security_type": "OPT",
        "expiration": "2026-08-21",
        "strike": strike,
        "right": "C",
        "side": side,
        "ratio": 1,
        "multiplier": "100",
        "currency": "USD",
        "exchange": "SMART",
        "bid": bid,
        "ask": ask,
        "observed_at": observed_at.isoformat(),
        "quote_snapshot_id": batch_id,
    }


def _candidate(candidate_id: str = "spread-1") -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "symbol": "SPY",
        "structure": "DEBIT_VERTICAL",
        "legs": (
            _leg(
                con_id=101,
                strike="100",
                side="LONG",
                bid="2.00",
                ask="2.10",
            ),
            _leg(
                con_id=102,
                strike="110",
                side="SHORT",
                bid="1.00",
                ask="1.10",
            ),
        ),
        "terminal_scenarios": (),
        "estimated_commissions_usd": "5.00",
        "estimated_slippage_usd": "15.00",
        "debit_usd": "210.00",
        "credit_usd": "100.00",
        "all_in_cost_usd": "130.00",
        "max_loss_usd": "130.00",
        "max_profit_usd": "870.00",
        "quote_batch_id": "quotes-1",
        "execution_cost_contract_version": "v1",
        "execution_cost_contract_hash": EXECUTION_COST_HASH,
    }


def _scenario(
    candidate_id: str = "spread-1",
    *,
    down_probability: str = "0.50",
    up_probability: str = "0.50",
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "action": "TRADE",
        "cost_version": "v1",
        "cost_hash": EXECUTION_COST_HASH,
        "scenarios": (
            {"terminal_price": "100", "probability": down_probability},
            {"terminal_price": "120", "probability": up_probability},
        ),
    }


def _credit_candidate() -> dict[str, object]:
    candidate = _candidate("credit-spread")
    candidate["structure"] = "CREDIT_VERTICAL"
    candidate["legs"] = (
        _leg(
            con_id=201,
            strike="100",
            side="SHORT",
            bid="2.00",
            ask="2.10",
        ),
        _leg(
            con_id=202,
            strike="110",
            side="LONG",
            bid="1.00",
            ask="1.10",
        ),
    )
    candidate["debit_usd"] = "110.00"
    candidate["credit_usd"] = "200.00"
    candidate["all_in_cost_usd"] = "-70.00"
    candidate["max_loss_usd"] = "930.00"
    candidate["max_profit_usd"] = "70.00"
    return candidate


def _resolve(
    candidate: object | None = None,
    scenario: object | None = None,
) -> ExecutionCostResolution:
    return _resolver().resolve(
        now=NOW,
        scan_run_id="scan-1",
        candidates=(_candidate() if candidate is None else candidate,),
        scenarios=(_scenario() if scenario is None else scenario,),
    )


def _copy_contract(tmp_path: Path) -> Path:
    path = tmp_path / "execution-cost.json"
    path.write_text(CONTRACT_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    return path


def _write_correction(path: Path) -> None:
    prior = load_contract(path)
    correction = create_correction(
        prior,
        version="v2",
        effective_at=NOW - timedelta(minutes=2),
        provenance=prior.provenance,
        payload=prior.payload,
        actor=prior.actor,
        signed_at=NOW - timedelta(minutes=1),
    )
    path.write_text(
        json.dumps(correction.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


class _TestOnlyAuthorityReadLease:
    """Controllable authority-side lease; never a production file lock."""

    test_only = True

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.write_attempted = threading.Event()
        self.write_completed = threading.Event()

    def guard_read(self, callback):
        with self._lock:
            return callback()

    def replace_contract(self, path: Path) -> None:
        self.write_attempted.set()
        with self._lock:
            _write_correction(path)
        self.write_completed.set()


def test_two_leg_costs_and_scenario_ev_are_recomputed_from_frozen_legs() -> None:
    resolution = _resolve()

    assert resolution.cost_version == "v1"
    assert resolution.cost_hash == EXECUTION_COST_HASH
    assert resolution.scan_run_id == "scan-1"
    assert len(resolution.candidates) == 1
    result = resolution.candidates[0]
    assert result.candidate_id == "spread-1"
    assert result.commission_usd == 5
    assert result.slippage_usd == 15
    assert result.execution_cost_usd == 20
    assert result.expected_value_before_costs_usd == 390
    assert result.after_cost_expected_value == 370
    assert result.stress_execution_cost_usd == 25
    assert result.stress_after_cost_expected_value == 365
    assert result.scenario_count == 2
    assert len(result.calculation_hash) == 64


def test_each_candidate_receives_its_own_scenario_ev_and_hash() -> None:
    second = _candidate("spread-2")
    second["quote_batch_id"] = "quotes-2"
    for leg in second["legs"]:
        leg["quote_snapshot_id"] = "quotes-2"

    resolution = _resolver().resolve(
        now=NOW,
        scan_run_id="scan-many",
        candidates=(_candidate(), second),
        scenarios=(
            _scenario(),
            _scenario(
                "spread-2",
                down_probability="0.75",
                up_probability="0.25",
            ),
        ),
    )

    by_id = {item.candidate_id: item for item in resolution.candidates}
    assert by_id["spread-1"].after_cost_expected_value == 370
    assert by_id["spread-2"].after_cost_expected_value == 120
    assert by_id["spread-1"].calculation_hash != by_id["spread-2"].calculation_hash


def test_candidate_supplied_expected_value_is_not_an_authority() -> None:
    candidate = _candidate()
    candidate["after_cost_expected_value"] = "999999"
    candidate["expected_value_usd"] = "999999"

    result = _resolve(candidate).candidates[0]

    assert result.after_cost_expected_value == 370


def test_defined_risk_credit_spread_keeps_legitimate_negative_all_in_cost() -> None:
    result = _resolver().resolve(
        now=NOW,
        scan_run_id="scan-credit",
        candidates=(_credit_candidate(),),
        scenarios=(_scenario("credit-spread"),),
    ).candidates[0]

    assert result.execution_cost_usd == 20
    assert result.expected_value_before_costs_usd == -410
    assert result.after_cost_expected_value == -430


def test_real_scenario_engine_output_shape_is_accepted_without_candidate_id() -> None:
    policy = InitialPolicyResolver().resolve(now=NOW)
    scenario = ScenarioEngine().evaluate_pre_cost(
        {
            "spot": Decimal("105"),
            "atm_iv": Decimal("0.20"),
            "dte": 16,
            "max_loss": Decimal("130"),
            "market_score": Decimal("0.5"),
            "volatility_score": Decimal("0"),
            "cost_version": "v1",
            "cost_hash": EXECUTION_COST_HASH,
            "hard_evidence": {
                "MARKET": {"eligible": True, "hash": "1" * 64},
                "VOLATILITY": {"eligible": True, "hash": "2" * 64},
                "LIQUIDITY": {"eligible": True, "hash": "3" * 64},
            },
            "input_hash": "4" * 64,
        },
        now=NOW,
        resolved_policy=policy,
        risk_authority=RiskTierAuthority.normal("5" * 64),
    )

    resolution = _resolve(scenario=scenario)

    assert resolution.candidates[0].scenario_count == 5
    assert resolution.candidates[0].after_cost_expected_value.is_finite()


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    (
        ("estimated_commissions_usd", "4.99", "ESTIMATED_COMMISSIONS_USD_MISMATCH"),
        ("estimated_slippage_usd", "-1", "ESTIMATED_SLIPPAGE_USD_NEGATIVE"),
        ("all_in_cost_usd", "129.99", "ALL_IN_COST_USD_MISMATCH"),
        ("max_loss_usd", "129.99", "CANDIDATE_MAX_LOSS_MISMATCH"),
    ),
)
def test_frozen_cost_and_payoff_claims_must_match_exactly(
    field: str, value: str, reason: str
) -> None:
    candidate = _candidate()
    candidate[field] = value

    with pytest.raises(ExecutionCostResolutionError, match=reason):
        _resolve(candidate)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("bid", None),
        ("bid", "NaN"),
        ("ask", "Infinity"),
        ("bid", 2.0),
    ),
)
def test_missing_nonfinite_and_binary_float_quotes_fail_closed(
    field: str, value: object
) -> None:
    candidate = _candidate()
    candidate["legs"][0][field] = value

    with pytest.raises(ExecutionCostResolutionError):
        _resolve(candidate)


@pytest.mark.parametrize(
    ("bid", "ask"),
    (("2.10", "2.10"), ("2.11", "2.10")),
)
def test_locked_and_crossed_quotes_fail_closed(bid: str, ask: str) -> None:
    candidate = _candidate()
    candidate["legs"][0]["bid"] = bid
    candidate["legs"][0]["ask"] = ask

    with pytest.raises(ExecutionCostResolutionError, match="QUOTE_LOCKED_OR_CROSSED"):
        _resolve(candidate)


def test_stale_future_and_mixed_batch_quotes_fail_closed() -> None:
    stale = _candidate()
    stale["legs"][0]["observed_at"] = (NOW - timedelta(seconds=6)).isoformat()
    with pytest.raises(ExecutionCostResolutionError, match="QUOTE_STALE_OR_FUTURE"):
        _resolve(stale)

    future = _candidate()
    future["legs"][0]["observed_at"] = (NOW + timedelta(microseconds=1)).isoformat()
    with pytest.raises(ExecutionCostResolutionError, match="QUOTE_STALE_OR_FUTURE"):
        _resolve(future)

    mixed = _candidate()
    mixed["legs"][1]["quote_snapshot_id"] = "another-batch"
    with pytest.raises(ExecutionCostResolutionError, match="QUOTE_BATCH_MISMATCH"):
        _resolve(mixed)


def test_scenario_count_probability_identity_and_cost_bindings_are_strict() -> None:
    with pytest.raises(
        ExecutionCostResolutionError, match="SCENARIO_CANDIDATE_COUNT_MISMATCH"
    ):
        _resolver().resolve(
            now=NOW,
            scan_run_id="scan-1",
            candidates=(_candidate(),),
            scenarios=(),
        )

    wrong_id = _scenario("another-candidate")
    with pytest.raises(
        ExecutionCostResolutionError, match="SCENARIO_CANDIDATE_ID_MISMATCH"
    ):
        _resolve(scenario=wrong_id)

    bad_probability = _scenario(
        down_probability="0.60", up_probability="0.50"
    )
    with pytest.raises(
        ExecutionCostResolutionError, match="SCENARIO_PROBABILITY_SUM_INVALID"
    ):
        _resolve(scenario=bad_probability)

    bad_cost = _scenario()
    bad_cost["cost_hash"] = "f" * 64
    with pytest.raises(
        ExecutionCostResolutionError, match="SCENARIO_COST_HASH_MISMATCH"
    ):
        _resolve(scenario=bad_cost)


def test_duplicate_candidate_ids_and_duplicate_scenario_prices_are_rejected() -> None:
    with pytest.raises(ExecutionCostResolutionError, match="DUPLICATE_CANDIDATE_ID"):
        _resolver().resolve(
            now=NOW,
            scan_run_id="scan-duplicates",
            candidates=(_candidate(), deepcopy(_candidate())),
            scenarios=(_scenario(), deepcopy(_scenario())),
        )

    scenario = _scenario()
    scenario["scenarios"][1]["terminal_price"] = "100"
    with pytest.raises(ExecutionCostResolutionError, match="DUPLICATE_SCENARIO_PRICE"):
        _resolve(scenario=scenario)


def test_identity_only_resolution_supports_approval_currentness() -> None:
    resolver = _resolver()

    resolution = resolver.resolve(now=NOW)

    assert resolution.candidates == ()
    assert resolution.scan_run_id is None
    assert resolver.is_current(resolution) is True
    assert resolver.assert_current(resolution) is resolution


def test_policy_binding_must_name_the_same_signed_cost_contract() -> None:
    policy = {
        "current_policy_hash": "a" * 64,
        "payload": {
            "hard_no_trade_thresholds": {
                "cost_and_expectancy": {
                    "execution_cost_contract_hash": EXECUTION_COST_HASH
                }
            }
        },
    }
    resolver = _resolver()
    assert resolver.resolve(
        now=NOW, current_policy=policy, resolved_policy=deepcopy(policy)
    ).cost_hash == EXECUTION_COST_HASH

    policy["payload"]["hard_no_trade_thresholds"]["cost_and_expectancy"][
        "execution_cost_contract_hash"
    ] = "f" * 64
    with pytest.raises(
        ExecutionCostResolutionError,
        match="POLICY_EXECUTION_COST_BINDING_MISMATCH",
    ):
        resolver.resolve(now=NOW, current_policy=policy)


def test_missing_tampered_future_and_replaced_contracts_fail_closed(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ExecutionCostResolutionError, match="SIGNED_EXECUTION_COST_CONTRACT_INVALID"
    ):
        _resolver(tmp_path / "missing.json").resolve(now=NOW)

    tampered_path = _copy_contract(tmp_path)
    document = json.loads(tampered_path.read_text(encoding="utf-8"))
    document["payload"]["commission_and_fees"][
        "fallback_usd_per_contract_side"
    ] = "0.01"
    tampered_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(
        ExecutionCostResolutionError, match="SIGNED_EXECUTION_COST_CONTRACT_INVALID"
    ):
        _resolver(tampered_path).resolve(now=NOW)

    before_effective = datetime(2026, 8, 3, 16, tzinfo=timezone.utc)
    with pytest.raises(
        ExecutionCostResolutionError, match="SIGNED_EXECUTION_COST_CONTRACT_INVALID"
    ):
        _resolver(clock_time=before_effective).resolve(now=before_effective)

    replacement_path = _copy_contract(tmp_path)
    _write_correction(replacement_path)
    with pytest.raises(
        ExecutionCostResolutionError, match="SIGNED_EXECUTION_COST_CONTRACT_INVALID"
    ):
        _resolver(replacement_path).resolve(now=NOW)


def test_currentness_detects_a_valid_contract_head_change(tmp_path: Path) -> None:
    path = _copy_contract(tmp_path)
    resolver = _resolver(path)
    resolution = resolver.resolve(now=NOW)
    _write_correction(path)

    assert resolver.is_current(resolution) is False
    with pytest.raises(ExecutionCostCurrentnessError, match="head changed"):
        resolver.assert_current(resolution)


def test_context_and_environment_cannot_override_the_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bogus = tmp_path / "candidate-controlled.json"
    bogus.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("OPTIONS_COPILOT_EXECUTION_COST_CONTRACT", str(bogus))
    monkeypatch.setenv("IBKR_EXECUTION_COST_CONTRACT", str(bogus))

    resolution = _resolver().resolve(
        now=NOW,
        context={"execution_cost_contract_path": str(bogus)},
    )

    assert resolution.cost_hash == EXECUTION_COST_HASH


def test_resolution_rejects_forged_or_wrongly_typed_currentness_values() -> None:
    resolver = _resolver()
    assert resolver.is_current({"cost_hash": EXECUTION_COST_HASH}) is False
    with pytest.raises(
        ExecutionCostCurrentnessError,
        match="resolution must be an ExecutionCostResolution",
    ):
        resolver.assert_current({"cost_hash": EXECUTION_COST_HASH})


def test_production_guard_is_unavailable_without_authority_side_lease(
    tmp_path: Path,
) -> None:
    path = _copy_contract(tmp_path)
    resolver = _resolver(path)
    resolution = resolver.resolve(now=NOW)
    callback_called = False

    def replace_during_callback() -> str:
        nonlocal callback_called
        callback_called = True
        _write_correction(path)
        return "committed"

    assert resolver.guard_current(
        resolution,
        callback=replace_during_callback,
    ) is None
    assert callback_called is False
    assert resolver.is_current(resolution) is True


def test_test_only_authority_lease_requires_explicit_gate() -> None:
    lease = _TestOnlyAuthorityReadLease()

    with pytest.raises(ValueError, match="TEST_ONLY"):
        SignedExecutionCostResolver(authority_read_lease=lease)
    with pytest.raises(ValueError, match="TEST_ONLY"):
        SignedExecutionCostResolver(
            authority_read_lease=object(),
            allow_test_authority_lease=True,
        )


def test_test_only_guard_holds_lease_across_callback_against_external_replace(
    tmp_path: Path,
) -> None:
    path = _copy_contract(tmp_path)
    lease = _TestOnlyAuthorityReadLease()
    resolver = SignedExecutionCostResolver(
        path,
        clock=lambda: NOW,
        authority_read_lease=lease,
        allow_test_authority_lease=True,
    )
    resolution = resolver.resolve(now=NOW)
    callback_entered = threading.Event()
    release_callback = threading.Event()
    results: list[object | None] = []
    failures: list[BaseException] = []

    def callback() -> str:
        callback_entered.set()
        if not release_callback.wait(timeout=5):
            raise TimeoutError("test callback barrier timed out")
        return "committed"

    def run_guard() -> None:
        try:
            results.append(resolver.guard_current(resolution, callback=callback))
        except BaseException as exc:  # pragma: no cover - surfaced below
            failures.append(exc)

    guard_thread = threading.Thread(target=run_guard)
    writer_thread = threading.Thread(target=lease.replace_contract, args=(path,))
    guard_thread.start()
    try:
        assert callback_entered.wait(timeout=5)
        writer_thread.start()
        assert lease.write_attempted.wait(timeout=5)
        assert lease.write_completed.wait(timeout=0.1) is False
    finally:
        release_callback.set()
    guard_thread.join(timeout=5)
    writer_thread.join(timeout=5)

    assert guard_thread.is_alive() is False
    assert writer_thread.is_alive() is False
    assert failures == []
    assert results == ["committed"]
    assert lease.write_completed.is_set()
    assert resolver.is_current(resolution) is False


def test_guard_propagates_callback_exception_and_releases_test_lease(
    tmp_path: Path,
) -> None:
    path = _copy_contract(tmp_path)
    lease = _TestOnlyAuthorityReadLease()
    resolver = SignedExecutionCostResolver(
        path,
        clock=lambda: NOW,
        authority_read_lease=lease,
        allow_test_authority_lease=True,
    )
    resolution = resolver.resolve(now=NOW)
    failure = RuntimeError("approval transaction failed")

    def fail_callback() -> None:
        raise failure

    with pytest.raises(RuntimeError, match="approval transaction failed") as caught:
        resolver.guard_current(resolution, callback=fail_callback)

    assert caught.value is failure
    lease.replace_contract(path)
    assert lease.write_completed.is_set()


def test_guard_fails_closed_before_callback_but_propagates_lease_exit_failure(
    tmp_path: Path,
) -> None:
    path = _copy_contract(tmp_path)

    class FailingLease:
        test_only = True

        def __init__(self, *, fail_after_callback: bool) -> None:
            self.fail_after_callback = fail_after_callback

        def guard_read(self, callback):
            if not self.fail_after_callback:
                raise RuntimeError("lease acquisition failed")
            callback()
            raise RuntimeError("lease integrity failed after callback")

    callback_calls = 0

    def callback() -> str:
        nonlocal callback_calls
        callback_calls += 1
        return "committed"

    before = SignedExecutionCostResolver(
        path,
        clock=lambda: NOW,
        authority_read_lease=FailingLease(fail_after_callback=False),
        allow_test_authority_lease=True,
    )
    before_resolution = before.resolve(now=NOW)
    assert before.guard_current(before_resolution, callback=callback) is None
    assert callback_calls == 0

    after = SignedExecutionCostResolver(
        path,
        clock=lambda: NOW,
        authority_read_lease=FailingLease(fail_after_callback=True),
        allow_test_authority_lease=True,
    )
    after_resolution = after.resolve(now=NOW)
    with pytest.raises(
        RuntimeError,
        match="lease integrity failed after callback",
    ):
        after.guard_current(after_resolution, callback=callback)
    assert callback_calls == 1
