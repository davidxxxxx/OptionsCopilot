from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest

from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
    InitialPolicyResolver,
    ScenarioAction,
    ScenarioEngine,
)
from options_copilot.analytics.volatility import EvidenceRole, VolatilityEngine


NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)
POLICY_PATH = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "initial_champion_scenario_policy.v1.json"
)
EXECUTION_COST_HASH = (
    "d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b"
)


def _baseline() -> dict[str, object]:
    return {
        "spot": Decimal("100"),
        "atm_iv": Decimal("0.20"),
        "dte": 20,
        "market_score": Decimal("0.15"),
        "volatility_score": Decimal("-0.10"),
        "max_loss": Decimal("100"),
        "max_profit": Decimal("200"),
        "execution_cost_usd": Decimal("20"),
        "stress_after_cost_expected_value": Decimal("5"),
        "after_cost_expected_value": Decimal("10"),
        "cost_hash": EXECUTION_COST_HASH,
        "cost_version": "v1",
        "input_hash": "a" * 64,
        "hard_evidence": {
            "MARKET": {"eligible": True, "hash": "b" * 64},
            "VOLATILITY": {"eligible": True, "hash": "c" * 64},
            "LIQUIDITY": {"eligible": True, "hash": "d" * 64},
        },
    }


def _trusted_risk_authority() -> dict[str, str]:
    return {
        "version": "risk-v1",
        "risk_authority_marker_hash": "e" * 64,
        "risk_contract_hash": "f" * 64,
    }


def test_initial_policy_fallback_is_current_and_tamper_stale_future_fail_closed(
    tmp_path: Path,
) -> None:
    resolver = InitialPolicyResolver()
    resolved = resolver.resolve(now=NOW)

    assert resolved.current_policy_version == INITIAL_POLICY_VERSION
    assert resolved.current_policy_hash == INITIAL_POLICY_HASH
    assert len(resolved.policy_authority_marker_hash) == 64
    assert resolver.is_current(resolved)

    tampered_document = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    tampered_document["contract_hash"] = "0" * 64
    tampered_path = tmp_path / "tampered-policy.json"
    tampered_path.write_text(json.dumps(tampered_document), encoding="utf-8")

    cases = (
        ScenarioEngine(InitialPolicyResolver(tampered_path)).evaluate(
            _baseline(), now=NOW
        ),
        ScenarioEngine(
            InitialPolicyResolver(maximum_age=timedelta(hours=1))
        ).evaluate(_baseline(), now=NOW),
        ScenarioEngine().evaluate(
            _baseline(),
            now=datetime(2026, 8, 3, 16, 0, tzinfo=timezone.utc),
        ),
    )

    for decision in cases:
        assert decision.action is ScenarioAction.NO_TRADE
        assert decision.reasons == ("POLICY_UNAVAILABLE",)
        assert decision.scenarios == ()
        assert decision.current_policy_hash is None
        assert decision.policy_authority_marker_hash is None


def test_immutable_initial_policy_has_no_implicit_expiry() -> None:
    resolved = InitialPolicyResolver().resolve(
        now=datetime(2030, 1, 2, tzinfo=timezone.utc),
    )

    assert resolved.current_policy_hash == INITIAL_POLICY_HASH


def test_scenario_probabilities_are_positive_and_sum_exactly_to_one() -> None:
    decision = ScenarioEngine().evaluate(_baseline(), now=NOW)

    assert decision.action is ScenarioAction.TRADE
    assert [scenario.name for scenario in decision.scenarios] == [
        "STRONG_DOWN",
        "DOWN",
        "RANGE",
        "UP",
        "STRONG_UP",
    ]
    assert all(scenario.probability > 0 for scenario in decision.scenarios)
    assert sum(
        (scenario.probability for scenario in decision.scenarios), Decimal("0")
    ) == Decimal("1.000000")


@pytest.mark.parametrize("method", ("evaluate", "evaluate_pre_cost"))
@pytest.mark.parametrize("field,reason", (
    ("market_score", "MISSING_MARKET_DIRECTION_INPUT"),
    ("volatility_score", "MISSING_VOLATILITY_STATE_INPUT"),
))
@pytest.mark.parametrize("value", (None, True, "0", Decimal("NaN"), Decimal("Infinity")))
def test_missing_or_invalid_policy_features_are_never_imputed(
    method: str, field: str, reason: str, value: object,
) -> None:
    raw = _baseline()
    if value is None:
        raw.pop(field)
    else:
        raw[field] = value
    decision = getattr(ScenarioEngine(), method)(raw, now=NOW)
    assert decision.action is ScenarioAction.NO_TRADE
    assert reason in decision.reasons
    assert decision.scenarios == ()
    assert decision.after_cost_expected_value is None


@pytest.mark.parametrize("method", ("evaluate", "evaluate_pre_cost"))
def test_observed_zero_policy_features_remain_valid(method: str) -> None:
    raw = _baseline()
    raw.update(market_score=Decimal("0"), volatility_score=Decimal("0"))
    decision = getattr(ScenarioEngine(), method)(raw, now=NOW)
    assert decision.action is ScenarioAction.TRADE


def test_pipeline_preserves_missing_policy_features_without_zero_default() -> None:
    from options_copilot.decision.pipeline import _scenario_input

    result = _scenario_input({}, {}, {"symbol": "SPY"})
    assert result["market_score"] is None
    assert result["volatility_score"] is None


@pytest.mark.parametrize("changes,reason", (
    ({"after_cost_expected_value": Decimal("1")}, "SIGNED_MINIMUM_AFTER_COST_EV_NOT_MET"),
    ({"after_cost_expected_value": Decimal("5")}, "SIGNED_MINIMUM_AFTER_COST_EV_NOT_MET"),
    ({"max_loss": Decimal("200"), "after_cost_expected_value": Decimal("10")}, "SIGNED_MINIMUM_AFTER_COST_EV_NOT_MET"),
    ({"max_profit": Decimal("119.99")}, "SIGNED_MINIMUM_REWARD_RISK_NOT_MET"),
    ({"execution_cost_usd": Decimal("40.01")}, "SIGNED_MAXIMUM_COST_RATIO_EXCEEDED"),
    ({"stress_after_cost_expected_value": Decimal("-0.01")}, "SIGNED_STRESS_AFTER_COST_EV_NEGATIVE"),
    ({"stress_after_cost_expected_value": None}, "SIGNED_STRESS_EV_EVIDENCE_UNAVAILABLE"),
    ({"execution_cost_usd": True}, "SIGNED_COST_RATIO_EVIDENCE_UNAVAILABLE"),
))
def test_signed_economic_minima_cannot_be_replaced_by_merely_positive_ev(changes, reason) -> None:
    decision = ScenarioEngine().evaluate({**_baseline(), **changes}, now=NOW)
    assert decision.action is ScenarioAction.NO_TRADE
    assert reason in decision.reasons


def test_signed_economic_inclusive_limits_and_strict_ev_threshold() -> None:
    raw = {**_baseline(), "max_profit": Decimal("120"),
           "execution_cost_usd": Decimal("24"),
           "after_cost_expected_value": Decimal("5.01"),
           "stress_after_cost_expected_value": Decimal("0")}
    assert ScenarioEngine().evaluate(raw, now=NOW).action is ScenarioAction.TRADE


@pytest.mark.parametrize("evidence_class", ["MARKET", "VOLATILITY", "LIQUIDITY"])
@pytest.mark.parametrize("failure", ["missing", "ineligible", "unbound"])
def test_each_hard_evidence_class_fails_closed_independently(
    evidence_class: str,
    failure: str,
) -> None:
    candidate = deepcopy(_baseline())
    evidence = candidate["hard_evidence"]
    assert isinstance(evidence, dict)

    if failure == "missing":
        evidence.pop(evidence_class)
    elif failure == "ineligible":
        evidence[evidence_class]["eligible"] = False
    else:
        evidence[evidence_class]["hash"] = "not-a-bound-hash"

    decision = ScenarioEngine().evaluate(candidate, now=NOW)

    assert decision.action is ScenarioAction.NO_TRADE
    if failure in {"missing", "ineligible"}:
        assert f"MISSING_OR_INELIGIBLE_{evidence_class}_EVIDENCE" in decision.reasons
    if failure in {"missing", "unbound"}:
        assert f"UNBOUND_{evidence_class}_EVIDENCE" in decision.reasons


def test_supporting_only_inputs_cannot_replace_hard_evidence_or_flip_eligibility() -> None:
    eligible_input = _baseline()
    eligible = ScenarioEngine().evaluate(eligible_input, now=NOW)

    with_supporting = deepcopy(eligible_input)
    hard_evidence = with_supporting["hard_evidence"]
    assert isinstance(hard_evidence, dict)
    hard_evidence.update(
        {
            "NEWS": {"eligible": True, "hash": "1" * 64},
            "EARNINGS": {"eligible": True, "hash": "2" * 64},
            "POSITIONING": {"eligible": True, "hash": "3" * 64},
        }
    )
    with_supporting["supporting_evidence"] = {
        "NEWS": {"score": Decimal("100")},
        "POSITIONING": {"score": Decimal("100")},
    }
    still_eligible = ScenarioEngine().evaluate(with_supporting, now=NOW)

    assert eligible.action is ScenarioAction.TRADE
    assert still_eligible.action is ScenarioAction.TRADE
    assert still_eligible.scenarios == eligible.scenarios
    assert still_eligible.result_hash == eligible.result_hash
    for source in ("NEWS", "EARNINGS", "POSITIONING", "MAX_PAIN", "GEX"):
        assert VolatilityEngine.role_for(source) is EvidenceRole.SUPPORTING_ONLY

    hard_evidence.pop("LIQUIDITY")
    rejected = ScenarioEngine().evaluate(with_supporting, now=NOW)

    assert rejected.action is ScenarioAction.NO_TRADE
    assert "MISSING_OR_INELIGIBLE_LIQUIDITY_EVIDENCE" in rejected.reasons
    assert "UNBOUND_LIQUIDITY_EVIDENCE" in rejected.reasons


def test_cost_policy_and_risk_authority_identities_are_bound_to_decision() -> None:
    resolver = InitialPolicyResolver()
    policy = resolver.resolve(now=NOW)
    risk = _trusted_risk_authority()
    candidate = _baseline()
    candidate.update(
        {
            "current_policy_version": policy.current_policy_version,
            "current_policy_hash": policy.current_policy_hash,
            "policy_authority_marker_hash": policy.policy_authority_marker_hash,
            "risk_authority_version": risk["version"],
            "risk_authority_marker_hash": risk["risk_authority_marker_hash"],
            "risk_contract_hash": risk["risk_contract_hash"],
        }
    )

    baseline = ScenarioEngine(resolver).evaluate(
        candidate,
        now=NOW,
        resolved_policy=policy,
        risk_authority=risk,
    )

    assert baseline.action is ScenarioAction.TRADE
    assert baseline.current_policy_version == policy.current_policy_version
    assert baseline.current_policy_hash == policy.current_policy_hash
    assert baseline.policy_authority_marker_hash == policy.policy_authority_marker_hash
    assert baseline.risk_authority_version == risk["version"]
    assert baseline.risk_authority_marker_hash == risk["risk_authority_marker_hash"]
    assert baseline.risk_contract_hash == risk["risk_contract_hash"]
    assert baseline.cost_version == "v1"
    assert baseline.cost_hash == EXECUTION_COST_HASH

    mismatched_policy = {
        **candidate,
        "policy_authority_marker_hash": "4" * 64,
    }
    policy_rejected = ScenarioEngine(resolver).evaluate(
        mismatched_policy,
        now=NOW,
        resolved_policy=policy,
        risk_authority=risk,
    )
    assert policy_rejected.action is ScenarioAction.NO_TRADE
    assert "POLICY_RESOLUTION_DISAGREEMENT" in policy_rejected.reasons
    assert policy_rejected.result_hash != baseline.result_hash

    mismatched_risk = {
        **candidate,
        "risk_authority_marker_hash": "5" * 64,
    }
    risk_rejected = ScenarioEngine(resolver).evaluate(
        mismatched_risk,
        now=NOW,
        resolved_policy=policy,
        risk_authority=risk,
    )
    assert risk_rejected.action is ScenarioAction.NO_TRADE
    assert "RISK_AUTHORITY_RESOLUTION_DISAGREEMENT" in risk_rejected.reasons
    assert risk_rejected.result_hash != baseline.result_hash

    mismatched_cost = {**candidate, "cost_hash": "6" * 64}
    cost_rejected = ScenarioEngine(resolver).evaluate(
        mismatched_cost,
        now=NOW,
        resolved_policy=policy,
        risk_authority=risk,
    )
    assert cost_rejected.action is ScenarioAction.NO_TRADE
    assert "COST_CONTRACT_MISMATCH" in cost_rejected.reasons
    assert cost_rejected.cost_hash == "6" * 64
    assert cost_rejected.result_hash != baseline.result_hash

    alternate_policy = replace(
        policy,
        policy_authority_marker_hash="7" * 64,
    )
    alternate_candidate = {
        **candidate,
        "policy_authority_marker_hash": alternate_policy.policy_authority_marker_hash,
    }
    alternate = ScenarioEngine(resolver).evaluate(
        alternate_candidate,
        now=NOW,
        resolved_policy=alternate_policy,
        risk_authority=risk,
    )
    assert alternate.action is ScenarioAction.TRADE
    assert alternate.result_hash != baseline.result_hash
