from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib
from io import StringIO
import json
from pathlib import Path

import pytest


SIGNED_AT = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
EFFECTIVE_AT = datetime(2026, 8, 3, 13, 0, tzinfo=timezone.utc)
SOURCE_HASH = "a" * 64


def _contracts():
    return importlib.import_module("options_copilot.governance.contracts")


def _cli():
    return importlib.import_module("options_copilot.governance.cli")


def _provenance() -> dict[str, object]:
    return {
        "source": "P0-03 human decision evidence",
        "source_hash": SOURCE_HASH,
        "observed_at": "2026-08-03T11:30:00.000000+00:00",
        "references": ["options_copilot/BASELINE.md"],
    }


def _payload() -> dict[str, object]:
    return {
        "equation": "strategy_nav = anchor + strategy_pnl - external_cash_flows",
        "rounding": "ROUND_HALF_EVEN:0.01",
        "nlv_fallback_allowed": False,
    }


def _signed(kind: str = "STRATEGY_NAV", *, version: str = "v1"):
    api = _contracts()
    return api.sign_contract(
        kind=kind,
        version=version,
        effective_at=EFFECTIVE_AT,
        provenance=_provenance(),
        payload=_payload(),
        actor="human.operator",
        signed_at=SIGNED_AT,
    )


@pytest.mark.parametrize(
    "kind",
    [
        "STRATEGY_NAV",
        "EXECUTION_COST",
        "INITIAL_CHAMPION_SCENARIO_POLICY",
    ],
)
def test_all_governance_contract_kinds_are_canonically_signed(kind: str) -> None:
    api = _contracts()
    first = _signed(kind)
    second = api.sign_contract(
        kind=kind.lower(),
        version="v1",
        effective_at=EFFECTIVE_AT,
        provenance=dict(reversed(list(_provenance().items()))),
        payload=dict(reversed(list(_payload().items()))),
        actor="human.operator",
        signed_at=SIGNED_AT,
    )

    assert first.contract_hash == second.contract_hash
    assert len(first.contract_hash) == 64
    assert first.actor == "human.operator"
    assert first.signed_at == SIGNED_AT
    assert first.effective_at == EFFECTIVE_AT
    assert first.to_dict()["contract_kind"] == kind
    assert api.SignedContract.from_dict(first.to_dict()) == first


def test_contract_field_mutation_and_hash_substitution_fail_closed() -> None:
    api = _contracts()
    original = _signed().to_dict()

    mutated = deepcopy(original)
    mutated["payload"]["nlv_fallback_allowed"] = True
    with pytest.raises(api.ContractValidationError, match="hash"):
        api.SignedContract.from_dict(mutated)

    wrong_hash = deepcopy(original)
    wrong_hash["contract_hash"] = "0" * 64
    with pytest.raises(api.ContractValidationError, match="hash"):
        api.SignedContract.from_dict(wrong_hash)


@pytest.mark.parametrize("missing", ["actor", "signed_at", "provenance"])
def test_missing_signature_or_provenance_fails_closed(missing: str) -> None:
    api = _contracts()
    document = _signed().to_dict()
    document.pop(missing)

    with pytest.raises(api.ContractValidationError, match=missing):
        api.SignedContract.from_dict(document)


@pytest.mark.parametrize(
    ("expectation", "value", "message"),
    [
        ("expected_kind", "EXECUTION_COST", "kind"),
        ("expected_version", "v9", "version"),
        ("expected_hash", "f" * 64, "hash"),
        ("expected_signer", "another.operator", "signer"),
        (
            "expected_effective_at",
            EFFECTIVE_AT + timedelta(seconds=1),
            "effective",
        ),
    ],
)
def test_consumer_expectations_reject_wrong_contract_identity(
    expectation: str, value: object, message: str
) -> None:
    api = _contracts()
    with pytest.raises(api.ContractValidationError, match=message):
        api.verify_contract(_signed(), **{expectation: value})


def test_consumer_rejects_contract_not_yet_effective() -> None:
    api = _contracts()
    with pytest.raises(api.ContractValidationError, match="not effective"):
        api.verify_contract(
            _signed(),
            as_of=EFFECTIVE_AT - timedelta(microseconds=1),
        )


def test_correction_chains_to_prior_hash_and_preserves_prior_artifact(
    tmp_path: Path,
) -> None:
    api = _contracts()
    prior = _signed()
    prior_path = tmp_path / "strategy-nav.v1.json"
    correction_path = tmp_path / "strategy-nav.v2.json"
    api.write_contract(prior, prior_path)
    prior_bytes = prior_path.read_bytes()

    correction = api.create_correction(
        prior,
        version="v2",
        effective_at=EFFECTIVE_AT + timedelta(days=1),
        provenance={
            **_provenance(),
            "source_hash": "b" * 64,
            "observed_at": "2026-08-04T11:30:00.000000+00:00",
        },
        payload={**_payload(), "rounding": "ROUND_HALF_EVEN:0.0001"},
        actor="human.operator",
        signed_at=SIGNED_AT + timedelta(days=1),
    )
    api.write_contract(correction, correction_path)

    assert correction.supersedes_version == "v1"
    assert correction.supersedes_hash == prior.contract_hash
    assert correction.contract_hash != prior.contract_hash
    assert api.verify_correction(prior, correction) == correction
    assert prior_path.read_bytes() == prior_bytes
    assert api.load_contract(prior_path) == prior
    assert api.load_contract(correction_path) == correction


def test_correction_requires_complete_supersession_identity() -> None:
    api = _contracts()
    with pytest.raises(api.ContractValidationError, match="supersedes_hash"):
        api.sign_contract(
            kind="STRATEGY_NAV",
            version="v2",
            effective_at=EFFECTIVE_AT,
            provenance=_provenance(),
            payload=_payload(),
            actor="human.operator",
            signed_at=SIGNED_AT,
            supersedes_version="v1",
        )


def test_existing_contract_path_cannot_be_overwritten(tmp_path: Path) -> None:
    api = _contracts()
    path = tmp_path / "contract.json"
    first = _signed()
    api.write_contract(first, path)
    original = path.read_bytes()

    with pytest.raises(api.ContractWriteError, match="already exists"):
        api.write_contract(_signed("EXECUTION_COST"), path)

    assert path.read_bytes() == original


def test_signing_rejects_secret_material() -> None:
    api = _contracts()
    with pytest.raises(api.ContractValidationError, match="secret"):
        api.sign_contract(
            kind="EXECUTION_COST",
            version="v1",
            effective_at=EFFECTIVE_AT,
            provenance=_provenance(),
            payload={"api_token": "must-not-be-signed"},
            actor="human.operator",
            signed_at=SIGNED_AT,
        )


def test_verify_cli_has_fixed_missing_and_invalid_exit_codes(tmp_path: Path) -> None:
    cli = _cli()
    missing_out = StringIO()
    missing_err = StringIO()
    missing_code = cli.main(
        ["verify-contract", "--path", str(tmp_path / "missing.json"), "--json"],
        stdout=missing_out,
        stderr=missing_err,
    )
    missing_payload = json.loads(missing_out.getvalue())

    invalid_path = tmp_path / "invalid.json"
    invalid_path.write_text("{not-json", encoding="utf-8")
    invalid_out = StringIO()
    invalid_err = StringIO()
    invalid_code = cli.main(
        ["verify-contract", "--path", str(invalid_path), "--json"],
        stdout=invalid_out,
        stderr=invalid_err,
    )
    invalid_payload = json.loads(invalid_out.getvalue())

    assert missing_code == cli.EXIT_NOT_FOUND == 3
    assert missing_payload["error"]["code"] == "CONTRACT_NOT_FOUND"
    assert missing_err.getvalue() == ""
    assert invalid_code == cli.EXIT_INVALID == 4
    assert invalid_payload["error"]["code"] == "INVALID_CONTRACT"
    assert invalid_err.getvalue() == ""


def test_sign_and_verify_cli_round_trip_without_secret_arguments(
    tmp_path: Path,
) -> None:
    api = _contracts()
    cli = _cli()
    draft_path = tmp_path / "strategy-nav-draft.json"
    output_path = tmp_path / "strategy-nav.v1.json"
    draft_path.write_text(
        json.dumps(
            {
                "version": "v1",
                "effective_at": EFFECTIVE_AT.isoformat(timespec="microseconds"),
                "provenance": _provenance(),
                "payload": _payload(),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    sign_out = StringIO()
    sign_err = StringIO()
    sign_code = cli.main(
        [
            "sign-strategy-nav",
            "--draft",
            str(draft_path),
            "--output",
            str(output_path),
            "--actor",
            "human.operator",
            "--signed-at",
            SIGNED_AT.isoformat(timespec="microseconds"),
            "--json",
        ],
        stdout=sign_out,
        stderr=sign_err,
    )

    verify_out = StringIO()
    verify_err = StringIO()
    contract = api.load_contract(output_path)
    verify_code = cli.main(
        [
            "verify-contract",
            "--path",
            str(output_path),
            "--expect-kind",
            "STRATEGY_NAV",
            "--expect-version",
            "v1",
            "--expect-hash",
            contract.contract_hash,
            "--expect-signer",
            "human.operator",
            "--expect-effective-at",
            EFFECTIVE_AT.isoformat(timespec="microseconds"),
            "--json",
        ],
        stdout=verify_out,
        stderr=verify_err,
    )

    assert sign_code == cli.EXIT_OK == 0
    assert json.loads(sign_out.getvalue())["ok"] is True
    assert sign_err.getvalue() == ""
    assert verify_code == 0
    assert json.loads(verify_out.getvalue())["contract"]["contract_hash"] == (
        contract.contract_hash
    )
    assert verify_err.getvalue() == ""


def test_checked_in_execution_cost_contract_is_complete_and_source_bound() -> None:
    api = _contracts()
    root = Path(__file__).resolve().parents[2]
    contract_path = (
        root / "options_copilot" / "governance" / "execution_cost_contract.v1.json"
    )
    source_path = (
        root
        / "data"
        / "options_copilot"
        / "evidence"
        / "checkpoints"
        / "P0"
        / "execution-cost-contract"
        / "calibration-source.v1.json"
    )
    decision_path = source_path.with_name("decision-evidence.v1.json")

    contract = api.load_contract(
        contract_path,
        expected_kind="EXECUTION_COST",
        expected_version="v1",
        expected_hash="d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b",
        expected_signer="human:xujie",
        expected_effective_at="2026-08-03T16:05:00.316359+00:00",
    )
    source_document = json.loads(source_path.read_text(encoding="utf-8"))
    source_hash = api.canonical_hash(source_document)
    assert source_hash == contract.provenance["source_hash"]

    payload = contract.payload
    assert payload["calibration"]["sample_counts"] == {
        "completed_round_trip_strategy_lifecycles": 1,
        "contract_sides": 12,
        "fill_records": 9,
        "grouped_execution_events": 3,
    }
    fees = payload["commission_and_fees"]
    assert fees["fallback_usd_per_contract_side"] == "1.25"
    assert fees["minimum_usd_per_order"] == "1.00"
    assert fees["round_trip_reserved_at_candidate_creation"] is True
    quote_gates = payload["quote_spread_and_slippage"]["quote_hard_gates"]
    assert quote_gates["maximum_age_seconds"] == "5"
    assert quote_gates["locked_bid_equals_ask"] == "NO_TRADE"
    assert quote_gates["crossed_bid_greater_than_ask"] == "NO_TRADE"
    assignment = payload["assignment_exercise_and_dividend"]["assignment"]
    assert assignment["planned_assignment_allowed"] is False
    assert assignment["fallback_when_risk_cannot_be_bounded"] == "NO_TRADE"
    assert payload["review_policy"][
        "minimum_independent_complete_execution_events"
    ] == 30
    targets = set(payload["downstream_bindings"]["required_targets"])
    assert {
        "GLD management candidates",
        "strategy candidates",
        "ranking ledger rows",
        "replay rows",
        "outcome rows",
        "GUI approvals",
        "creator payloads",
    }.issubset(targets)

    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    assert decision["approval_status"] == "APPROVED"
    assert decision["actor"] == contract.actor
    assert decision["signed_at"] == contract.to_dict()["signed_at"]
    assert decision["contract_hash"] == contract.contract_hash
    evidence_body = dict(decision)
    evidence_hash = evidence_body.pop("evidence_hash")
    assert api.canonical_hash(evidence_body) == evidence_hash


def test_checked_in_initial_policy_is_frozen_complete_and_source_bound() -> None:
    api = _contracts()
    root = Path(__file__).resolve().parents[2]
    contract_path = (
        root
        / "options_copilot"
        / "governance"
        / "initial_champion_scenario_policy.v1.json"
    )
    source_path = (
        root
        / "data"
        / "options_copilot"
        / "evidence"
        / "checkpoints"
        / "P0"
        / "initial-policy"
        / "calibration-source.v1.json"
    )
    decision_path = source_path.with_name("decision-evidence.v1.json")

    contract = api.load_contract(
        contract_path,
        expected_kind="INITIAL_CHAMPION_SCENARIO_POLICY",
        expected_version="v1",
        expected_hash="b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c",
        expected_signer="human:xujie",
        expected_effective_at="2026-08-03T16:11:43.941811+00:00",
    )
    source_document = json.loads(source_path.read_text(encoding="utf-8"))
    assert api.canonical_hash(source_document) == contract.provenance["source_hash"]

    payload = contract.payload
    assert payload["calibration"]["eligible_labeled_scenarios"] == 0
    assert payload["calibration"]["production_profit_claim"] is False
    probability = payload["probability_methodology"]
    assert probability["uniform_shrinkage_fraction"] == "0.20"
    assert probability["softmax_temperature"] == "1.25"
    assert probability["supporting_evidence_probability_effect"] == "NONE"
    assert [row["name"] for row in probability["scenarios"]] == [
        "STRONG_DOWN",
        "DOWN",
        "RANGE",
        "UP",
        "STRONG_UP",
    ]

    weights = payload["baseline_feature_weights"]
    assert weights["market_direction_score"]["weights"] == {
        "momentum_5d": "0.25",
        "price_volume_confirmation": "0.20",
        "sector_relative_strength_20d": "0.20",
        "trend_20d": "0.35",
    }
    assert weights["volatility_state_score"]["weights"] == {
        "iv_percentile": "0.30",
        "realized_implied_gap": "0.20",
        "skew_tail_pressure": "0.25",
        "term_structure": "0.25",
    }
    assert weights["ranking_score_points"]["total_before_supporting_bonus"] == (
        "100"
    )

    support = payload["supporting_evidence_caps"]
    assert support["aggregate_maximum_absolute_ranking_points"] == "3.00"
    assert support["eligibility_effect"] == "NONE"
    assert support["probability_effect"] == "NONE"
    assert support["risk_limit_effect"] == "NONE"
    assert support["families"]["POSITIONING"][
        "maximum_absolute_ranking_points"
    ] == "1.00"

    hard = payload["hard_no_trade_thresholds"]
    assert hard["dte_and_holding"]["minimum_dte"] == 7
    assert hard["dte_and_holding"]["normal_dte_minimum"] == 14
    assert hard["dte_and_holding"]["normal_dte_maximum"] == 35
    assert hard["portfolio_risk"]["normal_fraction"] == "0.10"
    assert hard["portfolio_risk"]["a_grade_fraction"].startswith("0.15")
    assert hard["portfolio_risk"]["hard_reject_fraction"] == (
        "risk_fraction >= 0.20"
    )
    assert hard["cost_and_expectancy"]["execution_cost_contract_hash"] == (
        "d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b"
    )
    assert hard["account_and_authority"]["working_order_count_required"] == 0
    assert hard["account_and_authority"][
        "unsubmitted_instruction_count_required"
    ] == 0

    ranking = payload["ranking_policy"]
    assert ranking["eligibility_precedes_scoring"] is True
    assert ranking["maximum_candidates"] == 3
    assert ranking["rank_one_only_can_enter_approval"] is True
    learning = payload["learning_and_promotion"]
    assert learning["minimum_independent_scenarios_for_discovery"] == 30
    assert learning["threshold_effect"] == "DISCOVERY_ONLY"
    assert learning["production_auto_promotion"] is False
    assert payload["correction_policy"]["automatic_mutation"] is False
    assert payload["consumer_gates"]["P5"].startswith("BLOCKED")

    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    assert decision["approval_status"] == "APPROVED"
    assert decision["actor"] == contract.actor
    assert decision["signed_at"] == contract.to_dict()["signed_at"]
    assert decision["contract_hash"] == contract.contract_hash
    assert decision["immutability"]["P9_can_overwrite_or_auto_promote"] is False
    evidence_body = dict(decision)
    evidence_hash = evidence_body.pop("evidence_hash")
    assert api.canonical_hash(evidence_body) == evidence_hash
