from __future__ import annotations

from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from options_copilot.learning.evaluation import (
    EvaluationSample,
    LearningEvaluationStage,
    build_evaluation_report,
)
from options_copilot.learning.similarity import SimilarityIndex


NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
HASHES = {name: character * 64 for name, character in {
    "dataset": "1",
    "policy": "2",
    "challenger": "3",
    "cost": "4",
    "independence": "5",
}.items()}


def _samples(count: int) -> tuple[EvaluationSample, ...]:
    return tuple(
        EvaluationSample(
            scenario_id=f"scenario-{index}",
            independence_key=f"independent-{index}",
            scenario_bucket="UP" if index % 2 else "RANGE",
            outcome=Decimal("1") if index % 3 else Decimal("0"),
            champion_probability=Decimal("0.45"),
            challenger_probability=Decimal("0.55"),
            challenger_expected_value_usd=Decimal("8.00"),
            actual_gross_pnl_usd=Decimal("10.00") if index % 4 else Decimal("-15.00"),
            execution_cost_usd=Decimal("2.00"),
            covered=index % 5 != 0,
            champion_expected_value_usd=Decimal("4.00"),
            economic_time=NOW + timedelta(minutes=index),
        )
        for index in range(count)
    )


def test_similarity_index_is_read_only_and_hash_bound() -> None:
    index = SimilarityIndex(
        {
            "evidence-a": (Decimal("0"), Decimal("0")),
            "evidence-b": (Decimal("1"), Decimal("1")),
        },
        version="v1",
    )

    result = index.query((Decimal("0.1"), Decimal("0.1")), limit=1)

    assert result.index_version == "v1"
    assert len(result.index_hash) == 64
    assert result.matches[0].evidence_id == "evidence-a"
    assert result.matches[0].distance >= Decimal("0")
    assert result.shadow_only is True
    assert result.can_mutate_policy is False
    assert not hasattr(index, "add")
    with pytest.raises(TypeError):
        index.evidence["evidence-c"] = (Decimal("2"), Decimal("2"))  # type: ignore[index]


def test_thirty_independent_samples_end_at_discovery_without_authority() -> None:
    initial = Path("options_copilot/governance/initial_champion_scenario_policy.v1.json")
    before = initial.read_bytes()

    report = build_evaluation_report(
        samples=_samples(30),
        generated_at=NOW,
        reference_dataset_hash=HASHES["dataset"],
        initial_champion_policy_hash=HASHES["policy"],
        challenger_version="challenger-v1",
        challenger_hash=HASHES["challenger"],
        execution_cost_hash=HASHES["cost"],
        independence_hash=HASHES["independence"],
    )

    assert report.independent_count == 30
    assert report.stage is LearningEvaluationStage.DISCOVERY
    assert report.comparison_complete is True
    assert report.promotion_blocked_reasons == ()
    assert report.challenger_cost_adjusted_ev_delta_usd == Decimal("4.00")
    assert report.challenger_brier_improvement is not None
    assert report.challenger_brier_improvement > 0
    assert report.shadow_only is True
    assert report.can_promote is False
    assert report.can_change_risk is False
    assert report.can_change_ranking is False
    assert report.can_create_instruction is False
    assert report.reference_dataset_hash == HASHES["dataset"]
    assert report.initial_champion_policy_hash == HASHES["policy"]
    assert report.challenger_hash == HASHES["challenger"]
    assert report.execution_cost_hash == HASHES["cost"]
    assert report.independence_hash == HASHES["independence"]
    assert report.cost_adjusted_expected_value_usd.is_finite()
    assert report.brier_score.is_finite()
    assert report.calibration_error.is_finite()
    assert report.coverage.is_finite()
    assert report.maximum_drawdown_usd >= 0
    assert report.tail_loss_usd >= 0
    assert {row.scenario_bucket for row in report.scenario_breakdown} == {"RANGE", "UP"}
    assert len(report.report_hash) == 64
    assert initial.read_bytes() == before


def test_duplicate_independence_keys_are_rejected_instead_of_selecting_by_input_order() -> None:
    samples = tuple(
        EvaluationSample(
            scenario_id=f"scenario-{index}",
            independence_key="same-event",
            scenario_bucket="RANGE",
            outcome=Decimal("1"),
            champion_probability=Decimal("0.5"),
            challenger_probability=Decimal("0.5"),
            challenger_expected_value_usd=Decimal("1"),
            actual_gross_pnl_usd=Decimal("1"),
            execution_cost_usd=Decimal("0.1"),
            covered=True,
            economic_time=NOW + timedelta(minutes=index),
        )
        for index in range(30)
    )
    with pytest.raises(ValueError, match="independence_key values must be unique"):
        build_evaluation_report(
            samples=samples,
            generated_at=NOW,
            reference_dataset_hash=HASHES["dataset"],
            initial_champion_policy_hash=HASHES["policy"],
            challenger_version="challenger-v1",
            challenger_hash=HASHES["challenger"],
            execution_cost_hash=HASHES["cost"],
            independence_hash=HASHES["independence"],
        )


def test_evaluation_metrics_and_hash_are_independent_of_input_order() -> None:
    samples = _samples(12)
    forward = build_evaluation_report(
        samples=samples,
        generated_at=NOW,
        reference_dataset_hash=HASHES["dataset"],
        initial_champion_policy_hash=HASHES["policy"],
        challenger_version="challenger-v1",
        challenger_hash=HASHES["challenger"],
        execution_cost_hash=HASHES["cost"],
        independence_hash=HASHES["independence"],
    )
    reverse = build_evaluation_report(
        samples=tuple(reversed(samples)),
        generated_at=NOW,
        reference_dataset_hash=HASHES["dataset"],
        initial_champion_policy_hash=HASHES["policy"],
        challenger_version="challenger-v1",
        challenger_hash=HASHES["challenger"],
        execution_cost_hash=HASHES["cost"],
        independence_hash=HASHES["independence"],
    )

    assert reverse == forward
    assert reverse.report_hash == forward.report_hash


def test_duplicate_independence_representative_is_deterministic() -> None:
    first = EvaluationSample(
        scenario_id="scenario-a",
        independence_key="same-event",
        scenario_bucket="RANGE",
        outcome=Decimal("1"),
        champion_probability=Decimal("0.4"),
        challenger_probability=Decimal("0.8"),
        challenger_expected_value_usd=Decimal("9"),
        actual_gross_pnl_usd=Decimal("7"),
        execution_cost_usd=Decimal("1"),
        covered=True,
        champion_expected_value_usd=Decimal("3"),
        economic_time=NOW,
    )
    second = EvaluationSample(
        scenario_id="scenario-z",
        independence_key="same-event",
        scenario_bucket="DOWN",
        outcome=Decimal("0"),
        champion_probability=Decimal("0.6"),
        challenger_probability=Decimal("0.2"),
        challenger_expected_value_usd=Decimal("1"),
        actual_gross_pnl_usd=Decimal("-4"),
        execution_cost_usd=Decimal("2"),
        covered=False,
        champion_expected_value_usd=Decimal("5"),
        economic_time=NOW + timedelta(minutes=1),
    )
    common = {
        "generated_at": NOW,
        "reference_dataset_hash": HASHES["dataset"],
        "initial_champion_policy_hash": HASHES["policy"],
        "challenger_version": "challenger-v1",
        "challenger_hash": HASHES["challenger"],
        "execution_cost_hash": HASHES["cost"],
        "independence_hash": HASHES["independence"],
    }

    with pytest.raises(ValueError, match="independence_key values must be unique"):
        build_evaluation_report(samples=(first, second), **common)
    with pytest.raises(ValueError, match="independence_key values must be unique"):
        build_evaluation_report(samples=(second, first), **common)


def test_missing_champion_baselines_block_complete_comparison() -> None:
    sample = EvaluationSample(
        scenario_id="scenario-missing-baseline",
        independence_key="independent-missing-baseline",
        scenario_bucket="RANGE",
        outcome=Decimal("1"),
        champion_probability=None,
        challenger_probability=Decimal("0.6"),
        challenger_expected_value_usd=Decimal("3"),
        actual_gross_pnl_usd=Decimal("2"),
        execution_cost_usd=Decimal("0.5"),
        covered=True,
        economic_time=NOW,
    )

    report = build_evaluation_report(
        samples=(sample,),
        generated_at=NOW,
        reference_dataset_hash=HASHES["dataset"],
        initial_champion_policy_hash=HASHES["policy"],
        challenger_version="challenger-v1",
        challenger_hash=HASHES["challenger"],
        execution_cost_hash=HASHES["cost"],
        independence_hash=HASHES["independence"],
    )

    assert report.comparison_complete is False
    assert report.champion_brier_score is None
    assert report.champion_calibration_error is None
    assert report.champion_cost_adjusted_expected_value_usd is None
    assert report.challenger_cost_adjusted_ev_delta_usd is None
    assert report.challenger_brier_improvement is None
    assert report.promotion_blocked_reasons == (
        "CHAMPION_EXPECTED_VALUE_BASELINE_MISSING",
        "CHAMPION_PROBABILITY_BASELINE_MISSING",
    )


def test_report_authority_flags_cannot_be_replaced_or_constructed() -> None:
    report = build_evaluation_report(
        samples=_samples(1),
        generated_at=NOW,
        reference_dataset_hash=HASHES["dataset"],
        initial_champion_policy_hash=HASHES["policy"],
        challenger_version="challenger-v1",
        challenger_hash=HASHES["challenger"],
        execution_cost_hash=HASHES["cost"],
        independence_hash=HASHES["independence"],
    )

    with pytest.raises(TypeError):
        replace(report, can_promote=True)
    constructor = {
        field.name: getattr(report, field.name)
        for field in fields(report)
        if field.init
    }
    constructor["can_change_risk"] = True
    with pytest.raises(TypeError):
        type(report)(**constructor)


def test_drawdown_uses_immutable_economic_time_not_scenario_or_input_order() -> None:
    values = (
        ("scenario-z", 0, "-2"),
        ("scenario-a", 1, "10"),
        ("scenario-m", 2, "-3"),
    )
    samples = tuple(
        EvaluationSample(
            scenario_id=scenario_id,
            independence_key=f"independent-{scenario_id}",
            scenario_bucket="RANGE",
            outcome=Decimal("1"),
            champion_probability=Decimal("0.5"),
            challenger_probability=Decimal("0.6"),
            challenger_expected_value_usd=Decimal("1"),
            actual_gross_pnl_usd=Decimal(pnl),
            execution_cost_usd=Decimal("0"),
            covered=True,
            champion_expected_value_usd=Decimal("0"),
            economic_time=NOW + timedelta(minutes=offset),
        )
        for scenario_id, offset, pnl in values
    )
    common = {
        "generated_at": NOW,
        "reference_dataset_hash": HASHES["dataset"],
        "initial_champion_policy_hash": HASHES["policy"],
        "challenger_version": "challenger-v1",
        "challenger_hash": HASHES["challenger"],
        "execution_cost_hash": HASHES["cost"],
        "independence_hash": HASHES["independence"],
    }

    report = build_evaluation_report(samples=tuple(reversed(samples)), **common)

    assert report.maximum_drawdown_usd == Decimal("3")
