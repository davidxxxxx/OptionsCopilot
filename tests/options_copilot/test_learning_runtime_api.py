from __future__ import annotations

import asyncio
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import options_copilot.learning_shadow as learning_shadow_module
from options_copilot.api import create_app
from options_copilot.config import OptionsCopilotConfig
from options_copilot.learning.outcome_processor import OutcomeProcessingResult
from options_copilot.runtime import (
    BASELINE_MODEL_VERSION,
    OptionsCopilotRuntime,
    RuntimeServices,
    _learning_evaluation_stage,
)
from options_copilot.learning_shadow import (
    LedgerTampered,
    OutcomeRecord,
    ShadowLearningLedger,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json, datetime_text


@pytest.mark.parametrize(
    ("count", "expected"),
    (
        (0, "COLLECTING"),
        (1, "COMPARISON_AVAILABLE"),
        (29, "COMPARISON_AVAILABLE"),
        (30, "DISCOVERY"),
    ),
)
def test_learning_evaluation_stage_requires_thirty_independent_samples(
    count: int,
    expected: str,
) -> None:
    status = "COLLECTING" if count == 0 else "AVAILABLE"
    assert _learning_evaluation_stage(status, count) == expected


def test_shadow_schema_version_serializes_with_concurrent_ledger_work(
    tmp_path: Path,
) -> None:
    ledger = ShadowLearningLedger(tmp_path / "shadow-learning.sqlite3")
    lock_acquired = threading.Event()
    release_lock = threading.Event()
    read_finished = threading.Event()
    result: list[int] = []

    def hold_ledger_lock() -> None:
        with ledger._lock:  # noqa: SLF001 - deterministic concurrency regression
            lock_acquired.set()
            assert release_lock.wait(timeout=5)

    def read_schema_version() -> None:
        result.append(ledger.schema_version)
        read_finished.set()

    holder = threading.Thread(target=hold_ledger_lock)
    reader = threading.Thread(target=read_schema_version)
    try:
        holder.start()
        assert lock_acquired.wait(timeout=5)
        reader.start()
        assert not read_finished.wait(timeout=0.1)
        release_lock.set()
        holder.join(timeout=5)
        reader.join(timeout=5)
        assert not holder.is_alive()
        assert not reader.is_alive()
        assert result == [1]
    finally:
        release_lock.set()
        holder.join(timeout=5)
        reader.join(timeout=5)
        ledger.close()


def _runtime(tmp_path: Path) -> OptionsCopilotRuntime:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    unavailable = RuntimeServices(
        **{field.name: None for field in fields(RuntimeServices)}
    )
    return OptionsCopilotRuntime(config, runtime_services=unavailable)


def _bounded_outcome_result(now: datetime) -> OutcomeProcessingResult:
    return OutcomeProcessingResult(
        status="WAITING_FOR_OBSERVATIONS",
        checked_at=now,
        subjects_seen=100,
        horizons_requested=100,
        due_count=100,
        records_appended=0,
        records_superseded=0,
        records_skipped=0,
        records_blocked=100,
        records_rejected=0,
        reason_codes=(
            "OUTCOME_OBSERVATION_NOT_AVAILABLE",
            "OUTCOME_PROCESSING_BOUNDED",
        ),
        candidate_ledger_head_hash="1" * 64,
        shadow_ledger_head_hash="2" * 64,
        manifest_hash="3" * 64,
        processing_hash="4" * 64,
        prediction_cursor=2621,
        remaining_count=200,
        progress_sequence=2,
        progress_hash="5" * 64,
        bounded=True,
    )


def test_outcome_result_survives_supporting_shadow_evaluation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    now = datetime.now(timezone.utc)
    result = replace(_bounded_outcome_result(now), records_appended=1)
    runtime.outcome_processor = SimpleNamespace(process=lambda **_kwargs: result)

    def fail_refresh(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("simulated supporting-only evaluation failure")

    monkeypatch.setattr(runtime.shadow_evaluation_store, "refresh", fail_refresh)
    try:
        payload = runtime._process_outcomes(
            cancel_event=threading.Event(),
            deadline_at=now + timedelta(seconds=60),
            operation_token="scan.outcome-regression",
        )

        assert isinstance(payload, dict)
        assert payload["status"] == "WAITING_FOR_OBSERVATIONS"
        assert payload["processing_hash"] == result.processing_hash
        assert payload["records_blocked"] == 100
        assert payload["reason_codes"] == result.reason_codes
        assert payload["shadow_evaluation_refresh"] == {
            "status": "DEGRADED",
            "reason": "SHADOW_EVALUATION_REFRESH_FAILED",
            "decision_authority": "SUPPORTING_ONLY",
            "persisted": False,
        }
    finally:
        runtime.close()


def test_unchanged_outcome_result_skips_shadow_evaluation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    now = datetime.now(timezone.utc)
    result = _bounded_outcome_result(now)
    runtime.outcome_processor = SimpleNamespace(process=lambda **_kwargs: result)

    monkeypatch.setattr(
        runtime.shadow_evaluation_store,
        "refresh",
        lambda *_args, **_kwargs: pytest.fail(
            "unchanged outcomes must not rescan shadow evaluation"
        ),
    )
    try:
        payload = runtime._process_outcomes(
            cancel_event=threading.Event(),
            deadline_at=now + timedelta(seconds=60),
            operation_token="scan.outcome-unchanged",
        )

        assert isinstance(payload, dict)
        assert payload["status"] == "WAITING_FOR_OBSERVATIONS"
        assert payload["processing_hash"] == result.processing_hash
        assert payload["shadow_evaluation_refresh"] == {
            "status": "SKIPPED",
            "reason": "SHADOW_EVALUATION_UNCHANGED",
            "decision_authority": "SUPPORTING_ONLY",
            "persisted": False,
        }
    finally:
        runtime.close()


def test_changed_outcome_defers_shadow_evaluation_before_scheduler_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    now = datetime.now(timezone.utc)
    result = replace(_bounded_outcome_result(now), records_appended=1)
    runtime.outcome_processor = SimpleNamespace(process=lambda **_kwargs: result)

    monkeypatch.setattr(
        runtime.shadow_evaluation_store,
        "refresh",
        lambda *_args, **_kwargs: pytest.fail(
            "evaluation must yield the remaining scheduler deadline reserve"
        ),
    )
    try:
        payload = runtime._process_outcomes(
            cancel_event=threading.Event(),
            deadline_at=now + timedelta(seconds=4),
            operation_token="scan.outcome-deadline-reserve",
        )

        assert isinstance(payload, dict)
        assert payload["status"] == "WAITING_FOR_OBSERVATIONS"
        assert payload["processing_hash"] == result.processing_hash
        assert payload["shadow_evaluation_refresh"] == {
            "status": "DEFERRED",
            "reason": "SHADOW_EVALUATION_DEFERRED_DEADLINE",
            "decision_authority": "SUPPORTING_ONLY",
            "persisted": False,
        }
    finally:
        runtime.close()


def test_primary_outcome_processor_receives_terminalization_reserve(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    now = datetime.now(timezone.utc)
    hard_deadline = now + timedelta(seconds=60)
    result = _bounded_outcome_result(now)
    received: dict[str, object] = {}

    def process(**kwargs: object) -> OutcomeProcessingResult:
        received.update(kwargs)
        return result

    runtime.outcome_processor = SimpleNamespace(process=process)
    try:
        payload = runtime._process_outcomes(
            cancel_event=threading.Event(),
            deadline_at=hard_deadline,
            operation_token="scan.outcome-primary-reserve",
        )

        assert isinstance(payload, dict)
        assert received["deadline_at"] == hard_deadline - timedelta(seconds=5)
        assert received["operation_token"] == "scan.outcome-primary-reserve"
        assert payload["processing_hash"] == result.processing_hash
        assert payload["shadow_evaluation_refresh"]["reason"] == (
            "SHADOW_EVALUATION_UNCHANGED"
        )
    finally:
        runtime.close()


def _seed(runtime: OptionsCopilotRuntime) -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    thesis = runtime.shadow_learning.record_thesis(
        "thesis-runtime",
        champion_version=BASELINE_MODEL_VERSION,
        challenger_version="challenger-v2",
        thesis={"claim": "falling real yields support GLD"},
        created_at=now - timedelta(minutes=10),
        tags=("gld", "macro"),
    )
    evidence = runtime.shadow_learning.record_evidence(
        "evidence-runtime",
        thesis.thesis_id,
        source="official.release",
        evidence={"real_yield_bps": -5},
        published_at=now - timedelta(minutes=9),
        first_seen_at=now - timedelta(minutes=8),
        tags=("rates",),
    )
    first = runtime.shadow_learning.record_prediction(
        "prediction-runtime-1",
        thesis.thesis_id,
        evidence_ids=(evidence.evidence_id,),
        prediction={"direction": "UP", "probability": "0.62"},
        predicted_at=now - timedelta(minutes=7),
        independence_key="macro-window-1",
        tags=("gld", "rates"),
    )
    runtime.shadow_learning.resolve_outcome(
        first.prediction_id,
        outcome={"return": "0.013"},
        observed_at=now - timedelta(minutes=3),
        resolved_at=now - timedelta(minutes=2),
    )
    second = runtime.shadow_learning.record_prediction(
        "prediction-runtime-2",
        thesis.thesis_id,
        evidence_ids=(evidence.evidence_id,),
        prediction={"direction": "UP", "probability": "0.55"},
        predicted_at=now - timedelta(minutes=1),
        independence_key="macro-window-2",
        tags=("gld", "rates", "volatility"),
    )
    return first.prediction_id, second.prediction_id


def _seed_resolved_samples(
    runtime: OptionsCopilotRuntime,
    challenger_version: str,
    count: int,
    *,
    day_offset: int,
) -> None:
    base = datetime(2026, 8, 1, tzinfo=timezone.utc) + timedelta(days=day_offset)
    thesis = runtime.shadow_learning.record_thesis(
        f"thesis-{challenger_version}",
        champion_version=BASELINE_MODEL_VERSION,
        challenger_version=challenger_version,
        thesis={"claim": f"isolated cohort for {challenger_version}"},
        created_at=base,
        tags=("isolation", challenger_version),
    )
    evidence = runtime.shadow_learning.record_evidence(
        f"evidence-{challenger_version}",
        thesis.thesis_id,
        source="official.release",
        evidence={"challenger": challenger_version},
        published_at=base + timedelta(minutes=1),
        first_seen_at=base + timedelta(minutes=2),
        tags=("isolation",),
    )
    for index in range(count):
        prediction = runtime.shadow_learning.record_prediction(
            f"prediction-{challenger_version}-{index}",
            thesis.thesis_id,
            evidence_ids=(evidence.evidence_id,),
            prediction={"index": index},
            predicted_at=base + timedelta(minutes=3, seconds=index),
            independence_key=f"{challenger_version}-window-{index}",
            tags=("isolation",),
        )
        runtime.shadow_learning.resolve_outcome(
            prediction.prediction_id,
            outcome={"index": index},
            observed_at=base + timedelta(hours=1, seconds=index),
            resolved_at=base + timedelta(hours=2, seconds=index),
        )


def _seed_evaluation_samples(
    runtime: OptionsCopilotRuntime,
    count: int,
) -> None:
    base = datetime(2026, 8, 1, tzinfo=timezone.utc)
    thesis = runtime.shadow_learning.record_thesis(
        "thesis-api-evaluation-stage",
        champion_version=BASELINE_MODEL_VERSION,
        challenger_version="deepseek-news-advisory-v2",
        thesis={"purpose": "runtime API frontend stage regression"},
        created_at=base,
    )
    evidence = runtime.shadow_learning.record_evidence(
        "evidence-api-evaluation-stage",
        thesis.thesis_id,
        source="TEST_ONLY",
        evidence={"symbol": "SPY"},
        published_at=base + timedelta(minutes=1),
        first_seen_at=base + timedelta(minutes=2),
    )
    champion_classification = {"direction": "BEARISH", "confidence": "0.60"}
    baseline_body = {
        "schema": "options_copilot.deterministic_champion_baseline.v1",
        "champion_version": BASELINE_MODEL_VERSION,
        "analysis_cutoff_at": (base + timedelta(minutes=3)).isoformat(),
        "classification": champion_classification,
        "classification_hash": canonical_hash(champion_classification),
    }
    champion_baseline = {
        **baseline_body,
        "baseline_hash": canonical_hash(baseline_body),
    }
    for index in range(count):
        prediction = runtime.shadow_learning.record_prediction(
            f"prediction-api-evaluation-stage-{index}",
            thesis.thesis_id,
            evidence_ids=(evidence.evidence_id,),
            prediction={
                "schema": "options_copilot.news_shadow_prediction.v2",
                "symbol": "SPY",
                "classification": {
                    "direction": "BULLISH",
                    "confidence": "0.80",
                },
                "model_visible_snapshot_hash": "a" * 64,
                "champion_baseline": champion_baseline,
            },
            predicted_at=base + timedelta(minutes=4, seconds=index),
            independence_key=f"event:api-evaluation-stage-{index}",
        )
        runtime.shadow_learning.resolve_outcome(
            prediction.prediction_id,
            outcome={
                "underlying": {"baseline_price": "100", "price": "102"},
                "thesis_validity": {"status": "VALID", "valid": True},
            },
            observed_at=base + timedelta(hours=1, seconds=index),
            resolved_at=base + timedelta(hours=1, minutes=1, seconds=index),
            prediction_hash=prediction.content_hash,
        )


def test_learning_evaluation_stage_survives_runtime_api_and_frontend(
    tmp_path: Path,
) -> None:
    expected_by_count = {
        0: "COLLECTING",
        1: "COMPARISON_AVAILABLE",
        29: "COMPARISON_AVAILABLE",
        30: "DISCOVERY",
    }
    api_evaluations: list[dict[str, object]] = []
    for count, expected in expected_by_count.items():
        runtime = _runtime(tmp_path / f"samples-{count}")
        try:
            if count:
                _seed_evaluation_samples(runtime, count)
                runtime.shadow_evaluation_store.refresh(runtime.shadow_learning)
            app = create_app(runtime.services())
            summary = asyncio.run(_route(app, "/api/learning")())
            evaluation = summary["governance"]["evaluation"]

            assert evaluation["status"] == (
                "COLLECTING" if count == 0 else "AVAILABLE"
            )
            assert evaluation["independent_count"] == count
            assert evaluation["stage"] == expected
            api_evaluations.append(evaluation)
        finally:
            runtime.close()

    script_uri = (
        Path(__file__).parents[2] / "options_copilot" / "frontend" / "app.js"
    ).resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ learningEvaluationStage }} = await import("{script_uri}");
const evaluations = {json.dumps(api_evaluations)};
console.log(JSON.stringify(evaluations.map((evaluation) => ({{
  apiStage: evaluation.stage,
  frontendStage: learningEvaluationStage(evaluation.independent_count),
}}))));'''
    rendered = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert json.loads(rendered.stdout) == [
        {"apiStage": expected, "frontendStage": expected}
        for expected in expected_by_count.values()
    ]


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)


def test_runtime_composes_durable_shadow_learning_without_changing_legacy_contract(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        first_id, second_id = _seed(runtime)

        status = runtime.learning_status()
        assert status["champion"] == BASELINE_MODEL_VERSION
        assert status["challenger"] == "challenger-v2"
        assert status["stage"] == "COLLECTING"
        assert status["decision_records"] == 1
        assert status["automatic_production_promotion"] is False
        assert status["a_grade_unlocked"] is False
        assert status["minimum_discovery_scenarios"] == 30
        shadow = status["shadow_learning"]
        assert shadow["mode"] == "SHADOW_ONLY"
        assert shadow["record_counts"] == {
            "THESIS": 1,
            "EVIDENCE": 1,
            "PREDICTION": 2,
            "OUTCOME": 1,
        }
        assert shadow["independent_samples"] == 1
        assert shadow["selected_challenger"] == "challenger-v2"
        assert shadow["ledger"]["integrity_verified"] is True
        assert set(shadow["authority"].values()) == {False, True}
        assert shadow["authority"]["external_human_approval_required"] is True
        assert status["outcome_processing"]["status"] == "UNAVAILABLE"
        assert status["outcome_processing"]["reason_codes"] == (
            "OUTCOME_PROCESSOR_UNAVAILABLE",
        )
        assert status["outcome_capture"] == {
            "status": "UNAVAILABLE",
            "checked_at": None,
            "specs_seen": 0,
            "specs_due": 0,
            "observations_appended": 0,
            "records_blocked": 0,
            "records_skipped": 0,
            "reason_codes": ("OUTCOME_CAPTURE_UNAVAILABLE",),
            "durable_status_counts": {},
            "durable_blocker_counts": {},
            "direction_outcomes_enabled": False,
            "option_economics_requires_bound_candidate": True,
            "decision_authority": "SUPPORTING_ONLY",
        }
        horizons = status["outcome_horizons"]
        assert horizons["status"] == "READY"
        assert horizons["selected_challenger"] == "challenger-v2"
        assert horizons["complete_through_sequence"] == 5
        assert horizons["verified_head_sequence"] == 5
        assert horizons["verified_head_hash"] == runtime.shadow_learning.head_hash()
        assert set(horizons["horizons"]) == {
            "30M",
            "SESSION_CLOSE",
            "1D",
            "3D",
            "5D",
        }
        assert {
            item["status"] for item in horizons["horizons"].values()
        } == {"NOT_OBSERVED"}

        records = runtime.learning_records("PREDICTION", "challenger-v2", 10)
        assert records["count"] == 2
        assert [row["prediction_id"] for row in records["records"]] == [
            first_id,
            second_id,
        ]
        detail = runtime.learning_record(first_id)["record"]
        assert detail["decision_authority"] == "OBSERVATION_ONLY"
        assert detail["evidence_bindings"][0]["evidence_id"] == "evidence-runtime"
        replay = runtime.learning_replay(first_id)["replay"]
        assert replay["outcome"]["prediction_hash"] == detail["content_hash"]
        similar = runtime.learning_similar(first_id, 10)
        assert similar["matches"][0]["replay"]["prediction"]["prediction_id"] == (
            second_id
        )

        services = runtime.services()
        assert services.learning_records_provider is not None
        assert services.learning_record_provider is not None
        assert services.learning_replay_provider is not None
        assert services.learning_similarity_provider is not None
    finally:
        runtime.close()

    reopened = _runtime(tmp_path)
    try:
        assert reopened.learning_status()["shadow_learning"]["record_count"] == 5
        assert reopened.learning_replay(first_id)["replay"]["outcome"] is not None
    finally:
        reopened.close()


def test_learning_summary_never_combines_samples_across_challengers(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        _seed_resolved_samples(runtime, "challenger-a", 15, day_offset=0)
        _seed_resolved_samples(runtime, "challenger-b", 15, day_offset=1)

        status = runtime.learning_status()
        shadow = status["shadow_learning"]

        assert status["challenger"] == "challenger-a"
        assert status["stage"] == "COLLECTING"
        assert shadow["selected_challenger"] == "challenger-a"
        assert shadow["challengers"] == ["challenger-a", "challenger-b"]
        assert shadow["independent_samples"] == 15
        assert shadow["stage"] == "COLLECTING"
        assert shadow["discovery_ready"] is False
        assert runtime.shadow_learning.governance_state(
            "challenger-a"
        ).independent_samples == 15
        assert runtime.shadow_learning.governance_state(
            "challenger-b"
        ).independent_samples == 15
        with pytest.raises(RuntimeError, match="challenger_version is required"):
            runtime.shadow_learning.independent_sample_count()
    finally:
        runtime.close()


def test_outcome_horizon_summary_reuses_one_verified_snapshot_and_aggregates_subjects() -> None:
    observed_at = datetime(2026, 8, 9, tzinfo=timezone.utc)
    rows: list[OutcomeRecord] = []

    def row(
        sequence: int,
        *,
        horizon: str,
        challenger: str = "challenger-a",
        status: str = "OBSERVED",
    ) -> OutcomeRecord:
        digest = f"{sequence:064x}"
        return OutcomeRecord(
            sequence=sequence,
            outcome_id=f"outcome-{sequence}",
            prediction_id=f"prediction-{sequence}",
            prediction_hash=digest,
            thesis_id="thesis-a",
            challenger_version=challenger,
            independence_key=f"subject-{sequence}",
            outcome={
                "schema": "options_copilot.outcome_observation.v2",
                "subject_kind": "PREDICTION",
                "subject_id": f"subject-{sequence}",
                "horizon": horizon,
                "status": status,
            },
            observed_at=observed_at + timedelta(seconds=sequence),
            resolved_at=observed_at + timedelta(seconds=sequence + 1),
            tags=(),
            content_hash=digest,
            previous_hash="0" * 64,
            chain_hash=digest,
            appended_at=observed_at + timedelta(seconds=sequence + 2),
        )

    rows.extend(row(index, horizon="30M") for index in range(1, 5002))
    rows.extend(
        (
            row(5002, horizon="SESSION_CLOSE", status="BLOCKED"),
            row(5003, horizon="1D", status="UNCERTAIN"),
            row(5004, horizon="3D"),
            row(5005, horizon="5D"),
            row(5006, horizon="30M", challenger="challenger-b"),
        )
    )

    class FakeLedger:
        def __init__(self) -> None:
            self.calls = 0

        def verified_replay_snapshot(self):
            self.calls += 1
            return SimpleNamespace(
                records=tuple(rows),
                verified_head_sequence=5006,
                verified_head_hash=f"{5006:064x}",
            )

    ledger = FakeLedger()
    holder = SimpleNamespace(shadow_learning=ledger)
    summary = OptionsCopilotRuntime._outcome_horizon_summary(
        holder,
        "challenger-a",
    )

    assert ledger.calls == 1
    assert summary["complete_through_sequence"] == 5006
    assert summary["verified_head_sequence"] == 5006
    assert summary["verified_head_hash"] == f"{5006:064x}"
    horizons = summary["horizons"]
    assert horizons["30M"]["status"] == "OBSERVED"
    assert horizons["30M"]["count"] == 5001
    assert horizons["30M"]["observed_count"] == 5001
    assert horizons["SESSION_CLOSE"]["blocked_count"] == 1
    assert horizons["1D"]["uncertain_count"] == 1
    assert horizons["3D"]["observed_count"] == 1
    assert horizons["5D"]["observed_count"] == 1


def test_verified_record_cursor_traverses_more_than_5000_rows_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    ledger = runtime.shadow_learning
    created_at = datetime(2026, 8, 9, tzinfo=timezone.utc)
    previous_hash = "0" * 64
    rows: list[tuple[object, ...]] = []
    for sequence in range(1, 5002):
        thesis_id = f"cursor-thesis-{sequence}"
        document = {
            "schema_version": 1,
            "record_type": "THESIS",
            "thesis_id": thesis_id,
            "champion_version": BASELINE_MODEL_VERSION,
            "challenger_version": "challenger-cursor",
            "created_at": datetime_text(created_at),
            "tags": [],
            "thesis": {"sequence": sequence},
        }
        document_json = canonical_json(document)
        content_hash = canonical_hash(document)
        chain_hash = hashlib.sha256(
            f"{sequence}:{previous_hash}:{content_hash}".encode("ascii")
        ).hexdigest()
        rows.append(
            (
                sequence,
                thesis_id,
                "THESIS",
                thesis_id,
                None,
                "challenger-cursor",
                None,
                datetime_text(created_at),
                canonical_json(()),
                document_json,
                content_hash,
                previous_hash,
                chain_hash,
                datetime_text(created_at),
            )
        )
        previous_hash = chain_hash

    try:
        with ledger._lock:  # noqa: SLF001 - bulk fixture avoids O(N^2) appends.
            ledger._connection.executemany(  # noqa: SLF001
                """
                INSERT INTO shadow_learning_records(
                    sequence, record_id, record_type, thesis_id, prediction_id,
                    challenger_version, independence_key, occurred_at, tags_json,
                    document_json, content_hash, previous_hash, chain_hash, appended_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        def repeated_full_scan_forbidden() -> None:
            raise AssertionError("cursor paging must not call full assert_integrity")

        original_verified_row = learning_shadow_module._verified_row
        verified_rows = 0

        def counted_verified_row(*args, **kwargs):
            nonlocal verified_rows
            verified_rows += 1
            return original_verified_row(*args, **kwargs)

        monkeypatch.setattr(ledger, "assert_integrity", repeated_full_scan_forbidden)
        monkeypatch.setattr(
            learning_shadow_module,
            "_verified_row",
            counted_verified_row,
        )
        cursor = ledger.open_verified_record_cursor()
        records = []
        page_count = 0
        while not cursor.complete:
            page, cursor = ledger.verified_record_cursor_page(
                cursor=cursor,
                limit=5000,
            )
            page_count += 1
            records.extend(page)

        assert page_count == 2
        assert len(records) == 5001
        assert verified_rows == 5001
        assert [record.sequence for record in records[:2]] == [1, 2]
        assert records[-1].sequence == 5001
        assert cursor.complete is True
        assert cursor.verified_head_sequence == 5001
        assert cursor.verified_head_hash == previous_hash

        cold_started = time.perf_counter()
        runtime.learning_status()
        cold_elapsed = time.perf_counter() - cold_started
        verified_after_first_summary = verified_rows
        started = time.perf_counter()
        second_summary = runtime.learning_status()
        second_elapsed = time.perf_counter() - started

        assert second_summary["shadow_learning"]["record_count"] == 5001
        assert verified_after_first_summary == 10002
        assert verified_rows == verified_after_first_summary
        assert cold_elapsed < 5.0
        assert second_elapsed < 1.0
    finally:
        runtime.close()


def test_verified_record_cursor_allows_append_beyond_frozen_head(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    ledger = runtime.shadow_learning
    created_at = datetime(2026, 8, 9, tzinfo=timezone.utc)
    try:
        ledger.record_thesis(
            "cursor-head-one",
            champion_version=BASELINE_MODEL_VERSION,
            challenger_version="challenger-cursor",
            thesis={"sequence": 1},
            created_at=created_at,
        )
        cursor = ledger.open_verified_record_cursor()
        ledger.record_thesis(
            "cursor-head-two",
            champion_version=BASELINE_MODEL_VERSION,
            challenger_version="challenger-cursor",
            thesis={"sequence": 2},
            created_at=created_at,
        )

        page, cursor = ledger.verified_record_cursor_page(
            cursor=cursor,
            limit=5000,
        )

        assert tuple(record.thesis_id for record in page) == ("cursor-head-one",)
        assert cursor.complete is True
        assert cursor.verified_head_sequence == 1

        next_cursor = ledger.open_verified_record_cursor()
        next_page, next_cursor = ledger.verified_record_cursor_page(
            cursor=next_cursor,
            limit=5000,
        )
        assert tuple(record.thesis_id for record in next_page) == (
            "cursor-head-one",
            "cursor-head-two",
        )
        assert next_cursor.complete is True
    finally:
        runtime.close()


def test_verified_replay_snapshot_caches_head_and_verifies_only_new_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    ledger = runtime.shadow_learning
    created_at = datetime(2026, 8, 9, tzinfo=timezone.utc)
    original_verified_row = learning_shadow_module._verified_row
    verified_rows = 0

    def counted_verified_row(*args, **kwargs):
        nonlocal verified_rows
        verified_rows += 1
        return original_verified_row(*args, **kwargs)

    monkeypatch.setattr(
        learning_shadow_module,
        "_verified_row",
        counted_verified_row,
    )
    try:
        ledger.record_thesis(
            "snapshot-head-one",
            champion_version=BASELINE_MODEL_VERSION,
            challenger_version="challenger-snapshot",
            thesis={"sequence": 1},
            created_at=created_at,
        )
        first = ledger.verified_replay_snapshot()
        same = ledger.verified_replay_snapshot()

        assert same is first
        assert verified_rows == 1

        ledger.record_thesis(
            "snapshot-head-two",
            champion_version=BASELINE_MODEL_VERSION,
            challenger_version="challenger-snapshot",
            thesis={"sequence": 2},
            created_at=created_at,
        )
        verified_before_increment = verified_rows
        extended = ledger.verified_replay_snapshot()

        assert tuple(record.sequence for record in extended.records) == (1, 2)
        assert extended.verified_head_sequence == 2
        assert verified_before_increment == 2
        assert verified_rows == verified_before_increment
        assert ledger.verified_replay_snapshot() is extended
        assert verified_rows == verified_before_increment
    finally:
        runtime.close()


def test_verified_snapshot_rejects_same_head_external_tamper(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    ledger = runtime.shadow_learning
    created_at = datetime(2026, 8, 9, tzinfo=timezone.utc)
    external = sqlite3.connect(ledger.path)
    try:
        ledger.record_thesis(
            "same-head-tamper",
            champion_version=BASELINE_MODEL_VERSION,
            challenger_version="challenger-tamper",
            thesis={"direction": "BULLISH"},
            created_at=created_at,
        )
        ledger.assert_integrity()
        row = external.execute(
            "SELECT document_json FROM shadow_learning_records WHERE sequence=1"
        ).fetchone()
        assert row is not None
        document = json.loads(str(row[0]))
        document["thesis"]["direction"] = "BEARISH"
        external.executescript(
            """
            DROP TRIGGER shadow_learning_records_no_update;
            DROP TRIGGER shadow_learning_records_no_delete;
            """
        )
        external.execute(
            "UPDATE shadow_learning_records SET document_json=? WHERE sequence=1",
            (canonical_json(document),),
        )
        external.executescript(
            """
            CREATE TRIGGER shadow_learning_records_no_update
            BEFORE UPDATE ON shadow_learning_records
            BEGIN
                SELECT RAISE(ABORT, 'shadow learning records are immutable');
            END;
            CREATE TRIGGER shadow_learning_records_no_delete
            BEFORE DELETE ON shadow_learning_records
            BEGIN
                SELECT RAISE(ABORT, 'shadow learning records are immutable');
            END;
            """
        )
        external.commit()

        with pytest.raises(LedgerTampered, match="content hash mismatch"):
            ledger.assert_integrity()
        with pytest.raises(LedgerTampered, match="content hash mismatch"):
            ledger.get_thesis("same-head-tamper")
    finally:
        external.close()
        runtime.close()


def test_learning_projection_refreshes_safely_during_concurrent_appends(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    ledger = runtime.shadow_learning
    created_at = datetime(2026, 8, 9, tzinfo=timezone.utc)
    append_errors: list[BaseException] = []

    def append_rows() -> None:
        try:
            for index in range(20):
                ledger.record_thesis(
                    f"concurrent-thesis-{index}",
                    champion_version=BASELINE_MODEL_VERSION,
                    challenger_version="challenger-concurrent",
                    thesis={"sequence": index},
                    created_at=created_at,
                )
        except BaseException as exc:  # pragma: no cover - asserted below
            append_errors.append(exc)

    worker = threading.Thread(target=append_rows)
    try:
        worker.start()
        observed: list[tuple[int, int]] = []
        while worker.is_alive():
            status = runtime.learning_status()
            observed.append(
                (
                    int(status["shadow_learning"]["record_count"]),
                    int(status["outcome_horizons"]["verified_head_sequence"]),
                )
            )
        worker.join(timeout=5)
        final = runtime.learning_status()

        assert not worker.is_alive()
        assert append_errors == []
        assert all(count == head for count, head in observed)
        assert final["shadow_learning"]["record_count"] == 20
        assert final["outcome_horizons"]["verified_head_sequence"] == 20
    finally:
        worker.join(timeout=5)
        runtime.close()


def test_learning_api_adds_only_get_queries_and_forces_observation_authority(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    try:
        first_id, second_id = _seed(runtime)
        app = create_app(runtime.services())

        summary = asyncio.run(_route(app, "/api/learning")())
        records = asyncio.run(
            _route(app, "/api/learning/records")(
                "PREDICTION", "challenger-v2", 10
            )
        )
        detail = asyncio.run(
            _route(app, "/api/learning/records/{record_id}")(first_id)
        )
        replay = asyncio.run(
            _route(
                app, "/api/learning/predictions/{prediction_id}/replay"
            )(first_id)
        )
        similar = asyncio.run(
            _route(
                app, "/api/learning/predictions/{prediction_id}/similar"
            )(first_id, 10)
        )

        for payload in (summary, records, detail, replay, similar):
            assert payload["read_only"] is True
            assert payload["decision_authority"] == "OBSERVATION_ONLY"
            assert payload["automatic_production_promotion"] is False
            assert payload["production_weights_mutable"] is False
            assert payload["production_rules_mutable"] is False
            assert payload["a_grade_unlocked"] is False
            assert payload["approval_authority"] is False
            assert payload["bridge_authority"] is False
            assert payload["order_authority"] is False

        assert summary["champion"] == BASELINE_MODEL_VERSION
        assert summary["decision_records"] == 1
        assert set(summary["outcome_horizons"]) == {
            "status",
            "decision_authority",
            "selected_challenger",
            "complete_through_sequence",
            "verified_head_sequence",
            "verified_head_hash",
            "horizons",
        }
        assert summary["outcome_horizons"]["decision_authority"] == (
            "SUPPORTING_ONLY"
        )
        assert records["count"] == 2
        assert detail["record"]["prediction_id"] == first_id
        assert replay["replay"]["outcome"] is not None
        assert similar["matches"][0]["replay"]["prediction"]["prediction_id"] == (
            second_id
        )

        learning_routes = [
            route
            for route in app.routes
            if getattr(route, "path", "").startswith("/api/learning")
        ]
        assert {route.path for route in learning_routes} == {
            "/api/learning",
            "/api/learning/records",
            "/api/learning/records/{record_id}",
            "/api/learning/predictions/{prediction_id}/replay",
            "/api/learning/predictions/{prediction_id}/similar",
        }
        assert all(route.methods == {"GET"} for route in learning_routes)

        with pytest.raises(HTTPException) as missing:
            asyncio.run(
                _route(app, "/api/learning/records/{record_id}")("missing-record")
            )
        assert missing.value.status_code == 404
        with pytest.raises(HTTPException) as invalid:
            asyncio.run(_route(app, "/api/learning/records")(None, None, 0))
        assert invalid.value.status_code == 422
    finally:
        runtime.close()
