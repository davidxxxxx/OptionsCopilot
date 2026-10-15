"""Immutable multi-horizon outcome processing and authority isolation tests."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from typing import Mapping

import pytest

import options_copilot.learning.outcome_processor as outcome_processor_module

from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.learning.outcome_processor import (
    ExactHorizonOutcomeCapture,
    OUTCOME_CAPTURE_SPEC_KIND,
    OUTCOME_OBSERVATION_KIND,
    OUTCOME_HORIZONS,
    OUTCOME_TARGET_RULES,
    EvidenceStoreOutcomeObservationProvider,
    ImmutableOutcomeProcessor,
    OutcomeObservationUnavailable,
    OutcomeCaptureCoordinator,
    OutcomeCaptureLoop,
)
from options_copilot.learning.outcomes import (
    OutcomeRecorder,
    OutcomeValidationError,
    normalize_bound_outcome_result,
    resolve_outcome_horizon,
)
from options_copilot.learning.progress import OutcomeProgressStore
from options_copilot.learning.evaluation_runtime import ShadowEvaluationStore
from options_copilot.learning_shadow import ShadowLearningLedger
from options_copilot.news.macro_proxy import MarketProxyBinding
from options_copilot.news.models import (
    ClassifiedEvent,
    EventCategory,
    ImpactDirection,
    ImpactHorizon,
    NewsInput,
)
from options_copilot.news.shadow_research import (
    PredictionSpec,
    ResearchAdvisoryProjection,
)
from options_copilot.news.shadow_prediction import (
    NEWS_SHADOW_PREDICTION_SCHEMA,
    news_shadow_prediction_id,
)
from options_copilot.news.shadow_store import NewsShadowLearningWriter
from options_copilot.runtime import OptionsCopilotRuntime
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore, StoredEvidence


BASE = datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)
POLICY_HASH = "a" * 64


class MutableClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class FixtureObservationProvider:
    def __init__(self, observations: Mapping[tuple[str, str, str], Mapping[str, object]]) -> None:
        self.observations = dict(observations)

    def observe(
        self,
        target: Mapping[str, object],
        *,
        horizon: str,
        as_of: datetime,
    ) -> Mapping[str, object] | None:
        value = self.observations.get(
            (str(target["subject_kind"]), str(target["subject_id"]), horizon)
        )
        if value is None:
            return None
        received_at = value.get("revision_received_at")
        if not isinstance(received_at, datetime) or received_at > as_of:
            return None
        return value


def _seed_predictions(ledger: ShadowLearningLedger) -> tuple[str, ...]:
    thesis = ledger.record_thesis(
        "thesis-outcome-processor",
        champion_version="champion-v1",
        challenger_version="challenger-v1",
        thesis={"purpose": "research priority calibration only"},
        created_at=BASE - timedelta(hours=1),
        tags=("shadow", "research-priority"),
    )
    evidence = ledger.record_evidence(
        "evidence-outcome-processor",
        thesis.thesis_id,
        source="TEST_ONLY",
        evidence={"symbol": "SPY"},
        published_at=BASE - timedelta(minutes=2),
        first_seen_at=BASE - timedelta(minutes=1),
        tags=("symbol:SPY",),
    )
    prediction_ids = []
    for horizon in OUTCOME_HORIZONS:
        prediction_id = f"prediction-{horizon.lower()}"
        ledger.record_prediction(
            prediction_id,
            thesis.thesis_id,
            evidence_ids=(evidence.evidence_id,),
            prediction={
                "schema": "options_copilot.test_prediction.v1",
                "symbol": "SPY",
                "horizon": horizon,
                "target_rule": OUTCOME_TARGET_RULES[horizon],
                "research_priority_score": "0.75",
                "decision_authority": "SUPPORTING_ONLY",
            },
            predicted_at=BASE,
            horizon_at=None,
            independence_key="event:spy:outcome-processor",
            tags=("symbol:SPY", f"horizon:{horizon}"),
        )
        prediction_ids.append(prediction_id)
    return tuple(prediction_ids)


def _candidate_target() -> dict[str, object]:
    candidate_id = "candidate-outcome-processor"
    candidate_hash = canonical_hash({"candidate_id": candidate_id, "symbol": "SPY"})
    result_authority = _result_authority(candidate_hash)
    return {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "CANDIDATE",
        "subject_id": candidate_id,
        "subject_hash": candidate_hash,
        "symbol": "SPY",
        "occurred_at": BASE,
        "result_authority": result_authority,
        "outcome_template": {
            "decision_id": "decision-outcome-processor",
            "decision_hash": "1" * 64,
            "candidate_id": candidate_id,
            "candidate_hash": candidate_hash,
            "ranking_snapshot_id": "ranking-outcome-processor",
            "ranking_snapshot_hash": "2" * 64,
            "ranking_basis_hash": "3" * 64,
            "decision_at": BASE,
            "input_hash": "4" * 64,
            "evidence_hash": "5" * 64,
            "broker_snapshot_hash": "6" * 64,
            "current_policy_version": "v1",
            "current_policy_hash": POLICY_HASH,
            "policy_authority_marker_hash": "7" * 64,
            "cost_version": EXECUTION_COST_VERSION,
            "cost_hash": EXECUTION_COST_HASH,
            "exit_policy_hash": "8" * 64,
            "thesis_hash": "9" * 64,
            "quote_identity_hash": "a" * 64,
            "position_management_hash": "b" * 64,
            "counterfactual_spec_hash": "c" * 64,
            "quote_quality": {
                "status": "MISSING",
                "bid_ask_complete": False,
                "source": "OUTCOME_EVIDENCE_UNAVAILABLE",
            },
            "cluster_evidence": {
                "status": "KNOWN",
                "ticker": "SPY",
                "issuer_id": "issuer-SPY",
                "provider": "RANKING_LEDGER",
                "event_id": "scan-outcome-processor",
                "slot_at": BASE,
            },
        },
    }


def _capture_target(subject_id: str = "candidate-outcome-processor") -> dict[str, object]:
    target = _candidate_target()
    target["subject_id"] = subject_id
    target["subject_hash"] = canonical_hash(
        {"candidate_subject_id": subject_id, "version": 1}
    )
    outcome_template = target["outcome_template"]
    assert isinstance(outcome_template, dict)
    outcome_template["candidate_id"] = subject_id
    outcome_template["candidate_hash"] = target["subject_hash"]
    target["result_authority"] = _result_authority(str(target["subject_hash"]))
    target["capture_plan"] = {
        "schema": "options_copilot.outcome_capture_plan.v1",
        "status": "READY",
        "reason_codes": (),
        "benchmark_symbol": "SPY",
        "underlying": {
            "symbol": "SPY",
            "price": "100.00",
            "observed_at": BASE,
            "source": "IBKR_READ_ONLY_BASELINE",
            "source_id": "underlying-baseline",
            "source_hash": "1" * 64,
        },
        "benchmark": {
            "symbol": "SPY",
            "price": "100.00",
            "observed_at": BASE,
            "source": "IBKR_READ_ONLY_BASELINE",
            "source_id": "benchmark-baseline",
            "source_hash": "2" * 64,
        },
        "legs": (
            {
                "con_id": 101,
                "side": "BUY",
                "ratio": 1,
                "multiplier": 100,
                "strike": "500",
                "bid": "1.20",
                "ask": "1.30",
                "implied_volatility": "0.20",
                "volume": 100,
            },
            {
                "con_id": 102,
                "side": "SELL",
                "ratio": 1,
                "multiplier": 100,
                "strike": "505",
                "bid": "0.45",
                "ask": "0.55",
                "implied_volatility": "0.22",
                "volume": 80,
            },
        ),
        "max_loss_usd": "130.00",
    }
    return target


class FakeOutcomeMarketAdapter:
    def __init__(self, *, delay_seconds: int = 1) -> None:
        self.delay_seconds = delay_seconds
        self.calls: list[tuple[Mapping[str, object], ...]] = []

    def observe(
        self,
        specs: tuple[Mapping[str, object], ...],
        *,
        expected_at: datetime,
    ) -> Mapping[str, object]:
        self.calls.append(specs)
        observed_at = expected_at + timedelta(seconds=self.delay_seconds)
        return {
            "schema": "options_copilot.outcome_market_batch.v1",
            "observed_at": observed_at,
            "source": "IBKR_READ_ONLY_FAKE",
            "source_id": f"batch:{expected_at.isoformat()}",
            "source_hash": canonical_hash(
                {"expected_at": expected_at, "count": len(specs)}
            ),
            "underlyings": (
                {
                    "symbol": "SPY",
                    "price": "101.00",
                    "observed_at": observed_at,
                    "source_id": "underlying-current",
                    "source_hash": "3" * 64,
                },
            ),
            "quotes": (
                {
                    "con_id": 101,
                    "bid": "1.40",
                    "ask": "1.50",
                    "implied_volatility": "0.21",
                    "volume": 140,
                    "observed_at": observed_at,
                },
                {
                    "con_id": 102,
                    "bid": "0.50",
                    "ask": "0.60",
                    "implied_volatility": "0.23",
                    "volume": 100,
                    "observed_at": observed_at,
                },
            ),
        }


def _calendar_sessions() -> list[dict[str, object]]:
    closes = (
        datetime(2026, 8, 3, 20, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 4, 20, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 5, 20, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 6, 20, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 7, 20, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 10, 20, 0, tzinfo=timezone.utc),
    )
    sessions = []
    for index, close_at in enumerate(closes):
        row = {
            "trading_date": close_at.date().isoformat(),
            "open_at": close_at - timedelta(hours=6, minutes=30),
            "close_at": close_at,
            "source": "OFFICIAL_SESSION_CALENDAR",
            "source_id": f"session-{close_at.date().isoformat()}",
            "source_hash": f"{index + 1:x}" * 64,
            "observed_at": BASE - timedelta(minutes=5),
        }
        sessions.append(row)
    return sessions


def _horizon_at(horizon: str) -> datetime:
    if horizon == "30M":
        return BASE + timedelta(minutes=30)
    index = {"SESSION_CLOSE": 0, "1D": 1, "3D": 3, "5D": 5}[horizon]
    return _calendar_sessions()[index]["close_at"]  # type: ignore[return-value]


def _provenance(name: str, observed_at: datetime) -> dict[str, object]:
    return {
        "status": "AVAILABLE",
        "source": "TEST_ONLY_DURABLE_OBSERVATION",
        "source_id": f"{name}:{observed_at.isoformat()}",
        "source_hash": canonical_hash({"name": name, "observed_at": observed_at}),
        "observed_at": observed_at,
    }


def _unavailable(reason: str) -> dict[str, object]:
    return {
        "status": "UNAVAILABLE",
        "reason_code": reason,
        "source": None,
        "source_id": None,
        "source_hash": None,
        "observed_at": None,
    }


def _result_authority(candidate_hash: str) -> dict[str, object]:
    costs = Decimal("2.50")
    maximum_loss = Decimal("100.00")
    body = {
        "schema": "options_copilot.outcome_result_authority.v1",
        "candidate_hash": candidate_hash,
        "cost_contract_hash": EXECUTION_COST_HASH,
        "legs": (
            {
                "contract_id": "SPY-TEST-LONG",
                "side": "BUY",
                "quantity": 1,
            },
        ),
        "entry_value_usd": Decimal("185.00"),
        "costs_usd": costs,
        "costs_hash": canonical_hash(
            {
                "schema": "options_copilot.outcome_cost_evidence.v1",
                "candidate_hash": candidate_hash,
                "cost_contract_hash": EXECUTION_COST_HASH,
                "costs_usd": costs,
            }
        ),
        "max_loss_usd": maximum_loss,
        "max_loss_evidence_hash": canonical_hash(
            {
                "schema": "options_copilot.outcome_max_loss_evidence.v1",
                "candidate_hash": candidate_hash,
                "max_loss_usd": maximum_loss,
            }
        ),
    }
    return {**body, "authority_hash": canonical_hash(body)}


def _result_quote(
    candidate_hash: str,
    observed_at: datetime,
    *,
    bid: Decimal,
    ask: Decimal,
) -> dict[str, object]:
    body = {
        "contract_id": "SPY-TEST-LONG",
        "side": "BUY",
        "quantity": 1,
        "bid": bid,
        "ask": ask,
        "source": "TEST_ONLY",
        "source_content_hash": "9" * 64,
        "observed_at": observed_at,
    }
    return {
        **body,
        "quote_identity_hash": canonical_hash(
            {
                "schema": "options_copilot.outcome_executable_quote.v1",
                "candidate_hash": candidate_hash,
                **body,
            }
        ),
    }


def _observation(target: Mapping[str, object], horizon: str) -> dict[str, object]:
    horizon_at = _horizon_at(horizon)
    observed_at = horizon_at + timedelta(seconds=1)
    horizon_evidence = {
        "schema": "options_copilot.outcome_horizon_evidence.v1",
        "target_rule": OUTCOME_TARGET_RULES[horizon],
        "method": "ELAPSED_TIME" if horizon == "30M" else "SESSION_CALENDAR",
        "sessions": [] if horizon == "30M" else _calendar_sessions(),
    }
    prediction = target["subject_kind"] == "PREDICTION"
    return {
        "schema": "options_copilot.outcome_observation.v2",
        "subject_kind": target["subject_kind"],
        "subject_id": target["subject_id"],
        "subject_hash": target["subject_hash"],
        "horizon": horizon,
        "target_rule": OUTCOME_TARGET_RULES[horizon],
        "horizon_at": horizon_at,
        "economic_observed_at": observed_at,
        "revision_received_at": observed_at + timedelta(seconds=1),
        "horizon_evidence": horizon_evidence,
        "ledger_binding": {
            "status": "ACTIVE",
            "evidence_id": "evidence-" + canonical_hash(
                {"subject": target["subject_id"], "horizon": horizon}
            ),
            "identity": "outcome-observation-" + canonical_hash(
                {"subject": target["subject_id"], "horizon": horizon}
            ),
            "content_hash": "9" * 64,
            "row_hash": "a" * 64,
            "provider": "TEST_ONLY",
            "source_id": f"observation-{target['subject_id']}-{horizon}",
            "observed_at": observed_at + timedelta(seconds=1),
        },
        "underlying": {
            "symbol": "SPY",
            "baseline_price": "100.00",
            "price": "101.00",
            "provenance": _provenance("underlying", observed_at),
        },
        "benchmark": {
            "symbol": "SPY",
            "baseline_price": "100.00",
            "price": "100.50",
            "provenance": _provenance("benchmark", observed_at),
        },
        "option_market": {
            "iv_change": {
                "value": "-0.02",
                "provenance": _provenance("iv", observed_at),
            },
            "skew_change": {
                "value": None,
                "provenance": _unavailable("SKEW_HISTORY_UNAVAILABLE"),
            },
            "volume_change": {
                "value": "1200",
                "provenance": _provenance("volume", observed_at),
            },
        },
        "combination": {
            "estimated_pnl_usd": "12.50",
            "estimated_return": "0.09615384615384615384615384615",
            "max_loss_usd": "130.00",
            "provenance": _provenance("combination", observed_at),
        },
        "thesis_validity": {
            "status": "VALID" if prediction else "UNKNOWN",
            "valid": True if prediction else None,
            "reason_code": (
                "THESIS_STILL_VALID"
                if prediction
                else "THESIS_BINDING_UNAVAILABLE"
            ),
            "thesis_hash": target.get("thesis_hash") if prediction else None,
            "provenance": (
                _provenance("thesis", observed_at)
                if prediction
                else _unavailable("THESIS_BINDING_UNAVAILABLE")
            ),
        },
    }


def _processor_fixture(tmp_path: Path):
    clock = MutableClock(BASE + timedelta(seconds=1))
    shadow = ShadowLearningLedger(tmp_path / "shadow.sqlite3", clock=clock)
    prediction_ids = _seed_predictions(shadow)
    candidate = _candidate_target()
    observations: dict[tuple[str, str, str], Mapping[str, object]] = {}
    for prediction_id in prediction_ids:
        horizon = prediction_id.removeprefix("prediction-").upper()
        target = {
            "subject_kind": "PREDICTION",
            "subject_id": prediction_id,
            "subject_hash": shadow.get_prediction(prediction_id).content_hash,
            "thesis_hash": shadow.get_prediction(prediction_id).thesis_hash,
        }
        observations[("PREDICTION", prediction_id, horizon)] = _observation(
            target,
            horizon,
        )
    for horizon in OUTCOME_HORIZONS:
        observations[("CANDIDATE", str(candidate["subject_id"]), horizon)] = (
            _observation(candidate, horizon)
        )
    recorder = OutcomeRecorder(tmp_path / "candidate-outcomes.sqlite3", clock=clock)
    processor = ImmutableOutcomeProcessor(
        shadow_ledger=shadow,
        candidate_recorder=recorder,
        candidate_targets=lambda: (candidate,),
        observation_provider=FixtureObservationProvider(observations),
        clock=clock,
    )
    clock.now = BASE - timedelta(seconds=1)
    return clock, shadow, recorder, processor, observations


def test_durable_processor_is_bounded_and_resumes_without_rescanning(tmp_path: Path) -> None:
    clock, shadow, recorder, _, observations = _processor_fixture(tmp_path)
    candidate = _candidate_target()
    candidate["source_sequence"] = 1

    def targets(*, after_sequence: int = 0):
        return (candidate,) if after_sequence < 1 else ()

    progress_path = tmp_path / "outcome-progress.sqlite3"
    clock.now = BASE + timedelta(days=10)
    progress = OutcomeProgressStore(progress_path)
    try:
        first = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=targets,
            observation_provider=FixtureObservationProvider(observations),
            progress_store=progress,
            maximum_work_items=2,
            maximum_pending_items=20,
            clock=clock,
        ).process(deadline_at=clock.now + timedelta(seconds=30))
        assert first.bounded is True
        assert first.status == "WAITING_FOR_OBSERVATIONS"
        assert first.remaining_count == 5
        assert first.prediction_cursor == 4
        assert first.candidate_cursor == 1
        assert first.records_appended == 2
    finally:
        progress.close()

    restarted_progress = OutcomeProgressStore(progress_path)
    try:
        second = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=targets,
            observation_provider=FixtureObservationProvider(observations),
            progress_store=restarted_progress,
            maximum_work_items=2,
            maximum_pending_items=20,
            clock=clock,
        ).process(deadline_at=clock.now + timedelta(seconds=30))
        assert second.progress_sequence == 2
        assert second.prediction_cursor > first.prediction_cursor
        assert second.candidate_cursor == first.candidate_cursor
        assert second.remaining_count == 5
        assert second.records_appended == 2
    finally:
        restarted_progress.close()
        recorder.close()
        shadow.close()


def test_live_scale_batch_verifies_evidence_once_for_one_hundred_lookups(
    tmp_path: Path,
    monkeypatch,
) -> None:
    clock = MutableClock(BASE + timedelta(days=10))
    evidence = EvidenceStore(tmp_path / "batch-evidence.sqlite3")
    shadow = ShadowLearningLedger(tmp_path / "batch-shadow.sqlite3")
    recorder = OutcomeRecorder(tmp_path / "batch-outcomes.sqlite3")
    progress = OutcomeProgressStore(tmp_path / "batch-progress.sqlite3")
    targets = []
    for sequence in range(1, 21):
        target = _capture_target(f"candidate-batch-{sequence}")
        target["source_sequence"] = sequence
        targets.append(target)
    integrity_calls = 0
    original_assert_integrity = evidence.assert_integrity

    def counted_integrity() -> None:
        nonlocal integrity_calls
        integrity_calls += 1
        original_assert_integrity()

    monkeypatch.setattr(evidence, "assert_integrity", counted_integrity)
    try:
        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda **_: tuple(targets),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            progress_store=progress,
            maximum_work_items=100,
            maximum_pending_items=200,
            clock=clock,
        ).process(deadline_at=clock.now + timedelta(seconds=60))

        assert integrity_calls == 1
        assert result.horizons_requested == 100
        assert result.records_blocked == 100
        assert result.remaining_count == 100
        assert result.stopped is False
        assert progress.latest().sequence == 1
    finally:
        progress.close()
        recorder.close()
        shadow.close()
        evidence.close()


def test_verified_batch_is_released_after_unexpected_processor_exception(
    tmp_path: Path,
    monkeypatch,
) -> None:
    clock = MutableClock(BASE + timedelta(days=10))
    evidence = EvidenceStore(tmp_path / "batch-cleanup-evidence.sqlite3")
    shadow = ShadowLearningLedger(tmp_path / "batch-cleanup-shadow.sqlite3")
    recorder = OutcomeRecorder(tmp_path / "batch-cleanup-outcomes.sqlite3")
    provider = EvidenceStoreOutcomeObservationProvider(evidence)
    processor = ImmutableOutcomeProcessor(
        shadow_ledger=shadow,
        candidate_recorder=recorder,
        candidate_targets=lambda **_: (),
        observation_provider=provider,
        clock=clock,
    )

    def fail_ingest(**_kwargs):
        raise RuntimeError("forced unexpected processor failure")

    monkeypatch.setattr(processor, "_ingest_predictions", fail_ingest)
    try:
        with pytest.raises(
            RuntimeError,
            match="forced unexpected processor failure",
        ):
            processor.process(deadline_at=clock.now + timedelta(seconds=30))

        assert provider._batch_active is False
        assert provider._batch_as_of is None
        assert provider._batch_head_sequence == 0
        assert provider._terminal_capture_reasons == {}
    finally:
        recorder.close()
        shadow.close()
        evidence.close()


def test_verified_evidence_prefix_excludes_append_started_after_integrity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    evidence = EvidenceStore(tmp_path / "atomic-prefix.sqlite3")
    evidence.append(
        EvidenceRecord(
            identity="atomic-prefix:first",
            kind="TEST_ONLY",
            symbol="SPY",
            provider="TEST_ONLY",
            source_id="atomic-prefix:first",
            published_at=BASE,
            first_seen_at=BASE,
            ingested_at=BASE,
            observed_at=BASE,
            payload={"version": 1},
        )
    )
    original_assert_integrity = evidence.assert_integrity
    append_started = threading.Event()
    append_finished = threading.Event()

    def append_after_integrity() -> None:
        append_started.set()
        evidence.append(
            EvidenceRecord(
                identity="atomic-prefix:second",
                kind="TEST_ONLY",
                symbol="SPY",
                provider="TEST_ONLY",
                source_id="atomic-prefix:second",
                published_at=BASE,
                first_seen_at=BASE,
                ingested_at=BASE,
                observed_at=BASE,
                payload={"version": 2},
            )
        )
        append_finished.set()

    worker: threading.Thread | None = None

    def integrity_then_start_append() -> None:
        nonlocal worker
        original_assert_integrity()
        worker = threading.Thread(target=append_after_integrity)
        worker.start()
        assert append_started.wait(timeout=1)

    monkeypatch.setattr(evidence, "assert_integrity", integrity_then_start_append)
    try:
        head = evidence.verified_head_sequence(first_seen_at_or_before=BASE)
        assert append_finished.wait(timeout=2)
        assert worker is not None
        worker.join(timeout=2)
        assert head == 1
        assert len(evidence.query_page(at_or_before_sequence=head)) == 1
        assert len(evidence.query_page()) == 2
    finally:
        evidence.close()


def test_query_page_conflicts_are_frozen_to_sequence_and_time_prefix(
    tmp_path: Path,
) -> None:
    evidence = EvidenceStore(tmp_path / "prefix-conflict.sqlite3")
    try:
        first = evidence.append(
            EvidenceRecord(
                identity="prefix-conflict:identity",
                kind="TEST_ONLY",
                symbol="SPY",
                provider="TEST_ONLY",
                source_id="prefix-conflict:first",
                published_at=BASE,
                first_seen_at=BASE,
                ingested_at=BASE,
                observed_at=BASE,
                payload={"version": 1},
            )
        ).evidence
        evidence.append(
            EvidenceRecord(
                identity="prefix-conflict:identity",
                kind="TEST_ONLY",
                symbol="SPY",
                provider="TEST_ONLY",
                source_id="prefix-conflict:second",
                published_at=BASE,
                first_seen_at=BASE + timedelta(seconds=1),
                ingested_at=BASE + timedelta(seconds=1),
                observed_at=BASE + timedelta(seconds=1),
                payload={"version": 2},
            )
        )

        sequence_prefix = evidence.query_page(
            at_or_before_sequence=first.sequence,
        )
        time_prefix = evidence.query_page(
            first_seen_at_or_before=BASE,
        )
        current = evidence.query_page()

        assert len(sequence_prefix) == 1
        assert sequence_prefix[0].status == "ACTIVE"
        assert len(time_prefix) == 1
        assert time_prefix[0].status == "ACTIVE"
        assert len(current) == 2
        assert {row.status for row in current} == {"CONFLICTED"}
    finally:
        evidence.close()


def test_terminal_capture_is_not_requeued_across_processor_restart(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE)
    evidence = EvidenceStore(tmp_path / "terminal-evidence.sqlite3", clock=clock)
    shadow = ShadowLearningLedger(tmp_path / "terminal-shadow.sqlite3", clock=clock)
    recorder = OutcomeRecorder(tmp_path / "terminal-outcomes.sqlite3", clock=clock)
    progress_path = tmp_path / "terminal-progress.sqlite3"
    target = _capture_target("candidate-terminal")
    target["source_sequence"] = 1
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    registered = capture.register_target(target, horizon="30M")
    clock.now = BASE + timedelta(minutes=30, seconds=6)
    terminal = capture.tick(now=clock.now)
    assert registered.status == "REGISTERED"
    assert terminal.status == "BLOCKED"

    progress = OutcomeProgressStore(progress_path)
    try:
        first = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda **_: (target,),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            progress_store=progress,
            maximum_work_items=5,
            maximum_pending_items=10,
            clock=clock,
        ).process(deadline_at=clock.now + timedelta(seconds=30))
        assert "OUTCOME_CAPTURE_WINDOW_MISSED" in first.reason_codes
        assert first.records_blocked == 5
        assert first.remaining_count == 4
    finally:
        progress.close()

    restarted = OutcomeProgressStore(progress_path)
    try:
        second = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda **_: (),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            progress_store=restarted,
            maximum_work_items=5,
            maximum_pending_items=10,
            clock=clock,
        ).process(deadline_at=clock.now + timedelta(seconds=30))
        assert second.prediction_cursor == first.prediction_cursor
        assert second.candidate_cursor == first.candidate_cursor
        assert second.remaining_count == 4
        assert restarted.latest().sequence == 2
    finally:
        restarted.close()
        recorder.close()
        shadow.close()
        evidence.close()


@pytest.mark.parametrize(
    "tamper",
    (
        "provider",
        "source",
        "identity",
        "hash",
        "prior",
        "target",
        "symbol",
        "capture_key",
        "reason",
        "multiple_reasons",
    ),
)
def test_forged_terminal_capture_revision_is_never_indexed(
    tmp_path: Path,
    tamper: str,
) -> None:
    clock = MutableClock(BASE)
    evidence = EvidenceStore(tmp_path / f"forged-terminal-{tamper}.sqlite3")
    target = _capture_target(f"candidate-forged-terminal-{tamper}")
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    try:
        capture.register_target(target, horizon="30M")
        clock.now = BASE + timedelta(minutes=30, seconds=6)
        terminal = capture.tick(now=clock.now)
        assert terminal.status == "BLOCKED"
        rows = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
        previous = rows[-1]
        capture_key = str(previous.record.payload["capture_key"])
        body = dict(previous.record.payload)
        body["revision"] = 3
        body["prior_capture_spec_hash"] = previous.record.payload[
            "capture_spec_hash"
        ]
        body["status"] = "BLOCKED"
        body["reason_codes"] = ("OUTCOME_CAPTURE_WINDOW_MISSED",)
        if tamper == "prior":
            body["prior_capture_spec_hash"] = "e" * 64
        if tamper == "target":
            body["subject_id"] = "different-target"
        if tamper == "symbol":
            body["symbol"] = "QQQ"
        if tamper == "capture_key":
            body.pop("capture_key")
        if tamper == "reason":
            body["reason_codes"] = ("FORGED_TERMINAL_REASON",)
        if tamper == "multiple_reasons":
            body["reason_codes"] = (
                "OUTCOME_CAPTURE_WINDOW_MISSED",
                "OUTCOME_OBSERVATION_CONFLICTED",
            )
        body.pop("capture_spec_hash", None)
        body["capture_spec_hash"] = canonical_hash(body)
        identity = f"{capture_key}:r3"
        provider = "OPTIONS_COPILOT_CAPTURE"
        source_id = "capture-spec:" + canonical_hash(
            {
                "identity": identity,
                "capture_spec_hash": body["capture_spec_hash"],
            }
        )
        if tamper == "provider":
            provider = "TEST_ONLY"
        elif tamper == "source":
            source_id = "forged-terminal-source"
        elif tamper == "identity":
            identity = f"forged-terminal:{tamper}"
        elif tamper == "hash":
            body["capture_spec_hash"] = "f" * 64
        evidence.append(
            EvidenceRecord(
                identity=identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol=str(body["symbol"]),
                provider=provider,
                source_id=source_id,
                published_at=BASE,
                first_seen_at=clock.now,
                ingested_at=clock.now,
                observed_at=clock.now,
                payload=body,
                supersedes_id=previous.evidence_id,
            )
        )

        provider_adapter = EvidenceStoreOutcomeObservationProvider(evidence)
        provider_adapter.begin_verified_batch(as_of=clock.now)
        try:
            assert provider_adapter.terminal_reason(
                target,
                horizon="30M",
                as_of=clock.now,
            ) is None
        finally:
            provider_adapter.end_verified_batch()
    finally:
        evidence.close()


@pytest.mark.parametrize("tamper", ("premature", "thesis", "binding"))
def test_allowlisted_terminal_capture_requires_real_condition_and_stable_lineage(
    tmp_path: Path,
    tamper: str,
) -> None:
    evidence = EvidenceStore(tmp_path / f"allowlisted-terminal-{tamper}.sqlite3")
    target = _capture_target(f"candidate-allowlisted-terminal-{tamper}")
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    try:
        capture.register_target(target, horizon="30M")
        previous = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))[-1]
        capture_key = str(previous.record.payload["capture_key"])
        terminal_at = (
            BASE + timedelta(seconds=1)
            if tamper == "premature"
            else BASE + timedelta(minutes=30, seconds=6)
        )
        body = dict(previous.record.payload)
        body["revision"] = 2
        body["prior_capture_spec_hash"] = previous.record.payload[
            "capture_spec_hash"
        ]
        body["status"] = "BLOCKED"
        body["reason_codes"] = ("OUTCOME_CAPTURE_WINDOW_MISSED",)
        if tamper == "thesis":
            body["thesis_hash"] = "f" * 64
        if tamper == "binding":
            body["prediction_candidate_binding"] = {
                "schema": "options_copilot.forged_binding.v1"
            }
        body.pop("capture_spec_hash", None)
        body["capture_spec_hash"] = canonical_hash(body)
        identity = f"{capture_key}:r2"
        evidence.append(
            EvidenceRecord(
                identity=identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol=str(body["symbol"]),
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id="capture-spec:"
                + canonical_hash(
                    {
                        "identity": identity,
                        "capture_spec_hash": body["capture_spec_hash"],
                    }
                ),
                published_at=BASE,
                first_seen_at=terminal_at,
                ingested_at=terminal_at,
                observed_at=terminal_at,
                payload=body,
                supersedes_id=previous.evidence_id,
            )
        )

        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        provider.begin_verified_batch(as_of=terminal_at)
        try:
            assert provider.terminal_reason(
                target,
                horizon="30M",
                as_of=terminal_at,
            ) is None
        finally:
            provider.end_verified_batch()
    finally:
        evidence.close()


@pytest.mark.parametrize("tamper", ("root_plan", "root_horizon"))
def test_semantically_forged_root_capture_never_authorizes_terminal_reason(
    tmp_path: Path,
    tamper: str,
) -> None:
    evidence = EvidenceStore(tmp_path / f"forged-root-{tamper}.sqlite3")
    target = _capture_target(f"candidate-forged-root-{tamper}")
    capture_key = outcome_processor_module._capture_spec_identity(target, "30M")
    terminal_at = (
        BASE + timedelta(minutes=30, seconds=6)
        if tamper == "root_plan"
        else BASE + timedelta(seconds=6)
    )
    try:
        root = dict(
            outcome_processor_module._build_capture_spec(
                target,
                "30M",
                registered_at=BASE,
            )
        )
        if tamper == "root_plan":
            root["capture_plan"] = {
                "schema": "options_copilot.outcome_capture_plan.v1",
                "status": "WAITING",
                "reason_codes": ("FORGED_ROOT_PLAN",),
            }
        else:
            root["horizon_at"] = BASE
        root["capture_key"] = capture_key
        root["revision"] = 1
        root["prior_capture_spec_hash"] = None
        root.pop("capture_spec_hash", None)
        root["capture_spec_hash"] = canonical_hash(root)
        root_identity = f"{capture_key}:r1"
        stored_root = evidence.append(
            EvidenceRecord(
                identity=root_identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol=str(root["symbol"]),
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id="capture-spec:"
                + canonical_hash(
                    {
                        "identity": root_identity,
                        "capture_spec_hash": root["capture_spec_hash"],
                    }
                ),
                published_at=BASE,
                first_seen_at=BASE,
                ingested_at=BASE,
                observed_at=BASE,
                payload=root,
            )
        ).evidence

        terminal = dict(stored_root.record.payload)
        terminal["status"] = "BLOCKED"
        terminal["reason_codes"] = ("OUTCOME_CAPTURE_WINDOW_MISSED",)
        terminal["revision"] = 2
        terminal["prior_capture_spec_hash"] = stored_root.record.payload[
            "capture_spec_hash"
        ]
        terminal.pop("capture_spec_hash", None)
        terminal["capture_spec_hash"] = canonical_hash(terminal)
        terminal_identity = f"{capture_key}:r2"
        evidence.append(
            EvidenceRecord(
                identity=terminal_identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol=str(terminal["symbol"]),
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id="capture-spec:"
                + canonical_hash(
                    {
                        "identity": terminal_identity,
                        "capture_spec_hash": terminal["capture_spec_hash"],
                    }
                ),
                published_at=BASE,
                first_seen_at=terminal_at,
                ingested_at=terminal_at,
                observed_at=terminal_at,
                payload=terminal,
                supersedes_id=stored_root.evidence_id,
            )
        )

        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        provider.begin_verified_batch(as_of=terminal_at)
        try:
            assert provider.terminal_reason(
                target,
                horizon="30M",
                as_of=terminal_at,
            ) is None
        finally:
            provider.end_verified_batch()
    finally:
        evidence.close()


@pytest.mark.parametrize(
    "tamper",
    ("added_baseline", "added_binding", "changed_plan"),
)
def test_semantically_forged_dynamic_revision_never_authorizes_terminal_reason(
    tmp_path: Path,
    tamper: str,
) -> None:
    evidence = EvidenceStore(tmp_path / f"forged-dynamic-{tamper}.sqlite3")
    prediction = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": f"prediction-forged-dynamic-{tamper}",
        "subject_hash": "c" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "d" * 64,
        "predicted_direction": "BULLISH",
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "DIRECTION_ONLY",
            "reason_codes": (),
        },
    }

    def append_revision(
        previous: StoredEvidence,
        body: dict[str, object],
        *,
        recorded_at: datetime,
    ) -> StoredEvidence:
        revision = int(previous.record.payload["revision"]) + 1
        body["revision"] = revision
        body["prior_capture_spec_hash"] = previous.record.payload[
            "capture_spec_hash"
        ]
        body.pop("capture_spec_hash", None)
        body["capture_spec_hash"] = canonical_hash(body)
        capture_key = str(body["capture_key"])
        identity = f"{capture_key}:r{revision}"
        return evidence.append(
            EvidenceRecord(
                identity=identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol=str(body["symbol"]),
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id="capture-spec:"
                + canonical_hash(
                    {
                        "identity": identity,
                        "capture_spec_hash": body["capture_spec_hash"],
                    }
                ),
                published_at=BASE,
                first_seen_at=recorded_at,
                ingested_at=recorded_at,
                observed_at=recorded_at,
                payload=body,
                supersedes_id=previous.evidence_id,
            )
        ).evidence

    try:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=BulkOutcomeMarketAdapter(),
        )
        capture.register_target(prediction, horizon="30M", registered_at=BASE)
        rows = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
        root = rows[-1]
        if tamper == "added_baseline":
            previous = root
            forged = dict(previous.record.payload)
            forged["prediction_baseline"] = {
                "schema": "options_copilot.forged_prediction_baseline.v1",
                "status": "AVAILABLE",
            }
        else:
            capture.tick(now=BASE)
            previous = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))[-1]
            baseline = previous.record.payload["prediction_baseline"]
            assert isinstance(baseline, Mapping)
            forged = dict(previous.record.payload)
            forged["status"] = "READY"
            forged["reason_codes"] = ()
            if tamper == "added_binding":
                candidate_plan = _capture_target(
                    "candidate-forged-dynamic-binding"
                )["capture_plan"]
                assert isinstance(candidate_plan, dict)
                candidate_plan["underlying"] = baseline["underlying"]
                candidate_plan["benchmark"] = baseline["benchmark"]
                candidate_plan["benchmark_symbol"] = baseline[
                    "benchmark_symbol"
                ]
                forged["capture_plan"] = candidate_plan
                forged["prediction_candidate_binding"] = {
                    "schema": "options_copilot.forged_binding.v1",
                }
            else:
                forged["capture_plan"] = {
                    "schema": "options_copilot.outcome_capture_plan.v1",
                    "status": "DIRECTION_ONLY",
                    "reason_codes": (
                        "PREDICTION_CANDIDATE_NOT_POINT_IN_TIME",
                    ),
                    "forged_extra": True,
                }
        forged_row = append_revision(
            previous,
            forged,
            recorded_at=BASE + timedelta(seconds=1),
        )
        terminal_at = BASE + timedelta(minutes=30, seconds=6)
        terminal = dict(forged_row.record.payload)
        terminal["status"] = "BLOCKED"
        terminal["reason_codes"] = ("OUTCOME_CAPTURE_WINDOW_MISSED",)
        append_revision(forged_row, terminal, recorded_at=terminal_at)

        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        provider.begin_verified_batch(as_of=terminal_at)
        try:
            assert provider.terminal_reason(
                prediction,
                horizon="30M",
                as_of=terminal_at,
            ) is None
        finally:
            provider.end_verified_batch()
    finally:
        evidence.close()


@pytest.mark.parametrize(
    "reason",
    (
        "OUTCOME_CAPTURE_PLAN_INVALID",
        "OUTCOME_CAPTURE_LEGS_INVALID",
        "OUTCOME_CAPTURE_SPEC_INVALID",
        "OUTCOME_OBSERVATION_CONFLICTED",
    ),
)
def test_unproven_diagnostic_terminal_reason_has_no_processing_authority(
    tmp_path: Path,
    reason: str,
) -> None:
    evidence = EvidenceStore(tmp_path / f"unproven-terminal-{reason}.sqlite3")
    target = _capture_target(f"candidate-unproven-{reason}")
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    try:
        capture.register_target(target, horizon="30M")
        previous = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))[-1]
        capture_key = str(previous.record.payload["capture_key"])
        body = dict(previous.record.payload)
        body["revision"] = 2
        body["prior_capture_spec_hash"] = previous.record.payload[
            "capture_spec_hash"
        ]
        body["status"] = "BLOCKED"
        body["reason_codes"] = (reason,)
        body.pop("capture_spec_hash", None)
        body["capture_spec_hash"] = canonical_hash(body)
        identity = f"{capture_key}:r2"
        terminal_at = BASE + timedelta(minutes=30)
        evidence.append(
            EvidenceRecord(
                identity=identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol=str(body["symbol"]),
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id="capture-spec:"
                + canonical_hash(
                    {
                        "identity": identity,
                        "capture_spec_hash": body["capture_spec_hash"],
                    }
                ),
                published_at=BASE,
                first_seen_at=terminal_at,
                ingested_at=terminal_at,
                observed_at=terminal_at,
                payload=body,
                supersedes_id=previous.evidence_id,
            )
        )

        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        provider.begin_verified_batch(as_of=terminal_at)
        try:
            assert provider.terminal_reason(
                target,
                horizon="30M",
                as_of=terminal_at,
            ) is None
        finally:
            provider.end_verified_batch()
    finally:
        evidence.close()


def test_forged_ready_status_cannot_override_unchanged_waiting_plan(
    tmp_path: Path,
) -> None:
    evidence = EvidenceStore(tmp_path / "forged-ready-waiting-plan.sqlite3")
    prediction = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "prediction-forged-ready-waiting-plan",
        "subject_hash": "c" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "d" * 64,
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "WAITING",
            "reason_codes": (
                "OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE",
            ),
        },
    }
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    try:
        capture.register_target(prediction, horizon="30M", registered_at=BASE)
        root = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))[-1]
        capture_key = str(root.record.payload["capture_key"])
        forged = dict(root.record.payload)
        forged["revision"] = 2
        forged["prior_capture_spec_hash"] = root.record.payload[
            "capture_spec_hash"
        ]
        forged["status"] = "READY"
        forged["reason_codes"] = ()
        forged.pop("capture_spec_hash", None)
        forged["capture_spec_hash"] = canonical_hash(forged)
        forged_identity = f"{capture_key}:r2"
        forged_row = evidence.append(
            EvidenceRecord(
                identity=forged_identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol="SPY",
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id="capture-spec:"
                + canonical_hash(
                    {
                        "identity": forged_identity,
                        "capture_spec_hash": forged["capture_spec_hash"],
                    }
                ),
                published_at=BASE,
                first_seen_at=BASE + timedelta(seconds=1),
                ingested_at=BASE + timedelta(seconds=1),
                observed_at=BASE + timedelta(seconds=1),
                payload=forged,
                supersedes_id=root.evidence_id,
            )
        ).evidence

        terminal = dict(forged_row.record.payload)
        terminal["revision"] = 3
        terminal["prior_capture_spec_hash"] = forged_row.record.payload[
            "capture_spec_hash"
        ]
        terminal["status"] = "BLOCKED"
        terminal["reason_codes"] = ("OUTCOME_CAPTURE_WINDOW_MISSED",)
        terminal.pop("capture_spec_hash", None)
        terminal["capture_spec_hash"] = canonical_hash(terminal)
        terminal_identity = f"{capture_key}:r3"
        terminal_at = BASE + timedelta(minutes=30, seconds=6)
        evidence.append(
            EvidenceRecord(
                identity=terminal_identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol="SPY",
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id="capture-spec:"
                + canonical_hash(
                    {
                        "identity": terminal_identity,
                        "capture_spec_hash": terminal["capture_spec_hash"],
                    }
                ),
                published_at=BASE,
                first_seen_at=terminal_at,
                ingested_at=terminal_at,
                observed_at=terminal_at,
                payload=terminal,
                supersedes_id=forged_row.evidence_id,
            )
        )

        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        provider.begin_verified_batch(as_of=terminal_at)
        try:
            assert provider.terminal_reason(
                prediction,
                horizon="30M",
                as_of=terminal_at,
            ) is None
        finally:
            provider.end_verified_batch()
    finally:
        evidence.close()


def test_multi_key_capture_component_is_invalidated_without_row_duplication(
    tmp_path: Path,
) -> None:
    evidence = EvidenceStore(tmp_path / "multi-key-terminal-component.sqlite3")
    target = _capture_target("candidate-multi-key-terminal-component")
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    try:
        capture.register_target(target, horizon="30M")
        previous = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))[-1]
        for index in range(1, 33):
            body = dict(previous.record.payload)
            capture_key = "outcome-capture-spec:" + f"{index:064x}"
            body["capture_key"] = capture_key
            body["revision"] = index + 1
            body["prior_capture_spec_hash"] = previous.record.payload[
                "capture_spec_hash"
            ]
            body.pop("capture_spec_hash", None)
            body["capture_spec_hash"] = canonical_hash(body)
            identity = f"{capture_key}:r{index + 1}"
            previous = evidence.append(
                EvidenceRecord(
                    identity=identity,
                    kind=OUTCOME_CAPTURE_SPEC_KIND,
                    symbol=str(body["symbol"]),
                    provider="OPTIONS_COPILOT_CAPTURE",
                    source_id="capture-spec:"
                    + canonical_hash(
                        {
                            "identity": identity,
                            "capture_spec_hash": body["capture_spec_hash"],
                        }
                    ),
                    published_at=BASE,
                    first_seen_at=BASE + timedelta(seconds=index),
                    ingested_at=BASE + timedelta(seconds=index),
                    observed_at=BASE + timedelta(seconds=index),
                    payload=body,
                    supersedes_id=previous.evidence_id,
                )
            ).evidence

        rows = evidence.query_page(kinds=(OUTCOME_CAPTURE_SPEC_KIND,), limit=100)
        grouped = outcome_processor_module._capture_spec_rows_by_key(rows)

        assert len(grouped) == 33
        assert all(component is None for component in grouped.values())
    finally:
        evidence.close()


def test_terminal_prediction_backlog_advances_cursor_without_filling_pending(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE + timedelta(days=10))
    evidence = EvidenceStore(tmp_path / "terminal-backlog-evidence.sqlite3")
    shadow = ShadowLearningLedger(tmp_path / "terminal-backlog-shadow.sqlite3")
    recorder = OutcomeRecorder(tmp_path / "terminal-backlog-outcomes.sqlite3")
    progress = OutcomeProgressStore(tmp_path / "terminal-backlog-progress.sqlite3")
    prediction_ids = _seed_predictions(shadow)
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    projected = outcome_processor_module._project_prediction_targets(
        tuple(shadow.get_prediction(item) for item in prediction_ids)
    )
    assert len(projected) == len(prediction_ids)
    clock.now = max(
        outcome_processor_module._time_from(target["baseline_at"])
        for target, _horizon in projected
    ) + timedelta(days=10)
    for target, horizon in projected:
        capture.register_target(
            target,
            horizon=horizon,
            registered_at=outcome_processor_module._time_from(target["baseline_at"]),
        )
    terminal = capture.tick(now=clock.now)
    assert terminal.status == "BLOCKED"
    assert terminal.records_blocked == len(prediction_ids)
    try:
        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda **_: (),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            progress_store=progress,
            maximum_work_items=2,
            maximum_pending_items=20,
            clock=clock,
        ).process(deadline_at=clock.now + timedelta(seconds=30))

        assert result.prediction_cursor == max(
            shadow.get_prediction(item).sequence for item in prediction_ids
        )
        assert result.horizons_requested == len(prediction_ids)
        assert result.records_blocked == len(prediction_ids)
        assert result.remaining_count == 0
        assert result.bounded is False
        assert "OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED" in result.reason_codes
        assert progress.latest().sequence == 1
    finally:
        progress.close()
        recorder.close()
        shadow.close()
        evidence.close()


def test_deadline_during_batch_verification_leaves_restartable_zero_progress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    clock = MutableClock(BASE + timedelta(days=10))
    deadline = clock.now + timedelta(seconds=1)
    evidence = EvidenceStore(tmp_path / "batch-deadline-evidence.sqlite3")
    shadow = ShadowLearningLedger(tmp_path / "batch-deadline-shadow.sqlite3")
    recorder = OutcomeRecorder(tmp_path / "batch-deadline-outcomes.sqlite3")
    progress_path = tmp_path / "batch-deadline-progress.sqlite3"
    progress = OutcomeProgressStore(progress_path)
    target = _capture_target("candidate-batch-deadline")
    target["source_sequence"] = 1
    original_assert_integrity = evidence.assert_integrity

    def integrity_then_expire() -> None:
        original_assert_integrity()
        clock.now = deadline

    monkeypatch.setattr(evidence, "assert_integrity", integrity_then_expire)
    try:
        timed_out = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda **_: (target,),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            progress_store=progress,
            maximum_work_items=5,
            maximum_pending_items=10,
            clock=clock,
        ).process(deadline_at=deadline)
        assert timed_out.stopped is True
        assert timed_out.records_appended == 0
        assert timed_out.remaining_count == 0
        assert progress.latest().sequence == 0
    finally:
        progress.close()

    monkeypatch.setattr(evidence, "assert_integrity", original_assert_integrity)
    clock.now = deadline + timedelta(seconds=1)
    restarted = OutcomeProgressStore(progress_path)
    try:
        resumed = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda **_: (target,),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            progress_store=restarted,
            maximum_work_items=5,
            maximum_pending_items=10,
            clock=clock,
        ).process(deadline_at=clock.now + timedelta(seconds=30))
        assert resumed.stopped is False
        assert resumed.candidate_cursor == 1
        assert resumed.remaining_count == 5
        assert restarted.latest().sequence == 1
    finally:
        restarted.close()
        recorder.close()
        shadow.close()
        evidence.close()


def test_durable_processor_cooperatively_returns_at_deadline(tmp_path: Path) -> None:
    clock, shadow, recorder, _, observations = _processor_fixture(tmp_path)
    candidate = _candidate_target()
    candidate["source_sequence"] = 1
    progress = OutcomeProgressStore(tmp_path / "deadline-progress.sqlite3")
    clock.now = BASE + timedelta(days=10)
    try:
        started = time.monotonic()
        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda *, after_sequence=0: (
                (candidate,) if after_sequence < 1 else ()
            ),
            observation_provider=FixtureObservationProvider(observations),
            progress_store=progress,
            maximum_work_items=2,
            maximum_pending_items=20,
            clock=clock,
        ).process(deadline_at=clock.now, operation_token="scheduler-slot")
        assert time.monotonic() - started < 1
        assert result.status == "DEGRADED"
        assert "OUTCOME_PROCESSING_CANCELLED" in result.reason_codes
        assert result.stopped is True
        assert result.stop_reason == "OUTCOME_PROCESSING_CANCELLED"
        assert result.records_appended == 0
        assert result.remaining_count == 0
        assert result.progress_sequence == 0
        assert progress.latest().sequence == 0
    finally:
        progress.close()
        recorder.close()
        shadow.close()


def test_committed_outcome_crossing_deadline_stops_before_later_writes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    clock, shadow, recorder, processor, _ = _processor_fixture(tmp_path)
    progress = OutcomeProgressStore(tmp_path / "atomic-deadline-progress.sqlite3")
    evaluation = ShadowEvaluationStore(tmp_path / "atomic-deadline-evaluation.sqlite3")
    processor.progress_store = progress
    candidate = _candidate_target()
    candidate["source_sequence"] = 1
    processor.candidate_targets = lambda **_: (candidate,)
    deadline = _horizon_at("30M") + timedelta(seconds=3)
    clock.now = _horizon_at("30M") + timedelta(seconds=2)
    real_record = recorder.record

    def record_then_cross_deadline(*args, **kwargs):
        stored = real_record(*args, **kwargs)
        clock.now = deadline
        return stored

    monkeypatch.setattr(recorder, "record", record_then_cross_deadline)
    try:
        result = processor.process(deadline_at=deadline)
        assert result.stopped is True
        assert result.stop_reason == "OUTCOME_PROCESSING_CANCELLED"
        assert "OUTCOME_PROCESSING_CANCELLED" in result.reason_codes
        assert recorder.count() == 1
        assert progress.latest().sequence == 0

        evaluation.refresh(
            shadow,
            generated_at=clock.now,
            deadline_at=deadline,
            clock=lambda: clock.now,
        )
        count = evaluation._connection.execute(
            "SELECT COUNT(*) FROM shadow_evaluation_reports"
        ).fetchone()[0]
        assert count == 0
    finally:
        evaluation.close()
        progress.close()
        recorder.close()
        shadow.close()


def test_deadline_after_observation_requeues_item_without_any_durable_write(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, _, observations = _processor_fixture(tmp_path)
    candidate = _candidate_target()
    candidate["source_sequence"] = 1
    progress = OutcomeProgressStore(tmp_path / "post-observation-deadline.sqlite3")
    clock.now = _horizon_at("30M") + timedelta(seconds=2)
    deadline = clock.now + timedelta(seconds=1)

    class ExpiringProvider(FixtureObservationProvider):
        def observe(self, target, *, horizon, as_of):
            value = super().observe(target, horizon=horizon, as_of=as_of)
            clock.now = deadline
            return value

    try:
        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda *, after_sequence=0: (
                (candidate,) if after_sequence < 1 else ()
            ),
            observation_provider=ExpiringProvider(observations),
            progress_store=progress,
            maximum_work_items=2,
            maximum_pending_items=20,
            clock=clock,
        ).process(deadline_at=deadline)
        assert "OUTCOME_PROCESSING_CANCELLED" in result.reason_codes
        assert result.records_appended == 0
        assert recorder.row_count() == 0
        assert shadow.record_counts()["outcomes"] == 0
        assert progress.latest().sequence == 0
        assert result.remaining_count == 7
    finally:
        progress.close()
        recorder.close()
        shadow.close()


def test_expired_deadline_does_not_invoke_large_candidate_source(tmp_path: Path) -> None:
    clock, shadow, recorder, _, observations = _processor_fixture(tmp_path)
    progress = OutcomeProgressStore(tmp_path / "large-source-deadline.sqlite3")
    clock.now = BASE + timedelta(days=10)
    calls = 0

    def targets(*, after_sequence=0):
        nonlocal calls
        calls += 1
        return tuple(_candidate_target() for _ in range(10000))

    try:
        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=targets,
            observation_provider=FixtureObservationProvider(observations),
            progress_store=progress,
            maximum_work_items=2,
            maximum_pending_items=20,
            clock=clock,
        ).process(deadline_at=clock.now)
        assert "OUTCOME_PROCESSING_CANCELLED" in result.reason_codes
        assert calls == 0
        assert progress.latest().sequence == 0
    finally:
        progress.close()
        recorder.close()
        shadow.close()


def test_processor_records_each_prediction_and_candidate_only_after_horizon_matures(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, _ = _processor_fixture(tmp_path)
    try:
        before = processor.process()
        assert before.status == "WAITING_FOR_OBSERVATIONS"
        assert before.records_appended == 0
        assert recorder.count() == 0
        assert shadow.record_counts()["outcomes"] == 0

        clock.now = _horizon_at("30M") + timedelta(seconds=2)
        first = processor.process()
        assert first.status == "DEGRADED", first.as_dict()
        assert first.records_appended == 2
        assert {item.horizon for item in recorder.query()} == {"30M"}
        assert shadow.record_counts()["outcomes"] == 1
        assert shadow.independent_sample_count("challenger-v1") == 0

        clock.now = _horizon_at("5D") + timedelta(seconds=2)
        final = processor.process()
        assert final.status == "COMPLETED"
        assert final.records_appended == 8
        assert {item.horizon for item in recorder.query()} == set(OUTCOME_HORIZONS)
        assert shadow.record_counts()["outcomes"] == 5
        assert shadow.independent_sample_count("challenger-v1") == 1
        for stored in recorder.query():
            market = stored.body["market_outcome"]
            assert market["underlying"]["price"] == Decimal("101")
            assert market["underlying_return"] == Decimal("0.01")
            assert market["benchmark_return"] == Decimal("0.005")
            assert market["excess_return"] == Decimal("0.005")
            assert market["option_market"]["skew_change"]["provenance"]["status"] == "UNAVAILABLE"
            assert market["combination"]["estimated_pnl_usd"] == Decimal("12.5")
            assert market["thesis_validity"]["valid"] is None
            assert (
                market["thesis_validity"]["candidate_thesis_authority"]
                == "UNKNOWN"
            )
            assert market["decision_authority"] == "SUPPORTING_ONLY"
            assert market["affects_production_weights"] is False
            assert market["affects_eligibility"] is False
            assert market["affects_risk"] is False
            assert market["affects_ranking"] is False
        assert recorder.verify_integrity() is True
        assert shadow.verify_integrity() is True
    finally:
        recorder.close()
        shadow.close()


def test_candidate_outcome_persists_hash_bound_unavailable_management_results(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, _ = _processor_fixture(tmp_path)
    try:
        clock.now = _horizon_at("30M") + timedelta(seconds=2)
        result = processor.process()
        assert result.records_appended == 2, result.as_dict()

        stored = recorder.query()[0]
        management = stored.body["position_management_result"]
        counterfactual = stored.body["counterfactual_result"]
        assert management["status"] == "UNAVAILABLE"
        assert management["position_management_hash"] == "b" * 64
        assert management["realized_or_executable_pnl_usd"] is None
        assert management["reason_code"] == "POSITION_MANAGEMENT_EVIDENCE_UNAVAILABLE"
        assert counterfactual["status"] == "UNAVAILABLE"
        assert counterfactual["counterfactual_spec_hash"] == "c" * 64
        assert all(path["pnl_usd"] is None for path in counterfactual["paths"])
        assert stored.body["position_management_result_hash"] == canonical_hash(
            {key: value for key, value in management.items() if key != "result_hash"}
        )
        assert stored.body["counterfactual_result_hash"] == canonical_hash(
            {key: value for key, value in counterfactual.items() if key != "result_hash"}
        )
        assert stored.evaluation_eligible is False
        assert "POSITION_MANAGEMENT_RESULT_UNAVAILABLE" in stored.exclusion_reasons
        assert "COUNTERFACTUAL_RESULT_UNAVAILABLE" in stored.exclusion_reasons
    finally:
        recorder.close()
        shadow.close()


@pytest.mark.parametrize("tamper", ["binding", "result_hash"])
def test_candidate_outcome_rejects_tampered_management_result(
    tmp_path: Path,
    tamper: str,
) -> None:
    clock, shadow, recorder, processor, observations = _processor_fixture(tmp_path)
    try:
        key = ("CANDIDATE", "candidate-outcome-processor", "30M")
        raw = dict(observations[key])
        body = {
            "schema": "options_copilot.position_management_outcome.v1",
            "position_management_hash": "b" * 64,
            "status": "AVAILABLE",
            "recommended_action": "HOLD",
            "realized_or_executable_pnl_usd": "12.50",
        }
        document = {**body, "result_hash": canonical_hash(body)}
        if tamper == "binding":
            document["position_management_hash"] = "d" * 64
        else:
            document["result_hash"] = "e" * 64
        raw["position_management_result"] = document
        processor.observation_provider.observations[key] = raw  # type: ignore[attr-defined]

        clock.now = _horizon_at("30M") + timedelta(seconds=2)
        result = processor.process()
        assert result.records_rejected == 1, result.as_dict()
        assert "OUTCOME_MANAGEMENT_RESULT_INVALID" in result.reason_codes
        assert recorder.count() == 0
    finally:
        recorder.close()
        shadow.close()


def test_candidate_outcome_rejects_present_non_mapping_result(tmp_path: Path) -> None:
    clock, shadow, recorder, processor, observations = _processor_fixture(tmp_path)
    try:
        key = ("CANDIDATE", "candidate-outcome-processor", "30M")
        raw = dict(observations[key])
        raw["position_management_result"] = "self-certified"
        processor.observation_provider.observations[key] = raw  # type: ignore[attr-defined]
        clock.now = _horizon_at("30M") + timedelta(seconds=2)

        result = processor.process()

        assert result.records_rejected == 1
        assert "OUTCOME_MANAGEMENT_RESULT_INVALID" in result.reason_codes
        assert recorder.count() == 0
    finally:
        recorder.close()
        shadow.close()


@pytest.mark.parametrize("tamper", ("source", "contract", "cost", "max_loss"))
def test_available_result_rejects_self_consistent_fabricated_authority(
    tamper: str,
) -> None:
    candidate_hash = str(_candidate_target()["subject_hash"])
    authority = _result_authority(candidate_hash)
    observed_at = _horizon_at("30M") + timedelta(seconds=1)
    quote_body: dict[str, object] = {
        "contract_id": "SPY-TEST-LONG",
        "side": "BUY",
        "quantity": 1,
        "bid": Decimal("2.00"),
        "ask": Decimal("2.10"),
        "source": "TEST_ONLY",
        "source_content_hash": "9" * 64,
        "observed_at": observed_at,
    }
    if tamper == "source":
        quote_body["source"] = "FABRICATED"
        quote_body["source_content_hash"] = "f" * 64
    elif tamper == "contract":
        quote_body["contract_id"] = "SPY-FABRICATED-LONG"
    quote = {
        **quote_body,
        "quote_identity_hash": canonical_hash(
            {
                "schema": "options_copilot.outcome_executable_quote.v1",
                "candidate_hash": candidate_hash,
                **quote_body,
            }
        ),
    }
    leg_quotes = (quote,)
    costs = Decimal("3.00") if tamper == "cost" else Decimal("2.50")
    maximum_loss = (
        Decimal("101.00") if tamper == "max_loss" else Decimal("100.00")
    )
    pnl = Decimal("200.00") - Decimal("185.00") - costs
    costs_hash = (
        canonical_hash(
            {
                "schema": "options_copilot.outcome_cost_evidence.v1",
                "candidate_hash": candidate_hash,
                "cost_contract_hash": EXECUTION_COST_HASH,
                "costs_usd": costs,
            }
        )
        if tamper == "cost"
        else authority["costs_hash"]
    )
    max_loss_hash = (
        canonical_hash(
            {
                "schema": "options_copilot.outcome_max_loss_evidence.v1",
                "candidate_hash": candidate_hash,
                "max_loss_usd": maximum_loss,
            }
        )
        if tamper == "max_loss"
        else authority["max_loss_evidence_hash"]
    )
    body = {
        "schema": "options_copilot.position_management_outcome.v1",
        "position_management_hash": "b" * 64,
        "status": "AVAILABLE",
        "recommended_action": "HOLD",
        "thesis_invalidation_hit": False,
        "risk_stop_hit": False,
        "profit_take_hit": False,
        "time_stop_hit": False,
        "realized_or_executable_pnl_usd": pnl,
        "entry_value_usd": Decimal("185.00"),
        "costs_usd": costs,
        "max_loss_usd": maximum_loss,
        "economic_observed_at": observed_at,
        "leg_quotes": leg_quotes,
        "reason_code": None,
        "provenance": {
            "status": "EVIDENCE_BOUND",
            "quote_batch_hash": canonical_hash(leg_quotes),
            "costs_hash": costs_hash,
            "max_loss_evidence_hash": max_loss_hash,
        },
    }
    document = {**body, "result_hash": canonical_hash(body)}

    with pytest.raises(OutcomeValidationError):
        normalize_bound_outcome_result(
            document,
            schema="options_copilot.position_management_outcome.v1",
            binding_field="position_management_hash",
            binding_hash="b" * 64,
            candidate_hash=candidate_hash,
            authority=authority,
            ledger_binding={"provider": "TEST_ONLY", "content_hash": "9" * 64},
        )


def test_candidate_outcome_accepts_matching_available_result_documents(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, observations = _processor_fixture(tmp_path)
    try:
        key = ("CANDIDATE", "candidate-outcome-processor", "30M")
        raw = dict(observations[key])
        observed_at = raw["economic_observed_at"]
        assert isinstance(observed_at, datetime)
        candidate_hash = str(_candidate_target()["subject_hash"])
        leg_quotes = (
            _result_quote(
                candidate_hash,
                observed_at,
                bid=Decimal("2.00"),
                ask=Decimal("2.10"),
            ),
        )
        costs = Decimal("2.50")
        maximum_loss = Decimal("100.00")
        entry_value = Decimal("185.00")
        pnl = Decimal("12.50")
        management_body = {
            "schema": "options_copilot.position_management_outcome.v1",
            "position_management_hash": "b" * 64,
            "status": "AVAILABLE",
            "recommended_action": "HOLD",
            "thesis_invalidation_hit": False,
            "risk_stop_hit": False,
            "profit_take_hit": False,
            "time_stop_hit": False,
            "realized_or_executable_pnl_usd": pnl,
            "entry_value_usd": entry_value,
            "costs_usd": costs,
            "max_loss_usd": maximum_loss,
            "economic_observed_at": observed_at,
            "leg_quotes": leg_quotes,
            "reason_code": None,
            "provenance": {
                "status": "EVIDENCE_BOUND",
                "quote_batch_hash": canonical_hash(leg_quotes),
                "costs_hash": _result_authority(candidate_hash)["costs_hash"],
                "max_loss_evidence_hash": _result_authority(candidate_hash)[
                    "max_loss_evidence_hash"
                ],
            },
        }
        def path_body(path: str, path_pnl: Decimal) -> dict[str, object]:
            path_bid = (entry_value + costs + path_pnl) / Decimal("100")
            path_quotes = (
                _result_quote(
                    candidate_hash,
                    observed_at,
                    bid=path_bid,
                    ask=path_bid + Decimal("0.10"),
                ),
            )
            return {
                "path": path,
                "status": "AVAILABLE",
                "pnl_usd": path_pnl,
                "entry_value_usd": entry_value,
                "costs_usd": costs,
                "max_loss_usd": maximum_loss,
                "economic_observed_at": observed_at,
                "leg_quotes": path_quotes,
                "reason_code": None,
            }
        paths = (
            path_body("FOLLOW_EXIT_POLICY", Decimal("12.50")),
            path_body("HOLD_TO_HORIZON", Decimal("8.00")),
        )
        counterfactual_body = {
            "schema": "options_copilot.outcome_counterfactual_result.v1",
            "counterfactual_spec_hash": "c" * 64,
            "status": "AVAILABLE",
            "paths": paths,
            "provenance": {
                "status": "EVIDENCE_BOUND",
                "result_set_hash": canonical_hash(paths),
            },
        }
        raw["position_management_result"] = {
            **management_body,
            "result_hash": canonical_hash(management_body),
        }
        raw["counterfactual_result"] = {
            **counterfactual_body,
            "result_hash": canonical_hash(counterfactual_body),
        }
        processor.observation_provider.observations[key] = raw  # type: ignore[attr-defined]

        clock.now = _horizon_at("30M") + timedelta(seconds=2)
        result = processor.process()
        assert result.records_rejected == 0, result.as_dict()
        stored = recorder.query()[0]
        assert stored.body["position_management_result"]["status"] == "AVAILABLE"
        assert stored.body["counterfactual_result"]["status"] == "AVAILABLE"
        assert "POSITION_MANAGEMENT_RESULT_UNAVAILABLE" not in stored.exclusion_reasons
        assert "COUNTERFACTUAL_RESULT_UNAVAILABLE" not in stored.exclusion_reasons
    finally:
        recorder.close()
        shadow.close()


@pytest.mark.parametrize(
    "mutation",
    (
        lambda body: body.update({"recommended_action": "MAGIC"}),
        lambda body: body.update({"realized_or_executable_pnl_usd": "NaN"}),
        lambda body: body.update({"unknown_economics": "1"}),
        lambda body: body.update({"status": "UNAVAILABLE"}),
    ),
)
def test_candidate_outcome_rejects_hash_valid_semantic_management_corruption(
    tmp_path: Path,
    mutation,
) -> None:
    clock, shadow, recorder, processor, observations = _processor_fixture(tmp_path)
    try:
        key = ("CANDIDATE", "candidate-outcome-processor", "30M")
        raw = dict(observations[key])
        body = {
            "schema": "options_copilot.position_management_outcome.v1",
            "position_management_hash": "b" * 64,
            "status": "AVAILABLE",
            "recommended_action": "HOLD",
            "realized_or_executable_pnl_usd": "12.50",
        }
        mutation(body)
        raw["position_management_result"] = {
            **body,
            "result_hash": canonical_hash(body),
        }
        processor.observation_provider.observations[key] = raw  # type: ignore[attr-defined]
        clock.now = _horizon_at("30M") + timedelta(seconds=2)
        result = processor.process()
        assert result.records_rejected == 1, result.as_dict()
        assert "OUTCOME_MANAGEMENT_RESULT_INVALID" in result.reason_codes
        assert recorder.count() == 0
    finally:
        recorder.close()
        shadow.close()


def test_hostile_or_stale_observation_fails_closed_without_authority_leakage(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, observations = _processor_fixture(tmp_path)
    try:
        clock.now = _horizon_at("5D") + timedelta(seconds=2)
        candidate_key = (
            "CANDIDATE",
            "candidate-outcome-processor",
            "30M",
        )
        hostile = dict(observations[candidate_key])
        hostile["production_weights"] = {"supporting_news": "1.0"}
        observations[candidate_key] = hostile
        processor.observation_provider.observations[candidate_key] = hostile  # type: ignore[attr-defined]

        stale_key = (
            "PREDICTION",
            "prediction-session_close",
            "SESSION_CLOSE",
        )
        stale = dict(observations[stale_key])
        stale["economic_observed_at"] = _horizon_at("SESSION_CLOSE") + timedelta(
            seconds=6
        )
        processor.observation_provider.observations[stale_key] = stale  # type: ignore[attr-defined]

        result = processor.process()
        assert result.status == "DEGRADED"
        assert result.records_rejected == 2, result.as_dict()
        assert "OUTCOME_OBSERVATION_FIELD_UNKNOWN" in result.reason_codes
        assert "OUTCOME_OBSERVATION_STALE" in result.reason_codes
        assert recorder.count() == 4
        assert shadow.record_counts()["outcomes"] == 4
        assert shadow.independent_sample_count("challenger-v1") == 0
        assert all(
            "production_weights" not in item.body["market_outcome"]
            for item in recorder.query()
        )
    finally:
        recorder.close()
        shadow.close()


def test_candidate_revision_appends_superseding_version_without_mutating_v1(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, _ = _processor_fixture(tmp_path)
    try:
        clock.now = _horizon_at("30M") + timedelta(seconds=2)
        first = processor.process()
        assert first.records_appended == 2, first.as_dict()
        original = recorder.query()[0]

        key = ("CANDIDATE", "candidate-outcome-processor", "30M")
        revised = dict(processor.observation_provider.observations[key])  # type: ignore[attr-defined]
        revised_underlying = dict(revised["underlying"])  # type: ignore[arg-type]
        revised_underlying["price"] = "102.00"
        revised["underlying"] = revised_underlying
        revised["revision_received_at"] = clock.now + timedelta(minutes=1)
        processor.observation_provider.observations[key] = revised  # type: ignore[attr-defined]
        clock.now = clock.now + timedelta(minutes=1)

        second = processor.process()
        assert second.records_superseded == 1
        history = recorder.history(original.base_identity_hash)
        assert len(history) == 2
        assert history[0].content_hash == original.content_hash
        assert history[1].supersedes_outcome_id == history[0].outcome_id
        assert history[1].supersedes_hash == history[0].content_hash
        assert history[0].body["market_outcome"]["underlying"]["price"] == Decimal(
            "101"
        )
        assert history[1].body["market_outcome"]["underlying"]["price"] == Decimal(
            "102"
        )
    finally:
        recorder.close()
        shadow.close()


def test_restart_is_idempotent_and_recovers_existing_partial_progress(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, observations = _processor_fixture(tmp_path)
    shadow_path = shadow.path
    recorder_path = recorder.path
    reopened_shadow: ShadowLearningLedger | None = None
    reopened_recorder: OutcomeRecorder | None = None
    try:
        clock.now = _horizon_at("5D") + timedelta(seconds=2)
        first = processor.process()
        assert first.records_appended == 10
        manifest_hash = first.manifest_hash
        shadow.close()
        recorder.close()

        reopened_shadow = ShadowLearningLedger(shadow_path, clock=clock)
        reopened_recorder = OutcomeRecorder(recorder_path, clock=clock)
        restarted = ImmutableOutcomeProcessor(
            shadow_ledger=reopened_shadow,
            candidate_recorder=reopened_recorder,
            candidate_targets=lambda: (_candidate_target(),),
            observation_provider=FixtureObservationProvider(observations),
            clock=clock,
        ).process()

        assert restarted.status == "COMPLETED"
        assert restarted.records_appended == 0
        assert restarted.records_superseded == 0
        assert restarted.records_skipped == 10
        assert restarted.manifest_hash == manifest_hash
        assert reopened_shadow.record_counts()["outcomes"] == 5
        assert reopened_recorder.count() == 5
        assert reopened_shadow.verify_integrity() is True
        assert reopened_recorder.verify_integrity() is True
    finally:
        if reopened_recorder is not None:
            reopened_recorder.close()
        if reopened_shadow is not None:
            reopened_shadow.close()
        recorder.close()
        shadow.close()


def test_partial_candidate_write_recovers_without_duplicate_prediction(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, _ = _processor_fixture(tmp_path)
    original_record = recorder.record
    interrupted = True

    def fail_once(document: Mapping[str, object]):
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise ValueError("CANDIDATE_WRITE_INTERRUPTED")
        return original_record(document)

    recorder.record = fail_once  # type: ignore[method-assign]
    try:
        clock.now = _horizon_at("30M") + timedelta(seconds=2)
        first = processor.process()
        assert first.status == "DEGRADED"
        assert first.records_appended == 1
        assert first.records_rejected == 1
        assert shadow.record_counts()["outcomes"] == 1
        assert recorder.count() == 0

        second = processor.process()
        assert second.status == "DEGRADED"
        assert second.records_appended == 1
        assert second.records_skipped == 1
        assert shadow.record_counts()["outcomes"] == 1
        assert recorder.count() == 1
    finally:
        recorder.close()
        shadow.close()


def test_legacy_news_prediction_is_excluded_from_scheduled_processor(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE + timedelta(hours=1))
    shadow = ShadowLearningLedger(tmp_path / "legacy-shadow.sqlite3", clock=clock)
    recorder = OutcomeRecorder(tmp_path / "legacy-outcomes.sqlite3", clock=clock)
    thesis = shadow.record_thesis(
        "legacy-thesis",
        champion_version="champion-v1",
        challenger_version="deepseek-news-advisory-v1",
        thesis={"purpose": "legacy exclusion regression"},
        created_at=BASE - timedelta(hours=1),
    )
    evidence = shadow.record_evidence(
        "legacy-evidence",
        thesis.thesis_id,
        source="TEST_ONLY",
        evidence={"symbol": "SPY"},
        published_at=BASE - timedelta(minutes=2),
        first_seen_at=BASE - timedelta(minutes=1),
    )
    shadow.record_prediction(
        "legacy-advisory:30m",
        thesis.thesis_id,
        evidence_ids=(evidence.evidence_id,),
        prediction={
            "schema": "options_copilot.news_shadow_prediction.v1",
            "symbol": "SPY",
            "horizon": "30M",
            "target_rule": OUTCOME_TARGET_RULES["30M"],
        },
        predicted_at=BASE,
        independence_key="legacy-event",
    )

    class RejectObservation:
        def observe(self, *args, **kwargs):
            raise AssertionError("legacy prediction must not request an observation")

    try:
        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda: (),
            observation_provider=RejectObservation(),
            clock=clock,
        ).process()

        assert result.records_rejected == 1
        assert "LEGACY_V1_CONTRACT_EXCLUDED" in result.reason_codes
        assert shadow.record_counts()["outcomes"] == 0
    finally:
        recorder.close()
        shadow.close()


def test_outcomes_cannot_mutate_research_priority_or_production_authority(
    tmp_path: Path,
) -> None:
    clock, shadow, recorder, processor, _ = _processor_fixture(tmp_path)
    target_hash_before = canonical_hash(_candidate_target())
    prediction_before = tuple(
        (
            replay.prediction.content_hash,
            replay.prediction.prediction["research_priority_score"],
        )
        for replay in shadow.query_replays(limit=100)
    )
    try:
        clock.now = _horizon_at("5D") + timedelta(seconds=2)
        result = processor.process()
        prediction_after = tuple(
            (
                replay.prediction.content_hash,
                replay.prediction.prediction["research_priority_score"],
            )
            for replay in shadow.query_replays(limit=100)
        )

        assert prediction_after == prediction_before
        assert canonical_hash(_candidate_target()) == target_hash_before
        assert result.decision_authority == "SUPPORTING_ONLY"
        assert result.affects_production_weights is False
        assert result.affects_eligibility is False
        assert result.affects_risk is False
        assert result.affects_ranking is False
        assert result.approval_eligible is False
        assert result.instruction_creation_allowed is False
        assert result.order_allowed is False
        assert all(
            outcome.body["market_outcome"][field] is False
            for outcome in recorder.query()
            for field in (
                "affects_production_weights",
                "affects_eligibility",
                "affects_risk",
                "affects_ranking",
            )
        )
    finally:
        recorder.close()
        shadow.close()


def test_evidence_store_provider_accepts_one_identity_and_rejects_conflicts(
    tmp_path: Path,
) -> None:
    target = _candidate_target()
    horizon = "30M"
    payload = _observation(target, horizon)
    payload.pop("ledger_binding")
    observed_at = payload["revision_received_at"]
    assert isinstance(observed_at, datetime)
    with EvidenceStore(tmp_path / "evidence.sqlite3") as evidence:
        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        identity = provider.identity(target, horizon)
        record = EvidenceRecord(
            identity=identity,
            kind=OUTCOME_OBSERVATION_KIND,
            symbol="SPY",
            provider="TEST_ONLY",
            source_id="outcome-30m",
            published_at=observed_at,
            first_seen_at=observed_at,
            ingested_at=observed_at,
            observed_at=observed_at,
            payload=payload,
        )
        first = evidence.append(record)
        duplicate = evidence.append(record)

        assert first.inserted is True
        assert duplicate.inserted is False
        loaded = provider.observe(
            target,
            horizon=horizon,
            as_of=observed_at,
        )
        assert loaded is not None
        assert loaded["ledger_binding"]["status"] == "ACTIVE"  # type: ignore[index]

        conflict_payload = dict(payload)
        conflict_underlying = dict(conflict_payload["underlying"])  # type: ignore[arg-type]
        conflict_underlying["price"] = "102.00"
        conflict_payload["underlying"] = conflict_underlying
        evidence.append(
            EvidenceRecord(
                identity=identity,
                kind=OUTCOME_OBSERVATION_KIND,
                symbol="SPY",
                provider="TEST_ONLY",
                source_id="outcome-30m-conflict",
                published_at=observed_at,
                first_seen_at=observed_at,
                ingested_at=observed_at,
                observed_at=observed_at,
                payload=conflict_payload,
            )
        )
        with pytest.raises(
            OutcomeObservationUnavailable,
            match="OUTCOME_OBSERVATION_CONFLICTED",
        ):
            provider.observe(target, horizon=horizon, as_of=observed_at)


def test_verified_observation_batch_does_not_backfill_later_append(
    tmp_path: Path,
) -> None:
    target = _candidate_target()
    horizon = "30M"
    payload = _observation(target, horizon)
    payload.pop("ledger_binding")
    observed_at = payload["revision_received_at"]
    assert isinstance(observed_at, datetime)
    with EvidenceStore(tmp_path / "point-in-time-evidence.sqlite3") as evidence:
        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        provider.begin_verified_batch(as_of=observed_at)
        evidence.append(
            EvidenceRecord(
                identity=provider.identity(target, horizon),
                kind=OUTCOME_OBSERVATION_KIND,
                symbol="SPY",
                provider="TEST_ONLY",
                source_id="later-append",
                published_at=observed_at,
                first_seen_at=observed_at,
                ingested_at=observed_at,
                observed_at=observed_at,
                payload=payload,
            )
        )
        assert provider.observe(
            target,
            horizon=horizon,
            as_of=observed_at,
        ) is None
        provider.end_verified_batch()
        assert provider.observe(
            target,
            horizon=horizon,
            as_of=observed_at,
        ) is not None


def test_tampered_observation_store_blocks_processing_with_visible_reason(
    tmp_path: Path,
) -> None:
    target = _candidate_target()
    horizon = "30M"
    payload = _observation(target, horizon)
    payload.pop("ledger_binding")
    observed_at = payload["revision_received_at"]
    assert isinstance(observed_at, datetime)
    evidence = EvidenceStore(tmp_path / "tampered-evidence.sqlite3")
    shadow = ShadowLearningLedger(tmp_path / "tampered-shadow.sqlite3")
    recorder = OutcomeRecorder(tmp_path / "tampered-outcomes.sqlite3")
    try:
        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        evidence.append(
            EvidenceRecord(
                identity=provider.identity(target, horizon),
                kind=OUTCOME_OBSERVATION_KIND,
                symbol="SPY",
                provider="TEST_ONLY",
                source_id="tamper-target",
                published_at=observed_at,
                first_seen_at=observed_at,
                ingested_at=observed_at,
                observed_at=observed_at,
                payload=payload,
            )
        )
        evidence._connection.execute(  # type: ignore[attr-defined]
            "DROP TRIGGER evidence_records_no_update"
        )
        evidence._connection.execute(  # type: ignore[attr-defined]
            "UPDATE evidence_records SET content_hash=? WHERE sequence=1",
            ("f" * 64,),
        )

        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda: (target,),
            observation_provider=provider,
            clock=lambda: _horizon_at("5D") + timedelta(seconds=2),
        ).process()

        assert result.status == "BLOCKED"
        assert result.records_appended == 0
        assert result.records_superseded == 0
        assert result.records_blocked == len(OUTCOME_HORIZONS)
        assert "OUTCOME_OBSERVATION_STORE_CORRUPT" in result.reason_codes
        assert recorder.count() == 0
    finally:
        recorder.close()
        shadow.close()
        evidence.close()


def test_processor_pages_beyond_five_thousand_predictions(
    tmp_path: Path,
) -> None:
    seed = ShadowLearningLedger(tmp_path / "page-seed.sqlite3")
    fake: ShadowLearningLedger | None = None
    recorder = OutcomeRecorder(tmp_path / "page-outcomes.sqlite3")
    try:
        _seed_predictions(seed)
        template = seed.query_replays(limit=1)[0]
        replays = []
        for sequence in range(1, 5002):
            prediction_id = f"prediction-page-{sequence:05d}"
            prediction = replace(
                template.prediction,
                sequence=sequence,
                prediction_id=prediction_id,
                content_hash=canonical_hash(
                    {"prediction_id": prediction_id, "sequence": sequence}
                ),
            )
            replays.append(
                replace(
                    template,
                    prediction=prediction,
                    outcome=(object() if sequence <= 5000 else None),  # type: ignore[arg-type]
                )
            )

        class PagedShadowLedger(ShadowLearningLedger):
            def __init__(self, path: Path) -> None:
                super().__init__(path)
                self.resolved: list[str] = []

            def query_replays(
                self,
                *,
                challenger_version: str | None = None,
                as_of: datetime | None = None,
                resolved_only: bool = False,
                limit: int = 500,
                after_sequence: int = 0,
            ):
                del challenger_version, as_of, resolved_only
                return tuple(
                    item
                    for item in replays
                    if item.prediction.sequence > after_sequence
                )[:limit]

            def resolve_outcome(self, prediction_id: str, **_: object):
                self.resolved.append(prediction_id)
                return None

        fake = PagedShadowLedger(tmp_path / "paged-shadow.sqlite3")
        final = replays[-1].prediction
        target = {
            "schema": "options_copilot.outcome_target.v1",
            "subject_kind": "PREDICTION",
            "subject_id": final.prediction_id,
            "subject_hash": final.content_hash,
            "symbol": "SPY",
            "occurred_at": final.predicted_at,
            "thesis_hash": final.thesis_hash,
        }
        observation = _observation(target, "30M")
        processor = ImmutableOutcomeProcessor(
            shadow_ledger=fake,
            candidate_recorder=recorder,
            candidate_targets=lambda: (),
            observation_provider=FixtureObservationProvider(
                {("PREDICTION", final.prediction_id, "30M"): observation}
            ),
            clock=lambda: _horizon_at("30M") + timedelta(seconds=2),
        )

        result = processor.process()

        assert fake.resolved == [final.prediction_id]
        assert result.subjects_seen == 5001
        assert result.horizons_requested == 5001
        assert "PREDICTION_TARGET_LIMIT_REACHED" not in result.reason_codes
    finally:
        recorder.close()
        if fake is not None:
            fake.close()
        seed.close()


def test_exact_horizon_capture_generates_observation_then_candidate_outcome(
    tmp_path: Path,
) -> None:
    target = _capture_target()
    due_at = BASE + timedelta(minutes=30)
    adapter = FakeOutcomeMarketAdapter()
    evidence = EvidenceStore(tmp_path / "capture-evidence.sqlite3")
    shadow = ShadowLearningLedger(tmp_path / "capture-shadow.sqlite3")
    recorder = OutcomeRecorder(tmp_path / "capture-outcomes.sqlite3")
    try:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        registered = capture.register_target(target, horizon="30M")
        captured = capture.tick(now=due_at + timedelta(seconds=1))

        assert registered.status == "REGISTERED"
        assert captured.status == "COMPLETED"
        assert captured.specs_due == 1
        assert captured.observations_appended == 1
        assert len(adapter.calls) == 1
        assert len(adapter.calls[0]) == 1
        rows = evidence.query(kinds=(OUTCOME_OBSERVATION_KIND,), limit=10)
        assert len(rows) == 1

        processed = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda: (target,),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            clock=lambda: due_at + timedelta(seconds=2),
        ).process()

        assert processed.status == "DEGRADED"
        assert processed.records_appended == 1
        assert processed.records_blocked == 4
        assert recorder.count() == 1
        market = recorder.query()[0].body["market_outcome"]
        assert market["underlying_return"] == Decimal("0.01")
        assert market["benchmark_return"] == Decimal("0.01")
        assert market["excess_return"] == Decimal("0")
        assert market["combination"]["estimated_pnl_usd"] == Decimal("15")
    finally:
        recorder.close()
        shadow.close()
        evidence.close()


def test_exact_horizon_capture_batches_due_specs_and_never_backfills_late_source(
    tmp_path: Path,
) -> None:
    due_at = BASE + timedelta(minutes=30)
    adapter = FakeOutcomeMarketAdapter()
    evidence = EvidenceStore(tmp_path / "capture-batch.sqlite3")
    try:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        capture.register_target(_capture_target("candidate-batch-1"), horizon="30M")
        capture.register_target(_capture_target("candidate-batch-2"), horizon="30M")

        late = capture.tick(now=due_at + timedelta(seconds=6))

        assert late.status == "BLOCKED"
        assert late.specs_due == 2
        assert late.observations_appended == 0
        assert late.records_blocked == 2
        assert "OUTCOME_CAPTURE_WINDOW_MISSED" in late.reason_codes
        assert adapter.calls == []
        assert evidence.query(kinds=(OUTCOME_OBSERVATION_KIND,), limit=10) == ()
    finally:
        evidence.close()


def test_durable_capture_counts_do_not_wait_for_background_capture_lock(
    tmp_path: Path,
) -> None:
    evidence = EvidenceStore(tmp_path / "capture-count-read-model.sqlite3")
    capture = ExactHorizonOutcomeCapture(
        evidence_store=evidence,
        market_adapter=FakeOutcomeMarketAdapter(),
    )
    lock_acquired = threading.Event()
    release_lock = threading.Event()

    def hold_capture_lock() -> None:
        with capture._lock:  # noqa: SLF001 - concurrency regression boundary
            lock_acquired.set()
            assert release_lock.wait(timeout=5)

    holder = threading.Thread(target=hold_capture_lock)
    try:
        registered = capture.register_target(
            _capture_target("durable-count-read-model"),
            horizon="30M",
        )
        assert registered.status == "REGISTERED"
        expected = capture.durable_counts()
        holder.start()
        assert lock_acquired.wait(timeout=5)

        started = time.perf_counter()
        observed = capture.durable_counts()
        elapsed = time.perf_counter() - started

        assert observed == expected
        assert observed["durable_status_counts"] == {"READY": 1}
        assert elapsed < 0.2
    finally:
        release_lock.set()
        holder.join(timeout=5)
        evidence.close()


def test_exact_horizon_capture_is_restart_idempotent_and_batches_once(
    tmp_path: Path,
) -> None:
    path = tmp_path / "capture-restart.sqlite3"
    due_at = BASE + timedelta(minutes=30)
    target_one = _capture_target("candidate-restart-1")
    target_two = _capture_target("candidate-restart-2")
    first_adapter = FakeOutcomeMarketAdapter()
    with EvidenceStore(path) as evidence:
        first = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=first_adapter,
        )
        first.register_target(target_one, horizon="30M")
        first.register_target(target_two, horizon="30M")
        result = first.tick(now=due_at + timedelta(seconds=1))
        assert result.observations_appended == 2
        assert len(first_adapter.calls) == 1
        assert len(first_adapter.calls[0]) == 2

    restarted_adapter = FakeOutcomeMarketAdapter()
    with EvidenceStore(path) as reopened:
        restarted = ExactHorizonOutcomeCapture(
            evidence_store=reopened,
            market_adapter=restarted_adapter,
        )
        duplicate = restarted.register_target(target_one, horizon="30M")
        replay = restarted.tick(now=due_at + timedelta(seconds=2))

        assert duplicate.status == "ALREADY_REGISTERED"
        assert replay.observations_appended == 0
        assert restarted_adapter.calls == []
        assert len(
            reopened.query(kinds=(OUTCOME_OBSERVATION_KIND,), limit=10)
        ) == 2


def test_capture_coordinator_persists_prospective_candidate_and_prediction_specs(
    tmp_path: Path,
) -> None:
    candidate = _capture_target("candidate-coordinator")
    prediction = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "prediction-coordinator-30m",
        "subject_hash": "e" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "f" * 64,
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "BLOCKED",
            "reason_codes": (
                "OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE",
            ),
        },
    }

    class UnavailableCalendar:
        calls = 0

        def snapshot(self, *, now: datetime) -> None:
            self.calls += 1
            assert now == BASE + timedelta(minutes=30, seconds=1)
            return None

    evidence = EvidenceStore(tmp_path / "capture-coordinator.sqlite3")
    try:
        adapter = FakeOutcomeMarketAdapter()
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        calendar = UnavailableCalendar()
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda: (candidate,),
            prediction_targets=lambda: ((prediction, "30M"),),
            calendar_provider=calendar,
            clock=lambda: BASE + timedelta(minutes=30, seconds=1),
        )

        result = coordinator.tick()
        specs = evidence.query(kinds=(OUTCOME_CAPTURE_SPEC_KIND,), limit=20)
        observations = evidence.query(kinds=(OUTCOME_OBSERVATION_KIND,), limit=20)

        assert calendar.calls == 1
        assert len(specs) == 6
        assert len(observations) == 1
        assert result.observations_appended == 1
        assert len(adapter.calls) == 1
        prediction_spec = next(
            item
            for item in specs
            if item.record.payload["subject_kind"] == "PREDICTION"
        )
        assert prediction_spec.record.payload["status"] == "BLOCKED"
        assert (
            "OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE"
            in prediction_spec.record.payload["reason_codes"]
        )
    finally:
        evidence.close()


def test_capture_loop_stops_without_leaking_its_worker_thread(tmp_path: Path) -> None:
    evidence = EvidenceStore(tmp_path / "capture-loop.sqlite3")
    try:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=FakeOutcomeMarketAdapter(),
        )

        class Calendar:
            def snapshot(self, *, now: datetime) -> None:
                return None

        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda: (),
            prediction_targets=lambda: (),
            calendar_provider=Calendar(),
        )
        loop = OutcomeCaptureLoop(coordinator, interval_seconds=0.05)

        loop.start()
        time.sleep(0.08)

        assert loop.close() is True
        assert loop._thread is None
        assert loop.last_result is not None
    finally:
        evidence.close()


def test_capture_coordinator_reports_target_provider_failures(tmp_path: Path) -> None:
    def unavailable_targets(*, after_sequence: int = 0):
        del after_sequence
        raise RuntimeError("provider unavailable")

    with EvidenceStore(tmp_path / "capture-provider-failure.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=FakeOutcomeMarketAdapter(),
        )
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=unavailable_targets,
            prediction_targets=unavailable_targets,
            calendar_provider=type(
                "Calendar",
                (),
                {"snapshot": lambda self, **_: None},
            )(),
            clock=lambda: BASE,
        )

        result = coordinator.tick()

        assert result.status == "BLOCKED"
        assert result.records_blocked == 2
        assert result.reason_codes == (
            "CANDIDATE_TARGET_PROVIDER_UNAVAILABLE",
            "PREDICTION_TARGET_PROVIDER_UNAVAILABLE",
        )


def test_capture_loop_publishes_fail_closed_result_on_worker_exception(
    tmp_path: Path,
) -> None:
    with EvidenceStore(tmp_path / "capture-loop-failure.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=FakeOutcomeMarketAdapter(),
        )
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda: (),
            prediction_targets=lambda: (),
            calendar_provider=type(
                "Calendar",
                (),
                {"snapshot": lambda self, **_: None},
            )(),
            clock=lambda: BASE,
        )
        loop = OutcomeCaptureLoop(coordinator, interval_seconds=0.05)

        def fail_once() -> OutcomeCaptureResult:
            loop._stop.set()
            raise RuntimeError("capture failed")

        coordinator.tick = fail_once  # type: ignore[method-assign]
        loop._run()

        result = loop.last_result
        assert result is not None
        assert result.status == "BLOCKED"
        assert result.records_blocked == 1
        assert result.reason_codes == ("OUTCOME_CAPTURE_LOOP_FAILED",)


def test_session_horizons_honor_holidays_early_close_and_dst_shift() -> None:
    occurred_at = datetime(2026, 10, 30, 19, 0, tzinfo=timezone.utc)
    closes = (
        datetime(2026, 10, 30, 20, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 2, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 3, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 4, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 5, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 6, 21, 0, tzinfo=timezone.utc),
    )
    observed_at = occurred_at - timedelta(minutes=5)
    sessions = [
        {
            "trading_date": close_at.date().isoformat(),
            "open_at": close_at - timedelta(hours=6, minutes=30),
            "close_at": close_at,
            "source": "OFFICIAL_SESSION_CALENDAR",
            "source_id": f"session-{close_at.date().isoformat()}",
            "source_hash": canonical_hash({"close_at": close_at}),
            "observed_at": observed_at,
        }
        for close_at in closes
    ]
    evidence = {
        "schema": "options_copilot.outcome_horizon_evidence.v1",
        "target_rule": OUTCOME_TARGET_RULES["1D"],
        "method": "SESSION_CALENDAR",
        "sessions": sessions,
    }
    one_day, _ = resolve_outcome_horizon(
        "1D",
        occurred_at=occurred_at,
        evidence=evidence,
        as_of=datetime(2026, 11, 7, tzinfo=timezone.utc),
    )
    assert one_day == datetime(2026, 11, 2, 21, 0, tzinfo=timezone.utc)

    holiday_occurred_at = datetime(2026, 11, 25, 15, 0, tzinfo=timezone.utc)
    holiday_closes = (
        datetime(2026, 11, 25, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc),
        datetime(2026, 11, 30, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 12, 1, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 12, 2, 21, 0, tzinfo=timezone.utc),
        datetime(2026, 12, 3, 21, 0, tzinfo=timezone.utc),
    )
    holiday_sessions = [
        {
            "trading_date": close_at.date().isoformat(),
            "open_at": close_at - timedelta(hours=6, minutes=30),
            "close_at": close_at,
            "source": "OFFICIAL_SESSION_CALENDAR",
            "source_id": f"holiday-session-{close_at.date().isoformat()}",
            "source_hash": canonical_hash({"holiday_close_at": close_at}),
            "observed_at": holiday_occurred_at - timedelta(minutes=5),
        }
        for close_at in holiday_closes
    ]
    holiday_evidence = {
        "schema": "options_copilot.outcome_horizon_evidence.v1",
        "target_rule": OUTCOME_TARGET_RULES["1D"],
        "method": "SESSION_CALENDAR",
        "sessions": holiday_sessions,
    }
    holiday_one_day, _ = resolve_outcome_horizon(
        "1D",
        occurred_at=holiday_occurred_at,
        evidence=holiday_evidence,
        as_of=datetime(2026, 12, 4, tzinfo=timezone.utc),
    )
    assert holiday_one_day == datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc)


def _binding_candidate(
    subject_id: str,
    *,
    symbol: str = "SPY",
    occurred_at: datetime = BASE + timedelta(minutes=5),
    rank: int = 1,
    event_ids: tuple[str, ...] = ("news-event-1",),
) -> dict[str, object]:
    target = _capture_target(subject_id)
    target["symbol"] = symbol
    target["occurred_at"] = occurred_at
    plan = target["capture_plan"]
    assert isinstance(plan, dict)
    for name in ("underlying", "benchmark"):
        binding = plan[name]
        assert isinstance(binding, dict)
        binding["symbol"] = symbol
        binding["observed_at"] = occurred_at
    plan["benchmark_symbol"] = symbol
    target["binding_context"] = {
        "source_sequence": int(subject_id.rsplit("-", 1)[-1]),
        "scan_run_id": f"scan-{subject_id}",
        "ranking_snapshot_id": f"ranking-{subject_id}",
        "ranking_snapshot_hash": canonical_hash({"ranking": subject_id}),
        "candidate_id": subject_id,
        "candidate_hash": target["subject_hash"],
        "ranking_basis_hash": canonical_hash({"basis": subject_id}),
        "rank": rank,
        "event_ids": event_ids,
    }
    return target


class BulkOutcomeMarketAdapter:
    def __init__(self, *, fail_call: int | None = None) -> None:
        self.fail_call = fail_call
        self.calls: list[tuple[Mapping[str, object], ...]] = []

    def observe(
        self,
        specs: tuple[Mapping[str, object], ...],
        *,
        expected_at: datetime,
    ) -> Mapping[str, object]:
        self.calls.append(specs)
        if self.fail_call == len(self.calls):
            raise RuntimeError("bounded fake chunk failure")
        observed_at = expected_at + timedelta(seconds=1)
        symbols: dict[str, dict[str, object]] = {}
        quotes: dict[int, dict[str, object]] = {}
        for spec in specs:
            plan = spec["capture_plan"]
            assert isinstance(plan, Mapping)
            baseline_request = spec.get("prediction_baseline_request")
            prediction_baseline = spec.get("prediction_baseline")
            benchmark_symbol = (
                prediction_baseline.get("benchmark_symbol")
                if isinstance(prediction_baseline, Mapping)
                else baseline_request.get("benchmark_symbol")
                if isinstance(baseline_request, Mapping)
                else plan.get("benchmark_symbol")
            )
            for symbol in (str(spec["symbol"]), str(benchmark_symbol)):
                symbols[symbol] = {
                    "symbol": symbol,
                    "price": "101",
                    "observed_at": observed_at,
                    "source_id": f"underlying:{symbol}",
                    "source_hash": canonical_hash({"symbol": symbol}),
                }
            legs = plan.get("legs", ())
            assert isinstance(legs, tuple)
            for leg in legs:
                assert isinstance(leg, Mapping)
                con_id = int(leg["con_id"])
                quotes[con_id] = {
                    "con_id": con_id,
                    "bid": "1.40",
                    "ask": "1.50",
                    "implied_volatility": "0.21",
                    "volume": 140,
                    "observed_at": observed_at,
                }
        return {
            "schema": "options_copilot.outcome_market_batch.v1",
            "observed_at": observed_at,
            "source": "IBKR_READ_ONLY_BULK_FAKE",
            "source_id": f"bulk:{len(self.calls)}",
            "source_hash": canonical_hash(
                {"call": len(self.calls), "subjects": [item["subject_id"] for item in specs]}
            ),
            "underlyings": tuple(symbols.values()),
            "quotes": tuple(quotes.values()),
        }


class FlakyPerSpecOutcomeAdapter(BulkOutcomeMarketAdapter):
    def __init__(
        self,
        *,
        invalid_calls: tuple[int, ...] = (),
        omit_optional_market_fields: bool = False,
    ) -> None:
        super().__init__()
        self.invalid_calls = set(invalid_calls)
        self.omit_optional_market_fields = omit_optional_market_fields

    def observe(self, specs, *, expected_at):
        result = dict(super().observe(specs, expected_at=expected_at))
        quotes = [dict(item) for item in result["quotes"]]
        if len(self.calls) in self.invalid_calls and quotes:
            quotes[0]["ask"] = Decimal("0.50")
            quotes[0]["bid"] = Decimal("1.50")
        if self.omit_optional_market_fields:
            for quote in quotes:
                quote.pop("implied_volatility", None)
                quote.pop("volume", None)
        result["quotes"] = tuple(quotes)
        return result


def test_prediction_waiting_spec_is_superseded_by_hash_bound_rank_one_then_captured(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE + timedelta(seconds=1))
    shadow = ShadowLearningLedger(tmp_path / "prediction-upgrade-shadow.sqlite3", clock=clock)
    recorder = OutcomeRecorder(tmp_path / "prediction-upgrade-outcomes.sqlite3", clock=clock)
    evidence = EvidenceStore(tmp_path / "prediction-upgrade-evidence.sqlite3", clock=clock)
    try:
        thesis = shadow.record_thesis(
            "news-thesis",
            champion_version="champion-v1",
            challenger_version="challenger-v1",
            thesis={"purpose": "news shadow prediction"},
            created_at=BASE - timedelta(minutes=2),
        )
        news = shadow.record_evidence(
            "news-evidence-1",
            thesis.thesis_id,
            source="NEWS_SHADOW_STORE",
            evidence={"event_id": "news-event-1", "symbol": "SPY"},
            published_at=BASE - timedelta(minutes=1),
            first_seen_at=BASE,
        )
        advisory_id = "advisory-1"
        prediction_set_predicted_at = BASE
        prediction_baseline_hash = canonical_hash(
            {
                "schema": "options_copilot.news_prediction_baseline.v2",
                "advisory_id": advisory_id,
                "event_id": "news-event-1",
                "symbol": "SPY",
                "model_visible_snapshot_hash": "a" * 64,
                "prediction_set_predicted_at": prediction_set_predicted_at,
            }
        )
        prediction = shadow.record_prediction(
            news_shadow_prediction_id(advisory_id, "30m"),
            thesis.thesis_id,
            evidence_ids=(news.evidence_id,),
            prediction={
                "schema": NEWS_SHADOW_PREDICTION_SCHEMA,
                "advisory_id": advisory_id,
                "event_id": "news-event-1",
                "symbol": "SPY",
                "horizon": "30M",
                "target_rule": OUTCOME_TARGET_RULES["30M"],
                "classification": {"direction": "BULLISH"},
                "symbol_binding": None,
                "model_visible_snapshot_hash": "a" * 64,
                "prediction_set_predicted_at": prediction_set_predicted_at.isoformat(),
                "prediction_baseline_hash": prediction_baseline_hash,
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            },
            predicted_at=BASE,
            independence_key="news-event-1:SPY:30M",
        )
        runtime = object.__new__(OptionsCopilotRuntime)
        runtime.shadow_learning = shadow
        candidate_rows: list[Mapping[str, object]] = []
        adapter = BulkOutcomeMarketAdapter()
        capture = ExactHorizonOutcomeCapture(evidence_store=evidence, market_adapter=adapter)
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda *, after_sequence=0: tuple(
                item
                for item in candidate_rows
                if int(item["binding_context"]["source_sequence"]) > after_sequence  # type: ignore[index]
            ),
            prediction_targets=runtime._prediction_outcome_targets,
            calendar_provider=type("Calendar", (), {"snapshot": lambda self, **_: None})(),
            clock=clock,
        )

        first = coordinator.tick()
        initial_specs = tuple(evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,)))
        assert first.status == "WAITING"
        assert [row.record.payload["status"] for row in initial_specs] == [
            "WAITING",
            "WAITING",
        ]
        prediction_baseline = initial_specs[-1].record.payload["prediction_baseline"]
        assert prediction_baseline["status"] == "AVAILABLE"  # type: ignore[index]
        baseline_observed_at = datetime.fromisoformat(
            str(prediction_baseline["underlying"]["observed_at"])  # type: ignore[index]
        )
        assert baseline_observed_at <= BASE + timedelta(seconds=5)
        assert len(adapter.calls) == 1

        candidate_rows.append(
            _binding_candidate(
                "candidate-1",
                occurred_at=BASE - timedelta(seconds=1),
            )
        )
        clock.now = BASE + timedelta(minutes=10)
        second = coordinator.tick()
        revised_specs = tuple(evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,)))
        assert second.status == "WAITING"
        assert len(revised_specs) == 8
        prediction_specs = [
            row for row in revised_specs if row.record.payload["subject_kind"] == "PREDICTION"
        ]
        assert [row.record.payload["status"] for row in prediction_specs] == [
            "WAITING",
            "WAITING",
            "READY",
        ]
        binding = prediction_specs[-1].record.payload["prediction_candidate_binding"]
        assert binding["prediction_hash"] == prediction.content_hash  # type: ignore[index]
        assert binding["independence_key"] == prediction.independence_key  # type: ignore[index]
        assert binding["rank"] == 1  # type: ignore[index]
        assert binding["candidate_hash"] == candidate_rows[0]["subject_hash"]  # type: ignore[index]
        plan = prediction_specs[-1].record.payload["capture_plan"]
        assert plan["underlying"] == prediction_baseline["underlying"]  # type: ignore[index]
        assert plan["benchmark"] == prediction_baseline["benchmark"]  # type: ignore[index]
        assert prediction_specs[-1].record.supersedes_id == prediction_specs[-2].evidence_id

        clock.now = BASE + timedelta(minutes=30, seconds=1)
        captured = coordinator.tick()
        assert captured.observations_appended == 2
        assert len(adapter.calls) == 3
        assert {
            call[0]["subject_kind"] for call in adapter.calls[1:]
        } == {"CANDIDATE", "PREDICTION"}
        observation = next(
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_OBSERVATION_KIND,))
            if row.record.payload["subject_kind"] == "PREDICTION"
        )
        assert observation["underlying"]["baseline_price"] == Decimal("101")  # type: ignore[index]

        processed = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda: (),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            clock=lambda: BASE + timedelta(minutes=30, seconds=2),
        ).process()
        assert processed.records_appended == 1
        assert shadow.get_outcome(prediction.prediction_id) is not None
    finally:
        evidence.close()
        recorder.close()
        shadow.close()


def test_jin10_cpi_proxy_prediction_uses_spy_baseline_and_spy_outcome(
    tmp_path: Path,
) -> None:
    """Exercise the durable macro proxy from advisory through one real horizon."""

    clock = MutableClock(BASE + timedelta(seconds=1))
    shadow = ShadowLearningLedger(tmp_path / "jin10-proxy-shadow.sqlite3", clock=clock)
    recorder = OutcomeRecorder(tmp_path / "jin10-proxy-outcomes.sqlite3", clock=clock)
    evidence = EvidenceStore(tmp_path / "jin10-proxy-evidence.sqlite3", clock=clock)
    try:
        binding = MarketProxyBinding(
            event_category="US_INFLATION",
            source="JIN10",
            proxy_symbol="SPY",
        )
        news = NewsInput(
            event_id="evt-jin10-cpi-proxy-outcome",
            headline="US CPI came in below consensus",
            summary="Core CPI also slowed versus expectation.",
            source="Jin10",
            source_url="https://flash.jin10.com/detail/cpi-proxy-outcome",
            published_at=BASE - timedelta(seconds=20),
            first_seen_at=BASE - timedelta(seconds=10),
            evidence_ids=("evidence-jin10-cpi-proxy-outcome",),
            symbols=("SPY",),
        )
        classification = ClassifiedEvent(
            category=EventCategory.MACRO,
            symbols=("SPY",),
            direction=ImpactDirection.BULLISH,
            horizon=ImpactHorizon.INTRADAY,
            confidence=Decimal("0.80"),
            counter_evidence=("Rates may still remain restrictive",),
            evidence_ids=news.evidence_ids,
            classifier="STRUCTURED_LLM",
        )
        advisory_id = "news-advisory:" + "9" * 64
        independence_key = "news-event:" + "8" * 64
        targets = (
            ("30M", OUTCOME_TARGET_RULES["30M"], "30m"),
            ("SESSION_CLOSE", OUTCOME_TARGET_RULES["SESSION_CLOSE"], "session-close"),
            ("1D", OUTCOME_TARGET_RULES["1D"], "1d"),
            ("3D", OUTCOME_TARGET_RULES["3D"], "3d"),
            ("5D", OUTCOME_TARGET_RULES["5D"], "5d"),
        )
        advisory = ResearchAdvisoryProjection(
            advisory_id=advisory_id,
            event_id=news.event_id,
            symbol="SPY",
            classification=classification,
            research_priority_score=Decimal("91.00"),
            prediction_specs=tuple(
                PredictionSpec(
                    prediction_id=news_shadow_prediction_id(advisory_id, slug),
                    advisory_id=advisory_id,
                    event_id=news.event_id,
                    symbol="SPY",
                    horizon=horizon,
                    target_rule=target_rule,
                    independence_key=independence_key,
                )
                for horizon, target_rule, slug in targets
            ),
            symbol_binding=binding,
        )
        writer = NewsShadowLearningWriter(
            shadow,
            enabled_at=BASE - timedelta(minutes=1),
        )
        writer.record(advisory, news, recorded_at=BASE)

        predictions = shadow.query_replays(limit=10)
        assert len(predictions) == 5
        assert {item.prediction.prediction["symbol"] for item in predictions} == {
            "SPY"
        }
        assert {
            canonical_hash(item.prediction.prediction["symbol_binding"])
            for item in predictions
        } == {canonical_hash(binding.as_dict())}
        assert all(
            item.prediction.prediction["decision_authority"] == "SUPPORTING_ONLY"
            and item.prediction.prediction["approval_eligible"] is False
            and item.prediction.prediction["instruction_creation_allowed"] is False
            and item.prediction.prediction["order_allowed"] is False
            for item in predictions
        )

        runtime = object.__new__(OptionsCopilotRuntime)
        runtime.shadow_learning = shadow
        projected = runtime._prediction_outcome_targets()
        assert len(projected) == 5
        assert {target[0]["symbol"] for target in projected} == {"SPY"}
        assert all(
            target[0]["prediction_baseline_request"]["benchmark_symbol"] == "SPY"
            for target in projected
        )

        adapter = BulkOutcomeMarketAdapter()
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=runtime._prediction_outcome_targets,
            calendar_provider=type(
                "Calendar",
                (),
                {"snapshot": lambda self, **_: None},
            )(),
            clock=clock,
        )

        initial = coordinator.tick()
        assert initial.status == "WAITING"
        assert adapter.calls
        assert all(
            spec["symbol"] == "SPY"
            for call in adapter.calls
            for spec in call
        )
        baseline_specs = [
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
            if row.record.payload["subject_kind"] == "PREDICTION"
            and isinstance(row.record.payload.get("prediction_baseline"), Mapping)
        ]
        assert baseline_specs
        assert all(
            row["prediction_baseline"]["underlying"]["symbol"] == "SPY"
            for row in baseline_specs
        )

        clock.now = BASE + timedelta(minutes=30, seconds=1)
        captured = coordinator.tick()
        assert captured.observations_appended == 1
        observation = next(
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_OBSERVATION_KIND,))
        )
        assert observation["underlying"]["symbol"] == "SPY"

        processed = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda: (),
            observation_provider=EvidenceStoreOutcomeObservationProvider(evidence),
            clock=lambda: BASE + timedelta(minutes=30, seconds=2),
        ).process()
        assert processed.records_appended == 1
        stored = shadow.get_outcome(news_shadow_prediction_id(advisory_id, "30m"))
        assert stored is not None
        assert stored.outcome["underlying"]["symbol"] == "SPY"
        assert stored.outcome["subject_id"] == news_shadow_prediction_id(
            advisory_id,
            "30m",
        )
        assert stored.outcome["approval_eligible"] is False
        assert stored.outcome["instruction_creation_allowed"] is False
        assert stored.outcome["order_allowed"] is False
    finally:
        evidence.close()
        recorder.close()
        shadow.close()


@pytest.mark.parametrize(
    ("targets", "maximum_symbols", "maximum_contracts"),
    (
        (
            tuple(
                _binding_candidate(f"symbol-{index}", symbol=f"SYM{index:03d}")
                for index in range(60)
            ),
            50,
            50,
        ),
        (
            tuple(
                _binding_candidate(f"contract-{index}")
                for index in range(40)
            ),
            50,
            50,
        ),
    ),
)
def test_same_horizon_capture_is_partitioned_within_symbol_and_contract_bounds(
    tmp_path: Path,
    targets: tuple[dict[str, object], ...],
    maximum_symbols: int,
    maximum_contracts: int,
) -> None:
    for index, target in enumerate(targets):
        plan = target["capture_plan"]
        assert isinstance(plan, dict)
        if str(target["subject_id"]).startswith("contract-"):
            for leg_index, leg in enumerate(plan["legs"]):
                leg["con_id"] = 10000 + (index * 2) + leg_index
    adapter = BulkOutcomeMarketAdapter()
    with EvidenceStore(tmp_path / f"bounded-{len(targets)}.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(evidence_store=evidence, market_adapter=adapter)
        for target in targets:
            capture.register_target(target, horizon="30M")

        result = capture.tick(now=BASE + timedelta(minutes=35, seconds=1))

        assert result.observations_appended == len(targets)
        assert len(adapter.calls) >= 2
        for call in adapter.calls:
            symbols = {
                str(value)
                for spec in call
                for value in (
                    spec["symbol"],
                    spec["capture_plan"]["benchmark_symbol"],  # type: ignore[index]
                )
            }
            contracts = {
                int(leg["con_id"])
                for spec in call
                for leg in spec["capture_plan"]["legs"]  # type: ignore[index]
            }
            assert len(symbols) <= maximum_symbols
            assert len(contracts) <= maximum_contracts


def test_chunk_failure_is_degraded_without_discarding_successful_chunks(tmp_path: Path) -> None:
    targets = tuple(
        _binding_candidate(f"partial-{index}", symbol=f"PS{index:03d}")
        for index in range(60)
    )
    adapter = BulkOutcomeMarketAdapter(fail_call=2)
    with EvidenceStore(tmp_path / "partial-chunk.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(evidence_store=evidence, market_adapter=adapter)
        for target in targets:
            capture.register_target(target, horizon="30M")

        result = capture.tick(now=BASE + timedelta(minutes=35, seconds=1))

        assert result.status == "DEGRADED"
        assert result.observations_appended > 0
        assert result.records_blocked > 0
        assert result.observations_appended + result.records_blocked == len(targets)
        assert len(adapter.calls) >= 2


def test_missing_iv_and_volume_are_explicitly_unavailable_not_capture_failure(
    tmp_path: Path,
) -> None:
    target = _capture_target("optional-market-fields")
    plan = target["capture_plan"]
    assert isinstance(plan, dict)
    for leg in plan["legs"]:
        leg.pop("implied_volatility")
        leg.pop("volume")
    adapter = FlakyPerSpecOutcomeAdapter(omit_optional_market_fields=True)
    with EvidenceStore(tmp_path / "optional-market-fields.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        assert capture.register_target(target, horizon="30M").status == "REGISTERED"

        result = capture.tick(now=BASE + timedelta(minutes=30, seconds=1))

        assert result.observations_appended == 1
        observation = tuple(
            evidence.iter_verified(kinds=(OUTCOME_OBSERVATION_KIND,))
        )[0].record.payload
        assert observation["option_market"]["iv_change"]["value"] is None  # type: ignore[index]
        assert observation["option_market"]["iv_change"]["provenance"]["reason_code"] == "OPTION_IV_UNAVAILABLE"  # type: ignore[index]
        assert observation["option_market"]["volume_change"]["value"] is None  # type: ignore[index]
        assert observation["option_market"]["volume_change"]["provenance"]["reason_code"] == "OPTION_VOLUME_UNAVAILABLE"  # type: ignore[index]


def test_transient_per_spec_failure_requeues_then_captures_within_window(
    tmp_path: Path,
) -> None:
    adapter = FlakyPerSpecOutcomeAdapter(invalid_calls=(1,))
    with EvidenceStore(tmp_path / "per-spec-retry.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        capture.register_target(_capture_target("per-spec-retry"), horizon="30M")

        first = capture.tick(now=BASE + timedelta(minutes=30, seconds=1))
        second = capture.tick(now=BASE + timedelta(minutes=30, seconds=2))

        assert first.records_blocked == 1
        assert first.observations_appended == 0
        assert second.observations_appended == 1
        assert len(adapter.calls) == 2


def test_repeated_per_spec_failure_terminalizes_at_plus_five_without_backfill(
    tmp_path: Path,
) -> None:
    adapter = FlakyPerSpecOutcomeAdapter(invalid_calls=(1, 2, 3))
    with EvidenceStore(tmp_path / "per-spec-terminal.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        capture.register_target(_capture_target("per-spec-terminal"), horizon="30M")

        capture.tick(now=BASE + timedelta(minutes=30, seconds=1))
        capture.tick(now=BASE + timedelta(minutes=30, seconds=4))
        terminal = capture.tick(now=BASE + timedelta(minutes=30, seconds=6))
        replay = capture.tick(now=BASE + timedelta(minutes=30, seconds=7))

        specs = tuple(evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,)))
        assert [row.record.payload["status"] for row in specs] == [
            "READY",
            "BLOCKED",
        ]
        assert specs[-1].record.payload["reason_codes"] == (
            "OUTCOME_CAPTURE_WINDOW_MISSED",
        )
        assert specs[-1].record.supersedes_id == specs[0].evidence_id
        assert terminal.status == "BLOCKED"
        assert replay.observations_appended == 0
        assert len(adapter.calls) == 2


def test_prediction_baseline_chunk_failure_retries_then_terminalizes_after_window(
    tmp_path: Path,
) -> None:
    class FailingBaselineAdapter:
        def __init__(self) -> None:
            self.calls = 0

        def observe(self, specs, *, expected_at):
            self.calls += 1
            raise RuntimeError("bounded prediction baseline failure")

    target = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "baseline-chunk-terminal",
        "subject_hash": "7" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "8" * 64,
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "WAITING",
            "reason_codes": ("OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE",),
        },
    }
    adapter = FailingBaselineAdapter()
    with EvidenceStore(tmp_path / "prediction-baseline-chunk-terminal.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,  # type: ignore[arg-type]
        )
        assert capture.register_target(target, horizon="30M").status == "WAITING"

        retry = capture.tick(now=BASE + timedelta(seconds=1))
        terminal = capture.tick(now=BASE + timedelta(seconds=6))
        replay = capture.tick(now=BASE + timedelta(seconds=7))

        specs = tuple(evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,)))
        assert retry.status == "BLOCKED"
        assert [row.record.payload["status"] for row in specs] == [
            "WAITING",
            "BLOCKED",
        ]
        assert specs[-1].record.payload["reason_codes"] == (
            "OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED",
        )
        assert terminal.status == "BLOCKED"
        assert replay.observations_appended == 0
        assert adapter.calls == 1


def test_startup_many_expired_prediction_baselines_terminalize_without_adapter_calls(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE + timedelta(minutes=10))
    predictions = tuple(
        (
            {
                "schema": "options_copilot.outcome_target.v1",
                "subject_kind": "PREDICTION",
                "subject_id": f"expired-startup-{index}",
                "subject_hash": canonical_hash({"expired": index}),
                "symbol": f"S{index:03d}",
                "occurred_at": BASE,
                "thesis_hash": canonical_hash({"thesis": index}),
                "source_sequence": index,
                "prediction_hash": canonical_hash({"expired": index}),
                "independence_key": f"expired:{index}:30M",
                "binding_context": {
                    "source_sequence": index,
                    "event_ids": (),
                    "independence_key": f"expired:{index}:30M",
                },
                "prediction_baseline_request": {
                    "schema": "options_copilot.prediction_baseline_request.v1",
                    "benchmark_symbol": "SPY",
                },
                "capture_plan": {
                    "schema": "options_copilot.outcome_capture_plan.v1",
                    "status": "WAITING",
                    "reason_codes": (
                        "OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE",
                    ),
                },
            },
            "30M",
        )
        for index in range(1, 76)
    )
    provider_calls: list[int] = []

    def prediction_targets(*, after_sequence: int = 0):
        provider_calls.append(after_sequence)
        return tuple(
            item
            for item in predictions
            if int(item[0]["source_sequence"]) > after_sequence
        )

    adapter = BulkOutcomeMarketAdapter()
    with EvidenceStore(tmp_path / "expired-startup-predictions.sqlite3") as evidence:
        coordinator = OutcomeCaptureCoordinator(
            capture=ExactHorizonOutcomeCapture(
                evidence_store=evidence,
                market_adapter=adapter,
            ),
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=prediction_targets,
            calendar_provider=type("Calendar", (), {"snapshot": lambda self, **_: None})(),
            clock=clock,
        )

        terminal = coordinator.tick()
        replay = coordinator.tick()

        specs = tuple(evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,)))
        assert terminal.status == "BLOCKED"
        assert replay.observations_appended == 0
        assert len(specs) == 150
        assert sum(row.record.payload["status"] == "WAITING" for row in specs) == 75
        assert sum(row.record.payload["status"] == "BLOCKED" for row in specs) == 75
        assert {
            row.record.payload["reason_codes"]
            for row in specs
            if row.record.payload["status"] == "BLOCKED"
        } == {("OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED",)}
        assert adapter.calls == []
        assert provider_calls == [0, 75]
        assert coordinator._pending_predictions == {}


def test_fresh_prediction_baseline_is_not_starved_by_expired_startup_backlog(
    tmp_path: Path,
) -> None:
    checked_at = BASE + timedelta(minutes=10)
    expired = tuple(
        (
            {
                "schema": "options_copilot.outcome_target.v1",
                "subject_kind": "PREDICTION",
                "subject_id": f"expired-priority-{index}",
                "subject_hash": canonical_hash({"expired-priority": index}),
                "symbol": f"E{index:03d}",
                "occurred_at": BASE,
                "thesis_hash": canonical_hash({"expired-thesis": index}),
                "source_sequence": index,
                "prediction_hash": canonical_hash({"expired-prediction": index}),
                "independence_key": f"expired-priority:{index}",
                "prediction_baseline_request": {
                    "schema": "options_copilot.prediction_baseline_request.v1",
                    "benchmark_symbol": "SPY",
                },
                "capture_plan": {
                    "schema": "options_copilot.outcome_capture_plan.v1",
                    "status": "DIRECTION_ONLY",
                    "reason_codes": (),
                },
            },
            "30M",
        )
        for index in range(1, 76)
    )
    fresh = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "fresh-priority",
        "subject_hash": "f" * 64,
        "symbol": "SPY",
        "occurred_at": checked_at,
        "thesis_hash": "e" * 64,
        "source_sequence": 76,
        "prediction_hash": "f" * 64,
        "independence_key": "fresh-priority",
        "predicted_direction": "BULLISH",
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "DIRECTION_ONLY",
            "reason_codes": (),
        },
    }
    adapter = BulkOutcomeMarketAdapter()
    with EvidenceStore(tmp_path / "fresh-priority.sqlite3") as evidence:
        coordinator = OutcomeCaptureCoordinator(
            capture=ExactHorizonOutcomeCapture(
                evidence_store=evidence,
                market_adapter=adapter,
            ),
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=lambda *, after_sequence=0: (
                (*expired, (fresh, "30M")) if after_sequence < 76 else ()
            ),
            calendar_provider=type("Calendar", (), {"snapshot": lambda self, **_: None})(),
            clock=lambda: checked_at,
        )

        coordinator.tick()

        fresh_specs = [
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
            if row.record.payload["subject_id"] == "fresh-priority"
        ]
        assert fresh_specs[-1]["prediction_baseline"]["status"] == "AVAILABLE"
        assert adapter.calls[0][0]["subject_id"] == "fresh-priority"


def test_prediction_horizons_share_one_direction_baseline_and_need_no_candidate(
    tmp_path: Path,
) -> None:
    prediction = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "shared-direction",
        "subject_hash": "c" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "d" * 64,
        "source_sequence": 1,
        "prediction_hash": "c" * 64,
        "independence_key": "shared-direction",
        "predicted_direction": "BULLISH",
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "DIRECTION_ONLY",
            "reason_codes": (),
        },
    }
    adapter = BulkOutcomeMarketAdapter()
    with EvidenceStore(tmp_path / "shared-direction.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=adapter,
        )
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=lambda *, after_sequence=0: (
                tuple((prediction, horizon) for horizon in OUTCOME_HORIZONS)
                if after_sequence < 1
                else ()
            ),
            calendar_provider=type(
                "Calendar",
                (),
                {"snapshot": lambda self, **_: type("S", (), {"sessions": _calendar_sessions()})()},
            )(),
            clock=lambda: BASE,
        )

        coordinator.tick()

        specs = [
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
            if row.record.payload["subject_id"] == "shared-direction"
        ]
        baselines = [
            row["prediction_baseline"]
            for row in specs
            if isinstance(row.get("prediction_baseline"), Mapping)
        ]
        assert len(adapter.calls) == 1
        assert len({canonical_hash(item) for item in baselines}) == 1
        assert all(
            row["capture_plan"]["status"] == "DIRECTION_ONLY"
            for row in specs
        )

        clock = MutableClock(BASE + timedelta(minutes=30, seconds=1))
        coordinator._clock = clock
        captured = coordinator.tick()
        assert captured.observations_appended == 1
        observation = next(
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_OBSERVATION_KIND,))
        )
        assert observation["combination"]["estimated_pnl_usd"] is None
        assert observation["combination"]["provenance"]["reason_code"] == (
            "OPTION_COMBINATION_NOT_BOUND"
        )
        assert observation["thesis_validity"]["status"] in {"VALID", "INVALID"}
        assert observation["thesis_validity"]["reason_code"] in {
            "PREDICTED_DIRECTION_MATCHED",
            "PREDICTED_DIRECTION_MISSED",
        }


def test_direction_only_upgrade_terminalizes_and_is_not_requeued_after_restart(
    tmp_path: Path,
) -> None:
    prediction = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "direction-only-terminal",
        "subject_hash": "c" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "d" * 64,
        "source_sequence": 1,
        "prediction_hash": "c" * 64,
        "independence_key": "direction-only-terminal",
        "predicted_direction": "BULLISH",
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "DIRECTION_ONLY",
            "reason_codes": (),
        },
    }
    path = tmp_path / "direction-only-terminal.sqlite3"
    with EvidenceStore(path) as evidence:
        clock = MutableClock(BASE)
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=BulkOutcomeMarketAdapter(),
        )
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=lambda *, after_sequence=0: (
                ((prediction, "30M"),) if after_sequence < 1 else ()
            ),
            calendar_provider=type(
                "Calendar",
                (),
                {"snapshot": lambda self, **_: None},
            )(),
            clock=clock,
        )

        coordinator.tick()
        clock.now = BASE + timedelta(seconds=1)
        coordinator.tick()
        direction_specs = tuple(
            row
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
            if row.record.payload["subject_id"] == "direction-only-terminal"
        )
        assert direction_specs[-1].record.payload["status"] == "READY"
        assert direction_specs[-1].record.payload["capture_plan"]["status"] == (
            "DIRECTION_ONLY"
        )

        failing = BulkOutcomeMarketAdapter(fail_call=1)
        resumed = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=failing,
        )
        retry = resumed.tick(now=BASE + timedelta(minutes=30, seconds=1))
        terminal = resumed.tick(now=BASE + timedelta(minutes=30, seconds=6))

        assert retry.records_blocked == 1
        assert terminal.reason_codes == ("OUTCOME_CAPTURE_WINDOW_MISSED",)
        provider = EvidenceStoreOutcomeObservationProvider(evidence)
        checked_at = BASE + timedelta(minutes=30, seconds=6)
        provider.begin_verified_batch(as_of=checked_at)
        try:
            assert provider.terminal_reason(
                prediction,
                horizon="30M",
                as_of=checked_at,
            ) == "OUTCOME_CAPTURE_WINDOW_MISSED"
        finally:
            provider.end_verified_batch()

    with EvidenceStore(path) as reopened:
        adapter = BulkOutcomeMarketAdapter()
        restarted = ExactHorizonOutcomeCapture(
            evidence_store=reopened,
            market_adapter=adapter,
        )
        replay = restarted.tick(now=BASE + timedelta(minutes=30, seconds=7))
        assert replay.observations_appended == 0
        assert adapter.calls == []


def test_old_five_day_spec_and_observation_survive_more_than_five_thousand_later_rows(
    tmp_path: Path,
) -> None:
    target = _capture_target("old-five-day")
    plan = target["capture_plan"]
    assert isinstance(plan, dict)
    plan["session_calendar"] = _calendar_sessions()
    adapter = BulkOutcomeMarketAdapter()
    path = tmp_path / "capture-page-starvation.sqlite3"
    with EvidenceStore(path) as evidence:
        capture = ExactHorizonOutcomeCapture(evidence_store=evidence, market_adapter=adapter)
        capture.register_target(target, horizon="5D")
        for index in range(5001):
            evidence.append(
                EvidenceRecord(
                    identity=f"later-spec:{index}",
                    kind=OUTCOME_CAPTURE_SPEC_KIND,
                    symbol="SPY",
                    provider="TEST_ONLY",
                    source_id=f"later-spec:{index}",
                    published_at=BASE,
                    first_seen_at=BASE,
                    ingested_at=BASE,
                    observed_at=BASE,
                    payload={"schema": "unrelated.spec.v1", "index": index},
                )
            )

        first = capture.tick(now=_horizon_at("5D") + timedelta(seconds=1))
        assert first.observations_appended == 1

    restarted_adapter = BulkOutcomeMarketAdapter()
    with EvidenceStore(path) as reopened:
        for index in range(5001):
            reopened.append(
                EvidenceRecord(
                    identity=f"later-observation:{index}",
                    kind=OUTCOME_OBSERVATION_KIND,
                    symbol="SPY",
                    provider="TEST_ONLY",
                    source_id=f"later-observation:{index}",
                    published_at=BASE,
                    first_seen_at=BASE,
                    ingested_at=BASE,
                    observed_at=BASE,
                    payload={"schema": "unrelated.observation.v1", "index": index},
                )
            )
        restarted = ExactHorizonOutcomeCapture(
            evidence_store=reopened,
            market_adapter=restarted_adapter,
        )
        replay = restarted.tick(now=_horizon_at("5D") + timedelta(seconds=2))

        assert replay.records_skipped >= 1
        assert restarted_adapter.calls == []


def test_idle_capture_uses_one_integrity_scan_and_incremental_cursors(tmp_path: Path) -> None:
    class InstrumentedEvidenceStore(EvidenceStore):
        def __init__(self, path: Path) -> None:
            self.integrity_calls = 0
            self.page_calls = 0
            self.legacy_query_calls = 0
            super().__init__(path)

        def assert_integrity(self) -> None:
            self.integrity_calls += 1
            super().assert_integrity()

        def query_page(self, **kwargs):
            self.page_calls += 1
            return super().query_page(**kwargs)

        def query(self, **kwargs):
            self.legacy_query_calls += 1
            return super().query(**kwargs)

    clock = MutableClock(BASE)
    target = _capture_target("incremental-1")
    target["source_sequence"] = 1
    candidate_calls: list[int] = []

    def candidates(*, after_sequence: int = 0):
        candidate_calls.append(after_sequence)
        return (target,) if after_sequence < 1 else ()

    evidence = InstrumentedEvidenceStore(tmp_path / "incremental.sqlite3")
    try:
        adapter = BulkOutcomeMarketAdapter()
        capture = ExactHorizonOutcomeCapture(evidence_store=evidence, market_adapter=adapter)
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=candidates,
            prediction_targets=lambda *, after_sequence=0: (),
            calendar_provider=type("Calendar", (), {"snapshot": lambda self, **_: None})(),
            clock=clock,
        )
        coordinator.tick()
        page_calls_after_startup = evidence.page_calls
        for _ in range(5):
            coordinator.tick()

        assert evidence.integrity_calls == 1
        assert evidence.legacy_query_calls == 0
        assert evidence.page_calls >= page_calls_after_startup
        assert candidate_calls == [0, 1, 1, 1, 1, 1]

        clock.now = BASE + timedelta(minutes=30, seconds=1)
        due = coordinator.tick()
        assert due.observations_appended == 1
        assert evidence.integrity_calls == 1
    finally:
        evidence.close()


def test_transient_calendar_failure_retries_and_appends_ready_revision(tmp_path: Path) -> None:
    class Calendar:
        def __init__(self) -> None:
            self.calls = 0

        def snapshot(self, *, now: datetime):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary calendar failure")
            sessions = tuple(
                type(
                    "Session",
                    (),
                    {
                        "trading_date": datetime.fromisoformat(str(row["trading_date"])).date(),
                        "open_utc": row["open_at"],
                        "close_utc": row["close_at"],
                    },
                )()
                for row in _calendar_sessions()
            )
            calendar_hash = canonical_hash({"sessions": _calendar_sessions()})
            return type(
                "Snapshot",
                (),
                {
                    "status": type("Status", (), {"value": "READY"})(),
                    "sessions": sessions,
                    "observed_at": BASE - timedelta(minutes=5),
                    "calendar_hash": calendar_hash,
                    "source": "OFFICIAL_SESSION_CALENDAR",
                    "verify_hash": lambda self: True,
                },
            )()

    clock = MutableClock(BASE + timedelta(minutes=1))
    target = _capture_target("calendar-retry")
    target["source_sequence"] = 1
    with EvidenceStore(tmp_path / "calendar-retry.sqlite3") as evidence:
        capture = ExactHorizonOutcomeCapture(
            evidence_store=evidence,
            market_adapter=BulkOutcomeMarketAdapter(),
        )
        coordinator = OutcomeCaptureCoordinator(
            capture=capture,
            candidate_targets=lambda *, after_sequence=0: (target,) if after_sequence < 1 else (),
            prediction_targets=lambda *, after_sequence=0: (),
            calendar_provider=Calendar(),
            clock=clock,
        )

        coordinator.tick()
        first_rows = tuple(evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,)))
        session_close = [
            row for row in first_rows if row.record.payload["horizon"] == "SESSION_CLOSE"
        ]
        assert len(session_close) == 1
        assert session_close[0].record.payload["status"] == "WAITING"

        clock.now = BASE + timedelta(minutes=2)
        coordinator.tick()
        revised = [
            row
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
            if row.record.payload["horizon"] == "SESSION_CLOSE"
        ]
        assert [row.record.payload["status"] for row in revised] == [
            "WAITING",
            "READY",
        ]
        assert revised[-1].record.supersedes_id == revised[0].evidence_id


def test_unresolved_prediction_terminalizes_after_window_without_backfill(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE)
    target = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "terminal-prediction",
        "subject_hash": "d" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "e" * 64,
        "source_sequence": 1,
        "prediction_hash": "d" * 64,
        "independence_key": "terminal-event:SPY:30M",
        "binding_context": {
            "source_sequence": 1,
            "event_id": "terminal-event",
            "event_ids": ("terminal-event",),
            "independence_key": "terminal-event:SPY:30M",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "WAITING",
            "reason_codes": (
                "OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE",
            ),
        },
    }
    with EvidenceStore(tmp_path / "terminal-prediction.sqlite3") as evidence:
        adapter = BulkOutcomeMarketAdapter()
        coordinator = OutcomeCaptureCoordinator(
            capture=ExactHorizonOutcomeCapture(
                evidence_store=evidence,
                market_adapter=adapter,
            ),
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=lambda *, after_sequence=0: (
                ((target, "30M"),) if after_sequence < 1 else ()
            ),
            calendar_provider=type("Calendar", (), {"snapshot": lambda self, **_: None})(),
            clock=clock,
        )

        coordinator.tick()
        clock.now = BASE + timedelta(minutes=30, seconds=6)
        terminal = coordinator.tick()
        coordinator.tick()

        rows = tuple(evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,)))
        assert [row.record.payload["status"] for row in rows] == [
            "WAITING",
            "BLOCKED",
        ]
        assert rows[-1].record.payload["reason_codes"] == (
            "OUTCOME_CAPTURE_WINDOW_MISSED",
        )
        assert rows[-1].record.supersedes_id == rows[0].evidence_id
        assert terminal.status == "BLOCKED"
        assert adapter.calls == []
        assert coordinator._pending_predictions == {}


def test_candidate_after_prediction_horizon_never_binds_or_changes_prediction_baseline(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE)
    prediction = {
        "schema": "options_copilot.outcome_target.v1",
        "subject_kind": "PREDICTION",
        "subject_id": "hostile-future-prediction",
        "subject_hash": "a" * 64,
        "symbol": "SPY",
        "occurred_at": BASE,
        "thesis_hash": "b" * 64,
        "source_sequence": 1,
        "prediction_hash": "a" * 64,
        "independence_key": "hostile-event:SPY:30M",
        "binding_context": {
            "source_sequence": 1,
            "event_id": "hostile-event",
            "event_ids": ("hostile-event",),
            "independence_key": "hostile-event:SPY:30M",
        },
        "prediction_baseline_request": {
            "schema": "options_copilot.prediction_baseline_request.v1",
            "benchmark_symbol": "SPY",
        },
        "capture_plan": {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "WAITING",
            "reason_codes": (
                "OUTCOME_CAPTURE_COMBINATION_BASELINE_UNAVAILABLE",
            ),
        },
    }
    candidates: list[Mapping[str, object]] = []
    adapter = BulkOutcomeMarketAdapter()
    with EvidenceStore(tmp_path / "hostile-future-binding.sqlite3") as evidence:
        coordinator = OutcomeCaptureCoordinator(
            capture=ExactHorizonOutcomeCapture(
                evidence_store=evidence,
                market_adapter=adapter,
            ),
            candidate_targets=lambda *, after_sequence=0: tuple(
                item
                for item in candidates
                if int(item["binding_context"]["source_sequence"]) > after_sequence  # type: ignore[index]
            ),
            prediction_targets=lambda *, after_sequence=0: (
                ((prediction, "30M"),) if after_sequence < 1 else ()
            ),
            calendar_provider=type("Calendar", (), {"snapshot": lambda self, **_: None})(),
            clock=clock,
        )

        coordinator.tick()
        candidates.append(
            _binding_candidate(
                "hostile-2",
                occurred_at=BASE + timedelta(minutes=30, seconds=6),
                event_ids=("hostile-event",),
            )
        )
        clock.now = BASE + timedelta(minutes=30, seconds=6)
        coordinator.tick()

        prediction_specs = [
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
            if row.record.payload["subject_kind"] == "PREDICTION"
        ]
        assert prediction_specs[-1]["status"] == "BLOCKED"
        assert "prediction_candidate_binding" not in prediction_specs[-1]
        assert prediction_specs[-1]["prediction_baseline"] == prediction_specs[-2]["prediction_baseline"]
        assert len(adapter.calls) == 1


def test_real_shadow_cursor_verifies_once_advances_over_idle_and_new_prediction(
    tmp_path: Path,
) -> None:
    ledger = ShadowLearningLedger(tmp_path / "shadow-cursor.sqlite3")
    try:
        prediction_ids = _seed_predictions(ledger)
        prior = ledger.get_prediction(prediction_ids[0])
        integrity_calls = 0
        replay_calls = 0
        verify = ledger.assert_integrity
        replays = ledger.query_replays

        def counted_integrity() -> None:
            nonlocal integrity_calls
            integrity_calls += 1
            verify()

        def counted_replays(**kwargs):
            nonlocal replay_calls
            replay_calls += 1
            return replays(**kwargs)

        ledger.assert_integrity = counted_integrity  # type: ignore[method-assign]
        ledger.query_replays = counted_replays  # type: ignore[method-assign]
        provider_type = getattr(
            outcome_processor_module,
            "ShadowPredictionTargetCursor",
            None,
        )
        assert provider_type is not None
        provider = provider_type(ledger)

        startup = provider(after_sequence=0)
        for _ in range(5):
            assert provider(after_sequence=0) == ()

        assert len(startup) == len(OUTCOME_HORIZONS)
        assert integrity_calls == 1
        assert replay_calls == 0

        ledger.record_prediction(
            "prediction-cursor-new",
            prior.thesis_id,
            evidence_ids=tuple(
                binding.evidence_id for binding in prior.evidence_bindings
            ),
            prediction={
                "schema": "options_copilot.test_prediction.v1",
                "symbol": "SPY",
                "horizon": "30M",
                "target_rule": OUTCOME_TARGET_RULES["30M"],
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            },
            predicted_at=BASE + timedelta(minutes=1),
            independence_key="cursor-event:SPY:30M",
        )
        calls_after_append = integrity_calls
        appended = provider(after_sequence=0)
        assert [item[0]["subject_id"] for item in appended] == [
            "prediction-cursor-new"
        ]
        assert provider(after_sequence=0) == ()
        assert integrity_calls == calls_after_append

        ledger._connection.execute(
            "DROP TRIGGER shadow_learning_records_no_update"
        )
        with pytest.raises(Exception, match="trigger"):
            provider(after_sequence=0)
    finally:
        ledger.close()


def _production_news_prediction_record(
    *,
    body_changes: Mapping[str, object] | None = None,
    record_changes: Mapping[str, object] | None = None,
) -> SimpleNamespace:
    advisory_id = "news-advisory:" + "b" * 64
    body: dict[str, object] = {
        "schema": NEWS_SHADOW_PREDICTION_SCHEMA,
        "advisory_id": advisory_id,
        "event_id": "jin10-cpi-outcome-gate",
        "symbol": "SPY",
        "horizon": "30M",
        "target_rule": OUTCOME_TARGET_RULES["30M"],
        "classification": {"direction": "BULLISH"},
        "symbol_binding": MarketProxyBinding(
            event_category="US_INFLATION",
            source="JIN10",
            proxy_symbol="SPY",
        ).as_dict(),
        "model_visible_snapshot_hash": "c" * 64,
        "prediction_set_predicted_at": BASE.isoformat(),
        "prediction_baseline_hash": canonical_hash(
            {
                "schema": "options_copilot.news_prediction_baseline.v2",
                "advisory_id": advisory_id,
                "event_id": "jin10-cpi-outcome-gate",
                "symbol": "SPY",
                "model_visible_snapshot_hash": "c" * 64,
                "prediction_set_predicted_at": BASE,
            }
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    body.update(body_changes or {})
    record = {
        "prediction": body,
        "prediction_id": news_shadow_prediction_id(advisory_id, "30m"),
        "sequence": 1,
        "content_hash": "d" * 64,
        "independence_key": "news-event:" + "e" * 64,
        "predicted_at": BASE,
        "thesis_hash": "f" * 64,
    }
    record.update(record_changes or {})
    return SimpleNamespace(**record)


def _runtime_prediction_targets(record: SimpleNamespace):
    runtime = object.__new__(OptionsCopilotRuntime)
    runtime.shadow_learning = SimpleNamespace(
        query_replays=lambda **_: (SimpleNamespace(prediction=record),)
    )
    return runtime._prediction_outcome_targets()


def test_production_news_prediction_projects_through_both_outcome_entrypoints() -> None:
    record = _production_news_prediction_record()

    cursor_targets = outcome_processor_module._project_prediction_targets((record,))
    runtime_targets = _runtime_prediction_targets(record)

    assert len(cursor_targets) == 1
    assert len(runtime_targets) == 1
    assert cursor_targets[0][0]["subject_id"] == record.prediction_id
    assert runtime_targets[0][0]["subject_id"] == record.prediction_id


def test_prediction_baseline_window_starts_when_prediction_becomes_durable(
    tmp_path: Path,
) -> None:
    appended_at = BASE + timedelta(seconds=12)
    record = _production_news_prediction_record(
        record_changes={"appended_at": appended_at}
    )
    target = outcome_processor_module._project_prediction_targets((record,))[0]
    adapter = BulkOutcomeMarketAdapter()

    assert datetime.fromisoformat(str(target[0]["occurred_at"])) == BASE
    assert datetime.fromisoformat(str(target[0]["baseline_at"])) == appended_at

    with EvidenceStore(tmp_path / "durable-baseline.sqlite3") as evidence:
        coordinator = OutcomeCaptureCoordinator(
            capture=ExactHorizonOutcomeCapture(
                evidence_store=evidence,
                market_adapter=adapter,
            ),
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=lambda *, after_sequence=0: (
                (target,) if after_sequence < 1 else ()
            ),
            calendar_provider=type(
                "Calendar",
                (),
                {"snapshot": lambda self, **_: None},
            )(),
            clock=lambda: appended_at + timedelta(seconds=1),
        )

        result = coordinator.tick()
        specs = tuple(
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
        )

    assert result.status == "WAITING"
    assert specs[-1]["prediction_baseline"]["status"] == "AVAILABLE"
    assert adapter.calls[0][0]["subject_id"] == record.prediction_id


def test_durable_prediction_set_reuses_one_point_in_time_baseline(
    tmp_path: Path,
) -> None:
    first_appended_at = BASE + timedelta(seconds=6)
    second_appended_at = BASE + timedelta(seconds=12)
    first = _production_news_prediction_record(
        record_changes={"appended_at": first_appended_at}
    )
    advisory_id = str(first.prediction["advisory_id"])
    second = _production_news_prediction_record(
        body_changes={
            "horizon": "SESSION_CLOSE",
            "target_rule": OUTCOME_TARGET_RULES["SESSION_CLOSE"],
        },
        record_changes={
            "prediction_id": news_shadow_prediction_id(
                advisory_id,
                "session-close",
            ),
            "sequence": 2,
            "content_hash": "a" * 64,
            "appended_at": second_appended_at,
        },
    )
    records = [first]

    def predictions(*, after_sequence: int = 0):
        return tuple(
            item
            for item in outcome_processor_module._project_prediction_targets(
                records
            )
            if item[0]["source_sequence"] > after_sequence
        )

    clock = MutableClock(first_appended_at + timedelta(seconds=1))
    adapter = BulkOutcomeMarketAdapter()
    with EvidenceStore(tmp_path / "shared-durable-baseline.sqlite3") as evidence:
        coordinator = OutcomeCaptureCoordinator(
            capture=ExactHorizonOutcomeCapture(
                evidence_store=evidence,
                market_adapter=adapter,
            ),
            candidate_targets=lambda *, after_sequence=0: (),
            prediction_targets=predictions,
            calendar_provider=type(
                "Calendar",
                (),
                {"snapshot": lambda self, **_: None},
            )(),
            clock=clock,
        )

        coordinator.tick()
        records.append(second)
        clock.now = second_appended_at + timedelta(seconds=1)
        coordinator.tick()
        specs = tuple(
            row.record.payload
            for row in evidence.iter_verified(kinds=(OUTCOME_CAPTURE_SPEC_KIND,))
            if isinstance(row.record.payload.get("prediction_baseline"), Mapping)
        )

    baselines = {
        canonical_hash(row["prediction_baseline"])
        for row in specs
    }
    assert len(adapter.calls) == 1
    assert len(baselines) == 1
    assert {row["subject_id"] for row in specs} == {
        first.prediction_id,
        second.prediction_id,
    }


@pytest.mark.parametrize(
    ("body_changes", "record_changes"),
    (
        ({"target_rule": "WRONG_TARGET_RULE"}, {}),
        ({"symbol_binding": {"mapping_version": "STALE"}}, {}),
        ({"model_visible_snapshot_hash": "not-a-sha256"}, {}),
        ({"decision_authority": "PRIMARY"}, {}),
        ({"approval_eligible": True}, {}),
        ({"instruction_creation_allowed": True}, {}),
        ({"order_allowed": True}, {}),
        ({"classification": None}, {}),
        ({"event_id": ""}, {}),
        ({}, {"independence_key": ""}),
        ({}, {"prediction_id": "wrong-prediction-id"}),
    ),
)
def test_production_news_prediction_tampering_is_rejected_by_both_outcome_entrypoints(
    body_changes: Mapping[str, object],
    record_changes: Mapping[str, object],
) -> None:
    record = _production_news_prediction_record(
        body_changes=body_changes,
        record_changes=record_changes,
    )

    assert outcome_processor_module._project_prediction_targets((record,)) == ()
    assert _runtime_prediction_targets(record) == ()


def test_malformed_v2_prediction_is_excluded_by_scheduled_processor(
    tmp_path: Path,
) -> None:
    clock = MutableClock(BASE + timedelta(hours=1))
    shadow = ShadowLearningLedger(tmp_path / "malformed-v2.sqlite3", clock=clock)
    recorder = OutcomeRecorder(tmp_path / "malformed-v2-outcomes.sqlite3", clock=clock)
    record = _production_news_prediction_record(
        body_changes={"decision_authority": "PRIMARY"}
    )
    thesis = shadow.record_thesis(
        "malformed-v2-thesis",
        champion_version="champion-v1",
        challenger_version="deepseek-news-advisory-v2",
        thesis={"purpose": "write-boundary validation"},
        created_at=BASE - timedelta(hours=1),
    )
    evidence = shadow.record_evidence(
        "malformed-v2-evidence",
        thesis.thesis_id,
        source="TEST_ONLY",
        evidence={"symbol": "SPY"},
        published_at=BASE - timedelta(minutes=2),
        first_seen_at=BASE - timedelta(minutes=1),
    )
    shadow.record_prediction(
        record.prediction_id,
        thesis.thesis_id,
        evidence_ids=(evidence.evidence_id,),
        prediction=record.prediction,
        predicted_at=record.predicted_at,
        independence_key=record.independence_key,
    )

    class RejectObservation:
        def observe(self, *args, **kwargs):
            raise AssertionError("malformed v2 must not request an observation")

    try:
        result = ImmutableOutcomeProcessor(
            shadow_ledger=shadow,
            candidate_recorder=recorder,
            candidate_targets=lambda: (),
            observation_provider=RejectObservation(),
            clock=clock,
        ).process()

        assert result.records_rejected == 1
        assert "AUTHORITY_INVALID" in result.reason_codes
        assert shadow.record_counts()["outcomes"] == 0
    finally:
        recorder.close()
        shadow.close()
