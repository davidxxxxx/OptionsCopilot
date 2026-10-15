"""Reconcile durable outcome evidence into the existing immutable ledgers.

The processor never acquires market data. It reads already-appended evidence,
derives maturity only from bound horizon evidence, and writes research-only
prediction outcomes plus diagnostic candidate outcomes.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import heapq
import inspect
import threading
from typing import Protocol

from options_copilot.learning_shadow import ShadowLearningLedger
from options_copilot.news.shadow_prediction import (
    projectable_shadow_prediction_identity,
    shadow_prediction_exclusion_reason,
)
from options_copilot.ranking.store import RankingStore
from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)
from options_copilot.storage.evidence import EvidenceRecord, StoredEvidence

from .outcomes import (
    GENESIS_HASH,
    OUTCOME_HORIZONS,
    OUTCOME_TARGET_RULES,
    OutcomeIdentityConflict,
    OutcomeRecorder,
    OutcomeValidationError,
    normalize_outcome_observation,
    normalize_bound_outcome_result,
    resolve_outcome_horizon,
)
from .progress import OutcomeProgressState, OutcomeProgressStore


OUTCOME_OBSERVATION_KIND = "OUTCOME_OBSERVATION"
OUTCOME_CAPTURE_SPEC_KIND = "OUTCOME_CAPTURE_SPEC"
_CAPTURE_WINDOW = timedelta(seconds=5)
_CAPTURE_BATCH_SYMBOL_LIMIT = 50
_CAPTURE_BATCH_CONTRACT_LIMIT = 50
_TERMINAL_CAPTURE_REASONS = frozenset(
    {
        "OUTCOME_CAPTURE_WINDOW_MISSED",
        "OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED",
        "OUTCOME_CAPTURE_PLAN_INVALID",
        "OUTCOME_CAPTURE_LEGS_INVALID",
        "OUTCOME_CAPTURE_SPEC_INVALID",
        "OUTCOME_OBSERVATION_CONFLICTED",
    }
)
_AUTHORITATIVE_TERMINAL_CAPTURE_REASONS = frozenset(
    {
        "OUTCOME_CAPTURE_WINDOW_MISSED",
        "OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED",
    }
)
_CAPTURE_SPEC_OPTIONAL_FIELDS = frozenset(
    {
        "prediction_candidate_binding",
        "prediction_baseline_request",
        "prediction_baseline",
        "prediction_set_hash",
        "predicted_direction",
        "thesis_hash",
    }
)
_CAPTURE_SPEC_MODELED_REVISION_FIELDS = frozenset(
    {
        "status",
        "reason_codes",
        "registered_at",
        "revision",
        "prior_capture_spec_hash",
        "capture_spec_hash",
        "prediction_baseline",
        "prediction_candidate_binding",
        "capture_plan",
    }
)


class OutcomeObservationProvider(Protocol):
    """Read one already-durable observation without fetching external data."""

    def observe(
        self,
        target: Mapping[str, object],
        *,
        horizon: str,
        as_of: datetime,
    ) -> Mapping[str, object] | None:
        ...


class RankingOutcomeTargetCursor:
    """Stateful ranking source: verify once, then consume anchored pages."""

    def __init__(self, store: RankingStore) -> None:
        if not isinstance(store, RankingStore):
            raise TypeError("store must be a RankingStore")
        self._store = store
        self._initialized = False
        self._snapshot_sequence = 0
        self._snapshot_hash = GENESIS_HASH
        self._lock = threading.RLock()

    def __call__(
        self,
        *,
        after_sequence: int = 0,
    ) -> tuple[Mapping[str, object], ...]:
        del after_sequence
        with self._lock:
            if not self._initialized:
                targets, sequence, head_hash = (
                    self._store.open_outcome_target_cursor()
                )
                self._snapshot_sequence = sequence
                self._snapshot_hash = head_hash
                self._initialized = True
                return targets
            output: list[Mapping[str, object]] = []
            while True:
                targets, sequence, head_hash = (
                    self._store.outcome_target_cursor_page(
                        after_snapshot_sequence=self._snapshot_sequence,
                        previous_snapshot_hash=self._snapshot_hash,
                        limit=500,
                    )
                )
                output.extend(targets)
                advanced = sequence - self._snapshot_sequence
                self._snapshot_sequence = sequence
                self._snapshot_hash = head_hash
                if advanced < 500:
                    break
            return tuple(output)


class ShadowPredictionTargetCursor:
    """Stateful prediction source with one startup verification."""

    def __init__(self, ledger: ShadowLearningLedger) -> None:
        if not isinstance(ledger, ShadowLearningLedger):
            raise TypeError("ledger must be a ShadowLearningLedger")
        self._ledger = ledger
        self._initialized = False
        self._sequence = 0
        self._head_hash = GENESIS_HASH
        self._lock = threading.RLock()

    def __call__(
        self,
        *,
        after_sequence: int = 0,
    ) -> tuple[tuple[Mapping[str, object], str], ...]:
        del after_sequence
        with self._lock:
            if not self._initialized:
                predictions, sequence, head_hash = (
                    self._ledger.open_prediction_cursor()
                )
                self._sequence = sequence
                self._head_hash = head_hash
                self._initialized = True
                return _project_prediction_targets(predictions)
            output: list[tuple[Mapping[str, object], str]] = []
            while True:
                predictions, sequence, head_hash = (
                    self._ledger.prediction_cursor_page(
                        after_sequence=self._sequence,
                        previous_hash=self._head_hash,
                        limit=5000,
                    )
                )
                output.extend(_project_prediction_targets(predictions))
                advanced = sequence - self._sequence
                self._sequence = sequence
                self._head_hash = head_hash
                if advanced < 5000:
                    break
            return tuple(output)


class OutcomeMarketAdapter(Protocol):
    """Acquire one bounded batch through an injected read-only market adapter."""

    def observe(
        self,
        specs: tuple[Mapping[str, object], ...],
        *,
        expected_at: datetime,
    ) -> Mapping[str, object]:
        ...


class OutcomeProcessingError(RuntimeError):
    pass


class OutcomeObservationUnavailable(OutcomeProcessingError):
    pass


class OutcomeObservationTerminal(OutcomeObservationUnavailable):
    """The bound capture attempt ended without an observable outcome."""


@dataclass(frozen=True, slots=True)
class OutcomeCaptureRegistrationResult:
    status: str
    subject_id: str
    horizon: str
    horizon_at: datetime | None
    reason_codes: tuple[str, ...]
    capture_spec_hash: str | None
    decision_authority: str = "SUPPORTING_ONLY"
    order_allowed: bool = False

    def __post_init__(self) -> None:
        if self.status not in {
            "REGISTERED",
            "ALREADY_REGISTERED",
            "REVISED",
            "WAITING",
            "BLOCKED",
        }:
            raise ValueError("invalid outcome capture registration status")
        if self.horizon_at is not None:
            object.__setattr__(
                self,
                "horizon_at",
                utc_datetime(self.horizon_at, field="horizon_at"),
            )


@dataclass(frozen=True, slots=True)
class OutcomeCaptureResult:
    status: str
    checked_at: datetime
    specs_seen: int
    specs_due: int
    observations_appended: int
    records_blocked: int
    records_skipped: int
    reason_codes: tuple[str, ...]
    decision_authority: str = "SUPPORTING_ONLY"
    affects_production_weights: bool = False
    affects_eligibility: bool = False
    affects_risk: bool = False
    affects_ranking: bool = False
    approval_eligible: bool = False
    instruction_creation_allowed: bool = False
    order_allowed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "checked_at",
            utc_datetime(self.checked_at, field="checked_at"),
        )
        if self.status not in {"COMPLETED", "WAITING", "BLOCKED", "DEGRADED"}:
            raise ValueError("invalid outcome capture status")


@dataclass(frozen=True, slots=True)
class OutcomeProcessingResult:
    status: str
    checked_at: datetime
    subjects_seen: int
    horizons_requested: int
    due_count: int
    records_appended: int
    records_superseded: int
    records_skipped: int
    records_blocked: int
    records_rejected: int
    reason_codes: tuple[str, ...]
    candidate_ledger_head_hash: str
    shadow_ledger_head_hash: str
    manifest_hash: str
    processing_hash: str
    prediction_cursor: int = 0
    candidate_cursor: int = 0
    remaining_count: int = 0
    progress_sequence: int = 0
    progress_hash: str | None = None
    bounded: bool = False
    stopped: bool = False
    stop_reason: str | None = None
    decision_authority: str = "SUPPORTING_ONLY"
    calibration_only: bool = True
    research_priority_only: bool = True
    affects_production_weights: bool = False
    affects_eligibility: bool = False
    affects_risk: bool = False
    affects_ranking: bool = False
    approval_eligible: bool = False
    instruction_creation_allowed: bool = False
    order_allowed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "checked_at",
            utc_datetime(self.checked_at, field="checked_at"),
        )
        if self.status not in {
            "COMPLETED",
            "WAITING_FOR_OBSERVATIONS",
            "BLOCKED",
            "DEGRADED",
        }:
            raise ValueError("invalid outcome processing status")

    def identity_document(self) -> dict[str, object]:
        return {
            field.name: (
                datetime_text(value) if isinstance(value, datetime) else value
            )
            for field in fields(self)
            if field.name != "processing_hash"
            for value in (getattr(self, field.name),)
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.identity_document(), "processing_hash": self.processing_hash}


class EvidenceStoreOutcomeObservationProvider:
    """Read exact outcome identities from the existing append-only evidence store."""

    def __init__(self, evidence_store: object) -> None:
        if not callable(getattr(evidence_store, "query", None)):
            raise TypeError("evidence_store must expose query")
        if not callable(getattr(evidence_store, "assert_integrity", None)):
            raise TypeError("evidence_store must expose assert_integrity")
        if not callable(getattr(evidence_store, "verified_head_sequence", None)):
            raise TypeError("evidence_store must expose verified_head_sequence")
        self._evidence_store = evidence_store
        self._batch_lock = threading.RLock()
        self._batch_active = False
        self._batch_as_of: datetime | None = None
        self._batch_head_sequence = 0
        self._batch_error: str | None = None
        self._terminal_capture_reasons: dict[str, str] = {}

    def begin_verified_batch(self, *, as_of: datetime) -> None:
        """Verify and freeze one evidence prefix for a processor run.

        Integrity verification is intentionally O(ledger) and therefore must
        happen once per bounded run, not once per work item.  Every subsequent
        lookup is capped at the verified prefix, so concurrent evidence appends
        cannot change the point-in-time batch being reconciled.
        """

        checked_at = utc_datetime(as_of, field="as_of")
        with self._batch_lock:
            self._batch_active = True
            self._batch_as_of = checked_at
            self._batch_head_sequence = 0
            self._batch_error = None
            self._terminal_capture_reasons = {}
            try:
                self._batch_head_sequence = int(
                    self._evidence_store.verified_head_sequence(
                        first_seen_at_or_before=checked_at,
                    )
                )
                cursor = 0
                capture_rows: list[StoredEvidence] = []
                while cursor < self._batch_head_sequence:
                    rows = tuple(
                        self._evidence_store.query_page(
                            after_sequence=cursor,
                            at_or_before_sequence=self._batch_head_sequence,
                            first_seen_at_or_before=checked_at,
                            kinds=(OUTCOME_CAPTURE_SPEC_KIND,),
                            limit=5000,
                        )
                    )
                    if not rows:
                        break
                    capture_rows.extend(rows)
                    cursor = rows[-1].sequence
                for capture_key, rows in _capture_spec_rows_by_key(
                    tuple(capture_rows)
                ).items():
                    if rows is None:
                        continue
                    reason = _verified_terminal_capture_reason(
                        rows,
                        capture_key=capture_key,
                    )
                    if reason is not None:
                        self._terminal_capture_reasons[capture_key] = reason
            except Exception:
                self._batch_error = "OUTCOME_OBSERVATION_STORE_CORRUPT"
                self._batch_head_sequence = 0
                self._terminal_capture_reasons = {}

    def end_verified_batch(self) -> None:
        """Release the frozen prefix after one processor run."""

        with self._batch_lock:
            self._batch_active = False
            self._batch_as_of = None
            self._batch_head_sequence = 0
            self._batch_error = None
            self._terminal_capture_reasons = {}

    def terminal_reason(
        self,
        target: Mapping[str, object],
        *,
        horizon: str,
        as_of: datetime,
    ) -> str | None:
        """Return the terminal capture reason from the active verified prefix."""

        checked_at = utc_datetime(as_of, field="as_of")
        with self._batch_lock:
            if not self._batch_active or self._batch_as_of != checked_at:
                return None
            if self._batch_error is not None:
                return None
            return self._terminal_capture_reasons.get(
                _capture_spec_identity(target, horizon)
            )

    @staticmethod
    def identity(target: Mapping[str, object], horizon: str) -> str:
        return "outcome-observation:" + canonical_hash(
            {
                "subject_kind": target.get("subject_kind"),
                "subject_id": target.get("subject_id"),
                "subject_hash": target.get("subject_hash"),
                "horizon": horizon,
            }
        )

    def observe(
        self,
        target: Mapping[str, object],
        *,
        horizon: str,
        as_of: datetime,
    ) -> Mapping[str, object] | None:
        checked_at = utc_datetime(as_of, field="as_of")
        identity = self.identity(target, horizon)
        with self._batch_lock:
            batch_active = self._batch_active
            if batch_active and self._batch_as_of != checked_at:
                raise OutcomeObservationUnavailable(
                    "OUTCOME_OBSERVATION_BATCH_TIME_CONFLICT"
                )
            if batch_active and self._batch_error is not None:
                raise OutcomeObservationUnavailable(self._batch_error)
            if batch_active:
                rows = tuple(
                    self._evidence_store.query_page(
                        after_sequence=0,
                        at_or_before_sequence=self._batch_head_sequence,
                        first_seen_at_or_before=checked_at,
                        identities=(identity,),
                        kinds=(OUTCOME_OBSERVATION_KIND,),
                        limit=2,
                    )
                )
            else:
                try:
                    self._evidence_store.assert_integrity()
                except Exception:
                    raise OutcomeObservationUnavailable(
                        "OUTCOME_OBSERVATION_STORE_CORRUPT"
                    ) from None
                rows = tuple(
                    self._evidence_store.query(
                        first_seen_at_or_before=checked_at,
                        identities=(identity,),
                        kinds=(OUTCOME_OBSERVATION_KIND,),
                        limit=2,
                    )
                )
        if not rows:
            terminal_reason = self.terminal_reason(
                target,
                horizon=horizon,
                as_of=checked_at,
            )
            if terminal_reason is not None:
                raise OutcomeObservationTerminal(terminal_reason)
            return None
        if len(rows) != 1 or getattr(rows[0], "status", None) != "ACTIVE":
            raise OutcomeObservationUnavailable("OUTCOME_OBSERVATION_CONFLICTED")
        stored = rows[0]
        record = getattr(stored, "record", None)
        payload = getattr(record, "payload", None)
        if not isinstance(payload, Mapping):
            raise OutcomeObservationUnavailable("OUTCOME_OBSERVATION_INVALID")
        observed_at = getattr(record, "observed_at", None)
        if not isinstance(observed_at, datetime) or observed_at > checked_at:
            raise OutcomeObservationUnavailable("OUTCOME_OBSERVATION_TIME_TRAVEL")
        body = thaw_json(payload)
        if not isinstance(body, dict):
            raise OutcomeObservationUnavailable("OUTCOME_OBSERVATION_INVALID")
        if "ledger_binding" in body:
            raise OutcomeObservationUnavailable("OUTCOME_OBSERVATION_BINDING_FORGED")
        body["ledger_binding"] = {
            "status": "ACTIVE",
            "evidence_id": str(getattr(stored, "evidence_id")),
            "identity": str(getattr(record, "identity")),
            "content_hash": str(getattr(stored, "content_hash")),
            "row_hash": str(getattr(stored, "row_hash")),
            "provider": str(getattr(record, "provider")),
            "source_id": str(getattr(record, "source_id")),
            "observed_at": observed_at,
        }
        return body


def _adjust_count(counts: dict[str, int], key: str, delta: int) -> None:
    updated = counts.get(key, 0) + delta
    if updated <= 0:
        counts.pop(key, None)
    else:
        counts[key] = updated


class ExactHorizonOutcomeCapture:
    """Persist capture intent and acquire only inside the bound five-second window."""

    def __init__(
        self,
        *,
        evidence_store: object,
        market_adapter: OutcomeMarketAdapter,
    ) -> None:
        if not callable(getattr(evidence_store, "append", None)):
            raise TypeError("evidence_store must expose append")
        if not callable(getattr(evidence_store, "query_page", None)):
            raise TypeError("evidence_store must expose query_page")
        if not callable(getattr(evidence_store, "iter_verified", None)):
            raise TypeError("evidence_store must expose iter_verified")
        if not callable(getattr(evidence_store, "assert_integrity", None)):
            raise TypeError("evidence_store must expose assert_integrity")
        if not callable(getattr(market_adapter, "observe", None)):
            raise TypeError("market_adapter must expose observe")
        self._evidence_store = evidence_store
        self._market_adapter = market_adapter
        self._lock = threading.RLock()
        self._initialized = False
        self._cursor = 0
        self._latest_specs: dict[str, StoredEvidence] = {}
        self._observation_status: dict[str, str] = {}
        self._due_heap: list[tuple[datetime, int, str]] = []
        self._durable_counts_lock = threading.Lock()
        self._durable_status_counts: dict[str, int] = {}
        self._durable_blocker_counts: dict[str, int] = {}

    @property
    def next_due_at(self) -> datetime | None:
        with self._lock:
            while self._due_heap:
                due_at, sequence, capture_key = self._due_heap[0]
                row = self._latest_specs.get(capture_key)
                if row is not None and row.sequence == sequence:
                    return due_at
                heapq.heappop(self._due_heap)
        return None

    def durable_counts(self) -> Mapping[str, Mapping[str, int]]:
        """Return an incrementally maintained read model without a ledger scan."""

        if not self._initialized:
            with self._lock:
                self._refresh_store()
        with self._durable_counts_lock:
            return {
                "durable_status_counts": dict(
                    sorted(self._durable_status_counts.items())
                ),
                "durable_blocker_counts": dict(
                    sorted(self._durable_blocker_counts.items())
                ),
            }

    def prediction_baseline(
        self,
        target: Mapping[str, object],
        horizon: str,
    ) -> Mapping[str, object] | None:
        capture_key = _capture_spec_identity(target, str(horizon).upper())
        with self._lock:
            row = self._latest_specs.get(capture_key)
            value = None if row is None else row.record.payload.get(
                "prediction_baseline"
            )
        return value if isinstance(value, Mapping) and value.get("status") == "AVAILABLE" else None

    def shared_prediction_baseline(
        self,
        target: Mapping[str, object],
    ) -> Mapping[str, object] | None:
        """Return one event-level point-in-time baseline shared by all horizons."""

        prediction_set_hash = str(target.get("prediction_set_hash", ""))
        with self._lock:
            matches: list[tuple[int, Mapping[str, object]]] = []
            for row in self._latest_specs.values():
                payload = row.record.payload
                baseline = payload.get("prediction_baseline")
                if (
                    (
                        bool(prediction_set_hash)
                        and payload.get("prediction_set_hash")
                        == prediction_set_hash
                    )
                    or (
                        not prediction_set_hash
                        and payload.get("subject_kind")
                        == target.get("subject_kind")
                        and payload.get("subject_id") == target.get("subject_id")
                        and payload.get("subject_hash")
                        == target.get("subject_hash")
                    )
                ) and isinstance(baseline, Mapping) and baseline.get(
                    "status"
                ) == "AVAILABLE":
                    matches.append((row.sequence, baseline))
        if not matches:
            return None
        return max(matches, key=lambda item: item[0])[1]

    def target_horizon_at(
        self,
        target: Mapping[str, object],
        horizon: str,
    ) -> datetime | None:
        capture_key = _capture_spec_identity(target, str(horizon).upper())
        with self._lock:
            row = self._latest_specs.get(capture_key)
            value = None if row is None else row.record.payload.get("horizon_at")
        return None if value is None else _time_from(value)

    def target_status(
        self,
        target: Mapping[str, object],
        horizon: str,
    ) -> str | None:
        capture_key = _capture_spec_identity(target, str(horizon).upper())
        with self._lock:
            row = self._latest_specs.get(capture_key)
            value = None if row is None else row.record.payload.get("status")
        return None if value is None else str(value)

    def register_target(
        self,
        target: Mapping[str, object],
        *,
        horizon: str,
        registered_at: datetime | None = None,
    ) -> OutcomeCaptureRegistrationResult:
        checked_horizon = str(horizon).upper()
        subject_id = str(target.get("subject_id", "")) if isinstance(target, Mapping) else ""
        if checked_horizon not in OUTCOME_HORIZONS:
            return OutcomeCaptureRegistrationResult(
                status="BLOCKED",
                subject_id=subject_id,
                horizon=checked_horizon,
                horizon_at=None,
                reason_codes=("OUTCOME_HORIZON_INVALID",),
                capture_spec_hash=None,
            )
        try:
            checked_target = _capture_target(target)
        except Exception as exc:
            return OutcomeCaptureRegistrationResult(
                status="BLOCKED",
                subject_id=subject_id,
                horizon=checked_horizon,
                horizon_at=None,
                reason_codes=(_reason(exc),),
                capture_spec_hash=None,
            )

        occurred_at = _time_from(checked_target["occurred_at"])
        baseline_at = _time_from(checked_target.get("baseline_at", occurred_at))
        checked_registered_at = (
            occurred_at
            if registered_at is None
            else utc_datetime(registered_at, field="registered_at")
        )
        if checked_registered_at < occurred_at or checked_registered_at < baseline_at:
            return OutcomeCaptureRegistrationResult(
                status="BLOCKED",
                subject_id=str(checked_target["subject_id"]),
                horizon=checked_horizon,
                horizon_at=None,
                reason_codes=("OUTCOME_CAPTURE_REGISTRATION_TIME_TRAVEL",),
                capture_spec_hash=None,
            )
        spec = _build_capture_spec(
            checked_target,
            checked_horizon,
            registered_at=checked_registered_at,
        )
        capture_key = _capture_spec_identity(checked_target, checked_horizon)
        capture_spec_hash = str(spec["capture_spec_hash"])
        horizon_at = (
            None if spec["horizon_at"] is None else _time_from(spec["horizon_at"])
        )
        with self._lock:
            try:
                self._refresh_store()
            except Exception:
                return OutcomeCaptureRegistrationResult(
                    status="BLOCKED",
                    subject_id=str(checked_target["subject_id"]),
                    horizon=checked_horizon,
                    horizon_at=horizon_at,
                    reason_codes=("OUTCOME_CAPTURE_STORE_CORRUPT",),
                    capture_spec_hash=capture_spec_hash,
                )
            existing_row = self._latest_specs.get(capture_key)
            if existing_row is not None:
                existing = existing_row.record.payload
                existing_baseline = existing.get("prediction_baseline")
                if (
                    isinstance(existing_baseline, Mapping)
                    and existing_baseline.get("status") == "AVAILABLE"
                    and not isinstance(spec.get("prediction_baseline"), Mapping)
                ):
                    updated = thaw_json(spec)
                    if not isinstance(updated, dict):
                        raise OutcomeProcessingError(
                            "OUTCOME_CAPTURE_SPEC_INVALID"
                        )
                    updated["prediction_baseline"] = thaw_json(
                        existing_baseline
                    )
                    plan = updated.get("capture_plan")
                    if isinstance(plan, dict) and plan.get("status") == "READY":
                        plan["underlying"] = thaw_json(
                            existing_baseline["underlying"]
                        )
                        plan["benchmark"] = thaw_json(
                            existing_baseline["benchmark"]
                        )
                        plan["benchmark_symbol"] = existing_baseline[
                            "benchmark_symbol"
                        ]
                    updated.pop("capture_spec_hash", None)
                    updated["capture_spec_hash"] = canonical_hash(updated)
                    frozen_updated = freeze_json(updated)
                    assert isinstance(frozen_updated, Mapping)
                    spec = frozen_updated
                    capture_spec_hash = str(spec["capture_spec_hash"])
                if any(
                    existing.get(name) != spec.get(name)
                    for name in (
                        "subject_kind",
                        "subject_id",
                        "subject_hash",
                        "horizon",
                        "observation_identity",
                    )
                ):
                    return OutcomeCaptureRegistrationResult(
                        status="BLOCKED",
                        subject_id=str(checked_target["subject_id"]),
                        horizon=checked_horizon,
                        horizon_at=horizon_at,
                        reason_codes=("OUTCOME_CAPTURE_SPEC_CONFLICTED",),
                        capture_spec_hash=capture_spec_hash,
                    )
                retry_horizon_at = existing.get("horizon_at") or spec.get(
                    "horizon_at"
                )
                if (
                    existing.get("status") == "WAITING"
                    and retry_horizon_at is not None
                    and checked_registered_at
                    > _time_from(retry_horizon_at) + _CAPTURE_WINDOW
                ):
                    missed = thaw_json(spec)
                    if not isinstance(missed, dict):
                        raise OutcomeProcessingError(
                            "OUTCOME_CAPTURE_SPEC_INVALID"
                        )
                    missed["status"] = "BLOCKED"
                    missed["reason_codes"] = (
                        "OUTCOME_CAPTURE_WINDOW_MISSED",
                    )
                    missed.pop("capture_spec_hash", None)
                    missed["capture_spec_hash"] = canonical_hash(missed)
                    frozen_missed = freeze_json(missed)
                    assert isinstance(frozen_missed, Mapping)
                    spec = frozen_missed
                    capture_spec_hash = str(spec["capture_spec_hash"])
                existing_horizon_at = (
                    None
                    if existing.get("horizon_at") is None
                    else _time_from(existing["horizon_at"])
                )
                if (
                    existing.get("status") != "WAITING"
                    or _capture_spec_semantic_hash(existing)
                    == _capture_spec_semantic_hash(spec)
                ):
                    return OutcomeCaptureRegistrationResult(
                        status="ALREADY_REGISTERED",
                        subject_id=str(checked_target["subject_id"]),
                        horizon=checked_horizon,
                        horizon_at=existing_horizon_at,
                        reason_codes=tuple(
                            str(item) for item in existing.get("reason_codes", ())
                        ),
                        capture_spec_hash=str(existing.get("capture_spec_hash")),
                    )
            try:
                stored = self._append_capture_spec(
                    capture_key=capture_key,
                    spec=spec,
                    previous=existing_row,
                    occurred_at=occurred_at,
                    recorded_at=checked_registered_at,
                )
            except Exception:
                return OutcomeCaptureRegistrationResult(
                    status="BLOCKED",
                    subject_id=str(checked_target["subject_id"]),
                    horizon=checked_horizon,
                    horizon_at=horizon_at,
                    reason_codes=("OUTCOME_CAPTURE_SPEC_APPEND_FAILED",),
                    capture_spec_hash=capture_spec_hash,
                )

        stored_spec = stored.record.payload
        spec_status = str(stored_spec["status"])
        status = (
            "REVISED"
            if existing_row is not None and spec_status == "READY"
            else "REGISTERED"
            if spec_status == "READY"
            else "WAITING"
            if spec_status == "WAITING"
            else "BLOCKED"
        )
        return OutcomeCaptureRegistrationResult(
            status=status,
            subject_id=str(checked_target["subject_id"]),
            horizon=checked_horizon,
            horizon_at=(
                None
                if stored_spec.get("horizon_at") is None
                else _time_from(stored_spec["horizon_at"])
            ),
            reason_codes=tuple(str(item) for item in stored_spec["reason_codes"]),
            capture_spec_hash=str(stored_spec["capture_spec_hash"]),
        )

    def _refresh_store(self) -> None:
        if not self._initialized:
            rows = self._evidence_store.iter_verified(
                kinds=(OUTCOME_CAPTURE_SPEC_KIND, OUTCOME_OBSERVATION_KIND),
                page_size=5000,
            )
            for row in rows:
                self._apply_row(row)
            self._initialized = True
            return
        while True:
            page = tuple(
                self._evidence_store.query_page(
                    after_sequence=self._cursor,
                    kinds=(OUTCOME_CAPTURE_SPEC_KIND, OUTCOME_OBSERVATION_KIND),
                    limit=5000,
                )
            )
            if not page:
                return
            for row in page:
                self._apply_row(row)
            if len(page) < 5000:
                return

    def _apply_row(self, row: StoredEvidence) -> None:
        self._cursor = max(self._cursor, row.sequence)
        if row.record.kind == OUTCOME_OBSERVATION_KIND:
            self._observation_status[row.record.identity] = row.status
            return
        payload = row.record.payload
        if payload.get("schema") != "options_copilot.outcome_capture_spec.v1":
            return
        capture_key = str(
            payload.get("capture_key")
            or _capture_spec_identity(payload, str(payload.get("horizon", "")))
        )
        current = self._latest_specs.get(capture_key)
        if current is not None and current.sequence >= row.sequence:
            return
        if current is not None:
            self._adjust_durable_spec_counts(current.record.payload, delta=-1)
        self._latest_specs[capture_key] = row
        self._adjust_durable_spec_counts(payload, delta=1)
        raw_horizon_at = payload.get("horizon_at")
        if payload.get("status") == "READY" and raw_horizon_at is not None:
            try:
                heapq.heappush(
                    self._due_heap,
                    (_time_from(raw_horizon_at), row.sequence, capture_key),
                )
            except (TypeError, ValueError):
                return

        elif (
            payload.get("status") == "WAITING"
            and payload.get("subject_kind") == "PREDICTION"
            and isinstance(payload.get("prediction_baseline_request"), Mapping)
            and not (
                isinstance(payload.get("prediction_baseline"), Mapping)
                and payload["prediction_baseline"].get("status") == "AVAILABLE"  # type: ignore[index]
            )
        ):
            try:
                heapq.heappush(
                    self._due_heap,
                    (
                        _time_from(
                            payload.get("baseline_at", payload["occurred_at"])
                        ),
                        row.sequence,
                        capture_key,
                    ),
                )
            except (KeyError, TypeError, ValueError):
                return

    def _adjust_durable_spec_counts(
        self,
        payload: Mapping[str, object],
        *,
        delta: int,
    ) -> None:
        status = str(payload.get("status") or "UNKNOWN").upper()
        with self._durable_counts_lock:
            _adjust_count(self._durable_status_counts, status, delta)
            if status != "BLOCKED":
                return
            reasons = payload.get("reason_codes")
            if not isinstance(reasons, Sequence) or isinstance(
                reasons,
                (str, bytes, bytearray, memoryview),
            ):
                return
            for raw in reasons:
                reason = str(raw).strip().upper()
                if reason:
                    _adjust_count(self._durable_blocker_counts, reason, delta)

    def _append_capture_spec(
        self,
        *,
        capture_key: str,
        spec: Mapping[str, object],
        previous: StoredEvidence | None,
        occurred_at: datetime,
        recorded_at: datetime,
    ) -> StoredEvidence:
        body = thaw_json(spec)
        if not isinstance(body, dict):
            raise OutcomeProcessingError("OUTCOME_CAPTURE_SPEC_INVALID")
        revision = (
            1
            if previous is None
            else int(previous.record.payload.get("revision", 1)) + 1
        )
        body["capture_key"] = capture_key
        body["revision"] = revision
        body["prior_capture_spec_hash"] = (
            None
            if previous is None
            else str(previous.record.payload.get("capture_spec_hash"))
        )
        body.pop("capture_spec_hash", None)
        body["capture_spec_hash"] = canonical_hash(body)
        identity = f"{capture_key}:r{revision}"
        source_hash = canonical_hash(
            {"identity": identity, "capture_spec_hash": body["capture_spec_hash"]}
        )
        result = self._evidence_store.append(
            EvidenceRecord(
                identity=identity,
                kind=OUTCOME_CAPTURE_SPEC_KIND,
                symbol=str(body["symbol"]),
                provider="OPTIONS_COPILOT_CAPTURE",
                source_id=f"capture-spec:{source_hash}",
                published_at=occurred_at,
                first_seen_at=recorded_at,
                ingested_at=recorded_at,
                observed_at=recorded_at,
                payload=body,
                supersedes_id=None if previous is None else previous.evidence_id,
            )
        )
        stored = result.evidence
        self._apply_row(stored)
        return stored

    def _terminalize_missed(
        self,
        row: StoredEvidence,
        *,
        recorded_at: datetime,
        reason: str = "OUTCOME_CAPTURE_WINDOW_MISSED",
    ) -> StoredEvidence:
        checked_reason = str(reason).strip().upper()
        if checked_reason not in _TERMINAL_CAPTURE_REASONS:
            raise OutcomeProcessingError("OUTCOME_CAPTURE_SPEC_INVALID")
        body = thaw_json(row.record.payload)
        if not isinstance(body, dict):
            raise OutcomeProcessingError("OUTCOME_CAPTURE_SPEC_INVALID")
        body["status"] = "BLOCKED"
        body["reason_codes"] = (checked_reason,)
        if not _valid_terminal_capture_condition(
            body,
            previous_payload=row.record.payload,
            terminal_at=recorded_at,
        ):
            raise OutcomeProcessingError("OUTCOME_CAPTURE_SPEC_INVALID")
        body.pop("capture_spec_hash", None)
        body["capture_spec_hash"] = canonical_hash(body)
        return self._append_capture_spec(
            capture_key=str(row.record.payload["capture_key"]),
            spec=body,
            previous=row,
            occurred_at=_time_from(row.record.payload["occurred_at"]),
            recorded_at=recorded_at,
        )

    def _append_prediction_baseline(
        self,
        row: StoredEvidence,
        baseline: Mapping[str, object],
        *,
        recorded_at: datetime,
    ) -> StoredEvidence:
        body = thaw_json(row.record.payload)
        if not isinstance(body, dict):
            raise OutcomeProcessingError("OUTCOME_CAPTURE_SPEC_INVALID")
        body["prediction_baseline"] = thaw_json(baseline)
        body.pop("capture_spec_hash", None)
        body["capture_spec_hash"] = canonical_hash(body)
        return self._append_capture_spec(
            capture_key=str(row.record.payload["capture_key"]),
            spec=body,
            previous=row,
            occurred_at=_time_from(row.record.payload["occurred_at"]),
            recorded_at=recorded_at,
        )

    def tick(self, *, now: datetime) -> OutcomeCaptureResult:
        checked_at = utc_datetime(now, field="now")
        reasons: list[str] = []
        appended = 0
        blocked = 0
        skipped = 0
        due = 0
        try:
            with self._lock:
                self._refresh_store()
        except Exception:
            return OutcomeCaptureResult(
                status="BLOCKED",
                checked_at=checked_at,
                specs_seen=0,
                specs_due=0,
                observations_appended=0,
                records_blocked=1,
                records_skipped=0,
                reason_codes=("OUTCOME_CAPTURE_STORE_CORRUPT",),
            )

        groups: dict[datetime, list[Mapping[str, object]]] = {}
        baseline_groups: dict[datetime, list[Mapping[str, object]]] = {}
        due_rows: list[StoredEvidence] = []
        with self._lock:
            while self._due_heap and self._due_heap[0][0] <= checked_at:
                _, sequence, capture_key = heapq.heappop(self._due_heap)
                row = self._latest_specs.get(capture_key)
                if row is None or row.sequence != sequence:
                    continue
                due_rows.append(row)
        for row in due_rows:
            spec = row.record.payload
            if (
                spec.get("status") == "WAITING"
                and isinstance(spec.get("prediction_baseline_request"), Mapping)
                and not isinstance(spec.get("prediction_baseline"), Mapping)
            ):
                baseline_at = _time_from(
                    spec.get("baseline_at", spec["occurred_at"])
                )
                due += 1
                if checked_at > baseline_at + _CAPTURE_WINDOW:
                    blocked += 1
                    reasons.append("OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED")
                    try:
                        self._terminalize_missed(
                            row,
                            recorded_at=checked_at,
                            reason="OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED",
                        )
                    except Exception:
                        reasons.append("OUTCOME_CAPTURE_SPEC_APPEND_FAILED")
                    continue
                baseline_groups.setdefault(baseline_at, []).append(spec)
                continue
            raw_horizon_at = spec.get("horizon_at")
            try:
                horizon_at = _time_from(raw_horizon_at)
            except (TypeError, ValueError):
                blocked += 1
                reasons.append("OUTCOME_CAPTURE_SPEC_INVALID")
                continue
            due += 1
            observation_identity = str(spec.get("observation_identity", ""))
            existing_status = self._observation_status.get(observation_identity)
            if existing_status == "ACTIVE":
                skipped += 1
                continue
            if existing_status is not None:
                blocked += 1
                reasons.append("OUTCOME_OBSERVATION_CONFLICTED")
                try:
                    self._terminalize_missed(
                        row,
                        recorded_at=checked_at,
                        reason="OUTCOME_OBSERVATION_CONFLICTED",
                    )
                except Exception:
                    reasons.append("OUTCOME_CAPTURE_SPEC_APPEND_FAILED")
                continue
            if checked_at > horizon_at + _CAPTURE_WINDOW:
                blocked += 1
                reasons.append("OUTCOME_CAPTURE_WINDOW_MISSED")
                try:
                    self._terminalize_missed(row, recorded_at=checked_at)
                except Exception:
                    reasons.append("OUTCOME_CAPTURE_SPEC_APPEND_FAILED")
                continue
            groups.setdefault(horizon_at, []).append(spec)

        for baseline_at, specs in sorted(baseline_groups.items()):
            for chunk in _partition_capture_specs(specs):
                try:
                    raw_batch = self._market_adapter.observe(
                        chunk,
                        expected_at=baseline_at,
                    )
                    batch = _market_batch(raw_batch, horizon_at=baseline_at)
                except Exception as exc:
                    blocked += len(chunk)
                    reasons.append(_capture_reason(exc))
                    for spec in chunk:
                        capture_key = str(spec.get("capture_key", ""))
                        with self._lock:
                            current = self._latest_specs.get(capture_key)
                            if current is not None and checked_at <= baseline_at + _CAPTURE_WINDOW:
                                heapq.heappush(
                                    self._due_heap,
                                    (baseline_at, current.sequence, capture_key),
                                )
                        if current is not None and checked_at > baseline_at + _CAPTURE_WINDOW:
                            reasons.append("OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED")
                            try:
                                self._terminalize_missed(
                                    current,
                                    recorded_at=checked_at,
                                    reason="OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED",
                                )
                            except Exception:
                                reasons.append("OUTCOME_CAPTURE_SPEC_APPEND_FAILED")
                    continue
                for spec in chunk:
                    try:
                        baseline, received_at = _capture_prediction_baseline(
                            spec,
                            batch,
                            expected_at=baseline_at,
                        )
                        capture_key = str(spec["capture_key"])
                        current = self._latest_specs[capture_key]
                        self._append_prediction_baseline(
                            current,
                            baseline,
                            recorded_at=received_at,
                        )
                    except Exception as exc:
                        blocked += 1
                        reasons.append(_capture_reason(exc))
                        capture_key = str(spec.get("capture_key", ""))
                        with self._lock:
                            current = self._latest_specs.get(capture_key)
                            if current is not None and checked_at <= baseline_at + _CAPTURE_WINDOW:
                                heapq.heappush(
                                    self._due_heap,
                                    (baseline_at, current.sequence, capture_key),
                                )
                        if current is not None and checked_at > baseline_at + _CAPTURE_WINDOW:
                            reasons.append("OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED")
                            try:
                                self._terminalize_missed(
                                    current,
                                    recorded_at=checked_at,
                                    reason="OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED",
                                )
                            except Exception:
                                reasons.append("OUTCOME_CAPTURE_SPEC_APPEND_FAILED")

        for horizon_at, specs in sorted(groups.items()):
            for chunk in _partition_capture_specs(specs):
                try:
                    raw_batch = self._market_adapter.observe(
                        chunk,
                        expected_at=horizon_at,
                    )
                    batch = _market_batch(raw_batch, horizon_at=horizon_at)
                except Exception as exc:
                    blocked += len(chunk)
                    reasons.append(_capture_reason(exc))
                    with self._lock:
                        for spec in chunk:
                            capture_key = str(spec.get("capture_key", ""))
                            row = self._latest_specs.get(capture_key)
                            if row is not None:
                                heapq.heappush(
                                    self._due_heap,
                                    (horizon_at, row.sequence, capture_key),
                                )
                    continue
                for spec in chunk:
                    try:
                        observation, revision_received_at = _capture_observation(
                            spec,
                            batch,
                            horizon_at=horizon_at,
                        )
                        identity = str(spec["observation_identity"])
                        source_hash = str(batch["source_hash"])
                        result = self._evidence_store.append(
                            EvidenceRecord(
                                identity=identity,
                                kind=OUTCOME_OBSERVATION_KIND,
                                symbol=str(spec["symbol"]),
                                provider=str(batch["source"]),
                                source_id=(
                                    "capture-batch:"
                                    + canonical_hash(
                                        {
                                            "source_id": batch["source_id"],
                                            "source_hash": source_hash,
                                        }
                                    )
                                ),
                                published_at=revision_received_at,
                                first_seen_at=revision_received_at,
                                ingested_at=revision_received_at,
                                observed_at=revision_received_at,
                                payload=observation,
                            )
                        )
                        self._apply_row(result.evidence)
                        if bool(getattr(result, "inserted", False)):
                            appended += 1
                        else:
                            skipped += 1
                    except Exception as exc:
                        blocked += 1
                        reason = _capture_reason(exc)
                        reasons.append(reason)
                        capture_key = str(spec.get("capture_key", ""))
                        with self._lock:
                            current = self._latest_specs.get(capture_key)
                            if (
                                current is not None
                                and checked_at <= horizon_at + _CAPTURE_WINDOW
                                and _retryable_per_spec_failure(reason)
                            ):
                                heapq.heappush(
                                    self._due_heap,
                                    (horizon_at, current.sequence, capture_key),
                                )
                        if current is not None and not _retryable_per_spec_failure(
                            reason
                        ):
                            try:
                                self._terminalize_missed(
                                    current,
                                    recorded_at=checked_at,
                                    reason=reason,
                                )
                            except Exception:
                                reasons.append("OUTCOME_CAPTURE_SPEC_APPEND_FAILED")

        unique_reasons = tuple(dict.fromkeys(reason for reason in reasons if reason))
        completed = appended + skipped
        status = (
            "DEGRADED"
            if blocked and completed
            else "BLOCKED"
            if blocked
            else "COMPLETED"
            if due and completed == due
            else "WAITING"
        )
        return OutcomeCaptureResult(
            status=status,
            checked_at=checked_at,
            specs_seen=len(self._latest_specs),
            specs_due=due,
            observations_appended=appended,
            records_blocked=blocked,
            records_skipped=skipped,
            reason_codes=unique_reasons,
        )


class OutcomeCaptureCoordinator:
    """Discover targets incrementally and retry unresolved capture bindings."""

    def __init__(
        self,
        *,
        capture: ExactHorizonOutcomeCapture,
        candidate_targets: Callable[..., Sequence[Mapping[str, object]]],
        prediction_targets: Callable[
            ..., Sequence[tuple[Mapping[str, object], str]]
        ],
        calendar_provider: object,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(capture, ExactHorizonOutcomeCapture):
            raise TypeError("capture must be ExactHorizonOutcomeCapture")
        if not callable(candidate_targets) or not callable(prediction_targets):
            raise TypeError("outcome target providers must be callable")
        if not callable(getattr(calendar_provider, "snapshot", None)):
            raise TypeError("calendar_provider must expose snapshot")
        self.capture = capture
        self.candidate_targets = candidate_targets
        self.prediction_targets = prediction_targets
        self.calendar_provider = calendar_provider
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._candidate_cursor = 0
        self._prediction_cursor = 0
        self._pending_candidates: dict[
            tuple[str, str], Mapping[str, object]
        ] = {}
        self._pending_predictions: dict[
            tuple[str, str], tuple[Mapping[str, object], str]
        ] = {}
        self._candidate_cache: dict[str, Mapping[str, object]] = {}
        self._lock = threading.RLock()

    def tick(self) -> OutcomeCaptureResult:
        checked_at = utc_datetime(self._clock(), field="capture coordinator clock")
        registration_blocked = 0
        registration_reasons: list[str] = []
        try:
            candidates = tuple(
                _call_cursor_provider(
                    self.candidate_targets,
                    after_sequence=self._candidate_cursor,
                )
            )
        except Exception:
            candidates = ()
            registration_blocked += 1
            registration_reasons.append("CANDIDATE_TARGET_PROVIDER_UNAVAILABLE")
        try:
            predictions = tuple(
                _call_cursor_provider(
                    self.prediction_targets,
                    after_sequence=self._prediction_cursor,
                )
            )
        except Exception:
            predictions = ()
            registration_blocked += 1
            registration_reasons.append("PREDICTION_TARGET_PROVIDER_UNAVAILABLE")
        with self._lock:
            for target in candidates:
                if not isinstance(target, Mapping):
                    continue
                self._candidate_cursor = max(
                    self._candidate_cursor,
                    _target_source_sequence(target),
                )
                subject_hash = str(target.get("subject_hash", ""))
                if subject_hash:
                    self._candidate_cache[subject_hash] = target
                for horizon in OUTCOME_HORIZONS:
                    self._pending_candidates[(subject_hash, horizon)] = target
            self._trim_candidate_cache()
            for target, horizon in predictions:
                if not isinstance(target, Mapping):
                    continue
                self._prediction_cursor = max(
                    self._prediction_cursor,
                    _target_source_sequence(target),
                )
                checked_horizon = str(horizon).upper()
                subject_hash = str(target.get("subject_hash", ""))
                self._pending_predictions[(subject_hash, checked_horizon)] = (
                    target,
                    checked_horizon,
                )

            needs_calendar = any(
                horizon != "30M"
                for _, horizon in self._pending_candidates
            )
            calendar_sessions = (
                _capture_calendar_sessions(self.calendar_provider, now=checked_at)
                if needs_calendar
                else None
            )
            for key, target in tuple(self._pending_candidates.items()):
                _, horizon = key
                result = self.capture.register_target(
                    _target_with_calendar(target, calendar_sessions),
                    horizon=horizon,
                    registered_at=checked_at,
                )
                if result.status in {"REGISTERED", "REVISED", "BLOCKED"}:
                    self._pending_candidates.pop(key, None)
                if result.status == "BLOCKED":
                    registration_blocked += 1
                    registration_reasons.extend(result.reason_codes)

            baseline_requests: set[str] = set()
            for key, (target, checked_horizon) in self._ordered_predictions(
                checked_at
            ):
                baseline = self.capture.prediction_baseline(target, checked_horizon)
                if baseline is None:
                    baseline = self.capture.shared_prediction_baseline(target)
                prediction_set_hash = str(
                    target.get("prediction_set_hash", "")
                )
                if (
                    baseline is None
                    and prediction_set_hash
                    and prediction_set_hash in baseline_requests
                ):
                    continue
                horizon_at = self.capture.target_horizon_at(target, checked_horizon)
                bound_target = (
                    target
                    if baseline is None
                    else _bind_prediction_candidate(
                        target,
                        tuple(self._candidate_cache.values()),
                        prediction_baseline=baseline,
                        horizon_at=horizon_at,
                    )
                )
                result = self.capture.register_target(
                    bound_target,
                    horizon=checked_horizon,
                    registered_at=checked_at,
                )
                if (
                    baseline is None
                    and prediction_set_hash
                    and result.status in {"REGISTERED", "REVISED", "WAITING"}
                ):
                    baseline_requests.add(prediction_set_hash)
                if result.status in {"REGISTERED", "REVISED", "BLOCKED"}:
                    self._pending_predictions.pop(key, None)
                if result.status == "BLOCKED":
                    registration_blocked += 1
                    registration_reasons.extend(result.reason_codes)
        capture_result = self.capture.tick(now=checked_at)
        with self._lock:
            for key, (target, checked_horizon) in tuple(
                self._pending_predictions.items()
            ):
                if self.capture.target_status(target, checked_horizon) == "BLOCKED":
                    self._pending_predictions.pop(key, None)
        if not registration_blocked:
            return capture_result
        completed = (
            capture_result.observations_appended
            + capture_result.records_skipped
        )
        return replace(
            capture_result,
            status="DEGRADED" if completed else "BLOCKED",
            records_blocked=(
                capture_result.records_blocked + registration_blocked
            ),
            reason_codes=tuple(
                dict.fromkeys(
                    (*capture_result.reason_codes, *registration_reasons)
                )
            ),
        )

    def _ordered_predictions(
        self,
        checked_at: datetime,
    ) -> tuple[
        tuple[tuple[str, str], tuple[Mapping[str, object], str]], ...
    ]:
        """Register live baselines before terminalizing an expired startup backlog."""

        return tuple(
            sorted(
                self._pending_predictions.items(),
                key=lambda item: (
                    0
                    if _prediction_baseline_window_open(
                        item[1][0],
                        checked_at=checked_at,
                    )
                    else 1,
                    -_target_source_sequence(item[1][0]),
                    item[0],
                ),
            )
        )

    def _trim_candidate_cache(self) -> None:
        if len(self._candidate_cache) <= 1000:
            return
        ordered = sorted(
            self._candidate_cache.items(),
            key=lambda item: (
                _target_source_sequence(item[1]),
                item[0],
            ),
            reverse=True,
        )
        self._candidate_cache = dict(ordered[:1000])


class OutcomeCaptureLoop:
    """Poll persisted due-times fast enough to honor the five-second hard gate."""

    def __init__(
        self,
        coordinator: OutcomeCaptureCoordinator,
        *,
        interval_seconds: float = 1.0,
    ) -> None:
        if not isinstance(coordinator, OutcomeCaptureCoordinator):
            raise TypeError("coordinator must be OutcomeCaptureCoordinator")
        if isinstance(interval_seconds, bool) or not 0 < interval_seconds <= 1:
            raise ValueError("outcome capture interval must be in (0, 1]")
        self.coordinator = coordinator
        self.interval_seconds = float(interval_seconds)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._last_result: OutcomeCaptureResult | None = None

    @property
    def last_result(self) -> OutcomeCaptureResult | None:
        with self._lock:
            return self._last_result

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="options-copilot-outcome-capture",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> bool:
        with self._lock:
            self._stop.set()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
            if thread.is_alive():
                return False
        with self._lock:
            self._thread = None
        return True

    def tick_once(self) -> OutcomeCaptureResult:
        result = self.coordinator.tick()
        with self._lock:
            self._last_result = result
        return result

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                result = self.coordinator.tick()
                with self._lock:
                    self._last_result = result
            except Exception:
                failure = OutcomeCaptureResult(
                    status="BLOCKED",
                    checked_at=datetime.now(timezone.utc),
                    specs_seen=0,
                    specs_due=0,
                    observations_appended=0,
                    records_blocked=1,
                    records_skipped=0,
                    reason_codes=("OUTCOME_CAPTURE_LOOP_FAILED",),
                )
                with self._lock:
                    self._last_result = failure
            delay = self.interval_seconds
            due_at = self.coordinator.capture.next_due_at
            if due_at is not None:
                seconds_until_due = (
                    due_at - datetime.now(timezone.utc)
                ).total_seconds()
                delay = max(0.05, min(delay, seconds_until_due))
            self._stop.wait(delay)


class ImmutableOutcomeProcessor:
    """Incrementally reconcile durable outcomes within one cooperative budget."""

    def __init__(
        self,
        *,
        shadow_ledger: ShadowLearningLedger,
        candidate_recorder: OutcomeRecorder,
        candidate_targets: Callable[[], Sequence[Mapping[str, object]]],
        observation_provider: OutcomeObservationProvider | None,
        progress_store: OutcomeProgressStore | None = None,
        maximum_work_items: int = 250,
        maximum_pending_items: int = 1000,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(shadow_ledger, ShadowLearningLedger):
            raise TypeError("shadow_ledger must be a ShadowLearningLedger")
        if not isinstance(candidate_recorder, OutcomeRecorder):
            raise TypeError("candidate_recorder must be an OutcomeRecorder")
        if not callable(candidate_targets):
            raise TypeError("candidate_targets must be callable")
        if observation_provider is not None and not callable(
            getattr(observation_provider, "observe", None)
        ):
            raise TypeError("observation_provider must expose observe")
        if progress_store is not None and not isinstance(
            progress_store, OutcomeProgressStore
        ):
            raise TypeError("progress_store must be an OutcomeProgressStore")
        if (
            not isinstance(maximum_work_items, int)
            or isinstance(maximum_work_items, bool)
            or maximum_work_items <= 0
        ):
            raise ValueError("maximum_work_items must be a positive integer")
        if (
            not isinstance(maximum_pending_items, int)
            or isinstance(maximum_pending_items, bool)
            or maximum_pending_items < maximum_work_items
        ):
            raise ValueError("maximum_pending_items must cover one work batch")
        self.shadow_ledger = shadow_ledger
        self.candidate_recorder = candidate_recorder
        self.candidate_targets = candidate_targets
        self.observation_provider = observation_provider
        self.progress_store = progress_store
        self.maximum_work_items = maximum_work_items
        self.maximum_pending_items = maximum_pending_items
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._result_lock = threading.RLock()
        self._last_result: OutcomeProcessingResult | None = None
        self._memory_progress = OutcomeProgressState()

    @property
    def last_result(self) -> OutcomeProcessingResult | None:
        with self._result_lock:
            return self._last_result

    def process(
        self,
        *,
        cancel_event: threading.Event | None = None,
        deadline_at: datetime | None = None,
        operation_token: str | None = None,
    ) -> OutcomeProcessingResult:
        """Process one batch and always release any frozen evidence prefix."""

        verified_provider = (
            self.observation_provider
            if isinstance(
                self.observation_provider,
                EvidenceStoreOutcomeObservationProvider,
            )
            else None
        )
        try:
            return self._process_once(
                cancel_event=cancel_event,
                deadline_at=deadline_at,
                operation_token=operation_token,
            )
        finally:
            if verified_provider is not None:
                verified_provider.end_verified_batch()

    def _process_once(
        self,
        *,
        cancel_event: threading.Event | None = None,
        deadline_at: datetime | None = None,
        operation_token: str | None = None,
    ) -> OutcomeProcessingResult:
        checked_at = utc_datetime(self._clock(), field="clock result")
        deadline = None if deadline_at is None else utc_datetime(
            deadline_at, field="deadline_at"
        )
        cancelled_at_entry = self._should_stop(cancel_event, deadline)
        previous_result = self.last_result
        if cancelled_at_entry:
            initial_candidate_head = (
                GENESIS_HASH
                if previous_result is None
                else previous_result.candidate_ledger_head_hash
            )
            initial_shadow_head = (
                GENESIS_HASH
                if previous_result is None
                else previous_result.shadow_ledger_head_hash
            )
        else:
            initial_candidate_head = self.candidate_recorder.head_hash()
            initial_shadow_head = _shadow_head(self.shadow_ledger)
        state = (
            OutcomeProgressState()
            if cancelled_at_entry or self.progress_store is None
            else self.progress_store.latest()
        )
        prediction_cursor = state.prediction_cursor
        candidate_cursor = state.candidate_cursor
        pending = [dict(thaw_json(item)) for item in state.pending]
        reasons: list[str] = []
        counters = {
            "appended": 0, "superseded": 0, "skipped": 0, "blocked": 0,
            "rejected": 0, "due": 0, "subjects": 0, "requested": 0,
        }
        manifest_rows: list[Mapping[str, object]] = []

        effective_pending_limit = (
            self.maximum_pending_items
            if self.progress_store is not None
            else max(self.maximum_pending_items, 50000)
        )
        capacity = max(0, effective_pending_limit - len(pending))
        effective_work_limit = (
            self.maximum_work_items
            if self.progress_store is not None
            else max(self.maximum_work_items, 10000)
        )
        cancelled = cancelled_at_entry or self._should_stop(cancel_event, deadline)
        verified_provider = (
            self.observation_provider
            if not cancelled
            and isinstance(
                self.observation_provider,
                EvidenceStoreOutcomeObservationProvider,
            )
            else None
        )
        if verified_provider is not None:
            verified_provider.begin_verified_batch(as_of=checked_at)
        if cancelled:
            reasons.append("OUTCOME_PROCESSING_CANCELLED")
        else:
            prediction_cursor = self._ingest_predictions(
                checked_at=checked_at,
                after_sequence=prediction_cursor,
                capacity=capacity,
                pending=pending,
                counters=counters,
                reasons=reasons,
                manifest_rows=manifest_rows,
                work_limit=effective_work_limit,
                cancel_event=cancel_event,
                deadline_at=deadline,
            )
            cancelled = self._should_stop(cancel_event, deadline)
            if cancelled:
                reasons.append("OUTCOME_PROCESSING_CANCELLED")

        capacity = max(0, effective_pending_limit - len(pending))
        candidates: Sequence[Mapping[str, object]] = ()
        if not cancelled:
            try:
                candidates = self._candidate_values(candidate_cursor)
            except Exception:
                counters["rejected"] += 1
                reasons.append("CANDIDATE_TARGETS_UNAVAILABLE")
            cancelled = self._should_stop(cancel_event, deadline)
            if cancelled:
                reasons.append("OUTCOME_PROCESSING_CANCELLED")
        if not cancelled:
            for raw_target in candidates[: capacity // len(OUTCOME_HORIZONS)]:
                if self._should_stop(cancel_event, deadline):
                    cancelled = True
                    reasons.append("OUTCOME_PROCESSING_CANCELLED")
                    break
                try:
                    target = _candidate_target(raw_target)
                    source_sequence = int(target.get("source_sequence") or 0)
                    if self.progress_store is not None and source_sequence <= candidate_cursor:
                        continue
                    pending.extend(self._work_item(target, horizon) for horizon in OUTCOME_HORIZONS)
                    candidate_cursor = max(candidate_cursor, source_sequence)
                except (OutcomeProcessingError, TypeError, ValueError) as exc:
                    counters["rejected"] += 1
                    reasons.append(_reason(exc))

        batch = () if cancelled else pending[:effective_work_limit]
        pending = pending if cancelled else pending[effective_work_limit:]
        waiting: list[dict[str, object]] = []
        for index, item in enumerate(batch):
            if self._should_stop(cancel_event, deadline):
                waiting.extend(batch[index:])
                reasons.append("OUTCOME_PROCESSING_CANCELLED")
                break
            target = item.get("target")
            if not isinstance(target, Mapping):
                counters["rejected"] += 1
                reasons.append("OUTCOME_TARGET_INVALID")
                continue
            horizon = str(item["horizon"])
            counters["subjects"] += 1
            counters["requested"] += 1
            manifest_rows.append({
                "subject_kind": item["subject_kind"],
                "subject_id": item["subject_id"],
                "subject_hash": item["subject_hash"],
                "horizon": horizon,
            })
            raw, terminal = self._observation(
                target,
                horizon,
                checked_at,
                reasons,
            )
            if self._should_stop(cancel_event, deadline):
                waiting.extend(batch[index:])
                reasons.append("OUTCOME_PROCESSING_CANCELLED")
                cancelled = True
                break
            if raw is None:
                counters["blocked"] += 1
                if not terminal:
                    waiting.append(item)
                continue
            try:
                normalized = normalize_outcome_observation(
                    raw,
                    subject_kind=str(item["subject_kind"]),
                    subject_id=str(item["subject_id"]),
                    subject_hash=str(item["subject_hash"]),
                    symbol=str(item["symbol"]),
                    horizon=horizon,
                    occurred_at=_time_from(item["occurred_at"]),
                    as_of=checked_at,
                    thesis_hash=None if item.get("thesis_hash") is None else str(item["thesis_hash"]),
                )
                counters["due"] += 1
                if item["subject_kind"] == "PREDICTION":
                    if self._should_stop(cancel_event, deadline):
                        waiting.extend(batch[index:])
                        reasons.append("OUTCOME_PROCESSING_CANCELLED")
                        cancelled = True
                        break
                    resolved = self.shadow_ledger.resolve_outcome(
                        str(item["subject_id"]),
                        outcome=thaw_json(normalized),
                        observed_at=_time_from(normalized["economic_observed_at"]),
                        resolved_at=_time_from(normalized["revision_received_at"]),
                        prediction_hash=str(item["subject_hash"]),
                    )
                    initial_shadow_head = str(
                        getattr(resolved, "chain_hash", initial_shadow_head)
                    )
                    counters["appended"] += 1
                    if self._should_stop(cancel_event, deadline):
                        reasons.append("OUTCOME_PROCESSING_CANCELLED")
                        cancelled = True
                        waiting.extend(batch[index + 1 :])
                        break
                else:
                    template = thaw_json(target["outcome_template"])
                    if not isinstance(template, dict):
                        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
                    management_result, counterfactual_result = (
                        _candidate_management_documents(target, raw)
                    )
                    if self._should_stop(cancel_event, deadline):
                        waiting.extend(batch[index:])
                        reasons.append("OUTCOME_PROCESSING_CANCELLED")
                        cancelled = True
                        break
                    before_count = self.candidate_recorder.row_count()
                    stored = self.candidate_recorder.record({
                        **template,
                        "symbol": target["symbol"],
                        "horizon": horizon,
                        "horizon_at": normalized["horizon_at"],
                        "economic_observed_at": normalized["economic_observed_at"],
                        "revision_received_at": normalized["revision_received_at"],
                        "market_outcome": thaw_json(raw),
                        "outcome_result_authority": target.get("result_authority"),
                        "position_management_result": management_result,
                        "counterfactual_result": counterfactual_result,
                    })
                    initial_candidate_head = stored.chain_hash
                    if stored.sequence <= before_count:
                        counters["skipped"] += 1
                    elif stored.version > 1:
                        counters["superseded"] += 1
                    else:
                        counters["appended"] += 1
                    if self._should_stop(cancel_event, deadline):
                        reasons.append("OUTCOME_PROCESSING_CANCELLED")
                        cancelled = True
                        waiting.extend(batch[index + 1 :])
                        break
            except (OutcomeIdentityConflict, OutcomeValidationError, OutcomeProcessingError, KeyError, ValueError, TypeError) as exc:
                counters["rejected"] += 1
                reasons.append(_reason(exc))
        pending.extend(waiting)
        bounded = bool(pending)
        if not cancelled and self._should_stop(cancel_event, deadline):
            cancelled = True
            reasons.append("OUTCOME_PROCESSING_CANCELLED")
        if bounded and "OUTCOME_PROCESSING_CANCELLED" not in reasons:
            reasons.append("OUTCOME_PROCESSING_BOUNDED")
        candidate_head = (
            initial_candidate_head
            if cancelled
            else self.candidate_recorder.head_hash()
        )
        shadow_head = (
            initial_shadow_head
            if cancelled
            else _shadow_head(self.shadow_ledger)
        )
        manifest_hash = canonical_hash({
            "schema": "options_copilot.outcome_processing_manifest.v2",
            "checked_at": checked_at,
            "targets": manifest_rows,
            "candidate_ledger_head_hash": candidate_head,
            "shadow_ledger_head_hash": shadow_head,
            "prediction_cursor": prediction_cursor,
            "candidate_cursor": candidate_cursor,
            "remaining_count": len(pending),
        })
        unique_reasons = tuple(dict.fromkeys(reasons))
        satisfied = counters["appended"] + counters["superseded"] + counters["skipped"]
        hard_block = counters["blocked"] and any(
            reason not in {
                "OUTCOME_OBSERVATION_NOT_AVAILABLE",
                "OUTCOME_PROCESSING_BOUNDED",
            }
            for reason in unique_reasons
        )
        status = (
            "DEGRADED"
            if (
                counters["rejected"]
                or (counters["blocked"] and satisfied)
                or "OUTCOME_PROCESSING_CANCELLED" in unique_reasons
            )
            else "BLOCKED"
            if hard_block
            else "COMPLETED"
            if counters["requested"] and satisfied == counters["requested"]
            else "WAITING_FOR_OBSERVATIONS"
        )
        if (
            self.progress_store is not None
            and pending
            and not counters["rejected"]
            and "OUTCOME_PROCESSING_CANCELLED" not in unique_reasons
        ):
            status = "WAITING_FOR_OBSERVATIONS"
        base = {
            "status": status,
            "checked_at": checked_at,
            "subjects_seen": counters["subjects"],
            "horizons_requested": counters["requested"],
            "due_count": counters["due"],
            "records_appended": counters["appended"],
            "records_superseded": counters["superseded"],
            "records_skipped": counters["skipped"],
            "records_blocked": counters["blocked"],
            "records_rejected": counters["rejected"],
            "reason_codes": unique_reasons,
            "candidate_ledger_head_hash": candidate_head,
            "shadow_ledger_head_hash": shadow_head,
            "manifest_hash": manifest_hash,
            "prediction_cursor": prediction_cursor,
            "candidate_cursor": candidate_cursor,
            "remaining_count": len(pending),
            "bounded": bounded,
            "stopped": cancelled,
            "stop_reason": (
                "OUTCOME_PROCESSING_CANCELLED" if cancelled else None
            ),
        }
        shell = OutcomeProcessingResult(**base, processing_hash="0" * 64)
        result = OutcomeProcessingResult(**base, processing_hash=canonical_hash(shell.identity_document()))
        run = {
            "operation_token": operation_token,
            "checked_at": datetime_text(checked_at),
            "status": result.status,
            "processing_hash": result.processing_hash,
            "remaining_count": len(pending),
        }
        if cancelled or self._should_stop(cancel_event, deadline):
            progress = state
        elif self.progress_store is None:
            progress = OutcomeProgressState(
                prediction_cursor=prediction_cursor,
                candidate_cursor=candidate_cursor,
                pending=tuple(freeze_json(item) for item in pending),
                sequence=state.sequence + 1,
                chain_hash=canonical_hash(run),
                recorded_at=checked_at,
            )
            self._memory_progress = progress
        else:
            progress = self.progress_store.append(
                prediction_cursor=prediction_cursor,
                candidate_cursor=candidate_cursor,
                pending=pending,
                run=run,
                recorded_at=checked_at,
            )
        result = replace(
            result,
            progress_sequence=progress.sequence,
            progress_hash=progress.chain_hash,
        )
        result = replace(result, processing_hash=canonical_hash(result.identity_document()))
        with self._result_lock:
            self._last_result = result
        return result

    def _work_item(
        self,
        target: Mapping[str, object],
        horizon: str,
    ) -> dict[str, object]:
        return {
            "subject_kind": target["subject_kind"],
            "subject_id": target["subject_id"],
            "subject_hash": target["subject_hash"],
            "symbol": target["symbol"],
            "horizon": horizon,
            "occurred_at": datetime_text(_time_from(target["occurred_at"])),
            "thesis_hash": target.get("thesis_hash"),
            "target": thaw_json(freeze_json(target)),
        }

    def _ingest_predictions(
        self,
        *,
        checked_at: datetime,
        after_sequence: int,
        capacity: int,
        pending: list[dict[str, object]],
        counters: dict[str, int],
        reasons: list[str],
        manifest_rows: list[Mapping[str, object]],
        work_limit: int,
        cancel_event: threading.Event | None,
        deadline_at: datetime | None,
    ) -> int:
        cursor = after_sequence
        queued = 0
        queue_limit = min(capacity, work_limit)
        scan_remaining = max(work_limit, min(self.maximum_pending_items * 5, 5000))
        while queued < queue_limit and scan_remaining > 0:
            if self._should_stop(cancel_event, deadline_at):
                break
            page_limit = min(scan_remaining, 5000)
            rows = self.shadow_ledger.query_replays(
                as_of=checked_at,
                limit=page_limit,
                after_sequence=cursor,
            )
            if not rows:
                break
            for replay in rows:
                if self._should_stop(cancel_event, deadline_at):
                    return cursor
                prediction = replay.prediction
                cursor = max(cursor, prediction.sequence)
                identity = projectable_shadow_prediction_identity(
                    prediction.prediction,
                    prediction_id=prediction.prediction_id,
                    independence_key=prediction.independence_key,
                    predicted_at=prediction.predicted_at,
                )
                if identity is None:
                    exclusion = shadow_prediction_exclusion_reason(
                        prediction.prediction,
                        prediction_id=prediction.prediction_id,
                        independence_key=prediction.independence_key,
                        predicted_at=prediction.predicted_at,
                    )
                    counters["rejected"] += 1
                    reasons.append(
                        "OUTCOME_TARGET_INVALID"
                        if exclusion in {None, "NOT_NEWS_SHADOW"}
                        else exclusion
                    )
                    continue
                if replay.outcome is not None:
                    counters["subjects"] += 1
                    counters["requested"] += 1
                    counters["skipped"] += 1
                    manifest_rows.append({
                        "subject_kind": "PREDICTION",
                        "subject_id": prediction.prediction_id,
                        "subject_hash": prediction.content_hash,
                        "horizon": identity[0],
                    })
                    continue
                horizon, symbol = identity
                target = {
                    "schema": "options_copilot.outcome_target.v1",
                    "subject_kind": "PREDICTION",
                    "subject_id": prediction.prediction_id,
                    "subject_hash": prediction.content_hash,
                    "symbol": symbol,
                    "occurred_at": datetime_text(prediction.predicted_at),
                    "thesis_hash": prediction.thesis_hash,
                }
                terminal_reason = (
                    self.observation_provider.terminal_reason(
                        target,
                        horizon=horizon,
                        as_of=checked_at,
                    )
                    if isinstance(
                        self.observation_provider,
                        EvidenceStoreOutcomeObservationProvider,
                    )
                    else None
                )
                if terminal_reason is not None:
                    counters["subjects"] += 1
                    counters["requested"] += 1
                    counters["blocked"] += 1
                    reasons.append(terminal_reason)
                    manifest_rows.append({
                        "subject_kind": "PREDICTION",
                        "subject_id": prediction.prediction_id,
                        "subject_hash": prediction.content_hash,
                        "horizon": horizon,
                    })
                    continue
                pending.append(self._work_item(target, horizon))
                queued += 1
                if queued >= queue_limit:
                    return cursor
            scan_remaining -= len(rows)
            if len(rows) < page_limit:
                break
        return cursor

    def _candidate_values(self, after_sequence: int) -> Sequence[Mapping[str, object]]:
        if self.progress_store is None:
            return self.candidate_targets()
        parameters = inspect.signature(self.candidate_targets).parameters
        if "after_sequence" in parameters or any(
            value.kind == inspect.Parameter.VAR_KEYWORD
            for value in parameters.values()
        ):
            return self.candidate_targets(after_sequence=after_sequence)  # type: ignore[call-arg]
        return tuple(
            item for item in self.candidate_targets()
            if int(item.get("source_sequence") or 0) > after_sequence
        )

    def _should_stop(
        self,
        cancel_event: threading.Event | None,
        deadline_at: datetime | None,
    ) -> bool:
        if cancel_event is not None and cancel_event.is_set():
            return True
        return deadline_at is not None and utc_datetime(
            self._clock(), field="clock result"
        ) >= deadline_at

    def _observation(
        self,
        target: Mapping[str, object],
        horizon: str,
        checked_at: datetime,
        reasons: list[str],
    ) -> tuple[Mapping[str, object] | None, bool]:
        provider = self.observation_provider
        if provider is None:
            reasons.append("OUTCOME_OBSERVATION_PROVIDER_UNAVAILABLE")
            return None, False
        try:
            value = provider.observe(target, horizon=horizon, as_of=checked_at)
        except OutcomeObservationTerminal as exc:
            reasons.append(_reason(exc))
            return None, True
        except OutcomeObservationUnavailable as exc:
            reasons.append(_reason(exc))
            return None, False
        except Exception:
            reasons.append("OUTCOME_OBSERVATION_PROVIDER_FAILED")
            return None, False
        if value is None:
            reasons.append("OUTCOME_OBSERVATION_NOT_AVAILABLE")
            return None, False
        if not isinstance(value, Mapping):
            reasons.append("OUTCOME_OBSERVATION_INVALID")
            return None, False
        return value, False


def _candidate_management_documents(
    target: Mapping[str, object],
    observation: Mapping[str, object],
) -> tuple[Mapping[str, object], Mapping[str, object]]:
    template = target.get("outcome_template")
    if not isinstance(template, Mapping):
        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
    management_binding = template.get("position_management_hash")
    counterfactual_spec = template.get("counterfactual_spec_hash")
    candidate_hash = str(template.get("candidate_hash") or "")
    authority = target.get("result_authority")
    ledger_binding = observation.get("ledger_binding")
    supplied_management = observation.get("position_management_result")
    supplied_counterfactual = observation.get("counterfactual_result")
    management = _verified_result_document(
        supplied_management,
        schema="options_copilot.position_management_outcome.v1",
        binding_field="position_management_hash",
        binding_hash=management_binding,
        candidate_hash=candidate_hash,
        authority=authority,
        ledger_binding=ledger_binding,
    )
    if management is None:
        body = {
            "schema": "options_copilot.position_management_outcome.v1",
            "position_management_hash": management_binding,
            "status": "UNAVAILABLE",
            "recommended_action": None,
            "thesis_invalidation_hit": None,
            "risk_stop_hit": None,
            "profit_take_hit": None,
            "time_stop_hit": None,
            "realized_or_executable_pnl_usd": None,
            "entry_value_usd": None,
            "costs_usd": None,
            "max_loss_usd": None,
            "economic_observed_at": None,
            "leg_quotes": (),
            "reason_code": "POSITION_MANAGEMENT_EVIDENCE_UNAVAILABLE",
            "provenance": {
                "status": "UNAVAILABLE",
                "reason_code": "POSITION_MANAGEMENT_EVIDENCE_UNAVAILABLE",
            },
        }
        management = {**body, "result_hash": canonical_hash(body)}
    counterfactual = _verified_result_document(
        supplied_counterfactual,
        schema="options_copilot.outcome_counterfactual_result.v1",
        binding_field="counterfactual_spec_hash",
        binding_hash=counterfactual_spec,
        candidate_hash=candidate_hash,
        authority=authority,
        ledger_binding=ledger_binding,
    )
    if counterfactual is None:
        body = {
            "schema": "options_copilot.outcome_counterfactual_result.v1",
            "counterfactual_spec_hash": counterfactual_spec,
            "status": "UNAVAILABLE",
            "paths": (
                {
                    "path": "FOLLOW_EXIT_POLICY",
                    "status": "UNAVAILABLE",
                    "pnl_usd": None,
                    "entry_value_usd": None,
                    "costs_usd": None,
                    "max_loss_usd": None,
                    "economic_observed_at": None,
                    "leg_quotes": (),
                    "reason_code": "EXIT_RULE_PATH_EVIDENCE_UNAVAILABLE",
                },
                {
                    "path": "HOLD_TO_HORIZON",
                    "status": "UNAVAILABLE",
                    "pnl_usd": None,
                    "entry_value_usd": None,
                    "costs_usd": None,
                    "max_loss_usd": None,
                    "economic_observed_at": None,
                    "leg_quotes": (),
                    "reason_code": "EXECUTABLE_OPTION_ECONOMICS_UNAVAILABLE",
                },
            ),
            "provenance": {
                "status": "UNAVAILABLE",
                "reason_code": "COUNTERFACTUAL_MARKET_PATH_UNAVAILABLE",
            },
        }
        counterfactual = {**body, "result_hash": canonical_hash(body)}
    return freeze_json(management), freeze_json(counterfactual)


def _verified_result_document(
    value: object,
    *,
    schema: str,
    binding_field: str,
    binding_hash: object,
    candidate_hash: str,
    authority: object,
    ledger_binding: object,
) -> Mapping[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise OutcomeProcessingError("OUTCOME_MANAGEMENT_RESULT_INVALID")
    if authority is not None and not isinstance(authority, Mapping):
        raise OutcomeProcessingError("OUTCOME_MANAGEMENT_RESULT_INVALID")
    if ledger_binding is not None and not isinstance(ledger_binding, Mapping):
        raise OutcomeProcessingError("OUTCOME_MANAGEMENT_RESULT_INVALID")
    try:
        normalized, _ = normalize_bound_outcome_result(
            value,
            schema=schema,
            binding_field=binding_field,
            binding_hash=(
                None if binding_hash is None else str(binding_hash)
            ),
            candidate_hash=candidate_hash,
            authority=authority,
            ledger_binding=ledger_binding,
        )
    except OutcomeValidationError as exc:
        raise OutcomeProcessingError("OUTCOME_MANAGEMENT_RESULT_INVALID") from exc
    if normalized is None:
        raise OutcomeProcessingError("OUTCOME_MANAGEMENT_RESULT_INVALID")
    return normalized


def _candidate_target(value: object) -> Mapping[str, object]:
    required = {
        "schema",
        "subject_kind",
        "subject_id",
        "subject_hash",
        "symbol",
        "occurred_at",
        "outcome_template",
    }
    optional = {
        "thesis_hash",
        "capture_plan",
        "binding_context",
        "source_sequence",
        "result_authority",
    }
    if (
        not isinstance(value, Mapping)
        or not required.issubset(value)
        or set(value) - required - optional
    ):
        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
    if value.get("schema") != "options_copilot.outcome_target.v1" or value.get(
        "subject_kind"
    ) != "CANDIDATE":
        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
    template = value.get("outcome_template")
    if not isinstance(template, Mapping):
        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
    if "capture_plan" in value and not isinstance(value.get("capture_plan"), Mapping):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_PLAN_INVALID")
    if "result_authority" in value and not isinstance(
        value.get("result_authority"), Mapping
    ):
        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
    frozen = freeze_json(value)
    assert isinstance(frozen, Mapping)
    return frozen


def _project_prediction_targets(
    predictions: Sequence[object],
) -> tuple[tuple[Mapping[str, object], str], ...]:
    output: list[tuple[Mapping[str, object], str]] = []
    for prediction in predictions:
        body = getattr(prediction, "prediction", None)
        identity = projectable_shadow_prediction_identity(
            body,
            prediction_id=getattr(prediction, "prediction_id", None),
            independence_key=getattr(prediction, "independence_key", None),
            predicted_at=getattr(prediction, "predicted_at", None),
        )
        if identity is None:
            continue
        horizon, symbol = identity
        sequence = int(getattr(prediction, "sequence"))
        prediction_hash = str(getattr(prediction, "content_hash"))
        independence_key = str(getattr(prediction, "independence_key"))
        classification = body.get("classification")
        predicted_direction = (
            str(classification.get("direction", "UNKNOWN")).upper()
            if isinstance(classification, Mapping)
            else "UNKNOWN"
        )
        target = freeze_json(
            {
                "schema": "options_copilot.outcome_target.v1",
                "subject_kind": "PREDICTION",
                "subject_id": str(getattr(prediction, "prediction_id")),
                "subject_hash": prediction_hash,
                "symbol": symbol,
                "occurred_at": getattr(prediction, "predicted_at"),
                "baseline_at": _prediction_baseline_at(prediction),
                "thesis_hash": str(getattr(prediction, "thesis_hash")),
                "source_sequence": sequence,
                "prediction_hash": prediction_hash,
                "independence_key": independence_key,
                "predicted_direction": predicted_direction,
                "prediction_set_hash": str(
                    body.get("prediction_baseline_hash", "")
                ),
                "binding_context": {
                    "source_sequence": sequence,
                    "event_id": body.get("event_id"),
                    "event_ids": (
                        (str(body.get("event_id")),)
                        if body.get("event_id")
                        else ()
                    ),
                    "independence_key": independence_key,
                },
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
        )
        assert isinstance(target, Mapping)
        output.append((target, horizon))
    return tuple(output)


def _capture_target(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
    if str(value.get("subject_kind", "")).upper() == "CANDIDATE":
        return _candidate_target(value)
    required = {
        "schema",
        "subject_kind",
        "subject_id",
        "subject_hash",
        "symbol",
        "occurred_at",
        "thesis_hash",
    }
    optional = {
        "baseline_at",
        "capture_plan",
        "binding_context",
        "source_sequence",
        "independence_key",
        "prediction_hash",
        "predicted_direction",
        "prediction_set_hash",
        "prediction_candidate_binding",
        "prediction_baseline_request",
        "prediction_baseline",
    }
    if (
        not required.issubset(value)
        or set(value) - required - optional
        or value.get("schema") != "options_copilot.outcome_target.v1"
        or str(value.get("subject_kind", "")).upper() != "PREDICTION"
        or ("capture_plan" in value and not isinstance(value.get("capture_plan"), Mapping))
    ):
        raise OutcomeProcessingError("OUTCOME_TARGET_INVALID")
    frozen = freeze_json(value)
    assert isinstance(frozen, Mapping)
    return frozen


def _capture_spec_identity(target: Mapping[str, object], horizon: str) -> str:
    return "outcome-capture-spec:" + canonical_hash(
        {
            "subject_kind": target.get("subject_kind"),
            "subject_id": target.get("subject_id"),
            "subject_hash": target.get("subject_hash"),
            "horizon": horizon,
        }
    )


def _capture_spec_rows_by_key(
    rows: Sequence[StoredEvidence],
) -> dict[str, tuple[StoredEvidence, ...] | None]:
    """Bind linked revisions before validating their payload-declared key."""

    by_id = {row.evidence_id: row for row in rows}
    neighbors: dict[str, set[str]] = {
        evidence_id: set() for evidence_id in by_id
    }
    for evidence_id, row in by_id.items():
        parent_id = row.record.supersedes_id
        if parent_id is not None and parent_id in by_id:
            neighbors[evidence_id].add(parent_id)
            neighbors[parent_id].add(evidence_id)

    grouped: dict[str, tuple[StoredEvidence, ...] | None] = {}
    visited: set[str] = set()
    for evidence_id in by_id:
        if evidence_id in visited:
            continue
        pending = [evidence_id]
        component: list[StoredEvidence] = []
        keys: set[str] = set()
        while pending:
            current_id = pending.pop()
            if current_id in visited:
                continue
            visited.add(current_id)
            row = by_id[current_id]
            component.append(row)
            payload_key = row.record.payload.get("capture_key")
            if isinstance(payload_key, str) and payload_key:
                keys.add(payload_key)
            identity_key, separator, revision_text = row.record.identity.rpartition(
                ":r"
            )
            if (
                separator
                and identity_key.startswith("outcome-capture-spec:")
                and revision_text.isdigit()
            ):
                keys.add(identity_key)
            pending.extend(neighbors[current_id] - visited)
        if len(keys) != 1:
            for capture_key in keys:
                grouped[capture_key] = None
            continue
        capture_key = next(iter(keys))
        if capture_key in grouped:
            grouped[capture_key] = None
            continue
        grouped[capture_key] = tuple(
            sorted(component, key=lambda row: row.sequence)
        )

    return grouped


def _verified_terminal_capture_reason(
    rows: Sequence[StoredEvidence],
    *,
    capture_key: str,
) -> str | None:
    """Return a terminal reason only for a complete authoritative revision chain."""

    previous: StoredEvidence | None = None
    for expected_revision, row in enumerate(rows, start=1):
        record = row.record
        payload = record.payload
        horizon = str(payload.get("horizon") or "").upper()
        revision = payload.get("revision")
        capture_spec_hash = payload.get("capture_spec_hash")
        required = {
            "schema",
            "status",
            "reason_codes",
            "subject_kind",
            "subject_id",
            "subject_hash",
            "symbol",
            "occurred_at",
            "baseline_at",
            "registered_at",
            "horizon",
            "target_rule",
            "horizon_at",
            "horizon_evidence",
            "horizon_evidence_hash",
            "capture_plan",
            "observation_identity",
            "decision_authority",
            "instruction_creation_allowed",
            "order_allowed",
            "capture_key",
            "revision",
            "prior_capture_spec_hash",
            "capture_spec_hash",
        }
        allowed = required | _CAPTURE_SPEC_OPTIONAL_FIELDS
        if (
            row.status != "ACTIVE"
            or record.status != "ACTIVE"
            or record.kind != OUTCOME_CAPTURE_SPEC_KIND
            or record.provider != "OPTIONS_COPILOT_CAPTURE"
            or record.decision_authority != "SUPPORTING_ONLY"
            or not required.issubset(payload)
            or set(payload) - allowed
            or payload.get("schema") != "options_copilot.outcome_capture_spec.v1"
            or str(payload.get("status") or "").upper()
            not in {"READY", "WAITING", "BLOCKED"}
            or horizon not in OUTCOME_HORIZONS
            or isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision != expected_revision
            or not isinstance(capture_spec_hash, str)
            or len(capture_spec_hash) != 64
            or payload.get("decision_authority") != "SUPPORTING_ONLY"
            or payload.get("instruction_creation_allowed") is not False
            or payload.get("order_allowed") is not False
            or payload.get("capture_key") != capture_key
            or _capture_spec_identity(payload, horizon) != capture_key
            or payload.get("target_rule") != OUTCOME_TARGET_RULES[horizon]
            or payload.get("observation_identity")
            != EvidenceStoreOutcomeObservationProvider.identity(payload, horizon)
            or record.identity != f"{capture_key}:r{revision}"
            or record.symbol != payload.get("symbol")
        ):
            return None
        expected_prior_hash = (
            None
            if previous is None
            else previous.record.payload.get("capture_spec_hash")
        )
        expected_supersedes_id = (
            None if previous is None else previous.evidence_id
        )
        if (
            payload.get("prior_capture_spec_hash") != expected_prior_hash
            or record.supersedes_id != expected_supersedes_id
            or record.source_id
            != "capture-spec:"
            + canonical_hash(
                {
                    "identity": record.identity,
                    "capture_spec_hash": capture_spec_hash,
                }
            )
        ):
            return None
        body = thaw_json(payload)
        if not isinstance(body, dict):
            return None
        body.pop("capture_spec_hash", None)
        if canonical_hash(body) != capture_spec_hash:
            return None
        try:
            if _time_from(payload["occurred_at"]) != record.published_at:
                return None
            _time_from(payload["baseline_at"])
            _time_from(payload["registered_at"])
            if payload.get("horizon_at") is not None:
                _time_from(payload["horizon_at"])
        except (KeyError, TypeError, ValueError):
            return None
        if not _valid_capture_spec_revision(previous, row):
            return None
        previous = row

    if previous is None:
        return None
    payload = previous.record.payload
    if str(payload.get("status") or "").upper() != "BLOCKED":
        return None
    reason = _terminal_capture_reason(payload)
    return (
        reason
        if reason in _AUTHORITATIVE_TERMINAL_CAPTURE_REASONS
        else None
    )


def _valid_capture_spec_revision(
    previous: StoredEvidence | None,
    current: StoredEvidence,
) -> bool:
    payload = current.record.payload
    target = _validated_capture_spec_target(payload)
    if target is None:
        return False
    status = str(payload.get("status") or "").upper()
    raw_reasons = payload.get("reason_codes")
    if not isinstance(raw_reasons, Sequence) or isinstance(
        raw_reasons,
        (str, bytes, bytearray, memoryview),
    ):
        return False
    reasons = tuple(str(item).strip().upper() for item in raw_reasons)
    if any(not reason for reason in reasons):
        return False
    try:
        registered_at = _time_from(payload["registered_at"])
    except (KeyError, TypeError, ValueError):
        return False
    terminal_at = current.record.first_seen_at
    if (
        current.record.ingested_at != terminal_at
        or current.record.observed_at != terminal_at
        or registered_at > terminal_at
    ):
        return False
    if status == "READY" and reasons:
        return False
    if status == "BLOCKED" and _terminal_capture_reason(payload) is None:
        return False
    if previous is None:
        return (
            status in {"READY", "WAITING"}
            and registered_at == terminal_at
            and _valid_root_capture_spec(payload, target=target)
        )

    previous_payload = previous.record.payload
    previous_status = str(previous_payload.get("status") or "").upper()
    if (
        previous_status == "BLOCKED"
        or terminal_at < previous.record.first_seen_at
    ):
        return False
    try:
        if registered_at < _time_from(previous_payload["registered_at"]):
            return False
    except (KeyError, TypeError, ValueError):
        return False

    previous_lineage = {
        name: value
        for name, value in previous_payload.items()
        if name not in _CAPTURE_SPEC_MODELED_REVISION_FIELDS
    }
    current_lineage = {
        name: value
        for name, value in payload.items()
        if name not in _CAPTURE_SPEC_MODELED_REVISION_FIELDS
    }
    if canonical_hash(previous_lineage) != canonical_hash(current_lineage):
        return False

    previous_baseline = previous_payload.get("prediction_baseline")
    current_baseline = payload.get("prediction_baseline")
    baseline_added = previous_baseline is None and isinstance(
        current_baseline, Mapping
    )
    if previous_baseline is not None and previous_baseline != current_baseline:
        return False
    if baseline_added and not (
        previous_status == "WAITING"
        and status in {"WAITING", "READY"}
        and str(current_baseline.get("status") or "").upper() == "AVAILABLE"
    ):
        return False
    if previous_baseline is None and current_baseline is not None and not baseline_added:
        return False

    previous_binding = previous_payload.get("prediction_candidate_binding")
    current_binding = payload.get("prediction_candidate_binding")
    binding_added = previous_binding is None and isinstance(
        current_binding, Mapping
    )
    if previous_binding is not None and previous_binding != current_binding:
        return False
    if binding_added and not (
        previous_status == "WAITING"
        and status == "READY"
        and str(payload.get("subject_kind") or "").upper() == "PREDICTION"
    ):
        return False
    if previous_binding is None and current_binding is not None and not binding_added:
        return False

    previous_plan = previous_payload.get("capture_plan")
    current_plan = payload.get("capture_plan")
    if previous_plan != current_plan and not _valid_capture_plan_upgrade(
        previous_plan,
        current_plan,
        previous_status=previous_status,
        current_status=status,
        prediction_baseline=current_baseline,
        prediction_binding=current_binding,
    ):
        return False

    if status == "WAITING":
        return previous_status == "WAITING" and reasons == tuple(
            str(item).strip().upper()
            for item in previous_payload.get("reason_codes", ())
        )
    if status == "READY":
        return previous_status == "WAITING"
    if status != "BLOCKED" or previous_status not in {"READY", "WAITING"}:
        return False
    return _valid_terminal_capture_condition(
        payload,
        previous_payload=previous_payload,
        terminal_at=terminal_at,
    )


def _valid_capture_plan_upgrade(
    previous_plan: object,
    current_plan: object,
    *,
    previous_status: str,
    current_status: str,
    prediction_baseline: object,
    prediction_binding: object,
) -> bool:
    if not (
        previous_status == "WAITING"
        and current_status == "READY"
        and isinstance(previous_plan, Mapping)
        and str(previous_plan.get("status") or "").upper()
        in {"WAITING", "DIRECTION_ONLY"}
        and isinstance(current_plan, Mapping)
        and isinstance(prediction_baseline, Mapping)
        and str(prediction_baseline.get("status") or "").upper() == "AVAILABLE"
    ):
        return False
    current_plan_status = str(current_plan.get("status") or "").upper()
    if current_plan_status == "DIRECTION_ONLY":
        raw_reasons = current_plan.get("reason_codes")
        return (
            prediction_binding is None
            and isinstance(raw_reasons, Sequence)
            and not isinstance(
                raw_reasons,
                (str, bytes, bytearray, memoryview),
            )
            and tuple(str(item).strip().upper() for item in raw_reasons)
            == ("PREDICTION_CANDIDATE_NOT_POINT_IN_TIME",)
        )
    if current_plan_status != "READY" or not isinstance(
        prediction_binding,
        Mapping,
    ):
        return False
    return (
        current_plan.get("underlying") == prediction_baseline.get("underlying")
        and current_plan.get("benchmark") == prediction_baseline.get("benchmark")
        and current_plan.get("benchmark_symbol")
        == prediction_baseline.get("benchmark_symbol")
    )


def _valid_root_capture_spec(
    payload: Mapping[str, object],
    *,
    target: Mapping[str, object],
) -> bool:
    """Rebuild revision one instead of trusting its self-consistent hashes."""

    try:
        rebuilt = thaw_json(
            _build_capture_spec(
                target,
                str(payload["horizon"]).upper(),
                registered_at=_time_from(payload["registered_at"]),
            )
        )
        current = thaw_json(payload)
        if not isinstance(rebuilt, dict) or not isinstance(current, dict):
            return False
        rebuilt.pop("capture_spec_hash", None)
        for name in (
            "capture_key",
            "revision",
            "prior_capture_spec_hash",
            "capture_spec_hash",
        ):
            current.pop(name, None)
        return canonical_hash(rebuilt) == canonical_hash(current)
    except (
        KeyError,
        OutcomeProcessingError,
        OutcomeValidationError,
        TypeError,
        ValueError,
    ):
        return False


def _validated_capture_spec_target(
    payload: Mapping[str, object],
) -> Mapping[str, object] | None:
    """Validate modeled fields on every revision and return its rebuild target."""

    try:
        subject_kind = str(payload.get("subject_kind") or "").upper()
        if subject_kind not in {"CANDIDATE", "PREDICTION"}:
            return None
        _nonblank_capture(payload.get("subject_id"), "OUTCOME_CAPTURE_SPEC_INVALID")
        _nonblank_capture(payload.get("symbol"), "OUTCOME_CAPTURE_SPEC_INVALID")
        _capture_digest(payload.get("subject_hash"), "OUTCOME_CAPTURE_SPEC_INVALID")
        _time_from(payload["occurred_at"])
        _time_from(payload["baseline_at"])
        if "thesis_hash" in payload:
            _capture_digest(
                payload.get("thesis_hash"),
                "OUTCOME_CAPTURE_SPEC_INVALID",
            )
        if "prediction_set_hash" in payload:
            prediction_set_hash = payload.get("prediction_set_hash")
            if prediction_set_hash != "":
                _capture_digest(
                    prediction_set_hash,
                    "OUTCOME_CAPTURE_SPEC_INVALID",
                )
        if not _valid_root_prediction_lineage(payload, subject_kind=subject_kind):
            return None
        if not _valid_capture_spec_status_plan(
            payload,
            subject_kind=subject_kind,
        ):
            return None

        target: dict[str, object] = {
            "schema": "options_copilot.outcome_target.v1",
            "subject_kind": subject_kind,
            "subject_id": payload["subject_id"],
            "subject_hash": payload["subject_hash"],
            "symbol": payload["symbol"],
            "occurred_at": payload["occurred_at"],
            "baseline_at": payload["baseline_at"],
            "capture_plan": payload["capture_plan"],
        }
        for name in _CAPTURE_SPEC_OPTIONAL_FIELDS:
            if name in payload:
                target[name] = payload[name]
        plan = payload.get("capture_plan")
        if plan is not None:
            if not isinstance(plan, Mapping):
                return None
            normalized_plan = _capture_plan(plan, target=target)
            if canonical_hash(normalized_plan) != canonical_hash(plan):
                return None
        frozen = freeze_json(target)
        assert isinstance(frozen, Mapping)
        return frozen
    except (
        KeyError,
        OutcomeProcessingError,
        TypeError,
        ValueError,
    ):
        return None


def _valid_capture_spec_status_plan(
    payload: Mapping[str, object],
    *,
    subject_kind: str,
) -> bool:
    """Keep top-level readiness consistent with the normalized capture plan."""

    status = str(payload.get("status") or "").upper()
    if status == "BLOCKED":
        return True
    plan = payload.get("capture_plan")
    if plan is None:
        return status == "WAITING"
    if not isinstance(plan, Mapping):
        return False
    plan_status = str(plan.get("status") or "").upper()
    baseline = payload.get("prediction_baseline")
    binding = payload.get("prediction_candidate_binding")
    baseline_available = (
        isinstance(baseline, Mapping)
        and str(baseline.get("status") or "").upper() == "AVAILABLE"
    )
    if status == "READY":
        if plan_status == "DIRECTION_ONLY":
            return (
                subject_kind == "PREDICTION"
                and baseline_available
                and binding is None
            )
        if plan_status == "READY":
            return (
                subject_kind == "CANDIDATE"
                or (
                    subject_kind == "PREDICTION"
                    and baseline_available
                    and isinstance(binding, Mapping)
                )
            )
        return False
    if status != "WAITING":
        return False
    return plan_status == "WAITING" or (
        subject_kind == "PREDICTION" and plan_status == "DIRECTION_ONLY"
    )


def _valid_root_prediction_lineage(
    payload: Mapping[str, object],
    *,
    subject_kind: str,
) -> bool:
    prediction_fields = {
        "prediction_candidate_binding",
        "prediction_baseline_request",
        "prediction_baseline",
        "prediction_set_hash",
        "predicted_direction",
    }
    if subject_kind == "CANDIDATE":
        return not any(name in payload for name in prediction_fields)

    if "thesis_hash" not in payload:
        return False
    direction = payload.get("predicted_direction")
    if direction is not None and str(direction).upper() not in {
        "BULLISH",
        "BEARISH",
        "NEUTRAL",
        "MIXED",
        "UNCERTAIN",
        "UNKNOWN",
    }:
        return False
    request = payload.get("prediction_baseline_request")
    if request is not None and not _valid_prediction_baseline_request(request):
        return False
    baseline = payload.get("prediction_baseline")
    if baseline is not None and not _valid_prediction_baseline(
        baseline,
        payload=payload,
    ):
        return False
    binding = payload.get("prediction_candidate_binding")
    if binding is not None and not _valid_prediction_candidate_binding(
        binding,
        payload=payload,
    ):
        return False

    plan = payload.get("capture_plan")
    if not isinstance(plan, Mapping):
        return binding is None
    plan_status = str(plan.get("status") or "").upper()
    baseline_available = (
        isinstance(baseline, Mapping)
        and str(baseline.get("status") or "").upper() == "AVAILABLE"
    )
    if plan_status == "READY":
        return (
            baseline_available
            and isinstance(binding, Mapping)
            and plan.get("underlying") == baseline.get("underlying")
            and plan.get("benchmark") == baseline.get("benchmark")
            and plan.get("benchmark_symbol") == baseline.get("benchmark_symbol")
        )
    if plan_status == "DIRECTION_ONLY":
        return binding is None
    return binding is None


def _valid_prediction_baseline_request(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"schema", "benchmark_symbol"}
        and value.get("schema")
        == "options_copilot.prediction_baseline_request.v1"
        and isinstance(value.get("benchmark_symbol"), str)
        and bool(str(value.get("benchmark_symbol")).strip())
    )


def _valid_prediction_baseline(
    value: object,
    *,
    payload: Mapping[str, object],
) -> bool:
    required = {
        "schema",
        "status",
        "benchmark_symbol",
        "captured_at",
        "underlying",
        "benchmark",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != required
        or value.get("schema") != "options_copilot.prediction_baseline.v1"
        or str(value.get("status") or "").upper() != "AVAILABLE"
    ):
        return False
    try:
        benchmark_symbol = _nonblank_capture(
            value.get("benchmark_symbol"),
            "OUTCOME_PREDICTION_BASELINE_INVALID",
        ).upper()
        underlying = _baseline_market_binding(
            value.get("underlying"),
            expected_symbol=str(payload["symbol"]),
        )
        benchmark = _baseline_market_binding(
            value.get("benchmark"),
            expected_symbol=benchmark_symbol,
        )
        baseline_at = _time_from(payload["baseline_at"])
        captured_at = _time_from(value["captured_at"])
        observed_at = (
            _time_from(underlying["observed_at"]),
            _time_from(benchmark["observed_at"]),
        )
    except (KeyError, OutcomeProcessingError, TypeError, ValueError):
        return False
    request = payload.get("prediction_baseline_request")
    return (
        baseline_at <= captured_at <= baseline_at + _CAPTURE_WINDOW
        and all(baseline_at <= item <= captured_at for item in observed_at)
        and (
            not isinstance(request, Mapping)
            or str(request.get("benchmark_symbol") or "").upper()
            == benchmark_symbol
        )
    )


def _valid_prediction_candidate_binding(
    value: object,
    *,
    payload: Mapping[str, object],
) -> bool:
    required = {
        "prediction_hash",
        "independence_key",
        "ranking_snapshot_id",
        "ranking_snapshot_hash",
        "scan_run_id",
        "candidate_id",
        "candidate_hash",
        "ranking_basis_hash",
        "rank",
        "candidate_occurred_at",
        "candidate_binding_cutoff_at",
        "event_ids",
        "binding_rule",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        return False
    try:
        for name in (
            "prediction_hash",
            "ranking_snapshot_hash",
            "candidate_hash",
            "ranking_basis_hash",
        ):
            _capture_digest(value.get(name), "OUTCOME_PREDICTION_BINDING_INVALID")
        for name in (
            "independence_key",
            "ranking_snapshot_id",
            "scan_run_id",
            "candidate_id",
        ):
            _nonblank_capture(value.get(name), "OUTCOME_PREDICTION_BINDING_INVALID")
        candidate_at = _time_from(value["candidate_occurred_at"])
        cutoff_at = _time_from(value["candidate_binding_cutoff_at"])
        occurred_at = _time_from(payload["occurred_at"])
    except (KeyError, OutcomeProcessingError, TypeError, ValueError):
        return False
    event_ids = value.get("event_ids")
    return (
        value.get("prediction_hash") == payload.get("subject_hash")
        and value.get("rank") == 1
        and not isinstance(value.get("rank"), bool)
        and cutoff_at == occurred_at
        and candidate_at <= cutoff_at
        and isinstance(event_ids, Sequence)
        and not isinstance(event_ids, (str, bytes, bytearray, memoryview))
        and all(isinstance(item, str) and bool(item) for item in event_ids)
        and tuple(event_ids) == tuple(sorted(set(event_ids)))
        and value.get("binding_rule")
        in {
            "EXACT_EVENT_SAME_SYMBOL_RANK_ONE",
            "SAME_SYMBOL_RANK_ONE_FALLBACK",
        }
    )


def _terminal_capture_reason(payload: Mapping[str, object]) -> str | None:
    raw_reasons = payload.get("reason_codes")
    if not isinstance(raw_reasons, Sequence) or isinstance(
        raw_reasons,
        (str, bytes, bytearray, memoryview),
    ):
        return None
    reasons = tuple(
        str(item).strip().upper()
        for item in raw_reasons
        if str(item).strip()
    )
    if len(reasons) != 1 or reasons[0] not in _TERMINAL_CAPTURE_REASONS:
        return None
    return reasons[0]


def _valid_terminal_capture_condition(
    payload: Mapping[str, object],
    *,
    previous_payload: Mapping[str, object],
    terminal_at: datetime,
) -> bool:
    reason = _terminal_capture_reason(payload)
    if reason is None:
        return False
    if reason == "OUTCOME_CAPTURE_WINDOW_MISSED":
        try:
            horizon_at = _time_from(payload["horizon_at"])
        except (KeyError, TypeError, ValueError):
            return False
        return terminal_at > horizon_at + _CAPTURE_WINDOW
    if reason == "OUTCOME_PREDICTION_BASELINE_WINDOW_MISSED":
        baseline_request = previous_payload.get("prediction_baseline_request")
        baseline = previous_payload.get("prediction_baseline")
        try:
            baseline_at = _time_from(payload["baseline_at"])
        except (KeyError, TypeError, ValueError):
            return False
        return (
            isinstance(baseline_request, Mapping)
            and not (
                isinstance(baseline, Mapping)
                and str(baseline.get("status") or "").upper() == "AVAILABLE"
            )
            and terminal_at > baseline_at + _CAPTURE_WINDOW
        )
    try:
        horizon_at = _time_from(payload["horizon_at"])
    except (KeyError, TypeError, ValueError):
        return False
    return terminal_at >= horizon_at


def _capture_spec_semantic_hash(spec: Mapping[str, object]) -> str:
    ignored = {
        "capture_key",
        "capture_spec_hash",
        "revision",
        "prior_capture_spec_hash",
        "registered_at",
    }
    return canonical_hash(
        {name: value for name, value in spec.items() if name not in ignored}
    )


def _build_capture_spec(
    target: Mapping[str, object],
    horizon: str,
    *,
    registered_at: datetime,
) -> Mapping[str, object]:
    occurred_at = _time_from(target["occurred_at"])
    baseline_at = _time_from(target.get("baseline_at", occurred_at))
    reasons: list[str] = []
    if baseline_at < occurred_at:
        reasons.append("OUTCOME_PREDICTION_BASELINE_TIME_TRAVEL")
    plan: Mapping[str, object] | None = None
    try:
        plan = _capture_plan(target.get("capture_plan"), target=target)
        if plan.get("status") not in {"READY", "DIRECTION_ONLY"}:
            reasons.extend(str(item) for item in plan.get("reason_codes", ()))
    except OutcomeProcessingError as exc:
        reasons.append(_reason(exc))
    try:
        raw_horizon_evidence = _capture_horizon_evidence(
            horizon,
            plan=plan,
        )
        horizon_at, normalized_evidence = resolve_outcome_horizon(
            horizon,
            occurred_at=occurred_at,
            evidence=raw_horizon_evidence,
            as_of=registered_at,
        )
        horizon_evidence = _raw_horizon_evidence(normalized_evidence)
        horizon_evidence_hash: str | None = canonical_hash(normalized_evidence)
    except (OutcomeProcessingError, OutcomeValidationError, TypeError, ValueError) as exc:
        reasons.append(_reason(exc))
        horizon_at = None
        horizon_evidence = None
        horizon_evidence_hash = None

    status = (
        "READY"
        if plan is not None
        and plan.get("status") in {"READY", "DIRECTION_ONLY"}
        and (
            plan.get("status") != "DIRECTION_ONLY"
            or isinstance(target.get("prediction_baseline"), Mapping)
        )
        and horizon_at is not None
        and not reasons
        else "BLOCKED"
        if plan is not None and plan.get("status") == "BLOCKED"
        else "WAITING"
    )
    body: dict[str, object] = {
        "schema": "options_copilot.outcome_capture_spec.v1",
        "status": status,
        "reason_codes": tuple(dict.fromkeys(reasons)),
        "subject_kind": target["subject_kind"],
        "subject_id": target["subject_id"],
        "subject_hash": target["subject_hash"],
        "symbol": target["symbol"],
        "occurred_at": occurred_at,
        "baseline_at": baseline_at,
        "registered_at": registered_at,
        "horizon": horizon,
        "target_rule": OUTCOME_TARGET_RULES[horizon],
        "horizon_at": horizon_at,
        "horizon_evidence": horizon_evidence,
        "horizon_evidence_hash": horizon_evidence_hash,
        "capture_plan": plan,
        "observation_identity": EvidenceStoreOutcomeObservationProvider.identity(
            target,
            horizon,
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    binding = target.get("prediction_candidate_binding")
    if isinstance(binding, Mapping):
        body["prediction_candidate_binding"] = binding
    baseline_request = target.get("prediction_baseline_request")
    if isinstance(baseline_request, Mapping):
        body["prediction_baseline_request"] = baseline_request
    prediction_baseline = target.get("prediction_baseline")
    if isinstance(prediction_baseline, Mapping):
        body["prediction_baseline"] = prediction_baseline
    prediction_set_hash = target.get("prediction_set_hash")
    if prediction_set_hash is not None:
        body["prediction_set_hash"] = prediction_set_hash
    predicted_direction = target.get("predicted_direction")
    if predicted_direction is not None:
        body["predicted_direction"] = predicted_direction
    thesis_hash = target.get("thesis_hash")
    if thesis_hash is not None:
        body["thesis_hash"] = thesis_hash
    body["capture_spec_hash"] = canonical_hash(body)
    frozen = freeze_json(body)
    assert isinstance(frozen, Mapping)
    return frozen


def _capture_plan(
    value: object,
    *,
    target: Mapping[str, object],
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_PLAN_MISSING")
    if (
        value.get("schema") == "options_copilot.outcome_capture_plan.v1"
        and value.get("status") in {"WAITING", "BLOCKED"}
    ):
        reason_codes = value.get("reason_codes")
        if (
            not isinstance(reason_codes, Sequence)
            or isinstance(reason_codes, (str, bytes, bytearray, memoryview))
            or not tuple(str(item).strip() for item in reason_codes)
        ):
            raise OutcomeProcessingError("OUTCOME_CAPTURE_PLAN_INVALID")
        frozen_blocked = freeze_json(
            {
                "schema": "options_copilot.outcome_capture_plan.v1",
                "status": value.get("status"),
                "reason_codes": tuple(
                    dict.fromkeys(str(item).strip() for item in reason_codes)
                ),
            }
        )
        assert isinstance(frozen_blocked, Mapping)
        return frozen_blocked
    if (
        value.get("schema") == "options_copilot.outcome_capture_plan.v1"
        and value.get("status") == "DIRECTION_ONLY"
        and set(value) == {"schema", "status", "reason_codes"}
    ):
        reason_codes = value.get("reason_codes")
        if not isinstance(reason_codes, Sequence) or isinstance(
            reason_codes,
            (str, bytes, bytearray, memoryview),
        ):
            raise OutcomeProcessingError("OUTCOME_CAPTURE_PLAN_INVALID")
        frozen_direction = freeze_json(
            {
                "schema": "options_copilot.outcome_capture_plan.v1",
                "status": "DIRECTION_ONLY",
                "reason_codes": tuple(
                    dict.fromkeys(
                        str(item).strip()
                        for item in reason_codes
                        if str(item).strip()
                    )
                ),
            }
        )
        assert isinstance(frozen_direction, Mapping)
        return frozen_direction
    required = {
        "schema",
        "status",
        "reason_codes",
        "benchmark_symbol",
        "underlying",
        "benchmark",
        "legs",
        "max_loss_usd",
    }
    optional = {
        "horizon_evidence",
        "session_calendar",
        "broker_snapshot_hash",
    }
    if (
        not required.issubset(value)
        or set(value) - required - optional
        or value.get("schema") != "options_copilot.outcome_capture_plan.v1"
        or value.get("status") != "READY"
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_PLAN_INVALID")
    reason_codes = value.get("reason_codes")
    if (
        not isinstance(reason_codes, Sequence)
        or isinstance(reason_codes, (str, bytes, bytearray, memoryview))
        or tuple(reason_codes)
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_PLAN_INVALID")
    underlying = _baseline_market_binding(
        value.get("underlying"),
        expected_symbol=str(target["symbol"]),
    )
    benchmark_symbol = _nonblank_capture(
        value.get("benchmark_symbol"),
        "OUTCOME_CAPTURE_BENCHMARK_INVALID",
    ).upper()
    benchmark = _baseline_market_binding(
        value.get("benchmark"),
        expected_symbol=benchmark_symbol,
    )
    legs_raw = value.get("legs")
    if (
        not isinstance(legs_raw, Sequence)
        or isinstance(legs_raw, (str, bytes, bytearray, memoryview))
        or len(legs_raw) < 2
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_LEGS_INVALID")
    legs = tuple(_capture_leg(item) for item in legs_raw)
    con_ids = tuple(int(item["con_id"]) for item in legs)
    if len(set(con_ids)) != len(con_ids):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_LEGS_INVALID")
    maximum_loss = _positive_decimal(
        value.get("max_loss_usd"),
        "OUTCOME_CAPTURE_MAX_LOSS_INVALID",
    )
    baseline_cutoff = _time_from(
        target.get("baseline_at", target["occurred_at"])
    )
    if str(target.get("subject_kind", "")).upper() == "PREDICTION":
        baseline_cutoff += _CAPTURE_WINDOW
    if (
        _time_from(underlying["observed_at"]) > baseline_cutoff
        or _time_from(benchmark["observed_at"]) > baseline_cutoff
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_BASELINE_TIME_TRAVEL")
    normalized: dict[str, object] = {
        "schema": "options_copilot.outcome_capture_plan.v1",
        "status": "READY",
        "reason_codes": (),
        "benchmark_symbol": benchmark_symbol,
        "underlying": underlying,
        "benchmark": benchmark,
        "legs": legs,
        "max_loss_usd": maximum_loss,
    }
    for name in optional:
        if name in value:
            normalized[name] = value[name]
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _baseline_market_binding(
    value: object,
    *,
    expected_symbol: str,
) -> Mapping[str, object]:
    required = {"symbol", "price", "observed_at", "source", "source_id", "source_hash"}
    if not isinstance(value, Mapping) or set(value) != required:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_BASELINE_INVALID")
    symbol = _nonblank_capture(
        value.get("symbol"),
        "OUTCOME_CAPTURE_BASELINE_INVALID",
    ).upper()
    if symbol != expected_symbol.upper():
        raise OutcomeProcessingError("OUTCOME_CAPTURE_BASELINE_INVALID")
    normalized = {
        "symbol": symbol,
        "price": _positive_decimal(
            value.get("price"),
            "OUTCOME_CAPTURE_BASELINE_INVALID",
        ),
        "observed_at": utc_datetime(
            _time_from(value.get("observed_at")),
            field="baseline observed_at",
        ),
        "source": _nonblank_capture(
            value.get("source"),
            "OUTCOME_CAPTURE_BASELINE_INVALID",
        ),
        "source_id": _nonblank_capture(
            value.get("source_id"),
            "OUTCOME_CAPTURE_BASELINE_INVALID",
        ),
        "source_hash": _capture_digest(
            value.get("source_hash"),
            "OUTCOME_CAPTURE_BASELINE_INVALID",
        ),
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _capture_leg(value: object) -> Mapping[str, object]:
    required = {
        "con_id",
        "side",
        "ratio",
        "multiplier",
        "strike",
        "bid",
        "ask",
    }
    optional = {"contract", "implied_volatility", "volume"}
    if (
        not isinstance(value, Mapping)
        or not required.issubset(value)
        or set(value) - required - optional
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_LEGS_INVALID")
    con_id = _positive_int(value.get("con_id"), "OUTCOME_CAPTURE_LEGS_INVALID")
    ratio = _positive_int(value.get("ratio"), "OUTCOME_CAPTURE_LEGS_INVALID")
    multiplier = _positive_int(
        value.get("multiplier"),
        "OUTCOME_CAPTURE_LEGS_INVALID",
    )
    side = str(value.get("side", "")).upper()
    if side not in {"BUY", "SELL"}:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_LEGS_INVALID")
    bid = _nonnegative_decimal(value.get("bid"), "OUTCOME_CAPTURE_LEGS_INVALID")
    ask = _nonnegative_decimal(value.get("ask"), "OUTCOME_CAPTURE_LEGS_INVALID")
    if ask < bid:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_LEGS_INVALID")
    implied_volatility = (
        None
        if value.get("implied_volatility") is None
        else _nonnegative_decimal(
            value.get("implied_volatility"),
            "OUTCOME_CAPTURE_LEGS_INVALID",
        )
    )
    volume = (
        None
        if value.get("volume") is None
        else _nonnegative_int(value.get("volume"), "OUTCOME_CAPTURE_LEGS_INVALID")
    )
    normalized = {
        "con_id": con_id,
        "side": side,
        "ratio": ratio,
        "multiplier": multiplier,
        "strike": _positive_decimal(
            value.get("strike"),
            "OUTCOME_CAPTURE_LEGS_INVALID",
        ),
        "bid": bid,
        "ask": ask,
        "implied_volatility": implied_volatility,
        "volume": volume,
    }
    if "contract" in value:
        contract = value.get("contract")
        if not isinstance(contract, Mapping):
            raise OutcomeProcessingError("OUTCOME_CAPTURE_LEGS_INVALID")
        normalized["contract"] = contract
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _capture_horizon_evidence(
    horizon: str,
    *,
    plan: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if horizon == "30M":
        return {
            "schema": "options_copilot.outcome_horizon_evidence.v1",
            "target_rule": OUTCOME_TARGET_RULES[horizon],
            "method": "ELAPSED_TIME",
            "sessions": (),
        }
    if plan is None:
        raise OutcomeProcessingError("OUTCOME_SESSION_EVIDENCE_MISSING")
    supplied = plan.get("horizon_evidence")
    if isinstance(supplied, Mapping) and horizon in supplied:
        supplied = supplied[horizon]
    if isinstance(supplied, Mapping):
        frozen = freeze_json(supplied)
        assert isinstance(frozen, Mapping)
        return frozen
    sessions = plan.get("session_calendar")
    if not isinstance(sessions, Sequence) or isinstance(
        sessions,
        (str, bytes, bytearray, memoryview),
    ):
        raise OutcomeProcessingError("OUTCOME_SESSION_EVIDENCE_MISSING")
    return {
        "schema": "options_copilot.outcome_horizon_evidence.v1",
        "target_rule": OUTCOME_TARGET_RULES[horizon],
        "method": "SESSION_CALENDAR",
        "sessions": tuple(sessions),
    }


def _raw_horizon_evidence(value: Mapping[str, object]) -> Mapping[str, object]:
    sessions = []
    raw_sessions = value.get("sessions")
    if isinstance(raw_sessions, Sequence):
        for item in raw_sessions:
            if not isinstance(item, Mapping):
                raise OutcomeProcessingError("OUTCOME_HORIZON_EVIDENCE_INVALID")
            sessions.append(
                {
                    name: item[name]
                    for name in (
                        "trading_date",
                        "open_at",
                        "close_at",
                        "source",
                        "source_id",
                        "source_hash",
                        "observed_at",
                    )
                }
            )
    frozen = freeze_json(
        {
            "schema": value["schema"],
            "target_rule": value["target_rule"],
            "method": value["method"],
            "sessions": sessions,
        }
    )
    assert isinstance(frozen, Mapping)
    return frozen


def _market_batch(
    value: object,
    *,
    horizon_at: datetime,
) -> Mapping[str, object]:
    required = {
        "schema",
        "observed_at",
        "source",
        "source_id",
        "source_hash",
        "underlyings",
        "quotes",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != required
        or value.get("schema") != "options_copilot.outcome_market_batch.v1"
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_BATCH_INVALID")
    observed_at = _capture_window_time(
        value.get("observed_at"),
        horizon_at=horizon_at,
    )
    underlyings = value.get("underlyings")
    quotes = value.get("quotes")
    if (
        not isinstance(underlyings, Sequence)
        or isinstance(underlyings, (str, bytes, bytearray, memoryview))
        or not isinstance(quotes, Sequence)
        or isinstance(quotes, (str, bytes, bytearray, memoryview))
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_BATCH_INVALID")
    normalized = {
        "schema": "options_copilot.outcome_market_batch.v1",
        "observed_at": observed_at,
        "source": _nonblank_capture(
            value.get("source"),
            "OUTCOME_CAPTURE_BATCH_INVALID",
        ),
        "source_id": _nonblank_capture(
            value.get("source_id"),
            "OUTCOME_CAPTURE_BATCH_INVALID",
        ),
        "source_hash": _capture_digest(
            value.get("source_hash"),
            "OUTCOME_CAPTURE_BATCH_INVALID",
        ),
        "underlyings": tuple(underlyings),
        "quotes": tuple(quotes),
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _partition_capture_specs(
    specs: Sequence[Mapping[str, object]],
) -> tuple[tuple[Mapping[str, object], ...], ...]:
    """Greedily partition one due-time in deterministic source order."""

    chunks: list[tuple[Mapping[str, object], ...]] = []
    current: list[Mapping[str, object]] = []
    current_symbols: set[str] = set()
    current_contracts: set[int] = set()
    for spec in specs:
        plan = spec.get("capture_plan")
        baseline_request = spec.get("prediction_baseline_request")
        if not isinstance(plan, Mapping):
            spec_symbols: set[str] = set()
            spec_contracts: set[int] = set()
        else:
            benchmark_symbol = (
                baseline_request.get("benchmark_symbol")
                if isinstance(baseline_request, Mapping)
                else (
                    spec.get("prediction_baseline", {}).get("benchmark_symbol")
                    if isinstance(spec.get("prediction_baseline"), Mapping)
                    else plan.get("benchmark_symbol")
                )
            )
            spec_symbols = {
                str(spec.get("symbol", "")).upper(),
                str(benchmark_symbol or "").upper(),
            }
            spec_symbols.discard("")
            legs = plan.get("legs")
            spec_contracts = {
                int(leg["con_id"])
                for leg in legs
                if isinstance(legs, Sequence)
                and isinstance(leg, Mapping)
                and isinstance(leg.get("con_id"), int)
                and not isinstance(leg.get("con_id"), bool)
            } if isinstance(legs, Sequence) else set()
        next_symbols = current_symbols | spec_symbols
        next_contracts = current_contracts | spec_contracts
        if current and (
            len(next_symbols) > _CAPTURE_BATCH_SYMBOL_LIMIT
            or len(next_contracts) > _CAPTURE_BATCH_CONTRACT_LIMIT
        ):
            chunks.append(tuple(current))
            current = []
            current_symbols = set()
            current_contracts = set()
        current.append(spec)
        current_symbols.update(spec_symbols)
        current_contracts.update(spec_contracts)
    if current:
        chunks.append(tuple(current))
    return tuple(chunks)


def _capture_observation(
    spec: Mapping[str, object],
    batch: Mapping[str, object],
    *,
    horizon_at: datetime,
) -> tuple[Mapping[str, object], datetime]:
    plan = spec.get("capture_plan")
    horizon_evidence = spec.get("horizon_evidence")
    if not isinstance(plan, Mapping) or not isinstance(horizon_evidence, Mapping):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_SPEC_INVALID")
    if plan.get("status") == "DIRECTION_ONLY":
        return _capture_direction_observation(
            spec,
            batch,
            horizon_at=horizon_at,
        )
    baseline_underlying = plan.get("underlying")
    baseline_benchmark = plan.get("benchmark")
    legs = plan.get("legs")
    if (
        not isinstance(baseline_underlying, Mapping)
        or not isinstance(baseline_benchmark, Mapping)
        or not isinstance(legs, Sequence)
    ):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_PLAN_INVALID")

    current_underlying = _current_underlying(
        batch,
        symbol=str(spec["symbol"]),
        horizon_at=horizon_at,
    )
    current_benchmark = _current_underlying(
        batch,
        symbol=str(plan["benchmark_symbol"]),
        horizon_at=horizon_at,
    )
    current_legs = tuple(
        _current_leg(batch, baseline=item, horizon_at=horizon_at)
        for item in legs
        if isinstance(item, Mapping)
    )
    if len(current_legs) != len(legs):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_LEGS_INVALID")
    timestamps = [
        _time_from(batch["observed_at"]),
        _time_from(current_underlying["observed_at"]),
        _time_from(current_benchmark["observed_at"]),
        *(_time_from(item["observed_at"]) for item in current_legs),
    ]
    economic_observed_at = max(timestamps)
    if economic_observed_at > horizon_at + _CAPTURE_WINDOW:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_WINDOW_MISSED")

    batch_provenance = _available_capture_provenance(
        source=str(batch["source"]),
        source_id=str(batch["source_id"]),
        source_hash=str(batch["source_hash"]),
        observed_at=_time_from(batch["observed_at"]),
    )
    underlying_provenance = _available_capture_provenance(
        source=str(batch["source"]),
        source_id=str(current_underlying["source_id"]),
        source_hash=str(current_underlying["source_hash"]),
        observed_at=_time_from(current_underlying["observed_at"]),
    )
    benchmark_provenance = _available_capture_provenance(
        source=str(batch["source"]),
        source_id=str(current_benchmark["source_id"]),
        source_hash=str(current_benchmark["source_hash"]),
        observed_at=_time_from(current_benchmark["observed_at"]),
    )

    baseline_value = Decimal("0")
    current_value = Decimal("0")
    baseline_ivs: list[Decimal] = []
    current_ivs: list[Decimal] = []
    iv_available = True
    baseline_volume = 0
    current_volume = 0
    volume_available = True
    for baseline, current in zip(legs, current_legs, strict=True):
        assert isinstance(baseline, Mapping)
        direction = Decimal("1") if baseline["side"] == "BUY" else Decimal("-1")
        quantity = Decimal(int(baseline["ratio"])) * Decimal(int(baseline["multiplier"]))
        baseline_mid = (Decimal(baseline["bid"]) + Decimal(baseline["ask"])) / 2
        current_mid = (Decimal(current["bid"]) + Decimal(current["ask"])) / 2
        baseline_value += direction * quantity * baseline_mid
        current_value += direction * quantity * current_mid
        if (
            baseline.get("implied_volatility") is None
            or current.get("implied_volatility") is None
        ):
            iv_available = False
        else:
            baseline_ivs.append(Decimal(baseline["implied_volatility"]))
            current_ivs.append(Decimal(current["implied_volatility"]))
        if baseline.get("volume") is None or current.get("volume") is None:
            volume_available = False
        else:
            baseline_volume += int(baseline["volume"])
            current_volume += int(current["volume"])

    pnl = current_value - baseline_value
    maximum_loss = Decimal(plan["max_loss_usd"])
    quote_observed_at = max(_time_from(item["observed_at"]) for item in current_legs)
    option_provenance = _available_capture_provenance(
        source=str(batch["source"]),
        source_id=str(batch["source_id"]),
        source_hash=str(batch["source_hash"]),
        observed_at=quote_observed_at,
    )
    iv_change = (
        sum(current_ivs, Decimal("0")) / Decimal(len(current_ivs))
        - sum(baseline_ivs, Decimal("0")) / Decimal(len(baseline_ivs))
        if iv_available and baseline_ivs
        else None
    )
    observation = {
        "schema": "options_copilot.outcome_observation.v2",
        "subject_kind": spec["subject_kind"],
        "subject_id": spec["subject_id"],
        "subject_hash": spec["subject_hash"],
        "horizon": spec["horizon"],
        "target_rule": spec["target_rule"],
        "horizon_at": horizon_at,
        "economic_observed_at": economic_observed_at,
        "revision_received_at": economic_observed_at,
        "horizon_evidence": horizon_evidence,
        "underlying": {
            "symbol": spec["symbol"],
            "baseline_price": baseline_underlying["price"],
            "price": current_underlying["price"],
            "provenance": underlying_provenance,
        },
        "benchmark": {
            "symbol": plan["benchmark_symbol"],
            "baseline_price": baseline_benchmark["price"],
            "price": current_benchmark["price"],
            "provenance": benchmark_provenance,
        },
        "option_market": {
            "iv_change": {
                "value": iv_change,
                "provenance": (
                    option_provenance
                    if iv_change is not None
                    else _unavailable_capture_provenance("OPTION_IV_UNAVAILABLE")
                ),
            },
            "skew_change": {
                "value": None,
                "provenance": _unavailable_capture_provenance(
                    "SKEW_HISTORY_UNAVAILABLE"
                ),
            },
            "volume_change": {
                "value": (
                    Decimal(current_volume - baseline_volume)
                    if volume_available
                    else None
                ),
                "provenance": (
                    option_provenance
                    if volume_available
                    else _unavailable_capture_provenance("OPTION_VOLUME_UNAVAILABLE")
                ),
            },
        },
        "combination": {
            "estimated_pnl_usd": pnl,
            "estimated_return": pnl / maximum_loss,
            "max_loss_usd": maximum_loss,
            "provenance": batch_provenance,
        },
        "thesis_validity": {
            "status": "UNKNOWN",
            "valid": None,
            "reason_code": "THESIS_BINDING_UNAVAILABLE",
            "thesis_hash": None,
            "provenance": _unavailable_capture_provenance(
                "THESIS_BINDING_UNAVAILABLE"
            ),
        },
    }
    frozen = freeze_json(observation)
    assert isinstance(frozen, Mapping)
    return frozen, economic_observed_at


def _direction_thesis_validity(
    spec: Mapping[str, object],
    *,
    baseline_underlying: Mapping[str, object],
    current_underlying: Mapping[str, object],
    observed_at: datetime,
    source: str,
) -> Mapping[str, object]:
    direction = str(spec.get("predicted_direction", "UNKNOWN")).upper()
    if direction not in {"BULLISH", "BEARISH"}:
        return {
            "status": "UNKNOWN",
            "valid": None,
            "reason_code": "DIRECTION_NOT_BINARY",
            "thesis_hash": None,
            "provenance": _unavailable_capture_provenance(
                "DIRECTION_NOT_BINARY"
            ),
        }
    baseline_price = Decimal(baseline_underlying["price"])
    current_price = Decimal(current_underlying["price"])
    valid = (
        current_price > baseline_price
        if direction == "BULLISH"
        else current_price < baseline_price
    )
    return {
        "status": "VALID" if valid else "INVALID",
        "valid": valid,
        "reason_code": (
            "PREDICTED_DIRECTION_MATCHED"
            if valid
            else "PREDICTED_DIRECTION_MISSED"
        ),
        "thesis_hash": spec["thesis_hash"],
        "provenance": _available_capture_provenance(
            source=source,
            source_id=str(current_underlying["source_id"]),
            source_hash=str(current_underlying["source_hash"]),
            observed_at=observed_at,
        ),
    }


def _capture_direction_observation(
    spec: Mapping[str, object],
    batch: Mapping[str, object],
    *,
    horizon_at: datetime,
) -> tuple[Mapping[str, object], datetime]:
    """Resolve news direction without inventing unavailable option economics."""

    baseline = spec.get("prediction_baseline")
    horizon_evidence = spec.get("horizon_evidence")
    if not isinstance(baseline, Mapping) or not isinstance(horizon_evidence, Mapping):
        raise OutcomeProcessingError("OUTCOME_CAPTURE_SPEC_INVALID")
    baseline_underlying = baseline.get("underlying")
    baseline_benchmark = baseline.get("benchmark")
    benchmark_symbol = str(baseline.get("benchmark_symbol", "")).upper()
    if (
        not isinstance(baseline_underlying, Mapping)
        or not isinstance(baseline_benchmark, Mapping)
        or not benchmark_symbol
    ):
        raise OutcomeProcessingError("OUTCOME_PREDICTION_BASELINE_INVALID")
    current_underlying = _current_underlying(
        batch,
        symbol=str(spec["symbol"]),
        horizon_at=horizon_at,
    )
    current_benchmark = _current_underlying(
        batch,
        symbol=benchmark_symbol,
        horizon_at=horizon_at,
    )
    economic_observed_at = max(
        _time_from(batch["observed_at"]),
        _time_from(current_underlying["observed_at"]),
        _time_from(current_benchmark["observed_at"]),
    )
    source = str(batch["source"])
    unavailable_option = _unavailable_capture_provenance(
        "OPTION_COMBINATION_NOT_BOUND"
    )
    observation = {
        "schema": "options_copilot.outcome_observation.v2",
        "subject_kind": spec["subject_kind"],
        "subject_id": spec["subject_id"],
        "subject_hash": spec["subject_hash"],
        "horizon": spec["horizon"],
        "target_rule": spec["target_rule"],
        "horizon_at": horizon_at,
        "economic_observed_at": economic_observed_at,
        "revision_received_at": economic_observed_at,
        "horizon_evidence": horizon_evidence,
        "underlying": {
            "symbol": spec["symbol"],
            "baseline_price": baseline_underlying["price"],
            "price": current_underlying["price"],
            "provenance": _available_capture_provenance(
                source=source,
                source_id=str(current_underlying["source_id"]),
                source_hash=str(current_underlying["source_hash"]),
                observed_at=_time_from(current_underlying["observed_at"]),
            ),
        },
        "benchmark": {
            "symbol": benchmark_symbol,
            "baseline_price": baseline_benchmark["price"],
            "price": current_benchmark["price"],
            "provenance": _available_capture_provenance(
                source=source,
                source_id=str(current_benchmark["source_id"]),
                source_hash=str(current_benchmark["source_hash"]),
                observed_at=_time_from(current_benchmark["observed_at"]),
            ),
        },
        "option_market": {
            name: {"value": None, "provenance": unavailable_option}
            for name in ("iv_change", "skew_change", "volume_change")
        },
        "combination": {
            "estimated_pnl_usd": None,
            "estimated_return": None,
            "max_loss_usd": None,
            "provenance": unavailable_option,
        },
        "thesis_validity": _direction_thesis_validity(
            spec,
            baseline_underlying=baseline_underlying,
            current_underlying=current_underlying,
            observed_at=economic_observed_at,
            source=source,
        ),
    }
    frozen = freeze_json(observation)
    assert isinstance(frozen, Mapping)
    return frozen, economic_observed_at


def _capture_prediction_baseline(
    spec: Mapping[str, object],
    batch: Mapping[str, object],
    *,
    expected_at: datetime,
) -> tuple[Mapping[str, object], datetime]:
    request = spec.get("prediction_baseline_request")
    if (
        not isinstance(request, Mapping)
        or request.get("schema")
        != "options_copilot.prediction_baseline_request.v1"
    ):
        raise OutcomeProcessingError("OUTCOME_PREDICTION_BASELINE_INVALID")
    benchmark_symbol = _nonblank_capture(
        request.get("benchmark_symbol"),
        "OUTCOME_PREDICTION_BASELINE_INVALID",
    ).upper()
    underlying = _current_underlying(
        batch,
        symbol=str(spec["symbol"]),
        horizon_at=expected_at,
    )
    benchmark = _current_underlying(
        batch,
        symbol=benchmark_symbol,
        horizon_at=expected_at,
    )
    received_at = max(
        _time_from(batch["observed_at"]),
        _time_from(underlying["observed_at"]),
        _time_from(benchmark["observed_at"]),
    )
    source = str(batch["source"])
    baseline = freeze_json(
        {
            "schema": "options_copilot.prediction_baseline.v1",
            "status": "AVAILABLE",
            "benchmark_symbol": benchmark_symbol,
            "captured_at": received_at,
            "underlying": {
                "symbol": str(spec["symbol"]),
                "price": underlying["price"],
                "observed_at": underlying["observed_at"],
                "source": source,
                "source_id": underlying["source_id"],
                "source_hash": underlying["source_hash"],
            },
            "benchmark": {
                "symbol": benchmark_symbol,
                "price": benchmark["price"],
                "observed_at": benchmark["observed_at"],
                "source": source,
                "source_id": benchmark["source_id"],
                "source_hash": benchmark["source_hash"],
            },
        }
    )
    assert isinstance(baseline, Mapping)
    return baseline, received_at


def _current_underlying(
    batch: Mapping[str, object],
    *,
    symbol: str,
    horizon_at: datetime,
) -> Mapping[str, object]:
    rows = batch.get("underlyings")
    assert isinstance(rows, Sequence)
    matches = tuple(
        item
        for item in rows
        if isinstance(item, Mapping)
        and str(item.get("symbol", "")).upper() == symbol.upper()
    )
    if len(matches) != 1:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_UNDERLYING_UNAVAILABLE")
    value = matches[0]
    required = {"symbol", "price", "observed_at", "source_id", "source_hash"}
    if set(value) != required:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_UNDERLYING_INVALID")
    normalized = {
        "symbol": symbol.upper(),
        "price": _positive_decimal(
            value.get("price"),
            "OUTCOME_CAPTURE_UNDERLYING_INVALID",
        ),
        "observed_at": _capture_window_time(
            value.get("observed_at"),
            horizon_at=horizon_at,
        ),
        "source_id": _nonblank_capture(
            value.get("source_id"),
            "OUTCOME_CAPTURE_UNDERLYING_INVALID",
        ),
        "source_hash": _capture_digest(
            value.get("source_hash"),
            "OUTCOME_CAPTURE_UNDERLYING_INVALID",
        ),
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _current_leg(
    batch: Mapping[str, object],
    *,
    baseline: Mapping[str, object],
    horizon_at: datetime,
) -> Mapping[str, object]:
    rows = batch.get("quotes")
    assert isinstance(rows, Sequence)
    matches = tuple(
        item
        for item in rows
        if isinstance(item, Mapping) and item.get("con_id") == baseline["con_id"]
    )
    if len(matches) != 1:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_QUOTE_UNAVAILABLE")
    value = matches[0]
    required = {"con_id", "bid", "ask", "observed_at"}
    optional = {"implied_volatility", "volume"}
    if not required.issubset(value) or set(value) - required - optional:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_QUOTE_INVALID")
    bid = _nonnegative_decimal(value.get("bid"), "OUTCOME_CAPTURE_QUOTE_INVALID")
    ask = _nonnegative_decimal(value.get("ask"), "OUTCOME_CAPTURE_QUOTE_INVALID")
    if ask < bid:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_QUOTE_INVALID")
    normalized = {
        "con_id": int(baseline["con_id"]),
        "bid": bid,
        "ask": ask,
        "implied_volatility": (
            None
            if value.get("implied_volatility") is None
            else _nonnegative_decimal(
                value.get("implied_volatility"),
                "OUTCOME_CAPTURE_QUOTE_INVALID",
            )
        ),
        "volume": (
            None
            if value.get("volume") is None
            else _nonnegative_int(
                value.get("volume"),
                "OUTCOME_CAPTURE_QUOTE_INVALID",
            )
        ),
        "observed_at": _capture_window_time(
            value.get("observed_at"),
            horizon_at=horizon_at,
        ),
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _available_capture_provenance(
    *,
    source: str,
    source_id: str,
    source_hash: str,
    observed_at: datetime,
) -> Mapping[str, object]:
    return {
        "status": "AVAILABLE",
        "source": source,
        "source_id": source_id,
        "source_hash": source_hash,
        "observed_at": observed_at,
    }


def _unavailable_capture_provenance(reason: str) -> Mapping[str, object]:
    return {
        "status": "UNAVAILABLE",
        "reason_code": reason,
        "source": None,
        "source_id": None,
        "source_hash": None,
        "observed_at": None,
    }


def _capture_window_time(value: object, *, horizon_at: datetime) -> datetime:
    observed_at = _time_from(value)
    if not horizon_at <= observed_at <= horizon_at + _CAPTURE_WINDOW:
        raise OutcomeProcessingError("OUTCOME_CAPTURE_WINDOW_MISSED")
    return observed_at


def _positive_decimal(value: object, reason: str) -> Decimal:
    result = _decimal_capture(value, reason)
    if result <= 0:
        raise OutcomeProcessingError(reason)
    return result


def _nonnegative_decimal(value: object, reason: str) -> Decimal:
    result = _decimal_capture(value, reason)
    if result < 0:
        raise OutcomeProcessingError(reason)
    return result


def _decimal_capture(value: object, reason: str) -> Decimal:
    if isinstance(value, bool):
        raise OutcomeProcessingError(reason)
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise OutcomeProcessingError(reason) from None
    if not result.is_finite():
        raise OutcomeProcessingError(reason)
    return result


def _positive_int(value: object, reason: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise OutcomeProcessingError(reason)
    return value


def _nonnegative_int(value: object, reason: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise OutcomeProcessingError(reason)
    return value


def _nonblank_capture(value: object, reason: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OutcomeProcessingError(reason)
    return value.strip()


def _capture_digest(value: object, reason: str) -> str:
    text = str(value)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise OutcomeProcessingError(reason)
    return text


def _capture_reason(exc: BaseException) -> str:
    reason = _reason(exc)
    if reason.startswith("OUTCOME_CAPTURE_") or reason.startswith(
        "OUTCOME_PREDICTION_"
    ) or reason in {
        "OUTCOME_CAPTURE_WINDOW_MISSED",
        "OUTCOME_CAPTURE_BATCH_INVALID",
        "OUTCOME_CAPTURE_UNDERLYING_UNAVAILABLE",
        "OUTCOME_CAPTURE_UNDERLYING_INVALID",
        "OUTCOME_CAPTURE_QUOTE_UNAVAILABLE",
        "OUTCOME_CAPTURE_QUOTE_INVALID",
        "OUTCOME_CAPTURE_PLAN_INVALID",
        "OUTCOME_CAPTURE_LEGS_INVALID",
        "OUTCOME_CAPTURE_SPEC_INVALID",
        "OUTCOME_OBSERVATION_CONFLICTED",
    }:
        return reason
    return "OUTCOME_CAPTURE_ADAPTER_FAILED"


def _retryable_per_spec_failure(reason: str) -> bool:
    return reason not in {
        "OUTCOME_CAPTURE_WINDOW_MISSED",
        "OUTCOME_CAPTURE_PLAN_INVALID",
        "OUTCOME_CAPTURE_LEGS_INVALID",
        "OUTCOME_CAPTURE_SPEC_INVALID",
        "OUTCOME_OBSERVATION_CONFLICTED",
    }


def _capture_calendar_sessions(
    provider: object,
    *,
    now: datetime,
) -> tuple[Mapping[str, object], ...] | None:
    try:
        snapshot = provider.snapshot(now=now)  # type: ignore[attr-defined]
        status = getattr(getattr(snapshot, "status", None), "value", None)
        verify = getattr(snapshot, "verify_hash", None)
        sessions = tuple(getattr(snapshot, "sessions"))
        observed_at = utc_datetime(
            getattr(snapshot, "observed_at"),
            field="calendar observed_at",
        )
        calendar_hash = _capture_digest(
            getattr(snapshot, "calendar_hash"),
            "OUTCOME_SESSION_EVIDENCE_INVALID",
        )
        if status != "READY" or not callable(verify) or verify() is not True:
            return None
    except Exception:
        return None
    output: list[Mapping[str, object]] = []
    for session in sessions:
        try:
            trading_date = getattr(session, "trading_date")
            opened_at = utc_datetime(
                getattr(session, "open_utc"),
                field="session open_at",
            )
            closed_at = utc_datetime(
                getattr(session, "close_utc"),
                field="session close_at",
            )
            body = {
                "trading_date": trading_date.isoformat(),
                "open_at": opened_at,
                "close_at": closed_at,
                "source": str(getattr(snapshot, "source")),
                "source_id": f"calendar:{calendar_hash}:{trading_date.isoformat()}",
                "source_hash": canonical_hash(
                    {
                        "calendar_hash": calendar_hash,
                        "trading_date": trading_date,
                        "open_at": opened_at,
                        "close_at": closed_at,
                    }
                ),
                "observed_at": observed_at,
            }
            frozen = freeze_json(body)
            assert isinstance(frozen, Mapping)
            output.append(frozen)
        except Exception:
            return None
    return tuple(output) if output else None


def _call_cursor_provider(
    provider: Callable[..., Sequence[object]],
    *,
    after_sequence: int,
) -> Sequence[object]:
    try:
        parameters = inspect.signature(provider).parameters.values()
        cursor_aware = any(
            parameter.name == "after_sequence"
            or parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )
    except (TypeError, ValueError):
        cursor_aware = True
    return (
        provider(after_sequence=after_sequence)
        if cursor_aware
        else provider()
    )


def _target_source_sequence(target: Mapping[str, object]) -> int:
    raw = target.get("source_sequence")
    if raw is None:
        context = target.get("binding_context")
        raw = context.get("source_sequence") if isinstance(context, Mapping) else None
    return raw if isinstance(raw, int) and not isinstance(raw, bool) and raw > 0 else 0


def _prediction_baseline_at(prediction: object) -> datetime:
    """Start the hard capture window only once the prediction is durable."""

    predicted_at = utc_datetime(
        getattr(prediction, "predicted_at"),
        field="predicted_at",
    )
    raw_appended_at = getattr(prediction, "appended_at", predicted_at)
    try:
        appended_at = utc_datetime(raw_appended_at, field="appended_at")
    except (TypeError, ValueError):
        return predicted_at
    return max(predicted_at, appended_at)


def _prediction_baseline_window_open(
    target: Mapping[str, object],
    *,
    checked_at: datetime,
) -> bool:
    try:
        baseline_at = _time_from(
            target.get("baseline_at", target["occurred_at"])
        )
    except (KeyError, TypeError, ValueError):
        return False
    return baseline_at <= checked_at <= baseline_at + _CAPTURE_WINDOW


def _bind_prediction_candidate(
    prediction: Mapping[str, object],
    candidates: Sequence[Mapping[str, object]],
    *,
    prediction_baseline: Mapping[str, object],
    horizon_at: datetime | None,
) -> Mapping[str, object]:
    plan = prediction.get("capture_plan")
    if isinstance(plan, Mapping) and plan.get("status") == "BLOCKED":
        return prediction
    symbol = str(prediction.get("symbol", "")).upper()
    baseline_benchmark_symbol = str(
        prediction_baseline.get("benchmark_symbol", "")
    ).upper()
    baseline_underlying = prediction_baseline.get("underlying")
    baseline_benchmark = prediction_baseline.get("benchmark")
    if (
        prediction_baseline.get("status") != "AVAILABLE"
        or not baseline_benchmark_symbol
        or not isinstance(baseline_underlying, Mapping)
        or not isinstance(baseline_benchmark, Mapping)
    ):
        return prediction
    prediction_context = prediction.get("binding_context")
    prediction_events = _binding_event_ids(prediction_context)
    try:
        prediction_occurred_at = _time_from(prediction["occurred_at"])
    except (KeyError, TypeError, ValueError):
        prediction_occurred_at = None
    eligible: list[tuple[int, int, int, str, Mapping[str, object]]] = []
    for candidate in candidates:
        candidate_plan = candidate.get("capture_plan")
        context = candidate.get("binding_context")
        if (
            str(candidate.get("symbol", "")).upper() != symbol
            or not isinstance(candidate_plan, Mapping)
            or candidate_plan.get("status") != "READY"
            or str(candidate_plan.get("benchmark_symbol", "")).upper()
            != baseline_benchmark_symbol
            or not isinstance(context, Mapping)
        ):
            continue
        try:
            candidate_occurred_at = _time_from(candidate["occurred_at"])
        except (KeyError, TypeError, ValueError):
            continue
        if (
            prediction_occurred_at is None
            or candidate_occurred_at > prediction_occurred_at
        ):
            continue
        rank = context.get("rank")
        if not isinstance(rank, int) or isinstance(rank, bool) or rank != 1:
            continue
        candidate_events = _binding_event_ids(context)
        exact = bool(prediction_events and prediction_events & candidate_events)
        eligible.append(
            (
                0 if exact else 1,
                rank,
                -_target_source_sequence(candidate),
                str(candidate.get("subject_hash", "")),
                candidate,
            )
        )
    if not eligible:
        body = thaw_json(prediction)
        if not isinstance(body, dict):
            return prediction
        body["prediction_baseline"] = thaw_json(prediction_baseline)
        body["capture_plan"] = {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "DIRECTION_ONLY",
            "reason_codes": ("PREDICTION_CANDIDATE_NOT_POINT_IN_TIME",),
        }
        frozen_direction = freeze_json(body)
        assert isinstance(frozen_direction, Mapping)
        return frozen_direction
    _, _, _, _, candidate = min(eligible)
    context = candidate["binding_context"]
    assert isinstance(context, Mapping)
    body = thaw_json(prediction)
    if not isinstance(body, dict):
        return prediction
    capture_plan = thaw_json(candidate["capture_plan"])
    if not isinstance(capture_plan, dict):
        return prediction
    capture_plan["underlying"] = thaw_json(baseline_underlying)
    capture_plan["benchmark"] = thaw_json(baseline_benchmark)
    capture_plan["benchmark_symbol"] = baseline_benchmark_symbol
    body["capture_plan"] = capture_plan
    body["prediction_baseline"] = thaw_json(prediction_baseline)
    body["capture_plan"] = (
        capture_plan
        if capture_plan.get("status") == "READY"
        else {
            "schema": "options_copilot.outcome_capture_plan.v1",
            "status": "DIRECTION_ONLY",
            "reason_codes": (),
        }
    )
    exact = bool(prediction_events & _binding_event_ids(context))
    body["prediction_candidate_binding"] = {
        "prediction_hash": str(
            prediction.get("prediction_hash") or prediction.get("subject_hash")
        ),
        "independence_key": str(
            prediction.get("independence_key")
            or (
                prediction_context.get("independence_key")
                if isinstance(prediction_context, Mapping)
                else ""
            )
        ),
        "ranking_snapshot_id": context.get("ranking_snapshot_id"),
        "ranking_snapshot_hash": context.get("ranking_snapshot_hash"),
        "scan_run_id": context.get("scan_run_id"),
        "candidate_id": context.get("candidate_id") or candidate.get("subject_id"),
        "candidate_hash": context.get("candidate_hash") or candidate.get("subject_hash"),
        "ranking_basis_hash": context.get("ranking_basis_hash"),
        "rank": context.get("rank"),
        "candidate_occurred_at": candidate.get("occurred_at"),
        "candidate_binding_cutoff_at": prediction_occurred_at,
        "event_ids": tuple(sorted(_binding_event_ids(context))),
        "binding_rule": (
            "EXACT_EVENT_SAME_SYMBOL_RANK_ONE"
            if exact
            else "SAME_SYMBOL_RANK_ONE_FALLBACK"
        ),
    }
    frozen = freeze_json(body)
    assert isinstance(frozen, Mapping)
    return frozen


def _binding_event_ids(value: object) -> set[str]:
    if not isinstance(value, Mapping):
        return set()
    raw = value.get("event_ids")
    if isinstance(raw, Sequence) and not isinstance(
        raw,
        (str, bytes, bytearray, memoryview),
    ):
        return {str(item) for item in raw if str(item)}
    event_id = value.get("event_id")
    return {str(event_id)} if event_id else set()


def _target_with_calendar(
    target: Mapping[str, object],
    sessions: tuple[Mapping[str, object], ...] | None,
) -> Mapping[str, object]:
    if sessions is None:
        return target
    body = thaw_json(target)
    if not isinstance(body, dict):
        return target
    plan = body.get("capture_plan")
    if not isinstance(plan, dict) or plan.get("status") != "READY":
        return target
    plan["session_calendar"] = [thaw_json(item) for item in sessions]
    frozen = freeze_json(body)
    assert isinstance(frozen, Mapping)
    return frozen


def _shadow_head(ledger: ShadowLearningLedger) -> str:
    return ledger.head_hash()


def _time_from(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return utc_datetime(parsed)


def _reason(exc: BaseException) -> str:
    text = str(exc).strip()
    return text if text and " " not in text else type(exc).__name__.upper()


__all__ = [
    "EvidenceStoreOutcomeObservationProvider",
    "ExactHorizonOutcomeCapture",
    "ImmutableOutcomeProcessor",
    "OUTCOME_CAPTURE_SPEC_KIND",
    "OUTCOME_HORIZONS",
    "OUTCOME_OBSERVATION_KIND",
    "OUTCOME_TARGET_RULES",
    "OutcomeCaptureRegistrationResult",
    "OutcomeCaptureCoordinator",
    "OutcomeCaptureLoop",
    "OutcomeCaptureResult",
    "OutcomeMarketAdapter",
    "OutcomeObservationProvider",
    "OutcomeObservationTerminal",
    "OutcomeObservationUnavailable",
    "OutcomeProcessingError",
    "OutcomeProcessingResult",
    "RankingOutcomeTargetCursor",
    "ShadowPredictionTargetCursor",
]
