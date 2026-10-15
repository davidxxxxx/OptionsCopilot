"""Local, append-only Champion/Challenger shadow-learning ledger.

This module has one deliberately narrow authority boundary: it records and
replays research evidence.  It cannot change production weights or rules,
approve risk, promote a Challenger, contact a broker, or use the network.

Every prediction is bound to the exact point-in-time evidence hashes that were
knowable when it was made.  A later outcome is bound to the immutable
prediction hash.  SQLite append-only triggers plus a global hash chain make
ordinary mutation impossible and out-of-band mutation detectable on every
read.  Thirty independent resolved samples change only the descriptive stage
to ``DISCOVERY``; operation remains ``SHADOW_ONLY``.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import threading

from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


SCHEMA_VERSION = 1
DISCOVERY_SAMPLE_THRESHOLD = 30
GENESIS_HASH = "0" * 64
EXTERNAL_HUMAN_AUTHORITY = "EXTERNAL_HUMAN_APPROVAL_REQUIRED"
_CALIBRATION_OUTCOME_SCHEMA = "options_copilot.outcome_observation.v2"
_CALIBRATION_OUTCOME_HORIZONS = frozenset(
    {"30M", "SESSION_CLOSE", "1D", "3D", "5D"}
)

_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")


class RecordType(str, Enum):
    THESIS = "THESIS"
    EVIDENCE = "EVIDENCE"
    PREDICTION = "PREDICTION"
    OUTCOME = "OUTCOME"


class GovernanceStage(str, Enum):
    COLLECTING = "COLLECTING"
    DISCOVERY = "DISCOVERY"


class ShadowLearningError(RuntimeError):
    """Base class for fail-closed shadow-ledger errors."""


class DuplicateRecordError(ShadowLearningError):
    """An immutable identity, prediction outcome, or document was repeated."""


class UnknownRecordError(ShadowLearningError):
    """A referenced immutable record does not exist."""


class BindingError(ShadowLearningError):
    """A supplied or stored hash binding does not match its target."""


class TimeTravelError(ShadowLearningError):
    """A record claims knowledge or an outcome before it was possible."""


class LedgerTampered(ShadowLearningError):
    """Stored rows, bindings, chronology, or the append chain were changed."""


@dataclass(frozen=True, slots=True)
class EvidenceBinding:
    evidence_id: str
    evidence_hash: str

    def as_dict(self) -> dict[str, str]:
        return {
            "evidence_id": self.evidence_id,
            "evidence_hash": self.evidence_hash,
        }


@dataclass(frozen=True, slots=True)
class ThesisRecord:
    sequence: int
    thesis_id: str
    champion_version: str
    challenger_version: str
    thesis: Mapping[str, object]
    created_at: datetime
    tags: tuple[str, ...]
    content_hash: str
    previous_hash: str
    chain_hash: str
    appended_at: datetime

    @property
    def record_type(self) -> RecordType:
        return RecordType.THESIS

    def as_dict(self) -> dict[str, object]:
        return shadow_record_to_dict(self)


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    sequence: int
    evidence_id: str
    thesis_id: str
    thesis_hash: str
    challenger_version: str
    source: str
    evidence: Mapping[str, object]
    published_at: datetime
    first_seen_at: datetime
    tags: tuple[str, ...]
    content_hash: str
    previous_hash: str
    chain_hash: str
    appended_at: datetime

    @property
    def record_type(self) -> RecordType:
        return RecordType.EVIDENCE

    def as_dict(self) -> dict[str, object]:
        return shadow_record_to_dict(self)


@dataclass(frozen=True, slots=True)
class PredictionRecord:
    sequence: int
    prediction_id: str
    thesis_id: str
    thesis_hash: str
    champion_version: str
    challenger_version: str
    prediction: Mapping[str, object]
    predicted_at: datetime
    horizon_at: datetime | None
    evidence_bindings: tuple[EvidenceBinding, ...]
    evidence_bundle_hash: str
    independence_key: str
    tags: tuple[str, ...]
    content_hash: str
    previous_hash: str
    chain_hash: str
    appended_at: datetime

    @property
    def record_type(self) -> RecordType:
        return RecordType.PREDICTION

    @property
    def evidence_ids(self) -> tuple[str, ...]:
        return tuple(binding.evidence_id for binding in self.evidence_bindings)

    @property
    def evidence_hashes(self) -> tuple[str, ...]:
        return tuple(binding.evidence_hash for binding in self.evidence_bindings)

    def as_dict(self) -> dict[str, object]:
        return shadow_record_to_dict(self)


@dataclass(frozen=True, slots=True)
class OutcomeRecord:
    sequence: int
    outcome_id: str
    prediction_id: str
    prediction_hash: str
    thesis_id: str
    challenger_version: str
    independence_key: str
    outcome: Mapping[str, object]
    observed_at: datetime
    resolved_at: datetime
    tags: tuple[str, ...]
    content_hash: str
    previous_hash: str
    chain_hash: str
    appended_at: datetime

    @property
    def record_type(self) -> RecordType:
        return RecordType.OUTCOME

    def as_dict(self) -> dict[str, object]:
        return shadow_record_to_dict(self)


ShadowRecord = ThesisRecord | EvidenceRecord | PredictionRecord | OutcomeRecord


@dataclass(frozen=True, slots=True)
class VerifiedReplaySnapshot:
    """One immutable, fully verified genesis-to-head ledger projection."""

    records: tuple[ShadowRecord, ...]
    verified_head_sequence: int
    verified_head_hash: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.verified_head_sequence, int)
            or isinstance(self.verified_head_sequence, bool)
            or self.verified_head_sequence < 0
        ):
            raise ValueError("verified_head_sequence must be non-negative")
        _digest("verified_head_hash", self.verified_head_hash)
        if len(self.records) != self.verified_head_sequence:
            raise ValueError("verified replay snapshot sequence is incomplete")
        if self.records:
            if (
                self.records[-1].sequence != self.verified_head_sequence
                or self.records[-1].chain_hash != self.verified_head_hash
            ):
                raise ValueError("verified replay snapshot head binding mismatch")
        elif self.verified_head_hash != GENESIS_HASH:
            raise ValueError("empty verified replay snapshot must use genesis hash")


def _shadow_record_id(record: ShadowRecord) -> str:
    if isinstance(record, ThesisRecord):
        return record.thesis_id
    if isinstance(record, EvidenceRecord):
        return record.evidence_id
    if isinstance(record, PredictionRecord):
        return record.prediction_id
    return record.outcome_id


@dataclass(slots=True)
class _ShadowBindingState:
    records: dict[str, ShadowRecord] = field(default_factory=dict)
    theses: dict[str, ThesisRecord] = field(default_factory=dict)
    evidence: dict[str, EvidenceRecord] = field(default_factory=dict)
    predictions: dict[str, PredictionRecord] = field(default_factory=dict)
    outcomes: dict[str, OutcomeRecord] = field(default_factory=dict)

    def accept(self, record: ShadowRecord) -> None:
        record_id = _shadow_record_id(record)
        if record_id in self.records:
            raise LedgerTampered("duplicate immutable record identity")
        if isinstance(record, ThesisRecord):
            if record.thesis_id in self.theses:
                raise LedgerTampered("duplicate thesis identity")
            self.theses[record.thesis_id] = record
            self.records[record_id] = record
            return
        thesis = self.theses.get(record.thesis_id)
        if thesis is None or thesis.sequence >= record.sequence:
            raise LedgerTampered("record references a missing or future thesis")
        if isinstance(record, EvidenceRecord):
            if record.thesis_hash != thesis.content_hash:
                raise LedgerTampered("evidence thesis hash binding mismatch")
            if record.challenger_version != thesis.challenger_version:
                raise LedgerTampered("evidence Challenger identity binding mismatch")
            if record.first_seen_at < thesis.created_at:
                raise LedgerTampered("evidence predates its thesis")
            if record.published_at > record.first_seen_at:
                raise LedgerTampered("evidence publication chronology is invalid")
            self.evidence[record.evidence_id] = record
            self.records[record_id] = record
            return
        if isinstance(record, PredictionRecord):
            if record.thesis_hash != thesis.content_hash:
                raise LedgerTampered("prediction thesis hash binding mismatch")
            if (
                record.champion_version != thesis.champion_version
                or record.challenger_version != thesis.challenger_version
            ):
                raise LedgerTampered("prediction model identity binding mismatch")
            if record.predicted_at < thesis.created_at:
                raise LedgerTampered("prediction predates its thesis")
            if record.horizon_at is not None and (
                record.horizon_at <= record.predicted_at
            ):
                raise LedgerTampered("prediction horizon chronology is invalid")
            if not record.evidence_bindings:
                raise LedgerTampered("prediction has no evidence binding")
            if tuple(sorted(record.evidence_ids)) != record.evidence_ids:
                raise LedgerTampered("prediction evidence bindings are not canonical")
            if len(set(record.evidence_ids)) != len(record.evidence_ids):
                raise LedgerTampered("prediction repeats an evidence binding")
            if canonical_hash(
                [item.as_dict() for item in record.evidence_bindings]
            ) != record.evidence_bundle_hash:
                raise LedgerTampered("prediction evidence bundle hash mismatch")
            for binding in record.evidence_bindings:
                item = self.evidence.get(binding.evidence_id)
                if item is None or item.sequence >= record.sequence:
                    raise LedgerTampered(
                        "prediction references missing or future evidence"
                    )
                if item.thesis_id != record.thesis_id:
                    raise LedgerTampered("prediction evidence thesis mismatch")
                if item.content_hash != binding.evidence_hash:
                    raise LedgerTampered("prediction evidence hash binding mismatch")
                if item.first_seen_at > record.predicted_at:
                    raise LedgerTampered("prediction contains future evidence")
            self.predictions[record.prediction_id] = record
            self.records[record_id] = record
            return
        assert isinstance(record, OutcomeRecord)
        prediction = self.predictions.get(record.prediction_id)
        if prediction is None or prediction.sequence >= record.sequence:
            raise LedgerTampered("outcome references a missing or future prediction")
        if record.prediction_id in self.outcomes:
            raise LedgerTampered("prediction has more than one outcome")
        if record.prediction_hash != prediction.content_hash:
            raise LedgerTampered("outcome prediction hash binding mismatch")
        if (
            record.thesis_id != prediction.thesis_id
            or record.challenger_version != prediction.challenger_version
            or record.independence_key != prediction.independence_key
        ):
            raise LedgerTampered("outcome prediction identity binding mismatch")
        if record.observed_at < prediction.predicted_at:
            raise LedgerTampered("outcome observation predates prediction")
        if record.resolved_at < record.observed_at:
            raise LedgerTampered("outcome resolution predates observation")
        self.outcomes[record.prediction_id] = record
        self.records[record_id] = record


@dataclass(slots=True)
class _VerifiedRecordCursorState:
    verified_head_sequence: int
    verified_head_hash: str
    after_sequence: int = 0
    previous_hash: str = GENESIS_HASH
    complete: bool = False
    failed: bool = False
    bindings: _ShadowBindingState = field(default_factory=_ShadowBindingState)


@dataclass(frozen=True, slots=True)
class VerifiedRecordCursor:
    """Opaque cursor for one coherent, incrementally verified ledger head."""

    _owner_token: object
    _state: _VerifiedRecordCursorState

    @property
    def after_sequence(self) -> int:
        return self._state.after_sequence

    @property
    def verified_head_sequence(self) -> int:
        return self._state.verified_head_sequence

    @property
    def verified_head_hash(self) -> str:
        return self._state.verified_head_hash

    @property
    def complete(self) -> bool:
        return self._state.complete


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    thesis: ThesisRecord
    evidence: tuple[EvidenceRecord, ...]
    prediction: PredictionRecord
    outcome: OutcomeRecord | None
    replayed_as_of: datetime | None

    @property
    def point_in_time_evidence_hash(self) -> str:
        return self.prediction.evidence_bundle_hash

    def as_dict(self) -> dict[str, object]:
        return replay_record_to_dict(self)


@dataclass(frozen=True, slots=True)
class SimilarityMatch:
    replay: ReplayRecord
    similarity: float
    distance: float
    shared_tags: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return similarity_match_to_dict(self)


@dataclass(frozen=True, slots=True)
class GovernanceState:
    challenger_version: str | None
    independent_samples: int
    discovery_threshold: int
    stage: GovernanceStage
    mode: str = "SHADOW_ONLY"
    grade: str = "COLLECTING"
    can_auto_promote: bool = False
    can_change_production_weights: bool = False
    can_change_production_rules: bool = False
    a_grade_15_percent_unlocked: bool = False
    promotion_requires_external_human_approval: bool = True
    a_grade_requires_external_human_approval: bool = True
    authority_boundary: str = EXTERNAL_HUMAN_AUTHORITY

    def __post_init__(self) -> None:
        if (
            not isinstance(self.independent_samples, int)
            or isinstance(self.independent_samples, bool)
            or self.independent_samples < 0
        ):
            raise ValueError("independent_samples must be a nonnegative integer")
        if self.discovery_threshold != DISCOVERY_SAMPLE_THRESHOLD:
            raise ValueError("the Discovery threshold is fixed at 30 samples")
        expected_stage = (
            GovernanceStage.DISCOVERY
            if self.independent_samples >= DISCOVERY_SAMPLE_THRESHOLD
            else GovernanceStage.COLLECTING
        )
        if self.stage is not expected_stage or self.grade != expected_stage.value:
            raise ValueError("governance stage must be derived from independent samples")
        if self.mode != "SHADOW_ONLY":
            raise ValueError("shadow learning mode cannot become production")
        if (
            self.can_auto_promote
            or self.can_change_production_weights
            or self.can_change_production_rules
            or self.a_grade_15_percent_unlocked
        ):
            raise ValueError("shadow learning cannot grant production authority")
        if not (
            self.promotion_requires_external_human_approval
            and self.a_grade_requires_external_human_approval
        ):
            raise ValueError("production decisions require external human approval")
        if self.authority_boundary != EXTERNAL_HUMAN_AUTHORITY:
            raise ValueError("authority boundary cannot be changed by shadow learning")

    @property
    def discovery_ready(self) -> bool:
        return self.stage is GovernanceStage.DISCOVERY

    def as_dict(self) -> dict[str, object]:
        return {
            "challenger_version": self.challenger_version,
            "independent_samples": self.independent_samples,
            "discovery_threshold": self.discovery_threshold,
            "stage": self.stage.value,
            "mode": self.mode,
            "grade": self.grade,
            "can_auto_promote": self.can_auto_promote,
            "can_change_production_weights": self.can_change_production_weights,
            "can_change_production_rules": self.can_change_production_rules,
            "a_grade_15_percent_unlocked": self.a_grade_15_percent_unlocked,
            "promotion_requires_external_human_approval": (
                self.promotion_requires_external_human_approval
            ),
            "a_grade_requires_external_human_approval": (
                self.a_grade_requires_external_human_approval
            ),
            "authority_boundary": self.authority_boundary,
        }


class ShadowLearningLedger:
    """Thread-safe SQLite ledger with research-only, shadow-only authority."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._cursor_owner_token = object()
        self._verified_snapshot_cache: VerifiedReplaySnapshot | None = None
        self._verified_snapshot_bindings: _ShadowBindingState | None = None
        self._verified_snapshot_guard: tuple[int, int, int] | None = None
        self._closed = False
        self._connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA busy_timeout=10000")
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            synchronous = int(
                self._connection.execute("PRAGMA synchronous").fetchone()[0]
            )
            self._synchronous = {
                0: "off",
                1: "normal",
                2: "full",
                3: "extra",
            }.get(synchronous, str(synchronous))
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._migrate()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "ShadowLearningLedger":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        return self._synchronous

    @property
    def schema_version(self) -> int:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute("PRAGMA user_version").fetchone()
            if row is None:
                raise ShadowLearningError("shadow learning schema version is unavailable")
            return int(row[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def record_thesis(
        self,
        thesis_id: str,
        *,
        champion_version: str,
        challenger_version: str,
        thesis: Mapping[str, object],
        created_at: datetime,
        tags: Sequence[str] = (),
    ) -> ThesisRecord:
        thesis_id = _identifier("thesis_id", thesis_id)
        champion_version = _text("champion_version", champion_version)
        challenger_version = _text("challenger_version", challenger_version)
        if champion_version == challenger_version:
            raise ValueError("Champion and Challenger versions must be distinct")
        created_at = _time(created_at, "created_at")
        normalized_tags = _tags(tags)
        thesis_body = _document_mapping("thesis", thesis)
        document = {
            "schema_version": SCHEMA_VERSION,
            "record_type": RecordType.THESIS.value,
            "thesis_id": thesis_id,
            "champion_version": champion_version,
            "challenger_version": challenger_version,
            "created_at": datetime_text(created_at),
            "tags": list(normalized_tags),
            "thesis": thesis_body,
        }
        record = self._append(
            record_type=RecordType.THESIS,
            record_id=thesis_id,
            thesis_id=thesis_id,
            prediction_id=None,
            challenger_version=challenger_version,
            independence_key=None,
            occurred_at=created_at,
            tags=normalized_tags,
            document=document,
        )
        assert isinstance(record, ThesisRecord)
        return record

    append_thesis = record_thesis

    def record_evidence(
        self,
        evidence_id: str,
        thesis_id: str,
        *,
        source: str,
        evidence: Mapping[str, object],
        published_at: datetime,
        first_seen_at: datetime,
        tags: Sequence[str] = (),
    ) -> EvidenceRecord:
        evidence_id = _identifier("evidence_id", evidence_id)
        thesis = self.get_thesis(thesis_id)
        source = _text("source", source)
        published_at = _time(published_at, "published_at")
        first_seen_at = _time(first_seen_at, "first_seen_at")
        if published_at > first_seen_at:
            raise TimeTravelError("published_at cannot be after first_seen_at")
        if first_seen_at < thesis.created_at:
            raise TimeTravelError("evidence cannot be first seen before its thesis exists")
        normalized_tags = _tags(tags)
        evidence_body = _document_mapping("evidence", evidence)
        document = {
            "schema_version": SCHEMA_VERSION,
            "record_type": RecordType.EVIDENCE.value,
            "evidence_id": evidence_id,
            "thesis_id": thesis.thesis_id,
            "thesis_hash": thesis.content_hash,
            "challenger_version": thesis.challenger_version,
            "source": source,
            "published_at": datetime_text(published_at),
            "first_seen_at": datetime_text(first_seen_at),
            "tags": list(normalized_tags),
            "evidence": evidence_body,
        }
        record = self._append(
            record_type=RecordType.EVIDENCE,
            record_id=evidence_id,
            thesis_id=thesis.thesis_id,
            prediction_id=None,
            challenger_version=thesis.challenger_version,
            independence_key=None,
            occurred_at=first_seen_at,
            tags=normalized_tags,
            document=document,
        )
        assert isinstance(record, EvidenceRecord)
        return record

    append_evidence = record_evidence

    def record_prediction(
        self,
        prediction_id: str,
        thesis_id: str,
        *,
        evidence_ids: Sequence[str],
        prediction: Mapping[str, object],
        predicted_at: datetime,
        independence_key: str,
        horizon_at: datetime | None = None,
        tags: Sequence[str] = (),
        challenger_version: str | None = None,
        evidence_hashes: Mapping[str, str] | None = None,
    ) -> PredictionRecord:
        prediction_id = _identifier("prediction_id", prediction_id)
        thesis = self.get_thesis(thesis_id)
        predicted_at = _time(predicted_at, "predicted_at")
        if predicted_at < thesis.created_at:
            raise TimeTravelError("prediction cannot precede its thesis")
        if horizon_at is not None:
            horizon_at = _time(horizon_at, "horizon_at")
            if horizon_at <= predicted_at:
                raise TimeTravelError("horizon_at must be after predicted_at")
        if challenger_version is not None:
            challenger_version = _text("challenger_version", challenger_version)
            if challenger_version != thesis.challenger_version:
                raise BindingError("Challenger version does not match the bound thesis")
        independence_key = _text("independence_key", independence_key, maximum=512)
        if isinstance(evidence_ids, (str, bytes, bytearray)):
            raise TypeError("evidence_ids must be a sequence of identifiers")
        normalized_ids = tuple(
            sorted(_identifier("evidence_id", item) for item in evidence_ids)
        )
        if not normalized_ids:
            raise ValueError("a prediction requires at least one evidence record")
        if len(set(normalized_ids)) != len(normalized_ids):
            raise DuplicateRecordError("duplicate evidence IDs in one prediction")
        evidence_records = tuple(self.get_evidence(item) for item in normalized_ids)
        for item in evidence_records:
            if item.thesis_id != thesis.thesis_id:
                raise BindingError("prediction evidence belongs to another thesis")
            if item.first_seen_at > predicted_at:
                raise TimeTravelError(
                    f"evidence {item.evidence_id} was not knowable at predicted_at"
                )
        bindings = tuple(
            EvidenceBinding(item.evidence_id, item.content_hash)
            for item in evidence_records
        )
        if evidence_hashes is not None:
            if not isinstance(evidence_hashes, Mapping):
                raise TypeError("evidence_hashes must be a mapping")
            supplied = {
                _identifier("evidence_id", key): _digest("evidence_hash", value)
                for key, value in evidence_hashes.items()
            }
            expected = {
                binding.evidence_id: binding.evidence_hash for binding in bindings
            }
            if supplied != expected:
                raise BindingError("supplied evidence hashes do not match stored evidence")
        binding_document = [binding.as_dict() for binding in bindings]
        bundle_hash = canonical_hash(binding_document)
        normalized_tags = _tags(tags)
        if not normalized_tags:
            normalized_tags = tuple(
                sorted(
                    {
                        *thesis.tags,
                        *(tag for item in evidence_records for tag in item.tags),
                    }
                )
            )
        prediction_body = _document_mapping("prediction", prediction)
        document = {
            "schema_version": SCHEMA_VERSION,
            "record_type": RecordType.PREDICTION.value,
            "prediction_id": prediction_id,
            "thesis_id": thesis.thesis_id,
            "thesis_hash": thesis.content_hash,
            "champion_version": thesis.champion_version,
            "challenger_version": thesis.challenger_version,
            "predicted_at": datetime_text(predicted_at),
            "horizon_at": None if horizon_at is None else datetime_text(horizon_at),
            "evidence_bindings": binding_document,
            "evidence_bundle_hash": bundle_hash,
            "independence_key": independence_key,
            "tags": list(normalized_tags),
            "prediction": prediction_body,
        }
        record = self._append(
            record_type=RecordType.PREDICTION,
            record_id=prediction_id,
            thesis_id=thesis.thesis_id,
            prediction_id=prediction_id,
            challenger_version=thesis.challenger_version,
            independence_key=independence_key,
            occurred_at=predicted_at,
            tags=normalized_tags,
            document=document,
        )
        assert isinstance(record, PredictionRecord)
        return record

    append_prediction = record_prediction

    def resolve_outcome(
        self,
        prediction_id: str,
        *,
        outcome: Mapping[str, object],
        observed_at: datetime,
        resolved_at: datetime | None = None,
        outcome_id: str | None = None,
        prediction_hash: str | None = None,
    ) -> OutcomeRecord:
        prediction = self.get_prediction(prediction_id)
        observed_at = _time(observed_at, "observed_at")
        resolved_at = self._now() if resolved_at is None else _time(
            resolved_at, "resolved_at"
        )
        if observed_at < prediction.predicted_at:
            raise TimeTravelError("outcome observation cannot precede its prediction")
        if resolved_at < observed_at:
            raise TimeTravelError("outcome resolution cannot precede observation")
        if prediction_hash is not None:
            prediction_hash = _digest("prediction_hash", prediction_hash)
            if prediction_hash != prediction.content_hash:
                raise BindingError("supplied prediction hash does not match prediction")
        outcome_id = _identifier(
            "outcome_id", outcome_id or f"outcome:{prediction.prediction_id}"
        )
        outcome_body = _document_mapping("outcome", outcome)
        document = {
            "schema_version": SCHEMA_VERSION,
            "record_type": RecordType.OUTCOME.value,
            "outcome_id": outcome_id,
            "prediction_id": prediction.prediction_id,
            "prediction_hash": prediction.content_hash,
            "thesis_id": prediction.thesis_id,
            "challenger_version": prediction.challenger_version,
            "independence_key": prediction.independence_key,
            "observed_at": datetime_text(observed_at),
            "resolved_at": datetime_text(resolved_at),
            "tags": list(prediction.tags),
            "outcome": outcome_body,
        }
        try:
            record = self._append(
                record_type=RecordType.OUTCOME,
                record_id=outcome_id,
                thesis_id=prediction.thesis_id,
                prediction_id=prediction.prediction_id,
                challenger_version=prediction.challenger_version,
                independence_key=prediction.independence_key,
                occurred_at=resolved_at,
                tags=prediction.tags,
                document=document,
            )
        except DuplicateRecordError as exc:
            if self.get_outcome(prediction.prediction_id) is not None:
                raise DuplicateRecordError(
                    f"prediction {prediction.prediction_id} already has an outcome"
                ) from exc
            raise
        assert isinstance(record, OutcomeRecord)
        return record

    record_outcome = resolve_outcome
    append_outcome = resolve_outcome

    def get_thesis(self, thesis_id: str) -> ThesisRecord:
        record = self._get(_identifier("thesis_id", thesis_id))
        if not isinstance(record, ThesisRecord):
            raise UnknownRecordError(f"unknown thesis: {thesis_id}")
        return record

    def get_evidence(self, evidence_id: str) -> EvidenceRecord:
        record = self._get(_identifier("evidence_id", evidence_id))
        if not isinstance(record, EvidenceRecord):
            raise UnknownRecordError(f"unknown evidence: {evidence_id}")
        return record

    def get_prediction(self, prediction_id: str) -> PredictionRecord:
        record = self._get(_identifier("prediction_id", prediction_id))
        if not isinstance(record, PredictionRecord):
            raise UnknownRecordError(f"unknown prediction: {prediction_id}")
        return record

    def get_outcome(self, prediction_id: str) -> OutcomeRecord | None:
        prediction_id = _identifier("prediction_id", prediction_id)
        with self._lock:
            self.verified_replay_snapshot()
            bindings = self._verified_snapshot_bindings
            if bindings is None:
                raise LedgerTampered("verified shadow bindings are unavailable")
            return bindings.outcomes.get(prediction_id)

    def get_record(self, record_id: str) -> ShadowRecord:
        record = self._get(_identifier("record_id", record_id))
        if record is None:
            raise UnknownRecordError(f"unknown shadow record: {record_id}")
        return record

    def query_records(
        self,
        *,
        record_type: RecordType | str | None = None,
        challenger_version: str | None = None,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> tuple[ShadowRecord, ...]:
        """Query immutable rows without exposing the SQLite connection."""

        normalized_type = (
            None if record_type is None else _coerce_record_type(record_type)
        )
        if challenger_version is not None:
            challenger_version = _text("challenger_version", challenger_version)
        if not isinstance(after_sequence, int) or isinstance(after_sequence, bool):
            raise TypeError("after_sequence must be an integer")
        if after_sequence < 0:
            raise ValueError("after_sequence cannot be negative")
        limit = _limit(limit, maximum=5000)
        with self._lock:
            snapshot = self.verified_replay_snapshot()
            output: list[ShadowRecord] = []
            for record in snapshot.records:
                if record.sequence <= after_sequence:
                    continue
                if (
                    normalized_type is not None
                    and record.record_type is not normalized_type
                ):
                    continue
                if (
                    challenger_version is not None
                    and record.challenger_version != challenger_version
                ):
                    continue
                output.append(record)
                if len(output) >= limit:
                    break
            return tuple(output)

    def query_predictions_by_ids(
        self,
        prediction_ids: Iterable[str],
        *,
        challenger_version: str,
    ) -> tuple[PredictionRecord, ...]:
        """Restore a bounded exact prediction set after one integrity pass."""

        checked_ids = tuple(
            dict.fromkeys(
                _identifier("prediction_id", value) for value in prediction_ids
            )
        )
        if len(checked_ids) > 2500:
            raise ValueError("at most 2500 prediction_ids may be restored")
        if not checked_ids:
            return ()
        checked_challenger = _text("challenger_version", challenger_version)
        with self._lock:
            self.verified_replay_snapshot()
            bindings = self._verified_snapshot_bindings
            if bindings is None:
                raise LedgerTampered("verified shadow bindings are unavailable")
            predictions = tuple(
                record
                for prediction_id in checked_ids
                for record in (bindings.predictions.get(prediction_id),)
                if record is not None
                and record.challenger_version == checked_challenger
            )
            return tuple(sorted(predictions, key=lambda item: item.sequence))

    def record_counts(
        self,
        *,
        snapshot: VerifiedReplaySnapshot | None = None,
    ) -> Mapping[str, int]:
        snapshot = snapshot or self.verified_replay_snapshot()
        by_type: dict[str, int] = {}
        for record in snapshot.records:
            name = record.record_type.value
            by_type[name] = by_type.get(name, 0) + 1
        return {
            "theses": by_type.get(RecordType.THESIS.value, 0),
            "evidence": by_type.get(RecordType.EVIDENCE.value, 0),
            "predictions": by_type.get(RecordType.PREDICTION.value, 0),
            "outcomes": by_type.get(RecordType.OUTCOME.value, 0),
        }

    def challenger_versions(
        self,
        *,
        snapshot: VerifiedReplaySnapshot | None = None,
    ) -> tuple[str, ...]:
        snapshot = snapshot or self.verified_replay_snapshot()
        return tuple(
            sorted(
                {
                    record.challenger_version
                    for record in snapshot.records
                    if isinstance(record, ThesisRecord)
                }
            )
        )

    def replay(
        self,
        prediction_id: str,
        *,
        as_of: datetime | None = None,
    ) -> ReplayRecord:
        prediction_id = _identifier("prediction_id", prediction_id)
        cutoff = None if as_of is None else _time(as_of, "as_of")
        with self._lock:
            self.verified_replay_snapshot()
            bindings = self._verified_snapshot_bindings
            if bindings is None:
                raise LedgerTampered("verified shadow bindings are unavailable")
            return self._replay_from_bindings(
                prediction_id,
                bindings=bindings,
                cutoff=cutoff,
            )

    get_replay = replay

    def query_replays(
        self,
        *,
        challenger_version: str | None = None,
        as_of: datetime | None = None,
        resolved_only: bool = False,
        limit: int = 500,
        after_sequence: int = 0,
    ) -> tuple[ReplayRecord, ...]:
        limit = _limit(limit, maximum=5000)
        if (
            not isinstance(after_sequence, int)
            or isinstance(after_sequence, bool)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        cutoff = None if as_of is None else _time(as_of, "as_of")
        if challenger_version is not None:
            challenger_version = _text("challenger_version", challenger_version)
        with self._lock:
            snapshot = self.verified_replay_snapshot()
            bindings = self._verified_snapshot_bindings
            if bindings is None:
                raise LedgerTampered("verified shadow bindings are unavailable")
            output: list[ReplayRecord] = []
            for record in snapshot.records:
                if not isinstance(record, PredictionRecord):
                    continue
                if record.sequence <= after_sequence:
                    continue
                if (
                    challenger_version is not None
                    and record.challenger_version != challenger_version
                ):
                    continue
                if cutoff is not None and record.predicted_at > cutoff:
                    continue
                replay = self._replay_from_bindings(
                    record.prediction_id,
                    bindings=bindings,
                    cutoff=cutoff,
                )
                if resolved_only and replay.outcome is None:
                    continue
                output.append(replay)
                if len(output) >= limit:
                    break
            return tuple(output)

    def head_hash(self) -> str:
        """Return the verified append-chain tail without a record-count cap."""

        self.assert_integrity()
        with self._lock:
            row = self._connection.execute(
                "SELECT chain_hash FROM shadow_learning_records "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        return GENESIS_HASH if row is None else str(row["chain_hash"])

    def open_verified_record_cursor(self) -> VerifiedRecordCursor:
        """Capture one physical head for a genesis-to-head verified traversal."""

        self._ensure_open()
        with self._lock:
            self._assert_cursor_guard_locked()
            head_sequence, head_hash = self._cursor_head_locked()
        return VerifiedRecordCursor(
            self._cursor_owner_token,
            _VerifiedRecordCursorState(
                verified_head_sequence=head_sequence,
                verified_head_hash=head_hash,
                complete=head_sequence == 0,
            ),
        )

    def verified_replay_snapshot(self) -> VerifiedReplaySnapshot:
        """Return a cached verified prefix, rebuilding only when the head changes."""

        self._ensure_open()
        with self._lock:
            self._assert_cursor_guard_locked()
            head_sequence, head_hash = self._cursor_head_locked()
            cached = self._verified_snapshot_cache
            current_guard = self._snapshot_cache_guard_locked()
            cache_guard_matches = (
                self._verified_snapshot_guard is not None
                and self._verified_snapshot_guard == current_guard
            )
            if (
                cached is not None
                and cached.verified_head_sequence == head_sequence
                and cached.verified_head_hash == head_hash
                and cache_guard_matches
            ):
                return cached

            if cached is not None:
                if head_sequence < cached.verified_head_sequence:
                    raise LedgerTampered(
                        "shadow verified replay snapshot head moved backwards"
                    )
                if (
                    head_sequence == cached.verified_head_sequence
                    and head_hash != cached.verified_head_hash
                ):
                    raise LedgerTampered(
                        "shadow verified replay snapshot head hash changed"
                    )
                if cached.verified_head_sequence:
                    anchor = self._connection.execute(
                        "SELECT chain_hash FROM shadow_learning_records WHERE sequence=?",
                        (cached.verified_head_sequence,),
                    ).fetchone()
                    if (
                        anchor is None
                        or str(anchor["chain_hash"]) != cached.verified_head_hash
                    ):
                        raise LedgerTampered(
                            "shadow verified replay snapshot prefix changed"
                        )
                bindings_cache = self._verified_snapshot_bindings
                if bindings_cache is not None and cache_guard_matches:
                    bindings = _ShadowBindingState(
                        records=dict(bindings_cache.records),
                        theses=dict(bindings_cache.theses),
                        evidence=dict(bindings_cache.evidence),
                        predictions=dict(bindings_cache.predictions),
                        outcomes=dict(bindings_cache.outcomes),
                    )
                    rows = self._connection.execute(
                        "SELECT * FROM shadow_learning_records "
                        "WHERE sequence > ? AND sequence <= ? ORDER BY sequence",
                        (cached.verified_head_sequence, head_sequence),
                    ).fetchall()
                    expected_sequence = cached.verified_head_sequence + 1
                    expected_previous = cached.verified_head_hash
                    appended: list[ShadowRecord] = []
                    for row in rows:
                        record, expected_previous = _verified_row(
                            row,
                            expected_sequence=expected_sequence,
                            expected_previous=expected_previous,
                            context="shadow verified replay snapshot increment",
                        )
                        bindings.accept(record)
                        appended.append(record)
                        expected_sequence += 1
                    if expected_sequence - 1 != head_sequence:
                        raise LedgerTampered(
                            "shadow verified replay snapshot increment contains a gap"
                        )
                    if expected_previous != head_hash:
                        raise LedgerTampered(
                            "shadow verified replay snapshot increment head mismatch"
                        )
                    snapshot = VerifiedReplaySnapshot(
                        records=(*cached.records, *appended),
                        verified_head_sequence=head_sequence,
                        verified_head_hash=head_hash,
                    )
                    self._verified_snapshot_cache = snapshot
                    self._verified_snapshot_bindings = bindings
                    self._verified_snapshot_guard = current_guard
                    return snapshot

            rows = self._connection.execute(
                "SELECT * FROM shadow_learning_records "
                "WHERE sequence <= ? ORDER BY sequence",
                (head_sequence,),
            ).fetchall()
            expected_sequence = 1
            expected_previous = GENESIS_HASH
            bindings = _ShadowBindingState()
            records: list[ShadowRecord] = []
            for row in rows:
                record, expected_previous = _verified_row(
                    row,
                    expected_sequence=expected_sequence,
                    expected_previous=expected_previous,
                    context="shadow verified replay snapshot",
                )
                bindings.accept(record)
                records.append(record)
                expected_sequence += 1
            if expected_sequence - 1 != head_sequence:
                raise LedgerTampered("shadow verified replay snapshot contains a gap")
            if expected_previous != head_hash:
                raise LedgerTampered("shadow verified replay snapshot head hash mismatch")
            snapshot = VerifiedReplaySnapshot(
                records=tuple(records),
                verified_head_sequence=head_sequence,
                verified_head_hash=head_hash,
            )
            self._verified_snapshot_cache = snapshot
            self._verified_snapshot_bindings = bindings
            self._verified_snapshot_guard = current_guard
            return snapshot

    def verified_record_cursor_page(
        self,
        *,
        cursor: VerifiedRecordCursor,
        limit: int = 5000,
    ) -> tuple[tuple[ShadowRecord, ...], VerifiedRecordCursor]:
        """Verify the next page against a stable head and prior page hash.

        Each immutable row and its cross-record bindings are checked once as
        the cursor advances from genesis.  A cursor becomes unusable after any
        verification failure so a partially checked page cannot be retried or
        treated as authoritative.
        """

        if not isinstance(cursor, VerifiedRecordCursor):
            raise TypeError("cursor must be a VerifiedRecordCursor")
        if cursor._owner_token is not self._cursor_owner_token:
            raise ValueError("verified record cursor belongs to another ledger")
        state = cursor._state
        if state.failed:
            raise LedgerTampered("verified record cursor is invalid")
        limit = _limit(limit, maximum=5000)
        self._ensure_open()
        with self._lock:
            state.failed = True
            self._assert_cursor_guard_locked()
            if state.complete:
                state.failed = False
                return (), cursor
            if state.after_sequence:
                anchor = self._connection.execute(
                    "SELECT chain_hash FROM shadow_learning_records WHERE sequence=?",
                    (state.after_sequence,),
                ).fetchone()
                if (
                    anchor is None
                    or str(anchor["chain_hash"]) != state.previous_hash
                ):
                    raise LedgerTampered("shadow verified cursor anchor mismatch")
            elif state.previous_hash != GENESIS_HASH:
                raise LedgerTampered("shadow verified cursor genesis mismatch")
            rows = self._connection.execute(
                "SELECT * FROM shadow_learning_records "
                "WHERE sequence > ? AND sequence <= ? "
                "ORDER BY sequence LIMIT ?",
                (
                    state.after_sequence,
                    state.verified_head_sequence,
                    limit,
                ),
            ).fetchall()
            if not rows:
                raise LedgerTampered("shadow verified cursor sequence contains a gap")

            expected_sequence = state.after_sequence + 1
            expected_previous = state.previous_hash
            records: list[ShadowRecord] = []
            for row in rows:
                record, expected_previous = _verified_row(
                    row,
                    expected_sequence=expected_sequence,
                    expected_previous=expected_previous,
                    context="shadow verified cursor",
                )
                state.bindings.accept(record)
                records.append(record)
                expected_sequence += 1

            after_sequence = expected_sequence - 1
            complete = after_sequence == state.verified_head_sequence
            if not complete and len(rows) < limit:
                raise LedgerTampered("shadow verified cursor sequence contains a gap")
            if complete and expected_previous != state.verified_head_hash:
                raise LedgerTampered("shadow verified cursor head hash mismatch")

            state.after_sequence = after_sequence
            state.previous_hash = expected_previous
            state.complete = complete
            state.failed = False
        return tuple(records), cursor

    def _cursor_head_locked(self) -> tuple[int, str]:
        row = self._connection.execute(
            "SELECT sequence, chain_hash FROM shadow_learning_records "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return 0, GENESIS_HASH
        return int(row["sequence"]), str(row["chain_hash"])

    def _snapshot_cache_guard_locked(self) -> tuple[int, int, int]:
        """Bind a warm verification to physical DB and schema mutations."""

        data_version = int(
            self._connection.execute("PRAGMA data_version").fetchone()[0]
        )
        schema_version = int(
            self._connection.execute("PRAGMA schema_version").fetchone()[0]
        )
        return data_version, schema_version, self._connection.total_changes

    def open_prediction_cursor(
        self,
    ) -> tuple[tuple[PredictionRecord, ...], int, str]:
        """Verify once, then return all predictions and the physical chain tail."""

        self.assert_integrity()
        with self._lock:
            snapshot = self.verified_replay_snapshot()
            predictions = tuple(
                record
                for record in snapshot.records
                if isinstance(record, PredictionRecord)
            )
            return (
                predictions,
                snapshot.verified_head_sequence,
                snapshot.verified_head_hash,
            )

    def prediction_cursor_page(
        self,
        *,
        after_sequence: int,
        previous_hash: str,
        limit: int = 5000,
    ) -> tuple[tuple[PredictionRecord, ...], int, str]:
        """Read and verify one append-only page anchored to the prior tail."""

        if (
            not isinstance(after_sequence, int)
            or isinstance(after_sequence, bool)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        previous_hash = _digest("previous_hash", previous_hash)
        limit = _limit(limit, maximum=5000)
        self._ensure_open()
        with self._lock:
            self._assert_cursor_guard_locked()
            if after_sequence:
                anchor = self._connection.execute(
                    "SELECT chain_hash FROM shadow_learning_records WHERE sequence=?",
                    (after_sequence,),
                ).fetchone()
                if anchor is None or str(anchor["chain_hash"]) != previous_hash:
                    raise LedgerTampered("shadow cursor anchor mismatch")
            elif previous_hash != GENESIS_HASH:
                raise LedgerTampered("shadow cursor genesis mismatch")
            rows = self._connection.execute(
                "SELECT * FROM shadow_learning_records WHERE sequence > ? "
                "ORDER BY sequence LIMIT ?",
                (after_sequence, limit),
            ).fetchall()
            expected_sequence = after_sequence + 1
            expected_previous = previous_hash
            predictions: list[PredictionRecord] = []
            for row in rows:
                sequence = int(row["sequence"])
                if sequence != expected_sequence:
                    raise LedgerTampered("shadow cursor sequence contains a gap")
                try:
                    document = json.loads(str(row["document_json"]))
                    if canonical_json(document) != str(row["document_json"]):
                        raise LedgerTampered("shadow cursor document mismatch")
                    content_hash = canonical_hash(document)
                    if content_hash != str(row["content_hash"]):
                        raise LedgerTampered("shadow cursor content hash mismatch")
                    if str(row["previous_hash"]) != expected_previous:
                        raise LedgerTampered("shadow cursor previous hash mismatch")
                    chain_hash = _chain_hash(
                        sequence,
                        expected_previous,
                        content_hash,
                    )
                    if chain_hash != str(row["chain_hash"]):
                        raise LedgerTampered("shadow cursor chain hash mismatch")
                    record = _row_to_record(row, document=document)
                    _assert_row_projection(row, record)
                except LedgerTampered:
                    raise
                except Exception as exc:
                    raise LedgerTampered(
                        f"invalid shadow cursor row at sequence {sequence}"
                    ) from exc
                if isinstance(record, PredictionRecord):
                    predictions.append(record)
                expected_sequence += 1
                expected_previous = chain_hash
        return tuple(predictions), expected_sequence - 1, expected_previous

    def _assert_cursor_guard_locked(self) -> None:
        if (
            int(self._connection.execute("PRAGMA user_version").fetchone()[0])
            != SCHEMA_VERSION
        ):
            raise LedgerTampered("shadow cursor schema guard failed")
        rows = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name IN (?, ?)",
            (
                "shadow_learning_records_no_update",
                "shadow_learning_records_no_delete",
            ),
        ).fetchall()
        if {str(row["name"]) for row in rows} != {
            "shadow_learning_records_no_update",
            "shadow_learning_records_no_delete",
        }:
            raise LedgerTampered("shadow cursor append-only trigger guard failed")

    replay_query = query_replays

    def query_similar(
        self,
        tags: Sequence[str] | None = None,
        *,
        prediction_id: str | None = None,
        challenger_version: str | None = None,
        as_of: datetime | None = None,
        resolved_only: bool = False,
        exclude_source: bool = True,
        minimum_similarity: float = 0.0,
        limit: int = 10,
    ) -> tuple[SimilarityMatch, ...]:
        """Return deterministic Jaccard similarity over immutable context tags."""

        if tags is None:
            if prediction_id is None:
                raise ValueError("tags or prediction_id is required")
            source = self.get_prediction(prediction_id)
            query_tags = source.tags
        else:
            query_tags = _tags(tags)
            if not query_tags:
                raise ValueError("similarity query requires at least one tag")
            source = None
            if prediction_id is not None:
                source = self.get_prediction(prediction_id)
        if not isinstance(minimum_similarity, (int, float)) or isinstance(
            minimum_similarity, bool
        ):
            raise TypeError("minimum_similarity must be numeric")
        minimum_similarity = float(minimum_similarity)
        if not math.isfinite(minimum_similarity) or not 0.0 <= minimum_similarity <= 1.0:
            raise ValueError("minimum_similarity must be between 0 and 1")
        limit = _limit(limit, maximum=1000)
        candidates = self.query_replays(
            challenger_version=challenger_version,
            as_of=as_of,
            resolved_only=resolved_only,
            limit=5000,
        )
        query_set = frozenset(query_tags)
        matches: list[SimilarityMatch] = []
        for replay in candidates:
            candidate = replay.prediction
            if (
                exclude_source
                and source is not None
                and candidate.prediction_id == source.prediction_id
            ):
                continue
            candidate_set = frozenset(candidate.tags)
            union = query_set | candidate_set
            similarity = 0.0 if not union else len(query_set & candidate_set) / len(union)
            if similarity < minimum_similarity:
                continue
            matches.append(
                SimilarityMatch(
                    replay=replay,
                    similarity=similarity,
                    distance=1.0 - similarity,
                    shared_tags=tuple(sorted(query_set & candidate_set)),
                )
            )
        matches.sort(
            key=lambda item: (
                -item.similarity,
                item.replay.prediction.predicted_at,
                item.replay.prediction.prediction_id,
            )
        )
        return tuple(matches[:limit])

    similarity_query = query_similar

    def independent_sample_keys(
        self,
        challenger_version: str | None = None,
        *,
        snapshot: VerifiedReplaySnapshot | None = None,
    ) -> tuple[str, ...]:
        if challenger_version is not None:
            challenger_version = _text("challenger_version", challenger_version)
        snapshot = snapshot or self.verified_replay_snapshot()
        rows = tuple(
            record
            for record in snapshot.records
            if isinstance(record, OutcomeRecord)
        )
        if challenger_version is None:
            resolved_challengers = {
                row.challenger_version
                for row in rows
            }
            if len(resolved_challengers) > 1:
                raise ShadowLearningError(
                    "challenger_version is required when multiple challengers have outcomes"
                )
            challenger_version = next(iter(resolved_challengers), None)
        legacy_keys: set[str] = set()
        multi_horizon: dict[str, list[tuple[str, bool]]] = {}
        for row in rows:
            if (
                challenger_version is not None
                and row.challenger_version != challenger_version
            ):
                continue
            key = row.independence_key
            outcome = row.outcome
            if not isinstance(outcome, Mapping):
                raise LedgerTampered("shadow outcome body is not a mapping")
            if outcome.get("schema") != _CALIBRATION_OUTCOME_SCHEMA:
                legacy_keys.add(key)
                continue
            horizon = str(outcome.get("horizon", "")).upper()
            eligible = (
                outcome.get("subject_kind") == "PREDICTION"
                and outcome.get("calibration_eligible") is True
                and horizon in _CALIBRATION_OUTCOME_HORIZONS
            )
            multi_horizon.setdefault(key, []).append((horizon, eligible))
        complete_keys = {
            key
            for key, values in multi_horizon.items()
            if len(values) == len(_CALIBRATION_OUTCOME_HORIZONS)
            and all(eligible for _, eligible in values)
            and {horizon for horizon, _ in values}
            == _CALIBRATION_OUTCOME_HORIZONS
        }
        return tuple(sorted(legacy_keys | complete_keys))

    def independent_sample_count(
        self,
        challenger_version: str | None = None,
        *,
        snapshot: VerifiedReplaySnapshot | None = None,
    ) -> int:
        return len(
            self.independent_sample_keys(
                challenger_version,
                snapshot=snapshot,
            )
        )

    count_independent_samples = independent_sample_count

    def governance_state(
        self,
        challenger_version: str | None = None,
        *,
        snapshot: VerifiedReplaySnapshot | None = None,
    ) -> GovernanceState:
        snapshot = snapshot or self.verified_replay_snapshot()
        if challenger_version is None:
            challengers = self.challenger_versions(snapshot=snapshot)
            if challengers:
                states = tuple(
                    self.governance_state(version, snapshot=snapshot)
                    for version in challengers
                )
                return max(states, key=lambda state: state.independent_samples)
        count = self.independent_sample_count(
            challenger_version,
            snapshot=snapshot,
        )
        stage = (
            GovernanceStage.DISCOVERY
            if count >= DISCOVERY_SAMPLE_THRESHOLD
            else GovernanceStage.COLLECTING
        )
        return GovernanceState(
            challenger_version=challenger_version,
            independent_samples=count,
            discovery_threshold=DISCOVERY_SAMPLE_THRESHOLD,
            stage=stage,
            grade=stage.value,
        )

    governance = governance_state

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self.verified_replay_snapshot()

    def _get(self, record_id: str) -> ShadowRecord | None:
        with self._lock:
            self.verified_replay_snapshot()
            bindings = self._verified_snapshot_bindings
            if bindings is None:
                raise LedgerTampered("verified shadow bindings are unavailable")
            return bindings.records.get(record_id)

    @staticmethod
    def _replay_from_bindings(
        prediction_id: str,
        *,
        bindings: _ShadowBindingState,
        cutoff: datetime | None,
    ) -> ReplayRecord:
        prediction = bindings.predictions.get(prediction_id)
        if prediction is None:
            raise UnknownRecordError(f"unknown prediction: {prediction_id}")
        if cutoff is not None and prediction.predicted_at > cutoff:
            raise TimeTravelError("prediction did not exist at the replay cutoff")
        thesis = bindings.theses.get(prediction.thesis_id)
        if thesis is None:
            raise LedgerTampered("verified prediction thesis binding is unavailable")
        try:
            evidence = tuple(
                bindings.evidence[binding.evidence_id]
                for binding in prediction.evidence_bindings
            )
        except KeyError as exc:
            raise LedgerTampered(
                "verified prediction evidence binding is unavailable"
            ) from exc
        outcome = bindings.outcomes.get(prediction.prediction_id)
        if outcome is not None and cutoff is not None and outcome.resolved_at > cutoff:
            outcome = None
        return ReplayRecord(
            thesis=thesis,
            evidence=evidence,
            prediction=prediction,
            outcome=outcome,
            replayed_as_of=cutoff,
        )

    def _append(
        self,
        *,
        record_type: RecordType,
        record_id: str,
        thesis_id: str,
        prediction_id: str | None,
        challenger_version: str,
        independence_key: str | None,
        occurred_at: datetime,
        tags: tuple[str, ...],
        document: Mapping[str, object],
    ) -> ShadowRecord:
        appended_at = self._now()
        if occurred_at > appended_at:
            raise TimeTravelError("record timestamp cannot be in the ledger clock's future")
        document_json = canonical_json(document)
        content_hash = canonical_hash(document)
        previous_guard = self._verified_snapshot_guard
        try:
            with self._transaction():
                # The warm snapshot proves the immutable prefix.  Each local
                # append is then verified as one new chain suffix instead of
                # replaying the entire ledger from genesis.
                self.verified_replay_snapshot()
                existing = self._connection.execute(
                    "SELECT record_type FROM shadow_learning_records WHERE record_id=?",
                    (record_id,),
                ).fetchone()
                if existing is not None:
                    raise DuplicateRecordError(
                        f"record ID {record_id} already exists as {existing['record_type']}"
                    )
                duplicate = self._connection.execute(
                    "SELECT record_id FROM shadow_learning_records WHERE content_hash=?",
                    (content_hash,),
                ).fetchone()
                if duplicate is not None:
                    raise DuplicateRecordError(
                        f"immutable content duplicates record {duplicate['record_id']}"
                    )
                tail = self._connection.execute(
                    "SELECT sequence, chain_hash FROM shadow_learning_records "
                    "ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                sequence = 1 if tail is None else int(tail["sequence"]) + 1
                previous_hash = (
                    GENESIS_HASH if tail is None else str(tail["chain_hash"])
                )
                chain_hash = _chain_hash(sequence, previous_hash, content_hash)
                try:
                    self._connection.execute(
                        """
                        INSERT INTO shadow_learning_records(
                            sequence, record_id, record_type, thesis_id, prediction_id,
                            challenger_version, independence_key, occurred_at, tags_json,
                            document_json, content_hash, previous_hash, chain_hash, appended_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            sequence,
                            record_id,
                            record_type.value,
                            thesis_id,
                            prediction_id,
                            challenger_version,
                            independence_key,
                            datetime_text(occurred_at),
                            canonical_json(tags),
                            document_json,
                            content_hash,
                            previous_hash,
                            chain_hash,
                            datetime_text(appended_at),
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise DuplicateRecordError(
                        f"duplicate {record_type.value.lower()} record {record_id}"
                    ) from exc
                self._verified_snapshot_guard = self._snapshot_cache_guard_locked()
        except BaseException:
            self._verified_snapshot_guard = previous_guard
            raise

        with self._lock:
            self.verified_replay_snapshot()
            bindings = self._verified_snapshot_bindings
            if bindings is None:
                raise LedgerTampered("verified shadow bindings are unavailable")
            record = bindings.records.get(record_id)
            if record is None:
                raise LedgerTampered("newly appended record disappeared")
            return record

    def _outcome_for_prediction_unchecked(
        self, prediction_id: str
    ) -> OutcomeRecord | None:
        row = self._connection.execute(
            "SELECT * FROM shadow_learning_records "
            "WHERE record_type='OUTCOME' AND prediction_id=?",
            (prediction_id,),
        ).fetchone()
        if row is None:
            return None
        record = _row_to_record(row)
        assert isinstance(record, OutcomeRecord)
        return record

    def _assert_integrity_locked(self) -> None:
        trigger_rows = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name IN (?, ?)",
            (
                "shadow_learning_records_no_update",
                "shadow_learning_records_no_delete",
            ),
        ).fetchall()
        triggers = {str(row["name"]) for row in trigger_rows}
        if triggers != {
            "shadow_learning_records_no_update",
            "shadow_learning_records_no_delete",
        }:
            raise LedgerTampered("append-only shadow ledger trigger is missing")
        rows = self._connection.execute(
            "SELECT * FROM shadow_learning_records ORDER BY sequence"
        ).fetchall()
        expected_sequence = 1
        expected_previous = GENESIS_HASH
        bindings = _ShadowBindingState()
        for row in rows:
            record, expected_previous = _verified_row(
                row,
                expected_sequence=expected_sequence,
                expected_previous=expected_previous,
                context="shadow ledger",
            )
            bindings.accept(record)
            expected_sequence += 1

    @staticmethod
    def _assert_bindings(records: Sequence[ShadowRecord]) -> None:
        bindings = _ShadowBindingState()
        for record in records:
            bindings.accept(record)

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError(
                f"shadow learning schema {version} is newer than supported"
            )
        with self._lock:
            self._connection.executescript(
                f"""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS shadow_learning_records (
                    sequence INTEGER PRIMARY KEY,
                    record_id TEXT NOT NULL UNIQUE,
                    record_type TEXT NOT NULL CHECK (
                        record_type IN ('THESIS','EVIDENCE','PREDICTION','OUTCOME')
                    ),
                    thesis_id TEXT NOT NULL,
                    prediction_id TEXT,
                    challenger_version TEXT NOT NULL,
                    independence_key TEXT,
                    occurred_at TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    document_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    previous_hash TEXT NOT NULL,
                    chain_hash TEXT NOT NULL UNIQUE,
                    appended_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS shadow_learning_type_sequence_idx
                    ON shadow_learning_records(record_type, sequence);
                CREATE INDEX IF NOT EXISTS shadow_learning_thesis_idx
                    ON shadow_learning_records(thesis_id, sequence);
                CREATE INDEX IF NOT EXISTS shadow_learning_challenger_idx
                    ON shadow_learning_records(challenger_version, record_type, sequence);
                CREATE UNIQUE INDEX IF NOT EXISTS shadow_learning_one_outcome_idx
                    ON shadow_learning_records(prediction_id)
                    WHERE record_type='OUTCOME';
                CREATE TRIGGER IF NOT EXISTS shadow_learning_records_no_update
                BEFORE UPDATE ON shadow_learning_records
                BEGIN
                    SELECT RAISE(ABORT, 'immutable shadow learning ledger: update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS shadow_learning_records_no_delete
                BEFORE DELETE ON shadow_learning_records
                BEGIN
                    SELECT RAISE(ABORT, 'immutable shadow learning ledger: delete forbidden');
                END;
                PRAGMA user_version={SCHEMA_VERSION};
                COMMIT;
                """
            )

    @contextmanager
    def _transaction(self):
        self._ensure_open()
        self._lock.acquire()
        began = False
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            began = True
            yield
            self._connection.execute("COMMIT")
            began = False
        except BaseException:
            if began and self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            raise
        finally:
            self._lock.release()

    def _now(self) -> datetime:
        return _time(self._clock(), "clock result")

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("shadow learning ledger is closed")


def _row_to_record(
    row: sqlite3.Row,
    *,
    document: Mapping[str, object] | None = None,
) -> ShadowRecord:
    if document is None:
        document = json.loads(str(row["document_json"]))
    if document.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported immutable document schema_version")
    record_type = RecordType(str(document["record_type"]))
    common = {
        "sequence": int(row["sequence"]),
        "content_hash": _digest("content_hash", str(row["content_hash"])),
        "previous_hash": _digest("previous_hash", str(row["previous_hash"])),
        "chain_hash": _digest("chain_hash", str(row["chain_hash"])),
        "appended_at": _parse_time(row["appended_at"]),
    }
    if record_type is RecordType.THESIS:
        return ThesisRecord(
            thesis_id=_identifier("thesis_id", document["thesis_id"]),
            champion_version=_text("champion_version", document["champion_version"]),
            challenger_version=_text(
                "challenger_version", document["challenger_version"]
            ),
            thesis=_frozen_mapping("thesis", document["thesis"]),
            created_at=_parse_time(document["created_at"]),
            tags=_tags(document["tags"]),  # type: ignore[arg-type]
            **common,
        )
    if record_type is RecordType.EVIDENCE:
        return EvidenceRecord(
            evidence_id=_identifier("evidence_id", document["evidence_id"]),
            thesis_id=_identifier("thesis_id", document["thesis_id"]),
            thesis_hash=_digest("thesis_hash", document["thesis_hash"]),
            challenger_version=_text(
                "challenger_version", document["challenger_version"]
            ),
            source=_text("source", document["source"]),
            evidence=_frozen_mapping("evidence", document["evidence"]),
            published_at=_parse_time(document["published_at"]),
            first_seen_at=_parse_time(document["first_seen_at"]),
            tags=_tags(document["tags"]),  # type: ignore[arg-type]
            **common,
        )
    if record_type is RecordType.PREDICTION:
        raw_bindings = document["evidence_bindings"]
        if not isinstance(raw_bindings, list):
            raise TypeError("evidence_bindings must be a list")
        bindings = tuple(
            EvidenceBinding(
                evidence_id=_identifier("evidence_id", item["evidence_id"]),
                evidence_hash=_digest("evidence_hash", item["evidence_hash"]),
            )
            for item in raw_bindings
            if isinstance(item, Mapping)
        )
        if len(bindings) != len(raw_bindings):
            raise TypeError("evidence binding must be a mapping")
        horizon_raw = document["horizon_at"]
        return PredictionRecord(
            prediction_id=_identifier("prediction_id", document["prediction_id"]),
            thesis_id=_identifier("thesis_id", document["thesis_id"]),
            thesis_hash=_digest("thesis_hash", document["thesis_hash"]),
            champion_version=_text("champion_version", document["champion_version"]),
            challenger_version=_text(
                "challenger_version", document["challenger_version"]
            ),
            prediction=_frozen_mapping("prediction", document["prediction"]),
            predicted_at=_parse_time(document["predicted_at"]),
            horizon_at=None if horizon_raw is None else _parse_time(horizon_raw),
            evidence_bindings=bindings,
            evidence_bundle_hash=_digest(
                "evidence_bundle_hash", document["evidence_bundle_hash"]
            ),
            independence_key=_text(
                "independence_key", document["independence_key"], maximum=512
            ),
            tags=_tags(document["tags"]),  # type: ignore[arg-type]
            **common,
        )
    return OutcomeRecord(
        outcome_id=_identifier("outcome_id", document["outcome_id"]),
        prediction_id=_identifier("prediction_id", document["prediction_id"]),
        prediction_hash=_digest("prediction_hash", document["prediction_hash"]),
        thesis_id=_identifier("thesis_id", document["thesis_id"]),
        challenger_version=_text(
            "challenger_version", document["challenger_version"]
        ),
        independence_key=_text(
            "independence_key", document["independence_key"], maximum=512
        ),
        outcome=_frozen_mapping("outcome", document["outcome"]),
        observed_at=_parse_time(document["observed_at"]),
        resolved_at=_parse_time(document["resolved_at"]),
        tags=_tags(document["tags"]),  # type: ignore[arg-type]
        **common,
    )


def _verified_row(
    row: sqlite3.Row,
    *,
    expected_sequence: int,
    expected_previous: str,
    context: str,
) -> tuple[ShadowRecord, str]:
    sequence = int(row["sequence"])
    if sequence != expected_sequence:
        raise LedgerTampered(f"{context} sequence contains a gap")
    try:
        document = json.loads(str(row["document_json"]))
        normalized = canonical_json(document)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LedgerTampered(f"invalid document at sequence {sequence}") from exc
    if normalized != str(row["document_json"]):
        raise LedgerTampered(
            f"immutable document mismatch at sequence {sequence}"
        )
    expected_content = canonical_hash(document)
    if expected_content != str(row["content_hash"]):
        raise LedgerTampered(f"content hash mismatch at sequence {sequence}")
    if str(row["previous_hash"]) != expected_previous:
        raise LedgerTampered(f"previous hash mismatch at sequence {sequence}")
    expected_chain = _chain_hash(sequence, expected_previous, expected_content)
    if str(row["chain_hash"]) != expected_chain:
        raise LedgerTampered(f"chain hash mismatch at sequence {sequence}")
    try:
        record = _row_to_record(row, document=document)
        _assert_row_projection(row, record)
    except (KeyError, TypeError, ValueError, AssertionError) as exc:
        raise LedgerTampered(
            f"invalid immutable record at sequence {sequence}: {exc}"
        ) from exc
    if record.appended_at < _record_time(record):
        raise LedgerTampered(
            f"record append time precedes content at sequence {sequence}"
        )
    return record, expected_chain


def _assert_row_projection(row: sqlite3.Row, record: ShadowRecord) -> None:
    if str(row["record_type"]) != record.record_type.value:
        raise ValueError("record_type projection mismatch")
    if isinstance(record, ThesisRecord):
        record_id = record.thesis_id
        thesis_id = record.thesis_id
        prediction_id = None
        challenger = record.challenger_version
        independence_key = None
    elif isinstance(record, EvidenceRecord):
        record_id = record.evidence_id
        thesis_id = record.thesis_id
        prediction_id = None
        challenger = record.challenger_version
        independence_key = None
    elif isinstance(record, PredictionRecord):
        record_id = record.prediction_id
        thesis_id = record.thesis_id
        prediction_id = record.prediction_id
        challenger = record.challenger_version
        independence_key = record.independence_key
    else:
        record_id = record.outcome_id
        thesis_id = record.thesis_id
        prediction_id = record.prediction_id
        challenger = record.challenger_version
        independence_key = record.independence_key
    expected = {
        "record_id": record_id,
        "thesis_id": thesis_id,
        "prediction_id": prediction_id,
        "challenger_version": challenger,
        "independence_key": independence_key,
        "occurred_at": datetime_text(_record_time(record)),
        "tags_json": canonical_json(record.tags),
    }
    for field, value in expected.items():
        actual = row[field]
        if actual != value:
            raise ValueError(f"{field} projection mismatch")


def _record_time(record: ShadowRecord) -> datetime:
    if isinstance(record, ThesisRecord):
        return record.created_at
    if isinstance(record, EvidenceRecord):
        return record.first_seen_at
    if isinstance(record, PredictionRecord):
        return record.predicted_at
    return record.resolved_at


def _chain_hash(sequence: int, previous_hash: str, content_hash: str) -> str:
    return hashlib.sha256(
        f"{sequence}:{previous_hash}:{content_hash}".encode("ascii")
    ).hexdigest()


def shadow_record_to_dict(record: ShadowRecord) -> dict[str, object]:
    """Return a JSON-safe audit projection of one immutable ledger record."""

    common: dict[str, object] = {
        "sequence": record.sequence,
        "record_type": record.record_type.value,
        "content_hash": record.content_hash,
        "previous_hash": record.previous_hash,
        "chain_hash": record.chain_hash,
        "appended_at": datetime_text(record.appended_at),
        "read_only": True,
        "decision_authority": "OBSERVATION_ONLY",
    }
    if isinstance(record, ThesisRecord):
        return {
            **common,
            "record_id": record.thesis_id,
            "thesis_id": record.thesis_id,
            "champion_version": record.champion_version,
            "challenger_version": record.challenger_version,
            "created_at": datetime_text(record.created_at),
            "tags": list(record.tags),
            "thesis": thaw_json(record.thesis),
        }
    if isinstance(record, EvidenceRecord):
        return {
            **common,
            "record_id": record.evidence_id,
            "evidence_id": record.evidence_id,
            "thesis_id": record.thesis_id,
            "thesis_hash": record.thesis_hash,
            "challenger_version": record.challenger_version,
            "source": record.source,
            "published_at": datetime_text(record.published_at),
            "first_seen_at": datetime_text(record.first_seen_at),
            "tags": list(record.tags),
            "evidence": thaw_json(record.evidence),
        }
    if isinstance(record, PredictionRecord):
        return {
            **common,
            "record_id": record.prediction_id,
            "prediction_id": record.prediction_id,
            "thesis_id": record.thesis_id,
            "thesis_hash": record.thesis_hash,
            "champion_version": record.champion_version,
            "challenger_version": record.challenger_version,
            "predicted_at": datetime_text(record.predicted_at),
            "horizon_at": (
                None if record.horizon_at is None else datetime_text(record.horizon_at)
            ),
            "evidence_bindings": [
                binding.as_dict() for binding in record.evidence_bindings
            ],
            "evidence_bundle_hash": record.evidence_bundle_hash,
            "independence_key": record.independence_key,
            "tags": list(record.tags),
            "prediction": thaw_json(record.prediction),
        }
    return {
        **common,
        "record_id": record.outcome_id,
        "outcome_id": record.outcome_id,
        "prediction_id": record.prediction_id,
        "prediction_hash": record.prediction_hash,
        "thesis_id": record.thesis_id,
        "challenger_version": record.challenger_version,
        "independence_key": record.independence_key,
        "observed_at": datetime_text(record.observed_at),
        "resolved_at": datetime_text(record.resolved_at),
        "tags": list(record.tags),
        "outcome": thaw_json(record.outcome),
    }


def replay_record_to_dict(replay: ReplayRecord) -> dict[str, object]:
    return {
        "thesis": shadow_record_to_dict(replay.thesis),
        "evidence": [shadow_record_to_dict(item) for item in replay.evidence],
        "prediction": shadow_record_to_dict(replay.prediction),
        "outcome": (
            None if replay.outcome is None else shadow_record_to_dict(replay.outcome)
        ),
        "point_in_time_evidence_hash": replay.point_in_time_evidence_hash,
        "replayed_as_of": (
            None
            if replay.replayed_as_of is None
            else datetime_text(replay.replayed_as_of)
        ),
        "read_only": True,
        "decision_authority": "OBSERVATION_ONLY",
    }


def similarity_match_to_dict(match: SimilarityMatch) -> dict[str, object]:
    return {
        "similarity": match.similarity,
        "distance": match.distance,
        "shared_tags": list(match.shared_tags),
        "replay": replay_record_to_dict(match.replay),
        "read_only": True,
        "decision_authority": "OBSERVATION_ONLY",
    }


def _coerce_record_type(value: RecordType | str) -> RecordType:
    if isinstance(value, RecordType):
        return value
    if not isinstance(value, str):
        raise TypeError("record_type must be RecordType or string")
    try:
        return RecordType(value.strip().upper())
    except ValueError as exc:
        raise ValueError(f"unsupported shadow record type: {value}") from exc


def _identifier(field: str, value: object) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a valid identifier")
    return value


def _text(field: str, value: object, *, maximum: int = 256) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{field} must contain 1 to {maximum} characters")
    if any(ord(character) < 32 for character in normalized):
        raise ValueError(f"{field} cannot contain control characters")
    return normalized


def _digest(field: str, value: object) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _time(value: datetime, field: str) -> datetime:
    try:
        return utc_datetime(value, field=field)
    except (TypeError, ValueError) as exc:
        if isinstance(exc, TypeError):
            raise
        raise ValueError(str(exc)) from exc


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise TypeError("stored timestamp must be a string")
    return _time(datetime.fromisoformat(value), "stored timestamp")


def _tags(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("tags must be a sequence of strings")
    normalized = tuple(
        sorted({_text("tag", value, maximum=80).casefold() for value in values})
    )
    if len(normalized) > 128:
        raise ValueError("at most 128 unique tags are allowed")
    return normalized


def _document_mapping(field: str, value: Mapping[str, object]) -> dict[str, object]:
    frozen = _frozen_mapping(field, value)
    thawed = thaw_json(frozen)
    assert isinstance(thawed, dict)
    return thawed


def _frozen_mapping(field: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{field} must be a nonempty mapping")
    frozen = freeze_json(value)
    if not isinstance(frozen, Mapping):
        raise TypeError(f"{field} must be a mapping")
    return frozen


def _limit(value: int, *, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("limit must be an integer")
    if not 1 <= value <= maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    return value


__all__ = [
    "BindingError",
    "DISCOVERY_SAMPLE_THRESHOLD",
    "DuplicateRecordError",
    "EXTERNAL_HUMAN_AUTHORITY",
    "EvidenceBinding",
    "EvidenceRecord",
    "GovernanceStage",
    "GovernanceState",
    "LedgerTampered",
    "OutcomeRecord",
    "PredictionRecord",
    "RecordType",
    "ReplayRecord",
    "SCHEMA_VERSION",
    "ShadowLearningError",
    "ShadowLearningLedger",
    "SimilarityMatch",
    "ThesisRecord",
    "TimeTravelError",
    "UnknownRecordError",
    "VerifiedReplaySnapshot",
    "VerifiedRecordCursor",
    "replay_record_to_dict",
    "shadow_record_to_dict",
    "similarity_match_to_dict",
]
