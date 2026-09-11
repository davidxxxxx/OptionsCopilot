"""Pure, append-only state tracking for post-calendar-event research.

This module deliberately has no storage, network, broker, runtime, or order
dependencies.  Every operation returns a new immutable ledger, and every
piece of evidence plus every transition is bound by a canonical SHA-256 hash.
The complete workflow remains observation-only and ``SUPPORTING_ONLY``.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
import re

from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


GENESIS_HASH = "0" * 64
SUPPORTING_ONLY = "SUPPORTING_ONLY"
OBSERVATION_ONLY = "OBSERVATION_ONLY"
NO_TRADE = "NO_TRADE"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_TERMINAL_STAGES: frozenset["ReactionStage"]


class ReactionStage(str, Enum):
    SCHEDULED = "SCHEDULED"
    AWAITING_RELEASE = "AWAITING_RELEASE"
    RELEASE_CAPTURED = "RELEASE_CAPTURED"
    SURPRISE_ASSESSED = "SURPRISE_ASSESSED"
    MARKET_REACTION_OBSERVED = "MARKET_REACTION_OBSERVED"
    OPTION_REEVALUATED = "OPTION_REEVALUATED"
    DEGRADED = "DEGRADED"
    CONFLICTED = "CONFLICTED"
    NO_TRADE = "NO_TRADE"


_TERMINAL_STAGES = frozenset(
    {ReactionStage.DEGRADED, ReactionStage.CONFLICTED, ReactionStage.NO_TRADE}
)


class ReactionReason(str, Enum):
    MISSING_OFFICIAL_ACTUAL = "MISSING_OFFICIAL_ACTUAL"
    OFFICIAL_ACTUAL_UNVERIFIED = "OFFICIAL_ACTUAL_UNVERIFIED"
    OFFICIAL_SOURCE_MISMATCH = "OFFICIAL_SOURCE_MISMATCH"
    OFFICIAL_RELEASE_CONFLICT = "OFFICIAL_RELEASE_CONFLICT"
    RELEASE_EXPECTATION_MISMATCH = "RELEASE_EXPECTATION_MISMATCH"
    MISSING_MARKET_EVIDENCE = "MISSING_MARKET_EVIDENCE"
    STALE_MARKET_EVIDENCE = "STALE_MARKET_EVIDENCE"
    MISSING_OPTION_EVIDENCE = "MISSING_OPTION_EVIDENCE"
    STALE_OPTION_EVIDENCE = "STALE_OPTION_EVIDENCE"


class ReactionError(RuntimeError):
    """Base class for reaction-ledger failures."""


class ReactionIntegrityError(ReactionError):
    """Canonical evidence or the append chain has been altered."""


class ReactionTransitionError(ReactionError):
    """A transition is out of order or attempts time travel."""


class ReactionBindingError(ReactionError):
    """Evidence belongs to a different event or predecessor."""


class DuplicateReactionError(ReactionIntegrityError):
    """An already-recorded transition or evidence item was supplied again."""


class ReactionAuthorityError(ReactionError):
    """Supporting evidence attempted to claim trading authority."""


def _nonblank(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not (normalized := value.strip()):
        raise ValueError(f"{field_name} cannot be blank")
    return normalized


def _time(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    return utc_datetime(value, field=field_name)


def _digest(value: object, field_name: str) -> str:
    normalized = _nonblank(value, field_name).lower()
    if _HASH_RE.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return normalized


def _finite_decimal(value: object, field_name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise TypeError(f"{field_name} must be a finite Decimal")
    return value


def _optional_nonblank(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _nonblank(value, field_name)


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if not normalized else format(normalized, "f")


def _hashes(values: Iterable[str], field_name: str) -> tuple[str, ...]:
    result = tuple(_digest(value, field_name) for value in values)
    if len(result) != len(set(result)):
        raise DuplicateReactionError(f"duplicate {field_name}")
    return result


def _authority_document() -> dict[str, object]:
    return {
        "decision_authority": SUPPORTING_ONLY,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


_FORBIDDEN_AUTHORITY_KEYS = frozenset(
    {
        "approval",
        "approved",
        "approval_eligible",
        "eligibility",
        "eligible",
        "instruction",
        "instructions",
        "instruction_creation_allowed",
        "order",
        "orders",
        "order_creation_allowed",
        "order_submission_allowed",
        "submit_order",
    }
)


def _reject_authority_claims(value: object, *, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} keys must be strings")
            normalized = key.strip().lower()
            if normalized == "decision_authority" and item != SUPPORTING_ONLY:
                raise ReactionAuthorityError(
                    f"{path}.{key} must remain {SUPPORTING_ONLY}"
                )
            if normalized in _FORBIDDEN_AUTHORITY_KEYS:
                raise ReactionAuthorityError(
                    f"supporting evidence cannot claim authority at {path}.{key}"
                )
            _reject_authority_claims(item, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for index, item in enumerate(value):
            _reject_authority_claims(item, path=f"{path}[{index}]")


def _frozen_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    _reject_authority_claims(value, path=field_name)
    frozen = freeze_json(value)
    assert isinstance(frozen, Mapping)
    return frozen


def _set_content_hash(instance: object, document: Mapping[str, object]) -> None:
    expected = canonical_hash(document)
    claimed = getattr(instance, "content_hash")
    if claimed:
        if _digest(claimed, "content_hash") != expected:
            raise ReactionIntegrityError("content hash does not match canonical evidence")
    object.__setattr__(instance, "content_hash", expected)


class _SupportingEvidence:
    content_hash: str

    @property
    def decision_authority(self) -> str:
        return SUPPORTING_ONLY

    @property
    def approval_eligible(self) -> bool:
        return False

    @property
    def instruction_creation_allowed(self) -> bool:
        return False

    @property
    def order_creation_allowed(self) -> bool:
        return False

    def hash_document(self) -> dict[str, object]:
        raise NotImplementedError

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_document(), "content_hash": self.content_hash}

    def assert_integrity(self) -> None:
        if canonical_hash(self.hash_document()) != self.content_hash:
            raise ReactionIntegrityError(
                f"{type(self).__name__} canonical content was altered"
            )


@dataclass(frozen=True, slots=True)
class ScheduledEventIdentity(_SupportingEvidence):
    event_id: str
    official_source: str
    official_source_id: str
    title: str
    category: str
    scheduled_at: datetime
    # Official calendars commonly expose the release schedule without exposing
    # when that schedule document itself was published.  Preserve that absence
    # in the identity instead of fabricating a publication timestamp.
    schedule_published_at: datetime | None
    schedule_first_seen_at: datetime
    schedule_observed_at: datetime
    symbols: tuple[str, ...] = ()
    reaction_root_hash: str | None = None
    content_hash: str = ""

    def __post_init__(self) -> None:
        for name in (
            "event_id",
            "official_source",
            "official_source_id",
            "title",
            "category",
        ):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        for name in (
            "scheduled_at",
            "schedule_first_seen_at",
            "schedule_observed_at",
        ):
            object.__setattr__(self, name, _time(getattr(self, name), name))
        if self.schedule_published_at is not None:
            object.__setattr__(
                self,
                "schedule_published_at",
                _time(self.schedule_published_at, "schedule_published_at"),
            )
        if not (
            self.schedule_first_seen_at
            <= self.schedule_observed_at
            <= self.scheduled_at
        ) or (
            self.schedule_published_at is not None
            and self.schedule_published_at > self.schedule_first_seen_at
        ):
            raise ReactionTransitionError(
                "schedule provenance must be known in order before the event"
            )
        symbols = tuple(
            dict.fromkeys(_nonblank(item, "symbol").upper() for item in self.symbols)
        )
        object.__setattr__(self, "symbols", symbols)
        if self.reaction_root_hash is not None:
            object.__setattr__(
                self,
                "reaction_root_hash",
                _digest(self.reaction_root_hash, "reaction_root_hash"),
            )
        _set_content_hash(self, self.hash_document())

    @property
    def event_hash(self) -> str:
        return self.reaction_root_hash or self.content_hash

    def hash_document(self) -> dict[str, object]:
        document = {
            "event_id": self.event_id,
            "official_source": self.official_source,
            "official_source_id": self.official_source_id,
            "title": self.title,
            "category": self.category,
            "scheduled_at": datetime_text(self.scheduled_at),
            "schedule_published_at": (
                None
                if self.schedule_published_at is None
                else datetime_text(self.schedule_published_at)
            ),
            "schedule_first_seen_at": datetime_text(self.schedule_first_seen_at),
            "schedule_observed_at": datetime_text(self.schedule_observed_at),
            "symbols": list(self.symbols),
            **_authority_document(),
        }
        if self.reaction_root_hash is not None:
            document["reaction_root_hash"] = self.reaction_root_hash
        return document


@dataclass(frozen=True, slots=True)
class ConsensusExpectation(_SupportingEvidence):
    event_hash: str
    metric: str
    expected_value: Decimal
    unit: str
    provider: str
    source_id: str
    published_at: datetime
    first_seen_at: datetime
    observed_at: datetime
    vintage: str
    # None preserves legacy ledgers, but assess_consensus() never treats it as comparable.
    period: str | None = None
    basis: str | None = None
    content_hash: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_hash", _digest(self.event_hash, "event_hash"))
        for name in ("metric", "unit", "provider", "source_id", "vintage"):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        for name in ("period", "basis"):
            object.__setattr__(
                self,
                name,
                _optional_nonblank(getattr(self, name), name),
            )
        _finite_decimal(self.expected_value, "expected_value")
        for name in ("published_at", "first_seen_at", "observed_at"):
            object.__setattr__(self, name, _time(getattr(self, name), name))
        if not self.published_at <= self.first_seen_at <= self.observed_at:
            raise ReactionTransitionError("expectation provenance contains time travel")
        _set_content_hash(self, self.hash_document())

    def hash_document(self) -> dict[str, object]:
        return {
            "event_hash": self.event_hash,
            "metric": self.metric,
            "expected_value": self.expected_value,
            "unit": self.unit,
            "period": self.period,
            "basis": self.basis,
            "provider": self.provider,
            "source_id": self.source_id,
            "published_at": datetime_text(self.published_at),
            "first_seen_at": datetime_text(self.first_seen_at),
            "observed_at": datetime_text(self.observed_at),
            "vintage": self.vintage,
            **_authority_document(),
        }


@dataclass(frozen=True, slots=True)
class OfficialRelease(_SupportingEvidence):
    event_hash: str
    metric: str
    actual_value: Decimal | None
    unit: str
    official_source: str
    source_id: str
    released_at: datetime
    vintage_at: datetime
    captured_at: datetime
    # None preserves legacy ledgers, but assess_consensus() never treats it as comparable.
    period: str | None = None
    basis: str | None = None
    raw_calculated_value: Decimal | None = None
    published_precision: str | None = None
    revision: int = 0
    supersedes_hash: str | None = None
    content_hash: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_hash", _digest(self.event_hash, "event_hash"))
        for name in ("metric", "unit", "official_source", "source_id"):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        for name in ("period", "basis"):
            object.__setattr__(
                self,
                name,
                _optional_nonblank(getattr(self, name), name),
            )
        if self.actual_value is not None:
            _finite_decimal(self.actual_value, "actual_value")
        if self.raw_calculated_value is not None:
            _finite_decimal(self.raw_calculated_value, "raw_calculated_value")
        if self.published_precision is not None:
            precision = _nonblank(
                self.published_precision,
                "published_precision",
            )
            try:
                increment = Decimal(precision)
            except Exception:
                raise ValueError("published_precision must be a decimal increment") from None
            if not increment.is_finite() or increment <= 0:
                raise ValueError("published_precision must be positive")
            object.__setattr__(self, "published_precision", precision)
        for name in ("released_at", "vintage_at", "captured_at"):
            object.__setattr__(self, name, _time(getattr(self, name), name))
        if not self.released_at <= self.vintage_at <= self.captured_at:
            raise ReactionTransitionError("release provenance contains time travel")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int):
            raise TypeError("revision must be an integer")
        if self.revision < 0:
            raise ValueError("revision must be non-negative")
        if self.revision == 0 and self.supersedes_hash is not None:
            raise ValueError("initial release cannot supersede another vintage")
        if self.revision > 0 and self.supersedes_hash is None:
            raise ValueError("a revision must bind its superseded release hash")
        if self.supersedes_hash is not None:
            object.__setattr__(
                self,
                "supersedes_hash",
                _digest(self.supersedes_hash, "supersedes_hash"),
            )
        _set_content_hash(self, self.hash_document())

    def hash_document(self) -> dict[str, object]:
        document = {
            "event_hash": self.event_hash,
            "metric": self.metric,
            "actual_value": self.actual_value,
            "unit": self.unit,
            "period": self.period,
            "basis": self.basis,
            "official_source": self.official_source,
            "source_id": self.source_id,
            "released_at": datetime_text(self.released_at),
            "vintage_at": datetime_text(self.vintage_at),
            "captured_at": datetime_text(self.captured_at),
            "revision": self.revision,
            "supersedes_hash": self.supersedes_hash,
            **_authority_document(),
        }
        # Optional calculation metadata was added after the original ledger
        # contract.  Omitting absent values preserves verification of legacy
        # release hashes while new BLS releases retain both raw and published
        # precision values.
        if self.raw_calculated_value is not None:
            document["raw_calculated_value"] = self.raw_calculated_value
        if self.published_precision is not None:
            document["published_precision"] = self.published_precision
        return document


@dataclass(frozen=True, slots=True)
class ConsensusAssessment(_SupportingEvidence):
    """Canonical supporting-only comparison of one release and one estimate."""

    event_hash: str
    label: str
    reason: str | None
    metric: str
    unit: str
    period: str | None
    basis: str | None
    actual_value: Decimal | None
    consensus_value: Decimal | None
    actual_evidence_hash: str
    consensus_evidence_hash: str | None
    release_at: datetime
    consensus_observed_at: datetime | None
    assessed_at: datetime
    comparable: bool
    content_hash: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_hash", _digest(self.event_hash, "event_hash"))
        object.__setattr__(self, "label", _nonblank(self.label, "label").upper())
        if self.reason is not None:
            object.__setattr__(
                self,
                "reason",
                _nonblank(self.reason, "reason").upper(),
            )
        for name in ("metric", "unit"):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        for name in ("period", "basis"):
            object.__setattr__(
                self,
                name,
                _optional_nonblank(getattr(self, name), name),
            )
        if self.actual_value is not None:
            _finite_decimal(self.actual_value, "actual_value")
        if self.consensus_value is not None:
            _finite_decimal(self.consensus_value, "consensus_value")
        object.__setattr__(
            self,
            "actual_evidence_hash",
            _digest(self.actual_evidence_hash, "actual_evidence_hash"),
        )
        if self.consensus_evidence_hash is not None:
            object.__setattr__(
                self,
                "consensus_evidence_hash",
                _digest(
                    self.consensus_evidence_hash,
                    "consensus_evidence_hash",
                ),
            )
        object.__setattr__(self, "release_at", _time(self.release_at, "release_at"))
        if self.consensus_observed_at is not None:
            object.__setattr__(
                self,
                "consensus_observed_at",
                _time(self.consensus_observed_at, "consensus_observed_at"),
            )
        object.__setattr__(self, "assessed_at", _time(self.assessed_at, "assessed_at"))
        if self.assessed_at < self.release_at:
            raise ReactionTransitionError("consensus assessment predates the release")
        if not isinstance(self.comparable, bool):
            raise TypeError("comparable must be a boolean")

        comparable_labels = frozenset({"BEAT", "MISS", "IN_LINE"})
        uncertain_reasons = frozenset(
            {"CONSENSUS_NOT_POINT_IN_TIME", "CONSENSUS_NOT_COMPARABLE"}
        )
        if self.comparable:
            if self.label not in comparable_labels or self.reason is not None:
                raise ReactionIntegrityError(
                    "comparable consensus requires an exact label and no reason"
                )
            if (
                self.actual_value is None
                or self.consensus_value is None
                or self.consensus_evidence_hash is None
                or self.consensus_observed_at is None
                or self.period is None
                or self.basis is None
            ):
                raise ReactionIntegrityError(
                    "comparable consensus requires complete bound evidence"
                )
            if self.consensus_observed_at >= self.release_at:
                raise ReactionIntegrityError(
                    "comparable consensus must be observed before release"
                )
            expected_label = (
                "BEAT"
                if self.actual_value > self.consensus_value
                else "MISS"
                if self.actual_value < self.consensus_value
                else "IN_LINE"
            )
            if self.label != expected_label:
                raise ReactionIntegrityError("consensus label is not deterministic")
        elif self.label != "UNCERTAIN" or self.reason not in uncertain_reasons:
            raise ReactionIntegrityError(
                "non-comparable consensus must be explicit UNCERTAIN"
            )
        if self.consensus_evidence_hash is None:
            if self.consensus_value is not None or self.consensus_observed_at is not None:
                raise ReactionIntegrityError(
                    "missing consensus hash cannot carry consensus values"
                )
        elif self.consensus_value is None or self.consensus_observed_at is None:
            raise ReactionIntegrityError(
                "bound consensus evidence requires value and observed_at"
            )
        _set_content_hash(self, self.hash_document())

    def hash_document(self) -> dict[str, object]:
        return {
            "event_hash": self.event_hash,
            "label": self.label,
            "reason": self.reason,
            "metric": self.metric,
            "unit": self.unit,
            "period": self.period,
            "basis": self.basis,
            "actual_value": (
                None
                if self.actual_value is None
                else _decimal_text(self.actual_value)
            ),
            "consensus_value": (
                None
                if self.consensus_value is None
                else _decimal_text(self.consensus_value)
            ),
            "actual_evidence_hash": self.actual_evidence_hash,
            "consensus_evidence_hash": self.consensus_evidence_hash,
            "release_at": datetime_text(self.release_at),
            "consensus_observed_at": (
                None
                if self.consensus_observed_at is None
                else datetime_text(self.consensus_observed_at)
            ),
            "assessed_at": datetime_text(self.assessed_at),
            "comparable": self.comparable,
            **_authority_document(),
        }


def assess_consensus(
    *,
    release: OfficialRelease,
    expectation: ConsensusExpectation | None,
    assessed_at: datetime,
) -> ConsensusAssessment:
    """Compare only complete, exact consensus captured before release."""

    if not isinstance(release, OfficialRelease):
        raise TypeError("release must be an OfficialRelease")
    if expectation is not None and not isinstance(expectation, ConsensusExpectation):
        raise TypeError("expectation must be a ConsensusExpectation or None")
    release.assert_integrity()
    at = _time(assessed_at, "assessed_at")
    if at < release.captured_at:
        raise ReactionTransitionError("consensus assessment predates captured release")

    label = "UNCERTAIN"
    reason = "CONSENSUS_NOT_POINT_IN_TIME"
    comparable = False
    consensus_value: Decimal | None = None
    consensus_hash: str | None = None
    consensus_observed_at: datetime | None = None

    if expectation is not None:
        expectation.assert_integrity()
        if expectation.event_hash != release.event_hash:
            raise ReactionBindingError(
                "consensus expectation is bound to a different event"
            )
        consensus_value = expectation.expected_value
        consensus_hash = expectation.content_hash
        consensus_observed_at = expectation.observed_at
        if expectation.observed_at < release.released_at:
            dimensions_match = (
                release.actual_value is not None
                and expectation.period is not None
                and release.period is not None
                and expectation.basis is not None
                and release.basis is not None
                and expectation.metric == release.metric
                and expectation.unit == release.unit
                and expectation.period == release.period
                and expectation.basis == release.basis
            )
            if dimensions_match:
                assert release.actual_value is not None
                comparable = True
                reason = None
                label = (
                    "BEAT"
                    if release.actual_value > expectation.expected_value
                    else "MISS"
                    if release.actual_value < expectation.expected_value
                    else "IN_LINE"
                )
            else:
                reason = "CONSENSUS_NOT_COMPARABLE"

    return ConsensusAssessment(
        event_hash=release.event_hash,
        label=label,
        reason=reason,
        metric=release.metric,
        unit=release.unit,
        period=release.period,
        basis=release.basis,
        actual_value=release.actual_value,
        consensus_value=consensus_value,
        actual_evidence_hash=release.content_hash,
        consensus_evidence_hash=consensus_hash,
        release_at=release.released_at,
        consensus_observed_at=consensus_observed_at,
        assessed_at=at,
        comparable=comparable,
    )


@dataclass(frozen=True, slots=True)
class SurpriseAssessment(_SupportingEvidence):
    event_hash: str
    expectation_hash: str
    release_hash: str
    metric: str
    unit: str
    expected_value: Decimal
    actual_value: Decimal
    delta: Decimal
    relative_delta: Decimal | None
    assessed_at: datetime
    supporting_evidence_hashes: tuple[str, ...] = ()
    calculation: str = field(default="ACTUAL_MINUS_EXPECTED", init=False)
    content_hash: str = ""

    def __post_init__(self) -> None:
        for name in ("event_hash", "expectation_hash", "release_hash"):
            object.__setattr__(self, name, _digest(getattr(self, name), name))
        for name in ("metric", "unit"):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        for name in ("expected_value", "actual_value", "delta"):
            _finite_decimal(getattr(self, name), name)
        if self.relative_delta is not None:
            _finite_decimal(self.relative_delta, "relative_delta")
        expected_delta = self.actual_value - self.expected_value
        expected_relative = (
            None if self.expected_value == 0 else expected_delta / abs(self.expected_value)
        )
        if self.delta != expected_delta or self.relative_delta != expected_relative:
            raise ReactionIntegrityError("surprise values are not deterministic")
        object.__setattr__(self, "assessed_at", _time(self.assessed_at, "assessed_at"))
        object.__setattr__(
            self,
            "supporting_evidence_hashes",
            _hashes(self.supporting_evidence_hashes, "supporting_evidence_hash"),
        )
        _set_content_hash(self, self.hash_document())

    def hash_document(self) -> dict[str, object]:
        return {
            "event_hash": self.event_hash,
            "expectation_hash": self.expectation_hash,
            "release_hash": self.release_hash,
            "metric": self.metric,
            "unit": self.unit,
            "expected_value": self.expected_value,
            "actual_value": self.actual_value,
            "delta": self.delta,
            "relative_delta": self.relative_delta,
            "assessed_at": datetime_text(self.assessed_at),
            "supporting_evidence_hashes": list(self.supporting_evidence_hashes),
            "calculation": self.calculation,
            **_authority_document(),
        }


@dataclass(frozen=True, slots=True)
class MarketReactionEvidence(_SupportingEvidence):
    event_hash: str
    release_hash: str
    source: str
    window_start: datetime
    window_end: datetime
    evidence_asof: datetime
    observed_at: datetime
    metrics: Mapping[str, object]
    content_hash: str = ""

    def __post_init__(self) -> None:
        for name in ("event_hash", "release_hash"):
            object.__setattr__(self, name, _digest(getattr(self, name), name))
        object.__setattr__(self, "source", _nonblank(self.source, "source"))
        for name in ("window_start", "window_end", "evidence_asof", "observed_at"):
            object.__setattr__(self, name, _time(getattr(self, name), name))
        if not (
            self.window_start
            < self.window_end
            <= self.evidence_asof
            <= self.observed_at
        ):
            raise ReactionTransitionError(
                "market evidence must close its declared window before observation"
            )
        object.__setattr__(self, "metrics", _frozen_mapping(self.metrics, "metrics"))
        _set_content_hash(self, self.hash_document())

    def hash_document(self) -> dict[str, object]:
        return {
            "event_hash": self.event_hash,
            "release_hash": self.release_hash,
            "source": self.source,
            "window_start": datetime_text(self.window_start),
            "window_end": datetime_text(self.window_end),
            "evidence_asof": datetime_text(self.evidence_asof),
            "observed_at": datetime_text(self.observed_at),
            "metrics": thaw_json(self.metrics),
            **_authority_document(),
        }


@dataclass(frozen=True, slots=True)
class OptionReevaluationEvidence(_SupportingEvidence):
    event_hash: str
    release_hash: str
    market_reaction_hash: str
    option_id: str
    candidate_hash: str
    source: str
    evidence_asof: datetime
    observed_at: datetime
    input_evidence_hashes: tuple[str, ...]
    result: Mapping[str, object]
    content_hash: str = ""

    def __post_init__(self) -> None:
        for name in (
            "event_hash",
            "release_hash",
            "market_reaction_hash",
            "candidate_hash",
        ):
            object.__setattr__(self, name, _digest(getattr(self, name), name))
        for name in ("option_id", "source"):
            object.__setattr__(self, name, _nonblank(getattr(self, name), name))
        for name in ("evidence_asof", "observed_at"):
            object.__setattr__(self, name, _time(getattr(self, name), name))
        if self.evidence_asof > self.observed_at:
            raise ReactionTransitionError("option evidence contains time travel")
        hashes = _hashes(self.input_evidence_hashes, "input_evidence_hash")
        if not hashes:
            raise ValueError("option re-evaluation requires input evidence hashes")
        object.__setattr__(self, "input_evidence_hashes", hashes)
        object.__setattr__(self, "result", _frozen_mapping(self.result, "result"))
        _set_content_hash(self, self.hash_document())

    def hash_document(self) -> dict[str, object]:
        return {
            "event_hash": self.event_hash,
            "release_hash": self.release_hash,
            "market_reaction_hash": self.market_reaction_hash,
            "option_id": self.option_id,
            "candidate_hash": self.candidate_hash,
            "source": self.source,
            "evidence_asof": datetime_text(self.evidence_asof),
            "observed_at": datetime_text(self.observed_at),
            "input_evidence_hashes": list(self.input_evidence_hashes),
            "result": thaw_json(self.result),
            **_authority_document(),
        }


@dataclass(frozen=True, slots=True)
class ReactionRecord:
    sequence: int
    stage: ReactionStage
    recorded_at: datetime
    event_hash: str
    evidence_hashes: tuple[str, ...]
    prior_hash: str
    reasons: tuple[ReactionReason, ...] = ()
    record_hash: str = ""

    def __post_init__(self) -> None:
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int):
            raise TypeError("sequence must be an integer")
        if self.sequence <= 0:
            raise ValueError("sequence must be positive")
        try:
            stage = ReactionStage(self.stage)
        except ValueError as exc:
            raise ValueError(f"invalid reaction stage: {self.stage!r}") from exc
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "recorded_at", _time(self.recorded_at, "recorded_at"))
        object.__setattr__(self, "event_hash", _digest(self.event_hash, "event_hash"))
        object.__setattr__(
            self,
            "evidence_hashes",
            _hashes(self.evidence_hashes, "evidence_hash"),
        )
        object.__setattr__(self, "prior_hash", _digest(self.prior_hash, "prior_hash"))
        reasons = tuple(ReactionReason(reason) for reason in self.reasons)
        if stage in _TERMINAL_STAGES and not reasons:
            raise ValueError("a terminal reaction record requires a reason")
        if stage not in _TERMINAL_STAGES and reasons:
            raise ValueError("only terminal reaction records may carry reasons")
        object.__setattr__(self, "reasons", reasons)
        expected = canonical_hash(self.hash_document())
        if self.record_hash and _digest(self.record_hash, "record_hash") != expected:
            raise ReactionIntegrityError("reaction record hash mismatch")
        object.__setattr__(self, "record_hash", expected)

    @property
    def terminal(self) -> bool:
        return self.stage in _TERMINAL_STAGES

    @property
    def decision(self) -> str:
        return NO_TRADE if self.terminal else OBSERVATION_ONLY

    @property
    def decision_authority(self) -> str:
        return SUPPORTING_ONLY

    def hash_document(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "stage": self.stage.value,
            "recorded_at": datetime_text(self.recorded_at),
            "event_hash": self.event_hash,
            "evidence_hashes": list(self.evidence_hashes),
            "prior_hash": self.prior_hash,
            "reasons": [reason.value for reason in self.reasons],
            "decision": self.decision,
            **_authority_document(),
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_document(), "record_hash": self.record_hash}

    def assert_integrity(self) -> None:
        if canonical_hash(self.hash_document()) != self.record_hash:
            raise ReactionIntegrityError(
                f"reaction record {self.sequence} canonical content was altered"
            )


_ALLOWED_NEXT: dict[ReactionStage, frozenset[ReactionStage]] = {
    ReactionStage.SCHEDULED: frozenset({ReactionStage.AWAITING_RELEASE}),
    ReactionStage.AWAITING_RELEASE: frozenset({ReactionStage.RELEASE_CAPTURED}),
    ReactionStage.RELEASE_CAPTURED: frozenset({ReactionStage.SURPRISE_ASSESSED}),
    ReactionStage.SURPRISE_ASSESSED: frozenset(
        {ReactionStage.MARKET_REACTION_OBSERVED}
    ),
    ReactionStage.MARKET_REACTION_OBSERVED: frozenset(
        {ReactionStage.OPTION_REEVALUATED}
    ),
    ReactionStage.OPTION_REEVALUATED: frozenset(),
}


@dataclass(frozen=True, slots=True)
class EventReactionLedger:
    """Immutable event-reaction state machine and canonical audit chain."""

    identity: ScheduledEventIdentity
    expectation: ConsensusExpectation
    transitions: tuple[ReactionRecord, ...]
    release_chain: tuple[OfficialRelease, ...] = ()
    surprise: SurpriseAssessment | None = None
    market_reaction: MarketReactionEvidence | None = None
    option_reevaluation: OptionReevaluationEvidence | None = None
    max_market_evidence_age: timedelta = timedelta(minutes=5)
    max_option_evidence_age: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ScheduledEventIdentity):
            raise TypeError("identity must be a ScheduledEventIdentity")
        if not isinstance(self.expectation, ConsensusExpectation):
            raise TypeError("expectation must be a ConsensusExpectation")
        object.__setattr__(self, "transitions", tuple(self.transitions))
        object.__setattr__(self, "release_chain", tuple(self.release_chain))
        for name in ("max_market_evidence_age", "max_option_evidence_age"):
            value = getattr(self, name)
            if not isinstance(value, timedelta) or value <= timedelta(0):
                raise ValueError(f"{name} must be a positive timedelta")
        self.assert_integrity()

    @classmethod
    def schedule(
        cls,
        identity: ScheduledEventIdentity,
        expectation: ConsensusExpectation,
        *,
        recorded_at: datetime,
        max_market_evidence_age: timedelta = timedelta(minutes=5),
        max_option_evidence_age: timedelta = timedelta(minutes=5),
    ) -> "EventReactionLedger":
        at = _time(recorded_at, "recorded_at")
        if expectation.event_hash != identity.event_hash:
            raise ReactionBindingError("expectation is bound to a different event")
        if expectation.observed_at > identity.scheduled_at:
            raise ReactionTransitionError("expectation arrived after the scheduled release")
        if at < max(identity.schedule_observed_at, expectation.observed_at):
            raise ReactionTransitionError("scheduled record predates its evidence")
        if at > identity.scheduled_at:
            raise ReactionTransitionError("SCHEDULED cannot be recorded after the event")
        record = ReactionRecord(
            sequence=1,
            stage=ReactionStage.SCHEDULED,
            recorded_at=at,
            event_hash=identity.event_hash,
            evidence_hashes=(identity.event_hash, expectation.content_hash),
            prior_hash=GENESIS_HASH,
        )
        return cls(
            identity=identity,
            expectation=expectation,
            transitions=(record,),
            max_market_evidence_age=max_market_evidence_age,
            max_option_evidence_age=max_option_evidence_age,
        )

    @property
    def current_stage(self) -> ReactionStage:
        return self.transitions[-1].stage

    @property
    def terminal(self) -> bool:
        return self.transitions[-1].terminal

    @property
    def decision(self) -> str:
        return self.transitions[-1].decision

    @property
    def decision_authority(self) -> str:
        return SUPPORTING_ONLY

    @property
    def approval_eligible(self) -> bool:
        return False

    @property
    def instruction_creation_allowed(self) -> bool:
        return False

    @property
    def order_creation_allowed(self) -> bool:
        return False

    @property
    def head_hash(self) -> str:
        return self.transitions[-1].record_hash

    def await_release(self, *, recorded_at: datetime) -> "EventReactionLedger":
        self._require_stage(ReactionStage.SCHEDULED, ReactionStage.AWAITING_RELEASE)
        at = self._next_time(recorded_at)
        if at < self.identity.scheduled_at:
            raise ReactionTransitionError("cannot await an unreached release time")
        return replace(
            self,
            transitions=self._append(
                ReactionStage.AWAITING_RELEASE,
                at,
                evidence_hashes=(),
            ),
        )

    def capture_release(
        self,
        releases: OfficialRelease | Iterable[OfficialRelease] | None,
        *,
        recorded_at: datetime,
    ) -> "EventReactionLedger":
        self._require_stage(
            ReactionStage.AWAITING_RELEASE, ReactionStage.RELEASE_CAPTURED
        )
        at = self._next_time(recorded_at)
        if releases is None:
            return self._terminate(
                ReactionStage.DEGRADED,
                ReactionReason.MISSING_OFFICIAL_ACTUAL,
                at,
            )
        supplied = (releases,) if isinstance(releases, OfficialRelease) else tuple(releases)
        if not supplied:
            return self._terminate(
                ReactionStage.DEGRADED,
                ReactionReason.MISSING_OFFICIAL_ACTUAL,
                at,
            )
        if any(not isinstance(item, OfficialRelease) for item in supplied):
            raise TypeError("releases must contain OfficialRelease values")
        hashes = _hashes((item.content_hash for item in supplied), "release_hash")
        for release in supplied:
            release.assert_integrity()
            if release.event_hash != self.identity.event_hash:
                raise ReactionBindingError("release is bound to a different event")
            if release.released_at < self.identity.scheduled_at:
                raise ReactionTransitionError("official release predates its schedule")
            if release.captured_at > at:
                raise ReactionTransitionError("release was captured in the future")
        ordered = tuple(sorted(supplied, key=lambda item: (item.revision, item.vintage_at)))
        source_mismatch = any(
            item.official_source != self.identity.official_source for item in ordered
        )
        if source_mismatch:
            return self._terminate(
                ReactionStage.DEGRADED,
                ReactionReason.OFFICIAL_SOURCE_MISMATCH,
                at,
                hashes,
            )
        metric_mismatch = any(
            item.metric != self.expectation.metric
            or item.unit != self.expectation.unit
            or item.period != self.expectation.period
            or item.basis != self.expectation.basis
            for item in ordered
        )
        if metric_mismatch:
            return self._terminate(
                ReactionStage.CONFLICTED,
                ReactionReason.RELEASE_EXPECTATION_MISMATCH,
                at,
                hashes,
            )
        if self._releases_conflict(ordered):
            return self._terminate(
                ReactionStage.CONFLICTED,
                ReactionReason.OFFICIAL_RELEASE_CONFLICT,
                at,
                hashes,
            )
        if ordered[-1].actual_value is None:
            return self._terminate(
                ReactionStage.DEGRADED,
                ReactionReason.MISSING_OFFICIAL_ACTUAL,
                at,
                hashes,
            )
        return replace(
            self,
            release_chain=ordered,
            transitions=self._append(
                ReactionStage.RELEASE_CAPTURED,
                at,
                evidence_hashes=hashes,
            ),
        )

    def mark_official_actual_unverified(
        self,
        *,
        recorded_at: datetime,
    ) -> "EventReactionLedger":
        """Fail closed after release when no official actual can be verified."""

        self._require_stage(ReactionStage.AWAITING_RELEASE, ReactionStage.DEGRADED)
        return self._terminate(
            ReactionStage.DEGRADED,
            ReactionReason.OFFICIAL_ACTUAL_UNVERIFIED,
            self._next_time(recorded_at),
        )

    def assess_surprise(
        self,
        *,
        recorded_at: datetime,
        supporting_evidence_hashes: Iterable[str] = (),
    ) -> "EventReactionLedger":
        self._require_stage(
            ReactionStage.RELEASE_CAPTURED, ReactionStage.SURPRISE_ASSESSED
        )
        at = self._next_time(recorded_at)
        release = self.release_chain[-1]
        if release.actual_value is None:
            return self._terminate(
                ReactionStage.DEGRADED,
                ReactionReason.MISSING_OFFICIAL_ACTUAL,
                at,
            )
        delta = release.actual_value - self.expectation.expected_value
        relative = (
            None
            if self.expectation.expected_value == 0
            else delta / abs(self.expectation.expected_value)
        )
        assessment = SurpriseAssessment(
            event_hash=self.identity.event_hash,
            expectation_hash=self.expectation.content_hash,
            release_hash=release.content_hash,
            metric=self.expectation.metric,
            unit=self.expectation.unit,
            expected_value=self.expectation.expected_value,
            actual_value=release.actual_value,
            delta=delta,
            relative_delta=relative,
            assessed_at=at,
            supporting_evidence_hashes=tuple(supporting_evidence_hashes),
        )
        return replace(
            self,
            surprise=assessment,
            transitions=self._append(
                ReactionStage.SURPRISE_ASSESSED,
                at,
                evidence_hashes=(assessment.content_hash,),
            ),
        )

    def assess_consensus(self, *, recorded_at: datetime) -> ConsensusAssessment:
        """Return the canonical point-in-time assessment for the latest release."""

        self.assert_integrity()
        if not self.release_chain:
            raise ReactionTransitionError(
                "consensus assessment requires a captured release"
            )
        return assess_consensus(
            release=self.release_chain[-1],
            expectation=self.expectation,
            assessed_at=recorded_at,
        )

    def observe_market(
        self,
        evidence: MarketReactionEvidence | None,
        *,
        recorded_at: datetime,
    ) -> "EventReactionLedger":
        self._require_stage(
            ReactionStage.SURPRISE_ASSESSED,
            ReactionStage.MARKET_REACTION_OBSERVED,
        )
        at = self._next_time(recorded_at)
        if evidence is None:
            return self._terminate(
                ReactionStage.DEGRADED,
                ReactionReason.MISSING_MARKET_EVIDENCE,
                at,
            )
        if not isinstance(evidence, MarketReactionEvidence):
            raise TypeError("evidence must be MarketReactionEvidence or None")
        evidence.assert_integrity()
        release = self.release_chain[-1]
        if evidence.event_hash != self.identity.event_hash:
            raise ReactionBindingError("market evidence is bound to a different event")
        if evidence.release_hash != release.content_hash:
            raise ReactionBindingError("market evidence is bound to a different release")
        if evidence.window_start < release.released_at:
            raise ReactionTransitionError("market reaction window predates the release")
        if evidence.observed_at > at:
            raise ReactionTransitionError("market evidence was observed in the future")
        if at - evidence.evidence_asof > self.max_market_evidence_age:
            return replace(
                self._terminate(
                    ReactionStage.NO_TRADE,
                    ReactionReason.STALE_MARKET_EVIDENCE,
                    at,
                    (evidence.content_hash,),
                ),
                market_reaction=evidence,
            )
        return replace(
            self,
            market_reaction=evidence,
            transitions=self._append(
                ReactionStage.MARKET_REACTION_OBSERVED,
                at,
                evidence_hashes=(evidence.content_hash,),
            ),
        )

    def reevaluate_option(
        self,
        evidence: OptionReevaluationEvidence | None,
        *,
        recorded_at: datetime,
    ) -> "EventReactionLedger":
        self._require_stage(
            ReactionStage.MARKET_REACTION_OBSERVED,
            ReactionStage.OPTION_REEVALUATED,
        )
        at = self._next_time(recorded_at)
        if evidence is None:
            return self._terminate(
                ReactionStage.DEGRADED,
                ReactionReason.MISSING_OPTION_EVIDENCE,
                at,
            )
        if not isinstance(evidence, OptionReevaluationEvidence):
            raise TypeError("evidence must be OptionReevaluationEvidence or None")
        evidence.assert_integrity()
        release = self.release_chain[-1]
        market = self.market_reaction
        assert market is not None
        if evidence.event_hash != self.identity.event_hash:
            raise ReactionBindingError("option evidence is bound to a different event")
        if evidence.release_hash != release.content_hash:
            raise ReactionBindingError("option evidence is bound to a different release")
        if evidence.market_reaction_hash != market.content_hash:
            raise ReactionBindingError(
                "option evidence is bound to a different market reaction"
            )
        if evidence.evidence_asof < market.evidence_asof:
            raise ReactionTransitionError("option evidence predates the market window")
        if evidence.observed_at > at:
            raise ReactionTransitionError("option evidence was observed in the future")
        if at - evidence.evidence_asof > self.max_option_evidence_age:
            return replace(
                self._terminate(
                    ReactionStage.NO_TRADE,
                    ReactionReason.STALE_OPTION_EVIDENCE,
                    at,
                    (evidence.content_hash,),
                ),
                option_reevaluation=evidence,
            )
        return replace(
            self,
            option_reevaluation=evidence,
            transitions=self._append(
                ReactionStage.OPTION_REEVALUATED,
                at,
                evidence_hashes=(evidence.content_hash,),
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "event": self.identity.as_dict(),
            "expectation": self.expectation.as_dict(),
            "release_chain": [item.as_dict() for item in self.release_chain],
            "surprise": None if self.surprise is None else self.surprise.as_dict(),
            "market_reaction": (
                None if self.market_reaction is None else self.market_reaction.as_dict()
            ),
            "option_reevaluation": (
                None
                if self.option_reevaluation is None
                else self.option_reevaluation.as_dict()
            ),
            "transitions": [item.as_dict() for item in self.transitions],
            "current_stage": self.current_stage.value,
            "terminal": self.terminal,
            "decision": self.decision,
            "head_hash": self.head_hash,
            **_authority_document(),
        }

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self.identity.assert_integrity()
        self.expectation.assert_integrity()
        if self.expectation.event_hash != self.identity.event_hash:
            raise ReactionBindingError("expectation/event binding mismatch")
        if not self.transitions:
            raise ReactionIntegrityError("reaction ledger cannot be empty")
        previous_stage: ReactionStage | None = None
        previous_hash = GENESIS_HASH
        previous_time: datetime | None = None
        seen_hashes: set[str] = set()
        for expected_sequence, record in enumerate(self.transitions, start=1):
            if not isinstance(record, ReactionRecord):
                raise TypeError("transitions must contain ReactionRecord values")
            record.assert_integrity()
            if record.sequence != expected_sequence:
                raise ReactionIntegrityError("reaction sequence contains a gap")
            if record.event_hash != self.identity.event_hash:
                raise ReactionBindingError("transition/event binding mismatch")
            if record.prior_hash != previous_hash:
                raise ReactionIntegrityError("reaction prior hash mismatch")
            if previous_time is not None and record.recorded_at < previous_time:
                raise ReactionTransitionError("reaction transition contains time travel")
            if previous_stage is None:
                if record.stage is not ReactionStage.SCHEDULED:
                    raise ReactionTransitionError("reaction ledger must begin SCHEDULED")
            elif previous_stage in _TERMINAL_STAGES:
                raise ReactionTransitionError("terminal reaction state cannot be extended")
            elif record.stage not in _ALLOWED_NEXT[previous_stage] and record.stage not in _TERMINAL_STAGES:
                raise ReactionTransitionError(
                    f"invalid reaction transition {previous_stage.value} -> {record.stage.value}"
                )
            if record.record_hash in seen_hashes:
                raise DuplicateReactionError("duplicate reaction record hash")
            seen_hashes.add(record.record_hash)
            previous_stage = record.stage
            previous_hash = record.record_hash
            previous_time = record.recorded_at
        for release in self.release_chain:
            release.assert_integrity()
        if self.surprise is not None:
            self.surprise.assert_integrity()
        if self.market_reaction is not None:
            self.market_reaction.assert_integrity()
        if self.option_reevaluation is not None:
            self.option_reevaluation.assert_integrity()
        self._assert_evidence_projection()

    def _assert_evidence_projection(self) -> None:
        first = self.transitions[0]
        if first.evidence_hashes != (
            self.identity.event_hash,
            self.expectation.content_hash,
        ):
            raise ReactionIntegrityError("scheduled evidence projection mismatch")
        by_stage = {record.stage: record for record in self.transitions}
        release_record = by_stage.get(ReactionStage.RELEASE_CAPTURED)
        if release_record is not None and release_record.evidence_hashes != tuple(
            item.content_hash for item in self.release_chain
        ):
            raise ReactionIntegrityError("release evidence projection mismatch")
        surprise_record = by_stage.get(ReactionStage.SURPRISE_ASSESSED)
        if surprise_record is not None and (
            self.surprise is None
            or surprise_record.evidence_hashes != (self.surprise.content_hash,)
        ):
            raise ReactionIntegrityError("surprise evidence projection mismatch")
        market_record = by_stage.get(ReactionStage.MARKET_REACTION_OBSERVED)
        if market_record is not None and (
            self.market_reaction is None
            or market_record.evidence_hashes != (self.market_reaction.content_hash,)
        ):
            raise ReactionIntegrityError("market evidence projection mismatch")
        option_record = by_stage.get(ReactionStage.OPTION_REEVALUATED)
        if option_record is not None and (
            self.option_reevaluation is None
            or option_record.evidence_hashes != (self.option_reevaluation.content_hash,)
        ):
            raise ReactionIntegrityError("option evidence projection mismatch")

    def _require_stage(self, required: ReactionStage, target: ReactionStage) -> None:
        if self.terminal:
            raise ReactionTransitionError("terminal reaction state cannot be extended")
        if self.current_stage is target:
            raise DuplicateReactionError(f"duplicate {target.value} transition")
        if self.current_stage is not required:
            raise ReactionTransitionError(
                f"{target.value} requires {required.value}, got {self.current_stage.value}"
            )

    def _next_time(self, value: datetime) -> datetime:
        at = _time(value, "recorded_at")
        if at < self.transitions[-1].recorded_at:
            raise ReactionTransitionError("reaction transition contains time travel")
        return at

    def _append(
        self,
        stage: ReactionStage,
        recorded_at: datetime,
        *,
        evidence_hashes: Iterable[str],
        reasons: tuple[ReactionReason, ...] = (),
    ) -> tuple[ReactionRecord, ...]:
        record = ReactionRecord(
            sequence=len(self.transitions) + 1,
            stage=stage,
            recorded_at=recorded_at,
            event_hash=self.identity.event_hash,
            evidence_hashes=tuple(evidence_hashes),
            prior_hash=self.head_hash,
            reasons=reasons,
        )
        return (*self.transitions, record)

    def _terminate(
        self,
        stage: ReactionStage,
        reason: ReactionReason,
        recorded_at: datetime,
        evidence_hashes: Iterable[str] = (),
    ) -> "EventReactionLedger":
        if stage not in _TERMINAL_STAGES:
            raise ValueError("terminal stage required")
        return replace(
            self,
            transitions=self._append(
                stage,
                recorded_at,
                evidence_hashes=evidence_hashes,
                reasons=(reason,),
            ),
        )

    @staticmethod
    def _releases_conflict(releases: tuple[OfficialRelease, ...]) -> bool:
        versions: set[tuple[int, datetime]] = set()
        previous: OfficialRelease | None = None
        for release in releases:
            key = (release.revision, release.vintage_at)
            if key in versions:
                return True
            versions.add(key)
            if previous is not None:
                if release.revision <= previous.revision:
                    return True
                if release.supersedes_hash != previous.content_hash:
                    return True
            previous = release
        return False


# Readable alias for callers that prefer the state-machine name.
EventReactionStateMachine = EventReactionLedger


__all__ = [
    "ConsensusAssessment",
    "ConsensusExpectation",
    "DuplicateReactionError",
    "EventReactionLedger",
    "EventReactionStateMachine",
    "MarketReactionEvidence",
    "OfficialRelease",
    "OptionReevaluationEvidence",
    "ReactionAuthorityError",
    "ReactionBindingError",
    "ReactionError",
    "ReactionIntegrityError",
    "ReactionReason",
    "ReactionRecord",
    "ReactionStage",
    "ReactionTransitionError",
    "ScheduledEventIdentity",
    "SurpriseAssessment",
    "assess_consensus",
]
