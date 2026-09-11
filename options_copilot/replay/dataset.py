"""Leakage-safe, contract-bound historical replay datasets."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from options_copilot.learning.outcomes import (
    IndependenceSpecValidationError,
    VerifiedIndependenceSpec,
    verify_independence_spec,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)
from options_copilot.storage.evidence import EvidenceStore


DATASET_SCHEMA = "options_copilot.replay.dataset_manifest.v3"
WINDOW_SCHEMA = "options_copilot.replay.chronological_window.v2"
MISSING_EXECUTION_DATA = "MISSING_EXECUTION_DATA"
STALE_EXECUTION_DATA = "STALE_EXECUTION_DATA"
SUPPORTED_INDEPENDENCE_VERSIONS = frozenset({"v1"})
MAX_QUOTE_AGE_SECONDS = Decimal("5")


class DatasetError(RuntimeError):
    pass


class DatasetContractError(DatasetError):
    pass


class DatasetValidationError(DatasetError):
    pass


class IndependenceSpecArtifactError(DatasetError):
    pass


class IndependenceSpecArtifactStore:
    """Immutable, hash-addressed independence-spec artifacts.

    The store owns a deep-frozen copy of every artifact and re-runs the one
    canonical verifier on every resolution.  A manifest can therefore bind a
    hash, but it cannot declare whether that hash is test-only or production.
    """

    __slots__ = ("_artifacts", "_store_hash")

    def __setattr__(self, name: str, value: object) -> None:
        del name, value
        raise IndependenceSpecArtifactError("INDEPENDENCE_ARTIFACT_STORE_IMMUTABLE")

    def __init__(
        self,
        artifacts: Sequence[VerifiedIndependenceSpec | Mapping[str, object]],
    ) -> None:
        indexed: dict[str, Mapping[str, object]] = {}
        for value in artifacts:
            document = value.as_dict() if isinstance(value, VerifiedIndependenceSpec) else value
            frozen = freeze_json(document)
            if not isinstance(frozen, Mapping):
                raise IndependenceSpecArtifactError("INDEPENDENCE_ARTIFACT_INVALID")
            spec_hash = _digest("independence spec_hash", frozen.get("spec_hash"))
            prior = indexed.get(spec_hash)
            if prior is not None and prior != frozen:
                raise IndependenceSpecArtifactError("INDEPENDENCE_ARTIFACT_CONFLICT")
            indexed[spec_hash] = frozen
        if not indexed:
            raise IndependenceSpecArtifactError("INDEPENDENCE_ARTIFACT_REQUIRED")
        ordered = tuple((key, indexed[key]) for key in sorted(indexed))
        object.__setattr__(self, "_artifacts", ordered)
        object.__setattr__(self, "_store_hash", canonical_hash(thaw_json(ordered)))

    @property
    def store_hash(self) -> str:
        return self._store_hash

    def assert_integrity(self) -> None:
        if canonical_hash(thaw_json(self._artifacts)) != self._store_hash:
            raise IndependenceSpecArtifactError("INDEPENDENCE_ARTIFACT_STORE_CORRUPT")

    def resolve(
        self,
        spec_hash: str,
        *,
        allow_test_fixture: bool,
        as_of: datetime,
    ) -> VerifiedIndependenceSpec:
        self.assert_integrity()
        identity = _digest("independence_hash", spec_hash)
        matches = [document for key, document in self._artifacts if key == identity]
        if len(matches) != 1:
            raise IndependenceSpecArtifactError("INDEPENDENCE_ARTIFACT_NOT_FOUND")
        try:
            return verify_independence_spec(
                thaw_json(matches[0]),
                allow_test_fixture=allow_test_fixture,
                as_of=as_of,
            )
        except IndependenceSpecValidationError as exc:
            raise IndependenceSpecArtifactError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class ContractBinding:
    kind: str
    version: str
    content_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", _text("contract kind", self.kind).upper())
        object.__setattr__(self, "version", _text("contract version", self.version))
        object.__setattr__(
            self, "content_hash", _digest("contract content_hash", self.content_hash)
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "version": self.version,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class SourceBinding:
    source: str
    version: str
    content_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _text("source", self.source))
        object.__setattr__(self, "version", _text("source version", self.version))
        object.__setattr__(
            self, "content_hash", _digest("source content_hash", self.content_hash)
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "source": self.source,
            "version": self.version,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class ChronologicalWindow:
    window_id: str
    train_samples: tuple[Mapping[str, object], ...]
    calibration_samples: tuple[Mapping[str, object], ...]
    test_samples: tuple[Mapping[str, object], ...]
    train_start: datetime
    train_end: datetime
    calibration_start: datetime
    calibration_end: datetime
    test_start: datetime
    test_end: datetime
    window_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "window_id", _text("window_id", self.window_id))
        for name in ("train_samples", "calibration_samples", "test_samples"):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, tuple):
                raise TypeError(f"{name} must be a sequence")
            object.__setattr__(self, name, frozen)
        for name in (
            "train_start",
            "train_end",
            "calibration_start",
            "calibration_end",
            "test_start",
            "test_end",
        ):
            object.__setattr__(
                self, name, utc_datetime(getattr(self, name), field=name)
            )

    @property
    def train_ids(self) -> tuple[str, ...]:
        return tuple(str(item["sample_id"]) for item in self.train_samples)

    @property
    def calibration_ids(self) -> tuple[str, ...]:
        return tuple(str(item["sample_id"]) for item in self.calibration_samples)

    @property
    def test_ids(self) -> tuple[str, ...]:
        return tuple(str(item["sample_id"]) for item in self.test_samples)

    def identity_document(self) -> dict[str, object]:
        return {
            "schema": WINDOW_SCHEMA,
            "window_id": self.window_id,
            "train_samples": thaw_json(self.train_samples),
            "calibration_samples": thaw_json(self.calibration_samples),
            "test_samples": thaw_json(self.test_samples),
            "train_start": datetime_text(self.train_start),
            "train_end": datetime_text(self.train_end),
            "calibration_start": datetime_text(self.calibration_start),
            "calibration_end": datetime_text(self.calibration_end),
            "test_start": datetime_text(self.test_start),
            "test_end": datetime_text(self.test_end),
        }

    def verify(self) -> "ChronologicalWindow":
        partitions = (
            ("train", self.train_samples, self.train_start, self.train_end),
            (
                "calibration",
                self.calibration_samples,
                self.calibration_start,
                self.calibration_end,
            ),
            ("test", self.test_samples, self.test_start, self.test_end),
        )
        if any(not samples for _, samples, _, _ in partitions):
            raise DatasetContractError("WINDOW_PARTITION_EMPTY")
        if not (
            self.train_start
            <= self.train_end
            < self.calibration_start
            <= self.calibration_end
            < self.test_start
            <= self.test_end
        ):
            raise DatasetContractError("WINDOW_NOT_CHRONOLOGICAL")
        seen: set[str] = set()
        for name, samples, start, end in partitions:
            previous: datetime | None = None
            for item in samples:
                sample_id = _text(f"{name} sample_id", item.get("sample_id"))
                decision_at = _timestamp(
                    f"{name} decision_at", item.get("decision_at")
                )
                if sample_id in seen:
                    raise DatasetContractError("WINDOW_SAMPLE_OVERLAP")
                if not start <= decision_at <= end:
                    raise DatasetContractError("WINDOW_SAMPLE_OUTSIDE_PARTITION")
                if previous is not None and decision_at < previous:
                    raise DatasetContractError("WINDOW_SAMPLE_ORDER_INVALID")
                seen.add(sample_id)
                previous = decision_at
        if canonical_hash(self.identity_document()) != self.window_hash:
            raise DatasetContractError("WINDOW_HASH_MISMATCH")
        return self

    def as_dict(self) -> dict[str, object]:
        return {**self.identity_document(), "window_hash": self.window_hash}


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    decision_at: datetime
    sample_id: str
    scan_run_id: str
    ranking_snapshot_id: str | None
    ranking_snapshot_hash: str | None
    universe_membership: tuple[Mapping[str, object], ...]
    included_evidence: tuple[Mapping[str, object], ...]
    exclusions: tuple[Mapping[str, object], ...]
    execution_availability: tuple[Mapping[str, object], ...]
    initial_policy_version: str
    initial_policy_hash: str
    execution_cost_version: str
    execution_cost_hash: str
    independence_version: str
    independence_hash: str
    source_versions: tuple[Mapping[str, object], ...]
    source_versions_hash: str
    window: Mapping[str, object]
    window_hash: str
    pipeline_version: str
    pipeline_hash: str
    input_hash: str
    evidence_hash: str
    broker_snapshot_hash: str
    dataset_hash: str
    schema: str = DATASET_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "decision_at", utc_datetime(self.decision_at, field="decision_at")
        )
        for name in (
            "universe_membership",
            "included_evidence",
            "exclusions",
            "execution_availability",
            "source_versions",
        ):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, tuple):
                raise TypeError(f"{name} must be a sequence")
            object.__setattr__(self, name, frozen)
        frozen_window = freeze_json(self.window)
        if not isinstance(frozen_window, Mapping):
            raise TypeError("window must be a mapping")
        object.__setattr__(self, "window", frozen_window)

    def identity_document(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "decision_at": datetime_text(self.decision_at),
            "sample_id": self.sample_id,
            "scan_run_id": self.scan_run_id,
            "ranking_snapshot_id": self.ranking_snapshot_id,
            "ranking_snapshot_hash": self.ranking_snapshot_hash,
            "universe_membership": thaw_json(self.universe_membership),
            "included_evidence": thaw_json(self.included_evidence),
            "exclusions": thaw_json(self.exclusions),
            "execution_availability": thaw_json(self.execution_availability),
            "initial_policy_version": self.initial_policy_version,
            "initial_policy_hash": self.initial_policy_hash,
            "execution_cost_version": self.execution_cost_version,
            "execution_cost_hash": self.execution_cost_hash,
            "independence_version": self.independence_version,
            "independence_hash": self.independence_hash,
            "source_versions": thaw_json(self.source_versions),
            "source_versions_hash": self.source_versions_hash,
            "window": thaw_json(self.window),
            "window_hash": self.window_hash,
            "pipeline_version": self.pipeline_version,
            "pipeline_hash": self.pipeline_hash,
            "input_hash": self.input_hash,
            "evidence_hash": self.evidence_hash,
            "broker_snapshot_hash": self.broker_snapshot_hash,
        }

    def verify(
        self,
        *,
        independence_store: IndependenceSpecArtifactStore,
        allow_test_fixture: bool,
        verification_as_of: datetime,
    ) -> "DatasetManifest":
        if self.schema != DATASET_SCHEMA:
            raise DatasetValidationError("DATASET_SCHEMA_MISMATCH")
        if type(independence_store) is not IndependenceSpecArtifactStore:
            raise DatasetValidationError("INDEPENDENCE_ARTIFACT_STORE_REQUIRED")
        try:
            verified = independence_store.resolve(
                self.independence_hash,
                allow_test_fixture=allow_test_fixture,
                as_of=verification_as_of,
            )
        except IndependenceSpecArtifactError as exc:
            raise DatasetValidationError(str(exc)) from exc
        if (
            verified.version != self.independence_version
            or verified.spec_hash != self.independence_hash
        ):
            raise DatasetValidationError("INDEPENDENCE_ARTIFACT_BINDING_MISMATCH")
        for name in (
            "initial_policy_hash",
            "execution_cost_hash",
            "independence_hash",
            "source_versions_hash",
            "window_hash",
            "pipeline_hash",
            "input_hash",
            "evidence_hash",
            "broker_snapshot_hash",
            "dataset_hash",
        ):
            _digest(name, getattr(self, name))
        if self.ranking_snapshot_hash is not None:
            _digest("ranking_snapshot_hash", self.ranking_snapshot_hash)
        if (self.ranking_snapshot_id is None) is not (
            self.ranking_snapshot_hash is None
        ):
            raise DatasetValidationError("RANKING_SNAPSHOT_BINDING_INCOMPLETE")
        if canonical_hash(thaw_json(self.source_versions)) != self.source_versions_hash:
            raise DatasetValidationError("SOURCE_VERSIONS_HASH_MISMATCH")
        if canonical_hash(thaw_json(self.window)) != self.window_hash:
            raise DatasetValidationError("WINDOW_HASH_MISMATCH")
        test_samples = self.window.get("test_samples")
        if not isinstance(test_samples, tuple):
            raise DatasetValidationError("WINDOW_TEST_PARTITION_INVALID")
        matching = [row for row in test_samples if row.get("sample_id") == self.sample_id]
        if len(matching) != 1 or _timestamp(
            "current sample decision_at", matching[0].get("decision_at")
        ) != self.decision_at:
            raise DatasetValidationError("CURRENT_SAMPLE_NOT_IN_TEST")
        for record in self.included_evidence:
            if _timestamp("first_seen_at", record.get("first_seen_at")) > self.decision_at:
                raise DatasetValidationError("FUTURE_EVIDENCE_IN_DATASET")
        for record in self.universe_membership:
            if (
                _timestamp("first_seen_at", record.get("first_seen_at"))
                > self.decision_at
                or _timestamp("as_of", record.get("as_of")) > self.decision_at
            ):
                raise DatasetValidationError("UNIVERSE_AFTER_CUTOFF")
        for record in self.execution_availability:
            if (
                _timestamp("observed_at", record.get("observed_at"))
                > self.decision_at
                or _timestamp("received_at", record.get("received_at"))
                > self.decision_at
            ):
                raise DatasetValidationError("EXECUTION_AFTER_CUTOFF")
        _verify_exclusions(self)
        if canonical_hash(self.identity_document()) != self.dataset_hash:
            raise DatasetValidationError("DATASET_HASH_MISMATCH")
        return self

    def as_dict(self) -> dict[str, object]:
        return {**self.identity_document(), "dataset_hash": self.dataset_hash}


class PointInTimeDatasetBuilder:
    def __init__(
        self,
        *,
        allow_test_fixtures: bool = False,
        verification_as_of: datetime | None = None,
    ) -> None:
        self.allow_test_fixtures = allow_test_fixtures is True
        self.verification_as_of = utc_datetime(
            verification_as_of or datetime.now(timezone.utc),
            field="verification_as_of",
        )

    def build(
        self,
        *,
        decision_at: datetime,
        sample_id: str,
        scan_run_id: str,
        ranking_snapshot_id: str | None,
        ranking_snapshot_hash: str | None,
        universe_membership: Sequence[Mapping[str, object]],
        evidence_store: EvidenceStore,
        execution_records: Sequence[Mapping[str, object] | object],
        initial_policy: ContractBinding | Mapping[str, object],
        execution_cost: ContractBinding | Mapping[str, object],
        independence_store: IndependenceSpecArtifactStore,
        independence_hash: str,
        source_versions: Mapping[str, SourceBinding | Mapping[str, object]],
        window: ChronologicalWindow,
        pipeline_version: str,
        pipeline_hash: str,
        input_hash: str,
        evidence_hash: str,
        broker_snapshot_hash: str,
    ) -> DatasetManifest:
        cutoff = utc_datetime(decision_at, field="decision_at")
        current_sample_id = _text("sample_id", sample_id)
        policy = _contract_binding(initial_policy, expected_kind="INITIAL_POLICY")
        cost = _contract_binding(execution_cost, expected_kind="EXECUTION_COST")
        independence = self._verify_independence(
            independence_store,
            independence_hash,
        )
        if (
            independence.initial_policy_version != policy.version
            or independence.initial_policy_hash != policy.content_hash
            or independence.execution_cost_version != cost.version
            or independence.execution_cost_hash != cost.content_hash
        ):
            raise DatasetContractError("INDEPENDENCE_PREREQUISITE_MISMATCH")
        if independence.version not in SUPPORTED_INDEPENDENCE_VERSIONS:
            raise DatasetContractError("INDEPENDENCE_VERSION_UNSUPPORTED")
        if type(window) is not ChronologicalWindow:
            raise DatasetContractError("CHRONOLOGICAL_WINDOW_REQUIRED")
        window.verify()
        test_matches = [
            item for item in window.test_samples if item.get("sample_id") == current_sample_id
        ]
        if len(test_matches) != 1 or _timestamp(
            "current sample decision_at", test_matches[0].get("decision_at")
        ) != cutoff:
            raise DatasetContractError("CURRENT_SAMPLE_NOT_IN_TEST")
        ranking_id = (
            None
            if ranking_snapshot_id is None
            else _text("ranking_snapshot_id", ranking_snapshot_id)
        )
        ranking_hash = (
            None
            if ranking_snapshot_hash is None
            else _digest("ranking_snapshot_hash", ranking_snapshot_hash)
        )
        if (ranking_id is None) is not (ranking_hash is None):
            raise DatasetContractError("RANKING_SNAPSHOT_BINDING_INCOMPLETE")
        universe, universe_exclusions = _universe_rows(
            universe_membership, cutoff
        )
        included, evidence_exclusions = _evidence_rows(evidence_store, cutoff)
        execution, execution_exclusions = _execution_rows(
            execution_records, cutoff
        )
        exclusions = tuple(
            sorted(
                universe_exclusions + evidence_exclusions + execution_exclusions,
                key=lambda row: (
                    str(row["record_type"]),
                    str(row["record_id"]),
                    str(row["reason"]),
                ),
            )
        )
        sources = _source_bindings(source_versions)
        source_document = tuple(item.as_dict() for item in sources)
        window_document = window.identity_document()
        kwargs = {
            "decision_at": cutoff,
            "sample_id": current_sample_id,
            "scan_run_id": _text("scan_run_id", scan_run_id),
            "ranking_snapshot_id": ranking_id,
            "ranking_snapshot_hash": ranking_hash,
            "universe_membership": universe,
            "included_evidence": included,
            "exclusions": exclusions,
            "execution_availability": execution,
            "initial_policy_version": policy.version,
            "initial_policy_hash": policy.content_hash,
            "execution_cost_version": cost.version,
            "execution_cost_hash": cost.content_hash,
            "independence_version": independence.version,
            "independence_hash": independence.spec_hash,
            "source_versions": source_document,
            "source_versions_hash": canonical_hash(source_document),
            "window": window_document,
            "window_hash": window.window_hash,
            "pipeline_version": _text("pipeline_version", pipeline_version),
            "pipeline_hash": _digest("pipeline_hash", pipeline_hash),
            "input_hash": _digest("input_hash", input_hash),
            "evidence_hash": _digest("evidence_hash", evidence_hash),
            "broker_snapshot_hash": _digest(
                "broker_snapshot_hash", broker_snapshot_hash
            ),
        }
        shell = DatasetManifest(**kwargs, dataset_hash="0" * 64)
        manifest = DatasetManifest(
            **kwargs, dataset_hash=canonical_hash(shell.identity_document())
        )
        return manifest.verify(
            independence_store=independence_store,
            allow_test_fixture=self.allow_test_fixtures,
            verification_as_of=self.verification_as_of,
        )

    def _verify_independence(
        self,
        store: IndependenceSpecArtifactStore,
        spec_hash: str,
    ) -> VerifiedIndependenceSpec:
        if type(store) is not IndependenceSpecArtifactStore:
            raise DatasetContractError("INDEPENDENCE_ARTIFACT_STORE_REQUIRED")
        try:
            verified = store.resolve(
                spec_hash,
                allow_test_fixture=self.allow_test_fixtures,
                as_of=self.verification_as_of,
            )
        except IndependenceSpecArtifactError as exc:
            raise DatasetContractError(str(exc)) from exc
        if verified.test_only and not self.allow_test_fixtures:
            raise DatasetContractError(
                "test fixture requires explicit allow_test_fixtures"
            )
        return verified


def build_dataset_manifest(**kwargs: object) -> DatasetManifest:
    return PointInTimeDatasetBuilder().build(**kwargs)  # type: ignore[arg-type]


def build_rolling_windows(
    samples: Sequence[Mapping[str, object] | object],
    *,
    train_size: int,
    calibration_size: int,
    test_size: int,
    step_size: int | None = None,
) -> tuple[ChronologicalWindow, ...]:
    sizes = {
        "train_size": train_size,
        "calibration_size": calibration_size,
        "test_size": test_size,
        "step_size": test_size if step_size is None else step_size,
    }
    for name, value in sizes.items():
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    normalized: list[dict[str, object]] = []
    for raw in samples:
        row = _document(raw)
        normalized.append(
            {
                "sample_id": _text(
                    "sample_id", row.get("sample_id", row.get("id"))
                ),
                "decision_at": datetime_text(
                    _timestamp("decision_at", row.get("decision_at"))
                ),
            }
        )
    normalized.sort(key=lambda item: (str(item["decision_at"]), str(item["sample_id"])))
    ids = [str(item["sample_id"]) for item in normalized]
    if len(ids) != len(set(ids)):
        raise DatasetValidationError("DUPLICATE_SAMPLE_ID")
    total = train_size + calibration_size + test_size
    windows: list[ChronologicalWindow] = []
    for start in range(0, len(normalized) - total + 1, sizes["step_size"]):
        train = tuple(normalized[start : start + train_size])
        calibration = tuple(
            normalized[start + train_size : start + train_size + calibration_size]
        )
        test = tuple(
            normalized[start + train_size + calibration_size : start + total]
        )
        boundaries = {
            "train_start": _timestamp("train_start", train[0]["decision_at"]),
            "train_end": _timestamp("train_end", train[-1]["decision_at"]),
            "calibration_start": _timestamp(
                "calibration_start", calibration[0]["decision_at"]
            ),
            "calibration_end": _timestamp(
                "calibration_end", calibration[-1]["decision_at"]
            ),
            "test_start": _timestamp("test_start", test[0]["decision_at"]),
            "test_end": _timestamp("test_end", test[-1]["decision_at"]),
        }
        identity = {
            "schema": WINDOW_SCHEMA,
            "window_id": f"rolling-{len(windows) + 1:04d}",
            "train_samples": train,
            "calibration_samples": calibration,
            "test_samples": test,
            **{key: datetime_text(value) for key, value in boundaries.items()},
        }
        window = ChronologicalWindow(
            window_id=str(identity["window_id"]),
            train_samples=train,
            calibration_samples=calibration,
            test_samples=test,
            **boundaries,
            window_hash=canonical_hash(identity),
        )
        windows.append(window.verify())
    return tuple(windows)


build_walk_forward_windows = build_rolling_windows


def _contract_binding(
    value: ContractBinding | Mapping[str, object], *, expected_kind: str
) -> ContractBinding:
    if isinstance(value, ContractBinding):
        result = value
    elif isinstance(value, Mapping):
        result = ContractBinding(
            str(value.get("kind", value.get("contract_kind", ""))),
            str(value.get("version", "")),
            str(value.get("content_hash", value.get("contract_hash", ""))),
        )
    else:
        raise DatasetContractError("CONTRACT_BINDING_REQUIRED")
    aliases = {
        "INITIAL_POLICY": {
            "INITIAL_POLICY",
            "INITIAL_CHAMPION_SCENARIO_POLICY",
        },
        "EXECUTION_COST": {"EXECUTION_COST"},
    }
    if result.kind not in aliases[expected_kind]:
        raise DatasetContractError(f"{expected_kind}_BINDING_MISMATCH")
    return result


def _source_bindings(
    values: Mapping[str, SourceBinding | Mapping[str, object]],
) -> tuple[SourceBinding, ...]:
    if not isinstance(values, Mapping) or not values:
        raise DatasetContractError("SOURCE_VERSIONS_REQUIRED")
    result: list[SourceBinding] = []
    for name, raw in values.items():
        if isinstance(raw, SourceBinding):
            item = raw
        elif isinstance(raw, Mapping):
            item = SourceBinding(
                str(raw.get("source", name)),
                str(raw.get("version", "")),
                str(raw.get("content_hash", raw.get("hash", ""))),
            )
        else:
            raise DatasetContractError("SOURCE_BINDING_INVALID")
        if item.source != name:
            raise DatasetContractError("SOURCE_BINDING_NAME_MISMATCH")
        result.append(item)
    return tuple(sorted(result, key=lambda item: item.source))


def _universe_rows(
    values: Sequence[Mapping[str, object]], cutoff: datetime
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    rows: list[dict[str, object]] = []
    exclusions: list[dict[str, object]] = []
    snapshots: set[tuple[str, str]] = set()
    for raw in values:
        if not isinstance(raw, Mapping):
            raise DatasetContractError("UNIVERSE_BINDING_INVALID")
        symbol = _text("universe symbol", raw.get("symbol")).upper()
        included = raw.get("included")
        if not isinstance(included, bool):
            raise DatasetContractError("UNIVERSE_MEMBERSHIP_INVALID")
        reason = None if included else _text("universe reason", raw.get("reason"))
        first_seen_at = _timestamp("first_seen_at", raw.get("first_seen_at"))
        as_of = _timestamp("as_of", raw.get("as_of"))
        if first_seen_at > cutoff or as_of > cutoff:
            raise DatasetContractError("UNIVERSE_AFTER_CUTOFF")
        source_id = _text("universe source_id", raw.get("source_id"))
        source_hash = _digest("universe source_hash", raw.get("source_hash"))
        source_snapshot_id = _text(
            "source_snapshot_id", raw.get("source_snapshot_id")
        )
        source_snapshot_hash = _digest(
            "source_snapshot_hash", raw.get("source_snapshot_hash")
        )
        snapshots.add((source_snapshot_id, source_snapshot_hash))
        item = {
            "symbol": symbol,
            "included": included,
            "reason": reason,
            "first_seen_at": datetime_text(first_seen_at),
            "as_of": datetime_text(as_of),
            "source_id": source_id,
            "source_hash": source_hash,
            "source_snapshot_id": source_snapshot_id,
            "source_snapshot_hash": source_snapshot_hash,
        }
        rows.append(item)
        if reason is not None:
            exclusions.append(
                _exclusion("UNIVERSE", symbol, reason, source_id, source_hash)
            )
    if not rows:
        raise DatasetContractError("UNIVERSE_MEMBERSHIP_REQUIRED")
    if len(snapshots) != 1:
        raise DatasetContractError("MIXED_UNIVERSE_SNAPSHOT")
    rows.sort(key=lambda item: str(item["symbol"]))
    if len({str(item["symbol"]) for item in rows}) != len(rows):
        raise DatasetContractError("DUPLICATE_UNIVERSE_MEMBER")
    return tuple(rows), tuple(exclusions)


def _evidence_rows(
    store: EvidenceStore, cutoff: datetime
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    if type(store) is not EvidenceStore:
        raise DatasetContractError("EVIDENCE_STORE_REQUIRED")
    store.assert_integrity()
    records = store.query(first_seen_at_or_before=cutoff, limit=5000)
    if len(records) == 5000:
        raise DatasetContractError("EVIDENCE_QUERY_LIMIT_REACHED")
    included: list[dict[str, object]] = []
    exclusions: list[dict[str, object]] = []
    for stored in records:
        row = stored.as_dict()
        first_seen = _timestamp("first_seen_at", row["first_seen_at"])
        if first_seen > cutoff:
            raise DatasetValidationError("FUTURE_EVIDENCE_FROM_STORE")
        evidence_id = _text("evidence_id", row["evidence_id"])
        source_id = _text("source_id", row["source_id"])
        source_hash = _digest("content_hash", row["content_hash"])
        status = _text("effective_status", row["effective_status"]).upper()
        if status != "ACTIVE":
            reason = (
                "CONFLICTED_EVIDENCE"
                if status == "CONFLICTED"
                else f"EVIDENCE_STATUS_{status}"
            )
            exclusions.append(
                _exclusion("EVIDENCE", evidence_id, reason, source_id, source_hash)
            )
            continue
        included.append(
            {
                "evidence_id": evidence_id,
                "identity": row["identity"],
                "source_id": source_id,
                "source_hash": source_hash,
                "first_seen_at": datetime_text(first_seen),
                "kind": row["kind"],
                "symbol": row["symbol"],
                "provider": row["provider"],
                "effective_status": status,
                "payload_hash": canonical_hash(row["payload"]),
                "row_hash": _digest("row_hash", row["row_hash"]),
            }
        )
    included.sort(key=lambda item: str(item["evidence_id"]))
    exclusions.sort(key=lambda item: str(item["record_id"]))
    return tuple(included), tuple(exclusions)


def _execution_rows(
    values: Sequence[Mapping[str, object] | object], cutoff: datetime
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    rows: dict[str, dict[str, object]] = {}
    exclusions: list[dict[str, object]] = []
    batches: set[str] = set()
    for raw in values:
        value = _document(raw)
        contract_id = _text(
            "contract_id",
            value.get("contract_id", value.get("con_id")),
        )
        source_id = _text("execution source_id", value.get("source_id"))
        source_hash = _digest(
            "execution source_hash", value.get("source_hash", value.get("content_hash"))
        )
        observed_at = _timestamp("observed_at", value.get("observed_at"))
        received_at = _timestamp("received_at", value.get("received_at"))
        if observed_at > received_at:
            raise DatasetContractError("EXECUTION_TIME_ORDER_INVALID")
        if observed_at > cutoff or received_at > cutoff:
            raise DatasetContractError("EXECUTION_AFTER_CUTOFF")
        quote_batch_id = _text("quote_batch_id", value.get("quote_batch_id"))
        batches.add(quote_batch_id)
        secdef_hash = _optional_digest(value.get("secdef_hash"))
        bid = _positive_decimal(value.get("bid"))
        ask = _positive_decimal(value.get("ask"))
        age = Decimal(str((cutoff - observed_at).total_seconds()))
        if age > MAX_QUOTE_AGE_SECONDS:
            reason = STALE_EXECUTION_DATA
        elif secdef_hash is None:
            reason = "MISSING_SECDEF"
        elif bid is None or ask is None or bid >= ask:
            reason = MISSING_EXECUTION_DATA
        else:
            reason = None
        item = {
            "contract_id": contract_id,
            "source_id": source_id,
            "source_hash": source_hash,
            "observed_at": datetime_text(observed_at),
            "received_at": datetime_text(received_at),
            "quote_age_seconds": str(age.normalize()),
            "quote_batch_id": quote_batch_id,
            "secdef_hash": secdef_hash,
            "bid": None if bid is None else _decimal_text(bid),
            "ask": None if ask is None else _decimal_text(ask),
            "available": reason is None,
            "reason": reason,
        }
        prior = rows.get(contract_id)
        if prior is not None and prior != item:
            raise DatasetContractError("CONFLICTING_EXECUTION_RECORD")
        rows[contract_id] = item
        if reason is not None:
            exclusions.append(
                _exclusion("EXECUTION", contract_id, reason, source_id, source_hash)
            )
    if not rows:
        raise DatasetContractError("EXECUTION_RECORDS_REQUIRED")
    if len(batches) != 1:
        raise DatasetContractError("MIXED_QUOTE_BATCH")
    return tuple(rows[key] for key in sorted(rows)), tuple(exclusions)


def _verify_exclusions(manifest: DatasetManifest) -> None:
    keys: set[tuple[object, ...]] = set()
    for row in manifest.exclusions:
        key = (
            row.get("record_type"),
            row.get("record_id"),
            row.get("reason"),
            _text("exclusion source_id", row.get("source_id")),
            _digest("exclusion source_hash", row.get("source_hash")),
        )
        if key in keys:
            raise DatasetValidationError("DUPLICATE_EXCLUSION")
        keys.add(key)
    semantic = {key[:3] for key in keys}
    for row in manifest.universe_membership:
        if row.get("included") is False and (
            "UNIVERSE",
            row.get("symbol"),
            row.get("reason"),
        ) not in semantic:
            raise DatasetValidationError("UNIVERSE_EXCLUSION_MISSING")
    for row in manifest.execution_availability:
        if row.get("available") is False and (
            "EXECUTION",
            row.get("contract_id"),
            row.get("reason"),
        ) not in semantic:
            raise DatasetValidationError("EXECUTION_EXCLUSION_MISSING")


def _exclusion(
    record_type: str,
    record_id: str,
    reason: str,
    source_id: str,
    source_hash: str,
) -> dict[str, object]:
    return {
        "record_type": record_type,
        "record_id": record_id,
        "reason": reason,
        "source_id": source_id,
        "source_hash": source_hash,
    }


def _document(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    return vars(value) if hasattr(value, "__dict__") else {}


def _text(field: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DatasetValidationError(f"{field} must be nonblank")
    return value.strip()


def _digest(field: str, value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise DatasetValidationError(f"{field} must be lowercase SHA-256")
    return value


def _optional_digest(value: object) -> str | None:
    try:
        return _digest("hash", value)
    except DatasetValidationError:
        return None


def _timestamp(field: str, value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DatasetValidationError(f"{field} must be an ISO timestamp") from exc
    else:
        raise DatasetValidationError(f"{field} must be a timestamp")
    try:
        return utc_datetime(parsed, field=field)
    except (TypeError, ValueError) as exc:
        raise DatasetValidationError(f"{field} must be timezone-aware") from exc


def _positive_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if not normalized else format(normalized, "f")


__all__ = [
    "ChronologicalWindow",
    "ContractBinding",
    "DATASET_SCHEMA",
    "DatasetContractError",
    "DatasetError",
    "DatasetManifest",
    "DatasetValidationError",
    "IndependenceSpecArtifactError",
    "IndependenceSpecArtifactStore",
    "MISSING_EXECUTION_DATA",
    "PointInTimeDatasetBuilder",
    "STALE_EXECUTION_DATA",
    "SourceBinding",
    "build_dataset_manifest",
    "build_rolling_windows",
    "build_walk_forward_windows",
]
