"""Immutable, cost-adjusted forward outcomes and independent sample counts.

This module is intentionally outside broker and approval paths.  It accepts
only frozen identities and observed results, writes an append-only local
ledger, and produces evaluation inputs only when an injected independence
specification has been canonically verified.  Missing production independence
authority therefore fails closed without losing the raw observation.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any

from options_copilot.execution_cost import (
    CandidateCostResolution,
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
    ExecutionCostResolution,
    SignedExecutionCostResolver,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


SCHEMA_VERSION = 1
GENESIS_HASH = "0" * 64
INDEPENDENCE_SPEC_SCHEMA = "options_copilot.learning.independence_spec.v1"
BROKER_OUTCOME_EVIDENCE_REF_SCHEMA = (
    "options_copilot.broker_outcome_evidence_ref.v1"
)
BROKER_OUTCOME_EVIDENCE_SCHEMA = "options_copilot.broker_outcome_evidence.v1"
BROKER_OUTCOME_EVIDENCE_HEAD_SCHEMA = (
    "options_copilot.broker_outcome_evidence_head.v1"
)
INITIAL_POLICY_VERSION = "v1"
INITIAL_POLICY_HASH = "b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_VERSION_RE = re.compile(r"v[1-9][0-9]*(?:\.[0-9]+)*\Z")
_MAXIMUM_QUOTE_AGE_SECONDS = Decimal("5")
_MAXIMUM_ECONOMIC_OBSERVATION_DELAY_SECONDS = Decimal("5")
OUTCOME_HORIZONS = ("30M", "SESSION_CLOSE", "1D", "3D", "5D")
OUTCOME_TARGET_RULES = {
    "30M": "PREDICTED_AT_PLUS_30_MINUTES",
    "SESSION_CLOSE": "NEXT_ELIGIBLE_SESSION_CLOSE",
    "1D": "SESSION_CLOSE_PLUS_1_TRADING_DAY",
    "3D": "SESSION_CLOSE_PLUS_3_TRADING_DAYS",
    "5D": "SESSION_CLOSE_PLUS_5_TRADING_DAYS",
}
_COST_FIELDS = (
    "commission_usd",
    "fees_usd",
    "spread_usd",
    "slippage_usd",
    "assignment_usd",
    "exercise_usd",
    "dividend_usd",
)
_BOUND_FIELDS = (
    "decision_id",
    "decision_hash",
    "candidate_id",
    "candidate_hash",
    "ranking_snapshot_id",
    "ranking_snapshot_hash",
    "ranking_basis_hash",
    "horizon",
    "horizon_at",
    "decision_at",
    "economic_observed_at",
    "input_hash",
    "evidence_hash",
    "broker_snapshot_hash",
    "current_policy_version",
    "current_policy_hash",
    "policy_authority_marker_hash",
    "cost_version",
    "cost_hash",
    "execution_cost_resolution_hash",
    "cost_recompute_request_hash",
    "candidate_cost_calculation_hash",
    "exit_policy_hash",
    "thesis_hash",
    "quote_identity_hash",
    "position_management_hash",
    "counterfactual_spec_hash",
    "position_management_result_hash",
    "counterfactual_result_hash",
    "outcome_result_authority_hash",
    "independence_version",
    "independence_hash",
    "broker_outcome_evidence_authority_hash",
)
_BASE_IDENTITY_FIELDS = (
    "decision_id",
    "candidate_hash",
    "ranking_snapshot_id",
    "horizon",
)
_DEFAULT_RULES: dict[str, object] = {
    "same_event_identity": True,
    "event_identity_namespace_fields": ["ticker", "issuer_id", "provider"],
    "same_ticker_event": True,
    "same_corporate_family_event": True,
    "adjacent_same_ticker_slots": True,
    "adjacent_slot_hours": 2,
    "overlapping_event_windows": True,
    "correlated_tickers_same_macro_event": True,
    "cross_provider_duplicates": True,
    "overlapping_holdings_thesis_structure": True,
    "representative_selection": "EARLIEST_DECISION_THEN_ID",
    "independent_weight": 1,
    "duplicate_weight": 0,
    "unknown_weight": 0,
    "unknown_exclusion_reason": "INDEPENDENCE_UNKNOWN",
    "correlated_ticker_groups": [],
}


class OutcomeError(RuntimeError):
    """Base error for outcome recording and evaluation."""


class OutcomeValidationError(OutcomeError, ValueError):
    """An outcome is malformed or cannot be represented safely."""


class OutcomeIdentityConflict(OutcomeError):
    """A late version changed an identity that the original froze."""


class BrokerOutcomeEvidenceStore:
    """Reserved marker for a future fixed production broker-evidence adapter.

    Production outcome evidence must be backed by a concrete adapter owned by
    the broker-evidence authority and must hold an authority-side read lease
    through the outcome INSERT.  A caller-supplied callback facade cannot
    establish that trust.  No such production adapter is installed yet, so the
    marker is deliberately non-constructible and production stays diagnostic.
    Explicit ``test_only`` stores remain available behind
    ``allow_test_fixture`` and can never contribute evaluation weight.
    """

    test_only = False

    def __init__(self, *_: object, **__: object) -> None:
        raise OutcomeValidationError(
            "trusted production broker outcome evidence adapter is unavailable"
        )


class OutcomeLedgerCorruption(OutcomeError):
    """The append-only chain or canonical row content no longer verifies."""


class IndependenceSpecValidationError(OutcomeError, ValueError):
    """An independence specification is unsigned, malformed, or mismatched."""


class IndependenceSpecUnavailable(OutcomeError):
    """Evaluation was requested without a verified independence authority."""


class MixedIndependenceSpecError(OutcomeError):
    """One report attempted to combine outcomes from different spec heads."""


@dataclass(frozen=True, slots=True)
class VerifiedIndependenceSpec:
    """Canonical, prerequisite-bound clustering authority.

    ``test_only`` fixtures are deliberately explicit and use a ``test:`` actor;
    they cannot be mistaken for the absent production human decision.
    """

    schema: str
    version: str
    effective_at: datetime
    actor: str
    signed_at: datetime
    test_only: bool
    initial_policy_version: str
    initial_policy_hash: str
    execution_cost_version: str
    execution_cost_hash: str
    rules: Mapping[str, object]
    spec_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "effective_at", utc_datetime(self.effective_at, field="effective_at")
        )
        object.__setattr__(
            self, "signed_at", utc_datetime(self.signed_at, field="signed_at")
        )
        frozen = freeze_json(self.rules)
        if not isinstance(frozen, Mapping):
            raise IndependenceSpecValidationError("rules must be a mapping")
        object.__setattr__(self, "rules", frozen)

    @classmethod
    def for_test(
        cls,
        *,
        version: str,
        effective_at: datetime,
        initial_policy_version: str,
        initial_policy_hash: str,
        execution_cost_version: str,
        execution_cost_hash: str,
        rules: Mapping[str, object] | None = None,
    ) -> "VerifiedIndependenceSpec":
        """Create an unmistakably non-production fixture for deterministic tests."""

        merged = {**_DEFAULT_RULES, **dict(rules or {})}
        at = utc_datetime(effective_at, field="effective_at")
        body = {
            "schema": INDEPENDENCE_SPEC_SCHEMA,
            "version": version,
            "effective_at": datetime_text(at),
            "actor": "test:fixture",
            "signed_at": datetime_text(at),
            "test_only": True,
            "initial_policy_version": initial_policy_version,
            "initial_policy_hash": initial_policy_hash,
            "execution_cost_version": execution_cost_version,
            "execution_cost_hash": execution_cost_hash,
            "rules": merged,
        }
        return verify_independence_spec(
            {**body, "spec_hash": canonical_hash(body)},
            allow_test_fixture=True,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "effective_at": datetime_text(self.effective_at),
            "actor": self.actor,
            "signed_at": datetime_text(self.signed_at),
            "test_only": self.test_only,
            "initial_policy_version": self.initial_policy_version,
            "initial_policy_hash": self.initial_policy_hash,
            "execution_cost_version": self.execution_cost_version,
            "execution_cost_hash": self.execution_cost_hash,
            "rules": thaw_json(self.rules),
            "spec_hash": self.spec_hash,
        }


def verify_independence_spec(
    value: VerifiedIndependenceSpec | Mapping[str, object],
    *,
    allow_test_fixture: bool = False,
    as_of: datetime | None = None,
) -> VerifiedIndependenceSpec:
    """Verify a signed-like immutable spec and its frozen prerequisite hashes."""

    document = (
        value.as_dict()
        if isinstance(value, VerifiedIndependenceSpec)
        else _mapping(value, "spec")
    )
    required = {
        "schema",
        "version",
        "effective_at",
        "actor",
        "signed_at",
        "test_only",
        "initial_policy_version",
        "initial_policy_hash",
        "execution_cost_version",
        "execution_cost_hash",
        "rules",
        "spec_hash",
    }
    missing = sorted(required.difference(document))
    if missing:
        raise IndependenceSpecValidationError(
            f"independence spec is missing {missing[0]}"
        )
    unknown = sorted(set(document).difference(required))
    if unknown:
        raise IndependenceSpecValidationError(
            f"independence spec contains unknown field {unknown[0]}"
        )
    if document["schema"] != INDEPENDENCE_SPEC_SCHEMA:
        raise IndependenceSpecValidationError("independence spec schema mismatch")
    version = _version(document["version"], "version")
    effective_at = _timestamp(document["effective_at"], "effective_at")
    signed_at = _timestamp(document["signed_at"], "signed_at")
    if signed_at < effective_at:
        raise IndependenceSpecValidationError("signed_at cannot predate effective_at")
    actor = _nonblank(document["actor"], "actor")
    test_only = document["test_only"]
    if not isinstance(test_only, bool):
        raise IndependenceSpecValidationError("test_only must be boolean")
    if test_only:
        if not allow_test_fixture:
            raise IndependenceSpecValidationError(
                "test fixture requires explicit allow_test_fixture"
            )
        if not actor.startswith("test:"):
            raise IndependenceSpecValidationError(
                "test fixture actor must use the test: namespace"
            )
    else:
        if not actor.startswith("human:"):
            raise IndependenceSpecValidationError(
                "production independence spec requires an explicit human actor"
            )
        raise IndependenceSpecValidationError(
            "production independence authority is unavailable; "
            "canonical hash is not a signature"
        )

    initial_version = _version(
        document["initial_policy_version"], "initial_policy_version"
    )
    initial_hash = _digest(document["initial_policy_hash"], "initial_policy_hash")
    cost_version = _version(
        document["execution_cost_version"], "execution_cost_version"
    )
    cost_hash = _digest(document["execution_cost_hash"], "execution_cost_hash")
    if initial_version != INITIAL_POLICY_VERSION or initial_hash != INITIAL_POLICY_HASH:
        raise IndependenceSpecValidationError(
            "initial policy prerequisite identity mismatch"
        )
    if cost_version != EXECUTION_COST_VERSION or cost_hash != EXECUTION_COST_HASH:
        raise IndependenceSpecValidationError(
            "execution cost prerequisite identity mismatch"
        )
    rules = _validate_rules(document["rules"])
    normalized_body = {
        "schema": INDEPENDENCE_SPEC_SCHEMA,
        "version": version,
        "effective_at": datetime_text(effective_at),
        "actor": actor,
        "signed_at": datetime_text(signed_at),
        "test_only": test_only,
        "initial_policy_version": initial_version,
        "initial_policy_hash": initial_hash,
        "execution_cost_version": cost_version,
        "execution_cost_hash": cost_hash,
        "rules": rules,
    }
    expected_hash = canonical_hash(normalized_body)
    supplied_hash = _digest(document["spec_hash"], "spec_hash")
    if supplied_hash != expected_hash:
        raise IndependenceSpecValidationError("independence spec hash mismatch")
    checked_at = utc_datetime(as_of or datetime.now(timezone.utc), field="as_of")
    if not test_only and (effective_at > checked_at or signed_at > checked_at):
        raise IndependenceSpecValidationError(
            "independence spec is not yet effective and signed"
        )
    return VerifiedIndependenceSpec(
        schema=INDEPENDENCE_SPEC_SCHEMA,
        version=version,
        effective_at=effective_at,
        actor=actor,
        signed_at=signed_at,
        test_only=test_only,
        initial_policy_version=initial_version,
        initial_policy_hash=initial_hash,
        execution_cost_version=cost_version,
        execution_cost_hash=cost_hash,
        rules=rules,
        spec_hash=supplied_hash,
    )


@dataclass(frozen=True, slots=True)
class OutcomeClusterAssignment:
    cluster_id: str | None
    representative_decision_id: str | None
    count_weight: int
    diagnostic_count_weight: int
    exclusion_reason: str | None


@dataclass(frozen=True, slots=True)
class StoredOutcome:
    sequence: int
    outcome_id: str
    base_identity_hash: str
    version: int
    body: Mapping[str, object]
    observation_hash: str
    content_hash: str
    supersedes_outcome_id: str | None
    supersedes_hash: str | None
    previous_hash: str
    chain_hash: str
    recorded_at: datetime
    assignment: OutcomeClusterAssignment | None = None

    def __post_init__(self) -> None:
        frozen = freeze_json(self.body)
        if not isinstance(frozen, Mapping):
            raise TypeError("body must be a mapping")
        object.__setattr__(self, "body", frozen)
        object.__setattr__(self, "recorded_at", utc_datetime(self.recorded_at))

    def _value(self, name: str) -> object:
        return self.body.get(name)

    @property
    def decision_id(self) -> str:
        return str(self._value("decision_id"))

    @property
    def decision_hash(self) -> str:
        return str(self._value("decision_hash"))

    @property
    def candidate_id(self) -> str:
        return str(self._value("candidate_id"))

    @property
    def candidate_hash(self) -> str:
        return str(self._value("candidate_hash"))

    @property
    def ranking_snapshot_id(self) -> str:
        return str(self._value("ranking_snapshot_id"))

    @property
    def ranking_snapshot_hash(self) -> str:
        return str(self._value("ranking_snapshot_hash"))

    @property
    def horizon(self) -> str:
        return str(self._value("horizon"))

    @property
    def observed_at(self) -> datetime:
        return self.economic_observed_at

    @property
    def economic_observed_at(self) -> datetime:
        return _timestamp(
            self._value("economic_observed_at"),
            "economic_observed_at",
        )

    @property
    def revision_received_at(self) -> datetime:
        return _timestamp(
            self._value("revision_received_at"),
            "revision_received_at",
        )

    @property
    def independence_version(self) -> str | None:
        value = self._value("independence_version")
        return None if value is None else str(value)

    @property
    def independence_hash(self) -> str | None:
        value = self._value("independence_hash")
        return None if value is None else str(value)

    @property
    def cluster_id(self) -> str | None:
        return None if self.assignment is None else self.assignment.cluster_id

    @property
    def representative_decision_id(self) -> str | None:
        return (
            None
            if self.assignment is None
            else self.assignment.representative_decision_id
        )

    @property
    def count_weight(self) -> int:
        return 0 if self.assignment is None else self.assignment.count_weight

    @property
    def diagnostic_count_weight(self) -> int:
        return (
            0 if self.assignment is None else self.assignment.diagnostic_count_weight
        )

    @property
    def cluster_exclusion_reason(self) -> str | None:
        return None if self.assignment is None else self.assignment.exclusion_reason

    @property
    def mfe_usd(self) -> Decimal | None:
        return _stored_optional_decimal(self._value("mfe_usd"))

    @property
    def mae_usd(self) -> Decimal | None:
        return _stored_optional_decimal(self._value("mae_usd"))

    @property
    def mark_pnl_before_costs_usd(self) -> Decimal | None:
        return _stored_optional_decimal(self._value("mark_pnl_before_costs_usd"))

    @property
    def mark_pnl_after_costs_usd(self) -> Decimal | None:
        return _stored_optional_decimal(self._value("mark_pnl_after_costs_usd"))

    @property
    def executable_exit_pnl_before_costs_usd(self) -> Decimal | None:
        return _stored_optional_decimal(
            self._value("executable_exit_pnl_before_costs_usd")
        )

    @property
    def executable_exit_pnl_after_costs_usd(self) -> Decimal | None:
        return _stored_optional_decimal(
            self._value("executable_exit_pnl_after_costs_usd")
        )

    @property
    def total_cost_usd(self) -> Decimal | None:
        return _stored_optional_decimal(self._value("total_cost_usd"))

    @property
    def costs(self) -> Mapping[str, object]:
        value = self._value("costs")
        assert isinstance(value, Mapping)
        return value

    @property
    def exit_rule_hits(self) -> tuple[str, ...]:
        value = self._value("exit_rule_hits")
        assert isinstance(value, tuple)
        return tuple(str(item) for item in value)

    @property
    def quote_quality_status(self) -> str:
        return str(self._value("quote_quality_status"))

    @property
    def quote_age_at_observation_seconds(self) -> Decimal | None:
        return _stored_optional_decimal(
            self._value("quote_age_at_observation_seconds")
        )

    @property
    def pnl_classification(self) -> str:
        return str(self._value("pnl_classification"))

    @property
    def evaluation_eligible(self) -> bool:
        return self._value("evaluation_eligible") is True

    @property
    def diagnostic_eligible(self) -> bool:
        return self._value("diagnostic_eligible") is True

    @property
    def independence_test_only(self) -> bool:
        return self._value("independence_test_only") is True

    @property
    def candidate_cost_calculation_hash(self) -> str | None:
        value = self._value("candidate_cost_calculation_hash")
        return None if value is None else str(value)

    @property
    def exclusion_reasons(self) -> tuple[str, ...]:
        value = self._value("exclusion_reasons")
        assert isinstance(value, tuple)
        return tuple(str(item) for item in value)

    @property
    def cluster_evidence(self) -> Mapping[str, object]:
        value = self._value("cluster_evidence")
        assert isinstance(value, Mapping)
        return value

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "outcome_id": self.outcome_id,
            "base_identity_hash": self.base_identity_hash,
            "version": self.version,
            "body": thaw_json(self.body),
            "observation_hash": self.observation_hash,
            "content_hash": self.content_hash,
            "supersedes_outcome_id": self.supersedes_outcome_id,
            "supersedes_hash": self.supersedes_hash,
            "previous_hash": self.previous_hash,
            "chain_hash": self.chain_hash,
            "recorded_at": datetime_text(self.recorded_at),
            "assignment": (
                None
                if self.assignment is None
                else {
                    "cluster_id": self.assignment.cluster_id,
                    "representative_decision_id": (
                        self.assignment.representative_decision_id
                    ),
                    "count_weight": self.assignment.count_weight,
                    "diagnostic_count_weight": self.assignment.diagnostic_count_weight,
                    "exclusion_reason": self.assignment.exclusion_reason,
                }
            ),
        }


@dataclass(frozen=True, slots=True)
class OutcomeEvaluationInput:
    outcome_id: str
    outcome_hash: str
    decision_id: str
    cluster_id: str | None
    representative_decision_id: str | None
    count_weight: int
    diagnostic_count_weight: int
    independence_version: str
    independence_hash: str
    test_only: bool
    evaluation_eligible: bool
    diagnostic_eligible: bool
    exclusion_reasons: tuple[str, ...]
    body: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class OutcomeAggregate:
    independence_version: str
    independence_hash: str
    total_outcomes: int
    eligible_outcomes: int
    diagnostic_eligible_outcomes: int
    excluded_outcomes: int
    unknown_outcomes: int
    independent_count: int
    diagnostic_cluster_count: int
    test_only: bool
    production_eligible: bool
    clusters: tuple[Mapping[str, object], ...]
    outcome_hashes: tuple[str, ...]
    aggregate_hash: str

    @property
    def count_weight(self) -> int:
        return self.independent_count


class OutcomeRecorder:
    """SQLite/WAL append-only outcome recorder and spec-bound counter."""

    def __init__(
        self,
        path: str | Path,
        *,
        independence_spec: VerifiedIndependenceSpec
        | Mapping[str, object]
        | None = None,
        allow_test_fixture: bool = False,
        execution_cost_resolver: object | None = None,
        broker_outcome_evidence_store: object | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._allow_test_fixture = allow_test_fixture
        self.independence_spec = (
            None
            if independence_spec is None
            else verify_independence_spec(
                independence_spec,
                allow_test_fixture=allow_test_fixture,
            )
        )
        if execution_cost_resolver is None:
            self._execution_cost_resolver = SignedExecutionCostResolver()
        elif isinstance(execution_cost_resolver, SignedExecutionCostResolver):
            self._execution_cost_resolver = execution_cost_resolver
        elif allow_test_fixture:
            self._execution_cost_resolver = execution_cost_resolver
        else:
            raise OutcomeValidationError(
                "production execution_cost_resolver must be "
                "SignedExecutionCostResolver"
            )
        if broker_outcome_evidence_store is None:
            self._broker_outcome_evidence_store = None
        elif (
            allow_test_fixture
            and getattr(broker_outcome_evidence_store, "test_only", False) is True
            and callable(
                getattr(broker_outcome_evidence_store, "acquire_read_lease", None)
            )
        ):
            self._broker_outcome_evidence_store = broker_outcome_evidence_store
        else:
            raise OutcomeValidationError(
                "trusted production broker outcome evidence adapter is unavailable; "
                "TEST_ONLY stores require allow_test_fixture and an authority-side "
                "read lease"
            )
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path,
            isolation_level=None,
            check_same_thread=False,
            timeout=10,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA busy_timeout=10000")
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._migrate()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "OutcomeRecorder":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        value = int(self._connection.execute("PRAGMA synchronous").fetchone()[0])
        return {0: "off", 1: "normal", 2: "full", 3: "extra"}[value]

    @property
    def schema_version(self) -> int:
        self._ensure_open()
        return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def record(
        self,
        outcome: Mapping[str, object] | object | None = None,
        **fields_: object,
    ) -> StoredOutcome:
        """Append one observation or a strictly later superseding version."""

        document = _document(outcome) if outcome is not None else {}
        if fields_:
            overlap = set(document).intersection(fields_)
            if overlap:
                raise OutcomeValidationError(
                    f"duplicate outcome field {sorted(overlap)[0]}"
                )
            document = {**document, **fields_}
        recorded_at = utc_datetime(self._clock(), field="clock result")
        lease_context = (
            nullcontext(None)
            if self._broker_outcome_evidence_store is None
            else self._broker_outcome_evidence_store.acquire_read_lease()
        )
        with lease_context as leased_evidence_store:
            if (
                self._broker_outcome_evidence_store is not None
                and leased_evidence_store is not self._broker_outcome_evidence_store
            ):
                raise OutcomeValidationError(
                    "broker outcome evidence lease returned a different authority"
                )
            prepared = _prepare_observation(
                document,
                self.independence_spec,
                recorded_at=recorded_at,
                execution_cost_resolver=self._execution_cost_resolver,
                broker_outcome_evidence_store=leased_evidence_store,
            )
            return self._record_prepared(
                prepared,
                recorded_at=recorded_at,
                broker_outcome_evidence_store=leased_evidence_store,
            )

    def _record_prepared(
        self,
        prepared: Mapping[str, object],
        *,
        recorded_at: datetime,
        broker_outcome_evidence_store: object | None,
    ) -> StoredOutcome:
        """Insert while the broker authority read lease remains held."""

        base_identity_hash = canonical_hash(
            {
                "schema": "options_copilot.outcome_base_identity.v1",
                **{name: prepared[name] for name in _BASE_IDENTITY_FIELDS},
            }
        )
        observation_hash = canonical_hash(
            {
                "schema": "options_copilot.outcome_observation.v1",
                "base_identity_hash": base_identity_hash,
                "observation": prepared,
            }
        )
        with self._transaction():
            self.assert_integrity()
            prior_row = self._connection.execute(
                "SELECT * FROM outcome_records WHERE base_identity_hash=? "
                "ORDER BY version DESC LIMIT 1",
                (base_identity_hash,),
            ).fetchone()
            prior = None if prior_row is None else _stored_outcome(prior_row)
            if prior is not None:
                if prior.observation_hash == observation_hash:
                    projected = self._project_latest()
                    return next(
                        item
                        for item in projected
                        if item.outcome_id == prior.outcome_id
                    )
                changed = [
                    name
                    for name in _BOUND_FIELDS
                    if prior.body.get(name) != prepared.get(name)
                ]
                if changed:
                    raise OutcomeIdentityConflict(
                        "late outcome changed frozen binding " + changed[0]
                    )
                if (
                    _timestamp(
                        prepared["revision_received_at"],
                        "revision_received_at",
                    )
                    <= prior.revision_received_at
                ):
                    raise OutcomeIdentityConflict(
                        "changed outcome must have a strictly later "
                        "revision_received_at"
                    )

            version = 1 if prior is None else prior.version + 1
            outcome_id = f"outcome:{base_identity_hash}:v{version}"
            body = prepared
            supersedes_id = None if prior is None else prior.outcome_id
            supersedes_hash = None if prior is None else prior.content_hash
            content_payload = _content_payload(
                outcome_id=outcome_id,
                base_identity_hash=base_identity_hash,
                version=version,
                body=body,
                observation_hash=observation_hash,
                supersedes_outcome_id=supersedes_id,
                supersedes_hash=supersedes_hash,
                recorded_at=recorded_at,
            )
            content_hash = canonical_hash(content_payload)
            tail = self._connection.execute(
                "SELECT sequence, chain_hash FROM outcome_records "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            previous_hash = GENESIS_HASH if tail is None else str(tail["chain_hash"])
            chain_hash = canonical_hash(
                {
                    "schema": "options_copilot.outcome_chain.v1",
                    "sequence": sequence,
                    "previous_hash": previous_hash,
                    "content_hash": content_hash,
                }
            )
            currentness_reason = _broker_outcome_evidence_insert_guard_reason(
                prepared,
                store=broker_outcome_evidence_store,
            )
            if currentness_reason is not None:
                raise OutcomeValidationError(currentness_reason)
            self._connection.execute(
                """
                INSERT INTO outcome_records(
                    sequence, outcome_id, base_identity_hash, version,
                    decision_id, candidate_hash, ranking_snapshot_id, horizon,
                    independence_version, independence_hash, body_json,
                    observation_hash, content_hash, supersedes_outcome_id,
                    supersedes_hash, previous_hash, chain_hash, recorded_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    outcome_id,
                    base_identity_hash,
                    version,
                    body["decision_id"],
                    body["candidate_hash"],
                    body["ranking_snapshot_id"],
                    body["horizon"],
                    body["independence_version"],
                    body["independence_hash"],
                    canonical_json(body),
                    observation_hash,
                    content_hash,
                    supersedes_id,
                    supersedes_hash,
                    previous_hash,
                    chain_hash,
                    datetime_text(recorded_at),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM outcome_records WHERE sequence=?", (sequence,)
            ).fetchone()
            if row is None:
                raise OutcomeLedgerCorruption("inserted outcome is missing")
            projected = self._project_latest()
            return next(item for item in projected if item.outcome_id == outcome_id)

    append = record
    record_outcome = record

    def history(self, base_identity_hash: str) -> tuple[StoredOutcome, ...]:
        digest = _digest(base_identity_hash, "base_identity_hash")
        self._ensure_open()
        rows = self._connection.execute(
            "SELECT * FROM outcome_records WHERE base_identity_hash=? ORDER BY version",
            (digest,),
        ).fetchall()
        return tuple(_stored_outcome(row) for row in rows)

    def latest(self, base_identity_hash: str) -> StoredOutcome | None:
        digest = _digest(base_identity_hash, "base_identity_hash")
        self._ensure_open()
        return next(
            (
                item
                for item in self._project_latest()
                if item.base_identity_hash == digest
            ),
            None,
        )

    def query(self, *, latest_only: bool = False) -> tuple[StoredOutcome, ...]:
        self._ensure_open()
        rows = self._connection.execute(
            "SELECT * FROM outcome_records ORDER BY sequence"
        ).fetchall()
        values = tuple(_stored_outcome(row) for row in rows)
        if not latest_only:
            return values
        return self._project_latest(values)

    def count(self, *, latest_only: bool = False) -> int:
        return len(self.query(latest_only=latest_only))

    def row_count(self) -> int:
        """Return the durable row count without materializing the ledger."""

        self._ensure_open()
        row = self._connection.execute(
            "SELECT COUNT(*) FROM outcome_records"
        ).fetchone()
        return 0 if row is None else int(row[0])

    def head_hash(self) -> str:
        """Return the current chain head without scanning outcome bodies."""

        self._ensure_open()
        row = self._connection.execute(
            "SELECT chain_hash FROM outcome_records ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return GENESIS_HASH if row is None else str(row[0])

    def aggregate(
        self,
        *,
        independence_version: str | None = None,
        independence_hash: str | None = None,
    ) -> OutcomeAggregate:
        evaluations, clusters, identity = self._evaluation(
            independence_version=independence_version,
            independence_hash=independence_hash,
        )
        version, spec_hash = identity
        heads = [
            item
            for item in self.query(latest_only=True)
            if item.independence_version == version
            and item.independence_hash == spec_hash
        ]
        eligible = sum(item.evaluation_eligible for item in heads)
        diagnostic_eligible = sum(item.diagnostic_eligible for item in heads)
        unknown = sum(
            item.cluster_evidence.get("status") == "UNKNOWN" for item in heads
        )
        outcome_hashes = tuple(item.outcome_hash for item in evaluations)
        cluster_documents = tuple(clusters)
        test_only = bool(self.independence_spec and self.independence_spec.test_only)
        independent_count = sum(item.count_weight for item in evaluations)
        diagnostic_cluster_count = sum(
            item.diagnostic_count_weight for item in evaluations
        )
        body = {
            "schema": "options_copilot.outcome_aggregate.v1",
            "independence_version": version,
            "independence_hash": spec_hash,
            "total_outcomes": len(heads),
            "eligible_outcomes": eligible,
            "diagnostic_eligible_outcomes": diagnostic_eligible,
            "excluded_outcomes": len(heads) - eligible,
            "unknown_outcomes": unknown,
            "independent_count": independent_count,
            "diagnostic_cluster_count": diagnostic_cluster_count,
            "test_only": test_only,
            "production_eligible": not test_only,
            "clusters": cluster_documents,
            "outcome_hashes": outcome_hashes,
        }
        return OutcomeAggregate(
            independence_version=version,
            independence_hash=spec_hash,
            total_outcomes=len(heads),
            eligible_outcomes=eligible,
            diagnostic_eligible_outcomes=diagnostic_eligible,
            excluded_outcomes=len(heads) - eligible,
            unknown_outcomes=unknown,
            independent_count=independent_count,
            diagnostic_cluster_count=diagnostic_cluster_count,
            test_only=test_only,
            production_eligible=not test_only,
            clusters=cluster_documents,
            outcome_hashes=outcome_hashes,
            aggregate_hash=canonical_hash(body),
        )

    def evaluation_inputs(
        self,
        *,
        independence_version: str | None = None,
        independence_hash: str | None = None,
    ) -> tuple[OutcomeEvaluationInput, ...]:
        evaluations, _, _ = self._evaluation(
            independence_version=independence_version,
            independence_hash=independence_hash,
        )
        return evaluations

    def _evaluation(
        self,
        *,
        independence_version: str | None,
        independence_hash: str | None,
    ) -> tuple[
        tuple[OutcomeEvaluationInput, ...],
        tuple[Mapping[str, object], ...],
        tuple[str, str],
    ]:
        self.assert_integrity()
        heads = self.query(latest_only=True)
        identities = {
            (item.independence_version, item.independence_hash)
            for item in heads
            if item.independence_version is not None
            and item.independence_hash is not None
        }
        if independence_version is None and independence_hash is None:
            if not identities:
                if self.independence_spec is None:
                    raise IndependenceSpecUnavailable(
                        "no verified independence specification is bound"
                    )
                selected_version = self.independence_spec.version
                selected_hash = self.independence_spec.spec_hash
            if len(identities) > 1:
                raise MixedIndependenceSpecError(
                    "evaluation cannot mix independence spec versions"
                )
            if identities:
                selected_version, selected_hash = next(iter(identities))
        elif independence_version is None or independence_hash is None:
            raise OutcomeValidationError(
                "independence_version and independence_hash are required together"
            )
        else:
            selected_version = _version(independence_version, "independence_version")
            selected_hash = _digest(independence_hash, "independence_hash")
        spec = self.independence_spec
        if (
            spec is None
            or spec.version != selected_version
            or spec.spec_hash != selected_hash
        ):
            raise IndependenceSpecUnavailable(
                "the requested independence spec is not injected and verified"
            )
        selected = [
            item
            for item in heads
            if item.independence_version == selected_version
            and item.independence_hash == selected_hash
        ]
        evaluations: list[OutcomeEvaluationInput] = []
        for item in selected:
            evaluations.append(
                OutcomeEvaluationInput(
                    outcome_id=item.outcome_id,
                    outcome_hash=item.content_hash,
                    decision_id=item.decision_id,
                    cluster_id=item.cluster_id,
                    representative_decision_id=item.representative_decision_id,
                    count_weight=item.count_weight,
                    diagnostic_count_weight=item.diagnostic_count_weight,
                    independence_version=spec.version,
                    independence_hash=spec.spec_hash,
                    test_only=spec.test_only,
                    evaluation_eligible=item.evaluation_eligible,
                    diagnostic_eligible=item.diagnostic_eligible,
                    exclusion_reasons=item.exclusion_reasons,
                    body=item.body,
                )
            )
        clusters: list[Mapping[str, object]] = []
        cluster_ids = sorted(
            {
                item.cluster_id
                for item in selected
                if item.cluster_id is not None and item.diagnostic_eligible
            }
        )
        for cluster_id in cluster_ids:
            members = [item for item in selected if item.cluster_id == cluster_id]
            representative = next(
                item for item in members if item.diagnostic_count_weight == 1
            )
            clusters.append(
                freeze_json(
                    {
                        "cluster_id": cluster_id,
                        "representative_decision_id": representative.decision_id,
                        "count_weight": sum(
                            item.count_weight for item in members
                        ),
                        "diagnostic_count_weight": 1,
                        "test_only": spec.test_only,
                        "member_outcome_hashes": tuple(
                            sorted(item.content_hash for item in members)
                        ),
                    }
                )
            )
        return (
            tuple(evaluations),
            tuple(clusters),
            (spec.version, spec.spec_hash),
        )

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self._ensure_open()
        rows = self._connection.execute(
            "SELECT * FROM outcome_records ORDER BY sequence"
        ).fetchall()
        previous = GENESIS_HASH
        by_id: dict[str, StoredOutcome] = {}
        for expected_sequence, row in enumerate(rows, start=1):
            stored = _stored_outcome(row)
            if stored.sequence != expected_sequence:
                raise OutcomeLedgerCorruption("outcome sequence contains a gap")
            if datetime_text(stored.recorded_at) != str(row["recorded_at"]):
                raise OutcomeLedgerCorruption(
                    "outcome content hash recorded_at encoding is not canonical"
                )
            body_json = canonical_json(stored.body)
            if body_json != str(row["body_json"]):
                raise OutcomeLedgerCorruption("outcome body is not canonical")
            projections = {
                "decision_id": str(row["decision_id"]),
                "candidate_hash": str(row["candidate_hash"]),
                "ranking_snapshot_id": str(row["ranking_snapshot_id"]),
                "horizon": str(row["horizon"]),
                "independence_version": (
                    None
                    if row["independence_version"] is None
                    else str(row["independence_version"])
                ),
                "independence_hash": (
                    None
                    if row["independence_hash"] is None
                    else str(row["independence_hash"])
                ),
            }
            if any(
                stored.body.get(name) != value for name, value in projections.items()
            ):
                raise OutcomeLedgerCorruption("outcome column projection mismatch")
            base_hash = canonical_hash(
                {
                    "schema": "options_copilot.outcome_base_identity.v1",
                    **{name: stored.body[name] for name in _BASE_IDENTITY_FIELDS},
                }
            )
            if base_hash != stored.base_identity_hash:
                raise OutcomeLedgerCorruption("outcome base identity mismatch")
            if stored.outcome_id != f"outcome:{base_hash}:v{stored.version}":
                raise OutcomeLedgerCorruption("outcome version identity mismatch")
            expected_observation_hash = canonical_hash(
                {
                    "schema": "options_copilot.outcome_observation.v1",
                    "base_identity_hash": base_hash,
                    "observation": stored.body,
                }
            )
            if expected_observation_hash != stored.observation_hash:
                raise OutcomeLedgerCorruption("outcome observation hash mismatch")
            payload = _content_payload(
                outcome_id=stored.outcome_id,
                base_identity_hash=stored.base_identity_hash,
                version=stored.version,
                body=stored.body,
                observation_hash=stored.observation_hash,
                supersedes_outcome_id=stored.supersedes_outcome_id,
                supersedes_hash=stored.supersedes_hash,
                recorded_at=stored.recorded_at,
            )
            if canonical_hash(payload) != stored.content_hash:
                raise OutcomeLedgerCorruption("outcome content hash mismatch")
            if stored.previous_hash != previous:
                raise OutcomeLedgerCorruption("outcome hash chain is broken")
            expected_chain = canonical_hash(
                {
                    "schema": "options_copilot.outcome_chain.v1",
                    "sequence": stored.sequence,
                    "previous_hash": stored.previous_hash,
                    "content_hash": stored.content_hash,
                }
            )
            if expected_chain != stored.chain_hash:
                raise OutcomeLedgerCorruption("outcome chain hash mismatch")
            if stored.version == 1:
                if (
                    stored.supersedes_outcome_id is not None
                    or stored.supersedes_hash is not None
                ):
                    raise OutcomeLedgerCorruption(
                        "first outcome version supersedes a row"
                    )
            else:
                prior = by_id.get(str(stored.supersedes_outcome_id))
                if (
                    prior is None
                    or prior.base_identity_hash != stored.base_identity_hash
                    or prior.version + 1 != stored.version
                    or prior.content_hash != stored.supersedes_hash
                ):
                    raise OutcomeLedgerCorruption("outcome supersession is invalid")
            by_id[stored.outcome_id] = stored
            previous = stored.chain_hash
        if self._connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise OutcomeLedgerCorruption("SQLite integrity check failed")

    def _project_latest(
        self,
        values: Sequence[StoredOutcome] | None = None,
    ) -> tuple[StoredOutcome, ...]:
        """Project one deterministic cluster assignment over current heads.

        Assignments are deliberately derived instead of persisted in the immutable
        observation.  This makes out-of-order inserts and late corrections converge
        on the same connected-component truth without rewriting ledger history.
        """

        if values is None:
            rows = self._connection.execute(
                "SELECT * FROM outcome_records ORDER BY sequence"
            ).fetchall()
            values = tuple(_stored_outcome(row) for row in rows)
        heads: dict[str, StoredOutcome] = {}
        for item in values:
            heads[item.base_identity_hash] = item
        ordered = tuple(sorted(heads.values(), key=lambda item: item.sequence))
        spec = self.independence_spec
        assignments: dict[str, OutcomeClusterAssignment] = {}

        diagnostic_eligible: list[StoredOutcome] = []
        for item in ordered:
            if (
                spec is not None
                and item.independence_version == spec.version
                and item.independence_hash == spec.spec_hash
                and item.diagnostic_eligible
            ):
                diagnostic_eligible.append(item)
                continue
            evidence = item.cluster_evidence
            if evidence.get("status") == "UNKNOWN" and spec is not None:
                reason = str(spec.rules["unknown_exclusion_reason"])
            elif item.exclusion_reasons:
                reason = item.exclusion_reasons[0]
            else:
                reason = "INDEPENDENCE_SPEC_UNAVAILABLE"
            assignments[item.outcome_id] = OutcomeClusterAssignment(
                cluster_id=None,
                representative_decision_id=None,
                count_weight=0,
                diagnostic_count_weight=0,
                exclusion_reason=reason,
            )

        if spec is not None:
            production_representatives = {
                min(members, key=_representative_key).outcome_id
                for members in _connected_components(
                    tuple(
                        item
                        for item in diagnostic_eligible
                        if item.evaluation_eligible
                    ),
                    spec,
                )
            }
            for members in _connected_components(diagnostic_eligible, spec):
                ordered_members = sorted(members, key=_representative_key)
                representative = ordered_members[0]
                cluster_id = canonical_hash(
                    {
                        "schema": "options_copilot.independence_cluster_seed.v1",
                        "independence_version": spec.version,
                        "independence_hash": spec.spec_hash,
                        "representative_base_identity_hash": (
                            representative.base_identity_hash
                        ),
                    }
                )
                for item in ordered_members:
                    diagnostic_weight = int(
                        item.outcome_id == representative.outcome_id
                    )
                    assignments[item.outcome_id] = OutcomeClusterAssignment(
                        cluster_id=cluster_id,
                        representative_decision_id=representative.decision_id,
                        count_weight=int(
                            not spec.test_only
                            and item.evaluation_eligible
                            and item.outcome_id in production_representatives
                        ),
                        diagnostic_count_weight=diagnostic_weight,
                        exclusion_reason=(
                            None
                            if diagnostic_weight
                            else "CORRELATED_OR_DUPLICATE_CLUSTER"
                        ),
                    )

        return tuple(
            replace(item, assignment=assignments[item.outcome_id]) for item in ordered
        )

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError("outcome schema is newer than supported")
        with self._lock:
            self._connection.executescript(
                f"""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS outcome_records(
                    sequence INTEGER PRIMARY KEY,
                    outcome_id TEXT NOT NULL UNIQUE,
                    base_identity_hash TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version >= 1),
                    decision_id TEXT NOT NULL,
                    candidate_hash TEXT NOT NULL,
                    ranking_snapshot_id TEXT NOT NULL,
                    horizon TEXT NOT NULL,
                    independence_version TEXT,
                    independence_hash TEXT,
                    body_json TEXT NOT NULL,
                    observation_hash TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    supersedes_outcome_id TEXT,
                    supersedes_hash TEXT,
                    previous_hash TEXT NOT NULL,
                    chain_hash TEXT NOT NULL UNIQUE,
                    recorded_at TEXT NOT NULL,
                    UNIQUE(base_identity_hash, version),
                    FOREIGN KEY(supersedes_outcome_id) REFERENCES outcome_records(outcome_id)
                );
                CREATE INDEX IF NOT EXISTS outcome_identity_idx
                    ON outcome_records(base_identity_hash, version);
                CREATE INDEX IF NOT EXISTS outcome_independence_idx
                    ON outcome_records(independence_version, independence_hash, sequence);
                CREATE TRIGGER IF NOT EXISTS outcome_records_no_update
                BEFORE UPDATE ON outcome_records
                BEGIN SELECT RAISE(ABORT, 'immutable outcome ledger: update forbidden'); END;
                CREATE TRIGGER IF NOT EXISTS outcome_records_no_delete
                BEFORE DELETE ON outcome_records
                BEGIN SELECT RAISE(ABORT, 'immutable outcome ledger: delete forbidden'); END;
                PRAGMA user_version={SCHEMA_VERSION};
                COMMIT;
                """
            )

    class _Transaction:
        def __init__(self, recorder: "OutcomeRecorder") -> None:
            self.recorder = recorder

        def __enter__(self) -> None:
            self.recorder._ensure_open()
            self.recorder._lock.acquire()
            try:
                self.recorder._connection.execute("BEGIN IMMEDIATE")
            except BaseException:
                self.recorder._lock.release()
                raise

        def __exit__(self, exc_type: object, *_: object) -> None:
            try:
                self.recorder._connection.execute("ROLLBACK" if exc_type else "COMMIT")
            finally:
                self.recorder._lock.release()

    def _transaction(self) -> "OutcomeRecorder._Transaction":
        return self._Transaction(self)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("outcome recorder is closed")


def _resolve_broker_outcome_evidence(
    value: object,
    *,
    expected_broker_snapshot_hash: object,
    store: object | None,
) -> tuple[
    dict[str, object],
    Mapping[str, object],
    Mapping[str, object],
    tuple[str, ...],
]:
    empty_binding: dict[str, object] = {
        "broker_outcome_evidence_ref": freeze_json({}),
        "broker_outcome_evidence_hash": None,
        "broker_outcome_evidence_ledger_head_hash": None,
        "broker_outcome_evidence_authority_hash": None,
        "broker_outcome_evidence_test_only": False,
    }
    if not isinstance(value, Mapping):
        return (
            empty_binding,
            freeze_json({}),
            freeze_json({}),
            ("BROKER_OUTCOME_EVIDENCE_REFERENCE_MISSING",),
        )
    try:
        required = {
            "schema",
            "stream_id",
            "entry_id",
            "evidence_hash",
            "broker_snapshot_hash",
            "ledger_head_hash",
            "authority_hash",
            "reference_hash",
        }
        if set(value) != required:
            raise OutcomeValidationError("broker evidence reference fields are incomplete")
        if value.get("schema") != BROKER_OUTCOME_EVIDENCE_REF_SCHEMA:
            raise OutcomeValidationError("broker evidence reference schema mismatch")
        reference_hash = _digest(
            value.get("reference_hash"),
            "broker outcome evidence reference_hash",
        )
        if reference_hash != canonical_hash(
            {key: item for key, item in value.items() if key != "reference_hash"}
        ):
            raise OutcomeValidationError("broker evidence reference hash mismatch")
        stream_id = _nonblank(value.get("stream_id"), "broker evidence stream_id")
        entry_id = _nonblank(value.get("entry_id"), "broker evidence entry_id")
        evidence_hash = _digest(
            value.get("evidence_hash"), "broker outcome evidence_hash"
        )
        snapshot_hash = _digest(
            value.get("broker_snapshot_hash"),
            "broker outcome evidence broker_snapshot_hash",
        )
        ledger_head_hash = _digest(
            value.get("ledger_head_hash"),
            "broker outcome evidence ledger_head_hash",
        )
        authority_hash = _digest(
            value.get("authority_hash"),
            "broker outcome evidence authority_hash",
        )
        frozen_reference = freeze_json(value)
        binding = {
            "broker_outcome_evidence_ref": frozen_reference,
            "broker_outcome_evidence_hash": evidence_hash,
            "broker_outcome_evidence_ledger_head_hash": ledger_head_hash,
            "broker_outcome_evidence_authority_hash": authority_hash,
            "broker_outcome_evidence_test_only": bool(
                store is not None and getattr(store, "test_only", False) is True
            ),
        }
    except (KeyError, TypeError, ValueError, OutcomeValidationError):
        return (
            empty_binding,
            freeze_json({}),
            freeze_json({}),
            ("BROKER_OUTCOME_EVIDENCE_REFERENCE_INVALID",),
        )

    try:
        expected_snapshot = _digest(
            expected_broker_snapshot_hash,
            "broker_snapshot_hash",
        )
    except (TypeError, ValueError):
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_SNAPSHOT_MISMATCH",
        )
    if snapshot_hash != expected_snapshot:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_SNAPSHOT_MISMATCH",
        )
    if store is None:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_STORE_UNAVAILABLE",
        )
    resolve = getattr(store, "resolve", None)
    read_head = getattr(store, "read_head", None)
    if not callable(resolve) or not callable(read_head):
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_STORE_INVALID",
        )
    try:
        store_authority_hash = _digest(
            getattr(store, "authority_hash", None),
            "broker evidence store authority_hash",
        )
    except (TypeError, ValueError):
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_AUTHORITY_INVALID",
        )
    if store_authority_hash != authority_hash:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_AUTHORITY_MISMATCH",
        )
    try:
        head_before = _digest(
            read_head(stream_id),
            "broker evidence current ledger head",
        )
    except KeyError:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_REFERENCE_NOT_FOUND",
        )
    except Exception:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_STORE_READ_FAILED",
        )
    if head_before != ledger_head_hash:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_NOT_CURRENT",
        )
    try:
        raw_entry = resolve(entry_id)
    except KeyError:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_REFERENCE_NOT_FOUND",
        )
    except Exception:
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_STORE_READ_FAILED",
        )
    if not isinstance(raw_entry, Mapping):
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_ENTRY_INVALID",
        )
    try:
        frozen_entry = freeze_json(raw_entry)
        assert isinstance(frozen_entry, Mapping)
        required_entry = {
            "schema",
            "stream_id",
            "entry_id",
            "sequence",
            "previous_head_hash",
            "broker_snapshot_hash",
            "authority_hash",
            "valuation_evidence",
            "execution_evidence",
            "broker_cost_evidence",
            "costs",
            "evidence_hash",
            "ledger_head_hash",
        }
        if set(frozen_entry) != required_entry:
            raise OutcomeValidationError("broker evidence entry fields are incomplete")
        if frozen_entry.get("schema") != BROKER_OUTCOME_EVIDENCE_SCHEMA:
            raise OutcomeValidationError("broker evidence entry schema mismatch")
        sequence = _positive_int(
            frozen_entry.get("sequence"), "broker evidence sequence"
        )
        previous_head_hash = _digest(
            frozen_entry.get("previous_head_hash"),
            "broker evidence previous_head_hash",
        )
        entry_evidence_hash = _digest(
            frozen_entry.get("evidence_hash"),
            "broker evidence entry evidence_hash",
        )
        entry_head_hash = _digest(
            frozen_entry.get("ledger_head_hash"),
            "broker evidence entry ledger_head_hash",
        )
        entry_body = {
            key: item
            for key, item in frozen_entry.items()
            if key not in {"evidence_hash", "ledger_head_hash"}
        }
        if entry_evidence_hash != canonical_hash(entry_body):
            raise OutcomeValidationError("broker evidence entry hash mismatch")
        expected_head_hash = canonical_hash(
            {
                "schema": BROKER_OUTCOME_EVIDENCE_HEAD_SCHEMA,
                "stream_id": stream_id,
                "sequence": sequence,
                "previous_head_hash": previous_head_hash,
                "evidence_hash": entry_evidence_hash,
            }
        )
        if entry_head_hash != expected_head_hash:
            raise OutcomeValidationError("broker evidence ledger head mismatch")
        if (
            frozen_entry.get("stream_id") != stream_id
            or frozen_entry.get("entry_id") != entry_id
            or entry_evidence_hash != evidence_hash
            or entry_head_hash != ledger_head_hash
            or _digest(
                frozen_entry.get("authority_hash"),
                "broker evidence entry authority_hash",
            )
            != authority_hash
        ):
            return binding, freeze_json({}), freeze_json({}), (
                "BROKER_OUTCOME_EVIDENCE_REFERENCE_MISMATCH",
            )
        if _digest(
            frozen_entry.get("broker_snapshot_hash"),
            "broker evidence entry broker_snapshot_hash",
        ) != snapshot_hash:
            return binding, freeze_json({}), freeze_json({}), (
                "BROKER_OUTCOME_EVIDENCE_SNAPSHOT_MISMATCH",
            )
        head_after = _digest(
            read_head(stream_id),
            "broker evidence current ledger head",
        )
        if head_after != ledger_head_hash:
            return binding, freeze_json({}), freeze_json({}), (
                "BROKER_OUTCOME_EVIDENCE_NOT_CURRENT",
            )
        context = freeze_json(
            {
                "valuation_evidence": frozen_entry["valuation_evidence"],
                "execution_evidence": frozen_entry["execution_evidence"],
                "broker_cost_evidence": frozen_entry["broker_cost_evidence"],
                "costs": frozen_entry["costs"],
            }
        )
        currentness = freeze_json(
            {
                "stream_id": stream_id,
                "ledger_head_hash": ledger_head_hash,
            }
        )
        assert isinstance(context, Mapping)
        assert isinstance(currentness, Mapping)
        return binding, context, currentness, ()
    except (KeyError, TypeError, ValueError, OutcomeValidationError):
        return binding, freeze_json({}), freeze_json({}), (
            "BROKER_OUTCOME_EVIDENCE_ENTRY_INVALID",
        )


def _broker_outcome_evidence_currentness_reason(
    currentness: Mapping[str, object],
    *,
    store: object | None,
) -> str | None:
    if not currentness or store is None:
        return None
    read_head = getattr(store, "read_head", None)
    if not callable(read_head):
        return "BROKER_OUTCOME_EVIDENCE_STORE_INVALID"
    try:
        actual = _digest(
            read_head(str(currentness["stream_id"])),
            "broker evidence final ledger head",
        )
        expected = _digest(
            currentness["ledger_head_hash"],
            "broker evidence resolved ledger head",
        )
    except Exception:
        return "BROKER_OUTCOME_EVIDENCE_STORE_READ_FAILED"
    if actual != expected:
        return "BROKER_OUTCOME_EVIDENCE_NOT_CURRENT"
    return None


def _broker_outcome_evidence_insert_guard_reason(
    prepared: Mapping[str, object],
    *,
    store: object | None,
) -> str | None:
    """Recheck the authority head immediately before INSERT under its lease."""

    if prepared.get("diagnostic_eligible") is not True:
        return None
    reference = prepared.get("broker_outcome_evidence_ref")
    if not isinstance(reference, Mapping) or not reference:
        return "BROKER_OUTCOME_EVIDENCE_STORE_UNAVAILABLE"
    return _broker_outcome_evidence_currentness_reason(
        freeze_json(
            {
                "stream_id": reference.get("stream_id"),
                "ledger_head_hash": reference.get("ledger_head_hash"),
            }
        ),
        store=store,
    )


def _prepare_observation(
    raw: Mapping[str, object],
    spec: VerifiedIndependenceSpec | None,
    *,
    recorded_at: datetime,
    execution_cost_resolver: object | None,
    broker_outcome_evidence_store: object | None,
) -> dict[str, object]:
    document = dict(raw)
    recorded_at = utc_datetime(recorded_at, field="recorded_at")
    decision_at = _timestamp(document.get("decision_at"), "decision_at")
    horizon_at = _timestamp(document.get("horizon_at"), "horizon_at")
    economic_observed_at = _timestamp(
        document.get("economic_observed_at"),
        "economic_observed_at",
    )
    revision_received_at = _timestamp(
        document.get("revision_received_at"),
        "revision_received_at",
    )
    if horizon_at < decision_at:
        raise OutcomeValidationError("horizon_at cannot predate decision_at")
    economic_delay = _timedelta_seconds(economic_observed_at - horizon_at)
    if economic_delay < 0:
        raise OutcomeValidationError(
            "economic_observed_at cannot predate horizon_at"
        )
    if economic_delay > _MAXIMUM_ECONOMIC_OBSERVATION_DELAY_SECONDS:
        raise OutcomeValidationError(
            "economic observation window exceeds the frozen 5 second tolerance"
        )
    if revision_received_at < economic_observed_at:
        raise OutcomeValidationError(
            "revision_received_at cannot predate economic_observed_at"
        )
    if revision_received_at > recorded_at:
        raise OutcomeValidationError(
            "revision_received_at cannot be after recorded_at"
        )

    cost_version = _optional_version(document.get("cost_version"), "cost_version")
    cost_hash = _optional_digest(document.get("cost_hash"), "cost_hash")
    supplied_independence_version = document.get("independence_version")
    supplied_independence_hash = document.get("independence_hash")
    if spec is None:
        if (
            supplied_independence_version is not None
            or supplied_independence_hash is not None
        ):
            raise IndependenceSpecValidationError(
                "outcome cannot self-assert an uninjected independence spec"
            )
        independence_version = None
        independence_hash = None
    else:
        if supplied_independence_version not in (None, spec.version):
            raise IndependenceSpecValidationError(
                "outcome independence version disagrees with injected spec"
            )
        if supplied_independence_hash not in (None, spec.spec_hash):
            raise IndependenceSpecValidationError(
                "outcome independence hash disagrees with injected spec"
            )
        independence_version = spec.version
        independence_hash = spec.spec_hash

    caller_evidence_reasons: list[str] = []
    if any(
        name in document
        for name in (
            "valuation_evidence",
            "execution_evidence",
            "broker_cost_evidence",
            "costs",
        )
    ):
        caller_evidence_reasons.append(
            "CALLER_BROKER_OUTCOME_EVIDENCE_FORBIDDEN"
        )
    (
        broker_outcome_binding,
        broker_outcome_context,
        broker_outcome_currentness,
        broker_outcome_reasons,
    ) = _resolve_broker_outcome_evidence(
        document.get("broker_outcome_evidence_ref"),
        expected_broker_snapshot_hash=document.get("broker_snapshot_hash"),
        store=broker_outcome_evidence_store,
    )
    costs, cost_reasons = _normalize_costs(
        broker_outcome_context.get("costs")
    )
    total_cost = (
        None
        if any(value is None for value in costs.values())
        else sum((value for value in costs.values() if value is not None), Decimal("0"))
    )
    candidate_id = _nonblank(document.get("candidate_id"), "candidate_id")
    candidate_hash = _digest(document.get("candidate_hash"), "candidate_hash")
    ranking_snapshot_id = _nonblank(
        document.get("ranking_snapshot_id"), "ranking_snapshot_id"
    )
    outcome_subject_id_raw = document.get("outcome_subject_id")
    outcome_subject_hash_raw = document.get("outcome_subject_hash")
    if (outcome_subject_id_raw is None) is not (outcome_subject_hash_raw is None):
        raise OutcomeValidationError("OUTCOME_SUBJECT_BINDING_INCOMPLETE")
    outcome_subject_id = (
        candidate_id
        if outcome_subject_id_raw is None
        else _nonblank(outcome_subject_id_raw, "outcome_subject_id")
    )
    outcome_subject_hash = (
        candidate_hash
        if outcome_subject_hash_raw is None
        else _digest(outcome_subject_hash_raw, "outcome_subject_hash")
    )
    horizon = _outcome_horizon(document.get("horizon"))
    market_outcome_raw = document.get("market_outcome")
    market_outcome = (
        None
        if market_outcome_raw is None
        else normalize_outcome_observation(
            market_outcome_raw,
            subject_kind="CANDIDATE",
            subject_id=outcome_subject_id,
            subject_hash=outcome_subject_hash,
            symbol=_candidate_symbol(document),
            horizon=horizon,
            occurred_at=decision_at,
            as_of=recorded_at,
        )
    )
    market_ledger_binding = (
        None
        if market_outcome is None
        else market_outcome.get("ledger_binding")
    )
    if market_ledger_binding is not None and not isinstance(
        market_ledger_binding, Mapping
    ):
        raise OutcomeValidationError("OUTCOME_LEDGER_BINDING_INVALID")
    valuation, valuation_context, valuation_reasons = _normalize_valuation_evidence(
        broker_outcome_context.get("valuation_evidence"),
        broker_snapshot_hash=document.get("broker_snapshot_hash"),
        decision_at=decision_at,
        economic_observed_at=economic_observed_at,
    )
    cost_binding, cost_binding_reasons = _normalize_execution_cost_binding(
        document,
        candidate_id=candidate_id,
        cost_version=cost_version,
        cost_hash=cost_hash,
        costs=costs,
        decision_at=decision_at,
        execution_cost_resolver=execution_cost_resolver,
        valuation_context=valuation_context,
        execution_evidence=broker_outcome_context.get("execution_evidence"),
    )
    broker_cost_binding, broker_cost_reasons = _normalize_broker_cost_evidence(
        broker_outcome_context.get("broker_cost_evidence"),
        costs=costs,
        broker_snapshot_hash=document.get("broker_snapshot_hash"),
    )
    mfe = valuation["mfe_usd"]
    mae = valuation["mae_usd"]
    mark_before = valuation["mark_pnl_before_costs_usd"]
    executable_before = valuation["executable_exit_pnl_before_costs_usd"]
    pnl_reasons: list[str] = []
    for name, derived in (
        ("mfe_usd", mfe),
        ("mae_usd", mae),
        ("mark_pnl_before_costs_usd", mark_before),
        ("executable_exit_pnl_before_costs_usd", executable_before),
    ):
        claimed = _optional_decimal(document.get(name), name)
        if derived is not None and claimed != derived:
            pnl_reasons.append("PNL_RECOMPUTATION_MISMATCH")
    mark_after = (
        None if total_cost is None or mark_before is None else mark_before - total_cost
    )
    executable_after = (
        None
        if total_cost is None or executable_before is None
        else executable_before - total_cost
    )
    quote_quality, quote_status = _normalize_quote_quality(
        document.get("quote_quality")
    )
    cluster_evidence = _normalize_cluster_evidence(
        document.get("cluster_evidence"), decision_at=decision_at
    )
    reasons = [
        *caller_evidence_reasons,
        *broker_outcome_reasons,
        *cost_reasons,
        *cost_binding_reasons,
        *valuation_reasons,
        *broker_cost_reasons,
        *pnl_reasons,
    ]
    currentness_reason = _broker_outcome_evidence_currentness_reason(
        broker_outcome_currentness,
        store=broker_outcome_evidence_store,
    )
    if currentness_reason is not None:
        reasons.append(currentness_reason)
    if cost_version is None or cost_hash is None:
        reasons.append("COST_CONTRACT_MISSING")
    elif (
        cost_version != EXECUTION_COST_VERSION
        or cost_hash != EXECUTION_COST_HASH
        or (
            spec is not None
            and (
                cost_version != spec.execution_cost_version
                or cost_hash != spec.execution_cost_hash
            )
        )
    ):
        reasons.append("COST_CONTRACT_MISMATCH")
    if quote_status != "EXECUTABLE" or executable_before is None:
        reasons.append("EXECUTABLE_EXIT_QUOTE_MISSING")
    current_policy_version = _version(
        document.get("current_policy_version"), "current_policy_version"
    )
    current_policy_hash = _digest(
        document.get("current_policy_hash"), "current_policy_hash"
    )
    if spec is not None and (
        current_policy_version != spec.initial_policy_version
        or current_policy_hash != spec.initial_policy_hash
    ):
        reasons.append("POLICY_IDENTITY_MISMATCH")
    exit_policy_raw = document.get("exit_policy_hash")
    if exit_policy_raw is None:
        exit_policy_hash = None
        reasons.append("EXIT_POLICY_HASH_MISSING")
    else:
        exit_policy_hash = _digest(exit_policy_raw, "exit_policy_hash")
    position_management_hash = _optional_digest(
        document.get("position_management_hash"), "position_management_hash"
    )
    counterfactual_spec_hash = _optional_digest(
        document.get("counterfactual_spec_hash"), "counterfactual_spec_hash"
    )
    result_authority = _normalize_result_authority(
        document.get("outcome_result_authority"),
        candidate_hash=candidate_hash,
        cost_contract_hash=cost_hash,
    )
    position_management_result, position_management_result_hash = (
        normalize_bound_outcome_result(
            document.get("position_management_result"),
            schema="options_copilot.position_management_outcome.v1",
            binding_field="position_management_hash",
            binding_hash=position_management_hash,
            candidate_hash=candidate_hash,
            authority=result_authority,
            ledger_binding=market_ledger_binding,
        )
    )
    counterfactual_result, counterfactual_result_hash = (
        normalize_bound_outcome_result(
            document.get("counterfactual_result"),
            schema="options_copilot.outcome_counterfactual_result.v1",
            binding_field="counterfactual_spec_hash",
            binding_hash=counterfactual_spec_hash,
            candidate_hash=candidate_hash,
            authority=result_authority,
            ledger_binding=market_ledger_binding,
        )
    )
    if (
        position_management_result is not None
        and position_management_result.get("status") != "AVAILABLE"
    ):
        reasons.append("POSITION_MANAGEMENT_RESULT_UNAVAILABLE")
    if (
        counterfactual_result is not None
        and counterfactual_result.get("status") != "AVAILABLE"
    ):
        reasons.append("COUNTERFACTUAL_RESULT_UNAVAILABLE")
    if spec is None:
        reasons.append("INDEPENDENCE_SPEC_UNAVAILABLE")
    elif cluster_evidence["status"] == "UNKNOWN":
        reasons.append(str(spec.rules["unknown_exclusion_reason"]))
    diagnostic_eligible = not reasons
    broker_evidence_test_only = bool(
        broker_outcome_binding.get("broker_outcome_evidence_test_only")
    )
    if broker_evidence_test_only:
        reasons.append("BROKER_OUTCOME_EVIDENCE_TEST_ONLY")
    if spec is not None and spec.test_only:
        reasons.append("INDEPENDENCE_SPEC_TEST_ONLY")
    reasons = list(dict.fromkeys(reasons))

    exit_hits_raw = document.get("exit_rule_hits", ())
    if not isinstance(exit_hits_raw, Sequence) or isinstance(
        exit_hits_raw, (str, bytes, bytearray, memoryview)
    ):
        raise OutcomeValidationError("exit_rule_hits must be a sequence")
    exit_hits = tuple(
        _nonblank(value, "exit_rule_hit").upper() for value in exit_hits_raw
    )
    if len(exit_hits) != len(set(exit_hits)):
        raise OutcomeValidationError("exit_rule_hits cannot contain duplicates")

    classification = _pnl_classification(executable_before, executable_after)
    normalized = {
        "schema": "options_copilot.outcome.v1",
        "decision_id": _nonblank(document.get("decision_id"), "decision_id"),
        "decision_hash": _digest(document.get("decision_hash"), "decision_hash"),
        "candidate_id": candidate_id,
        "candidate_hash": candidate_hash,
        "ranking_snapshot_id": ranking_snapshot_id,
        "ranking_snapshot_hash": _digest(
            document.get("ranking_snapshot_hash"), "ranking_snapshot_hash"
        ),
        "ranking_basis_hash": _digest(
            document.get("ranking_basis_hash"), "ranking_basis_hash"
        ),
        "horizon": horizon,
        "horizon_at": datetime_text(horizon_at),
        "decision_at": datetime_text(decision_at),
        "economic_observed_at": datetime_text(economic_observed_at),
        "revision_received_at": datetime_text(revision_received_at),
        "economic_observation_delay_seconds": economic_delay,
        "quote_observed_at": valuation["quote_observed_at"],
        "quote_age_at_observation_seconds": valuation[
            "quote_age_at_observation_seconds"
        ],
        "input_hash": _digest(document.get("input_hash"), "input_hash"),
        "evidence_hash": _digest(document.get("evidence_hash"), "evidence_hash"),
        "broker_snapshot_hash": _digest(
            document.get("broker_snapshot_hash"), "broker_snapshot_hash"
        ),
        "current_policy_version": current_policy_version,
        "current_policy_hash": current_policy_hash,
        "policy_authority_marker_hash": _digest(
            document.get("policy_authority_marker_hash"),
            "policy_authority_marker_hash",
        ),
        "cost_version": cost_version,
        "cost_hash": cost_hash,
        **broker_outcome_binding,
        **cost_binding,
        **broker_cost_binding,
        "valuation_hash": valuation["valuation_hash"],
        "valuation_contracts_hash": valuation["valuation_contracts_hash"],
        "exit_policy_hash": exit_policy_hash,
        "thesis_hash": _optional_digest(document.get("thesis_hash"), "thesis_hash"),
        "quote_identity_hash": _optional_digest(
            document.get("quote_identity_hash"), "quote_identity_hash"
        ),
        "position_management_hash": position_management_hash,
        "counterfactual_spec_hash": counterfactual_spec_hash,
        "outcome_result_authority": result_authority,
        "outcome_result_authority_hash": (
            None if result_authority is None else result_authority["authority_hash"]
        ),
        "position_management_result": position_management_result,
        "position_management_result_hash": position_management_result_hash,
        "counterfactual_result": counterfactual_result,
        "counterfactual_result_hash": counterfactual_result_hash,
        "independence_version": independence_version,
        "independence_hash": independence_hash,
        "mfe_usd": mfe,
        "mae_usd": mae,
        "mark_pnl_before_costs_usd": mark_before,
        "mark_pnl_after_costs_usd": mark_after,
        "executable_exit_pnl_before_costs_usd": executable_before,
        "executable_exit_pnl_after_costs_usd": executable_after,
        "costs": costs,
        "cost_components_hash": canonical_hash(costs),
        "total_cost_usd": total_cost,
        "pnl_classification": classification,
        "exit_rule_hits": exit_hits,
        "quote_quality": quote_quality,
        "quote_quality_status": quote_status,
        "cluster_evidence": cluster_evidence,
        "independence_test_only": bool(spec and spec.test_only),
        "diagnostic_eligible": diagnostic_eligible,
        "evaluation_eligible": diagnostic_eligible
        and spec is not None
        and not spec.test_only
        and not broker_evidence_test_only,
        "exclusion_reasons": tuple(reasons),
    }
    if market_outcome is not None:
        normalized["outcome_subject_id"] = outcome_subject_id
        normalized["outcome_subject_hash"] = outcome_subject_hash
        normalized["market_outcome"] = market_outcome
    return normalized


def normalize_outcome_observation(
    value: object,
    *,
    subject_kind: str,
    subject_id: str,
    subject_hash: str,
    symbol: str,
    horizon: str,
    occurred_at: datetime,
    as_of: datetime,
    thesis_hash: str | None = None,
) -> Mapping[str, object]:
    """Validate one provenance-complete, authority-free horizon observation."""

    if not isinstance(value, Mapping):
        raise OutcomeValidationError("OUTCOME_OBSERVATION_INVALID")
    required = {
        "schema",
        "subject_kind",
        "subject_id",
        "subject_hash",
        "horizon",
        "target_rule",
        "horizon_at",
        "economic_observed_at",
        "revision_received_at",
        "horizon_evidence",
        "ledger_binding",
        "underlying",
        "benchmark",
        "option_market",
        "combination",
        "thesis_validity",
    }
    optional = {"position_management_result", "counterfactual_result"}
    supplied = set(value)
    if not required.issubset(supplied) or supplied - required - optional:
        raise OutcomeValidationError("OUTCOME_OBSERVATION_FIELD_UNKNOWN")
    checked_kind = _nonblank(subject_kind, "subject_kind").upper()
    if checked_kind not in {"PREDICTION", "CANDIDATE"}:
        raise OutcomeValidationError("OUTCOME_SUBJECT_KIND_INVALID")
    if checked_kind != "CANDIDATE" and supplied & optional:
        raise OutcomeValidationError("OUTCOME_OBSERVATION_FIELD_UNKNOWN")
    checked_id = _nonblank(subject_id, "subject_id")
    checked_hash = _digest(subject_hash, "subject_hash")
    checked_symbol = _nonblank(symbol, "symbol").upper()
    checked_horizon = _outcome_horizon(horizon)
    checked_occurred_at = utc_datetime(occurred_at, field="occurred_at")
    checked_as_of = utc_datetime(as_of, field="as_of")
    if (
        value.get("schema") != "options_copilot.outcome_observation.v2"
        or str(value.get("subject_kind", "")).upper() != checked_kind
        or value.get("subject_id") != checked_id
        or value.get("subject_hash") != checked_hash
        or value.get("horizon") != checked_horizon
        or value.get("target_rule") != OUTCOME_TARGET_RULES[checked_horizon]
    ):
        raise OutcomeValidationError("OUTCOME_OBSERVATION_IDENTITY_CONFLICT")

    revision_received_at = _timestamp(
        value.get("revision_received_at"),
        "revision_received_at",
    )
    if revision_received_at > checked_as_of:
        raise OutcomeValidationError("OUTCOME_OBSERVATION_TIME_TRAVEL")
    horizon_at, horizon_evidence = resolve_outcome_horizon(
        checked_horizon,
        occurred_at=checked_occurred_at,
        evidence=value.get("horizon_evidence"),
        as_of=revision_received_at,
    )
    supplied_horizon_at = _timestamp(value.get("horizon_at"), "horizon_at")
    if supplied_horizon_at != horizon_at:
        raise OutcomeValidationError("OUTCOME_HORIZON_BINDING_MISMATCH")
    economic_observed_at = _timestamp(
        value.get("economic_observed_at"),
        "economic_observed_at",
    )
    economic_delay = _timedelta_seconds(economic_observed_at - horizon_at)
    if economic_delay < 0:
        raise OutcomeValidationError("OUTCOME_OBSERVATION_TIME_TRAVEL")
    if economic_delay > _MAXIMUM_ECONOMIC_OBSERVATION_DELAY_SECONDS:
        raise OutcomeValidationError("OUTCOME_OBSERVATION_STALE")
    if revision_received_at < economic_observed_at:
        raise OutcomeValidationError("OUTCOME_OBSERVATION_TIME_TRAVEL")

    ledger_binding = _normalize_outcome_ledger_binding(
        value.get("ledger_binding"),
        as_of=checked_as_of,
    )
    underlying = _normalize_outcome_price_component(
        value.get("underlying"),
        field="underlying",
        expected_symbol=checked_symbol,
        horizon_at=horizon_at,
        economic_observed_at=economic_observed_at,
        as_of=checked_as_of,
    )
    benchmark = _normalize_outcome_price_component(
        value.get("benchmark"),
        field="benchmark",
        expected_symbol=None,
        horizon_at=horizon_at,
        economic_observed_at=economic_observed_at,
        as_of=checked_as_of,
    )
    option_market = _normalize_option_market_outcome(
        value.get("option_market"),
        horizon_at=horizon_at,
        economic_observed_at=economic_observed_at,
        as_of=checked_as_of,
    )
    combination = _normalize_combination_outcome(
        value.get("combination"),
        horizon_at=horizon_at,
        economic_observed_at=economic_observed_at,
        as_of=checked_as_of,
    )
    thesis = _normalize_thesis_validity(
        value.get("thesis_validity"),
        subject_kind=checked_kind,
        expected_thesis_hash=thesis_hash,
        horizon_at=horizon_at,
        economic_observed_at=economic_observed_at,
        as_of=checked_as_of,
    )
    underlying_return = underlying["return"]
    benchmark_return = benchmark["return"]
    assert isinstance(underlying_return, Decimal)
    assert isinstance(benchmark_return, Decimal)
    normalized = {
        "schema": "options_copilot.outcome_observation.v2",
        "subject_kind": checked_kind,
        "subject_id": checked_id,
        "subject_hash": checked_hash,
        "symbol": checked_symbol,
        "horizon": checked_horizon,
        "target_rule": OUTCOME_TARGET_RULES[checked_horizon],
        "horizon_at": datetime_text(horizon_at),
        "economic_observed_at": datetime_text(economic_observed_at),
        "revision_received_at": datetime_text(revision_received_at),
        "horizon_evidence": horizon_evidence,
        "ledger_binding": ledger_binding,
        "underlying": underlying,
        "underlying_return": underlying_return,
        "benchmark": benchmark,
        "benchmark_return": benchmark_return,
        "excess_return": underlying_return - benchmark_return,
        "option_market": option_market,
        "combination": combination,
        "thesis_validity": thesis,
        "calibration_eligible": (
            checked_kind == "PREDICTION"
            and thesis["status"] in {"VALID", "INVALID"}
        ),
        "decision_authority": "SUPPORTING_ONLY",
        "affects_production_weights": False,
        "affects_eligibility": False,
        "affects_risk": False,
        "affects_ranking": False,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    normalized["observation_hash"] = canonical_hash(normalized)
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def resolve_outcome_horizon(
    horizon: str,
    *,
    occurred_at: datetime,
    evidence: object,
    as_of: datetime,
) -> tuple[datetime, Mapping[str, object]]:
    """Resolve a horizon from immutable elapsed-time or session evidence."""

    checked_horizon = _outcome_horizon(horizon)
    checked_occurred_at = utc_datetime(occurred_at, field="occurred_at")
    checked_as_of = utc_datetime(as_of, field="as_of")
    if not isinstance(evidence, Mapping):
        raise OutcomeValidationError("OUTCOME_HORIZON_EVIDENCE_MISSING")
    allowed = {"schema", "target_rule", "method", "sessions"}
    if set(evidence) != allowed:
        raise OutcomeValidationError("OUTCOME_HORIZON_EVIDENCE_INVALID")
    expected_method = "ELAPSED_TIME" if checked_horizon == "30M" else "SESSION_CALENDAR"
    if (
        evidence.get("schema") != "options_copilot.outcome_horizon_evidence.v1"
        or evidence.get("target_rule") != OUTCOME_TARGET_RULES[checked_horizon]
        or evidence.get("method") != expected_method
    ):
        raise OutcomeValidationError("OUTCOME_HORIZON_EVIDENCE_INVALID")
    sessions_raw = evidence.get("sessions")
    if not isinstance(sessions_raw, Sequence) or isinstance(
        sessions_raw,
        (str, bytes, bytearray, memoryview),
    ):
        raise OutcomeValidationError("OUTCOME_HORIZON_EVIDENCE_INVALID")
    if checked_horizon == "30M":
        if sessions_raw:
            raise OutcomeValidationError("OUTCOME_HORIZON_EVIDENCE_INVALID")
        horizon_at = checked_occurred_at + timedelta(minutes=30)
        normalized_sessions: tuple[Mapping[str, object], ...] = ()
    else:
        normalized_sessions = tuple(
            _normalize_outcome_session(item, as_of=checked_as_of)
            for item in sessions_raw
        )
        if not normalized_sessions:
            raise OutcomeValidationError("OUTCOME_HORIZON_EVIDENCE_MISSING")
        closes = tuple(
            _timestamp(item["close_at"], "close_at") for item in normalized_sessions
        )
        if tuple(sorted(closes)) != closes or len(set(closes)) != len(closes):
            raise OutcomeValidationError("OUTCOME_HORIZON_EVIDENCE_CONFLICT")
        first_index = next(
            (index for index, close_at in enumerate(closes) if close_at > checked_occurred_at),
            None,
        )
        if first_index is None:
            raise OutcomeValidationError("OUTCOME_HORIZON_NOT_OBSERVABLE")
        offset = {"SESSION_CLOSE": 0, "1D": 1, "3D": 3, "5D": 5}[checked_horizon]
        target_index = first_index + offset
        if target_index >= len(closes):
            raise OutcomeValidationError("OUTCOME_HORIZON_NOT_OBSERVABLE")
        horizon_at = closes[target_index]
    normalized_evidence = {
        "schema": "options_copilot.outcome_horizon_evidence.v1",
        "target_rule": OUTCOME_TARGET_RULES[checked_horizon],
        "method": expected_method,
        "sessions": normalized_sessions,
    }
    normalized_evidence["horizon_evidence_hash"] = canonical_hash(
        normalized_evidence
    )
    frozen = freeze_json(normalized_evidence)
    assert isinstance(frozen, Mapping)
    return horizon_at, frozen


def _normalize_outcome_session(
    value: object,
    *,
    as_of: datetime,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "trading_date",
        "open_at",
        "close_at",
        "source",
        "source_id",
        "source_hash",
        "observed_at",
    }:
        raise OutcomeValidationError("OUTCOME_SESSION_EVIDENCE_INVALID")
    open_at = _timestamp(value.get("open_at"), "open_at")
    close_at = _timestamp(value.get("close_at"), "close_at")
    observed_at = _timestamp(value.get("observed_at"), "observed_at")
    if open_at >= close_at or observed_at > as_of:
        raise OutcomeValidationError("OUTCOME_SESSION_EVIDENCE_TIME_TRAVEL")
    trading_date = _nonblank(value.get("trading_date"), "trading_date")
    try:
        parsed_date = datetime.fromisoformat(trading_date).date()
    except ValueError as exc:
        raise OutcomeValidationError("OUTCOME_SESSION_EVIDENCE_INVALID") from exc
    if parsed_date != close_at.date():
        raise OutcomeValidationError("OUTCOME_SESSION_EVIDENCE_DATE_MISMATCH")
    normalized = {
        "trading_date": trading_date,
        "open_at": datetime_text(open_at),
        "close_at": datetime_text(close_at),
        "source": _nonblank(value.get("source"), "source"),
        "source_id": _nonblank(value.get("source_id"), "source_id"),
        "source_hash": _digest(value.get("source_hash"), "source_hash"),
        "observed_at": datetime_text(observed_at),
    }
    normalized["session_evidence_hash"] = canonical_hash(normalized)
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _normalize_outcome_ledger_binding(
    value: object,
    *,
    as_of: datetime,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "status",
        "evidence_id",
        "identity",
        "content_hash",
        "row_hash",
        "provider",
        "source_id",
        "observed_at",
    }:
        raise OutcomeValidationError("OUTCOME_LEDGER_BINDING_INVALID")
    observed_at = _timestamp(value.get("observed_at"), "observed_at")
    if value.get("status") != "ACTIVE" or observed_at > as_of:
        raise OutcomeValidationError("OUTCOME_LEDGER_BINDING_UNAVAILABLE")
    normalized = {
        "status": "ACTIVE",
        "evidence_id": _nonblank(value.get("evidence_id"), "evidence_id"),
        "identity": _nonblank(value.get("identity"), "identity"),
        "content_hash": _digest(value.get("content_hash"), "content_hash"),
        "row_hash": _digest(value.get("row_hash"), "row_hash"),
        "provider": _nonblank(value.get("provider"), "provider"),
        "source_id": _nonblank(value.get("source_id"), "source_id"),
        "observed_at": datetime_text(observed_at),
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _normalize_outcome_price_component(
    value: object,
    *,
    field: str,
    expected_symbol: str | None,
    horizon_at: datetime,
    economic_observed_at: datetime,
    as_of: datetime,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "symbol",
        "baseline_price",
        "price",
        "provenance",
    }:
        raise OutcomeValidationError("OUTCOME_PRICE_EVIDENCE_INCOMPLETE")
    symbol = _nonblank(value.get("symbol"), f"{field}.symbol").upper()
    if expected_symbol is not None and symbol != expected_symbol:
        raise OutcomeValidationError("OUTCOME_OBSERVATION_IDENTITY_CONFLICT")
    baseline = _decimal(value.get("baseline_price"), f"{field}.baseline_price")
    price = _decimal(value.get("price"), f"{field}.price")
    if baseline <= 0 or price <= 0:
        raise OutcomeValidationError("OUTCOME_PRICE_INVALID")
    provenance = _normalize_outcome_provenance(
        value.get("provenance"),
        field=f"{field}.provenance",
        allow_unavailable=False,
        horizon_at=horizon_at,
        economic_observed_at=economic_observed_at,
        as_of=as_of,
    )
    normalized = {
        "symbol": symbol,
        "baseline_price": baseline,
        "price": price,
        "return": (price - baseline) / baseline,
        "provenance": provenance,
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _normalize_option_market_outcome(
    value: object,
    *,
    horizon_at: datetime,
    economic_observed_at: datetime,
    as_of: datetime,
) -> Mapping[str, object]:
    expected = {"iv_change", "skew_change", "volume_change"}
    if not isinstance(value, Mapping) or set(value) != expected:
        raise OutcomeValidationError("OUTCOME_OPTION_MARKET_EVIDENCE_INCOMPLETE")
    output: dict[str, object] = {}
    for name in sorted(expected):
        raw = value.get(name)
        if not isinstance(raw, Mapping) or set(raw) != {"value", "provenance"}:
            raise OutcomeValidationError("OUTCOME_OPTION_MARKET_EVIDENCE_INCOMPLETE")
        provenance = _normalize_outcome_provenance(
            raw.get("provenance"),
            field=f"option_market.{name}.provenance",
            allow_unavailable=True,
            horizon_at=horizon_at,
            economic_observed_at=economic_observed_at,
            as_of=as_of,
        )
        metric = raw.get("value")
        if provenance["status"] == "UNAVAILABLE":
            if metric is not None:
                raise OutcomeValidationError("OUTCOME_UNAVAILABLE_METRIC_HAS_VALUE")
            normalized_value = None
        else:
            if metric is None:
                raise OutcomeValidationError("OUTCOME_AVAILABLE_METRIC_MISSING")
            normalized_value = _decimal(metric, name)
        output[name] = {"value": normalized_value, "provenance": provenance}
    frozen = freeze_json(output)
    assert isinstance(frozen, Mapping)
    return frozen


def _normalize_combination_outcome(
    value: object,
    *,
    horizon_at: datetime,
    economic_observed_at: datetime,
    as_of: datetime,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "estimated_pnl_usd",
        "estimated_return",
        "max_loss_usd",
        "provenance",
    }:
        raise OutcomeValidationError("OUTCOME_COMBINATION_EVIDENCE_INCOMPLETE")
    provenance = _normalize_outcome_provenance(
        value.get("provenance"),
        field="combination.provenance",
        allow_unavailable=True,
        horizon_at=horizon_at,
        economic_observed_at=economic_observed_at,
        as_of=as_of,
    )
    if provenance["status"] == "UNAVAILABLE":
        if any(
            value.get(name) is not None
            for name in ("estimated_pnl_usd", "estimated_return", "max_loss_usd")
        ):
            raise OutcomeValidationError("OUTCOME_UNAVAILABLE_METRIC_HAS_VALUE")
        frozen_unavailable = freeze_json(
            {
                "estimated_pnl_usd": None,
                "estimated_return": None,
                "max_loss_usd": None,
                "provenance": provenance,
            }
        )
        assert isinstance(frozen_unavailable, Mapping)
        return frozen_unavailable
    pnl = _decimal(value.get("estimated_pnl_usd"), "estimated_pnl_usd")
    maximum_loss = _decimal(value.get("max_loss_usd"), "max_loss_usd")
    estimated_return = _decimal(value.get("estimated_return"), "estimated_return")
    if maximum_loss <= 0 or estimated_return != pnl / maximum_loss:
        raise OutcomeValidationError("OUTCOME_COMBINATION_ESTIMATE_INVALID")
    normalized = {
        "estimated_pnl_usd": pnl,
        "estimated_return": estimated_return,
        "max_loss_usd": maximum_loss,
        "provenance": provenance,
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _normalize_thesis_validity(
    value: object,
    *,
    subject_kind: str,
    expected_thesis_hash: str | None,
    horizon_at: datetime,
    economic_observed_at: datetime,
    as_of: datetime,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "status",
        "valid",
        "reason_code",
        "thesis_hash",
        "provenance",
    }:
        raise OutcomeValidationError("OUTCOME_THESIS_EVIDENCE_INCOMPLETE")
    status = _nonblank(value.get("status"), "thesis_validity.status").upper()
    if status not in {"VALID", "INVALID", "UNKNOWN"}:
        raise OutcomeValidationError("OUTCOME_THESIS_STATUS_INVALID")
    valid = value.get("valid")
    supplied_hash = value.get("thesis_hash")
    if status == "UNKNOWN":
        if valid is not None or supplied_hash is not None:
            raise OutcomeValidationError("OUTCOME_THESIS_UNKNOWN_BINDING_INVALID")
        provenance = _normalize_outcome_provenance(
            value.get("provenance"),
            field="thesis_validity.provenance",
            allow_unavailable=True,
            horizon_at=horizon_at,
            economic_observed_at=economic_observed_at,
            as_of=as_of,
        )
        if provenance["status"] != "UNAVAILABLE":
            raise OutcomeValidationError("OUTCOME_THESIS_UNKNOWN_PROVENANCE_INVALID")
        normalized_hash = None
    else:
        if not isinstance(valid, bool) or valid is not (status == "VALID"):
            raise OutcomeValidationError("OUTCOME_THESIS_STATUS_INVALID")
        if expected_thesis_hash is None:
            raise OutcomeValidationError("THESIS_BINDING_UNAVAILABLE")
        normalized_hash = _digest(supplied_hash, "thesis_hash")
        if normalized_hash != _digest(expected_thesis_hash, "expected_thesis_hash"):
            raise OutcomeValidationError("OUTCOME_THESIS_BINDING_MISMATCH")
        provenance = _normalize_outcome_provenance(
            value.get("provenance"),
            field="thesis_validity.provenance",
            allow_unavailable=False,
            horizon_at=horizon_at,
            economic_observed_at=economic_observed_at,
            as_of=as_of,
        )
    normalized = {
        "status": status,
        "valid": valid,
        "reason_code": _nonblank(value.get("reason_code"), "reason_code"),
        "thesis_hash": normalized_hash,
        "provenance": provenance,
        "candidate_thesis_authority": (
            "HASH_BOUND" if subject_kind == "CANDIDATE" and normalized_hash else "UNKNOWN"
        ),
    }
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _normalize_outcome_provenance(
    value: object,
    *,
    field: str,
    allow_unavailable: bool,
    horizon_at: datetime,
    economic_observed_at: datetime,
    as_of: datetime,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise OutcomeValidationError("OUTCOME_PROVENANCE_MISSING")
    status = str(value.get("status", "")).upper()
    if status == "AVAILABLE":
        if set(value) != {
            "status",
            "source",
            "source_id",
            "source_hash",
            "observed_at",
        }:
            raise OutcomeValidationError("OUTCOME_PROVENANCE_INVALID")
        observed_at = _timestamp(value.get("observed_at"), f"{field}.observed_at")
        if not horizon_at <= observed_at <= economic_observed_at or observed_at > as_of:
            raise OutcomeValidationError("OUTCOME_PROVENANCE_STALE")
        normalized = {
            "status": "AVAILABLE",
            "reason_code": None,
            "source": _nonblank(value.get("source"), f"{field}.source"),
            "source_id": _nonblank(value.get("source_id"), f"{field}.source_id"),
            "source_hash": _digest(value.get("source_hash"), f"{field}.source_hash"),
            "observed_at": datetime_text(observed_at),
        }
    elif status == "UNAVAILABLE" and allow_unavailable:
        if set(value) != {
            "status",
            "reason_code",
            "source",
            "source_id",
            "source_hash",
            "observed_at",
        } or any(
            value.get(name) is not None
            for name in ("source", "source_id", "source_hash", "observed_at")
        ):
            raise OutcomeValidationError("OUTCOME_UNAVAILABLE_PROVENANCE_INVALID")
        normalized = {
            "status": "UNAVAILABLE",
            "reason_code": _nonblank(value.get("reason_code"), "reason_code"),
            "source": None,
            "source_id": None,
            "source_hash": None,
            "observed_at": None,
        }
    else:
        raise OutcomeValidationError("OUTCOME_PROVENANCE_UNAVAILABLE")
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen


def _outcome_horizon(value: object) -> str:
    horizon = _nonblank(value, "horizon").upper()
    if horizon not in OUTCOME_HORIZONS:
        raise OutcomeValidationError("OUTCOME_HORIZON_INVALID")
    return horizon


def _candidate_symbol(document: Mapping[str, object]) -> str:
    raw = document.get("symbol")
    if raw is None:
        cluster = document.get("cluster_evidence")
        if isinstance(cluster, Mapping):
            raw = cluster.get("ticker")
    if raw is None:
        request = document.get("cost_recompute_request")
        if isinstance(request, Mapping):
            candidate = request.get("candidate")
            if isinstance(candidate, Mapping):
                raw = candidate.get("symbol")
    return _nonblank(raw, "symbol").upper()


def _normalize_valuation_evidence(
    value: object,
    *,
    broker_snapshot_hash: object,
    decision_at: datetime,
    economic_observed_at: datetime,
) -> tuple[dict[str, object], dict[str, object], tuple[str, ...]]:
    empty = {
        "valuation_hash": None,
        "valuation_contracts_hash": None,
        "quote_observed_at": None,
        "quote_age_at_observation_seconds": None,
        "mfe_usd": None,
        "mae_usd": None,
        "mark_pnl_before_costs_usd": None,
        "executable_exit_pnl_before_costs_usd": None,
    }
    if not isinstance(value, Mapping):
        return empty, {}, ("VALUATION_EVIDENCE_MISSING",)
    try:
        required = {
            "schema",
            "broker_snapshot_hash",
            "legs",
            "mark_snapshots",
            "valuation_hash",
        }
        if set(value) != required:
            raise OutcomeValidationError("valuation evidence fields are incomplete")
        if value.get("schema") != "options_copilot.valuation_evidence.v1":
            raise OutcomeValidationError("valuation evidence schema mismatch")
        bound_broker_hash = _digest(
            broker_snapshot_hash,
            "broker_snapshot_hash",
        )
        if _digest(
            value.get("broker_snapshot_hash"),
            "valuation broker_snapshot_hash",
        ) != bound_broker_hash:
            raise OutcomeValidationError("valuation broker snapshot mismatch")
        expected_valuation_hash = canonical_hash(
            {key: item for key, item in value.items() if key != "valuation_hash"}
        )
        valuation_hash = _digest(value.get("valuation_hash"), "valuation_hash")
        if valuation_hash != expected_valuation_hash:
            raise OutcomeValidationError("valuation evidence hash mismatch")
        raw_legs = value.get("legs")
        if not isinstance(raw_legs, Sequence) or isinstance(
            raw_legs, (str, bytes, bytearray, memoryview)
        ):
            raise OutcomeValidationError("valuation legs must be a sequence")
        if not raw_legs:
            raise OutcomeValidationError("valuation legs cannot be empty")

        legs: dict[int, dict[str, object]] = {}
        contract_bindings: list[dict[str, object]] = []
        entry_batches: set[str] = set()
        exit_batches: set[str] = set()
        exit_ages: list[Decimal] = []
        entry_net_debit = Decimal("0")
        executable_exit_value = Decimal("0")
        mark_exit_value = Decimal("0")
        exit_times: list[datetime] = []
        for raw_leg in raw_legs:
            if not isinstance(raw_leg, Mapping):
                raise OutcomeValidationError("valuation leg must be a mapping")
            identity_names = (
                "con_id",
                "underlying",
                "security_type",
                "expiration",
                "strike",
                "right",
                "multiplier",
                "currency",
                "exchange",
            )
            required_leg = {
                *identity_names,
                "side",
                "quantity",
                "contract_hash",
                "entry_quote",
                "exit_quote",
            }
            if set(raw_leg) != required_leg:
                raise OutcomeValidationError("valuation leg fields are incomplete")
            con_id = _positive_int(raw_leg.get("con_id"), "con_id")
            if con_id in legs:
                raise OutcomeValidationError("valuation con_id must be unique")
            identity = {name: raw_leg[name] for name in identity_names}
            multiplier = _decimal(identity["multiplier"], "multiplier")
            if multiplier != Decimal("100"):
                raise OutcomeValidationError("valuation multiplier must be 100")
            if _nonblank(identity["security_type"], "security_type").upper() != "OPT":
                raise OutcomeValidationError("valuation security_type must be OPT")
            if _nonblank(identity["currency"], "currency").upper() != "USD":
                raise OutcomeValidationError("valuation currency must be USD")
            contract_hash = _digest(raw_leg.get("contract_hash"), "contract_hash")
            if contract_hash != canonical_hash(identity):
                raise OutcomeValidationError("valuation contract hash mismatch")
            quantity = _positive_int(raw_leg.get("quantity"), "quantity")
            side = _nonblank(raw_leg.get("side"), "side").upper()
            if side not in {"LONG", "SHORT"}:
                raise OutcomeValidationError("valuation side must be LONG or SHORT")
            entry = _normalize_bound_quote(
                raw_leg.get("entry_quote"),
                expected_con_id=con_id,
            )
            exit_quote = _normalize_bound_quote(
                raw_leg.get("exit_quote"),
                expected_con_id=con_id,
            )
            entry_age = _timedelta_seconds(decision_at - entry["observed_at"])
            if entry_age < 0 or entry_age > _MAXIMUM_QUOTE_AGE_SECONDS:
                raise OutcomeValidationError("entry quote is stale or future")
            exit_age = _timedelta_seconds(
                economic_observed_at - exit_quote["observed_at"]
            )
            entry_batches.add(str(entry["batch_id"]))
            exit_batches.add(str(exit_quote["batch_id"]))
            exit_ages.append(exit_age)
            exit_times.append(exit_quote["observed_at"])
            direction = Decimal("1") if side == "LONG" else Decimal("-1")
            entry_price = entry["ask"] if side == "LONG" else entry["bid"]
            exit_price = exit_quote["bid"] if side == "LONG" else exit_quote["ask"]
            midpoint = (exit_quote["bid"] + exit_quote["ask"]) / Decimal("2")
            scale = Decimal(quantity) * multiplier
            entry_net_debit += direction * entry_price * scale
            executable_exit_value += direction * exit_price * scale
            mark_exit_value += direction * midpoint * scale
            legs[con_id] = {
                "con_id": con_id,
                "identity": identity,
                "side": side,
                "quantity": quantity,
                "multiplier": multiplier,
                "contract_hash": contract_hash,
                "entry_quote": entry,
                "exit_quote": exit_quote,
            }
            contract_bindings.append(
                {
                    "con_id": con_id,
                    "side": side,
                    "quantity": quantity,
                    "multiplier": multiplier,
                    "contract_hash": contract_hash,
                    "entry_quote_hash": entry["quote_hash"],
                    "exit_quote_hash": exit_quote["quote_hash"],
                }
            )
        if len(entry_batches) != 1 or len(exit_batches) != 1:
            raise OutcomeValidationError("valuation quote batches are mixed")

        raw_snapshots = value.get("mark_snapshots")
        if not isinstance(raw_snapshots, Sequence) or isinstance(
            raw_snapshots, (str, bytes, bytearray, memoryview)
        ):
            raise OutcomeValidationError("mark_snapshots must be a sequence")
        if not raw_snapshots:
            raise OutcomeValidationError("mark_snapshots cannot be empty")
        mark_pnls: list[Decimal] = []
        prior_snapshot_at: datetime | None = None
        for raw_snapshot in raw_snapshots:
            if not isinstance(raw_snapshot, Mapping) or set(raw_snapshot) != {
                "observed_at",
                "batch_id",
                "quotes",
            }:
                raise OutcomeValidationError("mark snapshot fields are incomplete")
            snapshot_at = _timestamp(
                raw_snapshot.get("observed_at"),
                "mark snapshot observed_at",
            )
            if snapshot_at < decision_at or snapshot_at > economic_observed_at:
                raise OutcomeValidationError("mark snapshot is outside holding window")
            if prior_snapshot_at is not None and snapshot_at <= prior_snapshot_at:
                raise OutcomeValidationError("mark snapshots must be strictly ordered")
            prior_snapshot_at = snapshot_at
            batch_id = _nonblank(raw_snapshot.get("batch_id"), "mark batch_id")
            raw_quotes = raw_snapshot.get("quotes")
            if not isinstance(raw_quotes, Sequence) or isinstance(
                raw_quotes, (str, bytes, bytearray, memoryview)
            ):
                raise OutcomeValidationError("mark quotes must be a sequence")
            snapshot_quotes: dict[int, dict[str, object]] = {}
            for raw_quote in raw_quotes:
                quote = _normalize_bound_quote(raw_quote)
                con_id = int(quote["con_id"])
                if con_id not in legs or con_id in snapshot_quotes:
                    raise OutcomeValidationError("mark quote con_id set mismatch")
                if quote["observed_at"] != snapshot_at or quote["batch_id"] != batch_id:
                    raise OutcomeValidationError(
                        "mark quote timestamp or batch mismatch"
                    )
                snapshot_quotes[con_id] = quote
            if set(snapshot_quotes) != set(legs):
                raise OutcomeValidationError("mark snapshot is incomplete")
            portfolio_value = Decimal("0")
            for con_id, leg in legs.items():
                quote = snapshot_quotes[con_id]
                midpoint = (quote["bid"] + quote["ask"]) / Decimal("2")
                direction = Decimal("1") if leg["side"] == "LONG" else Decimal("-1")
                portfolio_value += (
                    direction
                    * midpoint
                    * Decimal(int(leg["quantity"]))
                    * leg["multiplier"]
                )
            mark_pnls.append(portfolio_value - entry_net_debit)

        mark_exit_pnl = mark_exit_value - entry_net_debit
        mark_pnls.append(mark_exit_pnl)
        executable_pnl = executable_exit_value - entry_net_debit
        reasons: list[str] = []
        if any(age < 0 for age in exit_ages):
            reasons.append("EXECUTABLE_EXIT_QUOTE_FUTURE")
            quote_age = min(exit_ages)
        else:
            quote_age = max(exit_ages)
            if quote_age > _MAXIMUM_QUOTE_AGE_SECONDS:
                reasons.append("EXECUTABLE_EXIT_QUOTE_STALE")
        context = {
            "legs": legs,
            "entry_quote_batch_id": next(iter(entry_batches)),
            "exit_quote_batch_id": next(iter(exit_batches)),
            "contract_bindings": tuple(contract_bindings),
        }
        return (
            {
                "valuation_hash": valuation_hash,
                "valuation_contracts_hash": canonical_hash(contract_bindings),
                "quote_observed_at": datetime_text(min(exit_times)),
                "quote_age_at_observation_seconds": quote_age,
                "mfe_usd": max(mark_pnls),
                "mae_usd": min(mark_pnls),
                "mark_pnl_before_costs_usd": mark_exit_pnl,
                "executable_exit_pnl_before_costs_usd": executable_pnl,
            },
            context,
            tuple(reasons),
        )
    except (KeyError, TypeError, ValueError, OutcomeValidationError):
        return empty, {}, ("VALUATION_EVIDENCE_INVALID",)


def _normalize_bound_quote(
    value: object,
    *,
    expected_con_id: int | None = None,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "con_id",
        "bid",
        "ask",
        "observed_at",
        "batch_id",
        "quote_hash",
    }:
        raise OutcomeValidationError("bound quote fields are incomplete")
    con_id = _positive_int(value.get("con_id"), "quote con_id")
    if expected_con_id is not None and con_id != expected_con_id:
        raise OutcomeValidationError("quote con_id binding mismatch")
    bid = _nonnegative_decimal(value.get("bid"), "quote bid")
    ask = _nonnegative_decimal(value.get("ask"), "quote ask")
    if bid >= ask:
        raise OutcomeValidationError("quote must have bid below ask")
    observed_at = _timestamp(value.get("observed_at"), "quote observed_at")
    batch_id = _nonblank(value.get("batch_id"), "quote batch_id")
    quote_hash = _digest(value.get("quote_hash"), "quote_hash")
    if quote_hash != canonical_hash(
        {key: item for key, item in value.items() if key != "quote_hash"}
    ):
        raise OutcomeValidationError("quote hash mismatch")
    return {
        "con_id": con_id,
        "bid": bid,
        "ask": ask,
        "observed_at": observed_at,
        "batch_id": batch_id,
        "quote_hash": quote_hash,
    }


def _normalize_execution_cost_binding(
    document: Mapping[str, object],
    *,
    candidate_id: str,
    cost_version: str | None,
    cost_hash: str | None,
    costs: Mapping[str, Decimal | None],
    decision_at: datetime,
    execution_cost_resolver: object | None,
    valuation_context: Mapping[str, object],
    execution_evidence: object,
) -> tuple[dict[str, object], tuple[str, ...]]:
    output: dict[str, object] = {
        "execution_cost_resolution_hash": None,
        "cost_recompute_request_hash": None,
        "candidate_cost_calculation_hash": None,
        "broker_evidence_hash": None,
        "quote_batch_id": None,
        "execution_contracts_hash": None,
        "total_contract_sides": None,
    }
    reasons: list[str] = []
    if "execution_cost_resolution" in document:
        reasons.append("CALLER_COST_RESOLUTION_FORBIDDEN")
    request = document.get("cost_recompute_request")
    if not isinstance(request, Mapping):
        reasons.append("COST_RECOMPUTE_REQUEST_MISSING")
        return output, tuple(reasons)
    try:
        if set(request) != {"scan_run_id", "candidate", "scenario"}:
            raise OutcomeValidationError("cost recompute request fields are invalid")
        scan_run_id = _nonblank(request.get("scan_run_id"), "scan_run_id")
        candidate_request = request.get("candidate")
        scenario_request = request.get("scenario")
        if not isinstance(candidate_request, Mapping) or not isinstance(
            scenario_request, Mapping
        ):
            raise OutcomeValidationError("cost recompute inputs must be mappings")
        output["cost_recompute_request_hash"] = canonical_hash(request)
        _validate_cost_candidate_binding(
            document,
            candidate_request=candidate_request,
            scenario_request=scenario_request,
            candidate_id=candidate_id,
            cost_version=cost_version,
            cost_hash=cost_hash,
            valuation_context=valuation_context,
        )
        resolve = getattr(execution_cost_resolver, "resolve", None)
        if not callable(resolve):
            raise OutcomeValidationError("execution cost resolver is unavailable")
        resolution = resolve(
            now=decision_at,
            scan_run_id=scan_run_id,
            candidates=(candidate_request,),
            scenarios=(scenario_request,),
        )
        if not isinstance(resolution, ExecutionCostResolution):
            raise OutcomeValidationError(
                "resolver did not return ExecutionCostResolution"
            )
        resolution_version = _version(
            resolution.cost_version,
            "resolution cost_version",
        )
        resolution_hash = _digest(resolution.cost_hash, "resolution cost_hash")
        contract_effective_at = _timestamp(
            resolution.contract_effective_at,
            "resolution contract_effective_at",
        )
        contract_signed_at = _timestamp(
            resolution.contract_signed_at,
            "resolution contract_signed_at",
        )
        marker_hash = _digest(
            resolution.contract_marker_hash,
            "resolution contract_marker_hash",
        )
        resolved_at = _timestamp(resolution.resolved_at, "resolution resolved_at")
        scan_run_id = _nonblank(resolution.scan_run_id, "resolution scan_run_id")
        candidate_documents = tuple(
            _candidate_cost_document(item) for item in resolution.candidates
        )
        expected_resolution_hash = canonical_hash(
            {
                "schema": "options_copilot.execution_cost_resolution.v1",
                "cost_version": resolution_version,
                "cost_hash": resolution_hash,
                "contract_effective_at": contract_effective_at,
                "contract_signed_at": contract_signed_at,
                "contract_marker_hash": marker_hash,
                "resolved_at": resolved_at,
                "scan_run_id": scan_run_id,
                "candidates": candidate_documents,
            }
        )
        supplied_resolution_hash = _digest(
            resolution.resolution_hash,
            "execution_cost_resolution_hash",
        )
        if supplied_resolution_hash != expected_resolution_hash:
            raise OutcomeValidationError("execution cost resolution hash mismatch")
        if resolved_at != decision_at:
            raise OutcomeValidationError(
                "execution cost resolution time must equal decision_at"
            )
        output["execution_cost_resolution_hash"] = supplied_resolution_hash
    except Exception:
        reasons.append("COST_RECOMPUTATION_FAILED")
        return output, tuple(dict.fromkeys(reasons))

    assert_current = getattr(execution_cost_resolver, "assert_current", None)
    if not callable(assert_current):
        reasons.append("COST_RESOLUTION_AUTHORITY_UNAVAILABLE")
    else:
        try:
            current = assert_current(resolution)
            if current is not resolution and current != resolution:
                raise ValueError("cost authority returned a different resolution")
        except Exception:
            reasons.append("COST_RESOLUTION_NOT_CURRENT")

    matching = [
        item
        for item in resolution.candidates
        if isinstance(item, CandidateCostResolution)
        and item.candidate_id == candidate_id
    ]
    if len(matching) != 1:
        reasons.append("COST_RESOLUTION_CANDIDATE_MISSING")
        return output, tuple(dict.fromkeys(reasons))
    candidate = matching[0]
    try:
        _candidate_cost_document(candidate)
        calculation_hash = _digest(
            candidate.calculation_hash,
            "candidate cost calculation_hash",
        )
        output["candidate_cost_calculation_hash"] = calculation_hash
        if (
            candidate.cost_version != resolution.cost_version
            or candidate.cost_hash != resolution.cost_hash
            or candidate.cost_version != cost_version
            or candidate.cost_hash != cost_hash
        ):
            reasons.append("COST_RESOLUTION_CONTRACT_MISMATCH")
        if candidate.execution_cost_usd != (
            candidate.commission_usd + candidate.slippage_usd
        ):
            reasons.append("COST_RESOLUTION_COMPONENT_MISMATCH")
        commission = costs.get("commission_usd")
        spread = costs.get("spread_usd")
        slippage = costs.get("slippage_usd")
        if (
            commission is not None
            and spread is not None
            and slippage is not None
            and (
                commission != candidate.commission_usd
                or spread + slippage != candidate.slippage_usd
                or commission + spread + slippage != candidate.execution_cost_usd
            )
        ):
            reasons.append("COST_RESOLUTION_COMPONENT_MISMATCH")
    except (AttributeError, TypeError, ValueError, OutcomeValidationError):
        reasons.append("COST_RESOLUTION_CANDIDATE_INVALID")
        return output, tuple(dict.fromkeys(reasons))

    evidence, evidence_reasons = _normalize_execution_evidence(
        execution_evidence,
        expected_broker_snapshot_hash=document.get("broker_snapshot_hash"),
        valuation_context=valuation_context,
    )
    output.update(evidence)
    reasons.extend(evidence_reasons)
    return output, tuple(dict.fromkeys(reasons))


def _validate_cost_candidate_binding(
    document: Mapping[str, object],
    *,
    candidate_request: Mapping[str, object],
    scenario_request: Mapping[str, object],
    candidate_id: str,
    cost_version: str | None,
    cost_hash: str | None,
    valuation_context: Mapping[str, object],
) -> None:
    if not valuation_context:
        raise OutcomeValidationError("valuation context is unavailable")
    if _nonblank(candidate_request.get("candidate_id"), "candidate_id") != candidate_id:
        raise OutcomeValidationError("cost candidate id mismatch")
    if candidate_request.get("execution_cost_contract_version") != cost_version:
        raise OutcomeValidationError("cost candidate version mismatch")
    if candidate_request.get("execution_cost_contract_hash") != cost_hash:
        raise OutcomeValidationError("cost candidate hash mismatch")
    if _digest(document.get("candidate_hash"), "candidate_hash") != canonical_hash(
        candidate_request
    ):
        raise OutcomeValidationError("frozen candidate hash mismatch")
    entry_batch_id = str(valuation_context["entry_quote_batch_id"])
    if candidate_request.get("quote_batch_id") != entry_batch_id:
        raise OutcomeValidationError("cost candidate quote batch mismatch")
    raw_legs = candidate_request.get("legs")
    if not isinstance(raw_legs, Sequence) or isinstance(
        raw_legs, (str, bytes, bytearray, memoryview)
    ):
        raise OutcomeValidationError("cost candidate legs must be a sequence")
    valuation_legs = valuation_context["legs"]
    assert isinstance(valuation_legs, Mapping)
    seen: set[int] = set()
    for raw_leg in raw_legs:
        if not isinstance(raw_leg, Mapping):
            raise OutcomeValidationError("cost candidate leg must be a mapping")
        con_id = _positive_int(raw_leg.get("con_id"), "candidate con_id")
        if con_id in seen or con_id not in valuation_legs:
            raise OutcomeValidationError("cost candidate con_id mismatch")
        seen.add(con_id)
        valuation_leg = valuation_legs[con_id]
        assert isinstance(valuation_leg, Mapping)
        quantity = raw_leg.get("ratio", raw_leg.get("quantity"))
        if _positive_int(quantity, "candidate quantity") != valuation_leg["quantity"]:
            raise OutcomeValidationError("cost candidate quantity mismatch")
        if _decimal(raw_leg.get("multiplier"), "candidate multiplier") != Decimal(
            "100"
        ):
            raise OutcomeValidationError("cost candidate multiplier mismatch")
        side = _nonblank(raw_leg.get("side"), "candidate side").upper()
        if side != valuation_leg["side"]:
            raise OutcomeValidationError("cost candidate side mismatch")
        identity = valuation_leg["identity"]
        assert isinstance(identity, Mapping)
        if any(raw_leg.get(name) != value for name, value in identity.items()):
            raise OutcomeValidationError("cost candidate contract mismatch")
        entry_quote = valuation_leg["entry_quote"]
        assert isinstance(entry_quote, Mapping)
        leg_batch = raw_leg.get(
            "quote_snapshot_id",
            raw_leg.get("quote_batch_id", raw_leg.get("batch_id")),
        )
        if (
            _decimal(raw_leg.get("bid"), "candidate bid") != entry_quote["bid"]
            or _decimal(raw_leg.get("ask"), "candidate ask") != entry_quote["ask"]
            or _timestamp(raw_leg.get("observed_at"), "candidate observed_at")
            != entry_quote["observed_at"]
            or leg_batch != entry_quote["batch_id"]
        ):
            raise OutcomeValidationError("cost candidate entry quote mismatch")
    if seen != set(valuation_legs):
        raise OutcomeValidationError("cost candidate leg set is incomplete")
    if scenario_request.get("candidate_id") != candidate_id:
        raise OutcomeValidationError("cost scenario candidate mismatch")
    if (
        scenario_request.get("cost_version") != cost_version
        or scenario_request.get("cost_hash") != cost_hash
    ):
        raise OutcomeValidationError("cost scenario contract mismatch")


def _candidate_cost_document(candidate: object) -> dict[str, object]:
    if not isinstance(candidate, CandidateCostResolution):
        raise OutcomeValidationError(
            "resolution candidate must be CandidateCostResolution"
        )
    scenario_count = candidate.scenario_count
    if (
        isinstance(scenario_count, bool)
        or not isinstance(scenario_count, int)
        or scenario_count <= 0
    ):
        raise OutcomeValidationError("candidate scenario_count must be positive")
    return {
        "candidate_id": _nonblank(
            candidate.candidate_id,
            "candidate cost candidate_id",
        ),
        "cost_version": _version(candidate.cost_version, "candidate cost_version"),
        "cost_hash": _digest(candidate.cost_hash, "candidate cost_hash"),
        "quote_batch_id": _nonblank(
            candidate.quote_batch_id,
            "candidate quote_batch_id",
        ),
        "commission_usd": _nonnegative_decimal(
            candidate.commission_usd,
            "candidate commission_usd",
        ),
        "slippage_usd": _nonnegative_decimal(
            candidate.slippage_usd,
            "candidate slippage_usd",
        ),
        "execution_cost_usd": _nonnegative_decimal(
            candidate.execution_cost_usd,
            "candidate execution_cost_usd",
        ),
        "expected_value_before_costs_usd": _decimal(
            candidate.expected_value_before_costs_usd,
            "candidate expected_value_before_costs_usd",
        ),
        "after_cost_expected_value": _decimal(
            candidate.after_cost_expected_value,
            "candidate after_cost_expected_value",
        ),
        "stress_execution_cost_usd": _nonnegative_decimal(
            candidate.stress_execution_cost_usd,
            "candidate stress_execution_cost_usd",
        ),
        "stress_after_cost_expected_value": _decimal(
            candidate.stress_after_cost_expected_value,
            "candidate stress_after_cost_expected_value",
        ),
        "scenario_count": scenario_count,
        "calculation_hash": _digest(
            candidate.calculation_hash,
            "candidate calculation_hash",
        ),
    }


def _normalize_execution_evidence(
    value: object,
    *,
    expected_broker_snapshot_hash: object,
    valuation_context: Mapping[str, object],
) -> tuple[dict[str, object], tuple[str, ...]]:
    empty: dict[str, object] = {
        "broker_evidence_hash": None,
        "quote_batch_id": None,
        "execution_contracts_hash": None,
        "total_contract_sides": None,
    }
    if not isinstance(value, Mapping):
        return empty, ("EXECUTION_CONTRACT_EVIDENCE_INVALID",)
    try:
        required = {
            "broker_snapshot_hash",
            "broker_evidence_hash",
            "entry_quote_batch_id",
            "exit_quote_batch_id",
            "contracts",
            "contracts_hash",
            "total_contract_sides",
        }
        if set(value) != required:
            raise OutcomeValidationError("execution evidence fields are incomplete")
        broker_snapshot_hash = _digest(
            value.get("broker_snapshot_hash"),
            "execution evidence broker_snapshot_hash",
        )
        if broker_snapshot_hash != _digest(
            expected_broker_snapshot_hash,
            "broker_snapshot_hash",
        ):
            raise OutcomeValidationError("broker snapshot binding mismatch")
        broker_evidence_hash = _digest(
            value.get("broker_evidence_hash"),
            "broker_evidence_hash",
        )
        if broker_evidence_hash != canonical_hash(
            {key: item for key, item in value.items() if key != "broker_evidence_hash"}
        ):
            raise OutcomeValidationError("broker evidence hash mismatch")
        entry_quote_batch_id = _nonblank(
            value.get("entry_quote_batch_id"),
            "entry_quote_batch_id",
        )
        exit_quote_batch_id = _nonblank(
            value.get("exit_quote_batch_id"),
            "exit_quote_batch_id",
        )
        if (
            entry_quote_batch_id != valuation_context.get("entry_quote_batch_id")
            or exit_quote_batch_id != valuation_context.get("exit_quote_batch_id")
        ):
            raise OutcomeValidationError("quote batch binding mismatch")
        contracts = value.get("contracts")
        if not isinstance(contracts, Sequence) or isinstance(
            contracts, (str, bytes, bytearray, memoryview)
        ):
            raise OutcomeValidationError("contracts must be a sequence")
        if not contracts:
            raise OutcomeValidationError("contracts cannot be empty")
        contracts_hash = _digest(value.get("contracts_hash"), "contracts_hash")
        if contracts_hash != canonical_hash(contracts):
            raise OutcomeValidationError("contracts hash mismatch")
        total_contract_sides = value.get("total_contract_sides")
        if (
            isinstance(total_contract_sides, bool)
            or not isinstance(total_contract_sides, int)
            or total_contract_sides <= 0
        ):
            raise OutcomeValidationError("total_contract_sides must be positive")
        seen_con_ids: set[int] = set()
        calculated_sides = 0
        valuation_legs = valuation_context.get("legs")
        if not isinstance(valuation_legs, Mapping):
            raise OutcomeValidationError("valuation leg binding is unavailable")
        for raw_contract in contracts:
            if not isinstance(raw_contract, Mapping):
                raise OutcomeValidationError("contract evidence must be a mapping")
            contract_required = {
                "con_id",
                "quantity",
                "multiplier",
                "side",
                "contract_hash",
                "entry_quote_hash",
                "exit_quote_hash",
            }
            if set(raw_contract) != contract_required:
                raise OutcomeValidationError("contract evidence fields are incomplete")
            con_id = _positive_int(raw_contract.get("con_id"), "contract con_id")
            quantity = _positive_int(
                raw_contract.get("quantity"),
                "contract quantity",
            )
            if con_id in seen_con_ids:
                raise OutcomeValidationError("contract con_id must be unique")
            seen_con_ids.add(con_id)
            multiplier = _decimal(raw_contract.get("multiplier"), "multiplier")
            if multiplier != Decimal("100"):
                raise OutcomeValidationError("contract multiplier must be 100")
            side = _nonblank(raw_contract.get("side"), "side").upper()
            if con_id not in valuation_legs:
                raise OutcomeValidationError("contract con_id is not valued")
            valued = valuation_legs[con_id]
            assert isinstance(valued, Mapping)
            if (
                quantity != valued["quantity"]
                or multiplier != valued["multiplier"]
                or side != valued["side"]
                or _digest(raw_contract.get("contract_hash"), "contract_hash")
                != valued["contract_hash"]
                or _digest(
                    raw_contract.get("entry_quote_hash"),
                    "entry_quote_hash",
                )
                != valued["entry_quote"]["quote_hash"]
                or _digest(
                    raw_contract.get("exit_quote_hash"),
                    "exit_quote_hash",
                )
                != valued["exit_quote"]["quote_hash"]
            ):
                raise OutcomeValidationError("contract valuation binding mismatch")
            calculated_sides += quantity
        if seen_con_ids != set(valuation_legs):
            raise OutcomeValidationError("execution contract set is incomplete")
        if calculated_sides != total_contract_sides:
            raise OutcomeValidationError("total contract sides binding mismatch")
        return (
            {
                "broker_evidence_hash": broker_evidence_hash,
                "quote_batch_id": entry_quote_batch_id,
                "execution_contracts_hash": contracts_hash,
                "total_contract_sides": total_contract_sides,
            },
            (),
        )
    except (TypeError, ValueError, OutcomeValidationError):
        return empty, ("EXECUTION_CONTRACT_EVIDENCE_INVALID",)


def _normalize_broker_cost_evidence(
    value: object,
    *,
    costs: Mapping[str, Decimal | None],
    broker_snapshot_hash: object,
) -> tuple[dict[str, object], tuple[str, ...]]:
    empty = {
        "broker_cost_evidence_hash": None,
    }
    if not isinstance(value, Mapping):
        return empty, ("BROKER_COST_EVIDENCE_MISSING",)
    try:
        if set(value) != {"schema", "components", "evidence_hash"}:
            raise OutcomeValidationError("broker cost evidence fields are incomplete")
        if value.get("schema") != "options_copilot.broker_cost_evidence.v1":
            raise OutcomeValidationError("broker cost evidence schema mismatch")
        evidence_hash = _digest(
            value.get("evidence_hash"),
            "broker_cost_evidence_hash",
        )
        if evidence_hash != canonical_hash(
            {key: item for key, item in value.items() if key != "evidence_hash"}
        ):
            raise OutcomeValidationError("broker cost evidence hash mismatch")
        components = value.get("components")
        if not isinstance(components, Mapping):
            raise OutcomeValidationError("broker cost components must be a mapping")
        required_names = {
            "fees_usd",
            "assignment_usd",
            "exercise_usd",
            "dividend_usd",
        }
        if set(components) != required_names:
            raise OutcomeValidationError("broker cost components are incomplete")
        expected_broker_hash = _digest(
            broker_snapshot_hash,
            "broker_snapshot_hash",
        )
        for name in sorted(required_names):
            component = components[name]
            if not isinstance(component, Mapping) or set(component) != {
                "component",
                "amount_usd",
                "source",
                "reference_id",
                "broker_snapshot_hash",
                "evidence_hash",
            }:
                raise OutcomeValidationError("broker cost component is incomplete")
            if component.get("component") != name:
                raise OutcomeValidationError("broker cost component name mismatch")
            source = _nonblank(component.get("source"), "broker cost source").upper()
            if source not in {"IBKR_FILL", "IBKR_STATEMENT"}:
                raise OutcomeValidationError("broker cost source is not authoritative")
            _nonblank(component.get("reference_id"), "broker cost reference_id")
            if _digest(
                component.get("broker_snapshot_hash"),
                "broker cost broker_snapshot_hash",
            ) != expected_broker_hash:
                raise OutcomeValidationError("broker cost snapshot binding mismatch")
            amount = _nonnegative_decimal(
                component.get("amount_usd"),
                "broker cost amount_usd",
            )
            if costs.get(name) is None or amount != costs[name]:
                raise OutcomeValidationError("broker cost amount mismatch")
            component_hash = _digest(
                component.get("evidence_hash"),
                "broker cost component evidence_hash",
            )
            if component_hash != canonical_hash(
                {
                    key: item
                    for key, item in component.items()
                    if key != "evidence_hash"
                }
            ):
                raise OutcomeValidationError("broker cost component hash mismatch")
        return (
            {
                "broker_cost_evidence_hash": evidence_hash,
            },
            (),
        )
    except (KeyError, TypeError, ValueError, OutcomeValidationError):
        return empty, ("BROKER_COST_EVIDENCE_INVALID",)


def _normalize_costs(
    value: object,
) -> tuple[dict[str, Decimal | None], tuple[str, ...]]:
    if not isinstance(value, Mapping):
        return ({name: None for name in _COST_FIELDS}, ("COST_COMPONENTS_MISSING",))
    output: dict[str, Decimal | None] = {}
    missing = False
    for name in _COST_FIELDS:
        if name not in value or value[name] is None:
            output[name] = None
            missing = True
            continue
        amount = _decimal(value[name], name)
        if amount < 0:
            raise OutcomeValidationError(f"{name} cannot be negative")
        output[name] = amount
    unknown = sorted(set(value).difference(_COST_FIELDS))
    if unknown:
        raise OutcomeValidationError(f"unknown cost component {unknown[0]}")
    return output, (("COST_COMPONENTS_MISSING",) if missing else ())


def _normalize_quote_quality(value: object) -> tuple[Mapping[str, object], str]:
    if not isinstance(value, Mapping) or not value:
        return freeze_json({}), "MISSING"
    normalized = json.loads(canonical_json(value))
    status = str(normalized.get("status", "UNKNOWN")).strip().upper()
    complete = normalized.get("bid_ask_complete") is True
    if status in {"EXECUTABLE", "GOOD"} and complete:
        status = "EXECUTABLE"
    elif status == "MISSING":
        status = "MISSING"
    else:
        status = "UNRELIABLE"
    frozen = freeze_json(normalized)
    assert isinstance(frozen, Mapping)
    return frozen, status


def _normalize_cluster_evidence(
    value: object,
    *,
    decision_at: datetime,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not value:
        return freeze_json({"status": "UNKNOWN"})
    status = str(value.get("status", "UNKNOWN")).strip().upper()
    if status not in {"KNOWN", "UNKNOWN"}:
        raise OutcomeValidationError("cluster evidence status must be KNOWN or UNKNOWN")
    output: dict[str, object] = {"status": status}
    string_fields = (
        "ticker",
        "issuer_id",
        "provider",
        "event_id",
        "catalyst_id",
        "corporate_family",
        "macro_event_id",
        "correlation_group",
        "provider_content_hash",
        "thesis_hash",
        "structure_hash",
    )
    for name in string_fields:
        raw = value.get(name)
        if raw is None:
            output[name] = None
            continue
        text = _nonblank(raw, name)
        output[name] = (
            text.upper()
            if name
            in {
                "ticker",
                "issuer_id",
                "provider",
                "corporate_family",
                "correlation_group",
            }
            else text
        )
    for name in ("slot_at", "window_start", "window_end"):
        raw = value.get(name)
        output[name] = None if raw is None else datetime_text(_timestamp(raw, name))
    if output["slot_at"] is None:
        output["slot_at"] = datetime_text(decision_at)
    if (
        output["window_start"] is not None
        and output["window_end"] is not None
        and _timestamp(output["window_start"], "window_start")
        > _timestamp(output["window_end"], "window_end")
    ):
        raise OutcomeValidationError("cluster window_start cannot be after window_end")
    holdings_raw = value.get("holding_ids", ())
    if not isinstance(holdings_raw, Sequence) or isinstance(
        holdings_raw, (str, bytes, bytearray, memoryview)
    ):
        raise OutcomeValidationError("holding_ids must be a sequence")
    output["holding_ids"] = tuple(
        sorted({_nonblank(item, "holding_id") for item in holdings_raw})
    )
    if status == "KNOWN" and output.get("ticker") is None:
        raise OutcomeValidationError("KNOWN cluster evidence requires ticker")
    if status == "KNOWN" and output.get("event_id") is not None and (
        output.get("issuer_id") is None or output.get("provider") is None
    ):
        raise OutcomeValidationError(
            "KNOWN event identity requires issuer_id and provider namespace"
        )
    if status == "KNOWN" and not any(
        output.get(name)
        for name in (
            "ticker",
            "event_id",
            "macro_event_id",
            "provider_content_hash",
            "holding_ids",
        )
    ):
        output["status"] = "UNKNOWN"
    frozen = freeze_json(output)
    assert isinstance(frozen, Mapping)
    return frozen


def _pnl_classification(
    before: Decimal | None,
    after: Decimal | None,
) -> str:
    if before is None or after is None:
        return "EXECUTABLE_EXIT_UNAVAILABLE"
    if before > 0 and after < 0:
        return "COST_FLIPPED_NEGATIVE"
    if after > 0:
        return "AFTER_COST_POSITIVE"
    if after == 0:
        return "AFTER_COST_ZERO"
    return "AFTER_COST_NEGATIVE"


def _connected_components(
    outcomes: Sequence[StoredOutcome],
    spec: VerifiedIndependenceSpec,
) -> tuple[tuple[StoredOutcome, ...], ...]:
    parent = list(range(len(outcomes)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    for left in range(len(outcomes)):
        for right in range(left + 1, len(outcomes)):
            if _same_cluster(outcomes[left], outcomes[right], spec):
                union(left, right)
    groups: dict[int, list[StoredOutcome]] = {}
    for index, item in enumerate(outcomes):
        groups.setdefault(find(index), []).append(item)
    return tuple(
        tuple(group)
        for _, group in sorted(
            groups.items(),
            key=lambda item: _representative_key(min(item[1], key=_representative_key)),
        )
    )


def _same_cluster(
    left: StoredOutcome,
    right: StoredOutcome,
    spec: VerifiedIndependenceSpec,
) -> bool:
    a = left.cluster_evidence
    b = right.cluster_evidence
    if a.get("status") != "KNOWN" or b.get("status") != "KNOWN":
        return False
    rules = spec.rules
    ticker_a, ticker_b = a.get("ticker"), b.get("ticker")
    exact_event_a, exact_event_b = a.get("event_id"), b.get("event_id")
    if (
        rules["same_event_identity"] is True
        and exact_event_a is not None
        and exact_event_a == exact_event_b
        and _same_event_namespace(a, b, rules)
    ):
        return True
    event_a = a.get("event_id") or a.get("catalyst_id")
    event_b = b.get("event_id") or b.get("catalyst_id")
    if (
        rules["same_ticker_event"] is True
        and ticker_a is not None
        and ticker_a == ticker_b
        and event_a is not None
        and event_a == event_b
        and _same_event_namespace(a, b, rules)
    ):
        return True
    if (
        rules["same_corporate_family_event"] is True
        and a.get("corporate_family") is not None
        and a.get("corporate_family") == b.get("corporate_family")
        and event_a is not None
        and event_a == event_b
    ):
        return True
    if (
        rules["cross_provider_duplicates"] is True
        and a.get("provider_content_hash") is not None
        and a.get("provider_content_hash") == b.get("provider_content_hash")
    ):
        return True
    if (
        rules["adjacent_same_ticker_slots"] is True
        and ticker_a is not None
        and ticker_a == ticker_b
    ):
        slot_a = _optional_timestamp(a.get("slot_at"), "slot_at")
        slot_b = _optional_timestamp(b.get("slot_at"), "slot_at")
        if slot_a is not None and slot_b is not None:
            maximum = timedelta(hours=int(rules["adjacent_slot_hours"]))
            if abs(slot_a - slot_b) <= maximum:
                return True
    if rules["overlapping_event_windows"] is True and (
        (ticker_a is not None and ticker_a == ticker_b)
        or (
            a.get("corporate_family") is not None
            and a.get("corporate_family") == b.get("corporate_family")
        )
    ):
        if _windows_overlap(a, b):
            return True
    if (
        rules["correlated_tickers_same_macro_event"] is True
        and a.get("macro_event_id") is not None
        and a.get("macro_event_id") == b.get("macro_event_id")
    ):
        group_a = a.get("correlation_group") or _configured_group(ticker_a, rules)
        group_b = b.get("correlation_group") or _configured_group(ticker_b, rules)
        if (ticker_a is not None and ticker_a == ticker_b) or (
            group_a is not None and group_a == group_b
        ):
            return True
    if rules["overlapping_holdings_thesis_structure"] is True:
        holdings_a = set(a.get("holding_ids") or ())
        holdings_b = set(b.get("holding_ids") or ())
        if (
            holdings_a.intersection(holdings_b)
            and a.get("thesis_hash") is not None
            and a.get("thesis_hash") == b.get("thesis_hash")
            and a.get("structure_hash") is not None
            and a.get("structure_hash") == b.get("structure_hash")
        ):
            return True
    return False


def _same_event_namespace(
    left: Mapping[str, object],
    right: Mapping[str, object],
    rules: Mapping[str, object],
) -> bool:
    fields = rules.get("event_identity_namespace_fields")
    if not isinstance(fields, tuple):
        return False
    return all(
        left.get(name) is not None and left.get(name) == right.get(name)
        for name in fields
    )


def _windows_overlap(a: Mapping[str, object], b: Mapping[str, object]) -> bool:
    start_a = _optional_timestamp(a.get("window_start"), "window_start")
    end_a = _optional_timestamp(a.get("window_end"), "window_end")
    start_b = _optional_timestamp(b.get("window_start"), "window_start")
    end_b = _optional_timestamp(b.get("window_end"), "window_end")
    return (
        start_a is not None
        and end_a is not None
        and start_b is not None
        and end_b is not None
        and max(start_a, start_b) <= min(end_a, end_b)
    )


def _configured_group(ticker: object, rules: Mapping[str, object]) -> str | None:
    if ticker is None:
        return None
    groups = rules.get("correlated_ticker_groups", ())
    if not isinstance(groups, tuple):
        return None
    for index, group in enumerate(groups):
        if isinstance(group, tuple) and ticker in group:
            return f"SIGNED-GROUP-{index}"
    return None


def _representative_key(
    item: StoredOutcome,
) -> tuple[datetime, str, str]:
    body = item.body
    return (
        _timestamp(body["decision_at"], "decision_at"),
        str(body["decision_id"]),
        item.content_hash,
    )


def _content_payload(
    *,
    outcome_id: str,
    base_identity_hash: str,
    version: int,
    body: Mapping[str, object],
    observation_hash: str,
    supersedes_outcome_id: str | None,
    supersedes_hash: str | None,
    recorded_at: datetime,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.outcome_record.v1",
        "outcome_id": outcome_id,
        "base_identity_hash": base_identity_hash,
        "version": version,
        "body": dict(body),
        "observation_hash": observation_hash,
        "supersedes_outcome_id": supersedes_outcome_id,
        "supersedes_hash": supersedes_hash,
        "recorded_at": datetime_text(recorded_at),
    }


def _stored_outcome(row: sqlite3.Row) -> StoredOutcome:
    try:
        raw = json.loads(str(row["body_json"]))
        body = freeze_json(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise OutcomeLedgerCorruption("stored outcome body is invalid") from exc
    if not isinstance(body, Mapping):
        raise OutcomeLedgerCorruption("stored outcome body is not a mapping")
    return StoredOutcome(
        sequence=int(row["sequence"]),
        outcome_id=str(row["outcome_id"]),
        base_identity_hash=str(row["base_identity_hash"]),
        version=int(row["version"]),
        body=body,
        observation_hash=str(row["observation_hash"]),
        content_hash=str(row["content_hash"]),
        supersedes_outcome_id=(
            None
            if row["supersedes_outcome_id"] is None
            else str(row["supersedes_outcome_id"])
        ),
        supersedes_hash=(
            None if row["supersedes_hash"] is None else str(row["supersedes_hash"])
        ),
        previous_hash=str(row["previous_hash"]),
        chain_hash=str(row["chain_hash"]),
        recorded_at=datetime.fromisoformat(str(row["recorded_at"])),
    )


def _validate_rules(value: object) -> dict[str, object]:
    rules = _mapping(value, "rules")
    missing = sorted(set(_DEFAULT_RULES).difference(rules))
    if missing:
        raise IndependenceSpecValidationError(
            f"independence rules are missing {missing[0]}"
        )
    unknown = sorted(set(rules).difference(_DEFAULT_RULES))
    if unknown:
        raise IndependenceSpecValidationError(
            f"independence rules contain unknown field {unknown[0]}"
        )
    boolean_names = (
        "same_event_identity",
        "same_ticker_event",
        "same_corporate_family_event",
        "adjacent_same_ticker_slots",
        "overlapping_event_windows",
        "correlated_tickers_same_macro_event",
        "cross_provider_duplicates",
        "overlapping_holdings_thesis_structure",
    )
    if any(not isinstance(rules[name], bool) for name in boolean_names):
        raise IndependenceSpecValidationError("clustering rule flags must be boolean")
    namespace_fields = rules["event_identity_namespace_fields"]
    if not isinstance(namespace_fields, Sequence) or isinstance(
        namespace_fields,
        (str, bytes, bytearray, memoryview),
    ):
        raise IndependenceSpecValidationError(
            "event_identity_namespace_fields must be a sequence"
        )
    normalized_namespace = tuple(
        _nonblank(item, "event identity namespace field")
        for item in namespace_fields
    )
    if normalized_namespace != ("ticker", "issuer_id", "provider"):
        raise IndependenceSpecValidationError(
            "event identity namespace must freeze ticker issuer_id provider"
        )
    hours = rules["adjacent_slot_hours"]
    if isinstance(hours, bool) or not isinstance(hours, int) or hours <= 0:
        raise IndependenceSpecValidationError("adjacent_slot_hours must be positive")
    if rules["representative_selection"] != "EARLIEST_DECISION_THEN_ID":
        raise IndependenceSpecValidationError(
            "unsupported representative selection rule"
        )
    if (
        rules["independent_weight"] != 1
        or rules["duplicate_weight"] != 0
        or rules["unknown_weight"] != 0
    ):
        raise IndependenceSpecValidationError(
            "count weights must be restricted to 0 or 1"
        )
    if rules["unknown_exclusion_reason"] != "INDEPENDENCE_UNKNOWN":
        raise IndependenceSpecValidationError(
            "UNKNOWN must have the fixed INDEPENDENCE_UNKNOWN reason"
        )
    groups = rules["correlated_ticker_groups"]
    if not isinstance(groups, Sequence) or isinstance(
        groups, (str, bytes, bytearray, memoryview)
    ):
        raise IndependenceSpecValidationError(
            "correlated_ticker_groups must be a sequence"
        )
    normalized_groups: list[tuple[str, ...]] = []
    for group in groups:
        if not isinstance(group, Sequence) or isinstance(
            group, (str, bytes, bytearray, memoryview)
        ):
            raise IndependenceSpecValidationError(
                "each correlated ticker group must be a sequence"
            )
        tickers = tuple(sorted({_nonblank(item, "ticker").upper() for item in group}))
        if len(tickers) < 2:
            raise IndependenceSpecValidationError(
                "correlated ticker groups need at least two tickers"
            )
        normalized_groups.append(tickers)
    return {
        **{name: rules[name] for name in boolean_names},
        "event_identity_namespace_fields": normalized_namespace,
        "adjacent_slot_hours": hours,
        "representative_selection": rules["representative_selection"],
        "independent_weight": 1,
        "duplicate_weight": 0,
        "unknown_weight": 0,
        "unknown_exclusion_reason": "INDEPENDENCE_UNKNOWN",
        "correlated_ticker_groups": normalized_groups,
    }


def _document(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    if hasattr(value, "__dict__"):
        return dict(vars(value))
    raise OutcomeValidationError("outcome must be a mapping or data object")


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise IndependenceSpecValidationError(f"{field} must be a mapping")
    try:
        normalized = json.loads(canonical_json(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IndependenceSpecValidationError(f"{field} is not canonical JSON") from exc
    if not isinstance(normalized, dict):
        raise IndependenceSpecValidationError(f"{field} must be a mapping")
    return normalized


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError as exc:
            raise OutcomeValidationError(f"{field} is not ISO-8601") from exc
    try:
        return utc_datetime(value, field=field)
    except (TypeError, ValueError) as exc:
        raise OutcomeValidationError(str(exc)) from exc


def _optional_timestamp(value: object, field: str) -> datetime | None:
    return None if value is None else _timestamp(value, field)


def _timedelta_seconds(value: timedelta) -> Decimal:
    return (
        Decimal(value.days * 86400 + value.seconds)
        + Decimal(value.microseconds) / Decimal("1000000")
    )


def _decimal(value: object, field: str) -> Decimal:
    if (
        isinstance(value, bool)
        or isinstance(value, float)
        or not isinstance(value, (Decimal, int, str))
    ):
        raise OutcomeValidationError(
            f"{field} must be Decimal-compatible without binary float"
        )
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise OutcomeValidationError(f"{field} must be a finite Decimal") from exc
    if not result.is_finite():
        raise OutcomeValidationError(f"{field} must be a finite Decimal")
    return result


def _nonnegative_decimal(value: object, field: str) -> Decimal:
    result = _decimal(value, field)
    if result < 0:
        raise OutcomeValidationError(f"{field} cannot be negative")
    return result


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise OutcomeValidationError(f"{field} must be a positive integer")
    return value


def _optional_decimal(value: object, field: str) -> Decimal | None:
    return None if value is None else _decimal(value, field)


def _stored_decimal(value: object) -> Decimal:
    if not isinstance(value, Decimal):
        raise OutcomeLedgerCorruption("stored Decimal field is invalid")
    return value


def _stored_optional_decimal(value: object) -> Decimal | None:
    return None if value is None else _stored_decimal(value)


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise OutcomeValidationError(f"{field} must be a lowercase SHA-256 hash")
    return value


def _optional_digest(value: object, field: str) -> str | None:
    return None if value is None else _digest(value, field)


_MANAGEMENT_ACTIONS = frozenset(
    {"HOLD", "EXIT", "TAKE_PROFIT", "STOP_LOSS", "TIME_EXIT"}
)
_COUNTERFACTUAL_PATHS = ("FOLLOW_EXIT_POLICY", "HOLD_TO_HORIZON")
_RESULT_QUOTE_FIELDS = frozenset(
    {
        "contract_id",
        "side",
        "quantity",
        "bid",
        "ask",
        "quote_identity_hash",
        "source",
        "source_content_hash",
        "observed_at",
    }
)
_RESULT_AUTHORITY_FIELDS = frozenset(
    {
        "schema",
        "candidate_hash",
        "cost_contract_hash",
        "legs",
        "entry_value_usd",
        "costs_usd",
        "costs_hash",
        "max_loss_usd",
        "max_loss_evidence_hash",
        "authority_hash",
    }
)


def _exact_fields(
    value: Mapping[str, object],
    expected: set[str] | frozenset[str],
    field: str,
) -> None:
    if set(value) != set(expected):
        raise OutcomeValidationError(f"{field} fields are invalid")


def _normalize_result_authority(
    value: object,
    *,
    candidate_hash: str,
    cost_contract_hash: str | None,
) -> Mapping[str, object] | None:
    if value is None:
        return None
    if cost_contract_hash is None:
        raise OutcomeValidationError("outcome result cost authority is unavailable")
    if not isinstance(value, Mapping):
        raise OutcomeValidationError("outcome result authority must be a mapping")
    _exact_fields(value, _RESULT_AUTHORITY_FIELDS, "outcome result authority")
    if (
        value.get("schema") != "options_copilot.outcome_result_authority.v1"
        or value.get("candidate_hash") != candidate_hash
        or value.get("cost_contract_hash") != cost_contract_hash
    ):
        raise OutcomeValidationError("outcome result authority binding is invalid")
    legs_raw = value.get("legs")
    if not isinstance(legs_raw, Sequence) or isinstance(
        legs_raw, (str, bytes, bytearray, memoryview)
    ) or not legs_raw:
        raise OutcomeValidationError("outcome result authority legs are invalid")
    legs: list[Mapping[str, object]] = []
    identities: set[str] = set()
    for raw in legs_raw:
        if not isinstance(raw, Mapping):
            raise OutcomeValidationError("outcome result authority leg is invalid")
        _exact_fields(raw, {"contract_id", "side", "quantity"}, "authority leg")
        contract_id = _nonblank(raw.get("contract_id"), "contract_id")
        if contract_id in identities:
            raise OutcomeValidationError("outcome result authority leg is duplicated")
        identities.add(contract_id)
        side = _nonblank(raw.get("side"), "side").upper()
        if side not in {"BUY", "SELL"}:
            raise OutcomeValidationError("outcome result authority side is invalid")
        legs.append(
            freeze_json(
                {
                    "contract_id": contract_id,
                    "side": side,
                    "quantity": _positive_int(raw.get("quantity"), "quantity"),
                }
            )
        )
    entry_value = _decimal(value.get("entry_value_usd"), "entry_value_usd")
    costs = _nonnegative_decimal(value.get("costs_usd"), "costs_usd")
    maximum_loss = _nonnegative_decimal(value.get("max_loss_usd"), "max_loss_usd")
    costs_document = {
        "schema": "options_copilot.outcome_cost_evidence.v1",
        "candidate_hash": candidate_hash,
        "cost_contract_hash": cost_contract_hash,
        "costs_usd": costs,
    }
    maximum_loss_document = {
        "schema": "options_copilot.outcome_max_loss_evidence.v1",
        "candidate_hash": candidate_hash,
        "max_loss_usd": maximum_loss,
    }
    normalized = {
        "schema": "options_copilot.outcome_result_authority.v1",
        "candidate_hash": candidate_hash,
        "cost_contract_hash": cost_contract_hash,
        "legs": tuple(legs),
        "entry_value_usd": entry_value,
        "costs_usd": costs,
        "costs_hash": canonical_hash(costs_document),
        "max_loss_usd": maximum_loss,
        "max_loss_evidence_hash": canonical_hash(maximum_loss_document),
    }
    if (
        value.get("costs_hash") != normalized["costs_hash"]
        or value.get("max_loss_evidence_hash")
        != normalized["max_loss_evidence_hash"]
        or value.get("authority_hash") != canonical_hash(normalized)
    ):
        raise OutcomeValidationError("outcome result authority hash is invalid")
    frozen = freeze_json({**normalized, "authority_hash": value["authority_hash"]})
    assert isinstance(frozen, Mapping)
    return frozen


def _result_nonnegative_decimal(value: object, field: str) -> Decimal:
    checked = _decimal(value, field)
    if checked < 0:
        raise OutcomeValidationError(f"{field} cannot be negative")
    return checked


def _normalize_result_quotes(
    value: object,
    *,
    economic_observed_at: datetime,
    candidate_hash: str,
    authority_legs: Sequence[Mapping[str, object]],
    ledger_binding: Mapping[str, object],
) -> tuple[tuple[Mapping[str, object], ...], Decimal]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ) or len(value) != len(authority_legs):
        raise OutcomeValidationError("leg_quotes must be a nonempty sequence")
    normalized: list[Mapping[str, object]] = []
    executable_value = Decimal("0")
    identities: set[str] = set()
    for raw, expected in zip(value, authority_legs, strict=True):
        if not isinstance(raw, Mapping):
            raise OutcomeValidationError("leg_quote must be a mapping")
        _exact_fields(raw, _RESULT_QUOTE_FIELDS, "leg_quote")
        contract_id = _nonblank(raw.get("contract_id"), "contract_id")
        if contract_id in identities:
            raise OutcomeValidationError("leg quote contract identity is duplicated")
        identities.add(contract_id)
        side = _nonblank(raw.get("side"), "side").upper()
        if side not in {"BUY", "SELL"}:
            raise OutcomeValidationError("leg quote side is invalid")
        quantity = _positive_int(raw.get("quantity"), "quantity")
        if (
            contract_id != expected.get("contract_id")
            or side != expected.get("side")
            or quantity != expected.get("quantity")
        ):
            raise OutcomeValidationError("leg quote authority binding mismatch")
        bid = _result_nonnegative_decimal(raw.get("bid"), "bid")
        ask = _result_nonnegative_decimal(raw.get("ask"), "ask")
        if ask < bid:
            raise OutcomeValidationError("leg quote market is crossed")
        observed_at = _timestamp(raw.get("observed_at"), "observed_at")
        if observed_at != economic_observed_at:
            raise OutcomeValidationError("leg quote observation time mismatch")
        source = _nonblank(raw.get("source"), "source")
        source_content_hash = _digest(
            raw.get("source_content_hash"), "source_content_hash"
        )
        if (
            source != ledger_binding.get("provider")
            or source_content_hash != ledger_binding.get("content_hash")
        ):
            raise OutcomeValidationError("leg quote durable source binding mismatch")
        quote_body = {
            "contract_id": contract_id,
            "side": side,
            "quantity": quantity,
            "bid": bid,
            "ask": ask,
            "source": source,
            "source_content_hash": source_content_hash,
            "observed_at": datetime_text(observed_at),
        }
        quote_identity_hash = canonical_hash(
            {
                "schema": "options_copilot.outcome_executable_quote.v1",
                "candidate_hash": candidate_hash,
                **quote_body,
            }
        )
        if raw.get("quote_identity_hash") != quote_identity_hash:
            raise OutcomeValidationError("leg quote identity hash mismatch")
        quote = {**quote_body, "quote_identity_hash": quote_identity_hash}
        normalized.append(quote)
        multiplier = Decimal(quantity * 100)
        executable_value += (
            bid * multiplier if side == "BUY" else -(ask * multiplier)
        )
    return tuple(normalized), executable_value


def _normalize_available_economics(
    value: Mapping[str, object],
    *,
    field: str,
    candidate_hash: str,
    authority: Mapping[str, object],
    ledger_binding: Mapping[str, object],
) -> dict[str, object]:
    observed_at = _timestamp(
        value.get("economic_observed_at"), "economic_observed_at"
    )
    quotes, executable_value = _normalize_result_quotes(
        value.get("leg_quotes"),
        economic_observed_at=observed_at,
        candidate_hash=candidate_hash,
        authority_legs=authority["legs"],
        ledger_binding=ledger_binding,
    )
    entry_value = _decimal(value.get("entry_value_usd"), "entry_value_usd")
    costs = _result_nonnegative_decimal(value.get("costs_usd"), "costs_usd")
    maximum_loss = _result_nonnegative_decimal(
        value.get("max_loss_usd"), "max_loss_usd"
    )
    if (
        entry_value != authority.get("entry_value_usd")
        or costs != authority.get("costs_usd")
        or maximum_loss != authority.get("max_loss_usd")
    ):
        raise OutcomeValidationError("result economics authority binding mismatch")
    supplied_pnl = _decimal(value.get("pnl_usd"), field)
    recomputed_pnl = executable_value - entry_value - costs
    if supplied_pnl != recomputed_pnl:
        raise OutcomeValidationError("result PnL recomputation mismatch")
    return {
        "pnl_usd": supplied_pnl,
        "entry_value_usd": entry_value,
        "costs_usd": costs,
        "max_loss_usd": maximum_loss,
        "economic_observed_at": datetime_text(observed_at),
        "leg_quotes": quotes,
    }


def _normalize_management_result(
    value: Mapping[str, object],
    *,
    binding_field: str,
    binding_hash: str | None,
    candidate_hash: str,
    authority: Mapping[str, object] | None,
    ledger_binding: Mapping[str, object] | None,
) -> Mapping[str, object]:
    common = {
        "schema", binding_field, "status", "recommended_action",
        "thesis_invalidation_hit", "risk_stop_hit", "profit_take_hit",
        "time_stop_hit", "realized_or_executable_pnl_usd",
        "entry_value_usd", "costs_usd", "max_loss_usd",
        "economic_observed_at", "leg_quotes", "reason_code", "provenance",
        "result_hash",
    }
    _exact_fields(value, common, binding_field)
    status = str(value.get("status") or "")
    if status == "UNAVAILABLE":
        nullable = (
            "recommended_action", "thesis_invalidation_hit", "risk_stop_hit",
            "profit_take_hit", "time_stop_hit",
            "realized_or_executable_pnl_usd", "entry_value_usd", "costs_usd",
            "max_loss_usd", "economic_observed_at",
        )
        if any(value.get(name) is not None for name in nullable) or value.get(
            "leg_quotes"
        ) not in ((), []):
            raise OutcomeValidationError("unavailable management economics must be null")
        provenance = value.get("provenance")
        if not isinstance(provenance, Mapping):
            raise OutcomeValidationError("management provenance is invalid")
        _exact_fields(provenance, {"status", "reason_code"}, "provenance")
        reason = _nonblank(value.get("reason_code"), "reason_code").upper()
        if provenance.get("status") != "UNAVAILABLE" or provenance.get(
            "reason_code"
        ) != reason:
            raise OutcomeValidationError("management unavailable provenance mismatch")
        body = {key: item for key, item in value.items() if key != "result_hash"}
        return freeze_json(body)
    if status != "AVAILABLE":
        raise OutcomeValidationError("management result status is invalid")
    if authority is None or ledger_binding is None:
        raise OutcomeValidationError("management result authority is unavailable")
    action = _nonblank(value.get("recommended_action"), "recommended_action").upper()
    if action not in _MANAGEMENT_ACTIONS:
        raise OutcomeValidationError("recommended_action is invalid")
    hits = {
        name: value.get(name)
        for name in (
            "thesis_invalidation_hit", "risk_stop_hit", "profit_take_hit",
            "time_stop_hit",
        )
    }
    if any(not isinstance(item, bool) for item in hits.values()):
        raise OutcomeValidationError("management rule hits must be booleans")
    economics = _normalize_available_economics(
        {
            **value,
            "pnl_usd": value.get("realized_or_executable_pnl_usd"),
        },
        field="realized_or_executable_pnl_usd",
        candidate_hash=candidate_hash,
        authority=authority,
        ledger_binding=ledger_binding,
    )
    provenance = value.get("provenance")
    if not isinstance(provenance, Mapping):
        raise OutcomeValidationError("management provenance is invalid")
    _exact_fields(
        provenance,
        {"status", "quote_batch_hash", "costs_hash", "max_loss_evidence_hash"},
        "provenance",
    )
    if (
        provenance.get("status") != "EVIDENCE_BOUND"
        or provenance.get("quote_batch_hash") != canonical_hash(economics["leg_quotes"])
        or provenance.get("costs_hash") != authority.get("costs_hash")
        or provenance.get("max_loss_evidence_hash")
        != authority.get("max_loss_evidence_hash")
    ):
        raise OutcomeValidationError("management provenance binding is invalid")
    reason = value.get("reason_code")
    if reason is not None:
        _nonblank(reason, "reason_code")
    body = {
        "schema": value["schema"],
        binding_field: binding_hash,
        "status": status,
        "recommended_action": action,
        **hits,
        "realized_or_executable_pnl_usd": economics["pnl_usd"],
        "entry_value_usd": economics["entry_value_usd"],
        "costs_usd": economics["costs_usd"],
        "max_loss_usd": economics["max_loss_usd"],
        "economic_observed_at": economics["economic_observed_at"],
        "leg_quotes": economics["leg_quotes"],
        "reason_code": reason,
        "provenance": freeze_json(provenance),
    }
    return freeze_json(body)


def _normalize_counterfactual_result(
    value: Mapping[str, object],
    *,
    binding_field: str,
    binding_hash: str | None,
    candidate_hash: str,
    authority: Mapping[str, object] | None,
    ledger_binding: Mapping[str, object] | None,
) -> Mapping[str, object]:
    _exact_fields(
        value,
        {"schema", binding_field, "status", "paths", "provenance", "result_hash"},
        binding_field,
    )
    paths_raw = value.get("paths")
    if not isinstance(paths_raw, Sequence) or isinstance(
        paths_raw, (str, bytes, bytearray, memoryview)
    ) or len(paths_raw) != len(_COUNTERFACTUAL_PATHS):
        raise OutcomeValidationError("counterfactual paths are invalid")
    normalized_paths: list[Mapping[str, object]] = []
    top_status = str(value.get("status") or "")
    for expected_path, raw in zip(_COUNTERFACTUAL_PATHS, paths_raw, strict=True):
        if not isinstance(raw, Mapping):
            raise OutcomeValidationError("counterfactual path is invalid")
        _exact_fields(
            raw,
            {
                "path", "status", "pnl_usd", "entry_value_usd", "costs_usd",
                "max_loss_usd", "economic_observed_at", "leg_quotes", "reason_code",
            },
            "counterfactual path",
        )
        if raw.get("path") != expected_path:
            raise OutcomeValidationError("counterfactual path identity is invalid")
        path_status = str(raw.get("status") or "")
        if path_status == "UNAVAILABLE":
            if any(
                raw.get(name) is not None
                for name in (
                    "pnl_usd", "entry_value_usd", "costs_usd", "max_loss_usd",
                    "economic_observed_at",
                )
            ) or raw.get("leg_quotes") not in ((), []):
                raise OutcomeValidationError(
                    "unavailable counterfactual economics must be null"
                )
            reason = _nonblank(raw.get("reason_code"), "reason_code").upper()
            normalized_paths.append(freeze_json({**raw, "reason_code": reason}))
        elif path_status == "AVAILABLE":
            if authority is None or ledger_binding is None:
                raise OutcomeValidationError(
                    "counterfactual result authority is unavailable"
                )
            economics = _normalize_available_economics(
                raw,
                field="pnl_usd",
                candidate_hash=candidate_hash,
                authority=authority,
                ledger_binding=ledger_binding,
            )
            reason = raw.get("reason_code")
            if reason is not None:
                _nonblank(reason, "reason_code")
            normalized_paths.append(
                freeze_json(
                    {
                        "path": expected_path,
                        "status": path_status,
                        **economics,
                        "reason_code": reason,
                    }
                )
            )
        else:
            raise OutcomeValidationError("counterfactual path status is invalid")
    expected_top_status = (
        "AVAILABLE"
        if all(path["status"] == "AVAILABLE" for path in normalized_paths)
        else "UNAVAILABLE"
    )
    if top_status != expected_top_status:
        raise OutcomeValidationError("counterfactual aggregate status mismatch")
    provenance = value.get("provenance")
    if not isinstance(provenance, Mapping):
        raise OutcomeValidationError("counterfactual provenance is invalid")
    if top_status == "AVAILABLE":
        _exact_fields(provenance, {"status", "result_set_hash"}, "provenance")
        if provenance.get("status") != "EVIDENCE_BOUND" or provenance.get(
            "result_set_hash"
        ) != canonical_hash(tuple(normalized_paths)):
            raise OutcomeValidationError("counterfactual provenance binding is invalid")
    else:
        _exact_fields(provenance, {"status", "reason_code"}, "provenance")
        if provenance.get("status") != "UNAVAILABLE":
            raise OutcomeValidationError("counterfactual unavailable provenance invalid")
        _nonblank(provenance.get("reason_code"), "reason_code")
    return freeze_json(
        {
            "schema": value["schema"],
            binding_field: binding_hash,
            "status": top_status,
            "paths": tuple(normalized_paths),
            "provenance": freeze_json(provenance),
        }
    )


def normalize_bound_outcome_result(
    value: object,
    *,
    schema: str,
    binding_field: str,
    binding_hash: str | None,
    candidate_hash: str,
    authority: Mapping[str, object] | None,
    ledger_binding: Mapping[str, object] | None,
) -> tuple[Mapping[str, object] | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, Mapping):
        raise OutcomeValidationError(f"{binding_field} result must be a mapping")
    if (
        value.get("schema") != schema
        or value.get(binding_field) != binding_hash
    ):
        raise OutcomeValidationError(f"{binding_field} result binding is invalid")
    if schema == "options_copilot.position_management_outcome.v1":
        normalized_body = _normalize_management_result(
            value,
            binding_field=binding_field,
            binding_hash=binding_hash,
            candidate_hash=candidate_hash,
            authority=authority,
            ledger_binding=ledger_binding,
        )
    elif schema == "options_copilot.outcome_counterfactual_result.v1":
        normalized_body = _normalize_counterfactual_result(
            value,
            binding_field=binding_field,
            binding_hash=binding_hash,
            candidate_hash=candidate_hash,
            authority=authority,
            ledger_binding=ledger_binding,
        )
    else:
        raise OutcomeValidationError("outcome result schema is unsupported")
    result_hash = _digest(value.get("result_hash"), "result_hash")
    if result_hash != canonical_hash(normalized_body):
        raise OutcomeValidationError(f"{binding_field} result hash mismatch")
    frozen = freeze_json({**normalized_body, "result_hash": result_hash})
    assert isinstance(frozen, Mapping)
    return frozen, result_hash


def _version(value: object, field: str) -> str:
    if not isinstance(value, str) or _VERSION_RE.fullmatch(value) is None:
        raise OutcomeValidationError(f"{field} must be an immutable version like v1")
    return value


def _optional_version(value: object, field: str) -> str | None:
    return None if value is None else _version(value, field)


def _nonblank(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise OutcomeValidationError(f"{field} must be a nonblank string")
    return value


__all__ = [
    "BROKER_OUTCOME_EVIDENCE_HEAD_SCHEMA",
    "BROKER_OUTCOME_EVIDENCE_REF_SCHEMA",
    "BROKER_OUTCOME_EVIDENCE_SCHEMA",
    "BrokerOutcomeEvidenceStore",
    "GENESIS_HASH",
    "INDEPENDENCE_SPEC_SCHEMA",
    "INITIAL_POLICY_HASH",
    "INITIAL_POLICY_VERSION",
    "IndependenceSpecUnavailable",
    "IndependenceSpecValidationError",
    "MixedIndependenceSpecError",
    "OutcomeAggregate",
    "OutcomeClusterAssignment",
    "OutcomeError",
    "OutcomeEvaluationInput",
    "OutcomeIdentityConflict",
    "OUTCOME_HORIZONS",
    "OUTCOME_TARGET_RULES",
    "OutcomeLedgerCorruption",
    "OutcomeRecorder",
    "OutcomeValidationError",
    "SCHEMA_VERSION",
    "StoredOutcome",
    "VerifiedIndependenceSpec",
    "normalize_outcome_observation",
    "normalize_bound_outcome_result",
    "resolve_outcome_horizon",
    "verify_independence_spec",
]
