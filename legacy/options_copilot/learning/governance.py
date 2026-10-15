"""Fail-closed Champion/Challenger governance for self-learning policies.

Learning happens in shadow.  Thirty independent scenarios establish only a
DISCOVERY finding; they never create an A grade or change production.  A
production change needs an immutable report followed by a distinct, explicit
human approval.  Every transition is appended to the decision ledger so the
current champion and rollback history can be reconstructed after restart.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
import re
import threading

from options_copilot.storage import (
    DecisionKind,
    DecisionLedger,
    DecisionRecord,
    PointInTime,
    StoredDecision,
)
from options_copilot.storage.canonical import freeze_json, utc_datetime


MINIMUM_DISCOVERY_SCENARIOS = 30
PRODUCTION_APPROVAL_MARKER = "I_APPROVE_PRODUCTION_PROMOTION"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")


class ModelRole(str, Enum):
    CHAMPION = "CHAMPION"
    CHALLENGER = "CHALLENGER"


class LearningStage(str, Enum):
    COLLECTING = "COLLECTING"
    DISCOVERY = "DISCOVERY"


class GovernanceError(RuntimeError):
    pass


class PromotionBlocked(GovernanceError):
    pass


@dataclass(frozen=True, slots=True)
class RegisteredModel:
    version_id: str
    artifact_hash: str
    role: ModelRole
    registered_at: datetime
    parent_version: str | None
    metadata: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "registered_at", utc_datetime(self.registered_at))
        frozen = freeze_json(self.metadata)
        assert isinstance(frozen, Mapping)
        object.__setattr__(self, "metadata", frozen)


@dataclass(frozen=True, slots=True)
class PromotionAssessment:
    challenger_version: str
    independent_scenarios: int
    minimum_scenarios: int
    stage: LearningStage
    grade: str
    automatic_grade: str | None
    can_auto_promote: bool
    human_report_required: bool
    human_approval_required: bool

    @property
    def discovery_ready(self) -> bool:
        return self.stage is LearningStage.DISCOVERY


@dataclass(frozen=True, slots=True)
class PromotionReport:
    report_id: str
    challenger_version: str
    independent_scenarios: int
    report_hash: str
    generated_at: datetime
    report: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class HumanPromotionApproval:
    approval_id: str
    report_id: str
    report_hash: str
    approved_by: str
    approved_at: datetime
    approval_marker: str


@dataclass(frozen=True, slots=True)
class ChampionTransition:
    from_version: str
    to_version: str
    changed_at: datetime
    reason: str
    transition_hash: str
    rollback: bool = False


class LearningGovernance:
    """Append-only learning control plane backed by :class:`DecisionLedger`."""

    def __init__(self, ledger: DecisionLedger | str | Path) -> None:
        if isinstance(ledger, DecisionLedger):
            self.ledger = ledger
            self._owns_ledger = False
        else:
            self.ledger = DecisionLedger(ledger)
            self._owns_ledger = True
        self._lock = threading.RLock()

    def __enter__(self) -> "LearningGovernance":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_ledger:
            self.ledger.close()

    def register_champion(
        self,
        version_id: str,
        artifact_hash: str,
        *,
        registered_at: datetime,
        metadata: Mapping[str, object] | None = None,
    ) -> RegisteredModel:
        with self._lock:
            if self.current_champion() is not None:
                raise GovernanceError("an initial champion is already registered")
            return self._register_model(
                version_id,
                artifact_hash,
                role=ModelRole.CHAMPION,
                parent_version=None,
                registered_at=registered_at,
                metadata=metadata or {},
            )

    def register_challenger(
        self,
        version_id: str,
        artifact_hash: str,
        *,
        parent_version: str,
        registered_at: datetime,
        metadata: Mapping[str, object] | None = None,
    ) -> RegisteredModel:
        _identifier("parent_version", parent_version)
        if self.model(parent_version) is None:
            raise GovernanceError("challenger parent version is not registered")
        return self._register_model(
            version_id,
            artifact_hash,
            role=ModelRole.CHALLENGER,
            parent_version=parent_version,
            registered_at=registered_at,
            metadata=metadata or {},
        )

    def _register_model(
        self,
        version_id: str,
        artifact_hash: str,
        *,
        role: ModelRole,
        parent_version: str | None,
        registered_at: datetime,
        metadata: Mapping[str, object],
    ) -> RegisteredModel:
        _identifier("version_id", version_id)
        _digest("artifact_hash", artifact_hash)
        if self.model(version_id) is not None:
            raise GovernanceError(f"model {version_id} is already registered")
        at = utc_datetime(registered_at, field="registered_at")
        stored = self.ledger.append(
            DecisionRecord(
                decision_id=f"model:{version_id}",
                kind=DecisionKind.MODEL_REGISTERED,
                scenario_id=f"model:{version_id}",
                source="learning_governance",
                model_version=version_id,
                timing=_same_time(at),
                payload={
                    "artifact_hash": artifact_hash,
                    "role": role.value,
                    "parent_version": parent_version,
                    "metadata": metadata,
                },
            )
        ).decision
        return _stored_to_model(stored)

    def model(self, version_id: str) -> RegisteredModel | None:
        _identifier("version_id", version_id)
        row = self.ledger.get(f"model:{version_id}")
        if row is None:
            return None
        if row.record.kind is not DecisionKind.MODEL_REGISTERED:
            raise GovernanceError("model identity points to a non-model record")
        return _stored_to_model(row)

    def record_shadow_result(
        self,
        challenger_version: str,
        scenario_id: str,
        *,
        outcome: str,
        metrics: Mapping[str, object],
        observed_at: datetime,
        champion_version: str | None = None,
    ) -> StoredDecision:
        challenger = self.model(challenger_version)
        if challenger is None or challenger.role is not ModelRole.CHALLENGER:
            raise GovernanceError("shadow results require a registered challenger")
        _identifier("scenario_id", scenario_id)
        if not isinstance(outcome, str) or not outcome.strip():
            raise ValueError("outcome is required")
        champion = champion_version or self.current_champion()
        if champion is None or self.model(champion) is None:
            raise GovernanceError("shadow comparison requires a registered champion")
        at = utc_datetime(observed_at, field="observed_at")
        return self.ledger.append(
            DecisionRecord(
                decision_id=f"shadow:{challenger_version}:{scenario_id}",
                kind=DecisionKind.SHADOW_RESULT,
                scenario_id=scenario_id,
                source="learning_governance",
                model_version=challenger_version,
                timing=_same_time(at),
                payload={
                    "challenger_version": challenger_version,
                    "champion_version": champion,
                    "outcome": outcome.strip(),
                    "metrics": metrics,
                },
            )
        ).decision

    def assess(self, challenger_version: str) -> PromotionAssessment:
        challenger = self.model(challenger_version)
        if challenger is None or challenger.role is not ModelRole.CHALLENGER:
            raise GovernanceError("assessment requires a registered challenger")
        scenarios = {
            row.record.scenario_id
            for row in self._events(DecisionKind.SHADOW_RESULT)
            if row.record.model_version == challenger_version
        }
        count = len(scenarios)
        stage = (
            LearningStage.DISCOVERY
            if count >= MINIMUM_DISCOVERY_SCENARIOS
            else LearningStage.COLLECTING
        )
        # Deliberately no A grade and no automated route to production.
        return PromotionAssessment(
            challenger_version=challenger_version,
            independent_scenarios=count,
            minimum_scenarios=MINIMUM_DISCOVERY_SCENARIOS,
            stage=stage,
            grade=stage.value,
            automatic_grade=None,
            can_auto_promote=False,
            human_report_required=True,
            human_approval_required=True,
        )

    def create_promotion_report(
        self,
        report_id: str,
        challenger_version: str,
        *,
        report: Mapping[str, object],
        generated_at: datetime,
    ) -> PromotionReport:
        _identifier("report_id", report_id)
        if not isinstance(report, Mapping) or not report:
            raise ValueError("promotion report must be a nonempty mapping")
        assessment = self.assess(challenger_version)
        if not assessment.discovery_ready:
            raise PromotionBlocked(
                f"{assessment.independent_scenarios}/"
                f"{assessment.minimum_scenarios} independent scenarios; DISCOVERY not reached"
            )
        evidence = [
            row.content_hash
            for row in self._events(DecisionKind.SHADOW_RESULT)
            if row.record.model_version == challenger_version
        ]
        at = utc_datetime(generated_at, field="generated_at")
        stored = self.ledger.append(
            DecisionRecord(
                decision_id=f"promotion-report:{report_id}",
                kind=DecisionKind.PROMOTION_REPORT,
                scenario_id=f"promotion:{challenger_version}",
                source="learning_governance",
                model_version=challenger_version,
                timing=_same_time(at),
                payload={
                    "report_id": report_id,
                    "challenger_version": challenger_version,
                    "independent_scenarios": assessment.independent_scenarios,
                    "stage": assessment.stage.value,
                    "automatic_grade": None,
                    "can_auto_promote": False,
                    "evidence_hashes": evidence,
                    "report": report,
                },
            )
        ).decision
        return _stored_to_report(stored)

    def approve_promotion(
        self,
        approval_id: str,
        report_id: str,
        *,
        approved_by: str,
        approved_at: datetime,
        explicit_approval: bool,
        approval_marker: str,
    ) -> HumanPromotionApproval:
        _identifier("approval_id", approval_id)
        _identifier("report_id", report_id)
        _identifier("approved_by", approved_by)
        if not explicit_approval or approval_marker != PRODUCTION_APPROVAL_MARKER:
            raise PromotionBlocked("explicit human production approval marker is required")
        report = self.promotion_report(report_id)
        if report is None:
            raise PromotionBlocked("promotion report does not exist")
        at = utc_datetime(approved_at, field="approved_at")
        if at < report.generated_at:
            raise ValueError("approval cannot predate its report")
        stored = self.ledger.append(
            DecisionRecord(
                decision_id=f"promotion-approval:{approval_id}",
                kind=DecisionKind.PROMOTION_APPROVAL,
                scenario_id=f"promotion:{report.challenger_version}",
                source="human_operator",
                model_version=report.challenger_version,
                related_decision_id=f"promotion-report:{report_id}",
                timing=_same_time(at),
                payload={
                    "approval_id": approval_id,
                    "report_id": report_id,
                    "report_hash": report.report_hash,
                    "approved_by": approved_by,
                    "approval_marker": approval_marker,
                    "explicit_approval": True,
                },
            )
        ).decision
        return _stored_to_approval(stored)

    def promote(
        self,
        challenger_version: str,
        *,
        report_id: str,
        approval_id: str,
        promoted_at: datetime,
        reason: str,
    ) -> ChampionTransition:
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("promotion reason is required")
        report = self.promotion_report(report_id)
        approval = self.promotion_approval(approval_id)
        if report is None or approval is None:
            raise PromotionBlocked("both promotion report and human approval are required")
        if report.challenger_version != challenger_version:
            raise PromotionBlocked("report is for a different challenger")
        if approval.report_id != report.report_id or approval.report_hash != report.report_hash:
            raise PromotionBlocked("human approval is not bound to this immutable report")
        prior_promotions = self._events(DecisionKind.MODEL_PROMOTED)
        if any(
            row.record.payload.get("approval_id") == approval_id
            or row.record.payload.get("report_id") == report_id
            for row in prior_promotions
        ):
            raise PromotionBlocked(
                "promotion report and human approval are single-use; create fresh review evidence"
            )
        current = self.current_champion()
        if current is None:
            raise PromotionBlocked("no current champion is registered")
        if current == challenger_version:
            raise PromotionBlocked("challenger is already the production champion")
        at = utc_datetime(promoted_at, field="promoted_at")
        if at < approval.approved_at:
            raise ValueError("promotion cannot predate human approval")
        event_id = f"promotion:{report_id}:{approval_id}"
        stored = self.ledger.append(
            DecisionRecord(
                decision_id=event_id,
                kind=DecisionKind.MODEL_PROMOTED,
                scenario_id=f"promotion:{challenger_version}",
                source="learning_governance",
                model_version=challenger_version,
                related_decision_id=f"promotion-approval:{approval_id}",
                timing=_same_time(at),
                payload={
                    "from_version": current,
                    "to_version": challenger_version,
                    "report_id": report_id,
                    "report_hash": report.report_hash,
                    "approval_id": approval_id,
                    "approved_by": approval.approved_by,
                    "reason": reason.strip(),
                },
            )
        ).decision
        return _stored_to_transition(stored, rollback=False)

    def rollback(
        self,
        rollback_id: str,
        to_version: str,
        *,
        requested_by: str,
        reason: str,
        rolled_back_at: datetime,
    ) -> ChampionTransition:
        _identifier("rollback_id", rollback_id)
        _identifier("requested_by", requested_by)
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("rollback reason is required")
        current = self.current_champion()
        if current is None:
            raise PromotionBlocked("no current champion exists")
        if to_version == current:
            raise PromotionBlocked("rollback target is already the champion")
        if to_version not in self.champion_history():
            raise PromotionBlocked("rollback target was never a production champion")
        at = utc_datetime(rolled_back_at, field="rolled_back_at")
        stored = self.ledger.append(
            DecisionRecord(
                decision_id=f"rollback:{rollback_id}",
                kind=DecisionKind.MODEL_ROLLED_BACK,
                scenario_id=f"rollback:{current}",
                source="human_operator",
                model_version=to_version,
                timing=_same_time(at),
                payload={
                    "from_version": current,
                    "to_version": to_version,
                    "requested_by": requested_by,
                    "reason": reason.strip(),
                },
            )
        ).decision
        return _stored_to_transition(stored, rollback=True)

    def current_champion(self) -> str | None:
        champion: str | None = None
        events = self._events(
            DecisionKind.MODEL_REGISTERED,
            DecisionKind.MODEL_PROMOTED,
            DecisionKind.MODEL_ROLLED_BACK,
        )
        for row in events:
            payload = row.record.payload
            if row.record.kind is DecisionKind.MODEL_REGISTERED:
                if payload.get("role") == ModelRole.CHAMPION.value and champion is None:
                    champion = row.record.model_version
            else:
                champion = str(payload["to_version"])
        return champion

    def champion_history(self) -> tuple[str, ...]:
        history: list[str] = []
        for row in self._events(
            DecisionKind.MODEL_REGISTERED,
            DecisionKind.MODEL_PROMOTED,
            DecisionKind.MODEL_ROLLED_BACK,
        ):
            if row.record.kind is DecisionKind.MODEL_REGISTERED:
                if row.record.payload.get("role") != ModelRole.CHAMPION.value:
                    continue
                version = row.record.model_version
            else:
                version = str(row.record.payload["to_version"])
            if version not in history:
                history.append(version)
        return tuple(history)

    def promotion_report(self, report_id: str) -> PromotionReport | None:
        _identifier("report_id", report_id)
        row = self.ledger.get(f"promotion-report:{report_id}")
        if row is None:
            return None
        if row.record.kind is not DecisionKind.PROMOTION_REPORT:
            raise GovernanceError("report identity points to the wrong record kind")
        return _stored_to_report(row)

    def promotion_approval(self, approval_id: str) -> HumanPromotionApproval | None:
        _identifier("approval_id", approval_id)
        row = self.ledger.get(f"promotion-approval:{approval_id}")
        if row is None:
            return None
        if row.record.kind is not DecisionKind.PROMOTION_APPROVAL:
            raise GovernanceError("approval identity points to the wrong record kind")
        return _stored_to_approval(row)

    def export_audit(self, destination: str | Path) -> int:
        return self.ledger.export_jsonl(
            destination,
            kinds=(
                DecisionKind.MODEL_REGISTERED,
                DecisionKind.SHADOW_RESULT,
                DecisionKind.PROMOTION_REPORT,
                DecisionKind.PROMOTION_APPROVAL,
                DecisionKind.MODEL_PROMOTED,
                DecisionKind.MODEL_ROLLED_BACK,
            ),
        )

    def _events(self, *kinds: DecisionKind) -> tuple[StoredDecision, ...]:
        after = 0
        output: list[StoredDecision] = []
        while rows := self.ledger.query(kinds=kinds, after_sequence=after, limit=5000):
            output.extend(rows)
            after = rows[-1].sequence
        return tuple(output)


def _same_time(at: datetime) -> PointInTime:
    at = utc_datetime(at)
    return PointInTime(first_seen=at, published=at, ingested=at, asof=at)


def _stored_to_model(row: StoredDecision) -> RegisteredModel:
    payload = row.record.payload
    metadata = payload.get("metadata", {})
    assert isinstance(metadata, Mapping)
    parent = payload.get("parent_version")
    return RegisteredModel(
        version_id=row.record.model_version,
        artifact_hash=str(payload["artifact_hash"]),
        role=ModelRole(str(payload["role"])),
        registered_at=row.record.asof,
        parent_version=None if parent is None else str(parent),
        metadata=metadata,
    )


def _stored_to_report(row: StoredDecision) -> PromotionReport:
    payload = row.record.payload
    report = payload["report"]
    assert isinstance(report, Mapping)
    return PromotionReport(
        report_id=str(payload["report_id"]),
        challenger_version=str(payload["challenger_version"]),
        independent_scenarios=int(payload["independent_scenarios"]),
        report_hash=row.content_hash,
        generated_at=row.record.asof,
        report=report,
    )


def _stored_to_approval(row: StoredDecision) -> HumanPromotionApproval:
    payload = row.record.payload
    return HumanPromotionApproval(
        approval_id=str(payload["approval_id"]),
        report_id=str(payload["report_id"]),
        report_hash=str(payload["report_hash"]),
        approved_by=str(payload["approved_by"]),
        approved_at=row.record.asof,
        approval_marker=str(payload["approval_marker"]),
    )


def _stored_to_transition(row: StoredDecision, *, rollback: bool) -> ChampionTransition:
    payload = row.record.payload
    return ChampionTransition(
        from_version=str(payload["from_version"]),
        to_version=str(payload["to_version"]),
        changed_at=row.record.asof,
        reason=str(payload["reason"]),
        transition_hash=row.content_hash,
        rollback=rollback,
    )


def _identifier(field: str, value: str) -> None:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a valid governance identifier")


def _digest(field: str, value: str) -> None:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase 64-character hash")


__all__ = [
    "ChampionTransition",
    "GovernanceError",
    "HumanPromotionApproval",
    "LearningGovernance",
    "LearningStage",
    "MINIMUM_DISCOVERY_SCENARIOS",
    "ModelRole",
    "PRODUCTION_APPROVAL_MARKER",
    "PromotionAssessment",
    "PromotionBlocked",
    "PromotionReport",
    "RegisteredModel",
]
