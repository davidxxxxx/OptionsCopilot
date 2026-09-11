"""Exact-slot weekly-brief lifecycle over the append-only store."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
import threading

from options_copilot.decision.gates import ProvisionalWatchGatePreview
from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.storage.canonical import canonical_hash

from .research_session_calendar import ResearchSessionCalendarSnapshot
from .weekly_brief import (
    WeeklyBrief,
    WeeklyBriefEvidenceItem,
    WeeklyBriefSlotDecision,
    WeeklyBriefSourceHealth,
    evaluate_weekly_brief_slot,
    weekly_brief_idempotency_key,
)
from .weekly_brief_builder import build_weekly_brief_inputs
from .weekly_brief_store import (
    StoredWeeklyBrief,
    WeeklyBriefStore,
    WeeklyBriefStoreConflict,
)


WEEKLY_BRIEF_READ_MODEL_SCHEMA = "options_copilot.weekly_brief_read_model.v1"


class WeeklyBriefRuntime:
    """Serialize slot evaluation and expose only persisted or honest NOT_RUN state."""

    def __init__(self, store: WeeklyBriefStore) -> None:
        if not isinstance(store, WeeklyBriefStore):
            raise TypeError("store must be a WeeklyBriefStore")
        self._store = store
        self._lock = threading.RLock()
        self._last_result: dict[str, object] | None = None

    def evaluate_and_append(
        self,
        *,
        scheduled_for: datetime,
        evaluated_at: datetime,
        official_session_dates: Sequence[date],
        calendar_hash: str,
        evidence_items: Sequence[WeeklyBriefEvidenceItem],
        source_health: Sequence[WeeklyBriefSourceHealth],
        watch_items: Sequence[ProvisionalWatchGatePreview],
    ) -> dict[str, object]:
        """Evaluate exactly once under the store lock; a missed slot never replays."""

        with self._lock:
            checked_at = _aware_datetime(evaluated_at)
            slot = evaluate_weekly_brief_slot(
                scheduled_for=scheduled_for,
                evaluated_at=checked_at,
                official_session_dates=official_session_dates,
                calendar_hash=calendar_hash,
            )
            if not slot.produce_allowed:
                result = self._persist_not_run(slot)
                self._last_result = result
                return result
            idempotency_key = weekly_brief_idempotency_key(
                week_start=slot.week_start,
                cutoff_at=slot.cutoff_at,
            )
            existing = self._store.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                result = _stored_read_model(existing, inserted=False)
                if result is None:
                    raise RuntimeError("stored weekly outcome is not a read model")
                self._last_result = result
                return result
            try:
                brief = WeeklyBrief.build(
                    slot=slot,
                    evidence_items=evidence_items,
                    source_health=source_health,
                    watch_items=watch_items,
                )
            except (TypeError, ValueError) as exc:
                reason = (
                    "WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"
                    if "WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE" in str(exc)
                    else "WEEKLY_BRIEF_BUILD_FAILED"
                )
                result = self._persist_not_run(slot, reasons=(reason,))
                self._last_result = result
                return result
            try:
                stored = self._store.append(brief)
            except WeeklyBriefStoreConflict:
                existing = self._store.get_by_idempotency_key(idempotency_key)
                if existing is None:
                    raise
                result = _stored_read_model(existing, inserted=False)
                if result is None:
                    raise RuntimeError("stored weekly outcome is not a read model")
                self._last_result = result
                return result
            result = {
                **brief.as_dict(),
                "read_model_schema": WEEKLY_BRIEF_READ_MODEL_SCHEMA,
                "persistence": {
                    "sequence": stored.record.sequence,
                    "row_hash": stored.record.row_hash,
                    "inserted": stored.inserted,
                    "append_only": True,
                },
            }
            self._last_result = result
            return result

    def evaluate_research_read_models(
        self,
        *,
        calendar: ResearchSessionCalendarSnapshot,
        scheduled_for: datetime,
        evaluated_at: datetime,
        news_payload: Mapping[str, object],
        calendar_payload: Mapping[str, object],
    ) -> dict[str, object]:
        """Build from the bounded offline calendar and cached evidence only."""

        if not isinstance(calendar, ResearchSessionCalendarSnapshot):
            raise TypeError("calendar must be a ResearchSessionCalendarSnapshot")
        if not isinstance(news_payload, Mapping):
            raise TypeError("news_payload must be a mapping")
        if not isinstance(calendar_payload, Mapping):
            raise TypeError("calendar_payload must be a mapping")
        return self._evaluate_research_read_models(
            calendar=calendar,
            calendar_hash_valid=calendar.verify_hash(),
            scheduled_for=scheduled_for,
            evaluated_at=evaluated_at,
            news_payload=news_payload,
            calendar_payload=calendar_payload,
        )

    def _evaluate_research_read_models(
        self,
        *,
        calendar: ResearchSessionCalendarSnapshot,
        calendar_hash_valid: bool,
        scheduled_for: datetime,
        evaluated_at: datetime,
        news_payload: Mapping[str, object],
        calendar_payload: Mapping[str, object],
    ) -> dict[str, object]:
        scheduled = _aware_datetime(scheduled_for).astimezone(US_OPTIONS_TIMEZONE)
        calendar_valid = calendar.ready and calendar_hash_valid
        if calendar_valid:
            checked_calendar = calendar
        else:
            reasons = set(calendar.reason_codes)
            if not calendar_hash_valid:
                reasons.add("CALENDAR_HASH_INVALID")
            checked_calendar = ResearchSessionCalendarSnapshot(
                status="UNAVAILABLE",
                reason_codes=tuple(sorted(reasons or {"CALENDAR_PROVIDER_UNAVAILABLE"})),
                week_start=calendar.week_start,
                # Preserve declared dates so the exact slot remains
                # evaluable, while UNAVAILABLE health prevents production.
                sessions=calendar.sessions,
                source=calendar.source,
                effective_at=calendar.effective_at,
                calendar_hash=calendar.calendar_hash,
            )
        inputs = build_weekly_brief_inputs(
            cutoff_at=scheduled,
            calendar=checked_calendar,
            news_payload=news_payload,
            calendar_payload=calendar_payload,
        )
        return self.evaluate_and_append(
            scheduled_for=scheduled,
            evaluated_at=evaluated_at,
            official_session_dates=checked_calendar.sessions,
            calendar_hash=checked_calendar.calendar_hash,
            evidence_items=inputs.evidence_items,
            source_health=inputs.source_health,
            watch_items=inputs.watch_items,
        )

    def _persist_not_run(
        self,
        slot: WeeklyBriefSlotDecision,
        *,
        reasons: Sequence[str] | None = None,
    ) -> dict[str, object]:
        idempotency_key = weekly_brief_idempotency_key(
            week_start=slot.week_start,
            cutoff_at=slot.cutoff_at,
        )
        existing = self._store.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            restored = _stored_read_model(existing, inserted=False)
            if restored is None:
                raise RuntimeError("stored weekly outcome is not a read model")
            return restored
        core = _not_run_core(slot, reasons=reasons)
        content_hash = canonical_hash(core)
        try:
            stored = self._store.append_document(
                idempotency_key=idempotency_key,
                content_hash=content_hash,
                payload={
                    "schema": "options_copilot.weekly_brief_append.v1",
                    "record_type": "WEEKLY_BRIEF_NOT_RUN",
                    "idempotency_key": idempotency_key,
                    "content_hash": content_hash,
                    "weekly_brief": {**core, "content_hash": content_hash},
                },
            )
        except WeeklyBriefStoreConflict:
            existing = self._store.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            restored = _stored_read_model(existing, inserted=False)
            if restored is None:
                raise RuntimeError("stored weekly outcome is not a read model")
            return restored
        restored = _stored_read_model(stored.record, inserted=stored.inserted)
        if restored is None:
            raise RuntimeError("stored weekly outcome is not a read model")
        return restored

    def read_model(self) -> dict[str, object]:
        with self._lock:
            if self._last_result is not None:
                return dict(self._last_result)
            latest = self._store.latest()
            if latest is not None:
                restored = _stored_read_model(latest, inserted=False)
                if restored is not None:
                    return restored
            return {
                "read_model_schema": WEEKLY_BRIEF_READ_MODEL_SCHEMA,
                "status": "NOT_RUN",
                "decision": "OBSERVATION_ONLY",
                "decision_authority": "SUPPORTING_ONLY",
                "execution_allowed": False,
                "review_allowed": False,
                "combination_generation_allowed": False,
                "reason_codes": ["WEEKLY_BRIEF_NOT_AVAILABLE"],
                "weekly_brief": None,
                "persistence": {
                    "sequence": None,
                    "row_hash": None,
                    "inserted": False,
                    "append_only": True,
                },
            }


def _not_run_core(
    slot: WeeklyBriefSlotDecision,
    *,
    reasons: Sequence[str] | None = None,
) -> dict[str, object]:
    return {
        "read_model_schema": WEEKLY_BRIEF_READ_MODEL_SCHEMA,
        "status": "NOT_RUN",
        "decision": "OBSERVATION_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "execution_allowed": False,
        "review_allowed": False,
        "combination_generation_allowed": False,
        "reason_codes": list(slot.reason_codes if reasons is None else reasons),
        "slot": slot.as_dict(),
        "weekly_brief": None,
    }


def _stored_read_model(
    record: StoredWeeklyBrief,
    *,
    inserted: bool,
) -> dict[str, object] | None:
    raw = record.payload.get("weekly_brief")
    if not isinstance(raw, Mapping):
        return None
    return {
        **dict(raw),
        "read_model_schema": WEEKLY_BRIEF_READ_MODEL_SCHEMA,
        "persistence": {
            "sequence": record.sequence,
            "row_hash": record.row_hash,
            "inserted": inserted,
            "append_only": True,
        },
    }


def _aware_datetime(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise TypeError("value must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


__all__ = ["WEEKLY_BRIEF_READ_MODEL_SCHEMA", "WeeklyBriefRuntime"]
