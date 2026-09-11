"""Immutable publication diagnostics for bounded news read models."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
from typing import Mapping, Sequence


_UTC = timezone.utc
_MAX_SOURCE_ROWS = 64
_PROGRESS_STATUSES = frozenset({"IDLE", "RUNNING", "COMPLETED", "FAILED"})
_PROGRESS_STAGES = frozenset(
    {
        "IDLE",
        "LOCAL_ANALYSIS_RESTORE",
        "NEWS_PROVIDERS",
        "NEWS_APPEND",
        "IBKR_BINDINGS",
        "PRESELECTIONS",
        "CALENDAR_PROVIDERS",
        "READ_MODEL_REBUILD",
    }
)


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("publication timestamps must be timezone-aware")
    return value.astimezone(_UTC)


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(_UTC)


def _rows_json(rows: Sequence[Mapping[str, object]]) -> str:
    bounded = [dict(row) for row in rows[:_MAX_SOURCE_ROWS]]
    return json.dumps(bounded, sort_keys=True, separators=(",", ":"))


def _load_rows(value: str) -> list[dict[str, object]]:
    loaded = json.loads(value)
    return [dict(row) for row in loaded if isinstance(row, Mapping)]


@dataclass(frozen=True, slots=True)
class RefreshProgress:
    """One immutable refresh-cycle progress observation."""

    status: str
    stage: str
    cycle_started_at: datetime | None = None
    cycle_completed_at: datetime | None = None
    stage_started_at: datetime | None = None
    stage_started_monotonic: float | None = None
    stage_durations_ms: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        if self.status not in _PROGRESS_STATUSES:
            raise ValueError("invalid refresh progress status")
        if self.stage not in _PROGRESS_STAGES:
            raise ValueError("invalid refresh progress stage")
        for value in (
            self.cycle_started_at,
            self.cycle_completed_at,
            self.stage_started_at,
        ):
            if value is not None:
                _aware(value)
        if self.stage_started_monotonic is not None and self.stage_started_monotonic < 0:
            raise ValueError("invalid monotonic stage start")


@dataclass(frozen=True, slots=True)
class PublicationDiagnostic:
    """JSON-frozen source rows plus immutable publication/progress clocks."""

    observed_at: datetime
    source_health_json: str = "[]"
    source_runtime_json: str = "[]"
    source_deadlines_json: str = "{}"
    read_model_published_at: datetime | None = None
    read_model_asof: datetime | None = None
    action_expires_at: datetime | None = None
    progress: RefreshProgress = RefreshProgress("IDLE", "IDLE")

    def __post_init__(self) -> None:
        _aware(self.observed_at)
        if self.read_model_published_at is not None:
            _aware(self.read_model_published_at)
        if self.read_model_asof is not None:
            _aware(self.read_model_asof)
        if self.action_expires_at is not None:
            _aware(self.action_expires_at)

    def capture_sources(
        self,
        *,
        source_health: Sequence[Mapping[str, object]],
        source_runtime: Sequence[Mapping[str, object]],
        observed_at: datetime,
        default_freshness_seconds: int,
    ) -> PublicationDiagnostic:
        """Freeze sanitized owner-lane observations and their expiry boundaries."""

        observed = _aware(observed_at)
        prior_health = _load_rows(self.source_health_json)
        merged_health = {
            (str(row.get("source") or ""), str(row.get("source_kind") or "")): row
            for row in prior_health
        }
        for row in source_health:
            merged_health[
                (str(row.get("source") or ""), str(row.get("source_kind") or ""))
            ] = dict(row)
        health_rows = list(merged_health.values())[:_MAX_SOURCE_ROWS]
        runtime_rows = [dict(row) for row in source_runtime[:_MAX_SOURCE_ROWS]]
        runtime_by_lane = {
            (str(row.get("source_id") or ""), str(row.get("source_kind") or "")): row
            for row in runtime_rows
        }
        deadlines: dict[str, str] = {}
        for index, row in enumerate(health_rows):
            source = str(row.get("source") or "")
            kind = str(row.get("source_kind") or "")
            runtime = runtime_by_lane.get((source, kind), {})
            deadline = _timestamp(runtime.get("next_due"))
            if deadline is None:
                asof = _timestamp(row.get("asof")) or observed
                interval = runtime.get("interval_seconds", default_freshness_seconds)
                if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
                    interval = default_freshness_seconds
                deadline = asof + timedelta(seconds=interval)
            deadlines[str(index)] = deadline.isoformat()
        return replace(
            self,
            observed_at=observed,
            source_health_json=_rows_json(health_rows),
            source_runtime_json=_rows_json(runtime_rows),
            source_deadlines_json=json.dumps(deadlines, sort_keys=True, separators=(",", ":")),
        )

    def publish_read_model(
        self,
        *,
        published_at: datetime,
        read_model_asof: datetime,
        action_expires_at: datetime | None,
    ) -> PublicationDiagnostic:
        return replace(
            self,
            read_model_published_at=_aware(published_at),
            read_model_asof=_aware(read_model_asof),
            action_expires_at=(
                None if action_expires_at is None else _aware(action_expires_at)
            ),
        )

    def with_progress(self, progress: RefreshProgress) -> PublicationDiagnostic:
        return replace(self, progress=progress)

    def project(self, *, evaluated_at: datetime, monotonic_now: float) -> dict[str, object]:
        """Clone and re-age captured facts without touching providers or persistence."""

        evaluated = _aware(evaluated_at)
        source_health = _load_rows(self.source_health_json)
        source_runtime = _load_rows(self.source_runtime_json)
        source_times = [
            timestamp
            for row in source_health
            if (timestamp := _timestamp(row.get("asof"))) is not None
        ]
        cadence_observation_times = [
            timestamp
            for row in source_runtime
            for field in ("last_attempt", "last_success")
            if (timestamp := _timestamp(row.get(field))) is not None
        ]
        clock_regressed = (
            evaluated < self.observed_at
            or any(evaluated < timestamp for timestamp in source_times)
            or any(evaluated < timestamp for timestamp in cadence_observation_times)
        )
        deadlines_raw = json.loads(self.source_deadlines_json)
        for index, row in enumerate(source_health):
            if row.get("status") != "READY":
                continue
            deadline = _timestamp(deadlines_raw.get(str(index)))
            if clock_regressed:
                row["status"] = "DEGRADED"
                row["reason"] = "SOURCE_STATUS_CLOCK_REGRESSED"
            elif deadline is not None and evaluated > deadline:
                row["status"] = "DEGRADED"
                row["reason"] = "SOURCE_STATUS_STALE"
        for row in source_runtime:
            if row.get("configured") is not True:
                continue
            next_due = _timestamp(row.get("next_due"))
            last_success = _timestamp(row.get("last_success"))
            interval = row.get("interval_seconds")
            freshness_deadline = (
                last_success + timedelta(seconds=interval)
                if last_success is not None
                and isinstance(interval, int)
                and not isinstance(interval, bool)
                and interval > 0
                else None
            )
            if clock_regressed:
                row["cadence_status"] = "DUE"
                row["freshness"] = "STALE"
                row["failure_code"] = "SOURCE_STATUS_CLOCK_REGRESSED"
            else:
                if next_due is None or evaluated >= next_due:
                    row["cadence_status"] = "DUE"
                if freshness_deadline is not None and evaluated > freshness_deadline:
                    row["freshness"] = "STALE"

        progress = self.progress
        stage_elapsed_ms = 0.0
        if progress.status == "RUNNING" and progress.stage_started_monotonic is not None:
            stage_elapsed_ms = max(
                0.0,
                (float(monotonic_now) - progress.stage_started_monotonic) * 1000,
            )
        cycle_elapsed_ms = sum(duration for _stage, duration in progress.stage_durations_ms)
        if progress.status == "RUNNING":
            cycle_elapsed_ms += stage_elapsed_ms
        return {
            "source_status_scope": "LIVE_ACQUISITION_DIAGNOSTIC",
            "source_status_observed_at": self.observed_at.isoformat(),
            "source_status_evaluated_at": evaluated.isoformat(),
            "source_health": source_health,
            "source_runtime": source_runtime,
            "read_model_published_at": (
                None
                if self.read_model_published_at is None
                else self.read_model_published_at.isoformat()
            ),
            "refresh_progress": {
                "status": progress.status,
                "stage": progress.stage,
                "cycle_started_at": (
                    None
                    if progress.cycle_started_at is None
                    else progress.cycle_started_at.isoformat()
                ),
                "cycle_completed_at": (
                    None
                    if progress.cycle_completed_at is None
                    else progress.cycle_completed_at.isoformat()
                ),
                "stage_started_at": (
                    None
                    if progress.stage_started_at is None
                    else progress.stage_started_at.isoformat()
                ),
                "elapsed_ms": round(cycle_elapsed_ms, 3),
                "stage_elapsed_ms": round(stage_elapsed_ms, 3),
                "stage_durations_ms": {
                    stage: round(duration, 3)
                    for stage, duration in progress.stage_durations_ms
                },
                "read_model_asof": (
                    None
                    if self.read_model_asof is None
                    else self.read_model_asof.isoformat()
                ),
            },
            "action_expires_at": (
                None if self.action_expires_at is None else self.action_expires_at.isoformat()
            ),
        }


__all__ = ["PublicationDiagnostic", "RefreshProgress"]
