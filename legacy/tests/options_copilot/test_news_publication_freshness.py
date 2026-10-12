from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading

import pytest

from options_copilot.news_runtime import NewsCoordinator


NOW = datetime(2026, 9, 9, 1, 0, tzinfo=timezone.utc)


class _SwitchingProvider:
    health = "READY"
    health_reason = None

    def __init__(self) -> None:
        self.calls = 0

    def health_snapshot(self) -> dict[str, object]:
        return {"source": "SEC", "status": self.health}

    def news(self, _symbols: tuple[str, ...], *, limit: int = 50):
        assert limit == 50
        self.calls += 1
        if self.calls > 1:
            raise RuntimeError("provider-secret-must-not-escape")
        return ()


def test_new_failure_is_visible_while_old_read_model_rebuild_is_blocked(
    tmp_path: Path,
) -> None:
    current = [NOW]
    provider = _SwitchingProvider()
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(provider,),
        clock=lambda: current[0],
    )
    entered = threading.Event()
    release = threading.Event()
    try:
        coordinator.refresh_once()
        frozen_decision = coordinator.decision_event_payload()
        frozen_asof = coordinator.news_payload()["asof"]
        original_rebuild = coordinator._rebuild_read_model

        def blocking_rebuild(*args, **kwargs) -> None:
            entered.set()
            assert release.wait(timeout=2)
            original_rebuild(*args, **kwargs)

        coordinator._rebuild_read_model = blocking_rebuild  # type: ignore[method-assign]
        current[0] = NOW + timedelta(minutes=1)
        worker = threading.Thread(target=coordinator.refresh_once, daemon=True)
        worker.start()
        assert entered.wait(timeout=2)

        public = coordinator.news_payload()
        assert public["source_health"][0]["status"] == "DEGRADED"
        assert public["source_health"][0]["reason"] == "REQUEST_FAILED"
        assert public["source_status_observed_at"] == current[0].isoformat()
        assert public["refresh_progress"]["status"] == "RUNNING"
        assert public["refresh_progress"]["stage"] == "READ_MODEL_REBUILD"
        assert public["asof"] == frozen_asof
        assert public["refresh_progress"]["read_model_asof"] == frozen_asof
        assert coordinator.decision_event_payload() == frozen_decision
        assert "provider-secret" not in repr(public)
    finally:
        release.set()
        if "worker" in locals():
            worker.join(timeout=2)
        coordinator.close()


def test_publication_reages_at_cadence_boundary_without_laundering_asof(
    tmp_path: Path,
) -> None:
    current = [NOW]
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(_SwitchingProvider(),),
        clock=lambda: current[0],
        cadence_path=tmp_path / "cadence.json",
    )
    try:
        coordinator.refresh_once()
        frozen_asof = coordinator.news_payload()["asof"]
        current[0] = NOW + timedelta(seconds=90)
        boundary = coordinator.news_payload()
        assert boundary["source_health"][0]["status"] == "READY"
        assert boundary["source_runtime"][0]["cadence_status"] == "DUE"
        assert boundary["source_runtime"][0]["freshness"] == "CURRENT"

        current[0] += timedelta(microseconds=1)
        expired = coordinator.news_payload()
        assert expired["source_health"][0]["status"] == "DEGRADED"
        assert expired["source_health"][0]["reason"] == "SOURCE_STATUS_STALE"
        assert expired["source_runtime"][0]["freshness"] == "STALE"
        assert expired["asof"] == frozen_asof
    finally:
        coordinator.close()


def test_public_reader_only_clones_and_reages_captured_snapshot(
    tmp_path: Path,
) -> None:
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(_SwitchingProvider(),),
        clock=lambda: NOW,
    )
    try:
        coordinator.refresh_once()

        def forbidden(*_args, **_kwargs):
            raise AssertionError("reader crossed an owner-lane boundary")

        coordinator._cadence.projections = forbidden  # type: ignore[method-assign]
        coordinator._cadence.due = forbidden  # type: ignore[method-assign]
        coordinator._cadence.record = forbidden  # type: ignore[method-assign]
        coordinator.evidence_store.query = forbidden  # type: ignore[method-assign]

        lock_acquired = threading.Event()
        release = threading.Event()

        def hold_refresh_lock() -> None:
            with coordinator._refresh_lock:
                lock_acquired.set()
                assert release.wait(timeout=2)

        holder = threading.Thread(target=hold_refresh_lock, daemon=True)
        holder.start()
        assert lock_acquired.wait(timeout=2)
        try:
            assert coordinator.news_payload()["source_status_scope"] == (
                "LIVE_ACQUISITION_DIAGNOSTIC"
            )
            assert coordinator.calendar_payload()["source_status_scope"] == (
                "LIVE_ACQUISITION_DIAGNOSTIC"
            )
        finally:
            release.set()
            holder.join(timeout=2)
    finally:
        coordinator.close()


def test_failed_refresh_stage_is_finalized_without_exception_text(
    tmp_path: Path,
) -> None:
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(_SwitchingProvider(),),
        clock=lambda: NOW,
    )
    try:
        def failed_rebuild(*_args, **_kwargs) -> None:
            raise RuntimeError("database-secret-must-not-escape")

        coordinator._rebuild_read_model = failed_rebuild  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="database-secret"):
            coordinator.refresh_once()

        payload = coordinator.news_payload()
        progress = payload["refresh_progress"]
        assert progress["status"] == "FAILED"
        assert progress["stage"] == "READ_MODEL_REBUILD"
        assert progress["cycle_completed_at"] == NOW.isoformat()
        assert progress["stage_started_at"] is None
        assert progress["stage_durations_ms"]["READ_MODEL_REBUILD"] >= 0
        assert "database-secret" not in repr(payload)
    finally:
        coordinator.close()


def test_backwards_clock_fails_source_diagnostics_closed(tmp_path: Path) -> None:
    current = [NOW]
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(_SwitchingProvider(),),
        clock=lambda: current[0],
    )
    try:
        coordinator.refresh_once()
        current[0] = NOW - timedelta(seconds=1)
        payload = coordinator.news_payload()
        assert payload["source_health"][0]["status"] == "DEGRADED"
        assert payload["source_health"][0]["reason"] == (
            "SOURCE_STATUS_CLOCK_REGRESSED"
        )
        assert payload["source_runtime"][0]["cadence_status"] == "DUE"
        assert payload["source_runtime"][0]["freshness"] == "STALE"
    finally:
        coordinator.close()


def test_future_persisted_cadence_observation_fails_closed_without_health_rows(
    tmp_path: Path,
) -> None:
    current = [NOW]
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: current[0],
        cadence_path=tmp_path / "cadence.json",
    )
    try:
        with coordinator._state_lock:
            diagnostic = coordinator._publication_diagnostic
            coordinator._publication_diagnostic = diagnostic.capture_sources(
                source_health=(),
                source_runtime=(
                    {
                        "source_id": "SEC",
                        "source_kind": "NEWS",
                        "configured": True,
                        "cadence_status": "WAITING",
                        "freshness": "CURRENT",
                        "interval_seconds": 90,
                        "last_attempt": (NOW + timedelta(seconds=1)).isoformat(),
                        "last_success": (NOW + timedelta(seconds=1)).isoformat(),
                        "next_due": (NOW + timedelta(seconds=91)).isoformat(),
                    },
                ),
                observed_at=NOW,
                default_freshness_seconds=90,
            )

        row = coordinator.news_payload()["source_runtime"][0]
        assert row["cadence_status"] == "DUE"
        assert row["freshness"] == "STALE"
        assert row["failure_code"] == "SOURCE_STATUS_CLOCK_REGRESSED"
    finally:
        coordinator.close()


def test_invalid_publication_clock_keeps_prior_body_and_metadata_atomic(
    tmp_path: Path,
) -> None:
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: NOW,
    )
    try:
        before_news = coordinator.news_payload()
        before_calendar = coordinator.calendar_payload()
        before_decision = coordinator.decision_event_payload()
        coordinator._clock = lambda: datetime(2026, 9, 9, 1, 1)  # type: ignore[assignment]

        with coordinator._refresh_lock:
            with pytest.raises(ValueError, match="timezone-aware"):
                coordinator._rebuild_read_model(asof=NOW, analysis_budget=0)

        coordinator._clock = lambda: NOW  # type: ignore[assignment]
        after_news = coordinator.news_payload()
        after_calendar = coordinator.calendar_payload()
        assert after_news["asof"] == before_news["asof"]
        assert after_news["read_model_published_at"] == (
            before_news["read_model_published_at"]
        )
        assert after_calendar["asof"] == before_calendar["asof"]
        assert after_calendar["read_model_published_at"] == (
            before_calendar["read_model_published_at"]
        )
        assert coordinator.decision_event_payload() == before_decision
    finally:
        coordinator.close()


def test_cadence_projection_failure_keeps_entire_prior_generation_atomic(
    tmp_path: Path,
) -> None:
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        news_providers=(_SwitchingProvider(),),
        clock=lambda: NOW,
    )
    try:
        coordinator.refresh_once()
        before_news = coordinator.news_payload()
        before_calendar = coordinator.calendar_payload()
        before_decision = coordinator.decision_event_payload()
        before_equity = coordinator.decision_equity_news_payload()
        original_projection = coordinator._cadence.projections

        def failed_projection(*_args, **_kwargs):
            raise RuntimeError("cadence-secret-must-not-escape")

        coordinator._cadence.projections = failed_projection  # type: ignore[method-assign]
        try:
            with coordinator._refresh_lock:
                with pytest.raises(RuntimeError, match="cadence-secret"):
                    coordinator._rebuild_read_model(asof=NOW, analysis_budget=0)
        finally:
            coordinator._cadence.projections = original_projection  # type: ignore[method-assign]

        after_news = coordinator.news_payload()
        after_calendar = coordinator.calendar_payload()
        assert after_news == before_news
        assert after_calendar == before_calendar
        assert coordinator.decision_event_payload() == before_decision
        assert coordinator.decision_equity_news_payload() == before_equity
        assert "cadence-secret" not in repr(after_news)
    finally:
        coordinator.close()


def test_read_model_publish_uses_one_cadence_projection_for_whole_generation(
    tmp_path: Path,
) -> None:
    current = [NOW]
    coordinator = NewsCoordinator(
        tmp_path / "evidence.sqlite3",
        clock=lambda: current[0],
    )
    try:
        original_projection = coordinator._cadence.projections
        calls = 0

        def second_call_fails(*, now: datetime):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("second cadence projection is a torn publish")
            return original_projection(now=now)

        coordinator._cadence.projections = second_call_fails  # type: ignore[method-assign]
        current[0] = NOW + timedelta(minutes=1)
        try:
            with coordinator._refresh_lock:
                coordinator._rebuild_read_model(
                    asof=current[0],
                    analysis_budget=0,
                )
        finally:
            coordinator._cadence.projections = original_projection  # type: ignore[method-assign]

        assert calls == 1
        news = coordinator.news_payload()
        calendar = coordinator.calendar_payload()
        decision = coordinator.decision_event_payload()
        assert news["asof"] == current[0].isoformat()
        assert calendar["asof"] == current[0].isoformat()
        assert news["refresh_progress"]["read_model_asof"] == current[0].isoformat()
        assert news["source_runtime"] == calendar["source_runtime"]
        assert decision["news_asof"] == decision["calendar_asof"] == current[0].isoformat()
    finally:
        coordinator.close()
