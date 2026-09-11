"""Durable bounded outcome progress tests."""
from __future__ import annotations

from datetime import datetime, timezone
import sqlite3

import pytest

from options_copilot.learning.progress import (
    OutcomeProgressCorruption,
    OutcomeProgressStore,
)


NOW = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)


def _pending(subject_id: str) -> dict[str, object]:
    return {
        "subject_kind": "PREDICTION",
        "subject_id": subject_id,
        "subject_hash": "a" * 64,
        "symbol": "SPY",
        "horizon": "30M",
        "occurred_at": NOW.isoformat(),
        "thesis_hash": "b" * 64,
        "target": {
            "schema": "options_copilot.outcome_target.v1",
            "subject_kind": "PREDICTION",
            "subject_id": subject_id,
            "subject_hash": "a" * 64,
            "symbol": "SPY",
            "occurred_at": NOW.isoformat(),
            "thesis_hash": "b" * 64,
        },
    }


def test_progress_resumes_after_restart_and_cursors_are_monotonic(tmp_path) -> None:
    path = tmp_path / "outcome-progress.sqlite3"
    first = OutcomeProgressStore(path)
    try:
        written = first.append(
            prediction_cursor=7,
            candidate_cursor=12,
            pending=(_pending("prediction-1"),),
            run={"status": "WAITING_FOR_OBSERVATIONS"},
            recorded_at=NOW,
        )
        assert written.sequence == 1
    finally:
        first.close()

    restarted = OutcomeProgressStore(path)
    try:
        restored = restarted.latest()
        assert restored.prediction_cursor == 7
        assert restored.candidate_cursor == 12
        assert restored.pending[0]["subject_id"] == "prediction-1"
        with pytest.raises(ValueError, match="cannot move backwards"):
            restarted.append(
                prediction_cursor=6,
                candidate_cursor=12,
                pending=(),
                run={"status": "DEGRADED"},
                recorded_at=NOW,
            )
    finally:
        restarted.close()


def test_progress_rows_are_append_only_and_corruption_fails_closed(tmp_path) -> None:
    path = tmp_path / "outcome-progress.sqlite3"
    store = OutcomeProgressStore(path)
    try:
        store.append(
            prediction_cursor=1,
            candidate_cursor=2,
            pending=(),
            run={"status": "COMPLETED"},
            recorded_at=NOW,
        )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            store._connection.execute(
                "UPDATE outcome_progress SET payload_hash=? WHERE sequence=1",
                ("f" * 64,),
            )
        store._connection.execute("DROP TRIGGER outcome_progress_no_update")
        store._connection.execute(
            "UPDATE outcome_progress SET payload_hash=? WHERE sequence=1",
            ("f" * 64,),
        )
        with pytest.raises(OutcomeProgressCorruption, match="trigger"):
            store.assert_integrity()
    finally:
        store.close()
