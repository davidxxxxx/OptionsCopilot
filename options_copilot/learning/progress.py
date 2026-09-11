"""Append-only durable progress for bounded outcome reconciliation."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
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


GENESIS_HASH = "0" * 64
MAXIMUM_PENDING_ITEMS = 5000
MAXIMUM_PAYLOAD_BYTES = 16 * 1024 * 1024


class OutcomeProgressCorruption(RuntimeError):
    """The append-only progress chain no longer verifies."""


@dataclass(frozen=True, slots=True)
class OutcomeProgressState:
    prediction_cursor: int = 0
    candidate_cursor: int = 0
    pending: tuple[Mapping[str, object], ...] = ()
    sequence: int = 0
    chain_hash: str = GENESIS_HASH
    recorded_at: datetime | None = None


class OutcomeProgressStore:
    """Persist complete processor checkpoints as an immutable hash chain."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
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
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._migrate()
            self.assert_integrity()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def latest(self) -> OutcomeProgressState:
        with self._lock:
            self.assert_integrity()
            row = self._connection.execute(
                "SELECT * FROM outcome_progress ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return OutcomeProgressState()
            payload = json.loads(str(row["payload_json"]))
            pending = payload.get("pending", ())
            if not isinstance(pending, list):
                raise OutcomeProgressCorruption("pending progress is invalid")
            frozen = freeze_json(pending)
            if not isinstance(frozen, tuple) or any(
                not isinstance(item, Mapping) for item in frozen
            ):
                raise OutcomeProgressCorruption("pending progress rows are invalid")
            return OutcomeProgressState(
                prediction_cursor=int(payload["prediction_cursor"]),
                candidate_cursor=int(payload["candidate_cursor"]),
                pending=frozen,
                sequence=int(row["sequence"]),
                chain_hash=str(row["chain_hash"]),
                recorded_at=utc_datetime(
                    datetime.fromisoformat(str(row["recorded_at"])),
                    field="recorded_at",
                ),
            )

    def append(
        self,
        *,
        prediction_cursor: int,
        candidate_cursor: int,
        pending: Sequence[Mapping[str, object]],
        run: Mapping[str, object],
        recorded_at: datetime | None = None,
    ) -> OutcomeProgressState:
        if min(prediction_cursor, candidate_cursor) < 0:
            raise ValueError("progress cursors cannot be negative")
        if len(pending) > MAXIMUM_PENDING_ITEMS:
            raise ValueError("pending progress exceeds the durable bound")
        at = utc_datetime(
            recorded_at or datetime.now(timezone.utc),
            field="recorded_at",
        )
        frozen_pending = freeze_json(tuple(pending))
        if not isinstance(frozen_pending, tuple) or any(
            not isinstance(item, Mapping) for item in frozen_pending
        ):
            raise TypeError("pending must contain mappings")
        payload = {
            "schema": "options_copilot.outcome_progress.v1",
            "prediction_cursor": prediction_cursor,
            "candidate_cursor": candidate_cursor,
            "pending": thaw_json(frozen_pending),
            "run": thaw_json(freeze_json(run)),
        }
        payload_json = canonical_json(payload)
        if len(payload_json.encode("utf-8")) > MAXIMUM_PAYLOAD_BYTES:
            raise ValueError("outcome progress checkpoint is too large")
        payload_hash = canonical_hash(payload)
        with self._lock:
            self.assert_integrity()
            tail = self._connection.execute(
                "SELECT sequence, chain_hash FROM outcome_progress "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            previous_hash = GENESIS_HASH if tail is None else str(tail["chain_hash"])
            if tail is not None:
                current = self.latest()
                if (
                    prediction_cursor < current.prediction_cursor
                    or candidate_cursor < current.candidate_cursor
                ):
                    raise ValueError("outcome progress cursor cannot move backwards")
            chain_hash = canonical_hash(
                {
                    "schema": "options_copilot.outcome_progress_chain.v1",
                    "sequence": sequence,
                    "previous_hash": previous_hash,
                    "payload_hash": payload_hash,
                    "recorded_at": datetime_text(at),
                }
            )
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._connection.execute(
                    "INSERT INTO outcome_progress VALUES(?,?,?,?,?,?)",
                    (
                        sequence,
                        payload_json,
                        payload_hash,
                        previous_hash,
                        chain_hash,
                        datetime_text(at),
                    ),
                )
                self._connection.execute("COMMIT")
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
        return OutcomeProgressState(
            prediction_cursor=prediction_cursor,
            candidate_cursor=candidate_cursor,
            pending=frozen_pending,
            sequence=sequence,
            chain_hash=chain_hash,
            recorded_at=at,
        )

    def assert_integrity(self) -> None:
        if self._closed:
            raise RuntimeError("outcome progress store is closed")
        triggers = {
            str(row[0])
            for row in self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        }
        if not {
            "outcome_progress_no_update",
            "outcome_progress_no_delete",
        }.issubset(triggers):
            raise OutcomeProgressCorruption(
                "outcome progress append-only trigger is unavailable"
            )
        rows = self._connection.execute(
            "SELECT * FROM outcome_progress ORDER BY sequence"
        ).fetchall()
        previous = GENESIS_HASH
        for expected, row in enumerate(rows, start=1):
            if int(row["sequence"]) != expected:
                raise OutcomeProgressCorruption("progress sequence gap")
            try:
                payload = json.loads(str(row["payload_json"]))
            except (TypeError, ValueError) as exc:
                raise OutcomeProgressCorruption("progress payload is invalid") from exc
            payload_hash = canonical_hash(payload)
            if payload_hash != str(row["payload_hash"]):
                raise OutcomeProgressCorruption("progress payload hash mismatch")
            if str(row["previous_hash"]) != previous:
                raise OutcomeProgressCorruption("progress chain is broken")
            expected_hash = canonical_hash(
                {
                    "schema": "options_copilot.outcome_progress_chain.v1",
                    "sequence": expected,
                    "previous_hash": previous,
                    "payload_hash": payload_hash,
                    "recorded_at": str(row["recorded_at"]),
                }
            )
            if expected_hash != str(row["chain_hash"]):
                raise OutcomeProgressCorruption("progress chain hash mismatch")
            previous = expected_hash

    def _migrate(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS outcome_progress(
                sequence INTEGER PRIMARY KEY,
                payload_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                chain_hash TEXT NOT NULL UNIQUE,
                recorded_at TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TRIGGER IF NOT EXISTS outcome_progress_no_update
            BEFORE UPDATE ON outcome_progress
            BEGIN SELECT RAISE(ABORT, 'outcome progress is append-only'); END
            """
        )
        self._connection.execute(
            """
            CREATE TRIGGER IF NOT EXISTS outcome_progress_no_delete
            BEFORE DELETE ON outcome_progress
            BEGIN SELECT RAISE(ABORT, 'outcome progress is append-only'); END
            """
        )


__all__ = [
    "OutcomeProgressCorruption",
    "OutcomeProgressState",
    "OutcomeProgressStore",
]
