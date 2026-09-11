"""Append-only persistence for provisional weekly research briefs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, Iterator

from options_copilot.storage.canonical import canonical_hash, canonical_json, datetime_text

from .weekly_brief import WeeklyBrief


GENESIS_HASH = "0" * 64
WEEKLY_BRIEF_STORE_SCHEMA_VERSION = 1


class WeeklyBriefStoreError(RuntimeError):
    """Base weekly-brief persistence failure."""


class WeeklyBriefStoreConflict(WeeklyBriefStoreError):
    """One idempotency key was reused with different immutable content."""


class WeeklyBriefStoreCorruption(WeeklyBriefStoreError):
    """The SQLite ledger or its append chain failed verification."""


@dataclass(frozen=True, slots=True)
class StoredWeeklyBrief:
    sequence: int
    idempotency_key: str
    content_hash: str
    payload: Mapping[str, object]
    appended_at: datetime
    prior_hash: str
    row_hash: str


@dataclass(frozen=True, slots=True)
class WeeklyBriefAppendResult:
    record: StoredWeeklyBrief
    inserted: bool


class WeeklyBriefStore:
    """SQLite WAL/FULL append-only ledger with strict retry identity."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            self._migrate()
            self.assert_integrity()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> WeeklyBriefStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        self._ensure_open()
        value = int(self._connection.execute("PRAGMA synchronous").fetchone()[0])
        return {0: "off", 1: "normal", 2: "full", 3: "extra"}.get(
            value,
            str(value),
        )

    @property
    def count(self) -> int:
        self._ensure_open()
        row = self._connection.execute(
            "SELECT COUNT(*) AS count FROM weekly_briefs"
        ).fetchone()
        return int(row["count"])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def append(self, brief: WeeklyBrief) -> WeeklyBriefAppendResult:
        if not isinstance(brief, WeeklyBrief):
            raise TypeError("brief must be a WeeklyBrief")
        return self.append_document(
            idempotency_key=brief.idempotency_key,
            content_hash=brief.content_hash,
            payload=brief.append_payload(),
        )

    def append_document(
        self,
        *,
        idempotency_key: str,
        content_hash: str,
        payload: Mapping[str, object],
    ) -> WeeklyBriefAppendResult:
        """Append one hash-bound weekly outcome, including explicit NOT_RUN."""

        checked_key = _digest("idempotency_key", idempotency_key)
        checked_hash = _digest("content_hash", content_hash)
        if not isinstance(payload, Mapping):
            raise TypeError("payload must be a mapping")
        payload = dict(payload)
        if payload.get("idempotency_key") != checked_key:
            raise ValueError("weekly document idempotency binding is invalid")
        if payload.get("content_hash") != checked_hash:
            raise ValueError("weekly document content binding is invalid")
        payload_json = canonical_json(payload)
        appended_at = _aware(self._clock())
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM weekly_briefs WHERE idempotency_key=?",
                (checked_key,),
            ).fetchone()
            if existing is not None:
                stored = self._stored(existing)
                if (
                    stored.content_hash != checked_hash
                    or canonical_json(stored.payload) != payload_json
                ):
                    raise WeeklyBriefStoreConflict(
                        "weekly brief idempotency key conflicts with immutable content"
                    )
                return WeeklyBriefAppendResult(stored, False)
            tail = self._connection.execute(
                "SELECT sequence, row_hash FROM weekly_briefs "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            prior_hash = GENESIS_HASH if tail is None else str(tail["row_hash"])
            row_hash = _row_hash(
                sequence=sequence,
                prior_hash=prior_hash,
                idempotency_key=checked_key,
                content_hash=checked_hash,
                payload_json=payload_json,
                appended_at=appended_at,
            )
            self._connection.execute(
                """
                INSERT INTO weekly_briefs(
                    sequence, idempotency_key, content_hash, payload_json,
                    appended_at, prior_hash, row_hash
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    sequence,
                    checked_key,
                    checked_hash,
                    payload_json,
                    datetime_text(appended_at),
                    prior_hash,
                    row_hash,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM weekly_briefs WHERE sequence=?",
                (sequence,),
            ).fetchone()
            if row is None:
                raise WeeklyBriefStoreCorruption("inserted weekly brief is missing")
            return WeeklyBriefAppendResult(self._stored(row), True)

    def latest(self) -> StoredWeeklyBrief | None:
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM weekly_briefs ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            return None if row is None else self._stored(row)

    def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> StoredWeeklyBrief | None:
        """Return an existing immutable weekly outcome without mutating it."""

        checked_key = _digest("idempotency_key", idempotency_key)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM weekly_briefs WHERE idempotency_key=?",
                (checked_key,),
            ).fetchone()
            return None if row is None else self._stored(row)

    def assert_integrity(self) -> None:
        self._ensure_open()
        with self._lock:
            result = str(
                self._connection.execute("PRAGMA integrity_check").fetchone()[0]
            )
            if result.lower() != "ok":
                raise WeeklyBriefStoreCorruption("weekly brief SQLite integrity failed")
            prior_hash = GENESIS_HASH
            rows = self._connection.execute(
                "SELECT * FROM weekly_briefs ORDER BY sequence"
            ).fetchall()
            for expected_sequence, row in enumerate(rows, start=1):
                stored = self._stored(row)
                if stored.sequence != expected_sequence or stored.prior_hash != prior_hash:
                    raise WeeklyBriefStoreCorruption("weekly brief append chain is broken")
                payload_json = canonical_json(stored.payload)
                if stored.payload.get("idempotency_key") != stored.idempotency_key:
                    raise WeeklyBriefStoreCorruption("weekly brief identity binding is invalid")
                if stored.payload.get("content_hash") != stored.content_hash:
                    raise WeeklyBriefStoreCorruption("weekly brief content binding is invalid")
                expected_hash = _row_hash(
                    sequence=stored.sequence,
                    prior_hash=stored.prior_hash,
                    idempotency_key=stored.idempotency_key,
                    content_hash=stored.content_hash,
                    payload_json=payload_json,
                    appended_at=stored.appended_at,
                )
                if stored.row_hash != expected_hash:
                    raise WeeklyBriefStoreCorruption("weekly brief row hash is invalid")
                prior_hash = stored.row_hash

    def _stored(self, row: sqlite3.Row) -> StoredWeeklyBrief:
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise WeeklyBriefStoreCorruption("weekly brief payload JSON is invalid") from exc
        if not isinstance(payload, dict):
            raise WeeklyBriefStoreCorruption("weekly brief payload must be an object")
        return StoredWeeklyBrief(
            sequence=int(row["sequence"]),
            idempotency_key=str(row["idempotency_key"]),
            content_hash=str(row["content_hash"]),
            payload=payload,
            appended_at=datetime.fromisoformat(str(row["appended_at"])),
            prior_hash=str(row["prior_hash"]),
            row_hash=str(row["row_hash"]),
        )

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > WEEKLY_BRIEF_STORE_SCHEMA_VERSION:
            raise WeeklyBriefStoreCorruption("weekly brief store schema is newer than runtime")
        if version == 0:
            with self._transaction():
                self._connection.execute(
                    """
                    CREATE TABLE weekly_briefs(
                        sequence INTEGER PRIMARY KEY,
                        idempotency_key TEXT NOT NULL UNIQUE,
                        content_hash TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        appended_at TEXT NOT NULL,
                        prior_hash TEXT NOT NULL,
                        row_hash TEXT NOT NULL UNIQUE
                    )
                    """
                )
                self._connection.execute(
                    """
                    CREATE TRIGGER weekly_briefs_no_update
                    BEFORE UPDATE ON weekly_briefs
                    BEGIN SELECT RAISE(ABORT, 'weekly_briefs append-only'); END
                    """
                )
                self._connection.execute(
                    """
                    CREATE TRIGGER weekly_briefs_no_delete
                    BEFORE DELETE ON weekly_briefs
                    BEGIN SELECT RAISE(ABORT, 'weekly_briefs append-only'); END
                    """
                )
                self._connection.execute("PRAGMA user_version=1")

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._ensure_open()
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def _ensure_open(self) -> None:
        if self._closed:
            raise WeeklyBriefStoreError("weekly brief store is closed")


def _row_hash(
    *,
    sequence: int,
    prior_hash: str,
    idempotency_key: str,
    content_hash: str,
    payload_json: str,
    appended_at: datetime,
) -> str:
    return canonical_hash(
        {
            "sequence": sequence,
            "prior_hash": prior_hash,
            "idempotency_key": idempotency_key,
            "content_hash": content_hash,
            "payload_json": payload_json,
            "appended_at": datetime_text(appended_at),
        }
    )


def _aware(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise TypeError("clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _digest(field: str, value: object) -> str:
    text = str(value).strip().lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return text


__all__ = [
    "StoredWeeklyBrief",
    "WEEKLY_BRIEF_STORE_SCHEMA_VERSION",
    "WeeklyBriefAppendResult",
    "WeeklyBriefStore",
    "WeeklyBriefStoreConflict",
    "WeeklyBriefStoreCorruption",
    "WeeklyBriefStoreError",
]
