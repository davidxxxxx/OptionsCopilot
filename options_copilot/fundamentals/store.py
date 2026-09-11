"""Append-only hash-chained store for fundamental revisions."""
from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import threading
import uuid

from options_copilot.storage.canonical import canonical_hash, canonical_json, utc_datetime

from .models import FundamentalMetric, FundamentalObservation


SCHEMA_VERSION = 1
GENESIS_HASH = "0" * 64


class FundamentalsStoreCorruption(RuntimeError):
    """The local revision ledger failed an integrity check."""


@dataclass(frozen=True, slots=True)
class StoredFundamental:
    sequence: int
    observation_id: str
    observation: FundamentalObservation
    revision_number: int
    supersedes_hash: str | None
    prior_hash: str
    row_hash: str

    def as_dict(self) -> dict[str, object]:
        return {
            **self.observation.as_dict(),
            "sequence": self.sequence,
            "observation_id": self.observation_id,
            "revision_number": self.revision_number,
            "supersedes_hash": self.supersedes_hash,
            "prior_hash": self.prior_hash,
            "row_hash": self.row_hash,
        }


@dataclass(frozen=True, slots=True)
class FundamentalAppendResult:
    record: StoredFundamental
    inserted: bool


class FundamentalsStore:
    """SQLite ledger that never updates or deletes a fundamental record."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
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
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
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

    def append(self, observation: FundamentalObservation) -> FundamentalAppendResult:
        if not isinstance(observation, FundamentalObservation):
            raise TypeError("observation must be FundamentalObservation")
        content_hash = observation.content_hash
        with self._transaction():
            duplicate = self._connection.execute(
                "SELECT * FROM fundamental_records WHERE semantic_hash=?",
                (observation.semantic_hash,),
            ).fetchone()
            if duplicate is not None:
                return FundamentalAppendResult(self._stored(duplicate), False)
            tail = self._connection.execute(
                "SELECT sequence,row_hash FROM fundamental_records ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            series_head = self._connection.execute(
                """SELECT content_hash,revision_number FROM fundamental_records
                   WHERE series_key=? ORDER BY sequence DESC LIMIT 1""",
                (observation.series_key,),
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            prior_hash = GENESIS_HASH if tail is None else str(tail["row_hash"])
            supersedes_hash = (
                None if series_head is None else str(series_head["content_hash"])
            )
            revision_number = (
                1 if series_head is None else int(series_head["revision_number"]) + 1
            )
            row_hash = canonical_hash(
                {
                    "sequence": sequence,
                    "prior_hash": prior_hash,
                    "content_hash": content_hash,
                    "series_key": observation.series_key,
                    "revision_number": revision_number,
                    "supersedes_hash": supersedes_hash,
                }
            )
            observation_id = f"fund_{uuid.uuid4().hex}"
            self._connection.execute(
                """INSERT INTO fundamental_records(
                    sequence,observation_id,series_key,symbol,metric,period_end,
                    observed_at,source_filed_date,content_json,content_hash,semantic_hash,
                    revision_number,supersedes_hash,prior_hash,row_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    sequence,
                    observation_id,
                    observation.series_key,
                    observation.symbol,
                    observation.metric.value,
                    observation.period_end.isoformat(),
                    observation.observed_at.isoformat(),
                    None
                    if observation.source_filed_date is None
                    else observation.source_filed_date.isoformat(),
                    canonical_json(observation.hash_payload()),
                    content_hash,
                    observation.semantic_hash,
                    revision_number,
                    supersedes_hash,
                    prior_hash,
                    row_hash,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM fundamental_records WHERE sequence=?",
                (sequence,),
            ).fetchone()
            if row is None:
                raise FundamentalsStoreCorruption("inserted fundamental is missing")
            return FundamentalAppendResult(self._stored(row), True)

    def append_many(
        self,
        observations: Sequence[FundamentalObservation],
    ) -> tuple[FundamentalAppendResult, ...]:
        return tuple(self.append(item) for item in observations)

    def current(
        self,
        *,
        symbols: Sequence[str] = (),
        as_of: datetime | None = None,
        limit: int = 500,
    ) -> tuple[StoredFundamental, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        clauses: list[str] = []
        params: list[object] = []
        if symbols:
            checked = tuple(dict.fromkeys(str(item).strip().upper() for item in symbols))
            clauses.append("symbol IN (" + ",".join("?" for _ in checked) + ")")
            params.extend(checked)
        if as_of is not None:
            clauses.append("observed_at <= ?")
            params.append(utc_datetime(as_of, field="as_of").isoformat())
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        query = (
            "SELECT * FROM fundamental_records"
            + where
            + " ORDER BY sequence DESC"
        )
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        heads: dict[str, sqlite3.Row] = {}
        for row in rows:
            heads.setdefault(str(row["series_key"]), row)
            if len(heads) >= limit:
                break
        return tuple(
            self._stored(row)
            for row in sorted(heads.values(), key=lambda item: int(item["sequence"]), reverse=True)
        )

    def verified_current(
        self,
        *,
        symbols: Sequence[str] = (),
        as_of: datetime | None = None,
        limit: int = 500,
    ) -> tuple[StoredFundamental, ...]:
        """Return current rows only while holding a verified ledger boundary."""

        with self._lock:
            self._assert_integrity_unlocked()
            return self.current(symbols=symbols, as_of=as_of, limit=limit)

    def revisions(
        self,
        series_key: str,
        *,
        as_of: datetime | None = None,
        limit: int = 100,
    ) -> tuple[StoredFundamental, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        clauses = ["series_key=?"]
        params: list[object] = [str(series_key)]
        if as_of is not None:
            clauses.append("observed_at <= ?")
            params.append(utc_datetime(as_of, field="as_of").isoformat())
        params.append(limit)
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM fundamental_records WHERE "
                + " AND ".join(clauses)
                + " ORDER BY revision_number DESC LIMIT ?",
                params,
            ).fetchall()
        return tuple(self._stored(row) for row in rows)

    def assert_integrity(self) -> None:
        with self._lock:
            self._assert_integrity_unlocked()

    def _assert_integrity_unlocked(self) -> None:
        quick = str(self._connection.execute("PRAGMA quick_check").fetchone()[0])
        if quick.lower() != "ok":
            raise FundamentalsStoreCorruption("SQLite quick_check failed")
        prior_hash = GENESIS_HASH
        expected_sequence = 1
        series_heads: dict[str, tuple[str, int]] = {}
        for row in self._connection.execute(
            "SELECT * FROM fundamental_records ORDER BY sequence"
        ):
            sequence = int(row["sequence"])
            series_key = str(row["series_key"])
            content_hash = str(row["content_hash"])
            revision = int(row["revision_number"])
            previous = series_heads.get(series_key)
            expected_supersedes = None if previous is None else previous[0]
            expected_revision = 1 if previous is None else previous[1] + 1
            expected_row_hash = canonical_hash(
                {
                    "sequence": sequence,
                    "prior_hash": prior_hash,
                    "content_hash": content_hash,
                    "series_key": series_key,
                    "revision_number": revision,
                    "supersedes_hash": row["supersedes_hash"],
                }
            )
            if (
                sequence != expected_sequence
                or row["prior_hash"] != prior_hash
                or row["supersedes_hash"] != expected_supersedes
                or revision != expected_revision
                or row["row_hash"] != expected_row_hash
            ):
                raise FundamentalsStoreCorruption("fundamental hash chain is invalid")
            document = json.loads(str(row["content_json"]))
            if canonical_hash(document) != content_hash:
                raise FundamentalsStoreCorruption("fundamental content hash is invalid")
            semantic_document = dict(document)
            semantic_document.pop("observed_at", None)
            if canonical_hash(semantic_document) != row["semantic_hash"]:
                raise FundamentalsStoreCorruption("fundamental semantic hash is invalid")
            prior_hash = str(row["row_hash"])
            series_heads[series_key] = (content_hash, revision)
            expected_sequence += 1

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version not in {0, SCHEMA_VERSION}:
            raise FundamentalsStoreCorruption("unsupported fundamentals schema")
        if version == 0:
            self._connection.executescript(
                """
                CREATE TABLE fundamental_records(
                    sequence INTEGER PRIMARY KEY,
                    observation_id TEXT NOT NULL UNIQUE,
                    series_key TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    source_filed_date TEXT,
                    content_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    semantic_hash TEXT NOT NULL UNIQUE,
                    revision_number INTEGER NOT NULL,
                    supersedes_hash TEXT,
                    prior_hash TEXT NOT NULL,
                    row_hash TEXT NOT NULL UNIQUE
                );
                CREATE INDEX fundamental_series_idx
                    ON fundamental_records(series_key,sequence DESC);
                CREATE INDEX fundamental_symbol_asof_idx
                    ON fundamental_records(symbol,observed_at,sequence DESC);
                PRAGMA user_version=1;
                """
            )

    def _stored(self, row: sqlite3.Row) -> StoredFundamental:
        payload = json.loads(str(row["content_json"]))
        observation = FundamentalObservation(
            symbol=payload["symbol"],
            cik=payload.get("cik"),
            metric=FundamentalMetric(payload["metric"]),
            value=Decimal(str(payload["value"]["$decimal"])),
            unit=payload["unit"],
            basis=payload["basis"],
            period_end=date.fromisoformat(payload["period_end"]["$date"]),
            fiscal_period=payload["fiscal_period"],
            fiscal_year=payload.get("fiscal_year"),
            source=payload["source"],
            source_id=payload["source_id"],
            source_url=payload["source_url"],
            source_filed_date=(
                None
                if payload.get("source_filed_date") is None
                else date.fromisoformat(payload["source_filed_date"]["$date"])
            ),
            observed_at=datetime.fromisoformat(payload["observed_at"]),
            taxonomy=payload.get("taxonomy"),
            tag=payload.get("tag"),
            form=payload.get("form"),
        )
        return StoredFundamental(
            sequence=int(row["sequence"]),
            observation_id=str(row["observation_id"]),
            observation=observation,
            revision_number=int(row["revision_number"]),
            supersedes_hash=(
                None if row["supersedes_hash"] is None else str(row["supersedes_hash"])
            ),
            prior_hash=str(row["prior_hash"]),
            row_hash=str(row["row_hash"]),
        )

    @contextmanager
    def _transaction(self):
        with self._lock:
            if self._closed:
                raise RuntimeError("fundamentals store is closed")
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")


__all__ = [
    "FundamentalAppendResult",
    "FundamentalsStore",
    "FundamentalsStoreCorruption",
    "StoredFundamental",
]
