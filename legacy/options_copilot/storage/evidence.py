"""Immutable, point-in-time evidence ledger for external provider facts.

The store is deliberately separate from the decision ledger: external facts
may conflict or arrive late, and both versions must remain replayable without
granting them trading authority.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import re
import sqlite3
import threading
import uuid

from .canonical import canonical_json, datetime_text, freeze_json, thaw_json, utc_datetime


SCHEMA_VERSION = 2
GENESIS_HASH = "0" * 64
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
_SECRET_KEY_RE = re.compile(r"(?:api[_-]?key|authorization|credential|password|secret|token)", re.I)
_SECRET_VALUE_RE = re.compile(r"(?:bearer\s+|(?:sk|pk)_[A-Za-z0-9_-]{12,})", re.I)


class EvidenceStoreError(RuntimeError):
    pass


class EvidenceStoreCorruption(EvidenceStoreError):
    pass


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    identity: str
    kind: str
    symbol: str | None
    provider: str
    source_id: str
    published_at: datetime
    first_seen_at: datetime
    ingested_at: datetime
    observed_at: datetime
    payload: Mapping[str, object]
    status: str = "ACTIVE"
    supersedes_id: str | None = None
    decision_authority: str = "SUPPORTING_ONLY"

    def __post_init__(self) -> None:
        for field in ("identity", "kind", "provider", "source_id"):
            _identifier(field, getattr(self, field))
        if self.symbol is not None:
            _identifier("symbol", self.symbol)
        if self.supersedes_id is not None:
            _identifier("supersedes_id", self.supersedes_id)
        if self.decision_authority != "SUPPORTING_ONLY":
            raise ValueError("evidence decision_authority must be SUPPORTING_ONLY")
        if not isinstance(self.payload, Mapping):
            raise TypeError("payload must be a mapping")
        _reject_secret_like(self.payload)
        frozen = freeze_json(self.payload)
        assert isinstance(frozen, Mapping)
        object.__setattr__(self, "payload", frozen)
        for field in ("published_at", "first_seen_at", "ingested_at", "observed_at"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        if self.published_at > self.first_seen_at:
            raise ValueError("published_at cannot be after first_seen_at")
        if self.first_seen_at > self.ingested_at:
            raise ValueError("first_seen_at cannot be after ingested_at")
        if self.ingested_at > self.observed_at:
            raise ValueError("ingested_at cannot be after observed_at")

    def immutable_document(self) -> dict[str, object]:
        return {
            "identity": self.identity,
            "kind": self.kind,
            "symbol": self.symbol,
            "provider": self.provider,
            "source_id": self.source_id,
            "published_at": datetime_text(self.published_at),
            "first_seen_at": datetime_text(self.first_seen_at),
            "ingested_at": datetime_text(self.ingested_at),
            "observed_at": datetime_text(self.observed_at),
            "payload": thaw_json(self.payload),
            "status": self.status,
            "supersedes_id": self.supersedes_id,
            "decision_authority": self.decision_authority,
        }


@dataclass(frozen=True, slots=True)
class StoredEvidence:
    sequence: int
    evidence_id: str
    record: EvidenceRecord
    content_hash: str
    prior_hash: str
    row_hash: str
    status: str

    @property
    def identity(self) -> str:
        return self.record.identity

    @property
    def decision_authority(self) -> str:
        return self.record.decision_authority

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "evidence_id": self.evidence_id,
            **self.record.immutable_document(),
            "content_hash": self.content_hash,
            "prior_hash": self.prior_hash,
            "row_hash": self.row_hash,
            "effective_status": self.status,
        }


@dataclass(frozen=True, slots=True)
class EvidenceAppendResult:
    evidence: StoredEvidence
    inserted: bool

    def __iter__(self):
        yield self.evidence
        yield self.inserted


class EvidenceStore:
    """Thread-safe append-only local evidence store with directed migration."""

    def __init__(self, path: str | Path, *, clock: Callable[[], datetime] | None = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(path, timeout=10.0, isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        try:
            self._journal_mode = str(self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            value = int(self._connection.execute("PRAGMA synchronous").fetchone()[0])
            self._synchronous = {0: "off", 1: "normal", 2: "full", 3: "extra"}.get(value, str(value))
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._migrate()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "EvidenceStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        return self._synchronous

    @property
    def schema_version(self) -> int:
        self._ensure_open()
        return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def append(self, record: EvidenceRecord) -> EvidenceAppendResult:
        if not isinstance(record, EvidenceRecord):
            raise TypeError("record must be an EvidenceRecord")
        immutable_json = canonical_json(record.immutable_document())
        content_hash = hashlib.sha256(immutable_json.encode("utf-8")).hexdigest()
        with self._transaction():
            duplicate = self._connection.execute(
                "SELECT * FROM evidence_records WHERE identity=? AND content_hash=?", (record.identity, content_hash)
            ).fetchone()
            if duplicate is not None:
                return EvidenceAppendResult(self._stored(duplicate), False)
            tail = self._connection.execute(
                "SELECT sequence, row_hash FROM evidence_records ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            prior_hash = GENESIS_HASH if tail is None else str(tail["row_hash"])
            row_hash = _row_hash(sequence, prior_hash, content_hash)
            evidence_id = f"ev_{uuid.uuid4().hex}"
            self._connection.execute(
                """INSERT INTO evidence_records(
                    sequence,evidence_id,identity,kind,symbol,provider,source_id,published_at,
                    first_seen_at,ingested_at,observed_at,payload_json,immutable_json,content_hash,
                    prior_hash,row_hash,status,supersedes_id,decision_authority
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sequence, evidence_id, record.identity, record.kind, record.symbol, record.provider,
                 record.source_id, datetime_text(record.published_at), datetime_text(record.first_seen_at),
                 datetime_text(record.ingested_at), datetime_text(record.observed_at), canonical_json(record.payload),
                 immutable_json, content_hash, prior_hash, row_hash, record.status, record.supersedes_id,
                 record.decision_authority),
            )
            row = self._connection.execute("SELECT * FROM evidence_records WHERE sequence=?", (sequence,)).fetchone()
            if row is None:
                raise EvidenceStoreCorruption("inserted evidence is missing")
            return EvidenceAppendResult(self._stored(row), True)

    append_record = append

    def query(
        self,
        *,
        first_seen_at_or_before: datetime | None = None,
        identities: Sequence[str] | None = None,
        kinds: Sequence[str] | None = None,
        limit: int = 500,
    ) -> tuple[StoredEvidence, ...]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        clauses: list[str] = []
        params: list[object] = []
        if first_seen_at_or_before is not None:
            clauses.append("first_seen_at <= ?")
            params.append(datetime_text(utc_datetime(first_seen_at_or_before, field="first_seen_at_or_before")))
        if identities:
            values = tuple(_identifier("identity", value) for value in identities)
            clauses.append("identity IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        if kinds is not None:
            if isinstance(kinds, (str, bytes, bytearray)) or not isinstance(
                kinds, Sequence
            ):
                raise TypeError("kinds must be a sequence of identifiers")
            values = tuple(_identifier("kind", value) for value in kinds)
            if not values:
                raise ValueError("kinds cannot be empty")
            if len(set(values)) != len(values):
                raise ValueError("kinds cannot contain duplicates")
            clauses.append("kind IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        params.append(limit)
        sql = "SELECT *, CASE WHEN COUNT(*) OVER (PARTITION BY identity) > 1 THEN 'CONFLICTED' ELSE status END AS effective_status FROM evidence_records"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        # Bound memory by selecting the newest matching records first, then
        # restore append-chain order for deterministic callers.  Selecting in
        # ascending order made every record after the first 5,000 invisible to
        # restart reconstruction.
        sql = f"SELECT * FROM ({sql} ORDER BY sequence DESC LIMIT ?) AS recent ORDER BY sequence"
        self._ensure_open()
        with self._lock:
            rows = self._connection.execute(sql, params).fetchall()
        return tuple(self._stored(row) for row in rows)

    def query_page(
        self,
        *,
        after_sequence: int = 0,
        at_or_before_sequence: int | None = None,
        first_seen_at_or_before: datetime | None = None,
        identities: Sequence[str] | None = None,
        kinds: Sequence[str] | None = None,
        limit: int = 500,
    ) -> tuple[StoredEvidence, ...]:
        """Read one ascending cursor page without hiding older matching rows."""

        if (
            not isinstance(after_sequence, int)
            or isinstance(after_sequence, bool)
            or after_sequence < 0
        ):
            raise ValueError("after_sequence must be a non-negative integer")
        if at_or_before_sequence is not None and (
            not isinstance(at_or_before_sequence, int)
            or isinstance(at_or_before_sequence, bool)
            or at_or_before_sequence < after_sequence
        ):
            raise ValueError(
                "at_or_before_sequence must be an integer at or after the cursor"
            )
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        clauses = ["records.sequence > ?"]
        params: list[object] = [after_sequence]
        conflict_clauses = ["conflicts.identity=records.identity"]
        conflict_params: list[object] = []
        if at_or_before_sequence is not None:
            clauses.append("records.sequence <= ?")
            params.append(at_or_before_sequence)
            conflict_clauses.append("conflicts.sequence <= ?")
            conflict_params.append(at_or_before_sequence)
        if first_seen_at_or_before is not None:
            checked_first_seen = datetime_text(
                utc_datetime(
                    first_seen_at_or_before,
                    field="first_seen_at_or_before",
                )
            )
            clauses.append("records.first_seen_at <= ?")
            params.append(checked_first_seen)
            conflict_clauses.append("conflicts.first_seen_at <= ?")
            conflict_params.append(checked_first_seen)
        if identities:
            values = tuple(_identifier("identity", value) for value in identities)
            clauses.append("records.identity IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        if kinds is not None:
            if isinstance(kinds, (str, bytes, bytearray)) or not isinstance(
                kinds,
                Sequence,
            ):
                raise TypeError("kinds must be a sequence of identifiers")
            values = tuple(_identifier("kind", value) for value in kinds)
            if not values:
                raise ValueError("kinds cannot be empty")
            if len(set(values)) != len(values):
                raise ValueError("kinds cannot contain duplicates")
            clauses.append("records.kind IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
        sql = (
            "SELECT records.*, CASE WHEN "
            "(SELECT COUNT(*) FROM evidence_records AS conflicts "
            "WHERE "
            + " AND ".join(conflict_clauses)
            + ") > 1 "
            "THEN 'CONFLICTED' ELSE records.status END AS effective_status "
            "FROM evidence_records AS records WHERE "
            + " AND ".join(clauses)
            + " ORDER BY records.sequence LIMIT ?"
        )
        self._ensure_open()
        with self._lock:
            rows = self._connection.execute(
                sql,
                [*conflict_params, *params, limit],
            ).fetchall()
        return tuple(self._stored(row) for row in rows)

    def verified_head_sequence(
        self,
        *,
        first_seen_at_or_before: datetime | None = None,
    ) -> int:
        """Verify the chain and freeze one eligible head under the store lock."""

        checked_first_seen = (
            None
            if first_seen_at_or_before is None
            else datetime_text(
                utc_datetime(
                    first_seen_at_or_before,
                    field="first_seen_at_or_before",
                )
            )
        )
        self._ensure_open()
        # The outer RLock deliberately spans both integrity verification and
        # head selection.  Append uses the same lock through _transaction(), so
        # no in-process append can enter the frozen prefix after verification.
        with self._lock:
            self.assert_integrity()
            if checked_first_seen is None:
                row = self._connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM evidence_records"
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) FROM evidence_records "
                    "WHERE first_seen_at <= ?",
                    (checked_first_seen,),
                ).fetchone()
            return 0 if row is None else int(row[0])

    def iter_verified(
        self,
        *,
        after_sequence: int = 0,
        first_seen_at_or_before: datetime | None = None,
        identities: Sequence[str] | None = None,
        kinds: Sequence[str] | None = None,
        page_size: int = 5000,
    ) -> Iterator[StoredEvidence]:
        """Verify one chain snapshot, then yield every matching row in order."""

        tail_sequence = self.verified_head_sequence(
            first_seen_at_or_before=first_seen_at_or_before,
        )
        cursor = after_sequence
        while cursor < tail_sequence:
            page = self.query_page(
                after_sequence=cursor,
                at_or_before_sequence=tail_sequence,
                first_seen_at_or_before=first_seen_at_or_before,
                identities=identities,
                kinds=kinds,
                limit=page_size,
            )
            if not page:
                break
            yield from page
            cursor = page[-1].sequence

    def get(self, evidence_id: str) -> StoredEvidence | None:
        _identifier("evidence_id", evidence_id)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                "SELECT *, CASE WHEN COUNT(*) OVER (PARTITION BY identity) > 1 THEN 'CONFLICTED' ELSE status END AS effective_status FROM evidence_records WHERE evidence_id=?", (evidence_id,)
            ).fetchone()
        return None if row is None else self._stored(row)

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self._ensure_open()
        with self._lock:
            rows = self._connection.execute("SELECT * FROM evidence_records ORDER BY sequence").fetchall()
        prior_hash = GENESIS_HASH
        for sequence, row in enumerate(rows, start=1):
            if int(row["sequence"]) != sequence:
                raise EvidenceStoreCorruption("evidence sequence contains a gap")
            document = json.loads(str(row["immutable_json"]))
            if canonical_json(document) != str(row["immutable_json"]):
                raise EvidenceStoreCorruption(f"immutable document mismatch at sequence {sequence}")
            if hashlib.sha256(str(row["immutable_json"]).encode("utf-8")).hexdigest() != str(row["content_hash"]):
                raise EvidenceStoreCorruption(f"content hash mismatch at sequence {sequence}")
            if str(row["prior_hash"]) != prior_hash:
                raise EvidenceStoreCorruption(f"prior hash mismatch at sequence {sequence}")
            expected = _row_hash(sequence, prior_hash, str(row["content_hash"]))
            if str(row["row_hash"]) != expected:
                raise EvidenceStoreCorruption(f"row hash mismatch at sequence {sequence}")
            prior_hash = expected
        with self._lock:
            if self._connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise EvidenceStoreCorruption("SQLite integrity check failed")

    def _stored(self, row: sqlite3.Row) -> StoredEvidence:
        document = json.loads(str(row["immutable_json"]))
        record = EvidenceRecord(
            identity=str(document["identity"]), kind=str(document["kind"]), symbol=document["symbol"],
            provider=str(document["provider"]), source_id=str(document["source_id"]),
            published_at=_parse_time(document["published_at"]), first_seen_at=_parse_time(document["first_seen_at"]),
            ingested_at=_parse_time(document["ingested_at"]), observed_at=_parse_time(document["observed_at"]),
            payload=document["payload"], status=str(document["status"]), supersedes_id=document["supersedes_id"],
            decision_authority=str(document["decision_authority"]),
        )
        return StoredEvidence(int(row["sequence"]), str(row["evidence_id"]), record, str(row["content_hash"]), str(row["prior_hash"]), str(row["row_hash"]), str(row["effective_status"] if "effective_status" in row.keys() else row["status"]))

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"evidence schema {version} is newer than supported")
        with self._lock:
            if version == 0:
                self._create_schema()
            elif version == 1:
                columns = {str(row[1]) for row in self._connection.execute("PRAGMA table_info(evidence_records)")}
                required = {"sequence", "evidence_id", "identity", "kind", "provider", "source_id", "published_at", "first_seen_at", "ingested_at", "payload_json", "immutable_json", "content_hash", "row_hash", "status", "supersedes_id"}
                if not required <= columns or not ({"previous_hash", "prior_hash"} & columns):
                    raise RuntimeError("malformed v1 evidence schema")
                rename = "ALTER TABLE evidence_records RENAME COLUMN previous_hash TO prior_hash;" if "previous_hash" in columns else ""
                self._connection.executescript(
                    "BEGIN IMMEDIATE; " + rename
                    + "ALTER TABLE evidence_records ADD COLUMN observed_at TEXT; "
                    + "ALTER TABLE evidence_records ADD COLUMN decision_authority TEXT NOT NULL DEFAULT 'SUPPORTING_ONLY'; "
                    + "UPDATE evidence_records SET observed_at=ingested_at WHERE observed_at IS NULL; "
                    + "CREATE UNIQUE INDEX IF NOT EXISTS evidence_records_identity_hash_idx ON evidence_records(identity, content_hash); "
                    + "CREATE INDEX IF NOT EXISTS evidence_records_first_seen_idx ON evidence_records(first_seen_at, sequence); "
                    + "PRAGMA user_version=2; COMMIT;"
                )
                self._install_triggers()
            else:
                self._validate_v2_schema()

    def _create_schema(self) -> None:
        self._connection.executescript("""
            BEGIN IMMEDIATE;
            CREATE TABLE evidence_records (
                sequence INTEGER PRIMARY KEY, evidence_id TEXT NOT NULL UNIQUE, identity TEXT NOT NULL,
                kind TEXT NOT NULL, symbol TEXT, provider TEXT NOT NULL, source_id TEXT NOT NULL,
                published_at TEXT NOT NULL, first_seen_at TEXT NOT NULL, ingested_at TEXT NOT NULL,
                observed_at TEXT NOT NULL, payload_json TEXT NOT NULL, immutable_json TEXT NOT NULL,
                content_hash TEXT NOT NULL, prior_hash TEXT NOT NULL, row_hash TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL, supersedes_id TEXT, decision_authority TEXT NOT NULL,
                FOREIGN KEY(supersedes_id) REFERENCES evidence_records(evidence_id)
            );
            CREATE UNIQUE INDEX evidence_records_identity_hash_idx ON evidence_records(identity, content_hash);
            CREATE INDEX evidence_records_first_seen_idx ON evidence_records(first_seen_at, sequence);
            PRAGMA user_version=2;
            COMMIT;
        """)
        self._install_triggers()

    def _validate_v2_schema(self) -> None:
        columns = {str(row[1]) for row in self._connection.execute("PRAGMA table_info(evidence_records)")}
        required = {"sequence", "evidence_id", "identity", "kind", "provider", "source_id", "published_at", "first_seen_at", "ingested_at", "observed_at", "payload_json", "immutable_json", "content_hash", "prior_hash", "row_hash", "status", "supersedes_id", "decision_authority"}
        if not required <= columns:
            raise RuntimeError("malformed v2 evidence schema")
        self._install_triggers()

    def _install_triggers(self) -> None:
        self._connection.executescript("""
            CREATE TRIGGER IF NOT EXISTS evidence_records_no_update BEFORE UPDATE ON evidence_records
            BEGIN SELECT RAISE(ABORT, 'immutable evidence: update forbidden'); END;
            CREATE TRIGGER IF NOT EXISTS evidence_records_no_delete BEFORE DELETE ON evidence_records
            BEGIN SELECT RAISE(ABORT, 'immutable evidence: delete forbidden'); END;
        """)

    @contextmanager
    def _transaction(self):
        self._ensure_open()
        self._lock.acquire()
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            yield
            self._connection.execute("COMMIT")
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        finally:
            self._lock.release()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("evidence store is closed")


def _row_hash(sequence: int, prior_hash: str, content_hash: str) -> str:
    return hashlib.sha256(f"{sequence}:{prior_hash}:{content_hash}".encode("ascii")).hexdigest()


def _identifier(field: str, value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a valid identifier")
    return value


def _reject_secret_like(value: object, *, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("payload keys must be strings")
            if _SECRET_KEY_RE.search(key):
                raise ValueError(f"secret-like field is not permitted at {path}.{key}")
            _reject_secret_like(item, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _reject_secret_like(item, path=f"{path}[{index}]")
    elif isinstance(value, str) and _SECRET_VALUE_RE.search(value):
        raise ValueError(f"secret-like value is not permitted at {path}")


def _parse_time(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return utc_datetime(parsed)


__all__ = ["EvidenceAppendResult", "EvidenceRecord", "EvidenceStore", "EvidenceStoreCorruption", "EvidenceStoreError", "StoredEvidence"]
