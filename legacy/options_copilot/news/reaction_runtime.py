"""Point-in-time production adapter for macro-event reaction research.

Jin10 is used only for a pre-release consensus vintage.  A value reported by
Jin10 after release is retained as supplemental evidence and never becomes an
``OfficialRelease``.  Only the bounded BLS Public Data adapter may create the
official actual used by the reaction state machine.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
from types import MappingProxyType
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    freeze_json,
    utc_datetime,
)

from .reaction import (
    ConsensusExpectation,
    EventReactionLedger,
    MarketReactionEvidence,
    OfficialRelease,
    OptionReevaluationEvidence,
    ReactionStage,
    ScheduledEventIdentity,
)
from .reaction_specs import (
    CalendarReactionDescriptor,
    DocumentStage,
    EventFamily,
    FAMILY_SPECS,
    MeasureIdentity,
    ParentEventIdentity,
    ScheduledReactionSpec,
    SupportState,
    assess_support,
    classify_event_family,
    fomc_meeting_range_from_title,
    parent_identity_from_calendar,
    reaction_descriptor_from_calendar,
)
from .event_reaction_coverage import (
    MarketSample,
    OptionRepriceBundle,
    ProspectiveMarketWindow,
    REQUIRED_OPTION_GATES,
    WINDOW_DURATION,
)


_UTC = timezone.utc
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
_MAXIMUM_BLS_BYTES = 2 * 1024 * 1024
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_MONTH_RE = re.compile(r"(?P<month>1[0-2]|[1-9])月")
_QUARTER_RE = re.compile(
    r"(?:(?P<quarter>[1-4])|(?P<quarter_word>第一|第二|第三|第四))季度"
)
_MACRO_SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD", "UUP")
_BASELINE_ARM_LEAD = timedelta(minutes=15)
_BASELINE_ACQUISITION_LEAD = timedelta(seconds=5)
_ENDPOINT_GRACE = timedelta(seconds=5)
_BLS_SERIES = MappingProxyType(
    {
        ("CPI", False, "YOY"): "CUUR0000SA0",
        ("CPI", False, "MOM"): "CUSR0000SA0",
        ("CPI", True, "YOY"): "CUUR0000SA0L1E",
        ("CPI", True, "MOM"): "CUSR0000SA0L1E",
        ("PPI", False, "YOY"): "WPUFD4",
        ("PPI", False, "MOM"): "WPSFD4",
        ("PPI", True, "YOY"): "WPUFD49104",
        ("PPI", True, "MOM"): "WPSFD49104",
    }
)
_BLS_PUBLISHED_INCREMENT = Decimal("0.1")
_REACTION_SCHEMA_VERSION = 4
_LEGACY_UNBOUND_OFFICIAL_EVENT_HASH = "0" * 64
_REACTION_KINDS = frozenset(
    {
        "JIN10_EXPECTATION",
        "JIN10_REPORTED_ACTUAL",
        "BLS_OFFICIAL_ACTUAL",
        "MARKET_REACTION",
        "OPTION_REEVALUATION",
    }
)


class MacroReactionError(RuntimeError):
    """Fixed-code, credential-free production reaction failure."""


@dataclass(frozen=True, slots=True)
class VerifiedReactionBatch:
    """Immutable evidence and release vintages from one verified DB snapshot."""

    event_keys: tuple[tuple[str, str], ...]
    evidence_by_key: Mapping[tuple[str, str], tuple[Mapping[str, object], ...]]
    vintages_by_key: Mapping[tuple[str, str], tuple[Mapping[str, object], ...]]

    def records(
        self,
        event_id: str,
        official_event_hash: str,
    ) -> tuple[Mapping[str, object], ...]:
        return self.evidence_by_key.get((event_id, official_event_hash), ())

    def release_vintages(
        self,
        event_id: str,
        official_event_hash: str,
    ) -> tuple[Mapping[str, object], ...]:
        return self.vintages_by_key.get((event_id, official_event_hash), ())


@dataclass(frozen=True, slots=True)
class Jin10MacroObservation:
    title: str
    scheduled_at: datetime
    metric: str
    unit: str
    period: str
    basis: str
    series_id: str
    calculation: str
    consensus: Decimal | None
    reported_actual: Decimal | None
    previous: Decimal | None
    source_id: str
    observed_at: datetime
    content_hash: str = ""

    def __post_init__(self) -> None:
        scheduled = utc_datetime(self.scheduled_at, field="scheduled_at")
        observed = utc_datetime(self.observed_at, field="observed_at")
        object.__setattr__(self, "scheduled_at", scheduled)
        object.__setattr__(self, "observed_at", observed)
        body = self.as_dict(include_hash=False)
        digest = canonical_hash(body)
        if self.content_hash and self.content_hash != digest:
            raise ValueError("Jin10 macro observation hash mismatch")
        object.__setattr__(self, "content_hash", digest)

    def as_dict(self, *, include_hash: bool = True) -> dict[str, object]:
        body: dict[str, object] = {
            "schema": "options_copilot.jin10_macro_observation.v1",
            "title": self.title,
            "scheduled_at": self.scheduled_at.isoformat(),
            "metric": self.metric,
            "unit": self.unit,
            "period": self.period,
            "basis": self.basis,
            "series_id": self.series_id,
            "calculation": self.calculation,
            "consensus": None if self.consensus is None else str(self.consensus),
            "reported_actual": (
                None if self.reported_actual is None else str(self.reported_actual)
            ),
            "previous": None if self.previous is None else str(self.previous),
            "source_id": self.source_id,
            "observed_at": self.observed_at.isoformat(),
            "decision_authority": "SUPPORTING_ONLY",
            "official_actual_verified": False,
        }
        if include_hash:
            body["content_hash"] = self.content_hash
        return body


def normalize_jin10_calendar(
    payload: Mapping[str, object],
    *,
    observed_at: datetime,
) -> tuple[Jin10MacroObservation, ...]:
    """Normalize only supported US CPI/PPI rows from one observed MCP batch."""

    if payload.get("status") not in {200, "200"}:
        raise MacroReactionError("JIN10_CALENDAR_STATUS_INVALID")
    rows = payload.get("data")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        raise MacroReactionError("JIN10_CALENDAR_DATA_INVALID")
    checked_at = utc_datetime(observed_at, field="observed_at")
    output: list[Jin10MacroObservation] = []
    for index, raw in enumerate(rows[:500]):
        if not isinstance(raw, Mapping):
            continue
        title = str(raw.get("title") or "").strip()
        scheduled = _jin10_time(raw.get("pub_time"))
        spec = _macro_spec(title, scheduled)
        if scheduled is None or spec is None:
            continue
        metric, period, basis, series_id, calculation = spec
        source_id = str(raw.get("id") or "").strip() or (
            "jin10-calendar:" + hashlib.sha256(
                f"{title}|{scheduled.isoformat()}|{index}".encode("utf-8")
            ).hexdigest()[:24]
        )
        output.append(
            Jin10MacroObservation(
                title=title,
                scheduled_at=scheduled,
                metric=metric,
                unit=(
                    "THOUSANDS"
                    if metric == "total_nonfarm_payroll_change_thousands"
                    else "PERCENT"
                ),
                period=period,
                basis=basis,
                series_id=series_id,
                calculation=calculation,
                consensus=_decimal_or_none(raw.get("consensus")),
                reported_actual=_decimal_or_none(raw.get("actual")),
                previous=_decimal_or_none(raw.get("previous")),
                source_id=source_id,
                observed_at=checked_at,
            )
        )
    return tuple(output)


def _reaction_row_hash(
    *,
    sequence: int,
    prior_hash: str,
    event_id: str,
    official_event_hash: str,
    kind: str,
    observed_at: str,
    content_hash: str,
) -> str:
    """Bind chain position, record identity, observation time, and content."""

    return canonical_hash(
        {
            "schema": "options_copilot.macro_reaction_row.v3",
            "sequence": sequence,
            "prior_hash": prior_hash,
            "event_id": event_id,
            "official_event_hash": official_event_hash,
            "kind": kind,
            "observed_at": observed_at,
            "content_hash": content_hash,
        }
    )


def _reaction_row_hash_v2(
    *,
    sequence: int,
    prior_hash: str,
    event_id: str,
    kind: str,
    observed_at: str,
    content_hash: str,
) -> str:
    """Verify the legacy v2 chain before a directed v3 migration."""

    return canonical_hash(
        {
            "schema": "options_copilot.macro_reaction_row.v2",
            "sequence": sequence,
            "prior_hash": prior_hash,
            "event_id": event_id,
            "kind": kind,
            "observed_at": observed_at,
            "content_hash": content_hash,
        }
    )


class ReactionEvidenceStore:
    """Append-only hash-chained store for the complete macro reaction chain."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._db = sqlite3.connect(
            self.path, timeout=10.0, isolation_level=None, check_same_thread=False
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._initialize_schema()
        self._ensure_v4_extension_schema()
        self.assert_integrity()

    def _ensure_v4_extension_schema(self) -> None:
        """Add v4 discovery/vintage chains without changing legacy evidence rows."""

        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS macro_reaction_discovery_records(
                sequence INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL,
                official_event_hash TEXT NOT NULL,
                parent_hash TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                UNIQUE(event_id,official_event_hash,content_hash)
            );
            CREATE TABLE IF NOT EXISTS macro_reaction_release_vintages(
                sequence INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL,
                official_event_hash TEXT NOT NULL,
                parent_hash TEXT NOT NULL,
                raw_hash TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                UNIQUE(event_id,official_event_hash,content_hash)
            );
            CREATE TRIGGER IF NOT EXISTS macro_reaction_discovery_no_update
            BEFORE UPDATE ON macro_reaction_discovery_records
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction discovery'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_discovery_no_delete
            BEFORE DELETE ON macro_reaction_discovery_records
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction discovery'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_vintage_no_update
            BEFORE UPDATE ON macro_reaction_release_vintages
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction vintage'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_vintage_no_delete
            BEFORE DELETE ON macro_reaction_release_vintages
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction vintage'); END;
            CREATE TABLE IF NOT EXISTS macro_reaction_observer_baselines(
                event_hash TEXT PRIMARY KEY,
                scheduled_at TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS macro_reaction_baseline_no_update
            BEFORE UPDATE ON macro_reaction_observer_baselines
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction baseline'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_baseline_no_delete
            BEFORE DELETE ON macro_reaction_observer_baselines
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction baseline'); END;
            CREATE TABLE IF NOT EXISTS macro_reaction_observer_endpoints(
                event_hash TEXT PRIMARY KEY,
                scheduled_at TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS macro_reaction_endpoint_no_update
            BEFORE UPDATE ON macro_reaction_observer_endpoints
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction endpoint'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_endpoint_no_delete
            BEFORE DELETE ON macro_reaction_observer_endpoints
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction endpoint'); END;
            CREATE TABLE IF NOT EXISTS macro_reaction_schedules(
                event_id TEXT NOT NULL,
                official_event_hash TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                PRIMARY KEY(event_id, official_event_hash)
            );
            CREATE TRIGGER IF NOT EXISTS macro_reaction_schedule_no_update
            BEFORE UPDATE ON macro_reaction_schedules
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction schedule'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_schedule_no_delete
            BEFORE DELETE ON macro_reaction_schedules
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction schedule'); END;
            CREATE TABLE IF NOT EXISTS macro_reaction_schedule_cache(
                sequence INTEGER PRIMARY KEY,
                stable_event_key TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                UNIQUE(stable_event_key, content_hash)
            );
            CREATE TRIGGER IF NOT EXISTS macro_reaction_schedule_cache_no_update
            BEFORE UPDATE ON macro_reaction_schedule_cache
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction schedule cache'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_schedule_cache_no_delete
            BEFORE DELETE ON macro_reaction_schedule_cache
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction schedule cache'); END;
            CREATE TABLE IF NOT EXISTS macro_reaction_schedule_generations(
                sequence INTEGER PRIMARY KEY,
                generation_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                UNIQUE(generation_id, content_hash)
            );
            CREATE TRIGGER IF NOT EXISTS macro_reaction_schedule_generation_no_update
            BEFORE UPDATE ON macro_reaction_schedule_generations
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction schedule generation'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_schedule_generation_no_delete
            BEFORE DELETE ON macro_reaction_schedule_generations
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction schedule generation'); END;
            CREATE TABLE IF NOT EXISTS macro_reaction_worker_failures(
                sequence INTEGER PRIMARY KEY,
                lane TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                reason TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE
            );
            CREATE TRIGGER IF NOT EXISTS macro_reaction_worker_failure_no_update
            BEFORE UPDATE ON macro_reaction_worker_failures
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction worker failure'); END;
            CREATE TRIGGER IF NOT EXISTS macro_reaction_worker_failure_no_delete
            BEFORE DELETE ON macro_reaction_worker_failures
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction worker failure'); END;
            """
        )

    def _initialize_schema(self) -> None:
        version = int(self._db.execute("PRAGMA user_version").fetchone()[0])
        table_exists = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='macro_reaction_evidence'"
        ).fetchone()
        if version == 0 and table_exists is None:
            self._db.executescript(
                """
                CREATE TABLE macro_reaction_evidence(
                    sequence INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    official_event_hash TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    document_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    prior_hash TEXT NOT NULL,
                    row_hash TEXT NOT NULL UNIQUE,
                    UNIQUE(event_id, official_event_hash, kind, content_hash)
                );
                CREATE TRIGGER macro_reaction_no_update
                BEFORE UPDATE ON macro_reaction_evidence
                BEGIN SELECT RAISE(ABORT, 'immutable macro reaction evidence'); END;
                CREATE TRIGGER macro_reaction_no_delete
                BEFORE DELETE ON macro_reaction_evidence
                BEGIN SELECT RAISE(ABORT, 'immutable macro reaction evidence'); END;
                CREATE TABLE macro_reaction_raw_documents(
                    sequence INTEGER PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    official_event_hash TEXT NOT NULL,
                    source_role TEXT NOT NULL,
                    official_url TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    raw_bytes BLOB NOT NULL,
                    content_hash TEXT NOT NULL,
                    prior_hash TEXT NOT NULL,
                    row_hash TEXT NOT NULL UNIQUE,
                    UNIQUE(event_id,official_event_hash,source_role,content_hash)
                );
                PRAGMA user_version=4;
                """
            )
            self._create_raw_document_triggers()
            return
        if version in {0, 1, 2} and table_exists is not None:
            self._migrate_legacy_to_v3(hash_version=1 if version in {0, 1} else 2)
            self._migrate_v3_to_v4()
            return
        if version == 3 and table_exists is not None:
            self._migrate_v3_to_v4()
            return
        if version != _REACTION_SCHEMA_VERSION or table_exists is None:
            raise MacroReactionError("REACTION_EVIDENCE_SCHEMA_UNSUPPORTED")
        self._create_immutability_triggers()
        self._create_raw_document_triggers()

    def _migrate_v3_to_v4(self) -> None:
        """Add the raw-document chain without rewriting legacy rows or hashes."""

        self._assert_integrity_rows(hash_version=3)
        before = tuple(self._db.execute("SELECT * FROM macro_reaction_evidence ORDER BY sequence").fetchall())
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.execute(
                "CREATE TABLE macro_reaction_raw_documents("
                "sequence INTEGER PRIMARY KEY,event_id TEXT NOT NULL,"
                "official_event_hash TEXT NOT NULL,source_role TEXT NOT NULL,"
                "official_url TEXT NOT NULL,received_at TEXT NOT NULL,"
                "media_type TEXT NOT NULL,raw_bytes BLOB NOT NULL,"
                "content_hash TEXT NOT NULL,prior_hash TEXT NOT NULL,"
                "row_hash TEXT NOT NULL UNIQUE,"
                "UNIQUE(event_id,official_event_hash,source_role,content_hash))"
            )
            self._create_raw_document_triggers()
            self._db.execute("PRAGMA user_version=4")
            after = tuple(self._db.execute("SELECT * FROM macro_reaction_evidence ORDER BY sequence").fetchall())
            if [tuple(row) for row in before] != [tuple(row) for row in after]:
                raise MacroReactionError("REACTION_LEGACY_ROWS_CHANGED")
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def _migrate_legacy_to_v3(self, *, hash_version: int) -> None:
        """Preserve legacy evidence without guessing its official identity.

        Old Jin10 observations contain no official schedule hash.  They are
        retained under an all-zero, permanently unbound identity so a later
        calendar refresh cannot silently reinterpret them as current evidence.
        Documents that already carry a valid event hash keep that exact hash.
        """

        self._assert_integrity_rows(hash_version=hash_version)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.execute("DROP TRIGGER IF EXISTS macro_reaction_no_update")
            self._db.execute("DROP TRIGGER IF EXISTS macro_reaction_no_delete")
            rows = self._db.execute(
                "SELECT * FROM macro_reaction_evidence ORDER BY sequence"
            ).fetchall()
            self._db.execute(
                "ALTER TABLE macro_reaction_evidence "
                "RENAME TO macro_reaction_evidence_legacy"
            )
            self._db.execute(
                "CREATE TABLE macro_reaction_evidence("
                "sequence INTEGER PRIMARY KEY,"
                "event_id TEXT NOT NULL,"
                "official_event_hash TEXT NOT NULL,"
                "kind TEXT NOT NULL,"
                "observed_at TEXT NOT NULL,"
                "document_json TEXT NOT NULL,"
                "content_hash TEXT NOT NULL,"
                "prior_hash TEXT NOT NULL,"
                "row_hash TEXT NOT NULL UNIQUE,"
                "UNIQUE(event_id,official_event_hash,kind,content_hash))"
            )
            prior_hash = "0" * 64
            for sequence, row in enumerate(rows, start=1):
                document = json.loads(str(row["document_json"]))
                document_event_hash = str(
                    document.get("official_event_hash")
                    or document.get("event_hash")
                    or ""
                ).lower()
                official_event_hash = (
                    document_event_hash
                    if _HASH_RE.fullmatch(document_event_hash)
                    else _LEGACY_UNBOUND_OFFICIAL_EVENT_HASH
                )
                row_hash = _reaction_row_hash(
                    sequence=sequence,
                    prior_hash=prior_hash,
                    event_id=str(row["event_id"]),
                    official_event_hash=official_event_hash,
                    kind=str(row["kind"]),
                    observed_at=str(row["observed_at"]),
                    content_hash=str(row["content_hash"]),
                )
                self._db.execute(
                    "INSERT INTO macro_reaction_evidence("
                    "sequence,event_id,official_event_hash,kind,observed_at,"
                    "document_json,content_hash,prior_hash,row_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        sequence,
                        str(row["event_id"]),
                        official_event_hash,
                        str(row["kind"]),
                        str(row["observed_at"]),
                        str(row["document_json"]),
                        str(row["content_hash"]),
                        prior_hash,
                        row_hash,
                    ),
                )
                prior_hash = row_hash
            self._db.execute("DROP TABLE macro_reaction_evidence_legacy")
            self._create_immutability_triggers()
            self._db.execute("PRAGMA user_version=3")
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def _create_immutability_triggers(self) -> None:
        self._db.execute(
            "CREATE TRIGGER IF NOT EXISTS macro_reaction_no_update "
            "BEFORE UPDATE ON macro_reaction_evidence "
            "BEGIN SELECT RAISE(ABORT, 'immutable macro reaction evidence'); END"
        )
        self._db.execute(
            "CREATE TRIGGER IF NOT EXISTS macro_reaction_no_delete "
            "BEFORE DELETE ON macro_reaction_evidence "
            "BEGIN SELECT RAISE(ABORT, 'immutable macro reaction evidence'); END"
        )

    def _create_raw_document_triggers(self) -> None:
        self._db.execute("CREATE TRIGGER IF NOT EXISTS macro_reaction_raw_no_update BEFORE UPDATE ON macro_reaction_raw_documents BEGIN SELECT RAISE(ABORT, 'immutable macro reaction raw document'); END")
        self._db.execute("CREATE TRIGGER IF NOT EXISTS macro_reaction_raw_no_delete BEFORE DELETE ON macro_reaction_raw_documents BEGIN SELECT RAISE(ABORT, 'immutable macro reaction raw document'); END")

    def append_raw_document(self, *, event_id: str, official_event_hash: str, source_role: str, official_url: str, received_at: datetime, media_type: str, raw_bytes: bytes) -> bool:
        """Append exact official bytes to the independent v4 document chain."""

        event_hash = str(official_event_hash).strip().lower()
        if not event_id.strip() or _HASH_RE.fullmatch(event_hash) is None or not source_role.strip() or not official_url.startswith("https://") or not media_type.strip() or not isinstance(raw_bytes, bytes) or not raw_bytes:
            raise ValueError("raw official document identity is invalid")
        received = utc_datetime(received_at, field="received_at").isoformat()
        content_hash = hashlib.sha256(raw_bytes).hexdigest()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                duplicate = self._db.execute("SELECT 1 FROM macro_reaction_raw_documents WHERE event_id=? AND official_event_hash=? AND source_role=? AND content_hash=?", (event_id, event_hash, source_role, content_hash)).fetchone()
                if duplicate is not None:
                    self._db.execute("COMMIT")
                    return False
                tail = self._db.execute("SELECT sequence,row_hash FROM macro_reaction_raw_documents ORDER BY sequence DESC LIMIT 1").fetchone()
                sequence = 1 if tail is None else int(tail["sequence"]) + 1
                prior_hash = "0" * 64 if tail is None else str(tail["row_hash"])
                row_hash = canonical_hash({"schema": "options_copilot.macro_reaction_raw_row.v1", "sequence": sequence, "prior_hash": prior_hash, "event_id": event_id, "official_event_hash": event_hash, "source_role": source_role, "official_url": official_url, "received_at": received, "media_type": media_type, "content_hash": content_hash})
                self._db.execute("INSERT INTO macro_reaction_raw_documents VALUES(?,?,?,?,?,?,?,?,?,?,?)", (sequence, event_id, event_hash, source_role, official_url, received, media_type, raw_bytes, content_hash, prior_hash, row_hash))
                self._db.execute("COMMIT")
                return True
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def raw_documents(self, event_id: str, official_event_hash: str) -> tuple[Mapping[str, object], ...]:
        with self._lock:
            self.assert_integrity()
            rows = self._db.execute("SELECT * FROM macro_reaction_raw_documents WHERE event_id=? AND official_event_hash=? ORDER BY sequence", (event_id, official_event_hash)).fetchall()
        return tuple(MappingProxyType({key: row[key] for key in row.keys()}) for row in rows)

    def append_verified_capture(self, *, event_id: str, captured: object) -> bool:
        """Persist discovery, raw bytes, and parsed vintage for one bound capture."""

        request = getattr(captured, "request", None)
        discovery = getattr(captured, "discovery", None)
        document = getattr(captured, "document", None)
        parsed = getattr(captured, "parsed", None)
        parent = getattr(request, "parent", None)
        if any(item is None for item in (request, discovery, document, parsed, parent)):
            raise ValueError("verified capture contract is incomplete")
        official_event_hash = str(request.official_event_hash).lower()
        parent_hash = str(parent.content_hash).lower()
        measure_identities = [
            {
                "measure_id": measure.measure_id,
                "measure_hash": MeasureIdentity(parent, measure.measure_id).content_hash,
                "value": None if measure.value is None else str(measure.value),
                "unit": measure.unit,
                "basis": measure.basis,
                "label": measure.label,
            }
            for measure in parsed.measures
        ]
        discovery_document = {
            "schema": "options_copilot.reaction_discovery_record.v1",
            "event_id": event_id,
            "official_event_hash": official_event_hash,
            "parent_id": parent.stable_id,
            "parent_hash": parent_hash,
            "family": parent.family.value,
            "feed_url": discovery.feed_url,
            "observed_at": discovery.observed_at.isoformat(),
            "feed_raw_hash": discovery.raw_hash,
            "fixed_path_identity_hash": discovery.identity_hash,
            "discovered_url": discovery.discovered_url,
            "discovery_hash": discovery.content_hash,
            "decision_authority": "SUPPORTING_ONLY",
        }
        vintage_document = {
            "schema": "options_copilot.reaction_release_vintage.v1",
            "event_id": event_id,
            "official_event_hash": official_event_hash,
            "parent_id": parent.stable_id,
            "parent_hash": parent_hash,
            "family": parsed.family.value,
            "reference_period": parsed.reference_period,
            "estimate_label": parsed.estimate_label,
            "official_url": document.url,
            "raw_hash": document.raw_hash,
            "document_hash": parsed.document_hash,
            "declared_release_at": None if parsed.declared_release_at is None else parsed.declared_release_at.isoformat(),
            "first_observed_release_at": None if parsed.first_observed_release_at is None else parsed.first_observed_release_at.isoformat(),
            "captured_at": document.received_at.isoformat(),
            "actual_parse_available": parsed.actual_parse_available,
            "measures": measure_identities,
            "revision_narrative": parsed.revision_narrative,
            "revision_of": parsed.revision_of,
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                inserted = self._append_extension_row("macro_reaction_discovery_records", event_id=event_id, official_event_hash=official_event_hash, parent_hash=parent_hash, raw_hash=None, observed_at=discovery.observed_at, document=discovery_document)
                raw_inserted = self._append_raw_document_in_transaction(event_id=event_id, official_event_hash=official_event_hash, source_role="OFFICIAL_RELEASE_DOCUMENT", official_url=document.url, received_at=document.received_at, media_type=document.media_type, raw_bytes=document.raw_bytes)
                vintage_inserted = self._append_extension_row("macro_reaction_release_vintages", event_id=event_id, official_event_hash=official_event_hash, parent_hash=parent_hash, raw_hash=document.raw_hash, observed_at=document.received_at, document=vintage_document)
                self._db.execute("COMMIT")
                return inserted or raw_inserted or vintage_inserted
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def _append_raw_document_in_transaction(self, *, event_id: str, official_event_hash: str, source_role: str, official_url: str, received_at: datetime, media_type: str, raw_bytes: bytes) -> bool:
        content_hash = hashlib.sha256(raw_bytes).hexdigest()
        duplicate = self._db.execute("SELECT 1 FROM macro_reaction_raw_documents WHERE event_id=? AND official_event_hash=? AND source_role=? AND content_hash=?", (event_id, official_event_hash, source_role, content_hash)).fetchone()
        if duplicate is not None:
            return False
        tail = self._db.execute("SELECT sequence,row_hash FROM macro_reaction_raw_documents ORDER BY sequence DESC LIMIT 1").fetchone()
        sequence = 1 if tail is None else int(tail["sequence"]) + 1
        prior_hash = "0" * 64 if tail is None else str(tail["row_hash"])
        observed = utc_datetime(received_at, field="received_at").isoformat()
        row_hash = canonical_hash({"schema": "options_copilot.macro_reaction_raw_row.v1", "sequence": sequence, "prior_hash": prior_hash, "event_id": event_id, "official_event_hash": official_event_hash, "source_role": source_role, "official_url": official_url, "received_at": observed, "media_type": media_type, "content_hash": content_hash})
        self._db.execute("INSERT INTO macro_reaction_raw_documents VALUES(?,?,?,?,?,?,?,?,?,?,?)", (sequence, event_id, official_event_hash, source_role, official_url, observed, media_type, raw_bytes, content_hash, prior_hash, row_hash))
        return True

    def _append_extension_row(self, table: str, *, event_id: str, official_event_hash: str, parent_hash: str, raw_hash: str | None, observed_at: datetime, document: Mapping[str, object]) -> bool:
        if table not in {"macro_reaction_discovery_records", "macro_reaction_release_vintages"}:
            raise ValueError("unsupported extension table")
        if table == "macro_reaction_release_vintages" and raw_hash is not None:
            duplicate_raw = self._db.execute(
                "SELECT 1 FROM macro_reaction_release_vintages "
                "WHERE event_id=? AND official_event_hash=? AND raw_hash=?",
                (event_id, official_event_hash, raw_hash),
            ).fetchone()
            if duplicate_raw is not None:
                return False
        if table == "macro_reaction_discovery_records":
            for existing in self._db.execute(
                "SELECT document_json FROM macro_reaction_discovery_records "
                "WHERE event_id=? AND official_event_hash=?",
                (event_id, official_event_hash),
            ).fetchall():
                value = json.loads(str(existing["document_json"]))
                if (
                    value.get("feed_raw_hash") == document.get("feed_raw_hash")
                    and value.get("discovered_url") == document.get("discovered_url")
                ):
                    return False
        rendered = canonical_json(document)
        content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
        duplicate = self._db.execute(f"SELECT 1 FROM {table} WHERE event_id=? AND official_event_hash=? AND content_hash=?", (event_id, official_event_hash, content_hash)).fetchone()
        if duplicate is not None:
            return False
        tail = self._db.execute(f"SELECT sequence,row_hash FROM {table} ORDER BY sequence DESC LIMIT 1").fetchone()
        sequence = 1 if tail is None else int(tail["sequence"]) + 1
        prior_hash = "0" * 64 if tail is None else str(tail["row_hash"])
        observed = utc_datetime(observed_at, field="observed_at").isoformat()
        row_document = {"schema": f"options_copilot.{table}_row.v1", "sequence": sequence, "prior_hash": prior_hash, "event_id": event_id, "official_event_hash": official_event_hash, "parent_hash": parent_hash, "observed_at": observed, "content_hash": content_hash}
        if raw_hash is not None:
            row_document["raw_hash"] = raw_hash
        row_hash = canonical_hash(row_document)
        columns = "sequence,event_id,official_event_hash,parent_hash,observed_at,document_json,content_hash,prior_hash,row_hash"
        values: tuple[object, ...] = (sequence, event_id, official_event_hash, parent_hash, observed, rendered, content_hash, prior_hash, row_hash)
        if raw_hash is not None:
            columns = "sequence,event_id,official_event_hash,parent_hash,raw_hash,observed_at,document_json,content_hash,prior_hash,row_hash"
            values = (sequence, event_id, official_event_hash, parent_hash, raw_hash, observed, rendered, content_hash, prior_hash, row_hash)
        self._db.execute(f"INSERT INTO {table}({columns}) VALUES({','.join('?' for _ in values)})", values)
        return True

    def release_vintages(self, event_id: str, official_event_hash: str) -> tuple[Mapping[str, object], ...]:
        with self._lock:
            self.assert_integrity()
            rows = self._db.execute("SELECT * FROM macro_reaction_release_vintages WHERE event_id=? AND official_event_hash=? ORDER BY sequence", (event_id, official_event_hash)).fetchall()
        return tuple(MappingProxyType({"sequence": int(row["sequence"]), "content_hash": str(row["content_hash"]), "row_hash": str(row["row_hash"]), "document": freeze_json(json.loads(str(row["document_json"])))}) for row in rows)

    def append(
        self,
        *,
        event_id: str,
        official_event_hash: str,
        kind: str,
        observed_at: datetime,
        document: Mapping[str, object],
    ) -> bool:
        normalized_event_hash = str(official_event_hash).strip().lower()
        if (
            not event_id.strip()
            or _HASH_RE.fullmatch(normalized_event_hash) is None
            or normalized_event_hash == _LEGACY_UNBOUND_OFFICIAL_EVENT_HASH
            or kind not in _REACTION_KINDS
        ):
            raise ValueError("macro reaction evidence identity is invalid")
        observed = utc_datetime(observed_at, field="observed_at")
        observed_text = observed.isoformat()
        rendered = canonical_json(document)
        content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                duplicate = self._db.execute(
                    "SELECT 1 FROM macro_reaction_evidence "
                    "WHERE event_id=? AND official_event_hash=? "
                    "AND kind=? AND content_hash=?",
                    (event_id, normalized_event_hash, kind, content_hash),
                ).fetchone()
                if duplicate is not None:
                    self._db.execute("COMMIT")
                    return False
                tail = self._db.execute(
                    "SELECT sequence,row_hash FROM macro_reaction_evidence "
                    "ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                sequence = 1 if tail is None else int(tail["sequence"]) + 1
                prior_hash = "0" * 64 if tail is None else str(tail["row_hash"])
                row_hash = _reaction_row_hash(
                    sequence=sequence,
                    prior_hash=prior_hash,
                    event_id=event_id,
                    official_event_hash=normalized_event_hash,
                    kind=kind,
                    observed_at=observed_text,
                    content_hash=content_hash,
                )
                self._db.execute(
                    "INSERT INTO macro_reaction_evidence("
                    "sequence,event_id,official_event_hash,kind,observed_at,"
                    "document_json,content_hash,prior_hash,row_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        sequence,
                        event_id,
                        normalized_event_hash,
                        kind,
                        observed_text,
                        rendered,
                        content_hash,
                        prior_hash,
                        row_hash,
                    ),
                )
                self._db.execute("COMMIT")
                return True
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def save_baseline(
        self,
        identity: ScheduledEventIdentity,
        sample: MarketSample,
    ) -> bool:
        document = {
            "schema": "options_copilot.reaction_observer_baseline.v1",
            "event_hash": identity.event_hash,
            "scheduled_at": identity.scheduled_at.isoformat(),
            "observed_at": sample.observed_at.isoformat(),
            "values": {key: str(value) for key, value in sample.values.items()},
            "decision_authority": "SUPPORTING_ONLY",
        }
        rendered = canonical_json(document)
        content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
        with self._lock:
            existing = self._db.execute(
                "SELECT document_json,content_hash FROM macro_reaction_observer_baselines WHERE event_hash=?",
                (identity.event_hash,),
            ).fetchone()
            if existing is not None:
                if str(existing["document_json"]) != rendered or str(existing["content_hash"]) != content_hash:
                    raise MacroReactionError("REACTION_BASELINE_CONFLICT")
                return False
            self._db.execute(
                "INSERT INTO macro_reaction_observer_baselines VALUES(?,?,?,?,?)",
                (
                    identity.event_hash,
                    identity.scheduled_at.isoformat(),
                    sample.observed_at.isoformat(),
                    rendered,
                    content_hash,
                ),
            )
        return True

    def load_baseline(
        self,
        event_hash: str,
        *,
        scheduled_at: datetime,
    ) -> MarketSample | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM macro_reaction_observer_baselines WHERE event_hash=?",
                (event_hash,),
            ).fetchone()
        if row is None or str(row["scheduled_at"]) != utc_datetime(
            scheduled_at,
            field="scheduled_at",
        ).isoformat():
            return None
        document = json.loads(str(row["document_json"]))
        values = document.get("values")
        if not isinstance(values, Mapping):
            raise MacroReactionError("REACTION_BASELINE_INVALID")
        return MarketSample(
            _parse_time(document["observed_at"]),
            {key: Decimal(str(value)) for key, value in values.items()},
        )

    def save_endpoint(
        self,
        identity: ScheduledEventIdentity,
        sample: MarketSample,
    ) -> bool:
        """Persist the first accepted T+5 endpoint under the stable lifecycle root."""

        document = {
            "schema": "options_copilot.reaction_observer_endpoint.v1",
            "event_hash": identity.event_hash,
            "scheduled_at": identity.scheduled_at.isoformat(),
            "logical_endpoint_at": (
                identity.scheduled_at + WINDOW_DURATION
            ).isoformat(),
            "observed_at": sample.observed_at.isoformat(),
            "values": {key: str(value) for key, value in sample.values.items()},
            "decision_authority": "SUPPORTING_ONLY",
        }
        rendered = canonical_json(document)
        content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
        with self._lock:
            existing = self._db.execute(
                "SELECT document_json,content_hash FROM "
                "macro_reaction_observer_endpoints WHERE event_hash=?",
                (identity.event_hash,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["document_json"]) != rendered
                    or str(existing["content_hash"]) != content_hash
                ):
                    raise MacroReactionError("REACTION_ENDPOINT_CONFLICT")
                return False
            self._db.execute(
                "INSERT INTO macro_reaction_observer_endpoints VALUES(?,?,?,?,?)",
                (
                    identity.event_hash,
                    identity.scheduled_at.isoformat(),
                    sample.observed_at.isoformat(),
                    rendered,
                    content_hash,
                ),
            )
        return True

    def load_endpoint(
        self,
        event_hash: str,
        *,
        scheduled_at: datetime,
    ) -> MarketSample | None:
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM macro_reaction_observer_endpoints WHERE event_hash=?",
                (event_hash,),
            ).fetchone()
        if row is None or str(row["scheduled_at"]) != utc_datetime(
            scheduled_at,
            field="scheduled_at",
        ).isoformat():
            return None
        document = json.loads(str(row["document_json"]))
        values = document.get("values")
        if not isinstance(values, Mapping):
            raise MacroReactionError("REACTION_ENDPOINT_INVALID")
        return MarketSample(
            _parse_time(document["observed_at"]),
            {key: Decimal(str(value)) for key, value in values.items()},
        )

    def save_schedule(
        self,
        event_id: str,
        spec: ScheduledReactionSpec,
        *,
        observed_at: datetime,
    ) -> bool:
        document = {
            "schema": "options_copilot.reaction_schedule.v1",
            "event_id": event_id,
            "official_event_hash": spec.official_event_hash,
            "parent_hash": spec.parent.content_hash,
            "scheduled_at": spec.scheduled_at.isoformat(),
            "capture_deadline": spec.capture_deadline.isoformat(),
            "decision_authority": "SUPPORTING_ONLY",
        }
        rendered = canonical_json(document)
        content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
        observed = utc_datetime(observed_at, field="observed_at").isoformat()
        with self._lock:
            existing = self._db.execute(
                "SELECT document_json,content_hash FROM macro_reaction_schedules WHERE event_id=? AND official_event_hash=?",
                (event_id, spec.official_event_hash),
            ).fetchone()
            if existing is not None:
                if str(existing["document_json"]) != rendered or str(existing["content_hash"]) != content_hash:
                    raise MacroReactionError("REACTION_SCHEDULE_CONFLICT")
                return False
            self._db.execute(
                "INSERT INTO macro_reaction_schedules VALUES(?,?,?,?,?)",
                (event_id, spec.official_event_hash, observed, rendered, content_hash),
            )
        return True

    def save_schedule_cache(
        self,
        descriptor: CalendarReactionDescriptor,
        spec: ScheduledReactionSpec | None,
        *,
        public_event_id: str,
        identity: ScheduledEventIdentity | None,
        observed_at: datetime,
        schedule_status: str,
        schedule_reason: str | None,
        schedule_hash: str | None,
        supersedes_reaction_root_hash: str | None = None,
    ) -> bool:
        """Append one full typed schedule version keyed by stable event identity."""

        document = {
            "schema": "options_copilot.reaction_schedule_cache.v1",
            "stable_event_key": descriptor.stable_event_key,
            "public_event_id": public_event_id,
            "publisher": descriptor.publisher,
            "family": descriptor.family.value,
            "source_id": descriptor.source_id,
            "source_url": descriptor.source_url,
            "title": descriptor.title,
            "scheduled_at": None if descriptor.scheduled_at is None else descriptor.scheduled_at.isoformat(),
            "schedule_precision": descriptor.schedule_precision,
            "reference_period": descriptor.reference_period,
            "estimate_label": descriptor.estimate_label,
            "meeting_range": descriptor.meeting_range,
            "calendar_event_hash": descriptor.calendar_event_hash,
            "calendar_record_hash": descriptor.calendar_record_hash,
            "calendar_feed_hash": descriptor.calendar_feed_hash,
            "descriptor_identity_hash": descriptor.identity_hash,
            "descriptor_provenance_hash": descriptor.version_provenance_hash,
            "wait_reason": descriptor.wait_reason,
            "identity": None if identity is None else identity.as_dict(),
            "parent": None if spec is None else {
                "publisher": spec.parent.publisher,
                "family": spec.parent.family.value,
                "reference_period": spec.parent.reference_period,
                "scheduled_date": spec.parent.scheduled_date.isoformat(),
                "estimate_label": spec.parent.estimate_label,
                "parent_hash": spec.parent.content_hash,
            },
            "capture_deadline": None if spec is None else spec.capture_deadline.isoformat(),
            "next_eligible_release_at": None if spec is None or spec.next_eligible_release_at is None else spec.next_eligible_release_at.isoformat(),
            "measure_hashes": [] if spec is None else [
                MeasureIdentity(spec.parent, item.measure_id).content_hash
                for item in FAMILY_SPECS[spec.parent.family].measures
            ],
            "schedule_status": schedule_status,
            "schedule_reason": schedule_reason,
            "schedule_hash": schedule_hash,
            "supersedes_reaction_root_hash": supersedes_reaction_root_hash,
            "decision_authority": "SUPPORTING_ONLY",
        }
        return self._append_cache_row(
            "macro_reaction_schedule_cache",
            key_name="stable_event_key",
            key_value=descriptor.stable_event_key,
            observed_at=observed_at,
            document=document,
        )

    def save_schedule_generation(
        self,
        rows: Sequence[
            tuple[
                str,
                CalendarReactionDescriptor | None,
                ScheduledReactionSpec | None,
                str,
                ScheduledEventIdentity | None,
                datetime,
                str | None,
            ]
        ],
        *,
        observed_at: datetime,
        schedule_status: str,
        schedule_reason: str | None,
        schedule_hash: str | None,
        unsupported_count: int,
        active_roots: Sequence[Mapping[str, object]],
        attempted_at: datetime | None,
    ) -> bool:
        """Persist one complete schedule generation in a single transaction."""

        inserted = False
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for (
                    stable_event_key,
                    descriptor,
                    spec,
                    public_event_id,
                    identity,
                    row_observed_at,
                    supersedes_reaction_root_hash,
                ) in rows:
                    if spec is not None:
                        inserted = self.save_schedule(
                            stable_event_key,
                            spec,
                            observed_at=row_observed_at,
                        ) or inserted
                    if descriptor is not None:
                        inserted = self.save_schedule_cache(
                            descriptor,
                            spec,
                            public_event_id=public_event_id,
                            identity=identity,
                            observed_at=row_observed_at,
                            schedule_status=schedule_status,
                            schedule_reason=schedule_reason,
                            schedule_hash=schedule_hash,
                            supersedes_reaction_root_hash=(
                                supersedes_reaction_root_hash
                            ),
                        ) or inserted
                root_documents = tuple(
                    freeze_json(dict(item))
                    for item in sorted(
                        active_roots,
                        key=lambda item: str(item.get("stable_event_key") or ""),
                    )
                )
                root_set_hash = canonical_hash(root_documents)
                authority_document = {
                    "schema": "options_copilot.reaction_schedule_generation.v2",
                    "observed_at": utc_datetime(
                        observed_at,
                        field="observed_at",
                    ).isoformat(),
                    "attempted_at": (
                        None
                        if attempted_at is None
                        else utc_datetime(
                            attempted_at,
                            field="attempted_at",
                        ).isoformat()
                    ),
                    "active_roots": root_documents,
                    "root_set_hash": root_set_hash,
                    "schedule_status": schedule_status,
                    "schedule_reason": schedule_reason,
                    "schedule_hash": schedule_hash,
                    "unsupported_count": unsupported_count,
                    "decision_authority": "SUPPORTING_ONLY",
                }
                generation_id = canonical_hash(authority_document)
                authority_document["generation_id"] = generation_id
                inserted = self._append_cache_row(
                    "macro_reaction_schedule_generations",
                    key_name="generation_id",
                    key_value=generation_id,
                    observed_at=observed_at,
                    document=authority_document,
                ) or inserted
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return inserted

    def load_schedule_generation_authority(self) -> Mapping[str, object] | None:
        """Load the latest verified generation marker by time then append order."""

        self.assert_integrity()
        with self._lock:
            row = self._db.execute(
                "SELECT document_json FROM macro_reaction_schedule_generations "
                "ORDER BY observed_at DESC, sequence DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        document = json.loads(str(row["document_json"]))
        generation_id = str(document.get("generation_id") or "")
        generation_basis = dict(document)
        generation_basis.pop("generation_id", None)
        active_roots = document.get("active_roots")
        if (
            not _valid_hash(generation_id)
            or canonical_hash(generation_basis) != generation_id
            or
            not isinstance(active_roots, Sequence)
            or isinstance(active_roots, (str, bytes, bytearray, memoryview))
            or canonical_hash(tuple(active_roots)) != document.get("root_set_hash")
        ):
            raise MacroReactionError("REACTION_SCHEDULE_AUTHORITY_INVALID")
        attempted_at = document.get("attempted_at")
        if attempted_at is not None:
            try:
                _parse_time(attempted_at)
            except Exception as exc:
                raise MacroReactionError(
                    "REACTION_SCHEDULE_AUTHORITY_INVALID"
                ) from exc
        return MappingProxyType(freeze_json(document))

    def load_schedule_cache(self) -> tuple[Mapping[str, object], ...]:
        self.assert_integrity()
        rows = self._db.execute(
            "SELECT * FROM macro_reaction_schedule_cache "
            "ORDER BY stable_event_key, observed_at, sequence"
        ).fetchall()
        latest: dict[str, Mapping[str, object]] = {}
        for row in rows:
            latest[str(row["stable_event_key"])] = MappingProxyType(
                {
                    "sequence": int(row["sequence"]),
                    "observed_at": str(row["observed_at"]),
                    "document": freeze_json(json.loads(str(row["document_json"]))),
                    "content_hash": str(row["content_hash"]),
                    "row_hash": str(row["row_hash"]),
                }
            )
        return tuple(latest[key] for key in sorted(latest))

    def record_worker_failure(
        self,
        lane: str,
        *,
        attempted_at: datetime,
        reason: str,
    ) -> bool:
        checked_lane = str(lane).strip().upper()
        checked_reason = str(reason).strip().upper()
        if checked_lane not in {"SCHEDULE", "CAPTURE", "OBSERVER"} or re.fullmatch(
            r"[A-Z0-9_:-]+", checked_reason
        ) is None:
            raise ValueError("reaction worker failure identity is invalid")
        document = {
            "schema": "options_copilot.reaction_worker_failure.v1",
            "lane": checked_lane,
            "attempted_at": utc_datetime(attempted_at, field="attempted_at").isoformat(),
            "reason": checked_reason,
            "decision_authority": "SUPPORTING_ONLY",
        }
        return self._append_cache_row(
            "macro_reaction_worker_failures",
            key_name="lane",
            key_value=checked_lane,
            observed_at=attempted_at,
            document=document,
            reason=checked_reason,
        )

    def worker_failures(self) -> tuple[Mapping[str, object], ...]:
        self.assert_integrity()
        rows = self._db.execute(
            "SELECT document_json FROM macro_reaction_worker_failures ORDER BY sequence"
        ).fetchall()
        return tuple(
            MappingProxyType(json.loads(str(row["document_json"]))) for row in rows
        )

    def _append_cache_row(
        self,
        table: str,
        *,
        key_name: str,
        key_value: str,
        observed_at: datetime,
        document: Mapping[str, object],
        reason: str | None = None,
    ) -> bool:
        rendered = canonical_json(document)
        content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
        observed = utc_datetime(observed_at, field="observed_at").isoformat()
        with self._lock:
            duplicate = self._db.execute(
                f"SELECT 1 FROM {table} WHERE {key_name}=? AND content_hash=?",
                (key_value, content_hash),
            ).fetchone()
            if duplicate is not None:
                return False
            tail = self._db.execute(
                f"SELECT sequence,row_hash FROM {table} ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = 1 if tail is None else int(tail["sequence"]) + 1
            prior_hash = "0" * 64 if tail is None else str(tail["row_hash"])
            row_hash = canonical_hash(
                {
                    "schema": f"options_copilot.{table}_row.v1",
                    "sequence": sequence,
                    "prior_hash": prior_hash,
                    key_name: key_value,
                    "observed_at": observed,
                    "reason": reason,
                    "content_hash": content_hash,
                }
            )
            columns = [
                "sequence",
                key_name,
                (
                    "observed_at"
                    if table
                    in {
                        "macro_reaction_schedule_cache",
                        "macro_reaction_schedule_generations",
                    }
                    else "attempted_at"
                ),
            ]
            values: list[object] = [sequence, key_value, observed]
            if table == "macro_reaction_worker_failures":
                columns.append("reason")
                values.append(reason)
            columns.extend(("document_json", "content_hash", "prior_hash", "row_hash"))
            values.extend((rendered, content_hash, prior_hash, row_hash))
            self._db.execute(
                f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in values)})",
                tuple(values),
            )
        return True

    def records(
        self,
        event_keys: Sequence[tuple[str, str]],
    ) -> tuple[Mapping[str, object], ...]:
        wanted = tuple(
            dict.fromkeys(
                (
                    str(event_id).strip(),
                    str(official_event_hash).strip().lower(),
                )
                for event_id, official_event_hash in event_keys
                if str(event_id).strip()
            )
        )
        if not wanted:
            return ()
        if len(wanted) > 500:
            raise ValueError("at most 500 reaction event identities may be read")
        if any(_HASH_RE.fullmatch(event_hash) is None for _, event_hash in wanted):
            raise ValueError("reaction event hash is invalid")
        with self._lock:
            self.assert_integrity()
            predicates = " OR ".join(
                "(event_id=? AND official_event_hash=?)" for _ in wanted
            )
            parameters = tuple(item for pair in wanted for item in pair)
            rows = self._db.execute(
                "SELECT * FROM macro_reaction_evidence WHERE "
                f"{predicates} ORDER BY sequence",
                parameters,
            ).fetchall()
        return tuple(
            MappingProxyType(
                {
                    "sequence": int(row["sequence"]),
                    "event_id": str(row["event_id"]),
                    "official_event_hash": str(row["official_event_hash"]),
                    "kind": str(row["kind"]),
                    "observed_at": str(row["observed_at"]),
                    "document": freeze_json(
                        json.loads(str(row["document_json"]))
                    ),
                    "content_hash": str(row["content_hash"]),
                    "row_hash": str(row["row_hash"]),
                }
            )
            for row in rows
        )

    def verified_event_batch(
        self,
        event_keys: Sequence[tuple[str, str]],
    ) -> VerifiedReactionBatch:
        """Verify all chains and materialize requested rows in one read snapshot."""

        return self._verified_event_batch(event_keys, include_expectation_children=False)

    def verified_reaction_tree_batch(
        self,
        parent_event_keys: Sequence[tuple[str, str]],
    ) -> VerifiedReactionBatch:
        """Verify and materialize parent plus expectation-derived child rows."""

        return self._verified_event_batch(
            parent_event_keys,
            include_expectation_children=True,
        )

    def _verified_event_batch(
        self,
        event_keys: Sequence[tuple[str, str]],
        *,
        include_expectation_children: bool,
    ) -> VerifiedReactionBatch:

        wanted = tuple(
            dict.fromkeys(
                (
                    str(event_id).strip(),
                    str(official_event_hash).strip().lower(),
                )
                for event_id, official_event_hash in event_keys
            )
        )
        if len(wanted) > 500:
            raise ValueError("at most 500 reaction event identities may be read")
        if any(
            not event_id or _HASH_RE.fullmatch(event_hash) is None
            for event_id, event_hash in wanted
        ):
            raise ValueError("reaction event identity is invalid")

        with self._lock:
            owns_snapshot = not self._db.in_transaction
            if owns_snapshot:
                self._db.execute("BEGIN")
            try:
                if owns_snapshot:
                    # Establish the WAL read snapshot before another connection
                    # can publish a generation that this verification did not see.
                    self._db.execute(
                        "SELECT 1 FROM sqlite_master LIMIT 1"
                    ).fetchone()
                self.assert_integrity()
                evidence_rows: Sequence[sqlite3.Row] = ()
                vintage_rows: Sequence[sqlite3.Row] = ()
                if wanted:
                    predicates = " OR ".join(
                        "(event_id=? AND official_event_hash=?)" for _ in wanted
                    )
                    parameters = tuple(item for pair in wanted for item in pair)
                    evidence_rows = self._db.execute(
                        "SELECT * FROM macro_reaction_evidence WHERE "
                        f"{predicates} ORDER BY sequence",
                        parameters,
                    ).fetchall()
                    if include_expectation_children:
                        derived_keys: list[tuple[str, str]] = []
                        for row in evidence_rows:
                            if str(row["kind"]) != "JIN10_EXPECTATION":
                                continue
                            try:
                                observation = _observation_from_document(
                                    json.loads(str(row["document_json"]))
                                )
                            except Exception:
                                continue
                            derived_keys.append(
                                (
                                    f"{row['event_id']}:{observation.metric}",
                                    str(row["official_event_hash"]),
                                )
                            )
                        expanded = tuple(dict.fromkeys((*wanted, *derived_keys)))
                        if len(expanded) > 500:
                            raise ValueError(
                                "at most 500 reaction event identities may be read"
                            )
                        if expanded != wanted:
                            wanted = expanded
                            predicates = " OR ".join(
                                "(event_id=? AND official_event_hash=?)"
                                for _ in wanted
                            )
                            parameters = tuple(
                                item for pair in wanted for item in pair
                            )
                            evidence_rows = self._db.execute(
                                "SELECT * FROM macro_reaction_evidence WHERE "
                                f"{predicates} ORDER BY sequence",
                                parameters,
                            ).fetchall()
                    vintage_rows = self._db.execute(
                        "SELECT * FROM macro_reaction_release_vintages WHERE "
                        f"{predicates} ORDER BY sequence",
                        parameters,
                    ).fetchall()
                evidence_by_key: dict[
                    tuple[str, str], list[Mapping[str, object]]
                ] = {key: [] for key in wanted}
                vintages_by_key: dict[
                    tuple[str, str], list[Mapping[str, object]]
                ] = {key: [] for key in wanted}
                for row in evidence_rows:
                    key = (str(row["event_id"]), str(row["official_event_hash"]))
                    evidence_by_key[key].append(
                        MappingProxyType(
                            {
                                "sequence": int(row["sequence"]),
                                "event_id": key[0],
                                "official_event_hash": key[1],
                                "kind": str(row["kind"]),
                                "observed_at": str(row["observed_at"]),
                                "document": freeze_json(
                                    json.loads(str(row["document_json"]))
                                ),
                                "content_hash": str(row["content_hash"]),
                                "row_hash": str(row["row_hash"]),
                            }
                        )
                    )
                for row in vintage_rows:
                    key = (str(row["event_id"]), str(row["official_event_hash"]))
                    vintages_by_key[key].append(
                        MappingProxyType(
                            {
                                "sequence": int(row["sequence"]),
                                "content_hash": str(row["content_hash"]),
                                "row_hash": str(row["row_hash"]),
                                "document": freeze_json(
                                    json.loads(str(row["document_json"]))
                                ),
                            }
                        )
                    )
                batch = VerifiedReactionBatch(
                    event_keys=wanted,
                    evidence_by_key=MappingProxyType(
                        {key: tuple(rows) for key, rows in evidence_by_key.items()}
                    ),
                    vintages_by_key=MappingProxyType(
                        {key: tuple(rows) for key, rows in vintages_by_key.items()}
                    ),
                )
                if owns_snapshot:
                    self._db.execute("COMMIT")
                return batch
            except BaseException:
                if owns_snapshot and self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise

    def assert_integrity(self) -> None:
        with self._lock:
            version = int(self._db.execute("PRAGMA user_version").fetchone()[0])
            if version != _REACTION_SCHEMA_VERSION:
                raise MacroReactionError("REACTION_EVIDENCE_SCHEMA_UNSUPPORTED")
            self._assert_integrity_rows(hash_version=3)
            self._assert_raw_document_integrity()
            self._assert_extension_integrity("macro_reaction_discovery_records", raw_hash=False)
            self._assert_extension_integrity("macro_reaction_release_vintages", raw_hash=True)
            self._assert_observer_state_integrity()
            self._assert_cache_chain_integrity(
                "macro_reaction_schedule_cache",
                key_name="stable_event_key",
                time_name="observed_at",
            )
            self._assert_cache_chain_integrity(
                "macro_reaction_schedule_generations",
                key_name="generation_id",
                time_name="observed_at",
            )
            self._assert_cache_chain_integrity(
                "macro_reaction_worker_failures",
                key_name="lane",
                time_name="attempted_at",
                include_reason=True,
            )

    def _assert_observer_state_integrity(self) -> None:
        for table in (
            "macro_reaction_observer_baselines",
            "macro_reaction_observer_endpoints",
            "macro_reaction_schedules",
        ):
            rows = self._db.execute(f"SELECT * FROM {table}").fetchall()
            for row in rows:
                rendered = str(row["document_json"])
                if (
                    canonical_json(json.loads(rendered)) != rendered
                    or hashlib.sha256(rendered.encode("utf-8")).hexdigest()
                    != str(row["content_hash"])
                ):
                    raise MacroReactionError("REACTION_OBSERVER_STATE_INTEGRITY_FAILED")

    def _assert_extension_integrity(self, table: str, *, raw_hash: bool) -> None:
        prior = "0" * 64
        rows = self._db.execute(f"SELECT * FROM {table} ORDER BY sequence").fetchall()
        for sequence, row in enumerate(rows, start=1):
            rendered = str(row["document_json"])
            content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
            row_document = {"schema": f"options_copilot.{table}_row.v1", "sequence": sequence, "prior_hash": prior, "event_id": str(row["event_id"]), "official_event_hash": str(row["official_event_hash"]), "parent_hash": str(row["parent_hash"]), "observed_at": str(row["observed_at"]), "content_hash": content_hash}
            if raw_hash:
                row_document["raw_hash"] = str(row["raw_hash"])
            expected = canonical_hash(row_document)
            if canonical_json(json.loads(rendered)) != rendered or str(row["content_hash"]) != content_hash or str(row["prior_hash"]) != prior or str(row["row_hash"]) != expected:
                raise MacroReactionError("REACTION_V4_EXTENSION_INTEGRITY_FAILED")
            prior = expected

    def _assert_cache_chain_integrity(
        self,
        table: str,
        *,
        key_name: str,
        time_name: str,
        include_reason: bool = False,
    ) -> None:
        prior = "0" * 64
        rows = self._db.execute(f"SELECT * FROM {table} ORDER BY sequence").fetchall()
        for sequence, row in enumerate(rows, start=1):
            rendered = str(row["document_json"])
            content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
            row_document = {
                "schema": f"options_copilot.{table}_row.v1",
                "sequence": sequence,
                "prior_hash": prior,
                key_name: str(row[key_name]),
                "observed_at": str(row[time_name]),
                "reason": str(row["reason"]) if include_reason else None,
                "content_hash": content_hash,
            }
            expected = canonical_hash(row_document)
            if (
                int(row["sequence"]) != sequence
                or canonical_json(json.loads(rendered)) != rendered
                or str(row["content_hash"]) != content_hash
                or str(row["prior_hash"]) != prior
                or str(row["row_hash"]) != expected
            ):
                raise MacroReactionError("REACTION_CACHE_INTEGRITY_FAILED")
            prior = expected

    def _assert_raw_document_integrity(self) -> None:
        prior = "0" * 64
        rows = self._db.execute("SELECT * FROM macro_reaction_raw_documents ORDER BY sequence").fetchall()
        for expected_sequence, row in enumerate(rows, start=1):
            content_hash = hashlib.sha256(bytes(row["raw_bytes"])).hexdigest()
            expected = canonical_hash({"schema": "options_copilot.macro_reaction_raw_row.v1", "sequence": expected_sequence, "prior_hash": prior, "event_id": str(row["event_id"]), "official_event_hash": str(row["official_event_hash"]), "source_role": str(row["source_role"]), "official_url": str(row["official_url"]), "received_at": str(row["received_at"]), "media_type": str(row["media_type"]), "content_hash": content_hash})
            if int(row["sequence"]) != expected_sequence or str(row["prior_hash"]) != prior or str(row["content_hash"]) != content_hash or str(row["row_hash"]) != expected:
                raise MacroReactionError("REACTION_RAW_DOCUMENT_INTEGRITY_FAILED")
            prior = expected

    def _assert_integrity_rows(self, *, hash_version: int) -> None:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM macro_reaction_evidence ORDER BY sequence"
            ).fetchall()
        prior = "0" * 64
        for sequence, row in enumerate(rows, start=1):
            try:
                rendered = str(row["document_json"])
                content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
                if hash_version == 1:
                    expected = hashlib.sha256(
                        f"{sequence}:{prior}:{content_hash}".encode("ascii")
                    ).hexdigest()
                elif hash_version == 2:
                    expected = _reaction_row_hash_v2(
                        sequence=sequence,
                        prior_hash=prior,
                        event_id=str(row["event_id"]),
                        kind=str(row["kind"]),
                        observed_at=str(row["observed_at"]),
                        content_hash=content_hash,
                    )
                else:
                    official_event_hash = str(row["official_event_hash"])
                    expected = _reaction_row_hash(
                        sequence=sequence,
                        prior_hash=prior,
                        event_id=str(row["event_id"]),
                        official_event_hash=official_event_hash,
                        kind=str(row["kind"]),
                        observed_at=str(row["observed_at"]),
                        content_hash=content_hash,
                    )
                valid = (
                    int(row["sequence"]) == sequence
                    and str(row["event_id"]).strip() != ""
                    and (
                        hash_version < 3
                        or _HASH_RE.fullmatch(str(row["official_event_hash"]))
                        is not None
                    )
                    and str(row["kind"]) in _REACTION_KINDS
                    and canonical_json(json.loads(rendered)) == rendered
                    and str(row["content_hash"]) == content_hash
                    and str(row["prior_hash"]) == prior
                    and str(row["row_hash"]) == expected
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                valid = False
            if not valid:
                raise MacroReactionError("REACTION_EVIDENCE_INTEGRITY_FAILED")
            prior = expected

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise HTTPError(req.full_url, code, "redirect disabled", headers, fp)


@dataclass(frozen=True, slots=True)
class LatestRevisedSeriesObservation:
    series_id: str
    period: str
    calculation: str
    value: Decimal
    raw_calculated_value: Decimal
    observed_at: datetime

    @property
    def source_role(self) -> str:
        return "LATEST_REVISED_SERIES"


class BlsPublicDataActualProvider:
    """GET-only BLS latest-revised-series corroboration adapter.

    The historical class name remains for compatibility, but ``actual`` never
    creates an initial official release. Only a bound BLS release document may
    do that.
    """

    def __init__(self, *, opener: object | None = None, timeout_seconds: float = 8.0) -> None:
        self._opener = opener or build_opener(ProxyHandler(), _NoRedirect())
        self._timeout = timeout_seconds

    def actual(
        self,
        identity: ScheduledEventIdentity,
        expectation: ConsensusExpectation,
        *,
        series_id: str,
        calculation: str,
        captured_at: datetime,
    ) -> OfficialRelease | None:
        return None

    def latest_revised(
        self,
        *,
        series_id: str,
        period: str,
        calculation: str,
        observed_at: datetime,
    ) -> LatestRevisedSeriesObservation | None:
        checked_at = utc_datetime(observed_at, field="observed_at")
        if series_id not in set(_BLS_SERIES.values()) | {"CES0000000001", "LNS14000000"}:
            return None
        year, month = (int(item) for item in period.split("-")[:2])
        get_url = f"{_BLS_API_URL}{series_id}?{urlencode({'startyear': year - 1, 'endyear': year})}"
        request = Request(get_url, headers={"Accept": "application/json", "User-Agent": "OptionsCopilot/1.0 BLS corroboration"}, method="GET")
        values: dict[tuple[int, int], Decimal] = {}
        try:
            response = self._opener.open(request, timeout=self._timeout)  # type: ignore[union-attr]
            with response:
                if str(response.geturl()) != get_url:
                    return None
                content_type = str(response.headers.get("Content-Type") or "").lower()
                if "json" not in content_type:
                    return None
                raw = response.read(_MAXIMUM_BLS_BYTES + 1)
            if len(raw) > _MAXIMUM_BLS_BYTES:
                return None
            payload = json.loads(raw.decode("utf-8"))
            values = _bls_values(payload, series_id)
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
            return None
        current = values.get((year, month))
        if current is None:
            return None
        if calculation == "LEVEL":
            raw_value = current
            value = current
        else:
            prior = values.get((year - 1, month)) if calculation == "YOY" else values.get(((year - 1, 12) if month == 1 else (year, month - 1)))
            if prior in (None, Decimal("0")):
                return None
            raw_value = ((current - prior) / prior * Decimal("100")).quantize(Decimal("0.0001"))
            value = raw_value.quantize(_BLS_PUBLISHED_INCREMENT, rounding=ROUND_HALF_UP)
        return LatestRevisedSeriesObservation(series_id, period, calculation, value, raw_value, checked_at)

class ProductionReactionObserver:
    """Build reaction/reprice evidence only from fresh local cached bindings."""

    def __init__(
        self,
        news_adapter: object,
        pacing_guard: object,
        *,
        store: ReactionEvidenceStore | None = None,
    ) -> None:
        self._news_adapter = news_adapter
        self._pacing_guard = pacing_guard
        self._store = store
        self._baselines: dict[str, MarketSample] = {}
        self._endpoints: dict[str, MarketSample] = {}
        self._lock = threading.RLock()
        self.last_reason = "NOT_OBSERVED"

    def tick(self, identity: ScheduledEventIdentity, *, now: datetime) -> bool:
        """Capture baseline or first eligible endpoint using only the local quote port."""

        checked_at = utc_datetime(now, field="now")
        if checked_at <= identity.scheduled_at:
            return self.arm(identity, now=checked_at)
        endpoint_at = identity.scheduled_at + WINDOW_DURATION
        if checked_at < endpoint_at or checked_at > endpoint_at + _ENDPOINT_GRACE:
            return False
        with self._lock:
            if identity.event_hash in self._endpoints:
                return True
            if self._store is not None:
                restored = self._store.load_endpoint(
                    identity.event_hash,
                    scheduled_at=identity.scheduled_at,
                )
                if restored is not None:
                    self._endpoints[identity.event_hash] = restored
                    self.last_reason = "PROSPECTIVE_FIVE_MINUTE_ENDPOINT_RESTORED"
                    return True
        sample = self._read_market_sample(identity, now=checked_at)
        if (
            sample is None
            or sample.observed_at < endpoint_at
            or sample.observed_at > endpoint_at + _ENDPOINT_GRACE
        ):
            self.last_reason = "WAITING_PROSPECTIVE_FIVE_MINUTE_ENDPOINT"
            return False
        with self._lock:
            self._endpoints[identity.event_hash] = sample
            if self._store is not None:
                self._store.save_endpoint(identity, sample)
        self.last_reason = "PROSPECTIVE_FIVE_MINUTE_ENDPOINT_CAPTURED"
        return True

    def arm(self, identity: ScheduledEventIdentity, *, now: datetime) -> bool:
        """Capture one pre-release baseline; never backfill it after T0."""

        checked_at = utc_datetime(now, field="now")
        if checked_at > identity.scheduled_at or identity.scheduled_at - checked_at > _BASELINE_ARM_LEAD:
            return False
        with self._lock:
            if identity.event_hash in self._baselines:
                return True
            if self._store is not None:
                restored = self._store.load_baseline(
                    identity.event_hash,
                    scheduled_at=identity.scheduled_at,
                )
                if restored is not None:
                    self._baselines[identity.event_hash] = restored
                    self.last_reason = "PROSPECTIVE_PRE_RELEASE_BASELINE_RESTORED"
                    return True
        sample = self._read_market_sample(identity, now=checked_at)
        if sample is None or sample.observed_at > identity.scheduled_at:
            self.last_reason = "WAITING_PROSPECTIVE_PRE_RELEASE_BASELINE"
            return False
        with self._lock:
            self._baselines[identity.event_hash] = sample
            if self._store is not None:
                self._store.save_baseline(identity, sample)
        self.last_reason = "PROSPECTIVE_PRE_RELEASE_BASELINE_ARMED"
        return True

    def observe(
        self,
        ledger: EventReactionLedger,
        *,
        now: datetime,
    ) -> tuple[MarketReactionEvidence | None, OptionReevaluationEvidence | None]:
        checked_at = utc_datetime(now, field="now")
        if getattr(self._pacing_guard, "ready", False) is not True:
            self.last_reason = "PACING_BLOCKED"
            return None, None
        release = ledger.release_chain[-1]
        with self._lock:
            baseline = self._baselines.get(ledger.identity.event_hash)
            endpoint = self._endpoints.get(ledger.identity.event_hash)
            if baseline is None and self._store is not None:
                baseline = self._store.load_baseline(
                    ledger.identity.event_hash,
                    scheduled_at=ledger.identity.scheduled_at,
                )
                if baseline is not None:
                    self._baselines[ledger.identity.event_hash] = baseline
            if endpoint is None and self._store is not None:
                endpoint = self._store.load_endpoint(
                    ledger.identity.event_hash,
                    scheduled_at=ledger.identity.scheduled_at,
                )
                if endpoint is not None:
                    self._endpoints[ledger.identity.event_hash] = endpoint
        if baseline is None:
            self.last_reason = "WAITING_PROSPECTIVE_PRE_RELEASE_BASELINE"
            return None, None
        endpoint_at = ledger.identity.scheduled_at + WINDOW_DURATION
        if checked_at < endpoint_at:
            self.last_reason = "WAITING_PROSPECTIVE_FIVE_MINUTE_ENDPOINT"
            return None, None
        if (
            endpoint is None
            or endpoint.observed_at < endpoint_at
            or endpoint.observed_at > endpoint_at + _ENDPOINT_GRACE
        ):
            self.last_reason = "WAITING_PROSPECTIVE_FIVE_MINUTE_ENDPOINT"
            return None, None
        window = ProspectiveMarketWindow(
            ledger.identity.event_hash,
            release.content_hash,
            release.released_at,
            baseline.observed_at,
            (endpoint,),
            baseline,
        )
        projection = window.projection(now=checked_at)
        if projection["complete"] is not True:
            self.last_reason = str(projection["reason"])
            return None, None
        deltas = {
            symbol: endpoint.values[symbol] - baseline.values[symbol]
            for symbol in baseline.values.keys() & endpoint.values.keys()
        }
        market = MarketReactionEvidence(
            event_hash=ledger.identity.event_hash,
            release_hash=release.content_hash,
            source="IBKR_READ_ONLY",
            window_start=release.released_at,
            window_end=endpoint_at,
            evidence_asof=endpoint.observed_at,
            observed_at=checked_at,
            metrics={
                "baseline": {key: str(value) for key, value in baseline.values.items()},
                "endpoint": {key: str(value) for key, value in endpoint.values.items()},
                "endpoint_observed_at": endpoint.observed_at.isoformat(),
                "logical_endpoint_at": endpoint_at.isoformat(),
                "delta": {key: str(value) for key, value in deltas.items()},
                "prospective_window_hash": projection["content_hash"],
                "decision_authority": "SUPPORTING_ONLY",
            },
        )
        option = self.reevaluate_option(ledger, market, now=checked_at)
        return market, option

    def _read_market_sample(
        self,
        identity: ScheduledEventIdentity,
        *,
        now: datetime,
    ) -> MarketSample | None:
        bindings_reader = getattr(
            self._news_adapter,
            "cached_reaction_underlying_quotes",
            None,
        )
        if not callable(bindings_reader):
            self.last_reason = "REACTION_QUOTE_CACHE_UNAVAILABLE"
            return None
        try:
            bindings = tuple(bindings_reader(_MACRO_SYMBOLS))
        except Exception:
            self.last_reason = "REACTION_QUOTE_UNAVAILABLE"
            return None
        symbols = tuple(str(getattr(item, "symbol", "")).upper() for item in bindings)
        timestamps = tuple(getattr(item, "observed_at", None) for item in bindings)
        numeric_rows = tuple(
            (getattr(item, "bid", None), getattr(item, "ask", None))
            for item in bindings
        )
        if (
            len(bindings) != len(_MACRO_SYMBOLS)
            or len(set(symbols)) != len(symbols)
            or set(symbols) != set(_MACRO_SYMBOLS)
            or any(
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() is None
                for value in timestamps
            )
            or len(set(timestamps)) != 1
            or any(
                not isinstance(bid, Decimal)
                or not isinstance(ask, Decimal)
                or not bid.is_finite()
                or not ask.is_finite()
                or bid <= 0
                or ask <= 0
                or ask < bid
                for bid, ask in numeric_rows
            )
        ):
            return None
        observed_at = timestamps[0]
        if observed_at > now or now - observed_at > timedelta(seconds=5):
            return None
        return MarketSample(
            observed_at,
            {
                item.symbol: (item.bid + item.ask) / Decimal("2")
                for item in bindings
            },
        )

    def reevaluate_option(
        self,
        ledger: EventReactionLedger,
        market: MarketReactionEvidence,
        *,
        now: datetime,
    ) -> OptionReevaluationEvidence | None:
        """Bind current option research to one already-verified market record."""

        checked_at = utc_datetime(now, field="now")
        market.assert_integrity()
        release = ledger.release_chain[-1]
        if (
            market.event_hash != ledger.identity.event_hash
            or market.release_hash != release.content_hash
        ):
            self.last_reason = "MARKET_REACTION_BINDING_INVALID"
            return None
        preselections_reader = getattr(self._news_adapter, "preselections", None)
        if not callable(preselections_reader):
            self.last_reason = "REACTION_QUOTE_ADAPTER_UNAVAILABLE"
            return None
        try:
            candidates = tuple(preselections_reader())
        except Exception:
            candidates = ()
        requested_symbols = ledger.identity.symbols or _MACRO_SYMBOLS
        candidate = next(
            (item for item in candidates if item.underlying in requested_symbols),
            None,
        )
        if candidate is None:
            self.last_reason = "OPTION_REPRICE_UNAVAILABLE"
            return None
        result = candidate.as_dict()
        required = ("maximum_loss_usd", "estimated_cost_usd", "cost_after_ev_usd")
        if any(result.get(name) is None for name in required):
            self.last_reason = "OPTION_REPRICE_ECONOMICS_INCOMPLETE"
            return None
        gate_bundle = self._canonical_gate_bundle(candidate)
        if gate_bundle is None:
            return None
        gate_results, gate_bundle_hash = gate_bundle
        required_hash_fields = (
            "broker_snapshot_hash",
            "account_snapshot_hash",
            "risk_policy_hash",
            "strategy_nav_post_hash",
            "economics_calculation_hash",
        )
        if any(not _valid_hash(result.get(name)) for name in required_hash_fields):
            self.last_reason = "OPTION_GATE_EVIDENCE_HASHES_INCOMPLETE"
            return None
        quote_asof = candidate.economics_quote_asof
        if (
            not isinstance(quote_asof, datetime)
            or quote_asof.tzinfo is None
            or quote_asof.utcoffset() is None
            or quote_asof > checked_at
            or checked_at - quote_asof > timedelta(seconds=5)
        ):
            self.last_reason = "OPTION_GATE_QUOTE_FRESHNESS_FAILED"
            return None
        candidate_hash = canonical_hash(result)
        supporting_bundle = OptionRepriceBundle(
            ledger.identity.event_hash,
            release.content_hash,
            market.content_hash,
            candidate_hash,
            gate_bundle_hash,
            gate_results,
            (
                ledger.identity.event_hash,
                release.content_hash,
                market.content_hash,
                gate_bundle_hash,
                *(str(result[name]) for name in required_hash_fields),
            ),
            checked_at,
        )
        bundle_projection = supporting_bundle.as_dict()
        if bundle_projection["status"] != "AVAILABLE":
            self.last_reason = ":".join(str(item) for item in bundle_projection["blockers"])
            return None
        result = {
            **{
                key: value
                for key, value in result.items()
                if key
                not in {
                    "approval_eligible",
                    "instruction_creation_allowed",
                    "order_creation_allowed",
                }
            },
            "reaction_gate_bundle": {
                key: value
                for key, value in bundle_projection.items()
                if key
                not in {
                    "approval_eligible",
                    "instruction_creation_allowed",
                    "order_creation_allowed",
                }
            },
        }
        option = OptionReevaluationEvidence(
            event_hash=ledger.identity.event_hash,
            release_hash=release.content_hash,
            market_reaction_hash=market.content_hash,
            option_id=candidate.preselection_id,
            candidate_hash=candidate_hash,
            source="IBKR_READ_ONLY",
            evidence_asof=market.evidence_asof,
            observed_at=checked_at,
            input_evidence_hashes=(
                ledger.expectation.content_hash,
                release.content_hash,
                market.content_hash,
                *candidate.evidence_hashes,
            ),
            result=result,
        )
        self.last_reason = "READY"
        return option

    def _canonical_gate_bundle(
        self,
        candidate: ConditionalOptionPreselection,
    ) -> tuple[Mapping[str, bool], str] | None:
        ranking_store = getattr(self._news_adapter, "ranking_store", None)
        read_snapshot = getattr(ranking_store, "read_snapshot", None)
        snapshot_id = getattr(candidate, "ranking_snapshot_id", None)
        candidate_hash = getattr(candidate, "ranking_candidate_hash", None)
        if (
            not callable(read_snapshot)
            or not isinstance(snapshot_id, str)
            or not snapshot_id
            or not _valid_hash(candidate_hash)
        ):
            self.last_reason = "OPTION_GATE_BUNDLE_UNAVAILABLE"
            return None
        try:
            payload = read_snapshot(snapshot_id)
        except Exception:
            self.last_reason = "OPTION_GATE_BUNDLE_UNAVAILABLE"
            return None
        if payload.get("ranking_snapshot_id") != snapshot_id:
            self.last_reason = "OPTION_GATE_CANDIDATE_BINDING_UNAVAILABLE"
            return None
        ranking_candidates = payload.get("candidates")
        if not isinstance(ranking_candidates, Sequence) or isinstance(
            ranking_candidates,
            (str, bytes, bytearray, memoryview),
        ):
            self.last_reason = "OPTION_GATE_CANDIDATE_BINDING_UNAVAILABLE"
            return None
        ranking_rows = [
            item
            for item in ranking_candidates
            if isinstance(item, Mapping)
            and item.get("candidate_id") == candidate.preselection_id
        ]
        if len(ranking_rows) != 1:
            self.last_reason = "OPTION_GATE_CANDIDATE_BINDING_UNAVAILABLE"
            return None
        ranking_row = ranking_rows[0]
        ranking_body = ranking_row.get("candidate_body")
        if (
            ranking_row.get("candidate_hash") != candidate_hash
            or not isinstance(ranking_body, Mapping)
            or canonical_hash(ranking_body) != candidate_hash
        ):
            self.last_reason = "OPTION_GATE_CANDIDATE_BINDING_UNAVAILABLE"
            return None
        expected_hash = payload.get("gate_bundle_hash")
        records = payload.get("decision_records")
        if not _valid_hash(expected_hash) or not isinstance(records, Sequence):
            self.last_reason = "OPTION_GATE_BUNDLE_UNAVAILABLE"
            return None
        bundles = []
        for record in records:
            if not isinstance(record, Mapping):
                continue
            value = record.get("record")
            bundle = value.get("gate_bundle") if isinstance(value, Mapping) else None
            if isinstance(bundle, Mapping) and bundle.get("gate_bundle_hash") == expected_hash:
                bundles.append(bundle)
        if len(bundles) != 1:
            self.last_reason = "OPTION_GATE_BUNDLE_UNAVAILABLE"
            return None
        bundle = bundles[0]
        hash_payload = {
            key: value
            for key, value in bundle.items()
            if key not in {"gate_bundle_hash", "hard_failure_candidate_keys"}
        }
        if canonical_hash(hash_payload) != expected_hash:
            self.last_reason = "OPTION_GATE_BUNDLE_HASH_INVALID"
            return None
        candidates = bundle.get("candidates")
        if not isinstance(candidates, Mapping):
            self.last_reason = "OPTION_GATE_BUNDLE_INVALID"
            return None
        candidate_rows = [
            item
            for item in candidates.values()
            if isinstance(item, Mapping)
            and item.get("candidate_id") == candidate.preselection_id
            and item.get("candidate_hash") == candidate_hash
        ]
        if len(candidate_rows) != 1:
            self.last_reason = "OPTION_GATE_CANDIDATE_BINDING_UNAVAILABLE"
            return None
        layers = candidate_rows[0].get("layers")
        if not isinstance(layers, Sequence):
            self.last_reason = "OPTION_GATE_BUNDLE_INVALID"
            return None
        results = {
            str(layer.get("gate_id")): layer.get("status") == "PASS"
            for layer in layers
            if isinstance(layer, Mapping)
        }
        if set(results) != set(REQUIRED_OPTION_GATES):
            self.last_reason = "OPTION_GATE_BUNDLE_INCOMPLETE"
            return None
        return MappingProxyType(results), str(expected_hash)


@dataclass(frozen=True, slots=True)
class _ReactionProviderSnapshot:
    generation: int
    identities: Mapping[str, ScheduledEventIdentity]
    specs: Mapping[str, ScheduledReactionSpec]
    descriptors: Mapping[str, CalendarReactionDescriptor]
    public_to_stable: Mapping[str, str]
    stable_to_public: Mapping[str, str]
    descriptor_reasons: Mapping[str, str]
    supported_event_ids: tuple[str, ...]
    eligible_event_ids: tuple[str, ...]
    unsupported_count: int
    family_counts: Mapping[str, int]
    measure_count: int
    scope_known: bool
    schedule_refresh_status: str
    schedule_refresh_reason: str | None
    schedule_hash: str | None
    root_supersessions: Mapping[str, str]
    capture_reasons: Mapping[str, str]
    supervisor_health: Mapping[str, Mapping[str, object]]
    last_attempt: datetime | None
    last_reason: str


class ProductionMacroReactionProvider:
    """Refresh and replay strict reaction ledgers for official calendar rows."""

    def __init__(
        self,
        store: ReactionEvidenceStore,
        *,
        jin10_client: object | None,
        jin10_secret_store: object | None,
        official_actual_provider: object,
        official_document_provider: object | None = None,
        reaction_observer: ProductionReactionObserver | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._jin10_client = jin10_client
        self._jin10_secret_store = jin10_secret_store
        self._actual_provider = official_actual_provider
        self._official_document_provider = official_document_provider
        self._observer = reaction_observer
        self._clock = clock or (lambda: datetime.now(_UTC))
        self._state_lock = threading.RLock()
        self._activity_drained = threading.Condition(self._state_lock)
        self._schedule_write_lock = threading.RLock()
        self._schedule_generation = 0
        self._active_schedule_activities: dict[int, int] = {}
        self._identities: dict[str, ScheduledEventIdentity] = {}
        self._reaction_specs: dict[str, ScheduledReactionSpec] = {}
        self._descriptors: dict[str, CalendarReactionDescriptor] = {}
        self._public_to_stable: dict[str, str] = {}
        self._stable_to_public: dict[str, str] = {}
        self._descriptor_reasons: dict[str, str] = {}
        self._capture_attempted_at: dict[str, datetime] = {}
        self._capture_reasons: dict[str, str] = {}
        self._supported_event_ids: tuple[str, ...] = ()
        self._eligible_event_ids: tuple[str, ...] = ()
        self._unsupported_count = 0
        self._family_counts: dict[str, int] = {}
        self._measure_count = 0
        self._scope_known = False
        self._last_attempt: datetime | None = None
        self._schedule_refresh_status = "UNKNOWN"
        self._schedule_refresh_reason: str | None = "NOT_REFRESHED"
        self._schedule_hash: str | None = None
        self._supervisor_health: dict[str, dict[str, object]] = {}
        self._root_supersessions: dict[str, str] = {}
        self.last_reason = "NOT_REFRESHED"
        self._restore_schedule_cache()
        try:
            for failure in self._store.worker_failures():
                lane = str(failure.get("lane") or "").lower()
                if lane in {"schedule", "capture", "observer"}:
                    failure_attempted_at = _parse_time(failure["attempted_at"])
                    self._supervisor_health[lane] = {
                        "status": "DEGRADED",
                        "reason": str(failure.get("reason") or "WORKER_FAILURE"),
                        "attempted_at": failure.get("attempted_at"),
                        "durable": True,
                    }
                    if (
                        self._last_attempt is None
                        or failure_attempted_at > self._last_attempt
                    ):
                        self._last_attempt = failure_attempted_at
        except Exception:
            pass
        self._publish_state_snapshot()

    def _publish_state_snapshot(self, *, schedule_change: bool = False) -> None:
        if schedule_change:
            self._schedule_generation += 1
        self._state_snapshot = _ReactionProviderSnapshot(
            self._schedule_generation,
            MappingProxyType(dict(self._identities)),
            MappingProxyType(dict(self._reaction_specs)),
            MappingProxyType(dict(self._descriptors)),
            MappingProxyType(dict(self._public_to_stable)),
            MappingProxyType(dict(self._stable_to_public)),
            MappingProxyType(dict(self._descriptor_reasons)),
            self._supported_event_ids,
            self._eligible_event_ids,
            self._unsupported_count,
            MappingProxyType(dict(self._family_counts)),
            self._measure_count,
            self._scope_known,
            self._schedule_refresh_status,
            self._schedule_refresh_reason,
            self._schedule_hash,
            MappingProxyType(dict(self._root_supersessions)),
            MappingProxyType(dict(self._capture_reasons)),
            MappingProxyType(
                {
                    key: MappingProxyType(dict(value))
                    for key, value in self._supervisor_health.items()
                }
            ),
            self._last_attempt,
            self.last_reason,
        )

    def _state(self) -> _ReactionProviderSnapshot:
        with self._state_lock:
            return self._state_snapshot

    def _update_display_state(
        self,
        *,
        reason: str,
        attempted_at: datetime | None = None,
        capture_reason: tuple[str, str] | None = None,
        expected_generation: int | None = None,
    ) -> bool:
        """Publish API-visible metadata as one immutable projection generation."""

        with self._state_lock:
            if (
                expected_generation is not None
                and self._state_snapshot.generation != expected_generation
            ):
                return False
            if attempted_at is not None:
                self._last_attempt = attempted_at
            if capture_reason is not None:
                self._capture_reasons[capture_reason[0]] = capture_reason[1]
            self.last_reason = reason
            self._publish_state_snapshot()
            return True

    def _clear_worker_failure(
        self,
        lane: str,
        *,
        expected_generation: int | None = None,
    ) -> bool:
        """Clear only the current health projection after one successful cycle.

        Durable failure rows remain append-only diagnostic history.  They must
        not, however, keep a recovered worker permanently degraded in the live
        read model.  A generation check prevents an old observer/capture cycle
        from clearing a failure published for a newer schedule.
        """

        checked_lane = str(lane).strip().lower()
        if checked_lane not in {"schedule", "capture", "observer"}:
            raise ValueError("reaction worker lane is invalid")
        with self._state_lock:
            if (
                expected_generation is not None
                and self._state_snapshot.generation != expected_generation
            ):
                return False
            if checked_lane not in self._supervisor_health:
                return False
            self._supervisor_health.pop(checked_lane, None)
            self._publish_state_snapshot()
            return True

    def _begin_schedule_activity(self) -> _ReactionProviderSnapshot:
        """Bind one observer/capture operation to the active schedule generation."""

        with self._activity_drained:
            state = self._state_snapshot
            self._active_schedule_activities[state.generation] = (
                self._active_schedule_activities.get(state.generation, 0) + 1
            )
            return state

    def _finish_schedule_activity(self, generation: int) -> None:
        with self._activity_drained:
            remaining = self._active_schedule_activities.get(generation, 0) - 1
            if remaining > 0:
                self._active_schedule_activities[generation] = remaining
            else:
                self._active_schedule_activities.pop(generation, None)
                self._activity_drained.notify_all()

    def _wait_for_schedule_activity(self, generation: int) -> None:
        """Wait without holding the state lock over observer/provider I/O."""

        with self._activity_drained:
            while self._active_schedule_activities.get(generation, 0):
                self._activity_drained.wait()

    def _restore_schedule_cache(self) -> None:
        """Reconstruct the last verified typed schedule before any provider call."""

        rows = self._store.load_schedule_cache()
        for row in rows:
            document = row.get("document")
            if not isinstance(document, Mapping):
                continue
            try:
                stable_key = str(document["stable_event_key"])
                scheduled_text = document.get("scheduled_at")
                scheduled_at = (
                    None if scheduled_text is None else _parse_time(scheduled_text)
                )
                descriptor = CalendarReactionDescriptor(
                    event_id=str(document["public_event_id"]),
                    publisher=str(document["publisher"]),
                    family=EventFamily(str(document["family"])),
                    source_id=str(document["source_id"]),
                    source_url=str(document["source_url"]),
                    title=str(document["title"]),
                    scheduled_at=scheduled_at,
                    schedule_precision=str(document["schedule_precision"]),
                    reference_period=(
                        None
                        if document.get("reference_period") is None
                        else str(document["reference_period"])
                    ),
                    estimate_label=(
                        None
                        if document.get("estimate_label") is None
                        else str(document["estimate_label"])
                    ),
                    meeting_range=(
                        None
                        if document.get("meeting_range") is None
                        else str(document["meeting_range"])
                    ),
                    calendar_event_hash=str(document["calendar_event_hash"]),
                    calendar_record_hash=str(document["calendar_record_hash"]),
                    calendar_feed_hash=str(document["calendar_feed_hash"]),
                    stable_event_key=stable_key,
                    identity_hash=str(document["descriptor_identity_hash"]),
                    version_provenance_hash=str(document["descriptor_provenance_hash"]),
                    wait_reason=(
                        None
                        if document.get("wait_reason") is None
                        else str(document["wait_reason"])
                    ),
                )
                identity_document = document.get("identity")
                identity = None
                if isinstance(identity_document, Mapping):
                    identity = ScheduledEventIdentity(
                        event_id=stable_key,
                        official_source=str(identity_document["official_source"]),
                        official_source_id=str(identity_document["official_source_id"]),
                        title=str(identity_document["title"]),
                        category=str(identity_document["category"]),
                        scheduled_at=_parse_time(identity_document["scheduled_at"]),
                        schedule_published_at=(
                            None
                            if identity_document.get("schedule_published_at") is None
                            else _parse_time(identity_document["schedule_published_at"])
                        ),
                        schedule_first_seen_at=_parse_time(
                            identity_document["schedule_first_seen_at"]
                        ),
                        schedule_observed_at=_parse_time(
                            identity_document["schedule_observed_at"]
                        ),
                        symbols=tuple(identity_document.get("symbols", ())),
                        reaction_root_hash=stable_key,
                    )
                parent_document = document.get("parent")
                spec = None
                if identity is not None and isinstance(parent_document, Mapping):
                    parent = ParentEventIdentity(
                        str(parent_document["publisher"]),
                        EventFamily(str(parent_document["family"])),
                        str(parent_document["reference_period"]),
                        datetime.fromisoformat(
                            str(parent_document["scheduled_date"])
                        ).date(),
                        (
                            None
                            if parent_document.get("estimate_label") is None
                            else str(parent_document["estimate_label"])
                        ),
                    )
                    spec = ScheduledReactionSpec(
                        parent,
                        identity.scheduled_at,
                        identity.event_hash,
                        _parse_time(document["capture_deadline"]),
                        (
                            None
                            if document.get("next_eligible_release_at") is None
                            else _parse_time(document["next_eligible_release_at"])
                        ),
                    )
            except Exception:
                continue
            public_id = descriptor.event_id
            self._descriptors[stable_key] = descriptor
            self._public_to_stable[public_id] = stable_key
            self._stable_to_public[stable_key] = public_id
            if identity is not None:
                self._identities[stable_key] = identity
            if spec is not None:
                self._reaction_specs[stable_key] = spec
            if descriptor.wait_reason is not None:
                self._descriptor_reasons[public_id] = descriptor.wait_reason
            superseded = document.get("supersedes_reaction_root_hash")
            if _valid_hash(superseded):
                self._root_supersessions[stable_key] = str(superseded)
        inactive_roots = set(self._root_supersessions.values())
        for root in inactive_roots:
            self._identities.pop(root, None)
            self._reaction_specs.pop(root, None)
            self._descriptors.pop(root, None)
            public_id = self._stable_to_public.pop(root, None)
            if public_id is not None and self._public_to_stable.get(public_id) == root:
                self._public_to_stable.pop(public_id, None)
        authority = self._store.load_schedule_generation_authority()
        if authority is not None:
            raw_roots = authority.get("active_roots")
            if not isinstance(raw_roots, Sequence) or isinstance(
                raw_roots,
                (str, bytes, bytearray, memoryview),
            ):
                raise MacroReactionError("REACTION_SCHEDULE_AUTHORITY_INVALID")
            authority_roots: dict[
                str,
                tuple[str, str, bool, ScheduledEventIdentity, ScheduledReactionSpec | None],
            ] = {}
            for raw_root in raw_roots:
                if not isinstance(raw_root, Mapping):
                    raise MacroReactionError("REACTION_SCHEDULE_AUTHORITY_INVALID")
                stable_key = str(raw_root.get("stable_event_key") or "")
                public_id = str(raw_root.get("public_event_id") or "")
                official_event_hash = str(raw_root.get("official_event_hash") or "")
                cache_backed = raw_root.get("cache_backed")
                identity_document = raw_root.get("identity")
                spec_document = raw_root.get("spec")
                if (
                    not stable_key
                    or not public_id
                    or not _valid_hash(official_event_hash)
                    or not isinstance(cache_backed, bool)
                    or not isinstance(identity_document, Mapping)
                    or stable_key in authority_roots
                ):
                    raise MacroReactionError("REACTION_SCHEDULE_AUTHORITY_INVALID")
                try:
                    marker_identity = ScheduledEventIdentity(
                        event_id=stable_key,
                        official_source=str(identity_document["official_source"]),
                        official_source_id=str(identity_document["official_source_id"]),
                        title=str(identity_document["title"]),
                        category=str(identity_document["category"]),
                        scheduled_at=_parse_time(identity_document["scheduled_at"]),
                        schedule_published_at=(
                            None
                            if identity_document.get("schedule_published_at") is None
                            else _parse_time(identity_document["schedule_published_at"])
                        ),
                        schedule_first_seen_at=_parse_time(
                            identity_document["schedule_first_seen_at"]
                        ),
                        schedule_observed_at=_parse_time(
                            identity_document["schedule_observed_at"]
                        ),
                        symbols=tuple(identity_document.get("symbols", ())),
                        reaction_root_hash=(
                            stable_key if cache_backed else None
                        ),
                    )
                    marker_spec = None
                    if spec_document is not None:
                        if not isinstance(spec_document, Mapping):
                            raise ValueError("invalid spec document")
                        parent_document = spec_document["parent"]
                        if not isinstance(parent_document, Mapping):
                            raise ValueError("invalid parent document")
                        parent = ParentEventIdentity(
                            str(parent_document["publisher"]),
                            EventFamily(str(parent_document["family"])),
                            str(parent_document["reference_period"]),
                            datetime.fromisoformat(
                                str(parent_document["scheduled_date"])
                            ).date(),
                            (
                                None
                                if parent_document.get("estimate_label") is None
                                else str(parent_document["estimate_label"])
                            ),
                        )
                        marker_spec = ScheduledReactionSpec(
                            parent=parent,
                            scheduled_at=_parse_time(spec_document["scheduled_at"]),
                            official_event_hash=str(
                                spec_document["official_event_hash"]
                            ),
                            capture_deadline=_parse_time(
                                spec_document["capture_deadline"]
                            ),
                            next_eligible_release_at=(
                                None
                                if spec_document.get("next_eligible_release_at") is None
                                else _parse_time(
                                    spec_document["next_eligible_release_at"]
                                )
                            ),
                        )
                except Exception as exc:
                    raise MacroReactionError(
                        "REACTION_SCHEDULE_AUTHORITY_INVALID"
                    ) from exc
                if marker_identity.event_hash != official_event_hash or (
                    marker_spec is not None
                    and marker_spec.official_event_hash != official_event_hash
                ):
                    raise MacroReactionError(
                        "REACTION_SCHEDULE_AUTHORITY_ROOT_MISMATCH"
                    )
                authority_roots[stable_key] = (
                    public_id,
                    official_event_hash,
                    cache_backed,
                    marker_identity,
                    marker_spec,
                )
            for stable_key, (
                public_id,
                official_event_hash,
                cache_backed,
                marker_identity,
                marker_spec,
            ) in authority_roots.items():
                identity = self._identities.get(stable_key)
                if identity is None and not cache_backed:
                    identity = marker_identity
                    self._identities[stable_key] = identity
                    self._stable_to_public[stable_key] = public_id
                    self._public_to_stable[public_id] = stable_key
                    if marker_spec is not None:
                        self._reaction_specs[stable_key] = marker_spec
                if (
                    identity is None
                    or identity.event_hash != official_event_hash
                    or self._stable_to_public.get(stable_key) != public_id
                ):
                    raise MacroReactionError("REACTION_SCHEDULE_AUTHORITY_ROOT_MISMATCH")
            retained_roots = set(authority_roots)
            self._identities = {
                key: value
                for key, value in self._identities.items()
                if key in retained_roots
            }
            self._reaction_specs = {
                key: value
                for key, value in self._reaction_specs.items()
                if key in retained_roots
            }
            self._descriptors = {
                key: value
                for key, value in self._descriptors.items()
                if key in retained_roots
            }
            self._stable_to_public = {
                key: value
                for key, value in self._stable_to_public.items()
                if key in retained_roots
            }
            self._public_to_stable = {
                public_id: stable_key
                for stable_key, public_id in self._stable_to_public.items()
            }
            self._descriptor_reasons = {
                public_id: reason
                for public_id, reason in self._descriptor_reasons.items()
                if public_id in self._public_to_stable
            }
            status = str(authority.get("schedule_status") or "UNAVAILABLE")
            self._schedule_refresh_status = (
                status
                if status in {"READY", "DEGRADED", "UNAVAILABLE"}
                else "UNAVAILABLE"
            )
            self._schedule_refresh_reason = (
                None
                if authority.get("schedule_reason") is None
                else str(authority["schedule_reason"])
            )
            cached_hash = authority.get("schedule_hash")
            self._schedule_hash = (
                str(cached_hash) if _valid_hash(cached_hash) else None
            )
            authority_attempted_at = authority.get("attempted_at")
            self._last_attempt = (
                None
                if authority_attempted_at is None
                else _parse_time(authority_attempted_at)
            )
            raw_unsupported_count = authority.get("unsupported_count")
            if (
                isinstance(raw_unsupported_count, bool)
                or not isinstance(raw_unsupported_count, int)
                or raw_unsupported_count < 0
            ):
                raise MacroReactionError("REACTION_SCHEDULE_AUTHORITY_INVALID")
            self._unsupported_count = raw_unsupported_count
            self._scope_known = True
            self.last_reason = (
                "DURABLE_REACTION_SCHEDULE_RESTORED"
                if authority_roots
                else "NO_ELIGIBLE_REACTION_EVENTS"
            )
        if self._identities:
            self._supported_event_ids = tuple(
                dict.fromkeys(
                    self._stable_to_public[key]
                    for key in self._identities
                    if key in self._stable_to_public
                )
            )
            self._eligible_event_ids = tuple(
                self._stable_to_public[key]
                for key, spec in self._reaction_specs.items()
                if FAMILY_SPECS[spec.parent.family].surprise_supported
            )
            self._scope_known = True
            active_families = tuple(
                classify_event_family(
                    identity.official_source,
                    identity.title,
                    identity.category,
                )
                for identity in self._identities.values()
            )
            self._family_counts = {
                family.value: active_families.count(family)
                for family in dict.fromkeys(active_families)
            }
            self._measure_count = sum(
                len(FAMILY_SPECS[family].measures)
                for family in active_families
                if FAMILY_SPECS[family].support_state is SupportState.SUPPORTED
            )

    def update_schedule_refresh(
        self,
        *,
        status: str,
        reason: str | None,
        schedule_hash: str | None,
        publish: bool = True,
    ) -> None:
        with self._state_lock:
            self._schedule_refresh_status = status if status in {"READY", "DEGRADED", "UNAVAILABLE"} else "UNAVAILABLE"
            self._schedule_refresh_reason = reason
            self._schedule_hash = schedule_hash if _valid_hash(schedule_hash) else None
            if publish:
                self._publish_state_snapshot()

    def record_worker_failure(
        self,
        lane: str,
        *,
        now: datetime,
        reason: str,
        expected_generation: int | None = None,
    ) -> bool:
        checked_at = utc_datetime(now, field="now")
        checked_lane = str(lane).strip().upper()
        with self._state_lock:
            if (
                expected_generation is not None
                and self._state_snapshot.generation != expected_generation
            ):
                return False
        durable = True
        try:
            self._store.record_worker_failure(
                checked_lane,
                attempted_at=checked_at,
                reason=reason,
            )
        except Exception:
            durable = False
        with self._state_lock:
            if (
                expected_generation is not None
                and self._state_snapshot.generation != expected_generation
            ):
                return False
            self._supervisor_health[checked_lane.lower()] = {
                "status": "DEGRADED",
                "reason": reason,
                "attempted_at": checked_at.isoformat(),
                "durable": durable,
            }
            self._last_attempt = checked_at
            self.last_reason = reason
            self._publish_state_snapshot()
            return True

    def restore_official_identities(self, events: Iterable[object]) -> None:
        """Restore immutable schedule identities without contacting providers."""

        with self._schedule_write_lock:
            self._restore_official_identities_locked(events)

    def _restore_official_identities_locked(
        self,
        events: Iterable[object],
        *,
        schedule_health: tuple[str, str | None, str | None] | None = None,
        attempted_at: datetime | None = None,
    ) -> None:
        """Install one generation while the public/schedule writer lock is held."""

        checked_events = tuple(events)[:500]
        prior = self._state()
        if schedule_health is None:
            candidate_schedule_status = prior.schedule_refresh_status
            candidate_schedule_reason = prior.schedule_refresh_reason
            candidate_schedule_hash = prior.schedule_hash
        else:
            status, candidate_schedule_reason, raw_schedule_hash = schedule_health
            candidate_schedule_status = (
                status
                if status in {"READY", "DEGRADED", "UNAVAILABLE"}
                else "UNAVAILABLE"
            )
            candidate_schedule_hash = (
                raw_schedule_hash if _valid_hash(raw_schedule_hash) else None
            )
        public_to_stable = dict(prior.public_to_stable)
        stable_to_public = dict(prior.stable_to_public)
        supported_events = tuple(
            event for event in checked_events if _is_supported_official_event(event)
        )
        families = tuple(
            classify_event_family(
                getattr(event, "source", ""),
                getattr(event, "title", ""),
                getattr(event, "category", ""),
            )
            for event in checked_events
        )
        family_counts = {
            family.value: families.count(family) for family in dict.fromkeys(families)
        }
        measure_count = sum(
            len(FAMILY_SPECS[family].measures)
            for family in families
            if FAMILY_SPECS[family].support_state is SupportState.SUPPORTED
        )
        supported_event_ids = tuple(
            dict.fromkeys(
                str(getattr(event, "event_id", "")).strip()
                for event in supported_events
                if str(getattr(event, "event_id", "")).strip()
            )
        )
        unsupported_count = len(checked_events) - len(supported_events)
        meeting_ranges = tuple(
            value
            for event in checked_events
            if "FEDERAL RESERVE" in str(getattr(event, "source", "")).upper()
            and (value := fomc_meeting_range_from_title(getattr(event, "title", "")))
            is not None
        )
        descriptors: dict[str, CalendarReactionDescriptor] = {}
        descriptor_observed_at: dict[str, datetime] = {}
        supersessions: dict[str, str] = {}
        descriptor_reasons: dict[str, str] = dict(prior.descriptor_reasons)
        stale_roots: set[str] = set()
        accepted_supported_events: list[object] = []
        for event in supported_events:
            public_id = str(getattr(event, "event_id", "")).strip()
            if not public_id or not hasattr(event, "schedule_precision") or not hasattr(event, "provenance"):
                accepted_supported_events.append(event)
                continue
            try:
                descriptor = reaction_descriptor_from_calendar(
                    event,
                    meeting_ranges=meeting_ranges,
                )
            except (TypeError, ValueError):
                descriptor_reasons[public_id] = "WAIT_REACTION_DESCRIPTOR_INVALID"
                continue
            observed_at = utc_datetime(
                getattr(event, "observed_at"),
                field="event.observed_at",
            )
            prior_identity = prior.identities.get(descriptor.stable_event_key)
            if (
                prior_identity is not None
                and observed_at <= prior_identity.schedule_observed_at
            ):
                stale_roots.add(descriptor.stable_event_key)
                continue
            accepted_supported_events.append(event)
            descriptors[descriptor.stable_event_key] = descriptor
            descriptor_observed_at[descriptor.stable_event_key] = observed_at
            old_public_id = stable_to_public.get(descriptor.stable_event_key)
            if (
                old_public_id is not None
                and old_public_id != public_id
                and public_to_stable.get(old_public_id)
                == descriptor.stable_event_key
            ):
                public_to_stable.pop(old_public_id, None)
            public_to_stable[public_id] = descriptor.stable_event_key
            stable_to_public[descriptor.stable_event_key] = public_id
            if descriptor.wait_reason is not None:
                descriptor_reasons[public_id] = descriptor.wait_reason
            else:
                descriptor_reasons.pop(public_id, None)
        if checked_events and not accepted_supported_events and len(stale_roots) == len(
            supported_events
        ) == len(checked_events):
            return
        all_descriptor_versions = dict(prior.descriptors)
        all_descriptor_versions.update(descriptors)
        version_groups: dict[
            tuple[str, EventFamily, str, str | None, str | None],
            list[tuple[str, CalendarReactionDescriptor, datetime]],
        ] = {}
        for root, descriptor in all_descriptor_versions.items():
            observed_at = descriptor_observed_at.get(root)
            if observed_at is None:
                prior_identity = prior.identities.get(root)
                if prior_identity is None:
                    continue
                observed_at = prior_identity.schedule_observed_at
            version_groups.setdefault(
                (
                    descriptor.publisher,
                    descriptor.family,
                    descriptor.source_id,
                    descriptor.reference_period,
                    descriptor.estimate_label,
                ),
                [],
            ).append((root, descriptor, observed_at))
        for versions in version_groups.values():
            ordered = sorted(versions, key=lambda item: (item[2], item[0]))
            for older, newer in zip(ordered, ordered[1:]):
                if older[0] != newer[0]:
                    supersessions[newer[0]] = older[0]
        restored: dict[str, ScheduledEventIdentity] = {}
        restored_events: dict[str, object] = {}
        for event in accepted_supported_events:
            public_id = str(getattr(event, "event_id", "")).strip()
            stable_key = public_to_stable.get(public_id, public_id)
            try:
                identity = ScheduledEventIdentity(
                    event_id=stable_key,
                    official_source=event.source,
                    official_source_id=event.source_id,
                    title=event.title,
                    category=event.category,
                    scheduled_at=event.scheduled_at,
                    schedule_published_at=event.published_at,
                    schedule_first_seen_at=event.first_seen_at,
                    schedule_observed_at=event.observed_at,
                    symbols=event.symbols,
                    reaction_root_hash=(
                        stable_key if stable_key in descriptors else None
                    ),
                )
            except Exception:
                continue
            restored[stable_key] = identity
            restored_events[stable_key] = event
            public_to_stable.setdefault(public_id, stable_key)
            stable_to_public.setdefault(stable_key, public_id)
        next_release_by_event: dict[str, datetime | None] = {}
        for event_id, identity in restored.items():
            family = classify_event_family(
                identity.official_source,
                identity.title,
                identity.category,
            )
            next_release_by_event[event_id] = min(
                (
                    candidate.scheduled_at
                    for candidate_id, candidate in restored.items()
                    if candidate_id != event_id
                    and candidate.scheduled_at > identity.scheduled_at
                    and classify_event_family(
                        candidate.official_source,
                        candidate.title,
                        candidate.category,
                    )
                    is family
                ),
                default=None,
            )
        specs: dict[str, ScheduledReactionSpec] = {}
        for event_id, identity in restored.items():
            if event_id in stale_roots:
                prior_spec = prior.specs.get(event_id)
                if prior_spec is not None:
                    specs[event_id] = prior_spec
                continue
            event = restored_events[event_id]
            try:
                descriptor = None
                if hasattr(event, "schedule_precision") and hasattr(event, "provenance"):
                    descriptor = descriptors.get(event_id)
                    parent = None if descriptor is None else descriptor.parent
                else:
                    parent = parent_identity_from_calendar(
                        source=identity.official_source,
                        title=identity.title,
                        category=identity.category,
                        scheduled_at=identity.scheduled_at,
                    )
                if parent is None and classify_event_family(
                    identity.official_source,
                    identity.title,
                    identity.category,
                ) is EventFamily.FOMC_MINUTES:
                    candidates = [
                        value
                        for value in meeting_ranges
                        if 14
                        <= (
                            identity.scheduled_at.date()
                            - datetime.strptime(value.split("/", 1)[1], "%Y-%m-%d").date()
                        ).days
                        <= 35
                    ]
                    if len(candidates) == 1:
                        parent = ParentEventIdentity(
                            "FED",
                            EventFamily.FOMC_MINUTES,
                            candidates[0],
                            identity.scheduled_at.date(),
                        )
                if parent is None:
                    continue
                specs[event_id] = ScheduledReactionSpec(
                    parent=parent,
                    scheduled_at=identity.scheduled_at,
                    official_event_hash=identity.event_hash,
                    capture_deadline=identity.scheduled_at + timedelta(minutes=15),
                    next_eligible_release_at=next_release_by_event[event_id],
                )
            except (TypeError, ValueError):
                continue
        identities = dict(prior.identities)
        identities.update(restored)
        all_specs = dict(prior.specs)
        for event_id in restored:
            all_specs.pop(event_id, None)
        all_specs.update(specs)
        all_descriptors = dict(prior.descriptors)
        all_descriptors.update(descriptors)
        inactive_roots = set(self._root_supersessions.values()) | set(
            supersessions.values()
        )
        for root in inactive_roots:
            identities.pop(root, None)
            all_specs.pop(root, None)
            all_descriptors.pop(root, None)
            public_id = stable_to_public.pop(root, None)
            if public_id is not None and public_to_stable.get(public_id) == root:
                public_to_stable.pop(public_id, None)
        active_restored = {
            event_id: identity
            for event_id, identity in identities.items()
            if event_id not in inactive_roots and event_id in stable_to_public
        }
        supported_event_ids = tuple(
            dict.fromkeys(stable_to_public[event_id] for event_id in active_restored)
        )
        eligible_event_ids = tuple(
            stable_to_public[event_id]
            for event_id, spec in all_specs.items()
            if event_id in active_restored
            and FAMILY_SPECS[spec.parent.family].surprise_supported
        )
        active_families = tuple(
            classify_event_family(
                identity.official_source,
                identity.title,
                identity.category,
            )
            for identity in active_restored.values()
        )
        family_counts = {
            family.value: active_families.count(family)
            for family in dict.fromkeys(active_families)
        }
        measure_count = sum(
            len(FAMILY_SPECS[family].measures)
            for family in active_families
            if FAMILY_SPECS[family].support_state is SupportState.SUPPORTED
        )
        generation_rows = []
        for stable_key, identity in restored.items():
            public_id = stable_to_public.get(stable_key)
            if (
                public_id is None
                or stable_key in inactive_roots
                or stable_key in stale_roots
            ):
                continue
            spec = specs.get(stable_key)
            descriptor = descriptors.get(stable_key)
            generation_rows.append(
                (
                    stable_key,
                    descriptor,
                    spec,
                    public_id,
                    identity,
                    identity.schedule_observed_at,
                    supersessions.get(stable_key),
                )
            )
        try:
            authority_observed_at = attempted_at or max(
                (
                    identity.schedule_observed_at
                    for identity in active_restored.values()
                ),
                default=utc_datetime(self._clock(), field="clock"),
            )
            self._store.save_schedule_generation(
                tuple(generation_rows),
                observed_at=authority_observed_at,
                schedule_status=candidate_schedule_status,
                schedule_reason=candidate_schedule_reason,
                schedule_hash=candidate_schedule_hash,
                unsupported_count=unsupported_count,
                active_roots=tuple(
                    {
                        "stable_event_key": stable_key,
                        "public_event_id": stable_to_public[stable_key],
                        "official_event_hash": identity.event_hash,
                        "cache_backed": stable_key in all_descriptors,
                        "identity": identity.as_dict(),
                        "spec": (
                            None
                            if all_specs.get(stable_key) is None
                            else {
                                "parent": {
                                    "publisher": all_specs[stable_key].parent.publisher,
                                    "family": all_specs[stable_key].parent.family.value,
                                    "reference_period": all_specs[stable_key].parent.reference_period,
                                    "scheduled_date": all_specs[stable_key].parent.scheduled_date.isoformat(),
                                    "estimate_label": all_specs[stable_key].parent.estimate_label,
                                },
                                "scheduled_at": all_specs[stable_key].scheduled_at.isoformat(),
                                "official_event_hash": all_specs[stable_key].official_event_hash,
                                "capture_deadline": all_specs[stable_key].capture_deadline.isoformat(),
                                "next_eligible_release_at": (
                                    None
                                    if all_specs[stable_key].next_eligible_release_at is None
                                    else all_specs[stable_key].next_eligible_release_at.isoformat()
                                ),
                            }
                        ),
                    }
                    for stable_key, identity in active_restored.items()
                ),
                attempted_at=(
                    attempted_at
                    if attempted_at is not None
                    else prior.last_attempt
                ),
            )
            with self._state_lock:
                self._identities = identities
                self._reaction_specs = all_specs
                self._descriptors = all_descriptors
                self._public_to_stable = public_to_stable
                self._stable_to_public = stable_to_public
                self._descriptor_reasons = descriptor_reasons
                self._eligible_event_ids = eligible_event_ids
                self._supported_event_ids = supported_event_ids
                self._unsupported_count = unsupported_count
                self._family_counts = family_counts
                self._measure_count = measure_count
                self._scope_known = True
                self._schedule_refresh_status = candidate_schedule_status
                self._schedule_refresh_reason = candidate_schedule_reason
                self._schedule_hash = candidate_schedule_hash
                self._root_supersessions.update(supersessions)
                if attempted_at is not None:
                    self._last_attempt = attempted_at
                self.last_reason = (
                    "DURABLE_OFFICIAL_IDENTITIES_RESTORED"
                    if all_specs
                    else "NO_ELIGIBLE_REACTION_EVENTS"
                )
                self._publish_state_snapshot(schedule_change=True)
        finally:
            self._wait_for_schedule_activity(prior.generation)

    def projection(self) -> dict[str, object]:
        """Expose the bounded current support scope without claiming progression."""

        now = utc_datetime(self._clock(), field="clock")
        state = self._state()
        worker_health = {
            key: dict(value) for key, value in state.supervisor_health.items()
        }
        verified = self._store.verified_event_batch(
            tuple(
                (event_id, spec.official_event_hash)
                for event_id, spec in state.specs.items()
            )
        )
        vintages_by_event = {
            event_id: verified.release_vintages(event_id, spec.official_event_hash)
            for event_id, spec in state.specs.items()
        }
        capture_count = sum(len(vintages) for vintages in vintages_by_event.values())
        next_releases = [
            value
            for spec in state.specs.values()
            for value in (spec.scheduled_at, spec.next_eligible_release_at)
            if value is not None and value > now
        ]
        next_releases.extend(
            descriptor.scheduled_at
            for descriptor in state.descriptors.values()
            if descriptor.scheduled_at is not None
            and descriptor.scheduled_at > now
            and FAMILY_SPECS[descriptor.family].support_state is SupportState.SUPPORTED
        )
        compatible_expectation_count = 0
        captured_measure_count = 0
        for event_id, spec in state.specs.items():
            vintages = vintages_by_event[event_id]
            if not vintages:
                continue
            latest = vintages[-1].get("document")
            measures = latest.get("measures", ()) if isinstance(latest, Mapping) else ()
            captured_measure_count += sum(isinstance(item, Mapping) for item in measures)
            rows = verified.records(event_id, spec.official_event_hash)
            compatible = False
            for row in rows:
                if row["kind"] != "JIN10_EXPECTATION" or _parse_time(row["observed_at"]) >= spec.scheduled_at:
                    continue
                try:
                    observation = _observation_from_document(row["document"])
                except Exception:
                    continue
                if any(
                    isinstance(measure, Mapping)
                    and measure.get("measure_id") == observation.metric
                    and measure.get("unit") == observation.unit
                    and measure.get("basis") == observation.basis
                    and isinstance(latest, Mapping)
                    and latest.get("reference_period") == observation.period
                    for measure in measures
                ):
                    compatible = True
                    break
            if compatible:
                compatible_expectation_count += 1
        return {
            "supported_event_ids": list(state.supported_event_ids),
            "supported_count": len(state.supported_event_ids),
            "eligible_event_ids": list(state.eligible_event_ids),
            "eligible_count": len(state.eligible_event_ids),
            "unsupported_count": state.unsupported_count,
            "event_count": sum(state.family_counts.values()),
            "measure_count": state.measure_count,
            "capture_spec_count": len(state.specs),
            "captured_release_vintage_count": capture_count,
            "captured_measure_count": captured_measure_count,
            "capture_eligible_count": sum(
                spec.scheduled_at <= now <= spec.capture_deadline
                for spec in state.specs.values()
            ),
            "surprise_ready_count": compatible_expectation_count,
            "progressed_event_count": sum(
                bool(vintages) for vintages in vintages_by_event.values()
            ),
            "next_eligible_release_at": (
                "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
                if not next_releases
                else min(next_releases).isoformat()
            ),
            "schedule_refresh_status": state.schedule_refresh_status,
            "schedule_refresh_reason": state.schedule_refresh_reason,
            "schedule_hash": state.schedule_hash,
            "descriptor_wait_reasons": dict(state.descriptor_reasons),
            "descriptor_wait_count": len(state.descriptor_reasons),
            "family_counts": dict(state.family_counts),
            "support_matrix": [
                {
                    "family": family.value,
                    "support_state": spec.support_state.value,
                    "measure_count": len(spec.measures),
                    "surprise_supported": spec.surprise_supported,
                    "reason": spec.reason,
                }
                for family, spec in FAMILY_SPECS.items()
            ],
            "worker_health": worker_health,
            "lifecycle_supersessions": dict(state.root_supersessions),
            "last_attempt": (
                None if state.last_attempt is None else state.last_attempt.isoformat()
            ),
            "scope_known": state.scope_known,
            "reason": state.last_reason,
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_creation_allowed": False,
        }

    def coverage(self, event_ids: tuple[str, ...]) -> Mapping[str, Mapping[str, object]]:
        """Expose spec-driven WAIT/progression state independently of legacy ledgers."""

        now = utc_datetime(self._clock(), field="clock")
        state = self._state()
        capture_reasons = dict(state.capture_reasons)
        descriptor_reasons = dict(state.descriptor_reasons)
        output: dict[str, Mapping[str, object]] = {}
        public_event_ids = _bounded_reaction_event_ids(event_ids)
        verified = self._store.verified_event_batch(
            tuple(
                (stable_key, identity.event_hash)
                for public_event_id in public_event_ids
                if (
                    stable_key := state.public_to_stable.get(
                        public_event_id,
                        public_event_id,
                    )
                )
                and (identity := state.identities.get(stable_key)) is not None
            )
        )
        for public_event_id in public_event_ids:
            stable_key = state.public_to_stable.get(public_event_id, public_event_id)
            identity = state.identities.get(stable_key)
            if identity is None:
                continue
            family = classify_event_family(
                identity.official_source,
                identity.title,
                identity.category,
            )
            scheduled_spec = state.specs.get(stable_key)
            vintages = verified.release_vintages(stable_key, identity.event_hash)
            evidence_rows = verified.records(stable_key, identity.event_hash)
            expectation_times = [
                _parse_time(row["observed_at"])
                for row in evidence_rows
                if row["kind"] == "JIN10_EXPECTATION"
                and _parse_time(row["observed_at"]) < identity.scheduled_at
            ]
            assessment = assess_support(
                family,
                scheduled_at=identity.scheduled_at,
                now=now,
                expectation_captured_at=(
                    None if not expectation_times else max(expectation_times)
                ),
                release_document_captured=bool(vintages),
            )
            latest_vintage = None if not vintages else vintages[-1]["document"]
            measure_reactions = []
            if isinstance(latest_vintage, Mapping):
                for measure in latest_vintage.get("measures", ()):
                    if not isinstance(measure, Mapping):
                        continue
                    measure_id = str(measure.get("measure_id") or "")
                    if not measure_id:
                        continue
                    measure_reactions.append(
                        {
                            "reaction_id": f"{stable_key}:{measure_id}",
                            "measure_id": measure_id,
                            "measure_hash": measure.get("measure_hash"),
                            "release_vintage_hash": vintages[-1]["content_hash"],
                            "stage": "ACTUAL_CAPTURED",
                        }
                    )
            document_stage = (
                DocumentStage.SCHEDULED
                if now < identity.scheduled_at
                else DocumentStage.DOCUMENT_CAPTURED
                if vintages
                else DocumentStage.WAITING_RELEASE_DOCUMENT
            )
            document_market = next(
                (
                    row["document"]
                    for row in reversed(evidence_rows)
                    if row["kind"] == "MARKET_REACTION"
                    and isinstance(row.get("document"), Mapping)
                ),
                None,
            )
            output[public_event_id] = MappingProxyType(
                {
                    "event_id": public_event_id,
                    "event_hash": identity.event_hash,
                    "scheduled_at": identity.scheduled_at.isoformat(),
                    "family": family.value,
                    "support_state": assessment.support_state.value,
                    "supported": assessment.supported,
                    "capture_eligible": assessment.capture_eligible,
                    "surprise_eligible": assessment.surprise_eligible,
                    "progressed": assessment.progressed,
                    "next_action": assessment.next_action,
                    "reason": assessment.reason,
                    "event_count": assessment.event_count,
                    "measure_count": assessment.measure_count,
                    "capture_spec_available": scheduled_spec is not None,
                    "capture_count": len(vintages),
                    "document_stage": document_stage.value,
                    "document_market_reaction": document_market,
                    "measure_reactions": tuple(measure_reactions),
                    "next_eligible_release_at": (
                        "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
                        if scheduled_spec is None
                        else identity.scheduled_at.isoformat()
                        if now < identity.scheduled_at
                        else scheduled_spec.next_release_text
                    ),
                    "capture_reason": capture_reasons.get(
                        stable_key,
                        descriptor_reasons.get(public_event_id),
                    ),
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_creation_allowed": False,
                }
            )
        return MappingProxyType(output)

    def reaction_roots(self, event_ids: tuple[str, ...]) -> Mapping[str, str]:
        """Return active public-to-semantic roots without exposing mutable state."""

        state = self._state()
        return MappingProxyType(
            {
                public_event_id: stable_key
                for public_event_id in tuple(event_ids)[:500]
                if (
                    stable_key := state.public_to_stable.get(public_event_id)
                ) is not None
                and stable_key in state.identities
            }
        )

    def active_reaction_symbols(self, *, now: datetime) -> tuple[str, ...]:
        """Return bounded symbols whose active reaction window needs sampling."""

        checked_at = utc_datetime(now, field="now")
        state = self._state()
        return tuple(
            dict.fromkeys(
                symbol
                for stable_key, spec in state.specs.items()
                if spec.scheduled_at - _BASELINE_ARM_LEAD
                <= checked_at
                <= spec.scheduled_at + WINDOW_DURATION + _ENDPOINT_GRACE
                for identity in (state.identities.get(stable_key),)
                if identity is not None
                for symbol in identity.symbols
            )
        )[:64]

    def pending_reaction_sample_symbols(self, *, now: datetime) -> tuple[str, ...]:
        """Return symbols only while one strict quote sample is still missing."""

        checked_at = utc_datetime(now, field="now")
        state = self._state()
        pending: list[str] = []
        for stable_key, spec in state.specs.items():
            identity = state.identities.get(stable_key)
            if identity is None:
                continue
            needs_sample = False
            if (
                spec.scheduled_at - _BASELINE_ACQUISITION_LEAD
                <= checked_at
                <= spec.scheduled_at
            ):
                needs_sample = self._store.load_baseline(
                    identity.event_hash,
                    scheduled_at=identity.scheduled_at,
                ) is None
            endpoint_at = spec.scheduled_at + WINDOW_DURATION
            if endpoint_at <= checked_at <= endpoint_at + _ENDPOINT_GRACE:
                needs_sample = self._store.load_endpoint(
                    identity.event_hash,
                    scheduled_at=identity.scheduled_at,
                ) is None
            if needs_sample:
                pending.extend(_MACRO_SYMBOLS)
        return tuple(dict.fromkeys(pending))[:64]

    def refresh_schedule(self, official_snapshot: object, *, now: datetime) -> None:
        """Install one independently acquired schedule; perform no capture I/O."""

        checked_at = utc_datetime(now, field="now")
        events = tuple(getattr(official_snapshot, "events", ()))[:500]
        with self._schedule_write_lock:
            self._restore_official_identities_locked(
                events,
                attempted_at=checked_at,
            )
            self._clear_worker_failure("SCHEDULE")

    def refresh_schedule_with_health(
        self,
        official_snapshot: object,
        *,
        now: datetime,
        status: str,
        reason: str | None,
        schedule_hash: str | None,
    ) -> None:
        """Atomically install schedule health and its matching identity generation."""

        checked_at = utc_datetime(now, field="now")
        events = tuple(getattr(official_snapshot, "events", ()))[:500]
        with self._schedule_write_lock:
            self._restore_official_identities_locked(
                events,
                schedule_health=(status, reason, schedule_hash),
                attempted_at=checked_at,
            )
            self._clear_worker_failure("SCHEDULE")

    def observe_local(self, *, now: datetime) -> None:
        """Capture baseline/endpoint from local quotes before publication locks."""

        state = self._begin_schedule_activity()
        try:
            self._observe_local_generation(state, now=now)
            self._clear_worker_failure(
                "OBSERVER",
                expected_generation=state.generation,
            )
        finally:
            self._finish_schedule_activity(state.generation)

    def _observe_local_generation(
        self,
        state: _ReactionProviderSnapshot,
        *,
        now: datetime,
    ) -> None:
        checked_at = utc_datetime(now, field="now")
        tick = getattr(self._observer, "tick", None)
        if callable(tick):
            for stable_key, scheduled_spec in state.specs.items():
                identity = state.identities.get(stable_key)
                if (
                    identity is not None
                    and scheduled_spec.scheduled_at - _BASELINE_ARM_LEAD
                    <= checked_at
                    <= scheduled_spec.scheduled_at + WINDOW_DURATION + _ENDPOINT_GRACE
                ):
                    tick(identity, now=checked_at)
        self._persist_cached_market_evidence(state, now=checked_at)

    def _persist_cached_market_evidence(
        self,
        state: _ReactionProviderSnapshot,
        *,
        now: datetime,
    ) -> None:
        """Bind already-captured endpoints to releases; perform no quote acquisition."""

        if self._observer is None:
            return
        public_ids = tuple(
            state.stable_to_public.get(key, key) for key in state.specs
        )
        ledgers = list(self.reactions(public_ids))
        for children in self.child_reactions(public_ids).values():
            ledgers.extend(children)
        for ledger in ledgers:
            if ledger.current_stage is not ReactionStage.SURPRISE_ASSESSED:
                continue
            market, option = self._observer.observe(ledger, now=now)
            if market is None:
                continue
            root_hash = ledger.identity.event_hash
            self._store.append(
                event_id=ledger.identity.event_id,
                official_event_hash=root_hash,
                kind="MARKET_REACTION",
                observed_at=market.observed_at,
                document=market.as_dict(),
            )
            if option is not None:
                self._store.append(
                    event_id=ledger.identity.event_id,
                    official_event_hash=root_hash,
                    kind="OPTION_REEVALUATION",
                    observed_at=option.observed_at,
                    document=option.as_dict(),
                )
        for stable_key, spec in state.specs.items():
            if FAMILY_SPECS[spec.parent.family].surprise_supported:
                continue
            identity = state.identities.get(stable_key)
            if identity is None:
                continue
            vintages = self._store.release_vintages(
                stable_key,
                identity.event_hash,
            )
            baseline = self._store.load_baseline(
                identity.event_hash,
                scheduled_at=identity.scheduled_at,
            )
            endpoint = self._store.load_endpoint(
                identity.event_hash,
                scheduled_at=identity.scheduled_at,
            )
            if not vintages or baseline is None or endpoint is None:
                continue
            release_hash = str(vintages[0]["content_hash"])
            market = MarketReactionEvidence(
                event_hash=identity.event_hash,
                release_hash=release_hash,
                source="LOCAL_CACHED_READ_ONLY_QUOTES",
                window_start=identity.scheduled_at,
                window_end=identity.scheduled_at + WINDOW_DURATION,
                evidence_asof=endpoint.observed_at,
                observed_at=endpoint.observed_at,
                metrics={
                    "baseline": {
                        key: str(value) for key, value in baseline.values.items()
                    },
                    "endpoint": {
                        key: str(value) for key, value in endpoint.values.items()
                    },
                    "document_only": True,
                    "numeric_surprise": "UNAVAILABLE",
                    "decision_authority": "SUPPORTING_ONLY",
                },
            )
            self._store.append(
                event_id=stable_key,
                official_event_hash=identity.event_hash,
                kind="MARKET_REACTION",
                observed_at=market.observed_at,
                document=market.as_dict(),
            )

    def refresh_capture(self, *, now: datetime) -> None:
        """Run bounded consensus/document capture only inside an armed window."""

        state = self._begin_schedule_activity()
        try:
            self._refresh_capture_generation(state, now=now)
            self._clear_worker_failure(
                "CAPTURE",
                expected_generation=state.generation,
            )
        finally:
            self._finish_schedule_activity(state.generation)

    def _refresh_capture_generation(
        self,
        state: _ReactionProviderSnapshot,
        *,
        now: datetime,
    ) -> None:
        checked_at = utc_datetime(now, field="now")
        armed_specs = {
            stable_key: spec
            for stable_key, spec in state.specs.items()
            if spec.scheduled_at - _BASELINE_ARM_LEAD
            <= checked_at
            <= spec.capture_deadline
        }
        if not armed_specs:
            self._update_display_state(
                reason="NO_ARMED_REACTION_CAPTURE_WINDOW",
                attempted_at=checked_at,
                expected_generation=state.generation,
            )
            return
        capture = getattr(self._official_document_provider, "capture", None)
        capture_failed = False
        capture_progressed = False
        if callable(capture):
            for stable_key, scheduled_spec in armed_specs.items():
                if checked_at < scheduled_spec.scheduled_at:
                    continue
                if self._store.release_vintages(
                    stable_key,
                    scheduled_spec.official_event_hash,
                ):
                    self._update_display_state(
                        reason="INITIAL_OFFICIAL_RELEASE_VINTAGE_CAPTURED",
                        attempted_at=checked_at,
                        capture_reason=(
                            stable_key,
                            "INITIAL_OFFICIAL_RELEASE_VINTAGE_CAPTURED",
                        ),
                        expected_generation=state.generation,
                    )
                    continue
                if checked_at > scheduled_spec.capture_deadline:
                    self._update_display_state(
                        reason="WAIT_NEXT_ELIGIBLE_RELEASE:LATE_CAPTURE_DEADLINE",
                        attempted_at=checked_at,
                        capture_reason=(
                            stable_key,
                            "WAIT_NEXT_ELIGIBLE_RELEASE:LATE_CAPTURE_DEADLINE",
                        ),
                        expected_generation=state.generation,
                    )
                    continue
                with self._state_lock:
                    last_attempt = self._capture_attempted_at.get(stable_key)
                    if (
                        last_attempt is not None
                        and checked_at - last_attempt < timedelta(seconds=30)
                    ):
                        continue
                    self._capture_attempted_at[stable_key] = checked_at
                try:
                    captured = capture(scheduled_spec, now=checked_at)
                    inserted = self._store.append_verified_capture(
                        event_id=stable_key,
                        captured=captured,
                    )
                    capture_progressed = capture_progressed or inserted
                    capture_reason = (
                        "OFFICIAL_PARSED_RELEASE_CAPTURED"
                        if inserted
                        else "OFFICIAL_RELEASE_VINTAGE_ALREADY_CAPTURED"
                    )
                    self._update_display_state(
                        reason=capture_reason,
                        attempted_at=checked_at,
                        capture_reason=(stable_key, capture_reason),
                        expected_generation=state.generation,
                    )
                except Exception as exc:
                    capture_failed = True
                    reason = str(exc).strip()
                    safe_reason = (
                        reason
                        if re.fullmatch(r"[A-Z0-9_:-]+", reason)
                        else "OFFICIAL_RELEASE_DOCUMENT_UNAVAILABLE"
                    )
                    self._update_display_state(
                        reason=safe_reason,
                        attempted_at=checked_at,
                        capture_reason=(stable_key, safe_reason),
                        expected_generation=state.generation,
                    )
                    self.record_worker_failure(
                        "CAPTURE",
                        now=checked_at,
                        reason=safe_reason,
                        expected_generation=state.generation,
                    )
        numeric_keys = tuple(
            stable_key
            for stable_key, spec in armed_specs.items()
            if FAMILY_SPECS[spec.parent.family].surprise_supported
            and checked_at < spec.scheduled_at
        )
        if not numeric_keys:
            final_reason = (
                "OFFICIAL_RELEASE_DOCUMENT_UNAVAILABLE"
                if capture_failed
                else "OFFICIAL_RELEASE_DOCUMENT_CAPTURED"
                if capture_progressed
                else "NO_ELIGIBLE_REACTION_EVENTS"
            )
            self._update_display_state(
                reason=final_reason,
                attempted_at=checked_at,
                expected_generation=state.generation,
            )
            return
        identities = {
            stable_key: identity
            for stable_key, identity in state.identities.items()
            if stable_key in numeric_keys
        }
        reader = getattr(self._jin10_client, "fetch_calendar", None)
        getter = getattr(self._jin10_secret_store, "get", None)
        if not callable(reader) or not callable(getter):
            self._update_display_state(
                reason="JIN10_EXPECTATION_UNAVAILABLE",
                attempted_at=checked_at,
                expected_generation=state.generation,
            )
            return
        token = getter("JIN10_MCP_TOKEN")
        if not token:
            self._update_display_state(
                reason="JIN10_EXPECTATION_UNAVAILABLE",
                attempted_at=checked_at,
                expected_generation=state.generation,
            )
            return
        try:
            batch = reader(token)
            observations = normalize_jin10_calendar(batch.payload, observed_at=checked_at)
        except Exception:
            self._update_display_state(
                reason="JIN10_EXPECTATION_UNAVAILABLE",
                attempted_at=checked_at,
                expected_generation=state.generation,
            )
            self.record_worker_failure(
                "CAPTURE",
                now=checked_at,
                reason="JIN10_EXPECTATION_UNAVAILABLE",
                expected_generation=state.generation,
            )
            return
        matched = 0
        for observation in observations:
            identity = _match_official_identity(
                observation,
                tuple(identities.values()),
                state.specs,
            )
            if identity is None:
                continue
            document = observation.as_dict()
            if observation.consensus is not None and checked_at < identity.scheduled_at:
                self._store.append(
                    event_id=identity.event_id,
                    official_event_hash=identity.event_hash,
                    kind="JIN10_EXPECTATION",
                    observed_at=checked_at,
                    document=document,
                )
                matched += 1
            if observation.reported_actual is not None:
                # Retain, but never promote, a supplemental reported value.
                self._store.append(
                    event_id=identity.event_id,
                    official_event_hash=identity.event_hash,
                    kind="JIN10_REPORTED_ACTUAL",
                    observed_at=checked_at,
                    document=document,
                )
        self._update_display_state(
            reason="READY" if matched else "NO_PRE_RELEASE_EXPECTATION",
            attempted_at=checked_at,
            expected_generation=state.generation,
        )

    def refresh(self, official_snapshot: object, *, now: datetime) -> None:
        """Compatibility wrapper for explicit tests; production workers are split."""

        self.refresh_schedule(official_snapshot, now=now)
        self.refresh_capture(now=now)

    def reactions(self, event_ids: tuple[str, ...]) -> Iterable[EventReactionLedger]:
        now = utc_datetime(self._clock(), field="clock")
        state = self._state()
        stable_keys = tuple(
            state.public_to_stable.get(event_id, event_id) for event_id in event_ids
        )
        event_keys = tuple(
            (stable_key, identity.event_hash)
            for stable_key in stable_keys
            if (identity := state.identities.get(stable_key)) is not None
        )
        verified = self._store.verified_event_batch(event_keys)
        grouped: dict[tuple[str, str], list[Mapping[str, object]]] = {}
        for event_key in verified.event_keys:
            grouped[event_key] = list(verified.records(*event_key))
        ledgers: list[EventReactionLedger] = []
        for event_id in stable_keys:
            identity = state.identities.get(event_id)
            if identity is None:
                continue
            rows = grouped.get((event_id, identity.event_hash), [])
            expectation_row = next(
                (
                    row for row in reversed(rows)
                    if row["kind"] == "JIN10_EXPECTATION"
                    and _parse_time(row["observed_at"]) < identity.scheduled_at
                ),
                None,
            )
            if expectation_row is None:
                continue
            observation = _observation_from_document(expectation_row["document"])
            if observation.consensus is None:
                continue
            expectation = ConsensusExpectation(
                event_hash=identity.event_hash,
                metric=observation.metric,
                expected_value=observation.consensus,
                unit=observation.unit,
                period=observation.period,
                basis=observation.basis,
                provider="Jin10 point-in-time calendar",
                source_id=observation.source_id,
                published_at=observation.observed_at,
                first_seen_at=observation.observed_at,
                observed_at=observation.observed_at,
                vintage=observation.observed_at.isoformat(),
            )
            ledger = EventReactionLedger.schedule(
                identity,
                expectation,
                recorded_at=expectation.observed_at,
            )
            if now < identity.scheduled_at:
                ledgers.append(ledger)
                continue
            ledger = ledger.await_release(recorded_at=identity.scheduled_at)
            releases = _releases_from_vintages(
                identity,
                expectation,
                verified.release_vintages(event_id, identity.event_hash),
            )
            if not releases:
                ledgers.append(
                    ledger.mark_official_actual_unverified(recorded_at=now)
                )
                continue
            # Derive transition time from immutable evidence, never from the
            # replay clock.  This keeps a restored terminal ledger byte-for-
            # byte stable across process restarts.
            ledger = ledger.capture_release(
                (releases[0],),
                recorded_at=releases[0].captured_at,
            )
            ledger = ledger.assess_surprise(recorded_at=releases[0].captured_at)
            selected_release_hash = releases[0].content_hash
            market_row = next(
                (
                    row
                    for row in reversed(rows)
                    if row["kind"] == "MARKET_REACTION"
                    and isinstance(row.get("document"), Mapping)
                    and row["document"].get("release_hash") == selected_release_hash
                ),
                None,
            )
            option_row = next(
                (
                    row
                    for row in reversed(rows)
                    if row["kind"] == "OPTION_REEVALUATION"
                    and isinstance(row.get("document"), Mapping)
                    and row["document"].get("release_hash") == selected_release_hash
                ),
                None,
            )
            market = (
                None
                if market_row is None
                else _market_reaction_from_document(market_row["document"])
            )
            if market_row is not None and market is None:
                raise MacroReactionError("STORED_MARKET_REACTION_INVALID")
            option = (
                None
                if option_row is None
                else _option_reevaluation_from_document(option_row["document"])
            )
            if option_row is not None and option is None:
                raise MacroReactionError("STORED_OPTION_REEVALUATION_INVALID")
            if market is not None:
                ledger = ledger.observe_market(
                    market,
                    recorded_at=market.observed_at,
                )
                if option is not None and not ledger.terminal:
                    ledger = ledger.reevaluate_option(
                        option,
                        recorded_at=option.observed_at,
                    )
            ledgers.append(ledger)
        return tuple(ledgers)

    def child_reactions(
        self,
        event_ids: tuple[str, ...],
    ) -> Mapping[str, tuple[EventReactionLedger, ...]]:
        """Replay numeric families as independent parent-bound measure ledgers."""

        now = utc_datetime(self._clock(), field="clock")
        state = self._state()
        public_event_ids = _bounded_reaction_event_ids(event_ids)
        parent_keys = tuple(
            (stable_key, identity.event_hash)
            for public_event_id in public_event_ids
            if (
                stable_key := state.public_to_stable.get(
                    public_event_id,
                    public_event_id,
                )
            )
            and (identity := state.identities.get(stable_key)) is not None
            and (spec := state.specs.get(stable_key)) is not None
            and FAMILY_SPECS[spec.parent.family].surprise_supported
        )
        verified = self._store.verified_reaction_tree_batch(parent_keys)
        return self._child_reactions_from_verified(
            public_event_ids,
            state=state,
            verified=verified,
            now=now,
        )

    def _child_reactions_from_verified(
        self,
        public_event_ids: tuple[str, ...],
        *,
        state: _ReactionProviderSnapshot,
        verified: VerifiedReactionBatch,
        now: datetime,
    ) -> Mapping[str, tuple[EventReactionLedger, ...]]:
        output: dict[str, tuple[EventReactionLedger, ...]] = {}
        for public_event_id in public_event_ids:
            stable_key = state.public_to_stable.get(public_event_id, public_event_id)
            identity = state.identities.get(stable_key)
            spec = state.specs.get(stable_key)
            if identity is None or spec is None or not FAMILY_SPECS[spec.parent.family].surprise_supported:
                continue
            rows = verified.records(stable_key, identity.event_hash)
            expectations: dict[str, Jin10MacroObservation] = {}
            for row in rows:
                if row["kind"] != "JIN10_EXPECTATION" or _parse_time(row["observed_at"]) >= identity.scheduled_at:
                    continue
                try:
                    observation = _observation_from_document(row["document"])
                except Exception:
                    continue
                if observation.consensus is not None:
                    expectations[observation.metric] = observation
            children: list[EventReactionLedger] = []
            for metric, observation in expectations.items():
                child_identity = ScheduledEventIdentity(
                    event_id=f"{stable_key}:{metric}",
                    official_source=identity.official_source,
                    official_source_id=f"{identity.official_source_id}:{metric}",
                    title=f"{identity.title} [{metric}]",
                    category=identity.category,
                    scheduled_at=identity.scheduled_at,
                    schedule_published_at=identity.schedule_published_at,
                    schedule_first_seen_at=identity.schedule_first_seen_at,
                    schedule_observed_at=identity.schedule_observed_at,
                    symbols=identity.symbols,
                    reaction_root_hash=identity.event_hash,
                )
                expectation = ConsensusExpectation(
                    event_hash=child_identity.event_hash,
                    metric=metric,
                    expected_value=observation.consensus,
                    unit=observation.unit,
                    period=observation.period,
                    basis=observation.basis,
                    provider="Jin10 point-in-time calendar",
                    source_id=observation.source_id,
                    published_at=observation.observed_at,
                    first_seen_at=observation.observed_at,
                    observed_at=observation.observed_at,
                    vintage=observation.observed_at.isoformat(),
                )
                ledger = EventReactionLedger.schedule(
                    child_identity,
                    expectation,
                    recorded_at=expectation.observed_at,
                )
                if now >= identity.scheduled_at:
                    ledger = ledger.await_release(recorded_at=identity.scheduled_at)
                    releases = _releases_from_vintages(
                        child_identity,
                        expectation,
                        verified.release_vintages(stable_key, identity.event_hash),
                    )
                    if releases:
                        ledger = ledger.capture_release(
                            (releases[0],),
                            recorded_at=releases[0].captured_at,
                        ).assess_surprise(recorded_at=releases[0].captured_at)
                        child_rows = verified.records(
                            child_identity.event_id,
                            identity.event_hash,
                        )
                        market_row = next(
                            (
                                row
                                for row in reversed(child_rows)
                                if row["kind"] == "MARKET_REACTION"
                                and isinstance(row.get("document"), Mapping)
                                and row["document"].get("release_hash")
                                == releases[0].content_hash
                            ),
                            None,
                        )
                        if market_row is not None:
                            market = _market_reaction_from_document(
                                market_row["document"]
                            )
                            if market is None:
                                raise MacroReactionError(
                                    "STORED_MARKET_REACTION_INVALID"
                                )
                            ledger = ledger.observe_market(
                                market,
                                recorded_at=market.observed_at,
                            )
                            option_row = next(
                                (
                                    row
                                    for row in reversed(child_rows)
                                    if row["kind"] == "OPTION_REEVALUATION"
                                    and isinstance(row.get("document"), Mapping)
                                    and row["document"].get("release_hash")
                                    == releases[0].content_hash
                                ),
                                None,
                            )
                            if option_row is not None and not ledger.terminal:
                                option = _option_reevaluation_from_document(
                                    option_row["document"]
                                )
                                if option is None:
                                    raise MacroReactionError(
                                        "STORED_OPTION_REEVALUATION_INVALID"
                                    )
                                ledger = ledger.reevaluate_option(
                                    option,
                                    recorded_at=option.observed_at,
                                )
                    else:
                        ledger = ledger.mark_official_actual_unverified(recorded_at=now)
                children.append(ledger)
            if children:
                output[public_event_id] = tuple(children)
        return MappingProxyType(output)

    def revision_views(
        self,
        event_ids: tuple[str, ...],
    ) -> Mapping[str, tuple[Mapping[str, object], ...]]:
        """Project explicit revision chains without mutating the initial reaction."""

        now = utc_datetime(self._clock(), field="clock")
        state = self._state()
        public_event_ids = _bounded_reaction_event_ids(event_ids)
        parent_keys = tuple(
            (stable_key, identity.event_hash)
            for public_event_id in public_event_ids
            if (
                stable_key := state.public_to_stable.get(
                    public_event_id,
                    public_event_id,
                )
            )
            and (identity := state.identities.get(stable_key)) is not None
            and (spec := state.specs.get(stable_key)) is not None
            and FAMILY_SPECS[spec.parent.family].surprise_supported
        )
        verified = self._store.verified_reaction_tree_batch(parent_keys)
        output: dict[str, tuple[Mapping[str, object], ...]] = {}
        children = self._child_reactions_from_verified(
            public_event_ids,
            state=state,
            verified=verified,
            now=now,
        )
        for public_event_id, ledgers in children.items():
            stable_key = state.public_to_stable.get(
                public_event_id,
                public_event_id,
            )
            identity = state.identities.get(stable_key)
            if identity is None:
                continue
            vintages = verified.release_vintages(
                stable_key,
                identity.event_hash,
            )
            views: list[Mapping[str, object]] = []
            for ledger in ledgers:
                releases = _releases_from_vintages(
                    ledger.identity,
                    ledger.expectation,
                    vintages,
                )
                if not releases:
                    continue
                initial = releases[0]
                revised = releases[-1] if len(releases) > 1 else None
                views.append(
                    MappingProxyType(
                        {
                            "reaction_id": ledger.identity.event_id,
                            "initial_release": initial.as_dict(),
                            "revised_release": (
                                None if revised is None else revised.as_dict()
                            ),
                            "revision_history": tuple(
                                item.as_dict() for item in releases[1:]
                            ),
                            "initial_reaction_immutable": True,
                            "decision_authority": "SUPPORTING_ONLY",
                        }
                    )
                )
            if views:
                output[public_event_id] = tuple(views)
        return MappingProxyType(output)

    def close(self) -> None:
        self._store.close()


def _bounded_reaction_event_ids(event_ids: Sequence[str]) -> tuple[str, ...]:
    requested = tuple(event_ids)
    if any(
        not isinstance(event_id, str)
        or not event_id.strip()
        or event_id != event_id.strip()
        for event_id in requested
    ):
        raise ValueError("reaction coverage event id is invalid")
    bounded = tuple(dict.fromkeys(requested))
    if len(bounded) > 500:
        raise ValueError("at most 500 reaction coverage event ids may be read")
    return bounded


def _is_supported_official_event(event: object) -> bool:
    """Return whether G040 admits the event's official-document family."""

    family = classify_event_family(
        getattr(event, "source", ""),
        getattr(event, "title", ""),
        getattr(event, "category", ""),
    )
    return FAMILY_SPECS[family].support_state is SupportState.SUPPORTED


def _jin10_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M").replace(
                tzinfo=_SHANGHAI
            )
        except ValueError:
            return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=_SHANGHAI)
    return parsed.astimezone(_UTC)


def _macro_spec(
    title: str,
    scheduled_at: datetime | None,
) -> tuple[str, str, str, str, str] | None:
    if scheduled_at is None or "美国" not in title:
        return None
    local = scheduled_at.astimezone(_SHANGHAI)
    month_match = _MONTH_RE.search(title)
    quarter_match = _QUARTER_RE.search(title)
    if month_match is None and quarter_match is None:
        return None
    if month_match is not None:
        month = int(month_match.group("month"))
        year = local.year - 1 if month > local.month else local.year
        period = f"{year:04d}-{month:02d}"
    else:
        assert quarter_match is not None
        quarter = int(
            quarter_match.group("quarter")
            or {"第一": "1", "第二": "2", "第三": "3", "第四": "4"}[
                quarter_match.group("quarter_word")
            ]
        )
        year = local.year - 1 if quarter * 3 > local.month + 3 else local.year
        period = f"{year:04d}-Q{quarter}"
    core = "核心" in title
    yoy = "年率" in title
    calculation = "YOY" if yoy else "MOM"
    if "CPI" in title.upper():
        series = _BLS_SERIES[("CPI", core, calculation)]
        metric = ("core_" if core else "headline_") + "cpi_" + ("yoy_pct" if yoy else "mom_pct")
        basis = "NOT_SEASONALLY_ADJUSTED" if yoy else "SEASONALLY_ADJUSTED"
    elif "PPI" in title.upper():
        series = _BLS_SERIES[("PPI", core, calculation)]
        metric = ("core_" if core else "headline_") + "ppi_" + ("yoy_pct" if yoy else "mom_pct")
        basis = "NOT_SEASONALLY_ADJUSTED" if yoy else "SEASONALLY_ADJUSTED"
    elif "非农" in title:
        metric = "total_nonfarm_payroll_change_thousands"
        basis = "SEASONALLY_ADJUSTED"
        series = "CES0000000001"
        calculation = "LEVEL"
    elif "失业率" in title:
        metric = "unemployment_rate_pct"
        basis = "SEASONALLY_ADJUSTED"
        series = "LNS14000000"
        calculation = "LEVEL"
    elif "PCE" in title.upper():
        metric = (
            ("core_" if core else "")
            + "pce_price_index_"
            + ("yoy_pct" if yoy else "mom_pct")
        )
        basis = (
            "NOT_SEASONALLY_ADJUSTED_YEAR_OVER_YEAR"
            if yoy
            else "SEASONALLY_ADJUSTED"
        )
        series = "BEA:PCE"
    elif "GDP" in title.upper() and quarter_match is not None:
        metric = "real_gdp_annual_rate_pct"
        basis = "SEASONALLY_ADJUSTED_ANNUAL_RATE"
        series = "BEA:GDP"
        calculation = "LEVEL"
    else:
        return None
    return metric, period, basis, series, calculation


def _decimal_or_none(value: object) -> Decimal | None:
    if value in (None, "", "--"):
        return None
    try:
        result = Decimal(str(value).replace("%", "").strip())
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _valid_hash(value: object) -> bool:
    return isinstance(value, str) and _HASH_RE.fullmatch(value.lower()) is not None


def _bls_values(payload: object, series_id: str) -> dict[tuple[int, int], Decimal]:
    if not isinstance(payload, Mapping) or payload.get("status") != "REQUEST_SUCCEEDED":
        return {}
    results = payload.get("Results")
    series = results.get("series") if isinstance(results, Mapping) else None
    if not isinstance(series, Sequence) or len(series) != 1 or not isinstance(series[0], Mapping):
        return {}
    if series[0].get("seriesID") != series_id:
        return {}
    output: dict[tuple[int, int], Decimal] = {}
    for row in series[0].get("data", ()):
        if not isinstance(row, Mapping):
            continue
        period = str(row.get("period") or "")
        if not re.fullmatch(r"M(0[1-9]|1[0-2])", period):
            continue
        value = _decimal_or_none(row.get("value"))
        if value is not None:
            output[(int(row["year"]), int(period[1:]))] = value
    return output


def _match_official_identity(
    observation: Jin10MacroObservation,
    identities: Sequence[ScheduledEventIdentity],
    specs: Mapping[str, ScheduledReactionSpec] | None = None,
) -> ScheduledEventIdentity | None:
    if "ppi_" in observation.metric:
        family = EventFamily.PPI
    elif "cpi_" in observation.metric:
        family = EventFamily.CPI
    elif observation.metric in {
        "total_nonfarm_payroll_change_thousands",
        "unemployment_rate_pct",
    }:
        family = EventFamily.EMPLOYMENT_SITUATION
    elif "pce_price_index" in observation.metric:
        family = EventFamily.PCE
    elif observation.metric == "real_gdp_annual_rate_pct":
        family = EventFamily.GDP
    else:
        return None
    matches = [
        item for item in identities
        if classify_event_family(item.official_source, item.title, item.category) is family
        and abs((item.scheduled_at - observation.scheduled_at).total_seconds()) <= 300
        and (
            specs is None
            or (spec := specs.get(item.event_id)) is None
            or spec.parent.reference_period == observation.period
        )
        and (
            family is not EventFamily.GDP
            or specs is None
            or (gdp_spec := specs.get(item.event_id)) is None
            or gdp_spec.parent.estimate_label == _jin10_gdp_estimate_label(observation.title)
        )
    ]
    return matches[0] if len(matches) == 1 else None


def _jin10_gdp_estimate_label(title: str) -> str | None:
    upper = title.upper()
    if "初值" in title or "ADVANCE" in upper:
        return "ADVANCE"
    if "修正值" in title or "SECOND" in upper:
        return "SECOND"
    if "终值" in title or "THIRD" in upper:
        return "THIRD"
    return None


def _parse_time(value: object) -> datetime:
    return utc_datetime(datetime.fromisoformat(str(value)), field="stored time")


def _observation_from_document(value: object) -> Jin10MacroObservation:
    if not isinstance(value, Mapping):
        raise ValueError("stored Jin10 observation is invalid")
    return Jin10MacroObservation(
        title=str(value["title"]),
        scheduled_at=_parse_time(value["scheduled_at"]),
        metric=str(value["metric"]),
        unit=str(value["unit"]),
        period=str(value["period"]),
        basis=str(value["basis"]),
        series_id=str(value["series_id"]),
        calculation=str(value["calculation"]),
        consensus=_decimal_or_none(value.get("consensus")),
        reported_actual=_decimal_or_none(value.get("reported_actual")),
        previous=_decimal_or_none(value.get("previous")),
        source_id=str(value["source_id"]),
        observed_at=_parse_time(value["observed_at"]),
        content_hash=str(value.get("content_hash") or ""),
    )


def _releases_from_vintages(
    identity: ScheduledEventIdentity,
    expectation: ConsensusExpectation,
    vintages: Sequence[Mapping[str, object]],
) -> tuple[OfficialRelease, ...]:
    """Project only exact measure-compatible parsed vintages into the legacy ledger."""

    selected_chain: tuple[OfficialRelease, ...] = ()
    original_root_hash: str | None = None
    releases_by_hash: dict[str, OfficialRelease] = {}
    chains_by_hash: dict[str, tuple[OfficialRelease, ...]] = {}
    roots_by_hash: dict[str, str] = {}
    for row in vintages:
        document = row.get("document")
        if not isinstance(document, Mapping):
            continue
        measures = document.get("measures")
        if not isinstance(measures, Sequence) or isinstance(measures, (str, bytes, bytearray)):
            continue
        matches = [
            measure
            for measure in measures
            if isinstance(measure, Mapping)
            and measure.get("measure_id") == expectation.metric
            and measure.get("unit") == expectation.unit
            and measure.get("basis") == expectation.basis
            and document.get("reference_period") == expectation.period
        ]
        if len(matches) != 1 or matches[0].get("value") is None:
            continue
        try:
            captured_at = _parse_time(document["captured_at"])
            released_at = _parse_time(
                document.get("declared_release_at") or identity.scheduled_at.isoformat()
            )
            vintage_at = _parse_time(
                document.get("first_observed_release_at")
                or document["captured_at"]
            )
            revision_of = document.get("revision_of")
            if revision_of is not None and not _valid_hash(revision_of):
                continue
            previous = (
                None
                if revision_of is None
                else releases_by_hash.get(str(revision_of))
            )
            if revision_of is not None and previous is None:
                # A narrative or later row is not revision authority. Only an
                # explicit link to a validated prior release may extend a chain.
                continue
            release = OfficialRelease(
                event_hash=identity.event_hash,
                metric=expectation.metric,
                actual_value=_decimal_or_none(matches[0].get("value")),
                unit=expectation.unit,
                period=expectation.period,
                basis=expectation.basis,
                official_source=identity.official_source,
                source_id=(
                    f"{document.get('official_url')}#"
                    f"{document.get('raw_hash')}"
                ),
                released_at=released_at,
                vintage_at=vintage_at,
                captured_at=captured_at,
                published_precision="0.1",
                revision=0 if previous is None else previous.revision + 1,
                supersedes_hash=None if previous is None else previous.content_hash,
            )
        except Exception:
            continue
        releases_by_hash[release.content_hash] = release
        chain = (
            (release,)
            if previous is None
            else (*chains_by_hash[previous.content_hash], release)
        )
        chains_by_hash[release.content_hash] = chain
        root_hash = (
            release.content_hash
            if previous is None
            else roots_by_hash[previous.content_hash]
        )
        roots_by_hash[release.content_hash] = root_hash
        if original_root_hash is None:
            original_root_hash = root_hash
        if root_hash == original_root_hash:
            selected_chain = chain
    return selected_chain


def _release_from_document(value: object) -> OfficialRelease | None:
    if not isinstance(value, Mapping):
        return None
    try:
        return OfficialRelease(
            event_hash=str(value["event_hash"]),
            metric=str(value["metric"]),
            actual_value=_decimal_or_none(value.get("actual_value")),
            unit=str(value["unit"]),
            period=None if value.get("period") is None else str(value["period"]),
            basis=None if value.get("basis") is None else str(value["basis"]),
            raw_calculated_value=_decimal_or_none(
                value.get("raw_calculated_value")
            ),
            published_precision=(
                None
                if value.get("published_precision") is None
                else str(value["published_precision"])
            ),
            official_source=str(value["official_source"]),
            source_id=str(value["source_id"]),
            released_at=_parse_time(value["released_at"]),
            vintage_at=_parse_time(value["vintage_at"]),
            captured_at=_parse_time(value["captured_at"]),
            revision=int(value.get("revision", 0)),
            supersedes_hash=value.get("supersedes_hash"),
            content_hash=str(value.get("content_hash") or ""),
        )
    except Exception:
        return None


def _market_reaction_from_document(
    value: object,
) -> MarketReactionEvidence | None:
    if not isinstance(value, Mapping):
        return None
    try:
        metrics = value["metrics"]
        if not isinstance(metrics, Mapping):
            return None
        return MarketReactionEvidence(
            event_hash=str(value["event_hash"]),
            release_hash=str(value["release_hash"]),
            source=str(value["source"]),
            window_start=_parse_time(value["window_start"]),
            window_end=_parse_time(value["window_end"]),
            evidence_asof=_parse_time(value["evidence_asof"]),
            observed_at=_parse_time(value["observed_at"]),
            metrics=metrics,
            content_hash=str(value.get("content_hash") or ""),
        )
    except Exception:
        return None


def _option_reevaluation_from_document(
    value: object,
) -> OptionReevaluationEvidence | None:
    if not isinstance(value, Mapping):
        return None
    try:
        result = value["result"]
        hashes = value["input_evidence_hashes"]
        if (
            not isinstance(result, Mapping)
            or not isinstance(hashes, Sequence)
            or isinstance(hashes, (str, bytes, bytearray))
        ):
            return None
        return OptionReevaluationEvidence(
            event_hash=str(value["event_hash"]),
            release_hash=str(value["release_hash"]),
            market_reaction_hash=str(value["market_reaction_hash"]),
            option_id=str(value["option_id"]),
            candidate_hash=str(value["candidate_hash"]),
            source=str(value["source"]),
            evidence_asof=_parse_time(value["evidence_asof"]),
            observed_at=_parse_time(value["observed_at"]),
            input_evidence_hashes=tuple(str(item) for item in hashes),
            result=result,
            content_hash=str(value.get("content_hash") or ""),
        )
    except Exception:
        return None


__all__ = [
    "BlsPublicDataActualProvider",
    "Jin10MacroObservation",
    "MacroReactionError",
    "ProductionMacroReactionProvider",
    "ProductionReactionObserver",
    "ReactionEvidenceStore",
    "VerifiedReactionBatch",
    "normalize_jin10_calendar",
]
